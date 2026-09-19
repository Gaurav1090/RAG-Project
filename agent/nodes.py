"""LangGraph node functions (LLD Part D.5). One function per state in Section 6's diagram,
plus the Advisory branch and the SynthesizeAssessment join added for the two-pass design.

`build_nodes(spark, catalog, workspace_url, token, prompts_dir)` returns a dict of node
functions closing over the Databricks session/credentials — infra concerns that don't belong
in AssessmentState (which holds assessment data, not configuration).
"""

import json
import re
from datetime import datetime, timezone

from engine.metric_calculator import compute_metrics
from engine.policy_evaluator import evaluate_rules
from engine.result_builder import build_result
from engine.rule_loader import resolve_headline_version, select_applicable_rules

from agent import tools

MAX_EXPLANATION_ATTEMPTS = 3

ELIGIBILITY_TOKENS = {
    "eligible", "ineligible", "conditional", "manual review", "manual_review",
    "insufficient information", "insufficient_information", "approved", "rejected",
    "declined", "denied",
}


def _decimal_numbers(text):
    return {float(n) for n in re.findall(r"\d+\.\d+", text or "")}


def _numeric_cross_check(draft_explanation, engine_result, retrieved_evidence):
    """Every decimal number the Explanation Pass writes must already appear either in the
    engine's own rule_results or in the retrieved policy evidence text — never a number the
    model introduced on its own."""
    allowed = set()
    for r in engine_result.get("rule_results", []):
        if r.get("actual_value") is not None:
            allowed.add(float(r["actual_value"]))
        if r.get("required_value") is not None:
            allowed.add(float(r["required_value"]))
    evidence_text = " ".join(e.get("chunk_text", "") for e in retrieved_evidence)
    allowed |= _decimal_numbers(evidence_text)

    found = _decimal_numbers(draft_explanation)
    unverified = [n for n in found if not any(abs(n - a) < 1e-6 for a in allowed)]
    return [f"unverified number in explanation: {n}" for n in unverified]


def _advisory_verdict_guard(advisory_notes):
    lowered = (advisory_notes or "").lower()
    hits = [token for token in ELIGIBILITY_TOKENS if token in lowered]
    return [f"Advisory Pass output contains eligibility-status token(s): {hits}"] if hits else []


def build_nodes(spark, catalog, workspace_url, token, prompts_dir):

    def _load_prompt(filename):
        with open(f"{prompts_dir}/{filename}") as f:
            return f.read()

    explanation_template = _load_prompt("explanation_prompt.md")
    advisory_template = _load_prompt("advisory_prompt.md")

    def identify_application(state):
        row = spark.sql(f"""
            SELECT application_id, borrower_id, product, assessment_date, requested_loan_amount
            FROM {catalog}.gold.applications
            WHERE application_id = '{tools.sql_escape(state["application_id"])}'
        """).collect()[0]
        return {
            "borrower_id": row["borrower_id"],
            "application": {
                "application_id": row["application_id"],
                "requested_loan_amount": row["requested_loan_amount"],
            },
            "as_of_date": row["assessment_date"],
        }

    def retrieve_data(state):
        borrower_id = state["borrower_id"]
        borrower = tools.get_borrower_financials(spark, catalog, borrower_id)
        pan = tools.get_borrower_pan(spark, catalog, borrower_id)
        cibil = tools.get_cibil_report(spark, catalog, pan)
        repeat_offender = tools.check_repeat_offender_signals(spark, catalog, borrower_id)
        promoters = tools.get_promoter_financials(spark, catalog, borrower.get("owner_ids") or [])
        return {"raw_inputs": {
            "borrower": borrower, "cibil": cibil,
            "repeat_offender": repeat_offender, "promoters": promoters,
        }}

    def validate_data(state):
        borrower = state["raw_inputs"]["borrower"]
        missing = []
        if borrower.get("cibil_score") is None:
            missing.append("External bureau report")
        if borrower.get("dscr") is None:
            missing.append("Borrower DSCR / financial statement")
        return {"missing_fields": missing}

    def flag_missing_data(state):
        # Explicit no-op state so the graph's shape matches Section 6 exactly
        # (ValidateData -> FlagMissingData -> ResolvePolicy) — validate_data already computed
        # missing_fields; nothing further to annotate.
        return {}

    def evaluate_policy_rules(state):
        borrower = state["raw_inputs"]["borrower"]
        application = state["application"]
        as_of_date = state["as_of_date"]
        applicable_sector = borrower["sector"]

        rule_rows = spark.sql(f"""
            SELECT pr.rule_uid, pr.rule_id, pr.metric, pr.operator, pr.threshold,
                   pr.applicable_sector, pr.effective_date, pr.supersedes,
                   pr.source_document_id, pr.approval_status, doc.version
            FROM {catalog}.gold.policy_rules pr
            JOIN {catalog}.bronze.raw_policy_documents doc ON pr.source_document_id = doc.document_id
        """).collect()
        all_rules = [r.asDict() for r in rule_rows]

        applicable_rules = select_applicable_rules(all_rules, applicable_sector, as_of_date)
        metrics = compute_metrics(application, borrower)
        rule_results = evaluate_rules(metrics, applicable_rules)
        policy_version = resolve_headline_version(applicable_rules, applicable_sector)

        engine_result = build_result(
            application, rule_results, policy_version,
            missing_fields=list(state.get("missing_fields", [])),
            hard_block=bool(state["raw_inputs"]["repeat_offender"].get("wilful_defaulter_flag")),
        )
        return {"engine_result": engine_result}

    def retrieve_evidence(state):
        borrower = state["raw_inputs"]["borrower"]
        query = f"policy rules for {borrower['sector']} DSCR collateral coverage days past due"
        evidence = tools.retrieve_policy(spark, catalog, query, borrower["sector"], state["as_of_date"])
        return {"retrieved_evidence": evidence}

    def generate_explanation(state):
        engine_result = state["engine_result"]
        validation_feedback = ""
        if state.get("validation_errors"):
            validation_feedback = (
                "\nYour previous attempt was rejected for this reason — fix it:\n"
                f"{state['validation_errors']}\n"
            )
        prompt = explanation_template.format(
            borrower=json.dumps(state["raw_inputs"]["borrower"], default=str),
            engine_result=json.dumps(engine_result, default=str),
            evidence=json.dumps(state.get("retrieved_evidence", []), default=str),
            validation_feedback=validation_feedback,
            eligibility_status=engine_result["eligibility_status"],
        )
        draft = tools.call_llm(workspace_url, token, [{"role": "user", "content": prompt}])
        return {
            "draft_explanation": draft,
            "_explanation_attempts": state.get("_explanation_attempts", 0) + 1,
        }

    def run_advisory_pass(state):
        borrower = state["raw_inputs"]["borrower"]
        memos = tools.get_committee_memos(spark, catalog, borrower["sector"], state["borrower_id"])
        return {"committee_memos": memos}

    def generate_advisory_notes(state):
        prompt = advisory_template.format(
            borrower=json.dumps(state["raw_inputs"]["borrower"], default=str),
            promoters=json.dumps(state["raw_inputs"]["promoters"], default=str),
            memos=json.dumps(state.get("committee_memos", []), default=str),
        )
        notes = tools.call_llm(workspace_url, token, [{"role": "user", "content": prompt}])
        return {"advisory_notes": notes}

    def synthesize_assessment(state):
        engine_result = state["engine_result"]
        final_assessment = {
            "assessment_summary": (
                f"{state['application_id']}: "
                f"{engine_result['eligibility_status'].replace('_', ' ').title()}"
            ),
            "borrower_facts": state["raw_inputs"]["borrower"],
            "credit_metrics": engine_result["rule_results"],
            "policy_evaluation": engine_result["rule_results"],
            # Assessment Outcome is copied verbatim from the deterministic engine — never from
            # either LLM branch. This is the one line in the whole agent that makes the
            # "LLM never decides" claim structurally true rather than just documented.
            "assessment_outcome": engine_result["eligibility_status"],
            "narrative": state.get("draft_explanation", ""),
            "missing_information": engine_result.get("missing_information", []),
            "ai_advisory_notes": state.get("advisory_notes", ""),
            "policy_version": engine_result.get("policy_version"),
            "human_review": {
                "reviewer": None, "comments": None,
                "final_decision": None, "override_reason": None,
            },
        }
        return {"final_assessment": final_assessment}

    def validate_response(state):
        errors = _numeric_cross_check(
            state.get("draft_explanation", ""), state["engine_result"], state.get("retrieved_evidence", []),
        )
        errors += _advisory_verdict_guard(state.get("advisory_notes", ""))
        return {"validation_errors": errors}

    def human_review(state):
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.ops.decision_audit_log (
                application_id STRING, eligibility_status STRING, reviewer STRING,
                comments STRING, final_decision STRING, override_reason STRING,
                reviewed_at TIMESTAMP
            ) USING DELTA
        """)
        # Phase 4 has no human-in-the-loop UI yet (Section 12 names a full case-management
        # screen as a later extension) — log a pending_review row so the audit trail exists
        # end to end; a credit officer fills reviewer/comments/final_decision later.
        spark.sql(f"""
            INSERT INTO {catalog}.ops.decision_audit_log
            VALUES (
                '{tools.sql_escape(state["application_id"])}',
                '{tools.sql_escape(state["engine_result"]["eligibility_status"])}',
                NULL, NULL, 'pending_review', NULL,
                TIMESTAMP'{datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")}'
            )
        """)
        return {}

    return {
        "identify_application": identify_application,
        "retrieve_data": retrieve_data,
        "validate_data": validate_data,
        "flag_missing_data": flag_missing_data,
        "evaluate_policy_rules": evaluate_policy_rules,
        "retrieve_evidence": retrieve_evidence,
        "generate_explanation": generate_explanation,
        "run_advisory_pass": run_advisory_pass,
        "generate_advisory_notes": generate_advisory_notes,
        "synthesize_assessment": synthesize_assessment,
        "validate_response": validate_response,
        "human_review": human_review,
    }
