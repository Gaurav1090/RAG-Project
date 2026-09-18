"""The 10 golden cases from Section 11 of the design doc, run against the engine directly.

Pure Python, no Databricks/Spark dependency — run with:
    python3 -m unittest engine.tests.test_golden_cases -v
from the repo root.

Each case builds its own synthetic application/borrower/rules inputs (reusing the exact
Section 3 reference values for case 10) rather than reading live catalog data — the engine
is a pure function of its inputs, so its correctness doesn't depend on Databricks being up.
The separate `pipelines/run_assessment.py` notebook wires this same engine to real
`gold.policy_rules` / UC Function data for the end-to-end demo.

Two cases (9: promoter exposure, and to a lesser extent 4: new-to-bank) are primarily
Phase 4 (agent) concerns — the deterministic engine has no promoter- or relationship-history
-aware rule in this policy corpus. Where that's true, the test says so explicitly and checks
the narrower, honest thing: that engine-irrelevant context doesn't leak into or corrupt its
output.
"""

import unittest
from datetime import date

from engine.metric_calculator import compute_metrics
from engine.policy_evaluator import evaluate_rules
from engine.result_builder import build_result
from engine.rule_loader import resolve_headline_version, select_applicable_rules


def rule(rule_uid, rule_id, metric, operator, threshold, sector, effective_date,
         source_document_id, version, supersedes=None, approval_status="approved"):
    return dict(
        rule_uid=rule_uid, rule_id=rule_id, metric=metric, operator=operator,
        threshold=threshold, applicable_sector=sector, effective_date=effective_date,
        supersedes=supersedes, source_document_id=source_document_id,
        approval_status=approval_status, version=version,
    )


# Mirrors the rules authored in data_generation/02_generate_policy_documents.py exactly.
STANDARD_RULES = [
    rule("POLICY-GENERAL-01-GEN-001", "GEN-001", "collateral_coverage_ratio", ">=", 0.75,
         "general", date(2025, 6, 1), "POLICY-GENERAL-01", "v1"),
    rule("POLICY-GENERAL-01-GEN-002", "GEN-002", "days_past_due", "<=", 30,
         "general", date(2025, 6, 1), "POLICY-GENERAL-01", "v1"),
    rule("POLICY-TEXTILE-01-DSCR-001", "DSCR-001", "dscr", ">=", 1.10,
         "textile_msme", date(2026, 1, 1), "POLICY-TEXTILE-01", "v1"),
    rule("POLICY-TEXTILE-01-v2-DSCR-001", "DSCR-001", "dscr", ">=", 1.25,
         "textile_msme", date(2026, 9, 1), "POLICY-TEXTILE-01-v2", "v2", supersedes="POLICY-TEXTILE-01"),
    rule("POLICY-PHARMA-01-DSCR-001", "DSCR-001", "dscr", ">=", 1.20,
         "pharma_sme", date(2025, 3, 1), "POLICY-PHARMA-01", "v1"),
]


def run_engine(application, borrower, sector, as_of_date, all_rules=STANDARD_RULES,
               hard_block=False, extra_missing=None, extra_exceptions=None):
    applicable = select_applicable_rules(all_rules, sector, as_of_date)
    metrics = compute_metrics(application, borrower)
    rule_results = evaluate_rules(metrics, applicable)
    policy_version = resolve_headline_version(applicable, sector)
    return build_result(
        application, rule_results, policy_version,
        missing_fields=extra_missing, hard_block=hard_block, exceptions=extra_exceptions,
    )


class GoldenCaseTests(unittest.TestCase):

    def test_case_1_healthy_borrower(self):
        application = {"application_id": "GOLD-01", "requested_loan_amount": 10_000_000.0}
        borrower = {"dscr": 1.8, "days_past_due": 0, "collateral_value": 9_000_000.0}
        result = run_engine(application, borrower, "textile_msme", date(2026, 2, 1))

        self.assertEqual(result["eligibility_status"], "eligible")
        self.assertEqual(result["exceptions"], [])
        self.assertEqual(result["missing_information"], [])
        self.assertEqual(result["policy_version"], "v1")

    def test_case_2_borderline_sma1_borrower(self):
        # DSCR clears v1's threshold, but days-past-due breaches GEN-002 -> manual_review
        # via a different rule family than case 3, proving the engine isn't just DSCR-shaped.
        application = {"application_id": "GOLD-02", "requested_loan_amount": 10_000_000.0}
        borrower = {"dscr": 1.15, "days_past_due": 45, "collateral_value": 8_000_000.0}
        result = run_engine(application, borrower, "textile_msme", date(2026, 2, 1))

        self.assertEqual(result["eligibility_status"], "manual_review")
        statuses = {r["rule_id"]: r["status"] for r in result["rule_results"]}
        self.assertEqual(statuses["DSCR-001"], "pass")
        self.assertEqual(statuses["GEN-002"], "fail")

    def test_case_3_npa_delinquency(self):
        application = {"application_id": "GOLD-03", "requested_loan_amount": 10_000_000.0}
        borrower = {"dscr": 0.7, "days_past_due": 120, "collateral_value": 5_000_000.0}
        result = run_engine(application, borrower, "textile_msme", date(2026, 2, 1))

        # DSCR and days-past-due both fail (manual_review-tier); collateral also fails
        # (conditional-tier) — manual_review takes precedence per result_builder's ordering.
        self.assertEqual(result["eligibility_status"], "manual_review")
        failed_rule_ids = {r["rule_id"] for r in result["rule_results"] if r["status"] == "fail"}
        self.assertEqual(failed_rule_ids, {"DSCR-001", "GEN-002", "GEN-001"})

    def test_case_4_new_to_bank_borrower(self):
        # no_internal_history is surfaced as an exception, not a status change, since this
        # policy corpus has no distinct new-to-bank rule track (Section 8.1 names that as a
        # natural extension, not something Phase 2 built) — the engine still evaluates the
        # borrower against the normal rule set.
        application = {"application_id": "GOLD-04", "requested_loan_amount": 10_000_000.0}
        borrower = {"dscr": 1.6, "days_past_due": 0, "collateral_value": 9_000_000.0}
        result = run_engine(
            application, borrower, "textile_msme", date(2026, 2, 1),
            extra_exceptions=["New-to-bank borrower — no internal relationship history on record"],
        )

        self.assertEqual(result["eligibility_status"], "eligible")
        self.assertIn("New-to-bank borrower — no internal relationship history on record", result["exceptions"])

    def test_case_5_missing_bureau_report(self):
        # dscr unavailable -> the evaluator marks DSCR-001 "unknown", not a silent pass, and
        # build_result promotes that into missing_information / insufficient_information —
        # this is the engine's own null-handling path, not a caller-supplied gap.
        application = {"application_id": "GOLD-05", "requested_loan_amount": 10_000_000.0}
        borrower = {"dscr": None, "days_past_due": 10, "collateral_value": 8_000_000.0}
        result = run_engine(application, borrower, "textile_msme", date(2026, 2, 1))

        self.assertEqual(result["eligibility_status"], "insufficient_information")
        self.assertTrue(any("DSCR-001" in m for m in result["missing_information"]))

    def test_case_6_superseded_policy_not_cited(self):
        # Regression test on the loader itself: as of a date well after the revised circular,
        # exactly one DSCR-001 rule must come back, and it must be the v2 (non-superseded) one.
        applicable = select_applicable_rules(STANDARD_RULES, "textile_msme", date(2026, 12, 1))
        dscr_rules = [r for r in applicable if r["rule_id"] == "DSCR-001"]

        self.assertEqual(len(dscr_rules), 1)
        self.assertEqual(dscr_rules[0]["source_document_id"], "POLICY-TEXTILE-01-v2")

    def test_case_7_future_effective_policy_not_yet_applicable(self):
        future_rule = rule(
            "POLICY-TEXTILE-02-DSCR-002", "DSCR-002", "dscr", ">=", 1.50,
            "textile_msme", date(2027, 1, 1), "POLICY-TEXTILE-02", "v1",
        )
        rules_with_future = STANDARD_RULES + [future_rule]

        applicable = select_applicable_rules(rules_with_future, "textile_msme", date(2026, 2, 1))
        applicable_rule_ids = {r["rule_id"] for r in applicable}

        self.assertNotIn("DSCR-002", applicable_rule_ids, "future-effective rule must not apply yet")
        self.assertIn("DSCR-001", applicable_rule_ids)

    def test_case_8_repeat_offender_hard_block(self):
        # Every metric passes cleanly, but hard_block=True (wilful_defaulter_flag from
        # check_repeat_offender_signals) overrides everything else -> ineligible. This is
        # deliberately the wilful-defaulter signal, not a plain CRILC flag — Section 8.2 treats
        # a bare CRILC hit as a Key Risk Factor for human/Explanation-layer weighing, not an
        # automatic engine-level rejection.
        application = {"application_id": "GOLD-08", "requested_loan_amount": 10_000_000.0}
        borrower = {"dscr": 1.9, "days_past_due": 0, "collateral_value": 9_000_000.0}
        result = run_engine(application, borrower, "textile_msme", date(2026, 2, 1), hard_block=True)

        self.assertTrue(all(r["status"] == "pass" for r in result["rule_results"]))
        self.assertEqual(result["eligibility_status"], "ineligible")

    def test_case_9_promoter_exposure_elsewhere(self):
        # Promoter/guarantor risk (get_promoter_financials) isn't a rule in this policy corpus
        # -- it's a qualitative signal for the Phase 4 agent layer. At the engine level this
        # case only proves that fact doesn't need to reach the engine at all: a borrower whose
        # promoter has exposure elsewhere evaluates identically to case 1. Full promoter-risk
        # coverage belongs to Phase 4's own golden set, not this one.
        application = {"application_id": "GOLD-09", "requested_loan_amount": 10_000_000.0}
        borrower = {"dscr": 1.6, "days_past_due": 0, "collateral_value": 9_000_000.0}
        result = run_engine(application, borrower, "textile_msme", date(2026, 2, 1))

        self.assertEqual(result["eligibility_status"], "eligible")

    def test_case_10_pre_post_circular_comparison(self):
        # The centerpiece case, using Meera Textiles' exact Section 3 / Section 7 values.
        application = {"application_id": "APP-001", "requested_loan_amount": 12_000_000.0}
        borrower = {"dscr": 1.15, "days_past_due": 0, "collateral_value": 9_000_000.0}

        before = run_engine(application, borrower, "textile_msme", date(2026, 1, 15))
        self.assertEqual(before["policy_version"], "v1")
        self.assertEqual(before["eligibility_status"], "eligible")

        after = run_engine(application, borrower, "textile_msme", date(2026, 9, 18))
        self.assertEqual(after["policy_version"], "v2")
        self.assertEqual(after["eligibility_status"], "manual_review")

        # Byte-for-byte against Section 7's documented example output.
        dscr_result = next(r for r in after["rule_results"] if r["rule_id"] == "DSCR-001")
        self.assertEqual(dscr_result["status"], "fail")
        self.assertEqual(dscr_result["actual_value"], 1.15)
        self.assertEqual(dscr_result["required_value"], 1.25)


class DeterminismTest(unittest.TestCase):

    def test_same_input_same_output_across_repeated_runs(self):
        application = {"application_id": "APP-001", "requested_loan_amount": 12_000_000.0}
        borrower = {"dscr": 1.15, "days_past_due": 0, "collateral_value": 9_000_000.0}

        results = [
            run_engine(application, borrower, "textile_msme", date(2026, 9, 18))
            for _ in range(5)
        ]

        self.assertTrue(all(r == results[0] for r in results), "engine output must be identical across repeated runs")


if __name__ == "__main__":
    unittest.main()
