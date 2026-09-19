"""Phase 6 — Credit Officer Case-Management UI (Databricks App, Streamlit).

Lets a credit officer look up an assessment produced by the agent (Phase 4:
ops.assessment_explanations), see the deterministic outcome clearly separated from the
AI Advisory Notes, and record their own review into ops.decision_audit_log — replacing the
"pending_review" placeholder Phase 4's human_review node writes with a real decision.

One app deployment (this same code, deployed twice as credit-platform-review-dev/-prod for
dev/prod parity) serves both catalogs — the catalog is a runtime choice in the sidebar rather
than a build-time env var, since a reviewer plausibly wants to compare dev and prod from the
same session.

Auth: WorkspaceClient() auto-resolves the app's own service-principal credentials from the
Databricks Apps runtime environment — no token handling needed here, unlike the notebooks
(which fetch dbutils' notebook context token) or the local CLI scripts (which use a named
~/.databrickscfg profile). `.config.authenticate()` returns ready-to-use request headers
regardless of which auth mechanism is actually in effect.
"""

import json
from datetime import datetime, timezone

import requests
import streamlit as st
from databricks.sdk import WorkspaceClient

WAREHOUSE_ID = "c1fc34bf07af74a8"


@st.cache_resource
def get_client():
    return WorkspaceClient()


def sql_escape(value):
    return str(value).replace("'", "''")


def run_sql(statement):
    w = get_client()
    headers = w.config.authenticate()
    resp = requests.post(
        f"{w.config.host}/api/2.0/sql/statements",
        headers=headers,
        json={"warehouse_id": WAREHOUSE_ID, "statement": statement, "wait_timeout": "30s"},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    if data["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(data["status"])
    return data.get("result", {}).get("data_array", [])


st.set_page_config(page_title="Credit Officer Review", layout="wide")

st.sidebar.title("Credit Officer Review")
catalog = st.sidebar.selectbox("Catalog", ["credit_platform", "credit_platform_prod"], index=0)
application_id = st.sidebar.text_input("Application ID", value="APP-001")
load_clicked = st.sidebar.button("Load assessment", type="primary")

st.title("Credit Decision Support — Case Review")

if load_clicked:
    st.session_state["loaded_app"] = application_id
    st.session_state["loaded_catalog"] = catalog

if not st.session_state.get("loaded_app"):
    st.info("Enter an application ID in the sidebar and click **Load assessment**.")
    st.stop()

app_id = st.session_state["loaded_app"]
cat = st.session_state["loaded_catalog"]

rows = run_sql(f"""
    SELECT final_assessment_json, assessment_outcome, assessed_at
    FROM {cat}.ops.assessment_explanations
    WHERE application_id = '{sql_escape(app_id)}'
    ORDER BY assessed_at DESC LIMIT 1
""")

if not rows:
    st.warning(f"No assessment found for **{app_id}** in `{cat}`. Run the agent "
               f"(`dev_phase4_agent` / `prod_phase4_agent`) for this application first.")
    st.stop()

final_assessment = json.loads(rows[0][0])
assessment_outcome = rows[0][1]
assessed_at = rows[0][2]

OUTCOME_COLORS = {
    "eligible": "green", "manual_review": "orange", "conditional": "orange",
    "ineligible": "red", "insufficient_information": "gray",
}
color = OUTCOME_COLORS.get(assessment_outcome, "blue")

st.markdown(f"### Assessment Outcome: :{color}[{assessment_outcome.replace('_', ' ').title()}]")
st.caption(
    f"Deterministic — produced by the rules engine, never by the LLM. "
    f"Assessed at {assessed_at} · Application `{app_id}` · Catalog `{cat}`"
)

col1, col2 = st.columns(2)
with col1:
    st.subheader("Borrower Facts")
    st.json(final_assessment["borrower_facts"])
with col2:
    st.subheader("Policy Evaluation")
    st.dataframe(final_assessment["policy_evaluation"], use_container_width=True)

st.subheader("Explanation")
st.write(final_assessment["narrative"])

if final_assessment.get("missing_information"):
    st.subheader("Missing Information")
    for item in final_assessment["missing_information"]:
        st.write(f"- {item}")

st.subheader("AI Advisory Notes")
st.info(final_assessment["ai_advisory_notes"])
st.caption("Advisory only — a subordinate, independent qualitative read. Never part of the eligibility decision above.")

st.caption(f"Policy version: {final_assessment.get('policy_version', 'n/a')}")

st.divider()
st.subheader("Review History")
history_rows = run_sql(f"""
    SELECT reviewer, comments, final_decision, override_reason, reviewed_at
    FROM {cat}.ops.decision_audit_log
    WHERE application_id = '{sql_escape(app_id)}'
    ORDER BY reviewed_at DESC
""")
if history_rows:
    st.dataframe(
        [{"reviewer": r[0], "comments": r[1], "final_decision": r[2],
          "override_reason": r[3], "reviewed_at": r[4]} for r in history_rows],
        use_container_width=True,
    )
else:
    st.caption("No review history yet — the agent's own run logs a `pending_review` placeholder row here.")

st.divider()
st.subheader("Record Your Review")
with st.form("review_form"):
    reviewer = st.text_input("Reviewer name")
    comments = st.text_area("Comments")
    final_decision = st.selectbox("Final decision", ["Approved", "Rejected", "Escalated", "Deferred"])
    override_reason = st.text_area(
        "Override reason (required only if your decision disagrees with the deterministic outcome above)"
    )
    submitted = st.form_submit_button("Submit review")

    if submitted:
        disagrees = (
            (assessment_outcome == "eligible" and final_decision == "Rejected")
            or (assessment_outcome == "ineligible" and final_decision == "Approved")
        )
        if not reviewer.strip():
            st.error("Reviewer name is required.")
        elif disagrees and not override_reason.strip():
            st.error("An override reason is required when your decision disagrees with the deterministic outcome.")
        else:
            override_literal = f"'{sql_escape(override_reason)}'" if override_reason.strip() else "NULL"
            run_sql(f"""
                INSERT INTO {cat}.ops.decision_audit_log
                VALUES (
                    '{sql_escape(app_id)}', '{sql_escape(assessment_outcome)}',
                    '{sql_escape(reviewer)}', '{sql_escape(comments)}',
                    '{sql_escape(final_decision)}', {override_literal},
                    TIMESTAMP'{datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")}'
                )
            """)
            st.success("Review recorded.")
            st.rerun()
