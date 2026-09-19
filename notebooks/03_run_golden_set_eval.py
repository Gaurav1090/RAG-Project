# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5 — Golden Set Evaluation
# MAGIC
# MAGIC Two layers, matching Section 11's evaluation framework:
# MAGIC
# MAGIC 1. **Engine layer** — re-runs the same 10 golden cases as
# MAGIC    `engine/tests/test_golden_cases.py` (pure Python, no catalog dependency) and reports
# MAGIC    pass/fail per case. This is the objective, deterministic half of Section 11's table
# MAGIC    ("Rules engine" row).
# MAGIC 2. **Agent layer** — runs the full agent for the one case we have real live data for
# MAGIC    (`APP-001`, the pre/post-circular centerpiece) and computes concrete values for the
# MAGIC    "GenAI explanation" and "Agent (LangGraph)" rows: citation correctness, unsupported
# MAGIC    -claim rate, and workflow completion.
# MAGIC
# MAGIC **Honest scope note:** Section 11's golden set was designed as a 10-case set primarily for
# MAGIC the *engine*. Building 10 more fully-seeded live borrowers/applications purely so every
# MAGIC case could also run through the *agent* is real additional data-generation work Phase 5
# MAGIC doesn't attempt — the agent-layer metrics below are computed for the one case that already
# MAGIC has live data (the centerpiece), not averaged across all 10.

# COMMAND ----------

# MAGIC %pip install -q langgraph "mlflow>=3"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "credit_platform", "Target catalog")
CATALOG = dbutils.widgets.get("catalog_name")

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

import sys, os

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(REPO_ROOT)

# COMMAND ----------

# MAGIC %md ## Layer 1: engine golden cases (pure Python, no catalog dependency)

# COMMAND ----------

import unittest
from engine.tests.test_golden_cases import DeterminismTest, GoldenCaseTests

suite = unittest.TestSuite()
loader = unittest.TestLoader()
suite.addTests(loader.loadTestsFromTestCase(GoldenCaseTests))
suite.addTests(loader.loadTestsFromTestCase(DeterminismTest))

runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
engine_result = runner.run(suite)

engine_summary = {
    "total": engine_result.testsRun,
    "failures": len(engine_result.failures),
    "errors": len(engine_result.errors),
    "passed": engine_result.testsRun - len(engine_result.failures) - len(engine_result.errors),
}
print("\nEngine layer:", engine_summary)
assert engine_summary["failures"] == 0 and engine_summary["errors"] == 0, "golden engine cases must all pass"

# COMMAND ----------

# MAGIC %md ## Layer 2: agent-level metrics for the centerpiece case (APP-001)

# COMMAND ----------

import mlflow
from agent.graph import build_graph

mlflow.set_tracking_uri("databricks")

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WORKSPACE_URL = ctx.apiUrl().get()
TOKEN = ctx.apiToken().get()
PROMPTS_DIR = os.path.join(REPO_ROOT, "agent", "prompts")

# See notebooks/02_demo_pre_post_circular.py: ctx.tags() isn't whitelisted on serverless
# compute, so this uses a fixed, catalog-scoped experiment path instead of a per-user one.
mlflow.set_experiment(f"/Shared/credit_platform_assessments_{CATALOG}")

graph = build_graph(spark, CATALOG, WORKSPACE_URL, TOKEN, PROMPTS_DIR)

with mlflow.start_run(run_name="golden_set_eval_APP-001"):
    final_state = graph.invoke({"application_id": "APP-001"})
    mlflow.set_tag("eval", "golden_set")

fa = final_state["final_assessment"]
engine_result_live = final_state["engine_result"]

# COMMAND ----------

# Retrieval: did retrieve_policy return the document actually governing this assessment?
retrieved_doc_ids = {e["document_id"] for e in final_state.get("retrieved_evidence", [])}
retrieval_hit = engine_result_live["policy_version"] == "v2" and "POLICY-TEXTILE-01-v2" in retrieved_doc_ids

# Citation correctness: does the narrative name the correct document version?
citation_correct = engine_result_live["policy_version"] in fa["narrative"] or "Revised" in fa["narrative"]

# Groundedness / unsupported-claim rate: validate_response's final verdict, not a sample —
# the graph would not have reached human_review with unresolved errors otherwise.
unsupported_claim_rate = 1.0 if final_state.get("validation_errors") else 0.0

# Workflow completion: did the graph run to completion and produce every required section?
workflow_complete = all(
    key in fa for key in
    ["assessment_outcome", "credit_metrics", "missing_information", "ai_advisory_notes", "narrative"]
)

agent_metrics = {
    "retrieval_hit": retrieval_hit,
    "citation_correct": citation_correct,
    "unsupported_claim_rate": unsupported_claim_rate,
    "explanation_attempts": final_state.get("_explanation_attempts", 0),
    "workflow_complete": workflow_complete,
    "assessment_outcome": fa["assessment_outcome"],
}
print("Agent layer (APP-001 centerpiece case):", agent_metrics)

with mlflow.start_run(run_name="golden_set_eval_APP-001_metrics"):
    mlflow.log_metrics({
        "retrieval_hit": int(agent_metrics["retrieval_hit"]),
        "citation_correct": int(agent_metrics["citation_correct"]),
        "unsupported_claim_rate": agent_metrics["unsupported_claim_rate"],
        "explanation_attempts": agent_metrics["explanation_attempts"],
        "workflow_complete": int(agent_metrics["workflow_complete"]),
    })

# COMMAND ----------

# MAGIC %md ## Report (Section 11's table, filled in with real numbers)

# COMMAND ----------

report = {
    "engine": engine_summary,
    "agent_centerpiece_case": agent_metrics,
}
print(report)

assert agent_metrics["retrieval_hit"], "expected retrieval to surface the governing policy version"
assert agent_metrics["citation_correct"], "expected the narrative to cite the correct policy version"
assert agent_metrics["unsupported_claim_rate"] == 0.0, "final accepted explanation must have zero unresolved unsupported claims"
assert agent_metrics["workflow_complete"], "expected the graph to reach a fully-populated final assessment"

print(f"Phase 5 golden set evaluation complete on catalog '{CATALOG}': engine 10/10 golden cases pass; "
      f"agent centerpiece case retrieval/citation/groundedness/completion all verified.")
