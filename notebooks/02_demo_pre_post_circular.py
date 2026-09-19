# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5 — The Centerpiece Demo: Pre/Post-Circular Comparison
# MAGIC
# MAGIC Asks the identical question — *"Is Meera Textiles eligible for this loan under current
# MAGIC policy?"* — twice against the same application (`APP-001`), once as of a date before the
# MAGIC RBI textile circular's revision (2026-01-15) and once after (2026-09-18, the application's
# MAGIC own stored assessment date). Section 3's narrative hook, reproduced live.
# MAGIC
# MAGIC Both runs go through the full agent (`agent/graph.py`) — not just the deterministic engine
# MAGIC — so this also proves the Advisory Pass's memo retrieval is correctly point-in-time: the
# MAGIC "before" run must not see the September committee memo about softening export volumes,
# MAGIC since it hadn't been written yet as of January.

# COMMAND ----------

# MAGIC %pip install -q langgraph "mlflow>=3"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "credit_platform", "Target catalog")
CATALOG = dbutils.widgets.get("catalog_name")

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

import sys, os
from datetime import date

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(REPO_ROOT)

import mlflow
from agent.graph import build_graph

mlflow.set_tracking_uri("databricks")

# COMMAND ----------

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WORKSPACE_URL = ctx.apiUrl().get()
TOKEN = ctx.apiToken().get()
PROMPTS_DIR = os.path.join(REPO_ROOT, "agent", "prompts")

# A fixed, catalog-scoped path rather than a per-user one: ctx.tags() (used to look up the
# current user) isn't whitelisted on this workspace's serverless compute
# (Py4JSecurityException), and a shared project-level experiment is the right home for this
# regardless of which principal runs the job.
mlflow.set_experiment(f"/Shared/credit_platform_assessments_{CATALOG}")

graph = build_graph(spark, CATALOG, WORKSPACE_URL, TOKEN, PROMPTS_DIR)

APPLICATION_ID = "APP-001"
BEFORE_DATE = date(2026, 1, 15)   # after v1 (2026-01-01), before v2 (2026-09-01)
AFTER_DATE = date(2026, 9, 18)    # after v2 — the application's own stored assessment_date

# COMMAND ----------

# MAGIC %md ## Run "before" and "after"

# COMMAND ----------

def run_and_log(label, as_of_date_override):
    with mlflow.start_run(run_name=f"demo_{label}_{APPLICATION_ID}"):
        final_state = graph.invoke({
            "application_id": APPLICATION_ID,
            "as_of_date_override": as_of_date_override,
        })
        mlflow.set_tag("phase", label)
        mlflow.set_tag("policy_version", final_state["engine_result"]["policy_version"])
        mlflow.set_tag("eligibility_status", final_state["engine_result"]["eligibility_status"])
        mlflow.log_dict(final_state["final_assessment"], f"final_assessment_{label}.json")
        return final_state

before_state = run_and_log("before", BEFORE_DATE)
after_state = run_and_log("after", AFTER_DATE)

# COMMAND ----------

# MAGIC %md ## Side-by-side comparison

# COMMAND ----------

def summarize(label, state):
    fa = state["final_assessment"]
    print(f"=== {label} ({state['as_of_date']}) ===")
    print("Policy version:", fa["policy_version"])
    print("Outcome:", fa["assessment_outcome"])
    print("Advisory Notes:", fa["ai_advisory_notes"])
    print()

summarize("BEFORE the circular", before_state)
summarize("AFTER the circular", after_state)

# COMMAND ----------

# MAGIC %md ## Exit criterion: reproduces Section 9's exact outcome flip

# COMMAND ----------

assert before_state["final_assessment"]["policy_version"] == "v1"
assert before_state["final_assessment"]["assessment_outcome"] == "eligible"

assert after_state["final_assessment"]["policy_version"] == "v2"
assert after_state["final_assessment"]["assessment_outcome"] == "manual_review"

before_memo_ids = {m["memo_id"] for m in before_state["committee_memos"]}
after_memo_ids = {m["memo_id"] for m in after_state["committee_memos"]}
assert "MEMO-003" not in before_memo_ids, "post-circular memo leaked into the pre-circular run"
assert "MEMO-003" in after_memo_ids, "post-circular memo missing from the post-circular run"

print(
    f"Phase 5 demo exit criterion met on catalog '{CATALOG}': outcome flips eligible -> manual_review "
    f"as policy_version flips v1 -> v2, and the Advisory Pass only sees the September memo in the "
    f"'after' run — reproducing Section 3's narrative hook end to end through the full agent."
)
