# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4 — Run the LangGraph Agent End to End
# MAGIC
# MAGIC Invokes the full state graph (`agent/graph.py`) for a given `application_id`: fetches
# MAGIC live data via the Phase 1/2 UC Functions, runs the Phase 3 engine, then runs the
# MAGIC Explanation Pass and Advisory Pass in parallel branches and synthesizes the final
# MAGIC assessment record.
# MAGIC
# MAGIC **Dependency note:** `databricks-langchain` (the documented "LangChain tools" route) pulls
# MAGIC in `openai-agents`, whose dependency tree pip's resolver can't finish on this workspace
# MAGIC (`ResolutionTooDeep`) — confirmed by installing each package individually. This notebook
# MAGIC installs only `langgraph` and calls the model serving endpoint directly via REST instead
# MAGIC (`agent/tools.py:call_llm`) — see LLD Part D.5 for the full writeup.

# COMMAND ----------

# MAGIC %pip install -q langgraph
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "credit_platform", "Target catalog")
dbutils.widgets.text("application_id", "APP-001", "Application to assess")
CATALOG = dbutils.widgets.get("catalog_name")
APPLICATION_ID = dbutils.widgets.get("application_id")

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

import sys, os

# Databricks Repos: os.getcwd() for a notebook is its own directory (.../RAG-Project/pipelines);
# agent/ and engine/ live one level up, at the repo root (same pattern as run_assessment.py).
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(REPO_ROOT)

from agent.graph import build_graph

# COMMAND ----------

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WORKSPACE_URL = ctx.apiUrl().get()
TOKEN = ctx.apiToken().get()
PROMPTS_DIR = os.path.join(REPO_ROOT, "agent", "prompts")

graph = build_graph(spark, CATALOG, WORKSPACE_URL, TOKEN, PROMPTS_DIR)

# COMMAND ----------

# MAGIC %md ## Run the graph

# COMMAND ----------

final_state = graph.invoke({"application_id": APPLICATION_ID})

print("validation_errors:", final_state.get("validation_errors"))
print("explanation attempts:", final_state.get("_explanation_attempts"))
print()
print("assessment_outcome:", final_state["final_assessment"]["assessment_outcome"])
print()
print("narrative:\n", final_state["final_assessment"]["narrative"])
print()
print("ai_advisory_notes:\n", final_state["final_assessment"]["ai_advisory_notes"])

# COMMAND ----------

# MAGIC %md ## Persist to ops.assessment_explanations (append-only)

# COMMAND ----------

import json
from datetime import datetime, timezone
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.ops.assessment_explanations (
        application_id STRING,
        assessment_outcome STRING,
        final_assessment_json STRING,
        validation_errors_json STRING,
        explanation_attempts INT,
        assessed_at TIMESTAMP
    ) USING DELTA
""")

explanation_schema = StructType([
    StructField("application_id", StringType()),
    StructField("assessment_outcome", StringType()),
    StructField("final_assessment_json", StringType()),
    StructField("validation_errors_json", StringType()),
    StructField("explanation_attempts", IntegerType()),
    StructField("assessed_at", TimestampType()),
])

row = (
    APPLICATION_ID,
    final_state["final_assessment"]["assessment_outcome"],
    json.dumps(final_state["final_assessment"], default=str),
    json.dumps(final_state.get("validation_errors", [])),
    int(final_state.get("_explanation_attempts", 0)),
    datetime.now(timezone.utc),
)

spark.createDataFrame([row], schema=explanation_schema) \
    .write.mode("append").saveAsTable(f"{CATALOG}.ops.assessment_explanations")

# COMMAND ----------

# MAGIC %md ## Exit criterion checks
# MAGIC
# MAGIC 1. Assessment Outcome is byte-identical to the deterministic engine's own status,
# MAGIC    regardless of anything either LLM branch wrote.
# MAGIC 2. The Advisory Pass never emits an eligibility verdict.
# MAGIC 3. The Explanation Pass cites no unverified numbers (validate_response would otherwise
# MAGIC    have looped back up to MAX_EXPLANATION_ATTEMPTS times).

# COMMAND ----------

assert final_state["final_assessment"]["assessment_outcome"] == final_state["engine_result"]["eligibility_status"]

from agent.nodes import _advisory_verdict_guard
assert _advisory_verdict_guard(final_state["final_assessment"]["ai_advisory_notes"]) == [], \
    "Advisory Pass emitted an eligibility-status token"

if final_state.get("_explanation_attempts", 0) >= 3 and final_state.get("validation_errors"):
    print(f"WARNING: exhausted retries with unresolved validation errors: {final_state['validation_errors']}")
else:
    print(f"Phase 4 exit criterion met on catalog '{CATALOG}' for {APPLICATION_ID}: "
          f"Assessment Outcome matches RESULT exactly; Advisory Pass carries no eligibility verdict.")
