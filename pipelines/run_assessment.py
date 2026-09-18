# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3 — Run the Deterministic Rules Engine Against Live Catalog Data
# MAGIC
# MAGIC Wires the pure-Python engine in `engine/` (unit-tested standalone in
# MAGIC `engine/tests/test_golden_cases.py`, no Databricks dependency) to real
# MAGIC `gold.applications` / `gold.borrower_360` / `gold.policy_rules` data for a given
# MAGIC `application_id`, and writes the result to `ops.assessment_results`.
# MAGIC
# MAGIC This is the "installable engine producing the exact JSON output shape" deliverable —
# MAGIC the engine logic itself was already proven correct locally; this notebook proves it also
# MAGIC works end-to-end against the live catalog.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "credit_platform", "Target catalog")
dbutils.widgets.text("application_id", "APP-001", "Application to assess")
CATALOG = dbutils.widgets.get("catalog_name")
APPLICATION_ID = dbutils.widgets.get("application_id")

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

import sys, os

# Databricks Repos: os.getcwd() for a notebook is its own directory (.../RAG-Project/pipelines);
# the engine/ package lives one level up, at the repo root.
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..")))

from engine.metric_calculator import compute_metrics
from engine.policy_evaluator import evaluate_rules
from engine.result_builder import build_result
from engine.rule_loader import resolve_headline_version, select_applicable_rules

# COMMAND ----------

# MAGIC %md ## Fetch the application, borrower, and applicable policy rules

# COMMAND ----------

app_row = spark.sql(f"""
    SELECT application_id, borrower_id, product, assessment_date, requested_loan_amount
    FROM {CATALOG}.gold.applications
    WHERE application_id = '{APPLICATION_ID}'
""").collect()[0]

application = dict(
    application_id=app_row["application_id"],
    requested_loan_amount=app_row["requested_loan_amount"],
)
borrower_id = app_row["borrower_id"]
as_of_date = app_row["assessment_date"]

borrower_struct = spark.sql(f"""
    SELECT {CATALOG}.gold.get_borrower_financials('{borrower_id}') AS r
""").collect()[0]["r"]
borrower = borrower_struct.asDict()

repeat_offender_struct = spark.sql(f"""
    SELECT {CATALOG}.gold.check_repeat_offender_signals('{borrower_id}') AS r
""").collect()[0]["r"]
repeat_offender = repeat_offender_struct.asDict()

applicable_sector = borrower["sector"]

rule_rows = spark.sql(f"""
    SELECT pr.rule_uid, pr.rule_id, pr.metric, pr.operator, pr.threshold,
           pr.applicable_sector, pr.effective_date, pr.supersedes,
           pr.source_document_id, pr.approval_status, doc.version
    FROM {CATALOG}.gold.policy_rules pr
    JOIN {CATALOG}.bronze.raw_policy_documents doc ON pr.source_document_id = doc.document_id
""").collect()
all_rules = [r.asDict() for r in rule_rows]

print(f"application={application}, borrower_id={borrower_id}, as_of_date={as_of_date}, sector={applicable_sector}")

# COMMAND ----------

# MAGIC %md ## Run the engine

# COMMAND ----------

extra_missing = []
if borrower.get("cibil_score") is None:
    extra_missing.append("External bureau report")

applicable_rules = select_applicable_rules(all_rules, applicable_sector, as_of_date)
metrics = compute_metrics(application, borrower)
rule_results = evaluate_rules(metrics, applicable_rules)
policy_version = resolve_headline_version(applicable_rules, applicable_sector)

result = build_result(
    application, rule_results, policy_version,
    missing_fields=extra_missing,
    hard_block=bool(repeat_offender.get("wilful_defaulter_flag")),
)

print(result)

# COMMAND ----------

# MAGIC %md ## Persist to ops.assessment_results (append-only — never overwrite a past assessment)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.ops.assessment_results (
        application_id STRING,
        policy_version STRING,
        eligibility_status STRING,
        rule_results ARRAY<STRUCT<
            rule_id: STRING, metric: STRING, operator: STRING, status: STRING,
            actual_value: DOUBLE, required_value: DOUBLE
        >>,
        exceptions ARRAY<STRING>,
        missing_information ARRAY<STRING>,
        assessed_at TIMESTAMP
    ) USING DELTA
""")

from datetime import datetime, timezone
from pyspark.sql import Row
from pyspark.sql.types import (
    ArrayType, DoubleType, StringType, StructField, StructType, TimestampType,
)

# Explicit schema — an empty exceptions/missing_information list on a single-row DataFrame
# gives Spark nothing to infer the array element type from (CANNOT_DETERMINE_TYPE), so we
# don't rely on inference here at all.
assessment_result_schema = StructType([
    StructField("application_id", StringType()),
    StructField("policy_version", StringType()),
    StructField("eligibility_status", StringType()),
    StructField("rule_results", ArrayType(StructType([
        StructField("rule_id", StringType()), StructField("metric", StringType()),
        StructField("operator", StringType()), StructField("status", StringType()),
        StructField("actual_value", DoubleType()), StructField("required_value", DoubleType()),
    ]))),
    StructField("exceptions", ArrayType(StringType())),
    StructField("missing_information", ArrayType(StringType())),
    StructField("assessed_at", TimestampType()),
])

rule_results_typed = [
    Row(
        rule_id=r["rule_id"], metric=r["metric"], operator=r["operator"], status=r["status"],
        actual_value=float(r["actual_value"]) if r["actual_value"] is not None else None,
        required_value=float(r["required_value"]) if r["required_value"] is not None else None,
    )
    for r in result["rule_results"]
]

result_row = (
    result["application_id"], result["policy_version"], result["eligibility_status"],
    rule_results_typed, result["exceptions"], result["missing_information"],
    datetime.now(timezone.utc),
)

spark.createDataFrame([result_row], schema=assessment_result_schema) \
    .write.mode("append").saveAsTable(f"{CATALOG}.ops.assessment_results")

display(spark.sql(f"""
    SELECT * FROM {CATALOG}.ops.assessment_results
    WHERE application_id = '{APPLICATION_ID}'
    ORDER BY assessed_at DESC LIMIT 1
"""))

# COMMAND ----------

# MAGIC %md ## Exit criterion: for APP-001 (assessed 2026-09-18, post-circular), match Section 7 exactly

# COMMAND ----------

if APPLICATION_ID == "APP-001":
    assert result["policy_version"] == "v2", result
    assert result["eligibility_status"] == "manual_review", result
    dscr_result = next(r for r in result["rule_results"] if r["rule_id"] == "DSCR-001")
    assert dscr_result["status"] == "fail"
    assert dscr_result["actual_value"] == 1.15
    assert dscr_result["required_value"] == 1.25
    print(f"Phase 3 exit criterion met on catalog '{CATALOG}': APP-001 matches Section 7 exactly.")
else:
    print(f"Ran assessment for {APPLICATION_ID} on catalog '{CATALOG}': {result['eligibility_status']}")
