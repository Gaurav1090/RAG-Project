# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 0 — Workspace & Repo Bootstrap: Setup + Smoke Test
# MAGIC
# MAGIC Run this once the `credit_platform` catalog exists (created via the Catalog Explorer UI,
# MAGIC since Databricks Free Edition's Default Storage catalogs can't be created via API/CLI).
# MAGIC
# MAGIC This notebook creates the five schemas, two policy volumes, and proves read/write access
# MAGIC with a throwaway Delta table — the Phase 0 exit criterion from `Phased Implementation Plan & LLD.md`.

# COMMAND ----------

CATALOG = "credit_platform"

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

for schema in ["bronze", "silver", "gold", "policy", "ops"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.policy.policy_pdfs
    COMMENT 'Source policy PDFs / RBI circulars for ai_parse_document ingestion'
""")
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.policy.committee_memos
    COMMENT 'Source credit-committee memo PDFs for the Advisory Pass corpus'
""")

display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.policy"))

# COMMAND ----------

# MAGIC %md ## Smoke test: write + read a throwaway Delta table

# COMMAND ----------

test_df = spark.createDataFrame(
    [(1, "hello world"), (2, "phase 0 smoke test")],
    ["id", "message"],
)
test_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.bronze._phase0_smoke_test")

result = spark.table(f"{CATALOG}.bronze._phase0_smoke_test")
display(result)

assert result.count() == 2, "Smoke test failed: expected 2 rows"
print("Phase 0 exit criterion met: catalog, schemas, volumes, and Delta read/write all working.")

# COMMAND ----------

# Cleanup the throwaway table — schemas and volumes stay for Phase 1+
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.bronze._phase0_smoke_test")
