# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 — Policy & Memo Ingestion
# MAGIC
# MAGIC Parses the PDFs `data_generation/02_generate_policy_documents.py` uploaded, using
# MAGIC `ai_parse_document`, chunks the parsed text (one chunk per parsed element — title/paragraph
# MAGIC — which maps naturally onto our short synthetic documents), and writes `gold.policy_chunks`
# MAGIC and `gold.committee_memos`. Then creates/syncs the Vector Search Delta Sync index over
# MAGIC `gold.policy_chunks`.
# MAGIC
# MAGIC **Implementation note:** Spark's `binaryFile` datasource / `read_files` table-valued
# MAGIC function cannot see Unity Catalog Volume files on this workspace's serverless compute
# MAGIC (verified: `spark.read.format("binaryFile").load("/Volumes/...")` returns 0 rows even though
# MAGIC the file exists). Plain Python `open(path, "rb")` on the same `/Volumes/...` path works fine,
# MAGIC so this notebook reads file bytes that way and hands them to `ai_parse_document` via a
# MAGIC small in-memory DataFrame per file, instead of bulk-loading the volume with `binaryFile`.
# MAGIC
# MAGIC Depends on `data_generation/02_generate_policy_documents.py` having already run.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "credit_platform", "Target catalog")
dbutils.widgets.text("vector_search_endpoint_name", "credit_platform_dev", "Vector Search endpoint")
CATALOG = dbutils.widgets.get("catalog_name")
VS_ENDPOINT = dbutils.widgets.get("vector_search_endpoint_name")

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

import json
from pyspark.sql.functions import expr


def parse_pdf_text_elements(path):
    """Reads a PDF from a Volume path and returns its parsed (type, content) elements via ai_parse_document."""
    with open(path, "rb") as f:
        content = f.read()
    df = spark.createDataFrame([(content,)], ["content"])
    parsed_json_str = df.select(expr("ai_parse_document(content) as parsed")).collect()[0]["parsed"]
    parsed = json.loads(str(parsed_json_str))
    elements = parsed.get("document", {}).get("elements", [])
    return [(el["id"], el["type"], el["content"]) for el in elements if el.get("type") in ("title", "text")]

# COMMAND ----------

# MAGIC %md ## Policy documents → gold.policy_chunks

# COMMAND ----------

policy_docs = spark.table(f"{CATALOG}.bronze.raw_policy_documents").collect()

policy_chunk_rows = []
for doc in policy_docs:
    path = f"/Volumes/{CATALOG}/policy/policy_pdfs/{doc['filename']}"
    elements = parse_pdf_text_elements(path)
    for element_id, _element_type, content in elements:
        policy_chunk_rows.append(dict(
            chunk_id=f"{doc['document_id']}-{element_id}",
            document_id=doc["document_id"],
            version=doc["version"],
            chunk_text=content,
            effective_date=doc["effective_date"],
            supersedes=doc["supersedes"],
            applicable_sector=doc["applicable_sector"],
        ))

print(f"Parsed {len(policy_chunk_rows)} chunks from {len(policy_docs)} policy documents")

# COMMAND ----------

from pyspark.sql.types import DateType, StringType, StructField, StructType

policy_chunk_schema = StructType([
    StructField("chunk_id", StringType()), StructField("document_id", StringType()),
    StructField("version", StringType()), StructField("chunk_text", StringType()),
    StructField("effective_date", DateType()), StructField("supersedes", StringType()),
    StructField("applicable_sector", StringType()),
])

policy_chunks_df = spark.createDataFrame(
    [(r["chunk_id"], r["document_id"], r["version"], r["chunk_text"], r["effective_date"], r["supersedes"], r["applicable_sector"])
     for r in policy_chunk_rows],
    schema=policy_chunk_schema,
)
policy_chunks_df.createOrReplaceTempView("_policy_chunks_staging")

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.gold.policy_chunks
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
    AS SELECT * FROM _policy_chunks_staging
""")

display(spark.table(f"{CATALOG}.gold.policy_chunks"))

# COMMAND ----------

# MAGIC %md ## Committee memos → gold.committee_memos (no chunking, no vector index — Advisory Pass reads directly)

# COMMAND ----------

memo_manifest = spark.table(f"{CATALOG}.bronze.raw_committee_memo_manifest").collect()

memo_rows = []
for memo in memo_manifest:
    path = f"/Volumes/{CATALOG}/policy/committee_memos/{memo['filename']}"
    elements = parse_pdf_text_elements(path)
    memo_text = "\n".join(content for _id, _type, content in elements)
    memo_rows.append(dict(
        memo_id=memo["memo_id"], applicable_sector=memo["applicable_sector"],
        borrower_id=memo["borrower_id"], memo_date=memo["memo_date"],
        memo_text=memo_text, source_document_id=memo["memo_id"],
    ))

memo_schema = StructType([
    StructField("memo_id", StringType()), StructField("applicable_sector", StringType()),
    StructField("borrower_id", StringType()), StructField("memo_date", DateType()),
    StructField("memo_text", StringType()), StructField("source_document_id", StringType()),
])
spark.createDataFrame(
    [(r["memo_id"], r["applicable_sector"], r["borrower_id"], r["memo_date"], r["memo_text"], r["source_document_id"])
     for r in memo_rows],
    schema=memo_schema,
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.committee_memos")

display(spark.table(f"{CATALOG}.gold.committee_memos"))

# COMMAND ----------

# MAGIC %md ## Create/sync the Vector Search Delta Sync index over gold.policy_chunks

# COMMAND ----------

import requests

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
workspace_url = ctx.apiUrl().get()
token = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}"}

INDEX_NAME = f"{CATALOG}.gold.policy_chunks_index"
SOURCE_TABLE = f"{CATALOG}.gold.policy_chunks"

get_resp = requests.get(f"{workspace_url}/api/2.0/vector-search/indexes/{INDEX_NAME}", headers=headers)

if get_resp.status_code == 404:
    create_body = {
        "name": INDEX_NAME,
        "endpoint_name": VS_ENDPOINT,
        "primary_key": "chunk_id",
        "index_type": "DELTA_SYNC",
        "delta_sync_index_spec": {
            "source_table": SOURCE_TABLE,
            "pipeline_type": "TRIGGERED",
            "embedding_source_columns": [
                {"name": "chunk_text", "embedding_model_endpoint_name": "databricks-gte-large-en"}
            ],
        },
    }
    create_resp = requests.post(f"{workspace_url}/api/2.0/vector-search/indexes", headers=headers, json=create_body)
    create_resp.raise_for_status()
    print(f"Created Vector Search index {INDEX_NAME} on endpoint {VS_ENDPOINT}")
else:
    get_resp.raise_for_status()
    sync_resp = requests.post(f"{workspace_url}/api/2.0/vector-search/indexes/{INDEX_NAME}/sync", headers=headers)
    sync_resp.raise_for_status()
    print(f"Triggered sync for existing Vector Search index {INDEX_NAME}")

# COMMAND ----------

assert len(policy_chunk_rows) >= len(policy_docs), "Expected at least one chunk per policy document"
assert spark.table(f"{CATALOG}.gold.committee_memos").count() == len(memo_manifest)
print(f"Phase 2 ingestion complete on catalog '{CATALOG}': {len(policy_chunk_rows)} policy chunks, {len(memo_manifest)} committee memos.")
