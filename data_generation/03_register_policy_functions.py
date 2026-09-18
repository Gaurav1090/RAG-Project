# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 — Register retrieve_policy and get_committee_memos
# MAGIC
# MAGIC `retrieve_policy` calls the `VECTOR_SEARCH` SQL table function against
# MAGIC `gold.policy_chunks_index`, then filters candidates down to the sector and to only
# MAGIC currently-effective (non-superseded) document versions as of `as_of_date`.
# MAGIC
# MAGIC **Implementation note:** `VECTOR_SEARCH` on this workspace only accepts `index`, `query` /
# MAGIC `query_text`, and `num_results` — there is no `filters` parameter (confirmed by calling it
# MAGIC directly: passing `filters` raises `UNRECOGNIZED_PARAMETER_NAME`). So sector/effective-date/
# MAGIC supersedes filtering happens in the wrapping SQL, over a wide `num_results` candidate set,
# MAGIC rather than being pushed into the vector search call itself.
# MAGIC
# MAGIC Depends on `pipelines/policy_ingestion.py` having already run and the Vector Search index
# MAGIC being `ready` (check with `databricks vector-search-indexes get-index` before running this).

# COMMAND ----------

dbutils.widgets.text("catalog_name", "credit_platform", "Target catalog")
CATALOG = dbutils.widgets.get("catalog_name")

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md ## retrieve_policy

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.gold.retrieve_policy(
    p_query STRING,
    p_applicable_sector STRING,
    p_as_of_date DATE
)
RETURNS ARRAY<STRUCT<
    chunk_id: STRING, document_id: STRING, version: STRING, chunk_text: STRING, effective_date: DATE
>>
COMMENT 'Semantic retrieval over gold.policy_chunks, filtered to the sector and to only currently-effective (non-superseded) document versions as of as_of_date.'
RETURN (
    WITH candidates AS (
        SELECT chunk_id, document_id, version, chunk_text, effective_date, supersedes, applicable_sector
        FROM VECTOR_SEARCH(
            index => '{CATALOG}.gold.policy_chunks_index',
            query_text => p_query,
            num_results => 20
        )
        WHERE applicable_sector = p_applicable_sector
          AND effective_date <= p_as_of_date
    ),
    valid_documents AS (
        SELECT DISTINCT c.document_id
        FROM candidates c
        WHERE NOT EXISTS (
            SELECT 1 FROM candidates c2 WHERE c2.supersedes = c.document_id
        )
    )
    SELECT collect_list(named_struct(
        'chunk_id', chunk_id, 'document_id', document_id, 'version', version,
        'chunk_text', chunk_text, 'effective_date', effective_date
    ))
    FROM (
        SELECT c.chunk_id, c.document_id, c.version, c.chunk_text, c.effective_date
        FROM candidates c
        JOIN valid_documents vd ON c.document_id = vd.document_id
        LIMIT 5
    )
)
""")

# COMMAND ----------

# MAGIC %md ## get_committee_memos

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.gold.get_committee_memos(
    p_applicable_sector STRING,
    p_borrower_id STRING DEFAULT NULL
)
RETURNS ARRAY<STRUCT<
    memo_id: STRING, applicable_sector: STRING, borrower_id: STRING, memo_date: DATE, memo_text: STRING
>>
COMMENT 'Committee memos for a sector (and optionally a specific borrower), most recent first. Advisory Pass only — never called by the Explanation Pass or the rules engine.'
RETURN (
    SELECT array_sort(
        collect_list(named_struct(
            'memo_id', memo_id, 'applicable_sector', applicable_sector, 'borrower_id', borrower_id,
            'memo_date', memo_date, 'memo_text', memo_text
        )),
        (a, b) -> CASE WHEN a.memo_date > b.memo_date THEN -1
                       WHEN a.memo_date < b.memo_date THEN 1
                       ELSE 0 END
    )
    FROM {CATALOG}.gold.committee_memos
    WHERE applicable_sector = p_applicable_sector
      AND (p_borrower_id IS NULL OR borrower_id = p_borrower_id OR borrower_id IS NULL)
)
""")

# COMMAND ----------

# MAGIC %md ## Smoke tests — Phase 2 exit criterion: version-switching across the circular date

# COMMAND ----------

before = spark.sql(f"""
    SELECT {CATALOG}.gold.retrieve_policy('DSCR threshold textile', 'textile_msme', DATE'2026-08-01') AS r
""").collect()[0]["r"]
before_docs = {row["document_id"] for row in before}
assert "POLICY-TEXTILE-01" in before_docs, f"Expected v1 in results before the circular, got {before_docs}"
assert "POLICY-TEXTILE-01-v2" not in before_docs, f"v2 leaked into a pre-circular query: {before_docs}"
print("retrieve_policy (before circular) OK — returned only v1:", before_docs)

after = spark.sql(f"""
    SELECT {CATALOG}.gold.retrieve_policy('DSCR threshold textile', 'textile_msme', DATE'2026-09-15') AS r
""").collect()[0]["r"]
after_docs = {row["document_id"] for row in after}
assert "POLICY-TEXTILE-01-v2" in after_docs, f"Expected v2 in results after the circular, got {after_docs}"
assert "POLICY-TEXTILE-01" not in after_docs, f"Superseded v1 leaked into a post-circular query: {after_docs}"
print("retrieve_policy (after circular) OK — returned only v2:", after_docs)

memos_before = spark.sql(f"""
    SELECT {CATALOG}.gold.get_committee_memos('textile_msme', 'MT-2026-0142') AS r
""").collect()[0]["r"]
assert len(memos_before) >= 1
print("get_committee_memos OK:", [(m["memo_id"], str(m["memo_date"])) for m in memos_before])

print(f"Phase 2 exit criterion met on catalog '{CATALOG}': retrieve_policy version-switches correctly; get_committee_memos returns seeded memos.")
