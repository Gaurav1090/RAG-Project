# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1 — Register UC Functions
# MAGIC
# MAGIC Registers the 4 Phase-1 Unity Catalog Functions (LLD Part D.3) in `gold`, the schema the
# MAGIC deployment diagram (Part A) shows as the source for all data-layer UC Functions. Each is a
# MAGIC SQL scalar function so it's callable identically from SQL, from the Phase 3 rules engine,
# MAGIC and as a LangChain tool in the Phase 4 agent — one implementation, multiple callers.
# MAGIC
# MAGIC `retrieve_policy` and `get_committee_memos` are registered in Phase 2, once the policy and
# MAGIC memo corpora exist.
# MAGIC
# MAGIC Depends on `00_generate_dummy_data.py` having already run against this catalog.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "credit_platform", "Target catalog")
CATALOG = dbutils.widgets.get("catalog_name")

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md ## get_borrower_financials
# MAGIC
# MAGIC The `p_owner_ids` parameter exists for interface stability with the LLD's documented
# MAGIC signature (Section 8.4) but isn't used in the returned struct — promoter detail comes from
# MAGIC a separate `get_promoter_financials(borrower_360.owner_ids)` call, so the two functions
# MAGIC don't duplicate each other's data.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.gold.get_borrower_financials(
    p_borrower_id STRING,
    p_owner_ids ARRAY<STRING> DEFAULT NULL
)
RETURNS STRUCT<
    borrower_id: STRING, legal_name: STRING, sector: STRING, annual_turnover: DOUBLE,
    dscr: DOUBLE, days_past_due: INT, existing_exposure: DOUBLE, collateral_value: DOUBLE,
    cibil_score: INT, crilc_flag: BOOLEAN, wilful_defaulter_flag: BOOLEAN,
    no_internal_history: BOOLEAN, owner_ids: ARRAY<STRING>, data_as_of: DATE
>
COMMENT 'Returns the borrower_360 record for a given borrower_id. Pass the returned owner_ids into get_promoter_financials for promoter-risk checks (Section 8.4).'
RETURN (
    SELECT first(named_struct(
        'borrower_id', borrower_id, 'legal_name', legal_name, 'sector', sector,
        'annual_turnover', annual_turnover, 'dscr', dscr, 'days_past_due', days_past_due,
        'existing_exposure', existing_exposure, 'collateral_value', collateral_value,
        'cibil_score', cibil_score, 'crilc_flag', crilc_flag,
        'wilful_defaulter_flag', wilful_defaulter_flag, 'no_internal_history', no_internal_history,
        'owner_ids', owner_ids, 'data_as_of', data_as_of
    ), true)
    FROM {CATALOG}.gold.borrower_360
    WHERE borrower_id = p_borrower_id
)
""")

# COMMAND ----------

# MAGIC %md ## get_cibil_report

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.gold.get_cibil_report(p_pan STRING)
RETURNS STRUCT<
    pan: STRING, borrower_id: STRING, cibil_score: INT, active_accounts: INT,
    delinquency_flag: BOOLEAN, enquiry_count: INT, report_date: DATE
>
COMMENT 'Wraps the bureau lookup for a given PAN (company or individual). Section 8.5: treated as a required input for every assessment, not optional.'
RETURN (
    SELECT first(named_struct(
        'pan', pan, 'borrower_id', borrower_id, 'cibil_score', cibil_score,
        'active_accounts', active_accounts, 'delinquency_flag', delinquency_flag,
        'enquiry_count', enquiry_count, 'report_date', report_date
    ), true)
    FROM {CATALOG}.silver.cibil_reports
    WHERE pan = p_pan
)
""")

# COMMAND ----------

# MAGIC %md ## check_repeat_offender_signals
# MAGIC
# MAGIC Combines external CRILC/wilful-defaulter signal with internal cross-product history
# MAGIC (Section 8.2) — a hit here weighs heavily in Key Risk Factors regardless of how clean the
# MAGIC current application looks.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.gold.check_repeat_offender_signals(p_borrower_id STRING)
RETURNS STRUCT<
    crilc_flag: BOOLEAN, wilful_defaulter_flag: BOOLEAN, cross_bank_exposure: DOUBLE,
    internal_missed_payment_flag: BOOLEAN, internal_product_count: INT
>
COMMENT 'External (CRILC + wilful defaulter list) and internal (cross-product history) repeat-offender signals for a borrower.'
RETURN (
    SELECT named_struct(
        'crilc_flag', COALESCE(MAX(c.crilc_flag), false),
        'wilful_defaulter_flag', COALESCE(MAX(c.wilful_defaulter_flag), false),
        'cross_bank_exposure', COALESCE(MAX(c.cross_bank_exposure), 0.0),
        'internal_missed_payment_flag', COALESCE(MAX(h.missed_payment_flag), false),
        'internal_product_count', COUNT(h.product_type)
    )
    FROM {CATALOG}.silver.crilc_records c
    LEFT JOIN {CATALOG}.silver.internal_product_history h ON h.borrower_id = c.borrower_id
    WHERE c.borrower_id = p_borrower_id
)
""")

# COMMAND ----------

# MAGIC %md ## get_promoter_financials

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.gold.get_promoter_financials(p_owner_ids ARRAY<STRING>)
RETURNS ARRAY<STRUCT<
    owner_id: STRING, borrower_id: STRING, name: STRING, individual_cibil_score: INT,
    other_business_interests: STRING, guarantor_flag: BOOLEAN, exposure_elsewhere: DOUBLE
>>
COMMENT 'Promoter/guarantor risk sub-object for a list of owner_ids (Section 8.4): individual CIBIL, other business interests, and guarantor exposure elsewhere.'
RETURN (
    SELECT collect_list(named_struct(
        'owner_id', owner_id, 'borrower_id', borrower_id, 'name', name,
        'individual_cibil_score', individual_cibil_score,
        'other_business_interests', other_business_interests,
        'guarantor_flag', guarantor_flag, 'exposure_elsewhere', exposure_elsewhere
    ))
    FROM {CATALOG}.silver.promoter_records
    WHERE array_contains(p_owner_ids, owner_id)
)
""")

# COMMAND ----------

# MAGIC %md ## Smoke test — call all 4 functions against the reference borrower

# COMMAND ----------

REFERENCE_BORROWER_ID = "MT-2026-0142"

financials = spark.sql(f"SELECT {CATALOG}.gold.get_borrower_financials('{REFERENCE_BORROWER_ID}') AS r").collect()[0]["r"]
assert financials["dscr"] == 1.15, f"get_borrower_financials returned unexpected dscr: {financials['dscr']}"
assert financials["sector"] == "textile_msme"
print("get_borrower_financials OK:", financials)

cibil = spark.sql(f"SELECT {CATALOG}.gold.get_cibil_report('AABCM1234M') AS r").collect()[0]["r"]
assert cibil["borrower_id"] == REFERENCE_BORROWER_ID
print("get_cibil_report OK:", cibil)

repeat_offender = spark.sql(f"SELECT {CATALOG}.gold.check_repeat_offender_signals('{REFERENCE_BORROWER_ID}') AS r").collect()[0]["r"]
assert repeat_offender["crilc_flag"] is False
print("check_repeat_offender_signals OK:", repeat_offender)

promoters = spark.sql(f"SELECT {CATALOG}.gold.get_promoter_financials(array('OWN-MT-2026-0142-1')) AS r").collect()[0]["r"]
assert len(promoters) == 1
print("get_promoter_financials OK:", promoters)

print(f"Phase 1 UC Functions verified on catalog '{CATALOG}'.")
