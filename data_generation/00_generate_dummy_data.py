# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1 — Synthetic Data Layer: Generate Dummy Data
# MAGIC
# MAGIC Generates ~200 synthetic SME borrowers (bronze), validates/types them (silver), and
# MAGIC builds `gold.borrower_360` + a seeded `gold.applications` row for the reference scenario.
# MAGIC Fully synthetic — no real borrower, bureau, or regulatory data anywhere in this pipeline.
# MAGIC
# MAGIC Re-runnable: every write is `overwrite`/`CREATE OR REPLACE`, and generation uses a fixed
# MAGIC random seed, so dev and prod end up with an identical synthetic pool and re-running never
# MAGIC produces a different dataset.
# MAGIC
# MAGIC Catalog is a parameter (`catalog_name` widget) so this notebook runs unchanged against
# MAGIC `credit_platform` (dev) and `credit_platform_prod` (prod) — see `resources/jobs.yml`.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "credit_platform", "Target catalog")
CATALOG = dbutils.widgets.get("catalog_name")

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

import random
from datetime import date, timedelta

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

random.seed(42)  # reproducible pool across dev/prod and repeated runs

TODAY = date(2026, 9, 18)
REFERENCE_BORROWER_ID = "MT-2026-0142"
N_RANDOM_BORROWERS = 199  # + 1 fixed reference borrower = 200 total

SECTORS = [
    "textile_msme", "manufacturing_sme", "trading_sme",
    "services_sme", "agri_processing_sme", "pharma_sme",
]
CITIES = ["Surat", "Ahmedabad", "Mumbai", "Delhi", "Bengaluru", "Chennai", "Pune", "Jaipur"]
NAME_PREFIXES = ["Suraj", "Vikas", "Shree", "Ganga", "Bharat", "Sunrise", "Metro", "National", "United", "Prime"]
NAME_SUFFIXES = ["Textiles", "Exports", "Traders", "Industries", "Enterprises", "Agro", "Pharma", "Fabrics", "Logistics", "Chem"]
LEGAL_SUFFIXES = ["Pvt Ltd", "Private Limited", "LLP"]
FIRST_NAMES = ["Ramesh", "Suresh", "Anita", "Priya", "Vikram", "Arjun", "Kavita", "Rahul", "Sunita", "Deepak"]
LAST_NAMES = ["Sharma", "Patel", "Shah", "Mehta", "Gupta", "Reddy", "Nair", "Iyer", "Singh", "Kumar"]

# COMMAND ----------

# MAGIC %md ## Generators

# COMMAND ----------

def random_pan():
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    digits = "0123456789"
    return (
        "".join(random.choice(letters) for _ in range(5))
        + "".join(random.choice(digits) for _ in range(4))
        + random.choice(letters)
    )


def random_legal_name():
    return f"{random.choice(NAME_PREFIXES)} {random.choice(NAME_SUFFIXES)} {random.choice(LEGAL_SUFFIXES)}"


def random_person_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


SEGMENTS = ["clean", "borderline", "delinquent", "repeat_offender"]
SEGMENT_WEIGHTS = [0.70, 0.15, 0.10, 0.05]


def gen_segment_metrics(segment):
    if segment == "clean":
        return dict(
            dscr=round(random.uniform(1.4, 2.2), 2), days_past_due=0,
            cibil_score=random.randint(750, 900), crilc_flag=False,
            wilful_defaulter_flag=False, cross_bank_exposure=round(random.uniform(0, 2_000_000), 2),
            missed_payment_flag=False,
        )
    if segment == "borderline":
        return dict(
            dscr=round(random.uniform(1.05, 1.3), 2), days_past_due=random.randint(1, 30),
            cibil_score=random.randint(650, 749), crilc_flag=False,
            wilful_defaulter_flag=False, cross_bank_exposure=round(random.uniform(0, 5_000_000), 2),
            missed_payment_flag=random.random() < 0.3,
        )
    if segment == "delinquent":
        return dict(
            dscr=round(random.uniform(0.6, 1.0), 2), days_past_due=random.randint(90, 180),
            cibil_score=random.randint(400, 649), crilc_flag=random.random() < 0.3,
            wilful_defaulter_flag=False, cross_bank_exposure=round(random.uniform(1_000_000, 8_000_000), 2),
            missed_payment_flag=True,
        )
    # repeat_offender
    return dict(
        dscr=round(random.uniform(0.7, 1.4), 2), days_past_due=random.choice([0, 30, 90, 180]),
        cibil_score=random.randint(300, 650), crilc_flag=True,
        wilful_defaulter_flag=random.random() < 0.5, cross_bank_exposure=round(random.uniform(5_000_000, 20_000_000), 2),
        missed_payment_flag=True,
    )

# COMMAND ----------

# MAGIC %md ## Build the borrower pool: 1 fixed reference borrower + 199 random

# COMMAND ----------

borrowers, financials, cibil_reports, crilc_records, product_history, promoters = [], [], [], [], [], []


def add_borrower(borrower_id, legal_name, sector, annual_turnover, pan, city,
                  owner_ids, no_internal_history, relationship_start_date,
                  metrics, existing_exposure, collateral_value):
    borrowers.append(dict(
        borrower_id=borrower_id, legal_name=legal_name, sector=sector,
        annual_turnover=annual_turnover, pan=pan, city=city,
        owner_ids=owner_ids, no_internal_history=no_internal_history,
        relationship_start_date=relationship_start_date,
    ))
    financials.append(dict(
        borrower_id=borrower_id, statement_date=TODAY,
        dscr=metrics["dscr"], days_past_due=metrics["days_past_due"],
        existing_exposure=existing_exposure, collateral_value=collateral_value,
    ))
    cibil_reports.append(dict(
        pan=pan, borrower_id=borrower_id, cibil_score=metrics["cibil_score"],
        active_accounts=random.randint(1, 10), delinquency_flag=metrics["days_past_due"] > 0,
        enquiry_count=random.randint(0, 8), report_date=TODAY,
    ))
    crilc_records.append(dict(
        borrower_id=borrower_id, crilc_flag=metrics["crilc_flag"],
        wilful_defaulter_flag=metrics["wilful_defaulter_flag"],
        cross_bank_exposure=metrics["cross_bank_exposure"], reporting_date=TODAY,
    ))
    for _ in range(random.randint(0, 3)):
        product_history.append(dict(
            borrower_id=borrower_id,
            product_type=random.choice(["credit_card", "overdraft", "term_loan"]),
            missed_payment_flag=bool(metrics["missed_payment_flag"] and random.random() < 0.6),
            days_past_due=metrics["days_past_due"] if metrics["missed_payment_flag"] else 0,
            product_open_date=TODAY - timedelta(days=random.randint(90, 2000)),
        ))
    for owner_id in owner_ids:
        promoters.append(dict(
            owner_id=owner_id, borrower_id=borrower_id, name=random_person_name(),
            pan=random_pan(), individual_cibil_score=random.randint(600, 900),
            other_business_interests=random.choice(["", "One other trading firm", "Two other MSME units"]),
            guarantor_flag=random.random() < 0.5,
            exposure_elsewhere=round(random.uniform(0, 3_000_000), 2),
        ))


# Fixed reference borrower — exact values from Section 3 of the design doc.
# get_borrower_financials('MT-2026-0142') must reproduce these exactly (Phase 1 exit criterion).
add_borrower(
    borrower_id=REFERENCE_BORROWER_ID, legal_name="Meera Textiles Pvt Ltd",
    sector="textile_msme", annual_turnover=48_000_000.0, pan="AABCM1234M",
    city="Surat", owner_ids=["OWN-MT-2026-0142-1"], no_internal_history=False,
    relationship_start_date=date(2022, 4, 1),
    metrics=dict(
        dscr=1.15, days_past_due=0, cibil_score=720, crilc_flag=False,
        wilful_defaulter_flag=False, cross_bank_exposure=0.0, missed_payment_flag=False,
    ),
    existing_exposure=3_500_000.0, collateral_value=9_000_000.0,
)

# Random pool
segment_choices = random.choices(SEGMENTS, weights=SEGMENT_WEIGHTS, k=N_RANDOM_BORROWERS)
for i, segment in enumerate(segment_choices, start=1):
    borrower_id = f"BRW-2026-{i:04d}"
    metrics = gen_segment_metrics(segment)
    turnover = round(random.uniform(1, 15) * 10_000_000, 2)
    add_borrower(
        borrower_id=borrower_id, legal_name=random_legal_name(),
        sector=random.choice(SECTORS), annual_turnover=turnover,
        pan=random_pan(), city=random.choice(CITIES),
        owner_ids=[f"OWN-{borrower_id}-1"], no_internal_history=random.random() < 0.10,
        relationship_start_date=TODAY - timedelta(days=random.randint(30, 3000)),
        metrics=metrics,
        existing_exposure=round(turnover * random.uniform(0.05, 0.4), 2),
        collateral_value=round(turnover * random.uniform(0.1, 0.6), 2),
    )

print(
    f"Generated {len(borrowers)} borrowers total: 1 reference + "
    f"{segment_choices.count('clean')} clean, {segment_choices.count('borderline')} borderline, "
    f"{segment_choices.count('delinquent')} delinquent, {segment_choices.count('repeat_offender')} repeat_offender"
)

# COMMAND ----------

# MAGIC %md ## Write bronze tables (explicit schemas, overwrite each run)

# COMMAND ----------

borrower_schema = StructType([
    StructField("borrower_id", StringType()),
    StructField("legal_name", StringType()),
    StructField("sector", StringType()),
    StructField("annual_turnover", DoubleType()),
    StructField("pan", StringType()),
    StructField("city", StringType()),
    StructField("owner_ids", ArrayType(StringType())),
    StructField("no_internal_history", BooleanType()),
    StructField("relationship_start_date", DateType()),
])

financial_schema = StructType([
    StructField("borrower_id", StringType()),
    StructField("statement_date", DateType()),
    StructField("dscr", DoubleType()),
    StructField("days_past_due", IntegerType()),
    StructField("existing_exposure", DoubleType()),
    StructField("collateral_value", DoubleType()),
])

cibil_schema = StructType([
    StructField("pan", StringType()),
    StructField("borrower_id", StringType()),
    StructField("cibil_score", IntegerType()),
    StructField("active_accounts", IntegerType()),
    StructField("delinquency_flag", BooleanType()),
    StructField("enquiry_count", IntegerType()),
    StructField("report_date", DateType()),
])

crilc_schema = StructType([
    StructField("borrower_id", StringType()),
    StructField("crilc_flag", BooleanType()),
    StructField("wilful_defaulter_flag", BooleanType()),
    StructField("cross_bank_exposure", DoubleType()),
    StructField("reporting_date", DateType()),
])

product_history_schema = StructType([
    StructField("borrower_id", StringType()),
    StructField("product_type", StringType()),
    StructField("missed_payment_flag", BooleanType()),
    StructField("days_past_due", IntegerType()),
    StructField("product_open_date", DateType()),
])

promoter_schema = StructType([
    StructField("owner_id", StringType()),
    StructField("borrower_id", StringType()),
    StructField("name", StringType()),
    StructField("pan", StringType()),
    StructField("individual_cibil_score", IntegerType()),
    StructField("other_business_interests", StringType()),
    StructField("guarantor_flag", BooleanType()),
    StructField("exposure_elsewhere", DoubleType()),
])

tables_to_write = [
    ("raw_borrowers", borrowers, borrower_schema),
    ("raw_financial_statements", financials, financial_schema),
    ("raw_cibil_reports", cibil_reports, cibil_schema),
    ("raw_crilc_records", crilc_records, crilc_schema),
    ("raw_internal_product_history", product_history, product_history_schema),
    ("raw_promoter_records", promoters, promoter_schema),
]

for table_name, rows, schema in tables_to_write:
    df = spark.createDataFrame(rows, schema=schema)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.bronze.{table_name}")
    print(f"bronze.{table_name}: {df.count()} rows")

# COMMAND ----------

# MAGIC %md ## Silver: dedupe on primary key, drop rows missing required fields

# COMMAND ----------

SILVER_MAP = {
    "raw_borrowers": ("borrowers", ["borrower_id"]),
    "raw_financial_statements": ("financial_statements", ["borrower_id", "statement_date"]),
    "raw_cibil_reports": ("cibil_reports", ["pan"]),
    "raw_crilc_records": ("crilc_records", ["borrower_id"]),
    "raw_internal_product_history": ("internal_product_history", ["borrower_id", "product_type", "product_open_date"]),
    "raw_promoter_records": ("promoter_records", ["owner_id"]),
}

for raw_name, (silver_name, pk_cols) in SILVER_MAP.items():
    df = spark.table(f"{CATALOG}.bronze.{raw_name}").dropna(subset=pk_cols).dropDuplicates(pk_cols)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.{silver_name}")
    print(f"silver.{silver_name}: {df.count()} rows")

# COMMAND ----------

# MAGIC %md ## Gold: borrower_360 (wide, query-ready) + applications (reference scenario seed)

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.gold.borrower_360 AS
    SELECT
        b.borrower_id,
        b.legal_name,
        b.sector,
        b.annual_turnover,
        f.dscr,
        f.days_past_due,
        f.existing_exposure,
        f.collateral_value,
        c.cibil_score,
        COALESCE(cr.crilc_flag, false) AS crilc_flag,
        COALESCE(cr.wilful_defaulter_flag, false) AS wilful_defaulter_flag,
        b.no_internal_history,
        b.owner_ids,
        current_date() AS data_as_of
    FROM {CATALOG}.silver.borrowers b
    LEFT JOIN {CATALOG}.silver.financial_statements f ON b.borrower_id = f.borrower_id
    LEFT JOIN {CATALOG}.silver.cibil_reports c ON b.pan = c.pan
    LEFT JOIN {CATALOG}.silver.crilc_records cr ON b.borrower_id = cr.borrower_id
""")

display(spark.sql(f"SELECT COUNT(*) AS borrower_count FROM {CATALOG}.gold.borrower_360"))

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.gold.applications (
        application_id STRING,
        borrower_id STRING,
        product STRING,
        assessment_date DATE,
        requested_loan_amount DOUBLE
    ) USING DELTA
""")

spark.sql(f"""
    MERGE INTO {CATALOG}.gold.applications t
    USING (
        SELECT
            'APP-001' AS application_id,
            '{REFERENCE_BORROWER_ID}' AS borrower_id,
            'working_capital' AS product,
            DATE'{TODAY.isoformat()}' AS assessment_date,
            12000000.0 AS requested_loan_amount
    ) s
    ON t.application_id = s.application_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

display(spark.table(f"{CATALOG}.gold.applications"))

# COMMAND ----------

# MAGIC %md ## Phase 1 exit criterion: MT-2026-0142 must match Section 3 exactly

# COMMAND ----------

row = spark.sql(f"""
    SELECT sector, annual_turnover, dscr, days_past_due, existing_exposure, collateral_value
    FROM {CATALOG}.gold.borrower_360
    WHERE borrower_id = '{REFERENCE_BORROWER_ID}'
""").collect()[0]

expected = dict(
    sector="textile_msme", annual_turnover=48_000_000.0, dscr=1.15,
    days_past_due=0, existing_exposure=3_500_000.0, collateral_value=9_000_000.0,
)

for field, expected_value in expected.items():
    actual_value = row[field]
    assert actual_value == expected_value, f"Mismatch on {field}: expected {expected_value}, got {actual_value}"

print(f"Phase 1 exit criterion met on catalog '{CATALOG}': {REFERENCE_BORROWER_ID} matches Section 3 exactly.")
