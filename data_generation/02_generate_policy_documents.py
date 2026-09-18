# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 — Generate Dummy Policy Documents & Committee Memos
# MAGIC
# MAGIC Authors 4 dummy policy PDFs (general SME policy, textile circular v1, textile circular v2
# MAGIC revised, one unrelated-sector policy as a negative-retrieval control) and 7 dummy committee
# MAGIC memos, uploads them to the `policy_pdfs` / `committee_memos` Volumes, and writes their
# MAGIC manifest metadata as bronze tables.
# MAGIC
# MAGIC The structured `gold.policy_rules` table is also seeded here, directly from the same
# MAGIC ground truth used to write the documents — representing the "human-validated" output that
# MAGIC Section 5 of the design doc describes, without running a separate AI extraction step over
# MAGIC content we already know exactly (this is synthetic ground truth, not a real regulatory feed).
# MAGIC
# MAGIC `pipelines/policy_ingestion.py` (run after this) does the actual `ai_parse_document` parsing
# MAGIC and chunking of the PDFs this notebook creates.

# COMMAND ----------

# MAGIC %pip install -q reportlab
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "credit_platform", "Target catalog")
CATALOG = dbutils.widgets.get("catalog_name")
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas
from io import BytesIO


def render_pdf(title, paragraphs):
    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=LETTER)
    width, height = LETTER
    y = height - 72

    c.setFont("Helvetica-Bold", 16)
    c.drawString(72, y, title)
    y -= 36

    c.setFont("Helvetica", 11)
    for paragraph in paragraphs:
        words = paragraph.split()
        line = ""
        for word in words:
            test_line = f"{line} {word}".strip()
            if c.stringWidth(test_line, "Helvetica", 11) > width - 144:
                c.drawString(72, y, line)
                y -= 16
                line = word
            else:
                line = test_line
        if line:
            c.drawString(72, y, line)
            y -= 16
        y -= 12  # paragraph gap

    c.save()
    buffer.seek(0)
    return buffer.read()

# COMMAND ----------

# MAGIC %md ## Document ground truth (policy PDFs + their structured rules)

# COMMAND ----------

POLICY_DOCUMENTS = [
    dict(
        document_id="POLICY-GENERAL-01", version="v1", filename="SME_General_Credit_Policy.pdf",
        doc_type="internal_policy", effective_date="2025-06-01", supersedes=None,
        applicable_sector="general", approval_status="approved",
        title="SME General Credit Policy",
        paragraphs=[
            "This policy establishes baseline underwriting criteria applicable to all SME lending "
            "products at Bharat Vikas Bank (BVB), regardless of sector, unless superseded by a "
            "sector-specific circular.",
            "Rule GEN-001: The Collateral Coverage Ratio (collateral value divided by requested loan "
            "amount) must be greater than or equal to 0.75 for all working-capital and term-loan "
            "facilities.",
            "Rule GEN-002: Days Past Due on any existing facility must be less than or equal to 30 at "
            "the time of assessment. Applications exceeding this threshold require manual review "
            "regardless of other metrics.",
            "This policy applies in addition to, not instead of, any sector-specific circular in "
            "force at the time of assessment.",
        ],
        rules=[
            dict(rule_id="GEN-001", metric="collateral_coverage_ratio", operator=">=", threshold=0.75),
            dict(rule_id="GEN-002", metric="days_past_due", operator="<=", threshold=30.0),
        ],
    ),
    dict(
        document_id="POLICY-TEXTILE-01", version="v1", filename="RBI_Textile_Sector_Circular_v1.pdf",
        doc_type="rbi_circular", effective_date="2026-01-01", supersedes=None,
        applicable_sector="textile_msme", approval_status="approved",
        title="RBI Textile MSME Sector Lending Circular",
        paragraphs=[
            "This circular sets sector-specific credit norms for textile MSME exporters, effective "
            "January 1, 2026.",
            "Rule DSCR-001: The Debt Service Coverage Ratio (DSCR) for textile MSME export borrowers "
            "must be greater than or equal to 1.10.",
            "Borrowers falling below this threshold are not automatically ineligible but require "
            "additional scrutiny under the bank's manual review process.",
        ],
        rules=[
            dict(rule_id="DSCR-001", metric="dscr", operator=">=", threshold=1.10),
        ],
    ),
    dict(
        document_id="POLICY-TEXTILE-01-v2", version="v2", filename="RBI_Textile_Sector_Circular_v2_Revised.pdf",
        doc_type="rbi_circular", effective_date="2026-09-01", supersedes="POLICY-TEXTILE-01",
        applicable_sector="textile_msme", approval_status="approved",
        title="RBI Textile MSME Sector Lending Circular (Revised)",
        paragraphs=[
            "This circular revises and supersedes the RBI Textile MSME Sector Lending Circular "
            "effective January 1, 2026, in light of tightening NPA and sector-exposure norms for "
            "textile MSME lending.",
            "Rule DSCR-001 (Revised): The Debt Service Coverage Ratio (DSCR) for textile MSME export "
            "borrowers must be greater than or equal to 1.25, effective September 1, 2026.",
            "Applications assessed on or after the effective date must use this revised threshold. "
            "Applications assessed before this date remain governed by the prior circular.",
        ],
        rules=[
            dict(rule_id="DSCR-001", metric="dscr", operator=">=", threshold=1.25),
        ],
    ),
    dict(
        document_id="POLICY-PHARMA-01", version="v1", filename="Pharma_Sector_Lending_Policy.pdf",
        doc_type="sector_policy", effective_date="2025-03-01", supersedes=None,
        applicable_sector="pharma_sme", approval_status="approved",
        title="Pharma SME Sector Lending Policy",
        paragraphs=[
            "This policy sets sector-specific credit norms for pharmaceutical SME borrowers.",
            "Rule DSCR-001: The Debt Service Coverage Ratio (DSCR) for pharma SME borrowers must be "
            "greater than or equal to 1.20.",
        ],
        rules=[
            dict(rule_id="DSCR-001", metric="dscr", operator=">=", threshold=1.20),
        ],
    ),
]

COMMITTEE_MEMOS = [
    dict(memo_id="MEMO-001", filename="MEMO-001.pdf", applicable_sector="textile_msme", borrower_id=None,
         memo_date="2026-02-01", title="Committee Review — February 2026",
         paragraphs=["Textile MSME export demand remains healthy across the portfolio. No sector-wide "
                     "concerns flagged this cycle."]),
    dict(memo_id="MEMO-002", filename="MEMO-002.pdf", applicable_sector="textile_msme", borrower_id="MT-2026-0142",
         memo_date="2026-03-15", title="Borrower Note — Meera Textiles Pvt Ltd (MT-2026-0142)",
         paragraphs=["Promoter conduct reviewed favorably during the quarterly relationship review. "
                     "No adverse findings; no exposure elsewhere on record for the named promoter."]),
    dict(memo_id="MEMO-003", filename="MEMO-003.pdf", applicable_sector="textile_msme", borrower_id=None,
         memo_date="2026-09-10", title="Committee Review — September 2026",
         paragraphs=["Sector-wide textile export order volumes softened in Q2 2026 following demand "
                     "shifts in key export markets. Committee recommends enhanced monitoring for "
                     "exposures in this sector, though no immediate action is required for "
                     "individually performing accounts."]),
    dict(memo_id="MEMO-004", filename="MEMO-004.pdf", applicable_sector="manufacturing_sme", borrower_id=None,
         memo_date="2026-04-01", title="Committee Review — April 2026",
         paragraphs=["General manufacturing SME segment stable; input cost inflation noted but not "
                     "yet affecting repayment behavior."]),
    dict(memo_id="MEMO-005", filename="MEMO-005.pdf", applicable_sector="pharma_sme", borrower_id=None,
         memo_date="2026-05-01", title="Committee Review — May 2026",
         paragraphs=["Pharma SME segment facing regulatory pricing pressure on select formulations; "
                     "portfolio-level impact assessed as limited for now."]),
    dict(memo_id="MEMO-006", filename="MEMO-006.pdf", applicable_sector="trading_sme", borrower_id=None,
         memo_date="2026-06-01", title="Committee Review — June 2026",
         paragraphs=["Working capital cycles lengthening for trading SME accounts due to extended "
                     "receivable days; recommend closer monitoring of exposure concentration."]),
    dict(memo_id="MEMO-007", filename="MEMO-007.pdf", applicable_sector="agri_processing_sme", borrower_id=None,
         memo_date="2026-07-01", title="Committee Review — July 2026",
         paragraphs=["Agri-processing SME segment shows expected seasonal softness ahead of monsoon; "
                     "no structural concerns identified."]),
]

# COMMAND ----------

# MAGIC %md ## Render + upload PDFs, build manifests

# COMMAND ----------

policy_manifest_rows = []
policy_rule_rows = []

for doc in POLICY_DOCUMENTS:
    pdf_bytes = render_pdf(doc["title"], doc["paragraphs"])
    path = f"/Volumes/{CATALOG}/policy/policy_pdfs/{doc['filename']}"
    with open(path, "wb") as f:
        f.write(pdf_bytes)

    policy_manifest_rows.append(dict(
        document_id=doc["document_id"], version=doc["version"], filename=doc["filename"],
        doc_type=doc["doc_type"], effective_date=doc["effective_date"], supersedes=doc["supersedes"],
        applicable_sector=doc["applicable_sector"], approval_status=doc["approval_status"],
    ))

    for rule in doc["rules"]:
        policy_rule_rows.append(dict(
            rule_uid=f"{doc['document_id']}-{rule['rule_id']}",
            rule_id=rule["rule_id"], metric=rule["metric"], operator=rule["operator"],
            threshold=float(rule["threshold"]), applicable_sector=doc["applicable_sector"],
            effective_date=doc["effective_date"], supersedes=doc["supersedes"],
            source_document_id=doc["document_id"], approval_status=doc["approval_status"],
        ))

print(f"Uploaded {len(POLICY_DOCUMENTS)} policy PDFs, seeded {len(policy_rule_rows)} rule rows")

# COMMAND ----------

memo_manifest_rows = []

for memo in COMMITTEE_MEMOS:
    pdf_bytes = render_pdf(memo["title"], memo["paragraphs"])
    path = f"/Volumes/{CATALOG}/policy/committee_memos/{memo['filename']}"
    with open(path, "wb") as f:
        f.write(pdf_bytes)

    memo_manifest_rows.append(dict(
        memo_id=memo["memo_id"], filename=memo["filename"], applicable_sector=memo["applicable_sector"],
        borrower_id=memo["borrower_id"], memo_date=memo["memo_date"],
    ))

print(f"Uploaded {len(COMMITTEE_MEMOS)} committee memo PDFs")

# COMMAND ----------

# MAGIC %md ## Write manifests (bronze) and the ground-truth rules table (gold)

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType, DoubleType
from pyspark.sql.functions import to_date

policy_manifest_schema = StructType([
    StructField("document_id", StringType()), StructField("version", StringType()),
    StructField("filename", StringType()), StructField("doc_type", StringType()),
    StructField("effective_date", StringType()), StructField("supersedes", StringType()),
    StructField("applicable_sector", StringType()), StructField("approval_status", StringType()),
])
spark.createDataFrame(policy_manifest_rows, schema=policy_manifest_schema) \
    .withColumn("effective_date", to_date("effective_date")) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.bronze.raw_policy_documents")

memo_manifest_schema = StructType([
    StructField("memo_id", StringType()), StructField("filename", StringType()),
    StructField("applicable_sector", StringType()), StructField("borrower_id", StringType()),
    StructField("memo_date", StringType()),
])
spark.createDataFrame(memo_manifest_rows, schema=memo_manifest_schema) \
    .withColumn("memo_date", to_date("memo_date")) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.bronze.raw_committee_memo_manifest")

rules_schema = StructType([
    StructField("rule_uid", StringType()), StructField("rule_id", StringType()),
    StructField("metric", StringType()), StructField("operator", StringType()),
    StructField("threshold", DoubleType()), StructField("applicable_sector", StringType()),
    StructField("effective_date", StringType()), StructField("supersedes", StringType()),
    StructField("source_document_id", StringType()), StructField("approval_status", StringType()),
])
rules_df = spark.createDataFrame(policy_rule_rows, schema=rules_schema).withColumn("effective_date", to_date("effective_date"))
rules_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.policy_rules")

display(spark.table(f"{CATALOG}.gold.policy_rules"))

# COMMAND ----------

assert spark.table(f"{CATALOG}.bronze.raw_policy_documents").count() == len(POLICY_DOCUMENTS)
assert spark.table(f"{CATALOG}.bronze.raw_committee_memo_manifest").count() == len(COMMITTEE_MEMOS)
assert spark.table(f"{CATALOG}.gold.policy_rules").count() == len(policy_rule_rows)
print(f"Phase 2 document generation complete on catalog '{CATALOG}'.")
