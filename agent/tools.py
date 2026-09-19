"""Thin wrappers over the Unity Catalog Functions (Phase 1/2) plus the LLM call.

These are deterministic-orchestration helpers, not LLM function-calling tools — the graph
(agent/graph.py) decides exactly when each one runs; no model ever picks which of these to
call. That's the whole point of "a controlled state graph, not an open-ended agent"
(Section 6): every tool call is explicit in the graph's edges, never in a model's discretion.

Each function takes `spark` and `catalog` explicitly rather than relying on a notebook global,
so they're callable the same way from any notebook (or a test with a fake spark session).
"""

import requests


def sql_escape(value):
    """Minimal defense against a stray apostrophe in an LLM-authored query string breaking
    the SQL literal it's interpolated into. Every other value in this project is
    system-controlled (IDs, dates), not free text, so this is only exercised by retrieve_policy's
    query argument in practice."""
    return str(value).replace("'", "''")


def get_borrower_financials(spark, catalog, borrower_id):
    row = spark.sql(
        f"SELECT {catalog}.gold.get_borrower_financials('{sql_escape(borrower_id)}') AS r"
    ).collect()[0]["r"]
    return row.asDict() if row is not None else {}


def get_borrower_pan(spark, catalog, borrower_id):
    """silver.borrowers.pan isn't part of the get_borrower_financials struct (Phase 1 keeps
    company PAN internal to the join, not a field the agent/engine consume) — fetched directly
    here so retrieve_data can still call get_cibil_report for the company's own bureau record."""
    row = spark.sql(
        f"SELECT pan FROM {catalog}.silver.borrowers WHERE borrower_id = '{sql_escape(borrower_id)}'"
    ).collect()[0]
    return row["pan"]


def get_cibil_report(spark, catalog, pan):
    row = spark.sql(
        f"SELECT {catalog}.gold.get_cibil_report('{sql_escape(pan)}') AS r"
    ).collect()[0]["r"]
    return row.asDict() if row is not None else {}


def check_repeat_offender_signals(spark, catalog, borrower_id):
    row = spark.sql(
        f"SELECT {catalog}.gold.check_repeat_offender_signals('{sql_escape(borrower_id)}') AS r"
    ).collect()[0]["r"]
    return row.asDict() if row is not None else {}


def get_promoter_financials(spark, catalog, owner_ids):
    if not owner_ids:
        return []
    ids_sql = ", ".join(f"'{sql_escape(o)}'" for o in owner_ids)
    rows = spark.sql(
        f"SELECT {catalog}.gold.get_promoter_financials(array({ids_sql})) AS r"
    ).collect()[0]["r"]
    return [r.asDict() for r in rows] if rows else []


def retrieve_policy(spark, catalog, query, applicable_sector, as_of_date):
    rows = spark.sql(f"""
        SELECT {catalog}.gold.retrieve_policy('{sql_escape(query)}', '{sql_escape(applicable_sector)}', DATE'{as_of_date}') AS r
    """).collect()[0]["r"]
    return [r.asDict() for r in rows] if rows else []


def get_committee_memos(spark, catalog, applicable_sector, borrower_id=None, as_of_date=None):
    borrower_literal = f"'{sql_escape(borrower_id)}'" if borrower_id else "NULL"
    as_of_literal = f"DATE'{as_of_date}'" if as_of_date else "NULL"
    rows = spark.sql(f"""
        SELECT {catalog}.gold.get_committee_memos('{sql_escape(applicable_sector)}', {borrower_literal}, {as_of_literal}) AS r
    """).collect()[0]["r"]
    return [r.asDict() for r in rows] if rows else []


def call_llm(workspace_url, token, messages, endpoint="databricks-meta-llama-3-3-70b-instruct",
             max_tokens=800, temperature=0.2):
    """Direct REST call to a Databricks Model Serving foundation model endpoint.

    Implementation note: `databricks-langchain` (the documented "LangChain tools" route) pulls
    in `databricks-openai` -> `openai-agents`, whose dependency tree pip's resolver can't
    finish (`ResolutionTooDeep`) on this workspace — confirmed by installing each package
    individually. `langgraph` and plain `langchain` install fine on their own; this REST call
    replaces only the `ChatDatabricks` convenience wrapper, using the same OpenAI-compatible
    `/serving-endpoints/<name>/invocations` schema it would have called internally anyway.
    """
    resp = requests.post(
        f"{workspace_url}/serving-endpoints/{endpoint}/invocations",
        headers={"Authorization": f"Bearer {token}"},
        json={"messages": messages, "max_tokens": max_tokens, "temperature": temperature},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]
