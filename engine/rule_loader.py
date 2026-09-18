"""Resolves which policy rules apply to an assessment.

Pure functions only — no Spark, no network calls. Callers (a Databricks notebook, a test)
fetch rule rows from `gold.policy_rules` (or construct synthetic ones) and pass them in as
plain dicts; this module never touches the catalog itself.

A rule row is a dict with keys: rule_uid, rule_id, metric, operator, threshold,
applicable_sector, effective_date (date), supersedes, source_document_id, approval_status —
the columns `gold.policy_rules` has (Phase 2) — plus `version`, which `gold.policy_rules`
itself doesn't carry (it lives on `bronze.raw_policy_documents`). The Databricks-side caller
is expected to join the two before calling this module, so `version` is always present here.
"""


def select_applicable_rules(all_rules, applicable_sector, as_of_date):
    """Returns the one currently-effective rule per rule_id for this sector, as of as_of_date.

    Includes both sector-specific rules and "general" rules (Section 5: the general policy
    "applies in addition to, not instead of" any sector-specific circular). Excludes rules
    not yet effective and rules whose document has been superseded by another in-effect one —
    this is the mechanism that makes the pre/post-circular narrative hold at the engine layer,
    the same way it does for retrieve_policy in Phase 2.
    """
    candidates = [
        r for r in all_rules
        if r["applicable_sector"] in (applicable_sector, "general")
        and r["effective_date"] <= as_of_date
        and r["approval_status"] == "approved"
    ]
    superseded_document_ids = {r["supersedes"] for r in candidates if r.get("supersedes")}
    current = [r for r in candidates if r["source_document_id"] not in superseded_document_ids]

    latest_by_rule_id = {}
    for rule in current:
        existing = latest_by_rule_id.get(rule["rule_id"])
        if existing is None or rule["effective_date"] > existing["effective_date"]:
            latest_by_rule_id[rule["rule_id"]] = rule
    return list(latest_by_rule_id.values())


def resolve_headline_version(applicable_rules, applicable_sector):
    """The version label reported at the top level of the assessment result.

    Prefers the sector-specific document's version (e.g. "v2" for the post-circular textile
    circular) over the general policy's, since that's the regulatory context a credit officer
    actually cares about. Falls back to "n/a" if only general rules apply.
    """
    for rule in applicable_rules:
        if rule["applicable_sector"] == applicable_sector:
            return rule["version"]
    return "n/a"
