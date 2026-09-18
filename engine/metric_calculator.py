"""Computes credit metrics from raw application + borrower inputs.

Pure function — no Spark, no network calls, no rounding surprises: every metric it can
compute, it computes; anything it can't (a missing input) comes back as None so the
evaluator can flag it rather than silently treating a gap as a pass.
"""


def compute_metrics(application, borrower):
    """application: dict with at least `requested_loan_amount`.
    borrower: a borrower_360-shaped dict (dscr, days_past_due, collateral_value, ...).
    Returns: dict[metric_name -> value], keyed to match rule "metric" values in gold.policy_rules.
    """
    requested_loan_amount = application.get("requested_loan_amount")
    collateral_value = borrower.get("collateral_value")

    collateral_coverage_ratio = None
    if collateral_value is not None and requested_loan_amount:
        collateral_coverage_ratio = round(collateral_value / requested_loan_amount, 4)

    return {
        "dscr": borrower.get("dscr"),
        "days_past_due": borrower.get("days_past_due"),
        "collateral_coverage_ratio": collateral_coverage_ratio,
    }
