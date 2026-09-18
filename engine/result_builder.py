"""Assembles the Section 7 AssessmentResult JSON shape from rule results. Pure function.

Eligibility status is decided by a fixed, documented precedence — this is the one place in
the engine where "what does a failure actually mean" is decided, so it's deliberately the
only place that knows about it:

1. Any required metric unavailable, or a caller-supplied gap (e.g. no CIBIL report)
   -> "insufficient_information" — never guess past a missing input.
2. A hard block (wilful defaulter) -> "ineligible", regardless of every other metric.
3. A failed DSCR or days-past-due rule -> "manual_review".
4. A failed collateral-coverage rule -> "conditional".
5. Otherwise -> "eligible".

A plain CRILC flag (repeat-offender signal short of wilful-defaulter) does not by itself
change eligibility_status here — Section 8.2 treats it as a Key Risk Factor for the human
reviewer and the Explanation/Advisory passes to weigh, not grounds for automatic rejection.
Only `hard_block=True` (wilful_defaulter_flag) forces "ineligible".
"""

MANUAL_REVIEW_METRICS = {"dscr", "days_past_due"}
CONDITIONAL_METRICS = {"collateral_coverage_ratio"}


def build_result(application, rule_results, policy_version, missing_fields=None,
                  hard_block=False, exceptions=None):
    missing_fields = list(missing_fields or [])
    exceptions = list(exceptions or [])

    failed = [r for r in rule_results if r["status"] == "fail"]
    unknown = [r for r in rule_results if r["status"] == "unknown"]

    for r in unknown:
        missing_fields.append(f"metric '{r['metric']}' unavailable for rule {r['rule_id']}")

    for r in failed:
        exceptions.append(
            f"{r['metric']} {r['operator']} {r['required_value']} not satisfied "
            f"(actual: {r['actual_value']})"
        )

    if missing_fields:
        eligibility_status = "insufficient_information"
    elif hard_block:
        eligibility_status = "ineligible"
    elif any(r["metric"] in MANUAL_REVIEW_METRICS for r in failed):
        eligibility_status = "manual_review"
    elif any(r["metric"] in CONDITIONAL_METRICS for r in failed):
        eligibility_status = "conditional"
    else:
        eligibility_status = "eligible"

    return {
        "application_id": application["application_id"],
        "policy_version": policy_version,
        "eligibility_status": eligibility_status,
        "rule_results": rule_results,
        "exceptions": exceptions,
        "missing_information": missing_fields,
    }
