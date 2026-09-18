"""Evaluates computed metrics against applicable rules. Pure function, no side effects."""

import operator as _operator

_OPERATORS = {
    ">=": _operator.ge,
    "<=": _operator.le,
    ">": _operator.gt,
    "<": _operator.lt,
    "==": _operator.eq,
}


def evaluate_rules(metrics, rules):
    """metrics: dict[str, float | int | None], from metric_calculator.compute_metrics.
    rules: list[dict], from rule_loader.select_applicable_rules.
    Returns: list[dict] — {rule_id, metric, operator, status, actual_value, required_value}.
    status is "pass", "fail", or "unknown" (metric value unavailable — never silently a pass).
    """
    results = []
    for rule in rules:
        metric_name = rule["metric"]
        actual_value = metrics.get(metric_name)
        compare = _OPERATORS[rule["operator"]]

        if actual_value is None:
            status = "unknown"
        else:
            status = "pass" if compare(actual_value, rule["threshold"]) else "fail"

        results.append({
            "rule_id": rule["rule_id"],
            "metric": metric_name,
            "operator": rule["operator"],
            "status": status,
            "actual_value": actual_value,
            "required_value": rule["threshold"],
        })
    return results
