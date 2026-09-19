"""AssessmentState — the LangGraph state schema (LLD Part D.5).

`total=False` since every field is populated incrementally as the graph progresses; a field
missing simply means that node hasn't run yet.

Two fields exist here that Part D.5's original sketch didn't name explicitly, added during
implementation because the graph genuinely needs them:
- `application` / `as_of_date`: the application-level facts `identify_application` fetches
  (requested_loan_amount, assessment_date) — the engine needs these alongside the borrower.
- `_explanation_attempts`: bounds the `ValidateResponse -> GenerateExplanation` retry loop
  (Section 6's diagram shows the loop but doesn't cap it; an uncapped retry against a model
  that keeps citing an ungrounded number would loop forever).
"""

from typing import Any, TypedDict


class AssessmentState(TypedDict, total=False):
    application_id: str
    borrower_id: str
    application: dict[str, Any]
    as_of_date: Any  # datetime.date
    raw_inputs: dict[str, Any]           # {"borrower": ..., "cibil": ..., "repeat_offender": ..., "promoters": [...]}
    missing_fields: list[str]
    engine_result: dict[str, Any]         # Section 7 output — set once by evaluate_policy_rules, never mutated after
    retrieved_evidence: list[dict[str, Any]]
    draft_explanation: str
    committee_memos: list[dict[str, Any]]
    advisory_notes: str                   # Advisory Pass output — never eligibility-bearing
    validation_errors: list[str]
    final_assessment: dict[str, Any]      # Section 9's assessment record
    _explanation_attempts: int
