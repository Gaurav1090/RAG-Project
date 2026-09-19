"""Assembles the LangGraph state graph from agent/nodes.py, matching Section 6's diagram:
a controlled state graph, not an open-ended agent — every edge below is explicit, and neither
branch can reach human_review without first passing through evaluate_policy_rules.
"""

from langgraph.graph import END, StateGraph

from agent.nodes import MAX_EXPLANATION_ATTEMPTS, build_nodes
from agent.state import AssessmentState


def build_graph(spark, catalog, workspace_url, token, prompts_dir):
    nodes = build_nodes(spark, catalog, workspace_url, token, prompts_dir)

    graph = StateGraph(AssessmentState)
    for name, fn in nodes.items():
        graph.add_node(name, fn)

    graph.set_entry_point("identify_application")
    graph.add_edge("identify_application", "retrieve_data")
    graph.add_edge("retrieve_data", "validate_data")

    graph.add_conditional_edges(
        "validate_data",
        lambda state: "flag_missing_data" if state.get("missing_fields") else "evaluate_policy_rules",
        {"flag_missing_data": "flag_missing_data", "evaluate_policy_rules": "evaluate_policy_rules"},
    )
    graph.add_edge("flag_missing_data", "evaluate_policy_rules")

    # Fan-out: Explanation Pass and Advisory Pass both branch off evaluate_policy_rules and
    # run independently — the Advisory branch is not gated on the Explanation branch.
    graph.add_edge("evaluate_policy_rules", "retrieve_evidence")
    graph.add_edge("evaluate_policy_rules", "run_advisory_pass")

    graph.add_edge("retrieve_evidence", "generate_explanation")
    graph.add_edge("run_advisory_pass", "generate_advisory_notes")

    # Join: synthesize_assessment waits on both branches.
    graph.add_edge("generate_explanation", "synthesize_assessment")
    graph.add_edge("generate_advisory_notes", "synthesize_assessment")

    graph.add_edge("synthesize_assessment", "validate_response")

    def route_after_validate_response(state):
        if state.get("validation_errors") and state.get("_explanation_attempts", 0) < MAX_EXPLANATION_ATTEMPTS:
            return "generate_explanation"
        return "human_review"

    graph.add_conditional_edges(
        "validate_response", route_after_validate_response,
        {"generate_explanation": "generate_explanation", "human_review": "human_review"},
    )

    graph.add_edge("human_review", END)

    return graph.compile()
