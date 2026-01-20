//! Error case tests.

use crate::common::graphs::create_small_graph;

#[test]
fn one_on_empty_result_returns_error() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let result = g.v().has_label("nonexistent").one();
    assert!(result.is_err());
}

#[test]
fn one_on_multiple_results_returns_error() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let result = g.v().has_label("person").one();
    assert!(result.is_err());
}

#[test]
fn navigation_on_non_element_produces_nothing() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Injected integers can't be navigated
    let results = g.inject([1i64, 2i64]).out().to_list();
    assert!(results.is_empty());
}

#[test]
fn values_on_non_element_produces_nothing() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Injected integers don't have properties
    let results = g.inject([1i64, 2i64]).values("name").to_list();
    assert!(results.is_empty());
}

#[test]
fn out_v_on_non_edge_produces_nothing() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Vertices can't use out_v (that's for edges)
    let results = g.v().out_v().to_list();
    assert!(results.is_empty());
}

#[test]
fn select_missing_label_filters_out() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Select a label that was never defined
    let results = g
        .v_ids([tg.alice])
        .as_("start")
        .out()
        .select(&["start", "nonexistent"])
        .to_list();

    // Should filter out because "nonexistent" label doesn't exist
    assert!(results.is_empty());
}
