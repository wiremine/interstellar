//! Terminal step tests.

use interstellar::value::Value;

use crate::common::graphs::{create_empty_graph, create_small_graph};

#[test]
fn to_list_collects_all_values() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g.v().to_list();
    assert_eq!(results.len(), 4);
}

#[test]
fn to_set_deduplicates() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g.inject([1i64, 2i64, 1i64, 3i64]).to_set();
    assert_eq!(results.len(), 3);
}

#[test]
fn next_returns_first_value() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let result = g.v().next();
    assert!(result.is_some());
    assert!(result.unwrap().is_vertex());
}

#[test]
fn next_returns_none_for_empty() {
    let graph = create_empty_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    let result = g.v().next();
    assert!(result.is_none());
}

#[test]
fn has_next_returns_true_when_results_exist() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    assert!(g.v().has_next());
}

#[test]
fn has_next_returns_false_when_empty() {
    let graph = create_empty_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    assert!(!g.v().has_next());
}

#[test]
fn one_returns_single_result() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let result = g.v_ids([tg.alice]).one();
    assert!(result.is_ok());
    assert_eq!(result.unwrap().as_vertex_id(), Some(tg.alice));
}

#[test]
fn one_errors_on_empty() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Query for nonexistent label
    let result = g.v().has_label("nonexistent").one();
    assert!(result.is_err());
}

#[test]
fn one_errors_on_multiple() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let result = g.v().one();
    assert!(result.is_err());
}

#[test]
fn count_returns_correct_count() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    assert_eq!(g.v().count(), 4);
    assert_eq!(g.e().count(), 5);
    assert_eq!(g.v().has_label("person").count(), 3);
}

#[test]
fn sum_adds_numeric_values() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let result = g.inject([1i64, 2i64, 3i64, 4i64]).sum();
    assert_eq!(result, Value::Int(10));
}

#[test]
fn sum_handles_floats() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let values: Vec<Value> = vec![Value::Int(1), Value::Float(2.5), Value::Int(3)];
    let result = g.inject(values).sum();
    if let Value::Float(f) = result {
        assert!((f - 6.5).abs() < 1e-10);
    } else {
        panic!("Expected Float, got {:?}", result);
    }
}

#[test]
fn sum_returns_zero_for_empty() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let values: Vec<Value> = vec![];
    let result = g.inject(values).sum();
    assert_eq!(result, Value::Int(0));
}

#[test]
fn min_finds_minimum() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let result = g.inject([5i64, 2i64, 8i64, 1i64, 9i64]).min();
    assert_eq!(result, Some(Value::Int(1)));
}

#[test]
fn min_returns_none_for_empty() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let values: Vec<Value> = vec![];
    let result = g.inject(values).min();
    assert!(result.is_none());
}

#[test]
fn max_finds_maximum() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let result = g.inject([5i64, 2i64, 8i64, 1i64, 9i64]).max();
    assert_eq!(result, Some(Value::Int(9)));
}

#[test]
fn max_returns_none_for_empty() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let values: Vec<Value> = vec![];
    let result = g.inject(values).max();
    assert!(result.is_none());
}

#[test]
fn fold_accumulates_values() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let result = g
        .inject([1i64, 2i64, 3i64])
        .fold(0i64, |acc, v| acc + v.as_i64().unwrap_or(0));
    assert_eq!(result, 6);
}

#[test]
fn take_returns_first_n_values() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g.v().take(2);
    assert_eq!(results.len(), 2);
}

#[test]
fn iterate_consumes_without_collecting() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Should not panic
    g.v().iterate();
}
