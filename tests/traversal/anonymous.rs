//! Anonymous traversal tests.

#![allow(unused_variables)]
use interstellar::traversal::__;
use interstellar::value::Value;

use crate::common::graphs::create_small_graph;

#[test]
fn identity_passes_through_unchanged() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity();
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 4);
}

#[test]
fn out_anonymous_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::out();
    let results = g.v_ids([tg.alice]).append(anon).to_list();
    assert_eq!(results.len(), 2);
}

#[test]
fn has_label_anonymous_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::has_label("person");
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 3);
}

#[test]
fn chained_anonymous_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::out_labels(&["knows"]).has_label("person");
    let results = g.v_ids([tg.alice]).append(anon).to_list();
    assert_eq!(results.len(), 1); // Alice knows Bob (person)
}

#[test]
fn values_anonymous_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::values("name");
    let results = g.v().has_label("person").append(anon).to_list();
    assert_eq!(results.len(), 3);
}

#[test]
fn filter_anonymous_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::filter(|_ctx, v| matches!(v, Value::Int(n) if *n > 2));
    let results = g.inject([1i64, 2i64, 3i64, 4i64]).append(anon).to_list();
    assert_eq!(results.len(), 2);
}

#[test]
fn dedup_anonymous_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::dedup();
    let results = g.inject([1i64, 2i64, 1i64, 3i64]).append(anon).to_list();
    assert_eq!(results.len(), 3);
}

#[test]
fn limit_anonymous_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::limit(2);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 2);
}

#[test]
fn map_anonymous_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::map(|_ctx, v| {
        if let Value::Int(n) = v {
            Value::Int(n * 10)
        } else {
            v.clone()
        }
    });
    let results = g.inject([1i64, 2i64]).append(anon).to_list();
    assert_eq!(results[0], Value::Int(10));
    assert_eq!(results[1], Value::Int(20));
}

#[test]
fn constant_anonymous_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::constant(42i64);
    let results = g.v().limit(3).append(anon).to_list();
    assert_eq!(results.len(), 3);
    for r in results {
        assert_eq!(r, Value::Int(42));
    }
}

#[test]
fn complex_anonymous_traversal_chain() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Find names of people that Alice knows
    let anon = __::out_labels(&["knows"])
        .has_label("person")
        .values("name");

    let results = g.v_ids([tg.alice]).append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Bob".to_string()));
}
