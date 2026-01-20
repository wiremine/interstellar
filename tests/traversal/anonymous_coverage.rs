//! Coverage tests for traversal/anonymous.rs (__ factory functions).
//!
//! This module tests the anonymous traversal factory functions that are not
//! covered by other test modules. These functions create traversal fragments
//! that can be composed with bound traversals.

#![allow(unused_variables)]
use interstellar::traversal::p;
use interstellar::traversal::__;
use interstellar::value::Value;

use crate::common::graphs::{create_medium_graph, create_small_graph};

// =============================================================================
// Navigation - Vertex to Vertex (with labels)
// =============================================================================

#[test]
fn anon_in_labels() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::in_labels(&["knows"]);
    let results = g.v_ids([tg.bob]).append(anon).to_list();
    assert_eq!(results.len(), 1); // Alice knows Bob
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn anon_both_labels() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::both_labels(&["knows"]);
    let results = g.v_ids([tg.bob]).append(anon).to_list();
    // Bob: Alice->Bob (in) and Bob->Charlie (out)
    assert_eq!(results.len(), 2);
}

// =============================================================================
// Navigation - Vertex to Edge (with labels)
// =============================================================================

#[test]
fn anon_out_e_labels() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::out_e_labels(&["knows"]);
    let results = g.v_ids([tg.alice]).append(anon).to_list();
    assert_eq!(results.len(), 1); // Alice has one "knows" edge
}

#[test]
fn anon_in_e_labels() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::in_e_labels(&["knows"]);
    let results = g.v_ids([tg.bob]).append(anon).to_list();
    assert_eq!(results.len(), 1); // One incoming "knows" edge to Bob
}

#[test]
fn anon_both_e_labels() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::both_e_labels(&["knows"]);
    let results = g.v_ids([tg.bob]).append(anon).to_list();
    assert_eq!(results.len(), 2); // Alice->Bob and Bob->Charlie
}

// =============================================================================
// Filter Steps
// =============================================================================

#[test]
fn anon_has_not() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::has_not("age");
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 1); // Software doesn't have age
}

#[test]
fn anon_has_id() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::has_id(tg.alice);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn anon_has_ids() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::has_ids([tg.alice, tg.bob]);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 2);
}

#[test]
fn anon_has_where() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::has_where("age", p::gte(30));
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 2); // Alice (30) and Charlie (35)
}

#[test]
fn anon_is_predicate() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::is_(p::gt(25));
    let results = g
        .v()
        .has_label("person")
        .values("age")
        .append(anon)
        .to_list();
    assert_eq!(results.len(), 2); // Ages 30 and 35
}

#[test]
fn anon_is_eq() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::is_eq(30i64);
    let results = g
        .v()
        .has_label("person")
        .values("age")
        .append(anon)
        .to_list();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(30));
}

#[test]
fn anon_dedup_by_key() {
    let tg = create_medium_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::dedup_by_key("status");
    let results = g.v().has_label("person").append(anon).to_list();
    assert_eq!(results.len(), 2); // "active" and "inactive"
}

#[test]
fn anon_dedup_by_label() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::dedup_by_label();
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 2); // person and software
}

#[test]
fn anon_dedup_by_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Dedup by label using a sub-traversal
    let anon = __::dedup_by(__::label());
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 2); // One per unique label
}

#[test]
fn anon_skip() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::skip(2);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 2); // 4 - 2 = 2
}

#[test]
fn anon_range() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::range(1, 3);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 2); // Indices 1 and 2
}

#[test]
fn anon_simple_path() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::simple_path();
    let results = g
        .v_ids([tg.alice])
        .with_path()
        .out()
        .out()
        .append(anon)
        .to_list();
    // All traversals at depth 2 should be simple
    assert!(results.len() <= 4);
}

#[test]
fn anon_cyclic_path() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::cyclic_path();
    let results = g
        .v_ids([tg.alice])
        .with_path()
        .out()
        .out()
        .out()
        .append(anon)
        .to_list();
    // May or may not find cycles (test just exercises the code path)
    let _ = results.len();
}

#[test]
fn anon_tail() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::tail();
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 1);
}

#[test]
fn anon_tail_n() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::tail_n(2);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 2);
}

#[test]
fn anon_coin() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // p=0 filters everything
    let anon = __::coin(0.0);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 0);

    // p=1 keeps everything
    let anon2 = __::coin(1.0);
    let results2 = g.v().append(anon2).to_list();
    assert_eq!(results2.len(), 4);
}

#[test]
fn anon_sample() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::sample(2);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 2);
}

#[test]
fn anon_has_key() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::has_key("name");
    let results = g
        .v()
        .has_label("person")
        .limit(1)
        .properties()
        .append(anon)
        .to_list();
    assert_eq!(results.len(), 1);
}

#[test]
fn anon_has_key_any() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::has_key_any(["name", "age"]);
    let results = g
        .v()
        .has_label("person")
        .limit(1)
        .properties()
        .append(anon)
        .to_list();
    assert_eq!(results.len(), 2);
}

#[test]
fn anon_has_prop_value() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::has_prop_value("Alice");
    let results = g
        .v()
        .has_label("person")
        .properties()
        .append(anon)
        .to_list();
    assert_eq!(results.len(), 1);
}

#[test]
fn anon_has_prop_value_any() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::has_prop_value_any(["Alice", "Bob"]);
    let results = g
        .v()
        .has_label("person")
        .properties()
        .append(anon)
        .to_list();
    assert_eq!(results.len(), 2);
}

#[test]
fn anon_where_p() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::where_p(p::between(25, 35));
    let results = g
        .v()
        .has_label("person")
        .values("age")
        .append(anon)
        .to_list();
    assert_eq!(results.len(), 2); // 25 and 30 are in range [25, 35)
}

// =============================================================================
// Transform Steps
// =============================================================================

#[test]
fn anon_values_multi() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::values_multi(["name", "age"]);
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 2);
}

#[test]
fn anon_properties() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::properties();
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 2); // name and age
}

#[test]
fn anon_properties_keys() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::properties_keys(&["name"]);
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
}

#[test]
fn anon_value_map() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::value_map();
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Value::Map(_)));
}

#[test]
fn anon_value_map_keys() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::value_map_keys(&["name"]);
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
    if let Value::Map(m) = &results[0] {
        assert!(m.contains_key("name"));
        assert!(!m.contains_key("age"));
    }
}

#[test]
fn anon_value_map_with_tokens() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::value_map_with_tokens();
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
    if let Value::Map(m) = &results[0] {
        assert!(m.contains_key("id"));
        assert!(m.contains_key("label"));
    }
}

#[test]
fn anon_element_map() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::element_map();
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
    if let Value::Map(m) = &results[0] {
        assert!(m.contains_key("id"));
        assert!(m.contains_key("label"));
    }
}

#[test]
fn anon_element_map_keys() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::element_map_keys(&["name"]);
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
    if let Value::Map(m) = &results[0] {
        assert!(m.contains_key("name"));
        assert!(!m.contains_key("age"));
    }
}

#[test]
fn anon_property_map() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::property_map();
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Value::Map(_)));
}

#[test]
fn anon_property_map_keys() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::property_map_keys(&["name"]);
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
}

#[test]
fn anon_unfold() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let list = Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
    let anon = __::unfold();
    let results = g.inject([list]).append(anon).to_list();
    assert_eq!(results.len(), 3);
}

#[test]
fn anon_mean() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::mean();
    let results = g
        .v()
        .has_label("person")
        .values("age")
        .append(anon)
        .to_list();
    assert_eq!(results.len(), 1);
    // (30 + 25 + 35) / 3 = 30
    assert_eq!(results[0], Value::Float(30.0));
}

#[test]
fn anon_id() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::id();
    let results = g.v().limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Value::Int(_)));
}

#[test]
fn anon_label() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::label();
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("person".to_string()));
}

#[test]
fn anon_key() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::key();
    let results = g
        .v()
        .has_label("person")
        .limit(1)
        .properties()
        .limit(1)
        .append(anon)
        .to_list();
    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Value::String(_)));
}

#[test]
fn anon_value() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::value();
    let results = g
        .v()
        .has_label("person")
        .limit(1)
        .properties()
        .has_key("name")
        .append(anon)
        .to_list();
    assert_eq!(results.len(), 1);
}

#[test]
fn anon_loops() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // loops() returns the current loop counter, which is 0 outside of repeat
    let anon = __::loops();
    let results = g.v().limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(0));
}

#[test]
fn anon_index() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::index();
    let results = g.inject([1i64, 2i64, 3i64]).append(anon).to_list();
    assert_eq!(results.len(), 3);
    // Each result should be a list [index, value]
    for r in &results {
        assert!(matches!(r, Value::List(_)));
    }
}

#[test]
fn anon_path() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::path();
    let results = g.v_ids([tg.alice]).with_path().out().append(anon).to_list();
    assert_eq!(results.len(), 2);
    for r in &results {
        assert!(matches!(r, Value::List(_)));
    }
}

#[test]
fn anon_as_and_select() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::as_("start").out().select(&["start"]);
    let results = g.v_ids([tg.alice]).with_path().append(anon).to_list();
    assert_eq!(results.len(), 2);
}

#[test]
fn anon_select_one() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::as_("x").out().select_one("x");
    let results = g.v_ids([tg.alice]).with_path().append(anon).to_list();
    assert_eq!(results.len(), 2);
    for r in &results {
        assert_eq!(r.as_vertex_id(), Some(tg.alice));
    }
}

#[test]
fn anon_flat_map() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::flat_map(|_ctx, v| {
        if let Value::Int(n) = v {
            (0..*n).map(Value::Int).collect()
        } else {
            vec![]
        }
    });
    let results = g.inject([3i64]).append(anon).to_list();
    assert_eq!(results.len(), 3);
}

// =============================================================================
// Filter Steps with Sub-traversals
// =============================================================================

#[test]
fn anon_where_subtraversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::where_(__::out());
    let results = g.v().append(anon).to_list();
    // All persons have outgoing edges
    assert!(results.len() >= 2);
}

#[test]
fn anon_not_subtraversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::not(__::out());
    let results = g.v().append(anon).to_list();
    // Software has no outgoing edges
    assert!(results.len() >= 1);
}

#[test]
fn anon_and_subtraversals() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::and_(vec![__::out(), __::in_()]);
    let results = g.v().append(anon).to_list();
    // Bob has both outgoing and incoming
    assert!(results.len() >= 1);
}

#[test]
fn anon_or_subtraversals() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::or_(vec![__::has_label("person"), __::in_()]);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 4); // All vertices match
}

// =============================================================================
// Branch Steps
// =============================================================================

#[test]
fn anon_union() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::union(vec![__::out(), __::in_()]);
    let results = g.v_ids([tg.bob]).append(anon).to_list();
    assert!(results.len() >= 2);
}

#[test]
fn anon_coalesce() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::coalesce(vec![__::values("nickname"), __::values("name")]);
    let results = g.v().has_label("person").append(anon).to_list();
    assert_eq!(results.len(), 3); // Falls back to name for all
}

#[test]
fn anon_choose() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::choose(
        __::has_label("person"),
        __::values("age"),
        __::values("name"),
    );
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 4); // 3 ages + 1 name
}

#[test]
fn anon_optional() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::optional(__::out_labels(&["knows"]));
    let results = g.v_ids([tg.alice]).append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn anon_local() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::local(__::out().limit(1));
    let results = g.v().has_label("person").append(anon).to_list();
    assert_eq!(results.len(), 3); // One neighbor per person
}

#[test]
fn anon_branch() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // branch() creates a step that can route based on label
    let anon = __::branch(__::label());
    let results = g.v().limit(1).append(anon).to_list();
    // Without options configured, branch returns empty
    assert_eq!(results.len(), 0);
}

// =============================================================================
// Mutation Steps (test creation, not execution)
// =============================================================================

#[test]
fn anon_add_v_creates_traversal() {
    // Just test that add_v creates a valid traversal
    let _anon = __::add_v("person");
    // The traversal compiles and is created - execution requires mutation executor
}

#[test]
fn anon_property_creates_traversal() {
    let _anon = __::property("name", "Alice");
    // The traversal compiles and is created - execution requires mutation executor
}

#[test]
fn anon_drop_creates_traversal() {
    let _anon = __::drop();
    // The traversal compiles and is created - execution requires mutation executor
}

// =============================================================================
// Side Effect Steps
// =============================================================================

#[test]
fn anon_store() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::store("x");
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 4);
}

#[test]
fn anon_aggregate() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::aggregate("all");
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 4);
}

#[test]
fn anon_cap() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::store("x").cap("x");
    let results = g.v().limit(2).append(anon).to_list();
    assert_eq!(results.len(), 1);
    if let Value::List(list) = &results[0] {
        assert_eq!(list.len(), 2);
    }
}

#[test]
fn anon_side_effect() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::side_effect(__::out().store("neighbors"));
    let results = g.v_ids([tg.alice]).append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn anon_profile() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::profile();
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 4);
}

#[test]
fn anon_profile_as() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::profile_as("my_profile");
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 4);
}

// =============================================================================
// Aggregate/Group Steps
// =============================================================================

#[test]
fn anon_order() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::order().build();
    let results = g.inject([3i64, 1i64, 2i64]).append(anon).to_list();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Int(1));
    assert_eq!(results[1], Value::Int(2));
    assert_eq!(results[2], Value::Int(3));
}

#[test]
fn anon_project() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::project(&["name", "age"])
        .by_key("name")
        .by_key("age")
        .build();
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
    if let Value::Map(m) = &results[0] {
        assert!(m.contains_key("name"));
        assert!(m.contains_key("age"));
    }
}

#[test]
fn anon_group() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::group().by_label().by_value().build();
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Value::Map(_)));
}

#[test]
fn anon_group_count() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::group_count().by_label().build();
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 1);
    if let Value::Map(m) = &results[0] {
        // Should have counts by label
        assert!(m.contains_key("person") || m.contains_key("software"));
    }
}

#[test]
fn anon_math() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::math("_ * 2").build();
    let results = g.inject([5.0f64]).append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Float(10.0));
}

// =============================================================================
// Navigation - Edge to Vertex
// =============================================================================

#[test]
fn anon_out_v() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::out_v();
    let results = g.v_ids([tg.alice]).out_e().append(anon).to_list();
    assert_eq!(results.len(), 2); // Source vertex of each edge
    for r in &results {
        assert_eq!(r.as_vertex_id(), Some(tg.alice));
    }
}

#[test]
fn anon_in_v() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::in_v();
    let results = g.v_ids([tg.alice]).out_e().append(anon).to_list();
    assert_eq!(results.len(), 2); // Target vertex of each edge
}

#[test]
fn anon_both_v() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::both_v();
    let results = g.v_ids([tg.alice]).out_e().limit(1).append(anon).to_list();
    assert_eq!(results.len(), 2); // Both vertices of edge
}

#[test]
fn anon_other_v() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::other_v();
    let results = g
        .v_ids([tg.alice])
        .with_path()
        .out_e()
        .append(anon)
        .to_list();
    assert_eq!(results.len(), 2); // The "other" vertex (targets)
}

// =============================================================================
// Additional Filter Functions
// =============================================================================

#[test]
fn anon_has() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::has("age");
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 3); // Only persons have age
}

#[test]
fn anon_has_value() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::has_value("name", "Alice");
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 1);
}

#[test]
fn anon_has_label_any() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::has_label_any(&["person", "software"]);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 4);
}

#[test]
fn anon_both() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::both();
    let results = g.v_ids([tg.bob]).append(anon).to_list();
    assert!(results.len() >= 2);
}

#[test]
fn anon_out_e() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::out_e();
    let results = g.v_ids([tg.alice]).append(anon).to_list();
    assert_eq!(results.len(), 2);
}

#[test]
fn anon_in_e() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::in_e();
    let results = g.v_ids([tg.bob]).append(anon).to_list();
    assert_eq!(results.len(), 1);
}

#[test]
fn anon_both_e() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::both_e();
    let results = g.v_ids([tg.bob]).append(anon).to_list();
    assert!(results.len() >= 2);
}

#[test]
fn anon_in_() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::in_();
    let results = g.v_ids([tg.bob]).append(anon).to_list();
    assert_eq!(results.len(), 1);
}
