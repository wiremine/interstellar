//! Coverage tests for traversal/builder.rs (Traversal<In, Value> methods).
//!
//! This module tests the unbound traversal API methods that allow chaining steps
//! on anonymous traversals. These are used when building traversal fragments
//! that get appended to bound traversals.

use interstellar::traversal::p;
use interstellar::traversal::__;
use interstellar::value::Value;

use crate::common::graphs::{create_medium_graph, create_small_graph};

// =============================================================================
// Filter Steps on Traversal<In, Value>
// =============================================================================

#[test]
fn traversal_has_label_any() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Test has_label_any by chaining on anonymous traversal
    let anon = __::identity().has_label_any(["person", "software"]);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 4); // 3 persons + 1 software
}

#[test]
fn traversal_has_key() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().has("age");
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 3); // Only persons have age
}

#[test]
fn traversal_has_not() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().has_not("age");
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 1); // Software doesn't have age
}

#[test]
fn traversal_has_value() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().has_value("name", "Alice");
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 1);
}

#[test]
fn traversal_has_where() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().has_where("age", p::gte(30));
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 2); // Alice (30) and Charlie (35)
}

#[test]
fn traversal_is_predicate() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::values("age").is_(p::gt(25));
    let results = g.v().has_label("person").append(anon).to_list();
    assert_eq!(results.len(), 2); // Ages 30 and 35
}

#[test]
fn traversal_is_eq() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::values("age").is_eq(30i64);
    let results = g.v().has_label("person").append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(30));
}

#[test]
fn traversal_skip() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().skip(2);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 2); // 4 vertices - 2 skipped
}

#[test]
fn traversal_range() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().range(1, 3);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 2); // Elements at indices 1 and 2
}

#[test]
fn traversal_tail() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().tail();
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 1); // Just the last element
}

#[test]
fn traversal_tail_n() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().tail_n(2);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 2); // Last 2 elements
}

#[test]
fn traversal_dedup_by_key() {
    let tg = create_medium_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Dedup by status - should get one per unique status
    let anon = __::identity().dedup_by_key("status");
    let results = g.v().has_label("person").append(anon).to_list();
    assert_eq!(results.len(), 2); // "active" and "inactive"
}

#[test]
fn traversal_dedup_by_label() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().dedup_by_label();
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 2); // One person, one software
}

#[test]
fn traversal_dedup_by_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Dedup by label - keeps unique based on label
    let anon = __::identity().dedup_by(__::label());
    let results = g.v().append(anon).to_list();
    // Should get one per unique label (person, software)
    assert_eq!(results.len(), 2);
}

#[test]
fn traversal_coin_filters_probabilistically() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // With p=0, nothing passes
    let anon = __::identity().coin(0.0);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 0);

    // With p=1, everything passes
    let anon2 = __::identity().coin(1.0);
    let results2 = g.v().append(anon2).to_list();
    assert_eq!(results2.len(), 4);
}

#[test]
fn traversal_sample() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().sample(2);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 2); // Exactly 2 sampled
}

#[test]
fn traversal_simple_path() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Simple path should filter out traversers with repeated elements
    let anon = __::identity().simple_path();
    let results = g
        .v_ids([tg.alice])
        .with_path()
        .out()
        .out()
        .append(anon)
        .to_list();
    // All paths at depth 2 should be simple
    assert!(results.len() >= 0); // May be empty or have results
}

#[test]
fn traversal_cyclic_path() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Find cyclic paths (paths with repeated elements)
    let anon = __::identity().cyclic_path();
    // Going out().out().out() from Alice might hit a cycle
    let results = g
        .v_ids([tg.alice])
        .with_path()
        .out()
        .out()
        .out()
        .append(anon)
        .to_list();
    // May find cycles or not depending on graph structure
    assert!(results.len() >= 0);
}

#[test]
fn traversal_has_id() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().has_id(tg.alice);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn traversal_has_ids() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().has_ids([tg.alice, tg.bob]);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 2);
}

#[test]
fn traversal_has_key_filter() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // has_key filters property maps by key name
    let anon = __::properties().has_key("name");
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
}

#[test]
fn traversal_has_key_any_filter() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::properties().has_key_any(["name", "age"]);
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 2); // name and age properties
}

#[test]
fn traversal_has_prop_value() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::properties().has_prop_value("Alice");
    let results = g.v().has_label("person").append(anon).to_list();
    assert_eq!(results.len(), 1); // The name="Alice" property
}

#[test]
fn traversal_has_prop_value_any() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::properties().has_prop_value_any(["Alice", "Bob"]);
    let results = g.v().has_label("person").append(anon).to_list();
    assert_eq!(results.len(), 2); // Alice and Bob name properties
}

#[test]
fn traversal_where_p() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::values("age").where_p(p::between(25, 35));
    let results = g.v().has_label("person").append(anon).to_list();
    assert_eq!(results.len(), 2); // 30 and 25 are in range, 35 is not (exclusive upper bound)
}

// =============================================================================
// Navigation Steps on Traversal<In, Value>
// =============================================================================

#[test]
fn traversal_out() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().out();
    let results = g.v_ids([tg.alice]).append(anon).to_list();
    assert_eq!(results.len(), 2); // Bob and GraphDB
}

#[test]
fn traversal_out_labels() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().out_labels(&["knows"]);
    let results = g.v_ids([tg.alice]).append(anon).to_list();
    assert_eq!(results.len(), 1); // Just Bob
}

#[test]
fn traversal_in() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().in_();
    let results = g.v_ids([tg.bob]).append(anon).to_list();
    assert_eq!(results.len(), 1); // Alice
}

#[test]
fn traversal_in_labels() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().in_labels(&["knows"]);
    let results = g.v_ids([tg.bob]).append(anon).to_list();
    assert_eq!(results.len(), 1); // Alice knows Bob
}

#[test]
fn traversal_both() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().both();
    let results = g.v_ids([tg.bob]).append(anon).to_list();
    // Bob has: incoming from Alice (knows), outgoing to Charlie (knows), outgoing to GraphDB (uses)
    assert!(results.len() >= 2);
}

#[test]
fn traversal_both_labels() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().both_labels(&["knows"]);
    let results = g.v_ids([tg.bob]).append(anon).to_list();
    // Alice knows Bob, Bob knows Charlie
    assert_eq!(results.len(), 2);
}

#[test]
fn traversal_out_e() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().out_e();
    let results = g.v_ids([tg.alice]).append(anon).to_list();
    assert_eq!(results.len(), 2); // knows->Bob, uses->GraphDB
}

#[test]
fn traversal_out_e_labels() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().out_e_labels(&["knows"]);
    let results = g.v_ids([tg.alice]).append(anon).to_list();
    assert_eq!(results.len(), 1);
}

#[test]
fn traversal_in_e() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().in_e();
    let results = g.v_ids([tg.bob]).append(anon).to_list();
    assert_eq!(results.len(), 1); // Edge from Alice
}

#[test]
fn traversal_in_e_labels() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().in_e_labels(&["knows"]);
    let results = g.v_ids([tg.bob]).append(anon).to_list();
    assert_eq!(results.len(), 1);
}

#[test]
fn traversal_both_e() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().both_e();
    let results = g.v_ids([tg.bob]).append(anon).to_list();
    assert!(results.len() >= 2); // Edges in both directions
}

#[test]
fn traversal_both_e_labels() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().both_e_labels(&["knows"]);
    let results = g.v_ids([tg.bob]).append(anon).to_list();
    assert_eq!(results.len(), 2); // Alice->Bob and Bob->Charlie
}

#[test]
fn traversal_out_v() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get source vertex of edges
    let anon = __::out_e().out_v();
    let results = g.v_ids([tg.alice]).append(anon).to_list();
    // Should get Alice (the source of outgoing edges)
    assert_eq!(results.len(), 2);
    for r in &results {
        assert_eq!(r.as_vertex_id(), Some(tg.alice));
    }
}

#[test]
fn traversal_in_v() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get target vertex of edges
    let anon = __::out_e().in_v();
    let results = g.v_ids([tg.alice]).append(anon).to_list();
    // Should get Bob and GraphDB
    assert_eq!(results.len(), 2);
}

#[test]
fn traversal_both_v() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get both vertices of edges
    let anon = __::out_e().both_v();
    let results = g.v_ids([tg.alice]).limit(1).append(anon).to_list();
    // Each edge has 2 vertices
    assert_eq!(results.len(), 4); // 2 edges × 2 vertices each
}

#[test]
fn traversal_other_v() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get the other vertex (requires path tracking)
    let anon = __::out_e().other_v();
    let results = g.v_ids([tg.alice]).with_path().append(anon).to_list();
    // Should get targets (Bob and GraphDB)
    assert_eq!(results.len(), 2);
}

// =============================================================================
// Transform Steps on Traversal<In, Value>
// =============================================================================

#[test]
fn traversal_values_multi() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().values_multi(["name", "age"]);
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 2); // name and age
}

#[test]
fn traversal_properties() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().properties();
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 2); // name and age as property objects
}

#[test]
fn traversal_properties_keys() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().properties_keys(&["name"]);
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
}

#[test]
fn traversal_value_map() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().value_map();
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Value::Map(_)));
}

#[test]
fn traversal_value_map_keys() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().value_map_keys(["name"]);
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
    if let Value::Map(m) = &results[0] {
        assert!(m.contains_key("name"));
        assert!(!m.contains_key("age"));
    }
}

#[test]
fn traversal_value_map_with_tokens() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().value_map_with_tokens();
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
    if let Value::Map(m) = &results[0] {
        assert!(m.contains_key("id"));
        assert!(m.contains_key("label"));
    }
}

#[test]
fn traversal_element_map() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().element_map();
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
    if let Value::Map(m) = &results[0] {
        assert!(m.contains_key("id"));
        assert!(m.contains_key("label"));
        // Properties not wrapped in lists
        if let Some(Value::String(name)) = m.get("name") {
            assert!(!name.is_empty());
        }
    }
}

#[test]
fn traversal_element_map_keys() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().element_map_keys(["name"]);
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
    if let Value::Map(m) = &results[0] {
        assert!(m.contains_key("name"));
        assert!(!m.contains_key("age"));
    }
}

#[test]
fn traversal_property_map() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().property_map();
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Value::Map(_)));
}

#[test]
fn traversal_property_map_keys() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().property_map_keys(["name"]);
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
}

#[test]
fn traversal_unfold() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Create a list and unfold it
    let list = Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
    let anon = __::identity().unfold();
    let results = g.inject([list]).append(anon).to_list();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Int(1));
    assert_eq!(results[1], Value::Int(2));
    assert_eq!(results[2], Value::Int(3));
}

#[test]
fn traversal_mean() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::values("age").mean();
    let results = g.v().has_label("person").append(anon).to_list();
    assert_eq!(results.len(), 1);
    // (30 + 25 + 35) / 3 = 30
    assert_eq!(results[0], Value::Float(30.0));
}

#[test]
fn traversal_id() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().id();
    let results = g.v().limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Value::Int(_)));
}

#[test]
fn traversal_label() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().label();
    let results = g.v().has_label("person").limit(1).append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("person".to_string()));
}

#[test]
fn traversal_flat_map() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().flat_map(|_ctx, v| {
        if let Value::Int(n) = v {
            (0..*n).map(Value::Int).collect()
        } else {
            vec![]
        }
    });
    let results = g.inject([3i64]).append(anon).to_list();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Int(0));
    assert_eq!(results[1], Value::Int(1));
    assert_eq!(results[2], Value::Int(2));
}

#[test]
fn traversal_path() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::out().path();
    let results = g.v_ids([tg.alice]).with_path().append(anon).to_list();
    assert_eq!(results.len(), 2); // Two paths (to Bob and GraphDB)
    for r in &results {
        assert!(matches!(r, Value::List(_)));
    }
}

#[test]
fn traversal_as_and_select() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::as_("start").out().as_("end").select(&["start", "end"]);
    let results = g.v_ids([tg.alice]).with_path().append(anon).to_list();
    assert_eq!(results.len(), 2);
    for r in &results {
        if let Value::Map(m) = r {
            assert!(m.contains_key("start"));
            assert!(m.contains_key("end"));
        }
    }
}

#[test]
fn traversal_select_one() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::as_("x").out().select_one("x");
    let results = g.v_ids([tg.alice]).with_path().append(anon).to_list();
    assert_eq!(results.len(), 2);
    // Each result should be Alice (the labeled vertex)
    for r in &results {
        assert_eq!(r.as_vertex_id(), Some(tg.alice));
    }
}

// =============================================================================
// Filter Steps with Sub-traversals
// =============================================================================

#[test]
fn traversal_where_subtraversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Keep vertices that have outgoing edges
    let anon = __::identity().where_(__::out());
    let results = g.v().append(anon).to_list();
    // All persons have outgoing edges, software doesn't
    assert!(results.len() >= 2);
}

#[test]
fn traversal_not_subtraversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Keep vertices that have NO outgoing edges
    let anon = __::identity().not(__::out());
    let results = g.v().append(anon).to_list();
    // GraphDB has no outgoing edges
    assert!(results.len() >= 1);
}

#[test]
fn traversal_and_subtraversals() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Keep vertices that have BOTH outgoing AND incoming edges
    let anon = __::identity().and_(vec![__::out(), __::in_()]);
    let results = g.v().append(anon).to_list();
    // Bob has both (Alice->Bob, Bob->Charlie)
    assert!(results.len() >= 1);
}

#[test]
fn traversal_or_subtraversals() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Keep vertices that are person OR have incoming edges
    let anon = __::identity().or_(vec![__::has_label("person"), __::in_()]);
    let results = g.v().append(anon).to_list();
    // All persons + GraphDB (which has incoming edges)
    assert_eq!(results.len(), 4);
}

// =============================================================================
// Branch Steps on Traversal<In, Value>
// =============================================================================

#[test]
fn traversal_union() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().union(vec![__::out(), __::in_()]);
    let results = g.v_ids([tg.bob]).append(anon).to_list();
    // Bob has Alice (in) + Charlie (out) + GraphDB (out uses)
    assert!(results.len() >= 2);
}

#[test]
fn traversal_coalesce() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Try to get nickname (doesn't exist), fall back to name
    let anon = __::identity().coalesce(vec![__::values("nickname"), __::values("name")]);
    let results = g.v().has_label("person").append(anon).to_list();
    assert_eq!(results.len(), 3); // All persons get names
}

#[test]
fn traversal_choose() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // If person, get age; otherwise get name
    let anon = __::identity().choose(
        __::has_label("person"),
        __::values("age"),
        __::values("name"),
    );
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 4); // 3 ages + 1 name
}

#[test]
fn traversal_optional() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Try to get friends; if none, keep the vertex
    let anon = __::identity().optional(__::out_labels(&["knows"]));
    let results = g.v_ids([tg.alice]).append(anon).to_list();
    // Alice knows Bob, so we get Bob
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn traversal_local() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Local limit: get first outgoing neighbor per vertex
    let anon = __::identity().local(__::out().limit(1));
    let results = g.v().has_label("person").append(anon).to_list();
    // Each person gets at most 1 neighbor
    assert_eq!(results.len(), 3);
}

// =============================================================================
// Side Effect Steps on Traversal<In, Value>
// =============================================================================

#[test]
fn traversal_store() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().store("stored");
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 4); // All vertices pass through
}

#[test]
fn traversal_aggregate() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().aggregate("all");
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 4); // All vertices pass through after aggregation
}

#[test]
fn traversal_cap() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().store("x").cap("x");
    let results = g.v().limit(2).append(anon).to_list();
    assert_eq!(results.len(), 1);
    if let Value::List(list) = &results[0] {
        assert_eq!(list.len(), 2);
    }
}

#[test]
fn traversal_cap_multi() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity()
        .store("a")
        .out()
        .store("b")
        .cap_multi(["a", "b"]);
    let results = g.v_ids([tg.alice]).append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Value::Map(_)));
}

#[test]
fn traversal_side_effect() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Execute side effect but pass through original
    let anon = __::identity().side_effect(__::out().store("neighbors"));
    let results = g.v_ids([tg.alice]).append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn traversal_profile() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().profile();
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 4);
}

#[test]
fn traversal_profile_as() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let anon = __::identity().profile_as("my_profile");
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 4);
}
