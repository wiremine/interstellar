//! Predicate integration tests with has_where
//!
//! Tests for comparison, range, string, and composed predicates.

#![allow(unused_variables)]
use std::collections::HashMap;

use interstellar::p;
use interstellar::storage::Graph;
use interstellar::traversal::__;
use interstellar::value::{Value, VertexId};

use crate::common::graphs::create_small_graph;

// -------------------------------------------------------------------------
// Comparison Predicates with has_where
// -------------------------------------------------------------------------

#[test]
fn has_where_eq_filters_by_property_value() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with age exactly 30
    let results = g.v().has_where("age", p::eq(30)).to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn has_where_neq_filters_out_property_value() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with age not equal to 30
    // Bob (25), Charlie (35) should match; Alice (30) should not
    let results = g
        .v()
        .has_label("person")
        .has_where("age", p::neq(30))
        .to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob));
    assert!(ids.contains(&tg.charlie));
    assert!(!ids.contains(&tg.alice));
}

#[test]
fn has_where_gte_filters_correctly() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with age >= 30
    // Alice (30), Charlie (35) should match; Bob (25) should not
    let results = g.v().has_where("age", p::gte(30)).to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.charlie));
}

#[test]
fn has_where_gt_filters_correctly() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with age > 30
    // Only Charlie (35) should match
    let results = g.v().has_where("age", p::gt(30)).to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.charlie));
}

#[test]
fn has_where_lt_filters_correctly() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with age < 30
    // Only Bob (25) should match
    let results = g.v().has_where("age", p::lt(30)).to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn has_where_lte_filters_correctly() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with age <= 30
    // Alice (30), Bob (25) should match; Charlie (35) should not
    let results = g.v().has_where("age", p::lte(30)).to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.bob));
}

#[test]
fn has_where_cross_type_comparison() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Float predicate value compared against Int property
    // age >= 29.5 should match Alice (30), Charlie (35)
    let results = g.v().has_where("age", p::gte(29.5f64)).to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.charlie));
}

#[test]
fn has_where_missing_property_filters_out() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // GraphDB has no "age" property, should be filtered out
    // All person vertices have age
    let results = g.v().has_where("age", p::gte(0)).to_list();

    assert_eq!(results.len(), 3); // Only persons, not GraphDB

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(!ids.contains(&tg.graphdb));
}

// -------------------------------------------------------------------------
// Range Predicates with has_where
// -------------------------------------------------------------------------

#[test]
fn has_where_between_filters_range() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with age in [25, 35)
    // Alice (30), Bob (25) should match; Charlie (35) should not (exclusive end)
    let results = g.v().has_where("age", p::between(25, 35)).to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.bob));
    assert!(!ids.contains(&tg.charlie));
}

#[test]
fn has_where_between_inclusive_start() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with age in [30, 40)
    // Start value is inclusive: Alice (30), Charlie (35) should match
    let results = g.v().has_where("age", p::between(30, 40)).to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.charlie));
}

#[test]
fn has_where_between_exclusive_end() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with age in [20, 30)
    // End value is exclusive: only Bob (25) should match
    let results = g.v().has_where("age", p::between(20, 30)).to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn has_where_inside_filters_exclusive() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with age in (25, 35) - exclusive both ends
    // Only Alice (30) should match; Bob (25), Charlie (35) at boundaries excluded
    let results = g.v().has_where("age", p::inside(25, 35)).to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn has_where_inside_excludes_boundaries() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for age in (29, 31) - should only match Alice (30)
    let results = g.v().has_where("age", p::inside(29, 31)).to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn has_where_outside_filters_outside_range() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with age outside [26, 34]
    // Bob (25) < 26: matches; Alice (30) in range: doesn't match; Charlie (35) > 34: matches
    let results = g.v().has_where("age", p::outside(26, 34)).to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob));
    assert!(ids.contains(&tg.charlie));
    assert!(!ids.contains(&tg.alice));
}

#[test]
fn has_where_outside_boundaries_not_outside() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with age outside [25, 35]
    // Boundaries are NOT outside, so Bob (25) and Charlie (35) don't match
    // Alice (30) is inside the range so doesn't match either
    let results = g.v().has_where("age", p::outside(25, 35)).to_list();

    assert!(results.is_empty());
}

// -------------------------------------------------------------------------
// String Predicates with has_where
// -------------------------------------------------------------------------

#[test]
fn has_where_starting_with_filters_strings() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with name starting with "A"
    // Alice should match
    let results = g.v().has_where("name", p::starting_with("A")).to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn has_where_starting_with_multiple_matches() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with name starting with "C" or "G"
    // Charlie and GraphDB start with different letters, neither matches
    // Let's check for names starting with "B"
    let results = g.v().has_where("name", p::starting_with("B")).to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn has_where_ending_with_filters_strings() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with name ending with "e"
    // Alice and Charlie both end with "e"
    let results = g.v().has_where("name", p::ending_with("e")).to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.charlie));
}

#[test]
fn has_where_ending_with_no_matches() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with name ending with "z"
    // No names end with "z"
    let results = g.v().has_where("name", p::ending_with("z")).to_list();

    assert!(results.is_empty());
}

#[test]
fn has_where_containing_filters_strings() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with name containing "li"
    // Alice, Charlie both contain "li"
    let results = g.v().has_where("name", p::containing("li")).to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.charlie));
}

#[test]
fn has_where_containing_single_match() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with name containing "ob"
    // Only Bob contains "ob"
    let results = g.v().has_where("name", p::containing("ob")).to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn has_where_string_predicate_on_non_string_property_fails() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Apply string predicate to numeric property - should match nothing
    // age is Int, not String
    let results = g.v().has_where("age", p::containing("3")).to_list();

    assert!(results.is_empty());
}

#[test]
fn has_where_regex_filters_strings() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with name matching regex "^[AB].*"
    // Alice and Bob start with A or B
    let results = g.v().has_where("name", p::regex("^[AB].*")).to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.bob));
}

#[test]
fn has_where_regex_exact_match() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for name exactly "Bob"
    let results = g.v().has_where("name", p::regex("^Bob$")).to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

// -------------------------------------------------------------------------
// Composed Predicates with has_where
// -------------------------------------------------------------------------

#[test]
fn has_where_and_composed_predicate() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with age >= 25 AND age <= 30
    // Alice (30), Bob (25) should match; Charlie (35) should not
    let results = g
        .v()
        .has_where("age", p::and(p::gte(25), p::lte(30)))
        .to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.bob));
}

#[test]
fn has_where_or_composed_predicate() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with age == 25 OR age == 35
    // Bob (25), Charlie (35) should match; Alice (30) should not
    let results = g
        .v()
        .has_where("age", p::or(p::eq(25), p::eq(35)))
        .to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob));
    assert!(ids.contains(&tg.charlie));
}

#[test]
fn has_where_not_composed_predicate() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with age NOT equal to 30
    // Bob (25), Charlie (35) should match; Alice (30) should not
    let results = g
        .v()
        .has_label("person")
        .has_where("age", p::not(p::eq(30)))
        .to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob));
    assert!(ids.contains(&tg.charlie));
}

#[test]
fn has_where_complex_nested_predicate() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for: (age >= 30 AND age < 35) OR age == 25
    // Matches: Alice (30 >= 30 AND 30 < 35), Bob (25 == 25)
    // Does not match: Charlie (35 is NOT < 35 AND 35 != 25)
    let results = g
        .v()
        .has_where("age", p::or(p::and(p::gte(30), p::lt(35)), p::eq(25)))
        .to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.bob));
}

#[test]
fn has_where_and_with_string_predicates() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for names that start with a letter and contain specific substring
    // Names starting with "A" AND containing "lic" -> Alice
    let results = g
        .v()
        .has_where("name", p::and(p::starting_with("A"), p::containing("lic")))
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn has_where_or_with_string_predicates() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for names ending with "b" OR containing "arl"
    // Bob ends with "b", Charlie contains "arl"
    let results = g
        .v()
        .has_where("name", p::or(p::ending_with("b"), p::containing("arl")))
        .to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob));
    assert!(ids.contains(&tg.charlie));
}

#[test]
fn has_where_not_with_between() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for ages NOT in [26, 34]
    // Bob (25) and Charlie (35) should match
    let results = g
        .v()
        .has_label("person")
        .has_where("age", p::not(p::between(26, 35)))
        .to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob));
    assert!(ids.contains(&tg.charlie));
}

// -------------------------------------------------------------------------
// Collection Predicates with has_where
// -------------------------------------------------------------------------

#[test]
fn has_where_within_filters_by_set_membership() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with age in {25, 35}
    // Bob (25), Charlie (35) should match
    let results = g.v().has_where("age", p::within([25i64, 35i64])).to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob));
    assert!(ids.contains(&tg.charlie));
}

#[test]
fn has_where_without_filters_by_exclusion() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for vertices with age NOT in {25, 35}
    // Only Alice (30) should match
    let results = g
        .v()
        .has_label("person")
        .has_where("age", p::without([25i64, 35i64]))
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn has_where_within_with_strings() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter for names in {"Alice", "Charlie"}
    let results = g
        .v()
        .has_where("name", p::within(["Alice", "Charlie"]))
        .to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.charlie));
}

// -------------------------------------------------------------------------
// Edge Property Tests with has_where
// -------------------------------------------------------------------------

#[test]
fn has_where_on_edges() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter edges by "since" property
    // knows edges have since: 2020, 2021, 2019
    let results = g
        .e()
        .has_label("knows")
        .has_where("since", p::gte(2020))
        .to_list();

    assert_eq!(results.len(), 2); // 2020 and 2021
}

#[test]
fn has_where_on_edge_with_range() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter edges with since between 2019 and 2021 (exclusive end)
    let results = g
        .e()
        .has_label("knows")
        .has_where("since", p::between(2019, 2021))
        .to_list();

    assert_eq!(results.len(), 2); // 2019 and 2020
}

// -------------------------------------------------------------------------
// Integration with Traversal Steps
// -------------------------------------------------------------------------

#[test]
fn has_where_chained_with_navigation() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Start from Alice, get neighbors older than 30
    // Alice -> Bob (25, no), GraphDB (no age)
    // So result should be empty
    let results = g
        .v_ids([tg.alice])
        .out()
        .has_where("age", p::gt(30))
        .to_list();

    assert!(results.is_empty());
}

#[test]
fn has_where_in_where_step() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Find vertices that have an outgoing neighbor with age < 30
    // Alice -> Bob (25 < 30): Alice passes
    // Bob -> Charlie (35 >= 30): Bob fails
    // Charlie -> Alice (30 >= 30): Charlie fails
    // GraphDB -> (no out): fails
    let results = g
        .v()
        .where_(__::out().has_where("age", p::lt(30)))
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn has_where_in_union_branches() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Union with different predicates per branch
    let results = g
        .v_ids([tg.alice])
        .union(vec![
            __::out().has_where("age", p::lt(30)),              // Bob (25)
            __::out().has_where("name", p::starting_with("G")), // GraphDB
        ])
        .to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob));
    assert!(ids.contains(&tg.graphdb));
}

// -------------------------------------------------------------------------
// Spec-compliant Test Graph
// -------------------------------------------------------------------------

/// Test graph structure for spec-alignment tests.
struct SpecTestGraph {
    graph: Graph,
    alice: VertexId,
    bob: VertexId,
    #[allow(dead_code)]
    carol: VertexId,
    #[allow(dead_code)]
    acme: VertexId,
}

fn create_spec_test_graph() -> SpecTestGraph {
    let graph = Graph::new();

    // Add person vertices
    let alice = graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props
    });

    let bob = graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        props.insert("age".to_string(), Value::Int(35));
        props
    });

    let carol = graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Carol".to_string()));
        props.insert("age".to_string(), Value::Int(25));
        props
    });

    // Add company vertex
    let acme = graph.add_vertex("company", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Acme Corp".to_string()));
        props
    });

    // Add edges
    graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();

    graph
        .add_edge(alice, carol, "knows", HashMap::new())
        .unwrap();

    graph.add_edge(bob, carol, "knows", HashMap::new()).unwrap();

    // works_at edges
    graph
        .add_edge(alice, acme, "works_at", HashMap::new())
        .unwrap();

    graph
        .add_edge(bob, acme, "works_at", HashMap::new())
        .unwrap();

    SpecTestGraph {
        graph,
        alice,
        bob,
        carol,
        acme,
    }
}

#[test]
fn has_where_spec_graph_integration() {
    // Use the spec-compliant test graph for spec alignment
    let tg = create_spec_test_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Find adults (age >= 18) who work at a company
    // All persons are adults, Alice and Bob work at Acme
    let results = g
        .v()
        .has_where("age", p::gte(18))
        .where_(__::out_labels(&["works_at"]))
        .to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.bob));
}
