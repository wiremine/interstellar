//! Conditional and branching pattern tests.
//!
//! Tests for branching traversal patterns including:
//! - choose() for conditional branching
//! - coalesce() for fallback patterns
//! - union() for merging traversals
//! - optional() for optional steps
//! - and_()/or_() for logical combinations

#![allow(unused_variables)]

use std::collections::HashMap;

use interstellar::p;
use interstellar::storage::Graph;
use interstellar::traversal::__;
use interstellar::value::Value;

use crate::common::graphs::{create_medium_graph, create_small_graph};

// =============================================================================
// Choose (If-Then-Else)
// =============================================================================

#[test]
fn choose_with_predicate_true_branch() {
    let tg = create_medium_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // If person has status "active", go to knows, else go to created
    let results = g
        .v()
        .has_label("person")
        .choose(
            __.has_value("status", "active"),
            __.out_labels(&["knows"]),
            __.out_labels(&["created"]),
        )
        .to_list();

    // Alice and Charlie are active, Bob is inactive
    assert!(!results.is_empty());
}

#[test]
fn choose_with_predicate_false_branch() {
    let tg = create_medium_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Start from Bob (inactive) specifically
    let results = g
        .v_ids([tg.bob])
        .choose(
            __.has_value("status", "active"),
            __.out_labels(&["knows"]),
            __.out_labels(&["created"]),
        )
        .to_list();

    // Bob is inactive, so should follow "created" branch
    // Bob created Redis
    assert!(!results.is_empty());
}

#[test]
fn choose_with_constant_branches() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Return different constants based on label
    let results = g
        .v()
        .choose(
            __.has_label("person"),
            __.constant(Value::String("is_person".to_string())),
            __.constant(Value::String("is_other".to_string())),
        )
        .to_list();

    assert_eq!(results.len(), 4);
    let person_count = results
        .iter()
        .filter(|v| *v == &Value::String("is_person".to_string()))
        .count();
    assert_eq!(person_count, 3);
}

#[test]
fn choose_with_value_extraction() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Extract different properties based on label
    let results = g
        .v()
        .choose(
            __.has_label("person"),
            __.values("age"),
            __.values("version"),
        )
        .to_list();

    // 3 ages + 1 version
    assert_eq!(results.len(), 4);
}

// =============================================================================
// Coalesce (First Non-Empty)
// =============================================================================

#[test]
fn coalesce_returns_first_match() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Try to get nickname (doesn't exist), fall back to name
    let results = g
        .v()
        .has_label("person")
        .coalesce(vec![__.values("nickname"), __.values("name")])
        .to_list();

    // No nickname exists, so all should be names
    assert_eq!(results.len(), 3);
    assert!(results.contains(&Value::String("Alice".to_string())));
}

#[test]
fn coalesce_with_constant_fallback() {
    let graph = Graph::new();
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("HasName".to_string()));
        props
    });
    graph.add_vertex("test", HashMap::new()); // No name

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let results = g
        .v()
        .coalesce(vec![
            __.values("name"),
            __.constant(Value::String("Unknown".to_string())),
        ])
        .to_list();

    assert_eq!(results.len(), 2);
    assert!(results.contains(&Value::String("HasName".to_string())));
    assert!(results.contains(&Value::String("Unknown".to_string())));
}

#[test]
fn coalesce_with_navigation() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Try "manages" edges first, fall back to "knows"
    let results = g
        .v_ids([tg.alice])
        .coalesce(vec![__.out_labels(&["manages"]), __.out_labels(&["knows"])])
        .to_list();

    // No "manages" edges, so should return knows (Bob)
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn coalesce_all_empty_returns_empty() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // All traversals produce empty
    let results = g
        .v_ids([tg.graphdb])
        .coalesce(vec![__.out_labels(&["manages"]), __.out_labels(&["owns"])])
        .to_list();

    assert!(results.is_empty());
}

// =============================================================================
// Union (Merge Multiple Traversals)
// =============================================================================

#[test]
fn union_combines_traversals() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get both knows and uses edges from Alice
    let results = g
        .v_ids([tg.alice])
        .union(vec![__.out_labels(&["knows"]), __.out_labels(&["uses"])])
        .to_list();

    // Bob (knows) + GraphDB (uses)
    assert_eq!(results.len(), 2);
}

#[test]
fn union_with_different_depths() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Combine 1-hop and 2-hop traversals
    let results = g
        .v_ids([tg.alice])
        .union(vec![
            __.out_labels(&["knows"]),
            __.out_labels(&["knows"]).out_labels(&["knows"]),
        ])
        .dedup()
        .to_list();

    // Bob (1-hop) and Charlie (2-hop)
    assert_eq!(results.len(), 2);
}

#[test]
fn union_with_values() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get both name and age from person
    let results = g
        .v_ids([tg.alice])
        .union(vec![__.values("name"), __.values("age")])
        .to_list();

    assert_eq!(results.len(), 2);
    assert!(results.contains(&Value::String("Alice".to_string())));
    assert!(results.contains(&Value::Int(30)));
}

#[test]
fn union_preserves_duplicates() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Both branches return the same vertex
    let results = g
        .v_ids([tg.alice])
        .union(vec![__.out_labels(&["knows"]), __.out_labels(&["knows"])])
        .to_list();

    // Should have Bob twice (no automatic dedup)
    assert_eq!(results.len(), 2);
}

// =============================================================================
// Optional (May or May Not Match)
// =============================================================================

#[test]
fn optional_with_match() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Optional knows traversal (exists for Alice)
    let results = g
        .v_ids([tg.alice])
        .optional(__.out_labels(&["knows"]))
        .to_list();

    // Should return Bob (the match)
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn optional_without_match_returns_input() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Optional manages traversal (doesn't exist)
    let results = g
        .v_ids([tg.alice])
        .optional(__.out_labels(&["manages"]))
        .to_list();

    // Should return Alice (the input)
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn optional_in_chain() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Chain with optional in the middle
    let results = g
        .v_ids([tg.alice])
        .out_labels(&["knows"])
        .optional(__.out_labels(&["manages"]))
        .values("name")
        .to_list();

    // Bob has no manages, so Bob is returned, then name extracted
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Bob".to_string()));
}

// =============================================================================
// And/Or Logical Combinations
// =============================================================================

#[test]
fn and_requires_all_conditions() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Person with age > 25 AND outgoing knows edge
    let results = g
        .v()
        .has_label("person")
        .and_(vec![
            __.has_where("age", p::gt(25i64)),
            __.out_labels(&["knows"]),
        ])
        .to_list();

    // Alice (30, knows Bob) and Charlie (35, knows Alice) - but need to check
    // Actually: Alice (30) has knows, Charlie (35) has knows
    assert!(results.len() >= 2);
}

#[test]
fn and_with_empty_branch_filters_all() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Condition that can't be satisfied
    let results = g
        .v()
        .has_label("person")
        .and_(vec![
            __.has_where("age", p::gt(100i64)),
            __.out_labels(&["knows"]),
        ])
        .to_list();

    // No one is over 100
    assert!(results.is_empty());
}

#[test]
fn or_matches_any_condition() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Person with age < 26 OR age > 34
    let results = g
        .v()
        .has_label("person")
        .or_(vec![
            __.has_where("age", p::lt(26i64)),
            __.has_where("age", p::gt(34i64)),
        ])
        .to_list();

    // Bob (25) and Charlie (35)
    assert_eq!(results.len(), 2);
}

#[test]
fn or_with_navigation_conditions() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Vertices with knows OR uses outgoing edges
    let results = g
        .v()
        .or_(vec![__.out_labels(&["knows"]), __.out_labels(&["uses"])])
        .to_list();

    // Alice, Bob, Charlie have knows; Alice, Bob have uses
    assert!(!results.is_empty());
}

// =============================================================================
// Not (Negation)
// =============================================================================

#[test]
fn not_filters_matching() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Vertices that do NOT have outgoing "knows" edges
    let results = g.v().not(__.out_labels(&["knows"])).to_list();

    // GraphDB has no outgoing edges
    assert!(!results.is_empty());
    // Should include GraphDB
    let ids: Vec<_> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.graphdb));
}

#[test]
fn not_with_property_condition() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // People who do NOT have age > 30
    let results = g
        .v()
        .has_label("person")
        .not(__.has_where("age", p::gt(30i64)))
        .to_list();

    // Alice (30) and Bob (25) - not greater than 30
    assert_eq!(results.len(), 2);
}

// =============================================================================
// Where Subtraversal
// =============================================================================

#[test]
fn where_with_exists_pattern() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // People who know someone
    let results = g
        .v()
        .has_label("person")
        .where_(__.out_labels(&["knows"]))
        .to_list();

    // Alice, Bob, Charlie all have outgoing knows
    assert_eq!(results.len(), 3);
}

#[test]
fn where_with_count_predicate() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // People who know at least 1 person
    let results = g
        .v()
        .has_label("person")
        .where_(__.out_labels(&["knows"]))
        .to_list();

    assert!(!results.is_empty());
}

#[test]
fn where_with_property_match() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // People who know someone younger than 30
    let results = g
        .v()
        .has_label("person")
        .where_(__.out_labels(&["knows"]).has_where("age", p::lt(30i64)))
        .to_list();

    // Alice knows Bob (25)
    assert!(!results.is_empty());
}

// =============================================================================
// Complex Conditional Patterns
// =============================================================================

#[test]
fn nested_choose() {
    let tg = create_medium_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Nested conditional logic
    let results = g
        .v()
        .has_label("person")
        .choose(
            __.has_value("status", "active"),
            __.choose(
                __.has_where("age", p::gt(30i64)),
                __.constant(Value::String("active_senior".to_string())),
                __.constant(Value::String("active_junior".to_string())),
            ),
            __.constant(Value::String("inactive".to_string())),
        )
        .to_list();

    assert_eq!(results.len(), 3);
}

#[test]
fn coalesce_in_union() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Complex combination
    let results = g
        .v_ids([tg.alice])
        .union(vec![
            __.out_labels(&["knows"]),
            __.coalesce(vec![__.out_labels(&["manages"]), __.out_labels(&["uses"])]),
        ])
        .to_list();

    // knows -> Bob, coalesce -> uses -> GraphDB
    assert_eq!(results.len(), 2);
}
