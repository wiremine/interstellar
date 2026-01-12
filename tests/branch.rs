//! Integration tests for branch and choose-option steps.
//!
//! These tests verify multi-way branching functionality including:
//! - `branch()` with label-based routing
//! - `branch()` with property-based routing
//! - `option_none()` default branch
//! - Filtering when no match and no default
//! - `choose_by()` equivalence to `branch()`
//! - Various key types (string, int, bool)
//! - Path preservation through branch

use std::collections::HashMap;

use intersteller::graph::Graph;
use intersteller::storage::InMemoryGraph;
use intersteller::traversal::__;
use intersteller::value::{Value, VertexId};

// =============================================================================
// Test Graph Setup
// =============================================================================

/// Test graph with vertex IDs for use in tests.
#[allow(dead_code)]
struct TestGraph {
    graph: Graph,
    alice: VertexId,
    bob: VertexId,
    charlie: VertexId,
    graphdb: VertexId,
    redis: VertexId,
}

/// Creates a test graph with:
/// - 5 vertices: Alice (person), Bob (person), Charlie (person), GraphDB (software), Redis (software)
/// - Multiple edges for routing tests
///
/// Graph structure:
/// ```text
///     Alice ----knows----> Bob ----knows----> Charlie
///       |                   |                   
///       |                   |                   
///     created             created              
///       |                   |                   
///       v                   v                   
///     GraphDB              Redis               
/// ```
fn create_test_graph() -> TestGraph {
    let mut storage = InMemoryGraph::new();

    // Add person vertices
    let alice = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props.insert("status".to_string(), Value::String("active".to_string()));
        props.insert("priority".to_string(), Value::Int(1));
        props
    });

    let bob = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        props.insert("age".to_string(), Value::Int(25));
        props.insert("status".to_string(), Value::String("inactive".to_string()));
        props.insert("priority".to_string(), Value::Int(2));
        props
    });

    let charlie = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Charlie".to_string()));
        props.insert("age".to_string(), Value::Int(35));
        props.insert("status".to_string(), Value::String("active".to_string()));
        props.insert("priority".to_string(), Value::Int(1));
        props
    });

    // Add software vertices
    let graphdb = storage.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("GraphDB".to_string()));
        props.insert("version".to_string(), Value::Float(2.0));
        props.insert("priority".to_string(), Value::Int(3));
        props
    });

    let redis = storage.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Redis".to_string()));
        props.insert("version".to_string(), Value::Float(7.0));
        props.insert("priority".to_string(), Value::Int(2));
        props
    });

    // Add edges
    // Alice knows Bob
    storage
        .add_edge(alice, bob, "knows", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2020));
            props
        })
        .unwrap();

    // Bob knows Charlie
    storage
        .add_edge(bob, charlie, "knows", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2021));
            props
        })
        .unwrap();

    // Alice created GraphDB
    storage
        .add_edge(alice, graphdb, "created", {
            let mut props = HashMap::new();
            props.insert("year".to_string(), Value::Int(2019));
            props
        })
        .unwrap();

    // Bob created Redis
    storage
        .add_edge(bob, redis, "created", {
            let mut props = HashMap::new();
            props.insert("year".to_string(), Value::Int(2020));
            props
        })
        .unwrap();

    TestGraph {
        graph: Graph::new(storage),
        alice,
        bob,
        charlie,
        graphdb,
        redis,
    }
}

// =============================================================================
// Branch with Label-based Routing
// =============================================================================

#[test]
fn test_branch_routes_by_label() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Route based on vertex label:
    // - person vertices: follow "knows" edges
    // - software vertices: follow incoming "created" edges
    let results = g
        .v()
        .branch(__::label())
        .option("person", __::out_labels(&["knows"]))
        .option("software", __::in_labels(&["created"]))
        .values("name")
        .to_list();

    // Person vertices (Alice, Bob, Charlie) should produce Bob, Charlie (from knows edges)
    // Software vertices (GraphDB, Redis) should produce Alice, Bob (from incoming created edges)
    assert!(results.contains(&Value::String("Bob".to_string())));
    assert!(results.contains(&Value::String("Charlie".to_string())));
    assert!(results.contains(&Value::String("Alice".to_string())));

    // Alice appears from: GraphDB->in(created)->Alice
    // Bob appears from: Alice->out(knows)->Bob and Redis->in(created)->Bob
    // Charlie appears from: Bob->out(knows)->Charlie
    assert_eq!(results.len(), 4); // Bob, Charlie, Alice, Bob
}

#[test]
fn test_branch_with_identity_option() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Route by label, use identity for persons to keep them
    let results = g
        .v()
        .branch(__::label())
        .option("person", __::identity())
        .option("software", __::identity())
        .values("name")
        .to_list();

    // All vertices should be returned via identity
    assert_eq!(results.len(), 5);
    assert!(results.contains(&Value::String("Alice".to_string())));
    assert!(results.contains(&Value::String("Bob".to_string())));
    assert!(results.contains(&Value::String("Charlie".to_string())));
    assert!(results.contains(&Value::String("GraphDB".to_string())));
    assert!(results.contains(&Value::String("Redis".to_string())));
}

// =============================================================================
// Branch with Property-based Routing
// =============================================================================

#[test]
fn test_branch_with_property_value_key() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Route by status property
    let results = g
        .v()
        .has_label("person")
        .branch(__::values("status"))
        .option("active", __::out_labels(&["knows"]))
        .option("inactive", __::identity())
        .values("name")
        .to_list();

    // Alice (active) -> out(knows) -> Bob
    // Bob (inactive) -> identity -> Bob
    // Charlie (active) -> out(knows) -> (no outgoing knows edges)
    assert!(results.contains(&Value::String("Bob".to_string())));
    // Bob appears twice: from Alice's knows edge and from Bob's identity
    let bob_count = results
        .iter()
        .filter(|v| **v == Value::String("Bob".to_string()))
        .count();
    assert_eq!(bob_count, 2);
}

// =============================================================================
// Option None (Default Branch)
// =============================================================================

#[test]
fn test_branch_with_none_default() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Route by label, with default for unknown labels
    let results = g
        .v()
        .branch(__::label())
        .option("person", __::values("name"))
        .option_none(__::constant(Value::String("other".to_string())))
        .to_list();

    // Person vertices get their names
    assert!(results.contains(&Value::String("Alice".to_string())));
    assert!(results.contains(&Value::String("Bob".to_string())));
    assert!(results.contains(&Value::String("Charlie".to_string())));

    // Software vertices (GraphDB, Redis) get "other" via option_none
    let other_count = results
        .iter()
        .filter(|v| **v == Value::String("other".to_string()))
        .count();
    assert_eq!(other_count, 2);
}

#[test]
fn test_branch_none_branch_used_when_no_match() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Only handle "person", use default for everything else
    let results = g
        .v()
        .branch(__::label())
        .option("person", __::values("age"))
        .option_none(__::values("version"))
        .to_list();

    // Persons get age values: 30, 25, 35
    assert!(results.contains(&Value::Int(30)));
    assert!(results.contains(&Value::Int(25)));
    assert!(results.contains(&Value::Int(35)));

    // Software gets version values: 2.0, 7.0
    assert!(results.contains(&Value::Float(2.0)));
    assert!(results.contains(&Value::Float(7.0)));
}

// =============================================================================
// Filtering Without None Branch
// =============================================================================

#[test]
fn test_branch_filters_unmatched_without_none() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Only handle "person", no default -> software vertices filtered out
    let count = g
        .v()
        .branch(__::label())
        .option("person", __::identity())
        // No option_none, so non-person vertices are filtered
        .count();

    // Only 3 person vertices should pass through
    assert_eq!(count, 3);
}

#[test]
fn test_branch_filters_when_branch_traversal_empty() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Use a property that doesn't exist on some vertices
    let results = g
        .v()
        .branch(__::values("status")) // only persons have "status"
        .option("active", __::values("name"))
        .option("inactive", __::values("name"))
        // No option_none -> software vertices (no status) are filtered
        .to_list();

    // Only person vertices have "status", so only they produce results
    assert!(results.contains(&Value::String("Alice".to_string())));
    assert!(results.contains(&Value::String("Bob".to_string())));
    assert!(results.contains(&Value::String("Charlie".to_string())));
    assert_eq!(results.len(), 3);
}

// =============================================================================
// Integer Keys
// =============================================================================

#[test]
fn test_branch_with_integer_key() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Route by priority property (integer)
    let results = g
        .v()
        .branch(__::values("priority"))
        .option(1i64, __::values("name"))
        .option(2i64, __::constant(Value::String("priority-2".to_string())))
        .option(3i64, __::constant(Value::String("priority-3".to_string())))
        .to_list();

    // Priority 1: Alice, Charlie -> names
    assert!(results.contains(&Value::String("Alice".to_string())));
    assert!(results.contains(&Value::String("Charlie".to_string())));

    // Priority 2: Bob, Redis -> "priority-2"
    let p2_count = results
        .iter()
        .filter(|v| **v == Value::String("priority-2".to_string()))
        .count();
    assert_eq!(p2_count, 2);

    // Priority 3: GraphDB -> "priority-3"
    assert!(results.contains(&Value::String("priority-3".to_string())));
}

#[test]
fn test_branch_with_i32_key() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // i32 keys should work (converted to i64 internally)
    let results = g
        .v()
        .branch(__::values("priority"))
        .option(1i32, __::values("name"))
        .option_none(__::constant(Value::String("other".to_string())))
        .to_list();

    // Priority 1: Alice, Charlie -> names
    assert!(results.contains(&Value::String("Alice".to_string())));
    assert!(results.contains(&Value::String("Charlie".to_string())));

    // Others get "other"
    let other_count = results
        .iter()
        .filter(|v| **v == Value::String("other".to_string()))
        .count();
    assert_eq!(other_count, 3); // Bob, GraphDB, Redis
}

// =============================================================================
// Boolean Keys
// =============================================================================

#[test]
fn test_branch_with_boolean_key() {
    let mut storage = InMemoryGraph::new();

    // Create vertices with boolean property
    let _v1 = storage.add_vertex("item", {
        let mut props = HashMap::new();
        props.insert("active".to_string(), Value::Bool(true));
        props.insert("name".to_string(), Value::String("Item1".to_string()));
        props
    });

    let _v2 = storage.add_vertex("item", {
        let mut props = HashMap::new();
        props.insert("active".to_string(), Value::Bool(false));
        props.insert("name".to_string(), Value::String("Item2".to_string()));
        props
    });

    let _v3 = storage.add_vertex("item", {
        let mut props = HashMap::new();
        props.insert("active".to_string(), Value::Bool(true));
        props.insert("name".to_string(), Value::String("Item3".to_string()));
        props
    });

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    let results = g
        .v()
        .branch(__::values("active"))
        .option(true, __::values("name"))
        .option(false, __::constant(Value::String("inactive".to_string())))
        .to_list();

    // Active items: Item1, Item3 -> names
    assert!(results.contains(&Value::String("Item1".to_string())));
    assert!(results.contains(&Value::String("Item3".to_string())));

    // Inactive item: Item2 -> "inactive"
    assert!(results.contains(&Value::String("inactive".to_string())));
    assert_eq!(results.len(), 3);
}

// =============================================================================
// Choose By Equivalence
// =============================================================================

#[test]
fn test_choose_by_equivalent_to_branch() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Using branch()
    let branch_results = g
        .v()
        .branch(__::label())
        .option("person", __::out())
        .to_list();

    // Using choose_by() - should be identical
    let choose_results = g
        .v()
        .choose_by(__::label())
        .option("person", __::out())
        .to_list();

    assert_eq!(branch_results.len(), choose_results.len());

    // Both should produce the same results (though order may differ)
    for result in &branch_results {
        assert!(choose_results.contains(result));
    }
}

#[test]
fn test_choose_by_with_option_none() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    let results = g
        .v()
        .choose_by(__::values("status"))
        .option("active", __::constant(Value::String("ACTIVE".to_string())))
        .option(
            "inactive",
            __::constant(Value::String("INACTIVE".to_string())),
        )
        .option_none(__::constant(Value::String("NO_STATUS".to_string())))
        .to_list();

    // Active persons: Alice, Charlie
    let active_count = results
        .iter()
        .filter(|v| **v == Value::String("ACTIVE".to_string()))
        .count();
    assert_eq!(active_count, 2);

    // Inactive person: Bob
    assert!(results.contains(&Value::String("INACTIVE".to_string())));

    // Software (no status): GraphDB, Redis
    let no_status_count = results
        .iter()
        .filter(|v| **v == Value::String("NO_STATUS".to_string()))
        .count();
    assert_eq!(no_status_count, 2);
}

// =============================================================================
// Path Preservation
// =============================================================================

#[test]
fn test_branch_preserves_path() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    let results = g
        .v_ids([test.alice])
        .as_("start")
        .branch(__::label())
        .option("person", __::out_labels(&["knows"]).as_("end"))
        .path()
        .to_list();

    // Should have path from Alice through branch to Bob
    assert_eq!(results.len(), 1);

    // Path should contain labeled elements
    if let Value::List(path) = &results[0] {
        assert_eq!(path.len(), 2); // start (Alice) and end (Bob)
    } else {
        panic!("Expected path to be a list");
    }
}

#[test]
fn test_branch_path_with_multiple_inputs() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    let results = g
        .v()
        .has_label("person")
        .as_("person")
        .branch(__::values("status"))
        .option("active", __::out_labels(&["knows"]).as_("friend"))
        .option("inactive", __::identity().as_("self"))
        .path()
        .to_list();

    // Each person that matches should have a path
    // Active persons (Alice, Charlie) follow knows edges
    // Inactive person (Bob) keeps identity
    for result in &results {
        if let Value::List(path) = result {
            assert!(path.len() >= 1);
        }
    }
}

// =============================================================================
// Continuation Methods
// =============================================================================

#[test]
fn test_branch_builder_continuation_out() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Using out() continuation after branch
    let results = g
        .v()
        .has_label("person")
        .branch(__::values("status"))
        .option("active", __::identity())
        .option("inactive", __::identity())
        .out() // continuation method
        .values("name")
        .to_list();

    // All persons pass through identity, then out() finds their neighbors
    assert!(!results.is_empty());
}

#[test]
fn test_branch_builder_continuation_has_label() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Using has_label() continuation after branch
    let count = g
        .v()
        .branch(__::label())
        .option("person", __::out())
        .option("software", __::in_())
        .has_label("person") // continuation method
        .count();

    // Should filter to only person vertices after branch
    assert!(count > 0);
}

#[test]
fn test_branch_builder_terminal_count() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Direct terminal method on builder
    let count = g
        .v()
        .branch(__::label())
        .option("person", __::identity())
        .count();

    // Only person vertices pass through
    assert_eq!(count, 3);
}

#[test]
fn test_branch_builder_terminal_next() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Using next() terminal
    let result = g
        .v()
        .has_label("person")
        .branch(__::label())
        .option("person", __::values("name"))
        .next();

    assert!(result.is_some());
    if let Some(Value::String(name)) = result {
        assert!(name == "Alice" || name == "Bob" || name == "Charlie");
    }
}

#[test]
fn test_branch_builder_terminal_has_next() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // has_next should return true when there are results
    let has_results = g
        .v()
        .has_label("person")
        .branch(__::label())
        .option("person", __::identity())
        .has_next();

    assert!(has_results);

    // has_next should return false when no results
    let no_results = g
        .v()
        .has_label("nonexistent")
        .branch(__::label())
        .option("nonexistent", __::identity())
        .has_next();

    assert!(!no_results);
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]
fn test_branch_with_empty_input() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // No vertices match, so branch receives empty input
    let results = g
        .v()
        .has_label("nonexistent")
        .branch(__::label())
        .option("nonexistent", __::identity())
        .to_list();

    assert!(results.is_empty());
}

#[test]
fn test_branch_option_produces_multiple_results() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Option branch that produces multiple results per input
    let results = g
        .v_ids([test.alice])
        .branch(__::label())
        .option("person", __::out()) // Alice has 2 outgoing edges
        .to_list();

    // Alice -> out() -> Bob (knows), GraphDB (created)
    assert_eq!(results.len(), 2);
}

#[test]
fn test_branch_multiple_options_same_key_overwrites() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Adding same key twice should overwrite
    let results = g
        .v()
        .has_label("person")
        .branch(__::label())
        .option("person", __::constant(Value::String("first".to_string())))
        .option("person", __::constant(Value::String("second".to_string()))) // overwrites
        .to_list();

    // All results should be "second"
    for result in &results {
        assert_eq!(*result, Value::String("second".to_string()));
    }
}

// =============================================================================
// Complex Scenarios
// =============================================================================

#[test]
fn test_branch_chained_with_filter() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    let results = g
        .v()
        .branch(__::label())
        .option("person", __::out_labels(&["knows"]))
        .option("software", __::in_labels(&["created"]))
        .has_label("person") // filter after branch
        .values("name")
        .dedup()
        .to_list();

    // Should only include person vertices after filter
    for result in &results {
        if let Value::String(name) = result {
            assert!(
                name == "Alice" || name == "Bob" || name == "Charlie",
                "Unexpected name: {}",
                name
            );
        }
    }
}

#[test]
fn test_branch_with_transform_in_option() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Use values in branch to transform per-vertex
    let results = g
        .v()
        .has_label("person")
        .branch(__::values("status"))
        .option("active", __::values("age"))
        .option("inactive", __::values("age"))
        .to_list();

    // Each person should produce their age
    // Alice (active): 30
    // Bob (inactive): 25
    // Charlie (active): 35
    assert_eq!(results.len(), 3);
    assert!(results.contains(&Value::Int(30)));
    assert!(results.contains(&Value::Int(25)));
    assert!(results.contains(&Value::Int(35)));
}
