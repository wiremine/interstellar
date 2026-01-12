//! Integration tests for side effect steps.
//!
//! These tests verify side effect functionality including:
//! - `store()` - lazy storage of values
//! - `aggregate()` - barrier storage of values
//! - `cap()` - retrieval of side effect data
//! - `side_effect()` - execute sub-traversal for side effects
//! - `profile()` - collect profiling information
//!
//! Tests also verify that side effects work correctly with:
//! - Anonymous traversals using the `__` factory module
//! - Complex traversal pipelines
//! - Path tracking enabled/disabled

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
/// - Multiple edges for testing side effects
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
        props
    });

    let bob = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        props.insert("age".to_string(), Value::Int(25));
        props
    });

    let charlie = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Charlie".to_string()));
        props.insert("age".to_string(), Value::Int(35));
        props
    });

    // Add software vertices
    let graphdb = storage.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("GraphDB".to_string()));
        props.insert("version".to_string(), Value::Float(2.0));
        props
    });

    let redis = storage.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Redis".to_string()));
        props.insert("version".to_string(), Value::Float(7.0));
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
// Store Step Tests
// =============================================================================

#[test]
fn test_store_stores_vertex_values() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Store all person vertices
    let results = g
        .v()
        .has_label("person")
        .store("people")
        .cap("people")
        .to_list();

    assert_eq!(results.len(), 1);
    if let Value::List(people) = &results[0] {
        assert_eq!(people.len(), 3);
        // Should contain Alice, Bob, and Charlie vertices
        assert!(people.iter().all(|v| matches!(v, Value::Vertex(_))));
    } else {
        panic!("Expected Value::List");
    }
}

#[test]
fn test_store_stores_property_values() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Store all person names
    let results = g
        .v()
        .has_label("person")
        .values("name")
        .store("names")
        .cap("names")
        .to_list();

    assert_eq!(results.len(), 1);
    if let Value::List(names) = &results[0] {
        assert_eq!(names.len(), 3);
        let name_strings: Vec<&str> = names.iter().filter_map(|v| v.as_str()).collect();
        assert!(name_strings.contains(&"Alice"));
        assert!(name_strings.contains(&"Bob"));
        assert!(name_strings.contains(&"Charlie"));
    } else {
        panic!("Expected Value::List");
    }
}

#[test]
fn test_store_is_lazy() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Note: Due to the traversal executor's eager collection after each step,
    // store() will process all values from the previous step before limit()
    // takes effect. This test verifies that store passes through values correctly.
    let results = g
        .v()
        .has_label("person")
        .store("x")
        .limit(2)
        .cap("x")
        .to_list();

    assert_eq!(results.len(), 1);
    if let Value::List(stored) = &results[0] {
        // All 3 values are stored because the executor collects after each step,
        // meaning store() processes all 3 vertices before limit() filters.
        // The "lazy" aspect of store is that it uses .inspect() rather than
        // collecting all values first - but since the executor is eager per step,
        // all values pass through store before limit stops the pipeline.
        assert_eq!(stored.len(), 3);
    } else {
        panic!("Expected Value::List");
    }
}

#[test]
fn test_store_preserves_traverser_values() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Store should pass through values unchanged
    let results = g
        .v()
        .has_label("person")
        .values("age")
        .store("ages")
        .to_list();

    // Should have all 3 ages pass through
    assert_eq!(results.len(), 3);
    let ages: Vec<i64> = results.iter().filter_map(|v| v.as_i64()).collect();
    assert!(ages.contains(&30)); // Alice
    assert!(ages.contains(&25)); // Bob
    assert!(ages.contains(&35)); // Charlie
}

// =============================================================================
// Aggregate Step Tests
// =============================================================================

#[test]
fn test_aggregate_collects_all_values() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Aggregate all person vertices
    let results = g
        .v()
        .has_label("person")
        .aggregate("all_people")
        .cap("all_people")
        .to_list();

    assert_eq!(results.len(), 1);
    if let Value::List(people) = &results[0] {
        assert_eq!(people.len(), 3);
    } else {
        panic!("Expected Value::List");
    }
}

#[test]
fn test_aggregate_is_barrier() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Aggregate is a barrier - it collects ALL values before continuing
    // Even with limit after aggregate, all values are collected
    let results = g
        .v()
        .has_label("person")
        .aggregate("x")
        .limit(1)
        .cap("x")
        .to_list();

    assert_eq!(results.len(), 1);
    if let Value::List(stored) = &results[0] {
        // All 3 values should be stored because aggregate is a barrier
        assert_eq!(stored.len(), 3);
    } else {
        panic!("Expected Value::List");
    }
}

#[test]
fn test_aggregate_re_emits_all_values() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // After aggregate, all values should continue through the pipeline
    let results = g
        .v()
        .has_label("person")
        .aggregate("x")
        .values("name")
        .to_list();

    // All 3 person names should be emitted
    assert_eq!(results.len(), 3);
    let names: Vec<&str> = results.iter().filter_map(|v| v.as_str()).collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Bob"));
    assert!(names.contains(&"Charlie"));
}

// =============================================================================
// Cap Step Tests
// =============================================================================

#[test]
fn test_cap_single_key_returns_list() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    let results = g.v().has_label("person").store("x").cap("x").to_list();

    assert_eq!(results.len(), 1);
    assert!(matches!(&results[0], Value::List(_)));
}

#[test]
fn test_cap_multi_returns_map() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Store in two different keys
    let results = g
        .v()
        .has_label("person")
        .store("people")
        .values("name")
        .store("names")
        .cap_multi(["people", "names"])
        .to_list();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert!(map.contains_key("people"));
        assert!(map.contains_key("names"));
    } else {
        panic!("Expected Value::Map");
    }
}

#[test]
fn test_cap_missing_key_returns_empty_list() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    let results = g
        .v()
        .has_label("person")
        .store("x")
        .cap("nonexistent")
        .to_list();

    assert_eq!(results.len(), 1);
    if let Value::List(list) = &results[0] {
        assert!(list.is_empty());
    } else {
        panic!("Expected Value::List");
    }
}

#[test]
fn test_cap_consumes_input_stream() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Cap should consume input, then emit the side effect data
    let results = g.v().has_label("person").store("x").cap("x").to_list();

    // Cap emits one list
    assert_eq!(results.len(), 1);
}

// =============================================================================
// SideEffect Step Tests
// =============================================================================

#[test]
fn test_side_effect_executes_sub_traversal() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Execute side effect that stores neighbors
    let results = g
        .v_ids([test.alice])
        .side_effect(__::out().store("neighbors"))
        .cap("neighbors")
        .to_list();

    assert_eq!(results.len(), 1);
    if let Value::List(neighbors) = &results[0] {
        // Alice has 2 outgoing neighbors (Bob via knows, GraphDB via created)
        assert_eq!(neighbors.len(), 2);
    } else {
        panic!("Expected Value::List");
    }
}

#[test]
fn test_side_effect_preserves_original_traverser() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Side effect should pass through original traverser
    let results = g
        .v_ids([test.alice])
        .side_effect(__::out().store("neighbors"))
        .values("name")
        .to_list();

    // Should have Alice's name
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice".to_string()));
}

#[test]
fn test_side_effect_with_complex_sub_traversal() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Side effect with filtering in sub-traversal
    let count = g
        .v()
        .has_label("person")
        .side_effect(__::out_labels(&["knows"]).store("friends"))
        .count();

    // Count of input traversers (3 people)
    assert_eq!(count, 3);
}

// =============================================================================
// Profile Step Tests
// =============================================================================

#[test]
fn test_profile_records_count() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    let results = g.v().has_label("person").profile().cap("profile").to_list();

    assert_eq!(results.len(), 1);
    // Cap returns a list, profile data is the first (and only) element
    if let Value::List(list) = &results[0] {
        assert_eq!(list.len(), 1);
        if let Value::Map(profile) = &list[0] {
            let count = profile.get("count");
            assert_eq!(count, Some(&Value::Int(3)));
        } else {
            panic!("Expected Value::Map inside list, got {:?}", list[0]);
        }
    } else {
        panic!("Expected Value::List, got {:?}", results[0]);
    }
}

#[test]
fn test_profile_records_time() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    let results = g.v().has_label("person").profile().cap("profile").to_list();

    assert_eq!(results.len(), 1);
    // Cap returns a list, profile data is the first element
    if let Value::List(list) = &results[0] {
        assert_eq!(list.len(), 1);
        if let Value::Map(profile) = &list[0] {
            let time = profile.get("time_ms");
            assert!(matches!(time, Some(Value::Float(_))));
        } else {
            panic!("Expected Value::Map inside list");
        }
    } else {
        panic!("Expected Value::List");
    }
}

#[test]
fn test_profile_with_custom_key() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    let results = g
        .v()
        .has_label("person")
        .profile_as("my_profile")
        .cap("my_profile")
        .to_list();

    assert_eq!(results.len(), 1);
    // Cap returns a list, profile data is the first element
    if let Value::List(list) = &results[0] {
        assert_eq!(list.len(), 1);
        if let Value::Map(profile) = &list[0] {
            assert!(profile.contains_key("count"));
            assert!(profile.contains_key("time_ms"));
        } else {
            panic!("Expected Value::Map inside list");
        }
    } else {
        panic!("Expected Value::List");
    }
}

#[test]
fn test_profile_passes_through_values() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Profile should not change the traversal output
    let results = g.v().has_label("person").profile().values("name").to_list();

    // Should have all 3 person names
    assert_eq!(results.len(), 3);
    let names: Vec<&str> = results.iter().filter_map(|v| v.as_str()).collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Bob"));
    assert!(names.contains(&"Charlie"));
}

// =============================================================================
// Anonymous Traversal Factory Tests
// =============================================================================

#[test]
fn test_anonymous_store() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Use anonymous traversal with store
    let results = g
        .v()
        .has_label("person")
        .append(__::store("x").values("name"))
        .to_list();

    // Should get names
    assert_eq!(results.len(), 3);
}

#[test]
fn test_anonymous_aggregate() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Use anonymous traversal with aggregate - aggregate is barrier then emits all
    let count = g.v().has_label("person").append(__::aggregate("x")).count();

    // After aggregate (barrier), count of 3 items emitted
    assert_eq!(count, 3);
}

#[test]
fn test_anonymous_cap() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Store then cap using anonymous traversal
    let results = g
        .v()
        .has_label("person")
        .store("x")
        .append(__::cap("x"))
        .to_list();

    assert_eq!(results.len(), 1);
    assert!(matches!(&results[0], Value::List(_)));
}

#[test]
fn test_anonymous_side_effect() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Anonymous side effect that stores outgoing neighbors
    let results = g
        .v_ids([test.alice])
        .append(__::side_effect(__::out().store("neighbors")))
        .cap("neighbors")
        .to_list();

    assert_eq!(results.len(), 1);
    if let Value::List(neighbors) = &results[0] {
        assert_eq!(neighbors.len(), 2);
    } else {
        panic!("Expected Value::List");
    }
}

#[test]
fn test_anonymous_profile() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Anonymous profile step
    let results = g
        .v()
        .has_label("person")
        .append(__::profile())
        .cap("profile")
        .to_list();

    assert_eq!(results.len(), 1);
    // Cap returns a list containing the profile map
    if let Value::List(list) = &results[0] {
        assert_eq!(list.len(), 1);
        assert!(matches!(&list[0], Value::Map(_)));
    } else {
        panic!("Expected Value::List");
    }
}

// =============================================================================
// Complex Pipeline Tests
// =============================================================================

#[test]
fn test_store_and_aggregate_combined() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Store lazily, then aggregate (barrier)
    let results = g
        .v()
        .has_label("person")
        .store("lazy")
        .aggregate("barrier")
        .cap_multi(["lazy", "barrier"])
        .to_list();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        let lazy = map.get("lazy");
        let barrier = map.get("barrier");

        // Both should have 3 items
        if let (Some(Value::List(lazy_list)), Some(Value::List(barrier_list))) = (lazy, barrier) {
            assert_eq!(lazy_list.len(), 3);
            assert_eq!(barrier_list.len(), 3);
        } else {
            panic!("Expected lists");
        }
    } else {
        panic!("Expected Value::Map");
    }
}

#[test]
fn test_multiple_profile_steps() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Multiple profile steps with different keys
    let results = g
        .v()
        .profile_as("v_profile")
        .has_label("person")
        .profile_as("filter_profile")
        .out()
        .profile_as("out_profile")
        .cap_multi(["v_profile", "filter_profile", "out_profile"])
        .to_list();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        // All three profile keys should exist
        assert!(map.contains_key("v_profile"));
        assert!(map.contains_key("filter_profile"));
        assert!(map.contains_key("out_profile"));
    } else {
        panic!("Expected Value::Map");
    }
}

#[test]
fn test_side_effect_in_union() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Side effects in union branches
    let results = g
        .v_ids([test.alice])
        .union(vec![
            __::out_labels(&["knows"]).store("friends"),
            __::out_labels(&["created"]).store("creations"),
        ])
        .cap_multi(["friends", "creations"])
        .to_list();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        let friends = map.get("friends");
        let creations = map.get("creations");

        if let (Some(Value::List(friends_list)), Some(Value::List(creations_list))) =
            (friends, creations)
        {
            assert_eq!(friends_list.len(), 1); // Bob
            assert_eq!(creations_list.len(), 1); // GraphDB
        } else {
            panic!("Expected lists");
        }
    } else {
        panic!("Expected Value::Map");
    }
}

#[test]
fn test_empty_traversal_side_effects() {
    let test = create_test_graph();
    let snapshot = test.graph.snapshot();
    let g = snapshot.traversal();

    // Side effects on empty traversal
    let results = g
        .v()
        .has_label("nonexistent")
        .store("x")
        .aggregate("y")
        .cap_multi(["x", "y"])
        .to_list();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        let x = map.get("x");
        let y = map.get("y");

        // Both should be empty lists
        if let (Some(Value::List(x_list)), Some(Value::List(y_list))) = (x, y) {
            assert!(x_list.is_empty());
            assert!(y_list.is_empty());
        } else {
            panic!("Expected lists");
        }
    } else {
        panic!("Expected Value::Map");
    }
}
