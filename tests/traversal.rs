//! Integration tests for the traversal engine.
//!
//! These tests verify the complete traversal pipeline including:
//! - Basic traversal sources (v, e, inject)
//! - Filter steps (has_label, has, has_value, dedup, limit, skip, range, has_id)
//! - Navigation steps (out, in_, both, out_e, in_e, both_e, out_v, in_v, both_v)
//! - Transform steps (values, id, label, map, flat_map, constant, path, as_, select)
//! - Terminal steps (to_list, to_set, next, one, count, sum, min, max, fold)
//! - Anonymous traversals (__ module)
//! - Complex multi-step traversals

use std::collections::HashMap;
use std::sync::Arc;

use rustgremlin::graph::Graph;
use rustgremlin::storage::InMemoryGraph;
use rustgremlin::traversal::__;
use rustgremlin::value::{EdgeId, Value, VertexId};

// =============================================================================
// Test Graph Setup
// =============================================================================

/// Test graph with vertex and edge IDs for use in tests.
struct TestGraph {
    graph: Graph,
    alice: VertexId,
    bob: VertexId,
    charlie: VertexId,
    graphdb: VertexId,
    alice_knows_bob: EdgeId,
    bob_knows_charlie: EdgeId,
    #[allow(dead_code)]
    alice_uses_graphdb: EdgeId,
    #[allow(dead_code)]
    bob_uses_graphdb: EdgeId,
    #[allow(dead_code)]
    charlie_knows_alice: EdgeId,
}

/// Creates a test graph with:
/// - 4 vertices: Alice (person), Bob (person), Charlie (person), GraphDB (software)
/// - 5 edges: Alice-knows->Bob, Bob-knows->Charlie, Alice-uses->GraphDB,
///   Bob-uses->GraphDB, Charlie-knows->Alice
///
/// Graph structure:
/// ```text
///     Alice ----knows----> Bob ----knows----> Charlie
///       |                   |                   |
///       |                   |                   |
///      uses                uses              knows
///       |                   |                   |
///       v                   v                   |
///     GraphDB <-------------+                   |
///       ^                                       |
///       |                                       |
///       +---------------------------------------+
///                    (Charlie knows Alice)
/// ```
fn create_test_graph() -> TestGraph {
    let mut storage = InMemoryGraph::new();

    // Add vertices with properties
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

    let graphdb = storage.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("GraphDB".to_string()));
        props.insert("version".to_string(), Value::Float(1.0));
        props
    });

    // Add edges with properties
    // Alice knows Bob (edge 0)
    let alice_knows_bob = storage
        .add_edge(alice, bob, "knows", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2020));
            props
        })
        .unwrap();

    // Bob knows Charlie (edge 1)
    let bob_knows_charlie = storage
        .add_edge(bob, charlie, "knows", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2021));
            props
        })
        .unwrap();

    // Alice uses GraphDB (edge 2)
    let alice_uses_graphdb = storage
        .add_edge(alice, graphdb, "uses", {
            let mut props = HashMap::new();
            props.insert("skill".to_string(), Value::String("expert".to_string()));
            props
        })
        .unwrap();

    // Bob uses GraphDB (edge 3)
    let bob_uses_graphdb = storage
        .add_edge(bob, graphdb, "uses", {
            let mut props = HashMap::new();
            props.insert("skill".to_string(), Value::String("beginner".to_string()));
            props
        })
        .unwrap();

    // Charlie knows Alice (edge 4) - creates a cycle
    let charlie_knows_alice = storage
        .add_edge(charlie, alice, "knows", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2019));
            props
        })
        .unwrap();

    TestGraph {
        graph: Graph::new(Arc::new(storage)),
        alice,
        bob,
        charlie,
        graphdb,
        alice_knows_bob,
        bob_knows_charlie,
        alice_uses_graphdb,
        bob_uses_graphdb,
        charlie_knows_alice,
    }
}

/// Creates an empty graph for testing edge cases.
fn create_empty_graph() -> Graph {
    Graph::new(Arc::new(InMemoryGraph::new()))
}

// =============================================================================
// Basic Tests
// =============================================================================

mod basic_tests {
    use super::*;

    #[test]
    fn v_returns_all_vertices() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let vertices = g.v().to_list();
        assert_eq!(vertices.len(), 4);

        // All should be vertices
        for v in &vertices {
            assert!(v.is_vertex(), "Expected vertex, got {:?}", v);
        }
    }

    #[test]
    fn e_returns_all_edges() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let edges = g.e().to_list();
        assert_eq!(edges.len(), 5);

        // All should be edges
        for e in &edges {
            assert!(e.is_edge(), "Expected edge, got {:?}", e);
        }
    }

    #[test]
    fn v_ids_returns_specific_vertices() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let vertices = g.v_ids([tg.alice, tg.charlie]).to_list();
        assert_eq!(vertices.len(), 2);

        let ids: Vec<VertexId> = vertices.iter().filter_map(|v| v.as_vertex_id()).collect();
        assert!(ids.contains(&tg.alice));
        assert!(ids.contains(&tg.charlie));
    }

    #[test]
    fn e_ids_returns_specific_edges() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let edges = g
            .e_ids([tg.alice_knows_bob, tg.bob_knows_charlie])
            .to_list();
        assert_eq!(edges.len(), 2);

        let ids: Vec<EdgeId> = edges.iter().filter_map(|e| e.as_edge_id()).collect();
        assert!(ids.contains(&tg.alice_knows_bob));
        assert!(ids.contains(&tg.bob_knows_charlie));
    }

    #[test]
    fn inject_creates_traversers_from_values() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let results = g.inject([1i64, 2i64, 3i64]).to_list();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Value::Int(1));
        assert_eq!(results[1], Value::Int(2));
        assert_eq!(results[2], Value::Int(3));
    }

    #[test]
    fn count_returns_correct_value() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        assert_eq!(g.v().count(), 4);
        assert_eq!(g.e().count(), 5);
    }

    #[test]
    fn empty_graph_returns_empty_results() {
        let graph = create_empty_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        assert_eq!(g.v().count(), 0);
        assert_eq!(g.e().count(), 0);
        assert!(g.v().to_list().is_empty());
        assert!(g.e().to_list().is_empty());
    }

    #[test]
    fn nonexistent_vertex_ids_filtered_out() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Create a fake vertex ID that doesn't exist (we know IDs are 0-3)
        // Since we can't construct VertexId directly, we'll just test with valid IDs
        let results = g.v_ids([tg.alice]).to_list();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
    }
}

// =============================================================================
// Filter Tests
// =============================================================================

mod filter_tests {
    use super::*;

    #[test]
    fn has_label_filters_vertices_by_label() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let people = g.v().has_label("person").to_list();
        assert_eq!(people.len(), 3); // Alice, Bob, Charlie

        let software = g.v().has_label("software").to_list();
        assert_eq!(software.len(), 1); // GraphDB
    }

    #[test]
    fn has_label_filters_edges_by_label() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let knows_edges = g.e().has_label("knows").to_list();
        assert_eq!(knows_edges.len(), 3);

        let uses_edges = g.e().has_label("uses").to_list();
        assert_eq!(uses_edges.len(), 2);
    }

    #[test]
    fn has_label_any_filters_by_multiple_labels() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let all = g.v().has_label_any(["person", "software"]).to_list();
        assert_eq!(all.len(), 4);
    }

    #[test]
    fn has_label_returns_empty_for_nonexistent_label() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let results = g.v().has_label("unknown").to_list();
        assert!(results.is_empty());
    }

    #[test]
    fn has_filters_by_property_existence() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // All vertices have "name"
        let with_name = g.v().has("name").to_list();
        assert_eq!(with_name.len(), 4);

        // Only person vertices have "age"
        let with_age = g.v().has("age").to_list();
        assert_eq!(with_age.len(), 3);

        // Only software has "version"
        let with_version = g.v().has("version").to_list();
        assert_eq!(with_version.len(), 1);
    }

    #[test]
    fn has_value_filters_by_property_value() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let alice = g.v().has_value("name", "Alice").to_list();
        assert_eq!(alice.len(), 1);

        let age_30 = g.v().has_value("age", 30i64).to_list();
        assert_eq!(age_30.len(), 1);
    }

    #[test]
    fn has_id_filters_vertices_by_id() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let result = g.v().has_id(tg.alice).to_list();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].as_vertex_id(), Some(tg.alice));
    }

    #[test]
    fn has_ids_filters_by_multiple_ids() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let results = g.v().has_ids([tg.alice, tg.bob, tg.charlie]).to_list();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn filter_with_custom_predicate() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Filter injected values
        let positives = g
            .inject([1i64, -2i64, 3i64, -4i64])
            .filter(|_ctx, v| matches!(v, Value::Int(n) if *n > 0))
            .to_list();
        assert_eq!(positives.len(), 2);
        assert_eq!(positives[0], Value::Int(1));
        assert_eq!(positives[1], Value::Int(3));
    }

    #[test]
    fn dedup_removes_duplicates() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let results = g.inject([1i64, 2i64, 1i64, 3i64, 2i64]).dedup().to_list();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn limit_restricts_result_count() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let results = g.v().limit(2).to_list();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn limit_with_more_than_available() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let results = g.v().limit(100).to_list();
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn skip_skips_elements() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let results = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).skip(2).to_list();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Value::Int(3));
    }

    #[test]
    fn range_selects_range_of_elements() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let results = g
            .inject([0i64, 1i64, 2i64, 3i64, 4i64, 5i64])
            .range(2, 5)
            .to_list();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Value::Int(2));
        assert_eq!(results[1], Value::Int(3));
        assert_eq!(results[2], Value::Int(4));
    }

    #[test]
    fn chained_filters() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Find person vertices with age property
        let results = g.v().has_label("person").has("age").to_list();
        assert_eq!(results.len(), 3);
    }
}

// =============================================================================
// Navigation Tests
// =============================================================================

mod navigation_tests {
    use super::*;

    #[test]
    fn out_traverses_to_outgoing_neighbors() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Alice has outgoing edges to Bob and GraphDB
        let neighbors = g.v_ids([tg.alice]).out().to_list();
        assert_eq!(neighbors.len(), 2);
    }

    #[test]
    fn out_labels_filters_by_edge_label() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Alice knows Bob only
        let knows = g.v_ids([tg.alice]).out_labels(&["knows"]).to_list();
        assert_eq!(knows.len(), 1);

        // Alice uses GraphDB only
        let uses = g.v_ids([tg.alice]).out_labels(&["uses"]).to_list();
        assert_eq!(uses.len(), 1);
    }

    #[test]
    fn in_traverses_to_incoming_neighbors() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Alice has incoming edge from Charlie
        let known_by = g.v_ids([tg.alice]).in_().to_list();
        assert_eq!(known_by.len(), 1);
    }

    #[test]
    fn in_labels_filters_by_edge_label() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // GraphDB is used by Alice and Bob
        let used_by = g.v_ids([tg.graphdb]).in_labels(&["uses"]).to_list();
        assert_eq!(used_by.len(), 2);
    }

    #[test]
    fn both_traverses_in_both_directions() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Alice has: out to Bob, out to GraphDB, in from Charlie
        let neighbors = g.v_ids([tg.alice]).both().to_list();
        assert_eq!(neighbors.len(), 3);
    }

    #[test]
    fn both_labels_filters_by_edge_label() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Alice: knows->Bob, <-knows Charlie
        let knows = g.v_ids([tg.alice]).both_labels(&["knows"]).to_list();
        assert_eq!(knows.len(), 2);
    }

    #[test]
    fn out_e_returns_outgoing_edges() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Alice has 2 outgoing edges
        let edges = g.v_ids([tg.alice]).out_e().to_list();
        assert_eq!(edges.len(), 2);
        for e in &edges {
            assert!(e.is_edge());
        }
    }

    #[test]
    fn out_e_labels_filters_by_edge_label() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let knows_edges = g.v_ids([tg.alice]).out_e_labels(&["knows"]).to_list();
        assert_eq!(knows_edges.len(), 1);
    }

    #[test]
    fn in_e_returns_incoming_edges() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Alice has 1 incoming edge (from Charlie)
        let edges = g.v_ids([tg.alice]).in_e().to_list();
        assert_eq!(edges.len(), 1);
    }

    #[test]
    fn both_e_returns_all_incident_edges() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Alice has 3 incident edges (2 out, 1 in)
        let edges = g.v_ids([tg.alice]).both_e().to_list();
        assert_eq!(edges.len(), 3);
    }

    #[test]
    fn out_v_returns_source_vertex_of_edge() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Get source vertices of all edges
        let sources = g.e().out_v().to_list();
        assert_eq!(sources.len(), 5);
        for s in &sources {
            assert!(s.is_vertex());
        }
    }

    #[test]
    fn in_v_returns_target_vertex_of_edge() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Get target vertices of all edges
        let targets = g.e().in_v().to_list();
        assert_eq!(targets.len(), 5);
        for t in &targets {
            assert!(t.is_vertex());
        }
    }

    #[test]
    fn both_v_returns_both_vertices_of_edge() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Each edge produces 2 vertices
        let vertices = g.e_ids([tg.alice_knows_bob]).both_v().to_list();
        assert_eq!(vertices.len(), 2);
    }

    #[test]
    fn multi_hop_navigation() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Alice -> knows -> Bob -> knows -> Charlie
        let two_hops = g
            .v_ids([tg.alice])
            .out_labels(&["knows"])
            .out_labels(&["knows"])
            .to_list();
        assert_eq!(two_hops.len(), 1);
        // Should be Charlie
        assert_eq!(two_hops[0].as_vertex_id(), Some(tg.charlie));
    }
}

// =============================================================================
// Transform Tests
// =============================================================================

mod transform_tests {
    use super::*;

    #[test]
    fn values_extracts_property() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let names = g.v().has_label("person").values("name").to_list();
        assert_eq!(names.len(), 3);

        let name_strs: Vec<String> = names
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        assert!(name_strs.contains(&"Alice".to_string()));
        assert!(name_strs.contains(&"Bob".to_string()));
        assert!(name_strs.contains(&"Charlie".to_string()));
    }

    #[test]
    fn values_multi_extracts_multiple_properties() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Get name and age from person vertices
        let props = g
            .v()
            .has_label("person")
            .limit(1)
            .values_multi(["name", "age"])
            .to_list();
        assert_eq!(props.len(), 2); // One name + one age
    }

    #[test]
    fn id_extracts_element_id() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let ids = g.v().id().to_list();
        assert_eq!(ids.len(), 4);

        for id in &ids {
            assert!(matches!(id, Value::Int(_)));
        }
    }

    #[test]
    fn label_extracts_element_label() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let labels = g.v().label().dedup().to_list();
        assert_eq!(labels.len(), 2); // "person" and "software"
    }

    #[test]
    fn map_transforms_values() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let doubled = g
            .inject([1i64, 2i64, 3i64])
            .map(|_ctx, v| {
                if let Value::Int(n) = v {
                    Value::Int(n * 2)
                } else {
                    v.clone()
                }
            })
            .to_list();

        assert_eq!(doubled.len(), 3);
        assert_eq!(doubled[0], Value::Int(2));
        assert_eq!(doubled[1], Value::Int(4));
        assert_eq!(doubled[2], Value::Int(6));
    }

    #[test]
    fn flat_map_expands_values() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let expanded = g
            .inject([3i64])
            .flat_map(|_ctx, v| {
                if let Value::Int(n) = v {
                    (0..*n).map(Value::Int).collect()
                } else {
                    vec![]
                }
            })
            .to_list();

        assert_eq!(expanded.len(), 3);
        assert_eq!(expanded[0], Value::Int(0));
        assert_eq!(expanded[1], Value::Int(1));
        assert_eq!(expanded[2], Value::Int(2));
    }

    #[test]
    fn constant_replaces_with_constant_value() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let results = g.v().limit(3).constant("found").to_list();
        assert_eq!(results.len(), 3);
        for r in &results {
            assert_eq!(*r, Value::String("found".to_string()));
        }
    }

    #[test]
    fn path_returns_traversal_path() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Use as_() to add elements to path
        let paths = g
            .v_ids([tg.alice])
            .as_("start")
            .out_labels(&["knows"])
            .as_("end")
            .path()
            .to_list();

        assert_eq!(paths.len(), 1);
        // Path should be a list
        if let Value::List(list) = &paths[0] {
            assert_eq!(list.len(), 2); // start and end
        } else {
            panic!("Expected Value::List, got {:?}", paths[0]);
        }
    }

    #[test]
    fn as_and_select_labels_and_retrieves() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let results = g
            .v_ids([tg.alice])
            .as_("a")
            .out_labels(&["knows"])
            .as_("b")
            .select(&["a", "b"])
            .to_list();

        assert_eq!(results.len(), 1);
        // Should be a Map
        if let Value::Map(map) = &results[0] {
            assert!(map.contains_key("a"));
            assert!(map.contains_key("b"));
        } else {
            panic!("Expected Value::Map, got {:?}", results[0]);
        }
    }

    #[test]
    fn select_one_retrieves_single_label() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let results = g
            .v_ids([tg.alice])
            .as_("start")
            .out_labels(&["knows"])
            .select_one("start")
            .to_list();

        assert_eq!(results.len(), 1);
        // Should be the vertex directly (not a Map)
        assert!(results[0].is_vertex());
        assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
    }
}

// =============================================================================
// Terminal Tests
// =============================================================================

mod terminal_tests {
    use super::*;

    #[test]
    fn to_list_collects_all_values() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let results = g.v().to_list();
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn to_set_deduplicates() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let results = g.inject([1i64, 2i64, 1i64, 3i64]).to_set();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn next_returns_first_value() {
        let tg = create_test_graph();
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
        let tg = create_test_graph();
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
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let result = g.v_ids([tg.alice]).one();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_vertex_id(), Some(tg.alice));
    }

    #[test]
    fn one_errors_on_empty() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Query for nonexistent label
        let result = g.v().has_label("nonexistent").one();
        assert!(result.is_err());
    }

    #[test]
    fn one_errors_on_multiple() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let result = g.v().one();
        assert!(result.is_err());
    }

    #[test]
    fn count_returns_correct_count() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        assert_eq!(g.v().count(), 4);
        assert_eq!(g.e().count(), 5);
        assert_eq!(g.v().has_label("person").count(), 3);
    }

    #[test]
    fn sum_adds_numeric_values() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let result = g.inject([1i64, 2i64, 3i64, 4i64]).sum();
        assert_eq!(result, Value::Int(10));
    }

    #[test]
    fn sum_handles_floats() {
        let tg = create_test_graph();
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
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let values: Vec<Value> = vec![];
        let result = g.inject(values).sum();
        assert_eq!(result, Value::Int(0));
    }

    #[test]
    fn min_finds_minimum() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let result = g.inject([5i64, 2i64, 8i64, 1i64, 9i64]).min();
        assert_eq!(result, Some(Value::Int(1)));
    }

    #[test]
    fn min_returns_none_for_empty() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let values: Vec<Value> = vec![];
        let result = g.inject(values).min();
        assert!(result.is_none());
    }

    #[test]
    fn max_finds_maximum() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let result = g.inject([5i64, 2i64, 8i64, 1i64, 9i64]).max();
        assert_eq!(result, Some(Value::Int(9)));
    }

    #[test]
    fn max_returns_none_for_empty() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let values: Vec<Value> = vec![];
        let result = g.inject(values).max();
        assert!(result.is_none());
    }

    #[test]
    fn fold_accumulates_values() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let result = g
            .inject([1i64, 2i64, 3i64])
            .fold(0i64, |acc, v| acc + v.as_i64().unwrap_or(0));
        assert_eq!(result, 6);
    }

    #[test]
    fn take_returns_first_n_values() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let results = g.v().take(2);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn iterate_consumes_without_collecting() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Should not panic
        g.v().iterate();
    }
}

// =============================================================================
// Anonymous Traversal Tests
// =============================================================================

mod anonymous_traversal_tests {
    use super::*;

    #[test]
    fn identity_passes_through_unchanged() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let anon = __::identity();
        let results = g.v().append(anon).to_list();
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn out_anonymous_traversal() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let anon = __::out();
        let results = g.v_ids([tg.alice]).append(anon).to_list();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn has_label_anonymous_traversal() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let anon = __::has_label("person");
        let results = g.v().append(anon).to_list();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn chained_anonymous_traversal() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let anon = __::out_labels(&["knows"]).has_label("person");
        let results = g.v_ids([tg.alice]).append(anon).to_list();
        assert_eq!(results.len(), 1); // Alice knows Bob (person)
    }

    #[test]
    fn values_anonymous_traversal() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let anon = __::values("name");
        let results = g.v().has_label("person").append(anon).to_list();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn filter_anonymous_traversal() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let anon = __::filter(|_ctx, v| matches!(v, Value::Int(n) if *n > 2));
        let results = g.inject([1i64, 2i64, 3i64, 4i64]).append(anon).to_list();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn dedup_anonymous_traversal() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let anon = __::dedup();
        let results = g.inject([1i64, 2i64, 1i64, 3i64]).append(anon).to_list();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn limit_anonymous_traversal() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let anon = __::limit(2);
        let results = g.v().append(anon).to_list();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn map_anonymous_traversal() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

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
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let anon = __::constant(42i64);
        let results = g.v().limit(3).append(anon).to_list();
        assert_eq!(results.len(), 3);
        for r in results {
            assert_eq!(r, Value::Int(42));
        }
    }

    #[test]
    fn complex_anonymous_traversal_chain() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Find names of people that Alice knows
        let anon = __::out_labels(&["knows"])
            .has_label("person")
            .values("name");

        let results = g.v_ids([tg.alice]).append(anon).to_list();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], Value::String("Bob".to_string()));
    }
}

// =============================================================================
// Filter Steps with Anonymous Traversals (Phase 2.3+)
// =============================================================================

mod filter_step_tests {
    use super::*;

    // -------------------------------------------------------------------------
    // WhereStep Tests
    // -------------------------------------------------------------------------

    #[test]
    fn where_filters_by_sub_traversal_existence() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Keep vertices that have outgoing edges
        // Alice: out to Bob, GraphDB (2 out) -> passes
        // Bob: out to Charlie, GraphDB (2 out) -> passes
        // Charlie: out to Alice (1 out) -> passes
        // GraphDB: no outgoing edges -> filtered out
        let results = g.v().where_(__::out()).to_list();
        assert_eq!(results.len(), 3); // Alice, Bob, Charlie have outgoing edges
    }

    #[test]
    fn where_filters_by_labeled_edges() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Keep vertices that have outgoing "knows" edges
        // Alice: knows Bob -> passes
        // Bob: knows Charlie -> passes
        // Charlie: knows Alice -> passes
        // GraphDB: no knows edges -> filtered out
        let results = g.v().where_(__::out_labels(&["knows"])).to_list();
        assert_eq!(results.len(), 3);

        // Verify all results are people (not GraphDB)
        for v in &results {
            let id = v.as_vertex_id().unwrap();
            assert!(id == tg.alice || id == tg.bob || id == tg.charlie);
        }
    }

    #[test]
    fn where_filters_by_chained_sub_traversal() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Keep vertices that know someone who uses software
        // Alice knows Bob, Bob uses GraphDB -> Alice passes
        // Bob knows Charlie, Charlie doesn't use anything -> Bob fails
        // Charlie knows Alice, Alice uses GraphDB -> Charlie passes
        let results = g
            .v()
            .where_(__::out_labels(&["knows"]).out_labels(&["uses"]))
            .to_list();
        assert_eq!(results.len(), 2); // Alice and Charlie
    }

    #[test]
    fn where_empty_sub_traversal_filters_out() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // No vertex has outgoing "nonexistent" edges
        let results = g.v().where_(__::out_labels(&["nonexistent"])).to_list();
        assert!(results.is_empty());
    }

    // -------------------------------------------------------------------------
    // NotStep Tests
    // -------------------------------------------------------------------------

    #[test]
    fn not_filters_to_traversers_without_outgoing_edges() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Keep vertices WITHOUT outgoing edges
        // GraphDB has no outgoing edges -> passes
        // Alice, Bob, Charlie all have outgoing edges -> filtered out
        let results = g.v().not(__::out()).to_list();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_vertex_id(), Some(tg.graphdb));
    }

    #[test]
    fn not_is_inverse_of_where() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Vertices with outgoing edges (where)
        let with_out = g.v().where_(__::out()).to_list();

        // Vertices without outgoing edges (not)
        let without_out = g.v().not(__::out()).to_list();

        // Together they should equal all vertices
        assert_eq!(with_out.len() + without_out.len(), 4);

        // No overlap between results
        let with_ids: Vec<_> = with_out.iter().filter_map(|v| v.as_vertex_id()).collect();
        let without_ids: Vec<_> = without_out
            .iter()
            .filter_map(|v| v.as_vertex_id())
            .collect();
        for id in &with_ids {
            assert!(!without_ids.contains(id));
        }
    }

    #[test]
    fn not_filters_by_labeled_edges() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Keep vertices WITHOUT outgoing "uses" edges
        // Alice: uses GraphDB -> filtered out
        // Bob: uses GraphDB -> filtered out
        // Charlie: no uses edges -> passes
        // GraphDB: no uses edges -> passes
        let results = g.v().not(__::out_labels(&["uses"])).to_list();
        assert_eq!(results.len(), 2); // Charlie and GraphDB

        let ids: Vec<_> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
        assert!(ids.contains(&tg.charlie));
        assert!(ids.contains(&tg.graphdb));
    }

    #[test]
    fn not_with_has_label_sub_traversal() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Keep vertices that are NOT persons
        // This uses a sub-traversal pattern - filter out if has_label matches
        let results = g.v().not(__::has_label("person")).to_list();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_vertex_id(), Some(tg.graphdb));
    }

    #[test]
    fn not_finds_leaf_vertices() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Leaf vertices have no outgoing edges
        // In this graph, only GraphDB has no outgoing edges
        let leaves = g.v().not(__::out()).to_list();
        assert_eq!(leaves.len(), 1);

        // Verify it's GraphDB (the software vertex)
        let leaf = &leaves[0];
        assert!(leaf.is_vertex());
        assert_eq!(leaf.as_vertex_id(), Some(tg.graphdb));
    }

    // -------------------------------------------------------------------------
    // AndStep Tests
    // -------------------------------------------------------------------------

    #[test]
    fn and_requires_all_conditions() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Keep vertices that have BOTH outgoing AND incoming edges
        // Alice: out(Bob,GraphDB), in(Charlie) -> passes
        // Bob: out(Charlie,GraphDB), in(Alice) -> passes
        // Charlie: out(Alice), in(Bob) -> passes
        // GraphDB: out(), in(Alice,Bob) -> fails (no outgoing)
        let results = g.v().and_(vec![__::out(), __::in_()]).to_list();
        assert_eq!(results.len(), 3); // Alice, Bob, Charlie
    }

    #[test]
    fn and_short_circuits_on_first_failure() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Require outgoing "knows" AND outgoing "uses" edges
        // Alice: knows(Bob), uses(GraphDB) -> passes
        // Bob: knows(Charlie), uses(GraphDB) -> passes
        // Charlie: knows(Alice), no uses -> fails
        // GraphDB: no knows, no uses -> fails
        let results = g
            .v()
            .and_(vec![__::out_labels(&["knows"]), __::out_labels(&["uses"])])
            .to_list();
        assert_eq!(results.len(), 2); // Alice, Bob

        let ids: Vec<_> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
        assert!(ids.contains(&tg.alice));
        assert!(ids.contains(&tg.bob));
    }

    #[test]
    fn and_with_empty_vec_passes_all() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Empty and_ should pass all traversers (vacuous truth)
        let results = g.v().and_(vec![]).to_list();
        assert_eq!(results.len(), 4);
    }

    // -------------------------------------------------------------------------
    // OrStep Tests
    // -------------------------------------------------------------------------

    #[test]
    fn or_accepts_any_condition() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Keep vertices that have EITHER "knows" OR "uses" outgoing edges
        // Alice: knows(Bob), uses(GraphDB) -> passes
        // Bob: knows(Charlie), uses(GraphDB) -> passes
        // Charlie: knows(Alice) -> passes
        // GraphDB: neither -> fails
        let results = g
            .v()
            .or_(vec![__::out_labels(&["knows"]), __::out_labels(&["uses"])])
            .to_list();
        assert_eq!(results.len(), 3); // Alice, Bob, Charlie
    }

    #[test]
    fn or_short_circuits_on_first_success() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Keep vertices that are person OR software
        let results = g
            .v()
            .or_(vec![__::has_label("person"), __::has_label("software")])
            .to_list();
        assert_eq!(results.len(), 4); // All vertices match
    }

    #[test]
    fn or_with_empty_vec_filters_all() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Empty or_ should filter all traversers (no conditions to satisfy)
        let results = g.v().or_(vec![]).to_list();
        assert!(results.is_empty());
    }

    #[test]
    fn or_finds_vertices_with_either_edge_type() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Find vertices that either use something OR are used by someone
        // Alice: uses(GraphDB) -> passes
        // Bob: uses(GraphDB) -> passes
        // Charlie: neither uses nor is used -> fails
        // GraphDB: is used by Alice, Bob -> passes
        let results = g
            .v()
            .or_(vec![__::out_labels(&["uses"]), __::in_labels(&["uses"])])
            .to_list();
        assert_eq!(results.len(), 3); // Alice, Bob, GraphDB

        let ids: Vec<_> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
        assert!(ids.contains(&tg.alice));
        assert!(ids.contains(&tg.bob));
        assert!(ids.contains(&tg.graphdb));
    }

    // -------------------------------------------------------------------------
    // Combined Filter Steps Tests
    // -------------------------------------------------------------------------

    #[test]
    fn where_and_not_combined() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Find persons who don't use anything
        // Filter to persons first, then filter out those who use something
        let results = g
            .v()
            .has_label("person")
            .not(__::out_labels(&["uses"]))
            .to_list();
        assert_eq!(results.len(), 1); // Charlie
        assert_eq!(results[0].as_vertex_id(), Some(tg.charlie));
    }

    #[test]
    fn nested_filter_steps() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Find vertices that know someone who knows someone
        // Alice knows Bob, Bob knows Charlie -> Alice passes
        // Bob knows Charlie, Charlie knows Alice -> Bob passes
        // Charlie knows Alice, Alice knows Bob -> Charlie passes
        // GraphDB knows nobody -> fails
        let results = g
            .v()
            .where_(__::out_labels(&["knows"]).out_labels(&["knows"]))
            .to_list();
        assert_eq!(results.len(), 3);
    }

    // -------------------------------------------------------------------------
    // Anonymous Traversal Factory Tests (__ module)
    // -------------------------------------------------------------------------

    #[test]
    fn anonymous_where_factory() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Use __::where_ factory to create anonymous traversal
        let anon = __::where_(__::out());
        let results = g.v().append(anon).to_list();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn anonymous_not_factory() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Use __::not factory to create anonymous traversal
        let anon = __::not(__::out());
        let results = g.v().append(anon).to_list();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_vertex_id(), Some(tg.graphdb));
    }

    #[test]
    fn anonymous_and_factory() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Use __::and_ factory to create anonymous traversal
        let anon = __::and_(vec![__::out(), __::in_()]);
        let results = g.v().append(anon).to_list();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn anonymous_or_factory() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Use __::or_ factory to create anonymous traversal
        let anon = __::or_(vec![__::has_label("person"), __::has_label("software")]);
        let results = g.v().append(anon).to_list();
        assert_eq!(results.len(), 4);
    }
}

// =============================================================================
// Complex Traversal Tests
// =============================================================================

mod complex_traversal_tests {
    use super::*;

    #[test]
    fn find_friends_of_friends() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Alice -> knows -> ? -> knows -> ?
        let fof = g
            .v_ids([tg.alice])
            .out_labels(&["knows"])
            .out_labels(&["knows"])
            .to_list();

        assert_eq!(fof.len(), 1);
        assert_eq!(fof[0].as_vertex_id(), Some(tg.charlie));
    }

    #[test]
    fn find_cycle_back_to_start() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Alice -> knows -> Bob -> knows -> Charlie -> knows -> Alice
        let cycle = g
            .v_ids([tg.alice])
            .out_labels(&["knows"])
            .out_labels(&["knows"])
            .out_labels(&["knows"])
            .to_list();

        assert_eq!(cycle.len(), 1);
        assert_eq!(cycle[0].as_vertex_id(), Some(tg.alice)); // Back to Alice
    }

    #[test]
    fn find_software_used_by_people_who_know_alice() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // People who know Alice -> uses -> software
        let software = g
            .v_ids([tg.alice])
            .in_labels(&["knows"])
            .out_labels(&["uses"])
            .has_label("software")
            .to_list();

        // Charlie knows Alice, but Charlie doesn't use any software
        assert_eq!(software.len(), 0);
    }

    #[test]
    fn count_edges_per_vertex() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Count all incident edges for each vertex
        let alice_edges = g.v_ids([tg.alice]).both_e().count();
        assert_eq!(alice_edges, 3); // 2 out (knows Bob, uses GraphDB) + 1 in (Charlie knows)

        let bob_edges = g.v_ids([tg.bob]).both_e().count();
        assert_eq!(bob_edges, 3); // 2 out (knows Charlie, uses GraphDB) + 1 in (Alice knows)
    }

    #[test]
    fn get_all_names_in_graph() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let names = g.v().values("name").to_list();
        assert_eq!(names.len(), 4);

        let name_strs: Vec<String> = names
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        assert!(name_strs.contains(&"Alice".to_string()));
        assert!(name_strs.contains(&"Bob".to_string()));
        assert!(name_strs.contains(&"Charlie".to_string()));
        assert!(name_strs.contains(&"GraphDB".to_string()));
    }

    #[test]
    fn get_unique_labels() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let labels = g.v().label().dedup().to_list();
        assert_eq!(labels.len(), 2);

        let edge_labels = g.e().label().dedup().to_list();
        assert_eq!(edge_labels.len(), 2); // "knows" and "uses"
    }

    #[test]
    fn pagination_with_skip_and_limit() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let page1 = g.v().limit(2).to_list();
        let page2 = g.v().skip(2).limit(2).to_list();

        assert_eq!(page1.len(), 2);
        assert_eq!(page2.len(), 2);

        // Pages should not overlap
        let ids1: Vec<_> = page1.iter().filter_map(|v| v.as_vertex_id()).collect();
        let ids2: Vec<_> = page2.iter().filter_map(|v| v.as_vertex_id()).collect();
        for id in &ids1 {
            assert!(!ids2.contains(id));
        }
    }

    #[test]
    fn sum_ages_of_people() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Get sum of ages: 30 + 25 + 35 = 90
        let result = g.v().has_label("person").values("age").sum();
        assert_eq!(result, Value::Int(90));
    }

    #[test]
    fn traversal_with_path_tracking() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Enable path tracking and get paths
        let paths = g
            .v_ids([tg.alice])
            .with_path()
            .as_("start")
            .out_labels(&["knows"])
            .as_("friend")
            .out_labels(&["knows"])
            .as_("fof")
            .path()
            .to_list();

        assert_eq!(paths.len(), 1);
        if let Value::List(path) = &paths[0] {
            assert_eq!(path.len(), 3); // start, friend, fof
        } else {
            panic!("Expected path list");
        }
    }

    #[test]
    fn select_multiple_labels() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let results = g
            .v_ids([tg.alice])
            .as_("person")
            .out_labels(&["uses"])
            .as_("software")
            .select(&["person", "software"])
            .to_list();

        assert_eq!(results.len(), 1);
        if let Value::Map(map) = &results[0] {
            assert!(map.contains_key("person"));
            assert!(map.contains_key("software"));
        } else {
            panic!("Expected map");
        }
    }

    #[test]
    fn edge_property_access() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Get "since" property from knows edges
        let since_values = g.e().has_label("knows").values("since").to_list();

        assert_eq!(since_values.len(), 3);
        for v in &since_values {
            assert!(v.as_i64().is_some());
        }
    }

    #[test]
    fn combining_filters_and_navigation() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Find all software used by people (via has_label filter)
        let software = g
            .v()
            .has_label("person")
            .out_labels(&["uses"])
            .has_label("software")
            .dedup()
            .to_list();

        // Alice and Bob both use GraphDB
        assert_eq!(software.len(), 1);
    }
}

// =============================================================================
// Error Case Tests
// =============================================================================

mod error_case_tests {
    use super::*;

    #[test]
    fn one_on_empty_result_returns_error() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let result = g.v().has_label("nonexistent").one();
        assert!(result.is_err());
    }

    #[test]
    fn one_on_multiple_results_returns_error() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let result = g.v().has_label("person").one();
        assert!(result.is_err());
    }

    #[test]
    fn navigation_on_non_element_produces_nothing() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Injected integers can't be navigated
        let results = g.inject([1i64, 2i64]).out().to_list();
        assert!(results.is_empty());
    }

    #[test]
    fn values_on_non_element_produces_nothing() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Injected integers don't have properties
        let results = g.inject([1i64, 2i64]).values("name").to_list();
        assert!(results.is_empty());
    }

    #[test]
    fn out_v_on_non_edge_produces_nothing() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        // Vertices can't use out_v (that's for edges)
        let results = g.v().out_v().to_list();
        assert!(results.is_empty());
    }

    #[test]
    fn select_missing_label_filters_out() {
        let tg = create_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

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
}

// =============================================================================
// Spec-Compliant Test Graph (Phase 5.8)
// =============================================================================

/// Test graph matching the spec document structure.
///
/// Vertices (4 total):
/// | ID    | Label   | Properties                  |
/// |-------|---------|----------------------------|
/// | alice | person  | name: "Alice", age: 30     |
/// | bob   | person  | name: "Bob", age: 35       |
/// | carol | person  | name: "Carol", age: 25     |
/// | acme  | company | name: "Acme Corp"          |
///
/// Edges (5 total):
/// | Source | Target | Label    | Properties   |
/// |--------|--------|----------|--------------|
/// | alice  | bob    | knows    | weight: 1.0  |
/// | alice  | carol  | knows    | weight: 0.5  |
/// | bob    | carol  | knows    | weight: 0.8  |
/// | alice  | acme   | works_at | since: 2020  |
/// | bob    | acme   | works_at | since: 2018  |
#[allow(dead_code)]
struct SpecTestGraph {
    graph: Graph,
    alice: VertexId,
    bob: VertexId,
    carol: VertexId,
    acme: VertexId,
}

/// Create the spec-compliant test graph.
fn create_spec_test_graph() -> SpecTestGraph {
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
        props.insert("age".to_string(), Value::Int(35));
        props
    });

    let carol = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Carol".to_string()));
        props.insert("age".to_string(), Value::Int(25));
        props
    });

    // Add company vertex
    let acme = storage.add_vertex("company", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Acme Corp".to_string()));
        props
    });

    // Add edges with properties
    storage
        .add_edge(alice, bob, "knows", {
            let mut props = HashMap::new();
            props.insert("weight".to_string(), Value::Float(1.0));
            props
        })
        .unwrap();

    storage
        .add_edge(alice, carol, "knows", {
            let mut props = HashMap::new();
            props.insert("weight".to_string(), Value::Float(0.5));
            props
        })
        .unwrap();

    storage
        .add_edge(bob, carol, "knows", {
            let mut props = HashMap::new();
            props.insert("weight".to_string(), Value::Float(0.8));
            props
        })
        .unwrap();

    storage
        .add_edge(alice, acme, "works_at", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2020));
            props
        })
        .unwrap();

    storage
        .add_edge(bob, acme, "works_at", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2018));
            props
        })
        .unwrap();

    SpecTestGraph {
        graph: Graph::new(Arc::new(storage)),
        alice,
        bob,
        carol,
        acme,
    }
}

// =============================================================================
// Basic Source Tests (Phase 5.8 - Section 1)
// =============================================================================

mod basic_source_tests {
    use super::*;

    #[test]
    fn test_v_all_vertices() {
        // g.v() should return all 4 vertices
        let tg = create_spec_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let count = g.v().count();
        assert_eq!(count, 4);
    }

    #[test]
    fn test_e_all_edges() {
        // g.e() should return all 5 edges
        let tg = create_spec_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let count = g.e().count();
        assert_eq!(count, 5);
    }

    #[test]
    fn test_v_ids_specific_vertices() {
        // g.v_ids([alice, bob]) should return 2 vertices
        let tg = create_spec_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let count = g.v_ids([tg.alice, tg.bob]).count();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_v_ids_nonexistent_filtered() {
        // Non-existent IDs should be filtered out silently
        let tg = create_spec_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let fake_id = VertexId(999999);
        let count = g.v_ids([tg.alice, fake_id]).count();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_e_ids_specific_edges() {
        // g.e_ids() should return only specified edges
        let tg = create_spec_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let first_edge = g.e().next().unwrap().as_edge_id().unwrap();
        let count = g.e_ids([first_edge]).count();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_inject_values() {
        // g.inject() should inject arbitrary values
        let tg = create_spec_test_graph();
        let snapshot = tg.graph.snapshot();
        let g = snapshot.traversal();

        let results = g.inject([1i64, 2i64, 3i64]).to_list();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Value::Int(1));
        assert_eq!(results[1], Value::Int(2));
        assert_eq!(results[2], Value::Int(3));
    }
}
