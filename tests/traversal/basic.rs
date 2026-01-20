//! Basic traversal tests including source steps (v, e, inject) and basic operations.

use std::collections::HashMap;

use interstellar::storage::Graph;
use interstellar::value::{EdgeId, Value, VertexId};

use crate::common::graphs::{create_empty_graph, create_small_graph};

// =============================================================================
// Spec-Compliant Test Graph (used by basic_source_tests)
// =============================================================================

/// Test graph matching the spec document structure (different from small_graph).
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

fn create_spec_test_graph() -> SpecTestGraph {
    let graph = Graph::new();

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

    let acme = graph.add_vertex("company", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Acme Corp".to_string()));
        props
    });

    graph
        .add_edge(alice, bob, "knows", {
            let mut props = HashMap::new();
            props.insert("weight".to_string(), Value::Float(1.0));
            props
        })
        .unwrap();

    graph
        .add_edge(alice, carol, "knows", {
            let mut props = HashMap::new();
            props.insert("weight".to_string(), Value::Float(0.5));
            props
        })
        .unwrap();

    graph
        .add_edge(bob, carol, "knows", {
            let mut props = HashMap::new();
            props.insert("weight".to_string(), Value::Float(0.8));
            props
        })
        .unwrap();

    graph
        .add_edge(alice, acme, "works_at", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2020));
            props
        })
        .unwrap();

    graph
        .add_edge(bob, acme, "works_at", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2018));
            props
        })
        .unwrap();

    SpecTestGraph {
        graph,
        alice,
        bob,
        carol,
        acme,
    }
}

// =============================================================================
// Basic Tests
// =============================================================================

#[test]
fn v_returns_all_vertices() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let vertices = g.v().to_list();
    assert_eq!(vertices.len(), 4);

    for v in &vertices {
        assert!(v.is_vertex(), "Expected vertex, got {:?}", v);
    }
}

#[test]
fn e_returns_all_edges() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let edges = g.e().to_list();
    assert_eq!(edges.len(), 5);

    for e in &edges {
        assert!(e.is_edge(), "Expected edge, got {:?}", e);
    }
}

#[test]
fn v_ids_returns_specific_vertices() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let vertices = g.v_ids([tg.alice, tg.charlie]).to_list();
    assert_eq!(vertices.len(), 2);

    let ids: Vec<VertexId> = vertices.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.charlie));
}

#[test]
fn e_ids_returns_specific_edges() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

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
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let results = g.inject([1i64, 2i64, 3i64]).to_list();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Int(1));
    assert_eq!(results[1], Value::Int(2));
    assert_eq!(results[2], Value::Int(3));
}

#[test]
fn count_returns_correct_value() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    assert_eq!(g.v().count(), 4);
    assert_eq!(g.e().count(), 5);
}

#[test]
fn empty_graph_returns_empty_results() {
    let graph = create_empty_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    assert_eq!(g.v().count(), 0);
    assert_eq!(g.e().count(), 0);
    assert!(g.v().to_list().is_empty());
    assert!(g.e().to_list().is_empty());
}

#[test]
fn nonexistent_vertex_ids_filtered_out() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let results = g.v_ids([tg.alice]).to_list();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

// =============================================================================
// Basic Source Tests (Phase 5.8 - Section 1)
// =============================================================================

#[test]
fn test_v_all_vertices() {
    let tg = create_spec_test_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let count = g.v().count();
    assert_eq!(count, 4);
}

#[test]
fn test_e_all_edges() {
    let tg = create_spec_test_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let count = g.e().count();
    assert_eq!(count, 5);
}

#[test]
fn test_v_ids_specific_vertices() {
    let tg = create_spec_test_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let count = g.v_ids([tg.alice, tg.bob]).count();
    assert_eq!(count, 2);
}

#[test]
fn test_v_ids_nonexistent_filtered() {
    let tg = create_spec_test_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let fake_id = VertexId(999999);
    let count = g.v_ids([tg.alice, fake_id]).count();
    assert_eq!(count, 1);
}

#[test]
fn test_e_ids_specific_edges() {
    let tg = create_spec_test_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let first_edge = g.e().next().unwrap().as_edge_id().unwrap();
    let count = g.e_ids([first_edge]).count();
    assert_eq!(count, 1);
}

#[test]
fn test_inject_values() {
    let tg = create_spec_test_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let results = g.inject([1i64, 2i64, 3i64]).to_list();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Int(1));
    assert_eq!(results[1], Value::Int(2));
    assert_eq!(results[2], Value::Int(3));
}
