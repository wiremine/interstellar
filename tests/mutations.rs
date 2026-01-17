//! Integration tests for mutation steps.
//!
//! Tests for `addV()`, `addE()`, `property()`, and `drop()` mutation steps.

use std::collections::HashMap;

use interstellar::graph::Graph;
use interstellar::storage::{GraphStorage, InMemoryGraph};
use interstellar::traversal::{MutationExecutor, MutationResult, PendingMutation};
use interstellar::value::{EdgeId, Value, VertexId};

// =============================================================================
// Helper functions
// =============================================================================

/// Creates a test graph with some initial data.
fn create_test_graph() -> InMemoryGraph {
    let mut storage = InMemoryGraph::new();

    // Add vertices
    let alice_id = storage.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("age".to_string(), Value::Int(30)),
        ]),
    );

    let bob_id = storage.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::String("Bob".to_string())),
            ("age".to_string(), Value::Int(25)),
        ]),
    );

    let _software_id = storage.add_vertex(
        "software",
        HashMap::from([("name".to_string(), Value::String("Gremlin".to_string()))]),
    );

    // Add edges
    storage
        .add_edge(
            alice_id,
            bob_id,
            "knows",
            HashMap::from([("since".to_string(), Value::Int(2020))]),
        )
        .unwrap();

    storage
}

/// Executes pending mutations from traversal results.
fn execute_mutations(
    storage: &mut InMemoryGraph,
    traversers: impl Iterator<Item = interstellar::traversal::Traverser>,
) -> MutationResult {
    let mut executor = MutationExecutor::new(storage);
    executor.execute(traversers)
}

// =============================================================================
// AddV Tests
// =============================================================================

#[test]
fn add_v_creates_pending_vertex() {
    let storage = InMemoryGraph::new();
    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // Execute add_v traversal - creates a pending mutation marker
    let results: Vec<Value> = g.add_v("person").to_list();

    assert_eq!(results.len(), 1);

    // The result should be a pending add_v marker
    if let Value::Map(map) = &results[0] {
        assert!(map.contains_key("__pending_add_v"));
        assert_eq!(map.get("label"), Some(&Value::String("person".to_string())));
    } else {
        panic!("Expected Map value with pending marker");
    }
}

#[test]
fn add_v_with_properties_creates_pending_vertex() {
    let storage = InMemoryGraph::new();
    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // Execute add_v with properties
    let results: Vec<Value> = g
        .add_v("person")
        .property("name", "Charlie")
        .property("age", 35i64)
        .to_list();

    assert_eq!(results.len(), 1);

    // Verify the pending marker has properties
    if let Value::Map(map) = &results[0] {
        assert!(map.contains_key("__pending_add_v"));
        if let Some(Value::Map(props)) = map.get("properties") {
            assert_eq!(
                props.get("name"),
                Some(&Value::String("Charlie".to_string()))
            );
            assert_eq!(props.get("age"), Some(&Value::Int(35)));
        } else {
            panic!("Expected properties map");
        }
    } else {
        panic!("Expected Map value");
    }
}

#[test]
fn mutation_executor_creates_vertex() {
    let mut storage = InMemoryGraph::new();
    let initial_count = storage.vertex_count();

    // Create pending add_v mutation
    let mutation = PendingMutation::AddVertex {
        label: "person".to_string(),
        properties: HashMap::from([
            ("name".to_string(), Value::String("Diana".to_string())),
            ("age".to_string(), Value::Int(28)),
        ]),
    };

    // Execute the mutation
    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute_mutation(mutation);

    // Verify vertex was created
    assert!(result.is_some());
    if let Some(Value::Vertex(id)) = result {
        let vertex = storage.get_vertex(id).expect("Vertex should exist");
        assert_eq!(vertex.label, "person");
        assert_eq!(
            vertex.properties.get("name"),
            Some(&Value::String("Diana".to_string()))
        );
        assert_eq!(vertex.properties.get("age"), Some(&Value::Int(28)));
    }

    assert_eq!(storage.vertex_count(), initial_count + 1);
}

#[test]
fn mutation_executor_from_traversal() {
    let mut storage = InMemoryGraph::new();

    // First run the traversal to get pending mutations
    {
        let graph = Graph::in_memory();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let traversers: Vec<_> = g
            .add_v("person")
            .property("name", "Eve")
            .execute()
            .collect();

        // Now execute mutations on the actual storage
        let result = execute_mutations(&mut storage, traversers.into_iter());

        assert_eq!(result.vertices_added, 1);
        assert_eq!(result.values.len(), 1);
    }

    // Verify the vertex exists
    assert_eq!(storage.vertex_count(), 1);
    let vertex = storage
        .all_vertices()
        .next()
        .expect("Should have one vertex");
    assert_eq!(vertex.label, "person");
    assert_eq!(
        vertex.properties.get("name"),
        Some(&Value::String("Eve".to_string()))
    );
}

// =============================================================================
// AddE Tests
// =============================================================================

#[test]
fn add_e_creates_pending_edge() {
    let storage = create_test_graph();
    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // Get vertex IDs
    let vertices: Vec<Value> = g.v().to_list();
    let v1 = vertices[0].as_vertex_id().unwrap();
    let v2 = vertices[1].as_vertex_id().unwrap();

    // Create add_e traversal
    let results: Vec<Value> = g.add_e("friend").from_vertex(v1).to_vertex(v2).to_list();

    assert_eq!(results.len(), 1);

    // Verify pending edge marker
    if let Value::Map(map) = &results[0] {
        assert!(map.contains_key("__pending_add_e"));
        assert_eq!(map.get("label"), Some(&Value::String("friend".to_string())));
        assert_eq!(map.get("from"), Some(&Value::Vertex(v1)));
        assert_eq!(map.get("to"), Some(&Value::Vertex(v2)));
    } else {
        panic!("Expected Map with pending edge marker");
    }
}

#[test]
fn add_e_from_bound_traversal() {
    let storage = create_test_graph();
    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // Get Alice's vertex ID
    let alice_vertex = g
        .v()
        .has_value("name", "Alice")
        .next()
        .expect("Alice should exist");
    let alice_id = alice_vertex.as_vertex_id().unwrap();

    // Get Bob's vertex ID
    let bob_vertex = g
        .v()
        .has_value("name", "Bob")
        .next()
        .expect("Bob should exist");
    let bob_id = bob_vertex.as_vertex_id().unwrap();

    // Create edge from Alice to Bob using bound traversal
    let results: Vec<Value> = g
        .v_ids([alice_id])
        .add_e("works_with")
        .to_vertex(bob_id)
        .property("project", "Interstellar")
        .to_list();

    assert_eq!(results.len(), 1);

    // Verify pending edge
    if let Value::Map(map) = &results[0] {
        assert!(map.contains_key("__pending_add_e"));
        assert_eq!(map.get("from"), Some(&Value::Vertex(alice_id)));
        assert_eq!(map.get("to"), Some(&Value::Vertex(bob_id)));

        if let Some(Value::Map(props)) = map.get("properties") {
            assert_eq!(
                props.get("project"),
                Some(&Value::String("Interstellar".to_string()))
            );
        }
    }
}

#[test]
fn mutation_executor_creates_edge() {
    let mut storage = create_test_graph();
    let initial_edge_count = storage.edge_count();

    // Get two vertex IDs
    let vertices: Vec<_> = storage.all_vertices().collect();
    let v1 = vertices[0].id;
    let v2 = vertices[1].id;

    // Create pending add_e mutation
    let mutation = PendingMutation::AddEdge {
        label: "colleague".to_string(),
        from: v1,
        to: v2,
        properties: HashMap::from([("dept".to_string(), Value::String("Engineering".to_string()))]),
    };

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute_mutation(mutation);

    // Verify edge was created
    assert!(result.is_some());
    if let Some(Value::Edge(id)) = result {
        let edge = storage.get_edge(id).expect("Edge should exist");
        assert_eq!(edge.label, "colleague");
        assert_eq!(edge.src, v1);
        assert_eq!(edge.dst, v2);
        assert_eq!(
            edge.properties.get("dept"),
            Some(&Value::String("Engineering".to_string()))
        );
    }

    assert_eq!(storage.edge_count(), initial_edge_count + 1);
}

// =============================================================================
// Property Tests
// =============================================================================

#[test]
fn property_on_vertex_creates_pending_update() {
    let storage = create_test_graph();
    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // Get a vertex and add a property
    let results: Vec<Value> = g
        .v()
        .has_value("name", "Alice")
        .property("status", "active")
        .to_list();

    assert_eq!(results.len(), 1);

    // Should be a pending property update
    if let Value::Map(map) = &results[0] {
        assert!(map.contains_key("__pending_property_vertex"));
        assert_eq!(map.get("key"), Some(&Value::String("status".to_string())));
        assert_eq!(map.get("value"), Some(&Value::String("active".to_string())));
    }
}

#[test]
fn mutation_executor_sets_vertex_property() {
    let mut storage = create_test_graph();

    // Find Alice's vertex ID
    let alice_id = storage
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
        .expect("Alice should exist")
        .id;

    // Create pending property mutation
    let mutation = PendingMutation::SetVertexProperty {
        id: alice_id,
        key: "email".to_string(),
        value: Value::String("alice@example.com".to_string()),
    };

    let mut executor = MutationExecutor::new(&mut storage);
    executor.execute_mutation(mutation);

    // Verify property was set
    let alice = storage.get_vertex(alice_id).expect("Alice should exist");
    assert_eq!(
        alice.properties.get("email"),
        Some(&Value::String("alice@example.com".to_string()))
    );
}

#[test]
fn mutation_executor_sets_edge_property() {
    let mut storage = create_test_graph();

    // Get Alice's vertex (she has the outgoing edge)
    let alice = storage
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
        .expect("Alice should exist");
    let edge = storage
        .out_edges(alice.id)
        .next()
        .expect("Alice should have an outgoing edge");
    let edge_id = edge.id;

    // Create pending edge property mutation
    let mutation = PendingMutation::SetEdgeProperty {
        id: edge_id,
        key: "weight".to_string(),
        value: Value::Float(0.8),
    };

    let mut executor = MutationExecutor::new(&mut storage);
    executor.execute_mutation(mutation);

    // Verify property was set
    let edge = storage.get_edge(edge_id).expect("Edge should exist");
    assert_eq!(edge.properties.get("weight"), Some(&Value::Float(0.8)));
}

// =============================================================================
// Drop Tests
// =============================================================================

#[test]
fn drop_vertex_creates_pending_deletion() {
    let storage = create_test_graph();
    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // Get a vertex and drop it
    let results: Vec<Value> = g.v().has_value("name", "Alice").drop().to_list();

    assert_eq!(results.len(), 1);

    // Should be a pending drop marker
    if let Value::Map(map) = &results[0] {
        assert!(map.contains_key("__pending_drop_vertex"));
    }
}

#[test]
fn mutation_executor_removes_vertex() {
    let mut storage = create_test_graph();
    let initial_count = storage.vertex_count();

    // Find a vertex to remove
    let vertex_id = storage
        .all_vertices()
        .next()
        .expect("Should have vertices")
        .id;

    let mutation = PendingMutation::DropVertex { id: vertex_id };

    let mut executor = MutationExecutor::new(&mut storage);
    executor.execute_mutation(mutation);

    // Verify vertex was removed
    assert!(storage.get_vertex(vertex_id).is_none());
    assert_eq!(storage.vertex_count(), initial_count - 1);
}

#[test]
fn mutation_executor_removes_edge() {
    let mut storage = create_test_graph();
    let initial_count = storage.edge_count();

    // Find an edge to remove - Alice has the outgoing edge
    let alice = storage
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
        .expect("Alice should exist");
    let edge_id = storage
        .out_edges(alice.id)
        .next()
        .expect("Alice should have edges")
        .id;

    let mutation = PendingMutation::DropEdge { id: edge_id };

    let mut executor = MutationExecutor::new(&mut storage);
    executor.execute_mutation(mutation);

    // Verify edge was removed
    assert!(storage.get_edge(edge_id).is_none());
    assert_eq!(storage.edge_count(), initial_count - 1);
}

// =============================================================================
// MutationResult Tests
// =============================================================================

#[test]
fn mutation_result_tracks_statistics() {
    let mut storage = InMemoryGraph::new();

    // Create multiple pending mutations
    let traversers = vec![
        interstellar::traversal::Traverser::new(Value::Map(HashMap::from([
            ("__pending_add_v".to_string(), Value::Bool(true)),
            ("label".to_string(), Value::String("person".to_string())),
            ("properties".to_string(), Value::Map(HashMap::new())),
        ]))),
        interstellar::traversal::Traverser::new(Value::Map(HashMap::from([
            ("__pending_add_v".to_string(), Value::Bool(true)),
            ("label".to_string(), Value::String("person".to_string())),
            ("properties".to_string(), Value::Map(HashMap::new())),
        ]))),
    ];

    let result = execute_mutations(&mut storage, traversers.into_iter());

    assert_eq!(result.vertices_added, 2);
    assert_eq!(result.values.len(), 2);
}

#[test]
fn mutation_result_passes_through_non_mutations() {
    let mut storage = InMemoryGraph::new();

    // Mix of pending mutations and regular values
    let traversers = vec![
        interstellar::traversal::Traverser::new(Value::Int(42)),
        interstellar::traversal::Traverser::new(Value::Map(HashMap::from([
            ("__pending_add_v".to_string(), Value::Bool(true)),
            ("label".to_string(), Value::String("test".to_string())),
            ("properties".to_string(), Value::Map(HashMap::new())),
        ]))),
        interstellar::traversal::Traverser::new(Value::String("hello".to_string())),
    ];

    let result = execute_mutations(&mut storage, traversers.into_iter());

    // Should have 3 values: the int, the new vertex, and the string
    assert_eq!(result.values.len(), 3);
    assert_eq!(result.vertices_added, 1);

    // First and third values should be passed through
    assert_eq!(result.values[0], Value::Int(42));
    assert_eq!(result.values[2], Value::String("hello".to_string()));
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]
fn drop_non_existent_vertex_is_silent() {
    let mut storage = InMemoryGraph::new();

    // Try to drop a vertex that doesn't exist
    let mutation = PendingMutation::DropVertex { id: VertexId(9999) };

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute_mutation(mutation);

    // Should return None (no error, just silently fails)
    assert!(result.is_none());
}

#[test]
fn add_edge_to_non_existent_vertex_is_silent() {
    let mut storage = InMemoryGraph::new();

    // Try to add edge between non-existent vertices
    let mutation = PendingMutation::AddEdge {
        label: "test".to_string(),
        from: VertexId(1),
        to: VertexId(2),
        properties: HashMap::new(),
    };

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute_mutation(mutation);

    // Should return None (edge not created)
    assert!(result.is_none());
    assert_eq!(storage.edge_count(), 0);
}

#[test]
fn property_on_non_existent_vertex_is_silent() {
    let mut storage = InMemoryGraph::new();

    let mutation = PendingMutation::SetVertexProperty {
        id: VertexId(9999),
        key: "test".to_string(),
        value: Value::Int(1),
    };

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute_mutation(mutation);

    // Should return None (property not set)
    assert!(result.is_none());
}

// =============================================================================
// PendingMutation Parsing Tests
// =============================================================================

#[test]
fn pending_mutation_parses_add_v() {
    let value = Value::Map(HashMap::from([
        ("__pending_add_v".to_string(), Value::Bool(true)),
        ("label".to_string(), Value::String("test".to_string())),
        (
            "properties".to_string(),
            Value::Map(HashMap::from([(
                "key".to_string(),
                Value::String("value".to_string()),
            )])),
        ),
    ]));

    let mutation = PendingMutation::from_value(&value);
    assert!(matches!(
        mutation,
        Some(PendingMutation::AddVertex { label, properties })
        if label == "test" && properties.len() == 1
    ));
}

#[test]
fn pending_mutation_parses_add_e() {
    let value = Value::Map(HashMap::from([
        ("__pending_add_e".to_string(), Value::Bool(true)),
        ("label".to_string(), Value::String("edge".to_string())),
        ("from".to_string(), Value::Vertex(VertexId(1))),
        ("to".to_string(), Value::Vertex(VertexId(2))),
        ("properties".to_string(), Value::Map(HashMap::new())),
    ]));

    let mutation = PendingMutation::from_value(&value);
    assert!(matches!(
        mutation,
        Some(PendingMutation::AddEdge { label, from, to, .. })
        if label == "edge" && from == VertexId(1) && to == VertexId(2)
    ));
}

#[test]
fn pending_mutation_parses_drop_vertex() {
    let value = Value::Map(HashMap::from([
        ("__pending_drop_vertex".to_string(), Value::Bool(true)),
        ("id".to_string(), Value::Vertex(VertexId(42))),
    ]));

    let mutation = PendingMutation::from_value(&value);
    assert!(matches!(
        mutation,
        Some(PendingMutation::DropVertex { id }) if id == VertexId(42)
    ));
}

#[test]
fn pending_mutation_parses_drop_edge() {
    let value = Value::Map(HashMap::from([
        ("__pending_drop_edge".to_string(), Value::Bool(true)),
        ("id".to_string(), Value::Edge(EdgeId(7))),
    ]));

    let mutation = PendingMutation::from_value(&value);
    assert!(matches!(
        mutation,
        Some(PendingMutation::DropEdge { id }) if id == EdgeId(7)
    ));
}

#[test]
fn pending_mutation_ignores_regular_values() {
    assert!(PendingMutation::from_value(&Value::Int(42)).is_none());
    assert!(PendingMutation::from_value(&Value::String("test".to_string())).is_none());
    assert!(PendingMutation::from_value(&Value::Vertex(VertexId(1))).is_none());
    assert!(PendingMutation::from_value(&Value::Bool(true)).is_none());
}
