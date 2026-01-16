//! Additional coverage tests for traversal/mutation.rs.
//!
//! These tests target uncovered branches and edge cases in mutation steps.

use std::collections::HashMap;

use intersteller::graph::Graph;
use intersteller::storage::{GraphStorage, InMemoryGraph};
use intersteller::traversal::mutation::{
    AddEStep, AddVStep, DropStep, EdgeEndpoint, MutationExecutor, MutationResult, PendingMutation,
    PropertyStep,
};
use intersteller::traversal::step::AnyStep;
use intersteller::traversal::Traverser;
use intersteller::value::{EdgeId, Value, VertexId};

// =============================================================================
// Helper Functions
// =============================================================================

fn create_test_graph() -> InMemoryGraph {
    let mut storage = InMemoryGraph::new();

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

// =============================================================================
// AddVStep Tests
// =============================================================================

#[test]
fn add_v_step_clone_box() {
    let step = AddVStep::new("person");
    let cloned = step.clone_box();
    assert_eq!(cloned.name(), "addV");
}

#[test]
fn add_v_step_with_empty_properties() {
    let step = AddVStep::with_properties("person", HashMap::new());
    assert!(step.properties().is_empty());
}

#[test]
fn add_v_step_accessors() {
    let props = HashMap::from([("name".to_string(), Value::String("Test".to_string()))]);
    let step = AddVStep::with_properties("test_label", props.clone());

    assert_eq!(step.label(), "test_label");
    assert_eq!(step.properties().len(), 1);
    assert_eq!(
        step.properties().get("name"),
        Some(&Value::String("Test".to_string()))
    );
}

// =============================================================================
// PropertyStep Tests
// =============================================================================

#[test]
fn property_step_clone_box() {
    let step = PropertyStep::new("name", "Alice");
    let cloned = step.clone_box();
    assert_eq!(cloned.name(), "property");
}

#[test]
fn property_step_accessors() {
    let step = PropertyStep::new("key", 42i64);
    assert_eq!(step.key(), "key");
    assert_eq!(step.value(), &Value::Int(42));
}

// =============================================================================
// DropStep Tests
// =============================================================================

#[test]
fn drop_step_clone_box() {
    let step = DropStep::new();
    let cloned = step.clone_box();
    assert_eq!(cloned.name(), "drop");
}

#[test]
fn drop_step_default() {
    // Test Default trait
    let step = DropStep::default();
    assert_eq!(step.name(), "drop");
}

// =============================================================================
// AddEStep Tests
// =============================================================================

#[test]
fn add_e_step_clone_box() {
    let step = AddEStep::new("knows");
    let cloned = step.clone_box();
    assert_eq!(cloned.name(), "addE");
}

#[test]
fn add_e_step_to_traverser() {
    let step = AddEStep::new("knows")
        .from_vertex(VertexId(1))
        .to_traverser();

    assert!(matches!(step.to_endpoint(), Some(EdgeEndpoint::Traverser)));
}

#[test]
fn add_e_step_all_builder_methods() {
    let step = AddEStep::new("knows")
        .from_vertex(VertexId(1))
        .to_vertex(VertexId(2))
        .property("since", 2020i64);

    assert_eq!(step.label(), "knows");
    assert!(matches!(
        step.from_endpoint(),
        Some(EdgeEndpoint::VertexId(VertexId(1)))
    ));
    assert!(matches!(
        step.to_endpoint(),
        Some(EdgeEndpoint::VertexId(VertexId(2)))
    ));
}

#[test]
fn add_e_step_from_traverser() {
    let step = AddEStep::new("knows")
        .from_traverser()
        .to_vertex(VertexId(2));
    assert!(matches!(
        step.from_endpoint(),
        Some(EdgeEndpoint::Traverser)
    ));
}

#[test]
fn add_e_step_from_label_to_label() {
    let step = AddEStep::new("knows").from_label("start").to_label("end");

    assert!(matches!(
        step.from_endpoint(),
        Some(EdgeEndpoint::StepLabel(ref s)) if s == "start"
    ));
    assert!(matches!(
        step.to_endpoint(),
        Some(EdgeEndpoint::StepLabel(ref s)) if s == "end"
    ));
}

// =============================================================================
// PendingMutation Tests
// =============================================================================

#[test]
fn pending_mutation_from_set_vertex_property() {
    let value = Value::Map(HashMap::from([
        ("__pending_property_vertex".to_string(), Value::Bool(true)),
        ("id".to_string(), Value::Vertex(VertexId(10))),
        ("key".to_string(), Value::String("status".to_string())),
        ("value".to_string(), Value::String("active".to_string())),
    ]));

    let mutation = PendingMutation::from_value(&value);
    assert!(matches!(
        mutation,
        Some(PendingMutation::SetVertexProperty { id, key, value })
        if id == VertexId(10) && key == "status" && value == Value::String("active".to_string())
    ));
}

#[test]
fn pending_mutation_from_set_edge_property() {
    let value = Value::Map(HashMap::from([
        ("__pending_property_edge".to_string(), Value::Bool(true)),
        ("id".to_string(), Value::Edge(EdgeId(20))),
        ("key".to_string(), Value::String("weight".to_string())),
        ("value".to_string(), Value::Int(5)),
    ]));

    let mutation = PendingMutation::from_value(&value);
    assert!(matches!(
        mutation,
        Some(PendingMutation::SetEdgeProperty { id, key, value })
        if id == EdgeId(20) && key == "weight" && value == Value::Int(5)
    ));
}

#[test]
fn pending_mutation_from_drop_edge() {
    let value = Value::Map(HashMap::from([
        ("__pending_drop_edge".to_string(), Value::Bool(true)),
        ("id".to_string(), Value::Edge(EdgeId(50))),
    ]));

    let mutation = PendingMutation::from_value(&value);
    assert!(matches!(
        mutation,
        Some(PendingMutation::DropEdge { id })
        if id == EdgeId(50)
    ));
}

#[test]
fn pending_mutation_from_add_v_missing_properties() {
    // Test add_v with missing properties (should use default)
    let value = Value::Map(HashMap::from([
        ("__pending_add_v".to_string(), Value::Bool(true)),
        ("label".to_string(), Value::String("item".to_string())),
        // No properties key
    ]));

    let mutation = PendingMutation::from_value(&value);
    assert!(matches!(
        mutation,
        Some(PendingMutation::AddVertex { label, properties })
        if label == "item" && properties.is_empty()
    ));
}

#[test]
fn pending_mutation_from_add_v_missing_label() {
    // Test add_v with missing label (should use empty string)
    let value = Value::Map(HashMap::from([
        ("__pending_add_v".to_string(), Value::Bool(true)),
        // No label key
    ]));

    let mutation = PendingMutation::from_value(&value);
    assert!(matches!(
        mutation,
        Some(PendingMutation::AddVertex { label, .. })
        if label.is_empty()
    ));
}

#[test]
fn pending_mutation_from_map_without_marker() {
    // Test that a map without any pending marker returns None
    let value = Value::Map(HashMap::from([
        ("some_key".to_string(), Value::Int(42)),
        ("other_key".to_string(), Value::String("value".to_string())),
    ]));

    let mutation = PendingMutation::from_value(&value);
    assert!(mutation.is_none());
}

#[test]
fn pending_mutation_from_add_e_missing_from_or_to() {
    // Test add_e with missing from (should return None)
    let value = Value::Map(HashMap::from([
        ("__pending_add_e".to_string(), Value::Bool(true)),
        ("label".to_string(), Value::String("knows".to_string())),
        ("to".to_string(), Value::Vertex(VertexId(2))),
        // Missing "from"
    ]));

    let mutation = PendingMutation::from_value(&value);
    assert!(mutation.is_none());
}

#[test]
fn pending_mutation_from_set_vertex_property_missing_id() {
    let value = Value::Map(HashMap::from([
        ("__pending_property_vertex".to_string(), Value::Bool(true)),
        // Missing "id"
        ("key".to_string(), Value::String("status".to_string())),
        ("value".to_string(), Value::Int(1)),
    ]));

    let mutation = PendingMutation::from_value(&value);
    assert!(mutation.is_none());
}

#[test]
fn pending_mutation_from_drop_vertex_missing_id() {
    let value = Value::Map(HashMap::from([
        ("__pending_drop_vertex".to_string(), Value::Bool(true)),
        // Missing "id"
    ]));

    let mutation = PendingMutation::from_value(&value);
    assert!(mutation.is_none());
}

#[test]
fn pending_mutation_from_drop_edge_missing_id() {
    let value = Value::Map(HashMap::from([
        ("__pending_drop_edge".to_string(), Value::Bool(true)),
        // Missing "id"
    ]));

    let mutation = PendingMutation::from_value(&value);
    assert!(mutation.is_none());
}

#[test]
fn pending_mutation_from_set_edge_property_missing_id() {
    let value = Value::Map(HashMap::from([
        ("__pending_property_edge".to_string(), Value::Bool(true)),
        // Missing "id"
        ("key".to_string(), Value::String("prop".to_string())),
        ("value".to_string(), Value::Int(1)),
    ]));

    let mutation = PendingMutation::from_value(&value);
    assert!(mutation.is_none());
}

// =============================================================================
// MutationResult Tests
// =============================================================================

#[test]
fn mutation_result_new() {
    let result = MutationResult::new();
    assert!(result.values.is_empty());
    assert_eq!(result.vertices_added, 0);
    assert_eq!(result.edges_added, 0);
    assert_eq!(result.vertices_removed, 0);
    assert_eq!(result.edges_removed, 0);
    assert_eq!(result.properties_set, 0);
}

#[test]
fn mutation_result_default() {
    let result = MutationResult::default();
    assert!(result.values.is_empty());
}

// =============================================================================
// MutationExecutor Tests
// =============================================================================

#[test]
fn mutation_executor_execute_add_edge_failure() {
    let mut storage = InMemoryGraph::new();

    // Try to add edge with non-existent vertices
    let traverser = Traverser::new(Value::Map(HashMap::from([
        ("__pending_add_e".to_string(), Value::Bool(true)),
        ("label".to_string(), Value::String("knows".to_string())),
        ("from".to_string(), Value::Vertex(VertexId(999))), // Non-existent
        ("to".to_string(), Value::Vertex(VertexId(998))),   // Non-existent
        ("properties".to_string(), Value::Map(HashMap::new())),
    ])));

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute(std::iter::once(traverser));

    // Edge should not be added due to missing vertices
    assert_eq!(result.edges_added, 0);
}

#[test]
fn mutation_executor_execute_set_vertex_property_failure() {
    let mut storage = InMemoryGraph::new();

    // Try to set property on non-existent vertex
    let traverser = Traverser::new(Value::Map(HashMap::from([
        ("__pending_property_vertex".to_string(), Value::Bool(true)),
        ("id".to_string(), Value::Vertex(VertexId(999))),
        ("key".to_string(), Value::String("prop".to_string())),
        ("value".to_string(), Value::Int(1)),
    ])));

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute(std::iter::once(traverser));

    assert_eq!(result.properties_set, 0);
}

#[test]
fn mutation_executor_execute_set_edge_property() {
    let mut storage = create_test_graph();

    // Get an edge ID
    let edge_id = storage.all_edges().next().unwrap().id;

    let traverser = Traverser::new(Value::Map(HashMap::from([
        ("__pending_property_edge".to_string(), Value::Bool(true)),
        ("id".to_string(), Value::Edge(edge_id)),
        ("key".to_string(), Value::String("strength".to_string())),
        ("value".to_string(), Value::Int(10)),
    ])));

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute(std::iter::once(traverser));

    assert_eq!(result.properties_set, 1);
    assert_eq!(result.values.len(), 1);

    // Verify property was set
    let edge = storage.get_edge(edge_id).unwrap();
    assert_eq!(edge.properties.get("strength"), Some(&Value::Int(10)));
}

#[test]
fn mutation_executor_execute_set_edge_property_failure() {
    let mut storage = InMemoryGraph::new();

    // Try to set property on non-existent edge
    let traverser = Traverser::new(Value::Map(HashMap::from([
        ("__pending_property_edge".to_string(), Value::Bool(true)),
        ("id".to_string(), Value::Edge(EdgeId(999))),
        ("key".to_string(), Value::String("prop".to_string())),
        ("value".to_string(), Value::Int(1)),
    ])));

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute(std::iter::once(traverser));

    assert_eq!(result.properties_set, 0);
}

#[test]
fn mutation_executor_execute_drop_vertex() {
    let mut storage = InMemoryGraph::new();

    // Create a vertex without edges
    let id = storage.add_vertex("item", HashMap::new());

    let traverser = Traverser::new(Value::Map(HashMap::from([
        ("__pending_drop_vertex".to_string(), Value::Bool(true)),
        ("id".to_string(), Value::Vertex(id)),
    ])));

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute(std::iter::once(traverser));

    assert_eq!(result.vertices_removed, 1);
    assert!(storage.get_vertex(id).is_none());
}

#[test]
fn mutation_executor_execute_drop_vertex_failure() {
    let mut storage = InMemoryGraph::new();

    // Try to drop non-existent vertex
    let traverser = Traverser::new(Value::Map(HashMap::from([
        ("__pending_drop_vertex".to_string(), Value::Bool(true)),
        ("id".to_string(), Value::Vertex(VertexId(999))),
    ])));

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute(std::iter::once(traverser));

    assert_eq!(result.vertices_removed, 0);
}

#[test]
fn mutation_executor_execute_drop_edge() {
    let mut storage = create_test_graph();
    let initial_count = storage.edge_count();

    // Get an edge ID
    let edge_id = storage.all_edges().next().unwrap().id;

    let traverser = Traverser::new(Value::Map(HashMap::from([
        ("__pending_drop_edge".to_string(), Value::Bool(true)),
        ("id".to_string(), Value::Edge(edge_id)),
    ])));

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute(std::iter::once(traverser));

    assert_eq!(result.edges_removed, 1);
    assert_eq!(storage.edge_count(), initial_count - 1);
}

#[test]
fn mutation_executor_execute_drop_edge_failure() {
    let mut storage = InMemoryGraph::new();

    // Try to drop non-existent edge
    let traverser = Traverser::new(Value::Map(HashMap::from([
        ("__pending_drop_edge".to_string(), Value::Bool(true)),
        ("id".to_string(), Value::Edge(EdgeId(999))),
    ])));

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute(std::iter::once(traverser));

    assert_eq!(result.edges_removed, 0);
}

#[test]
fn mutation_executor_execute_non_mutation_value() {
    let mut storage = InMemoryGraph::new();

    // Pass through a regular value
    let traverser = Traverser::new(Value::String("hello".to_string()));

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute(std::iter::once(traverser));

    // Regular values pass through
    assert_eq!(result.values.len(), 1);
    assert_eq!(result.values[0], Value::String("hello".to_string()));
}

#[test]
fn mutation_executor_execute_mutation_add_vertex() {
    let mut storage = InMemoryGraph::new();

    let mutation = PendingMutation::AddVertex {
        label: "test".to_string(),
        properties: HashMap::from([("key".to_string(), Value::Int(42))]),
    };

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute_mutation(mutation);

    assert!(result.is_some());
    assert_eq!(storage.vertex_count(), 1);
}

#[test]
fn mutation_executor_execute_mutation_add_edge_failure() {
    let mut storage = InMemoryGraph::new();

    let mutation = PendingMutation::AddEdge {
        label: "test".to_string(),
        from: VertexId(999),
        to: VertexId(998),
        properties: HashMap::new(),
    };

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute_mutation(mutation);

    assert!(result.is_none());
}

#[test]
fn mutation_executor_execute_mutation_set_vertex_property_failure() {
    let mut storage = InMemoryGraph::new();

    let mutation = PendingMutation::SetVertexProperty {
        id: VertexId(999),
        key: "prop".to_string(),
        value: Value::Int(1),
    };

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute_mutation(mutation);

    assert!(result.is_none());
}

#[test]
fn mutation_executor_execute_mutation_set_edge_property() {
    let mut storage = create_test_graph();
    let edge_id = storage.all_edges().next().unwrap().id;

    let mutation = PendingMutation::SetEdgeProperty {
        id: edge_id,
        key: "weight".to_string(),
        value: Value::Float(0.5),
    };

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute_mutation(mutation);

    assert!(result.is_some());
}

#[test]
fn mutation_executor_execute_mutation_set_edge_property_failure() {
    let mut storage = InMemoryGraph::new();

    let mutation = PendingMutation::SetEdgeProperty {
        id: EdgeId(999),
        key: "prop".to_string(),
        value: Value::Int(1),
    };

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute_mutation(mutation);

    assert!(result.is_none());
}

#[test]
fn mutation_executor_execute_mutation_drop_vertex() {
    let mut storage = InMemoryGraph::new();
    let id = storage.add_vertex("item", HashMap::new());

    let mutation = PendingMutation::DropVertex { id };

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute_mutation(mutation);

    // Drop returns None
    assert!(result.is_none());
    assert!(storage.get_vertex(id).is_none());
}

#[test]
fn mutation_executor_execute_mutation_drop_vertex_failure() {
    let mut storage = InMemoryGraph::new();

    let mutation = PendingMutation::DropVertex { id: VertexId(999) };

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute_mutation(mutation);

    assert!(result.is_none());
}

#[test]
fn mutation_executor_execute_mutation_drop_edge() {
    let mut storage = create_test_graph();
    let edge_id = storage.all_edges().next().unwrap().id;

    let mutation = PendingMutation::DropEdge { id: edge_id };

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute_mutation(mutation);

    // Drop returns None
    assert!(result.is_none());
    assert!(storage.get_edge(edge_id).is_none());
}

#[test]
fn mutation_executor_execute_mutation_drop_edge_failure() {
    let mut storage = InMemoryGraph::new();

    let mutation = PendingMutation::DropEdge { id: EdgeId(999) };

    let mut executor = MutationExecutor::new(&mut storage);
    let result = executor.execute_mutation(mutation);

    assert!(result.is_none());
}

// =============================================================================
// API Integration Tests
// =============================================================================

#[test]
fn add_v_via_api() {
    let graph = Graph::in_memory();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    let results: Vec<Value> = g.add_v("test_label").property("name", "Test").to_list();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert!(map.contains_key("__pending_add_v"));
    }
}

#[test]
fn add_e_via_api() {
    let graph = Graph::in_memory();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    let results: Vec<Value> = g
        .add_e("knows")
        .from_vertex(VertexId(1))
        .to_vertex(VertexId(2))
        .to_list();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert!(map.contains_key("__pending_add_e"));
    }
}

#[test]
fn drop_vertex_via_api() {
    let mut storage = InMemoryGraph::new();
    let id = storage.add_vertex("item", HashMap::new());

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    let results: Vec<Value> = g.v_ids([id]).drop().to_list();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert!(map.contains_key("__pending_drop_vertex"));
    }
}
