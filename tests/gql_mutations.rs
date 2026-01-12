//! Integration tests for GQL mutation statements.
//!
//! Tests for CREATE, SET, REMOVE, DELETE, DETACH DELETE, and MERGE clauses.

use std::collections::HashMap;

use intersteller::gql::{execute_mutation, parse_statement, MutationError};
use intersteller::storage::{GraphStorage, InMemoryGraph};
use intersteller::value::Value;

// =============================================================================
// Helper Functions
// =============================================================================

/// Creates a test graph with some initial data.
fn create_test_graph() -> InMemoryGraph {
    let mut storage = InMemoryGraph::new();

    let alice_id = storage.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("age".to_string(), Value::Int(30)),
        ]),
    );

    let bob_id = storage.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Bob".to_string())),
            ("age".to_string(), Value::Int(25)),
        ]),
    );

    let _software_id = storage.add_vertex(
        "Software",
        HashMap::from([("name".to_string(), Value::String("Gremlin".to_string()))]),
    );

    storage
        .add_edge(
            alice_id,
            bob_id,
            "KNOWS",
            HashMap::from([("since".to_string(), Value::Int(2020))]),
        )
        .unwrap();

    storage
}

/// Execute a GQL mutation query against storage.
fn execute_gql(storage: &mut InMemoryGraph, query: &str) -> Result<Vec<Value>, MutationError> {
    let stmt = parse_statement(query).map_err(|e| {
        MutationError::Compile(intersteller::gql::CompileError::UnsupportedFeature(format!(
            "Parse error: {}",
            e
        )))
    })?;
    execute_mutation(&stmt, storage)
}

// =============================================================================
// CREATE Tests
// =============================================================================

#[test]
fn test_create_single_vertex() {
    let mut storage = InMemoryGraph::new();

    execute_gql(&mut storage, "CREATE (n:Person {name: 'Charlie', age: 35})").unwrap();

    assert_eq!(storage.vertex_count(), 1);

    let vertex = storage.all_vertices().next().unwrap();
    assert_eq!(vertex.label, "Person");
    assert_eq!(
        vertex.properties.get("name"),
        Some(&Value::String("Charlie".to_string()))
    );
    assert_eq!(vertex.properties.get("age"), Some(&Value::Int(35)));
}

#[test]
fn test_create_multiple_vertices() {
    let mut storage = InMemoryGraph::new();

    execute_gql(
        &mut storage,
        "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})",
    )
    .unwrap();

    assert_eq!(storage.vertex_count(), 2);
}

#[test]
fn test_create_vertex_and_edge() {
    let mut storage = InMemoryGraph::new();

    execute_gql(
        &mut storage,
        "CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})",
    )
    .unwrap();

    assert_eq!(storage.vertex_count(), 2);
    assert_eq!(storage.edge_count(), 1);

    let edge = storage.all_edges().next().unwrap();
    assert_eq!(edge.label, "KNOWS");
    assert_eq!(edge.properties.get("since"), Some(&Value::Int(2020)));
}

#[test]
fn test_create_with_return() {
    let mut storage = InMemoryGraph::new();

    let results = execute_gql(&mut storage, "CREATE (n:Person {name: 'Alice'}) RETURN n").unwrap();

    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Value::Vertex(_)));
}

#[test]
fn test_create_with_return_property() {
    let mut storage = InMemoryGraph::new();

    let results = execute_gql(
        &mut storage,
        "CREATE (n:Person {name: 'Alice'}) RETURN n.name",
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice".to_string()));
}

// =============================================================================
// MATCH + CREATE Tests
// =============================================================================

#[test]
fn test_match_create_edge() {
    let mut storage = create_test_graph();
    let initial_edge_count = storage.edge_count();

    // First create a new edge between existing vertices by first matching them
    // Note: Our current implementation requires the pattern to include vertex labels for matching
    execute_gql(
        &mut storage,
        r#"
        MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
        CREATE (a)-[:WORKS_WITH {project: 'Gremlin'}]->(b)
        "#,
    )
    .unwrap();

    assert_eq!(storage.edge_count(), initial_edge_count + 1);
}

// =============================================================================
// SET Tests
// =============================================================================

#[test]
fn test_match_set_property() {
    let mut storage = create_test_graph();

    execute_gql(
        &mut storage,
        "MATCH (n:Person {name: 'Alice'}) SET n.age = 31",
    )
    .unwrap();

    let alice = storage
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
        .expect("Alice should exist");
    assert_eq!(alice.properties.get("age"), Some(&Value::Int(31)));
}

#[test]
fn test_match_set_multiple_properties() {
    let mut storage = create_test_graph();

    execute_gql(
        &mut storage,
        "MATCH (n:Person {name: 'Alice'}) SET n.age = 31, n.status = 'active'",
    )
    .unwrap();

    let alice = storage
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
        .expect("Alice should exist");
    assert_eq!(alice.properties.get("age"), Some(&Value::Int(31)));
    assert_eq!(
        alice.properties.get("status"),
        Some(&Value::String("active".to_string()))
    );
}

#[test]
fn test_match_set_with_return() {
    let mut storage = create_test_graph();

    let results = execute_gql(
        &mut storage,
        "MATCH (n:Person {name: 'Alice'}) SET n.age = 31 RETURN n.age",
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(31));
}

// =============================================================================
// REMOVE Tests
// =============================================================================

#[test]
fn test_match_remove_property() {
    let mut storage = create_test_graph();

    execute_gql(
        &mut storage,
        "MATCH (n:Person {name: 'Alice'}) REMOVE n.age",
    )
    .unwrap();

    let alice = storage
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
        .expect("Alice should exist");
    // Property should be set to Null (our REMOVE implementation)
    assert_eq!(alice.properties.get("age"), Some(&Value::Null));
}

// =============================================================================
// DELETE Tests
// =============================================================================

#[test]
fn test_delete_edge() {
    let mut storage = create_test_graph();
    assert_eq!(storage.edge_count(), 1);

    // Match the edge with explicit endpoint patterns
    execute_gql(
        &mut storage,
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) DELETE r",
    )
    .unwrap();

    assert_eq!(storage.edge_count(), 0);
}

#[test]
fn test_delete_vertex_without_edges() {
    let mut storage = InMemoryGraph::new();
    storage.add_vertex(
        "Person",
        HashMap::from([("name".to_string(), Value::String("Solo".to_string()))]),
    );

    assert_eq!(storage.vertex_count(), 1);

    execute_gql(&mut storage, "MATCH (n:Person {name: 'Solo'}) DELETE n").unwrap();

    assert_eq!(storage.vertex_count(), 0);
}

#[test]
fn test_delete_vertex_with_edges_fails() {
    let mut storage = create_test_graph();

    let result = execute_gql(&mut storage, "MATCH (n:Person {name: 'Alice'}) DELETE n");

    assert!(matches!(result, Err(MutationError::VertexHasEdges(_))));
    // Vertex should still exist
    assert_eq!(storage.vertex_count(), 3);
}

// =============================================================================
// DETACH DELETE Tests
// =============================================================================

#[test]
fn test_detach_delete_vertex() {
    let mut storage = create_test_graph();
    assert_eq!(storage.vertex_count(), 3);
    assert_eq!(storage.edge_count(), 1);

    execute_gql(
        &mut storage,
        "MATCH (n:Person {name: 'Alice'}) DETACH DELETE n",
    )
    .unwrap();

    // Alice is gone, but Bob and Gremlin remain
    assert_eq!(storage.vertex_count(), 2);
    // Edge is also gone
    assert_eq!(storage.edge_count(), 0);
}

// =============================================================================
// MERGE Tests
// =============================================================================

#[test]
fn test_merge_creates_when_not_exists() {
    let mut storage = InMemoryGraph::new();

    execute_gql(
        &mut storage,
        "MERGE (n:Person {name: 'New'}) ON CREATE SET n.created = true",
    )
    .unwrap();

    assert_eq!(storage.vertex_count(), 1);

    let vertex = storage.all_vertices().next().unwrap();
    assert_eq!(
        vertex.properties.get("name"),
        Some(&Value::String("New".to_string()))
    );
    assert_eq!(vertex.properties.get("created"), Some(&Value::Bool(true)));
}

#[test]
fn test_merge_matches_when_exists() {
    let mut storage = create_test_graph();
    let initial_count = storage.vertex_count();

    execute_gql(
        &mut storage,
        "MERGE (n:Person {name: 'Alice'}) ON MATCH SET n.updated = true",
    )
    .unwrap();

    // No new vertex created
    assert_eq!(storage.vertex_count(), initial_count);

    let alice = storage
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
        .expect("Alice should exist");
    assert_eq!(alice.properties.get("updated"), Some(&Value::Bool(true)));
}

#[test]
fn test_merge_with_both_actions() {
    let mut storage = InMemoryGraph::new();

    // First MERGE creates
    execute_gql(
        &mut storage,
        "MERGE (n:Person {name: 'Test'}) ON CREATE SET n.status = 'new' ON MATCH SET n.status = 'existing'",
    )
    .unwrap();

    let vertex = storage.all_vertices().next().unwrap();
    assert_eq!(
        vertex.properties.get("status"),
        Some(&Value::String("new".to_string()))
    );

    // Second MERGE matches
    execute_gql(
        &mut storage,
        "MERGE (n:Person {name: 'Test'}) ON CREATE SET n.status = 'new' ON MATCH SET n.status = 'existing'",
    )
    .unwrap();

    // Still just one vertex
    assert_eq!(storage.vertex_count(), 1);

    let vertex = storage.all_vertices().next().unwrap();
    assert_eq!(
        vertex.properties.get("status"),
        Some(&Value::String("existing".to_string()))
    );
}

// =============================================================================
// WHERE Clause Tests
// =============================================================================

#[test]
fn test_match_where_set() {
    let mut storage = create_test_graph();

    // Only update vertices where age > 26
    execute_gql(
        &mut storage,
        "MATCH (n:Person) WHERE n.age > 26 SET n.adult = true",
    )
    .unwrap();

    // Only Alice (age 30) should be updated
    let alice = storage
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
        .expect("Alice should exist");
    assert_eq!(alice.properties.get("adult"), Some(&Value::Bool(true)));

    // Bob (age 25) should not be updated
    let bob = storage
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Bob".to_string())))
        .expect("Bob should exist");
    assert_eq!(bob.properties.get("adult"), None);
}

#[test]
fn test_match_where_no_matches() {
    let mut storage = create_test_graph();

    // No matches - no updates
    let results = execute_gql(
        &mut storage,
        "MATCH (n:Person) WHERE n.age > 100 SET n.centenarian = true RETURN n",
    )
    .unwrap();

    assert!(results.is_empty());
}

// =============================================================================
// Complex Query Tests
// =============================================================================

#[test]
fn test_create_multiple_edges_chain() {
    let mut storage = InMemoryGraph::new();

    execute_gql(
        &mut storage,
        "CREATE (a:Person {name: 'A'})-[:FOLLOWS]->(b:Person {name: 'B'})-[:FOLLOWS]->(c:Person {name: 'C'})",
    )
    .unwrap();

    assert_eq!(storage.vertex_count(), 3);
    assert_eq!(storage.edge_count(), 2);
}

#[test]
fn test_set_expression_value() {
    let mut storage = create_test_graph();

    // Set a computed value
    execute_gql(
        &mut storage,
        "MATCH (n:Person {name: 'Alice'}) SET n.next_age = n.age + 1",
    )
    .unwrap();

    let alice = storage
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
        .expect("Alice should exist");
    assert_eq!(alice.properties.get("next_age"), Some(&Value::Int(31)));
}

// =============================================================================
// Error Case Tests
// =============================================================================

#[test]
fn test_set_unbound_variable_fails() {
    let mut storage = create_test_graph();

    let result = execute_gql(&mut storage, "MATCH (n:Person) SET m.age = 50");

    assert!(matches!(result, Err(MutationError::UnboundVariable(_))));
}

#[test]
fn test_delete_unbound_variable_fails() {
    let mut storage = create_test_graph();

    let result = execute_gql(&mut storage, "MATCH (n:Person) DELETE m");

    assert!(matches!(result, Err(MutationError::UnboundVariable(_))));
}
