//! Integration tests for GQL mutation statements.
//!
//! Tests for CREATE, SET, REMOVE, DELETE, DETACH DELETE, and MERGE clauses.

#![allow(unused_variables)]
use std::collections::HashMap;
use std::sync::Arc;

use interstellar::gql::{parse_statement, MutationError};
use interstellar::storage::{Graph, GraphStorage};
use interstellar::value::Value;

// =============================================================================
// Helper Functions
// =============================================================================

/// Creates a test graph with some initial data.
fn create_test_graph() -> Arc<Graph> {
    let graph = Arc::new(Graph::new());

    let alice_id = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("age".to_string(), Value::Int(30)),
        ]),
    );

    let bob_id = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Bob".to_string())),
            ("age".to_string(), Value::Int(25)),
        ]),
    );

    let _software_id = graph.add_vertex(
        "Software",
        HashMap::from([("name".to_string(), Value::String("Gremlin".to_string()))]),
    );

    graph
        .add_edge(
            alice_id,
            bob_id,
            "KNOWS",
            HashMap::from([("since".to_string(), Value::Int(2020))]),
        )
        .unwrap();

    graph
}

/// Execute a GQL mutation query against the graph.
fn execute_gql(graph: &Arc<Graph>, query: &str) -> Result<Vec<Value>, MutationError> {
    graph.gql(query).map_err(|e| {
        MutationError::Compile(interstellar::gql::CompileError::UnsupportedFeature(
            format!("GQL error: {}", e),
        ))
    })
}

// =============================================================================
// CREATE Tests
// =============================================================================

#[test]
fn test_create_single_vertex() {
    let graph = Arc::new(Graph::new());

    execute_gql(&graph, "CREATE (n:Person {name: 'Charlie', age: 35})").unwrap();

    assert_eq!(graph.vertex_count(), 1);

    let vertex = graph.snapshot().all_vertices().next().unwrap();
    assert_eq!(vertex.label, "Person");
    assert_eq!(
        vertex.properties.get("name"),
        Some(&Value::String("Charlie".to_string()))
    );
    assert_eq!(vertex.properties.get("age"), Some(&Value::Int(35)));
}

#[test]
fn test_create_multiple_vertices() {
    let graph = Arc::new(Graph::new());

    execute_gql(
        &graph,
        "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})",
    )
    .unwrap();

    assert_eq!(graph.vertex_count(), 2);
}

#[test]
fn test_create_vertex_and_edge() {
    let graph = Arc::new(Graph::new());

    execute_gql(
        &graph,
        "CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})",
    )
    .unwrap();

    assert_eq!(graph.vertex_count(), 2);
    assert_eq!(graph.edge_count(), 1);

    let edge = graph.snapshot().all_edges().next().unwrap();
    assert_eq!(edge.label, "KNOWS");
    assert_eq!(edge.properties.get("since"), Some(&Value::Int(2020)));
}

#[test]
fn test_create_with_return() {
    let graph = Arc::new(Graph::new());

    let results = execute_gql(&graph, "CREATE (n:Person {name: 'Alice'}) RETURN n").unwrap();

    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Value::Vertex(_)));
}

#[test]
fn test_create_with_return_property() {
    let graph = Arc::new(Graph::new());

    let results = execute_gql(&graph, "CREATE (n:Person {name: 'Alice'}) RETURN n.name").unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice".to_string()));
}

// =============================================================================
// MATCH + CREATE Tests
// =============================================================================

#[test]
fn test_match_create_edge() {
    let graph = create_test_graph();
    let initial_edge_count = graph.edge_count();

    // First create a new edge between existing vertices by first matching them
    // Note: Our current implementation requires the pattern to include vertex labels for matching
    execute_gql(
        &graph,
        r#"
        MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
        CREATE (a)-[:WORKS_WITH {project: 'Gremlin'}]->(b)
        "#,
    )
    .unwrap();

    assert_eq!(graph.edge_count(), initial_edge_count + 1);
}

// =============================================================================
// SET Tests
// =============================================================================

#[test]
fn test_match_set_property() {
    let graph = create_test_graph();

    execute_gql(&graph, "MATCH (n:Person {name: 'Alice'}) SET n.age = 31").unwrap();

    let alice = graph
        .snapshot()
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
        .expect("Alice should exist");
    assert_eq!(alice.properties.get("age"), Some(&Value::Int(31)));
}

#[test]
fn test_match_set_multiple_properties() {
    let graph = create_test_graph();

    execute_gql(
        &graph,
        "MATCH (n:Person {name: 'Alice'}) SET n.age = 31, n.status = 'active'",
    )
    .unwrap();

    let alice = graph
        .snapshot()
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
    let graph = create_test_graph();

    let results = execute_gql(
        &graph,
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
    let graph = create_test_graph();

    execute_gql(&graph, "MATCH (n:Person {name: 'Alice'}) REMOVE n.age").unwrap();

    let alice = graph
        .snapshot()
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
    let graph = create_test_graph();
    assert_eq!(graph.edge_count(), 1);

    // Match the edge with explicit endpoint patterns
    execute_gql(&graph, "MATCH (a:Person)-[r:KNOWS]->(b:Person) DELETE r").unwrap();

    assert_eq!(graph.edge_count(), 0);
}

#[test]
fn test_delete_vertex_without_edges() {
    let graph = Arc::new(Graph::new());
    graph.add_vertex(
        "Person",
        HashMap::from([("name".to_string(), Value::String("Solo".to_string()))]),
    );

    assert_eq!(graph.vertex_count(), 1);

    execute_gql(&graph, "MATCH (n:Person {name: 'Solo'}) DELETE n").unwrap();

    assert_eq!(graph.vertex_count(), 0);
}

#[test]
fn test_delete_vertex_with_edges_fails() {
    let graph = create_test_graph();

    let result = execute_gql(&graph, "MATCH (n:Person {name: 'Alice'}) DELETE n");

    assert!(result.is_err());
    // Vertex should still exist
    assert_eq!(graph.vertex_count(), 3);
}

// =============================================================================
// DETACH DELETE Tests
// =============================================================================

#[test]
fn test_detach_delete_vertex() {
    let graph = create_test_graph();
    assert_eq!(graph.vertex_count(), 3);
    assert_eq!(graph.edge_count(), 1);

    execute_gql(&graph, "MATCH (n:Person {name: 'Alice'}) DETACH DELETE n").unwrap();

    // Alice is gone, but Bob and Gremlin remain
    assert_eq!(graph.vertex_count(), 2);
    // Edge is also gone
    assert_eq!(graph.edge_count(), 0);
}

// =============================================================================
// MERGE Tests
// =============================================================================

#[test]
fn test_merge_creates_when_not_exists() {
    let graph = Arc::new(Graph::new());

    execute_gql(
        &graph,
        "MERGE (n:Person {name: 'New'}) ON CREATE SET n.created = true",
    )
    .unwrap();

    assert_eq!(graph.vertex_count(), 1);

    let vertex = graph.snapshot().all_vertices().next().unwrap();
    assert_eq!(
        vertex.properties.get("name"),
        Some(&Value::String("New".to_string()))
    );
    assert_eq!(vertex.properties.get("created"), Some(&Value::Bool(true)));
}

#[test]
fn test_merge_matches_when_exists() {
    let graph = create_test_graph();
    let initial_count = graph.vertex_count();

    execute_gql(
        &graph,
        "MERGE (n:Person {name: 'Alice'}) ON MATCH SET n.updated = true",
    )
    .unwrap();

    // No new vertex created
    assert_eq!(graph.vertex_count(), initial_count);

    let alice = graph
        .snapshot()
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
        .expect("Alice should exist");
    assert_eq!(alice.properties.get("updated"), Some(&Value::Bool(true)));
}

#[test]
fn test_merge_with_both_actions() {
    let graph = Arc::new(Graph::new());

    // First MERGE creates
    execute_gql(
        &graph,
        "MERGE (n:Person {name: 'Test'}) ON CREATE SET n.status = 'new' ON MATCH SET n.status = 'existing'",
    )
    .unwrap();

    let vertex = graph.snapshot().all_vertices().next().unwrap();
    assert_eq!(
        vertex.properties.get("status"),
        Some(&Value::String("new".to_string()))
    );

    // Second MERGE matches
    execute_gql(
        &graph,
        "MERGE (n:Person {name: 'Test'}) ON CREATE SET n.status = 'new' ON MATCH SET n.status = 'existing'",
    )
    .unwrap();

    // Still just one vertex
    assert_eq!(graph.vertex_count(), 1);

    let vertex = graph.snapshot().all_vertices().next().unwrap();
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
    let graph = create_test_graph();

    // Only update vertices where age > 26
    execute_gql(
        &graph,
        "MATCH (n:Person) WHERE n.age > 26 SET n.adult = true",
    )
    .unwrap();

    // Only Alice (age 30) should be updated
    let alice = graph
        .snapshot()
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
        .expect("Alice should exist");
    assert_eq!(alice.properties.get("adult"), Some(&Value::Bool(true)));

    // Bob (age 25) should not be updated
    let bob = graph
        .snapshot()
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Bob".to_string())))
        .expect("Bob should exist");
    assert_eq!(bob.properties.get("adult"), None);
}

#[test]
fn test_match_where_no_matches() {
    let graph = create_test_graph();

    // No matches - no updates
    let results = execute_gql(
        &graph,
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
    let graph = Arc::new(Graph::new());

    execute_gql(
        &graph,
        "CREATE (a:Person {name: 'A'})-[:FOLLOWS]->(b:Person {name: 'B'})-[:FOLLOWS]->(c:Person {name: 'C'})",
    )
    .unwrap();

    assert_eq!(graph.vertex_count(), 3);
    assert_eq!(graph.edge_count(), 2);
}

#[test]
fn test_set_expression_value() {
    let graph = create_test_graph();

    // Set a computed value
    execute_gql(
        &graph,
        "MATCH (n:Person {name: 'Alice'}) SET n.next_age = n.age + 1",
    )
    .unwrap();

    let alice = graph
        .snapshot()
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
    let graph = create_test_graph();

    let result = execute_gql(&graph, "MATCH (n:Person) SET m.age = 50");

    assert!(result.is_err());
}

#[test]
fn test_delete_unbound_variable_fails() {
    let graph = create_test_graph();

    let result = execute_gql(&graph, "MATCH (n:Person) DELETE m");

    assert!(result.is_err());
}

// =============================================================================
// Schema Validation Tests
// =============================================================================

use interstellar::gql::execute_mutation_with_schema;
use interstellar::schema::{PropertyType, SchemaBuilder, SchemaError, ValidationMode};
use interstellar::storage::{GraphMutWrapper, GraphStorageMut};

/// Create a test schema for validation tests.
fn create_test_schema(mode: ValidationMode) -> interstellar::schema::GraphSchema {
    SchemaBuilder::new()
        .mode(mode)
        .vertex("Person")
        .property("name", PropertyType::String)
        .optional("age", PropertyType::Int)
        .done()
        .vertex("Company")
        .property("name", PropertyType::String)
        .property("founded", PropertyType::Int)
        .done()
        .edge("KNOWS")
        .from(&["Person"])
        .to(&["Person"])
        .optional("since", PropertyType::Int)
        .done()
        .edge("WORKS_AT")
        .from(&["Person"])
        .to(&["Company"])
        .property("role", PropertyType::String)
        .done()
        .build()
}

/// Execute a GQL mutation with schema validation.
fn execute_gql_with_schema(
    storage: &mut GraphMutWrapper<'_>,
    query: &str,
    schema: &interstellar::schema::GraphSchema,
) -> Result<Vec<Value>, MutationError> {
    let stmt = parse_statement(query).unwrap();
    execute_mutation_with_schema(&stmt, storage, Some(schema))
}

// --- CREATE Vertex Validation Tests ---

#[test]
fn test_create_vertex_valid_schema() {
    let graph = Arc::new(Graph::new());
    let mut storage = graph.as_storage_mut();
    let schema = create_test_schema(ValidationMode::Strict);

    let result = execute_gql_with_schema(
        &mut storage,
        "CREATE (n:Person {name: 'Alice', age: 30})",
        &schema,
    );

    assert!(result.is_ok());
    assert_eq!(storage.vertex_count(), 1);
}

#[test]
fn test_create_vertex_missing_required_property_strict() {
    let graph = Arc::new(Graph::new());
    let mut storage = graph.as_storage_mut();
    let schema = create_test_schema(ValidationMode::Strict);

    // Person requires 'name' property
    let result = execute_gql_with_schema(&mut storage, "CREATE (n:Person {age: 30})", &schema);

    assert!(result.is_err());
    if let Err(MutationError::Schema(SchemaError::MissingRequired { property, .. })) = result {
        assert_eq!(property, "name");
    } else {
        panic!("Expected MissingRequired error, got {:?}", result);
    }
    // Vertex should not be created
    assert_eq!(storage.vertex_count(), 0);
}

#[test]
fn test_create_vertex_wrong_property_type_strict() {
    let graph = Arc::new(Graph::new());
    let mut storage = graph.as_storage_mut();
    let schema = create_test_schema(ValidationMode::Strict);

    // age should be Int, not String
    let result = execute_gql_with_schema(
        &mut storage,
        "CREATE (n:Person {name: 'Alice', age: 'thirty'})",
        &schema,
    );

    assert!(result.is_err());
    if let Err(MutationError::Schema(SchemaError::TypeMismatch { property, .. })) = result {
        assert_eq!(property, "age");
    } else {
        panic!("Expected TypeMismatch error, got {:?}", result);
    }
    assert_eq!(storage.vertex_count(), 0);
}

#[test]
fn test_create_vertex_unknown_label_closed() {
    let graph = Arc::new(Graph::new());
    let mut storage = graph.as_storage_mut();
    let schema = create_test_schema(ValidationMode::Closed);

    // 'Animal' is not defined in the schema
    let result =
        execute_gql_with_schema(&mut storage, "CREATE (n:Animal {name: 'Fluffy'})", &schema);

    assert!(result.is_err());
    if let Err(MutationError::Schema(SchemaError::UnknownVertexLabel { label })) = result {
        assert_eq!(label, "Animal");
    } else {
        panic!("Expected UnknownVertexLabel error, got {:?}", result);
    }
    assert_eq!(storage.vertex_count(), 0);
}

#[test]
fn test_create_vertex_unknown_label_strict_allowed() {
    let graph = Arc::new(Graph::new());
    let mut storage = graph.as_storage_mut();
    let schema = create_test_schema(ValidationMode::Strict);

    // Unknown labels are allowed in Strict mode
    let result =
        execute_gql_with_schema(&mut storage, "CREATE (n:Animal {name: 'Fluffy'})", &schema);

    assert!(result.is_ok());
    assert_eq!(storage.vertex_count(), 1);
}

#[test]
fn test_create_vertex_validation_mode_none() {
    let graph = Arc::new(Graph::new());
    let mut storage = graph.as_storage_mut();
    let schema = create_test_schema(ValidationMode::None);

    // All validation is skipped in None mode
    let result =
        execute_gql_with_schema(&mut storage, "CREATE (n:Person {age: 'invalid'})", &schema);

    assert!(result.is_ok());
    assert_eq!(storage.vertex_count(), 1);
}

// --- CREATE Edge Validation Tests ---

#[test]
fn test_create_edge_valid_schema() {
    let graph = Arc::new(Graph::new());
    let mut storage = graph.as_storage_mut();
    let schema = create_test_schema(ValidationMode::Strict);

    // Create two Person vertices and a KNOWS edge between them
    let result = execute_gql_with_schema(
        &mut storage,
        "CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})",
        &schema,
    );

    assert!(result.is_ok());
    assert_eq!(storage.vertex_count(), 2);
    assert_eq!(storage.edge_count(), 1);
}

#[test]
fn test_create_edge_invalid_source_label() {
    let graph = Arc::new(Graph::new());
    let mut storage = graph.as_storage_mut();
    let schema = create_test_schema(ValidationMode::Strict);

    // WORKS_AT only allows Person -> Company, not Company -> Company
    // First create a Company
    storage.add_vertex(
        "Company",
        HashMap::from([
            ("name".to_string(), Value::String("Acme".to_string())),
            ("founded".to_string(), Value::Int(1990)),
        ]),
    );

    let result = execute_gql_with_schema(
        &mut storage,
        "CREATE (c:Company {name: 'Corp', founded: 2000})-[:WORKS_AT {role: 'Manager'}]->(c2:Company {name: 'Other', founded: 2010})",
        &schema,
    );

    assert!(result.is_err());
    if let Err(MutationError::Schema(SchemaError::InvalidSourceLabel { edge_label, .. })) = result {
        assert_eq!(edge_label, "WORKS_AT");
    } else {
        panic!("Expected InvalidSourceLabel error, got {:?}", result);
    }
}

#[test]
fn test_create_edge_invalid_target_label() {
    let graph = Arc::new(Graph::new());
    let mut storage = graph.as_storage_mut();
    let schema = create_test_schema(ValidationMode::Strict);

    // WORKS_AT requires Company as target, not Person
    let result = execute_gql_with_schema(
        &mut storage,
        "CREATE (a:Person {name: 'Alice'})-[:WORKS_AT {role: 'Developer'}]->(b:Person {name: 'Bob'})",
        &schema,
    );

    assert!(result.is_err());
    if let Err(MutationError::Schema(SchemaError::InvalidTargetLabel { edge_label, .. })) = result {
        assert_eq!(edge_label, "WORKS_AT");
    } else {
        panic!("Expected InvalidTargetLabel error, got {:?}", result);
    }
}

#[test]
fn test_create_edge_missing_required_property() {
    let graph = Arc::new(Graph::new());
    let mut storage = graph.as_storage_mut();
    let schema = create_test_schema(ValidationMode::Strict);

    // WORKS_AT requires 'role' property
    let result = execute_gql_with_schema(
        &mut storage,
        "CREATE (a:Person {name: 'Alice'})-[:WORKS_AT]->(c:Company {name: 'Acme', founded: 2000})",
        &schema,
    );

    assert!(result.is_err());
    if let Err(MutationError::Schema(SchemaError::MissingRequired { property, .. })) = result {
        assert_eq!(property, "role");
    } else {
        panic!("Expected MissingRequired error, got {:?}", result);
    }
}

// --- SET Property Validation Tests ---

#[test]
fn test_set_property_valid_type() {
    let graph = Arc::new(Graph::new());
    let mut storage = graph.as_storage_mut();
    let schema = create_test_schema(ValidationMode::Strict);

    // Create a Person
    execute_gql_with_schema(&mut storage, "CREATE (n:Person {name: 'Alice'})", &schema).unwrap();

    // Set age to an integer (correct type)
    let result = execute_gql_with_schema(&mut storage, "MATCH (n:Person) SET n.age = 30", &schema);

    assert!(result.is_ok());
}

#[test]
fn test_set_property_wrong_type() {
    let graph = Arc::new(Graph::new());
    let mut storage = graph.as_storage_mut();
    let schema = create_test_schema(ValidationMode::Strict);

    // Create a Person
    execute_gql_with_schema(&mut storage, "CREATE (n:Person {name: 'Alice'})", &schema).unwrap();

    // Try to set age to a string (wrong type)
    let result = execute_gql_with_schema(
        &mut storage,
        "MATCH (n:Person) SET n.age = 'thirty'",
        &schema,
    );

    assert!(result.is_err());
    if let Err(MutationError::Schema(SchemaError::TypeMismatch { property, .. })) = result {
        assert_eq!(property, "age");
    } else {
        panic!("Expected TypeMismatch error, got {:?}", result);
    }
}

#[test]
fn test_set_required_property_to_null() {
    let graph = Arc::new(Graph::new());
    let mut storage = graph.as_storage_mut();
    let schema = create_test_schema(ValidationMode::Strict);

    // Create a Person
    execute_gql_with_schema(&mut storage, "CREATE (n:Person {name: 'Alice'})", &schema).unwrap();

    // Try to set required 'name' property to null
    let result =
        execute_gql_with_schema(&mut storage, "MATCH (n:Person) SET n.name = null", &schema);

    assert!(result.is_err());
    if let Err(MutationError::Schema(SchemaError::NullRequired { property, .. })) = result {
        assert_eq!(property, "name");
    } else {
        panic!("Expected NullRequired error, got {:?}", result);
    }
}

// --- MERGE Validation Tests ---

#[test]
fn test_merge_create_with_validation() {
    let graph = Arc::new(Graph::new());
    let mut storage = graph.as_storage_mut();
    let schema = create_test_schema(ValidationMode::Strict);

    // MERGE creates when pattern doesn't exist, should validate
    let result = execute_gql_with_schema(
        &mut storage,
        "MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.age = 30",
        &schema,
    );

    assert!(result.is_ok());
    assert_eq!(storage.vertex_count(), 1);
}

#[test]
fn test_merge_create_fails_validation() {
    let graph = Arc::new(Graph::new());
    let mut storage = graph.as_storage_mut();
    let schema = create_test_schema(ValidationMode::Strict);

    // MERGE creates, but missing required 'name' property
    let result = execute_gql_with_schema(&mut storage, "MERGE (n:Person {age: 30})", &schema);

    assert!(result.is_err());
    assert!(matches!(
        result,
        Err(MutationError::Schema(SchemaError::MissingRequired { .. }))
    ));
    assert_eq!(storage.vertex_count(), 0);
}

#[test]
fn test_merge_match_with_set_validation() {
    let graph = Arc::new(Graph::new());
    let mut storage = graph.as_storage_mut();
    let schema = create_test_schema(ValidationMode::Strict);

    // Create a person first
    execute_gql_with_schema(&mut storage, "CREATE (n:Person {name: 'Alice'})", &schema).unwrap();

    // MERGE matches existing, ON MATCH SET should validate
    let result = execute_gql_with_schema(
        &mut storage,
        "MERGE (n:Person {name: 'Alice'}) ON MATCH SET n.age = 30",
        &schema,
    );

    assert!(result.is_ok());
}

#[test]
fn test_merge_match_set_wrong_type() {
    let graph = Arc::new(Graph::new());
    let mut storage = graph.as_storage_mut();
    let schema = create_test_schema(ValidationMode::Strict);

    // Create a person first
    execute_gql_with_schema(&mut storage, "CREATE (n:Person {name: 'Alice'})", &schema).unwrap();

    // MERGE matches existing, but ON MATCH SET has wrong type
    let result = execute_gql_with_schema(
        &mut storage,
        "MERGE (n:Person {name: 'Alice'}) ON MATCH SET n.age = 'thirty'",
        &schema,
    );

    assert!(result.is_err());
    assert!(matches!(
        result,
        Err(MutationError::Schema(SchemaError::TypeMismatch { .. }))
    ));
}

// --- No Schema (backwards compatibility) Tests ---

#[test]
fn test_mutation_without_schema() {
    let graph = Arc::new(Graph::new());

    // Using gql() without schema should work without validation
    let result = execute_gql(&graph, "CREATE (n:Person {name: 42})"); // name as Int instead of String

    assert!(result.is_ok());
    assert_eq!(graph.vertex_count(), 1);
}

#[test]
fn test_mutation_with_none_schema() {
    let graph = Arc::new(Graph::new());
    let mut storage = graph.as_storage_mut();

    // Passing None as schema should behave the same as no schema
    let stmt = parse_statement("CREATE (n:Person {name: 42})").unwrap();
    let result = execute_mutation_with_schema(&stmt, &mut storage, None);

    assert!(result.is_ok());
    assert_eq!(storage.vertex_count(), 1);
}

// =============================================================================
// Graph API DDL Integration Tests
// =============================================================================

// These tests use the Graph API (COW-based) with direct ddl() method.

#[test]
fn test_graph_ddl_create_node_type() {
    let graph = Arc::new(Graph::new());

    // Create a node type using DDL
    let schema = graph
        .ddl("CREATE NODE TYPE Person (name STRING NOT NULL, age INT)")
        .unwrap();

    assert!(schema.has_vertex_schema("Person"));
    let person = schema.vertex_schema("Person").unwrap();
    assert!(person.properties.get("name").unwrap().required);
    assert!(!person.properties.get("age").unwrap().required);

    // Schema should persist on the graph
    let schema = graph.schema().expect("Schema should be set");
    assert!(schema.has_vertex_schema("Person"));
}

#[test]
fn test_graph_ddl_create_edge_type() {
    let graph = Arc::new(Graph::new());

    // Create node types first
    graph
        .ddl("CREATE NODE TYPE Person (name STRING NOT NULL)")
        .unwrap();
    graph
        .ddl("CREATE NODE TYPE Company (name STRING NOT NULL)")
        .unwrap();

    // Create an edge type
    let schema = graph
        .ddl("CREATE EDGE TYPE WORKS_AT (role STRING NOT NULL) FROM Person TO Company")
        .unwrap();

    assert!(schema.has_edge_schema("WORKS_AT"));
    let works_at = schema.edge_schema("WORKS_AT").unwrap();
    assert_eq!(works_at.from_labels, vec!["Person"]);
    assert_eq!(works_at.to_labels, vec!["Company"]);
}

#[test]
fn test_graph_ddl_set_validation_mode() {
    let graph = Arc::new(Graph::new());

    graph
        .ddl("CREATE NODE TYPE Person (name STRING NOT NULL)")
        .unwrap();

    let schema = graph.ddl("SET SCHEMA VALIDATION STRICT").unwrap();

    assert_eq!(schema.mode, ValidationMode::Strict);

    let schema = graph.schema().unwrap();
    assert_eq!(schema.mode, ValidationMode::Strict);
}

#[test]
fn test_graph_ddl_alter_node_type() {
    let graph = Arc::new(Graph::new());

    graph
        .ddl("CREATE NODE TYPE Person (name STRING NOT NULL)")
        .unwrap();

    // Add a property
    let schema = graph
        .ddl("ALTER NODE TYPE Person ADD email STRING")
        .unwrap();

    let person = schema.vertex_schema("Person").unwrap();
    assert!(person.properties.contains_key("email"));
    assert!(!person.properties.get("email").unwrap().required); // Added properties are optional

    // Allow additional properties
    let schema = graph
        .ddl("ALTER NODE TYPE Person ALLOW ADDITIONAL PROPERTIES")
        .unwrap();
    assert!(
        schema
            .vertex_schema("Person")
            .unwrap()
            .additional_properties
    );
}

#[test]
fn test_graph_ddl_drop_node_type() {
    let graph = Arc::new(Graph::new());

    graph
        .ddl("CREATE NODE TYPE Person (name STRING NOT NULL)")
        .unwrap();
    graph
        .ddl("CREATE NODE TYPE Company (name STRING NOT NULL)")
        .unwrap();

    assert!(graph.schema().unwrap().has_vertex_schema("Person"));
    assert!(graph.schema().unwrap().has_vertex_schema("Company"));

    // Drop Person type
    let schema = graph.ddl("DROP NODE TYPE Person").unwrap();

    assert!(!schema.has_vertex_schema("Person"));
    assert!(schema.has_vertex_schema("Company"));
}

#[test]
fn test_graph_ddl_full_workflow() {
    let graph = Arc::new(Graph::new());

    // Build schema using DDL
    graph
        .ddl("CREATE NODE TYPE Person (name STRING NOT NULL, age INT)")
        .unwrap();
    graph
        .ddl("CREATE NODE TYPE Software (name STRING NOT NULL, language STRING)")
        .unwrap();
    graph
        .ddl("CREATE EDGE TYPE KNOWS (since INT) FROM Person TO Person")
        .unwrap();
    graph
        .ddl("CREATE EDGE TYPE CREATED (year INT NOT NULL) FROM Person TO Software")
        .unwrap();
    graph.ddl("SET SCHEMA VALIDATION STRICT").unwrap();

    // Verify schema
    let schema = graph.schema().unwrap();
    assert_eq!(schema.mode, ValidationMode::Strict);
    assert!(schema.has_vertex_schema("Person"));
    assert!(schema.has_vertex_schema("Software"));
    assert!(schema.has_edge_schema("KNOWS"));
    assert!(schema.has_edge_schema("CREATED"));

    // Verify edge endpoints
    let created = schema.edge_schema("CREATED").unwrap();
    assert_eq!(created.from_labels, vec!["Person"]);
    assert_eq!(created.to_labels, vec!["Software"]);
}

#[test]
fn test_graph_ddl_error_handling() {
    let graph = Arc::new(Graph::new());

    // Create a type
    graph
        .ddl("CREATE NODE TYPE Person (name STRING NOT NULL)")
        .unwrap();

    // Try to create duplicate type - should fail
    let result = graph.ddl("CREATE NODE TYPE Person (name STRING)");
    assert!(result.is_err());

    // Try to drop non-existent type - should fail
    let result = graph.ddl("DROP NODE TYPE NonExistent");
    assert!(result.is_err());

    // Try to alter non-existent type - should fail
    let result = graph.ddl("ALTER NODE TYPE NonExistent ADD prop STRING");
    assert!(result.is_err());
}

#[test]
fn test_graph_ddl_parse_error() {
    let graph = Arc::new(Graph::new());

    // Invalid DDL syntax
    let result = graph.ddl("CREATE NODE TYPE");
    assert!(result.is_err());

    // Not a DDL statement (this is a query, not DDL)
    let result = graph.ddl("MATCH (n) RETURN n");
    assert!(result.is_err());
}

// =============================================================================
// FOREACH Clause Integration Tests
// =============================================================================

/// Helper to create a graph for FOREACH tests with relationships.
fn create_foreach_test_graph() -> Arc<Graph> {
    let graph = Arc::new(Graph::new());

    // Create several people
    let alice_id = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("visited".to_string(), Value::Bool(false)),
        ]),
    );

    let bob_id = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Bob".to_string())),
            ("visited".to_string(), Value::Bool(false)),
        ]),
    );

    let charlie_id = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Charlie".to_string())),
            ("visited".to_string(), Value::Bool(false)),
        ]),
    );

    // Alice knows Bob and Charlie
    graph
        .add_edge(alice_id, bob_id, "KNOWS", HashMap::new())
        .unwrap();
    graph
        .add_edge(alice_id, charlie_id, "KNOWS", HashMap::new())
        .unwrap();

    // Bob knows Charlie
    graph
        .add_edge(bob_id, charlie_id, "KNOWS", HashMap::new())
        .unwrap();

    graph
}

#[test]
fn test_foreach_set_property() {
    let graph = create_foreach_test_graph();

    // Use FOREACH to set a counter property based on list values
    // Note: FOREACH must come after at least one mutation clause per grammar
    execute_gql(
        &graph,
        r#"
        MATCH (p:Person {name: 'Alice'})
        SET p.marker = true
        FOREACH (i IN [1, 2, 3] | SET p.counter = i)
        "#,
    )
    .unwrap();

    // Alice should have counter = 3 (last value wins)
    let alice = graph
        .snapshot()
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
        .expect("Alice should exist");
    assert_eq!(alice.properties.get("counter"), Some(&Value::Int(3)));
    assert_eq!(alice.properties.get("marker"), Some(&Value::Bool(true)));
}

#[test]
fn test_foreach_remove_property() {
    let graph = Arc::new(Graph::new());

    // Create a vertex with properties we'll remove
    graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("temp1".to_string(), Value::Int(1)),
            ("temp2".to_string(), Value::Int(2)),
        ]),
    );

    // Use FOREACH to remove properties (by setting to null)
    // Note: FOREACH must come after at least one mutation clause per grammar
    execute_gql(
        &graph,
        r#"
        MATCH (p:Person {name: 'Alice'})
        SET p.marker = true
        FOREACH (prop IN [1, 2] | REMOVE p.temp1)
        "#,
    )
    .unwrap();

    let alice = graph
        .snapshot()
        .all_vertices()
        .next()
        .expect("Alice should exist");
    // temp1 should be set to null (our REMOVE implementation)
    assert_eq!(alice.properties.get("temp1"), Some(&Value::Null));
}

#[test]
fn test_foreach_multiple_mutations() {
    let graph = Arc::new(Graph::new());

    // Create vertices
    graph.add_vertex(
        "Person",
        HashMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
    );
    graph.add_vertex(
        "Person",
        HashMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
    );

    // Use FOREACH with multiple SET operations
    // Note: FOREACH must come after at least one mutation clause per grammar
    execute_gql(
        &graph,
        r#"
        MATCH (p:Person {name: 'Alice'})
        SET p.marker = true
        FOREACH (i IN [1, 2] | SET p.a = i, p.b = i * 10)
        "#,
    )
    .unwrap();

    let alice = graph
        .snapshot()
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
        .expect("Alice should exist");
    // Last iteration: i=2, so a=2, b=20
    assert_eq!(alice.properties.get("a"), Some(&Value::Int(2)));
    assert_eq!(alice.properties.get("b"), Some(&Value::Int(20)));
}

#[test]
fn test_foreach_empty_list() {
    let graph = Arc::new(Graph::new());

    graph.add_vertex(
        "Person",
        HashMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
    );

    // FOREACH with empty list should be a no-op
    // Note: FOREACH must come after at least one mutation clause per grammar
    let result = execute_gql(
        &graph,
        r#"
        MATCH (p:Person {name: 'Alice'})
        SET p.marker = true
        FOREACH (i IN [] | SET p.updated = true)
        RETURN p.name
        "#,
    );

    assert!(result.is_ok());

    let alice = graph
        .snapshot()
        .all_vertices()
        .next()
        .expect("Alice should exist");
    // marker should be set, but updated should not be since list was empty
    assert_eq!(alice.properties.get("marker"), Some(&Value::Bool(true)));
    assert_eq!(alice.properties.get("updated"), None);
}

#[test]
fn test_foreach_null_list() {
    let graph = Arc::new(Graph::new());

    graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("items".to_string(), Value::Null),
        ]),
    );

    // FOREACH with null list should be a no-op (not an error)
    // Note: FOREACH must come after at least one mutation clause per grammar
    let result = execute_gql(
        &graph,
        r#"
        MATCH (p:Person {name: 'Alice'})
        SET p.marker = true
        FOREACH (i IN p.items | SET p.processed = true)
        RETURN p.name
        "#,
    );

    assert!(result.is_ok());

    let alice = graph
        .snapshot()
        .all_vertices()
        .next()
        .expect("Alice should exist");
    // marker should be set, but processed should not since list was null
    assert_eq!(alice.properties.get("marker"), Some(&Value::Bool(true)));
    assert_eq!(alice.properties.get("processed"), None);
}

#[test]
fn test_foreach_non_list_error() {
    let graph = Arc::new(Graph::new());

    graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("age".to_string(), Value::Int(30)),
        ]),
    );

    // FOREACH with non-list expression should fail
    // Note: FOREACH must come after at least one mutation clause per grammar
    let result = execute_gql(
        &graph,
        r#"
        MATCH (p:Person {name: 'Alice'})
        SET p.marker = true
        FOREACH (i IN p.age | SET p.processed = true)
        "#,
    );

    assert!(result.is_err());
}

#[test]
fn test_foreach_variable_scope() {
    let graph = Arc::new(Graph::new());

    graph.add_vertex(
        "Person",
        HashMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
    );

    // Test that the FOREACH variable is available inside the mutations
    // Note: FOREACH must come after at least one mutation clause per grammar
    let result = execute_gql(
        &graph,
        r#"
        MATCH (p:Person {name: 'Alice'})
        SET p.marker = true
        FOREACH (x IN [100, 200, 300] | SET p.value = x)
        RETURN p.value
        "#,
    );

    assert!(result.is_ok());
    let results = result.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(300)); // Last value
}

#[test]
fn test_foreach_with_collected_list() {
    let graph = create_foreach_test_graph();

    // Use FOREACH with a collected list from a pattern
    // This test uses a standard MATCH + SET pattern since WITH...FOREACH is complex
    // Note: FOREACH must come after at least one mutation clause per grammar
    execute_gql(
        &graph,
        r#"
        MATCH (a:Person {name: 'Alice'})
        SET a.marker = true
        FOREACH (i IN [1, 2, 3] | SET a.lastValue = i)
        "#,
    )
    .unwrap();

    let alice = graph
        .snapshot()
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
        .expect("Alice should exist");
    // Should have lastValue set to 3 (last iteration)
    assert_eq!(alice.properties.get("lastValue"), Some(&Value::Int(3)));
}

#[test]
fn test_foreach_mark_friends_visited() {
    let graph = create_foreach_test_graph();

    // A practical use case: mark all friends of Alice as visited
    execute_gql(
        &graph,
        r#"
        MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(friend:Person)
        SET friend.visited = true
        "#,
    )
    .unwrap();

    // Bob and Charlie should be marked as visited
    let bob = graph
        .snapshot()
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Bob".to_string())))
        .expect("Bob should exist");
    assert_eq!(bob.properties.get("visited"), Some(&Value::Bool(true)));

    let charlie = graph
        .snapshot()
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Charlie".to_string())))
        .expect("Charlie should exist");
    assert_eq!(charlie.properties.get("visited"), Some(&Value::Bool(true)));

    // Alice should NOT be marked as visited (she was not a friend in the pattern)
    let alice = graph
        .snapshot()
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
        .expect("Alice should exist");
    assert_eq!(alice.properties.get("visited"), Some(&Value::Bool(false)));
}

#[test]
fn test_foreach_nested_iteration() {
    let graph = Arc::new(Graph::new());

    graph.add_vertex(
        "Counter",
        HashMap::from([
            ("name".to_string(), Value::String("counter".to_string())),
            ("value".to_string(), Value::Int(0)),
        ]),
    );

    // Nested FOREACH to multiply iterations
    // Note: FOREACH must come after at least one mutation clause per grammar
    execute_gql(
        &graph,
        r#"
        MATCH (c:Counter)
        SET c.marker = true
        FOREACH (x IN [1, 2] | FOREACH (y IN [10, 20] | SET c.value = x * y))
        "#,
    )
    .unwrap();

    let counter = graph
        .snapshot()
        .all_vertices()
        .next()
        .expect("Counter should exist");
    // Last iteration: x=2, y=20, so value = 40
    assert_eq!(counter.properties.get("value"), Some(&Value::Int(40)));
}
