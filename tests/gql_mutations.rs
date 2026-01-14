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
        MutationError::Compile(intersteller::gql::CompileError::UnsupportedFeature(
            format!("Parse error: {}", e),
        ))
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

// =============================================================================
// Schema Validation Tests
// =============================================================================

use intersteller::gql::execute_mutation_with_schema;
use intersteller::schema::{PropertyType, SchemaBuilder, SchemaError, ValidationMode};

/// Create a test schema for validation tests.
fn create_test_schema(mode: ValidationMode) -> intersteller::schema::GraphSchema {
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
    storage: &mut InMemoryGraph,
    query: &str,
    schema: &intersteller::schema::GraphSchema,
) -> Result<Vec<Value>, MutationError> {
    let stmt = parse_statement(query).unwrap();
    execute_mutation_with_schema(&stmt, storage, Some(schema))
}

// --- CREATE Vertex Validation Tests ---

#[test]
fn test_create_vertex_valid_schema() {
    let mut storage = InMemoryGraph::new();
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
    let mut storage = InMemoryGraph::new();
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
    let mut storage = InMemoryGraph::new();
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
    let mut storage = InMemoryGraph::new();
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
    let mut storage = InMemoryGraph::new();
    let schema = create_test_schema(ValidationMode::Strict);

    // Unknown labels are allowed in Strict mode
    let result =
        execute_gql_with_schema(&mut storage, "CREATE (n:Animal {name: 'Fluffy'})", &schema);

    assert!(result.is_ok());
    assert_eq!(storage.vertex_count(), 1);
}

#[test]
fn test_create_vertex_validation_mode_none() {
    let mut storage = InMemoryGraph::new();
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
    let mut storage = InMemoryGraph::new();
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
    let mut storage = InMemoryGraph::new();
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
    let mut storage = InMemoryGraph::new();
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
    let mut storage = InMemoryGraph::new();
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
    let mut storage = InMemoryGraph::new();
    let schema = create_test_schema(ValidationMode::Strict);

    // Create a Person
    execute_gql_with_schema(&mut storage, "CREATE (n:Person {name: 'Alice'})", &schema).unwrap();

    // Set age to an integer (correct type)
    let result = execute_gql_with_schema(&mut storage, "MATCH (n:Person) SET n.age = 30", &schema);

    assert!(result.is_ok());
}

#[test]
fn test_set_property_wrong_type() {
    let mut storage = InMemoryGraph::new();
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
    let mut storage = InMemoryGraph::new();
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
    let mut storage = InMemoryGraph::new();
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
    let mut storage = InMemoryGraph::new();
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
    let mut storage = InMemoryGraph::new();
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
    let mut storage = InMemoryGraph::new();
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
    let mut storage = InMemoryGraph::new();

    // Using regular execute_mutation should work without validation
    let result = execute_gql(&mut storage, "CREATE (n:Person {name: 42})"); // name as Int instead of String

    assert!(result.is_ok());
    assert_eq!(storage.vertex_count(), 1);
}

#[test]
fn test_mutation_with_none_schema() {
    let mut storage = InMemoryGraph::new();

    // Passing None as schema should behave the same as no schema
    let stmt = parse_statement("CREATE (n:Person {name: 42})").unwrap();
    let result = execute_mutation_with_schema(&stmt, &mut storage, None);

    assert!(result.is_ok());
    assert_eq!(storage.vertex_count(), 1);
}

// =============================================================================
// Graph API DDL Integration Tests
// =============================================================================

use intersteller::prelude::Graph;

#[test]
fn test_graph_ddl_create_node_type() {
    let graph = Graph::in_memory();

    {
        let mut_handle = graph.mutate();

        // Create a node type using DDL
        let schema = mut_handle
            .ddl("CREATE NODE TYPE Person (name STRING NOT NULL, age INT)")
            .unwrap();

        assert!(schema.has_vertex_schema("Person"));
        let person = schema.vertex_schema("Person").unwrap();
        assert!(person.properties.get("name").unwrap().required);
        assert!(!person.properties.get("age").unwrap().required);
    }

    // Schema should persist on the graph
    let schema = graph.schema().expect("Schema should be set");
    assert!(schema.has_vertex_schema("Person"));
}

#[test]
fn test_graph_ddl_create_edge_type() {
    let graph = Graph::in_memory();

    {
        let mut_handle = graph.mutate();

        // Create node types first
        mut_handle
            .ddl("CREATE NODE TYPE Person (name STRING NOT NULL)")
            .unwrap();
        mut_handle
            .ddl("CREATE NODE TYPE Company (name STRING NOT NULL)")
            .unwrap();

        // Create an edge type
        let schema = mut_handle
            .ddl("CREATE EDGE TYPE WORKS_AT (role STRING NOT NULL) FROM Person TO Company")
            .unwrap();

        assert!(schema.has_edge_schema("WORKS_AT"));
        let works_at = schema.edge_schema("WORKS_AT").unwrap();
        assert_eq!(works_at.from_labels, vec!["Person"]);
        assert_eq!(works_at.to_labels, vec!["Company"]);
    }
}

#[test]
fn test_graph_ddl_set_validation_mode() {
    let graph = Graph::in_memory();

    {
        let mut_handle = graph.mutate();

        mut_handle
            .ddl("CREATE NODE TYPE Person (name STRING NOT NULL)")
            .unwrap();

        let schema = mut_handle.ddl("SET SCHEMA VALIDATION STRICT").unwrap();

        assert_eq!(schema.mode, ValidationMode::Strict);
    }

    let schema = graph.schema().unwrap();
    assert_eq!(schema.mode, ValidationMode::Strict);
}

#[test]
fn test_graph_ddl_alter_node_type() {
    let graph = Graph::in_memory();

    {
        let mut_handle = graph.mutate();

        mut_handle
            .ddl("CREATE NODE TYPE Person (name STRING NOT NULL)")
            .unwrap();

        // Add a property
        let schema = mut_handle
            .ddl("ALTER NODE TYPE Person ADD email STRING")
            .unwrap();

        let person = schema.vertex_schema("Person").unwrap();
        assert!(person.properties.contains_key("email"));
        assert!(!person.properties.get("email").unwrap().required); // Added properties are optional

        // Allow additional properties
        let schema = mut_handle
            .ddl("ALTER NODE TYPE Person ALLOW ADDITIONAL PROPERTIES")
            .unwrap();
        assert!(
            schema
                .vertex_schema("Person")
                .unwrap()
                .additional_properties
        );
    }
}

#[test]
fn test_graph_ddl_drop_node_type() {
    let graph = Graph::in_memory();

    {
        let mut_handle = graph.mutate();

        mut_handle
            .ddl("CREATE NODE TYPE Person (name STRING NOT NULL)")
            .unwrap();
        mut_handle
            .ddl("CREATE NODE TYPE Company (name STRING NOT NULL)")
            .unwrap();

        assert!(graph.schema().unwrap().has_vertex_schema("Person"));
        assert!(graph.schema().unwrap().has_vertex_schema("Company"));

        // Drop Person type
        let schema = mut_handle.ddl("DROP NODE TYPE Person").unwrap();

        assert!(!schema.has_vertex_schema("Person"));
        assert!(schema.has_vertex_schema("Company"));
    }
}

#[test]
fn test_graph_ddl_full_workflow() {
    let storage = InMemoryGraph::new();
    let graph = Graph::with_schema(storage, intersteller::schema::GraphSchema::new());

    {
        let mut_handle = graph.mutate();

        // Build schema using DDL
        mut_handle
            .ddl("CREATE NODE TYPE Person (name STRING NOT NULL, age INT)")
            .unwrap();
        mut_handle
            .ddl("CREATE NODE TYPE Software (name STRING NOT NULL, language STRING)")
            .unwrap();
        mut_handle
            .ddl("CREATE EDGE TYPE KNOWS (since INT) FROM Person TO Person")
            .unwrap();
        mut_handle
            .ddl("CREATE EDGE TYPE CREATED (year INT NOT NULL) FROM Person TO Software")
            .unwrap();
        mut_handle.ddl("SET SCHEMA VALIDATION STRICT").unwrap();
    }

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
    let graph = Graph::in_memory();

    {
        let mut_handle = graph.mutate();

        // Create a type
        mut_handle
            .ddl("CREATE NODE TYPE Person (name STRING NOT NULL)")
            .unwrap();

        // Try to create duplicate type - should fail
        let result = mut_handle.ddl("CREATE NODE TYPE Person (name STRING)");
        assert!(result.is_err());

        // Try to drop non-existent type - should fail
        let result = mut_handle.ddl("DROP NODE TYPE NonExistent");
        assert!(result.is_err());

        // Try to alter non-existent type - should fail
        let result = mut_handle.ddl("ALTER NODE TYPE NonExistent ADD prop STRING");
        assert!(result.is_err());
    }
}

#[test]
fn test_graph_ddl_parse_error() {
    let graph = Graph::in_memory();

    {
        let mut_handle = graph.mutate();

        // Invalid DDL syntax
        let result = mut_handle.ddl("CREATE NODE TYPE");
        assert!(result.is_err());

        // Not a DDL statement (this is a query, not DDL)
        let result = mut_handle.ddl("MATCH (n) RETURN n");
        assert!(result.is_err());
    }
}
