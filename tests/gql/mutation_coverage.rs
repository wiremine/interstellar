//! Additional coverage tests for gql/mutation.rs.
//!
//! These tests target uncovered branches and edge cases in mutation execution.

use std::collections::HashMap;

use interstellar::gql::{
    execute_mutation, execute_mutation_with_schema, parse, parse_statement, CompileError,
    MutationError,
};
use interstellar::schema::{PropertyType, SchemaBuilder, ValidationMode};
use interstellar::storage::{GraphStorage, InMemoryGraph};
use interstellar::value::Value;

// =============================================================================
// Helper Functions
// =============================================================================

/// Creates a test graph with vertices and edges.
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

    let charlie_id = storage.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Charlie".to_string())),
            ("age".to_string(), Value::Int(35)),
        ]),
    );

    // Create edges
    storage
        .add_edge(
            alice_id,
            bob_id,
            "KNOWS",
            HashMap::from([("since".to_string(), Value::Int(2020))]),
        )
        .unwrap();

    storage
        .add_edge(
            bob_id,
            charlie_id,
            "KNOWS",
            HashMap::from([("since".to_string(), Value::Int(2021))]),
        )
        .unwrap();

    storage
}

/// Execute a GQL mutation query against storage.
fn execute_gql(storage: &mut InMemoryGraph, query: &str) -> Result<Vec<Value>, MutationError> {
    let stmt = parse_statement(query).map_err(|e| {
        MutationError::Compile(CompileError::UnsupportedFeature(format!(
            "Parse error: {}",
            e
        )))
    })?;
    execute_mutation(&stmt, storage)
}

// =============================================================================
// Statement Type Errors
// =============================================================================

#[test]
fn test_execute_mutation_with_query_statement_error() {
    let mut storage = InMemoryGraph::new();

    // Parse a read query (not a mutation)
    let query = parse("MATCH (n:Person) RETURN n").unwrap();

    // Try to execute it as a mutation - should fail
    let result = execute_mutation(
        &interstellar::gql::Statement::Query(Box::new(query)),
        &mut storage,
    );

    assert!(result.is_err());
    match result {
        Err(MutationError::Compile(CompileError::UnsupportedFeature(msg))) => {
            assert!(msg.contains("Expected mutation statement"));
        }
        _ => panic!("Expected UnsupportedFeature error"),
    }
}

#[test]
fn test_execute_mutation_with_ddl_statement_error() {
    let mut storage = InMemoryGraph::new();

    // Parse a DDL statement (must have parentheses for properties)
    let stmt = parse_statement("CREATE NODE TYPE Person ()").unwrap();

    // Try to execute it as a mutation - should fail
    let result = execute_mutation(&stmt, &mut storage);

    assert!(result.is_err());
    match result {
        Err(MutationError::Compile(CompileError::UnsupportedFeature(msg))) => {
            assert!(msg.contains("DDL statement"));
        }
        _ => panic!("Expected UnsupportedFeature error for DDL"),
    }
}

#[test]
fn test_execute_mutation_with_schema_ddl_error() {
    let mut storage = InMemoryGraph::new();
    let schema = SchemaBuilder::new()
        .mode(ValidationMode::Strict)
        .vertex("Person")
        .done()
        .build();

    // Parse a DDL statement (must have parentheses for properties)
    let stmt = parse_statement("CREATE NODE TYPE Person ()").unwrap();

    // Try to execute it with schema - should fail
    let result = execute_mutation_with_schema(&stmt, &mut storage, Some(&schema));

    assert!(result.is_err());
    match result {
        Err(MutationError::Compile(CompileError::UnsupportedFeature(msg))) => {
            assert!(msg.contains("DDL statement"));
        }
        _ => panic!("Expected UnsupportedFeature error for DDL"),
    }
}

// =============================================================================
// Schema Validation Tests
// =============================================================================

#[test]
fn test_create_vertex_with_schema_validation() {
    let mut storage = InMemoryGraph::new();
    let schema = SchemaBuilder::new()
        .mode(ValidationMode::Strict)
        .vertex("Person")
        .property("name", PropertyType::String)
        .property("age", PropertyType::Int)
        .done()
        .build();

    let stmt = parse_statement("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    let result = execute_mutation_with_schema(&stmt, &mut storage, Some(&schema));

    assert!(result.is_ok());
    assert_eq!(storage.vertex_count(), 1);
}

#[test]
fn test_set_edge_property_with_schema() {
    let mut storage = create_test_graph();
    let schema = SchemaBuilder::new()
        .mode(ValidationMode::Strict)
        .edge("KNOWS")
        .property("since", PropertyType::Int)
        .property("strength", PropertyType::Int)
        .done()
        .build();

    // Update edge property
    let stmt =
        parse_statement("MATCH (a:Person)-[r:KNOWS]->(b:Person) SET r.strength = 10").unwrap();
    let result = execute_mutation_with_schema(&stmt, &mut storage, Some(&schema));

    assert!(result.is_ok());
}

#[test]
fn test_create_edge_with_schema_validation() {
    let mut storage = InMemoryGraph::new();
    let schema = SchemaBuilder::new()
        .mode(ValidationMode::Strict)
        .vertex("Person")
        .property("name", PropertyType::String)
        .done()
        .edge("KNOWS")
        .from(&["Person"])
        .to(&["Person"])
        .property("since", PropertyType::Int)
        .done()
        .build();

    let stmt = parse_statement(
        "CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})",
    )
    .unwrap();
    let result = execute_mutation_with_schema(&stmt, &mut storage, Some(&schema));

    assert!(result.is_ok());
    assert_eq!(storage.edge_count(), 1);
}

// =============================================================================
// Edge Direction Tests
// =============================================================================

#[test]
fn test_match_incoming_edge() {
    let mut storage = create_test_graph();

    // Match incoming edges (Bob receives KNOWS from Alice)
    let result = execute_gql(
        &mut storage,
        "MATCH (b:Person {name: 'Bob'})<-[r:KNOWS]-(a:Person) SET b.incoming = true",
    );

    assert!(result.is_ok());
}

#[test]
fn test_match_bidirectional_edge() {
    let mut storage = create_test_graph();

    // Match edges in both directions
    let result = execute_gql(
        &mut storage,
        "MATCH (a:Person)-[r:KNOWS]-(b:Person) WHERE a.name = 'Bob' SET a.connected = true",
    );

    assert!(result.is_ok());
}

// =============================================================================
// Expression Evaluation Tests
// =============================================================================

#[test]
fn test_match_with_is_null_expression() {
    let mut storage = InMemoryGraph::new();

    // Create vertex with and without a property
    storage.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("email".to_string(), Value::Null),
        ]),
    );
    storage.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Bob".to_string())),
            (
                "email".to_string(),
                Value::String("bob@example.com".to_string()),
            ),
        ]),
    );

    // Match where email IS NULL
    let result = execute_gql(
        &mut storage,
        "MATCH (n:Person) WHERE n.email IS NULL SET n.needs_email = true",
    );

    assert!(result.is_ok());
}

#[test]
fn test_match_with_is_not_null_expression() {
    let mut storage = InMemoryGraph::new();

    storage.add_vertex(
        "Person",
        HashMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
    );
    storage.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Bob".to_string())),
            (
                "email".to_string(),
                Value::String("bob@example.com".to_string()),
            ),
        ]),
    );

    // Match where email IS NOT NULL
    let result = execute_gql(
        &mut storage,
        "MATCH (n:Person) WHERE n.email IS NOT NULL SET n.has_email = true",
    );

    assert!(result.is_ok());
}

#[test]
fn test_match_with_in_list_expression() {
    let mut storage = create_test_graph();

    // Match where name IN list
    let result = execute_gql(
        &mut storage,
        "MATCH (n:Person) WHERE n.name IN ['Alice', 'Bob'] SET n.in_group = true",
    );

    assert!(result.is_ok());
}

#[test]
fn test_match_with_not_in_list_expression() {
    let mut storage = create_test_graph();

    // Match where name NOT IN list
    let result = execute_gql(
        &mut storage,
        "MATCH (n:Person) WHERE n.name NOT IN ['Alice', 'Bob'] SET n.not_in_group = true",
    );

    assert!(result.is_ok());
}

#[test]
fn test_match_with_not_expression() {
    let mut storage = create_test_graph();

    // Match with NOT expression
    let result = execute_gql(
        &mut storage,
        "MATCH (n:Person) WHERE NOT n.age > 30 SET n.young = true",
    );

    assert!(result.is_ok());
}

#[test]
fn test_match_with_or_expression() {
    let mut storage = create_test_graph();

    // Match with OR expression
    let result = execute_gql(
        &mut storage,
        "MATCH (n:Person) WHERE n.age < 26 OR n.age > 34 SET n.extreme_age = true",
    );

    assert!(result.is_ok());
}

#[test]
fn test_match_with_arithmetic_expression() {
    let mut storage = create_test_graph();

    // SET with arithmetic expression
    let result = execute_gql(
        &mut storage,
        "MATCH (n:Person) SET n.doubled_age = n.age * 2",
    );

    assert!(result.is_ok());

    // Verify arithmetic worked
    for vertex in storage.all_vertices() {
        if let (Some(Value::Int(age)), Some(Value::Int(doubled))) = (
            vertex.properties.get("age"),
            vertex.properties.get("doubled_age"),
        ) {
            assert_eq!(*doubled, *age * 2);
        }
    }
}

#[test]
fn test_match_with_subtraction_expression() {
    let mut storage = create_test_graph();

    let result = execute_gql(
        &mut storage,
        "MATCH (n:Person) SET n.age_minus_ten = n.age - 10",
    );

    assert!(result.is_ok());
}

#[test]
fn test_match_with_division_expression() {
    let mut storage = create_test_graph();

    let result = execute_gql(&mut storage, "MATCH (n:Person) SET n.half_age = n.age / 2");

    assert!(result.is_ok());
}

#[test]
fn test_match_with_modulo_expression() {
    let mut storage = create_test_graph();

    let result = execute_gql(&mut storage, "MATCH (n:Person) SET n.age_mod = n.age % 10");

    assert!(result.is_ok());
}

#[test]
fn test_match_with_negation_expression() {
    let mut storage = create_test_graph();

    let result = execute_gql(&mut storage, "MATCH (n:Person) SET n.neg_age = -n.age");

    assert!(result.is_ok());
}

// =============================================================================
// Comparison Operators
// =============================================================================

#[test]
fn test_match_with_less_than_or_equal() {
    let mut storage = create_test_graph();

    let result = execute_gql(
        &mut storage,
        "MATCH (n:Person) WHERE n.age <= 30 SET n.thirty_or_less = true",
    );

    assert!(result.is_ok());
}

#[test]
fn test_match_with_greater_than_or_equal() {
    let mut storage = create_test_graph();

    let result = execute_gql(
        &mut storage,
        "MATCH (n:Person) WHERE n.age >= 30 SET n.thirty_or_more = true",
    );

    assert!(result.is_ok());
}

#[test]
fn test_match_with_not_equal() {
    let mut storage = create_test_graph();

    let result = execute_gql(
        &mut storage,
        "MATCH (n:Person) WHERE n.name <> 'Alice' SET n.not_alice = true",
    );

    assert!(result.is_ok());
}

// =============================================================================
// RETURN Clause Tests
// =============================================================================

#[test]
fn test_return_multiple_items() {
    let mut storage = InMemoryGraph::new();

    let results = execute_gql(
        &mut storage,
        "CREATE (n:Person {name: 'Alice', age: 30}) RETURN n.name, n.age",
    );

    assert!(results.is_ok());
    let results = results.unwrap();
    assert_eq!(results.len(), 1);

    // Should return a map with both values
    if let Value::Map(map) = &results[0] {
        assert!(map.contains_key("n.name"));
        assert!(map.contains_key("n.age"));
    } else {
        panic!("Expected Map value");
    }
}

#[test]
fn test_return_with_alias() {
    let mut storage = InMemoryGraph::new();

    let results = execute_gql(
        &mut storage,
        "CREATE (n:Person {name: 'Alice', age: 30}) RETURN n.name AS person_name, n.age AS person_age",
    );

    assert!(results.is_ok());
    let results = results.unwrap();

    if let Value::Map(map) = &results[0] {
        assert!(map.contains_key("person_name"));
        assert!(map.contains_key("person_age"));
    } else {
        panic!("Expected Map value with aliases");
    }
}

#[test]
fn test_return_empty_items() {
    let mut storage = InMemoryGraph::new();

    // Create without return clause
    let results = execute_gql(&mut storage, "CREATE (n:Person {name: 'Alice'})");

    assert!(results.is_ok());
    assert!(results.unwrap().is_empty());
}

// =============================================================================
// Edge Property Tests
// =============================================================================

#[test]
fn test_match_edge_and_set_property() {
    let mut storage = create_test_graph();

    // Match edge and set new property
    let result = execute_gql(
        &mut storage,
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) SET r.verified = true",
    );

    assert!(result.is_ok());

    // Verify edge property was set
    for edge in storage.all_edges() {
        assert_eq!(edge.properties.get("verified"), Some(&Value::Bool(true)));
    }
}

#[test]
fn test_match_edge_property_filter() {
    let mut storage = create_test_graph();

    // Match edge with property filter
    let result = execute_gql(
        &mut storage,
        "MATCH (a:Person)-[r:KNOWS {since: 2020}]->(b:Person) SET a.knows_since_2020 = true",
    );

    assert!(result.is_ok());
}

#[test]
fn test_remove_edge_property() {
    let mut storage = create_test_graph();

    // Remove edge property
    let result = execute_gql(
        &mut storage,
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) REMOVE r.since",
    );

    assert!(result.is_ok());

    // Verify property was removed (set to Null)
    for edge in storage.all_edges() {
        assert_eq!(edge.properties.get("since"), Some(&Value::Null));
    }
}

// =============================================================================
// MERGE Tests
// =============================================================================

#[test]
fn test_merge_creates_when_no_match() {
    let mut storage = InMemoryGraph::new();

    let result = execute_gql(
        &mut storage,
        "MERGE (n:Person {name: 'Diana'}) ON CREATE SET n.new = true",
    );

    assert!(result.is_ok());
    assert_eq!(storage.vertex_count(), 1);

    let vertex = storage.all_vertices().next().unwrap();
    assert_eq!(vertex.properties.get("new"), Some(&Value::Bool(true)));
}

#[test]
fn test_merge_matches_existing() {
    let mut storage = create_test_graph();
    let initial_count = storage.vertex_count();

    let result = execute_gql(
        &mut storage,
        "MERGE (n:Person {name: 'Alice'}) ON MATCH SET n.merged = true",
    );

    assert!(result.is_ok());
    assert_eq!(storage.vertex_count(), initial_count); // No new vertex

    // Verify Alice was updated
    let alice = storage
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
        .unwrap();
    assert_eq!(alice.properties.get("merged"), Some(&Value::Bool(true)));
}

// =============================================================================
// Error Cases
// =============================================================================

#[test]
fn test_set_unbound_variable_error() {
    let mut storage = create_test_graph();

    // Try to set property on unbound variable
    let result = execute_gql(&mut storage, "MATCH (n:Person) SET x.prop = 1");

    assert!(matches!(result, Err(MutationError::UnboundVariable(_))));
}

#[test]
fn test_delete_unbound_variable_error() {
    let mut storage = create_test_graph();

    let result = execute_gql(&mut storage, "MATCH (n:Person) DELETE x");

    assert!(matches!(result, Err(MutationError::UnboundVariable(_))));
}

#[test]
fn test_remove_unbound_variable_error() {
    let mut storage = create_test_graph();

    let result = execute_gql(&mut storage, "MATCH (n:Person) REMOVE x.prop");

    assert!(matches!(result, Err(MutationError::UnboundVariable(_))));
}

#[test]
fn test_detach_delete_unbound_variable_error() {
    let mut storage = create_test_graph();

    let result = execute_gql(&mut storage, "MATCH (n:Person) DETACH DELETE x");

    assert!(matches!(result, Err(MutationError::UnboundVariable(_))));
}

#[test]
fn test_create_vertex_missing_label_error() {
    let mut storage = InMemoryGraph::new();

    // This should fail because anonymous vertex needs a label
    // But our parser likely won't allow this syntax anyway
    // Let's test via execute with pattern that might lack label
    let result = execute_gql(&mut storage, "CREATE (n)");

    // Either parse error or MissingLabel
    assert!(result.is_err());
}

#[test]
fn test_delete_vertex_with_edges_error() {
    let mut storage = create_test_graph();

    // Try to delete vertex with edges
    let result = execute_gql(&mut storage, "MATCH (n:Person {name: 'Alice'}) DELETE n");

    assert!(matches!(result, Err(MutationError::VertexHasEdges(_))));
}

#[test]
fn test_match_no_results() {
    let mut storage = create_test_graph();

    // Match that finds nothing
    let result = execute_gql(
        &mut storage,
        "MATCH (n:Person {name: 'NonExistent'}) SET n.found = true",
    );

    assert!(result.is_ok());
    assert!(result.unwrap().is_empty());
}

// =============================================================================
// CREATE Edge Tests
// =============================================================================

#[test]
fn test_create_edge_between_matched_vertices() {
    let mut storage = create_test_graph();
    let initial_edge_count = storage.edge_count();

    // Match existing vertices via a path and create new edge
    // Alice->Bob already exists, so we can match it and create another edge
    let result = execute_gql(
        &mut storage,
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) CREATE (a)-[:FRIENDS]->(b)",
    );

    assert!(result.is_ok());
    assert_eq!(storage.edge_count(), initial_edge_count + 1);
}

#[test]
fn test_create_incoming_edge() {
    let mut storage = InMemoryGraph::new();

    // Create vertices and incoming edge
    let result = execute_gql(
        &mut storage,
        "CREATE (a:Person {name: 'Alice'})<-[:FOLLOWS]-(b:Person {name: 'Bob'})",
    );

    assert!(result.is_ok());
    assert_eq!(storage.vertex_count(), 2);
    assert_eq!(storage.edge_count(), 1);

    // Verify edge direction
    let edge = storage.all_edges().next().unwrap();
    assert_eq!(edge.label, "FOLLOWS");
}

#[test]
fn test_create_with_edge_variable() {
    let mut storage = InMemoryGraph::new();

    // Create edge with variable and return it
    let results = execute_gql(
        &mut storage,
        "CREATE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) RETURN r",
    );

    assert!(results.is_ok());
    let results = results.unwrap();
    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Value::Edge(_)));
}

// =============================================================================
// DETACH DELETE Tests
// =============================================================================

#[test]
fn test_detach_delete_edge() {
    let mut storage = create_test_graph();
    let initial_edge_count = storage.edge_count();

    // Delete an edge using DETACH DELETE
    let result = execute_gql(
        &mut storage,
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.name = 'Alice' DETACH DELETE r",
    );

    assert!(result.is_ok());
    assert_eq!(storage.edge_count(), initial_edge_count - 1);
}

#[test]
fn test_detach_delete_removes_all_edges() {
    let mut storage = create_test_graph();

    // DETACH DELETE vertex should remove its edges too
    let result = execute_gql(
        &mut storage,
        "MATCH (n:Person {name: 'Bob'}) DETACH DELETE n",
    );

    assert!(result.is_ok());
    // Bob was in the middle of Alice->Bob->Charlie, so both edges should be gone
    // and only Alice and Charlie remain
    assert_eq!(storage.vertex_count(), 2);
}

// =============================================================================
// Float Comparison Tests
// =============================================================================

#[test]
fn test_match_with_float_comparison() {
    let mut storage = InMemoryGraph::new();

    storage.add_vertex(
        "Item",
        HashMap::from([
            ("name".to_string(), Value::String("A".to_string())),
            ("price".to_string(), Value::Float(10.5)),
        ]),
    );
    storage.add_vertex(
        "Item",
        HashMap::from([
            ("name".to_string(), Value::String("B".to_string())),
            ("price".to_string(), Value::Float(20.5)),
        ]),
    );

    let result = execute_gql(
        &mut storage,
        "MATCH (n:Item) WHERE n.price > 15.0 SET n.expensive = true",
    );

    assert!(result.is_ok());
}

#[test]
fn test_float_arithmetic() {
    let mut storage = InMemoryGraph::new();

    storage.add_vertex(
        "Item",
        HashMap::from([("value".to_string(), Value::Float(10.5))]),
    );

    let result = execute_gql(&mut storage, "MATCH (n:Item) SET n.doubled = n.value * 2.0");

    assert!(result.is_ok());

    let item = storage.all_vertices().next().unwrap();
    if let Some(Value::Float(doubled)) = item.properties.get("doubled") {
        assert!((doubled - 21.0).abs() < 0.01);
    }
}

// =============================================================================
// Mixed Int/Float Operations
// =============================================================================

#[test]
fn test_int_float_mixed_comparison() {
    let mut storage = InMemoryGraph::new();

    storage.add_vertex(
        "Item",
        HashMap::from([
            ("int_val".to_string(), Value::Int(10)),
            ("float_val".to_string(), Value::Float(10.5)),
        ]),
    );

    // Compare int to float
    let result = execute_gql(
        &mut storage,
        "MATCH (n:Item) WHERE n.int_val < n.float_val SET n.int_smaller = true",
    );

    assert!(result.is_ok());
}

#[test]
fn test_int_float_mixed_arithmetic() {
    let mut storage = InMemoryGraph::new();

    storage.add_vertex(
        "Item",
        HashMap::from([("int_val".to_string(), Value::Int(10))]),
    );

    // Add int to float literal
    let result = execute_gql(&mut storage, "MATCH (n:Item) SET n.sum = n.int_val + 5.5");

    assert!(result.is_ok());
}

// =============================================================================
// String Comparison
// =============================================================================

#[test]
fn test_string_comparison() {
    let mut storage = create_test_graph();

    // String less than comparison
    let result = execute_gql(
        &mut storage,
        "MATCH (n:Person) WHERE n.name < 'Bob' SET n.before_bob = true",
    );

    assert!(result.is_ok());

    // Alice comes before Bob alphabetically
    let alice = storage
        .all_vertices()
        .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
        .unwrap();
    assert_eq!(alice.properties.get("before_bob"), Some(&Value::Bool(true)));
}

// =============================================================================
// Division by Zero
// =============================================================================

#[test]
fn test_division_by_zero_int() {
    let mut storage = InMemoryGraph::new();

    storage.add_vertex(
        "Item",
        HashMap::from([("value".to_string(), Value::Int(10))]),
    );

    // Division by zero should return 0 for int
    let result = execute_gql(&mut storage, "MATCH (n:Item) SET n.result = n.value / 0");

    assert!(result.is_ok());

    let item = storage.all_vertices().next().unwrap();
    assert_eq!(item.properties.get("result"), Some(&Value::Int(0)));
}

#[test]
fn test_modulo_by_zero() {
    let mut storage = InMemoryGraph::new();

    storage.add_vertex(
        "Item",
        HashMap::from([("value".to_string(), Value::Int(10))]),
    );

    // Modulo by zero should return 0 for int
    let result = execute_gql(&mut storage, "MATCH (n:Item) SET n.result = n.value % 0");

    assert!(result.is_ok());
}
