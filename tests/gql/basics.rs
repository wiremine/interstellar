//! Basic GQL query tests.
//!
//! Tests for fundamental GQL operations:
//! - Basic MATCH queries
//! - Parser tests (case insensitivity, whitespace)
//! - Error handling tests
//! - API integration tests

use interstellar::gql::{compile, parse, GqlError};
use interstellar::prelude::*;
use interstellar::storage::Graph;
use std::collections::HashMap;

/// Helper to create a test graph with sample data
fn create_test_graph() -> Graph {
    let graph = Graph::new();

    // Create Person vertices
    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    alice_props.insert("age".to_string(), Value::from(30i64));
    graph.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    bob_props.insert("age".to_string(), Value::from(25i64));
    graph.add_vertex("Person", bob_props);

    let mut charlie_props = HashMap::new();
    charlie_props.insert("name".to_string(), Value::from("Charlie"));
    charlie_props.insert("age".to_string(), Value::from(35i64));
    graph.add_vertex("Person", charlie_props);

    // Create Company vertices
    let mut acme_props = HashMap::new();
    acme_props.insert("name".to_string(), Value::from("Acme Corp"));
    graph.add_vertex("Company", acme_props);

    let mut globex_props = HashMap::new();
    globex_props.insert("name".to_string(), Value::from("Globex"));
    graph.add_vertex("Company", globex_props);

    graph
}

// =============================================================================
// Basic Query Tests
// =============================================================================

#[test]
fn test_gql_match_with_label() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (n:Person) RETURN n").unwrap();

    // Should find 3 Person vertices
    assert_eq!(results.len(), 3);

    // All results should be vertices
    for result in &results {
        assert!(matches!(result, Value::Vertex(_)));
    }
}

#[test]
fn test_gql_match_different_label() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (c:Company) RETURN c").unwrap();

    // Should find 2 Company vertices
    assert_eq!(results.len(), 2);
}

#[test]
fn test_gql_match_all_vertices() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // No label filter - should return all vertices
    let results = snapshot.gql("MATCH (n) RETURN n").unwrap();

    // Should find all 5 vertices (3 Person + 2 Company)
    assert_eq!(results.len(), 5);
}

#[test]
fn test_gql_match_no_results() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // Label that doesn't exist
    let results = snapshot.gql("MATCH (x:NonExistent) RETURN x").unwrap();

    assert_eq!(results.len(), 0);
}

#[test]
fn test_gql_empty_graph() {
    let graph = Graph::new();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (n:Person) RETURN n").unwrap();

    assert_eq!(results.len(), 0);
}

// =============================================================================
// Parser Tests
// =============================================================================

#[test]
fn test_gql_case_insensitive_keywords() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // lowercase
    let r1 = snapshot.gql("match (n:Person) return n").unwrap();
    assert_eq!(r1.len(), 3);

    // UPPERCASE
    let r2 = snapshot.gql("MATCH (n:Person) RETURN n").unwrap();
    assert_eq!(r2.len(), 3);

    // Mixed case
    let r3 = snapshot.gql("Match (n:Person) Return n").unwrap();
    assert_eq!(r3.len(), 3);
}

#[test]
fn test_gql_whitespace_tolerance() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // Extra whitespace
    let results = snapshot
        .gql("  MATCH  (  n  :  Person  )  RETURN  n  ")
        .unwrap();
    assert_eq!(results.len(), 3);
}

// =============================================================================
// Error Tests
// =============================================================================

#[test]
fn test_gql_parse_error_missing_return() {
    let graph = Graph::new();
    let snapshot = graph.snapshot();

    let result = snapshot.gql("MATCH (n:Person)");

    assert!(result.is_err());
    assert!(matches!(result, Err(GqlError::Parse(_))));
}

#[test]
fn test_gql_parse_error_missing_match() {
    let result = parse("RETURN n");

    assert!(result.is_err());
}

#[test]
fn test_gql_parse_error_unclosed_paren() {
    let graph = Graph::new();
    let snapshot = graph.snapshot();

    let result = snapshot.gql("MATCH (n:Person RETURN n");

    assert!(result.is_err());
    assert!(matches!(result, Err(GqlError::Parse(_))));
}

#[test]
fn test_gql_compile_error_undefined_variable() {
    let graph = Graph::new();
    let snapshot = graph.snapshot();

    // Variable 'x' is not defined in MATCH clause
    let result = snapshot.gql("MATCH (n:Person) RETURN x");

    assert!(result.is_err());
    assert!(matches!(result, Err(GqlError::Compile(_))));
}

// =============================================================================
// API Integration Tests
// =============================================================================

#[test]
fn test_gql_method_on_snapshot() {
    let graph = Graph::new();
    let props = HashMap::new();
    graph.add_vertex("Test", props);

    // Test that gql() method works on snapshot
    let snapshot = graph.snapshot();
    let results = snapshot.gql("MATCH (n:Test) RETURN n").unwrap();

    assert_eq!(results.len(), 1);
}

#[test]
fn test_gql_multiple_queries_same_snapshot() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // Run multiple queries on the same snapshot
    let persons = snapshot.gql("MATCH (p:Person) RETURN p").unwrap();
    let companies = snapshot.gql("MATCH (c:Company) RETURN c").unwrap();
    let all = snapshot.gql("MATCH (n) RETURN n").unwrap();

    assert_eq!(persons.len(), 3);
    assert_eq!(companies.len(), 2);
    assert_eq!(all.len(), 5);
}

// =============================================================================
// Direct Parser and Compiler Tests
// =============================================================================

#[test]
fn test_parse_function_export() {
    let query = parse("MATCH (n:Person) RETURN n").unwrap();

    assert_eq!(query.match_clause.patterns.len(), 1);
    assert_eq!(query.return_clause.items.len(), 1);
}

#[test]
fn test_compile_function_export() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse("MATCH (n:Person) RETURN n").unwrap();
    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 3);
}
