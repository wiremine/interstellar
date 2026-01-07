//! Integration tests for GQL module.

use rustgremlin::gql::{parse, GqlError};
use rustgremlin::prelude::*;
use rustgremlin::storage::InMemoryGraph;
use std::collections::HashMap;
use std::sync::Arc;

/// Helper to create a test graph with sample data
fn create_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // Create Person vertices
    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    alice_props.insert("age".to_string(), Value::from(30i64));
    storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    bob_props.insert("age".to_string(), Value::from(25i64));
    storage.add_vertex("Person", bob_props);

    let mut charlie_props = HashMap::new();
    charlie_props.insert("name".to_string(), Value::from("Charlie"));
    charlie_props.insert("age".to_string(), Value::from(35i64));
    storage.add_vertex("Person", charlie_props);

    // Create Company vertices
    let mut acme_props = HashMap::new();
    acme_props.insert("name".to_string(), Value::from("Acme Corp"));
    storage.add_vertex("Company", acme_props);

    let mut globex_props = HashMap::new();
    globex_props.insert("name".to_string(), Value::from("Globex"));
    storage.add_vertex("Company", globex_props);

    Graph::new(Arc::new(storage))
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
    let graph = Graph::in_memory();
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
    let graph = Graph::in_memory();
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
    let graph = Graph::in_memory();
    let snapshot = graph.snapshot();

    let result = snapshot.gql("MATCH (n:Person RETURN n");

    assert!(result.is_err());
    assert!(matches!(result, Err(GqlError::Parse(_))));
}

#[test]
fn test_gql_compile_error_undefined_variable() {
    let graph = Graph::in_memory();
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
    let mut storage = InMemoryGraph::new();
    let props = HashMap::new();
    storage.add_vertex("Test", props);

    let graph = Graph::new(Arc::new(storage));

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
    use rustgremlin::gql::parse;

    let query = parse("MATCH (n:Person) RETURN n").unwrap();

    assert_eq!(query.match_clause.patterns.len(), 1);
    assert_eq!(query.return_clause.items.len(), 1);
}

#[test]
fn test_compile_function_export() {
    use rustgremlin::gql::{compile, parse};

    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse("MATCH (n:Person) RETURN n").unwrap();
    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 3);
}

// =============================================================================
// Edge Traversal Tests
// =============================================================================

/// Helper to create a test graph with edges for traversal tests
fn create_graph_with_edges() -> Graph {
    let mut storage = InMemoryGraph::new();

    // Create Person vertices
    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    let alice = storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    let bob = storage.add_vertex("Person", bob_props);

    let mut carol_props = HashMap::new();
    carol_props.insert("name".to_string(), Value::from("Carol"));
    let carol = storage.add_vertex("Person", carol_props);

    let mut dave_props = HashMap::new();
    dave_props.insert("name".to_string(), Value::from("Dave"));
    let dave = storage.add_vertex("Person", dave_props);

    // Create edges:
    // Alice -[KNOWS]-> Bob
    // Alice -[KNOWS]-> Carol
    // Bob -[KNOWS]-> Carol
    // Bob -[WORKS_WITH]-> Dave
    storage
        .add_edge(alice, bob, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, carol, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, carol, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, dave, "WORKS_WITH", HashMap::new())
        .unwrap();

    Graph::new(Arc::new(storage))
}

#[test]
fn test_gql_outgoing_edge_traversal() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    // Alice knows 2 people (Bob and Carol)
    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(friend) RETURN friend")
        .unwrap();

    assert_eq!(results.len(), 2, "Alice should know 2 people");
}

#[test]
fn test_gql_incoming_edge_traversal() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    // Carol is known by 2 people (Alice and Bob)
    let results = snapshot
        .gql("MATCH (c:Person {name: 'Carol'})<-[:KNOWS]-(source) RETURN source")
        .unwrap();

    assert_eq!(results.len(), 2, "Carol should be known by 2 people");
}

#[test]
fn test_gql_bidirectional_edge_traversal() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    // Bob is connected via KNOWS to: Alice (incoming), Carol (outgoing)
    let results = snapshot
        .gql("MATCH (b:Person {name: 'Bob'})-[:KNOWS]-(connected) RETURN connected")
        .unwrap();

    assert_eq!(
        results.len(),
        2,
        "Bob should be connected to 2 people via KNOWS"
    );
}

#[test]
fn test_gql_edge_without_label() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    // Bob has 3 outgoing edges total (2 KNOWS + 1 WORKS_WITH)
    let results = snapshot
        .gql("MATCH (b:Person {name: 'Bob'})-[]->(target) RETURN target")
        .unwrap();

    assert_eq!(
        results.len(),
        2,
        "Bob should have 2 outgoing edges (Carol via KNOWS, Dave via WORKS_WITH)"
    );
}

#[test]
fn test_gql_multi_hop_traversal() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    // Alice -> Bob -> Carol (two hops via KNOWS)
    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c")
        .unwrap();

    // Alice knows Bob, Bob knows Carol -> should find Carol
    assert_eq!(results.len(), 1, "Should find Carol via two-hop traversal");
}

#[test]
fn test_gql_multi_hop_via_different_labels() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    // Alice -> Bob (KNOWS) -> Dave (WORKS_WITH)
    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:WORKS_WITH]->(c) RETURN c")
        .unwrap();

    assert_eq!(
        results.len(),
        1,
        "Should find Dave via Alice->Bob->Dave path"
    );
}

#[test]
fn test_gql_property_filter_on_pattern() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    // Find friends of Alice named Bob
    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(friend:Person {name: 'Bob'}) RETURN friend")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find exactly Bob");
}

#[test]
fn test_gql_no_matching_edges() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    // Alice has no outgoing WORKS_WITH edges
    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:WORKS_WITH]->(coworker) RETURN coworker")
        .unwrap();

    assert_eq!(results.len(), 0, "Alice should have no WORKS_WITH edges");
}

#[test]
fn test_gql_chain_of_three_hops() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    // Three-hop traversal: a -> b -> c -> d
    // With our graph: Alice -> Bob -> Carol, but Carol has no outgoing KNOWS edges
    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(d) RETURN d")
        .unwrap();

    // Carol has no outgoing KNOWS edges, so this should return empty
    assert_eq!(results.len(), 0, "No three-hop KNOWS path from Alice");
}
