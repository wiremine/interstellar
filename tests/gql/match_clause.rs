//! MATCH clause and edge traversal tests.
//!
//! Tests for MATCH patterns including:
//! - Edge traversal (outgoing, incoming, bidirectional)
//! - Property access in patterns
//! - Multi-hop traversal
//! - Property access in RETURN clause

use interstellar::gql::GqlError;
use interstellar::prelude::*;
use interstellar::storage::Graph;
use std::collections::HashMap;

/// Helper to create a test graph with sample data
fn create_test_graph() -> Graph {
    let graph = Graph::new();

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

    let mut acme_props = HashMap::new();
    acme_props.insert("name".to_string(), Value::from("Acme Corp"));
    graph.add_vertex("Company", acme_props);

    let mut globex_props = HashMap::new();
    globex_props.insert("name".to_string(), Value::from("Globex"));
    graph.add_vertex("Company", globex_props);

    graph
}

/// Helper to create a test graph with edges for traversal tests
fn create_graph_with_edges() -> Graph {
    let graph = Graph::new();

    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    let alice = graph.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    let bob = graph.add_vertex("Person", bob_props);

    let mut carol_props = HashMap::new();
    carol_props.insert("name".to_string(), Value::from("Carol"));
    let carol = graph.add_vertex("Person", carol_props);

    let mut dave_props = HashMap::new();
    dave_props.insert("name".to_string(), Value::from("Dave"));
    let dave = graph.add_vertex("Person", dave_props);

    graph.add_edge(alice, bob, "KNOWS", HashMap::new()).unwrap();
    graph
        .add_edge(alice, carol, "KNOWS", HashMap::new())
        .unwrap();
    graph.add_edge(bob, carol, "KNOWS", HashMap::new()).unwrap();
    graph
        .add_edge(bob, dave, "WORKS_WITH", HashMap::new())
        .unwrap();

    graph
}

// =============================================================================
// Edge Traversal Tests
// =============================================================================

#[test]
fn test_gql_outgoing_edge_traversal() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(friend) RETURN friend")
        .unwrap();

    assert_eq!(results.len(), 2, "Alice should know 2 people");
}

#[test]
fn test_gql_incoming_edge_traversal() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (c:Person {name: 'Carol'})<-[:KNOWS]-(source) RETURN source")
        .unwrap();

    assert_eq!(results.len(), 2, "Carol should be known by 2 people");
}

#[test]
fn test_gql_bidirectional_edge_traversal() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    let results = graph
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

    let results = graph
        .gql("MATCH (b:Person {name: 'Bob'})-[]->(target) RETURN target")
        .unwrap();

    assert_eq!(results.len(), 2, "Bob should have 2 outgoing edges");
}

#[test]
fn test_gql_multi_hop_traversal() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find Carol via two-hop traversal");
}

#[test]
fn test_gql_multi_hop_via_different_labels() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    let results = graph
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

    let results = graph
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(friend:Person {name: 'Bob'}) RETURN friend")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find exactly Bob");
}

#[test]
fn test_gql_no_matching_edges() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (a:Person {name: 'Alice'})-[:WORKS_WITH]->(coworker) RETURN coworker")
        .unwrap();

    assert_eq!(results.len(), 0, "Alice should have no WORKS_WITH edges");
}

#[test]
fn test_gql_chain_of_three_hops() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(d) RETURN d")
        .unwrap();

    assert_eq!(results.len(), 0, "No three-hop KNOWS path from Alice");
}

// =============================================================================
// Property Access in RETURN Clause Tests
// =============================================================================

#[test]
fn test_gql_return_single_property() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = graph.gql("MATCH (n:Person) RETURN n.name").unwrap();

    assert_eq!(results.len(), 3, "Should find 3 Person vertices");

    for result in &results {
        assert!(
            matches!(result, Value::String(_)),
            "Expected String, got {:?}",
            result
        );
    }

    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Bob"));
    assert!(names.contains(&"Charlie"));
}

#[test]
fn test_gql_return_numeric_property() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = graph.gql("MATCH (n:Person) RETURN n.age").unwrap();

    assert_eq!(results.len(), 3, "Should find 3 Person vertices with age");

    for result in &results {
        assert!(
            matches!(result, Value::Int(_)),
            "Expected Int, got {:?}",
            result
        );
    }

    let ages: Vec<i64> = results
        .iter()
        .filter_map(|v| match v {
            Value::Int(n) => Some(*n),
            _ => None,
        })
        .collect();
    assert!(ages.contains(&30));
    assert!(ages.contains(&25));
    assert!(ages.contains(&35));
}

#[test]
fn test_gql_return_multiple_properties() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (n:Person {name: 'Alice'}) RETURN n.name, n.age")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find 1 Alice vertex");

    match &results[0] {
        Value::Map(map) => {
            assert!(map.contains_key("n.name"), "Map should contain n.name key");
            assert!(map.contains_key("n.age"), "Map should contain n.age key");
            assert_eq!(map.get("n.name"), Some(&Value::String("Alice".to_string())));
            assert_eq!(map.get("n.age"), Some(&Value::Int(30)));
        }
        other => panic!("Expected Map, got {:?}", other),
    }
}

#[test]
fn test_gql_return_property_with_alias() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (n:Person {name: 'Alice'}) RETURN n.name AS personName")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find 1 Alice vertex");
    assert_eq!(results[0], Value::String("Alice".to_string()));
}

#[test]
fn test_gql_return_multiple_properties_with_aliases() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (n:Person {name: 'Bob'}) RETURN n.name AS name, n.age AS years")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find 1 Bob vertex");

    match &results[0] {
        Value::Map(map) => {
            assert!(map.contains_key("name"), "Map should contain 'name' key");
            assert!(map.contains_key("years"), "Map should contain 'years' key");
            assert_eq!(map.get("name"), Some(&Value::String("Bob".to_string())));
            assert_eq!(map.get("years"), Some(&Value::Int(25)));
        }
        other => panic!("Expected Map, got {:?}", other),
    }
}

#[test]
fn test_gql_return_missing_property() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = graph.gql("MATCH (c:Company) RETURN c.age").unwrap();

    assert_eq!(results.len(), 0, "No Company has age property");
}

#[test]
fn test_gql_return_property_from_traversal() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(friend) RETURN friend.name")
        .unwrap();

    assert_eq!(results.len(), 2, "Alice knows 2 people");

    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    assert!(names.contains(&"Bob"));
    assert!(names.contains(&"Carol"));
}

#[test]
fn test_gql_return_undefined_variable_in_property() {
    let graph = Graph::new();
    let snapshot = graph.snapshot();

    let result = graph.gql("MATCH (n:Person) RETURN x.name");

    assert!(result.is_err());
    assert!(matches!(result, Err(GqlError::Compile(_))));
}

#[test]
fn test_gql_return_mixed_variable_and_property() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (n:Person {name: 'Alice'}) RETURN n, n.name")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find 1 Alice vertex");

    match &results[0] {
        Value::Map(map) => {
            assert!(map.contains_key("n"), "Map should contain 'n' key");
            assert!(
                map.contains_key("n.name"),
                "Map should contain 'n.name' key"
            );
            assert!(matches!(map.get("n"), Some(Value::Vertex(_))));
            assert_eq!(map.get("n.name"), Some(&Value::String("Alice".to_string())));
        }
        other => panic!("Expected Map, got {:?}", other),
    }
}
