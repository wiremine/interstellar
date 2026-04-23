//! WHERE clause tests.
//!
//! Tests for WHERE predicates including:
//! - Comparison operators (>, <, >=, <=, =, <>)
//! - Boolean operators (AND, OR, NOT)
//! - String operations (CONTAINS, STARTS WITH, ENDS WITH)
//! - List operations (IN, NOT IN)
//! - Null checks (IS NULL, IS NOT NULL)

#![allow(unused_variables)]
use interstellar::gql::GqlError;
use interstellar::prelude::*;
use interstellar::storage::Graph;
use std::collections::HashMap;
use std::sync::Arc;

/// Helper to create a graph for WHERE clause tests
fn create_where_test_graph() -> Arc<Graph> {
    let graph = Arc::new(Graph::new());

    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    alice_props.insert("age".to_string(), Value::from(30i64));
    alice_props.insert("city".to_string(), Value::from("NYC"));
    alice_props.insert("active".to_string(), Value::from(true));
    graph.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    bob_props.insert("age".to_string(), Value::from(25i64));
    bob_props.insert("city".to_string(), Value::from("LA"));
    bob_props.insert("active".to_string(), Value::from(true));
    graph.add_vertex("Person", bob_props);

    let mut carol_props = HashMap::new();
    carol_props.insert("name".to_string(), Value::from("Carol"));
    carol_props.insert("age".to_string(), Value::from(35i64));
    carol_props.insert("city".to_string(), Value::from("NYC"));
    carol_props.insert("active".to_string(), Value::from(false));
    graph.add_vertex("Person", carol_props);

    let mut dave_props = HashMap::new();
    dave_props.insert("name".to_string(), Value::from("Dave"));
    dave_props.insert("age".to_string(), Value::from(28i64));
    dave_props.insert("city".to_string(), Value::from("Chicago"));
    // Note: Dave has no 'active' property (for IS NULL tests)
    graph.add_vertex("Person", dave_props);

    graph
}

#[test]
fn test_gql_where_greater_than() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (p:Person) WHERE p.age > 28 RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 2, "Should find 2 people over 28");

    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert!(names.contains(&"Alice"), "Should include Alice (30)");
    assert!(names.contains(&"Carol"), "Should include Carol (35)");
}

#[test]
fn test_gql_where_less_than() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (p:Person) WHERE p.age < 28 RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find 1 person under 28");
    assert_eq!(results[0], Value::String("Bob".to_string()));
}

#[test]
fn test_gql_where_equality() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (p:Person) WHERE p.name = 'Alice' RETURN p")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find exactly Alice");
}

#[test]
fn test_gql_where_not_equal() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (p:Person) WHERE p.city <> 'NYC' RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 2, "Should find 2 people not in NYC");

    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert!(names.contains(&"Bob"), "Should include Bob (LA)");
    assert!(names.contains(&"Dave"), "Should include Dave (Chicago)");
}

#[test]
fn test_gql_where_and() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (p:Person) WHERE p.age > 25 AND p.city = 'NYC' RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 2, "Should find 2 people over 25 in NYC");

    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert!(names.contains(&"Alice"), "Should include Alice");
    assert!(names.contains(&"Carol"), "Should include Carol");
}

#[test]
fn test_gql_where_or() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (p:Person) WHERE p.name = 'Alice' OR p.name = 'Bob' RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 2, "Should find Alice or Bob");

    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Bob"));
}

#[test]
fn test_gql_where_not() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (p:Person) WHERE NOT p.active RETURN p.name")
        .unwrap();

    // Carol has active=false, Dave has no active property (treated as falsy)
    assert!(!results.is_empty(), "Should find at least Carol");
}

#[test]
fn test_gql_where_greater_equal_less_equal() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    // >= test
    let results = graph
        .gql("MATCH (p:Person) WHERE p.age >= 30 RETURN p.name")
        .unwrap();
    assert_eq!(results.len(), 2, "Should find Alice (30) and Carol (35)");

    // <= test
    let results = graph
        .gql("MATCH (p:Person) WHERE p.age <= 28 RETURN p.name")
        .unwrap();
    assert_eq!(results.len(), 2, "Should find Bob (25) and Dave (28)");
}

#[test]
fn test_gql_where_contains() {
    let graph = Arc::new(Graph::new());

    let mut props1 = HashMap::new();
    props1.insert("name".to_string(), Value::from("Alice Anderson"));
    graph.add_vertex("Person", props1);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Bob Brown"));
    graph.add_vertex("Person", props2);

    let mut props3 = HashMap::new();
    props3.insert("name".to_string(), Value::from("Carol Anderson"));
    graph.add_vertex("Person", props3);

    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (p:Person) WHERE p.name CONTAINS 'Anderson' RETURN p")
        .unwrap();

    assert_eq!(results.len(), 2, "Should find 2 Andersons");
}

#[test]
fn test_gql_where_starts_with() {
    let graph = Arc::new(Graph::new());

    let mut props1 = HashMap::new();
    props1.insert("name".to_string(), Value::from("Alice"));
    graph.add_vertex("Person", props1);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Albert"));
    graph.add_vertex("Person", props2);

    let mut props3 = HashMap::new();
    props3.insert("name".to_string(), Value::from("Bob"));
    graph.add_vertex("Person", props3);

    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (p:Person) WHERE p.name STARTS WITH 'Al' RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 2, "Should find Alice and Albert");
}

#[test]
fn test_gql_where_ends_with() {
    let graph = Arc::new(Graph::new());

    let mut props1 = HashMap::new();
    props1.insert("email".to_string(), Value::from("alice@example.com"));
    graph.add_vertex("Person", props1);

    let mut props2 = HashMap::new();
    props2.insert("email".to_string(), Value::from("bob@test.org"));
    graph.add_vertex("Person", props2);

    let mut props3 = HashMap::new();
    props3.insert("email".to_string(), Value::from("carol@example.com"));
    graph.add_vertex("Person", props3);

    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (p:Person) WHERE p.email ENDS WITH '.com' RETURN p")
        .unwrap();

    assert_eq!(results.len(), 2, "Should find 2 .com emails");
}

#[test]
fn test_gql_where_in_list() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (p:Person) WHERE p.city IN ['NYC', 'LA'] RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 3, "Should find 3 people in NYC or LA");

    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Bob"));
    assert!(names.contains(&"Carol"));
}

#[test]
fn test_gql_where_not_in_list() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (p:Person) WHERE p.city NOT IN ['NYC', 'LA'] RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find only Dave in Chicago");
    assert_eq!(results[0], Value::String("Dave".to_string()));
}

#[test]
fn test_gql_where_is_null() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (p:Person) WHERE p.active IS NULL RETURN p.name")
        .unwrap();

    assert_eq!(
        results.len(),
        1,
        "Should find Dave who has no active property"
    );
    assert_eq!(results[0], Value::String("Dave".to_string()));
}

#[test]
fn test_gql_where_is_not_null() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (p:Person) WHERE p.active IS NOT NULL RETURN p.name")
        .unwrap();

    assert_eq!(
        results.len(),
        3,
        "Should find 3 people with active property"
    );
}

#[test]
fn test_gql_where_complex_predicate() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (p:Person) WHERE (p.age > 25 AND p.city = 'NYC') OR p.name = 'Bob' RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 3, "Should find Alice, Carol, and Bob");
}

#[test]
fn test_gql_where_age_range() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (p:Person) WHERE p.age >= 25 AND p.age <= 30 RETURN p.name")
        .unwrap();

    assert_eq!(
        results.len(),
        3,
        "Should find Bob (25), Dave (28), Alice (30)"
    );

    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Bob"));
    assert!(names.contains(&"Dave"));
}

#[test]
fn test_gql_where_with_traversal() {
    let graph = Arc::new(Graph::new());

    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    let alice = graph.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    bob_props.insert("age".to_string(), Value::from(25i64));
    let bob = graph.add_vertex("Person", bob_props);

    let mut carol_props = HashMap::new();
    carol_props.insert("name".to_string(), Value::from("Carol"));
    carol_props.insert("age".to_string(), Value::from(35i64));
    let carol = graph.add_vertex("Person", carol_props);

    graph.add_edge(alice, bob, "KNOWS", HashMap::new()).unwrap();
    graph
        .add_edge(alice, carol, "KNOWS", HashMap::new())
        .unwrap();

    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(friend) WHERE friend.age > 30 RETURN friend.name")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find only Carol");
    assert_eq!(results[0], Value::String("Carol".to_string()));
}

#[test]
fn test_gql_where_undefined_variable() {
    let graph = Arc::new(Graph::new());
    let snapshot = graph.snapshot();

    let result = graph.gql("MATCH (n:Person) WHERE x.age > 30 RETURN n");

    assert!(result.is_err());
    assert!(matches!(result, Err(GqlError::Compile(_))));
}
