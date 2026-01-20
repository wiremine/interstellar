//! ORDER BY, LIMIT, and SKIP tests.
//!
//! Tests for result ordering and pagination:
//! - ORDER BY (ASC, DESC)
//! - LIMIT
//! - OFFSET and SKIP
//! - Combinations with WHERE

use interstellar::prelude::*;
use interstellar::storage::Graph;
use std::collections::HashMap;

/// Helper function to create a graph with people of various ages for ORDER BY tests
fn create_order_by_test_graph() -> Graph {
    let graph = Graph::new();

    let people = vec![
        ("Alice", 30i64),
        ("Bob", 25i64),
        ("Carol", 35i64),
        ("Dave", 28i64),
        ("Eve", 22i64),
    ];

    for (name, age) in people {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(name));
        props.insert("age".to_string(), Value::from(age));
        graph.add_vertex("Person", props);
    }

    graph
}

// =============================================================================
// ORDER BY Tests
// =============================================================================

#[test]
fn test_gql_order_by_ascending() {
    let graph = create_order_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.age ORDER BY p.age")
        .unwrap();

    assert_eq!(results.len(), 5);

    let ages: Vec<i64> = results
        .iter()
        .filter_map(|v| match v {
            Value::Int(n) => Some(*n),
            _ => None,
        })
        .collect();

    assert_eq!(
        ages,
        vec![22, 25, 28, 30, 35],
        "Ages should be in ascending order"
    );
}

#[test]
fn test_gql_order_by_descending() {
    let graph = create_order_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.age ORDER BY p.age DESC")
        .unwrap();

    assert_eq!(results.len(), 5);

    let ages: Vec<i64> = results
        .iter()
        .filter_map(|v| match v {
            Value::Int(n) => Some(*n),
            _ => None,
        })
        .collect();

    assert_eq!(
        ages,
        vec![35, 30, 28, 25, 22],
        "Ages should be in descending order"
    );
}

#[test]
fn test_gql_order_by_asc_explicit() {
    let graph = create_order_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.age ORDER BY p.age ASC")
        .unwrap();

    let ages: Vec<i64> = results
        .iter()
        .filter_map(|v| match v {
            Value::Int(n) => Some(*n),
            _ => None,
        })
        .collect();

    assert_eq!(
        ages,
        vec![22, 25, 28, 30, 35],
        "Ages should be in ascending order"
    );
}

#[test]
fn test_gql_order_by_string() {
    let graph = create_order_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.name ORDER BY p.name")
        .unwrap();

    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        names,
        vec!["Alice", "Bob", "Carol", "Dave", "Eve"],
        "Names should be in alphabetical order"
    );
}

#[test]
fn test_gql_order_by_with_where() {
    let graph = create_order_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.age > 25 RETURN p.age ORDER BY p.age")
        .unwrap();

    let ages: Vec<i64> = results
        .iter()
        .filter_map(|v| match v {
            Value::Int(n) => Some(*n),
            _ => None,
        })
        .collect();

    assert_eq!(
        ages,
        vec![28, 30, 35],
        "Should only include ages > 25, sorted"
    );
}

// =============================================================================
// LIMIT Tests
// =============================================================================

#[test]
fn test_gql_limit() {
    let graph = create_order_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.age ORDER BY p.age LIMIT 3")
        .unwrap();

    assert_eq!(results.len(), 3, "Should return only 3 results");

    let ages: Vec<i64> = results
        .iter()
        .filter_map(|v| match v {
            Value::Int(n) => Some(*n),
            _ => None,
        })
        .collect();

    assert_eq!(
        ages,
        vec![22, 25, 28],
        "Should return first 3 ages in order"
    );
}

#[test]
fn test_gql_limit_offset() {
    let graph = create_order_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.age ORDER BY p.age LIMIT 2 OFFSET 2")
        .unwrap();

    assert_eq!(results.len(), 2, "Should return 2 results after skipping 2");

    let ages: Vec<i64> = results
        .iter()
        .filter_map(|v| match v {
            Value::Int(n) => Some(*n),
            _ => None,
        })
        .collect();

    assert_eq!(ages, vec![28, 30], "Should return ages 28 and 30");
}

#[test]
fn test_gql_limit_larger_than_results() {
    let graph = create_order_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.age ORDER BY p.age LIMIT 100")
        .unwrap();

    assert_eq!(
        results.len(),
        5,
        "Should return all 5 results when LIMIT > count"
    );
}

#[test]
fn test_gql_offset_larger_than_results() {
    let graph = create_order_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.age ORDER BY p.age LIMIT 10 OFFSET 100")
        .unwrap();

    assert_eq!(results.len(), 0, "Should return empty when OFFSET > count");
}

#[test]
fn test_gql_limit_without_order() {
    let graph = create_order_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.name LIMIT 2")
        .unwrap();

    assert_eq!(results.len(), 2, "Should return 2 results");
}

#[test]
fn test_gql_order_limit_where() {
    let graph = create_order_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.age >= 25 RETURN p.age ORDER BY p.age DESC LIMIT 2")
        .unwrap();

    let ages: Vec<i64> = results
        .iter()
        .filter_map(|v| match v {
            Value::Int(n) => Some(*n),
            _ => None,
        })
        .collect();

    assert_eq!(ages, vec![35, 30], "Should return top 2 ages descending");
}

// =============================================================================
// SKIP Tests (alias for OFFSET)
// =============================================================================

#[test]
fn test_gql_limit_skip() {
    let graph = create_order_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.age ORDER BY p.age LIMIT 2 SKIP 2")
        .unwrap();

    assert_eq!(results.len(), 2, "Should return 2 results after skipping 2");

    let ages: Vec<i64> = results
        .iter()
        .filter_map(|v| match v {
            Value::Int(n) => Some(*n),
            _ => None,
        })
        .collect();

    assert_eq!(ages, vec![28, 30], "Should return ages 28 and 30");
}

#[test]
fn test_gql_skip_limit() {
    let graph = create_order_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.age ORDER BY p.age SKIP 2 LIMIT 2")
        .unwrap();

    assert_eq!(results.len(), 2, "Should return 2 results after skipping 2");

    let ages: Vec<i64> = results
        .iter()
        .filter_map(|v| match v {
            Value::Int(n) => Some(*n),
            _ => None,
        })
        .collect();

    assert_eq!(
        ages,
        vec![28, 30],
        "Should return ages 28 and 30 with SKIP first"
    );
}

#[test]
fn test_gql_skip_equals_offset() {
    let graph = create_order_by_test_graph();
    let snapshot = graph.snapshot();

    let offset_results = snapshot
        .gql("MATCH (p:Person) RETURN p.age ORDER BY p.age LIMIT 2 OFFSET 1")
        .unwrap();

    let skip_results = snapshot
        .gql("MATCH (p:Person) RETURN p.age ORDER BY p.age LIMIT 2 SKIP 1")
        .unwrap();

    assert_eq!(
        offset_results, skip_results,
        "SKIP and OFFSET should produce identical results"
    );
}

#[test]
fn test_gql_skip_larger_than_results() {
    let graph = create_order_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.age ORDER BY p.age LIMIT 10 SKIP 100")
        .unwrap();

    assert_eq!(results.len(), 0, "Should return empty when SKIP > count");
}
