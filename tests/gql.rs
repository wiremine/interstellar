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

// =============================================================================
// Property Access in RETURN Clause Tests (Phase 2.5)
// =============================================================================

#[test]
fn test_gql_return_single_property() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // RETURN n.name should return property values, not vertices
    let results = snapshot.gql("MATCH (n:Person) RETURN n.name").unwrap();

    assert_eq!(results.len(), 3, "Should find 3 Person vertices");

    // All results should be strings (property values)
    for result in &results {
        assert!(
            matches!(result, Value::String(_)),
            "Expected String, got {:?}",
            result
        );
    }

    // Check that the names are correct
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

    // RETURN n.age should return integer values
    let results = snapshot.gql("MATCH (n:Person) RETURN n.age").unwrap();

    assert_eq!(results.len(), 3, "Should find 3 Person vertices with age");

    // All results should be integers
    for result in &results {
        assert!(
            matches!(result, Value::Int(_)),
            "Expected Int, got {:?}",
            result
        );
    }

    // Check that the ages are correct
    let ages: Vec<i64> = results
        .iter()
        .filter_map(|v| match v {
            Value::Int(n) => Some(*n),
            _ => None,
        })
        .collect();
    assert!(ages.contains(&30)); // Alice
    assert!(ages.contains(&25)); // Bob
    assert!(ages.contains(&35)); // Charlie
}

#[test]
fn test_gql_return_multiple_properties() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // RETURN n.name, n.age should return maps with both properties
    let results = snapshot
        .gql("MATCH (n:Person {name: 'Alice'}) RETURN n.name, n.age")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find 1 Alice vertex");

    // Result should be a map
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

    // RETURN n.name AS personName should use the alias as the key
    let results = snapshot
        .gql("MATCH (n:Person {name: 'Alice'}) RETURN n.name AS personName")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find 1 Alice vertex");

    // Single item with alias still returns the value directly
    assert_eq!(results[0], Value::String("Alice".to_string()));
}

#[test]
fn test_gql_return_multiple_properties_with_aliases() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // RETURN n.name AS name, n.age AS years should use aliases as keys
    let results = snapshot
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

    // Company vertices don't have 'age' property
    let results = snapshot.gql("MATCH (c:Company) RETURN c.age").unwrap();

    // Missing properties should filter out the result
    assert_eq!(results.len(), 0, "No Company has age property");
}

#[test]
fn test_gql_return_property_from_traversal() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    // Get names of people Alice knows
    let results = snapshot
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
    let graph = Graph::in_memory();
    let snapshot = graph.snapshot();

    // Variable 'x' is not defined in MATCH clause
    let result = snapshot.gql("MATCH (n:Person) RETURN x.name");

    assert!(result.is_err());
    assert!(matches!(result, Err(GqlError::Compile(_))));
}

#[test]
fn test_gql_return_mixed_variable_and_property() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // Return both the vertex and a property
    let results = snapshot
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

            // 'n' should be a vertex
            assert!(matches!(map.get("n"), Some(Value::Vertex(_))));

            // 'n.name' should be the name string
            assert_eq!(map.get("n.name"), Some(&Value::String("Alice".to_string())));
        }
        other => panic!("Expected Map, got {:?}", other),
    }
}

// =============================================================================
// Phase 2.6: Integration Tests - Patterns
// =============================================================================

/// Helper to create a graph with rich edge relationships for pattern tests
fn create_pattern_test_graph() -> Graph {
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

    // Create edges:
    // Alice -[KNOWS]-> Bob
    // Alice -[KNOWS]-> Carol
    // Bob -[WORKS_WITH]-> Carol
    storage
        .add_edge(alice, bob, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, carol, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, carol, "WORKS_WITH", HashMap::new())
        .unwrap();

    Graph::new(Arc::new(storage))
}

/// Phase 2.6 test: Edge traversal patterns
///
/// Tests outgoing, incoming, and bidirectional edge traversals with
/// various label filters to ensure pattern matching works correctly.
#[test]
fn test_gql_edge_traversal_phase_2_6() {
    let graph = create_pattern_test_graph();
    let snapshot = graph.snapshot();

    // Test outgoing edge: Alice knows 2 people
    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(friend) RETURN friend")
        .unwrap();
    assert_eq!(
        results.len(),
        2,
        "Alice should know 2 people (Bob and Carol)"
    );

    // Test incoming edge: Bob is known by Alice
    let results = snapshot
        .gql("MATCH (b:Person {name: 'Bob'})<-[:KNOWS]-(source) RETURN source")
        .unwrap();
    assert_eq!(results.len(), 1, "Bob should be known by 1 person (Alice)");

    // Test bidirectional: Bob is connected via KNOWS to Alice (incoming)
    // Note: Bob has no outgoing KNOWS edges, so only incoming from Alice counts
    let results = snapshot
        .gql("MATCH (b:Person {name: 'Bob'})-[:KNOWS]-(connected) RETURN connected")
        .unwrap();
    assert_eq!(
        results.len(),
        1,
        "Bob should be connected to 1 person via KNOWS (Alice)"
    );
}

/// Helper to create a graph with age property for property return tests
fn create_property_return_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // Create Person vertices with name and age
    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    alice_props.insert("age".to_string(), Value::from(30i64));
    storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    bob_props.insert("age".to_string(), Value::from(25i64));
    storage.add_vertex("Person", bob_props);

    Graph::new(Arc::new(storage))
}

/// Phase 2.6 test: Property access in RETURN clause
///
/// Tests returning property values instead of entire vertices,
/// ensuring that the correct property values are extracted.
#[test]
fn test_gql_property_return_phase_2_6() {
    let graph = create_property_return_graph();
    let snapshot = graph.snapshot();

    // Return single property - should return property values, not vertices
    let results = snapshot.gql("MATCH (p:Person) RETURN p.name").unwrap();
    assert_eq!(results.len(), 2, "Should find 2 Person vertices");

    // Collect names and verify
    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        names.contains(&"Alice"),
        "Results should contain Alice, got: {:?}",
        names
    );
    assert!(
        names.contains(&"Bob"),
        "Results should contain Bob, got: {:?}",
        names
    );
}

/// Helper to create a graph for multi-hop traversal tests
fn create_multi_hop_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // Create a chain: Alice -> Bob -> Carol
    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    let alice = storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    let bob = storage.add_vertex("Person", bob_props);

    let mut carol_props = HashMap::new();
    carol_props.insert("name".to_string(), Value::from("Carol"));
    let carol = storage.add_vertex("Person", carol_props);

    // Alice -[KNOWS]-> Bob -[KNOWS]-> Carol
    storage
        .add_edge(alice, bob, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, carol, "KNOWS", HashMap::new())
        .unwrap();

    Graph::new(Arc::new(storage))
}

/// Phase 2.6 test: Multi-hop traversal
///
/// Tests traversing multiple edges in a single pattern,
/// returning the property value at the end of the chain.
#[test]
fn test_gql_multi_hop_phase_2_6() {
    let graph = create_multi_hop_graph();
    let snapshot = graph.snapshot();

    // Two-hop traversal: Alice -> Bob -> Carol
    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.name")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find exactly one path to Carol");
    assert_eq!(
        results[0],
        Value::String("Carol".to_string()),
        "Should find Carol at the end of two-hop traversal"
    );
}

/// Phase 2.6 test: Comprehensive edge traversal test
///
/// Tests the complete edge traversal scenario from the plan,
/// including property filters on starting nodes.
#[test]
fn test_gql_comprehensive_edge_traversal() {
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

    // Create KNOWS edges
    storage
        .add_edge(alice, bob, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, carol, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, carol, "WORKS_WITH", HashMap::new())
        .unwrap();

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // Test 1: Outgoing edge with property filter on source
    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(friend) RETURN friend")
        .unwrap();
    assert_eq!(results.len(), 2, "Alice knows 2 people");

    // Test 2: Incoming edge with property filter on target
    let results = snapshot
        .gql("MATCH (b:Person {name: 'Bob'})<-[:KNOWS]-(source) RETURN source")
        .unwrap();
    assert_eq!(results.len(), 1, "Bob is known by 1 person");

    // Test 3: Bidirectional - Bob connected via KNOWS (only Alice, since Bob has no outgoing KNOWS)
    let results = snapshot
        .gql("MATCH (b:Person {name: 'Bob'})-[:KNOWS]-(connected) RETURN connected")
        .unwrap();
    assert_eq!(
        results.len(),
        1,
        "Bob connected via KNOWS to 1 person (Alice)"
    );
}

/// Phase 2.6 test: Property return values verified against known data
///
/// Tests that property values match expected values in order or content.
#[test]
fn test_gql_property_return_values() {
    let mut storage = InMemoryGraph::new();

    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    alice_props.insert("age".to_string(), Value::from(30i64));
    storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    bob_props.insert("age".to_string(), Value::from(25i64));
    storage.add_vertex("Person", bob_props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // Return single property
    let results = snapshot.gql("MATCH (p:Person) RETURN p.name").unwrap();
    assert_eq!(results.len(), 2);

    // Verify both expected names are present
    assert!(results.contains(&Value::String("Alice".to_string())));
    assert!(results.contains(&Value::String("Bob".to_string())));
}

/// Phase 2.6 test: Multi-hop with property filter at each step
#[test]
fn test_gql_multi_hop_with_property_filters() {
    let mut storage = InMemoryGraph::new();

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

    // Alice -[KNOWS]-> Bob -[KNOWS]-> Carol
    // Alice -[KNOWS]-> Dave (no further connection)
    storage
        .add_edge(alice, bob, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, carol, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, dave, "KNOWS", HashMap::new())
        .unwrap();

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // Two-hop traversal from Alice via Bob to Carol
    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})-[:KNOWS]->(c) RETURN c.name")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find exactly one result");
    assert_eq!(
        results[0],
        Value::String("Carol".to_string()),
        "Should find Carol via the Bob path"
    );
}
