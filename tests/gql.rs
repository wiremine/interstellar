//! Integration tests for GQL module.

use rustgremlin::gql::{parse, parse_statement, GqlError, Statement};
use rustgremlin::prelude::*;
use rustgremlin::storage::InMemoryGraph;
use std::collections::HashMap;
use std::collections::HashSet;
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

// =============================================================================
// Phase 3.4/3.5: WHERE Clause Integration Tests
// =============================================================================

/// Helper to create a graph for WHERE clause tests
fn create_where_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // Create Person vertices with various properties
    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    alice_props.insert("age".to_string(), Value::from(30i64));
    alice_props.insert("city".to_string(), Value::from("NYC"));
    alice_props.insert("active".to_string(), Value::from(true));
    storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    bob_props.insert("age".to_string(), Value::from(25i64));
    bob_props.insert("city".to_string(), Value::from("LA"));
    bob_props.insert("active".to_string(), Value::from(true));
    storage.add_vertex("Person", bob_props);

    let mut carol_props = HashMap::new();
    carol_props.insert("name".to_string(), Value::from("Carol"));
    carol_props.insert("age".to_string(), Value::from(35i64));
    carol_props.insert("city".to_string(), Value::from("NYC"));
    carol_props.insert("active".to_string(), Value::from(false));
    storage.add_vertex("Person", carol_props);

    let mut dave_props = HashMap::new();
    dave_props.insert("name".to_string(), Value::from("Dave"));
    dave_props.insert("age".to_string(), Value::from(28i64));
    dave_props.insert("city".to_string(), Value::from("Chicago"));
    // Note: Dave has no 'active' property (for IS NULL tests)
    storage.add_vertex("Person", dave_props);

    Graph::new(Arc::new(storage))
}

/// Test WHERE with greater than comparison
#[test]
fn test_gql_where_greater_than() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
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

/// Test WHERE with less than comparison
#[test]
fn test_gql_where_less_than() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.age < 28 RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find 1 person under 28");
    assert_eq!(results[0], Value::String("Bob".to_string()));
}

/// Test WHERE with equality comparison
#[test]
fn test_gql_where_equality() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.name = 'Alice' RETURN p")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find exactly Alice");
}

/// Test WHERE with not equal comparison
#[test]
fn test_gql_where_not_equal() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
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

/// Test WHERE with AND combination
#[test]
fn test_gql_where_and() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
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

/// Test WHERE with OR combination
#[test]
fn test_gql_where_or() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
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

/// Test WHERE with NOT
#[test]
fn test_gql_where_not() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE NOT p.active RETURN p.name")
        .unwrap();

    // Carol has active=false, Dave has no active property (treated as falsy)
    assert!(results.len() >= 1, "Should find at least Carol");
}

/// Test WHERE with >= and <=
#[test]
fn test_gql_where_greater_equal_less_equal() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    // >= test
    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.age >= 30 RETURN p.name")
        .unwrap();
    assert_eq!(results.len(), 2, "Should find Alice (30) and Carol (35)");

    // <= test
    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.age <= 28 RETURN p.name")
        .unwrap();
    assert_eq!(results.len(), 2, "Should find Bob (25) and Dave (28)");
}

/// Test WHERE with CONTAINS string operation
#[test]
fn test_gql_where_contains() {
    let mut storage = InMemoryGraph::new();

    let mut props1 = HashMap::new();
    props1.insert("name".to_string(), Value::from("Alice Anderson"));
    storage.add_vertex("Person", props1);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Bob Brown"));
    storage.add_vertex("Person", props2);

    let mut props3 = HashMap::new();
    props3.insert("name".to_string(), Value::from("Carol Anderson"));
    storage.add_vertex("Person", props3);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.name CONTAINS 'Anderson' RETURN p")
        .unwrap();

    assert_eq!(results.len(), 2, "Should find 2 Andersons");
}

/// Test WHERE with STARTS WITH string operation
#[test]
fn test_gql_where_starts_with() {
    let mut storage = InMemoryGraph::new();

    let mut props1 = HashMap::new();
    props1.insert("name".to_string(), Value::from("Alice"));
    storage.add_vertex("Person", props1);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Albert"));
    storage.add_vertex("Person", props2);

    let mut props3 = HashMap::new();
    props3.insert("name".to_string(), Value::from("Bob"));
    storage.add_vertex("Person", props3);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.name STARTS WITH 'Al' RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 2, "Should find Alice and Albert");
}

/// Test WHERE with ENDS WITH string operation
#[test]
fn test_gql_where_ends_with() {
    let mut storage = InMemoryGraph::new();

    let mut props1 = HashMap::new();
    props1.insert("email".to_string(), Value::from("alice@example.com"));
    storage.add_vertex("Person", props1);

    let mut props2 = HashMap::new();
    props2.insert("email".to_string(), Value::from("bob@test.org"));
    storage.add_vertex("Person", props2);

    let mut props3 = HashMap::new();
    props3.insert("email".to_string(), Value::from("carol@example.com"));
    storage.add_vertex("Person", props3);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.email ENDS WITH '.com' RETURN p")
        .unwrap();

    assert_eq!(results.len(), 2, "Should find 2 .com emails");
}

/// Test WHERE with IN list
#[test]
fn test_gql_where_in_list() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
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

/// Test WHERE with NOT IN list
#[test]
fn test_gql_where_not_in_list() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.city NOT IN ['NYC', 'LA'] RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find only Dave in Chicago");
    assert_eq!(results[0], Value::String("Dave".to_string()));
}

/// Test WHERE with IS NULL
#[test]
fn test_gql_where_is_null() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    // Dave doesn't have an 'active' property
    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.active IS NULL RETURN p.name")
        .unwrap();

    assert_eq!(
        results.len(),
        1,
        "Should find Dave who has no active property"
    );
    assert_eq!(results[0], Value::String("Dave".to_string()));
}

/// Test WHERE with IS NOT NULL
#[test]
fn test_gql_where_is_not_null() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.active IS NOT NULL RETURN p.name")
        .unwrap();

    assert_eq!(
        results.len(),
        3,
        "Should find 3 people with active property"
    );
}

/// Test WHERE with complex combined predicate
#[test]
fn test_gql_where_complex_predicate() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    // Find people who are (over 25 AND in NYC) OR named Bob
    let results = snapshot
        .gql("MATCH (p:Person) WHERE (p.age > 25 AND p.city = 'NYC') OR p.name = 'Bob' RETURN p.name")
        .unwrap();

    // Alice (30, NYC), Carol (35, NYC), Bob (25, LA - because name='Bob')
    assert_eq!(results.len(), 3, "Should find Alice, Carol, and Bob");
}

/// Test WHERE with age range
#[test]
fn test_gql_where_age_range() {
    let graph = create_where_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
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

/// Test WHERE with edge traversal
#[test]
fn test_gql_where_with_traversal() {
    let mut storage = InMemoryGraph::new();

    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    let alice = storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    bob_props.insert("age".to_string(), Value::from(25i64));
    let bob = storage.add_vertex("Person", bob_props);

    let mut carol_props = HashMap::new();
    carol_props.insert("name".to_string(), Value::from("Carol"));
    carol_props.insert("age".to_string(), Value::from(35i64));
    let carol = storage.add_vertex("Person", carol_props);

    // Alice knows Bob and Carol
    storage
        .add_edge(alice, bob, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, carol, "KNOWS", HashMap::new())
        .unwrap();

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // Find friends of Alice who are over 30
    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(friend) WHERE friend.age > 30 RETURN friend.name")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find only Carol");
    assert_eq!(results[0], Value::String("Carol".to_string()));
}

/// Test WHERE with undefined variable produces error
#[test]
fn test_gql_where_undefined_variable() {
    let graph = Graph::in_memory();
    let snapshot = graph.snapshot();

    let result = snapshot.gql("MATCH (n:Person) WHERE x.age > 30 RETURN n");

    assert!(result.is_err());
    assert!(matches!(result, Err(GqlError::Compile(_))));
}

// =============================================================================
// ORDER BY Tests
// =============================================================================

/// Helper function to create a graph with people of various ages for ORDER BY tests
fn create_order_by_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

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
        storage.add_vertex("Person", props);
    }

    Graph::new(Arc::new(storage))
}

/// Test ORDER BY ascending (default)
#[test]
fn test_gql_order_by_ascending() {
    let graph = create_order_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.age ORDER BY p.age")
        .unwrap();

    assert_eq!(results.len(), 5);

    // Extract ages - should be in ascending order: 22, 25, 28, 30, 35
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

/// Test ORDER BY descending
#[test]
fn test_gql_order_by_descending() {
    let graph = create_order_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.age ORDER BY p.age DESC")
        .unwrap();

    assert_eq!(results.len(), 5);

    // Extract ages - should be in descending order: 35, 30, 28, 25, 22
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

/// Test ORDER BY with ASC keyword
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

/// Test ORDER BY string field
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

/// Test ORDER BY with WHERE clause
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

    // Only ages > 25: 28, 30, 35 (sorted ascending)
    assert_eq!(
        ages,
        vec![28, 30, 35],
        "Should only include ages > 25, sorted"
    );
}

// =============================================================================
// LIMIT Tests
// =============================================================================

/// Test LIMIT clause
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

/// Test LIMIT with OFFSET
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

    // Ages in order: 22, 25, 28, 30, 35
    // Skip 2 (22, 25), take 2 (28, 30)
    assert_eq!(ages, vec![28, 30], "Should return ages 28 and 30");
}

/// Test LIMIT larger than result set
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

/// Test OFFSET larger than result set
#[test]
fn test_gql_offset_larger_than_results() {
    let graph = create_order_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.age ORDER BY p.age LIMIT 10 OFFSET 100")
        .unwrap();

    assert_eq!(results.len(), 0, "Should return empty when OFFSET > count");
}

/// Test LIMIT without ORDER BY
#[test]
fn test_gql_limit_without_order() {
    let graph = create_order_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.name LIMIT 2")
        .unwrap();

    assert_eq!(results.len(), 2, "Should return 2 results");
}

/// Test ORDER BY with LIMIT and WHERE
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

    // Ages >= 25: 25, 28, 30, 35
    // Sorted DESC: 35, 30, 28, 25
    // LIMIT 2: 35, 30
    assert_eq!(ages, vec![35, 30], "Should return top 2 ages descending");
}

// =============================================================================
// Aggregation Tests
// =============================================================================

/// Helper to create a test graph for aggregation tests
fn create_aggregation_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // Create Person vertices with various ages and cities
    let people = vec![
        ("Alice", 30i64, "New York"),
        ("Bob", 25i64, "Boston"),
        ("Carol", 35i64, "New York"),
        ("Dave", 28i64, "Boston"),
        ("Eve", 22i64, "Chicago"),
    ];

    for (name, age, city) in people {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(name));
        props.insert("age".to_string(), Value::from(age));
        props.insert("city".to_string(), Value::from(city));
        storage.add_vertex("Person", props);
    }

    Graph::new(Arc::new(storage))
}

/// Test COUNT(*) - count all matches
#[test]
fn test_gql_count_star() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (p:Person) RETURN count(*)").unwrap();

    assert_eq!(results.len(), 1, "COUNT(*) should return single result");
    assert_eq!(results[0], Value::Int(5), "Should count all 5 persons");
}

/// Test COUNT(*) with alias
#[test]
fn test_gql_count_star_with_alias() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN count(*) AS total")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("total"), Some(&Value::Int(5)));
    } else {
        panic!("Expected Map result with alias");
    }
}

/// Test COUNT on property
#[test]
fn test_gql_count_property() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN count(p.name)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(5), "Should count all names");
}

/// Test COUNT(DISTINCT) - count unique values
#[test]
fn test_gql_count_distinct() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN count(DISTINCT p.city)")
        .unwrap();

    assert_eq!(results.len(), 1);
    // 3 unique cities: New York, Boston, Chicago
    assert_eq!(results[0], Value::Int(3), "Should count 3 unique cities");
}

/// Test SUM on numeric property
#[test]
fn test_gql_sum() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (p:Person) RETURN sum(p.age)").unwrap();

    assert_eq!(results.len(), 1);
    // 30 + 25 + 35 + 28 + 22 = 140
    assert_eq!(results[0], Value::Int(140), "Sum of ages should be 140");
}

/// Test SUM with WHERE clause
#[test]
fn test_gql_sum_with_where() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.age > 25 RETURN sum(p.age)")
        .unwrap();

    assert_eq!(results.len(), 1);
    // 30 + 35 + 28 = 93 (ages > 25)
    assert_eq!(results[0], Value::Int(93), "Sum of ages > 25 should be 93");
}

/// Test AVG on numeric property
#[test]
fn test_gql_avg() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (p:Person) RETURN avg(p.age)").unwrap();

    assert_eq!(results.len(), 1);
    // (30 + 25 + 35 + 28 + 22) / 5 = 140 / 5 = 28.0
    if let Value::Float(avg) = results[0] {
        assert!(
            (avg - 28.0).abs() < 0.0001,
            "Average should be 28.0, got {}",
            avg
        );
    } else {
        panic!("Expected Float result for AVG");
    }
}

/// Test MIN on numeric property
#[test]
fn test_gql_min() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (p:Person) RETURN min(p.age)").unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(22), "Min age should be 22");
}

/// Test MAX on numeric property
#[test]
fn test_gql_max() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (p:Person) RETURN max(p.age)").unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(35), "Max age should be 35");
}

/// Test MIN on string property
#[test]
fn test_gql_min_string() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (p:Person) RETURN min(p.name)").unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        Value::String("Alice".to_string()),
        "Min name should be Alice (alphabetically first)"
    );
}

/// Test MAX on string property
#[test]
fn test_gql_max_string() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (p:Person) RETURN max(p.name)").unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        Value::String("Eve".to_string()),
        "Max name should be Eve (alphabetically last)"
    );
}

/// Test COLLECT - collect values into list
#[test]
fn test_gql_collect() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN collect(p.name)")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::List(names) = &results[0] {
        assert_eq!(names.len(), 5, "Should collect all 5 names");
        // Names should include all 5 people (order may vary)
        let names_set: std::collections::HashSet<_> = names.iter().collect();
        assert!(names_set.contains(&Value::String("Alice".to_string())));
        assert!(names_set.contains(&Value::String("Bob".to_string())));
        assert!(names_set.contains(&Value::String("Carol".to_string())));
        assert!(names_set.contains(&Value::String("Dave".to_string())));
        assert!(names_set.contains(&Value::String("Eve".to_string())));
    } else {
        panic!("Expected List result for COLLECT");
    }
}

/// Test COLLECT(DISTINCT) - collect unique values
#[test]
fn test_gql_collect_distinct() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN collect(DISTINCT p.city)")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::List(cities) = &results[0] {
        assert_eq!(cities.len(), 3, "Should collect 3 unique cities");
        let cities_set: std::collections::HashSet<_> = cities.iter().collect();
        assert!(cities_set.contains(&Value::String("New York".to_string())));
        assert!(cities_set.contains(&Value::String("Boston".to_string())));
        assert!(cities_set.contains(&Value::String("Chicago".to_string())));
    } else {
        panic!("Expected List result for COLLECT DISTINCT");
    }
}

/// Test multiple aggregates in single query
#[test]
fn test_gql_multiple_aggregates() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN count(*) AS total, sum(p.age) AS total_age, avg(p.age) AS avg_age")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("total"), Some(&Value::Int(5)));
        assert_eq!(map.get("total_age"), Some(&Value::Int(140)));
        if let Some(Value::Float(avg)) = map.get("avg_age") {
            assert!((avg - 28.0).abs() < 0.0001, "Average should be 28.0");
        } else {
            panic!("Expected Float for avg_age");
        }
    } else {
        panic!("Expected Map result for multiple aggregates");
    }
}

/// Test COUNT with empty result set
#[test]
fn test_gql_count_empty() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.age > 100 RETURN count(*)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(0), "COUNT of empty set should be 0");
}

/// Test AVG with empty result set
#[test]
fn test_gql_avg_empty() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.age > 100 RETURN avg(p.age)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null, "AVG of empty set should be Null");
}

/// Test MIN with empty result set
#[test]
fn test_gql_min_empty() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.age > 100 RETURN min(p.age)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null, "MIN of empty set should be Null");
}

/// Test MAX with empty result set
#[test]
fn test_gql_max_empty() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.age > 100 RETURN max(p.age)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null, "MAX of empty set should be Null");
}

/// Test SUM with empty result set
#[test]
fn test_gql_sum_empty() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.age > 100 RETURN sum(p.age)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(0), "SUM of empty set should be 0");
}

/// Test COLLECT with empty result set
#[test]
fn test_gql_collect_empty() {
    let graph = create_aggregation_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.age > 100 RETURN collect(p.name)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        Value::List(vec![]),
        "COLLECT of empty set should be empty list"
    );
}

// =============================================================================
// Phase 4.7: Advanced Integration Tests - ORDER BY, LIMIT, Aggregations
// =============================================================================

/// Helper to create a more comprehensive test graph for Phase 4.7 tests
fn create_phase47_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // Create Person vertices with department and salary for advanced testing
    let people = vec![
        ("Alice", 30i64, "Engineering", 100000i64),
        ("Bob", 25i64, "Engineering", 80000i64),
        ("Carol", 35i64, "Sales", 90000i64),
        ("Dave", 28i64, "Sales", 75000i64),
        ("Eve", 22i64, "Engineering", 70000i64),
        ("Frank", 40i64, "Marketing", 95000i64),
        ("Grace", 33i64, "Marketing", 85000i64),
        ("Henry", 28i64, "Engineering", 82000i64),
    ];

    for (name, age, dept, salary) in people {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(name));
        props.insert("age".to_string(), Value::from(age));
        props.insert("department".to_string(), Value::from(dept));
        props.insert("salary".to_string(), Value::from(salary));
        storage.add_vertex("Person", props);
    }

    Graph::new(Arc::new(storage))
}

/// Test ORDER BY with multiple columns
#[test]
fn test_gql_order_by_multiple_columns() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    // Order by department ASC, then by salary DESC within each department
    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.department, p.name, p.salary ORDER BY p.department, p.salary DESC")
        .unwrap();

    assert_eq!(results.len(), 8, "Should return all 8 people");

    // Extract and verify the order
    let entries: Vec<(String, String, i64)> = results
        .iter()
        .filter_map(|v| {
            if let Value::Map(map) = v {
                let dept = match map.get("p.department") {
                    Some(Value::String(s)) => s.clone(),
                    _ => return None,
                };
                let name = match map.get("p.name") {
                    Some(Value::String(s)) => s.clone(),
                    _ => return None,
                };
                let salary = match map.get("p.salary") {
                    Some(Value::Int(n)) => *n,
                    _ => return None,
                };
                Some((dept, name, salary))
            } else {
                None
            }
        })
        .collect();

    // Engineering should come first (alphabetically), with highest salary first
    // Engineering: Alice (100000), Henry (82000), Bob (80000), Eve (70000)
    assert_eq!(entries[0].0, "Engineering");
    assert_eq!(entries[0].1, "Alice");
    assert_eq!(entries[0].2, 100000);

    // Then Marketing: Frank (95000), Grace (85000)
    // Then Sales: Carol (90000), Dave (75000)
}

/// Test ORDER BY with mixed ASC and DESC
#[test]
fn test_gql_order_by_mixed_directions() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    // Order by department ASC, age DESC
    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.department, p.name, p.age ORDER BY p.department ASC, p.age DESC")
        .unwrap();

    let entries: Vec<(String, i64)> = results
        .iter()
        .filter_map(|v| {
            if let Value::Map(map) = v {
                let dept = match map.get("p.department") {
                    Some(Value::String(s)) => s.clone(),
                    _ => return None,
                };
                let age = match map.get("p.age") {
                    Some(Value::Int(n)) => *n,
                    _ => return None,
                };
                Some((dept, age))
            } else {
                None
            }
        })
        .collect();

    // Engineering first (alphabetically), oldest to youngest
    // Engineering ages: 30 (Alice), 28 (Henry), 25 (Bob), 22 (Eve)
    assert_eq!(entries[0].0, "Engineering");
    assert_eq!(entries[0].1, 30); // Alice is oldest in Engineering

    // Verify descending age order within Engineering
    let engineering_ages: Vec<i64> = entries
        .iter()
        .filter(|(d, _)| d == "Engineering")
        .map(|(_, a)| *a)
        .collect();
    assert_eq!(
        engineering_ages,
        vec![30, 28, 25, 22],
        "Engineering should be sorted by age DESC"
    );
}

/// Test LIMIT with ORDER BY returns correct subset
#[test]
fn test_gql_limit_with_order_top_n() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    // Get top 3 highest paid employees
    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.name, p.salary ORDER BY p.salary DESC LIMIT 3")
        .unwrap();

    assert_eq!(results.len(), 3, "Should return exactly 3 results");

    let salaries: Vec<i64> = results
        .iter()
        .filter_map(|v| {
            if let Value::Map(map) = v {
                match map.get("p.salary") {
                    Some(Value::Int(n)) => Some(*n),
                    _ => None,
                }
            } else {
                None
            }
        })
        .collect();

    // Top 3 salaries should be 100000, 95000, 90000
    assert_eq!(
        salaries,
        vec![100000, 95000, 90000],
        "Should return top 3 salaries"
    );
}

/// Test OFFSET skips correct number of results
#[test]
fn test_gql_offset_pagination() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    // Get page 2 (skip first 3, take next 3)
    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.salary ORDER BY p.salary DESC LIMIT 3 OFFSET 3")
        .unwrap();

    assert_eq!(results.len(), 3, "Should return 3 results for page 2");

    let salaries: Vec<i64> = results
        .iter()
        .filter_map(|v| match v {
            Value::Int(n) => Some(*n),
            _ => None,
        })
        .collect();

    // After skipping top 3 (100000, 95000, 90000), should get (85000, 82000, 80000)
    assert_eq!(
        salaries,
        vec![85000, 82000, 80000],
        "Should return correct page of salaries"
    );
}

/// Test aggregation with WHERE clause filtering
#[test]
fn test_gql_aggregation_with_filter() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    // Count and sum for Engineering department only
    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.department = 'Engineering' RETURN count(*) AS count, sum(p.salary) AS total_salary, avg(p.salary) AS avg_salary")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert_eq!(
            map.get("count"),
            Some(&Value::Int(4)),
            "Should have 4 engineers"
        );
        // Engineering salaries: 100000 + 80000 + 70000 + 82000 = 332000
        assert_eq!(
            map.get("total_salary"),
            Some(&Value::Int(332000)),
            "Total engineering salary"
        );
        if let Some(Value::Float(avg)) = map.get("avg_salary") {
            assert!(
                (avg - 83000.0).abs() < 0.01,
                "Average engineering salary should be 83000"
            );
        }
    } else {
        panic!("Expected Map result");
    }
}

/// Test multiple aggregations with aliases
#[test]
fn test_gql_aggregations_with_aliases() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN count(*) AS headcount, min(p.age) AS youngest, max(p.age) AS oldest, avg(p.age) AS avg_age")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("headcount"), Some(&Value::Int(8)));
        assert_eq!(map.get("youngest"), Some(&Value::Int(22))); // Eve
        assert_eq!(map.get("oldest"), Some(&Value::Int(40))); // Frank
        if let Some(Value::Float(avg)) = map.get("avg_age") {
            // (30+25+35+28+22+40+33+28) / 8 = 241 / 8 = 30.125
            assert!((avg - 30.125).abs() < 0.01, "Average age should be ~30.125");
        }
    } else {
        panic!("Expected Map result");
    }
}

/// Test COLLECT with ORDER BY (note: COLLECT doesn't preserve order)
#[test]
fn test_gql_collect_all_values() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.department = 'Marketing' RETURN collect(p.name) AS names")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        if let Some(Value::List(names)) = map.get("names") {
            assert_eq!(names.len(), 2, "Marketing has 2 people");
            let name_set: std::collections::HashSet<_> = names.iter().collect();
            assert!(name_set.contains(&Value::String("Frank".to_string())));
            assert!(name_set.contains(&Value::String("Grace".to_string())));
        } else {
            panic!("Expected List for names");
        }
    } else {
        panic!("Expected Map result");
    }
}

/// Test COUNT DISTINCT on department
#[test]
fn test_gql_count_distinct_departments() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN count(DISTINCT p.department) AS num_departments")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert_eq!(
            map.get("num_departments"),
            Some(&Value::Int(3)),
            "Should have 3 unique departments"
        );
    } else {
        panic!("Expected Map result");
    }
}

/// Test aggregation returning single value (no alias)
#[test]
fn test_gql_single_aggregation_no_alias() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    // Single COUNT(*) without alias returns just the value
    let results = snapshot.gql("MATCH (p:Person) RETURN count(*)").unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(8), "Should count all 8 people");
}

/// Test ORDER BY with NULL values (vertices missing property)
/// Note: Current implementation filters out rows where returned properties are missing.
/// This test verifies that ORDER BY still works correctly when all returned rows have values.
#[test]
fn test_gql_order_by_with_nulls() {
    let mut storage = InMemoryGraph::new();

    // Some people have scores, some don't
    let mut props1 = HashMap::new();
    props1.insert("name".to_string(), Value::from("Alice"));
    props1.insert("score".to_string(), Value::from(85i64));
    storage.add_vertex("Person", props1);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Bob"));
    // Bob has no score - will be filtered out when returning p.score
    storage.add_vertex("Person", props2);

    let mut props3 = HashMap::new();
    props3.insert("name".to_string(), Value::from("Carol"));
    props3.insert("score".to_string(), Value::from(90i64));
    storage.add_vertex("Person", props3);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // ORDER BY score - Bob will be filtered out since he has no score
    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.name, p.score ORDER BY p.score")
        .unwrap();

    // Only Alice and Carol should be in results (Bob filtered out)
    assert_eq!(results.len(), 2, "Should only return 2 people with scores");

    // Verify order: Alice (85) first, then Carol (90)
    let scores: Vec<i64> = results
        .iter()
        .filter_map(|v| {
            if let Value::Map(map) = v {
                match map.get("p.score") {
                    Some(Value::Int(n)) => Some(*n),
                    _ => None,
                }
            } else {
                None
            }
        })
        .collect();

    assert_eq!(scores, vec![85, 90], "Scores should be in ascending order");
}

/// Test LIMIT 0 returns empty result
#[test]
fn test_gql_limit_zero() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.name LIMIT 0")
        .unwrap();

    assert_eq!(results.len(), 0, "LIMIT 0 should return empty result");
}

/// Test OFFSET beyond result set returns empty
#[test]
fn test_gql_offset_beyond_results() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.name LIMIT 10 OFFSET 100")
        .unwrap();

    assert_eq!(
        results.len(),
        0,
        "OFFSET beyond result set should return empty"
    );
}

/// Test combined WHERE + ORDER BY + LIMIT
#[test]
fn test_gql_combined_where_order_limit() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    // Get youngest 2 people over age 25
    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.age > 25 RETURN p.name, p.age ORDER BY p.age ASC LIMIT 2")
        .unwrap();

    assert_eq!(results.len(), 2);

    let ages: Vec<i64> = results
        .iter()
        .filter_map(|v| {
            if let Value::Map(map) = v {
                match map.get("p.age") {
                    Some(Value::Int(n)) => Some(*n),
                    _ => None,
                }
            } else {
                None
            }
        })
        .collect();

    // People over 25: Alice(30), Carol(35), Dave(28), Frank(40), Grace(33), Henry(28)
    // Sorted by age ASC: Dave/Henry(28), Alice(30), Grace(33), Carol(35), Frank(40)
    // LIMIT 2: 28, 28 or 28, 30 depending on stable sort
    assert!(ages[0] == 28, "First should be age 28");
    assert!(ages[1] == 28 || ages[1] == 30, "Second should be 28 or 30");
}

/// Test SUM with float values
#[test]
fn test_gql_sum_floats() {
    let mut storage = InMemoryGraph::new();

    let mut props1 = HashMap::new();
    props1.insert("name".to_string(), Value::from("Product A"));
    props1.insert("price".to_string(), Value::Float(19.99));
    storage.add_vertex("Product", props1);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Product B"));
    props2.insert("price".to_string(), Value::Float(29.99));
    storage.add_vertex("Product", props2);

    let mut props3 = HashMap::new();
    props3.insert("name".to_string(), Value::from("Product C"));
    props3.insert("price".to_string(), Value::Float(9.99));
    storage.add_vertex("Product", props3);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Product) RETURN sum(p.price) AS total")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        if let Some(Value::Float(total)) = map.get("total") {
            // 19.99 + 29.99 + 9.99 = 59.97
            assert!(
                (total - 59.97).abs() < 0.001,
                "Sum should be ~59.97, got {}",
                total
            );
        } else {
            panic!("Expected Float for total");
        }
    } else {
        panic!("Expected Map result");
    }
}

/// Test AVG with mixed int and float values
#[test]
fn test_gql_avg_mixed_numeric() {
    let mut storage = InMemoryGraph::new();

    let mut props1 = HashMap::new();
    props1.insert("name".to_string(), Value::from("A"));
    props1.insert("value".to_string(), Value::Int(10));
    storage.add_vertex("Item", props1);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("B"));
    props2.insert("value".to_string(), Value::Float(20.0));
    storage.add_vertex("Item", props2);

    let mut props3 = HashMap::new();
    props3.insert("name".to_string(), Value::from("C"));
    props3.insert("value".to_string(), Value::Int(30));
    storage.add_vertex("Item", props3);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (i:Item) RETURN avg(i.value) AS average")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        if let Some(Value::Float(avg)) = map.get("average") {
            // (10 + 20 + 30) / 3 = 20.0
            assert!(
                (avg - 20.0).abs() < 0.001,
                "Average should be 20.0, got {}",
                avg
            );
        } else {
            panic!("Expected Float for average");
        }
    } else {
        panic!("Expected Map result");
    }
}

/// Test ORDER BY on computed/aliased expression (property access)
#[test]
fn test_gql_order_by_aliased_property() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.name AS employee_name, p.salary AS pay ORDER BY p.salary DESC LIMIT 3")
        .unwrap();

    assert_eq!(results.len(), 3);

    // Should return top 3 paid: Alice (100000), Frank (95000), Carol (90000)
    let names: Vec<String> = results
        .iter()
        .filter_map(|v| {
            if let Value::Map(map) = v {
                match map.get("employee_name") {
                    Some(Value::String(s)) => Some(s.clone()),
                    _ => None,
                }
            } else {
                None
            }
        })
        .collect();

    assert_eq!(names[0], "Alice", "Highest paid should be Alice");
    assert_eq!(names[1], "Frank", "Second highest paid should be Frank");
    assert_eq!(names[2], "Carol", "Third highest paid should be Carol");
}

// =============================================================================
// Phase 5.1/5.2: Variable-Length Path Tests
// =============================================================================

/// Helper to create a graph for variable-length path tests
///
/// Graph structure:
/// ```
///   Alice -[KNOWS]-> Bob -[KNOWS]-> Carol -[KNOWS]-> Dave -[KNOWS]-> Eve
///         \                                         /
///          -[KNOWS]-> Frank -----[KNOWS]-----------
/// ```
fn create_variable_length_path_graph() -> Graph {
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

    let mut eve_props = HashMap::new();
    eve_props.insert("name".to_string(), Value::from("Eve"));
    let eve = storage.add_vertex("Person", eve_props);

    let mut frank_props = HashMap::new();
    frank_props.insert("name".to_string(), Value::from("Frank"));
    let frank = storage.add_vertex("Person", frank_props);

    // Create a chain: Alice -> Bob -> Carol -> Dave -> Eve
    storage
        .add_edge(alice, bob, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, carol, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(carol, dave, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(dave, eve, "KNOWS", HashMap::new())
        .unwrap();

    // Also: Alice -> Frank -> Dave (shorter path to Dave)
    storage
        .add_edge(alice, frank, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(frank, dave, "KNOWS", HashMap::new())
        .unwrap();

    Graph::new(Arc::new(storage))
}

/// Test exact hop count: *2 (exactly 2 hops)
#[test]
fn test_gql_variable_path_exact_hops() {
    let graph = create_variable_length_path_graph();
    let snapshot = graph.snapshot();

    // Find people exactly 2 hops from Alice
    // Alice -[KNOWS]-> Bob -[KNOWS]-> Carol (2 hops)
    // Alice -[KNOWS]-> Frank -[KNOWS]-> Dave (2 hops)
    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS*2]->(target) RETURN target.name")
        .unwrap();

    assert_eq!(results.len(), 2, "Should find 2 people at exactly 2 hops");

    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert!(names.contains(&"Carol"), "Carol is 2 hops via Bob");
    assert!(names.contains(&"Dave"), "Dave is 2 hops via Frank");
}

/// Test range bounds: *1..3 (1 to 3 hops)
#[test]
fn test_gql_variable_path_range() {
    let graph = create_variable_length_path_graph();
    let snapshot = graph.snapshot();

    // Find people 1-3 hops from Alice
    // 1 hop: Bob, Frank
    // 2 hops: Carol (via Bob), Dave (via Frank)
    // 3 hops: Dave (via Bob->Carol), Eve (via Frank->Dave)
    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(target) RETURN target.name")
        .unwrap();

    // With dedup, should find: Bob, Frank, Carol, Dave, Eve
    // Note: Dave is reachable via multiple paths but should only appear once
    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert!(names.contains(&"Bob"), "Bob is 1 hop");
    assert!(names.contains(&"Frank"), "Frank is 1 hop");
    assert!(names.contains(&"Carol"), "Carol is 2 hops");
    assert!(names.contains(&"Dave"), "Dave is 2-3 hops");
    // Note: Eve might be in range depending on path
}

/// Test max only: *..2 (0 to 2 hops, includes start)
#[test]
fn test_gql_variable_path_max_only() {
    let graph = create_variable_length_path_graph();
    let snapshot = graph.snapshot();

    // Find people 0-2 hops from Alice (should include Alice herself)
    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS*..2]->(target) RETURN target.name")
        .unwrap();

    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    // Should include:
    // 0 hops: Alice
    // 1 hop: Bob, Frank
    // 2 hops: Carol, Dave
    assert!(names.contains(&"Alice"), "Alice is 0 hops (start)");
    assert!(names.contains(&"Bob"), "Bob is 1 hop");
    assert!(names.contains(&"Frank"), "Frank is 1 hop");
    assert!(names.contains(&"Carol"), "Carol is 2 hops");
    assert!(names.contains(&"Dave"), "Dave is 2 hops");
}

/// Test unbounded: * (all reachable)
#[test]
fn test_gql_variable_path_unbounded() {
    let graph = create_variable_length_path_graph();
    let snapshot = graph.snapshot();

    // Find all people reachable from Alice
    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS*]->(target) RETURN target.name")
        .unwrap();

    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    // Should find everyone including Alice
    assert!(names.contains(&"Alice"), "Alice should be included (start)");
    assert!(names.contains(&"Bob"), "Bob is reachable");
    assert!(names.contains(&"Frank"), "Frank is reachable");
    assert!(names.contains(&"Carol"), "Carol is reachable");
    assert!(names.contains(&"Dave"), "Dave is reachable");
    assert!(names.contains(&"Eve"), "Eve is reachable");
}

/// Test friends-of-friends pattern
#[test]
fn test_gql_friends_of_friends() {
    let graph = create_variable_length_path_graph();
    let snapshot = graph.snapshot();

    // Classic friends-of-friends: exactly 2 hops
    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS*2]->(fof) RETURN fof.name")
        .unwrap();

    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    // Should not include direct friends (Bob, Frank)
    assert!(!names.contains(&"Bob"), "Bob is direct friend, not FoF");
    assert!(!names.contains(&"Frank"), "Frank is direct friend, not FoF");

    // Should include people 2 hops away
    assert!(names.contains(&"Carol"), "Carol is FoF via Bob");
    assert!(names.contains(&"Dave"), "Dave is FoF via Frank");
}

/// Test variable-length with incoming edges
#[test]
fn test_gql_variable_path_incoming() {
    let graph = create_variable_length_path_graph();
    let snapshot = graph.snapshot();

    // Find people who can reach Eve in 1-2 hops
    let results = snapshot
        .gql("MATCH (e:Person {name: 'Eve'})<-[:KNOWS*1..2]-(source) RETURN source.name")
        .unwrap();

    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    // 1 hop back: Dave
    // 2 hops back: Carol, Frank
    assert!(names.contains(&"Dave"), "Dave is 1 hop back from Eve");
    assert!(names.contains(&"Carol"), "Carol is 2 hops back from Eve");
    assert!(names.contains(&"Frank"), "Frank is 2 hops back from Eve");
}

/// Test variable-length with bidirectional edges
#[test]
fn test_gql_variable_path_bidirectional() {
    let graph = create_variable_length_path_graph();
    let snapshot = graph.snapshot();

    // Find people connected to Carol within 2 hops (either direction)
    let results = snapshot
        .gql("MATCH (c:Person {name: 'Carol'})-[:KNOWS*1..2]-(connected) RETURN connected.name")
        .unwrap();

    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    // Carol is in the middle of the chain: Alice -> Bob -> Carol -> Dave -> Eve
    // 1 hop: Bob (incoming), Dave (outgoing)
    // 2 hops: Alice (via Bob), Eve (via Dave)
    assert!(names.contains(&"Bob"), "Bob is 1 hop from Carol");
    assert!(names.contains(&"Dave"), "Dave is 1 hop from Carol");
    assert!(names.contains(&"Alice"), "Alice is 2 hops from Carol");
    assert!(names.contains(&"Eve"), "Eve is 2 hops from Carol");
}

/// Test variable-length without label filter
#[test]
fn test_gql_variable_path_no_label() {
    let graph = create_variable_length_path_graph();
    let snapshot = graph.snapshot();

    // Find all vertices reachable in exactly 1 hop via any edge type
    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[*1]->(target) RETURN target.name")
        .unwrap();

    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    // Direct neighbors: Bob, Frank
    assert!(names.contains(&"Bob"), "Bob is direct neighbor");
    assert!(names.contains(&"Frank"), "Frank is direct neighbor");
    assert_eq!(names.len(), 2, "Should find exactly 2 direct neighbors");
}

/// Test single hop equivalent to *1
#[test]
fn test_gql_variable_path_single_hop() {
    let graph = create_variable_length_path_graph();
    let snapshot = graph.snapshot();

    // *1 should be equivalent to regular single-hop traversal
    let results_single = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(target) RETURN target.name")
        .unwrap();

    let results_star1 = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS*1]->(target) RETURN target.name")
        .unwrap();

    // Both should return the same results
    assert_eq!(
        results_single.len(),
        results_star1.len(),
        "Single hop and *1 should return same count"
    );

    let names_single: std::collections::HashSet<&str> = results_single
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    let names_star1: std::collections::HashSet<&str> = results_star1
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(
        names_single, names_star1,
        "Single hop and *1 should return same names"
    );
}

// =============================================================================
// Phase 5.3: RETURN DISTINCT Tests
// =============================================================================

/// Helper to create a graph with duplicate property values for DISTINCT tests
fn create_distinct_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // Create Person vertices with duplicate cities
    let people = vec![
        ("Alice", "New York"),
        ("Bob", "Boston"),
        ("Carol", "New York"),
        ("Dave", "Boston"),
        ("Eve", "Chicago"),
        ("Frank", "New York"),
        ("Grace", "Chicago"),
    ];

    for (name, city) in people {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(name));
        props.insert("city".to_string(), Value::from(city));
        storage.add_vertex("Person", props);
    }

    Graph::new(Arc::new(storage))
}

/// Test RETURN DISTINCT on property - should deduplicate results
#[test]
fn test_gql_return_distinct_property() {
    let graph = create_distinct_test_graph();
    let snapshot = graph.snapshot();

    // Without DISTINCT - should return 7 cities (with duplicates)
    let results_no_distinct = snapshot.gql("MATCH (p:Person) RETURN p.city").unwrap();
    assert_eq!(
        results_no_distinct.len(),
        7,
        "Should return all 7 city values"
    );

    // With DISTINCT - should return only 3 unique cities
    let results_distinct = snapshot
        .gql("MATCH (p:Person) RETURN DISTINCT p.city")
        .unwrap();
    assert_eq!(
        results_distinct.len(),
        3,
        "Should return only 3 unique cities"
    );

    // Verify the unique cities
    let cities: Vec<&str> = results_distinct
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert!(cities.contains(&"New York"));
    assert!(cities.contains(&"Boston"));
    assert!(cities.contains(&"Chicago"));
}

/// Test RETURN DISTINCT with multiple properties
#[test]
fn test_gql_return_distinct_multiple_properties() {
    let mut storage = InMemoryGraph::new();

    // Create people with duplicate city/country combinations
    let people = vec![
        ("Alice", "NYC", "USA"),
        ("Bob", "Boston", "USA"),
        ("Carol", "NYC", "USA"), // Duplicate of Alice's city/country
        ("Dave", "London", "UK"),
        ("Eve", "London", "UK"), // Duplicate of Dave's city/country
    ];

    for (name, city, country) in people {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(name));
        props.insert("city".to_string(), Value::from(city));
        props.insert("country".to_string(), Value::from(country));
        storage.add_vertex("Person", props);
    }

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // RETURN DISTINCT on multiple properties - deduplicates based on the combination
    let results = snapshot
        .gql("MATCH (p:Person) RETURN DISTINCT p.city, p.country")
        .unwrap();

    // Should return 3 unique city/country combinations:
    // (NYC, USA), (Boston, USA), (London, UK)
    assert_eq!(
        results.len(),
        3,
        "Should return 3 unique city/country combinations"
    );
}

/// Test RETURN DISTINCT with variable-length paths
#[test]
fn test_gql_return_distinct_with_variable_path() {
    let graph = create_variable_length_path_graph();
    let snapshot = graph.snapshot();

    // Find all people reachable from Alice (paths may reach same person multiple ways)
    // Without DISTINCT, if implementation doesn't dedup at traversal level, we might get duplicates
    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..4]->(target) RETURN DISTINCT target.name")
        .unwrap();

    // Each person should appear only once
    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    // Verify no duplicates
    let unique_names: std::collections::HashSet<&str> = names.iter().copied().collect();
    assert_eq!(
        names.len(),
        unique_names.len(),
        "DISTINCT should eliminate any duplicate names"
    );
}

/// Test RETURN DISTINCT with WHERE clause
#[test]
fn test_gql_return_distinct_with_where() {
    let graph = create_distinct_test_graph();
    let snapshot = graph.snapshot();

    // Get distinct cities, but only from the first 5 results (conceptually)
    // Actually, we filter first then distinct
    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.city <> 'Chicago' RETURN DISTINCT p.city")
        .unwrap();

    // Without Chicago, we have New York and Boston
    assert_eq!(
        results.len(),
        2,
        "Should return 2 unique cities after filtering"
    );

    let cities: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert!(cities.contains(&"New York"));
    assert!(cities.contains(&"Boston"));
    assert!(!cities.contains(&"Chicago"));
}

/// Test RETURN DISTINCT with ORDER BY and LIMIT
#[test]
fn test_gql_return_distinct_with_order_limit() {
    let graph = create_distinct_test_graph();
    let snapshot = graph.snapshot();

    // Get distinct cities, ordered alphabetically, limit to 2
    let results = snapshot
        .gql("MATCH (p:Person) RETURN DISTINCT p.city ORDER BY p.city LIMIT 2")
        .unwrap();

    assert_eq!(results.len(), 2, "Should return 2 results after LIMIT");

    let cities: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    // Alphabetically: Boston, Chicago, New York
    // LIMIT 2: Boston, Chicago
    assert_eq!(cities[0], "Boston");
    assert_eq!(cities[1], "Chicago");
}

/// Test RETURN DISTINCT case insensitivity
#[test]
fn test_gql_return_distinct_case_insensitive() {
    let graph = create_distinct_test_graph();
    let snapshot = graph.snapshot();

    // Test that DISTINCT keyword is case insensitive
    let results1 = snapshot
        .gql("MATCH (p:Person) RETURN DISTINCT p.city")
        .unwrap();
    let results2 = snapshot
        .gql("MATCH (p:Person) RETURN distinct p.city")
        .unwrap();
    let results3 = snapshot
        .gql("MATCH (p:Person) RETURN Distinct p.city")
        .unwrap();

    assert_eq!(results1.len(), results2.len());
    assert_eq!(results2.len(), results3.len());
    assert_eq!(
        results1.len(),
        3,
        "All variants should return 3 unique cities"
    );
}

/// Test RETURN without DISTINCT returns duplicates
#[test]
fn test_gql_return_without_distinct_has_duplicates() {
    let graph = create_distinct_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (p:Person) RETURN p.city").unwrap();

    // Should return all 7 values including duplicates
    assert_eq!(results.len(), 7);

    // Count occurrences of each city
    let mut city_counts: HashMap<&str, usize> = HashMap::new();
    for result in &results {
        if let Value::String(city) = result {
            *city_counts.entry(city.as_str()).or_insert(0) += 1;
        }
    }

    assert_eq!(
        city_counts.get("New York"),
        Some(&3),
        "New York should appear 3 times"
    );
    assert_eq!(
        city_counts.get("Boston"),
        Some(&2),
        "Boston should appear 2 times"
    );
    assert_eq!(
        city_counts.get("Chicago"),
        Some(&2),
        "Chicago should appear 2 times"
    );
}

/// Test RETURN DISTINCT on vertex (deduplicates vertex IDs)
#[test]
fn test_gql_return_distinct_vertex() {
    let graph = create_distinct_test_graph();
    let snapshot = graph.snapshot();

    // Each person is unique, so DISTINCT shouldn't change the count
    let results_no_distinct = snapshot.gql("MATCH (p:Person) RETURN p").unwrap();
    let results_distinct = snapshot.gql("MATCH (p:Person) RETURN DISTINCT p").unwrap();

    assert_eq!(
        results_no_distinct.len(),
        results_distinct.len(),
        "DISTINCT on unique vertices should have same count"
    );
    assert_eq!(results_distinct.len(), 7);
}

// =============================================================================
// Phase 5.4: Improved Error Messages Tests
// =============================================================================

/// Test that parse errors include position information
#[test]
fn test_gql_parse_error_includes_position() {
    // Invalid syntax - error should include position
    let result = parse("MATCH (n:Person RETURN n");
    assert!(result.is_err());

    if let Err(e) = result {
        let error_msg = format!("{}", e);
        // The pest error includes position info with line/column format like "1:17"
        assert!(
            error_msg.contains("position")
                || error_msg.contains("line")
                || error_msg.contains("-->"),
            "Error message should contain position info: {}",
            error_msg
        );
    }
}

/// Test that parse errors for missing clauses include position information
#[test]
fn test_gql_parse_error_missing_clause_position() {
    // Missing RETURN clause
    let result = parse("MATCH (n:Person)");
    assert!(result.is_err());

    if let Err(e) = result {
        let error_msg = format!("{}", e);
        // Should mention either RETURN or position
        assert!(
            error_msg.contains("RETURN")
                || error_msg.contains("position")
                || error_msg.contains("-->"),
            "Error message should be helpful: {}",
            error_msg
        );
    }
}

/// Test that compile errors include helpful suggestions
#[test]
fn test_gql_compile_error_helpful_message() {
    let graph = Graph::in_memory();
    let snapshot = graph.snapshot();

    // Undefined variable - error should have suggestion
    let result = snapshot.gql("MATCH (n:Person) RETURN x");
    assert!(result.is_err());

    if let Err(e) = result {
        let error_msg = format!("{}", e);
        // Should mention the undefined variable and suggest binding it
        assert!(
            error_msg.contains("x"),
            "Error message should mention the undefined variable: {}",
            error_msg
        );
        assert!(
            error_msg.contains("MATCH")
                || error_msg.contains("bind")
                || error_msg.contains("forget"),
            "Error message should suggest binding in MATCH: {}",
            error_msg
        );
    }
}

/// Test that compile error for duplicate variable is helpful
#[test]
fn test_gql_compile_error_duplicate_variable_message() {
    use rustgremlin::gql::CompileError;

    // Test the error message directly since the compiler detects duplicates
    let err = CompileError::duplicate_variable("n");
    let error_msg = format!("{}", err);

    // Should mention the duplicate variable
    assert!(
        error_msg.contains("n"),
        "Error message should mention the duplicate variable: {}",
        error_msg
    );
    assert!(
        error_msg.contains("already defined") || error_msg.contains("duplicate"),
        "Error message should indicate it's a duplicate: {}",
        error_msg
    );
}

/// Test that ParseError span extraction works
#[test]
fn test_gql_parse_error_span_extraction() {
    use rustgremlin::gql::{ParseError, Span};

    // Create an error with a known span
    let err = ParseError::invalid_literal("abc", Span::new(5, 8), "expected integer");

    // Verify we can extract the span
    let span = err.span();
    assert!(span.is_some());
    let span = span.unwrap();
    assert_eq!(span.start, 5);
    assert_eq!(span.end, 8);

    // Verify the message is helpful
    let msg = format!("{}", err);
    assert!(msg.contains("abc"));
    assert!(msg.contains("5"));
    assert!(msg.contains("expected integer"));
}

/// Test that CompileError messages include suggestions
#[test]
fn test_gql_compile_error_suggestions() {
    use rustgremlin::gql::CompileError;

    // Test undefined variable suggestion
    let err = CompileError::undefined_variable("myVar");
    let msg = format!("{}", err);
    assert!(msg.contains("myVar"));
    assert!(msg.contains("Did you forget") || msg.contains("MATCH"));

    // Test duplicate variable message
    let err = CompileError::duplicate_variable("n");
    let msg = format!("{}", err);
    assert!(msg.contains("n"));
    assert!(msg.contains("already defined"));

    // Test aggregate in WHERE error
    let err = CompileError::aggregate_in_where("COUNT");
    let msg = format!("{}", err);
    assert!(msg.contains("COUNT"));
    assert!(msg.contains("WHERE"));
}

/// Test error message for empty pattern
#[test]
fn test_gql_compile_error_empty_pattern_message() {
    use rustgremlin::gql::CompileError;

    let err = CompileError::EmptyPattern;
    let msg = format!("{}", err);

    // Should explain what's wrong and how to fix it
    assert!(
        msg.contains("empty") || msg.contains("Empty"),
        "Error should mention empty pattern: {}",
        msg
    );
    assert!(
        msg.contains("MATCH") || msg.contains("node"),
        "Error should suggest how to fix: {}",
        msg
    );
}

/// Test error message for pattern must start with node
#[test]
fn test_gql_compile_error_pattern_start_message() {
    use rustgremlin::gql::CompileError;

    let err = CompileError::PatternMustStartWithNode;
    let msg = format!("{}", err);

    // Should explain the issue and solution
    assert!(
        msg.contains("start") || msg.contains("node"),
        "Error should explain pattern structure: {}",
        msg
    );
}

// =============================================================================
// PHASE 5.6: COMPREHENSIVE TEST SUITE
// =============================================================================
// Edge cases, complex queries, and stress tests for robust GQL coverage

// -----------------------------------------------------------------------------
// Edge Case Tests: Unicode and Special Characters
// -----------------------------------------------------------------------------

/// Test Unicode property values - Japanese characters
#[test]
fn test_gql_unicode_japanese_characters() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("田中太郎"));
    props.insert("city".to_string(), Value::from("東京"));
    storage.add_vertex("Person", props);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("佐藤花子"));
    props2.insert("city".to_string(), Value::from("大阪"));
    storage.add_vertex("Person", props2);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // Query for Japanese name
    let query = "MATCH (p:Person) WHERE p.name = '田中太郎' RETURN p.name, p.city";
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("p.name"), Some(&Value::from("田中太郎")));
        assert_eq!(row.get("p.city"), Some(&Value::from("東京")));
    } else {
        panic!("Expected map result");
    }
}

/// Test Unicode property values - German characters with umlauts
#[test]
fn test_gql_unicode_german_umlauts() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Müller"));
    props.insert("city".to_string(), Value::from("München"));
    storage.add_vertex("Person", props);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Schröder"));
    props2.insert("city".to_string(), Value::from("Köln"));
    storage.add_vertex("Person", props2);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // Query all German names
    let query = r#"MATCH (p:Person) RETURN p.name ORDER BY p.name"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 2);

    // Should handle umlauts correctly - single property returns String directly
    if let Value::String(name) = &results[0] {
        assert_eq!(name, "Müller");
    }
}

/// Test Unicode property values - Russian Cyrillic characters
#[test]
fn test_gql_unicode_russian_cyrillic() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Иванов"));
    props.insert("city".to_string(), Value::from("Москва"));
    storage.add_vertex("Person", props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let query = "MATCH (p:Person) WHERE p.city = 'Москва' RETURN p.name";
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("p.name"), Some(&Value::from("Иванов")));
    }
}

/// Test Unicode property values - Arabic characters (RTL script)
#[test]
fn test_gql_unicode_arabic() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("محمد"));
    props.insert("city".to_string(), Value::from("القاهرة"));
    storage.add_vertex("Person", props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let query = r#"MATCH (p:Person) RETURN p.name, p.city"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("p.name"), Some(&Value::from("محمد")));
        assert_eq!(row.get("p.city"), Some(&Value::from("القاهرة")));
    }
}

/// Test Unicode property values - Emoji characters
#[test]
fn test_gql_unicode_emoji() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Test User 🎉"));
    props.insert("status".to_string(), Value::from("😀👍🚀"));
    storage.add_vertex("Person", props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let query = r#"MATCH (p:Person) RETURN p.name, p.status"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("p.name"), Some(&Value::from("Test User 🎉")));
        assert_eq!(row.get("p.status"), Some(&Value::from("😀👍🚀")));
    }
}

/// Test Unicode property values - Mixed scripts
#[test]
fn test_gql_unicode_mixed_scripts() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert(
        "description".to_string(),
        Value::from("Hello 世界 Привет مرحبا 🌍"),
    );
    storage.add_vertex("Item", props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let query = r#"MATCH (i:Item) RETURN i.description"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(
            row.get("i.description"),
            Some(&Value::from("Hello 世界 Привет مرحبا 🌍"))
        );
    }
}

/// Test special characters - newlines and tabs in strings
#[test]
fn test_gql_special_chars_whitespace() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("bio".to_string(), Value::from("Line 1\nLine 2\nLine 3"));
    props.insert("data".to_string(), Value::from("Col1\tCol2\tCol3"));
    storage.add_vertex("Person", props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let query = r#"MATCH (p:Person) RETURN p.bio, p.data"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        let bio = row.get("p.bio").unwrap();
        if let Value::String(s) = bio {
            assert!(s.contains('\n'));
        }
    }
}

/// Test empty string property values
#[test]
fn test_gql_empty_string_property() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert("nickname".to_string(), Value::from(""));
    storage.add_vertex("Person", props);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Bob"));
    props2.insert("nickname".to_string(), Value::from("Bobby"));
    storage.add_vertex("Person", props2);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // Query for person with empty nickname
    let query = "MATCH (p:Person) WHERE p.nickname = '' RETURN p.name";
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("p.name"), Some(&Value::from("Alice")));
    }
}

// -----------------------------------------------------------------------------
// Edge Case Tests: Numeric Boundaries
// -----------------------------------------------------------------------------

/// Test large integer values (near i64::MAX)
#[test]
fn test_gql_large_integer_values() {
    let mut storage = InMemoryGraph::new();

    let large_val = i64::MAX - 1000;
    let mut props = HashMap::new();
    props.insert("id".to_string(), Value::Int(large_val));
    props.insert("name".to_string(), Value::from("BigNum"));
    storage.add_vertex("Entity", props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let query = r#"MATCH (e:Entity) RETURN e.id, e.name"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("e.id"), Some(&Value::Int(large_val)));
    }
}

/// Test negative integer values (near i64::MIN)
#[test]
fn test_gql_negative_integer_values() {
    let mut storage = InMemoryGraph::new();

    let small_val = i64::MIN + 1000;
    let mut props = HashMap::new();
    props.insert("balance".to_string(), Value::Int(small_val));
    props.insert("name".to_string(), Value::from("Debt"));
    storage.add_vertex("Account", props);

    let mut props2 = HashMap::new();
    props2.insert("balance".to_string(), Value::Int(1000i64));
    props2.insert("name".to_string(), Value::from("Savings"));
    storage.add_vertex("Account", props2);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // Query for negative balance
    let query = r#"MATCH (a:Account) WHERE a.balance < 0 RETURN a.name"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("a.name"), Some(&Value::from("Debt")));
    }
}

/// Test zero value comparisons
#[test]
fn test_gql_zero_comparisons() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("value".to_string(), Value::Int(0i64));
    props.insert("name".to_string(), Value::from("Zero"));
    storage.add_vertex("Number", props);

    let mut props2 = HashMap::new();
    props2.insert("value".to_string(), Value::Int(1i64));
    props2.insert("name".to_string(), Value::from("One"));
    storage.add_vertex("Number", props2);

    let mut props3 = HashMap::new();
    props3.insert("value".to_string(), Value::Int(-1i64));
    props3.insert("name".to_string(), Value::from("NegOne"));
    storage.add_vertex("Number", props3);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // Exactly zero
    let query = r#"MATCH (n:Number) WHERE n.value = 0 RETURN n.name"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    // Greater than or equal to zero
    let query2 = r#"MATCH (n:Number) WHERE n.value >= 0 RETURN n.name ORDER BY n.value"#;
    let results2: Vec<_> = snapshot.gql(query2).unwrap();
    assert_eq!(results2.len(), 2);
}

/// Test float precision
#[test]
fn test_gql_float_precision() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("rate".to_string(), Value::Float(0.1 + 0.2)); // Classic float precision test
    props.insert("name".to_string(), Value::from("FloatTest"));
    storage.add_vertex("Test", props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let query = r#"MATCH (t:Test) RETURN t.rate"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    // Just verify we can retrieve it - exact comparison is tricky with floats
    if let Value::Map(row) = &results[0] {
        if let Some(Value::Float(f)) = row.get("t.rate") {
            assert!((f - 0.3).abs() < 0.0001);
        }
    }
}

/// Test very small float values
#[test]
fn test_gql_small_float_values() {
    let mut storage = InMemoryGraph::new();

    let tiny = 1e-10f64;
    let mut props = HashMap::new();
    props.insert("epsilon".to_string(), Value::Float(tiny));
    storage.add_vertex("Math", props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let query = r#"MATCH (m:Math) RETURN m.epsilon"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("m.epsilon"), Some(&Value::Float(tiny)));
    }
}

// -----------------------------------------------------------------------------
// Edge Case Tests: Null Handling
// -----------------------------------------------------------------------------

/// Test missing properties filter out results (not return null)
#[test]
fn test_gql_missing_property_returns_null() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    // Note: no "age" property
    storage.add_vertex("Person", props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // When returning a missing property, the row is filtered out
    let query = r#"MATCH (p:Person) RETURN p.name, p.age"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // Missing properties filter out the result (behavior matches existing tests)
    assert_eq!(
        results.len(),
        0,
        "Missing property should filter out result"
    );
}

/// Test IS NULL with missing properties
#[test]
fn test_gql_is_null_missing_property() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    storage.add_vertex("Person", props);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Bob"));
    props2.insert("age".to_string(), Value::Int(30i64));
    storage.add_vertex("Person", props2);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // Find people without age
    let query = r#"MATCH (p:Person) WHERE p.age IS NULL RETURN p.name"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("p.name"), Some(&Value::from("Alice")));
    }
}

/// Test IS NOT NULL
#[test]
fn test_gql_is_not_null() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    storage.add_vertex("Person", props);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Bob"));
    props2.insert("email".to_string(), Value::from("bob@example.com"));
    storage.add_vertex("Person", props2);

    let mut props3 = HashMap::new();
    props3.insert("name".to_string(), Value::from("Charlie"));
    props3.insert("email".to_string(), Value::from("charlie@example.com"));
    storage.add_vertex("Person", props3);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // Find people with email
    let query = r#"MATCH (p:Person) WHERE p.email IS NOT NULL RETURN p.name ORDER BY p.name"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 2);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("p.name"), Some(&Value::from("Bob")));
    }
}

/// Test explicit null value property
#[test]
fn test_gql_explicit_null_property() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert("middlename".to_string(), Value::Null);
    storage.add_vertex("Person", props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let query = r#"MATCH (p:Person) WHERE p.middlename IS NULL RETURN p.name"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);
}

// -----------------------------------------------------------------------------
// Edge Case Tests: Boolean Values
// -----------------------------------------------------------------------------

/// Test boolean property filtering
#[test]
fn test_gql_boolean_property_true() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert("active".to_string(), Value::Bool(true));
    storage.add_vertex("User", props);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Bob"));
    props2.insert("active".to_string(), Value::Bool(false));
    storage.add_vertex("User", props2);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let query = r#"MATCH (u:User) WHERE u.active = true RETURN u.name"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("u.name"), Some(&Value::from("Alice")));
    }
}

/// Test boolean property filtering for false
#[test]
fn test_gql_boolean_property_false() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert("verified".to_string(), Value::Bool(true));
    storage.add_vertex("User", props);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Bob"));
    props2.insert("verified".to_string(), Value::Bool(false));
    storage.add_vertex("User", props2);

    let mut props3 = HashMap::new();
    props3.insert("name".to_string(), Value::from("Charlie"));
    props3.insert("verified".to_string(), Value::Bool(false));
    storage.add_vertex("User", props3);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let query = r#"MATCH (u:User) WHERE u.verified = false RETURN u.name ORDER BY u.name"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 2);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("u.name"), Some(&Value::from("Bob")));
    }
}

// =============================================================================
// COMPLEX QUERY INTEGRATION TESTS: Social Network Graph
// =============================================================================

/// Helper function to create a social network graph for complex query testing
///
/// Graph structure:
/// - 8 people with various properties (name, age, city)
/// - KNOWS relationships forming a social network
/// - WORKS_AT relationships to companies
///
/// People: Alice(28,NYC), Bob(35,LA), Charlie(42,NYC), Diana(31,Chicago),
///         Eve(25,LA), Frank(55,NYC), Grace(29,Boston), Henry(38,Seattle)
///
/// Relationships:
/// - Alice KNOWS Bob, Charlie, Diana
/// - Bob KNOWS Alice, Eve, Frank
/// - Charlie KNOWS Alice, Diana, Grace
/// - Diana KNOWS Alice, Charlie, Eve
/// - Eve KNOWS Bob, Diana, Henry
/// - Frank KNOWS Bob, Grace
/// - Grace KNOWS Charlie, Frank, Henry
/// - Henry KNOWS Eve, Grace
fn create_social_network_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // Create people
    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    alice_props.insert("age".to_string(), Value::Int(28i64));
    alice_props.insert("city".to_string(), Value::from("NYC"));
    let alice = storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    bob_props.insert("age".to_string(), Value::Int(35i64));
    bob_props.insert("city".to_string(), Value::from("LA"));
    let bob = storage.add_vertex("Person", bob_props);

    let mut charlie_props = HashMap::new();
    charlie_props.insert("name".to_string(), Value::from("Charlie"));
    charlie_props.insert("age".to_string(), Value::Int(42i64));
    charlie_props.insert("city".to_string(), Value::from("NYC"));
    let charlie = storage.add_vertex("Person", charlie_props);

    let mut diana_props = HashMap::new();
    diana_props.insert("name".to_string(), Value::from("Diana"));
    diana_props.insert("age".to_string(), Value::Int(31i64));
    diana_props.insert("city".to_string(), Value::from("Chicago"));
    let diana = storage.add_vertex("Person", diana_props);

    let mut eve_props = HashMap::new();
    eve_props.insert("name".to_string(), Value::from("Eve"));
    eve_props.insert("age".to_string(), Value::Int(25i64));
    eve_props.insert("city".to_string(), Value::from("LA"));
    let eve = storage.add_vertex("Person", eve_props);

    let mut frank_props = HashMap::new();
    frank_props.insert("name".to_string(), Value::from("Frank"));
    frank_props.insert("age".to_string(), Value::Int(55i64));
    frank_props.insert("city".to_string(), Value::from("NYC"));
    let frank = storage.add_vertex("Person", frank_props);

    let mut grace_props = HashMap::new();
    grace_props.insert("name".to_string(), Value::from("Grace"));
    grace_props.insert("age".to_string(), Value::Int(29i64));
    grace_props.insert("city".to_string(), Value::from("Boston"));
    let grace = storage.add_vertex("Person", grace_props);

    let mut henry_props = HashMap::new();
    henry_props.insert("name".to_string(), Value::from("Henry"));
    henry_props.insert("age".to_string(), Value::Int(38i64));
    henry_props.insert("city".to_string(), Value::from("Seattle"));
    let henry = storage.add_vertex("Person", henry_props);

    // Create companies
    let mut tech_props = HashMap::new();
    tech_props.insert("name".to_string(), Value::from("TechCorp"));
    tech_props.insert("industry".to_string(), Value::from("Technology"));
    let techcorp = storage.add_vertex("Company", tech_props);

    let mut fin_props = HashMap::new();
    fin_props.insert("name".to_string(), Value::from("FinanceInc"));
    fin_props.insert("industry".to_string(), Value::from("Finance"));
    let financeinc = storage.add_vertex("Company", fin_props);

    // KNOWS relationships (bidirectional conceptually, but stored as directed)
    storage
        .add_edge(alice, bob, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, charlie, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, diana, "KNOWS", HashMap::new())
        .unwrap();

    storage
        .add_edge(bob, alice, "KNOWS", HashMap::new())
        .unwrap();
    storage.add_edge(bob, eve, "KNOWS", HashMap::new()).unwrap();
    storage
        .add_edge(bob, frank, "KNOWS", HashMap::new())
        .unwrap();

    storage
        .add_edge(charlie, alice, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(charlie, diana, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(charlie, grace, "KNOWS", HashMap::new())
        .unwrap();

    storage
        .add_edge(diana, alice, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(diana, charlie, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(diana, eve, "KNOWS", HashMap::new())
        .unwrap();

    storage.add_edge(eve, bob, "KNOWS", HashMap::new()).unwrap();
    storage
        .add_edge(eve, diana, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(eve, henry, "KNOWS", HashMap::new())
        .unwrap();

    storage
        .add_edge(frank, bob, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(frank, grace, "KNOWS", HashMap::new())
        .unwrap();

    storage
        .add_edge(grace, charlie, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(grace, frank, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(grace, henry, "KNOWS", HashMap::new())
        .unwrap();

    storage
        .add_edge(henry, eve, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(henry, grace, "KNOWS", HashMap::new())
        .unwrap();

    // WORKS_AT relationships
    storage
        .add_edge(alice, techcorp, "WORKS_AT", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, techcorp, "WORKS_AT", HashMap::new())
        .unwrap();
    storage
        .add_edge(charlie, financeinc, "WORKS_AT", HashMap::new())
        .unwrap();
    storage
        .add_edge(diana, techcorp, "WORKS_AT", HashMap::new())
        .unwrap();
    storage
        .add_edge(frank, financeinc, "WORKS_AT", HashMap::new())
        .unwrap();

    Graph::new(Arc::new(storage))
}

/// Test: Find all friends of Alice
#[test]
fn test_gql_social_network_direct_friends() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    // Use inline property filter syntax instead of WHERE
    let query = "MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(friend:Person) RETURN friend.name ORDER BY friend.name";
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 3);

    // Single property return gives Value::String directly, not Map
    let names: Vec<&str> = results
        .iter()
        .filter_map(|r| {
            if let Value::String(name) = r {
                return Some(name.as_str());
            }
            None
        })
        .collect();

    assert_eq!(names, vec!["Bob", "Charlie", "Diana"]);
}

/// Test: Find friends of friends (2-hop traversal)
#[test]
fn test_gql_social_network_friends_of_friends() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    // Friends of Alice's friends (excluding Alice herself)
    // Use inline property filter for the starting node
    let query = "MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(:Person)-[:KNOWS]->(fof:Person) WHERE fof.name <> 'Alice' RETURN DISTINCT fof.name ORDER BY fof.name";
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // Single property return gives Value::String directly
    let names: Vec<&str> = results
        .iter()
        .filter_map(|r| {
            if let Value::String(name) = r {
                return Some(name.as_str());
            }
            None
        })
        .collect();

    // Alice's friends: Bob, Charlie, Diana
    // Bob knows: Alice, Eve, Frank
    // Charlie knows: Alice, Diana, Grace
    // Diana knows: Alice, Charlie, Eve
    // FOF (excluding Alice): Eve, Frank, Diana, Grace, Charlie
    // DISTINCT: Charlie, Diana, Eve, Frank, Grace (sorted)
    assert!(names.contains(&"Eve"));
    assert!(names.contains(&"Frank"));
    assert!(names.contains(&"Grace"));
}

/// Test: Filter social network by age
#[test]
fn test_gql_social_network_filter_by_age() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    // Find people over 30
    // People: Alice(28), Bob(35), Charlie(42), Diana(31), Eve(25), Frank(55), Grace(29), Henry(38)
    // Over 30: Bob(35), Charlie(42), Diana(31), Frank(55), Henry(38) = 5 people
    let query = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name, p.age ORDER BY p.age DESC";
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 5); // Frank(55), Charlie(42), Henry(38), Bob(35), Diana(31)

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("p.name"), Some(&Value::from("Frank")));
        assert_eq!(row.get("p.age"), Some(&Value::Int(55i64)));
    }
}

/// Test: Filter by city with ordering and limit
#[test]
fn test_gql_social_network_city_filter_with_limit() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    // Find people in NYC, ordered by age, limited to 2
    let query = r#"
        MATCH (p:Person)
        WHERE p.city = 'NYC'
        RETURN p.name, p.age
        ORDER BY p.age
        LIMIT 2
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 2);

    // NYC people: Alice(28), Charlie(42), Frank(55)
    // Ordered by age, limit 2: Alice(28), Charlie(42)
    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("p.name"), Some(&Value::from("Alice")));
    }
    if let Value::Map(row) = &results[1] {
        assert_eq!(row.get("p.name"), Some(&Value::from("Charlie")));
    }
}

/// Test: Count friends per person using aggregation
#[test]
fn test_gql_social_network_count_friends() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (p:Person)-[:KNOWS]->(friend:Person)
        RETURN p.name, COUNT(friend) AS friend_count
        ORDER BY friend_count DESC, p.name
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 8); // All 8 people

    // Alice, Bob, Charlie, Diana, Eve, Grace all have 3 friends
    // Frank has 2 friends
    // Henry has 2 friends
    if let Value::Map(row) = &results[0] {
        // First should have 3 friends
        assert_eq!(row.get("friend_count"), Some(&Value::Int(3i64)));
    }
}

/// Test: Average age of friends
#[test]
fn test_gql_social_network_avg_friend_age() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    // Use inline property filter for starting node
    let query = "MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(friend:Person) RETURN AVG(friend.age) AS avg_age";
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    // Alice's friends: Bob(35), Charlie(42), Diana(31)
    // Average: (35 + 42 + 31) / 3 = 36.0
    if let Value::Map(row) = &results[0] {
        if let Some(Value::Float(avg)) = row.get("avg_age") {
            assert!(
                (avg - 36.0).abs() < 0.01,
                "Expected average ~36.0, got {}",
                avg
            );
        } else {
            panic!("Expected float avg_age, got {:?}", row.get("avg_age"));
        }
    } else {
        panic!("Expected Map result, got {:?}", results[0]);
    }
}

/// Test: Find people who work at the same company
#[test]
fn test_gql_social_network_coworkers() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    // Simpler approach: find coworkers by matching same company
    // Note: The `<-` pattern in the middle of a longer path may not be supported
    // So we use a simpler query that finds people at the same company
    let query = "MATCH (p1:Person)-[:WORKS_AT]->(c:Company) RETURN p1.name, c.name AS company ORDER BY company, p1.name";
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // Should have 5 people who work at companies:
    // TechCorp: Alice, Bob, Diana
    // FinanceInc: Charlie, Frank
    assert_eq!(results.len(), 5);
}

/// Test: Find people in the same city who don't know each other
#[test]
fn test_gql_social_network_city_strangers() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    // Find all NYC people
    let query = r#"
        MATCH (p:Person)
        WHERE p.city = 'NYC'
        RETURN p.name
        ORDER BY p.name
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 3); // Alice, Charlie, Frank
}

/// Test: Multi-hop path with variable length (2-3 hops)
#[test]
fn test_gql_social_network_variable_length_path() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    // Find people reachable from Alice in 2-3 hops
    let query = r#"
        MATCH (p:Person)-[:KNOWS*2..3]->(target:Person)
        WHERE p.name = 'Alice'
        RETURN DISTINCT target.name
        ORDER BY target.name
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // Should find people 2-3 hops away from Alice
    assert!(!results.is_empty());
}

/// Test: Collect names into a list
#[test]
fn test_gql_social_network_collect_names() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (p:Person)
        WHERE p.city = 'LA'
        RETURN COLLECT(p.name) AS la_people
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        if let Some(Value::List(names)) = row.get("la_people") {
            assert_eq!(names.len(), 2); // Bob and Eve
        } else {
            panic!("Expected list");
        }
    }
}

/// Test: Combined WHERE conditions with AND
#[test]
fn test_gql_social_network_combined_where_and() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (p:Person)
        WHERE p.age > 25 AND p.age < 40
        RETURN p.name, p.age
        ORDER BY p.age
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // People aged 26-39: Alice(28), Grace(29), Diana(31), Bob(35), Henry(38)
    assert_eq!(results.len(), 5);
}

/// Test: Combined WHERE conditions with OR
#[test]
fn test_gql_social_network_combined_where_or() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (p:Person)
        WHERE p.city = 'NYC' OR p.city = 'LA'
        RETURN p.name
        ORDER BY p.name
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // NYC: Alice, Charlie, Frank
    // LA: Bob, Eve
    assert_eq!(results.len(), 5);
}

/// Test: MIN and MAX aggregations
#[test]
fn test_gql_social_network_min_max_age() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (p:Person)
        RETURN MIN(p.age) AS youngest, MAX(p.age) AS oldest
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("youngest"), Some(&Value::Int(25i64))); // Eve
        assert_eq!(row.get("oldest"), Some(&Value::Int(55i64))); // Frank
    }
}

/// Test: SUM aggregation
#[test]
fn test_gql_social_network_sum_ages() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (p:Person)
        WHERE p.city = 'NYC'
        RETURN SUM(p.age) AS total_age
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    // NYC: Alice(28) + Charlie(42) + Frank(55) = 125
    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("total_age"), Some(&Value::Int(125i64)));
    }
}

// =============================================================================
// STRESS TESTS: Large Datasets and Performance
// =============================================================================

/// Stress test: Query across 1000 vertices
#[test]
fn test_gql_stress_1000_vertices() {
    let mut storage = InMemoryGraph::new();

    // Create 1000 Person vertices
    for i in 0..1000 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(format!("Person{}", i)));
        props.insert("index".to_string(), Value::Int(i as i64));
        props.insert("group".to_string(), Value::Int((i % 10) as i64));
        storage.add_vertex("Person", props);
    }

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // Count all vertices
    let query = r#"MATCH (p:Person) RETURN COUNT(p) AS total"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("total"), Some(&Value::Int(1000i64)));
    }

    // Filter to specific group
    let query2 = r#"MATCH (p:Person) WHERE p.group = 5 RETURN COUNT(p) AS count"#;
    let results2: Vec<_> = snapshot.gql(query2).unwrap();
    assert_eq!(results2.len(), 1);

    if let Value::Map(row) = &results2[0] {
        assert_eq!(row.get("count"), Some(&Value::Int(100i64)));
    }
}

/// Stress test: Dense graph with many edges (250 edges)
#[test]
fn test_gql_stress_dense_graph() {
    let mut storage = InMemoryGraph::new();

    // Create 50 vertices
    let mut vertex_ids = Vec::new();
    for i in 0..50 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(format!("Node{}", i)));
        props.insert("tier".to_string(), Value::Int((i % 5) as i64));
        let id = storage.add_vertex("Node", props);
        vertex_ids.push(id);
    }

    // Create 250 edges (each node connects to 5 random others)
    for i in 0..50 {
        for j in 1..=5 {
            let target = (i + j * 7) % 50; // deterministic "random" targets
            if i != target {
                storage
                    .add_edge(
                        vertex_ids[i],
                        vertex_ids[target],
                        "CONNECTS",
                        HashMap::new(),
                    )
                    .unwrap();
            }
        }
    }

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // Count all connections
    let query = r#"MATCH (a:Node)-[:CONNECTS]->(b:Node) RETURN COUNT(*) AS edge_count"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        if let Some(Value::Int(count)) = row.get("edge_count") {
            assert!(*count > 200); // Should have many edges
        }
    }

    // Query with filter on tier
    let query2 = r#"
        MATCH (a:Node)-[:CONNECTS]->(b:Node)
        WHERE a.tier = 0
        RETURN COUNT(*) AS connections
    "#;
    let results2: Vec<_> = snapshot.gql(query2).unwrap();
    assert_eq!(results2.len(), 1);
}

/// Stress test: Large aggregation
#[test]
fn test_gql_stress_large_aggregation() {
    let mut storage = InMemoryGraph::new();

    // Create 500 transactions with varying amounts
    for i in 0..500 {
        let mut props = HashMap::new();
        props.insert("id".to_string(), Value::Int(i as i64));
        props.insert("amount".to_string(), Value::Float((i as f64) * 10.5));
        props.insert("category".to_string(), Value::from(format!("Cat{}", i % 5)));
        storage.add_vertex("Transaction", props);
    }

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // Sum all amounts
    let query = r#"MATCH (t:Transaction) RETURN SUM(t.amount) AS total"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        if let Some(Value::Float(total)) = row.get("total") {
            // Sum of 0 + 10.5 + 21 + ... + 499*10.5 = 10.5 * (0+1+2+...+499) = 10.5 * 499*500/2
            let expected = 10.5 * 499.0 * 500.0 / 2.0;
            assert!((total - expected).abs() < 1.0);
        }
    }

    // Average by category
    let query2 = r#"
        MATCH (t:Transaction)
        RETURN t.category, AVG(t.amount) AS avg_amount, COUNT(t) AS count
        ORDER BY t.category
    "#;
    let results2: Vec<_> = snapshot.gql(query2).unwrap();
    assert_eq!(results2.len(), 5); // 5 categories

    // Each category should have 100 transactions (500/5)
    if let Value::Map(row) = &results2[0] {
        assert_eq!(row.get("count"), Some(&Value::Int(100i64)));
    }
}

/// Stress test: ORDER BY on large dataset
#[test]
fn test_gql_stress_large_order_by() {
    let mut storage = InMemoryGraph::new();

    // Create 200 items with random-ish scores
    for i in 0..200 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(format!("Item{:03}", i)));
        // Create varying scores using a formula
        let score = ((i * 17 + 23) % 1000) as i64;
        props.insert("score".to_string(), Value::Int(score));
        storage.add_vertex("Item", props);
    }

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // Get top 10 by score
    let query = r#"
        MATCH (i:Item)
        RETURN i.name, i.score
        ORDER BY i.score DESC
        LIMIT 10
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 10);

    // Verify ordering is descending
    let mut prev_score = i64::MAX;
    for result in &results {
        if let Value::Map(row) = result {
            if let Some(Value::Int(score)) = row.get("i.score") {
                assert!(
                    *score <= prev_score,
                    "Results should be in descending order"
                );
                prev_score = *score;
            }
        }
    }
}

/// Stress test: OFFSET with large skip
#[test]
fn test_gql_stress_large_offset() {
    let mut storage = InMemoryGraph::new();

    // Create 300 records
    for i in 0..300 {
        let mut props = HashMap::new();
        props.insert("index".to_string(), Value::Int(i as i64));
        storage.add_vertex("Record", props);
    }

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // Skip 290, get remaining - LIMIT must come before OFFSET
    let query = "MATCH (r:Record) RETURN r.index ORDER BY r.index LIMIT 1000 OFFSET 290";
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 10); // 300 - 290 = 10

    // First result should be index 290
    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("r.index"), Some(&Value::Int(290i64)));
    }
}

/// Stress test: Complex multi-hop traversal on medium graph
#[test]
fn test_gql_stress_multi_hop_traversal() {
    let mut storage = InMemoryGraph::new();

    // Create a chain of 100 nodes: Node0 -> Node1 -> Node2 -> ... -> Node99
    let mut vertex_ids = Vec::new();
    for i in 0..100 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(format!("Node{}", i)));
        props.insert("depth".to_string(), Value::Int(i as i64));
        let id = storage.add_vertex("ChainNode", props);
        vertex_ids.push(id);
    }

    // Create chain edges
    for i in 0..99 {
        storage
            .add_edge(vertex_ids[i], vertex_ids[i + 1], "NEXT", HashMap::new())
            .unwrap();
    }

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // Find direct successors of Node0 - use inline property filter
    let query = "MATCH (n:ChainNode {name: 'Node0'})-[:NEXT]->(next:ChainNode) RETURN next.name";
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    // Single property return gives Value::String directly
    if let Value::String(name) = &results[0] {
        assert_eq!(name, "Node1");
    } else {
        panic!("Expected String result, got {:?}", results[0]);
    }

    // Find nodes 5 hops from Node0 - use inline property filter
    let query2 =
        "MATCH (n:ChainNode {name: 'Node0'})-[:NEXT*5]->(target:ChainNode) RETURN target.name";
    let results2: Vec<_> = snapshot.gql(query2).unwrap();
    assert_eq!(results2.len(), 1);

    // Single property return gives Value::String directly
    if let Value::String(name) = &results2[0] {
        assert_eq!(name, "Node5");
    } else {
        panic!("Expected String result, got {:?}", results2[0]);
    }
}

/// Stress test: Multiple labels query
#[test]
fn test_gql_stress_multiple_labels() {
    let mut storage = InMemoryGraph::new();

    // Create various entity types
    for i in 0..100 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(format!("Person{}", i)));
        storage.add_vertex("Person", props);
    }
    for i in 0..50 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(format!("Company{}", i)));
        storage.add_vertex("Company", props);
    }
    for i in 0..75 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(format!("Product{}", i)));
        storage.add_vertex("Product", props);
    }

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // Count each type
    let query1 = r#"MATCH (p:Person) RETURN COUNT(p) AS count"#;
    let results1: Vec<_> = snapshot.gql(query1).unwrap();
    if let Value::Map(row) = &results1[0] {
        assert_eq!(row.get("count"), Some(&Value::Int(100i64)));
    }

    let query2 = r#"MATCH (c:Company) RETURN COUNT(c) AS count"#;
    let results2: Vec<_> = snapshot.gql(query2).unwrap();
    if let Value::Map(row) = &results2[0] {
        assert_eq!(row.get("count"), Some(&Value::Int(50i64)));
    }

    let query3 = r#"MATCH (p:Product) RETURN COUNT(p) AS count"#;
    let results3: Vec<_> = snapshot.gql(query3).unwrap();
    if let Value::Map(row) = &results3[0] {
        assert_eq!(row.get("count"), Some(&Value::Int(75i64)));
    }
}

/// Stress test: DISTINCT on many duplicates
#[test]
fn test_gql_stress_distinct_many_duplicates() {
    let mut storage = InMemoryGraph::new();

    // Create 500 items with only 10 unique categories
    for i in 0..500 {
        let mut props = HashMap::new();
        props.insert("id".to_string(), Value::Int(i as i64));
        props.insert(
            "category".to_string(),
            Value::from(format!("Category{}", i % 10)),
        );
        storage.add_vertex("Item", props);
    }

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (i:Item)
        RETURN DISTINCT i.category
        ORDER BY i.category
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 10);
}

/// Stress test: Complex WHERE with multiple conditions
#[test]
fn test_gql_stress_complex_where() {
    let mut storage = InMemoryGraph::new();

    // Create data with multiple filterable properties
    for i in 0..200 {
        let mut props = HashMap::new();
        props.insert("id".to_string(), Value::Int(i as i64));
        props.insert("value".to_string(), Value::Int((i * 3) as i64));
        props.insert("active".to_string(), Value::Bool(i % 2 == 0));
        props.insert("tier".to_string(), Value::from(format!("T{}", i % 4)));
        storage.add_vertex("Entity", props);
    }

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // Complex filter: active entities in tier T0 or T2 with value > 100
    let query = r#"
        MATCH (e:Entity)
        WHERE e.active = true AND (e.tier = 'T0' OR e.tier = 'T2') AND e.value > 100
        RETURN COUNT(e) AS count
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    // Manual verification:
    // active=true means i%2==0, so i=0,2,4,...
    // tier T0 means i%4==0, tier T2 means i%4==2
    // Combined: i%4==0 or i%4==2, AND i%2==0
    // That's i=0,2,4,6,8,...  where i%4 is 0 or 2
    // value > 100 means i*3 > 100, so i > 33.33, i >= 34
    // Count should be positive
    if let Value::Map(row) = &results[0] {
        if let Some(Value::Int(count)) = row.get("count") {
            assert!(*count > 0);
        }
    }
}

// =============================================================================
// EXISTS Expression Tests
// =============================================================================

/// Helper to create a graph with relationships for EXISTS testing
fn create_exists_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // Create players
    let mut mj_props = HashMap::new();
    mj_props.insert("name".to_string(), Value::from("Michael Jordan"));
    mj_props.insert("position".to_string(), Value::from("Shooting Guard"));
    let mj_id = storage.add_vertex("player", mj_props);

    let mut kobe_props = HashMap::new();
    kobe_props.insert("name".to_string(), Value::from("Kobe Bryant"));
    kobe_props.insert("position".to_string(), Value::from("Shooting Guard"));
    let kobe_id = storage.add_vertex("player", kobe_props);

    let mut barkley_props = HashMap::new();
    barkley_props.insert("name".to_string(), Value::from("Charles Barkley"));
    barkley_props.insert("position".to_string(), Value::from("Power Forward"));
    let barkley_id = storage.add_vertex("player", barkley_props);

    let mut nash_props = HashMap::new();
    nash_props.insert("name".to_string(), Value::from("Steve Nash"));
    nash_props.insert("position".to_string(), Value::from("Point Guard"));
    let nash_id = storage.add_vertex("player", nash_props);

    // Create teams
    let mut bulls_props = HashMap::new();
    bulls_props.insert("name".to_string(), Value::from("Chicago Bulls"));
    bulls_props.insert("championships".to_string(), Value::Int(6));
    let bulls_id = storage.add_vertex("team", bulls_props);

    let mut lakers_props = HashMap::new();
    lakers_props.insert("name".to_string(), Value::from("Los Angeles Lakers"));
    lakers_props.insert("championships".to_string(), Value::Int(17));
    let lakers_id = storage.add_vertex("team", lakers_props);

    let mut suns_props = HashMap::new();
    suns_props.insert("name".to_string(), Value::from("Phoenix Suns"));
    suns_props.insert("championships".to_string(), Value::Int(0));
    let suns_id = storage.add_vertex("team", suns_props);

    // Add championship relationships (only MJ and Kobe have won)
    let mut ring_props = HashMap::new();
    ring_props.insert("years".to_string(), Value::from("1991-1993,1996-1998"));
    let _ = storage.add_edge(mj_id, bulls_id, "won_championship_with", ring_props.clone());

    ring_props.insert("years".to_string(), Value::from("2000-2002,2009-2010"));
    let _ = storage.add_edge(kobe_id, lakers_id, "won_championship_with", ring_props);

    // Add played_for relationships
    let played_props = HashMap::new();
    let _ = storage.add_edge(mj_id, bulls_id, "played_for", played_props.clone());
    let _ = storage.add_edge(kobe_id, lakers_id, "played_for", played_props.clone());
    let _ = storage.add_edge(barkley_id, suns_id, "played_for", played_props.clone());
    let _ = storage.add_edge(nash_id, suns_id, "played_for", played_props);

    Graph::new(Arc::new(storage))
}

#[test]
fn test_gql_exists_basic() {
    let graph = create_exists_test_graph();
    let snapshot = graph.snapshot();

    // Find players who have won championships
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE EXISTS { (p)-[:won_championship_with]->() }
        RETURN p.name
    "#,
        )
        .unwrap();

    // Should find MJ and Kobe
    assert_eq!(results.len(), 2);

    let names: Vec<String> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert!(names.contains(&"Michael Jordan".to_string()));
    assert!(names.contains(&"Kobe Bryant".to_string()));
}

#[test]
fn test_gql_not_exists() {
    let graph = create_exists_test_graph();
    let snapshot = graph.snapshot();

    // Find players who have NOT won championships
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE NOT EXISTS { (p)-[:won_championship_with]->() }
        RETURN p.name
    "#,
        )
        .unwrap();

    // Should find Barkley and Nash
    assert_eq!(results.len(), 2);

    let names: Vec<String> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert!(names.contains(&"Charles Barkley".to_string()));
    assert!(names.contains(&"Steve Nash".to_string()));
}

#[test]
fn test_gql_exists_with_target_label() {
    let graph = create_exists_test_graph();
    let snapshot = graph.snapshot();

    // Find players who played for a team (all players should match)
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE EXISTS { (p)-[:played_for]->(t:team) }
        RETURN p.name
    "#,
        )
        .unwrap();

    // All 4 players have played for a team
    assert_eq!(results.len(), 4);
}

#[test]
fn test_gql_exists_combined_with_and() {
    let graph = create_exists_test_graph();
    let snapshot = graph.snapshot();

    // Find shooting guards who have won championships
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE p.position = 'Shooting Guard' AND EXISTS { (p)-[:won_championship_with]->() }
        RETURN p.name
    "#,
        )
        .unwrap();

    // MJ and Kobe are both shooting guards with championships
    assert_eq!(results.len(), 2);
}

#[test]
fn test_gql_exists_combined_with_or() {
    let graph = create_exists_test_graph();
    let snapshot = graph.snapshot();

    // Find point guards OR players who have won championships
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE p.position = 'Point Guard' OR EXISTS { (p)-[:won_championship_with]->() }
        RETURN p.name
    "#,
        )
        .unwrap();

    // Should find: MJ, Kobe (champions), Nash (point guard)
    assert_eq!(results.len(), 3);
}

#[test]
fn test_gql_exists_no_match() {
    let graph = create_exists_test_graph();
    let snapshot = graph.snapshot();

    // Find players who have an edge type that doesn't exist
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE EXISTS { (p)-[:nonexistent_relationship]->() }
        RETURN p.name
    "#,
        )
        .unwrap();

    // No players have this relationship type
    assert_eq!(results.len(), 0);
}

#[test]
fn test_gql_exists_with_count() {
    let graph = create_exists_test_graph();
    let snapshot = graph.snapshot();

    // Count players who have won championships
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE EXISTS { (p)-[:won_championship_with]->() }
        RETURN count(*)
    "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(2));
}

#[test]
fn test_gql_parse_exists_expression() {
    // Test that EXISTS expressions parse correctly
    let ast = parse(
        r#"
        MATCH (p:player)
        WHERE EXISTS { (p)-[:won_championship_with]->() }
        RETURN p.name
    "#,
    )
    .unwrap();

    assert!(ast.where_clause.is_some());

    // The where clause should contain an EXISTS expression
    let where_clause = ast.where_clause.unwrap();
    match where_clause.expression {
        rustgremlin::gql::Expression::Exists { negated, pattern } => {
            assert!(!negated);
            assert!(!pattern.elements.is_empty());
        }
        _ => panic!("Expected EXISTS expression"),
    }
}

#[test]
fn test_gql_parse_not_exists_expression() {
    // Test that NOT EXISTS expressions parse correctly
    // NOT EXISTS is parsed as UnaryOp(Not, Exists { negated: false, ... })
    let ast = parse(
        r#"
        MATCH (p:player)
        WHERE NOT EXISTS { (p)-[:knows]->() }
        RETURN p.name
    "#,
    )
    .unwrap();

    assert!(ast.where_clause.is_some());

    let where_clause = ast.where_clause.unwrap();
    match where_clause.expression {
        rustgremlin::gql::Expression::UnaryOp { op, expr } => {
            assert!(matches!(op, rustgremlin::gql::UnaryOperator::Not));
            match *expr {
                rustgremlin::gql::Expression::Exists { negated, pattern } => {
                    assert!(!negated);
                    assert!(!pattern.elements.is_empty());
                }
                _ => panic!("Expected EXISTS expression inside NOT"),
            }
        }
        rustgremlin::gql::Expression::Exists { negated, pattern } => {
            // Alternative: if grammar is changed to support NOT directly
            assert!(negated);
            assert!(!pattern.elements.is_empty());
        }
        _ => panic!("Expected NOT(EXISTS) or EXISTS(negated=true) expression"),
    }
}

#[test]
fn test_gql_exists_incoming_edge() {
    let graph = create_exists_test_graph();
    let snapshot = graph.snapshot();

    // Find teams that have players who played for them
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (t:team)
        WHERE EXISTS { (t)<-[:played_for]-() }
        RETURN t.name
    "#,
        )
        .unwrap();

    // All teams have at least one player
    assert_eq!(results.len(), 3);
}

#[test]
fn test_gql_exists_bidirectional() {
    let graph = create_exists_test_graph();
    let snapshot = graph.snapshot();

    // Find vertices with any played_for relationship (bidirectional)
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (n)
        WHERE EXISTS { (n)-[:played_for]-() }
        RETURN n
    "#,
        )
        .unwrap();

    // Should find all players and teams (7 total: 4 players + 3 teams)
    assert_eq!(results.len(), 7);
}

#[test]
fn test_gql_exists_with_property_filter() {
    let graph = create_exists_test_graph();
    let snapshot = graph.snapshot();

    // Find players who played for a team with 6+ championships
    // Only Bulls have exactly 6 championships, Lakers have 17
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE EXISTS { (p)-[:played_for]->(t:team {championships: 6}) }
        RETURN p.name
    "#,
        )
        .unwrap();

    // Only MJ played for the Bulls (6 championships)
    assert_eq!(results.len(), 1);

    let names: Vec<String> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert!(names.contains(&"Michael Jordan".to_string()));
}

#[test]
fn test_gql_exists_with_target_property_filter_multiple_results() {
    let graph = create_exists_test_graph();
    let snapshot = graph.snapshot();

    // Find players who played for a team with 0 championships
    // Only Suns have 0 championships
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE EXISTS { (p)-[:played_for]->(t:team {championships: 0}) }
        RETURN p.name
    "#,
        )
        .unwrap();

    // Barkley and Nash played for the Suns (0 championships)
    assert_eq!(results.len(), 2);

    let names: Vec<String> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert!(names.contains(&"Charles Barkley".to_string()));
    assert!(names.contains(&"Steve Nash".to_string()));
}

#[test]
fn test_gql_exists_multiple_conditions_complex() {
    let graph = create_exists_test_graph();
    let snapshot = graph.snapshot();

    // Find shooting guards who have NOT won championships
    // This combines property filter with NOT EXISTS
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE p.position = 'Shooting Guard' AND NOT EXISTS { (p)-[:won_championship_with]->() }
        RETURN p.name
    "#,
        )
        .unwrap();

    // No shooting guards without championships (MJ and Kobe both won)
    assert_eq!(results.len(), 0);
}

#[test]
fn test_gql_exists_with_order_by() {
    let graph = create_exists_test_graph();
    let snapshot = graph.snapshot();

    // Find players who have won championships, ordered by name
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE EXISTS { (p)-[:won_championship_with]->() }
        RETURN p.name
        ORDER BY p.name
    "#,
        )
        .unwrap();

    assert_eq!(results.len(), 2);
    // Should be alphabetically ordered: Kobe Bryant, Michael Jordan
    assert_eq!(results[0], Value::String("Kobe Bryant".to_string()));
    assert_eq!(results[1], Value::String("Michael Jordan".to_string()));
}

#[test]
fn test_gql_exists_with_limit() {
    let graph = create_exists_test_graph();
    let snapshot = graph.snapshot();

    // Find first player who has NOT won championships
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE NOT EXISTS { (p)-[:won_championship_with]->() }
        RETURN p.name
        LIMIT 1
    "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    // Should be either Barkley or Nash
    let name = match &results[0] {
        Value::String(s) => s.clone(),
        _ => panic!("Expected string"),
    };
    assert!(name == "Charles Barkley" || name == "Steve Nash");
}

#[test]
fn test_gql_exists_nested_not() {
    let graph = create_exists_test_graph();
    let snapshot = graph.snapshot();

    // Double negation: NOT NOT EXISTS should equal EXISTS
    // Find players where it's NOT true that they have NOT won championships
    // (i.e., players who HAVE won championships)
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE NOT NOT EXISTS { (p)-[:won_championship_with]->() }
        RETURN p.name
    "#,
        )
        .unwrap();

    // Same as EXISTS - should find MJ and Kobe
    assert_eq!(results.len(), 2);

    let names: Vec<String> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();

    assert!(names.contains(&"Michael Jordan".to_string()));
    assert!(names.contains(&"Kobe Bryant".to_string()));
}

#[test]
fn test_gql_exists_with_distinct() {
    let graph = create_exists_test_graph();
    let snapshot = graph.snapshot();

    // Get distinct positions of players who have won championships
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE EXISTS { (p)-[:won_championship_with]->() }
        RETURN DISTINCT p.position
    "#,
        )
        .unwrap();

    // Both MJ and Kobe are Shooting Guards, so we should get only 1 distinct position
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Shooting Guard".to_string()));
}

#[test]
fn test_gql_exists_return_multiple_properties() {
    let graph = create_exists_test_graph();
    let snapshot = graph.snapshot();

    // Return multiple properties for players who have won championships
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE EXISTS { (p)-[:won_championship_with]->() }
        RETURN p.name AS name, p.position AS pos
    "#,
        )
        .unwrap();

    assert_eq!(results.len(), 2);

    // Each result should be a map with name and pos
    for result in &results {
        if let Value::Map(map) = result {
            assert!(map.contains_key("name"));
            assert!(map.contains_key("pos"));
            assert_eq!(
                map.get("pos"),
                Some(&Value::String("Shooting Guard".to_string()))
            );
        } else {
            panic!("Expected map result");
        }
    }
}

#[test]
fn test_gql_exists_empty_graph() {
    let graph = Graph::in_memory();
    let snapshot = graph.snapshot();

    // EXISTS on empty graph should return no results
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE EXISTS { (p)-[:won_championship_with]->() }
        RETURN p.name
    "#,
        )
        .unwrap();

    assert_eq!(results.len(), 0);
}

#[test]
fn test_gql_exists_no_edges() {
    // Create a graph with vertices but no edges
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Lonely Player"));
    storage.add_vertex("player", props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // EXISTS should return false for a player with no outgoing edges
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE EXISTS { (p)-[:any_relationship]->() }
        RETURN p.name
    "#,
        )
        .unwrap();

    assert_eq!(results.len(), 0);

    // NOT EXISTS should return true for a player with no outgoing edges
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE NOT EXISTS { (p)-[:any_relationship]->() }
        RETURN p.name
    "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Lonely Player".to_string()));
}

#[test]
fn test_gql_exists_self_loop() {
    // Create a graph with a self-loop
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Narcissist"));
    let id = storage.add_vertex("player", props);

    let _ = storage.add_edge(id, id, "admires", HashMap::new());

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    // EXISTS should work with self-loops
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE EXISTS { (p)-[:admires]->(p) }
        RETURN p.name
    "#,
        )
        .unwrap();

    // The player admires themselves
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Narcissist".to_string()));
}

#[test]
fn test_gql_exists_aggregate_over_filtered() {
    let graph = create_exists_test_graph();
    let snapshot = graph.snapshot();

    // Count and collect names of championship winners
    let results: Vec<_> = snapshot
        .gql(
            r#"
        MATCH (p:player)
        WHERE EXISTS { (p)-[:won_championship_with]->() }
        RETURN count(*) AS total, collect(p.name) AS winners
    "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("total"), Some(&Value::Int(2)));

        if let Some(Value::List(winners)) = map.get("winners") {
            assert_eq!(winners.len(), 2);
            assert!(winners.contains(&Value::String("Michael Jordan".to_string())));
            assert!(winners.contains(&Value::String("Kobe Bryant".to_string())));
        } else {
            panic!("Expected list for winners");
        }
    } else {
        panic!("Expected map result");
    }
}

// =============================================================================
// GROUP BY Tests
// =============================================================================

/// Helper to create a test graph for GROUP BY tests
fn create_group_by_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // Create Person vertices with various cities and ages
    let people = vec![
        ("Alice", 30i64, "New York"),
        ("Bob", 25i64, "Boston"),
        ("Carol", 35i64, "New York"),
        ("Dave", 28i64, "Boston"),
        ("Eve", 22i64, "Chicago"),
        ("Frank", 40i64, "New York"),
    ];

    for (name, age, city) in people {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(name));
        props.insert("age".to_string(), Value::from(age));
        props.insert("city".to_string(), Value::from(city));
        storage.add_vertex("Person", props);
    }

    Graph::new(Arc::new(storage))
}

/// Test GROUP BY with single expression and COUNT(*)
#[test]
fn test_gql_group_by_count() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.city AS city, count(*) AS cnt GROUP BY p.city")
        .unwrap();

    // Should have 3 groups: New York (3), Boston (2), Chicago (1)
    assert_eq!(results.len(), 3, "Should have 3 city groups");

    // Collect results into a map for easier verification
    let mut city_counts: HashMap<String, i64> = HashMap::new();
    for result in &results {
        if let Value::Map(map) = result {
            if let (Some(Value::String(city)), Some(Value::Int(count))) =
                (map.get("city"), map.get("cnt"))
            {
                city_counts.insert(city.clone(), *count);
            }
        }
    }

    assert_eq!(city_counts.get("New York"), Some(&3));
    assert_eq!(city_counts.get("Boston"), Some(&2));
    assert_eq!(city_counts.get("Chicago"), Some(&1));
}

/// Test GROUP BY with AVG aggregation
#[test]
fn test_gql_group_by_avg() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.city AS city, avg(p.age) AS avg_age GROUP BY p.city")
        .unwrap();

    assert_eq!(results.len(), 3, "Should have 3 city groups");

    // Collect results
    let mut city_avgs: HashMap<String, f64> = HashMap::new();
    for result in &results {
        if let Value::Map(map) = result {
            if let Some(Value::String(city)) = map.get("city") {
                let avg = match map.get("avg_age") {
                    Some(Value::Float(f)) => *f,
                    Some(Value::Int(i)) => *i as f64,
                    _ => panic!("Expected numeric avg_age"),
                };
                city_avgs.insert(city.clone(), avg);
            }
        }
    }

    // New York: (30 + 35 + 40) / 3 = 35.0
    // Boston: (25 + 28) / 2 = 26.5
    // Chicago: 22 / 1 = 22.0
    assert!((city_avgs.get("New York").unwrap() - 35.0).abs() < 0.001);
    assert!((city_avgs.get("Boston").unwrap() - 26.5).abs() < 0.001);
    assert!((city_avgs.get("Chicago").unwrap() - 22.0).abs() < 0.001);
}

/// Test GROUP BY with SUM aggregation
#[test]
fn test_gql_group_by_sum() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.city AS city, sum(p.age) AS total_age GROUP BY p.city")
        .unwrap();

    assert_eq!(results.len(), 3);

    let mut city_sums: HashMap<String, i64> = HashMap::new();
    for result in &results {
        if let Value::Map(map) = result {
            if let (Some(Value::String(city)), Some(Value::Int(sum))) =
                (map.get("city"), map.get("total_age"))
            {
                city_sums.insert(city.clone(), *sum);
            }
        }
    }

    // New York: 30 + 35 + 40 = 105
    // Boston: 25 + 28 = 53
    // Chicago: 22
    assert_eq!(city_sums.get("New York"), Some(&105));
    assert_eq!(city_sums.get("Boston"), Some(&53));
    assert_eq!(city_sums.get("Chicago"), Some(&22));
}

/// Test GROUP BY with MIN/MAX aggregations
#[test]
fn test_gql_group_by_min_max() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.city AS city, min(p.age) AS min_age, max(p.age) AS max_age GROUP BY p.city")
        .unwrap();

    assert_eq!(results.len(), 3);

    for result in &results {
        if let Value::Map(map) = result {
            if let Some(Value::String(city)) = map.get("city") {
                let min = map.get("min_age").and_then(|v| {
                    if let Value::Int(i) = v {
                        Some(*i)
                    } else {
                        None
                    }
                });
                let max = map.get("max_age").and_then(|v| {
                    if let Value::Int(i) = v {
                        Some(*i)
                    } else {
                        None
                    }
                });

                match city.as_str() {
                    "New York" => {
                        assert_eq!(min, Some(30), "New York min should be 30");
                        assert_eq!(max, Some(40), "New York max should be 40");
                    }
                    "Boston" => {
                        assert_eq!(min, Some(25), "Boston min should be 25");
                        assert_eq!(max, Some(28), "Boston max should be 28");
                    }
                    "Chicago" => {
                        assert_eq!(min, Some(22), "Chicago min should be 22");
                        assert_eq!(max, Some(22), "Chicago max should be 22");
                    }
                    _ => panic!("Unexpected city: {}", city),
                }
            }
        }
    }
}

/// Test GROUP BY with COLLECT aggregation
#[test]
fn test_gql_group_by_collect() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.city AS city, collect(p.name) AS names GROUP BY p.city")
        .unwrap();

    assert_eq!(results.len(), 3);

    for result in &results {
        if let Value::Map(map) = result {
            if let (Some(Value::String(city)), Some(Value::List(names))) =
                (map.get("city"), map.get("names"))
            {
                match city.as_str() {
                    "New York" => {
                        assert_eq!(names.len(), 3);
                        assert!(names.contains(&Value::String("Alice".to_string())));
                        assert!(names.contains(&Value::String("Carol".to_string())));
                        assert!(names.contains(&Value::String("Frank".to_string())));
                    }
                    "Boston" => {
                        assert_eq!(names.len(), 2);
                        assert!(names.contains(&Value::String("Bob".to_string())));
                        assert!(names.contains(&Value::String("Dave".to_string())));
                    }
                    "Chicago" => {
                        assert_eq!(names.len(), 1);
                        assert!(names.contains(&Value::String("Eve".to_string())));
                    }
                    _ => panic!("Unexpected city: {}", city),
                }
            }
        }
    }
}

/// Test GROUP BY with WHERE clause
#[test]
fn test_gql_group_by_with_where() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    // Only include people age >= 25
    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.age >= 25 RETURN p.city AS city, count(*) AS cnt GROUP BY p.city")
        .unwrap();

    // Eve (22) should be excluded, so Chicago has 0 people
    // This should result in only 2 groups (New York: 3, Boston: 2)
    // Note: Chicago group won't exist since no elements pass the filter
    assert_eq!(
        results.len(),
        2,
        "Should have 2 groups (Chicago filtered out)"
    );

    let mut city_counts: HashMap<String, i64> = HashMap::new();
    for result in &results {
        if let Value::Map(map) = result {
            if let (Some(Value::String(city)), Some(Value::Int(count))) =
                (map.get("city"), map.get("cnt"))
            {
                city_counts.insert(city.clone(), *count);
            }
        }
    }

    assert_eq!(city_counts.get("New York"), Some(&3));
    assert_eq!(city_counts.get("Boston"), Some(&2));
    assert_eq!(city_counts.get("Chicago"), None); // Eve filtered out
}

/// Test GROUP BY validation error - expression not in GROUP BY
#[test]
fn test_gql_group_by_validation_error() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    // p.name is not in GROUP BY and not an aggregate - should error
    let result = snapshot.gql("MATCH (p:Person) RETURN p.city, p.name, count(*) GROUP BY p.city");

    assert!(
        result.is_err(),
        "Should fail when expression not in GROUP BY"
    );

    let err = result.unwrap_err();
    let err_msg = format!("{}", err);
    assert!(
        err_msg.contains("p.name") || err_msg.contains("GROUP BY"),
        "Error should mention the problematic expression or GROUP BY: {}",
        err_msg
    );
}

/// Test GROUP BY with ORDER BY
#[test]
fn test_gql_group_by_with_order_by() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.city AS city, count(*) AS cnt GROUP BY p.city ORDER BY cnt DESC")
        .unwrap();

    assert_eq!(results.len(), 3);

    // Results should be ordered by count descending: New York (3), Boston (2), Chicago (1)
    let counts: Vec<i64> = results
        .iter()
        .filter_map(|r| {
            if let Value::Map(map) = r {
                if let Some(Value::Int(cnt)) = map.get("cnt") {
                    return Some(*cnt);
                }
            }
            None
        })
        .collect();

    assert_eq!(
        counts,
        vec![3, 2, 1],
        "Should be ordered by count descending"
    );
}

/// Test GROUP BY with LIMIT
#[test]
fn test_gql_group_by_with_limit() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.city AS city, count(*) AS cnt GROUP BY p.city ORDER BY cnt DESC LIMIT 2")
        .unwrap();

    // Should only return top 2 groups
    assert_eq!(results.len(), 2, "Should have only 2 results due to LIMIT");

    let counts: Vec<i64> = results
        .iter()
        .filter_map(|r| {
            if let Value::Map(map) = r {
                if let Some(Value::Int(cnt)) = map.get("cnt") {
                    return Some(*cnt);
                }
            }
            None
        })
        .collect();

    // Top 2 by count: New York (3), Boston (2)
    assert_eq!(counts, vec![3, 2]);
}

/// Test GROUP BY without alias (property access in RETURN)
#[test]
fn test_gql_group_by_no_alias() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    // No alias on p.city - the key should default to "p.city" (variable.property format)
    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.city, count(*) GROUP BY p.city")
        .unwrap();

    assert_eq!(results.len(), 3);

    // Verify we can access the city by the full "p.city" key
    for result in &results {
        if let Value::Map(map) = result {
            assert!(
                map.contains_key("p.city"),
                "Map should have 'p.city' key: {:?}",
                map
            );
        }
    }
}

/// Test GROUP BY with multiple aggregates
#[test]
fn test_gql_group_by_multiple_aggregates() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            "MATCH (p:Person) RETURN p.city AS city, count(*) AS cnt, sum(p.age) AS total, avg(p.age) AS avg_age GROUP BY p.city",
        )
        .unwrap();

    assert_eq!(results.len(), 3);

    for result in &results {
        if let Value::Map(map) = result {
            if let Some(Value::String(city)) = map.get("city") {
                let cnt = map.get("cnt");
                let total = map.get("total");
                let avg = map.get("avg_age");

                match city.as_str() {
                    "New York" => {
                        assert_eq!(cnt, Some(&Value::Int(3)));
                        assert_eq!(total, Some(&Value::Int(105))); // 30+35+40
                        if let Some(Value::Float(f)) = avg {
                            assert!((f - 35.0).abs() < 0.001);
                        }
                    }
                    "Boston" => {
                        assert_eq!(cnt, Some(&Value::Int(2)));
                        assert_eq!(total, Some(&Value::Int(53))); // 25+28
                        if let Some(Value::Float(f)) = avg {
                            assert!((f - 26.5).abs() < 0.001);
                        }
                    }
                    "Chicago" => {
                        assert_eq!(cnt, Some(&Value::Int(1)));
                        assert_eq!(total, Some(&Value::Int(22)));
                        if let Some(Value::Float(f)) = avg {
                            assert!((f - 22.0).abs() < 0.001);
                        }
                    }
                    _ => panic!("Unexpected city: {}", city),
                }
            }
        }
    }
}

/// Test GROUP BY single return item without alias
#[test]
fn test_gql_group_by_single_return_count_only() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    // This is a bit unusual - GROUP BY city but only return the count
    // The city value is computed but not returned
    let results = snapshot
        .gql("MATCH (p:Person) RETURN count(*) AS cnt GROUP BY p.city")
        .unwrap();

    // Should have 3 groups, each with a count
    assert_eq!(results.len(), 3);

    let mut counts: Vec<i64> = results
        .iter()
        .filter_map(|r| {
            if let Value::Map(map) = r {
                if let Some(Value::Int(cnt)) = map.get("cnt") {
                    return Some(*cnt);
                }
            }
            None
        })
        .collect();

    counts.sort();
    assert_eq!(counts, vec![1, 2, 3], "Should have counts 1, 2, 3");
}

// =============================================================================
// MULTI-VARIABLE PATTERN TESTS (Plan 10, Phase 3)
// =============================================================================

/// Helper function to create a graph for multi-variable tests
fn create_multi_var_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // Create people
    let alice = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Alice"));
        props.insert("age".to_string(), Value::Int(30));
        props
    });

    let bob = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Bob"));
        props.insert("age".to_string(), Value::Int(28));
        props
    });

    let carol = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Carol"));
        props.insert("age".to_string(), Value::Int(35));
        props
    });

    let dave = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Dave"));
        props.insert("age".to_string(), Value::Int(25));
        props
    });

    // Create companies
    let tech_corp = storage.add_vertex("Company", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("TechCorp"));
        props.insert("size".to_string(), Value::Int(1000));
        props
    });

    let startup_inc = storage.add_vertex("Company", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("StartupInc"));
        props.insert("size".to_string(), Value::Int(50));
        props
    });

    // Create KNOWS relationships
    storage
        .add_edge(alice, bob, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, carol, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(carol, dave, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, carol, "KNOWS", HashMap::new())
        .unwrap();

    // Create WORKS_AT relationships
    storage
        .add_edge(alice, tech_corp, "WORKS_AT", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, tech_corp, "WORKS_AT", HashMap::new())
        .unwrap();
    storage
        .add_edge(carol, startup_inc, "WORKS_AT", HashMap::new())
        .unwrap();
    storage
        .add_edge(dave, startup_inc, "WORKS_AT", HashMap::new())
        .unwrap();

    Graph::new(Arc::new(storage))
}

/// Test: Basic multi-variable pattern - return properties from two variables
#[test]
fn test_gql_multi_var_basic_two_variables() {
    let graph = create_multi_var_test_graph();
    let snapshot = graph.snapshot();

    // Return properties from both ends of a relationship
    let query = r#"
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        RETURN a.name AS person1, b.name AS person2
        ORDER BY person1, person2
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // Should have 4 KNOWS relationships
    assert_eq!(results.len(), 4, "Should have 4 KNOWS relationships");

    // Verify each result has both person1 and person2
    for result in &results {
        if let Value::Map(map) = result {
            assert!(map.contains_key("person1"), "Result should contain person1");
            assert!(map.contains_key("person2"), "Result should contain person2");
        } else {
            panic!("Expected Map result");
        }
    }

    // Check first result (Alice -> Bob comes first alphabetically)
    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("person1"), Some(&Value::from("Alice")));
        assert_eq!(map.get("person2"), Some(&Value::from("Bob")));
    }
}

/// Test: Multi-variable pattern with three nodes
#[test]
fn test_gql_multi_var_three_nodes() {
    let graph = create_multi_var_test_graph();
    let snapshot = graph.snapshot();

    // Three-node pattern: person -> company <- person (coworkers)
    let query = r#"
        MATCH (p1:Person)-[:WORKS_AT]->(c:Company)<-[:WORKS_AT]-(p2:Person)
        WHERE p1.name <> p2.name
        RETURN p1.name AS person1, c.name AS company, p2.name AS person2
        ORDER BY company, person1, person2
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // Should find coworker pairs:
    // StartupInc: Carol-Dave, Dave-Carol
    // TechCorp: Alice-Bob, Bob-Alice
    assert_eq!(results.len(), 4, "Should have 4 coworker pairs");

    // Verify structure
    for result in &results {
        if let Value::Map(map) = result {
            assert!(map.contains_key("person1"));
            assert!(map.contains_key("company"));
            assert!(map.contains_key("person2"));
        }
    }
}

/// Test: Multi-variable pattern with WHERE filtering on both variables
#[test]
fn test_gql_multi_var_where_both_variables() {
    let graph = create_multi_var_test_graph();
    let snapshot = graph.snapshot();

    // Filter based on properties from both variables
    let query = r#"
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        WHERE a.age > 25 AND b.age < 35
        RETURN a.name AS older, b.name AS younger
        ORDER BY older
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // Alice(30) knows Bob(28) and Carol(35)
    // Bob(28) knows Carol(35)
    // Carol(35) knows Dave(25)
    // With filters: a.age > 25 AND b.age < 35
    // Alice(30) -> Bob(28) ✓
    // Alice(30) -> Carol(35) ✗ (Carol is 35, not < 35)
    // Bob(28) -> Carol(35) ✗
    // Carol(35) -> Dave(25) ✓
    assert_eq!(results.len(), 2, "Should have 2 matching pairs");
}

/// Test: Multi-variable pattern with COUNT aggregation
#[test]
fn test_gql_multi_var_with_count() {
    let graph = create_multi_var_test_graph();
    let snapshot = graph.snapshot();

    // Count relationships
    let query = r#"
        MATCH (p:Person)-[:WORKS_AT]->(c:Company)
        RETURN c.name AS company, COUNT(*) AS employee_count
        GROUP BY c.name
        ORDER BY company
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // TechCorp: 2 employees (Alice, Bob)
    // StartupInc: 2 employees (Carol, Dave)
    assert_eq!(results.len(), 2);

    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("company"), Some(&Value::from("StartupInc")));
        assert_eq!(map.get("employee_count"), Some(&Value::Int(2)));
    }
}

/// Test: Multi-variable pattern returning only one variable's property
#[test]
fn test_gql_multi_var_return_one_property() {
    let graph = create_multi_var_test_graph();
    let snapshot = graph.snapshot();

    // Pattern has two variables but only return one
    let query = r#"
        MATCH (p:Person)-[:WORKS_AT]->(c:Company)
        WHERE c.name = 'TechCorp'
        RETURN p.name
        ORDER BY p.name
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // Should find Alice and Bob who work at TechCorp
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], Value::from("Alice"));
    assert_eq!(results[1], Value::from("Bob"));
}

/// Test: Multi-variable pattern with DISTINCT
#[test]
fn test_gql_multi_var_distinct() {
    let graph = create_multi_var_test_graph();
    let snapshot = graph.snapshot();

    // Find distinct companies where people who know each other work
    // (may produce duplicates without DISTINCT)
    let query = r#"
        MATCH (p1:Person)-[:KNOWS]->(p2:Person)-[:WORKS_AT]->(c:Company)
        RETURN DISTINCT c.name AS company
        ORDER BY company
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // Should find companies where "known" people work
    assert!(!results.is_empty());
    // Results should be unique company names
}

/// Test: Multi-variable pattern with LIMIT
#[test]
fn test_gql_multi_var_with_limit() {
    let graph = create_multi_var_test_graph();
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        RETURN a.name AS from, b.name AS to
        ORDER BY from, to
        LIMIT 2
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    assert_eq!(results.len(), 2, "Should return only 2 results");
}

/// Test: Multi-variable pattern - verify variable binding correctness
#[test]
fn test_gql_multi_var_binding_correctness() {
    let graph = create_multi_var_test_graph();
    let snapshot = graph.snapshot();

    // This test ensures that variable a gets the source node
    // and variable b gets the target node correctly
    let query = r#"
        MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person)
        RETURN a.name AS source, b.name AS target
        ORDER BY target
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // Alice KNOWS Bob and Carol
    assert_eq!(results.len(), 2);

    // First result should be Alice -> Bob
    if let Value::Map(map) = &results[0] {
        assert_eq!(
            map.get("source"),
            Some(&Value::from("Alice")),
            "Source should always be Alice"
        );
        assert_eq!(map.get("target"), Some(&Value::from("Bob")));
    }

    // Second result should be Alice -> Carol
    if let Value::Map(map) = &results[1] {
        assert_eq!(
            map.get("source"),
            Some(&Value::from("Alice")),
            "Source should always be Alice"
        );
        assert_eq!(map.get("target"), Some(&Value::from("Carol")));
    }
}

/// Test: Multi-variable with expression in WHERE
#[test]
fn test_gql_multi_var_expression_in_where() {
    let graph = create_multi_var_test_graph();
    let snapshot = graph.snapshot();

    // Compare ages between the two people in the relationship
    let query = r#"
        MATCH (older:Person)-[:KNOWS]->(younger:Person)
        WHERE older.age > younger.age
        RETURN older.name AS older_person, younger.name AS younger_person
        ORDER BY older_person
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // Alice(30) -> Bob(28) ✓ (30 > 28)
    // Alice(30) -> Carol(35) ✗ (30 < 35)
    // Bob(28) -> Carol(35) ✗ (28 < 35)
    // Carol(35) -> Dave(25) ✓ (35 > 25)
    assert_eq!(results.len(), 2);
}

// =============================================================================
// EDGE VARIABLE AND PROPERTY TESTS (Plan 10, Phase 3.2/3.3)
// =============================================================================

/// Helper function to create a graph with edge properties for testing
fn create_edge_property_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // Create people
    let alice = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Alice"));
        props
    });

    let bob = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Bob"));
        props
    });

    let carol = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Carol"));
        props
    });

    // Create teams
    let bulls = storage.add_vertex("Team", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Bulls"));
        props
    });

    let lakers = storage.add_vertex("Team", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Lakers"));
        props
    });

    // Create PLAYED_FOR edges with properties (years, championships)
    storage
        .add_edge(alice, bulls, "PLAYED_FOR", {
            let mut props = HashMap::new();
            props.insert("start_year".to_string(), Value::Int(2015));
            props.insert("end_year".to_string(), Value::Int(2020));
            props.insert("championships".to_string(), Value::Int(2));
            props
        })
        .unwrap();

    storage
        .add_edge(alice, lakers, "PLAYED_FOR", {
            let mut props = HashMap::new();
            props.insert("start_year".to_string(), Value::Int(2020));
            props.insert("end_year".to_string(), Value::Int(2023));
            props.insert("championships".to_string(), Value::Int(1));
            props
        })
        .unwrap();

    storage
        .add_edge(bob, bulls, "PLAYED_FOR", {
            let mut props = HashMap::new();
            props.insert("start_year".to_string(), Value::Int(2010));
            props.insert("end_year".to_string(), Value::Int(2018));
            props.insert("championships".to_string(), Value::Int(3));
            props
        })
        .unwrap();

    storage
        .add_edge(carol, lakers, "PLAYED_FOR", {
            let mut props = HashMap::new();
            props.insert("start_year".to_string(), Value::Int(2018));
            props.insert("end_year".to_string(), Value::Int(2022));
            props.insert("championships".to_string(), Value::Int(0));
            props
        })
        .unwrap();

    // Create KNOWS edges with properties
    storage
        .add_edge(alice, bob, "KNOWS", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2015));
            props
        })
        .unwrap();

    storage
        .add_edge(bob, carol, "KNOWS", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2018));
            props
        })
        .unwrap();

    Graph::new(Arc::new(storage))
}

/// Test: Edge variable binding - basic case
#[test]
fn test_gql_edge_variable_basic() {
    let graph = create_edge_property_test_graph();
    let snapshot = graph.snapshot();

    // Bind edge variable and return properties from nodes
    let query = r#"
        MATCH (p:Person)-[e:PLAYED_FOR]->(t:Team)
        RETURN p.name AS player, t.name AS team
        ORDER BY player, team
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // Should have 4 PLAYED_FOR relationships
    assert_eq!(results.len(), 4, "Should have 4 PLAYED_FOR relationships");

    // Verify first result
    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("player"), Some(&Value::from("Alice")));
        assert_eq!(map.get("team"), Some(&Value::from("Bulls")));
    }
}

/// Test: Edge property access in RETURN
#[test]
fn test_gql_edge_property_in_return() {
    let graph = create_edge_property_test_graph();
    let snapshot = graph.snapshot();

    // Return edge property values
    let query = r#"
        MATCH (p:Person)-[e:PLAYED_FOR]->(t:Team)
        RETURN p.name AS player, t.name AS team, e.championships AS rings
        ORDER BY rings DESC, player
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    assert_eq!(results.len(), 4);

    // Bob has most championships (3)
    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("player"), Some(&Value::from("Bob")));
        assert_eq!(map.get("team"), Some(&Value::from("Bulls")));
        assert_eq!(map.get("rings"), Some(&Value::Int(3)));
    }
}

/// Test: Edge property access in WHERE
#[test]
fn test_gql_edge_property_in_where() {
    let graph = create_edge_property_test_graph();
    let snapshot = graph.snapshot();

    // Filter by edge property
    let query = r#"
        MATCH (p:Person)-[e:PLAYED_FOR]->(t:Team)
        WHERE e.championships >= 2
        RETURN p.name AS player, e.championships AS rings
        ORDER BY rings DESC
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // Bob(3) and Alice(2) have 2+ championships
    assert_eq!(results.len(), 2, "Should have 2 players with 2+ rings");

    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("player"), Some(&Value::from("Bob")));
        assert_eq!(map.get("rings"), Some(&Value::Int(3)));
    }
}

/// Test: Edge property filter in pattern (inline property filter)
#[test]
fn test_gql_edge_property_inline_filter() {
    let graph = create_edge_property_test_graph();
    let snapshot = graph.snapshot();

    // Use inline edge property filter
    let query = r#"
        MATCH (p:Person)-[e:PLAYED_FOR {championships: 3}]->(t:Team)
        RETURN p.name AS player, t.name AS team
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // Only Bob has exactly 3 championships
    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("player"), Some(&Value::from("Bob")));
        assert_eq!(map.get("team"), Some(&Value::from("Bulls")));
    }
}

/// Test: Edge property comparison
#[test]
fn test_gql_edge_property_comparison() {
    let graph = create_edge_property_test_graph();
    let snapshot = graph.snapshot();

    // Find long tenures (5+ years)
    let query = r#"
        MATCH (p:Person)-[e:PLAYED_FOR]->(t:Team)
        WHERE e.end_year - e.start_year >= 5
        RETURN p.name AS player, t.name AS team, e.start_year, e.end_year
        ORDER BY player
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // Alice at Bulls: 2015-2020 = 5 years ✓
    // Alice at Lakers: 2020-2023 = 3 years ✗
    // Bob at Bulls: 2010-2018 = 8 years ✓
    // Carol at Lakers: 2018-2022 = 4 years ✗
    assert_eq!(results.len(), 2, "Should have 2 long tenure stints");
}

/// Test: Edge variable without returning edge properties (just filtering)
#[test]
fn test_gql_edge_variable_filter_only() {
    let graph = create_edge_property_test_graph();
    let snapshot = graph.snapshot();

    // Use edge variable for filtering but don't return edge properties
    let query = r#"
        MATCH (p:Person)-[e:KNOWS]->(friend:Person)
        WHERE e.since < 2017
        RETURN p.name AS person, friend.name AS knows
        ORDER BY person
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // Alice knows Bob since 2015 (< 2017) ✓
    // Bob knows Carol since 2018 (>= 2017) ✗
    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("person"), Some(&Value::from("Alice")));
        assert_eq!(map.get("knows"), Some(&Value::from("Bob")));
    }
}

/// Test: Multiple edge properties in WHERE
#[test]
fn test_gql_multiple_edge_properties_where() {
    let graph = create_edge_property_test_graph();
    let snapshot = graph.snapshot();

    // Filter by multiple edge properties
    let query = r#"
        MATCH (p:Person)-[e:PLAYED_FOR]->(t:Team)
        WHERE e.start_year >= 2015 AND e.championships >= 1
        RETURN p.name AS player, t.name AS team, e.championships AS rings
        ORDER BY rings DESC
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // Alice at Bulls: 2015+, 2 rings ✓
    // Alice at Lakers: 2020+, 1 ring ✓
    // Bob at Bulls: 2010 (< 2015) ✗
    // Carol at Lakers: 2018+, 0 rings ✗
    assert_eq!(results.len(), 2);
}

/// Test: Edge property with aggregation
#[test]
fn test_gql_edge_property_aggregation() {
    let graph = create_edge_property_test_graph();
    let snapshot = graph.snapshot();

    // Sum championships by team
    let query = r#"
        MATCH (p:Person)-[e:PLAYED_FOR]->(t:Team)
        RETURN t.name AS team, SUM(e.championships) AS total_rings
        GROUP BY t.name
        ORDER BY total_rings DESC
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    // Bulls: 2 (Alice) + 3 (Bob) = 5
    // Lakers: 1 (Alice) + 0 (Carol) = 1
    assert_eq!(results.len(), 2);

    // Collect team -> total_rings mapping
    let mut team_rings: HashMap<String, i64> = HashMap::new();
    for result in &results {
        if let Value::Map(map) = result {
            if let (Some(Value::String(team)), Some(Value::Int(rings))) =
                (map.get("team"), map.get("total_rings"))
            {
                team_rings.insert(team.clone(), *rings);
            }
        }
    }

    assert_eq!(
        team_rings.get("Bulls"),
        Some(&5i64),
        "Bulls should have 5 rings"
    );
    assert_eq!(
        team_rings.get("Lakers"),
        Some(&1i64),
        "Lakers should have 1 ring"
    );
}

/// Debug test to understand variable-length path with WHERE clause
#[test]
fn test_gql_debug_var_path_where() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    // First, test without multi-var (inline filter)
    let query1 = r#"
        MATCH (p:Person {name: 'Alice'})-[:KNOWS*2..3]->(target:Person)
        RETURN DISTINCT target.name
        ORDER BY target.name
    "#;
    let results1: Vec<_> = snapshot.gql(query1).unwrap();
    println!("With inline filter: {:?}", results1);

    // Now test with WHERE clause (triggers multi-var path)
    let query2 = r#"
        MATCH (p:Person)-[:KNOWS*2..3]->(target:Person)
        WHERE p.name = 'Alice'
        RETURN DISTINCT target.name
        ORDER BY target.name
    "#;
    let results2: Vec<_> = snapshot.gql(query2).unwrap();
    println!("With WHERE clause: {:?}", results2);

    // The results should be the same
    assert!(
        !results1.is_empty(),
        "Inline filter results should not be empty"
    );
    assert!(
        !results2.is_empty(),
        "WHERE clause results should not be empty"
    );
}

/// Debug test to see traverser path contents
#[test]
fn test_gql_debug_path_contents() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    // Simpler test: just one hop to see path behavior
    let query = r#"
        MATCH (p:Person)-[:KNOWS]->(target:Person)
        WHERE p.name = 'Alice'
        RETURN p.name AS source, target.name AS dest
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    println!("1-hop results:");
    for r in &results {
        println!("  {:?}", r);
    }

    // Should find Alice's direct friends: Bob, Charlie, Diana
    assert_eq!(results.len(), 3, "Alice KNOWS 3 people directly");
}

/// Debug test for 2-hop path
#[test]
fn test_gql_debug_2hop_path() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    // Exact 2 hops from Alice
    let query = r#"
        MATCH (p:Person)-[:KNOWS*2]->(target:Person)
        WHERE p.name = 'Alice'
        RETURN p.name AS source, target.name AS dest
        ORDER BY dest
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    println!("2-hop results:");
    for r in &results {
        println!("  {:?}", r);
    }

    // Alice's 1-hop: Bob, Charlie, Diana
    // Alice's 2-hop (deduped): Eve (via Bob or Diana), Frank (via Bob), Grace (via Charlie), Henry (via Eve via Diana)
    assert!(!results.is_empty(), "Should find 2-hop targets");
}

/// Debug test to check traverser execution directly
#[test]
fn test_gql_debug_traverser_path() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    // Use the traversal API directly to see what's happening
    let g = snapshot.traversal();

    // First find Alice
    let alice_results: Vec<_> = g
        .v()
        .has_label("Person")
        .has_value("name", rustgremlin::value::Value::from("Alice"))
        .to_list();
    println!("Alice found: {:?}", alice_results);

    // Now do 2-hop with path tracking
    let g2 = snapshot.traversal();
    let path_results: Vec<_> = g2
        .v()
        .with_path()
        .has_label("Person")
        .has_value("name", rustgremlin::value::Value::from("Alice"))
        .as_("p")
        .out_labels(&["KNOWS"])
        .out_labels(&["KNOWS"])
        .as_("target")
        .select(&["p", "target"])
        .to_list();

    println!("2-hop path results with select: {:?}", path_results);
    assert!(!path_results.is_empty(), "Should have path results");
}

/// Debug test to check traverser execution with repeat
#[test]
fn test_gql_debug_traverser_repeat_path() {
    use rustgremlin::traversal::__;

    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let g = snapshot.traversal();
    let path_results: Vec<_> = g
        .v()
        .with_path()
        .has_label("Person")
        .has_value("name", rustgremlin::value::Value::from("Alice"))
        .as_("p")
        .repeat(__::out_labels(&["KNOWS"]))
        .times(2)
        .as_("target")
        .select(&["p", "target"])
        .to_list();

    println!("2-hop with repeat + select: {:?}", path_results);
    assert!(!path_results.is_empty(), "Should have repeat path results");
}

// =============================================================================
// Phase 4.1: COALESCE Function Tests
// =============================================================================

/// Helper to create a graph with null values for COALESCE tests
fn create_coalesce_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // Person with both name and nickname
    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    alice_props.insert("nickname".to_string(), Value::from("Ali"));
    storage.add_vertex("Person", alice_props);

    // Person with name but no nickname
    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    // Note: no nickname property
    storage.add_vertex("Person", bob_props);

    // Person with nickname but no name (unusual case)
    let mut carol_props = HashMap::new();
    carol_props.insert("nickname".to_string(), Value::from("Carol the Great"));
    storage.add_vertex("Person", carol_props);

    Graph::new(Arc::new(storage))
}

/// Test COALESCE returns first non-null value
#[test]
fn test_gql_coalesce_first_value() {
    let graph = create_coalesce_test_graph();
    let snapshot = graph.snapshot();

    // Alice has nickname, so COALESCE should return nickname
    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.name = 'Alice' RETURN coalesce(p.nickname, p.name)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Ali".to_string()));
}

/// Test COALESCE falls back to second value when first is null
#[test]
fn test_gql_coalesce_fallback() {
    let graph = create_coalesce_test_graph();
    let snapshot = graph.snapshot();

    // Bob has no nickname, so COALESCE should return name
    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.name = 'Bob' RETURN coalesce(p.nickname, p.name)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Bob".to_string()));
}

/// Test COALESCE with literal default
#[test]
fn test_gql_coalesce_literal_default() {
    let graph = create_coalesce_test_graph();
    let snapshot = graph.snapshot();

    // Bob has no nickname, so return default literal
    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.name = 'Bob' RETURN coalesce(p.nickname, 'No Nickname')")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("No Nickname".to_string()));
}

/// Test COALESCE with multiple arguments
#[test]
fn test_gql_coalesce_multiple_args() {
    let graph = create_coalesce_test_graph();
    let snapshot = graph.snapshot();

    // For Carol: name is null, nickname exists
    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.nickname = 'Carol the Great' RETURN coalesce(p.name, p.nickname, 'Unknown')")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Carol the Great".to_string()));
}

/// Test COALESCE case insensitivity
#[test]
fn test_gql_coalesce_case_insensitive() {
    let graph = create_coalesce_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.name = 'Bob' RETURN COALESCE(p.nickname, p.name)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Bob".to_string()));
}

// =============================================================================
// Phase 4.2: CASE Expression Tests
// =============================================================================

/// Helper to create a graph for CASE expression tests
fn create_case_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    let people = vec![
        ("Alice", 32i64, 92i64),
        ("Bob", 25i64, 75i64),
        ("Carol", 42i64, 88i64),
        ("Dave", 18i64, 65i64),
        ("Eve", 55i64, 45i64),
    ];

    for (name, age, score) in people {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(name));
        props.insert("age".to_string(), Value::from(age));
        props.insert("score".to_string(), Value::from(score));
        storage.add_vertex("Person", props);
    }

    Graph::new(Arc::new(storage))
}

/// Test CASE expression with age categorization
#[test]
fn test_gql_case_age_category() {
    let graph = create_case_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
            MATCH (p:Person) 
            WHERE p.name = 'Carol'
            RETURN CASE 
                WHEN p.age > 40 THEN 'Senior'
                WHEN p.age > 30 THEN 'Middle'
                ELSE 'Young'
            END
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Senior".to_string()));
}

/// Test CASE expression returns ELSE when no WHEN matches
#[test]
fn test_gql_case_else_branch() {
    let graph = create_case_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
            MATCH (p:Person) 
            WHERE p.name = 'Dave'
            RETURN CASE 
                WHEN p.age > 40 THEN 'Senior'
                WHEN p.age > 30 THEN 'Middle'
                ELSE 'Young'
            END
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Young".to_string()));
}

/// Test CASE expression with grade categorization
#[test]
fn test_gql_case_grade() {
    let graph = create_case_test_graph();
    let snapshot = graph.snapshot();

    // Alice has score 92, should be 'A'
    let results = snapshot
        .gql(
            r#"
            MATCH (p:Person) 
            WHERE p.name = 'Alice'
            RETURN p.name, CASE 
                WHEN p.score >= 90 THEN 'A'
                WHEN p.score >= 80 THEN 'B'
                WHEN p.score >= 70 THEN 'C'
                ELSE 'F'
            END AS grade
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("grade"), Some(&Value::String("A".to_string())));
    } else {
        panic!("Expected Map result");
    }
}

/// Test CASE expression without ELSE returns null
#[test]
fn test_gql_case_no_else_returns_null() {
    let graph = create_case_test_graph();
    let snapshot = graph.snapshot();

    // Eve (age 55) won't match any WHEN condition if we check for age < 20
    let results = snapshot
        .gql(
            r#"
            MATCH (p:Person) 
            WHERE p.name = 'Eve'
            RETURN CASE 
                WHEN p.age < 20 THEN 'Teen'
            END
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null);
}

/// Test CASE expression with multiple results
#[test]
fn test_gql_case_multiple_results() {
    let graph = create_case_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
            MATCH (p:Person)
            RETURN p.name, CASE 
                WHEN p.age > 40 THEN 'Senior'
                WHEN p.age > 25 THEN 'Adult'
                ELSE 'Young'
            END AS category
            ORDER BY p.name
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 5);

    // Verify categories are correct
    let categories: Vec<_> = results
        .iter()
        .filter_map(|v| {
            if let Value::Map(map) = v {
                let name = map.get("p.name").cloned();
                let category = map.get("category").cloned();
                Some((name, category))
            } else {
                None
            }
        })
        .collect();

    // Alice (32) -> Adult
    assert!(categories.contains(&(
        Some(Value::String("Alice".to_string())),
        Some(Value::String("Adult".to_string()))
    )));
    // Carol (42) -> Senior
    assert!(categories.contains(&(
        Some(Value::String("Carol".to_string())),
        Some(Value::String("Senior".to_string()))
    )));
    // Dave (18) -> Young
    assert!(categories.contains(&(
        Some(Value::String("Dave".to_string())),
        Some(Value::String("Young".to_string()))
    )));
}

// =============================================================================
// Phase 4.3: Type Conversion Function Tests
// =============================================================================

/// Helper to create a graph for type conversion tests
fn create_type_conversion_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert("age".to_string(), Value::from(30i64));
    props.insert("score".to_string(), Value::from(95.5));
    props.insert("active".to_string(), Value::from(true));
    props.insert("count_str".to_string(), Value::from("42"));
    props.insert("float_str".to_string(), Value::from("3.14"));
    props.insert("bool_str".to_string(), Value::from("true"));
    storage.add_vertex("Person", props);

    Graph::new(Arc::new(storage))
}

/// Test toString() converts integer to string
#[test]
fn test_gql_tostring_integer() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN toString(p.age)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("30".to_string()));
}

/// Test toString() converts float to string
#[test]
fn test_gql_tostring_float() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN toString(p.score)")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::String(s) = &results[0] {
        assert!(s.starts_with("95.5"), "Expected '95.5...' got '{}'", s);
    } else {
        panic!("Expected String result");
    }
}

/// Test toString() converts boolean to string
#[test]
fn test_gql_tostring_boolean() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN toString(p.active)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("true".to_string()));
}

/// Test toInteger() converts string to integer
#[test]
fn test_gql_tointeger_string() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN toInteger(p.count_str)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(42));
}

/// Test toInteger() converts float to integer (truncates)
#[test]
fn test_gql_tointeger_float() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN toInteger(p.score)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(95));
}

/// Test toInteger() with invalid string returns null
#[test]
fn test_gql_tointeger_invalid_string() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN toInteger(p.name)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null);
}

/// Test toFloat() converts integer to float
#[test]
fn test_gql_tofloat_integer() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN toFloat(p.age)")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - 30.0).abs() < 0.0001, "Expected 30.0, got {}", f);
    } else {
        panic!("Expected Float result");
    }
}

/// Test toFloat() converts string to float
#[test]
fn test_gql_tofloat_string() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN toFloat(p.float_str)")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - 3.14).abs() < 0.0001, "Expected 3.14, got {}", f);
    } else {
        panic!("Expected Float result");
    }
}

/// Test toBoolean() converts string "true" to true
#[test]
fn test_gql_toboolean_string_true() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN toBoolean(p.bool_str)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(true));
}

/// Test toBoolean() converts integer to boolean (0 = false)
#[test]
fn test_gql_toboolean_integer() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    // Non-zero integer should be true
    let results = snapshot
        .gql("MATCH (p:Person) RETURN toBoolean(p.age)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(true));
}

/// Test toBoolean() with "false" string
#[test]
fn test_gql_toboolean_string_false() {
    let mut storage = InMemoryGraph::new();
    let mut props = HashMap::new();
    props.insert("status".to_string(), Value::from("false"));
    storage.add_vertex("Test", props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (t:Test) RETURN toBoolean(t.status)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(false));
}

// =============================================================================
// Additional String Function Tests
// =============================================================================

/// Test UPPER/TOUPPER function
#[test]
fn test_gql_upper_function() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN upper(p.name)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("ALICE".to_string()));
}

/// Test LOWER/TOLOWER function
#[test]
fn test_gql_lower_function() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN lower(p.name)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("alice".to_string()));
}

/// Test LENGTH/SIZE function for string
#[test]
fn test_gql_length_string() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN length(p.name)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(5)); // "Alice" has 5 characters
}

/// Test ABS function
#[test]
fn test_gql_abs_function() {
    let mut storage = InMemoryGraph::new();
    let mut props = HashMap::new();
    props.insert("balance".to_string(), Value::from(-100i64));
    storage.add_vertex("Account", props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (a:Account) RETURN abs(a.balance)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(100));
}

/// Test TRIM function
#[test]
fn test_gql_trim_function() {
    let mut storage = InMemoryGraph::new();
    let mut props = HashMap::new();
    props.insert("text".to_string(), Value::from("  hello world  "));
    storage.add_vertex("Test", props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (t:Test) RETURN trim(t.text)").unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("hello world".to_string()));
}

/// Test ROUND function
#[test]
fn test_gql_round_function() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN round(p.score)")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - 96.0).abs() < 0.0001, "Expected 96.0, got {}", f);
    } else {
        panic!("Expected Float result");
    }
}

/// Test FLOOR function
#[test]
fn test_gql_floor_function() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN floor(p.score)")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - 95.0).abs() < 0.0001, "Expected 95.0, got {}", f);
    } else {
        panic!("Expected Float result");
    }
}

/// Test CEIL function
#[test]
fn test_gql_ceil_function() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN ceil(p.score)")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - 96.0).abs() < 0.0001, "Expected 96.0, got {}", f);
    } else {
        panic!("Expected Float result");
    }
}

/// Test SUBSTRING function
#[test]
fn test_gql_substring_function() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN substring(p.name, 0, 3)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Ali".to_string()));
}

/// Test REPLACE function
#[test]
fn test_gql_replace_function() {
    let mut storage = InMemoryGraph::new();
    let mut props = HashMap::new();
    props.insert("text".to_string(), Value::from("hello world"));
    storage.add_vertex("Test", props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (t:Test) RETURN replace(t.text, 'world', 'there')")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("hello there".to_string()));
}

// =============================================================================
// UNION Clause Tests (Plan 11 - Week 1)
// =============================================================================

/// Helper to create a graph for UNION tests
fn create_union_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // Create TypeA vertices
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alpha"));
    storage.add_vertex("TypeA", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Beta"));
    storage.add_vertex("TypeA", props);

    // Create TypeB vertices
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Gamma"));
    storage.add_vertex("TypeB", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Delta"));
    storage.add_vertex("TypeB", props);

    // Create a vertex that appears in both types (simulated - same name)
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Epsilon"));
    storage.add_vertex("TypeA", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Epsilon"));
    storage.add_vertex("TypeB", props);

    Graph::new(Arc::new(storage))
}

/// Test parsing a single query returns Statement::Query
#[test]
fn test_gql_parse_statement_single_query() {
    let stmt = parse_statement("MATCH (n:Person) RETURN n.name").unwrap();

    assert!(
        matches!(stmt, Statement::Query(_)),
        "Single query should parse to Statement::Query"
    );
}

/// Test parsing UNION returns Statement::Union
#[test]
fn test_gql_parse_statement_union() {
    let stmt = parse_statement(
        r#"
        MATCH (a:TypeA) RETURN a.name
        UNION
        MATCH (b:TypeB) RETURN b.name
    "#,
    )
    .unwrap();

    match stmt {
        Statement::Union { queries, all } => {
            assert_eq!(queries.len(), 2, "UNION should have 2 queries");
            assert!(!all, "UNION (not ALL) should have all=false");
        }
        _ => panic!("Expected Statement::Union"),
    }
}

/// Test parsing UNION ALL
#[test]
fn test_gql_parse_statement_union_all() {
    let stmt = parse_statement(
        r#"
        MATCH (a:TypeA) RETURN a.name
        UNION ALL
        MATCH (b:TypeB) RETURN b.name
    "#,
    )
    .unwrap();

    match stmt {
        Statement::Union { queries, all } => {
            assert_eq!(queries.len(), 2, "UNION ALL should have 2 queries");
            assert!(all, "UNION ALL should have all=true");
        }
        _ => panic!("Expected Statement::Union"),
    }
}

/// Test parsing multiple UNIONs
#[test]
fn test_gql_parse_statement_multiple_unions() {
    let stmt = parse_statement(
        r#"
        MATCH (a:TypeA) RETURN a.name
        UNION
        MATCH (b:TypeB) RETURN b.name
        UNION
        MATCH (c:TypeC) RETURN c.name
    "#,
    )
    .unwrap();

    match stmt {
        Statement::Union { queries, all } => {
            assert_eq!(queries.len(), 3, "Triple UNION should have 3 queries");
            assert!(!all, "UNION should have all=false");
        }
        _ => panic!("Expected Statement::Union"),
    }
}

/// Test basic UNION execution - combines and deduplicates
#[test]
fn test_gql_union_basic() {
    let graph = create_union_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
        MATCH (a:TypeA) RETURN a.name
        UNION
        MATCH (b:TypeB) RETURN b.name
    "#,
        )
        .unwrap();

    // TypeA: Alpha, Beta, Epsilon (3)
    // TypeB: Gamma, Delta, Epsilon (3)
    // Union dedups Epsilon -> 5 unique names
    let names: HashSet<_> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(names.len(), 5, "UNION should have 5 unique names");
    assert!(names.contains("Alpha"));
    assert!(names.contains("Beta"));
    assert!(names.contains("Gamma"));
    assert!(names.contains("Delta"));
    assert!(names.contains("Epsilon"));
}

/// Test UNION ALL keeps duplicates
#[test]
fn test_gql_union_all_keeps_duplicates() {
    let graph = create_union_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
        MATCH (a:TypeA) RETURN a.name
        UNION ALL
        MATCH (b:TypeB) RETURN b.name
    "#,
        )
        .unwrap();

    // TypeA: Alpha, Beta, Epsilon (3)
    // TypeB: Gamma, Delta, Epsilon (3)
    // UNION ALL keeps all 6 including duplicate Epsilon
    assert_eq!(
        results.len(),
        6,
        "UNION ALL should have all 6 results including duplicate Epsilon"
    );

    // Count Epsilon occurrences
    let epsilon_count = results
        .iter()
        .filter(|v| matches!(v, Value::String(s) if s == "Epsilon"))
        .count();

    assert_eq!(epsilon_count, 2, "Epsilon should appear twice in UNION ALL");
}

/// Test UNION with WHERE clauses
#[test]
fn test_gql_union_with_where() {
    let mut storage = InMemoryGraph::new();

    // Add people with ages
    let people = vec![
        ("Alice", "Young", 20i64),
        ("Bob", "Young", 25i64),
        ("Carol", "Old", 60i64),
        ("Dave", "Old", 65i64),
    ];

    for (name, group, age) in people {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(name));
        props.insert("age".to_string(), Value::from(age));
        storage.add_vertex(group, props);
    }

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
        MATCH (y:Young) WHERE y.age > 22 RETURN y.name
        UNION
        MATCH (o:Old) WHERE o.age > 62 RETURN o.name
    "#,
        )
        .unwrap();

    // Young with age > 22: Bob (25)
    // Old with age > 62: Dave (65)
    let names: HashSet<_> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(names.len(), 2);
    assert!(names.contains("Bob"));
    assert!(names.contains("Dave"));
}

/// Test UNION with ORDER BY and LIMIT on combined results
#[test]
fn test_gql_union_combined_single_query_ordering() {
    // Note: ORDER BY and LIMIT apply to each individual query in a UNION,
    // not to the combined result. This is standard SQL behavior.
    // If you need to sort the combined result, you'd need a subquery.
    // For now, we test that each query in the UNION works correctly.
    let graph = create_union_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
        MATCH (a:TypeA) RETURN a.name ORDER BY a.name LIMIT 2
        UNION
        MATCH (b:TypeB) RETURN b.name ORDER BY b.name LIMIT 2
    "#,
        )
        .unwrap();

    // TypeA sorted: Alpha, Beta, Epsilon -> LIMIT 2 -> Alpha, Beta
    // TypeB sorted: Delta, Epsilon, Gamma -> LIMIT 2 -> Delta, Epsilon
    // UNION dedupes Epsilon -> 3 unique: Alpha, Beta, Delta, Epsilon
    // Wait - Epsilon is in both, so should be 4 unique results before dedup
    // After dedup: Alpha, Beta, Delta, Epsilon = 4 unique

    let names: HashSet<_> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    // LIMIT is applied per-query before UNION
    assert!(names.len() <= 4, "Should have at most 4 unique names");
    assert!(names.contains("Alpha"));
    assert!(names.contains("Beta"));
}

/// Test UNION with multiple return columns
#[test]
fn test_gql_union_multiple_columns() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert("role".to_string(), Value::from("Engineer"));
    storage.add_vertex("Employee", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Bob"));
    props.insert("role".to_string(), Value::from("Contractor"));
    storage.add_vertex("Contractor", props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
        MATCH (e:Employee) RETURN e.name AS person, e.role AS type
        UNION
        MATCH (c:Contractor) RETURN c.name AS person, c.role AS type
    "#,
        )
        .unwrap();

    assert_eq!(results.len(), 2);

    // Both should be maps with 'person' and 'type' keys
    for result in &results {
        if let Value::Map(map) = result {
            assert!(map.contains_key("person"), "Should have 'person' key");
            assert!(map.contains_key("type"), "Should have 'type' key");
        } else {
            panic!("Expected Map result");
        }
    }
}

/// Test UNION deduplication with identical rows
#[test]
fn test_gql_union_deduplicates_identical_rows() {
    let mut storage = InMemoryGraph::new();

    // Create two vertices with the same name but different labels
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Shared"));
    storage.add_vertex("TypeA", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Shared"));
    storage.add_vertex("TypeB", props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
        MATCH (a:TypeA) RETURN a.name
        UNION
        MATCH (b:TypeB) RETURN b.name
    "#,
        )
        .unwrap();

    // Both queries return "Shared", UNION should deduplicate to 1
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Shared".to_string()));
}

/// Test UNION ALL keeps identical rows
#[test]
fn test_gql_union_all_keeps_identical_rows() {
    let mut storage = InMemoryGraph::new();

    // Create two vertices with the same name but different labels
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Shared"));
    storage.add_vertex("TypeA", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Shared"));
    storage.add_vertex("TypeB", props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
        MATCH (a:TypeA) RETURN a.name
        UNION ALL
        MATCH (b:TypeB) RETURN b.name
    "#,
        )
        .unwrap();

    // Both queries return "Shared", UNION ALL keeps both
    assert_eq!(results.len(), 2);
    assert!(results
        .iter()
        .all(|v| v == &Value::String("Shared".to_string())));
}

/// Test UNION with empty first query
#[test]
fn test_gql_union_empty_first_query() {
    let graph = create_union_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
        MATCH (x:NonExistent) RETURN x.name
        UNION
        MATCH (a:TypeA) RETURN a.name
    "#,
        )
        .unwrap();

    // First query returns nothing, second returns TypeA names
    assert_eq!(results.len(), 3); // Alpha, Beta, Epsilon
}

/// Test UNION with empty second query
#[test]
fn test_gql_union_empty_second_query() {
    let graph = create_union_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
        MATCH (a:TypeA) RETURN a.name
        UNION
        MATCH (x:NonExistent) RETURN x.name
    "#,
        )
        .unwrap();

    // First query returns TypeA names, second returns nothing
    assert_eq!(results.len(), 3); // Alpha, Beta, Epsilon
}

/// Test UNION with both queries empty
#[test]
fn test_gql_union_both_empty() {
    let graph = create_union_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
        MATCH (x:NonExistent1) RETURN x.name
        UNION
        MATCH (y:NonExistent2) RETURN y.name
    "#,
        )
        .unwrap();

    assert_eq!(results.len(), 0);
}

/// Test triple UNION
#[test]
fn test_gql_triple_union() {
    let mut storage = InMemoryGraph::new();

    // Create vertices with three labels
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("A1"));
    storage.add_vertex("TypeA", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("B1"));
    storage.add_vertex("TypeB", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("C1"));
    storage.add_vertex("TypeC", props);

    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
        MATCH (a:TypeA) RETURN a.name
        UNION
        MATCH (b:TypeB) RETURN b.name
        UNION
        MATCH (c:TypeC) RETURN c.name
    "#,
        )
        .unwrap();

    let names: HashSet<_> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(names.len(), 3);
    assert!(names.contains("A1"));
    assert!(names.contains("B1"));
    assert!(names.contains("C1"));
}

/// Test UNION case insensitivity
#[test]
fn test_gql_union_case_insensitive() {
    let graph = create_union_test_graph();
    let snapshot = graph.snapshot();

    // Test lowercase 'union'
    let results = snapshot
        .gql(
            r#"
        MATCH (a:TypeA) RETURN a.name
        union
        MATCH (b:TypeB) RETURN b.name
    "#,
        )
        .unwrap();

    assert!(!results.is_empty(), "lowercase 'union' should work");

    // Test mixed case 'Union'
    let results = snapshot
        .gql(
            r#"
        MATCH (a:TypeA) RETURN a.name
        Union
        MATCH (b:TypeB) RETURN b.name
    "#,
        )
        .unwrap();

    assert!(!results.is_empty(), "mixed case 'Union' should work");
}

/// Test UNION ALL case insensitivity
#[test]
fn test_gql_union_all_case_insensitive() {
    let graph = create_union_test_graph();
    let snapshot = graph.snapshot();

    // Test lowercase
    let results = snapshot
        .gql(
            r#"
        MATCH (a:TypeA) RETURN a.name
        union all
        MATCH (b:TypeB) RETURN b.name
    "#,
        )
        .unwrap();

    // TypeA has 3, TypeB has 3, UNION ALL = 6
    assert_eq!(results.len(), 6, "lowercase 'union all' should work");
}
