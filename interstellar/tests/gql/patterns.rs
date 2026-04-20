//! Pattern tests for GQL.
//!
//! Tests for pattern matching and traversal features including:
//! - Variable-length paths (*N, *M..N, *.., etc.)
//! - RETURN DISTINCT
//! - Multi-variable patterns
//! - Edge variable binding and properties
//! - Path expressions

#![allow(unused_variables)]
use interstellar::prelude::*;
use interstellar::storage::Graph;
use std::collections::HashMap;

// =============================================================================
// Variable-Length Path Tests
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
    let graph = Graph::new();

    // Create Person vertices
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

    let mut eve_props = HashMap::new();
    eve_props.insert("name".to_string(), Value::from("Eve"));
    let eve = graph.add_vertex("Person", eve_props);

    let mut frank_props = HashMap::new();
    frank_props.insert("name".to_string(), Value::from("Frank"));
    let frank = graph.add_vertex("Person", frank_props);

    // Create a chain: Alice -> Bob -> Carol -> Dave -> Eve
    graph.add_edge(alice, bob, "KNOWS", HashMap::new()).unwrap();
    graph.add_edge(bob, carol, "KNOWS", HashMap::new()).unwrap();
    graph
        .add_edge(carol, dave, "KNOWS", HashMap::new())
        .unwrap();
    graph.add_edge(dave, eve, "KNOWS", HashMap::new()).unwrap();

    // Also: Alice -> Frank -> Dave (shorter path to Dave)
    graph
        .add_edge(alice, frank, "KNOWS", HashMap::new())
        .unwrap();
    graph
        .add_edge(frank, dave, "KNOWS", HashMap::new())
        .unwrap();

    graph
}

/// Test exact hop count: *2 (exactly 2 hops)
#[test]
fn test_gql_variable_path_exact_hops() {
    let graph = create_variable_length_path_graph();
    let snapshot = graph.snapshot();

    // Find people exactly 2 hops from Alice
    // Alice -[KNOWS]-> Bob -[KNOWS]-> Carol (2 hops)
    // Alice -[KNOWS]-> Frank -[KNOWS]-> Dave (2 hops)
    let results = graph
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
    let results = graph
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
    let results = graph
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
    let results = graph
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
    let results = graph
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
    let results = graph
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
    let results = graph
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
    let results = graph
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
    let results_single = graph
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(target) RETURN target.name")
        .unwrap();

    let results_star1 = graph
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
// RETURN DISTINCT Tests
// =============================================================================

/// Helper to create a graph with duplicate property values for DISTINCT tests
fn create_distinct_test_graph() -> Graph {
    let graph = Graph::new();

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
        graph.add_vertex("Person", props);
    }

    graph
}

/// Test RETURN DISTINCT on property - should deduplicate results
#[test]
fn test_gql_return_distinct_property() {
    let graph = create_distinct_test_graph();
    let snapshot = graph.snapshot();

    // Without DISTINCT - should return 7 cities (with duplicates)
    let results_no_distinct = graph.gql("MATCH (p:Person) RETURN p.city").unwrap();
    assert_eq!(
        results_no_distinct.len(),
        7,
        "Should return all 7 city values"
    );

    // With DISTINCT - should return only 3 unique cities
    let results_distinct = graph
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
    let graph = Graph::new();

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
        graph.add_vertex("Person", props);
    }

    let snapshot = graph.snapshot();

    // RETURN DISTINCT on multiple properties - deduplicates based on the combination
    let results = graph
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
    let results = graph
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
    let results = graph
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
    let results = graph
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
    let results1 = graph
        .gql("MATCH (p:Person) RETURN DISTINCT p.city")
        .unwrap();
    let results2 = graph
        .gql("MATCH (p:Person) RETURN distinct p.city")
        .unwrap();
    let results3 = graph
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

    let results = graph.gql("MATCH (p:Person) RETURN p.city").unwrap();

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
    let results_no_distinct = graph.gql("MATCH (p:Person) RETURN p").unwrap();
    let results_distinct = graph.gql("MATCH (p:Person) RETURN DISTINCT p").unwrap();

    assert_eq!(
        results_no_distinct.len(),
        results_distinct.len(),
        "DISTINCT on unique vertices should have same count"
    );
    assert_eq!(results_distinct.len(), 7);
}

// =============================================================================
// Multi-Variable Pattern Tests
// =============================================================================

/// Helper function to create a graph for multi-variable tests
fn create_multi_var_test_graph() -> Graph {
    let graph = Graph::new();

    // Create people
    let alice = graph.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Alice"));
        props.insert("age".to_string(), Value::Int(30));
        props
    });

    let bob = graph.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Bob"));
        props.insert("age".to_string(), Value::Int(28));
        props
    });

    let carol = graph.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Carol"));
        props.insert("age".to_string(), Value::Int(35));
        props
    });

    let dave = graph.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Dave"));
        props.insert("age".to_string(), Value::Int(25));
        props
    });

    // Create companies
    let tech_corp = graph.add_vertex("Company", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("TechCorp"));
        props.insert("size".to_string(), Value::Int(1000));
        props
    });

    let startup_inc = graph.add_vertex("Company", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("StartupInc"));
        props.insert("size".to_string(), Value::Int(50));
        props
    });

    // Create KNOWS relationships
    graph.add_edge(alice, bob, "KNOWS", HashMap::new()).unwrap();
    graph.add_edge(bob, carol, "KNOWS", HashMap::new()).unwrap();
    graph
        .add_edge(carol, dave, "KNOWS", HashMap::new())
        .unwrap();
    graph
        .add_edge(alice, carol, "KNOWS", HashMap::new())
        .unwrap();

    // Create WORKS_AT relationships
    graph
        .add_edge(alice, tech_corp, "WORKS_AT", HashMap::new())
        .unwrap();
    graph
        .add_edge(bob, tech_corp, "WORKS_AT", HashMap::new())
        .unwrap();
    graph
        .add_edge(carol, startup_inc, "WORKS_AT", HashMap::new())
        .unwrap();
    graph
        .add_edge(dave, startup_inc, "WORKS_AT", HashMap::new())
        .unwrap();

    graph
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
    let results: Vec<_> = graph.gql(query).unwrap();

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
    let results: Vec<_> = graph.gql(query).unwrap();

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
    let results: Vec<_> = graph.gql(query).unwrap();

    // Alice(30) knows Bob(28) and Carol(35)
    // Bob(28) knows Carol(35)
    // Carol(35) knows Dave(25)
    // With filters: a.age > 25 AND b.age < 35
    // Alice(30) -> Bob(28) OK
    // Alice(30) -> Carol(35) NO (Carol is 35, not < 35)
    // Bob(28) -> Carol(35) NO
    // Carol(35) -> Dave(25) OK
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
    let results: Vec<_> = graph.gql(query).unwrap();

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
    let results: Vec<_> = graph.gql(query).unwrap();

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
    let results: Vec<_> = graph.gql(query).unwrap();

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
        RETURN a.name AS src, b.name AS dest
        ORDER BY src, dest
        LIMIT 2
    "#;
    let results: Vec<_> = graph.gql(query).unwrap();

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
    let results: Vec<_> = graph.gql(query).unwrap();

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
    let results: Vec<_> = graph.gql(query).unwrap();

    // Alice(30) -> Bob(28) OK (30 > 28)
    // Alice(30) -> Carol(35) NO (30 < 35)
    // Bob(28) -> Carol(35) NO (28 < 35)
    // Carol(35) -> Dave(25) OK (35 > 25)
    assert_eq!(results.len(), 2);
}

// =============================================================================
// Edge Variable and Property Tests
// =============================================================================

/// Helper function to create a graph with edge properties for testing
fn create_edge_property_test_graph() -> Graph {
    let graph = Graph::new();

    // Create people
    let alice = graph.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Alice"));
        props
    });

    let bob = graph.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Bob"));
        props
    });

    let carol = graph.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Carol"));
        props
    });

    // Create teams
    let bulls = graph.add_vertex("Team", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Bulls"));
        props
    });

    let lakers = graph.add_vertex("Team", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Lakers"));
        props
    });

    // Create PLAYED_FOR edges with properties (years, championships)
    graph
        .add_edge(alice, bulls, "PLAYED_FOR", {
            let mut props = HashMap::new();
            props.insert("start_year".to_string(), Value::Int(2015));
            props.insert("end_year".to_string(), Value::Int(2020));
            props.insert("championships".to_string(), Value::Int(2));
            props
        })
        .unwrap();

    graph
        .add_edge(alice, lakers, "PLAYED_FOR", {
            let mut props = HashMap::new();
            props.insert("start_year".to_string(), Value::Int(2020));
            props.insert("end_year".to_string(), Value::Int(2023));
            props.insert("championships".to_string(), Value::Int(1));
            props
        })
        .unwrap();

    graph
        .add_edge(bob, bulls, "PLAYED_FOR", {
            let mut props = HashMap::new();
            props.insert("start_year".to_string(), Value::Int(2010));
            props.insert("end_year".to_string(), Value::Int(2018));
            props.insert("championships".to_string(), Value::Int(3));
            props
        })
        .unwrap();

    graph
        .add_edge(carol, lakers, "PLAYED_FOR", {
            let mut props = HashMap::new();
            props.insert("start_year".to_string(), Value::Int(2018));
            props.insert("end_year".to_string(), Value::Int(2022));
            props.insert("championships".to_string(), Value::Int(0));
            props
        })
        .unwrap();

    // Create KNOWS edges with properties
    graph
        .add_edge(alice, bob, "KNOWS", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2015));
            props
        })
        .unwrap();

    graph
        .add_edge(bob, carol, "KNOWS", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2018));
            props
        })
        .unwrap();

    graph
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
    let results: Vec<_> = graph.gql(query).unwrap();

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
    let results: Vec<_> = graph.gql(query).unwrap();

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
    let results: Vec<_> = graph.gql(query).unwrap();

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
    let results: Vec<_> = graph.gql(query).unwrap();

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
    let results: Vec<_> = graph.gql(query).unwrap();

    // Alice at Bulls: 2015-2020 = 5 years OK
    // Alice at Lakers: 2020-2023 = 3 years NO
    // Bob at Bulls: 2010-2018 = 8 years OK
    // Carol at Lakers: 2018-2022 = 4 years NO
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
    let results: Vec<_> = graph.gql(query).unwrap();

    // Alice knows Bob since 2015 (< 2017) OK
    // Bob knows Carol since 2018 (>= 2017) NO
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
    let results: Vec<_> = graph.gql(query).unwrap();

    // Alice at Bulls: 2015+, 2 rings OK
    // Alice at Lakers: 2020+, 1 ring OK
    // Bob at Bulls: 2010 (< 2015) NO
    // Carol at Lakers: 2018+, 0 rings NO
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
    let results: Vec<_> = graph.gql(query).unwrap();

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

// =============================================================================
// Debug/Integration Tests for Variable-Length Paths with WHERE
// =============================================================================

/// Helper to create a social network graph for debug tests
fn create_social_network_graph() -> Graph {
    let graph = Graph::new();

    // Create people
    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    alice_props.insert("age".to_string(), Value::Int(28i64));
    alice_props.insert("city".to_string(), Value::from("NYC"));
    let alice = graph.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    bob_props.insert("age".to_string(), Value::Int(35i64));
    bob_props.insert("city".to_string(), Value::from("LA"));
    let bob = graph.add_vertex("Person", bob_props);

    let mut charlie_props = HashMap::new();
    charlie_props.insert("name".to_string(), Value::from("Charlie"));
    charlie_props.insert("age".to_string(), Value::Int(42i64));
    charlie_props.insert("city".to_string(), Value::from("NYC"));
    let charlie = graph.add_vertex("Person", charlie_props);

    let mut diana_props = HashMap::new();
    diana_props.insert("name".to_string(), Value::from("Diana"));
    diana_props.insert("age".to_string(), Value::Int(31i64));
    diana_props.insert("city".to_string(), Value::from("Chicago"));
    let diana = graph.add_vertex("Person", diana_props);

    let mut eve_props = HashMap::new();
    eve_props.insert("name".to_string(), Value::from("Eve"));
    eve_props.insert("age".to_string(), Value::Int(25i64));
    eve_props.insert("city".to_string(), Value::from("LA"));
    let eve = graph.add_vertex("Person", eve_props);

    let mut frank_props = HashMap::new();
    frank_props.insert("name".to_string(), Value::from("Frank"));
    frank_props.insert("age".to_string(), Value::Int(55i64));
    frank_props.insert("city".to_string(), Value::from("NYC"));
    let frank = graph.add_vertex("Person", frank_props);

    let mut grace_props = HashMap::new();
    grace_props.insert("name".to_string(), Value::from("Grace"));
    grace_props.insert("age".to_string(), Value::Int(29i64));
    grace_props.insert("city".to_string(), Value::from("Boston"));
    let grace = graph.add_vertex("Person", grace_props);

    let mut henry_props = HashMap::new();
    henry_props.insert("name".to_string(), Value::from("Henry"));
    henry_props.insert("age".to_string(), Value::Int(38i64));
    henry_props.insert("city".to_string(), Value::from("Seattle"));
    let henry = graph.add_vertex("Person", henry_props);

    // Create companies
    let mut tech_props = HashMap::new();
    tech_props.insert("name".to_string(), Value::from("TechCorp"));
    tech_props.insert("industry".to_string(), Value::from("Technology"));
    let techcorp = graph.add_vertex("Company", tech_props);

    let mut fin_props = HashMap::new();
    fin_props.insert("name".to_string(), Value::from("FinanceInc"));
    fin_props.insert("industry".to_string(), Value::from("Finance"));
    let financeinc = graph.add_vertex("Company", fin_props);

    // KNOWS relationships (bidirectional conceptually, but stored as directed)
    graph.add_edge(alice, bob, "KNOWS", HashMap::new()).unwrap();
    graph
        .add_edge(alice, charlie, "KNOWS", HashMap::new())
        .unwrap();
    graph
        .add_edge(alice, diana, "KNOWS", HashMap::new())
        .unwrap();

    graph.add_edge(bob, alice, "KNOWS", HashMap::new()).unwrap();
    graph.add_edge(bob, eve, "KNOWS", HashMap::new()).unwrap();
    graph.add_edge(bob, frank, "KNOWS", HashMap::new()).unwrap();

    graph
        .add_edge(charlie, alice, "KNOWS", HashMap::new())
        .unwrap();
    graph
        .add_edge(charlie, diana, "KNOWS", HashMap::new())
        .unwrap();
    graph
        .add_edge(charlie, grace, "KNOWS", HashMap::new())
        .unwrap();

    graph
        .add_edge(diana, alice, "KNOWS", HashMap::new())
        .unwrap();
    graph
        .add_edge(diana, charlie, "KNOWS", HashMap::new())
        .unwrap();
    graph.add_edge(diana, eve, "KNOWS", HashMap::new()).unwrap();

    graph.add_edge(eve, bob, "KNOWS", HashMap::new()).unwrap();
    graph.add_edge(eve, diana, "KNOWS", HashMap::new()).unwrap();
    graph.add_edge(eve, henry, "KNOWS", HashMap::new()).unwrap();

    graph.add_edge(frank, bob, "KNOWS", HashMap::new()).unwrap();
    graph
        .add_edge(frank, grace, "KNOWS", HashMap::new())
        .unwrap();

    graph
        .add_edge(grace, charlie, "KNOWS", HashMap::new())
        .unwrap();
    graph
        .add_edge(grace, frank, "KNOWS", HashMap::new())
        .unwrap();
    graph
        .add_edge(grace, henry, "KNOWS", HashMap::new())
        .unwrap();

    graph.add_edge(henry, eve, "KNOWS", HashMap::new()).unwrap();
    graph
        .add_edge(henry, grace, "KNOWS", HashMap::new())
        .unwrap();

    // WORKS_AT relationships
    graph
        .add_edge(alice, techcorp, "WORKS_AT", HashMap::new())
        .unwrap();
    graph
        .add_edge(bob, techcorp, "WORKS_AT", HashMap::new())
        .unwrap();
    graph
        .add_edge(charlie, financeinc, "WORKS_AT", HashMap::new())
        .unwrap();
    graph
        .add_edge(diana, techcorp, "WORKS_AT", HashMap::new())
        .unwrap();
    graph
        .add_edge(frank, financeinc, "WORKS_AT", HashMap::new())
        .unwrap();

    graph
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
    let results1: Vec<_> = graph.gql(query1).unwrap();
    println!("With inline filter: {:?}", results1);

    // Now test with WHERE clause (triggers multi-var path)
    let query2 = r#"
        MATCH (p:Person)-[:KNOWS*2..3]->(target:Person)
        WHERE p.name = 'Alice'
        RETURN DISTINCT target.name
        ORDER BY target.name
    "#;
    let results2: Vec<_> = graph.gql(query2).unwrap();
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
    let results: Vec<_> = graph.gql(query).unwrap();
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
    let results: Vec<_> = graph.gql(query).unwrap();
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
    let g = snapshot.gremlin();

    // First find Alice
    let alice_results: Vec<_> = g
        .v()
        .has_label("Person")
        .has_value("name", interstellar::value::Value::from("Alice"))
        .to_list();
    println!("Alice found: {:?}", alice_results);

    // Now do 2-hop with path tracking
    let g2 = snapshot.gremlin();
    let path_results: Vec<_> = g2
        .v()
        .with_path()
        .has_label("Person")
        .has_value("name", interstellar::value::Value::from("Alice"))
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
    use interstellar::traversal::__;

    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let g = snapshot.gremlin();
    let path_results: Vec<_> = g
        .v()
        .with_path()
        .has_label("Person")
        .has_value("name", interstellar::value::Value::from("Alice"))
        .as_("p")
        .repeat(__.out_labels(&["KNOWS"]))
        .times(2)
        .as_("target")
        .select(&["p", "target"])
        .to_list();

    println!("2-hop with repeat + select: {:?}", path_results);
    assert!(!path_results.is_empty(), "Should have repeat path results");
}

// =============================================================================
// Multi-Pattern MATCH Tests (`MATCH (a)…, (b)…`)
// =============================================================================

/// Build a small family graph used by the multi-pattern tests:
///
/// ```text
///   parent ──PARENT_OF──> child_a
///   parent ──PARENT_OF──> child_b
///   parent ──PARENT_OF──> child_c
/// ```
///
/// Each child has a HAS_NAME edge to a Name vertex with a `sortOrder`.
fn create_family_graph() -> (Graph, VertexId, VertexId, VertexId, VertexId) {
    let graph = Graph::new();

    let mut parent_props = HashMap::new();
    parent_props.insert("name".to_string(), Value::from("Parent"));
    let parent_id = graph.add_vertex("Person", parent_props);

    let mut a_props = HashMap::new();
    a_props.insert("name".to_string(), Value::from("ChildA"));
    let a_id = graph.add_vertex("Person", a_props);

    let mut b_props = HashMap::new();
    b_props.insert("name".to_string(), Value::from("ChildB"));
    let b_id = graph.add_vertex("Person", b_props);

    let mut c_props = HashMap::new();
    c_props.insert("name".to_string(), Value::from("ChildC"));
    let c_id = graph.add_vertex("Person", c_props);

    let _ = graph.add_edge(parent_id, a_id, "PARENT_OF", HashMap::new());
    let _ = graph.add_edge(parent_id, b_id, "PARENT_OF", HashMap::new());
    let _ = graph.add_edge(parent_id, c_id, "PARENT_OF", HashMap::new());

    let mut a_name = HashMap::new();
    a_name.insert("given".to_string(), Value::from("Alice"));
    a_name.insert("surname".to_string(), Value::from("Smith"));
    a_name.insert("sortOrder".to_string(), Value::Int(0));
    let a_name_id = graph.add_vertex("Name", a_name);
    let _ = graph.add_edge(a_id, a_name_id, "HAS_NAME", HashMap::new());

    let mut b_name = HashMap::new();
    b_name.insert("given".to_string(), Value::from("Bob"));
    b_name.insert("surname".to_string(), Value::from("Smith"));
    b_name.insert("sortOrder".to_string(), Value::Int(0));
    let b_name_id = graph.add_vertex("Name", b_name);
    let _ = graph.add_edge(b_id, b_name_id, "HAS_NAME", HashMap::new());

    let mut c_name = HashMap::new();
    c_name.insert("given".to_string(), Value::from("Carol"));
    c_name.insert("surname".to_string(), Value::from("Smith"));
    c_name.insert("sortOrder".to_string(), Value::Int(0));
    let c_name_id = graph.add_vertex("Name", c_name);
    let _ = graph.add_edge(c_id, c_name_id, "HAS_NAME", HashMap::new());

    (graph, parent_id, a_id, b_id, c_id)
}

#[test]
fn test_gql_multi_pattern_shared_anchor_finds_siblings() {
    // Reproduces the primary bug example: find siblings of a given child by
    // joining two patterns that share the `parent` variable.
    let (graph, _parent_id, a_id, b_id, c_id) = create_family_graph();
    let _snapshot = graph.snapshot();

    let query = format!(
        r#"
        MATCH (parent:Person)-[:PARENT_OF]->(p:Person),
              (parent)-[:PARENT_OF]->(sibling:Person)-[:HAS_NAME]->(n:Name)
        WHERE ID(p) = {a} AND ID(sibling) <> {a} AND n.sortOrder = 0
        RETURN DISTINCT n.given
        "#,
        a = a_id.0,
    );

    let results: Vec<_> = graph.gql(&query).unwrap();

    let names: Vec<String> = results
        .into_iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s),
            _ => None,
        })
        .collect();

    assert_eq!(names.len(), 2, "expected 2 siblings, got {:?}", names);
    assert!(names.contains(&"Bob".to_string()));
    assert!(names.contains(&"Carol".to_string()));
    assert!(!names.contains(&"Alice".to_string()));
    let _ = (b_id, c_id);
}

#[test]
fn test_gql_multi_pattern_introduces_variable_in_second_pattern() {
    // The second pattern introduces brand-new variables (`sibling`, `n`) that
    // must be visible in WHERE and RETURN.
    let (graph, _parent_id, a_id, _b_id, _c_id) = create_family_graph();
    let _snapshot = graph.snapshot();

    let query = format!(
        r#"
        MATCH (parent:Person)-[:PARENT_OF]->(p:Person),
              (parent)-[:PARENT_OF]->(sibling:Person)-[:HAS_NAME]->(n:Name)
        WHERE ID(p) = {a} AND ID(sibling) <> {a}
        RETURN DISTINCT n.given, n.surname
        "#,
        a = a_id.0,
    );

    let results: Vec<_> = graph.gql(&query).unwrap();
    assert_eq!(results.len(), 2, "expected 2 sibling rows, got {:?}", results);

    // Each row should be a map with `n.given` and `n.surname` keys (column
    // ordering preserved per Bug 4 fix).
    for row in &results {
        match row {
            Value::Map(map) => {
                assert!(map.contains_key("n.given"));
                assert!(map.contains_key("n.surname"));
            }
            _ => panic!("expected map row, got {:?}", row),
        }
    }
}

#[test]
fn test_gql_multi_pattern_cartesian_fully_disjoint() {
    // Two disjoint patterns produce a Cartesian product. The graph has 3
    // children (Alice, Bob, Carol). `MATCH (p1:Person {name:'ChildA'}),
    // (p2:Person {name:'ChildB'})` should produce one row.
    let (graph, _parent_id, _a_id, _b_id, _c_id) = create_family_graph();
    let _snapshot = graph.snapshot();

    let results: Vec<_> = graph
        .gql(
            r#"
        MATCH (p1:Person {name: 'ChildA'}),
              (p2:Person {name: 'ChildB'})
        RETURN p1.name, p2.name
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1, "expected exactly 1 row, got {:?}", results);
}

#[test]
fn test_gql_multi_pattern_shared_variable_equality_constraint() {
    // Reproduces the "same bound variable in second pattern" form from the
    // bug report: two HAS_SPOUSE edges from the same Marriage, distinct
    // spouses. Here we model with the family graph: two PARENT_OF edges from
    // the same parent to distinct children.
    let (graph, _parent_id, _a_id, _b_id, _c_id) = create_family_graph();
    let _snapshot = graph.snapshot();

    let results: Vec<_> = graph
        .gql(
            r#"
        MATCH (parent:Person)-[:PARENT_OF]->(p:Person),
              (parent)-[:PARENT_OF]->(s:Person)
        WHERE ID(p) <> ID(s)
        RETURN ID(p) AS pid, ID(s) AS sid
        "#,
        )
        .unwrap();

    // 3 children → 3 * 2 = 6 ordered pairs (p, s) with p ≠ s.
    assert_eq!(results.len(), 6, "expected 6 rows, got {:?}", results);
}

#[test]
fn test_gql_multi_pattern_no_match_in_second_pattern() {
    // The second pattern references a non-existent edge label, so the join
    // must produce zero rows even though the first pattern matches.
    let (graph, _parent_id, _a_id, _b_id, _c_id) = create_family_graph();
    let _snapshot = graph.snapshot();

    let results: Vec<_> = graph
        .gql(
            r#"
        MATCH (parent:Person)-[:PARENT_OF]->(p:Person),
              (parent)-[:NONEXISTENT]->(other:Person)
        RETURN p.name
        "#,
        )
        .unwrap();

    assert!(results.is_empty(), "expected 0 rows, got {:?}", results);
}
