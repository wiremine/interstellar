//! Clause tests for GQL query language
//!
//! Tests for various GQL clauses including:
//! - UNION and UNION ALL
//! - OPTIONAL MATCH
//! - WITH PATH and path() function
//! - UNWIND

use interstellar::gql::{parse, parse_statement, Statement};
use interstellar::prelude::*;
use interstellar::storage::Graph;
use std::collections::{HashMap, HashSet};

// =============================================================================
// Test Graph Helpers
// =============================================================================

/// Helper to create a graph for UNION tests
fn create_union_test_graph() -> Graph {
    let graph = Graph::new();

    // Create TypeA vertices
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alpha"));
    graph.add_vertex("TypeA", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Beta"));
    graph.add_vertex("TypeA", props);

    // Create TypeB vertices
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Gamma"));
    graph.add_vertex("TypeB", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Delta"));
    graph.add_vertex("TypeB", props);

    // Create a vertex that appears in both types (simulated - same name)
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Epsilon"));
    graph.add_vertex("TypeA", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Epsilon"));
    graph.add_vertex("TypeB", props);

    graph
}

/// Helper to create a graph for OPTIONAL MATCH tests
fn create_optional_match_test_graph() -> Graph {
    let graph = Graph::new();

    // Create players
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Michael Jordan"));
    let mj = graph.add_vertex("player", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Scottie Pippen"));
    let sp = graph.add_vertex("player", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Charles Barkley"));
    let cb = graph.add_vertex("player", props);

    // Create teams
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Chicago Bulls"));
    let bulls = graph.add_vertex("team", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Phoenix Suns"));
    let suns = graph.add_vertex("team", props);

    // Create relationships
    // MJ and Pippen won championships with Bulls
    let _ = graph.add_edge(mj, bulls, "won_championship_with", HashMap::new());
    let _ = graph.add_edge(sp, bulls, "won_championship_with", HashMap::new());

    // All players played for their teams
    let _ = graph.add_edge(mj, bulls, "played_for", HashMap::new());
    let _ = graph.add_edge(sp, bulls, "played_for", HashMap::new());
    let _ = graph.add_edge(cb, suns, "played_for", HashMap::new());

    graph
}

/// Helper to create a basic test graph
fn create_test_graph() -> Graph {
    let graph = Graph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert("age".to_string(), Value::from(30i64));
    graph.add_vertex("Person", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Bob"));
    props.insert("age".to_string(), Value::from(25i64));
    graph.add_vertex("Person", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Carol"));
    props.insert("age".to_string(), Value::from(35i64));
    graph.add_vertex("Person", props);

    graph
}

/// Helper to create a graph with edges for path tests
fn create_graph_with_edges() -> Graph {
    let graph = Graph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    let alice = graph.add_vertex("Person", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Bob"));
    let bob = graph.add_vertex("Person", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Carol"));
    let carol = graph.add_vertex("Person", props);

    // Alice knows Bob and Carol
    graph.add_edge(alice, bob, "KNOWS", HashMap::new()).unwrap();
    graph
        .add_edge(alice, carol, "KNOWS", HashMap::new())
        .unwrap();

    // Bob knows Carol
    graph.add_edge(bob, carol, "KNOWS", HashMap::new()).unwrap();

    graph
}

// =============================================================================
// UNION Clause Tests (Plan 11 - Week 1)
// =============================================================================

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
    let graph = Graph::new();

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
        graph.add_vertex(group, props);
    }

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
    let graph = Graph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert("role".to_string(), Value::from("Engineer"));
    graph.add_vertex("Employee", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Bob"));
    props.insert("role".to_string(), Value::from("Contractor"));
    graph.add_vertex("Contractor", props);

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
    let graph = Graph::new();

    // Create two vertices with the same name but different labels
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Shared"));
    graph.add_vertex("TypeA", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Shared"));
    graph.add_vertex("TypeB", props);

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
    let graph = Graph::new();

    // Create two vertices with the same name but different labels
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Shared"));
    graph.add_vertex("TypeA", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Shared"));
    graph.add_vertex("TypeB", props);

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
    let graph = Graph::new();

    // Create vertices with three labels
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("A1"));
    graph.add_vertex("TypeA", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("B1"));
    graph.add_vertex("TypeB", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("C1"));
    graph.add_vertex("TypeC", props);

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

// =============================================================================
// OPTIONAL MATCH Tests
// =============================================================================

/// Test parsing OPTIONAL MATCH
#[test]
fn test_gql_parse_optional_match() {
    let query = parse(
        r#"
        MATCH (p:player)
        OPTIONAL MATCH (p)-[:won_championship_with]->(t:team)
        RETURN p.name, t.name
    "#,
    )
    .unwrap();

    assert!(!query.optional_match_clauses.is_empty());
    assert_eq!(query.optional_match_clauses.len(), 1);
}

/// Test parsing multiple OPTIONAL MATCH clauses
#[test]
fn test_gql_parse_multiple_optional_match() {
    let query = parse(
        r#"
        MATCH (p:player)
        OPTIONAL MATCH (p)-[:won_championship_with]->(t:team)
        OPTIONAL MATCH (p)-[:played_for]->(t2:team)
        RETURN p.name, t.name, t2.name
    "#,
    )
    .unwrap();

    assert_eq!(query.optional_match_clauses.len(), 2);
}

/// Test OPTIONAL MATCH returns null when no match
#[test]
fn test_gql_optional_match_returns_null() {
    let graph = create_optional_match_test_graph();
    let snapshot = graph.snapshot();

    // Charles Barkley never won a championship
    let results = snapshot
        .gql(
            r#"
        MATCH (p:player {name: 'Charles Barkley'})
        OPTIONAL MATCH (p)-[:won_championship_with]->(t:team)
        RETURN p.name, t.name
    "#,
        )
        .unwrap();

    // Should return one row with Barkley's name and null for team
    assert_eq!(results.len(), 1);

    if let Value::Map(map) = &results[0] {
        assert_eq!(
            map.get("p.name"),
            Some(&Value::String("Charles Barkley".to_string()))
        );
        // t.name should be null
        assert!(
            map.get("t.name").is_none() || map.get("t.name") == Some(&Value::Null),
            "Expected null for t.name, got {:?}",
            map.get("t.name")
        );
    } else {
        panic!("Expected Map result, got {:?}", results[0]);
    }
}

/// Test OPTIONAL MATCH returns value when match exists
#[test]
fn test_gql_optional_match_returns_value() {
    let graph = create_optional_match_test_graph();
    let snapshot = graph.snapshot();

    // Michael Jordan won championships
    let results = snapshot
        .gql(
            r#"
        MATCH (p:player {name: 'Michael Jordan'})
        OPTIONAL MATCH (p)-[:won_championship_with]->(t:team)
        RETURN p.name, t.name
    "#,
        )
        .unwrap();

    // Should return one row with MJ's name and Bulls
    assert_eq!(results.len(), 1);

    if let Value::Map(map) = &results[0] {
        assert_eq!(
            map.get("p.name"),
            Some(&Value::String("Michael Jordan".to_string()))
        );
        assert_eq!(
            map.get("t.name"),
            Some(&Value::String("Chicago Bulls".to_string()))
        );
    } else {
        panic!("Expected Map result, got {:?}", results[0]);
    }
}

/// Test OPTIONAL MATCH with all players - mixed results
#[test]
fn test_gql_optional_match_mixed_results() {
    let graph = create_optional_match_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
        MATCH (p:player)
        OPTIONAL MATCH (p)-[:won_championship_with]->(t:team)
        RETURN p.name, t.name
    "#,
        )
        .unwrap();

    // Should return 3 rows (one per player)
    // MJ and Pippen have teams, Barkley has null
    assert_eq!(results.len(), 3);

    let mut with_championship = 0;
    let mut without_championship = 0;

    for result in &results {
        if let Value::Map(map) = result {
            if let Some(Value::String(_)) = map.get("t.name") {
                with_championship += 1;
            } else {
                without_championship += 1;
            }
        }
    }

    assert_eq!(with_championship, 2, "MJ and Pippen have championships");
    assert_eq!(without_championship, 1, "Barkley has no championship");
}

/// Test OPTIONAL MATCH case insensitivity
#[test]
fn test_gql_optional_match_case_insensitive() {
    let graph = create_optional_match_test_graph();
    let snapshot = graph.snapshot();

    // Test lowercase
    let results = snapshot
        .gql(
            r#"
        MATCH (p:player {name: 'Michael Jordan'})
        optional match (p)-[:won_championship_with]->(t:team)
        RETURN p.name
    "#,
        )
        .unwrap();

    assert!(
        !results.is_empty(),
        "lowercase 'optional match' should work"
    );

    // Test mixed case
    let results = snapshot
        .gql(
            r#"
        MATCH (p:player {name: 'Michael Jordan'})
        Optional Match (p)-[:won_championship_with]->(t:team)
        RETURN p.name
    "#,
        )
        .unwrap();

    assert!(
        !results.is_empty(),
        "mixed case 'Optional Match' should work"
    );
}

/// Test OPTIONAL MATCH with WHERE clause
#[test]
fn test_gql_optional_match_with_where() {
    let graph = create_optional_match_test_graph();
    let snapshot = graph.snapshot();

    // Filter to only rows where optional match succeeded
    let results = snapshot
        .gql(
            r#"
        MATCH (p:player)
        OPTIONAL MATCH (p)-[:won_championship_with]->(t:team)
        WHERE t.name IS NOT NULL
        RETURN p.name, t.name
    "#,
        )
        .unwrap();

    // Should return only MJ and Pippen (the ones with championships)
    assert_eq!(results.len(), 2);

    for result in &results {
        if let Value::Map(map) = result {
            let name = map.get("p.name");
            assert!(
                name == Some(&Value::String("Michael Jordan".to_string()))
                    || name == Some(&Value::String("Scottie Pippen".to_string())),
                "Expected MJ or Pippen, got {:?}",
                name
            );
        }
    }
}

/// Test OPTIONAL MATCH preserves rows even with no match
#[test]
fn test_gql_optional_match_preserves_base_rows() {
    let graph = create_optional_match_test_graph();
    let snapshot = graph.snapshot();

    // Count all players (should be 3 regardless of optional match)
    let results = snapshot
        .gql(
            r#"
        MATCH (p:player)
        OPTIONAL MATCH (p)-[:won_championship_with]->(t:team)
        RETURN p.name
    "#,
        )
        .unwrap();

    // All 3 players should be returned
    assert_eq!(results.len(), 3);
}

// =============================================================================
// WITH PATH and path() Function Tests
// =============================================================================

/// Test WITH PATH returns path as list
#[test]
fn test_gql_with_path_returns_path() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    // Query: find path from Alice to her friends
    let results = snapshot
        .gql(
            r#"
        MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person)
        WITH PATH
        RETURN path()
    "#,
        )
        .unwrap();

    // Alice knows 2 people (Bob and Carol), so we should have 2 paths
    assert_eq!(results.len(), 2);

    // Each result should be a list representing the path
    for result in &results {
        match result {
            Value::List(path_elements) => {
                // Path should have at least 2 elements (Alice and friend)
                assert!(
                    path_elements.len() >= 2,
                    "Path should have at least 2 elements, got {}",
                    path_elements.len()
                );
            }
            _ => panic!("Expected path() to return a list, got {:?}", result),
        }
    }
}

/// Test path() function returns correct path elements
#[test]
fn test_gql_path_function_elements() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    // Single path traversal
    let results = snapshot
        .gql(
            r#"
        MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
        WITH PATH
        RETURN path()
    "#,
        )
        .unwrap();

    assert_eq!(
        results.len(),
        1,
        "Should find exactly one path from Alice to Bob"
    );

    if let Value::List(path_elements) = &results[0] {
        // Path should contain vertices (Alice and Bob)
        // The exact content depends on path tracking implementation
        assert!(!path_elements.is_empty(), "Path should not be empty");

        // First element should be a vertex (Alice)
        assert!(
            matches!(path_elements.first(), Some(Value::Vertex(_))),
            "First path element should be a vertex"
        );
    } else {
        panic!("Expected path() to return a list");
    }
}

/// Test path() function with longer path
#[test]
fn test_gql_path_function_multi_hop() {
    let graph = create_graph_with_edges();
    let snapshot = graph.snapshot();

    // Multi-hop path: Alice -> Bob -> Carol
    let results = snapshot
        .gql(
            r#"
        MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})-[:KNOWS]->(c:Person {name: 'Carol'})
        WITH PATH
        RETURN path()
    "#,
        )
        .unwrap();

    assert_eq!(
        results.len(),
        1,
        "Should find exactly one path from Alice to Bob to Carol"
    );

    if let Value::List(path_elements) = &results[0] {
        // Path should contain all three vertices
        assert!(
            path_elements.len() >= 3,
            "Multi-hop path should have at least 3 elements, got {}",
            path_elements.len()
        );
    } else {
        panic!("Expected path() to return a list");
    }
}

// =============================================================================
// UNWIND Tests
// =============================================================================

/// Test UNWIND with list property
#[test]
fn test_gql_unwind_list() {
    let graph = Graph::new();

    // Create a vertex with a list property
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert(
        "hobbies".to_string(),
        Value::List(vec![
            Value::from("reading"),
            Value::from("coding"),
            Value::from("gaming"),
        ]),
    );
    graph.add_vertex("Person", props);

    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
        MATCH (p:Person)
        UNWIND p.hobbies AS hobby
        RETURN hobby
    "#,
        )
        .unwrap();

    // Should unwind to 3 rows
    assert_eq!(results.len(), 3);

    let hobbies: HashSet<String> = results
        .into_iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s),
            _ => None,
        })
        .collect();

    assert!(hobbies.contains("reading"));
    assert!(hobbies.contains("coding"));
    assert!(hobbies.contains("gaming"));
}

/// Test UNWIND with null produces no rows
#[test]
fn test_gql_unwind_null_produces_no_rows() {
    let graph = Graph::new();

    // Create a vertex without the list property
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    graph.add_vertex("Person", props);

    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
        MATCH (p:Person)
        UNWIND p.missing_property AS item
        RETURN item
    "#,
        )
        .unwrap();

    // UNWIND null produces no rows
    assert_eq!(results.len(), 0, "UNWIND null should produce no rows");
}

/// Test UNWIND non-list wraps in single-element list
#[test]
fn test_gql_unwind_non_list_wraps() {
    let graph = Graph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert("age".to_string(), Value::from(30i64));
    graph.add_vertex("Person", props);

    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
        MATCH (p:Person)
        UNWIND p.age AS val
        RETURN val
    "#,
        )
        .unwrap();

    // UNWIND non-list treats it as single-element list
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(30));
}

/// Test multiple UNWIND clauses
#[test]
fn test_gql_multiple_unwind() {
    let graph = Graph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert(
        "colors".to_string(),
        Value::List(vec![Value::from("red"), Value::from("blue")]),
    );
    props.insert(
        "sizes".to_string(),
        Value::List(vec![Value::from("S"), Value::from("M")]),
    );
    graph.add_vertex("Person", props);

    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
        MATCH (p:Person)
        UNWIND p.colors AS color
        UNWIND p.sizes AS size
        RETURN color, size
    "#,
        )
        .unwrap();

    // Should produce cartesian product: 2 colors x 2 sizes = 4 rows
    assert_eq!(
        results.len(),
        4,
        "Multiple UNWIND should produce cartesian product"
    );

    // Verify we have all combinations
    let combinations: Vec<(String, String)> = results
        .iter()
        .filter_map(|v| match v {
            Value::Map(map) => {
                let color = map.get("color").and_then(|c| match c {
                    Value::String(s) => Some(s.clone()),
                    _ => None,
                })?;
                let size = map.get("size").and_then(|s| match s {
                    Value::String(s) => Some(s.clone()),
                    _ => None,
                })?;
                Some((color, size))
            }
            _ => None,
        })
        .collect();

    assert_eq!(combinations.len(), 4);
}

/// Test UNWIND with inline list
#[test]
fn test_gql_unwind_inline_list() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
        MATCH (p:Person)
        UNWIND [1, 2, 3] AS num
        RETURN num
        LIMIT 9
    "#,
        )
        .unwrap();

    // 3 people x 3 numbers = 9 rows
    assert_eq!(results.len(), 9);

    // All results should be 1, 2, or 3
    for result in &results {
        match result {
            Value::Int(n) => {
                assert!(*n >= 1 && *n <= 3, "Expected 1, 2, or 3, got {}", n);
            }
            _ => panic!("Expected Int, got {:?}", result),
        }
    }
}

/// Test UNWIND with WHERE filter
#[test]
fn test_gql_unwind_with_where() {
    let graph = Graph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert(
        "scores".to_string(),
        Value::List(vec![
            Value::from(85i64),
            Value::from(92i64),
            Value::from(78i64),
            Value::from(95i64),
        ]),
    );
    graph.add_vertex("Person", props);

    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
        MATCH (p:Person)
        UNWIND p.scores AS score
        WHERE score >= 90
        RETURN score
    "#,
        )
        .unwrap();

    // Only scores >= 90: 92 and 95
    assert_eq!(results.len(), 2);

    let scores: HashSet<i64> = results
        .into_iter()
        .filter_map(|v| match v {
            Value::Int(n) => Some(n),
            _ => None,
        })
        .collect();

    assert!(scores.contains(&92));
    assert!(scores.contains(&95));
}
