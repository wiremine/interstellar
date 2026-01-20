//! Expression tests for GQL.
//!
//! Tests for expression evaluation including:
//! - EXISTS subqueries
//! - COALESCE function
//! - CASE expressions
//! - Type conversion functions (toString, toInteger, toFloat, toBoolean)
//! - String functions (upper, lower, length, trim, substring, replace)
//! - Math functions (abs, round, floor, ceil)

use interstellar::prelude::*;
use interstellar::storage::Graph;
use std::collections::HashMap;

// =============================================================================
// EXISTS Expression Tests
// =============================================================================

/// Helper to create a graph with relationships for EXISTS testing
fn create_exists_test_graph() -> Graph {
    let graph = Graph::new();

    // Create players
    let mut mj_props = HashMap::new();
    mj_props.insert("name".to_string(), Value::from("Michael Jordan"));
    mj_props.insert("position".to_string(), Value::from("Shooting Guard"));
    let mj_id = graph.add_vertex("player", mj_props);

    let mut kobe_props = HashMap::new();
    kobe_props.insert("name".to_string(), Value::from("Kobe Bryant"));
    kobe_props.insert("position".to_string(), Value::from("Shooting Guard"));
    let kobe_id = graph.add_vertex("player", kobe_props);

    let mut barkley_props = HashMap::new();
    barkley_props.insert("name".to_string(), Value::from("Charles Barkley"));
    barkley_props.insert("position".to_string(), Value::from("Power Forward"));
    let barkley_id = graph.add_vertex("player", barkley_props);

    let mut nash_props = HashMap::new();
    nash_props.insert("name".to_string(), Value::from("Steve Nash"));
    nash_props.insert("position".to_string(), Value::from("Point Guard"));
    let nash_id = graph.add_vertex("player", nash_props);

    // Create teams
    let mut bulls_props = HashMap::new();
    bulls_props.insert("name".to_string(), Value::from("Chicago Bulls"));
    bulls_props.insert("championships".to_string(), Value::Int(6));
    let bulls_id = graph.add_vertex("team", bulls_props);

    let mut lakers_props = HashMap::new();
    lakers_props.insert("name".to_string(), Value::from("Los Angeles Lakers"));
    lakers_props.insert("championships".to_string(), Value::Int(17));
    let lakers_id = graph.add_vertex("team", lakers_props);

    let mut suns_props = HashMap::new();
    suns_props.insert("name".to_string(), Value::from("Phoenix Suns"));
    suns_props.insert("championships".to_string(), Value::Int(0));
    let suns_id = graph.add_vertex("team", suns_props);

    // Add championship relationships (only MJ and Kobe have won)
    let mut ring_props = HashMap::new();
    ring_props.insert("years".to_string(), Value::from("1991-1993,1996-1998"));
    let _ = graph.add_edge(mj_id, bulls_id, "won_championship_with", ring_props.clone());

    ring_props.insert("years".to_string(), Value::from("2000-2002,2009-2010"));
    let _ = graph.add_edge(kobe_id, lakers_id, "won_championship_with", ring_props);

    // Add played_for relationships
    let played_props = HashMap::new();
    let _ = graph.add_edge(mj_id, bulls_id, "played_for", played_props.clone());
    let _ = graph.add_edge(kobe_id, lakers_id, "played_for", played_props.clone());
    let _ = graph.add_edge(barkley_id, suns_id, "played_for", played_props.clone());
    let _ = graph.add_edge(nash_id, suns_id, "played_for", played_props);

    graph
}

#[test]
fn test_gql_exists_basic() {
    let graph = create_exists_test_graph();
    let snapshot = graph.snapshot();

    // Find players who have won championships
    let results: Vec<_> = graph
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
    let results: Vec<_> = graph
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
    let results: Vec<_> = graph
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
    let results: Vec<_> = graph
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
    let results: Vec<_> = graph
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
    let results: Vec<_> = graph
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
    let results: Vec<_> = graph
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
    use interstellar::gql::parse;

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
        interstellar::gql::Expression::Exists { negated, pattern } => {
            assert!(!negated);
            assert!(!pattern.elements.is_empty());
        }
        _ => panic!("Expected EXISTS expression"),
    }
}

#[test]
fn test_gql_parse_not_exists_expression() {
    use interstellar::gql::parse;

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
        interstellar::gql::Expression::UnaryOp { op, expr } => {
            assert!(matches!(op, interstellar::gql::UnaryOperator::Not));
            match *expr {
                interstellar::gql::Expression::Exists { negated, pattern } => {
                    assert!(!negated);
                    assert!(!pattern.elements.is_empty());
                }
                _ => panic!("Expected EXISTS expression inside NOT"),
            }
        }
        interstellar::gql::Expression::Exists { negated, pattern } => {
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
    let results: Vec<_> = graph
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
    let results: Vec<_> = graph
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
    let results: Vec<_> = graph
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
    let results: Vec<_> = graph
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
    let results: Vec<_> = graph
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
    let results: Vec<_> = graph
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
    let results: Vec<_> = graph
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
    let results: Vec<_> = graph
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
    let results: Vec<_> = graph
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
    let results: Vec<_> = graph
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
    let graph = Graph::new();
    let snapshot = graph.snapshot();

    // EXISTS on empty graph should return no results
    let results: Vec<_> = graph
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
    let graph = Graph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Lonely Player"));
    graph.add_vertex("player", props);

    let snapshot = graph.snapshot();

    // EXISTS should return false for a player with no outgoing edges
    let results: Vec<_> = graph
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
    let results: Vec<_> = graph
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
    let graph = Graph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Narcissist"));
    let id = graph.add_vertex("player", props);

    let _ = graph.add_edge(id, id, "admires", HashMap::new());

    let snapshot = graph.snapshot();

    // EXISTS should work with self-loops
    let results: Vec<_> = graph
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
    let results: Vec<_> = graph
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
// COALESCE Function Tests
// =============================================================================

/// Helper to create a graph with null values for COALESCE tests
fn create_coalesce_test_graph() -> Graph {
    let graph = Graph::new();

    // Person with both name and nickname
    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    alice_props.insert("nickname".to_string(), Value::from("Ali"));
    graph.add_vertex("Person", alice_props);

    // Person with name but no nickname
    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    // Note: no nickname property
    graph.add_vertex("Person", bob_props);

    // Person with nickname but no name (unusual case)
    let mut carol_props = HashMap::new();
    carol_props.insert("nickname".to_string(), Value::from("Carol the Great"));
    graph.add_vertex("Person", carol_props);

    graph
}

/// Test COALESCE returns first non-null value
#[test]
fn test_gql_coalesce_first_value() {
    let graph = create_coalesce_test_graph();
    let snapshot = graph.snapshot();

    // Alice has nickname, so COALESCE should return nickname
    let results = graph
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
    let results = graph
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
    let results = graph
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
    let results = graph
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

    let results = graph
        .gql("MATCH (p:Person) WHERE p.name = 'Bob' RETURN COALESCE(p.nickname, p.name)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Bob".to_string()));
}

// =============================================================================
// CASE Expression Tests
// =============================================================================

/// Helper to create a graph for CASE expression tests
fn create_case_test_graph() -> Graph {
    let graph = Graph::new();

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
        graph.add_vertex("Person", props);
    }

    graph
}

/// Test CASE expression with age categorization
#[test]
fn test_gql_case_age_category() {
    let graph = create_case_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
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

    let results = graph
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
    let results = graph
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
    let results = graph
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

    let results = graph
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
// Type Conversion Function Tests
// =============================================================================

/// Helper to create a graph for type conversion tests
fn create_type_conversion_test_graph() -> Graph {
    let graph = Graph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert("age".to_string(), Value::from(30i64));
    props.insert("score".to_string(), Value::from(95.5));
    props.insert("active".to_string(), Value::from(true));
    props.insert("count_str".to_string(), Value::from("42"));
    props.insert("float_str".to_string(), Value::from("3.15"));
    props.insert("bool_str".to_string(), Value::from("true"));
    graph.add_vertex("Person", props);

    graph
}

/// Test toString() converts integer to string
#[test]
fn test_gql_tostring_integer() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
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

    let results = graph
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

    let results = graph
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

    let results = graph
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

    let results = graph
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

    let results = graph
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

    let results = graph.gql("MATCH (p:Person) RETURN toFloat(p.age)").unwrap();

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

    let results = graph
        .gql("MATCH (p:Person) RETURN toFloat(p.float_str)")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - 3.15).abs() < 0.0001, "Expected 3.15, got {}", f);
    } else {
        panic!("Expected Float result");
    }
}

/// Test toBoolean() converts string "true" to true
#[test]
fn test_gql_toboolean_string_true() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = graph
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
    let results = graph
        .gql("MATCH (p:Person) RETURN toBoolean(p.age)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(true));
}

/// Test toBoolean() with "false" string
#[test]
fn test_gql_toboolean_string_false() {
    let graph = Graph::new();
    let mut props = HashMap::new();
    props.insert("status".to_string(), Value::from("false"));
    graph.add_vertex("Test", props);

    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (t:Test) RETURN toBoolean(t.status)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(false));
}

// =============================================================================
// String Function Tests
// =============================================================================

/// Test UPPER/TOUPPER function
#[test]
fn test_gql_upper_function() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = graph.gql("MATCH (p:Person) RETURN upper(p.name)").unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("ALICE".to_string()));
}

/// Test LOWER/TOLOWER function
#[test]
fn test_gql_lower_function() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = graph.gql("MATCH (p:Person) RETURN lower(p.name)").unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("alice".to_string()));
}

/// Test LENGTH/SIZE function for string
#[test]
fn test_gql_length_string() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = graph.gql("MATCH (p:Person) RETURN length(p.name)").unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(5)); // "Alice" has 5 characters
}

/// Test ABS function
#[test]
fn test_gql_abs_function() {
    let graph = Graph::new();
    let mut props = HashMap::new();
    props.insert("balance".to_string(), Value::from(-100i64));
    graph.add_vertex("Account", props);

    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (a:Account) RETURN abs(a.balance)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(100));
}

/// Test TRIM function
#[test]
fn test_gql_trim_function() {
    let graph = Graph::new();
    let mut props = HashMap::new();
    props.insert("text".to_string(), Value::from("  hello world  "));
    graph.add_vertex("Test", props);

    let snapshot = graph.snapshot();

    let results = graph.gql("MATCH (t:Test) RETURN trim(t.text)").unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("hello world".to_string()));
}

/// Test ROUND function
#[test]
fn test_gql_round_function() {
    let graph = create_type_conversion_test_graph();
    let snapshot = graph.snapshot();

    let results = graph.gql("MATCH (p:Person) RETURN round(p.score)").unwrap();

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

    let results = graph.gql("MATCH (p:Person) RETURN floor(p.score)").unwrap();

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

    let results = graph.gql("MATCH (p:Person) RETURN ceil(p.score)").unwrap();

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

    let results = graph
        .gql("MATCH (p:Person) RETURN substring(p.name, 0, 3)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Ali".to_string()));
}

/// Test REPLACE function
#[test]
fn test_gql_replace_function() {
    let graph = Graph::new();
    let mut props = HashMap::new();
    props.insert("text".to_string(), Value::from("hello world"));
    graph.add_vertex("Test", props);

    let snapshot = graph.snapshot();

    let results = graph
        .gql("MATCH (t:Test) RETURN replace(t.text, 'world', 'there')")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("hello there".to_string()));
}
