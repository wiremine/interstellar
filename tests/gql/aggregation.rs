//! Aggregation tests for GQL.
//!
//! Tests for aggregation functions and grouping including:
//! - COUNT, SUM, AVG, MIN, MAX, COLLECT
//! - COUNT(DISTINCT) and COLLECT(DISTINCT)
//! - GROUP BY clause
//! - HAVING clause
//! - Multiple aggregates in single query
//! - Empty result set handling

use interstellar::prelude::*;
use interstellar::storage::InMemoryGraph;
use std::collections::HashMap;

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

    Graph::new(storage)
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

    Graph::new(storage)
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
// HAVING Clause Tests
// =============================================================================

/// Test HAVING with count(*) filter
#[test]
fn test_gql_having_count_filter() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    // Only return groups with more than 1 person
    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.city AS city, count(*) AS cnt GROUP BY p.city HAVING count(*) > 1")
        .unwrap();

    // New York has 3 people, Boston has 2, Chicago has 1
    // HAVING count(*) > 1 should return only New York and Boston
    assert_eq!(results.len(), 2, "Should have 2 groups with count > 1");

    let mut cities: Vec<String> = results
        .iter()
        .filter_map(|r| {
            if let Value::Map(map) = r {
                if let Some(Value::String(city)) = map.get("city") {
                    return Some(city.clone());
                }
            }
            None
        })
        .collect();

    cities.sort();
    assert_eq!(cities, vec!["Boston", "New York"]);
}

/// Test HAVING with count(*) >= filter
#[test]
fn test_gql_having_count_gte() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    // Only return groups with 2 or more people
    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.city AS city, count(*) AS cnt GROUP BY p.city HAVING count(*) >= 2")
        .unwrap();

    assert_eq!(results.len(), 2, "Should have 2 groups with count >= 2");
}

/// Test HAVING with count(*) = filter
#[test]
fn test_gql_having_count_equals() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    // Only return groups with exactly 3 people
    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.city AS city, count(*) AS cnt GROUP BY p.city HAVING count(*) = 3")
        .unwrap();

    assert_eq!(results.len(), 1, "Should have 1 group with count = 3");

    if let Value::Map(map) = &results[0] {
        assert_eq!(
            map.get("city"),
            Some(&Value::String("New York".to_string()))
        );
        assert_eq!(map.get("cnt"), Some(&Value::Int(3)));
    } else {
        panic!("Expected Value::Map");
    }
}

/// Test HAVING with avg() filter
#[test]
fn test_gql_having_avg_filter() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    // Only return groups with average age >= 30
    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.city AS city, avg(p.age) AS avg_age GROUP BY p.city HAVING avg(p.age) >= 30")
        .unwrap();

    // Verify all returned groups have avg_age >= 30
    for result in &results {
        if let Value::Map(map) = result {
            if let Some(Value::Float(avg)) = map.get("avg_age") {
                assert!(*avg >= 30.0, "avg_age should be >= 30, got {}", avg);
            } else if let Some(Value::Int(avg)) = map.get("avg_age") {
                assert!(*avg >= 30, "avg_age should be >= 30, got {}", avg);
            }
        }
    }
}

/// Test HAVING with alias reference
#[test]
fn test_gql_having_with_alias() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    // Use alias in HAVING clause
    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.city AS city, count(*) AS cnt GROUP BY p.city HAVING cnt > 1")
        .unwrap();

    assert_eq!(results.len(), 2, "Should have 2 groups with cnt > 1");
}

/// Test HAVING with AND logic
#[test]
fn test_gql_having_and_condition() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    // Multiple conditions with AND
    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.city AS city, count(*) AS cnt, avg(p.age) AS avg_age GROUP BY p.city HAVING count(*) > 1 AND avg(p.age) < 35")
        .unwrap();

    // Each result should satisfy both conditions
    for result in &results {
        if let Value::Map(map) = result {
            if let Some(Value::Int(cnt)) = map.get("cnt") {
                assert!(*cnt > 1, "cnt should be > 1");
            }
            if let Some(Value::Float(avg)) = map.get("avg_age") {
                assert!(*avg < 35.0, "avg_age should be < 35");
            }
        }
    }
}

/// Test HAVING with OR logic
#[test]
fn test_gql_having_or_condition() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    // Multiple conditions with OR
    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.city AS city, count(*) AS cnt GROUP BY p.city HAVING count(*) = 1 OR count(*) = 3")
        .unwrap();

    // Should return Chicago (1 person) and New York (3 people), but not Boston (2)
    assert_eq!(
        results.len(),
        2,
        "Should have 2 groups with count = 1 OR count = 3"
    );

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
    assert_eq!(counts, vec![1, 3], "Should have counts 1 and 3");
}

/// Test HAVING filters all groups (empty result)
#[test]
fn test_gql_having_filters_all() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    // No group has count > 10
    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.city AS city, count(*) AS cnt GROUP BY p.city HAVING count(*) > 10")
        .unwrap();

    assert_eq!(results.len(), 0, "Should have 0 groups with count > 10");
}

/// Test HAVING with sum() filter
#[test]
fn test_gql_having_sum_filter() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    // Only return groups where sum of ages > 50
    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.city AS city, sum(p.age) AS total_age GROUP BY p.city HAVING sum(p.age) > 50")
        .unwrap();

    // Verify all returned groups have total_age > 50
    for result in &results {
        if let Value::Map(map) = result {
            if let Some(Value::Int(total)) = map.get("total_age") {
                assert!(*total > 50, "total_age should be > 50, got {}", total);
            }
        }
    }
}

/// Test HAVING combined with ORDER BY and LIMIT
#[test]
fn test_gql_having_with_order_by_limit() {
    let graph = create_group_by_test_graph();
    let snapshot = graph.snapshot();

    // Filter, order, and limit
    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.city AS city, count(*) AS cnt GROUP BY p.city HAVING count(*) > 1 ORDER BY cnt DESC LIMIT 1")
        .unwrap();

    assert_eq!(results.len(), 1, "Should have 1 result after LIMIT");

    // Should be New York with the highest count (3) among filtered groups
    if let Value::Map(map) = &results[0] {
        assert_eq!(
            map.get("city"),
            Some(&Value::String("New York".to_string()))
        );
        assert_eq!(map.get("cnt"), Some(&Value::Int(3)));
    }
}
