//! Aggregation and grouping pattern tests.
//!
//! Tests for aggregation queries including:
//! - Counting and basic statistics
//! - Group by operations
//! - Fold and reduce patterns
//! - Min/max with property extraction

#![allow(unused_variables)]

use std::collections::HashMap;

use interstellar::p;
use interstellar::traversal::SnapshotLike;
use interstellar::value::{Value, VertexId};

use crate::common::graphs::{create_small_graph, create_social_graph};

// =============================================================================
// Basic Counting
// =============================================================================

#[test]
fn count_all_vertices() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let count = g.v().count();
    assert_eq!(count, 4); // 3 people + 1 software
}

#[test]
fn count_all_edges() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let count = g.e().count();
    assert_eq!(count, 5);
}

#[test]
fn count_by_label() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let person_count = g.v().has_label("person").count();
    assert_eq!(person_count, 3);

    let software_count = g.v().has_label("software").count();
    assert_eq!(software_count, 1);
}

#[test]
fn count_with_filter() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Count people older than 25
    let count = g
        .v()
        .has_label("person")
        .has_where("age", p::gt(25i64))
        .count();

    assert_eq!(count, 2); // Alice (30), Charlie (35)
}

#[test]
fn count_neighbors() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Count Alice's outgoing neighbors
    let neighbor_count = g.v_ids([tg.alice]).out().count();
    assert_eq!(neighbor_count, 2);
}

// =============================================================================
// Sum Aggregation
// =============================================================================

#[test]
fn sum_numeric_property() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Sum of all ages
    let total_age = g.v().has_label("person").values("age").sum();

    // Alice (30) + Bob (25) + Charlie (35) = 90
    assert_eq!(total_age, Value::Int(90));
}

#[test]
fn sum_filtered_values() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Sum ages of people older than 25
    let sum = g
        .v()
        .has_label("person")
        .has_where("age", p::gt(25i64))
        .values("age")
        .sum();

    // Alice (30) + Charlie (35) = 65
    assert_eq!(sum, Value::Int(65));
}

#[test]
fn sum_empty_returns_zero() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Sum with no matching values
    let sum = g.v().has_value("name", "Nobody").values("age").sum();

    assert_eq!(sum, Value::Int(0));
}

// =============================================================================
// Min/Max Aggregation
// =============================================================================

#[test]
fn min_numeric_property() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let min_age = g.v().has_label("person").values("age").min();
    assert_eq!(min_age, Some(Value::Int(25))); // Bob
}

#[test]
fn max_numeric_property() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let max_age = g.v().has_label("person").values("age").max();
    assert_eq!(max_age, Some(Value::Int(35))); // Charlie
}

#[test]
fn min_max_with_filter() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Min age of people over 25
    let min = g
        .v()
        .has_label("person")
        .has_where("age", p::gt(25i64))
        .values("age")
        .min();

    assert_eq!(min, Some(Value::Int(30))); // Alice
}

#[test]
fn min_max_edge_property() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Min "since" value on knows edges
    let min_since = g.e().has_label("knows").values("since").min();
    assert_eq!(min_since, Some(Value::Int(2019))); // Charlie knows Alice
}

// =============================================================================
// Fold Operations
// =============================================================================

#[test]
fn fold_collects_to_list() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Collect all names into a list
    let names = g.v().has_label("person").values("name").to_list();

    assert_eq!(names.len(), 3);
    assert!(names.contains(&Value::String("Alice".to_string())));
    assert!(names.contains(&Value::String("Bob".to_string())));
    assert!(names.contains(&Value::String("Charlie".to_string())));
}

#[test]
fn fold_with_custom_accumulator() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Custom fold to sum ages
    let total = g
        .v()
        .has_label("person")
        .values("age")
        .fold(0i64, |acc, v| acc + v.as_i64().unwrap_or(0));

    assert_eq!(total, 90);
}

#[test]
fn fold_to_count() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Count using fold
    let count = g.v().has_label("person").fold(0usize, |acc, _| acc + 1);

    assert_eq!(count, 3);
}

// =============================================================================
// To Set (Deduplication)
// =============================================================================

#[test]
fn to_set_deduplicates_values() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get unique labels
    let labels = g.v().label().to_set();

    assert_eq!(labels.len(), 2); // "person" and "software"
}

#[test]
fn to_set_on_injected_duplicates() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let unique = g.inject([1i64, 2i64, 1i64, 3i64, 2i64]).to_set();
    assert_eq!(unique.len(), 3); // 1, 2, 3
}

// =============================================================================
// Property Value Aggregation
// =============================================================================

#[test]
fn aggregate_property_values() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get all ages as a collection
    let ages: Vec<i64> = g
        .v()
        .has_label("person")
        .values("age")
        .to_list()
        .iter()
        .filter_map(|v| v.as_i64())
        .collect();

    assert_eq!(ages.len(), 3);
    assert!(ages.contains(&30));
    assert!(ages.contains(&25));
    assert!(ages.contains(&35));
}

#[test]
fn aggregate_after_navigation() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Sum ages of Alice's friends
    let friends_age_sum = g
        .v_ids([tg.alice])
        .out_labels(&["knows"])
        .values("age")
        .sum();

    assert_eq!(friends_age_sum, Value::Int(25)); // Just Bob
}

// =============================================================================
// Multi-step Aggregation Patterns
// =============================================================================

#[test]
fn count_per_label_pattern() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Count for each label type
    let person_count = g.v().has_label("person").count();
    let software_count = g.v().has_label("software").count();

    assert_eq!(person_count + software_count, 4);
}

#[test]
fn aggregate_edge_properties() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Sum of "since" values on knows edges
    let total_since = g.e().has_label("knows").values("since").sum();

    // 2020 + 2021 + 2019 = 6060
    assert_eq!(total_since, Value::Int(6060));
}

#[test]
fn min_max_string_values() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Min/max on string values (lexicographic)
    let min_name = g.v().has_label("person").values("name").min();
    let max_name = g.v().has_label("person").values("name").max();

    // Alphabetically: Alice < Bob < Charlie
    assert_eq!(min_name, Some(Value::String("Alice".to_string())));
    assert_eq!(max_name, Some(Value::String("Charlie".to_string())));
}

#[test]
fn aggregation_on_filtered_navigation() {
    let tg = create_social_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Count software created by people Alice knows
    let count = g
        .v_ids([tg.alice])
        .out_labels(&["knows"])
        .out_labels(&["created"])
        .has_label("software")
        .count();

    // Bob created Redis
    assert!(count >= 1);
}

// =============================================================================
// Aggregation with Grouping Patterns
// =============================================================================

#[test]
fn manual_group_by_pattern() {
    let tg = create_social_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Group people by city (manually)
    let people = g.v().has_label("person").to_list();

    let mut by_city: HashMap<String, Vec<VertexId>> = HashMap::new();
    for person in &people {
        if let Some(vid) = person.as_vertex_id() {
            if let Some(vertex) = snapshot.storage().get_vertex(vid) {
                if let Some(Value::String(city)) = vertex.properties.get("city") {
                    by_city.entry(city.clone()).or_default().push(vid);
                }
            }
        }
    }

    // Verify grouping worked
    assert!(
        by_city.contains_key("NYC") || by_city.contains_key("SF") || by_city.contains_key("LA")
    );
}

#[test]
fn count_by_edge_label() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let knows_count = g.e().has_label("knows").count();
    let uses_count = g.e().has_label("uses").count();

    assert_eq!(knows_count, 3);
    assert_eq!(uses_count, 2);
    assert_eq!(knows_count + uses_count, 5);
}

// =============================================================================
// Statistical Patterns
// =============================================================================

#[test]
fn calculate_average_manually() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let ages: Vec<i64> = g
        .v()
        .has_label("person")
        .values("age")
        .to_list()
        .iter()
        .filter_map(|v| v.as_i64())
        .collect();

    let sum: i64 = ages.iter().sum();
    let avg = sum as f64 / ages.len() as f64;

    assert!((avg - 30.0).abs() < 0.01); // (30 + 25 + 35) / 3 = 30
}

#[test]
fn find_vertex_with_max_property() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Find the oldest person
    let oldest = g
        .v()
        .has_label("person")
        .order()
        .by_key_desc("age")
        .build()
        .limit(1)
        .to_list();

    assert_eq!(oldest.len(), 1);
    // Should be Charlie (age 35)
    assert_eq!(oldest[0].as_vertex_id(), Some(tg.charlie));
}

#[test]
fn find_vertex_with_min_property() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Find the youngest person
    let youngest = g
        .v()
        .has_label("person")
        .order()
        .by_key_asc("age")
        .build()
        .limit(1)
        .to_list();

    assert_eq!(youngest.len(), 1);
    // Should be Bob (age 25)
    assert_eq!(youngest[0].as_vertex_id(), Some(tg.bob));
}

// =============================================================================
// Fraud Detection Pattern Tests
// =============================================================================
//
// These tests prove we can support complex fraud detection queries like:
//
// ```groovy
// g.V().hasLabel('Transaction')
//   .has('amount', gt(1000))
//   .has('timestamp', gte(now - 86400000))
//   .group()
//     .by(__.in('made').id())
//     .by(
//       fold()
//         .project('count', 'locations', 'totalAmount', 'user')
//           .by(count(local))
//           .by(unfold().values('location').dedup().fold())
//           .by(unfold().values('amount').sum())
//           .by(unfold().in('made').values('email').limit(1))
//     )
//   .unfold()
//   .filter(select(values).select('count').is(gte(3)))
//   .filter(select(values).select('locations').count(local).is(gte(2)))
//   .select(values)
//   .order().by(select('totalAmount'), desc)
// ```

use interstellar::storage::Graph;
use interstellar::traversal::__;

/// Creates a fraud detection test graph with:
/// - Users who make transactions
/// - Transactions with amounts and locations
/// - Some users with suspicious patterns (many transactions from multiple locations)
fn create_fraud_detection_graph() -> (Graph, Vec<VertexId>) {
    let graph = Graph::new();

    // Create users
    let mut user1_props = HashMap::new();
    user1_props.insert(
        "email".to_string(),
        Value::String("alice@example.com".to_string()),
    );
    let user1 = graph.add_vertex("User", user1_props);

    let mut user2_props = HashMap::new();
    user2_props.insert(
        "email".to_string(),
        Value::String("bob@example.com".to_string()),
    );
    let user2 = graph.add_vertex("User", user2_props);

    let mut user3_props = HashMap::new();
    user3_props.insert(
        "email".to_string(),
        Value::String("charlie@example.com".to_string()),
    );
    let user3 = graph.add_vertex("User", user3_props);

    // User1 (Alice) - Suspicious: 4 transactions from 3 different locations, high amounts
    let mut tx1_props = HashMap::new();
    tx1_props.insert("amount".to_string(), Value::Int(1500));
    tx1_props.insert("location".to_string(), Value::String("NYC".to_string()));
    let tx1 = graph.add_vertex("Transaction", tx1_props);

    let mut tx2_props = HashMap::new();
    tx2_props.insert("amount".to_string(), Value::Int(2000));
    tx2_props.insert("location".to_string(), Value::String("LA".to_string()));
    let tx2 = graph.add_vertex("Transaction", tx2_props);

    let mut tx3_props = HashMap::new();
    tx3_props.insert("amount".to_string(), Value::Int(1800));
    tx3_props.insert("location".to_string(), Value::String("Chicago".to_string()));
    let tx3 = graph.add_vertex("Transaction", tx3_props);

    let mut tx4_props = HashMap::new();
    tx4_props.insert("amount".to_string(), Value::Int(2500));
    tx4_props.insert("location".to_string(), Value::String("NYC".to_string()));
    let tx4 = graph.add_vertex("Transaction", tx4_props);

    // User2 (Bob) - Normal: 2 transactions from 1 location
    let mut tx5_props = HashMap::new();
    tx5_props.insert("amount".to_string(), Value::Int(500));
    tx5_props.insert("location".to_string(), Value::String("SF".to_string()));
    let tx5 = graph.add_vertex("Transaction", tx5_props);

    let mut tx6_props = HashMap::new();
    tx6_props.insert("amount".to_string(), Value::Int(300));
    tx6_props.insert("location".to_string(), Value::String("SF".to_string()));
    let tx6 = graph.add_vertex("Transaction", tx6_props);

    // User3 (Charlie) - Borderline: 3 transactions from 2 locations
    let mut tx7_props = HashMap::new();
    tx7_props.insert("amount".to_string(), Value::Int(1200));
    tx7_props.insert("location".to_string(), Value::String("Boston".to_string()));
    let tx7 = graph.add_vertex("Transaction", tx7_props);

    let mut tx8_props = HashMap::new();
    tx8_props.insert("amount".to_string(), Value::Int(1100));
    tx8_props.insert("location".to_string(), Value::String("Boston".to_string()));
    let tx8 = graph.add_vertex("Transaction", tx8_props);

    let mut tx9_props = HashMap::new();
    tx9_props.insert("amount".to_string(), Value::Int(1300));
    tx9_props.insert("location".to_string(), Value::String("Miami".to_string()));
    let tx9 = graph.add_vertex("Transaction", tx9_props);

    // Create "made" edges from users to transactions
    graph.add_edge(user1, tx1, "made", HashMap::new()).unwrap();
    graph.add_edge(user1, tx2, "made", HashMap::new()).unwrap();
    graph.add_edge(user1, tx3, "made", HashMap::new()).unwrap();
    graph.add_edge(user1, tx4, "made", HashMap::new()).unwrap();

    graph.add_edge(user2, tx5, "made", HashMap::new()).unwrap();
    graph.add_edge(user2, tx6, "made", HashMap::new()).unwrap();

    graph.add_edge(user3, tx7, "made", HashMap::new()).unwrap();
    graph.add_edge(user3, tx8, "made", HashMap::new()).unwrap();
    graph.add_edge(user3, tx9, "made", HashMap::new()).unwrap();

    (graph, vec![user1, user2, user3])
}

/// Test: sum() aggregates numeric values
#[test]
fn fraud_pattern_sum_step() {
    let (graph, _users) = create_fraud_detection_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Sum all transaction amounts
    let total = g.v().has_label("Transaction").values("amount").sum();

    // 1500 + 2000 + 1800 + 2500 + 500 + 300 + 1200 + 1100 + 1300 = 12200
    assert_eq!(total, Value::Int(12200));
}

/// Test: count_local() counts elements within a collection using anonymous traversal
#[test]
fn fraud_pattern_count_local_step() {
    let (graph, _users) = create_fraud_detection_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Use append with anonymous traversal to fold then count locally
    let result = g
        .v()
        .has_label("Transaction")
        .append(__.fold().count_local())
        .to_list();

    // All transactions folded into one list, then counted locally = 9
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], Value::Int(9));
}

/// Test: sum_local() sums elements within a collection
#[test]
fn fraud_pattern_sum_local_step() {
    let (graph, _users) = create_fraud_detection_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Collect all amounts then sum them locally
    let amounts: Vec<Value> = g.v().has_label("Transaction").values("amount").to_list();

    // Create a list value and test sum_local on it
    let list = Value::List(amounts);
    let result = g.inject([list]).append(__.sum_local()).to_list();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], Value::Int(12200));
}

/// Test: unfold() expands a list back into individual elements
#[test]
fn fraud_pattern_fold_unfold_roundtrip() {
    let (graph, _users) = create_fraud_detection_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Get original count
    let original_count = g.v().has_label("Transaction").count();

    // Test unfold via anonymous traversal
    let all_amounts: Vec<_> = g.v().has_label("Transaction").values("amount").to_list();

    // Create a folded list and unfold it
    let unfolded = g
        .inject([Value::List(all_amounts)])
        .append(__.unfold())
        .to_list();

    assert_eq!(unfolded.len() as u64, original_count);
    assert_eq!(original_count, 9);
}

/// Test: select_keys() and select_values() extract from map entries
#[test]
fn fraud_pattern_select_keys_values() {
    let (graph, _users) = create_fraud_detection_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Group by label, unfold, then extract keys
    let keys = g
        .v()
        .group()
        .by_label()
        .by_value()
        .build()
        .append(__.unfold().select_keys())
        .to_list();

    // Should have "User" and "Transaction" as keys
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&Value::String("User".to_string())));
    assert!(keys.contains(&Value::String("Transaction".to_string())));
}

/// Test: group().by_label() grouping
#[test]
fn fraud_pattern_group_by_label() {
    let (graph, _users) = create_fraud_detection_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Group all vertices by label
    let grouped = g.v().group().by_label().by_value().build().to_list();

    assert_eq!(grouped.len(), 1);
    if let Value::Map(map) = &grouped[0] {
        assert_eq!(map.len(), 2); // User and Transaction
        assert!(map.contains_key("User"));
        assert!(map.contains_key("Transaction"));
    } else {
        panic!("Expected Value::Map");
    }
}

/// Test: dedup() removes duplicate values (for unique locations)
#[test]
fn fraud_pattern_dedup_locations() {
    let (graph, users) = create_fraud_detection_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Get unique locations for user1's transactions (Alice)
    // Alice has: NYC, LA, Chicago, NYC -> unique: NYC, LA, Chicago
    let locations = g
        .v_ids([users[0]])
        .out_labels(&["made"])
        .values("location")
        .dedup()
        .to_list();

    assert_eq!(locations.len(), 3);
    assert!(locations.contains(&Value::String("NYC".to_string())));
    assert!(locations.contains(&Value::String("LA".to_string())));
    assert!(locations.contains(&Value::String("Chicago".to_string())));
}

/// Test: Complex aggregation pattern - count per user with threshold filtering
#[test]
fn fraud_pattern_count_threshold() {
    let (graph, _users) = create_fraud_detection_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Count transactions per user and filter for users with >= 3 transactions
    // User1 (Alice): 4 transactions
    // User2 (Bob): 2 transactions
    // User3 (Charlie): 3 transactions
    // Expected: 2 users (Alice and Charlie)

    let high_activity_users = g
        .v()
        .has_label("User")
        .where_(__.out_labels(&["made"]).count().is_(p::gte(3i64)))
        .to_list();

    assert_eq!(high_activity_users.len(), 2);
}

/// Test: Complex aggregation pattern - sum amounts per user
#[test]
fn fraud_pattern_sum_per_user() {
    let (graph, users) = create_fraud_detection_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Sum transaction amounts for Alice (user1)
    // Alice: 1500 + 2000 + 1800 + 2500 = 7800
    let alice_total = g
        .v_ids([users[0]])
        .out_labels(&["made"])
        .values("amount")
        .sum();

    assert_eq!(alice_total, Value::Int(7800));

    // Sum transaction amounts for Bob (user2)
    // Bob: 500 + 300 = 800
    let bob_total = g
        .v_ids([users[1]])
        .out_labels(&["made"])
        .values("amount")
        .sum();

    assert_eq!(bob_total, Value::Int(800));
}

/// Test: Full fraud detection pattern simulation
/// This test demonstrates we can support all the steps needed for the fraud query
#[test]
fn fraud_pattern_full_simulation() {
    let (graph, users) = create_fraud_detection_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // For each user, we want to compute:
    // 1. Transaction count
    // 2. Unique location count
    // 3. Total amount
    // Then filter for suspicious users (count >= 3 AND locations >= 2)

    // Get all users with their transaction data
    let suspicious_users: Vec<_> = g
        .v()
        .has_label("User")
        .where_(
            // Has at least 3 transactions
            __.out_labels(&["made"]).count().is_(p::gte(3i64)),
        )
        .where_(
            // Has transactions from at least 2 different locations
            __.out_labels(&["made"])
                .values("location")
                .dedup()
                .count()
                .is_(p::gte(2i64)),
        )
        .to_list();

    // Should find Alice (4 tx, 3 locations) and Charlie (3 tx, 2 locations)
    assert_eq!(suspicious_users.len(), 2);

    // Verify it's the right users
    let suspicious_ids: Vec<_> = suspicious_users
        .iter()
        .filter_map(|v| v.as_vertex_id())
        .collect();
    assert!(suspicious_ids.contains(&users[0])); // Alice
    assert!(suspicious_ids.contains(&users[2])); // Charlie
    assert!(!suspicious_ids.contains(&users[1])); // Bob should NOT be included
}

/// Test: project() with count_local() and sum_local() in by() modulator
#[test]
fn fraud_pattern_project_with_local_aggregations() {
    let (graph, users) = create_fraud_detection_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // For Alice, collect transactions then create projection
    let transactions: Vec<Value> = g.v_ids([users[0]]).out_labels(&["made"]).to_list();

    // Create a folded list and project on it
    let projection = g
        .inject([Value::List(transactions)])
        .project(&["count", "total"])
        .by(__.count_local())
        .by(__.unfold().values("amount").sum())
        .build()
        .to_list();

    assert_eq!(projection.len(), 1);
    if let Value::Map(map) = &projection[0] {
        assert_eq!(map.get("count"), Some(&Value::Int(4)));
        assert_eq!(map.get("total"), Some(&Value::Int(7800)));
    } else {
        panic!("Expected Value::Map");
    }
}

/// Test: Anonymous traversal factory functions work correctly
#[test]
fn fraud_pattern_anonymous_factory_functions() {
    let (graph, _users) = create_fraud_detection_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Test __.fold() via append - folds all values into a single list
    let folded = g
        .v()
        .has_label("Transaction")
        .values("amount")
        .append(__.fold())
        .to_list();
    // All 9 amounts folded into a single list
    assert_eq!(folded.len(), 1);
    if let Value::List(list) = &folded[0] {
        assert_eq!(list.len(), 9);
    } else {
        panic!("Expected Value::List");
    }

    // Test __.count_local()
    let list = Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
    let local_count = g.inject([list]).append(__.count_local()).to_list();
    assert_eq!(local_count.len(), 1);
    assert_eq!(local_count[0], Value::Int(3));

    // Test __.sum_local()
    let list2 = Value::List(vec![Value::Int(10), Value::Int(20), Value::Int(30)]);
    let local_sum = g.inject([list2]).append(__.sum_local()).to_list();
    assert_eq!(local_sum.len(), 1);
    assert_eq!(local_sum[0], Value::Int(60));

    // Test __.select_keys() on a map
    let mut test_map = HashMap::new();
    test_map.insert("a".to_string(), Value::Int(1));
    test_map.insert("b".to_string(), Value::Int(2));
    let keys = g
        .inject([Value::Map(test_map)])
        .append(__.unfold().select_keys())
        .to_list();
    assert_eq!(keys.len(), 2);

    // Test __.select_values() on a map
    let mut test_map2 = HashMap::new();
    test_map2.insert("x".to_string(), Value::Int(100));
    test_map2.insert("y".to_string(), Value::Int(200));
    let values = g
        .inject([Value::Map(test_map2)])
        .append(__.unfold().select_values())
        .to_list();
    assert_eq!(values.len(), 2);
    assert!(values.contains(&Value::Int(100)));
    assert!(values.contains(&Value::Int(200)));
}
