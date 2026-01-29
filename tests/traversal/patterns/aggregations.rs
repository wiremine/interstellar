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
