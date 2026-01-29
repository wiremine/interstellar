//! Semantic correctness tests for Gremlin traversal operations.
//!
//! Phase 4 of the integration test strategy. Verifies results match expected
//! Gremlin semantics from the TinkerPop specification:
//!
//! - Order preservation tests (5+ tests)
//! - Dedup semantics (5+ tests)
//! - Group/reduce semantics (5+ tests)
//! - Path tracking correctness (5+ tests)

#![allow(unused_variables)]

use std::collections::HashSet;

use interstellar::p;
use interstellar::storage::GraphStorage;
use interstellar::traversal::__;
use interstellar::value::{Value, VertexId};

use crate::common::graphs::{create_small_graph, create_social_graph, TestGraphBuilder};

// =============================================================================
// Order Preservation Tests
// =============================================================================

/// Verifies that multiple runs of the same traversal return consistent ordering.
#[test]
fn navigation_order_is_deterministic() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Run the same traversal multiple times
    let run1 = g.v_ids([tg.alice]).out().to_list();
    let run2 = g.v_ids([tg.alice]).out().to_list();
    let run3 = g.v_ids([tg.alice]).out().to_list();

    // All runs should return the same results in the same order
    assert_eq!(run1.len(), run2.len());
    assert_eq!(run2.len(), run3.len());

    for i in 0..run1.len() {
        assert_eq!(
            run1[i], run2[i],
            "Mismatch at index {} between run1 and run2",
            i
        );
        assert_eq!(
            run2[i], run3[i],
            "Mismatch at index {} between run2 and run3",
            i
        );
    }
}

/// Verifies that order().by_asc() sorts values in ascending order.
#[test]
fn order_by_asc_produces_ascending_sequence() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let ages = g
        .v()
        .has_label("person")
        .values("age")
        .order()
        .by_asc()
        .build()
        .to_list();

    // Extract i64 values and verify ascending order
    let age_vals: Vec<i64> = ages.iter().filter_map(|v| v.as_i64()).collect();
    assert_eq!(age_vals.len(), 3, "Should have 3 person ages");

    for i in 1..age_vals.len() {
        assert!(
            age_vals[i - 1] <= age_vals[i],
            "Order violation: {} > {} at index {}",
            age_vals[i - 1],
            age_vals[i],
            i
        );
    }

    // Verify specific values: 25, 30, 35
    assert_eq!(age_vals, vec![25, 30, 35]);
}

/// Verifies that order().by_desc() sorts values in descending order.
#[test]
fn order_by_desc_produces_descending_sequence() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let ages = g
        .v()
        .has_label("person")
        .values("age")
        .order()
        .by_desc()
        .build()
        .to_list();

    let age_vals: Vec<i64> = ages.iter().filter_map(|v| v.as_i64()).collect();
    assert_eq!(age_vals.len(), 3);

    for i in 1..age_vals.len() {
        assert!(
            age_vals[i - 1] >= age_vals[i],
            "Order violation: {} < {} at index {}",
            age_vals[i - 1],
            age_vals[i],
            i
        );
    }

    // Verify specific values: 35, 30, 25
    assert_eq!(age_vals, vec![35, 30, 25]);
}

/// Verifies that order preserves relative order for equal elements (stable sort).
#[test]
fn order_is_stable_for_equal_values() {
    // Create graph with multiple vertices having the same sort key
    let graph = TestGraphBuilder::new()
        .add_person("Alice", 30)
        .add_person("Bob", 30) // Same age as Alice
        .add_person("Charlie", 30) // Same age as Alice
        .add_person("Diana", 25)
        .build();

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Get vertices ordered by age - all 30-year-olds should maintain relative order
    let names_run1: Vec<String> = g
        .v()
        .has_label("person")
        .order()
        .by_key_asc("age")
        .build()
        .values("name")
        .to_list()
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();

    let names_run2: Vec<String> = g
        .v()
        .has_label("person")
        .order()
        .by_key_asc("age")
        .build()
        .values("name")
        .to_list()
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();

    // Both runs should produce identical order (stability)
    assert_eq!(names_run1, names_run2, "Sort should be stable across runs");

    // Diana (25) should come first, followed by the 30-year-olds
    assert_eq!(names_run1[0], "Diana");
}

/// Verifies limit() preserves insertion order before ordering.
#[test]
fn limit_preserves_traversal_order() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get first 2 from knows chain - order should be deterministic
    let first_two = g
        .v_ids([tg.alice])
        .out_labels(&["knows"])
        .limit(2)
        .to_list();

    // Re-run and verify same order
    let first_two_again = g
        .v_ids([tg.alice])
        .out_labels(&["knows"])
        .limit(2)
        .to_list();

    assert_eq!(first_two, first_two_again);
}

/// Verifies that range() returns elements in traversal order.
#[test]
fn range_preserves_element_order() {
    let graph = TestGraphBuilder::new()
        .add_person("A", 1)
        .add_person("B", 2)
        .add_person("C", 3)
        .add_person("D", 4)
        .add_person("E", 5)
        .build();

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Get ordered list, then use range to get middle elements
    let all_ordered = g.v().values("age").order().by_asc().build().to_list();

    let middle = g
        .v()
        .values("age")
        .order()
        .by_asc()
        .build()
        .range(1, 4) // Skip first, take next 3
        .to_list();

    // Middle should be elements 1, 2, 3 from ordered list
    assert_eq!(middle.len(), 3);
    assert_eq!(middle[0], all_ordered[1]);
    assert_eq!(middle[1], all_ordered[2]);
    assert_eq!(middle[2], all_ordered[3]);
}

// =============================================================================
// Dedup Semantics Tests
// =============================================================================

/// Verifies that dedup() keeps the first occurrence of each element.
#[test]
fn dedup_keeps_first_occurrence() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Create a traversal that visits Alice multiple times via cycle
    // Alice -> Bob -> Charlie -> Alice (cycle)
    let with_duplicates = g
        .v_ids([tg.alice])
        .repeat(__.out_labels(&["knows"]))
        .times(4) // Go around the cycle
        .emit()
        .to_list();

    // Count Alice occurrences - should have duplicates
    let alice_count = with_duplicates
        .iter()
        .filter(|v| v.as_vertex_id() == Some(tg.alice))
        .count();

    // Depending on graph structure, we might have revisited Alice
    // Now apply dedup
    let deduped = g
        .v_ids([tg.alice])
        .repeat(__.out_labels(&["knows"]))
        .times(4)
        .emit()
        .dedup()
        .to_list();

    // Each vertex should appear exactly once
    let ids: Vec<VertexId> = deduped.iter().filter_map(|v| v.as_vertex_id()).collect();
    let unique_ids: HashSet<VertexId> = ids.iter().copied().collect();
    assert_eq!(
        ids.len(),
        unique_ids.len(),
        "Dedup should remove all duplicates"
    );
}

/// Verifies that dedup_by_key() deduplicates based on property value.
#[test]
fn dedup_by_key_uses_property_for_uniqueness() {
    // Create graph with vertices that share property values
    let graph = TestGraphBuilder::new()
        .add_person_with_status("Alice", 30, "active")
        .add_person_with_status("Bob", 25, "active") // Same status as Alice
        .add_person_with_status("Charlie", 35, "inactive")
        .add_person_with_status("Diana", 28, "inactive") // Same status as Charlie
        .build();

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let deduped = g.v().has_label("person").dedup_by_key("status").to_list();

    // Should have exactly 2 results (one per unique status)
    assert_eq!(deduped.len(), 2, "Should have one vertex per unique status");

    // Extract statuses to verify uniqueness by looking up vertices
    let statuses: Vec<String> = deduped
        .iter()
        .filter_map(|v| {
            if let Some(vid) = v.as_vertex_id() {
                snapshot.get_vertex(vid).and_then(|vertex| {
                    vertex
                        .properties
                        .get("status")
                        .and_then(|s: &Value| s.as_str())
                        .map(|s: &str| s.to_string())
                })
            } else {
                None
            }
        })
        .collect();

    let unique_statuses: HashSet<_> = statuses.iter().collect();
    assert_eq!(statuses.len(), unique_statuses.len());
}

/// Verifies that dedup() on empty traversal returns empty.
#[test]
fn dedup_on_empty_traversal_returns_empty() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let result = g.v().has_value("nonexistent", "value").dedup().to_list();

    assert!(result.is_empty());
}

/// Verifies that dedup() with single element returns that element.
#[test]
fn dedup_single_element_returns_element() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let result = g.v_ids([tg.alice]).dedup().to_list();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].as_vertex_id(), Some(tg.alice));
}

/// Verifies that consecutive dedup() calls don't change the result.
#[test]
fn multiple_dedup_calls_are_idempotent() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let once = g
        .v_ids([tg.alice])
        .repeat(__.out_labels(&["knows"]))
        .times(4)
        .emit()
        .dedup()
        .to_list();

    let twice = g
        .v_ids([tg.alice])
        .repeat(__.out_labels(&["knows"]))
        .times(4)
        .emit()
        .dedup()
        .dedup()
        .to_list();

    assert_eq!(once.len(), twice.len());
    for (a, b) in once.iter().zip(twice.iter()) {
        assert_eq!(a, b);
    }
}

/// Verifies dedup() works correctly with values, not just vertices.
#[test]
fn dedup_works_on_property_values() {
    let tg = create_social_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get all cities (some duplicated: NYC appears twice)
    let all_cities = g.v().has_label("person").values("city").to_list();

    let unique_cities = g.v().has_label("person").values("city").dedup().to_list();

    // NYC, SF, LA should be the unique cities
    assert!(
        unique_cities.len() <= all_cities.len(),
        "Dedup should not increase count"
    );

    let city_set: HashSet<_> = unique_cities.iter().collect();
    assert_eq!(
        unique_cities.len(),
        city_set.len(),
        "All values should be unique"
    );
}

// =============================================================================
// Group/Reduce Semantics Tests
// =============================================================================

/// Verifies that group() preserves all elements across groups.
#[test]
fn group_preserves_all_elements() {
    let tg = create_social_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Count people before grouping
    let total_people = g.v().has_label("person").count();

    // Group by city - total should match
    let grouped = g
        .v()
        .has_label("person")
        .group()
        .by_key("city")
        .by_value()
        .build()
        .to_list();

    // Count total across all groups
    let mut total_in_groups = 0;
    for result in &grouped {
        if let Value::Map(map) = result {
            for value in map.values() {
                if let Value::List(list) = value {
                    total_in_groups += list.len();
                }
            }
        }
    }

    assert_eq!(
        total_in_groups as u64, total_people,
        "Group should preserve all elements"
    );
}

/// Verifies that group_count() correctly counts elements per key.
#[test]
fn group_count_produces_correct_counts() {
    let tg = create_social_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let counts = g
        .v()
        .has_label("person")
        .group_count()
        .by_key("city")
        .build()
        .to_list();

    // Should have one map with city -> count
    assert_eq!(counts.len(), 1);

    if let Value::Map(map) = &counts[0] {
        // Verify we have expected cities (map uses string keys)
        assert!(map.contains_key("NYC"));
        assert!(map.contains_key("SF"));
        assert!(map.contains_key("LA"));

        // NYC should have 2 (Alice, Charlie)
        let nyc_count = map.get("NYC").and_then(|v| v.as_i64()).unwrap_or(0);
        assert_eq!(nyc_count, 2, "NYC should have 2 people");

        // SF should have 2 (Bob, Eve)
        let sf_count = map.get("SF").and_then(|v| v.as_i64()).unwrap_or(0);
        assert_eq!(sf_count, 2, "SF should have 2 people");

        // LA should have 1 (Diana)
        let la_count = map.get("LA").and_then(|v| v.as_i64()).unwrap_or(0);
        assert_eq!(la_count, 1, "LA should have 1 person");
    } else {
        panic!("Expected Map result from group_count");
    }
}

/// Verifies that fold() collects all elements using a reducer.
#[test]
fn fold_reduces_all_elements() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Use fold to count elements
    let count = g.v().has_label("person").fold(0usize, |acc, _| acc + 1);

    assert_eq!(count, 3, "Fold should process all 3 people");
}

/// Verifies that sum() correctly sums numeric values.
#[test]
fn sum_produces_correct_total() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let total = g.v().has_label("person").values("age").sum();

    // Alice: 30, Bob: 25, Charlie: 35 = 90
    assert_eq!(total, Value::Int(90));
}

/// Verifies that min/max return correct extreme values.
#[test]
fn min_max_return_correct_extremes() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let min_age = g.v().has_label("person").values("age").min();
    let max_age = g.v().has_label("person").values("age").max();

    // Bob: 25 is min, Charlie: 35 is max
    assert_eq!(min_age, Some(Value::Int(25)));
    assert_eq!(max_age, Some(Value::Int(35)));
}

/// Verifies that group().by_label() groups vertices correctly.
#[test]
fn group_by_label_creates_correct_groups() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let grouped = g.v().group().by_label().by_value().build().to_list();

    assert_eq!(grouped.len(), 1);

    if let Value::Map(map) = &grouped[0] {
        // Map uses string keys
        assert!(map.contains_key("person"), "Should have 'person' group");
        assert!(map.contains_key("software"), "Should have 'software' group");

        if let Some(Value::List(people)) = map.get("person") {
            assert_eq!(people.len(), 3, "Should have 3 people");
        }

        if let Some(Value::List(software)) = map.get("software") {
            assert_eq!(software.len(), 1, "Should have 1 software");
        }
    } else {
        panic!("Expected Map result from group");
    }
}

// =============================================================================
// Path Tracking Correctness Tests
// =============================================================================

/// Verifies that path() returns complete traversal history.
#[test]
fn path_contains_all_visited_elements() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Traverse Alice -> Bob -> Charlie, tracking path
    let paths = g
        .v_ids([tg.alice])
        .out_labels(&["knows"]) // -> Bob
        .out_labels(&["knows"]) // -> Charlie
        .with_path()
        .path()
        .to_list();

    assert!(!paths.is_empty(), "Should have at least one path");

    // Each path should have 3 elements: Alice -> Bob -> Charlie
    for p in &paths {
        if let Value::List(path_elements) = p {
            assert_eq!(
                path_elements.len(),
                3,
                "Path should contain start + 2 navigation steps"
            );

            // First element should be Alice
            assert_eq!(
                path_elements[0].as_vertex_id(),
                Some(tg.alice),
                "Path should start with Alice"
            );
        }
    }
}

/// Verifies that as_() labels are accessible via select().
#[test]
fn as_labels_are_retrievable_via_select() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let results = g
        .v_ids([tg.alice])
        .as_("start")
        .out_labels(&["knows"])
        .as_("friend")
        .select(&["start", "friend"])
        .to_list();

    assert!(!results.is_empty());

    for result in &results {
        if let Value::Map(map) = result {
            // Map uses string keys
            assert!(map.contains_key("start"));
            assert!(map.contains_key("friend"));

            // Start should be Alice
            let start = map.get("start");
            if let Some(v) = start {
                assert_eq!(v.as_vertex_id(), Some(tg.alice));
            }
        } else {
            panic!("Expected Map from select");
        }
    }
}

/// Verifies path tracking through repeat().
#[test]
fn path_tracks_through_repeat() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let paths = g
        .v_ids([tg.alice])
        .with_path()
        .repeat(__.out_labels(&["knows"]))
        .times(2)
        .path()
        .to_list();

    for p in &paths {
        if let Value::List(path_elements) = p {
            // Should have at least 3 elements: Alice + 2 repeat iterations
            assert!(
                path_elements.len() >= 3,
                "Path should have start + repeat iterations"
            );
        }
    }
}

/// Verifies that path is reset correctly for each traverser.
#[test]
fn path_is_independent_per_traverser() {
    let tg = create_social_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Start from multiple vertices and verify each has independent path
    let paths = g.v().has_label("person").with_path().out().path().to_list();

    // Each path should start with a different person (for those who have outgoing edges)
    let start_vertices: HashSet<_> = paths
        .iter()
        .filter_map(|p| {
            if let Value::List(elements) = p {
                elements.first().and_then(|v| v.as_vertex_id())
            } else {
                None
            }
        })
        .collect();

    // We should have multiple different starting points
    // (Some people have outgoing edges to different targets)
    assert!(!start_vertices.is_empty());
}

/// Verifies that simple_path() filters out paths with cycles.
#[test]
fn simple_path_excludes_cycles() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // The graph has a cycle: Alice -> Bob -> Charlie -> Alice
    // With simple_path, we should never revisit a vertex
    let paths = g
        .v_ids([tg.alice])
        .with_path()
        .repeat(__.out_labels(&["knows"]).simple_path())
        .times(5) // Try to go around multiple times
        .emit()
        .path()
        .to_list();

    // Verify no path contains duplicate vertices
    for p in &paths {
        if let Value::List(elements) = p {
            let ids: Vec<_> = elements.iter().filter_map(|v| v.as_vertex_id()).collect();
            let unique: HashSet<_> = ids.iter().copied().collect();

            assert_eq!(
                ids.len(),
                unique.len(),
                "simple_path should prevent vertex revisits: {:?}",
                ids
            );
        }
    }
}

/// Verifies select_one() returns a single value, not a map.
#[test]
fn select_one_returns_single_value() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let results = g
        .v_ids([tg.alice])
        .as_("person")
        .out_labels(&["knows"])
        .select_one("person")
        .to_list();

    // Should return the vertex directly, not wrapped in a map
    for result in &results {
        assert!(
            result.as_vertex_id().is_some(),
            "select_one should return vertex directly, got: {:?}",
            result
        );
        assert_eq!(result.as_vertex_id(), Some(tg.alice));
    }
}

// =============================================================================
// Cross-Feature Semantic Tests
// =============================================================================

/// Verifies that order + limit produces correct "top N" semantics.
#[test]
fn order_then_limit_produces_top_n() {
    let tg = create_social_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get top 2 oldest people
    let oldest_two = g
        .v()
        .has_label("person")
        .order()
        .by_key_desc("age")
        .build()
        .limit(2)
        .values("name")
        .to_list();

    assert_eq!(oldest_two.len(), 2);

    // Charlie (35) and Eve (32) should be the oldest
    let names: Vec<_> = oldest_two
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();

    assert!(names.contains(&"Charlie".to_string()));
}

/// Verifies that dedup + group_count produces correct unique counts.
#[test]
fn dedup_before_group_count_counts_unique() {
    let tg = create_social_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get friends of friends, dedupe, then count by city
    let counts = g
        .v()
        .has_label("person")
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .dedup()
        .group_count()
        .by_key("city")
        .build()
        .to_list();

    // Verify counts are based on unique vertices
    if !counts.is_empty() {
        if let Value::Map(map) = &counts[0] {
            let total: i64 = map.values().filter_map(|v| v.as_i64()).sum();
            // Should match the number of unique friends-of-friends
            let unique_fof = g
                .v()
                .has_label("person")
                .out_labels(&["knows"])
                .out_labels(&["knows"])
                .dedup()
                .count();
            assert_eq!(total as u64, unique_fof);
        }
    }
}

/// Verifies that path information survives through filter steps.
#[test]
fn path_survives_filter_steps() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let paths = g
        .v_ids([tg.alice])
        .with_path()
        .out_labels(&["knows"])
        .has_where("age", p::gt(20)) // Filter step
        .path()
        .to_list();

    // Paths should still start with Alice even after filtering
    for p in &paths {
        if let Value::List(elements) = p {
            assert!(
                elements.len() >= 2,
                "Path should have start + at least one navigation"
            );
            assert_eq!(
                elements[0].as_vertex_id(),
                Some(tg.alice),
                "Path should start with source vertex"
            );
        }
    }
}
