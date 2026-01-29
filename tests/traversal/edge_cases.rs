//! Edge case tests for traversal engine.
//!
//! Tests boundary conditions and edge cases including:
//! - Empty result handling
//! - Single element operations
//! - Missing property handling
//! - Type coercion scenarios

#![allow(unused_variables)]

use std::collections::HashMap;

use interstellar::p;
use interstellar::storage::Graph;
use interstellar::traversal::__;
use interstellar::value::Value;

use crate::common::graphs::{create_empty_graph, create_small_graph};

// =============================================================================
// Empty Traversal Handling (10+ tests)
// =============================================================================

#[test]
fn empty_graph_v_returns_empty_list() {
    let graph = create_empty_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let results = g.v().to_list();
    assert!(results.is_empty());
}

#[test]
fn empty_graph_e_returns_empty_list() {
    let graph = create_empty_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let results = g.e().to_list();
    assert!(results.is_empty());
}

#[test]
fn empty_graph_count_returns_zero() {
    let graph = create_empty_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    assert_eq!(g.v().count(), 0);
    assert_eq!(g.e().count(), 0);
}

#[test]
fn empty_graph_out_returns_empty() {
    let graph = create_empty_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let results = g.v().out().to_list();
    assert!(results.is_empty());
}

#[test]
fn empty_graph_values_returns_empty() {
    let graph = create_empty_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let results = g.v().values("name").to_list();
    assert!(results.is_empty());
}

#[test]
fn filter_removes_all_vertices() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter that matches nothing
    let results = g.v().has_value("name", "NonexistentPerson").to_list();
    assert!(results.is_empty());
}

#[test]
fn filter_removes_all_then_chained_steps() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Chained steps after empty should remain empty
    let results = g
        .v()
        .has_value("name", "NonexistentPerson")
        .out()
        .values("name")
        .to_list();
    assert!(results.is_empty());
}

#[test]
fn empty_traversal_in_where_filters_all() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // where_() with non-matching sub-traversal should filter all
    // No "manages" edges exist in small_graph
    let results = g
        .v()
        .has_label("person")
        .where_(__.out_labels(&["manages"]))
        .to_list();
    assert!(results.is_empty());
}

#[test]
fn empty_start_with_v_ids_nonexistent() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Request vertex IDs that don't exist
    let results = g.v_ids([interstellar::value::VertexId(99999)]).to_list();
    assert!(results.is_empty());
}

#[test]
fn empty_navigation_chain() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // GraphDB has no outgoing edges
    let results = g.v_ids([tg.graphdb]).out().to_list();
    assert!(results.is_empty());

    // Chained navigation from empty should stay empty
    let results = g.v_ids([tg.graphdb]).out().out().out().to_list();
    assert!(results.is_empty());
}

#[test]
fn empty_dedup_on_empty_traversal() {
    let graph = create_empty_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let results = g.v().dedup().to_list();
    assert!(results.is_empty());
}

#[test]
fn empty_limit_on_empty_traversal() {
    let graph = create_empty_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let results = g.v().limit(10).to_list();
    assert!(results.is_empty());
}

#[test]
fn empty_traversal_sum_returns_zero() {
    let graph = create_empty_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let result = g.v().values("age").sum();
    assert_eq!(result, Value::Int(0));
}

#[test]
fn empty_traversal_min_returns_none() {
    let graph = create_empty_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let result = g.v().values("age").min();
    assert!(result.is_none());
}

#[test]
fn empty_traversal_max_returns_none() {
    let graph = create_empty_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let result = g.v().values("age").max();
    assert!(result.is_none());
}

#[test]
fn empty_traversal_next_returns_none() {
    let graph = create_empty_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    assert!(g.v().next().is_none());
}

#[test]
fn empty_traversal_has_next_returns_false() {
    let graph = create_empty_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    assert!(!g.v().has_next());
}

// =============================================================================
// Single Element Operations (5+ tests)
// =============================================================================

#[test]
fn single_vertex_one_succeeds() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // one() on single result should succeed
    let result = g.v_ids([tg.alice]).one();
    assert!(result.is_ok());
    assert_eq!(result.unwrap().as_vertex_id(), Some(tg.alice));
}

#[test]
fn single_vertex_after_filter_one_succeeds() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // one() on single result after filter
    let result = g.v().has_value("name", "Alice").one();
    assert!(result.is_ok());
}

#[test]
fn one_fails_on_multiple_results() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // one() should fail on multiple results
    let result = g.v().has_label("person").one();
    assert!(result.is_err());
}

#[test]
fn one_fails_on_empty_results() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // one() should fail on empty results
    let result = g.v().has_value("name", "Nobody").one();
    assert!(result.is_err());
}

#[test]
fn single_element_min_max_equal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // min/max on single value should be equal
    let min = g.v_ids([tg.alice]).values("age").min();
    let max = g.v_ids([tg.alice]).values("age").max();

    assert!(min.is_some());
    assert!(max.is_some());
    assert_eq!(min, max); // Single element: min == max
}

#[test]
fn single_element_count_is_one() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    assert_eq!(g.v_ids([tg.alice]).count(), 1);
}

#[test]
fn single_element_dedup_unchanged() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let results = g.v_ids([tg.alice]).dedup().to_list();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn single_element_limit_larger_than_one() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // limit(10) on single element should return 1
    let results = g.v_ids([tg.alice]).limit(10).to_list();
    assert_eq!(results.len(), 1);
}

// =============================================================================
// Missing Property Handling (5+ tests)
// =============================================================================

#[test]
fn values_skips_vertices_without_property() {
    let graph = Graph::new();
    let v1 = graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("A".to_string()));
        props
    });
    let v2 = graph.add_vertex("test", HashMap::new()); // No "name" property

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // values() should skip vertices without the property
    let names = g.v().values("name").to_list();
    assert_eq!(names.len(), 1);
    assert_eq!(names[0], Value::String("A".to_string()));
}

#[test]
fn has_property_filters_out_missing() {
    let graph = Graph::new();
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("A".to_string()));
        props
    });
    graph.add_vertex("test", HashMap::new()); // No "name" property
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("C".to_string()));
        props
    });

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // has() filters out vertices without property
    let with_name = g.v().has("name").to_list();
    assert_eq!(with_name.len(), 2);
}

#[test]
fn has_value_filters_out_missing_property() {
    let graph = Graph::new();
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props
    });
    graph.add_vertex("test", HashMap::new()); // No properties

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // has_value should not match vertex without the property
    let results = g.v().has_value("name", "Alice").to_list();
    assert_eq!(results.len(), 1);
}

#[test]
fn has_where_on_missing_property() {
    let graph = Graph::new();
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("age".to_string(), Value::Int(30));
        props
    });
    graph.add_vertex("test", HashMap::new()); // No "age" property

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // has_where on missing property should filter that vertex
    let results = g.v().has_where("age", p::gt(20)).to_list();
    assert_eq!(results.len(), 1);
}

#[test]
fn multiple_properties_some_missing() {
    let graph = Graph::new();
    graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props
    });
    graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        // No age
        props
    });
    graph.add_vertex("person", {
        let mut props = HashMap::new();
        // No name
        props.insert("age".to_string(), Value::Int(25));
        props
    });

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Only vertices with both properties match
    let with_both = g.v().has("name").has("age").to_list();
    assert_eq!(with_both.len(), 1);

    // Count of name values
    let names = g.v().values("name").to_list();
    assert_eq!(names.len(), 2);

    // Count of age values
    let ages = g.v().values("age").to_list();
    assert_eq!(ages.len(), 2);
}

#[test]
fn aggregation_on_partially_missing_properties() {
    let graph = Graph::new();
    graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("age".to_string(), Value::Int(30));
        props
    });
    graph.add_vertex("person", HashMap::new()); // No age
    graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("age".to_string(), Value::Int(20));
        props
    });

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // sum/min/max should only consider vertices with the property
    let sum = g.v().values("age").sum();
    assert_eq!(sum, Value::Int(50)); // 30 + 20

    let min = g.v().values("age").min();
    assert_eq!(min, Some(Value::Int(20)));

    let max = g.v().values("age").max();
    assert_eq!(max, Some(Value::Int(30)));
}

#[test]
fn order_by_missing_property() {
    let graph = Graph::new();
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("B".to_string()));
        props.insert("priority".to_string(), Value::Int(2));
        props
    });
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("A".to_string()));
        // No priority
        props
    });
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("C".to_string()));
        props.insert("priority".to_string(), Value::Int(1));
        props
    });

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Ordering by property - behavior for missing property may vary
    // Just verify it doesn't panic
    let results = g.v().values("priority").order().by_asc().build().to_list();
    // Should have 2 values (vertices with priority)
    assert_eq!(results.len(), 2);
}

// =============================================================================
// Type Coercion Scenarios (5+ tests)
// =============================================================================

#[test]
fn mixed_int_float_sum() {
    let graph = Graph::new();
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::Int(10));
        props
    });
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::Float(5.5));
        props
    });
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::Int(20));
        props
    });

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Sum of mixed Int/Float should work and produce Float
    let result = g.v().values("value").sum();
    if let Value::Float(f) = result {
        assert!((f - 35.5).abs() < 1e-10);
    } else {
        panic!("Expected Float for mixed sum, got {:?}", result);
    }
}

#[test]
fn mixed_int_float_min() {
    let graph = Graph::new();
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::Int(10));
        props
    });
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::Float(5.5));
        props
    });
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::Int(20));
        props
    });

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // min of mixed Int/Float uses type discriminant ordering (Int < Float)
    // so the minimum is the smallest Int, not the smallest numeric value
    let result = g.v().values("value").min();
    assert!(result.is_some());
    // Int(10) is minimum because Int < Float in type ordering
    assert_eq!(result, Some(Value::Int(10)));
}

#[test]
fn mixed_int_float_max() {
    let graph = Graph::new();
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::Int(10));
        props
    });
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::Float(25.5));
        props
    });
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::Int(20));
        props
    });

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // max of mixed Int/Float uses type discriminant ordering (Int < Float)
    // so the maximum is the largest Float, not the largest numeric value
    let result = g.v().values("value").max();
    assert!(result.is_some());
    // Float(25.5) is maximum because Float > Int in type ordering
    if let Some(Value::Float(f)) = result {
        assert!((f - 25.5).abs() < 1e-10);
    } else {
        panic!("Expected Float maximum, got {:?}", result);
    }
}

#[test]
fn int_comparison_with_int_predicate() {
    let graph = Graph::new();
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::Int(42));
        props
    });
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::Int(10));
        props
    });

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Exact match with Int
    let results = g.v().has_where("value", p::eq(42i64)).to_list();
    assert_eq!(results.len(), 1);

    // Greater than comparison
    let results = g.v().has_where("value", p::gt(30i64)).to_list();
    assert_eq!(results.len(), 1);
}

#[test]
fn string_comparison_exact_match() {
    let graph = Graph::new();
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props
    });
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        props
    });

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Exact string match
    let results = g.v().has_value("name", "Alice").to_list();
    assert_eq!(results.len(), 1);

    // Non-matching string
    let results = g.v().has_value("name", "Charlie").to_list();
    assert!(results.is_empty());
}

#[test]
fn bool_property_filtering() {
    let graph = Graph::new();
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("active".to_string(), Value::Bool(true));
        props
    });
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("active".to_string(), Value::Bool(false));
        props
    });

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Filter by boolean property
    let active = g.v().has_value("active", true).to_list();
    assert_eq!(active.len(), 1);

    let inactive = g.v().has_value("active", false).to_list();
    assert_eq!(inactive.len(), 1);
}

#[test]
fn null_property_handling() {
    let graph = Graph::new();
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::Null);
        props
    });
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::Int(42));
        props
    });

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // has() should still find vertex with null property (property exists)
    let with_value = g.v().has("value").to_list();
    assert_eq!(with_value.len(), 2);

    // values() returns both (including null)
    let values = g.v().values("value").to_list();
    assert_eq!(values.len(), 2);
}

#[test]
fn type_mismatch_in_predicate() {
    let graph = Graph::new();
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::String("42".to_string()));
        props
    });
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::Int(42));
        props
    });

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Type-specific comparison: string "42" != int 42
    // This tests strict type matching behavior
    let int_results = g.v().has_where("value", p::eq(42i64)).to_list();
    // Should only match the Int(42), not String("42")
    assert_eq!(int_results.len(), 1);
}

#[test]
fn ordering_mixed_numeric_types() {
    let graph = Graph::new();
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("score".to_string(), Value::Int(10));
        props
    });
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("score".to_string(), Value::Float(5.5));
        props
    });
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("score".to_string(), Value::Int(20));
        props
    });
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("score".to_string(), Value::Float(15.0));
        props
    });

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Order by mixed numeric types ascending
    // Note: Types are ordered by type discriminant first (Int < Float),
    // then by value within each type
    let ordered = g.v().values("score").order().by_asc().build().to_list();
    assert_eq!(ordered.len(), 4);

    // Verify: All Ints come before all Floats, sorted within each type
    // Expected order: Int(10), Int(20), Float(5.5), Float(15.0)
    assert_eq!(ordered[0], Value::Int(10));
    assert_eq!(ordered[1], Value::Int(20));
    if let Value::Float(f) = ordered[2] {
        assert!((f - 5.5).abs() < 1e-10);
    } else {
        panic!("Expected Float at index 2, got {:?}", ordered[2]);
    }
    if let Value::Float(f) = ordered[3] {
        assert!((f - 15.0).abs() < 1e-10);
    } else {
        panic!("Expected Float at index 3, got {:?}", ordered[3]);
    }
}

#[test]
fn list_property_handling() {
    let graph = Graph::new();
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert(
            "tags".to_string(),
            Value::List(vec![
                Value::String("rust".to_string()),
                Value::String("graph".to_string()),
            ]),
        );
        props
    });
    graph.add_vertex("test", {
        let mut props = HashMap::new();
        props.insert("tags".to_string(), Value::List(vec![]));
        props
    });

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Both vertices have the "tags" property
    let with_tags = g.v().has("tags").to_list();
    assert_eq!(with_tags.len(), 2);

    // values() returns List values
    let tags = g.v().values("tags").to_list();
    assert_eq!(tags.len(), 2);
}
