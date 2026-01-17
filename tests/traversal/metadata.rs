//! Metadata step tests.
//!
//! Tests for metadata transform steps:
//! - `key()` - extracts "key" from property objects
//! - `value()` - extracts "value" from property objects
//! - `loops()` - returns loop depth in repeat traversals
//! - `index()` - wraps values with their index
//! - `property_map()` - returns properties as map of property objects

use interstellar::traversal::__;
use interstellar::value::Value;

use crate::common::graphs::create_small_graph;

// -------------------------------------------------------------------------
// key() and value() Steps
// -------------------------------------------------------------------------

#[test]
fn key_step_extracts_property_keys() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Get all property keys from Alice vertex
    let keys: Vec<Value> = g.v_ids([tg.alice]).properties().key().to_list();

    // Alice has "name" and "age" properties
    assert_eq!(keys.len(), 2);
    let key_strings: Vec<String> = keys
        .iter()
        .filter_map(|v| {
            if let Value::String(s) = v {
                Some(s.clone())
            } else {
                None
            }
        })
        .collect();
    assert!(key_strings.contains(&"name".to_string()));
    assert!(key_strings.contains(&"age".to_string()));
}

#[test]
fn value_step_extracts_property_values() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Get all property values from Alice vertex
    let values: Vec<Value> = g.v_ids([tg.alice]).properties().value().to_list();

    // Alice has name="Alice" and age=30
    assert_eq!(values.len(), 2);
    assert!(values.contains(&Value::String("Alice".to_string())));
    assert!(values.contains(&Value::Int(30)));
}

#[test]
fn key_and_value_work_with_filtered_properties() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Get only "name" property keys
    let keys: Vec<Value> = g
        .v_ids([tg.alice])
        .properties_keys(["name"])
        .key()
        .to_list();
    assert_eq!(keys, vec![Value::String("name".to_string())]);

    // Get only "name" property values
    let values: Vec<Value> = g
        .v_ids([tg.alice])
        .properties_keys(["name"])
        .value()
        .to_list();
    assert_eq!(values, vec![Value::String("Alice".to_string())]);
}

#[test]
fn key_value_chain_on_all_vertices() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Get all "name" property values from all vertices
    let names: Vec<Value> = g.v().properties_keys(["name"]).value().to_list();

    // 4 vertices, all have "name" property
    assert_eq!(names.len(), 4);
    assert!(names.contains(&Value::String("Alice".to_string())));
    assert!(names.contains(&Value::String("Bob".to_string())));
    assert!(names.contains(&Value::String("Charlie".to_string())));
    assert!(names.contains(&Value::String("GraphDB".to_string())));
}

// -------------------------------------------------------------------------
// loops() Step
// -------------------------------------------------------------------------

#[test]
fn loops_returns_zero_outside_repeat() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Outside of repeat, loops() returns 0
    let loop_counts: Vec<Value> = g.v_ids([tg.alice]).loops().to_list();
    assert_eq!(loop_counts, vec![Value::Int(0)]);
}

#[test]
fn loops_increments_within_repeat() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Traverse out from Alice up to 2 times, emit at each step with loop count
    // Alice -> Bob (loops=0 after first out)
    // Bob -> Charlie (loops=1 after second out)
    let results: Vec<Value> = g
        .v_ids([tg.alice])
        .repeat(__::out_labels(&["knows"]))
        .times(2)
        .emit()
        .loops()
        .to_list();

    // Should have loop counts from repeat iterations
    // After 1st iteration: loops=1, after 2nd: loops=2
    assert!(!results.is_empty());
    for result in &results {
        if let Value::Int(n) = result {
            assert!(*n >= 0 && *n <= 2);
        }
    }
}

#[test]
fn loops_with_until_condition() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Repeat until we find Charlie, then get loop count
    let results: Vec<Value> = g
        .v_ids([tg.alice])
        .repeat(__::out_labels(&["knows"]))
        .until(__::has_value("name", "Charlie"))
        .loops()
        .to_list();

    // Alice -> Bob -> Charlie, so loops should be 2 when we reach Charlie
    assert!(!results.is_empty());
}

// -------------------------------------------------------------------------
// index() Step
// -------------------------------------------------------------------------

#[test]
fn index_wraps_values_with_position() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Get indexed names
    let indexed: Vec<Value> = g.v().has_label("person").values("name").index().to_list();

    // Should have 3 persons, each wrapped as [value, index]
    assert_eq!(indexed.len(), 3);

    // Check structure
    for (expected_idx, result) in indexed.iter().enumerate() {
        if let Value::List(pair) = result {
            assert_eq!(pair.len(), 2);
            // Second element should be the index
            assert_eq!(pair[1], Value::Int(expected_idx as i64));
        } else {
            panic!("Expected [value, index] pair");
        }
    }
}

#[test]
fn index_starts_at_zero() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Get first indexed value
    let first: Option<Value> = g.inject([100i64, 200i64, 300i64]).index().next();

    if let Some(Value::List(pair)) = first {
        assert_eq!(pair[0], Value::Int(100));
        assert_eq!(pair[1], Value::Int(0)); // 0-based index
    } else {
        panic!("Expected [value, index] pair");
    }
}

#[test]
fn index_preserves_traverser_metadata() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Use index in a pipeline with path tracking
    let results: Vec<Value> = g
        .v_ids([tg.alice])
        .as_("start")
        .out_labels(&["knows"])
        .values("name")
        .index()
        .to_list();

    // Should still work, producing indexed values
    assert!(!results.is_empty());
    for result in &results {
        if let Value::List(pair) = result {
            assert_eq!(pair.len(), 2);
        } else {
            panic!("Expected [value, index] pair");
        }
    }
}

// -------------------------------------------------------------------------
// property_map() Step
// -------------------------------------------------------------------------

#[test]
fn property_map_extracts_all_properties() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let maps: Vec<Value> = g.v_ids([tg.alice]).property_map().to_list();

    assert_eq!(maps.len(), 1);
    if let Value::Map(map) = &maps[0] {
        // Should have "name" and "age" keys
        assert!(map.contains_key("name"));
        assert!(map.contains_key("age"));

        // Each value should be a list of property objects
        if let Some(Value::List(name_list)) = map.get("name") {
            assert_eq!(name_list.len(), 1);
            if let Value::Map(prop_obj) = &name_list[0] {
                assert_eq!(
                    prop_obj.get("key"),
                    Some(&Value::String("name".to_string()))
                );
                assert_eq!(
                    prop_obj.get("value"),
                    Some(&Value::String("Alice".to_string()))
                );
            }
        } else {
            panic!("Expected name to be a list");
        }
    } else {
        panic!("Expected Value::Map");
    }
}

#[test]
fn property_map_with_specific_keys() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let maps: Vec<Value> = g.v_ids([tg.alice]).property_map_keys(["name"]).to_list();

    assert_eq!(maps.len(), 1);
    if let Value::Map(map) = &maps[0] {
        // Should only have "name" key
        assert!(map.contains_key("name"));
        assert!(!map.contains_key("age"));
    } else {
        panic!("Expected Value::Map");
    }
}

#[test]
fn property_map_on_edges() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let maps: Vec<Value> = g.e_ids([tg.alice_knows_bob]).property_map().to_list();

    assert_eq!(maps.len(), 1);
    if let Value::Map(map) = &maps[0] {
        // Edge has "since" property
        assert!(map.contains_key("since"));
        if let Some(Value::List(since_list)) = map.get("since") {
            assert_eq!(since_list.len(), 1);
            if let Value::Map(prop_obj) = &since_list[0] {
                assert_eq!(
                    prop_obj.get("key"),
                    Some(&Value::String("since".to_string()))
                );
                assert_eq!(prop_obj.get("value"), Some(&Value::Int(2020)));
            }
        }
    } else {
        panic!("Expected Value::Map");
    }
}

#[test]
fn property_map_returns_empty_for_non_elements() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Inject a non-element value and call property_map
    let maps: Vec<Value> = g.inject([42i64]).property_map().to_list();

    assert_eq!(maps.len(), 1);
    if let Value::Map(map) = &maps[0] {
        assert!(map.is_empty());
    } else {
        panic!("Expected Value::Map");
    }
}

// -------------------------------------------------------------------------
// Combined Metadata Steps
// -------------------------------------------------------------------------

#[test]
fn key_value_difference_from_values() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // values("name") directly extracts the value
    let direct_values: Vec<Value> = g.v_ids([tg.alice]).values("name").to_list();
    assert_eq!(direct_values, vec![Value::String("Alice".to_string())]);

    // properties().value() goes through property objects
    let prop_values: Vec<Value> = g
        .v_ids([tg.alice])
        .properties_keys(["name"])
        .value()
        .to_list();
    assert_eq!(prop_values, vec![Value::String("Alice".to_string())]);

    // Both should produce the same result
    assert_eq!(direct_values, prop_values);
}

#[test]
fn value_map_vs_property_map() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // value_map returns {key: [value], ...}
    let value_maps: Vec<Value> = g.v_ids([tg.alice]).value_map().to_list();

    // property_map returns {key: [{key: k, value: v}], ...}
    let property_maps: Vec<Value> = g.v_ids([tg.alice]).property_map().to_list();

    assert_eq!(value_maps.len(), 1);
    assert_eq!(property_maps.len(), 1);

    // Check value_map structure
    if let Value::Map(vmap) = &value_maps[0] {
        if let Some(Value::List(name_list)) = vmap.get("name") {
            // value_map wraps values directly in lists
            assert_eq!(name_list[0], Value::String("Alice".to_string()));
        }
    }

    // Check property_map structure
    if let Value::Map(pmap) = &property_maps[0] {
        if let Some(Value::List(name_list)) = pmap.get("name") {
            // property_map wraps property objects in lists
            if let Value::Map(prop_obj) = &name_list[0] {
                assert_eq!(
                    prop_obj.get("value"),
                    Some(&Value::String("Alice".to_string()))
                );
            }
        }
    }
}

#[test]
fn index_with_dedup() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Index after dedup
    let indexed: Vec<Value> = g
        .v()
        .has_label("person")
        .out_labels(&["knows"])
        .dedup()
        .values("name")
        .index()
        .to_list();

    // After dedup, indices should be continuous from 0
    for (i, result) in indexed.iter().enumerate() {
        if let Value::List(pair) = result {
            assert_eq!(pair[1], Value::Int(i as i64));
        }
    }
}

#[test]
fn anonymous_traversal_key_value() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Use __::key() and __::value() in anonymous traversals
    let keys: Vec<Value> = g.v_ids([tg.alice]).properties().local(__::key()).to_list();

    assert_eq!(keys.len(), 2);

    let values: Vec<Value> = g
        .v_ids([tg.alice])
        .properties()
        .local(__::value())
        .to_list();

    assert_eq!(values.len(), 2);
}

#[test]
fn anonymous_traversal_index() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Use __::index() in anonymous traversal
    let indexed: Vec<Value> = g.inject([1i64, 2i64, 3i64]).local(__::index()).to_list();

    // Each element should be indexed
    assert_eq!(indexed.len(), 3);
}

#[test]
fn anonymous_traversal_property_map() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Use __::property_map() in anonymous traversal
    let maps: Vec<Value> = g.v_ids([tg.alice]).local(__::property_map()).to_list();

    assert_eq!(maps.len(), 1);
    if let Value::Map(map) = &maps[0] {
        assert!(map.contains_key("name"));
        assert!(map.contains_key("age"));
    }
}

#[test]
fn complex_metadata_pipeline() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Complex pipeline using multiple metadata steps:
    // 1. Get all person vertices
    // 2. Get their properties
    // 3. Extract keys
    // 4. Dedup to unique keys
    // 5. Index the unique keys
    let indexed_keys: Vec<Value> = g
        .v()
        .has_label("person")
        .properties()
        .key()
        .dedup()
        .index()
        .to_list();

    // Persons have "name" and "age" properties
    assert_eq!(indexed_keys.len(), 2);

    // Each should be [key, index] pair
    for (i, result) in indexed_keys.iter().enumerate() {
        if let Value::List(pair) = result {
            assert_eq!(pair.len(), 2);
            assert_eq!(pair[1], Value::Int(i as i64));
        }
    }
}
