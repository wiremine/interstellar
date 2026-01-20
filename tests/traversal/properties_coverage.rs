//! Additional coverage tests for traversal/transform/properties.rs
//!
//! This module covers edge cases and branches not covered by inline tests,
//! focusing on:
//! - ValueMapStep with include_tokens
//! - ElementMapStep variations
//! - PropertyMapStep edge cases
//! - Empty properties handling
//! - Non-element inputs

use interstellar::storage::Graph;
use interstellar::value::{EdgeId, Value, VertexId};
use std::collections::HashMap;

// =============================================================================
// Helper Functions
// =============================================================================

fn create_test_graph() -> Graph {
    let graph = Graph::new();

    // Vertex 0: person with multiple properties
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Alice".to_string()));
    props.insert("age".to_string(), Value::Int(30));
    props.insert("city".to_string(), Value::String("NYC".to_string()));
    graph.add_vertex("person", props);

    // Vertex 1: person with fewer properties
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Bob".to_string()));
    graph.add_vertex("person", props);

    // Vertex 2: empty properties
    graph.add_vertex("empty", HashMap::new());

    // Edge 0: with properties
    let mut props = HashMap::new();
    props.insert("weight".to_string(), Value::Float(0.5));
    props.insert("since".to_string(), Value::Int(2020));
    graph
        .add_edge(VertexId(0), VertexId(1), "knows", props)
        .unwrap();

    // Edge 1: no properties
    graph
        .add_edge(VertexId(1), VertexId(2), "knows", HashMap::new())
        .unwrap();

    graph
}

// =============================================================================
// PropertiesStep Tests
// =============================================================================

mod properties_step_tests {
    use super::*;

    #[test]
    fn properties_extracts_all_vertex_properties() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Get all properties from vertex with multiple props
        let results: Vec<Value> = g.v_ids([VertexId(0)]).properties().to_list();

        // Should have 3 properties: name, age, city
        assert_eq!(results.len(), 3);

        // Each should be a Map with "key" and "value"
        for result in &results {
            if let Value::Map(map) = result {
                assert!(map.contains_key("key"));
                assert!(map.contains_key("value"));
            } else {
                panic!("Expected Map for property");
            }
        }
    }

    #[test]
    fn properties_extracts_specific_keys() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Get only name property
        let results: Vec<Value> = g.v_ids([VertexId(0)]).properties_keys(["name"]).to_list();

        assert_eq!(results.len(), 1);
        if let Value::Map(map) = &results[0] {
            assert_eq!(map.get("key"), Some(&Value::String("name".to_string())));
            assert_eq!(map.get("value"), Some(&Value::String("Alice".to_string())));
        }
    }

    #[test]
    fn properties_handles_empty_vertex() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Get properties from vertex with no properties
        let results: Vec<Value> = g.v_ids([VertexId(2)]).properties().to_list();

        assert!(results.is_empty());
    }

    #[test]
    fn properties_handles_edge() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Get properties from edge
        let results: Vec<Value> = g.e_ids([EdgeId(0)]).properties().to_list();

        assert_eq!(results.len(), 2); // weight and since
    }

    #[test]
    fn properties_handles_edge_without_properties() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Get properties from edge with no properties
        let results: Vec<Value> = g.e_ids([EdgeId(1)]).properties().to_list();

        assert!(results.is_empty());
    }

    #[test]
    fn properties_ignores_non_elements() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Inject non-element values and call properties
        let results: Vec<Value> = g
            .inject([Value::Int(42), Value::String("hello".to_string())])
            .properties()
            .to_list();

        assert!(results.is_empty());
    }

    #[test]
    fn properties_with_nonexistent_key() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Get property that doesn't exist
        let results: Vec<Value> = g
            .v_ids([VertexId(0)])
            .properties_keys(["nonexistent"])
            .to_list();

        assert!(results.is_empty());
    }
}

// =============================================================================
// ValueMapStep Tests
// =============================================================================

mod value_map_step_tests {
    use super::*;

    #[test]
    fn value_map_extracts_all_properties() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let results: Vec<Value> = g.v_ids([VertexId(0)]).value_map().to_list();

        assert_eq!(results.len(), 1);
        if let Value::Map(map) = &results[0] {
            assert!(map.contains_key("name"));
            assert!(map.contains_key("age"));
            assert!(map.contains_key("city"));
        }
    }

    #[test]
    fn value_map_with_specific_keys() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let results: Vec<Value> = g
            .v_ids([VertexId(0)])
            .value_map_keys(["name", "age"])
            .to_list();

        assert_eq!(results.len(), 1);
        if let Value::Map(map) = &results[0] {
            assert!(map.contains_key("name"));
            assert!(map.contains_key("age"));
            // city should NOT be present
            assert!(!map.contains_key("city"));
        }
    }

    #[test]
    fn value_map_empty_properties() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let results: Vec<Value> = g
            .v_ids([VertexId(2)]) // empty vertex
            .value_map()
            .to_list();

        assert_eq!(results.len(), 1);
        if let Value::Map(map) = &results[0] {
            assert!(map.is_empty());
        }
    }

    #[test]
    fn value_map_edge() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let results: Vec<Value> = g.e_ids([EdgeId(0)]).value_map().to_list();

        assert_eq!(results.len(), 1);
        if let Value::Map(map) = &results[0] {
            assert!(map.contains_key("weight"));
            assert!(map.contains_key("since"));
        }
    }

    #[test]
    fn value_map_ignores_non_elements() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let results: Vec<Value> = g.inject([Value::Int(42)]).value_map().to_list();

        // Non-elements produce empty maps (1 result with empty map)
        assert_eq!(results.len(), 1);
        if let Value::Map(map) = &results[0] {
            assert!(map.is_empty());
        } else {
            panic!("Expected Value::Map");
        }
    }
}

// =============================================================================
// ElementMapStep Tests
// =============================================================================

mod element_map_step_tests {
    use super::*;

    #[test]
    fn element_map_includes_id_and_label() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let results: Vec<Value> = g.v_ids([VertexId(0)]).element_map().to_list();

        assert_eq!(results.len(), 1);
        if let Value::Map(map) = &results[0] {
            // Should include id and label (without tilde prefix in this impl)
            assert!(map.contains_key("id"));
            assert!(map.contains_key("label"));
            // Plus properties
            assert!(map.contains_key("name"));
            assert!(map.contains_key("age"));
        }
    }

    #[test]
    fn element_map_with_specific_keys() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let results: Vec<Value> = g.v_ids([VertexId(0)]).element_map_keys(["name"]).to_list();

        assert_eq!(results.len(), 1);
        if let Value::Map(map) = &results[0] {
            // Should always include id and label
            assert!(map.contains_key("id"));
            assert!(map.contains_key("label"));
            // Only requested property
            assert!(map.contains_key("name"));
            assert!(!map.contains_key("age"));
        }
    }

    #[test]
    fn element_map_edge_includes_endpoints() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let results: Vec<Value> = g.e_ids([EdgeId(0)]).element_map().to_list();

        assert_eq!(results.len(), 1);
        if let Value::Map(map) = &results[0] {
            assert!(map.contains_key("id"));
            assert!(map.contains_key("label"));
            // Edge-specific: IN and OUT vertex references
            assert!(map.contains_key("IN"));
            assert!(map.contains_key("OUT"));
            // Properties
            assert!(map.contains_key("weight"));
        }
    }

    #[test]
    fn element_map_empty_vertex() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let results: Vec<Value> = g.v_ids([VertexId(2)]).element_map().to_list();

        assert_eq!(results.len(), 1);
        if let Value::Map(map) = &results[0] {
            // Should still have id and label
            assert!(map.contains_key("id"));
            assert!(map.contains_key("label"));
            // No properties beyond those
            assert_eq!(map.len(), 2);
        }
    }

    #[test]
    fn element_map_ignores_non_elements() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let results: Vec<Value> = g
            .inject([Value::String("test".to_string())])
            .element_map()
            .to_list();

        // Non-elements produce empty maps (1 result with empty map)
        assert_eq!(results.len(), 1);
        if let Value::Map(map) = &results[0] {
            assert!(map.is_empty());
        } else {
            panic!("Expected Value::Map");
        }
    }
}

// =============================================================================
// PropertyMapStep Tests
// =============================================================================

mod property_map_step_tests {
    use super::*;

    #[test]
    fn property_map_wraps_values_in_lists() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let results: Vec<Value> = g.v_ids([VertexId(0)]).property_map().to_list();

        assert_eq!(results.len(), 1);
        if let Value::Map(map) = &results[0] {
            // Each value should be a list (for multi-value property support)
            for (_key, value) in map.iter() {
                assert!(matches!(value, Value::List(_)));
            }
        }
    }

    #[test]
    fn property_map_with_specific_keys() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let results: Vec<Value> = g.v_ids([VertexId(0)]).property_map_keys(["name"]).to_list();

        assert_eq!(results.len(), 1);
        if let Value::Map(map) = &results[0] {
            assert!(map.contains_key("name"));
            assert!(!map.contains_key("age"));
        }
    }

    #[test]
    fn property_map_empty_vertex() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let results: Vec<Value> = g.v_ids([VertexId(2)]).property_map().to_list();

        assert_eq!(results.len(), 1);
        if let Value::Map(map) = &results[0] {
            assert!(map.is_empty());
        }
    }

    #[test]
    fn property_map_edge() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let results: Vec<Value> = g.e_ids([EdgeId(0)]).property_map().to_list();

        assert_eq!(results.len(), 1);
        if let Value::Map(map) = &results[0] {
            assert!(map.contains_key("weight"));
            // Values should be lists
            if let Some(Value::List(weights)) = map.get("weight") {
                assert!(!weights.is_empty());
            }
        }
    }

    #[test]
    fn property_map_ignores_non_elements() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let results: Vec<Value> = g.inject([Value::Int(42)]).property_map().to_list();

        // Non-elements produce empty maps (1 result with empty map)
        assert_eq!(results.len(), 1);
        if let Value::Map(map) = &results[0] {
            assert!(map.is_empty());
        } else {
            panic!("Expected Value::Map");
        }
    }
}

// =============================================================================
// Path Preservation Tests
// =============================================================================

mod path_preservation_tests {
    use super::*;

    #[test]
    fn properties_preserves_path() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Path tracking with properties step
        let results: Vec<Value> = g
            .v_ids([VertexId(0)])
            .with_path()
            .as_("start")
            .properties()
            .to_list();

        // Should produce results for each property
        assert!(!results.is_empty());
    }

    #[test]
    fn value_map_preserves_path() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let results: Vec<Value> = g
            .v_ids([VertexId(0)])
            .with_path()
            .as_("start")
            .value_map()
            .to_list();

        assert_eq!(results.len(), 1);
    }
}

// =============================================================================
// Bulk Preservation Tests
// =============================================================================

mod bulk_preservation_tests {
    use super::*;
    use interstellar::traversal::step::AnyStep;
    use interstellar::traversal::transform::PropertiesStep;
    use interstellar::traversal::{ExecutionContext, SnapshotLike, Traverser};

    #[test]
    fn properties_step_expands_bulk() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

        let step = PropertiesStep::new();

        let mut traverser = Traverser::from_vertex(VertexId(0));
        traverser.bulk = 5;

        let input = vec![traverser];
        let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

        // Should produce multiple outputs (one per property)
        // Each should inherit the bulk
        assert_eq!(output.len(), 3); // name, age, city
        for t in output {
            assert_eq!(t.bulk, 5);
        }
    }
}

// =============================================================================
// Step Name Tests
// =============================================================================

mod step_name_tests {
    use interstellar::traversal::step::AnyStep;
    use interstellar::traversal::transform::{
        ElementMapStep, PropertiesStep, PropertyMapStep, ValueMapStep,
    };

    #[test]
    fn properties_step_name() {
        let step = PropertiesStep::new();
        assert_eq!(step.name(), "properties");
    }

    #[test]
    fn value_map_step_name() {
        let step = ValueMapStep::new();
        assert_eq!(step.name(), "valueMap");
    }

    #[test]
    fn element_map_step_name() {
        let step = ElementMapStep::new();
        assert_eq!(step.name(), "elementMap");
    }

    #[test]
    fn property_map_step_name() {
        let step = PropertyMapStep::new();
        assert_eq!(step.name(), "propertyMap");
    }
}

// =============================================================================
// from_keys Constructor Tests
// =============================================================================

mod from_keys_tests {
    use interstellar::traversal::step::AnyStep;
    use interstellar::traversal::transform::PropertiesStep;

    #[test]
    fn properties_from_keys_iterator() {
        let keys = vec!["name", "age"];
        let step = PropertiesStep::from_keys(keys);
        // Just verify it constructs without error
        assert_eq!(step.name(), "properties");
    }

    #[test]
    fn properties_from_keys_array() {
        let step = PropertiesStep::from_keys(["name", "age"]);
        assert_eq!(step.name(), "properties");
    }
}
