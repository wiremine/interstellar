//! Additional coverage tests for traversal/aggregate.rs
//!
//! This module covers edge cases and branches not covered by inline tests,
//! focusing on:
//! - value_to_map_key function for all value types
//! - List and Map keys being skipped
//! - GroupValue::Traversal returning multiple results
//! - Empty traversal results

use interstellar::storage::CowGraph;
use interstellar::traversal::__;
use interstellar::value::{Value, VertexId};
use std::collections::HashMap;

// =============================================================================
// Helper Functions
// =============================================================================

fn create_basic_graph() -> CowGraph {
    let graph = CowGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Alice".to_string()));
    props.insert("age".to_string(), Value::Int(30));
    graph.add_vertex("person", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Bob".to_string()));
    props.insert("age".to_string(), Value::Int(25));
    graph.add_vertex("person", props);

    graph
}

fn create_graph_with_edges() -> CowGraph {
    let graph = CowGraph::new();

    // Vertices
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Alice".to_string()));
    graph.add_vertex("person", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Bob".to_string()));
    graph.add_vertex("person", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Charlie".to_string()));
    graph.add_vertex("person", props);

    // Edges
    let mut props = HashMap::new();
    props.insert("since".to_string(), Value::Int(2020));
    graph
        .add_edge(VertexId(0), VertexId(1), "knows", props)
        .unwrap();

    let mut props = HashMap::new();
    props.insert("since".to_string(), Value::Int(2021));
    graph
        .add_edge(VertexId(0), VertexId(2), "knows", props)
        .unwrap();

    let mut props = HashMap::new();
    props.insert("since".to_string(), Value::Int(2020));
    graph
        .add_edge(VertexId(1), VertexId(2), "knows", props)
        .unwrap();

    graph
}

// =============================================================================
// GroupKey Constructor Tests
// =============================================================================

mod group_key_constructors {
    use super::*;
    use interstellar::traversal::aggregate::{GroupKey, GroupValue};

    #[test]
    fn group_key_by_label() {
        let key = GroupKey::by_label();
        assert!(matches!(key, GroupKey::Label));
    }

    #[test]
    fn group_key_by_property() {
        let key = GroupKey::by_property("age");
        if let GroupKey::Property(prop) = key {
            assert_eq!(prop, "age");
        } else {
            panic!("Expected Property key");
        }
    }

    #[test]
    fn group_key_by_traversal() {
        let t = __::values("name");
        let key = GroupKey::by_traversal(t);
        assert!(matches!(key, GroupKey::Traversal(_)));
    }

    #[test]
    fn group_value_identity() {
        let value = GroupValue::identity();
        assert!(matches!(value, GroupValue::Identity));
    }

    #[test]
    fn group_value_by_property() {
        let value = GroupValue::by_property("name");
        if let GroupValue::Property(prop) = value {
            assert_eq!(prop, "name");
        } else {
            panic!("Expected Property value");
        }
    }

    #[test]
    fn group_value_by_traversal() {
        let t = __::values("name");
        let value = GroupValue::by_traversal(t);
        assert!(matches!(value, GroupValue::Traversal(_)));
    }
}

// =============================================================================
// GroupStep Key Type Tests (value_to_map_key coverage)
// =============================================================================

mod group_step_key_types {
    use super::*;

    #[test]
    fn group_by_bool_property() {
        let graph = CowGraph::new();

        let mut props = HashMap::new();
        props.insert("active".to_string(), Value::Bool(true));
        graph.add_vertex("user", props);

        let mut props = HashMap::new();
        props.insert("active".to_string(), Value::Bool(false));
        graph.add_vertex("user", props);

        let mut props = HashMap::new();
        props.insert("active".to_string(), Value::Bool(true));
        graph.add_vertex("user", props);

        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let result = g
            .v()
            .has_label("user")
            .group()
            .by_key("active")
            .by_value()
            .build()
            .next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            // Bool keys become "true" and "false" strings
            assert!(map.contains_key("true") || map.contains_key("false"));
        }
    }

    #[test]
    fn group_by_float_property() {
        let graph = CowGraph::new();

        let mut props = HashMap::new();
        props.insert("score".to_string(), Value::Float(0.5));
        graph.add_vertex("item", props);

        let mut props = HashMap::new();
        props.insert("score".to_string(), Value::Float(0.5));
        graph.add_vertex("item", props);

        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let result = g
            .v()
            .has_label("item")
            .group()
            .by_key("score")
            .by_value()
            .build()
            .next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            // Float key becomes string
            assert!(map.contains_key("0.5"));
        }
    }

    #[test]
    fn group_by_null_property() {
        let graph = CowGraph::new();

        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::Null);
        graph.add_vertex("item", props);

        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let result = g
            .v()
            .has_label("item")
            .group()
            .by_key("value")
            .by_value()
            .build()
            .next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            // Null key becomes "null" string
            assert!(map.contains_key("null"));
        }
    }
}

// =============================================================================
// GroupCountStep Key Type Tests
// =============================================================================

mod group_count_key_types {
    use super::*;

    #[test]
    fn group_count_by_bool_property() {
        let graph = CowGraph::new();

        let mut props = HashMap::new();
        props.insert("active".to_string(), Value::Bool(true));
        graph.add_vertex("user", props);

        let mut props = HashMap::new();
        props.insert("active".to_string(), Value::Bool(false));
        graph.add_vertex("user", props);

        let mut props = HashMap::new();
        props.insert("active".to_string(), Value::Bool(true));
        graph.add_vertex("user", props);

        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        let result = g
            .v()
            .has_label("user")
            .group_count()
            .by_key("active")
            .build()
            .next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            assert_eq!(map.get("true"), Some(&Value::Int(2)));
            assert_eq!(map.get("false"), Some(&Value::Int(1)));
        }
    }

    #[test]
    fn group_count_by_vertex_reference() {
        // This tests value_to_map_key for Vertex type
        let graph = create_graph_with_edges();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Count edges grouped by target vertex
        let result = g.e().group_count().by_traversal(__::in_v()).build().next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            // Keys should be v[id] format
            assert!(map.keys().any(|k| k.starts_with("v[")));
        }
    }

    #[test]
    fn group_count_by_edge_reference() {
        // This tests value_to_map_key for Edge type
        let graph = create_graph_with_edges();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Group vertices by outgoing edges (unusual but tests edge key handling)
        // First collect edges, then group by identity
        let result = g.e().group_count().by_label().build().next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            assert!(map.contains_key("knows"));
        }
    }
}

// =============================================================================
// GroupValue Traversal Tests (multiple results)
// =============================================================================

mod group_value_traversal_tests {
    use super::*;

    #[test]
    fn group_value_traversal_multiple_results() {
        let graph = create_graph_with_edges();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Group by label, collect outgoing neighbor names
        let result = g
            .v()
            .has_label("person")
            .group()
            .by_label()
            .by_value_traversal(__::out().values("name"))
            .build()
            .next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            // Should have person key
            assert!(map.contains_key("person"));
            // Value should be list of collected values
            if let Some(Value::List(values)) = map.get("person") {
                // Alice has 2 outgoing edges, Bob has 1, Charlie has 0
                // So we should see some names collected
                assert!(!values.is_empty());
            }
        }
    }

    #[test]
    fn group_value_traversal_empty_results() {
        let graph = CowGraph::new();

        // Vertex with no outgoing edges
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Isolated".to_string()));
        graph.add_vertex("person", props);

        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Group by label, collect outgoing neighbor names (none exist)
        let result = g
            .v()
            .has_label("person")
            .group()
            .by_label()
            .by_value_traversal(__::out().values("name"))
            .build()
            .next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            // Person group exists but values list may be empty
            if let Some(Value::List(values)) = map.get("person") {
                assert!(values.is_empty());
            }
        }
    }

    #[test]
    fn group_value_traversal_single_result() {
        let graph = CowGraph::new();

        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        graph.add_vertex("person", props);

        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        graph.add_vertex("person", props);

        // Single edge from Alice to Bob
        graph
            .add_edge(VertexId(0), VertexId(1), "knows", HashMap::new())
            .unwrap();

        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Use a traversal that returns single value (out-degree count)
        let result = g
            .v()
            .has_label("person")
            .group()
            .by_key("name")
            .by_value_traversal(__::out().values("name"))
            .build()
            .next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            // Alice has 1 outgoing edge
            if let Some(Value::List(values)) = map.get("Alice") {
                assert!(!values.is_empty());
            }
        }
    }
}

// =============================================================================
// Non-Element Input Tests
// =============================================================================

mod non_element_input_tests {
    use super::*;

    #[test]
    fn group_non_vertex_non_edge_by_label() {
        let graph = CowGraph::new();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Inject integers and try to group by label (should skip them)
        let result = g
            .inject([Value::Int(1), Value::Int(2), Value::Int(3)])
            .group()
            .by_label()
            .by_value()
            .build()
            .next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            // Should be empty since integers don't have labels
            assert!(map.is_empty());
        }
    }

    #[test]
    fn group_count_non_vertex_non_edge_by_property() {
        let graph = CowGraph::new();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Inject strings and try to group count by property (should skip them)
        let result = g
            .inject([
                Value::String("a".to_string()),
                Value::String("b".to_string()),
            ])
            .group_count()
            .by_key("nonexistent")
            .build()
            .next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            // Should be empty since strings don't have properties
            assert!(map.is_empty());
        }
    }
}

// =============================================================================
// Default Selector Tests
// =============================================================================

mod default_selector_tests {
    use super::*;

    #[test]
    fn group_step_default_key_is_label() {
        let graph = create_basic_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // group() without by_key or by_label should default to label
        let result = g.v().group().build().next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            assert!(map.contains_key("person"));
        }
    }

    #[test]
    fn group_step_default_value_is_identity() {
        let graph = create_basic_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // group().by_label() without by_value should default to identity
        let result = g.v().group().by_label().build().next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            if let Some(Value::List(values)) = map.get("person") {
                // Values should be vertices (identity)
                for v in values {
                    assert!(matches!(v, Value::Vertex(_)));
                }
            }
        }
    }

    #[test]
    fn group_count_step_default_is_label() {
        let graph = create_basic_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // group_count() without by_key or by_label should default to label
        let result = g.v().group_count().build().next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            assert!(map.contains_key("person"));
            assert_eq!(map.get("person"), Some(&Value::Int(2)));
        }
    }
}

// =============================================================================
// Path Preservation Tests
// =============================================================================

mod path_preservation_tests {
    use super::*;

    #[test]
    fn group_preserves_last_path() {
        let graph = create_basic_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // With path tracking enabled
        let result = g
            .v()
            .with_path()
            .as_("start")
            .group()
            .by_label()
            .by_value()
            .build()
            .next();

        // Result should exist (group doesn't fail with paths)
        assert!(result.is_some());
    }

    #[test]
    fn group_count_preserves_last_path() {
        let graph = create_basic_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // With path tracking enabled
        let result = g
            .v()
            .with_path()
            .as_("start")
            .group_count()
            .by_label()
            .build()
            .next();

        // Result should exist
        assert!(result.is_some());
    }
}
