//! Additional coverage tests for traversal/transform/order.rs
//!
//! This module covers edge cases and branches not covered by inline tests,
//! focusing on:
//! - Mixed type comparisons in sorting
//! - Bool sorting
//! - Edge property sorting
//! - None/missing value handling
//! - Sub-traversal sorting

use interstellar::storage::Graph;
use interstellar::traversal::SnapshotLike;
use interstellar::value::{Value, VertexId};
use std::collections::HashMap;

// =============================================================================
// Helper Functions
// =============================================================================

fn create_mixed_type_graph() -> Graph {
    let graph = Graph::new();

    // Vertex 0: int value
    let mut props = HashMap::new();
    props.insert("value".to_string(), Value::Int(10));
    graph.add_vertex("item", props);

    // Vertex 1: float value
    let mut props = HashMap::new();
    props.insert("value".to_string(), Value::Float(5.5));
    graph.add_vertex("item", props);

    // Vertex 2: string value
    let mut props = HashMap::new();
    props.insert("value".to_string(), Value::String("abc".to_string()));
    graph.add_vertex("item", props);

    // Vertex 3: bool value
    let mut props = HashMap::new();
    props.insert("value".to_string(), Value::Bool(true));
    graph.add_vertex("item", props);

    graph
}

fn create_bool_graph() -> Graph {
    let graph = Graph::new();

    let mut props = HashMap::new();
    props.insert("active".to_string(), Value::Bool(false));
    graph.add_vertex("flag", props);

    let mut props = HashMap::new();
    props.insert("active".to_string(), Value::Bool(true));
    graph.add_vertex("flag", props);

    let mut props = HashMap::new();
    props.insert("active".to_string(), Value::Bool(false));
    graph.add_vertex("flag", props);

    graph
}

fn create_edge_graph() -> Graph {
    let graph = Graph::new();

    graph.add_vertex("person", HashMap::new());
    graph.add_vertex("person", HashMap::new());
    graph.add_vertex("person", HashMap::new());

    // Edge with weight 0.5
    let mut props = HashMap::new();
    props.insert("weight".to_string(), Value::Float(0.5));
    graph
        .add_edge(VertexId(0), VertexId(1), "knows", props)
        .unwrap();

    // Edge with weight 0.8
    let mut props = HashMap::new();
    props.insert("weight".to_string(), Value::Float(0.8));
    graph
        .add_edge(VertexId(1), VertexId(2), "knows", props)
        .unwrap();

    // Edge with weight 0.2
    let mut props = HashMap::new();
    props.insert("weight".to_string(), Value::Float(0.2));
    graph
        .add_edge(VertexId(0), VertexId(2), "knows", props)
        .unwrap();

    graph
}

fn create_missing_prop_graph() -> Graph {
    let graph = Graph::new();

    // Vertex with age
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Alice".to_string()));
    props.insert("age".to_string(), Value::Int(30));
    graph.add_vertex("person", props);

    // Vertex without age
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Bob".to_string()));
    graph.add_vertex("person", props);

    // Vertex with age
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Charlie".to_string()));
    props.insert("age".to_string(), Value::Int(25));
    graph.add_vertex("person", props);

    graph
}

// =============================================================================
// Mixed Type Sorting Tests
// =============================================================================

mod mixed_type_sorting {
    use super::*;

    #[test]
    fn order_by_mixed_type_property_asc() {
        let graph = create_mixed_type_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Sort by value property which has different types
        let results = g
            .v()
            .has_label("item")
            .order()
            .by_key_asc("value")
            .build()
            .to_list();

        // Should sort by type discriminant when types differ
        // Int < Float < String < Bool (based on compare_values impl)
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn order_natural_with_mixed_value_types() {
        let graph = Graph::new();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Inject mixed types and sort naturally
        let results = g
            .inject([
                Value::Bool(true),
                Value::String("hello".to_string()),
                Value::Int(42),
                Value::Float(3.14),
            ])
            .order()
            .by_asc()
            .build()
            .to_list();

        assert_eq!(results.len(), 4);
        // First should be Int (lowest discriminant for non-null)
        assert!(matches!(results[0], Value::Int(_)));
    }
}

// =============================================================================
// Bool Sorting Tests
// =============================================================================

mod bool_sorting {
    use super::*;

    #[test]
    fn order_by_bool_property_ascending() {
        let graph = create_bool_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let results = g
            .v()
            .has_label("flag")
            .order()
            .by_key_asc("active")
            .build()
            .to_list();

        assert_eq!(results.len(), 3);
        // false < true, so false values should come first
        // We can verify by checking the first vertex
        if let Value::Vertex(id) = &results[0] {
            let vertex = snapshot.storage().get_vertex(*id).unwrap();
            assert_eq!(vertex.properties.get("active"), Some(&Value::Bool(false)));
        }
    }

    #[test]
    fn order_by_bool_property_descending() {
        let graph = create_bool_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let results = g
            .v()
            .has_label("flag")
            .order()
            .by_key_desc("active")
            .build()
            .to_list();

        assert_eq!(results.len(), 3);
        // true > false, so true values should come first
        if let Value::Vertex(id) = &results[0] {
            let vertex = snapshot.storage().get_vertex(*id).unwrap();
            assert_eq!(vertex.properties.get("active"), Some(&Value::Bool(true)));
        }
    }

    #[test]
    fn order_natural_bools_ascending() {
        let graph = Graph::new();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let results = g
            .inject([Value::Bool(true), Value::Bool(false), Value::Bool(true)])
            .order()
            .by_asc()
            .build()
            .to_list();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Value::Bool(false));
        assert_eq!(results[1], Value::Bool(true));
        assert_eq!(results[2], Value::Bool(true));
    }
}

// =============================================================================
// Edge Sorting Tests
// =============================================================================

mod edge_sorting {
    use super::*;

    #[test]
    fn order_edges_by_property_ascending() {
        let graph = create_edge_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let results = g.e().order().by_key_asc("weight").build().to_list();

        assert_eq!(results.len(), 3);

        // Extract weights in order
        let weights: Vec<f64> = results
            .iter()
            .filter_map(|v| {
                if let Value::Edge(id) = v {
                    snapshot
                        .storage()
                        .get_edge(*id)
                        .and_then(|e| e.properties.get("weight").cloned())
                        .and_then(|w| {
                            if let Value::Float(f) = w {
                                Some(f)
                            } else {
                                None
                            }
                        })
                } else {
                    None
                }
            })
            .collect();

        // Should be sorted: 0.2, 0.5, 0.8
        assert!((weights[0] - 0.2).abs() < 0.001);
        assert!((weights[1] - 0.5).abs() < 0.001);
        assert!((weights[2] - 0.8).abs() < 0.001);
    }

    #[test]
    fn order_edges_by_property_descending() {
        let graph = create_edge_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let results = g.e().order().by_key_desc("weight").build().to_list();

        assert_eq!(results.len(), 3);

        // Extract weights in order
        let weights: Vec<f64> = results
            .iter()
            .filter_map(|v| {
                if let Value::Edge(id) = v {
                    snapshot
                        .storage()
                        .get_edge(*id)
                        .and_then(|e| e.properties.get("weight").cloned())
                        .and_then(|w| {
                            if let Value::Float(f) = w {
                                Some(f)
                            } else {
                                None
                            }
                        })
                } else {
                    None
                }
            })
            .collect();

        // Should be sorted descending: 0.8, 0.5, 0.2
        assert!((weights[0] - 0.8).abs() < 0.001);
        assert!((weights[1] - 0.5).abs() < 0.001);
        assert!((weights[2] - 0.2).abs() < 0.001);
    }
}

// =============================================================================
// Missing Property Sorting Tests
// =============================================================================

mod missing_property_sorting {
    use super::*;

    #[test]
    fn order_with_some_missing_properties() {
        let graph = create_missing_prop_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Sort by age - one vertex doesn't have age property
        let results = g
            .v()
            .has_label("person")
            .order()
            .by_key_asc("age")
            .build()
            .to_list();

        assert_eq!(results.len(), 3);
        // Vertices with values should come before None values
        // So Charlie (25) and Alice (30) should come before Bob (no age)
    }

    #[test]
    fn order_by_nonexistent_property() {
        let graph = create_missing_prop_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Sort by property that doesn't exist on any vertex
        let results = g
            .v()
            .has_label("person")
            .order()
            .by_key_asc("nonexistent")
            .build()
            .to_list();

        // Should still return all vertices (order may be arbitrary)
        assert_eq!(results.len(), 3);
    }
}

// =============================================================================
// Sub-traversal Sorting Tests
// =============================================================================

mod subtraversal_sorting {
    use super::*;
    use interstellar::traversal::__;

    #[test]
    fn order_by_subtraversal_property() {
        let graph = Graph::new();

        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("score".to_string(), Value::Int(100));
        graph.add_vertex("person", props);

        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        props.insert("score".to_string(), Value::Int(50));
        graph.add_vertex("person", props);

        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Charlie".to_string()));
        props.insert("score".to_string(), Value::Int(75));
        graph.add_vertex("person", props);

        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Sort by score using a sub-traversal
        let sub = __::values("score");
        let results = g
            .v()
            .has_label("person")
            .order()
            .by_traversal(sub, true) // descending
            .build()
            .to_list();

        assert_eq!(results.len(), 3);

        // Extract names in order - should be Alice, Charlie, Bob (by score desc)
        let names: Vec<String> = results
            .iter()
            .filter_map(|v| {
                if let Value::Vertex(id) = v {
                    snapshot
                        .storage()
                        .get_vertex(*id)
                        .and_then(|v| v.properties.get("name").cloned())
                        .and_then(|n| {
                            if let Value::String(s) = n {
                                Some(s)
                            } else {
                                None
                            }
                        })
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(names[0], "Alice");
        assert_eq!(names[1], "Charlie");
        assert_eq!(names[2], "Bob");
    }
}

// =============================================================================
// OrderBuilder Edge Cases
// =============================================================================

mod order_builder_edge_cases {
    use super::*;

    #[test]
    fn order_with_no_by_clause_defaults_to_asc() {
        let graph = Graph::new();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // order() with no by_* calls should default to natural ascending
        let results = g
            .inject([Value::Int(3), Value::Int(1), Value::Int(2)])
            .order()
            .build()
            .to_list();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Value::Int(1));
        assert_eq!(results[1], Value::Int(2));
        assert_eq!(results[2], Value::Int(3));
    }

    #[test]
    fn order_with_multiple_by_clauses() {
        let graph = Graph::new();

        // Same last name, different first names
        let mut props = HashMap::new();
        props.insert("first".to_string(), Value::String("Alice".to_string()));
        props.insert("last".to_string(), Value::String("Smith".to_string()));
        graph.add_vertex("person", props);

        let mut props = HashMap::new();
        props.insert("first".to_string(), Value::String("Bob".to_string()));
        props.insert("last".to_string(), Value::String("Jones".to_string()));
        graph.add_vertex("person", props);

        let mut props = HashMap::new();
        props.insert("first".to_string(), Value::String("Charlie".to_string()));
        props.insert("last".to_string(), Value::String("Smith".to_string()));
        graph.add_vertex("person", props);

        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Sort by last name asc, then first name asc
        let results = g
            .v()
            .has_label("person")
            .order()
            .by_key_asc("last")
            .by_key_asc("first")
            .build()
            .to_list();

        assert_eq!(results.len(), 3);

        // Jones should come before Smith (alphabetically)
        // Among Smiths, Alice should come before Charlie
        let names: Vec<(String, String)> = results
            .iter()
            .filter_map(|v| {
                if let Value::Vertex(id) = v {
                    let vertex = snapshot.storage().get_vertex(*id)?;
                    let first = match vertex.properties.get("first")? {
                        Value::String(s) => s.clone(),
                        _ => return None,
                    };
                    let last = match vertex.properties.get("last")? {
                        Value::String(s) => s.clone(),
                        _ => return None,
                    };
                    Some((last, first))
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(names[0], ("Jones".to_string(), "Bob".to_string()));
        assert_eq!(names[1], ("Smith".to_string(), "Alice".to_string()));
        assert_eq!(names[2], ("Smith".to_string(), "Charlie".to_string()));
    }
}

// =============================================================================
// Non-element Value Sorting
// =============================================================================

mod non_element_sorting {
    use super::*;

    #[test]
    fn order_null_values() {
        let graph = Graph::new();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let results = g
            .inject([Value::Null, Value::Int(1), Value::Null, Value::Int(2)])
            .order()
            .by_asc()
            .build()
            .to_list();

        assert_eq!(results.len(), 4);
        // Ints come before other types including Null
        assert!(matches!(results[0], Value::Int(_)));
        assert!(matches!(results[1], Value::Int(_)));
    }

    #[test]
    fn order_list_values() {
        let graph = Graph::new();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let results = g
            .inject([
                Value::List(vec![Value::Int(1), Value::Int(2)]),
                Value::Int(5),
                Value::List(vec![Value::Int(3)]),
            ])
            .order()
            .by_asc()
            .build()
            .to_list();

        assert_eq!(results.len(), 3);
        // Int comes before List
        assert!(matches!(results[0], Value::Int(_)));
    }

    #[test]
    fn order_map_values() {
        let graph = Graph::new();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let mut map1 = HashMap::new();
        map1.insert("key".to_string(), Value::Int(1));

        let results = g
            .inject([Value::Map(map1), Value::Int(5)])
            .order()
            .by_asc()
            .build()
            .to_list();

        assert_eq!(results.len(), 2);
        // Int comes before Map
        assert!(matches!(results[0], Value::Int(_)));
    }
}
