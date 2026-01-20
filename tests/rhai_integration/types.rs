//! Type conversion integration tests.

use interstellar::prelude::*;
use interstellar::rhai::RhaiEngine;

use super::create_social_graph;

#[test]
fn test_value_int_roundtrip() {
    let engine = RhaiEngine::new();
    let result: i64 = engine.eval("42").unwrap();
    assert_eq!(result, 42);
}

#[test]
fn test_value_float_roundtrip() {
    let engine = RhaiEngine::new();
    let result: f64 = engine.eval("3.14").unwrap();
    assert!((result - 3.14).abs() < 0.001);
}

#[test]
fn test_value_string_roundtrip() {
    let engine = RhaiEngine::new();
    let result: String = engine.eval(r#""hello world""#).unwrap();
    assert_eq!(result, "hello world");
}

#[test]
fn test_value_bool_roundtrip() {
    let engine = RhaiEngine::new();
    let result: bool = engine.eval("true").unwrap();
    assert!(result);
}

#[test]
fn test_vertex_id_from_traversal() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Get vertex IDs from traversal
    let script = r#"
        let g = graph.traversal();
        g.v().id().to_list()
    "#;

    let result: rhai::Array = engine.eval_with_graph(graph.clone(), script).unwrap();
    assert_eq!(result.len(), 6); // 5 people + 1 company
}

#[test]
fn test_value_list_from_traversal() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Get names as list
    let script = r#"
        let g = graph.traversal();
        g.v().has_label("person").values("name").to_list()
    "#;

    let result: rhai::Array = engine.eval_with_graph(graph.clone(), script).unwrap();
    assert_eq!(result.len(), 5);

    // Check that names are strings
    for item in result {
        assert!(item.is_string());
    }
}

#[test]
fn test_value_map_from_traversal() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Get value maps
    let script = r#"
        let g = graph.traversal();
        g.v().has_value("name", "Alice").value_map().to_list()
    "#;

    let result: rhai::Array = engine.eval_with_graph(graph.clone(), script).unwrap();
    assert_eq!(result.len(), 1);

    // The result should be a map
    let map = result.into_iter().next().unwrap();
    assert!(map.is_map());
}

#[test]
fn test_dynamic_to_value_in_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Test that dynamic values work in predicates
    let script = r#"
        let age_limit = 30;
        let g = graph.traversal();
        g.v().has_where("age", gte(age_limit)).count()
    "#;

    let count: i64 = engine.eval_with_graph(graph.clone(), script).unwrap();
    assert_eq!(count, 3); // Alice(30), Carol(35), Dave(40)
}

#[test]
fn test_array_in_within_predicate() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Test that arrays work in within()
    let script = r#"
        let names = ["Alice", "Bob"];
        let g = graph.traversal();
        g.v().has_where("name", within(names)).count()
    "#;

    let count: i64 = engine.eval_with_graph(graph.clone(), script).unwrap();
    assert_eq!(count, 2);
}

#[test]
fn test_mixed_types_in_script() {
    let engine = RhaiEngine::new();
    let graph = create_social_graph();

    // Test script with mixed types
    let script = r#"
        let g = graph.traversal();
        let count = g.v().count();
        let names = g.v().has_label("person").values("name").to_list();
        
        // Return a map with results
        #{
            vertex_count: count,
            person_names: names
        }
    "#;

    let result: rhai::Map = engine.eval_with_graph(graph.clone(), script).unwrap();
    assert!(result.contains_key("vertex_count"));
    assert!(result.contains_key("person_names"));
}
