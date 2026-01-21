//! Tests for Rhai scripting with different storage backends.
//!
//! These tests verify that the Rhai integration works correctly with both
//! in-memory (Graph) and memory-mapped (CowMmapGraph) storage backends.

#![allow(unused_variables)]
#![allow(unused_imports)]

use interstellar::prelude::*;
use interstellar::rhai::RhaiEngine;
use std::collections::HashMap;
use std::sync::Arc;

use super::create_social_graph;

// =============================================================================
// In-Memory Graph Tests (baseline)
// =============================================================================

#[test]
fn test_inmemory_graph_traversal() {
    let graph = create_social_graph();

    let engine = RhaiEngine::new();
    let count: i64 = engine
        .eval_with_graph(
            graph,
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 5);
}

#[test]
fn test_inmemory_graph_values() {
    let graph = create_social_graph();

    let engine = RhaiEngine::new();
    let names: rhai::Array = engine
        .eval_with_graph(
            graph,
            r#"
            let g = graph.gremlin();
            g.v().has_label("person").values("name").to_list()
        "#,
        )
        .unwrap();

    assert_eq!(names.len(), 5);
}

#[test]
fn test_inmemory_graph_navigation() {
    let graph = create_social_graph();

    let engine = RhaiEngine::new();
    let count: i64 = engine
        .eval_with_graph(
            graph,
            r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice").out("knows").count()
        "#,
        )
        .unwrap();

    assert_eq!(count, 2); // Alice knows Bob and Carol
}

// =============================================================================
// Mmap Graph Tests
// =============================================================================

#[cfg(feature = "mmap")]
mod mmap_tests {
    use super::*;
    use interstellar::storage::CowMmapGraph;
    use tempfile::tempdir;

    /// Create a mmap graph with the same structure as the social graph.
    fn create_mmap_social_graph() -> (Arc<CowMmapGraph>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_social.db");

        let graph = CowMmapGraph::open(&path).unwrap();

        // People
        let alice = graph
            .add_vertex(
                "person",
                HashMap::from([
                    ("name".to_string(), Value::String("Alice".to_string())),
                    ("age".to_string(), Value::Int(30)),
                    ("active".to_string(), Value::Bool(true)),
                ]),
            )
            .unwrap();

        let bob = graph
            .add_vertex(
                "person",
                HashMap::from([
                    ("name".to_string(), Value::String("Bob".to_string())),
                    ("age".to_string(), Value::Int(25)),
                    ("active".to_string(), Value::Bool(true)),
                ]),
            )
            .unwrap();

        let carol = graph
            .add_vertex(
                "person",
                HashMap::from([
                    ("name".to_string(), Value::String("Carol".to_string())),
                    ("age".to_string(), Value::Int(35)),
                    ("active".to_string(), Value::Bool(false)),
                ]),
            )
            .unwrap();

        let dave = graph
            .add_vertex(
                "person",
                HashMap::from([
                    ("name".to_string(), Value::String("Dave".to_string())),
                    ("age".to_string(), Value::Int(40)),
                    ("active".to_string(), Value::Bool(true)),
                ]),
            )
            .unwrap();

        let eve = graph
            .add_vertex(
                "person",
                HashMap::from([
                    ("name".to_string(), Value::String("Eve".to_string())),
                    ("age".to_string(), Value::Int(28)),
                    ("active".to_string(), Value::Bool(true)),
                ]),
            )
            .unwrap();

        // Company
        let acme = graph
            .add_vertex(
                "company",
                HashMap::from([
                    ("name".to_string(), Value::String("Acme Corp".to_string())),
                    (
                        "industry".to_string(),
                        Value::String("Technology".to_string()),
                    ),
                ]),
            )
            .unwrap();

        // Edges
        graph
            .add_edge(
                alice,
                bob,
                "knows",
                HashMap::from([("since".to_string(), Value::Int(2020))]),
            )
            .unwrap();

        graph
            .add_edge(
                alice,
                carol,
                "knows",
                HashMap::from([("since".to_string(), Value::Int(2018))]),
            )
            .unwrap();

        graph.add_edge(bob, carol, "knows", HashMap::new()).unwrap();

        graph.add_edge(dave, eve, "knows", HashMap::new()).unwrap();

        graph
            .add_edge(alice, acme, "works_at", HashMap::new())
            .unwrap();

        (Arc::new(graph), dir)
    }

    #[test]
    fn test_mmap_graph_traversal() {
        let (graph, _dir) = create_mmap_social_graph();

        let engine = RhaiEngine::new();
        let count: i64 = engine
            .eval_with_mmap_graph(
                graph,
                r#"
                let g = graph.gremlin();
                g.v().has_label("person").count()
            "#,
            )
            .unwrap();

        assert_eq!(count, 5);
    }

    #[test]
    fn test_mmap_graph_values() {
        let (graph, _dir) = create_mmap_social_graph();

        let engine = RhaiEngine::new();
        let names: rhai::Array = engine
            .eval_with_mmap_graph(
                graph,
                r#"
                let g = graph.gremlin();
                g.v().has_label("person").values("name").to_list()
            "#,
            )
            .unwrap();

        assert_eq!(names.len(), 5);
    }

    #[test]
    fn test_mmap_graph_navigation() {
        let (graph, _dir) = create_mmap_social_graph();

        let engine = RhaiEngine::new();
        let count: i64 = engine
            .eval_with_mmap_graph(
                graph,
                r#"
                let g = graph.gremlin();
                g.v().has_value("name", "Alice").out("knows").count()
            "#,
            )
            .unwrap();

        assert_eq!(count, 2); // Alice knows Bob and Carol
    }

    #[test]
    fn test_mmap_graph_predicate() {
        let (graph, _dir) = create_mmap_social_graph();

        let engine = RhaiEngine::new();
        let count: i64 = engine
            .eval_with_mmap_graph(
                graph,
                r#"
                let g = graph.gremlin();
                g.v().has_where("age", gte(30)).count()
            "#,
            )
            .unwrap();

        assert_eq!(count, 3); // Alice (30), Carol (35), Dave (40)
    }

    #[test]
    fn test_mmap_eval_ast() {
        let (graph, _dir) = create_mmap_social_graph();

        let engine = RhaiEngine::new();
        let ast = engine
            .compile(
                r#"
                let g = graph.gremlin();
                g.v().count()
            "#,
            )
            .unwrap();

        let count: i64 = engine.eval_ast_with_mmap_graph(graph, &ast).unwrap();
        assert_eq!(count, 6); // 5 people + 1 company
    }

    #[test]
    fn test_mmap_eval_dynamic() {
        let (graph, _dir) = create_mmap_social_graph();

        let engine = RhaiEngine::new();
        let result = engine
            .eval_with_mmap_graph_dynamic(
                graph,
                r#"
                let g = graph.gremlin();
                g.v().count()
            "#,
            )
            .unwrap();

        assert_eq!(result.as_int().unwrap(), 6);
    }

    #[test]
    fn test_mmap_run_script() {
        let (graph, _dir) = create_mmap_social_graph();

        let engine = RhaiEngine::new();
        // Should not error - just runs the script without returning a value
        engine
            .run_with_mmap_graph(
                graph,
                r#"
                let g = graph.gremlin();
                let count = g.v().count();
            "#,
            )
            .unwrap();
    }

    // =========================================================================
    // Backend Parity Tests
    // =========================================================================

    /// Helper to run the same script on both backends and compare results.
    fn assert_script_parity(script: &str) {
        let inmem_graph = super::create_social_graph();
        let (mmap_graph, _dir) = create_mmap_social_graph();

        let engine = RhaiEngine::new();

        let inmem_result: rhai::Dynamic = engine.eval_with_graph(inmem_graph, script).unwrap();
        let mmap_result: rhai::Dynamic = engine.eval_with_mmap_graph(mmap_graph, script).unwrap();

        // Compare as strings since Dynamic doesn't implement Eq directly
        assert_eq!(
            format!("{:?}", inmem_result),
            format!("{:?}", mmap_result),
            "Script: {}",
            script
        );
    }

    #[test]
    fn test_parity_v_count() {
        assert_script_parity("graph.gremlin().v().count()");
    }

    #[test]
    fn test_parity_e_count() {
        assert_script_parity("graph.gremlin().e().count()");
    }

    #[test]
    fn test_parity_has_label() {
        assert_script_parity("graph.gremlin().v().has_label(\"person\").count()");
    }

    #[test]
    fn test_parity_out_navigation() {
        assert_script_parity("graph.gremlin().v().out().count()");
    }

    #[test]
    fn test_parity_has_value() {
        assert_script_parity(
            "graph.gremlin().v().has_value(\"name\", \"Alice\").out(\"knows\").count()",
        );
    }

    // =========================================================================
    // Complex Traversal Tests
    // =========================================================================

    #[test]
    fn test_mmap_complex_traversal() {
        let (graph, _dir) = create_mmap_social_graph();

        let engine = RhaiEngine::new();

        // Friends of friends (2-hop)
        let result: rhai::Array = engine
            .eval_with_mmap_graph(
                graph,
                r#"
                let g = graph.gremlin();
                g.v().has_value("name", "Alice")
                    .out("knows")
                    .out("knows")
                    .values("name")
                    .to_list()
            "#,
            )
            .unwrap();

        // Alice -> Bob -> Carol, and Alice -> Carol (no outgoing)
        // So result should be just "Carol" from Alice->Bob->Carol
        assert!(result.len() >= 1);
    }

    #[test]
    fn test_mmap_dedup() {
        let (graph, _dir) = create_mmap_social_graph();

        let engine = RhaiEngine::new();

        let result: rhai::Array = engine
            .eval_with_mmap_graph(
                graph,
                r#"
                let g = graph.gremlin();
                g.v().has_label("person").values("active").dedup().to_list()
            "#,
            )
            .unwrap();

        // Should have 2 unique values: true and false
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_mmap_limit() {
        let (graph, _dir) = create_mmap_social_graph();

        let engine = RhaiEngine::new();

        let result: rhai::Array = engine
            .eval_with_mmap_graph(
                graph,
                r#"
                let g = graph.gremlin();
                g.v().has_label("person").limit(3).to_list()
            "#,
            )
            .unwrap();

        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_mmap_order_by() {
        let (graph, _dir) = create_mmap_social_graph();

        let engine = RhaiEngine::new();

        let result: rhai::Array = engine
            .eval_with_mmap_graph(
                graph,
                r#"
                let g = graph.gremlin();
                g.v().has_label("person").order_by("age").values("name").to_list()
            "#,
            )
            .unwrap();

        // Should be ordered by age: Bob(25), Eve(28), Alice(30), Carol(35), Dave(40)
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].clone().into_string().unwrap(), "Bob");
    }
}
