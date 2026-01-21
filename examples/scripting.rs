//! # Interstellar Scripting with Rhai
//!
//! Demonstrates embedded scripting for dynamic graph queries.
//!
//! Run: `cargo run --example scripting --features rhai`

use interstellar::prelude::*;
use interstellar::rhai::RhaiEngine;
use interstellar::storage::Graph;
use std::collections::HashMap;
use std::sync::Arc;

fn main() {
    println!("=== Interstellar Rhai Scripting ===\n");

    // 1. Create a graph and RhaiEngine
    let graph = build_graph();
    let engine = RhaiEngine::new();

    // 2. Basic traversal via script
    println!("--- Basic Traversal ---");
    let script = r#"
        let g = graph.gremlin();
        g.v().has_label("person").values("name").to_list()
    "#;
    let names: rhai::Array = engine.eval_with_graph(graph.clone(), script).unwrap();
    println!("People: {:?}", names);
    println!(
        "Vertex count: {}",
        eval_count(&engine, graph.clone(), "g.v().count()")
    );

    // 3. Predicates in scripts (gt, between, within)
    println!("\n--- Predicates ---");
    let script = r#"
        let g = graph.gremlin();
        g.v().has_label("person").has_where("age", gt(30)).values("name").to_list()
    "#;
    let over_30: rhai::Array = engine.eval_with_graph(graph.clone(), script).unwrap();
    println!("Age > 30: {:?}", over_30);

    let script = r#"
        let g = graph.gremlin();
        g.v().has_label("person").has_where("age", between(25, 35)).values("name").to_list()
    "#;
    let in_range: rhai::Array = engine.eval_with_graph(graph.clone(), script).unwrap();
    println!("Age 25-35: {:?}", in_range);

    let script = r#"
        let g = graph.gremlin();
        g.v().has_label("person")
            .has_where("city", within(["New York", "Boston"]))
            .values("name").to_list()
    "#;
    let in_cities: rhai::Array = engine.eval_with_graph(graph.clone(), script).unwrap();
    println!("In NYC/Boston: {:?}", in_cities);

    // 4. Navigation patterns (out, in_)
    println!("\n--- Navigation ---");
    let script = r#"
        let g = graph.gremlin();
        g.v().has_value("name", "Alice").out("knows").values("name").to_list()
    "#;
    let friends: rhai::Array = engine.eval_with_graph(graph.clone(), script).unwrap();
    println!("Alice knows: {:?}", friends);

    let script = r#"
        let g = graph.gremlin();
        g.v().has_value("name", "Carol").in_("knows").values("name").to_list()
    "#;
    let known_by: rhai::Array = engine.eval_with_graph(graph.clone(), script).unwrap();
    println!("Who knows Carol: {:?}", known_by);

    let script = r#"
        let g = graph.gremlin();
        g.v().has_value("name", "Alice")
            .out("works_at").in_("works_at")
            .has_label("person").dedup()
            .values("name").to_list()
    "#;
    let coworkers: rhai::Array = engine.eval_with_graph(graph.clone(), script).unwrap();
    println!("Alice's coworkers: {:?}", coworkers);

    // 5. Anonymous traversals (A.out(), A.values())
    println!("\n--- Anonymous Traversals ---");
    let script = r#"
        let g = graph.gremlin();
        g.v().has_value("name", "Alice")
            .union([
                A.out("knows").values("name"),
                A.out("works_at").values("name")
            ])
            .to_list()
    "#;
    let combined: rhai::Array = engine.eval_with_graph(graph.clone(), script).unwrap();
    println!("Union (friends + workplace): {:?}", combined);

    let script = r#"
        let g = graph.gremlin();
        g.v().has_value("name", "Alice")
            .coalesce([
                A.out("manages"),   // No managers exist, so...
                A.out("knows")      // ...fall back to friends
            ])
            .values("name").to_list()
    "#;
    let fallback: rhai::Array = engine.eval_with_graph(graph.clone(), script).unwrap();
    println!("Coalesce (managers or friends): {:?}", fallback);

    // 6. Pre-compiled scripts for performance
    println!("\n--- Pre-compiled Scripts ---");
    let ast = engine
        .compile(r#"graph.gremlin().v().has_label("person").count()"#)
        .expect("Compilation failed");
    println!("Script compiled successfully");
    for i in 1..=3 {
        let count: i64 = engine.eval_ast_with_graph(graph.clone(), &ast).unwrap();
        println!("  Run {}: {} people", i, count);
    }

    // 7. Complex query returning structured data
    println!("\n--- Complex Query ---");
    let script = r#"
        let g = graph.gremlin();
        
        let company = g.v().has_value("name", "Alice")
            .out("works_at").values("name").first();
        
        let fof = g.v().has_value("name", "Alice")
            .out("knows").out("knows")
            .has_label("person").dedup()
            .values("name").to_list();
        
        #{ company: company, friends_of_friends: fof }
    "#;
    let result: rhai::Map = engine.eval_with_graph(graph, script).unwrap();
    println!("Alice's company: {}", result.get("company").unwrap());
    println!(
        "Friends of friends: {:?}",
        result.get("friends_of_friends").unwrap()
    );

    println!("\n=== Done ===");
}

/// Build a sample social network graph.
fn build_graph() -> Arc<Graph> {
    let graph = Graph::new();

    // Create people
    let alice = graph.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("age".to_string(), Value::Int(30)),
            ("city".to_string(), Value::String("New York".to_string())),
        ]),
    );

    let bob = graph.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::String("Bob".to_string())),
            ("age".to_string(), Value::Int(25)),
            ("city".to_string(), Value::String("Boston".to_string())),
        ]),
    );

    let carol = graph.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::String("Carol".to_string())),
            ("age".to_string(), Value::Int(35)),
            ("city".to_string(), Value::String("Chicago".to_string())),
        ]),
    );

    let dave = graph.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::String("Dave".to_string())),
            ("age".to_string(), Value::Int(40)),
            ("city".to_string(), Value::String("Denver".to_string())),
        ]),
    );

    // Create companies
    let acme = graph.add_vertex(
        "company",
        HashMap::from([
            ("name".to_string(), Value::String("Acme Corp".to_string())),
            (
                "industry".to_string(),
                Value::String("Technology".to_string()),
            ),
        ]),
    );

    let _globex = graph.add_vertex(
        "company",
        HashMap::from([
            ("name".to_string(), Value::String("Globex Inc".to_string())),
            ("industry".to_string(), Value::String("Finance".to_string())),
        ]),
    );

    // Create relationships
    graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    graph
        .add_edge(alice, carol, "knows", HashMap::new())
        .unwrap();
    graph.add_edge(bob, carol, "knows", HashMap::new()).unwrap();
    graph.add_edge(bob, dave, "knows", HashMap::new()).unwrap();
    graph
        .add_edge(carol, dave, "knows", HashMap::new())
        .unwrap();
    graph
        .add_edge(alice, acme, "works_at", HashMap::new())
        .unwrap();
    graph
        .add_edge(bob, acme, "works_at", HashMap::new())
        .unwrap();

    Arc::new(graph)
}

/// Helper to evaluate count queries
fn eval_count(engine: &RhaiEngine, graph: Arc<Graph>, expr: &str) -> i64 {
    let script = format!("let g = graph.gremlin(); {}", expr);
    engine.eval_with_graph(graph, &script).unwrap()
}
