//! Rhai Scripting Integration Example
//!
//! This example demonstrates how to use Rhai scripts to query graph data
//! in Interstellar. Rhai provides a safe, embedded scripting language that
//! exposes the full traversal API.
//!
//! Run with: cargo run --example rhai_scripting --features rhai

use interstellar::prelude::*;
use interstellar::rhai::RhaiEngine;
use interstellar::storage::Graph;
use std::collections::HashMap;
use std::sync::Arc;

fn main() {
    println!("=== Interstellar Rhai Scripting Example ===\n");

    // Create a sample social graph (wrapped in Arc for sharing with Rhai)
    let graph = create_social_graph();
    let engine = RhaiEngine::new();

    // Example 1: Basic traversal
    example_basic_traversal(&engine, graph.clone());

    // Example 2: Filtering with predicates
    example_predicates(&engine, graph.clone());

    // Example 3: Navigation patterns
    example_navigation(&engine, graph.clone());

    // Example 4: Anonymous traversals
    example_anonymous(&engine, graph.clone());

    // Example 5: Pre-compiled scripts
    example_precompiled(&engine, graph.clone());

    // Example 6: Complex query
    example_complex_query(&engine, graph);

    println!("\n=== All examples completed! ===");
}

/// Create a sample social network graph.
fn create_social_graph() -> Arc<Graph> {
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

    let globex = graph.add_vertex(
        "company",
        HashMap::from([
            ("name".to_string(), Value::String("Globex Inc".to_string())),
            ("industry".to_string(), Value::String("Finance".to_string())),
        ]),
    );

    // Create relationships
    graph
        .add_edge(
            alice,
            bob,
            "knows",
            HashMap::from([("since".to_string(), Value::Int(2020))]),
        )
        .unwrap();
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
    graph
        .add_edge(carol, globex, "works_at", HashMap::new())
        .unwrap();

    Arc::new(graph)
}

fn example_basic_traversal(engine: &RhaiEngine, graph: Arc<Graph>) {
    println!("--- Example 1: Basic Traversal ---");

    // Count vertices
    let script = r#"
        let g = graph.gremlin();
        g.v().count()
    "#;

    let count: i64 = engine.eval_with_graph(graph.clone(), script).unwrap();
    println!("Total vertices: {}", count);

    // Get all person names
    let script = r#"
        let g = graph.gremlin();
        g.v().has_label("person").values("name").to_list()
    "#;

    let names: rhai::Array = engine.eval_with_graph(graph, script).unwrap();
    print!("People: ");
    for (i, name) in names.iter().enumerate() {
        if i > 0 {
            print!(", ");
        }
        print!("{}", name);
    }
    println!("\n");
}

fn example_predicates(engine: &RhaiEngine, graph: Arc<Graph>) {
    println!("--- Example 2: Filtering with Predicates ---");

    // Find people over 30
    let script = r#"
        let g = graph.gremlin();
        g.v().has_label("person").has_where("age", gt(30)).values("name").to_list()
    "#;

    let names: rhai::Array = engine.eval_with_graph(graph.clone(), script).unwrap();
    print!("People over 30: ");
    for (i, name) in names.iter().enumerate() {
        if i > 0 {
            print!(", ");
        }
        print!("{}", name);
    }
    println!();

    // Find people between 25 and 35
    let script = r#"
        let g = graph.gremlin();
        g.v().has_label("person")
            .has_where("age", between(25, 35))
            .values("name")
            .to_list()
    "#;

    let names: rhai::Array = engine.eval_with_graph(graph.clone(), script).unwrap();
    print!("People aged 25-35: ");
    for (i, name) in names.iter().enumerate() {
        if i > 0 {
            print!(", ");
        }
        print!("{}", name);
    }
    println!();

    // Find people in specific cities
    let script = r#"
        let g = graph.gremlin();
        g.v().has_label("person")
            .has_where("city", within(["New York", "Boston"]))
            .values("name")
            .to_list()
    "#;

    let names: rhai::Array = engine.eval_with_graph(graph, script).unwrap();
    print!("People in NYC or Boston: ");
    for (i, name) in names.iter().enumerate() {
        if i > 0 {
            print!(", ");
        }
        print!("{}", name);
    }
    println!("\n");
}

fn example_navigation(engine: &RhaiEngine, graph: Arc<Graph>) {
    println!("--- Example 3: Navigation Patterns ---");

    // Find who Alice knows
    let script = r#"
        let g = graph.gremlin();
        g.v().has_value("name", "Alice").out("knows").values("name").to_list()
    "#;

    let friends: rhai::Array = engine.eval_with_graph(graph.clone(), script).unwrap();
    print!("Alice knows: ");
    for (i, name) in friends.iter().enumerate() {
        if i > 0 {
            print!(", ");
        }
        print!("{}", name);
    }
    println!();

    // Find who knows Carol
    let script = r#"
        let g = graph.gremlin();
        g.v().has_value("name", "Carol").in_("knows").values("name").to_list()
    "#;

    let knowers: rhai::Array = engine.eval_with_graph(graph.clone(), script).unwrap();
    print!("People who know Carol: ");
    for (i, name) in knowers.iter().enumerate() {
        if i > 0 {
            print!(", ");
        }
        print!("{}", name);
    }
    println!();

    // Find Alice's coworkers (people at the same company)
    let script = r#"
        let g = graph.gremlin();
        g.v().has_value("name", "Alice")
            .out("works_at")
            .in_("works_at")
            .has_label("person")
            .dedup()
            .values("name")
            .to_list()
    "#;

    let coworkers: rhai::Array = engine.eval_with_graph(graph, script).unwrap();
    print!("Alice's coworkers (including herself): ");
    for (i, name) in coworkers.iter().enumerate() {
        if i > 0 {
            print!(", ");
        }
        print!("{}", name);
    }
    println!("\n");
}

fn example_anonymous(engine: &RhaiEngine, graph: Arc<Graph>) {
    println!("--- Example 4: Anonymous Traversals ---");

    // Union: find both friends and workplace
    let script = r#"
        let g = graph.gremlin();
        g.v().has_value("name", "Alice")
            .union([
                A.out("knows").values("name"),
                A.out("works_at").values("name")
            ])
            .to_list()
    "#;

    let results: rhai::Array = engine.eval_with_graph(graph.clone(), script).unwrap();
    print!("Alice's connections (friends + workplace): ");
    for (i, name) in results.iter().enumerate() {
        if i > 0 {
            print!(", ");
        }
        print!("{}", name);
    }
    println!();

    // Coalesce: try to find manager, fall back to direct reports
    let script = r#"
        let g = graph.gremlin();
        g.v().has_value("name", "Alice")
            .coalesce([
                A.out("manages"),    // No managers, so...
                A.out("knows")       // Fall back to friends
            ])
            .values("name")
            .to_list()
    "#;

    let results: rhai::Array = engine.eval_with_graph(graph, script).unwrap();
    print!("Coalesce result (managers or friends): ");
    for (i, name) in results.iter().enumerate() {
        if i > 0 {
            print!(", ");
        }
        print!("{}", name);
    }
    println!("\n");
}

fn example_precompiled(engine: &RhaiEngine, graph: Arc<Graph>) {
    println!("--- Example 5: Pre-compiled Scripts ---");

    // Compile once, run multiple times
    let ast = engine
        .compile(
            r#"
        let g = graph.gremlin();
        g.v().has_label("person").values("name").to_list()
    "#,
        )
        .expect("Compilation failed");

    println!("Script compiled successfully!");

    // Execute multiple times
    for i in 1..=3 {
        let names: rhai::Array = engine.eval_ast_with_graph(graph.clone(), &ast).unwrap();
        println!("  Execution {}: found {} people", i, names.len());
    }
    println!();
}

fn example_complex_query(engine: &RhaiEngine, graph: Arc<Graph>) {
    println!("--- Example 6: Complex Query ---");

    // Find friends of friends who work at a different company
    let script = r#"
        // Define reusable traversal fragments
        let friends = A.out("knows");
        let workplace = A.out("works_at");
        
        let g = graph.gremlin();
        
        // Start from Alice
        let alice_company = g.v()
            .has_value("name", "Alice")
            .out("works_at")
            .values("name")
            .first();
        
        // Find friends of friends
        let fof = g.v()
            .has_value("name", "Alice")
            .out("knows")           // Direct friends
            .out("knows")           // Their friends
            .has_label("person")
            .dedup()
            .values("name")
            .to_list();
        
        // Return results as a map
        #{
            alice_company: alice_company,
            friends_of_friends: fof
        }
    "#;

    let result: rhai::Map = engine.eval_with_graph(graph, script).unwrap();

    println!("Alice works at: {}", result.get("alice_company").unwrap());
    print!("Friends of friends: ");
    if let Some(fof) = result.get("friends_of_friends") {
        if let Some(arr) = fof.clone().try_cast::<rhai::Array>() {
            for (i, name) in arr.iter().enumerate() {
                if i > 0 {
                    print!(", ");
                }
                print!("{}", name);
            }
        }
    }
    println!();
}
