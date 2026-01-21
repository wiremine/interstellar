//! # Interstellar Scripting with Rhai
//!
//! Demonstrates embedded scripting for dynamic graph queries.
//!
//! Run: `cargo run --example scripting --features rhai`

use interstellar::rhai::RhaiEngine;
use interstellar::storage::Graph;
use std::sync::Arc;

fn main() {
    println!("=== Interstellar Rhai Scripting ===\n");

    // 1. Create an empty graph and RhaiEngine
    let graph = Arc::new(Graph::new());
    let engine = RhaiEngine::new();

    // 2. Build the graph using Rhai scripting
    println!("--- Building Graph via Script ---");
    let build_script = r#"
        let g = graph.gremlin();
        
        // Create people - use .id().first() to get IDs for edge creation
        let alice = g.add_v("person")
            .property("name", "Alice")
            .property("age", 30)
            .property("city", "New York")
            .id().first();
        
        let bob = g.add_v("person")
            .property("name", "Bob")
            .property("age", 25)
            .property("city", "Boston")
            .id().first();
        
        let carol = g.add_v("person")
            .property("name", "Carol")
            .property("age", 35)
            .property("city", "Chicago")
            .id().first();
        
        let dave = g.add_v("person")
            .property("name", "Dave")
            .property("age", 40)
            .property("city", "Denver")
            .id().first();
        
        // Create companies
        let acme = g.add_v("company")
            .property("name", "Acme Corp")
            .property("industry", "Technology")
            .id().first();
        
        let globex = g.add_v("company")
            .property("name", "Globex Inc")
            .property("industry", "Finance")
            .id().first();
        
        // Create relationships using the captured vertex IDs
        g.add_e("knows").from_v(alice).to_v(bob).first();
        g.add_e("knows").from_v(alice).to_v(carol).first();
        g.add_e("knows").from_v(bob).to_v(carol).first();
        g.add_e("knows").from_v(bob).to_v(dave).first();
        g.add_e("knows").from_v(carol).to_v(dave).first();
        g.add_e("works_at").from_v(alice).to_v(acme).first();
        g.add_e("works_at").from_v(bob).to_v(acme).first();
        
        // Return stats
        #{ vertices: g.v().count(), edges: g.e().count() }
    "#;
    let stats: rhai::Map = engine.eval_with_graph(graph.clone(), build_script).unwrap();
    println!(
        "Created {} vertices and {} edges",
        stats.get("vertices").unwrap(),
        stats.get("edges").unwrap()
    );

    // 3. Basic traversal via script
    println!("\n--- Basic Traversal ---");
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

    // 4. Predicates in scripts (gt, between, within)
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

    // 5. Navigation patterns (out, in_)
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

    // 6. Anonymous traversals (A.out(), A.values())
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

    // 7. Pre-compiled scripts for performance
    println!("\n--- Pre-compiled Scripts ---");
    let ast = engine
        .compile(r#"graph.gremlin().v().has_label("person").count()"#)
        .expect("Compilation failed");
    println!("Script compiled successfully");
    for i in 1..=3 {
        let count: i64 = engine.eval_ast_with_graph(graph.clone(), &ast).unwrap();
        println!("  Run {}: {} people", i, count);
    }

    // 8. Complex query returning structured data
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
    let result: rhai::Map = engine.eval_with_graph(graph.clone(), script).unwrap();
    println!("Alice's company: {}", result.get("company").unwrap());
    println!(
        "Friends of friends: {:?}",
        result.get("friends_of_friends").unwrap()
    );

    // 9. Dynamic graph updates via script
    println!("\n--- Dynamic Updates ---");
    let update_script = r#"
        let g = graph.gremlin();
        
        // Add a new person
        let eve = g.add_v("person")
            .property("name", "Eve")
            .property("age", 28)
            .property("city", "Seattle")
            .id().first();
        
        // Connect Eve to the social network
        let alice = g.v().has_value("name", "Alice").id().first();
        g.add_e("knows").from_v(alice).to_v(eve).first();
        
        // Eve works at Globex
        let globex = g.v().has_value("name", "Globex Inc").id().first();
        g.add_e("works_at").from_v(eve).to_v(globex).first();
        
        // Return updated stats
        #{ 
            vertices: g.v().count(), 
            edges: g.e().count(),
            people: g.v().has_label("person").values("name").to_list()
        }
    "#;
    let updated: rhai::Map = engine.eval_with_graph(graph, update_script).unwrap();
    println!(
        "After update: {} vertices, {} edges",
        updated.get("vertices").unwrap(),
        updated.get("edges").unwrap()
    );
    println!("All people: {:?}", updated.get("people").unwrap());

    println!("\n=== Done ===");
}

/// Helper to evaluate count queries
fn eval_count(engine: &RhaiEngine, graph: Arc<Graph>, expr: &str) -> i64 {
    let script = format!("let g = graph.gremlin(); {}", expr);
    engine.eval_with_graph(graph, &script).unwrap()
}
