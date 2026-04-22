//! # Interstellar Gremlin Script Quickstart
//!
//! A demonstration of Interstellar's Gremlin text script execution with variable support.
//!
//! This example demonstrates:
//! - Executing multi-statement Gremlin scripts
//! - Variable assignment and reference
//! - REPL-style workflows with persistent context
//! - Accessing bound variables after execution
//!
//! Run: `cargo run --example quickstart_gremlin_script --features gremlin`

use std::sync::Arc;

use interstellar::gremlin::{ExecutionResult, VariableContext};
use interstellar::storage::Graph;

fn main() {
    println!("=== Interstellar Gremlin Script Quickstart ===\n");

    // -------------------------------------------------------------------------
    // 1. Create an in-memory graph
    // -------------------------------------------------------------------------
    let graph = Arc::new(Graph::new());

    // -------------------------------------------------------------------------
    // 2. Execute a multi-statement script
    // -------------------------------------------------------------------------
    println!("-- Multi-Statement Script Execution --\n");

    let script_result = graph
        .execute_script(
            r#"
            alice = g.addV('Person').property('name', 'Alice').property('age', 30).next()
            bob = g.addV('Person').property('name', 'Bob').property('age', 25).next()
            carol = g.addV('Person').property('name', 'Carol').property('age', 35).next()
            acme = g.addV('Company').property('name', 'Acme Corp').next()
            g.addE('knows').from(alice).to(bob).property('since', 2020).next()
            g.addE('knows').from(alice).to(carol).next()
            g.addE('knows').from(bob).to(carol).next()
            g.addE('works_at').from(alice).to(acme).next()
            g.V().hasLabel('Person').values('name').toList()
        "#,
        )
        .expect("Script execution failed");

    // The result is from the last statement
    println!("All person names:");
    if let ExecutionResult::List(names) = &script_result.result {
        for name in names {
            println!("  - {:?}", name);
        }
    }

    // Variables are accessible after execution
    println!("\nBound variables:");
    for var_name in script_result.variables.variables() {
        let value = script_result.variables.get(var_name).unwrap();
        println!("  {} = {:?}", var_name, value);
    }

    println!(
        "\nGraph state: {} vertices, {} edges",
        graph.vertex_count(),
        graph.edge_count()
    );

    // -------------------------------------------------------------------------
    // 3. REPL-style workflow with persistent context
    // -------------------------------------------------------------------------
    println!("\n-- REPL-Style Workflow --\n");

    // Start with a fresh graph for this demo
    let repl_graph = Arc::new(Graph::new());
    let mut ctx = VariableContext::new();

    // Simulate REPL commands one at a time

    // Command 1: Create a vertex
    println!("> marko = g.addV('Person').property('name', 'Marko').next()");
    let result = repl_graph
        .execute_script_with_context(
            "marko = g.addV('Person').property('name', 'Marko').next()",
            ctx,
        )
        .unwrap();
    ctx = result.variables;
    println!("  (created vertex, marko is now bound)\n");

    // Command 2: Create another vertex
    println!("> vadas = g.addV('Person').property('name', 'Vadas').next()");
    let result = repl_graph
        .execute_script_with_context(
            "vadas = g.addV('Person').property('name', 'Vadas').next()",
            ctx,
        )
        .unwrap();
    ctx = result.variables;
    println!("  (created vertex, vadas is now bound)\n");

    // Command 3: Create an edge using previously bound variables
    println!("> g.addE('knows').from(marko).to(vadas).property('weight', 0.5).next()");
    let result = repl_graph
        .execute_script_with_context(
            "g.addE('knows').from(marko).to(vadas).property('weight', 0.5).next()",
            ctx,
        )
        .unwrap();
    ctx = result.variables;
    println!("  (created edge from marko to vadas)\n");

    // Command 4: Query using a bound variable
    println!("> g.V(marko).out('knows').values('name').toList()");
    let result = repl_graph
        .execute_script_with_context("g.V(marko).out('knows').values('name').toList()", ctx)
        .unwrap();
    ctx = result.variables;

    if let ExecutionResult::List(names) = &result.result {
        println!("  Result: {:?}\n", names);
    }

    // Show current session state
    println!(
        "Session variables: {:?}",
        ctx.variables().collect::<Vec<_>>()
    );

    // -------------------------------------------------------------------------
    // 4. Accessing vertex IDs from variables
    // -------------------------------------------------------------------------
    println!("\n-- Accessing Vertex IDs --\n");

    if let Some(marko_id) = ctx.get_vertex_id("marko") {
        println!("marko's vertex ID: {:?}", marko_id);
    }

    if let Some(vadas_id) = ctx.get_vertex_id("vadas") {
        println!("vadas's vertex ID: {:?}", vadas_id);
    }

    // -------------------------------------------------------------------------
    // 5. Complex traversal with variables
    // -------------------------------------------------------------------------
    println!("\n-- Complex Traversal with Variables --\n");

    let complex_graph = Arc::new(Graph::new());

    let result = complex_graph
        .execute_script(
            r#"
            peter = g.addV('Person').property('name', 'Peter').property('age', 35).next()
            josh = g.addV('Person').property('name', 'Josh').property('age', 32).next()
            lop = g.addV('Software').property('name', 'lop').property('lang', 'java').next()
            ripple = g.addV('Software').property('name', 'ripple').property('lang', 'java').next()
            
            g.addE('created').from(peter).to(lop).property('weight', 0.2).next()
            g.addE('created').from(josh).to(lop).property('weight', 0.4).next()
            g.addE('created').from(josh).to(ripple).property('weight', 1.0).next()
            
            g.V(lop).in('created').values('name').toList()
        "#,
        )
        .expect("Complex script failed");

    println!("Who created 'lop'?");
    if let ExecutionResult::List(creators) = &result.result {
        for creator in creators {
            println!("  - {:?}", creator);
        }
    }

    println!(
        "\nVariables from script: {:?}",
        result.variables.variables().collect::<Vec<_>>()
    );

    // -------------------------------------------------------------------------
    // 6. Error handling
    // -------------------------------------------------------------------------
    println!("\n-- Error Handling --\n");

    let error_graph = Arc::new(Graph::new());

    // Try to use an undefined variable
    let result = error_graph.execute_script("g.V(undefined_var).toList()");

    match result {
        Ok(_) => println!("Unexpected success"),
        Err(e) => println!("Expected error for undefined variable:\n  {}", e),
    }

    println!("\n=== Gremlin Script Quickstart Complete ===");
}
