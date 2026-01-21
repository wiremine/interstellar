//! # Interstellar Persistent Storage
//!
//! Demonstrates memory-mapped storage for data persistence.
//!
//! Run: `cargo run --example storage --features mmap`

use interstellar::storage::PersistentGraph;
use interstellar::value::Value;
use std::path::PathBuf;

fn main() {
    println!("=== Interstellar Persistent Storage Demo ===\n");

    // Use a temporary directory for the example
    let temp_dir = std::env::temp_dir();
    let db_path: PathBuf = temp_dir.join("interstellar_storage_demo.db");

    // Clean up any previous run
    cleanup(&db_path);

    // =========================================================================
    // 1. Create a persistent database and add data
    // =========================================================================
    println!("--- 1. Create Database and Add Data ---\n");
    {
        let graph = PersistentGraph::open(&db_path).expect("Failed to create database");
        println!("Created database: {:?}", db_path);

        let g = graph.gremlin();

        // Add vertices
        let alice_id = g
            .add_v("Person")
            .property("name", "Alice")
            .property("age", 30)
            .next()
            .expect("Failed to create Alice")
            .as_vertex_id()
            .expect("Expected vertex ID");

        let bob_id = g
            .add_v("Person")
            .property("name", "Bob")
            .property("age", 28)
            .next()
            .expect("Failed to create Bob")
            .as_vertex_id()
            .expect("Expected vertex ID");

        // Add an edge
        g.add_e("KNOWS")
            .from_id(alice_id)
            .to_id(bob_id)
            .property("since", 2020)
            .iterate();

        println!("Added 2 vertices and 1 edge");
        println!("Vertex count: {}", g.v().count());
        println!("Edge count: {}", g.e().count());

        // Checkpoint for durability
        graph.checkpoint().expect("Checkpoint failed");
        println!("\nCheckpoint complete - data persisted to disk");

        // Graph is dropped here, simulating process exit
    }

    // =========================================================================
    // 2. Reopen and verify data persisted
    // =========================================================================
    println!("\n--- 2. Reopen and Verify Persistence ---\n");
    {
        let graph = PersistentGraph::open(&db_path).expect("Failed to reopen database");
        let g = graph.gremlin();

        // Verify counts
        let vertex_count = g.v().count();
        let edge_count = g.e().count();
        println!(
            "After reopen - Vertices: {}, Edges: {}",
            vertex_count, edge_count
        );

        // Query the persisted data
        let names: Vec<Value> = g.v().has_label("Person").values("name").to_list();
        println!("People in graph: {:?}", names);

        // Traverse relationships
        let alice_knows: Vec<Value> = g
            .v()
            .has_value("name", Value::from("Alice"))
            .out_label("KNOWS")
            .values("name")
            .to_list();
        println!("Alice knows: {:?}", alice_knows);
    }

    // =========================================================================
    // 3. Cleanup
    // =========================================================================
    println!("\n--- 3. Cleanup ---\n");
    cleanup(&db_path);
    println!("Database files removed");

    println!("\n=== Demo Complete ===");
}

/// Remove database files
fn cleanup(db_path: &PathBuf) {
    if db_path.exists() {
        std::fs::remove_file(db_path).ok();
    }
    let idx_path = db_path.with_extension("db.idx");
    if idx_path.exists() {
        std::fs::remove_file(&idx_path).ok();
    }
}
