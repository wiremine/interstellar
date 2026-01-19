//! Unified API Example for CowMmapGraph (Persistent Memory-Mapped)
//!
//! This example demonstrates how `CowMmapGraph` provides the same unified API
//! as `CowGraph`, but with persistent storage backed by memory-mapped files.
//!
//! # Key Features Demonstrated
//!
//! 1. **Persistent Storage**: Data survives process restarts
//! 2. **Gremlin-style Traversal API**: Same fluent API as CowGraph
//! 3. **GQL Mutations**: SQL-like syntax for graph mutations
//! 4. **Unified Interface**: No mode switching between reads and writes
//! 5. **Checkpointing**: Explicit durability control
//!
//! Run: `cargo run --example cow_mmap_unified_api --features mmap`

use interstellar::storage::cow_mmap::CowMmapGraph;
use interstellar::value::{Value, VertexId};
use std::path::PathBuf;

fn main() {
    println!("=== CowMmapGraph Unified API Demo ===\n");

    // Use a temporary directory for the example
    let temp_dir = std::env::temp_dir();
    let db_path: PathBuf = temp_dir.join("interstellar_demo.db");

    // Clean up any previous run
    if db_path.exists() {
        std::fs::remove_file(&db_path).ok();
    }
    // Also clean up the index file if it exists
    let index_path = db_path.with_extension("db.idx");
    if index_path.exists() {
        std::fs::remove_file(&index_path).ok();
    }

    println!("Database path: {:?}\n", db_path);

    // Store vertex IDs for use across scopes (initialized in Part 1)
    let alice_id: VertexId;
    let bob_id: VertexId;
    let charlie_id: VertexId;

    // =========================================================================
    // Part 1: Create and Populate Graph
    // =========================================================================
    {
        println!("--- Part 1: Create and Populate Graph ---\n");

        // Open/create the persistent graph
        let graph = CowMmapGraph::open(&db_path).expect("Failed to open graph");

        // Get a traversal source
        let g = graph.traversal();

        // Create vertices using Gremlin-style API
        // next() returns Option<Value>, extract VertexId with as_vertex_id()
        alice_id = g
            .add_v("Person")
            .property("name", "Alice")
            .property("age", 30)
            .property("department", "Engineering")
            .next()
            .expect("Failed to create Alice")
            .as_vertex_id()
            .expect("Expected vertex ID");

        bob_id = g
            .add_v("Person")
            .property("name", "Bob")
            .property("age", 28)
            .property("department", "Marketing")
            .next()
            .expect("Failed to create Bob")
            .as_vertex_id()
            .expect("Expected vertex ID");

        charlie_id = g
            .add_v("Person")
            .property("name", "Charlie")
            .property("age", 35)
            .property("department", "Engineering")
            .next()
            .expect("Failed to create Charlie")
            .as_vertex_id()
            .expect("Expected vertex ID");

        println!(
            "Created vertices: Alice={:?}, Bob={:?}, Charlie={:?}",
            alice_id, bob_id, charlie_id
        );

        // Create edges
        g.add_e("WORKS_WITH")
            .from_id(alice_id)
            .to_id(charlie_id)
            .property("project", "Phoenix")
            .iterate();

        g.add_e("KNOWS")
            .from_id(alice_id)
            .to_id(bob_id)
            .property("since", 2021)
            .iterate();

        g.add_e("KNOWS")
            .from_id(bob_id)
            .to_id(charlie_id)
            .property("since", 2022)
            .iterate();

        println!("Created edges\n");

        // Query to verify
        let all_people: Vec<Value> = g.v().has_label("Person").values("name").to_list();
        println!("People in graph: {:?}", all_people);
        println!("Vertex count: {}", g.v().count());
        println!("Edge count: {}", g.e().count());

        // Checkpoint to ensure data is persisted
        graph.checkpoint().expect("Checkpoint failed");
        println!("\nCheckpoint complete - data persisted to disk");

        // Graph will be dropped here, simulating process exit
    }

    // =========================================================================
    // Part 2: Reopen and Verify Persistence
    // =========================================================================
    {
        println!("\n--- Part 2: Reopen and Verify Persistence ---\n");

        // Reopen the same database
        let graph = CowMmapGraph::open(&db_path).expect("Failed to reopen graph");
        let g = graph.traversal();

        // Verify data persisted
        let vertex_count = g.v().count();
        let edge_count = g.e().count();
        println!(
            "After reopen - Vertices: {}, Edges: {}",
            vertex_count, edge_count
        );

        // Query the persisted data
        let engineers: Vec<Value> = g
            .v()
            .has_label("Person")
            .has_value("department", Value::from("Engineering"))
            .values("name")
            .to_list();
        println!("Engineers: {:?}", engineers);

        // Find people aged exactly 30 using has_value
        let age_30: Vec<Value> = g
            .v()
            .has_label("Person")
            .has_value("age", Value::from(30))
            .values("name")
            .to_list();
        println!("People aged exactly 30: {:?}", age_30);
    }

    // =========================================================================
    // Part 3: GQL Mutations on Persistent Graph
    // =========================================================================
    {
        println!("\n--- Part 3: GQL Mutations ---\n");

        let graph = CowMmapGraph::open(&db_path).expect("Failed to reopen graph");

        // Add new person with relationship via GQL (creates both nodes and edge)
        let results = graph
            .gql(
                r#"
                CREATE (d:Person {name: 'Diana', age: 32, department: 'Sales'})-[:MENTORS {since: 2023}]->(e:Person {name: 'Eve', age: 24, department: 'Sales'})
                RETURN d.name, e.name
                "#,
            )
            .expect("GQL CREATE failed");
        println!("Created via GQL: {:?}", results);

        // Update via GQL SET
        graph
            .gql(
                r#"
                MATCH (b:Person {name: 'Bob'})
                SET b.promoted = true, b.title = 'Senior Manager'
                "#,
            )
            .expect("GQL SET failed");
        println!("Updated Bob's properties via GQL");

        // Verify with Gremlin query
        let g = graph.traversal();
        let all_names: Vec<Value> = g.v().has_label("Person").values("name").to_list();
        println!("All people after GQL: {:?}", all_names);

        graph.checkpoint().expect("Checkpoint failed");
        println!("Checkpoint complete");
    }

    // =========================================================================
    // Part 4: Complex Queries
    // =========================================================================
    {
        println!("\n--- Part 4: Complex Queries ---\n");

        let graph = CowMmapGraph::open(&db_path).expect("Failed to reopen graph");
        let g = graph.traversal();

        // Who does Alice know? (using stored ID)
        let alice_knows: Vec<Value> = g.v_id(alice_id).out_label("KNOWS").values("name").to_list();
        println!("Alice knows: {:?}", alice_knows);

        // Who works with Alice?
        let alice_coworkers: Vec<Value> = g
            .v_id(alice_id)
            .out_label("WORKS_WITH")
            .values("name")
            .to_list();
        println!("Alice works with: {:?}", alice_coworkers);

        // Find all KNOWS relationships
        let knows_edges = g.e().has_label("KNOWS").count();
        println!("Total KNOWS relationships: {}", knows_edges);

        // Find MENTORS relationships (created via GQL)
        let mentors_edges = g.e().has_label("MENTORS").count();
        println!("Total MENTORS relationships: {}", mentors_edges);

        // People using limit
        let first_two: Vec<Value> = g.v().has_label("Person").limit(2).values("name").to_list();
        println!("First 2 people: {:?}", first_two);
    }

    // =========================================================================
    // Part 5: Mutations via Traversal
    // =========================================================================
    {
        println!("\n--- Part 5: Traversal Mutations ---\n");

        let graph = CowMmapGraph::open(&db_path).expect("Failed to reopen graph");
        let g = graph.traversal();

        // Add property via traversal
        g.v_id(charlie_id).property("skill", "Rust").iterate();
        println!("Added skill property to Charlie");

        // Verify
        let charlie_skill: Vec<Value> = g.v_id(charlie_id).values("skill").to_list();
        println!("Charlie's skill: {:?}", charlie_skill);

        // Drop WORKS_WITH edges
        let before_count = g.e().has_label("WORKS_WITH").count();
        println!("WORKS_WITH edges before drop: {}", before_count);

        g.e().has_label("WORKS_WITH").drop().iterate();

        let after_count = g.e().has_label("WORKS_WITH").count();
        println!("WORKS_WITH edges after drop: {}", after_count);

        graph.checkpoint().expect("Checkpoint failed");
    }

    // =========================================================================
    // Part 6: GQL DELETE
    // =========================================================================
    {
        println!("\n--- Part 6: GQL DELETE ---\n");

        let graph = CowMmapGraph::open(&db_path).expect("Failed to reopen graph");
        let g = graph.traversal();

        let before = g.v().count();
        println!("Vertices before delete: {}", before);

        // Delete Eve with DETACH DELETE (removes vertex and connected edges)
        graph
            .gql(
                r#"
                MATCH (e:Person {name: 'Eve'})
                DETACH DELETE e
                "#,
            )
            .expect("GQL DELETE failed");
        println!("Deleted Eve via DETACH DELETE");

        let after = g.v().count();
        println!("Vertices after delete: {}", after);

        // Final state
        let remaining: Vec<Value> = g.v().has_label("Person").values("name").to_list();
        println!("Remaining people: {:?}", remaining);

        graph.checkpoint().expect("Final checkpoint");
    }

    // =========================================================================
    // Cleanup
    // =========================================================================
    println!("\n--- Cleanup ---\n");
    std::fs::remove_file(&db_path).ok();
    std::fs::remove_file(db_path.with_extension("db.idx")).ok();
    println!("Cleaned up database files");

    // =========================================================================
    // Summary
    // =========================================================================
    println!("\n=== Summary ===");
    println!("Demonstrated CowMmapGraph unified API:");
    println!("  - Same Gremlin API as CowGraph (add_v, add_e, v, out, etc.)");
    println!("  - Same GQL mutations (CREATE, SET, DELETE)");
    println!("  - Data persists across process restarts");
    println!("  - Explicit checkpointing for durability control");
    println!("  - No mode switching between reads and writes");
}
