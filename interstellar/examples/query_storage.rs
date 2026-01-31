//! # Interstellar Query Storage
//!
//! Demonstrates saving, retrieving, and managing named queries in persistent storage.
//!
//! Run: `cargo run --example query_storage --features mmap`

use interstellar::query::QueryType;
use interstellar::storage::PersistentGraph;
use std::path::PathBuf;
use std::sync::Arc;

fn main() {
    println!("=== Interstellar Query Storage Demo ===\n");

    // Use a temporary directory for the example
    let temp_dir = std::env::temp_dir();
    let db_path: PathBuf = temp_dir.join("interstellar_query_demo.db");

    // Clean up any previous run
    cleanup(&db_path);

    // =========================================================================
    // 1. Create Database and Save Queries
    // =========================================================================
    println!("--- 1. Create Database and Save Queries ---\n");
    {
        let graph = Arc::new(PersistentGraph::open(&db_path).expect("Failed to create database"));
        println!("Created database: {:?}", db_path);

        // Add some sample data first
        let g = graph.gremlin(Arc::clone(&graph));

        let alice_id = g
            .add_v("Person")
            .property("name", "Alice")
            .property("age", 30)
            .property("city", "Boston")
            .next()
            .expect("create Alice")
            .id();

        let bob_id = g
            .add_v("Person")
            .property("name", "Bob")
            .property("age", 25)
            .property("city", "New York")
            .next()
            .expect("create Bob")
            .id();

        let charlie_id = g
            .add_v("Person")
            .property("name", "Charlie")
            .property("age", 35)
            .property("city", "Boston")
            .next()
            .expect("create Charlie")
            .id();

        g.add_e("KNOWS")
            .from_id(alice_id)
            .to_id(bob_id)
            .property("since", 2020)
            .iterate();

        g.add_e("KNOWS")
            .from_id(alice_id)
            .to_id(charlie_id)
            .property("since", 2018)
            .iterate();

        g.add_e("KNOWS")
            .from_id(bob_id)
            .to_id(charlie_id)
            .property("since", 2022)
            .iterate();

        println!(
            "Added {} vertices and {} edges",
            g.v().count(),
            g.e().count()
        );

        // Save some named queries
        println!("\nSaving queries...");

        // Gremlin query with parameter
        let query_id = graph
            .save_query(
                "find_person_by_name",
                QueryType::Gremlin,
                "Find a person by their name",
                "g.V().hasLabel('Person').has('name', $name)",
            )
            .expect("save query");
        println!("  Saved 'find_person_by_name' (id: {})", query_id);

        // GQL query with parameter
        let query_id = graph
            .save_query(
                "people_in_city",
                QueryType::Gql,
                "Find all people in a given city",
                "MATCH (p:Person) WHERE p.city = $city RETURN p.name, p.age",
            )
            .expect("save query");
        println!("  Saved 'people_in_city' (id: {})", query_id);

        // Query without parameters
        let query_id = graph
            .save_query(
                "count_relationships",
                QueryType::Gremlin,
                "Count all KNOWS relationships",
                "g.E().hasLabel('KNOWS').count()",
            )
            .expect("save query");
        println!("  Saved 'count_relationships' (id: {})", query_id);

        // Query with multiple parameters
        let query_id = graph
            .save_query(
                "people_age_range",
                QueryType::Gql,
                "Find people within an age range",
                "MATCH (p:Person) WHERE p.age >= $min_age AND p.age <= $max_age RETURN p",
            )
            .expect("save query");
        println!("  Saved 'people_age_range' (id: {})", query_id);

        // Checkpoint for durability
        graph.checkpoint().expect("checkpoint failed");
        println!("\nCheckpoint complete - queries persisted to disk");
    }

    // =========================================================================
    // 2. Retrieve and Inspect Queries
    // =========================================================================
    println!("\n--- 2. Retrieve and Inspect Queries ---\n");
    {
        let graph = Arc::new(PersistentGraph::open(&db_path).expect("reopen database"));

        // List all queries
        let queries = graph.list_queries();
        println!("Found {} saved queries:\n", queries.len());

        for query in &queries {
            println!("  Query: {}", query.name);
            println!("    ID: {}", query.id);
            println!("    Type: {}", query.query_type);
            println!("    Description: {}", query.description);
            println!("    Text: {}", query.query);
            if !query.parameters.is_empty() {
                let params: Vec<_> = query.parameters.iter().map(|p| &p.name).collect();
                println!("    Parameters: {:?}", params);
            }
            println!();
        }

        // Get a specific query by name
        println!("Looking up 'find_person_by_name'...");
        if let Some(query) = graph.get_query("find_person_by_name") {
            println!("  Found: {} ({})", query.name, query.query_type);
            println!("  Query text: {}", query.query);
        }
    }

    // =========================================================================
    // 3. Delete a Query
    // =========================================================================
    println!("\n--- 3. Delete a Query ---\n");
    {
        let graph = Arc::new(PersistentGraph::open(&db_path).expect("reopen database"));

        println!("Deleting 'count_relationships'...");
        graph
            .delete_query("count_relationships")
            .expect("delete query");
        println!("  Deleted successfully");

        // Verify deletion
        let queries = graph.list_queries();
        println!("\nRemaining queries: {}", queries.len());
        for query in &queries {
            println!("  - {}", query.name);
        }

        // Verify it's really gone
        if graph.get_query("count_relationships").is_none() {
            println!("\n'count_relationships' is no longer accessible");
        }

        graph.checkpoint().expect("checkpoint");
    }

    // =========================================================================
    // 4. Verify Persistence After Reopen
    // =========================================================================
    println!("\n--- 4. Verify Persistence After Reopen ---\n");
    {
        let graph = Arc::new(PersistentGraph::open(&db_path).expect("reopen database"));

        let queries = graph.list_queries();
        println!("Queries after reopen: {}", queries.len());
        for query in &queries {
            println!("  - {} ({})", query.name, query.query_type);
        }

        // The deleted query should still be gone
        assert!(
            graph.get_query("count_relationships").is_none(),
            "Deleted query should not exist after reopen"
        );
        println!("\nDeleted query correctly not found after reopen");
    }

    // =========================================================================
    // 5. Cleanup
    // =========================================================================
    println!("\n--- 5. Cleanup ---\n");
    cleanup(&db_path);
    println!("Database files removed");

    println!("\n=== Demo Complete ===");
}

/// Remove database files
fn cleanup(db_path: &PathBuf) {
    if db_path.exists() {
        std::fs::remove_file(db_path).ok();
    }
    let wal_path = db_path.with_extension("wal");
    if wal_path.exists() {
        std::fs::remove_file(&wal_path).ok();
    }
    let idx_path = db_path.with_extension("db.idx");
    if idx_path.exists() {
        std::fs::remove_file(&idx_path).ok();
    }
}
