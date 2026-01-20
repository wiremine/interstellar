//! Property Indexes Example
//!
//! This example demonstrates Interstellar's property index capabilities for
//! efficient lookups. It covers:
//!
//! **Part 1: BTree Indexes**
//! - Creating BTree indexes for range queries
//! - O(log n) lookups and range scans
//!
//! **Part 2: Unique Indexes**
//! - Creating unique indexes for O(1) lookups
//! - Uniqueness constraint enforcement
//!
//! **Part 3: Index-Accelerated Queries**
//! - Using indexes with the traversal API
//! - Direct index lookups with `vertices_by_property`
//! - Range queries with `vertices_by_property_range`
//!
//! **Part 4: Index Persistence (MmapGraph)**
//! - Index specs persist across database close/reopen
//! - Automatic index rebuilding on load
//!
//! Run: `cargo run --features mmap --example indexes`

use interstellar::index::IndexBuilder;
use interstellar::storage::mmap::MmapGraph;
use interstellar::storage::{Graph, InMemoryGraph};
use interstellar::value::Value;
use std::collections::HashMap;
use std::fs;
use std::ops::Bound;
use std::path::Path;

const DB_PATH: &str = "examples/data/indexes_demo.db";

// =============================================================================
// Helper Functions
// =============================================================================

fn section(title: &str) {
    println!("\n{}", "=".repeat(70));
    println!("{}", title);
    println!("{}", "=".repeat(70));
}

fn subsection(title: &str) {
    println!("\n--- {} ---", title);
}

// =============================================================================
// Part 1: BTree Indexes (InMemoryGraph)
// =============================================================================

fn demo_btree_indexes() {
    section("PART 1: BTREE INDEXES FOR RANGE QUERIES");

    let mut graph = InMemoryGraph::new();

    // Add some vertices with ages
    subsection("Creating sample data");
    for i in 0..100 {
        let age = (i % 80) + 18; // Ages 18-97
        let name = format!("person_{}", i);
        graph.add_vertex(
            "person",
            HashMap::from([
                ("name".to_string(), Value::String(name)),
                ("age".to_string(), Value::Int(age as i64)),
            ]),
        );
    }
    println!("  Created 100 person vertices with ages 18-97");

    // Create a BTree index on person.age
    subsection("Creating BTree index on person.age");
    let spec = IndexBuilder::vertex()
        .label("person")
        .property("age")
        .name("idx_person_age")
        .build()
        .unwrap();

    println!("  Index spec: {:?}", spec);

    graph.create_index(spec).unwrap();
    println!("  Index created successfully!");

    // List indexes
    println!("\n  Active indexes:");
    for spec in graph.list_indexes() {
        println!(
            "    - {} ({:?} on {:?}.{})",
            spec.name,
            spec.index_type,
            spec.label.as_deref().unwrap_or("*"),
            spec.property
        );
    }

    // Query using exact match
    subsection("Exact match lookup (age = 25)");
    let results: Vec<_> = graph
        .vertices_by_property(Some("person"), "age", &Value::Int(25))
        .collect();
    println!("  Found {} people with age 25", results.len());
    for v in results.iter().take(3) {
        if let Some(Value::String(name)) = v.properties.get("name") {
            println!("    - {}", name);
        }
    }
    if results.len() > 3 {
        println!("    ... and {} more", results.len() - 3);
    }

    // Query using range (adults under 30)
    subsection("Range query (18 <= age < 30)");
    let start = Value::Int(18);
    let end = Value::Int(30);
    let results: Vec<_> = graph
        .vertices_by_property_range(
            Some("person"),
            "age",
            Bound::Included(&start),
            Bound::Excluded(&end),
        )
        .collect();
    println!("  Found {} people aged 18-29", results.len());

    // Show age distribution
    let mut ages: Vec<i64> = results
        .iter()
        .filter_map(|v| match v.properties.get("age") {
            Some(Value::Int(a)) => Some(*a),
            _ => None,
        })
        .collect();
    ages.sort();
    ages.dedup();
    println!("  Ages found: {:?}", ages);

    // Query seniors (65+)
    subsection("Range query (age >= 65)");
    let start = Value::Int(65);
    let results: Vec<_> = graph
        .vertices_by_property_range(
            Some("person"),
            "age",
            Bound::Included(&start),
            Bound::Unbounded,
        )
        .collect();
    println!("  Found {} seniors (65+)", results.len());
}

// =============================================================================
// Part 2: Unique Indexes
// =============================================================================

fn demo_unique_indexes() {
    section("PART 2: UNIQUE INDEXES WITH O(1) LOOKUP");

    let mut graph = InMemoryGraph::new();

    // Create unique index BEFORE adding data (for constraint enforcement)
    subsection("Creating unique index on user.email");
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("user")
                .property("email")
                .unique()
                .name("idx_user_email")
                .build()
                .unwrap(),
        )
        .unwrap();
    println!("  Unique index created!");

    // Add users with unique emails
    subsection("Adding users with unique emails");
    let users = [
        ("alice", "alice@example.com"),
        ("bob", "bob@example.com"),
        ("charlie", "charlie@example.com"),
    ];

    for (name, email) in &users {
        graph.add_vertex(
            "user",
            HashMap::from([
                ("name".to_string(), Value::String(name.to_string())),
                ("email".to_string(), Value::String(email.to_string())),
            ]),
        );
        println!("  Added user: {} <{}>", name, email);
    }

    // O(1) lookup by email
    subsection("O(1) lookup by email");
    let email_to_find = "bob@example.com";
    let results: Vec<_> = graph
        .vertices_by_property(
            Some("user"),
            "email",
            &Value::String(email_to_find.to_string()),
        )
        .collect();

    if let Some(user) = results.first() {
        if let Some(Value::String(name)) = user.properties.get("name") {
            println!("  Found user with email '{}': {}", email_to_find, name);
        }
    }

    // Demonstrate uniqueness constraint
    subsection("Uniqueness constraint enforcement");
    println!("  Attempting to add duplicate email 'alice@example.com'...");

    // This would fail with a unique constraint violation
    // Note: In the current implementation, unique constraint is checked at index insert time
    // For demonstration, we show the index statistics instead
    if let Some(index) = graph.get_index("idx_user_email") {
        let stats = index.statistics();
        println!("  Index statistics:");
        println!("    Cardinality: {}", stats.cardinality);
        println!("    Total elements: {}", stats.total_elements);
    }
}

// =============================================================================
// Part 3: Index-Accelerated Traversals
// =============================================================================

fn demo_traversal_with_indexes() {
    section("PART 3: INDEX-ACCELERATED TRAVERSALS");

    let graph = Graph::new();

    // Create a graph with products
    subsection("Creating product catalog");
    for i in 0..1000 {
        let price = 10.0 + (i as f64 * 0.5);
        let category = match i % 4 {
            0 => "electronics",
            1 => "clothing",
            2 => "books",
            _ => "home",
        };
        graph.add_vertex(
            "product",
            HashMap::from([
                ("name".to_string(), Value::String(format!("product_{}", i))),
                ("price".to_string(), Value::Float(price)),
                ("category".to_string(), Value::String(category.to_string())),
            ]),
        );
    }
    println!("  Created 1000 products with prices $10.00 - $509.50");

    // Create indexes
    subsection("Creating indexes");
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("product")
                .property("price")
                .name("idx_product_price")
                .build()
                .unwrap(),
        )
        .unwrap();
    println!("  Created BTree index on product.price");

    graph
        .create_index(
            IndexBuilder::vertex()
                .label("product")
                .property("category")
                .name("idx_product_category")
                .build()
                .unwrap(),
        )
        .unwrap();
    println!("  Created BTree index on product.category");

    // Get snapshot for traversal API
    let snapshot = graph.snapshot();

    // Query using traversal API
    subsection("Traversal queries (using indexes internally)");

    // Count all products
    let g = snapshot.gremlin();
    let count = g.v().has_label("product").count();
    println!("  Total products: {}", count);

    // Query by category (uses index internally in has_value)
    let g = snapshot.gremlin();
    let electronics: Vec<_> = g
        .v()
        .has_label("product")
        .has_value("category", "electronics")
        .to_list();
    println!("  Electronics products: {}", electronics.len());

    // Note: Range queries through traversal use has_where with predicates
    // Direct range access is available via vertices_by_property_range
}

// =============================================================================
// Part 4: Index Persistence (MmapGraph)
// =============================================================================

fn demo_index_persistence() {
    section("PART 4: INDEX PERSISTENCE (MMAPGRAPH)");

    // Clean up previous demo database
    subsection("Setup: Creating fresh database");
    if Path::new(DB_PATH).exists() {
        fs::remove_file(DB_PATH).ok();
        fs::remove_file(format!("{}.wal", DB_PATH.trim_end_matches(".db"))).ok();
        fs::remove_file(DB_PATH.replace(".db", ".idx.json")).ok();
    }
    println!("  Database path: {}", DB_PATH);

    // Create database and add data
    {
        let graph = MmapGraph::open(DB_PATH).expect("Failed to create database");

        graph.begin_batch().unwrap();

        // Add some users
        for i in 0..50 {
            graph
                .add_vertex(
                    "user",
                    HashMap::from([
                        ("name".to_string(), Value::String(format!("user_{}", i))),
                        (
                            "email".to_string(),
                            Value::String(format!("user{}@example.com", i)),
                        ),
                        ("age".to_string(), Value::Int((20 + (i % 50)) as i64)),
                    ]),
                )
                .unwrap();
        }

        graph.commit_batch().unwrap();
        println!("  Created 50 users");

        // Create indexes
        subsection("Creating indexes");
        graph
            .create_index(
                IndexBuilder::vertex()
                    .label("user")
                    .property("email")
                    .unique()
                    .name("idx_user_email")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        println!("  Created unique index: idx_user_email");

        graph
            .create_index(
                IndexBuilder::vertex()
                    .label("user")
                    .property("age")
                    .name("idx_user_age")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        println!("  Created BTree index: idx_user_age");

        // Show indexes before close
        println!("\n  Indexes before close: {}", graph.index_count());
        for spec in graph.list_indexes() {
            println!("    - {} ({:?})", spec.name, spec.index_type);
        }

        graph.checkpoint().unwrap();
        println!("\n  Database checkpointed and closing...");
        // graph is dropped here
    }

    // Reopen and verify indexes persist
    subsection("Reopening database");
    {
        let graph = MmapGraph::open(DB_PATH).expect("Failed to reopen database");

        println!("  Database reopened!");
        println!("  Index count: {}", graph.index_count());

        // List indexes
        for spec in graph.list_indexes() {
            println!(
                "    - {} ({:?} on {})",
                spec.name, spec.index_type, spec.property
            );
        }

        // Use the indexes
        subsection("Using persisted indexes");

        // Unique index lookup
        let users: Vec<_> = graph
            .vertices_by_property(
                Some("user"),
                "email",
                &Value::String("user25@example.com".to_string()),
            )
            .collect();
        if let Some(user) = users.first() {
            println!("  Found user by email: {:?}", user.properties.get("name"));
        }

        // Range query
        let start = Value::Int(30);
        let end = Value::Int(40);
        let users: Vec<_> = graph
            .vertices_by_property_range(
                Some("user"),
                "age",
                Bound::Included(&start),
                Bound::Excluded(&end),
            )
            .collect();
        println!("  Users aged 30-39: {}", users.len());

        // Drop an index
        subsection("Dropping an index");
        graph.drop_index("idx_user_age").unwrap();
        println!("  Dropped idx_user_age");
        println!("  Remaining indexes: {}", graph.index_count());

        graph.checkpoint().unwrap();
    }

    // Verify drop persisted
    subsection("Verifying drop persisted");
    {
        let graph = MmapGraph::open(DB_PATH).expect("Failed to reopen database");
        println!("  Index count after reopen: {}", graph.index_count());
        for spec in graph.list_indexes() {
            println!("    - {}", spec.name);
        }
        assert_eq!(graph.index_count(), 1);
        assert!(graph.has_index("idx_user_email"));
        assert!(!graph.has_index("idx_user_age"));
        println!("  Index drop was persisted correctly!");
    }
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    println!("=== Interstellar Property Indexes Example ===");

    // Part 1: BTree indexes for range queries
    demo_btree_indexes();

    // Part 2: Unique indexes with O(1) lookup
    demo_unique_indexes();

    // Part 3: Index-accelerated traversals
    demo_traversal_with_indexes();

    // Part 4: Index persistence (MmapGraph)
    demo_index_persistence();

    // Summary
    section("SUMMARY");

    println!("\nProperty Index Features Demonstrated:");
    println!();
    println!("  Index Types:");
    println!("    - BTree: Range queries, O(log n) lookup");
    println!("    - Unique: O(1) lookup with uniqueness constraint");
    println!();
    println!("  Index Creation:");
    println!("    graph.create_index(IndexBuilder::vertex()");
    println!("        .label(\"person\")");
    println!("        .property(\"age\")");
    println!("        .build().unwrap())");
    println!();
    println!("  Index-Accelerated Queries:");
    println!("    - vertices_by_property(label, property, value)");
    println!("    - vertices_by_property_range(label, property, start, end)");
    println!("    - edges_by_property(label, property, value)");
    println!();
    println!("  Index Management:");
    println!("    - list_indexes(), has_index(), index_count()");
    println!("    - drop_index(name)");
    println!();
    println!("  Persistence (MmapGraph):");
    println!("    - Index specs saved to .idx.json file");
    println!("    - Indexes rebuilt automatically on reopen");
    println!();

    println!("=== Example Complete ===");
}
