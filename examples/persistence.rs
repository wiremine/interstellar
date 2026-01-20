//! Persistent Graph Database Example (Memory-Mapped Storage)
//!
//! This example demonstrates Interstellar's persistent storage capabilities using
//! memory-mapped files. It covers the full lifecycle of a persistent graph:
//!
//! **Part 1: Creating and Writing a Persistent Graph**
//! - Opening/creating an MmapGraph database
//! - Schema definition with SchemaBuilder
//! - Schema persistence (save_schema/load_schema)
//! - Batch mode for efficient bulk loading
//! - Checkpointing for durability
//!
//! **Part 2: Reading and Querying a Persistent Graph**
//! - Opening an existing database
//! - Loading schema from database
//! - Running traversal queries
//! - Running GQL queries
//!
//! **Part 3: Schema Validation**
//! - Demonstrating schema enforcement
//! - Missing required properties
//! - Wrong property types
//! - Invalid edge endpoints
//! - Validation modes comparison
//!
//! Run: `cargo run --features mmap --example persistence`

use interstellar::gql::{
    execute_mutation_with_schema, parse_statement, CompileError, MutationError,
};
use interstellar::graph::LegacyGraph;
use interstellar::schema::{GraphSchema, PropertyType, SchemaBuilder, ValidationMode};
use interstellar::storage::mmap::MmapGraph;
use interstellar::value::Value;
use std::fs;
use std::path::Path;
use std::sync::Arc;

const DB_PATH: &str = "examples/data/persistence_demo.db";

// =============================================================================
// Helper Functions
// =============================================================================

fn execute_with_schema(
    storage: &mut MmapGraph,
    query: &str,
    schema: &GraphSchema,
) -> Result<Vec<Value>, MutationError> {
    let stmt = parse_statement(query).map_err(|e| {
        MutationError::Compile(CompileError::UnsupportedFeature(format!(
            "Parse error: {}",
            e
        )))
    })?;
    execute_mutation_with_schema(&stmt, storage, Some(schema))
}

fn section(title: &str) {
    println!("\n{}", "=".repeat(70));
    println!("{}", title);
    println!("{}", "=".repeat(70));
}

// =============================================================================
// Part 1: Creating and Writing a Persistent Graph
// =============================================================================

fn create_schema() -> GraphSchema {
    SchemaBuilder::new()
        .mode(ValidationMode::Strict)
        // Person vertex type
        .vertex("Person")
        .property("name", PropertyType::String)
        .optional("age", PropertyType::Int)
        .optional("email", PropertyType::String)
        .done()
        // Company vertex type
        .vertex("Company")
        .property("name", PropertyType::String)
        .optional("founded", PropertyType::Int)
        .optional("industry", PropertyType::String)
        .done()
        // Project vertex type
        .vertex("Project")
        .property("name", PropertyType::String)
        .property("status", PropertyType::String)
        .optional("budget", PropertyType::Float)
        .done()
        // KNOWS edge: Person -> Person
        .edge("KNOWS")
        .from(&["Person"])
        .to(&["Person"])
        .optional("since", PropertyType::Int)
        .done()
        // WORKS_AT edge: Person -> Company (requires role)
        .edge("WORKS_AT")
        .from(&["Person"])
        .to(&["Company"])
        .property("role", PropertyType::String)
        .optional("start_year", PropertyType::Int)
        .done()
        // WORKS_ON edge: Person -> Project
        .edge("WORKS_ON")
        .from(&["Person"])
        .to(&["Project"])
        .optional("hours_per_week", PropertyType::Int)
        .done()
        // OWNS edge: Company -> Project
        .edge("OWNS")
        .from(&["Company"])
        .to(&["Project"])
        .done()
        .build()
}

fn demo_write_graph() -> GraphSchema {
    section("PART 1: CREATING A PERSISTENT GRAPH");

    // Step 1: Define schema
    println!("\n--- Step 1: Define Schema ---");
    let schema = create_schema();

    println!("Schema defined with ValidationMode::Strict");
    println!(
        "  Vertex types: {:?}",
        schema.vertex_labels().collect::<Vec<_>>()
    );
    println!(
        "  Edge types: {:?}",
        schema.edge_labels().collect::<Vec<_>>()
    );

    if let Some(person) = schema.vertex_schema("Person") {
        println!("\n  Person type:");
        println!(
            "    Required: {:?}",
            person.required_properties().collect::<Vec<_>>()
        );
        println!(
            "    Optional: {:?}",
            person.optional_properties().collect::<Vec<_>>()
        );
    }

    if let Some(works_at) = schema.edge_schema("WORKS_AT") {
        println!("\n  WORKS_AT edge:");
        println!(
            "    From: {:?} -> To: {:?}",
            works_at.from_labels, works_at.to_labels
        );
        println!(
            "    Required: {:?}",
            works_at.required_properties().collect::<Vec<_>>()
        );
    }

    // Step 2: Create/Open database
    println!("\n--- Step 2: Create Database ---");
    println!("Database path: {}", DB_PATH);

    // Delete existing database to start fresh
    if Path::new(DB_PATH).exists() {
        println!("  Removing existing database...");
        let _ = fs::remove_file(DB_PATH);
        let _ = fs::remove_file(format!("{}.wal", DB_PATH.trim_end_matches(".db")));
    }

    let mut storage = MmapGraph::open(DB_PATH).expect("Failed to create database");
    println!("  Database created successfully!");

    // Step 3: Save schema to database
    println!("\n--- Step 3: Save Schema to Database ---");
    storage.save_schema(&schema).expect("Failed to save schema");
    println!("  Schema saved to database file");

    // Verify schema was saved
    if let Ok(Some(loaded)) = storage.load_schema() {
        println!(
            "  Verified: {} vertex types, {} edge types loaded back",
            loaded.vertex_labels().count(),
            loaded.edge_labels().count()
        );
    }

    // Step 4: Insert data with batch mode
    println!("\n--- Step 4: Insert Data (Batch Mode) ---");
    storage.begin_batch().expect("Failed to begin batch");
    println!("  Batch mode enabled for efficient bulk loading");

    // Insert People
    println!("\n  Inserting Person vertices...");
    let people = [
        "CREATE (p:Person {name: 'Alice', age: 30, email: 'alice@example.com'})",
        "CREATE (p:Person {name: 'Bob', age: 28})",
        "CREATE (p:Person {name: 'Charlie', age: 35, email: 'charlie@example.com'})",
        "CREATE (p:Person {name: 'Diana', age: 32})",
    ];
    for query in &people {
        match execute_with_schema(&mut storage, query, &schema) {
            Ok(_) => println!(
                "    [ok] {}",
                query.chars().skip(8).take(40).collect::<String>()
            ),
            Err(e) => println!("    [err] {}", e),
        }
    }

    // Insert Companies
    println!("\n  Inserting Company vertices...");
    let companies = [
        "CREATE (c:Company {name: 'TechCorp', founded: 2010, industry: 'Technology'})",
        "CREATE (c:Company {name: 'DataInc', founded: 2015, industry: 'Analytics'})",
    ];
    for query in &companies {
        match execute_with_schema(&mut storage, query, &schema) {
            Ok(_) => println!(
                "    [ok] {}",
                query.chars().skip(8).take(50).collect::<String>()
            ),
            Err(e) => println!("    [err] {}", e),
        }
    }

    // Insert Projects
    println!("\n  Inserting Project vertices...");
    let projects = [
        "CREATE (p:Project {name: 'Alpha', status: 'active', budget: 100000.0})",
        "CREATE (p:Project {name: 'Beta', status: 'planning', budget: 50000.0})",
    ];
    for query in &projects {
        match execute_with_schema(&mut storage, query, &schema) {
            Ok(_) => println!(
                "    [ok] {}",
                query.chars().skip(8).take(50).collect::<String>()
            ),
            Err(e) => println!("    [err] {}", e),
        }
    }

    // Insert edges
    println!("\n  Inserting edges...");

    let edges = [
        "CREATE (a:Person {name: 'Eve'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Frank'})",
        "CREATE (p:Person {name: 'Grace'})-[:WORKS_AT {role: 'Engineer', start_year: 2021}]->(c:Company {name: 'CloudSoft'})",
        "CREATE (p:Person {name: 'Henry'})-[:WORKS_ON {hours_per_week: 20}]->(proj:Project {name: 'Gamma', status: 'active'})",
        "CREATE (c:Company {name: 'MegaCorp'})-[:OWNS]->(p:Project {name: 'Delta', status: 'planning'})",
    ];
    for query in &edges {
        match execute_with_schema(&mut storage, query, &schema) {
            Ok(_) => println!("    [ok] Created edge"),
            Err(e) => println!("    [err] {}", e),
        }
    }

    // Step 5: Commit and checkpoint
    println!("\n--- Step 5: Commit and Checkpoint ---");
    storage.commit_batch().expect("Failed to commit batch");
    println!("  Batch committed");

    storage.checkpoint().expect("Failed to checkpoint");
    println!("  Checkpoint created - data is now durable!");

    // Show database size
    if let Ok(metadata) = fs::metadata(DB_PATH) {
        println!(
            "\n  Database size: {:.2} KB",
            metadata.len() as f64 / 1024.0
        );
    }

    schema
}

// =============================================================================
// Part 2: Reading and Querying
// =============================================================================

fn demo_read_graph() {
    section("PART 2: READING A PERSISTENT GRAPH");

    println!("\n--- Step 1: Open Existing Database ---");
    let storage = Arc::new(MmapGraph::open(DB_PATH).expect("Failed to open database"));
    println!("  Database opened: {}", DB_PATH);

    // Load schema
    println!("\n--- Step 2: Load Schema from Database ---");
    let schema = storage
        .load_schema()
        .expect("Failed to load schema")
        .expect("No schema found");

    println!("  Schema loaded:");
    println!("    Mode: {:?}", schema.mode);
    println!(
        "    Vertex types: {:?}",
        schema.vertex_labels().collect::<Vec<_>>()
    );
    println!(
        "    Edge types: {:?}",
        schema.edge_labels().collect::<Vec<_>>()
    );

    // Create LegacyGraph wrapper for querying
    let graph = LegacyGraph::from_arc(storage.clone());
    let snapshot = graph.snapshot();

    // Query with Fluent API
    println!("\n--- Step 3: Query with Fluent API ---");

    let g = snapshot.traversal();
    let person_count = g.v().has_label("Person").count();
    let company_count = g.v().has_label("Company").count();
    let project_count = g.v().has_label("Project").count();
    let edge_count = g.e().count();

    println!("  Database contents:");
    println!("    Persons: {}", person_count);
    println!("    Companies: {}", company_count);
    println!("    Projects: {}", project_count);
    println!("    Edges: {}", edge_count);

    // List all people
    let g = snapshot.traversal();
    let names: Vec<String> = g
        .v()
        .has_label("Person")
        .values("name")
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    println!("\n  All people: {:?}", names);

    // Query with GQL
    println!("\n--- Step 4: Query with GQL ---");

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.name, p.age")
        .unwrap();
    println!("  GQL: MATCH (p:Person) RETURN p.name, p.age");
    for result in results.iter().take(3) {
        println!("    {:?}", result);
    }
    if results.len() > 3 {
        println!("    ... ({} total)", results.len());
    }

    // Query edges
    let results = snapshot
        .gql("MATCH (p:Person)-[r:WORKS_AT]->(c:Company) RETURN p.name, r.role, c.name")
        .unwrap();
    println!("\n  GQL: MATCH (p)-[:WORKS_AT]->(c) RETURN p.name, r.role, c.name");
    for result in &results {
        println!("    {:?}", result);
    }
}

// =============================================================================
// Part 3: Schema Validation
// =============================================================================

fn demo_validation(schema: &GraphSchema) {
    section("PART 3: SCHEMA VALIDATION");

    let mut storage = MmapGraph::open(DB_PATH).expect("Failed to open database");

    // Test 1: Missing required property
    println!("\n--- Test 1: Missing Required Property ---");
    println!("  Query: CREATE (p:Person {{age: 25}})  -- missing 'name'");
    let result = execute_with_schema(&mut storage, "CREATE (p:Person {age: 25})", schema);
    match result {
        Ok(_) => println!("  [unexpected] Mutation succeeded"),
        Err(e) => println!("  [rejected] {}", e),
    }

    // Test 2: Wrong property type
    println!("\n--- Test 2: Wrong Property Type ---");
    println!("  Query: CREATE (p:Person {{name: 'X', age: 'thirty'}})  -- age should be int");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {name: 'Test', age: 'thirty'})",
        schema,
    );
    match result {
        Ok(_) => println!("  [unexpected] Mutation succeeded"),
        Err(e) => println!("  [rejected] {}", e),
    }

    // Test 3: Invalid edge endpoint
    println!("\n--- Test 3: Invalid Edge Endpoint ---");
    println!("  Query: CREATE (p:Person)-[:KNOWS]->(c:Company)  -- KNOWS is Person->Person");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {name: 'A'})-[:KNOWS]->(c:Company {name: 'B'})",
        schema,
    );
    match result {
        Ok(_) => println!("  [unexpected] Mutation succeeded"),
        Err(e) => println!("  [rejected] {}", e),
    }

    // Test 4: Missing required edge property
    println!("\n--- Test 4: Missing Required Edge Property ---");
    println!("  Query: CREATE (p:Person)-[:WORKS_AT]->(c:Company)  -- missing 'role'");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {name: 'X'})-[:WORKS_AT]->(c:Company {name: 'Y'})",
        schema,
    );
    match result {
        Ok(_) => println!("  [unexpected] Mutation succeeded"),
        Err(e) => println!("  [rejected] {}", e),
    }

    // Test 5: Valid mutation
    println!("\n--- Test 5: Valid Mutation ---");
    println!("  Query: CREATE (p:Person {{name: 'ValidPerson', age: 40}})");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {name: 'ValidPerson', age: 40})",
        schema,
    );
    match result {
        Ok(_) => println!("  [accepted] Person created successfully"),
        Err(e) => println!("  [unexpected] {}", e),
    }

    // Validation modes comparison
    println!("\n--- Validation Modes Comparison ---");

    // Mode: None
    let schema_none = SchemaBuilder::new()
        .mode(ValidationMode::None)
        .vertex("Person")
        .property("name", PropertyType::String)
        .done()
        .build();

    println!("\n  ValidationMode::None (no enforcement):");
    let result = execute_with_schema(&mut storage, "CREATE (p:Person {age: 25})", &schema_none);
    match result {
        Ok(_) => println!("    Missing 'name': [allowed]"),
        Err(_) => println!("    Missing 'name': [rejected]"),
    }

    // Mode: Closed
    let schema_closed = SchemaBuilder::new()
        .mode(ValidationMode::Closed)
        .vertex("Person")
        .property("name", PropertyType::String)
        .done()
        .build();

    println!("\n  ValidationMode::Closed (all labels must have schemas):");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (r:Robot {model: 'R2D2'})",
        &schema_closed,
    );
    match result {
        Ok(_) => println!("    Unknown label 'Robot': [allowed]"),
        Err(_) => println!("    Unknown label 'Robot': [rejected]"),
    }

    // Mode: Strict (original)
    println!("\n  ValidationMode::Strict (unknown labels allowed, violations rejected):");
    let result = execute_with_schema(&mut storage, "CREATE (r:Robot {model: 'R2D2'})", schema);
    match result {
        Ok(_) => println!("    Unknown label 'Robot': [allowed]"),
        Err(_) => println!("    Unknown label 'Robot': [rejected]"),
    }
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    println!("=== Interstellar Persistent Storage Example ===\n");

    // Part 1: Create and write a persistent graph
    let schema = demo_write_graph();

    // Part 2: Read and query the persistent graph
    demo_read_graph();

    // Part 3: Schema validation demonstration
    demo_validation(&schema);

    // Summary
    section("SUMMARY");

    println!("\nPersistent Storage Features Demonstrated:");
    println!();
    println!("  Database Operations:");
    println!("    - MmapGraph::open(path)     -- Create or open database");
    println!("    - begin_batch() / commit_batch() -- Batch mode for bulk ops");
    println!("    - checkpoint()              -- Ensure durability");
    println!();
    println!("  Schema Persistence:");
    println!("    - save_schema(&schema)      -- Save schema to database");
    println!("    - load_schema()             -- Load schema from database");
    println!();
    println!("  Schema Validation:");
    println!("    - Required/optional properties");
    println!("    - Property type checking (String, Int, Float, Bool)");
    println!("    - Edge endpoint constraints (FROM/TO)");
    println!("    - Validation modes: None, Strict, Closed");
    println!();
    println!("  Query APIs:");
    println!("    - Fluent API: g.v().has_label(\"Person\").values(\"name\")");
    println!("    - GQL: MATCH (p:Person) RETURN p.name");
    println!();

    // Show final database info
    if let Ok(metadata) = fs::metadata(DB_PATH) {
        println!(
            "Database: {} ({:.2} KB)",
            DB_PATH,
            metadata.len() as f64 / 1024.0
        );
    }

    println!("\n=== Example Complete ===");
}
