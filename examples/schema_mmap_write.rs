//! Schema + MmapGraph Write Example
//!
//! This example demonstrates creating a persistent graph database with a schema.
//! It defines a schema with strict validation, then inserts valid data into the
//! memory-mapped database.
//!
//! Features demonstrated:
//! - Creating a schema with SchemaBuilder
//! - Vertex type definitions with required and optional properties
//! - Edge type definitions with FROM/TO constraints
//! - Opening/creating an MmapGraph database
//! - Saving the schema to the database file using save_schema()
//! - Batch mode for efficient bulk loading
//! - Schema-validated inserts using GQL mutations
//!
//! Run with: `cargo run --features mmap --example schema_mmap_write`
//!
//! After running this example, run `schema_mmap_validate` to see schema validation in action.

use intersteller::gql::{
    execute_mutation_with_schema, parse_statement, CompileError, MutationError,
};
use intersteller::schema::{GraphSchema, PropertyType, SchemaBuilder, ValidationMode};
use intersteller::storage::mmap::MmapGraph;
use intersteller::value::Value;
use std::fs;
use std::path::Path;

const DB_PATH: &str = "examples/data/schema_graph.db";

/// Helper function to execute a GQL mutation with schema validation.
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

fn main() {
    println!("=== Schema + MmapGraph Write Example ===\n");

    // =========================================================================
    // Step 1: Define the Schema
    // =========================================================================
    println!("Step 1: Defining the schema...\n");

    let schema = SchemaBuilder::new()
        .mode(ValidationMode::Strict)
        // Person vertex type
        .vertex("Person")
        .property("name", PropertyType::String) // Required
        .optional("age", PropertyType::Int)
        .optional("email", PropertyType::String)
        .done()
        // Company vertex type
        .vertex("Company")
        .property("name", PropertyType::String) // Required
        .optional("founded", PropertyType::Int)
        .optional("industry", PropertyType::String)
        .done()
        // Project vertex type
        .vertex("Project")
        .property("name", PropertyType::String) // Required
        .property("status", PropertyType::String) // Required
        .optional("budget", PropertyType::Float)
        .done()
        // KNOWS edge: Person -> Person
        .edge("KNOWS")
        .from(&["Person"])
        .to(&["Person"])
        .optional("since", PropertyType::Int)
        .optional("relationship", PropertyType::String)
        .done()
        // WORKS_AT edge: Person -> Company (requires role)
        .edge("WORKS_AT")
        .from(&["Person"])
        .to(&["Company"])
        .property("role", PropertyType::String) // Required
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
        .build();

    println!("Schema defined with ValidationMode::Strict");
    println!(
        "  Vertex types: {:?}",
        schema.vertex_labels().collect::<Vec<_>>()
    );
    println!(
        "  Edge types: {:?}",
        schema.edge_labels().collect::<Vec<_>>()
    );
    println!();

    // Display schema details
    println!("Schema Details:");
    if let Some(person) = schema.vertex_schema("Person") {
        println!("  Person:");
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
        println!("  WORKS_AT edge:");
        println!("    From: {:?}", works_at.from_labels);
        println!("    To: {:?}", works_at.to_labels);
        println!(
            "    Required props: {:?}",
            works_at.required_properties().collect::<Vec<_>>()
        );
    }
    println!();

    // =========================================================================
    // Step 2: Create/Open the MmapGraph Database
    // =========================================================================
    println!("Step 2: Opening MmapGraph database at {}...\n", DB_PATH);

    // Delete existing database to start fresh
    if Path::new(DB_PATH).exists() {
        println!("  Removing existing database files...");
        let _ = fs::remove_file(DB_PATH);
        let _ = fs::remove_file(format!("{}.wal", DB_PATH.trim_end_matches(".db")));
    }

    let mut storage = MmapGraph::open(DB_PATH).expect("Failed to open/create MmapGraph database");

    println!("  Database created successfully!\n");

    // =========================================================================
    // Step 3: Save the Schema to the Database
    // =========================================================================
    println!("Step 3: Saving schema to the database...\n");

    storage
        .save_schema(&schema)
        .expect("Failed to save schema to database");
    println!("  Schema saved to database file\n");

    // Verify it was saved
    if let Ok(Some(loaded)) = storage.load_schema() {
        println!(
            "  Verified: Schema loaded back with {} vertex types, {} edge types\n",
            loaded.vertex_labels().count(),
            loaded.edge_labels().count()
        );
    }

    // =========================================================================
    // Step 4: Begin Batch Mode and Insert Data
    // =========================================================================
    println!("Step 4: Beginning batch mode and inserting data...\n");

    storage.begin_batch().expect("Failed to begin batch mode");
    println!("  Batch mode enabled\n");

    // Insert People
    println!("  Inserting People vertices...");

    let queries = [
        "CREATE (p:Person {name: 'Alice', age: 30, email: 'alice@example.com'})",
        "CREATE (p:Person {name: 'Bob', age: 28})",
        "CREATE (p:Person {name: 'Charlie', age: 35, email: 'charlie@example.com'})",
        "CREATE (p:Person {name: 'Diana', age: 32})",
        "CREATE (p:Person {name: 'Eve', age: 27, email: 'eve@example.com'})",
    ];

    for query in &queries {
        match execute_with_schema(&mut storage, query, &schema) {
            Ok(_) => println!("    [OK] {}", query),
            Err(e) => println!("    [ERROR] {}: {}", query, e),
        }
    }
    println!();

    // Insert Companies
    println!("  Inserting Company vertices...");

    let queries = [
        "CREATE (c:Company {name: 'TechCorp', founded: 2010, industry: 'Technology'})",
        "CREATE (c:Company {name: 'DataInc', founded: 2015, industry: 'Data Analytics'})",
        "CREATE (c:Company {name: 'CloudSoft', founded: 2018})",
    ];

    for query in &queries {
        match execute_with_schema(&mut storage, query, &schema) {
            Ok(_) => println!("    [OK] {}", query),
            Err(e) => println!("    [ERROR] {}: {}", query, e),
        }
    }
    println!();

    // Insert Projects
    println!("  Inserting Project vertices...");

    let queries = [
        "CREATE (p:Project {name: 'Alpha', status: 'active', budget: 100000.0})",
        "CREATE (p:Project {name: 'Beta', status: 'planning', budget: 50000.0})",
        "CREATE (p:Project {name: 'Gamma', status: 'completed'})",
    ];

    for query in &queries {
        match execute_with_schema(&mut storage, query, &schema) {
            Ok(_) => println!("    [OK] {}", query),
            Err(e) => println!("    [ERROR] {}: {}", query, e),
        }
    }
    println!();

    // Insert KNOWS edges (Person -> Person)
    println!("  Inserting KNOWS edges...");

    let queries = [
        "CREATE (a:Person {name: 'Alice2'})-[:KNOWS {since: 2020, relationship: 'friend'}]->(b:Person {name: 'Bob2'})",
        "CREATE (c:Person {name: 'Charlie2'})-[:KNOWS {since: 2019}]->(d:Person {name: 'Diana2'})",
    ];

    for query in &queries {
        match execute_with_schema(&mut storage, query, &schema) {
            Ok(_) => println!("    [OK] Created KNOWS relationship"),
            Err(e) => println!("    [ERROR] {}", e),
        }
    }
    println!();

    // Insert WORKS_AT edges (Person -> Company with required 'role')
    println!("  Inserting WORKS_AT edges...");

    let queries = [
        "CREATE (p:Person {name: 'Frank'})-[:WORKS_AT {role: 'Engineer', start_year: 2021}]->(c:Company {name: 'TechCorp2'})",
        "CREATE (p:Person {name: 'Grace'})-[:WORKS_AT {role: 'Manager'}]->(c:Company {name: 'DataInc2'})",
    ];

    for query in &queries {
        match execute_with_schema(&mut storage, query, &schema) {
            Ok(_) => println!("    [OK] Created WORKS_AT relationship"),
            Err(e) => println!("    [ERROR] {}", e),
        }
    }
    println!();

    // Insert WORKS_ON edges (Person -> Project)
    println!("  Inserting WORKS_ON edges...");

    let query = "CREATE (p:Person {name: 'Henry'})-[:WORKS_ON {hours_per_week: 20}]->(proj:Project {name: 'Delta', status: 'active'})";
    match execute_with_schema(&mut storage, query, &schema) {
        Ok(_) => println!("    [OK] Created WORKS_ON relationship"),
        Err(e) => println!("    [ERROR] {}", e),
    }
    println!();

    // Insert OWNS edges (Company -> Project)
    println!("  Inserting OWNS edges...");

    let query = "CREATE (c:Company {name: 'MegaCorp'})-[:OWNS]->(p:Project {name: 'Epsilon', status: 'planning'})";
    match execute_with_schema(&mut storage, query, &schema) {
        Ok(_) => println!("    [OK] Created OWNS relationship"),
        Err(e) => println!("    [ERROR] {}", e),
    }
    println!();

    // =========================================================================
    // Step 5: Commit and Checkpoint
    // =========================================================================
    println!("Step 5: Committing batch and creating checkpoint...\n");

    storage.commit_batch().expect("Failed to commit batch");
    println!("  Batch committed successfully!");

    storage.checkpoint().expect("Failed to create checkpoint");
    println!("  Checkpoint created - data is now durable!\n");

    // =========================================================================
    // Summary
    // =========================================================================
    println!("=== Write Complete ===\n");
    println!("Database written to: {}", DB_PATH);
    println!("Schema stored in: database file (no separate .schema file needed)");

    // Get file size
    if let Ok(metadata) = fs::metadata(DB_PATH) {
        println!("Database size: {:.2} KB", metadata.len() as f64 / 1024.0);
    }

    println!("\nSchema constraints:");
    println!("  - Person: requires 'name' (string)");
    println!("  - Company: requires 'name' (string)");
    println!("  - Project: requires 'name' (string) and 'status' (string)");
    println!("  - KNOWS: Person -> Person only");
    println!("  - WORKS_AT: Person -> Company, requires 'role' (string)");
    println!("  - WORKS_ON: Person -> Project only");
    println!("  - OWNS: Company -> Project only");

    println!("\nNext step: Run the validation example to see schema enforcement:");
    println!("  cargo run --features mmap --example schema_mmap_validate");
}
