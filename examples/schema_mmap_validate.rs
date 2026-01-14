//! Schema Validation Demo
//!
//! This example demonstrates schema validation by opening a persistent graph
//! database and showing how invalid vertices and edges are rejected.
//!
//! Features demonstrated:
//! - Loading a schema from file
//! - Opening an existing MmapGraph database
//! - Attempting invalid mutations that violate schema constraints:
//!   - Missing required properties
//!   - Wrong property types
//!   - Invalid edge endpoints (wrong source/target vertex types)
//!   - Unknown vertex/edge labels (in Closed mode)
//!   - Additional properties not in schema
//!
//! Run first: `cargo run --features mmap --example schema_mmap_write`
//! Then run:  `cargo run --features mmap --example schema_mmap_validate`

use intersteller::gql::{
    execute_mutation_with_schema, parse_statement, CompileError, MutationError,
};
use intersteller::graph::Graph;
use intersteller::schema::{
    deserialize_schema, GraphSchema, PropertyType, SchemaBuilder, ValidationMode,
};
use intersteller::storage::mmap::MmapGraph;
use intersteller::value::Value;
use std::fs;
use std::sync::Arc;

const DB_PATH: &str = "examples/data/schema_graph.db";
const SCHEMA_PATH: &str = "examples/data/schema_graph.schema";

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

fn print_section(title: &str) {
    println!("\n{}", "=".repeat(70));
    println!("{}", title);
    println!("{}", "=".repeat(70));
}

fn print_test(description: &str) {
    println!("\n--- {} ---", description);
}

fn main() {
    println!("=== Schema + MmapGraph Read Example - Validation Demo ===\n");

    // =========================================================================
    // Step 1: Load the Schema
    // =========================================================================
    println!("Step 1: Loading schema from {}...\n", SCHEMA_PATH);

    let schema_bytes = match fs::read(SCHEMA_PATH) {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("Error: Failed to read schema file: {}", e);
            eprintln!("\nMake sure you've run the write example first:");
            eprintln!("  cargo run --features mmap --example schema_mmap_write");
            std::process::exit(1);
        }
    };

    let schema = match deserialize_schema(&schema_bytes) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error: Failed to deserialize schema: {}", e);
            std::process::exit(1);
        }
    };

    println!("Schema loaded successfully!");
    println!("  Mode: {:?}", schema.mode);
    println!(
        "  Vertex types: {:?}",
        schema.vertex_labels().collect::<Vec<_>>()
    );
    println!(
        "  Edge types: {:?}",
        schema.edge_labels().collect::<Vec<_>>()
    );

    // =========================================================================
    // Step 2: Open the Database
    // =========================================================================
    println!("\nStep 2: Opening database from {}...\n", DB_PATH);

    let storage = match MmapGraph::open(DB_PATH) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            eprintln!("Error: Failed to open database: {}", e);
            eprintln!("\nMake sure you've run the write example first:");
            eprintln!("  cargo run --features mmap --example schema_mmap_write");
            std::process::exit(1);
        }
    };

    // Display current data
    let graph = Graph::from_arc(storage.clone());
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    let person_count = g.v().has_label("Person").count();
    let company_count = g.v().has_label("Company").count();
    let project_count = g.v().has_label("Project").count();
    let edge_count = g.e().count();

    println!("Database opened successfully!");
    println!("  Persons: {}", person_count);
    println!("  Companies: {}", company_count);
    println!("  Projects: {}", project_count);
    println!("  Total edges: {}", edge_count);

    // Drop the snapshot to allow mutations
    drop(g);
    drop(snapshot);
    drop(graph);
    drop(storage);

    // Reopen for mutations
    let mut storage = MmapGraph::open(DB_PATH).expect("Failed to reopen database");

    // =========================================================================
    // SECTION 1: Missing Required Properties
    // =========================================================================
    print_section("1. MISSING REQUIRED PROPERTIES");

    print_test("Person without required 'name' property");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {age: 25})", // Missing 'name'
        &schema,
    );
    match result {
        Ok(_) => println!("  UNEXPECTED: Mutation succeeded"),
        Err(e) => println!("  REJECTED: {}", e),
    }

    print_test("Company without required 'name' property");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (c:Company {founded: 2020})", // Missing 'name'
        &schema,
    );
    match result {
        Ok(_) => println!("  UNEXPECTED: Mutation succeeded"),
        Err(e) => println!("  REJECTED: {}", e),
    }

    print_test("Project without required 'status' property");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Project {name: 'TestProject'})", // Missing 'status'
        &schema,
    );
    match result {
        Ok(_) => println!("  UNEXPECTED: Mutation succeeded"),
        Err(e) => println!("  REJECTED: {}", e),
    }

    print_test("WORKS_AT edge without required 'role' property");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {name: 'Test'})-[:WORKS_AT]->(c:Company {name: 'TestCorp'})", // Missing 'role'
        &schema,
    );
    match result {
        Ok(_) => println!("  UNEXPECTED: Mutation succeeded"),
        Err(e) => println!("  REJECTED: {}", e),
    }

    // =========================================================================
    // SECTION 2: Wrong Property Types
    // =========================================================================
    print_section("2. WRONG PROPERTY TYPES");

    print_test("Person with wrong type for 'age' (string instead of int)");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {name: 'TypeTest', age: 'thirty'})", // 'age' should be int
        &schema,
    );
    match result {
        Ok(_) => println!("  UNEXPECTED: Mutation succeeded"),
        Err(e) => println!("  REJECTED: {}", e),
    }

    print_test("Company with wrong type for 'founded' (string instead of int)");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (c:Company {name: 'TypeTest', founded: 'two thousand'})", // 'founded' should be int
        &schema,
    );
    match result {
        Ok(_) => println!("  UNEXPECTED: Mutation succeeded"),
        Err(e) => println!("  REJECTED: {}", e),
    }

    print_test("Project with wrong type for 'budget' (string instead of float)");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Project {name: 'TypeTest', status: 'active', budget: 'lots of money'})",
        &schema,
    );
    match result {
        Ok(_) => println!("  UNEXPECTED: Mutation succeeded"),
        Err(e) => println!("  REJECTED: {}", e),
    }

    print_test("KNOWS edge with wrong type for 'since' (string instead of int)");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (a:Person {name: 'A'})-[:KNOWS {since: 'long ago'}]->(b:Person {name: 'B'})",
        &schema,
    );
    match result {
        Ok(_) => println!("  UNEXPECTED: Mutation succeeded"),
        Err(e) => println!("  REJECTED: {}", e),
    }

    // =========================================================================
    // SECTION 3: Invalid Edge Endpoints
    // =========================================================================
    print_section("3. INVALID EDGE ENDPOINTS");

    print_test("KNOWS edge from Person to Company (should be Person -> Person)");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {name: 'PersonA'})-[:KNOWS]->(c:Company {name: 'CompanyX'})",
        &schema,
    );
    match result {
        Ok(_) => println!("  UNEXPECTED: Mutation succeeded"),
        Err(e) => println!("  REJECTED: {}", e),
    }

    print_test("WORKS_AT edge from Company to Person (should be Person -> Company)");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (c:Company {name: 'CompanyY'})-[:WORKS_AT {role: 'Test'}]->(p:Person {name: 'PersonB'})",
        &schema,
    );
    match result {
        Ok(_) => println!("  UNEXPECTED: Mutation succeeded"),
        Err(e) => println!("  REJECTED: {}", e),
    }

    print_test("WORKS_ON edge from Company to Project (should be Person -> Project)");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (c:Company {name: 'CompanyZ'})-[:WORKS_ON]->(p:Project {name: 'ProjX', status: 'active'})",
        &schema,
    );
    match result {
        Ok(_) => println!("  UNEXPECTED: Mutation succeeded"),
        Err(e) => println!("  REJECTED: {}", e),
    }

    print_test("OWNS edge from Person to Project (should be Company -> Project)");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {name: 'PersonC'})-[:OWNS]->(proj:Project {name: 'ProjY', status: 'planning'})",
        &schema,
    );
    match result {
        Ok(_) => println!("  UNEXPECTED: Mutation succeeded"),
        Err(e) => println!("  REJECTED: {}", e),
    }

    print_test("OWNS edge from Company to Person (should be Company -> Project)");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (c:Company {name: 'CompanyW'})-[:OWNS]->(p:Person {name: 'PersonD'})",
        &schema,
    );
    match result {
        Ok(_) => println!("  UNEXPECTED: Mutation succeeded"),
        Err(e) => println!("  REJECTED: {}", e),
    }

    // =========================================================================
    // SECTION 4: Valid Mutations (Positive Tests)
    // =========================================================================
    print_section("4. VALID MUTATIONS (Positive Tests)");

    print_test("Valid Person vertex with all properties");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {name: 'ValidPerson', age: 40, email: 'valid@example.com'})",
        &schema,
    );
    match result {
        Ok(_) => println!("  ACCEPTED: Person created successfully"),
        Err(e) => println!("  UNEXPECTED ERROR: {}", e),
    }

    print_test("Valid Person vertex with only required property");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {name: 'MinimalPerson'})",
        &schema,
    );
    match result {
        Ok(_) => println!("  ACCEPTED: Person created with minimal properties"),
        Err(e) => println!("  UNEXPECTED ERROR: {}", e),
    }

    print_test("Valid KNOWS edge (Person -> Person)");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (a:Person {name: 'Friend1'})-[:KNOWS {since: 2023}]->(b:Person {name: 'Friend2'})",
        &schema,
    );
    match result {
        Ok(_) => println!("  ACCEPTED: KNOWS edge created successfully"),
        Err(e) => println!("  UNEXPECTED ERROR: {}", e),
    }

    print_test("Valid WORKS_AT edge with required 'role'");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {name: 'Employee1'})-[:WORKS_AT {role: 'Developer', start_year: 2022}]->(c:Company {name: 'ValidCorp'})",
        &schema,
    );
    match result {
        Ok(_) => println!("  ACCEPTED: WORKS_AT edge created successfully"),
        Err(e) => println!("  UNEXPECTED ERROR: {}", e),
    }

    print_test("Valid OWNS edge (Company -> Project)");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (c:Company {name: 'OwnerCorp'})-[:OWNS]->(p:Project {name: 'OwnedProject', status: 'active'})",
        &schema,
    );
    match result {
        Ok(_) => println!("  ACCEPTED: OWNS edge created successfully"),
        Err(e) => println!("  UNEXPECTED ERROR: {}", e),
    }

    // =========================================================================
    // SECTION 5: Validation Mode Comparison
    // =========================================================================
    print_section("5. VALIDATION MODE COMPARISON");

    println!("\nThe schema was saved with ValidationMode::Strict.");
    println!("Let's compare behavior with different validation modes:\n");

    // Test with ValidationMode::None (allows everything)
    let schema_none = SchemaBuilder::new()
        .mode(ValidationMode::None)
        .vertex("Person")
        .property("name", PropertyType::String)
        .done()
        .build();

    print_test("ValidationMode::None - Missing required 'name'");
    let result = execute_with_schema(&mut storage, "CREATE (p:Person {age: 99})", &schema_none);
    match result {
        Ok(_) => println!("  ALLOWED (None mode ignores schema violations)"),
        Err(e) => println!("  Error: {}", e),
    }

    // Test with ValidationMode::Closed (rejects unknown labels)
    let schema_closed = SchemaBuilder::new()
        .mode(ValidationMode::Closed)
        .vertex("Person")
        .property("name", PropertyType::String)
        .done()
        .build();

    print_test("ValidationMode::Closed - Unknown vertex label 'Robot'");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (r:Robot {model: 'R2D2'})",
        &schema_closed,
    );
    match result {
        Ok(_) => println!("  UNEXPECTED: Mutation succeeded"),
        Err(e) => println!("  REJECTED: {}", e),
    }

    // Test with original Strict mode - unknown labels allowed
    print_test("ValidationMode::Strict - Unknown vertex label 'Robot'");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (r:Robot {model: 'R2D2'})",
        &schema, // Original schema with Strict mode
    );
    match result {
        Ok(_) => println!("  ALLOWED (Strict mode allows unknown labels)"),
        Err(e) => println!("  REJECTED: {}", e),
    }

    // =========================================================================
    // SECTION 6: Additional Properties
    // =========================================================================
    print_section("6. ADDITIONAL (UNEXPECTED) PROPERTIES");

    print_test("Person with extra property 'phone' (not in schema)");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {name: 'ExtraPropPerson', phone: '555-1234'})",
        &schema,
    );
    match result {
        Ok(_) => println!("  UNEXPECTED: Mutation succeeded"),
        Err(e) => println!("  REJECTED: {}", e),
    }

    // Create schema that allows additional properties
    let schema_with_extra = SchemaBuilder::new()
        .mode(ValidationMode::Strict)
        .vertex("Person")
        .property("name", PropertyType::String)
        .allow_additional() // This allows extra properties
        .done()
        .build();

    print_test("Person with extra property (schema allows additional)");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {name: 'FlexiblePerson', nickname: 'Flex', phone: '555-5678'})",
        &schema_with_extra,
    );
    match result {
        Ok(_) => println!("  ALLOWED (schema has ALLOW ADDITIONAL PROPERTIES)"),
        Err(e) => println!("  REJECTED: {}", e),
    }

    // =========================================================================
    // Summary
    // =========================================================================
    print_section("SUMMARY");

    println!("\nSchema validation successfully demonstrated:");
    println!();
    println!("  REJECTED (Invalid mutations):");
    println!("    - Missing required properties on vertices");
    println!("    - Missing required properties on edges");
    println!("    - Wrong property types (string vs int, etc.)");
    println!("    - Invalid edge endpoints (wrong source/target vertex types)");
    println!("    - Additional properties not in schema (in Strict mode)");
    println!("    - Unknown labels (in Closed mode)");
    println!();
    println!("  ACCEPTED (Valid mutations):");
    println!("    - Vertices with all required properties");
    println!("    - Vertices with only required properties (optional omitted)");
    println!("    - Edges with correct endpoint types");
    println!("    - Edges with required properties");
    println!("    - Unknown labels (in Strict mode - allowed)");
    println!("    - Additional properties (when schema allows it)");
    println!();
    println!("=== Validation Demo Complete ===");
}
