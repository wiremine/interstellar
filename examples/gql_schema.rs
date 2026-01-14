//! GQL Schema and DDL Example
//!
//! This example demonstrates how to use GQL Data Definition Language (DDL) to define
//! graph schemas and validate mutations. Key features covered:
//!
//! - DDL statements: CREATE NODE TYPE, CREATE EDGE TYPE, ALTER TYPE, DROP TYPE
//! - Schema validation modes: None, Warn, Strict, Closed
//! - Property constraints: NOT NULL, DEFAULT values, type checking
//! - Edge endpoint validation: FROM/TO type constraints
//! - Using the SchemaBuilder API as an alternative to DDL
//!
//! Run with: `cargo run --example gql_schema`

use intersteller::gql::{
    execute_mutation_with_schema, parse_statement, CompileError, MutationError,
};
use intersteller::prelude::*;
use intersteller::schema::{PropertyType, SchemaBuilder, ValidationMode};
use intersteller::storage::InMemoryGraph;

/// Helper function to execute a GQL mutation with optional schema validation.
fn execute_with_schema(
    storage: &mut InMemoryGraph,
    query: &str,
    schema: Option<&intersteller::schema::GraphSchema>,
) -> Result<Vec<Value>, MutationError> {
    let stmt = parse_statement(query).map_err(|e| {
        MutationError::Compile(CompileError::UnsupportedFeature(format!(
            "Parse error: {}",
            e
        )))
    })?;
    execute_mutation_with_schema(&stmt, storage, schema)
}

fn main() {
    println!("=== Intersteller GQL Schema and DDL Example ===\n");

    // =========================================================================
    // Part 1: Building Schemas with the Rust API (SchemaBuilder)
    // =========================================================================
    println!("=== Part 1: Building Schemas with SchemaBuilder ===\n");

    // Build a schema using the fluent SchemaBuilder API
    let schema = SchemaBuilder::new()
        .mode(ValidationMode::Strict)
        // Define a Person vertex type
        .vertex("Person")
        .property("name", PropertyType::String) // Required: NOT NULL
        .optional("age", PropertyType::Int) // Optional
        .optional_with_default("active", PropertyType::Bool, Value::Bool(true))
        .done()
        // Define a Company vertex type
        .vertex("Company")
        .property("name", PropertyType::String)
        .optional("founded", PropertyType::Int)
        .done()
        // Define a KNOWS edge type (Person -> Person)
        .edge("KNOWS")
        .from(&["Person"])
        .to(&["Person"])
        .optional("since", PropertyType::Int)
        .optional_with_default("weight", PropertyType::Float, Value::Float(1.0))
        .done()
        // Define a WORKS_AT edge type (Person -> Company)
        .edge("WORKS_AT")
        .from(&["Person"])
        .to(&["Company"])
        .property("role", PropertyType::String) // Required
        .optional("start_date", PropertyType::Int)
        .done()
        .build();

    println!("Created schema with SchemaBuilder:");
    println!(
        "  Vertex types: {:?}",
        schema.vertex_labels().collect::<Vec<_>>()
    );
    println!(
        "  Edge types: {:?}",
        schema.edge_labels().collect::<Vec<_>>()
    );
    println!("  Validation mode: {:?}", schema.mode);
    println!();

    // Examine the Person schema
    if let Some(person_schema) = schema.vertex_schema("Person") {
        println!("Person schema:");
        println!(
            "  Required properties: {:?}",
            person_schema.required_properties().collect::<Vec<_>>()
        );
        println!(
            "  Optional properties: {:?}",
            person_schema.optional_properties().collect::<Vec<_>>()
        );
        println!(
            "  Allows additional properties: {}",
            person_schema.additional_properties
        );
    }
    println!();

    // =========================================================================
    // Part 2: Schema Validation in Action
    // =========================================================================
    println!("=== Part 2: Schema Validation in Action ===\n");

    let mut storage = InMemoryGraph::new();

    // Valid mutation: Person with required 'name' property
    println!("Test 1: Creating a valid Person vertex");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {name: 'Alice', age: 30})",
        Some(&schema),
    );
    match result {
        Ok(_) => println!("  ✓ Success: Created Alice"),
        Err(e) => println!("  ✗ Error: {}", e),
    }
    println!();

    // Invalid mutation: Person missing required 'name' property
    println!("Test 2: Creating a Person without required 'name' property");
    let result = execute_with_schema(&mut storage, "CREATE (p:Person {age: 25})", Some(&schema));
    match result {
        Ok(_) => println!("  ✗ Unexpected success"),
        Err(e) => println!("  ✓ Correctly rejected: {}", e),
    }
    println!();

    // Invalid mutation: Wrong type for 'age' property
    println!("Test 3: Creating a Person with wrong type for 'age'");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {name: 'Bob', age: 'twenty-five'})",
        Some(&schema),
    );
    match result {
        Ok(_) => println!("  ✗ Unexpected success"),
        Err(e) => println!("  ✓ Correctly rejected: {}", e),
    }
    println!();

    // Valid mutation: Company vertex
    println!("Test 4: Creating a valid Company vertex");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (c:Company {name: 'Acme Corp', founded: 2010})",
        Some(&schema),
    );
    match result {
        Ok(_) => println!("  ✓ Success: Created Acme Corp"),
        Err(e) => println!("  ✗ Error: {}", e),
    }
    println!();

    // =========================================================================
    // Part 3: Edge Endpoint Validation
    // =========================================================================
    println!("=== Part 3: Edge Endpoint Validation ===\n");

    // Create edges by specifying full patterns with new vertices
    // Note: GQL CREATE with edges requires creating the nodes in the same pattern

    // Valid edge: KNOWS from Person to Person (creates both nodes and edge)
    println!("Test 5: Creating valid KNOWS edge (Person -> Person)");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (a:Person {name: 'Charlie'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Diana'})",
        Some(&schema),
    );
    match result {
        Ok(_) => println!("  ✓ Success: Created Charlie -[:KNOWS]-> Diana"),
        Err(e) => println!("  ✗ Error: {}", e),
    }
    println!();

    // Valid edge: WORKS_AT from Person to Company with required 'role'
    println!("Test 6: Creating valid WORKS_AT edge (Person -> Company)");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {name: 'Eve'})-[:WORKS_AT {role: 'Engineer', start_date: 2022}]->(c:Company {name: 'TechCorp'})",
        Some(&schema),
    );
    match result {
        Ok(_) => println!("  ✓ Success: Created Eve -[:WORKS_AT]-> TechCorp"),
        Err(e) => println!("  ✗ Error: {}", e),
    }
    println!();

    // Invalid edge: WORKS_AT missing required 'role' property
    println!("Test 7: Creating WORKS_AT edge without required 'role'");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {name: 'Frank'})-[:WORKS_AT]->(c:Company {name: 'NoCorp'})",
        Some(&schema),
    );
    match result {
        Ok(_) => println!("  ✗ Unexpected success"),
        Err(e) => println!("  ✓ Correctly rejected: {}", e),
    }
    println!();

    // Invalid edge: WORKS_AT from Company to Person (wrong source type)
    println!("Test 8: Creating WORKS_AT edge with wrong endpoint types");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (c:Company {name: 'BadCorp'})-[:WORKS_AT {role: 'Employee'}]->(p:Person {name: 'Greg'})",
        Some(&schema),
    );
    match result {
        Ok(_) => println!("  ✗ Unexpected success"),
        Err(e) => println!("  ✓ Correctly rejected: {}", e),
    }
    println!();

    // =========================================================================
    // Part 4: Validation Modes
    // =========================================================================
    println!("=== Part 4: Validation Modes ===\n");

    // Mode: None - no validation (schema is documentation only)
    let schema_none = SchemaBuilder::new()
        .mode(ValidationMode::None)
        .vertex("Person")
        .property("name", PropertyType::String)
        .done()
        .build();

    let mut storage_none = InMemoryGraph::new();
    println!("ValidationMode::None - Schema violations are allowed");
    let result = execute_with_schema(
        &mut storage_none,
        "CREATE (p:Person {age: 25})", // Missing required 'name'
        Some(&schema_none),
    );
    match result {
        Ok(_) => println!("  ✓ Allowed (as expected in None mode)"),
        Err(e) => println!("  ✗ Error: {}", e),
    }
    println!();

    // Mode: Strict - unknown labels allowed, schema violations rejected
    let schema_strict = SchemaBuilder::new()
        .mode(ValidationMode::Strict)
        .vertex("Person")
        .property("name", PropertyType::String)
        .done()
        .build();

    let mut storage_strict = InMemoryGraph::new();
    println!("ValidationMode::Strict - Unknown labels allowed, violations rejected");

    // Unknown label is allowed in Strict mode
    let result = execute_with_schema(
        &mut storage_strict,
        "CREATE (r:Robot {model: 'R2D2'})", // Unknown 'Robot' label
        Some(&schema_strict),
    );
    match result {
        Ok(_) => println!("  ✓ Unknown label 'Robot' allowed in Strict mode"),
        Err(e) => println!("  ✗ Error: {}", e),
    }

    // Schema violation is rejected in Strict mode
    let result = execute_with_schema(
        &mut storage_strict,
        "CREATE (p:Person {age: 25})", // Missing required 'name'
        Some(&schema_strict),
    );
    match result {
        Ok(_) => println!("  ✗ Unexpected success"),
        Err(e) => println!("  ✓ Schema violation rejected: {}", e),
    }
    println!();

    // Mode: Closed - all labels must have schemas defined
    let schema_closed = SchemaBuilder::new()
        .mode(ValidationMode::Closed)
        .vertex("Person")
        .property("name", PropertyType::String)
        .done()
        .build();

    let mut storage_closed = InMemoryGraph::new();
    println!("ValidationMode::Closed - All labels must have schemas");

    // Unknown label is rejected in Closed mode
    let result = execute_with_schema(
        &mut storage_closed,
        "CREATE (r:Robot {model: 'R2D2'})", // Unknown 'Robot' label
        Some(&schema_closed),
    );
    match result {
        Ok(_) => println!("  ✗ Unexpected success"),
        Err(e) => println!("  ✓ Unknown label rejected: {}", e),
    }
    println!();

    // =========================================================================
    // Part 5: Additional Properties
    // =========================================================================
    println!("=== Part 5: Additional Properties ===\n");

    // By default, additional properties not in schema are rejected
    let schema_no_extra = SchemaBuilder::new()
        .mode(ValidationMode::Strict)
        .vertex("Person")
        .property("name", PropertyType::String)
        .done()
        .build();

    let mut storage_extra = InMemoryGraph::new();
    println!("Without ALLOW ADDITIONAL PROPERTIES:");
    let result = execute_with_schema(
        &mut storage_extra,
        "CREATE (p:Person {name: 'Alice', email: 'alice@example.com'})", // 'email' not in schema
        Some(&schema_no_extra),
    );
    match result {
        Ok(_) => println!("  ✗ Unexpected success"),
        Err(e) => println!("  ✓ Extra property rejected: {}", e),
    }

    // With allow_additional(), extra properties are permitted
    let schema_with_extra = SchemaBuilder::new()
        .mode(ValidationMode::Strict)
        .vertex("Person")
        .property("name", PropertyType::String)
        .allow_additional() // Allow properties not in schema
        .done()
        .build();

    let mut storage_with_extra = InMemoryGraph::new();
    println!("\nWith ALLOW ADDITIONAL PROPERTIES:");
    let result = execute_with_schema(
        &mut storage_with_extra,
        "CREATE (p:Person {name: 'Alice', email: 'alice@example.com'})", // 'email' not in schema
        Some(&schema_with_extra),
    );
    match result {
        Ok(_) => println!("  ✓ Extra property allowed"),
        Err(e) => println!("  ✗ Error: {}", e),
    }
    println!();

    // =========================================================================
    // Part 6: Using the Graph API with Schema
    // =========================================================================
    println!("=== Part 6: Using the Graph API with Schema ===\n");

    // Create a graph with schema using the high-level API
    let storage = InMemoryGraph::new();
    let graph = Graph::with_schema(storage, schema.clone());

    // Access the schema from the graph
    if let Some(schema) = graph.schema() {
        println!("Graph has schema attached via Graph::with_schema():");
        println!("  Mode: {:?}", schema.mode);
        println!(
            "  Vertex types: {:?}",
            schema.vertex_labels().collect::<Vec<_>>()
        );
    }

    // Alternative: Create graph then set schema
    let graph2 = Graph::in_memory();
    graph2.set_schema(Some(schema.clone()));
    println!("\nSchema can also be set via Graph::set_schema()");

    // Access schema from snapshot
    let snapshot = graph.snapshot();
    if let Some(_schema) = snapshot.schema() {
        println!("Schema accessible from GraphSnapshot::schema()");
    }

    println!("\nGraph API methods for schema management:");
    println!("  - Graph::with_schema(storage, schema) - Create with schema");
    println!("  - Graph::in_memory_with_schema(schema) - Convenience constructor");
    println!("  - Graph::set_schema(Some(schema)) - Set or replace schema");
    println!("  - Graph::schema() -> Option<GraphSchema> - Get current schema");
    println!("  - GraphMut::ddl(query) - Execute DDL statements");
    println!("  - GraphMut::gql(query, storage) - Execute mutations with validation");
    println!();

    // =========================================================================
    // Part 7: DDL Execution via Graph API
    // =========================================================================
    println!("=== Part 7: DDL Execution via Graph API ===\n");

    // The Graph API provides ddl() method on GraphMut for schema modifications
    let graph = Graph::in_memory();

    println!("Creating schema via DDL statements:");

    {
        let mut_handle = graph.mutate();

        // Create node types using DDL
        mut_handle
            .ddl("CREATE NODE TYPE Employee (name STRING NOT NULL, email STRING)")
            .unwrap();
        println!("  Created Employee node type");

        mut_handle
            .ddl("CREATE NODE TYPE Department (name STRING NOT NULL, budget INT)")
            .unwrap();
        println!("  Created Department node type");

        // Create an edge type
        mut_handle
            .ddl("CREATE EDGE TYPE BELONGS_TO (since INT) FROM Employee TO Department")
            .unwrap();
        println!("  Created BELONGS_TO edge type");

        // Alter a type to add properties
        mut_handle
            .ddl("ALTER NODE TYPE Employee ADD hire_date INT")
            .unwrap();
        println!("  Added hire_date property to Employee");

        // Allow additional properties on a type
        mut_handle
            .ddl("ALTER NODE TYPE Employee ALLOW ADDITIONAL PROPERTIES")
            .unwrap();
        println!("  Employee now allows additional properties");

        // Set validation mode
        mut_handle.ddl("SET SCHEMA VALIDATION STRICT").unwrap();
        println!("  Set validation mode to STRICT");
    }

    // Verify the schema
    let schema = graph.schema().expect("Schema should be set");
    println!("\nFinal schema via DDL:");
    println!(
        "  Vertex types: {:?}",
        schema.vertex_labels().collect::<Vec<_>>()
    );
    println!(
        "  Edge types: {:?}",
        schema.edge_labels().collect::<Vec<_>>()
    );
    println!("  Validation mode: {:?}", schema.mode);

    if let Some(employee) = schema.vertex_schema("Employee") {
        println!("\n  Employee type:");
        println!(
            "    Properties: {:?}",
            employee.properties.keys().collect::<Vec<_>>()
        );
        println!("    Allows additional: {}", employee.additional_properties);
    }

    if let Some(belongs_to) = schema.edge_schema("BELONGS_TO") {
        println!("\n  BELONGS_TO edge type:");
        println!("    From: {:?}", belongs_to.from_labels);
        println!("    To: {:?}", belongs_to.to_labels);
    }
    println!();

    // =========================================================================
    // Part 8: Property Types
    // =========================================================================
    println!("=== Part 8: Property Types ===\n");

    let type_schema = SchemaBuilder::new()
        .mode(ValidationMode::Strict)
        .vertex("TypeDemo")
        .property("string_prop", PropertyType::String)
        .property("int_prop", PropertyType::Int)
        .property("float_prop", PropertyType::Float)
        .property("bool_prop", PropertyType::Bool)
        .optional(
            "list_prop",
            PropertyType::List(Some(Box::new(PropertyType::String))),
        )
        .optional("map_prop", PropertyType::Map(None))
        .optional("any_prop", PropertyType::Any)
        .done()
        .build();

    println!("Supported property types:");
    println!("  STRING   - Text values");
    println!("  INT      - 64-bit integers");
    println!("  FLOAT    - 64-bit floating point");
    println!("  BOOL     - Boolean true/false");
    println!("  LIST     - Arrays (optionally typed: LIST<STRING>)");
    println!("  MAP      - Key-value maps (optionally typed: MAP<INT>)");
    println!("  ANY      - Accepts any value type");
    println!();

    if let Some(demo) = type_schema.vertex_schema("TypeDemo") {
        println!("TypeDemo vertex properties:");
        for (name, def) in &demo.properties {
            println!(
                "  {} - {} {}",
                name,
                def.value_type,
                if def.required { "(required)" } else { "" }
            );
        }
    }
    println!();

    // =========================================================================
    // Summary
    // =========================================================================
    println!("=== Summary ===\n");
    println!("Key concepts demonstrated:");
    println!("  1. SchemaBuilder API for defining schemas programmatically");
    println!("  2. Property constraints: required (NOT NULL), optional, defaults");
    println!("  3. Edge endpoint validation: FROM/TO type constraints");
    println!("  4. Validation modes: None, Warn, Strict, Closed");
    println!("  5. Additional properties: allow_additional() for flexibility");
    println!("  6. Graph API integration: Graph::with_schema(), graph.schema()");
    println!("  7. DDL execution: GraphMut::ddl() for schema modifications");
    println!("  8. Property types: STRING, INT, FLOAT, BOOL, LIST, MAP, ANY");
    println!();
    println!("DDL statement syntax examples:");
    println!("  CREATE NODE TYPE Person (name STRING NOT NULL, age INT)");
    println!("  CREATE EDGE TYPE KNOWS (since INT) FROM Person TO Person");
    println!("  ALTER NODE TYPE Person ADD email STRING");
    println!("  ALTER NODE TYPE Person ALLOW ADDITIONAL PROPERTIES");
    println!("  DROP NODE TYPE OldType");
    println!("  SET SCHEMA VALIDATION STRICT");
    println!();
    println!("=== Example Complete ===");
}
