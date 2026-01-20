//! GQL (Graph Query Language) Comprehensive Example
//!
//! This example demonstrates the full range of GQL capabilities in Interstellar:
//!
//! **Part 1: Basic Queries and Advanced Features**
//! - Inline WHERE in patterns: `(n:Person WHERE n.age > 21)`
//! - Query Parameters: `$paramName`
//! - LET Clause: `LET count = COUNT(x)`
//! - List Comprehensions: `[x IN list | x.name]`
//! - String Concatenation: `'a' || 'b'`
//! - Map Literals: `{name: n.name, age: n.age}`
//!
//! **Part 2: Mutations**
//! - CREATE: Adding vertices and edges
//! - SET: Updating properties
//! - REMOVE: Removing properties
//! - DELETE / DETACH DELETE: Removing elements
//! - MERGE: Upsert operations
//!
//! **Part 3: Schema and DDL**
//! - SchemaBuilder API for programmatic schema definition
//! - DDL statements: CREATE NODE TYPE, CREATE EDGE TYPE, ALTER TYPE
//! - Validation modes: None, Warn, Strict, Closed
//! - Property constraints and type checking
//!
//! Run: `cargo run --example gql`

use interstellar::gql::{
    execute_mutation, execute_mutation_with_schema, parse_statement, CompileError, MutationError,
};
use interstellar::prelude::*;
use interstellar::schema::{PropertyType, SchemaBuilder, ValidationMode};
use interstellar::storage::{Graph, GraphStorage, InMemoryGraph};
use std::collections::HashMap;

// =============================================================================
// Helper Functions
// =============================================================================

fn execute(storage: &mut InMemoryGraph, query: &str) -> Result<Vec<Value>, MutationError> {
    let stmt = parse_statement(query).map_err(|e| {
        MutationError::Compile(CompileError::UnsupportedFeature(format!(
            "Parse error: {}",
            e
        )))
    })?;
    execute_mutation(&stmt, storage)
}

fn execute_with_schema(
    storage: &mut InMemoryGraph,
    query: &str,
    schema: Option<&interstellar::schema::GraphSchema>,
) -> Result<Vec<Value>, MutationError> {
    let stmt = parse_statement(query).map_err(|e| {
        MutationError::Compile(CompileError::UnsupportedFeature(format!(
            "Parse error: {}",
            e
        )))
    })?;
    execute_mutation_with_schema(&stmt, storage, schema)
}

// =============================================================================
// Part 1: Advanced GQL Queries
// =============================================================================

/// Build a family tree graph for demonstrating advanced query features.
fn build_family_graph() -> Graph {
    let graph = Graph::new();

    // Create people using add_vertex
    let john = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("John Smith".to_string())),
            ("id".to_string(), Value::Int(1)),
        ]),
    );

    let mary = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Mary Smith".to_string())),
            ("id".to_string(), Value::Int(2)),
        ]),
    );

    let alice = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice Smith".to_string())),
            ("id".to_string(), Value::Int(4)),
        ]),
    );

    let bob = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Bob Smith".to_string())),
            ("id".to_string(), Value::Int(5)),
        ]),
    );

    // Create birth events
    let alice_birth = graph.add_vertex(
        "Birth",
        HashMap::from([("year".to_string(), Value::Int(1990))]),
    );

    let bob_birth = graph.add_vertex(
        "Birth",
        HashMap::from([("year".to_string(), Value::Int(1992))]),
    );

    // Connect people to birth events
    graph
        .add_edge(
            alice,
            alice_birth,
            "PARTICIPATED_IN",
            HashMap::from([("role".to_string(), Value::String("child".to_string()))]),
        )
        .unwrap();

    graph
        .add_edge(
            john,
            alice_birth,
            "PARTICIPATED_IN",
            HashMap::from([("role".to_string(), Value::String("parent".to_string()))]),
        )
        .unwrap();

    graph
        .add_edge(
            mary,
            alice_birth,
            "PARTICIPATED_IN",
            HashMap::from([("role".to_string(), Value::String("parent".to_string()))]),
        )
        .unwrap();

    graph
        .add_edge(
            bob,
            bob_birth,
            "PARTICIPATED_IN",
            HashMap::from([("role".to_string(), Value::String("child".to_string()))]),
        )
        .unwrap();

    graph
        .add_edge(
            john,
            bob_birth,
            "PARTICIPATED_IN",
            HashMap::from([("role".to_string(), Value::String("parent".to_string()))]),
        )
        .unwrap();

    graph
        .add_edge(
            mary,
            bob_birth,
            "PARTICIPATED_IN",
            HashMap::from([("role".to_string(), Value::String("parent".to_string()))]),
        )
        .unwrap();

    graph
}

fn demo_advanced_queries() {
    println!("============================================================");
    println!("PART 1: ADVANCED GQL QUERIES");
    println!("============================================================\n");

    let graph = build_family_graph();

    // With the unified API, all GQL queries go through graph.gql()
    // Reads and mutations are auto-detected

    // --- Inline WHERE ---
    println!("--- Inline WHERE in Node Patterns ---");
    let results = graph
        .gql("MATCH (p:Person WHERE p.id > 3) RETURN p.name AS name, p.id AS id")
        .unwrap();
    println!("People with id > 3:");
    for result in &results {
        println!("  {:?}", result);
    }
    println!();

    // --- Query Parameters ---
    println!("--- Query Parameters ---");
    let mut params = HashMap::new();
    params.insert("targetId".to_string(), Value::Int(4));
    let results = graph
        .gql_with_params(
            "MATCH (p:Person WHERE p.id = $targetId) RETURN p.name AS name",
            &params,
        )
        .unwrap();
    println!("Person with id = $targetId (4): {:?}", results);
    println!();

    // --- Map Literals ---
    println!("--- Map Literals ---");
    let results = graph
        .gql("MATCH (p:Person WHERE p.id = 4) RETURN {personName: p.name, personId: p.id} AS profile")
        .unwrap();
    println!("Person profile as map: {:?}", results);
    println!();

    // --- String Concatenation ---
    println!("--- String Concatenation ---");
    let results = graph
        .gql("MATCH (p:Person WHERE p.id <= 2) RETURN p.name || ' (ID: ' || p.id || ')' AS formatted")
        .unwrap();
    println!("Formatted names:");
    for result in &results {
        println!("  {:?}", result);
    }
    println!();

    // --- LET Clause ---
    println!("--- LET Clause ---");
    let results = graph
        .gql("MATCH (p:Person) LET personCount = COUNT(p) RETURN p.name AS name, personCount")
        .unwrap();
    println!("People with total count (first 3):");
    for result in results.iter().take(3) {
        println!("  {:?}", result);
    }
    println!();

    // --- List Comprehensions ---
    println!("--- List Comprehensions ---");
    let results = graph
        .gql(
            "MATCH (p:Person) \
             LET names = COLLECT(p.name) \
             LET upperNames = [n IN names | UPPER(n)] \
             RETURN upperNames",
        )
        .unwrap();
    println!("Uppercase names: {:?}", results.first());
    println!();

    // --- Inline WHERE on Edges ---
    println!("--- Inline WHERE on Edges ---");
    let results = graph
        .gql(
            "MATCH (p:Person)-[r:PARTICIPATED_IN WHERE r.role = 'child']->(e) \
             RETURN p.name AS person, labels(e) AS eventType",
        )
        .unwrap();
    println!("People as children in events:");
    for result in &results {
        println!("  {:?}", result);
    }
    println!();

    // --- Complex Query with Multiple Features ---
    println!("--- Combined Features: Find Family Relationships ---");
    let mut params = HashMap::new();
    params.insert("personId".to_string(), Value::Int(4)); // Alice

    let results = graph
        .gql_with_params(
            r#"
            MATCH (person:Person WHERE person.id = $personId)
                  -[r:PARTICIPATED_IN WHERE r.role = 'child']->(event)
                  <-[pr:PARTICIPATED_IN WHERE pr.role = 'parent']-(parent:Person)
            LET parentInfo = COLLECT({parentName: parent.name, eventLabels: labels(event)})
            LET summary = person.name || ' has ' || SIZE(parentInfo) || ' parent connection(s)'
            RETURN person.name AS person, summary
            "#,
            &params,
        )
        .unwrap();
    println!("Alice's family connections:");
    for result in &results {
        println!("  {:?}", result);
    }
    println!();
}

// =============================================================================
// Part 2: GQL Mutations
// =============================================================================

fn demo_mutations() {
    println!("============================================================");
    println!("PART 2: GQL MUTATIONS");
    println!("============================================================\n");

    let mut storage = InMemoryGraph::new();

    // --- CREATE ---
    println!("--- CREATE: Adding Vertices and Edges ---");

    execute(
        &mut storage,
        "CREATE (alice:Person {name: 'Alice', age: 30, city: 'New York'})",
    )
    .unwrap();
    execute(
        &mut storage,
        "CREATE (bob:Person {name: 'Bob', age: 25, city: 'Boston'})",
    )
    .unwrap();
    println!("Created Alice and Bob");

    let results = execute(
        &mut storage,
        "CREATE (carol:Person {name: 'Carol', age: 35}) RETURN carol.name",
    )
    .unwrap();
    println!("Created Carol with RETURN: {:?}", results);

    execute(
        &mut storage,
        "CREATE (x:Person {name: 'Dave'})-[:FOLLOWS]->(y:Person {name: 'Eve'})",
    )
    .unwrap();
    println!("Created edge: Dave -[:FOLLOWS]-> Eve");

    println!("  Vertex count: {}", storage.vertex_count());
    println!("  Edge count: {}", storage.edge_count());
    println!();

    // --- SET ---
    println!("--- SET: Updating Properties ---");

    execute(
        &mut storage,
        "MATCH (n:Person {name: 'Alice'}) SET n.age = 31",
    )
    .unwrap();
    println!("Updated Alice's age: SET n.age = 31");

    execute(
        &mut storage,
        "MATCH (n:Person {name: 'Alice'}) SET n.status = 'active', n.verified = true",
    )
    .unwrap();
    println!("Set multiple properties on Alice");

    execute(
        &mut storage,
        "MATCH (n:Person) WHERE n.age > 30 SET n.senior = true",
    )
    .unwrap();
    println!("Conditional update: WHERE n.age > 30 SET n.senior = true");
    println!();

    // --- REMOVE ---
    println!("--- REMOVE: Removing Properties ---");

    execute(
        &mut storage,
        "MATCH (n:Person {name: 'Bob'}) SET n.temporary = 'will be removed'",
    )
    .unwrap();
    execute(
        &mut storage,
        "MATCH (n:Person {name: 'Bob'}) REMOVE n.temporary",
    )
    .unwrap();
    println!("Added and removed temporary property from Bob");
    println!();

    // --- MERGE ---
    println!("--- MERGE: Upsert Operations ---");

    execute(
        &mut storage,
        "MERGE (n:Person {name: 'Henry'}) ON CREATE SET n.created = true, n.source = 'merge'",
    )
    .unwrap();
    println!("MERGE created new vertex Henry");

    let count_before = storage.vertex_count();
    execute(
        &mut storage,
        "MERGE (n:Person {name: 'Henry'}) ON MATCH SET n.updated = true, n.visits = 1",
    )
    .unwrap();
    assert_eq!(storage.vertex_count(), count_before);
    println!("MERGE matched existing Henry (no new vertex)");

    execute(
        &mut storage,
        r#"
        MERGE (n:Person {name: 'Ivy'}) 
        ON CREATE SET n.status = 'new'
        ON MATCH SET n.status = 'existing'
        "#,
    )
    .unwrap();
    println!("MERGE with both ON CREATE and ON MATCH");
    println!();

    // --- DELETE ---
    println!("--- DELETE and DETACH DELETE ---");

    execute(&mut storage, "CREATE (temp:Temporary {name: 'ToDelete'})").unwrap();
    let count_before = storage.vertex_count();
    execute(
        &mut storage,
        "MATCH (n:Temporary {name: 'ToDelete'}) DELETE n",
    )
    .unwrap();
    assert_eq!(storage.vertex_count(), count_before - 1);
    println!("DELETE removed isolated vertex");

    execute(
        &mut storage,
        "CREATE (hub:Hub {name: 'Central'})-[:CONNECTS]->(spoke:Spoke {name: 'Spoke1'})",
    )
    .unwrap();
    execute(
        &mut storage,
        "MATCH (n:Hub {name: 'Central'}) DETACH DELETE n",
    )
    .unwrap();
    println!("DETACH DELETE removed hub vertex and its edges");
    println!();

    println!(
        "Final state: {} vertices, {} edges",
        storage.vertex_count(),
        storage.edge_count()
    );
    println!();
}

// =============================================================================
// Part 3: Schema and DDL
// =============================================================================

fn demo_schema_builder() {
    println!("============================================================");
    println!("PART 3A: SCHEMA WITH SchemaBuilder API");
    println!("============================================================\n");

    // Build schema using fluent API
    let schema = SchemaBuilder::new()
        .mode(ValidationMode::Strict)
        .vertex("Person")
        .property("name", PropertyType::String)
        .optional("age", PropertyType::Int)
        .optional_with_default("active", PropertyType::Bool, Value::Bool(true))
        .done()
        .vertex("Company")
        .property("name", PropertyType::String)
        .optional("founded", PropertyType::Int)
        .done()
        .edge("KNOWS")
        .from(&["Person"])
        .to(&["Person"])
        .optional("since", PropertyType::Int)
        .done()
        .edge("WORKS_AT")
        .from(&["Person"])
        .to(&["Company"])
        .property("role", PropertyType::String)
        .done()
        .build();

    println!("Schema created with SchemaBuilder:");
    println!(
        "  Vertex types: {:?}",
        schema.vertex_labels().collect::<Vec<_>>()
    );
    println!(
        "  Edge types: {:?}",
        schema.edge_labels().collect::<Vec<_>>()
    );
    println!("  Mode: {:?}", schema.mode);
    println!();

    let mut storage = InMemoryGraph::new();

    // Valid mutation
    println!("Test: Creating valid Person vertex");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {name: 'Alice', age: 30})",
        Some(&schema),
    );
    match result {
        Ok(_) => println!("  [pass] Created Alice"),
        Err(e) => println!("  [fail] Error: {}", e),
    }

    // Missing required property
    println!("Test: Creating Person without required 'name'");
    let result = execute_with_schema(&mut storage, "CREATE (p:Person {age: 25})", Some(&schema));
    match result {
        Ok(_) => println!("  [fail] Unexpected success"),
        Err(e) => println!("  [pass] Correctly rejected: {}", e),
    }

    // Wrong type
    println!("Test: Creating Person with wrong type for 'age'");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {name: 'Bob', age: 'twenty'})",
        Some(&schema),
    );
    match result {
        Ok(_) => println!("  [fail] Unexpected success"),
        Err(e) => println!("  [pass] Correctly rejected: {}", e),
    }

    // Edge with wrong endpoint
    println!("Test: Creating WORKS_AT edge with wrong direction");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (c:Company {name: 'Corp'})-[:WORKS_AT {role: 'Employee'}]->(p:Person {name: 'Dan'})",
        Some(&schema),
    );
    match result {
        Ok(_) => println!("  [fail] Unexpected success"),
        Err(e) => println!("  [pass] Correctly rejected: {}", e),
    }
    println!();
}

fn demo_ddl() {
    println!("============================================================");
    println!("PART 3B: SCHEMA WITH DDL STATEMENTS");
    println!("============================================================\n");

    let graph = Graph::new();

    println!("Creating schema via DDL:");

    graph
        .ddl("CREATE NODE TYPE Employee (name STRING NOT NULL, email STRING)")
        .unwrap();
    println!("  CREATE NODE TYPE Employee (name STRING NOT NULL, email STRING)");

    graph
        .ddl("CREATE NODE TYPE Department (name STRING NOT NULL, budget INT)")
        .unwrap();
    println!("  CREATE NODE TYPE Department (name STRING NOT NULL, budget INT)");

    graph
        .ddl("CREATE EDGE TYPE BELONGS_TO (since INT) FROM Employee TO Department")
        .unwrap();
    println!("  CREATE EDGE TYPE BELONGS_TO ... FROM Employee TO Department");

    graph
        .ddl("ALTER NODE TYPE Employee ADD hire_date INT")
        .unwrap();
    println!("  ALTER NODE TYPE Employee ADD hire_date INT");

    graph
        .ddl("ALTER NODE TYPE Employee ALLOW ADDITIONAL PROPERTIES")
        .unwrap();
    println!("  ALTER NODE TYPE Employee ALLOW ADDITIONAL PROPERTIES");

    graph.ddl("SET SCHEMA VALIDATION STRICT").unwrap();
    println!("  SET SCHEMA VALIDATION STRICT");

    let schema = graph.schema().expect("Schema should be set");
    println!("\nResulting schema:");
    println!(
        "  Vertex types: {:?}",
        schema.vertex_labels().collect::<Vec<_>>()
    );
    println!(
        "  Edge types: {:?}",
        schema.edge_labels().collect::<Vec<_>>()
    );
    println!("  Mode: {:?}", schema.mode);

    if let Some(employee) = schema.vertex_schema("Employee") {
        println!(
            "\n  Employee properties: {:?}",
            employee.properties.keys().collect::<Vec<_>>()
        );
        println!("  Allows additional: {}", employee.additional_properties);
    }
    println!();
}

fn demo_validation_modes() {
    println!("============================================================");
    println!("PART 3C: VALIDATION MODES");
    println!("============================================================\n");

    // Mode: None
    let schema_none = SchemaBuilder::new()
        .mode(ValidationMode::None)
        .vertex("Person")
        .property("name", PropertyType::String)
        .done()
        .build();

    let mut storage = InMemoryGraph::new();
    println!("ValidationMode::None - Violations allowed (documentation only)");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {age: 25})", // Missing required 'name'
        Some(&schema_none),
    );
    match result {
        Ok(_) => println!("  [pass] Allowed (as expected)"),
        Err(e) => println!("  [fail] Error: {}", e),
    }
    println!();

    // Mode: Strict
    let schema_strict = SchemaBuilder::new()
        .mode(ValidationMode::Strict)
        .vertex("Person")
        .property("name", PropertyType::String)
        .done()
        .build();

    let mut storage = InMemoryGraph::new();
    println!("ValidationMode::Strict - Unknown labels allowed, violations rejected");

    let result = execute_with_schema(
        &mut storage,
        "CREATE (r:Robot {model: 'R2D2'})", // Unknown label
        Some(&schema_strict),
    );
    match result {
        Ok(_) => println!("  [pass] Unknown label 'Robot' allowed"),
        Err(e) => println!("  [fail] Error: {}", e),
    }

    let result = execute_with_schema(
        &mut storage,
        "CREATE (p:Person {age: 25})", // Missing required
        Some(&schema_strict),
    );
    match result {
        Ok(_) => println!("  [fail] Unexpected success"),
        Err(_) => println!("  [pass] Schema violation rejected"),
    }
    println!();

    // Mode: Closed
    let schema_closed = SchemaBuilder::new()
        .mode(ValidationMode::Closed)
        .vertex("Person")
        .property("name", PropertyType::String)
        .done()
        .build();

    let mut storage = InMemoryGraph::new();
    println!("ValidationMode::Closed - All labels must have schemas");
    let result = execute_with_schema(
        &mut storage,
        "CREATE (r:Robot {model: 'R2D2'})", // Unknown label
        Some(&schema_closed),
    );
    match result {
        Ok(_) => println!("  [fail] Unexpected success"),
        Err(_) => println!("  [pass] Unknown label 'Robot' rejected"),
    }
    println!();
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    println!("=== Interstellar GQL Comprehensive Example ===\n");

    demo_advanced_queries();
    demo_mutations();
    demo_schema_builder();
    demo_ddl();
    demo_validation_modes();

    println!("============================================================");
    println!("SUMMARY: GQL FEATURES DEMONSTRATED");
    println!("============================================================\n");

    println!("Advanced Queries:");
    println!("  - Inline WHERE: (p:Person WHERE p.id > 3)");
    println!("  - Parameters: $paramName");
    println!("  - LET Clause: LET count = COUNT(p)");
    println!("  - List Comprehensions: [n IN names | UPPER(n)]");
    println!("  - String Concat: name || ' (ID: ' || id || ')'");
    println!("  - Map Literals: {{name: p.name, id: p.id}}");
    println!();

    println!("Mutations:");
    println!("  - CREATE (n:Label {{prop: value}})");
    println!("  - SET n.prop = value");
    println!("  - REMOVE n.prop");
    println!("  - DELETE n / DETACH DELETE n");
    println!("  - MERGE ... ON CREATE SET ... ON MATCH SET ...");
    println!();

    println!("Schema & DDL:");
    println!("  - SchemaBuilder::new().vertex(\"Type\").property(...)");
    println!("  - CREATE NODE TYPE / CREATE EDGE TYPE");
    println!("  - ALTER NODE TYPE ... ADD / ALLOW ADDITIONAL PROPERTIES");
    println!("  - SET SCHEMA VALIDATION STRICT|CLOSED|NONE");
    println!("  - Validation modes: None, Warn, Strict, Closed");
    println!();

    println!("=== Example Complete ===");
}
