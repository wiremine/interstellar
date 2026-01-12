//! GQL Mutations Example
//!
//! This example demonstrates the GQL (Graph Query Language) mutation operations:
//! - CREATE: Adding new vertices and edges
//! - SET: Updating properties
//! - REMOVE: Removing properties
//! - DELETE: Deleting vertices and edges
//! - DETACH DELETE: Deleting vertices with automatic edge removal
//! - MERGE: Upsert operations (create if not exists, update if exists)
//!
//! Unlike the Gremlin-style fluent API (see `mutations.rs`), GQL provides a
//! declarative SQL-like syntax for graph mutations.
//!
//! Run with: `cargo run --example gql_mutations`

use intersteller::gql::{execute_mutation, parse_statement, MutationError};
use intersteller::storage::{GraphStorage, InMemoryGraph};
use intersteller::value::Value;

/// Helper function to execute a GQL mutation and handle errors.
fn execute(storage: &mut InMemoryGraph, query: &str) -> Result<Vec<Value>, MutationError> {
    let stmt = parse_statement(query).map_err(|e| {
        MutationError::Compile(intersteller::gql::CompileError::UnsupportedFeature(format!(
            "Parse error: {}",
            e
        )))
    })?;
    execute_mutation(&stmt, storage)
}

fn main() {
    println!("=== Intersteller GQL Mutations Example ===\n");

    // -------------------------------------------------------------------------
    // Step 1: Create an empty graph
    // -------------------------------------------------------------------------
    let mut storage = InMemoryGraph::new();
    println!("Created empty graph");
    println!("  Initial vertex count: {}", storage.vertex_count());
    println!("  Initial edge count: {}", storage.edge_count());
    println!();

    // -------------------------------------------------------------------------
    // Step 2: CREATE - Adding Vertices
    // -------------------------------------------------------------------------
    println!("--- CREATE: Adding Vertices ---\n");

    // Create a single vertex with properties
    execute(
        &mut storage,
        "CREATE (alice:Person {name: 'Alice', age: 30, city: 'New York'})",
    )
    .unwrap();
    println!("Created Alice: CREATE (alice:Person {{name: 'Alice', age: 30, city: 'New York'}})");

    // Create multiple vertices in one statement
    execute(
        &mut storage,
        "CREATE (bob:Person {name: 'Bob', age: 25, city: 'Boston'}), (carol:Person {name: 'Carol', age: 35, city: 'Seattle'})"
    ).unwrap();
    println!("Created Bob and Carol with multiple patterns");

    // Create with RETURN to get the created vertex
    let results = execute(
        &mut storage,
        "CREATE (dave:Person {name: 'Dave', age: 28}) RETURN dave.name",
    )
    .unwrap();
    println!("Created Dave with RETURN: {:?}", results);

    println!();
    println!("After creating vertices:");
    println!("  Vertex count: {}", storage.vertex_count());
    println!();

    // -------------------------------------------------------------------------
    // Step 3: CREATE - Adding Edges
    // -------------------------------------------------------------------------
    println!("--- CREATE: Adding Edges ---\n");

    // Create an edge pattern between new vertices
    execute(
        &mut storage,
        "CREATE (software:Software {name: 'Intersteller', language: 'Rust', version: '1.0'})",
    )
    .unwrap();
    println!("Created Software vertex");

    // Match existing vertices and create edges between them
    // Note: We need to match using a connected pattern for now
    execute(
        &mut storage,
        r#"
        MATCH (a:Person {name: 'Alice'})-[:KNOWS*0..0]->(a)
        CREATE (a)-[:KNOWS {since: 2020, relationship: 'friends'}]->(b:Person {name: 'Bob_temp'})
        "#,
    )
    .unwrap_or_else(|_| {
        // Fallback: Create edge pattern directly
        execute(
            &mut storage,
            "CREATE (a:Person {name: 'Alice_2'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob_2'})"
        ).unwrap()
    });

    // Create a chain of relationships
    execute(
        &mut storage,
        "CREATE (x:Person {name: 'Eve'})-[:FOLLOWS]->(y:Person {name: 'Frank'})-[:FOLLOWS]->(z:Person {name: 'Grace'})"
    ).unwrap();
    println!("Created chain: Eve -[:FOLLOWS]-> Frank -[:FOLLOWS]-> Grace");

    println!();
    println!("After creating edges:");
    println!("  Vertex count: {}", storage.vertex_count());
    println!("  Edge count: {}", storage.edge_count());
    println!();

    // -------------------------------------------------------------------------
    // Step 4: SET - Updating Properties
    // -------------------------------------------------------------------------
    println!("--- SET: Updating Properties ---\n");

    // Update a single property
    execute(
        &mut storage,
        "MATCH (n:Person {name: 'Alice'}) SET n.age = 31",
    )
    .unwrap();
    println!("Updated Alice's age: SET n.age = 31");

    // Update multiple properties
    execute(
        &mut storage,
        "MATCH (n:Person {name: 'Alice'}) SET n.status = 'active', n.verified = true",
    )
    .unwrap();
    println!("Set multiple properties: SET n.status = 'active', n.verified = true");

    // Update with computed expression
    execute(
        &mut storage,
        "MATCH (n:Person {name: 'Alice'}) SET n.next_birthday_age = n.age + 1",
    )
    .unwrap();
    println!("Set computed value: SET n.next_birthday_age = n.age + 1");

    // Update with RETURN to see the result
    let results = execute(
        &mut storage,
        "MATCH (n:Person {name: 'Alice'}) SET n.updated_at = 1234567890 RETURN n.name, n.age, n.status",
    ).unwrap();
    println!("Updated with RETURN: {:?}", results);

    // Update using WHERE clause to filter
    execute(
        &mut storage,
        "MATCH (n:Person) WHERE n.age > 30 SET n.senior = true",
    )
    .unwrap();
    println!("Set property on filtered vertices: WHERE n.age > 30 SET n.senior = true");

    println!();

    // -------------------------------------------------------------------------
    // Step 5: REMOVE - Removing Properties
    // -------------------------------------------------------------------------
    println!("--- REMOVE: Removing Properties ---\n");

    // First add a property to remove
    execute(
        &mut storage,
        "MATCH (n:Person {name: 'Bob'}) SET n.temporary = 'will be removed'",
    )
    .unwrap();
    println!("Added temporary property to Bob");

    // Remove the property
    execute(
        &mut storage,
        "MATCH (n:Person {name: 'Bob'}) REMOVE n.temporary",
    )
    .unwrap();
    println!("Removed property: REMOVE n.temporary");

    println!();

    // -------------------------------------------------------------------------
    // Step 6: MERGE - Upsert Operations
    // -------------------------------------------------------------------------
    println!("--- MERGE: Upsert Operations ---\n");

    // MERGE creates if not exists
    execute(
        &mut storage,
        "MERGE (n:Person {name: 'Henry'}) ON CREATE SET n.created = true, n.source = 'merge'",
    )
    .unwrap();
    println!("MERGE created new vertex Henry: ON CREATE SET n.created = true");

    // Verify Henry was created
    let vertex_count_before = storage.vertex_count();

    // MERGE finds existing and applies ON MATCH
    execute(
        &mut storage,
        "MERGE (n:Person {name: 'Henry'}) ON MATCH SET n.updated = true, n.visits = 1",
    )
    .unwrap();
    println!("MERGE matched existing Henry: ON MATCH SET n.updated = true");

    // No new vertex should be created
    assert_eq!(storage.vertex_count(), vertex_count_before);
    println!(
        "Verified: No new vertex created (count still {})",
        vertex_count_before
    );

    // MERGE with both ON CREATE and ON MATCH
    execute(
        &mut storage,
        r#"
        MERGE (n:Person {name: 'Ivy'}) 
        ON CREATE SET n.status = 'new', n.created_at = 1000
        ON MATCH SET n.status = 'existing', n.updated_at = 2000
        "#,
    )
    .unwrap();
    println!("MERGE with both actions created Ivy with status='new'");

    // Second MERGE should match
    execute(
        &mut storage,
        r#"
        MERGE (n:Person {name: 'Ivy'}) 
        ON CREATE SET n.status = 'new', n.created_at = 1000
        ON MATCH SET n.status = 'existing', n.updated_at = 2000
        "#,
    )
    .unwrap();
    println!("Second MERGE matched Ivy, set status='existing'");

    println!();

    // -------------------------------------------------------------------------
    // Step 7: DELETE - Removing Elements
    // -------------------------------------------------------------------------
    println!("--- DELETE: Removing Elements ---\n");

    // First, let's see our current state
    println!("Before deletion:");
    println!("  Vertex count: {}", storage.vertex_count());
    println!("  Edge count: {}", storage.edge_count());

    // Create a vertex specifically for deletion (without edges)
    execute(
        &mut storage,
        "CREATE (temp:Temporary {name: 'ToDelete', purpose: 'deletion test'})",
    )
    .unwrap();
    println!("Created temporary vertex for deletion test");

    let count_before = storage.vertex_count();

    // DELETE the isolated vertex
    execute(
        &mut storage,
        "MATCH (n:Temporary {name: 'ToDelete'}) DELETE n",
    )
    .unwrap();
    println!("Deleted temporary vertex: DELETE n");

    assert_eq!(storage.vertex_count(), count_before - 1);
    println!("Verified: Vertex count decreased by 1");

    // Demonstrate DELETE failing when vertex has edges
    // First create connected vertices
    execute(
        &mut storage,
        "CREATE (a:Test {name: 'Connected1'})-[:LINK]->(b:Test {name: 'Connected2'})",
    )
    .unwrap();

    let result = execute(&mut storage, "MATCH (n:Test {name: 'Connected1'}) DELETE n");
    match result {
        Err(MutationError::VertexHasEdges(_)) => {
            println!("DELETE correctly failed: vertex has edges (use DETACH DELETE)");
        }
        Ok(_) => println!("Unexpected: DELETE succeeded on vertex with edges"),
        Err(e) => println!("Unexpected error: {:?}", e),
    }

    // DELETE an edge
    execute(&mut storage, "MATCH (a:Test)-[r:LINK]->(b:Test) DELETE r").unwrap();
    println!("Deleted edge: MATCH (a)-[r:LINK]->(b) DELETE r");

    // Now we can delete the vertices
    execute(&mut storage, "MATCH (n:Test) DELETE n").unwrap();
    println!("Deleted Test vertices after removing edges");

    println!();

    // -------------------------------------------------------------------------
    // Step 8: DETACH DELETE - Removing Vertices with Edges
    // -------------------------------------------------------------------------
    println!("--- DETACH DELETE: Removing Vertices with Edges ---\n");

    // Create a connected structure
    execute(
        &mut storage,
        "CREATE (hub:Hub {name: 'Central'})-[:CONNECTS]->(spoke1:Spoke {name: 'Spoke1'})",
    )
    .unwrap();
    execute(
        &mut storage,
        "CREATE (hub2:Hub {name: 'Central2'})-[:CONNECTS]->(spoke2:Spoke {name: 'Spoke2'})",
    )
    .unwrap();

    let edge_count_before = storage.edge_count();
    let vertex_count_before = storage.vertex_count();
    println!("Created hub-and-spoke structure");
    println!("  Vertex count: {}", vertex_count_before);
    println!("  Edge count: {}", edge_count_before);

    // DETACH DELETE removes vertex AND all connected edges
    execute(
        &mut storage,
        "MATCH (n:Hub {name: 'Central'}) DETACH DELETE n",
    )
    .unwrap();
    println!("DETACH DELETE removed hub vertex and its edges");

    println!("After DETACH DELETE:");
    println!(
        "  Vertex count: {} (was {})",
        storage.vertex_count(),
        vertex_count_before
    );
    println!(
        "  Edge count: {} (was {})",
        storage.edge_count(),
        edge_count_before
    );

    println!();

    // -------------------------------------------------------------------------
    // Step 9: Complex Mutation Examples
    // -------------------------------------------------------------------------
    println!("--- Complex Mutation Examples ---\n");

    // Create, then immediately update
    execute(
        &mut storage,
        "CREATE (company:Company {name: 'TechCorp', founded: 2020})",
    )
    .unwrap();
    execute(
        &mut storage,
        "MATCH (c:Company {name: 'TechCorp'}) SET c.employees = 100, c.revenue = 1000000",
    )
    .unwrap();
    println!("Created company and set additional properties");

    // Conditional update with WHERE
    execute(
        &mut storage,
        "MATCH (p:Person) WHERE p.age >= 30 AND p.senior IS NULL SET p.category = 'experienced'",
    )
    .unwrap();
    println!("Conditional update on multiple matching vertices");

    // MERGE with complex conditions
    execute(
        &mut storage,
        r#"
        MERGE (config:Config {key: 'app_version'})
        ON CREATE SET config.value = '1.0.0', config.created = true
        ON MATCH SET config.value = '1.0.1', config.updated = true
        "#,
    )
    .unwrap();
    println!("MERGE for configuration upsert pattern");

    println!();

    // -------------------------------------------------------------------------
    // Step 10: Summary
    // -------------------------------------------------------------------------
    println!("=== Final Graph State ===\n");
    println!("Vertex count: {}", storage.vertex_count());
    println!("Edge count: {}", storage.edge_count());
    println!();

    println!("All vertices:");
    for vertex in storage.all_vertices() {
        let name = vertex
            .properties
            .get("name")
            .map(|v| format!("{:?}", v))
            .unwrap_or_else(|| "unnamed".to_string());
        println!("  [{:?}] :{} - {}", vertex.id, vertex.label, name);
    }

    println!();
    println!("All edges:");
    for edge in storage.all_edges() {
        println!(
            "  [{:?}] :{} ({:?} -> {:?})",
            edge.id, edge.label, edge.src, edge.dst
        );
    }

    println!();
    println!("=== GQL Mutations Summary ===");
    println!();
    println!("CREATE patterns:");
    println!("  CREATE (n:Label {{prop: value}})          -- Create vertex");
    println!("  CREATE (a)-[:REL]->(b)                   -- Create edge pattern");
    println!("  CREATE (n:Label) RETURN n                -- Create with return");
    println!();
    println!("SET property updates:");
    println!("  MATCH (n) SET n.prop = value             -- Set single property");
    println!("  MATCH (n) SET n.a = 1, n.b = 2           -- Set multiple properties");
    println!("  MATCH (n) SET n.x = n.y + 1              -- Computed value");
    println!();
    println!("REMOVE properties:");
    println!("  MATCH (n) REMOVE n.prop                  -- Remove property");
    println!();
    println!("DELETE elements:");
    println!("  MATCH (n) DELETE n                       -- Delete vertex (no edges)");
    println!("  MATCH ()-[r]->() DELETE r                -- Delete edge");
    println!();
    println!("DETACH DELETE:");
    println!("  MATCH (n) DETACH DELETE n                -- Delete vertex + edges");
    println!();
    println!("MERGE upsert:");
    println!("  MERGE (n:Label {{key: value}})             -- Match or create");
    println!("  ... ON CREATE SET n.prop = value         -- Set if created");
    println!("  ... ON MATCH SET n.prop = value          -- Set if matched");
    println!();
    println!("=== Example Complete ===");
}
