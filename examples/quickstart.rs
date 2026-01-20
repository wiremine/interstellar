//! Interstellar Quickstart Example
//!
//! This example provides a quick introduction to Interstellar's graph database
//! capabilities, demonstrating both Gremlin-style traversals and GQL queries.
//!
//! # What You'll Learn
//!
//! 1. **Creating graphs** - In-memory graph storage
//! 2. **Gremlin-style API** - Fluent traversal API for mutations and queries
//! 3. **GQL queries** - SQL-like graph query language
//!
//! # Running the Example
//!
//! ```bash
//! cargo run --example quickstart
//! ```

use interstellar::storage::Graph;
use interstellar::value::Value;

fn main() {
    println!("========================================================================");
    println!("          Interstellar Graph Database - Quickstart");
    println!("========================================================================\n");

    // Create an in-memory graph
    let graph = Graph::new();

    // Part 1: Gremlin-style mutations and traversals
    demo_gremlin(&graph);

    // Part 2: GQL queries
    demo_gql(&graph);

    println!("\nQuickstart complete!");
}

// =============================================================================
// Part 1: Gremlin-Style Mutations and Traversals
// =============================================================================

fn demo_gremlin(graph: &Graph) {
    println!("------------------------------------------------------------------------");
    println!("  PART 1: GREMLIN-STYLE API");
    println!("------------------------------------------------------------------------\n");

    // -------------------------------------------------------------------------
    // Mutations: Adding vertices and edges
    // -------------------------------------------------------------------------
    println!("## Mutations\n");

    // Get a traversal source for mutations
    let g = graph.gremlin();

    // Add vertices using addV()
    println!("Adding vertices with g.add_v()...");

    let alice = g
        .add_v("Person")
        .property("name", "Alice")
        .property("age", 30i64)
        .property("city", "New York")
        .next()
        .unwrap();

    let bob = g
        .add_v("Person")
        .property("name", "Bob")
        .property("age", 25i64)
        .property("city", "Boston")
        .next()
        .unwrap();

    let carol = g
        .add_v("Person")
        .property("name", "Carol")
        .property("age", 35i64)
        .property("city", "Chicago")
        .next()
        .unwrap();

    let dave = g
        .add_v("Person")
        .property("name", "Dave")
        .property("age", 28i64)
        .property("city", "Denver")
        .next()
        .unwrap();

    let acme = g
        .add_v("Company")
        .property("name", "Acme Corp")
        .property("industry", "Technology")
        .next()
        .unwrap();

    println!("   Created 4 Person vertices and 1 Company vertex");

    // Extract vertex IDs for edge creation
    let alice_id = alice.as_vertex_id().unwrap();
    let bob_id = bob.as_vertex_id().unwrap();
    let carol_id = carol.as_vertex_id().unwrap();
    let dave_id = dave.as_vertex_id().unwrap();
    let acme_id = acme.as_vertex_id().unwrap();

    // Add edges using addE()
    println!("\nAdding edges with g.add_e()...");

    g.add_e("knows")
        .from_id(alice_id)
        .to_id(bob_id)
        .property("since", 2020i64)
        .iterate();

    g.add_e("knows")
        .from_id(alice_id)
        .to_id(carol_id)
        .property("since", 2018i64)
        .iterate();

    g.add_e("knows").from_id(bob_id).to_id(carol_id).iterate();

    g.add_e("knows").from_id(carol_id).to_id(dave_id).iterate();

    g.add_e("works_at")
        .from_id(alice_id)
        .to_id(acme_id)
        .property("role", "Engineer")
        .iterate();

    g.add_e("works_at")
        .from_id(bob_id)
        .to_id(acme_id)
        .property("role", "Manager")
        .iterate();

    println!("   Created 6 edges (4 'knows', 2 'works_at')");

    // -------------------------------------------------------------------------
    // Traversals: Querying the graph
    // -------------------------------------------------------------------------
    println!("\n## Traversals\n");

    // Get a fresh traversal source
    let g = graph.gremlin();

    // Count vertices and edges
    println!("Basic counts:");
    println!("   g.v().count() = {}", g.v().count());
    println!("   g.e().count() = {}", g.e().count());
    println!(
        "   g.v().has_label(\"Person\").count() = {}",
        g.v().has_label("Person").count()
    );

    // Get all names
    println!("\nGet all person names:");
    println!("   g.v().has_label(\"Person\").values(\"name\")");
    let names: Vec<_> = g.v().has_label("Person").values("name").to_list();
    for name in &names {
        println!("   -> {}", format_value(name));
    }

    // Navigation - outgoing edges
    println!("\nNavigate outgoing edges (who does Alice know?):");
    println!("   g.v().has_value(\"name\", \"Alice\").out_label(\"knows\")");
    let alice_knows: Vec<_> = g
        .v()
        .has_value("name", Value::from("Alice"))
        .out_label("knows")
        .values("name")
        .to_list();
    for name in &alice_knows {
        println!("   -> {}", format_value(name));
    }

    // Multi-hop traversal
    println!("\nMulti-hop traversal (friends of friends):");
    println!("   g.v().has_value(\"name\", \"Alice\").out_label(\"knows\").out_label(\"knows\")");
    let fof: Vec<_> = g
        .v()
        .has_value("name", Value::from("Alice"))
        .out_label("knows")
        .out_label("knows")
        .values("name")
        .to_list();
    for name in &fof {
        println!("   -> {}", format_value(name));
    }

    // Incoming edges
    println!("\nNavigate incoming edges (who works at Acme?):");
    println!("   g.v().has_value(\"name\", \"Acme Corp\").in_label(\"works_at\")");
    let employees: Vec<_> = g
        .v()
        .has_value("name", Value::from("Acme Corp"))
        .in_label("works_at")
        .values("name")
        .to_list();
    for name in &employees {
        println!("   -> {}", format_value(name));
    }

    // Drop a vertex
    println!("\nMutations - drop vertex:");
    println!("   g.v().has_value(\"name\", \"Dave\").drop()");
    g.v()
        .has_value("name", Value::from("Dave"))
        .drop()
        .iterate();
    println!(
        "   Vertex count after drop: {}",
        graph.gremlin().v().count()
    );
}

// =============================================================================
// Part 2: GQL Queries
// =============================================================================

fn demo_gql(graph: &Graph) {
    println!("\n------------------------------------------------------------------------");
    println!("  PART 2: GQL (GRAPH QUERY LANGUAGE)");
    println!("------------------------------------------------------------------------\n");

    // With the unified API, all GQL queries go through graph.gql()
    // It auto-detects reads vs mutations and handles them appropriately

    // Basic MATCH query
    println!("## Basic Queries\n");

    println!("Count all people:");
    let query = "MATCH (p:Person) RETURN count(*)";
    println!("   {}", query);
    let result = graph.gql(query).unwrap();
    println!("   -> {:?}\n", result[0]);

    // Return properties
    println!("Get names and ages, ordered by age:");
    let query = "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age";
    println!("   {}", query);
    let results = graph.gql(query).unwrap();
    for row in &results {
        if let Value::Map(m) = row {
            let name = m.get("p.name").unwrap_or(&Value::Null);
            let age = m.get("p.age").unwrap_or(&Value::Null);
            println!("   -> {}, age {}", format_value(name), format_value(age));
        }
    }

    // Pattern matching with relationships
    println!("\n## Pattern Matching\n");

    println!("Who knows whom:");
    let query = "MATCH (a:Person)-[:knows]->(b:Person) RETURN a.name, b.name";
    println!("   {}", query);
    let results = graph.gql(query).unwrap();
    for row in &results {
        if let Value::Map(m) = row {
            let a = m.get("a.name").unwrap_or(&Value::Null);
            let b = m.get("b.name").unwrap_or(&Value::Null);
            println!("   -> {} knows {}", format_value(a), format_value(b));
        }
    }

    // WHERE clause filtering
    println!("\n## Filtering with WHERE\n");

    println!("People over 28:");
    let query = "MATCH (p:Person) WHERE p.age > 28 RETURN p.name, p.age";
    println!("   {}", query);
    let results = graph.gql(query).unwrap();
    for row in &results {
        if let Value::Map(m) = row {
            let name = m.get("p.name").unwrap_or(&Value::Null);
            let age = m.get("p.age").unwrap_or(&Value::Null);
            println!("   -> {} ({})", format_value(name), format_value(age));
        }
    }

    // Multi-hop patterns
    println!("\n## Multi-hop Patterns\n");

    println!("Friends of Alice's friends:");
    let query = r#"
        MATCH (a:Person {name: 'Alice'})-[:knows]->()-[:knows]->(fof:Person)
        RETURN DISTINCT fof.name
    "#;
    println!("   MATCH (a:Person {{name: 'Alice'}})-[:knows]->()-[:knows]->(fof:Person)");
    println!("   RETURN DISTINCT fof.name");
    let results = graph.gql(query).unwrap();
    for row in &results {
        println!("   -> {}", format_value(row));
    }

    // Aggregation
    println!("\n## Aggregation\n");

    println!("Average age by city:");
    let query = r#"
        MATCH (p:Person)
        RETURN p.city, avg(p.age) AS avg_age
        GROUP BY p.city
        ORDER BY avg_age DESC
    "#;
    println!("   MATCH (p:Person)");
    println!("   RETURN p.city, avg(p.age) AS avg_age");
    println!("   GROUP BY p.city ORDER BY avg_age DESC");
    let results = graph.gql(query).unwrap();
    for row in &results {
        if let Value::Map(m) = row {
            let city = m.get("p.city").unwrap_or(&Value::Null);
            let avg = m.get("avg_age").unwrap_or(&Value::Null);
            println!("   -> {}: {}", format_value(city), format_value(avg));
        }
    }

    // Mutations with GQL
    println!("\n## Mutations with GQL\n");

    println!("Create a new vertex:");
    let query = "CREATE (:Person {name: 'Eve', age: 22, city: 'Seattle'})";
    println!("   {}", query);
    graph.gql(query).unwrap();
    println!(
        "   Vertex count after CREATE: {:?}",
        graph.gql("MATCH (p:Person) RETURN count(*)").unwrap()[0]
    );

    println!("\nUpdate a property:");
    let query = "MATCH (p:Person) WHERE p.name = 'Alice' SET p.age = 31";
    println!("   {}", query);
    graph.gql(query).unwrap();
    let result = graph
        .gql("MATCH (p:Person {name: 'Alice'}) RETURN p.age")
        .unwrap();
    println!("   Alice's new age: {:?}", result[0]);
}

// =============================================================================
// Helper Functions
// =============================================================================

fn format_value(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Int(n) => n.to_string(),
        Value::Float(f) => format!("{:.2}", f),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::List(items) => {
            let formatted: Vec<String> = items.iter().map(format_value).collect();
            format!("[{}]", formatted.join(", "))
        }
        Value::Map(map) => {
            let pairs: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}: {}", k, format_value(v)))
                .collect();
            format!("{{{}}}", pairs.join(", "))
        }
        Value::Vertex(vid) => format!("v[{}]", vid.0),
        Value::Edge(eid) => format!("e[{}]", eid.0),
    }
}
