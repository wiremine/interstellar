//! # Interstellar GQL Quickstart
//!
//! A minimal introduction to Interstellar's GQL (Graph Query Language).
//!
//! Run: `cargo run --example quickstart_gql`

use interstellar::storage::Graph;
use interstellar::value::Value;
use interstellar::ValueMap;

fn main() {
    println!("=== Interstellar GQL Quickstart ===\n");
    let graph = Graph::new();

    // CREATE vertices and edges
    println!("## CREATE Vertices and Edges\n");
    graph
        .gql("CREATE (:Person {name: 'Alice', age: 30})-[:KNOWS]->(:Person {name: 'Bob', age: 25})")
        .unwrap();
    graph
        .gql("CREATE (:Person {name: 'Carol', age: 35}), (:Company {name: 'Acme'})")
        .unwrap();
    graph.gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b) CREATE (b)-[:KNOWS]->(:Person {name: 'Dave', age: 28})").unwrap();
    graph.gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b) CREATE (a)-[:WORKS_AT]->(:Company {name: 'TechCo'})").unwrap();
    println!("   Created 4 Person, 2 Company vertices + edges\n");

    // MATCH, RETURN, WHERE
    println!("## Basic Queries\n");
    println!(
        "   count(*): {:?}",
        graph.gql("MATCH (p:Person) RETURN count(*)").unwrap()[0]
    );
    println!("\n   ORDER BY age:");
    for r in graph
        .gql("MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age")
        .unwrap()
    {
        print_row(&r, &["p.name", "p.age"]);
    }
    println!("\n   WHERE age > 28:");
    for r in graph
        .gql("MATCH (p:Person) WHERE p.age > 28 RETURN p.name, p.age")
        .unwrap()
    {
        print_row(&r, &["p.name", "p.age"]);
    }

    // Pattern matching
    println!("\n## Pattern Matching\n");
    println!("   Who knows whom:");
    for r in graph
        .gql("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        .unwrap()
    {
        if let Value::Map(m) = r {
            println!("   {} -> {}", fmt(&m, "a.name"), fmt(&m, "b.name"));
        }
    }
    println!("\n   Friends of friends (multi-hop):");
    for r in graph
        .gql("MATCH (:Person {name: 'Alice'})-[:KNOWS]->()-[:KNOWS]->(fof) RETURN fof.name")
        .unwrap()
    {
        println!("   -> {}", format_value(&r));
    }

    // Aggregations
    println!("\n## Aggregations\n");
    println!(
        "   avg(age): {:?}",
        graph.gql("MATCH (p:Person) RETURN avg(p.age)").unwrap()[0]
    );
    println!("\n   Top 2 oldest (LIMIT):");
    for r in graph
        .gql("MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age DESC LIMIT 2")
        .unwrap()
    {
        print_row(&r, &["p.name", "p.age"]);
    }

    // Updates: SET, DELETE
    println!("\n## Updates\n");
    graph
        .gql("MATCH (p:Person {name: 'Alice'}) SET p.age = 31")
        .unwrap();
    println!(
        "   SET: Alice age = {:?}",
        graph
            .gql("MATCH (p:Person {name: 'Alice'}) RETURN p.age")
            .unwrap()[0]
    );
    graph
        .gql("MATCH (p:Person {name: 'Carol'}) DETACH DELETE p")
        .unwrap();
    println!(
        "   DETACH DELETE: count = {:?}",
        graph.gql("MATCH (p:Person) RETURN count(*)").unwrap()[0]
    );

    println!("\n=== Quickstart Complete ===");
}

fn format_value(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Int(n) => n.to_string(),
        Value::Float(f) => format!("{:.1}", f),
        Value::Map(m) => m.values().next().map(format_value).unwrap_or_default(),
        _ => format!("{:?}", v),
    }
}

fn fmt(m: &ValueMap, k: &str) -> String {
    format_value(m.get(k).unwrap_or(&Value::Null))
}

fn print_row(row: &Value, keys: &[&str]) {
    if let Value::Map(m) = row {
        println!(
            "   -> {}",
            keys.iter()
                .map(|k| fmt(m, k))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
}
