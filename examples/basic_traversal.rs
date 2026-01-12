//! Basic Traversal Example
//!
//! This example demonstrates the fundamental graph operations:
//! - Creating an in-memory graph
//! - Adding vertices and edges
//! - Basic traversal with `v()`, `e()`, `v_ids()`, `e_ids()`, and `inject()`
//!
//! Run with: `cargo run --example basic_traversal`

use intersteller::graph::Graph;
use intersteller::storage::InMemoryGraph;
use intersteller::value::Value;
use std::collections::HashMap;
use std::sync::Arc;

fn main() {
    println!("=== Intersteller Basic Traversal Example ===\n");

    // -------------------------------------------------------------------------
    // Step 1: Create graph and add data
    // -------------------------------------------------------------------------
    let mut storage = InMemoryGraph::new();

    // Add person vertices
    let alice = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props
    });

    let bob = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        props.insert("age".to_string(), Value::Int(25));
        props
    });

    let charlie = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Charlie".to_string()));
        props.insert("age".to_string(), Value::Int(35));
        props
    });

    // Add software vertex
    let graph_db = storage.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("GraphDB".to_string()));
        props.insert("version".to_string(), Value::Float(1.0));
        props
    });

    // Add edges
    let e1 = storage
        .add_edge(alice, bob, "knows", HashMap::new())
        .unwrap();
    let _e2 = storage
        .add_edge(bob, charlie, "knows", HashMap::new())
        .unwrap();
    let e3 = storage
        .add_edge(alice, graph_db, "uses", HashMap::new())
        .unwrap();
    let _e4 = storage
        .add_edge(bob, graph_db, "uses", HashMap::new())
        .unwrap();

    println!("Created graph with:");
    println!("  - 4 vertices (3 people, 1 software)");
    println!("  - 4 edges (2 'knows', 2 'uses')");
    println!();

    // Wrap in Graph for traversal API
    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // -------------------------------------------------------------------------
    // Step 2: Traverse all vertices
    // -------------------------------------------------------------------------
    println!("--- g.v() - All vertices ---");
    let all_vertices = g.v().to_list();
    println!("Total vertices: {}", all_vertices.len());
    for v in &all_vertices {
        if let Some(vid) = v.as_vertex_id() {
            println!("  Vertex: {:?}", vid);
        }
    }
    println!();

    // -------------------------------------------------------------------------
    // Step 3: Traverse all edges
    // -------------------------------------------------------------------------
    println!("--- g.e() - All edges ---");
    let all_edges = g.e().to_list();
    println!("Total edges: {}", all_edges.len());
    for e in &all_edges {
        if let Some(eid) = e.as_edge_id() {
            println!("  Edge: {:?}", eid);
        }
    }
    println!();

    // -------------------------------------------------------------------------
    // Step 4: Traverse specific vertices by ID
    // -------------------------------------------------------------------------
    println!("--- g.v_ids([...]) - Specific vertices ---");
    let specific = g.v_ids([alice, charlie]).to_list();
    println!("Vertices {:?} and {:?}:", alice, charlie);
    for v in &specific {
        println!("  Found: {:?}", v.as_vertex_id());
    }
    println!();

    // -------------------------------------------------------------------------
    // Step 5: Traverse specific edges by ID
    // -------------------------------------------------------------------------
    println!("--- g.e_ids([...]) - Specific edges ---");
    let specific_edges = g.e_ids([e1, e3]).to_list();
    println!("Edges {:?} and {:?}:", e1, e3);
    for e in &specific_edges {
        println!("  Found: {:?}", e.as_edge_id());
    }
    println!();

    // -------------------------------------------------------------------------
    // Step 6: Handle non-existent IDs gracefully
    // -------------------------------------------------------------------------
    println!("--- Handling non-existent IDs ---");
    // When we query for vertices that exist and ones that don't,
    // non-existent IDs are simply filtered out
    let all_exist = g.v_ids([alice, bob]).to_list();
    println!(
        "Requested alice and bob: found {} vertices",
        all_exist.len()
    );
    for v in &all_exist {
        println!("  {:?}", v.as_vertex_id());
    }
    println!();

    // -------------------------------------------------------------------------
    // Step 7: Inject arbitrary values
    // -------------------------------------------------------------------------
    println!("--- g.inject([...]) - Inject values ---");
    let injected = g.inject([1i64, 2i64, 3i64]).to_list();
    println!("Injected integers:");
    for v in &injected {
        println!("  {:?}", v);
    }
    println!();

    // Inject mixed Value types
    let mixed: Vec<Value> = vec![
        Value::Int(42),
        Value::String("hello".to_string()),
        Value::Bool(true),
        Value::Float(3.14),
    ];
    let injected_mixed = g.inject(mixed).to_list();
    println!("Injected mixed types:");
    for v in &injected_mixed {
        println!("  {:?}", v);
    }
    println!();

    // -------------------------------------------------------------------------
    // Step 8: Count operations
    // -------------------------------------------------------------------------
    println!("--- count() - Counting elements ---");
    println!("Vertex count: {}", g.v().count());
    println!("Edge count: {}", g.e().count());
    println!();

    // -------------------------------------------------------------------------
    // Step 9: Existence checks
    // -------------------------------------------------------------------------
    println!("--- has_next() - Existence checks ---");
    println!("Has vertices: {}", g.v().has_next());
    println!("Has edges: {}", g.e().has_next());
    // Querying for a vertex that doesn't exist will return false
    println!("Has alice vertex: {}", g.v_ids([alice]).has_next());
    println!();

    println!("=== Example Complete ===");
}
