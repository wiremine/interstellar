//! # Interstellar Gremlin Quickstart
//!
//! A minimal introduction to Interstellar's Gremlin-style traversal API.
//!
//! This example demonstrates:
//! - Creating an in-memory graph
//! - Adding vertices and edges with properties
//! - Basic traversals and navigation
//! - Terminal steps for collecting results
//! - Deleting elements
//!
//! Run: `cargo run --example quickstart_gremlin`

use interstellar::storage::Graph;
use interstellar::value::Value;

fn main() {
    println!("=== Interstellar Gremlin Quickstart ===\n");

    // -------------------------------------------------------------------------
    // 1. Create an in-memory graph
    // -------------------------------------------------------------------------
    let graph = Graph::new();
    let g = graph.gremlin();

    // -------------------------------------------------------------------------
    // 2. Mutations: Adding vertices with add_v() and property()
    // -------------------------------------------------------------------------
    println!("-- Adding Vertices --\n");

    let alice = g
        .add_v("Person")
        .property("name", "Alice")
        .property("age", 30i64)
        .next()
        .unwrap();

    let bob = g
        .add_v("Person")
        .property("name", "Bob")
        .property("age", 25i64)
        .next()
        .unwrap();

    let carol = g
        .add_v("Person")
        .property("name", "Carol")
        .property("age", 35i64)
        .next()
        .unwrap();

    let acme = g
        .add_v("Company")
        .property("name", "Acme Corp")
        .next()
        .unwrap();

    println!("Created: Alice, Bob, Carol (Person) and Acme Corp (Company)");

    // Extract vertex IDs for edge creation
    let alice_id = alice.as_vertex_id().unwrap();
    let bob_id = bob.as_vertex_id().unwrap();
    let carol_id = carol.as_vertex_id().unwrap();
    let acme_id = acme.as_vertex_id().unwrap();

    // -------------------------------------------------------------------------
    // 3. Mutations: Adding edges with add_e()
    // -------------------------------------------------------------------------
    println!("\n-- Adding Edges --\n");

    g.add_e("knows")
        .from_id(alice_id)
        .to_id(bob_id)
        .property("since", 2020i64)
        .iterate();

    g.add_e("knows").from_id(alice_id).to_id(carol_id).iterate();
    g.add_e("knows").from_id(bob_id).to_id(carol_id).iterate();
    g.add_e("works_at")
        .from_id(alice_id)
        .to_id(acme_id)
        .iterate();

    println!("Created: Alice->Bob, Alice->Carol, Bob->Carol (knows), Alice->Acme (works_at)");

    // -------------------------------------------------------------------------
    // 4. Basic Traversals: Counting and filtering
    // -------------------------------------------------------------------------
    println!("\n-- Basic Traversals --\n");

    // count() - terminal step returning u64
    println!("Total vertices: {}", g.v().count());
    println!("Total edges: {}", g.e().count());
    println!("Person vertices: {}", g.v().has_label("Person").count());

    // -------------------------------------------------------------------------
    // 5. Property Access: values() and to_list()
    // -------------------------------------------------------------------------
    println!("\n-- Property Access --\n");

    // to_list() - terminal step returning Vec<Value>
    let names: Vec<Value> = g.v().has_label("Person").values("name").to_list();
    println!("All person names: {:?}", names);

    // -------------------------------------------------------------------------
    // 6. Navigation: out(), in_(), has_value()
    // -------------------------------------------------------------------------
    println!("\n-- Navigation --\n");

    // Find Alice's connections via "knows" edges
    let alice_knows: Vec<Value> = g
        .v()
        .has_value("name", Value::from("Alice"))
        .out_label("knows")
        .values("name")
        .to_list();
    println!("Alice knows: {:?}", alice_knows);

    // Find who works at Acme (incoming "works_at" edges)
    let acme_employees: Vec<Value> = g
        .v()
        .has_value("name", Value::from("Acme Corp"))
        .in_label("works_at")
        .values("name")
        .to_list();
    println!("Acme employees: {:?}", acme_employees);

    // Multi-hop: friends of friends
    let fof: Vec<Value> = g
        .v()
        .has_value("name", Value::from("Alice"))
        .out_label("knows")
        .out_label("knows")
        .values("name")
        .to_list();
    println!("Friends of Alice's friends: {:?}", fof);

    // -------------------------------------------------------------------------
    // 7. Delete: drop() and iterate()
    // -------------------------------------------------------------------------
    println!("\n-- Delete --\n");

    println!("Vertices before drop: {}", g.v().count());

    // drop() marks elements for deletion, iterate() executes
    g.v()
        .has_value("name", Value::from("Carol"))
        .drop()
        .iterate();

    println!("Vertices after dropping Carol: {}", g.v().count());

    println!("\n=== Quickstart Complete ===");
}
