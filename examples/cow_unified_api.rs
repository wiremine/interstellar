//! Unified API Example for Graph (In-Memory Copy-on-Write)
//!
//! This example demonstrates how `Graph` provides a unified API where both
//! reads and mutations use the same traversal interface. There's no need to
//! explicitly switch between "read mode" and "mutation mode".
//!
//! # Key Features Demonstrated
//!
//! 1. **Gremlin-style Traversal API**: Fluent, chainable API for graph operations
//! 2. **GQL Mutations**: SQL-like syntax for graph mutations
//! 3. **Unified Interface**: Same `g.v()`, `g.add_v()` etc. for both reads and writes
//! 4. **Automatic Mutation Execution**: Mutations execute when terminal steps are called
//!
//! Run: `cargo run --example cow_unified_api`

use interstellar::storage::Graph;
use interstellar::value::{Value, VertexId};

fn main() {
    println!("=== Graph Unified API Demo ===\n");

    // Create a new in-memory COW graph
    let graph = Graph::new();

    // =========================================================================
    // Part 1: Gremlin-style Mutations
    // =========================================================================
    println!("--- Part 1: Gremlin-style Mutations ---\n");

    // Get a traversal source - this is the entry point for all operations
    let g = graph.traversal();

    // Create vertices using add_v() - mutations execute automatically on next()
    // next() returns Option<Value>, and for add_v it's Value::Vertex(id)
    let alice_id: VertexId = g
        .add_v("Person")
        .property("name", "Alice")
        .property("age", 30)
        .property("city", "New York")
        .next()
        .expect("Failed to create Alice")
        .as_vertex_id()
        .expect("Expected vertex ID");

    let bob_id: VertexId = g
        .add_v("Person")
        .property("name", "Bob")
        .property("age", 25)
        .property("city", "Boston")
        .next()
        .expect("Failed to create Bob")
        .as_vertex_id()
        .expect("Expected vertex ID");

    let charlie_id: VertexId = g
        .add_v("Person")
        .property("name", "Charlie")
        .property("age", 35)
        .property("city", "Chicago")
        .next()
        .expect("Failed to create Charlie")
        .as_vertex_id()
        .expect("Expected vertex ID");

    println!(
        "Created vertices: Alice={:?}, Bob={:?}, Charlie={:?}",
        alice_id, bob_id, charlie_id
    );

    // Create edges using add_e() - same unified API
    g.add_e("KNOWS")
        .from_id(alice_id)
        .to_id(bob_id)
        .property("since", 2020)
        .property("relationship", "colleague")
        .iterate();

    g.add_e("KNOWS")
        .from_id(alice_id)
        .to_id(charlie_id)
        .property("since", 2018)
        .property("relationship", "friend")
        .iterate();

    g.add_e("KNOWS")
        .from_id(bob_id)
        .to_id(charlie_id)
        .property("since", 2022)
        .iterate();

    println!("Created edges between people\n");

    // =========================================================================
    // Part 2: Gremlin-style Queries (using same API)
    // =========================================================================
    println!("--- Part 2: Gremlin-style Queries ---\n");

    // Count all vertices
    let vertex_count = g.v().count();
    println!("Total vertices: {}", vertex_count);

    // Count all edges
    let edge_count = g.e().count();
    println!("Total edges: {}\n", edge_count);

    // Query: Get all person names
    let names: Vec<Value> = g.v().has_label("Person").values("name").to_list();
    println!("All people: {:?}", names);

    // Query: Find people in specific cities using has_value
    let new_yorkers: Vec<Value> = g
        .v()
        .has_label("Person")
        .has_value("city", Value::from("New York"))
        .values("name")
        .to_list();
    println!("People in New York: {:?}", new_yorkers);

    // Query: Find Alice's friends
    let alice_friends: Vec<Value> = g.v_id(alice_id).out_label("KNOWS").values("name").to_list();
    println!("Alice knows: {:?}", alice_friends);

    // Query: Find who knows Charlie
    let charlie_known_by: Vec<Value> = g
        .v_id(charlie_id)
        .in_label("KNOWS")
        .values("name")
        .to_list();
    println!("Charlie is known by: {:?}\n", charlie_known_by);

    // =========================================================================
    // Part 3: GQL Mutations - CREATE new nodes with edges
    // =========================================================================
    println!("--- Part 3: GQL Mutations ---\n");

    // Create a new person with an edge in a single GQL statement
    // (GQL CREATE creates new elements - both nodes and the edge between them)
    let results = graph
        .gql(
            r#"
            CREATE (d:Person {name: 'Diana', age: 28, city: 'Denver'})-[:FRIENDS_WITH {since: 2023}]->(e:Person {name: 'Eve', age: 27, city: 'Seattle'})
            RETURN d.name, e.name
            "#,
        )
        .expect("GQL CREATE failed");
    println!("Created via GQL: {:?}", results);

    // Update properties using GQL SET (matches existing nodes)
    graph
        .gql(
            r#"
            MATCH (b:Person {name: 'Bob'})
            SET b.title = 'Engineer', b.age = 26
            "#,
        )
        .expect("GQL SET failed");
    println!("Updated Bob's properties via GQL\n");

    // =========================================================================
    // Part 4: Verify Combined Results
    // =========================================================================
    println!("--- Part 4: Verify Combined Results ---\n");

    // Query using Gremlin to verify GQL changes
    let all_names: Vec<Value> = g.v().has_label("Person").values("name").to_list();
    println!("All people after GQL mutations: {:?}", all_names);

    // Count total (should be 5 now: Alice, Bob, Charlie, Diana, Eve)
    let final_count = g.v().count();
    println!("Total vertices: {}", final_count);

    // Find Alice's friends via Gremlin
    let alice_friends_updated: Vec<Value> =
        g.v_id(alice_id).out_label("KNOWS").values("name").to_list();
    println!("Alice knows: {:?}", alice_friends_updated);

    // Verify Bob's updated age
    let bob_age: Vec<Value> = g.v_id(bob_id).values("age").to_list();
    println!("Bob's updated age: {:?}", bob_age);

    // Check new FRIENDS_WITH edges created by GQL
    let friends_edges = g.e().has_label("FRIENDS_WITH").count();
    println!("FRIENDS_WITH edges: {}\n", friends_edges);

    // =========================================================================
    // Part 5: Mutations via Traversal (property updates, drops)
    // =========================================================================
    println!("--- Part 5: More Traversal Mutations ---\n");

    // Update property via traversal
    g.v_id(charlie_id).property("nickname", "Chuck").iterate();
    println!("Added nickname to Charlie");

    // Verify the property was added
    let charlie_props: Vec<Value> = g.v_id(charlie_id).values("nickname").to_list();
    println!("Charlie's nickname: {:?}", charlie_props);

    // Drop Bob's outgoing edges via traversal
    let bob_out_edges_before = g.v_id(bob_id).out_e().count();
    println!("Bob's outgoing edges before drop: {}", bob_out_edges_before);

    // Drop Bob's outgoing edges
    g.v_id(bob_id).out_e().drop().iterate();

    let bob_out_edges_after = g.v_id(bob_id).out_e().count();
    println!("Bob's outgoing edges after drop: {}", bob_out_edges_after);

    // =========================================================================
    // Part 6: GQL DELETE
    // =========================================================================
    println!("\n--- Part 6: GQL DELETE ---\n");

    // Delete Eve using GQL DETACH DELETE (removes vertex and connected edges)
    graph
        .gql(
            r#"
            MATCH (e:Person {name: 'Eve'})
            DETACH DELETE e
            "#,
        )
        .expect("GQL DELETE failed");
    println!("Deleted Eve via GQL DETACH DELETE");

    // Verify deletion
    let final_names: Vec<Value> = g.v().has_label("Person").values("name").to_list();
    println!("Remaining people: {:?}", final_names);
    println!("Final vertex count: {}", g.v().count());

    // =========================================================================
    // Summary
    // =========================================================================
    println!("\n=== Summary ===");
    println!("Demonstrated unified API where:");
    println!("  - g.add_v(), g.add_e() create vertices/edges (Gremlin)");
    println!("  - g.v(), g.out(), g.values() query the graph (Gremlin)");
    println!("  - graph.gql() runs GQL mutations (CREATE, SET, DELETE)");
    println!("  - Both APIs work on the same graph seamlessly");
    println!("  - No explicit transaction or mode switching required");
}
