//! Mutation Steps Example
//!
//! This example demonstrates the graph mutation operations:
//! - Creating vertices with `add_v()` and `property()`
//! - Creating edges with `add_e()`, `from_vertex()`, `to_vertex()`
//! - Updating properties with `property()`
//! - Deleting elements with `drop()`
//! - Using `MutationExecutor` to apply pending mutations
//! - Using anonymous traversal factory functions for mutations
//!
//! Run with: `cargo run --example mutations`

use intersteller::graph::Graph;
use intersteller::storage::{GraphStorage, InMemoryGraph};
use intersteller::traversal::{MutationExecutor, __};

fn main() {
    println!("=== Intersteller Mutation Steps Example ===\n");

    // -------------------------------------------------------------------------
    // Step 1: Create an empty graph
    // -------------------------------------------------------------------------
    let mut storage = InMemoryGraph::new();
    println!("Created empty graph");
    println!("  Initial vertex count: {}", storage.vertex_count());
    println!("  Initial edge count: {}", storage.edge_count());
    println!();

    // -------------------------------------------------------------------------
    // Step 2: Create vertices using add_v() with property chaining
    // -------------------------------------------------------------------------
    println!("--- Creating vertices with add_v().property() ---");

    // Create Alice with properties using traversal API
    let alice_id = {
        // Use a temporary graph to generate the traversal
        let temp_graph = Graph::in_memory();
        let snapshot = temp_graph.snapshot();
        let g = snapshot.traversal();

        // Build the add_v traversal with chained properties
        let traversers: Vec<_> = g
            .add_v("person")
            .property("name", "Alice")
            .property("age", 30i64)
            .property("city", "New York")
            .execute()
            .collect();

        // Execute mutations on actual storage
        let mut executor = MutationExecutor::new(&mut storage);
        let result = executor.execute(traversers.into_iter());

        println!("Created Alice:");
        println!("  Vertices added: {}", result.vertices_added);

        // Get the created vertex ID
        result
            .values
            .first()
            .and_then(|v| v.as_vertex_id())
            .expect("Should have created a vertex")
    };
    println!("  Alice's ID: {:?}", alice_id);

    // Create Bob
    let bob_id = {
        let temp_graph = Graph::in_memory();
        let snapshot = temp_graph.snapshot();
        let g = snapshot.traversal();

        let traversers: Vec<_> = g
            .add_v("person")
            .property("name", "Bob")
            .property("age", 25i64)
            .property("city", "Boston")
            .execute()
            .collect();

        let mut executor = MutationExecutor::new(&mut storage);
        let result = executor.execute(traversers.into_iter());

        println!("Created Bob:");
        println!("  Vertices added: {}", result.vertices_added);

        result
            .values
            .first()
            .and_then(|v| v.as_vertex_id())
            .expect("Should have created a vertex")
    };
    println!("  Bob's ID: {:?}", bob_id);

    // Create a software vertex
    let software_id = {
        let temp_graph = Graph::in_memory();
        let snapshot = temp_graph.snapshot();
        let g = snapshot.traversal();

        let traversers: Vec<_> = g
            .add_v("software")
            .property("name", "Intersteller")
            .property("version", "1.0")
            .property("language", "Rust")
            .execute()
            .collect();

        let mut executor = MutationExecutor::new(&mut storage);
        let result = executor.execute(traversers.into_iter());

        println!("Created Intersteller software:");
        println!("  Vertices added: {}", result.vertices_added);

        result
            .values
            .first()
            .and_then(|v| v.as_vertex_id())
            .expect("Should have created a vertex")
    };
    println!("  Software ID: {:?}", software_id);

    println!();
    println!("After creating vertices:");
    println!("  Vertex count: {}", storage.vertex_count());
    println!();

    // -------------------------------------------------------------------------
    // Step 3: Create edges using add_e() with from/to endpoints
    // -------------------------------------------------------------------------
    println!("--- Creating edges with add_e().from_vertex().to_vertex() ---");

    // Create "knows" edge from Alice to Bob
    let knows_edge_id = {
        let temp_graph = Graph::in_memory();
        let snapshot = temp_graph.snapshot();
        let g = snapshot.traversal();

        // Use build() to get a BoundTraversal, then execute() on that
        let traversers: Vec<_> = g
            .add_e("knows")
            .from_vertex(alice_id)
            .to_vertex(bob_id)
            .property("since", 2020i64)
            .property("relationship", "friends")
            .build()
            .execute()
            .collect();

        let mut executor = MutationExecutor::new(&mut storage);
        let result = executor.execute(traversers.into_iter());

        println!("Created 'knows' edge (Alice -> Bob):");
        println!("  Edges added: {}", result.edges_added);

        result.values.first().and_then(|v| v.as_edge_id())
    };
    println!("  Edge ID: {:?}", knows_edge_id);

    // Create "uses" edges
    let _alice_uses_edge = {
        let temp_graph = Graph::in_memory();
        let snapshot = temp_graph.snapshot();
        let g = snapshot.traversal();

        let traversers: Vec<_> = g
            .add_e("uses")
            .from_vertex(alice_id)
            .to_vertex(software_id)
            .property("skill_level", "expert")
            .build()
            .execute()
            .collect();

        let mut executor = MutationExecutor::new(&mut storage);
        let result = executor.execute(traversers.into_iter());

        println!("Created 'uses' edge (Alice -> Intersteller):");
        println!("  Edges added: {}", result.edges_added);

        result.values.first().and_then(|v| v.as_edge_id())
    };

    let bob_uses_edge = {
        let temp_graph = Graph::in_memory();
        let snapshot = temp_graph.snapshot();
        let g = snapshot.traversal();

        let traversers: Vec<_> = g
            .add_e("uses")
            .from_vertex(bob_id)
            .to_vertex(software_id)
            .property("skill_level", "beginner")
            .build()
            .execute()
            .collect();

        let mut executor = MutationExecutor::new(&mut storage);
        let result = executor.execute(traversers.into_iter());

        println!("Created 'uses' edge (Bob -> Intersteller):");
        println!("  Edges added: {}", result.edges_added);

        result.values.first().and_then(|v| v.as_edge_id())
    };

    println!();
    println!("After creating edges:");
    println!("  Edge count: {}", storage.edge_count());
    println!();

    // -------------------------------------------------------------------------
    // Step 4: Query the graph to verify data
    // -------------------------------------------------------------------------
    println!("--- Querying the graph to verify ---");

    // Create a fresh graph view for querying
    // Note: We need to wrap our storage in Arc for the Graph API
    // For this example, we'll query directly from storage
    let all_vertices: Vec<_> = storage.all_vertices().collect();
    println!("All vertices:");
    for v in &all_vertices {
        println!("  {:?}: {} - {:?}", v.id, v.label, v.properties.get("name"));
    }

    let all_edges: Vec<_> = storage.all_edges().collect();
    println!("All edges:");
    for e in &all_edges {
        println!("  {:?}: {} ({:?} -> {:?})", e.id, e.label, e.src, e.dst);
    }
    println!();

    // -------------------------------------------------------------------------
    // Step 5: Update properties on existing elements
    // -------------------------------------------------------------------------
    println!("--- Updating properties with property() ---");

    // Get Alice's current age
    let current_age = storage
        .get_vertex(alice_id)
        .and_then(|v| v.properties.get("age").cloned());
    println!("Alice's current age: {:?}", current_age);

    // Update Alice's age using traversal + property step
    {
        let temp_graph = Graph::in_memory();
        let snapshot = temp_graph.snapshot();
        let g = snapshot.traversal();

        let traversers: Vec<_> = g
            .v_ids([alice_id])
            .property("age", 31i64)
            .property("status", "active")
            .execute()
            .collect();

        let mut executor = MutationExecutor::new(&mut storage);
        let result = executor.execute(traversers.into_iter());

        println!("Updated Alice's properties:");
        println!("  Properties set: {}", result.properties_set);
    }

    // Verify the update
    let updated = storage.get_vertex(alice_id);
    if let Some(alice) = updated {
        println!("Alice's new age: {:?}", alice.properties.get("age"));
        println!("Alice's new status: {:?}", alice.properties.get("status"));
    }
    println!();

    // -------------------------------------------------------------------------
    // Step 6: Demonstrate anonymous traversal mutation factories
    // -------------------------------------------------------------------------
    println!("--- Using __::add_v(), __::property(), __::drop() factories ---");

    // The __ module provides factory functions for creating anonymous
    // mutation traversals that can be composed with other steps

    // Create an anonymous add_v traversal (for demonstration)
    let _add_person = __::add_v("person")
        .property("name", "Anonymous")
        .property("age", 99i64);
    println!("Created anonymous add_v traversal with 2 properties");

    // Create an anonymous property traversal
    let _set_status = __::property("verified", true);
    println!("Created anonymous property traversal");

    // Create an anonymous drop traversal
    let _delete_step = __::drop();
    println!("Created anonymous drop traversal");
    println!();

    // -------------------------------------------------------------------------
    // Step 7: Delete elements with drop()
    // -------------------------------------------------------------------------
    println!("--- Deleting elements with drop() ---");

    println!("Before deletion:");
    println!("  Vertex count: {}", storage.vertex_count());
    println!("  Edge count: {}", storage.edge_count());

    // Delete Bob's "uses" edge
    if let Some(edge_id) = bob_uses_edge {
        let temp_graph = Graph::in_memory();
        let snapshot = temp_graph.snapshot();
        let g = snapshot.traversal();

        let traversers: Vec<_> = g.e_ids([edge_id]).drop().execute().collect();

        let mut executor = MutationExecutor::new(&mut storage);
        let result = executor.execute(traversers.into_iter());

        println!("Dropped Bob's 'uses' edge:");
        println!("  Edges removed: {}", result.edges_removed);
    }

    println!();
    println!("After dropping edge:");
    println!("  Vertex count: {}", storage.vertex_count());
    println!("  Edge count: {}", storage.edge_count());

    // Delete Bob vertex (this should also clean up incident edges in the storage)
    {
        let temp_graph = Graph::in_memory();
        let snapshot = temp_graph.snapshot();
        let g = snapshot.traversal();

        let traversers: Vec<_> = g.v_ids([bob_id]).drop().execute().collect();

        let mut executor = MutationExecutor::new(&mut storage);
        let result = executor.execute(traversers.into_iter());

        println!("Dropped Bob vertex:");
        println!("  Vertices removed: {}", result.vertices_removed);
    }

    println!();
    println!("After dropping Bob:");
    println!("  Vertex count: {}", storage.vertex_count());
    println!("  Edge count: {}", storage.edge_count());

    // Verify Bob is gone
    let bob_exists = storage.get_vertex(bob_id).is_some();
    println!("  Bob still exists: {}", bob_exists);

    // -------------------------------------------------------------------------
    // Step 8: Summary of mutation workflow
    // -------------------------------------------------------------------------
    println!();
    println!("=== Mutation Workflow Summary ===");
    println!();
    println!("1. Build traversal with mutation steps:");
    println!("   g.add_v(\"label\").property(\"key\", value)");
    println!("   g.add_e(\"label\").from_vertex(id).to_vertex(id).build()");
    println!("   g.v_ids([id]).property(\"key\", value)");
    println!("   g.v_ids([id]).drop()");
    println!();
    println!("2. Execute traversal to get pending mutations:");
    println!("   let traversers: Vec<_> = traversal.execute().collect();");
    println!();
    println!("3. Apply mutations with MutationExecutor:");
    println!("   let mut executor = MutationExecutor::new(&mut storage);");
    println!("   let result = executor.execute(traversers.into_iter());");
    println!();
    println!("4. Check results:");
    println!("   result.vertices_added, result.edges_added,");
    println!("   result.vertices_removed, result.edges_removed,");
    println!("   result.properties_set, result.values");
    println!();

    println!("=== Example Complete ===");
}
