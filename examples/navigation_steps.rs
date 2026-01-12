//! Navigation Steps Example
//!
//! This example demonstrates graph navigation steps:
//!
//! Vertex-to-Vertex navigation:
//! - `out()`, `out_labels()` - Follow outgoing edges to target vertices
//! - `in_()`, `in_labels()` - Follow incoming edges to source vertices
//! - `both()`, `both_labels()` - Follow edges in both directions
//!
//! Vertex-to-Edge navigation:
//! - `out_e()`, `out_e_labels()` - Get outgoing edges
//! - `in_e()`, `in_e_labels()` - Get incoming edges
//! - `both_e()`, `both_e_labels()` - Get all incident edges
//!
//! Edge-to-Vertex navigation:
//! - `out_v()` - Get source vertex of an edge
//! - `in_v()` - Get target vertex of an edge
//! - `both_v()` - Get both vertices of an edge
//!
//! Run with: `cargo run --example navigation_steps`

use intersteller::graph::Graph;
use intersteller::storage::InMemoryGraph;
use intersteller::value::{Value, VertexId};
use std::collections::HashMap;
use std::sync::Arc;

fn main() {
    println!("=== Intersteller Navigation Steps Example ===\n");

    // Create test graph
    let (graph, vertices) = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    let alice = vertices.alice;
    let bob = vertices.bob;

    println!("Graph structure:");
    println!("  Alice --knows--> Bob --knows--> Charlie");
    println!("  Alice --uses--> GraphDB");
    println!("  Bob --uses--> GraphDB");
    println!("  Charlie --knows--> Alice (cycle)");
    println!();

    // -------------------------------------------------------------------------
    // out() - Follow outgoing edges to target vertices
    // -------------------------------------------------------------------------
    println!("--- out() - Outgoing neighbors ---");
    let alice_out = g.v_ids([alice]).out().to_list();
    println!("Alice's outgoing neighbors: {} vertices", alice_out.len());
    for v in &alice_out {
        println!("  {:?}", v.as_vertex_id());
    }

    let all_out = g.v().out().to_list();
    println!(
        "All vertices' outgoing neighbors: {} (with duplicates)",
        all_out.len()
    );
    println!();

    // -------------------------------------------------------------------------
    // out_labels() - Follow specific edge labels
    // -------------------------------------------------------------------------
    println!("--- out_labels() - Follow specific edges ---");
    let alice_knows = g.v_ids([alice]).out_labels(&["knows"]).to_list();
    println!("Alice --knows-->: {} vertices", alice_knows.len());

    let alice_uses = g.v_ids([alice]).out_labels(&["uses"]).to_list();
    println!("Alice --uses-->: {} vertices", alice_uses.len());

    // Multiple labels
    let alice_all = g.v_ids([alice]).out_labels(&["knows", "uses"]).to_list();
    println!("Alice --knows|uses-->: {} vertices", alice_all.len());
    println!();

    // -------------------------------------------------------------------------
    // in_() - Follow incoming edges to source vertices
    // -------------------------------------------------------------------------
    println!("--- in_() - Incoming neighbors ---");
    let bob_in = g.v_ids([bob]).in_().to_list();
    println!(
        "Bob's incoming neighbors (who knows Bob): {} vertices",
        bob_in.len()
    );

    let alice_in = g.v_ids([alice]).in_().to_list();
    println!("Alice's incoming neighbors: {} vertices", alice_in.len());
    println!();

    // -------------------------------------------------------------------------
    // in_labels() - Follow specific incoming edge labels
    // -------------------------------------------------------------------------
    println!("--- in_labels() - Incoming via specific edges ---");
    let graphdb = vertices.graph_db;
    let graphdb_users = g.v_ids([graphdb]).in_labels(&["uses"]).to_list();
    println!("Who uses GraphDB: {} vertices", graphdb_users.len());
    println!();

    // -------------------------------------------------------------------------
    // both() - Follow edges in both directions
    // -------------------------------------------------------------------------
    println!("--- both() - Neighbors in both directions ---");
    let bob_both = g.v_ids([bob]).both().to_list();
    println!("Bob's neighbors (in + out): {} vertices", bob_both.len());

    // Note: both() can return duplicates if there are edges in both directions
    let bob_both_dedup = g.v_ids([bob]).both().dedup().to_list();
    println!(
        "Bob's neighbors (deduped): {} vertices",
        bob_both_dedup.len()
    );
    println!();

    // -------------------------------------------------------------------------
    // both_labels() - Both directions with specific labels
    // -------------------------------------------------------------------------
    println!("--- both_labels() - Bidirectional via specific edges ---");
    let bob_knows_both = g.v_ids([bob]).both_labels(&["knows"]).to_list();
    println!("Bob <--knows-->: {} vertices", bob_knows_both.len());
    println!();

    // -------------------------------------------------------------------------
    // out_e() - Get outgoing edges
    // -------------------------------------------------------------------------
    println!("--- out_e() - Outgoing edges ---");
    let alice_out_edges = g.v_ids([alice]).out_e().to_list();
    println!("Alice's outgoing edges: {}", alice_out_edges.len());
    for e in &alice_out_edges {
        println!("  {:?}", e.as_edge_id());
    }
    println!();

    // -------------------------------------------------------------------------
    // out_e_labels() - Outgoing edges with specific labels
    // -------------------------------------------------------------------------
    println!("--- out_e_labels() - Specific outgoing edges ---");
    let alice_knows_edges = g.v_ids([alice]).out_e_labels(&["knows"]).to_list();
    println!("Alice's 'knows' edges: {}", alice_knows_edges.len());
    println!();

    // -------------------------------------------------------------------------
    // in_e() - Get incoming edges
    // -------------------------------------------------------------------------
    println!("--- in_e() - Incoming edges ---");
    let bob_in_edges = g.v_ids([bob]).in_e().to_list();
    println!("Bob's incoming edges: {}", bob_in_edges.len());
    println!();

    // -------------------------------------------------------------------------
    // in_e_labels() - Incoming edges with specific labels
    // -------------------------------------------------------------------------
    println!("--- in_e_labels() - Specific incoming edges ---");
    let graphdb_uses_edges = g.v_ids([graphdb]).in_e_labels(&["uses"]).to_list();
    println!(
        "Edges pointing to GraphDB via 'uses': {}",
        graphdb_uses_edges.len()
    );
    println!();

    // -------------------------------------------------------------------------
    // both_e() - All incident edges
    // -------------------------------------------------------------------------
    println!("--- both_e() - All incident edges ---");
    let bob_all_edges = g.v_ids([bob]).both_e().to_list();
    println!("Bob's incident edges (in + out): {}", bob_all_edges.len());
    println!();

    // -------------------------------------------------------------------------
    // both_e_labels() - Incident edges with specific labels
    // -------------------------------------------------------------------------
    println!("--- both_e_labels() - Specific incident edges ---");
    let bob_knows_edges = g.v_ids([bob]).both_e_labels(&["knows"]).to_list();
    println!("Bob's 'knows' edges (in + out): {}", bob_knows_edges.len());
    println!();

    // -------------------------------------------------------------------------
    // out_v() - Get source vertex of edges
    // -------------------------------------------------------------------------
    println!("--- out_v() - Edge source vertices ---");
    let edge_sources = g.e().has_label("knows").out_v().to_list();
    println!("Source vertices of 'knows' edges: {}", edge_sources.len());
    println!();

    // -------------------------------------------------------------------------
    // in_v() - Get target vertex of edges
    // -------------------------------------------------------------------------
    println!("--- in_v() - Edge target vertices ---");
    let edge_targets = g.e().has_label("knows").in_v().to_list();
    println!("Target vertices of 'knows' edges: {}", edge_targets.len());
    println!();

    // -------------------------------------------------------------------------
    // both_v() - Get both vertices of edges
    // -------------------------------------------------------------------------
    println!("--- both_v() - Both edge vertices ---");
    let edge_endpoints = g.e().has_label("uses").both_v().to_list();
    println!(
        "Vertices connected by 'uses' edges: {} (2 per edge)",
        edge_endpoints.len()
    );
    println!();

    // -------------------------------------------------------------------------
    // Chained navigation - Multi-hop traversals
    // -------------------------------------------------------------------------
    println!("--- Chained navigation ---");

    // Two-hop: friends of friends
    let fof = g
        .v_ids([alice])
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .to_list();
    println!("Alice's friends of friends: {} vertices", fof.len());

    // Three-hop with dedup
    let three_hop = g.v_ids([alice]).out().out().out().dedup().to_list();
    println!(
        "Three hops from Alice (deduped): {} vertices",
        three_hop.len()
    );

    // Navigate vertex -> edge -> vertex
    let via_edges = g.v_ids([alice]).out_e_labels(&["knows"]).in_v().to_list();
    println!(
        "Alice --knows(edge)--> target: {} vertices",
        via_edges.len()
    );
    println!();

    // -------------------------------------------------------------------------
    // Combining navigation with filters
    // -------------------------------------------------------------------------
    println!("--- Navigation + Filters ---");

    // Find people who use software
    let software_users = g
        .v()
        .has_label("software")
        .in_labels(&["uses"])
        .has_label("person")
        .dedup()
        .to_list();
    println!("People who use software: {} vertices", software_users.len());

    // Find unique neighbors of people
    let people_neighbors = g.v().has_label("person").out().dedup().to_list();
    println!(
        "Unique neighbors of all people: {} vertices",
        people_neighbors.len()
    );
    println!();

    println!("=== Example Complete ===");
}

/// Vertex IDs for easy reference
struct VertexIds {
    alice: VertexId,
    bob: VertexId,
    #[allow(dead_code)]
    charlie: VertexId,
    graph_db: VertexId,
}

/// Create a test graph with people and software
fn create_test_graph() -> (Graph, VertexIds) {
    let mut storage = InMemoryGraph::new();

    // Add person vertices
    let alice = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props
    });

    let bob = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        props
    });

    let charlie = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Charlie".to_string()));
        props
    });

    // Add software vertex
    let graph_db = storage.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("GraphDB".to_string()));
        props
    });

    // Add edges:
    // Alice --knows--> Bob --knows--> Charlie --knows--> Alice (cycle)
    // Alice --uses--> GraphDB
    // Bob --uses--> GraphDB
    storage
        .add_edge(alice, bob, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, charlie, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(charlie, alice, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, graph_db, "uses", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, graph_db, "uses", HashMap::new())
        .unwrap();

    (
        Graph::new(storage),
        VertexIds {
            alice,
            bob,
            charlie,
            graph_db,
        },
    )
}
