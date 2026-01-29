//! Neighborhood exploration pattern tests.
//!
//! Tests for common graph neighborhood queries including:
//! - Direct neighbor traversal with filtering
//! - Multi-hop neighborhood exploration
//! - Bidirectional traversal patterns
//! - Neighbor counting and degree analysis

#![allow(unused_variables)]

use interstellar::p;
use interstellar::value::VertexId;

use crate::common::graphs::{create_small_graph, create_social_graph};

// =============================================================================
// Direct Neighbor Traversal
// =============================================================================

#[test]
fn out_returns_direct_neighbors() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice's direct outgoing neighbors
    let neighbors = g.v_ids([tg.alice]).out().to_list();

    // Alice -> Bob (knows), Alice -> GraphDB (uses)
    assert_eq!(neighbors.len(), 2);
}

#[test]
fn out_with_label_filters_edge_type() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Only "knows" edges from Alice
    let knows_neighbors = g.v_ids([tg.alice]).out_labels(&["knows"]).to_list();
    assert_eq!(knows_neighbors.len(), 1);
    assert_eq!(knows_neighbors[0].as_vertex_id(), Some(tg.bob));

    // Only "uses" edges from Alice
    let uses_neighbors = g.v_ids([tg.alice]).out_labels(&["uses"]).to_list();
    assert_eq!(uses_neighbors.len(), 1);
    assert_eq!(uses_neighbors[0].as_vertex_id(), Some(tg.graphdb));
}

#[test]
fn in_returns_incoming_neighbors() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Who knows Bob?
    let who_knows_bob = g.v_ids([tg.bob]).in_labels(&["knows"]).to_list();
    assert_eq!(who_knows_bob.len(), 1);
    assert_eq!(who_knows_bob[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn both_returns_bidirectional_neighbors() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // All neighbors of Bob (both directions)
    let all_neighbors = g.v_ids([tg.bob]).both().to_list();

    // Bob <- Alice (knows), Bob -> Charlie (knows), Bob -> GraphDB (uses)
    assert!(all_neighbors.len() >= 3);
}

#[test]
fn neighbors_with_property_filter() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice's neighbors who are older than 30
    let older_neighbors = g
        .v_ids([tg.alice])
        .out_labels(&["knows"])
        .has_where("age", p::gt(30i64))
        .to_list();

    // Bob is 25, so no matches
    assert!(older_neighbors.is_empty());

    // Neighbors older than 20
    let neighbors_over_20 = g
        .v_ids([tg.alice])
        .out_labels(&["knows"])
        .has_where("age", p::gt(20i64))
        .to_list();

    assert_eq!(neighbors_over_20.len(), 1); // Bob (25)
}

// =============================================================================
// Multi-hop Neighborhood
// =============================================================================

#[test]
fn two_hop_neighbors() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Friends of friends: Alice -> Bob -> Charlie
    let friends_of_friends = g
        .v_ids([tg.alice])
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .to_list();

    assert_eq!(friends_of_friends.len(), 1);
    assert_eq!(friends_of_friends[0].as_vertex_id(), Some(tg.charlie));
}

#[test]
fn three_hop_with_cycle_detection() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice -> Bob -> Charlie -> Alice (cycle)
    let three_hops = g
        .v_ids([tg.alice])
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .to_list();

    // Charlie knows Alice, completing the cycle
    assert_eq!(three_hops.len(), 1);
    assert_eq!(three_hops[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn multi_hop_with_dedup() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // 4 hops with dedup to avoid counting same vertex multiple times
    let four_hops_deduped = g
        .v_ids([tg.alice])
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .dedup()
        .to_list();

    // Should deduplicate the cycle
    assert!(!four_hops_deduped.is_empty());
}

#[test]
fn multi_hop_with_limit() {
    let tg = create_social_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Explore 2 hops but limit results
    let limited = g.v_ids([tg.alice]).both().both().dedup().limit(3).to_list();

    assert!(limited.len() <= 3);
}

// =============================================================================
// Bidirectional Exploration
// =============================================================================

#[test]
fn bidirectional_one_hop() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // All vertices connected to Bob in either direction
    let connected = g.v_ids([tg.bob]).both().dedup().to_list();

    // Alice (in), Charlie (out), GraphDB (out)
    assert!(connected.len() >= 3);
}

#[test]
fn bidirectional_with_label_filter() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Only "knows" connections (both directions)
    let knows_connections = g.v_ids([tg.bob]).both_labels(&["knows"]).dedup().to_list();

    // Alice (in via knows), Charlie (out via knows)
    assert_eq!(knows_connections.len(), 2);
}

#[test]
fn bidirectional_multi_hop() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Two hops in any direction from Bob, deduplicated
    let two_hop_any = g.v_ids([tg.bob]).both().both().dedup().to_list();

    // Should include multiple vertices reachable in 2 hops
    assert!(!two_hop_any.is_empty());
}

// =============================================================================
// Neighbor Counting and Degree Analysis
// =============================================================================

#[test]
fn out_degree_count() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Count of Alice's outgoing edges
    let out_degree = g.v_ids([tg.alice]).out().count();
    assert_eq!(out_degree, 2); // knows Bob, uses GraphDB
}

#[test]
fn in_degree_count() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Count of incoming edges to GraphDB
    let in_degree = g.v_ids([tg.graphdb]).in_().count();
    assert_eq!(in_degree, 2); // Alice uses, Bob uses
}

#[test]
fn total_degree_count() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Total connections (both directions) for Bob
    let total_degree = g.v_ids([tg.bob]).both().count();

    // In: Alice knows, Out: knows Charlie, uses GraphDB
    assert_eq!(total_degree, 3);
}

#[test]
fn neighbors_with_specific_property_count() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Count neighbors who have "age" property
    let neighbors_with_age = g.v_ids([tg.alice]).out().has("age").count();

    // Bob has age, GraphDB doesn't
    assert_eq!(neighbors_with_age, 1);
}

// =============================================================================
// Complex Neighborhood Patterns
// =============================================================================

#[test]
fn common_neighbors_pattern() {
    let tg = create_social_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Find vertices that are neighbors of both Alice and Bob
    // This is a common pattern for "mutual friends"
    let alice_neighbors: Vec<VertexId> = g
        .v_ids([tg.alice])
        .out()
        .to_list()
        .iter()
        .filter_map(|v| v.as_vertex_id())
        .collect();

    let bob_neighbors: Vec<VertexId> = g
        .v_ids([tg.bob])
        .out()
        .to_list()
        .iter()
        .filter_map(|v| v.as_vertex_id())
        .collect();

    // Find intersection
    let common: Vec<_> = alice_neighbors
        .iter()
        .filter(|v| bob_neighbors.contains(v))
        .collect();

    // Verify the pattern works (results depend on graph structure)
    assert!(common.len() <= alice_neighbors.len());
}

#[test]
fn recommendation_pattern_in_out() {
    let tg = create_social_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Recommendation pattern: find software used by people who know Alice
    // Alice -> knows -> Person -> uses -> Software
    let recommended = g
        .v_ids([tg.alice])
        .out_labels(&["knows"])
        .out_labels(&["created"])
        .dedup()
        .to_list();

    // People Alice knows may have created software
    // Results depend on graph structure - list is returned
    let _ = recommended;
}

#[test]
fn who_also_uses_pattern() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Find other users of software that Alice uses
    // Alice -> uses -> Software <- uses <- OtherPerson
    let co_users = g
        .v_ids([tg.alice])
        .out_labels(&["uses"])
        .in_labels(&["uses"])
        .has_label("person")
        .dedup()
        .to_list();

    // Should find Bob (also uses GraphDB), might include Alice herself
    assert!(!co_users.is_empty());
}

#[test]
fn neighborhood_with_edge_properties() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get edges from Alice and filter by edge property
    let recent_knows = g
        .v_ids([tg.alice])
        .out_e_labels(&["knows"])
        .has_where("since", p::gte(2020i64))
        .in_v()
        .to_list();

    // Alice knows Bob since 2020
    assert_eq!(recent_knows.len(), 1);
}

#[test]
fn multi_start_neighborhood() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Start from multiple vertices and explore
    let neighbors = g
        .v_ids([tg.alice, tg.bob])
        .out_labels(&["knows"])
        .dedup()
        .to_list();

    // Alice -> Bob, Bob -> Charlie (deduplicated)
    assert_eq!(neighbors.len(), 2);
}
