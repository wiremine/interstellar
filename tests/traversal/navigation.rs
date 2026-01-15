//! Navigation step tests.

use crate::common::graphs::create_small_graph;

#[test]
fn out_traverses_to_outgoing_neighbors() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Alice has outgoing edges to Bob and GraphDB
    let neighbors = g.v_ids([tg.alice]).out().to_list();
    assert_eq!(neighbors.len(), 2);
}

#[test]
fn out_labels_filters_by_edge_label() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Alice knows Bob only
    let knows = g.v_ids([tg.alice]).out_labels(&["knows"]).to_list();
    assert_eq!(knows.len(), 1);

    // Alice uses GraphDB only
    let uses = g.v_ids([tg.alice]).out_labels(&["uses"]).to_list();
    assert_eq!(uses.len(), 1);
}

#[test]
fn in_traverses_to_incoming_neighbors() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Alice has incoming edge from Charlie
    let known_by = g.v_ids([tg.alice]).in_().to_list();
    assert_eq!(known_by.len(), 1);
}

#[test]
fn in_labels_filters_by_edge_label() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // GraphDB is used by Alice and Bob
    let used_by = g.v_ids([tg.graphdb]).in_labels(&["uses"]).to_list();
    assert_eq!(used_by.len(), 2);
}

#[test]
fn both_traverses_in_both_directions() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Alice has: out to Bob, out to GraphDB, in from Charlie
    let neighbors = g.v_ids([tg.alice]).both().to_list();
    assert_eq!(neighbors.len(), 3);
}

#[test]
fn both_labels_filters_by_edge_label() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Alice: knows->Bob, <-knows Charlie
    let knows = g.v_ids([tg.alice]).both_labels(&["knows"]).to_list();
    assert_eq!(knows.len(), 2);
}

#[test]
fn out_e_returns_outgoing_edges() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Alice has 2 outgoing edges
    let edges = g.v_ids([tg.alice]).out_e().to_list();
    assert_eq!(edges.len(), 2);
    for e in &edges {
        assert!(e.is_edge());
    }
}

#[test]
fn out_e_labels_filters_by_edge_label() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let knows_edges = g.v_ids([tg.alice]).out_e_labels(&["knows"]).to_list();
    assert_eq!(knows_edges.len(), 1);
}

#[test]
fn in_e_returns_incoming_edges() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Alice has 1 incoming edge (from Charlie)
    let edges = g.v_ids([tg.alice]).in_e().to_list();
    assert_eq!(edges.len(), 1);
}

#[test]
fn both_e_returns_all_incident_edges() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Alice has 3 incident edges (2 out, 1 in)
    let edges = g.v_ids([tg.alice]).both_e().to_list();
    assert_eq!(edges.len(), 3);
}

#[test]
fn out_v_returns_source_vertex_of_edge() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Get source vertices of all edges
    let sources = g.e().out_v().to_list();
    assert_eq!(sources.len(), 5);
    for s in &sources {
        assert!(s.is_vertex());
    }
}

#[test]
fn in_v_returns_target_vertex_of_edge() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Get target vertices of all edges
    let targets = g.e().in_v().to_list();
    assert_eq!(targets.len(), 5);
    for t in &targets {
        assert!(t.is_vertex());
    }
}

#[test]
fn both_v_returns_both_vertices_of_edge() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Each edge produces 2 vertices
    let vertices = g.e_ids([tg.alice_knows_bob]).both_v().to_list();
    assert_eq!(vertices.len(), 2);
}

#[test]
fn multi_hop_navigation() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Alice -> knows -> Bob -> knows -> Charlie
    let two_hops = g
        .v_ids([tg.alice])
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .to_list();
    assert_eq!(two_hops.len(), 1);
    // Should be Charlie
    assert_eq!(two_hops[0].as_vertex_id(), Some(tg.charlie));
}
