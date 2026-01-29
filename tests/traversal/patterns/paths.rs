//! Path tracking and analysis pattern tests.
//!
//! Tests for path-related traversal patterns including:
//! - Basic path tracking with path()
//! - Labeled paths with as_() and select()
//! - Path length analysis
//! - Path filtering and deduplication

#![allow(unused_variables)]

use interstellar::p;
use interstellar::value::Value;

use crate::common::graphs::{create_small_graph, create_social_graph};

// =============================================================================
// Basic Path Tracking
// =============================================================================

#[test]
fn path_tracks_traversal_history() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Track path through Alice -> Bob -> Charlie
    // Note: with_path() must be called to enable path tracking
    let paths = g
        .v_ids([tg.alice])
        .with_path()
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .path()
        .to_list();

    assert_eq!(paths.len(), 1);

    // Path should contain 3 elements: Alice, Bob, Charlie
    if let Value::List(path) = &paths[0] {
        assert_eq!(path.len(), 3);
    } else {
        panic!("Expected path to be a List");
    }
}

#[test]
fn path_with_single_vertex() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Path with just the starting vertex
    let paths = g.v_ids([tg.alice]).with_path().path().to_list();

    assert_eq!(paths.len(), 1);
    if let Value::List(path) = &paths[0] {
        assert_eq!(path.len(), 1);
    }
}

#[test]
fn path_through_multiple_hops() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Longer path: Alice -> Bob -> Charlie -> Alice (cycle)
    let paths = g
        .v_ids([tg.alice])
        .with_path()
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .path()
        .to_list();

    assert_eq!(paths.len(), 1);
    if let Value::List(path) = &paths[0] {
        assert_eq!(path.len(), 4); // Alice, Bob, Charlie, Alice
    }
}

#[test]
fn path_from_multiple_starting_vertices() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Start from both Alice and Bob
    let paths = g
        .v_ids([tg.alice, tg.bob])
        .with_path()
        .out_labels(&["knows"])
        .path()
        .to_list();

    // Should have 2 paths: Alice->Bob, Bob->Charlie
    assert_eq!(paths.len(), 2);
}

// =============================================================================
// Labeled Paths with as_() and select()
// =============================================================================

#[test]
fn as_labels_step_in_path() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Label vertices in the path
    let results = g
        .v_ids([tg.alice])
        .with_path()
        .as_("start")
        .out_labels(&["knows"])
        .as_("friend")
        .select(&["start", "friend"])
        .to_list();

    assert_eq!(results.len(), 1);

    // Result should be a map with "start" and "friend" keys
    if let Value::Map(map) = &results[0] {
        assert!(map.contains_key("start"));
        assert!(map.contains_key("friend"));
    } else {
        panic!("Expected Map result from select");
    }
}

#[test]
fn select_single_label() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Select just one labeled step
    let results = g
        .v_ids([tg.alice])
        .with_path()
        .as_("person")
        .out_labels(&["knows"])
        .select_one("person")
        .to_list();

    assert_eq!(results.len(), 1);
    // Should return Alice (the labeled vertex)
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn multiple_labels_in_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Three labeled steps
    let results = g
        .v_ids([tg.alice])
        .with_path()
        .as_("a")
        .out_labels(&["knows"])
        .as_("b")
        .out_labels(&["knows"])
        .as_("c")
        .select(&["a", "b", "c"])
        .to_list();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert_eq!(map.len(), 3);
        assert!(map.contains_key("a"));
        assert!(map.contains_key("b"));
        assert!(map.contains_key("c"));
    }
}

#[test]
fn labeled_path_with_property_extraction() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Label and then get property values
    let names = g
        .v_ids([tg.alice])
        .as_("person")
        .out_labels(&["knows"])
        .values("name")
        .to_list();

    assert_eq!(names.len(), 1);
    assert_eq!(names[0], Value::String("Bob".to_string()));
}

// =============================================================================
// Path with Filtering
// =============================================================================

#[test]
fn path_after_filter() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter then track path
    let paths = g
        .v_ids([tg.alice])
        .with_path()
        .out_labels(&["knows"])
        .has_where("age", p::lt(30i64))
        .path()
        .to_list();

    // Only Bob (age 25) passes filter
    assert_eq!(paths.len(), 1);
    if let Value::List(path) = &paths[0] {
        assert_eq!(path.len(), 2); // Alice, Bob
    }
}

#[test]
fn path_with_dedup() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Multiple hops with dedup - path shows unique traversal
    let paths = g
        .v_ids([tg.alice])
        .with_path()
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .dedup()
        .path()
        .to_list();

    // Even with dedup, path shows the full traversal to each unique endpoint
    assert!(!paths.is_empty());
}

#[test]
fn path_with_limit() {
    let tg = create_social_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Limit number of paths returned
    let paths = g
        .v_ids([tg.alice])
        .with_path()
        .out()
        .out()
        .path()
        .limit(2)
        .to_list();

    assert!(paths.len() <= 2);
}

// =============================================================================
// Path Analysis
// =============================================================================

#[test]
fn count_paths() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Count number of distinct paths
    let path_count = g.v_ids([tg.alice]).with_path().out().out().path().count();

    // Alice has 2 outgoing edges, each destination may have more
    assert!(path_count > 0);
}

#[test]
fn path_contains_specific_vertex() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get all paths and check programmatically
    let paths = g
        .v_ids([tg.alice])
        .with_path()
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .path()
        .to_list();

    // Verify Bob is in the path
    for path_val in &paths {
        if let Value::List(path) = path_val {
            let has_bob = path.iter().any(|v| v.as_vertex_id() == Some(tg.bob));
            assert!(has_bob, "Path should contain Bob");
        }
    }
}

// =============================================================================
// Edge Paths
// =============================================================================

#[test]
fn path_with_edges() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Path through edges
    let paths = g
        .v_ids([tg.alice])
        .with_path()
        .out_e_labels(&["knows"])
        .in_v()
        .path()
        .to_list();

    assert_eq!(paths.len(), 1);
    if let Value::List(path) = &paths[0] {
        // Path: Alice, Edge, Bob
        assert_eq!(path.len(), 3);
    }
}

#[test]
fn path_alternating_vertices_and_edges() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Full path with vertices and edges
    let paths = g
        .v_ids([tg.alice])
        .with_path()
        .out_e_labels(&["knows"])
        .in_v()
        .out_e_labels(&["knows"])
        .in_v()
        .path()
        .to_list();

    assert_eq!(paths.len(), 1);
    if let Value::List(path) = &paths[0] {
        // Alice -> Edge -> Bob -> Edge -> Charlie
        assert_eq!(path.len(), 5);
    }
}

// =============================================================================
// Complex Path Patterns
// =============================================================================

#[test]
fn path_with_bidirectional_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Path using both directions
    let paths = g
        .v_ids([tg.bob])
        .with_path()
        .both_labels(&["knows"])
        .path()
        .to_list();

    // Bob has incoming from Alice and outgoing to Charlie
    assert_eq!(paths.len(), 2);
}

#[test]
fn path_in_cyclic_graph() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Complete cycle: Alice -> Bob -> Charlie -> Alice
    let paths = g
        .v_ids([tg.alice])
        .with_path()
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .path()
        .to_list();

    assert_eq!(paths.len(), 1);
    if let Value::List(path) = &paths[0] {
        // First and last should both be Alice
        assert_eq!(path.first().and_then(|v| v.as_vertex_id()), Some(tg.alice));
        assert_eq!(path.last().and_then(|v| v.as_vertex_id()), Some(tg.alice));
    }
}

#[test]
fn empty_path_from_no_results() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // GraphDB has no outgoing edges
    let paths = g.v_ids([tg.graphdb]).with_path().out().path().to_list();

    assert!(paths.is_empty());
}

#[test]
fn path_preserves_traversal_order() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let paths = g
        .v_ids([tg.alice])
        .with_path()
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .path()
        .to_list();

    if let Value::List(path) = &paths[0] {
        // Order must be: Alice, Bob, Charlie
        assert_eq!(path[0].as_vertex_id(), Some(tg.alice));
        assert_eq!(path[1].as_vertex_id(), Some(tg.bob));
        assert_eq!(path[2].as_vertex_id(), Some(tg.charlie));
    }
}
