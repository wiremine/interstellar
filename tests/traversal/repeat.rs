//! Repeat step tests

#![allow(unused_variables)]
use interstellar::traversal::__;
use interstellar::value::{Value, VertexId};

use crate::common::graphs::create_small_graph;

#[test]
fn repeat_out_compiles_with_anonymous_traversal() {
    // Acceptance criteria: g.v().repeat(__.out()) compiles
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // This should compile - the main acceptance criteria
    let _results = g.v().repeat(__::out()).times(1).to_list();
}

#[test]
fn repeat_returns_repeat_traversal_builder() {
    // Acceptance criteria: Returns RepeatTraversal for configuration
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Should be able to chain configuration methods
    let results = g
        .v_ids([tg.alice])
        .repeat(__::out_labels(&["knows"]))
        .times(1)
        .to_list();

    // Alice -knows-> Bob
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn repeat_out_times_2_traverses_two_hops() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice -knows-> Bob -knows-> Charlie
    let results = g
        .v_ids([tg.alice])
        .repeat(__::out_labels(&["knows"]))
        .times(2)
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.charlie));
}

#[test]
fn repeat_until_terminates_on_condition() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Traverse until reaching a software vertex
    // Note: Add times(5) as safety to prevent infinite loops on cyclic graphs
    // The graph has cycles (Charlie -> Alice), so without times limit it would loop forever
    // on paths that don't hit software vertices
    let results = g
        .v_ids([tg.alice])
        .repeat(__::out())
        .until(__::has_label("software"))
        .times(5) // Safety limit for cyclic graph
        .to_list();

    // Should contain GraphDB (hit via until condition) and possibly other exhausted paths
    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.graphdb));
}

#[test]
fn repeat_emit_includes_intermediates() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice -> Bob -> Charlie with emit
    let results = g
        .v_ids([tg.alice])
        .repeat(__::out_labels(&["knows"]))
        .times(2)
        .emit()
        .to_list();

    // emit() emits after each iteration: Bob (iteration 1), Charlie (iteration 2)
    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob));
    assert!(ids.contains(&tg.charlie));
}

#[test]
fn repeat_emit_first_includes_starting_vertex() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Include Alice in results
    let results = g
        .v_ids([tg.alice])
        .repeat(__::out_labels(&["knows"]))
        .times(1)
        .emit()
        .emit_first()
        .to_list();

    // emit_first + emit: Alice (start), Bob (iteration 1)
    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice)); // emit_first
    assert!(ids.contains(&tg.bob)); // emit
}

#[test]
fn repeat_emit_if_selectively_emits() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Emit only software vertices during traversal
    let results = g
        .v_ids([tg.alice])
        .repeat(__::out())
        .times(2)
        .emit_if(__::has_label("software"))
        .to_list();

    // Alice -> Bob, GraphDB (emit GraphDB)
    // Bob -> Charlie, GraphDB (emit GraphDB)
    // But dedup happens internally so we may get 1 or 2 depending on path
    // Actually emit_if emits each time condition matches
    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.iter().all(|id| *id == tg.graphdb));
}

#[test]
fn repeat_continuation_step_returns_bound_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // After repeat, should be able to continue with bound traversal methods
    let results = g
        .v_ids([tg.alice])
        .repeat(__::out_labels(&["knows"]))
        .times(1)
        .values("name")
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Bob".to_string()));
}

#[test]
fn repeat_with_dedup_continuation() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Multiple paths may reach same vertex; dedup after repeat
    let results = g
        .v_ids([tg.alice])
        .repeat(__::out())
        .times(2)
        .emit()
        .dedup()
        .to_list();

    // Bob (once), GraphDB (appears multiple times but deduped), Charlie
    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();

    // Verify no duplicates
    let unique_ids: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(ids.len(), unique_ids.len());
}

#[test]
fn repeat_from_multiple_starting_vertices() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Start from both Alice and Bob
    let results = g
        .v_ids([tg.alice, tg.bob])
        .repeat(__::out_labels(&["knows"]))
        .times(1)
        .to_list();

    // Alice -> Bob, Bob -> Charlie
    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob));
    assert!(ids.contains(&tg.charlie));
}

#[test]
fn repeat_from_leaf_vertex_with_no_outgoing_edges() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // GraphDB has no outgoing edges
    let results = g.v_ids([tg.graphdb]).repeat(__::out()).times(3).to_list();

    // Should emit GraphDB immediately due to exhaustion
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.graphdb));
}

#[test]
fn repeat_times_zero_returns_input_unchanged() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // times(0) means don't iterate at all
    let results = g.v_ids([tg.alice]).repeat(__::out()).times(0).to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}
