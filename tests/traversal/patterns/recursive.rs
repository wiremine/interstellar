//! Recursive traversal pattern tests.
//!
//! Tests for repeat-based traversal patterns including:
//! - Fixed depth traversal with times()
//! - Conditional termination with until()
//! - Emit patterns for intermediate results
//! - Loop detection and simple path

#![allow(unused_variables)]

use interstellar::p;
use interstellar::traversal::__;
use interstellar::value::Value;

use crate::common::graphs::{create_org_graph, create_small_graph};

// =============================================================================
// Fixed Depth with times()
// =============================================================================

#[test]
fn repeat_times_zero_returns_start() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Zero iterations should return starting vertex
    let results = g
        .v_ids([tg.alice])
        .repeat(__.out_labels(&["knows"]))
        .times(0)
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn repeat_times_one() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // One iteration: Alice -> Bob
    let results = g
        .v_ids([tg.alice])
        .repeat(__.out_labels(&["knows"]))
        .times(1)
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn repeat_times_two() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Two iterations: Alice -> Bob -> Charlie
    let results = g
        .v_ids([tg.alice])
        .repeat(__.out_labels(&["knows"]))
        .times(2)
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.charlie));
}

#[test]
fn repeat_times_three_with_cycle() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Three iterations: Alice -> Bob -> Charlie -> Alice (cycle)
    let results = g
        .v_ids([tg.alice])
        .repeat(__.out_labels(&["knows"]))
        .times(3)
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn repeat_times_from_multiple_starts() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Start from two vertices
    let results = g
        .v_ids([tg.alice, tg.bob])
        .repeat(__.out_labels(&["knows"]))
        .times(1)
        .to_list();

    // Alice -> Bob, Bob -> Charlie
    assert_eq!(results.len(), 2);
}

// =============================================================================
// Conditional Termination with until()
// =============================================================================

#[test]
fn repeat_until_condition_met() {
    let org = create_org_graph();
    let snapshot = org.snapshot();
    let g = snapshot.gremlin();

    // Go up reporting chain until reaching level 0 (CEO)
    // dev1 -> eng_mgr1 -> cto -> ceo
    let results = g
        .v_ids([org.dev1])
        .repeat(__.out_labels(&["reports_to"]))
        .until(__.has_where("level", p::eq(0i64)))
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(org.ceo));
}

#[test]
fn repeat_until_with_max_loops() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Until we find software (which we won't via knows edges)
    // But limit to 5 loops to prevent infinite loop
    let results = g
        .v_ids([tg.alice])
        .repeat(__.out_labels(&["knows"]))
        .until(__.has_label("software"))
        .times(5)
        .to_list();

    // Should terminate after 5 iterations even if condition not met
    assert!(!results.is_empty());
}

#[test]
fn repeat_until_immediate_match() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Start already matches condition
    let results = g
        .v_ids([tg.alice])
        .repeat(__.out_labels(&["knows"]))
        .until(__.has_label("person"))
        .to_list();

    // Alice is a person, but until is checked after first iteration
    // So should return Bob (first iteration result that's a person)
    assert!(!results.is_empty());
}

// =============================================================================
// Emit Patterns
// =============================================================================

#[test]
fn repeat_emit_all_intermediate() {
    let org = create_org_graph();
    let snapshot = org.snapshot();
    let g = snapshot.gremlin();

    // Emit all vertices in the path up the chain
    // dev1 -> eng_mgr1 -> cto -> ceo (3 hops)
    let results = g
        .v_ids([org.dev1])
        .repeat(__.out_labels(&["reports_to"]))
        .times(3)
        .emit()
        .to_list();

    // Should emit: eng_mgr1, cto, ceo (3 levels up)
    assert_eq!(results.len(), 3);
}

#[test]
fn repeat_emit_first() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Emit first means include starting vertex - must chain emit().emit_first()
    let results = g
        .v_ids([tg.alice])
        .repeat(__.out_labels(&["knows"]))
        .times(2)
        .emit()
        .emit_first()
        .to_list();

    // Should include Alice (start), Bob, Charlie
    assert_eq!(results.len(), 3);

    let ids: Vec<_> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.bob));
    assert!(ids.contains(&tg.charlie));
}

#[test]
fn repeat_emit_if_condition() {
    let org = create_org_graph();
    let snapshot = org.snapshot();
    let g = snapshot.gremlin();

    // Only emit vertices at level 1 or lower (executives)
    // dev1 -> eng_mgr1(2) -> cto(1) -> ceo(0)
    let results = g
        .v_ids([org.dev1])
        .repeat(__.out_labels(&["reports_to"]))
        .times(3)
        .emit_if(__.has_where("level", p::lte(1i64)))
        .to_list();

    // Should emit CTO (level 1) and CEO (level 0)
    assert_eq!(results.len(), 2);
}

#[test]
fn repeat_emit_with_until() {
    let org = create_org_graph();
    let snapshot = org.snapshot();
    let g = snapshot.gremlin();

    // Emit intermediates until reaching CEO
    // dev1 -> eng_mgr1 -> cto -> ceo
    let results = g
        .v_ids([org.dev1])
        .repeat(__.out_labels(&["reports_to"]))
        .until(__.has_where("level", p::eq(0i64)))
        .emit()
        .to_list();

    // Should emit eng_mgr1, cto, ceo
    assert!(results.len() >= 3);
}

// =============================================================================
// Path in Repeat
// =============================================================================

#[test]
fn repeat_with_path_tracking() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Track path through repeat - must enable with_path() first
    let paths = g
        .v_ids([tg.alice])
        .with_path()
        .repeat(__.out_labels(&["knows"]))
        .times(2)
        .path()
        .to_list();

    assert_eq!(paths.len(), 1);
    if let Value::List(path) = &paths[0] {
        // Alice -> Bob -> Charlie
        assert_eq!(path.len(), 3);
    }
}

#[test]
fn repeat_emit_with_path() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Emit with path tracking - must enable with_path() first
    let paths = g
        .v_ids([tg.alice])
        .with_path()
        .repeat(__.out_labels(&["knows"]))
        .times(2)
        .emit()
        .path()
        .to_list();

    // Should have 2 paths: [Alice, Bob] and [Alice, Bob, Charlie]
    assert_eq!(paths.len(), 2);
}

// =============================================================================
// Repeat with Dedup
// =============================================================================

#[test]
fn repeat_with_dedup_avoids_revisit() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Use dedup to avoid counting same vertex multiple times
    let results = g
        .v_ids([tg.alice])
        .repeat(__.out_labels(&["knows"]))
        .times(6)
        .emit()
        .dedup()
        .to_list();

    // With cycle, without dedup we'd see duplicates
    // With dedup, should have at most 3 unique vertices
    assert!(results.len() <= 3);
}

// =============================================================================
// Bidirectional Repeat
// =============================================================================

#[test]
fn repeat_both_directions() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Explore in both directions
    let results = g
        .v_ids([tg.bob])
        .repeat(__.both_labels(&["knows"]))
        .times(1)
        .dedup()
        .to_list();

    // Bob's knows connections: Alice (in), Charlie (out)
    assert_eq!(results.len(), 2);
}

#[test]
fn repeat_both_multiple_hops() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // 2 hops in any direction, deduplicated
    let results = g
        .v_ids([tg.bob])
        .repeat(__.both_labels(&["knows"]))
        .times(2)
        .dedup()
        .to_list();

    // Should reach more vertices
    assert!(!results.is_empty());
}

// =============================================================================
// Complex Repeat Patterns
// =============================================================================

#[test]
fn repeat_with_filter_inside() {
    let org = create_org_graph();
    let snapshot = org.snapshot();
    let g = snapshot.gremlin();

    // Repeat with filter applied each iteration
    let results = g
        .v_ids([org.dev1])
        .repeat(__.out_labels(&["reports_to"]).has_label("employee"))
        .times(3)
        .to_list();

    assert!(!results.is_empty());
}

#[test]
fn repeat_with_values_inside() {
    let org = create_org_graph();
    let snapshot = org.snapshot();
    let g = snapshot.gremlin();

    // Get names at each level
    // dev1 -> eng_mgr1 -> cto -> ceo
    let names = g
        .v_ids([org.dev1])
        .repeat(__.out_labels(&["reports_to"]))
        .times(3)
        .emit()
        .values("name")
        .to_list();

    assert_eq!(names.len(), 3);
    assert!(names.contains(&Value::String("Eng Manager 1".to_string())));
    assert!(names.contains(&Value::String("CTO".to_string())));
    assert!(names.contains(&Value::String("CEO".to_string())));
}

#[test]
fn repeat_times_with_no_outgoing() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // GraphDB has no outgoing edges
    // When exhausted (no path to follow), the starting vertex is emitted
    let results = g.v_ids([tg.graphdb]).repeat(__.out()).times(3).to_list();

    // Should emit GraphDB immediately due to exhaustion
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.graphdb));
}

#[test]
fn repeat_from_leaf_with_emit_first() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Emit first should include start even if no traversal possible
    // Must chain emit().emit_first()
    let results = g
        .v_ids([tg.graphdb])
        .repeat(__.out())
        .times(3)
        .emit()
        .emit_first()
        .to_list();

    // Should at least include GraphDB
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.graphdb));
}

#[test]
fn chained_repeat_equivalent_pattern() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Instead of chaining repeats, use times(2) for equivalent behavior
    // Alice -> Bob -> Charlie (equivalent to two times(1) calls)
    let results = g
        .v_ids([tg.alice])
        .repeat(__.out_labels(&["knows"]))
        .times(2)
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.charlie));
}

// =============================================================================
// Hierarchy Traversal Patterns
// =============================================================================

#[test]
fn find_all_descendants() {
    let org = create_org_graph();
    let snapshot = org.snapshot();
    let g = snapshot.gremlin();

    // Find all people who report (directly or indirectly) to CEO
    // CEO has 9 reports: cto, cfo, eng_mgr1, eng_mgr2, fin_mgr, dev1, dev2, dev3, accountant
    let reports = g
        .v_ids([org.ceo])
        .repeat(__.in_labels(&["reports_to"]))
        .times(4)
        .emit()
        .to_list();

    // Should find all 9 employees below CEO
    assert_eq!(reports.len(), 9);
}

#[test]
fn find_all_ancestors() {
    let org = create_org_graph();
    let snapshot = org.snapshot();
    let g = snapshot.gremlin();

    // Find all managers above dev1
    // dev1 -> eng_mgr1 -> cto -> ceo
    let managers = g
        .v_ids([org.dev1])
        .repeat(__.out_labels(&["reports_to"]))
        .times(3)
        .emit()
        .to_list();

    // eng_mgr1, cto, ceo
    assert_eq!(managers.len(), 3);
}

#[test]
fn count_hierarchy_depth() {
    let org = create_org_graph();
    let snapshot = org.snapshot();
    let g = snapshot.gremlin();

    // Count levels from dev1 to CEO - must enable with_path() first
    let path = g
        .v_ids([org.dev1])
        .with_path()
        .repeat(__.out_labels(&["reports_to"]))
        .until(__.has_where("level", p::eq(0i64)))
        .path()
        .to_list();

    if let Value::List(p) = &path[0] {
        // dev1 -> eng_mgr1 -> cto -> ceo = 4 vertices in path = 3 edges = depth 3
        assert_eq!(p.len(), 4);
    }
}
