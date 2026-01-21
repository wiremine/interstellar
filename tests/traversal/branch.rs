//! Branch step tests - union, coalesce, choose, optional, local

#![allow(unused_variables)]
use interstellar::p;
use interstellar::traversal::__;
use interstellar::value::{Value, VertexId};

use crate::common::graphs::create_small_graph;

// -------------------------------------------------------------------------
// UnionStep Tests
// -------------------------------------------------------------------------

#[test]
fn union_returns_neighbors_from_both_directions() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice has:
    // - out: Bob (knows), GraphDB (uses)
    // - in: Charlie (knows)
    // union(out, in) should return all 3
    let results = g
        .v_ids([tg.alice])
        .union(vec![__.out(), __.in_()])
        .to_list();

    assert_eq!(results.len(), 3);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob)); // out via knows
    assert!(ids.contains(&tg.graphdb)); // out via uses
    assert!(ids.contains(&tg.charlie)); // in via knows
}

#[test]
fn union_merges_results_in_traverser_major_order() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // With multiple inputs, results should be grouped by input traverser
    // Alice -> (out results, then in results)
    // Bob -> (out results, then in results)
    //
    // Using knows edges only for clearer test:
    // Alice knows Bob (out), Charlie knows Alice (in) -> Alice produces 2
    // Bob knows Charlie (out), Alice knows Bob (in) -> Bob produces 2
    let results = g
        .v_ids([tg.alice, tg.bob])
        .union(vec![__.out_labels(&["knows"]), __.in_labels(&["knows"])])
        .to_list();

    // Alice: out->Bob, in<-Charlie = 2
    // Bob: out->Charlie, in<-Alice = 2
    // Total = 4
    assert_eq!(results.len(), 4);

    // Verify traverser-major order:
    // First traverser (Alice) results should come first
    // - First branch (out): Bob
    // - Second branch (in): Charlie
    // Then second traverser (Bob) results
    // - First branch (out): Charlie
    // - Second branch (in): Alice
    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();

    // Alice's results first (Bob from out, Charlie from in)
    assert_eq!(ids[0], tg.bob); // Alice out knows -> Bob
    assert_eq!(ids[1], tg.charlie); // Alice in knows <- Charlie

    // Bob's results second (Charlie from out, Alice from in)
    assert_eq!(ids[2], tg.charlie); // Bob out knows -> Charlie
    assert_eq!(ids[3], tg.alice); // Bob in knows <- Alice
}

#[test]
fn union_with_empty_branches_vec() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Empty branches vec should produce no results
    let results = g.v().union(vec![]).to_list();
    assert!(results.is_empty());
}

#[test]
fn union_with_branch_producing_no_results() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // One branch produces results, one doesn't
    // GraphDB has no outgoing edges but has incoming "uses" edges
    let results = g
        .v_ids([tg.graphdb])
        .union(vec![__.out(), __.in_()])
        .to_list();

    // out() produces nothing, in() produces Alice and Bob
    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.bob));
}

#[test]
fn union_with_all_empty_branches() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // All branches produce no results (nonexistent edge labels)
    let results = g
        .v_ids([tg.alice])
        .union(vec![
            __.out_labels(&["nonexistent1"]),
            __.out_labels(&["nonexistent2"]),
        ])
        .to_list();

    assert!(results.is_empty());
}

#[test]
fn union_with_single_branch_matches_direct_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // union with single branch should equal direct traversal
    let union_results = g.v_ids([tg.alice]).union(vec![__.out()]).to_list();

    let direct_results = g.v_ids([tg.alice]).out().to_list();

    assert_eq!(union_results.len(), direct_results.len());

    let union_ids: Vec<VertexId> = union_results
        .iter()
        .filter_map(|v| v.as_vertex_id())
        .collect();
    let direct_ids: Vec<VertexId> = direct_results
        .iter()
        .filter_map(|v| v.as_vertex_id())
        .collect();

    for id in &direct_ids {
        assert!(union_ids.contains(id));
    }
}

#[test]
fn union_with_labeled_edges() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice: knows->Bob, uses->GraphDB
    // Get neighbors via both edge types using union
    let results = g
        .v_ids([tg.alice])
        .union(vec![__.out_labels(&["knows"]), __.out_labels(&["uses"])])
        .to_list();

    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob)); // via knows
    assert!(ids.contains(&tg.graphdb)); // via uses
}

#[test]
fn union_with_chained_sub_traversals() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get either outgoing neighbors or their names
    let results = g
        .v_ids([tg.alice])
        .union(vec![
            __.out_labels(&["knows"]),
            __.out_labels(&["knows"]).values("name"),
        ])
        .to_list();

    // First branch: Bob (vertex)
    // Second branch: "Bob" (string)
    assert_eq!(results.len(), 2);

    // One should be a vertex, one should be a string
    let has_vertex = results.iter().any(|v| v.is_vertex());
    let has_string = results.iter().any(|v| v.as_str().is_some());
    assert!(has_vertex);
    assert!(has_string);
}

#[test]
fn union_preserves_traverser_metadata() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Test that path metadata is preserved through union
    let results = g
        .v_ids([tg.alice])
        .as_("start")
        .union(vec![__.out_labels(&["knows"])])
        .as_("end")
        .select(&["start", "end"])
        .to_list();

    assert_eq!(results.len(), 1);

    // Should have both start and end labels
    if let Value::Map(map) = &results[0] {
        assert!(map.contains_key("start"));
        assert!(map.contains_key("end"));

        // start should be Alice
        if let Some(start) = map.get("start") {
            assert_eq!(start.as_vertex_id(), Some(tg.alice));
        }
        // end should be Bob
        if let Some(end) = map.get("end") {
            assert_eq!(end.as_vertex_id(), Some(tg.bob));
        }
    } else {
        panic!("Expected Value::Map, got {:?}", results[0]);
    }
}

#[test]
fn anonymous_union_factory() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Use __.union factory to create anonymous traversal
    let anon = __.union(vec![__.out(), __.in_()]);
    let results = g.v_ids([tg.alice]).append(anon).to_list();

    // Alice: out(Bob, GraphDB), in(Charlie)
    assert_eq!(results.len(), 3);
}

#[test]
fn union_on_all_vertices() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get all neighbors (both directions) for all vertices
    let results = g.v().union(vec![__.out(), __.in_()]).to_list();

    // Each vertex contributes its out + in neighbors
    // This will have duplicates since neighbors are shared
    // Alice: out(Bob, GraphDB) + in(Charlie) = 3
    // Bob: out(Charlie, GraphDB) + in(Alice) = 3
    // Charlie: out(Alice) + in(Bob) = 2
    // GraphDB: out() + in(Alice, Bob) = 2
    // Total = 10
    assert_eq!(results.len(), 10);
}

#[test]
fn union_dedup_removes_duplicates() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Union may produce duplicates; dedup should remove them
    let results = g
        .v_ids([tg.alice])
        .union(vec![
            __.out_labels(&["knows"]), // Bob
            __.out_labels(&["knows"]), // Bob again (same branch duplicated)
        ])
        .dedup()
        .to_list();

    // Should deduplicate to just Bob
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

// -------------------------------------------------------------------------
// CoalesceStep Tests
// -------------------------------------------------------------------------

#[test]
fn coalesce_returns_first_non_empty_branch() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice has "name" property but no "nickname" property
    // coalesce should skip the empty nickname branch and return name
    let results = g
        .v_ids([tg.alice])
        .coalesce(vec![__.values("nickname"), __.values("name")])
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice".to_string()));
}

#[test]
fn coalesce_uses_first_branch_when_it_has_results() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice has "name" property - first branch should be used
    let results = g
        .v_ids([tg.alice])
        .coalesce(vec![__.values("name"), __.values("age")])
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice".to_string()));
}

#[test]
fn coalesce_returns_empty_when_all_branches_empty() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // All branches produce no results (nonexistent properties)
    let results = g
        .v_ids([tg.alice])
        .coalesce(vec![
            __.values("nonexistent1"),
            __.values("nonexistent2"),
            __.values("nonexistent3"),
        ])
        .to_list();

    assert!(results.is_empty());
}

#[test]
fn coalesce_with_empty_branches_vec() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Empty branches vec should produce no results
    let results = g.v_ids([tg.alice]).coalesce(vec![]).to_list();
    assert!(results.is_empty());
}

#[test]
fn coalesce_short_circuits_on_first_success() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // First branch returns "name", second would return "age"
    // Coalesce should only return name (short-circuit)
    let results = g
        .v_ids([tg.alice])
        .coalesce(vec![__.values("name"), __.values("age")])
        .to_list();

    // Should only have name, not age
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice".to_string()));
}

#[test]
fn coalesce_with_traversal_branches() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // GraphDB has no outgoing edges but has incoming edges
    // First branch (out) should be empty, second (in) should have results
    let results = g
        .v_ids([tg.graphdb])
        .coalesce(vec![__.out(), __.in_()])
        .to_list();

    // Should have the incoming neighbors (Alice and Bob who use GraphDB)
    assert_eq!(results.len(), 2);
    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.bob));
}

#[test]
fn coalesce_on_multiple_inputs() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Each input traverser is evaluated independently
    // Alice has out edges (knows Bob, uses GraphDB) -> first branch succeeds
    // GraphDB has no out edges but has in edges -> falls back to second branch
    let results = g
        .v_ids([tg.alice, tg.graphdb])
        .coalesce(vec![__.out(), __.in_()])
        .to_list();

    // Alice: out -> Bob, GraphDB (2 results)
    // GraphDB: in -> Alice, Bob (2 results from uses edges)
    assert_eq!(results.len(), 4);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    // Alice's out neighbors
    assert!(ids.contains(&tg.bob));
    assert!(ids.contains(&tg.graphdb));
    // GraphDB's in neighbors (fallback)
    assert!(ids.contains(&tg.alice));
}

#[test]
fn coalesce_with_labeled_edge_branches() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Try to get "uses" neighbors first, fall back to "knows" neighbors
    // Alice has both: uses->GraphDB and knows->Bob
    // Should return GraphDB (first branch succeeds)
    let results = g
        .v_ids([tg.alice])
        .coalesce(vec![__.out_labels(&["uses"]), __.out_labels(&["knows"])])
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.graphdb));
}

#[test]
fn coalesce_falls_back_through_multiple_empty_branches() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // First two branches empty, third has results
    let results = g
        .v_ids([tg.alice])
        .coalesce(vec![
            __.out_labels(&["nonexistent1"]),
            __.out_labels(&["nonexistent2"]),
            __.out_labels(&["knows"]),
        ])
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn anonymous_coalesce_factory() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Use __.coalesce factory to create anonymous traversal
    let anon = __.coalesce(vec![__.values("nickname"), __.values("name")]);

    let results = g.v_ids([tg.alice]).append(anon).to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice".to_string()));
}

#[test]
fn coalesce_with_chained_sub_traversals() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // More complex branches with chained steps
    // First branch: out().has_label("software") - returns GraphDB
    // Second branch: out().has_label("person") - would return Bob
    let results = g
        .v_ids([tg.alice])
        .coalesce(vec![
            __.out().has_label("software"),
            __.out().has_label("person"),
        ])
        .to_list();

    // First branch succeeds with GraphDB
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.graphdb));
}

// -------------------------------------------------------------------------
// ChooseStep Tests
// -------------------------------------------------------------------------

#[test]
fn choose_branches_based_on_label() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // If person, get outgoing "knows" edges; otherwise get all outgoing edges
    // Alice is a person -> should get Bob (knows)
    let results = g
        .v_ids([tg.alice])
        .choose(
            __.has_label("person"),
            __.out_labels(&["knows"]),
            __.out(),
        )
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn choose_executes_if_false_branch_when_condition_fails() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // GraphDB is software, not person -> should take if_false branch
    // if_false branch: in_() returns Alice and Bob (who use GraphDB)
    let results = g
        .v_ids([tg.graphdb])
        .choose(
            __.has_label("person"),
            __.out_labels(&["knows"]),
            __.in_(),
        )
        .to_list();

    assert_eq!(results.len(), 2);
    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.bob));
}

#[test]
fn choose_evaluates_condition_per_traverser() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Multiple inputs: Alice (person) and GraphDB (software)
    // Each should be evaluated independently:
    // - Alice: condition true -> out_labels(["knows"]) -> Bob
    // - GraphDB: condition false -> in_() -> Alice, Bob
    let results = g
        .v_ids([tg.alice, tg.graphdb])
        .choose(
            __.has_label("person"),
            __.out_labels(&["knows"]),
            __.in_(),
        )
        .to_list();

    // Alice -> Bob (1), GraphDB -> Alice, Bob (2) = 3 total
    assert_eq!(results.len(), 3);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    // Bob appears twice (from Alice's true branch and GraphDB's false branch)
    assert_eq!(ids.iter().filter(|&&id| id == tg.bob).count(), 2);
    // Alice appears once (from GraphDB's false branch)
    assert_eq!(ids.iter().filter(|&&id| id == tg.alice).count(), 1);
}

#[test]
fn choose_with_property_condition() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Condition based on property: if age >= 30, get "knows" neighbors, else get all neighbors
    // Alice (age 30) -> true branch -> Bob
    // Bob (age 25) -> false branch -> Charlie (knows), GraphDB (uses)
    let results = g
        .v_ids([tg.alice, tg.bob])
        .choose(
            __.has_where("age", p::gte(30)),
            __.out_labels(&["knows"]),
            __.out(),
        )
        .to_list();

    // Alice (age 30, >= 30): true branch -> Bob (1 result)
    // Bob (age 25, < 30): false branch -> Charlie, GraphDB (2 results)
    assert_eq!(results.len(), 3);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob)); // from Alice
    assert!(ids.contains(&tg.charlie)); // from Bob
    assert!(ids.contains(&tg.graphdb)); // from Bob
}

#[test]
fn choose_if_true_branch_returns_empty() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Condition true but if_true branch returns nothing
    // Alice is person, but has no "worksAt" edges
    let results = g
        .v_ids([tg.alice])
        .choose(
            __.has_label("person"),
            __.out_labels(&["worksAt"]), // Empty - no such edges
            __.out(),
        )
        .to_list();

    // Condition is true, so if_true branch is taken, which returns empty
    assert!(results.is_empty());
}

#[test]
fn choose_if_false_branch_returns_empty() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // GraphDB is not person, so takes if_false branch
    // if_false branch looks for nonexistent edge label
    let results = g
        .v_ids([tg.graphdb])
        .choose(
            __.has_label("person"),
            __.out(),
            __.out_labels(&["nonexistent"]),
        )
        .to_list();

    assert!(results.is_empty());
}

#[test]
fn choose_with_chained_condition() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Complex condition: has outgoing "knows" edge to someone named "Bob"
    // Alice knows Bob -> condition true -> get "uses" edges -> GraphDB
    let results = g
        .v_ids([tg.alice])
        .choose(
            __.out_labels(&["knows"]).has_value("name", "Bob"),
            __.out_labels(&["uses"]),
            __.in_(),
        )
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.graphdb));
}

#[test]
fn choose_condition_false_for_chained_condition() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Bob knows Charlie (not "Alice"), so condition is false
    // Should take if_false branch -> in_() -> Alice (who knows Bob)
    let results = g
        .v_ids([tg.bob])
        .choose(
            __.out_labels(&["knows"]).has_value("name", "Alice"),
            __.out_labels(&["uses"]),
            __.in_labels(&["knows"]),
        )
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn anonymous_choose_factory() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Use __.choose factory to create anonymous traversal
    let anon = __.choose(
        __.has_label("person"),
        __.out_labels(&["knows"]),
        __.in_(),
    );

    let results = g.v_ids([tg.alice]).append(anon).to_list();

    // Alice is person -> true branch -> Bob
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn choose_with_identity_branches() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // If person, return self (identity); otherwise return nothing
    // Using identity() for if_true, empty traversal for if_false
    let results = g
        .v_ids([tg.alice, tg.graphdb])
        .choose(
            __.has_label("person"),
            __.identity(),
            __.out_labels(&["nonexistent"]), // Returns nothing
        )
        .to_list();

    // Alice (person) -> identity -> Alice
    // GraphDB (software) -> empty -> nothing
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn choose_all_persons_get_true_branch() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // All persons: Alice, Bob, Charlie
    // Each takes true branch (out_labels(["knows"]))
    let results = g
        .v_ids([tg.alice, tg.bob, tg.charlie])
        .choose(
            __.has_label("person"),
            __.out_labels(&["knows"]),
            __.in_(),
        )
        .to_list();

    // Alice -> Bob, Bob -> Charlie, Charlie -> Alice = 3 results
    assert_eq!(results.len(), 3);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob)); // from Alice
    assert!(ids.contains(&tg.charlie)); // from Bob
    assert!(ids.contains(&tg.alice)); // from Charlie
}

// -------------------------------------------------------------------------
// OptionalStep Tests
// -------------------------------------------------------------------------

#[test]
fn optional_returns_sub_traversal_results_when_present() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice has outgoing "knows" edge to Bob
    // optional should return Bob (sub-traversal result)
    let results = g
        .v_ids([tg.alice])
        .optional(__.out_labels(&["knows"]))
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn optional_keeps_original_when_sub_traversal_empty() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // GraphDB has no outgoing edges
    // optional should return GraphDB itself (original)
    let results = g.v_ids([tg.graphdb]).optional(__.out()).to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.graphdb));
}

#[test]
fn optional_per_traverser_evaluation() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice has out edges, GraphDB does not
    // Alice -> sub-traversal results (Bob, GraphDB)
    // GraphDB -> original (GraphDB)
    let results = g
        .v_ids([tg.alice, tg.graphdb])
        .optional(__.out())
        .to_list();

    // Alice: out -> Bob, GraphDB (2 results)
    // GraphDB: out empty -> GraphDB (1 result, original)
    assert_eq!(results.len(), 3);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob)); // from Alice's out
    assert!(ids.contains(&tg.graphdb)); // from Alice's out AND GraphDB's fallback
    assert_eq!(ids.iter().filter(|&&id| id == tg.graphdb).count(), 2);
}

#[test]
fn optional_with_labeled_edges() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Bob has "knows" edge to Charlie, but no "worksAt" edges
    // optional(out_labels(["worksAt"])) should return Bob (original)
    let results = g
        .v_ids([tg.bob])
        .optional(__.out_labels(&["worksAt"]))
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn optional_returns_multiple_results_from_sub_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice has two outgoing edges: knows->Bob, uses->GraphDB
    // optional should return both
    let results = g.v_ids([tg.alice]).optional(__.out()).to_list();

    assert_eq!(results.len(), 2);
    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob));
    assert!(ids.contains(&tg.graphdb));
}

#[test]
fn optional_with_chained_sub_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice -> out().has_label("person") -> Bob (Charlie is also person but not direct neighbor)
    let results = g
        .v_ids([tg.alice])
        .optional(__.out().has_label("person"))
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn optional_chained_sub_traversal_returns_empty() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice -> out().has_label("company") -> empty (no company vertices)
    // Should fall back to Alice
    let results = g
        .v_ids([tg.alice])
        .optional(__.out().has_label("company"))
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn optional_with_property_filter() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice -> out neighbors with age < 30 -> Bob (age 25)
    let results = g
        .v_ids([tg.alice])
        .optional(__.out().has_where("age", p::lt(30)))
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn optional_with_property_filter_returns_empty() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice -> out neighbors with age > 100 -> empty
    // Should fall back to Alice
    let results = g
        .v_ids([tg.alice])
        .optional(__.out().has_where("age", p::gt(100)))
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn anonymous_optional_factory() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Use __.optional factory to create anonymous traversal
    let anon = __.optional(__.out_labels(&["knows"]));

    let results = g.v_ids([tg.alice]).append(anon).to_list();

    // Alice knows Bob -> returns Bob
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn optional_all_inputs_have_results() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // All persons have outgoing "knows" edges
    // Alice -> Bob, Bob -> Charlie, Charlie -> Alice
    let results = g
        .v_ids([tg.alice, tg.bob, tg.charlie])
        .optional(__.out_labels(&["knows"]))
        .to_list();

    assert_eq!(results.len(), 3);
    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob)); // from Alice
    assert!(ids.contains(&tg.charlie)); // from Bob
    assert!(ids.contains(&tg.alice)); // from Charlie
}

#[test]
fn optional_all_inputs_fallback() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // No vertex has "nonexistent" edges, all should fall back to original
    let results = g
        .v_ids([tg.alice, tg.bob, tg.charlie])
        .optional(__.out_labels(&["nonexistent"]))
        .to_list();

    assert_eq!(results.len(), 3);
    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.bob));
    assert!(ids.contains(&tg.charlie));
}

#[test]
fn optional_mixed_results_and_fallbacks() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // "uses" edges: Alice->GraphDB, Bob->GraphDB
    // Charlie has no "uses" edges -> falls back to Charlie
    let results = g
        .v_ids([tg.alice, tg.bob, tg.charlie])
        .optional(__.out_labels(&["uses"]))
        .to_list();

    // Alice -> GraphDB, Bob -> GraphDB, Charlie -> Charlie (fallback)
    assert_eq!(results.len(), 3);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    // GraphDB appears twice (from Alice and Bob)
    assert_eq!(ids.iter().filter(|&&id| id == tg.graphdb).count(), 2);
    // Charlie appears once (fallback)
    assert!(ids.contains(&tg.charlie));
}

// -------------------------------------------------------------------------
// LocalStep Tests
// -------------------------------------------------------------------------

#[test]
fn local_executes_sub_traversal_per_traverser() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // local() should execute the sub-traversal independently for each input
    // Alice has 2 out neighbors (Bob, GraphDB)
    // Bob has 2 out neighbors (Charlie, GraphDB)
    let results = g.v_ids([tg.alice, tg.bob]).local(__.out()).to_list();

    // Should get all 4 neighbors
    assert_eq!(results.len(), 4);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob)); // from Alice
    assert!(ids.contains(&tg.graphdb)); // from both Alice and Bob
    assert!(ids.contains(&tg.charlie)); // from Bob
                                        // GraphDB appears twice
    assert_eq!(ids.iter().filter(|&&id| id == tg.graphdb).count(), 2);
}

#[test]
fn local_with_empty_sub_traversal_produces_nothing() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // GraphDB has no outgoing edges
    // local(out()) should produce nothing for GraphDB
    let results = g.v_ids([tg.graphdb]).local(__.out()).to_list();

    assert!(results.is_empty());
}

#[test]
fn local_limit_per_traverser() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice has 2 out neighbors, Bob has 2 out neighbors
    // local(out().limit(1)) should return 1 neighbor per-traverser
    let results = g
        .v_ids([tg.alice, tg.bob])
        .local(__.out().limit(1))
        .to_list();

    // One result per input traverser = 2 total
    assert_eq!(results.len(), 2);
}

#[test]
fn local_vs_global_limit() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Global limit: limits across all traversers
    let global_results = g.v_ids([tg.alice, tg.bob]).out().limit(2).to_list();
    // Takes first 2 from combined stream
    assert_eq!(global_results.len(), 2);

    // Local limit: limits per-traverser
    let local_results = g
        .v_ids([tg.alice, tg.bob])
        .local(__.out().limit(1))
        .to_list();
    // Takes first 1 from each traverser = 2 total
    assert_eq!(local_results.len(), 2);
}

#[test]
fn local_dedup_per_traverser() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Create a scenario where dedup matters per-traverser
    // Using union to create duplicates, then local dedup
    let results = g
        .v_ids([tg.alice])
        .local(__.union(vec![__.out_labels(&["knows"]), __.out_labels(&["knows"])]).dedup())
        .to_list();

    // Union creates Bob twice, dedup reduces to 1
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn local_dedup_per_traverser_multiple_inputs() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Multiple inputs, each gets its own dedup scope
    // Alice union(knows,knows) -> Bob, Bob -> dedup -> Bob
    // Bob union(knows,knows) -> Charlie, Charlie -> dedup -> Charlie
    let results = g
        .v_ids([tg.alice, tg.bob])
        .local(__.union(vec![__.out_labels(&["knows"]), __.out_labels(&["knows"])]).dedup())
        .to_list();

    // Each traverser produces 1 deduped result
    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob)); // from Alice
    assert!(ids.contains(&tg.charlie)); // from Bob
}

#[test]
fn local_with_filter_steps() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter within local scope
    // Get out neighbors that are persons
    let results = g
        .v_ids([tg.alice])
        .local(__.out().has_label("person"))
        .to_list();

    // Alice -> Bob (person), GraphDB (software)
    // Only Bob passes the filter
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn local_with_property_filter() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get out neighbors with age < 30
    // Alice's neighbors: Bob (25), GraphDB (no age)
    let results = g
        .v_ids([tg.alice])
        .local(__.out().has_where("age", p::lt(30)))
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.bob));
}

#[test]
fn local_with_values_transform() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get names of out neighbors, per-traverser
    let results = g
        .v_ids([tg.alice])
        .local(__.out().values("name"))
        .to_list();

    // Alice -> Bob, GraphDB
    assert_eq!(results.len(), 2);
    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| {
            if let Value::String(s) = v {
                Some(s.as_str())
            } else {
                None
            }
        })
        .collect();
    assert!(names.contains(&"Bob"));
    assert!(names.contains(&"GraphDB"));
}

#[test]
fn local_with_chained_navigation() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Two-hop traversal within local
    // Alice -> out -> (Bob, GraphDB) -> out -> (Charlie, GraphDB, Alice, Bob)
    // But local executes per input traverser
    let results = g.v_ids([tg.alice]).local(__.out().out()).to_list();

    // Alice -> Bob -> Charlie, GraphDB
    // Alice -> GraphDB -> (nothing, no out edges)
    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.charlie)); // Bob -> Charlie
    assert!(ids.contains(&tg.graphdb)); // Bob -> GraphDB
}

#[test]
fn anonymous_local_factory() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Use __.local factory to create anonymous traversal
    let anon = __.local(__.out().limit(1));

    let results = g.v_ids([tg.alice, tg.bob]).append(anon).to_list();

    // Each traverser gets limit(1) applied locally = 2 results
    assert_eq!(results.len(), 2);
}

#[test]
fn local_preserves_traverser_isolation() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Verify that each traverser's local scope is truly isolated
    // Using skip to show per-traverser behavior
    // Alice has 2 out (Bob, GraphDB), skip(1) -> 1 result
    // Bob has 2 out (Charlie, GraphDB), skip(1) -> 1 result
    let results = g
        .v_ids([tg.alice, tg.bob])
        .local(__.out().skip(1))
        .to_list();

    // Each traverser skips 1 of their 2 neighbors
    assert_eq!(results.len(), 2);
}

#[test]
fn local_with_range_step() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice has 2 neighbors, range(0,1) takes first 1
    // Bob has 2 neighbors, range(0,1) takes first 1
    let results = g
        .v_ids([tg.alice, tg.bob])
        .local(__.out().range(0, 1))
        .to_list();

    assert_eq!(results.len(), 2);
}

#[test]
fn local_with_labeled_edges() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get only "knows" neighbors within local scope
    let results = g
        .v_ids([tg.alice, tg.bob])
        .local(__.out_labels(&["knows"]))
        .to_list();

    // Alice knows Bob, Bob knows Charlie
    assert_eq!(results.len(), 2);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob));
    assert!(ids.contains(&tg.charlie));
}

#[test]
fn local_mixed_results_per_traverser() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice: 2 out neighbors
    // Charlie: 1 out neighbor (knows Alice)
    // GraphDB: 0 out neighbors
    let results = g
        .v_ids([tg.alice, tg.charlie, tg.graphdb])
        .local(__.out())
        .to_list();

    // Alice: 2, Charlie: 1, GraphDB: 0 = 3 total
    assert_eq!(results.len(), 3);

    let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.bob)); // from Alice
    assert!(ids.contains(&tg.graphdb)); // from Alice
    assert!(ids.contains(&tg.alice)); // from Charlie
}
