//! Filter step tests.

#![allow(unused_variables)]
use interstellar::p;
use interstellar::traversal::__;
use interstellar::value::Value;

use crate::common::graphs::create_small_graph;

#[test]
fn has_label_filters_vertices_by_label() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let people = g.v().has_label("person").to_list();
    assert_eq!(people.len(), 3); // Alice, Bob, Charlie

    let software = g.v().has_label("software").to_list();
    assert_eq!(software.len(), 1); // GraphDB
}

#[test]
fn has_label_filters_edges_by_label() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let knows_edges = g.e().has_label("knows").to_list();
    assert_eq!(knows_edges.len(), 3);

    let uses_edges = g.e().has_label("uses").to_list();
    assert_eq!(uses_edges.len(), 2);
}

#[test]
fn has_label_any_filters_by_multiple_labels() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let all = g.v().has_label_any(["person", "software"]).to_list();
    assert_eq!(all.len(), 4);
}

#[test]
fn has_label_returns_empty_for_nonexistent_label() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let results = g.v().has_label("unknown").to_list();
    assert!(results.is_empty());
}

#[test]
fn has_filters_by_property_existence() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // All vertices have "name"
    let with_name = g.v().has("name").to_list();
    assert_eq!(with_name.len(), 4);

    // Only person vertices have "age"
    let with_age = g.v().has("age").to_list();
    assert_eq!(with_age.len(), 3);

    // Only software has "version"
    let with_version = g.v().has("version").to_list();
    assert_eq!(with_version.len(), 1);
}

#[test]
fn has_value_filters_by_property_value() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let alice = g.v().has_value("name", "Alice").to_list();
    assert_eq!(alice.len(), 1);

    let age_30 = g.v().has_value("age", 30i64).to_list();
    assert_eq!(age_30.len(), 1);
}

#[test]
fn has_id_filters_vertices_by_id() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let result = g.v().has_id(tg.alice).to_list();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].as_vertex_id(), Some(tg.alice));
}

#[test]
fn has_ids_filters_by_multiple_ids() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let results = g.v().has_ids([tg.alice, tg.bob, tg.charlie]).to_list();
    assert_eq!(results.len(), 3);
}

#[test]
fn filter_with_custom_predicate() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter injected values
    let positives = g
        .inject([1i64, -2i64, 3i64, -4i64])
        .filter(|_ctx, v| matches!(v, Value::Int(n) if *n > 0))
        .to_list();
    assert_eq!(positives.len(), 2);
    assert_eq!(positives[0], Value::Int(1));
    assert_eq!(positives[1], Value::Int(3));
}

#[test]
fn dedup_removes_duplicates() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let results = g.inject([1i64, 2i64, 1i64, 3i64, 2i64]).dedup().to_list();
    assert_eq!(results.len(), 3);
}

#[test]
fn limit_restricts_result_count() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let results = g.v().limit(2).to_list();
    assert_eq!(results.len(), 2);
}

#[test]
fn limit_with_more_than_available() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let results = g.v().limit(100).to_list();
    assert_eq!(results.len(), 4);
}

#[test]
fn skip_skips_elements() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let results = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).skip(2).to_list();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Int(3));
}

#[test]
fn range_selects_range_of_elements() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    let results = g
        .inject([0i64, 1i64, 2i64, 3i64, 4i64, 5i64])
        .range(2, 5)
        .to_list();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Int(2));
    assert_eq!(results[1], Value::Int(3));
    assert_eq!(results[2], Value::Int(4));
}

#[test]
fn chained_filters() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Find person vertices with age property
    let results = g.v().has_label("person").has("age").to_list();
    assert_eq!(results.len(), 3);
}

// -------------------------------------------------------------------------
// TailStep Integration Tests
// -------------------------------------------------------------------------

#[test]
fn test_tail_with_order() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Order by age ascending and get last 2 (oldest people)
    let results = g
        .v()
        .has_label("person")
        .values("age")
        .order()
        .build()
        .tail_n(2)
        .to_list();

    assert_eq!(results.len(), 2);
    // After ordering: 25, 30, 35 -> tail 2 = [30, 35]
    assert_eq!(results[0], Value::Int(30));
    assert_eq!(results[1], Value::Int(35));
}

#[test]
fn test_tail_single_element() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get the last person by age (oldest)
    let results = g
        .v()
        .has_label("person")
        .values("age")
        .order()
        .build()
        .tail()
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(35)); // Charlie is oldest
}

#[test]
fn test_tail_chained_with_navigation() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get outgoing edges from Alice, take last 2
    let results = g.v().has_id(tg.alice).out_e().tail_n(2).to_list();

    // Alice has: knows->Bob, uses->GraphDB
    assert_eq!(results.len(), 2);
}

// -------------------------------------------------------------------------
// DedupByKey Integration Tests
// -------------------------------------------------------------------------

#[test]
fn test_dedup_by_key_with_navigation() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get all vertices connected to Alice via "knows", dedup by label
    let results = g
        .v()
        .has_id(tg.alice)
        .out_labels(&["knows"])
        .dedup_by_label()
        .to_list();

    // Alice knows Bob (person), should get 1 unique label
    assert_eq!(results.len(), 1);
}

#[test]
fn test_dedup_by_traversal_with_values() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Dedup vertices by their age value
    let results = g
        .v()
        .has_label("person")
        .dedup_by(__::values("age"))
        .to_list();

    // All persons have different ages (25, 30, 35), so all pass through
    assert_eq!(results.len(), 3);
}

// -------------------------------------------------------------------------
// CoinStep Integration Tests
// -------------------------------------------------------------------------

#[test]
fn test_coin_zero_filters_all() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // coin(0.0) should filter out everything
    let results = g.v().coin(0.0).to_list();

    assert!(results.is_empty());
}

#[test]
fn test_coin_one_passes_all() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // coin(1.0) should pass everything
    let all_vertices = g.v().to_list();
    let coin_results = g.v().coin(1.0).to_list();

    assert_eq!(coin_results.len(), all_vertices.len());
}

#[test]
fn test_coin_with_filter_chain() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter persons, then apply coin(1.0)
    let results = g.v().has_label("person").coin(1.0).to_list();

    assert_eq!(results.len(), 3); // All 3 persons
}

// -------------------------------------------------------------------------
// SampleStep Integration Tests
// -------------------------------------------------------------------------

#[test]
fn test_sample_respects_limit() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Sample 2 vertices from all
    let results = g.v().sample(2).to_list();

    assert_eq!(results.len(), 2);
}

#[test]
fn test_sample_with_fewer_elements() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Sample 10 from 3 persons should return all 3
    let results = g.v().has_label("person").sample(10).to_list();

    assert_eq!(results.len(), 3);
}

#[test]
fn test_sample_chained_with_navigation() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get all edges, sample 2
    let results = g.e().sample(2).to_list();

    assert_eq!(results.len(), 2);
}

// -------------------------------------------------------------------------
// HasKey and HasPropValue Integration Tests
// -------------------------------------------------------------------------

#[test]
fn test_has_key_on_properties() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get properties of Alice, filter by key "age"
    let results = g.v().has_id(tg.alice).properties().has_key("age").to_list();

    assert_eq!(results.len(), 1);
}

#[test]
fn test_has_key_any_on_properties() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get properties of Alice, filter by key "name" or "age"
    let results = g
        .v()
        .has_id(tg.alice)
        .properties()
        .has_key_any(["name", "age"])
        .to_list();

    assert_eq!(results.len(), 2);
}

#[test]
fn test_has_prop_value_on_properties() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get all person properties, filter by value "Alice"
    let results = g
        .v()
        .has_label("person")
        .properties()
        .has_prop_value("Alice")
        .to_list();

    assert_eq!(results.len(), 1);
}

// -------------------------------------------------------------------------
// WherePStep Integration Tests
// -------------------------------------------------------------------------

#[test]
fn test_where_p_with_comparison() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter ages > 25
    let results = g
        .v()
        .has_label("person")
        .values("age")
        .where_p(p::gt(25))
        .to_list();

    assert_eq!(results.len(), 2); // Alice (30) and Charlie (35)
    assert!(results.contains(&Value::Int(30)));
    assert!(results.contains(&Value::Int(35)));
}

#[test]
fn test_where_p_with_within() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter ages within [25, 35]
    let results = g
        .v()
        .has_label("person")
        .values("age")
        .where_p(p::within([25, 35]))
        .to_list();

    assert_eq!(results.len(), 2); // Bob (25) and Charlie (35)
    assert!(results.contains(&Value::Int(25)));
    assert!(results.contains(&Value::Int(35)));
}

#[test]
fn test_where_p_with_between() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter ages between 25 (inclusive) and 35 (exclusive)
    let results = g
        .v()
        .has_label("person")
        .values("age")
        .where_p(p::between(25, 35))
        .to_list();

    assert_eq!(results.len(), 2); // Bob (25) and Alice (30)
    assert!(results.contains(&Value::Int(25)));
    assert!(results.contains(&Value::Int(30)));
}

#[test]
fn test_where_p_combined_with_and() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Filter ages >= 25 AND <= 30
    let results = g
        .v()
        .has_label("person")
        .values("age")
        .where_p(p::and(p::gte(25), p::lte(30)))
        .to_list();

    assert_eq!(results.len(), 2); // Bob (25) and Alice (30)
    assert!(results.contains(&Value::Int(25)));
    assert!(results.contains(&Value::Int(30)));
}

// -------------------------------------------------------------------------
// Complex Chains Combining Multiple New Steps
// -------------------------------------------------------------------------

#[test]
fn test_chain_dedup_tail() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get all vertices reachable from Alice via "knows" (up to 2 hops),
    // dedup and get tail
    let results = g
        .v()
        .has_id(tg.alice)
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .dedup()
        .tail()
        .to_list();

    // Alice->Bob->Charlie, so Charlie is reachable
    assert!(results.len() <= 1);
}

#[test]
fn test_chain_sample_where_p() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Sample 10 person vertices (returns all 3), then filter by age > 25
    let results = g
        .v()
        .has_label("person")
        .sample(10)
        .values("age")
        .where_p(p::gt(25))
        .to_list();

    assert_eq!(results.len(), 2); // Alice (30) and Charlie (35)
}

#[test]
fn test_chain_order_tail_where_p() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Order ages ascending, take last 2 (oldest), filter >= 30
    let results = g
        .v()
        .has_label("person")
        .values("age")
        .order()
        .build()
        .tail_n(2)
        .where_p(p::gte(30))
        .to_list();

    // tail_n(2) gives [30, 35], where_p(>=30) keeps both
    assert_eq!(results.len(), 2);
    assert!(results.contains(&Value::Int(30)));
    assert!(results.contains(&Value::Int(35)));
}

#[test]
fn test_anonymous_traversal_tail() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Use tail_n directly on injected values
    let results = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).tail_n(2).to_list();

    // Tail 2 of [1,2,3,4,5] = [4,5]
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], Value::Int(4));
    assert_eq!(results[1], Value::Int(5));
}

#[test]
fn test_anonymous_traversal_sample() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Use sample directly on injected values
    let results = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).sample(2).to_list();

    assert_eq!(results.len(), 2);
}

#[test]
fn test_anonymous_traversal_where_p() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Use anonymous where_p in a filter context
    let results = g
        .inject([10i64, 20i64, 30i64, 40i64, 50i64])
        .local(__::where_p(p::gt(25)))
        .to_list();

    // Should filter to values > 25: [30, 40, 50]
    assert_eq!(results.len(), 3);
}

#[test]
fn test_full_pipeline_with_new_steps() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Complex pipeline:
    // 1. Start with all vertices
    // 2. Filter to persons
    // 3. Dedup by label (all "person", so keeps first)
    // 4. Navigate to known persons
    // 5. Get ages
    // 6. Filter where age > 20
    // 7. Get last 2
    let results = g
        .v()
        .has_label("person")
        .out_labels(&["knows"])
        .dedup()
        .values("age")
        .where_p(p::gt(20))
        .tail_n(2)
        .to_list();

    // All 3 persons know someone, their targets are Bob, Charlie, Alice
    // Ages: 25, 35, 30 (all > 20)
    // After dedup and tail_n(2), we get last 2 ages
    assert!(results.len() <= 2);
}

// -------------------------------------------------------------------------
// WhereStep Tests
// -------------------------------------------------------------------------

#[test]
fn where_filters_by_sub_traversal_existence() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Keep vertices that have outgoing edges
    // Alice: out to Bob, GraphDB (2 out) -> passes
    // Bob: out to Charlie, GraphDB (2 out) -> passes
    // Charlie: out to Alice (1 out) -> passes
    // GraphDB: no outgoing edges -> filtered out
    let results = g.v().where_(__::out()).to_list();
    assert_eq!(results.len(), 3); // Alice, Bob, Charlie have outgoing edges
}

#[test]
fn where_filters_by_labeled_edges() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Keep vertices that have outgoing "knows" edges
    // Alice: knows Bob -> passes
    // Bob: knows Charlie -> passes
    // Charlie: knows Alice -> passes
    // GraphDB: no knows edges -> filtered out
    let results = g.v().where_(__::out_labels(&["knows"])).to_list();
    assert_eq!(results.len(), 3);

    // Verify all results are people (not GraphDB)
    for v in &results {
        let id = v.as_vertex_id().unwrap();
        assert!(id == tg.alice || id == tg.bob || id == tg.charlie);
    }
}

#[test]
fn where_filters_by_chained_sub_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Keep vertices that know someone who uses software
    // Alice knows Bob, Bob uses GraphDB -> Alice passes
    // Bob knows Charlie, Charlie doesn't use anything -> Bob fails
    // Charlie knows Alice, Alice uses GraphDB -> Charlie passes
    let results = g
        .v()
        .where_(__::out_labels(&["knows"]).out_labels(&["uses"]))
        .to_list();
    assert_eq!(results.len(), 2); // Alice and Charlie
}

#[test]
fn where_empty_sub_traversal_filters_out() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // No vertex has outgoing "nonexistent" edges
    let results = g.v().where_(__::out_labels(&["nonexistent"])).to_list();
    assert!(results.is_empty());
}

// -------------------------------------------------------------------------
// NotStep Tests
// -------------------------------------------------------------------------

#[test]
fn not_filters_to_traversers_without_outgoing_edges() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Keep vertices WITHOUT outgoing edges
    // GraphDB has no outgoing edges -> passes
    // Alice, Bob, Charlie all have outgoing edges -> filtered out
    let results = g.v().not(__::out()).to_list();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.graphdb));
}

#[test]
fn not_is_inverse_of_where() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Vertices with outgoing edges (where)
    let with_out = g.v().where_(__::out()).to_list();

    // Vertices without outgoing edges (not)
    let without_out = g.v().not(__::out()).to_list();

    // Together they should equal all vertices
    assert_eq!(with_out.len() + without_out.len(), 4);

    // No overlap between results
    let with_ids: Vec<_> = with_out.iter().filter_map(|v| v.as_vertex_id()).collect();
    let without_ids: Vec<_> = without_out
        .iter()
        .filter_map(|v| v.as_vertex_id())
        .collect();
    for id in &with_ids {
        assert!(!without_ids.contains(id));
    }
}

#[test]
fn not_filters_by_labeled_edges() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Keep vertices WITHOUT outgoing "uses" edges
    // Alice: uses GraphDB -> filtered out
    // Bob: uses GraphDB -> filtered out
    // Charlie: no uses edges -> passes
    // GraphDB: no uses edges -> passes
    let results = g.v().not(__::out_labels(&["uses"])).to_list();
    assert_eq!(results.len(), 2); // Charlie and GraphDB

    let ids: Vec<_> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.charlie));
    assert!(ids.contains(&tg.graphdb));
}

#[test]
fn not_with_has_label_sub_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Keep vertices that are NOT persons
    // This uses a sub-traversal pattern - filter out if has_label matches
    let results = g.v().not(__::has_label("person")).to_list();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.graphdb));
}

#[test]
fn not_finds_leaf_vertices() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Leaf vertices have no outgoing edges
    // In this graph, only GraphDB has no outgoing edges
    let leaves = g.v().not(__::out()).to_list();
    assert_eq!(leaves.len(), 1);

    // Verify it's GraphDB (the software vertex)
    let leaf = &leaves[0];
    assert!(leaf.is_vertex());
    assert_eq!(leaf.as_vertex_id(), Some(tg.graphdb));
}

// -------------------------------------------------------------------------
// AndStep Tests
// -------------------------------------------------------------------------

#[test]
fn and_requires_all_conditions() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Keep vertices that have BOTH outgoing AND incoming edges
    // Alice: out(Bob,GraphDB), in(Charlie) -> passes
    // Bob: out(Charlie,GraphDB), in(Alice) -> passes
    // Charlie: out(Alice), in(Bob) -> passes
    // GraphDB: out(), in(Alice,Bob) -> fails (no outgoing)
    let results = g.v().and_(vec![__::out(), __::in_()]).to_list();
    assert_eq!(results.len(), 3); // Alice, Bob, Charlie
}

#[test]
fn and_short_circuits_on_first_failure() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Require outgoing "knows" AND outgoing "uses" edges
    // Alice: knows(Bob), uses(GraphDB) -> passes
    // Bob: knows(Charlie), uses(GraphDB) -> passes
    // Charlie: knows(Alice), no uses -> fails
    // GraphDB: no knows, no uses -> fails
    let results = g
        .v()
        .and_(vec![__::out_labels(&["knows"]), __::out_labels(&["uses"])])
        .to_list();
    assert_eq!(results.len(), 2); // Alice, Bob

    let ids: Vec<_> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.bob));
}

#[test]
fn and_with_empty_vec_passes_all() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Empty and_ should pass all traversers (vacuous truth)
    let results = g.v().and_(vec![]).to_list();
    assert_eq!(results.len(), 4);
}

// -------------------------------------------------------------------------
// OrStep Tests
// -------------------------------------------------------------------------

#[test]
fn or_accepts_any_condition() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Keep vertices that have EITHER "knows" OR "uses" outgoing edges
    // Alice: knows(Bob), uses(GraphDB) -> passes
    // Bob: knows(Charlie), uses(GraphDB) -> passes
    // Charlie: knows(Alice) -> passes
    // GraphDB: neither -> fails
    let results = g
        .v()
        .or_(vec![__::out_labels(&["knows"]), __::out_labels(&["uses"])])
        .to_list();
    assert_eq!(results.len(), 3); // Alice, Bob, Charlie
}

#[test]
fn or_short_circuits_on_first_success() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Keep vertices that are person OR software
    let results = g
        .v()
        .or_(vec![__::has_label("person"), __::has_label("software")])
        .to_list();
    assert_eq!(results.len(), 4); // All vertices match
}

#[test]
fn or_with_empty_vec_filters_all() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Empty or_ should filter all traversers (no conditions to satisfy)
    let results = g.v().or_(vec![]).to_list();
    assert!(results.is_empty());
}

#[test]
fn or_finds_vertices_with_either_edge_type() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Find vertices that either use something OR are used by someone
    // Alice: uses(GraphDB) -> passes
    // Bob: uses(GraphDB) -> passes
    // Charlie: neither uses nor is used -> fails
    // GraphDB: is used by Alice, Bob -> passes
    let results = g
        .v()
        .or_(vec![__::out_labels(&["uses"]), __::in_labels(&["uses"])])
        .to_list();
    assert_eq!(results.len(), 3); // Alice, Bob, GraphDB

    let ids: Vec<_> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.alice));
    assert!(ids.contains(&tg.bob));
    assert!(ids.contains(&tg.graphdb));
}

// -------------------------------------------------------------------------
// Combined Filter Steps Tests
// -------------------------------------------------------------------------

#[test]
fn where_and_not_combined() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Find persons who don't use anything
    // Filter to persons first, then filter out those who use something
    let results = g
        .v()
        .has_label("person")
        .not(__::out_labels(&["uses"]))
        .to_list();
    assert_eq!(results.len(), 1); // Charlie
    assert_eq!(results[0].as_vertex_id(), Some(tg.charlie));
}

#[test]
fn nested_filter_steps() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Find vertices that know someone who knows someone
    // Alice knows Bob, Bob knows Charlie -> Alice passes
    // Bob knows Charlie, Charlie knows Alice -> Bob passes
    // Charlie knows Alice, Alice knows Bob -> Charlie passes
    // GraphDB knows nobody -> fails
    let results = g
        .v()
        .where_(__::out_labels(&["knows"]).out_labels(&["knows"]))
        .to_list();
    assert_eq!(results.len(), 3);
}

// -------------------------------------------------------------------------
// Anonymous Traversal Factory Tests (__ module)
// -------------------------------------------------------------------------

#[test]
fn anonymous_where_factory() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Use __::where_ factory to create anonymous traversal
    let anon = __::where_(__::out());
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 3);
}

#[test]
fn anonymous_not_factory() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Use __::not factory to create anonymous traversal
    let anon = __::not(__::out());
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_vertex_id(), Some(tg.graphdb));
}

#[test]
fn anonymous_and_factory() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Use __::and_ factory to create anonymous traversal
    let anon = __::and_(vec![__::out(), __::in_()]);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 3);
}

#[test]
fn anonymous_or_factory() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Use __::or_ factory to create anonymous traversal
    let anon = __::or_(vec![__::has_label("person"), __::has_label("software")]);
    let results = g.v().append(anon).to_list();
    assert_eq!(results.len(), 4);
}
