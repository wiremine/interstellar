//! Complex traversal tests.

use interstellar::value::Value;

use crate::common::graphs::create_small_graph;

#[test]
fn find_friends_of_friends() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Alice -> knows -> ? -> knows -> ?
    let fof = g
        .v_ids([tg.alice])
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .to_list();

    assert_eq!(fof.len(), 1);
    assert_eq!(fof[0].as_vertex_id(), Some(tg.charlie));
}

#[test]
fn find_cycle_back_to_start() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Alice -> knows -> Bob -> knows -> Charlie -> knows -> Alice
    let cycle = g
        .v_ids([tg.alice])
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .to_list();

    assert_eq!(cycle.len(), 1);
    assert_eq!(cycle[0].as_vertex_id(), Some(tg.alice)); // Back to Alice
}

#[test]
fn find_software_used_by_people_who_know_alice() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // People who know Alice -> uses -> software
    let software = g
        .v_ids([tg.alice])
        .in_labels(&["knows"])
        .out_labels(&["uses"])
        .has_label("software")
        .to_list();

    // Charlie knows Alice, but Charlie doesn't use any software
    assert_eq!(software.len(), 0);
}

#[test]
fn count_edges_per_vertex() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Count all incident edges for each vertex
    let alice_edges = g.v_ids([tg.alice]).both_e().count();
    assert_eq!(alice_edges, 3); // 2 out (knows Bob, uses GraphDB) + 1 in (Charlie knows)

    let bob_edges = g.v_ids([tg.bob]).both_e().count();
    assert_eq!(bob_edges, 3); // 2 out (knows Charlie, uses GraphDB) + 1 in (Alice knows)
}

#[test]
fn get_all_names_in_graph() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let names = g.v().values("name").to_list();
    assert_eq!(names.len(), 4);

    let name_strs: Vec<String> = names
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();
    assert!(name_strs.contains(&"Alice".to_string()));
    assert!(name_strs.contains(&"Bob".to_string()));
    assert!(name_strs.contains(&"Charlie".to_string()));
    assert!(name_strs.contains(&"GraphDB".to_string()));
}

#[test]
fn get_unique_labels() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let labels = g.v().label().dedup().to_list();
    assert_eq!(labels.len(), 2);

    let edge_labels = g.e().label().dedup().to_list();
    assert_eq!(edge_labels.len(), 2); // "knows" and "uses"
}

#[test]
fn pagination_with_skip_and_limit() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let page1 = g.v().limit(2).to_list();
    let page2 = g.v().skip(2).limit(2).to_list();

    assert_eq!(page1.len(), 2);
    assert_eq!(page2.len(), 2);

    // Pages should not overlap
    let ids1: Vec<_> = page1.iter().filter_map(|v| v.as_vertex_id()).collect();
    let ids2: Vec<_> = page2.iter().filter_map(|v| v.as_vertex_id()).collect();
    for id in &ids1 {
        assert!(!ids2.contains(id));
    }
}

#[test]
fn sum_ages_of_people() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Get sum of ages: 30 + 25 + 35 = 90
    let result = g.v().has_label("person").values("age").sum();
    assert_eq!(result, Value::Int(90));
}

#[test]
fn traversal_with_path_tracking() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Enable path tracking and get paths
    let paths = g
        .v_ids([tg.alice])
        .with_path()
        .as_("start")
        .out_labels(&["knows"])
        .as_("friend")
        .out_labels(&["knows"])
        .as_("fof")
        .path()
        .to_list();

    assert_eq!(paths.len(), 1);
    if let Value::List(path) = &paths[0] {
        assert_eq!(path.len(), 3); // start, friend, fof
    } else {
        panic!("Expected path list");
    }
}

#[test]
fn select_multiple_labels() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g
        .v_ids([tg.alice])
        .as_("person")
        .out_labels(&["uses"])
        .as_("software")
        .select(&["person", "software"])
        .to_list();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert!(map.contains_key("person"));
        assert!(map.contains_key("software"));
    } else {
        panic!("Expected map");
    }
}

#[test]
fn edge_property_access() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Get "since" property from knows edges
    let since_values = g.e().has_label("knows").values("since").to_list();

    assert_eq!(since_values.len(), 3);
    for v in &since_values {
        assert!(v.as_i64().is_some());
    }
}

#[test]
fn combining_filters_and_navigation() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Find people who are older than 27 and know someone
    let results = g
        .v()
        .has_label("person")
        .has("age")
        .filter(|_ctx, v| {
            if let Some(vertex_id) = v.as_vertex_id() {
                // This is a simplification - in real code you'd check the age property
                vertex_id.0 != 1 // Filter out Bob (id=1, age=25)
            } else {
                false
            }
        })
        .out_labels(&["knows"])
        .to_list();

    // Alice and Charlie both know someone
    assert!(!results.is_empty());
}
