//! Transform step tests.

use interstellar::value::Value;

use crate::common::graphs::create_small_graph;

#[test]
fn values_extracts_property() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let names = g.v().has_label("person").values("name").to_list();
    assert_eq!(names.len(), 3);

    let name_strs: Vec<String> = names
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();
    assert!(name_strs.contains(&"Alice".to_string()));
    assert!(name_strs.contains(&"Bob".to_string()));
    assert!(name_strs.contains(&"Charlie".to_string()));
}

#[test]
fn values_multi_extracts_multiple_properties() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Get name and age from person vertices
    let props = g
        .v()
        .has_label("person")
        .limit(1)
        .values_multi(["name", "age"])
        .to_list();
    assert_eq!(props.len(), 2); // One name + one age
}

#[test]
fn id_extracts_element_id() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let ids = g.v().id().to_list();
    assert_eq!(ids.len(), 4);

    for id in &ids {
        assert!(matches!(id, Value::Int(_)));
    }
}

#[test]
fn label_extracts_element_label() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let labels = g.v().label().dedup().to_list();
    assert_eq!(labels.len(), 2); // "person" and "software"
}

#[test]
fn map_transforms_values() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let doubled = g
        .inject([1i64, 2i64, 3i64])
        .map(|_ctx, v| {
            if let Value::Int(n) = v {
                Value::Int(n * 2)
            } else {
                v.clone()
            }
        })
        .to_list();

    assert_eq!(doubled.len(), 3);
    assert_eq!(doubled[0], Value::Int(2));
    assert_eq!(doubled[1], Value::Int(4));
    assert_eq!(doubled[2], Value::Int(6));
}

#[test]
fn flat_map_expands_values() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let expanded = g
        .inject([3i64])
        .flat_map(|_ctx, v| {
            if let Value::Int(n) = v {
                (0..*n).map(Value::Int).collect()
            } else {
                vec![]
            }
        })
        .to_list();

    assert_eq!(expanded.len(), 3);
    assert_eq!(expanded[0], Value::Int(0));
    assert_eq!(expanded[1], Value::Int(1));
    assert_eq!(expanded[2], Value::Int(2));
}

#[test]
fn constant_replaces_with_constant_value() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g.v().limit(3).constant("found").to_list();
    assert_eq!(results.len(), 3);
    for r in &results {
        assert_eq!(*r, Value::String("found".to_string()));
    }
}

#[test]
fn path_returns_traversal_path() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Use as_() to add elements to path
    let paths = g
        .v_ids([tg.alice])
        .as_("start")
        .out_labels(&["knows"])
        .as_("end")
        .path()
        .to_list();

    assert_eq!(paths.len(), 1);
    // Path should be a list
    if let Value::List(list) = &paths[0] {
        assert_eq!(list.len(), 2); // start and end
    } else {
        panic!("Expected Value::List, got {:?}", paths[0]);
    }
}

#[test]
fn as_and_select_labels_and_retrieves() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g
        .v_ids([tg.alice])
        .as_("a")
        .out_labels(&["knows"])
        .as_("b")
        .select(&["a", "b"])
        .to_list();

    assert_eq!(results.len(), 1);
    // Should be a Map
    if let Value::Map(map) = &results[0] {
        assert!(map.contains_key("a"));
        assert!(map.contains_key("b"));
    } else {
        panic!("Expected Value::Map, got {:?}", results[0]);
    }
}

#[test]
fn select_one_retrieves_single_label() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g
        .v_ids([tg.alice])
        .as_("start")
        .out_labels(&["knows"])
        .select_one("start")
        .to_list();

    assert_eq!(results.len(), 1);
    // Should be the vertex directly (not a Map)
    assert!(results[0].is_vertex());
    assert_eq!(results[0].as_vertex_id(), Some(tg.alice));
}
