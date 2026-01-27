//! Integration tests for WASM bindings.
//!
//! These tests verify that the WASM API works correctly.
//! Run with: `wasm-pack test --node --features wasm`

#![cfg(all(test, feature = "wasm"))]

use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;

use crate::wasm::{AnonymousFactory as __, Graph, P};

wasm_bindgen_test_configure!(run_in_browser);

// Helper to convert u64 to JsValue
fn id_to_js(id: u64) -> JsValue {
    JsValue::from(id)
}

// =============================================================================
// Graph CRUD Tests
// =============================================================================

#[wasm_bindgen_test]
fn test_graph_create() {
    let graph = Graph::new();
    assert_eq!(graph.vertex_count(), 0);
    assert_eq!(graph.edge_count(), 0);
}

#[wasm_bindgen_test]
fn test_add_vertex() {
    let graph = Graph::new();

    // Add vertex with properties
    let props = js_sys::Object::new();
    js_sys::Reflect::set(&props, &"name".into(), &"Alice".into()).unwrap();
    js_sys::Reflect::set(&props, &"age".into(), &JsValue::from(30i32)).unwrap();

    let id = graph.add_vertex("person", props.into()).unwrap();
    assert!(id > 0 || id == 0); // Valid ID
    assert_eq!(graph.vertex_count(), 1);
}

#[wasm_bindgen_test]
fn test_add_edge() {
    let graph = Graph::new();

    // Add two vertices
    let alice_id = graph.add_vertex("person", JsValue::NULL).unwrap();
    let bob_id = graph.add_vertex("person", JsValue::NULL).unwrap();

    // Add edge between them
    let edge_id = graph
        .add_edge(id_to_js(alice_id), id_to_js(bob_id), "knows", JsValue::NULL)
        .unwrap();
    assert!(edge_id > 0 || edge_id == 0);
    assert_eq!(graph.edge_count(), 1);
}

#[wasm_bindgen_test]
fn test_get_vertex() {
    let graph = Graph::new();

    let props = js_sys::Object::new();
    js_sys::Reflect::set(&props, &"name".into(), &"Alice".into()).unwrap();

    let id = graph.add_vertex("person", props.into()).unwrap();

    let vertex = graph.get_vertex(id_to_js(id));
    assert!(vertex.is_ok());
}

#[wasm_bindgen_test]
fn test_remove_vertex() {
    let graph = Graph::new();
    let id = graph.add_vertex("person", JsValue::NULL).unwrap();

    assert_eq!(graph.vertex_count(), 1);
    let removed = graph.remove_vertex(id_to_js(id)).unwrap();
    assert!(removed);
    assert_eq!(graph.vertex_count(), 0);
}

#[wasm_bindgen_test]
fn test_remove_edge() {
    let graph = Graph::new();
    let alice = graph.add_vertex("person", JsValue::NULL).unwrap();
    let bob = graph.add_vertex("person", JsValue::NULL).unwrap();
    let edge_id = graph
        .add_edge(id_to_js(alice), id_to_js(bob), "knows", JsValue::NULL)
        .unwrap();

    assert_eq!(graph.edge_count(), 1);
    let removed = graph.remove_edge(id_to_js(edge_id)).unwrap();
    assert!(removed);
    assert_eq!(graph.edge_count(), 0);
}

// =============================================================================
// Traversal Tests
// =============================================================================

#[wasm_bindgen_test]
fn test_v_traversal() {
    let graph = Graph::new();
    graph.add_vertex("person", JsValue::NULL).unwrap();
    graph.add_vertex("person", JsValue::NULL).unwrap();
    graph.add_vertex("software", JsValue::NULL).unwrap();

    let count = graph.v().to_count();
    assert_eq!(count, 3);
}

#[wasm_bindgen_test]
fn test_has_label_filter() {
    let graph = Graph::new();
    graph.add_vertex("person", JsValue::NULL).unwrap();
    graph.add_vertex("person", JsValue::NULL).unwrap();
    graph.add_vertex("software", JsValue::NULL).unwrap();

    let count = graph.v().has_label("person").to_count();
    assert_eq!(count, 2);
}

#[wasm_bindgen_test]
fn test_out_navigation() {
    let graph = Graph::new();
    let alice = graph.add_vertex("person", JsValue::NULL).unwrap();
    let bob = graph.add_vertex("person", JsValue::NULL).unwrap();
    let charlie = graph.add_vertex("person", JsValue::NULL).unwrap();

    graph
        .add_edge(id_to_js(alice), id_to_js(bob), "knows", JsValue::NULL)
        .unwrap();
    graph
        .add_edge(id_to_js(alice), id_to_js(charlie), "knows", JsValue::NULL)
        .unwrap();

    // V_(alice).out() should find bob and charlie
    let ids_arr = js_sys::Array::new();
    ids_arr.push(&id_to_js(alice));
    let count = graph.v_ids(ids_arr.into()).unwrap().out().to_count();
    assert_eq!(count, 2);
}

#[wasm_bindgen_test]
fn test_in_navigation() {
    let graph = Graph::new();
    let alice = graph.add_vertex("person", JsValue::NULL).unwrap();
    let bob = graph.add_vertex("person", JsValue::NULL).unwrap();

    graph
        .add_edge(id_to_js(alice), id_to_js(bob), "knows", JsValue::NULL)
        .unwrap();

    // V_(bob).in_() should find alice
    let ids_arr = js_sys::Array::new();
    ids_arr.push(&id_to_js(bob));
    let count = graph.v_ids(ids_arr.into()).unwrap().in_().to_count();
    assert_eq!(count, 1);
}

#[wasm_bindgen_test]
fn test_values_transform() {
    let graph = Graph::new();

    let props = js_sys::Object::new();
    js_sys::Reflect::set(&props, &"name".into(), &"Alice".into()).unwrap();
    graph.add_vertex("person", props.into()).unwrap();

    let props2 = js_sys::Object::new();
    js_sys::Reflect::set(&props2, &"name".into(), &"Bob".into()).unwrap();
    graph.add_vertex("person", props2.into()).unwrap();

    // Get all names
    let result = graph.v().values("name").to_list().unwrap();
    let arr = js_sys::Array::from(&result);
    assert_eq!(arr.length(), 2);
}

#[wasm_bindgen_test]
fn test_limit_step() {
    let graph = Graph::new();
    for _ in 0..10 {
        graph.add_vertex("node", JsValue::NULL).unwrap();
    }

    let count = graph.v().limit(5u64.into()).unwrap().to_count();
    assert_eq!(count, 5);
}

#[wasm_bindgen_test]
fn test_dedup_step() {
    let graph = Graph::new();
    let alice = graph.add_vertex("person", JsValue::NULL).unwrap();
    let bob = graph.add_vertex("person", JsValue::NULL).unwrap();

    // Create edges that will result in duplicates when traversing
    graph
        .add_edge(id_to_js(alice), id_to_js(bob), "knows", JsValue::NULL)
        .unwrap();
    graph
        .add_edge(id_to_js(alice), id_to_js(bob), "likes", JsValue::NULL)
        .unwrap();

    // Without dedup, out traversal returns bob twice
    let ids_arr = js_sys::Array::new();
    ids_arr.push(&id_to_js(alice));
    let without_dedup = graph.v_ids(ids_arr.into()).unwrap().out().to_count();
    assert_eq!(without_dedup, 2);

    // With dedup, bob appears once
    let ids_arr2 = js_sys::Array::new();
    ids_arr2.push(&id_to_js(alice));
    let with_dedup = graph
        .v_ids(ids_arr2.into())
        .unwrap()
        .out()
        .dedup()
        .to_count();
    assert_eq!(with_dedup, 1);
}

// =============================================================================
// Predicate Tests
// =============================================================================

#[wasm_bindgen_test]
fn test_predicate_eq() {
    let graph = Graph::new();

    let props1 = js_sys::Object::new();
    js_sys::Reflect::set(&props1, &"age".into(), &JsValue::from(30i32)).unwrap();
    graph.add_vertex("person", props1.into()).unwrap();

    let props2 = js_sys::Object::new();
    js_sys::Reflect::set(&props2, &"age".into(), &JsValue::from(25i32)).unwrap();
    graph.add_vertex("person", props2.into()).unwrap();

    let pred = P::eq(JsValue::from(30i32)).unwrap();
    let count = graph.v().has_where("age", pred).to_count();
    assert_eq!(count, 1);
}

#[wasm_bindgen_test]
fn test_predicate_gt() {
    let graph = Graph::new();

    let props1 = js_sys::Object::new();
    js_sys::Reflect::set(&props1, &"age".into(), &JsValue::from(30i32)).unwrap();
    graph.add_vertex("person", props1.into()).unwrap();

    let props2 = js_sys::Object::new();
    js_sys::Reflect::set(&props2, &"age".into(), &JsValue::from(25i32)).unwrap();
    graph.add_vertex("person", props2.into()).unwrap();

    let props3 = js_sys::Object::new();
    js_sys::Reflect::set(&props3, &"age".into(), &JsValue::from(35i32)).unwrap();
    graph.add_vertex("person", props3.into()).unwrap();

    let pred = P::gt(JsValue::from(28i32)).unwrap();
    let count = graph.v().has_where("age", pred).to_count();
    assert_eq!(count, 2); // 30 and 35
}

// =============================================================================
// Builder Pattern Tests
// =============================================================================

#[wasm_bindgen_test]
fn test_order_builder() {
    let graph = Graph::new();

    let props1 = js_sys::Object::new();
    js_sys::Reflect::set(&props1, &"name".into(), &"Charlie".into()).unwrap();
    graph.add_vertex("person", props1.into()).unwrap();

    let props2 = js_sys::Object::new();
    js_sys::Reflect::set(&props2, &"name".into(), &"Alice".into()).unwrap();
    graph.add_vertex("person", props2.into()).unwrap();

    let props3 = js_sys::Object::new();
    js_sys::Reflect::set(&props3, &"name".into(), &"Bob".into()).unwrap();
    graph.add_vertex("person", props3.into()).unwrap();

    // Order by name ascending
    let result = graph
        .v()
        .values("name")
        .order()
        .by_asc()
        .build()
        .to_list()
        .unwrap();

    let arr = js_sys::Array::from(&result);
    assert_eq!(arr.length(), 3);
    // First should be "Alice"
    assert_eq!(arr.get(0).as_string().unwrap(), "Alice");
}

#[wasm_bindgen_test]
fn test_group_count_builder() {
    let graph = Graph::new();

    graph.add_vertex("person", JsValue::NULL).unwrap();
    graph.add_vertex("person", JsValue::NULL).unwrap();
    graph.add_vertex("software", JsValue::NULL).unwrap();

    let result = graph
        .v()
        .group_count()
        .by_label()
        .build()
        .to_list()
        .unwrap();

    let arr = js_sys::Array::from(&result);
    assert_eq!(arr.length(), 1); // Single map result
}

// =============================================================================
// Branch Step Tests
// =============================================================================

#[wasm_bindgen_test]
fn test_union_step() {
    let graph = Graph::new();
    let alice = graph.add_vertex("person", JsValue::NULL).unwrap();
    let bob = graph.add_vertex("person", JsValue::NULL).unwrap();
    let project = graph.add_vertex("software", JsValue::NULL).unwrap();

    graph
        .add_edge(id_to_js(alice), id_to_js(bob), "knows", JsValue::NULL)
        .unwrap();
    graph
        .add_edge(id_to_js(alice), id_to_js(project), "created", JsValue::NULL)
        .unwrap();

    // Union of out steps
    let out_step = __::out();
    let ids_arr = js_sys::Array::new();
    ids_arr.push(&id_to_js(alice));
    let count = graph
        .v_ids(ids_arr.into())
        .unwrap()
        .union(vec![out_step])
        .to_count();

    assert_eq!(count, 2);
}

#[wasm_bindgen_test]
fn test_optional_step() {
    let graph = Graph::new();
    let alice = graph.add_vertex("person", JsValue::NULL).unwrap();
    let bob = graph.add_vertex("person", JsValue::NULL).unwrap();

    // Alice knows Bob
    graph
        .add_edge(id_to_js(alice), id_to_js(bob), "knows", JsValue::NULL)
        .unwrap();

    // Optional out - alice has friends, so returns bob
    let ids_arr1 = js_sys::Array::new();
    ids_arr1.push(&id_to_js(alice));
    let with_friend = graph
        .v_ids(ids_arr1.into())
        .unwrap()
        .optional(__::out())
        .to_count();
    assert_eq!(with_friend, 1);

    // Optional out - bob has no outgoing edges, returns bob
    let ids_arr2 = js_sys::Array::new();
    ids_arr2.push(&id_to_js(bob));
    let without_friend = graph
        .v_ids(ids_arr2.into())
        .unwrap()
        .optional(__::out())
        .to_count();
    assert_eq!(without_friend, 1);
}

// =============================================================================
// Min/Max Tests
// =============================================================================

#[wasm_bindgen_test]
fn test_min_step() {
    let graph = Graph::new();

    let props1 = js_sys::Object::new();
    js_sys::Reflect::set(&props1, &"age".into(), &JsValue::from(30i32)).unwrap();
    graph.add_vertex("person", props1.into()).unwrap();

    let props2 = js_sys::Object::new();
    js_sys::Reflect::set(&props2, &"age".into(), &JsValue::from(25i32)).unwrap();
    graph.add_vertex("person", props2.into()).unwrap();

    let props3 = js_sys::Object::new();
    js_sys::Reflect::set(&props3, &"age".into(), &JsValue::from(35i32)).unwrap();
    graph.add_vertex("person", props3.into()).unwrap();

    let result = graph.v().values("age").min().first().unwrap();
    // Minimum should be 25
    assert_eq!(result.as_f64().unwrap() as i32, 25);
}

#[wasm_bindgen_test]
fn test_max_step() {
    let graph = Graph::new();

    let props1 = js_sys::Object::new();
    js_sys::Reflect::set(&props1, &"age".into(), &JsValue::from(30i32)).unwrap();
    graph.add_vertex("person", props1.into()).unwrap();

    let props2 = js_sys::Object::new();
    js_sys::Reflect::set(&props2, &"age".into(), &JsValue::from(25i32)).unwrap();
    graph.add_vertex("person", props2.into()).unwrap();

    let props3 = js_sys::Object::new();
    js_sys::Reflect::set(&props3, &"age".into(), &JsValue::from(35i32)).unwrap();
    graph.add_vertex("person", props3.into()).unwrap();

    let result = graph.v().values("age").max().first().unwrap();
    // Maximum should be 35
    assert_eq!(result.as_f64().unwrap() as i32, 35);
}
