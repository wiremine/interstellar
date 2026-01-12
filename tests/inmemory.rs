use intersteller::graph::Graph;
use intersteller::storage::{GraphStorage, InMemoryGraph};
use intersteller::value::Value;
use std::collections::HashMap;

#[test]
fn graph_in_memory_basic_usage() {
    let graph = Graph::in_memory();

    // Verify empty graph
    let snapshot = graph.snapshot();
    assert_eq!(snapshot.graph.storage().vertex_count(), 0);

    // Test traversal API
    let _g = snapshot.traversal();
}

#[test]
fn scale_test_10k_vertices_100k_edges() {
    let mut storage = InMemoryGraph::new();

    // Add 10,000 vertices
    let vertex_ids: Vec<_> = (0..10_000)
        .map(|i| {
            let mut props = HashMap::new();
            props.insert("index".to_string(), Value::Int(i));
            storage.add_vertex("node", props)
        })
        .collect();

    assert_eq!(storage.vertex_count(), 10_000);

    // Add 100,000 edges (random connections)
    let mut edge_count = 0;
    for i in 0..10_000 {
        for j in 0..10 {
            let src = vertex_ids[i];
            let dst = vertex_ids[(i + j + 1) % 10_000];
            storage
                .add_edge(src, dst, "connects", HashMap::new())
                .unwrap();
            edge_count += 1;
        }
    }

    assert_eq!(storage.edge_count(), edge_count);

    // Verify lookups work
    let v = storage.get_vertex(vertex_ids[5000]).unwrap();
    assert_eq!(v.properties.get("index"), Some(&Value::Int(5000)));

    // Verify adjacency
    let out: Vec<_> = storage.out_edges(vertex_ids[0]).collect();
    assert_eq!(out.len(), 10);

    // Verify label scan
    let all_nodes: Vec<_> = storage.vertices_with_label("node").collect();
    assert_eq!(all_nodes.len(), 10_000);
}

#[test]
fn label_index_performance() {
    let mut storage = InMemoryGraph::new();

    // Add mixed labels
    for _ in 0..1000 {
        storage.add_vertex("person", HashMap::new());
    }
    for _ in 0..500 {
        storage.add_vertex("software", HashMap::new());
    }
    for _ in 0..200 {
        storage.add_vertex("company", HashMap::new());
    }

    // Label scans should be efficient
    let people: Vec<_> = storage.vertices_with_label("person").collect();
    let software: Vec<_> = storage.vertices_with_label("software").collect();
    let companies: Vec<_> = storage.vertices_with_label("company").collect();

    assert_eq!(people.len(), 1000);
    assert_eq!(software.len(), 500);
    assert_eq!(companies.len(), 200);
}
