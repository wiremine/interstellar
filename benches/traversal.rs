//! Benchmarks for the traversal engine.
//!
//! Run with: `cargo bench`

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use interstellar::prelude::*;
use interstellar::storage::InMemoryGraph;
use std::collections::HashMap;
use std::sync::Arc;

/// Create a benchmark graph with specified number of vertices and edges.
///
/// Creates:
/// - `num_vertices` vertices with alternating "person" and "software" labels
/// - `num_edges` edges randomly connecting vertices with "knows" or "uses" labels
///
/// Each vertex has a "name" property and "age" (for persons) or "version" (for software).
fn create_benchmark_graph(num_vertices: usize, num_edges: usize) -> Graph {
    let mut storage = InMemoryGraph::new();

    // Create vertices
    let mut vertex_ids = Vec::with_capacity(num_vertices);
    for i in 0..num_vertices {
        let (label, props) = if i % 2 == 0 {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String(format!("person_{}", i)));
            props.insert("age".to_string(), Value::Int((i % 100) as i64));
            ("person", props)
        } else {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String(format!("software_{}", i)));
            props.insert(
                "version".to_string(),
                Value::String(format!("1.{}", i % 10)),
            );
            ("software", props)
        };
        let id = storage.add_vertex(label, props);
        vertex_ids.push(id);
    }

    // Create edges - use deterministic pattern for reproducibility
    for i in 0..num_edges {
        let src_idx = i % num_vertices;
        let dst_idx = (i * 7 + 13) % num_vertices; // Pseudo-random but deterministic

        // Skip self-loops
        if src_idx == dst_idx {
            continue;
        }

        let label = if i % 3 == 0 { "knows" } else { "uses" };
        let mut props = HashMap::new();
        props.insert("weight".to_string(), Value::Float((i % 100) as f64 / 10.0));

        // Ignore errors (duplicate edges are fine to skip)
        let _ = storage.add_edge(vertex_ids[src_idx], vertex_ids[dst_idx], label, props);
    }

    Graph::new(Arc::new(storage))
}

/// Benchmark: v().count()
fn bench_v_count(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function("v().count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.traversal();
            black_box(g.v().count())
        })
    });
}

/// Benchmark: v().has_label("person").count()
fn bench_v_has_label_count(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function("v().has_label(\"person\").count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.traversal();
            black_box(g.v().has_label("person").count())
        })
    });
}

/// Benchmark: v().out().limit(100).count()
fn bench_v_out_limit_count(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function("v().out().limit(100).count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.traversal();
            black_box(g.v().out().limit(100).count())
        })
    });
}

/// Benchmark: v().out().out().dedup().count()
fn bench_v_out_out_dedup_count(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function("v().out().out().dedup().count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.traversal();
            black_box(g.v().out().out().dedup().count())
        })
    });
}

/// Benchmark: v().out().out().out().dedup().count() (3-hop)
fn bench_v_out_out_out_dedup_count(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function("v().out().out().out().dedup().count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.traversal();
            black_box(g.v().out().out().out().dedup().count())
        })
    });
}

/// Benchmark: v().has_label("person").values("name").to_list()
fn bench_v_has_label_values(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function(
        "v().has_label(\"person\").values(\"name\").to_list()",
        |b| {
            b.iter(|| {
                let snapshot = graph.snapshot();
                let g = snapshot.traversal();
                black_box(g.v().has_label("person").values("name").to_list())
            })
        },
    );
}

/// Benchmark: v().out_e().in_v().count()
fn bench_v_out_e_in_v_count(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function("v().out_e().in_v().count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.traversal();
            black_box(g.v().out_e().in_v().count())
        })
    });
}

/// Benchmark: Small graph traversal (for comparison)
fn bench_small_graph_complex_traversal(c: &mut Criterion) {
    let graph = create_benchmark_graph(100, 500);

    c.bench_function(
        "small_graph: v().out().out().has_label().dedup().count()",
        |b| {
            b.iter(|| {
                let snapshot = graph.snapshot();
                let g = snapshot.traversal();
                black_box(g.v().out().out().has_label("person").dedup().count())
            })
        },
    );
}

criterion_group!(
    benches,
    bench_v_count,
    bench_v_has_label_count,
    bench_v_out_limit_count,
    bench_v_out_out_dedup_count,
    bench_v_out_out_out_dedup_count,
    bench_v_has_label_values,
    bench_v_out_e_in_v_count,
    bench_small_graph_complex_traversal,
);

criterion_main!(benches);
