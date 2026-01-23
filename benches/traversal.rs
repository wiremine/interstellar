//! Benchmarks for the traversal engine.
//!
//! Run with: `cargo bench --bench traversal`
//!
//! These benchmarks measure traversal performance for the in-memory Graph
//! using the COW-based storage with the Gremlin-style fluent API.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use interstellar::prelude::*;
use interstellar::storage::{Graph, GraphStorage};
use std::collections::HashMap;

/// Create a benchmark graph with specified number of vertices and edges.
///
/// Creates:
/// - `num_vertices` vertices with alternating "person" and "software" labels
/// - `num_edges` edges randomly connecting vertices with "knows" or "uses" labels
///
/// Each vertex has a "name" property and "age" (for persons) or "version" (for software).
fn create_benchmark_graph(num_vertices: usize, num_edges: usize) -> Graph {
    let graph = Graph::new();

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
        let id = graph.add_vertex(label, props);
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
        let _ = graph.add_edge(vertex_ids[src_idx], vertex_ids[dst_idx], label, props);
    }

    graph
}

// =============================================================================
// Basic Traversal Benchmarks
// =============================================================================

/// Benchmark: v().count()
fn bench_v_count(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function("inmemory: v().count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v().count())
        })
    });
}

/// Benchmark: v().has_label("person").count()
fn bench_v_has_label_count(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function("inmemory: v().has_label(\"person\").count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v().has_label("person").count())
        })
    });
}

/// Benchmark: v().out().limit(100).count()
fn bench_v_out_limit_count(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function("inmemory: v().out().limit(100).count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v().out().limit(100).count())
        })
    });
}

/// Benchmark: v().out().out().dedup().count()
fn bench_v_out_out_dedup_count(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function("inmemory: v().out().out().dedup().count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v().out().out().dedup().count())
        })
    });
}

/// Benchmark: v().out().out().out().dedup().count() (3-hop)
fn bench_v_out_out_out_dedup_count(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function("inmemory: v().out().out().out().dedup().count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v().out().out().out().dedup().count())
        })
    });
}

/// Benchmark: v().has_label("person").values("name").to_list()
fn bench_v_has_label_values(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function(
        "inmemory: v().has_label(\"person\").values(\"name\").to_list()",
        |b| {
            b.iter(|| {
                let snapshot = graph.snapshot();
                let g = snapshot.gremlin();
                black_box(g.v().has_label("person").values("name").to_list())
            })
        },
    );
}

/// Benchmark: v().out_e().in_v().count()
fn bench_v_out_e_in_v_count(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function("inmemory: v().out_e().in_v().count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v().out_e().in_v().count())
        })
    });
}

/// Benchmark: Small graph traversal (for comparison)
fn bench_small_graph_complex_traversal(c: &mut Criterion) {
    let graph = create_benchmark_graph(100, 500);

    c.bench_function(
        "inmemory: small_graph v().out().out().has_label().dedup().count()",
        |b| {
            b.iter(|| {
                let snapshot = graph.snapshot();
                let g = snapshot.gremlin();
                black_box(g.v().out().out().has_label("person").dedup().count())
            })
        },
    );
}

// =============================================================================
// Throughput Benchmarks - Vertices/Edges per Second
// =============================================================================

/// Benchmark vertex write throughput
fn bench_vertex_write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("inmemory: vertex_write_throughput");

    for size in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_with_setup(
                || Graph::new(),
                |graph| {
                    for i in 0..size {
                        let props = HashMap::from([("i".to_string(), Value::Int(i as i64))]);
                        black_box(graph.add_vertex("person", props));
                    }
                },
            )
        });
    }

    group.finish();
}

/// Benchmark edge write throughput
fn bench_edge_write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("inmemory: edge_write_throughput");

    for size in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_with_setup(
                || {
                    let graph = Graph::new();
                    let mut vertex_ids = Vec::with_capacity(1000);
                    for i in 0..1000 {
                        let props = HashMap::from([("i".to_string(), Value::Int(i as i64))]);
                        vertex_ids.push(graph.add_vertex("person", props));
                    }
                    (graph, vertex_ids)
                },
                |(graph, vertex_ids)| {
                    for i in 0..size {
                        let src = vertex_ids[i % 1000];
                        let dst = vertex_ids[(i + 1) % 1000];
                        let props = HashMap::from([("w".to_string(), Value::Int(i as i64))]);
                        let _ = black_box(graph.add_edge(src, dst, "knows", props));
                    }
                },
            )
        });
    }

    group.finish();
}

/// Benchmark vertex read throughput (single lookups)
fn bench_vertex_read_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("inmemory: vertex_read_throughput");

    for size in [100, 1_000, 10_000] {
        // Create graph with `size` vertices
        let graph = create_benchmark_graph(size, 0);

        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let snapshot = graph.snapshot();
                for i in 0..size as u64 {
                    black_box(snapshot.get_vertex(VertexId(i)));
                }
            })
        });
    }

    group.finish();
}

/// Benchmark traversal throughput (vertices processed per second)
fn bench_traversal_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("inmemory: traversal_throughput");

    for size in [1_000, 10_000, 50_000] {
        let graph = create_benchmark_graph(size, size * 5);

        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("v().to_list()", size), &size, |b, _| {
            b.iter(|| {
                let snapshot = graph.snapshot();
                let g = snapshot.gremlin();
                black_box(g.v().to_list())
            })
        });
    }

    group.finish();
}

// =============================================================================
// Common Query Pattern Benchmarks
// =============================================================================

/// Benchmark: Find neighbors of a specific vertex
fn bench_find_neighbors(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function(
        "inmemory: v(id).out().to_list() [single vertex neighbors]",
        |b| {
            b.iter(|| {
                let snapshot = graph.snapshot();
                let g = snapshot.gremlin();
                // Get neighbors of vertex 0
                black_box(g.v_ids([VertexId(0)]).out().to_list())
            })
        },
    );
}

/// Benchmark: Find vertices with property filter
fn bench_property_filter(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function("inmemory: v().has(\"age\", 42).to_list()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v().has_value("age", Value::Int(42)).to_list())
        })
    });
}

/// Benchmark: Path query (friends of friends)
fn bench_friends_of_friends(c: &mut Criterion) {
    let graph = create_benchmark_graph(1_000, 10_000);

    c.bench_function(
        "inmemory: v(id).out(\"knows\").out(\"knows\").dedup().to_list()",
        |b| {
            b.iter(|| {
                let snapshot = graph.snapshot();
                let g = snapshot.gremlin();
                black_box(
                    g.v_ids([VertexId(0)])
                        .out_labels(&["knows"])
                        .out_labels(&["knows"])
                        .dedup()
                        .to_list(),
                )
            })
        },
    );
}

/// Benchmark: Aggregate query (count by label)
fn bench_count_by_label(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function(
        "inmemory: v().has_label(\"person\").count() + v().has_label(\"software\").count()",
        |b| {
            b.iter(|| {
                let snapshot = graph.snapshot();
                let g = snapshot.gremlin();
                let person_count = g.v().has_label("person").count();
                let g2 = snapshot.gremlin();
                let software_count = g2.v().has_label("software").count();
                black_box((person_count, software_count))
            })
        },
    );
}

criterion_group!(
    basic_benches,
    bench_v_count,
    bench_v_has_label_count,
    bench_v_out_limit_count,
    bench_v_out_out_dedup_count,
    bench_v_out_out_out_dedup_count,
    bench_v_has_label_values,
    bench_v_out_e_in_v_count,
    bench_small_graph_complex_traversal,
);

criterion_group!(
    throughput_benches,
    bench_vertex_write_throughput,
    bench_edge_write_throughput,
    bench_vertex_read_throughput,
    bench_traversal_throughput,
);

criterion_group!(
    query_benches,
    bench_find_neighbors,
    bench_property_filter,
    bench_friends_of_friends,
    bench_count_by_label,
);

criterion_main!(basic_benches, throughput_benches, query_benches);
