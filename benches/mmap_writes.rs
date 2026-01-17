//! Write performance benchmarks for memory-mapped storage.
//!
//! Run with: `cargo bench --features mmap --bench mmap_writes`
//!
//! These benchmarks measure write performance comparing:
//! - MmapGraph in batch mode (deferred fsync)
//! - InMemoryGraph (baseline, no persistence)
//!
//! Note: Per-operation durable writes (~5ms per fsync) are not benchmarked
//! as they would dominate the results. For ACID durability without batching,
//! expect ~200 writes/sec on typical SSDs.

#![cfg(feature = "mmap")]

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use interstellar::storage::{InMemoryGraph, MmapGraph};
use interstellar::value::VertexId;
use std::collections::HashMap;
use tempfile::TempDir;

// =============================================================================
// Vertex Write Benchmarks
// =============================================================================

/// Benchmark: Vertex insertion throughput (batch mode)
fn bench_vertex_writes_mmap_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("writes: vertices");

    for size in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("mmap_batch", size), &size, |b, &size| {
            b.iter_with_setup(
                || {
                    let dir = TempDir::new().unwrap();
                    let path = dir.path().join("bench.db");
                    let graph = MmapGraph::open(&path).unwrap();
                    graph.begin_batch().unwrap();
                    (dir, graph)
                },
                |(_dir, graph)| {
                    for i in 0..size {
                        let props = HashMap::from([("i".to_string(), (i as i64).into())]);
                        black_box(graph.add_vertex("person", props).unwrap());
                    }
                    graph.commit_batch().unwrap();
                },
            )
        });

        group.bench_with_input(BenchmarkId::new("inmemory", size), &size, |b, &size| {
            b.iter_with_setup(
                || InMemoryGraph::new(),
                |mut graph| {
                    for i in 0..size {
                        let props = HashMap::from([("i".to_string(), (i as i64).into())]);
                        black_box(graph.add_vertex("person", props));
                    }
                },
            )
        });
    }

    group.finish();
}

// =============================================================================
// Edge Write Benchmarks
// =============================================================================

/// Benchmark: Edge insertion throughput (batch mode)
fn bench_edge_writes_mmap_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("writes: edges");

    for size in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("mmap_batch", size), &size, |b, &size| {
            b.iter_with_setup(
                || {
                    let dir = TempDir::new().unwrap();
                    let path = dir.path().join("bench.db");
                    let graph = MmapGraph::open(&path).unwrap();
                    graph.begin_batch().unwrap();

                    // Pre-create vertices
                    let mut vertex_ids = Vec::with_capacity(1000);
                    for i in 0..1000 {
                        let props = HashMap::from([("i".to_string(), (i as i64).into())]);
                        vertex_ids.push(graph.add_vertex("person", props).unwrap());
                    }
                    graph.commit_batch().unwrap();
                    graph.begin_batch().unwrap();

                    (dir, graph, vertex_ids)
                },
                |(_dir, graph, vertex_ids)| {
                    for i in 0..size {
                        let src = vertex_ids[i % 1000];
                        let dst = vertex_ids[(i + 1) % 1000];
                        let props = HashMap::from([("w".to_string(), (i as i64).into())]);
                        black_box(graph.add_edge(src, dst, "knows", props).unwrap());
                    }
                    graph.commit_batch().unwrap();
                },
            )
        });

        group.bench_with_input(BenchmarkId::new("inmemory", size), &size, |b, &size| {
            b.iter_with_setup(
                || {
                    let mut graph = InMemoryGraph::new();
                    let mut vertex_ids = Vec::with_capacity(1000);
                    for i in 0..1000 {
                        let props = HashMap::from([("i".to_string(), (i as i64).into())]);
                        vertex_ids.push(graph.add_vertex("person", props));
                    }
                    (graph, vertex_ids)
                },
                |(mut graph, vertex_ids)| {
                    for i in 0..size {
                        let src = vertex_ids[i % 1000];
                        let dst = vertex_ids[(i + 1) % 1000];
                        let props = HashMap::from([("w".to_string(), (i as i64).into())]);
                        let _ = black_box(graph.add_edge(src, dst, "knows", props));
                    }
                },
            )
        });
    }

    group.finish();
}

// =============================================================================
// Mixed Workload Benchmark
// =============================================================================

/// Benchmark: Mixed vertex + edge writes
fn bench_mixed_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("writes: mixed");

    // 1000 vertices + 5000 edges = realistic bulk load pattern
    let num_vertices = 1000;
    let num_edges = 5000;
    let total_ops = (num_vertices + num_edges) as u64;

    group.throughput(Throughput::Elements(total_ops));

    group.bench_function("mmap_batch", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                let path = dir.path().join("bench.db");
                let graph = MmapGraph::open(&path).unwrap();
                graph.begin_batch().unwrap();
                (dir, graph)
            },
            |(_dir, graph)| {
                let mut vertex_ids = Vec::with_capacity(num_vertices);
                for i in 0..num_vertices {
                    let props = HashMap::from([("i".to_string(), (i as i64).into())]);
                    vertex_ids.push(graph.add_vertex("person", props).unwrap());
                }
                for i in 0..num_edges {
                    let src = vertex_ids[i % num_vertices];
                    let dst = vertex_ids[(i + 1) % num_vertices];
                    let props = HashMap::from([("w".to_string(), (i as i64).into())]);
                    black_box(graph.add_edge(src, dst, "knows", props).unwrap());
                }
                graph.commit_batch().unwrap();
            },
        )
    });

    group.bench_function("inmemory", |b| {
        b.iter_with_setup(
            || InMemoryGraph::new(),
            |mut graph| {
                let mut vertex_ids = Vec::with_capacity(num_vertices);
                for i in 0..num_vertices {
                    let props = HashMap::from([("i".to_string(), (i as i64).into())]);
                    vertex_ids.push(graph.add_vertex("person", props));
                }
                for i in 0..num_edges {
                    let src = vertex_ids[i % num_vertices];
                    let dst = vertex_ids[(i + 1) % num_vertices];
                    let props = HashMap::from([("w".to_string(), (i as i64).into())]);
                    let _ = black_box(graph.add_edge(src, dst, "knows", props));
                }
            },
        )
    });

    group.finish();
}

// =============================================================================
// Checkpoint Benchmark
// =============================================================================

/// Benchmark: Checkpoint (fsync) after batch writes
fn bench_checkpoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("writes: checkpoint");

    for size in [1_000, 10_000] {
        group.bench_with_input(BenchmarkId::new("checkpoint", size), &size, |b, &size| {
            b.iter_with_setup(
                || {
                    let dir = TempDir::new().unwrap();
                    let path = dir.path().join("bench.db");
                    let graph = MmapGraph::open(&path).unwrap();
                    graph.begin_batch().unwrap();

                    // Pre-populate with data
                    let mut vertex_ids: Vec<VertexId> = Vec::with_capacity(size);
                    for i in 0..size {
                        let props = HashMap::from([("i".to_string(), (i as i64).into())]);
                        vertex_ids.push(graph.add_vertex("person", props).unwrap());
                    }
                    for i in 0..(size * 5) {
                        let src = vertex_ids[i % size];
                        let dst = vertex_ids[(i + 1) % size];
                        let props = HashMap::from([("w".to_string(), (i as i64).into())]);
                        let _ = graph.add_edge(src, dst, "knows", props);
                    }
                    graph.commit_batch().unwrap();

                    (dir, graph)
                },
                |(_dir, graph)| {
                    black_box(graph.checkpoint().unwrap());
                },
            )
        });
    }

    group.finish();
}

criterion_group!(
    write_benches,
    bench_vertex_writes_mmap_batch,
    bench_edge_writes_mmap_batch,
    bench_mixed_writes,
    bench_checkpoint,
);

criterion_main!(write_benches);
