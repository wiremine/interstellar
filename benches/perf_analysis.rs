//! Performance analysis benchmarks for Interstellar Graph Database.
//!
//! Run with: `cargo bench --bench perf_analysis`
//!
//! These benchmarks are designed to measure specific performance issues
//! documented in `perf-improvements/README.md` to:
//! 1. Establish baselines for optimization work
//! 2. Identify bottlenecks in the traversal engine
//! 3. Measure the impact of fixes
//!
//! Key issues being measured:
//! - Step count scaling (eager collection vs lazy evaluation)
//! - count() vs direct storage access
//! - Label comparison overhead (string vs ID)
//! - Navigation step overhead
//! - Concurrent read patterns

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use interstellar::prelude::*;
use interstellar::storage::{Graph, GraphStorage};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

// =============================================================================
// Helper Functions
// =============================================================================

/// Create a benchmark graph with specified number of vertices and edges.
///
/// Creates:
/// - `num_vertices` vertices with alternating "person" and "software" labels
/// - `num_edges` edges connecting vertices with "knows" or "uses" labels
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
        let dst_idx = (i * 7 + 13) % num_vertices;

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

/// Create a graph with many unique labels for label comparison benchmarks.
fn create_multi_label_graph(num_vertices: usize, num_labels: usize) -> Graph {
    let graph = Graph::new();

    for i in 0..num_vertices {
        let label = format!("label_{}", i % num_labels);
        let mut props = HashMap::new();
        props.insert("id".to_string(), Value::Int(i as i64));
        graph.add_vertex(&label, props);
    }

    graph
}

// =============================================================================
// Issue 1: Step Count Scaling (Eager Collection vs Lazy Evaluation)
// =============================================================================

/// Measures how traversal time scales with the number of steps.
///
/// If lazy evaluation is working, adding more steps should have minimal overhead
/// until terminal step execution. If eager collection is happening after each step,
/// time will scale linearly with step count.
///
/// Expected behavior (if lazy): O(N) regardless of step count
/// Actual behavior (with eager collect): O(N × steps)
fn bench_step_count_scaling(c: &mut Criterion) {
    let graph = create_benchmark_graph(1_000, 5_000);

    let mut group = c.benchmark_group("perf: step_count_scaling");
    group.throughput(Throughput::Elements(1_000));

    // 1 step
    group.bench_function("v().count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v().count())
        })
    });

    // 2 steps
    group.bench_function("v().out().count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v().out().count())
        })
    });

    // 3 steps
    group.bench_function("v().out().out().count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v().out().out().count())
        })
    });

    // 4 steps
    group.bench_function("v().out().out().out().count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v().out().out().out().count())
        })
    });

    // 5 steps (with dedup to limit explosion)
    group.bench_function("v().out().out().out().out().dedup().count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v().out().out().out().out().dedup().count())
        })
    });

    group.finish();
}

// =============================================================================
// Issue 2: count() vs Direct Storage Access
// =============================================================================

/// Compares g.v().count() (traversal) vs storage.vertex_count() (direct).
///
/// The traversal version should be nearly as fast as direct access for simple
/// counts. If it's significantly slower, it indicates eager materialization.
///
/// Current issue: count() calls execute() which collects ALL traversers,
/// then just returns .len() - wasting O(N) memory for O(1) operation.
fn bench_count_vs_direct(c: &mut Criterion) {
    let mut group = c.benchmark_group("perf: count_vs_direct");

    for size in [1_000, 10_000, 100_000] {
        let graph = create_benchmark_graph(size, 0);

        group.throughput(Throughput::Elements(size as u64));

        // Traversal count (current - eager)
        group.bench_with_input(BenchmarkId::new("g.v().count()", size), &size, |b, _| {
            b.iter(|| {
                let snapshot = graph.snapshot();
                let g = snapshot.gremlin();
                black_box(g.v().count())
            })
        });

        // Direct storage count (baseline)
        group.bench_with_input(
            BenchmarkId::new("snapshot.vertex_count()", size),
            &size,
            |b, _| {
                b.iter(|| {
                    let snapshot = graph.snapshot();
                    black_box(snapshot.vertex_count())
                })
            },
        );

        // Iterator count (middle ground)
        group.bench_with_input(
            BenchmarkId::new("all_vertices().count()", size),
            &size,
            |b, _| {
                b.iter(|| {
                    let snapshot = graph.snapshot();
                    black_box(snapshot.all_vertices().count())
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Issue 3: Label Comparison Overhead (String vs ID)
// =============================================================================

/// Measures has_label() performance with varying numbers of unique labels.
///
/// Current issue: HasLabelStep compares strings instead of interned label IDs.
/// With many unique labels, string comparison becomes expensive.
///
/// This benchmark tests:
/// - 1 unique label (best case for string comparison)
/// - 10 unique labels (moderate)
/// - 100 unique labels (worst case - long strings, many comparisons)
fn bench_has_label_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("perf: has_label_scaling");
    let num_vertices = 10_000;

    for num_labels in [1, 10, 100] {
        let graph = create_multi_label_graph(num_vertices, num_labels);
        let target_label = "label_0"; // Always exists

        group.bench_with_input(
            BenchmarkId::new("has_label()", num_labels),
            &num_labels,
            |b, _| {
                b.iter(|| {
                    let snapshot = graph.snapshot();
                    let g = snapshot.gremlin();
                    black_box(g.v().has_label(target_label).count())
                })
            },
        );
    }

    // Compare against direct iteration with label filter
    let graph = create_multi_label_graph(num_vertices, 100);
    group.bench_function("all_vertices().filter(label==)", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            black_box(
                snapshot
                    .all_vertices()
                    .filter(|v| v.label == "label_0")
                    .count(),
            )
        })
    });

    group.finish();
}

// =============================================================================
// Issue 4: Navigation Step Overhead
// =============================================================================

/// Measures the overhead of navigation steps (out, in, both).
///
/// Each navigation step:
/// - Allocates a label Vec (if labels specified)
/// - Boxes the iterator
/// - Collects results (due to eager execution)
///
/// This benchmark isolates navigation overhead from step count scaling.
fn bench_navigation_step_overhead(c: &mut Criterion) {
    let graph = create_benchmark_graph(1_000, 5_000);

    let mut group = c.benchmark_group("perf: navigation_overhead");

    // Single vertex, out() - measures pure navigation cost
    group.bench_function("v(0).out().count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v_ids([VertexId(0)]).out().count())
        })
    });

    // Single vertex, out() with label filter
    group.bench_function("v(0).out(\"knows\").count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v_ids([VertexId(0)]).out_labels(&["knows"]).count())
        })
    });

    // Single vertex, out_e().in_v() - explicit edge traversal
    group.bench_function("v(0).out_e().in_v().count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v_ids([VertexId(0)]).out_e().in_v().count())
        })
    });

    // Direct storage access (baseline)
    group.bench_function("storage.out_edges(0).count()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            black_box(snapshot.out_edges(VertexId(0)).count())
        })
    });

    group.finish();
}

// =============================================================================
// Issue 5: Iterator Boxing Overhead
// =============================================================================

/// Measures the overhead of boxed iterators in the storage layer.
///
/// Every storage method returns Box<dyn Iterator>, which:
/// - Allocates on the heap
/// - Prevents inlining
/// - Adds vtable dispatch overhead
///
/// This compares boxed (current) vs what concrete iterators might achieve.
fn bench_iterator_boxing(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 50_000);

    let mut group = c.benchmark_group("perf: iterator_boxing");
    group.throughput(Throughput::Elements(10_000));

    // Current: all_vertices() returns Box<dyn Iterator>
    group.bench_function("all_vertices() [boxed]", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            black_box(snapshot.all_vertices().count())
        })
    });

    // Comparison: collect to vec then iterate (removes boxing overhead, adds allocation)
    // This helps isolate whether boxing or the iterator itself is the bottleneck
    group.bench_function("all_vertices().collect::<Vec<_>>().len()", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let vertices: Vec<_> = snapshot.all_vertices().collect();
            black_box(vertices.len())
        })
    });

    group.finish();
}

// =============================================================================
// Issue 7: Concurrent Read Patterns
// =============================================================================

/// Measures performance under concurrent read access.
///
/// Current issue: Multiple lock acquisitions within single methods may cause
/// contention under concurrent access.
///
/// This benchmark compares:
/// - Single-threaded baseline
/// - Multi-threaded with potential contention
fn bench_concurrent_reads(c: &mut Criterion) {
    let graph = Arc::new(create_benchmark_graph(10_000, 50_000));

    let mut group = c.benchmark_group("perf: concurrent_reads");

    // Single-threaded baseline
    group.bench_function("single_thread: 1000 reads", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            for i in 0..1000 {
                black_box(snapshot.get_vertex(VertexId(i % 10_000)));
            }
        })
    });

    // Multi-threaded (4 threads)
    group.bench_function("4_threads: 250 reads each", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..4)
                .map(|thread_id| {
                    let graph_clone = Arc::clone(&graph);
                    thread::spawn(move || {
                        let snapshot = graph_clone.snapshot();
                        for i in 0..250 {
                            let vertex_id = (thread_id * 250 + i) % 10_000;
                            black_box(snapshot.get_vertex(VertexId(vertex_id as u64)));
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        })
    });

    // Multi-threaded traversals
    group.bench_function("4_threads: v().count() each", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    let graph_clone = Arc::clone(&graph);
                    thread::spawn(move || {
                        let snapshot = graph_clone.snapshot();
                        let g = snapshot.gremlin();
                        black_box(g.v().count())
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        })
    });

    group.finish();
}

// =============================================================================
// Additional: Memory Pressure Benchmarks
// =============================================================================

/// Measures how traversal performance degrades under memory pressure.
///
/// The eager collection after each step creates O(N × steps) intermediate
/// allocations. This benchmark shows the impact on larger graphs.
fn bench_memory_pressure(c: &mut Criterion) {
    let mut group = c.benchmark_group("perf: memory_pressure");

    for size in [1_000, 10_000, 50_000] {
        let graph = create_benchmark_graph(size, size * 5);

        group.throughput(Throughput::Elements(size as u64));

        // Simple count (baseline)
        group.bench_with_input(BenchmarkId::new("v().count()", size), &size, |b, _| {
            b.iter(|| {
                let snapshot = graph.snapshot();
                let g = snapshot.gremlin();
                black_box(g.v().count())
            })
        });

        // Two-hop with dedup (moderate pressure)
        group.bench_with_input(
            BenchmarkId::new("v().out().out().dedup().count()", size),
            &size,
            |b, _| {
                b.iter(|| {
                    let snapshot = graph.snapshot();
                    let g = snapshot.gremlin();
                    black_box(g.v().out().out().dedup().count())
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Summary: Key Metrics Benchmark
// =============================================================================

/// A quick summary benchmark that shows key performance metrics.
///
/// Run this to get a quick overview of current performance characteristics.
fn bench_key_metrics(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 50_000);

    let mut group = c.benchmark_group("perf: key_metrics");

    // Metric 1: Simple count
    group.bench_function("v().count() [10K vertices]", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v().count())
        })
    });

    // Metric 2: Filtered count
    group.bench_function("v().has_label().count() [10K vertices]", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v().has_label("person").count())
        })
    });

    // Metric 3: Two-hop traversal
    group.bench_function(
        "v().out().out().dedup().count() [10K vertices, 50K edges]",
        |b| {
            b.iter(|| {
                let snapshot = graph.snapshot();
                let g = snapshot.gremlin();
                black_box(g.v().out().out().dedup().count())
            })
        },
    );

    // Metric 4: Direct storage (baseline)
    group.bench_function("vertex_count() [direct]", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            black_box(snapshot.vertex_count())
        })
    });

    group.finish();
}

// =============================================================================
// Criterion Groups
// =============================================================================

criterion_group!(step_scaling, bench_step_count_scaling,);

criterion_group!(count_optimization, bench_count_vs_direct,);

criterion_group!(label_comparison, bench_has_label_scaling,);

criterion_group!(
    navigation,
    bench_navigation_step_overhead,
    bench_iterator_boxing,
);

criterion_group!(concurrency, bench_concurrent_reads,);

criterion_group!(memory, bench_memory_pressure,);

criterion_group!(key_metrics, bench_key_metrics,);

criterion_main!(
    step_scaling,
    count_optimization,
    label_comparison,
    navigation,
    concurrency,
    memory,
    key_metrics
);
