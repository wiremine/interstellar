//! Benchmarks for property index operations.
//!
//! These benchmarks compare indexed vs non-indexed lookups and measure
//! index performance characteristics.
//!
//! Run with: `cargo bench --bench indexes`

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use interstellar::index::IndexBuilder;
use interstellar::storage::Graph;
use interstellar::value::Value;
use std::collections::HashMap;
use std::ops::Bound;

/// Create a graph with the specified number of vertices.
///
/// Each vertex has:
/// - "name": String
/// - "age": Int (distributed 0-99)
/// - "email": String (unique per vertex)
fn create_graph(num_vertices: usize) -> Graph {
    let graph = Graph::new();

    for i in 0..num_vertices {
        graph.add_vertex(
            "person",
            HashMap::from([
                ("name".to_string(), Value::String(format!("person_{}", i))),
                ("age".to_string(), Value::Int((i % 100) as i64)),
                (
                    "email".to_string(),
                    Value::String(format!("user{}@example.com", i)),
                ),
            ]),
        );
    }

    graph
}

/// Create a graph with BTree index on age.
fn create_graph_with_age_index(num_vertices: usize) -> Graph {
    let graph = create_graph(num_vertices);
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("person")
                .property("age")
                .name("idx_age")
                .build()
                .unwrap(),
        )
        .unwrap();
    graph
}

/// Create a graph with unique index on email.
fn create_graph_with_email_index(num_vertices: usize) -> Graph {
    let graph = create_graph(num_vertices);
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("person")
                .property("email")
                .unique()
                .name("idx_email")
                .build()
                .unwrap(),
        )
        .unwrap();
    graph
}

// =============================================================================
// Benchmark: Exact Match (BTree Index vs Scan)
// =============================================================================

/// Benchmark exact match lookup WITHOUT index (O(n) scan).
fn bench_exact_match_no_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("exact_match");

    for size in [1_000, 10_000, 100_000].iter() {
        let graph = create_graph(*size);
        let target_age = Value::Int(42);

        group.bench_with_input(BenchmarkId::new("scan", size), size, |b, _| {
            b.iter(|| {
                let results: Vec<_> = graph
                    .vertices_by_property(Some("person"), "age", &target_age)
                    .collect();
                black_box(results.len())
            })
        });
    }

    group.finish();
}

/// Benchmark exact match lookup WITH BTree index (O(log n)).
fn bench_exact_match_with_btree_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("exact_match");

    for size in [1_000, 10_000, 100_000].iter() {
        let graph = create_graph_with_age_index(*size);
        let target_age = Value::Int(42);

        group.bench_with_input(BenchmarkId::new("btree_index", size), size, |b, _| {
            b.iter(|| {
                let results: Vec<_> = graph
                    .vertices_by_property(Some("person"), "age", &target_age)
                    .collect();
                black_box(results.len())
            })
        });
    }

    group.finish();
}

// =============================================================================
// Benchmark: Unique Index Lookup (O(1))
// =============================================================================

/// Benchmark unique index lookup (O(1)).
fn bench_unique_index_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("unique_lookup");

    for size in [1_000, 10_000, 100_000].iter() {
        let graph = create_graph_with_email_index(*size);
        // Look up an email in the middle of the range
        let target_email = Value::String(format!("user{}@example.com", size / 2));

        group.bench_with_input(BenchmarkId::new("unique_index", size), size, |b, _| {
            b.iter(|| {
                let results: Vec<_> = graph
                    .vertices_by_property(Some("person"), "email", &target_email)
                    .collect();
                black_box(results.len())
            })
        });
    }

    group.finish();
}

/// Benchmark non-indexed email lookup for comparison.
fn bench_email_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("unique_lookup");

    for size in [1_000, 10_000, 100_000].iter() {
        let graph = create_graph(*size); // No index
        let target_email = Value::String(format!("user{}@example.com", size / 2));

        group.bench_with_input(BenchmarkId::new("scan", size), size, |b, _| {
            b.iter(|| {
                let results: Vec<_> = graph
                    .vertices_by_property(Some("person"), "email", &target_email)
                    .collect();
                black_box(results.len())
            })
        });
    }

    group.finish();
}

// =============================================================================
// Benchmark: Range Query
// =============================================================================

/// Benchmark range query WITHOUT index.
fn bench_range_query_no_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_query");

    for size in [1_000, 10_000, 100_000].iter() {
        let graph = create_graph(*size);
        let start = Value::Int(20);
        let end = Value::Int(30);

        group.bench_with_input(BenchmarkId::new("scan", size), size, |b, _| {
            b.iter(|| {
                let results: Vec<_> = graph
                    .vertices_by_property_range(
                        Some("person"),
                        "age",
                        Bound::Included(&start),
                        Bound::Excluded(&end),
                    )
                    .collect();
                black_box(results.len())
            })
        });
    }

    group.finish();
}

/// Benchmark range query WITH BTree index.
fn bench_range_query_with_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_query");

    for size in [1_000, 10_000, 100_000].iter() {
        let graph = create_graph_with_age_index(*size);
        let start = Value::Int(20);
        let end = Value::Int(30);

        group.bench_with_input(BenchmarkId::new("btree_index", size), size, |b, _| {
            b.iter(|| {
                let results: Vec<_> = graph
                    .vertices_by_property_range(
                        Some("person"),
                        "age",
                        Bound::Included(&start),
                        Bound::Excluded(&end),
                    )
                    .collect();
                black_box(results.len())
            })
        });
    }

    group.finish();
}

// =============================================================================
// Benchmark: Index Creation
// =============================================================================

/// Benchmark index creation (populating from existing data).
fn bench_index_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_creation");

    for size in [1_000, 10_000, 100_000].iter() {
        group.bench_with_input(BenchmarkId::new("btree", size), size, |b, &size| {
            b.iter_with_setup(
                || create_graph(size),
                |graph| {
                    graph
                        .create_index(
                            IndexBuilder::vertex()
                                .label("person")
                                .property("age")
                                .build()
                                .unwrap(),
                        )
                        .unwrap();
                    black_box(graph)
                },
            )
        });
    }

    for size in [1_000, 10_000, 100_000].iter() {
        group.bench_with_input(BenchmarkId::new("unique", size), size, |b, &size| {
            b.iter_with_setup(
                || create_graph(size),
                |graph| {
                    graph
                        .create_index(
                            IndexBuilder::vertex()
                                .label("person")
                                .property("email")
                                .unique()
                                .build()
                                .unwrap(),
                        )
                        .unwrap();
                    black_box(graph)
                },
            )
        });
    }

    group.finish();
}

// =============================================================================
// Benchmark: Index Maintenance (Insert)
// =============================================================================

/// Benchmark insert with index maintenance.
fn bench_insert_with_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_with_index");

    // Pre-create graph with index, then measure insert cost
    let base_size = 10_000;

    group.bench_function("without_index", |b| {
        b.iter_with_setup(
            || create_graph(base_size),
            |graph| {
                for i in 0..100 {
                    graph.add_vertex(
                        "person",
                        HashMap::from([
                            (
                                "name".to_string(),
                                Value::String(format!("new_person_{}", i)),
                            ),
                            ("age".to_string(), Value::Int(25)),
                            (
                                "email".to_string(),
                                Value::String(format!("new{}@example.com", i)),
                            ),
                        ]),
                    );
                }
                black_box(graph)
            },
        )
    });

    group.bench_function("with_btree_index", |b| {
        b.iter_with_setup(
            || create_graph_with_age_index(base_size),
            |graph| {
                for i in 0..100 {
                    graph.add_vertex(
                        "person",
                        HashMap::from([
                            (
                                "name".to_string(),
                                Value::String(format!("new_person_{}", i)),
                            ),
                            ("age".to_string(), Value::Int(25)),
                            (
                                "email".to_string(),
                                Value::String(format!("new{}@example.com", i)),
                            ),
                        ]),
                    );
                }
                black_box(graph)
            },
        )
    });

    group.bench_function("with_unique_index", |b| {
        b.iter_with_setup(
            || create_graph_with_email_index(base_size),
            |graph| {
                for i in 0..100 {
                    graph.add_vertex(
                        "person",
                        HashMap::from([
                            (
                                "name".to_string(),
                                Value::String(format!("new_person_{}", i)),
                            ),
                            ("age".to_string(), Value::Int(25)),
                            (
                                "email".to_string(),
                                Value::String(format!("extra_new{}@example.com", i)),
                            ),
                        ]),
                    );
                }
                black_box(graph)
            },
        )
    });

    group.finish();
}

// =============================================================================
// Criterion Groups
// =============================================================================

criterion_group!(
    benches,
    bench_exact_match_no_index,
    bench_exact_match_with_btree_index,
    bench_unique_index_lookup,
    bench_email_scan,
    bench_range_query_no_index,
    bench_range_query_with_index,
    bench_index_creation,
    bench_insert_with_index,
);

criterion_main!(benches);
