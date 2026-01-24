//! Benchmarks for Rhai script execution performance.
//!
//! Run with: `cargo bench --features rhai --bench rhai`
//!
//! These benchmarks measure the performance of executing Gremlin-style
//! traversals through the Rhai scripting engine.

#![cfg(feature = "rhai")]

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use interstellar::prelude::*;
use interstellar::rhai::RhaiEngine;
use interstellar::storage::Graph;
use std::collections::HashMap;
use std::sync::Arc;

/// Create a benchmark graph with specified number of vertices and edges.
fn create_benchmark_graph(num_vertices: usize, num_edges: usize) -> Arc<Graph> {
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

    Arc::new(graph)
}

// =============================================================================
// Engine Creation Benchmarks
// =============================================================================

/// Benchmark: Engine creation overhead
fn bench_engine_creation(c: &mut Criterion) {
    c.bench_function("rhai: engine_creation", |b| {
        b.iter(|| black_box(RhaiEngine::new()))
    });
}

// =============================================================================
// Script Compilation Benchmarks
// =============================================================================

/// Benchmark: Script compilation (simple)
fn bench_compile_simple(c: &mut Criterion) {
    let engine = RhaiEngine::new();

    c.bench_function("rhai: compile_simple", |b| {
        b.iter(|| {
            black_box(
                engine
                    .compile(
                        r#"
                let g = graph.gremlin();
                g.v().count()
            "#,
                    )
                    .unwrap(),
            )
        })
    });
}

/// Benchmark: Script compilation (complex)
fn bench_compile_complex(c: &mut Criterion) {
    let engine = RhaiEngine::new();

    c.bench_function("rhai: compile_complex", |b| {
        b.iter(|| {
            black_box(
                engine
                    .compile(
                        r#"
                let g = graph.gremlin();
                let persons = g.v().has_label("person").to_list();
                let count = g.v().has_label("software").count();
                let names = g.v().has_label("person").values("name").to_list();
                [persons.len(), count, names.len()]
            "#,
                    )
                    .unwrap(),
            )
        })
    });
}

// =============================================================================
// Script Execution Benchmarks - Simple Queries
// =============================================================================

/// Benchmark: v().count() via Rhai
fn bench_eval_v_count(c: &mut Criterion) {
    let engine = RhaiEngine::new();
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function("rhai: eval v().count()", |b| {
        b.iter(|| {
            let result: i64 = engine
                .eval_with_graph(
                    Arc::clone(&graph),
                    r#"
                let g = graph.gremlin();
                g.v().count()
            "#,
                )
                .unwrap();
            black_box(result)
        })
    });
}

/// Benchmark: v().has_label("person").count() via Rhai
fn bench_eval_v_has_label_count(c: &mut Criterion) {
    let engine = RhaiEngine::new();
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function("rhai: eval v().has_label(\"person\").count()", |b| {
        b.iter(|| {
            let result: i64 = engine
                .eval_with_graph(
                    Arc::clone(&graph),
                    r#"
                let g = graph.gremlin();
                g.v().has_label("person").count()
            "#,
                )
                .unwrap();
            black_box(result)
        })
    });
}

/// Benchmark: v().out().limit(100).count() via Rhai
fn bench_eval_v_out_limit_count(c: &mut Criterion) {
    let engine = RhaiEngine::new();
    let graph = create_benchmark_graph(10_000, 100_000);

    c.bench_function("rhai: eval v().out().limit(100).count()", |b| {
        b.iter(|| {
            let result: i64 = engine
                .eval_with_graph(
                    Arc::clone(&graph),
                    r#"
                let g = graph.gremlin();
                g.v().out().limit(100).count()
            "#,
                )
                .unwrap();
            black_box(result)
        })
    });
}

// =============================================================================
// Pre-compiled AST Execution Benchmarks
// =============================================================================

/// Benchmark: Pre-compiled v().count() execution
fn bench_eval_ast_v_count(c: &mut Criterion) {
    let engine = RhaiEngine::new();
    let graph = create_benchmark_graph(10_000, 100_000);

    let ast = engine
        .compile(
            r#"
        let g = graph.gremlin();
        g.v().count()
    "#,
        )
        .unwrap();

    c.bench_function("rhai: eval_ast v().count()", |b| {
        b.iter(|| {
            let result: i64 = engine
                .eval_ast_with_graph(Arc::clone(&graph), &ast)
                .unwrap();
            black_box(result)
        })
    });
}

/// Benchmark: Pre-compiled v().has_label().count() execution
fn bench_eval_ast_v_has_label_count(c: &mut Criterion) {
    let engine = RhaiEngine::new();
    let graph = create_benchmark_graph(10_000, 100_000);

    let ast = engine
        .compile(
            r#"
        let g = graph.gremlin();
        g.v().has_label("person").count()
    "#,
        )
        .unwrap();

    c.bench_function("rhai: eval_ast v().has_label(\"person\").count()", |b| {
        b.iter(|| {
            let result: i64 = engine
                .eval_ast_with_graph(Arc::clone(&graph), &ast)
                .unwrap();
            black_box(result)
        })
    });
}

/// Benchmark: Pre-compiled complex traversal execution
fn bench_eval_ast_complex_traversal(c: &mut Criterion) {
    let engine = RhaiEngine::new();
    let graph = create_benchmark_graph(10_000, 100_000);

    let ast = engine
        .compile(
            r#"
        let g = graph.gremlin();
        g.v().out().out().dedup().count()
    "#,
        )
        .unwrap();

    c.bench_function("rhai: eval_ast v().out().out().dedup().count()", |b| {
        b.iter(|| {
            let result: i64 = engine
                .eval_ast_with_graph(Arc::clone(&graph), &ast)
                .unwrap();
            black_box(result)
        })
    });
}

// =============================================================================
// Compilation vs Interpretation Comparison
// =============================================================================

/// Compare interpreted vs pre-compiled execution
fn bench_compiled_vs_interpreted(c: &mut Criterion) {
    let engine = RhaiEngine::new();
    let graph = create_benchmark_graph(1_000, 5_000);

    let script = r#"
        let g = graph.gremlin();
        g.v().has_label("person").count()
    "#;

    let ast = engine.compile(script).unwrap();

    let mut group = c.benchmark_group("rhai: compiled_vs_interpreted");

    group.bench_function("interpreted", |b| {
        b.iter(|| {
            let result: i64 = engine.eval_with_graph(Arc::clone(&graph), script).unwrap();
            black_box(result)
        })
    });

    group.bench_function("pre_compiled", |b| {
        b.iter(|| {
            let result: i64 = engine
                .eval_ast_with_graph(Arc::clone(&graph), &ast)
                .unwrap();
            black_box(result)
        })
    });

    group.finish();
}

// =============================================================================
// Rhai vs Native Comparison
// =============================================================================

/// Compare Rhai execution to native Rust execution
fn bench_rhai_vs_native(c: &mut Criterion) {
    let engine = RhaiEngine::new();
    let graph = create_benchmark_graph(10_000, 100_000);

    let ast = engine
        .compile(
            r#"
        let g = graph.gremlin();
        g.v().count()
    "#,
        )
        .unwrap();

    let mut group = c.benchmark_group("rhai: rhai_vs_native");

    group.bench_function("rhai_precompiled", |b| {
        b.iter(|| {
            let result: i64 = engine
                .eval_ast_with_graph(Arc::clone(&graph), &ast)
                .unwrap();
            black_box(result)
        })
    });

    group.bench_function("native_rust", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v().count())
        })
    });

    group.finish();
}

// =============================================================================
// Throughput Benchmarks
// =============================================================================

/// Benchmark script execution throughput at different graph sizes
fn bench_script_throughput(c: &mut Criterion) {
    let engine = RhaiEngine::new();
    let mut group = c.benchmark_group("rhai: script_throughput");

    for size in [1_000, 10_000] {
        let graph = create_benchmark_graph(size, size * 5);

        // Compile once
        let ast = engine
            .compile(
                r#"
            let g = graph.gremlin();
            g.v().to_list()
        "#,
            )
            .unwrap();

        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let result: rhai::Dynamic = engine
                    .eval_ast_with_graph_dynamic(Arc::clone(&graph), &ast)
                    .unwrap();
                black_box(result)
            })
        });
    }

    group.finish();
}

// =============================================================================
// Anonymous Traversal Benchmarks
// =============================================================================

/// Benchmark anonymous traversals in Rhai scripts
fn bench_anonymous_traversal(c: &mut Criterion) {
    let engine = RhaiEngine::new();
    let graph = create_benchmark_graph(1_000, 5_000);

    // Pre-compile the script with anonymous traversal
    let ast = engine
        .compile(
            r#"
        let g = graph.gremlin();
        g.v().has_label("person").where_trav(A.out().has_label("software")).count()
    "#,
        )
        .unwrap();

    c.bench_function("rhai: anonymous_traversal where(__.out())", |b| {
        b.iter(|| {
            let result: i64 = engine
                .eval_ast_with_graph(Arc::clone(&graph), &ast)
                .unwrap();
            black_box(result)
        })
    });
}

criterion_group!(
    engine_benches,
    bench_engine_creation,
    bench_compile_simple,
    bench_compile_complex,
);

criterion_group!(
    eval_benches,
    bench_eval_v_count,
    bench_eval_v_has_label_count,
    bench_eval_v_out_limit_count,
);

criterion_group!(
    ast_benches,
    bench_eval_ast_v_count,
    bench_eval_ast_v_has_label_count,
    bench_eval_ast_complex_traversal,
);

criterion_group!(
    comparison_benches,
    bench_compiled_vs_interpreted,
    bench_rhai_vs_native,
);

criterion_group!(
    advanced_benches,
    bench_script_throughput,
    bench_anonymous_traversal,
);

criterion_main!(
    engine_benches,
    eval_benches,
    ast_benches,
    comparison_benches,
    advanced_benches
);
