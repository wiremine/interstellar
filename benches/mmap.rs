//! Benchmarks for memory-mapped storage read operations.
//!
//! Run with: `cargo bench --features mmap --bench mmap`
//!
//! These benchmarks measure the read performance of the MmapGraph backend
//! for vertices and edges from disk-backed storage.

#![cfg(feature = "mmap")]

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use interstellar::storage::{GraphStorage, MmapGraph};
use interstellar::value::{EdgeId, VertexId};
use std::collections::HashMap;
use tempfile::TempDir;

/// Create a benchmark database with specified number of vertices and edges.
///
/// Creates:
/// - `num_vertices` vertices with alternating "person" and "software" labels
/// - `num_edges` edges connecting vertices with "knows" or "created" labels
///
/// Uses batch mode for fast setup.
fn create_benchmark_db(num_vertices: usize, num_edges: usize) -> (TempDir, MmapGraph) {
    let dir = TempDir::new().expect("create temp dir");
    let db_path = dir.path().join("bench.db");

    let graph = MmapGraph::open(&db_path).expect("open graph");

    // Use batch mode for fast loading
    graph.begin_batch().expect("begin batch");

    // Create vertices
    let mut vertex_ids = Vec::with_capacity(num_vertices);
    for i in 0..num_vertices {
        let (label, props) = if i % 2 == 0 {
            let mut props = HashMap::new();
            props.insert("name".to_string(), format!("person_{}", i).into());
            props.insert("age".to_string(), ((i % 100) as i64).into());
            ("person", props)
        } else {
            let mut props = HashMap::new();
            props.insert("name".to_string(), format!("software_{}", i).into());
            props.insert("version".to_string(), format!("1.{}", i % 10).into());
            ("software", props)
        };
        let id = graph.add_vertex(label, props).expect("add vertex");
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

        let label = if i % 3 == 0 { "knows" } else { "created" };
        let mut props = HashMap::new();
        props.insert("weight".to_string(), ((i % 100) as f64 / 10.0).into());

        // Ignore errors (duplicate edges are fine to skip)
        let _ = graph.add_edge(vertex_ids[src_idx], vertex_ids[dst_idx], label, props);
    }

    graph.commit_batch().expect("commit batch");
    graph.checkpoint().expect("checkpoint");

    (dir, graph)
}

// =============================================================================
// Vertex Read Benchmarks
// =============================================================================

/// Benchmark: Single vertex lookup by ID (hot cache)
fn bench_get_vertex_single(c: &mut Criterion) {
    let (_dir, graph) = create_benchmark_db(10_000, 50_000);

    c.bench_function("mmap: get_vertex(single)", |b| {
        b.iter(|| {
            // Access vertex in the middle of the range
            black_box(graph.get_vertex(VertexId(5000)))
        })
    });
}

/// Benchmark: Random vertex lookups (simulates cache misses)
fn bench_get_vertex_random(c: &mut Criterion) {
    let (_dir, graph) = create_benchmark_db(10_000, 50_000);

    // Pre-generate "random" IDs for consistent benchmarking
    let ids: Vec<VertexId> = (0..1000)
        .map(|i| VertexId(((i * 7919) % 10_000) as u64))
        .collect();

    c.bench_function("mmap: get_vertex(1000 random)", |b| {
        b.iter(|| {
            for &id in &ids {
                black_box(graph.get_vertex(id));
            }
        })
    });
}

/// Benchmark: Sequential vertex scan
fn bench_get_vertex_sequential(c: &mut Criterion) {
    let (_dir, graph) = create_benchmark_db(10_000, 50_000);

    c.bench_function("mmap: get_vertex(1000 sequential)", |b| {
        b.iter(|| {
            for i in 0..1000u64 {
                black_box(graph.get_vertex(VertexId(i)));
            }
        })
    });
}

/// Benchmark: all_vertices() iteration
fn bench_all_vertices(c: &mut Criterion) {
    let (_dir, graph) = create_benchmark_db(10_000, 50_000);

    c.bench_function("mmap: all_vertices().count()", |b| {
        b.iter(|| black_box(graph.all_vertices().count()))
    });
}

/// Benchmark: vertices_with_label() using bitmap index
fn bench_vertices_with_label(c: &mut Criterion) {
    let (_dir, graph) = create_benchmark_db(10_000, 50_000);

    c.bench_function("mmap: vertices_with_label(\"person\").count()", |b| {
        b.iter(|| black_box(graph.vertices_with_label("person").count()))
    });
}

// =============================================================================
// Edge Read Benchmarks
// =============================================================================

/// Benchmark: Single edge lookup by ID
fn bench_get_edge_single(c: &mut Criterion) {
    let (_dir, graph) = create_benchmark_db(10_000, 50_000);

    c.bench_function("mmap: get_edge(single)", |b| {
        b.iter(|| {
            // Access edge in the middle of the range
            black_box(graph.get_edge(EdgeId(25000)))
        })
    });
}

/// Benchmark: Random edge lookups
fn bench_get_edge_random(c: &mut Criterion) {
    let (_dir, graph) = create_benchmark_db(10_000, 50_000);

    // Pre-generate "random" IDs for consistent benchmarking
    let ids: Vec<EdgeId> = (0..1000)
        .map(|i| EdgeId(((i * 7919) % 50_000) as u64))
        .collect();

    c.bench_function("mmap: get_edge(1000 random)", |b| {
        b.iter(|| {
            for &id in &ids {
                black_box(graph.get_edge(id));
            }
        })
    });
}

/// Benchmark: Sequential edge scan
fn bench_get_edge_sequential(c: &mut Criterion) {
    let (_dir, graph) = create_benchmark_db(10_000, 50_000);

    c.bench_function("mmap: get_edge(1000 sequential)", |b| {
        b.iter(|| {
            for i in 0..1000u64 {
                black_box(graph.get_edge(EdgeId(i)));
            }
        })
    });
}

/// Benchmark: all_edges() iteration
fn bench_all_edges(c: &mut Criterion) {
    let (_dir, graph) = create_benchmark_db(10_000, 50_000);

    c.bench_function("mmap: all_edges().count()", |b| {
        b.iter(|| black_box(graph.all_edges().count()))
    });
}

/// Benchmark: edges_with_label() using bitmap index
fn bench_edges_with_label(c: &mut Criterion) {
    let (_dir, graph) = create_benchmark_db(10_000, 50_000);

    c.bench_function("mmap: edges_with_label(\"knows\").count()", |b| {
        b.iter(|| black_box(graph.edges_with_label("knows").count()))
    });
}

// =============================================================================
// Adjacency Traversal Benchmarks
// =============================================================================

/// Benchmark: out_edges() traversal
fn bench_out_edges(c: &mut Criterion) {
    let (_dir, graph) = create_benchmark_db(10_000, 50_000);

    // Find a vertex with outgoing edges
    let vertex_id = VertexId(0);

    c.bench_function("mmap: out_edges(vertex).count()", |b| {
        b.iter(|| black_box(graph.out_edges(vertex_id).count()))
    });
}

/// Benchmark: in_edges() traversal
fn bench_in_edges(c: &mut Criterion) {
    let (_dir, graph) = create_benchmark_db(10_000, 50_000);

    // Find a vertex with incoming edges (vertex 13 is a common target due to our edge pattern)
    let vertex_id = VertexId(13);

    c.bench_function("mmap: in_edges(vertex).count()", |b| {
        b.iter(|| black_box(graph.in_edges(vertex_id).count()))
    });
}

/// Benchmark: Multi-hop traversal (out -> out) - limited scope
fn bench_two_hop_traversal(c: &mut Criterion) {
    let (_dir, graph) = create_benchmark_db(1_000, 5_000);

    c.bench_function("mmap: 2-hop out traversal (10 vertices)", |b| {
        b.iter(|| {
            let mut count = 0;
            for vertex in graph.all_vertices().take(10) {
                for edge in graph.out_edges(vertex.id) {
                    for _inner_edge in graph.out_edges(edge.dst) {
                        count += 1;
                    }
                }
            }
            black_box(count)
        })
    });
}

// =============================================================================
// Property Loading Benchmarks
// =============================================================================

/// Benchmark: Vertex read with property loading
fn bench_get_vertex_with_properties(c: &mut Criterion) {
    let (_dir, graph) = create_benchmark_db(10_000, 50_000);

    c.bench_function("mmap: get_vertex with props (1000)", |b| {
        b.iter(|| {
            for i in 0..1000u64 {
                let v = graph.get_vertex(VertexId(i));
                if let Some(vertex) = v {
                    black_box(&vertex.properties);
                }
            }
        })
    });
}

/// Benchmark: Edge read with property loading
fn bench_get_edge_with_properties(c: &mut Criterion) {
    let (_dir, graph) = create_benchmark_db(10_000, 50_000);

    c.bench_function("mmap: get_edge with props (1000)", |b| {
        b.iter(|| {
            for i in 0..1000u64 {
                let e = graph.get_edge(EdgeId(i));
                if let Some(edge) = e {
                    black_box(&edge.properties);
                }
            }
        })
    });
}

// =============================================================================
// Cold Cache Simulation
// =============================================================================

/// Benchmark: Vertex lookup after database reopen (cold start)
///
/// This simulates a cold cache scenario by reopening the database before each iteration.
/// Note: This is slower due to the overhead of reopening, but gives more realistic
/// "first access" performance numbers.
fn bench_get_vertex_cold_start(c: &mut Criterion) {
    let dir = TempDir::new().expect("create temp dir");
    let db_path = dir.path().join("bench.db");

    // Create the database
    {
        let graph = MmapGraph::open(&db_path).expect("open graph");
        graph.begin_batch().expect("begin batch");
        for i in 0..1000 {
            let props = HashMap::from([("i".to_string(), (i as i64).into())]);
            graph.add_vertex("node", props).expect("add vertex");
        }
        graph.commit_batch().expect("commit batch");
        graph.checkpoint().expect("checkpoint");
    }

    c.bench_function("mmap: get_vertex cold start (reopen + read)", |b| {
        b.iter(|| {
            // Reopen database (simulates cold start)
            let graph = MmapGraph::open(&db_path).expect("reopen");
            // Read a vertex
            black_box(graph.get_vertex(VertexId(500)))
        })
    });
}

criterion_group!(
    vertex_benches,
    bench_get_vertex_single,
    bench_get_vertex_random,
    bench_get_vertex_sequential,
    bench_all_vertices,
    bench_vertices_with_label,
    bench_get_vertex_with_properties,
);

criterion_group!(
    edge_benches,
    bench_get_edge_single,
    bench_get_edge_random,
    bench_get_edge_sequential,
    bench_all_edges,
    bench_edges_with_label,
    bench_get_edge_with_properties,
);

criterion_group!(
    traversal_benches,
    bench_out_edges,
    bench_in_edges,
    bench_two_hop_traversal,
);

criterion_group!(cold_benches, bench_get_vertex_cold_start,);

// =============================================================================
// Multi-Page Access Pattern Benchmark
// =============================================================================

/// Benchmark: Multi-page random access patterns.
///
/// This benchmark tests access patterns that span multiple OS pages to measure
/// more realistic cache behavior. With NodeRecord at 48 bytes and 4KB pages,
/// ~85 vertices fit per page. By striding by 100, we guarantee each access
/// hits a different page.
///
/// Compares:
/// - Same-page access (stride=1): all accesses within ~12 pages
/// - Cross-page access (stride=100): each access hits a different page
fn bench_multi_page_access(c: &mut Criterion) {
    // 50K vertices = ~588 pages of vertex data
    // This should exceed L1/L2 cache but fit in RAM
    let (_dir, graph) = create_benchmark_db(50_000, 0);

    let mut group = c.benchmark_group("mmap: page_access_patterns");

    // Same-page: 100 sequential reads (all within ~2 pages)
    group.bench_function("same_page_100_reads", |b| {
        b.iter(|| {
            for i in 0..100u64 {
                black_box(graph.get_vertex(VertexId(i)));
            }
        })
    });

    // Cross-page: 100 reads striding by 100 (each hits different page)
    // Accesses vertices 0, 100, 200, ... 9900 (100 different pages)
    group.bench_function("cross_page_100_reads", |b| {
        b.iter(|| {
            for i in 0..100u64 {
                black_box(graph.get_vertex(VertexId(i * 100)));
            }
        })
    });

    // Worst-case: 100 reads with large prime stride to defeat prefetching
    // Uses stride of 997 (prime) to create unpredictable access pattern
    group.bench_function("random_page_100_reads", |b| {
        // Pre-compute indices to avoid math in hot loop
        let indices: Vec<VertexId> = (0..100u64).map(|i| VertexId((i * 997) % 50_000)).collect();
        b.iter(|| {
            for &id in &indices {
                black_box(graph.get_vertex(id));
            }
        })
    });

    group.finish();
}

criterion_group!(page_benches, bench_multi_page_access,);

criterion_main!(
    vertex_benches,
    edge_benches,
    traversal_benches,
    cold_benches,
    page_benches
);
