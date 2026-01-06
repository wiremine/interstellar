use rustgremlin::storage::mmap::MmapGraph;
use rustgremlin::storage::GraphStorage;
use std::collections::HashMap;
use std::time::Instant;
use tempfile::TempDir;

fn main() {
    println!("=== MmapGraph Write Performance Benchmark ===\n");

    // Test with smaller numbers first due to fsync overhead
    bench_durable_writes();

    println!("\n--- Comparison with InMemoryGraph ---\n");
    bench_inmemory_writes();
}

fn bench_durable_writes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bench.db");
    let graph = MmapGraph::open(&path).unwrap();

    // Smaller test due to fsync overhead (~5ms per operation)
    let num_vertices = 500;
    let start = Instant::now();

    let mut vertex_ids = Vec::with_capacity(num_vertices);
    for i in 0..num_vertices {
        let props = HashMap::from([("i".to_string(), (i as i64).into())]);
        let id = graph.add_vertex("person", props).unwrap();
        vertex_ids.push(id);
    }

    let vertex_duration = start.elapsed();
    let vertex_per_sec = num_vertices as f64 / vertex_duration.as_secs_f64();
    let vertex_us = vertex_duration.as_micros() as f64 / num_vertices as f64;

    println!("MmapGraph - Durable Writes (with fsync per operation):");
    println!("  {} vertices in {:?}", num_vertices, vertex_duration);
    println!("  {:.0} vertices/sec", vertex_per_sec);
    println!("  {:.1} µs per vertex", vertex_us);

    // Benchmark edge writes
    let num_edges = 500;
    let start = Instant::now();

    for i in 0..num_edges {
        let src = vertex_ids[i % num_vertices];
        let dst = vertex_ids[(i + 1) % num_vertices];
        let props = HashMap::from([("weight".to_string(), (i as i64).into())]);
        graph.add_edge(src, dst, "knows", props).unwrap();
    }

    let edge_duration = start.elapsed();
    let edge_per_sec = num_edges as f64 / edge_duration.as_secs_f64();
    let edge_us = edge_duration.as_micros() as f64 / num_edges as f64;

    println!("\n  {} edges in {:?}", num_edges, edge_duration);
    println!("  {:.0} edges/sec", edge_per_sec);
    println!("  {:.1} µs per edge", edge_us);

    // Show file size
    graph.checkpoint().unwrap();
    let file_size = std::fs::metadata(&path).unwrap().len();
    println!("\n  File size: {:.2} MB", file_size as f64 / 1_000_000.0);

    println!("\n  Note: Each write does fsync() for ACID durability.");
    println!("  This is ~4-5ms on typical SSDs. For bulk loads,");
    println!("  consider batching or using InMemoryGraph first.");
}

fn bench_inmemory_writes() {
    use rustgremlin::storage::InMemoryGraph;

    let mut graph = InMemoryGraph::new();

    let num_vertices = 100_000;
    let start = Instant::now();

    let mut vertex_ids = Vec::with_capacity(num_vertices);
    for i in 0..num_vertices {
        let props = HashMap::from([("i".to_string(), (i as i64).into())]);
        let id = graph.add_vertex("person", props);
        vertex_ids.push(id);
    }

    let vertex_duration = start.elapsed();
    let vertex_per_sec = num_vertices as f64 / vertex_duration.as_secs_f64();
    let vertex_us = vertex_duration.as_micros() as f64 / num_vertices as f64;

    println!("InMemoryGraph - No persistence:");
    println!("  {} vertices in {:?}", num_vertices, vertex_duration);
    println!("  {:.0} vertices/sec", vertex_per_sec);
    println!("  {:.2} µs per vertex", vertex_us);

    // Benchmark edge writes
    let num_edges = 500_000;
    let start = Instant::now();

    for i in 0..num_edges {
        let src = vertex_ids[i % num_vertices];
        let dst = vertex_ids[(i + 1) % num_vertices];
        let props = HashMap::from([("weight".to_string(), (i as i64).into())]);
        graph.add_edge(src, dst, "knows", props);
    }

    let edge_duration = start.elapsed();
    let edge_per_sec = num_edges as f64 / edge_duration.as_secs_f64();
    let edge_us = edge_duration.as_micros() as f64 / num_edges as f64;

    println!("\n  {} edges in {:?}", num_edges, edge_duration);
    println!("  {:.0} edges/sec", edge_per_sec);
    println!("  {:.2} µs per edge", edge_us);

    // Read performance
    println!("\n--- Read Performance ---");

    let start = Instant::now();
    let mut count = 0u64;
    for id in &vertex_ids {
        if graph.get_vertex(*id).is_some() {
            count += 1;
        }
    }
    let read_duration = start.elapsed();
    println!("\n  {} vertex lookups in {:?}", count, read_duration);
    println!(
        "  {:.0} lookups/sec",
        count as f64 / read_duration.as_secs_f64()
    );
    println!(
        "  {:.2} µs per lookup",
        read_duration.as_micros() as f64 / count as f64
    );
}
