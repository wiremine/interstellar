use intersteller::storage::mmap::MmapGraph;
use intersteller::storage::GraphStorage;
use std::collections::HashMap;
use std::time::Instant;
use tempfile::TempDir;

fn main() {
    println!("=== MmapGraph Write Performance Benchmark ===\n");

    // Test with smaller numbers first due to fsync overhead
    bench_durable_writes();

    println!("\n--- Batch Mode (single fsync for all operations) ---\n");
    bench_batch_writes();

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
    println!("  use batch mode (see below) or InMemoryGraph.");
}

fn bench_batch_writes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bench_batch.db");
    let graph = MmapGraph::open(&path).unwrap();

    // Much larger test since batch mode avoids per-operation fsync
    let num_vertices = 10_000;
    let num_edges = 50_000;

    // Start batch mode
    graph.begin_batch().expect("begin batch");

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

    println!("MmapGraph - Batch Mode (deferred fsync):");
    println!("  {} vertices in {:?}", num_vertices, vertex_duration);
    println!("  {:.0} vertices/sec", vertex_per_sec);
    println!("  {:.1} µs per vertex", vertex_us);

    // Benchmark edge writes (still in batch mode)
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

    // Commit the batch (single fsync for all operations)
    let start = Instant::now();
    graph.commit_batch().expect("commit batch");
    let commit_duration = start.elapsed();
    println!("\n  Batch commit (single fsync): {:?}", commit_duration);

    // Checkpoint and show file size
    graph.checkpoint().unwrap();
    let file_size = std::fs::metadata(&path).unwrap().len();
    println!("  File size: {:.2} MB", file_size as f64 / 1_000_000.0);

    // Summary
    let total_ops = num_vertices + num_edges;
    let total_duration = vertex_duration + edge_duration + commit_duration;
    let total_per_sec = total_ops as f64 / total_duration.as_secs_f64();
    println!(
        "\n  Total: {} operations in {:?} ({:.0} ops/sec)",
        total_ops, total_duration, total_per_sec
    );
    println!("\n  Note: Batch mode groups all writes into a single");
    println!("  atomic transaction with one fsync at commit time.");
    println!("  Data is still readable during the batch.");
}

fn bench_inmemory_writes() {
    use intersteller::storage::InMemoryGraph;

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
        let _ = graph.add_edge(src, dst, "knows", props);
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
