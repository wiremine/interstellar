//! Memory usage comparison: Streaming vs Eager execution
//!
//! This test demonstrates that streaming execution uses O(1) memory
//! per traverser, while eager execution uses O(N) memory.
//!
//! Run with: cargo test --test memory_streaming -- --nocapture

use interstellar::prelude::*;
use interstellar::storage::Graph;
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};

/// Tracking allocator that counts allocations and total bytes.
/// Uses signed arithmetic to handle reset() safely (deallocations from
/// before reset will temporarily go negative, but that's fine for our purposes).
struct TrackingAllocator {
    inner: System,
    /// Current allocated bytes (can be negative after reset due to pending deallocs)
    allocated: AtomicIsize,
    /// Peak memory since last reset
    peak: AtomicUsize,
    /// Number of allocations since last reset
    allocation_count: AtomicUsize,
}

impl TrackingAllocator {
    const fn new() -> Self {
        Self {
            inner: System,
            allocated: AtomicIsize::new(0),
            peak: AtomicUsize::new(0),
            allocation_count: AtomicUsize::new(0),
        }
    }

    fn reset(&self) {
        self.allocated.store(0, Ordering::SeqCst);
        self.peak.store(0, Ordering::SeqCst);
        self.allocation_count.store(0, Ordering::SeqCst);
    }

    fn allocated(&self) -> usize {
        self.allocated.load(Ordering::SeqCst).max(0) as usize
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    fn allocation_count(&self) -> usize {
        self.allocation_count.load(Ordering::SeqCst)
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = self.inner.alloc(layout);
        if !ptr.is_null() {
            let size = layout.size() as isize;
            let new_val = self.allocated.fetch_add(size, Ordering::SeqCst) + size;
            // Only update peak if positive (ignore negative values from pre-reset deallocs)
            if new_val > 0 {
                let new_usize = new_val as usize;
                let mut peak = self.peak.load(Ordering::SeqCst);
                while new_usize > peak {
                    match self.peak.compare_exchange_weak(
                        peak,
                        new_usize,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => break,
                        Err(p) => peak = p,
                    }
                }
            }
            self.allocation_count.fetch_add(1, Ordering::SeqCst);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.allocated
            .fetch_sub(layout.size() as isize, Ordering::SeqCst);
        self.inner.dealloc(ptr, layout);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = self.inner.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            let old_size = layout.size() as isize;
            let new_size_signed = new_size as isize;
            let diff = new_size_signed - old_size;
            let new_val = self.allocated.fetch_add(diff, Ordering::SeqCst) + diff;
            if diff > 0 && new_val > 0 {
                let new_usize = new_val as usize;
                let mut peak = self.peak.load(Ordering::SeqCst);
                while new_usize > peak {
                    match self.peak.compare_exchange_weak(
                        peak,
                        new_usize,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => break,
                        Err(p) => peak = p,
                    }
                }
            }
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator::new();

fn create_test_graph(num_vertices: usize) -> Graph {
    let graph = Graph::new();
    for i in 0..num_vertices {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String(format!("person_{}", i)));
        props.insert("age".to_string(), Value::Int((i % 100) as i64));
        props.insert(
            "bio".to_string(),
            Value::String(format!(
                "This is a longer biography for person {} to increase memory usage per vertex.",
                i
            )),
        );
        graph.add_vertex("person", props);
    }
    graph
}

#[test]
fn compare_memory_streaming_vs_eager() {
    // Create graph outside of measurement
    let graph = create_test_graph(10_000);
    let snapshot = graph.snapshot();

    println!("\n{}", "=".repeat(60));
    println!("Memory Usage Comparison: Streaming vs Eager");
    println!("Graph: 10,000 vertices with properties");
    println!("{}\n", "=".repeat(60));

    // Test 1: Eager execution - collect all then take 10
    let eager_net;
    {
        ALLOCATOR.reset();
        let g = snapshot.gremlin();

        // Force a GC-like reset by dropping any cached data
        std::hint::black_box(());

        let start_alloc = ALLOCATOR.allocated();
        let start_count = ALLOCATOR.allocation_count();

        // Eager: to_list() collects ALL 10,000 values, then we take 10
        let result: Vec<_> = g
            .v()
            .values("name")
            .to_list()
            .into_iter()
            .take(10)
            .collect();

        let peak = ALLOCATOR.peak();
        let end_alloc = ALLOCATOR.allocated();
        let end_count = ALLOCATOR.allocation_count();
        eager_net = end_alloc.saturating_sub(start_alloc);

        println!("EAGER (to_list + take 10):");
        println!("  Result count: {}", result.len());
        println!("  Allocations: {}", end_count - start_count);
        println!(
            "  Peak memory: {} bytes ({:.2} KB)",
            peak,
            peak as f64 / 1024.0
        );
        println!("  Net allocated: {} bytes", eager_net);
        println!();
    }

    // Test 2: Streaming execution - stop after 10
    let streaming_net;
    {
        ALLOCATOR.reset();
        let g = snapshot.gremlin();

        std::hint::black_box(());

        let start_alloc = ALLOCATOR.allocated();
        let start_count = ALLOCATOR.allocation_count();

        // Streaming: iter() processes one at a time, take(10) stops early
        let result: Vec<_> = g.v().values("name").iter().take(10).collect();

        let peak = ALLOCATOR.peak();
        let end_alloc = ALLOCATOR.allocated();
        let end_count = ALLOCATOR.allocation_count();
        streaming_net = end_alloc.saturating_sub(start_alloc);

        println!("STREAMING (iter + take 10):");
        println!("  Result count: {}", result.len());
        println!("  Allocations: {}", end_count - start_count);
        println!(
            "  Peak memory: {} bytes ({:.2} KB)",
            peak,
            peak as f64 / 1024.0
        );
        println!("  Net allocated: {} bytes", streaming_net);
        println!();
    }

    // Summary
    if eager_net > 0 && streaming_net > 0 {
        let ratio = eager_net as f64 / streaming_net as f64;
        println!(
            "RESULT: Streaming uses {:.0}x less memory for early termination!\n",
            ratio
        );
    }

    // Test 3: Full collection comparison
    println!("--- Full Collection (all 10,000 values) ---\n");

    {
        ALLOCATOR.reset();
        let g = snapshot.gremlin();
        std::hint::black_box(());

        let start_count = ALLOCATOR.allocation_count();
        let result = g.v().values("name").to_list();
        let peak_eager = ALLOCATOR.peak();
        let count_eager = ALLOCATOR.allocation_count() - start_count;

        println!("EAGER (to_list all):");
        println!("  Result count: {}", result.len());
        println!("  Allocations: {}", count_eager);
        println!(
            "  Peak memory: {} bytes ({:.2} KB)",
            peak_eager,
            peak_eager as f64 / 1024.0
        );

        drop(result);
    }

    {
        ALLOCATOR.reset();
        let g = snapshot.gremlin();
        std::hint::black_box(());

        let start_count = ALLOCATOR.allocation_count();
        let result: Vec<_> = g.v().values("name").iter().collect();
        let peak_streaming = ALLOCATOR.peak();
        let count_streaming = ALLOCATOR.allocation_count() - start_count;

        println!("STREAMING (iter collect all):");
        println!("  Result count: {}", result.len());
        println!("  Allocations: {}", count_streaming);
        println!(
            "  Peak memory: {} bytes ({:.2} KB)",
            peak_streaming,
            peak_streaming as f64 / 1024.0
        );

        drop(result);
    }

    println!("\n{}", "=".repeat(60));
}

#[test]
fn streaming_constant_memory_per_element() {
    // This test verifies that streaming uses roughly constant memory
    // regardless of how many elements we process (when not collecting)

    let graph = create_test_graph(10_000);
    let snapshot = graph.snapshot();

    println!("\n{}", "=".repeat(60));
    println!("Streaming Memory Per Element (not collecting)");
    println!("{}\n", "=".repeat(60));

    // Process different numbers of elements, measure peak memory
    for count in [10, 100, 1000, 5000] {
        ALLOCATOR.reset();
        let g = snapshot.gremlin();

        let start_peak = ALLOCATOR.peak();

        // Process elements without collecting - just count them
        let mut processed = 0;
        for value in g.v().values("name").iter().take(count) {
            std::hint::black_box(&value);
            processed += 1;
        }

        let peak = ALLOCATOR.peak();
        let mem_per_element = if processed > 0 {
            (peak - start_peak) as f64 / processed as f64
        } else {
            0.0
        };

        println!(
            "Processed {} elements: peak {} bytes ({:.1} bytes/element)",
            processed, peak, mem_per_element
        );
    }

    println!("\nNote: Memory per element should be roughly constant (O(1))");
    println!("{}\n", "=".repeat(60));
}
