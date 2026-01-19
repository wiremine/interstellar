//! Copy-on-Write Snapshots Example
//!
//! This example demonstrates the Copy-on-Write (COW) snapshot implementation
//! for Interstellar, showcasing:
//!
//! **Part 1: Basic COW Operations**
//! - Creating a COW graph
//! - Adding vertices and edges
//! - Taking O(1) snapshots
//!
//! **Part 2: Snapshot Isolation**
//! - Snapshots see frozen state
//! - Mutations don't affect existing snapshots
//! - Multiple concurrent snapshots
//!
//! **Part 3: Thread Safety**
//! - Snapshots are Send + Sync
//! - Can be used across threads
//! - Lock-free reads
//!
//! **Part 4: Batch Operations**
//! - Atomic multi-operation batches
//! - Rollback on error
//!
//! **Part 5: GQL Mutations**
//! - Unified execute_mutation() API
//! - Statement-level atomicity
//!
//! **Part 6: Performance Characteristics**
//! - O(1) snapshot creation via structural sharing
//! - O(log₃₂ n) lookups (effectively constant for practical sizes)
//!
//! ## Key Concepts
//!
//! Copy-on-Write (COW) uses persistent data structures from the `im` crate.
//! When you mutate the graph, only the modified paths are copied - unchanged
//! portions are shared between the old and new versions. This enables:
//!
//! 1. **O(1) snapshots**: Just increment a reference count
//! 2. **Lock-free reads**: Snapshots don't hold any locks
//! 3. **Automatic cleanup**: Arc refcounting frees old versions
//!
//! Run: `cargo run --example cow_snapshots`

use interstellar::storage::cow::{BatchError, CowGraph};
use interstellar::storage::GraphStorage;
use interstellar::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

// =============================================================================
// Helper Functions
// =============================================================================

/// Helper to create property maps from slice of key-value pairs.
fn props(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

// =============================================================================
// Part 1: Basic COW Operations
// =============================================================================

/// Demonstrates basic CowGraph usage: creation, mutations, and snapshots.
///
/// Key points:
/// - CowGraph allows mutations via `&self` (not `&mut self`) because it uses
///   internal RwLock synchronization
/// - Each mutation increments the version counter
/// - Snapshots provide read-only access to a frozen state
fn demo_basic_operations() {
    println!("============================================================");
    println!("PART 1: BASIC COW OPERATIONS");
    println!("============================================================\n");

    // Create a new COW graph.
    // Unlike InMemoryGraph which requires &mut for mutations, CowGraph uses
    // interior mutability via RwLock, allowing mutations through &self.
    let graph = CowGraph::new();
    println!("Created new CowGraph");
    println!("  Initial vertex count: {}", graph.vertex_count());
    println!("  Initial edge count: {}", graph.edge_count());
    println!("  Initial version: {}", graph.version());
    println!();

    // Add vertices using &self (no &mut needed!)
    // Each add_vertex call:
    // 1. Acquires write lock
    // 2. Allocates new VertexId
    // 3. Updates the persistent im::HashMap (O(log n) with structural sharing)
    // 4. Increments version
    // 5. Releases lock
    let alice = graph.add_vertex(
        "Person",
        props(&[
            ("name", Value::String("Alice".to_string())),
            ("age", Value::Int(30)),
        ]),
    );
    println!("Added vertex Alice: {:?}", alice);

    let bob = graph.add_vertex(
        "Person",
        props(&[
            ("name", Value::String("Bob".to_string())),
            ("age", Value::Int(25)),
        ]),
    );
    println!("Added vertex Bob: {:?}", bob);

    let software = graph.add_vertex(
        "Software",
        props(&[("name", Value::String("Interstellar".to_string()))]),
    );
    println!("Added vertex Software: {:?}", software);

    // Add edges - these also update adjacency lists on both endpoints.
    // The persistent data structures ensure only modified paths are copied.
    let knows_edge = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    println!("Added edge Alice -[:knows]-> Bob: {:?}", knows_edge);

    let created_edge = graph
        .add_edge(alice, software, "created", HashMap::new())
        .unwrap();
    println!(
        "Added edge Alice -[:created]-> Software: {:?}",
        created_edge
    );

    println!();
    println!("Graph state after mutations:");
    println!("  Vertex count: {}", graph.vertex_count());
    println!("  Edge count: {}", graph.edge_count());
    // Version = 5 because: 3 add_vertex + 2 add_edge = 5 mutations
    println!("  Version: {}", graph.version());

    // Take a snapshot - this is O(1)!
    // Internally, it clones the im::HashMap (which just increments refcounts)
    // and wraps it in an Arc. The snapshot owns its state and doesn't hold locks.
    let snap = graph.snapshot();
    println!("\nSnapshot taken (version {})", snap.version());

    // Query the snapshot using GraphStorage trait methods.
    // The snapshot implements GraphStorage, so all read operations work.
    let alice_vertex = snap.get_vertex(alice).unwrap();
    println!(
        "  Alice from snapshot: label={}, name={:?}, age={:?}",
        alice_vertex.label,
        alice_vertex.properties.get("name"),
        alice_vertex.properties.get("age")
    );

    // Adjacency traversal works too
    let out_edges: Vec<_> = snap.out_edges(alice).collect();
    println!("  Alice's outgoing edges: {} edges", out_edges.len());
    for edge in &out_edges {
        println!("    -[:{}]-> {:?}", edge.label, edge.dst);
    }
    println!();
}

// =============================================================================
// Part 2: Snapshot Isolation
// =============================================================================

/// Demonstrates that snapshots provide isolation from subsequent mutations.
///
/// This is the core benefit of COW: readers and writers don't interfere.
/// - Snapshot 1 is taken at time T1
/// - Graph is mutated at time T2
/// - Snapshot 1 still sees state at T1
/// - Snapshot 2 (taken at T3) sees all changes
///
/// This is "snapshot isolation" - each snapshot sees a consistent view
/// of the graph as it existed when the snapshot was taken.
fn demo_snapshot_isolation() {
    println!("============================================================");
    println!("PART 2: SNAPSHOT ISOLATION");
    println!("============================================================\n");

    let graph = CowGraph::new();

    // Create initial state with one vertex
    let v1 = graph.add_vertex(
        "Person",
        props(&[("name", Value::String("Alice".to_string()))]),
    );
    println!("Created Alice (v1)");

    // SNAPSHOT 1: Capture current state (1 vertex, no 'status' property)
    let snap1 = graph.snapshot();
    println!(
        "Snapshot 1 taken: {} vertices, version {}",
        snap1.vertex_count(),
        snap1.version()
    );

    // MUTATION 1: Add a property to Alice.
    // This creates a new version of Alice's node data, but snap1 still
    // points to the old version via structural sharing.
    graph
        .set_vertex_property(v1, "status", Value::String("active".to_string()))
        .unwrap();
    println!("Modified Alice: added 'status' property");

    // MUTATION 2: Add another vertex.
    // snap1 won't see this vertex because it was created after the snapshot.
    let v2 = graph.add_vertex(
        "Person",
        props(&[("name", Value::String("Bob".to_string()))]),
    );
    println!("Created Bob (v2)");

    // SNAPSHOT 2: Capture new state (2 vertices, Alice has 'status')
    let snap2 = graph.snapshot();
    println!(
        "Snapshot 2 taken: {} vertices, version {}",
        snap2.vertex_count(),
        snap2.version()
    );

    // VERIFICATION: Prove that snapshots are isolated
    println!("\n--- Verifying Snapshot Isolation ---");

    let alice_in_snap1 = snap1.get_vertex(v1).unwrap();
    let alice_in_snap2 = snap2.get_vertex(v1).unwrap();

    // Snapshot 1 should NOT see the 'status' property (added after snap1)
    println!("Alice in Snapshot 1:");
    println!(
        "  Has 'status' property: {}",
        alice_in_snap1.properties.contains_key("status")
    );

    // Snapshot 2 SHOULD see the 'status' property
    println!("Alice in Snapshot 2:");
    println!(
        "  Has 'status' property: {}",
        alice_in_snap2.properties.contains_key("status")
    );
    println!("  status = {:?}", alice_in_snap2.properties.get("status"));

    // Snapshot 1 should NOT see Bob (created after snap1)
    println!("\nBob in Snapshot 1: {:?}", snap1.get_vertex(v2));
    // Snapshot 2 SHOULD see Bob
    println!(
        "Bob in Snapshot 2: {:?}",
        snap2.get_vertex(v2).map(|v| v.label)
    );

    // Assertions to prove correctness
    assert!(
        snap1.get_vertex(v2).is_none(),
        "Snapshot 1 should not see Bob"
    );
    assert!(
        !alice_in_snap1.properties.contains_key("status"),
        "Snapshot 1 should not see status property"
    );
    assert!(snap2.get_vertex(v2).is_some(), "Snapshot 2 should see Bob");
    assert!(
        alice_in_snap2.properties.contains_key("status"),
        "Snapshot 2 should see status property"
    );

    println!("\n[pass] Snapshot isolation verified!");
    println!();
}

// =============================================================================
// Part 3: Thread Safety
// =============================================================================

/// Demonstrates thread safety properties of CowGraph and CowSnapshot.
///
/// Key guarantees:
/// - CowGraph is Send + Sync (can be shared across threads via Arc)
/// - CowSnapshot is Send + Sync (can be moved to other threads)
/// - Snapshots don't hold locks, so readers never block writers
/// - Snapshots can outlive the source graph (they own their data via Arc)
///
/// This enables patterns like:
/// - Taking a snapshot in the main thread, sending to worker threads
/// - Multiple threads reading concurrently without contention
/// - Long-running traversals that don't block writes
fn demo_thread_safety() {
    println!("============================================================");
    println!("PART 3: THREAD SAFETY");
    println!("============================================================\n");

    // Wrap in Arc for sharing across threads.
    // CowGraph itself is Send + Sync, so Arc<CowGraph> works.
    let graph = Arc::new(CowGraph::new());

    // Populate with some data
    for i in 0..100 {
        graph.add_vertex("Node", props(&[("id", Value::Int(i))]));
    }
    println!("Created graph with 100 vertices");

    // Spawn multiple reader threads, each taking its own snapshot.
    // This demonstrates:
    // 1. Arc<CowGraph> can be cloned and sent to threads
    // 2. Each thread can independently take snapshots
    // 3. Snapshots work correctly across thread boundaries
    println!("\n--- Spawning 10 reader threads ---");
    let start = Instant::now();

    let handles: Vec<_> = (0..10)
        .map(|thread_id| {
            // Clone the Arc (cheap reference count increment)
            let g = Arc::clone(&graph);

            thread::spawn(move || {
                // Each thread takes its own snapshot - this is O(1) and lock-free.
                // The snapshot is now owned by this thread.
                let snap = g.snapshot();
                let count = snap.vertex_count();

                // Do some work with the snapshot.
                // This doesn't hold any locks on the main graph.
                let label_count = snap.vertices_with_label("Node").count();

                println!(
                    "  Thread {}: snapshot version={}, vertices={}, labeled={}",
                    thread_id,
                    snap.version(),
                    count,
                    label_count
                );

                count
            })
        })
        .collect();

    // Wait for all threads and collect results
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let elapsed = start.elapsed();

    println!("\nAll threads completed in {:?}", elapsed);
    println!("All threads saw {} vertices", results[0]);
    assert!(
        results.iter().all(|&c| c == 100),
        "All threads should see 100 vertices"
    );

    // Demonstrate that snapshots can outlive the source graph.
    // This is possible because CowSnapshot owns its data via Arc<CowGraphState>.
    println!("\n--- Snapshot Outliving Scope ---");
    let snap = {
        // Create a temporary graph
        let inner_graph = CowGraph::new();
        inner_graph.add_vertex("Temp", HashMap::new());

        // Take a snapshot
        let s = inner_graph.snapshot();

        // inner_graph will be dropped at the end of this block,
        // but the snapshot still owns a reference to the state.
        s
    };
    // inner_graph is now dropped, but snap is still valid!

    // Snapshot is still valid because it owns its state via Arc
    println!(
        "Snapshot from dropped graph: {} vertices",
        snap.vertex_count()
    );
    assert_eq!(snap.vertex_count(), 1);

    println!("\n[pass] Thread safety verified!");
    println!();
}

// =============================================================================
// Part 4: Batch Operations
// =============================================================================

/// Demonstrates atomic batch operations with automatic rollback.
///
/// The `batch()` method provides multi-operation atomicity:
/// - All operations in the batch succeed together, or
/// - If any operation fails, ALL operations are rolled back
///
/// This is implemented using COW's structural sharing:
/// 1. Clone the current state (O(1) due to structural sharing)
/// 2. Execute all operations on the cloned state
/// 3. If successful, atomically swap the new state in
/// 4. If failed, discard the cloned state (original unchanged)
fn demo_batch_operations() {
    println!("============================================================");
    println!("PART 4: BATCH OPERATIONS");
    println!("============================================================\n");

    let graph = CowGraph::new();
    graph.add_vertex(
        "Existing",
        props(&[("name", Value::String("Existing".to_string()))]),
    );
    println!("Initial state: {} vertices", graph.vertex_count());

    // SUCCESSFUL BATCH: All operations complete
    println!("\n--- Successful Batch ---");
    let result = graph.batch(|ctx| {
        // BatchContext provides mutation methods similar to CowGraph.
        // All mutations are applied to a working copy of the state.
        let alice = ctx.add_vertex(
            "Person",
            props(&[("name", Value::String("Alice".to_string()))]),
        );
        let bob = ctx.add_vertex(
            "Person",
            props(&[("name", Value::String("Bob".to_string()))]),
        );

        // Can reference vertices created within the same batch
        ctx.add_edge(alice, bob, "knows", HashMap::new())?;

        // Return values from the batch
        Ok((alice, bob))
    });

    match result {
        Ok((alice, bob)) => {
            println!("Batch succeeded!");
            println!("  Created Alice: {:?}", alice);
            println!("  Created Bob: {:?}", bob);
            println!(
                "  Graph now has {} vertices, {} edges",
                graph.vertex_count(),
                graph.edge_count()
            );
        }
        Err(e) => println!("Batch failed: {}", e),
    }

    // FAILED BATCH: Should rollback all changes
    println!("\n--- Failed Batch (Rollback) ---");
    let vertices_before = graph.vertex_count();

    let result: Result<(), BatchError> = graph.batch(|ctx| {
        // This vertex would be created in the working copy
        ctx.add_vertex(
            "Temp",
            props(&[("name", Value::String("Temp".to_string()))]),
        );

        // This will fail because vertex 999 doesn't exist.
        // When this fails, the entire batch is discarded.
        ctx.add_edge(
            interstellar::value::VertexId(999), // Non-existent!
            interstellar::value::VertexId(0),
            "invalid",
            HashMap::new(),
        )?;

        Ok(())
    });

    match result {
        Ok(_) => println!("Batch unexpectedly succeeded"),
        Err(e) => {
            println!("Batch failed as expected: {}", e);
            println!("  Vertices before: {}", vertices_before);
            println!("  Vertices after: {}", graph.vertex_count());

            // CRITICAL: The graph should be UNCHANGED because the batch failed.
            // The "Temp" vertex that was created in the batch should NOT exist.
            assert_eq!(
                graph.vertex_count(),
                vertices_before,
                "Graph should be unchanged after failed batch"
            );
            println!("  [pass] Rollback verified!");
        }
    }
    println!();
}

// =============================================================================
// Part 5: GQL Mutations
// =============================================================================

/// Demonstrates GQL mutation support via `execute_mutation()`.
///
/// CowGraph provides a unified mutation API that:
/// - Parses GQL statements
/// - Detects if the statement is read-only or a mutation
/// - Executes mutations atomically (all succeed or none)
/// - Returns results from RETURN clauses
///
/// Note: Read-only queries should use `snapshot().gql()` instead.
/// The `execute_mutation()` method is specifically for mutations.
fn demo_gql_mutations() {
    println!("============================================================");
    println!("PART 5: GQL MUTATIONS");
    println!("============================================================\n");

    // Each demo section uses a fresh graph to avoid state pollution
    let graph = CowGraph::new();

    // CREATE: Add vertices using GQL syntax.
    // The GQL parser converts this to internal mutation operations.
    println!("--- CREATE via GQL ---");
    match graph.execute_mutation("CREATE (n:Person {name: 'Alice', age: 30}) RETURN n") {
        Ok(results) => {
            println!("CREATE succeeded: {} result(s)", results.len());
            println!("  Graph now has {} vertices", graph.vertex_count());
        }
        Err(e) => println!("CREATE failed: {}", e),
    }

    // Create another vertex without RETURN
    match graph.execute_mutation("CREATE (n:Person {name: 'Bob', age: 25})") {
        Ok(_) => println!("Created Bob"),
        Err(e) => println!("Failed to create Bob: {}", e),
    }

    // QUERY: Use snapshot for read operations.
    // This is the recommended pattern: mutate via execute_mutation,
    // read via snapshot.
    println!("\n--- Query via Snapshot ---");
    let snap = graph.snapshot();
    println!(
        "Snapshot has {} vertices, {} edges",
        snap.vertex_count(),
        snap.edge_count()
    );

    let people: Vec<_> = snap.vertices_with_label("Person").collect();
    println!("People in snapshot:");
    for person in &people {
        println!(
            "  {} - name={:?}, age={:?}",
            person.id.0,
            person.properties.get("name"),
            person.properties.get("age")
        );
    }

    // SET: Update properties using GQL.
    // MATCH finds existing vertices, SET updates their properties.
    println!("\n--- SET via GQL ---");
    match graph.execute_mutation("MATCH (n:Person {name: 'Alice'}) SET n.status = 'active'") {
        Ok(_) => {
            // Take a new snapshot to see the updated state
            let snap2 = graph.snapshot();
            let alice: Vec<_> = snap2
                .vertices_with_label("Person")
                .filter(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
                .collect();
            if let Some(a) = alice.first() {
                println!("Alice's status: {:?}", a.properties.get("status"));
            }
        }
        Err(e) => println!("SET failed: {}", e),
    }

    // DELETE: Remove vertices using GQL.
    // Note: Use DETACH DELETE if the vertex has edges.
    println!("\n--- DELETE via GQL ---");
    let count_before = graph.vertex_count();
    match graph.execute_mutation("MATCH (n:Person {name: 'Bob'}) DELETE n") {
        Ok(_) => {
            println!("DELETE succeeded");
            println!("  Vertices before: {}", count_before);
            println!("  Vertices after: {}", graph.vertex_count());
        }
        Err(e) => println!("DELETE failed: {}", e),
    }
    println!();
}

// =============================================================================
// Part 6: Performance Characteristics
// =============================================================================

/// Demonstrates the performance characteristics of COW snapshots.
///
/// Key performance properties:
/// - Snapshot creation is O(1) regardless of graph size
/// - This is achieved through structural sharing (im crate)
/// - Vertex lookup is O(log₃₂ n), which is effectively O(1) for practical sizes
///   (log₃₂(1,000,000) ≈ 4)
///
/// The benchmarks here are illustrative; for accurate measurements,
/// use `cargo bench` with the criterion benchmarks.
fn demo_performance() {
    println!("============================================================");
    println!("PART 6: PERFORMANCE CHARACTERISTICS");
    println!("============================================================\n");

    let graph = CowGraph::new();

    // Create a larger graph to make measurements more meaningful
    println!("Creating graph with 10,000 vertices...");
    let start = Instant::now();
    for i in 0..10_000 {
        graph.add_vertex("Node", props(&[("id", Value::Int(i))]));
    }
    let create_time = start.elapsed();
    println!("  Created in {:?}", create_time);
    println!("  Final version: {}", graph.version());

    // Benchmark snapshot creation.
    // This should be O(1) - just cloning Arc pointers.
    println!("\nBenchmarking snapshot creation (1000 iterations)...");
    let start = Instant::now();
    for _ in 0..1000 {
        let _snap = graph.snapshot();
    }
    let snap_time = start.elapsed();
    println!("  Total: {:?}", snap_time);
    println!("  Per snapshot: {:?}", snap_time / 1000);

    // Benchmark vertex lookup via snapshot.
    // This is O(log₃₂ n) due to the im::HashMap tree structure.
    println!("\nBenchmarking vertex lookup (1000 iterations)...");
    let snap = graph.snapshot();
    let start = Instant::now();
    for i in 0..1000 {
        let id = interstellar::value::VertexId(i % 10_000);
        let _v = snap.get_vertex(id);
    }
    let lookup_time = start.elapsed();
    println!("  Total: {:?}", lookup_time);
    println!("  Per lookup: {:?}", lookup_time / 1000);

    // Demonstrate that snapshot creation time is independent of graph size.
    // This is the key property of structural sharing.
    println!("\nSnapshot size independence (O(1) creation):");
    for size in [1000, 5000, 10_000] {
        let g = CowGraph::new();
        for i in 0..size {
            g.add_vertex("Node", props(&[("id", Value::Int(i as i64))]));
        }

        let start = Instant::now();
        for _ in 0..100 {
            let _s = g.snapshot();
        }
        let time = start.elapsed() / 100;
        println!("  {} vertices: {:?} per snapshot", size, time);
    }
    println!();
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    println!("=== Interstellar Copy-on-Write Snapshots Example ===\n");

    demo_basic_operations();
    demo_snapshot_isolation();
    demo_thread_safety();
    demo_batch_operations();
    demo_gql_mutations();
    demo_performance();

    println!("============================================================");
    println!("SUMMARY: COW SNAPSHOT FEATURES DEMONSTRATED");
    println!("============================================================\n");

    println!("Core Features:");
    println!("  - CowGraph::new() - Create COW-enabled graph");
    println!("  - graph.add_vertex() / add_edge() - Mutations via &self");
    println!("  - graph.snapshot() - O(1) snapshot creation");
    println!("  - Snapshot isolation - Each snapshot sees frozen state");
    println!();

    println!("Thread Safety:");
    println!("  - CowGraph and CowSnapshot are Send + Sync");
    println!("  - Snapshots can be used across threads");
    println!("  - Readers never block writers");
    println!("  - Snapshots can outlive the source graph");
    println!();

    println!("Atomic Operations:");
    println!("  - graph.batch(|ctx| {{ ... }}) - Multi-op atomicity");
    println!("  - Automatic rollback on error");
    println!("  - graph.execute_mutation(gql) - GQL mutation support");
    println!();

    println!("Performance:");
    println!("  - Snapshot creation: O(1) via structural sharing");
    println!("  - Vertex lookup: O(log₃₂ n) ≈ O(1) for practical sizes");
    println!("  - Clone is O(1) regardless of graph size");
    println!();

    println!("=== Example Complete ===");
}
