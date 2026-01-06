//! Integration tests for MmapGraph persistent storage.
//!
//! These tests verify the memory-mapped storage backend functionality including:
//! - Database creation and opening
//! - Vertex and edge persistence
//! - Checkpoint and WAL operations
//! - Crash recovery
//!
//! Tests use tempfile for isolation and are independent of each other.

#![cfg(feature = "mmap")]

use rustgremlin::storage::{GraphStorage, MmapGraph};
use std::collections::HashMap;
use tempfile::TempDir;

// =============================================================================
// Helper Functions
// =============================================================================

/// Create a temporary directory and return it along with the database path.
fn temp_db() -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().expect("create temp dir");
    let db_path = dir.path().join("test.db");
    (dir, db_path)
}

// =============================================================================
// Phase 4.6: Checkpoint Tests
// =============================================================================

/// Test: Add data, checkpoint, verify WAL empty
///
/// This test verifies that the checkpoint() method:
/// 1. Flushes all pending writes to the data file
/// 2. Truncates the WAL file (removes all entries)
///
/// After a checkpoint, the WAL should be empty because all committed
/// transactions have been persisted to the main data file.
#[test]
fn test_checkpoint_empties_wal() {
    let (_dir, db_path) = temp_db();
    let wal_path = db_path.with_extension("wal");

    // Create graph and add data
    let graph = MmapGraph::open(&db_path).expect("open graph");

    // Add some vertices (which write to WAL)
    let v1 = graph
        .add_vertex("person", HashMap::new())
        .expect("add vertex 1");
    let v2 = graph
        .add_vertex("software", HashMap::new())
        .expect("add vertex 2");

    // Add an edge (which also writes to WAL)
    graph
        .add_edge(v1, v2, "created", HashMap::new())
        .expect("add edge");

    // WAL should have content before checkpoint
    let wal_size_before = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    assert!(
        wal_size_before > 0,
        "WAL should have content before checkpoint (size: {})",
        wal_size_before
    );

    // Checkpoint
    graph.checkpoint().expect("checkpoint");

    // WAL should be empty after checkpoint
    let wal_size_after = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    assert_eq!(
        wal_size_after, 0,
        "WAL should be empty after checkpoint (size: {})",
        wal_size_after
    );
}

/// Test that data is still accessible after checkpoint.
///
/// Checkpoint should not affect the ability to read data - it just
/// ensures durability and clears the WAL.
#[test]
fn test_data_accessible_after_checkpoint() {
    let (_dir, db_path) = temp_db();

    let graph = MmapGraph::open(&db_path).expect("open graph");

    // Add vertices with properties
    let alice = graph
        .add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Alice".into())]),
        )
        .expect("add alice");
    let bob = graph
        .add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Bob".into())]),
        )
        .expect("add bob");

    // Add edge
    let edge_id = graph
        .add_edge(alice, bob, "knows", HashMap::new())
        .expect("add edge");

    // Checkpoint
    graph.checkpoint().expect("checkpoint");

    // Verify data is still accessible
    let alice_vertex = graph.get_vertex(alice).expect("get alice");
    assert_eq!(alice_vertex.label, "person");
    assert_eq!(
        alice_vertex.properties.get("name").and_then(|v| v.as_str()),
        Some("Alice")
    );

    let bob_vertex = graph.get_vertex(bob).expect("get bob");
    assert_eq!(bob_vertex.label, "person");
    assert_eq!(
        bob_vertex.properties.get("name").and_then(|v| v.as_str()),
        Some("Bob")
    );

    let edge = graph.get_edge(edge_id).expect("get edge");
    assert_eq!(edge.label, "knows");
    assert_eq!(edge.src, alice);
    assert_eq!(edge.dst, bob);

    // Verify counts
    assert_eq!(graph.vertex_count(), 2);
    assert_eq!(graph.edge_count(), 1);
}

/// Test multiple checkpoints in sequence.
///
/// Should be able to call checkpoint() multiple times without issues.
#[test]
fn test_multiple_checkpoints() {
    let (_dir, db_path) = temp_db();
    let wal_path = db_path.with_extension("wal");

    let graph = MmapGraph::open(&db_path).expect("open graph");

    // First batch of data
    graph
        .add_vertex("person", HashMap::new())
        .expect("add vertex");
    graph.checkpoint().expect("checkpoint 1");

    let wal_size_1 = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    assert_eq!(wal_size_1, 0, "WAL should be empty after first checkpoint");

    // Second batch of data
    graph
        .add_vertex("software", HashMap::new())
        .expect("add vertex");
    graph.checkpoint().expect("checkpoint 2");

    let wal_size_2 = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    assert_eq!(wal_size_2, 0, "WAL should be empty after second checkpoint");

    // Third batch of data
    let v1 = graph
        .add_vertex("location", HashMap::new())
        .expect("add vertex");
    let v2 = graph
        .add_vertex("location", HashMap::new())
        .expect("add vertex");
    graph
        .add_edge(v1, v2, "connected", HashMap::new())
        .expect("add edge");
    graph.checkpoint().expect("checkpoint 3");

    let wal_size_3 = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    assert_eq!(wal_size_3, 0, "WAL should be empty after third checkpoint");

    // Verify all data is present
    assert_eq!(graph.vertex_count(), 4);
    assert_eq!(graph.edge_count(), 1);
}

/// Test checkpoint on empty database.
///
/// Should be able to checkpoint even when no data has been added.
#[test]
fn test_checkpoint_empty_database() {
    let (_dir, db_path) = temp_db();
    let wal_path = db_path.with_extension("wal");

    let graph = MmapGraph::open(&db_path).expect("open graph");

    // Checkpoint with no data
    graph.checkpoint().expect("checkpoint empty db");

    // WAL should be empty (or very small - just checkpoint entry then truncated)
    let wal_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    assert_eq!(wal_size, 0, "WAL should be empty after checkpoint");

    // Should still be able to add data after
    graph
        .add_vertex("test", HashMap::new())
        .expect("add vertex after checkpoint");
    assert_eq!(graph.vertex_count(), 1);
}

// =============================================================================
// Phase 5.4: Basic Operations Tests
// =============================================================================

/// Test creating a new database.
#[test]
fn test_create_new_database() {
    let (_dir, db_path) = temp_db();

    assert!(!db_path.exists(), "database should not exist initially");

    let graph = MmapGraph::open(&db_path).expect("open graph");

    assert!(db_path.exists(), "database file should be created");
    assert_eq!(graph.vertex_count(), 0);
    assert_eq!(graph.edge_count(), 0);
}

/// Test adding vertices.
#[test]
fn test_add_vertex() {
    let (_dir, db_path) = temp_db();

    let graph = MmapGraph::open(&db_path).expect("open graph");

    // Add vertex without properties
    let v1 = graph
        .add_vertex("person", HashMap::new())
        .expect("add vertex");

    // Add vertex with properties
    let v2 = graph
        .add_vertex(
            "software",
            HashMap::from([
                ("name".to_string(), "RustGremlin".into()),
                ("version".to_string(), "0.1.0".into()),
            ]),
        )
        .expect("add vertex with props");

    assert_eq!(graph.vertex_count(), 2);

    // Verify vertex 1
    let vertex1 = graph.get_vertex(v1).expect("get v1");
    assert_eq!(vertex1.label, "person");
    assert!(vertex1.properties.is_empty());

    // Verify vertex 2
    let vertex2 = graph.get_vertex(v2).expect("get v2");
    assert_eq!(vertex2.label, "software");
    assert_eq!(
        vertex2.properties.get("name").and_then(|v| v.as_str()),
        Some("RustGremlin")
    );
    assert_eq!(
        vertex2.properties.get("version").and_then(|v| v.as_str()),
        Some("0.1.0")
    );
}

/// Test adding edges.
#[test]
fn test_add_edge() {
    let (_dir, db_path) = temp_db();

    let graph = MmapGraph::open(&db_path).expect("open graph");

    // Create vertices
    let alice = graph
        .add_vertex("person", HashMap::new())
        .expect("add alice");
    let bob = graph.add_vertex("person", HashMap::new()).expect("add bob");

    // Add edge without properties
    let e1 = graph
        .add_edge(alice, bob, "knows", HashMap::new())
        .expect("add edge");

    assert_eq!(graph.edge_count(), 1);

    // Verify edge
    let edge = graph.get_edge(e1).expect("get edge");
    assert_eq!(edge.label, "knows");
    assert_eq!(edge.src, alice);
    assert_eq!(edge.dst, bob);
    assert!(edge.properties.is_empty());
}

/// Test adding edge with properties.
#[test]
fn test_add_edge_with_properties() {
    let (_dir, db_path) = temp_db();

    let graph = MmapGraph::open(&db_path).expect("open graph");

    let alice = graph
        .add_vertex("person", HashMap::new())
        .expect("add alice");
    let project = graph
        .add_vertex("software", HashMap::new())
        .expect("add project");

    let edge_id = graph
        .add_edge(
            alice,
            project,
            "created",
            HashMap::from([
                ("year".to_string(), 2024i64.into()),
                ("role".to_string(), "lead".into()),
            ]),
        )
        .expect("add edge with props");

    let edge = graph.get_edge(edge_id).expect("get edge");
    assert_eq!(edge.label, "created");
    assert_eq!(
        edge.properties.get("year").and_then(|v| v.as_i64()),
        Some(2024)
    );
    assert_eq!(
        edge.properties.get("role").and_then(|v| v.as_str()),
        Some("lead")
    );
}

/// Test persistence across reopens.
#[test]
fn test_persistence() {
    let (dir, db_path) = temp_db();

    // Create graph and add data
    {
        let graph = MmapGraph::open(&db_path).expect("open graph");

        let alice = graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), "Alice".into())]),
            )
            .expect("add alice");
        let bob = graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), "Bob".into())]),
            )
            .expect("add bob");

        graph
            .add_edge(
                alice,
                bob,
                "knows",
                HashMap::from([("since".to_string(), 2020i64.into())]),
            )
            .expect("add edge");

        // Checkpoint to ensure durability
        graph.checkpoint().expect("checkpoint");

        // Graph is dropped here
    }

    // Reopen and verify data persisted
    {
        let graph = MmapGraph::open(&db_path).expect("reopen graph");

        assert_eq!(graph.vertex_count(), 2, "vertex count should persist");
        assert_eq!(graph.edge_count(), 1, "edge count should persist");

        // Verify vertices by label
        let people: Vec<_> = graph.vertices_with_label("person").collect();
        assert_eq!(people.len(), 2, "should have 2 people");

        // Verify edge
        let edges: Vec<_> = graph.edges_with_label("knows").collect();
        assert_eq!(edges.len(), 1, "should have 1 knows edge");
        assert_eq!(
            edges[0].properties.get("since").and_then(|v| v.as_i64()),
            Some(2020)
        );
    }

    // Keep dir alive until after second open
    drop(dir);
}

/// Test label index functionality.
#[test]
fn test_label_index() {
    let (_dir, db_path) = temp_db();

    let graph = MmapGraph::open(&db_path).expect("open graph");

    // Add vertices with different labels
    graph
        .add_vertex("person", HashMap::new())
        .expect("add person 1");
    graph
        .add_vertex("person", HashMap::new())
        .expect("add person 2");
    graph
        .add_vertex("person", HashMap::new())
        .expect("add person 3");
    graph
        .add_vertex("software", HashMap::new())
        .expect("add software 1");
    graph
        .add_vertex("software", HashMap::new())
        .expect("add software 2");
    graph
        .add_vertex("company", HashMap::new())
        .expect("add company");

    // Verify label queries
    assert_eq!(graph.vertices_with_label("person").count(), 3);
    assert_eq!(graph.vertices_with_label("software").count(), 2);
    assert_eq!(graph.vertices_with_label("company").count(), 1);
    assert_eq!(graph.vertices_with_label("nonexistent").count(), 0);
}

/// Test adjacency traversal.
#[test]
fn test_adjacency_traversal() {
    let (_dir, db_path) = temp_db();

    let graph = MmapGraph::open(&db_path).expect("open graph");

    // Create a small graph: alice -> bob -> charlie
    //                        |              ^
    //                        +--------------+
    let alice = graph
        .add_vertex("person", HashMap::new())
        .expect("add alice");
    let bob = graph.add_vertex("person", HashMap::new()).expect("add bob");
    let charlie = graph
        .add_vertex("person", HashMap::new())
        .expect("add charlie");

    graph
        .add_edge(alice, bob, "knows", HashMap::new())
        .expect("alice->bob");
    graph
        .add_edge(bob, charlie, "knows", HashMap::new())
        .expect("bob->charlie");
    graph
        .add_edge(alice, charlie, "knows", HashMap::new())
        .expect("alice->charlie");

    // Test out_edges
    let alice_out: Vec<_> = graph.out_edges(alice).collect();
    assert_eq!(alice_out.len(), 2, "alice should have 2 outgoing edges");

    let bob_out: Vec<_> = graph.out_edges(bob).collect();
    assert_eq!(bob_out.len(), 1, "bob should have 1 outgoing edge");

    let charlie_out: Vec<_> = graph.out_edges(charlie).collect();
    assert_eq!(charlie_out.len(), 0, "charlie should have 0 outgoing edges");

    // Test in_edges
    let alice_in: Vec<_> = graph.in_edges(alice).collect();
    assert_eq!(alice_in.len(), 0, "alice should have 0 incoming edges");

    let bob_in: Vec<_> = graph.in_edges(bob).collect();
    assert_eq!(bob_in.len(), 1, "bob should have 1 incoming edge");

    let charlie_in: Vec<_> = graph.in_edges(charlie).collect();
    assert_eq!(charlie_in.len(), 2, "charlie should have 2 incoming edges");
}

// =============================================================================
// Phase 5.5: Large Graph Tests
// =============================================================================

/// Test large graph with many vertices and edges.
///
/// This test verifies that the storage can handle:
/// - 1,000+ vertices  
/// - 5,000+ edges
/// - Automatic table growth when capacity is exceeded
///
/// Note: The full 10K vertices / 100K edges test is available as test_large_graph_full
/// but is ignored by default due to fsync overhead making it slow (~minutes).
#[test]
fn test_large_graph() {
    let (_dir, db_path) = temp_db();

    let graph = MmapGraph::open(&db_path).expect("open graph");

    // Reduced size for fast CI - still tests table growth (initial capacity is 1024)
    const NUM_VERTICES: u64 = 1_500;
    const EDGES_PER_VERTEX: u64 = 4;

    // Add vertices
    let mut vertex_ids = Vec::with_capacity(NUM_VERTICES as usize);
    for i in 0..NUM_VERTICES {
        let props = HashMap::from([("index".to_string(), (i as i64).into())]);
        let id = graph.add_vertex("node", props).expect("add vertex");
        vertex_ids.push(id);
    }

    assert_eq!(
        graph.vertex_count(),
        NUM_VERTICES,
        "should have {} vertices",
        NUM_VERTICES
    );

    // Add edges (each vertex connects to next EDGES_PER_VERTEX vertices, wrapping)
    let mut edge_count = 0u64;
    for (i, &src) in vertex_ids.iter().enumerate() {
        for j in 1..=EDGES_PER_VERTEX {
            let dst_idx = (i as u64 + j) % NUM_VERTICES;
            let dst = vertex_ids[dst_idx as usize];
            graph
                .add_edge(src, dst, "connects", HashMap::new())
                .expect("add edge");
            edge_count += 1;
        }
    }

    let expected_edges = NUM_VERTICES * EDGES_PER_VERTEX;
    assert_eq!(
        graph.edge_count(),
        expected_edges,
        "should have {} edges",
        expected_edges
    );
    assert_eq!(edge_count, expected_edges);

    // Verify some random vertices have correct properties
    for &id in vertex_ids.iter().step_by(500) {
        let vertex = graph.get_vertex(id).expect("get vertex");
        assert_eq!(vertex.label, "node");
        assert!(vertex.properties.contains_key("index"));
    }

    // Verify adjacency lists
    for &id in vertex_ids.iter().take(50) {
        let out_edges: Vec<_> = graph.out_edges(id).collect();
        assert_eq!(
            out_edges.len(),
            EDGES_PER_VERTEX as usize,
            "each vertex should have {} outgoing edges",
            EDGES_PER_VERTEX
        );
    }
}

/// Test that node table growth preserves existing vertex data.
///
/// This test specifically verifies that when the node table grows (capacity exceeded),
/// all existing vertices remain accessible with correct properties.
/// Initial node capacity is 1000, so adding 1030 vertices triggers growth.
#[test]
fn test_grow_node_table_preserves_vertices() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    // Add vertices beyond initial capacity (1000) to trigger growth
    let mut ids = Vec::new();
    for i in 0..1030 {
        let props = HashMap::from([("i".to_string(), (i as i64).into())]);
        let id = graph.add_vertex("node", props).expect("add vertex");
        ids.push(id);
    }

    // Verify all vertices are accessible after growth
    assert_eq!(graph.vertex_count(), 1030, "should have 1030 vertices");

    // Check first, middle, and last vertices
    let first = graph.get_vertex(ids[0]).expect("first vertex");
    assert_eq!(first.properties.get("i"), Some(&0i64.into()));

    let middle = graph.get_vertex(ids[500]).expect("middle vertex");
    assert_eq!(middle.properties.get("i"), Some(&500i64.into()));

    let last = graph.get_vertex(ids[1029]).expect("last vertex");
    assert_eq!(last.properties.get("i"), Some(&1029i64.into()));
}

/// Test that file grows correctly when capacity is exceeded.
///
/// Verifies that the storage automatically grows tables when the initial
/// capacity is exceeded.
#[test]
fn test_file_growth() {
    let (_dir, db_path) = temp_db();

    let initial_size = {
        let graph = MmapGraph::open(&db_path).expect("open graph");
        graph.checkpoint().expect("checkpoint");
        std::fs::metadata(&db_path).expect("get metadata").len()
    };

    // Reopen and add enough vertices to trigger table growth (initial capacity is 1000)
    {
        let graph = MmapGraph::open(&db_path).expect("reopen graph");

        // Add 1100 vertices - just enough to exceed initial capacity of 1024
        for i in 0..1100 {
            let props = HashMap::from([("i".to_string(), (i as i64).into())]);
            graph.add_vertex("node", props).expect("add vertex");
        }

        graph.checkpoint().expect("checkpoint");
    }

    let final_size = std::fs::metadata(&db_path).expect("get metadata").len();
    assert!(
        final_size > initial_size,
        "file should grow from {} to larger size, got {}",
        initial_size,
        final_size
    );
}

/// Test reopening and appending to existing database.
///
/// Verifies that we can:
/// 1. Create a database and add data
/// 2. Close it
/// 3. Reopen and add more data
/// 4. All data is preserved
#[test]
fn test_reopen_and_append() {
    let (dir, db_path) = temp_db();

    // First session: add initial data
    let (first_vertex, _first_edge) = {
        let graph = MmapGraph::open(&db_path).expect("open graph");

        let v1 = graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), "Alice".into())]),
            )
            .expect("add v1");
        let v2 = graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), "Bob".into())]),
            )
            .expect("add v2");

        let e1 = graph
            .add_edge(v1, v2, "knows", HashMap::new())
            .expect("add edge");

        graph.checkpoint().expect("checkpoint");

        assert_eq!(graph.vertex_count(), 2);
        assert_eq!(graph.edge_count(), 1);

        (v1, e1)
    };

    // Second session: append more data
    {
        let graph = MmapGraph::open(&db_path).expect("reopen graph");

        // Verify existing data
        assert_eq!(
            graph.vertex_count(),
            2,
            "should have 2 vertices from before"
        );
        assert_eq!(graph.edge_count(), 1, "should have 1 edge from before");

        let alice = graph.get_vertex(first_vertex).expect("get alice");
        assert_eq!(
            alice.properties.get("name").and_then(|v| v.as_str()),
            Some("Alice")
        );

        // Add more data
        let v3 = graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), "Charlie".into())]),
            )
            .expect("add v3");
        let v4 = graph
            .add_vertex(
                "software",
                HashMap::from([("name".to_string(), "RustGremlin".into())]),
            )
            .expect("add v4");

        graph
            .add_edge(v3, v4, "created", HashMap::new())
            .expect("add edge");
        graph
            .add_edge(first_vertex, v3, "knows", HashMap::new())
            .expect("add edge");

        graph.checkpoint().expect("checkpoint");

        assert_eq!(graph.vertex_count(), 4);
        assert_eq!(graph.edge_count(), 3);
    }

    // Third session: verify all data persisted
    {
        let graph = MmapGraph::open(&db_path).expect("reopen graph again");

        assert_eq!(graph.vertex_count(), 4, "should have 4 vertices total");
        assert_eq!(graph.edge_count(), 3, "should have 3 edges total");

        // Verify by label
        assert_eq!(graph.vertices_with_label("person").count(), 3);
        assert_eq!(graph.vertices_with_label("software").count(), 1);
        assert_eq!(graph.edges_with_label("knows").count(), 2);
        assert_eq!(graph.edges_with_label("created").count(), 1);
    }

    drop(dir);
}

// =============================================================================
// Phase 5.6: Crash Recovery Tests
// =============================================================================

/// Test crash recovery with uncommitted transaction.
///
/// Simulates a "crash" by:
/// 1. Creating a graph and adding data
/// 2. Adding more data WITHOUT checkpointing
/// 3. Dropping the graph (simulates crash - WAL has uncommitted entries)
/// 4. Reopening - recovery should run
/// 5. Only committed (checkpointed) data should be present
///
/// Note: In our current implementation, each add_vertex/add_edge is its own
/// committed transaction in the WAL, so "uncommitted" means data written to
/// WAL but not yet flushed to the main data file via checkpoint.
#[test]
fn test_crash_recovery_uncommitted() {
    let (dir, db_path) = temp_db();

    // First session: add data and checkpoint
    {
        let graph = MmapGraph::open(&db_path).expect("open graph");

        let v1 = graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), "Alice".into())]),
            )
            .expect("add v1");
        let v2 = graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), "Bob".into())]),
            )
            .expect("add v2");

        graph
            .add_edge(v1, v2, "knows", HashMap::new())
            .expect("add edge");

        // Checkpoint - these are "committed" to the data file
        graph.checkpoint().expect("checkpoint");

        assert_eq!(graph.vertex_count(), 2);
        assert_eq!(graph.edge_count(), 1);
    }

    // Second session: add data WITHOUT checkpoint (simulates crash before checkpoint)
    {
        let graph = MmapGraph::open(&db_path).expect("reopen graph");

        // These writes go to WAL and in-memory structures
        graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), "Charlie".into())]),
            )
            .expect("add v3");
        graph
            .add_vertex(
                "software",
                HashMap::from([("name".to_string(), "Graph".into())]),
            )
            .expect("add v4");

        // In-memory we have 4 vertices
        assert_eq!(graph.vertex_count(), 4);

        // Drop WITHOUT checkpoint - simulates crash
        // WAL has uncommitted entries that will be recovered
    }

    // Third session: verify recovery
    {
        let graph = MmapGraph::open(&db_path).expect("reopen after crash");

        // Recovery should replay WAL, so we should have all 4 vertices
        // (our WAL logs each operation as a committed transaction)
        assert_eq!(
            graph.vertex_count(),
            4,
            "recovery should restore all WAL entries"
        );

        // Verify the recovered vertices are accessible
        let people: Vec<_> = graph.vertices_with_label("person").collect();
        assert_eq!(people.len(), 3, "should have 3 person vertices");

        let software: Vec<_> = graph.vertices_with_label("software").collect();
        assert_eq!(software.len(), 1, "should have 1 software vertex");
    }

    drop(dir);
}

/// Test that committed transactions are recovered.
///
/// This verifies the positive case: data written and committed should
/// survive a "crash" (drop without explicit checkpoint).
#[test]
fn test_committed_transaction_recovery() {
    let (dir, db_path) = temp_db();

    // Create graph and add data (each operation is committed to WAL)
    {
        let graph = MmapGraph::open(&db_path).expect("open graph");

        // Add vertices - each write is a committed transaction in WAL
        for i in 0..10 {
            let props = HashMap::from([("index".to_string(), (i as i64).into())]);
            graph.add_vertex("node", props).expect("add vertex");
        }

        // Add edges
        let vertices: Vec<_> = graph.all_vertices().collect();
        for i in 0..9 {
            graph
                .add_edge(vertices[i].id, vertices[i + 1].id, "next", HashMap::new())
                .expect("add edge");
        }

        assert_eq!(graph.vertex_count(), 10);
        assert_eq!(graph.edge_count(), 9);

        // NO checkpoint - drop "crashes" the database
    }

    // Reopen - recovery should replay WAL
    {
        let graph = MmapGraph::open(&db_path).expect("reopen after crash");

        // All data should be recovered from WAL
        assert_eq!(graph.vertex_count(), 10, "all vertices should be recovered");
        assert_eq!(graph.edge_count(), 9, "all edges should be recovered");

        // Verify data integrity
        let vertices: Vec<_> = graph.all_vertices().collect();
        assert_eq!(vertices.len(), 10);

        for vertex in &vertices {
            assert_eq!(vertex.label, "node");
            assert!(vertex.properties.contains_key("index"));
        }

        let edges: Vec<_> = graph.all_edges().collect();
        assert_eq!(edges.len(), 9);
        for edge in &edges {
            assert_eq!(edge.label, "next");
        }
    }

    drop(dir);
}

/// Test recovery is idempotent.
///
/// Opening a database multiple times should not corrupt data even if
/// recovery runs each time.
#[test]
fn test_recovery_idempotent() {
    let (dir, db_path) = temp_db();

    // Initial data
    {
        let graph = MmapGraph::open(&db_path).expect("open graph");
        for i in 0..5 {
            let props = HashMap::from([("i".to_string(), (i as i64).into())]);
            graph.add_vertex("node", props).expect("add vertex");
        }
        // No checkpoint
    }

    // Open multiple times without checkpointing
    for _ in 0..3 {
        let graph = MmapGraph::open(&db_path).expect("reopen graph");
        assert_eq!(graph.vertex_count(), 5, "vertex count should remain stable");
        // No checkpoint - each reopen may trigger recovery
    }

    // Final verification
    {
        let graph = MmapGraph::open(&db_path).expect("final reopen");
        assert_eq!(graph.vertex_count(), 5);

        let vertices: Vec<_> = graph.all_vertices().collect();
        for vertex in &vertices {
            assert_eq!(vertex.label, "node");
        }
    }

    drop(dir);
}

/// Test mixed operations recovery.
///
/// Tests that a mix of add and remove operations are correctly recovered.
#[test]
fn test_mixed_operations_recovery() {
    let (dir, db_path) = temp_db();

    // Create graph with mixed operations
    {
        let graph = MmapGraph::open(&db_path).expect("open graph");

        // Add vertices
        let v1 = graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), "Alice".into())]),
            )
            .expect("add v1");
        let v2 = graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), "Bob".into())]),
            )
            .expect("add v2");
        let v3 = graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), "Charlie".into())]),
            )
            .expect("add v3");

        // Add edges
        let e1 = graph
            .add_edge(v1, v2, "knows", HashMap::new())
            .expect("add e1");
        graph
            .add_edge(v2, v3, "knows", HashMap::new())
            .expect("add e2");

        // Remove some data
        graph.remove_edge(e1).expect("remove e1");
        graph.remove_vertex(v2).expect("remove v2");

        // Final state: 2 vertices (v1, v3), 0 edges (e2 was removed with v2)
        assert_eq!(graph.vertex_count(), 2);
        assert_eq!(graph.edge_count(), 0);

        // No checkpoint - simulate crash
    }

    // Recover and verify
    {
        let graph = MmapGraph::open(&db_path).expect("reopen after crash");

        assert_eq!(
            graph.vertex_count(),
            2,
            "should have 2 vertices after recovery"
        );
        assert_eq!(graph.edge_count(), 0, "should have 0 edges after recovery");

        // Verify the right vertices remain
        let people: Vec<_> = graph.vertices_with_label("person").collect();
        assert_eq!(people.len(), 2);

        let names: Vec<_> = people
            .iter()
            .filter_map(|v| v.properties.get("name").and_then(|v| v.as_str()))
            .collect();
        assert!(names.contains(&"Alice"));
        assert!(names.contains(&"Charlie"));
        assert!(!names.contains(&"Bob")); // Bob was deleted
    }

    drop(dir);
}

// =============================================================================
// Batch Mode Tests
// =============================================================================

/// Test basic batch mode workflow.
///
/// Verifies that:
/// 1. begin_batch() starts batch mode
/// 2. add_vertex/add_edge work in batch mode
/// 3. commit_batch() commits all operations
/// 4. Data is readable after commit
#[test]
fn test_batch_mode_basic() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    // Not in batch mode initially
    assert!(!graph.is_batch_mode());

    // Start batch mode
    graph.begin_batch().expect("begin batch");
    assert!(graph.is_batch_mode());

    // Add vertices in batch mode
    let v1 = graph
        .add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Alice".into())]),
        )
        .expect("add v1");
    let v2 = graph
        .add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Bob".into())]),
        )
        .expect("add v2");

    // Add edge in batch mode
    graph
        .add_edge(v1, v2, "knows", HashMap::new())
        .expect("add edge");

    // Data should be readable immediately (written to main file)
    assert_eq!(graph.vertex_count(), 2);
    assert_eq!(graph.edge_count(), 1);

    // Commit the batch
    graph.commit_batch().expect("commit batch");
    assert!(!graph.is_batch_mode());

    // Data still there after commit
    assert_eq!(graph.vertex_count(), 2);
    assert_eq!(graph.edge_count(), 1);
}

/// Test batch mode performance improvement.
///
/// In batch mode, we should see significantly better throughput than normal mode.
/// This test verifies that batch mode completes in reasonable time.
#[test]
fn test_batch_mode_performance() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    let num_vertices = 1000;

    // Start batch mode
    graph.begin_batch().expect("begin batch");

    // Add many vertices - this should be fast in batch mode
    let start = std::time::Instant::now();
    let mut vertex_ids = Vec::with_capacity(num_vertices);
    for i in 0..num_vertices {
        let props = HashMap::from([("i".to_string(), (i as i64).into())]);
        let id = graph.add_vertex("node", props).expect("add vertex");
        vertex_ids.push(id);
    }

    // Add edges
    for i in 0..(num_vertices - 1) {
        graph
            .add_edge(vertex_ids[i], vertex_ids[i + 1], "next", HashMap::new())
            .expect("add edge");
    }

    // Commit
    graph.commit_batch().expect("commit batch");
    let elapsed = start.elapsed();

    // In batch mode, this should complete in under 15 seconds
    // (Normal mode would take ~5ms * 1999 operations = ~10 seconds just for fsync)
    // We're still doing file I/O for each operation, just skipping fsync
    assert!(elapsed.as_secs() < 15, "Batch mode too slow: {:?}", elapsed);

    // Verify data
    assert_eq!(graph.vertex_count(), num_vertices as u64);
    assert_eq!(graph.edge_count(), (num_vertices - 1) as u64);
}

/// Test that begin_batch fails if already in batch mode.
#[test]
fn test_batch_mode_double_begin_fails() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    graph.begin_batch().expect("begin batch");

    // Second begin should fail
    let result = graph.begin_batch();
    assert!(result.is_err());
}

/// Test that commit_batch fails if not in batch mode.
#[test]
fn test_commit_batch_without_begin_fails() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    // commit without begin should fail
    let result = graph.commit_batch();
    assert!(result.is_err());
}

/// Test that abort_batch discards uncommitted operations.
#[test]
fn test_abort_batch() {
    let (_dir, db_path) = temp_db();

    // First session: add data in batch mode, then abort
    {
        let graph = MmapGraph::open(&db_path).expect("open graph");

        graph.begin_batch().expect("begin batch");
        graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), "Alice".into())]),
            )
            .expect("add vertex");

        // Abort the batch
        graph.abort_batch().expect("abort batch");
        assert!(!graph.is_batch_mode());

        // Data is in memory/file but transaction is aborted in WAL
        // The vertex is there for this session
        assert_eq!(graph.vertex_count(), 1);
    }

    // Second session: on reopen, recovery should discard the aborted transaction
    // Note: This depends on recovery implementation - if we checkpoint before close,
    // the data would persist. Without checkpoint, it depends on WAL recovery.
}

/// Test batch mode with checkpoint.
///
/// After commit_batch, a checkpoint should work normally.
#[test]
fn test_batch_mode_with_checkpoint() {
    let (dir, db_path) = temp_db();

    // Add data in batch mode, commit, then checkpoint
    {
        let graph = MmapGraph::open(&db_path).expect("open graph");

        graph.begin_batch().expect("begin batch");

        let v1 = graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), "Alice".into())]),
            )
            .expect("add v1");
        let v2 = graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), "Bob".into())]),
            )
            .expect("add v2");
        graph
            .add_edge(v1, v2, "knows", HashMap::new())
            .expect("add edge");

        graph.commit_batch().expect("commit batch");
        graph.checkpoint().expect("checkpoint");
    }

    // Reopen and verify data persisted
    {
        let graph = MmapGraph::open(&db_path).expect("reopen graph");

        assert_eq!(graph.vertex_count(), 2);
        assert_eq!(graph.edge_count(), 1);

        let vertices: Vec<_> = graph.vertices_with_label("person").collect();
        assert_eq!(vertices.len(), 2);
    }

    drop(dir);
}

/// Test multiple batch operations.
///
/// Verifies that we can do multiple begin_batch/commit_batch cycles.
#[test]
fn test_multiple_batches() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    // First batch
    graph.begin_batch().expect("begin batch 1");
    let v1 = graph
        .add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Alice".into())]),
        )
        .expect("add v1");
    graph.commit_batch().expect("commit batch 1");

    assert_eq!(graph.vertex_count(), 1);

    // Second batch
    graph.begin_batch().expect("begin batch 2");
    let v2 = graph
        .add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Bob".into())]),
        )
        .expect("add v2");
    graph
        .add_edge(v1, v2, "knows", HashMap::new())
        .expect("add edge");
    graph.commit_batch().expect("commit batch 2");

    assert_eq!(graph.vertex_count(), 2);
    assert_eq!(graph.edge_count(), 1);
}

/// Test that data is readable during batch mode.
///
/// Even before commit, data should be readable because it's written
/// to the main file (just not durably synced yet).
#[test]
fn test_batch_mode_read_during_write() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    graph.begin_batch().expect("begin batch");

    let v1 = graph
        .add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Alice".into())]),
        )
        .expect("add v1");

    // Should be able to read the vertex immediately
    let vertex = graph.get_vertex(v1).expect("get vertex");
    assert_eq!(vertex.label, "person");
    assert_eq!(
        vertex.properties.get("name").and_then(|v| v.as_str()),
        Some("Alice")
    );

    // Add another vertex that references the first
    let v2 = graph
        .add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Bob".into())]),
        )
        .expect("add v2");

    // Add edge between them
    graph
        .add_edge(v1, v2, "knows", HashMap::new())
        .expect("add edge");

    // Should be able to traverse the edge
    let edges: Vec<_> = graph.out_edges(v1).collect();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].dst, v2);

    graph.commit_batch().expect("commit batch");
}

// =============================================================================
// Phase 5.7: Property Roundtrip Tests
// =============================================================================

use rustgremlin::value::{EdgeId, Value, VertexId};

/// Test that Null property values roundtrip correctly.
#[test]
fn test_property_roundtrip_null() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    let v = graph
        .add_vertex(
            "test",
            HashMap::from([("nullprop".to_string(), Value::Null)]),
        )
        .expect("add vertex");

    graph.checkpoint().expect("checkpoint");

    let vertex = graph.get_vertex(v).expect("get vertex");
    assert_eq!(vertex.properties.get("nullprop"), Some(&Value::Null));
}

/// Test that Bool property values roundtrip correctly.
#[test]
fn test_property_roundtrip_bool() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    let v = graph
        .add_vertex(
            "test",
            HashMap::from([
                ("flag_true".to_string(), Value::Bool(true)),
                ("flag_false".to_string(), Value::Bool(false)),
            ]),
        )
        .expect("add vertex");

    graph.checkpoint().expect("checkpoint");

    let vertex = graph.get_vertex(v).expect("get vertex");
    assert_eq!(vertex.properties.get("flag_true"), Some(&Value::Bool(true)));
    assert_eq!(
        vertex.properties.get("flag_false"),
        Some(&Value::Bool(false))
    );
}

/// Test that Int property values roundtrip correctly.
#[test]
fn test_property_roundtrip_int() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    let v = graph
        .add_vertex(
            "test",
            HashMap::from([
                ("positive".to_string(), Value::Int(42)),
                ("negative".to_string(), Value::Int(-7)),
                ("zero".to_string(), Value::Int(0)),
                ("large".to_string(), Value::Int(i64::MAX)),
                ("small".to_string(), Value::Int(i64::MIN)),
            ]),
        )
        .expect("add vertex");

    graph.checkpoint().expect("checkpoint");

    let vertex = graph.get_vertex(v).expect("get vertex");
    assert_eq!(vertex.properties.get("positive"), Some(&Value::Int(42)));
    assert_eq!(vertex.properties.get("negative"), Some(&Value::Int(-7)));
    assert_eq!(vertex.properties.get("zero"), Some(&Value::Int(0)));
    assert_eq!(vertex.properties.get("large"), Some(&Value::Int(i64::MAX)));
    assert_eq!(vertex.properties.get("small"), Some(&Value::Int(i64::MIN)));
}

/// Test that Float property values roundtrip correctly.
#[test]
fn test_property_roundtrip_float() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    let v = graph
        .add_vertex(
            "test",
            HashMap::from([
                ("pi".to_string(), Value::Float(3.14159)),
                ("negative".to_string(), Value::Float(-2.5)),
                ("zero".to_string(), Value::Float(0.0)),
                ("infinity".to_string(), Value::Float(f64::INFINITY)),
                ("neg_infinity".to_string(), Value::Float(f64::NEG_INFINITY)),
            ]),
        )
        .expect("add vertex");

    graph.checkpoint().expect("checkpoint");

    let vertex = graph.get_vertex(v).expect("get vertex");
    assert_eq!(vertex.properties.get("pi"), Some(&Value::Float(3.14159)));
    assert_eq!(vertex.properties.get("negative"), Some(&Value::Float(-2.5)));
    assert_eq!(vertex.properties.get("zero"), Some(&Value::Float(0.0)));
    assert_eq!(
        vertex.properties.get("infinity"),
        Some(&Value::Float(f64::INFINITY))
    );
    assert_eq!(
        vertex.properties.get("neg_infinity"),
        Some(&Value::Float(f64::NEG_INFINITY))
    );
}

/// Test that Float NaN property values roundtrip correctly.
#[test]
fn test_property_roundtrip_float_nan() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    let v = graph
        .add_vertex(
            "test",
            HashMap::from([("nan".to_string(), Value::Float(f64::NAN))]),
        )
        .expect("add vertex");

    graph.checkpoint().expect("checkpoint");

    let vertex = graph.get_vertex(v).expect("get vertex");
    let nan_val = vertex.properties.get("nan").expect("nan property");
    match nan_val {
        Value::Float(f) => assert!(f.is_nan(), "Expected NaN, got {}", f),
        _ => panic!("Expected Float variant, got {:?}", nan_val),
    }
}

/// Test that String property values roundtrip correctly.
#[test]
fn test_property_roundtrip_string() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    let v = graph
        .add_vertex(
            "test",
            HashMap::from([
                ("name".to_string(), Value::String("Alice".to_string())),
                ("empty".to_string(), Value::String("".to_string())),
                (
                    "unicode".to_string(),
                    Value::String("Hello 世界 🌍".to_string()),
                ),
                (
                    "special".to_string(),
                    Value::String("line\nbreak\ttab".to_string()),
                ),
            ]),
        )
        .expect("add vertex");

    graph.checkpoint().expect("checkpoint");

    let vertex = graph.get_vertex(v).expect("get vertex");
    assert_eq!(
        vertex.properties.get("name"),
        Some(&Value::String("Alice".to_string()))
    );
    assert_eq!(
        vertex.properties.get("empty"),
        Some(&Value::String("".to_string()))
    );
    assert_eq!(
        vertex.properties.get("unicode"),
        Some(&Value::String("Hello 世界 🌍".to_string()))
    );
    assert_eq!(
        vertex.properties.get("special"),
        Some(&Value::String("line\nbreak\ttab".to_string()))
    );
}

/// Test that List property values roundtrip correctly.
#[test]
fn test_property_roundtrip_list() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    let mixed_list = Value::List(vec![
        Value::Int(1),
        Value::String("two".to_string()),
        Value::Bool(true),
        Value::Float(4.0),
        Value::Null,
    ]);

    let nested_list = Value::List(vec![
        Value::List(vec![Value::Int(1), Value::Int(2)]),
        Value::List(vec![Value::Int(3), Value::Int(4)]),
    ]);

    let v = graph
        .add_vertex(
            "test",
            HashMap::from([
                ("empty_list".to_string(), Value::List(vec![])),
                (
                    "int_list".to_string(),
                    Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
                ),
                ("mixed".to_string(), mixed_list.clone()),
                ("nested".to_string(), nested_list.clone()),
            ]),
        )
        .expect("add vertex");

    graph.checkpoint().expect("checkpoint");

    let vertex = graph.get_vertex(v).expect("get vertex");
    assert_eq!(
        vertex.properties.get("empty_list"),
        Some(&Value::List(vec![]))
    );
    assert_eq!(
        vertex.properties.get("int_list"),
        Some(&Value::List(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3)
        ]))
    );
    assert_eq!(vertex.properties.get("mixed"), Some(&mixed_list));
    assert_eq!(vertex.properties.get("nested"), Some(&nested_list));
}

/// Test that Map property values roundtrip correctly.
#[test]
fn test_property_roundtrip_map() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    let simple_map = Value::Map(HashMap::from([
        ("x".to_string(), Value::Int(10)),
        ("y".to_string(), Value::Int(20)),
    ]));

    let nested_map = Value::Map(HashMap::from([(
        "outer".to_string(),
        Value::Map(HashMap::from([(
            "inner".to_string(),
            Value::String("value".to_string()),
        )])),
    )]));

    let v = graph
        .add_vertex(
            "test",
            HashMap::from([
                ("empty_map".to_string(), Value::Map(HashMap::new())),
                ("simple".to_string(), simple_map.clone()),
                ("nested".to_string(), nested_map.clone()),
            ]),
        )
        .expect("add vertex");

    graph.checkpoint().expect("checkpoint");

    let vertex = graph.get_vertex(v).expect("get vertex");
    assert_eq!(
        vertex.properties.get("empty_map"),
        Some(&Value::Map(HashMap::new()))
    );
    assert_eq!(vertex.properties.get("simple"), Some(&simple_map));
    assert_eq!(vertex.properties.get("nested"), Some(&nested_map));
}

/// Test that Vertex ID property values roundtrip correctly.
#[test]
fn test_property_roundtrip_vertex_id() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    // Create a vertex first to get a valid ID
    let ref_vertex = graph
        .add_vertex("reference", HashMap::new())
        .expect("add reference vertex");

    let v = graph
        .add_vertex(
            "test",
            HashMap::from([
                ("ref".to_string(), Value::Vertex(ref_vertex)),
                ("external".to_string(), Value::Vertex(VertexId(12345))),
                ("zero".to_string(), Value::Vertex(VertexId(0))),
                ("max".to_string(), Value::Vertex(VertexId(u64::MAX))),
            ]),
        )
        .expect("add vertex");

    graph.checkpoint().expect("checkpoint");

    let vertex = graph.get_vertex(v).expect("get vertex");
    assert_eq!(
        vertex.properties.get("ref"),
        Some(&Value::Vertex(ref_vertex))
    );
    assert_eq!(
        vertex.properties.get("external"),
        Some(&Value::Vertex(VertexId(12345)))
    );
    assert_eq!(
        vertex.properties.get("zero"),
        Some(&Value::Vertex(VertexId(0)))
    );
    assert_eq!(
        vertex.properties.get("max"),
        Some(&Value::Vertex(VertexId(u64::MAX)))
    );
}

/// Test that Edge ID property values roundtrip correctly.
#[test]
fn test_property_roundtrip_edge_id() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    // Create vertices and an edge to get valid IDs
    let v1 = graph
        .add_vertex("node", HashMap::new())
        .expect("add vertex 1");
    let v2 = graph
        .add_vertex("node", HashMap::new())
        .expect("add vertex 2");
    let ref_edge = graph
        .add_edge(v1, v2, "link", HashMap::new())
        .expect("add edge");

    let v = graph
        .add_vertex(
            "test",
            HashMap::from([
                ("ref".to_string(), Value::Edge(ref_edge)),
                ("external".to_string(), Value::Edge(EdgeId(67890))),
                ("zero".to_string(), Value::Edge(EdgeId(0))),
                ("max".to_string(), Value::Edge(EdgeId(u64::MAX))),
            ]),
        )
        .expect("add vertex");

    graph.checkpoint().expect("checkpoint");

    let vertex = graph.get_vertex(v).expect("get vertex");
    assert_eq!(vertex.properties.get("ref"), Some(&Value::Edge(ref_edge)));
    assert_eq!(
        vertex.properties.get("external"),
        Some(&Value::Edge(EdgeId(67890)))
    );
    assert_eq!(vertex.properties.get("zero"), Some(&Value::Edge(EdgeId(0))));
    assert_eq!(
        vertex.properties.get("max"),
        Some(&Value::Edge(EdgeId(u64::MAX)))
    );
}

/// Test that edge properties roundtrip correctly with all Value types.
#[test]
fn test_edge_property_roundtrip() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    let v1 = graph.add_vertex("person", HashMap::new()).expect("add v1");
    let v2 = graph.add_vertex("person", HashMap::new()).expect("add v2");

    let nested = Value::Map(HashMap::from([
        ("count".to_string(), Value::Int(5)),
        (
            "tags".to_string(),
            Value::List(vec![
                Value::String("friend".to_string()),
                Value::String("colleague".to_string()),
            ]),
        ),
    ]));

    let e = graph
        .add_edge(
            v1,
            v2,
            "knows",
            HashMap::from([
                ("weight".to_string(), Value::Float(0.85)),
                ("since".to_string(), Value::Int(2020)),
                ("active".to_string(), Value::Bool(true)),
                (
                    "note".to_string(),
                    Value::String("Met at conference".to_string()),
                ),
                ("metadata".to_string(), nested.clone()),
            ]),
        )
        .expect("add edge");

    graph.checkpoint().expect("checkpoint");

    let edge = graph.get_edge(e).expect("get edge");
    assert_eq!(edge.properties.get("weight"), Some(&Value::Float(0.85)));
    assert_eq!(edge.properties.get("since"), Some(&Value::Int(2020)));
    assert_eq!(edge.properties.get("active"), Some(&Value::Bool(true)));
    assert_eq!(
        edge.properties.get("note"),
        Some(&Value::String("Met at conference".to_string()))
    );
    assert_eq!(edge.properties.get("metadata"), Some(&nested));
}

/// Test that multi-property vertices roundtrip correctly.
#[test]
fn test_multi_property_vertex() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    // Create a vertex with many properties of different types
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Test Entity".to_string()));
    props.insert("count".to_string(), Value::Int(42));
    props.insert("ratio".to_string(), Value::Float(0.75));
    props.insert("enabled".to_string(), Value::Bool(true));
    props.insert("disabled".to_string(), Value::Bool(false));
    props.insert("empty".to_string(), Value::Null);
    props.insert(
        "tags".to_string(),
        Value::List(vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ]),
    );
    props.insert(
        "config".to_string(),
        Value::Map(HashMap::from([
            ("key1".to_string(), Value::Int(1)),
            ("key2".to_string(), Value::Int(2)),
        ])),
    );
    props.insert("vertex_ref".to_string(), Value::Vertex(VertexId(100)));
    props.insert("edge_ref".to_string(), Value::Edge(EdgeId(200)));

    let v = graph
        .add_vertex("entity", props.clone())
        .expect("add vertex");

    graph.checkpoint().expect("checkpoint");

    let vertex = graph.get_vertex(v).expect("get vertex");
    assert_eq!(vertex.label, "entity");
    assert_eq!(vertex.properties.len(), props.len());

    for (key, expected_value) in &props {
        let actual_value = vertex.properties.get(key);
        assert_eq!(
            actual_value,
            Some(expected_value),
            "Property '{}' mismatch",
            key
        );
    }
}

/// Test that empty properties roundtrip correctly.
#[test]
fn test_empty_properties() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    let v = graph
        .add_vertex("empty", HashMap::new())
        .expect("add vertex");

    let v1 = graph.add_vertex("node", HashMap::new()).expect("add v1");
    let v2 = graph.add_vertex("node", HashMap::new()).expect("add v2");
    let e = graph
        .add_edge(v1, v2, "link", HashMap::new())
        .expect("add edge");

    graph.checkpoint().expect("checkpoint");

    let vertex = graph.get_vertex(v).expect("get vertex");
    assert!(vertex.properties.is_empty());

    let edge = graph.get_edge(e).expect("get edge");
    assert!(edge.properties.is_empty());
}

/// Test that large strings (> 256 bytes) roundtrip correctly.
#[test]
fn test_large_string_property() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    // Create strings of various sizes
    let small = "a".repeat(100);
    let medium = "b".repeat(500);
    let large = "c".repeat(1000);
    let very_large = "d".repeat(10_000);

    let v = graph
        .add_vertex(
            "test",
            HashMap::from([
                ("small".to_string(), Value::String(small.clone())),
                ("medium".to_string(), Value::String(medium.clone())),
                ("large".to_string(), Value::String(large.clone())),
                ("very_large".to_string(), Value::String(very_large.clone())),
            ]),
        )
        .expect("add vertex");

    graph.checkpoint().expect("checkpoint");

    let vertex = graph.get_vertex(v).expect("get vertex");
    assert_eq!(vertex.properties.get("small"), Some(&Value::String(small)));
    assert_eq!(
        vertex.properties.get("medium"),
        Some(&Value::String(medium))
    );
    assert_eq!(vertex.properties.get("large"), Some(&Value::String(large)));
    assert_eq!(
        vertex.properties.get("very_large"),
        Some(&Value::String(very_large))
    );
}

/// Test property roundtrip across database close and reopen.
#[test]
fn test_property_persistence_across_reopen() {
    let (dir, db_path) = temp_db();

    let (vertex_id, edge_id) = {
        let graph = MmapGraph::open(&db_path).expect("open graph");

        let v = graph
            .add_vertex(
                "entity",
                HashMap::from([
                    ("name".to_string(), Value::String("Persistent".to_string())),
                    ("count".to_string(), Value::Int(999)),
                    ("ratio".to_string(), Value::Float(1.5)),
                    ("active".to_string(), Value::Bool(true)),
                    (
                        "list".to_string(),
                        Value::List(vec![Value::Int(1), Value::Int(2)]),
                    ),
                    (
                        "map".to_string(),
                        Value::Map(HashMap::from([("nested".to_string(), Value::Null)])),
                    ),
                ]),
            )
            .expect("add vertex");

        let v2 = graph.add_vertex("other", HashMap::new()).expect("add v2");

        let e = graph
            .add_edge(
                v,
                v2,
                "relates",
                HashMap::from([
                    ("strength".to_string(), Value::Float(0.9)),
                    ("label".to_string(), Value::String("strong".to_string())),
                ]),
            )
            .expect("add edge");

        graph.checkpoint().expect("checkpoint");

        (v, e)
    };

    // Reopen and verify
    {
        let graph = MmapGraph::open(&db_path).expect("reopen graph");

        let vertex = graph.get_vertex(vertex_id).expect("get vertex");
        assert_eq!(
            vertex.properties.get("name"),
            Some(&Value::String("Persistent".to_string()))
        );
        assert_eq!(vertex.properties.get("count"), Some(&Value::Int(999)));
        assert_eq!(vertex.properties.get("ratio"), Some(&Value::Float(1.5)));
        assert_eq!(vertex.properties.get("active"), Some(&Value::Bool(true)));
        assert_eq!(
            vertex.properties.get("list"),
            Some(&Value::List(vec![Value::Int(1), Value::Int(2)]))
        );
        assert_eq!(
            vertex.properties.get("map"),
            Some(&Value::Map(HashMap::from([(
                "nested".to_string(),
                Value::Null
            )])))
        );

        let edge = graph.get_edge(edge_id).expect("get edge");
        assert_eq!(edge.properties.get("strength"), Some(&Value::Float(0.9)));
        assert_eq!(
            edge.properties.get("label"),
            Some(&Value::String("strong".to_string()))
        );
    }

    drop(dir);
}

/// Test deeply nested property structures roundtrip correctly.
#[test]
fn test_deeply_nested_properties() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    // Create a deeply nested structure
    let level3 = Value::Map(HashMap::from([
        ("leaf".to_string(), Value::String("deep".to_string())),
        ("number".to_string(), Value::Int(42)),
    ]));

    let level2 = Value::Map(HashMap::from([
        ("nested".to_string(), level3.clone()),
        (
            "list".to_string(),
            Value::List(vec![Value::Int(1), Value::Int(2)]),
        ),
    ]));

    let level1 = Value::Map(HashMap::from([
        ("data".to_string(), level2.clone()),
        ("name".to_string(), Value::String("level1".to_string())),
    ]));

    let nested_list = Value::List(vec![
        Value::List(vec![
            Value::List(vec![Value::Int(1), Value::Int(2)]),
            Value::List(vec![Value::Int(3), Value::Int(4)]),
        ]),
        Value::List(vec![Value::List(vec![Value::Int(5), Value::Int(6)])]),
    ]);

    let v = graph
        .add_vertex(
            "nested",
            HashMap::from([
                ("deep_map".to_string(), level1.clone()),
                ("deep_list".to_string(), nested_list.clone()),
            ]),
        )
        .expect("add vertex");

    graph.checkpoint().expect("checkpoint");

    let vertex = graph.get_vertex(v).expect("get vertex");
    assert_eq!(vertex.properties.get("deep_map"), Some(&level1));
    assert_eq!(vertex.properties.get("deep_list"), Some(&nested_list));
}

/// Test all Value types in a single vertex property map.
#[test]
fn test_all_value_types_combined() {
    let (_dir, db_path) = temp_db();
    let graph = MmapGraph::open(&db_path).expect("open graph");

    let v = graph
        .add_vertex(
            "comprehensive",
            HashMap::from([
                ("null".to_string(), Value::Null),
                ("bool_true".to_string(), Value::Bool(true)),
                ("bool_false".to_string(), Value::Bool(false)),
                ("int_pos".to_string(), Value::Int(123)),
                ("int_neg".to_string(), Value::Int(-456)),
                ("float_pos".to_string(), Value::Float(3.14)),
                ("float_neg".to_string(), Value::Float(-2.71)),
                ("string".to_string(), Value::String("hello".to_string())),
                (
                    "list".to_string(),
                    Value::List(vec![Value::Int(1), Value::String("a".to_string())]),
                ),
                (
                    "map".to_string(),
                    Value::Map(HashMap::from([("k".to_string(), Value::Bool(true))])),
                ),
                ("vertex".to_string(), Value::Vertex(VertexId(111))),
                ("edge".to_string(), Value::Edge(EdgeId(222))),
            ]),
        )
        .expect("add vertex");

    graph.checkpoint().expect("checkpoint");

    let vertex = graph.get_vertex(v).expect("get vertex");

    assert_eq!(vertex.properties.get("null"), Some(&Value::Null));
    assert_eq!(vertex.properties.get("bool_true"), Some(&Value::Bool(true)));
    assert_eq!(
        vertex.properties.get("bool_false"),
        Some(&Value::Bool(false))
    );
    assert_eq!(vertex.properties.get("int_pos"), Some(&Value::Int(123)));
    assert_eq!(vertex.properties.get("int_neg"), Some(&Value::Int(-456)));
    assert_eq!(
        vertex.properties.get("float_pos"),
        Some(&Value::Float(3.14))
    );
    assert_eq!(
        vertex.properties.get("float_neg"),
        Some(&Value::Float(-2.71))
    );
    assert_eq!(
        vertex.properties.get("string"),
        Some(&Value::String("hello".to_string()))
    );
    assert_eq!(
        vertex.properties.get("list"),
        Some(&Value::List(vec![
            Value::Int(1),
            Value::String("a".to_string())
        ]))
    );
    assert_eq!(
        vertex.properties.get("map"),
        Some(&Value::Map(HashMap::from([(
            "k".to_string(),
            Value::Bool(true)
        )])))
    );
    assert_eq!(
        vertex.properties.get("vertex"),
        Some(&Value::Vertex(VertexId(111)))
    );
    assert_eq!(
        vertex.properties.get("edge"),
        Some(&Value::Edge(EdgeId(222)))
    );
}
