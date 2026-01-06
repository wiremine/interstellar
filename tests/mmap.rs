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
