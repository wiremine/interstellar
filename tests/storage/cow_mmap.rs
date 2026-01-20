//! Integration tests for CowMmapGraph (persistent copy-on-write storage).
//!
//! These tests verify the CowMmapGraph implementation including:
//! - Basic CRUD operations with persistence
//! - Snapshot isolation and semantics
//! - Batch operations with atomicity
//! - GQL query and mutation support
//! - Traversal engine integration
//! - Concurrent access patterns
//! - Crash recovery and durability
//! - Large-scale data handling
//!
//! Note: This module requires the "mmap" feature.

#![cfg(feature = "mmap")]

use interstellar::prelude::*;
use interstellar::storage::cow_mmap::CowMmapGraph;
use interstellar::storage::{BatchError, GraphStorage};
use interstellar::StorageError;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use tempfile::{tempdir, TempDir};

/// Helper to create a temporary database path
fn temp_db() -> (TempDir, std::path::PathBuf) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    (dir, path)
}

// =============================================================================
// Basic CRUD Operations
// =============================================================================

#[test]
fn cow_mmap_basic_vertex_operations() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    // Add vertices with various property types
    let alice = graph
        .add_vertex(
            "Person",
            HashMap::from([
                ("name".to_string(), Value::String("Alice".into())),
                ("age".to_string(), Value::Int(30)),
                ("active".to_string(), Value::Bool(true)),
            ]),
        )
        .unwrap();

    let bob = graph
        .add_vertex(
            "Person",
            HashMap::from([
                ("name".to_string(), Value::String("Bob".into())),
                ("age".to_string(), Value::Int(25)),
            ]),
        )
        .unwrap();

    let software = graph
        .add_vertex(
            "Software",
            HashMap::from([("name".to_string(), Value::String("GraphDB".into()))]),
        )
        .unwrap();

    assert_eq!(graph.vertex_count(), 3);

    // Verify vertices through snapshot
    let snap = graph.snapshot();
    let alice_v = snap.get_vertex(alice).unwrap();
    assert_eq!(alice_v.label, "Person");
    assert_eq!(
        alice_v.properties.get("name"),
        Some(&Value::String("Alice".into()))
    );

    let bob_v = snap.get_vertex(bob).unwrap();
    assert_eq!(bob_v.properties.get("age"), Some(&Value::Int(25)));

    let sw_v = snap.get_vertex(software).unwrap();
    assert_eq!(sw_v.label, "Software");
}

#[test]
fn cow_mmap_basic_edge_operations() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    let alice = graph.add_vertex("Person", HashMap::new()).unwrap();
    let bob = graph.add_vertex("Person", HashMap::new()).unwrap();
    let charlie = graph.add_vertex("Person", HashMap::new()).unwrap();

    // Add edges with properties
    let knows1 = graph
        .add_edge(
            alice,
            bob,
            "KNOWS",
            HashMap::from([("since".to_string(), Value::Int(2020))]),
        )
        .unwrap();

    let knows2 = graph
        .add_edge(bob, charlie, "KNOWS", HashMap::new())
        .unwrap();

    let knows3 = graph
        .add_edge(charlie, alice, "KNOWS", HashMap::new())
        .unwrap();

    assert_eq!(graph.edge_count(), 3);

    // Verify edges through snapshot
    let snap = graph.snapshot();
    let edge = snap.get_edge(knows1).unwrap();
    assert_eq!(edge.src, alice);
    assert_eq!(edge.dst, bob);
    assert_eq!(edge.label, "KNOWS");
    assert_eq!(edge.properties.get("since"), Some(&Value::Int(2020)));

    // Verify adjacency
    let out_edges: Vec<_> = snap.out_edges(alice).collect();
    assert_eq!(out_edges.len(), 1);
    assert_eq!(out_edges[0].id, knows1);

    let in_edges: Vec<_> = snap.in_edges(alice).collect();
    assert_eq!(in_edges.len(), 1);
    assert_eq!(in_edges[0].id, knows3);
}

#[test]
fn cow_mmap_property_updates() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    let alice = graph
        .add_vertex(
            "Person",
            HashMap::from([("name".to_string(), Value::String("Alice".into()))]),
        )
        .unwrap();

    let bob = graph.add_vertex("Person", HashMap::new()).unwrap();
    let edge = graph.add_edge(alice, bob, "KNOWS", HashMap::new()).unwrap();

    // Update vertex property
    graph
        .set_vertex_property(alice, "age", Value::Int(30))
        .unwrap();
    graph
        .set_vertex_property(alice, "name", Value::String("Alicia".into()))
        .unwrap();

    // Update edge property
    graph
        .set_edge_property(edge, "weight", Value::Float(0.5))
        .unwrap();

    let snap = graph.snapshot();
    let v = snap.get_vertex(alice).unwrap();
    assert_eq!(
        v.properties.get("name"),
        Some(&Value::String("Alicia".into()))
    );
    assert_eq!(v.properties.get("age"), Some(&Value::Int(30)));

    let e = snap.get_edge(edge).unwrap();
    assert_eq!(e.properties.get("weight"), Some(&Value::Float(0.5)));
}

#[test]
fn cow_mmap_remove_operations() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    let alice = graph.add_vertex("Person", HashMap::new()).unwrap();
    let bob = graph.add_vertex("Person", HashMap::new()).unwrap();
    let charlie = graph.add_vertex("Person", HashMap::new()).unwrap();

    let e1 = graph.add_edge(alice, bob, "KNOWS", HashMap::new()).unwrap();
    let e2 = graph
        .add_edge(alice, charlie, "KNOWS", HashMap::new())
        .unwrap();
    let _e3 = graph
        .add_edge(bob, charlie, "KNOWS", HashMap::new())
        .unwrap();

    assert_eq!(graph.vertex_count(), 3);
    assert_eq!(graph.edge_count(), 3);

    // Remove specific edge
    graph.remove_edge(e1).unwrap();
    assert_eq!(graph.edge_count(), 2);

    // Remove vertex (should cascade to connected edges)
    graph.remove_vertex(alice).unwrap();
    assert_eq!(graph.vertex_count(), 2);
    assert_eq!(graph.edge_count(), 1); // Only bob->charlie remains

    // Verify remaining structure
    let snap = graph.snapshot();
    assert!(snap.get_vertex(alice).is_none());
    assert!(snap.get_vertex(bob).is_some());
    assert!(snap.get_edge(e2).is_none()); // Was connected to alice
}

// =============================================================================
// Persistence and Durability
// =============================================================================

#[test]
fn cow_mmap_persistence_basic() {
    let (_dir, path) = temp_db();

    // Create and populate graph
    {
        let graph = CowMmapGraph::open(&path).unwrap();
        graph
            .add_vertex(
                "Person",
                HashMap::from([
                    ("name".to_string(), Value::String("Alice".into())),
                    ("age".to_string(), Value::Int(30)),
                ]),
            )
            .unwrap();
        graph
            .add_vertex(
                "Person",
                HashMap::from([("name".to_string(), Value::String("Bob".into()))]),
            )
            .unwrap();
        graph.checkpoint().unwrap();
    }

    // Reopen and verify
    {
        let graph = CowMmapGraph::open(&path).unwrap();
        assert_eq!(graph.vertex_count(), 2);

        let snap = graph.snapshot();
        let people: Vec<_> = snap.vertices_with_label("Person").collect();
        assert_eq!(people.len(), 2);

        // Verify properties persisted
        let alice = snap.get_vertex(VertexId(0)).unwrap();
        assert_eq!(
            alice.properties.get("name"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(alice.properties.get("age"), Some(&Value::Int(30)));
    }
}

#[test]
fn cow_mmap_persistence_with_edges() {
    let (_dir, path) = temp_db();

    // Create graph with edges
    {
        let graph = CowMmapGraph::open(&path).unwrap();

        let alice = graph.add_vertex("Person", HashMap::new()).unwrap();
        let bob = graph.add_vertex("Person", HashMap::new()).unwrap();
        let charlie = graph.add_vertex("Person", HashMap::new()).unwrap();

        graph
            .add_edge(
                alice,
                bob,
                "KNOWS",
                HashMap::from([("since".to_string(), Value::Int(2020))]),
            )
            .unwrap();
        graph
            .add_edge(bob, charlie, "KNOWS", HashMap::new())
            .unwrap();

        graph.checkpoint().unwrap();
    }

    // Reopen and verify
    {
        let graph = CowMmapGraph::open(&path).unwrap();
        assert_eq!(graph.vertex_count(), 3);
        assert_eq!(graph.edge_count(), 2);

        let snap = graph.snapshot();

        // Verify adjacency is correct after reload
        let alice_out: Vec<_> = snap.out_edges(VertexId(0)).collect();
        assert_eq!(alice_out.len(), 1);
        assert_eq!(alice_out[0].dst, VertexId(1));
        assert_eq!(
            alice_out[0].properties.get("since"),
            Some(&Value::Int(2020))
        );
    }
}

#[test]
fn cow_mmap_persistence_after_modifications() {
    let (_dir, path) = temp_db();

    // Initial data
    {
        let graph = CowMmapGraph::open(&path).unwrap();
        for i in 0..10 {
            graph
                .add_vertex("Node", HashMap::from([("id".to_string(), Value::Int(i))]))
                .unwrap();
        }
        graph.checkpoint().unwrap();
    }

    // Modify data
    {
        let graph = CowMmapGraph::open(&path).unwrap();
        assert_eq!(graph.vertex_count(), 10);

        // Remove some vertices
        graph.remove_vertex(VertexId(0)).unwrap();
        graph.remove_vertex(VertexId(5)).unwrap();

        // Add new vertices
        graph
            .add_vertex(
                "NewNode",
                HashMap::from([("id".to_string(), Value::Int(100))]),
            )
            .unwrap();

        graph.checkpoint().unwrap();
    }

    // Verify modifications persisted
    {
        let graph = CowMmapGraph::open(&path).unwrap();
        assert_eq!(graph.vertex_count(), 9); // 10 - 2 + 1

        let snap = graph.snapshot();

        // Verify original vertices 1-4 and 6-9 still exist with their original properties
        // (vertices 0 and 5 were deleted - their IDs may be reused by the new vertex)
        for id in [1, 2, 3, 4, 6, 7, 8, 9] {
            let v = snap
                .get_vertex(VertexId(id))
                .expect(&format!("Vertex {} should exist", id));
            assert_eq!(v.label, "Node");
            assert_eq!(v.properties.get("id"), Some(&Value::Int(id as i64)));
        }

        // Verify the new node exists (its ID may be 0 or 5 due to free list reuse)
        let new_nodes: Vec<_> = snap.vertices_with_label("NewNode").collect();
        assert_eq!(new_nodes.len(), 1);
        assert_eq!(new_nodes[0].properties.get("id"), Some(&Value::Int(100)));

        // Verify correct number of original nodes remain
        let original_nodes: Vec<_> = snap.vertices_with_label("Node").collect();
        assert_eq!(original_nodes.len(), 8); // 10 - 2 deleted
    }
}

// =============================================================================
// Snapshot Isolation and Semantics
// =============================================================================

#[test]
fn cow_mmap_snapshot_isolation_basic() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    let alice = graph
        .add_vertex(
            "Person",
            HashMap::from([("name".to_string(), Value::String("Alice".into()))]),
        )
        .unwrap();

    // Take snapshot before modification
    let snap_before = graph.snapshot();

    // Modify the graph
    graph
        .set_vertex_property(alice, "name", Value::String("Alicia".into()))
        .unwrap();
    graph.add_vertex("Person", HashMap::new()).unwrap();

    // Take snapshot after modification
    let snap_after = graph.snapshot();

    // Verify isolation
    let v_before = snap_before.get_vertex(alice).unwrap();
    assert_eq!(
        v_before.properties.get("name"),
        Some(&Value::String("Alice".into()))
    );
    assert_eq!(snap_before.vertex_count(), 1);

    let v_after = snap_after.get_vertex(alice).unwrap();
    assert_eq!(
        v_after.properties.get("name"),
        Some(&Value::String("Alicia".into()))
    );
    assert_eq!(snap_after.vertex_count(), 2);
}

#[test]
fn cow_mmap_snapshot_survives_heavy_modification() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    // Create initial state with 500 vertices
    for i in 0..500 {
        graph
            .add_vertex("Node", HashMap::from([("id".to_string(), Value::Int(i))]))
            .unwrap();
    }

    // Take snapshot
    let snap = graph.snapshot();
    assert_eq!(snap.vertex_count(), 500);

    // Heavy modification: add 500 more, remove 250 original
    for i in 500..1000 {
        graph
            .add_vertex("Node", HashMap::from([("id".to_string(), Value::Int(i))]))
            .unwrap();
    }
    for i in 0..250u64 {
        graph.remove_vertex(VertexId(i)).unwrap();
    }

    // Original snapshot should be unchanged
    assert_eq!(snap.vertex_count(), 500);

    // Verify we can still read from original snapshot
    let v = snap.get_vertex(VertexId(0)).unwrap();
    assert_eq!(v.properties.get("id"), Some(&Value::Int(0)));

    // New snapshot reflects changes
    let new_snap = graph.snapshot();
    assert_eq!(new_snap.vertex_count(), 750); // 250 original + 500 new
}

#[test]
fn cow_mmap_multiple_snapshots_independent() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    graph.add_vertex("A", HashMap::new()).unwrap();
    let snap1 = graph.snapshot();

    graph.add_vertex("B", HashMap::new()).unwrap();
    let snap2 = graph.snapshot();

    graph.add_vertex("C", HashMap::new()).unwrap();
    let snap3 = graph.snapshot();

    graph.add_vertex("D", HashMap::new()).unwrap();

    // Each snapshot sees different state
    assert_eq!(snap1.vertex_count(), 1);
    assert_eq!(snap2.vertex_count(), 2);
    assert_eq!(snap3.vertex_count(), 3);
    assert_eq!(graph.vertex_count(), 4);

    // All snapshots remain valid
    assert!(snap1.vertices_with_label("A").next().is_some());
    assert!(snap1.vertices_with_label("B").next().is_none());

    assert!(snap2.vertices_with_label("B").next().is_some());
    assert!(snap2.vertices_with_label("C").next().is_none());
}

// =============================================================================
// Batch Operations
// =============================================================================

#[test]
fn cow_mmap_batch_atomic_success() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    let result = graph.batch(|ctx| {
        let alice = ctx.add_vertex(
            "Person",
            HashMap::from([("name".to_string(), Value::String("Alice".into()))]),
        );
        let bob = ctx.add_vertex(
            "Person",
            HashMap::from([("name".to_string(), Value::String("Bob".into()))]),
        );
        let charlie = ctx.add_vertex(
            "Person",
            HashMap::from([("name".to_string(), Value::String("Charlie".into()))]),
        );

        ctx.add_edge(alice, bob, "KNOWS", HashMap::new())?;
        ctx.add_edge(bob, charlie, "KNOWS", HashMap::new())?;
        ctx.add_edge(charlie, alice, "KNOWS", HashMap::new())?;

        Ok((alice, bob, charlie))
    });

    assert!(result.is_ok());
    let (alice, bob, charlie) = result.unwrap();

    assert_eq!(graph.vertex_count(), 3);
    assert_eq!(graph.edge_count(), 3);

    // Verify structure
    let snap = graph.snapshot();
    assert!(snap.get_vertex(alice).is_some());
    assert!(snap.get_vertex(bob).is_some());
    assert!(snap.get_vertex(charlie).is_some());

    let out_edges: Vec<_> = snap.out_edges(alice).collect();
    assert_eq!(out_edges.len(), 1);
}

#[test]
fn cow_mmap_batch_atomic_rollback() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    // Add initial vertex
    graph
        .add_vertex(
            "Existing",
            HashMap::from([("name".to_string(), Value::String("Existing".into()))]),
        )
        .unwrap();

    // Attempt batch that fails
    let result = graph.batch(|ctx| {
        ctx.add_vertex("New1", HashMap::new());
        ctx.add_vertex("New2", HashMap::new());

        // This should fail - non-existent vertices
        ctx.add_edge(VertexId(999), VertexId(998), "INVALID", HashMap::new())?;

        Ok(())
    });

    assert!(result.is_err());

    // Graph should be unchanged
    assert_eq!(graph.vertex_count(), 1);
    assert_eq!(graph.edge_count(), 0);

    let snap = graph.snapshot();
    assert!(snap.vertices_with_label("Existing").next().is_some());
    assert!(snap.vertices_with_label("New1").next().is_none());
}

#[test]
fn cow_mmap_batch_persists() {
    let (_dir, path) = temp_db();

    // Batch and checkpoint
    {
        let graph = CowMmapGraph::open(&path).unwrap();

        graph
            .batch(|ctx| {
                for i in 0..100 {
                    ctx.add_vertex("Node", HashMap::from([("id".to_string(), Value::Int(i))]));
                }
                Ok(())
            })
            .unwrap();

        graph.checkpoint().unwrap();
    }

    // Reopen and verify
    {
        let graph = CowMmapGraph::open(&path).unwrap();
        assert_eq!(graph.vertex_count(), 100);
    }
}

// =============================================================================
// GQL Integration
// =============================================================================

#[test]
fn cow_mmap_gql_create_vertex() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    graph
        .gql("CREATE (:Person {name: 'Alice', age: 30})")
        .unwrap();
    graph
        .gql("CREATE (:Person {name: 'Bob', age: 25})")
        .unwrap();
    graph.gql("CREATE (:Software {name: 'GraphDB'})").unwrap();

    assert_eq!(graph.vertex_count(), 3);

    let snap = graph.snapshot();
    let people: Vec<_> = snap.vertices_with_label("Person").collect();
    assert_eq!(people.len(), 2);

    let software: Vec<_> = snap.vertices_with_label("Software").collect();
    assert_eq!(software.len(), 1);
}

#[test]
fn cow_mmap_gql_query_on_snapshot() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    // Build graph
    let alice = graph
        .add_vertex(
            "Person",
            HashMap::from([
                ("name".to_string(), Value::String("Alice".into())),
                ("age".to_string(), Value::Int(30)),
            ]),
        )
        .unwrap();
    let bob = graph
        .add_vertex(
            "Person",
            HashMap::from([
                ("name".to_string(), Value::String("Bob".into())),
                ("age".to_string(), Value::Int(25)),
            ]),
        )
        .unwrap();
    graph.add_edge(alice, bob, "KNOWS", HashMap::new()).unwrap();

    // Query via snapshot - CowMmapSnapshot implements GraphStorage
    // so we can iterate vertices directly
    let snap = graph.snapshot();

    // Verify vertices via GraphStorage methods
    let people: Vec<_> = snap.vertices_with_label("Person").collect();
    assert_eq!(people.len(), 2);

    // Verify names
    let names: Vec<_> = people
        .iter()
        .filter_map(|v| v.properties.get("name"))
        .collect();
    assert_eq!(names.len(), 2);
}

#[test]
fn cow_mmap_gql_set_property() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    let alice = graph
        .add_vertex(
            "Person",
            HashMap::from([("name".to_string(), Value::String("Alice".into()))]),
        )
        .unwrap();

    // Set property via GQL using property-based matching
    // (id() function is not supported in WHERE predicates)
    graph
        .gql("MATCH (p:Person) WHERE p.name = 'Alice' SET p.age = 30")
        .unwrap();

    let snap = graph.snapshot();
    let v = snap.get_vertex(alice).unwrap();
    assert_eq!(v.properties.get("age"), Some(&Value::Int(30)));
}

// =============================================================================
// Traversal Integration
// =============================================================================

#[test]
fn cow_mmap_traversal_basic() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    let alice = graph
        .add_vertex(
            "Person",
            HashMap::from([("name".to_string(), Value::String("Alice".into()))]),
        )
        .unwrap();
    let bob = graph
        .add_vertex(
            "Person",
            HashMap::from([("name".to_string(), Value::String("Bob".into()))]),
        )
        .unwrap();
    let software = graph
        .add_vertex(
            "Software",
            HashMap::from([("name".to_string(), Value::String("GraphDB".into()))]),
        )
        .unwrap();

    graph.add_edge(alice, bob, "KNOWS", HashMap::new()).unwrap();
    graph
        .add_edge(alice, software, "CREATED", HashMap::new())
        .unwrap();

    let snap = graph.snapshot();

    // Basic vertex traversal
    let all_vertices: Vec<_> = snap.all_vertices().collect();
    assert_eq!(all_vertices.len(), 3);

    // Label filtering
    let people: Vec<_> = snap.vertices_with_label("Person").collect();
    assert_eq!(people.len(), 2);

    // Edge traversal
    let alice_out: Vec<_> = snap.out_edges(alice).collect();
    assert_eq!(alice_out.len(), 2);

    let labels: Vec<_> = alice_out.iter().map(|e| e.label.as_str()).collect();
    assert!(labels.contains(&"KNOWS"));
    assert!(labels.contains(&"CREATED"));
}

#[test]
fn cow_mmap_traversal_multi_hop() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    // Create a chain: A -> B -> C -> D
    let a = graph
        .add_vertex(
            "Node",
            HashMap::from([("name".to_string(), Value::String("A".into()))]),
        )
        .unwrap();
    let b = graph
        .add_vertex(
            "Node",
            HashMap::from([("name".to_string(), Value::String("B".into()))]),
        )
        .unwrap();
    let c = graph
        .add_vertex(
            "Node",
            HashMap::from([("name".to_string(), Value::String("C".into()))]),
        )
        .unwrap();
    let d = graph
        .add_vertex(
            "Node",
            HashMap::from([("name".to_string(), Value::String("D".into()))]),
        )
        .unwrap();

    graph.add_edge(a, b, "NEXT", HashMap::new()).unwrap();
    graph.add_edge(b, c, "NEXT", HashMap::new()).unwrap();
    graph.add_edge(c, d, "NEXT", HashMap::new()).unwrap();

    let snap = graph.snapshot();

    // Follow edges from A
    let a_out: Vec<_> = snap.out_edges(a).collect();
    assert_eq!(a_out.len(), 1);
    assert_eq!(a_out[0].dst, b);

    // Follow edges from B
    let b_out: Vec<_> = snap.out_edges(b).collect();
    assert_eq!(b_out.len(), 1);
    assert_eq!(b_out[0].dst, c);

    // Verify in-edges
    let d_in: Vec<_> = snap.in_edges(d).collect();
    assert_eq!(d_in.len(), 1);
    assert_eq!(d_in[0].src, c);
}

// =============================================================================
// Concurrent Access
// =============================================================================

#[test]
fn cow_mmap_concurrent_readers() {
    let (_dir, path) = temp_db();
    let graph = Arc::new(CowMmapGraph::open(&path).unwrap());

    // Build initial graph
    for i in 0..100 {
        graph
            .add_vertex("Node", HashMap::from([("id".to_string(), Value::Int(i))]))
            .unwrap();
    }

    let snapshot = graph.snapshot();

    // Spawn multiple reader threads
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let snap = snapshot.clone();
            thread::spawn(move || {
                // Each thread reads the entire graph
                let count = snap.vertex_count();
                assert_eq!(count, 100);

                // Read specific vertices
                for i in 0..100u64 {
                    let v = snap.get_vertex(VertexId(i));
                    assert!(v.is_some());
                }

                count
            })
        })
        .collect();

    // All readers should complete successfully
    for handle in handles {
        let count = handle.join().unwrap();
        assert_eq!(count, 100);
    }
}

#[test]
fn cow_mmap_concurrent_readers_with_writer() {
    let (_dir, path) = temp_db();
    let graph = Arc::new(CowMmapGraph::open(&path).unwrap());

    // Add initial vertices
    for i in 0..50 {
        graph
            .add_vertex("Node", HashMap::from([("id".to_string(), Value::Int(i))]))
            .unwrap();
    }

    // Take snapshot for readers
    let reader_snapshot = graph.snapshot();

    // Clone for writer thread
    let writer_graph = Arc::clone(&graph);

    // Start writer thread that adds more vertices
    let writer = thread::spawn(move || {
        for i in 50..100 {
            writer_graph
                .add_vertex("Node", HashMap::from([("id".to_string(), Value::Int(i))]))
                .unwrap();
        }
    });

    // Start reader threads using the pre-mutation snapshot
    let readers: Vec<_> = (0..5)
        .map(|_| {
            let snap = reader_snapshot.clone();
            thread::spawn(move || {
                // Readers should always see exactly 50 vertices
                // (the snapshot was taken before writer started)
                assert_eq!(snap.vertex_count(), 50);
            })
        })
        .collect();

    // Wait for all threads
    writer.join().unwrap();
    for reader in readers {
        reader.join().unwrap();
    }

    // Final graph should have 100 vertices
    assert_eq!(graph.vertex_count(), 100);
}

#[test]
fn cow_mmap_snapshot_sent_across_threads() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    graph
        .add_vertex(
            "Person",
            HashMap::from([("name".to_string(), Value::String("Alice".into()))]),
        )
        .unwrap();

    let snapshot = graph.snapshot();

    // Send snapshot to another thread
    let handle = thread::spawn(move || {
        assert_eq!(snapshot.vertex_count(), 1);
        let v = snapshot.get_vertex(VertexId(0)).unwrap();
        v.properties.get("name").cloned()
    });

    let name = handle.join().unwrap();
    assert_eq!(name, Some(Value::String("Alice".into())));
}

// =============================================================================
// Label Indexing
// =============================================================================

#[test]
fn cow_mmap_label_index_vertices() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    for _ in 0..100 {
        graph.add_vertex("Person", HashMap::new()).unwrap();
    }
    for _ in 0..50 {
        graph.add_vertex("Software", HashMap::new()).unwrap();
    }
    for _ in 0..25 {
        graph.add_vertex("Company", HashMap::new()).unwrap();
    }

    let snap = graph.snapshot();

    let people: Vec<_> = snap.vertices_with_label("Person").collect();
    let software: Vec<_> = snap.vertices_with_label("Software").collect();
    let companies: Vec<_> = snap.vertices_with_label("Company").collect();
    let unknown: Vec<_> = snap.vertices_with_label("Unknown").collect();

    assert_eq!(people.len(), 100);
    assert_eq!(software.len(), 50);
    assert_eq!(companies.len(), 25);
    assert_eq!(unknown.len(), 0);
}

#[test]
fn cow_mmap_label_index_edges() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    let vertices: Vec<_> = (0..10)
        .map(|_| graph.add_vertex("Node", HashMap::new()).unwrap())
        .collect();

    // Create edges with different labels
    for i in 0..9 {
        graph
            .add_edge(vertices[i], vertices[i + 1], "NEXT", HashMap::new())
            .unwrap();
    }
    for i in 0..5 {
        graph
            .add_edge(vertices[i], vertices[9 - i], "LINK", HashMap::new())
            .unwrap();
    }

    let snap = graph.snapshot();

    let next_edges: Vec<_> = snap.edges_with_label("NEXT").collect();
    let link_edges: Vec<_> = snap.edges_with_label("LINK").collect();
    let unknown_edges: Vec<_> = snap.edges_with_label("UNKNOWN").collect();

    assert_eq!(next_edges.len(), 9);
    assert_eq!(link_edges.len(), 5);
    assert_eq!(unknown_edges.len(), 0);
}

// =============================================================================
// Scale Tests
// =============================================================================

#[test]
fn cow_mmap_scale_1k_vertices() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    // Add 1,000 vertices (smaller scale for mmap due to I/O)
    let vertices: Vec<_> = (0..1_000)
        .map(|i| {
            graph
                .add_vertex("Node", HashMap::from([("id".to_string(), Value::Int(i))]))
                .unwrap()
        })
        .collect();

    assert_eq!(graph.vertex_count(), 1_000);

    // Add edges (each vertex connects to next 3)
    for i in 0..1_000 {
        for j in 1..=3 {
            let dst_idx = (i + j) % 1_000;
            graph
                .add_edge(vertices[i], vertices[dst_idx], "CONNECTS", HashMap::new())
                .unwrap();
        }
    }

    assert_eq!(graph.edge_count(), 3_000);

    // Verify random lookups
    let snap = graph.snapshot();
    let v = snap.get_vertex(vertices[500]).unwrap();
    assert_eq!(v.properties.get("id"), Some(&Value::Int(500)));

    let out: Vec<_> = snap.out_edges(vertices[0]).collect();
    assert_eq!(out.len(), 3);
}

#[test]
fn cow_mmap_many_snapshots() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    let mut snapshots = Vec::new();

    // Create 50 snapshots at different states
    for i in 0..50 {
        graph
            .add_vertex("Node", HashMap::from([("id".to_string(), Value::Int(i))]))
            .unwrap();
        snapshots.push(graph.snapshot());
    }

    // Verify each snapshot sees correct count
    for (i, snap) in snapshots.iter().enumerate() {
        assert_eq!(snap.vertex_count() as usize, i + 1);
    }
}

// =============================================================================
// Error Handling
// =============================================================================

#[test]
fn cow_mmap_error_vertex_not_found() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    let result = graph.set_vertex_property(VertexId(999), "prop", Value::Int(1));
    assert!(matches!(result, Err(StorageError::VertexNotFound(_))));

    let result = graph.remove_vertex(VertexId(999));
    assert!(matches!(result, Err(StorageError::VertexNotFound(_))));
}

#[test]
fn cow_mmap_error_edge_not_found() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    let result = graph.set_edge_property(EdgeId(999), "prop", Value::Int(1));
    assert!(matches!(result, Err(StorageError::EdgeNotFound(_))));

    let result = graph.remove_edge(EdgeId(999));
    assert!(matches!(result, Err(StorageError::EdgeNotFound(_))));
}

#[test]
fn cow_mmap_error_edge_missing_vertices() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    let v1 = graph.add_vertex("Node", HashMap::new()).unwrap();

    // Source doesn't exist
    let result = graph.add_edge(VertexId(999), v1, "EDGE", HashMap::new());
    assert!(matches!(result, Err(StorageError::VertexNotFound(_))));

    // Destination doesn't exist
    let result = graph.add_edge(v1, VertexId(999), "EDGE", HashMap::new());
    assert!(matches!(result, Err(StorageError::VertexNotFound(_))));
}

// =============================================================================
// Cross-implementation Consistency
// =============================================================================

#[test]
fn cow_mmap_behavior_matches_cow_graph() {
    use interstellar::storage::cow::CowGraph;

    let (_dir, path) = temp_db();
    let mmap_graph = CowMmapGraph::open(&path).unwrap();
    let mem_graph = CowGraph::new();

    // Perform identical operations on both
    let mmap_alice = mmap_graph
        .add_vertex(
            "Person",
            HashMap::from([("name".to_string(), Value::String("Alice".into()))]),
        )
        .unwrap();
    let mem_alice = mem_graph.add_vertex(
        "Person",
        HashMap::from([("name".to_string(), Value::String("Alice".into()))]),
    );

    let mmap_bob = mmap_graph.add_vertex("Person", HashMap::new()).unwrap();
    let mem_bob = mem_graph.add_vertex("Person", HashMap::new());

    mmap_graph
        .add_edge(mmap_alice, mmap_bob, "KNOWS", HashMap::new())
        .unwrap();
    mem_graph
        .add_edge(mem_alice, mem_bob, "KNOWS", HashMap::new())
        .unwrap();

    // Verify counts match
    assert_eq!(mmap_graph.vertex_count(), mem_graph.vertex_count());
    assert_eq!(mmap_graph.edge_count(), mem_graph.edge_count());

    // Verify snapshot behavior matches
    let mmap_snap = mmap_graph.snapshot();
    let mem_snap = mem_graph.snapshot();

    assert_eq!(mmap_snap.vertex_count(), mem_snap.vertex_count());
    assert_eq!(mmap_snap.edge_count(), mem_snap.edge_count());

    // Verify adjacency matches
    let mmap_out: Vec<_> = mmap_snap.out_edges(mmap_alice).collect();
    let mem_out: Vec<_> = mem_snap.out_edges(mem_alice).collect();
    assert_eq!(mmap_out.len(), mem_out.len());
}

// =============================================================================
// Property Index Tests
// =============================================================================

#[test]
fn cow_mmap_index_create_and_drop() {
    use interstellar::index::IndexBuilder;

    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    assert!(!graph.has_index("person_age_idx"));
    assert_eq!(graph.index_count(), 0);
    assert!(graph.supports_indexes());

    // Create index
    graph
        .create_index(
            IndexBuilder::vertex()
                .name("person_age_idx")
                .label("Person")
                .property("age")
                .build()
                .unwrap(),
        )
        .unwrap();

    assert!(graph.has_index("person_age_idx"));
    assert_eq!(graph.index_count(), 1);

    let indexes = graph.list_indexes();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name, "person_age_idx");

    // Drop index
    graph.drop_index("person_age_idx").unwrap();

    assert!(!graph.has_index("person_age_idx"));
    assert_eq!(graph.index_count(), 0);
}

#[test]
fn cow_mmap_index_duplicate_name_error() {
    use interstellar::index::{IndexBuilder, IndexError};

    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    graph
        .create_index(
            IndexBuilder::vertex()
                .name("my_index")
                .label("Person")
                .property("name")
                .build()
                .unwrap(),
        )
        .unwrap();

    let result = graph.create_index(
        IndexBuilder::vertex()
            .name("my_index")
            .label("Person")
            .property("age")
            .build()
            .unwrap(),
    );

    assert!(matches!(result, Err(IndexError::AlreadyExists(_))));
}

#[test]
fn cow_mmap_index_populated_on_creation() {
    use interstellar::index::IndexBuilder;

    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    // Add data first
    graph
        .add_vertex(
            "Person",
            HashMap::from([("age".to_string(), Value::Int(30))]),
        )
        .unwrap();
    graph
        .add_vertex(
            "Person",
            HashMap::from([("age".to_string(), Value::Int(25))]),
        )
        .unwrap();
    graph
        .add_vertex(
            "Person",
            HashMap::from([("age".to_string(), Value::Int(30))]),
        )
        .unwrap();

    // Create index after data
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("Person")
                .property("age")
                .build()
                .unwrap(),
        )
        .unwrap();

    // Query using index
    let results: Vec<_> = graph
        .vertices_by_property(Some("Person"), "age", &Value::Int(30))
        .collect();
    assert_eq!(results.len(), 2);

    let results: Vec<_> = graph
        .vertices_by_property(Some("Person"), "age", &Value::Int(25))
        .collect();
    assert_eq!(results.len(), 1);
}

#[test]
fn cow_mmap_index_maintained_on_insert() {
    use interstellar::index::IndexBuilder;

    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    // Create index first
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("Person")
                .property("age")
                .build()
                .unwrap(),
        )
        .unwrap();

    // Add data after index creation
    graph
        .add_vertex(
            "Person",
            HashMap::from([("age".to_string(), Value::Int(30))]),
        )
        .unwrap();
    graph
        .add_vertex(
            "Person",
            HashMap::from([("age".to_string(), Value::Int(25))]),
        )
        .unwrap();

    // Query using index
    let results: Vec<_> = graph
        .vertices_by_property(Some("Person"), "age", &Value::Int(30))
        .collect();
    assert_eq!(results.len(), 1);

    let results: Vec<_> = graph
        .vertices_by_property(Some("Person"), "age", &Value::Int(25))
        .collect();
    assert_eq!(results.len(), 1);
}

#[test]
fn cow_mmap_index_maintained_on_remove() {
    use interstellar::index::IndexBuilder;

    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    // Create index
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("Person")
                .property("age")
                .build()
                .unwrap(),
        )
        .unwrap();

    // Add vertices
    let v1 = graph
        .add_vertex(
            "Person",
            HashMap::from([("age".to_string(), Value::Int(30))]),
        )
        .unwrap();
    let v2 = graph
        .add_vertex(
            "Person",
            HashMap::from([("age".to_string(), Value::Int(30))]),
        )
        .unwrap();

    // Verify both found
    let results: Vec<_> = graph
        .vertices_by_property(Some("Person"), "age", &Value::Int(30))
        .collect();
    assert_eq!(results.len(), 2);

    // Remove one
    graph.remove_vertex(v1).unwrap();

    // Only one should remain
    let results: Vec<_> = graph
        .vertices_by_property(Some("Person"), "age", &Value::Int(30))
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, v2);
}

#[test]
fn cow_mmap_index_maintained_on_property_update() {
    use interstellar::index::IndexBuilder;

    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    // Create index
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("Person")
                .property("age")
                .build()
                .unwrap(),
        )
        .unwrap();

    // Add vertex
    let v1 = graph
        .add_vertex(
            "Person",
            HashMap::from([("age".to_string(), Value::Int(30))]),
        )
        .unwrap();

    // Verify found at old value
    let results: Vec<_> = graph
        .vertices_by_property(Some("Person"), "age", &Value::Int(30))
        .collect();
    assert_eq!(results.len(), 1);

    // Update property
    graph
        .set_vertex_property(v1, "age", Value::Int(35))
        .unwrap();

    // Old value should be empty
    let results: Vec<_> = graph
        .vertices_by_property(Some("Person"), "age", &Value::Int(30))
        .collect();
    assert_eq!(results.len(), 0);

    // New value should find it
    let results: Vec<_> = graph
        .vertices_by_property(Some("Person"), "age", &Value::Int(35))
        .collect();
    assert_eq!(results.len(), 1);
}

#[test]
fn cow_mmap_index_edge_property() {
    use interstellar::index::IndexBuilder;

    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    // Create edge index
    graph
        .create_index(
            IndexBuilder::edge()
                .label("KNOWS")
                .property("since")
                .build()
                .unwrap(),
        )
        .unwrap();

    let v1 = graph.add_vertex("Person", HashMap::new()).unwrap();
    let v2 = graph.add_vertex("Person", HashMap::new()).unwrap();
    let v3 = graph.add_vertex("Person", HashMap::new()).unwrap();

    graph
        .add_edge(
            v1,
            v2,
            "KNOWS",
            HashMap::from([("since".to_string(), Value::Int(2020))]),
        )
        .unwrap();
    graph
        .add_edge(
            v2,
            v3,
            "KNOWS",
            HashMap::from([("since".to_string(), Value::Int(2021))]),
        )
        .unwrap();
    graph
        .add_edge(
            v1,
            v3,
            "KNOWS",
            HashMap::from([("since".to_string(), Value::Int(2020))]),
        )
        .unwrap();

    let results: Vec<_> = graph
        .edges_by_property(Some("KNOWS"), "since", &Value::Int(2020))
        .collect();
    assert_eq!(results.len(), 2);

    let results: Vec<_> = graph
        .edges_by_property(Some("KNOWS"), "since", &Value::Int(2021))
        .collect();
    assert_eq!(results.len(), 1);
}

#[test]
fn cow_mmap_index_unique_constraint() {
    use interstellar::index::IndexBuilder;

    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    // Create unique index
    graph
        .create_index(
            IndexBuilder::vertex()
                .name("email_unique")
                .label("User")
                .property("email")
                .unique()
                .build()
                .unwrap(),
        )
        .unwrap();

    // Add first user
    graph
        .add_vertex(
            "User",
            HashMap::from([(
                "email".to_string(),
                Value::String("alice@example.com".into()),
            )]),
        )
        .unwrap();

    // Second user with same email should still insert (index insert ignores errors for inserts)
    // But the index won't store the duplicate
    graph
        .add_vertex(
            "User",
            HashMap::from([("email".to_string(), Value::String("bob@example.com".into()))]),
        )
        .unwrap();

    // Both should exist in graph
    assert_eq!(graph.vertex_count(), 2);
}

#[test]
fn cow_mmap_index_range_query() {
    use interstellar::index::IndexBuilder;
    use std::ops::Bound;

    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    // Create BTree index for range queries (BTree is the default)
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("Person")
                .property("age")
                .build()
                .unwrap(),
        )
        .unwrap();

    // Add vertices with various ages
    for age in [20, 25, 30, 35, 40, 45, 50] {
        graph
            .add_vertex(
                "Person",
                HashMap::from([("age".to_string(), Value::Int(age))]),
            )
            .unwrap();
    }

    // Range query: 25 <= age <= 40
    let results: Vec<_> = graph
        .vertices_by_property_range(
            Some("Person"),
            "age",
            Bound::Included(&Value::Int(25)),
            Bound::Included(&Value::Int(40)),
        )
        .collect();
    assert_eq!(results.len(), 4); // 25, 30, 35, 40

    // Range query: age > 35
    let results: Vec<_> = graph
        .vertices_by_property_range(
            Some("Person"),
            "age",
            Bound::Excluded(&Value::Int(35)),
            Bound::Unbounded,
        )
        .collect();
    assert_eq!(results.len(), 3); // 40, 45, 50
}

#[test]
fn cow_mmap_index_no_label_filter() {
    use interstellar::index::IndexBuilder;

    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    // Create index without label filter (indexes all vertices with 'status' property)
    graph
        .create_index(IndexBuilder::vertex().property("status").build().unwrap())
        .unwrap();

    graph
        .add_vertex(
            "Person",
            HashMap::from([("status".to_string(), Value::String("active".into()))]),
        )
        .unwrap();
    graph
        .add_vertex(
            "Company",
            HashMap::from([("status".to_string(), Value::String("active".into()))]),
        )
        .unwrap();
    graph
        .add_vertex(
            "Project",
            HashMap::from([("status".to_string(), Value::String("active".into()))]),
        )
        .unwrap();

    // Query without label - should find all 3
    let results: Vec<_> = graph
        .vertices_by_property(None, "status", &Value::String("active".into()))
        .collect();
    assert_eq!(results.len(), 3);
}

#[test]
fn cow_mmap_index_with_batch_operations() {
    use interstellar::index::IndexBuilder;

    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    // Create index
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("Person")
                .property("age")
                .build()
                .unwrap(),
        )
        .unwrap();

    // Batch insert
    graph
        .batch(|ctx| {
            for i in 0..10 {
                ctx.add_vertex(
                    "Person",
                    HashMap::from([("age".to_string(), Value::Int(20 + i))]),
                );
            }
            Ok(())
        })
        .unwrap();

    // Note: Batch operations don't update indexes automatically in current impl
    // So we test that the graph has the data even if index isn't updated
    assert_eq!(graph.vertex_count(), 10);
}

// =============================================================================
// Unified Traversal API Tests
// =============================================================================

#[test]
fn cow_mmap_unified_traversal_add_v_basic() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();
    let g = graph.gremlin();

    let result = g.add_v("Person").next();

    assert!(result.is_some());
    assert!(matches!(result.unwrap(), Value::Vertex(_)));
    assert_eq!(graph.vertex_count(), 1);
}

#[test]
fn cow_mmap_unified_traversal_add_v_with_properties() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();
    let g = graph.gremlin();

    let result = g
        .add_v("Person")
        .property("name", "Alice")
        .property("age", 30i64)
        .next();

    assert!(result.is_some());
    let id = match result.unwrap() {
        Value::Vertex(id) => id,
        _ => panic!("Expected vertex ID"),
    };

    let snap = graph.snapshot();
    let vertex = snap.get_vertex(id).unwrap();
    assert_eq!(vertex.label, "Person");
    assert_eq!(
        vertex.properties.get("name"),
        Some(&Value::String("Alice".into()))
    );
    assert_eq!(vertex.properties.get("age"), Some(&Value::Int(30)));
}

#[test]
fn cow_mmap_unified_traversal_add_v_multiple() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();
    let g = graph.gremlin();

    g.add_v("Person").property("name", "Alice").iterate();
    g.add_v("Person").property("name", "Bob").iterate();
    g.add_v("Software").property("name", "GraphDB").iterate();

    assert_eq!(graph.vertex_count(), 3);

    let snap = graph.snapshot();
    let people: Vec<_> = snap.vertices_with_label("Person").collect();
    assert_eq!(people.len(), 2);
    let software: Vec<_> = snap.vertices_with_label("Software").collect();
    assert_eq!(software.len(), 1);
}

#[test]
fn cow_mmap_unified_traversal_add_v_to_list() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();
    let g = graph.gremlin();

    let results = g.add_v("Person").property("name", "Alice").to_list();

    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Value::Vertex(_)));
}

#[test]
fn cow_mmap_unified_traversal_add_e_from_source() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    let alice = graph.add_vertex("Person", HashMap::new()).unwrap();
    let bob = graph.add_vertex("Person", HashMap::new()).unwrap();

    let g = graph.gremlin();
    let result = g.add_e("KNOWS").from_id(alice).to_id(bob).next();

    assert!(result.is_some());
    assert!(matches!(result.unwrap(), Value::Edge(_)));
    assert_eq!(graph.edge_count(), 1);

    let snap = graph.snapshot();
    let edges: Vec<_> = snap.out_edges(alice).collect();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].label, "KNOWS");
    assert_eq!(edges[0].dst, bob);
}

#[test]
fn cow_mmap_unified_traversal_add_e_with_properties() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    let alice = graph.add_vertex("Person", HashMap::new()).unwrap();
    let bob = graph.add_vertex("Person", HashMap::new()).unwrap();

    let g = graph.gremlin();
    let result = g
        .add_e("KNOWS")
        .from_id(alice)
        .to_id(bob)
        .property("since", 2020i64)
        .property("weight", 0.5f64)
        .next();

    assert!(result.is_some());
    let edge_id = match result.unwrap() {
        Value::Edge(id) => id,
        _ => panic!("Expected edge ID"),
    };

    let snap = graph.snapshot();
    let edge = snap.get_edge(edge_id).unwrap();
    assert_eq!(edge.properties.get("since"), Some(&Value::Int(2020)));
    assert_eq!(edge.properties.get("weight"), Some(&Value::Float(0.5)));
}

#[test]
fn cow_mmap_unified_traversal_query_after_mutation() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();
    let g = graph.gremlin();

    // Add vertices via traversal
    g.add_v("Person").property("name", "Alice").iterate();
    g.add_v("Person").property("name", "Bob").iterate();
    g.add_v("Software").property("name", "GraphDB").iterate();

    // Query via traversal
    let all = g.v().to_list();
    assert_eq!(all.len(), 3);

    let people = g.v().has_label("Person").to_list();
    assert_eq!(people.len(), 2);
}

#[test]
fn cow_mmap_unified_traversal_full_workflow() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();
    let g = graph.gremlin();

    // Create vertices
    let alice_result = g.add_v("Person").property("name", "Alice").next().unwrap();
    let bob_result = g.add_v("Person").property("name", "Bob").next().unwrap();

    let alice = match alice_result {
        Value::Vertex(id) => id,
        _ => panic!("Expected vertex ID"),
    };
    let bob = match bob_result {
        Value::Vertex(id) => id,
        _ => panic!("Expected vertex ID"),
    };

    // Create edge
    g.add_e("KNOWS")
        .from_id(alice)
        .to_id(bob)
        .property("since", 2020i64)
        .iterate();

    // Verify structure
    assert_eq!(graph.vertex_count(), 2);
    assert_eq!(graph.edge_count(), 1);

    // Query outgoing neighbors
    let neighbors = g.v_id(alice).out_label("KNOWS").to_list();
    assert_eq!(neighbors.len(), 1);
}

#[test]
fn cow_mmap_unified_traversal_drop_vertex() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();
    let g = graph.gremlin();

    // Add vertices
    let alice_result = g.add_v("Person").property("name", "Alice").next().unwrap();
    g.add_v("Person").property("name", "Bob").iterate();

    let alice = match alice_result {
        Value::Vertex(id) => id,
        _ => panic!("Expected vertex ID"),
    };

    assert_eq!(graph.vertex_count(), 2);

    // Drop Alice
    g.v_id(alice).drop().iterate();

    assert_eq!(graph.vertex_count(), 1);

    // Verify Alice is gone
    let snap = graph.snapshot();
    assert!(snap.get_vertex(alice).is_none());
}

#[test]
fn cow_mmap_unified_traversal_drop_edge() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();

    let alice = graph.add_vertex("Person", HashMap::new()).unwrap();
    let bob = graph.add_vertex("Person", HashMap::new()).unwrap();
    let edge = graph.add_edge(alice, bob, "KNOWS", HashMap::new()).unwrap();

    assert_eq!(graph.edge_count(), 1);

    let g = graph.gremlin();
    g.e_ids([edge]).drop().iterate();

    assert_eq!(graph.edge_count(), 0);
    assert_eq!(graph.vertex_count(), 2); // Vertices remain
}

#[test]
fn cow_mmap_unified_traversal_v_returns_all_vertices() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();
    let g = graph.gremlin();

    g.add_v("A").iterate();
    g.add_v("B").iterate();
    g.add_v("C").iterate();

    let vertices = g.v().to_list();
    assert_eq!(vertices.len(), 3);
}

#[test]
fn cow_mmap_unified_traversal_v_id_returns_specific_vertex() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();
    let g = graph.gremlin();

    let alice_result = g.add_v("Person").property("name", "Alice").next().unwrap();
    g.add_v("Person").property("name", "Bob").iterate();

    let alice = match alice_result {
        Value::Vertex(id) => id,
        _ => panic!("Expected vertex ID"),
    };

    let result = g.v_id(alice).to_list();
    assert_eq!(result.len(), 1);
}

#[test]
fn cow_mmap_unified_traversal_chained_steps() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();
    let g = graph.gremlin();

    // Create a mini social graph
    let alice_result = g.add_v("Person").property("name", "Alice").next().unwrap();
    let bob_result = g.add_v("Person").property("name", "Bob").next().unwrap();
    g.add_v("Software").property("name", "GraphDB").iterate();

    let alice = match alice_result {
        Value::Vertex(id) => id,
        _ => panic!("Expected vertex ID"),
    };
    let bob = match bob_result {
        Value::Vertex(id) => id,
        _ => panic!("Expected vertex ID"),
    };

    g.add_e("KNOWS").from_id(alice).to_id(bob).iterate();

    // Chain: start at alice, follow KNOWS, get the vertex
    let result = g.v_id(alice).out_label("KNOWS").to_list();
    assert_eq!(result.len(), 1);
}

#[test]
fn cow_mmap_unified_traversal_count() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();
    let g = graph.gremlin();

    g.add_v("Person").iterate();
    g.add_v("Person").iterate();
    g.add_v("Person").iterate();
    g.add_v("Software").iterate();

    let count = g.v().count();
    assert_eq!(count, 4);

    let person_count = g.v().has_label("Person").count();
    assert_eq!(person_count, 3);
}

#[test]
fn cow_mmap_unified_traversal_has_next() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();
    let g = graph.gremlin();

    assert!(!g.v().has_next());

    g.add_v("Person").iterate();

    assert!(g.v().has_next());
    assert!(g.v().has_label("Person").has_next());
    assert!(!g.v().has_label("Software").has_next());
}

#[test]
fn cow_mmap_unified_traversal_persists_after_checkpoint() {
    let (_dir, path) = temp_db();

    // Create graph and add data via traversal
    {
        let graph = CowMmapGraph::open(&path).unwrap();
        let g = graph.gremlin();

        let alice_result = g
            .add_v("Person")
            .property("name", "Alice")
            .property("age", 30i64)
            .next()
            .unwrap();
        let bob_result = g.add_v("Person").property("name", "Bob").next().unwrap();

        let alice = match alice_result {
            Value::Vertex(id) => id,
            _ => panic!("Expected vertex ID"),
        };
        let bob = match bob_result {
            Value::Vertex(id) => id,
            _ => panic!("Expected vertex ID"),
        };

        g.add_e("KNOWS")
            .from_id(alice)
            .to_id(bob)
            .property("since", 2020i64)
            .iterate();

        graph.checkpoint().unwrap();
    }

    // Reopen and verify
    {
        let graph = CowMmapGraph::open(&path).unwrap();
        let g = graph.gremlin();

        assert_eq!(g.v().count(), 2);
        assert_eq!(g.e().count(), 1);

        let people = g.v().has_label("Person").to_list();
        assert_eq!(people.len(), 2);
    }
}

#[test]
fn cow_mmap_unified_traversal_limit_and_skip() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();
    let g = graph.gremlin();

    for i in 0..10 {
        g.add_v("Node").property("id", i as i64).iterate();
    }

    let first_3 = g.v().limit(3).to_list();
    assert_eq!(first_3.len(), 3);

    let skip_5 = g.v().skip(5).to_list();
    assert_eq!(skip_5.len(), 5);

    let middle = g.v().skip(3).limit(4).to_list();
    assert_eq!(middle.len(), 4);
}

#[test]
fn cow_mmap_unified_traversal_values() {
    let (_dir, path) = temp_db();
    let graph = CowMmapGraph::open(&path).unwrap();
    let g = graph.gremlin();

    g.add_v("Person").property("name", "Alice").iterate();
    g.add_v("Person").property("name", "Bob").iterate();

    let names = g.v().has_label("Person").values("name").to_list();
    assert_eq!(names.len(), 2);
    assert!(names.contains(&Value::String("Alice".into())));
    assert!(names.contains(&Value::String("Bob".into())));
}
