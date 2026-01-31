//! Trait for graph access operations needed by GraphVertex/GraphEdge.
//!
//! This module defines the [`GraphAccess`] trait which abstracts read and write
//! access to the graph. Both `Arc<Graph>` (in-memory) and `Arc<CowMmapGraph>`
//! (persistent) implement this trait, allowing `GraphVertex` and `GraphEdge`
//! to work with any storage backend.
//!
//! # Overview
//!
//! The `GraphAccess` trait provides the minimal interface that `GraphVertex` and
//! `GraphEdge` need to access vertex/edge data and perform mutations. This enables
//! a unified API across storage backends.
//!
//! # Thread Safety
//!
//! Implementations must be `Send + Sync` to allow concurrent access. The methods
//! use interior mutability patterns (like `RwLock`) internally, so they take `&self`
//! even for mutation operations.
//!
//! # Example
//!
//! ```rust
//! use interstellar::prelude::*;
//! use interstellar::graph_access::GraphAccess;
//! use std::sync::Arc;
//!
//! // Arc<Graph> implements GraphAccess
//! let graph: Arc<Graph> = Arc::new(Graph::new());
//!
//! // Use GraphAccess methods
//! let snapshot = graph.snapshot();
//! let g = snapshot.gremlin();
//! ```

use std::collections::HashMap;

use crate::error::StorageError;
use crate::storage::{Edge, Vertex};
use crate::value::{EdgeId, Value, VertexId};

/// Trait for graph access operations needed by GraphVertex/GraphEdge.
///
/// This trait provides the minimal interface that `GraphVertex` and `GraphEdge`
/// need to access vertex/edge data and perform mutations. Both `Graph` (in-memory)
/// and `CowMmapGraph` (persistent) implement this trait.
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to allow concurrent access.
///
/// # Implementation Notes
///
/// The methods use interior mutability patterns (like `RwLock`) internally,
/// so they take `&self` even for mutation operations. This matches the
/// design of `Graph` which uses `RwLock<GraphState>` internally.
///
/// # Type Parameters
///
/// Implementations are typically for `Arc<Graph>` or `Arc<CowMmapGraph>`, not
/// the raw graph types, because `GraphVertex` and `GraphEdge` need to clone
/// the reference.
pub trait GraphAccess: Send + Sync + Clone + 'static {
    // =========================================================================
    // Read Operations
    // =========================================================================

    /// Get a vertex by ID.
    ///
    /// Returns `None` if the vertex doesn't exist.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_access::GraphAccess;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let id = graph.add_vertex("person", HashMap::new());
    ///
    /// let vertex = graph.get_vertex(id);
    /// assert!(vertex.is_some());
    /// assert_eq!(vertex.unwrap().label, "person");
    /// ```
    fn get_vertex(&self, id: VertexId) -> Option<Vertex>;

    /// Get an edge by ID.
    ///
    /// Returns `None` if the edge doesn't exist.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_access::GraphAccess;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let a = graph.add_vertex("person", HashMap::new());
    /// let b = graph.add_vertex("person", HashMap::new());
    /// let edge_id = graph.add_edge(a, b, "knows", HashMap::new()).unwrap();
    ///
    /// let edge = graph.get_edge(edge_id);
    /// assert!(edge.is_some());
    /// assert_eq!(edge.unwrap().label, "knows");
    /// ```
    fn get_edge(&self, id: EdgeId) -> Option<Edge>;

    /// Get outgoing edge IDs from a vertex.
    ///
    /// Returns edge IDs for edges where the vertex is the source.
    /// Returns an empty Vec if the vertex doesn't exist.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_access::GraphAccess;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let a = graph.add_vertex("person", HashMap::new());
    /// let b = graph.add_vertex("person", HashMap::new());
    /// graph.add_edge(a, b, "knows", HashMap::new()).unwrap();
    ///
    /// let out_edges = graph.out_edge_ids(a);
    /// assert_eq!(out_edges.len(), 1);
    /// ```
    fn out_edge_ids(&self, vertex: VertexId) -> Vec<EdgeId>;

    /// Get incoming edge IDs to a vertex.
    ///
    /// Returns edge IDs for edges where the vertex is the destination.
    /// Returns an empty Vec if the vertex doesn't exist.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_access::GraphAccess;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let a = graph.add_vertex("person", HashMap::new());
    /// let b = graph.add_vertex("person", HashMap::new());
    /// graph.add_edge(a, b, "knows", HashMap::new()).unwrap();
    ///
    /// let in_edges = graph.in_edge_ids(b);
    /// assert_eq!(in_edges.len(), 1);
    /// ```
    fn in_edge_ids(&self, vertex: VertexId) -> Vec<EdgeId>;

    // =========================================================================
    // Write Operations
    // =========================================================================

    /// Set a property on a vertex.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::VertexNotFound` if the vertex doesn't exist.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_access::GraphAccess;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let id = graph.add_vertex("person", HashMap::new());
    ///
    /// graph.set_vertex_property(id, "name", Value::from("Alice")).unwrap();
    ///
    /// let vertex = graph.get_vertex(id).unwrap();
    /// assert_eq!(vertex.properties.get("name"), Some(&Value::String("Alice".to_string())));
    /// ```
    fn set_vertex_property(
        &self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError>;

    /// Set a property on an edge.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::EdgeNotFound` if the edge doesn't exist.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_access::GraphAccess;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let a = graph.add_vertex("person", HashMap::new());
    /// let b = graph.add_vertex("person", HashMap::new());
    /// let edge_id = graph.add_edge(a, b, "knows", HashMap::new()).unwrap();
    ///
    /// graph.set_edge_property(edge_id, "since", Value::from(2020i64)).unwrap();
    ///
    /// let edge = graph.get_edge(edge_id).unwrap();
    /// assert_eq!(edge.properties.get("since"), Some(&Value::Int(2020)));
    /// ```
    fn set_edge_property(&self, id: EdgeId, key: &str, value: Value) -> Result<(), StorageError>;

    /// Add a new edge between vertices.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::VertexNotFound` if either `src` or `dst` doesn't exist.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_access::GraphAccess;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let a = graph.add_vertex("person", HashMap::new());
    /// let b = graph.add_vertex("person", HashMap::new());
    ///
    /// let edge_id = graph.add_edge(a, b, "knows", HashMap::new()).unwrap();
    /// assert!(graph.get_edge(edge_id).is_some());
    /// ```
    fn add_edge(
        &self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError>;

    /// Remove a vertex and all incident edges.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::VertexNotFound` if the vertex doesn't exist.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_access::GraphAccess;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let id = graph.add_vertex("person", HashMap::new());
    ///
    /// assert!(graph.get_vertex(id).is_some());
    /// graph.remove_vertex(id).unwrap();
    /// assert!(graph.get_vertex(id).is_none());
    /// ```
    fn remove_vertex(&self, id: VertexId) -> Result<(), StorageError>;

    /// Remove an edge.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::EdgeNotFound` if the edge doesn't exist.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_access::GraphAccess;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let a = graph.add_vertex("person", HashMap::new());
    /// let b = graph.add_vertex("person", HashMap::new());
    /// let edge_id = graph.add_edge(a, b, "knows", HashMap::new()).unwrap();
    ///
    /// assert!(graph.get_edge(edge_id).is_some());
    /// graph.remove_edge(edge_id).unwrap();
    /// assert!(graph.get_edge(edge_id).is_none());
    /// ```
    fn remove_edge(&self, id: EdgeId) -> Result<(), StorageError>;
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Graph;
    use std::sync::Arc;

    #[test]
    fn arc_graph_implements_graph_access() {
        let graph = Arc::new(Graph::new());
        let id = graph.add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Alice".into())]),
        );

        // Test get_vertex
        let v = graph.get_vertex(id).unwrap();
        assert_eq!(v.label, "person");
        assert_eq!(
            v.properties.get("name"),
            Some(&Value::String("Alice".to_string()))
        );

        // Test set_vertex_property
        graph
            .set_vertex_property(id, "age", Value::Int(30))
            .unwrap();
        let v = graph.get_vertex(id).unwrap();
        assert_eq!(v.properties.get("age"), Some(&Value::Int(30)));
    }

    #[test]
    fn arc_graph_edge_operations() {
        let graph = Arc::new(Graph::new());
        let a = graph.add_vertex("person", HashMap::new());
        let b = graph.add_vertex("person", HashMap::new());

        // Test add_edge via GraphAccess
        let edge_id = GraphAccess::add_edge(&graph, a, b, "knows", HashMap::new()).unwrap();
        assert!(graph.get_edge(edge_id).is_some());

        // Test out_edge_ids
        let out_edges = graph.out_edge_ids(a);
        assert_eq!(out_edges.len(), 1);
        assert_eq!(out_edges[0], edge_id);

        // Test in_edge_ids
        let in_edges = graph.in_edge_ids(b);
        assert_eq!(in_edges.len(), 1);
        assert_eq!(in_edges[0], edge_id);

        // Test set_edge_property
        graph
            .set_edge_property(edge_id, "since", Value::Int(2020))
            .unwrap();
        let e = graph.get_edge(edge_id).unwrap();
        assert_eq!(e.properties.get("since"), Some(&Value::Int(2020)));

        // Test remove_edge
        graph.remove_edge(edge_id).unwrap();
        assert!(graph.get_edge(edge_id).is_none());
    }

    #[test]
    fn arc_graph_vertex_removal() {
        let graph = Arc::new(Graph::new());
        let a = graph.add_vertex("person", HashMap::new());
        let b = graph.add_vertex("person", HashMap::new());
        let _edge_id = GraphAccess::add_edge(&graph, a, b, "knows", HashMap::new()).unwrap();

        // Remove vertex removes incident edges too
        graph.remove_vertex(a).unwrap();
        assert!(graph.get_vertex(a).is_none());
        // Edge should also be removed
        assert_eq!(graph.out_edge_ids(a).len(), 0);
    }

    #[test]
    fn arc_graph_error_on_nonexistent_vertex() {
        let graph = Arc::new(Graph::new());

        // set_vertex_property on nonexistent vertex
        let result = graph.set_vertex_property(VertexId(999), "name", Value::from("Bob"));
        assert!(matches!(result, Err(StorageError::VertexNotFound(_))));

        // remove_vertex on nonexistent vertex
        let result = graph.remove_vertex(VertexId(999));
        assert!(matches!(result, Err(StorageError::VertexNotFound(_))));
    }

    #[test]
    fn arc_graph_error_on_nonexistent_edge() {
        let graph = Arc::new(Graph::new());

        // set_edge_property on nonexistent edge
        let result = graph.set_edge_property(EdgeId(999), "since", Value::from(2020i64));
        assert!(matches!(result, Err(StorageError::EdgeNotFound(_))));

        // remove_edge on nonexistent edge
        let result = graph.remove_edge(EdgeId(999));
        assert!(matches!(result, Err(StorageError::EdgeNotFound(_))));
    }

    #[test]
    fn arc_graph_add_edge_error_on_nonexistent_vertices() {
        let graph = Arc::new(Graph::new());
        let a = graph.add_vertex("person", HashMap::new());

        // add_edge with nonexistent dst
        let result = GraphAccess::add_edge(&graph, a, VertexId(999), "knows", HashMap::new());
        assert!(matches!(result, Err(StorageError::VertexNotFound(_))));

        // add_edge with nonexistent src
        let result = GraphAccess::add_edge(&graph, VertexId(999), a, "knows", HashMap::new());
        assert!(matches!(result, Err(StorageError::VertexNotFound(_))));
    }

    #[test]
    fn arc_graph_clone_and_thread_safety() {
        use std::thread;

        let graph = Arc::new(Graph::new());
        let id = graph.add_vertex("person", HashMap::new());

        // Clone the Arc - both references should work
        let graph2 = Arc::clone(&graph);

        let handle = thread::spawn(move || {
            let v = graph2.get_vertex(id);
            assert!(v.is_some());
        });

        handle.join().unwrap();

        // Original should still work
        assert!(graph.get_vertex(id).is_some());
    }

    // =========================================================================
    // Arc<CowMmapGraph> Tests (mmap feature)
    // =========================================================================

    #[cfg(feature = "mmap")]
    mod mmap_tests {
        use super::*;
        use crate::storage::CowMmapGraph;
        use tempfile::tempdir;

        fn temp_db_path() -> (tempfile::TempDir, std::path::PathBuf) {
            let dir = tempdir().unwrap();
            let path = dir.path().join("test.db");
            (dir, path)
        }

        #[test]
        fn arc_mmap_graph_implements_graph_access() {
            let (_dir, path) = temp_db_path();
            let graph = Arc::new(CowMmapGraph::open(&path).unwrap());
            let id = graph
                .add_vertex(
                    "person",
                    HashMap::from([("name".to_string(), "Alice".into())]),
                )
                .unwrap();

            // Test get_vertex
            let v = graph.get_vertex(id).unwrap();
            assert_eq!(v.label, "person");
            assert_eq!(
                v.properties.get("name"),
                Some(&Value::String("Alice".to_string()))
            );

            // Test set_vertex_property
            graph
                .set_vertex_property(id, "age", Value::Int(30))
                .unwrap();
            let v = graph.get_vertex(id).unwrap();
            assert_eq!(v.properties.get("age"), Some(&Value::Int(30)));
        }

        #[test]
        fn arc_mmap_graph_edge_operations() {
            let (_dir, path) = temp_db_path();
            let graph = Arc::new(CowMmapGraph::open(&path).unwrap());
            let a = graph.add_vertex("person", HashMap::new()).unwrap();
            let b = graph.add_vertex("person", HashMap::new()).unwrap();

            // Test add_edge via GraphAccess
            let edge_id = GraphAccess::add_edge(&graph, a, b, "knows", HashMap::new()).unwrap();
            assert!(graph.get_edge(edge_id).is_some());

            // Test out_edge_ids
            let out_edges = graph.out_edge_ids(a);
            assert_eq!(out_edges.len(), 1);
            assert_eq!(out_edges[0], edge_id);

            // Test in_edge_ids
            let in_edges = graph.in_edge_ids(b);
            assert_eq!(in_edges.len(), 1);
            assert_eq!(in_edges[0], edge_id);

            // Test set_edge_property
            graph
                .set_edge_property(edge_id, "since", Value::Int(2020))
                .unwrap();
            let e = graph.get_edge(edge_id).unwrap();
            assert_eq!(e.properties.get("since"), Some(&Value::Int(2020)));

            // Test remove_edge
            graph.remove_edge(edge_id).unwrap();
            assert!(graph.get_edge(edge_id).is_none());
        }

        #[test]
        fn arc_mmap_graph_vertex_removal() {
            let (_dir, path) = temp_db_path();
            let graph = Arc::new(CowMmapGraph::open(&path).unwrap());
            let a = graph.add_vertex("person", HashMap::new()).unwrap();
            let b = graph.add_vertex("person", HashMap::new()).unwrap();
            let _edge_id = GraphAccess::add_edge(&graph, a, b, "knows", HashMap::new()).unwrap();

            // Remove vertex removes incident edges too
            graph.remove_vertex(a).unwrap();
            assert!(graph.get_vertex(a).is_none());
            // Edge should also be removed
            assert_eq!(graph.out_edge_ids(a).len(), 0);
        }

        #[test]
        fn arc_mmap_graph_error_on_nonexistent_vertex() {
            let (_dir, path) = temp_db_path();
            let graph = Arc::new(CowMmapGraph::open(&path).unwrap());

            // set_vertex_property on nonexistent vertex
            let result = graph.set_vertex_property(VertexId(999), "name", Value::from("Bob"));
            assert!(matches!(result, Err(StorageError::VertexNotFound(_))));

            // remove_vertex on nonexistent vertex
            let result = graph.remove_vertex(VertexId(999));
            assert!(matches!(result, Err(StorageError::VertexNotFound(_))));
        }

        #[test]
        fn arc_mmap_graph_error_on_nonexistent_edge() {
            let (_dir, path) = temp_db_path();
            let graph = Arc::new(CowMmapGraph::open(&path).unwrap());

            // set_edge_property on nonexistent edge
            let result = graph.set_edge_property(EdgeId(999), "since", Value::from(2020i64));
            assert!(matches!(result, Err(StorageError::EdgeNotFound(_))));

            // remove_edge on nonexistent edge
            let result = graph.remove_edge(EdgeId(999));
            assert!(matches!(result, Err(StorageError::EdgeNotFound(_))));
        }

        #[test]
        fn arc_mmap_graph_clone_and_thread_safety() {
            use std::thread;

            let (_dir, path) = temp_db_path();
            let graph = Arc::new(CowMmapGraph::open(&path).unwrap());
            let id = graph.add_vertex("person", HashMap::new()).unwrap();

            // Clone the Arc - both references should work
            let graph2 = Arc::clone(&graph);

            let handle = thread::spawn(move || {
                let v = graph2.get_vertex(id);
                assert!(v.is_some());
            });

            handle.join().unwrap();

            // Original should still work
            assert!(graph.get_vertex(id).is_some());
        }
    }
}
