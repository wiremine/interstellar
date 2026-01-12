//! Graph container with snapshot-based concurrency control.
//!
//! This module provides the main entry points for working with graphs in Intersteller:
//!
//! - [`Graph`] - The thread-safe container that owns graph storage
//! - [`GraphSnapshot`] - A read-only view for concurrent traversals
//! - [`GraphMut`] - An exclusive handle for mutations
//!
//! # Concurrency Model
//!
//! Intersteller uses a readers-writer lock to provide safe concurrent access:
//!
//! - **Multiple readers**: Any number of [`GraphSnapshot`]s can exist simultaneously,
//!   allowing concurrent read-only traversals across threads.
//! - **Single writer**: A [`GraphMut`] requires exclusive access. No snapshots or
//!   other mutations can be active while a `GraphMut` exists.
//!
//! This model ensures that traversals always see a consistent view of the graph,
//! even when mutations are pending.
//!
//! # Example
//!
//! ```rust
//! use intersteller::prelude::*;
//! use intersteller::storage::InMemoryGraph;
//! use std::collections::HashMap;
//! use std::sync::Arc;
//!
//! // Create an in-memory graph
//! let mut storage = InMemoryGraph::new();
//!
//! // Add some data
//! let alice = storage.add_vertex("person", {
//!     let mut props = HashMap::new();
//!     props.insert("name".to_string(), Value::from("Alice"));
//!     props.insert("age".to_string(), Value::from(30i64));
//!     props
//! });
//!
//! let bob = storage.add_vertex("person", {
//!     let mut props = HashMap::new();
//!     props.insert("name".to_string(), Value::from("Bob"));
//!     props.insert("age".to_string(), Value::from(25i64));
//!     props
//! });
//!
//! storage.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
//!
//! // Wrap storage in a Graph for traversal
//! let graph = Graph::new(Arc::new(storage));
//!
//! // Create a snapshot for read-only access
//! let snapshot = graph.snapshot();
//!
//! // Start traversing
//! let g = snapshot.traversal();
//! let people = g.v().has_label("person").to_list();
//! assert_eq!(people.len(), 2);
//!
//! // Count edges
//! let edge_count = g.e().count();
//! assert_eq!(edge_count, 1);
//! ```
//!
//! # Thread Safety
//!
//! [`Graph`] is both `Send` and `Sync`, making it safe to share across threads.
//! The typical pattern for concurrent access is to wrap the graph in an `Arc`:
//!
//! ```rust
//! use intersteller::prelude::*;
//! use std::sync::Arc;
//! use std::thread;
//!
//! let graph = Arc::new(Graph::in_memory());
//!
//! // Multiple threads can take snapshots concurrently
//! let handles: Vec<_> = (0..4).map(|i| {
//!     let g = Arc::clone(&graph);
//!     thread::spawn(move || {
//!         let snap = g.snapshot();
//!         let traversal = snap.traversal();
//!         traversal.v().count()
//!     })
//! }).collect();
//!
//! for handle in handles {
//!     let count = handle.join().unwrap();
//!     assert_eq!(count, 0); // Empty graph
//! }
//! ```

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::Arc;

use crate::storage::interner::StringInterner;
use crate::storage::{GraphStorage, InMemoryGraph};

/// A thread-safe graph container with snapshot-based concurrency.
///
/// `Graph` is the primary entry point for working with graph data. It owns
/// the underlying storage and provides controlled access through snapshots
/// (for reading) and mutation handles (for writing).
///
/// # Creating a Graph
///
/// There are two ways to create a `Graph`:
///
/// ```rust
/// use intersteller::prelude::*;
/// use intersteller::storage::InMemoryGraph;
/// use std::sync::Arc;
///
/// // Method 1: Convenience constructor for in-memory graphs
/// let graph = Graph::in_memory();
///
/// // Method 2: With custom storage
/// let storage = InMemoryGraph::new();
/// let graph = Graph::new(Arc::new(storage));
/// ```
///
/// # Accessing Data
///
/// To read from the graph, create a [`GraphSnapshot`] using [`snapshot()`](Graph::snapshot):
///
/// ```rust
/// use intersteller::prelude::*;
///
/// let graph = Graph::in_memory();
/// let snapshot = graph.snapshot();
/// let g = snapshot.traversal();
///
/// // Now you can traverse
/// let vertex_count = g.v().count();
/// ```
///
/// # Thread Safety
///
/// `Graph` implements `Send` and `Sync`, making it safe to share across threads.
/// The internal `RwLock` ensures that:
///
/// - Multiple [`GraphSnapshot`]s can exist simultaneously (shared read access)
/// - Only one [`GraphMut`] can exist at a time (exclusive write access)
/// - A [`GraphMut`] cannot coexist with any [`GraphSnapshot`]
///
/// # Panics
///
/// The `Graph` type uses `parking_lot::RwLock` which does not panic on lock
/// acquisition. However, if your storage implementation panics, that panic
/// will propagate through traversal operations.
pub struct Graph {
    pub(crate) storage: Arc<dyn GraphStorage>,
    pub(crate) lock: Arc<RwLock<()>>,
}

/// A read-only snapshot of a graph for concurrent traversals.
///
/// `GraphSnapshot` holds a read lock on the graph, allowing safe concurrent
/// access from multiple threads. While a snapshot exists, the graph data
/// is guaranteed not to change.
///
/// # Lifetime
///
/// The snapshot borrows from the parent [`Graph`] and cannot outlive it.
/// The read lock is held for the entire lifetime of the snapshot.
///
/// # Creating Traversals
///
/// Use [`traversal()`](GraphSnapshot::traversal) to create a
/// [`GraphTraversalSource`](crate::traversal::GraphTraversalSource) for
/// querying the graph:
///
/// ```rust
/// use intersteller::prelude::*;
///
/// let graph = Graph::in_memory();
/// let snapshot = graph.snapshot();
/// let g = snapshot.traversal();
///
/// // Multiple traversals can be created from the same snapshot
/// let count1 = g.v().count();
/// let count2 = g.e().count();
/// ```
///
/// # Concurrency
///
/// Multiple snapshots can exist simultaneously, even across threads:
///
/// ```rust
/// use intersteller::prelude::*;
///
/// let graph = Graph::in_memory();
///
/// // These can all exist at the same time
/// let snap1 = graph.snapshot();
/// let snap2 = graph.snapshot();
/// let snap3 = graph.snapshot();
/// ```
///
/// However, while any snapshot exists, calls to [`Graph::mutate()`] will
/// block, and [`Graph::try_mutate()`] will return `None`.
pub struct GraphSnapshot<'g> {
    /// Reference to the parent graph.
    pub graph: &'g Graph,
    /// The version of the graph at snapshot time (reserved for MVCC).
    pub version: u64,
    pub(crate) _guard: RwLockReadGuard<'g, ()>,
}

impl<'g> GraphSnapshot<'g> {
    /// Create a new traversal source for querying this snapshot.
    ///
    /// The returned [`GraphTraversalSource`](crate::traversal::GraphTraversalSource)
    /// provides a fluent API for graph traversals. Multiple traversals can be
    /// created from the same snapshot.
    ///
    /// # Example
    ///
    /// ```rust
    /// use intersteller::prelude::*;
    ///
    /// let graph = Graph::in_memory();
    /// let snapshot = graph.snapshot();
    /// let g = snapshot.traversal();
    ///
    /// // Start traversing
    /// let vertices = g.v().to_list();
    /// ```
    pub fn traversal(&self) -> crate::traversal::GraphTraversalSource<'_> {
        crate::traversal::GraphTraversalSource::new(self, self.interner())
    }

    /// Get the underlying storage.
    #[inline]
    pub fn storage(&self) -> &dyn GraphStorage {
        self.graph.storage.as_ref()
    }

    /// Get the string interner for label resolution.
    ///
    /// This is used by the traversal engine to efficiently resolve
    /// label strings to interned IDs.
    #[inline]
    pub fn interner(&self) -> &StringInterner {
        self.graph.storage.interner()
    }

    /// Execute a GQL query or statement against this snapshot.
    ///
    /// Parses and executes the GQL query or UNION statement, returning matching
    /// results as a vector of [`Value`](crate::value::Value)s.
    ///
    /// Supports both single queries and UNION/UNION ALL statements:
    /// - `MATCH (n:Person) RETURN n.name` - single query
    /// - `MATCH (a:A) RETURN a.name UNION MATCH (b:B) RETURN b.name` - UNION (deduplicates)
    /// - `MATCH (a:A) RETURN a.name UNION ALL MATCH (b:B) RETURN b.name` - UNION ALL (keeps dupes)
    ///
    /// # Example
    ///
    /// ```rust
    /// use intersteller::prelude::*;
    /// use intersteller::storage::InMemoryGraph;
    /// use std::sync::Arc;
    ///
    /// // Create storage with data
    /// let mut storage = InMemoryGraph::new();
    /// let mut props = std::collections::HashMap::new();
    /// props.insert("name".to_string(), Value::from("Alice"));
    /// storage.add_vertex("Person", props);
    ///
    /// // Wrap in Graph for querying
    /// let graph = Graph::new(Arc::new(storage));
    ///
    /// let snapshot = graph.snapshot();
    /// let results = snapshot.gql("MATCH (n:Person) RETURN n").unwrap();
    /// assert_eq!(results.len(), 1);
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a [`GqlError`](crate::gql::GqlError) if:
    /// - The query has a syntax error ([`ParseError`](crate::gql::ParseError))
    /// - The query references undefined variables ([`CompileError`](crate::gql::CompileError))
    pub fn gql(&self, query: &str) -> Result<Vec<crate::value::Value>, crate::gql::GqlError> {
        let stmt = crate::gql::parse_statement(query)?;
        let results = crate::gql::compile_statement(&stmt, self)?;
        Ok(results)
    }
}

/// An exclusive mutable handle to a graph.
///
/// `GraphMut` holds a write lock on the graph, providing exclusive access
/// for mutations. While a `GraphMut` exists, no other mutations or snapshots
/// can be created.
///
/// # Lifetime
///
/// The handle borrows from the parent [`Graph`] and cannot outlive it.
/// The write lock is held for the entire lifetime of the handle.
///
/// # Acquiring Mutable Access
///
/// There are two ways to acquire a `GraphMut`:
///
/// - [`Graph::mutate()`] - Blocks until exclusive access is available
/// - [`Graph::try_mutate()`] - Returns `None` immediately if access is unavailable
///
/// # Example
///
/// ```rust
/// use intersteller::prelude::*;
///
/// let graph = Graph::in_memory();
///
/// // Acquire exclusive access (blocks if needed)
/// {
///     let _mut_handle = graph.mutate();
///     // ... perform mutations ...
/// } // Lock released when handle is dropped
///
/// // Non-blocking alternative
/// let acquired = graph.try_mutate().is_some();
/// assert!(acquired); // No other locks held, so this succeeds
/// ```
///
/// # Concurrency
///
/// Only one `GraphMut` can exist at a time. While it exists:
///
/// - Calls to [`Graph::snapshot()`] will block
/// - Calls to [`Graph::mutate()`] will block
/// - Calls to [`Graph::try_mutate()`] will return `None`
pub struct GraphMut<'g> {
    /// Reference to the parent graph.
    pub graph: &'g Graph,
    pub(crate) _guard: RwLockWriteGuard<'g, ()>,
}

impl Graph {
    /// Create a new graph with the given storage backend.
    ///
    /// This is the general constructor that accepts any [`GraphStorage`]
    /// implementation. For convenience, use [`Graph::in_memory()`] to create
    /// an in-memory graph without manually constructing storage.
    ///
    /// # Example
    ///
    /// ```rust
    /// use intersteller::prelude::*;
    /// use intersteller::storage::InMemoryGraph;
    /// use std::sync::Arc;
    ///
    /// let storage = InMemoryGraph::new();
    /// let graph = Graph::new(Arc::new(storage));
    /// ```
    pub fn new(storage: Arc<dyn GraphStorage>) -> Self {
        Graph {
            storage,
            lock: Arc::new(RwLock::new(())),
        }
    }

    /// Create a read-only snapshot of the graph.
    ///
    /// The snapshot holds a read lock, allowing concurrent access from multiple
    /// threads. While any snapshot exists, mutations are blocked.
    ///
    /// # Blocking Behavior
    ///
    /// This method will block if a [`GraphMut`] currently exists. Once the
    /// mutation handle is dropped, the snapshot will be created.
    ///
    /// # Example
    ///
    /// ```rust
    /// use intersteller::prelude::*;
    ///
    /// let graph = Graph::in_memory();
    ///
    /// // Create a snapshot for read access
    /// let snapshot = graph.snapshot();
    /// let g = snapshot.traversal();
    ///
    /// // Traverse the graph
    /// let count = g.v().count();
    /// ```
    ///
    /// # Panics
    ///
    /// This method does not panic. The underlying `parking_lot::RwLock` is
    /// panic-free on acquisition.
    pub fn snapshot(&self) -> GraphSnapshot<'_> {
        GraphSnapshot {
            graph: self,
            version: 0,
            _guard: self.lock.read(),
        }
    }

    /// Acquire exclusive mutable access to the graph.
    ///
    /// Returns a [`GraphMut`] handle that holds a write lock. While this handle
    /// exists, no other mutations or snapshots can be created.
    ///
    /// # Blocking Behavior
    ///
    /// This method will block if:
    /// - Another [`GraphMut`] currently exists
    /// - Any [`GraphSnapshot`]s currently exist
    ///
    /// Use [`try_mutate()`](Graph::try_mutate) for a non-blocking alternative.
    ///
    /// # Example
    ///
    /// ```rust
    /// use intersteller::prelude::*;
    ///
    /// let graph = Graph::in_memory();
    ///
    /// {
    ///     let mut_handle = graph.mutate();
    ///     // ... perform mutations ...
    /// } // Write lock released here
    ///
    /// // Now snapshots can be taken again
    /// let snapshot = graph.snapshot();
    /// ```
    ///
    /// # Panics
    ///
    /// This method does not panic. The underlying `parking_lot::RwLock` is
    /// panic-free on acquisition.
    pub fn mutate(&self) -> GraphMut<'_> {
        GraphMut {
            graph: self,
            _guard: self.lock.write(),
        }
    }

    /// Try to acquire exclusive mutable access without blocking.
    ///
    /// Returns `Some(GraphMut)` if the write lock is available, or `None` if
    /// the lock is currently held by readers or another writer.
    ///
    /// This is useful when you want to attempt a mutation but fall back to
    /// other behavior if the graph is currently in use.
    ///
    /// # Example
    ///
    /// ```rust
    /// use intersteller::prelude::*;
    ///
    /// let graph = Graph::in_memory();
    ///
    /// // Check if we can acquire the lock
    /// assert!(graph.try_mutate().is_some()); // Succeeds, no other locks
    ///
    /// // While holding a snapshot, try_mutate fails
    /// let _snapshot = graph.snapshot();
    /// assert!(graph.try_mutate().is_none()); // Fails, read lock held
    /// ```
    ///
    /// # When This Returns `None`
    ///
    /// - Another thread holds a [`GraphMut`]
    /// - One or more threads hold [`GraphSnapshot`]s
    pub fn try_mutate(&self) -> Option<GraphMut<'_>> {
        self.lock.try_write().map(|guard| GraphMut {
            graph: self,
            _guard: guard,
        })
    }

    /// Create a new in-memory graph with no persistence.
    ///
    /// This is a convenience method that creates a [`Graph`] backed by an
    /// [`InMemoryGraph`]. The graph starts
    /// empty and all data is lost when the graph is dropped.
    ///
    /// # Example
    ///
    /// ```rust
    /// use intersteller::prelude::*;
    ///
    /// let graph = Graph::in_memory();
    ///
    /// // Ready to use - start with a snapshot
    /// let snapshot = graph.snapshot();
    /// let g = snapshot.traversal();
    /// assert_eq!(g.v().count(), 0); // Empty graph
    /// ```
    pub fn in_memory() -> Self {
        Self::new(Arc::new(InMemoryGraph::new()))
    }

    /// Get the underlying storage backend.
    ///
    /// This provides direct access to the storage implementation for advanced
    /// use cases. Most users should use [`snapshot()`](Graph::snapshot) and
    /// [`mutate()`](Graph::mutate) for safe concurrent access.
    ///
    /// # Warning
    ///
    /// Accessing storage directly bypasses the locking mechanism. If you need
    /// to perform operations that require consistency guarantees, use the
    /// snapshot/mutate pattern instead.
    ///
    /// # Example
    ///
    /// ```rust
    /// use intersteller::prelude::*;
    ///
    /// let graph = Graph::in_memory();
    ///
    /// // Get storage stats (read-only, safe without lock)
    /// let storage = graph.storage();
    /// println!("Vertex count: {}", storage.vertex_count());
    /// println!("Edge count: {}", storage.edge_count());
    /// ```
    pub fn storage(&self) -> &Arc<dyn GraphStorage> {
        &self.storage
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_try_mutate_succeeds_when_unlocked() {
        let graph = Graph::in_memory();
        let mut_handle = graph.try_mutate();
        assert!(
            mut_handle.is_some(),
            "try_mutate should succeed when unlocked"
        );
    }

    #[test]
    fn test_try_mutate_fails_when_locked() {
        let graph = Graph::in_memory();
        let _mut1 = graph.mutate(); // Hold write lock
        let mut2 = graph.try_mutate();
        assert!(
            mut2.is_none(),
            "try_mutate should fail when write lock is held"
        );
    }

    #[test]
    fn test_try_mutate_fails_when_snapshot_held() {
        let graph = Graph::in_memory();
        let _snap = graph.snapshot(); // Hold read lock
        let mut_handle = graph.try_mutate();
        assert!(
            mut_handle.is_none(),
            "try_mutate should fail when read lock is held"
        );
    }

    #[test]
    fn test_try_mutate_succeeds_after_lock_released() {
        let graph = Graph::in_memory();
        {
            let _mut1 = graph.mutate(); // Acquire and release
        } // Lock released here
        let mut2 = graph.try_mutate();
        assert!(
            mut2.is_some(),
            "try_mutate should succeed after lock is released"
        );
    }

    #[test]
    fn test_multiple_snapshots_allowed() {
        let graph = Graph::in_memory();
        let _snap1 = graph.snapshot();
        let _snap2 = graph.snapshot();
        let _snap3 = graph.snapshot();
        // All snapshots should coexist (multiple readers allowed)
    }

    #[test]
    fn test_concurrent_try_mutate() {
        let graph = Arc::new(Graph::in_memory());
        let graph_clone = Arc::clone(&graph);

        // Thread 1 holds the lock
        let handle1 = thread::spawn(move || {
            let _mut = graph_clone.mutate();
            thread::sleep(Duration::from_millis(100));
        });

        // Give thread 1 time to acquire the lock
        thread::sleep(Duration::from_millis(20));

        // Thread 2 tries to acquire
        let result = graph.try_mutate();
        assert!(
            result.is_none(),
            "try_mutate should fail when another thread holds lock"
        );

        handle1.join().unwrap();

        // Now it should succeed
        let result = graph.try_mutate();
        assert!(
            result.is_some(),
            "try_mutate should succeed after other thread releases lock"
        );
    }

    #[test]
    fn test_traversal_holds_read_lock() {
        let graph = Arc::new(Graph::in_memory());
        let graph_clone = Arc::clone(&graph);

        // Thread 1 holds snapshot with traversal
        let handle1 = thread::spawn(move || {
            let snap = graph_clone.snapshot();
            let _g = snap.traversal();
            thread::sleep(Duration::from_millis(100));
            // Read lock held until snap drops
        });

        // Give thread 1 time to acquire the read lock
        thread::sleep(Duration::from_millis(20));

        // Thread 2 tries to get write lock - should fail
        let result = graph.try_mutate();
        assert!(
            result.is_none(),
            "try_mutate should fail when traversal holds read lock"
        );

        handle1.join().unwrap();

        // Now it should succeed
        let result = graph.try_mutate();
        assert!(
            result.is_some(),
            "try_mutate should succeed after traversal completes"
        );
    }

    #[test]
    fn test_multiple_traversals_concurrent() {
        let graph = Arc::new(Graph::in_memory());

        // Multiple readers can hold snapshots concurrently
        let snap1 = graph.snapshot();
        let snap2 = graph.snapshot();
        let snap3 = graph.snapshot();

        let _g1 = snap1.traversal();
        let _g2 = snap2.traversal();
        let _g3 = snap3.traversal();

        // All traversals should coexist (multiple readers allowed)
        // No assertion needed - this tests that it doesn't deadlock
    }

    #[test]
    fn test_traversal_from_single_snapshot() {
        let graph = Graph::in_memory();
        let snap = graph.snapshot();

        // Can create multiple traversals from the same snapshot
        let _g1 = snap.traversal();
        let _g2 = snap.traversal();

        // Both should work without issues
    }
}
