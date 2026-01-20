//! Legacy graph container with RwLock-based concurrency control.
//!
//! **DEPRECATED**: This module contains legacy types that wrap `dyn GraphStorage` with
//! RwLock-based concurrency. For new code, use the COW-based [`Graph`](crate::storage::Graph)
//! from [`crate::storage`] (also available in the prelude) which provides:
//!
//! - O(1) snapshot creation via structural sharing
//! - Lock-free reads (snapshots don't hold locks)
//! - Owned snapshots that can outlive the source graph
//! - Simpler mutation API
//!
//! This module provides legacy types:
//!
//! - [`LegacyGraph`] - The thread-safe container that owns graph storage
//! - [`LegacyGraphSnapshot`] - A read-only view for concurrent traversals
//! - [`LegacyGraphMut`] - An exclusive handle for mutations
//!
//! # Migration
//!
//! Replace legacy usage:
//! ```ignore
//! // Old (deprecated):
//! use interstellar::graph::{Graph, GraphSnapshot};
//! let graph = Graph::new(storage);
//!
//! // New (recommended):
//! use interstellar::prelude::*;
//! let graph = Graph::new();  // Uses COW-based Graph
//! ```ignore
//!
//! # Concurrency Model
//!
//! This legacy implementation uses a readers-writer lock to provide safe concurrent access:
//!
//! - **Multiple readers**: Any number of snapshots can exist simultaneously,
//!   allowing concurrent read-only traversals across threads.
//! - **Single writer**: A [`LegacyGraphMut`] requires exclusive access. No snapshots or
//!   other mutations can be active while it exists.
//!
//! This model ensures that traversals always see a consistent view of the graph,
//! even when mutations are pending.

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::Arc;

use crate::schema::GraphSchema;
use crate::storage::interner::StringInterner;
use crate::storage::{GraphStorage, InMemoryGraph};

/// **DEPRECATED**: Use [`Graph`](crate::storage::Graph) from the prelude instead.
///
/// Legacy thread-safe graph container with RwLock-based concurrency.
///
/// This type wraps a `dyn GraphStorage` with readers-writer lock semantics.
/// For new code, use the COW-based [`Graph`](crate::storage::Graph) which provides
/// better performance through structural sharing.
///
/// # Migration
///
/// ```ignore
/// // Old (deprecated):
/// use interstellar::graph::LegacyGraph;
/// use interstellar::storage::InMemoryGraph;
/// let graph = LegacyGraph::new(InMemoryGraph::new());
///
/// // New (recommended):
/// use interstellar::prelude::*;
/// let graph = Graph::new();
/// ```ignore
#[deprecated(since = "0.2.0", note = "Use crate::storage::Graph instead")]
pub struct LegacyGraph {
    pub(crate) storage: Arc<dyn GraphStorage>,
    pub(crate) lock: Arc<RwLock<()>>,
    /// Optional schema for validating mutations.
    ///
    /// When set, mutation operations (CREATE, SET, MERGE) will validate
    /// against this schema according to its [`ValidationMode`](crate::schema::ValidationMode).
    pub(crate) schema: Arc<RwLock<Option<GraphSchema>>>,
}

/// **DEPRECATED**: Use [`GraphSnapshot`](crate::storage::GraphSnapshot) from the prelude instead.
///
/// Legacy read-only snapshot of a graph for concurrent traversals.
///
/// This snapshot holds a read lock on the graph. For new code, use the COW-based
/// [`GraphSnapshot`](crate::storage::GraphSnapshot) which is lock-free.
#[deprecated(since = "0.2.0", note = "Use crate::storage::GraphSnapshot instead")]
pub struct LegacyGraphSnapshot<'g> {
    /// Reference to the parent graph.
    #[allow(deprecated)]
    pub graph: &'g LegacyGraph,
    /// The version of the graph at snapshot time (reserved for MVCC).
    pub version: u64,
    pub(crate) _guard: RwLockReadGuard<'g, ()>,
}

#[allow(deprecated)]
impl<'g> LegacyGraphSnapshot<'g> {
    /// Create a new traversal source for querying this snapshot.
    ///
    /// The returned [`GraphTraversalSource`](crate::traversal::GraphTraversalSource)
    /// provides a fluent API for graph traversals. Multiple traversals can be
    /// created from the same snapshot.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::prelude::*;
    ///
    /// let graph = Graph::in_memory();
    /// let snapshot = graph.snapshot();
    /// let g = snapshot.gremlin();
    ///
    /// // Start traversing
    /// let vertices = g.v().to_list();
    /// ```
    pub fn gremlin(&self) -> crate::traversal::GraphTraversalSource<'_> {
        crate::traversal::GraphTraversalSource::from_snapshot(self)
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

    /// Get the current schema from the parent graph.
    ///
    /// Returns a clone of the schema to avoid holding additional locks.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::prelude::*;
    /// use interstellar::schema::{SchemaBuilder, PropertyType, ValidationMode};
    ///
    /// let schema = SchemaBuilder::new()
    ///     .mode(ValidationMode::Strict)
    ///     .vertex("Person")
    ///         .property("name", PropertyType::String)
    ///         .done()
    ///     .build();
    ///
    /// let graph = Graph::in_memory_with_schema(schema);
    /// let snapshot = graph.snapshot();
    ///
    /// let schema = snapshot.schema();
    /// assert!(schema.is_some());
    /// ```
    pub fn schema(&self) -> Option<GraphSchema> {
        self.graph.schema.read().clone()
    }
}

// Implement SnapshotLike for LegacyGraphSnapshot to enable generic traversal/GQL usage
#[allow(deprecated)]
impl<'g> crate::traversal::SnapshotLike for LegacyGraphSnapshot<'g> {
    fn storage(&self) -> &dyn crate::storage::GraphStorage {
        self.graph.storage.as_ref()
    }

    fn interner(&self) -> &StringInterner {
        self.graph.storage.interner()
    }
}

/// **DEPRECATED**: Use the direct mutation API on [`Graph`](crate::storage::Graph) instead.
///
/// Legacy exclusive mutable handle to a graph.
///
/// This handle holds a write lock on the graph. For new code, use the COW-based
/// [`Graph`](crate::storage::Graph) which provides direct mutation methods
/// without needing a separate handle.
#[deprecated(
    since = "0.2.0",
    note = "Use crate::storage::Graph direct mutation API instead"
)]
pub struct LegacyGraphMut<'g> {
    /// Reference to the parent graph.
    #[allow(deprecated)]
    pub graph: &'g LegacyGraph,
    pub(crate) _guard: RwLockWriteGuard<'g, ()>,
}

#[allow(deprecated)]
impl<'g> LegacyGraphMut<'g> {
    /// Get the current schema from the parent graph.
    ///
    /// Returns a clone of the schema to avoid holding additional locks.
    pub fn schema(&self) -> Option<GraphSchema> {
        self.graph.schema.read().clone()
    }

    /// Execute a GQL mutation statement with schema validation.
    ///
    /// Parses and executes the GQL mutation (CREATE, SET, DELETE, etc.),
    /// validating against the graph's schema if one is set.
    ///
    /// # Arguments
    ///
    /// * `query` - The GQL mutation statement to execute
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The query has a syntax error
    /// - Schema validation fails (missing required properties, type mismatch, etc.)
    /// - The mutation cannot be executed (e.g., deleting vertex with edges)
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::prelude::*;
    /// use interstellar::schema::{SchemaBuilder, PropertyType, ValidationMode};
    /// use interstellar::storage::{GraphStorage, GraphStorageMut, InMemoryGraph};
    ///
    /// // Create a graph with schema
    /// let schema = SchemaBuilder::new()
    ///     .mode(ValidationMode::Strict)
    ///     .vertex("Person")
    ///         .property("name", PropertyType::String)
    ///         .done()
    ///     .build();
    ///
    /// let mut storage = InMemoryGraph::new();
    /// let graph = Graph::with_schema(storage, schema);
    ///
    /// // Get the underlying storage for mutation
    /// // Note: For real usage, you would use a mutable storage reference
    /// ```
    pub fn gql<S: crate::storage::GraphStorageMut>(
        &self,
        query: &str,
        storage: &mut S,
    ) -> Result<Vec<crate::value::Value>, crate::gql::MutationError> {
        let stmt = crate::gql::parse_statement(query).map_err(|e| {
            crate::gql::MutationError::Compile(crate::gql::CompileError::UnsupportedFeature(
                e.to_string(),
            ))
        })?;
        let schema = self.schema();
        crate::gql::execute_mutation_with_schema(&stmt, storage, schema.as_ref())
    }

    /// Execute a GQL DDL statement (CREATE TYPE, ALTER TYPE, DROP TYPE).
    ///
    /// DDL statements modify the schema rather than the data. The schema
    /// changes are applied to the graph's schema immediately.
    ///
    /// # Arguments
    ///
    /// * `query` - The GQL DDL statement to execute
    ///
    /// # Returns
    ///
    /// Returns the updated schema after executing the DDL statement.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The query has a syntax error
    /// - The DDL statement is invalid (e.g., dropping a non-existent type)
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::prelude::*;
    /// use interstellar::schema::ValidationMode;
    ///
    /// let graph = Graph::in_memory();
    ///
    /// {
    ///     let mut_handle = graph.mutate();
    ///
    ///     // Create a node type
    ///     mut_handle.ddl("CREATE NODE TYPE Person (name STRING NOT NULL)").unwrap();
    ///
    ///     // Set validation mode
    ///     mut_handle.ddl("SET SCHEMA VALIDATION STRICT").unwrap();
    /// }
    ///
    /// // Schema is now active
    /// let schema = graph.schema().unwrap();
    /// assert!(schema.has_vertex_schema("Person"));
    /// assert_eq!(schema.mode, ValidationMode::Strict);
    /// ```
    pub fn ddl(&self, query: &str) -> Result<GraphSchema, crate::gql::GqlError> {
        let stmt = crate::gql::parse_statement(query)?;

        // Extract DDL statement from parsed statement
        let ddl = match stmt {
            crate::gql::Statement::Ddl(ddl) => ddl,
            _ => {
                return Err(crate::gql::GqlError::Compile(
                    crate::gql::CompileError::UnsupportedFeature(
                        "Expected DDL statement (CREATE TYPE, ALTER TYPE, DROP TYPE, SET SCHEMA VALIDATION)".into(),
                    ),
                ))
            }
        };

        // Get current schema or create empty one
        let mut schema = self.graph.schema.read().clone().unwrap_or_default();

        // Execute DDL
        crate::gql::execute_ddl(&mut schema, &ddl).map_err(|e| {
            crate::gql::GqlError::Compile(crate::gql::CompileError::UnsupportedFeature(
                e.to_string(),
            ))
        })?;

        // Update the graph's schema
        *self.graph.schema.write() = Some(schema.clone());

        Ok(schema)
    }
}

#[allow(deprecated)]
impl LegacyGraph {
    /// Create a new graph with the given storage backend.
    ///
    /// This is the general constructor that accepts any [`GraphStorage`]
    /// implementation. The storage is automatically wrapped in an `Arc` for
    /// thread-safe sharing. For convenience, use [`Graph::in_memory()`] to
    /// create an in-memory graph without manually constructing storage.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::prelude::*;
    /// use interstellar::storage::InMemoryGraph;
    ///
    /// let storage = InMemoryGraph::new();
    /// let graph = Graph::new(storage);
    /// ```
    pub fn new<S: GraphStorage + 'static>(storage: S) -> Self {
        LegacyGraph {
            storage: Arc::new(storage),
            lock: Arc::new(RwLock::new(())),
            schema: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a new graph with a schema for validation.
    ///
    /// This constructor creates a [`Graph`] with an associated schema that will
    /// be used to validate mutation operations (CREATE, SET, MERGE).
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::prelude::*;
    /// use interstellar::storage::InMemoryGraph;
    /// use interstellar::schema::{SchemaBuilder, PropertyType, ValidationMode};
    ///
    /// let storage = InMemoryGraph::new();
    /// let schema = SchemaBuilder::new()
    ///     .mode(ValidationMode::Strict)
    ///     .vertex("Person")
    ///         .property("name", PropertyType::String)
    ///         .done()
    ///     .build();
    ///
    /// let graph = Graph::with_schema(storage, schema);
    /// ```
    pub fn with_schema<S: GraphStorage + 'static>(storage: S, schema: GraphSchema) -> Self {
        LegacyGraph {
            storage: Arc::new(storage),
            lock: Arc::new(RwLock::new(())),
            schema: Arc::new(RwLock::new(Some(schema))),
        }
    }

    /// Create a new graph from an existing `Arc<dyn GraphStorage>`.
    ///
    /// Use this when you already have an Arc-wrapped storage, for example
    /// when sharing storage between multiple Graph instances or when
    /// integrating with external code that provides Arc-wrapped storage.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::prelude::*;
    /// use interstellar::storage::{GraphStorage, InMemoryGraph};
    /// use std::sync::Arc;
    ///
    /// let storage: Arc<dyn GraphStorage> = Arc::new(InMemoryGraph::new());
    /// let graph = Graph::from_arc(storage);
    /// ```
    pub fn from_arc(storage: Arc<dyn GraphStorage>) -> Self {
        LegacyGraph {
            storage,
            lock: Arc::new(RwLock::new(())),
            schema: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a new graph from an existing `Arc<dyn GraphStorage>` with a schema.
    ///
    /// Use this when you already have an Arc-wrapped storage and want to
    /// add schema validation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::prelude::*;
    /// use interstellar::storage::{GraphStorage, InMemoryGraph};
    /// use interstellar::schema::{SchemaBuilder, PropertyType, ValidationMode};
    /// use std::sync::Arc;
    ///
    /// let storage: Arc<dyn GraphStorage> = Arc::new(InMemoryGraph::new());
    /// let schema = SchemaBuilder::new()
    ///     .mode(ValidationMode::Strict)
    ///     .vertex("Person")
    ///         .property("name", PropertyType::String)
    ///         .done()
    ///     .build();
    ///
    /// let graph = Graph::from_arc_with_schema(storage, schema);
    /// ```
    pub fn from_arc_with_schema(storage: Arc<dyn GraphStorage>, schema: GraphSchema) -> Self {
        LegacyGraph {
            storage,
            lock: Arc::new(RwLock::new(())),
            schema: Arc::new(RwLock::new(Some(schema))),
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
    /// ```ignore
    /// use interstellar::prelude::*;
    ///
    /// let graph = Graph::in_memory();
    ///
    /// // Create a snapshot for read access
    /// let snapshot = graph.snapshot();
    /// let g = snapshot.gremlin();
    ///
    /// // Traverse the graph
    /// let count = g.v().count();
    /// ```
    ///
    /// # Panics
    ///
    /// This method does not panic. The underlying `parking_lot::RwLock` is
    /// panic-free on acquisition.
    pub fn snapshot(&self) -> LegacyGraphSnapshot<'_> {
        LegacyGraphSnapshot {
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
    /// ```ignore
    /// use interstellar::prelude::*;
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
    pub fn mutate(&self) -> LegacyGraphMut<'_> {
        LegacyGraphMut {
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
    /// ```ignore
    /// use interstellar::prelude::*;
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
    pub fn try_mutate(&self) -> Option<LegacyGraphMut<'_>> {
        self.lock.try_write().map(|guard| LegacyGraphMut {
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
    /// ```ignore
    /// use interstellar::prelude::*;
    ///
    /// let graph = Graph::in_memory();
    ///
    /// // Ready to use - start with a snapshot
    /// let snapshot = graph.snapshot();
    /// let g = snapshot.gremlin();
    /// assert_eq!(g.v().count(), 0); // Empty graph
    /// ```
    pub fn in_memory() -> Self {
        Self::new(InMemoryGraph::new())
    }

    /// Create a new in-memory graph with a schema for validation.
    ///
    /// This is a convenience method that creates an empty in-memory graph
    /// with an associated schema for validating mutations.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::prelude::*;
    /// use interstellar::schema::{SchemaBuilder, PropertyType, ValidationMode};
    ///
    /// let schema = SchemaBuilder::new()
    ///     .mode(ValidationMode::Strict)
    ///     .vertex("Person")
    ///         .property("name", PropertyType::String)
    ///         .done()
    ///     .build();
    ///
    /// let graph = Graph::in_memory_with_schema(schema);
    /// ```
    pub fn in_memory_with_schema(schema: GraphSchema) -> Self {
        Self::with_schema(InMemoryGraph::new(), schema)
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
    /// ```ignore
    /// use interstellar::prelude::*;
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

    /// Get the current schema, if one is set.
    ///
    /// Returns a clone of the schema to avoid holding a lock. The returned
    /// schema is a snapshot of the schema at the time of the call.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::prelude::*;
    /// use interstellar::schema::{SchemaBuilder, PropertyType, ValidationMode};
    ///
    /// let schema = SchemaBuilder::new()
    ///     .mode(ValidationMode::Strict)
    ///     .vertex("Person")
    ///         .property("name", PropertyType::String)
    ///         .done()
    ///     .build();
    ///
    /// let graph = Graph::in_memory_with_schema(schema);
    ///
    /// // Get the schema
    /// let schema = graph.schema();
    /// assert!(schema.is_some());
    /// assert!(schema.unwrap().has_vertex_schema("Person"));
    /// ```
    pub fn schema(&self) -> Option<GraphSchema> {
        self.schema.read().clone()
    }

    /// Create a new `Graph` that shares the same storage and schema.
    ///
    /// This creates a new `Graph` instance with its own lock but sharing
    /// the underlying `Arc<dyn GraphStorage>` and schema. This is useful
    /// for scenarios where you need multiple `Graph` handles to the same
    /// underlying data, such as scripting integrations.
    ///
    /// # Note
    ///
    /// Each `Graph` created by `share()` has its own independent lock. This means
    /// that locking one graph does not affect the other. Both graphs share the same
    /// underlying storage and schema data.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::prelude::*;
    ///
    /// let graph1 = Graph::in_memory();
    /// let graph2 = graph1.share();
    ///
    /// // Both graphs share the same storage
    /// assert_eq!(graph1.storage().vertex_count(), graph2.storage().vertex_count());
    /// ```
    pub fn share(&self) -> LegacyGraph {
        LegacyGraph {
            storage: Arc::clone(&self.storage),
            lock: Arc::clone(&self.lock),
            schema: Arc::clone(&self.schema),
        }
    }

    /// Set or replace the graph schema.
    ///
    /// The new schema will be used for all subsequent mutation operations.
    /// Pass `None` to disable schema validation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::prelude::*;
    /// use interstellar::schema::{SchemaBuilder, PropertyType, ValidationMode};
    ///
    /// let graph = Graph::in_memory();
    ///
    /// // Initially no schema
    /// assert!(graph.schema().is_none());
    ///
    /// // Add a schema
    /// let schema = SchemaBuilder::new()
    ///     .mode(ValidationMode::Strict)
    ///     .vertex("Person")
    ///         .property("name", PropertyType::String)
    ///         .done()
    ///     .build();
    ///
    /// graph.set_schema(Some(schema));
    /// assert!(graph.schema().is_some());
    ///
    /// // Remove the schema
    /// graph.set_schema(None);
    /// assert!(graph.schema().is_none());
    /// ```
    pub fn set_schema(&self, schema: Option<GraphSchema>) {
        *self.schema.write() = schema;
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_try_mutate_succeeds_when_unlocked() {
        let graph = LegacyGraph::in_memory();
        let mut_handle = graph.try_mutate();
        assert!(
            mut_handle.is_some(),
            "try_mutate should succeed when unlocked"
        );
    }

    #[test]
    fn test_try_mutate_fails_when_locked() {
        let graph = LegacyGraph::in_memory();
        let _mut1 = graph.mutate(); // Hold write lock
        let mut2 = graph.try_mutate();
        assert!(
            mut2.is_none(),
            "try_mutate should fail when write lock is held"
        );
    }

    #[test]
    fn test_try_mutate_fails_when_snapshot_held() {
        let graph = LegacyGraph::in_memory();
        let _snap = graph.snapshot(); // Hold read lock
        let mut_handle = graph.try_mutate();
        assert!(
            mut_handle.is_none(),
            "try_mutate should fail when read lock is held"
        );
    }

    #[test]
    fn test_try_mutate_succeeds_after_lock_released() {
        let graph = LegacyGraph::in_memory();
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
        let graph = LegacyGraph::in_memory();
        let _snap1 = graph.snapshot();
        let _snap2 = graph.snapshot();
        let _snap3 = graph.snapshot();
        // All snapshots should coexist (multiple readers allowed)
    }

    #[test]
    fn test_concurrent_try_mutate() {
        let graph = Arc::new(LegacyGraph::in_memory());
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
        let graph = Arc::new(LegacyGraph::in_memory());
        let graph_clone = Arc::clone(&graph);

        // Thread 1 holds snapshot with traversal
        let handle1 = thread::spawn(move || {
            let snap = graph_clone.snapshot();
            let _g = snap.gremlin();
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
        let graph = Arc::new(LegacyGraph::in_memory());

        // Multiple readers can hold snapshots concurrently
        let snap1 = graph.snapshot();
        let snap2 = graph.snapshot();
        let snap3 = graph.snapshot();

        let _g1 = snap1.gremlin();
        let _g2 = snap2.gremlin();
        let _g3 = snap3.gremlin();

        // All traversals should coexist (multiple readers allowed)
        // No assertion needed - this tests that it doesn't deadlock
    }

    #[test]
    fn test_traversal_from_single_snapshot() {
        let graph = LegacyGraph::in_memory();
        let snap = graph.snapshot();

        // Can create multiple traversals from the same snapshot
        let _g1 = snap.gremlin();
        let _g2 = snap.gremlin();

        // Both should work without issues
    }
}

// =============================================================================
// Unified Graph Traits (Spec 33)
// =============================================================================

use std::collections::HashMap;

use crate::error::StorageError;
use crate::gql::GqlError;
use crate::value::{EdgeId, Value, VertexId};

/// A unified graph database trait with COW snapshot semantics.
///
/// This trait provides a unified API for all graph implementations,
/// supporting both in-memory and persistent storage backends.
///
/// # COW Semantics
///
/// All implementations use copy-on-write (COW) semantics:
/// - Snapshots are O(1) to create via structural sharing
/// - Snapshots are immutable and don't hold locks
/// - Mutations create new versions without blocking readers
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync`, allowing the graph to be
/// shared across threads.
///
/// # Example
///
/// ```ignore
/// use interstellar::prelude::*;
///
/// fn count_people<G: UnifiedGraph>(graph: &G) -> usize {
///     graph.snapshot().vertex_count() as usize
/// }
/// ```ignore
pub trait UnifiedGraph: Send + Sync {
    /// The snapshot type returned by this graph.
    type Snapshot: UnifiedSnapshot;

    /// Create an immutable snapshot of the current graph state.
    ///
    /// This is an O(1) operation that creates a snapshot via structural
    /// sharing. The snapshot does not hold any locks and can outlive
    /// references to the source graph.
    fn snapshot(&self) -> Self::Snapshot;

    /// Get the string interner for label resolution.
    fn interner(&self) -> &StringInterner;

    /// Get the current vertex count.
    fn vertex_count(&self) -> usize;

    /// Get the current edge count.
    fn edge_count(&self) -> usize;

    /// Add a vertex with the given label and properties.
    ///
    /// Returns the ID of the newly created vertex.
    fn add_vertex(&self, label: &str, properties: HashMap<String, Value>) -> VertexId;

    /// Add an edge between two vertices.
    ///
    /// Returns the ID of the newly created edge, or an error if
    /// either vertex doesn't exist.
    fn add_edge(
        &self,
        from: VertexId,
        to: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError>;

    /// Remove a vertex and all its connected edges.
    fn remove_vertex(&self, id: VertexId) -> Result<(), StorageError>;

    /// Remove an edge.
    fn remove_edge(&self, id: EdgeId) -> Result<(), StorageError>;

    /// Set a property on a vertex.
    fn set_vertex_property(
        &self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError>;

    /// Set a property on an edge.
    fn set_edge_property(&self, id: EdgeId, key: &str, value: Value) -> Result<(), StorageError>;

    /// Remove a property from a vertex.
    fn remove_vertex_property(&self, id: VertexId, key: &str) -> Result<(), StorageError>;

    /// Remove a property from an edge.
    fn remove_edge_property(&self, id: EdgeId, key: &str) -> Result<(), StorageError>;

    /// Get the schema, if any.
    fn schema(&self) -> Option<GraphSchema>;

    /// Set the schema.
    fn set_schema(&self, schema: GraphSchema);

    /// Execute a GQL statement (both reads and mutations).
    ///
    /// - Read queries (MATCH...RETURN) are executed against a snapshot
    /// - Mutations (CREATE, SET, DELETE) are executed against the graph
    fn gql(&self, statement: &str) -> Result<Vec<Value>, GqlError>;

    /// Execute a parameterized GQL statement.
    fn gql_with_params(
        &self,
        statement: &str,
        params: HashMap<String, Value>,
    ) -> Result<Vec<Value>, GqlError>;
}

/// An immutable snapshot of a graph at a point in time.
///
/// Snapshots implement [`GraphStorage`](crate::storage::GraphStorage) for read
/// operations and can be used with the traversal engine and GQL compiler.
///
/// # COW Semantics
///
/// Snapshots are created via structural sharing (O(1)) and are completely
/// independent of the source graph after creation. They:
/// - Don't hold any locks
/// - Can be cloned cheaply
/// - Can be sent across threads
/// - Won't see mutations made after snapshot creation
///
/// # Example
///
/// ```ignore
/// let snap = graph.snapshot();
///
/// // Read via GraphStorage API
/// let vertex = snap.get_vertex(VertexId(1));
///
/// // Read via Gremlin traversal
/// let g = snap.gremlin();
/// let names = g.v().has_label("Person").values("name").to_list();
/// ```
pub trait UnifiedSnapshot: crate::storage::GraphStorage + Send + Sync + Clone {
    /// Get the string interner for label resolution.
    fn interner(&self) -> &StringInterner;
}

// =============================================================================
// Backward-Compatible Type Aliases (Deprecated)
// =============================================================================

/// **DEPRECATED**: Use [`crate::storage::Graph`] instead.
///
/// This alias is provided for backward compatibility during migration.
#[deprecated(since = "0.2.0", note = "Use crate::storage::Graph instead")]
#[allow(deprecated)]
pub type Graph = LegacyGraph;

/// **DEPRECATED**: Use [`crate::storage::GraphSnapshot`] instead.
///
/// This alias is provided for backward compatibility during migration.
#[deprecated(since = "0.2.0", note = "Use crate::storage::GraphSnapshot instead")]
#[allow(deprecated)]
pub type GraphSnapshot<'g> = LegacyGraphSnapshot<'g>;

/// **DEPRECATED**: Use the direct mutation API on [`crate::storage::Graph`] instead.
///
/// This alias is provided for backward compatibility during migration.
#[deprecated(
    since = "0.2.0",
    note = "Use crate::storage::Graph direct mutation API instead"
)]
#[allow(deprecated)]
pub type GraphMut<'g> = LegacyGraphMut<'g>;
