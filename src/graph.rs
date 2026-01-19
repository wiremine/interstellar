//! Graph container with snapshot-based concurrency control.
//!
//! This module provides the main entry points for working with graphs in Interstellar:
//!
//! - [`Graph`] - The thread-safe container that owns graph storage
//! - [`GraphSnapshot`] - A read-only view for concurrent traversals
//! - [`GraphMut`] - An exclusive handle for mutations
//!
//! # Concurrency Model
//!
//! Interstellar uses a readers-writer lock to provide safe concurrent access:
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
//! use interstellar::prelude::*;
//! use interstellar::storage::InMemoryGraph;
//! use std::collections::HashMap;
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
//! let graph = Graph::new(storage);
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
//! use interstellar::prelude::*;
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

use crate::schema::GraphSchema;
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
/// use interstellar::prelude::*;
/// use interstellar::storage::InMemoryGraph;
///
/// // Method 1: Convenience constructor for in-memory graphs
/// let graph = Graph::in_memory();
///
/// // Method 2: With custom storage
/// let storage = InMemoryGraph::new();
/// let graph = Graph::new(storage);
/// ```
///
/// # Accessing Data
///
/// To read from the graph, create a [`GraphSnapshot`] using [`snapshot()`](Graph::snapshot):
///
/// ```rust
/// use interstellar::prelude::*;
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
    /// Optional schema for validating mutations.
    ///
    /// When set, mutation operations (CREATE, SET, MERGE) will validate
    /// against this schema according to its [`ValidationMode`](crate::schema::ValidationMode).
    pub(crate) schema: Arc<RwLock<Option<GraphSchema>>>,
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
/// use interstellar::prelude::*;
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
/// use interstellar::prelude::*;
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
    /// use interstellar::prelude::*;
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
    /// use interstellar::prelude::*;
    /// use interstellar::storage::InMemoryGraph;
    ///
    /// // Create storage with data
    /// let mut storage = InMemoryGraph::new();
    /// let mut props = std::collections::HashMap::new();
    /// props.insert("name".to_string(), Value::from("Alice"));
    /// storage.add_vertex("Person", props);
    ///
    /// // Wrap in Graph for querying
    /// let graph = Graph::new(storage);
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

    /// Execute a parameterized GQL query against this snapshot.
    ///
    /// Similar to [`gql()`](Self::gql), but allows passing query parameters that
    /// can be referenced in the query using `$paramName` syntax. Parameters provide
    /// a safe way to inject values into queries without string concatenation.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::storage::InMemoryGraph;
    /// use interstellar::gql::Parameters;
    ///
    /// // Create storage with data
    /// let mut storage = InMemoryGraph::new();
    /// let mut props = std::collections::HashMap::new();
    /// props.insert("name".to_string(), Value::from("Alice"));
    /// props.insert("age".to_string(), Value::from(30));
    /// storage.add_vertex("Person", props);
    ///
    /// // Wrap in Graph for querying
    /// let graph = Graph::new(storage);
    ///
    /// let snapshot = graph.snapshot();
    ///
    /// // Use parameters instead of string interpolation
    /// let mut params = Parameters::new();
    /// params.insert("minAge".to_string(), Value::Int(25));
    ///
    /// let results = snapshot.gql_with_params(
    ///     "MATCH (n:Person) WHERE n.age >= $minAge RETURN n.name",
    ///     &params,
    /// ).unwrap();
    /// assert_eq!(results.len(), 1);
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a [`GqlError`](crate::gql::GqlError) if:
    /// - The query has a syntax error
    /// - A parameter referenced in the query is not provided in `params`
    /// - The query references undefined variables
    pub fn gql_with_params(
        &self,
        query: &str,
        params: &crate::gql::Parameters,
    ) -> Result<Vec<crate::value::Value>, crate::gql::GqlError> {
        let stmt = crate::gql::parse_statement(query)?;
        let results = crate::gql::compile_statement_with_params(&stmt, self, params)?;
        Ok(results)
    }

    /// Get the current schema from the parent graph.
    ///
    /// Returns a clone of the schema to avoid holding additional locks.
    ///
    /// # Example
    ///
    /// ```rust
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

// Implement SnapshotLike for GraphSnapshot to enable generic traversal/GQL usage
impl<'g> crate::traversal::SnapshotLike for GraphSnapshot<'g> {
    fn storage(&self) -> &dyn crate::storage::GraphStorage {
        self.graph.storage.as_ref()
    }

    fn interner(&self) -> &StringInterner {
        self.graph.storage.interner()
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
/// use interstellar::prelude::*;
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

impl<'g> GraphMut<'g> {
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
    /// ```rust
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
    /// ```rust
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

impl Graph {
    /// Create a new graph with the given storage backend.
    ///
    /// This is the general constructor that accepts any [`GraphStorage`]
    /// implementation. The storage is automatically wrapped in an `Arc` for
    /// thread-safe sharing. For convenience, use [`Graph::in_memory()`] to
    /// create an in-memory graph without manually constructing storage.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::storage::InMemoryGraph;
    ///
    /// let storage = InMemoryGraph::new();
    /// let graph = Graph::new(storage);
    /// ```
    pub fn new<S: GraphStorage + 'static>(storage: S) -> Self {
        Graph {
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
    /// ```rust
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
        Graph {
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
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::storage::{GraphStorage, InMemoryGraph};
    /// use std::sync::Arc;
    ///
    /// let storage: Arc<dyn GraphStorage> = Arc::new(InMemoryGraph::new());
    /// let graph = Graph::from_arc(storage);
    /// ```
    pub fn from_arc(storage: Arc<dyn GraphStorage>) -> Self {
        Graph {
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
    /// ```rust
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
        Graph {
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
    /// ```rust
    /// use interstellar::prelude::*;
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
    /// use interstellar::prelude::*;
    ///
    /// let graph = Graph::in_memory();
    ///
    /// // Ready to use - start with a snapshot
    /// let snapshot = graph.snapshot();
    /// let g = snapshot.traversal();
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
    /// ```rust
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
    /// ```rust
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
    /// ```rust
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
    /// ```rust
    /// use interstellar::prelude::*;
    ///
    /// let graph1 = Graph::in_memory();
    /// let graph2 = graph1.share();
    ///
    /// // Both graphs share the same storage
    /// assert_eq!(graph1.storage().vertex_count(), graph2.storage().vertex_count());
    /// ```
    pub fn share(&self) -> Graph {
        Graph {
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
    /// ```rust
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
/// ```
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

    /// Execute a GQL mutation statement.
    ///
    /// For read-only queries, use `snapshot().gql()` instead.
    fn gql(&self, statement: &str) -> Result<Vec<Value>, GqlError>;

    /// Execute a parameterized GQL mutation statement.
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
/// // Read via GQL
/// let results = snap.gql("MATCH (p:Person) RETURN p.name").unwrap();
///
/// // Read via traversal (when available on the concrete type)
/// // let g = snap.traversal();
/// ```
pub trait UnifiedSnapshot: crate::storage::GraphStorage + Send + Sync + Clone {
    /// Get the string interner for label resolution.
    fn interner(&self) -> &StringInterner;

    /// Execute a GQL read query against this snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the query is a mutation (use `graph.gql()` for mutations).
    fn gql(&self, query: &str) -> Result<Vec<Value>, GqlError>;

    /// Execute a parameterized GQL query against this snapshot.
    fn gql_with_params(
        &self,
        query: &str,
        params: HashMap<String, Value>,
    ) -> Result<Vec<Value>, GqlError>;

    // Note: traversal() method is intentionally not included here.
    // Each concrete type provides its own traversal() method that returns
    // the appropriate traversal source type. This will be unified in Phase 4.
}
