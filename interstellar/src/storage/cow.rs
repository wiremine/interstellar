//! Copy-on-Write graph storage with snapshot isolation.
//!
//! This module provides [`Graph`], a graph storage implementation using
//! persistent data structures from the `im` crate. This enables:
//!
//! - **Lock-free reads**: Snapshots don't hold locks
//! - **Owned snapshots**: Snapshots can outlive the source graph
//! - **Structural sharing**: O(1) snapshot creation via Arc cloning
//! - **Atomic mutations**: Each mutation is atomic without explicit transactions
//!
//! # Architecture
//!
//! ```text
//! Graph
//! ├── state: RwLock<GraphState>  # Mutable state container
//! └── schema: RwLock<Option<GraphSchema>>  # Optional schema
//!
//! GraphState (immutable, shareable via Clone)
//! ├── vertices: im::HashMap<VertexId, Arc<NodeData>>
//! ├── edges: im::HashMap<EdgeId, Arc<EdgeData>>
//! ├── vertex_labels: im::HashMap<u32, Arc<RoaringBitmap>>
//! ├── edge_labels: im::HashMap<u32, Arc<RoaringBitmap>>
//! ├── interner: Arc<StringInterner>
//! ├── version: u64
//! ├── next_vertex_id: u64
//! └── next_edge_id: u64
//!
//! GraphSnapshot (owned, immutable)
//! └── state: Arc<GraphState>
//! ```
//!
//! # Example
//!
//! ```
//! use interstellar::storage::Graph;
//! use interstellar::storage::GraphStorage;
//! use std::collections::HashMap;
//!
//! // Create a graph
//! let graph = Graph::new();
//!
//! // Add vertices
//! let alice = graph.add_vertex("person", HashMap::from([
//!     ("name".to_string(), "Alice".into()),
//! ]));
//!
//! // Take a snapshot - O(1) operation
//! let snap1 = graph.snapshot();
//!
//! // Mutate the graph
//! graph.set_vertex_property(alice, "age", 30.into()).unwrap();
//!
//! // snap1 still sees original state (no age property)
//! let v1 = snap1.get_vertex(alice).unwrap();
//! assert!(v1.properties.get("age").is_none());
//!
//! // New snapshot sees updated state
//! let snap2 = graph.snapshot();
//! let v2 = snap2.get_vertex(alice).unwrap();
//! assert_eq!(v2.properties.get("age").unwrap().as_i64(), Some(30));
//! ```
//!
//! # Thread Safety
//!
//! Both [`Graph`] and [`GraphSnapshot`] are `Send + Sync`:
//!
//! - Multiple threads can take snapshots concurrently
//! - Snapshots can be sent to other threads
//! - Writers are serialized via RwLock
//! - Readers never block writers (snapshots don't hold locks)

use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;

use im::HashMap as ImHashMap;
use parking_lot::RwLock;
use roaring::RoaringBitmap;

use crate::error::StorageError;
#[cfg(feature = "gql")]
use crate::gql::{self, GqlError};
use crate::graph_elements::{GraphEdge, GraphVertex, InMemoryEdge, InMemoryVertex};
use crate::index::{
    BTreeIndex, ElementType, IndexError, IndexSpec, IndexType, PropertyIndex, RTreeIndex,
    UniqueIndex,
};
use crate::schema::GraphSchema;
use crate::storage::interner::StringInterner;
use crate::storage::{Edge, GraphStorage, StreamableStorage, Vertex};
use crate::traversal::markers::{Edge as EdgeMarker, OutputMarker, Scalar, Vertex as VertexMarker};
use crate::traversal::mutation::{DropStep, PendingMutation, PropertyStep};
use crate::traversal::step::Step;
use crate::traversal::{
    ExecutionContext, HasLabelStep, HasStep, HasValueStep, IdStep, InEStep, InStep, InVStep,
    LabelStep, LimitStep, OutEStep, OutStep, OutVStep, SkipStep, Traversal, TraversalSource,
    Traverser, ValuesStep,
};
use crate::value::{EdgeId, IntoVertexId, Value, VertexId};
use std::marker::PhantomData;

// =============================================================================
// Core Data Structures
// =============================================================================

/// Internal vertex representation.
///
/// Wrapped in Arc for cheap cloning when modifying individual vertices.
#[derive(Clone, Debug)]
pub(crate) struct NodeData {
    /// Vertex identifier
    pub id: VertexId,
    /// Interned label string ID
    pub label_id: u32,
    /// Property key-value pairs
    pub properties: HashMap<String, Value>,
    /// Outgoing edge IDs (adjacency list)
    pub out_edges: Vec<EdgeId>,
    /// Incoming edge IDs (adjacency list)
    pub in_edges: Vec<EdgeId>,
}

/// Internal edge representation.
///
/// Wrapped in Arc for cheap cloning when modifying individual edges.
#[derive(Clone, Debug)]
pub(crate) struct EdgeData {
    /// Edge identifier
    pub id: EdgeId,
    /// Interned label string ID
    pub label_id: u32,
    /// Source vertex ID
    pub src: VertexId,
    /// Destination vertex ID
    pub dst: VertexId,
    /// Property key-value pairs
    pub properties: HashMap<String, Value>,
}

/// Immutable graph state that can be shared between snapshots.
///
/// All fields use persistent data structures from the `im` crate,
/// enabling O(1) cloning via structural sharing.
#[derive(Clone)]
pub struct GraphState {
    /// Vertex data: VertexId -> NodeData
    pub(crate) vertices: ImHashMap<VertexId, Arc<NodeData>>,

    /// Edge data: EdgeId -> EdgeData
    pub(crate) edges: ImHashMap<EdgeId, Arc<EdgeData>>,

    /// Label index: label_id -> set of vertex IDs
    pub(crate) vertex_labels: ImHashMap<u32, Arc<RoaringBitmap>>,

    /// Label index: label_id -> set of edge IDs
    pub(crate) edge_labels: ImHashMap<u32, Arc<RoaringBitmap>>,

    /// String interner (append-only, always shared)
    pub(crate) interner: Arc<RwLock<StringInterner>>,

    /// Monotonic version counter
    pub(crate) version: u64,

    /// Next vertex ID
    pub(crate) next_vertex_id: u64,

    /// Next edge ID
    pub(crate) next_edge_id: u64,
}

impl GraphState {
    /// Create a new empty graph state.
    pub fn new() -> Self {
        Self {
            vertices: ImHashMap::new(),
            edges: ImHashMap::new(),
            vertex_labels: ImHashMap::new(),
            edge_labels: ImHashMap::new(),
            interner: Arc::new(RwLock::new(StringInterner::new())),
            version: 0,
            next_vertex_id: 0,
            next_edge_id: 0,
        }
    }
}

impl Default for GraphState {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Graph - Main Graph Container
// =============================================================================

/// Copy-on-Write graph with snapshot support.
///
/// `Graph` uses persistent data structures to enable O(1) snapshot creation
/// and lock-free reads. Mutations are serialized via RwLock but don't block
/// existing snapshots.
///
/// # Creating a Graph
///
/// ```
/// use interstellar::storage::cow::Graph;
///
/// let graph = Graph::new();
/// ```
///
/// # Snapshots
///
/// Snapshots are O(1) and don't hold locks:
///
/// ```
/// use interstellar::storage::cow::Graph;
///
/// let graph = Graph::new();
/// let snap = graph.snapshot();
///
/// // snap can be sent to another thread, outlive the graph, etc.
/// ```
pub struct Graph {
    /// Current mutable state (protected by RwLock for thread safety).
    /// Wrapped in Arc to allow reactive snapshot factory closures to
    /// capture a reference to the live graph state.
    state: Arc<RwLock<GraphState>>,

    /// Schema for validation (optional)
    schema: RwLock<Option<GraphSchema>>,

    /// Property indexes for efficient lookups.
    /// Indexes are stored separately from state because they are mutable
    /// and don't need snapshot isolation (they always reflect current state).
    indexes: RwLock<HashMap<String, Box<dyn PropertyIndex>>>,

    /// Full-text indexes registered per indexed property name, split by
    /// element type. The two maps are independent storage but share a
    /// **global** property-name namespace: a single `property` may appear in
    /// at most one of them. Cross-map duplicates are rejected at creation.
    ///
    /// Stored as `Arc<dyn TextIndex>` so the search path can clone the Arc and
    /// run the Tantivy query without holding the outer lock. Each
    /// implementation uses interior mutability for `upsert`/`delete`/`commit`.
    #[cfg(feature = "full-text")]
    text_indexes_vertex:
        RwLock<HashMap<String, std::sync::Arc<dyn crate::storage::text::TextIndex>>>,

    #[cfg(feature = "full-text")]
    text_indexes_edge: RwLock<HashMap<String, std::sync::Arc<dyn crate::storage::text::TextIndex>>>,

    /// Event bus for reactive streaming queries. Only present when
    /// the `reactive` feature is enabled and not targeting WASM.
    #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
    event_bus: std::sync::Arc<crate::storage::events::EventBus>,

    /// Subscription manager for reactive streaming queries. Only present when
    /// the `reactive` feature is enabled and not targeting WASM.
    /// Lazily initialized on first `subscribe()` call.
    #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
    subscription_manager: std::sync::Arc<crate::traversal::reactive::SubscriptionManager>,
}

impl Graph {
    /// Create a new empty COW graph.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::Graph;
    ///
    /// let graph = Graph::new();
    /// assert_eq!(graph.vertex_count(), 0);
    /// ```
    pub fn new() -> Self {
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        let event_bus = std::sync::Arc::new(crate::storage::events::EventBus::new());

        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        let subscription_manager = {
            let eb = event_bus.clone();
            let event_sub_fn: std::sync::Arc<
                dyn Fn() -> std::sync::mpsc::Receiver<crate::storage::events::GraphEvent>
                    + Send
                    + Sync,
            > = std::sync::Arc::new(move || eb.subscribe());

            std::sync::Arc::new(crate::traversal::reactive::SubscriptionManager::new(
                event_sub_fn,
            ))
        };

        Self {
            state: Arc::new(RwLock::new(GraphState::new())),
            schema: RwLock::new(None),
            indexes: RwLock::new(HashMap::new()),
            #[cfg(feature = "full-text")]
            text_indexes_vertex: RwLock::new(HashMap::new()),
            #[cfg(feature = "full-text")]
            text_indexes_edge: RwLock::new(HashMap::new()),
            #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
            event_bus,
            #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
            subscription_manager,
        }
    }

    /// Convenience alias for [`Graph::new()`].
    ///
    /// This method exists for API compatibility with code that expects
    /// a `.in_memory()` constructor. Since `Graph` is always in-memory
    /// (for persistent storage use [`PersistentGraph`](crate::storage::PersistentGraph)),
    /// this is equivalent to `Graph::new()`.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::Graph;
    ///
    /// let graph = Graph::in_memory();
    /// assert_eq!(graph.vertex_count(), 0);
    /// ```
    #[inline]
    pub fn in_memory() -> Self {
        Self::new()
    }

    /// Create a new in-memory graph wrapped in an `Arc`.
    ///
    /// Convenience constructor for entry points that require an `Arc<Graph>`,
    /// such as [`Graph::query`], [`Graph::execute_script`], [`Graph::gql`],
    /// and [`Graph::gql_with_params`].
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::Graph;
    ///
    /// let graph = Graph::new_arc();
    /// // graph is Arc<Graph>; query/gql methods dispatch through the Arc receiver
    /// ```
    #[inline]
    pub fn new_arc() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Create a new in-memory graph with a schema for validation.
    ///
    /// Convenience alias for [`Graph::with_schema()`].
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::Graph;
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
    #[inline]
    pub fn in_memory_with_schema(schema: GraphSchema) -> Self {
        Self::with_schema(schema)
    }

    /// Create a new COW graph with a schema.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::Graph;
    /// use interstellar::schema::{SchemaBuilder, PropertyType, ValidationMode};
    ///
    /// let schema = SchemaBuilder::new()
    ///     .mode(ValidationMode::Strict)
    ///     .vertex("Person")
    ///         .property("name", PropertyType::String)
    ///         .done()
    ///     .build();
    ///
    /// let graph = Graph::with_schema(schema);
    /// ```
    pub fn with_schema(schema: GraphSchema) -> Self {
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        let event_bus = std::sync::Arc::new(crate::storage::events::EventBus::new());

        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        let subscription_manager = {
            let eb = event_bus.clone();
            let event_sub_fn: std::sync::Arc<
                dyn Fn() -> std::sync::mpsc::Receiver<crate::storage::events::GraphEvent>
                    + Send
                    + Sync,
            > = std::sync::Arc::new(move || eb.subscribe());

            std::sync::Arc::new(crate::traversal::reactive::SubscriptionManager::new(
                event_sub_fn,
            ))
        };

        Self {
            state: Arc::new(RwLock::new(GraphState::new())),
            schema: RwLock::new(Some(schema)),
            indexes: RwLock::new(HashMap::new()),
            #[cfg(feature = "full-text")]
            text_indexes_vertex: RwLock::new(HashMap::new()),
            #[cfg(feature = "full-text")]
            text_indexes_edge: RwLock::new(HashMap::new()),
            #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
            event_bus,
            #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
            subscription_manager,
        }
    }

    // =========================================================================
    // Snapshot Operations
    // =========================================================================

    /// Create a snapshot of the current graph state.
    ///
    /// This is an O(1) operation that creates a shared reference to the current
    /// state. The snapshot will not reflect any mutations made after this call.
    ///
    /// # Thread Safety
    ///
    /// This method briefly acquires a read lock to clone the state.
    /// The returned snapshot does not hold any locks.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::Graph;
    /// use interstellar::storage::GraphStorage;
    /// use std::collections::HashMap;
    ///
    /// let graph = Graph::new();
    /// let v1 = graph.add_vertex("person", HashMap::new());
    ///
    /// let snap = graph.snapshot();
    /// assert_eq!(snap.vertex_count(), 1);
    ///
    /// // Mutations after snapshot don't affect it
    /// graph.add_vertex("person", HashMap::new());
    /// assert_eq!(snap.vertex_count(), 1); // Still 1
    /// ```
    pub fn snapshot(&self) -> GraphSnapshot {
        let state = self.state.read();
        // Clone the interner to avoid shared lock issues
        let interner_snapshot = Arc::new(state.interner.read().clone());
        GraphSnapshot {
            state: Arc::new((*state).clone()),
            interner_snapshot,
            #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
            subscription_manager: self.subscription_manager.clone(),
            #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
            snapshot_fn: {
                let state_arc = self.state.clone();
                std::sync::Arc::new(move || {
                    let state = state_arc.read();
                    let interner_snapshot = Arc::new(state.interner.read().clone());
                    let snap = GraphSnapshot {
                        state: Arc::new((*state).clone()),
                        interner_snapshot,
                        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
                        subscription_manager: std::sync::Arc::new(
                            crate::traversal::reactive::SubscriptionManager::placeholder(),
                        ),
                        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
                        snapshot_fn: std::sync::Arc::new(|| {
                            panic!("nested snapshot_fn not supported")
                        }),
                    };
                    Box::new(snap)
                })
            },
        }
    }

    /// Create a mutable storage wrapper for this graph.
    ///
    /// Returns a [`GraphMutWrapper`] that implements both [`GraphStorage`] and
    /// [`GraphStorageMut`], allowing use with APIs that require mutable storage
    /// access (like the GQL mutation engine).
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::{Graph, GraphStorage, GraphStorageMut};
    /// use std::collections::HashMap;
    ///
    /// let graph = Graph::new();
    /// let mut wrapper = graph.as_storage_mut();
    ///
    /// // Use with APIs requiring GraphStorageMut
    /// let id = wrapper.add_vertex("person", HashMap::new());
    /// assert!(wrapper.get_vertex(id).is_some());
    /// ```
    ///
    /// [`GraphStorage`]: crate::storage::GraphStorage
    /// [`GraphStorageMut`]: crate::storage::GraphStorageMut
    pub fn as_storage_mut(&self) -> GraphMutWrapper<'_> {
        GraphMutWrapper { graph: self }
    }

    /// Create a Gremlin traversal source for this graph.
    ///
    /// The returned [`CowTraversalSource`] provides a fluent Gremlin-style API
    /// for both reads and mutations. Any mutations in the traversal are
    /// automatically executed when terminal steps are called.
    ///
    /// Terminal methods like `next()` and `to_list()` now return typed results:
    /// - `g.v().next()` returns `Option<GraphVertex>`
    /// - `g.e().next()` returns `Option<GraphEdge>`
    /// - `g.v().values("name").next()` returns `Option<Value>`
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::Graph;
    /// use std::sync::Arc;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let g = graph.gremlin(Arc::clone(&graph));
    ///
    /// // Create vertices - next() returns Option<GraphVertex>
    /// let alice = g.add_v("Person").property("name", "Alice").next();
    /// let bob = g.add_v("Person").property("name", "Bob").next();
    ///
    /// // Read - count() returns u64
    /// assert_eq!(g.v().count(), 2);
    /// ```
    pub fn gremlin(&self, graph_arc: Arc<Graph>) -> CowTraversalSource<'_> {
        CowTraversalSource::new_with_arc(self, graph_arc)
    }

    /// Create a typed traversal source for read-only traversals.
    ///
    /// The typed source returns `GraphVertex` and `GraphEdge` objects directly
    /// from terminal methods like `next()` and `to_list()`, without requiring
    /// an `Arc<Graph>` parameter.
    ///
    /// This is useful when you want type-safe traversals that track the output
    /// type at compile time.
    ///
    /// # Arguments
    ///
    /// * `graph_arc` - An Arc-wrapped reference to the same graph
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Alice".into()),
    /// ]));
    ///
    /// // Create typed traversal source
    /// let snapshot = graph.snapshot();
    /// let g = graph.typed_gremlin(&snapshot, Arc::clone(&graph));
    ///
    /// // next() returns Option<GraphVertex> directly
    /// let v = g.v().next().unwrap();
    /// assert_eq!(v.label(), Some("person".to_string()));
    /// ```
    pub fn typed_gremlin<'a>(
        &self,
        snapshot: &'a GraphSnapshot,
        graph_arc: Arc<Graph>,
    ) -> crate::traversal::typed::TypedTraversalSource<'a> {
        crate::traversal::typed::TypedTraversalSource::new(snapshot, graph_arc)
    }

    // =========================================================================
    // Read Operations (via current state)
    // =========================================================================

    /// Get a reference to the event bus for subscribing to mutation events.
    ///
    /// Only available when the `reactive` feature is enabled.
    #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
    pub fn event_bus(&self) -> &crate::storage::events::EventBus {
        &self.event_bus
    }

    /// Returns the subscription manager for reactive streaming queries.
    ///
    /// Only available when the `reactive` feature is enabled.
    #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
    pub fn subscription_manager(
        &self,
    ) -> &std::sync::Arc<crate::traversal::reactive::SubscriptionManager> {
        &self.subscription_manager
    }

    /// Returns the total number of vertices in the graph.
    pub fn vertex_count(&self) -> u64 {
        self.state.read().vertices.len() as u64
    }

    /// Returns the total number of edges in the graph.
    pub fn edge_count(&self) -> u64 {
        self.state.read().edges.len() as u64
    }

    /// Returns the current version number.
    pub fn version(&self) -> u64 {
        self.state.read().version
    }

    /// Get the current schema, if one is set.
    pub fn schema(&self) -> Option<GraphSchema> {
        self.schema.read().clone()
    }

    /// Set or replace the graph schema.
    pub fn set_schema(&self, schema: Option<GraphSchema>) {
        *self.schema.write() = schema;
    }

    // =========================================================================
    // Index Management
    // =========================================================================

    /// Creates a new property index and populates it with existing data.
    ///
    /// The index will be automatically maintained as vertices and edges
    /// are added, updated, or removed.
    ///
    /// # Arguments
    ///
    /// * `spec` - The index specification defining what to index
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::AlreadyExists`] if an index with the same name exists.
    /// Returns [`IndexError::DuplicateValue`] if creating a unique index and
    /// duplicate values exist.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::Graph;
    /// use interstellar::index::IndexBuilder;
    /// use std::collections::HashMap;
    ///
    /// let graph = Graph::new();
    ///
    /// // Add some data first
    /// graph.add_vertex("person", HashMap::from([
    ///     ("age".to_string(), 30i64.into()),
    /// ]));
    ///
    /// // Create a B+ tree index for range queries
    /// graph.create_index(
    ///     IndexBuilder::vertex()
    ///         .label("person")
    ///         .property("age")
    ///         .build()
    ///         .unwrap()
    /// ).unwrap();
    /// ```
    pub fn create_index(&self, spec: IndexSpec) -> Result<(), IndexError> {
        let mut indexes = self.indexes.write();

        // Check for duplicate name
        if indexes.contains_key(&spec.name) {
            return Err(IndexError::AlreadyExists(spec.name.clone()));
        }

        // Create the appropriate index type
        let mut index: Box<dyn PropertyIndex> = match spec.index_type {
            IndexType::BTree => Box::new(BTreeIndex::new(spec.clone())?),
            IndexType::Unique => Box::new(UniqueIndex::new(spec.clone())?),
            IndexType::RTree => Box::new(RTreeIndex::new(spec.clone())?),
        };

        // Populate index with existing data
        let state = self.state.read();
        Self::populate_index_internal(&state, &mut *index)?;

        indexes.insert(spec.name.clone(), index);
        Ok(())
    }

    /// Drops an index by name.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::NotFound`] if no index with that name exists.
    pub fn drop_index(&self, name: &str) -> Result<(), IndexError> {
        self.indexes
            .write()
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| IndexError::NotFound(name.to_string()))
    }

    /// Returns a vector of all index specifications.
    pub fn list_indexes(&self) -> Vec<IndexSpec> {
        self.indexes
            .read()
            .values()
            .map(|idx| idx.spec().clone())
            .collect()
    }

    /// Checks if an index with the given name exists.
    pub fn has_index(&self, name: &str) -> bool {
        self.indexes.read().contains_key(name)
    }

    /// Returns the number of indexes.
    pub fn index_count(&self) -> usize {
        self.indexes.read().len()
    }

    /// Returns whether this storage supports indexes.
    pub fn supports_indexes(&self) -> bool {
        true
    }

    // =========================================================================
    // Text Index Operations (full-text feature)
    // =========================================================================

    /// Register a Tantivy-backed full-text index on the given **vertex**
    /// property.
    ///
    /// Subsequent `add_vertex` / `set_vertex_property` / `remove_vertex` calls
    /// that touch this property will be reflected in the index. Existing
    /// vertices that already have a string value at `property` are
    /// back-filled into the index synchronously before this method returns.
    ///
    /// # Errors
    ///
    /// - [`TextIndexError::Storage`] (with [`StorageError::IndexError`])
    ///   if a text index for `property` already exists on **either**
    ///   vertices or edges (property names are globally unique across both
    ///   element types).
    /// - Any error returned by the Tantivy backend during construction or
    ///   back-fill.
    #[cfg(feature = "full-text")]
    pub fn create_text_index_v(
        &self,
        property: &str,
        config: crate::storage::text::TextIndexConfig,
    ) -> Result<(), crate::storage::text::TextIndexError> {
        self.create_text_index_inner(crate::index::ElementType::Vertex, property, config)
    }

    /// Register a Tantivy-backed full-text index on the given **edge**
    /// property.
    ///
    /// Subsequent `add_edge` / `set_edge_property` / `remove_edge` calls that
    /// touch this property will be reflected in the index. Existing edges
    /// that already have a string value at `property` are back-filled into
    /// the index synchronously before this method returns.
    ///
    /// # Errors
    ///
    /// Same as [`Self::create_text_index_v`]. Note in particular that you
    /// cannot register an edge index for a `property` that already has a
    /// vertex index (or vice versa).
    #[cfg(feature = "full-text")]
    pub fn create_text_index_e(
        &self,
        property: &str,
        config: crate::storage::text::TextIndexConfig,
    ) -> Result<(), crate::storage::text::TextIndexError> {
        self.create_text_index_inner(crate::index::ElementType::Edge, property, config)
    }

    #[cfg(feature = "full-text")]
    fn create_text_index_inner(
        &self,
        element_type: crate::index::ElementType,
        property: &str,
        config: crate::storage::text::TextIndexConfig,
    ) -> Result<(), crate::storage::text::TextIndexError> {
        use crate::index::ElementType;
        use crate::storage::text::{TantivyTextIndex, TextIndex, TextIndexError};

        // Take BOTH locks under a consistent order (vertex first, then edge)
        // so we can enforce the global property-name uniqueness invariant
        // without races. We hold both for the duration of construction +
        // back-fill so no concurrent `create_text_index_*` can squeeze in
        // between the duplicate check and the insert.
        let mut vmap = self.text_indexes_vertex.write();
        let mut emap = self.text_indexes_edge.write();

        if vmap.contains_key(property) || emap.contains_key(property) {
            return Err(TextIndexError::Storage(StorageError::IndexError(format!(
                "text index already exists for property `{property}` (property names are \
                 globally unique across vertex and edge indexes)"
            ))));
        }

        let index = TantivyTextIndex::in_memory(element_type, config)?;
        let arc: std::sync::Arc<dyn TextIndex> = std::sync::Arc::new(index);

        // Back-fill from the corresponding element collection.
        let state = self.state.read();
        match element_type {
            ElementType::Vertex => {
                for (vid, node) in state.vertices.iter() {
                    if let Some(Value::String(s)) = node.properties.get(property) {
                        arc.upsert(vid.0, s.as_str())?;
                    }
                }
            }
            ElementType::Edge => {
                for (eid, edge) in state.edges.iter() {
                    if let Some(Value::String(s)) = edge.properties.get(property) {
                        arc.upsert(eid.0, s.as_str())?;
                    }
                }
            }
        }
        drop(state);

        // Make the back-fill visible to searchers.
        arc.commit()?;

        match element_type {
            ElementType::Vertex => {
                vmap.insert(property.to_string(), arc);
            }
            ElementType::Edge => {
                emap.insert(property.to_string(), arc);
            }
        }
        Ok(())
    }

    /// Drop the **vertex** text index registered for `property`.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::IndexError`] (wrapped in `TextIndexError::Storage`)
    /// if no vertex text index is registered for `property`.
    #[cfg(feature = "full-text")]
    pub fn drop_text_index_v(
        &self,
        property: &str,
    ) -> Result<(), crate::storage::text::TextIndexError> {
        use crate::storage::text::TextIndexError;

        self.text_indexes_vertex
            .write()
            .remove(property)
            .map(|_| ())
            .ok_or_else(|| {
                TextIndexError::Storage(StorageError::IndexError(format!(
                    "no vertex text index registered for property `{property}`"
                )))
            })
    }

    /// Drop the **edge** text index registered for `property`.
    #[cfg(feature = "full-text")]
    pub fn drop_text_index_e(
        &self,
        property: &str,
    ) -> Result<(), crate::storage::text::TextIndexError> {
        use crate::storage::text::TextIndexError;

        self.text_indexes_edge
            .write()
            .remove(property)
            .map(|_| ())
            .ok_or_else(|| {
                TextIndexError::Storage(StorageError::IndexError(format!(
                    "no edge text index registered for property `{property}`"
                )))
            })
    }

    /// Returns a clone of the `Arc<dyn TextIndex>` registered for `property`
    /// on **vertices**, or `None` if none is registered.
    #[cfg(feature = "full-text")]
    pub fn text_index_v(
        &self,
        property: &str,
    ) -> Option<std::sync::Arc<dyn crate::storage::text::TextIndex>> {
        self.text_indexes_vertex.read().get(property).cloned()
    }

    /// Returns a clone of the `Arc<dyn TextIndex>` registered for `property`
    /// on **edges**, or `None` if none is registered.
    #[cfg(feature = "full-text")]
    pub fn text_index_e(
        &self,
        property: &str,
    ) -> Option<std::sync::Arc<dyn crate::storage::text::TextIndex>> {
        self.text_indexes_edge.read().get(property).cloned()
    }

    /// Returns `true` iff a vertex text index is registered for `property`.
    #[cfg(feature = "full-text")]
    pub fn has_text_index_v(&self, property: &str) -> bool {
        self.text_indexes_vertex.read().contains_key(property)
    }

    /// Returns `true` iff an edge text index is registered for `property`.
    #[cfg(feature = "full-text")]
    pub fn has_text_index_e(&self, property: &str) -> bool {
        self.text_indexes_edge.read().contains_key(property)
    }

    /// Returns the names of all properties that currently have a **vertex**
    /// text index.
    #[cfg(feature = "full-text")]
    pub fn list_text_indexes_v(&self) -> Vec<String> {
        self.text_indexes_vertex.read().keys().cloned().collect()
    }

    /// Returns the names of all properties that currently have an **edge**
    /// text index.
    #[cfg(feature = "full-text")]
    pub fn list_text_indexes_e(&self) -> Vec<String> {
        self.text_indexes_edge.read().keys().cloned().collect()
    }

    /// Returns the number of registered vertex text indexes.
    #[cfg(feature = "full-text")]
    pub fn text_index_count_v(&self) -> usize {
        self.text_indexes_vertex.read().len()
    }

    /// Returns the number of registered edge text indexes.
    #[cfg(feature = "full-text")]
    pub fn text_index_count_e(&self) -> usize {
        self.text_indexes_edge.read().len()
    }

    // =========================================================================
    // Index Query Methods
    // =========================================================================

    /// Lookup vertices by indexed property value.
    ///
    /// If an applicable index exists, uses it for O(log n) or O(1) lookup.
    /// Otherwise falls back to O(n) scan.
    pub fn vertices_by_property(
        &self,
        label: Option<&str>,
        property: &str,
        value: &Value,
    ) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let indexes = self.indexes.read();
        let state = self.state.read();

        // Try to find an applicable index
        for index in indexes.values() {
            let spec = index.spec();
            if spec.element_type != ElementType::Vertex {
                continue;
            }
            if spec.property != property {
                continue;
            }
            // Check label compatibility
            match (&spec.label, label) {
                (Some(idx_label), Some(filter_label)) if idx_label != filter_label => continue,
                (Some(_), None) => continue, // Index is label-specific, query is not
                _ => {}
            }

            // Use index
            let ids: Vec<u64> = index.lookup_eq(value).collect();
            let label_owned = label.map(|s| s.to_string());

            // Need to release locks and collect results
            drop(indexes);
            drop(state);

            return Box::new(
                ids.into_iter()
                    .filter_map(move |id| {
                        let state = self.state.read();
                        let node = state.vertices.get(&VertexId(id))?;
                        let label = state.interner.read().resolve(node.label_id)?.to_string();
                        Some(Vertex {
                            id: node.id,
                            label,
                            properties: node.properties.clone(),
                        })
                    })
                    .filter(move |v| {
                        label_owned.is_none() || Some(v.label.as_str()) == label_owned.as_deref()
                    }),
            );
        }

        // Fall back to scan - need to release locks first
        drop(indexes);
        drop(state);

        let label_owned = label.map(|s| s.to_string());
        let property_owned = property.to_string();
        let value_clone = value.clone();

        // Collect to avoid lifetime issues with snapshot
        let vertices: Vec<Vertex> = self
            .snapshot()
            .all_vertices()
            .filter(move |v| {
                if let Some(ref l) = label_owned {
                    if &v.label != l {
                        return false;
                    }
                }
                v.properties.get(&property_owned) == Some(&value_clone)
            })
            .collect();

        Box::new(vertices.into_iter())
    }

    /// Lookup edges by indexed property value.
    ///
    /// If an applicable index exists, uses it for O(log n) or O(1) lookup.
    /// Otherwise falls back to O(n) scan.
    pub fn edges_by_property(
        &self,
        label: Option<&str>,
        property: &str,
        value: &Value,
    ) -> Box<dyn Iterator<Item = Edge> + '_> {
        let indexes = self.indexes.read();
        let state = self.state.read();

        // Try to find an applicable index
        for index in indexes.values() {
            let spec = index.spec();
            if spec.element_type != ElementType::Edge {
                continue;
            }
            if spec.property != property {
                continue;
            }
            // Check label compatibility
            match (&spec.label, label) {
                (Some(idx_label), Some(filter_label)) if idx_label != filter_label => continue,
                (Some(_), None) => continue, // Index is label-specific, query is not
                _ => {}
            }

            // Use index
            let ids: Vec<u64> = index.lookup_eq(value).collect();
            let label_owned = label.map(|s| s.to_string());

            // Need to release locks and collect results
            drop(indexes);
            drop(state);

            return Box::new(
                ids.into_iter()
                    .filter_map(move |id| {
                        let state = self.state.read();
                        let edge = state.edges.get(&EdgeId(id))?;
                        let label = state.interner.read().resolve(edge.label_id)?.to_string();
                        Some(Edge {
                            id: edge.id,
                            label,
                            src: edge.src,
                            dst: edge.dst,
                            properties: edge.properties.clone(),
                        })
                    })
                    .filter(move |e| {
                        label_owned.is_none() || Some(e.label.as_str()) == label_owned.as_deref()
                    }),
            );
        }

        // Fall back to scan - need to release locks first
        drop(indexes);
        drop(state);

        let label_owned = label.map(|s| s.to_string());
        let property_owned = property.to_string();
        let value_clone = value.clone();

        // Collect to avoid lifetime issues with snapshot
        let edges: Vec<Edge> = self
            .snapshot()
            .all_edges()
            .filter(move |e| {
                if let Some(ref l) = label_owned {
                    if &e.label != l {
                        return false;
                    }
                }
                e.properties.get(&property_owned) == Some(&value_clone)
            })
            .collect();

        Box::new(edges.into_iter())
    }

    /// Lookup vertices by property range, using indexes if available.
    pub fn vertices_by_property_range(
        &self,
        label: Option<&str>,
        property: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let indexes = self.indexes.read();

        // Try to find an applicable BTree index
        for index in indexes.values() {
            let spec = index.spec();
            if spec.element_type != ElementType::Vertex {
                continue;
            }
            if spec.property != property {
                continue;
            }
            // BTree indexes support range queries; skip unique indexes
            if spec.index_type != IndexType::BTree {
                continue;
            }
            // Check label compatibility
            match (&spec.label, label) {
                (Some(idx_label), Some(filter_label)) if idx_label != filter_label => continue,
                (Some(_), None) => continue, // Index is label-specific, query is not
                _ => {}
            }

            // Use index for range lookup
            let ids: Vec<u64> = index.lookup_range(start, end).collect();
            let label_owned = label.map(|s| s.to_string());

            drop(indexes);

            return Box::new(
                ids.into_iter()
                    .filter_map(move |id| {
                        let state = self.state.read();
                        let node = state.vertices.get(&VertexId(id))?;
                        let label = state.interner.read().resolve(node.label_id)?.to_string();
                        Some(Vertex {
                            id: node.id,
                            label,
                            properties: node.properties.clone(),
                        })
                    })
                    .filter(move |v| {
                        label_owned.is_none() || Some(v.label.as_str()) == label_owned.as_deref()
                    }),
            );
        }

        // Fall back to scan with range filter
        drop(indexes);

        let label_owned = label.map(|s| s.to_string());
        let property_owned = property.to_string();
        let start_clone = match start {
            Bound::Included(v) => Bound::Included(v.clone()),
            Bound::Excluded(v) => Bound::Excluded(v.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_clone = match end {
            Bound::Included(v) => Bound::Included(v.clone()),
            Bound::Excluded(v) => Bound::Excluded(v.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        // Collect to avoid lifetime issues with snapshot
        let vertices: Vec<Vertex> = self
            .snapshot()
            .all_vertices()
            .filter(move |v| {
                if let Some(ref l) = label_owned {
                    if &v.label != l {
                        return false;
                    }
                }
                if let Some(prop_value) = v.properties.get(&property_owned) {
                    let prop_cmp = prop_value.to_comparable();
                    let in_start = match &start_clone {
                        Bound::Included(s) => prop_cmp >= s.to_comparable(),
                        Bound::Excluded(s) => prop_cmp > s.to_comparable(),
                        Bound::Unbounded => true,
                    };
                    let in_end = match &end_clone {
                        Bound::Included(e) => prop_cmp <= e.to_comparable(),
                        Bound::Excluded(e) => prop_cmp < e.to_comparable(),
                        Bound::Unbounded => true,
                    };
                    in_start && in_end
                } else {
                    false
                }
            })
            .collect();

        Box::new(vertices.into_iter())
    }

    // =========================================================================
    // Internal Index Helpers
    // =========================================================================

    /// Populate an index with existing graph data.
    fn populate_index_internal(
        state: &GraphState,
        index: &mut dyn PropertyIndex,
    ) -> Result<(), IndexError> {
        let spec = index.spec().clone();

        match spec.element_type {
            ElementType::Vertex => {
                for (id, node) in state.vertices.iter() {
                    // Check label filter
                    if let Some(ref label) = spec.label {
                        let node_label = state
                            .interner
                            .read()
                            .resolve(node.label_id)
                            .map(|s| s.to_string());
                        if node_label.as_deref() != Some(label.as_str()) {
                            continue;
                        }
                    }

                    // Get property value
                    if let Some(value) = node.properties.get(&spec.property) {
                        index.insert(value.clone(), id.0)?;
                    }
                }
            }
            ElementType::Edge => {
                for (id, edge) in state.edges.iter() {
                    // Check label filter
                    if let Some(ref label) = spec.label {
                        let edge_label = state
                            .interner
                            .read()
                            .resolve(edge.label_id)
                            .map(|s| s.to_string());
                        if edge_label.as_deref() != Some(label.as_str()) {
                            continue;
                        }
                    }

                    // Get property value
                    if let Some(value) = edge.properties.get(&spec.property) {
                        index.insert(value.clone(), id.0)?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Update indexes when a vertex is added.
    fn index_vertex_insert(&self, id: VertexId, label: &str, properties: &HashMap<String, Value>) {
        let mut indexes = self.indexes.write();
        for index in indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Vertex {
                continue;
            }
            if let Some(ref idx_label) = spec.label {
                if idx_label != label {
                    continue;
                }
            }
            if let Some(value) = properties.get(&spec.property) {
                // Ignore errors for BTree (no constraint), log for unique
                let _ = index.insert(value.clone(), id.0);
            }
        }
    }

    /// Update indexes when a vertex is removed.
    fn index_vertex_remove(&self, id: VertexId, label: &str, properties: &HashMap<String, Value>) {
        let mut indexes = self.indexes.write();
        for index in indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Vertex {
                continue;
            }
            if let Some(ref idx_label) = spec.label {
                if idx_label != label {
                    continue;
                }
            }
            if let Some(value) = properties.get(&spec.property) {
                let _ = index.remove(value, id.0);
            }
        }
    }

    /// Update indexes when an edge is added.
    fn index_edge_insert(&self, id: EdgeId, label: &str, properties: &HashMap<String, Value>) {
        let mut indexes = self.indexes.write();
        for index in indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Edge {
                continue;
            }
            if let Some(ref idx_label) = spec.label {
                if idx_label != label {
                    continue;
                }
            }
            if let Some(value) = properties.get(&spec.property) {
                let _ = index.insert(value.clone(), id.0);
            }
        }
    }

    /// Update indexes when an edge is removed.
    fn index_edge_remove(&self, id: EdgeId, label: &str, properties: &HashMap<String, Value>) {
        let mut indexes = self.indexes.write();
        for index in indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Edge {
                continue;
            }
            if let Some(ref idx_label) = spec.label {
                if idx_label != label {
                    continue;
                }
            }
            if let Some(value) = properties.get(&spec.property) {
                let _ = index.remove(value, id.0);
            }
        }
    }

    /// Update indexes when a vertex property changes.
    fn update_vertex_property_in_indexes(
        &self,
        id: VertexId,
        label: &str,
        property: &str,
        old_value: Option<&Value>,
        new_value: &Value,
    ) -> Result<(), StorageError> {
        let mut indexes = self.indexes.write();
        for index in indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Vertex {
                continue;
            }
            if spec.property != property {
                continue;
            }
            if let Some(ref idx_label) = spec.label {
                if idx_label != label {
                    continue;
                }
            }

            // Remove old value, insert new
            if let Some(old) = old_value {
                let _ = index.remove(old, id.0);
            }
            index
                .insert(new_value.clone(), id.0)
                .map_err(|e| StorageError::IndexError(e.to_string()))?;
        }
        Ok(())
    }

    /// Update indexes when an edge property changes.
    fn update_edge_property_in_indexes(
        &self,
        id: EdgeId,
        label: &str,
        property: &str,
        old_value: Option<&Value>,
        new_value: &Value,
    ) -> Result<(), StorageError> {
        let mut indexes = self.indexes.write();
        for index in indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Edge {
                continue;
            }
            if spec.property != property {
                continue;
            }
            if let Some(ref idx_label) = spec.label {
                if idx_label != label {
                    continue;
                }
            }

            // Remove old value, insert new
            if let Some(old) = old_value {
                let _ = index.remove(old, id.0);
            }
            index
                .insert(new_value.clone(), id.0)
                .map_err(|e| StorageError::IndexError(e.to_string()))?;
        }
        Ok(())
    }

    // =========================================================================
    // Text Index Mutation Hooks (full-text feature)
    // =========================================================================

    /// Update text indexes when a vertex is added.
    ///
    /// Iterates over all registered text indexes and upserts the document for
    /// any index whose property is present (and a `Value::String`) on the new
    /// vertex. Non-string values are silently ignored — see the spec for
    /// future strict-mode behavior.
    #[cfg(feature = "full-text")]
    fn text_index_vertex_insert(
        &self,
        id: VertexId,
        properties: &HashMap<String, Value>,
    ) -> Result<(), crate::storage::text::TextIndexError> {
        let indexes = self.text_indexes_vertex.read();
        for (prop, idx) in indexes.iter() {
            if let Some(Value::String(s)) = properties.get(prop) {
                idx.upsert(id.0, s.as_str())?;
                idx.commit()?;
            }
        }
        Ok(())
    }

    /// Update text indexes when a vertex is removed.
    ///
    /// Calls `delete(id)` on every registered text index. The index treats
    /// deletion of a missing vertex as a no-op, so it is safe to call without
    /// inspecting the vertex's properties.
    #[cfg(feature = "full-text")]
    fn text_index_vertex_remove(
        &self,
        id: VertexId,
    ) -> Result<(), crate::storage::text::TextIndexError> {
        let indexes = self.text_indexes_vertex.read();
        for idx in indexes.values() {
            idx.delete(id.0)?;
            idx.commit()?;
        }
        Ok(())
    }

    /// Update the text index for a specific property when its value changes
    /// on a single vertex.
    ///
    /// If `new_value` is a `Value::String` and a text index exists for
    /// `property`, the index entry is upserted. If `new_value` is not a string
    /// (e.g. the property was overwritten with a non-text value), any
    /// existing entry is deleted to keep the index consistent.
    ///
    /// If no text index is registered for `property`, this is a no-op.
    #[cfg(feature = "full-text")]
    fn text_index_property_update(
        &self,
        id: VertexId,
        property: &str,
        new_value: &Value,
    ) -> Result<(), crate::storage::text::TextIndexError> {
        let indexes = self.text_indexes_vertex.read();
        let Some(idx) = indexes.get(property) else {
            return Ok(());
        };
        match new_value {
            Value::String(s) => {
                idx.upsert(id.0, s.as_str())?;
                idx.commit()
            }
            _ => {
                idx.delete(id.0)?;
                idx.commit()
            }
        }
    }

    /// Update edge text indexes when an edge is added.
    ///
    /// Mirrors [`Self::text_index_vertex_insert`] for the edge map.
    #[cfg(feature = "full-text")]
    fn text_index_edge_insert(
        &self,
        id: EdgeId,
        properties: &HashMap<String, Value>,
    ) -> Result<(), crate::storage::text::TextIndexError> {
        let indexes = self.text_indexes_edge.read();
        for (prop, idx) in indexes.iter() {
            if let Some(Value::String(s)) = properties.get(prop) {
                idx.upsert(id.0, s.as_str())?;
                idx.commit()?;
            }
        }
        Ok(())
    }

    /// Update edge text indexes when an edge is removed.
    ///
    /// Mirrors [`Self::text_index_vertex_remove`] for the edge map.
    #[cfg(feature = "full-text")]
    fn text_index_edge_remove(
        &self,
        id: EdgeId,
    ) -> Result<(), crate::storage::text::TextIndexError> {
        let indexes = self.text_indexes_edge.read();
        for idx in indexes.values() {
            idx.delete(id.0)?;
            idx.commit()?;
        }
        Ok(())
    }

    /// Update an edge text index for a specific property when its value
    /// changes on a single edge. Mirrors [`Self::text_index_property_update`].
    #[cfg(feature = "full-text")]
    fn text_index_edge_property_update(
        &self,
        id: EdgeId,
        property: &str,
        new_value: &Value,
    ) -> Result<(), crate::storage::text::TextIndexError> {
        let indexes = self.text_indexes_edge.read();
        let Some(idx) = indexes.get(property) else {
            return Ok(());
        };
        match new_value {
            Value::String(s) => {
                idx.upsert(id.0, s.as_str())?;
                idx.commit()
            }
            _ => {
                idx.delete(id.0)?;
                idx.commit()
            }
        }
    }

    // =========================================================================
    // Mutation Operations
    // =========================================================================

    /// Add a vertex to the graph.
    ///
    /// This triggers copy-on-write: only the modified paths in the persistent
    /// data structure are copied. Existing snapshots continue to see the old state.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::Graph;
    /// use interstellar::storage::GraphStorage;
    /// use std::collections::HashMap;
    ///
    /// let graph = Graph::new();
    /// let id = graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Alice".into()),
    /// ]));
    ///
    /// let snap = graph.snapshot();
    /// let vertex = snap.get_vertex(id).unwrap();
    /// assert_eq!(vertex.label, "person");
    /// ```
    pub fn add_vertex(&self, label: &str, properties: HashMap<String, Value>) -> VertexId {
        let mut state = self.state.write();

        // Allocate ID
        let id = VertexId(state.next_vertex_id);
        state.next_vertex_id += 1;

        // Intern label
        let label_id = state.interner.write().intern(label);

        // Create node
        let node = Arc::new(NodeData {
            id,
            label_id,
            properties: properties.clone(),
            out_edges: Vec::new(),
            in_edges: Vec::new(),
        });

        // Insert into persistent map (O(log n), structural sharing)
        state.vertices = state.vertices.update(id, node);

        // Update label index
        let bitmap = state
            .vertex_labels
            .get(&label_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(RoaringBitmap::new()));
        let mut new_bitmap = (*bitmap).clone();
        new_bitmap.insert(id.0 as u32);
        state.vertex_labels = state.vertex_labels.update(label_id, Arc::new(new_bitmap));

        // Increment version
        state.version += 1;

        // Release state lock before updating indexes (to avoid deadlock)
        drop(state);

        // Update property indexes
        self.index_vertex_insert(id, label, &properties);

        // Update text indexes (full-text feature). Errors are intentionally
        // dropped to keep `add_vertex`'s infallible signature; consistent with
        // how unique-index errors are handled above. A future strict mode
        // (see spec-55) may surface these as `Result`-typed mutations.
        #[cfg(feature = "full-text")]
        {
            let _ = self.text_index_vertex_insert(id, &properties);
        }

        // Emit reactive event
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        if self.event_bus.subscriber_count() > 0 {
            self.event_bus.emit(crate::storage::events::GraphEvent::VertexAdded {
                id,
                label: label.to_string(),
                properties,
            });
        }

        id
    }

    /// Add an edge between two vertices.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::VertexNotFound` if either vertex doesn't exist.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::Graph;
    /// use interstellar::storage::GraphStorage;
    /// use std::collections::HashMap;
    ///
    /// let graph = Graph::new();
    /// let alice = graph.add_vertex("person", HashMap::new());
    /// let bob = graph.add_vertex("person", HashMap::new());
    ///
    /// let edge = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    ///
    /// let snap = graph.snapshot();
    /// let e = snap.get_edge(edge).unwrap();
    /// assert_eq!(e.src, alice);
    /// assert_eq!(e.dst, bob);
    /// ```
    pub fn add_edge(
        &self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError> {
        let mut state = self.state.write();

        // Verify vertices exist
        if !state.vertices.contains_key(&src) {
            return Err(StorageError::VertexNotFound(src));
        }
        if !state.vertices.contains_key(&dst) {
            return Err(StorageError::VertexNotFound(dst));
        }

        // Allocate edge ID
        let edge_id = EdgeId(state.next_edge_id);
        state.next_edge_id += 1;

        // Intern label
        let label_id = state.interner.write().intern(label);

        // Create edge
        let edge = Arc::new(EdgeData {
            id: edge_id,
            label_id,
            src,
            dst,
            properties: properties.clone(),
        });

        // Insert edge
        state.edges = state.edges.update(edge_id, edge);

        // Update source vertex's out_edges
        if let Some(src_node) = state.vertices.get(&src) {
            let mut new_src = (**src_node).clone();
            new_src.out_edges.push(edge_id);
            state.vertices = state.vertices.update(src, Arc::new(new_src));
        }

        // Update destination vertex's in_edges
        if let Some(dst_node) = state.vertices.get(&dst) {
            let mut new_dst = (**dst_node).clone();
            new_dst.in_edges.push(edge_id);
            state.vertices = state.vertices.update(dst, Arc::new(new_dst));
        }

        // Update label index
        let bitmap = state
            .edge_labels
            .get(&label_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(RoaringBitmap::new()));
        let mut new_bitmap = (*bitmap).clone();
        new_bitmap.insert(edge_id.0 as u32);
        state.edge_labels = state.edge_labels.update(label_id, Arc::new(new_bitmap));

        state.version += 1;

        // Release state lock before updating indexes (to avoid deadlock)
        drop(state);

        // Update property indexes
        self.index_edge_insert(edge_id, label, &properties);

        // Update edge text indexes (full-text feature). Errors surface as
        // `StorageError::IndexError` since `add_edge` already returns Result.
        #[cfg(feature = "full-text")]
        {
            self.text_index_edge_insert(edge_id, &properties)
                .map_err(|e| StorageError::IndexError(e.to_string()))?;
        }

        // Emit reactive event
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        if self.event_bus.subscriber_count() > 0 {
            self.event_bus.emit(crate::storage::events::GraphEvent::EdgeAdded {
                id: edge_id,
                src,
                dst,
                label: label.to_string(),
                properties,
            });
        }

        Ok(edge_id)
    }

    /// Update a vertex's property.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::VertexNotFound` if the vertex doesn't exist.
    pub fn set_vertex_property(
        &self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write();

        let node = state
            .vertices
            .get(&id)
            .ok_or(StorageError::VertexNotFound(id))?;

        // Get label and old value for index update
        let label = state
            .interner
            .read()
            .resolve(node.label_id)
            .map(|s| s.to_string())
            .unwrap_or_default();
        let old_value = node.properties.get(key).cloned();

        // Clone and modify the node
        let mut new_node = (**node).clone();
        new_node.properties.insert(key.to_string(), value.clone());

        // Update in persistent map
        state.vertices = state.vertices.update(id, Arc::new(new_node));
        state.version += 1;

        // Release state lock before updating indexes
        drop(state);

        // Update property indexes
        self.update_vertex_property_in_indexes(id, &label, key, old_value.as_ref(), &value)?;

        // Update text indexes (full-text feature). Surfaces backend errors as
        // `StorageError::IndexError` since `set_vertex_property` already
        // returns `Result`.
        #[cfg(feature = "full-text")]
        {
            self.text_index_property_update(id, key, &value)
                .map_err(|e| StorageError::IndexError(e.to_string()))?;
        }

        // Emit reactive event
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        if self.event_bus.subscriber_count() > 0 {
            self.event_bus.emit(crate::storage::events::GraphEvent::VertexPropertyChanged {
                id,
                key: key.to_string(),
                old_value,
                new_value: value,
            });
        }

        Ok(())
    }

    /// Update an edge's property.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::EdgeNotFound` if the edge doesn't exist.
    pub fn set_edge_property(
        &self,
        id: EdgeId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write();

        let edge = state.edges.get(&id).ok_or(StorageError::EdgeNotFound(id))?;

        // Get label and old value for index update
        let label = state
            .interner
            .read()
            .resolve(edge.label_id)
            .map(|s| s.to_string())
            .unwrap_or_default();
        let old_value = edge.properties.get(key).cloned();

        // Clone and modify the edge
        let mut new_edge = (**edge).clone();
        new_edge.properties.insert(key.to_string(), value.clone());

        // Update in persistent map
        state.edges = state.edges.update(id, Arc::new(new_edge));
        state.version += 1;

        // Release state lock before updating indexes
        drop(state);

        // Update property indexes
        self.update_edge_property_in_indexes(id, &label, key, old_value.as_ref(), &value)?;

        // Update edge text indexes (full-text feature).
        #[cfg(feature = "full-text")]
        {
            self.text_index_edge_property_update(id, key, &value)
                .map_err(|e| StorageError::IndexError(e.to_string()))?;
        }

        // Emit reactive event
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        if self.event_bus.subscriber_count() > 0 {
            self.event_bus.emit(crate::storage::events::GraphEvent::EdgePropertyChanged {
                id,
                key: key.to_string(),
                old_value,
                new_value: value,
            });
        }

        Ok(())
    }

    /// Remove a vertex and all its incident edges.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::VertexNotFound` if the vertex doesn't exist.
    pub fn remove_vertex(&self, id: VertexId) -> Result<(), StorageError> {
        let mut state = self.state.write();

        let node = state
            .vertices
            .get(&id)
            .ok_or(StorageError::VertexNotFound(id))?
            .clone();

        // Get label for index removal
        let label = state
            .interner
            .read()
            .resolve(node.label_id)
            .map(|s| s.to_string())
            .unwrap_or_default();
        let properties = node.properties.clone();

        // Collect edges to remove with their info for index updates
        #[allow(clippy::type_complexity)]
        let edges_to_remove: Vec<(EdgeId, VertexId, VertexId, String, HashMap<String, Value>)> = node
            .out_edges
            .iter()
            .chain(node.in_edges.iter())
            .filter_map(|&edge_id| {
                state.edges.get(&edge_id).map(|e| {
                    let edge_label = state
                        .interner
                        .read()
                        .resolve(e.label_id)
                        .map(|s| s.to_string())
                        .unwrap_or_default();
                    (edge_id, e.src, e.dst, edge_label, e.properties.clone())
                })
            })
            .collect();

        // Remove from vertex storage
        state.vertices = state.vertices.without(&id);

        // Remove from label index
        if let Some(bitmap) = state.vertex_labels.get(&node.label_id) {
            let mut new_bitmap = (**bitmap).clone();
            new_bitmap.remove(id.0 as u32);
            if new_bitmap.is_empty() {
                state.vertex_labels = state.vertex_labels.without(&node.label_id);
            } else {
                state.vertex_labels = state
                    .vertex_labels
                    .update(node.label_id, Arc::new(new_bitmap));
            }
        }

        // Remove all incident edges
        for (edge_id, _, _, _, _) in &edges_to_remove {
            Self::remove_edge_internal(&mut state, *edge_id, Some(id));
        }

        state.version += 1;

        // Release state lock before updating indexes
        drop(state);

        // Update property indexes - remove vertex
        self.index_vertex_remove(id, &label, &properties);

        // Update text indexes (full-text feature). Errors swallowed to keep
        // remove semantics: the vertex is already gone from canonical state.
        #[cfg(feature = "full-text")]
        {
            let _ = self.text_index_vertex_remove(id);
        }

        // Update property indexes - remove edges
        for (edge_id, _edge_src, _edge_dst, edge_label, edge_props) in &edges_to_remove {
            self.index_edge_remove(*edge_id, edge_label, edge_props);

            // Update edge text indexes for cascaded edges.
            #[cfg(feature = "full-text")]
            {
                let _ = self.text_index_edge_remove(*edge_id);
            }
        }

        // Emit reactive events
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        if self.event_bus.subscriber_count() > 0 {
            for (edge_id, edge_src, edge_dst, edge_label, _) in edges_to_remove {
                self.event_bus.emit(crate::storage::events::GraphEvent::EdgeRemoved {
                    id: edge_id,
                    src: edge_src,
                    dst: edge_dst,
                    label: edge_label,
                });
            }
            self.event_bus.emit(crate::storage::events::GraphEvent::VertexRemoved {
                id,
                label,
            });
        }

        Ok(())
    }

    /// Remove an edge from the graph.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::EdgeNotFound` if the edge doesn't exist.
    pub fn remove_edge(&self, id: EdgeId) -> Result<(), StorageError> {
        let mut state = self.state.write();

        let edge = state.edges.get(&id).ok_or(StorageError::EdgeNotFound(id))?;

        // Get label, endpoints, and properties for index removal / reactive events
        let label = state
            .interner
            .read()
            .resolve(edge.label_id)
            .map(|s| s.to_string())
            .unwrap_or_default();
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        let (src, dst) = (edge.src, edge.dst);
        let properties = edge.properties.clone();

        Self::remove_edge_internal(&mut state, id, None);
        state.version += 1;

        // Release state lock before updating indexes
        drop(state);

        // Update property indexes
        self.index_edge_remove(id, &label, &properties);

        // Update edge text indexes. Errors swallowed: edge is already gone
        // from canonical state, mirroring vertex remove semantics.
        #[cfg(feature = "full-text")]
        {
            let _ = self.text_index_edge_remove(id);
        }

        // Emit reactive event
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        if self.event_bus.subscriber_count() > 0 {
            self.event_bus.emit(crate::storage::events::GraphEvent::EdgeRemoved {
                id, src, dst, label,
            });
        }

        Ok(())
    }

    /// Internal edge removal, optionally skipping a vertex being deleted.
    fn remove_edge_internal(state: &mut GraphState, id: EdgeId, skip_vertex: Option<VertexId>) {
        let Some(edge) = state.edges.get(&id).cloned() else {
            return;
        };

        // Remove from edge storage
        state.edges = state.edges.without(&id);

        // Remove from label index
        if let Some(bitmap) = state.edge_labels.get(&edge.label_id) {
            let mut new_bitmap = (**bitmap).clone();
            new_bitmap.remove(id.0 as u32);
            if new_bitmap.is_empty() {
                state.edge_labels = state.edge_labels.without(&edge.label_id);
            } else {
                state.edge_labels = state
                    .edge_labels
                    .update(edge.label_id, Arc::new(new_bitmap));
            }
        }

        // Remove from source vertex's out_edges (if not being deleted)
        if skip_vertex != Some(edge.src) {
            if let Some(src_node) = state.vertices.get(&edge.src) {
                let mut new_src = (**src_node).clone();
                new_src.out_edges.retain(|&e| e != id);
                state.vertices = state.vertices.update(edge.src, Arc::new(new_src));
            }
        }

        // Remove from destination vertex's in_edges (if not being deleted)
        if skip_vertex != Some(edge.dst) {
            if let Some(dst_node) = state.vertices.get(&edge.dst) {
                let mut new_dst = (**dst_node).clone();
                new_dst.in_edges.retain(|&e| e != id);
                state.vertices = state.vertices.update(edge.dst, Arc::new(new_dst));
            }
        }
    }

    // =========================================================================
    // GQL API
    // =========================================================================

    /// Execute a GQL statement (both reads and mutations).
    ///
    /// This method parses and executes any GQL statement:
    /// - **Read queries** (MATCH...RETURN): Executed against a snapshot
    /// - **Mutations** (CREATE, SET, DELETE, MERGE): Executed against the graph
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::storage::cow::Graph;
    /// use interstellar::storage::GraphStorage;
    ///
    /// let graph = Graph::new();
    ///
    /// // Mutations
    /// graph.gql("CREATE (n:Person {name: 'Alice'})").unwrap();
    /// graph.gql("MATCH (n:Person) SET n.age = 30").unwrap();
    ///
    /// // Reads
    /// let results = graph.gql("MATCH (n:Person) RETURN n.name").unwrap();
    /// assert_eq!(results.len(), 1);
    /// ```
    #[cfg(feature = "gql")]
    pub fn gql(self: &Arc<Self>, query: &str) -> Result<Vec<Value>, GqlError> {
        let stmt = gql::parse_statement(query)?;

        if stmt.is_read_only() {
            // Execute reads against a snapshot
            let snapshot = self.snapshot();
            gql::compile_statement_with_graph(&stmt, &snapshot, Some(Arc::clone(self)))
                .map_err(GqlError::Compile)
        } else {
            // Execute mutations against the graph
            let mut wrapper = GraphMutWrapper {
                graph: self.as_ref(),
            };
            let schema = self.schema();
            gql::execute_mutation_with_schema(&stmt, &mut wrapper, schema.as_ref())
                .map_err(|e| GqlError::Mutation(e.to_string()))
        }
    }

    /// Execute a GQL query with parameters.
    ///
    /// This is a convenience method for executing parameterized queries.
    /// Parameters can be referenced in the query using `$paramName` syntax.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::storage::Graph;
    /// use interstellar::gql::Parameters;
    /// use interstellar::value::Value;
    ///
    /// let graph = Graph::new();
    /// graph.gql("CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    ///
    /// let mut params = Parameters::new();
    /// params.insert("minAge".to_string(), Value::Int(25));
    ///
    /// let results = graph.gql_with_params(
    ///     "MATCH (n:Person) WHERE n.age >= $minAge RETURN n.name",
    ///     &params,
    /// ).unwrap();
    /// assert_eq!(results.len(), 1);
    /// ```
    #[cfg(feature = "gql")]
    pub fn gql_with_params(
        self: &Arc<Self>,
        query: &str,
        params: &gql::Parameters,
    ) -> Result<Vec<Value>, GqlError> {
        let stmt = gql::parse_statement(query)?;

        if stmt.is_read_only() {
            // Execute reads against a snapshot
            let snapshot = self.snapshot();
            gql::compile_statement_with_params_and_graph(
                &stmt,
                &snapshot,
                params,
                Some(Arc::clone(self)),
            )
            .map_err(GqlError::Compile)
        } else {
            // Mutations with parameters not yet supported
            Err(GqlError::Mutation(
                "Parameterized mutations are not yet supported".into(),
            ))
        }
    }

    /// Execute a DDL (Data Definition Language) statement.
    ///
    /// DDL statements modify the graph's schema. Supported statements:
    /// - `CREATE NODE TYPE <name> (<properties>)` - Create a vertex type
    /// - `CREATE EDGE TYPE <name> (<properties>) FROM <labels> TO <labels>` - Create an edge type
    /// - `ALTER NODE TYPE <name> ADD <property>` - Add a property to a vertex type
    /// - `ALTER NODE TYPE <name> ALLOW ADDITIONAL PROPERTIES` - Allow extra properties
    /// - `SET SCHEMA VALIDATION STRICT|CLOSED|WARN|NONE` - Set validation mode
    /// - `DROP NODE TYPE <name>` - Drop a vertex type
    /// - `DROP EDGE TYPE <name>` - Drop an edge type
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::storage::Graph;
    /// use interstellar::schema::ValidationMode;
    ///
    /// let graph = Graph::new();
    ///
    /// graph.ddl("CREATE NODE TYPE Person (name STRING NOT NULL, age INT)").unwrap();
    /// graph.ddl("CREATE EDGE TYPE KNOWS (since INT) FROM Person TO Person").unwrap();
    /// graph.ddl("SET SCHEMA VALIDATION STRICT").unwrap();
    ///
    /// let schema = graph.schema().expect("Schema should be set");
    /// assert!(schema.vertex_schema("Person").is_some());
    /// assert!(schema.edge_schema("KNOWS").is_some());
    /// assert_eq!(schema.mode, ValidationMode::Strict);
    /// ```
    #[cfg(feature = "gql")]
    pub fn ddl(&self, query: &str) -> Result<GraphSchema, GqlError> {
        let stmt = gql::parse_statement(query)?;

        // Extract DDL statement from parsed statement
        let ddl = match stmt {
            gql::Statement::Ddl(ddl) => ddl,
            _ => {
                return Err(GqlError::Compile(gql::CompileError::UnsupportedFeature(
                    "Expected DDL statement (CREATE TYPE, ALTER TYPE, DROP TYPE, SET SCHEMA VALIDATION)".into(),
                )))
            }
        };

        // Get current schema or create empty one
        let mut schema = self.schema.read().clone().unwrap_or_default();

        // Execute DDL
        gql::execute_ddl(&mut schema, &ddl)
            .map_err(|e| GqlError::Compile(gql::CompileError::UnsupportedFeature(e.to_string())))?;

        // Update the graph's schema
        *self.schema.write() = Some(schema.clone());

        Ok(schema)
    }

    // =========================================================================
    // Batch Operations
    // =========================================================================

    /// Execute multiple operations atomically.
    ///
    /// The closure receives a `BatchContext` that buffers all mutations.
    /// Only when the closure returns `Ok(())` are all mutations applied.
    /// If the closure returns `Err` or panics, no mutations are applied.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::{Graph, BatchContext};
    /// use std::collections::HashMap;
    ///
    /// let graph = Graph::new();
    ///
    /// graph.batch(|ctx| {
    ///     let alice = ctx.add_vertex("Person", HashMap::from([
    ///         ("name".to_string(), "Alice".into()),
    ///     ]));
    ///     let bob = ctx.add_vertex("Person", HashMap::from([
    ///         ("name".to_string(), "Bob".into()),
    ///     ]));
    ///     ctx.add_edge(alice, bob, "knows", HashMap::new())?;
    ///     Ok(())
    /// }).unwrap();
    ///
    /// assert_eq!(graph.vertex_count(), 2);
    /// assert_eq!(graph.edge_count(), 1);
    /// ```
    pub fn batch<F, T>(&self, f: F) -> Result<T, BatchError>
    where
        F: FnOnce(&mut BatchContext) -> Result<T, BatchError>,
    {
        // Clone state for modification (O(1) due to structural sharing)
        let mut working_state = self.state.read().clone();

        // Create batch context
        let mut ctx = BatchContext {
            state: &mut working_state,
            #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
            pending_events: Vec::new(),
        };

        // Execute user function
        let result = f(&mut ctx)?;

        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        let pending_events = std::mem::take(&mut ctx.pending_events);

        // If successful, apply the working state
        *self.state.write() = working_state;

        // Emit batch event AFTER successful commit
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        if !pending_events.is_empty() && self.event_bus.subscriber_count() > 0 {
            self.event_bus.emit(crate::storage::events::GraphEvent::Batch(pending_events));
        }

        Ok(result)
    }

    /// Export this graph to GraphSON format.
    ///
    /// Returns a compact JSON string. For pretty-printed output, use
    /// [`to_graphson_pretty`](Self::to_graphson_pretty).
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::Graph;
    ///
    /// let graph = Graph::new();
    /// let json = graph.to_graphson().unwrap();
    /// assert!(json.contains("tinker:graph"));
    /// ```
    #[cfg(feature = "graphson")]
    pub fn to_graphson(&self) -> Result<String, serde_json::Error> {
        crate::graphson::to_string(&self.snapshot())
    }

    /// Export this graph to GraphSON format (pretty-printed).
    ///
    /// Returns a nicely formatted JSON string with indentation.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::Graph;
    ///
    /// let graph = Graph::new();
    /// let json = graph.to_graphson_pretty().unwrap();
    /// assert!(json.contains('\n')); // Contains newlines
    /// ```
    #[cfg(feature = "graphson")]
    pub fn to_graphson_pretty(&self) -> Result<String, serde_json::Error> {
        crate::graphson::to_string_pretty(&self.snapshot())
    }

    /// Create a graph from GraphSON data.
    ///
    /// Deserializes a GraphSON 3.0 formatted JSON string into a new graph.
    /// The original vertex and edge IDs from the GraphSON are mapped to
    /// new IDs; the original IDs are not preserved.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::Graph;
    ///
    /// let json = r#"{"@type": "tinker:graph", "@value": {"vertices": [], "edges": []}}"#;
    /// let graph = Graph::from_graphson(json).unwrap();
    /// assert_eq!(graph.vertex_count(), 0);
    /// ```
    #[cfg(feature = "graphson")]
    pub fn from_graphson(json: &str) -> crate::graphson::Result<Self> {
        crate::graphson::from_str(json)
    }
}

// Gremlin query string support
#[cfg(feature = "gremlin")]
impl Graph {
    /// Execute a Gremlin query string against this graph.
    ///
    /// This is a convenience method that takes a snapshot, parses the query,
    /// compiles it, and executes it in one call. For more control over the
    /// process (e.g., reusing parsed queries or controlling snapshot timing),
    /// use the [`crate::gremlin`] module directly.
    ///
    /// # Note
    ///
    /// This method takes a snapshot internally, so it provides a consistent
    /// view of the graph at the time of the call. For mutation queries, use
    /// the programmatic API instead.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use interstellar::prelude::*;
    ///
    /// let graph = Graph::new();
    /// // ... populate graph ...
    ///
    /// // Execute a Gremlin query
    /// let result = graph.query("g.V().hasLabel('person').values('name').toList()")?;
    ///
    /// if let ExecutionResult::List(names) = result {
    ///     for name in names {
    ///         println!("{}", name);
    ///     }
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a [`GremlinError`] if the query fails to parse or compile.
    pub fn query(
        self: &Arc<Self>,
        query: &str,
    ) -> Result<crate::gremlin::ExecutionResult, crate::gremlin::GremlinError> {
        let snapshot = self.snapshot();
        let ast = crate::gremlin::parse(query)?;
        let g = crate::traversal::GraphTraversalSource::from_snapshot_with_graph(
            &snapshot,
            Arc::clone(self),
        );
        let compiled = crate::gremlin::compile(&ast, &g)?;
        Ok(compiled.execute())
    }

    /// Execute a Gremlin query string with mutation support.
    ///
    /// Unlike [`query()`](Self::query), this method actually executes mutations
    /// (`addV`, `addE`, `property`, `drop`) against the graph.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::gremlin::ExecutionResult;
    ///
    /// let graph = Graph::new();
    ///
    /// // Create a vertex with mutations
    /// let result = graph.mutate("g.addV('person').property('name', 'Alice')").unwrap();
    ///
    /// // Verify the vertex was created
    /// assert_eq!(graph.vertex_count(), 1);
    ///
    /// // Read queries also work
    /// let result = graph.mutate("g.V().hasLabel('person').values('name').toList()").unwrap();
    /// if let ExecutionResult::List(names) = result {
    ///     assert_eq!(names.len(), 1);
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a [`GremlinError`] if the query fails to parse, compile, or execute.
    #[cfg(feature = "gremlin")]
    pub fn mutate(
        &self,
        query: &str,
    ) -> Result<crate::gremlin::ExecutionResult, crate::gremlin::GremlinError> {
        use crate::gremlin::{compile, parse, ExecutionResult};
        use crate::traversal::mutation::PendingMutation;

        // Parse the query
        let ast = parse(query)?;

        // Compile using a snapshot (read-only compilation)
        let snapshot = self.snapshot();
        let g = snapshot.gremlin();
        let compiled = compile(&ast, &g)?;

        // Get terminal step before consuming traversal
        let terminal = compiled.terminal().cloned();

        // Execute the traversal to get raw results (may include pending mutations)
        let raw_values = compiled.traversal.to_list();

        // Process results, executing any pending mutations
        let mut final_results = Vec::with_capacity(raw_values.len());

        for value in raw_values {
            if let Some(mutation) = PendingMutation::from_value(&value) {
                // Check if ID extraction was requested
                let extract_id = value
                    .as_map()
                    .map(|m| m.contains_key("__extract_id"))
                    .unwrap_or(false);

                // Execute the mutation
                let result = match mutation {
                    PendingMutation::AddVertex { label, properties } => {
                        let id = self.add_vertex(&label, properties);
                        Some(Value::Vertex(id))
                    }
                    PendingMutation::AddEdge {
                        label,
                        from,
                        to,
                        properties,
                    } => match self.add_edge(from, to, &label, properties) {
                        Ok(id) => Some(Value::Edge(id)),
                        Err(_) => None,
                    },
                    PendingMutation::SetVertexProperty { id, key, value } => {
                        if self.set_vertex_property(id, &key, value).is_ok() {
                            Some(Value::Vertex(id))
                        } else {
                            None
                        }
                    }
                    PendingMutation::SetEdgeProperty { id, key, value } => {
                        if self.set_edge_property(id, &key, value).is_ok() {
                            Some(Value::Edge(id))
                        } else {
                            None
                        }
                    }
                    PendingMutation::DropVertex { id } => {
                        let _ = self.remove_vertex(id);
                        None
                    }
                    PendingMutation::DropEdge { id } => {
                        let _ = self.remove_edge(id);
                        None
                    }
                };

                if let Some(result) = result {
                    if extract_id {
                        let id_value = match result {
                            Value::Vertex(vid) => Value::Int(vid.0 as i64),
                            Value::Edge(eid) => Value::Int(eid.0 as i64),
                            other => other,
                        };
                        final_results.push(id_value);
                    } else {
                        final_results.push(result);
                    }
                }
            } else {
                // Not a mutation, pass through
                final_results.push(value);
            }
        }

        // Return based on terminal step
        Ok(match terminal {
            None | Some(crate::gremlin::TerminalStep::ToList { .. }) => {
                ExecutionResult::List(final_results)
            }
            Some(crate::gremlin::TerminalStep::Next { count: None, .. }) => {
                ExecutionResult::Single(final_results.into_iter().next())
            }
            Some(crate::gremlin::TerminalStep::Next { count: Some(n), .. }) => {
                ExecutionResult::List(final_results.into_iter().take(n as usize).collect())
            }
            Some(crate::gremlin::TerminalStep::ToSet { .. }) => {
                ExecutionResult::Set(final_results.into_iter().collect())
            }
            Some(crate::gremlin::TerminalStep::Iterate { .. }) => ExecutionResult::Unit,
            Some(crate::gremlin::TerminalStep::HasNext { .. }) => {
                ExecutionResult::Bool(!final_results.is_empty())
            }
        })
    }

    /// Execute a multi-statement Gremlin script with variable assignment support.
    ///
    /// This is a convenience wrapper around [`execute_script_with_context`] that
    /// starts with an empty variable context.
    ///
    /// This method supports the full Gremlin script syntax including:
    /// - Variable assignment: `alice = g.addV('person').property('name', 'Alice').next()`
    /// - Variable references in V(): `g.V(alice)`
    /// - Variable references in from/to: `g.addE('knows').from(alice).to(bob).next()`
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::gremlin::{ExecutionResult, ScriptResult};
    /// use std::sync::Arc;
    ///
    /// let graph = Arc::new(Graph::new());
    ///
    /// // Execute a multi-statement script with variable assignments
    /// let result = graph.execute_script(r#"
    ///     alice = g.addV('person').property('name', 'Alice').next()
    ///     bob = g.addV('person').property('name', 'Bob').next()
    ///     g.addE('knows').from(alice).to(bob).next()
    ///     g.V(alice).out('knows').values('name').toList()
    /// "#).unwrap();
    ///
    /// // The result is from the last statement
    /// if let ExecutionResult::List(names) = result.result {
    ///     assert_eq!(names.len(), 1);
    ///     assert_eq!(names[0], Value::String("Bob".to_string()));
    /// }
    ///
    /// // Variables are available in result.variables
    /// assert!(result.variables.contains("alice"));
    /// assert!(result.variables.contains("bob"));
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a [`GremlinError`] if:
    /// - The script fails to parse
    /// - A traversal fails to compile
    /// - An assignment doesn't return a single value
    /// - A variable reference cannot be resolved
    #[cfg(feature = "gremlin")]
    pub fn execute_script(
        self: &Arc<Self>,
        script: &str,
    ) -> Result<crate::gremlin::ScriptResult, crate::gremlin::GremlinError> {
        self.execute_script_with_context(script, crate::gremlin::VariableContext::new())
    }

    /// Execute a multi-statement Gremlin script with an existing variable context.
    ///
    /// This enables REPL-style workflows where variables persist across calls:
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::gremlin::{ExecutionResult, ScriptResult, VariableContext};
    /// use std::sync::Arc;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let mut ctx = VariableContext::new();
    ///
    /// // First REPL command
    /// let result = graph.execute_script_with_context(
    ///     "alice = g.addV('person').property('name', 'Alice').next()",
    ///     ctx
    /// ).unwrap();
    /// ctx = result.variables;  // alice is now bound
    ///
    /// // Second REPL command (can reference alice from previous)
    /// let result = graph.execute_script_with_context(
    ///     "g.V(alice).values('name').toList()",
    ///     ctx
    /// ).unwrap();
    ///
    /// if let ExecutionResult::List(names) = result.result {
    ///     assert_eq!(names[0], Value::String("Alice".to_string()));
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a [`GremlinError`] if:
    /// - The script fails to parse
    /// - A traversal fails to compile
    /// - An assignment doesn't return a single value
    /// - A variable reference cannot be resolved
    #[cfg(feature = "gremlin")]
    pub fn execute_script_with_context(
        self: &Arc<Self>,
        script: &str,
        context: crate::gremlin::VariableContext,
    ) -> Result<crate::gremlin::ScriptResult, crate::gremlin::GremlinError> {
        use crate::gremlin::{parse_script, ExecutionResult, ScriptResult, Statement};

        // Parse the script
        let ast = parse_script(script)?;

        // Execute using our own mutation-aware execution
        let mut ctx = context;
        let mut last_result = ExecutionResult::Unit;

        for statement in &ast.statements {
            match statement {
                Statement::Assignment {
                    name, traversal, ..
                } => {
                    // Compile and execute the traversal
                    let snapshot = self.snapshot();
                    let g = crate::traversal::GraphTraversalSource::from_snapshot_with_graph(
                        &snapshot,
                        Arc::clone(self),
                    );
                    let compiled = crate::gremlin::compile_with_vars(traversal, &g, &ctx)?;
                    let terminal = compiled.terminal().cloned();
                    let raw_values = compiled.traversal.to_list();

                    // Process mutations and get results
                    let final_results = self.process_mutations(raw_values);

                    // Apply terminal semantics
                    let result = match terminal {
                        Some(crate::gremlin::TerminalStep::Next { count: None, .. }) => {
                            ExecutionResult::Single(final_results.into_iter().next())
                        }
                        _ => ExecutionResult::List(final_results),
                    };

                    // Bind the result to the variable
                    match result {
                        ExecutionResult::Single(Some(value)) => {
                            ctx.bind(name.clone(), value);
                        }
                        ExecutionResult::List(values) if values.len() == 1 => {
                            ctx.bind(name.clone(), values.into_iter().next().unwrap());
                        }
                        _ => {
                            return Err(crate::gremlin::GremlinError::Compile(
                                crate::gremlin::CompileError::InvalidArguments {
                                    step: "assignment".to_string(),
                                    message: format!(
                                        "assignment to '{}' requires single value from .next()",
                                        name
                                    ),
                                },
                            ));
                        }
                    }
                    last_result = ExecutionResult::Unit;
                }
                Statement::Traversal { traversal, .. } => {
                    // Compile and execute the traversal
                    let snapshot = self.snapshot();
                    let g = crate::traversal::GraphTraversalSource::from_snapshot_with_graph(
                        &snapshot,
                        Arc::clone(self),
                    );
                    let compiled = crate::gremlin::compile_with_vars(traversal, &g, &ctx)?;
                    let terminal = compiled.terminal().cloned();
                    let raw_values = compiled.traversal.to_list();

                    // Process mutations and get results
                    let final_results = self.process_mutations(raw_values);

                    // Return based on terminal step
                    last_result = match terminal {
                        None | Some(crate::gremlin::TerminalStep::ToList { .. }) => {
                            ExecutionResult::List(final_results)
                        }
                        Some(crate::gremlin::TerminalStep::Next { count: None, .. }) => {
                            ExecutionResult::Single(final_results.into_iter().next())
                        }
                        Some(crate::gremlin::TerminalStep::Next { count: Some(n), .. }) => {
                            ExecutionResult::List(
                                final_results.into_iter().take(n as usize).collect(),
                            )
                        }
                        Some(crate::gremlin::TerminalStep::ToSet { .. }) => {
                            ExecutionResult::Set(final_results.into_iter().collect())
                        }
                        Some(crate::gremlin::TerminalStep::Iterate { .. }) => ExecutionResult::Unit,
                        Some(crate::gremlin::TerminalStep::HasNext { .. }) => {
                            ExecutionResult::Bool(!final_results.is_empty())
                        }
                    };
                }
            }
        }

        Ok(ScriptResult {
            result: last_result,
            variables: ctx,
        })
    }

    /// Process raw traversal values, executing any pending mutations.
    #[cfg(feature = "gremlin")]
    fn process_mutations(&self, raw_values: Vec<Value>) -> Vec<Value> {
        use crate::traversal::mutation::PendingMutation;

        let mut final_results = Vec::with_capacity(raw_values.len());

        for value in raw_values {
            if let Some(mutation) = PendingMutation::from_value(&value) {
                let extract_id = value
                    .as_map()
                    .map(|m| m.contains_key("__extract_id"))
                    .unwrap_or(false);

                let result = match mutation {
                    PendingMutation::AddVertex { label, properties } => {
                        let id = self.add_vertex(&label, properties);
                        Some(Value::Vertex(id))
                    }
                    PendingMutation::AddEdge {
                        label,
                        from,
                        to,
                        properties,
                    } => match self.add_edge(from, to, &label, properties) {
                        Ok(id) => Some(Value::Edge(id)),
                        Err(_) => None,
                    },
                    PendingMutation::SetVertexProperty { id, key, value } => {
                        if self.set_vertex_property(id, &key, value).is_ok() {
                            Some(Value::Vertex(id))
                        } else {
                            None
                        }
                    }
                    PendingMutation::SetEdgeProperty { id, key, value } => {
                        if self.set_edge_property(id, &key, value).is_ok() {
                            Some(Value::Edge(id))
                        } else {
                            None
                        }
                    }
                    PendingMutation::DropVertex { id } => {
                        let _ = self.remove_vertex(id);
                        None
                    }
                    PendingMutation::DropEdge { id } => {
                        let _ = self.remove_edge(id);
                        None
                    }
                };

                if let Some(result) = result {
                    if extract_id {
                        let id_value = match result {
                            Value::Vertex(vid) => Value::Int(vid.0 as i64),
                            Value::Edge(eid) => Value::Int(eid.0 as i64),
                            other => other,
                        };
                        final_results.push(id_value);
                    } else {
                        final_results.push(result);
                    }
                }
            } else {
                final_results.push(value);
            }
        }

        final_results
    }
}

impl Default for Graph {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// CowTraversalSource - Unified Traversal API with Auto-Mutation
// =============================================================================

/// Entry point for traversals on a [`Graph`] with automatic mutation execution.
///
/// Unlike the read-only [`GraphTraversalSource`](crate::traversal::GraphTraversalSource),
/// this traversal source has access to the underlying `Graph` and will automatically
/// execute any mutations when terminal steps are called.
///
/// # Unified API
///
/// Both reads and writes use the same API - no separate "mutation mode":
///
/// ```
/// use interstellar::storage::cow::Graph;
/// use std::sync::Arc;
/// use std::collections::HashMap;
///
/// let graph = Arc::new(Graph::new());
/// let g = graph.gremlin(Arc::clone(&graph));
///
/// // Mutations are executed automatically
/// let alice = g.add_v("Person").property("name", "Alice").next();
/// let bob = g.add_v("Person").property("name", "Bob").next();
///
/// // Reads work normally
/// let count = g.v().count();  // 2
/// ```
pub struct CowTraversalSource<'g> {
    graph: &'g Graph,
    graph_arc: Arc<Graph>,
}

impl<'g> CowTraversalSource<'g> {
    /// Create a new traversal source with an Arc<Graph>.
    ///
    /// This is the primary constructor for `CowTraversalSource`.
    pub fn new_with_arc(graph: &'g Graph, graph_arc: Arc<Graph>) -> Self {
        Self { graph, graph_arc }
    }

    /// Start traversal from all vertices.
    ///
    /// Returns `GraphVertex` objects from terminal methods like `next()` and `to_list()`.
    pub fn v(&self) -> CowBoundTraversal<'g, (), Value, VertexMarker> {
        CowBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::AllVertices),
        )
    }

    /// Start traversal from specific vertex IDs.
    ///
    /// Returns `GraphVertex` objects from terminal methods.
    pub fn v_ids<I>(&self, ids: I) -> CowBoundTraversal<'g, (), Value, VertexMarker>
    where
        I: IntoIterator<Item = VertexId>,
    {
        CowBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::Vertices(ids.into_iter().collect())),
        )
    }

    /// Start traversal from a single vertex ID.
    ///
    /// Returns `GraphVertex` objects from terminal methods.
    pub fn v_id(&self, id: VertexId) -> CowBoundTraversal<'g, (), Value, VertexMarker> {
        self.v_ids([id])
    }

    /// Start traversal from a vertex reference.
    ///
    /// Accepts any type implementing `IntoVertexId`:
    /// - `VertexId`
    /// - `&GraphVertex`
    /// - `GraphVertex`
    /// - `u64`
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let g = graph.gremlin(Arc::clone(&graph));
    ///
    /// let alice = g.add_v("Person").property("name", "Alice").next().unwrap();
    /// let bob = g.add_v("Person").property("name", "Bob").next().unwrap();
    /// g.add_e("knows").from(&alice).to(&bob).property("since", 2020i64).next();
    ///
    /// // Start traversal from a GraphVertex reference
    /// let names: Vec<Value> = g.v_ref(&alice)
    ///     .out_label("knows")
    ///     .values("name")
    ///     .to_list();
    /// assert_eq!(names, vec![Value::String("Bob".to_string())]);
    /// ```
    pub fn v_ref(
        &self,
        vertex: impl IntoVertexId,
    ) -> CowBoundTraversal<'g, (), Value, VertexMarker> {
        self.v_id(vertex.into_vertex_id())
    }

    /// Start traversal from the top-`k` vertices matching a full-text query string.
    ///
    /// Looks up the text index registered for `property`, parses `query` using
    /// the index's configured analyzer, and seeds the traversal with the matching
    /// vertex IDs. Each emitted traverser carries its BM25 relevance score in its
    /// sack (retrievable via `Traverser::get_sack::<f32>()`).
    ///
    /// Returns an error if no text index is registered for `property`, or if the
    /// underlying search fails.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let scored = g.search_text("body", "graph database", 10)?
    ///     .has_label("article")
    ///     .to_list();
    /// ```
    #[cfg(feature = "full-text")]
    pub fn search_text(
        &self,
        property: &str,
        query: &str,
        k: usize,
    ) -> Result<CowBoundTraversal<'g, (), Value, VertexMarker>, crate::storage::text::TextIndexError>
    {
        use crate::storage::text::TextQuery;
        self.search_text_query(property, &TextQuery::Match(query.to_string()), k)
    }

    /// Start traversal from the top-`k` vertices matching a structured [`TextQuery`].
    ///
    /// Same as [`Self::search_text`] but accepts a pre-built [`TextQuery`] for
    /// callers that need phrase, boolean, or fuzzy queries.
    #[cfg(feature = "full-text")]
    pub fn search_text_query(
        &self,
        property: &str,
        query: &crate::storage::text::TextQuery,
        k: usize,
    ) -> Result<CowBoundTraversal<'g, (), Value, VertexMarker>, crate::storage::text::TextIndexError>
    {
        let index = self.graph.text_index_v(property).ok_or_else(|| {
            crate::storage::text::TextIndexError::Storage(crate::error::StorageError::IndexError(
                format!("no vertex text index registered for property {property:?}"),
            ))
        })?;
        let hits = index.search(query, k)?;
        let scored: Vec<(VertexId, f32)> = hits
            .into_iter()
            .filter_map(|h| h.element.as_vertex().map(|v| (v, h.score)))
            .collect();
        Ok(CowBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::VerticesWithTextScore(scored)),
        ))
    }

    /// Start traversal from the top-`k` **edges** matching a free-text query
    /// against the edge text index registered for `property`.
    ///
    /// Mirrors [`Self::search_text`] but for edges. The returned traversal is
    /// typed as [`EdgeMarker`] so subsequent steps see edges rather than
    /// vertices. Each emitted traverser carries its BM25 score in its sack
    /// (readable via `Traverser::get_sack::<f32>()`).
    ///
    /// # Errors
    ///
    /// Returns an error if no **edge** text index is registered for
    /// `property`, or if the underlying search fails.
    #[cfg(feature = "full-text")]
    pub fn search_text_e(
        &self,
        property: &str,
        query: &str,
        k: usize,
    ) -> Result<CowBoundTraversal<'g, (), Value, EdgeMarker>, crate::storage::text::TextIndexError>
    {
        use crate::storage::text::TextQuery;
        self.search_text_query_e(property, &TextQuery::Match(query.to_string()), k)
    }

    /// Start traversal from the top-`k` **edges** matching a structured
    /// [`TextQuery`]. Mirrors [`Self::search_text_query`] for edges.
    #[cfg(feature = "full-text")]
    pub fn search_text_query_e(
        &self,
        property: &str,
        query: &crate::storage::text::TextQuery,
        k: usize,
    ) -> Result<CowBoundTraversal<'g, (), Value, EdgeMarker>, crate::storage::text::TextIndexError>
    {
        let index = self.graph.text_index_e(property).ok_or_else(|| {
            crate::storage::text::TextIndexError::Storage(crate::error::StorageError::IndexError(
                format!("no edge text index registered for property {property:?}"),
            ))
        })?;
        let hits = index.search(query, k)?;
        let scored: Vec<(EdgeId, f32)> = hits
            .into_iter()
            .filter_map(|h| h.element.as_edge().map(|e| (e, h.score)))
            .collect();
        Ok(CowBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::EdgesWithTextScore(scored)),
        ))
    }

    /// Start traversal from all edges.
    ///
    /// Returns `GraphEdge` objects from terminal methods like `next()` and `to_list()`.
    pub fn e(&self) -> CowBoundTraversal<'g, (), Value, EdgeMarker> {
        CowBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::AllEdges),
        )
    }

    /// Start traversal from specific edge IDs.
    ///
    /// Returns `GraphEdge` objects from terminal methods.
    pub fn e_ids<I>(&self, ids: I) -> CowBoundTraversal<'g, (), Value, EdgeMarker>
    where
        I: IntoIterator<Item = EdgeId>,
    {
        CowBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::Edges(ids.into_iter().collect())),
        )
    }

    /// Start a traversal that creates a new vertex.
    ///
    /// The vertex is created when a terminal step is called.
    /// Returns `GraphVertex` from terminal methods.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::Graph;
    /// use std::sync::Arc;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let g = graph.gremlin(Arc::clone(&graph));
    ///
    /// let vertex = g.add_v("Person").property("name", "Alice").next();
    /// assert!(vertex.is_some());
    /// assert_eq!(graph.vertex_count(), 1);
    /// ```
    pub fn add_v(
        &self,
        label: impl Into<String>,
    ) -> CowBoundTraversal<'g, (), Value, VertexMarker> {
        use crate::traversal::mutation::AddVStep;

        let mut traversal = Traversal::<(), Value>::with_source(TraversalSource::Inject(vec![]));
        traversal = traversal.add_step(AddVStep::new(label));
        CowBoundTraversal::new_typed(self.graph, Arc::clone(&self.graph_arc), traversal)
    }

    /// Start a traversal that creates a new edge.
    ///
    /// Must specify `from` and `to` vertices before calling a terminal step.
    /// Returns `GraphEdge` from terminal methods.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::Graph;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let alice = graph.add_vertex("Person", HashMap::new());
    /// let bob = graph.add_vertex("Person", HashMap::new());
    ///
    /// let g = graph.gremlin(Arc::clone(&graph));
    /// let edge = g.add_e("KNOWS").from_id(alice).to_id(bob).next();
    /// assert!(edge.is_some());
    /// assert_eq!(graph.edge_count(), 1);
    /// ```
    pub fn add_e(&self, label: impl Into<String>) -> CowAddEdgeBuilder<'g> {
        CowAddEdgeBuilder::new_with_arc(self.graph, Arc::clone(&self.graph_arc), label.into())
    }

    /// Inject arbitrary values into the traversal stream.
    ///
    /// Returns `Value` objects from terminal methods (Scalar marker).
    pub fn inject<I>(&self, values: I) -> CowBoundTraversal<'g, (), Value, Scalar>
    where
        I: IntoIterator<Item = Value>,
    {
        CowBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::Inject(values.into_iter().collect())),
        )
    }

    // =========================================================================
    // Untyped Traversal Methods (for dynamic/scripting use cases)
    // =========================================================================
    //
    // These methods return `CowBoundTraversal<..., Scalar>` which always yields
    // `Value` from terminal methods. This is useful for dynamic scenarios
    // where compile-time type tracking isn't needed.

    /// Start untyped traversal from all vertices.
    ///
    /// Unlike `v()`, this returns `Value` from terminal methods instead of `GraphVertex`.
    /// Useful for dynamic scenarios.
    pub fn v_untyped(&self) -> CowBoundTraversal<'g, (), Value, Scalar> {
        CowBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::AllVertices),
        )
    }

    /// Start untyped traversal from specific vertex IDs.
    ///
    /// Returns `Value` from terminal methods.
    pub fn v_ids_untyped<I>(&self, ids: I) -> CowBoundTraversal<'g, (), Value, Scalar>
    where
        I: IntoIterator<Item = VertexId>,
    {
        CowBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::Vertices(ids.into_iter().collect())),
        )
    }

    /// Start untyped traversal from all edges.
    ///
    /// Unlike `e()`, this returns `Value` from terminal methods instead of `GraphEdge`.
    pub fn e_untyped(&self) -> CowBoundTraversal<'g, (), Value, Scalar> {
        CowBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::AllEdges),
        )
    }

    /// Start untyped traversal from specific edge IDs.
    ///
    /// Returns `Value` from terminal methods.
    pub fn e_ids_untyped<I>(&self, ids: I) -> CowBoundTraversal<'g, (), Value, Scalar>
    where
        I: IntoIterator<Item = EdgeId>,
    {
        CowBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::Edges(ids.into_iter().collect())),
        )
    }
}

// =============================================================================
// CowBoundTraversal - Traversal with Auto-Mutation Execution
// =============================================================================

/// A traversal bound to a [`Graph`] with automatic mutation execution.
///
/// When terminal steps (`to_list()`, `next()`, `iterate()`, etc.) are called,
/// any pending mutations in the traversal results are automatically executed
/// against the graph.
///
/// # Type Parameters
///
/// - `In` - The input type for the traversal
/// - `Out` - The output value type for the traversal  
/// - `Marker` - The output marker type (`VertexMarker`, `EdgeMarker`, or `Scalar`)
///
/// The `Marker` type determines what terminal methods return:
/// - `VertexMarker` → `next()` returns `Option<GraphVertex>`
/// - `EdgeMarker` → `next()` returns `Option<GraphEdge>`
/// - `Scalar` → `next()` returns `Option<Value>`
pub struct CowBoundTraversal<'g, In, Out, Marker: OutputMarker = Scalar> {
    graph: &'g Graph,
    graph_arc: Arc<Graph>,
    traversal: Traversal<In, Out>,
    track_paths: bool,
    _marker: PhantomData<Marker>,
}

impl<'g, In, Out, Marker: OutputMarker> CowBoundTraversal<'g, In, Out, Marker> {
    /// Create a new typed bound traversal.
    pub(crate) fn new_typed(
        graph: &'g Graph,
        graph_arc: Arc<Graph>,
        traversal: Traversal<In, Out>,
    ) -> Self {
        Self {
            graph,
            graph_arc,
            traversal,
            track_paths: false,
            _marker: PhantomData,
        }
    }

    /// Enable automatic path tracking for this traversal.
    pub fn with_path(mut self) -> Self {
        self.track_paths = true;
        self
    }

    /// Add a step to the traversal, preserving the marker type.
    pub fn add_step_same<NewOut>(
        self,
        step: impl crate::traversal::step::Step,
    ) -> CowBoundTraversal<'g, In, NewOut, Marker> {
        CowBoundTraversal {
            graph: self.graph,
            graph_arc: self.graph_arc,
            traversal: self.traversal.add_step(step),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Add a step to the traversal, preserving the marker type.
    ///
    /// Alias for `add_step_same` for backward compatibility.
    pub fn add_step<NewOut>(
        self,
        step: impl crate::traversal::step::Step,
    ) -> CowBoundTraversal<'g, In, NewOut, Marker> {
        self.add_step_same(step)
    }

    /// Add a step to the traversal with a new marker type.
    pub fn add_step_with_marker<NewOut, NewMarker: OutputMarker>(
        self,
        step: impl crate::traversal::step::Step,
    ) -> CowBoundTraversal<'g, In, NewOut, NewMarker> {
        CowBoundTraversal {
            graph: self.graph,
            graph_arc: self.graph_arc,
            traversal: self.traversal.add_step(step),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Append an anonymous traversal's steps (marker becomes Scalar).
    pub fn append<Mid>(self, anon: Traversal<Out, Mid>) -> CowBoundTraversal<'g, In, Mid, Scalar> {
        CowBoundTraversal {
            graph: self.graph,
            graph_arc: self.graph_arc,
            traversal: self.traversal.append(anon),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Execute the traversal and process any pending mutations.
    ///
    /// Returns a vector of raw Values with mutations applied.
    fn execute_with_mutations(self) -> Vec<Value> {
        use crate::traversal::step::StartStep;
        use crate::traversal::traverser::TraversalSource;

        // For mutation-only traversals (add_v, add_e, property, drop),
        // we can execute without a full ExecutionContext since these steps
        // don't need graph access - they just produce pending mutation markers.
        //
        // This avoids the deadlock issue where GraphSnapshot::interner() leaks
        // a read guard that blocks later write operations.

        // Decompose traversal into source and steps
        let (source, steps) = self.traversal.into_steps();

        // Check if this is a mutation-only traversal (source is Inject([]))
        let is_mutation_only =
            matches!(&source, Some(TraversalSource::Inject(values)) if values.is_empty());

        let results: Vec<Traverser> = if is_mutation_only {
            // For mutation-only traversals, we still need an ExecutionContext.
            // Use a snapshot from the actual graph (O(1) clone due to structural sharing).
            let snapshot = self.graph.snapshot();
            let interner = snapshot.interner();
            let storage_ref: &dyn GraphStorage = &snapshot;

            let ctx = if self.track_paths {
                ExecutionContext::with_path_tracking(storage_ref, interner)
            } else {
                ExecutionContext::new(storage_ref, interner)
            };

            // Start with empty input (since source is Inject([]))
            let mut current: Vec<Traverser> = Vec::new();

            // For add_v, we need to inject a single empty value to trigger the step
            // The add_v step ignores input and produces one pending mutation
            if !steps.is_empty() {
                // Inject a single traverser to trigger the mutation step
                current = vec![Traverser::new(Value::Null)];
            }

            // Apply each step in sequence
            for step in &steps {
                current = step
                    .apply_dyn(&ctx, Box::new(current.into_iter()))
                    .collect();
            }

            current
        } else {
            // For read traversals that need graph access, use the full path
            // Create a snapshot for read operations
            let snapshot = self.graph.snapshot();
            let interner = snapshot.interner();
            let storage_ref: &dyn GraphStorage = &snapshot;

            // Create execution context - GraphSnapshot implements GraphStorage
            let ctx = if self.track_paths {
                ExecutionContext::with_path_tracking(storage_ref, interner)
            } else {
                ExecutionContext::new(storage_ref, interner)
            };

            // Start with source traversers
            let mut current: Vec<Traverser> = match source {
                Some(src) => {
                    let start_step = StartStep::new(src);
                    start_step
                        .apply(&ctx, Box::new(std::iter::empty()))
                        .collect()
                }
                None => Vec::new(),
            };

            // Apply each step in sequence
            for step in &steps {
                current = step
                    .apply_dyn(&ctx, Box::new(current.into_iter()))
                    .collect();
            }

            current
            // Note: For read-only traversals, we don't need to mutate after,
            // so the leaked guard in interner() is not a problem
        };

        // Process results, executing any pending mutations
        let mut wrapper = GraphMutWrapper { graph: self.graph };
        let mut final_results = Vec::with_capacity(results.len());

        for traverser in results {
            if let Some(mutation) = PendingMutation::from_value(&traverser.value) {
                // Check if ID extraction was requested (via .id() step on pending mutation)
                let extract_id = traverser
                    .value
                    .as_map()
                    .map(|m| m.contains_key("__extract_id"))
                    .unwrap_or(false);

                // Execute the mutation and get the result
                if let Some(result) = Self::execute_mutation(&mut wrapper, mutation) {
                    if extract_id {
                        // Return the ID as an integer instead of the element
                        let id_value = match result {
                            Value::Vertex(vid) => Value::Int(vid.0 as i64),
                            Value::Edge(eid) => Value::Int(eid.0 as i64),
                            other => other,
                        };
                        final_results.push(id_value);
                    } else {
                        final_results.push(result);
                    }
                }
            } else {
                // Not a mutation, pass through
                final_results.push(traverser.value);
            }
        }

        final_results
    }

    /// Execute a single pending mutation.
    fn execute_mutation(
        wrapper: &mut GraphMutWrapper<'_>,
        mutation: PendingMutation,
    ) -> Option<Value> {
        use crate::storage::GraphStorageMut;

        match mutation {
            PendingMutation::AddVertex { label, properties } => {
                let id = wrapper.add_vertex(&label, properties);
                Some(Value::Vertex(id))
            }
            PendingMutation::AddEdge {
                label,
                from,
                to,
                properties,
            } => match wrapper.add_edge(from, to, &label, properties) {
                Ok(id) => Some(Value::Edge(id)),
                Err(_) => None,
            },
            PendingMutation::SetVertexProperty { id, key, value } => {
                wrapper.set_vertex_property(id, &key, value).ok()?;
                Some(Value::Vertex(id))
            }
            PendingMutation::SetEdgeProperty { id, key, value } => {
                wrapper.set_edge_property(id, &key, value).ok()?;
                Some(Value::Edge(id))
            }
            PendingMutation::DropVertex { id } => {
                wrapper.remove_vertex(id).ok()?;
                None
            }
            PendingMutation::DropEdge { id } => {
                wrapper.remove_edge(id).ok()?;
                None
            }
        }
    }

    // =========================================================================
    // Escape Hatch Methods (available on all marker types)
    // =========================================================================

    /// Execute and collect all raw values into a list.
    ///
    /// This is an escape hatch for when you need raw `Value` objects
    /// regardless of the marker type.
    pub fn to_value_list(self) -> Vec<Value> {
        self.execute_with_mutations()
    }

    /// Execute and return the first raw value, if any.
    ///
    /// This is an escape hatch for when you need a raw `Value` object
    /// regardless of the marker type.
    pub fn next_value(self) -> Option<Value> {
        self.execute_with_mutations().into_iter().next()
    }

    /// Execute and consume the traversal, discarding results.
    ///
    /// Any pending mutations are automatically executed.
    pub fn iterate(self) {
        let _ = self.execute_with_mutations();
    }

    /// Check if the traversal produces any results.
    pub fn has_next(self) -> bool {
        !self.execute_with_mutations().is_empty()
    }
}

// =============================================================================
// Terminal Methods for VertexMarker
// =============================================================================

/// Terminal methods when traversal produces vertices.
impl<'g, In, Out> CowBoundTraversal<'g, In, Out, VertexMarker> {
    /// Execute and return the first vertex, if any.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Alice".into()),
    /// ]));
    ///
    /// let v = graph.gremlin(Arc::clone(&graph)).v().next().unwrap();
    /// assert_eq!(v.label(), Some("person".to_string()));
    /// ```
    pub fn next(self) -> Option<InMemoryVertex> {
        let graph_arc = Arc::clone(&self.graph_arc);
        self.execute_with_mutations()
            .into_iter()
            .find_map(|v| match v {
                Value::Vertex(id) => Some(GraphVertex::new(id, Arc::clone(&graph_arc))),
                _ => None,
            })
    }

    /// Execute and collect all vertices into a list.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// graph.add_vertex("person", HashMap::from([("name".into(), "Alice".into())]));
    /// graph.add_vertex("person", HashMap::from([("name".into(), "Bob".into())]));
    ///
    /// let vertices = graph.gremlin(Arc::clone(&graph)).v().to_list();
    /// assert_eq!(vertices.len(), 2);
    /// ```
    pub fn to_list(self) -> Vec<InMemoryVertex> {
        let graph_arc = Arc::clone(&self.graph_arc);
        self.execute_with_mutations()
            .into_iter()
            .filter_map(|v| match v {
                Value::Vertex(id) => Some(GraphVertex::new(id, Arc::clone(&graph_arc))),
                _ => None,
            })
            .collect()
    }

    /// Execute and return exactly one vertex.
    ///
    /// # Errors
    ///
    /// Returns `TraversalError::NotOne` if zero or more than one vertex is found.
    pub fn one(self) -> Result<InMemoryVertex, crate::error::TraversalError> {
        let graph_arc = Arc::clone(&self.graph_arc);
        let ids: Vec<_> = self
            .execute_with_mutations()
            .into_iter()
            .filter_map(|v| match v {
                Value::Vertex(id) => Some(id),
                _ => None,
            })
            .take(2)
            .collect();
        match ids.len() {
            1 => Ok(GraphVertex::new(ids[0], graph_arc)),
            n => Err(crate::error::TraversalError::NotOne(n)),
        }
    }

    /// Execute and count the number of vertices.
    pub fn count(self) -> u64 {
        self.execute_with_mutations()
            .into_iter()
            .filter(|v| matches!(v, Value::Vertex(_)))
            .count() as u64
    }

    /// Execute and collect unique vertices into a set.
    ///
    /// Note: GraphVertex contains Arc<Graph> with interior mutability, but
    /// we only hash/compare by VertexId, so this is safe.
    #[allow(clippy::mutable_key_type)]
    pub fn to_set(self) -> std::collections::HashSet<InMemoryVertex> {
        self.to_list().into_iter().collect()
    }
}

// =============================================================================
// Terminal Methods for EdgeMarker
// =============================================================================

/// Terminal methods when traversal produces edges.
impl<'g, In, Out> CowBoundTraversal<'g, In, Out, EdgeMarker> {
    /// Execute and return the first edge, if any.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let a = graph.add_vertex("person", HashMap::new());
    /// let b = graph.add_vertex("person", HashMap::new());
    /// graph.add_edge(a, b, "knows", HashMap::new()).unwrap();
    ///
    /// let e = graph.gremlin(Arc::clone(&graph)).e().next().unwrap();
    /// assert_eq!(e.label(), Some("knows".to_string()));
    /// ```
    pub fn next(self) -> Option<InMemoryEdge> {
        let graph_arc = Arc::clone(&self.graph_arc);
        self.execute_with_mutations()
            .into_iter()
            .find_map(|v| match v {
                Value::Edge(id) => Some(GraphEdge::new(id, Arc::clone(&graph_arc))),
                _ => None,
            })
    }

    /// Execute and collect all edges into a list.
    pub fn to_list(self) -> Vec<InMemoryEdge> {
        let graph_arc = Arc::clone(&self.graph_arc);
        self.execute_with_mutations()
            .into_iter()
            .filter_map(|v| match v {
                Value::Edge(id) => Some(GraphEdge::new(id, Arc::clone(&graph_arc))),
                _ => None,
            })
            .collect()
    }

    /// Execute and return exactly one edge.
    ///
    /// # Errors
    ///
    /// Returns `TraversalError::NotOne` if zero or more than one edge is found.
    pub fn one(self) -> Result<InMemoryEdge, crate::error::TraversalError> {
        let graph_arc = Arc::clone(&self.graph_arc);
        let ids: Vec<_> = self
            .execute_with_mutations()
            .into_iter()
            .filter_map(|v| match v {
                Value::Edge(id) => Some(id),
                _ => None,
            })
            .take(2)
            .collect();
        match ids.len() {
            1 => Ok(GraphEdge::new(ids[0], graph_arc)),
            n => Err(crate::error::TraversalError::NotOne(n)),
        }
    }

    /// Execute and count the number of edges.
    pub fn count(self) -> u64 {
        self.execute_with_mutations()
            .into_iter()
            .filter(|v| matches!(v, Value::Edge(_)))
            .count() as u64
    }

    /// Execute and collect unique edges into a set.
    ///
    /// Note: GraphEdge contains Arc<Graph> with interior mutability, but
    /// we only hash/compare by EdgeId, so this is safe.
    #[allow(clippy::mutable_key_type)]
    pub fn to_set(self) -> std::collections::HashSet<InMemoryEdge> {
        self.to_list().into_iter().collect()
    }
}

// =============================================================================
// Terminal Methods for Scalar
// =============================================================================

/// Terminal methods when traversal produces scalar values.
impl<'g, In, Out> CowBoundTraversal<'g, In, Out, Scalar> {
    /// Execute and return the first value, if any.
    pub fn next(self) -> Option<Value> {
        self.execute_with_mutations().into_iter().next()
    }

    /// Execute and collect all values into a list.
    pub fn to_list(self) -> Vec<Value> {
        self.execute_with_mutations()
    }

    /// Execute and return exactly one value.
    ///
    /// # Errors
    ///
    /// Returns `TraversalError::NotOne` if zero or more than one value is found.
    pub fn one(self) -> Result<Value, crate::error::TraversalError> {
        let results: Vec<_> = self.execute_with_mutations().into_iter().take(2).collect();
        match results.len() {
            1 => Ok(results.into_iter().next().unwrap()),
            n => Err(crate::error::TraversalError::NotOne(n)),
        }
    }

    /// Execute and count the number of results.
    pub fn count(self) -> u64 {
        self.execute_with_mutations().len() as u64
    }

    /// Execute and collect unique values into a set.
    pub fn to_set(self) -> std::collections::HashSet<Value> {
        self.execute_with_mutations().into_iter().collect()
    }

    /// Sum all numeric values.
    pub fn sum(self) -> Value {
        let mut int_sum: i64 = 0;
        let mut float_sum: f64 = 0.0;
        let mut has_float = false;

        for value in self.execute_with_mutations() {
            match value {
                Value::Int(n) => int_sum += n,
                Value::Float(f) => {
                    has_float = true;
                    float_sum += f;
                }
                _ => {}
            }
        }

        if has_float {
            Value::Float(int_sum as f64 + float_sum)
        } else {
            Value::Int(int_sum)
        }
    }
}

// =============================================================================
// Step methods for VertexMarker traversals
// =============================================================================

impl<'g, In> CowBoundTraversal<'g, In, Value, VertexMarker> {
    /// Filter vertices by label (preserves VertexMarker).
    pub fn has_label(
        self,
        label: impl Into<String>,
    ) -> CowBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(HasLabelStep::single(label))
    }

    /// Filter to vertices that have a specific property key (preserves VertexMarker).
    pub fn has(self, key: impl Into<String>) -> CowBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(HasStep::new(key.into()))
    }

    /// Filter to vertices where a property equals a value (preserves VertexMarker).
    pub fn has_value(
        self,
        key: impl Into<String>,
        value: Value,
    ) -> CowBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(HasValueStep::new(key.into(), value))
    }

    /// Traverse to outgoing adjacent vertices (preserves VertexMarker).
    pub fn out(self) -> CowBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(OutStep::new())
    }

    /// Traverse to outgoing adjacent vertices via edges with label (preserves VertexMarker).
    pub fn out_label(
        self,
        label: impl Into<String>,
    ) -> CowBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(OutStep::with_labels(vec![label.into()]))
    }

    /// Traverse to incoming adjacent vertices (preserves VertexMarker).
    pub fn in_(self) -> CowBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(InStep::new())
    }

    /// Traverse to incoming adjacent vertices via edges with label (preserves VertexMarker).
    pub fn in_label(
        self,
        label: impl Into<String>,
    ) -> CowBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(InStep::with_labels(vec![label.into()]))
    }

    /// Traverse to adjacent vertices in both directions (preserves VertexMarker).
    pub fn both(self) -> CowBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(crate::traversal::BothStep::new())
    }

    /// Traverse to outgoing edges (transforms to EdgeMarker).
    pub fn out_e(self) -> CowBoundTraversal<'g, In, Value, EdgeMarker> {
        self.add_step_with_marker(OutEStep::new())
    }

    /// Traverse to incoming edges (transforms to EdgeMarker).
    pub fn in_e(self) -> CowBoundTraversal<'g, In, Value, EdgeMarker> {
        self.add_step_with_marker(InEStep::new())
    }

    /// Get property values by key (transforms to Scalar).
    pub fn values(self, key: impl Into<String>) -> CowBoundTraversal<'g, In, Value, Scalar> {
        self.add_step_with_marker(ValuesStep::new(key.into()))
    }

    /// Add a property to the current vertex (for mutation traversals, preserves VertexMarker).
    pub fn property(
        self,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> CowBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(PropertyStep::new(key.into(), value.into()))
    }

    /// Drop (delete) the current vertex (preserves VertexMarker).
    pub fn drop(self) -> CowBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(DropStep)
    }

    /// Add an edge from the current vertex.
    pub fn add_e(self, label: impl Into<String>) -> CowBoundAddEdgeBuilder<'g, In> {
        CowBoundAddEdgeBuilder::new_with_arc(
            self.graph,
            self.graph_arc,
            self.traversal,
            label.into(),
            self.track_paths,
        )
    }

    /// Limit results to first n (preserves VertexMarker).
    pub fn limit(self, n: usize) -> CowBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(LimitStep::new(n))
    }

    /// Skip first n results (preserves VertexMarker).
    pub fn skip(self, n: usize) -> CowBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(SkipStep::new(n))
    }

    /// Get element IDs (transforms to Scalar).
    pub fn id(self) -> CowBoundTraversal<'g, In, Value, Scalar> {
        self.add_step_with_marker(IdStep)
    }

    /// Get element labels (transforms to Scalar).
    pub fn label(self) -> CowBoundTraversal<'g, In, Value, Scalar> {
        self.add_step_with_marker(LabelStep)
    }
}

// =============================================================================
// Step methods for EdgeMarker traversals
// =============================================================================

impl<'g, In> CowBoundTraversal<'g, In, Value, EdgeMarker> {
    /// Filter edges by label (preserves EdgeMarker).
    pub fn has_label(
        self,
        label: impl Into<String>,
    ) -> CowBoundTraversal<'g, In, Value, EdgeMarker> {
        self.add_step_same(HasLabelStep::single(label))
    }

    /// Filter to edges that have a specific property key (preserves EdgeMarker).
    pub fn has(self, key: impl Into<String>) -> CowBoundTraversal<'g, In, Value, EdgeMarker> {
        self.add_step_same(HasStep::new(key.into()))
    }

    /// Filter to edges where a property equals a value (preserves EdgeMarker).
    pub fn has_value(
        self,
        key: impl Into<String>,
        value: Value,
    ) -> CowBoundTraversal<'g, In, Value, EdgeMarker> {
        self.add_step_same(HasValueStep::new(key.into(), value))
    }

    /// Traverse to the target vertex of an edge (transforms to VertexMarker).
    pub fn in_v(self) -> CowBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_with_marker(InVStep)
    }

    /// Traverse to the source vertex of an edge (transforms to VertexMarker).
    pub fn out_v(self) -> CowBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_with_marker(OutVStep)
    }

    /// Get property values by key (transforms to Scalar).
    pub fn values(self, key: impl Into<String>) -> CowBoundTraversal<'g, In, Value, Scalar> {
        self.add_step_with_marker(ValuesStep::new(key.into()))
    }

    /// Add a property to the current edge (for mutation traversals, preserves EdgeMarker).
    pub fn property(
        self,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> CowBoundTraversal<'g, In, Value, EdgeMarker> {
        self.add_step_same(PropertyStep::new(key.into(), value.into()))
    }

    /// Drop (delete) the current edge (preserves EdgeMarker).
    pub fn drop(self) -> CowBoundTraversal<'g, In, Value, EdgeMarker> {
        self.add_step_same(DropStep)
    }

    /// Limit results to first n (preserves EdgeMarker).
    pub fn limit(self, n: usize) -> CowBoundTraversal<'g, In, Value, EdgeMarker> {
        self.add_step_same(LimitStep::new(n))
    }

    /// Skip first n results (preserves EdgeMarker).
    pub fn skip(self, n: usize) -> CowBoundTraversal<'g, In, Value, EdgeMarker> {
        self.add_step_same(SkipStep::new(n))
    }

    /// Get element IDs (transforms to Scalar).
    pub fn id(self) -> CowBoundTraversal<'g, In, Value, Scalar> {
        self.add_step_with_marker(IdStep)
    }

    /// Get element labels (transforms to Scalar).
    pub fn label(self) -> CowBoundTraversal<'g, In, Value, Scalar> {
        self.add_step_with_marker(LabelStep)
    }
}

// =============================================================================
// Step methods for Scalar traversals
// =============================================================================

impl<'g, In> CowBoundTraversal<'g, In, Value, Scalar> {
    /// Filter to values that satisfy a condition (preserves Scalar).
    pub fn has(self, key: impl Into<String>) -> CowBoundTraversal<'g, In, Value, Scalar> {
        self.add_step_same(HasStep::new(key.into()))
    }

    /// Limit results to first n (preserves Scalar).
    pub fn limit(self, n: usize) -> CowBoundTraversal<'g, In, Value, Scalar> {
        self.add_step_same(LimitStep::new(n))
    }

    /// Skip first n results (preserves Scalar).
    pub fn skip(self, n: usize) -> CowBoundTraversal<'g, In, Value, Scalar> {
        self.add_step_same(SkipStep::new(n))
    }
}

// =============================================================================
// CowAddEdgeBuilder - Builder for add_e() from traversal source
// =============================================================================

/// Builder for creating edges from the traversal source.
pub struct CowAddEdgeBuilder<'g> {
    graph: &'g Graph,
    graph_arc: Arc<Graph>,
    label: String,
    from: Option<VertexId>,
    to: Option<VertexId>,
    properties: HashMap<String, Value>,
}

impl<'g> CowAddEdgeBuilder<'g> {
    fn new_with_arc(graph: &'g Graph, graph_arc: Arc<Graph>, label: String) -> Self {
        Self {
            graph,
            graph_arc,
            label,
            from: None,
            to: None,
            properties: HashMap::new(),
        }
    }

    /// Set the source vertex by ID.
    pub fn from_id(mut self, id: VertexId) -> Self {
        self.from = Some(id);
        self
    }

    /// Set the destination vertex by ID.
    pub fn to_id(mut self, id: VertexId) -> Self {
        self.to = Some(id);
        self
    }

    /// Set the source vertex using any type that can be converted to VertexId.
    ///
    /// Accepts:
    /// - `VertexId` directly
    /// - `&GraphVertex` (reference to a vertex object)
    /// - `GraphVertex` (owned vertex object)
    /// - `u64` (raw ID value)
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let alice = graph.add_vertex("Person", HashMap::new());
    /// let bob = graph.add_vertex("Person", HashMap::new());
    ///
    /// let g = graph.gremlin(Arc::clone(&graph));
    ///
    /// // All of these work:
    /// // Using VertexId directly
    /// g.add_e("knows").from(alice).to(bob).next();
    ///
    /// // Or using GraphVertex references
    /// let alice_v = g.add_v("Person").next().unwrap();
    /// let bob_v = g.add_v("Person").next().unwrap();
    /// g.add_e("knows").from(&alice_v).to(&bob_v).next();
    /// ```
    pub fn from(self, vertex: impl IntoVertexId) -> Self {
        self.from_id(vertex.into_vertex_id())
    }

    /// Set the destination vertex using any type that can be converted to VertexId.
    ///
    /// Accepts:
    /// - `VertexId` directly
    /// - `&GraphVertex` (reference to a vertex object)
    /// - `GraphVertex` (owned vertex object)
    /// - `u64` (raw ID value)
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let g = graph.gremlin(Arc::clone(&graph));
    ///
    /// let alice = g.add_v("Person").property("name", "Alice").next().unwrap();
    /// let bob = g.add_v("Person").property("name", "Bob").next().unwrap();
    ///
    /// // Create edge using GraphVertex references
    /// g.add_e("knows").from(&alice).to(&bob).next();
    /// ```
    pub fn to(self, vertex: impl IntoVertexId) -> Self {
        self.to_id(vertex.into_vertex_id())
    }

    /// Add a property to the edge.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Execute and return the created edge as a GraphEdge.
    pub fn next(self) -> Option<InMemoryEdge> {
        let from = self.from?;
        let to = self.to?;

        match self.graph.add_edge(from, to, &self.label, self.properties) {
            Ok(id) => Some(GraphEdge::new(id, self.graph_arc)),
            Err(_) => None,
        }
    }

    /// Execute, discarding the result.
    pub fn iterate(self) {
        let _ = self.next();
    }

    /// Execute and return results as a list.
    pub fn to_list(self) -> Vec<InMemoryEdge> {
        self.next().into_iter().collect()
    }
}

// =============================================================================
// CowBoundAddEdgeBuilder - Builder for add_e() from traversal
// =============================================================================

/// Builder for creating edges from an existing traversal.
pub struct CowBoundAddEdgeBuilder<'g, In> {
    graph: &'g Graph,
    graph_arc: Arc<Graph>,
    traversal: Traversal<In, Value>,
    label: String,
    to: Option<VertexId>,
    properties: HashMap<String, Value>,
    track_paths: bool,
}

impl<'g, In> CowBoundAddEdgeBuilder<'g, In> {
    fn new_with_arc(
        graph: &'g Graph,
        graph_arc: Arc<Graph>,
        traversal: Traversal<In, Value>,
        label: String,
        track_paths: bool,
    ) -> Self {
        Self {
            graph,
            graph_arc,
            traversal,
            label,
            to: None,
            properties: HashMap::new(),
            track_paths,
        }
    }

    /// Set the destination vertex by ID.
    pub fn to_id(mut self, id: VertexId) -> Self {
        self.to = Some(id);
        self
    }

    /// Set the destination vertex using any type that can be converted to VertexId.
    ///
    /// Accepts:
    /// - `VertexId` directly
    /// - `&GraphVertex` (reference to a vertex object)
    /// - `GraphVertex` (owned vertex object)
    /// - `u64` (raw ID value)
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let g = graph.gremlin(Arc::clone(&graph));
    ///
    /// let alice = g.add_v("Person").property("name", "Alice").next().unwrap();
    /// let bob = g.add_v("Person").property("name", "Bob").next().unwrap();
    ///
    /// // Create edge from traversal using GraphVertex reference
    /// g.v_ref(&alice).add_e("knows").to(&bob).next();
    /// ```
    pub fn to(self, vertex: impl IntoVertexId) -> Self {
        self.to_id(vertex.into_vertex_id())
    }

    /// Add a property to the edge.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Build and execute the traversal, creating edges.
    pub fn to_list(self) -> Vec<InMemoryEdge> {
        use crate::traversal::mutation::AddEStep;

        let to_id = match self.to {
            Some(id) => id,
            None => return vec![],
        };

        let mut step = AddEStep::new(&self.label);
        step = step.to_vertex(to_id);
        for (k, v) in self.properties {
            step = step.property(k, v);
        }

        let traversal: Traversal<In, Value> = self.traversal.add_step(step);
        let bound: CowBoundTraversal<'_, In, Value, EdgeMarker> = CowBoundTraversal {
            graph: self.graph,
            graph_arc: self.graph_arc,
            traversal,
            track_paths: self.track_paths,
            _marker: PhantomData,
        };

        bound.to_list()
    }

    /// Execute and return the first edge created.
    pub fn next(self) -> Option<InMemoryEdge> {
        self.to_list().into_iter().next()
    }

    /// Execute, discarding results.
    pub fn iterate(self) {
        let _ = self.to_list();
    }
}

// =============================================================================
// GraphSnapshot - Immutable Owned Snapshot
// =============================================================================

/// An owned snapshot of the graph at a point in time.
///
/// Unlike the current `GraphSnapshot<'g>`, this snapshot:
/// - Does not hold any locks
/// - Can be sent across threads (`Send + Sync`)
/// - Can outlive the source `Graph`
/// - Is immutable and will never change
///
/// # Example
///
/// ```
/// use interstellar::storage::cow::Graph;
/// use interstellar::storage::GraphStorage;
/// use std::collections::HashMap;
/// use std::thread;
///
/// let graph = Graph::new();
/// graph.add_vertex("person", HashMap::new());
///
/// let snap = graph.snapshot();
///
/// // Snapshot can be sent to another thread
/// let handle = thread::spawn(move || {
///     snap.vertex_count()
/// });
///
/// assert_eq!(handle.join().unwrap(), 1);
/// ```
#[derive(Clone)]
pub struct GraphSnapshot {
    /// Shared reference to frozen state
    state: Arc<GraphState>,
    /// Cloned interner - snapshot-local, no shared lock
    interner_snapshot: Arc<StringInterner>,
    /// Subscription manager reference for reactive queries.
    #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
    subscription_manager: std::sync::Arc<crate::traversal::reactive::SubscriptionManager>,
    /// Factory that creates fresh snapshots for reactive re-evaluation.
    #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
    snapshot_fn: std::sync::Arc<
        dyn Fn() -> Box<dyn crate::traversal::context::SnapshotLike + Send> + Send + Sync,
    >,
}

impl GraphSnapshot {
    /// Get the snapshot version.
    pub fn version(&self) -> u64 {
        self.state.version
    }

    /// Get the string interner for this snapshot.
    ///
    /// This returns a reference to the snapshot-local cloned interner.
    pub fn interner(&self) -> &StringInterner {
        &self.interner_snapshot
    }

    /// Get Arc-wrapped storage for streaming execution.
    ///
    /// Returns a clone of self wrapped in Arc. Since `GraphSnapshot`
    /// implements `GraphStorage`, this enables streaming pipelines
    /// to own the storage without lifetime constraints.
    #[inline]
    pub fn arc_storage(&self) -> Arc<dyn GraphStorage + Send + Sync> {
        Arc::new(self.clone())
    }

    /// Get Arc-wrapped interner for streaming execution.
    ///
    /// Returns a clone of the internal Arc reference.
    #[inline]
    pub fn arc_interner(&self) -> Arc<StringInterner> {
        Arc::clone(&self.interner_snapshot)
    }

    /// Create a Gremlin traversal source for this snapshot.
    ///
    /// This provides the full Gremlin-style fluent API for querying the graph.
    /// Since `GraphSnapshot` is immutable, only read operations are available.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::Graph;
    /// use interstellar::value::Value;
    /// use std::collections::HashMap;
    ///
    /// let graph = Graph::new();
    /// graph.add_vertex("Person", HashMap::from([
    ///     ("name".to_string(), Value::String("Alice".to_string())),
    /// ]));
    ///
    /// let snapshot = graph.snapshot();
    /// let g = snapshot.gremlin();
    ///
    /// let count = g.v().has_label("Person").count();
    /// assert_eq!(count, 1);
    /// ```
    pub fn gremlin(&self) -> crate::traversal::GraphTraversalSource<'_> {
        crate::traversal::GraphTraversalSource::from_snapshot(self)
    }
}

// Gremlin query string support
#[cfg(feature = "gremlin")]
impl GraphSnapshot {
    /// Execute a Gremlin query string against this snapshot.
    ///
    /// This is a convenience method that parses, compiles, and executes a Gremlin
    /// query in one call. For more control over the process (e.g., reusing parsed
    /// queries), use the [`crate::gremlin`] module directly.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use interstellar::prelude::*;
    ///
    /// let graph = Graph::new();
    /// // ... populate graph ...
    /// let snapshot = graph.snapshot();
    ///
    /// // Execute a Gremlin query
    /// let result = snapshot.query("g.V().hasLabel('person').values('name').toList()")?;
    ///
    /// if let ExecutionResult::List(names) = result {
    ///     for name in names {
    ///         println!("{}", name);
    ///     }
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a [`GremlinError`] if the query fails to parse or compile.
    pub fn query(
        &self,
        query: &str,
    ) -> Result<crate::gremlin::ExecutionResult, crate::gremlin::GremlinError> {
        let ast = crate::gremlin::parse(query)?;
        let g = self.gremlin();
        let compiled = crate::gremlin::compile(&ast, &g)?;
        Ok(compiled.execute())
    }
}

// Implement SnapshotLike for GraphSnapshot to enable generic traversal/GQL usage.
// Since GraphSnapshot implements GraphStorage directly, storage() returns self.
impl crate::traversal::SnapshotLike for GraphSnapshot {
    fn storage(&self) -> &dyn GraphStorage {
        self
    }

    fn interner(&self) -> &StringInterner {
        &self.interner_snapshot
    }

    fn as_dyn(&self) -> &dyn crate::traversal::SnapshotLike {
        self
    }

    fn arc_storage(&self) -> std::sync::Arc<dyn GraphStorage + Send + Sync> {
        self.arc_storage()
    }

    fn arc_interner(&self) -> std::sync::Arc<StringInterner> {
        self.arc_interner()
    }

    fn arc_streamable(&self) -> std::sync::Arc<dyn StreamableStorage> {
        self.arc_streamable()
    }

    #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
    fn subscription_manager(&self) -> Option<&crate::traversal::reactive::SubscriptionManager> {
        Some(&self.subscription_manager)
    }

    #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
    fn reactive_snapshot_fn(
        &self,
    ) -> Option<
        std::sync::Arc<
            dyn Fn() -> Box<dyn crate::traversal::context::SnapshotLike + Send> + Send + Sync,
        >,
    > {
        Some(self.snapshot_fn.clone())
    }
}

impl GraphStorage for GraphSnapshot {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        let node = self.state.vertices.get(&id)?;
        let label = self.interner_snapshot.resolve(node.label_id)?.to_string();

        Some(Vertex {
            id: node.id,
            label,
            properties: node.properties.clone(),
        })
    }

    fn vertex_count(&self) -> u64 {
        self.state.vertices.len() as u64
    }

    fn get_edge(&self, id: EdgeId) -> Option<Edge> {
        let edge = self.state.edges.get(&id)?;
        let label = self.interner_snapshot.resolve(edge.label_id)?.to_string();

        Some(Edge {
            id: edge.id,
            label,
            src: edge.src,
            dst: edge.dst,
            properties: edge.properties.clone(),
        })
    }

    fn edge_count(&self) -> u64 {
        self.state.edges.len() as u64
    }

    fn out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        let Some(node) = self.state.vertices.get(&vertex) else {
            return Box::new(std::iter::empty());
        };

        let edges: Vec<Edge> = node
            .out_edges
            .iter()
            .filter_map(|&eid| self.get_edge(eid))
            .collect();

        Box::new(edges.into_iter())
    }

    fn in_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        let Some(node) = self.state.vertices.get(&vertex) else {
            return Box::new(std::iter::empty());
        };

        let edges: Vec<Edge> = node
            .in_edges
            .iter()
            .filter_map(|&eid| self.get_edge(eid))
            .collect();

        Box::new(edges.into_iter())
    }

    fn vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let label_id = self.interner_snapshot.lookup(label);

        let Some(id) = label_id else {
            return Box::new(std::iter::empty());
        };

        let Some(bitmap) = self.state.vertex_labels.get(&id) else {
            return Box::new(std::iter::empty());
        };

        let vertices: Vec<Vertex> = bitmap
            .iter()
            .filter_map(|vid| self.get_vertex(VertexId(vid as u64)))
            .collect();

        Box::new(vertices.into_iter())
    }

    fn edges_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Edge> + '_> {
        let label_id = self.interner_snapshot.lookup(label);

        let Some(id) = label_id else {
            return Box::new(std::iter::empty());
        };

        let Some(bitmap) = self.state.edge_labels.get(&id) else {
            return Box::new(std::iter::empty());
        };

        let edges: Vec<Edge> = bitmap
            .iter()
            .filter_map(|eid| self.get_edge(EdgeId(eid as u64)))
            .collect();

        Box::new(edges.into_iter())
    }

    fn all_vertices(&self) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let vertices: Vec<Vertex> = self
            .state
            .vertices
            .keys()
            .filter_map(|&id| self.get_vertex(id))
            .collect();

        Box::new(vertices.into_iter())
    }

    fn all_edges(&self) -> Box<dyn Iterator<Item = Edge> + '_> {
        let edges: Vec<Edge> = self
            .state
            .edges
            .keys()
            .filter_map(|&id| self.get_edge(id))
            .collect();

        Box::new(edges.into_iter())
    }

    fn interner(&self) -> &StringInterner {
        // Return a reference to the snapshot-local cloned interner.
        // No locking needed since it's owned by this snapshot.
        &self.interner_snapshot
    }

    fn vertices_by_property(
        &self,
        label: Option<&str>,
        property: &str,
        value: &Value,
    ) -> Box<dyn Iterator<Item = Vertex> + '_> {
        // Default implementation: full scan with filter
        let label_owned = label.map(|s| s.to_string());
        let property_owned = property.to_string();
        let value_clone = value.clone();

        let vertices: Vec<Vertex> = self
            .all_vertices()
            .filter(move |v| {
                if let Some(ref l) = label_owned {
                    if &v.label != l {
                        return false;
                    }
                }
                v.properties.get(&property_owned) == Some(&value_clone)
            })
            .collect();

        Box::new(vertices.into_iter())
    }

    fn edges_by_property(
        &self,
        label: Option<&str>,
        property: &str,
        value: &Value,
    ) -> Box<dyn Iterator<Item = Edge> + '_> {
        let label_owned = label.map(|s| s.to_string());
        let property_owned = property.to_string();
        let value_clone = value.clone();

        let edges: Vec<Edge> = self
            .all_edges()
            .filter(move |e| {
                if let Some(ref l) = label_owned {
                    if &e.label != l {
                        return false;
                    }
                }
                e.properties.get(&property_owned) == Some(&value_clone)
            })
            .collect();

        Box::new(edges.into_iter())
    }

    fn vertices_by_property_range(
        &self,
        label: Option<&str>,
        property: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let label_owned = label.map(|s| s.to_string());
        let property_owned = property.to_string();
        let start_clone = match start {
            Bound::Included(v) => Bound::Included(v.clone()),
            Bound::Excluded(v) => Bound::Excluded(v.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_clone = match end {
            Bound::Included(v) => Bound::Included(v.clone()),
            Bound::Excluded(v) => Bound::Excluded(v.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let vertices: Vec<Vertex> = self
            .all_vertices()
            .filter(move |v| {
                if let Some(ref l) = label_owned {
                    if &v.label != l {
                        return false;
                    }
                }
                if let Some(prop_value) = v.properties.get(&property_owned) {
                    let prop_cmp = prop_value.to_comparable();
                    let in_start = match &start_clone {
                        Bound::Included(s) => prop_cmp >= s.to_comparable(),
                        Bound::Excluded(s) => prop_cmp > s.to_comparable(),
                        Bound::Unbounded => true,
                    };
                    let in_end = match &end_clone {
                        Bound::Included(e) => prop_cmp <= e.to_comparable(),
                        Bound::Excluded(e) => prop_cmp < e.to_comparable(),
                        Bound::Unbounded => true,
                    };
                    in_start && in_end
                } else {
                    false
                }
            })
            .collect();

        Box::new(vertices.into_iter())
    }
}

// GraphSnapshot is Send + Sync because it only contains Arc<GraphState>
// and GraphState only contains Send + Sync types
unsafe impl Send for GraphSnapshot {}
unsafe impl Sync for GraphSnapshot {}

// StreamableStorage implementation for GraphSnapshot.
//
// GraphSnapshot is ideal for streaming because it owns an Arc<GraphState> that
// can be cheaply cloned into returned iterators. This enables true O(1) streaming
// where the iterator holds its own reference to the graph state.
//
// The im::HashMap can be cloned in O(1) via structural sharing, so we clone it
// into each iterator to get owned ('static) iteration.
impl StreamableStorage for GraphSnapshot {
    fn stream_all_vertices(&self) -> Box<dyn Iterator<Item = VertexId> + Send> {
        // Clone the im::HashMap - O(1) due to structural sharing
        let vertices = self.state.vertices.clone();
        Box::new(vertices.into_iter().map(|(id, _)| id))
    }

    fn stream_all_edges(&self) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        // Clone the im::HashMap - O(1) due to structural sharing
        let edges = self.state.edges.clone();
        Box::new(edges.into_iter().map(|(id, _)| id))
    }

    fn stream_vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = VertexId> + Send> {
        // Look up label ID and get the RoaringBitmap
        let label_id = self.interner_snapshot.lookup(label);
        if let Some(lid) = label_id {
            if let Some(bitmap) = self.state.vertex_labels.get(&lid) {
                // Clone the RoaringBitmap to get owned iteration
                // RoaringBitmap::clone is O(n) but into_iter is lazy
                let bitmap_owned: RoaringBitmap = (**bitmap).clone();
                return Box::new(bitmap_owned.into_iter().map(|id| VertexId(id as u64)));
            }
        }
        Box::new(std::iter::empty())
    }

    fn stream_edges_with_label(&self, label: &str) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        // Look up label ID and get the RoaringBitmap
        let label_id = self.interner_snapshot.lookup(label);
        if let Some(lid) = label_id {
            if let Some(bitmap) = self.state.edge_labels.get(&lid) {
                // Clone the RoaringBitmap to get owned iteration
                // RoaringBitmap::clone is O(n) but into_iter is lazy
                let bitmap_owned: RoaringBitmap = (**bitmap).clone();
                return Box::new(bitmap_owned.into_iter().map(|id| EdgeId(id as u64)));
            }
        }
        Box::new(std::iter::empty())
    }

    fn stream_out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        if let Some(node) = self.state.vertices.get(&vertex) {
            // Clone the adjacency list - this is O(degree) but necessary for 'static
            let out_edges = node.out_edges.clone();
            Box::new(out_edges.into_iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn stream_in_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        if let Some(node) = self.state.vertices.get(&vertex) {
            // Clone the adjacency list - this is O(degree) but necessary for 'static
            let in_edges = node.in_edges.clone();
            Box::new(in_edges.into_iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn stream_out_neighbors(
        &self,
        vertex: VertexId,
        label_ids: &[u32],
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        let state = Arc::clone(&self.state);
        let label_ids_owned: Vec<u32> = label_ids.to_vec();

        if let Some(node) = state.vertices.get(&vertex) {
            let out_edges = node.out_edges.clone();
            let state_for_iter = Arc::clone(&state);

            Box::new(out_edges.into_iter().filter_map(move |edge_id| {
                state_for_iter.edges.get(&edge_id).and_then(|edge| {
                    if label_ids_owned.is_empty() || label_ids_owned.contains(&edge.label_id) {
                        Some(edge.dst)
                    } else {
                        None
                    }
                })
            }))
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn stream_in_neighbors(
        &self,
        vertex: VertexId,
        label_ids: &[u32],
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        let state = Arc::clone(&self.state);
        let label_ids_owned: Vec<u32> = label_ids.to_vec();

        if let Some(node) = state.vertices.get(&vertex) {
            let in_edges = node.in_edges.clone();
            let state_for_iter = Arc::clone(&state);

            Box::new(in_edges.into_iter().filter_map(move |edge_id| {
                state_for_iter.edges.get(&edge_id).and_then(|edge| {
                    if label_ids_owned.is_empty() || label_ids_owned.contains(&edge.label_id) {
                        Some(edge.src)
                    } else {
                        None
                    }
                })
            }))
        } else {
            Box::new(std::iter::empty())
        }
    }
}

impl GraphSnapshot {
    /// Returns an Arc<dyn StreamableStorage> for use with StreamingExecutor.
    ///
    /// This enables the traversal engine to hold an owned reference to the
    /// storage that can be used to create streaming iterators. The clone is
    /// cheap since `GraphSnapshot` is internally Arc-based.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let snapshot = graph.snapshot();
    /// let streamable = snapshot.arc_streamable();
    /// let interner = snapshot.arc_interner();
    /// let executor = StreamingExecutor::new_streaming(streamable, interner, ...);
    /// ```
    #[inline]
    pub fn arc_streamable(&self) -> Arc<dyn StreamableStorage> {
        Arc::new(self.clone())
    }
}

// =============================================================================
// BatchContext - For Atomic Multi-Operation Batches
// =============================================================================

/// Error type for batch operations.
#[derive(Debug, thiserror::Error)]
pub enum BatchError {
    /// Storage error occurred
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    /// Custom error message
    #[error("{0}")]
    Custom(String),
}

/// Context for batch operations.
///
/// All mutations made through this context are buffered and only applied
/// when the batch closure returns successfully.
pub struct BatchContext<'a> {
    state: &'a mut GraphState,
    #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
    pub(crate) pending_events: Vec<crate::storage::events::GraphEvent>,
}

impl<'a> BatchContext<'a> {
    /// Add a vertex within the batch.
    pub fn add_vertex(&mut self, label: &str, properties: HashMap<String, Value>) -> VertexId {
        let id = VertexId(self.state.next_vertex_id);
        self.state.next_vertex_id += 1;

        let label_id = self.state.interner.write().intern(label);

        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        let properties_clone = properties.clone();

        let node = Arc::new(NodeData {
            id,
            label_id,
            properties,
            out_edges: Vec::new(),
            in_edges: Vec::new(),
        });

        self.state.vertices = self.state.vertices.update(id, node);

        // Update label index
        let bitmap = self
            .state
            .vertex_labels
            .get(&label_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(RoaringBitmap::new()));
        let mut new_bitmap = (*bitmap).clone();
        new_bitmap.insert(id.0 as u32);
        self.state.vertex_labels = self
            .state
            .vertex_labels
            .update(label_id, Arc::new(new_bitmap));

        self.state.version += 1;

        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        self.pending_events.push(crate::storage::events::GraphEvent::VertexAdded {
            id,
            label: label.to_string(),
            properties: properties_clone,
        });

        id
    }

    /// Add an edge within the batch.
    pub fn add_edge(
        &mut self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, BatchError> {
        if !self.state.vertices.contains_key(&src) {
            return Err(BatchError::Storage(StorageError::VertexNotFound(src)));
        }
        if !self.state.vertices.contains_key(&dst) {
            return Err(BatchError::Storage(StorageError::VertexNotFound(dst)));
        }

        let edge_id = EdgeId(self.state.next_edge_id);
        self.state.next_edge_id += 1;

        let label_id = self.state.interner.write().intern(label);

        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        let properties_clone = properties.clone();

        let edge = Arc::new(EdgeData {
            id: edge_id,
            label_id,
            src,
            dst,
            properties,
        });

        self.state.edges = self.state.edges.update(edge_id, edge);

        // Update adjacency lists
        if let Some(src_node) = self.state.vertices.get(&src) {
            let mut new_src = (**src_node).clone();
            new_src.out_edges.push(edge_id);
            self.state.vertices = self.state.vertices.update(src, Arc::new(new_src));
        }

        if let Some(dst_node) = self.state.vertices.get(&dst) {
            let mut new_dst = (**dst_node).clone();
            new_dst.in_edges.push(edge_id);
            self.state.vertices = self.state.vertices.update(dst, Arc::new(new_dst));
        }

        // Update label index
        let bitmap = self
            .state
            .edge_labels
            .get(&label_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(RoaringBitmap::new()));
        let mut new_bitmap = (*bitmap).clone();
        new_bitmap.insert(edge_id.0 as u32);
        self.state.edge_labels = self
            .state
            .edge_labels
            .update(label_id, Arc::new(new_bitmap));

        self.state.version += 1;

        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        self.pending_events.push(crate::storage::events::GraphEvent::EdgeAdded {
            id: edge_id,
            src,
            dst,
            label: label.to_string(),
            properties: properties_clone,
        });

        Ok(edge_id)
    }

    /// Get a vertex within the batch (sees uncommitted changes).
    pub fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        let node = self.state.vertices.get(&id)?;
        let label = self
            .state
            .interner
            .read()
            .resolve(node.label_id)?
            .to_string();

        Some(Vertex {
            id: node.id,
            label,
            properties: node.properties.clone(),
        })
    }
}

// =============================================================================
// GraphMutWrapper - Implements GraphStorageMut for Graph
// =============================================================================

/// Wrapper that provides [`GraphStorageMut`] implementation for [`Graph`].
///
/// This wrapper allows using the COW-based `Graph` with APIs that require
/// mutable storage access, such as the GQL mutation engine.
///
/// # Example
///
/// ```
/// use interstellar::storage::{Graph, GraphStorage, GraphStorageMut};
///
/// let graph = Graph::new();
/// let mut wrapper = graph.as_storage_mut();
///
/// // Use wrapper with APIs requiring GraphStorageMut
/// let id = wrapper.add_vertex("person", std::collections::HashMap::new());
/// assert!(wrapper.get_vertex(id).is_some());
/// ```
pub struct GraphMutWrapper<'a> {
    graph: &'a Graph,
}

impl<'a> GraphStorage for GraphMutWrapper<'a> {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        let state = self.graph.state.read();
        let node = state.vertices.get(&id)?;
        let label = state.interner.read().resolve(node.label_id)?.to_string();

        Some(Vertex {
            id: node.id,
            label,
            properties: node.properties.clone(),
        })
    }

    fn vertex_count(&self) -> u64 {
        self.graph.state.read().vertices.len() as u64
    }

    fn get_edge(&self, id: EdgeId) -> Option<Edge> {
        let state = self.graph.state.read();
        let edge = state.edges.get(&id)?;
        let label = state.interner.read().resolve(edge.label_id)?.to_string();

        Some(Edge {
            id: edge.id,
            label,
            src: edge.src,
            dst: edge.dst,
            properties: edge.properties.clone(),
        })
    }

    fn edge_count(&self) -> u64 {
        self.graph.state.read().edges.len() as u64
    }

    fn out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        let state = self.graph.state.read();
        let Some(node) = state.vertices.get(&vertex) else {
            return Box::new(std::iter::empty());
        };

        let edge_ids: Vec<EdgeId> = node.out_edges.clone();
        drop(state);

        let edges: Vec<Edge> = edge_ids
            .into_iter()
            .filter_map(|eid| self.get_edge(eid))
            .collect();

        Box::new(edges.into_iter())
    }

    fn in_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        let state = self.graph.state.read();
        let Some(node) = state.vertices.get(&vertex) else {
            return Box::new(std::iter::empty());
        };

        let edge_ids: Vec<EdgeId> = node.in_edges.clone();
        drop(state);

        let edges: Vec<Edge> = edge_ids
            .into_iter()
            .filter_map(|eid| self.get_edge(eid))
            .collect();

        Box::new(edges.into_iter())
    }

    fn vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let state = self.graph.state.read();
        let label_id = state.interner.read().lookup(label);

        let Some(id) = label_id else {
            return Box::new(std::iter::empty());
        };

        let Some(bitmap) = state.vertex_labels.get(&id) else {
            return Box::new(std::iter::empty());
        };

        let vertex_ids: Vec<u64> = bitmap.iter().map(|v| v as u64).collect();
        drop(state);

        let vertices: Vec<Vertex> = vertex_ids
            .into_iter()
            .filter_map(|vid| self.get_vertex(VertexId(vid)))
            .collect();

        Box::new(vertices.into_iter())
    }

    fn edges_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Edge> + '_> {
        let state = self.graph.state.read();
        let label_id = state.interner.read().lookup(label);

        let Some(id) = label_id else {
            return Box::new(std::iter::empty());
        };

        let Some(bitmap) = state.edge_labels.get(&id) else {
            return Box::new(std::iter::empty());
        };

        let edge_ids: Vec<u64> = bitmap.iter().map(|e| e as u64).collect();
        drop(state);

        let edges: Vec<Edge> = edge_ids
            .into_iter()
            .filter_map(|eid| self.get_edge(EdgeId(eid)))
            .collect();

        Box::new(edges.into_iter())
    }

    fn all_vertices(&self) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let state = self.graph.state.read();
        let vertex_ids: Vec<VertexId> = state.vertices.keys().copied().collect();
        drop(state);

        let vertices: Vec<Vertex> = vertex_ids
            .into_iter()
            .filter_map(|id| self.get_vertex(id))
            .collect();

        Box::new(vertices.into_iter())
    }

    fn all_edges(&self) -> Box<dyn Iterator<Item = Edge> + '_> {
        let state = self.graph.state.read();
        let edge_ids: Vec<EdgeId> = state.edges.keys().copied().collect();
        drop(state);

        let edges: Vec<Edge> = edge_ids
            .into_iter()
            .filter_map(|id| self.get_edge(id))
            .collect();

        Box::new(edges.into_iter())
    }

    fn interner(&self) -> &StringInterner {
        // Similar approach to GraphSnapshot::interner()
        unsafe {
            let state = self.graph.state.read();
            let guard = state.interner.read();
            let ptr: *const StringInterner = &*guard;
            std::mem::forget(guard);
            &*ptr
        }
    }
}

impl<'a> crate::storage::GraphStorageMut for GraphMutWrapper<'a> {
    fn add_vertex(&mut self, label: &str, properties: HashMap<String, Value>) -> VertexId {
        self.graph.add_vertex(label, properties)
    }

    fn add_edge(
        &mut self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError> {
        self.graph.add_edge(src, dst, label, properties)
    }

    fn set_vertex_property(
        &mut self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        self.graph.set_vertex_property(id, key, value)
    }

    fn set_edge_property(
        &mut self,
        id: EdgeId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        self.graph.set_edge_property(id, key, value)
    }

    fn remove_vertex(&mut self, id: VertexId) -> Result<(), StorageError> {
        self.graph.remove_vertex(id)
    }

    fn remove_edge(&mut self, id: EdgeId) -> Result<(), StorageError> {
        self.graph.remove_edge(id)
    }
}

// =============================================================================
// GraphAccess Implementation for Arc<Graph>
// =============================================================================

impl crate::graph_access::GraphAccess for Arc<Graph> {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        self.snapshot().get_vertex(id)
    }

    fn get_edge(&self, id: EdgeId) -> Option<Edge> {
        self.snapshot().get_edge(id)
    }

    fn out_edge_ids(&self, vertex: VertexId) -> Vec<EdgeId> {
        self.snapshot().out_edges(vertex).map(|e| e.id).collect()
    }

    fn in_edge_ids(&self, vertex: VertexId) -> Vec<EdgeId> {
        self.snapshot().in_edges(vertex).map(|e| e.id).collect()
    }

    fn set_vertex_property(
        &self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        Graph::set_vertex_property(self, id, key, value)
    }

    fn set_edge_property(&self, id: EdgeId, key: &str, value: Value) -> Result<(), StorageError> {
        Graph::set_edge_property(self, id, key, value)
    }

    fn add_edge(
        &self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError> {
        Graph::add_edge(self, src, dst, label, properties)
    }

    fn remove_vertex(&self, id: VertexId) -> Result<(), StorageError> {
        Graph::remove_vertex(self, id)
    }

    fn remove_edge(&self, id: EdgeId) -> Result<(), StorageError> {
        Graph::remove_edge(self, id)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_graph_is_empty() {
        let graph = Graph::new();
        assert_eq!(graph.vertex_count(), 0);
        assert_eq!(graph.edge_count(), 0);
        assert_eq!(graph.version(), 0);
    }

    #[test]
    fn test_add_vertex() {
        let graph = Graph::new();
        let id = graph.add_vertex("person", HashMap::new());
        assert_eq!(id.0, 0);
        assert_eq!(graph.vertex_count(), 1);
        assert_eq!(graph.version(), 1);
    }

    #[test]
    fn test_add_vertex_with_properties() {
        let graph = Graph::new();
        let props = HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("age".to_string(), Value::Int(30)),
        ]);
        let id = graph.add_vertex("person", props);

        let snap = graph.snapshot();
        let vertex = snap.get_vertex(id).unwrap();
        assert_eq!(vertex.label, "person");
        assert_eq!(
            vertex.properties.get("name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(vertex.properties.get("age"), Some(&Value::Int(30)));
    }

    #[test]
    fn test_add_edge() {
        let graph = Graph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());

        let edge_id = graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();

        let snap = graph.snapshot();
        let edge = snap.get_edge(edge_id).unwrap();
        assert_eq!(edge.src, v1);
        assert_eq!(edge.dst, v2);
        assert_eq!(edge.label, "knows");
    }

    #[test]
    fn test_add_edge_missing_source() {
        let graph = Graph::new();
        let v1 = graph.add_vertex("person", HashMap::new());

        let result = graph.add_edge(VertexId(999), v1, "knows", HashMap::new());
        assert!(matches!(result, Err(StorageError::VertexNotFound(_))));
    }

    #[test]
    fn test_snapshot_isolation() {
        let graph = Graph::new();
        let v1 = graph.add_vertex(
            "person",
            HashMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
        );

        // Take snapshot
        let snap = graph.snapshot();

        // Modify graph
        graph
            .set_vertex_property(v1, "name", Value::String("Alicia".to_string()))
            .unwrap();

        // Snapshot still sees old value
        let vertex = snap.get_vertex(v1).unwrap();
        assert_eq!(
            vertex.properties.get("name"),
            Some(&Value::String("Alice".to_string()))
        );

        // New snapshot sees new value
        let snap2 = graph.snapshot();
        let vertex2 = snap2.get_vertex(v1).unwrap();
        assert_eq!(
            vertex2.properties.get("name"),
            Some(&Value::String("Alicia".to_string()))
        );
    }

    #[test]
    fn test_snapshot_survives_modification() {
        let graph = Graph::new();
        for i in 0..1000 {
            graph.add_vertex("node", HashMap::from([("id".to_string(), Value::Int(i))]));
        }

        let snap = graph.snapshot();
        assert_eq!(snap.vertex_count(), 1000);

        // Add more vertices
        for i in 1000..2000 {
            graph.add_vertex("node", HashMap::from([("id".to_string(), Value::Int(i))]));
        }

        // Snapshot unchanged
        assert_eq!(snap.vertex_count(), 1000);

        // New snapshot sees all
        assert_eq!(graph.snapshot().vertex_count(), 2000);
    }

    #[test]
    fn test_remove_vertex() {
        let graph = Graph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();

        graph.remove_vertex(v1).unwrap();

        assert_eq!(graph.vertex_count(), 1);
        assert_eq!(graph.edge_count(), 0); // Edge removed with vertex
    }

    #[test]
    fn test_remove_edge() {
        let graph = Graph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        let edge_id = graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();

        graph.remove_edge(edge_id).unwrap();

        assert_eq!(graph.edge_count(), 0);
        assert_eq!(graph.vertex_count(), 2); // Vertices still exist
    }

    #[test]
    fn test_vertices_with_label() {
        let graph = Graph::new();
        graph.add_vertex("person", HashMap::new());
        graph.add_vertex("person", HashMap::new());
        graph.add_vertex("software", HashMap::new());

        let snap = graph.snapshot();
        let people: Vec<_> = snap.vertices_with_label("person").collect();
        let software: Vec<_> = snap.vertices_with_label("software").collect();

        assert_eq!(people.len(), 2);
        assert_eq!(software.len(), 1);
    }

    #[test]
    fn test_out_edges() {
        let graph = Graph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        let v3 = graph.add_vertex("person", HashMap::new());

        graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
        graph.add_edge(v1, v3, "knows", HashMap::new()).unwrap();
        graph.add_edge(v2, v1, "knows", HashMap::new()).unwrap();

        let snap = graph.snapshot();
        let out: Vec<_> = snap.out_edges(v1).collect();
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|e| e.src == v1));
    }

    #[test]
    fn test_in_edges() {
        let graph = Graph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        let v3 = graph.add_vertex("person", HashMap::new());

        graph.add_edge(v2, v1, "knows", HashMap::new()).unwrap();
        graph.add_edge(v3, v1, "knows", HashMap::new()).unwrap();
        graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();

        let snap = graph.snapshot();
        let in_e: Vec<_> = snap.in_edges(v1).collect();
        assert_eq!(in_e.len(), 2);
        assert!(in_e.iter().all(|e| e.dst == v1));
    }

    #[test]
    fn test_batch_success() {
        let graph = Graph::new();

        graph
            .batch(|ctx| {
                let alice = ctx.add_vertex(
                    "Person",
                    HashMap::from([("name".to_string(), "Alice".into())]),
                );
                let bob = ctx.add_vertex(
                    "Person",
                    HashMap::from([("name".to_string(), "Bob".into())]),
                );
                ctx.add_edge(alice, bob, "knows", HashMap::new())?;
                Ok(())
            })
            .unwrap();

        assert_eq!(graph.vertex_count(), 2);
        assert_eq!(graph.edge_count(), 1);
    }

    #[test]
    fn test_batch_rollback_on_error() {
        let graph = Graph::new();
        graph.add_vertex("existing", HashMap::new());

        let result: Result<(), BatchError> = graph.batch(|ctx| {
            ctx.add_vertex("new", HashMap::new());
            // Try to add edge to non-existent vertex
            ctx.add_edge(VertexId(0), VertexId(999), "invalid", HashMap::new())?;
            Ok(())
        });

        assert!(result.is_err());
        // Graph should be unchanged
        assert_eq!(graph.vertex_count(), 1);
    }

    #[test]
    fn test_snapshot_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<GraphSnapshot>();
        assert_send_sync::<Graph>();
    }

    #[test]
    fn test_snapshot_can_outlive_scope() {
        let snap = {
            let graph = Graph::new();
            graph.add_vertex("person", HashMap::new());
            graph.snapshot()
        }; // graph dropped here

        // Snapshot still valid
        assert_eq!(snap.vertex_count(), 1);
    }

    #[test]
    fn test_concurrent_snapshots() {
        use std::sync::Arc;
        use std::thread;

        let graph = Arc::new(Graph::new());
        for i in 0..100 {
            graph.add_vertex("node", HashMap::from([("id".to_string(), Value::Int(i))]));
        }

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let g = Arc::clone(&graph);
                thread::spawn(move || {
                    let snap = g.snapshot();
                    snap.vertex_count()
                })
            })
            .collect();

        for handle in handles {
            assert_eq!(handle.join().unwrap(), 100);
        }
    }

    #[test]
    fn test_self_loop_edge() {
        let graph = Graph::new();
        let v1 = graph.add_vertex("person", HashMap::new());

        let e = graph.add_edge(v1, v1, "self", HashMap::new()).unwrap();

        let snap = graph.snapshot();
        let out: Vec<_> = snap.out_edges(v1).collect();
        let in_e: Vec<_> = snap.in_edges(v1).collect();

        assert_eq!(out.len(), 1);
        assert_eq!(in_e.len(), 1);
        assert_eq!(out[0].id, e);
        assert_eq!(in_e[0].id, e);
    }

    #[test]
    fn test_remove_vertex_with_self_loop() {
        let graph = Graph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        graph.add_edge(v1, v1, "self", HashMap::new()).unwrap();

        graph.remove_vertex(v1).unwrap();

        assert_eq!(graph.vertex_count(), 0);
        assert_eq!(graph.edge_count(), 0);
    }

    // =========================================================================
    // Typed Terminal Methods Tests (Breaking Change)
    // =========================================================================

    #[test]
    fn test_cow_to_vertex_list() {
        use std::sync::Arc;

        let graph = Arc::new(Graph::new());
        graph.add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Alice".into())]),
        );
        graph.add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Bob".into())]),
        );

        // g.v().to_list() now returns Vec<GraphVertex>
        let g = graph.gremlin(Arc::clone(&graph));
        let vertices = g.v().to_list();
        assert_eq!(vertices.len(), 2);

        // Verify we can access properties
        let names: Vec<_> = vertices.iter().filter_map(|v| v.property("name")).collect();
        assert_eq!(names.len(), 2);
    }

    #[test]
    fn test_cow_next_vertex() {
        use std::sync::Arc;

        let graph = Arc::new(Graph::new());
        graph.add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Alice".into())]),
        );

        // g.v().next() now returns Option<GraphVertex>
        let g = graph.gremlin(Arc::clone(&graph));
        let v = g.v().next();
        assert!(v.is_some());
        assert_eq!(
            v.unwrap().property("name"),
            Some(crate::value::Value::String("Alice".to_string()))
        );
    }

    #[test]
    fn test_cow_one_vertex() {
        use std::sync::Arc;

        let graph = Arc::new(Graph::new());
        let id = graph.add_vertex("person", HashMap::new());

        // g.v().one() now returns Result<GraphVertex, TraversalError>
        let g = graph.gremlin(Arc::clone(&graph));
        let result = g.v().one();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().id(), id);

        // Add another vertex - should fail
        graph.add_vertex("person", HashMap::new());
        let g2 = graph.gremlin(Arc::clone(&graph));
        let result = g2.v().one();
        assert!(result.is_err());
    }

    #[test]
    fn test_cow_to_edge_list() {
        use std::sync::Arc;

        let graph = Arc::new(Graph::new());
        let a = graph.add_vertex("person", HashMap::new());
        let b = graph.add_vertex("person", HashMap::new());
        graph.add_edge(a, b, "knows", HashMap::new()).unwrap();

        // g.e().to_list() now returns Vec<GraphEdge>
        let g = graph.gremlin(Arc::clone(&graph));
        let edges = g.e().to_list();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].label(), Some("knows".to_string()));
    }

    #[test]
    fn test_cow_next_edge() {
        use std::sync::Arc;

        let graph = Arc::new(Graph::new());
        let a = graph.add_vertex("person", HashMap::new());
        let b = graph.add_vertex("person", HashMap::new());
        graph.add_edge(a, b, "knows", HashMap::new()).unwrap();

        // g.e().next() now returns Option<GraphEdge>
        let g = graph.gremlin(Arc::clone(&graph));
        let e = g.e().next();
        assert!(e.is_some());
        assert_eq!(e.unwrap().label(), Some("knows".to_string()));
    }

    #[test]
    fn test_cow_one_edge() {
        use std::sync::Arc;

        let graph = Arc::new(Graph::new());
        let a = graph.add_vertex("person", HashMap::new());
        let b = graph.add_vertex("person", HashMap::new());
        let edge_id = graph.add_edge(a, b, "knows", HashMap::new()).unwrap();

        // g.e().one() now returns Result<GraphEdge, TraversalError>
        let g = graph.gremlin(Arc::clone(&graph));
        let result = g.e().one();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().id(), edge_id);

        // Add another edge - should fail
        graph.add_edge(b, a, "knows", HashMap::new()).unwrap();
        let g2 = graph.gremlin(Arc::clone(&graph));
        let result = g2.e().one();
        assert!(result.is_err());
    }

    #[test]
    fn test_cow_typed_gremlin() {
        use std::sync::Arc;

        let graph = Arc::new(Graph::new());
        graph.add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Alice".into())]),
        );

        let snapshot = graph.snapshot();
        let g = graph.typed_gremlin(&snapshot, Arc::clone(&graph));

        // TypedTraversalSource returns GraphVertex directly from next()
        let v = g.v().next();
        assert!(v.is_some());
        assert_eq!(
            v.unwrap().property("name"),
            Some(crate::value::Value::String("Alice".to_string()))
        );
    }

    #[test]
    fn test_cow_typed_vertex_traversal() {
        use std::sync::Arc;

        let graph = Arc::new(Graph::new());
        let alice = graph.add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Alice".into())]),
        );
        let bob = graph.add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Bob".into())]),
        );
        graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();

        // g.v().has_value(...).to_list() returns Vec<GraphVertex>
        let g = graph.gremlin(Arc::clone(&graph));
        let vertices = g
            .v()
            .has_value("name", crate::value::Value::String("Alice".to_string()))
            .to_list();

        assert_eq!(vertices.len(), 1);
        assert_eq!(
            vertices[0].property("name"),
            Some(crate::value::Value::String("Alice".to_string()))
        );

        // Can traverse from GraphVertex
        let friends = vertices[0].out("knows").to_list();
        assert_eq!(friends.len(), 1);
        assert_eq!(
            friends[0].property("name"),
            Some(crate::value::Value::String("Bob".to_string()))
        );
    }

    // =========================================================================
    // IntoVertexId / Gremlin Variable Assignment Tests (Spec 51)
    // =========================================================================

    #[test]
    fn test_from_to_accept_graph_vertex() {
        use std::sync::Arc;

        let graph = Arc::new(Graph::new());
        let g = graph.gremlin(Arc::clone(&graph));

        let alice = g.add_v("person").property("name", "Alice").next().unwrap();
        let bob = g.add_v("person").property("name", "Bob").next().unwrap();

        // Test from/to with GraphVertex reference
        let edge = g.add_e("knows").from(&alice).to(&bob).next();
        assert!(edge.is_some());

        // Test v_ref with GraphVertex reference
        let names: Vec<String> = g
            .v_ref(&alice)
            .out_label("knows")
            .values("name")
            .to_list()
            .into_iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        assert_eq!(names, vec!["Bob"]);
    }

    #[test]
    fn test_from_to_accept_vertex_id() {
        use std::sync::Arc;

        let graph = Arc::new(Graph::new());
        let g = graph.gremlin(Arc::clone(&graph));

        let alice = g.add_v("person").next().unwrap();
        let bob = g.add_v("person").next().unwrap();

        // Test from/to with VertexId
        let edge = g.add_e("knows").from(alice.id()).to(bob.id()).next();
        assert!(edge.is_some());
    }

    #[test]
    fn test_from_to_accept_u64() {
        use std::sync::Arc;

        let graph = Arc::new(Graph::new());
        let g = graph.gremlin(Arc::clone(&graph));

        g.add_v("person").next();
        g.add_v("person").next();

        // Test from/to with u64 raw ID values
        let edge = g.add_e("knows").from(0u64).to(1u64).next();
        assert!(edge.is_some());
    }

    #[test]
    fn test_v_ref_accepts_graph_vertex_ref() {
        use std::sync::Arc;

        let graph = Arc::new(Graph::new());
        let g = graph.gremlin(Arc::clone(&graph));

        let alice = g
            .add_v("person")
            .property("name", "Alice")
            .property("age", 30i64)
            .next()
            .unwrap();

        // v_ref accepts &GraphVertex
        let values = g.v_ref(&alice).values("name").to_list();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].as_str(), Some("Alice"));
    }

    #[test]
    fn test_v_ref_accepts_vertex_id() {
        use std::sync::Arc;

        let graph = Arc::new(Graph::new());
        let g = graph.gremlin(Arc::clone(&graph));

        let alice = g.add_v("person").property("name", "Alice").next().unwrap();

        // v_ref accepts VertexId
        let values = g.v_ref(alice.id()).values("name").to_list();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].as_str(), Some("Alice"));
    }

    #[test]
    fn test_full_gremlin_workflow_with_variables() {
        use std::sync::Arc;

        // This test demonstrates the target pattern from Spec 51:
        // alice = g.addV('person').property('name', 'Alice').next()
        // bob = g.addV('person').property('name', 'Bob').next()
        // g.addE('knows').from(alice).to(bob).next()
        // g.V(alice).out('knows').values('name').toList()

        let graph = Arc::new(Graph::new());
        let g = graph.gremlin(Arc::clone(&graph));

        // Create vertices and store references
        let alice = g
            .add_v("person")
            .property("name", "Alice")
            .property("age", 30i64)
            .next()
            .unwrap();
        let bob = g
            .add_v("person")
            .property("name", "Bob")
            .property("age", 25i64)
            .next()
            .unwrap();
        let charlie = g
            .add_v("person")
            .property("name", "Charlie")
            .property("age", 35i64)
            .next()
            .unwrap();
        let acme = g
            .add_v("company")
            .property("name", "Acme Corp")
            .next()
            .unwrap();

        // Create edges using the stored references (no .id() needed!)
        g.add_e("knows")
            .from(&alice)
            .to(&bob)
            .property("since", 2020i64)
            .next();
        g.add_e("knows")
            .from(&bob)
            .to(&charlie)
            .property("since", 2021i64)
            .next();
        g.add_e("works_at").from(&alice).to(&acme).next();
        g.add_e("works_at").from(&bob).to(&acme).next();

        // Verify counts
        assert_eq!(graph.vertex_count(), 4);
        assert_eq!(graph.edge_count(), 4);

        // Query using variables (no .id() needed!)
        let alice_knows: Vec<String> = g
            .v_ref(&alice)
            .out_label("knows")
            .values("name")
            .to_list()
            .into_iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        assert_eq!(alice_knows, vec!["Bob"]);

        // Chain traversals
        let alice_knows_knows: Vec<String> = g
            .v_ref(&alice)
            .out_label("knows")
            .out_label("knows")
            .values("name")
            .to_list()
            .into_iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        assert_eq!(alice_knows_knows, vec!["Charlie"]);

        // Query company employees
        let acme_employees: Vec<String> = g
            .v_ref(&acme)
            .in_label("works_at")
            .values("name")
            .to_list()
            .into_iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        assert_eq!(acme_employees.len(), 2);
        assert!(acme_employees.contains(&"Alice".to_string()));
        assert!(acme_employees.contains(&"Bob".to_string()));
    }

    #[test]
    fn test_bound_add_edge_builder_to_accepts_graph_vertex() {
        use std::sync::Arc;

        let graph = Arc::new(Graph::new());
        let g = graph.gremlin(Arc::clone(&graph));

        let alice = g.add_v("person").property("name", "Alice").next().unwrap();
        let bob = g.add_v("person").property("name", "Bob").next().unwrap();

        // Create edge from traversal using .to() with GraphVertex reference
        let edge = g.v_ref(&alice).add_e("knows").to(&bob).next();
        assert!(edge.is_some());
        assert_eq!(edge.unwrap().label(), Some("knows".to_string()));

        assert_eq!(graph.edge_count(), 1);
    }

    #[test]
    fn test_mixed_from_to_types() {
        use std::sync::Arc;

        let graph = Arc::new(Graph::new());
        let g = graph.gremlin(Arc::clone(&graph));

        let alice = g.add_v("person").next().unwrap();
        let bob_id = g.add_v("person").next().unwrap().id();

        // Mix GraphVertex reference with VertexId
        let edge = g.add_e("knows").from(&alice).to(bob_id).next();
        assert!(edge.is_some());

        // Mix VertexId with u64
        let charlie = g.add_v("person").next().unwrap().id();
        let edge2 = g.add_e("knows").from(charlie).to(1u64).next();
        assert!(edge2.is_some());
    }
}
