//! # Intersteller
//!
//! A high-performance Rust graph traversal library with a Gremlin-style fluent API.
//!
//! Intersteller provides a type-safe, ergonomic interface for graph operations with
//! support for both in-memory and persistent (memory-mapped) storage backends.
//!
//! ## Features
//!
//! - **Gremlin-style Fluent API**: Chainable traversal steps with lazy evaluation
//! - **Dual Storage Backends**: In-memory (HashMap-based) and memory-mapped (persistent)
//! - **Anonymous Traversals**: Composable fragments via the [`__`] factory module
//! - **Rich Predicate System**: Filtering with [`p::eq`], [`p::gt`], [`p::within`], [`p::regex`], and more
//! - **GQL Query Language**: Declarative queries as an alternative to the programmatic API
//! - **Thread-Safe**: Snapshot-based concurrency for safe parallel reads
//!
//! ## Quick Start
//!
//! ```rust
//! use intersteller::prelude::*;
//! use intersteller::storage::InMemoryGraph;
//!
//! // Create an in-memory graph
//! let mut storage = InMemoryGraph::new();
//!
//! // Add vertices with properties
//! let alice = storage.add_vertex("person", props! {
//!     "name" => "Alice",
//!     "age" => 30i64,
//! });
//!
//! let bob = storage.add_vertex("person", props! {
//!     "name" => "Bob",
//!     "age" => 25i64,
//! });
//!
//! // Add an edge
//! storage.add_edge(alice, bob, "knows", props! {}).unwrap();
//!
//! // Wrap storage in a Graph for traversal
//! let graph = Graph::new(storage);
//!
//! // Create a snapshot for read access
//! let snapshot = graph.snapshot();
//! let g = snapshot.traversal();
//!
//! // Traverse: find all people Alice knows
//! let friends = g.v_ids([alice])
//!     .out_labels(&["knows"])
//!     .values("name")
//!     .to_list();
//!
//! assert_eq!(friends, vec![Value::String("Bob".to_string())]);
//! ```
//!
//! ## Simpler Quick Start
//!
//! For the simplest setup, use the convenience constructor:
//!
//! ```rust
//! use intersteller::prelude::*;
//!
//! // Create an empty in-memory graph
//! let graph = Graph::in_memory();
//! let snapshot = graph.snapshot();
//! let g = snapshot.traversal();
//!
//! // Count vertices (0 in empty graph)
//! assert_eq!(g.v().count(), 0);
//! ```
//!
//! ## Module Overview
//!
//! | Module | Description |
//! |--------|-------------|
//! | [`graph`] | Graph container with snapshot-based concurrency ([`Graph`], [`GraphSnapshot`]) |
//! | [`storage`] | Storage backends ([`InMemoryGraph`](storage::InMemoryGraph), [`MmapGraph`](storage::mmap::MmapGraph)) |
//! | [`traversal`] | Fluent traversal API, steps, predicates ([`p`]), anonymous traversals ([`__`]) |
//! | [`value`] | Core value types ([`Value`], [`VertexId`], [`EdgeId`]) |
//! | [`error`] | Error types ([`StorageError`], [`TraversalError`], [`MutationError`](error::MutationError)) |
//! | [`gql`] | GQL query language parser and compiler |
//! | [`algorithms`] | Graph algorithms (BFS, DFS, shortest path) |
//!
//! ## Traversal API Overview
//!
//! The traversal API follows the Gremlin pattern with source, navigation, filter,
//! transform, branch, and terminal steps:
//!
//! ```rust
//! use intersteller::prelude::*;
//!
//! let graph = Graph::in_memory();
//! let snapshot = graph.snapshot();
//! let g = snapshot.traversal();
//!
//! // Source steps - where traversals begin
//! let _ = g.v();                    // All vertices
//! let _ = g.e();                    // All edges
//! // g.v_ids([id1, id2])            // Specific vertices
//! // g.inject([1, 2, 3])            // Inject arbitrary values
//!
//! // Navigation steps - traverse the graph structure
//! // .out("label")                  // Follow outgoing edges
//! // .in_("label")                  // Follow incoming edges
//! // .both("label")                 // Both directions
//! // .out_e() / .in_e() / .both_e() // Get edge objects
//! // .out_v() / .in_v() / .other_v() // Get edge endpoints
//!
//! // Filter steps - narrow down results
//! // .has("key")                    // Has property
//! // .has_value("key", value)       // Property equals value
//! // .has_where("key", p::gt(30))   // Property matches predicate
//! // .has_label("person")           // Filter by label
//! // .dedup()                       // Remove duplicates
//! // .limit(10)                     // Take first N
//!
//! // Transform steps - modify or extract data
//! // .values("name")                // Extract property value
//! // .value_map()                   // All properties as map
//! // .id() / .label()               // Element metadata
//! // .map(|v| ...)                  // Custom transformation
//!
//! // Terminal steps - execute and collect results
//! let count = g.v().count();        // Count results
//! let list = g.v().to_list();       // Collect all results
//! // .first()                       // First result (Option)
//! // .one()                         // Exactly one (Result)
//! ```
//!
//! ## Predicates
//!
//! The [`p`] module provides predicates for filtering:
//!
//! ```rust
//! use intersteller::prelude::*;
//!
//! // Comparison predicates
//! let _ = p::eq(30);                // Equals
//! let _ = p::gt(30);                // Greater than
//! let _ = p::between(20, 40);       // Range [20, 40)
//!
//! // Collection predicates
//! let _ = p::within([1i64, 2, 3]);  // In set
//!
//! // String predicates
//! let _ = p::containing("sub");     // Contains substring
//! let _ = p::regex(r"^\d+$");       // Regex match
//!
//! // Logical predicates
//! let _ = p::and(p::gt(20), p::lt(40));
//! let _ = p::or(p::lt(20), p::gt(40));
//! let _ = p::not(p::eq(30));
//! ```
//!
//! ## Anonymous Traversals
//!
//! The [`__`] module provides anonymous traversal fragments for composition:
//!
//! ```rust
//! use intersteller::prelude::*;
//!
//! let graph = Graph::in_memory();
//! let snapshot = graph.snapshot();
//! let g = snapshot.traversal();
//!
//! // Use in branch steps
//! let _ = g.v().union(vec![
//!     __::out_labels(&["knows"]),
//!     __::out_labels(&["created"]),
//! ]);
//!
//! // Use in repeat
//! let _ = g.v().repeat(__::out_labels(&["parent"])).times(3);
//!
//! // Use in where clauses
//! // g.v().where_(__::out("knows").count().is_(p::gt(3)))
//! ```
//!
//! ## GQL Query Language
//!
//! For declarative queries, use the GQL interface:
//!
//! ```rust
//! use intersteller::prelude::*;
//! use intersteller::storage::InMemoryGraph;
//!
//! let mut storage = InMemoryGraph::new();
//! storage.add_vertex("Person", props! {
//!     "name" => "Alice",
//! });
//!
//! let graph = Graph::new(storage);
//! let snapshot = graph.snapshot();
//!
//! // Execute a GQL query
//! let results = snapshot.gql("MATCH (n:Person) RETURN n.name").unwrap();
//! assert_eq!(results.len(), 1);
//! ```
//!
//! ## Error Handling
//!
//! Intersteller uses `Result` types throughout. See the [`error`] module for details
//! on error types and recovery patterns:
//!
//! ```rust
//! use intersteller::prelude::*;
//!
//! let graph = Graph::in_memory();
//! let snapshot = graph.snapshot();
//! let g = snapshot.traversal();
//!
//! // Handle "exactly one" requirement
//! match g.v().one() {
//!     Ok(vertex) => println!("Found: {:?}", vertex),
//!     Err(TraversalError::NotOne(0)) => println!("No vertices found"),
//!     Err(TraversalError::NotOne(n)) => println!("Too many: {}", n),
//!     Err(e) => println!("Error: {}", e),
//! }
//! ```
//!
//! ## Storage Backends
//!
//! ### In-Memory (Default)
//!
//! Fast HashMap-based storage for development and small graphs:
//!
//! ```rust
//! use intersteller::storage::InMemoryGraph;
//!
//! let storage = InMemoryGraph::new();
//! // Use directly or wrap in Graph for traversal
//! ```
//!
//! ### Memory-Mapped (Persistent)
//!
//! Persistent storage with write-ahead logging. Enable with the `mmap` feature:
//!
//! ```toml
//! [dependencies]
//! intersteller = { version = "0.1", features = ["mmap"] }
//! ```
//!
//! ```ignore
//! use intersteller::storage::MmapGraph;
//!
//! let graph = MmapGraph::open("my_graph.db").unwrap();
//! // Data persists across restarts
//! ```
//!
//! ## Feature Flags
//!
//! | Feature | Description | Default |
//! |---------|-------------|---------|
//! | `inmemory` | In-memory storage backend | Yes |
//! | `mmap` | Memory-mapped persistent storage | No |
//! | `full-text` | Full-text search (planned) | No |
//!
//! ## Thread Safety
//!
//! [`Graph`] uses a readers-writer lock for safe concurrent access:
//!
//! - Multiple [`GraphSnapshot`]s can exist simultaneously (shared reads)
//! - [`GraphMut`] requires exclusive access (exclusive writes)
//! - Snapshots see a consistent view of the graph
//!
//! ```rust
//! use intersteller::prelude::*;
//! use std::sync::Arc;
//! use std::thread;
//!
//! let graph = Arc::new(Graph::in_memory());
//!
//! // Multiple threads can read concurrently
//! let handles: Vec<_> = (0..4).map(|_| {
//!     let g = Arc::clone(&graph);
//!     thread::spawn(move || {
//!         let snap = g.snapshot();
//!         snap.traversal().v().count()
//!     })
//! }).collect();
//!
//! for handle in handles {
//!     let _ = handle.join().unwrap();
//! }
//! ```
//!
//! ## Examples
//!
//! The `examples/` directory contains comprehensive demonstrations:
//!
//! - `basic_traversal.rs` - Getting started with traversals
//! - `navigation_steps.rs` - Graph navigation patterns
//! - `filter_steps.rs` - Filtering and predicates
//! - `branch_steps.rs` - Branching and conditional logic
//! - `repeat_steps.rs` - Iterative traversals
//! - `british_royals.rs` - Real-world family tree queries
//! - `nba.rs` - Sports analytics queries
//!
//! Run examples with:
//!
//! ```bash
//! cargo run --example basic_traversal
//! cargo run --example british_royals
//! ```

/// Creates a property map for vertices and edges.
///
/// This macro provides a convenient way to construct `HashMap<String, Value>`
/// for use with [`InMemoryGraph::add_vertex`](storage::InMemoryGraph::add_vertex)
/// and [`InMemoryGraph::add_edge`](storage::InMemoryGraph::add_edge).
///
/// Values are automatically converted using [`Into<Value>`](Value), so you can
/// use native Rust types directly.
///
/// # Example
///
/// ```rust
/// use intersteller::prelude::*;
/// use intersteller::storage::InMemoryGraph;
///
/// let mut storage = InMemoryGraph::new();
///
/// // Create a vertex with properties
/// let alice = storage.add_vertex("person", props! {
///     "name" => "Alice",
///     "age" => 30i64,
///     "active" => true,
/// });
///
/// // Empty properties
/// let bob = storage.add_vertex("person", props! {});
///
/// // Edge with properties
/// storage.add_edge(alice, bob, "knows", props! {
///     "since" => 2020i64,
///     "weight" => 0.95,
/// }).unwrap();
/// ```
///
/// # Supported Types
///
/// Any type that implements `Into<Value>` can be used:
/// - `&str` and `String` → `Value::String`
/// - `i64` → `Value::Int`
/// - `f64` → `Value::Float`
/// - `bool` → `Value::Bool`
/// - `Vec<Value>` → `Value::List`
/// - `HashMap<String, Value>` → `Value::Map`
///
/// **Note**: For integers, use `i64` explicitly (e.g., `30i64`) since Rust's
/// default integer type is `i32` which doesn't implement `Into<Value>`.
#[macro_export]
macro_rules! props {
    // Empty case
    () => {
        ::std::collections::HashMap::new()
    };
    // Key-value pairs with trailing comma support
    ($($key:expr => $value:expr),* $(,)?) => {{
        #[allow(unused_mut)]
        let mut map = ::std::collections::HashMap::new();
        $(
            map.insert($key.to_string(), $crate::Value::from($value));
        )*
        map
    }};
}

pub mod algorithms;
pub mod error;
pub mod gql;
pub mod graph;
pub mod schema;
pub mod storage;
pub mod traversal;
pub mod value;

/// The prelude module re-exports commonly used types.
///
/// Import the prelude to get started quickly:
///
/// ```rust
/// use intersteller::prelude::*;
///
/// let graph = Graph::in_memory();
/// let snapshot = graph.snapshot();
/// let g = snapshot.traversal();
/// ```
///
/// This imports:
///
/// - Graph types: [`Graph`], [`GraphSnapshot`], [`GraphMut`]
/// - Traversal: [`Traversal`], [`BoundTraversal`], [`GraphTraversalSource`]
/// - Anonymous traversals: [`__`]
/// - Predicates: [`p`]
/// - Values: [`Value`], [`VertexId`], [`EdgeId`], [`ElementId`]
/// - Paths: [`Path`], [`PathElement`], [`PathValue`], [`Traverser`]
/// - Errors: [`StorageError`], [`TraversalError`]
/// - Macros: [`props!`]
pub mod prelude {
    pub use crate::error::{StorageError, TraversalError};
    pub use crate::graph::{Graph, GraphMut, GraphSnapshot};
    pub use crate::props;
    pub use crate::traversal::{
        p, BoundTraversal, CloneSack, ExecutionContext, GraphTraversalSource, GroupKey, GroupValue,
        Path, PathElement, PathValue, Traversal, Traverser, __,
    };
    pub use crate::value::{EdgeId, ElementId, Value, VertexId};
}

pub use prelude::*;
