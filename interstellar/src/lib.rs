#![feature(impl_trait_in_assoc_type)]
//! # Interstellar
//!
//! A high-performance Rust graph traversal library with a Gremlin-style fluent API.
//!
//! Interstellar provides a type-safe, ergonomic interface for graph operations with
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
//! use interstellar::prelude::*;
//! use std::collections::HashMap;
//!
//! // Create a new graph
//! let graph = Graph::new();
//!
//! // Add vertices with properties
//! let alice = graph.add_vertex("person", HashMap::from([
//!     ("name".to_string(), Value::from("Alice")),
//!     ("age".to_string(), Value::from(30i64)),
//! ]));
//!
//! let bob = graph.add_vertex("person", HashMap::from([
//!     ("name".to_string(), Value::from("Bob")),
//!     ("age".to_string(), Value::from(25i64)),
//! ]));
//!
//! // Add an edge
//! graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
//!
//! // Create a snapshot for read access
//! let snapshot = graph.snapshot();
//! let g = snapshot.gremlin();
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
//! use interstellar::prelude::*;
//!
//! // Create an empty in-memory graph
//! let graph = Graph::in_memory();
//! let snapshot = graph.snapshot();
//! let g = snapshot.gremlin();
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
//! | [`storage`] | Storage backends ([`Graph`](storage::Graph), [`MmapGraph`](storage::mmap::MmapGraph)) |
//! | [`traversal`] | Fluent traversal API, steps, predicates ([`p`]), anonymous traversals ([`__`]) |
//! | [`value`] | Core value types ([`Value`], [`VertexId`], [`EdgeId`]) |
//! | [`error`] | Error types ([`StorageError`], [`TraversalError`], [`MutationError`](error::MutationError)) |
//! | [`gql`] | GQL query language parser and compiler (requires `gql` feature) |
//! | [`algorithms`] | Graph algorithms (BFS, DFS, shortest path) |
//!
//! ## Traversal API Overview
//!
//! The traversal API follows the Gremlin pattern with source, navigation, filter,
//! transform, branch, and terminal steps:
//!
//! ```rust
//! use interstellar::prelude::*;
//!
//! let graph = Graph::in_memory();
//! let snapshot = graph.snapshot();
//! let g = snapshot.gremlin();
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
//! use interstellar::prelude::*;
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
//! use interstellar::prelude::*;
//!
//! let graph = Graph::in_memory();
//! let snapshot = graph.snapshot();
//! let g = snapshot.gremlin();
//!
//! // Use in branch steps
//! let _ = g.v().union(vec![
//!     __.out_labels(&["knows"]),
//!     __.out_labels(&["created"]),
//! ]);
//!
//! // Use in repeat
//! let _ = g.v().repeat(__.out_labels(&["parent"])).times(3);
//!
//! // Use in where clauses
//! // g.v().where_(__.out("knows").count().is_(p::gt(3)))
//! ```
//!
//! ## GQL Query Language
//!
//! For declarative queries, enable the `gql` feature and use the GQL interface:
//!
//! ```toml
//! [dependencies]
//! interstellar = { version = "0.1", features = ["gql"] }
//! ```
//!
//! ```rust,ignore
//! use interstellar::prelude::*;
//! use std::collections::HashMap;
//!
//! let graph = Graph::new();
//! graph.add_vertex("Person", HashMap::from([
//!     ("name".to_string(), Value::from("Alice")),
//! ]));
//!
//! let snapshot = graph.snapshot();
//!
//! // Execute a GQL query (requires `gql` feature)
//! let results = graph.gql("MATCH (n:Person) RETURN n.name").unwrap();
//! assert_eq!(results.len(), 1);
//! ```
//!
//! ## Error Handling
//!
//! Interstellar uses `Result` types throughout. See the [`error`] module for details
//! on error types and recovery patterns:
//!
//! ```rust
//! use interstellar::prelude::*;
//!
//! let graph = Graph::in_memory();
//! let snapshot = graph.snapshot();
//! let g = snapshot.gremlin();
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
//! The COW (Copy-on-Write) graph for development and small graphs:
//!
//! ```rust
//! use interstellar::storage::Graph;
//!
//! let graph = Graph::new();
//! // Use directly with traversal API
//! ```
//!
//! ### Memory-Mapped (Persistent)
//!
//! Persistent storage with write-ahead logging. Enable with the `mmap` feature:
//!
//! ```toml
//! [dependencies]
//! interstellar = { version = "0.1", features = ["mmap"] }
//! ```
//!
//! ```ignore
//! use interstellar::storage::MmapGraph;
//!
//! let graph = MmapGraph::open("my_graph.db").unwrap();
//! // Data persists across restarts
//! ```
//!
//! ## Feature Flags
//!
//! | Feature | Description | Default |
//! |---------|-------------|---------|
//! | `graphson` | GraphSON import/export support | Yes |
//! | `mmap` | Memory-mapped persistent storage (**not available on WASM**) | No |
//! | `gql` | GQL query language support | No |
//! | `full-text` | Full-text search with Tantivy (**not available on WASM**) | No |
//! | `full` | Enable all features | No |
//!
//! Note: In-memory graph storage is always available (core functionality).
//!
//! ## WASM Support
//!
//! Interstellar supports WebAssembly targets (`wasm32-unknown-unknown`).
//! The following features work on WASM:
//!
//! - Core in-memory `Graph`
//! - Full traversal API
//! - GQL query language (with `gql` feature)
//! - GraphSON serialization (string-based only; file I/O excluded)
//!
//! Build for WASM:
//!
//! ```bash
//! cargo build --target wasm32-unknown-unknown
//! cargo build --target wasm32-unknown-unknown --features gql
//! ```
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
//! use interstellar::prelude::*;
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
//!         snap.gremlin().v().count()
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
/// for use with [`Graph::add_vertex`](storage::Graph::add_vertex)
/// and [`Graph::add_edge`](storage::Graph::add_edge).
///
/// Values are automatically converted using [`Into<Value>`](Value), so you can
/// use native Rust types directly.
///
/// # Example
///
/// ```rust
/// use interstellar::prelude::*;
/// use interstellar::storage::Graph;
///
/// let graph = Graph::new();
///
/// // Create a vertex with properties
/// let alice = graph.add_vertex("person", props! {
///     "name" => "Alice",
///     "age" => 30i64,
///     "active" => true,
/// });
///
/// // Empty properties
/// let bob = graph.add_vertex("person", props! {});
///
/// // Edge with properties
/// graph.add_edge(alice, bob, "knows", props! {
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
#[cfg(feature = "gql")]
pub mod gql;
pub mod graph_access;
pub mod graph_elements;
#[cfg(feature = "graphson")]
pub mod graphson;
#[cfg(feature = "gremlin")]
pub mod gremlin;
pub mod index;
#[cfg(feature = "mmap")]
pub mod query;
pub mod schema;
pub mod storage;
// Internal time abstraction for WASM compatibility
pub(crate) mod time;
pub mod traversal;
pub mod value;
// WASM JavaScript bindings
#[cfg(feature = "wasm")]
pub mod wasm;

// Re-export graph element types for convenience
pub use graph_elements::{GraphEdge, GraphVertex, GraphVertexTraversal};

// Re-export GraphAccess trait for generic graph element support
pub use graph_access::GraphAccess;

#[cfg(kani)]
mod kani_proofs;

/// The prelude module re-exports commonly used types.
///
/// Import the prelude to get started quickly:
///
/// ```rust
/// use interstellar::prelude::*;
///
/// let graph = Graph::new();
/// let snapshot = graph.snapshot();
/// let g = snapshot.gremlin();
/// ```
///
/// This imports:
///
/// - Graph types: [`Graph`], [`GraphSnapshot`]
/// - Persistent graph types (mmap feature): [`PersistentGraph`], [`PersistentSnapshot`]
/// - Traversal: [`Traversal`], [`BoundTraversal`], [`GraphTraversalSource`]
/// - Anonymous traversals: [`__`]
/// - Predicates: [`p`]
/// - Values: [`Value`], [`VertexId`], [`EdgeId`], [`ElementId`]
/// - Paths: [`Path`], [`PathElement`], [`PathValue`], [`Traverser`]
/// - Errors: [`StorageError`], [`TraversalError`]
/// - Macros: [`props!`]
pub mod prelude {
    pub use crate::error::{StorageError, TraversalError};
    // Primary graph types (from storage::cow)
    pub use crate::props;
    pub use crate::storage::{Graph, GraphSnapshot};
    // Persistent graph types (mmap feature)
    #[cfg(feature = "mmap")]
    pub use crate::storage::{PersistentGraph, PersistentSnapshot};
    pub use crate::traversal::{
        p, BoundTraversal, CloneSack, ExecutionContext, GraphTraversalSource, GroupKey, GroupValue,
        Path, PathElement, PathValue, Traversal, Traverser, __,
    };
    pub use crate::value::{EdgeId, ElementId, IntoVertexId, Value, VertexId};
}

pub use prelude::*;
