//! Error types for storage and traversal operations.
//!
//! This module defines the error types used throughout Interstellar:
//!
//! - [`StorageError`] - Errors from storage backend operations
//! - [`TraversalError`] - Errors during graph traversals
//! - [`MutationError`] - Errors during graph mutations
//!
//! All error types implement [`std::error::Error`] and [`std::fmt::Display`]
//! via the [`thiserror`](https://docs.rs/thiserror) crate.
//!
//! # Error Handling Philosophy
//!
//! Interstellar follows Rust conventions by returning `Result` types for fallible
//! operations. The library **never panics** in normal operation—all error conditions
//! are communicated through return values.
//!
//! # When Errors Occur
//!
//! | Operation | Possible Errors |
//! |-----------|-----------------|
//! | Creating edges | [`StorageError::VertexNotFound`] if source/target doesn't exist |
//! | Looking up elements | [`StorageError::VertexNotFound`], [`StorageError::EdgeNotFound`] |
//! | Persistent storage | [`StorageError::Io`], [`StorageError::WalCorrupted`] |
//! | `.one()` terminal | [`TraversalError::NotOne`] if count ≠ 1 |
//! | Mutations | Various [`MutationError`] variants |
//!
//! # Quick Reference
//!
//! ```rust
//! use interstellar::prelude::*;
//! use interstellar::error::MutationError;
//!
//! // Most storage operations return Result<T, StorageError>
//! // Most traversal terminals return T directly or Result<T, TraversalError>
//!
//! // Terminal steps that return Result:
//! // - .one() -> Result<Value, TraversalError>
//!
//! // Terminal steps that return values directly:
//! // - .to_list() -> Vec<Value>
//! // - .count() -> usize
//! // - .first() -> Option<Value>
//! // - .has_next() -> bool
//! ```
//!
//! # Recovery Patterns
//!
//! ## Pattern 1: Match on Specific Errors
//!
//! Use pattern matching when you need to handle different error cases differently:
//!
//! ```rust
//! use interstellar::prelude::*;
//! use interstellar::storage::{Graph, GraphStorageMut};
//! use std::collections::HashMap;
//!
//! let graph = Graph::new();
//! let mut storage = graph.as_storage_mut();
//!
//! // Attempting to create an edge with invalid vertices
//! let result = storage.add_edge(
//!     VertexId(999),  // doesn't exist
//!     VertexId(888),  // doesn't exist
//!     "knows",
//!     HashMap::new(),
//! );
//!
//! match result {
//!     Ok(edge_id) => println!("Created edge: {:?}", edge_id),
//!     Err(StorageError::VertexNotFound(id)) => {
//!         // Specific handling: maybe create the missing vertex?
//!         println!("Cannot create edge: vertex {:?} doesn't exist", id);
//!     }
//!     Err(e) => {
//!         // Generic fallback for other storage errors
//!         println!("Storage error: {}", e);
//!     }
//! }
//! ```
//!
//! ## Pattern 2: Use the `?` Operator
//!
//! For functions that return `Result`, use `?` for concise error propagation:
//!
//! ```rust
//! use interstellar::prelude::*;
//! use std::collections::HashMap;
//!
//! fn setup_graph() -> Result<Graph, StorageError> {
//!     let graph = Graph::new();
//!     
//!     let alice = graph.add_vertex("person", HashMap::from([
//!         ("name".to_string(), Value::from("Alice")),
//!     ]));
//!     
//!     let bob = graph.add_vertex("person", HashMap::from([
//!         ("name".to_string(), Value::from("Bob")),
//!     ]));
//!     
//!     // The `?` propagates any error up to the caller
//!     graph.add_edge(alice, bob, "knows", HashMap::new())?;
//!     
//!     Ok(graph)
//! }
//! ```
//!
//! ## Pattern 3: Provide Defaults with `unwrap_or`
//!
//! When you have a sensible default for error cases:
//!
//! ```rust
//! use interstellar::prelude::*;
//!
//! let graph = Graph::in_memory();
//! let snapshot = graph.snapshot();
//! let g = snapshot.gremlin();
//!
//! // Use next() which returns Option, then provide a default
//! let name = g.v()
//!     .has_label("person")
//!     .values("name")
//!     .next()
//!     .unwrap_or(Value::String("Unknown".to_string()));
//! ```
//!
//! ## Pattern 4: Handle `.one()` Cardinality Errors
//!
//! The `.one()` terminal step is strict—it requires exactly one result:
//!
//! ```rust
//! use interstellar::prelude::*;
//! use std::collections::HashMap;
//!
//! let graph = Graph::new();
//! graph.add_vertex("person", HashMap::from([
//!     ("name".to_string(), Value::from("Alice")),
//! ]));
//! graph.add_vertex("person", HashMap::from([
//!     ("name".to_string(), Value::from("Bob")),
//! ]));
//!
//! let snapshot = graph.snapshot();
//! let g = snapshot.gremlin();
//!
//! // This will fail because there are 2 people
//! match g.v().has_label("person").one() {
//!     Ok(vertex) => println!("Found the person: {:?}", vertex),
//!     Err(TraversalError::NotOne(0)) => {
//!         println!("No people found - perhaps create one?");
//!     }
//!     Err(TraversalError::NotOne(count)) => {
//!         println!("Expected 1 person but found {}. Use to_list() or add filters.", count);
//!     }
//!     Err(e) => println!("Unexpected error: {}", e),
//! }
//!
//! // Better: Use next() if you just want any result
//! if let Some(vertex) = g.v().has_label("person").next() {
//!     println!("Found a person: {:?}", vertex);
//! }
//!
//! // Or use to_list() if you want all results
//! let all_people = g.v().has_label("person").to_list();
//! println!("Found {} people", all_people.len());
//! ```
//!
//! ## Pattern 5: Retry Logic for I/O Errors
//!
//! For persistent storage, I/O errors may be transient:
//!
//! ```ignore
//! use interstellar::prelude::*;
//! use interstellar::storage::MmapGraph;
//! use std::thread;
//! use std::time::Duration;
//!
//! fn open_with_retry(path: &str, max_retries: u32) -> Result<MmapGraph, StorageError> {
//!     let mut attempts = 0;
//!     
//!     loop {
//!         match MmapGraph::open(path) {
//!             Ok(graph) => return Ok(graph),
//!             Err(StorageError::Io(ref e)) if attempts < max_retries => {
//!                 // Transient I/O error - retry with backoff
//!                 attempts += 1;
//!                 let delay = Duration::from_millis(100 * 2_u64.pow(attempts));
//!                 eprintln!("I/O error (attempt {}): {}. Retrying in {:?}...",
//!                           attempts, e, delay);
//!                 thread::sleep(delay);
//!             }
//!             Err(e) => return Err(e),
//!         }
//!     }
//! }
//! ```
//!
//! ## Pattern 6: Graceful Degradation
//!
//! Handle errors by falling back to alternative behavior:
//!
//! ```rust
//! use interstellar::prelude::*;
//! use std::collections::HashMap;
//! use std::sync::Arc;
//!
//! fn get_user_friends(graph: &Graph, user_id: VertexId) -> Vec<Value> {
//!     let snapshot = graph.snapshot();
//!     let g = snapshot.gremlin();
//!     
//!     // Try to get friends, but return empty list if user doesn't exist
//!     // (Note: traversals handle missing vertices gracefully by returning empty results)
//!     g.v_ids([user_id])
//!         .out_labels(&["knows"])
//!         .values("name")
//!         .to_list()
//! }
//!
//! let graph = Graph::in_memory();
//!
//! // Even with an invalid ID, this returns an empty Vec, not an error
//! let friends = get_user_friends(&graph, VertexId(999));
//! assert!(friends.is_empty());
//! ```
//!
//! ## Pattern 7: Error Conversion with `From`
//!
//! Errors automatically convert between types using the `From` trait:
//!
//! ```rust
//! use interstellar::prelude::*;
//!
//! // TraversalError can wrap StorageError
//! fn combined_operation() -> Result<(), TraversalError> {
//!     let storage_error = StorageError::VertexNotFound(VertexId(1));
//!     
//!     // Automatically converts StorageError -> TraversalError
//!     Err(storage_error)?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! # Logging and Debugging
//!
//! All error types implement `Debug` and `Display`:
//!
//! ```rust
//! use interstellar::prelude::*;
//!
//! let error = StorageError::VertexNotFound(VertexId(42));
//!
//! // For user-facing messages, use Display (via {})
//! println!("Error: {}", error);
//! // Output: "Error: vertex not found: VertexId(42)"
//!
//! // For debugging, use Debug (via {:?})
//! println!("Debug: {:?}", error);
//! // Output: "Debug: VertexNotFound(VertexId(42))"
//! ```

use crate::value::{EdgeId, VertexId};

// =============================================================================
// StorageError
// =============================================================================

/// Errors that can occur during storage operations.
///
/// `StorageError` represents failures when interacting with the graph storage
/// backend, including missing elements, I/O failures, and data corruption.
///
/// # Variants
///
/// | Variant | Cause | Recovery |
/// |---------|-------|----------|
/// | [`VertexNotFound`](Self::VertexNotFound) | Vertex ID doesn't exist | Create vertex or check ID |
/// | [`EdgeNotFound`](Self::EdgeNotFound) | Edge ID doesn't exist | Create edge or check ID |
/// | [`Io`](Self::Io) | File system error | Retry or check permissions |
/// | [`WalCorrupted`](Self::WalCorrupted) | Write-ahead log damaged | Restore from backup |
/// | [`InvalidFormat`](Self::InvalidFormat) | File isn't a valid database | Check file path |
/// | [`CorruptedData`](Self::CorruptedData) | Data integrity failure | Restore from backup |
/// | [`OutOfSpace`](Self::OutOfSpace) | Storage capacity exceeded | Free space or resize |
///
/// # Common Causes
///
/// ## `VertexNotFound` / `EdgeNotFound`
///
/// These occur when referencing IDs that don't exist. Common causes:
/// - Using a hardcoded ID that was never created
/// - Using an ID from a previous session (IDs aren't stable across restarts for MmapGraph)
/// - Element was deleted but ID is still cached
///
/// ## `Io`
///
/// I/O errors from the underlying file system. Common causes:
/// - File permissions insufficient
/// - Disk full
/// - File locked by another process
/// - Network storage unavailable
///
/// ## `WalCorrupted` / `CorruptedData`
///
/// Data integrity issues. Common causes:
/// - Crash during write operation
/// - Disk hardware failure
/// - Manual file modification
///
/// # Example
///
/// ```rust
/// use interstellar::prelude::*;
/// use interstellar::storage::{Graph, GraphStorageMut};
/// use std::collections::HashMap;
///
/// let graph = Graph::new();
/// let mut storage = graph.as_storage_mut();
/// let alice = storage.add_vertex("person", HashMap::new());
///
/// // Try to create an edge to a non-existent vertex
/// let result = storage.add_edge(alice, VertexId(999), "knows", HashMap::new());
///
/// match result {
///     Ok(_) => println!("Edge created"),
///     Err(StorageError::VertexNotFound(id)) => {
///         println!("Vertex {:?} doesn't exist - create it first!", id);
///     }
///     Err(e) => println!("Other error: {}", e),
/// }
/// ```
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The requested vertex does not exist in the graph.
    ///
    /// This error is returned when attempting to:
    /// - Create an edge from or to a non-existent vertex
    /// - Look up a vertex by ID that doesn't exist
    /// - Modify properties on a deleted vertex
    ///
    /// # Recovery
    ///
    /// - Verify the vertex ID is correct
    /// - Create the vertex if it should exist
    /// - Check if the vertex was deleted
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::storage::{Graph, GraphStorageMut};
    /// use std::collections::HashMap;
    ///
    /// let graph = Graph::new();
    /// let mut storage = graph.as_storage_mut();
    ///
    /// // This vertex doesn't exist
    /// let result = storage.add_edge(
    ///     VertexId(999),
    ///     VertexId(888),
    ///     "knows",
    ///     HashMap::new()
    /// );
    ///
    /// assert!(matches!(result, Err(StorageError::VertexNotFound(_))));
    /// ```
    #[error("vertex not found: {0:?}")]
    VertexNotFound(VertexId),

    /// The requested edge does not exist in the graph.
    ///
    /// This error is returned when attempting to:
    /// - Look up an edge by ID that doesn't exist
    /// - Modify properties on a deleted edge
    ///
    /// # Recovery
    ///
    /// - Verify the edge ID is correct
    /// - Create the edge if it should exist
    /// - Check if the edge was deleted
    #[error("edge not found: {0:?}")]
    EdgeNotFound(EdgeId),

    /// An I/O operation failed.
    ///
    /// This typically occurs with persistent storage backends when reading
    /// from or writing to disk fails. The wrapped [`std::io::Error`] contains
    /// details about the specific failure.
    ///
    /// # Common Causes
    ///
    /// - Insufficient file permissions
    /// - Disk is full
    /// - File is locked by another process
    /// - Network storage is unavailable
    /// - File was deleted while in use
    ///
    /// # Recovery
    ///
    /// - Check file permissions (`chmod` / `chown`)
    /// - Free disk space
    /// - Close other processes using the file
    /// - Wait and retry for network issues
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::storage::MmapGraph;
    ///
    /// match MmapGraph::open("/nonexistent/path/graph.db") {
    ///     Ok(graph) => { /* use graph */ }
    ///     Err(StorageError::Io(io_error)) => {
    ///         match io_error.kind() {
    ///             std::io::ErrorKind::NotFound => {
    ///                 println!("Directory doesn't exist - creating it...");
    ///             }
    ///             std::io::ErrorKind::PermissionDenied => {
    ///                 println!("Permission denied - check file ownership");
    ///             }
    ///             _ => println!("I/O error: {}", io_error),
    ///         }
    ///     }
    ///     Err(e) => println!("Other error: {}", e),
    /// }
    /// ```
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The write-ahead log is corrupted.
    ///
    /// This indicates data integrity issues with the WAL file. The WAL ensures
    /// crash recovery, so corruption here means some recent operations may be lost.
    ///
    /// # Common Causes
    ///
    /// - System crash during write operation
    /// - Disk hardware failure
    /// - Manual modification of database files
    ///
    /// # Recovery
    ///
    /// 1. **If you have backups**: Restore from the most recent backup
    /// 2. **If no backups**: The database may still be usable—the WAL contains
    ///    only uncommitted changes. Try opening with recovery mode (if available).
    /// 3. **Prevent future issues**: Use `commit_batch()` for important data,
    ///    ensure proper shutdown, check disk health.
    #[error("WAL corrupted: {0}")]
    WalCorrupted(String),

    /// The storage file format is invalid.
    ///
    /// This can occur when opening a file that isn't a valid Interstellar
    /// database or when the file version is incompatible.
    ///
    /// # Common Causes
    ///
    /// - Wrong file path (opening a non-database file)
    /// - Database created with incompatible version
    /// - File truncated or partially overwritten
    ///
    /// # Recovery
    ///
    /// - Verify the file path is correct
    /// - Check the file is actually a Interstellar database
    /// - If upgrading, check for migration tools
    #[error("invalid file format")]
    InvalidFormat,

    /// The database file contains corrupted data.
    ///
    /// This indicates data integrity issues in the main database file,
    /// such as reading beyond valid offsets or encountering malformed records.
    ///
    /// # Common Causes
    ///
    /// - Disk hardware failure
    /// - File system corruption
    /// - Incomplete writes due to crash
    ///
    /// # Recovery
    ///
    /// - Restore from backup
    /// - Run file system check (`fsck`)
    /// - Check disk health (`smartctl`)
    #[error("corrupted data")]
    CorruptedData,

    /// The storage is out of space.
    ///
    /// This occurs when attempting to allocate space in fixed-size regions
    /// (property arena, string interner, etc.) and there isn't enough room.
    ///
    /// # Recovery
    ///
    /// - For MmapGraph: The database will auto-expand; this indicates a bug
    /// - Free disk space if the underlying file can't grow
    /// - Consider archiving old data
    #[error("out of space")]
    OutOfSpace,

    /// An index operation failed.
    ///
    /// This occurs when a property index constraint is violated, such as
    /// inserting a duplicate value into a unique index.
    ///
    /// # Common Causes
    ///
    /// - Unique index constraint violation (duplicate property value)
    /// - Index creation failed due to existing duplicates
    ///
    /// # Recovery
    ///
    /// - Ensure unique property values before inserting
    /// - Drop and recreate index after cleaning up duplicates
    #[error("index error: {0}")]
    IndexError(String),
}

// =============================================================================
// TraversalError
// =============================================================================

/// Errors that can occur during graph traversals.
///
/// `TraversalError` represents failures during traversal execution, including
/// cardinality violations and underlying storage errors.
///
/// # When Traversals Error
///
/// Most traversal operations are **infallible**—they return empty results rather
/// than errors when elements don't match. Errors only occur for:
///
/// | Situation | Error |
/// |-----------|-------|
/// | `.one()` with 0 or 2+ results | [`NotOne`](Self::NotOne) |
/// | Storage failure during traversal | [`Storage`](Self::Storage) |
/// | Mutation failure during traversal | [`Mutation`](Self::Mutation) |
///
/// # Error Conversion
///
/// `TraversalError` implements `From<StorageError>` and `From<MutationError>`,
/// allowing automatic conversion when using the `?` operator:
///
/// ```rust
/// use interstellar::prelude::*;
///
/// fn example() -> Result<(), TraversalError> {
///     // StorageError automatically converts to TraversalError
///     let storage_err: StorageError = StorageError::VertexNotFound(VertexId(1));
///     Err(storage_err)?;
///     Ok(())
/// }
/// ```
///
/// # Example
///
/// ```rust
/// use interstellar::prelude::*;
///
/// let graph = Graph::in_memory();
/// let snapshot = graph.snapshot();
/// let g = snapshot.gremlin();
///
/// // Empty graph: one() returns NotOne(0)
/// let result = g.v().one();
/// match result {
///     Ok(v) => println!("Found: {:?}", v),
///     Err(TraversalError::NotOne(0)) => {
///         println!("No results - graph is empty or filter too restrictive");
///     }
///     Err(TraversalError::NotOne(n)) => {
///         println!("Multiple results ({}) - add more filters", n);
///     }
///     Err(e) => println!("Error: {}", e),
/// }
/// ```
#[derive(Debug, thiserror::Error)]
pub enum TraversalError {
    /// Expected exactly one result but found a different count.
    ///
    /// This error is returned by the `.one()` terminal step when the
    /// traversal doesn't yield exactly one element.
    ///
    /// The contained `usize` indicates how many elements were actually found:
    /// - `0` means no elements matched
    /// - `> 1` means multiple elements matched
    ///
    /// # Recovery
    ///
    /// | Count | Meaning | Solution |
    /// |-------|---------|----------|
    /// | 0 | No matches | Check filters, verify data exists |
    /// | > 1 | Too many matches | Add more specific filters |
    ///
    /// # Alternatives to `.one()`
    ///
    /// If you don't need exactly one result:
    ///
    /// - `.first()` - Returns `Option<Value>`, never errors
    /// - `.to_list()` - Returns `Vec<Value>`, handles any count
    /// - `.count()` - Returns count without collecting
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use std::collections::HashMap;
    ///
    /// let graph = Graph::new();
    /// // Add two people - one() will fail
    /// graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), Value::from("Alice")),
    /// ]));
    /// graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), Value::from("Bob")),
    /// ]));
    ///
    /// let snapshot = graph.snapshot();
    /// let g = snapshot.gremlin();
    ///
    /// // Fails because there are 2 people
    /// match g.v().has_label("person").one() {
    ///     Err(TraversalError::NotOne(2)) => {
    ///         // Use a more specific query
    ///         let alice = g.v()
    ///             .has_label("person")
    ///             .has_value("name", "Alice")
    ///             .one()
    ///             .unwrap();
    ///         println!("Found Alice: {:?}", alice);
    ///     }
    ///     _ => {}
    /// }
    /// ```
    #[error("expected exactly one result, found {0}")]
    NotOne(usize),

    /// A storage operation failed during traversal.
    ///
    /// This wraps a [`StorageError`] that occurred while executing the traversal.
    /// See [`StorageError`] documentation for specific error types and recovery.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    ///
    /// let err = TraversalError::Storage(StorageError::VertexNotFound(VertexId(1)));
    ///
    /// // Extract the underlying storage error
    /// if let TraversalError::Storage(storage_err) = err {
    ///     println!("Storage failed: {}", storage_err);
    /// }
    /// ```
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    /// A mutation operation failed during traversal.
    ///
    /// This wraps a [`MutationError`] that occurred while executing a mutation
    /// step like `addV()`, `addE()`, `property()`, or `drop()`.
    ///
    /// See [`MutationError`] documentation for specific error types and recovery.
    #[error("mutation error: {0}")]
    Mutation(#[from] MutationError),
}

// =============================================================================
// MutationError
// =============================================================================

/// Errors that can occur during mutation operations.
///
/// `MutationError` represents failures during graph mutation steps such as
/// `addV()`, `addE()`, `property()`, and `drop()`.
///
/// # Variants
///
/// | Variant | Cause | Recovery |
/// |---------|-------|----------|
/// | [`EdgeSourceNotFound`](Self::EdgeSourceNotFound) | `from` vertex doesn't exist | Create vertex first |
/// | [`EdgeTargetNotFound`](Self::EdgeTargetNotFound) | `to` vertex doesn't exist | Create vertex first |
/// | [`MissingEdgeEndpoint`](Self::MissingEdgeEndpoint) | `from` or `to` not specified | Add missing endpoint |
/// | [`EmptyTraversalEndpoint`](Self::EmptyTraversalEndpoint) | Endpoint traversal returned nothing | Fix traversal |
/// | [`AmbiguousTraversalEndpoint`](Self::AmbiguousTraversalEndpoint) | Endpoint traversal returned multiple | Add filters |
/// | [`StepLabelNotFound`](Self::StepLabelNotFound) | Referenced label doesn't exist | Add `as_()` step |
/// | [`StepLabelNotVertex`](Self::StepLabelNotVertex) | Label references non-vertex | Use vertex step |
/// | [`Storage`](Self::Storage) | Underlying storage failed | See [`StorageError`] |
///
/// # Example
///
/// ```rust
/// use interstellar::prelude::*;
/// use interstellar::error::MutationError;
/// use interstellar::storage::{Graph, GraphStorageMut};
/// use std::collections::HashMap;
///
/// let graph = Graph::new();
/// let mut storage = graph.as_storage_mut();
/// let alice = storage.add_vertex("person", HashMap::new());
///
/// // Try to create edge to non-existent vertex
/// let result = storage.add_edge(alice, VertexId(999), "knows", HashMap::new());
///
/// // The error tells us which vertex is missing
/// match result {
///     Err(StorageError::VertexNotFound(id)) => {
///         println!("Target vertex {:?} doesn't exist", id);
///     }
///     _ => {}
/// }
/// ```
#[derive(Debug, thiserror::Error)]
pub enum MutationError {
    /// The source vertex for edge creation was not found.
    ///
    /// This occurs when attempting to create an edge from a vertex
    /// that doesn't exist in the graph.
    ///
    /// # Recovery
    ///
    /// Create the source vertex before creating the edge:
    ///
    /// ```rust
    /// use interstellar::storage::{Graph, GraphStorageMut};
    /// use interstellar::prelude::*;
    /// use std::collections::HashMap;
    ///
    /// let graph = Graph::new();
    /// let mut storage = graph.as_storage_mut();
    ///
    /// // Create both vertices first
    /// let alice = storage.add_vertex("person", HashMap::new());
    /// let bob = storage.add_vertex("person", HashMap::new());
    ///
    /// // Now the edge can be created
    /// storage.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    /// ```
    #[error("edge source vertex not found: {0:?}")]
    EdgeSourceNotFound(VertexId),

    /// The target vertex for edge creation was not found.
    ///
    /// This occurs when attempting to create an edge to a vertex
    /// that doesn't exist in the graph.
    ///
    /// # Recovery
    ///
    /// Same as [`EdgeSourceNotFound`](Self::EdgeSourceNotFound)—create the
    /// target vertex before creating the edge.
    #[error("edge target vertex not found: {0:?}")]
    EdgeTargetNotFound(VertexId),

    /// A required edge endpoint (from or to) was not specified.
    ///
    /// When creating an edge with `addE()`, both endpoints must be specified
    /// using `from_vertex()`, `from_label()`, `to_vertex()`, `to_label()`, etc.
    ///
    /// The contained string indicates which endpoint is missing: `"from"` or `"to"`.
    ///
    /// # Recovery
    ///
    /// ```ignore
    /// // Wrong: missing 'to' endpoint
    /// g.add_e("knows").from_vertex(alice);
    ///
    /// // Correct: both endpoints specified
    /// g.add_e("knows").from_vertex(alice).to_vertex(bob);
    /// ```
    #[error("missing edge endpoint: {0}")]
    MissingEdgeEndpoint(&'static str),

    /// A traversal used as an edge endpoint yielded no vertices.
    ///
    /// When using a traversal to specify an edge endpoint (via `from_traversal`
    /// or `to_traversal`), the traversal must yield exactly one vertex.
    ///
    /// # Recovery
    ///
    /// Ensure your endpoint traversal matches exactly one vertex:
    ///
    /// ```ignore
    /// // Check that the traversal finds something
    /// let exists = g.v().has_value("name", "Bob").has_next();
    /// if !exists {
    ///     println!("Bob doesn't exist - creating...");
    /// }
    /// ```
    #[error("traversal yielded no vertices for edge endpoint")]
    EmptyTraversalEndpoint,

    /// A traversal used as an edge endpoint yielded multiple vertices.
    ///
    /// When using a traversal to specify an edge endpoint, the traversal
    /// must yield exactly one vertex, not multiple.
    ///
    /// # Recovery
    ///
    /// Add more filters to make the traversal match exactly one vertex:
    ///
    /// ```ignore
    /// // Wrong: matches multiple people
    /// .to_traversal(__.v().has_label("person"))
    ///
    /// // Correct: matches exactly one person
    /// .to_traversal(__.v().has_label("person").has_value("name", "Bob"))
    /// ```
    #[error("traversal yielded multiple vertices for edge endpoint")]
    AmbiguousTraversalEndpoint,

    /// A step label referenced in edge creation was not found in the path.
    ///
    /// When using `from_label()` or `to_label()` to reference a previously
    /// labeled step via `as_()`, the label must exist in the traverser's path.
    ///
    /// # Recovery
    ///
    /// Ensure you've labeled the step you're referencing:
    ///
    /// ```ignore
    /// // Wrong: "source" label doesn't exist
    /// g.v().has_label("person").add_e("knows").from_label("source").to_vertex(bob);
    ///
    /// // Correct: label the step first
    /// g.v().has_label("person").as_("source").add_e("knows").from_label("source").to_vertex(bob);
    /// ```
    #[error("step label not found: {0}")]
    StepLabelNotFound(String),

    /// The labeled step value is not a vertex.
    ///
    /// When using `from_label()` or `to_label()`, the labeled value must
    /// be a vertex (not an edge, property value, etc.).
    ///
    /// # Recovery
    ///
    /// Ensure you're labeling a vertex, not a value:
    ///
    /// ```ignore
    /// // Wrong: labels a property value, not a vertex
    /// g.v().values("name").as_("x").add_e("knows").from_label("x")
    ///
    /// // Correct: labels the vertex
    /// g.v().as_("x").add_e("knows").from_label("x")
    /// ```
    #[error("step label '{0}' does not reference a vertex")]
    StepLabelNotVertex(String),

    /// A storage operation failed during mutation.
    ///
    /// This wraps a [`StorageError`] that occurred during the mutation.
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_error_display_variants() {
        let v_err = StorageError::VertexNotFound(VertexId(1));
        let e_err = StorageError::EdgeNotFound(EdgeId(2));
        let wal_err = StorageError::WalCorrupted("oops".to_string());
        let fmt_err = StorageError::InvalidFormat;

        assert!(format!("{}", v_err).contains("vertex not found"));
        assert!(format!("{}", e_err).contains("edge not found"));
        assert!(format!("{}", wal_err).contains("WAL corrupted"));
        assert!(format!("{}", fmt_err).contains("invalid file format"));
    }

    #[test]
    fn traversal_error_wraps_storage() {
        let inner = StorageError::EdgeNotFound(EdgeId(3));
        let err = TraversalError::from(inner);
        assert!(format!("{}", err).contains("storage error"));
    }

    #[test]
    fn mutation_error_display_variants() {
        let source_err = MutationError::EdgeSourceNotFound(VertexId(1));
        let target_err = MutationError::EdgeTargetNotFound(VertexId(2));
        let missing_err = MutationError::MissingEdgeEndpoint("to");
        let empty_err = MutationError::EmptyTraversalEndpoint;
        let ambig_err = MutationError::AmbiguousTraversalEndpoint;
        let label_err = MutationError::StepLabelNotFound("x".to_string());
        let not_vertex_err = MutationError::StepLabelNotVertex("y".to_string());

        assert!(format!("{}", source_err).contains("source vertex not found"));
        assert!(format!("{}", target_err).contains("target vertex not found"));
        assert!(format!("{}", missing_err).contains("missing edge endpoint"));
        assert!(format!("{}", empty_err).contains("no vertices"));
        assert!(format!("{}", ambig_err).contains("multiple vertices"));
        assert!(format!("{}", label_err).contains("not found"));
        assert!(format!("{}", not_vertex_err).contains("not reference a vertex"));
    }

    #[test]
    fn error_conversion_chain() {
        // StorageError -> MutationError
        let storage = StorageError::VertexNotFound(VertexId(1));
        let mutation: MutationError = storage.into();
        assert!(matches!(mutation, MutationError::Storage(_)));

        // MutationError -> TraversalError
        let traversal: TraversalError = mutation.into();
        assert!(matches!(traversal, TraversalError::Mutation(_)));

        // Direct StorageError -> TraversalError
        let storage2 = StorageError::EdgeNotFound(EdgeId(1));
        let traversal2: TraversalError = storage2.into();
        assert!(matches!(traversal2, TraversalError::Storage(_)));
    }

    #[test]
    fn not_one_error_display() {
        let zero = TraversalError::NotOne(0);
        let many = TraversalError::NotOne(5);

        assert!(format!("{}", zero).contains("found 0"));
        assert!(format!("{}", many).contains("found 5"));
    }
}
