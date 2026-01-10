//! Error types for storage and traversal operations.
//!
//! This module defines the error types used throughout RustGremlin:
//!
//! - [`StorageError`] - Errors from storage backend operations
//! - [`TraversalError`] - Errors during graph traversals
//!
//! Both error types implement [`std::error::Error`] and [`std::fmt::Display`]
//! via the [`thiserror`](https://docs.rs/thiserror) crate.
//!
//! # Error Handling Pattern
//!
//! Most operations in RustGremlin return `Result` types. Storage operations
//! return `Result<T, StorageError>`, while traversal terminal steps that can
//! fail return `Result<T, TraversalError>`.
//!
//! # Example
//!
//! ```rust
//! use rustgremlin::prelude::*;
//! use rustgremlin::storage::InMemoryGraph;
//! use std::collections::HashMap;
//! use std::sync::Arc;
//!
//! let mut storage = InMemoryGraph::new();
//! let alice = storage.add_vertex("person", HashMap::new());
//!
//! let graph = Graph::new(Arc::new(storage));
//! let snapshot = graph.snapshot();
//! let g = snapshot.traversal();
//!
//! // one() returns Result<Value, TraversalError>
//! match g.v_ids([alice]).one() {
//!     Ok(value) => println!("Found: {:?}", value),
//!     Err(TraversalError::NotOne(count)) => {
//!         println!("Expected 1 result, got {}", count);
//!     }
//!     Err(e) => println!("Error: {}", e),
//! }
//! ```

use crate::value::{EdgeId, VertexId};

/// Errors that can occur during storage operations.
///
/// `StorageError` represents failures when interacting with the graph storage
/// backend, including missing elements, I/O failures, and data corruption.
///
/// # Variants
///
/// - [`VertexNotFound`](StorageError::VertexNotFound) - A vertex ID doesn't exist
/// - [`EdgeNotFound`](StorageError::EdgeNotFound) - An edge ID doesn't exist
/// - [`Io`](StorageError::Io) - Underlying I/O operation failed
/// - [`WalCorrupted`](StorageError::WalCorrupted) - Write-ahead log is corrupted
/// - [`InvalidFormat`](StorageError::InvalidFormat) - Data format is invalid
///
/// # Example
///
/// ```rust
/// use rustgremlin::prelude::*;
/// use rustgremlin::storage::InMemoryGraph;
/// use std::collections::HashMap;
///
/// let mut storage = InMemoryGraph::new();
///
/// // Attempting to create an edge with non-existent vertices fails
/// let result = storage.add_edge(
///     VertexId(999),  // doesn't exist
///     VertexId(888),  // doesn't exist
///     "knows",
///     HashMap::new(),
/// );
///
/// match result {
///     Err(StorageError::VertexNotFound(id)) => {
///         println!("Vertex {:?} not found", id);
///     }
///     _ => {}
/// }
/// ```
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The requested vertex does not exist in the graph.
    #[error("vertex not found: {0:?}")]
    VertexNotFound(VertexId),

    /// The requested edge does not exist in the graph.
    #[error("edge not found: {0:?}")]
    EdgeNotFound(EdgeId),

    /// An I/O operation failed.
    ///
    /// This typically occurs with persistent storage backends when reading
    /// from or writing to disk fails.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The write-ahead log is corrupted.
    ///
    /// This indicates data integrity issues with the WAL file, which may
    /// require recovery or manual intervention.
    #[error("WAL corrupted: {0}")]
    WalCorrupted(String),

    /// The storage file format is invalid.
    ///
    /// This can occur when opening a file that isn't a valid RustGremlin
    /// database or when the file version is incompatible.
    #[error("invalid file format")]
    InvalidFormat,

    /// The database file contains corrupted data.
    ///
    /// This indicates data integrity issues, such as reading beyond valid
    /// offsets or encountering malformed records.
    #[error("corrupted data")]
    CorruptedData,

    /// The storage is out of space.
    ///
    /// This occurs when attempting to allocate space in the property arena
    /// or other fixed-size regions and there isn't enough room.
    #[error("out of space")]
    OutOfSpace,
}

/// Errors that can occur during graph traversals.
///
/// `TraversalError` represents failures during traversal execution, including
/// cardinality violations and underlying storage errors.
///
/// # Error Conversion
///
/// `TraversalError` implements `From<StorageError>`, allowing storage errors
/// to be automatically converted when using the `?` operator.
///
/// # Example
///
/// ```rust
/// use rustgremlin::prelude::*;
///
/// let graph = Graph::in_memory();
/// let snapshot = graph.snapshot();
/// let g = snapshot.traversal();
///
/// // one() expects exactly one result
/// let result = g.v().one();
///
/// match result {
///     Ok(vertex) => println!("Found vertex: {:?}", vertex),
///     Err(TraversalError::NotOne(0)) => println!("No vertices found"),
///     Err(TraversalError::NotOne(n)) => println!("Too many vertices: {}", n),
///     Err(e) => println!("Other error: {}", e),
/// }
/// ```
#[derive(Debug, thiserror::Error)]
pub enum TraversalError {
    /// Expected exactly one result but found a different count.
    ///
    /// This error is returned by terminal steps like `one()` when the
    /// traversal doesn't yield exactly one element.
    ///
    /// The contained `usize` indicates how many elements were actually found:
    /// - `0` means no elements matched
    /// - `> 1` means multiple elements matched
    #[error("expected exactly one result, found {0}")]
    NotOne(usize),

    /// A storage operation failed during traversal.
    ///
    /// This wraps a [`StorageError`] that occurred while executing the traversal.
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    /// A mutation operation failed during traversal.
    ///
    /// This wraps a [`MutationError`] that occurred while executing a mutation step.
    #[error("mutation error: {0}")]
    Mutation(#[from] MutationError),
}

/// Errors that can occur during mutation operations.
///
/// `MutationError` represents failures during graph mutation steps such as
/// `addV()`, `addE()`, `property()`, and `drop()`.
///
/// # Example
///
/// ```rust
/// use rustgremlin::prelude::*;
/// use rustgremlin::storage::InMemoryGraph;
/// use std::collections::HashMap;
///
/// let mut storage = InMemoryGraph::new();
/// let alice = storage.add_vertex("person", HashMap::new());
///
/// // Creating an edge with a non-existent target would fail
/// // with MutationError::EdgeTargetNotFound
/// ```
#[derive(Debug, thiserror::Error)]
pub enum MutationError {
    /// The source vertex for edge creation was not found.
    ///
    /// This occurs when attempting to create an edge from a vertex
    /// that doesn't exist in the graph.
    #[error("edge source vertex not found: {0:?}")]
    EdgeSourceNotFound(VertexId),

    /// The target vertex for edge creation was not found.
    ///
    /// This occurs when attempting to create an edge to a vertex
    /// that doesn't exist in the graph.
    #[error("edge target vertex not found: {0:?}")]
    EdgeTargetNotFound(VertexId),

    /// A required edge endpoint (from or to) was not specified.
    ///
    /// The contained string indicates which endpoint is missing:
    /// "from" or "to".
    #[error("missing edge endpoint: {0}")]
    MissingEdgeEndpoint(&'static str),

    /// A traversal used as an edge endpoint yielded no vertices.
    ///
    /// When using a traversal to specify an edge endpoint (via `from_traversal`
    /// or `to_traversal`), the traversal must yield exactly one vertex.
    #[error("traversal yielded no vertices for edge endpoint")]
    EmptyTraversalEndpoint,

    /// A traversal used as an edge endpoint yielded multiple vertices.
    ///
    /// When using a traversal to specify an edge endpoint (via `from_traversal`
    /// or `to_traversal`), the traversal must yield exactly one vertex.
    #[error("traversal yielded multiple vertices for edge endpoint")]
    AmbiguousTraversalEndpoint,

    /// A step label referenced in edge creation was not found in the path.
    ///
    /// When using `from_label` or `to_label` to reference a previously labeled
    /// step via `as_()`, the label must exist in the traverser's path.
    #[error("step label not found: {0}")]
    StepLabelNotFound(String),

    /// The labeled step value is not a vertex.
    ///
    /// When using `from_label` or `to_label`, the labeled value must be a vertex.
    #[error("step label '{0}' does not reference a vertex")]
    StepLabelNotVertex(String),

    /// A storage operation failed during mutation.
    ///
    /// This wraps a [`StorageError`] that occurred during the mutation.
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

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
}
