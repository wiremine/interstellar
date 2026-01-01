//! RustGremlin: A Fluent Graph Traversal Library

pub mod algorithms;
pub mod error;
pub mod graph;
pub mod storage;
pub mod traversal;
pub mod value;

pub mod prelude {
    pub use crate::error::{StorageError, TraversalError};
    pub use crate::graph::{Graph, GraphMut, GraphSnapshot};
    pub use crate::traversal::{p, GraphTraversalSource, Path, Traversal, Traverser, __};
    pub use crate::value::{EdgeId, ElementId, Value, VertexId};
}

pub use prelude::*;
