//! RustGremlin: A Fluent Graph Traversal Library

pub mod algorithms;
pub mod error;
pub mod gql;
pub mod graph;
pub mod storage;
pub mod traversal;
pub mod value;

pub mod prelude {
    pub use crate::error::{StorageError, TraversalError};
    pub use crate::graph::{Graph, GraphMut, GraphSnapshot};
    pub use crate::traversal::{
        p, BoundTraversal, CloneSack, ExecutionContext, GraphTraversalSource, GroupKey, GroupValue,
        Path, PathElement, PathValue, Traversal, Traverser, __,
    };
    pub use crate::value::{EdgeId, ElementId, Value, VertexId};
}

pub use prelude::*;
