//! Shared types for graph algorithms.
//!
//! This module provides the common types used across all algorithm implementations:
//! - [`AlgorithmError`]: Error type for algorithm failures
//! - [`PathResult`]: A discovered path through the graph
//! - [`Direction`]: Direction filter for neighbor expansion
//! - [`WeightFn`]: Edge weight extraction function
//! - [`Visitor`]: Callback trait for traversal algorithms

use crate::error::StorageError;
use crate::value::{EdgeId, Value, VertexId};
use std::collections::HashMap;
use thiserror::Error;

/// Errors that can occur during algorithm execution.
#[derive(Debug, Error)]
pub enum AlgorithmError {
    /// The specified vertex does not exist in the graph.
    #[error("vertex not found: {0:?}")]
    VertexNotFound(VertexId),

    /// A negative weight cycle was detected (Dijkstra cannot handle this).
    #[error("negative weight cycle detected")]
    NegativeWeightCycle,

    /// No path exists between the source and target vertices.
    #[error("no path exists between {from:?} and {to:?}")]
    NoPath { from: VertexId, to: VertexId },

    /// An underlying storage operation failed.
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    /// An edge's weight property is missing or not numeric.
    #[error("weight property '{0}' not found or not numeric")]
    InvalidWeight(String),

    /// The iterative deepening depth limit was exceeded.
    #[error("depth limit exceeded: {0}")]
    DepthLimitExceeded(u32),
}

/// A discovered path through the graph.
#[derive(Debug, Clone, PartialEq)]
pub struct PathResult {
    /// Ordered vertex IDs from source to target.
    pub vertices: Vec<VertexId>,
    /// Edge IDs traversed (len = vertices.len() - 1).
    pub edges: Vec<EdgeId>,
    /// Total weight (0.0 for unweighted, sum of edge weights for weighted).
    pub weight: f64,
}

/// Direction filter for neighbor expansion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Follow outgoing edges only.
    Out,
    /// Follow incoming edges only.
    In,
    /// Follow both outgoing and incoming edges.
    Both,
}

/// Function that extracts a numeric weight from an edge's properties.
/// Returns `None` to skip the edge (acts as a filter).
pub type WeightFn = Box<dyn Fn(EdgeId, &HashMap<String, Value>) -> Option<f64> + Send + Sync>;

/// Constant weight of 1.0 for every edge (unweighted graphs).
pub fn unit_weight() -> WeightFn {
    Box::new(|_, _| Some(1.0))
}

/// Extract weight from a named property. Non-numeric or missing → `None`.
pub fn property_weight(key: String) -> WeightFn {
    Box::new(move |_, props| {
        props.get(&key).and_then(|v| match v {
            Value::Int(n) => Some(*n as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        })
    })
}

/// Optional visitor callback for traversal algorithms.
pub trait Visitor {
    /// Called when a vertex is first discovered. Return `false` to prune.
    fn discover(&mut self, _vertex: VertexId, _depth: u32) -> bool {
        true
    }
    /// Called when a vertex is fully processed.
    fn finish(&mut self, _vertex: VertexId, _depth: u32) {}
}

/// No-op visitor that accepts all vertices.
pub struct NoopVisitor;
impl Visitor for NoopVisitor {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_algorithm_error_display() {
        let e = AlgorithmError::VertexNotFound(VertexId(42));
        assert!(e.to_string().contains("42"));

        let e = AlgorithmError::NegativeWeightCycle;
        assert!(e.to_string().contains("negative weight"));

        let e = AlgorithmError::NoPath {
            from: VertexId(1),
            to: VertexId(2),
        };
        assert!(e.to_string().contains("no path"));

        let e = AlgorithmError::InvalidWeight("weight".to_string());
        assert!(e.to_string().contains("weight"));

        let e = AlgorithmError::DepthLimitExceeded(10);
        assert!(e.to_string().contains("10"));
    }

    #[test]
    fn test_path_result_clone_eq() {
        let p = PathResult {
            vertices: vec![VertexId(1), VertexId(2)],
            edges: vec![EdgeId(10)],
            weight: 1.0,
        };
        assert_eq!(p, p.clone());
    }

    #[test]
    fn test_direction_copy() {
        let d = Direction::Out;
        let d2 = d;
        assert_eq!(d, d2);
        assert_eq!(Direction::In, Direction::In);
        assert_eq!(Direction::Both, Direction::Both);
        assert_ne!(Direction::Out, Direction::In);
    }

    #[test]
    fn test_unit_weight() {
        let w = unit_weight();
        assert_eq!(w(EdgeId(0), &HashMap::new()), Some(1.0));
    }

    #[test]
    fn test_property_weight() {
        let w = property_weight("cost".to_string());
        let mut props = HashMap::new();
        props.insert("cost".to_string(), Value::Int(5));
        assert_eq!(w(EdgeId(0), &props), Some(5.0));

        props.insert("cost".to_string(), Value::Float(3.14));
        assert_eq!(w(EdgeId(0), &props), Some(3.14));

        props.insert("cost".to_string(), Value::String("nope".to_string()));
        assert_eq!(w(EdgeId(0), &props), None);

        assert_eq!(w(EdgeId(0), &HashMap::new()), None);
    }

    #[test]
    fn test_noop_visitor() {
        let mut v = NoopVisitor;
        assert!(v.discover(VertexId(1), 0));
        v.finish(VertexId(1), 0); // no-op, just ensure no panic
    }

    #[test]
    fn test_storage_error_conversion() {
        let se = StorageError::VertexNotFound(VertexId(99));
        let ae: AlgorithmError = se.into();
        assert!(matches!(ae, AlgorithmError::Storage(_)));
    }
}
