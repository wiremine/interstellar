//! Output type markers for compile-time traversal type tracking.
//!
//! This module provides marker types that track what a traversal produces
//! at compile time, enabling type-safe terminal methods.
//!
//! # Overview
//!
//! When a traversal is executed, its output type determines what the
//! terminal methods return:
//!
//! - `Vertex` marker → `next()` returns `GraphVertex`
//! - `Edge` marker → `next()` returns `GraphEdge`
//! - `Scalar` marker → `next()` returns `Value`
//!
//! # Marker Types
//!
//! | Marker | Description | Terminal Returns |
//! |--------|-------------|------------------|
//! | [`Vertex`] | Traversal produces vertices | `GraphVertex` |
//! | [`Edge`] | Traversal produces edges | `GraphEdge` |
//! | [`Scalar`] | Traversal produces arbitrary values | `Value` |
//!
//! # Type Transformations
//!
//! Navigation and transform steps change the marker type:
//!
//! ```text
//! g.v()          → Vertex marker
//! g.v().out_e()  → Edge marker (Vertex → Edge)
//! g.v().values() → Scalar marker (Vertex → Scalar)
//! g.e().out_v()  → Vertex marker (Edge → Vertex)
//! ```
//!
//! # Example
//!
//! ```ignore
//! use interstellar::traversal::markers::{Vertex, Edge, Scalar, OutputMarker};
//!
//! // Type-level assertions
//! fn vertex_traversal<T: OutputMarker<Output = GraphVertex>>(_t: T) {}
//! fn edge_traversal<T: OutputMarker<Output = GraphEdge>>(_t: T) {}
//! fn scalar_traversal<T: OutputMarker<Output = Value>>(_t: T) {}
//!
//! vertex_traversal(Vertex);
//! edge_traversal(Edge);
//! scalar_traversal(Scalar);
//! ```

use crate::graph_elements::{GraphEdge, GraphVertex};
use crate::value::Value;

// =============================================================================
// Marker Types
// =============================================================================

/// Marker indicating traversal produces vertices.
///
/// When a traversal has this marker, terminal methods like `next()` and
/// `to_list()` return `GraphVertex` objects instead of raw `Value`s.
///
/// # Example
///
/// ```ignore
/// // g.v() produces Vertex marker
/// let v: GraphVertex = g.v().next().unwrap();
///
/// // g.v().out() also produces Vertex marker
/// let friends: Vec<GraphVertex> = g.v().out().to_list();
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Vertex;

/// Marker indicating traversal produces edges.
///
/// When a traversal has this marker, terminal methods like `next()` and
/// `to_list()` return `GraphEdge` objects instead of raw `Value`s.
///
/// # Example
///
/// ```ignore
/// // g.e() produces Edge marker
/// let e: GraphEdge = g.e().next().unwrap();
///
/// // g.v().out_e() also produces Edge marker
/// let edges: Vec<GraphEdge> = g.v().out_e().to_list();
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Edge;

/// Marker indicating traversal produces arbitrary values.
///
/// When a traversal has this marker, terminal methods return raw `Value`s.
/// This is the default for operations that extract properties or perform
/// aggregations.
///
/// # Example
///
/// ```ignore
/// // g.v().values("name") produces Scalar marker
/// let names: Vec<Value> = g.v().values("name").to_list();
///
/// // g.v().count() produces Scalar marker
/// let count: Value = g.v().count().next().unwrap();
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Scalar;

// =============================================================================
// OutputMarker Trait
// =============================================================================

/// Trait for traversal output markers.
///
/// This trait defines the associated types for terminal method returns
/// based on what the traversal produces.
///
/// # Sealed Trait
///
/// This is a sealed trait - only `Vertex`, `Edge`, and `Scalar` implement it.
/// Users cannot implement this trait for custom types.
///
/// # Associated Types
///
/// - `Output` - The type returned by `next()` and `one()`
/// - `OutputList` - The type returned by `to_list()`
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::markers::{Vertex, Edge, Scalar, OutputMarker};
///
/// // Vertex marker's Output is GraphVertex
/// type VertexOut = <Vertex as OutputMarker>::Output;  // GraphVertex
///
/// // Edge marker's Output is GraphEdge
/// type EdgeOut = <Edge as OutputMarker>::Output;      // GraphEdge
///
/// // Scalar marker's Output is Value
/// type ScalarOut = <Scalar as OutputMarker>::Output;  // Value
/// ```
pub trait OutputMarker: Clone + Send + Sync + 'static + private::Sealed {
    /// The terminal return type for `next()` and `one()`.
    type Output: Clone + Send + Sync;

    /// The terminal return type for `to_list()`.
    type OutputList: Clone + Send + Sync;
}

impl OutputMarker for Vertex {
    type Output = GraphVertex;
    type OutputList = Vec<GraphVertex>;
}

impl OutputMarker for Edge {
    type Output = GraphEdge;
    type OutputList = Vec<GraphEdge>;
}

impl OutputMarker for Scalar {
    type Output = Value;
    type OutputList = Vec<Value>;
}

// =============================================================================
// Sealed Trait (Private)
// =============================================================================

mod private {
    use super::*;

    /// Sealed trait to prevent external implementations of OutputMarker.
    pub trait Sealed {}

    impl Sealed for Vertex {}
    impl Sealed for Edge {}
    impl Sealed for Scalar {}
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_types_are_copy() {
        let v = Vertex;
        let v2 = v;
        assert_eq!(v, v2);

        let e = Edge;
        let e2 = e;
        assert_eq!(e, e2);

        let s = Scalar;
        let s2 = s;
        assert_eq!(s, s2);
    }

    #[test]
    fn marker_types_are_default() {
        let v = Vertex::default();
        assert_eq!(v, Vertex);

        let e = Edge::default();
        assert_eq!(e, Edge);

        let s = Scalar::default();
        assert_eq!(s, Scalar);
    }

    #[test]
    fn marker_types_are_debug() {
        assert_eq!(format!("{:?}", Vertex), "Vertex");
        assert_eq!(format!("{:?}", Edge), "Edge");
        assert_eq!(format!("{:?}", Scalar), "Scalar");
    }

    #[test]
    fn output_marker_associated_types() {
        // Compile-time check that associated types are correct
        fn assert_vertex_output<M: OutputMarker<Output = GraphVertex>>() {}
        fn assert_edge_output<M: OutputMarker<Output = GraphEdge>>() {}
        fn assert_scalar_output<M: OutputMarker<Output = Value>>() {}

        assert_vertex_output::<Vertex>();
        assert_edge_output::<Edge>();
        assert_scalar_output::<Scalar>();
    }

    #[test]
    fn markers_are_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<Vertex>();
        assert_send_sync::<Edge>();
        assert_send_sync::<Scalar>();
    }
}
