//! Typed traversals with compile-time output tracking.
//!
//! This module provides [`TypedTraversal`] and [`TypedTraversalSource`],
//! which track the output type at compile time using marker types.
//!
//! # Overview
//!
//! Traditional traversals return `Value` from terminal methods, requiring
//! runtime type checking. Typed traversals use marker types to determine
//! the return type at compile time:
//!
//! - `TypedTraversal<'g, Vertex>` → `next()` returns `Option<GraphVertex>`
//! - `TypedTraversal<'g, Edge>` → `next()` returns `Option<GraphEdge>`
//! - `TypedTraversal<'g, Scalar>` → `next()` returns `Option<Value>`
//!
//! # Example
//!
//! ```rust
//! use interstellar::prelude::*;
//! use interstellar::traversal::typed::TypedTraversalSource;
//! use std::sync::Arc;
//! use std::collections::HashMap;
//!
//! let graph = Arc::new(Graph::new());
//! let _ = graph.add_vertex("person", HashMap::from([
//!     ("name".to_string(), "Alice".into()),
//! ]));
//!
//! let snapshot = graph.snapshot();
//! let g = TypedTraversalSource::new(&snapshot, graph.clone());
//!
//! // g.v().next() returns Option<GraphVertex>
//! let v = g.v().next().unwrap();
//! assert_eq!(v.label(), Some("person".to_string()));
//!
//! // g.v().values("name").next() returns Option<Value>
//! let name = g.v().values("name").next();
//! assert!(matches!(name, Some(Value::String(_))));
//! ```
//!
//! # Type Transformations
//!
//! Navigation and transform steps change the marker type appropriately:
//!
//! | Step | Marker Transformation |
//! |------|----------------------|
//! | `out()`, `in_()`, `both()` | Vertex → Vertex |
//! | `out_e()`, `in_e()`, `both_e()` | Vertex → Edge |
//! | `out_v()`, `in_v()`, `both_v()` | Edge → Vertex |
//! | `values()`, `id()`, `label()` | Any → Scalar |
//! | `has_label()`, `has()`, `limit()` | Preserves current marker |

use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::error::TraversalError;
use crate::graph_elements::{GraphEdge, GraphVertex};
use crate::storage::cow::Graph;
use crate::storage::interner::StringInterner;
use crate::storage::GraphStorage;
use crate::traversal::context::SnapshotLike;
use crate::traversal::filter::{
    DedupStep, HasLabelStep, HasStep, HasValueStep, LimitStep, RangeStep, SkipStep,
};
use crate::traversal::markers::{Edge, OutputMarker, Scalar, Vertex};
use crate::traversal::navigation::{
    BothEStep, BothStep, InEStep, InStep, InVStep, OutEStep, OutStep, OutVStep,
};
use crate::traversal::step::{AnyStep, StartStep};
use crate::traversal::transform::{IdStep, LabelStep, ValuesStep};
use crate::traversal::{ExecutionContext, Traversal, TraversalSource, Traverser};
use crate::value::Value;

// =============================================================================
// TypedTraversalSource
// =============================================================================

/// Entry point for typed traversals with compile-time output tracking.
///
/// `TypedTraversalSource` produces [`TypedTraversal`]s that know their output
/// type at compile time, enabling type-safe terminal methods.
///
/// # Creating a Source
///
/// ```rust
/// use interstellar::prelude::*;
/// use interstellar::traversal::typed::TypedTraversalSource;
/// use std::sync::Arc;
///
/// let graph = Arc::new(Graph::new());
/// let snapshot = graph.snapshot();
/// let g = TypedTraversalSource::new(&snapshot, graph.clone());
/// ```
///
/// # Source Methods
///
/// | Method | Returns | Terminal Output |
/// |--------|---------|-----------------|
/// | `v()` | `TypedTraversal<Vertex>` | `GraphVertex` |
/// | `e()` | `TypedTraversal<Edge>` | `GraphEdge` |
/// | `inject()` | `TypedTraversal<Scalar>` | `Value` |
pub struct TypedTraversalSource<'g> {
    storage: &'g dyn GraphStorage,
    interner: &'g StringInterner,
    graph: Arc<Graph>,
}

impl<'g> TypedTraversalSource<'g> {
    /// Create a new typed traversal source.
    ///
    /// # Arguments
    ///
    /// * `snapshot` - A graph snapshot providing storage and interner
    /// * `graph` - An `Arc<Graph>` for creating rich element types
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::traversal::typed::TypedTraversalSource;
    /// use std::sync::Arc;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let snapshot = graph.snapshot();
    /// let g = TypedTraversalSource::new(&snapshot, graph.clone());
    /// ```
    pub fn new<S: SnapshotLike + ?Sized>(snapshot: &'g S, graph: Arc<Graph>) -> Self {
        Self {
            storage: snapshot.storage(),
            interner: snapshot.interner(),
            graph,
        }
    }

    /// Start traversal from all vertices.
    ///
    /// Returns a `TypedTraversal<Vertex>` where terminal methods return
    /// `GraphVertex` objects.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::traversal::typed::TypedTraversalSource;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let _ = graph.add_vertex("person", HashMap::new());
    ///
    /// let snapshot = graph.snapshot();
    /// let g = TypedTraversalSource::new(&snapshot, graph.clone());
    ///
    /// // next() returns Option<GraphVertex>
    /// let v = g.v().next();
    /// assert!(v.is_some());
    /// ```
    pub fn v(&self) -> TypedTraversal<'g, Vertex> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: Arc::clone(&self.graph),
            traversal: Traversal::with_source(TraversalSource::AllVertices),
            track_paths: false,
            _marker: PhantomData,
        }
    }

    /// Start traversal from all edges.
    ///
    /// Returns a `TypedTraversal<Edge>` where terminal methods return
    /// `GraphEdge` objects.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::traversal::typed::TypedTraversalSource;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let a = graph.add_vertex("person", HashMap::new());
    /// let b = graph.add_vertex("person", HashMap::new());
    /// graph.add_edge(a, b, "knows", HashMap::new()).unwrap();
    ///
    /// let snapshot = graph.snapshot();
    /// let g = TypedTraversalSource::new(&snapshot, graph.clone());
    ///
    /// // next() returns Option<GraphEdge>
    /// let e = g.e().next();
    /// assert!(e.is_some());
    /// ```
    pub fn e(&self) -> TypedTraversal<'g, Edge> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: Arc::clone(&self.graph),
            traversal: Traversal::with_source(TraversalSource::AllEdges),
            track_paths: false,
            _marker: PhantomData,
        }
    }

    /// Inject arbitrary values into the traversal.
    ///
    /// Returns a `TypedTraversal<Scalar>` where terminal methods return
    /// raw `Value` objects.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::traversal::typed::TypedTraversalSource;
    /// use std::sync::Arc;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let snapshot = graph.snapshot();
    /// let g = TypedTraversalSource::new(&snapshot, graph.clone());
    ///
    /// let values = g.inject([1i64, 2, 3]).to_list();
    /// assert_eq!(values.len(), 3);
    /// ```
    pub fn inject<T, I>(&self, values: I) -> TypedTraversal<'g, Scalar>
    where
        I: IntoIterator<Item = T>,
        T: Into<Value>,
    {
        let values: Vec<Value> = values.into_iter().map(Into::into).collect();
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: Arc::clone(&self.graph),
            traversal: Traversal::with_source(TraversalSource::Inject(values)),
            track_paths: false,
            _marker: PhantomData,
        }
    }

    /// Get the graph reference.
    #[inline]
    pub fn graph(&self) -> &Arc<Graph> {
        &self.graph
    }
}

// =============================================================================
// TypedTraversal
// =============================================================================

/// A typed traversal bound to a graph with compile-time output tracking.
///
/// The `Marker` type parameter tracks what the traversal produces:
/// - `Vertex` → terminal methods return `GraphVertex`
/// - `Edge` → terminal methods return `GraphEdge`
/// - `Scalar` → terminal methods return `Value`
///
/// # Type Safety
///
/// The marker type is automatically transformed by navigation and transform steps:
///
/// ```rust
/// use interstellar::prelude::*;
/// use interstellar::traversal::typed::TypedTraversalSource;
/// use std::sync::Arc;
/// use std::collections::HashMap;
///
/// let graph = Arc::new(Graph::new());
/// let a = graph.add_vertex("person", HashMap::from([
///     ("name".to_string(), "Alice".into()),
/// ]));
/// let b = graph.add_vertex("person", HashMap::new());
/// graph.add_edge(a, b, "knows", HashMap::new()).unwrap();
///
/// let snapshot = graph.snapshot();
/// let g = TypedTraversalSource::new(&snapshot, graph.clone());
///
/// // Start with Vertex marker
/// let v = g.v().next().unwrap();  // Returns GraphVertex
///
/// // out_e() transforms to Edge marker
/// let e = g.v().out_e().next().unwrap();  // Returns GraphEdge
///
/// // values() transforms to Scalar marker
/// let name = g.v().values("name").next();  // Returns Option<Value>
/// ```
pub struct TypedTraversal<'g, Marker: OutputMarker> {
    storage: &'g dyn GraphStorage,
    interner: &'g StringInterner,
    graph: Arc<Graph>,
    traversal: Traversal<(), Value>,
    track_paths: bool,
    _marker: PhantomData<Marker>,
}

impl<'g, Marker: OutputMarker> TypedTraversal<'g, Marker> {
    /// Enable automatic path tracking for this traversal.
    ///
    /// When path tracking is enabled, navigation steps automatically
    /// record visited elements to each traverser's path.
    pub fn with_path(mut self) -> Self {
        self.track_paths = true;
        self
    }

    /// Check if path tracking is enabled.
    #[inline]
    pub fn is_tracking_paths(&self) -> bool {
        self.track_paths
    }

    /// Add a step to the traversal (internal use).
    fn add_step(self, step: impl AnyStep + 'static) -> Self {
        Self {
            traversal: self.traversal.add_step(step),
            ..self
        }
    }

    /// Execute the traversal and return an iterator over traversers.
    fn execute(self) -> TypedTraversalExecutor {
        let ctx = if self.track_paths {
            ExecutionContext::with_path_tracking(self.storage, self.interner)
        } else {
            ExecutionContext::new(self.storage, self.interner)
        };
        let (source, steps) = self.traversal.into_steps();

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
            current = step.apply(&ctx, Box::new(current.into_iter())).collect();
        }

        TypedTraversalExecutor {
            results: current.into_iter(),
        }
    }

    /// Get the graph reference.
    #[inline]
    pub fn graph(&self) -> &Arc<Graph> {
        &self.graph
    }

    // =========================================================================
    // Escape Hatches (work on any marker type)
    // =========================================================================

    /// Execute and return the first raw value, regardless of marker type.
    ///
    /// This is an escape hatch for when you need the raw `Value` instead
    /// of the typed output.
    pub fn next_value(self) -> Option<Value> {
        self.execute().next().map(|t| t.value)
    }

    /// Execute and return all raw values, regardless of marker type.
    ///
    /// This is an escape hatch for when you need raw `Value`s instead
    /// of typed output.
    pub fn to_value_list(self) -> Vec<Value> {
        self.execute().map(|t| t.value).collect()
    }
}

// =============================================================================
// TypedTraversalExecutor (internal)
// =============================================================================

/// Internal executor for typed traversals.
struct TypedTraversalExecutor {
    results: std::vec::IntoIter<Traverser>,
}

impl Iterator for TypedTraversalExecutor {
    type Item = Traverser;

    fn next(&mut self) -> Option<Self::Item> {
        self.results.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.results.size_hint()
    }
}

impl ExactSizeIterator for TypedTraversalExecutor {
    fn len(&self) -> usize {
        self.results.len()
    }
}

// =============================================================================
// Terminal Methods for Vertex Marker
// =============================================================================

impl<'g> TypedTraversal<'g, Vertex> {
    /// Execute and return the first vertex.
    ///
    /// Returns `None` if the traversal produces no vertices.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::traversal::typed::TypedTraversalSource;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let _ = graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Alice".into()),
    /// ]));
    ///
    /// let snapshot = graph.snapshot();
    /// let g = TypedTraversalSource::new(&snapshot, graph.clone());
    ///
    /// let v = g.v().next().unwrap();
    /// assert_eq!(v.property("name"), Some(Value::String("Alice".to_string())));
    /// ```
    pub fn next(self) -> Option<GraphVertex> {
        let graph = Arc::clone(&self.graph);
        self.execute().find_map(|t| match t.value {
            Value::Vertex(id) => Some(GraphVertex::new(id, Arc::clone(&graph))),
            _ => None,
        })
    }

    /// Execute and return all vertices.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::traversal::typed::TypedTraversalSource;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let _ = graph.add_vertex("person", HashMap::new());
    /// let _ = graph.add_vertex("person", HashMap::new());
    ///
    /// let snapshot = graph.snapshot();
    /// let g = TypedTraversalSource::new(&snapshot, graph.clone());
    ///
    /// let vertices = g.v().to_list();
    /// assert_eq!(vertices.len(), 2);
    /// ```
    pub fn to_list(self) -> Vec<GraphVertex> {
        let graph = Arc::clone(&self.graph);
        self.execute()
            .filter_map(|t| match t.value {
                Value::Vertex(id) => Some(GraphVertex::new(id, Arc::clone(&graph))),
                _ => None,
            })
            .collect()
    }

    /// Execute and return exactly one vertex, or error.
    ///
    /// Returns an error if there are zero or more than one vertices.
    pub fn one(self) -> Result<GraphVertex, TraversalError> {
        let graph = Arc::clone(&self.graph);
        let ids: Vec<_> = self
            .execute()
            .filter_map(|t| match t.value {
                Value::Vertex(id) => Some(id),
                _ => None,
            })
            .take(2)
            .collect();
        match ids.len() {
            1 => Ok(GraphVertex::new(ids[0], graph)),
            n => Err(TraversalError::NotOne(n)),
        }
    }

    /// Execute and collect unique vertices into a set.
    ///
    /// Note: `GraphVertex` contains interior mutability (via `Arc<Graph>`),
    /// but hashing is based only on the vertex ID, which is immutable.
    #[allow(clippy::mutable_key_type)]
    pub fn to_set(self) -> HashSet<GraphVertex> {
        self.to_list().into_iter().collect()
    }

    /// Check if the traversal produces any vertices.
    pub fn has_next(self) -> bool {
        self.execute().any(|t| matches!(t.value, Value::Vertex(_)))
    }

    /// Execute and count the number of vertices.
    pub fn count(self) -> u64 {
        self.execute()
            .filter(|t| matches!(t.value, Value::Vertex(_)))
            .count() as u64
    }

    /// Execute and return the first n vertices.
    pub fn take(self, n: usize) -> Vec<GraphVertex> {
        let graph = Arc::clone(&self.graph);
        self.execute()
            .filter_map(|t| match t.value {
                Value::Vertex(id) => Some(GraphVertex::new(id, Arc::clone(&graph))),
                _ => None,
            })
            .take(n)
            .collect()
    }

    /// Execute and return an iterator over vertices.
    pub fn iter(self) -> impl Iterator<Item = GraphVertex> {
        let graph = Arc::clone(&self.graph);
        self.execute().filter_map(move |t| match t.value {
            Value::Vertex(id) => Some(GraphVertex::new(id, Arc::clone(&graph))),
            _ => None,
        })
    }
}

// =============================================================================
// Terminal Methods for Edge Marker
// =============================================================================

impl<'g> TypedTraversal<'g, Edge> {
    /// Execute and return the first edge.
    ///
    /// Returns `None` if the traversal produces no edges.
    pub fn next(self) -> Option<GraphEdge> {
        let graph = Arc::clone(&self.graph);
        self.execute().find_map(|t| match t.value {
            Value::Edge(id) => Some(GraphEdge::new(id, Arc::clone(&graph))),
            _ => None,
        })
    }

    /// Execute and return all edges.
    pub fn to_list(self) -> Vec<GraphEdge> {
        let graph = Arc::clone(&self.graph);
        self.execute()
            .filter_map(|t| match t.value {
                Value::Edge(id) => Some(GraphEdge::new(id, Arc::clone(&graph))),
                _ => None,
            })
            .collect()
    }

    /// Execute and return exactly one edge, or error.
    ///
    /// Returns an error if there are zero or more than one edges.
    pub fn one(self) -> Result<GraphEdge, TraversalError> {
        let graph = Arc::clone(&self.graph);
        let ids: Vec<_> = self
            .execute()
            .filter_map(|t| match t.value {
                Value::Edge(id) => Some(id),
                _ => None,
            })
            .take(2)
            .collect();
        match ids.len() {
            1 => Ok(GraphEdge::new(ids[0], graph)),
            n => Err(TraversalError::NotOne(n)),
        }
    }

    /// Execute and collect unique edges into a set.
    ///
    /// Note: `GraphEdge` contains interior mutability (via `Arc<Graph>`),
    /// but hashing is based only on the edge ID, which is immutable.
    #[allow(clippy::mutable_key_type)]
    pub fn to_set(self) -> HashSet<GraphEdge> {
        self.to_list().into_iter().collect()
    }

    /// Check if the traversal produces any edges.
    pub fn has_next(self) -> bool {
        self.execute().any(|t| matches!(t.value, Value::Edge(_)))
    }

    /// Execute and count the number of edges.
    pub fn count(self) -> u64 {
        self.execute()
            .filter(|t| matches!(t.value, Value::Edge(_)))
            .count() as u64
    }

    /// Execute and return the first n edges.
    pub fn take(self, n: usize) -> Vec<GraphEdge> {
        let graph = Arc::clone(&self.graph);
        self.execute()
            .filter_map(|t| match t.value {
                Value::Edge(id) => Some(GraphEdge::new(id, Arc::clone(&graph))),
                _ => None,
            })
            .take(n)
            .collect()
    }

    /// Execute and return an iterator over edges.
    pub fn iter(self) -> impl Iterator<Item = GraphEdge> {
        let graph = Arc::clone(&self.graph);
        self.execute().filter_map(move |t| match t.value {
            Value::Edge(id) => Some(GraphEdge::new(id, Arc::clone(&graph))),
            _ => None,
        })
    }
}

// =============================================================================
// Terminal Methods for Scalar Marker
// =============================================================================

impl<'g> TypedTraversal<'g, Scalar> {
    /// Execute and return the first value.
    ///
    /// Returns `None` if the traversal produces no values.
    pub fn next(self) -> Option<Value> {
        self.execute().next().map(|t| t.value)
    }

    /// Execute and return all values.
    pub fn to_list(self) -> Vec<Value> {
        self.execute().map(|t| t.value).collect()
    }

    /// Execute and return exactly one value, or error.
    ///
    /// Returns an error if there are zero or more than one values.
    pub fn one(self) -> Result<Value, TraversalError> {
        let results: Vec<_> = self.execute().take(2).collect();
        match results.len() {
            1 => Ok(results.into_iter().next().unwrap().value),
            n => Err(TraversalError::NotOne(n)),
        }
    }

    /// Execute and collect unique values into a set.
    pub fn to_set(self) -> HashSet<Value> {
        self.to_list().into_iter().collect()
    }

    /// Check if the traversal produces any values.
    pub fn has_next(self) -> bool {
        self.execute().next().is_some()
    }

    /// Execute and count the number of values.
    pub fn count(self) -> u64 {
        self.execute().count() as u64
    }

    /// Execute and return the first n values.
    pub fn take(self, n: usize) -> Vec<Value> {
        self.execute().take(n).map(|t| t.value).collect()
    }

    /// Execute and return an iterator over values.
    pub fn iter(self) -> impl Iterator<Item = Value> {
        self.execute().map(|t| t.value)
    }

    /// Sum all numeric values.
    ///
    /// Returns `Value::Int(0)` for empty traversals.
    /// Non-numeric values are skipped.
    pub fn sum(self) -> Value {
        let mut int_sum: i64 = 0;
        let mut float_sum: f64 = 0.0;
        let mut has_float = false;

        for traverser in self.execute() {
            match traverser.value {
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

    /// Find the minimum value.
    ///
    /// Returns `None` for empty traversals.
    pub fn min(self) -> Option<Value> {
        self.execute()
            .map(|t| t.value)
            .min_by(|a, b| a.to_comparable().cmp(&b.to_comparable()))
    }

    /// Find the maximum value.
    ///
    /// Returns `None` for empty traversals.
    pub fn max(self) -> Option<Value> {
        self.execute()
            .map(|t| t.value)
            .max_by(|a, b| a.to_comparable().cmp(&b.to_comparable()))
    }
}

// =============================================================================
// Navigation Steps for Vertex Marker
// =============================================================================

impl<'g> TypedTraversal<'g, Vertex> {
    /// Navigate to outgoing adjacent vertices.
    ///
    /// Preserves the `Vertex` marker.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::traversal::typed::TypedTraversalSource;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let a = graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Alice".into()),
    /// ]));
    /// let b = graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Bob".into()),
    /// ]));
    /// graph.add_edge(a, b, "knows", HashMap::new()).unwrap();
    ///
    /// let snapshot = graph.snapshot();
    /// let g = TypedTraversalSource::new(&snapshot, graph.clone());
    ///
    /// // out() preserves Vertex marker
    /// let friends = g.v().out().to_list();
    /// assert_eq!(friends.len(), 1);
    /// ```
    pub fn out(self) -> TypedTraversal<'g, Vertex> {
        self.add_step(OutStep::new())
    }

    /// Navigate to outgoing adjacent vertices with label filter.
    pub fn out_labels(self, labels: &[&str]) -> TypedTraversal<'g, Vertex> {
        self.add_step(OutStep::with_labels(
            labels.iter().map(|s| s.to_string()).collect(),
        ))
    }

    /// Navigate to incoming adjacent vertices.
    ///
    /// Preserves the `Vertex` marker.
    pub fn in_(self) -> TypedTraversal<'g, Vertex> {
        self.add_step(InStep::new())
    }

    /// Navigate to incoming adjacent vertices with label filter.
    pub fn in_labels(self, labels: &[&str]) -> TypedTraversal<'g, Vertex> {
        self.add_step(InStep::with_labels(
            labels.iter().map(|s| s.to_string()).collect(),
        ))
    }

    /// Navigate to adjacent vertices in both directions.
    ///
    /// Preserves the `Vertex` marker.
    pub fn both(self) -> TypedTraversal<'g, Vertex> {
        self.add_step(BothStep::new())
    }

    /// Navigate to adjacent vertices in both directions with label filter.
    pub fn both_labels(self, labels: &[&str]) -> TypedTraversal<'g, Vertex> {
        self.add_step(BothStep::with_labels(
            labels.iter().map(|s| s.to_string()).collect(),
        ))
    }

    /// Navigate to outgoing edges.
    ///
    /// Transforms `Vertex` → `Edge`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::traversal::typed::TypedTraversalSource;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let a = graph.add_vertex("person", HashMap::new());
    /// let b = graph.add_vertex("person", HashMap::new());
    /// graph.add_edge(a, b, "knows", HashMap::new()).unwrap();
    ///
    /// let snapshot = graph.snapshot();
    /// let g = TypedTraversalSource::new(&snapshot, graph.clone());
    ///
    /// // out_e() transforms Vertex → Edge
    /// let edges = g.v().out_e().to_list();
    /// assert_eq!(edges.len(), 1);
    /// ```
    pub fn out_e(self) -> TypedTraversal<'g, Edge> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph,
            traversal: self.traversal.add_step(OutEStep::new()),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Navigate to outgoing edges with label filter.
    pub fn out_e_labels(self, labels: &[&str]) -> TypedTraversal<'g, Edge> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph,
            traversal: self.traversal.add_step(OutEStep::with_labels(
                labels.iter().map(|s| s.to_string()).collect(),
            )),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Navigate to incoming edges.
    ///
    /// Transforms `Vertex` → `Edge`.
    pub fn in_e(self) -> TypedTraversal<'g, Edge> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph,
            traversal: self.traversal.add_step(InEStep::new()),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Navigate to incoming edges with label filter.
    pub fn in_e_labels(self, labels: &[&str]) -> TypedTraversal<'g, Edge> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph,
            traversal: self.traversal.add_step(InEStep::with_labels(
                labels.iter().map(|s| s.to_string()).collect(),
            )),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Navigate to edges in both directions.
    ///
    /// Transforms `Vertex` → `Edge`.
    pub fn both_e(self) -> TypedTraversal<'g, Edge> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph,
            traversal: self.traversal.add_step(BothEStep::new()),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Navigate to edges in both directions with label filter.
    pub fn both_e_labels(self, labels: &[&str]) -> TypedTraversal<'g, Edge> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph,
            traversal: self.traversal.add_step(BothEStep::with_labels(
                labels.iter().map(|s| s.to_string()).collect(),
            )),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Extract property values.
    ///
    /// Transforms `Vertex` → `Scalar`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::traversal::typed::TypedTraversalSource;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let _ = graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Alice".into()),
    /// ]));
    ///
    /// let snapshot = graph.snapshot();
    /// let g = TypedTraversalSource::new(&snapshot, graph.clone());
    ///
    /// // values() transforms Vertex → Scalar
    /// let names = g.v().values("name").to_list();
    /// assert_eq!(names, vec![Value::String("Alice".to_string())]);
    /// ```
    pub fn values(self, key: &str) -> TypedTraversal<'g, Scalar> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph,
            traversal: self.traversal.add_step(ValuesStep::new(key)),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Extract vertex IDs.
    ///
    /// Transforms `Vertex` → `Scalar`.
    pub fn id(self) -> TypedTraversal<'g, Scalar> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph,
            traversal: self.traversal.add_step(IdStep),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Extract vertex labels.
    ///
    /// Transforms `Vertex` → `Scalar`.
    pub fn label(self) -> TypedTraversal<'g, Scalar> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph,
            traversal: self.traversal.add_step(LabelStep),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Filter by label.
    ///
    /// Preserves the `Vertex` marker.
    pub fn has_label(self, label: impl Into<String>) -> TypedTraversal<'g, Vertex> {
        self.add_step(HasLabelStep::single(label))
    }

    /// Filter by property existence.
    ///
    /// Preserves the `Vertex` marker.
    pub fn has(self, key: impl Into<String>) -> TypedTraversal<'g, Vertex> {
        self.add_step(HasStep::new(key))
    }

    /// Filter by property value.
    ///
    /// Preserves the `Vertex` marker.
    pub fn has_value(self, key: &str, value: impl Into<Value>) -> TypedTraversal<'g, Vertex> {
        self.add_step(HasValueStep::new(key, value.into()))
    }

    /// Limit the number of results.
    ///
    /// Preserves the `Vertex` marker.
    pub fn limit(self, n: usize) -> TypedTraversal<'g, Vertex> {
        self.add_step(LimitStep::new(n))
    }

    /// Skip the first n results.
    ///
    /// Preserves the `Vertex` marker.
    pub fn skip(self, n: usize) -> TypedTraversal<'g, Vertex> {
        self.add_step(SkipStep::new(n))
    }

    /// Take a range of results.
    ///
    /// Preserves the `Vertex` marker.
    pub fn range(self, start: usize, end: usize) -> TypedTraversal<'g, Vertex> {
        self.add_step(RangeStep::new(start, end))
    }

    /// Remove duplicate vertices.
    ///
    /// Preserves the `Vertex` marker.
    pub fn dedup(self) -> TypedTraversal<'g, Vertex> {
        self.add_step(DedupStep::new())
    }
}

// =============================================================================
// Navigation Steps for Edge Marker
// =============================================================================

impl<'g> TypedTraversal<'g, Edge> {
    /// Navigate to the source (outgoing) vertex of each edge.
    ///
    /// Transforms `Edge` → `Vertex`.
    pub fn out_v(self) -> TypedTraversal<'g, Vertex> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph,
            traversal: self.traversal.add_step(OutVStep),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Navigate to the destination (incoming) vertex of each edge.
    ///
    /// Transforms `Edge` → `Vertex`.
    pub fn in_v(self) -> TypedTraversal<'g, Vertex> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph,
            traversal: self.traversal.add_step(InVStep),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Navigate to both endpoint vertices of each edge.
    ///
    /// Transforms `Edge` → `Vertex`.
    pub fn both_v(self) -> TypedTraversal<'g, Vertex> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph,
            traversal: self
                .traversal
                .add_step(crate::traversal::navigation::BothVStep),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Extract property values from edges.
    ///
    /// Transforms `Edge` → `Scalar`.
    pub fn values(self, key: &str) -> TypedTraversal<'g, Scalar> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph,
            traversal: self.traversal.add_step(ValuesStep::new(key)),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Extract edge IDs.
    ///
    /// Transforms `Edge` → `Scalar`.
    pub fn id(self) -> TypedTraversal<'g, Scalar> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph,
            traversal: self.traversal.add_step(IdStep),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Extract edge labels.
    ///
    /// Transforms `Edge` → `Scalar`.
    pub fn label(self) -> TypedTraversal<'g, Scalar> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph,
            traversal: self.traversal.add_step(LabelStep),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Filter by label.
    ///
    /// Preserves the `Edge` marker.
    pub fn has_label(self, label: impl Into<String>) -> TypedTraversal<'g, Edge> {
        self.add_step(HasLabelStep::single(label))
    }

    /// Filter by property existence.
    ///
    /// Preserves the `Edge` marker.
    pub fn has(self, key: impl Into<String>) -> TypedTraversal<'g, Edge> {
        self.add_step(HasStep::new(key))
    }

    /// Filter by property value.
    ///
    /// Preserves the `Edge` marker.
    pub fn has_value(self, key: &str, value: impl Into<Value>) -> TypedTraversal<'g, Edge> {
        self.add_step(HasValueStep::new(key, value.into()))
    }

    /// Limit the number of results.
    ///
    /// Preserves the `Edge` marker.
    pub fn limit(self, n: usize) -> TypedTraversal<'g, Edge> {
        self.add_step(LimitStep::new(n))
    }

    /// Skip the first n results.
    ///
    /// Preserves the `Edge` marker.
    pub fn skip(self, n: usize) -> TypedTraversal<'g, Edge> {
        self.add_step(SkipStep::new(n))
    }

    /// Take a range of results.
    ///
    /// Preserves the `Edge` marker.
    pub fn range(self, start: usize, end: usize) -> TypedTraversal<'g, Edge> {
        self.add_step(RangeStep::new(start, end))
    }

    /// Remove duplicate edges.
    ///
    /// Preserves the `Edge` marker.
    pub fn dedup(self) -> TypedTraversal<'g, Edge> {
        self.add_step(DedupStep::new())
    }
}

// =============================================================================
// Filter Steps for Scalar Marker
// =============================================================================

impl<'g> TypedTraversal<'g, Scalar> {
    /// Limit the number of results.
    ///
    /// Preserves the `Scalar` marker.
    pub fn limit(self, n: usize) -> TypedTraversal<'g, Scalar> {
        self.add_step(LimitStep::new(n))
    }

    /// Skip the first n results.
    ///
    /// Preserves the `Scalar` marker.
    pub fn skip(self, n: usize) -> TypedTraversal<'g, Scalar> {
        self.add_step(SkipStep::new(n))
    }

    /// Take a range of results.
    ///
    /// Preserves the `Scalar` marker.
    pub fn range(self, start: usize, end: usize) -> TypedTraversal<'g, Scalar> {
        self.add_step(RangeStep::new(start, end))
    }

    /// Remove duplicate values.
    ///
    /// Preserves the `Scalar` marker.
    pub fn dedup(self) -> TypedTraversal<'g, Scalar> {
        self.add_step(DedupStep::new())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn test_graph() -> Arc<Graph> {
        let graph = Graph::new();
        let alice = graph.add_vertex(
            "person",
            HashMap::from([
                ("name".to_string(), "Alice".into()),
                ("age".to_string(), 30i64.into()),
            ]),
        );
        let bob = graph.add_vertex(
            "person",
            HashMap::from([
                ("name".to_string(), "Bob".into()),
                ("age".to_string(), 25i64.into()),
            ]),
        );
        let charlie = graph.add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Charlie".into())]),
        );
        graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
        graph
            .add_edge(bob, charlie, "knows", HashMap::new())
            .unwrap();
        Arc::new(graph)
    }

    // =========================================================================
    // Source Tests
    // =========================================================================

    #[test]
    fn typed_source_v() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        let vertices = g.v().to_list();
        assert_eq!(vertices.len(), 3);
    }

    #[test]
    fn typed_source_e() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        let edges = g.e().to_list();
        assert_eq!(edges.len(), 2);
    }

    #[test]
    fn typed_source_inject() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        let values = g.inject([1i64, 2, 3]).to_list();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], Value::Int(1));
    }

    // =========================================================================
    // Vertex Terminal Tests
    // =========================================================================

    #[test]
    fn vertex_next() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        let v: Option<GraphVertex> = g.v().next();
        assert!(v.is_some());
        assert!(v.unwrap().label().is_some());
    }

    #[test]
    fn vertex_to_list() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        let vertices: Vec<GraphVertex> = g.v().to_list();
        assert_eq!(vertices.len(), 3);
    }

    #[test]
    fn vertex_one() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        // Multiple vertices - should error
        let result = g.v().one();
        assert!(result.is_err());

        // Single vertex with limit - should succeed
        let result = g.v().limit(1).one();
        assert!(result.is_ok());
    }

    #[test]
    fn vertex_count() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        assert_eq!(g.v().count(), 3);
    }

    #[test]
    fn vertex_has_next() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        assert!(g.v().has_next());
    }

    #[test]
    fn vertex_take() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        let vertices = g.v().take(2);
        assert_eq!(vertices.len(), 2);
    }

    // =========================================================================
    // Edge Terminal Tests
    // =========================================================================

    #[test]
    fn edge_next() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        let e: Option<GraphEdge> = g.e().next();
        assert!(e.is_some());
        assert_eq!(e.unwrap().label(), Some("knows".to_string()));
    }

    #[test]
    fn edge_to_list() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        let edges: Vec<GraphEdge> = g.e().to_list();
        assert_eq!(edges.len(), 2);
    }

    #[test]
    fn edge_count() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        assert_eq!(g.e().count(), 2);
    }

    // =========================================================================
    // Scalar Terminal Tests
    // =========================================================================

    #[test]
    fn scalar_next() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        let name: Option<Value> = g.v().values("name").next();
        assert!(matches!(name, Some(Value::String(_))));
    }

    #[test]
    fn scalar_to_list() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        let names: Vec<Value> = g.v().values("name").to_list();
        assert_eq!(names.len(), 3);
    }

    #[test]
    fn scalar_sum() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        let total = g.v().values("age").sum();
        // Alice=30, Bob=25, Charlie has no age
        assert_eq!(total, Value::Int(55));
    }

    // =========================================================================
    // Navigation Tests
    // =========================================================================

    #[test]
    fn vertex_out_navigation() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        // Alice -> Bob -> Charlie
        let friends: Vec<GraphVertex> = g.v().out().to_list();
        // Alice knows Bob, Bob knows Charlie = 2 results
        assert_eq!(friends.len(), 2);
    }

    #[test]
    fn vertex_in_navigation() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        // Who is known by someone?
        let known: Vec<GraphVertex> = g.v().in_().to_list();
        // Bob is known by Alice, Charlie is known by Bob = 2 results
        assert_eq!(known.len(), 2);
    }

    #[test]
    fn vertex_to_edge_transformation() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        // out_e() transforms Vertex -> Edge
        let edges: Vec<GraphEdge> = g.v().out_e().to_list();
        assert_eq!(edges.len(), 2);
        assert!(edges.iter().all(|e| e.label() == Some("knows".to_string())));
    }

    #[test]
    fn edge_to_vertex_transformation() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        // out_v() transforms Edge -> Vertex
        let sources: Vec<GraphVertex> = g.e().out_v().to_list();
        assert_eq!(sources.len(), 2);

        // in_v() transforms Edge -> Vertex
        let targets: Vec<GraphVertex> = g.e().in_v().to_list();
        assert_eq!(targets.len(), 2);
    }

    #[test]
    fn vertex_to_scalar_transformation() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        // values() transforms Vertex -> Scalar
        let names: Vec<Value> = g.v().values("name").to_list();
        assert_eq!(names.len(), 3);

        // id() transforms Vertex -> Scalar
        let ids: Vec<Value> = g.v().id().to_list();
        assert_eq!(ids.len(), 3);

        // label() transforms Vertex -> Scalar
        let labels: Vec<Value> = g.v().label().to_list();
        assert_eq!(labels.len(), 3);
        assert!(labels
            .iter()
            .all(|l| *l == Value::String("person".to_string())));
    }

    // =========================================================================
    // Filter Tests
    // =========================================================================

    #[test]
    fn vertex_has_label_filter() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        let people: Vec<GraphVertex> = g.v().has_label("person").to_list();
        assert_eq!(people.len(), 3);

        let software: Vec<GraphVertex> = g.v().has_label("software").to_list();
        assert_eq!(software.len(), 0);
    }

    #[test]
    fn vertex_has_value_filter() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        let alice: Vec<GraphVertex> = g.v().has_value("name", "Alice").to_list();
        assert_eq!(alice.len(), 1);
        assert_eq!(
            alice[0].property("name"),
            Some(Value::String("Alice".to_string()))
        );
    }

    #[test]
    fn vertex_limit_filter() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        let limited: Vec<GraphVertex> = g.v().limit(2).to_list();
        assert_eq!(limited.len(), 2);
    }

    #[test]
    fn vertex_dedup_filter() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        // out() from all vertices may produce duplicates if graph has cycles
        // dedup() removes them
        let unique: Vec<GraphVertex> = g.v().out().dedup().to_list();
        // Bob and Charlie are the only out neighbors (no duplicates)
        assert_eq!(unique.len(), 2);
    }

    // =========================================================================
    // Chained Navigation Tests
    // =========================================================================

    #[test]
    fn chained_navigation() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        // Alice -> Bob -> Charlie (2 hops)
        let fof: Vec<GraphVertex> = g.v().out().out().to_list();
        // Only Alice->Bob->Charlie path
        assert_eq!(fof.len(), 1);
        assert_eq!(
            fof[0].property("name"),
            Some(Value::String("Charlie".to_string()))
        );
    }

    #[test]
    fn edge_chain_navigation() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        // Get edge, get source vertex, get their name
        let source_names: Vec<Value> = g.e().out_v().values("name").to_list();
        assert_eq!(source_names.len(), 2);
    }

    // =========================================================================
    // Escape Hatch Tests
    // =========================================================================

    #[test]
    fn escape_hatch_next_value() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        // next_value() works on any marker type
        let v: Option<Value> = g.v().next_value();
        assert!(matches!(v, Some(Value::Vertex(_))));

        let e: Option<Value> = g.e().next_value();
        assert!(matches!(e, Some(Value::Edge(_))));
    }

    #[test]
    fn escape_hatch_to_value_list() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = TypedTraversalSource::new(&snapshot, graph.clone());

        // to_value_list() works on any marker type
        let values: Vec<Value> = g.v().to_value_list();
        assert_eq!(values.len(), 3);
        assert!(values.iter().all(|v| matches!(v, Value::Vertex(_))));
    }
}
