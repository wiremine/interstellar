# Plan 30: TinkerPop-Style Vertex & Edge Objects

## Overview

Implement TinkerPop-faithful vertex/edge objects where `g.v().next()` returns a rich `GraphVertex` object (not just an ID) that can access properties and spawn traversals directly.

**Spec**: `specs/tinkerpop-vertex-objects.md`

## Goals

Enable this usage pattern:

```rust
// Get a vertex and continue traversing from it
let v = g.v().has_value("name", "Alice").next().unwrap();
let friends = v.out("knows").to_list();

// Access vertex properties directly
println!("Label: {}", v.label());
println!("Name: {}", v.property("name").unwrap());

// Mutate via the vertex object
v.property_set("age", 31);
```

## Current State

- `g.v().next()` returns `Option<Value>` where `Value::Vertex(VertexId)` is just an ID
- No graph reference in traversal results - cannot access properties or traverse
- `BoundTraversal<'g, In, Out>` has generic `Out` type but no compile-time step validation

## Target State

- `g.v().next()` returns `Option<GraphVertex>` - a live object with graph reference
- `g.e().next()` returns `Option<GraphEdge>` - a live object with graph reference
- `g.v().values("name").next()` returns `Option<Value>` - scalar values unchanged
- Marker types (`Vertex`, `Edge`, `Scalar`) track output type at compile time
- Step methods transform markers appropriately

## Breaking Changes

This is a **breaking change**:
- `g.v().next()` changes from `Option<Value>` to `Option<GraphVertex>`
- `g.e().next()` changes from `Option<Value>` to `Option<GraphEdge>`

**Migration path**: Use `.next_value()` or `.to_value_list()` for old behavior.

---

## Implementation Phases

### Phase 1: Core Types (Non-Breaking) — 3-4 days

Add new types without changing existing behavior. All existing code continues to work.

#### 1.1 Create `src/graph_elements.rs`

New file with `GraphVertex` and `GraphEdge` types:

```rust
// src/graph_elements.rs

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::StorageError;
use crate::storage::cow::Graph;
use crate::storage::GraphStorage;
use crate::value::{EdgeId, Value, VertexId};

/// A vertex reference with access to the graph.
///
/// `GraphVertex` provides TinkerPop-style vertex semantics where
/// a vertex object can access its properties and spawn traversals.
///
/// # Thread Safety
///
/// `GraphVertex` is `Clone`, `Send`, and `Sync`. Multiple vertices
/// can reference the same graph concurrently.
#[derive(Clone)]
pub struct GraphVertex {
    id: VertexId,
    graph: Arc<Graph>,
}

impl GraphVertex {
    /// Create a new GraphVertex.
    pub(crate) fn new(id: VertexId, graph: Arc<Graph>) -> Self {
        Self { id, graph }
    }

    /// Get the vertex ID.
    pub fn id(&self) -> VertexId {
        self.id
    }

    /// Get the vertex label.
    pub fn label(&self) -> Option<String> {
        let snapshot = self.graph.snapshot();
        snapshot.get_vertex(self.id).map(|v| v.label)
    }

    /// Get a property value.
    pub fn property(&self, key: &str) -> Option<Value> {
        let snapshot = self.graph.snapshot();
        snapshot.get_vertex(self.id)
            .and_then(|v| v.properties.get(key).cloned())
    }

    /// Get all properties as a map.
    pub fn properties(&self) -> HashMap<String, Value> {
        let snapshot = self.graph.snapshot();
        snapshot.get_vertex(self.id)
            .map(|v| v.properties)
            .unwrap_or_default()
    }

    /// Check if the vertex still exists in the graph.
    pub fn exists(&self) -> bool {
        let snapshot = self.graph.snapshot();
        snapshot.get_vertex(self.id).is_some()
    }

    /// Set a property value.
    pub fn property_set(&self, key: &str, value: impl Into<Value>) -> Result<(), StorageError> {
        self.graph.set_vertex_property(self.id, key, value.into())
    }

    /// Get the graph reference.
    pub fn graph(&self) -> &Arc<Graph> {
        &self.graph
    }

    /// Convert to a lightweight Value for serialization.
    pub fn to_value(&self) -> Value {
        Value::Vertex(self.id)
    }
}

impl std::fmt::Debug for GraphVertex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphVertex")
            .field("id", &self.id)
            .field("label", &self.label())
            .finish()
    }
}

impl PartialEq for GraphVertex {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for GraphVertex {}

impl std::hash::Hash for GraphVertex {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

/// An edge reference with access to the graph.
#[derive(Clone)]
pub struct GraphEdge {
    id: EdgeId,
    graph: Arc<Graph>,
}

impl GraphEdge {
    /// Create a new GraphEdge.
    pub(crate) fn new(id: EdgeId, graph: Arc<Graph>) -> Self {
        Self { id, graph }
    }

    /// Get the edge ID.
    pub fn id(&self) -> EdgeId {
        self.id
    }

    /// Get the edge label.
    pub fn label(&self) -> Option<String> {
        let snapshot = self.graph.snapshot();
        snapshot.get_edge(self.id).map(|e| e.label)
    }

    /// Get the source (outgoing) vertex.
    pub fn out_v(&self) -> Option<GraphVertex> {
        let snapshot = self.graph.snapshot();
        snapshot.get_edge(self.id)
            .map(|e| GraphVertex::new(e.src, self.graph.clone()))
    }

    /// Get the destination (incoming) vertex.
    pub fn in_v(&self) -> Option<GraphVertex> {
        let snapshot = self.graph.snapshot();
        snapshot.get_edge(self.id)
            .map(|e| GraphVertex::new(e.dst, self.graph.clone()))
    }

    /// Get both endpoint vertices as (out, in) tuple.
    pub fn both_v(&self) -> Option<(GraphVertex, GraphVertex)> {
        let snapshot = self.graph.snapshot();
        snapshot.get_edge(self.id).map(|e| {
            (
                GraphVertex::new(e.src, self.graph.clone()),
                GraphVertex::new(e.dst, self.graph.clone()),
            )
        })
    }

    /// Get a property value.
    pub fn property(&self, key: &str) -> Option<Value> {
        let snapshot = self.graph.snapshot();
        snapshot.get_edge(self.id)
            .and_then(|e| e.properties.get(key).cloned())
    }

    /// Get all properties.
    pub fn properties(&self) -> HashMap<String, Value> {
        let snapshot = self.graph.snapshot();
        snapshot.get_edge(self.id)
            .map(|e| e.properties)
            .unwrap_or_default()
    }

    /// Set a property value.
    pub fn property_set(&self, key: &str, value: impl Into<Value>) -> Result<(), StorageError> {
        self.graph.set_edge_property(self.id, key, value.into())
    }

    /// Check if the edge still exists.
    pub fn exists(&self) -> bool {
        let snapshot = self.graph.snapshot();
        snapshot.get_edge(self.id).is_some()
    }

    /// Get the graph reference.
    pub fn graph(&self) -> &Arc<Graph> {
        &self.graph
    }

    /// Convert to a lightweight Value for serialization.
    pub fn to_value(&self) -> Value {
        Value::Edge(self.id)
    }
}

impl std::fmt::Debug for GraphEdge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphEdge")
            .field("id", &self.id)
            .field("label", &self.label())
            .finish()
    }
}

impl PartialEq for GraphEdge {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for GraphEdge {}

impl std::hash::Hash for GraphEdge {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
```

#### 1.2 Create `src/traversal/markers.rs`

New file with marker types for compile-time step validation:

```rust
// src/traversal/markers.rs

use std::marker::PhantomData;

use crate::graph_elements::{GraphEdge, GraphVertex};
use crate::value::Value;

/// Marker indicating traversal produces vertices.
#[derive(Clone, Copy, Debug, Default)]
pub struct Vertex;

/// Marker indicating traversal produces edges.
#[derive(Clone, Copy, Debug, Default)]
pub struct Edge;

/// Marker indicating traversal produces arbitrary values.
#[derive(Clone, Copy, Debug, Default)]
pub struct Scalar;

/// Trait for traversal output markers.
///
/// Sealed trait - only Vertex, Edge, and Scalar implement this.
pub trait OutputMarker: Clone + Send + Sync + 'static {
    /// The terminal return type for `next()`
    type Output;
    
    /// The terminal return type for `to_list()`  
    type OutputList;
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
```

#### 1.3 Update Module Structure

Add to `src/lib.rs`:
```rust
pub mod graph_elements;
pub use graph_elements::{GraphVertex, GraphEdge};
```

Add to `src/traversal/mod.rs`:
```rust
pub mod markers;
pub use markers::{Vertex as VertexMarker, Edge as EdgeMarker, Scalar as ScalarMarker, OutputMarker};
```

#### 1.4 Add `Arc<Graph>` to COW Traversal Infrastructure

Update `CowTraversalSource` and `CowBoundTraversal` in `src/storage/cow.rs` to carry `Arc<Graph>`:

```rust
// In cow.rs, update CowTraversalSource
pub struct CowTraversalSource<'g> {
    graph: &'g Graph,
    graph_arc: Arc<Graph>,  // NEW: for creating GraphVertex/GraphEdge
    // ... existing fields
}

impl<'g> CowTraversalSource<'g> {
    pub(crate) fn new(graph: &'g Graph) -> Self {
        Self {
            graph,
            graph_arc: Arc::new(/* need to create or share Arc */),
            // ...
        }
    }
}
```

**Challenge**: `Graph` doesn't currently wrap itself in `Arc`. Options:
1. Add `Arc::new(self)` pattern (requires owned Graph)
2. Use `Arc<GraphState>` from snapshot instead
3. Store `Weak<Graph>` reference

**Recommended approach**: Create a new `ArcGraph` wrapper or modify `Graph` to be `Arc`-friendly.

#### 1.5 Add Type-Specific Terminal Methods (Non-Breaking)

Add new methods alongside existing ones:

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    // Existing methods stay unchanged
    pub fn to_list(self) -> Vec<Value> { ... }
    pub fn next(self) -> Option<Value> { ... }
    
    // NEW: Type-specific terminal methods
    /// Execute and return all results as vertices.
    /// Panics if traversal doesn't produce vertices.
    pub fn to_vertex_list(self, graph: Arc<Graph>) -> Vec<GraphVertex> {
        self.execute()
            .filter_map(|t| match t.value {
                Value::Vertex(id) => Some(GraphVertex::new(id, graph.clone())),
                _ => None,
            })
            .collect()
    }
    
    /// Execute and return the first vertex.
    pub fn next_vertex(self, graph: Arc<Graph>) -> Option<GraphVertex> {
        self.execute()
            .find_map(|t| match t.value {
                Value::Vertex(id) => Some(GraphVertex::new(id, graph.clone())),
                _ => None,
            })
    }
    
    /// Execute and return all results as edges.
    pub fn to_edge_list(self, graph: Arc<Graph>) -> Vec<GraphEdge> {
        self.execute()
            .filter_map(|t| match t.value {
                Value::Edge(id) => Some(GraphEdge::new(id, graph.clone())),
                _ => None,
            })
            .collect()
    }
    
    /// Execute and return the first edge.
    pub fn next_edge(self, graph: Arc<Graph>) -> Option<GraphEdge> {
        self.execute()
            .find_map(|t| match t.value {
                Value::Edge(id) => Some(GraphEdge::new(id, graph.clone())),
                _ => None,
            })
    }
}
```

#### 1.6 Tests for Phase 1

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    fn test_graph() -> Arc<Graph> {
        let graph = Graph::new();
        let alice = graph.add_vertex("person", HashMap::from([
            ("name".to_string(), "Alice".into()),
            ("age".to_string(), 30i64.into()),
        ]));
        let bob = graph.add_vertex("person", HashMap::from([
            ("name".to_string(), "Bob".into()),
        ]));
        graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
        Arc::new(graph)
    }
    
    #[test]
    fn graph_vertex_property_access() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();
        
        let alice_id = g.v().has_value("name", "Alice").next().unwrap();
        let alice = GraphVertex::new(alice_id.as_vertex_id().unwrap(), graph.clone());
        
        assert_eq!(alice.label(), Some("person".to_string()));
        assert_eq!(alice.property("name"), Some(Value::String("Alice".into())));
        assert_eq!(alice.property("age"), Some(Value::Int(30)));
    }
    
    #[test]
    fn graph_vertex_mutation() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();
        
        let alice_id = g.v().has_value("name", "Alice").next().unwrap();
        let alice = GraphVertex::new(alice_id.as_vertex_id().unwrap(), graph.clone());
        
        alice.property_set("age", 31i64).unwrap();
        
        // New snapshot sees the change
        let snapshot2 = graph.snapshot();
        let g2 = snapshot2.gremlin();
        let age = g2.v().has_value("name", "Alice").values("age").next();
        assert_eq!(age, Some(Value::Int(31)));
    }
    
    #[test]
    fn graph_edge_endpoint_access() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();
        
        let edge_id = g.e().next().unwrap();
        let edge = GraphEdge::new(edge_id.as_edge_id().unwrap(), graph.clone());
        
        assert_eq!(edge.label(), Some("knows".to_string()));
        assert!(edge.out_v().is_some());
        assert!(edge.in_v().is_some());
        
        let (src, dst) = edge.both_v().unwrap();
        assert_eq!(src.property("name"), Some(Value::String("Alice".into())));
        assert_eq!(dst.property("name"), Some(Value::String("Bob".into())));
    }
}
```

### Phase 2: Marker-Based Type State — 3-4 days

Add compile-time tracking of output types using marker types.

#### 2.1 Update `BoundTraversal` with Marker

Create a new typed traversal wrapper:

```rust
// src/traversal/typed.rs (NEW FILE)

use std::marker::PhantomData;
use std::sync::Arc;

use crate::graph_elements::{GraphEdge, GraphVertex};
use crate::storage::cow::Graph;
use crate::storage::GraphStorage;
use crate::storage::interner::StringInterner;
use crate::traversal::markers::{Edge, OutputMarker, Scalar, Vertex};
use crate::traversal::{Traversal, TraversalSource, Traverser};
use crate::value::{Value, VertexId, EdgeId};

/// A typed traversal bound to a graph with compile-time output tracking.
///
/// The `Marker` type parameter tracks what the traversal produces:
/// - `Vertex` → terminal methods return `GraphVertex`
/// - `Edge` → terminal methods return `GraphEdge`  
/// - `Scalar` → terminal methods return `Value`
pub struct TypedTraversal<'g, Marker: OutputMarker> {
    storage: &'g dyn GraphStorage,
    interner: &'g StringInterner,
    graph: Arc<Graph>,
    traversal: Traversal<(), Value>,
    track_paths: bool,
    _marker: PhantomData<Marker>,
}

impl<'g, Marker: OutputMarker> TypedTraversal<'g, Marker> {
    /// Internal: change marker type
    fn cast<NewMarker: OutputMarker>(self) -> TypedTraversal<'g, NewMarker> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph,
            traversal: self.traversal,
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }
    
    /// Escape hatch: get raw Value results regardless of marker
    pub fn next_value(self) -> Option<Value> {
        self.execute().next().map(|t| t.value)
    }
    
    /// Escape hatch: get raw Value list regardless of marker
    pub fn to_value_list(self) -> Vec<Value> {
        self.execute().map(|t| t.value).collect()
    }
}
```

#### 2.2 Implement Terminal Methods by Marker

```rust
// Terminal methods for Vertex marker
impl<'g> TypedTraversal<'g, Vertex> {
    /// Execute and return the first vertex.
    pub fn next(self) -> Option<GraphVertex> {
        // Note: Arc::clone is cheap (just ref count increment)
        // We need to clone before execute() consumes self
        let graph = Arc::clone(&self.graph);
        self.execute()
            .find_map(|t| match t.value {
                Value::Vertex(id) => Some(GraphVertex::new(id, Arc::clone(&graph))),
                _ => None,
            })
    }
    
    /// Execute and return all vertices.
    pub fn to_list(self) -> Vec<GraphVertex> {
        let graph = Arc::clone(&self.graph);
        self.execute()
            .filter_map(|t| match t.value {
                Value::Vertex(id) => Some(GraphVertex::new(id, Arc::clone(&graph))),
                _ => None,
            })
            .collect()
    }
    
    /// Execute and return exactly one vertex.
    pub fn one(self) -> Result<GraphVertex, crate::error::TraversalError> {
        let graph = Arc::clone(&self.graph);
        let results: Vec<_> = self.execute()
            .filter_map(|t| match t.value {
                Value::Vertex(id) => Some(id),
                _ => None,
            })
            .take(2)
            .collect();
        match results.len() {
            1 => Ok(GraphVertex::new(results[0], graph)),
            n => Err(crate::error::TraversalError::NotOne(n)),
        }
    }
    
    /// Execute and collect unique vertices into a set.
    pub fn to_set(self) -> std::collections::HashSet<GraphVertex> {
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
    pub fn iter(self) -> impl Iterator<Item = GraphVertex> + 'g {
        let graph = Arc::clone(&self.graph);
        self.execute()
            .filter_map(move |t| match t.value {
                Value::Vertex(id) => Some(GraphVertex::new(id, Arc::clone(&graph))),
                _ => None,
            })
    }
}

// Terminal methods for Edge marker
impl<'g> TypedTraversal<'g, Edge> {
    /// Execute and return the first edge.
    pub fn next(self) -> Option<GraphEdge> {
        let graph = Arc::clone(&self.graph);
        self.execute()
            .find_map(|t| match t.value {
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
    
    /// Execute and return exactly one edge.
    pub fn one(self) -> Result<GraphEdge, crate::error::TraversalError> {
        let graph = Arc::clone(&self.graph);
        let results: Vec<_> = self.execute()
            .filter_map(|t| match t.value {
                Value::Edge(id) => Some(id),
                _ => None,
            })
            .take(2)
            .collect();
        match results.len() {
            1 => Ok(GraphEdge::new(results[0], graph)),
            n => Err(crate::error::TraversalError::NotOne(n)),
        }
    }
    
    /// Execute and collect unique edges into a set.
    pub fn to_set(self) -> std::collections::HashSet<GraphEdge> {
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
    pub fn iter(self) -> impl Iterator<Item = GraphEdge> + 'g {
        let graph = Arc::clone(&self.graph);
        self.execute()
            .filter_map(move |t| match t.value {
                Value::Edge(id) => Some(GraphEdge::new(id, Arc::clone(&graph))),
                _ => None,
            })
    }
}

// Terminal methods for Scalar marker
impl<'g> TypedTraversal<'g, Scalar> {
    /// Execute and return the first value.
    pub fn next(self) -> Option<Value> {
        self.execute().next().map(|t| t.value)
    }
    
    /// Execute and return all values.
    pub fn to_list(self) -> Vec<Value> {
        self.execute().map(|t| t.value).collect()
    }
    
    /// Execute and return exactly one value.
    pub fn one(self) -> Result<Value, crate::error::TraversalError> {
        let results: Vec<_> = self.execute().take(2).collect();
        match results.len() {
            1 => Ok(results.into_iter().next().unwrap().value),
            n => Err(crate::error::TraversalError::NotOne(n)),
        }
    }
    
    /// Execute and collect unique values into a set.
    pub fn to_set(self) -> std::collections::HashSet<Value> {
        self.to_list().into_iter().collect()
    }
    
    /// Check if the traversal produces any values.
    pub fn has_next(self) -> bool {
        self.next().is_some()
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
    pub fn iter(self) -> impl Iterator<Item = Value> + 'g {
        self.execute().map(|t| t.value)
    }
    
    /// Sum all numeric values.
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
}
```

#### 2.3 Implement Step Methods with Marker Transformations

```rust
// Navigation steps for Vertex marker
impl<'g> TypedTraversal<'g, Vertex> {
    /// Navigate to outgoing adjacent vertices (preserves Vertex marker)
    pub fn out(self) -> TypedTraversal<'g, Vertex> {
        use crate::traversal::OutStep;
        let traversal = self.traversal.add_step(OutStep::new(vec![]));
        TypedTraversal { traversal, ..self }
    }
    
    /// Navigate to outgoing edges (transforms Vertex → Edge)
    pub fn out_e(self) -> TypedTraversal<'g, Edge> {
        use crate::traversal::OutEStep;
        let traversal = self.traversal.add_step(OutEStep::new(vec![]));
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph,
            traversal,
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }
    
    /// Extract property values (transforms Vertex → Scalar)
    pub fn values(self, key: &str) -> TypedTraversal<'g, Scalar> {
        use crate::traversal::ValuesStep;
        let traversal = self.traversal.add_step(ValuesStep::new(key));
        self.cast()
    }
    
    /// Filter by label (preserves Vertex marker)
    pub fn has_label(self, label: &str) -> TypedTraversal<'g, Vertex> {
        use crate::traversal::HasLabelStep;
        let traversal = self.traversal.add_step(HasLabelStep::new(label));
        TypedTraversal { traversal, ..self }
    }
}

// Navigation steps for Edge marker
impl<'g> TypedTraversal<'g, Edge> {
    /// Navigate to source vertices (transforms Edge → Vertex)
    pub fn out_v(self) -> TypedTraversal<'g, Vertex> {
        use crate::traversal::OutVStep;
        let traversal = self.traversal.add_step(OutVStep);
        self.cast()
    }
    
    /// Navigate to destination vertices (transforms Edge → Vertex)
    pub fn in_v(self) -> TypedTraversal<'g, Vertex> {
        use crate::traversal::InVStep;
        let traversal = self.traversal.add_step(InVStep);
        self.cast()
    }
    
    /// Filter by label (preserves Edge marker)
    pub fn has_label(self, label: &str) -> TypedTraversal<'g, Edge> {
        use crate::traversal::HasLabelStep;
        let traversal = self.traversal.add_step(HasLabelStep::new(label));
        TypedTraversal { traversal, ..self }
    }
}
```

#### 2.4 Typed Source Methods

```rust
/// Typed traversal source that produces typed traversals.
pub struct TypedTraversalSource<'g> {
    storage: &'g dyn GraphStorage,
    interner: &'g StringInterner,
    graph: Arc<Graph>,
}

impl<'g> TypedTraversalSource<'g> {
    /// Start traversal from all vertices.
    /// Returns `TypedTraversal<Vertex>` where `next()` → `GraphVertex`
    pub fn v(&self) -> TypedTraversal<'g, Vertex> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph.clone(),
            traversal: Traversal::with_source(TraversalSource::AllVertices),
            track_paths: false,
            _marker: PhantomData,
        }
    }
    
    /// Start traversal from all edges.
    /// Returns `TypedTraversal<Edge>` where `next()` → `GraphEdge`
    pub fn e(&self) -> TypedTraversal<'g, Edge> {
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph.clone(),
            traversal: Traversal::with_source(TraversalSource::AllEdges),
            track_paths: false,
            _marker: PhantomData,
        }
    }
    
    /// Inject arbitrary values.
    /// Returns `TypedTraversal<Scalar>` where `next()` → `Value`
    pub fn inject<T, I>(&self, values: I) -> TypedTraversal<'g, Scalar>
    where
        I: IntoIterator<Item = T>,
        T: Into<Value>,
    {
        let values: Vec<Value> = values.into_iter().map(Into::into).collect();
        TypedTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph.clone(),
            traversal: Traversal::with_source(TraversalSource::Inject(values)),
            track_paths: false,
            _marker: PhantomData,
        }
    }
}
```

#### 2.5 Tests for Phase 2

```rust
#[test]
fn typed_traversal_vertex_marker() {
    let graph = Arc::new(test_graph());
    let g = TypedTraversalSource::new(&graph);
    
    // g.v().next() returns Option<GraphVertex>
    let v: Option<GraphVertex> = g.v().next();
    assert!(v.is_some());
    
    // g.v().out().to_list() returns Vec<GraphVertex>
    let friends: Vec<GraphVertex> = g.v().out().to_list();
}

#[test]
fn typed_traversal_marker_transforms() {
    let graph = Arc::new(test_graph());
    let g = TypedTraversalSource::new(&graph);
    
    // out_e() transforms Vertex → Edge
    let e: Option<GraphEdge> = g.v().out_e().next();
    assert!(e.is_some());
    
    // out_v() transforms Edge → Vertex
    let v: Option<GraphVertex> = g.v().out_e().out_v().next();
    assert!(v.is_some());
    
    // values() transforms to Scalar
    let name: Option<Value> = g.v().values("name").next();
    assert!(matches!(name, Some(Value::String(_))));
}

#[test]
fn escape_hatch_to_value() {
    let graph = Arc::new(test_graph());
    let g = TypedTraversalSource::new(&graph);
    
    // next_value() works on any marker type
    let v: Option<Value> = g.v().next_value();
    assert!(matches!(v, Some(Value::Vertex(_))));
}
```

### Phase 3: GraphVertex Traversal Methods — 2-3 days

Enable `v.out("knows").to_list()` pattern from a `GraphVertex` object.

#### 3.1 Add Traversal Methods to GraphVertex

```rust
// Add to src/graph_elements.rs

impl GraphVertex {
    /// Traverse to outgoing adjacent vertices.
    pub fn out(&self, label: &str) -> GraphVertexTraversal {
        GraphVertexTraversal::new(self.graph.clone(), self.id)
            .out_label(label)
    }
    
    /// Traverse to outgoing adjacent vertices (all labels).
    pub fn out_all(&self) -> GraphVertexTraversal {
        GraphVertexTraversal::new(self.graph.clone(), self.id).out()
    }
    
    /// Traverse to incoming adjacent vertices.
    pub fn in_(&self, label: &str) -> GraphVertexTraversal {
        GraphVertexTraversal::new(self.graph.clone(), self.id)
            .in_label(label)
    }
    
    /// Traverse to incoming adjacent vertices (all labels).
    pub fn in_all(&self) -> GraphVertexTraversal {
        GraphVertexTraversal::new(self.graph.clone(), self.id).in_()
    }
    
    /// Traverse to adjacent vertices in both directions.
    pub fn both(&self, label: &str) -> GraphVertexTraversal {
        GraphVertexTraversal::new(self.graph.clone(), self.id)
            .both_label(label)
    }
    
    /// Add an outgoing edge to another vertex.
    pub fn add_edge(&self, label: &str, to: &GraphVertex) -> Result<GraphEdge, StorageError> {
        self.add_edge_to_id(label, to.id)
    }
    
    /// Add an outgoing edge to a vertex by ID.
    pub fn add_edge_to_id(&self, label: &str, to: VertexId) -> Result<GraphEdge, StorageError> {
        let edge_id = self.graph.add_edge(self.id, to, label, HashMap::new())?;
        Ok(GraphEdge::new(edge_id, self.graph.clone()))
    }
    
    /// Remove this vertex from the graph (and all incident edges).
    pub fn remove(&self) -> Result<(), StorageError> {
        self.graph.remove_vertex(self.id)
    }
}
```

#### 3.2 Create GraphVertexTraversal Builder

```rust
// src/graph_elements.rs (continued)

/// A traversal starting from a specific vertex.
pub struct GraphVertexTraversal {
    graph: Arc<Graph>,
    start_id: VertexId,
    steps: Vec<TraversalStep>,
}

#[derive(Clone)]
enum TraversalStep {
    Out(Option<String>),
    In(Option<String>),
    Both(Option<String>),
    HasLabel(String),
    HasValue(String, Value),
}

impl GraphVertexTraversal {
    pub(crate) fn new(graph: Arc<Graph>, start_id: VertexId) -> Self {
        Self {
            graph,
            start_id,
            steps: Vec::new(),
        }
    }
    
    pub fn out(mut self) -> Self {
        self.steps.push(TraversalStep::Out(None));
        self
    }
    
    pub fn out_label(mut self, label: &str) -> Self {
        self.steps.push(TraversalStep::Out(Some(label.to_string())));
        self
    }
    
    pub fn in_(mut self) -> Self {
        self.steps.push(TraversalStep::In(None));
        self
    }
    
    pub fn in_label(mut self, label: &str) -> Self {
        self.steps.push(TraversalStep::In(Some(label.to_string())));
        self
    }
    
    pub fn both(mut self) -> Self {
        self.steps.push(TraversalStep::Both(None));
        self
    }
    
    pub fn both_label(mut self, label: &str) -> Self {
        self.steps.push(TraversalStep::Both(Some(label.to_string())));
        self
    }
    
    pub fn has_label(mut self, label: &str) -> Self {
        self.steps.push(TraversalStep::HasLabel(label.to_string()));
        self
    }
    
    pub fn has_value(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.steps.push(TraversalStep::HasValue(key.to_string(), value.into()));
        self
    }
    
    /// Execute and return all vertices.
    pub fn to_list(self) -> Vec<GraphVertex> {
        // Build traversal from start vertex
        let snapshot = self.graph.snapshot();
        let g = snapshot.gremlin();
        let mut traversal = g.v_ids([self.start_id]);
        
        for step in &self.steps {
            traversal = match step {
                TraversalStep::Out(None) => traversal.out(),
                TraversalStep::Out(Some(label)) => traversal.out_labels(&[label.as_str()]),
                TraversalStep::In(None) => traversal.in_(),
                TraversalStep::In(Some(label)) => traversal.in_labels(&[label.as_str()]),
                TraversalStep::Both(None) => traversal.both(),
                TraversalStep::Both(Some(label)) => traversal.both_labels(&[label.as_str()]),
                TraversalStep::HasLabel(label) => traversal.has_label(label),
                TraversalStep::HasValue(key, value) => traversal.has_value(key, value.clone()),
            };
        }
        
        traversal.to_list()
            .into_iter()
            .filter_map(|v| v.as_vertex_id())
            .map(|id| GraphVertex::new(id, self.graph.clone()))
            .collect()
    }
    
    /// Execute and return the first vertex.
    pub fn first(self) -> Option<GraphVertex> {
        self.to_list().into_iter().next()
    }
    
    /// Execute and count results.
    pub fn count(self) -> usize {
        self.to_list().len()
    }
    
    /// Check if any results exist.
    pub fn exists(self) -> bool {
        self.first().is_some()
    }
}
```

#### 3.3 Tests for Phase 3

```rust
#[test]
fn vertex_object_traversal() {
    let graph = Arc::new(test_graph_with_chain()); // Alice -> Bob -> Charlie
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();
    
    // Get Alice
    let alice_id = g.v().has_value("name", "Alice").next().unwrap().as_vertex_id().unwrap();
    let alice = GraphVertex::new(alice_id, graph.clone());
    
    // Traverse from the vertex object
    let friends = alice.out("knows").to_list();
    assert_eq!(friends.len(), 1);
    assert_eq!(friends[0].property("name"), Some(Value::String("Bob".into())));
    
    // Chain traversals
    let fof = alice.out("knows").out("knows").to_list();
    assert_eq!(fof.len(), 1);
    assert_eq!(fof[0].property("name"), Some(Value::String("Charlie".into())));
}

#[test]
fn vertex_add_edge() {
    let graph = Arc::new(Graph::new());
    
    let alice_id = graph.add_vertex("person", HashMap::from([
        ("name".to_string(), "Alice".into()),
    ]));
    let bob_id = graph.add_vertex("person", HashMap::from([
        ("name".to_string(), "Bob".into()),
    ]));
    
    let alice = GraphVertex::new(alice_id, graph.clone());
    let bob = GraphVertex::new(bob_id, graph.clone());
    
    // Add edge via vertex object
    let edge = alice.add_edge("knows", &bob).unwrap();
    assert_eq!(edge.label(), Some("knows".to_string()));
    
    // Verify traversal works
    let friends = alice.out("knows").to_list();
    assert_eq!(friends.len(), 1);
    assert_eq!(friends[0].id(), bob_id);
}
```

### Phase 4: Integration & Migration — 3-4 days

Update COW traversals and provide migration path.

#### 4.1 Update CowBoundTraversal

Add `Arc<Graph>` to `CowBoundTraversal` in `src/storage/cow.rs`:

```rust
pub struct CowBoundTraversal<'g, In, Out, Marker: OutputMarker = Scalar> {
    graph: &'g Graph,
    graph_arc: Option<Arc<Graph>>,  // For creating GraphVertex/GraphEdge
    traversal: Traversal<In, Out>,
    mutations: Vec<PendingMutation>,
    track_paths: bool,
    _marker: PhantomData<Marker>,
}
```

#### 4.2 Provide Typed CowTraversalSource

```rust
impl<'g> CowTraversalSource<'g> {
    /// Start typed traversal from all vertices.
    /// Terminal methods return GraphVertex.
    pub fn v_typed(&self) -> CowBoundTraversal<'g, (), Value, Vertex> {
        // ...
    }
    
    /// Start typed traversal from all edges.
    /// Terminal methods return GraphEdge.
    pub fn e_typed(&self) -> CowBoundTraversal<'g, (), Value, Edge> {
        // ...
    }
}
```

#### 4.3 Breaking Change: Update Default Behavior

After confirming the new API works, update `g.v()` and `g.e()` to return typed traversals:

```rust
// BREAKING: g.v().next() now returns Option<GraphVertex>
pub fn v(&self) -> CowBoundTraversal<'g, (), Value, Vertex> {
    // ...
}
```

Provide escape hatches:
```rust
impl<'g, In, Out, Marker: OutputMarker> CowBoundTraversal<'g, In, Out, Marker> {
    /// Execute and return raw Values (migration helper).
    pub fn next_value(self) -> Option<Value> { ... }
    pub fn to_value_list(self) -> Vec<Value> { ... }
}
```

#### 4.4 Update Rhai Integration

Register `GraphVertex` and `GraphEdge` as Rhai types:

```rust
// In src/rhai/types.rs
engine.register_type::<GraphVertex>()
    .register_fn("id", GraphVertex::id)
    .register_fn("label", GraphVertex::label)
    .register_fn("property", GraphVertex::property)
    .register_fn("exists", GraphVertex::exists);
```

#### 4.5 Update Documentation and Examples

- Update `README.md` with new API
- Update docstrings to show `GraphVertex`/`GraphEdge` returns
- Add migration guide for breaking changes

---

## Summary

| Phase | Scope | Effort | Risk |
|-------|-------|--------|------|
| 1. Core Types | `GraphVertex`, `GraphEdge`, markers, escape hatches | 3-4 days | Low |
| 2. Marker Type-State | Typed traversals with compile-time tracking | 3-4 days | Medium |
| 3. Vertex Traversal Methods | `v.out()`, `v.in_()` from vertex objects | 2-3 days | Low |
| 4. Integration & Migration | COW traversals, Rhai, breaking change rollout | 3-4 days | Medium |
| **Total** | | **11-15 days** | Low-Medium |

## Terminal Methods by Marker Type

All terminal methods are implemented for each marker type with appropriate return types:

| Terminal Method | `Vertex` Marker Returns | `Edge` Marker Returns | `Scalar` Marker Returns |
|-----------------|-------------------------|----------------------|------------------------|
| `next()` | `Option<GraphVertex>` | `Option<GraphEdge>` | `Option<Value>` |
| `to_list()` | `Vec<GraphVertex>` | `Vec<GraphEdge>` | `Vec<Value>` |
| `one()` | `Result<GraphVertex, TraversalError>` | `Result<GraphEdge, TraversalError>` | `Result<Value, TraversalError>` |
| `to_set()` | `HashSet<GraphVertex>` | `HashSet<GraphEdge>` | `HashSet<Value>` |
| `has_next()` | `bool` | `bool` | `bool` |
| `count()` | `u64` | `u64` | `u64` |
| `take(n)` | `Vec<GraphVertex>` | `Vec<GraphEdge>` | `Vec<Value>` |
| `iter()` | `impl Iterator<Item = GraphVertex>` | `impl Iterator<Item = GraphEdge>` | `impl Iterator<Item = Value>` |
| `sum()` | N/A | N/A | `Value` |

### Escape Hatches (Available on All Marker Types)

| Method | Returns | Purpose |
|--------|---------|---------|
| `next_value()` | `Option<Value>` | Get raw Value regardless of marker |
| `to_value_list()` | `Vec<Value>` | Get raw Values regardless of marker |

## Files to Create

- `src/graph_elements.rs` - `GraphVertex`, `GraphEdge`, `GraphVertexTraversal`
- `src/traversal/markers.rs` - `Vertex`, `Edge`, `Scalar`, `OutputMarker`
- `src/traversal/typed.rs` - `TypedTraversal`, `TypedTraversalSource`

## Files to Modify

- `src/lib.rs` - Add new module exports
- `src/traversal/mod.rs` - Add marker exports
- `src/traversal/source.rs` - Add type-specific terminal methods
- `src/storage/cow.rs` - Update `CowTraversalSource`, `CowBoundTraversal`
- `src/rhai/types.rs` - Register new types

## Test Strategy

1. **Unit tests**: Each phase includes unit tests for new functionality
2. **Integration tests**: End-to-end traversal scenarios with new API
3. **Migration tests**: Verify escape hatches work for existing code patterns
4. **Property tests**: `proptest` for roundtrip serialization of `GraphVertex`

## Open Questions

1. **`Arc<Graph>` acquisition**: How to get `Arc<Graph>` from a borrowed `&Graph`?
   - Option A: Require users to pass `Arc<Graph>` to terminal methods
   - Option B: Create `ArcGraph` wrapper that owns the Arc
   - Option C: Store `Weak<Graph>` in traversal infrastructure

2. **Anonymous traversals**: How do marker types work with `__.out()`?
   - Proposal: Anonymous traversals use `Any` marker resolved at append time

3. **Snapshot-based access**: Should `GraphVertex` take a snapshot or use current state?
   - Spec says use current state (not snapshot-isolated)
   - Consider adding `GraphVertex::at_snapshot()` for snapshot isolation
