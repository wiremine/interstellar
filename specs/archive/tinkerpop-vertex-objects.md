# TinkerPop-Style Vertex Objects Spec

## Overview

This spec describes a fundamental architecture change to make Interstellar's vertex/edge representation match TinkerPop's semantics, where vertices and edges are "live" objects that can spawn traversals.

### Goal

Enable this usage pattern:

```rust
// Get a vertex and continue traversing from it
let v = g.v().has_value("name", "Alice").first().unwrap();
let friends = v.out("knows").to_list();

// Access vertex properties directly
println!("Label: {}", v.label());
println!("Name: {}", v.property("name").unwrap());

// Mutate via the vertex object
v.property_set("age", 31);

// Chain naturally
let fof = v.out("knows").out("knows").to_list();
```

### Current State

```rust
pub enum Value {
    Vertex(VertexId),  // Just an ID - cannot traverse or access properties
    Edge(EdgeId),      // Just an ID
    // ...
}
```

After `g.v().first()`, you get `Value::Vertex(VertexId(42))` - a bare ID with no graph reference, no label, no properties, and no ability to traverse.

### Target State

```rust
pub enum Value {
    Vertex(GraphVertex),  // Live object with graph reference
    Edge(GraphEdge),      // Live object with graph reference
    // ...
}
```

After `g.v().first()`, you get a `GraphVertex` that:
- Holds a reference to the graph
- Can access `.id()`, `.label()`, `.property(key)`
- Can spawn traversals: `.out()`, `.in_()`, `.both()`
- Can mutate: `.property_set(key, value)`

---

## Part 1: Core Types

### 1.1 GraphVertex

```rust
// src/graph_elements.rs (new file)

use std::sync::Arc;
use std::collections::HashMap;
use crate::value::{VertexId, EdgeId, Value};
use crate::Graph;

/// A vertex reference with access to the graph.
///
/// `GraphVertex` provides TinkerPop-style vertex semantics where
/// a vertex object can access its properties and spawn traversals.
///
/// # Thread Safety
///
/// `GraphVertex` is `Clone`, `Send`, and `Sync`. Multiple vertices
/// can reference the same graph concurrently.
///
/// # Example
///
/// ```ignore
/// let v = g.v().has_value("name", "Alice").first().unwrap();
/// 
/// // Access properties
/// println!("ID: {:?}", v.id());
/// println!("Label: {}", v.label());
/// println!("Age: {:?}", v.property("age"));
///
/// // Traverse
/// let friends = v.out("knows").to_list();
/// ```
#[derive(Clone)]
pub struct GraphVertex {
    /// The vertex ID
    id: VertexId,
    /// Reference to the graph (for property access and traversal)
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
    ///
    /// Returns `None` if the vertex no longer exists in the graph.
    pub fn label(&self) -> Option<String> {
        self.graph.read(|storage, _| {
            storage.get_vertex(self.id).map(|v| v.label)
        })
    }

    /// Get a property value.
    ///
    /// Returns `None` if the property doesn't exist or the vertex
    /// no longer exists in the graph.
    pub fn property(&self, key: &str) -> Option<Value> {
        self.graph.read(|storage, _| {
            storage.get_vertex(self.id)
                .and_then(|v| v.properties.get(key).cloned())
        })
    }

    /// Get all properties as a map.
    pub fn properties(&self) -> HashMap<String, Value> {
        self.graph.read(|storage, _| {
            storage.get_vertex(self.id)
                .map(|v| v.properties)
                .unwrap_or_default()
        })
    }

    /// Check if the vertex still exists in the graph.
    pub fn exists(&self) -> bool {
        self.graph.read(|storage, _| {
            storage.get_vertex(self.id).is_some()
        })
    }

    /// Set a property value.
    ///
    /// Returns `Ok(())` on success, or an error if the vertex doesn't exist.
    pub fn property_set(&self, key: &str, value: impl Into<Value>) -> Result<(), crate::error::StorageError> {
        self.graph.mutate(|storage| {
            storage.set_vertex_property(self.id, key, value.into())
        })
    }

    /// Get the graph reference.
    pub fn graph(&self) -> &Arc<Graph> {
        &self.graph
    }

    // -------------------------------------------------------------------------
    // Traversal Methods
    // -------------------------------------------------------------------------

    /// Traverse to outgoing adjacent vertices.
    pub fn out(&self) -> GraphVertexTraversal {
        GraphVertexTraversal::new(self.graph.clone(), self.id).out()
    }

    /// Traverse to outgoing adjacent vertices via edges with given label.
    pub fn out_label(&self, label: &str) -> GraphVertexTraversal {
        GraphVertexTraversal::new(self.graph.clone(), self.id).out_label(label)
    }

    /// Traverse to incoming adjacent vertices.
    pub fn in_(&self) -> GraphVertexTraversal {
        GraphVertexTraversal::new(self.graph.clone(), self.id).in_()
    }

    /// Traverse to incoming adjacent vertices via edges with given label.
    pub fn in_label(&self, label: &str) -> GraphVertexTraversal {
        GraphVertexTraversal::new(self.graph.clone(), self.id).in_label(label)
    }

    /// Traverse to adjacent vertices in both directions.
    pub fn both(&self) -> GraphVertexTraversal {
        GraphVertexTraversal::new(self.graph.clone(), self.id).both()
    }

    /// Traverse to outgoing edges.
    pub fn out_e(&self) -> GraphEdgeTraversal {
        GraphEdgeTraversal::from_vertex(self.graph.clone(), self.id).out_e()
    }

    /// Traverse to incoming edges.
    pub fn in_e(&self) -> GraphEdgeTraversal {
        GraphEdgeTraversal::from_vertex(self.graph.clone(), self.id).in_e()
    }

    /// Traverse to all incident edges.
    pub fn both_e(&self) -> GraphEdgeTraversal {
        GraphEdgeTraversal::from_vertex(self.graph.clone(), self.id).both_e()
    }

    /// Add an outgoing edge to another vertex.
    pub fn add_edge(&self, label: &str, to: &GraphVertex) -> Result<GraphEdge, crate::error::StorageError> {
        self.add_edge_to_id(label, to.id)
    }

    /// Add an outgoing edge to a vertex by ID.
    pub fn add_edge_to_id(&self, label: &str, to: VertexId) -> Result<GraphEdge, crate::error::StorageError> {
        let edge_id = self.graph.mutate(|storage| {
            storage.add_edge(self.id, to, label, HashMap::new())
        })?;
        Ok(GraphEdge::new(edge_id, self.graph.clone()))
    }

    /// Remove this vertex from the graph.
    ///
    /// Also removes all incident edges.
    pub fn remove(&self) -> Result<(), crate::error::StorageError> {
        self.graph.mutate(|storage| {
            storage.remove_vertex(self.id)
        })
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
        // Two GraphVertex are equal if they reference the same vertex ID
        // We don't compare graph references (same ID in different graphs would be "equal")
        self.id == other.id
    }
}

impl Eq for GraphVertex {}

impl std::hash::Hash for GraphVertex {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
```

### 1.2 GraphEdge

```rust
// Continues in src/graph_elements.rs

/// An edge reference with access to the graph.
///
/// `GraphEdge` provides TinkerPop-style edge semantics where
/// an edge object can access its properties and endpoint vertices.
#[derive(Clone)]
pub struct GraphEdge {
    /// The edge ID
    id: EdgeId,
    /// Reference to the graph
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
        self.graph.read(|storage, _| {
            storage.get_edge(self.id).map(|e| e.label)
        })
    }

    /// Get the source (outgoing) vertex.
    pub fn out_v(&self) -> Option<GraphVertex> {
        self.graph.read(|storage, _| {
            storage.get_edge(self.id).map(|e| e.src)
        }).map(|id| GraphVertex::new(id, self.graph.clone()))
    }

    /// Get the destination (incoming) vertex.
    pub fn in_v(&self) -> Option<GraphVertex> {
        self.graph.read(|storage, _| {
            storage.get_edge(self.id).map(|e| e.dst)
        }).map(|id| GraphVertex::new(id, self.graph.clone()))
    }

    /// Get both endpoint vertices as (out, in) tuple.
    pub fn both_v(&self) -> Option<(GraphVertex, GraphVertex)> {
        self.graph.read(|storage, _| {
            storage.get_edge(self.id).map(|e| (e.src, e.dst))
        }).map(|(src, dst)| {
            (
                GraphVertex::new(src, self.graph.clone()),
                GraphVertex::new(dst, self.graph.clone()),
            )
        })
    }

    /// Get a property value.
    pub fn property(&self, key: &str) -> Option<Value> {
        self.graph.read(|storage, _| {
            storage.get_edge(self.id)
                .and_then(|e| e.properties.get(key).cloned())
        })
    }

    /// Get all properties.
    pub fn properties(&self) -> HashMap<String, Value> {
        self.graph.read(|storage, _| {
            storage.get_edge(self.id)
                .map(|e| e.properties)
                .unwrap_or_default()
        })
    }

    /// Set a property value.
    pub fn property_set(&self, key: &str, value: impl Into<Value>) -> Result<(), crate::error::StorageError> {
        self.graph.mutate(|storage| {
            storage.set_edge_property(self.id, key, value.into())
        })
    }

    /// Check if the edge still exists.
    pub fn exists(&self) -> bool {
        self.graph.read(|storage, _| {
            storage.get_edge(self.id).is_some()
        })
    }

    /// Get the graph reference.
    pub fn graph(&self) -> &Arc<Graph> {
        &self.graph
    }

    /// Remove this edge from the graph.
    pub fn remove(&self) -> Result<(), crate::error::StorageError> {
        self.graph.mutate(|storage| {
            storage.remove_edge(self.id)
        })
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

---

## Part 2: Type-State Pattern for TinkerPop-Faithful Returns

### 2.1 The Problem

The current `Value` enum stores only IDs:

```rust
pub enum Value {
    Vertex(VertexId),
    Edge(EdgeId),
    // ...
}
```

After calling `g.v().next()`, you get `Value::Vertex(VertexId(42))` - just an ID.

### 2.2 TinkerPop Semantics

In TinkerPop/Gremlin:
- `g.V().next()` returns `Vertex` directly
- `g.E().next()` returns `Edge` directly
- `g.V().values("name").next()` returns the property value directly

The return type is determined by the **traversal's type parameter**, not by the method name:

```java
GraphTraversal<Vertex, Vertex> t1 = g.V();
Vertex v = t1.next();  // Returns Vertex

GraphTraversal<Vertex, Edge> t2 = g.V().outE();
Edge e = t2.next();    // Returns Edge

GraphTraversal<Vertex, Object> t3 = g.V().values("name");
Object val = t3.next(); // Returns the value
```

### 2.3 Design Decision: Marker Types for Type-State Tracking

We use **marker types** in the `Out` parameter to track what type of element the traversal produces:

```rust
// src/traversal/markers.rs (NEW FILE)

use std::marker::PhantomData;

/// Marker indicating traversal produces vertices.
/// 
/// Terminal methods on traversals with this marker return `GraphVertex`.
#[derive(Clone, Copy, Debug)]
pub struct Vertex;

/// Marker indicating traversal produces edges.
///
/// Terminal methods on traversals with this marker return `GraphEdge`.
#[derive(Clone, Copy, Debug)]
pub struct Edge;

/// Marker indicating traversal produces arbitrary values.
///
/// Terminal methods on traversals with this marker return `Value`.
/// This is the fallback for mixed-type traversals.
#[derive(Clone, Copy, Debug)]
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

### 2.4 Step Type Transformations

Each step knows what output type it produces:

| Step Category | Input Marker | Output Marker |
|--------------|--------------|---------------|
| `g.v()`, `g.v_ids()` | - | `Vertex` |
| `g.e()`, `g.e_ids()` | - | `Edge` |
| `out()`, `in_()`, `both()` | `Vertex` | `Vertex` |
| `out_e()`, `in_e()`, `both_e()` | `Vertex` | `Edge` |
| `out_v()`, `in_v()`, `both_v()` | `Edge` | `Vertex` |
| `has_label()`, `has_value()`, `filter()` | `T` | `T` (preserves) |
| `values()`, `id()`, `label()` | any | `Scalar` |
| `properties()`, `value_map()` | any | `Scalar` |
| `count()`, `sum()`, `mean()` | any | `Scalar` |
| `fold()`, `unfold()` | any | `Scalar` |
| `union()`, `coalesce()` | any | `Scalar` (mixed) |
| `add_v()` | any | `Vertex` |
| `add_e()` | any | `Edge` |

### 2.5 Updated Traversal Types

```rust
// src/traversal/pipeline.rs - UPDATED

/// Main traversal type with output marker.
///
/// # Type Parameters
///
/// - `In`: Input type (phantom)
/// - `Out`: Output marker type - one of `Vertex`, `Edge`, or `Scalar`
pub struct Traversal<In, Out: OutputMarker> {
    pub(crate) steps: Vec<Box<dyn AnyStep>>,
    pub(crate) source: Option<TraversalSource>,
    pub(crate) _phantom: PhantomData<fn(In) -> Out>,
}

// src/traversal/source.rs - UPDATED

/// A traversal bound to a graph.
pub struct BoundTraversal<'g, In, Out: OutputMarker> {
    storage: &'g dyn GraphStorage,
    interner: &'g StringInterner,
    graph: Arc<Graph>,
    traversal: Traversal<In, Out>,
    track_paths: bool,
}
```

### 2.6 Terminal Methods by Output Marker

```rust
// Terminal methods for Vertex traversals
impl<'g, In> BoundTraversal<'g, In, Vertex> {
    /// Execute and return the first vertex.
    /// 
    /// TinkerPop equivalent: `g.V().next()`
    pub fn next(self) -> Option<GraphVertex> {
        let graph = self.graph.clone();
        self.execute()
            .find_map(|t| match t.value {
                Value::Vertex(id) => Some(GraphVertex::new(id, graph.clone())),
                _ => None,
            })
    }
    
    /// Execute and return all vertices.
    ///
    /// TinkerPop equivalent: `g.V().toList()`
    pub fn to_list(self) -> Vec<GraphVertex> {
        let graph = self.graph.clone();
        self.execute()
            .filter_map(|t| match t.value {
                Value::Vertex(id) => Some(GraphVertex::new(id, graph.clone())),
                _ => None,
            })
            .collect()
    }
}

// Terminal methods for Edge traversals  
impl<'g, In> BoundTraversal<'g, In, Edge> {
    /// Execute and return the first edge.
    pub fn next(self) -> Option<GraphEdge> {
        let graph = self.graph.clone();
        self.execute()
            .find_map(|t| match t.value {
                Value::Edge(id) => Some(GraphEdge::new(id, graph.clone())),
                _ => None,
            })
    }
    
    /// Execute and return all edges.
    pub fn to_list(self) -> Vec<GraphEdge> {
        let graph = self.graph.clone();
        self.execute()
            .filter_map(|t| match t.value {
                Value::Edge(id) => Some(GraphEdge::new(id, graph.clone())),
                _ => None,
            })
            .collect()
    }
}

// Terminal methods for Scalar traversals
impl<'g, In> BoundTraversal<'g, In, Scalar> {
    /// Execute and return the first value.
    pub fn next(self) -> Option<Value> {
        self.execute().next().map(|t| t.value)
    }
    
    /// Execute and return all values.
    pub fn to_list(self) -> Vec<Value> {
        self.execute().map(|t| t.value).collect()
    }
}
```

### 2.7 Step Method Signatures

Steps transform the output marker:

```rust
impl<'g, In> BoundTraversal<'g, In, Vertex> {
    // Navigation: Vertex -> Vertex
    pub fn out(self) -> BoundTraversal<'g, In, Vertex> { ... }
    pub fn in_(self) -> BoundTraversal<'g, In, Vertex> { ... }
    pub fn both(self) -> BoundTraversal<'g, In, Vertex> { ... }
    
    // Navigation: Vertex -> Edge
    pub fn out_e(self) -> BoundTraversal<'g, In, Edge> { ... }
    pub fn in_e(self) -> BoundTraversal<'g, In, Edge> { ... }
    pub fn both_e(self) -> BoundTraversal<'g, In, Edge> { ... }
    
    // Transform: Vertex -> Scalar
    pub fn values(self, key: &str) -> BoundTraversal<'g, In, Scalar> { ... }
    pub fn id(self) -> BoundTraversal<'g, In, Scalar> { ... }
    pub fn label(self) -> BoundTraversal<'g, In, Scalar> { ... }
    
    // Filter: preserves Vertex
    pub fn has_label(self, label: &str) -> BoundTraversal<'g, In, Vertex> { ... }
    pub fn has_value(self, key: &str, val: impl Into<Value>) -> BoundTraversal<'g, In, Vertex> { ... }
    pub fn filter<F>(self, f: F) -> BoundTraversal<'g, In, Vertex> { ... }
}

impl<'g, In> BoundTraversal<'g, In, Edge> {
    // Navigation: Edge -> Vertex
    pub fn out_v(self) -> BoundTraversal<'g, In, Vertex> { ... }
    pub fn in_v(self) -> BoundTraversal<'g, In, Vertex> { ... }
    pub fn both_v(self) -> BoundTraversal<'g, In, Vertex> { ... }
    
    // Transform: Edge -> Scalar
    pub fn values(self, key: &str) -> BoundTraversal<'g, In, Scalar> { ... }
    pub fn id(self) -> BoundTraversal<'g, In, Scalar> { ... }
    pub fn label(self) -> BoundTraversal<'g, In, Scalar> { ... }
    
    // Filter: preserves Edge
    pub fn has_label(self, label: &str) -> BoundTraversal<'g, In, Edge> { ... }
}
```

### 2.8 Usage Examples - TinkerPop Faithful

```rust
// g.V().next() returns Vertex (GraphVertex in Rust)
let v: Option<GraphVertex> = g.v().next();

// g.V().out().toList() returns List<Vertex>
let friends: Vec<GraphVertex> = g.v().out().to_list();

// g.V().outE().next() returns Edge (GraphEdge in Rust)
let e: Option<GraphEdge> = g.v().out_e().next();

// g.V().values("name").next() returns the value
let name: Option<Value> = g.v().values("name").next();

// Chaining works naturally with type transformations
let v = g.v().has_value("name", "Alice").next().unwrap();
//     ^^^                               ^^^^
//     Vertex marker preserved           Returns GraphVertex

// Type transforms through the chain
let ages = g.v()          // Vertex
    .has_label("person")  // Vertex (filter preserves)
    .values("age")        // Scalar (transform)
    .to_list();           // Vec<Value>
```

### 2.9 Value Enum - UNCHANGED

```rust
// src/value.rs - NO CHANGES
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<Value>),
    Map(HashMap<String, Value>),
    Vertex(VertexId),   // Stays as ID-only
    Edge(EdgeId),       // Stays as ID-only
}
```

The `Value` enum remains lightweight for:
- Internal traversal execution
- Serialization (JSON, binary)
- Property values
- Storage layer

---

## Part 3: Traverser - No Changes Required

The `Traverser` struct continues to hold `Value` internally. This keeps traversal execution efficient - the marker types are compile-time only and don't affect runtime representation.

```rust
// src/traversal/traverser.rs - NO CHANGES

pub struct Traverser {
    pub value: Value,           // Still uses lightweight Value
    pub path: Path,
    pub loops: usize,
    pub sack: Option<Box<dyn CloneSack>>,
    pub bulk: u64,
}
```

The marker type system is purely for compile-time type safety. At runtime, all values flow as `Value` enum variants. The conversion to `GraphVertex`/`GraphEdge` happens only at terminal step boundaries.

---

## Part 4: GraphTraversalSource and BoundTraversal Changes

### 4.1 GraphTraversalSource Returns Typed Traversals

```rust
// src/traversal/source.rs

pub struct GraphTraversalSource<'g> {
    storage: &'g dyn GraphStorage,
    interner: &'g StringInterner,
    graph: Arc<Graph>,  // NEW: for creating GraphVertex/GraphEdge
}

impl<'g> GraphTraversalSource<'g> {
    /// Start traversal from all vertices.
    /// 
    /// Returns a vertex-typed traversal where `next()` returns `GraphVertex`.
    pub fn v(&self) -> BoundTraversal<'g, (), Vertex> {
        BoundTraversal::new(
            self.storage,
            self.interner,
            self.graph.clone(),
            Traversal::with_source(TraversalSource::AllVertices),
        )
    }

    /// Start traversal from all edges.
    ///
    /// Returns an edge-typed traversal where `next()` returns `GraphEdge`.
    pub fn e(&self) -> BoundTraversal<'g, (), Edge> {
        BoundTraversal::new(
            self.storage,
            self.interner,
            self.graph.clone(),
            Traversal::with_source(TraversalSource::AllEdges),
        )
    }

    /// Inject arbitrary values.
    ///
    /// Returns a scalar-typed traversal where `next()` returns `Value`.
    pub fn inject<T, I>(&self, values: I) -> BoundTraversal<'g, (), Scalar>
    where
        I: IntoIterator<Item = T>,
        T: Into<Value>,
    {
        let values: Vec<Value> = values.into_iter().map(Into::into).collect();
        BoundTraversal::new(
            self.storage,
            self.interner,
            self.graph.clone(),
            Traversal::with_source(TraversalSource::Inject(values)),
        )
    }
}
```

### 4.2 BoundTraversal with Output Marker

```rust
// src/traversal/source.rs

use crate::traversal::markers::{Vertex, Edge, Scalar, OutputMarker};

pub struct BoundTraversal<'g, In, Out: OutputMarker> {
    storage: &'g dyn GraphStorage,
    interner: &'g StringInterner,
    graph: Arc<Graph>,
    traversal: Traversal<In, Out>,
    track_paths: bool,
}

impl<'g, In, Out: OutputMarker> BoundTraversal<'g, In, Out> {
    /// Add a step that preserves the output type.
    pub fn add_step_same(self, step: impl AnyStep + 'static) -> BoundTraversal<'g, In, Out> {
        BoundTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph,
            traversal: self.traversal.add_step(step),
            track_paths: self.track_paths,
        }
    }
    
    /// Add a step that changes the output type.
    pub fn add_step_to<NewOut: OutputMarker>(
        self, 
        step: impl AnyStep + 'static
    ) -> BoundTraversal<'g, In, NewOut> {
        BoundTraversal {
            storage: self.storage,
            interner: self.interner,
            graph: self.graph,
            traversal: self.traversal.add_step_cast(),
            track_paths: self.track_paths,
        }
    }
}
```

### 4.3 Backward Compatibility: `to_value_list()` Escape Hatch

For cases where users need the old behavior or are working with mixed-type traversals:

```rust
impl<'g, In, Out: OutputMarker> BoundTraversal<'g, In, Out> {
    /// Execute and return all results as raw Values.
    ///
    /// This bypasses the typed terminal methods and returns `Value` directly.
    /// Useful for debugging or when you need to handle mixed types.
    pub fn to_value_list(self) -> Vec<Value> {
        self.execute().map(|t| t.value).collect()
    }
    
    /// Execute and return the first result as raw Value.
    pub fn next_value(self) -> Option<Value> {
        self.execute().next().map(|t| t.value)
    }
}
```

### 4.4 Complete API Flow Example

```rust
// The type flows through the entire traversal chain

let g = graph.gremlin();  // GraphTraversalSource

// g.v() returns BoundTraversal<'_, (), Vertex>
let t1 = g.v();

// .has_label() preserves Vertex marker
let t2 = t1.has_label("person");  // BoundTraversal<'_, (), Vertex>

// .out() preserves Vertex marker  
let t3 = t2.out();  // BoundTraversal<'_, (), Vertex>

// .next() on Vertex traversal returns GraphVertex
let v: Option<GraphVertex> = t3.next();

// .values() transforms to Scalar marker
let t4 = g.v().values("name");  // BoundTraversal<'_, (), Scalar>

// .next() on Scalar traversal returns Value
let name: Option<Value> = t4.next();

// .out_e() transforms Vertex to Edge
let t5 = g.v().out_e();  // BoundTraversal<'_, (), Edge>

// .next() on Edge traversal returns GraphEdge
let e: Option<GraphEdge> = t5.next();
```

---

## Part 5: GraphVertex Traversal Methods

### 5.1 GraphVertexTraversal

When you call `v.out()` on a `GraphVertex`, it returns a `GraphVertexTraversal` - a fluent builder that collects traversal steps and executes when a terminal is called:

```rust
// src/graph_elements.rs

/// A traversal starting from a specific vertex.
///
/// This type enables TinkerPop-style traversal from a vertex object:
/// ```ignore
/// let friends = v.out("knows").has_label("person").to_list();
/// ```
pub struct GraphVertexTraversal {
    graph: Arc<Graph>,
    start_id: VertexId,
    steps: Vec<TraversalStep>,
}

enum TraversalStep {
    Out(Option<String>),        // out() or out("label")
    In(Option<String>),         // in_() or in_("label")
    Both(Option<String>),       // both() or both("label")
    OutE(Option<String>),       // outE() or outE("label")
    InE(Option<String>),        // inE() or inE("label")
    BothE(Option<String>),      // bothE() or bothE("label")
    HasLabel(String),           // hasLabel("person")
    HasValue(String, Value),    // has("name", "Alice")
    // ... other filter/transform steps
}

impl GraphVertexTraversal {
    pub(crate) fn new(graph: Arc<Graph>, start_id: VertexId) -> Self {
        Self {
            graph,
            start_id,
            steps: Vec::new(),
        }
    }

    // -------------------------------------------------------------------------
    // Navigation Steps
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Filter Steps
    // -------------------------------------------------------------------------

    pub fn has_label(mut self, label: &str) -> Self {
        self.steps.push(TraversalStep::HasLabel(label.to_string()));
        self
    }

    pub fn has_value(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.steps.push(TraversalStep::HasValue(key.to_string(), value.into()));
        self
    }

    // -------------------------------------------------------------------------
    // Terminal Steps
    // -------------------------------------------------------------------------

    /// Execute and return all results.
    pub fn to_list(self) -> Vec<GraphVertex> {
        // Build a BoundTraversal from g.v_id(start_id) and apply steps
        let g = self.graph.gremlin();
        let mut traversal = g.v_id(self.start_id);

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
                _ => traversal,
            };
        }

        // Execute and convert to GraphVertex
        traversal.to_list()
            .into_iter()
            .filter_map(|gv| gv.as_vertex().cloned())
            .collect()
    }

    /// Execute and return the first result.
    pub fn first(self) -> Option<GraphVertex> {
        self.to_list().into_iter().next()
    }

    /// Execute and return the count.
    pub fn count(self) -> usize {
        self.to_list().len()
    }

    /// Check if any results exist.
    pub fn exists(self) -> bool {
        self.first().is_some()
    }
}
```

### 5.2 Alternative: Delegate to Existing Traversal

Instead of duplicating traversal logic, `GraphVertexTraversal` can simply wrap and delegate to the existing `BoundTraversal`:

```rust
impl GraphVertexTraversal {
    /// Convert to a standard BoundTraversal for full API access.
    pub fn as_traversal(&self) -> BoundTraversal<'_, (), Value> {
        self.graph.gremlin().v_id(self.start_id)
    }
}
```

This keeps the implementation DRY - `GraphVertexTraversal` is a thin wrapper that provides a vertex-centric entry point.

---

## Part 6: Migration Strategy

### 6.1 Phased Approach

#### Phase 1: Add Core Types (Non-Breaking)
- Add `GraphVertex`, `GraphEdge` types in new `src/graph_elements.rs`
- Add `Arc<Graph>` to `GraphTraversalSource` and `BoundTraversal`
- Add type-specific terminal methods: `next_vertex()`, `to_vertex_list()`, `next_edge()`, `to_edge_list()`
- Existing `to_list()` / `next()` remain unchanged

**Effort**: 2-3 days
**Risk**: Low - purely additive, no breaking changes

#### Phase 2: Add Traversal Methods to GraphVertex/GraphEdge
- Implement `GraphVertex::out()`, `in_()`, `both()`, etc.
- Implement `GraphVertexTraversal` for fluent chaining
- Add mutation methods: `property_set()`, `remove()`, `add_edge()`

**Effort**: 2-3 days
**Risk**: Low - new functionality only

#### Phase 3: Update COW Storage Layer
- Update `CowBoundTraversal` to carry `Arc<Graph>`
- Add type-specific terminal methods to COW traversals
- Update tests in COW module

**Effort**: 2-3 days
**Risk**: Medium - touches mutation execution

#### Phase 4: Update Rhai/GQL Integration
- Add Rhai type registration for `GraphVertex`/`GraphEdge`
- Update GQL query results to optionally return rich objects
- Add Rhai methods for vertex/edge property access and traversal

**Effort**: 2-3 days
**Risk**: Medium - scripting API additions

### 6.2 Estimated Total Effort

| Phase | Effort | Risk |
|-------|--------|------|
| Phase 1: Core Types + Terminal Methods | 2-3 days | Low |
| Phase 2: Traversal Methods | 2-3 days | Low |
| Phase 3: COW Storage | 2-3 days | Medium |
| Phase 4: Rhai/GQL | 2-3 days | Medium |
| **Total** | **8-12 days** | Low-Medium |

---

## Part 7: Impact Analysis

### 7.1 Files Requiring Changes

| Category | Files | Scope |
|----------|-------|-------|
| **New Files** | `src/graph_elements.rs` | Core types |
| **Core Changes** | `src/traversal/source.rs` | Add `Arc<Graph>`, update terminals |
| | `src/traversal/context.rs` | Add graph reference |
| | `src/graph.rs` | Update `gremlin()` method |
| **Storage** | `src/storage/cow.rs` | Update `CowBoundTraversal` |
| | `src/storage/cow_mmap.rs` | Update `CowMmapBoundTraversal` |
| **Rhai** | `src/rhai/traversal.rs` | Update terminal steps |
| | `src/rhai/types.rs` | Add `GraphVertex` conversions |
| **GQL** | `src/gql/compiler/mod.rs` | Update result types |
| **Tests** | Multiple test modules | Update assertions |

### 7.2 Breaking Changes

**This is a breaking change** due to the marker type system:

1. **`g.v().next()` return type changes**: `Option<Value>` → `Option<GraphVertex>`
2. **`g.e().next()` return type changes**: `Option<Value>` → `Option<GraphEdge>`
3. **Step method signatures change**: Methods now carry marker types through the chain

### 7.3 Migration Strategy

For code that needs the old `Value` returns:

```rust
// Old code (will break)
let v: Option<Value> = g.v().next();

// Migration option 1: Use next_value() escape hatch
let v: Option<Value> = g.v().next_value();

// Migration option 2: Use to_value() on GraphVertex
let v: Option<Value> = g.v().next().map(|gv| gv.to_value());

// Migration option 3 (recommended): Embrace the new API
let v: Option<GraphVertex> = g.v().next();
// Now you can do: v.unwrap().label(), v.unwrap().out(), etc.
```

### 7.4 API Ergonomics

**Before (current):**
```rust
let v = g.v().next().unwrap();
// v is Value::Vertex(VertexId) - can't do much with it
let id = v.as_vertex_id().unwrap();
let friends = g.v_ids([id]).out().to_list();
```

**After (TinkerPop-faithful):**
```rust
let v = g.v().next().unwrap();
// v is GraphVertex - fully functional, just like TinkerPop!
let friends = v.out("knows").to_list();
println!("Label: {}", v.label().unwrap());
v.property_set("visited", true).unwrap();
```

---

## Part 8: Design Decisions & Open Questions

### Resolved Decisions

1. **Return type approach**: Marker types with type-state tracking
   - `g.v()` returns `BoundTraversal<..., Vertex>` where `next()` → `GraphVertex`
   - `g.v().values("x")` returns `BoundTraversal<..., Scalar>` where `next()` → `Value`
   - Fully TinkerPop-faithful

2. **Equality semantics**: ID-only comparison
   - `GraphVertex` equality compares vertex IDs, not graph references
   - Same vertex ID = equal, even if from different `Arc<Graph>` clones

3. **Stale reference handling**: Graceful degradation
   - Methods return `Option<T>` or `None` when vertex no longer exists
   - `exists()` method to check validity

4. **Thread safety**: Rely on `Graph`'s internal locking
   - `Arc<Graph>` enables shared access
   - `Graph::mutate()` provides interior mutability

5. **Serialization**: Use `to_value()` method
   - `GraphVertex::to_value() -> Value` for serialization
   - Lightweight `Value::Vertex(id)` can be serialized normally

### Open Questions

1. **Anonymous traversals**: How do marker types work with `__.out()`?
   - Proposal: Anonymous traversals use a special `Any` marker that gets resolved when appended

2. **Branch steps**: What marker does `union([__.out(), __.out_e()])` produce?
   - Proposal: `Scalar` (fallback for mixed types)

3. **Rhai scripting**: How should `GraphVertex` appear in Rhai scripts?
   - Proposal: Register as custom type with methods `.id()`, `.label()`, `.out()`, etc.

4. **GQL integration**: Should GQL queries return `GraphVertex` or `Value`?
   - Proposal: Return `GraphVertex`/`GraphEdge` when appropriate, `Value` for scalars

---

## Part 9: Test Cases

```rust
#[test]
fn tinkerpop_faithful_vertex_retrieval() {
    let graph = test_graph();  // Creates test graph with Alice -> Bob -> Charlie
    let g = graph.gremlin();

    // g.V().next() returns GraphVertex directly - just like TinkerPop!
    let v: Option<GraphVertex> = g.v().next();
    assert!(v.is_some());

    // g.V().has("name", "Alice").next() also returns GraphVertex
    let alice: GraphVertex = g.v().has_value("name", "Alice").next().unwrap();
    
    // Property access works directly on GraphVertex
    assert_eq!(alice.label(), Some("person".to_string()));
    assert_eq!(alice.property("name"), Some(Value::String("Alice".into())));
}

#[test]
fn tinkerpop_faithful_edge_retrieval() {
    let graph = test_graph();
    let g = graph.gremlin();

    // g.E().next() returns GraphEdge directly
    let e: Option<GraphEdge> = g.e().next();
    assert!(e.is_some());

    // g.V().outE().next() also returns GraphEdge
    let edge: GraphEdge = g.v().out_e().next().unwrap();
    assert!(edge.label().is_some());
    assert!(edge.out_v().is_some());
    assert!(edge.in_v().is_some());
}

#[test]
fn tinkerpop_faithful_values_retrieval() {
    let graph = test_graph();
    let g = graph.gremlin();

    // g.V().values("name").next() returns Value (Scalar marker)
    let name: Option<Value> = g.v().values("name").next();
    assert!(matches!(name, Some(Value::String(_))));

    // Type transforms through the chain
    let ages: Vec<Value> = g.v()      // Vertex marker
        .has_label("person")          // Vertex (filter preserves)
        .values("age")                // Scalar (transform)
        .to_list();                   // Vec<Value>
}

#[test]
fn type_marker_flow_through_navigation() {
    let graph = test_graph();
    let g = graph.gremlin();

    // out() preserves Vertex marker
    let friends: Vec<GraphVertex> = g.v()
        .has_value("name", "Alice")
        .out()                        // Vertex -> Vertex
        .to_list();
    assert!(friends.len() > 0);

    // out_e() transforms Vertex to Edge marker
    let edges: Vec<GraphEdge> = g.v()
        .has_value("name", "Alice")
        .out_e()                      // Vertex -> Edge
        .to_list();
    
    // in_v() transforms Edge back to Vertex marker
    let targets: Vec<GraphVertex> = g.v()
        .out_e()                      // Vertex -> Edge
        .in_v()                       // Edge -> Vertex
        .to_list();
}

#[test]
fn vertex_object_traversal_methods() {
    let graph = test_graph();
    let g = graph.gremlin();

    let alice = g.v().has_value("name", "Alice").next().unwrap();
    
    // GraphVertex has traversal methods
    let friends = alice.out("knows").to_list();
    assert_eq!(friends.len(), 1);
    assert_eq!(friends[0].property("name"), Some(Value::String("Bob".into())));

    // Chained traversal from vertex object
    let fof = alice.out("knows").out("knows").to_list();
    assert_eq!(fof.len(), 1);
    assert_eq!(fof[0].property("name"), Some(Value::String("Charlie".into())));
}

#[test]
fn vertex_mutation_via_object() {
    let graph = test_graph();
    let g = graph.gremlin();

    let alice = g.v().has_value("name", "Alice").next().unwrap();

    // Set property via vertex object
    alice.property_set("age", 31i64).unwrap();

    // Verify mutation persisted (values() returns Scalar, so next() -> Value)
    let age = g.v().has_value("name", "Alice").values("age").next();
    assert_eq!(age, Some(Value::Int(31)));
}

#[test]
fn escape_hatch_to_value() {
    let graph = test_graph();
    let g = graph.gremlin();

    // next_value() returns Value for any traversal type
    let v: Option<Value> = g.v().next_value();
    assert!(matches!(v, Some(Value::Vertex(_))));

    // to_value_list() also works
    let values: Vec<Value> = g.v().to_value_list();
    assert!(values.iter().all(|v| matches!(v, Value::Vertex(_))));
    
    // GraphVertex has to_value() for serialization
    let alice = g.v().has_value("name", "Alice").next().unwrap();
    let value: Value = alice.to_value();
    assert!(matches!(value, Value::Vertex(_)));
}

#[test]
fn vertex_existence_check() {
    let graph = test_graph();
    let g = graph.gremlin();

    let alice = g.v().has_value("name", "Alice").next().unwrap();
    assert!(alice.exists());

    // Delete the vertex
    alice.remove().unwrap();

    // Now methods return None (stale reference)
    assert!(!alice.exists());
    assert!(alice.label().is_none());
    assert!(alice.property("name").is_none());
}

#[test]
fn one_method_error_handling() {
    let graph = test_graph();
    let g = graph.gremlin();

    // one() succeeds when exactly one match
    let result = g.v().has_value("email", "alice@example.com").one();
    assert!(result.is_ok());

    // one() fails when multiple matches
    let result = g.v().has_label("person").one();
    assert!(matches!(result, Err(TraversalError::NotOne(_))));

    // one() fails when no matches
    let result = g.v().has_value("name", "Nobody").one();
    assert!(matches!(result, Err(TraversalError::NotOne(0))));
}
```

---

## Summary

This spec defines a path to **TinkerPop-faithful** vertex/edge objects in Interstellar using a **marker type system**.

### Core Architecture

1. **Marker types**: `Vertex`, `Edge`, `Scalar` track traversal output type at compile-time
2. **`GraphVertex` / `GraphEdge`**: Live objects with `Arc<Graph>` for property access and traversal
3. **Type-aware terminal methods**: `next()` return type determined by marker:
   - `BoundTraversal<..., Vertex>` → `next()` returns `Option<GraphVertex>`
   - `BoundTraversal<..., Edge>` → `next()` returns `Option<GraphEdge>`
   - `BoundTraversal<..., Scalar>` → `next()` returns `Option<Value>`

### Key Design Decisions

1. **TinkerPop fidelity** - `g.v().next()` returns `GraphVertex` directly, matching Java TinkerPop
2. **Type-state pattern** - Steps transform markers (e.g., `out_e()` changes `Vertex` → `Edge`)
3. **Escape hatch** - `next_value()` / `to_value_list()` available for raw `Value` access
4. **Internal efficiency** - Traversal execution uses lightweight `Value`; conversion at terminal boundary

### Breaking Change

This is a breaking change: `g.v().next()` changes from `Option<Value>` to `Option<GraphVertex>`.

Migration: Use `g.v().next_value()` for the old behavior.

### TinkerPop-Faithful API

```rust
// g.V().next() returns Vertex - just like TinkerPop!
let v: GraphVertex = g.v().next().unwrap();

// Property access and traversal work directly
println!("Label: {}", v.label().unwrap());
let friends = v.out("knows").to_list();
v.property_set("status", "active").unwrap();

// g.V().values("name").next() returns the value
let name: Value = g.v().values("name").next().unwrap();

// Type flows through the chain
let edges: Vec<GraphEdge> = g.v().out_e().has_label("knows").to_list();
```

### Implementation Phases

| Phase | Scope | Effort |
|-------|-------|--------|
| 1. Core types + markers | `GraphVertex`, `GraphEdge`, marker types, terminal methods | 3-4 days |
| 2. Step type transformations | Update all steps to carry correct markers | 3-4 days |
| 3. GraphVertex traversal methods | `v.out()`, `v.in_()`, etc. | 2-3 days |
| 4. COW/Rhai/GQL integration | Update mutation paths and scripting | 3-4 days |
| **Total** | | **11-15 days** |
