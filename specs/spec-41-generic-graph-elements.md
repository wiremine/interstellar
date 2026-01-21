# Spec 41: Generic GraphVertex and GraphEdge

## Overview

Make `GraphVertex` and `GraphEdge` generic over the storage backend so that both in-memory (`Graph`) and mmap-based (`CowMmapGraph`) graphs return rich element types from traversal terminal methods.

### Current State

`GraphVertex` and `GraphEdge` are hardcoded to use `Arc<Graph>` (in-memory storage):

```rust
// src/graph_elements.rs
pub struct GraphVertex {
    id: VertexId,
    graph: Arc<Graph>,  // Hardcoded to in-memory Graph
}

pub struct GraphEdge {
    id: EdgeId,
    graph: Arc<Graph>,  // Hardcoded to in-memory Graph
}
```

In the Rhai integration, mmap graphs fall back to returning raw `Value` types:

```rust
// src/rhai/traversal.rs
fn value_to_rich_dynamic(&self, value: Value) -> Dynamic {
    match &self.storage {
        StorageAdapter::InMemory(graph) => match value {
            Value::Vertex(id) => Dynamic::from(GraphVertex::new(id, Arc::clone(graph))),
            Value::Edge(id) => Dynamic::from(GraphEdge::new(id, Arc::clone(graph))),
            other => value_to_dynamic(other),
        },
        #[cfg(feature = "mmap")]
        StorageAdapter::Mmap(_) => {
            // Mmap doesn't support rich types yet, return Value
            value_to_dynamic(value)
        }
    }
}
```

### Goal

Enable both storage backends to return `GraphVertex<G>` and `GraphEdge<G>` where `G` implements a common graph access trait:

```rust
// In-memory
let v: GraphVertex<Graph> = g.v().next().unwrap();

// Mmap-based  
let v: GraphVertex<CowMmapGraph> = g.v().next().unwrap();

// Both support the same API
println!("Label: {}", v.label().unwrap());
let friends = v.out("knows").to_list();
```

---

## Part 1: GraphAccess Trait

### 1.1 Trait Definition

Define a trait that abstracts read and write access to the graph, implemented by both `Graph` and `CowMmapGraph`:

```rust
// src/graph_access.rs (NEW FILE)

use std::collections::HashMap;
use crate::error::StorageError;
use crate::storage::{Vertex, Edge};
use crate::value::{EdgeId, Value, VertexId};

/// Trait for graph access operations needed by GraphVertex/GraphEdge.
///
/// This trait provides the minimal interface that `GraphVertex` and `GraphEdge`
/// need to access vertex/edge data and perform mutations. Both `Graph` (in-memory)
/// and `CowMmapGraph` (persistent) implement this trait.
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to allow concurrent access.
///
/// # Implementation Notes
///
/// The methods use interior mutability patterns (like `RwLock`) internally,
/// so they take `&self` even for mutation operations. This matches the
/// design of `Graph` which uses `RwLock<InMemoryGraph>` internally.
pub trait GraphAccess: Send + Sync + Clone + 'static {
    // =========================================================================
    // Read Operations
    // =========================================================================

    /// Get a vertex by ID.
    ///
    /// Returns `None` if the vertex doesn't exist.
    fn get_vertex(&self, id: VertexId) -> Option<Vertex>;

    /// Get an edge by ID.
    ///
    /// Returns `None` if the edge doesn't exist.
    fn get_edge(&self, id: EdgeId) -> Option<Edge>;

    /// Get outgoing edge IDs from a vertex.
    ///
    /// Returns edge IDs for edges where the vertex is the source.
    fn out_edge_ids(&self, vertex: VertexId) -> Vec<EdgeId>;

    /// Get incoming edge IDs to a vertex.
    ///
    /// Returns edge IDs for edges where the vertex is the destination.
    fn in_edge_ids(&self, vertex: VertexId) -> Vec<EdgeId>;

    // =========================================================================
    // Write Operations
    // =========================================================================

    /// Set a property on a vertex.
    fn set_vertex_property(
        &self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError>;

    /// Set a property on an edge.
    fn set_edge_property(
        &self,
        id: EdgeId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError>;

    /// Add a new edge between vertices.
    fn add_edge(
        &self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError>;

    /// Remove a vertex and all incident edges.
    fn remove_vertex(&self, id: VertexId) -> Result<(), StorageError>;

    /// Remove an edge.
    fn remove_edge(&self, id: EdgeId) -> Result<(), StorageError>;
}
```

### 1.2 Implementation for Graph (In-Memory)

```rust
// src/storage/cow.rs - add impl

impl GraphAccess for Arc<Graph> {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        self.snapshot().get_vertex(id)
    }

    fn get_edge(&self, id: EdgeId) -> Option<Edge> {
        self.snapshot().get_edge(id)
    }

    fn out_edge_ids(&self, vertex: VertexId) -> Vec<EdgeId> {
        self.snapshot()
            .out_edges(vertex)
            .map(|e| e.id)
            .collect()
    }

    fn in_edge_ids(&self, vertex: VertexId) -> Vec<EdgeId> {
        self.snapshot()
            .in_edges(vertex)
            .map(|e| e.id)
            .collect()
    }

    fn set_vertex_property(
        &self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        Graph::set_vertex_property(self, id, key, value)
    }

    fn set_edge_property(
        &self,
        id: EdgeId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        Graph::set_edge_property(self, id, key, value)
    }

    fn add_edge(
        &self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError> {
        Graph::add_edge(self, src, dst, label, properties)
    }

    fn remove_vertex(&self, id: VertexId) -> Result<(), StorageError> {
        Graph::remove_vertex(self, id)
    }

    fn remove_edge(&self, id: EdgeId) -> Result<(), StorageError> {
        Graph::remove_edge(self, id)
    }
}
```

### 1.3 Implementation for CowMmapGraph (Persistent)

```rust
// src/storage/cow_mmap.rs - add impl

impl GraphAccess for Arc<CowMmapGraph> {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        self.snapshot().get_vertex(id)
    }

    fn get_edge(&self, id: EdgeId) -> Option<Edge> {
        self.snapshot().get_edge(id)
    }

    fn out_edge_ids(&self, vertex: VertexId) -> Vec<EdgeId> {
        self.snapshot()
            .out_edges(vertex)
            .map(|e| e.id)
            .collect()
    }

    fn in_edge_ids(&self, vertex: VertexId) -> Vec<EdgeId> {
        self.snapshot()
            .in_edges(vertex)
            .map(|e| e.id)
            .collect()
    }

    fn set_vertex_property(
        &self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        CowMmapGraph::set_vertex_property(self, id, key, value)
    }

    fn set_edge_property(
        &self,
        id: EdgeId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        CowMmapGraph::set_edge_property(self, id, key, value)
    }

    fn add_edge(
        &self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError> {
        CowMmapGraph::add_edge(self, src, dst, label, properties)
    }

    fn remove_vertex(&self, id: VertexId) -> Result<(), StorageError> {
        CowMmapGraph::remove_vertex(self, id)
    }

    fn remove_edge(&self, id: EdgeId) -> Result<(), StorageError> {
        CowMmapGraph::remove_edge(self, id)
    }
}
```

---

## Part 2: Generic GraphVertex and GraphEdge

### 2.1 Generic Type Definitions

```rust
// src/graph_elements.rs - UPDATE

use crate::graph_access::GraphAccess;

/// A vertex reference with access to the graph.
///
/// `GraphVertex<G>` is parameterized over the graph type `G` which must
/// implement [`GraphAccess`]. This allows the same vertex API to work
/// with both in-memory and persistent storage backends.
///
/// # Type Parameters
///
/// - `G`: The graph type, typically `Arc<Graph>` or `Arc<CowMmapGraph>`
///
/// # Thread Safety
///
/// `GraphVertex<G>` is `Clone`, `Send`, and `Sync` when `G` is.
///
/// # Example
///
/// ```rust
/// use interstellar::prelude::*;
/// use std::sync::Arc;
///
/// // In-memory graph
/// let graph = Arc::new(Graph::new());
/// let g = graph.gremlin(graph.clone());
/// let v: GraphVertex<Arc<Graph>> = g.v().next().unwrap();
///
/// // Persistent graph (with mmap feature)
/// // let graph = Arc::new(CowMmapGraph::open("path").unwrap());
/// // let v: GraphVertex<Arc<CowMmapGraph>> = g.v().next().unwrap();
/// ```
#[derive(Clone)]
pub struct GraphVertex<G: GraphAccess> {
    id: VertexId,
    graph: G,
}

impl<G: GraphAccess> GraphVertex<G> {
    /// Create a new GraphVertex.
    pub fn new(id: VertexId, graph: G) -> Self {
        Self { id, graph }
    }

    /// Get the vertex ID.
    #[inline]
    pub fn id(&self) -> VertexId {
        self.id
    }

    /// Get the vertex label.
    pub fn label(&self) -> Option<String> {
        self.graph.get_vertex(self.id).map(|v| v.label)
    }

    /// Get a property value by key.
    pub fn property(&self, key: &str) -> Option<Value> {
        self.graph
            .get_vertex(self.id)
            .and_then(|v| v.properties.get(key).cloned())
    }

    /// Get all properties as a map.
    pub fn properties(&self) -> HashMap<String, Value> {
        self.graph
            .get_vertex(self.id)
            .map(|v| v.properties)
            .unwrap_or_default()
    }

    /// Check if the vertex still exists.
    pub fn exists(&self) -> bool {
        self.graph.get_vertex(self.id).is_some()
    }

    /// Set a property value.
    pub fn property_set(&self, key: &str, value: impl Into<Value>) -> Result<(), StorageError> {
        self.graph.set_vertex_property(self.id, key, value.into())
    }

    /// Get the graph reference.
    #[inline]
    pub fn graph(&self) -> &G {
        &self.graph
    }

    /// Convert to a lightweight Value for serialization.
    #[inline]
    pub fn to_value(&self) -> Value {
        Value::Vertex(self.id)
    }

    /// Add an outgoing edge to another vertex.
    pub fn add_edge(&self, label: &str, to: &GraphVertex<G>) -> Result<GraphEdge<G>, StorageError> {
        self.add_edge_to_id(label, to.id)
    }

    /// Add an outgoing edge to a vertex by ID.
    pub fn add_edge_to_id(&self, label: &str, to: VertexId) -> Result<GraphEdge<G>, StorageError> {
        let edge_id = self.graph.add_edge(self.id, to, label, HashMap::new())?;
        Ok(GraphEdge::new(edge_id, self.graph.clone()))
    }

    /// Add an outgoing edge with properties.
    pub fn add_edge_with_props(
        &self,
        label: &str,
        to: &GraphVertex<G>,
        properties: HashMap<String, Value>,
    ) -> Result<GraphEdge<G>, StorageError> {
        let edge_id = self.graph.add_edge(self.id, to.id, label, properties)?;
        Ok(GraphEdge::new(edge_id, self.graph.clone()))
    }

    /// Remove this vertex from the graph.
    pub fn remove(&self) -> Result<(), StorageError> {
        self.graph.remove_vertex(self.id)
    }
}
```

### 2.2 Generic GraphEdge

```rust
// src/graph_elements.rs - UPDATE (continued)

/// An edge reference with access to the graph.
///
/// `GraphEdge<G>` is parameterized over the graph type `G` which must
/// implement [`GraphAccess`].
#[derive(Clone)]
pub struct GraphEdge<G: GraphAccess> {
    id: EdgeId,
    graph: G,
}

impl<G: GraphAccess> GraphEdge<G> {
    /// Create a new GraphEdge.
    pub fn new(id: EdgeId, graph: G) -> Self {
        Self { id, graph }
    }

    /// Get the edge ID.
    #[inline]
    pub fn id(&self) -> EdgeId {
        self.id
    }

    /// Get the edge label.
    pub fn label(&self) -> Option<String> {
        self.graph.get_edge(self.id).map(|e| e.label)
    }

    /// Get the source (outgoing) vertex.
    pub fn out_v(&self) -> Option<GraphVertex<G>> {
        self.graph
            .get_edge(self.id)
            .map(|e| GraphVertex::new(e.src, self.graph.clone()))
    }

    /// Get the destination (incoming) vertex.
    pub fn in_v(&self) -> Option<GraphVertex<G>> {
        self.graph
            .get_edge(self.id)
            .map(|e| GraphVertex::new(e.dst, self.graph.clone()))
    }

    /// Get both endpoint vertices.
    pub fn both_v(&self) -> Option<(GraphVertex<G>, GraphVertex<G>)> {
        self.graph.get_edge(self.id).map(|e| {
            (
                GraphVertex::new(e.src, self.graph.clone()),
                GraphVertex::new(e.dst, self.graph.clone()),
            )
        })
    }

    /// Get a property value by key.
    pub fn property(&self, key: &str) -> Option<Value> {
        self.graph
            .get_edge(self.id)
            .and_then(|e| e.properties.get(key).cloned())
    }

    /// Get all properties.
    pub fn properties(&self) -> HashMap<String, Value> {
        self.graph
            .get_edge(self.id)
            .map(|e| e.properties)
            .unwrap_or_default()
    }

    /// Set a property value.
    pub fn property_set(&self, key: &str, value: impl Into<Value>) -> Result<(), StorageError> {
        self.graph.set_edge_property(self.id, key, value.into())
    }

    /// Check if the edge exists.
    pub fn exists(&self) -> bool {
        self.graph.get_edge(self.id).is_some()
    }

    /// Get the graph reference.
    #[inline]
    pub fn graph(&self) -> &G {
        &self.graph
    }

    /// Convert to a lightweight Value.
    #[inline]
    pub fn to_value(&self) -> Value {
        Value::Edge(self.id)
    }

    /// Remove this edge.
    pub fn remove(&self) -> Result<(), StorageError> {
        self.graph.remove_edge(self.id)
    }
}
```

### 2.3 Type Aliases for Convenience

```rust
// src/graph_elements.rs - add type aliases

/// A vertex reference for in-memory graphs.
///
/// This is the most common type when using `Graph` (in-memory storage).
pub type InMemoryVertex = GraphVertex<Arc<Graph>>;

/// An edge reference for in-memory graphs.
pub type InMemoryEdge = GraphEdge<Arc<Graph>>;

/// A vertex reference for persistent mmap graphs.
#[cfg(feature = "mmap")]
pub type PersistentVertex = GraphVertex<Arc<CowMmapGraph>>;

/// An edge reference for persistent mmap graphs.
#[cfg(feature = "mmap")]
pub type PersistentEdge = GraphEdge<Arc<CowMmapGraph>>;
```

---

## Part 3: GraphVertexTraversal Generic Updates

### 3.1 Generic Traversal Type

The `GraphVertexTraversal` type also needs to be generic:

```rust
// src/graph_elements.rs - UPDATE

/// A traversal starting from a specific vertex.
#[derive(Clone)]
pub struct GraphVertexTraversal<G: GraphAccess> {
    graph: G,
    start_id: VertexId,
    steps: Vec<TraversalStep>,
}

impl<G: GraphAccess> GraphVertexTraversal<G> {
    pub(crate) fn new(graph: G, start_id: VertexId) -> Self {
        Self {
            graph,
            start_id,
            steps: Vec::new(),
        }
    }

    // ... navigation and filter steps remain the same ...

    /// Execute and return all vertices.
    pub fn to_list(self) -> Vec<GraphVertex<G>> {
        // Implementation needs to use the graph's traversal API
        // This may require additional trait bounds or methods
        todo!("Implement generic traversal execution")
    }

    /// Execute and return the first vertex.
    pub fn first(self) -> Option<GraphVertex<G>> {
        self.to_list().into_iter().next()
    }
}
```

### 3.2 Traversal Method on GraphVertex

```rust
impl<G: GraphAccess> GraphVertex<G> {
    /// Traverse to outgoing adjacent vertices with a specific edge label.
    pub fn out(&self, label: &str) -> GraphVertexTraversal<G> {
        GraphVertexTraversal::new(self.graph.clone(), self.id).out_label(label)
    }

    /// Traverse to outgoing adjacent vertices (all labels).
    pub fn out_all(&self) -> GraphVertexTraversal<G> {
        GraphVertexTraversal::new(self.graph.clone(), self.id).out()
    }

    /// Traverse to incoming adjacent vertices.
    pub fn in_(&self, label: &str) -> GraphVertexTraversal<G> {
        GraphVertexTraversal::new(self.graph.clone(), self.id).in_label(label)
    }

    // ... other traversal methods ...
}
```

---

## Part 4: Rhai Integration Updates

### 4.1 Updated value_to_rich_dynamic

```rust
// src/rhai/traversal.rs - UPDATE

fn value_to_rich_dynamic(&self, value: Value) -> Dynamic {
    match &self.storage {
        StorageAdapter::InMemory(graph) => match value {
            Value::Vertex(id) => {
                Dynamic::from(GraphVertex::new(id, Arc::clone(graph)))
            }
            Value::Edge(id) => {
                Dynamic::from(GraphEdge::new(id, Arc::clone(graph)))
            }
            other => value_to_dynamic(other),
        },
        #[cfg(feature = "mmap")]
        StorageAdapter::Mmap(graph) => match value {
            Value::Vertex(id) => {
                Dynamic::from(GraphVertex::new(id, Arc::clone(graph)))
            }
            Value::Edge(id) => {
                Dynamic::from(GraphEdge::new(id, Arc::clone(graph)))
            }
            other => value_to_dynamic(other),
        },
    }
}
```

### 4.2 Rhai Type Registration

Since Rhai doesn't support generic types directly, we need to register both concrete types:

```rust
// src/rhai/types.rs - UPDATE

/// Register GraphVertex<Arc<Graph>> for in-memory graphs.
fn register_graph_vertex_inmemory(engine: &mut Engine) {
    engine.register_type_with_name::<GraphVertex<Arc<Graph>>>("GraphVertex");
    
    engine.register_get("id", |v: &mut GraphVertex<Arc<Graph>>| v.id());
    engine.register_fn("label", |v: &mut GraphVertex<Arc<Graph>>| -> Dynamic {
        match v.label() {
            Some(label) => Dynamic::from(label),
            None => Dynamic::UNIT,
        }
    });
    engine.register_fn("property", |v: &mut GraphVertex<Arc<Graph>>, key: ImmutableString| -> Dynamic {
        match v.property(key.as_str()) {
            Some(val) => value_to_dynamic(val),
            None => Dynamic::UNIT,
        }
    });
    engine.register_fn("exists", |v: &mut GraphVertex<Arc<Graph>>| v.exists());
    engine.register_fn("to_value", |v: &mut GraphVertex<Arc<Graph>>| v.to_value());
}

/// Register GraphVertex<Arc<CowMmapGraph>> for mmap graphs.
#[cfg(feature = "mmap")]
fn register_graph_vertex_mmap(engine: &mut Engine) {
    engine.register_type_with_name::<GraphVertex<Arc<CowMmapGraph>>>("MmapGraphVertex");
    
    engine.register_get("id", |v: &mut GraphVertex<Arc<CowMmapGraph>>| v.id());
    engine.register_fn("label", |v: &mut GraphVertex<Arc<CowMmapGraph>>| -> Dynamic {
        match v.label() {
            Some(label) => Dynamic::from(label),
            None => Dynamic::UNIT,
        }
    });
    // ... same methods as in-memory version ...
}
```

**Alternative: Macro-based registration** to reduce duplication:

```rust
macro_rules! register_graph_vertex {
    ($engine:expr, $graph_type:ty, $name:literal) => {
        $engine.register_type_with_name::<GraphVertex<$graph_type>>($name);
        
        $engine.register_get("id", |v: &mut GraphVertex<$graph_type>| v.id());
        $engine.register_fn("label", |v: &mut GraphVertex<$graph_type>| -> Dynamic {
            match v.label() {
                Some(label) => Dynamic::from(label),
                None => Dynamic::UNIT,
            }
        });
        // ... etc ...
    };
}

// Usage:
register_graph_vertex!(engine, Arc<Graph>, "GraphVertex");
#[cfg(feature = "mmap")]
register_graph_vertex!(engine, Arc<CowMmapGraph>, "MmapGraphVertex");
```

---

## Part 5: Typed Traversal Updates

### 5.1 CowBoundTraversal Generic Parameter

The typed traversal system needs the graph type for creating rich elements:

```rust
// src/storage/cow.rs - UPDATE

pub struct CowBoundTraversal<'g, In, Out, G: GraphAccess = Arc<Graph>> {
    storage: &'g dyn GraphStorage,
    interner: &'g StringInterner,
    graph: G,
    traversal: Traversal<In, Out>,
    track_paths: bool,
}

impl<'g, In> CowBoundTraversal<'g, In, VertexMarker, Arc<Graph>> {
    pub fn next(self) -> Option<GraphVertex<Arc<Graph>>> {
        // ... existing implementation ...
    }
}

#[cfg(feature = "mmap")]
impl<'g, In> CowBoundTraversal<'g, In, VertexMarker, Arc<CowMmapGraph>> {
    pub fn next(self) -> Option<GraphVertex<Arc<CowMmapGraph>>> {
        // ... same implementation pattern ...
    }
}
```

---

## Part 6: Migration Strategy

### Phase 1: Add GraphAccess Trait (Non-Breaking)
- Create `src/graph_access.rs` with the `GraphAccess` trait
- Implement `GraphAccess` for `Arc<Graph>`
- Implement `GraphAccess` for `Arc<CowMmapGraph>` (behind mmap feature)
- No changes to existing types yet

**Effort**: 1-2 days

### Phase 2: Make GraphVertex/GraphEdge Generic (Breaking)
- Update `GraphVertex` and `GraphEdge` to be generic over `G: GraphAccess`
- Add type aliases for convenience (`InMemoryVertex`, `PersistentVertex`)
- Update `GraphVertexTraversal` to be generic
- Update all usages in the codebase

**Effort**: 2-3 days

### Phase 3: Update Rhai Integration
- Update `value_to_rich_dynamic` to handle mmap case
- Register both in-memory and mmap vertex/edge types with Rhai
- Update existing tests

**Effort**: 1-2 days

### Phase 4: Update CowBoundTraversal
- Add graph type parameter to `CowBoundTraversal`
- Implement typed terminal methods for both graph types
- Update `CowMmapBoundTraversal` similarly

**Effort**: 2-3 days

### Phase 5: Testing and Documentation
- Add tests for mmap-based `GraphVertex`/`GraphEdge`
- Add integration tests for Rhai with mmap graphs
- Update documentation and examples

**Effort**: 1-2 days

---

## Part 7: Impact Analysis

### Files Requiring Changes

| Category | Files | Scope |
|----------|-------|-------|
| **New Files** | `src/graph_access.rs` | New trait |
| **Core Changes** | `src/graph_elements.rs` | Generic types |
| **Storage** | `src/storage/cow.rs` | Implement trait, update traversal |
| | `src/storage/cow_mmap.rs` | Implement trait, update traversal |
| **Rhai** | `src/rhai/traversal.rs` | Update rich type conversion |
| | `src/rhai/types.rs` | Register mmap types |
| **Tests** | `tests/storage/cow.rs` | Update type annotations |
| | New mmap graph element tests | New tests |

### Breaking Changes

1. **Type signature changes**: `GraphVertex` → `GraphVertex<G>`
2. **Import changes**: Users may need to specify type parameters or use aliases
3. **Rhai type names**: May need different names for in-memory vs mmap

### Backward Compatibility

To minimize disruption:

```rust
// Default type alias preserves common usage
pub type GraphVertex = graph_elements::GraphVertex<Arc<Graph>>;
pub type GraphEdge = graph_elements::GraphEdge<Arc<Graph>>;
```

Most code that uses in-memory graphs will continue to work unchanged.

---

## Part 8: Design Decisions

### Resolved

1. **Generic approach over separate types**: Using `GraphVertex<G>` instead of `GraphVertex` + `MmapGraphVertex` reduces code duplication and provides a unified API.

2. **Trait on `Arc<G>` not `G`**: The `GraphAccess` trait is implemented for `Arc<Graph>` rather than `Graph` because graph elements need to clone the reference, and `Arc` provides the necessary semantics.

3. **Clone bound on GraphAccess**: Required because `GraphVertex` and `GraphEdge` need to store a cloneable reference to the graph.

### Open Questions

1. **Rhai type naming**: Should mmap vertices appear as `GraphVertex` or `MmapGraphVertex` in Rhai scripts?
   - **Proposal**: Use `GraphVertex` for both since they have identical APIs

2. **Trait method signature**: Should `GraphAccess` methods return owned `Vertex`/`Edge` or references?
   - **Proposal**: Return owned types for simplicity; the cost is minimal for these small structs

3. **Traversal execution**: How should `GraphVertexTraversal<G>` execute traversals generically?
   - **Proposal**: Add a `gremlin_source()` method to `GraphAccess` that returns a traversal source

---

## Part 9: Test Cases

```rust
#[test]
fn generic_vertex_inmemory() {
    let graph = Arc::new(Graph::new());
    let id = graph.add_vertex("person", HashMap::from([
        ("name".to_string(), "Alice".into()),
    ]));
    
    let v: GraphVertex<Arc<Graph>> = GraphVertex::new(id, graph.clone());
    assert_eq!(v.label(), Some("person".to_string()));
    assert_eq!(v.property("name"), Some(Value::String("Alice".to_string())));
}

#[cfg(feature = "mmap")]
#[test]
fn generic_vertex_mmap() {
    let dir = tempfile::tempdir().unwrap();
    let graph = Arc::new(CowMmapGraph::create(dir.path()).unwrap());
    let id = graph.add_vertex("person", HashMap::from([
        ("name".to_string(), "Alice".into()),
    ]));
    
    let v: GraphVertex<Arc<CowMmapGraph>> = GraphVertex::new(id, graph.clone());
    assert_eq!(v.label(), Some("person".to_string()));
    assert_eq!(v.property("name"), Some(Value::String("Alice".to_string())));
}

#[test]
fn type_alias_convenience() {
    // InMemoryVertex is just GraphVertex<Arc<Graph>>
    let graph = Arc::new(Graph::new());
    let id = graph.add_vertex("person", HashMap::new());
    
    let v: InMemoryVertex = GraphVertex::new(id, graph);
    assert!(v.exists());
}

#[cfg(feature = "mmap")]
#[test]
fn rhai_mmap_rich_types() {
    let dir = tempfile::tempdir().unwrap();
    let graph = Arc::new(CowMmapGraph::create(dir.path()).unwrap());
    graph.add_vertex("person", HashMap::from([
        ("name".to_string(), "Alice".into()),
    ]));
    
    let engine = RhaiEngine::new();
    let result: String = engine.eval_with_mmap_graph(
        graph,
        r#"
            let g = graph.gremlin();
            let v = g.v().first();
            v.label()
        "#,
    ).unwrap();
    
    assert_eq!(result, "person");
}
```

---

## Summary

This spec extends `GraphVertex` and `GraphEdge` to work with any storage backend by:

1. **Adding `GraphAccess` trait**: Abstracts graph read/write operations
2. **Making element types generic**: `GraphVertex<G>` and `GraphEdge<G>`
3. **Implementing for both backends**: `Arc<Graph>` and `Arc<CowMmapGraph>`
4. **Updating Rhai integration**: Both backends return rich types

### Estimated Effort

| Phase | Effort |
|-------|--------|
| Phase 1: GraphAccess trait | 1-2 days |
| Phase 2: Generic types | 2-3 days |
| Phase 3: Rhai updates | 1-2 days |
| Phase 4: Traversal updates | 2-3 days |
| Phase 5: Testing/docs | 1-2 days |
| **Total** | **7-12 days** |

### Key Benefits

- **Unified API**: Same `GraphVertex`/`GraphEdge` interface for all backends
- **Rich types everywhere**: Both in-memory and mmap graphs return rich elements
- **Type safety**: Compile-time verification of graph type compatibility
- **Minimal breaking changes**: Type aliases preserve most existing code
