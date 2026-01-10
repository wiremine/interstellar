# Spec 10: Mutation Steps

## Overview

This specification defines the implementation of Gremlin mutation steps for RustGremlin. Mutation steps allow creating, updating, and deleting vertices and edges in the graph.

## Goals

1. Implement `addV()` - Create new vertices
2. Implement `addE()` - Create new edges
3. Implement `property()` - Add/update properties on vertices and edges
4. Implement `drop()` - Delete vertices and edges
5. Support `from()` and `to()` modulators for edge creation
6. Ensure mutations integrate with both in-memory and mmap storage backends

## Non-Goals

- Transaction management (handled at storage level)
- `mergeV()` / `mergeE()` (upsert operations - future work)
- Batch mutation optimization (future work)

---

## 1. Mutation Steps

### 1.1 `addV(label)` - Add Vertex

Creates a new vertex with the specified label.

**Gremlin Syntax:**
```groovy
g.addV('person')
g.addV('person').property('name', 'Alice').property('age', 30)
```

**Rust API:**
```rust
// Basic vertex creation
let v = g.add_v("person").to_list();

// With properties (builder pattern)
let v = g.add_v("person")
    .property("name", "Alice")
    .property("age", 30)
    .next();

// With properties (map)
let props = HashMap::from([
    ("name", Value::String("Alice".into())),
    ("age", Value::Int(30)),
]);
let v = g.add_v_with_props("person", props).next();
```

**Behavior:**
- Creates a new vertex with an auto-generated `VertexId`
- The label is required
- Returns a traverser containing the newly created `Vertex`
- Properties can be added via chained `.property()` calls
- The vertex is immediately visible to subsequent traversal steps

**Return Type:** `Traversal<..., Vertex>`

### 1.2 `addE(label)` - Add Edge

Creates a new edge with the specified label, connecting two vertices.

**Gremlin Syntax:**
```groovy
g.V(1).addE('knows').to(g.V(2))
g.V(1).addE('knows').from(g.V(0)).to(g.V(2))
g.addE('knows').from(__.V(1)).to(__.V(2)).property('weight', 0.5)
```

**Rust API:**
```rust
// From current traverser to target vertex
let e = g.v_id(VertexId(1))
    .add_e("knows")
    .to_vertex(VertexId(2))
    .next();

// With explicit from/to
let e = g.add_e("knows")
    .from_vertex(VertexId(1))
    .to_vertex(VertexId(2))
    .next();

// With traversals for from/to
let e = g.add_e("knows")
    .from_traversal(__::has_value("name", "Alice"))
    .to_traversal(__::has_value("name", "Bob"))
    .property("since", 2020)
    .next();

// From traverser context (common pattern)
let edges = g.v()
    .has_label("person")
    .has_value("name", "Alice")
    .add_e("knows")
    .to_traversal(__::v().has_value("name", "Bob"))
    .to_list();
```

**Behavior:**
- Creates a new edge with an auto-generated `EdgeId`
- The label is required
- Requires both source and target vertices to be specified
- If called on a vertex traverser, that vertex is the implicit `from` vertex
- Returns a traverser containing the newly created `Edge`
- Properties can be added via chained `.property()` calls

**Return Type:** `Traversal<..., Edge>`

**Edge Builder State Machine:**
```
addE(label) -> AddEdgeBuilder [needs from & to]
    .from_vertex(id) -> AddEdgeBuilder [needs to]
    .from_traversal(t) -> AddEdgeBuilder [needs to]
    .to_vertex(id) -> AddEdgeBuilder [ready]
    .to_traversal(t) -> AddEdgeBuilder [ready]
    .property(k, v) -> AddEdgeBuilder [ready, with property]
    .build() -> Traversal<Edge> [if ready]
```

### 1.3 `property(key, value)` - Add/Update Property

Adds or updates a property on the current element (vertex or edge).

**Gremlin Syntax:**
```groovy
g.V(1).property('age', 31)
g.V(1).property(single, 'name', 'Alice')  // cardinality
g.E(1).property('weight', 0.8)
```

**Rust API:**
```rust
// On vertices
g.v_id(VertexId(1))
    .property("age", 31)
    .property("status", "active")
    .iterate();

// On edges
g.e_id(EdgeId(1))
    .property("weight", 0.8)
    .iterate();

// After addV/addE
g.add_v("person")
    .property("name", "Alice")
    .property("age", 30)
    .next();
```

**Behavior:**
- Sets the property value on the current element
- If the property exists, it is updated (single cardinality)
- Returns a traverser containing the modified element
- Works on both vertices and edges

**Return Type:** Same as input (`Traversal<..., Vertex>` or `Traversal<..., Edge>`)

### 1.4 `drop()` - Delete Element

Deletes the current element (vertex or edge) from the graph.

**Gremlin Syntax:**
```groovy
g.V(1).drop()                    // Delete vertex and its edges
g.E(1).drop()                    // Delete edge
g.V().has('status', 'deleted').drop()  // Delete multiple
```

**Rust API:**
```rust
// Delete single vertex
g.v_id(VertexId(1)).drop().iterate();

// Delete single edge
g.e_id(EdgeId(1)).drop().iterate();

// Delete matching elements
g.v().has_value("status", "deleted").drop().iterate();

// Delete all edges of a type
g.e().has_label("temp").drop().iterate();
```

**Behavior:**
- Removes the element from the graph
- When a vertex is dropped, all incident edges are also dropped
- Returns an empty traverser (no elements to continue with)
- Is a terminal-ish step (consumes the traverser)

**Return Type:** `Traversal<..., ()>` or requires `.iterate()` to execute

---

## 2. Modulator Steps

### 2.1 `from(source)` - Edge Source

Specifies the source vertex for edge creation.

**Variants:**
```rust
.from_vertex(VertexId)           // Direct vertex ID
.from_traversal(Traversal)       // Traversal that yields a vertex
.from_label(step_label: &str)    // Reference to a labeled step (via as_())
```

### 2.2 `to(target)` - Edge Target

Specifies the target vertex for edge creation.

**Variants:**
```rust
.to_vertex(VertexId)             // Direct vertex ID
.to_traversal(Traversal)         // Traversal that yields a vertex
.to_label(step_label: &str)      // Reference to a labeled step (via as_())
```

---

## 3. Storage Integration

### 3.1 GraphStorage Trait Extensions

The `GraphStorage` trait needs mutation methods:

```rust
pub trait GraphStorage: Send + Sync {
    // Existing read methods...
    
    // New mutation methods
    fn add_vertex(&self, label: &str, properties: HashMap<String, Value>) -> Result<VertexId, StorageError>;
    fn add_edge(&self, label: &str, from: VertexId, to: VertexId, properties: HashMap<String, Value>) -> Result<EdgeId, StorageError>;
    fn set_vertex_property(&self, id: VertexId, key: &str, value: Value) -> Result<(), StorageError>;
    fn set_edge_property(&self, id: EdgeId, key: &str, value: Value) -> Result<(), StorageError>;
    fn remove_vertex(&self, id: VertexId) -> Result<(), StorageError>;
    fn remove_edge(&self, id: EdgeId) -> Result<(), StorageError>;
}
```

### 3.2 In-Memory Storage

For `InMemoryGraph`:
- Mutations modify the internal `HashMap`/`Vec` structures
- Use `RwLock` for thread-safe concurrent access
- Vertex deletion cascades to edge deletion

### 3.3 Memory-Mapped Storage

For `MmapGraph`:
- Mutations append to the memory-mapped files
- Deleted elements are marked with a tombstone flag
- Compaction (future work) reclaims space from deleted elements

---

## 4. Traversal Execution Model

### 4.1 Immediate vs Lazy Execution

Mutation steps require special handling:

```rust
// Mutations are collected during traversal construction
let traversal = g.add_v("person").property("name", "Alice");

// Execution happens at terminal step
let vertex = traversal.next();  // <-- Mutation executed here
```

**Options:**

1. **Eager Execution** - Mutations execute immediately when the step is called
   - Pros: Simple mental model
   - Cons: Can't compose mutations, side effects during construction

2. **Lazy Execution** - Mutations execute at terminal step
   - Pros: Composable, consistent with read traversals
   - Cons: More complex implementation

3. **Hybrid** - `addV`/`addE` are lazy, but return a handle for chaining
   - Recommended approach for RustGremlin

### 4.2 Mutation Context

Mutations need access to the graph for writing:

```rust
pub struct MutationContext<'g, S: GraphStorage> {
    storage: &'g S,
    pending_vertices: Vec<PendingVertex>,
    pending_edges: Vec<PendingEdge>,
    pending_properties: Vec<PendingProperty>,
    pending_deletions: Vec<PendingDeletion>,
}

struct PendingVertex {
    label: String,
    properties: HashMap<String, Value>,
}

struct PendingEdge {
    label: String,
    from: EdgeEndpoint,
    to: EdgeEndpoint,
    properties: HashMap<String, Value>,
}

enum EdgeEndpoint {
    VertexId(VertexId),
    Traversal(Box<dyn AnonymousTraversal>),
    StepLabel(String),
}
```

---

## 5. Error Handling

### 5.1 Error Types

```rust
#[derive(Debug, Error)]
pub enum MutationError {
    #[error("vertex not found: {0:?}")]
    VertexNotFound(VertexId),
    
    #[error("edge not found: {0:?}")]
    EdgeNotFound(EdgeId),
    
    #[error("edge source vertex not found: {0:?}")]
    EdgeSourceNotFound(VertexId),
    
    #[error("edge target vertex not found: {0:?}")]
    EdgeTargetNotFound(VertexId),
    
    #[error("missing edge endpoint: {0}")]
    MissingEdgeEndpoint(&'static str),  // "from" or "to"
    
    #[error("traversal yielded no vertices for edge endpoint")]
    EmptyTraversalEndpoint,
    
    #[error("traversal yielded multiple vertices for edge endpoint")]
    AmbiguousTraversalEndpoint,
    
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}
```

### 5.2 Validation

- `addE()` must have both `from` and `to` specified before execution
- `from`/`to` traversals must yield exactly one vertex
- Referenced step labels (via `as_()`) must exist in the path
- Property keys must be valid strings

---

## 6. API Design Patterns

### 6.1 Builder Pattern for Edges

```rust
impl<S, E> Traversal<S, E, Vertex> {
    pub fn add_e(self, label: &str) -> AddEdgeBuilder<S, E> {
        AddEdgeBuilder {
            traversal: self,
            label: label.to_string(),
            from: Some(EdgeEndpoint::Traverser), // implicit from current
            to: None,
            properties: HashMap::new(),
        }
    }
}

impl<S, E> AddEdgeBuilder<S, E> {
    pub fn to_vertex(mut self, id: VertexId) -> Self {
        self.to = Some(EdgeEndpoint::VertexId(id));
        self
    }
    
    pub fn to_traversal<T: AnonymousTraversal>(mut self, t: T) -> Self {
        self.to = Some(EdgeEndpoint::Traversal(Box::new(t)));
        self
    }
    
    pub fn property(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.properties.insert(key.to_string(), value.into());
        self
    }
    
    // Terminal - executes the mutation
    pub fn next(self) -> Result<Edge, MutationError> {
        // Validate and execute
    }
}
```

### 6.2 Fluent Chaining

```rust
// Create interconnected vertices
let alice = g.add_v("person").property("name", "Alice").next()?;
let bob = g.add_v("person").property("name", "Bob").next()?;

g.v_id(alice.id())
    .add_e("knows")
    .to_vertex(bob.id())
    .property("since", 2020)
    .next()?;
```

---

## 7. Testing Requirements

### 7.1 Unit Tests

- `addV()` creates vertex with correct label
- `addV()` with properties stores all properties
- `addE()` creates edge with correct label and endpoints
- `addE()` with traversal endpoints resolves correctly
- `property()` adds new property
- `property()` updates existing property
- `drop()` removes vertex
- `drop()` removes vertex and cascades to edges
- `drop()` removes edge
- Error cases for all validation scenarios

### 7.2 Integration Tests

- Mutations visible to subsequent reads
- Mutations work with in-memory storage
- Mutations work with mmap storage
- Complex traversal with mixed read/write operations

### 7.3 Property-Based Tests

- Roundtrip: add vertex, read it back, verify properties
- Roundtrip: add edge, read it back, verify endpoints
- Delete vertex, verify edges are gone
- Concurrent mutations don't corrupt state

---

## 8. Example Usage

```rust
use rustgremlin::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let graph = InMemoryGraph::new();
    let g = graph.traversal();
    
    // Create a social network
    let alice = g.add_v("person")
        .property("name", "Alice")
        .property("age", 30)
        .next()?;
    
    let bob = g.add_v("person")
        .property("name", "Bob")
        .property("age", 32)
        .next()?;
    
    let charlie = g.add_v("person")
        .property("name", "Charlie")
        .property("age", 28)
        .next()?;
    
    // Create relationships
    g.v_id(alice.id())
        .add_e("knows").to_vertex(bob.id())
        .property("since", 2018)
        .next()?;
    
    g.v_id(alice.id())
        .add_e("knows").to_vertex(charlie.id())
        .property("since", 2020)
        .next()?;
    
    g.v_id(bob.id())
        .add_e("knows").to_vertex(charlie.id())
        .property("since", 2019)
        .next()?;
    
    // Query the graph
    let friends_of_alice = g.v_id(alice.id())
        .out_labels(&["knows"])
        .values("name")
        .to_list();
    
    println!("Alice's friends: {:?}", friends_of_alice);
    // Output: ["Bob", "Charlie"]
    
    // Update a property
    g.v_id(alice.id())
        .property("age", 31)
        .iterate();
    
    // Delete Bob and his edges
    g.v_id(bob.id()).drop().iterate();
    
    // Verify edges are gone
    let remaining_friends = g.v_id(alice.id())
        .out_labels(&["knows"])
        .values("name")
        .to_list();
    
    println!("Alice's remaining friends: {:?}", remaining_friends);
    // Output: ["Charlie"]
    
    Ok(())
}
```

---

## 9. Future Enhancements

- `mergeV()` / `mergeE()` - Upsert operations
- `Cardinality` support for multi-valued properties
- Batch mutation API for bulk operations
- Transaction boundaries with rollback support
- Mutation event hooks/listeners
