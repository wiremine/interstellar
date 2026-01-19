# Spec 33: Unified Graph API

## 1. Overview

This specification consolidates the graph storage and traversal APIs into a single, unified architecture based on Copy-on-Write (COW) semantics. The goal is to eliminate API fragmentation, reduce code duplication, and provide a clean, consistent developer experience.

### 1.1 Current State

The codebase currently has **three parallel graph APIs**:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Current Architecture                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Legacy Path (graph.rs + InMemoryGraph):                                │
│  ┌──────────────┐    ┌────────────────┐    ┌───────────────────────┐   │
│  │ InMemoryGraph│───▶│     Graph      │───▶│   GraphSnapshot<'g>   │   │
│  │  (storage)   │    │  (RwLock wrap) │    │  (borrows from Graph) │   │
│  └──────────────┘    └────────────────┘    └───────────────────────┘   │
│         │                                             │                  │
│         │                                             ▼                  │
│         │                                  ┌───────────────────────┐    │
│         │                                  │ GraphTraversalSource  │    │
│         │                                  │   BoundTraversal      │    │
│         │                                  │   (~90 step methods)  │    │
│         │                                  └───────────────────────┘    │
│         │                                                                │
│  COW In-Memory Path (cow.rs):                                           │
│  ┌──────────────┐                          ┌───────────────────────┐   │
│  │   CowGraph   │─────────────────────────▶│    CowSnapshot        │   │
│  │  (im crate)  │                          │  (owned, lock-free)   │   │
│  └──────────────┘                          └───────────────────────┘   │
│         │                                             │                  │
│         ▼                                             │                  │
│  ┌───────────────────────┐                           │                  │
│  │ CowTraversalSource    │                           │                  │
│  │ CowBoundTraversal     │◀──────────────────────────┘                  │
│  │ (~18 step methods)    │  ← INCOMPLETE API                            │
│  └───────────────────────┘                                              │
│                                                                          │
│  COW Persistent Path (cow_mmap.rs):                                     │
│  ┌──────────────┐                          ┌───────────────────────┐   │
│  │ CowMmapGraph │─────────────────────────▶│  CowMmapSnapshot      │   │
│  │ (mmap + COW) │                          │  (owned, lock-free)   │   │
│  └──────────────┘                          └───────────────────────┘   │
│         │                                             │                  │
│         ▼                                             │                  │
│  ┌───────────────────────┐                           │                  │
│  │ CowMmapTraversalSource│                           │                  │
│  │ CowMmapBoundTraversal │◀──────────────────────────┘                  │
│  │ (~18 step methods)    │  ← INCOMPLETE API                            │
│  └───────────────────────┘                                              │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Problems

| Problem | Impact |
|---------|--------|
| **API duplication** | `BoundTraversal` has 90 step methods; `CowBoundTraversal` has 18 (80% missing) |
| **Maintenance burden** | Changes must be made in 3 places (source.rs, cow.rs, cow_mmap.rs) |
| **Inconsistent experience** | Users must choose between APIs; COW users get fewer features |
| **Type proliferation** | 6+ traversal types that do nearly the same thing |
| **ExecutionContext coupling** | Hard-coded to `GraphSnapshot`, blocking COW adoption |
| **GQL compiler coupling** | Hard-coded to `GraphSnapshot`, can't work with COW snapshots |

### 1.3 Target State

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Target Architecture                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│                         ┌─────────────────────┐                         │
│                         │   Graph trait       │                         │
│                         │  (unified API)      │                         │
│                         └─────────────────────┘                         │
│                                   │                                      │
│                    ┌──────────────┴──────────────┐                      │
│                    │                             │                       │
│                    ▼                             ▼                       │
│         ┌──────────────────┐          ┌──────────────────┐             │
│         │  InMemoryGraph   │          │  PersistentGraph │             │
│         │  (im crate COW)  │          │  (mmap + COW)    │             │
│         │  "Graph::new()"  │          │  requires "mmap" │             │
│         └──────────────────┘          └──────────────────┘             │
│                    │                             │                       │
│                    └──────────────┬──────────────┘                      │
│                                   │                                      │
│                                   ▼                                      │
│                         ┌─────────────────────┐                         │
│                         │  Snapshot trait     │                         │
│                         │  (GraphStorage)     │                         │
│                         └─────────────────────┘                         │
│                                   │                                      │
│                                   ▼                                      │
│                         ┌─────────────────────┐                         │
│                         │  TraversalSource    │                         │
│                         │  BoundTraversal     │                         │
│                         │  (ALL 90+ methods)  │                         │
│                         └─────────────────────┘                         │
│                                                                          │
│  Usage:                                                                  │
│    let graph = Graph::new();              // In-memory (default)        │
│    let graph = Graph::open("path")?;      // Persistent (mmap feature)  │
│    let g = graph.traversal();             // Unified traversal API      │
│    g.add_v("Person").property("name", "Alice").iterate();               │
│    let count = g.v().has_label("Person").count();                       │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.4 Goals

| Goal | Description |
|------|-------------|
| **Single API** | One `Graph` type with unified traversal methods |
| **Full feature parity** | All 90+ traversal steps available on all graph types |
| **Zero code duplication** | Step methods defined once, work everywhere |
| **Trait-based extensibility** | Custom storage backends can implement `Graph` trait |
| **Clean migration** | Existing tests/examples can be updated incrementally |
| **Backward-compatible naming** | `Graph::new()` is the primary entry point |

### 1.5 Non-Goals

| Non-Goal | Rationale |
|----------|-----------|
| Backward compatibility with old API | Migration is required; old types will be deleted |
| Supporting non-COW storage | All storage now uses COW semantics |
| Runtime storage switching | Graph type is determined at construction |

---

## 2. Architecture

### 2.1 Core Traits

#### 2.1.1 `Graph` Trait

The primary trait that all graph types implement:

```rust
/// A graph database with COW snapshot semantics.
pub trait Graph: Send + Sync {
    /// The snapshot type returned by this graph.
    type Snapshot: Snapshot;
    
    /// Create an immutable snapshot for reads.
    /// 
    /// Snapshots are O(1) to create and own their data via structural sharing.
    /// They do not hold locks and can outlive the source graph reference.
    fn snapshot(&self) -> Self::Snapshot;
    
    /// Get the string interner for label resolution.
    fn interner(&self) -> &StringInterner;
    
    /// Get the current vertex count.
    fn vertex_count(&self) -> usize;
    
    /// Get the current edge count.
    fn edge_count(&self) -> usize;
    
    /// Add a vertex with label and properties.
    fn add_vertex(
        &self,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> VertexId;
    
    /// Add an edge between vertices.
    fn add_edge(
        &self,
        from: VertexId,
        to: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError>;
    
    /// Remove a vertex and all connected edges.
    fn remove_vertex(&self, id: VertexId) -> Result<(), StorageError>;
    
    /// Remove an edge.
    fn remove_edge(&self, id: EdgeId) -> Result<(), StorageError>;
    
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
    
    /// Create a traversal source for this graph.
    /// 
    /// The traversal source provides the Gremlin-style fluent API.
    /// Mutations are executed when terminal steps are called.
    fn traversal(&self) -> TraversalSource<'_, Self>
    where
        Self: Sized,
    {
        TraversalSource::new(self)
    }
    
    /// Execute a GQL mutation statement.
    /// 
    /// For read queries, use `snapshot().gql()` instead.
    fn gql(&self, statement: &str) -> Result<Vec<Value>, GqlError>;
    
    /// Execute a parameterized GQL mutation statement.
    fn gql_with_params(
        &self,
        statement: &str,
        params: HashMap<String, Value>,
    ) -> Result<Vec<Value>, GqlError>;
}
```

#### 2.1.2 `Snapshot` Trait

Trait for immutable graph snapshots:

```rust
/// An immutable snapshot of a graph at a point in time.
/// 
/// Snapshots implement `GraphStorage` for read operations and can
/// be used with the traversal engine and GQL compiler.
pub trait Snapshot: GraphStorage + Send + Sync + Clone {
    /// Get the string interner for label resolution.
    fn interner(&self) -> &StringInterner;
    
    /// Execute a GQL read query against this snapshot.
    fn gql(&self, query: &str) -> Result<Vec<Value>, GqlError>;
    
    /// Execute a parameterized GQL query against this snapshot.
    fn gql_with_params(
        &self,
        query: &str,
        params: HashMap<String, Value>,
    ) -> Result<Vec<Value>, GqlError>;
    
    /// Create a traversal source for read-only queries.
    fn traversal(&self) -> ReadOnlyTraversalSource<'_, Self>
    where
        Self: Sized,
    {
        ReadOnlyTraversalSource::new(self)
    }
}
```

### 2.2 Concrete Types

#### 2.2.1 `InMemoryGraph` (renamed from `CowGraph`)

```rust
/// In-memory graph with COW snapshot semantics.
/// 
/// This is the default graph type, suitable for:
/// - Development and testing
/// - Small to medium graphs that fit in memory
/// - Scenarios where persistence is not required
/// 
/// # Example
/// 
/// ```rust
/// use interstellar::Graph;
/// 
/// let graph = Graph::new();  // Returns InMemoryGraph
/// let g = graph.traversal();
/// 
/// let alice = g.add_v("Person").property("name", "Alice").next();
/// let bob = g.add_v("Person").property("name", "Bob").next();
/// g.add_e("KNOWS").from_id(alice).to_id(bob).iterate();
/// 
/// let count = g.v().has_label("Person").count();
/// assert_eq!(count, 2);
/// ```
pub struct InMemoryGraph {
    state: RwLock<CowGraphState>,
    schema: RwLock<Option<GraphSchema>>,
}

impl Graph for InMemoryGraph { ... }
```

#### 2.2.2 `PersistentGraph` (renamed from `CowMmapGraph`)

```rust
/// Persistent graph with COW snapshot semantics and mmap storage.
/// 
/// This graph type provides:
/// - Durable storage to disk
/// - Memory-mapped access for large graphs
/// - WAL-based crash recovery
/// - O(1) snapshots via COW layer
/// 
/// Requires the `mmap` feature.
/// 
/// # Example
/// 
/// ```rust
/// use interstellar::Graph;
/// 
/// let graph = Graph::open("my_database")?;  // Returns PersistentGraph
/// let g = graph.traversal();
/// 
/// g.add_v("Person").property("name", "Alice").iterate();
/// graph.checkpoint()?;  // Persist to disk
/// ```
#[cfg(feature = "mmap")]
pub struct PersistentGraph {
    mmap: MmapGraph,
    state: RwLock<CowGraphState>,
    interner_snapshot: Arc<StringInterner>,
}

#[cfg(feature = "mmap")]
impl Graph for PersistentGraph { ... }
```

### 2.3 Traversal Architecture

#### 2.3.1 Unified `ExecutionContext`

Make the execution context generic over storage:

```rust
/// Execution context for traversal operations.
/// 
/// Generic over any `GraphStorage` implementation, allowing the same
/// traversal code to work with any snapshot type.
pub struct ExecutionContext<'g, S: GraphStorage + ?Sized = dyn GraphStorage> {
    storage: &'g S,
    interner: &'g StringInterner,
    side_effects: SideEffects,
    track_paths: bool,
}

impl<'g, S: GraphStorage + ?Sized> ExecutionContext<'g, S> {
    pub fn new(storage: &'g S, interner: &'g StringInterner) -> Self { ... }
    
    pub fn storage(&self) -> &'g S { ... }
    pub fn interner(&self) -> &'g StringInterner { ... }
}
```

#### 2.3.2 Unified `BoundTraversal`

Single traversal type that works with any graph:

```rust
/// A traversal bound to a graph with automatic mutation execution.
/// 
/// This is the main traversal type returned by `graph.traversal().v()`, etc.
/// It provides all 90+ Gremlin step methods and executes mutations when
/// terminal steps are called.
pub struct BoundTraversal<'g, G: Graph, In, Out> {
    graph: &'g G,
    traversal: Traversal<In, Out>,
    track_paths: bool,
}

impl<'g, G: Graph, In> BoundTraversal<'g, G, In, Value> {
    // All 90+ step methods defined HERE, once
    pub fn has_label(self, label: impl Into<String>) -> Self { ... }
    pub fn has(self, key: impl Into<String>) -> Self { ... }
    pub fn out(self) -> Self { ... }
    pub fn in_(self) -> Self { ... }
    pub fn both(self) -> Self { ... }
    pub fn out_e(self) -> Self { ... }
    pub fn in_e(self) -> Self { ... }
    pub fn both_e(self) -> Self { ... }
    pub fn values(self, key: impl Into<String>) -> Self { ... }
    pub fn value_map(self) -> Self { ... }
    pub fn path(self) -> Self { ... }
    pub fn dedup(self) -> Self { ... }
    pub fn order(self) -> Self { ... }
    pub fn group(self) -> Self { ... }
    pub fn where_(self, pred: impl Into<Traversal>) -> Self { ... }
    pub fn repeat(self, traversal: impl Into<Traversal>) -> RepeatBuilder { ... }
    pub fn union(self, traversals: Vec<Traversal>) -> Self { ... }
    pub fn coalesce(self, traversals: Vec<Traversal>) -> Self { ... }
    // ... all other methods
}
```

---

## 3. Implementation Phases

### Phase 1: Make ExecutionContext Generic

**Goal**: Decouple the traversal engine from `GraphSnapshot`.

**Files to modify**:
- `src/traversal/context.rs` - Make `ExecutionContext<'g, S>` generic
- `src/traversal/step.rs` - Update `AnyStep::apply()` signature
- `src/traversal/*.rs` - Update all step implementations

**Changes**:

```rust
// Before (context.rs)
pub struct ExecutionContext<'g> {
    snapshot: &'g GraphSnapshot<'g>,
    interner: &'g StringInterner,
    ...
}

// After
pub struct ExecutionContext<'g, S: GraphStorage + ?Sized = dyn GraphStorage> {
    storage: &'g S,
    interner: &'g StringInterner,
    ...
}
```

**Validation**: All existing tests pass (they use `dyn GraphStorage`).

### Phase 2: Create Core Traits

**Goal**: Define `Graph` and `Snapshot` traits.

**Files to create**:
- `src/graph/traits.rs` - `Graph` and `Snapshot` trait definitions

**Files to modify**:
- `src/graph/mod.rs` - Re-export traits
- `src/lib.rs` - Update prelude

### Phase 3: Implement Traits for COW Types

**Goal**: Make `CowGraph` and `CowMmapGraph` implement the new traits.

**Files to modify**:
- `src/storage/cow.rs` - `impl Graph for CowGraph`
- `src/storage/cow_mmap.rs` - `impl Graph for CowMmapGraph`

**Temporary state**: Both old and new APIs coexist.

### Phase 4: Unify BoundTraversal

**Goal**: Single `BoundTraversal<'g, G: Graph, In, Out>` with all methods.

**Files to create**:
- `src/traversal/bound.rs` - Unified `BoundTraversal` implementation

**Files to modify**:
- `src/traversal/source.rs` - Update `GraphTraversalSource` to use new type
- `src/traversal/mod.rs` - Re-export unified types

**Key insight**: Move all step methods from `source.rs` into `bound.rs`, parameterized over `G: Graph`.

### Phase 5: Update GQL Compiler

**Goal**: Make GQL work with any `Snapshot` impl.

**Files to modify**:
- `src/gql/compiler_legacy.rs` - Change `&GraphSnapshot` to `&dyn Snapshot` or generic
- `src/gql/mod.rs` - Update public API

**Changes**:

```rust
// Before
pub fn compile<'g>(query: &Query, snapshot: &'g GraphSnapshot<'g>) -> ...

// After  
pub fn compile<'g, S: Snapshot>(query: &Query, snapshot: &'g S) -> ...
```

### Phase 6: Rename Types

**Goal**: Clean up naming for the public API.

| Old Name | New Name | Notes |
|----------|----------|-------|
| `CowGraph` | `InMemoryGraph` | Primary in-memory type |
| `CowSnapshot` | `InMemorySnapshot` | Snapshot for in-memory |
| `CowMmapGraph` | `PersistentGraph` | Primary persistent type |
| `CowMmapSnapshot` | `PersistentSnapshot` | Snapshot for persistent |
| `CowTraversalSource` | (deleted) | Merged into `TraversalSource` |
| `CowBoundTraversal` | (deleted) | Merged into `BoundTraversal` |

**Convenience aliases**:

```rust
// In lib.rs or prelude
/// Alias for the default in-memory graph.
pub type Graph = InMemoryGraph;

impl InMemoryGraph {
    /// Create a new in-memory graph.
    pub fn new() -> Self { ... }
}

#[cfg(feature = "mmap")]
impl PersistentGraph {
    /// Open or create a persistent graph at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> { ... }
}
```

### Phase 7: Delete Legacy Types

**Goal**: Remove old API entirely.

**Files to delete or gut**:
- `src/graph.rs` - Delete `Graph`, `GraphSnapshot`, `GraphMut` structs
- `src/storage/inmemory.rs` - Delete (functionality in `InMemoryGraph`)
- `src/traversal/source.rs` - Remove legacy `BoundTraversal` (if not already unified)

**Files to modify**:
- `src/lib.rs` - Remove old re-exports
- `src/prelude.rs` - Update to new types

### Phase 8: Migrate Tests

**Goal**: Update all tests to use new API.

**Files to modify**:
- `tests/common/graphs.rs` - Update `TestGraph` to use `InMemoryGraph`
- `tests/**/*.rs` - Bulk update (mostly mechanical find-replace)

**Migration patterns**:

```rust
// Before
let mut storage = InMemoryGraph::new();
storage.add_vertex("Person", props);
let graph = Graph::new(storage);
let snapshot = graph.snapshot();
let g = snapshot.traversal();

// After
let graph = Graph::new();
let g = graph.traversal();
g.add_v("Person").property("name", "Alice").iterate();
// Or for direct mutations:
graph.add_vertex("Person", props);
let g = graph.traversal();
```

### Phase 9: Migrate Examples

**Goal**: Update all examples to use new API.

**Files to modify**:
- `examples/*.rs` - Update to new patterns
- `examples/cow_unified_api.rs` - Rename to just `basic.rs` or `getting_started.rs`

---

## 4. API Examples

### 4.1 Basic Usage

```rust
use interstellar::prelude::*;

// Create an in-memory graph
let graph = Graph::new();
let g = graph.traversal();

// Add vertices via Gremlin API
let alice = g.add_v("Person")
    .property("name", "Alice")
    .property("age", 30)
    .next()
    .unwrap()
    .as_vertex_id()
    .unwrap();

let bob = g.add_v("Person")
    .property("name", "Bob")
    .property("age", 25)
    .next()
    .unwrap()
    .as_vertex_id()
    .unwrap();

// Add edge
g.add_e("KNOWS")
    .from_id(alice)
    .to_id(bob)
    .property("since", 2020)
    .iterate();

// Query
let names: Vec<Value> = g.v()
    .has_label("Person")
    .values("name")
    .to_list();
assert_eq!(names.len(), 2);

// Complex traversal
let friends_of_alice = g.v_id(alice)
    .out("KNOWS")
    .has_label("Person")
    .values("name")
    .to_list();
assert_eq!(friends_of_alice, vec![Value::from("Bob")]);
```

### 4.2 GQL Usage

```rust
use interstellar::prelude::*;

let graph = Graph::new();

// Mutations via GQL
graph.gql("CREATE (:Person {name: 'Alice', age: 30})")?;
graph.gql("CREATE (:Person {name: 'Bob', age: 25})")?;
graph.gql("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) 
           CREATE (a)-[:KNOWS {since: 2020}]->(b)")?;

// Reads via snapshot
let results = graph.snapshot().gql("MATCH (n:Person) RETURN n.name")?;
assert_eq!(results.len(), 2);
```

### 4.3 Persistent Graph

```rust
use interstellar::prelude::*;

// Open persistent graph (creates if not exists)
let graph = Graph::open("my_database")?;
let g = graph.traversal();

// Mutations are written to WAL immediately
g.add_v("Person").property("name", "Alice").iterate();

// Checkpoint to ensure durability
graph.checkpoint()?;

// Reopen later - data is still there
drop(graph);
let graph = Graph::open("my_database")?;
assert_eq!(graph.vertex_count(), 1);
```

### 4.4 Snapshots for Isolation

```rust
use interstellar::prelude::*;

let graph = Graph::new();
let g = graph.traversal();

g.add_v("Person").property("name", "Alice").iterate();

// Take snapshot
let snap = graph.snapshot();

// Mutate graph
g.add_v("Person").property("name", "Bob").iterate();

// Snapshot still sees old state
assert_eq!(snap.traversal().v().count(), 1);

// Current graph sees new state
assert_eq!(g.v().count(), 2);
```

---

## 5. Migration Checklist

### For Library Code

- [ ] Make `ExecutionContext` generic over `GraphStorage`
- [ ] Create `Graph` and `Snapshot` traits
- [ ] Implement traits for `CowGraph` and `CowMmapGraph`
- [ ] Unify `BoundTraversal` with all 90+ methods
- [ ] Update GQL compiler to use `Snapshot` trait
- [ ] Rename `CowGraph` → `InMemoryGraph`
- [ ] Rename `CowMmapGraph` → `PersistentGraph`
- [ ] Add `Graph::new()` and `Graph::open()` convenience methods
- [ ] Delete legacy `Graph`, `GraphSnapshot`, `GraphMut`
- [ ] Delete legacy `InMemoryGraph` (the old one)
- [ ] Update `lib.rs` exports and prelude

### For Tests

- [ ] Update `tests/common/graphs.rs` fixtures
- [ ] Migrate `tests/gql/*.rs` (largest set)
- [ ] Migrate `tests/traversal/*.rs`
- [ ] Migrate `tests/storage/*.rs`
- [ ] Migrate `tests/rhai*.rs`
- [ ] Delete redundant COW-specific tests (merged into main tests)

### For Examples

- [ ] Migrate `examples/gql.rs`
- [ ] Migrate `examples/nba.rs`
- [ ] Migrate `examples/marvel.rs`
- [ ] Migrate `examples/british_royals.rs`
- [ ] Migrate `examples/indexes.rs`
- [ ] Migrate `examples/rhai_scripting.rs`
- [ ] Merge/rename `examples/cow_unified_api.rs`
- [ ] Merge/rename `examples/cow_mmap_unified_api.rs`

---

## 6. Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Breaking changes in public API | High | High | No backward compat required; clean break |
| Step method parity issues | Medium | Medium | Comprehensive test coverage; run full suite after each phase |
| GQL compiler complexity | Medium | Medium | Refactor compiler first (spec-27) if needed |
| Performance regression | Low | Medium | Benchmark before/after; COW already proven |
| Rhai integration issues | Medium | Low | Rhai has its own traversal source; update separately |

---

## 7. Success Criteria

1. **Single `Graph` type** as the public API entry point
2. **All 90+ traversal steps** available via `graph.traversal()`
3. **Zero code duplication** for step implementations
4. **All tests passing** (including migrated tests)
5. **All examples working** with new API
6. **GQL works** with both in-memory and persistent graphs
7. **No legacy types** exported from `lib.rs`
