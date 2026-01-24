# Interstellar Implementation Plan

A detailed implementation roadmap for the Interstellar graph traversal library, derived from the design documents in `overview/`.

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Architecture Overview](#architecture-overview)
3. [Implementation Phases](#implementation-phases)
4. [Module Specifications](#module-specifications)
5. [Technical Decisions](#technical-decisions)
6. [Testing Strategy](#testing-strategy)
7. [Timeline & Milestones](#timeline--milestones)

---

## Executive Summary

Interstellar is a high-performance graph traversal library featuring:

- **Dual Storage Architecture**: In-memory (HashMap-based) and memory-mapped (persistent) backends
- **Gremlin-Style Fluent API**: Chainable traversal steps with lazy iterator-based execution
- **Anonymous Traversals**: Composable, reusable traversal fragments via the `__` factory module
- **Concurrency Model**: RwLock-based reader-writer access with WAL durability for persistence
- **Zero-Cost Abstractions**: Monomorphized traversal pipelines for optimal performance

### Design Principles

1. **Unified API**: Both storage backends expose identical traversal interfaces
2. **Lazy Evaluation**: Pull-based iterator model with no computation until terminal steps
3. **Type Safety**: Compile-time verification of traversal step compatibility
4. **Memory Efficiency**: O(1) memory per active pipeline, not result set size
5. **Portability**: Single-file database format with cross-platform support

---

## Architecture Overview

### High-Level Module Structure

```
interstellar/
├── Cargo.toml
├── src/
│   ├── lib.rs                 # Public API exports, prelude
│   ├── graph.rs               # Graph, GraphSnapshot, GraphMut
│   ├── value.rs               # Value enum, VertexId, EdgeId
│   ├── error.rs               # Error types
│   │
│   ├── storage/
│   │   ├── mod.rs             # GraphStorage trait, dispatch
│   │   ├── inmemory.rs        # HashMap-based storage
│   │   ├── mmap.rs            # Memory-mapped file storage
│   │   ├── records.rs         # On-disk record formats
│   │   ├── arena.rs           # Property/string allocation
│   │   ├── wal.rs             # Write-ahead logging
│   │   └── interner.rs        # String interning
│   │
│   ├── traversal/
│   │   ├── mod.rs             # Traversal, Traverser, Path
│   │   ├── source.rs          # GraphTraversalSource, V(), E()
│   │   ├── step.rs            # Step trait, composition
│   │   ├── filter.rs          # has(), hasLabel(), where_(), dedup()
│   │   ├── map.rs             # out(), in_(), both(), values()
│   │   ├── branch.rs          # union(), coalesce(), choose(), repeat()
│   │   ├── reduce.rs          # count(), sum(), group(), fold()
│   │   ├── sideeffect.rs      # store(), aggregate(), as_()
│   │   ├── terminal.rs        # toList(), next(), iterate()
│   │   ├── predicate.rs       # Predicate trait, p:: module
│   │   ├── anonymous.rs       # __ factory module
│   │   └── optimizer.rs       # Query planning (Phase 7)
│   │
│   └── algorithms/            # Graph algorithms (Phase 3+)
│       ├── mod.rs
│       ├── bfs.rs
│       ├── dfs.rs
│       └── path.rs
│
├── benches/                   # Criterion benchmarks
│   └── traversal.rs
│
└── tests/                     # Integration tests
    ├── inmemory.rs
    ├── mmap.rs
    └── traversal.rs
```

### Module Dependency Graph

```
                              ┌─────────────────┐
                              │     lib.rs      │
                              │  (public API)   │
                              └────────┬────────┘
                                       │
           ┌───────────────────────────┼───────────────────────────┐
           │                           │                           │
           ▼                           ▼                           ▼
    ┌─────────────┐            ┌──────────────┐            ┌──────────────┐
    │   graph.rs  │◀───────────│   storage/   │            │  traversal/  │
    │ Graph types │            │   backends   │            │  Fluent API  │
    └──────┬──────┘            └──────────────┘            └──────┬───────┘
           │                                                       │
           └───────────────────────────────────────────────────────┘
                                       │
                                       ▼
                               ┌──────────────┐
                               │  value.rs    │
                               │  error.rs    │
                               └──────────────┘
```

---

## Implementation Phases

### Phase 1: Core Foundation
**Duration: 3-4 weeks | Priority: Critical | Status: ✅ Complete**

Establishes the fundamental types and traits upon which all other modules depend.

#### Deliverables

| File | Description | Key Types |
|------|-------------|-----------|
| `src/value.rs` | Core value types | `Value`, `VertexId`, `EdgeId`, `ElementId` |
| `src/error.rs` | Error hierarchy | `StorageError`, `TraversalError` |
| `src/storage/mod.rs` | Storage abstraction | `GraphStorage` trait |
| `src/storage/interner.rs` | String deduplication | `StringInterner` |
| `src/graph.rs` | Graph handle types | `Graph`, `GraphSnapshot`, `GraphMut` |
| `src/lib.rs` | Initial exports | `prelude` module |

#### Detailed Specifications

**`src/value.rs`**
```rust
// Core identifiers
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Ord, PartialOrd)]
pub struct VertexId(pub(crate) u64);

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Ord, PartialOrd)]
pub struct EdgeId(pub(crate) u64);

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ElementId {
    Vertex(VertexId),
    Edge(EdgeId),
}

// Property value type
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<Value>),
    Map(HashMap<String, Value>),
}

// Implement From<T> for common types
// Implement serialization (binary format per algorithms.md)
// Implement ComparableValue for index ordering
```

**`src/error.rs`**
```rust
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("vertex not found: {0:?}")]
    VertexNotFound(VertexId),
    #[error("edge not found: {0:?}")]
    EdgeNotFound(EdgeId),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("WAL corrupted: {0}")]
    WalCorrupted(String),
    #[error("invalid file format")]
    InvalidFormat,
}

#[derive(Debug, thiserror::Error)]
pub enum TraversalError {
    #[error("expected exactly one result, found {0}")]
    NotOne(usize),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}
```

**`src/storage/mod.rs`**
```rust
pub trait GraphStorage: Send + Sync {
    // Vertex operations
    fn get_vertex(&self, id: VertexId) -> Option<Vertex>;
    fn vertex_count(&self) -> u64;
    
    // Edge operations  
    fn get_edge(&self, id: EdgeId) -> Option<Edge>;
    fn edge_count(&self) -> u64;
    
    // Adjacency traversal
    fn out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_>;
    fn in_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_>;
    
    // Label-based access
    fn vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Vertex> + '_>;
    fn edges_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Edge> + '_>;
    
    // All elements
    fn all_vertices(&self) -> Box<dyn Iterator<Item = Vertex> + '_>;
    fn all_edges(&self) -> Box<dyn Iterator<Item = Edge> + '_>;
}
```

#### Exit Criteria
- [x] All types compile with proper derives
- [x] Value serialization round-trips correctly
- [x] StringInterner handles interning and resolution
- [x] GraphStorage trait is finalized
- [x] Unit tests for Value conversions

---

### Phase 2: In-Memory Storage
**Duration: 2-3 weeks | Priority: Critical | Status: ✅ Complete**

Implements the fast, non-persistent storage backend.

#### Deliverables

| File | Description |
|------|-------------|
| `src/storage/inmemory.rs` | `Graph` implementation |

#### Detailed Specifications

**`src/storage/inmemory.rs`**
```rust
pub struct Graph {
    nodes: HashMap<VertexId, NodeData>,
    edges: HashMap<EdgeId, EdgeData>,
    next_vertex_id: AtomicU64,
    next_edge_id: AtomicU64,
    
    // Indexes (inline - no separate index module)
    vertex_labels: HashMap<u32, RoaringBitmap>,
    edge_labels: HashMap<u32, RoaringBitmap>,
    
    // String interning
    string_table: StringInterner,
}

struct NodeData {
    id: VertexId,
    label_id: u32,
    properties: HashMap<String, Value>,
    out_edges: Vec<EdgeId>,
    in_edges: Vec<EdgeId>,
}

struct EdgeData {
    id: EdgeId,
    label_id: u32,
    src: VertexId,
    dst: VertexId,
    properties: HashMap<String, Value>,
}

impl GraphStorage for Graph {
    // O(1) vertex/edge lookup
    // O(degree) adjacency traversal
    // O(n) label scans via RoaringBitmap
}

impl Graph {
    pub fn new() -> Self;
    pub fn add_vertex(&mut self, label: &str, properties: HashMap<String, Value>) -> VertexId;
    pub fn add_edge(&mut self, src: VertexId, dst: VertexId, label: &str, properties: HashMap<String, Value>) -> Result<EdgeId, StorageError>;
    pub fn remove_vertex(&mut self, id: VertexId) -> Result<(), StorageError>;
    pub fn remove_edge(&mut self, id: EdgeId) -> Result<(), StorageError>;
}
```

#### Exit Criteria
- [x] Graph implements GraphStorage
- [x] O(1) vertex/edge lookup verified
- [x] Label indexes work correctly (inline implementation)
- [x] Add/remove operations update indexes
- [x] Integration test with 10K vertices, 100K edges

---

### Phase 3: Traversal Engine Core
**Duration: 4-5 weeks | Priority: Critical | Status: ✅ Complete**

The heart of the library - implements the Gremlin-style fluent API using **type-erased steps** internally while maintaining **compile-time type safety** at API boundaries.

#### Architecture

The traversal engine uses `Box<dyn AnyStep>` internally for flexibility while `Traversal<In, Out>` provides type-safe APIs:

- **Unified Traversal type**: Same `Traversal<In, Out>` for both bound and anonymous traversals
- **ExecutionContext**: Graph access passed at execution time, not construction time
- **BoundTraversal wrapper**: Holds graph references for bound traversals
- **Phase 4 ready**: Anonymous traversals (`__`) work seamlessly

#### Deliverables

| File | Description |
|------|-------------|
| `src/traversal/mod.rs` | Core types: `Traversal`, `Traverser`, `Path`, re-exports |
| `src/traversal/context.rs` | `ExecutionContext`, `SideEffects` |
| `src/traversal/step.rs` | `AnyStep` trait and helper macros |
| `src/traversal/source.rs` | `GraphTraversalSource`, `BoundTraversal`, `StartStep` |
| `src/traversal/filter.rs` | Filter steps: `has_label`, `has`, `dedup`, `limit`, etc. |
| `src/traversal/navigation.rs` | Navigation steps: `out`, `in_`, `both`, `outE`, etc. |
| `src/traversal/transform.rs` | Transform steps: `values`, `id`, `label`, `map`, etc. |
| `src/traversal/terminal.rs` | Terminal steps: `to_list`, `next`, `count`, etc. |

#### Detailed Specifications

**`src/traversal/context.rs`**
```rust
/// Execution context passed to steps at runtime
/// Key to supporting anonymous traversals - graph access provided at execution time
pub struct ExecutionContext<'g> {
    pub snapshot: &'g GraphSnapshot<'g>,
    pub interner: &'g StringInterner,
    pub side_effects: SideEffects,
}

impl<'g> ExecutionContext<'g> {
    pub fn resolve_label(&self, label: &str) -> Option<u32>;
    pub fn resolve_labels(&self, labels: &[&str]) -> Vec<u32>;
    pub fn get_label(&self, id: u32) -> Option<&str>;
}

/// Storage for traversal side effects (store, aggregate, sack)
pub struct SideEffects {
    collections: HashMap<String, Vec<Value>>,
    data: HashMap<String, Box<dyn Any + Send + Sync>>,
}
```

**`src/traversal/mod.rs`**
```rust
/// Main traversal type - type-erased internally, type-safe externally
/// Same type for bound and anonymous traversals
pub struct Traversal<In, Out> {
    steps: Vec<Box<dyn AnyStep>>,
    source: Option<TraversalSource>,
    _phantom: PhantomData<fn(In) -> Out>,
}

/// Traverser carries a Value through the pipeline with metadata
/// Uses Value internally (not generic E) to enable type erasure
#[derive(Clone)]
pub struct Traverser {
    pub value: Value,
    pub path: Path,
    pub loops: u32,
    pub sack: Option<Box<dyn CloneSack>>,
    pub bulk: u64,
}

/// Path tracks traversal history
#[derive(Clone, Default)]
pub struct Path {
    objects: Vec<PathElement>,
    labels: HashMap<String, Vec<usize>>,
}

pub enum TraversalSource {
    AllVertices,
    Vertices(Vec<VertexId>),
    AllEdges,
    Edges(Vec<EdgeId>),
    Inject(Vec<Value>),
}
```

**`src/traversal/step.rs`**
```rust
/// Type-erased step trait enabling Box<dyn AnyStep> storage
pub trait AnyStep: Send + Sync {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a>;
    
    fn clone_box(&self) -> Box<dyn AnyStep>;
    fn name(&self) -> &'static str;
}

// Helper macros for implementing AnyStep
macro_rules! impl_filter_step { ... }
macro_rules! impl_flatmap_step { ... }
```

**`src/traversal/source.rs`**
```rust
/// Entry point for bound traversals
pub struct GraphTraversalSource<'g> {
    snapshot: &'g GraphSnapshot<'g>,
    interner: &'g StringInterner,
}

impl<'g> GraphTraversalSource<'g> {
    pub fn v(&self) -> BoundTraversal<'g, (), Value>;
    pub fn v_ids<I>(&self, ids: I) -> BoundTraversal<'g, (), Value>;
    pub fn e(&self) -> BoundTraversal<'g, (), Value>;
    pub fn e_ids<I>(&self, ids: I) -> BoundTraversal<'g, (), Value>;
    pub fn inject<T, I>(&self, values: I) -> BoundTraversal<'g, (), Value>;
}

/// Wrapper holding traversal + graph references for execution
pub struct BoundTraversal<'g, In, Out> {
    snapshot: &'g GraphSnapshot<'g>,
    interner: &'g StringInterner,
    traversal: Traversal<In, Out>,
}

impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    pub fn add_step<NewOut>(self, step: impl AnyStep) -> BoundTraversal<'g, In, NewOut>;
    pub fn append<Mid>(self, anon: Traversal<Out, Mid>) -> BoundTraversal<'g, In, Mid>;
    pub fn execute(self) -> impl Iterator<Item = Traverser> + 'g;
}
```

**`src/traversal/filter.rs`**
```rust
// Filter steps - check conditions, pass through or reject
pub struct HasLabelStep { labels: Vec<String> }
pub struct HasStep { key: String }
pub struct HasValueStep { key: String, value: Value }
pub struct HasIdStep { ids: Vec<Value> }
pub struct FilterStep<F> { predicate: F }  // F: Fn(&ExecutionContext, &Value) -> bool
pub struct DedupStep;
pub struct LimitStep { n: usize }
pub struct SkipStep { n: usize }
pub struct RangeStep { start: usize, end: usize }

// Builder methods
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    pub fn has_label(self, label: &str) -> BoundTraversal<'g, In, Out>;
    pub fn has_label_any(self, labels: &[&str]) -> BoundTraversal<'g, In, Out>;
    pub fn has(self, key: &str) -> BoundTraversal<'g, In, Out>;
    pub fn has_value(self, key: &str, value: impl Into<Value>) -> BoundTraversal<'g, In, Out>;
    pub fn filter<F>(self, predicate: F) -> BoundTraversal<'g, In, Out>;
    pub fn dedup(self) -> BoundTraversal<'g, In, Out>;
    pub fn limit(self, n: usize) -> BoundTraversal<'g, In, Out>;
    pub fn skip(self, n: usize) -> BoundTraversal<'g, In, Out>;
    pub fn range(self, start: usize, end: usize) -> BoundTraversal<'g, In, Out>;
}
```

**`src/traversal/navigation.rs`**
```rust
// Navigation steps - traverse graph structure
pub struct OutStep { labels: Vec<String> }
pub struct InStep { labels: Vec<String> }
pub struct BothStep { labels: Vec<String> }
pub struct OutEStep { labels: Vec<String> }
pub struct InEStep { labels: Vec<String> }
pub struct BothEStep { labels: Vec<String> }
pub struct OutVStep;
pub struct InVStep;
pub struct BothVStep;

// Builder methods
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    pub fn out(self) -> BoundTraversal<'g, In, Value>;
    pub fn out_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value>;
    pub fn in_(self) -> BoundTraversal<'g, In, Value>;
    pub fn in_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value>;
    pub fn both(self) -> BoundTraversal<'g, In, Value>;
    pub fn out_e(self) -> BoundTraversal<'g, In, Value>;
    pub fn in_e(self) -> BoundTraversal<'g, In, Value>;
    pub fn out_v(self) -> BoundTraversal<'g, In, Value>;
    pub fn in_v(self) -> BoundTraversal<'g, In, Value>;
    pub fn both_v(self) -> BoundTraversal<'g, In, Value>;
}
```

**`src/traversal/transform.rs`**
```rust
// Transform steps - map values to different types
pub struct ValuesStep { keys: Vec<String> }
pub struct IdStep;
pub struct LabelStep;
pub struct MapStep<F> { f: F }       // F: Fn(&ExecutionContext, &Value) -> Value
pub struct FlatMapStep<F> { f: F }   // F: Fn(&ExecutionContext, &Value) -> Vec<Value>
pub struct ConstantStep { value: Value }
pub struct PathStep;

// Builder methods
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    pub fn values(self, key: &str) -> BoundTraversal<'g, In, Value>;
    pub fn values_multi(self, keys: &[&str]) -> BoundTraversal<'g, In, Value>;
    pub fn id(self) -> BoundTraversal<'g, In, Value>;
    pub fn label(self) -> BoundTraversal<'g, In, Value>;
    pub fn map<F>(self, f: F) -> BoundTraversal<'g, In, Value>;
    pub fn flat_map<F>(self, f: F) -> BoundTraversal<'g, In, Value>;
    pub fn constant(self, value: impl Into<Value>) -> BoundTraversal<'g, In, Value>;
    pub fn path(self) -> BoundTraversal<'g, In, Value>;
}
```

**`src/traversal/terminal.rs`**
```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    pub fn to_list(self) -> Vec<Value>;
    pub fn to_set(self) -> HashSet<Value>;
    pub fn next(self) -> Option<Value>;
    pub fn one(self) -> Result<Value, TraversalError>;
    pub fn has_next(self) -> bool;
    pub fn iterate(self);
    pub fn take(self, n: usize) -> Vec<Value>;
    pub fn count(self) -> u64;
    pub fn sum(self) -> Value;
    pub fn min(self) -> Option<Value>;
    pub fn max(self) -> Option<Value>;
    pub fn fold<B, F>(self, init: B, f: F) -> B;
    pub fn iter(self) -> impl Iterator<Item = Value>;
    pub fn traversers(self) -> impl Iterator<Item = Traverser>;
}
```

#### Anonymous Traversals (Phase 4 Preview)

The architecture supports anonymous traversals seamlessly:

```rust
/// Anonymous traversal factory (Phase 4)
pub mod __ {
    pub fn out() -> Traversal<Value, Value>;
    pub fn out_labels(labels: &[&str]) -> Traversal<Value, Value>;
    pub fn in_() -> Traversal<Value, Value>;
    pub fn has_label(label: &str) -> Traversal<Value, Value>;
    pub fn has_value(key: &str, value: impl Into<Value>) -> Traversal<Value, Value>;
    pub fn values(key: &str) -> Traversal<Value, Value>;
    pub fn identity() -> Traversal<Value, Value>;
    pub fn constant(value: impl Into<Value>) -> Traversal<Value, Value>;
}

// Usage with bound traversals
g.v().has_label("person")
    .append(__.out_labels(&["knows"]))
    .to_list();
```

#### Exit Criteria
- [x] All core types compile (`Traversal`, `Traverser`, `Path`, `ExecutionContext`)
- [x] `AnyStep` trait works with type erasure
- [x] `GraphTraversalSource` with `v()` and `e()` starting points
- [x] `BoundTraversal` wrapper correctly manages execution context
- [x] Navigation steps work: `out()`, `in_()`, `both()`, `out_e()`, `in_e()`, `out_v()`, `in_v()`
- [x] Filter steps work: `has_label()`, `has()`, `has_value()`, `filter()`, `dedup()`, `limit()`, `skip()`, `range()`
- [x] Transform steps work: `values()`, `id()`, `label()`, `map()`, `flat_map()`, `constant()`, `path()`
- [x] Terminal steps work: `to_list()`, `to_set()`, `next()`, `one()`, `has_next()`, `iterate()`, `count()`, `sum()`, `min()`, `max()`
- [x] Lazy evaluation verified (no work until terminal step)
- [x] Path tracking works correctly
- [x] Label resolution works via ExecutionContext
- [x] Anonymous traversals can be appended to bound traversals
- [x] All unit and integration tests pass
- [x] Benchmarks run successfully

---

### Phase 4: Predicates & Anonymous Traversals
**Duration: 2-3 weeks | Priority: High | Status: ✅ Complete**

Enables expressive filtering and composable traversal fragments. Builds on Phase 3's unified `Traversal<In, Out>` type.

#### Deliverables

| File | Description |
|------|-------------|
| `src/traversal/predicate.rs` | Predicate system |
| `src/traversal/anonymous.rs` | `__` factory module (extends Phase 3 preview) |
| `src/traversal/branch.rs` | Branching steps: `where_`, `not`, `and_`, `or_` |

#### Detailed Specifications

**`src/traversal/predicate.rs`**
```rust
pub trait Predicate: Clone + Send + Sync {
    fn test(&self, value: &Value) -> bool;
}

pub mod p {
    // Comparison
    pub fn eq<T: Into<Value>>(value: T) -> impl Predicate;
    pub fn neq<T: Into<Value>>(value: T) -> impl Predicate;
    pub fn lt<T: Into<Value>>(value: T) -> impl Predicate;
    pub fn lte<T: Into<Value>>(value: T) -> impl Predicate;
    pub fn gt<T: Into<Value>>(value: T) -> impl Predicate;
    pub fn gte<T: Into<Value>>(value: T) -> impl Predicate;
    
    // Range
    pub fn between<T: Into<Value>>(start: T, end: T) -> impl Predicate;
    pub fn inside<T: Into<Value>>(start: T, end: T) -> impl Predicate;
    pub fn outside<T: Into<Value>>(start: T, end: T) -> impl Predicate;
    
    // Collection
    pub fn within<T: Into<Value>>(values: impl IntoIterator<Item = T>) -> impl Predicate;
    pub fn without<T: Into<Value>>(values: impl IntoIterator<Item = T>) -> impl Predicate;
    
    // String
    pub fn containing(substring: &str) -> impl Predicate;
    pub fn starting_with(prefix: &str) -> impl Predicate;
    pub fn ending_with(suffix: &str) -> impl Predicate;
    pub fn regex(pattern: &str) -> impl Predicate;
    
    // Logical composition
    pub fn and<P1: Predicate, P2: Predicate>(p1: P1, p2: P2) -> impl Predicate;
    pub fn or<P1: Predicate, P2: Predicate>(p1: P1, p2: P2) -> impl Predicate;
    pub fn not<P: Predicate>(p: P) -> impl Predicate;
}
```

**`src/traversal/anonymous.rs`**

The `__` module uses the same `Traversal<In, Out>` type as bound traversals:

```rust
/// Anonymous traversal factory
/// 
/// Creates Traversal<In, Out> instances without a graph binding.
/// These receive ExecutionContext when spliced into parent traversals.
pub mod __ {
    use super::*;

    // Navigation
    pub fn out() -> Traversal<Value, Value>;
    pub fn out_labels(labels: &[&str]) -> Traversal<Value, Value>;
    pub fn in_() -> Traversal<Value, Value>;
    pub fn in_labels(labels: &[&str]) -> Traversal<Value, Value>;
    pub fn both() -> Traversal<Value, Value>;
    
    pub fn out_e() -> Traversal<Value, Value>;
    pub fn in_e() -> Traversal<Value, Value>;
    pub fn out_v() -> Traversal<Value, Value>;
    pub fn in_v() -> Traversal<Value, Value>;
    
    // Properties
    pub fn values(key: &str) -> Traversal<Value, Value>;
    pub fn label() -> Traversal<Value, Value>;
    pub fn id() -> Traversal<Value, Value>;
    
    // Filtering
    pub fn has(key: &str) -> Traversal<Value, Value>;
    pub fn has_value(key: &str, value: impl Into<Value>) -> Traversal<Value, Value>;
    pub fn has_label(label: &str) -> Traversal<Value, Value>;
    
    // Utility
    pub fn identity() -> Traversal<Value, Value>;
    pub fn constant(value: impl Into<Value>) -> Traversal<Value, Value>;
    pub fn count() -> Traversal<Value, Value>;
    pub fn loops() -> Traversal<Value, Value>;
}

// Anonymous traversals chain like bound traversals
impl<In, Out> Traversal<In, Out> {
    pub fn out(self) -> Traversal<In, Value>;
    pub fn has_label(self, label: &str) -> Traversal<In, Out>;
    pub fn has_value(self, key: &str, value: impl Into<Value>) -> Traversal<In, Out>;
    // ... all the same builder methods
}
```

**`src/traversal/branch.rs`** (Filtering with Anonymous Traversals)
```rust
/// Filter steps that use anonymous traversals
pub struct WhereStep { sub: Traversal<Value, Value> }
pub struct NotStep { sub: Traversal<Value, Value> }
pub struct AndStep { subs: Vec<Traversal<Value, Value>> }
pub struct OrStep { subs: Vec<Traversal<Value, Value>> }
pub struct HasWhereStep { key: String, predicate: Box<dyn Predicate> }

impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Filter by sub-traversal producing results
    pub fn where_(self, sub: Traversal<Value, Value>) -> BoundTraversal<'g, In, Out>;
    
    /// Filter by sub-traversal NOT producing results  
    pub fn not(self, sub: Traversal<Value, Value>) -> BoundTraversal<'g, In, Out>;
    
    /// All sub-traversals must produce results
    pub fn and_(self, subs: Vec<Traversal<Value, Value>>) -> BoundTraversal<'g, In, Out>;
    
    /// At least one sub-traversal must produce results
    pub fn or_(self, subs: Vec<Traversal<Value, Value>>) -> BoundTraversal<'g, In, Out>;
    
    /// Filter by property with predicate
    pub fn has_where<P: Predicate>(self, key: &str, predicate: P) -> BoundTraversal<'g, In, Out>;
}
```

#### Key Design Note

Anonymous traversals use the **same `Traversal<In, Out>` type** as bound traversals:

| Aspect | Bound Traversal | Anonymous Traversal |
|--------|-----------------|---------------------|
| Type | `BoundTraversal<'g, In, Out>` | `Traversal<In, Out>` |
| Has source? | Yes (via wrapper) | No |
| Graph access | Via `BoundTraversal` wrapper | Via `ExecutionContext` at splice |
| `In` type | `()` (starts from nothing) | Input element type |

#### Exit Criteria
- [x] All predicates work with `has_where()`
- [x] Anonymous traversals compile: `__.out().has_value("name", "Bob")`
- [x] `where_()` accepts anonymous traversals
- [x] `not()`, `and_()`, `or_()` work correctly
- [x] Anonymous traversals chain: `__.out().has_label("person").values("name")`
- [x] Logical predicate composition works
- [x] Regex predicate compiles patterns correctly

---

### Phase 5: Branch & Reduce Steps
**Duration: 3-4 weeks | Priority: High | Status: Partially Complete**

Implements complex control flow and aggregation.

#### Deliverables

| File | Description |
|------|-------------|
| `src/traversal/branch.rs` | Branch steps |
| `src/traversal/reduce.rs` | Aggregation steps |
| `src/traversal/sideeffect.rs` | Side-effect steps |

#### Detailed Specifications

**`src/traversal/branch.rs`**
```rust
// Union: execute all branches, merge results
pub struct UnionStep<Branches> { branches: Branches }

// Coalesce: first non-empty branch wins
pub struct CoalesceStep<Branches> { branches: Branches }

// Choose: conditional branching
pub struct ChooseStep<Cond, IfTrue, IfFalse> {
    condition: Cond,
    if_true: IfTrue,
    if_false: IfFalse,
}

// Optional: try sub-traversal, keep original if empty
pub struct OptionalStep<Sub> { sub: Sub }

// Repeat: iterative traversal
pub struct RepeatStep<Sub> { sub: Sub }

pub struct RepeatTraversal<S, E, Sub> {
    traversal: Traversal<S, E, ...>,
    sub: Sub,
    times: Option<u32>,
    until: Option<Box<dyn Predicate>>,
    emit: EmitStrategy,
}

enum EmitStrategy {
    None,
    All,
    Conditional(Box<dyn Predicate>),
}

impl<S, E, Sub> RepeatTraversal<S, E, Sub> {
    pub fn times(self, n: u32) -> Traversal<...>;
    pub fn until<P: Predicate>(self, predicate: P) -> Traversal<...>;
    pub fn emit(self) -> Self;
    pub fn emit_if<P: Predicate>(self, predicate: P) -> Self;
}

// Local: isolated scope execution
pub struct LocalStep<Sub> { sub: Sub }
```

**`src/traversal/reduce.rs`**
```rust
pub struct CountStep;
pub struct SumStep;
pub struct MinStep;
pub struct MaxStep;
pub struct MeanStep;

pub struct GroupStep<F, K> { key_fn: F, _phantom: PhantomData<K> }
pub struct GroupCountStep<F, K> { key_fn: F, _phantom: PhantomData<K> }
pub struct FoldStep<A, F> { init: A, f: F }
pub struct ToListStep;
pub struct ToSetStep;

impl<S, E, Steps> Traversal<S, E, Steps> {
    pub fn count(self) -> Traversal<..., u64, ...>;
    pub fn sum(self) -> Traversal<..., f64, ...> where E: Into<f64>;
    pub fn min(self) -> Traversal<..., E, ...> where E: Ord;
    pub fn max(self) -> Traversal<..., E, ...> where E: Ord;
    pub fn mean(self) -> Traversal<..., f64, ...> where E: Into<f64>;
    
    pub fn group<F, K>(self, key_fn: F) -> Traversal<..., HashMap<K, Vec<E>>, ...>
    where F: Fn(&E) -> K, K: Eq + Hash;
    
    pub fn group_count<F, K>(self, key_fn: F) -> Traversal<..., HashMap<K, u64>, ...>
    where F: Fn(&E) -> K, K: Eq + Hash;
    
    pub fn fold<A, F>(self, init: A, f: F) -> Traversal<..., A, ...>
    where F: Fn(A, E) -> A;
}
```

**`src/traversal/sideeffect.rs`**
```rust
pub struct StoreStep { key: String }
pub struct AggregateStep { key: String }
pub struct SideEffectStep<F> { f: F }
pub struct AsStep { label: String }
pub struct PropertyStep { key: String, value: Value }
pub struct DropStep;

impl<S, E, Steps> Traversal<S, E, Steps> {
    pub fn store(self, key: &str) -> Self;
    pub fn aggregate(self, key: &str) -> Self;
    pub fn side_effect<F: Fn(&E)>(self, f: F) -> Self;
    pub fn as_(self, label: &str) -> Self;
    pub fn property(self, key: &str, value: impl Into<Value>) -> Self;
    pub fn drop(self) -> Traversal<..., (), ...>;
}
```

#### Exit Criteria
- [x] `union()` merges results in traverser-major order
- [x] `coalesce()` short-circuits on first success
- [x] `repeat().times(n)` iterates exactly n times
- [x] `repeat().until()` stops on condition
- [x] `repeat().emit()` yields all intermediate results
- [x] Aggregation steps produce correct results (`group()`, `groupCount()`, `mean()`)
- [x] Path labeling with `as_()` and `select()` works

---

### Phase 6: Memory-Mapped Storage
**Duration: 4-5 weeks | Priority: Medium | Status: ✅ Complete**

Implements persistent storage with memory-mapped files.

**Note**: Label indexing in mmap storage will use the same inline `HashMap<u32, RoaringBitmap>` approach as in-memory storage. Property indexes and composite indexes will be added in Phase 7 as optional secondary indexes.

#### Deliverables

| File | Description |
|------|-------------|
| `src/storage/mmap/records.rs` | On-disk record formats |
| `src/storage/mmap/arena.rs` | Property arena allocation |
| `src/storage/mmap/wal.rs` | Write-ahead logging |
| `src/storage/mmap/mod.rs` | `MmapGraph` implementation |
| `src/storage/mmap/freelist.rs` | Deleted slot reuse |
| `src/storage/mmap/recovery.rs` | Crash recovery |
| `src/storage/cow_mmap.rs` | COW wrapper for snapshots |

#### Detailed Specifications

**`src/storage/records.rs`**
```rust
/// File header (64 bytes)
#[repr(C, packed)]
pub struct FileHeader {
    pub magic: u32,              // 0x47524D4C ("GRML")
    pub version: u32,
    pub node_count: u64,
    pub node_capacity: u64,
    pub edge_count: u64,
    pub edge_capacity: u64,
    pub string_table_offset: u64,
    pub property_arena_offset: u64,
    pub free_node_head: u64,
    pub free_edge_head: u64,
}

/// Node record (48 bytes, cache-line friendly)
#[repr(C, packed)]
pub struct NodeRecord {
    pub id: u64,
    pub label_id: u32,
    pub flags: u32,
    pub first_out_edge: u64,
    pub first_in_edge: u64,
    pub prop_head: u64,
}

/// Edge record (56 bytes)
#[repr(C, packed)]
pub struct EdgeRecord {
    pub id: u64,
    pub label_id: u32,
    pub _padding: u32,
    pub src: u64,
    pub dst: u64,
    pub next_out: u64,
    pub next_in: u64,
    pub prop_head: u64,
}

// Flags
pub const FLAG_DELETED: u32 = 0x0001;
pub const FLAG_HAS_INDEX: u32 = 0x0002;
```

**`src/storage/wal.rs`**
```rust
#[derive(Serialize, Deserialize)]
pub enum WalEntry {
    BeginTx { tx_id: u64 },
    InsertNode { id: VertexId, record: NodeRecord },
    InsertEdge { id: EdgeId, record: EdgeRecord },
    UpdateProperty { element: ElementId, key: u32, old: Value, new: Value },
    DeleteNode { id: VertexId },
    DeleteEdge { id: EdgeId },
    CommitTx { tx_id: u64 },
    AbortTx { tx_id: u64 },
    Checkpoint { version: u64 },
}

pub struct WriteAheadLog {
    file: File,
    buffer: Vec<u8>,
    next_tx_id: u64,
}

impl WriteAheadLog {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self>;
    pub fn begin_transaction(&mut self) -> io::Result<u64>;
    pub fn log(&mut self, entry: WalEntry) -> io::Result<u64>;
    pub fn sync(&mut self) -> io::Result<()>;
    pub fn recover(&mut self, storage: &mut MmapStorage) -> io::Result<()>;
}
```

**`src/storage/mmap.rs`**
```rust
pub struct MmapGraph {
    mmap: Mmap,
    file: File,
    wal: WriteAheadLog,
    
    // Label indexes (inline, same as Graph)
    vertex_labels: HashMap<u32, RoaringBitmap>,
    edge_labels: HashMap<u32, RoaringBitmap>,
    
    string_table: StringInterner,
    lock: RwLock<()>,
}

impl MmapGraph {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError>;
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, StorageError>;
    
    // O(1) lookups via direct array access
    fn get_node(&self, id: VertexId) -> Option<&NodeRecord>;
    fn get_edge(&self, id: EdgeId) -> Option<&EdgeRecord>;
    
    // Linked list traversal
    fn out_edges(&self, vertex: VertexId) -> OutEdgeIter<'_>;
    fn in_edges(&self, vertex: VertexId) -> InEdgeIter<'_>;
}

impl GraphStorage for MmapGraph {
    // Implement all trait methods
}
```

#### Exit Criteria
- [x] MmapGraph opens/creates database files
- [x] Records are correctly aligned (48/56 bytes)
- [x] WAL logs operations with fsync
- [x] Crash recovery replays committed transactions
- [x] Free list manages deleted slots
- [x] Property arena stores variable-length data
- [x] Integration test: create, close, reopen, verify data

---

### Phase 7: Indexes & Optimization
**Duration: 3-4 weeks | Priority: Medium | Status: ✅ Substantially Complete**

Adds optional secondary indexes for property queries and query optimization.

**Note**: Primary label indexes are already implemented inline in both storage backends. This phase adds optional property and composite indexes for accelerating specific query patterns.

**Implementation Note**: Indexes are implemented in `src/index/` module rather than `src/storage/`. Composite indexes and dedicated query optimizer are deferred as future extensions.

#### Deliverables

| File | Description |
|------|-------------|
| `src/index/btree.rs` | B+ tree property index |
| `src/index/unique.rs` | Unique index with hash-based O(1) lookup |
| `src/index/traits.rs` | `PropertyIndex` trait definition |
| `src/index/spec.rs` | `IndexSpec`, `IndexBuilder`, `IndexPredicate` |
| `src/index/error.rs` | `IndexError` enum |

#### Detailed Specifications

**`src/storage/property_index.rs`**
```rust
pub struct PropertyIndex {
    root: Option<NodeId>,
    order: usize,  // B+ tree order (128-256)
    nodes: Vec<BTreeNode>,
}

enum BTreeNode {
    Internal {
        keys: Vec<IndexKey>,
        children: Vec<NodeId>,
    },
    Leaf {
        keys: Vec<IndexKey>,
        values: Vec<RoaringBitmap>,
        next_leaf: Option<NodeId>,
    },
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq)]
struct IndexKey {
    label_id: u32,
    property_key_id: u32,
    value: ComparableValue,
}

impl PropertyIndex {
    pub fn get(&self, key: &IndexKey) -> Option<&RoaringBitmap>;
    pub fn range(&self, start: &IndexKey, end: &IndexKey) -> impl Iterator<Item = (&IndexKey, &RoaringBitmap)>;
    pub fn insert(&mut self, key: IndexKey, element_id: u64);
    pub fn remove(&mut self, key: &IndexKey, element_id: u64);
}
```

**`src/traversal/optimizer.rs`**
```rust
pub struct QueryPlanner<'g> {
    graph: &'g Graph,
    statistics: Statistics,
}

pub struct Statistics {
    vertex_count: u64,
    edge_count: u64,
    label_counts: HashMap<u32, u64>,
    avg_out_degree: f64,
    avg_in_degree: f64,
}

impl<'g> QueryPlanner<'g> {
    pub fn optimize<S, E, Steps>(traversal: Traversal<S, E, Steps>) -> Traversal<S, E, OptimizedSteps>;
}

// Optimization rules:
// 1. Push filters down (filter early)
// 2. Combine adjacent has() steps
// 3. Eliminate redundant dedup()
// 4. Convert to index lookups where possible
// 5. Reorder for optimal selectivity
```

#### Exit Criteria
- [x] B+ tree supports insert/delete/lookup/range
- [x] Property indexes accelerate `has_value()` queries
- [ ] Query planner pushes filters down (deferred as future extension)
- [x] Statistics collection works
- [x] Benchmark: indexed vs non-indexed query speedup

---

### Phase 8: Mutations & Polish
**Duration: 2-3 weeks | Priority: Medium | Status: ✅ Complete (Alternative Architecture)**

Finalizes the mutation API and public interface.

**Implementation Note**: The architecture evolved beyond the original spec to use:
1. **COW-based Graph** (`crate::storage::Graph`) instead of RwLock-based `GraphMut`
2. **Traversal-based mutations** via `g.add_v()`, `g.add_e()` with lazy evaluation
3. **GQL mutations** via `graph.gql()` with CREATE/SET/DELETE/MERGE/FOREACH
4. **Unified trait-based API** (`UnifiedGraph`, `UnifiedSnapshot`)

#### Deliverables

| File | Description |
|------|-------------|
| `src/traversal/source.rs` | `GraphTraversalSource::add_v()`, `add_e()` |
| `src/traversal/mutation.rs` | `AddVStep`, `AddEStep`, `PropertyStep`, `DropStep`, `MutationExecutor` |
| `src/gql/mutation.rs` | GQL mutation compiler and executor |
| `src/lib.rs` | Final public API with prelude |
| `src/graph.rs` | `LegacyGraphMut` (deprecated), `UnifiedGraph` trait |

#### Detailed Specifications

**`src/graph.rs` (additions)**
```rust
impl<'g> GraphMut<'g> {
    pub fn add_v(self, label: &str) -> MutationBuilder<Vertex>;
    pub fn add_e(self, label: &str) -> EdgeBuilder;
    pub fn commit(self) -> Result<(), StorageError>;
    pub fn rollback(self);
}

pub struct MutationBuilder<E> {
    graph: *mut Graph,
    label: String,
    properties: HashMap<String, Value>,
}

impl MutationBuilder<Vertex> {
    pub fn property(mut self, key: &str, value: impl Into<Value>) -> Self;
    pub fn properties(mut self, props: HashMap<String, Value>) -> Self;
    pub fn build(self) -> VertexId;
}

pub struct EdgeBuilder {
    graph: *mut Graph,
    label: String,
    src: Option<VertexId>,
    dst: Option<VertexId>,
    properties: HashMap<String, Value>,
}

impl EdgeBuilder {
    pub fn from(mut self, src: VertexId) -> Self;
    pub fn to(mut self, dst: VertexId) -> Self;
    pub fn property(mut self, key: &str, value: impl Into<Value>) -> Self;
    pub fn build(self) -> EdgeId;
}
```

**`src/lib.rs`**
```rust
//! Interstellar: A Fluent Graph Traversal Library

pub mod graph;
pub mod value;
pub mod error;
pub mod storage;
pub mod index;
pub mod traversal;

pub mod prelude {
    pub use crate::graph::{Graph, GraphSnapshot, GraphMut};
    pub use crate::value::{Value, VertexId, EdgeId, ElementId};
    pub use crate::error::{StorageError, TraversalError};
    pub use crate::traversal::{
        Traversal, Traverser, Path,
        GraphTraversalSource,
        p, __,
    };
}

// Re-export main types at crate root
pub use prelude::*;
```

#### Exit Criteria
- [x] Mutation API compiles and works (`add_v()`, `add_e()`, `property()`, `drop()`)
- [x] Transactions commit/rollback correctly (WAL-based for mmap)
- [x] Public API is clean and well-documented
- [x] Examples in documentation compile
- [x] All phases integrated and working together

---

## Technical Decisions

### 1. Type System Strategy

**Decision: Type-erased steps with type-safe API**

- Steps are stored as `Box<dyn AnyStep>` for flexibility
- `Traversal<In, Out>` provides compile-time type safety at API boundaries
- Unified type for both bound and anonymous traversals

```rust
// Type-erased step trait
pub trait AnyStep: Send + Sync {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a>;
    
    fn clone_box(&self) -> Box<dyn AnyStep>;
}

// Type-safe traversal wrapper
pub struct Traversal<In, Out> {
    steps: Vec<Box<dyn AnyStep>>,  // Type-erased internally
    _phantom: PhantomData<fn(In) -> Out>,  // Type-safe externally
}
```

**Trade-offs:**
- Pros: Unified traversal type, simpler API, easy cloning for branching
- Cons: Virtual dispatch overhead (typically negligible vs I/O)
- Hot paths can be re-implemented with monomorphization later if needed

### 2. Storage Trait Design

**Decision: Associated types for core iteration, boxed for flexibility**

```rust
pub trait GraphStorage {
    type VertexIter<'a>: Iterator<Item = Vertex> where Self: 'a;
    type EdgeIter<'a>: Iterator<Item = Edge> where Self: 'a;
    
    fn out_edges(&self, v: VertexId) -> Self::EdgeIter<'_>;
    
    // Fallback for dynamic dispatch
    fn out_edges_boxed(&self, v: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        Box::new(self.out_edges(v))
    }
}
```

### 3. Concurrency Primitives

**Decision: Use `parking_lot` from the start**

```toml
[dependencies]
parking_lot = "0.12"
```

Design `GraphSnapshot` to be MVCC-ready:
```rust
pub struct GraphSnapshot<'g> {
    graph: &'g Graph,
    version: u64,  // For future MVCC
    _guard: RwLockReadGuard<'g, ()>,
}
```

### 4. String Interning

**Decision: Per-storage string table**

Each storage instance maintains its own `StringInterner`. This:
- Enables portable mmap files
- Avoids global state
- Simplifies multi-graph scenarios

### 5. Error Handling

**Decision: Result-based, no panics in library code**

- Use `thiserror` for error types
- All fallible operations return `Result`
- Panics only in tests or for internal invariant violations

### 6. Feature Flags

```toml
[features]
default = ["inmemory"]
inmemory = []
mmap = ["memmap2"]
full-text = ["tantivy"]  # Phase 2+
```

---

## Testing Strategy

### Unit Tests

Each module includes inline unit tests:

```rust
// src/storage/inmemory.rs
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_add_vertex() { ... }
    
    #[test]
    fn test_add_edge() { ... }
    
    #[test]
    fn test_out_edges() { ... }
}
```

### Integration Tests

`tests/` directory contains cross-module tests:

```rust
// tests/traversal.rs
#[test]
fn test_basic_traversal() {
    let graph = Graph::in_memory();
    // Build test graph
    let g = graph.traversal();
    let results = g.v().has_label("person").out("knows").to_list();
    assert_eq!(results.len(), expected);
}

#[test]
fn test_complex_query() {
    // Friends of friends who work at same company
}
```

### Property-Based Tests

Use `proptest` for storage correctness:

```rust
proptest! {
    #[test]
    fn roundtrip_value(value: Value) {
        let mut buf = Vec::new();
        value.serialize(&mut buf);
        let parsed = Value::deserialize(&buf, &mut 0).unwrap();
        assert_eq!(value, parsed);
    }
}
```

### Benchmarks

`benches/traversal.rs` using Criterion:

```rust
fn bench_traversal(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);
    
    c.bench_function("simple_traversal", |b| {
        b.iter(|| {
            graph.traversal().v().has_label("person").out().count()
        })
    });
    
    c.bench_function("complex_traversal", |b| {
        b.iter(|| {
            graph.traversal()
                .v().has_label("person")
                .where_(__.out("knows").has_value("age", p::gt(30)))
                .out("works_at")
                .dedup()
                .count()
        })
    });
}
```

---

## Timeline & Milestones

| Phase | Duration | Cumulative | Milestone | Status |
|-------|----------|------------|-----------|--------|
| **Phase 1**: Core Foundation | 3-4 weeks | Week 4 | Core types compile | ✅ Complete |
| **Phase 2**: In-Memory Storage | 2-3 weeks | Week 7 | Basic CRUD works | ✅ Complete |
| **Phase 3**: Traversal Engine | 4-5 weeks | Week 12 | **MVP: Basic queries work** | ✅ Complete |
| **Phase 4**: Predicates & Anonymous | 2-3 weeks | Week 15 | Expressive filtering | ✅ Complete |
| **Phase 5**: Branch & Reduce | 3-4 weeks | Week 19 | Complex queries work | ✅ Complete |
| **Phase 6**: Memory-Mapped | 4-5 weeks | Week 24 | Persistence works | ✅ Complete |
| **Phase 7**: Indexes & Optimization | 3-4 weeks | Week 28 | Performance optimized | ✅ Substantially Complete |
| **Phase 8**: Mutations & Polish | 2-3 weeks | Week 31 | **v1.0 Release** | ✅ Complete |

### Current Status: v1.0 Feature Complete

All 8 phases are now complete. The library provides:
- Dual storage backends (in-memory + persistent mmap)
- Full Gremlin-style traversal API
- Property indexes (B+ tree and Unique)
- COW-based snapshots for concurrent access
- GQL mutation support
- 885 tests passing

### Minimum Viable Product (MVP)

Phases 1-3 deliver a functional in-memory graph database:
- In-memory storage with label indexes
- Core Gremlin traversal API
- Navigation, filtering, and terminal steps
- ~12 weeks to MVP

### Full v1.0

All 8 phases deliver the complete vision:
- Dual storage (in-memory + persistent)
- Full Gremlin API
- Indexes and query optimization
- ~31 weeks total

---

## Dependencies

```toml
[dependencies]
# Core
thiserror = "1.0"
hashbrown = "0.14"
smallvec = "1.11"

# Serialization
serde = { version = "1.0", features = ["derive"] }
bincode = "1.3"

# Concurrency
parking_lot = "0.12"

# Indexes
roaring = "0.10"

# Memory-mapped storage (optional)
memmap2 = { version = "0.9", optional = true }

# CRC for WAL integrity
crc32fast = "1.3"

[dev-dependencies]
criterion = "0.5"
proptest = "1.4"
tempfile = "3.10"
```

---

## Risks & Mitigations

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Complex type system issues | Medium | High | Prototype tricky generics early |
| Memory-mapped complexity | Medium | Medium | Defer to Phase 6, simple impl first |
| Performance regressions | Low | Medium | Benchmark suite from Phase 3 |
| API design mistakes | Medium | High | Review against Gremlin spec regularly |
| Scope creep | High | Medium | Strict phase boundaries, MVP focus |

---

## Appendix: API Examples

### Basic Usage

```rust
use interstellar::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create in-memory graph
    let graph = Graph::in_memory();
    
    // Add data
    {
        let mut g = graph.mutate();
        let alice = g.add_v("person")
            .property("name", "Alice")
            .property("age", 30)
            .build();
        let bob = g.add_v("person")
            .property("name", "Bob")
            .property("age", 35)
            .build();
        g.add_e("knows")
            .from(alice)
            .to(bob)
            .property("since", 2020)
            .build();
        g.commit()?;
    }
    
    // Query
    let snap = graph.snapshot();
    let g = snap.traversal();
    let friends: Vec<String> = g.v()
        .has_value("name", "Alice")
        .out_labels(&["knows"])
        .values("name")
        .map(|v| v.as_string().unwrap().clone())
        .to_list();
    
    println!("Alice's friends: {:?}", friends);
    Ok(())
}
```

### Complex Query

```rust
// Find people who know someone over 30 and work at a tech company
let results = g.v()
    .has_label("person")
    .where_(__.out_labels(&["knows"]).has_where("age", p::gt(30)))
    .where_(__.out_labels(&["works_at"]).has_where("industry", p::eq("tech")))
    .value_map()
    .to_list();
```

### Graph Algorithms

```rust
// Find all vertices within 3 hops
let nearby = g.v_by_ids([start_id])
    .repeat(__.out())
    .times(3)
    .emit()
    .dedup()
    .to_list();

// Shortest path (BFS)
let path = g.v_by_ids([start_id])
    .repeat(__.out().simple_path())
    .until(__.has_id(target_id))
    .path()
    .limit(1)
    .next();
```
