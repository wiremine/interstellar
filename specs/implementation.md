# RustGremlin Implementation Plan

A detailed implementation roadmap for the RustGremlin graph traversal library, derived from the design documents in `overview/`.

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

RustGremlin is a high-performance graph traversal library featuring:

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
rustgremlin/
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
│   ├── index/
│   │   ├── mod.rs             # Index traits
│   │   ├── label.rs           # Label → ID index (RoaringBitmap)
│   │   ├── property.rs        # Property B+ tree index
│   │   ├── composite.rs       # Multi-property composite index
│   │   └── fulltext.rs        # Full-text search (Phase 2+)
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
    └──────┬──────┘            └──────┬───────┘            └──────┬───────┘
           │                          │                           │
           │                          ▼                           │
           │                   ┌──────────────┐                   │
           │                   │    index/    │                   │
           │                   │  B+ tree etc │                   │
           │                   └──────┬───────┘                   │
           │                          │                           │
           └──────────────────────────┼───────────────────────────┘
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
**Duration: 3-4 weeks | Priority: Critical**

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
**Duration: 2-3 weeks | Priority: Critical**

Implements the fast, non-persistent storage backend.

#### Deliverables

| File | Description |
|------|-------------|
| `src/storage/inmemory.rs` | `InMemoryGraph` implementation |
| `src/index/mod.rs` | Index trait abstractions |
| `src/index/label.rs` | Label index with RoaringBitmap |

#### Detailed Specifications

**`src/storage/inmemory.rs`**
```rust
pub struct InMemoryGraph {
    nodes: HashMap<VertexId, NodeData>,
    edges: HashMap<EdgeId, EdgeData>,
    next_vertex_id: AtomicU64,
    next_edge_id: AtomicU64,
    
    // Indexes
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

impl GraphStorage for InMemoryGraph {
    // O(1) vertex/edge lookup
    // O(degree) adjacency traversal
    // O(n) label scans via RoaringBitmap
}

impl InMemoryGraph {
    pub fn new() -> Self;
    pub fn add_vertex(&mut self, label: &str, properties: HashMap<String, Value>) -> VertexId;
    pub fn add_edge(&mut self, src: VertexId, dst: VertexId, label: &str, properties: HashMap<String, Value>) -> Result<EdgeId, StorageError>;
    pub fn remove_vertex(&mut self, id: VertexId) -> Result<(), StorageError>;
    pub fn remove_edge(&mut self, id: EdgeId) -> Result<(), StorageError>;
}
```

#### Exit Criteria
- [ ] InMemoryGraph implements GraphStorage
- [ ] O(1) vertex/edge lookup verified
- [ ] Label indexes work correctly
- [ ] Add/remove operations update indexes
- [ ] Integration test with 10K vertices, 100K edges

---

### Phase 3: Traversal Engine Core
**Duration: 4-5 weeks | Priority: Critical**

The heart of the library - implements the Gremlin-style fluent API.

#### Deliverables

| File | Description |
|------|-------------|
| `src/traversal/mod.rs` | Core types: `Traversal`, `Traverser`, `Path` |
| `src/traversal/step.rs` | `Step` trait and composition |
| `src/traversal/source.rs` | `GraphTraversalSource`, `V()`, `E()` |
| `src/traversal/filter.rs` | Filter steps |
| `src/traversal/map.rs` | Navigation and transform steps |
| `src/traversal/terminal.rs` | Terminal steps |

#### Detailed Specifications

**`src/traversal/mod.rs`**
```rust
/// Main traversal builder - monomorphized for zero-cost
pub struct Traversal<S, E, Steps> {
    source: S,
    steps: Steps,
    _phantom: PhantomData<E>,
}

/// Traverser carries element + metadata through pipeline
#[derive(Clone)]
pub struct Traverser<E> {
    pub element: E,
    pub path: Path,
    pub loops: u32,
    pub sack: Option<Box<dyn Any + Send>>,
    pub bulk: u64,
}

/// Path tracks traversal history
#[derive(Clone, Default)]
pub struct Path {
    objects: Vec<PathElement>,
    labels: HashMap<String, Vec<usize>>,
}

#[derive(Clone)]
pub struct PathElement {
    value: Value,
    labels: SmallVec<[String; 2]>,
}
```

**`src/traversal/step.rs`**
```rust
/// Core trait for traversal steps
pub trait Step<In, Out>: Clone {
    type Iter: Iterator<Item = Traverser<Out>>;
    
    fn apply<I>(self, input: I) -> Self::Iter
    where
        I: Iterator<Item = Traverser<In>>;
}

/// Compose two steps into a single step
pub struct Compose<S1, S2> {
    step1: S1,
    step2: S2,
}

impl<S1, S2, A, B, C> Step<A, C> for Compose<S1, S2>
where
    S1: Step<A, B>,
    S2: Step<B, C>,
{
    // Nested iterator composition
}
```

**`src/traversal/source.rs`**
```rust
pub struct GraphTraversalSource<'g> {
    graph: &'g Graph,
}

impl<'g> GraphTraversalSource<'g> {
    pub fn v(self) -> Traversal<Self, Vertex, impl Step<(), Vertex>>;
    pub fn v_by_ids(self, ids: impl IntoIterator<Item = VertexId>) -> Traversal<...>;
    pub fn e(self) -> Traversal<Self, Edge, impl Step<(), Edge>>;
    pub fn e_by_ids(self, ids: impl IntoIterator<Item = EdgeId>) -> Traversal<...>;
    pub fn inject<T>(self, values: impl IntoIterator<Item = T>) -> Traversal<...>;
}
```

**`src/traversal/filter.rs`**
```rust
// Step implementations
pub struct HasLabelStep { labels: Vec<u32> }
pub struct HasStep { key: String, predicate: Box<dyn Predicate> }
pub struct HasValueStep { key: String, value: Value }
pub struct WhereStep<Sub> { sub: Sub }
pub struct NotStep<Sub> { sub: Sub }
pub struct DedupStep<F, K> { key_fn: F, _phantom: PhantomData<K> }
pub struct LimitStep { n: usize }
pub struct SkipStep { n: usize }
pub struct RangeStep { start: usize, end: usize }
pub struct FilterStep<F> { predicate: F }

// Traversal builder methods
impl<S, E, Steps> Traversal<S, E, Steps> {
    pub fn has_label(self, label: &str) -> Traversal<...>;
    pub fn has_label_any(self, labels: &[&str]) -> Traversal<...>;
    pub fn has(self, key: &str) -> Traversal<...>;
    pub fn has_value(self, key: &str, value: impl Into<Value>) -> Traversal<...>;
    pub fn has_where<P: Predicate>(self, key: &str, predicate: P) -> Traversal<...>;
    pub fn where_<Sub>(self, sub: Sub) -> Traversal<...>;
    pub fn not<Sub>(self, sub: Sub) -> Traversal<...>;
    pub fn dedup(self) -> Traversal<...>;
    pub fn limit(self, n: usize) -> Traversal<...>;
    pub fn filter<F: Fn(&E) -> bool>(self, f: F) -> Traversal<...>;
}
```

**`src/traversal/map.rs`**
```rust
// Navigation steps (Vertex → Vertex)
pub struct OutStep { label_filters: Option<Vec<u32>> }
pub struct InStep { label_filters: Option<Vec<u32>> }
pub struct BothStep { label_filters: Option<Vec<u32>> }

// Edge navigation (Vertex → Edge)
pub struct OutEStep { label_filters: Option<Vec<u32>> }
pub struct InEStep { label_filters: Option<Vec<u32>> }
pub struct BothEStep { label_filters: Option<Vec<u32>> }

// Edge to Vertex
pub struct OutVStep;
pub struct InVStep;
pub struct BothVStep;
pub struct OtherVStep;

// Property access
pub struct ValuesStep { keys: Vec<String> }
pub struct ValueMapStep { keys: Option<Vec<String>> }
pub struct ElementMapStep;
pub struct IdStep;
pub struct LabelStep;

// Transform
pub struct MapStep<F> { f: F }
pub struct FlatMapStep<F> { f: F }
pub struct ConstantStep<V> { value: V }
pub struct PathStep;
pub struct SelectStep { labels: Vec<String> }
```

**`src/traversal/terminal.rs`**
```rust
impl<S, E, Steps> Traversal<S, E, Steps>
where
    Steps: Step<?, E>,
{
    pub fn to_list(self) -> Vec<E>;
    pub fn to_set(self) -> HashSet<E> where E: Eq + Hash;
    pub fn next(self) -> Option<E>;
    pub fn one(self) -> Result<E, TraversalError>;
    pub fn has_next(self) -> bool;
    pub fn iterate(self);
    pub fn take(self, n: usize) -> Vec<E>;
    pub fn iter(self) -> impl Iterator<Item = E>;
    pub fn count(self) -> u64;
}
```

#### Exit Criteria
- [ ] Basic traversals compile and execute: `g.v().has_label("person").out().to_list()`
- [ ] Path tracking works correctly
- [ ] Lazy evaluation verified (no work until terminal)
- [ ] All filter steps work with predicates
- [ ] Navigation steps handle label filtering
- [ ] Integration tests for common query patterns

---

### Phase 4: Predicates & Anonymous Traversals
**Duration: 2-3 weeks | Priority: High**

Enables expressive filtering and composable traversal fragments.

#### Deliverables

| File | Description |
|------|-------------|
| `src/traversal/predicate.rs` | Predicate system |
| `src/traversal/anonymous.rs` | `__` factory module |

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
```rust
/// Factory for anonymous traversals
pub struct __;

impl __ {
    // Identity
    pub fn identity<E>() -> AnonymousTraversal<E, E>;
    pub fn constant<E, V: Clone>(value: V) -> AnonymousTraversal<E, V>;
    
    // Navigation (mirrors bound traversal API)
    pub fn out() -> AnonymousTraversal<Vertex, Vertex>;
    pub fn out_labels(labels: &[&str]) -> AnonymousTraversal<Vertex, Vertex>;
    pub fn in_() -> AnonymousTraversal<Vertex, Vertex>;
    pub fn in_labels(labels: &[&str]) -> AnonymousTraversal<Vertex, Vertex>;
    pub fn both() -> AnonymousTraversal<Vertex, Vertex>;
    pub fn both_labels(labels: &[&str]) -> AnonymousTraversal<Vertex, Vertex>;
    
    pub fn out_e() -> AnonymousTraversal<Vertex, Edge>;
    pub fn in_e() -> AnonymousTraversal<Vertex, Edge>;
    pub fn both_e() -> AnonymousTraversal<Vertex, Edge>;
    
    pub fn out_v() -> AnonymousTraversal<Edge, Vertex>;
    pub fn in_v() -> AnonymousTraversal<Edge, Vertex>;
    pub fn both_v() -> AnonymousTraversal<Edge, Vertex>;
    
    // Properties
    pub fn values(key: &str) -> AnonymousTraversal<Element, Value>;
    pub fn label() -> AnonymousTraversal<Element, String>;
    pub fn id() -> AnonymousTraversal<Element, ElementId>;
    
    // Filtering
    pub fn has(key: &str) -> AnonymousTraversal<Element, Element>;
    pub fn has_value(key: &str, value: impl Into<Value>) -> AnonymousTraversal<Element, Element>;
    pub fn has_label(label: &str) -> AnonymousTraversal<Element, Element>;
    
    // Reduce (for use in where_ checks)
    pub fn count() -> AnonymousTraversal<Any, u64>;
    
    // Loop access
    pub fn loops() -> AnonymousTraversal<Any, u32>;
}

/// Anonymous traversal that can be chained
pub struct AnonymousTraversal<In, Out> {
    steps: Vec<Box<dyn AnyStep>>,
    _phantom: PhantomData<(In, Out)>,
}

impl<In, Out> AnonymousTraversal<In, Out> {
    // All the same builder methods as Traversal
    pub fn out(self) -> AnonymousTraversal<In, Vertex>;
    pub fn has_value(self, key: &str, value: impl Into<Value>) -> Self;
    // ... etc
}
```

#### Exit Criteria
- [ ] All predicates work with `has_where()`
- [ ] Anonymous traversals compile: `__.out().has_value("name", "Bob")`
- [ ] `where_()` accepts anonymous traversals
- [ ] Logical predicate composition works
- [ ] Regex predicate compiles patterns correctly

---

### Phase 5: Branch & Reduce Steps
**Duration: 3-4 weeks | Priority: High**

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
- [ ] `union()` merges results in traverser-major order
- [ ] `coalesce()` short-circuits on first success
- [ ] `repeat().times(n)` iterates exactly n times
- [ ] `repeat().until()` stops on condition
- [ ] `repeat().emit()` yields all intermediate results
- [ ] Aggregation steps produce correct results
- [ ] Path labeling with `as_()` and `select()` works

---

### Phase 6: Memory-Mapped Storage
**Duration: 4-5 weeks | Priority: Medium**

Implements persistent storage with memory-mapped files.

#### Deliverables

| File | Description |
|------|-------------|
| `src/storage/records.rs` | On-disk record formats |
| `src/storage/arena.rs` | Property arena allocation |
| `src/storage/wal.rs` | Write-ahead logging |
| `src/storage/mmap.rs` | `MmapGraph` implementation |

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
    label_index: LabelIndex,
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
- [ ] MmapGraph opens/creates database files
- [ ] Records are correctly aligned (48/56 bytes)
- [ ] WAL logs operations with fsync
- [ ] Crash recovery replays committed transactions
- [ ] Free list manages deleted slots
- [ ] Property arena stores variable-length data
- [ ] Integration test: create, close, reopen, verify data

---

### Phase 7: Indexes & Optimization
**Duration: 3-4 weeks | Priority: Medium**

Adds secondary indexes and query optimization.

#### Deliverables

| File | Description |
|------|-------------|
| `src/index/property.rs` | B+ tree property index |
| `src/index/composite.rs` | Composite index |
| `src/traversal/optimizer.rs` | Query planner |

#### Detailed Specifications

**`src/index/property.rs`**
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
- [ ] B+ tree supports insert/delete/lookup/range
- [ ] Property indexes accelerate `has_value()` queries
- [ ] Query planner pushes filters down
- [ ] Statistics collection works
- [ ] Benchmark: indexed vs non-indexed query speedup

---

### Phase 8: Mutations & Polish
**Duration: 2-3 weeks | Priority: Medium**

Finalizes the mutation API and public interface.

#### Deliverables

| File | Description |
|------|-------------|
| `src/graph.rs` | `GraphMut` mutations |
| `src/lib.rs` | Final public API |
| Documentation | README, rustdoc |

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
//! RustGremlin: A Fluent Graph Traversal Library

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
- [ ] Mutation API compiles and works
- [ ] Transactions commit/rollback correctly
- [ ] Public API is clean and well-documented
- [ ] Examples in documentation compile
- [ ] All phases integrated and working together

---

## Technical Decisions

### 1. Type System Strategy

**Decision: Hybrid approach (Option C)**

- Monomorphize hot paths (navigation steps, filters)
- Use `Box<dyn Step>` for complex branches (`union`, `repeat`)
- Balance binary size vs performance

```rust
// Hot path: fully monomorphized
pub struct OutStep { ... }
impl<In> Step<Traverser<Vertex>, Traverser<Vertex>> for OutStep { ... }

// Complex branch: boxed for flexibility
pub struct UnionStep {
    branches: Vec<Box<dyn AnyStep>>,
}
```

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

| Phase | Duration | Cumulative | Milestone |
|-------|----------|------------|-----------|
| **Phase 1**: Core Foundation | 3-4 weeks | Week 4 | Core types compile |
| **Phase 2**: In-Memory Storage | 2-3 weeks | Week 7 | Basic CRUD works |
| **Phase 3**: Traversal Engine | 4-5 weeks | Week 12 | **MVP: Basic queries work** |
| **Phase 4**: Predicates & Anonymous | 2-3 weeks | Week 15 | Expressive filtering |
| **Phase 5**: Branch & Reduce | 3-4 weeks | Week 19 | Complex queries work |
| **Phase 6**: Memory-Mapped | 4-5 weeks | Week 24 | Persistence works |
| **Phase 7**: Indexes & Optimization | 3-4 weeks | Week 28 | Performance optimized |
| **Phase 8**: Mutations & Polish | 2-3 weeks | Week 31 | **v1.0 Release** |

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
use rustgremlin::prelude::*;

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
    let g = graph.traversal();
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
