# Intersteller: A Fluent Graph Traversal Library

A high-performance, portable Rust library providing a Gremlin-style fluent API for graph traversals, supporting both in-memory and memory-mapped storage backends.

---

## 1. Backend Architecture

### 1.1 Storage Layer: Dual Storage Architecture

Intersteller supports **two storage modes** to accommodate different use cases:

- **In-Memory Storage**: HashMap-based, maximum performance, no persistence
- **Memory-Mapped Storage**: Persistent files via `memmap2`, larger capacity, WAL durability

Both storage backends expose the same unified Graph API, allowing seamless switching without code changes.

```
┌─────────────────────────────────────────────────────────────────┐
│                   Storage Architecture                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────┐         ┌──────────────────────┐     │
│  │   In-Memory Graph    │         │   On-Disk Graph      │     │
│  ├──────────────────────┤         ├──────────────────────┤     │
│  │ • HashMap-based      │         │ • Memory-mapped      │     │
│  │ • No persistence     │         │ • Persistent         │     │
│  │ • Fastest access     │         │ • Larger capacity    │     │
│  │ • Limited by RAM     │         │ • Page cache assist  │     │
│  └──────────────────────┘         └──────────────────────┘     │
│           │                                 │                   │
│           └────────────┬────────────────────┘                   │
│                        ▼                                        │
│            ┌─────────────────────┐                              │
│            │   Unified Graph API  │                             │
│            │  (same traversal     │                             │
│            │   interface)         │                             │
│            └─────────────────────┘                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

For details on storage implementations, see [storage.md](./storage.md).

### 1.2 Core Data Structures

```rust
/// Vertex identifier - strongly typed for safety
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct VertexId(pub(crate) u64);

/// Edge identifier - strongly typed for safety
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct EdgeId(pub(crate) u64);

/// Property values supported by the graph
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

/// On-disk node record (48 bytes, cache-line friendly)
#[repr(C, packed)]
struct NodeRecord {
    id: u64,
    label_id: u32,
    flags: u32,
    first_out_edge: u64,
    first_in_edge: u64,
    prop_head: u64,
}

/// On-disk edge record (56 bytes)
#[repr(C, packed)]
struct EdgeRecord {
    id: u64,
    label_id: u32,
    _padding: u32,
    src: u64,
    dst: u64,
    next_out: u64,
    next_in: u64,
    prop_head: u64,
}
```

### 1.3 Index Structures

```
┌─────────────────────────────────────────────────────────────────┐
│                     Index Architecture                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Primary Indexes (inline in storage backends):                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ • Node ID → NodeData           (HashMap lookup)         │   │
│  │ • Edge ID → EdgeData           (HashMap lookup)         │   │
│  │ • Label → Node/Edge ID set     (HashMap<u32, Bitmap>)   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Secondary Indexes (optional, future):                          │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ • Property Index: (label, key, value) → Element IDs     │   │
│  │   Implementation: B+ tree for range queries             │   │
│  │                                                         │   │
│  │ • Composite Index: (label, key₁, key₂, ...) → IDs       │   │
│  │   Implementation: Concatenated key B+ tree              │   │
│  │                                                         │   │
│  │ • Full-text Index: text property → Element IDs          │   │
│  │   Implementation: Inverted index                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Adjacency Structure (embedded in records):                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Node ──out_edges──→ Vec<EdgeId>                         │   │
│  │      ←─in_edges───   Vec<EdgeId>                        │   │
│  │                                                         │   │
│  │ Vec-based edge lists per vertex for O(degree) iteration │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 1.4 Concurrency Model

```rust
/// Thread-safe graph handle with RwLock-based concurrency
pub struct Graph {
    storage: Arc<dyn GraphStorage>,
    lock: Arc<RwLock<()>>,
}

/// Read-only snapshot for consistent traversals
pub struct GraphSnapshot<'g> {
    graph: &'g Graph,
    version: u64,
    _guard: RwLockReadGuard<'g, ()>,
}

/// Mutable transaction with write buffering
pub struct GraphMut<'g> {
    graph: &'g Graph,
    _guard: RwLockWriteGuard<'g, ()>,
}

impl Graph {
    pub fn snapshot(&self) -> GraphSnapshot<'_>;
    pub fn mutate(&self) -> GraphMut<'_>;
    pub fn try_mutate(&self) -> Option<GraphMut<'_>>; // Non-blocking write lock
}

impl<'g> GraphSnapshot<'g> {
    pub fn traversal(&self) -> GraphTraversalSource<'_>; // Traversal is on snapshot
}
```

**Concurrency guarantees:**
- Multiple concurrent readers via `RwLock`
- Single writer with buffered writes
- Atomic commit via write-ahead log (WAL)
- Snapshots see consistent graph state

### 1.5 Module Structure

```
intersteller/
├── src/
│   ├── lib.rs              # Public API exports
│   ├── graph.rs            # Graph, GraphSnapshot, GraphMut
│   ├── storage/
│   │   ├── mod.rs
│   │   ├── inmemory.rs     # HashMap-based in-memory storage
│   │   ├── interner.rs     # String interning
│   │   ├── mmap.rs         # Memory-mapped file handling (future)
│   │   ├── records.rs      # On-disk record formats (future)
│   │   ├── arena.rs        # Property/string allocation (future)
│   │   └── wal.rs          # Write-ahead logging (future)
│   ├── traversal/
│   │   ├── mod.rs          # Core types: Traversal, Traverser, Path
│   │   ├── context.rs      # ExecutionContext, SideEffects
│   │   ├── step.rs         # AnyStep trait, helper macros
│   │   ├── source.rs       # GraphTraversalSource, BoundTraversal, StartStep
│   │   ├── filter.rs       # has(), hasLabel(), dedup(), limit() (future)
│   │   ├── navigation.rs   # out(), in(), both(), outE(), inE() (future)
│   │   ├── transform.rs    # values(), id(), label(), map() (future)
│   │   ├── branch.rs       # union(), coalesce(), choose(), where_() (future)
│   │   ├── reduce.rs       # count(), sum(), fold() (future)
│   │   ├── sideeffect.rs   # store(), aggregate() (future)
│   │   └── terminal.rs     # toList(), next(), iterate() (future)
│   ├── value.rs            # Value enum and conversions
│   ├── error.rs            # Error types
│   └── algorithms/         # Graph algorithms (future)
│       └── mod.rs
└── Cargo.toml
```

**Note**: Label indexes are implemented inline within storage backends (`InMemoryGraph` and future `MmapGraph`) using `HashMap<u32, RoaringBitmap>`. Optional property indexes will be added as separate modules in future phases for advanced query optimization.

---

## 2. Gremlin-Style Fluent API

### 2.1 Core Traversal Types

The traversal engine uses **type-erased steps** (`Box<dyn AnyStep>`) internally while maintaining **compile-time type safety** at API boundaries through `Traversal<In, Out>`.

```rust
/// Main traversal type - type-erased internally, type-safe externally
/// Same type for both bound and anonymous traversals
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

/// Execution context passed to steps at runtime
/// Key to supporting anonymous traversals - graph access provided at execution time
pub struct ExecutionContext<'g> {
    pub snapshot: &'g GraphSnapshot<'g>,
    pub interner: &'g StringInterner,
    pub side_effects: SideEffects,
}

/// Traversal source - entry point for bound traversals
pub struct GraphTraversalSource<'g> {
    snapshot: &'g GraphSnapshot<'g>,
    interner: &'g StringInterner,
}

/// Wrapper for traversals bound to a graph
pub struct BoundTraversal<'g, In, Out> {
    snapshot: &'g GraphSnapshot<'g>,
    interner: &'g StringInterner,
    traversal: Traversal<In, Out>,
}

impl<'g> GraphTraversalSource<'g> {
    // Created from GraphSnapshot via snapshot.traversal()
}
```

### Key Design: Bound vs Anonymous Traversals

Both use the **same `Traversal<In, Out>` type**. The difference:

| Aspect | Bound Traversal | Anonymous Traversal |
|--------|-----------------|---------------------|
| Type | `BoundTraversal<'g, In, Out>` | `Traversal<In, Out>` |
| Creation | `g.v()` | `__.out()` |
| Has source? | Yes (via wrapper) | No |
| Graph access | Via `BoundTraversal` wrapper | Via `ExecutionContext` at splice |
| `In` type | `()` (starts from nothing) | Input element type |
| Execution | Direct (has context) | Must be spliced into parent |
```

### 2.2 Source Steps

```rust
impl<'g> GraphTraversalSource<'g> {
    /// Start traversal from all vertices
    pub fn v(&self) -> BoundTraversal<'g, (), Value>;
    
    /// Start from specific vertex IDs
    pub fn v_ids<I>(&self, ids: I) -> BoundTraversal<'g, (), Value>
    where
        I: IntoIterator<Item = VertexId>;
    
    /// Start traversal from all edges  
    pub fn e(&self) -> BoundTraversal<'g, (), Value>;
    
    /// Start from specific edge IDs
    pub fn e_ids<I>(&self, ids: I) -> BoundTraversal<'g, (), Value>
    where
        I: IntoIterator<Item = EdgeId>;
    
    /// Inject arbitrary values into traversal
    pub fn inject<T, I>(&self, values: I) -> BoundTraversal<'g, (), Value>
    where
        I: IntoIterator<Item = T>,
        T: Into<Value>;
}
```

### 2.3 Filter Steps

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Filter by label
    pub fn has_label(self, label: &str) -> Self;
    pub fn has_label_any(self, labels: &[&str]) -> Self;
    
    /// Filter by property existence
    pub fn has(self, key: &str) -> Self;
    
    /// Filter by property value
    pub fn has_value(self, key: &str, value: impl Into<Value>) -> Self;
    
    /// Filter by property predicate
    pub fn has_where<P: Predicate>(self, key: &str, predicate: P) -> Self;
    
    /// Filter by id
    pub fn has_id(self, id: impl Into<ElementId>) -> Self;
    
    /// Generic filter with closure
    pub fn filter<F>(self, predicate: F) -> Self
    where
        F: Fn(&ExecutionContext, &Value) -> bool + Clone + Send + Sync + 'static;
    
    /// Deduplicate traversers
    pub fn dedup(self) -> Self;
    
    /// Limit results
    pub fn limit(self, n: usize) -> Self;
    pub fn skip(self, n: usize) -> Self;
    pub fn range(self, start: usize, end: usize) -> Self;
    
    /// Filter by traversal existence (Phase 4)
    pub fn where_(self, sub: Traversal<Value, Value>) -> Self;
    pub fn not(self, sub: Traversal<Value, Value>) -> Self;
    
    /// Coin flip filter (random sampling)
    pub fn coin(self, probability: f64) -> Self;
    
    /// Sample n random elements
    pub fn sample(self, n: usize) -> Self;
}
```

### 2.4 Predicates

```rust
/// Comparison predicates for has_where()
pub mod p {
    pub fn eq<T: Into<Value>>(value: T) -> impl Predicate;
    pub fn neq<T: Into<Value>>(value: T) -> impl Predicate;
    pub fn lt<T: Into<Value>>(value: T) -> impl Predicate;
    pub fn lte<T: Into<Value>>(value: T) -> impl Predicate;
    pub fn gt<T: Into<Value>>(value: T) -> impl Predicate;
    pub fn gte<T: Into<Value>>(value: T) -> impl Predicate;
    pub fn between<T: Into<Value>>(start: T, end: T) -> impl Predicate;
    pub fn inside<T: Into<Value>>(start: T, end: T) -> impl Predicate;
    pub fn outside<T: Into<Value>>(start: T, end: T) -> impl Predicate;
    pub fn within<T: Into<Value>>(values: impl IntoIterator<Item = T>) -> impl Predicate;
    pub fn without<T: Into<Value>>(values: impl IntoIterator<Item = T>) -> impl Predicate;
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

### 2.5 Map Steps (Navigation)

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Traverse to outgoing adjacent vertices
    pub fn out(self) -> BoundTraversal<'g, In, Value>;
    pub fn out_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value>;
    
    /// Traverse to incoming adjacent vertices
    pub fn in_(self) -> BoundTraversal<'g, In, Value>;
    pub fn in_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value>;
    
    /// Traverse both directions
    pub fn both(self) -> BoundTraversal<'g, In, Value>;
    pub fn both_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value>;
    
    /// Traverse to outgoing edges
    pub fn out_e(self) -> BoundTraversal<'g, In, Value>;
    pub fn out_e_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value>;
    
    /// Traverse to incoming edges
    pub fn in_e(self) -> BoundTraversal<'g, In, Value>;
    pub fn in_e_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value>;
    
    /// Traverse to all incident edges
    pub fn both_e(self) -> BoundTraversal<'g, In, Value>;
    pub fn both_e_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value>;
    
    /// Get source vertex of edge
    pub fn out_v(self) -> BoundTraversal<'g, In, Value>;
    
    /// Get target vertex of edge
    pub fn in_v(self) -> BoundTraversal<'g, In, Value>;
    
    /// Get both vertices of edge
    pub fn both_v(self) -> BoundTraversal<'g, In, Value>;
    
    /// Get the other vertex (requires path context)
    pub fn other_v(self) -> BoundTraversal<'g, In, Value>;
}
```

### 2.6 Map Steps (Transform)

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Extract property value
    pub fn values(self, key: &str) -> BoundTraversal<'g, In, Value>;
    pub fn values_multi(self, keys: &[&str]) -> BoundTraversal<'g, In, Value>;
    
    /// Extract property as key-value
    pub fn properties(self, key: &str) -> BoundTraversal<'g, In, Value>;
    
    /// Extract all properties as map
    pub fn value_map(self) -> BoundTraversal<'g, In, Value>;
    pub fn value_map_keys(self, keys: &[&str]) -> BoundTraversal<'g, In, Value>;
    
    /// Extract element map (id, label, properties)
    pub fn element_map(self) -> BoundTraversal<'g, In, Value>;
    
    /// Get element ID
    pub fn id(self) -> BoundTraversal<'g, In, Value>;
    
    /// Get element label
    pub fn label(self) -> BoundTraversal<'g, In, Value>;
    
    /// Apply transformation function
    pub fn map<F>(self, f: F) -> BoundTraversal<'g, In, Value>
    where
        F: Fn(&ExecutionContext, &Value) -> Value + Clone + Send + Sync + 'static;
    
    /// Flatmap transformation
    pub fn flat_map<F>(self, f: F) -> BoundTraversal<'g, In, Value>
    where
        F: Fn(&ExecutionContext, &Value) -> Vec<Value> + Clone + Send + Sync + 'static;
    
    /// Unfold collections
    pub fn unfold(self) -> BoundTraversal<'g, In, Value>;
    
    /// Get traversal path
    pub fn path(self) -> BoundTraversal<'g, In, Value>;
    
    /// Select labeled steps from path
    pub fn select(self, labels: &[&str]) -> BoundTraversal<'g, In, Value>;
    
    /// Constant value
    pub fn constant(self, value: impl Into<Value>) -> BoundTraversal<'g, In, Value>;
    
    /// Math operations
    pub fn math(self, expression: &str) -> BoundTraversal<'g, In, Value>;
}
```

### 2.7 Branch Steps

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Union of multiple traversals - merge results from all branches
    /// Anonymous traversals receive ExecutionContext at execution time
    pub fn union(self, traversals: Vec<Traversal<Value, Value>>) -> BoundTraversal<'g, In, Value>;
    
    /// First successful traversal - short-circuits on first branch with results
    pub fn coalesce(self, traversals: Vec<Traversal<Value, Value>>) -> BoundTraversal<'g, In, Value>;
    
    /// Conditional branching based on traversal existence
    /// condition: anonymous traversal to test (any results = true)
    /// if_true/if_false: branches to execute based on condition
    pub fn choose(
        self,
        condition: Traversal<Value, Value>,
        if_true: Traversal<Value, Value>,
        if_false: Traversal<Value, Value>,
    ) -> BoundTraversal<'g, In, Value>;
    
    /// Pattern matching with options
    /// selector: function to extract branch key from current value
    /// options: map of keys to anonymous traversals
    pub fn branch<K>(
        self,
        selector: impl Fn(&ExecutionContext, &Value) -> K + Clone + Send + Sync + 'static,
        options: HashMap<K, Traversal<Value, Value>>,
    ) -> BoundTraversal<'g, In, Value>
    where
        K: Eq + Hash + Clone + Send + Sync + 'static;
    
    /// Optional traversal - returns original value if sub produces no results
    pub fn optional(self, sub: Traversal<Value, Value>) -> BoundTraversal<'g, In, Value>;
    
    /// Repeat traversal - iterative graph exploration
    /// sub: anonymous traversal to repeat (same Traversal type as bound)
    pub fn repeat(self, sub: Traversal<Value, Value>) -> RepeatTraversal<'g, In>;
    
    /// Local scope - isolated execution of sub-traversal
    /// Aggregations in sub operate per-traverser, not globally
    pub fn local(self, sub: Traversal<Value, Value>) -> BoundTraversal<'g, In, Value>;
    
    /// Filter by traversal existence (Phase 4)
    pub fn where_(self, sub: Traversal<Value, Value>) -> Self;
    
    /// Filter by traversal non-existence
    pub fn not(self, sub: Traversal<Value, Value>) -> Self;
}

/// Builder for repeat() configuration
pub struct RepeatTraversal<'g, In> {
    bound: BoundTraversal<'g, In, Value>,
    sub: Traversal<Value, Value>,
    // Configuration fields...
}

impl<'g, In> RepeatTraversal<'g, In> {
    /// Maximum iterations
    pub fn times(self, n: usize) -> BoundTraversal<'g, In, Value>;
    
    /// Emit traversers from each iteration
    pub fn emit(self) -> Self;
    
    /// Emit traversers matching condition
    pub fn emit_if(self, predicate: Traversal<Value, Value>) -> Self;
    
    /// Stop when condition is satisfied
    pub fn until(self, condition: Traversal<Value, Value>) -> BoundTraversal<'g, In, Value>;
    
    /// Stop on cycle detection (vertex already in path)
    pub fn until_cycle(self) -> BoundTraversal<'g, In, Value>;
}
```

### 2.8 Reduce Steps

Reduce steps aggregate traversers into a single result. In the type-erased architecture, 
these return `Value` variants (e.g., `Value::Int`, `Value::Float`, `Value::List`).

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Count elements - returns traversal producing Value::Int
    pub fn count_step(self) -> BoundTraversal<'g, In, Value>;
    
    /// Sum numeric values - returns Value::Float
    pub fn sum_step(self) -> BoundTraversal<'g, In, Value>;
    
    /// Min value - returns Value matching input type
    pub fn min_step(self) -> BoundTraversal<'g, In, Value>;
    
    /// Max value - returns Value matching input type  
    pub fn max_step(self) -> BoundTraversal<'g, In, Value>;
    
    /// Mean average - returns Value::Float
    pub fn mean_step(self) -> BoundTraversal<'g, In, Value>;
    
    /// Group by key function
    /// Returns Value::Map with keys from key_fn and Value::List values
    pub fn group<F>(self, key_fn: F) -> BoundTraversal<'g, In, Value>
    where
        F: Fn(&ExecutionContext, &Value) -> Value + Clone + Send + Sync + 'static;
    
    /// Group and count
    /// Returns Value::Map with keys and Value::Int counts
    pub fn group_count<F>(self, key_fn: F) -> BoundTraversal<'g, In, Value>
    where
        F: Fn(&ExecutionContext, &Value) -> Value + Clone + Send + Sync + 'static;
    
    /// Fold into single value with accumulator
    pub fn fold_step<F>(self, init: Value, f: F) -> BoundTraversal<'g, In, Value>
    where
        F: Fn(&ExecutionContext, Value, Value) -> Value + Clone + Send + Sync + 'static;
    
    /// Collect to list (as traversal step, not terminal)
    pub fn to_list_step(self) -> BoundTraversal<'g, In, Value>;
    
    /// Collect to set (as traversal step, not terminal)
    pub fn to_set_step(self) -> BoundTraversal<'g, In, Value>;
}
```

### 2.9 Side Effect Steps

Side effect steps perform actions without changing the traverser stream.
They use `SideEffects` in the `ExecutionContext` for storage.

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Store current element in named side-effect collection
    pub fn store(self, key: &str) -> Self;
    
    /// Aggregate all elements into named collection (eager)
    pub fn aggregate(self, key: &str) -> Self;
    
    /// Execute side-effect function on each traverser
    pub fn side_effect<F>(self, f: F) -> Self
    where
        F: Fn(&ExecutionContext, &Value) + Clone + Send + Sync + 'static;
    
    /// Label current step for path tracking and select()
    pub fn as_(self, label: &str) -> Self;
    
    /// Add property to current element (requires mutable context)
    pub fn property(self, key: &str, value: impl Into<Value>) -> Self;
    
    /// Add multiple properties
    pub fn properties_add(self, props: HashMap<String, Value>) -> Self;
    
    /// Drop (delete) current element - returns empty traversal
    pub fn drop(self) -> BoundTraversal<'g, In, Value>;
}
```

### 2.10 Terminal Steps

Terminal steps consume the traversal, create the `ExecutionContext`, and return results.
These are only available on `BoundTraversal` (not anonymous `Traversal`).

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Execute and collect all results to Vec<Value>
    pub fn to_list(self) -> Vec<Value>;
    
    /// Execute and collect to HashSet<Value> (deduplicates)
    pub fn to_set(self) -> HashSet<Value>;
    
    /// Get next result
    pub fn next(self) -> Option<Value>;
    
    /// Get exactly one result (error if 0 or 2+)
    pub fn one(self) -> Result<Value, TraversalError>;
    
    /// Check if any results exist
    pub fn has_next(self) -> bool;
    
    /// Execute for side effects only (discards results)
    pub fn iterate(self);
    
    /// Get first n results
    pub fn take(self, n: usize) -> Vec<Value>;
    
    /// Count all results
    pub fn count(self) -> u64;
    
    /// Sum numeric values
    pub fn sum(self) -> Value;
    
    /// Get min value
    pub fn min(self) -> Option<Value>;
    
    /// Get max value
    pub fn max(self) -> Option<Value>;
    
    /// Fold/reduce with accumulator
    pub fn fold<B, F>(self, init: B, f: F) -> B
    where
        F: FnMut(B, Value) -> B;
    
    /// Explain query plan (for debugging)
    pub fn explain(self) -> QueryPlan;
    
    /// Profile execution (for performance analysis)
    pub fn profile(self) -> ProfileResult;
    
    /// Get results as lazy iterator
    pub fn iter(self) -> impl Iterator<Item = Value> + 'g;
    
    /// Get traversers with full metadata (path, bulk, etc.)
    pub fn traversers(self) -> impl Iterator<Item = Traverser> + 'g;
}
```

### 2.11 Mutation Steps (via GraphMut)

```rust
impl<'g> GraphMut<'g> {
    /// Add a new vertex
    pub fn add_v(self, label: &str) -> MutationBuilder<Vertex>;
    
    /// Add a new edge
    pub fn add_e(self, label: &str) -> EdgeBuilder;
    
    /// Commit all changes
    pub fn commit(self) -> Result<(), StorageError>;
    
    /// Rollback all changes
    pub fn rollback(self);
}

pub struct MutationBuilder<E> { /* ... */ }

impl MutationBuilder<Vertex> {
    pub fn property(self, key: &str, value: impl Into<Value>) -> Self;
    pub fn properties(self, props: HashMap<String, Value>) -> Self;
    pub fn build(self) -> VertexId;
}

pub struct EdgeBuilder { /* ... */ }

impl EdgeBuilder {
    pub fn from(self, src: VertexId) -> Self;
    pub fn to(self, dst: VertexId) -> Self;
    pub fn property(self, key: &str, value: impl Into<Value>) -> Self;
    pub fn build(self) -> EdgeId;
}
```

---

## 3. Usage Examples

### Basic Traversals

```rust
use intersteller::prelude::*;

fn main() -> Result<(), Box<dyn Error>> {
    // Open or create graph
    let graph = Graph::in_memory();
    
    // Get traversal source via snapshot
    // GraphSnapshot provides consistent reads
    let snap = graph.snapshot();
    let g = snap.traversal();  // Returns GraphTraversalSource<'_>
    
    // Find all people - returns Vec<Value> where Value::Vertex(id)
    let people: Vec<Value> = g.v()
        .has_label("person")
        .to_list();
    
    // Find person by name and get their friends' names
    let snap = graph.snapshot();
    let g = snap.traversal();
    let friend_names: Vec<Value> = g.v()
        .has_label("person")
        .has_value("name", "Alice")
        .out_labels(&["knows"])
        .values("name")  // Extracts "name" property as Value::String
        .to_list();
    
    // Extract strings from Value::String variants
    let names: Vec<String> = friend_names
        .into_iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s),
            _ => None,
        })
        .collect();
    
    // Complex query: People who know someone over 30
    // Uses anonymous traversal (__) with where_()
    let snap = graph.snapshot();
    let g = snap.traversal();
    let results: Vec<Value> = g.v()
        .has_label("person")
        .where_(__.out_labels(&["knows"]).has_where("age", p::gt(30)))
        .value_map()  // Returns Value::Map with all properties
        .to_list();
    
    Ok(())
}
```

### Graph Mutations

```rust
fn populate_graph(graph: &Graph) -> Result<(), StorageError> {
    let mut g = graph.mutate();  // Returns GraphMut<'_>
    
    // Add vertices
    let alice = g.add_v("person")
        .property("name", "Alice")
        .property("age", 30)
        .build();  // Returns VertexId
    
    let bob = g.add_v("person")
        .property("name", "Bob")
        .property("age", 35)
        .build();
    
    // Add edge
    g.add_e("knows")
        .from(alice)
        .to(bob)
        .property("since", 2020)
        .build();  // Returns EdgeId
    
    g.commit()
}
```

### Repeat Traversals (BFS/DFS)

```rust
// Find all vertices within 3 hops
// Uses anonymous traversal __.out() which is Traversal<Value, Value>
let nearby: Vec<Value> = g.v_ids([start_id])
    .repeat(__.out())  // Anonymous traversal for iteration
    .times(3)
    .emit()            // Emit from each iteration
    .dedup()
    .to_list();

// Find path to target (BFS)
// until() takes anonymous traversal as condition
let path: Option<Value> = g.v_ids([start_id])
    .repeat(__.out().simple_path())  // Chained anonymous traversal
    .until(__.has_id(target_id))     // Condition: stop when target found
    .path()                          // Returns Value::List of path elements
    .limit(1)
    .next();
```

### Aggregations

```rust
// Count vertices by label
// group_count returns Value::Map with Value::String keys and Value::Int counts
let counts: Value = g.v()
    .group_count(|ctx, v| {
        // Extract label as Value::String for grouping key
        match v {
            Value::Vertex(id) => {
                ctx.snapshot.get_vertex(*id)
                    .and_then(|v| ctx.get_label(v.label_id()))
                    .map(|l| Value::String(l.to_string()))
                    .unwrap_or(Value::Null)
            }
            _ => Value::Null,
        }
    })
    .next()
    .unwrap();

// Average age by department using group + local aggregation
// Results are Value::Map with department names as keys
let grouped: Vec<Value> = g.v()
    .has_label("employee")
    .group(|ctx, v| {
        // Extract department property for grouping
        match v {
            Value::Vertex(id) => {
                ctx.snapshot.get_vertex(*id)
                    .and_then(|v| v.property("department").cloned())
                    .unwrap_or(Value::Null)
            }
            _ => Value::Null,
        }
    })
    .to_list();

// Using anonymous traversals for aggregation keys (cleaner)
let counts: Value = g.v()
    .has_label("person")
    .group_count(|_, v| v.clone())  // Group by vertex itself
    .next()
    .unwrap();
```

### Anonymous Traversal Composition

```rust
// Define reusable traversal fragments
fn friends_of_friends() -> Traversal<Value, Value> {
    __.out_labels(&["knows"])
      .out_labels(&["knows"])
      .dedup()
}

fn works_at_same_company() -> Traversal<Value, Value> {
    __.out_labels(&["works_at"])
      .in_labels(&["works_at"])
      .dedup()
}

// Compose in queries
let snap = graph.snapshot();
let g = snap.traversal();

// Find people connected through multiple paths
let connected: Vec<Value> = g.v()
    .has_value("name", "Alice")
    .union(vec![
        friends_of_friends(),
        works_at_same_company(),
    ])
    .dedup()
    .to_list();

// Use with where_ for filtering
let people_who_know_bob: Vec<Value> = g.v()
    .has_label("person")
    .where_(__.out_labels(&["knows"]).has_value("name", "Bob"))
    .values("name")
    .to_list();
```

---

## 5. Roadmap

### Phase 1: Core Foundation ✅ Complete
- ✅ Core value types (`Value`, `VertexId`, `EdgeId`)
- ✅ Error hierarchy (`StorageError`, `TraversalError`)
- ✅ `GraphStorage` trait abstraction
- ✅ String interning
- ✅ Graph handle types (`Graph`, `GraphSnapshot`, `GraphMut`)
- ✅ RwLock-based concurrency with `try_mutate()`

### Phase 2: In-Memory Storage ✅ Complete
- ✅ HashMap-based `InMemoryGraph`
- ✅ O(1) vertex/edge lookup
- ✅ Inline label indexes (`HashMap<u32, RoaringBitmap>`)
- ✅ Adjacency list traversal
- ✅ Add/remove vertex/edge operations
- ✅ Integration tests (10K vertices, 100K edges)

### Phase 3: Traversal Engine (In Progress)
- ✅ Basic traversal types (`Traversal`, `Traverser`, `Path`)
- ✅ `GraphTraversalSource` with `v()` and `e()` starting points
- 🔄 Navigation steps (out, in, both)
- 🔄 Filter steps (hasLabel, has, where)
- 🔄 Terminal steps (toList, next, count)
- 📋 Anonymous traversals (`__` factory)
- 📋 Predicates (`p::` module)

### Phase 4: Advanced Traversal Features (Planned)
- 📋 Branch steps (union, coalesce, repeat)
- 📋 Reduce steps (count, sum, group, fold)
- 📋 Side-effect steps (store, aggregate, as_)
- 📋 Graph algorithms (BFS, DFS, path finding)

### Phase 5: Persistent Storage (Planned)
- 📋 Memory-mapped file storage
- 📋 On-disk record formats
- 📋 Write-ahead logging (WAL)
- 📋 Crash recovery

### Phase 6: Optional Indexes & Optimization (Planned)
- 📋 Property B+ tree indexes (optional secondary indexes)
- 📋 Composite indexes
- 📋 Query optimization
- 📋 Statistics collection

### Phase 7: Advanced Features (Future)
- 📋 GQL subset implementation
- 📋 Full-text search indexes
- 📋 MVCC concurrency model
- 📋 Compression and partitioning

Legend: ✅ Complete | 🔄 In Progress | 📋 Planned

---

## 6. Performance Characteristics

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Vertex lookup by ID | O(1) | Direct array index |
| Edge lookup by ID | O(1) | Direct array index |
| Label scan | O(n) | n = elements with label |
| Property index lookup | O(log n) | B+ tree |
| Adjacency traversal | O(k) | k = vertex degree |
| Path finding (BFS) | O(V + E) | Standard BFS |
| Add vertex | O(1) amortized | May trigger compaction |
| Add edge | O(1) | Linked list insertion |

---

## 7. Dependencies

```toml
[dependencies]
memmap2 = "0.9"           # Memory-mapped files
parking_lot = "0.12"      # Fast synchronization primitives
hashbrown = "0.14"        # Fast hash maps
smallvec = "1.11"         # Stack-allocated small vectors
thiserror = "1.0"         # Error handling
serde = { version = "1.0", features = ["derive"] }  # Serialization
```