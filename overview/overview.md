# RustGremlin: A Fluent Graph Traversal Library

A high-performance, portable Rust library providing a Gremlin-style fluent API for graph traversals, supporting both in-memory and memory-mapped storage backends.

---

## 1. Backend Architecture

### 1.1 Storage Layer: Dual Storage Architecture

RustGremlin supports **two storage modes** to accommodate different use cases:

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
│  Primary Indexes (always maintained):                           │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ • Node ID → NodeRecord offset    (direct array lookup)  │   │
│  │ • Edge ID → EdgeRecord offset    (direct array lookup)  │   │
│  │ • Label → Node/Edge ID set       (hash map)             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Secondary Indexes (optional, user-created):                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ • Property Index: (label, key, value) → Element IDs     │   │
│  │   Implementation: B+ tree for range queries             │   │
│  │                                                         │   │
│  │ • Composite Index: (label, key₁, key₂, ...) → IDs       │   │
│  │   Implementation: Concatenated key B+ tree              │   │
│  │                                                         │   │
│  │ • Full-text Index: text property → Element IDs          │   │
│  │   Implementation: Inverted index with BK-tree           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Adjacency Structure (embedded in records):                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Node ──first_out──→ Edge ──next_out──→ Edge ──→ ...     │   │
│  │      ←─first_in───       ←─next_in───       ←── ...     │   │
│  │                                                         │   │
│  │ Doubly-linked edge lists per vertex for O(1) iteration  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 1.4 Concurrency Model

```rust
/// Thread-safe graph handle with RwLock-based concurrency
pub struct Graph {
    storage: Arc<Storage>,
    read_lock: RwLock<()>,
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
    write_buffer: WriteBuffer,
    _guard: RwLockWriteGuard<'g, ()>,
}
```

**Concurrency guarantees:**
- Multiple concurrent readers via `RwLock`
- Single writer with buffered writes
- Atomic commit via write-ahead log (WAL)
- Snapshots see consistent graph state

### 1.5 Module Structure

```
rustgremlin/
├── src/
│   ├── lib.rs              # Public API exports
│   ├── graph.rs            # Graph, GraphSnapshot, GraphMut
│   ├── storage/
│   │   ├── mod.rs
│   │   ├── mmap.rs         # Memory-mapped file handling
│   │   ├── records.rs      # On-disk record formats
│   │   ├── arena.rs        # Property/string allocation
│   │   └── wal.rs          # Write-ahead logging
│   ├── index/
│   │   ├── mod.rs
│   │   ├── label.rs        # Label → ID index
│   │   ├── property.rs     # Property B+ tree index
│   │   └── fulltext.rs     # Full-text search index
│   ├── traversal/
│   │   ├── mod.rs
│   │   ├── source.rs       # V(), E() starting steps
│   │   ├── filter.rs       # has(), hasLabel(), where()
│   │   ├── map.rs          # out(), in(), both(), values()
│   │   ├── branch.rs       # union(), coalesce(), choose()
│   │   ├── reduce.rs       # count(), sum(), fold()
│   │   ├── sideeffect.rs   # store(), aggregate()
│   │   └── terminal.rs     # toList(), next(), iterate()
│   ├── value.rs            # Value enum and conversions
│   └── error.rs            # Error types
└── Cargo.toml
```

---

## 2. Gremlin-Style Fluent API

### 2.1 Core Traversal Types

```rust
/// The main traversal builder - zero-cost abstractions via monomorphization
pub struct Traversal<S, E, T: Traverser<E>> {
    source: S,
    _phantom: PhantomData<(E, T)>,
}

/// Represents a position in the traversal with path history
pub trait Traverser<E>: Clone {
    fn current(&self) -> &E;
    fn path(&self) -> &Path;
    fn sack<T: Any>(&self) -> Option<&T>;
}

/// Traversal source - entry point for all traversals
pub struct GraphTraversalSource<'g> {
    graph: GraphSnapshot<'g>,
}

impl<'g> GraphTraversalSource<'g> {
    pub fn new(graph: &'g Graph) -> Self;
}
```

### 2.2 Source Steps

```rust
impl<'g> GraphTraversalSource<'g> {
    /// Start traversal from all vertices
    pub fn v(self) -> Traversal<Self, Vertex, impl Traverser<Vertex>>;
    
    /// Start from specific vertex IDs
    pub fn v_by_ids(self, ids: impl IntoIterator<Item = VertexId>) 
        -> Traversal<Self, Vertex, impl Traverser<Vertex>>;
    
    /// Start traversal from all edges  
    pub fn e(self) -> Traversal<Self, Edge, impl Traverser<Edge>>;
    
    /// Start from specific edge IDs
    pub fn e_by_ids(self, ids: impl IntoIterator<Item = EdgeId>)
        -> Traversal<Self, Edge, impl Traverser<Edge>>;
    
    /// Inject arbitrary values into traversal
    pub fn inject<T>(self, values: impl IntoIterator<Item = T>)
        -> Traversal<Self, T, impl Traverser<T>>;
}
```

### 2.3 Filter Steps

```rust
impl<S, E, T: Traverser<E>> Traversal<S, E, T> {
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
        F: Fn(&E) -> bool;
    
    /// Deduplicate traversers
    pub fn dedup(self) -> Self;
    pub fn dedup_by<F, K: Eq + Hash>(self, key_fn: F) -> Self
    where
        F: Fn(&E) -> K;
    
    /// Limit results
    pub fn limit(self, n: usize) -> Self;
    pub fn skip(self, n: usize) -> Self;
    pub fn range(self, start: usize, end: usize) -> Self;
    
    /// Filter by traversal existence
    pub fn where_<S2, E2, T2>(self, sub: Traversal<S2, E2, T2>) -> Self;
    pub fn not<S2, E2, T2>(self, sub: Traversal<S2, E2, T2>) -> Self;
    
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
impl<S, T: Traverser<Vertex>> Traversal<S, Vertex, T> {
    /// Traverse to outgoing adjacent vertices
    pub fn out(self) -> Self;
    pub fn out_labels(self, labels: &[&str]) -> Self;
    
    /// Traverse to incoming adjacent vertices
    pub fn in_(self) -> Self;  // trailing underscore: `in` is a keyword
    pub fn in_labels(self, labels: &[&str]) -> Self;
    
    /// Traverse both directions
    pub fn both(self) -> Self;
    pub fn both_labels(self, labels: &[&str]) -> Self;
    
    /// Traverse to outgoing edges
    pub fn out_e(self) -> Traversal<S, Edge, impl Traverser<Edge>>;
    pub fn out_e_labels(self, labels: &[&str]) -> Traversal<S, Edge, impl Traverser<Edge>>;
    
    /// Traverse to incoming edges
    pub fn in_e(self) -> Traversal<S, Edge, impl Traverser<Edge>>;
    pub fn in_e_labels(self, labels: &[&str]) -> Traversal<S, Edge, impl Traverser<Edge>>;
    
    /// Traverse to all incident edges
    pub fn both_e(self) -> Traversal<S, Edge, impl Traverser<Edge>>;
    pub fn both_e_labels(self, labels: &[&str]) -> Traversal<S, Edge, impl Traverser<Edge>>;
}

impl<S, T: Traverser<Edge>> Traversal<S, Edge, T> {
    /// Get source vertex of edge
    pub fn out_v(self) -> Traversal<S, Vertex, impl Traverser<Vertex>>;
    
    /// Get target vertex of edge
    pub fn in_v(self) -> Traversal<S, Vertex, impl Traverser<Vertex>>;
    
    /// Get both vertices of edge
    pub fn both_v(self) -> Traversal<S, Vertex, impl Traverser<Vertex>>;
    
    /// Get the other vertex (requires path context)
    pub fn other_v(self) -> Traversal<S, Vertex, impl Traverser<Vertex>>;
}
```

### 2.6 Map Steps (Transform)

```rust
impl<S, E: Element, T: Traverser<E>> Traversal<S, E, T> {
    /// Extract property value
    pub fn values(self, key: &str) -> Traversal<S, Value, impl Traverser<Value>>;
    pub fn values_multi(self, keys: &[&str]) -> Traversal<S, Value, impl Traverser<Value>>;
    
    /// Extract property as key-value
    pub fn properties(self, key: &str) -> Traversal<S, Property, impl Traverser<Property>>;
    
    /// Extract all properties as map
    pub fn value_map(self) -> Traversal<S, HashMap<String, Value>, impl Traverser<HashMap<String, Value>>>;
    pub fn value_map_keys(self, keys: &[&str]) -> Traversal<S, HashMap<String, Value>, impl Traverser<HashMap<String, Value>>>;
    
    /// Extract element map (id, label, properties)
    pub fn element_map(self) -> Traversal<S, HashMap<String, Value>, impl Traverser<HashMap<String, Value>>>;
    
    /// Get element ID
    pub fn id(self) -> Traversal<S, ElementId, impl Traverser<ElementId>>;
    
    /// Get element label
    pub fn label(self) -> Traversal<S, String, impl Traverser<String>>;
    
    /// Apply transformation function
    pub fn map<F, R>(self, f: F) -> Traversal<S, R, impl Traverser<R>>
    where
        F: Fn(&E) -> R;
    
    /// Flatmap transformation
    pub fn flat_map<F, I, R>(self, f: F) -> Traversal<S, R, impl Traverser<R>>
    where
        F: Fn(&E) -> I,
        I: IntoIterator<Item = R>;
    
    /// Unfold collections
    pub fn unfold(self) -> Traversal<S, E::Item, impl Traverser<E::Item>>
    where
        E: IntoIterator;
    
    /// Get traversal path
    pub fn path(self) -> Traversal<S, Path, impl Traverser<Path>>;
    
    /// Select labeled steps from path
    pub fn select(self, labels: &[&str]) -> Traversal<S, HashMap<String, Value>, impl Traverser<HashMap<String, Value>>>;
    
    /// Constant value
    pub fn constant<V: Clone>(self, value: V) -> Traversal<S, V, impl Traverser<V>>;
    
    /// Math operations
    pub fn math(self, expression: &str) -> Traversal<S, f64, impl Traverser<f64>>;
}
```

### 2.7 Branch Steps

```rust
impl<S, E, T: Traverser<E>> Traversal<S, E, T> {
    /// Union of multiple traversals
    pub fn union<S2, T2>(self, traversals: Vec<Traversal<S2, E, T2>>) 
        -> Self;
    
    /// First successful traversal
    pub fn coalesce<S2, T2>(self, traversals: Vec<Traversal<S2, E, T2>>)
        -> Self;
    
    /// Conditional branching
    pub fn choose<C, S2, S3, T2, T3>(
        self,
        condition: C,
        if_true: Traversal<S2, E, T2>,
        if_false: Traversal<S3, E, T3>,
    ) -> Self
    where
        C: Fn(&E) -> bool;
    
    /// Pattern matching with options
    pub fn branch<K, S2, T2>(
        self,
        selector: impl Fn(&E) -> K,
        options: HashMap<K, Traversal<S2, E, T2>>,
    ) -> Self
    where
        K: Eq + Hash;
    
    /// Optional traversal (returns original if empty)
    pub fn optional<S2, T2>(self, sub: Traversal<S2, E, T2>) -> Self;
    
    /// Repeat traversal
    pub fn repeat<S2, T2>(self, sub: Traversal<S2, E, T2>) -> RepeatTraversal<S, E, T>;
    
    /// Local scope (isolated traversal)
    pub fn local<S2, E2, T2>(self, sub: Traversal<S2, E2, T2>) 
        -> Traversal<S, E2, impl Traverser<E2>>;
}

/// Builder for repeat() configuration
pub struct RepeatTraversal<S, E, T> { /* ... */ }

impl<S, E, T: Traverser<E>> RepeatTraversal<S, E, T> {
    /// Maximum iterations
    pub fn times(self, n: usize) -> Traversal<S, E, T>;
    
    /// Emit during traversal
    pub fn emit(self) -> Self;
    pub fn emit_if<F>(self, predicate: F) -> Self
    where
        F: Fn(&E) -> bool;
    
    /// Stop condition
    pub fn until<F>(self, predicate: F) -> Traversal<S, E, T>
    where
        F: Fn(&E) -> bool;
    
    /// Loop detection
    pub fn until_cycle(self) -> Traversal<S, E, T>;
}
```

### 2.8 Reduce Steps

```rust
impl<S, E, T: Traverser<E>> Traversal<S, E, T> {
    /// Count elements
    pub fn count(self) -> Traversal<S, u64, impl Traverser<u64>>;
    
    /// Sum numeric values
    pub fn sum(self) -> Traversal<S, f64, impl Traverser<f64>>
    where
        E: Into<f64>;
    
    /// Min/Max
    pub fn min(self) -> Traversal<S, E, impl Traverser<E>>
    where
        E: Ord;
    pub fn max(self) -> Traversal<S, E, impl Traverser<E>>
    where
        E: Ord;
    
    /// Mean average
    pub fn mean(self) -> Traversal<S, f64, impl Traverser<f64>>
    where
        E: Into<f64>;
    
    /// Group by key
    pub fn group<F, K>(self, key_fn: F) 
        -> Traversal<S, HashMap<K, Vec<E>>, impl Traverser<HashMap<K, Vec<E>>>>
    where
        F: Fn(&E) -> K,
        K: Eq + Hash;
    
    /// Group and count
    pub fn group_count<F, K>(self, key_fn: F)
        -> Traversal<S, HashMap<K, u64>, impl Traverser<HashMap<K, u64>>>
    where
        F: Fn(&E) -> K,
        K: Eq + Hash;
    
    /// Fold into single value
    pub fn fold<A, F>(self, init: A, f: F) -> Traversal<S, A, impl Traverser<A>>
    where
        F: Fn(A, E) -> A;
    
    /// Collect to list
    pub fn to_list_step(self) -> Traversal<S, Vec<E>, impl Traverser<Vec<E>>>;
    
    /// Collect to set
    pub fn to_set_step(self) -> Traversal<S, HashSet<E>, impl Traverser<HashSet<E>>>
    where
        E: Eq + Hash;
}
```

### 2.9 Side Effect Steps

```rust
impl<S, E, T: Traverser<E>> Traversal<S, E, T> {
    /// Store current element in side-effect
    pub fn store(self, key: &str) -> Self;
    
    /// Aggregate all elements into collection
    pub fn aggregate(self, key: &str) -> Self;
    
    /// Execute side-effect
    pub fn side_effect<F>(self, f: F) -> Self
    where
        F: Fn(&E);
    
    /// Label current step for path tracking
    pub fn as_(self, label: &str) -> Self;
    
    /// Add property to element
    pub fn property(self, key: &str, value: impl Into<Value>) -> Self;
    
    /// Add multiple properties
    pub fn properties_add(self, props: HashMap<String, Value>) -> Self;
    
    /// Drop (delete) element
    pub fn drop(self) -> Traversal<S, (), impl Traverser<()>>;
}
```

### 2.10 Terminal Steps

```rust
impl<S, E, T: Traverser<E>> Traversal<S, E, T> {
    /// Execute and collect all results
    pub fn to_list(self) -> Vec<E>;
    
    /// Execute and collect to set
    pub fn to_set(self) -> HashSet<E>
    where
        E: Eq + Hash;
    
    /// Get next result
    pub fn next(self) -> Option<E>;
    
    /// Get exactly one result (error if 0 or 2+)
    pub fn one(self) -> Result<E, TraversalError>;
    
    /// Check if any results exist
    pub fn has_next(self) -> bool;
    
    /// Execute for side effects only
    pub fn iterate(self);
    
    /// Get first n results
    pub fn take(self, n: usize) -> Vec<E>;
    
    /// Explain query plan
    pub fn explain(self) -> QueryPlan;
    
    /// Profile execution
    pub fn profile(self) -> ProfileResult;
    
    /// Get results as iterator
    pub fn iter(self) -> impl Iterator<Item = E>;
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
use rustgremlin::prelude::*;

fn main() -> Result<(), Box<dyn Error>> {
    // Open or create graph
    let graph = Graph::open("my_graph.db")?;
    
    // Get traversal source (snapshot)
    let g = graph.traversal();
    
    // Find all people
    let people: Vec<Vertex> = g.v()
        .has_label("person")
        .to_list();
    
    // Find person by name and get their friends' names
    let friend_names: Vec<String> = g.v()
        .has_label("person")
        .has_value("name", "Alice")
        .out_labels(&["knows"])
        .values("name")
        .map(|v| v.as_string().unwrap().clone())
        .to_list();
    
    // Complex query: People who know someone over 30
    let results = g.v()
        .has_label("person")
        .where_(__.out_labels(&["knows"]).has_where("age", p::gt(30)))
        .value_map()
        .to_list();
    
    Ok(())
}
```

### Graph Mutations

```rust
fn populate_graph(graph: &Graph) -> Result<(), StorageError> {
    let mut g = graph.mutate();
    
    // Add vertices
    let alice = g.add_v("person")
        .property("name", "Alice")
        .property("age", 30)
        .build();
    
    let bob = g.add_v("person")
        .property("name", "Bob")
        .property("age", 35)
        .build();
    
    // Add edge
    g.add_e("knows")
        .from(alice)
        .to(bob)
        .property("since", 2020)
        .build();
    
    g.commit()
}
```

### Repeat Traversals (BFS/DFS)

```rust
// Find all vertices within 3 hops
let nearby = g.v_by_ids([start_id])
    .repeat(__.out())
    .times(3)
    .emit()
    .dedup()
    .to_list();

// Find path to target (BFS)
let path = g.v_by_ids([start_id])
    .repeat(__.out().simple_path())
    .until(__.has_id(target_id))
    .path()
    .limit(1)
    .next();
```

### Aggregations

```rust
// Count vertices by label
let counts: HashMap<String, u64> = g.v()
    .group_count(|v| v.label().to_string())
    .next()
    .unwrap();

// Average age by department
let avg_ages = g.v()
    .has_label("employee")
    .group(|v| v.property("department").unwrap())
    .map(|(dept, employees)| {
        let avg = employees.iter()
            .filter_map(|e| e.property("age").and_then(|v| v.as_i64()))
            .sum::<i64>() as f64 / employees.len() as f64;
        (dept, avg)
    })
    .to_list();
```

---

## 5. Roadmap

### Phase 1: Core Graph Database (Current Focus)
- ✅ Dual storage architecture (in-memory + memory-mapped)
- ✅ Gremlin-style fluent API
- ✅ Anonymous traversals
- ✅ Basic graph algorithms (BFS, DFS, path finding)
- ✅ Property indexes and label indexes
- ✅ WAL for durability
- ✅ Simple RwLock concurrency

### Phase 2: Query Language & Advanced Features
- 🔄 GQL subset implementation (see [gql.md](./gql.md))
- 🔄 Full-text search indexes
- 🔄 MVCC concurrency model
- 🔄 Compression and partitioning

### Phase 3: Scale & Performance
- 📋 Distributed graph storage
- 📋 Advanced query optimization
- 📋 Graph algorithms library
- 📋 Adaptive caching

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