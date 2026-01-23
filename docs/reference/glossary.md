# Glossary

This glossary defines key terms used throughout the Interstellar documentation.

## Core Concepts

### Graph

A data structure consisting of **vertices** (nodes) connected by **edges** (relationships). In Interstellar, graphs are property graphs where both vertices and edges can have labels and properties.

### Vertex (Node)

A point in the graph representing an entity. Each vertex has:
- A unique **VertexId**
- A **label** (type/category, e.g., "person", "product")
- Zero or more **properties** (key-value pairs)

### Edge (Relationship)

A directed connection between two vertices. Each edge has:
- A unique **EdgeId**
- A **source vertex** (outgoing from)
- A **target vertex** (incoming to)
- A **label** (relationship type, e.g., "knows", "purchased")
- Zero or more **properties**

### Property

A key-value pair attached to a vertex or edge. Keys are strings, values are `Value` enum instances (see [Value Types](value-types.md)).

### Label

A string categorizing a vertex or edge. Vertices typically have labels like "person", "company", "product". Edges have labels like "knows", "works_at", "purchased".

## Traversal Concepts

### Traversal

A query that navigates through the graph, starting from source vertices and following edges to discover related elements. Traversals are **lazy** and don't execute until a terminal step is called.

### Traverser

An object that moves through the graph during traversal. Each traverser carries:
- The **current value** (vertex, edge, or property value)
- The **path** taken to reach this point
- **Side effects** (aggregated data)

### Step

A single operation in a traversal pipeline. Steps can be:
- **Source steps**: Start the traversal (`v()`, `e()`)
- **Filter steps**: Remove traversers that don't match (`has()`, `where_()`)
- **Map steps**: Transform values (`values()`, `id()`)
- **FlatMap steps**: Transform and potentially multiply traversers (`out()`, `in_()`)
- **Terminal steps**: Execute and collect results (`to_list()`, `count()`)

### Anonymous Traversal

A traversal fragment created via the `__` module, used for composition within other traversals. Anonymous traversals are used in steps like `where_()`, `coalesce()`, `repeat()`, and `union()`.

```rust
use interstellar::traversal::__;

// Anonymous traversal for friends-of-friends
let fof = __::out("knows").out("knows");
```

### Predicate

A condition used in filter steps to test values. The `p` module provides predicate functions like `gt()`, `eq()`, `within()`, `containing()`, etc.

```rust
use interstellar::traversal::p;

// Filter vertices where age > 30
g.v().has_where("age", p::gt(30))
```

### Path

The sequence of elements visited during a traversal. Accessed via the `.path()` step or by labeling steps with `.as_()` and retrieving with `.select()`.

### Step Label

A string label attached to a point in the traversal via `.as_()`, allowing later retrieval via `.select()`.

```rust
g.v().as_("a").out("knows").as_("b").select_multi(["a", "b"])
```

## Storage Concepts

### Storage Backend

The underlying implementation that stores graph data. Interstellar provides:
- **InMemoryGraph**: HashMap-based, non-persistent
- **MmapGraph**: Memory-mapped, persistent

### Snapshot

A read-only view of the graph at a point in time. Snapshots provide consistent reads even during concurrent writes.

```rust
let snapshot = graph.snapshot();
let g = snapshot.gremlin();
```

### Write-Ahead Log (WAL)

A durability mechanism in MmapGraph. Changes are first written to a log file before being applied to the main data file. On crash, the WAL is replayed to recover uncommitted changes.

### Batch Mode

A write optimization in MmapGraph where multiple operations are buffered and committed together with a single fsync. Provides ~500x faster bulk writes.

```rust
graph.begin_batch()?;
// ... many writes ...
graph.commit_batch()?;
```

### Memory Mapping (mmap)

A technique where files are mapped directly into process memory, allowing the OS to manage paging between disk and RAM. Provides efficient random access to large files.

## Query Languages

### Gremlin

A graph traversal language originally from Apache TinkerPop. Interstellar provides a Gremlin-style fluent API for traversals.

```rust
g.v().has_label("person").out("knows").values("name").to_list()
```

### GQL (Graph Query Language)

A declarative query language with SQL-like syntax. Interstellar implements a subset of the ISO GQL standard.

```sql
MATCH (p:Person)-[:KNOWS]->(friend)
WHERE p.age > 25
RETURN p.name, friend.name
```

## API Concepts

### Fluent API

A programming interface where methods return `self`, allowing method chaining. Interstellar's traversal API is fluent:

```rust
g.v()
    .has_label("person")
    .has_where("age", p::gt(30))
    .out("knows")
    .values("name")
    .to_list()
```

### Terminal Step

A traversal step that executes the pipeline and returns results. Examples: `to_list()`, `count()`, `next()`, `one()`, `iterate()`.

### Source Step

A traversal step that starts the pipeline. Examples: `v()`, `e()`, `v_ids()`, `inject()`.

### Lazy Evaluation

Computation is deferred until results are needed. In Interstellar, traversals don't execute until a terminal step is called, and elements are processed one at a time (pull-based).

### Monomorphization

A Rust compiler optimization where generic code is specialized for each concrete type, eliminating runtime dispatch overhead. Interstellar uses this for zero-cost traversal pipelines.

## Indexing

### Label Index

An index that maps labels to element IDs, enabling fast filtering by label. Implemented using RoaringBitmap for memory efficiency.

### Property Index

An index on a specific property key, enabling fast lookup by property value.

### Unique Index

A property index that enforces uniqueness—no two elements can have the same value for the indexed property.

## Data Types

### Value

The dynamic type enum used for property values and traversal results. See [Value Types](value-types.md).

### VertexId

A unique identifier for a vertex, wrapping a `u64`.

### EdgeId

A unique identifier for an edge, wrapping a `u64`.

### ElementId

A union type that can hold either a `VertexId` or `EdgeId`.

### ComparableValue

A version of `Value` that implements `Ord` for sorting. Created via `value.to_comparable()`.

## Error Types

### StorageError

Errors from storage operations (missing elements, I/O failures, corruption).

### TraversalError

Errors during traversal execution (cardinality violations, storage failures).

### MutationError

Errors during mutation operations (missing endpoints, constraint violations).

See [Error Handling](error-handling.md) for details.

## Concurrency

### Thread Safety

The guarantee that code can be safely used from multiple threads. All Interstellar storage backends implement `Send + Sync`.

### RwLock

A reader-writer lock allowing multiple concurrent readers or one exclusive writer. Used internally for thread-safe access.

### Snapshot Isolation

A concurrency model where reads see a consistent point-in-time view of data, unaffected by concurrent writes.

## Verification

### Formal Verification

Mathematical proof that code satisfies its specification for all possible inputs. Interstellar uses Kani for this.

### Kani

A Rust verification tool that exhaustively checks code for all inputs within defined bounds, proving properties like memory safety and functional correctness.

### Proof Harness

A test function that Kani analyzes to verify properties. Marked with `#[kani::proof]`.

## See Also

- [Architecture](../concepts/architecture.md) - System design overview
- [Traversal Model](../concepts/traversal-model.md) - How traversals work
- [Value Types](value-types.md) - The Value type system
