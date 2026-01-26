# Architecture

Interstellar is a high-performance graph database library for Rust. This document provides an overview of the system architecture.

## Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        User Application                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│        ┌──────────────┐         ┌──────────────┐               │
│        │  Gremlin API │         │   GQL API    │               │
│        │  (Fluent)    │         │  (SQL-like)  │               │
│        └──────┬───────┘         └──────┬───────┘               │
│               │                        │                        │
│               └────────────────────────┘                        │
│                             ▼                                   │
│                   ┌─────────────────┐                           │
│                   │ Traversal Engine│                           │
│                   │  (Iterator-based)│                          │
│                   └────────┬────────┘                           │
│                            │                                    │
│              ┌─────────────┴─────────────┐                      │
│              ▼                           ▼                      │
│    ┌─────────────────┐         ┌─────────────────┐             │
│    │      Graph      │         │    MmapGraph    │             │
│    │   (In-Memory)   │         │  (Persistent)   │             │
│    └─────────────────┘         └─────────────────┘             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Core Components

### Query APIs

Interstellar provides two ways to query graphs:

| API | Style | Use Case |
|-----|-------|----------|
| **Gremlin** | Fluent/chainable | Rust applications, complex traversals |
| **GQL** | SQL-like strings | Simpler queries, user-provided queries |

Both APIs compile to the same underlying traversal engine.

### Traversal Engine

The traversal engine executes graph queries using a pull-based iterator model:

- **Lazy evaluation**: No work until results are consumed
- **Type-erased steps**: Steps stored as trait objects for flexibility
- **Composable**: Steps can be chained and nested

### Storage Layer

The `GraphStorage` trait abstracts over storage backends:

```rust
pub trait GraphStorage: Send + Sync {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex>;
    fn get_edge(&self, id: EdgeId) -> Option<Edge>;
    fn vertices(&self) -> Box<dyn Iterator<Item = Vertex>>;
    fn edges(&self) -> Box<dyn Iterator<Item = Edge>>;
    // ... more methods
}
```

Two implementations are provided:

- **Graph**: COW-based with interior mutability, fast, non-persistent
- **MmapGraph**: Memory-mapped files, persistent, larger capacity

---

## Data Model

### Property Graph Model

Interstellar implements the property graph model:

```
     ┌──────────────────────┐
     │       Vertex         │
     ├──────────────────────┤
     │ id: VertexId         │
     │ label: String        │
     │ properties: Map      │
     └──────────┬───────────┘
                │
                │ connected by
                ▼
     ┌──────────────────────┐
     │        Edge          │
     ├──────────────────────┤
     │ id: EdgeId           │
     │ label: String        │
     │ from: VertexId       │
     │ to: VertexId         │
     │ properties: Map      │
     └──────────────────────┘
```

### Value Types

Properties are stored as the `Value` enum:

```rust
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<Value>),
    Map(HashMap<String, Value>),
}
```

### Identifiers

Elements have strongly-typed identifiers:

```rust
pub struct VertexId(u64);
pub struct EdgeId(u64);
```

---

## Concurrency Model

### Read-Write Separation

Interstellar uses RwLock-based concurrency:

```rust
// Multiple concurrent readers
let snapshot = graph.snapshot();  // Acquires read lock

// Single writer
let mut_graph = graph.mutate();   // Acquires write lock
```

### Snapshots

`GraphSnapshot` provides a consistent view of the graph:

- Acquired with `graph.snapshot()`
- Holds a read lock for its lifetime
- Multiple snapshots can exist concurrently
- Traversals operate on snapshots

### Mutations

`GraphMut` provides exclusive write access:

- Acquired with `graph.mutate()`
- Holds a write lock for its lifetime
- Only one mutation context at a time
- Changes visible after commit

---

## Index Structures

### Primary Indexes

Built into storage backends:

| Index | Key | Value | Complexity |
|-------|-----|-------|------------|
| Vertex by ID | `VertexId` | Vertex data | O(1) |
| Edge by ID | `EdgeId` | Edge data | O(1) |
| Vertices by Label | Label string | RoaringBitmap of IDs | O(n) scan, O(1) per element |

### Adjacency Lists

Each vertex maintains edge lists:

```
Vertex
├── out_edges: Vec<EdgeId>  // Outgoing edges
└── in_edges: Vec<EdgeId>   // Incoming edges
```

Traversal from a vertex to its neighbors is O(degree).

### Optional Indexes (Future)

- Property indexes (B+ tree)
- Composite indexes
- Full-text indexes

---

## Module Organization

```
interstellar/
├── src/
│   ├── lib.rs              # Public API, prelude
│   ├── graph.rs            # Graph, GraphSnapshot, GraphMut
│   ├── value.rs            # Value enum, VertexId, EdgeId
│   ├── error.rs            # Error types
│   ├── storage/
│   │   ├── mod.rs          # GraphStorage trait
│   │   ├── cow_graph.rs    # Graph (COW-based in-memory)
│   │   └── mmap.rs         # MmapGraph (feature-gated)
│   ├── traversal/
│   │   ├── mod.rs          # Traversal types
│   │   ├── step.rs         # Step trait and implementations
│   │   └── p.rs            # Predicates
│   └── gql/
│       ├── mod.rs          # GQL parser and compiler
│       └── ...
```

---

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Vertex lookup by ID | O(1) | Direct index |
| Edge lookup by ID | O(1) | Direct index |
| Get all vertices | O(V) | Full scan |
| Get vertices by label | O(n) | n = vertices with label |
| Traverse edge | O(1) | Direct index |
| Get neighbors | O(degree) | Adjacency list scan |
| Add vertex | O(1) amortized | May resize |
| Add edge | O(1) | Append to lists |
| Delete vertex | O(degree) | Must remove edges |

---

## See Also

- [Storage Backends](storage-backends.md) - Detailed storage comparison
- [Traversal Model](traversal-model.md) - How traversals execute
- [Concurrency](concurrency.md) - Thread safety details
