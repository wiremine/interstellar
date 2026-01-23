# Storage Backends

Interstellar provides two storage backends: **InMemoryGraph** for development and small graphs, and **MmapGraph** for persistent production databases.

## Quick Comparison

| Feature | InMemoryGraph | MmapGraph |
|---------|---------------|-----------|
| Persistence | No | Yes |
| Capacity | Limited by RAM | Limited by disk |
| Startup time | Instant | Fast (mmap) |
| Write durability | None | WAL-based |
| Feature flag | Default | `mmap` |
| Best for | Dev, tests, small graphs | Production, large graphs |

---

## InMemoryGraph

HashMap-based storage that exists only in memory.

### When to Use

- Development and testing
- Small graphs (< 1M elements)
- Ephemeral data
- Maximum performance needed

### Usage

```rust
use interstellar::storage::InMemoryGraph;
use interstellar::graph::Graph;
use std::sync::Arc;

// Create storage
let mut storage = InMemoryGraph::new();

// Add data
let alice = storage.add_vertex("person", HashMap::from([
    ("name".to_string(), Value::String("Alice".to_string())),
]));

// Wrap for traversal API
let graph = Graph::new(Arc::new(storage));
```

### Characteristics

- **O(1) lookups**: HashMap-based vertex/edge access
- **No persistence**: Data lost on process exit
- **Full RAM usage**: All data in memory
- **Fast iteration**: Direct collection iteration

---

## MmapGraph

Memory-mapped persistent storage with write-ahead logging.

### When to Use

- Production databases
- Large graphs (millions of elements)
- Data must survive restarts
- Graphs larger than available RAM

### Requirements

Enable the `mmap` feature:

```toml
[dependencies]
interstellar = { version = "0.1", features = ["mmap"] }
```

### Usage

```rust
use interstellar::storage::MmapGraph;

// Open or create database
let graph = MmapGraph::open("my_graph.db")?;

// Add data (each operation is durable)
let alice = graph.add_vertex("person", HashMap::from([
    ("name".to_string(), Value::String("Alice".to_string())),
]))?;
```

### Batch Mode

For bulk loading, use batch mode to defer fsync:

```rust
// Start batch (disables per-operation fsync)
graph.begin_batch()?;

// Add many elements
for i in 0..100_000 {
    let props = HashMap::from([("i".to_string(), Value::Int(i))]);
    graph.add_vertex("node", props)?;
}

// Single fsync commits all operations
graph.commit_batch()?;
```

Batch mode is ~500x faster for bulk inserts.

### Characteristics

- **Persistent**: Data survives restarts
- **WAL durability**: Crash recovery via write-ahead log
- **Memory-mapped**: OS page cache manages memory
- **Slot reuse**: Deleted elements' space is reclaimed
- **String interning**: Compact storage for repeated strings

---

## Choosing a Backend

```
                    ┌─────────────────────┐
                    │ Need persistence?   │
                    └──────────┬──────────┘
                               │
              ┌────────────────┴────────────────┐
              │                                 │
              ▼                                 ▼
           No                                Yes
              │                                 │
              ▼                                 ▼
    ┌─────────────────┐               ┌─────────────────┐
    │ InMemoryGraph   │               │    MmapGraph    │
    └─────────────────┘               └─────────────────┘
```

### Use InMemoryGraph when:

- Running tests or development
- Data is temporary/regeneratable  
- Graph fits comfortably in RAM
- Maximum query performance is critical
- You don't need crash recovery

### Use MmapGraph when:

- Data must persist across restarts
- Graph may exceed RAM
- You need crash recovery
- Running in production
- Data is expensive to regenerate

---

## Performance Comparison

### Read Performance

Both backends have similar read performance for data in memory:

| Operation | InMemoryGraph | MmapGraph |
|-----------|---------------|-----------|
| Vertex by ID | O(1) | O(1) |
| Edge by ID | O(1) | O(1) |
| Label scan | O(n) | O(n) |
| Neighbor traversal | O(degree) | O(degree) |

MmapGraph may have cold-start latency if data isn't in page cache.

### Write Performance

| Mode | InMemoryGraph | MmapGraph |
|------|---------------|-----------|
| Single write | ~100ns | ~1ms (fsync) |
| Batch write | ~100ns | ~100ns |
| Batch commit | N/A | ~1ms (fsync) |

MmapGraph in batch mode approaches InMemoryGraph performance.

---

## Storage Files

MmapGraph creates these files:

```
my_graph.db           # Main data file
my_graph.db.wal       # Write-ahead log (during transactions)
my_graph.db.lock      # Process lock file
```

### File Management

```rust
// Check database size
let metadata = std::fs::metadata("my_graph.db")?;
println!("Database size: {} bytes", metadata.len());

// Compact database (removes deleted element space)
graph.compact()?;
```

---

## Memory Usage

### InMemoryGraph

All data lives in Rust heap allocations:

```
Total memory ≈ 
  vertices × (56 bytes + avg_props_size) +
  edges × (72 bytes + avg_props_size) +
  string_table_size
```

### MmapGraph

Memory usage depends on access patterns:

- Recently accessed pages: in RAM (page cache)
- Cold pages: on disk only
- OS manages page eviction automatically

Monitor with system tools (`htop`, `vmstat`).

---

## Switching Backends

The traversal API is identical for both backends:

```rust
// InMemoryGraph
let storage = InMemoryGraph::new();
let graph = Graph::new(Arc::new(storage));
let snapshot = graph.snapshot();
let g = snapshot.traversal();
let results = g.v().has_label("person").to_list();

// MmapGraph - same traversal code
let storage = MmapGraph::open("data.db")?;
let graph = Graph::new(Arc::new(storage));
let snapshot = graph.snapshot();
let g = snapshot.traversal();
let results = g.v().has_label("person").to_list();
```

---

## See Also

- [Architecture](architecture.md) - System overview
- [Performance Guide](../guides/performance.md) - Optimization tips
- [Feature Flags](../reference/feature-flags.md) - Enabling mmap feature
