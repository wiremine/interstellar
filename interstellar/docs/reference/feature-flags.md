# Feature Flags

Interstellar uses Cargo feature flags to enable optional functionality. This allows you to include only the features you need, reducing compile times and binary size.

## Available Features

| Feature | Default | Description |
|---------|---------|-------------|
| `inmemory` | Yes | In-memory HashMap-based storage |
| `mmap` | No | Memory-mapped persistent storage |
| `full-text` | No | Full-text search via Tantivy |

## Feature Details

### inmemory (Default)

The in-memory storage backend using HashMaps. Enabled by default.

```toml
[dependencies]
interstellar = "0.1"
```

**Provides:**
- `Graph` storage backend (in-memory with COW snapshots)
- Fast read/write access
- Ideal for development and testing
- No persistence (data lost on process exit)

**Example:**

```rust
use interstellar::prelude::*;

let graph = Graph::new();
```

### mmap

Memory-mapped persistent storage with write-ahead logging.

```toml
[dependencies]
interstellar = { version = "0.1", features = ["mmap"] }
```

**Provides:**
- `MmapGraph` storage backend
- Persistent storage to disk
- Crash recovery via WAL
- Zero-copy reads
- Batch mode for high-performance writes

**Dependencies added:**
- `memmap2` - Memory-mapped file support
- `serde_json` - JSON serialization for metadata

**Example:**

```rust
use interstellar::storage::MmapGraph;

// Open or create database
let graph = MmapGraph::open("my_graph.db").unwrap();

// Batch mode for bulk writes
graph.begin_batch().unwrap();
// ... add many vertices/edges ...
graph.commit_batch().unwrap();
```

### full-text

Full-text search indexing via Tantivy.

```toml
[dependencies]
interstellar = { version = "0.1", features = ["full-text"] }
```

**Provides:**
- Per-property text indexes on the `Graph` (COW backend) for **vertices** and **edges**, with automatic mutation hooks for `add_vertex` / `set_vertex_property` / `remove_vertex` and `add_edge` / `set_edge_property` / `remove_edge`.
- BM25-ranked top-`k` search via the `search_text(property, query, k)` / `search_text_query(property, &TextQuery, k)` (vertex) and `search_text_e` / `search_text_query_e` (edge) source steps on `CowTraversalSource`.
- Structured queries: `TextQuery::{Match, MatchAll, Phrase, Prefix, And, Or, Not}`.
- Per-traverser BM25 scores carried in the traverser sack.

**Dependencies added:**
- `tantivy` 0.25 — full-text search engine.

**Limitations:**
- In-memory `RamDirectory` only — text indexes are not persisted to the mmap backend in this release (planned for Phase 4 of `spec-55`).
- Property names are globally unique across vertex and edge indexes — you cannot register both `create_text_index_v("body", ...)` and `create_text_index_e("body", ...)` simultaneously.
- No GQL / Gremlin string-parser surface yet — use the typed Rust API.

**Quick reference:**

```rust
use std::sync::Arc;
use interstellar::storage::Graph;
use interstellar::storage::text::{TextIndexConfig, TextQuery};

let graph = Arc::new(Graph::new());
graph.create_text_index_v("body", TextIndexConfig::default()).unwrap();

let g = graph.gremlin(Arc::clone(&graph));
let hits = g.search_text("body", "raft consensus", 10).unwrap().to_value_list();
```

See the [Full-Text Search guide](../guides/full-text-search.md) and the
runnable [`quickstart_text_search`](../getting-started/examples.md#quickstart_text_search)
example for a complete walkthrough.

## Combining Features

Enable multiple features with a comma-separated list:

```toml
[dependencies]
# Memory-mapped storage + full-text search
interstellar = { version = "0.1", features = ["mmap", "full-text"] }
```

Or using array syntax:

```toml
[dependencies.interstellar]
version = "0.1"
features = ["mmap", "full-text"]
```

## Disabling Default Features

To use only non-default features:

```toml
[dependencies]
interstellar = { version = "0.1", default-features = false, features = ["mmap"] }
```

## Development vs Production

### Development Configuration

For development and testing, the defaults are usually sufficient:

```toml
[dependencies]
interstellar = "0.1"

[dev-dependencies]
interstellar = { version = "0.1", features = ["mmap"] }
```

### Production Configuration

For production, enable only what you need:

```toml
[dependencies]
# Minimal: just persistent storage
interstellar = { version = "0.1", default-features = false, features = ["mmap"] }
```

## Testing with Features

Run tests with specific features:

```bash
# Default features only
cargo test

# With mmap support
cargo test --features mmap

# All features (recommended for CI)
cargo test --features "mmap,full-text"
```

## Benchmarking with Features

```bash
# Default benchmarks
cargo bench

# Include mmap benchmarks
cargo bench --features mmap
```

## Building with Features

```bash
# Debug build with defaults
cargo build

# Release build with mmap
cargo build --release --features mmap

# All features
cargo build --features "mmap,full-text"
```

## Conditional Compilation

Use `#[cfg(feature = "...")]` for feature-specific code:

```rust
#[cfg(feature = "mmap")]
use interstellar::storage::MmapGraph;

fn main() {
    #[cfg(feature = "mmap")]
    {
        let graph = MmapGraph::open("data.db").unwrap();
        // Use persistent storage
    }
    
    #[cfg(not(feature = "mmap"))]
    {
        let graph = Graph::new();
        // Use in-memory storage
    }
}
```

## Feature Matrix

| Capability | inmemory | mmap | full-text |
|------------|----------|------|-----------|
| `Graph` (COW in-memory) | Yes | - | - |
| `MmapGraph` | - | Yes | - |
| `PersistentGraph` (COW + mmap) | - | Yes | - |
| Property indexes (BTree, Unique) | Yes | Yes | - |
| Text indexes (`create_text_index_v` / `_e`) | - | - | Yes (COW backend only) |
| `search_text` / `search_text_query` (vertices) | - | - | Yes |
| `search_text_e` / `search_text_query_e` (edges) | - | - | Yes |
| Persistence | No | Yes | No (RamDirectory only) |

## Dependency Tree

### Default (inmemory only)

```
interstellar
├── thiserror
├── hashbrown
├── im
├── smallvec
├── serde
├── parking_lot
├── roaring
├── regex
├── crc32fast
├── bincode
├── pest
├── pest_derive
├── mathexpr
└── rand
```

### With mmap

```
interstellar
├── (default dependencies)
├── memmap2
└── serde_json
```

### With full-text

```
interstellar
├── (default dependencies)
└── tantivy
```

## See Also

- [Installation](../getting-started/installation.md) - Getting started with features
- [Storage Backends](../concepts/storage-backends.md) - Detailed storage documentation
