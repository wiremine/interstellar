# Feature Flags

Interstellar uses Cargo feature flags to enable optional functionality. This allows you to include only the features you need, reducing compile times and binary size.

## Available Features

| Feature | Default | Description |
|---------|---------|-------------|
| `inmemory` | Yes | In-memory HashMap-based storage |
| `mmap` | No | Memory-mapped persistent storage |
| `rhai` | No | Rhai scripting engine integration |
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

### rhai

Embedded Rhai scripting engine for dynamic queries.

```toml
[dependencies]
interstellar = { version = "0.1", features = ["rhai"] }
```

**Provides:**
- `RhaiEngine` for script execution
- Gremlin-style traversal API in scripts
- All predicates available as functions
- Anonymous traversal support

**Dependencies added:**
- `rhai` (with `sync` feature) - Rhai scripting language

**Example:**

```rust
use interstellar::rhai::RhaiEngine;

let engine = RhaiEngine::new();

let results: Vec<String> = engine.eval_with_graph(&graph, r#"
    let g = graph.traversal();
    g.v()
        .has_label("person")
        .has_where("age", gt(25))
        .values("name")
        .to_list()
"#).unwrap();
```

### full-text

Full-text search indexing via Tantivy.

```toml
[dependencies]
interstellar = { version = "0.1", features = ["full-text"] }
```

**Provides:**
- Full-text property indexes
- Text search queries
- Tokenization and stemming

**Dependencies added:**
- `tantivy` - Full-text search engine

**Note:** This feature has a known upstream dependency conflict and may require careful dependency management.

## Combining Features

Enable multiple features with a comma-separated list:

```toml
[dependencies]
# Memory-mapped storage + Rhai scripting
interstellar = { version = "0.1", features = ["mmap", "rhai"] }
```

Or using array syntax:

```toml
[dependencies.interstellar]
version = "0.1"
features = ["mmap", "rhai"]
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
interstellar = { version = "0.1", features = ["mmap", "rhai"] }
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

# With Rhai scripting
cargo test --features rhai

# All features (recommended for CI)
cargo test --features "inmemory,mmap,rhai"
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
cargo build --features "mmap,rhai"
```

## Conditional Compilation

Use `#[cfg(feature = "...")]` for feature-specific code:

```rust
#[cfg(feature = "mmap")]
use interstellar::storage::MmapGraph;

#[cfg(feature = "rhai")]
use interstellar::rhai::RhaiEngine;

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

| Capability | inmemory | mmap | rhai | full-text |
|------------|----------|------|------|-----------|
| `Graph` | Yes | - | - | - |
| `MmapGraph` | - | Yes | - | - |
| `RhaiEngine` | - | - | Yes | - |
| Text indexes | - | - | - | Yes |
| Persistence | No | Yes | - | - |
| Scripting | - | - | Yes | - |
| Search | - | - | - | Yes |

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

### With rhai

```
interstellar
├── (default dependencies)
└── rhai (with sync)
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
- [Rhai Scripting](../api/rhai.md) - Rhai API reference
