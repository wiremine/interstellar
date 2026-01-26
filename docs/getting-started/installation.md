# Installation

Add Interstellar to your Rust project to build high-performance graph applications.

## Requirements

- Rust 1.70 or later
- Cargo package manager

## Basic Installation

Add Interstellar to your `Cargo.toml`:

```toml
[dependencies]
interstellar = "0.1"
```

This provides the core library with in-memory storage.

## Feature Flags

Interstellar uses Cargo features to enable optional functionality:

| Feature | Description | Use Case |
|---------|-------------|----------|
| `mmap` | Memory-mapped persistent storage | Production databases, large graphs |
| `full-text` | Full-text search indexes | Text search capabilities |

### Enabling Features

Enable features in your `Cargo.toml`:

```toml
[dependencies]
# Persistent storage
interstellar = { version = "0.1", features = ["mmap"] }

# Multiple features
interstellar = { version = "0.1", features = ["mmap", "full-text"] }
```

## Verifying Installation

Create a simple test to verify Interstellar is working:

```rust
use interstellar::prelude::*;

fn main() {
    // Create an in-memory graph
    let graph = Graph::new();
    
    // Get a snapshot for querying
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();
    
    // Count all vertices (should be 0)
    let count = g.v().count();
    println!("Vertex count: {}", count);
}
```

Run with:

```bash
cargo run
```

Expected output:

```
Vertex count: 0
```

## Next Steps

- [Quick Start](quick-start.md) - Create vertices, edges, and run queries
- [Examples](examples.md) - Explore included example programs

## See Also

- [Feature Flags Reference](../reference/feature-flags.md) - Detailed feature documentation
- [Storage Backends](../concepts/storage-backends.md) - Choosing the right storage
