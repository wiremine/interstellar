# AGENTS.md - RustGremlin Graph Database

Guidelines for AI coding agents working in this codebase.

## Project Overview

RustGremlin is a high-performance Rust graph traversal library with:
- **Dual storage backends**: In-memory (HashMap-based) and memory-mapped (persistent)
- **Gremlin-style fluent API**: Chainable traversal steps with lazy evaluation
- **Anonymous traversals**: Composable fragments via the `__` factory module
- **Zero-cost abstractions**: Monomorphized traversal pipelines

**Current state**: Phase 1 (Core Foundation) and Phase 2 (In-Memory Storage) are implemented. Traversal engine is stubbed.

## Build & Test Commands

```bash
cargo build                          # Build debug
cargo build --release                # Build release
cargo test                           # Run all tests
cargo test test_name                 # Run single test by name
cargo test test_name -- --exact      # Exact match
cargo test storage::inmemory::tests  # Tests in specific module
cargo test -- --nocapture            # Show test output
cargo test -- --ignored              # Run ignored tests
cargo check                          # Check without building
cargo clippy -- -D warnings          # Lint with warnings as errors
cargo fmt --check                    # Check formatting
cargo bench                          # Run benchmarks
cargo test --features mmap           # Test with feature flags
cargo doc --open                     # Build and open docs

```

### Coverage

Aim for 100% branch coverage.

```bash
cargo +nightly llvm-cov --branch --html        # Generate HTML report with branch coverage
cargo +nightly llvm-cov --branch --html --open # Generate and open in browser
```

## Project Structure

```
src/
├── lib.rs           # Public API, prelude
├── graph.rs         # Graph, GraphSnapshot, GraphMut
├── value.rs         # Value enum, VertexId, EdgeId
├── error.rs         # Error types (thiserror)
├── storage/         # GraphStorage trait + backends
├── traversal/       # Fluent API steps
└── algorithms/      # BFS, DFS, path algorithms
```

## Code Style

### Naming
- Types: `PascalCase` — `GraphTraversalSource`, `Value`
- Functions: `snake_case` — `out_edges()`, `has_label()`
- Rust keywords: trailing underscore — `in_()`, `where_()`, `as_()`
- Constants: `SCREAMING_SNAKE_CASE` — `NODE_RECORD_SIZE`

### Types
```rust
// Newtype pattern for IDs
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Ord, PartialOrd)]
pub struct VertexId(pub(crate) u64);

// #[repr(C, packed)] for on-disk structures
#[repr(C, packed)]
pub struct NodeRecord { pub id: u64, pub label_id: u32 }
```

### Error Handling
```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("vertex not found: {0:?}")]
    VertexNotFound(VertexId),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

// All fallible ops return Result; no panics in library code
pub fn get_vertex(&self, id: VertexId) -> Result<Vertex, StorageError>;
```

### Imports
```rust
// Order: std, external crates, local modules
use std::collections::HashMap;
use hashbrown::HashMap as FastHashMap;
use parking_lot::RwLock;
use crate::value::{Value, VertexId};
```

### Traits
```rust
pub trait Step<In, Out>: Clone {
    type Iter: Iterator<Item = Traverser<Out>>;
    fn apply<I>(self, input: I) -> Self::Iter
    where I: Iterator<Item = Traverser<In>>;
}

// Thread-safe bounds
pub trait GraphStorage: Send + Sync { }
```

### Performance
```rust
#[inline]
pub fn get_node(&self, id: VertexId) -> Option<&NodeRecord> { }

// Prefer iterators (lazy) over Vec (eager)
fn vertices(&self) -> impl Iterator<Item = Vertex>;

// SmallVec for small collections
labels: SmallVec<[String; 2]>,
```

### Testing
```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_add_vertex() {
        let graph = InMemoryGraph::new();
        let id = graph.add_vertex("person", HashMap::new());
        assert!(graph.get_vertex(id).is_some());
    }
}

// Property-based tests with proptest
proptest! {
    #[test]
    fn roundtrip_value(value: Value) {
        let parsed = Value::deserialize(&value.serialize()).unwrap();
        assert_eq!(value, parsed);
    }
}
```



Report output: `target/llvm-cov/html/index.html`

## Key Design Principles

1. **Lazy evaluation**: Pull-based iterator model, no work until terminal step
2. **Zero-cost abstractions**: Monomorphized hot paths, boxed for complex branches
3. **Type safety**: Compile-time traversal step verification
4. **Unified API**: Both storage backends expose identical interface
5. **Result-based errors**: No panics in library code

## Dependencies

```toml
[dependencies]
thiserror = "1.0"
hashbrown = "0.14"
smallvec = "1.11"
parking_lot = "0.12"
roaring = "0.10"
serde = { version = "1.0", features = ["derive"] }
memmap2 = { version = "0.9", optional = true }

[dev-dependencies]
criterion = "0.5"
proptest = "1.4"
tempfile = "3.10"
```

## Reference Documentation

- `overview/overview.md` - Main design document
- `overview/storage.md` - Storage architecture
- `overview/algorithms.md` - Traversal execution
- `overview/gremlin.md` - Gremlin API mapping
- `overview/anonymous_traversal.md` - `__` factory patterns
- `specs/implementation.md` - Implementation phases
