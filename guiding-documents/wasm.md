# WASM + JavaScript Bindings

This document outlines the plan for compiling Intersteller to WebAssembly with full JavaScript bindings, enabling browser-based graph traversal for demos and playgrounds.

## Goals

1. **Full traversal API** exposed to JavaScript with Gremlin-style fluent interface
2. **Graph construction/mutation** APIs for building graphs in the browser
3. **Demo/playground** use case - interactive graph exploration
4. **In-memory storage only** - mmap and full-text features are not WASM-compatible

## Compatibility Assessment

### WASM-Compatible Components

| Component | Status | Notes |
|-----------|--------|-------|
| InMemoryGraph | Ready | Pure Rust HashMaps |
| Traversal engine | Ready | Pure iterators |
| Value types | Ready | No system dependencies |
| StringInterner | Ready | Pure Rust |
| hashbrown | Ready | Pure Rust HashMap |
| smallvec | Ready | Pure Rust |
| serde | Ready | Pure Rust |
| roaring | Ready | Pure Rust bitmaps |
| regex | Ready | Has wasm32 support |

### Components Requiring Abstraction

| Component | Issue | Solution |
|-----------|-------|----------|
| `parking_lot::RwLock` | OS threading primitives | `RefCell` wrapper for WASM |
| `Send + Sync` bounds | Thread-safety markers | Conditional compilation |
| `AtomicU64` | Works but overkill | `Cell<u64>` for single-threaded |

### Incompatible Features (Excluded)

| Feature | Reason |
|---------|--------|
| `mmap` | Requires OS memory mapping |
| `full-text` | Tantivy uses threading/filesystem |

## Architecture

### Crate Structure

```
intersteller/
├── src/
│   ├── lib.rs
│   ├── sync.rs          # NEW: Platform-specific sync primitives
│   └── ...
├── intersteller-wasm/    # NEW: WASM bindings crate
│   ├── Cargo.toml
│   ├── src/
│   │   ├── lib.rs       # wasm-bindgen exports
│   │   ├── graph.rs     # JsGraph wrapper
│   │   ├── traversal.rs # JsTraversal wrapper
│   │   └── value.rs     # JS <-> Rust value conversion
│   └── pkg/             # Generated JS/TS files (wasm-pack output)
```

### Why a Separate Crate?

1. **Clean separation** - WASM bindings don't pollute the core library
2. **Different dependencies** - `wasm-bindgen`, `js-sys`, `web-sys` only in WASM crate
3. **Flexible packaging** - Core crate remains pure Rust, WASM crate is npm-publishable
4. **Build isolation** - WASM build config doesn't affect native builds

## Implementation Plan

### Phase 1: Core Library WASM Compatibility

**Goal:** Make `intersteller` compile for `wasm32-unknown-unknown` target.

#### 1.1 Create Sync Abstraction Layer

Create `src/sync.rs`:

```rust
//! Platform-specific synchronization primitives.
//!
//! On native platforms, uses `parking_lot` for high-performance locking.
//! On WASM, uses single-threaded `RefCell` wrappers since WASM is single-threaded.

#[cfg(target_arch = "wasm32")]
mod inner {
    use std::cell::{Ref, RefCell, RefMut};

    /// Single-threaded RwLock using RefCell for WASM.
    pub struct RwLock<T>(RefCell<T>);

    impl<T> RwLock<T> {
        pub fn new(value: T) -> Self {
            Self(RefCell::new(value))
        }

        pub fn read(&self) -> Ref<'_, T> {
            self.0.borrow()
        }

        pub fn write(&self) -> RefMut<'_, T> {
            self.0.borrow_mut()
        }
    }

    // Type aliases to match parking_lot API
    pub type RwLockReadGuard<'a, T> = Ref<'a, T>;
    pub type RwLockWriteGuard<'a, T> = RefMut<'a, T>;
}

#[cfg(not(target_arch = "wasm32"))]
mod inner {
    pub use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
}

pub use inner::*;
```

#### 1.2 Update Imports

Replace direct `parking_lot` imports with the sync abstraction:

**`src/graph.rs`:**
```rust
// Before
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

// After
use crate::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
```

**`src/traversal/context.rs`:**
```rust
// Before
use parking_lot::RwLock;

// After
use crate::sync::RwLock;
```

#### 1.3 Conditional Trait Bounds

**`src/storage/mod.rs`:**
```rust
#[cfg(not(target_arch = "wasm32"))]
pub trait GraphStorage: Send + Sync {
    // ... trait methods
}

#[cfg(target_arch = "wasm32")]
pub trait GraphStorage {
    // ... same trait methods
}
```

#### 1.4 Update Cargo.toml

```toml
[features]
default = ["inmemory"]
inmemory = []
mmap = ["memmap2"]
full-text = ["tantivy"]

[dependencies]
thiserror = "1.0"
hashbrown = "0.14"
smallvec = "1.11"
serde = { version = "1.0", features = ["derive"] }
roaring = "0.10"
regex = "1.10"
crc32fast = "1.3"

# parking_lot only on native
[target.'cfg(not(target_arch = "wasm32"))'.dependencies]
parking_lot = "0.12"

# Optional, native-only
memmap2 = { version = "0.9", optional = true }
tantivy = { version = "0.21", optional = true }
```

#### 1.5 Verify WASM Build

```bash
rustup target add wasm32-unknown-unknown
cargo build --target wasm32-unknown-unknown --no-default-features --features inmemory
```

### Phase 2: WASM Bindings Crate

**Goal:** Create `intersteller-wasm` crate with JavaScript bindings.

#### 2.1 Create Crate Structure

```bash
mkdir -p intersteller-wasm/src
```

**`intersteller-wasm/Cargo.toml`:**
```toml
[package]
name = "intersteller-wasm"
version = "0.1.0"
edition = "2021"
description = "WebAssembly bindings for Intersteller graph database"

[lib]
crate-type = ["cdylib", "rlib"]

[dependencies]
intersteller = { path = "..", default-features = false, features = ["inmemory"] }
wasm-bindgen = "0.2"
js-sys = "0.3"
serde = { version = "1.0", features = ["derive"] }
serde-wasm-bindgen = "0.6"

[dependencies.web-sys]
version = "0.3"
features = ["console"]

[dev-dependencies]
wasm-bindgen-test = "0.3"

[profile.release]
opt-level = "s"       # Optimize for size
lto = true            # Link-time optimization
```

#### 2.2 JavaScript API Design

The JS API should feel natural to JavaScript developers while maintaining Gremlin semantics.

**Graph Construction:**
```javascript
import { Graph } from 'intersteller';

const graph = new Graph();

// Add vertices
const alice = graph.addVertex('person', { name: 'Alice', age: 30 });
const bob = graph.addVertex('person', { name: 'Bob', age: 25 });
const company = graph.addVertex('company', { name: 'Acme Corp' });

// Add edges
graph.addEdge(alice, bob, 'knows', { since: 2020 });
graph.addEdge(alice, company, 'worksAt', { role: 'Engineer' });

// Bulk load from JSON
graph.loadJson({
  vertices: [...],
  edges: [...]
});
```

**Traversal API:**
```javascript
// Get traversal source
const g = graph.traversal();

// Fluent traversal - returns Promise for lazy evaluation
const friends = await g.V()
  .has('person', 'name', 'Alice')
  .out('knows')
  .values('name')
  .toList();

// With predicates
const adults = await g.V()
  .hasLabel('person')
  .has('age', P.gte(18))
  .toList();

// Path tracking
const paths = await g.V()
  .has('name', 'Alice')
  .out('knows')
  .out('knows')
  .path()
  .toList();
```

#### 2.3 Wrapper Types

**`intersteller-wasm/src/lib.rs`:**
```rust
use wasm_bindgen::prelude::*;

mod graph;
mod traversal;
mod value;
mod predicate;

pub use graph::JsGraph;
pub use traversal::JsTraversal;
pub use predicate::P;

#[wasm_bindgen(start)]
pub fn init() {
    // Set up panic hook for better error messages
    console_error_panic_hook::set_once();
}
```

**`intersteller-wasm/src/graph.rs`:**
```rust
use wasm_bindgen::prelude::*;
use intersteller::{Graph, InMemoryGraph, VertexId, EdgeId, Value};
use std::collections::HashMap;

#[wasm_bindgen]
pub struct JsGraph {
    inner: Graph,
}

#[wasm_bindgen]
impl JsGraph {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: Graph::new(InMemoryGraph::new()),
        }
    }

    /// Add a vertex with label and properties
    #[wasm_bindgen(js_name = addVertex)]
    pub fn add_vertex(&self, label: &str, properties: JsValue) -> Result<u64, JsError> {
        let props: HashMap<String, Value> = serde_wasm_bindgen::from_value(properties)?;
        let mut graph = self.inner.mutate();
        let id = graph.add_vertex(label, props)?;
        Ok(id.0)
    }

    /// Add an edge between vertices
    #[wasm_bindgen(js_name = addEdge)]
    pub fn add_edge(
        &self,
        from: u64,
        to: u64,
        label: &str,
        properties: JsValue,
    ) -> Result<u64, JsError> {
        let props: HashMap<String, Value> = serde_wasm_bindgen::from_value(properties)?;
        let mut graph = self.inner.mutate();
        let id = graph.add_edge(VertexId(from), VertexId(to), label, props)?;
        Ok(id.0)
    }

    /// Get a traversal source
    #[wasm_bindgen]
    pub fn traversal(&self) -> JsTraversalSource {
        JsTraversalSource {
            graph: self.inner.clone(),
        }
    }

    /// Load graph from JSON
    #[wasm_bindgen(js_name = loadJson)]
    pub fn load_json(&self, data: JsValue) -> Result<(), JsError> {
        // Parse and load vertices/edges
        todo!()
    }

    /// Export graph to JSON
    #[wasm_bindgen(js_name = toJson)]
    pub fn to_json(&self) -> Result<JsValue, JsError> {
        todo!()
    }

    /// Get vertex count
    #[wasm_bindgen(js_name = vertexCount)]
    pub fn vertex_count(&self) -> usize {
        self.inner.snapshot().vertex_count()
    }

    /// Get edge count
    #[wasm_bindgen(js_name = edgeCount)]
    pub fn edge_count(&self) -> usize {
        self.inner.snapshot().edge_count()
    }
}
```

**`intersteller-wasm/src/traversal.rs`:**
```rust
use wasm_bindgen::prelude::*;
use intersteller::{Graph, GraphTraversal};

#[wasm_bindgen]
pub struct JsTraversalSource {
    pub(crate) graph: Graph,
}

#[wasm_bindgen]
impl JsTraversalSource {
    /// Start traversal from all vertices
    #[wasm_bindgen(js_name = "V")]
    pub fn v(&self, ids: Option<Vec<u64>>) -> JsTraversal {
        // Return traversal wrapper
        todo!()
    }

    /// Start traversal from all edges
    #[wasm_bindgen(js_name = "E")]
    pub fn e(&self, ids: Option<Vec<u64>>) -> JsTraversal {
        todo!()
    }
}

#[wasm_bindgen]
pub struct JsTraversal {
    // Internal traversal state
}

#[wasm_bindgen]
impl JsTraversal {
    // Navigation steps
    #[wasm_bindgen]
    pub fn out(&self, labels: Option<Vec<String>>) -> JsTraversal { todo!() }

    #[wasm_bindgen(js_name = "in")]
    pub fn in_(&self, labels: Option<Vec<String>>) -> JsTraversal { todo!() }

    #[wasm_bindgen]
    pub fn both(&self, labels: Option<Vec<String>>) -> JsTraversal { todo!() }

    #[wasm_bindgen(js_name = outE)]
    pub fn out_e(&self, labels: Option<Vec<String>>) -> JsTraversal { todo!() }

    #[wasm_bindgen(js_name = inE)]
    pub fn in_e(&self, labels: Option<Vec<String>>) -> JsTraversal { todo!() }

    #[wasm_bindgen(js_name = bothE)]
    pub fn both_e(&self, labels: Option<Vec<String>>) -> JsTraversal { todo!() }

    #[wasm_bindgen(js_name = outV)]
    pub fn out_v(&self) -> JsTraversal { todo!() }

    #[wasm_bindgen(js_name = inV)]
    pub fn in_v(&self) -> JsTraversal { todo!() }

    #[wasm_bindgen(js_name = bothV)]
    pub fn both_v(&self) -> JsTraversal { todo!() }

    // Filter steps
    #[wasm_bindgen(js_name = hasLabel)]
    pub fn has_label(&self, labels: Vec<String>) -> JsTraversal { todo!() }

    #[wasm_bindgen]
    pub fn has(&self, key: &str, value: JsValue) -> JsTraversal { todo!() }

    #[wasm_bindgen(js_name = hasNot)]
    pub fn has_not(&self, key: &str) -> JsTraversal { todo!() }

    #[wasm_bindgen(js_name = hasId)]
    pub fn has_id(&self, ids: Vec<u64>) -> JsTraversal { todo!() }

    #[wasm_bindgen]
    pub fn filter(&self, predicate: &js_sys::Function) -> JsTraversal { todo!() }

    #[wasm_bindgen]
    pub fn dedup(&self) -> JsTraversal { todo!() }

    #[wasm_bindgen]
    pub fn limit(&self, n: usize) -> JsTraversal { todo!() }

    #[wasm_bindgen]
    pub fn skip(&self, n: usize) -> JsTraversal { todo!() }

    // Transform steps
    #[wasm_bindgen]
    pub fn values(&self, keys: Vec<String>) -> JsTraversal { todo!() }

    #[wasm_bindgen]
    pub fn id(&self) -> JsTraversal { todo!() }

    #[wasm_bindgen]
    pub fn label(&self) -> JsTraversal { todo!() }

    #[wasm_bindgen(js_name = valueMap)]
    pub fn value_map(&self) -> JsTraversal { todo!() }

    #[wasm_bindgen(js_name = elementMap)]
    pub fn element_map(&self) -> JsTraversal { todo!() }

    // Path tracking
    #[wasm_bindgen]
    pub fn path(&self) -> JsTraversal { todo!() }

    // Branching
    #[wasm_bindgen]
    pub fn union(&self, traversals: Vec<JsTraversal>) -> JsTraversal { todo!() }

    #[wasm_bindgen]
    pub fn coalesce(&self, traversals: Vec<JsTraversal>) -> JsTraversal { todo!() }

    #[wasm_bindgen]
    pub fn choose(&self, predicate: JsValue, t: JsTraversal, f: JsTraversal) -> JsTraversal { todo!() }

    // Repeat
    #[wasm_bindgen]
    pub fn repeat(&self, traversal: JsTraversal) -> JsTraversal { todo!() }

    #[wasm_bindgen]
    pub fn times(&self, n: usize) -> JsTraversal { todo!() }

    #[wasm_bindgen]
    pub fn until(&self, predicate: JsValue) -> JsTraversal { todo!() }

    // Terminal steps - these execute the traversal
    #[wasm_bindgen(js_name = toList)]
    pub fn to_list(&self) -> Result<JsValue, JsError> { todo!() }

    #[wasm_bindgen]
    pub fn next(&self) -> Result<JsValue, JsError> { todo!() }

    #[wasm_bindgen(js_name = hasNext)]
    pub fn has_next(&self) -> bool { todo!() }

    #[wasm_bindgen]
    pub fn count(&self) -> Result<u64, JsError> { todo!() }

    #[wasm_bindgen]
    pub fn fold(&self) -> Result<JsValue, JsError> { todo!() }
}
```

**`intersteller-wasm/src/predicate.rs`:**
```rust
use wasm_bindgen::prelude::*;

/// Predicate factory for comparison operations
#[wasm_bindgen]
pub struct P;

#[wasm_bindgen]
impl P {
    #[wasm_bindgen]
    pub fn eq(value: JsValue) -> JsPredicate { todo!() }

    #[wasm_bindgen]
    pub fn neq(value: JsValue) -> JsPredicate { todo!() }

    #[wasm_bindgen]
    pub fn lt(value: JsValue) -> JsPredicate { todo!() }

    #[wasm_bindgen]
    pub fn lte(value: JsValue) -> JsPredicate { todo!() }

    #[wasm_bindgen]
    pub fn gt(value: JsValue) -> JsPredicate { todo!() }

    #[wasm_bindgen]
    pub fn gte(value: JsValue) -> JsPredicate { todo!() }

    #[wasm_bindgen]
    pub fn within(values: Vec<JsValue>) -> JsPredicate { todo!() }

    #[wasm_bindgen]
    pub fn without(values: Vec<JsValue>) -> JsPredicate { todo!() }

    #[wasm_bindgen]
    pub fn between(min: JsValue, max: JsValue) -> JsPredicate { todo!() }

    #[wasm_bindgen]
    pub fn regex(pattern: &str) -> JsPredicate { todo!() }
}

#[wasm_bindgen]
pub struct JsPredicate {
    // Internal predicate representation
}
```

### Phase 3: Build & Package

#### 3.1 Install wasm-pack

```bash
cargo install wasm-pack
```

#### 3.2 Build WASM Package

```bash
cd intersteller-wasm
wasm-pack build --target web      # For ES modules
wasm-pack build --target bundler  # For webpack/rollup
wasm-pack build --target nodejs   # For Node.js
```

#### 3.3 Output Structure

```
intersteller-wasm/pkg/
├── intersteller_wasm.js      # JS glue code
├── intersteller_wasm.d.ts    # TypeScript definitions
├── intersteller_wasm_bg.wasm # WASM binary
├── intersteller_wasm_bg.js   # Background JS
└── package.json             # npm package metadata
```

### Phase 4: Demo/Playground

#### 4.1 Simple HTML Demo

```html
<!DOCTYPE html>
<html>
<head>
  <title>Intersteller Playground</title>
</head>
<body>
  <script type="module">
    import init, { Graph, P } from './pkg/intersteller_wasm.js';

    async function main() {
      await init();

      const graph = new Graph();

      // Build a social network
      const alice = graph.addVertex('person', { name: 'Alice', age: 30 });
      const bob = graph.addVertex('person', { name: 'Bob', age: 25 });
      const charlie = graph.addVertex('person', { name: 'Charlie', age: 35 });

      graph.addEdge(alice, bob, 'knows', { since: 2020 });
      graph.addEdge(bob, charlie, 'knows', { since: 2021 });
      graph.addEdge(alice, charlie, 'knows', { since: 2019 });

      // Query: Friends of Alice
      const g = graph.traversal();
      const friends = await g.V()
        .has('name', 'Alice')
        .out('knows')
        .values('name')
        .toList();

      console.log('Alice\'s friends:', friends);

      // Query: People over 25
      const adults = await g.V()
        .hasLabel('person')
        .has('age', P.gt(25))
        .values('name')
        .toList();

      console.log('People over 25:', adults);
    }

    main();
  </script>
</body>
</html>
```

#### 4.2 Interactive Playground Features (Future)

- Code editor with syntax highlighting
- Graph visualization (using D3.js or similar)
- Query history
- Sample datasets (Marvel, British Royals, NBA from examples/)
- Share queries via URL

## Testing

### Rust Unit Tests

```bash
cd intersteller-wasm
cargo test
```

### WASM Integration Tests

```bash
wasm-pack test --headless --chrome
wasm-pack test --headless --firefox
```

### Browser Manual Testing

```bash
# Serve the demo
python -m http.server 8080
# Open http://localhost:8080
```

## Performance Considerations

### WASM Size Optimization

```toml
# Cargo.toml
[profile.release]
opt-level = "s"       # Optimize for size (or "z" for even smaller)
lto = true            # Link-time optimization
codegen-units = 1     # Better optimization, slower compile
panic = "abort"       # Smaller binary, no unwinding
```

### Runtime Performance

- WASM runs at near-native speed for compute
- JS <-> WASM boundary crossings have overhead
- Batch operations when possible (use `toList()` over many `next()` calls)
- Large graphs should use typed arrays for bulk loading

## Limitations

1. **Single-threaded** - No parallel traversal in browser
2. **Memory limits** - Browser WASM has ~4GB max memory
3. **No persistence** - In-memory only (could add IndexedDB later)
4. **No full-text search** - Tantivy not available

## Future Enhancements

1. **IndexedDB persistence** - Save/load graphs to browser storage
2. **Web Workers** - Offload heavy traversals to background thread
3. **Streaming results** - Async iterators for large result sets
4. **Graph visualization** - Built-in rendering component
5. **npm package** - Publish to npm registry

## References

- [wasm-bindgen Guide](https://rustwasm.github.io/docs/wasm-bindgen/)
- [wasm-pack Documentation](https://rustwasm.github.io/docs/wasm-pack/)
- [Rust WASM Book](https://rustwasm.github.io/docs/book/)
- [Apache TinkerPop Gremlin](https://tinkerpop.apache.org/gremlin.html)
