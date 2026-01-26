# Interstellar: WebAssembly (WASM) Support

This document specifies changes required to compile Interstellar for WebAssembly targets, enabling use in browsers and Node.js environments.

---

## 1. Overview and Motivation

### 1.1 Current State

Interstellar currently targets native platforms only:
- Uses `std::time::Instant` and `SystemTime` (limited WASM support)
- Uses `rand::thread_rng()` without WASM entropy configuration
- File I/O functions assume filesystem access
- No target-specific dependency configuration

```
+------------------------------------------------------------------+
|              Current: Native-Only Build                           |
+------------------------------------------------------------------+
|                                                                   |
|   Issues for WASM:                                                |
|   - std::time::Instant panics on wasm32-unknown-unknown           |
|   - rand::thread_rng() has no entropy source                      |
|   - File I/O functions will fail at runtime                       |
|   - mmap/tantivy features incompatible                            |
|                                                                   |
+------------------------------------------------------------------+
```

### 1.2 Proposed Solution

Use target-based conditional compilation to support WASM:

```
+------------------------------------------------------------------+
|              Proposed: Automatic WASM Detection                   |
+------------------------------------------------------------------+
|                                                                   |
|   Cargo.toml:                                                     |
|   [target.'cfg(target_arch = "wasm32")'.dependencies]             |
|   getrandom = { version = "0.2", features = ["js"] }              |
|   web-time = "1.0"                                                |
|                                                                   |
|   Source code:                                                    |
|   #[cfg(not(target_arch = "wasm32"))]                             |
|   use std::time::Instant;                                         |
|                                                                   |
|   #[cfg(target_arch = "wasm32")]                                  |
|   use web_time::Instant;                                          |
|                                                                   |
|   Benefits:                                                       |
|   - No explicit feature flag needed                               |
|   - Users just build with --target wasm32-unknown-unknown         |
|   - Incompatible code excluded at compile time                    |
|   - Clean separation of platform-specific code                    |
|                                                                   |
+------------------------------------------------------------------+
```

### 1.3 Design Goals

| Goal | Description |
|------|-------------|
| Automatic detection | Use `#[cfg(target_arch = "wasm32")]` instead of feature flags |
| Browser + Node.js | Support both `wasm32-unknown-unknown` and Node.js WASM |
| Compile-time exclusion | Incompatible functions don't exist on WASM (not runtime errors) |
| Minimal changes | Only change what's necessary for WASM compatibility |
| Zero overhead | No runtime cost on native platforms |

### 1.4 Non-Goals

| Non-Goal | Rationale |
|----------|-----------|
| wasm-bindgen exports | Future spec - JS API design needs separate consideration |
| Feature flag for WASM | Automatic target detection is simpler |
| CI testing for WASM | Local testing only for now |

### 1.5 Future Work

- **wasm-bindgen exports**: Create JavaScript-friendly API with `#[wasm_bindgen]` attributes
- **npm package**: Publish to npm for easy JavaScript consumption
- **CI/CD**: Add WASM build and test to continuous integration

---

## 2. Target Environments

### 2.1 Supported WASM Targets

| Target | Environment | Use Case |
|--------|-------------|----------|
| `wasm32-unknown-unknown` | Browser | Web applications |
| `wasm32-unknown-unknown` | Node.js | Server-side JS |

### 2.2 Feature Compatibility Matrix

| Feature | Native | WASM | Notes |
|---------|--------|------|-------|
| Core `Graph` | Yes | Yes | In-memory storage |
| Traversal API | Yes | Yes | Full Gremlin-style API |
| `gql` | Yes | Yes | Query language (when enabled) |
| `graphson` (serialization) | Yes | Yes | String-based only on WASM |
| `graphson` (file I/O) | Yes | No | Compile-time excluded |
| `mmap` | Yes | No | Requires OS syscalls |
| `full-text` | Yes | No | Tantivy incompatible |

---

## 3. Cargo.toml Changes

### 3.1 Target-Specific Dependencies

Add WASM-specific dependencies that only compile for WASM targets:

```toml
# WASM support
[target.'cfg(target_arch = "wasm32")'.dependencies]
getrandom = { version = "0.2", features = ["js"] }
web-time = "1.1"
```

### 3.2 Dependency Notes

| Dependency | Purpose | Notes |
|------------|---------|-------|
| `getrandom` with `js` | Entropy for `rand` | Uses browser crypto API |
| `web-time` | `Instant`/`SystemTime` replacement | Uses `performance.now()` / `Date.now()` |

### 3.3 parking_lot Compatibility

The `parking_lot` crate automatically handles WASM targets in recent versions (0.12+). It uses single-threaded primitives when compiled for `wasm32`. No feature flag changes needed.

---

## 4. Source Code Changes

### 4.1 Time Abstraction Module

Create a new internal module to abstract time handling:

```rust
// src/time.rs

//! Platform-agnostic time utilities.
//!
//! On native platforms, uses `std::time`.
//! On WASM, uses `web-time` crate.

#[cfg(not(target_arch = "wasm32"))]
pub use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[cfg(target_arch = "wasm32")]
pub use web_time::{Instant, SystemTime, UNIX_EPOCH};
```

### 4.2 Files Requiring Time Import Changes

Update these files to use the new time module:

| File | Current Import | New Import |
|------|----------------|------------|
| `src/traversal/sideeffect.rs` | `use std::time::Instant` | `use crate::time::Instant` |
| `src/index/btree.rs` | `use std::time::SystemTime` | `use crate::time::SystemTime` |
| `src/index/unique.rs` | `use std::time::SystemTime` | `use crate::time::SystemTime` |

### 4.3 GraphSON File I/O Exclusion

Gate file-based functions to exclude them on WASM:

```rust
// src/graphson/mod.rs

/// Export graph to a file.
///
/// This function is not available on WASM targets.
/// Use [`export_to_string`] instead.
#[cfg(not(target_arch = "wasm32"))]
pub fn export_to_file<S, P>(storage: &S, path: P) -> Result<(), GraphSONError>
where
    S: GraphStorage,
    P: AsRef<std::path::Path>,
{
    // ... existing implementation
}

/// Import graph from a file.
///
/// This function is not available on WASM targets.
/// Use [`import_from_string`] instead.
#[cfg(not(target_arch = "wasm32"))]
pub fn import_from_file<P>(graph: &Graph, path: P) -> Result<GraphSONImportResult, GraphSONError>
where
    P: AsRef<std::path::Path>,
{
    // ... existing implementation
}

// String-based functions remain available on all platforms
pub fn export_to_string<S: GraphStorage>(storage: &S) -> Result<String, GraphSONError> {
    // ... existing implementation
}

pub fn import_from_string(graph: &Graph, json: &str) -> Result<GraphSONImportResult, GraphSONError> {
    // ... existing implementation
}
```

### 4.4 Module Export Updates

Update `src/lib.rs` to include the time module:

```rust
// src/lib.rs

// Internal time abstraction for WASM compatibility
pub(crate) mod time;
```

---

## 5. Feature Incompatibility

### 5.1 Compile-Time Errors

When users try to enable incompatible features on WASM, they'll get compile errors from the dependencies themselves:

| Feature | Error Source | Error Type |
|---------|--------------|------------|
| `mmap` | `memmap2` crate | Missing OS APIs |
| `full-text` | `tantivy` crate | Missing file I/O, threading |

### 5.2 Documentation

Add WASM compatibility notes to feature documentation in `src/lib.rs`:

```rust
//! ## Features
//!
//! - `graphson` (default): GraphSON import/export support
//! - `mmap`: Memory-mapped persistent storage (**not available on WASM**)
//! - `gql`: GQL query language
//! - `full-text`: Full-text search with Tantivy (**not available on WASM**)
//! - `full`: Enable all features
//!
//! ## WASM Support
//!
//! Interstellar supports WebAssembly targets (`wasm32-unknown-unknown`).
//! The following features work on WASM:
//!
//! - Core in-memory `Graph`
//! - Full traversal API
//! - GQL query language (with `gql` feature)
//! - GraphSON serialization (string-based only; file I/O excluded)
//!
//! Build for WASM:
//!
//! ```bash
//! cargo build --target wasm32-unknown-unknown
//! cargo build --target wasm32-unknown-unknown --features gql
//! ```
```

---

## 6. Files to Modify

| File | Change |
|------|--------|
| `Cargo.toml` | Add target-specific dependencies |
| `src/lib.rs` | Add `pub(crate) mod time;`, update docs |
| `src/time.rs` | **New file** - time abstraction |
| `src/traversal/sideeffect.rs` | Change `std::time::Instant` to `crate::time::Instant` |
| `src/index/btree.rs` | Change `std::time::SystemTime` to `crate::time::SystemTime` |
| `src/index/unique.rs` | Change `std::time::SystemTime` to `crate::time::SystemTime` |
| `src/graphson/mod.rs` | Add `#[cfg(not(target_arch = "wasm32"))]` to file I/O functions |

---

## 7. Implementation Checklist

### Phase 1: Dependencies

- [ ] Add `[target.'cfg(target_arch = "wasm32")'.dependencies]` section to Cargo.toml
- [ ] Add `getrandom = { version = "0.2", features = ["js"] }`
- [ ] Add `web-time = "1.1"`

### Phase 2: Time Abstraction

- [ ] Create `src/time.rs` with platform-conditional imports
- [ ] Add `pub(crate) mod time;` to `src/lib.rs`
- [ ] Update `src/traversal/sideeffect.rs` to use `crate::time::Instant`
- [ ] Update `src/index/btree.rs` to use `crate::time::SystemTime`
- [ ] Update `src/index/unique.rs` to use `crate::time::SystemTime`

### Phase 3: GraphSON File I/O

- [ ] Add `#[cfg(not(target_arch = "wasm32"))]` to `export_to_file`
- [ ] Add `#[cfg(not(target_arch = "wasm32"))]` to `import_from_file`
- [ ] Update function documentation to note WASM unavailability

### Phase 4: Documentation

- [ ] Update feature documentation in `src/lib.rs`
- [ ] Update README.md with WASM build instructions

### Phase 5: Verification

- [ ] `cargo build` succeeds (native)
- [ ] `cargo build --target wasm32-unknown-unknown` succeeds
- [ ] `cargo build --target wasm32-unknown-unknown --features gql` succeeds
- [ ] `cargo test` succeeds (native)
- [ ] Verify `mmap` feature fails to compile on WASM (expected)

---

## 8. Local Testing

### 8.1 Setup

Install the WASM target:

```bash
rustup target add wasm32-unknown-unknown
```

### 8.2 Build Commands

```bash
# Basic WASM build
cargo build --target wasm32-unknown-unknown

# With GQL
cargo build --target wasm32-unknown-unknown --features gql

# Release build
cargo build --target wasm32-unknown-unknown --release

# Check only (faster)
cargo check --target wasm32-unknown-unknown
```

### 8.3 Verify Incompatible Features Fail

```bash
# These should fail to compile (expected)
cargo build --target wasm32-unknown-unknown --features mmap
cargo build --target wasm32-unknown-unknown --features full-text
```

### 8.4 Future: Runtime Testing with wasm-pack

For future runtime testing (not in scope for this spec):

```bash
# Install wasm-pack
cargo install wasm-pack

# Run tests in headless browser
wasm-pack test --headless --firefox
wasm-pack test --headless --chrome

# Run tests in Node.js
wasm-pack test --node
```

---

## 9. Usage Examples

### 9.1 Building for Browser

```bash
# Install target
rustup target add wasm32-unknown-unknown

# Build
cargo build --target wasm32-unknown-unknown --features gql --release

# Output at: target/wasm32-unknown-unknown/release/interstellar.wasm
```

### 9.2 Using with wasm-bindgen (Future)

Once wasm-bindgen exports are added (future spec):

```bash
# Install wasm-pack
cargo install wasm-pack

# Build npm package
wasm-pack build --target web

# Use in JavaScript
import init, { Graph } from './pkg/interstellar.js';

await init();
const graph = new Graph();
```

### 9.3 GraphSON on WASM

```rust
use interstellar::prelude::*;
use interstellar::graphson;

let graph = Graph::new();
// ... add data ...

// Works on WASM - string-based serialization
let json = graphson::export_to_string(&graph.snapshot())?;
let result = graphson::import_from_string(&graph, &json)?;

// Does NOT exist on WASM - file I/O excluded at compile time
// graphson::export_to_file(&graph.snapshot(), "graph.json")?;  // Compile error on WASM
```

---

## 10. Future Considerations

### 10.1 wasm-bindgen JavaScript API

A future spec should define:

- Which types to export (`Graph`, `Value`, etc.)
- JavaScript-friendly naming conventions
- Error handling (JS exceptions vs Result)
- Async API design for large operations
- Memory management guidance

### 10.2 npm Package

Consider publishing to npm:

```bash
wasm-pack build --target bundler
wasm-pack publish
```

### 10.3 CI/CD Integration

Future CI additions:

```yaml
jobs:
  wasm:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install WASM target
        run: rustup target add wasm32-unknown-unknown
      - name: Build WASM
        run: cargo build --target wasm32-unknown-unknown --features gql
      - name: Test WASM (future)
        run: wasm-pack test --headless --firefox
```

### 10.4 Bundle Size Optimization

For production WASM builds:

```bash
# Use wasm-opt for smaller bundles
cargo install wasm-opt
wasm-opt -Oz -o optimized.wasm target/wasm32-unknown-unknown/release/interstellar.wasm
```

Consider `wee_alloc` for smaller allocator (tradeoff: slower allocations).
