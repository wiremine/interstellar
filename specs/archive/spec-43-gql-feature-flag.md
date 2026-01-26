# Interstellar: GQL as Optional Feature

This document specifies making GQL (Graph Query Language) an optional feature that can be enabled via Cargo feature flags, reducing compile time and binary size for users who don't need query language support.

---

## 1. Overview and Motivation

### 1.1 Current State

GQL is currently a required part of Interstellar:
- Always compiled, even when unused
- Adds ~3 dependencies (`pest`, `pest_derive`, `mathexpr`)
- Increases compile time due to pest grammar generation
- Increases binary size with parser/compiler code

```
+------------------------------------------------------------------+
|              Current: GQL Always Included                         |
+------------------------------------------------------------------+
|                                                                   |
|   Cargo.toml:                                                     |
|   [dependencies]                                                  |
|   pest = "2.7"           # Always compiled                        |
|   pest_derive = "2.7"    # Always compiled                        |
|   mathexpr = "0.1.1"     # Always compiled                        |
|                                                                   |
|   src/lib.rs:                                                     |
|   pub mod gql;           # Always exported                        |
|                                                                   |
|   Problem:                                                        |
|   - Users who only need traversal API pay for GQL                 |
|   - Increased compile time for all users                          |
|   - Larger binary size even when GQL unused                       |
|                                                                   |
+------------------------------------------------------------------+
```

### 1.2 Proposed Solution

Make GQL an opt-in feature via Cargo feature flags:

```
+------------------------------------------------------------------+
|              Proposed: GQL as Optional Feature                    |
+------------------------------------------------------------------+
|                                                                   |
|   Cargo.toml:                                                     |
|   [features]                                                      |
|   gql = ["pest", "pest_derive", "mathexpr"]                       |
|   full = ["mmap", "graphson", "gql", ...]                         |
|                                                                   |
|   [dependencies]                                                  |
|   pest = { version = "2.7", optional = true }                     |
|   pest_derive = { version = "2.7", optional = true }              |
|   mathexpr = { version = "0.1.1", optional = true }               |
|                                                                   |
|   Benefits:                                                       |
|   - Faster compile for users who don't need GQL                   |
|   - Smaller binary size                                           |
|   - Clear dependency boundaries                                   |
|   - `full` feature for users who want everything                  |
|                                                                   |
+------------------------------------------------------------------+
```

### 1.3 Design Goals

| Goal | Description |
|------|-------------|
| Opt-in GQL | GQL only compiled when `gql` feature enabled |
| Not in default | Default features remain `["graphson"]` |
| Full feature | New `full` feature enables all optional features |
| Remove dead feature | Remove unused `inmemory` feature (no-op) |
| Clean boundaries | All GQL code behind `#[cfg(feature = "gql")]` |
| No API breakage | Existing code works unchanged when `gql` enabled |

### 1.4 Non-Goals

| Non-Goal | Rationale |
|----------|-----------|
| Splitting GQL further | Single `gql` feature covers all GQL functionality |
| Runtime feature detection | Compile-time only via Cargo features |
| Deprecating GQL | GQL remains fully supported when enabled |

---

## 2. Cargo.toml Changes

### 2.1 Feature Definitions

```toml
[features]
default = ["graphson"]
mmap = ["memmap2", "serde_json"]
graphson = ["serde_json"]
full-text = ["tantivy"]

# NEW: GQL feature
gql = ["pest", "pest_derive", "mathexpr"]

# NEW: Full feature enabling everything
full = ["mmap", "graphson", "gql", "full-text"]
```

Note: The existing `inmemory` feature is removed as it was a no-op (defined as `inmemory = []` but never used in any `#[cfg]` attributes). In-memory storage is always available as the core functionality.

### 2.2 Dependency Changes

```toml
[dependencies]
# ... existing dependencies ...

# Make GQL dependencies optional
pest = { version = "2.7", optional = true }
pest_derive = { version = "2.7", optional = true }
mathexpr = { version = "0.1.1", optional = true }
```

### 2.3 Example Configuration

```toml
[[example]]
name = "quickstart_gql"
required-features = ["gql"]

[[example]]
name = "nba"
required-features = ["mmap", "gql"]
```

---

## 3. Source Code Changes

### 3.1 Module Export (src/lib.rs)

```rust
// src/lib.rs

// Conditionally export the gql module
#[cfg(feature = "gql")]
pub mod gql;
```

Update module-level documentation to note GQL is feature-gated:

```rust
//! ## Features
//!
//! - `graphson` (default): GraphSON import/export support
//! - `mmap`: Memory-mapped persistent storage
//! - `gql`: GQL query language support
//! - `full-text`: Full-text search with Tantivy
//! - `full`: Enable all features
//!
//! Note: In-memory graph storage is always available (core functionality).
```

### 3.2 Storage Integration (src/storage/cow.rs)

```rust
// src/storage/cow.rs

// Conditional import
#[cfg(feature = "gql")]
use crate::gql::{self, GqlError};

impl Graph {
    // ... existing methods ...

    /// Execute a GQL query against the current graph state.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use interstellar::prelude::*;
    /// let graph = Graph::new();
    /// // ... add data ...
    /// let results = graph.gql("MATCH (n:Person) RETURN n.name")?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `GqlError` if parsing or execution fails.
    #[cfg(feature = "gql")]
    pub fn gql(&self, query: &str) -> Result<Vec<Value>, GqlError> {
        // ... existing implementation ...
    }

    /// Execute a GQL query with parameters.
    #[cfg(feature = "gql")]
    pub fn gql_with_params(
        &self,
        query: &str,
        params: gql::Parameters,
    ) -> Result<Vec<Value>, GqlError> {
        // ... existing implementation ...
    }
}
```

### 3.3 Storage Integration (src/storage/cow_mmap.rs)

```rust
// src/storage/cow_mmap.rs

// Conditional import
#[cfg(feature = "gql")]
use crate::gql::{self, GqlError};

impl CowMmapGraph {
    // ... existing methods ...

    /// Execute a GQL query against the current graph state.
    #[cfg(feature = "gql")]
    pub fn gql(&self, query: &str) -> Result<Vec<Value>, GqlError> {
        // ... existing implementation ...
    }

    /// Execute a GQL query with parameters.
    #[cfg(feature = "gql")]
    pub fn gql_with_params(
        &self,
        query: &str,
        params: gql::Parameters,
    ) -> Result<Vec<Value>, GqlError> {
        // ... existing implementation ...
    }
}
```

---

## 4. Test Changes

### 4.1 Main GQL Test File (tests/gql.rs)

Add feature gate to the entire test module:

```rust
// tests/gql.rs

#![cfg(feature = "gql")]

mod gql {
    mod basics;
    mod match_clause;
    // ... rest of test modules ...
}
```

### 4.2 Snapshot Tests

Snapshot tests in `tests/gql/` are automatically gated by the parent module.

---

## 5. Example Changes

### 5.1 quickstart_gql.rs

No code changes needed; just requires `gql` feature in Cargo.toml:

```toml
[[example]]
name = "quickstart_gql"
required-features = ["gql"]
```

### 5.2 nba.rs

Add `gql` to required features:

```toml
[[example]]
name = "nba"
required-features = ["mmap", "gql"]
```

---

## 6. Documentation Updates

### 6.1 README.md

Add feature documentation:

```markdown
## Features

Interstellar uses Cargo features to allow optional functionality:

| Feature | Description | Default |
|---------|-------------|---------|
| `graphson` | GraphSON import/export | Yes |
| `mmap` | Memory-mapped persistent storage | No |
| `gql` | GQL query language | No |
| `full-text` | Full-text search (Tantivy) | No |
| `full` | Enable all features | No |

Note: In-memory graph storage is always available as core functionality (not feature-gated).

### Enabling Features

```toml
# Just the defaults
interstellar = "0.1"

# With GQL support
interstellar = { version = "0.1", features = ["gql"] }

# Everything
interstellar = { version = "0.1", features = ["full"] }
```
```

### 6.2 lib.rs Module Documentation

Update the crate-level documentation to reflect feature-gated modules.

---

## 7. CI/CD Considerations

### 7.1 Test Matrix

Ensure CI tests both with and without GQL:

```yaml
# Example GitHub Actions matrix
strategy:
  matrix:
    features:
      - ""                    # defaults only
      - "gql"                 # with GQL
      - "mmap,gql"            # mmap + GQL
      - "full"                # everything
```

### 7.2 Feature Combinations

Test critical feature combinations:

| Combination | Purpose |
|-------------|---------|
| `default` | Verify library works without GQL |
| `gql` | Verify GQL compiles and works |
| `mmap,gql` | Verify persistent storage + GQL |
| `full` | Verify all features together |

---

## 8. Migration Guide

### 8.1 For Existing Users

Users currently using GQL need to add the feature:

**Before:**
```toml
[dependencies]
interstellar = "0.1"
```

**After:**
```toml
[dependencies]
interstellar = { version = "0.1", features = ["gql"] }
```

### 8.2 Compile-Time Errors

If GQL is used without the feature, users will see clear compile errors:

```
error[E0433]: failed to resolve: could not find `gql` in `interstellar`
  --> src/main.rs:5:18
   |
5  | use interstellar::gql;
   |                   ^^^ could not find `gql` in `interstellar`
   |
   = help: consider enabling the `gql` feature: `interstellar = { features = ["gql"] }`
```

```
error[E0599]: no method named `gql` found for struct `Graph`
  --> src/main.rs:10:11
   |
10 |     graph.gql("MATCH (n) RETURN n")?;
   |           ^^^ method not found in `Graph`
   |
   = help: consider enabling the `gql` feature
```

---

## 9. Files to Modify

| File | Change |
|------|--------|
| `Cargo.toml` | Add `gql` and `full` features, make deps optional |
| `src/lib.rs` | Add `#[cfg(feature = "gql")]` to `pub mod gql` |
| `src/storage/cow.rs` | Feature-gate GQL import and methods |
| `src/storage/cow_mmap.rs` | Feature-gate GQL import and methods |
| `tests/gql.rs` | Add `#![cfg(feature = "gql")]` at top |

---

## 10. Implementation Checklist

### Phase 1: Cargo.toml

- [ ] Remove unused `inmemory` feature
- [ ] Update `default` to `["graphson"]`
- [ ] Add `gql` feature definition
- [ ] Add `full` feature definition  
- [ ] Make `pest` optional
- [ ] Make `pest_derive` optional
- [ ] Make `mathexpr` optional
- [ ] Add `required-features` to GQL examples

### Phase 2: Source Code

- [ ] Feature-gate `pub mod gql` in `src/lib.rs`
- [ ] Feature-gate GQL imports in `src/storage/cow.rs`
- [ ] Feature-gate `gql()` method in `src/storage/cow.rs`
- [ ] Feature-gate `gql_with_params()` method in `src/storage/cow.rs`
- [ ] Feature-gate GQL imports in `src/storage/cow_mmap.rs`
- [ ] Feature-gate `gql()` method in `src/storage/cow_mmap.rs`
- [ ] Feature-gate `gql_with_params()` method in `src/storage/cow_mmap.rs`

### Phase 3: Tests

- [ ] Add `#![cfg(feature = "gql")]` to `tests/gql.rs`

### Phase 4: Verification

- [ ] `cargo build` succeeds (without GQL)
- [ ] `cargo build --features gql` succeeds
- [ ] `cargo build --features full` succeeds
- [ ] `cargo test` succeeds (without GQL)
- [ ] `cargo test --features gql` succeeds
- [ ] `cargo test --features full` succeeds
- [ ] `cargo clippy -- -D warnings` passes for all feature combinations

---

## 11. Testing Strategy

### 11.1 Verification Commands

```bash
# Verify default build (no GQL)
cargo build
cargo test

# Verify with GQL
cargo build --features gql
cargo test --features gql

# Verify full feature set
cargo build --features full
cargo test --features full

# Verify no unused dependencies warning
cargo build --features gql 2>&1 | grep -i "unused"

# Verify examples compile
cargo build --example quickstart_gql --features gql
cargo build --example nba --features mmap,gql
```

### 11.2 Expected Outcomes

| Command | Expected Result |
|---------|-----------------|
| `cargo build` | Success, no GQL code compiled |
| `cargo build --features gql` | Success, GQL included |
| `cargo test` | GQL tests skipped |
| `cargo test --features gql` | All GQL tests run |

---

## 12. Future Considerations

### 12.1 Additional Granularity

Future work could split GQL further if needed:

```toml
# Potential future granularity (not in this spec)
gql-parser = ["pest", "pest_derive"]
gql-compiler = ["gql-parser"]
gql-mutations = ["gql-compiler"]
gql = ["gql-mutations", "mathexpr"]
```

### 12.2 Feature Documentation

Consider using `document-features` crate for auto-generated feature docs:

```toml
[dependencies]
document-features = "0.2"
```

```rust
//! ## Features
#![doc = document_features::document_features!()]
```
