# Spec 22: Integration Test Refactoring

## Overview

The integration tests have grown to ~26,500 lines across 9 files with inconsistent organization patterns. This spec defines a refactoring plan to improve maintainability, reduce duplication, and establish consistent structure.

## Current State Analysis

### Test File Inventory

| File | Lines | Organization | Issues |
|------|------:|--------------|--------|
| `gql.rs` | 12,885 | Flat with comments | Too large, hard to navigate |
| `traversal.rs` | 6,030 | 16 internal modules | Good structure, but file too large |
| `mmap.rs` | 2,705 | Flat with comments | Moderate size, could use modules |
| `gql_mutations.rs` | 1,439 | Flat | Acceptable |
| `gql_snapshots.rs` | 1,104 | Flat | Acceptable |
| `branch.rs` | 785 | Flat | Duplicates traversal concepts |
| `sideeffect.rs` | 774 | Flat | Duplicates traversal concepts |
| `mutations.rs` | 652 | Flat | Acceptable |
| `inmemory.rs` | 84 | Flat | Good |
| **Total** | **26,458** | | |

### Identified Problems

1. **Duplicated Test Graph Setup**: `TestGraph` struct and `create_test_graph()` defined in 6 different files with slight variations
2. **Oversized Files**: `gql.rs` at 12,885 lines is unwieldy; `traversal.rs` at 6,030 lines is borderline
3. **Inconsistent Organization**: Mix of flat files, comment-delimited sections, and internal modules
4. **Related Tests Scattered**: Branch tests split between `branch.rs` and `traversal.rs::branch_step_tests`
5. **No Shared Test Utilities**: Each file re-implements common patterns

## Target Architecture

### Directory Structure

```
tests/
├── common/
│   ├── mod.rs              # Test utilities and shared setup
│   └── graphs.rs           # TestGraph variants
├── traversal/
│   ├── mod.rs              # Module re-exports
│   ├── basic.rs            # v(), e(), inject(), count()
│   ├── filter.rs           # has_label, has, dedup, where_, not, and_, or_
│   ├── navigation.rs       # out, in_, both, out_e, in_e, both_e
│   ├── transform.rs        # values, id, label, map, flat_map, path
│   ├── terminal.rs         # to_list, to_set, next, count, sum, fold
│   ├── branch.rs           # union, coalesce, choose, optional
│   ├── repeat.rs           # repeat with times, until, emit
│   ├── sideeffect.rs       # store, aggregate, group, inject
│   ├── predicates.rs       # p::eq, p::gt, p::between, etc.
│   ├── anonymous.rs        # __ factory tests
│   ├── metadata.rs         # key(), value(), loops(), index()
│   ├── complex.rs          # Multi-step traversals
│   └── errors.rs           # Error handling tests
├── gql/
│   ├── mod.rs              # Module re-exports
│   ├── match_clause.rs     # MATCH patterns, node/edge binding
│   ├── where_clause.rs     # WHERE predicates, boolean logic
│   ├── return_clause.rs    # RETURN expressions, aliases
│   ├── aggregation.rs      # COUNT, SUM, AVG, COLLECT, GROUP BY
│   ├── ordering.rs         # ORDER BY, LIMIT, OFFSET, SKIP
│   ├── patterns.rs         # Variable-length paths, complex patterns
│   ├── expressions.rs      # CASE, arithmetic, string functions
│   ├── mutations.rs        # CREATE, SET, DELETE (merge gql_mutations.rs)
│   └── snapshots.rs        # Parser snapshot tests (merge gql_snapshots.rs)
├── storage/
│   ├── mod.rs              # Module re-exports
│   ├── inmemory.rs         # In-memory storage tests
│   └── mmap.rs             # Memory-mapped storage tests
└── mutations.rs            # Traversal mutation tests (keep separate)
```

### Shared Test Utilities

**`tests/common/mod.rs`**:
```rust
//! Shared test utilities and fixtures

pub mod graphs;

pub use graphs::{TestGraph, TestGraphBuilder};
pub use graphs::{create_small_graph, create_medium_graph, create_social_graph};
```

**`tests/common/graphs.rs`**:
```rust
//! Reusable test graph fixtures

use interstellar::prelude::*;
use std::collections::HashMap;

/// Standard test graph with vertices and their IDs for assertions
pub struct TestGraph {
    pub graph: Graph,
    pub alice: VertexId,
    pub bob: VertexId,
    pub charlie: VertexId,
    pub graphdb: VertexId,
    // Optional vertices for extended graphs
    pub redis: Option<VertexId>,
    pub eve: Option<VertexId>,
}

impl TestGraph {
    /// Access the graph traversal source
    pub fn g(&self) -> GraphTraversalSource<'_> {
        self.graph.traversal()
    }
}

/// Builder for creating test graphs with specific configurations
pub struct TestGraphBuilder {
    vertices: Vec<(&'static str, &'static str, HashMap<String, Value>)>,
    edges: Vec<(usize, usize, &'static str, HashMap<String, Value>)>,
}

impl TestGraphBuilder {
    pub fn new() -> Self { /* ... */ }
    pub fn add_person(self, name: &'static str, age: i64) -> Self { /* ... */ }
    pub fn add_software(self, name: &'static str, lang: &'static str) -> Self { /* ... */ }
    pub fn add_edge(self, from: usize, to: usize, label: &'static str) -> Self { /* ... */ }
    pub fn build(self) -> TestGraph { /* ... */ }
}

/// Small graph: 4 vertices (alice, bob, charlie, graphdb)
/// Standard for basic traversal tests
pub fn create_small_graph() -> TestGraph {
    let graph = Graph::new();
    
    let alice = graph.add_vertex("person", [
        ("name", "alice"),
        ("age", 30),
    ]);
    let bob = graph.add_vertex("person", [
        ("name", "bob"),
        ("age", 25),
    ]);
    let charlie = graph.add_vertex("person", [
        ("name", "charlie"),
        ("age", 35),
    ]);
    let graphdb = graph.add_vertex("software", [
        ("name", "graphdb"),
        ("lang", "rust"),
    ]);
    
    graph.add_edge(alice, bob, "knows", [("weight", 0.5)]);
    graph.add_edge(alice, charlie, "knows", [("weight", 1.0)]);
    graph.add_edge(bob, charlie, "knows", []);
    graph.add_edge(alice, graphdb, "created", []);
    graph.add_edge(bob, graphdb, "uses", []);
    
    TestGraph {
        graph,
        alice,
        bob,
        charlie,
        graphdb,
        redis: None,
        eve: None,
    }
}

/// Medium graph: 5 vertices (adds redis software)
/// Used for more complex traversal tests
pub fn create_medium_graph() -> TestGraph {
    let mut tg = create_small_graph();
    
    let redis = tg.graph.add_vertex("software", [
        ("name", "redis"),
        ("lang", "c"),
    ]);
    tg.graph.add_edge(tg.charlie, redis, "created", []);
    tg.redis = Some(redis);
    
    tg
}

/// Social network graph with more people and relationships
/// Used for complex path and aggregation tests
pub fn create_social_graph() -> TestGraph {
    // Extended graph with more vertices and edge types
    todo!()
}
```

## Implementation Phases

### Phase 1: Create Common Test Utilities

**Goal**: Extract shared test setup to eliminate duplication.

**Tasks**:
1. Create `tests/common/mod.rs` with module structure
2. Create `tests/common/graphs.rs` with:
   - `TestGraph` struct (unified version)
   - `TestGraphBuilder` for custom test graphs
   - `create_small_graph()` - 4 vertex standard graph
   - `create_medium_graph()` - 5 vertex extended graph
   - `create_social_graph()` - larger graph for complex tests
3. Add any shared assertion helpers or test utilities

**Files Created**:
- `tests/common/mod.rs`
- `tests/common/graphs.rs`

### Phase 2: Refactor `traversal.rs` into Directory

**Goal**: Split 6,030-line file into logical modules while preserving existing internal module structure.

**Tasks**:
1. Create `tests/traversal/` directory
2. Create `tests/traversal/mod.rs` with shared imports
3. Extract each internal module to its own file:

| From Module | To File | Approximate Lines |
|-------------|---------|------------------:|
| `basic_tests` + `basic_source_tests` | `basic.rs` | ~320 |
| `filter_tests` + `filter_step_tests` + `new_filter_steps_integration` | `filter.rs` | ~750 |
| `navigation_tests` | `navigation.rs` | ~180 |
| `transform_tests` | `transform.rs` | ~200 |
| `terminal_tests` | `terminal.rs` | ~220 |
| `branch_step_tests` | `branch.rs` | ~280 |
| `repeat_step_tests` | `repeat.rs` | ~240 |
| `predicate_integration_tests` | `predicates.rs` | ~720 |
| `anonymous_traversal_tests` | `anonymous.rs` | ~150 |
| `phase_7_integration_tests` | `phase7.rs` | ~770 |
| `metadata_steps_integration` | `metadata.rs` | ~490 |
| `complex_traversal_tests` | `complex.rs` | ~220 |
| `error_case_tests` | `errors.rs` | ~200 |

4. Merge `tests/branch.rs` (785 lines) into `tests/traversal/branch.rs`
5. Merge `tests/sideeffect.rs` (774 lines) into `tests/traversal/sideeffect.rs`
6. Update all imports to use `common::graphs`
7. Delete original `tests/traversal.rs`, `tests/branch.rs`, `tests/sideeffect.rs`

**Files Created**:
- `tests/traversal/mod.rs`
- `tests/traversal/basic.rs`
- `tests/traversal/filter.rs`
- `tests/traversal/navigation.rs`
- `tests/traversal/transform.rs`
- `tests/traversal/terminal.rs`
- `tests/traversal/branch.rs`
- `tests/traversal/repeat.rs`
- `tests/traversal/sideeffect.rs`
- `tests/traversal/predicates.rs`
- `tests/traversal/anonymous.rs`
- `tests/traversal/phase7.rs`
- `tests/traversal/metadata.rs`
- `tests/traversal/complex.rs`
- `tests/traversal/errors.rs`

**Files Deleted**:
- `tests/traversal.rs`
- `tests/branch.rs`
- `tests/sideeffect.rs`

### Phase 3: Refactor `gql.rs` into Directory

**Goal**: Split 12,885-line file into logical modules by GQL clause/feature.

**Tasks**:
1. Create `tests/gql/` directory
2. Analyze test content and group by feature:

| Feature Area | Target File | Test Categories |
|--------------|-------------|-----------------|
| MATCH clause | `match_clause.rs` | Node patterns, edge patterns, variable binding |
| WHERE clause | `where_clause.rs` | Predicates, boolean operators, property access |
| RETURN clause | `return_clause.rs` | Expressions, aliases, projections |
| Aggregation | `aggregation.rs` | COUNT, SUM, AVG, MIN, MAX, COLLECT, GROUP BY |
| Ordering | `ordering.rs` | ORDER BY, LIMIT, OFFSET, SKIP |
| Patterns | `patterns.rs` | Variable-length paths, complex patterns |
| Expressions | `expressions.rs` | CASE, math, strings, type functions |
| DDL | `ddl.rs` | Schema operations |

3. Create `tests/gql/mod.rs` with shared imports
4. Merge `tests/gql_mutations.rs` into `tests/gql/mutations.rs`
5. Merge `tests/gql_snapshots.rs` into `tests/gql/snapshots.rs`
6. Update all imports to use `common::graphs`
7. Delete original files

**Files Created**:
- `tests/gql/mod.rs`
- `tests/gql/match_clause.rs`
- `tests/gql/where_clause.rs`
- `tests/gql/return_clause.rs`
- `tests/gql/aggregation.rs`
- `tests/gql/ordering.rs`
- `tests/gql/patterns.rs`
- `tests/gql/expressions.rs`
- `tests/gql/ddl.rs`
- `tests/gql/mutations.rs`
- `tests/gql/snapshots.rs`

**Files Deleted**:
- `tests/gql.rs`
- `tests/gql_mutations.rs`
- `tests/gql_snapshots.rs`

### Phase 4: Refactor Storage Tests

**Goal**: Organize storage tests consistently.

**Tasks**:
1. Create `tests/storage/` directory
2. Move `tests/inmemory.rs` to `tests/storage/inmemory.rs`
3. Move `tests/mmap.rs` to `tests/storage/mmap.rs`
4. Create `tests/storage/mod.rs` with shared setup
5. Add internal modules to `mmap.rs` for better organization
6. Delete original files

**Files Created**:
- `tests/storage/mod.rs`
- `tests/storage/inmemory.rs`
- `tests/storage/mmap.rs`

**Files Deleted**:
- `tests/inmemory.rs`
- `tests/mmap.rs`

### Phase 5: Final Cleanup

**Goal**: Ensure consistency and verify all tests pass.

**Tasks**:
1. Verify `cargo test` passes with all tests
2. Run `cargo fmt` on all new test files
3. Run `cargo clippy` and fix any warnings
4. Update any documentation referencing old test locations
5. Verify test count matches before refactor

## File Organization Guidelines

### Module Structure Pattern

For test directories, use this consistent pattern:

```rust
// tests/traversal/mod.rs
mod basic;
mod filter;
mod navigation;
// ... etc

// Re-export if needed for cross-module test utilities
pub use basic::some_helper;
```

### Test Function Naming

Use descriptive names that indicate:
1. What is being tested
2. The expected behavior
3. Edge case (if applicable)

```rust
// Good
#[test]
fn out_step_returns_adjacent_vertices() { }

#[test]
fn has_label_filters_vertices_by_single_label() { }

#[test]
fn repeat_until_terminates_on_condition() { }

// Avoid
#[test]
fn test_out() { }

#[test]
fn test1() { }
```

### Import Pattern

Each test file should use consistent imports:

```rust
// tests/traversal/filter.rs
use crate::common::graphs::{create_small_graph, TestGraph};
use interstellar::prelude::*;
```

### Test Documentation

Add module-level documentation explaining what the tests cover:

```rust
//! Filter step integration tests
//!
//! Tests for filter steps: has_label, has, has_not, dedup, where_, not, and_, or_
//!
//! These tests verify correct filtering behavior across different
//! graph structures and predicate combinations.
```

## Success Criteria

1. **All tests pass**: `cargo test` succeeds with identical test count
2. **No file exceeds 1,500 lines**: Manageable file sizes
3. **No duplicated test graph setup**: Single source of truth in `common/graphs.rs`
4. **Consistent organization**: All test directories use module pattern
5. **Clear navigation**: Tests easy to find by feature area

## Estimated Effort

| Phase | Effort | Risk |
|-------|--------|------|
| Phase 1: Common utilities | 2-3 hours | Low |
| Phase 2: Traversal refactor | 4-6 hours | Medium |
| Phase 3: GQL refactor | 6-8 hours | Medium |
| Phase 4: Storage refactor | 1-2 hours | Low |
| Phase 5: Cleanup | 1-2 hours | Low |
| **Total** | **14-21 hours** | |

## Rollback Plan

If issues arise during refactoring:
1. Each phase is independent - can stop after any phase
2. Git commits after each phase allow easy rollback
3. Keep original files until new structure verified
4. Run full test suite after each phase

## Future Considerations

1. **Test data fixtures**: Consider JSON/YAML fixtures for complex test graphs
2. **Property-based tests**: Add proptest for edge cases
3. **Benchmark tests**: Keep benchmarks separate from unit tests
4. **Test categories**: Use `#[ignore]` or custom attributes for slow tests
