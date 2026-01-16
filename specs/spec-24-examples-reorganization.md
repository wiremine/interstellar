# Spec 24: Examples Directory Reorganization

## Overview

This specification defines a plan to reorganize the `examples/` directory by:
1. Removing redundant examples
2. Moving test-like examples to the integration test suite
3. Consolidating domain examples (NBA, British Royals)
4. Creating a single comprehensive GQL example

## Goals

- **Reduce duplication**: Many examples duplicate functionality already covered in tests
- **Clarify purpose**: Examples should demonstrate features to users, not test them
- **Consolidate GQL**: Combine multiple GQL examples into one comprehensive file
- **Maintain discoverability**: Keep real-world domain examples that help users understand the API

## Current State Analysis

### Existing Examples (23 files)

| File | Lines | Purpose | Category |
|------|-------|---------|----------|
| `basic_traversal.rs` | ~200 | Step-by-step traversal demo | Test-like |
| `navigation_steps.rs` | ~250 | Navigation step demo | Test-like |
| `filter_steps.rs` | ~300 | Filter step demo | Test-like |
| `terminal_steps.rs` | ~200 | Terminal step demo | Test-like |
| `anonymous_predicates.rs` | ~250 | Anonymous traversal demo | Test-like |
| `branch_steps.rs` | ~300 | Branch step demo | Test-like |
| `branch_combinations.rs` | ~350 | Complex branch combinations | Test-like |
| `repeat_steps.rs` | ~200 | Repeat/loop demo | Test-like |
| `side_effect_steps.rs` | ~300 | Side effect demo | Test-like |
| `path_tracking.rs` | ~200 | Path tracking demo | Test-like |
| `math_expressions.rs` | ~250 | Math operations demo | Test-like |
| `mutations.rs` | ~300 | Mutation demo | Test-like |
| `query_enhancements.rs` | ~250 | Query enhancements demo | Test-like |
| `call_subquery.rs` | ~200 | Subquery demo | Test-like |
| `nba.rs` | ~400 | NBA in-memory demo | Domain |
| `nba_gql.rs` | ~350 | NBA with GQL | Domain/GQL |
| `nba_mmap_write.rs` | ~380 | NBA mmap write | Domain/Persistence |
| `nba_mmap_read.rs` | ~1244 | NBA mmap read | Domain/Persistence |
| `british_royals.rs` | ~300 | British Royals in-memory | Domain |
| `british_royals_gql.rs` | ~250 | British Royals GQL | Domain/GQL |
| `marvel.rs` | ~200 | Marvel characters | Domain |
| `gql_schema.rs` | ~300 | GQL DDL/schema demo | GQL |
| `gql_mutations.rs` | ~350 | GQL mutations demo | GQL |
| `advanced_gql.rs` | ~400 | Advanced GQL features | GQL |
| `schema_mmap_write.rs` | ~250 | Schema + mmap write | Persistence |
| `schema_mmap_validate.rs` | ~200 | Schema validation | Persistence |
| `bench_writes.rs` | ~150 | Write benchmarking | Benchmark |

### Existing Test Coverage

The `tests/` directory already has comprehensive coverage:

```
tests/
├── traversal/
│   ├── basic.rs          # Basic traversal tests
│   ├── navigation.rs     # Navigation step tests  
│   ├── filter.rs         # Filter step tests
│   ├── terminal.rs       # Terminal step tests
│   ├── branch.rs         # Branch step tests
│   ├── repeat.rs         # Repeat step tests
│   ├── anonymous.rs      # Anonymous traversal tests
│   ├── transform.rs      # Transform step tests
│   ├── predicates.rs     # Predicate tests
│   ├── metadata.rs       # Metadata step tests
│   ├── complex.rs        # Complex traversal tests
│   └── errors.rs         # Error handling tests
├── gql/
│   ├── basics.rs         # GQL basic tests
│   ├── mutations.rs      # GQL mutation tests
│   ├── patterns.rs       # Pattern matching tests
│   ├── expressions.rs    # Expression tests
│   ├── aggregation.rs    # Aggregation tests
│   └── ... (11 more files)
├── mutations.rs          # Mutation tests
├── sideeffect.rs         # Side effect tests
├── branch.rs             # Branch tests
└── storage/              # Storage tests
```

**Key Finding**: Most "examples" are duplicating test coverage that already exists.

---

## Recommendations

### Category 1: REMOVE (Move to benchmarks)

| File | Action | Rationale |
|------|--------|-----------|
| `bench_writes.rs` | Move to `benches/` | Benchmarks belong in benchmark directory |

### Category 2: REMOVE (Already covered by tests)

These examples are essentially test suites with assertions. The functionality is already tested in the `tests/` directory.

| Example File | Existing Test Coverage |
|--------------|------------------------|
| `basic_traversal.rs` | `tests/traversal/basic.rs` |
| `navigation_steps.rs` | `tests/traversal/navigation.rs` |
| `filter_steps.rs` | `tests/traversal/filter.rs` |
| `terminal_steps.rs` | `tests/traversal/terminal.rs` |
| `anonymous_predicates.rs` | `tests/traversal/anonymous.rs`, `tests/traversal/predicates.rs` |
| `branch_steps.rs` | `tests/traversal/branch.rs`, `tests/branch.rs` |
| `branch_combinations.rs` | `tests/traversal/branch.rs` |
| `repeat_steps.rs` | `tests/traversal/repeat.rs` |
| `side_effect_steps.rs` | `tests/sideeffect.rs` |
| `path_tracking.rs` | `tests/traversal/basic.rs` (path tests exist) |
| `math_expressions.rs` | `tests/traversal/transform.rs` (math tests) |
| `mutations.rs` | `tests/mutations.rs` |
| `query_enhancements.rs` | `tests/traversal/complex.rs` |
| `call_subquery.rs` | Covered in GQL tests |

**Action**: Delete these 14 files. Review each to ensure no unique test cases are lost.

### Category 3: CONSOLIDATE (Domain Examples)

#### NBA Examples → `nba.rs`

Combine 4 files into 1:
- `nba.rs` (in-memory)
- `nba_gql.rs` (GQL queries)  
- `nba_mmap_write.rs` (persistence write)
- `nba_mmap_read.rs` (persistence read - 1244 lines!)

**New `nba.rs` structure** (~400 lines):
```rust
//! NBA Graph Example
//! 
//! Demonstrates Intersteller features using NBA players, teams, and games.
//! 
//! Sections:
//! 1. Building the graph (in-memory)
//! 2. Fluent API traversals
//! 3. GQL queries
//! 4. Persistence with MmapGraph

fn main() {
    // Part 1: Build the graph
    // Part 2: Fluent API examples (5-10 representative queries)
    // Part 3: GQL examples (5-10 representative queries)
    // Part 4: Persistence demo (write + read back)
}
```

#### British Royals Examples → `british_royals.rs`

Combine 2 files into 1:
- `british_royals.rs` (in-memory)
- `british_royals_gql.rs` (GQL queries)

**New `british_royals.rs` structure** (~300 lines):
```rust
//! British Royal Family Graph Example
//! 
//! Demonstrates family relationship traversals.
//! 
//! Sections:
//! 1. Building the family tree
//! 2. Fluent API traversals
//! 3. GQL queries

fn main() {
    // Part 1: Build the family tree
    // Part 2: Fluent API examples
    // Part 3: GQL examples
}
```

#### Marvel Example → Keep as-is

`marvel.rs` is a good standalone example. Keep it.

### Category 4: CONSOLIDATE (GQL Examples)

Combine 3 files into 1:
- `advanced_gql.rs`
- `gql_mutations.rs`
- `gql_schema.rs`

**New `gql.rs` structure** (~500 lines):
```rust
//! GQL (Graph Query Language) Example
//! 
//! Comprehensive demonstration of Intersteller's GQL support.
//! 
//! Sections:
//! 1. Schema/DDL - CREATE GRAPH TYPE, etc.
//! 2. Basic queries - MATCH, RETURN, WHERE
//! 3. Pattern matching - paths, variable-length
//! 4. Aggregations - COUNT, SUM, AVG, COLLECT
//! 5. Mutations - CREATE, SET, DELETE, MERGE
//! 6. Advanced features - subqueries, CASE, list comprehensions

fn main() {
    // Part 1: Schema
    // Part 2: Basic queries
    // Part 3: Pattern matching
    // Part 4: Aggregations
    // Part 5: Mutations
    // Part 6: Advanced features
}
```

### Category 5: CONSOLIDATE (Persistence Examples)

Combine 2 files into 1:
- `schema_mmap_write.rs`
- `schema_mmap_validate.rs`

**New `persistence.rs` structure** (~300 lines):
```rust
//! Persistence Example
//! 
//! Demonstrates MmapGraph for persistent storage.
//! 
//! Sections:
//! 1. Creating schema
//! 2. Writing to MmapGraph
//! 3. Reading from MmapGraph
//! 4. Schema validation

fn main() {
    // Part 1: Define schema
    // Part 2: Write graph
    // Part 3: Read and query
    // Part 4: Validate schema
}
```

---

## Proposed New Structure

```
examples/
├── data/
│   └── fixtures/
│       ├── british_royals.json
│       ├── marvel.json
│       └── nba.json
├── nba.rs                 # NBA demo (fluent + GQL + persistence)
├── british_royals.rs      # British Royals demo (fluent + GQL)
├── marvel.rs              # Marvel demo (keep as-is)
├── gql.rs                 # Comprehensive GQL demo
└── persistence.rs         # MmapGraph persistence demo
```

**Total: 5 example files** (down from 23)

---

## Implementation Plan

### Phase 1: Audit & Gap Analysis

Before deleting any examples, verify no unique test cases are lost:

1. For each file in "REMOVE" category:
   - List all test cases / assertions
   - Map to existing test coverage in `tests/`
   - Identify any gaps
   - Add missing tests to `tests/` if needed

### Phase 2: Create Consolidated Examples

1. **Create `examples/nba.rs`**
   - Extract graph building from `nba.rs`
   - Select 5-10 best fluent API queries
   - Select 5-10 best GQL queries from `nba_gql.rs`
   - Add simplified persistence section from mmap files

2. **Create `examples/british_royals.rs`**
   - Merge fluent and GQL versions
   - Keep family tree building
   - Select best traversal examples

3. **Create `examples/gql.rs`**
   - Combine all GQL examples
   - Organize by feature area
   - Include schema, queries, mutations, advanced

4. **Create `examples/persistence.rs`**
   - Combine schema_mmap files
   - Show complete write/read/validate flow

### Phase 3: Cleanup

1. Delete removed example files
2. Move `bench_writes.rs` to `benches/`
3. Update any documentation referencing old examples
4. Update `Cargo.toml` `[[example]]` entries

### Phase 4: Verification

1. Run all examples: `cargo run --example <name>`
2. Verify documentation is accurate
3. Run full test suite to ensure no regressions

---

## Migration Checklist

### Files to Delete (14)

- [ ] `examples/basic_traversal.rs`
- [ ] `examples/navigation_steps.rs`
- [ ] `examples/filter_steps.rs`
- [ ] `examples/terminal_steps.rs`
- [ ] `examples/anonymous_predicates.rs`
- [ ] `examples/branch_steps.rs`
- [ ] `examples/branch_combinations.rs`
- [ ] `examples/repeat_steps.rs`
- [ ] `examples/side_effect_steps.rs`
- [ ] `examples/path_tracking.rs`
- [ ] `examples/math_expressions.rs`
- [ ] `examples/mutations.rs`
- [ ] `examples/query_enhancements.rs`
- [ ] `examples/call_subquery.rs`

### Files to Move (1)

- [ ] `examples/bench_writes.rs` → `benches/writes.rs`

### Files to Consolidate (8 → 3)

- [ ] `nba.rs` + `nba_gql.rs` + `nba_mmap_write.rs` + `nba_mmap_read.rs` → `nba.rs`
- [ ] `british_royals.rs` + `british_royals_gql.rs` → `british_royals.rs`
- [ ] `advanced_gql.rs` + `gql_mutations.rs` + `gql_schema.rs` → `gql.rs`
- [ ] `schema_mmap_write.rs` + `schema_mmap_validate.rs` → `persistence.rs`

### Files to Keep (1)

- [ ] `examples/marvel.rs` (no changes)

---

## Success Criteria

1. Example count reduced from 23 to 5
2. All examples run successfully: `cargo run --example <name>`
3. No test coverage lost (verified in Phase 1)
4. Each example is self-contained and demonstrates real-world usage
5. Documentation updated to reflect new structure

---

## Open Questions

1. **NBA mmap example length**: The current `nba_mmap_read.rs` is 1244 lines. How many queries should the consolidated version include? Recommendation: 10-15 representative queries covering different features.

2. **Fixture data**: Should we keep the JSON fixtures in `examples/data/fixtures/` or inline the data in the examples? Recommendation: Keep fixtures for larger datasets (NBA), inline for smaller ones.

3. **Example documentation**: Should each example have accompanying markdown documentation, or is inline documentation sufficient? Recommendation: Inline documentation with clear section headers.
