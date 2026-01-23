# Spec 35: Examples Refactoring

This specification defines a complete refactoring of the `examples/` directory to provide clear, focused, and well-organized examples for new users.

---

## 1. Overview

### 1.1 Current State

The `examples/` directory currently contains 10 files with overlapping purposes:

| File | Lines | Description |
|------|-------|-------------|
| `quickstart.rs` | 366 | Combined Gremlin + GQL demo |
| `gql.rs` | 716 | Comprehensive GQL demo (queries, mutations, schema) |
| `marvel.rs` | 1420 | Marvel dataset with Gremlin features |
| `nba.rs` | 550 | NBA dataset with fluent API + GQL + mmap |
| `rhai_scripting.rs` | 401 | Rhai scripting integration |
| `persistence.rs` | 511 | Mmap storage with schema validation |
| `british_royals.rs` | 385 | Family tree traversals |
| `indexes.rs` | 508 | Property index features |
| `cow_unified_api.rs` | 240 | In-memory unified API demo |
| `cow_mmap_unified_api.rs` | 318 | Persistent unified API demo |

### 1.2 Problems

1. **Quickstart is too long** - 366 lines is overwhelming for a "quickstart"
2. **Gremlin and GQL are combined** - Users wanting to learn one query language must wade through both
3. **Dataset examples overlap in purpose** - NBA and Marvel both demo similar features
4. **Storage examples are scattered** - `persistence.rs`, `cow_mmap_unified_api.rs`, and `indexes.rs` all cover storage
5. **Missing dedicated scripting focus** - The Rhai example exists but isn't discoverable
6. **Dataset examples load external JSON** - These should be self-contained for easy copy-paste

### 1.3 Goals

1. **Clear separation of concerns** - One example per focused topic
2. **Gremlin-first and GQL-first examples** - Let users choose their preferred query language
3. **Self-contained dataset examples** - Build graphs inline, no external fixtures required
4. **Short quickstarts** - Under 150 lines for immediate value
5. **Feature-rich dataset examples** - Demonstrate advanced features with real-world data

---

## 2. New Examples Structure

### 2.1 Proposed Files

```
examples/
├── quickstart_gremlin.rs    # ~120 lines - Gremlin basics (mutations + traversals)
├── quickstart_gql.rs        # ~120 lines - GQL basics (mutations + traversals)
├── scripting.rs             # ~150 lines - Rhai scripting demo
├── storage.rs               # ~80 lines  - Mmap persistent storage (short)
├── marvel.rs                # ~500 lines - Marvel dataset, Gremlin-focused
└── nba.rs                   # ~500 lines - NBA dataset, GQL-focused

fixtures/ (keep existing)
├── marvel.json              # Retained for marvel.rs
└── nba.json                 # Retained for nba.rs
```

**Total: 6 example files** (down from 10)

### 2.2 Files to Remove

| File | Reason |
|------|--------|
| `quickstart.rs` | Replaced by `quickstart_gremlin.rs` and `quickstart_gql.rs` |
| `gql.rs` | Folded into `quickstart_gql.rs` and `nba.rs` |
| `british_royals.rs` | Redundant with Marvel/NBA examples |
| `indexes.rs` | Move index demo into `storage.rs` or dedicated test |
| `cow_unified_api.rs` | Covered by quickstarts |
| `cow_mmap_unified_api.rs` | Covered by `storage.rs` |
| `persistence.rs` | Replaced by simpler `storage.rs` |
| `rhai_scripting.rs` | Renamed to `scripting.rs` |

### 2.3 Fixtures to Remove

| File | Reason |
|------|--------|
| `british_royals.json` | Example removed |

---

## 3. Example Specifications

### 3.1 `quickstart_gremlin.rs` (~120 lines)

**Purpose**: Minimal introduction to Gremlin-style API for new users.

**Target audience**: Developers familiar with Gremlin/TinkerPop who want to see how Interstellar works.

**Sections**:

```rust
//! # Interstellar Gremlin Quickstart
//!
//! A minimal introduction to Interstellar's Gremlin-style traversal API.
//!
//! Run: `cargo run --example quickstart_gremlin`

// 1. Create an in-memory graph
// 2. Mutations: add_v(), add_e(), property()
// 3. Basic traversals: v(), has_label(), has_value(), out(), in_()
// 4. Terminal steps: to_list(), count(), next()
// 5. Property access: values()
```

**Key demonstrations**:
- `Graph::new()` - Create in-memory graph
- `g.add_v("Person").property("name", "Alice")` - Add vertices
- `g.add_e("KNOWS").from_id(a).to_id(b)` - Add edges
- `g.v().has_label("Person").count()` - Count vertices
- `g.v().has_value("name", "Alice").out_label("KNOWS")` - Navigation
- `g.v().values("name").to_list()` - Get properties
- `g.v().drop().iterate()` - Delete vertices

**What NOT to include**:
- GQL queries
- Anonymous traversals
- Repeat/branch steps
- Persistence
- Schema

---

### 3.2 `quickstart_gql.rs` (~120 lines)

**Purpose**: Minimal introduction to GQL (Graph Query Language) for new users.

**Target audience**: Developers familiar with SQL/Cypher who prefer declarative queries.

**Sections**:

```rust
//! # Interstellar GQL Quickstart
//!
//! A minimal introduction to Interstellar's GQL (Graph Query Language).
//!
//! Run: `cargo run --example quickstart_gql`

// 1. Create an in-memory graph
// 2. Mutations: CREATE vertices and edges
// 3. Basic queries: MATCH, RETURN, WHERE
// 4. Pattern matching: relationships, multi-hop
// 5. Aggregations: count(*), ORDER BY, LIMIT
// 6. Updates: SET, DELETE
```

**Key demonstrations**:
- `graph.gql("CREATE (:Person {name: 'Alice', age: 30})")` - Create vertices
- `graph.gql("CREATE (a)-[:KNOWS]->(b)")` - Create edges with pattern
- `graph.gql("MATCH (p:Person) RETURN p.name")` - Basic query
- `graph.gql("MATCH (a)-[:KNOWS]->(b) WHERE a.age > 25 RETURN b.name")` - Pattern + filter
- `graph.gql("MATCH (p:Person) RETURN count(*)")` - Aggregation
- `graph.gql("MATCH (p:Person) SET p.active = true")` - Update
- `graph.gql("MATCH (p:Person) DETACH DELETE p")` - Delete

**What NOT to include**:
- Gremlin traversals (except initial setup if needed)
- Schema/DDL
- Advanced features (LET, CASE, list comprehensions)
- Persistence

---

### 3.3 `scripting.rs` (~150 lines)

**Purpose**: Demonstrate Rhai scripting integration for dynamic graph queries.

**Renamed from**: `rhai_scripting.rs`

**Sections**:

```rust
//! # Interstellar Scripting with Rhai
//!
//! Demonstrates embedded scripting for dynamic graph queries.
//!
//! Run: `cargo run --example scripting --features rhai`

// 1. Create a graph and RhaiEngine
// 2. Basic traversal via script
// 3. Predicates in scripts (gt, between, within)
// 4. Navigation patterns (out, in_)
// 5. Anonymous traversals (A.out(), A.values())
// 6. Pre-compiled scripts for performance
// 7. Complex query returning structured data
```

**Key demonstrations**:
- `RhaiEngine::new()` - Create engine
- `engine.eval_with_graph(graph, script)` - Execute script
- Script syntax: `let g = graph.gremlin(); g.v().count()`
- Predicates: `has_where("age", gt(30))`
- Anonymous traversals: `A.out("knows")`
- Pre-compilation: `engine.compile(script)`, `engine.eval_ast_with_graph()`

**Required feature**: `rhai`

---

### 3.4 `storage.rs` (~80 lines)

**Purpose**: Short, focused demo of memory-mapped persistent storage.

**Sections**:

```rust
//! # Interstellar Persistent Storage
//!
//! Demonstrates memory-mapped storage for data persistence.
//!
//! Run: `cargo run --example storage --features mmap`

// 1. Open/create a persistent database
// 2. Add vertices and edges
// 3. Checkpoint for durability
// 4. Close and reopen
// 5. Verify data persisted
// 6. Cleanup
```

**Key demonstrations**:
- `MmapGraph::open("path.db")` - Open/create database
- Standard mutations work the same
- `graph.checkpoint()` - Force durability
- Data survives process restart
- Cleanup temporary files

**What NOT to include**:
- Schema (keep it simple)
- Indexes (separate concern)
- Batch operations (advanced)

**Required feature**: `mmap`

---

### 3.5 `marvel.rs` (~500 lines)

**Purpose**: Comprehensive Gremlin feature demonstration using Marvel Universe data.

**Data source**: `examples/fixtures/marvel.json` (existing fixture)

**Target**: Users who want to see advanced Gremlin features in action.

**Graph structure** (built from scratch in example):
- **Vertices**: Characters (heroes, villains, antiheroes), Teams, Locations
- **Edges**: `member_of`, `rivals_with`, `allies_with`, `mentors`, `related_to`, `works_for`, `located_in`

**Sections**:

```rust
//! # Marvel Universe Graph Example
//!
//! Comprehensive demonstration of Gremlin-style traversal features.
//!
//! Run: `cargo run --example marvel`

// Part 1: Load Data (from JSON fixture)
//   - Load characters, teams, locations
//   - Create relationship edges

// Part 2: Basic Queries
//   - Find heroes/villains/antiheroes
//   - Count by type
//   - List team members

// Part 3: Navigation
//   - out_labels(), in_labels()
//   - Multi-hop traversals
//   - Mentorship chains

// Part 4: Predicates (p:: module)
//   - p::lt(), p::gt(), p::between()
//   - p::eq(), p::neq()
//   - Filter by era (first_appearance)

// Part 5: Anonymous Traversals (__:: module)
//   - __::out_labels(), __::in_labels()
//   - where_(), not(), and_(), or_()
//   - Complex conditions

// Part 6: Branch Steps
//   - union() - combine results
//   - coalesce() - fallback paths
//   - choose() - conditional branching
//   - optional() - try path, keep original

// Part 7: Repeat Steps
//   - repeat().times(n)
//   - emit(), emit_first()
//   - Chain exploration

// Part 8: Path Tracking
//   - as_(), select()
//   - with_path(), path()
//   - Full traversal paths

// Part 9: Summary Statistics
//   - Edge counts by type
//   - Team counts by type
//   - Era distribution
```

**Gremlin features to demonstrate**:
- `v()`, `e()`, `has_label()`, `has_value()`, `has_where()`
- `out_labels()`, `in_labels()`, `out_e()`, `in_e()`
- `values()`, `to_list()`, `count()`, `dedup()`, `limit()`
- Predicates: `p::lt()`, `p::gt()`, `p::gte()`, `p::between()`, `p::neq()`
- Anonymous: `__::out_labels()`, `__::in_labels()`, `__::has_value()`, `__::has_label()`
- Branch: `union()`, `coalesce()`, `choose()`, `optional()`
- Repeat: `repeat()`, `times()`, `emit()`, `emit_first()`
- Path: `as_()`, `select()`, `with_path()`, `path()`
- Boolean: `where_()`, `not()`, `and_()`, `or_()`

---

### 3.6 `nba.rs` (~500 lines)

**Purpose**: Comprehensive GQL feature demonstration using NBA data.

**Data source**: `examples/fixtures/nba.json` (existing fixture)

**Target**: Users who want to see advanced GQL features in action.

**Graph structure** (built from scratch in example):
- **Vertices**: Players, Teams
- **Edges**: `played_for`, `won_championship_with`

**Sections**:

```rust
//! # NBA Graph Example
//!
//! Comprehensive demonstration of GQL (Graph Query Language) features.
//!
//! Run: `cargo run --example nba --features mmap`

// Part 1: Load Data (from JSON fixture)
//   - Load teams with championships
//   - Load players with career stats
//   - Create relationship edges

// Part 2: Basic GQL Queries
//   - MATCH (p:player) RETURN p.name
//   - WHERE clause filtering
//   - ORDER BY, LIMIT

// Part 3: Pattern Matching
//   - Single-hop: (p)-[:played_for]->(t)
//   - Multi-hop patterns
//   - Variable-length paths: [:played_for*1..3]

// Part 4: Aggregations
//   - count(*), sum(), avg()
//   - GROUP BY
//   - HAVING equivalent (filter after group)

// Part 5: Advanced Queries
//   - EXISTS subquery
//   - CASE expressions
//   - Introspection: id(), labels()

// Part 6: GQL Mutations
//   - CREATE vertices and edges
//   - SET property updates
//   - MERGE (upsert)
//   - DELETE, DETACH DELETE

// Part 7: Query Parameters
//   - $paramName syntax
//   - gql_with_params()

// Part 8: Advanced Features
//   - Inline WHERE: (p:Player WHERE p.age > 25)
//   - LET clause
//   - List comprehensions: [x IN list | x.prop]
//   - Map literals: {key: value}
//   - String concatenation: ||

// Part 9: Schema and DDL (brief)
//   - CREATE NODE TYPE
//   - CREATE EDGE TYPE
//   - Validation modes
```

**GQL features to demonstrate**:
- Basic: `MATCH`, `RETURN`, `WHERE`, `ORDER BY`, `LIMIT`, `DISTINCT`
- Patterns: single-hop, multi-hop, variable-length paths
- Aggregations: `count()`, `sum()`, `avg()`, `min()`, `max()`, `collect()`
- Grouping: `GROUP BY`
- Subqueries: `EXISTS { }`
- CASE expressions: `CASE WHEN ... THEN ... ELSE ... END`
- Introspection: `id()`, `labels()`
- Mutations: `CREATE`, `SET`, `REMOVE`, `DELETE`, `DETACH DELETE`, `MERGE`
- Parameters: `$paramName`, `gql_with_params()`
- Advanced: inline WHERE, LET clause, list comprehensions, map literals, string concat

---

## 4. Cargo.toml Updates

Update the example entries in `Cargo.toml`:

```toml
# Remove old entries and replace with:

[[example]]
name = "quickstart_gremlin"

[[example]]
name = "quickstart_gql"

[[example]]
name = "scripting"
required-features = ["rhai"]

[[example]]
name = "storage"
required-features = ["mmap"]

[[example]]
name = "marvel"

[[example]]
name = "nba"
required-features = ["mmap"]
```

---

## 5. Implementation Plan

### Phase 1: Create New Examples

1. **Create `quickstart_gremlin.rs`**
   - Extract and simplify from current `quickstart.rs` Part 1
   - Focus on Gremlin mutations and basic traversals
   - Target: ~120 lines

2. **Create `quickstart_gql.rs`**
   - Extract and simplify from current `quickstart.rs` Part 2 and `gql.rs`
   - Focus on GQL CRUD operations
   - Target: ~120 lines

3. **Rename `rhai_scripting.rs` to `scripting.rs`**
   - Clean up and simplify
   - Target: ~150 lines

4. **Create `storage.rs`**
   - Minimal mmap demo extracted from `persistence.rs`
   - Target: ~80 lines

5. **Rewrite `marvel.rs`**
   - Keep JSON fixture loading
   - Reorganize into clear sections
   - Add missing Gremlin features
   - Remove any GQL code
   - Target: ~500 lines

6. **Rewrite `nba.rs`**
   - Keep JSON fixture loading
   - Focus on GQL features (currently has both)
   - Add advanced GQL features from `gql.rs`
   - Target: ~500 lines

### Phase 2: Remove Old Examples

1. Delete `quickstart.rs`
2. Delete `gql.rs`
3. Delete `british_royals.rs`
4. Delete `indexes.rs`
5. Delete `cow_unified_api.rs`
6. Delete `cow_mmap_unified_api.rs`
7. Delete `persistence.rs`
8. Delete `examples/fixtures/british_royals.json`

### Phase 3: Update Cargo.toml

1. Remove old `[[example]]` entries
2. Add new `[[example]]` entries as specified

### Phase 4: Verification

1. Run all examples:
   ```bash
   cargo run --example quickstart_gremlin
   cargo run --example quickstart_gql
   cargo run --example scripting --features rhai
   cargo run --example storage --features mmap
   cargo run --example marvel
   cargo run --example nba --features mmap
   ```
2. Verify output is clear and educational
3. Update any documentation that references old examples

---

## 6. Migration Checklist

### Files to Create (4)
- [ ] `examples/quickstart_gremlin.rs`
- [ ] `examples/quickstart_gql.rs`
- [ ] `examples/storage.rs`
- [ ] (rename) `examples/scripting.rs` (from `rhai_scripting.rs`)

### Files to Rewrite (2)
- [ ] `examples/marvel.rs` - Focus on Gremlin features
- [ ] `examples/nba.rs` - Focus on GQL features

### Files to Delete (8)
- [ ] `examples/quickstart.rs`
- [ ] `examples/gql.rs`
- [ ] `examples/british_royals.rs`
- [ ] `examples/indexes.rs`
- [ ] `examples/cow_unified_api.rs`
- [ ] `examples/cow_mmap_unified_api.rs`
- [ ] `examples/persistence.rs`
- [ ] `examples/rhai_scripting.rs` (after rename)

### Fixtures to Delete (1)
- [ ] `examples/fixtures/british_royals.json`

### Cargo.toml Updates
- [ ] Remove old `[[example]]` entries
- [ ] Add new `[[example]]` entries

---

## 7. Success Criteria

1. Example count reduced from 10 to 6
2. Quickstart examples are under 150 lines each
3. Gremlin and GQL are clearly separated
4. All examples run successfully
5. Each example has a clear, focused purpose
6. Dataset examples demonstrate advanced features of their respective query language
7. `cargo run --example <name>` works for all examples

---

## 8. Relationship to Previous Specs

This spec supersedes **Spec 24: Examples Directory Reorganization**, which was partially implemented. The key differences are:

1. **Spec 24** aimed for 5 examples; this spec targets 6 for clearer separation
2. **Spec 24** kept a combined quickstart; this spec splits into Gremlin vs GQL
3. **Spec 24** focused on removing test-like examples; this spec refactors all examples
4. This spec explicitly assigns Gremlin features to Marvel and GQL features to NBA
