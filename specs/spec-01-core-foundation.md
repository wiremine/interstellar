# Spec 01: Core Foundation (Phase 1)

Defines the foundational types and traits for RustGremlin. Derived from `implementation.md` and aligned with `overview.md` and anonymous traversal semantics.

---

## Goals
- Establish core identifiers and value representation
- Define error hierarchy for storage and traversal
- Specify storage abstraction trait
- Provide string interning facility contract
- Introduce graph handle types for read/write access
- Prepare `lib.rs` exports for public API surface

## Scope
- `src/value.rs`: IDs, `Value`, conversions, serialization hooks
- `src/error.rs`: `StorageError`, `TraversalError`
- `src/storage/mod.rs`: `GraphStorage` trait (read-only contract for now)
- `src/storage/interner.rs`: `StringInterner` interface
- `src/graph.rs`: `Graph`, `GraphSnapshot`, `GraphMut` shells
- `src/lib.rs`: module wiring and prelude exports

## Design Principles (recap)
- Strongly-typed IDs (`VertexId`, `EdgeId`, `ElementId`)
- Lazy, iterator-driven traversals (no eager materialization by default)
- Result-based errors, no panics in library code
- Thread-safe graph handle with reader/writer separation
- Zero-cost abstractions on hot paths; boxing only where necessary

---

## File Specifications

### `src/value.rs`
**Purpose**: Core identifiers and property value type.

**Types**:
- `VertexId(pub(crate) u64)`, `EdgeId(pub(crate) u64)` — `Copy`, `Eq`, `Ord`, `Hash`, `Debug`
- `ElementId` enum: `Vertex(VertexId)` | `Edge(EdgeId)`
- `Value` enum variants: `Null`, `Bool(bool)`, `Int(i64)`, `Float(f64)`, `String(String)`, `List(Vec<Value>)`, `Map(HashMap<String, Value>)`

**Implementations**:
- `From` for common primitives (`bool`, numeric, `String`, `&str`, collections where reasonable)
- Comparability helper (e.g., `ComparableValue`) for ordered index keys (tie into index specs)
- Serialization hooks: binary serialize/deserialize stubs per `algorithms.md` (round-trip tests in later phases)

**Derives**: Prefer `Clone`, `Debug`, `PartialEq`; IDs also derive `Ord`, `PartialOrd`, `Hash`, `Copy`, `Eq`.

### `src/error.rs`
**Purpose**: Error hierarchy for storage and traversal.

**Types**:
- `StorageError` (`thiserror::Error`):
  - `VertexNotFound(VertexId)`
  - `EdgeNotFound(EdgeId)`
  - `Io(std::io::Error)` via `From`
  - `WalCorrupted(String)`
  - `InvalidFormat`
- `TraversalError` (`thiserror::Error`):
  - `NotOne(usize)` (expected exactly one result)
  - `Storage(#[from] StorageError)`

**Principles**:
- No panics in library paths; propagate `Result`.
- Keep errors printable and user-facing.

### `src/storage/mod.rs`
**Purpose**: Storage abstraction surface (read/query focused for Phase 1).

**Trait**: `GraphStorage: Send + Sync`
- Vertex ops: `get_vertex(VertexId) -> Option<Vertex>`, `vertex_count() -> u64`
- Edge ops: `get_edge(EdgeId) -> Option<Edge>`, `edge_count() -> u64`
- Adjacency: `out_edges(VertexId) -> Box<dyn Iterator<Item = Edge> + '_>`, `in_edges(VertexId) -> Box<dyn Iterator<Item = Edge> + '_>`
- Label scans: `vertices_with_label(&str) -> Box<dyn Iterator<Item = Vertex> + '_>`, `edges_with_label(&str) -> Box<dyn Iterator<Item = Edge> + '_>`
- Full scans: `all_vertices()`, `all_edges()`

**Notes**:
- Later phases may specialize iterators with associated types for performance; boxing is acceptable at this stage for uniformity.
- `Vertex`/`Edge` structs are declared in `graph.rs` (Phase 1 shell) or a shared module; concrete layout may evolve.

### `src/storage/interner.rs`
**Purpose**: String interning for labels/property keys.

**Contract**:
- `StringInterner::new() -> Self`
- `intern(&str) -> u32` (returns stable ID)
- `resolve(u32) -> Option<&str>` (or owned `String` depending on storage design)
- Thread-safety as required by storage backend (scope: per-storage instance, not global)

**Notes**:
- Backed by `hashbrown::HashMap` or similar; no global state.
- Enables portable on-disk representations (used by mmap backend later).

### `src/graph.rs`
**Purpose**: Public graph handle with snapshot/mutation shells.

**Types (shells for now)**:
- `Graph` (owns `Arc<dyn GraphStorage>` or variant)
- `GraphSnapshot<'g>`: read-only view, holds read guard/version
- `GraphMut<'g>`: write-capable view, holds write guard/buffer (Phase 1 need only type shells)

**Concurrency model (baseline)**:
- `RwLock`-based: multiple readers, single writer
- Snapshot consistency: `GraphSnapshot` captures version/guard
- Mutations buffered in `GraphMut`; commit/rollback semantics detailed in later phases

### `src/lib.rs`
**Purpose**: Module wiring and prelude exports.

**Modules**: `graph`, `value`, `error`, `storage`, `index`, `traversal`

**Prelude exports** (names only; implementations may be stubs):
- Graph types: `Graph`, `GraphSnapshot`, `GraphMut`
- IDs/values: `Value`, `VertexId`, `EdgeId`, `ElementId`
- Errors: `StorageError`, `TraversalError`
- Traversal API surface: `Traversal`, `Traverser`, `Path`, `GraphTraversalSource`, `p`, `__` (types exist as stubs until later phases)

---

## Acceptance Criteria
- All listed files exist with the specified type definitions and trait/error shapes
- Types derive the stated traits; code compiles under `cargo check`
- Prelude in `lib.rs` re-exports the core types (even if implementations are placeholders)
- `Value` conversions and IDs are strongly typed; no `u64` leakage in public API

## Testing Hooks
- Unit tests for `Value` conversions and ID ordering (can be skeletons now)
- Serialization round-trip tests planned; may be added once format is finalized
- Error display strings validated via `thiserror` derives

## References
- `specs/implementation.md` (Phase 1: Core Foundation)
- `guilding-documents/overview.md` (Architecture overview)
- `guilding-documents/anonymous_traversal.md` (context for traversal-related exports)
