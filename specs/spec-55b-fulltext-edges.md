# Spec 55b — Full-Text Search on Edges

**Status**: Draft
**Depends on**: [`spec-55-fulltext-search.md`](spec-55-fulltext-search.md) Phases 1 & 2 (shipped)
**Scope**: Single chunk of work, COW backend only. No mmap persistence (still deferred to spec-55 Phase 4).

---

## 1. Motivation

Spec-55 Phase 2 shipped a working Tantivy-backed full-text search system, but only over **vertex** properties. Edges with text payloads (e.g. `comment.body`, `message.text`, `audit_log.note`, edge `description`) currently have no first-class search path. The two existing workarounds — reifying edges as vertices, or denormalizing onto an endpoint — distort the data model and inflate the vertex count.

This spec adds symmetric full-text search support for edge properties.

## 2. Non-goals

- Persistence to the mmap backend (tracked under spec-55 Phase 4).
- GQL / Gremlin string-parser surface for `searchTextE(...)` (tracked under spec-55 Phase 3).
- Mixed-element queries ("search vertices and edges with one call"). Each call still targets exactly one element type.
- More-like-this (`search_text_from`).

## 3. Design summary (decisions already locked in)

| Question | Decision |
|---|---|
| Registry shape on `Graph` | **Two parallel maps** — `text_indexes_vertex` and `text_indexes_edge`. Mirrors the boilerplate convention used elsewhere (`indexes` for property indexes is currently a single map, but the parallel-map shape keeps each call site obviously typed and avoids tuple-keying the hot path). |
| `TextHit` shape | **Hard break** — replace `pub vertex: VertexId` with `pub element: ElementRef`, where `ElementRef::{Vertex(VertexId), Edge(EdgeId)}`. All Phase-1 unit tests, integration tests, docs, and the quickstart example are updated in the same PR. No deprecation period. |
| Index-creation API | **Generalize** — `create_text_index(element_type: ElementType, property: &str, config: TextIndexConfig)`. Existing callers add `ElementType::Vertex` as a positional argument. The change is backed by a one-line search-and-replace across the integration tests, quickstart, and docs. |
| Edge search method names | `search_text_e(property, query, k)` and `search_text_query_e(property, &TextQuery, k)` on `CowTraversalSource`, mirroring the project's `v` / `e` naming convention. Vertex-side methods keep their current names. |

## 4. API surface

### 4.1 `interstellar::storage::text`

```rust
/// Discriminator carried by every `TextHit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ElementRef {
    Vertex(VertexId),
    Edge(EdgeId),
}

#[derive(Debug, Clone, PartialEq)]
pub struct TextHit {
    /// The matching element (vertex or edge).
    pub element: ElementRef,
    /// BM25 relevance score (larger = more relevant).
    pub score: f32,
}

pub trait TextIndex: Send + Sync {
    fn config(&self) -> &TextIndexConfig;

    /// Insert or replace the text associated with an element.
    ///
    /// `id` is opaque from the index's perspective — callers (the COW backend)
    /// guarantee that the same `u64` space is used consistently per index. We
    /// reuse `u64` rather than `ElementRef` here because Tantivy's fast field
    /// is `u64` and there is exactly one element type per index instance.
    fn upsert(&self, id: u64, text: &str) -> Result<(), TextIndexError>;
    fn delete(&self, id: u64) -> Result<(), TextIndexError>;
    fn commit(&self) -> Result<(), TextIndexError>;
    fn merge(&self) -> Result<(), TextIndexError>;
    fn search(&self, query: &TextQuery, k: usize) -> Result<Vec<TextHit>, TextIndexError>;
    // ... existing methods unchanged ...
}
```

The Phase-1 trait took `id: VertexId`. We widen to `u64` (the inner repr of both `VertexId` and `EdgeId`) so a single trait implementation serves both element types. The surrounding registry knows whether it's holding a vertex- or edge-scoped index and constructs the appropriate `ElementRef` when reading back search hits.

### 4.2 `interstellar::storage::cow::Graph`

```rust
use interstellar::index::ElementType;

// Replaces the current vertex-only `create_text_index(property, config)`.
pub fn create_text_index(
    &self,
    element_type: ElementType,
    property: &str,
    config: TextIndexConfig,
) -> Result<(), TextIndexError>;

pub fn drop_text_index(
    &self,
    element_type: ElementType,
    property: &str,
) -> Result<(), TextIndexError>;

pub fn text_index(
    &self,
    element_type: ElementType,
    property: &str,
) -> Option<Arc<dyn TextIndex>>;

pub fn has_text_index(&self, element_type: ElementType, property: &str) -> bool;

/// Names of properties indexed for the given element type.
pub fn list_text_indexes(&self, element_type: ElementType) -> Vec<String>;

/// Total registered indexes across both element types.
pub fn text_index_count(&self) -> usize;
```

### 4.3 `interstellar::storage::cow::CowTraversalSource`

```rust
// Vertex-side (unchanged from Phase 2)
pub fn search_text(&self, property: &str, query: &str, k: usize)
    -> Result<CowBoundTraversal<'g, (), Value, VertexMarker>, TextIndexError>;
pub fn search_text_query(&self, property: &str, query: &TextQuery, k: usize)
    -> Result<CowBoundTraversal<'g, (), Value, VertexMarker>, TextIndexError>;

// Edge-side (new)
pub fn search_text_e(&self, property: &str, query: &str, k: usize)
    -> Result<CowBoundTraversal<'g, (), Value, EdgeMarker>, TextIndexError>;
pub fn search_text_query_e(&self, property: &str, query: &TextQuery, k: usize)
    -> Result<CowBoundTraversal<'g, (), Value, EdgeMarker>, TextIndexError>;
```

### 4.4 New traversal source variant

```rust
pub enum TraversalSource {
    // ... existing variants ...
    #[cfg(feature = "full-text")]
    VerticesWithTextScore(Vec<(VertexId, f32)>),
    #[cfg(feature = "full-text")]
    EdgesWithTextScore(Vec<(EdgeId, f32)>),
}
```

Each of the five existing `TraversalSource` match sites adds an `EdgesWithTextScore` arm that calls `Traverser::from_edge(id)` + `set_sack(score)`. The streaming variant uses `storage.get_edge(id)` for existence verification.

## 5. Implementation layers (mirrors Phase 2)

Each layer ends in a green `cargo check --features full-text -p interstellar` and gets committed before the next layer starts.

### Layer 1 — Trait widening (`u64` ids) and `TextHit` rework

- `TextIndex::upsert` / `delete` take `id: u64`.
- `TextHit` becomes `{ element: ElementRef, score: f32 }`. Add `ElementRef` enum.
- Update `tantivy_index.rs` to construct `TextHit` with `ElementRef::Vertex(VertexId(id))` (it doesn't yet know about edges; the registry will translate by element type — see Layer 3).
- **Update all 12 occurrences** of `hits[i].vertex` in `storage/text/tests.rs` plus the doc-comment example in `storage/text/mod.rs:34`.

### Layer 2 — Generalize the `Graph` registry

- Replace `text_indexes: RwLock<HashMap<String, Arc<dyn TextIndex>>>` with two fields: `text_indexes_vertex` and `text_indexes_edge`.
- Update both constructors (`new`, `with_schema`) to initialize both maps.
- Update all six `Graph` methods (`create_text_index`, `drop_text_index`, `text_index`, `has_text_index`, `list_text_indexes`, `text_index_count`) to take or honor an `ElementType` parameter per §4.2.
- Update the back-fill path in `create_text_index` to walk all *edges* when `element_type == Edge` (using `all_edges()`), reading the property from the edge's properties map. The vertex back-fill path is unchanged structurally.
- Tantivy stores `id: u64`; the registry knows the element type, so when surfacing hits to callers, it reconstructs `ElementRef::Vertex(VertexId(id))` or `ElementRef::Edge(EdgeId(id))`. **Decision**: the registry wraps each `Arc<dyn TextIndex>` in a tiny `TypedTextIndex { element_type: ElementType, inner: Arc<dyn TextIndex> }` shim that owns the `ElementRef` reconstruction in its `search` method. This keeps `TantivyTextIndex` element-type-agnostic.

  Alternative considered: thread `ElementType` through the trait. Rejected because it pollutes every call site for a concern only the registry cares about.

### Layer 3 — Edge mutation hooks

Add three private helpers parallel to the vertex ones:

- `text_index_edge_insert(id: EdgeId, properties: &HashMap<String, Value>)`
- `text_index_edge_remove(id: EdgeId)`
- `text_index_edge_property_update(id: EdgeId, property: &str, new_value: &Value)`

Wire them into:

- `Graph::add_edge` (cow.rs:1365) — infallible signature, errors swallowed (matches `add_vertex` precedent).
- `Graph::set_edge_property` (cow.rs:1494) — `Result`-returning, surface as `StorageError::IndexError(...)`.
- `Graph::remove_edge` (cow.rs:1620) — `Result`-returning. Errors surfaced.

Each hook calls `commit()` after each `upsert` / `delete`, matching the Phase-2 vertex-side behaviour. Batched-commit is still future work.

### Layer 4 — Traversal source step

- Add `TraversalSource::EdgesWithTextScore(Vec<(EdgeId, f32)>)` (gated).
- Add a match arm for it to all five sites already touched in Phase 2:
  1. `traversal/source.rs` `TraversalExecutor::new`
  2. `traversal/step.rs` `StartStep::apply`
  3. `traversal/step.rs` `StartStep::apply_streaming`
  4. `traversal/step.rs` `LazyExecutor::from_source`
  5. `traversal/streaming.rs` `build_streaming_source`
- Add `search_text_e` and `search_text_query_e` on `CowTraversalSource` returning `CowBoundTraversal<…, EdgeMarker>`. Implementation mirrors the vertex-side pair, looking up the index via `graph.text_index(ElementType::Edge, property)` and translating each `TextHit { element: ElementRef::Edge(eid), score }` into `(eid, score)`.

### Layer 5 — Tests

Extend `interstellar/tests/storage/text_search_integration.rs`:

- Update existing 13 vertex-side tests to pass `ElementType::Vertex` to `create_text_index` / `drop_text_index` / etc.
- Add 13 parallel edge-side tests (one-for-one mirror): lifecycle, mutation hooks for `add_edge` / `set_edge_property` / `remove_edge`, top-k, phrase, error on missing index, k=0, chaining with `has_label`, descending score order.
- Add **two cross-tests** that exercise the parallel-map design:
  - Same property name (`body`) indexed on both vertices and edges; searches return only the requested element type.
  - `text_index_count()` reports the sum across both maps.

Target: 26 + 2 = **28 integration tests** in `text_search_integration.rs` after this spec lands. Lib unit-test count climbs by ~12 (re-shaped for `ElementRef`).

### Layer 6 — Migration of docs and example

- `interstellar/docs/guides/full-text-search.md`:
  - Remove the "Vertex properties only" limitation.
  - Add an "Edge properties" section paralleling the existing vertex one, with a worked example (e.g. searching `comment.body` on edges between users).
  - Update every code snippet to pass `ElementType::Vertex` (or `Edge`).
  - Update the `TextHit` snippet to use `hit.element`.
- `interstellar/docs/reference/feature-flags.md`:
  - Update the "Limitations" subsection (drop the vertex-only line).
  - Update the quick-reference snippet to pass `ElementType::Vertex`.
  - Update the Feature Matrix row "Text indexes" to read "Yes (vertices and edges, COW backend only)".
- `interstellar/examples/quickstart_text_search.rs`: update `create_text_index` call to pass `ElementType::Vertex`. Add a small final section that mirrors the same workflow on an edge property (e.g. an `endorses` edge with a `note` body) so readers see the symmetric API.

### Layer 7 — Final gate

- `cargo test --features full-text -p interstellar --lib --test storage` — expect ~2010 lib + ~146 integration tests passing (currently 1998 + 133).
- `cargo clippy --features full-text -p interstellar --lib --tests` — zero warnings on any file we touched.
- `cargo check -p interstellar` (default features, no `full-text`) — clean.
- `cargo run --example quickstart_text_search --features full-text` — runs to completion, prints both vertex and edge search results.

## 6. Migration burden snapshot

Mechanical changes counted from the call-site survey:

| File | Change | Count |
|---|---|---|
| `interstellar/src/storage/text/mod.rs` | `TextHit` definition + doc snippet | 2 |
| `interstellar/src/storage/text/tantivy_index.rs` | `TextHit` construction site | 1 |
| `interstellar/src/storage/text/tests.rs` | `hits[i].vertex` → `hits[i].element` | 12 |
| `interstellar/src/storage/cow.rs` | Six `Graph::*_text_index` methods generalized; `search_text` impl reads via `(ElementType::Vertex, prop)` | ~10 |
| `interstellar/tests/storage/text_search_integration.rs` | All `create_text_index` / `drop_text_index` / etc. calls take `ElementType::Vertex` | ~25 |
| `interstellar/examples/quickstart_text_search.rs` | One `create_text_index` call + add edge section | 1 + new section |
| `interstellar/docs/guides/full-text-search.md` | Snippets + new edge section | ~8 |
| `interstellar/docs/reference/feature-flags.md` | Snippet + Feature Matrix + Limitations | 3 |

Estimated effort: **half a day** of focused work.

## 7. Risks

- **Edge property storage shape**: I have not re-verified the exact shape of `add_edge`'s properties argument. If edge properties live in a different structure than vertex properties, Layer 3 needs a small adapter. Will confirm during Layer 3 by reading `add_edge` (`cow.rs:1365`).
- **`u64` trait ids erase type safety**: a future refactor that wires the same `TantivyTextIndex` instance to both vertex and edge mutations would silently corrupt results. Mitigation: the registry never shares an instance across element types — each `(ElementType, property)` gets its own. Document this invariant in the trait doc-comment.
- **Cross-tests catch wrong-map writes**: the "same property name on both maps" cross-test specifically guards against a bug where a vertex insert leaks into an edge index (or vice versa).

## 8. Open questions

None blocking — all five design points were decided up front. If the edge property survey in Layer 3 turns up a surprise, we'll surface it as an inline note at that point.
