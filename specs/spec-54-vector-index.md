# Spec 54: Vector / Embedding Index (kNN)

## Overview

Add a first-class **approximate nearest neighbor (ANN)** index over high-dimensional float vectors stored as graph element properties. The index lets users mix semantic similarity search with graph traversal in a single composable pipeline — e.g. *"find the 50 documents most similar to this query embedding, then expand to their authors and filter by org."*

This is feature proposal #4 from `specs/feature-proposals.md` and is positioned as Tier 1 / table-stakes for modern graph databases (Neo4j 5, Memgraph, ArangoDB, TigerGraph all ship vector indexes).

### Goals

1. **New `Value` variant**: `Value::Vector(Vec<f32>)` for storing embeddings as properties.
2. **HNSW index**: Hierarchical Navigable Small World index over a named vector property, registered through the schema builder.
3. **Distance metrics**: Cosine, L2 (Euclidean), dot product.
4. **Traversal step**: `search_vector(prop, query, k)` yielding vertices ranked by similarity, with the score available via `select`/`project`.
5. **GQL surface**: A `VECTOR SEARCH` clause that composes with `MATCH`/`WHERE`/`RETURN`.
6. **Composability**: Search results flow into the existing pipeline — filter, traverse, aggregate downstream.
7. **Persistence**: Index serializable to disk for the mmap backend; rebuildable from scratch on the in-memory backend.
8. **Generic over storage**: Works against `GraphAccess`, both `cow` and `mmap` backends.
9. **100% branch coverage target**: Unit, property-based, and integration tests.

### Non-Goals

- **IVF-PQ / quantization**: Defer compressed indexes to a follow-up spec; HNSW first.
- **Vector arithmetic**: No `add`/`subtract`/`normalize` operators on vectors as first-class steps. Users compute embeddings externally.
- **Built-in embedding model**: Interstellar does not generate embeddings; it stores and indexes them.
- **Sparse vectors**: Dense `Vec<f32>` only.
- **Edge vector indexes**: Vertices only in v1. Edge vectors are a follow-up.
- **Multi-vector / ColBERT-style indexes**: Out of scope.
- **GPU acceleration**: CPU-only.
- **Hybrid (BM25 + vector) scoring**: Defer to integration with the planned full-text search work.

---

## Architecture

```
interstellar/src/storage/
└── vector/
    ├── mod.rs              # VectorIndex trait, IndexConfig, registration
    ├── hnsw.rs             # HNSW implementation wrapper
    ├── distance.rs         # Cosine, L2, Dot metrics
    ├── persistence.rs      # On-disk layout for mmap backend
    └── tests.rs

interstellar/src/traversal/steps/
└── search_vector.rs        # SearchVectorStep
```

The `Value` enum gains one variant in `interstellar/src/value.rs`:

```rust
pub enum Value {
    // ... existing variants ...
    /// A dense f32 vector (embedding). Length is fixed per indexed property.
    Vector(Vec<f32>),
}
```

`ComparableValue` treats `Vector` via lexicographic comparison of `OrderedFloat` components — sufficient for `BTreeMap` keying though not semantically meaningful.

### Core Types

```rust
// storage/vector/mod.rs

use crate::value::{VertexId, Value};
use crate::error::StorageError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum VectorIndexError {
    #[error("vector dimension mismatch: index expects {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },

    #[error("property '{0}' is not registered as a vector index")]
    PropertyNotIndexed(String),

    #[error("vertex {0:?} has no vector value for property '{1}'")]
    MissingVector(VertexId, String),

    #[error("invalid vector: contains NaN or infinite component")]
    InvalidVector,

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("index corruption: {0}")]
    Corruption(String),
}

/// Distance metric for similarity computation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    /// 1 - cos(a, b). Range [0, 2]. Most common for embeddings.
    Cosine,
    /// Euclidean (L2). Range [0, ∞).
    L2,
    /// -dot(a, b). Negated so smaller is more similar (consistent with Cosine/L2).
    Dot,
}

/// Configuration for an HNSW index.
#[derive(Debug, Clone)]
pub struct IndexConfig {
    pub dimension: usize,
    pub metric: DistanceMetric,
    /// HNSW parameter M: max connections per node per layer. Typical: 16–48.
    pub m: usize,
    /// HNSW parameter ef_construction: candidate list size during build. Typical: 100–400.
    pub ef_construction: usize,
    /// Default ef_search at query time (overridable per query). Typical: 50–200.
    pub ef_search: usize,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            dimension: 0, // must be set
            metric: DistanceMetric::Cosine,
            m: 16,
            ef_construction: 200,
            ef_search: 64,
        }
    }
}

/// A single search hit.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorHit {
    pub vertex: VertexId,
    /// Distance under the configured metric (smaller = more similar).
    pub distance: f32,
}

/// Trait every backend's vector index implements.
pub trait VectorIndex: Send + Sync {
    fn config(&self) -> &IndexConfig;

    /// Insert or update the vector for `vertex`.
    fn upsert(&mut self, vertex: VertexId, vec: &[f32]) -> Result<(), VectorIndexError>;

    /// Mark the vertex's vector as deleted (tombstone — HNSW deletes are soft).
    fn delete(&mut self, vertex: VertexId) -> Result<(), VectorIndexError>;

    /// Approximate top-k nearest neighbors. `ef_search` overrides the configured default.
    fn search(
        &self,
        query: &[f32],
        k: usize,
        ef_search: Option<usize>,
    ) -> Result<Vec<VectorHit>, VectorIndexError>;

    /// Number of live (non-tombstoned) entries.
    fn len(&self) -> usize;

    /// Tombstone ratio. Callers can use this to decide when to rebuild.
    fn fragmentation(&self) -> f32;

    /// Rebuild compactly from live entries (drops tombstones, optionally re-tunes graph).
    fn rebuild(&mut self) -> Result<(), VectorIndexError>;
}
```

### Storage Integration

The `GraphStorage` trait gains optional vector index methods (parallel to existing `optional indexed lookups`):

```rust
pub trait GraphStorage: Send + Sync {
    // ... existing methods ...

    /// Register a vector index on a vertex property. Idempotent.
    fn create_vector_index(
        &mut self,
        property: &str,
        config: IndexConfig,
    ) -> Result<(), VectorIndexError> {
        Err(VectorIndexError::Storage(StorageError::Unsupported))
    }

    fn drop_vector_index(&mut self, property: &str) -> Result<(), VectorIndexError> {
        Err(VectorIndexError::Storage(StorageError::Unsupported))
    }

    fn vector_index(&self, property: &str) -> Option<&dyn VectorIndex> { None }
}
```

Mutation paths (`set_property`, `add_vertex`, `drop_vertex`) hook into the registered indexes when a write touches an indexed property.

### Schema Builder Integration

```rust
schema.vertex("Document")
      .property("embedding", PropertyType::Vector { dim: 768 })
      .vector_index("embedding", IndexConfig {
          dimension: 768,
          metric: DistanceMetric::Cosine,
          ..Default::default()
      });
```

---

## Traversal Step: `search_vector`

```rust
// traversal/steps/search_vector.rs

pub struct SearchVectorStep {
    property: String,
    query: Vec<f32>,
    k: usize,
    ef_search: Option<usize>,
}

impl<G: GraphAccess> Step<(), Vertex> for SearchVectorStep { /* ... */ }
```

Usage:

```rust
// Pure similarity search
let hits: Vec<Vertex> = g.search_vector("embedding", &q, 10).to_list()?;

// Hybrid: similarity + graph expansion
let authors = g.search_vector("embedding", &q, 50)
    .in_("AUTHORED")
    .where_(__.out("MEMBER_OF").has("name", "acme"))
    .dedup()
    .to_list()?;

// Project the score
let scored = g.search_vector("embedding", &q, 10)
    .project(&["doc", "score"])
        .by(__.identity())
        .by(__.vector_score())   // pulls the score from the traverser side-effect
    .to_list()?;
```

The score is attached to each `Traverser` as a side-effect (`SideEffectKey::VectorScore`) so downstream `project`/`select` can read it without re-computing distance.

### Composition Rules

- `search_vector` is a **source step** (like `g.V()`); it does not consume an upstream traverser stream.
- A new variant `search_vector_from(prop, k)` consumes upstream vertices, treating each as the query (one search per input). Useful for "for each user, find similar users."
- Results are emitted in **ascending distance order**. `range`, `limit`, `order` apply as usual.

---

## GQL Surface

### Grammar Extension

```
vector_search ::= 'VECTOR' 'SEARCH' label '.' property
                  'NEAR' expression
                  ('TOP' integer)?
                  ('WITH' 'EF' integer)?
                  ('AS' identifier)?
```

### Examples

```sql
-- Pure search
VECTOR SEARCH Document.embedding NEAR $query TOP 10 AS d
RETURN d.title, d.score;

-- Hybrid with MATCH
VECTOR SEARCH Document.embedding NEAR $query TOP 50 AS d
MATCH (d)<-[:AUTHORED]-(a:Person)-[:MEMBER_OF]->(o:Org {name: 'acme'})
RETURN a.name, d.title, d.score
ORDER BY d.score
LIMIT 20;

-- Index DDL
CREATE VECTOR INDEX ON :Document(embedding)
  OPTIONS { dimension: 768, metric: 'cosine', m: 16, ef_construction: 200 };

DROP VECTOR INDEX ON :Document(embedding);
```

`d.score` is a synthetic property exposed only on results from a `VECTOR SEARCH` clause and corresponds to the `VectorHit::distance`.

### Gremlin Text

```
g.searchVector('embedding', $query, 10)
 .in('AUTHORED')
 .has('org', 'acme')
 .toList()
```

---

## Persistence (mmap backend)

HNSW graph layout on disk:

```
┌──────────────────────────────┐
│ VectorIndexHeader            │  magic + version + IndexConfig
├──────────────────────────────┤
│ Layer count + entry point    │
├──────────────────────────────┤
│ Vector arena                 │  fixed-stride f32 array, indexed by internal id
├──────────────────────────────┤
│ Adjacency arena per layer    │  variable-length neighbor lists (compressed)
├──────────────────────────────┤
│ VertexId ↔ internal id maps  │  bidirectional
├──────────────────────────────┤
│ Tombstone bitset             │  RoaringBitmap of deleted internal ids
└──────────────────────────────┘
```

- Writes go through the existing **WAL**: each `upsert` / `delete` records a `VectorOp` entry.
- On startup, the index is mmap'd; tombstones are honored at query time.
- Compaction is an offline / background operation that drops tombstoned entries and rewrites the adjacency lists. Triggered manually via `interstellar compact-vector ./db --property embedding` or automatically when `fragmentation() > 0.3`.

### In-memory backend

- HNSW lives entirely in `parking_lot::RwLock<HnswState>`.
- COW snapshots see the index as of the snapshot epoch — writes after snapshot are invisible to that snapshot.
- On crash there is no recovery; rebuild from properties.

---

## Distance Metrics

```rust
// storage/vector/distance.rs

pub fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if na == 0.0 || nb == 0.0 { return 1.0; }
    1.0 - (dot / (na * nb))
}

pub fn l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y).powi(2)).sum::<f32>().sqrt()
}

pub fn neg_dot(a: &[f32], b: &[f32]) -> f32 {
    -a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>()
}
```

SIMD acceleration (`std::simd` or `wide`) is a follow-up optimization gated behind a `vector-simd` feature flag.

---

## Error Handling

| Scenario | Error |
|---|---|
| Query vector wrong dimension | `DimensionMismatch { expected, actual }` |
| Searching unindexed property | `PropertyNotIndexed(name)` |
| Property holds non-vector `Value` on insert | `MissingVector(id, prop)` |
| Vector contains NaN / ±Inf | `InvalidVector` |
| WAL replay finds malformed entry | `Corruption(reason)` |
| Underlying storage failure | `Storage(StorageError)` |

No panics. All public entry points return `Result`.

---

## Implementation Phases

### Phase 1: `Value::Vector` & in-memory HNSW
- Add `Value::Vector(Vec<f32>)` variant + `ComparableValue` mirror.
- Wrap a pure-Rust HNSW crate (`hnsw_rs` or `instant-distance` — see Open Questions).
- `VectorIndex` trait + in-memory implementation.
- `IndexConfig` and `DistanceMetric`.
- Unit tests + proptest on dimension validation, distance metrics.

### Phase 2: Schema & traversal step
- Schema builder API (`vector_index`).
- Storage trait extension; wire mutation hooks in COW backend.
- `SearchVectorStep` and `search_vector` source step on `GraphTraversalSource`.
- `search_vector_from` consuming variant.
- Score side-effect plumbing for `project`/`select`.
- Integration tests via Rust API.

### Phase 3: GQL & Gremlin
- Pest grammar additions for `VECTOR SEARCH`, `CREATE/DROP VECTOR INDEX`.
- Compiler emits `SearchVectorStep` in the IR.
- Gremlin text parser extension for `searchVector(...)`.
- End-to-end query tests.

### Phase 4: Persistence (mmap backend)
- WAL `VectorOp` entries (upsert, delete, create_index, drop_index).
- On-disk layout + recovery.
- Compaction tool (`interstellar compact-vector`).
- Crash-injection tests at every fsync boundary.

### Phase 5: Bindings
- Node.js (napi-rs): expose `Float32Array` for query vectors.
- WASM: same, with `Float32Array` ↔ `Vec<f32>` glue.
- (Future) PyO3: NumPy `ndarray` bridge — defer to spec-47 follow-up.

---

## Testing Strategy

### Unit Tests
- Dimension mismatch rejected on insert and on query.
- NaN / Inf rejected.
- Each metric verified against hand-computed values on small cases.
- Empty index returns empty hit list.
- Tombstoned vertex absent from results.
- `rebuild()` produces identical search results minus tombstones.

### Property-Based Tests (proptest)
- For random vector sets, top-1 of `search` equals `argmin` of brute-force distance (HNSW is approximate, so assert recall ≥ 0.95 over many trials rather than exact equality — use larger `ef_search` for the assertion).
- Search results sorted by ascending distance.
- Upsert is idempotent: `upsert(v, x); upsert(v, x)` ≡ `upsert(v, x)`.
- Delete then re-upsert restores searchability.

### Integration Tests
- End-to-end Gremlin: `g.searchVector(...).in_(...).has(...).toList()`.
- End-to-end GQL: `VECTOR SEARCH ... MATCH ... RETURN`.
- COW snapshot isolation: writes after `snapshot()` invisible to the snapshot's searches.
- mmap recovery: kill mid-upsert, restart, verify index integrity.

### Benchmarks (`benches/vector.rs`)
- Build time: 10K, 100K, 1M vectors at dim=128, 384, 768.
- Query latency p50/p95/p99 at k=10, 100 across the same sizes.
- Recall@10 vs brute force at varying `ef_search`.
- Hybrid query: vector search + 2-hop traversal vs naive scan.

---

## Dependencies

```toml
[dependencies]
# One of (decided in Open Questions):
hnsw_rs = "0.3"            # candidate A
# instant-distance = "0.6" # candidate B

ordered-float = "4.2"      # already added in spec-53; reused here
```

Both candidates are pure Rust, MIT/Apache, no C dependencies. SIMD acceleration deferred to a `vector-simd` feature flag wrapping `wide` or `std::simd`.

---

## Open Questions

1. **HNSW crate choice.** `hnsw_rs` is more mature and supports custom metrics; `instant-distance` has a cleaner API but fewer knobs. Prototype both in Phase 1 and pick based on benchmarks and persistence ergonomics. Worst case, port a minimal HNSW into the tree.

2. **Score representation.** Distance is metric-dependent (`Cosine` ∈ [0,2], `L2` ∈ [0,∞), `NegDot` ∈ (-∞,∞)). Should the GQL `d.score` expose raw distance, or normalize to a `[0,1]` similarity? Proposal: expose raw distance and provide a built-in `similarity(d.score, 'cosine')` function.

3. **Update semantics under MVCC (spec-40).** HNSW's graph is shared mutable state. Options: (a) per-snapshot copy-on-write of the adjacency lists (memory-heavy); (b) tombstone-only with periodic rebuild (recommended for v1); (c) wait for spec-40 and integrate. Picking (b) keeps this spec independent.

4. **Edge vector indexes.** Out of v1 scope, but the trait and storage hooks should be generic enough that adding `EdgeId` indexes is a follow-up rather than a rewrite. Validate during Phase 2 design.

5. **`Value::Vector` in GraphSON.** Proposal: serialize as `{ "@type": "g:Vector", "@value": [f32, ...] }`. Confirm during Phase 1.

6. **Memory budget.** A 1M × 768-dim f32 index is ~3 GB of raw vectors plus HNSW overhead. Should we surface a `max_memory` config and reject inserts past the limit, or rely on OS OOM? Recommend documenting the math and deferring enforcement.
