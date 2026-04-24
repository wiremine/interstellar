# Spec 55: Full-Text Search Index (BM25 / Inverted Index)

## Overview

Add a first-class **full-text search (FTS) index** over string-valued graph element properties. The index lets users mix relevance-ranked text search with graph traversal in a single composable pipeline — e.g. *"find the 50 documents matching 'distributed consensus', then expand to their authors and filter by org."*

Today, `interstellar/Cargo.toml` declares a `full-text = ["tantivy"]` feature flag and pulls Tantivy as an optional dependency, but **no implementation exists** in `interstellar/src/`. The flag is a placeholder. This spec promotes FTS from placeholder to first-class capability and is the natural companion to the vector index work in `spec-54-vector-index.md`, which explicitly defers hybrid BM25 + vector scoring to "the planned full-text search work."

This is the unimplemented entry from `todos/todo.md:3` ("Full Text search") and the Phase 5 item in `guiding-documents/document-store.md`.

### Goals

1. **Tokenized inverted index** over user-selected string properties on vertices (and, optionally, edges — see Phase 2).
2. **BM25 relevance ranking** as the default scorer.
3. **Schema builder integration**: `text_index("body", TextIndexConfig::default())`.
4. **Traversal step**: `search_text(prop, query, k)` yielding vertices ordered by descending score, with the score available via `select`/`project`.
5. **GQL surface**: a `TEXT SEARCH` clause that composes with `MATCH`/`WHERE`/`RETURN`.
6. **Gremlin surface**: `g.searchText('body', 'distributed consensus', 10)`.
7. **Composability**: search results flow through the existing pipeline — filter, traverse, aggregate downstream.
8. **Persistence**: index serializable to disk for the `mmap` backend; rebuildable from properties on the in-memory backend.
9. **Pluggable analyzers**: standard (lowercase + Unicode word split + stopwords), whitespace, raw/keyword, n-gram. Default is standard English.
10. **Generic over storage**: works against `GraphAccess`, both `cow` and `mmap` backends.
11. **WASM-aware**: feature compiles only on non-WASM targets (Tantivy uses threads + filesystem). On WASM the trait stubs return `StorageError::Unsupported`.
12. **100% branch coverage target** for new code: unit, property-based, and integration tests.

### Non-Goals

- **Custom scoring functions.** BM25 only in v1; pluggable scorers deferred.
- **Synonym graphs / query expansion.** Out of scope; users can preprocess.
- **Phrase / proximity search beyond Tantivy's defaults.** We expose what Tantivy gives us, no custom rewriters.
- **Cross-field free-text search.** v1 indexes are per-property. A `multi_match` over multiple properties is a follow-up.
- **Faceting / aggregations** as native FTS operations. Compose via downstream traversal/aggregation steps.
- **Highlighting / snippets.** Deferred; expose hit positions in a follow-up.
- **Hybrid (BM25 + vector) scoring.** Coordinated with spec-54; the result-fusion step (RRF / weighted sum) is its own follow-up spec once both indexes ship.
- **Built-in language detection.** User picks the analyzer per index.
- **WASM support.** Tantivy is incompatible with `wasm32-unknown-unknown`. The `full-text` feature is mutually exclusive with the `wasm` feature.

---

## Architecture

```
interstellar/src/storage/
└── text/
    ├── mod.rs              # TextIndex trait, TextIndexConfig, registration
    ├── tantivy_index.rs    # Tantivy-backed implementation
    ├── analyzer.rs         # Analyzer enum + Tantivy tokenizer construction
    ├── query.rs            # Query parsing + safe builder API
    ├── persistence.rs      # On-disk layout + WAL hooks for mmap backend
    └── tests.rs

interstellar/src/traversal/steps/
└── search_text.rs          # SearchTextStep
```

No new `Value` variant is required — text indexes operate on existing `Value::String` properties (and optionally `Value::List(Vec<Value::String>)` for multi-valued fields).

### Core Types

```rust
// storage/text/mod.rs

use crate::value::VertexId;
use crate::error::StorageError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TextIndexError {
    #[error("property '{0}' is not registered as a text index")]
    PropertyNotIndexed(String),

    #[error("vertex {0:?} property '{1}' is not a string-valued field")]
    NonStringValue(VertexId, String),

    #[error("query parse error: {0}")]
    QueryParse(String),

    #[error("analyzer '{0}' is not registered")]
    UnknownAnalyzer(String),

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("index corruption: {0}")]
    Corruption(String),

    #[error("backend error: {0}")]
    Backend(String),
}

/// Built-in analyzers. Custom analyzers are deferred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Analyzer {
    /// Lowercase + Unicode word segmentation + English stopwords + Porter stemming.
    StandardEnglish,
    /// Lowercase + Unicode word segmentation. No stopwords, no stemming.
    Standard,
    /// Whitespace split only. Case-preserving.
    Whitespace,
    /// No tokenization. Indexes the raw string as a single term (useful for ids/tags).
    Raw,
    /// Character n-grams (min..=max). Good for substring / prefix match.
    NGram { min: usize, max: usize },
}

/// Configuration for a text index on one property.
#[derive(Debug, Clone)]
pub struct TextIndexConfig {
    pub analyzer: Analyzer,
    /// Persist positions for phrase queries. Costs ~30% disk.
    pub store_positions: bool,
    /// Tantivy `IndexSettings::sort_by_field` is left at default (none).
    /// BM25 parameters.
    pub bm25_k1: f32,   // default 1.2
    pub bm25_b: f32,    // default 0.75
    /// Auto-commit batch size. After N upserts, the writer commits.
    pub commit_every: usize,
}

impl Default for TextIndexConfig {
    fn default() -> Self {
        Self {
            analyzer: Analyzer::StandardEnglish,
            store_positions: true,
            bm25_k1: 1.2,
            bm25_b: 0.75,
            commit_every: 1024,
        }
    }
}

/// A single search hit.
#[derive(Debug, Clone, PartialEq)]
pub struct TextHit {
    pub vertex: VertexId,
    /// BM25 score under the configured parameters (larger = more relevant).
    pub score: f32,
}

/// Programmatic query builder. Mirrors the subset of Tantivy's QueryParser
/// we expose, but is backend-agnostic so we can swap implementations later.
#[derive(Debug, Clone)]
pub enum TextQuery {
    /// Free-text query parsed by the configured analyzer; OR-of-terms by default.
    Match(String),
    /// All terms must appear (intersection).
    MatchAll(String),
    /// Phrase query: terms in order, optional slop.
    Phrase { text: String, slop: u32 },
    /// Prefix on a single term.
    Prefix(String),
    /// Boolean composition.
    And(Vec<TextQuery>),
    Or(Vec<TextQuery>),
    Not(Box<TextQuery>),
}

/// Trait every backend's text index implements.
pub trait TextIndex: Send + Sync {
    fn config(&self) -> &TextIndexConfig;

    /// Insert or update the indexed text for `vertex`.
    /// Multi-valued: if the property is a `Value::List` of strings, all values
    /// are concatenated with U+0001 between them so phrase queries don't bridge.
    fn upsert(&mut self, vertex: VertexId, text: &str) -> Result<(), TextIndexError>;

    /// Remove the entry for `vertex`. Idempotent.
    fn delete(&mut self, vertex: VertexId) -> Result<(), TextIndexError>;

    /// Top-k matches, ordered by descending score.
    fn search(&self, query: &TextQuery, k: usize) -> Result<Vec<TextHit>, TextIndexError>;

    /// Total document count (logical, post-tombstone).
    fn len(&self) -> usize;

    /// Force a commit / segment flush.
    fn commit(&mut self) -> Result<(), TextIndexError>;

    /// Merge segments. Triggered manually or automatically based on segment count.
    fn merge(&mut self) -> Result<(), TextIndexError>;
}
```

### Storage Integration

The `GraphStorage` trait gains optional text-index methods, parallel to the vector index hooks in spec-54:

```rust
pub trait GraphStorage: Send + Sync {
    // ... existing methods ...

    fn create_text_index(
        &mut self,
        property: &str,
        config: TextIndexConfig,
    ) -> Result<(), TextIndexError> {
        Err(TextIndexError::Storage(StorageError::Unsupported))
    }

    fn drop_text_index(&mut self, property: &str) -> Result<(), TextIndexError> {
        Err(TextIndexError::Storage(StorageError::Unsupported))
    }

    fn text_index(&self, property: &str) -> Option<&dyn TextIndex> { None }

    fn text_index_mut(&mut self, property: &str) -> Option<&mut dyn TextIndex> { None }
}
```

Mutation paths (`set_property`, `add_vertex`, `drop_vertex`) hook into registered text indexes when a write touches an indexed property. If the new value is not `Value::String` or `Value::List(Value::String..)`, the index returns `NonStringValue` and the mutation fails atomically (consistent with the unique-index error path).

### Schema Builder Integration

```rust
schema.vertex("Document")
      .property("body", PropertyType::String)
      .text_index("body", TextIndexConfig {
          analyzer: Analyzer::StandardEnglish,
          ..Default::default()
      });
```

A `text_index_with_default(prop)` shortcut is also provided.

---

## Traversal Step: `search_text`

```rust
// traversal/steps/search_text.rs

pub struct SearchTextStep {
    property: String,
    query: TextQuery,
    k: usize,
}

impl<G: GraphAccess> Step<(), Vertex> for SearchTextStep { /* ... */ }
```

Usage:

```rust
// Pure relevance search
let hits: Vec<Vertex> = g.search_text("body", "distributed consensus", 10).to_list()?;

// Programmatic query builder for richer queries
let q = TextQuery::And(vec![
    TextQuery::Match("raft".into()),
    TextQuery::Not(Box::new(TextQuery::Match("paxos".into()))),
]);
let hits: Vec<Vertex> = g.search_text_query("body", q, 50).to_list()?;

// Hybrid: text + graph expansion
let authors = g.search_text("body", "raft", 50)
    .in_("AUTHORED")
    .where_(__.out("MEMBER_OF").has("name", "acme"))
    .dedup()
    .to_list()?;

// Project the score
let scored = g.search_text("body", "raft", 10)
    .project(&["doc", "score"])
        .by(__.identity())
        .by(__.text_score())   // pulls the score from the traverser side-effect
    .to_list()?;
```

The score is attached to each `Traverser` as a side-effect (`SideEffectKey::TextScore`) so downstream `project`/`select` can read it without re-querying. This mirrors `SideEffectKey::VectorScore` from spec-54 and the two side-effect channels coexist for hybrid queries.

### Composition Rules

- `search_text` is a **source step** (like `g.V()`); it does not consume an upstream traverser stream.
- A consuming variant `search_text_from(prop, k)` consumes upstream vertices and uses each vertex's *own* property value as the query. Useful for "more like this."
- Results are emitted in **descending score** order. `range`, `limit`, `order` apply as usual.
- `dedup()` is a no-op on direct results (the index never returns the same vertex twice) but useful after expansion.

---

## GQL Surface

### Grammar Extension

```
text_search ::= 'TEXT' 'SEARCH' label '.' property
                'MATCHES' string_literal
                ('TOP' integer)?
                ('AS' identifier)?

text_index_ddl ::= 'CREATE' 'TEXT' 'INDEX' 'ON' ':' label '(' property ')'
                   ('OPTIONS' '{' option_list '}')?
                 | 'DROP' 'TEXT' 'INDEX' 'ON' ':' label '(' property ')'
```

### Examples

```sql
-- Pure search
TEXT SEARCH Document.body MATCHES 'distributed consensus' TOP 10 AS d
RETURN d.title, d.score;

-- Hybrid with MATCH
TEXT SEARCH Document.body MATCHES 'raft' TOP 50 AS d
MATCH (d)<-[:AUTHORED]-(a:Person)-[:MEMBER_OF]->(o:Org {name: 'acme'})
RETURN a.name, d.title, d.score
ORDER BY d.score DESC
LIMIT 20;

-- Index DDL
CREATE TEXT INDEX ON :Document(body)
  OPTIONS { analyzer: 'standard_en', bm25_k1: 1.2, bm25_b: 0.75 };

DROP TEXT INDEX ON :Document(body);
```

`d.score` is a synthetic property exposed only on results from a `TEXT SEARCH` clause and corresponds to `TextHit::score`. It coexists with vector-index `score` when both clauses appear in one query (each clause binds its own alias).

### Query string syntax

Inside the `MATCHES` literal we accept the Tantivy query mini-language: `term`, `"phrase"`, `+required`, `-excluded`, `field:term` (rejected — single-property queries only in v1), `OR`, `AND`. Anything we don't allow is reported as `QueryParse(String)`.

### Gremlin Text

```
g.searchText('body', 'distributed consensus', 10)
 .in('AUTHORED')
 .has('org', 'acme')
 .toList()
```

For programmatic queries the Gremlin grammar gains a small DSL:

```
g.searchTextQuery('body', and(match('raft'), not(match('paxos'))), 50)
```

---

## Persistence (mmap backend)

Tantivy already manages its own on-disk segment-based layout. We do **not** reinvent it; we colocate the Tantivy index directory under the database root:

```
db_root/
├── nodes.dat
├── edges.dat
├── ...
└── text_indexes/
    └── <property_id>/
        ├── meta.json          # Tantivy meta
        ├── *.fast
        ├── *.idx
        ├── *.pos
        ├── *.term
        └── interstellar.toml  # our metadata: TextIndexConfig + schema_version
```

- Writes are journaled in our **WAL** as `TextOp::{Upsert, Delete, CreateIndex, DropIndex}` *before* being applied to Tantivy. The WAL replay path re-applies any operation whose Tantivy commit didn't land before the crash.
- Tantivy commits happen every `commit_every` upserts or every 1s, whichever first; the WAL allows us to lose only those uncommitted operations and replay them on startup.
- `DropIndex` removes the directory atomically (rename → delete).
- Segment merges run in a background thread; merging is opportunistic and never blocks writers.

### In-memory backend

- Tantivy is run in `RamDirectory` mode behind `parking_lot::RwLock<TantivyState>`.
- COW snapshots take a logical view of the index keyed by epoch — concretely, a snapshot holds a Tantivy `Searcher` from the snapshot moment; writers hold a `IndexWriter` that publishes new segments visible only to subsequent searchers. Snapshots are immutable.
- On crash there is no recovery; the schema-driven rebuild path (`storage.rebuild_text_index(prop)`) walks all vertices and re-indexes them.

---

## Analyzers

| Variant | Tantivy mapping |
|---|---|
| `Analyzer::StandardEnglish` | `default` tokenizer + `lowercase` + `stop_words(English)` + `stemmer(Stemmer::English)` |
| `Analyzer::Standard` | `default` tokenizer + `lowercase` |
| `Analyzer::Whitespace` | `whitespace` tokenizer |
| `Analyzer::Raw` | `raw` tokenizer |
| `Analyzer::NGram { min, max }` | `NgramTokenizer::new(min, max, false)` + `lowercase` |

Analyzers are constructed in `analyzer.rs` and registered with the Tantivy `TokenizerManager` per index. Changing an analyzer requires `DROP TEXT INDEX` + `CREATE TEXT INDEX` (a future migration helper is out of scope).

---

## Error Handling

| Scenario | Error |
|---|---|
| Searching unindexed property | `PropertyNotIndexed(name)` |
| Property holds non-string `Value` on insert | `NonStringValue(id, prop)` |
| Bad query syntax | `QueryParse(reason)` |
| Unknown analyzer name in DDL | `UnknownAnalyzer(name)` |
| WAL replay finds malformed entry | `Corruption(reason)` |
| Tantivy raises an internal error | `Backend(string)` |
| Underlying storage failure | `Storage(StorageError)` |
| Build target = `wasm32-*` and feature is enabled | compile error via `compile_error!` |

No panics. All public entry points return `Result`. Tantivy panics (rare; mostly OOM) are caught in `tantivy_index.rs` via `std::panic::catch_unwind` at the trait boundary and surfaced as `Backend(String)`.

---

## Feature Flag Wiring

```toml
[features]
default = ["graphson"]
mmap = ["memmap2", "serde_json"]
graphson = ["serde_json"]
full-text = ["tantivy"]                       # already declared; now actually used
gql = ["pest", "pest_derive", "mathexpr"]
gremlin = ["pest", "pest_derive"]
full = ["mmap", "graphson", "gql", "gremlin", "full-text"]
wasm = ["wasm-bindgen", "serde-wasm-bindgen", "js-sys"]
```

`interstellar/src/lib.rs` gains a top-of-file guard:

```rust
#[cfg(all(feature = "full-text", target_arch = "wasm32"))]
compile_error!("the `full-text` feature is not supported on wasm32 targets");
```

All FTS code lives behind `#[cfg(feature = "full-text")]`. The `GraphStorage` trait stubs above are gated on the feature too; without it, the methods don't exist and downstream code that doesn't depend on FTS is unaffected.

---

## Implementation Phases

### Phase 1: Trait + in-memory Tantivy
- `TextIndex` trait, `TextIndexConfig`, `Analyzer`, `TextHit`, `TextQuery`, `TextIndexError`.
- Tantivy-backed implementation in `RamDirectory` mode.
- Analyzer registration helpers.
- Unit + proptest coverage on tokenization, BM25 sanity, query parsing.
- Bump the Tantivy dependency from `0.21` to `0.25` in `interstellar/Cargo.toml:41`. A standalone probe confirms the upstream `zstd-safe`/`zstd-sys` conflict (the one warned about in `interstellar/README.md:628`) is resolved at this version. Verify the bump applies cleanly inside the full workspace and that it doesn't push the workspace MSRV above the declared `rust-version = "1.75"`.

### Phase 2: Schema, mutation hooks, traversal step
- Schema builder API (`text_index`).
- Storage trait extension; wire mutation hooks in COW (in-memory) backend.
- `SearchTextStep` and `search_text` source step on `GraphTraversalSource`.
- `search_text_query` (programmatic `TextQuery`) and `search_text_from` (more-like-this) variants.
- Score side-effect plumbing (`SideEffectKey::TextScore`) for `project`/`select`.
- Rust-API integration tests.

### Phase 3: GQL & Gremlin
- Pest grammar additions for `TEXT SEARCH`, `CREATE/DROP TEXT INDEX`.
- Compiler emits `SearchTextStep` in the IR.
- Gremlin text parser extension for `searchText(...)` and `searchTextQuery(...)`.
- End-to-end query tests against both surfaces.

### Phase 4: Persistence (mmap backend)
- WAL `TextOp` entries (upsert, delete, create_index, drop_index).
- On-disk layout under `db_root/text_indexes/<property_id>/`.
- Recovery: replay uncommitted WAL entries against Tantivy on open.
- Crash-injection tests at every fsync / Tantivy commit boundary.
- `interstellar rebuild-text-index ./db --property body` CLI tool for full rebuild.

### Phase 5: Bindings & Docs
- Node.js (napi-rs): expose `searchText`, `createTextIndex` on the JS Graph.
- PyO3 (when spec-47 lands): same surface plus a `pandas`-friendly result iterator.
- Update `interstellar/docs/reference/feature-flags.md`, `interstellar/README.md`, and `interstellar/src/lib.rs` doc comments to reflect the now-real feature.
- Mark `todos/todo.md:3` complete.

### Phase 6 (follow-up spec): Hybrid scoring
- Once both spec-54 (vectors) and spec-55 (text) ship, a separate spec defines result-fusion (Reciprocal Rank Fusion, weighted sum) and any required normalization.

---

## Testing Strategy

### Unit Tests
- Each analyzer round-trips representative inputs to expected token streams.
- BM25 sanity: a query term appearing more often in doc A than B ⇒ score(A) > score(B).
- `NonStringValue` returned when indexing a non-string property.
- Tombstoned vertex absent from results.
- `commit()` makes prior writes visible to subsequent searchers.
- Empty index returns empty hit list.
- Query parser rejects unsupported syntax (`field:term`).

### Property-Based Tests (proptest)
- Upsert is idempotent: `upsert(v, s); upsert(v, s)` ≡ `upsert(v, s)`.
- Delete then re-upsert restores searchability with the same rank as before (within float epsilon).
- For random doc sets, `Match("foo")` returns a non-empty result iff at least one indexed doc contains an analyzer-equivalent token.
- `Search(Match(t), k)` returns at most `k` hits, sorted descending by score, scores ≥ 0.

### Integration Tests
- End-to-end Gremlin: `g.searchText(...).in_(...).has(...).toList()`.
- End-to-end GQL: `TEXT SEARCH ... MATCH ... RETURN`.
- COW snapshot isolation: writes after `snapshot()` invisible to the snapshot's searches.
- mmap recovery: kill mid-upsert (between WAL append and Tantivy commit), restart, verify the entry is replayed and searchable.
- `DROP TEXT INDEX` followed by `CREATE TEXT INDEX` rebuilds and produces equivalent hits.
- Hybrid coexistence: a query that contains both `TEXT SEARCH` and `VECTOR SEARCH` clauses (each with its own alias) returns correct independent scores via `select`.

### Benchmarks (`benches/text.rs`)
- Index time: 10K, 100K, 1M docs at avg lengths 50, 500, 5000 tokens.
- Query latency p50/p95/p99 at k=10, 100 across the same sizes for `Match`, `Phrase`, `Prefix`.
- Hybrid query: text search + 2-hop traversal vs naive scan.
- Compare with and without `store_positions` to quantify the trade-off.

---

## Dependencies

```toml
[dependencies]
tantivy = { version = "0.25", optional = true }   # bump from 0.21 (verified to resolve known conflict)
```

The upstream dependency conflict warned about in `interstellar/README.md:628` is a `zstd-safe`/`zstd-sys` ABI break that affects anything still on `tantivy 0.21`. The chain at the currently pinned version is:

```
tantivy 0.21 → tantivy-columnar 0.2 → tantivy-sstable 0.2 → zstd 0.12 → zstd-safe 6.0.6 → zstd-sys 2.0.16+zstd.1.5.7
```

`zstd-safe 6.x` was written against older zstd C headers; `zstd-sys 2.0.16` bundles **zstd 1.5.7**, which renamed `ZSTD_paramSwitch_e` → `ZSTD_ParamSwitch_e` and removed `ZSTD_c_experimentalParam6`. The build fails inside `zstd-safe 6.0.6` with `E0432`/`E0433`. We have no path to fix this from our side — `tantivy 0.21`'s columnar/sstable subcrates are pinned to `zstd 0.12`.

At **tantivy 0.25** the chain advances to `zstd 0.13` → `zstd-safe 7.2.4`, which matches the new headers. A standalone probe with `tantivy = "0.25"` and no other dependencies builds cleanly (`cargo check` green). Resolution is therefore a straight version bump rather than vendoring.

Tantivy is pure Rust, MIT, no C dependencies of our own. Phase 1 still validates resolution within the actual interstellar workspace (other crates may force older transitive versions and trigger duplicates).

No new dev-dependencies are required.

---

## Open Questions

1. **Tantivy version & dependency conflict.** Resolved in principle: a standalone probe with `tantivy = "0.25"` builds cleanly because the chain advances past the broken `zstd-safe 6.x`. Phase 1 still must (a) confirm the bump applies cleanly inside the full interstellar workspace (no duplicate transitive versions force `zstd-safe 6.x` back in), (b) confirm Tantivy 0.25's MSRV is compatible with the workspace `rust-version = "1.75"` and bump it if not, and (c) port any API usage we add against the 0.21 → 0.25 changes (schema builder, tokenizer registration, `IndexBuilder`). The trait abstraction here is still designed to permit replacing Tantivy entirely (`fst` + a hand-rolled BM25 scorer over a roaring-bitmap posting list) if a future ecosystem break recurs.

2. **Edge text indexes.** Out of v1 scope (vertices only), but the `TextIndex` trait and storage hooks should be generic enough that adding `EdgeId`-keyed indexes is a follow-up rather than a rewrite. Validate during Phase 2 design.

3. **Multi-property / cross-field queries.** v1 is one index per property. A `multi_match(["title", "body"], "raft")` form is desirable but complicates schema, mutation hooks, and DDL. Defer to a follow-up; expose the per-property primitive cleanly so the higher-level form can be built on top.

4. **Score representation across queries.** BM25 scores are not comparable across indexes (different `avgdl`, doc counts). For hybrid scoring (spec-56) we will need either rank-based fusion (RRF) or per-index normalization. Document this clearly so users don't naively `ORDER BY text.score + vector.score`.

5. **Update semantics under MVCC (spec-40).** Tantivy already provides snapshot-isolated `Searcher`s tied to the moment they are created. Aligning that with our COW snapshot epochs requires either (a) caching a `Searcher` per snapshot (memory-heavy when snapshots are long-lived), or (b) tagging Tantivy docs with a `snapshot_visible_until` field and filtering at query time. (a) is simpler and recommended for v1.

6. **`Value::List(String)` indexing semantics.** Concatenation with a sentinel character is the simple option. The richer alternative is to add multiple Tantivy documents per vertex with the same `vertex_id` field — requires a `group_by(vertex_id).max(score)` at query time. Phase 1 chooses concatenation; revisit if users complain about phrase queries spanning list elements (the sentinel mitigates this).

7. **Hot-reload of `TextIndexConfig`.** Currently a config change requires drop + recreate. A future migration tool could reindex in place; out of scope here.

8. **Stopword and stemmer customization.** We expose only the built-in English set in v1. Surfacing custom stopword lists or alternate stemmer languages is a small addition once the analyzer registry is in place — design Phase 1's `analyzer.rs` so language is a parameter on `StandardEnglish` (rename to `Standard { language: Language }`) before stabilizing.
