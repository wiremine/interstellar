# Spec 55c — Full-Text Search via Gremlin and GQL

**Status**: Shipped (Layers 0–7 complete, all tests green)
**Depends on**: [`spec-55-fulltext-search.md`](spec-55-fulltext-search.md) Phases 1 & 2 (shipped), [`spec-55b-fulltext-edges.md`](spec-55b-fulltext-edges.md) (shipped)
**Scope**: Single chunk of work spanning the in-process traversal layer, the Gremlin string-parser surface, and the GQL string-parser surface. COW backend only.

---

## 1. Motivation

Spec-55 / spec-55b shipped a complete Tantivy-backed full-text search engine reachable through the Rust fluent API on `CowTraversalSource`:

```rust
g.search_text("body", "graph database", 10)?
g.search_text_query("body", &TextQuery::Phrase { text: ..., slop: 1 }, 10)?
g.search_text_e(...)? / g.search_text_query_e(...)?
```

Neither query language exposes any of this. Users querying a graph through `Graph::execute_script` (Gremlin) or `Graph::gql` (GQL) cannot use the FTS engine at all, and must drop into Rust to do anything more sophisticated than `WHERE p.name CONTAINS 'x'` (which is a full label-scan substring filter, not BM25-ranked text search).

This spec adds first-class FTS support to both query surfaces with parity to the Rust API.

## 2. Non-goals

- **mmap persistence** — still deferred to spec-55 Phase 4.
- **WHERE-side `FULLTEXT(...)` predicate in GQL** — considered and rejected. It would require a compiler rewrite to push the FTS lookup into the seed (otherwise it degrades to a per-row scan), and it cannot surface BM25 scores. Users get the same expressive power through `CALL ... YIELD score WHERE score > x`.
- **Structured-map argument for GQL CALL** (e.g. `{phrase: 'x', slop: 1}`) — considered and deferred. The per-variant procedure names cover every `TextQuery` shape with no parser changes.
- **Bare `CALL` without leading `MATCH` in GQL** — `match_clause` is currently mandatory in `query`. Users prepend `MATCH ()` (the empty pattern). Relaxing this is an orthogonal grammar change with broader implications and is deferred.
- **Fuzzy queries** — not in `TextQuery` today (per `query.rs:7-9` "the enum mirrors only the subset of Tantivy's expressive surface that interstellar guarantees").
- **GQL `CALL` from `WITH` / inside subqueries** beyond what the existing dispatcher already supports.

## 3. Design summary (decisions already locked in)

| Question | Decision |
|---|---|
| **Compiler ↔ FTS bridge** | Port the four `search_text*` methods from `CowTraversalSource` onto `GraphTraversalSource`. Both query-language compilers reach FTS through the same uniform mechanism. Score lives in the traverser sack, exactly as in the COW path. |
| **Gremlin syntax** | Source step + `TextQ` DSL: `g.searchTextV(prop, query, k)` / `g.searchTextE(...)`. Bare-string second arg is sugar for `TextQuery::Match`. Structured queries via `TextQ.match(s) | TextQ.matchAll(s) | TextQ.phrase(s [, slop]) | TextQ.prefix(s) | TextQ.and(...) | TextQ.or(...) | TextQ.not(q)`. Mirrors the existing `TextP` predicate convention. |
| **Gremlin score exposure** | New transform step `__.textScore()` reads `Traverser::get_sack::<f32>()` and emits `Value::Float(score as f64)`. Fulfills the existing doc-comment promise at `traverser.rs:622`. |
| **GQL syntax** | `CALL` procedure + `YIELD`. Zero grammar / AST / parser changes — uses the existing `call_procedure_clause`. One procedure name per `TextQuery` variant for ergonomics; `YIELD node, score` for vertex variants and `YIELD edge, score` for edge variants. |
| **GQL score exposure** | Native — `score` is a yielded column that participates in `WHERE`, `ORDER BY`, `RETURN`. |
| **Spec partition** | Single unified spec (this one) covering all surfaces. |

## 4. API surface

### 4.1 `GraphTraversalSource` (Layer 1)

```rust
impl<'g> GraphTraversalSource<'g> {
    /// Top-k BM25 search over a vertex text index.
    ///
    /// Convenience wrapper around `search_text_query` with `TextQuery::Match`.
    /// Each emitted traverser carries its BM25 score in its sack
    /// (read via `Traverser::get_sack::<f32>()`).
    #[cfg(feature = "full-text")]
    pub fn search_text(
        &self,
        property: &str,
        query: &str,
        k: usize,
    ) -> Result<BoundTraversal<'g, (), Value>, StorageError>;

    /// Structured top-k BM25 search over a vertex text index.
    #[cfg(feature = "full-text")]
    pub fn search_text_query(
        &self,
        property: &str,
        query: &TextQuery,
        k: usize,
    ) -> Result<BoundTraversal<'g, (), Value>, StorageError>;

    /// Edge symmetric variants.
    #[cfg(feature = "full-text")]
    pub fn search_text_e(...) -> Result<BoundTraversal<'g, (), Value>, StorageError>;
    #[cfg(feature = "full-text")]
    pub fn search_text_query_e(...) -> Result<BoundTraversal<'g, (), Value>, StorageError>;
}
```

These mirror the four existing methods on `CowTraversalSource` (`cow.rs:2726–2818`) one-to-one. Each:

1. Looks up the text index from the registry on the underlying `Graph`.
2. Calls `index.search(...)` to get `Vec<TextHit>`.
3. Filters to the appropriate `ElementRef` variant (`as_vertex()` / `as_edge()`).
4. Constructs a `BoundTraversal` seeded with `TraversalSource::VerticesWithTextScore(Vec<(VertexId, f32)>)` or `EdgesWithTextScore(Vec<(EdgeId, f32)>)` — the same source variants used today.

The traverser-sack score plumbing in `traversal/source.rs:3189–3214`, `traversal/streaming.rs:286–314`, and `traversal/step.rs:561–582, 659–680, 973–994` is unchanged; both surfaces reuse it as-is.

**Plumbing resolution**: `GraphSnapshot` is intentionally decoupled from `Graph` (`cow.rs:4010` — only `Arc<GraphState>` + `Arc<StringInterner>`), so a `GraphTraversalSource` constructed via `from_snapshot` cannot reach the live text-index registry, which lives on `Graph` (`text_indexes_vertex` / `text_indexes_edge` at `cow.rs:243-247`).

Resolution: `GraphTraversalSource` gains an **optional** `Arc<Graph>` handle.

- New constructor: `GraphTraversalSource::from_snapshot_with_graph(snapshot, Arc<Graph>)`.
- Existing `from_snapshot` still works; it sets the handle to `None`.
- The four FTS methods return `Err("FTS requires a live Graph handle; construct via Graph::execute_script/Graph::gql/Graph::query, not GraphSnapshot::gremlin()")` when the handle is absent. Otherwise they look up the index, run search, and seed a `BoundTraversal` with `TraversalSource::VerticesWithTextScore` / `EdgesWithTextScore`.
- Entry points (`Graph::execute_script`, `Graph::execute_script_with_context`, `Graph::query`, `Graph::gql`, `Graph::gql_with_params`) switch to the new constructor.
- `GraphSnapshot::gremlin()` keeps its current signature; FTS via detached snapshots is documented as unsupported. This matches the existing precedent at `compiler.rs:3195-3203` where mutations are also unavailable through `GraphTraversalSource`.

Strictly additive change; no breaking signatures.

### 4.2 Gremlin

```groovy
// Bare-string query (sugar for TextQ.match)
g.searchTextV('body', 'graph database', 10)
g.searchTextE('note', 'consensus', 5)

// Structured queries via TextQ DSL
g.searchTextV('body', TextQ.matchAll('graph database'), 10)
g.searchTextV('body', TextQ.phrase('replicated logs'), 10)
g.searchTextV('body', TextQ.phrase('replicated logs', 2), 10)   // slop=2
g.searchTextV('body', TextQ.prefix('grap'), 10)
g.searchTextV(
    'body',
    TextQ.and(TextQ.match('raft'), TextQ.not(TextQ.match('paxos'))),
    10
)

// Composes with regular steps (vertex traversal, score in sack)
g.searchTextV('body', 'consensus', 10).hasLabel('article').values('title')

// Score extraction
g.searchTextV('body', 'consensus', 10)
 .project('vertex', 'score')
   .by(__.identity())
   .by(__.textScore())
```

All `g.searchText*` source steps return a vertex- (or edge-) typed traversal that composes with every existing step. The score lives in the sack; downstream code reaches it via `__.textScore()`.

`textScore()` on a traverser without a sack value emits nothing (filter-out semantics, matching `__.id()` on a value traverser). This is intentional and tested.

### 4.3 GQL

```cypher
-- Bare lookup, ordered by score
MATCH ()
CALL interstellar.searchTextV('body', 'graph database', 10) YIELD node, score
RETURN node, score
ORDER BY score DESC

-- Aliased yield
MATCH ()
CALL interstellar.searchTextV('body', 'graph database', 10) YIELD node AS n, score AS s
WHERE s > 0.3
RETURN n.title AS title, s

-- Phrase / prefix / matchAll variants
CALL interstellar.searchTextPhraseV('body', 'replicated logs', 10) YIELD node, score
CALL interstellar.searchTextPhraseV('body', 'replicated logs', 10, 2) YIELD node, score   -- slop=2
CALL interstellar.searchTextPrefixV('body', 'grap', 10) YIELD node, score
CALL interstellar.searchTextAllV('body', 'graph database', 10) YIELD node, score

-- Edge symmetric variants
CALL interstellar.searchTextE('note', 'consensus', 10) YIELD edge, score
CALL interstellar.searchTextPhraseE('note', 'replicated logs', 10) YIELD edge, score

-- Compose with MATCH after CALL (cross-join semantics)
MATCH ()
CALL interstellar.searchTextV('body', 'consensus', 10) YIELD node, score
MATCH (node)-[:CITES]->(other)
RETURN other, score
```

Procedure name → `TextQuery` variant mapping:

| Procedure | `TextQuery` |
|---|---|
| `interstellar.searchTextV(prop, q, k)` | `Match(q)` |
| `interstellar.searchTextAllV(prop, q, k)` | `MatchAll(q)` |
| `interstellar.searchTextPhraseV(prop, q, k)` | `Phrase { text: q, slop: 0 }` |
| `interstellar.searchTextPhraseV(prop, q, k, slop)` | `Phrase { text: q, slop }` |
| `interstellar.searchTextPrefixV(prop, q, k)` | `Prefix(q)` |
| `interstellar.searchText{,All,Phrase,Prefix}E(...)` | edge symmetric set |

Boolean (`And`/`Or`/`Not`) compositions are not exposed via dedicated procedures in this spec — they are reachable by chaining multiple `CALL`s with intersection/union semantics over `MATCH`/`WITH`. If a single-procedure boolean DSL becomes important, the structured-map variant noted in §2 is the upgrade path.

YIELD shape is fixed:
- Vertex variants yield `node : Value::Vertex(VertexId)` and `score : Value::Float(f64)`.
- Edge variants yield `edge : Value::Edge(EdgeId)` and `score : Value::Float(f64)`.

The existing `bind_yield` helper (`compiler_legacy.rs:2630`) handles default and aliased names.

## 5. Implementation layers

### Layer 0 — Spec authoring
This document.

### Layer 1 — Bridge: `search_text*` on `GraphTraversalSource`

**Files**:
- `interstellar/src/traversal/source.rs` — four new methods on `GraphTraversalSource<'g>`, mirroring `cow.rs:2726–2818`. All gated `#[cfg(feature = "full-text")]`.
- Possibly `from_snapshot` constructor signature, if the registry is unreachable from snapshot state alone (verified at start of layer).

**Tests**: `interstellar/src/traversal/source.rs::tests` — 6 unit tests:
- `search_text_returns_vertices_in_score_order`
- `search_text_e_returns_edges_in_score_order`
- `search_text_query_phrase_works`
- `search_text_query_e_phrase_works`
- `search_text_unknown_property_errors`
- `search_text_with_zero_k_returns_empty`

### Layer 2 — Gremlin grammar

**File**: `interstellar/src/gremlin/grammar.pest`

Changes:
1. Add `"TextQ"` to the `keyword` list (line 59).
2. Extend `source_step` (line 72): prepend `search_text_v_step | search_text_e_step` (more specific first, per pest greedy semantics).
3. New rules:
   ```pest
   search_text_v_step = { "searchTextV" ~ "(" ~ string ~ "," ~ text_query_arg ~ "," ~ integer ~ ")" }
   search_text_e_step = { "searchTextE" ~ "(" ~ string ~ "," ~ text_query_arg ~ "," ~ integer ~ ")" }

   text_query_arg = { text_q_dsl | string }
   text_q_dsl = { tq_match | tq_match_all | tq_phrase | tq_prefix | tq_and | tq_or | tq_not }
   tq_match     = { "TextQ" ~ "." ~ "match"    ~ "(" ~ string ~ ")" }
   tq_match_all = { "TextQ" ~ "." ~ "matchAll" ~ "(" ~ string ~ ")" }
   tq_phrase    = { "TextQ" ~ "." ~ "phrase"   ~ "(" ~ string ~ ("," ~ integer)? ~ ")" }
   tq_prefix    = { "TextQ" ~ "." ~ "prefix"   ~ "(" ~ string ~ ")" }
   tq_and       = { "TextQ" ~ "." ~ "and"      ~ "(" ~ text_query_arg ~ ("," ~ text_query_arg)* ~ ")" }
   tq_or        = { "TextQ" ~ "." ~ "or"       ~ "(" ~ text_query_arg ~ ("," ~ text_query_arg)* ~ ")" }
   tq_not       = { "TextQ" ~ "." ~ "not"      ~ "(" ~ text_query_arg ~ ")" }
   ```
4. New `text_score_step = { "textScore" ~ "(" ~ ")" }` and wire into the step list (alongside `id_step`, `label_step`).

Pest grammar has no `#[cfg]` mechanism. Rules added unconditionally. Non-`full-text` builds reach them only if a query contains them, in which case the **parser/compiler arms** (which are gated) return a feature-disabled error.

### Layer 3 — Gremlin AST + parser

**Files**:
- `interstellar/src/gremlin/ast.rs`:
  - Two new `SourceStep` variants (`#[cfg(feature = "full-text")]`): `SearchTextV { property: String, query: TextQueryAst, k: u64, span: Span }`, `SearchTextE { ... }`.
  - One new `Step` variant: `TextScore { span }`.
  - One new sibling enum `TextQueryAst` mirroring `TextQuery` 1:1.
- `interstellar/src/gremlin/parser.rs`:
  - Extend `build_source` (line 224) with two new arms.
  - Extend the step builder with a `Rule::text_score_step` arm.
  - New helper `build_text_query_arg(pair) -> Result<TextQueryAst, ParseError>` recursing through the `tq_*` rules. Pattern mirrors `build_p_method` (`parser.rs:1188–1226`).
  - New helpers `build_search_text_v_args` / `build_search_text_e_args`.

**Tests** (`interstellar/src/gremlin/tests.rs`, `#[cfg(feature = "full-text")]`):
- `test_search_text_v_bare_string`
- `test_search_text_v_phrase`
- `test_search_text_v_phrase_with_slop`
- `test_search_text_v_prefix`
- `test_search_text_v_match_all`
- `test_search_text_v_boolean_nested`
- `test_search_text_e_bare_string`
- `test_search_text_e_phrase`
- `test_text_score_step_parses`

### Layer 4 — Gremlin compiler + `textScore()` step

**Files**:
- `interstellar/src/traversal/transform/text_score.rs` (new) — `TextScoreStep` reads `Traverser::get_sack::<f32>()` and emits `Value::Float(score as f64)`. Missing-sack emits nothing.
- `interstellar/src/traversal/transform/mod.rs` — register the new step.
- `interstellar/src/gremlin/compiler.rs`:
  - Extend `compile_source` (line 115) with two FTS arms calling the Layer-1 `g.search_text*` methods. Errors map to `CompileError::InvalidArguments { step, message }`.
  - Extend the step compiler with a `Step::TextScore` arm.
  - New helper `compile_text_query(ast: &TextQueryAst) -> TextQuery`.

**Tests** (in `tests.rs`, `#[cfg(feature = "full-text")]`):
- `test_compile_search_text_v_chains_with_has_label` — full execute_script
- `test_compile_search_text_v_phrase_returns_top_k`
- `test_compile_search_text_e_returns_edges`
- `test_compile_text_score_emits_score_via_project`
- `test_compile_text_score_no_sack_emits_nothing`
- `test_compile_search_text_unknown_property_errors`

### Layer 5 — GQL CALL procedure dispatch

**Files**:
- `interstellar/src/gql/compiler_legacy.rs` — extend `dispatch_procedure` (`:2429`) with eight new arms (vertex × {Match, MatchAll, Phrase, Prefix} + edge symmetric). All gated `#[cfg(feature = "full-text")]`.
  - Each arm validates argument count + types via existing helpers (`extract_string_arg`, `extract_int_arg`).
  - Each arm calls the Layer-1 `GraphTraversalSource::search_text*` method, iterates the resulting traversal pulling `Value::Vertex`/`Value::Edge` and `Traverser::get_sack::<f32>()`, emits one row per hit via `bind_yield("node"|"edge", ...)` + `bind_yield("score", Value::Float(score as f64))`.
- `interstellar/src/gql/error.rs` — update `UnknownProcedure` `#[error]` message to list new procedure names (cosmetic).

**Score-row plumbing**: rather than touching the global `traverser_to_row` (`compiler_legacy.rs:209`), the FTS dispatch arms walk the traverser iterator manually. Local change, no risk of regressing other procedures or MATCH-side row construction.

**Tests** (`interstellar/tests/gql/text_search.rs`, new file, `#[cfg(all(feature = "gql", feature = "full-text"))]`):
- `test_search_text_v_basic`
- `test_search_text_v_aliased_yield`
- `test_search_text_v_phrase`
- `test_search_text_v_phrase_with_slop`
- `test_search_text_v_prefix`
- `test_search_text_v_match_all`
- `test_search_text_e_basic`
- `test_search_text_e_phrase`
- `test_search_text_v_chains_with_match` — proves cross-join with subsequent MATCH
- `test_search_text_v_filter_by_score` — `WHERE score > x`
- `test_search_text_v_unknown_property_errors`
- `test_search_text_v_argument_count_errors`

### Layer 6 — Documentation + example

**Files**:
- `interstellar/docs/guides/full-text-search.md` — append "Searching from Gremlin" and "Searching from GQL" sections covering every variant.
- `interstellar/docs/reference/feature-flags.md` — note that `gremlin + full-text` and `gql + full-text` unlock the new surfaces.
- `interstellar/examples/quickstart_text_search.rs` — append two final sections demonstrating Gremlin script and GQL query usage.

### Layer 7 — Final gate

- `cargo test -p interstellar --features "full-text gremlin gql" --lib --tests` — full suite, ~3163 tests.
- `cargo test -p interstellar --features full-text --lib --tests` — non-language build still green.
- `cargo test -p interstellar --features gremlin --lib --tests` — Gremlin without FTS still green; FTS query produces feature-error.
- `cargo test -p interstellar --features gql --lib --tests` — same for GQL.
- `cargo clippy --features "full-text gremlin gql" -p interstellar --lib --tests` — zero warnings on touched files.
- `cargo check -p interstellar` — default features clean.
- `cargo run --example quickstart_text_search --features "full-text gremlin gql"` — runs end-to-end.

## 6. Risks and open questions

1. **`GraphTraversalSource` ↔ `Graph` plumbing** — RESOLVED in design phase. Optional `Arc<Graph>` handle on `GraphTraversalSource`; entry points use `from_snapshot_with_graph`; FTS via detached snapshot returns clear error. See §4.1.
2. **Score precision** — Tantivy emits `f32`; `Value::Float` is `f64`. Promotion is lossless; documented in the user-facing guide.
3. **GQL bare `CALL`** — `match_clause` is mandatory; users prepend `MATCH ()`. Documented; grammar relaxation deferred (per user decision).
4. **Pest grammar without cfg gates** — FTS rules exist even when feature is off. Acceptable; gated parser/compiler arms provide a friendly error.
5. **`textScore()` on non-FTS traversers** — emits nothing (filter-out). Matches `__.id()` on value traversers; tested explicitly.

## 7. Test count delta

- Layer 1: +6 unit tests on `GraphTraversalSource`
- Layer 3: +9 Gremlin parser tests
- Layer 4: +6 Gremlin compiler / textScore tests
- Layer 5: +12 GQL integration tests
- **Total**: ~33 new tests (3131 baseline → ~3164)

## 8. File touch list

| Layer | File | Status |
|---|---|---|
| 0 | `specs/spec-55c-fulltext-query-languages.md` | new |
| 1 | `interstellar/src/traversal/source.rs` | edit |
| 4 | `interstellar/src/traversal/transform/text_score.rs` | new |
| 4 | `interstellar/src/traversal/transform/mod.rs` | edit |
| 2 | `interstellar/src/gremlin/grammar.pest` | edit |
| 3 | `interstellar/src/gremlin/ast.rs` | edit |
| 3 | `interstellar/src/gremlin/parser.rs` | edit |
| 4 | `interstellar/src/gremlin/compiler.rs` | edit |
| 3,4 | `interstellar/src/gremlin/tests.rs` | edit |
| 5 | `interstellar/src/gql/compiler_legacy.rs` | edit |
| 5 | `interstellar/src/gql/error.rs` | edit |
| 5 | `interstellar/tests/gql/text_search.rs` | new |
| 6 | `interstellar/docs/guides/full-text-search.md` | edit |
| 6 | `interstellar/examples/quickstart_text_search.rs` | edit |
