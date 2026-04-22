# Full-Text Search

Interstellar ships an optional full-text search subsystem powered by [Tantivy](https://github.com/quickwit-oss/tantivy). It lets you register one or more text indexes per `Graph` — on vertex properties, edge properties, or both — ingest string-valued properties as documents, and seed Gremlin traversals with the top-`k` matches of a free-text or structured query.

This guide is enabled by the `full-text` Cargo feature:

```toml
[dependencies]
interstellar = { version = "0.1", features = ["full-text"] }
```

For a runnable end-to-end walkthrough, see [`examples/quickstart_text_search.rs`](../../examples/quickstart_text_search.rs):

```bash
cargo run --example quickstart_text_search --features full-text
```

---

## Mental model

A **text index** is a property-scoped, BM25-ranked inverted index that lives alongside the graph state. Every text index is bound to a single **element type** at construction — either *vertex* or *edge* — and indexes only that element type's properties.

- **Scope**: one index per `(element_type, property)` pair. Vertex and edge indexes are stored independently and queried through separate `_v` / `_e` API surfaces.
- **Property-name namespace**: globally unique across both element types. You cannot register an edge index for property `body` if a vertex index for `body` already exists, and vice versa. This keeps the user-visible mental model "one property name → at most one text index".
- **Storage**: in-memory `RamDirectory` for the COW backend. Persistence to the mmap backend is planned (see `specs/spec-55-fulltext-search.md`, Phase 4).
- **Lifecycle**: indexes are runtime objects on the live `Graph` (not part of `GraphSchema`). They are created, dropped, and queried through methods on `Graph` and on the `CowTraversalSource` returned by `graph.gremlin(...)`.
- **Mutation hooks**: every `add_vertex` / `set_vertex_property` / `remove_vertex` keeps every registered vertex index in sync, and every `add_edge` / `set_edge_property` / `remove_edge` keeps every registered edge index in sync. Removing a vertex also cascades into edge text indexes for any incident edges deleted alongside it. There is no manual "reindex" step.
- **Search results**: top-`k` matching `VertexId`s or `EdgeId`s, sorted by descending BM25 score. Each emitted traverser carries its score in its sack (`Traverser::get_sack::<f32>()`), so downstream steps can read it.

---

## Lifecycle

### Create an index

The two creation methods mirror the project's wider `_e` suffix convention used elsewhere in the API (`out_e`, `in_e`, etc.):

```rust
use std::sync::Arc;
use interstellar::storage::Graph;
use interstellar::storage::text::TextIndexConfig;

let graph = Arc::new(Graph::new());

// Vertex text index on the `bio` property.
graph
    .create_text_index_v("bio", TextIndexConfig::default())
    .expect("vertex index creation failed");

// Edge text index on the `body` property of (e.g.) `comment` edges.
graph
    .create_text_index_e("body", TextIndexConfig::default())
    .expect("edge index creation failed");
```

`TextIndexConfig::default()` selects the standard analyzer (lowercase + unicode tokenizer) and stores positions, which is what phrase queries need. See the `TextIndexConfig` struct in `interstellar::storage::text` for analyzer choices and storage tuning.

If the property already has a string value on existing elements, `create_text_index_v` / `create_text_index_e` **back-fills** them and commits before returning. The index is searchable as soon as the call succeeds.

> **Property-name uniqueness.** Both `create_text_index_v` and `create_text_index_e` reject the call if the property name is already registered on **either** map. This is the global-uniqueness invariant; it catches accidental re-use across element types.

### Inspect indexes

The lookup / management API is mirrored across both element types:

```rust
// Vertex side
graph.has_text_index_v("bio");      // bool
graph.list_text_indexes_v();        // Vec<String>
graph.text_index_count_v();         // usize
graph.text_index_v("bio");          // Option<Arc<dyn TextIndex>>

// Edge side
graph.has_text_index_e("body");     // bool
graph.list_text_indexes_e();        // Vec<String>
graph.text_index_count_e();         // usize
graph.text_index_e("body");         // Option<Arc<dyn TextIndex>>
```

### Drop an index

```rust
graph.drop_text_index_v("bio").expect("no such vertex index");
graph.drop_text_index_e("body").expect("no such edge index");
```

Returns an error if the property has no registered index for that element type.

---

## Searching

The four source steps live on the COW gremlin source (`CowTraversalSource`):

```rust
let g = graph.gremlin(Arc::clone(&graph));

// Vertex search: free-text query parsed by the index's analyzer
// (OR-of-terms by default).
let articles = g
    .search_text("bio", "raft consensus", 10)?
    .has_label("article")
    .values("title")
    .to_value_list();

// Edge search: identical mental model, returns an edge-typed traversal.
let comments = g
    .search_text_e("body", "raft consensus", 10)?
    .has_label("comment")
    .to_value_list();
```

Both vertex methods return `Result<CowBoundTraversal<…, VertexMarker>, TextIndexError>`. The edge variants return `Result<CowBoundTraversal<…, EdgeMarker>, TextIndexError>`. The error case is "no [vertex|edge] text index registered for `<property>`" — anything else is wrapped from Tantivy.

### Structured queries

`search_text_query` (vertices) and `search_text_query_e` (edges) accept a pre-built [`TextQuery`] for phrase, prefix, boolean, and fuzzy queries:

```rust
use interstellar::storage::text::TextQuery;

// Phrase with strict adjacency (slop = 0)
let phrase = TextQuery::Phrase {
    text: "replicated logs".to_string(),
    slop: 0,
};
let vertex_hits = g.search_text_query("bio", &phrase, 10)?;
let edge_hits   = g.search_text_query_e("body", &phrase, 10)?;

// Boolean: must contain "raft", must not contain "paxos"
let q = TextQuery::all([
    TextQuery::Match("raft".into()),
    TextQuery::not(TextQuery::Match("paxos".into())),
]);
let strong_raft_articles = g.search_text_query("bio", &q, 10)?;
```

The full `TextQuery` enum is documented in `interstellar::storage::text::TextQuery`.

### Composing with traversal steps

Both source steps compose with the rest of the Gremlin API:

```rust
// Articles about consensus, then their authors
let authors = g
    .search_text("bio", "consensus", 50)?
    .has_label("article")
    .out_label("authored_by")
    .dedup()
    .values("name")
    .to_value_list();

// Edges that mention "shipping it", then their target vertices
let recipients = g
    .search_text_e("body", "shipping it", 50)?
    .in_v()
    .dedup()
    .to_value_list();
```

### Reading BM25 scores

Each emitted traverser carries its BM25 score in its sack. The user-facing terminal methods (`to_value_list`, `to_list`, etc.) discard sacks, so for score-aware pipelines you typically use the score implicitly through ordering: Tantivy returns hits sorted by descending relevance, and the source step preserves that order through subsequent steps that don't reorder traversers (e.g. `has_label`, `values`).

For programmatic access to the score, query the index directly:

```rust
use interstellar::storage::text::{ElementRef, TextQuery};

let index = graph.text_index_v("bio").expect("registered");
let hits = index.search(&TextQuery::Match("raft".into()), 10)?;
for hit in &hits {
    // hit.element is an ElementRef::Vertex(...) (or ::Edge(...) for edge indexes).
    match hit.element {
        ElementRef::Vertex(vid) => println!("vertex={vid:?} score={}", hit.score),
        ElementRef::Edge(eid)   => println!("edge={eid:?} score={}", hit.score),
    }
}
```

---

## Mutation semantics

The COW backend wires the corresponding mutation paths into every registered text index:

### Vertex mutations → vertex indexes

| Operation | Behaviour |
|-----------|-----------|
| `Graph::add_vertex` | For each registered vertex text index whose property appears in the new vertex's properties as `Value::String`, call `upsert(id, &s)` then `commit()`. Non-string values are skipped. |
| `Graph::set_vertex_property` | If a vertex text index exists for the property: upsert on `Value::String`, delete on any non-string value (so changing a property's type purges stale tokens). Always commits. |
| `Graph::remove_vertex` | Delete the vertex from every registered vertex text index, then commit. Cascade-removed incident edges are also removed from every registered edge text index. |

### Edge mutations → edge indexes

| Operation | Behaviour |
|-----------|-----------|
| `Graph::add_edge` | For each registered edge text index whose property appears in the new edge's properties as `Value::String`, call `upsert(id, &s)` then `commit()`. Non-string values are skipped. |
| `Graph::set_edge_property` | If an edge text index exists for the property: upsert on `Value::String`, delete on any non-string value. Always commits. |
| `Graph::remove_edge` | Delete the edge from every registered edge text index, then commit. |

**Errors**: `add_vertex` and `remove_vertex` have infallible signatures and silently swallow text-index errors (matching the existing behaviour for unique indexes). `add_edge`, `set_vertex_property`, `set_edge_property` return `Result`, so text-index errors are surfaced as `StorageError::IndexError(...)`. `remove_edge` swallows text-index errors (canonical state has already changed).

**Performance note**: each mutation triggers a Tantivy commit, which is durable but not free. For large bulk loads, prefer creating the text index *after* the bulk load — the back-fill path commits once at the end. Batched-commit mutation paths are tracked for a future phase.

---

## Searching from query languages

Spec-55c surfaces the same FTS engine through both Gremlin and GQL. The Rust API remains the most expressive (it alone supports compound `And/Or/Not` queries), but the language surfaces are sufficient for the common cases.

### Gremlin

Both source steps and the `__.textScore()` step are gated on the `full-text` feature. Both source steps require a `Graph`-bound traversal source — the snapshot-only `g.snapshot()` path returns a clear compile error.

```text
// Bare-string sugar: second arg desugars to TextQ.match(...)
g.searchTextV('body', 'raft consensus', 10).hasLabel('article').values('title')

// Edge-side mirror
g.searchTextE('note', 'hello back', 5).inV().values('name')

// Read the BM25 score (Float)
g.searchTextV('body', 'raft', 5).textScore()

// Structured TextQ DSL — supports every leaf TextQuery variant plus And/Or/Not
g.searchTextV('body', TextQ.matchAll('distributed consensus'), 10)
g.searchTextV('body', TextQ.phrase('quick brown fox'), 5)
g.searchTextV('body', TextQ.prefix('consen'), 5)
g.searchTextV(
  'body',
  TextQ.and(
    TextQ.match('raft'),
    TextQ.or(TextQ.prefix('paxos'), TextQ.not(TextQ.match('byzantine')))
  ),
  10
)
```

`__.textScore()` reads the `f32` score the source step attached to the traverser sack and emits it as `Value::Float`. If a traverser arrives at `textScore()` without a sack (e.g. it came from a non-FTS source), the step emits `Value::Null` rather than aborting the pipeline.

### GQL (`CALL` procedures)

GQL surfaces the four leaf query shapes over both vertices and edges as eight `CALL` procedures. Compound queries (`And/Or/Not`) are intentionally **not** exposed here — use Gremlin's `TextQ.*` DSL or the Rust API when you need them.

| Procedure                                | Backing `TextQuery`           |
| ---------------------------------------- | ----------------------------- |
| `interstellar.searchTextV`               | `Match(query)`                |
| `interstellar.searchTextAllV`            | `MatchAll(query)`             |
| `interstellar.searchTextPhraseV`         | `Phrase { text: query, .. }`  |
| `interstellar.searchTextPrefixV`         | `Prefix(query)`               |
| `interstellar.searchTextE`               | edge-side `Match(query)`      |
| `interstellar.searchTextAllE`            | edge-side `MatchAll(query)`   |
| `interstellar.searchTextPhraseE`         | edge-side `Phrase { .. }`     |
| `interstellar.searchTextPrefixE`         | edge-side `Prefix(query)`     |

Each takes `(property STRING, query STRING, k INT)` and exposes three YIELD aliases:

| YIELD alias | Type / shape                                   |
| ----------- | ---------------------------------------------- |
| `elem`      | `Value::Map` — fully materialized props        |
| `elemId`    | `Value::Vertex(VertexId)` / `Value::Edge(EdgeId)` |
| `score`     | `Value::Float` — BM25, descending              |

`elem` materialization is lazy: when the YIELD clause does not name `elem`, the dispatcher skips the per-row property lookup, so id-only queries pay zero materialization cost. Hits are returned in descending score order, matching the underlying Tantivy ranking.

GQL requires every query to begin with `MATCH`, so to call an FTS procedure you anchor against a single row first. Either bind a known vertex by id, or use any single-row pattern:

```sql
-- Anchor on one vertex by id, then call the procedure exactly once.
MATCH (anchor) WHERE id(anchor) = 0
CALL interstellar.searchTextV('body', 'raft', 5)
YIELD elemId, score
RETURN elemId, score
```

```sql
-- Materialize the full element record.
MATCH (anchor) WHERE id(anchor) = 0
CALL interstellar.searchTextPhraseV('body', 'quick brown fox', 5)
YIELD elem
RETURN elem
```

```sql
-- Edge-side prefix search.
MATCH (anchor) WHERE id(anchor) = 0
CALL interstellar.searchTextPrefixE('note', 'gree', 5)
YIELD elemId, score
RETURN elemId, score
```

The procedures **must** be called via the graph-bound entry point ([`Graph::gql`](https://docs.rs/interstellar/latest/interstellar/storage/struct.Graph.html#method.gql) / [`Graph::gql_with_params`](https://docs.rs/interstellar/latest/interstellar/storage/struct.Graph.html#method.gql_with_params)). The snapshot-only [`gql::compile`](https://docs.rs/interstellar/latest/interstellar/gql/fn.compile.html) entry point intentionally does not pass a `Graph` handle, so calling an FTS procedure through it returns an actionable `ProcedureArgumentError` rather than silently producing empty results.

---

## Limitations and roadmap

The current release implements Phase 2 of `specs/spec-55-fulltext-search.md`, the edge-side extension `specs/spec-55b-fulltext-edges.md`, and the Gremlin / GQL query-language surface defined in `specs/spec-55c-fulltext-query-languages.md`. Items intentionally out of scope:

- **GQL compound queries** (`And/Or/Not`) — not exposed through `CALL` procedures. Use Gremlin's `TextQ.*` DSL or the Rust API.
- **Bare GQL `CALL` without a leading `MATCH`** — deferred. Anchor against a single row (e.g. `MATCH (n) WHERE id(n) = 0 ...`).
- **`WHERE FULLTEXT(...)` predicate** — rejected; predicates can't surface the BM25 score.
- **No persistence** — text indexes live in `RamDirectory` and are lost when the `Graph` is dropped. The mmap backend gains text-index WAL entries in Phase 4.
- **No `search_text_from`** (more-like-this). Tantivy supports it; we have not exposed it through the `TextIndex` trait yet.
- **Per-mutation commits** — see the performance note above.

---

## See also

- Spec (vertices): `specs/spec-55-fulltext-search.md`
- Spec (edges): `specs/spec-55b-fulltext-edges.md`
- Spec (query languages): `specs/spec-55c-fulltext-query-languages.md`
- API surface: [`interstellar::storage::text`](https://docs.rs/interstellar/latest/interstellar/storage/text/index.html)
- Example: [`examples/quickstart_text_search.rs`](../../examples/quickstart_text_search.rs)
- Feature flag reference: [Feature Flags](../reference/feature-flags.md)
