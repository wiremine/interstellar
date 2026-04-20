# Interstellar — Feature Proposals (Reference)

> **Status:** Brainstorm / menu of options. Not approved work.
> **See also:** `todos/todo.md`, `specs/implementation.md`, `guiding-documents/`, `specs/archive/`

This document is a reference catalogue of features worth considering for Interstellar. It is intentionally a *menu*, not a roadmap — each item should graduate to its own `spec-NN-*.md` before implementation begins. Items already covered by existing specs/plans/todos are listed under [Out of Scope / Already Planned](#out-of-scope--already-planned) so they are not re-proposed.

---

## Context

### What currently exists

- **Storage**: In-memory COW backend (`storage/cow.rs`), persistent mmap backend with WAL + recovery (`storage/mmap/`), COW-over-mmap hybrid (`storage/cow_mmap.rs`). Unified `GraphStorage` trait with optional indexed lookups and string interning.
- **Traversal engine**: Full Gremlin-style fluent pipeline — navigation (`out/in_/both/...E/...V`), filter (`has`, `where_`, `dedup`, `simple_path`, `range`, ...), transform (`values`, `value_map`, `select`, `project`, `path`, `count`, `sum/min/max/mean`, `fold`, `unfold`, `order`, `group`, ...), branch/loop (`repeat`, `until`, `times`, `emit`, `branch`, `choose`, `optional`, `local`, `union`, `coalesce`), aggregate/side-effect (`aggregate`, `store`, `cap`, `side_effect`, `inject`), mutation (`add_v`/`add_e`/`property`/`drop`), anonymous traversals (`__::`), predicates (`P::eq/lt/within/...`).
- **Query languages**: Gremlin text parser (pest grammar + compiler), GQL/Cypher-like parser & compiler with `MATCH/WHERE/RETURN/ORDER BY/LIMIT/SKIP/CREATE/SET/DELETE/MERGE/DETACH DELETE`, variable-length paths, aggregates.
- **Schema & indexes**: Schema builder + validation, B+ tree property index, unique hash index.
- **Serialization**: GraphSON import/export.
- **Bindings**: CLI + REPL, Node.js native (napi-rs), WASM.
- **Quality**: Kani formal-verification proofs, proptest, Criterion benches, integration tests, examples (marvel, nba, graphson, ...).

### Notable empty/stub modules

- `interstellar/src/algorithms/mod.rs` — empty placeholder despite being advertised in `AGENTS.md` and `guiding-documents/algorithms.md`.
- GQL `compiler_legacy.rs` — mid-migration into the new `compiler/` module.
- Rhai Gremlin scripting — partial (per `todos/todo.md`).

---

## Tier 1 — High leverage, fills obvious gaps

### 1. Graph Algorithms Library

Ship a real `algorithms/` module to back the empty placeholder.

- **Traversal**: BFS, DFS, bidirectional BFS, iterative deepening
- **Pathfinding**: shortest path (unweighted), Dijkstra, A*, all-pairs shortest paths, k-shortest paths (Yen's)
- **Centrality**: degree, betweenness, closeness, eigenvector, PageRank, personalized PageRank
- **Community**: connected components (weak/strong), Louvain, label propagation, triangle counting
- **Similarity**: Jaccard, cosine, optionally node2vec embeddings
- **Structure**: clustering coefficient, k-core decomposition, MST

Expose as a Rust API and as Gremlin/GQL `CALL` procedures so they're usable from every binding. Pair with `benches/algorithms.rs`.

### 2. Bolt-Compatible Wire Protocol (Server Mode)

Today Interstellar is library-only — external tools cannot connect. Implementing Bolt v4/v5 unlocks Neo4j Browser, `cypher-shell`, and the full set of official Neo4j drivers (Python, Java, Go, JS, .NET, Rust). Enables hosting Interstellar as an `interstellar-server` daemon.

**Lower-effort alternative:** HTTP+JSON query endpoint (Neo4j-style HTTP API). Also satisfies REST-API users and is a prerequisite for the planned web UI / GraphQL surfaces.

### 3. Real Multi-Statement Transactions

`todos/todo.md` lists "Transactions" as a single bullet. Worth fleshing out:

- `BEGIN` / `COMMIT` / `ROLLBACK` in GQL and Gremlin
- Transaction handles in Rust / Node / WASM APIs
- Read-your-writes semantics inside a transaction
- Savepoints
- Coordinates with the planned MVCC/OCC work (`spec-40`)

### 4. Vector / Embedding Index (kNN)

Now baseline for graph DBs (Neo4j 5, Memgraph, ArangoDB all added it). Mixes graph traversal with semantic similarity.

- HNSW or IVF-PQ index over a `Vec<f32>` property
- New step `search_vector(prop, query, k)` and GQL `VECTOR SEARCH` clause
- Distance metrics: cosine, L2, dot
- Composes with existing filter/traversal (e.g. "find similar nodes, then expand 2 hops")

### 5. Change Data Capture (CDC) Stream

Distinct from `spec-52` (reactive query subscriptions): a **raw mutation log** consumer.

- Tail the WAL as a typed event stream (`VertexAdded`, `EdgeRemoved`, `PropertyUpdated`, ...)
- Pluggable sinks: file, channel, Kafka adapter, NDJSON over websocket
- Foundation for replication, audit logs, search-index sync, materialized views

---

## Tier 2 — Strong graph-native differentiators

### 6. Property Graph Constraints

Schema exists but most constraints don't.

- `UNIQUE` (partially via unique index)
- `EXISTS` / `NOT NULL`
- `TYPE` (Int / Float / String / ...)
- Edge cardinality (`1:1`, `1:N`, `N:M`)
- Range / regex / enum value constraints
- Surfaced through schema builder and GQL `CREATE CONSTRAINT` DDL

### 7. Query Result Caching & Materialized Views

- Plan-level cache keyed by `(query, params, snapshot epoch)`
- Optional materialized subgraphs / views refreshed via CDC (#5)
- Big win once the planner exists; pairs naturally with #5

### 8. Graph Sampling & Sketching

- Random walk, forest fire, snowball sampling
- HyperLogLog for distinct counts; Count-Min Sketch for frequencies
- Bloom-filter-backed `has_label` / `has_property` fast paths

### 9. Bulk Loader / Importer

A first-class offline ingest path for large datasets.

- CSV / Parquet / JSONL bulk import
- Sorted-key build of mmap arenas (skip WAL during initial load)
- Parallel chunked ingest with progress reporting
- CLI: `interstellar import ./data.parquet --as-vertices Person --id-column id`
- Complements GraphSON, which is unsuitable for billions of rows

### 10. Geospatial Index & Predicates

- `Point` / `Polygon` `Value` variants
- R-tree or S2 cell index
- Predicates: `within_distance`, `intersects`, `contained_by`
- GQL: `WHERE point.distance(p, $center) < 5km`
- Common in real workloads (genealogy, logistics, social)

---

## Tier 3 — Operational maturity

### 11. Observability: Metrics & Tracing

- `tracing` spans on each traversal step + storage operation
- Prometheus metrics (in server mode): query latency histograms, cache hit rates, WAL throughput, mmap page faults
- Structured logging with query ID correlation

### 12. Backup, Snapshot & Restore

- Online consistent snapshot (uses existing COW snapshot machinery)
- `interstellar backup ./db ./backup.tar` / `interstellar restore`
- Incremental backups via WAL shipping
- Point-in-time recovery
- Pairs with replication (#13)

### 13. Replication (Single-Leader)

Stop short of full distribution; ship leader→follower(s) replication via WAL streaming. Enables HA reads and is a natural intermediate step before any sharded/clustered design.

### 14. Storage Compaction & Defrag Tooling

The mmap backend has a freelist; over time files fragment.

- Online compaction (background thread)
- Offline `interstellar compact ./db`
- `interstellar stats ./db` (live/dead bytes, fragmentation %, index health)

### 15. Authentication, Authorization, Audit

Required the moment server mode (#2) exists.

- Users / roles / API tokens
- Per-label / per-property RBAC ("can read but not write `:Person.ssn`")
- Audit log integrated with CDC (#5)
- TLS for the wire protocol

---

## Tier 4 — Developer & ecosystem reach

### 16. JDBC / ODBC Driver

Lets BI tools (Tableau, Metabase, DBeaver, Grafana) connect. Big "enterprise check-the-box" item; can be a thin shim once Bolt or HTTP is up.

### 17. Notebook / Jupyter Kernel

Once PyO3 bindings (`spec-47`) land, ship `%gql` / `%gremlin` IPython magics and a kernel that renders results as DataFrames + inline graph viz. Pairs with #18.

### 18. Lightweight Result Visualization

Separate from the full web UI: a small standalone HTML renderer that takes a result set and draws nodes/edges with d3-force or sigma.js. Embeddable in the CLI (`--render html`), notebooks, and CI artifacts. Much cheaper than `guiding-documents/web-ui.md` and useful immediately.

### 19. Differential / Streaming Computation Backend

Ambitious but a real moat: integrate with `differential-dataflow` so registered queries stay incrementally up-to-date — a super-set of `spec-52` that handles aggregations and joins, not just match patterns.

### 20. Fuzzing & Chaos Test Harness

Complement existing Kani proofs and proptests with:

- `cargo fuzz` targets for the GQL & Gremlin parsers
- Crash-injection tests for the mmap WAL path (kill at every fsync boundary)
- Concurrency stress harness for the COW backend

---

## Suggested Sequencing

If reduced to the smallest set with the highest impact:

1. **Graph algorithms** (#1) — closes the most obvious gap; small per-algorithm scope; massive demoability
2. **Bulk loader** (#9) — unblocks real benchmarking and real users
3. **Bolt or HTTP server mode** (#2) — turns the library into a *database*
4. **Vector index** (#4) — modern table-stakes; attracts AI/RAG users
5. **Multi-statement transactions** (#3) — table-stakes for any "real" workload
6. **Observability** (#11) + **backup/restore** (#12) — makes #2 actually deployable
7. **CDC** (#5) → enables replication (#13), materialized views (#7), audit (#15)

---

## Out of Scope / Already Planned

The following are intentionally **not** proposed here because they are already covered elsewhere. Cross-reference before opening any new spec:

| Topic | Where it lives |
|---|---|
| Query planner / optimizer | `todos/todo.md` |
| Composite indexes | `todos/todo.md` |
| Full-text search (Tantivy) | `todos/todo.md`, `full-text` feature flag |
| Transactions (single-bullet) | `todos/todo.md` (this doc proposes expanding it — see #3) |
| Finish Rhai Gremlin API | `todos/todo.md` |
| Optimistic Concurrency Control | `specs/spec-40-optimistic-concurrency.md` |
| MVCC | `guiding-documents/mvcc.md` |
| EXPLAIN / PROFILE | `specs/explain_profile.md` |
| PyO3 Python bindings | `specs/spec-47-pyo3-bindings.md` |
| Reactive streaming queries | `specs/spec-52-reactive-streaming-queries.md` |
| WASM enhancements | `specs/spec-44-wasm-support.md`, `spec-45-wasm-bindgen.md` |
| napi-rs improvements | `specs/spec-46-napi-rs-bindings.md` |
| Browser / IndexedDB persistence | `specs/wasm-indexeddb-persistence.md` |
| Kani verification | `specs/spec-23-kani-verification.md` |
| Query storage (mmap query lib) | `specs/query-storage.md` |
| Integration test strategy | `specs/integration-test-strategy.md` |
| GraphQL server | `guiding-documents/graphql.md` |
| RDF / SPARQL store | `guiding-documents/rdf-store.md` |
| Document-store extension | `guiding-documents/document-store.md` |
| Web UI / Graph UI | `guiding-documents/web-ui.md`, `graph-ui.md` |
| Dart / Flutter bindings | `guiding-documents/dart-bindings.md` |
| Schema migrations runtime | `guiding-documents/migrations.md` |
| Temporal / bitemporal graphs | `guiding-documents/temporarl_graphs.md` |
| File versioning | `guiding-documents/file_versioning.md` |
| GQL → IR pipeline / IR query plan | `guiding-documents/gql-to-ir-pipeline.md`, `ir-query-plan.md` |
| Storage advanced topics | `guiding-documents/storage-advanced.md` |
