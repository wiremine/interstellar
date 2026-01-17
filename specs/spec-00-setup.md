# Spec 00: Project Setup

Foundational setup for Interstellar prior to implementing core types. Draws from `overview.md` and `implementation.md`.

---

## Goals
- Establish repository layout and crate metadata
- Define dependencies, feature flags, and build profiles
- Document standard build/test commands
- Provide initial public API skeleton (`lib.rs`) hooks

## Scope
- Cargo configuration and features (`inmemory`, `mmap`, `full-text` placeholder)
- Directory structure for src, tests, benches
- Dependency/Dev-dependency set with purpose notes
- Baseline build/test workflows

## Directory Layout
```
interstellar/
├── Cargo.toml
├── src/
│   ├── lib.rs              # Public API, prelude (skeleton)
│   ├── graph.rs            # Graph, GraphSnapshot, GraphMut (later specs)
│   ├── value.rs            # Value enum, IDs (Phase 1)
│   ├── error.rs            # Error types (Phase 1)
│   ├── storage/            # Storage abstraction + backends
│   ├── index/              # Indexes
│   ├── traversal/          # Fluent API
│   └── algorithms/         # Graph algorithms
├── tests/                  # Integration tests
├── benches/                # Criterion benchmarks
└── specs/                  # Design/implementation specs
```

## Cargo Configuration
- Edition: 2021
- Features:
  - `default = ["inmemory"]`
  - `inmemory = []`
  - `mmap = ["memmap2"]`
  - `full-text = ["tantivy"]` (future; optional placeholder)
- Profiles: use Cargo defaults; enable `opt-level = 3` for release (standard)

### Dependencies (purpose)
- Core: `thiserror` (errors), `hashbrown` (maps), `smallvec` (stack vec)
- Serialization: `serde` with `derive`
- Concurrency: `parking_lot`
- Indexes: `roaring` (bitmap label/index)
- Storage optional: `memmap2` (with `mmap` feature)
- WAL integrity: `crc32fast` (reserved for later phases)

### Dev Dependencies
- `criterion` (benchmarks)
- `proptest` (property tests)
- `tempfile` (temp file handling)

## Build, Test & Coverage Commands
- Build: `cargo build` | `cargo build --release`
- Check: `cargo check`
- Test: `cargo test` (optionally `--features mmap`)
- Lint: `cargo clippy -- -D warnings`
- Format check: `cargo fmt --check`
- Bench: `cargo bench` (when benches exist)
- Coverage (Linux/macOS example): `cargo llvm-cov test --workspace --html` (requires `cargo-llvm-cov`)

## Coverage Expectations
- Target: 100% branch and line coverage for committed code
- Gate PRs with coverage runs where feasible (CI step recommended)
- Property tests contribute to coverage but must remain deterministic under fixed seeds

## Public API Skeleton (lib.rs)
- Modules: `graph`, `value`, `error`, `storage`, `index`, `traversal`
- Prelude re-exports: `Graph`, `GraphSnapshot`, `GraphMut`, `Value`, `VertexId`, `EdgeId`, `ElementId`, `StorageError`, `TraversalError`, `Traversal`, `Traverser`, `Path`, `GraphTraversalSource`, `p`, `__`
- No implementations required in this spec—only the structural contract for later phases

## Acceptance Criteria
- Cargo.toml declares dependencies and features above
- Repository follows the directory layout (empty modules allowed initially)
- `cargo check` succeeds with stub modules present
- `lib.rs` exposes module stubs and prelude re-exports (even if items are placeholders)

## References
- `guilding-documents/overview.md`
- `specs/implementation.md` (Architecture Overview, Dependencies section)
