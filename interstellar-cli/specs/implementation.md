# Interstellar CLI - Implementation Plan

## Overview

This document outlines the implementation plan for Interstellar CLI, a command-line interface for the Interstellar graph database. The implementation follows a phased approach, starting with core functionality and progressively adding features.

---

## Phase 1: Project Setup & Core Commands

### 1.1 Project Initialization

**Files to create:**
- `Cargo.toml` - Project manifest with dependencies
- `src/main.rs` - Entry point with CLI parsing
- `src/error.rs` - Custom error types

**Dependencies:**
```toml
[dependencies]
interstellar = { path = "../interstellar" }  # Core graph database
clap = { version = "4", features = ["derive", "env"] }
thiserror = "1"
anyhow = "1"
toml = "0.8"
serde = { version = "1", features = ["derive"] }
dirs = "5"
```

**Tasks:**
- [x] Initialize Cargo project
- [x] Define `CliError` enum with variants for database, query, I/O errors
- [x] Map errors to exit codes (0-5 as specified)

### 1.2 CLI Argument Parsing

**File:** `src/main.rs`

Define command structure using clap derive macros:

```rust
#[derive(Parser)]
#[command(name = "interstellar", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
    
    /// Database path (opens REPL if no subcommand)
    path: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Commands {
    Create { ... },
    Query { ... },
    Import { ... },
    Export { ... },
    Stats { ... },
    Schema { ... },
    Serve { ... },  // Future
}
```

**Tasks:**
- [x] Implement `Commands` enum with all subcommands
- [x] Add global flags (`--format`, `--limit`, etc.)
- [x] Handle default behavior (`interstellar <path>` opens REPL)
- [x] Wire up `INTERSTELLAR_*` environment variables via clap's `env` attribute

### 1.3 Configuration System

**Files:**
- `src/config/mod.rs` - Config loading and precedence logic
- `src/config/defaults.rs` - Default values

**Config struct:**
```rust
#[derive(Debug, Deserialize, Default)]
pub struct Config {
    pub format: OutputFormat,      // table | json | csv
    pub limit: usize,              // default: 100
    pub timing: bool,              // default: false
    pub repl: ReplConfig,
}

#[derive(Debug, Deserialize, Default)]
pub struct ReplConfig {
    pub default_mode: QueryMode,   // gql | gremlin
    pub command_prefix: String,    // "." or "\\"
    pub history_file: PathBuf,
    pub history_size: usize,
    pub highlight: bool,
    pub prompt_gql: String,
    pub prompt_gremlin: String,
    pub continue_prompt: String,
}
```

**Tasks:**
- [x] Load config from `~/.config/interstellar/config.toml`
- [x] Fallback to `~/.interstellar.toml`
- [x] Implement precedence: CLI flags > env vars > config file > defaults
- [x] Create `Config::load()` method

### 1.4 Create Command

**File:** `src/commands/create.rs`

```rust
pub fn execute(
    path: PathBuf,
    force: bool,
    with_sample: Option<String>,
    no_repl: bool,
    ui: bool,
) -> Result<()>
```

**Tasks:**
- [x] Check if database exists; error or overwrite based on `--force`
- [x] Create new `MmapGraph` at path
- [x] If `--with-sample`, load sample dataset (marvel, british_royals)
- [x] If `--no-repl`, exit after creation
- [x] If `-ui`, launch web UI (stub for Phase 6)
- [x] Otherwise, enter REPL (Phase 2)

### 1.5 Stats Command

**File:** `src/commands/stats.rs`

**Tasks:**
- [x] Open database in read-only mode
- [x] Count total vertices and edges
- [x] Group vertices by label with counts
- [x] Group edges by label with counts
- [x] Format output (text or JSON based on `--format`)

### 1.6 One-Shot Query Execution

**File:** `src/commands/query.rs`

**Tasks:**
- [x] Parse `--gql` or `--gremlin` flag (default: GQL)
- [x] If inline query provided, execute and return results
- [x] If `--file` provided, read query from file
- [x] Infer language from file extension (`.gql`, `.gremlin`)
- [x] Apply `--limit` and `--format` options
- [x] Show timing if `--timing` enabled
- [x] Support `--memory` for in-memory database
- [x] Support `--readonly` mode

---

## Phase 2: Interactive REPL

### 2.1 REPL Core

**Files:**
- `src/repl/mod.rs` - Main REPL loop
- `src/repl/history.rs` - Query history management

**Dependencies to add:**
```toml
rustyline = { version = "14", features = ["derive"] }
```

**Tasks:**
- [x] Create `Repl` struct with database handle, config, and mode
- [x] Implement main loop with `rustyline::Editor`
- [x] Detect multi-line queries (incomplete GQL statements)
- [x] Configure history file location from config
- [x] Persist history across sessions
- [x] Handle Ctrl+C (cancel current input) and Ctrl+D (exit)

### 2.2 REPL Dot-Commands

**File:** `src/repl/mod.rs`

| Command | Implementation |
|---------|----------------|
| `.help` | Print help text |
| `.schema` | Call schema inference (Phase 5) |
| `.stats` | Call stats command |
| `.history` | Display query history |
| `.mode <lang>` | Switch between GQL and Gremlin |
| `.clear` | Clear terminal screen |
| `.quit` / `.exit` | Exit REPL |
| `.set <key> <val>` | Update runtime config |
| `.read <file>` | Execute commands from file |
| `.output <file>` | Redirect output to file |

**Tasks:**
- [x] Parse dot-commands (or backslash if configured)
- [x] Implement each command handler
- [x] Show appropriate prompt based on mode (`gql>` or `gremlin>`)

### 2.3 Output Formatting

**Files:**
- `src/format/mod.rs` - Format trait and dispatcher
- `src/format/table.rs` - ASCII table output
- `src/format/json.rs` - JSON output
- `src/format/csv.rs` - CSV output

**Dependencies to add:**
```toml
comfy-table = "7"
serde_json = "1"
csv = "1"
```

**Tasks:**
- [x] Define `QueryResult` struct representing query output
- [x] Implement `TableFormatter` with column width calculation
- [x] Implement `JsonFormatter` with optional pretty-printing
- [x] Implement `CsvFormatter` for streaming output
- [x] Add row count and timing footer for table format
- [ ] Implement result pagination for large result sets

### 2.4 Tab Completion

**File:** `src/repl/completer.rs`

**Tasks:**
- [x] Implement `rustyline::Completer` trait
- [x] Complete vertex labels from database schema
- [ ] Complete property names based on current context
- [x] Complete GQL keywords (MATCH, RETURN, WHERE, etc.)
- [x] Complete dot-commands when line starts with `.`

### 2.5 Syntax Highlighting

**File:** `src/repl/highlighter.rs`

**Dependencies to add:**
```toml
colored = "2"
```

**Tasks:**
- [x] Implement `rustyline::Highlighter` trait
- [x] Highlight GQL keywords (blue)
- [x] Highlight string literals (green)
- [x] Highlight numbers (yellow)
- [x] Highlight labels (cyan)
- [x] Respect `config.repl.highlight` setting

---

## Phase 3: Gremlin Mode

### 3.1 Rhai Engine Setup

**Files:**
- `src/gremlin/mod.rs` - Engine initialization
- `src/gremlin/bindings.rs` - Graph API bindings
- `src/gremlin/stdlib.rs` - Built-in helper functions

**Dependencies to add:**
```toml
rhai = "1"
```

**Tasks:**
- [x] Initialize Rhai engine with custom scope
- [x] Disable unsafe features (file I/O, network)
- [x] Set up error handling and source mapping

### 3.2 Graph API Bindings

**File:** `src/gremlin/bindings.rs`

Expose graph traversal API to Rhai:

```rust
// Register types
engine.register_type::<VertexProxy>();
engine.register_type::<TraversalProxy>();

// Register methods
engine.register_fn("v", |g: &mut GraphProxy| -> TraversalProxy { ... });
engine.register_fn("has_label", |t: &mut TraversalProxy, label: &str| { ... });
engine.register_fn("has_value", |t: &mut TraversalProxy, key: &str, val: Dynamic| { ... });
engine.register_fn("out", |t: &mut TraversalProxy, label: &str| { ... });
engine.register_fn("in_", |t: &mut TraversalProxy, label: &str| { ... });
// ... etc
```

**Tasks:**
- [x] Create `GraphProxy` wrapper for database handle
- [x] Create `TraversalProxy` for lazy traversal building
- [x] Create `VertexProxy` for vertex access
- [x] Create `EdgeProxy` for edge access
- [x] Implement all traversal steps: `v()`, `e()`, `has_label()`, `has_value()`, `out()`, `in_()`, `both()`, `values()`, `count()`, `limit()`, `dedup()`, `first()`, `to_list()`
- [ ] Implement mutation steps: `add_v()`, `add_e()`, `property()`, `drop()`
- [x] Register `print()` function for output

### 3.3 Mode Switching

**Tasks:**
- [x] Add `QueryMode` enum (GQL, Gremlin)
- [x] Implement `.mode gql` and `.mode gremlin` commands
- [x] Update prompt based on current mode
- [x] Route queries to appropriate parser/executor

### 3.4 Gremlin One-Shot Queries

**Tasks:**
- [ ] Support `--gremlin "script"` flag for one-shot execution
- [x] Support `--file script.gremlin` execution
- [x] Format Rhai output values appropriately

---

## Phase 4: Import/Export

### 4.1 Import Command

**File:** `src/commands/import.rs`

**Dependencies to add:**
```toml
indicatif = "0.17"
```

**Tasks:**
- [ ] Detect file format from extension (`.json`, `.csv`)
- [ ] Implement JSON import (vertices and edges array)
- [ ] Implement CSV vertex import with `--label` flag
- [ ] Implement CSV edge import with `--edges`, `--from-col`, `--to-col`
- [ ] Implement database-to-database import
- [ ] Batch commits with `--batch-size` (default: 10000)
- [ ] Show progress bar with `indicatif`
- [ ] Report import statistics on completion

**JSON format:**
```json
{
  "vertices": [
    { "id": "v1", "label": "Person", "properties": { "name": "Alice" } }
  ],
  "edges": [
    { "id": "e1", "label": "knows", "from": "v1", "to": "v2", "properties": {} }
  ]
}
```

### 4.2 Export Command

**File:** `src/commands/export.rs`

**Tasks:**
- [ ] Export to JSON (single file with vertices and edges)
- [ ] Export to CSV (separate files for vertices/edges by label)
- [ ] Filter by `--label` for partial export
- [ ] Support `--pretty` for formatted JSON
- [ ] Show progress bar for large databases
- [ ] Report export statistics on completion

---

## Phase 5: Schema & Polish

### 5.1 Schema Inference

**File:** `src/commands/schema.rs`

**Tasks:**
- [ ] Scan vertices by label
- [ ] Infer property types from values (String, Int, Float, Bool, etc.)
- [ ] Detect required vs optional properties (present in 100% vs <100% of vertices)
- [ ] Infer edge patterns (source_label -> edge_label -> target_label)
- [ ] Output as text (default) or JSON

### 5.2 Shell Completions

**Tasks:**
- [ ] Generate Bash completions via clap
- [ ] Generate Zsh completions via clap
- [ ] Generate Fish completions via clap
- [ ] Add `interstellar completions <shell>` subcommand

### 5.3 Documentation

**Tasks:**
- [ ] Generate man pages via clap
- [ ] Write `--help` text for all commands
- [ ] Add examples to help output

---

## Phase 6: Web UI (Future)

### 6.1 Serve Command

**File:** `src/commands/serve.rs`

**Dependencies to add:**
```toml
axum = "0.7"
tokio = { version = "1", features = ["full"] }
tower-http = { version = "0.5", features = ["fs", "cors"] }
rust-embed = "8"
```

**Tasks:**
- [ ] Embed static web UI assets
- [ ] Serve on configurable port (default: 8080)
- [ ] Implement REST API endpoints:
  - `GET /api/stats` - Database statistics
  - `GET /api/schema` - Schema information
  - `POST /api/query` - Execute query
  - `GET /api/vertices` - List vertices
  - `GET /api/edges` - List edges
- [ ] Open browser if `--open` flag provided
- [ ] Handle graceful shutdown

---

## Implementation Order

```
Week 1-2: Phase 1 (Project Setup & Core Commands)
├── Day 1-2: Project init, error types, CLI parsing
├── Day 3-4: Configuration system
├── Day 5-6: Create command
├── Day 7-8: Stats command
└── Day 9-10: One-shot query execution

Week 3-4: Phase 2 (Interactive REPL)
├── Day 1-3: REPL core with history
├── Day 4-5: Dot-commands
├── Day 6-7: Output formatting
├── Day 8-9: Tab completion
└── Day 10: Syntax highlighting

Week 5-6: Phase 3 (Gremlin Mode)
├── Day 1-3: Rhai engine setup
├── Day 4-7: Graph API bindings
├── Day 8-9: Mode switching
└── Day 10: Gremlin one-shot queries

Week 7-8: Phase 4 (Import/Export)
├── Day 1-4: Import command (JSON, CSV)
└── Day 5-8: Export command (JSON, CSV)

Week 9: Phase 5 (Schema & Polish)
├── Day 1-3: Schema inference
├── Day 4-5: Shell completions
└── Day 6-7: Documentation

Week 10+: Phase 6 (Web UI)
└── Implement serve command with embedded UI
```

---

## Testing Strategy

### Unit Tests
- Config parsing and precedence
- Output formatters
- Query parsing
- Error code mapping

### Integration Tests
- Create database, add data, query, verify results
- Import/export round-trip
- REPL command execution
- Gremlin script execution

### Test Files
```
tests/
├── integration/
│   ├── create_test.rs
│   ├── query_test.rs
│   ├── import_export_test.rs
│   └── repl_test.rs
├── fixtures/
│   ├── sample.json
│   ├── vertices.csv
│   └── edges.csv
└── common/mod.rs
```

---

## Sample Datasets

### Marvel Universe
- Characters as vertices (name, alias, powers)
- Teams as vertices (Avengers, X-Men, etc.)
- Relationships: `member_of`, `ally`, `enemy`, `mentored_by`

### British Royals
- Royals as vertices (name, title, birth_year)
- Relationships: `parent_of`, `married_to`, `sibling_of`

**Location:** Embed as static assets or download on first use to `~/.config/interstellar/samples/`

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Large dataset performance | Batch commits, streaming output, progress indicators |
| Rhai security | Disable file/network ops, sandbox execution |
| Cross-platform paths | Use `dirs` crate, normalize paths |
| REPL edge cases | Comprehensive multi-line detection, graceful error recovery |
| Breaking interstellar API | Pin version, integration tests |

---

## Success Criteria

- [ ] All 6 phases implemented
- [ ] Test coverage >80%
- [ ] Sub-100ms startup for one-shot queries
- [ ] Handles databases with 1M+ vertices
- [ ] Works on Linux, macOS, Windows
- [ ] Published to crates.io
