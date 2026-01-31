# Interstellar CLI - Project Brief

## Overview

Interstellar CLI is a command-line interface for **Interstellar**, a high-performance Rust graph database with Gremlin-style fluent API and GQL (Graph Query Language) support. The CLI provides a unified interface for creating, querying, and managing graph databases from the terminal.

## Core Features

### 1. Database Management

#### Create Database

Create a new persistent graph database on disk and open an interactive REPL session.

```bash
interstellar create <path>
interstellar create ./my-graph.db
interstellar create ~/databases/social-network.db
```

Options:
- `--force` - Overwrite existing database if present
- `--with-sample <name>` - Initialize with sample dataset (e.g., `marvel`, `british_royals`)
- `--no-repl` - Create database and exit without opening REPL
- `-ui` - Create database and open web UI instead of REPL

### 2. Query Execution

#### Interactive REPL

Start an interactive query session. This is the default behavior when no subcommand is given.

```bash
# Open REPL with existing database (default behavior)
interstellar <path>
interstellar ./my-graph.db

# Explicit query subcommand (equivalent)
interstellar query <path>
interstellar query ./my-graph.db

# GQL mode (default)
interstellar query ./my-graph.db

# Explicit GQL mode
interstellar query ./my-graph.db --gql

# Gremlin mode
interstellar query ./my-graph.db --gremlin

# In-memory session (no persistence)
interstellar query --memory
interstellar query --memory --gremlin
```

The prompt reflects the current mode:

```
gql> MATCH (n:Person) RETURN n.name
```

```
gremlin> g.v().has_label("person").values("name").to_list()
```

Switch modes within the REPL using `.mode`:

```
gql> .mode gremlin
gremlin> g.v().count()
gremlin> .mode gql
gql> MATCH (n) RETURN n LIMIT 5
```

#### One-shot Queries

Execute a single query and exit.

```bash
# GQL query
interstellar query ./my-graph.db --gql "MATCH (n:Person) RETURN n.name LIMIT 10"

# Gremlin query
interstellar query ./my-graph.db --gremlin "g.v().has_label('person').count()"

# Query from file (language inferred from extension: .gql or .gremlin)
interstellar query ./my-graph.db --file queries/find-friends.gql
interstellar query ./my-graph.db --file queries/traversal.gremlin

# Explicit language for file (overrides extension)
interstellar query ./my-graph.db --file queries/my-query.txt --gql
```

#### Query Options

| Flag | Description |
|------|-------------|
| `--gql` | Use GQL parser (default) |
| `--gremlin` | Use Gremlin parser |
| `--file <path>` | Read query from file |
| `--format <fmt>` | Output format: `table` (default), `json`, `csv` |
| `--limit <n>` | Limit results (default: 100, 0 = unlimited) |
| `--timing` | Show query execution time |
| `--memory` | Use in-memory database instead of file |
| `--readonly` | Open in read-only mode (prevents mutations) |

### 3. Data Import/Export

#### Import

Load data from various formats.

```bash
# Import JSON (vertices and edges)
interstellar import ./my-graph.db data.json

# Import CSV (vertices or edges)
interstellar import ./my-graph.db people.csv --label Person
interstellar import ./my-graph.db relationships.csv --edges --from-col source --to-col target

# Import from another Interstellar database
interstellar import ./my-graph.db ./other.db
```

Options:
- `--label <label>` - Label for imported vertices
- `--edges` - Treat file as edge data
- `--from-col <col>` - Source column for edges (default: `from`)
- `--to-col <col>` - Target column for edges (default: `to`)
- `--batch-size <n>` - Batch commit size (default: 10000)

#### Export

Export database contents.

```bash
interstellar export ./my-graph.db output.json
interstellar export ./my-graph.db output.json --label Person
interstellar export ./my-graph.db --format csv --output-dir ./export/
```

Options:
- `--label <label>` - Export only vertices with specific label
- `--format <format>` - Output format: `json` (default), `csv`
- `--output-dir <dir>` - Directory for multi-file export (CSV mode)
- `--pretty` - Pretty-print JSON output

### 4. Database Inspection

#### Stats

Display database statistics.

```bash
interstellar stats ./my-graph.db

# Output:
# Database: ./my-graph.db
# Storage: MmapGraph (persistent)
# 
# Vertices: 1,234
# Edges: 5,678
# 
# Vertex Labels:
#   Person: 500
#   Company: 234
#   Software: 500
#
# Edge Labels:
#   knows: 2,000
#   works_at: 1,500
#   uses: 2,178
```

#### Schema

Infer and display schema information.

```bash
interstellar schema ./my-graph.db

# Output:
# Vertex Labels:
#   Person
#     - name: String (required)
#     - age: Int (optional)
#     - email: String (optional)
#   Company
#     - name: String (required)
#     - founded: Int (optional)
#
# Edge Labels:
#   knows: Person -> Person
#   works_at: Person -> Company
```

Options:
- `--format <format>` - Output format: `text` (default), `json`

### 5. REPL Features

The interactive REPL provides:

- **Multi-line query support** - Queries can span multiple lines
- **Query history** - Arrow keys navigate history, persisted across sessions
- **Tab completion** - Complete labels, property names, and step names
- **Help commands** - Built-in help for syntax and examples
- **Result pagination** - Large results paginated automatically
- **Mode switching** - Switch between GQL and Gremlin modes

#### REPL Commands

```
.help              Show help
.schema            Display schema
.stats             Show statistics
.history           Show query history
.mode <lang>       Switch language mode (gql | gremlin)
.clear             Clear screen
.quit / .exit      Exit REPL
.set <key> <value> Set configuration (format, limit, timing)
.read <file>       Execute commands from file
.output <file>     Redirect output to file (.output stdout to reset)
```

## Query Languages

### GQL (Graph Query Language)

SQL-like declarative syntax for pattern matching:

```sql
-- Find all people
MATCH (n:Person) RETURN n

-- Find friends of Alice
MATCH (a:Person {name: 'Alice'})-[:knows]->(friend:Person)
RETURN friend.name

-- Count by label
MATCH (n:Person)
RETURN n.city, COUNT(*) AS count
GROUP BY n.city
ORDER BY count DESC
LIMIT 10

-- Variable-length paths
MATCH (a:Person {name: 'Alice'})-[:knows*1..3]->(distant)
RETURN distant.name

-- Mutations
CREATE (n:Person {name: 'Charlie', age: 35})

MATCH (n:Person {name: 'Alice'})
SET n.age = 31

MATCH (n:Person {name: 'Bob'})
DETACH DELETE n
```

### Gremlin Mode

Fluent traversal API powered by an embedded scripting engine (Rhai). Supports variables, loops, and custom functions.

```
// Basic traversals
g.v()                                      // All vertices
g.v().has_label("person")                  // Filter by label
g.v().has_value("name", "Alice")           // Filter by property
g.v().out("knows").values("name")          // Traverse and project
g.v().has_label("person").count()          // Count

// Variables and composition
let alice = g.v().has_value("name", "Alice").first();
let friends = alice.out("knows").to_list();

// Loops and logic
for p in g.v().has_label("person").limit(5) {
    print(p.name + " is " + p.age + " years old");
}

// Custom functions
fn friends_of_friends(name) {
    g.v()
        .has_value("name", name)
        .out("knows")
        .out("knows")
        .dedup()
        .to_list()
}

friends_of_friends("Alice")
```

## Configuration

### Configuration File

Interstellar CLI reads configuration from `~/.config/interstellar/config.toml` (or `~/.interstellar.toml` as fallback). Settings can be overridden by command-line flags.

```toml
# ~/.config/interstellar/config.toml

# Default output format for queries
format = "table"  # table | json | csv

# Default result limit (0 = unlimited)
limit = 100

# Show query execution time
timing = true

# REPL settings
[repl]
# Default query language
default_mode = "gql"  # gql | gremlin

# Command prefix style
command_prefix = "."  # "." (sqlite-style) or "\\" (psql-style)

# History file location (default: ~/.config/interstellar/history)
history_file = "~/.config/interstellar/history"

# Maximum history entries
history_size = 1000

# Enable syntax highlighting
highlight = true

# Prompt strings
prompt_gql = "gql> "
prompt_gremlin = "gremlin> "
continue_prompt = "...> "
```

### Precedence

Settings are resolved in this order (highest to lowest):

1. Command-line flags (`--format json`)
2. Environment variables (`INTERSTELLAR_FORMAT=json`)
3. Config file
4. Built-in defaults

### Environment Variables

| Variable | Description |
|----------|-------------|
| `INTERSTELLAR_DB` | Default database path |
| `INTERSTELLAR_FORMAT` | Output format (`table`, `json`, `csv`) |
| `INTERSTELLAR_LIMIT` | Default result limit |
| `INTERSTELLAR_MODE` | Default query mode (`gql`, `gremlin`) |

Using `INTERSTELLAR_DB`:

```bash
export INTERSTELLAR_DB=./my-graph.db
interstellar query --gql "MATCH (n) RETURN n LIMIT 5"
interstellar stats
# Both operate on the same DB without repeating the path
```

## Output Formats

### Table (Default)

```
+----+--------+-----+
| id | name   | age |
+----+--------+-----+
| 1  | Alice  | 30  |
| 2  | Bob    | 25  |
| 3  | Charlie| 35  |
+----+--------+-----+
3 rows (12ms)
```

### JSON

```json
[
  {"id": 1, "name": "Alice", "age": 30},
  {"id": 2, "name": "Bob", "age": 25},
  {"id": 3, "name": "Charlie", "age": 35}
]
```

### CSV

```csv
id,name,age
1,Alice,30
2,Bob,25
3,Charlie,35
```

## Command Summary

| Command | Description |
|---------|-------------|
| `interstellar <path>` | Open REPL for existing database (default) |
| `interstellar create <path>` | Create new database and open REPL |
| `interstellar query [path]` | Interactive REPL or one-shot query |
| `interstellar import <path> <file>` | Import data |
| `interstellar export <path> <file>` | Export data |
| `interstellar stats [path]` | Show statistics |
| `interstellar schema [path]` | Display inferred schema |
| `interstellar serve <path>` | Start web UI server (future) |
| `interstellar help [command]` | Show help |
| `interstellar version` | Show version |

Note: Commands that accept `[path]` will use `INTERSTELLAR_DB` environment variable if path is omitted.

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Connection/database error |
| 3 | Query syntax error |
| 4 | Query execution error |
| 5 | File I/O error |

## Future: Web UI Integration

The CLI will include a `serve` command to launch an embedded web interface:

```bash
interstellar serve ./my-graph.db
interstellar serve ./my-graph.db --port 3000 --open
```

The web UI will provide:
- Visual graph exploration with Cytoscape.js
- Query editor with syntax highlighting
- Schema explorer
- CRUD operations for vertices and edges
- Import/export functionality
- Query history and saved queries

See the Interstellar web-ui specification for full details.

## Technical Architecture

```
interstellar-cli/
├── src/
│   ├── main.rs           # Entry point, CLI parsing
│   ├── commands/
│   │   ├── mod.rs
│   │   ├── create.rs     # Database creation
│   │   ├── query.rs      # Query execution (REPL + one-shot)
│   │   ├── import.rs     # Data import
│   │   ├── export.rs     # Data export
│   │   ├── stats.rs      # Statistics display
│   │   ├── schema.rs     # Schema inference
│   │   └── serve.rs      # Web UI server (future)
│   ├── repl/
│   │   ├── mod.rs
│   │   ├── completer.rs  # Tab completion
│   │   ├── highlighter.rs # Syntax highlighting
│   │   └── history.rs    # Query history
│   ├── gremlin/
│   │   ├── mod.rs        # Rhai engine setup
│   │   ├── bindings.rs   # Graph API bindings for Rhai
│   │   └── stdlib.rs     # Built-in helper functions
│   ├── format/
│   │   ├── mod.rs
│   │   ├── table.rs      # Table formatting
│   │   ├── json.rs       # JSON output
│   │   └── csv.rs        # CSV output
│   ├── config/
│   │   ├── mod.rs        # Config loading & precedence
│   │   └── defaults.rs   # Default values
│   └── error.rs          # Error types
├── Cargo.toml
└── README.md
```

### Key Dependencies

| Crate | Purpose |
|-------|---------|
| `interstellar` | Core graph database library |
| `clap` | Command-line argument parsing |
| `rustyline` | REPL with history and completion |
| `rhai` | Embedded scripting for Gremlin mode |
| `comfy-table` | Table formatting |
| `serde_json` | JSON serialization |
| `csv` | CSV parsing/writing |
| `toml` | Config file parsing |
| `dirs` | XDG/platform config paths |
| `indicatif` | Progress bars for long operations |
| `colored` | Terminal colors |

## Development Phases

### Phase 1: Core Commands
- [ ] `create` - Database creation
- [ ] `query` - One-shot GQL queries
- [ ] `stats` - Basic statistics
- [ ] Configuration file support
- [ ] Environment variable support

### Phase 2: Interactive REPL
- [ ] Basic REPL with history
- [ ] GQL query execution
- [ ] Output formatting (table, json, csv)
- [ ] Tab completion
- [ ] REPL dot-commands (`.help`, `.schema`, `.stats`, etc.)

### Phase 3: Gremlin Mode
- [ ] Rhai engine integration
- [ ] Graph API bindings
- [ ] Mode switching in REPL
- [ ] Gremlin one-shot queries

### Phase 4: Import/Export
- [ ] JSON import/export
- [ ] CSV import/export
- [ ] Batch mode for large datasets
- [ ] Progress indicators

### Phase 5: Schema & Polish
- [ ] Schema inference
- [ ] Shell completions generation
- [ ] Man page generation

### Phase 6: Web UI
- [ ] `serve` command
- [ ] Embedded web interface
- [ ] REST API endpoints

## Design Principles

1. **Intuitive Commands** - Follow conventions from popular CLIs (git, docker, psql, sqlite3)
2. **Sensible Defaults** - Work out of the box with minimal flags
3. **Progressive Disclosure** - Simple usage for simple tasks, power features available
4. **Consistent Output** - Predictable formatting across commands
5. **Script-Friendly** - JSON output, exit codes, and environment variables for automation
6. **Fast Startup** - Minimal overhead for one-shot queries
7. **Configuration Layering** - Flags override env vars override config file override defaults