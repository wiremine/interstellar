# Intersteller CLI Specification

**Spec 06: Command-Line Interface**

**Status**: Draft  
**Dependencies**: Phase 3 (Traversal Engine), Phase 5 (Query IR)  
**Estimated Effort**: 3-4 weeks

---

## 1. Overview

This specification defines the `intersteller` command-line interface (CLI) tool, providing an interactive and batch-mode interface for working with Intersteller graph databases.

### 1.1 Goals

1. **End-user friendly**: Primary target audience is end-users working with graph data
2. **Dual query interface**: Support both GQL (primary) and Gremlin-style syntax
3. **Interactive REPL**: Full-featured read-eval-print loop with history and completion
4. **Batch mode**: Script execution from files or stdin
5. **Flexible storage**: Work with both in-memory and persistent databases
6. **Minimal initial release**: Focus on core functionality, extensible for future features

### 1.2 Non-Goals (Initial Release)

- Import/export functionality (future phase)
- Server mode / network protocol (future phase)
- Authentication / access control
- Clustering / replication
- GUI or web interface

### 1.3 Binary Name

```
intersteller
```

---

## 2. Architecture

### 2.1 Component Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLI Application                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │    REPL      │  │  Batch Mode  │  │  Single Cmd  │          │
│  │  (rustyline) │  │  (file/stdin)│  │  (-e flag)   │          │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘          │
│         │                 │                 │                   │
│         └─────────────────┼─────────────────┘                   │
│                           ▼                                     │
│                 ┌──────────────────┐                            │
│                 │  Query Dispatcher │                           │
│                 └────────┬─────────┘                            │
│                          │                                      │
│         ┌────────────────┼────────────────┐                     │
│         ▼                ▼                ▼                     │
│  ┌────────────┐  ┌────────────┐  ┌────────────────┐            │
│  │ GQL Parser │  │  Gremlin   │  │ Meta Commands  │            │
│  │   (pest)   │  │  Parser    │  │  (:help, etc)  │            │
│  └─────┬──────┘  └─────┬──────┘  └───────┬────────┘            │
│        │               │                 │                      │
│        └───────┬───────┘                 │                      │
│                ▼                         │                      │
│        ┌──────────────┐                  │                      │
│        │   Query IR   │                  │                      │
│        │  (QueryPlan) │                  │                      │
│        └──────┬───────┘                  │                      │
│               │                          │                      │
│               ▼                          ▼                      │
│        ┌──────────────┐         ┌──────────────┐               │
│        │  Traversal   │         │   Command    │               │
│        │  Execution   │         │  Execution   │               │
│        └──────┬───────┘         └──────┬───────┘               │
│               │                        │                        │
│               └────────────┬───────────┘                        │
│                            ▼                                    │
│                   ┌────────────────┐                            │
│                   │ Result Display │                            │
│                   │ (table/json)   │                            │
│                   └────────────────┘                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
              ┌──────────────────────────────┐
              │      Intersteller Library     │
              │  (Graph, Storage, Traversal) │
              └──────────────────────────────┘
```

### 2.2 Module Structure

```
src/
├── bin/
│   └── intersteller.rs       # Binary entry point
└── cli/                     # CLI module (library code)
    ├── mod.rs               # Module exports
    ├── app.rs               # Application state and lifecycle
    ├── repl.rs              # REPL implementation
    ├── commands.rs          # Meta-command handling
    ├── parser/
    │   ├── mod.rs           # Parser dispatch
    │   ├── gql.rs           # GQL parser (pest)
    │   └── gremlin.rs       # Gremlin text parser
    ├── executor.rs          # Query execution
    └── output.rs            # Result formatting
```

---

## 3. Command-Line Interface

### 3.1 Usage

```bash
intersteller [OPTIONS] [DATABASE]

Arguments:
  [DATABASE]  Path to database file (omit for in-memory)

Options:
  -e, --execute <QUERY>    Execute query and exit
  -f, --file <FILE>        Execute queries from file and exit
  -o, --output <FORMAT>    Output format: table, json, csv [default: table]
  -q, --quiet              Suppress banner and prompts (for scripting)
  -v, --verbose            Enable verbose output
      --gremlin            Use Gremlin syntax (default is GQL)
  -h, --help               Print help
  -V, --version            Print version

Examples:
  intersteller                      # Start REPL with in-memory database
  intersteller data.db              # Open persistent database
  intersteller -e "MATCH (n) RETURN n LIMIT 10"
  intersteller -f queries.gql data.db
  cat queries.gql | intersteller -q data.db
```

### 3.2 Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Query error (syntax or execution) |
| 2 | Database error (I/O, corruption) |
| 3 | Command-line argument error |

---

## 4. REPL Interface

### 4.1 Startup Banner

```
Intersteller v0.1.0
Type :help for commands, or enter a query.
Using in-memory database.

gql>
```

For persistent database:
```
Intersteller v0.1.0
Type :help for commands, or enter a query.
Database: /path/to/data.db (1,234 vertices, 5,678 edges)

gql>
```

### 4.2 Prompt

The prompt indicates the current query mode:

```
gql>      # GQL mode (default)
gremlin>  # Gremlin mode
...>      # Multi-line continuation
```

### 4.3 Multi-line Input

Queries can span multiple lines. The REPL detects incomplete input by:
1. Unclosed parentheses, brackets, or braces
2. Trailing backslash `\`
3. Explicit continuation with `...>` prompt

```
gql> MATCH (p:Person)
...> WHERE p.age > 30
...> RETURN p.name
```

### 4.4 History and Completion

- **History**: Persistent across sessions (`~/.intersteller_history`)
- **Tab completion**: 
  - Keywords (MATCH, WHERE, RETURN, etc.)
  - Meta-commands (:help, :schema, etc.)
  - Labels and property keys (from schema introspection)

### 4.5 Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| Ctrl+C | Cancel current input |
| Ctrl+D | Exit REPL (if line empty) |
| Ctrl+L | Clear screen |
| Up/Down | Navigate history |
| Ctrl+R | Reverse history search |
| Tab | Autocomplete |

---

## 5. Meta-Commands

Meta-commands start with `:` and are processed by the CLI, not the query engine.

### 5.1 Help and Information

```
:help                    Show available commands
:help <command>          Show help for specific command
:version                 Show version information
```

### 5.2 Database Management

```
:open <path>             Open a persistent database
:close                   Close current database (switch to in-memory)
:memory                  Switch to new in-memory database
:status                  Show database statistics
```

**:status output:**
```
Database: /path/to/data.db
Storage: Memory-mapped
Vertices: 1,234
Edges: 5,678
Labels: person, company, knows, works_at
Indexes: person(name), company(name)
```

### 5.3 Schema Introspection

```
:schema                  Show full schema
:labels                  List all vertex labels
:edge-labels             List all edge labels  
:properties <label>      Show properties for a label
```

**:schema output:**
```
Vertex Labels:
  person (1,000 vertices)
    Properties: name (String), age (Int), email (String)
  company (234 vertices)
    Properties: name (String), founded (Int)

Edge Labels:
  knows (3,456 edges)
    Properties: since (Int)
  works_at (2,222 edges)
    Properties: role (String), since (Int)
```

### 5.4 Query Mode

```
:gql                     Switch to GQL mode (default)
:gremlin                 Switch to Gremlin mode
:mode                    Show current mode
```

### 5.5 Output Control

```
:format <format>         Set output format (table, json, csv)
:limit <n>               Set default result limit (0 = unlimited)
:timing [on|off]         Toggle query timing display
```

### 5.6 Session Management

```
:history                 Show command history
:clear                   Clear screen
:quit or :exit           Exit the REPL
```

### 5.7 Data Modification (Future)

```
:begin                   Start a transaction
:commit                  Commit current transaction
:rollback                Rollback current transaction
```

---

## 6. Query Languages

### 6.1 GQL Mode (Primary)

The primary query language is a subset of ISO GQL as defined in `guilding-documents/gql.md`.

**Supported constructs:**
- `MATCH` patterns with node and edge filters
- `WHERE` clause with expressions
- `RETURN` clause with projections and aliases
- `ORDER BY` clause
- `LIMIT` and `OFFSET`
- Aggregations: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COLLECT`
- Variable-length paths: `*`, `*2`, `*1..3`

**Example queries:**
```sql
-- Find all people
MATCH (p:Person)
RETURN p.name, p.age

-- Find Alice's friends
MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(friend:Person)
RETURN friend.name

-- Count friends per person
MATCH (p:Person)-[:KNOWS]->(f:Person)
RETURN p.name, COUNT(f) AS friends
ORDER BY friends DESC
LIMIT 10
```

### 6.2 Gremlin Mode (Secondary)

Gremlin text format for users familiar with Apache TinkerPop.

**Supported constructs:**
- Source steps: `g.V()`, `g.E()`, `g.V(id)`
- Filter steps: `has()`, `hasLabel()`, `where()`, `limit()`
- Navigation: `out()`, `in()`, `both()`, `outE()`, `inE()`, `bothE()`
- Transform: `values()`, `valueMap()`, `id()`, `label()`
- Terminal: `toList()`, `count()`, `next()`

**Example queries:**
```groovy
// Find all people
g.V().hasLabel('person').values('name')

// Find Alice's friends
g.V().has('person', 'name', 'Alice').out('knows').values('name')

// Count by label
g.V().groupCount().by(label())
```

### 6.3 Auto-Detection

When not in explicit mode, the CLI attempts to auto-detect query language:
- Starts with `MATCH`, `CREATE`, `RETURN` → GQL
- Starts with `g.` → Gremlin
- Starts with `:` → Meta-command

---

## 7. Output Formats

### 7.1 Table Format (Default)

```
gql> MATCH (p:Person) RETURN p.name, p.age LIMIT 3
┌─────────┬─────┐
│ p.name  │ p.age│
├─────────┼─────┤
│ Alice   │ 30  │
│ Bob     │ 35  │
│ Carol   │ 28  │
└─────────┴─────┘
3 rows (2.1 ms)
```

For vertex/edge results:
```
gql> MATCH (p:Person) RETURN p LIMIT 2
┌─────────────────────────────────────────────────┐
│ p                                               │
├─────────────────────────────────────────────────┤
│ (v[1]:Person {name: "Alice", age: 30})          │
│ (v[2]:Person {name: "Bob", age: 35})            │
└─────────────────────────────────────────────────┘
2 rows (1.5 ms)
```

### 7.2 JSON Format

```json
{
  "columns": ["p.name", "p.age"],
  "data": [
    {"p.name": "Alice", "p.age": 30},
    {"p.name": "Bob", "p.age": 35},
    {"p.name": "Carol", "p.age": 28}
  ],
  "stats": {
    "rows": 3,
    "time_ms": 2.1
  }
}
```

### 7.3 CSV Format

```csv
p.name,p.age
Alice,30
Bob,35
Carol,28
```

### 7.4 Path Display

For path results:
```
gql> MATCH path = (a:Person)-[:KNOWS*1..3]->(b:Person)
     WHERE a.name = 'Alice' AND b.name = 'Dave'
     RETURN path LIMIT 1
┌───────────────────────────────────────────────────────────────┐
│ path                                                          │
├───────────────────────────────────────────────────────────────┤
│ (v[1]:Person)-[e[10]:KNOWS]->(v[2]:Person)-[e[15]:KNOWS]->(v[4]:Person) │
└───────────────────────────────────────────────────────────────┘
1 row (5.2 ms)
```

---

## 8. Error Handling

### 8.1 Syntax Errors

```
gql> MATCH (p:Person RETURN p.name
Error: Syntax error at line 1, column 17
  MATCH (p:Person RETURN p.name
                  ^
Expected: ')' or property filter
```

### 8.2 Execution Errors

```
gql> MATCH (p:Person) WHERE p.age / 0 > 1 RETURN p
Error: Division by zero in expression: p.age / 0
```

### 8.3 Reference Errors

```
gql> MATCH (p:Person) RETURN q.name
Error: Unknown variable 'q'. Did you mean 'p'?
```

### 8.4 Warning Messages

```
gql> MATCH (p:Person) RETURN p
Warning: No LIMIT specified. Returning first 1000 rows.
... results ...
```

---

## 9. Configuration

### 9.1 Configuration File

Location: `~/.config/intersteller/config.toml` (or `~/.intersteller.toml`)

```toml
[display]
format = "table"           # table, json, csv
default_limit = 1000       # 0 = unlimited
show_timing = true
color = "auto"             # auto, always, never

[history]
file = "~/.intersteller_history"
max_entries = 10000

[editor]
multiline = true
tab_width = 2

[database]
default_path = ""          # Empty = in-memory by default
```

### 9.2 Environment Variables

```bash
INTERSTELLER_CONFIG      # Path to config file
INTERSTELLER_HISTORY     # Path to history file
INTERSTELLER_FORMAT      # Default output format
NO_COLOR                # Disable colored output
```

---

## 10. Implementation

### 10.1 Dependencies

```toml
[dependencies]
# CLI argument parsing
clap = { version = "4.4", features = ["derive"] }

# REPL / line editing
rustyline = "13.0"
rustyline-derive = "0.10"

# GQL parser
pest = "2.7"
pest_derive = "2.7"

# Output formatting
comfy-table = "7.1"      # Table formatting
serde_json = "1.0"       # JSON output
csv = "1.3"              # CSV output

# Colored output
colored = "2.1"

# Configuration
toml = "0.8"
dirs = "5.0"             # Platform-specific directories

# Error handling
thiserror = "1.0"
anyhow = "1.0"

# Async (for potential future server mode)
# tokio = { version = "1", features = ["full"], optional = true }
```

### 10.2 Project Structure Decision

**Option A: Binary in existing crate (Recommended for initial release)**

```toml
# Cargo.toml
[[bin]]
name = "intersteller"
path = "src/bin/intersteller.rs"

[features]
default = ["inmemory", "cli"]
cli = ["clap", "rustyline", "comfy-table", "colored", "dirs"]
```

Pros:
- Single crate, simpler build
- Shared types without re-export hassles
- Easier initial development

Cons:
- CLI dependencies pulled in when using as library (mitigated by features)

**Option B: Workspace with separate CLI crate**

```
intersteller/
├── Cargo.toml           # Workspace root
├── intersteller/         # Library crate
│   ├── Cargo.toml
│   └── src/
└── intersteller-cli/     # CLI crate
    ├── Cargo.toml
    └── src/
```

Pros:
- Clean separation of concerns
- Library has no CLI dependencies

Cons:
- More complex project structure
- Version coordination

**Recommendation**: Start with Option A, migrate to Option B if the CLI grows significantly.

### 10.3 Core Types

```rust
// src/cli/mod.rs

use intersteller::{Graph, GraphSnapshot, Value};
use std::path::PathBuf;

/// CLI application state
pub struct App {
    /// Current database (in-memory or file-backed)
    graph: Graph,
    /// Database path (None = in-memory)
    db_path: Option<PathBuf>,
    /// Current query mode
    mode: QueryMode,
    /// Output configuration
    output: OutputConfig,
    /// Whether timing is enabled
    timing: bool,
}

/// Query mode
#[derive(Debug, Clone, Copy, Default)]
pub enum QueryMode {
    #[default]
    Gql,
    Gremlin,
}

/// Output configuration
#[derive(Debug, Clone)]
pub struct OutputConfig {
    pub format: OutputFormat,
    pub default_limit: Option<usize>,
    pub color: ColorMode,
}

#[derive(Debug, Clone, Copy, Default)]
pub enum OutputFormat {
    #[default]
    Table,
    Json,
    Csv,
}

#[derive(Debug, Clone, Copy, Default)]
pub enum ColorMode {
    #[default]
    Auto,
    Always,
    Never,
}
```

### 10.4 Main Entry Point

```rust
// src/bin/intersteller.rs

use clap::Parser;
use intersteller::cli::{App, run_repl, run_batch, run_single};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "intersteller")]
#[command(version, about = "Graph database CLI")]
struct Cli {
    /// Path to database file (omit for in-memory)
    database: Option<PathBuf>,

    /// Execute query and exit
    #[arg(short, long)]
    execute: Option<String>,

    /// Execute queries from file
    #[arg(short, long)]
    file: Option<PathBuf>,

    /// Output format
    #[arg(short, long, default_value = "table")]
    output: String,

    /// Suppress banner and prompts
    #[arg(short, long)]
    quiet: bool,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,

    /// Use Gremlin syntax instead of GQL
    #[arg(long)]
    gremlin: bool,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    
    let mut app = App::new(cli.database.as_deref())?;
    
    if cli.gremlin {
        app.set_mode(QueryMode::Gremlin);
    }
    
    app.set_output_format(&cli.output)?;
    
    if let Some(query) = cli.execute {
        // Single query mode
        run_single(&mut app, &query, cli.quiet)?;
    } else if let Some(file) = cli.file {
        // Batch mode from file
        run_batch(&mut app, &file, cli.quiet)?;
    } else if atty::isnt(atty::Stream::Stdin) {
        // Batch mode from stdin
        run_batch_stdin(&mut app, cli.quiet)?;
    } else {
        // Interactive REPL
        run_repl(&mut app, cli.quiet)?;
    }
    
    Ok(())
}
```

### 10.5 REPL Implementation

```rust
// src/cli/repl.rs

use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Config, EditMode};
use rustyline::history::FileHistory;

pub fn run_repl(app: &mut App, quiet: bool) -> anyhow::Result<()> {
    if !quiet {
        print_banner(app);
    }
    
    let config = Config::builder()
        .history_ignore_space(true)
        .edit_mode(EditMode::Emacs)
        .build();
    
    let mut rl = DefaultEditor::with_config(config)?;
    
    // Load history
    let history_path = get_history_path();
    let _ = rl.load_history(&history_path);
    
    loop {
        let prompt = app.prompt();
        
        match rl.readline(&prompt) {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                
                rl.add_history_entry(line)?;
                
                match process_input(app, line) {
                    Ok(ProcessResult::Continue) => {}
                    Ok(ProcessResult::Exit) => break,
                    Err(e) => {
                        eprintln!("Error: {}", e);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl+C - cancel current line
                continue;
            }
            Err(ReadlineError::Eof) => {
                // Ctrl+D - exit
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }
    
    // Save history
    let _ = rl.save_history(&history_path);
    
    Ok(())
}

enum ProcessResult {
    Continue,
    Exit,
}

fn process_input(app: &mut App, input: &str) -> anyhow::Result<ProcessResult> {
    if input.starts_with(':') {
        // Meta-command
        return process_command(app, input);
    }
    
    // Query execution
    let result = app.execute_query(input)?;
    app.display_result(&result);
    
    Ok(ProcessResult::Continue)
}
```

### 10.6 Command Processing

```rust
// src/cli/commands.rs

use crate::cli::App;

pub fn process_command(app: &mut App, input: &str) -> anyhow::Result<ProcessResult> {
    let parts: Vec<&str> = input[1..].split_whitespace().collect();
    let command = parts.first().map(|s| s.to_lowercase());
    let args = &parts[1..];
    
    match command.as_deref() {
        Some("help") | Some("h") | Some("?") => {
            show_help(args.first().copied());
        }
        Some("quit") | Some("exit") | Some("q") => {
            return Ok(ProcessResult::Exit);
        }
        Some("open") => {
            let path = args.first().ok_or_else(|| anyhow!("Usage: :open <path>"))?;
            app.open_database(path)?;
        }
        Some("close") => {
            app.close_database()?;
        }
        Some("memory") => {
            app.switch_to_memory()?;
        }
        Some("status") => {
            show_status(app);
        }
        Some("schema") => {
            show_schema(app);
        }
        Some("labels") => {
            show_labels(app);
        }
        Some("gql") => {
            app.set_mode(QueryMode::Gql);
            println!("Switched to GQL mode");
        }
        Some("gremlin") => {
            app.set_mode(QueryMode::Gremlin);
            println!("Switched to Gremlin mode");
        }
        Some("format") => {
            let fmt = args.first().ok_or_else(|| anyhow!("Usage: :format <table|json|csv>"))?;
            app.set_output_format(fmt)?;
        }
        Some("timing") => {
            let enabled = match args.first() {
                Some(&"on") => true,
                Some(&"off") => false,
                _ => !app.timing,  // Toggle
            };
            app.timing = enabled;
            println!("Timing {}", if enabled { "enabled" } else { "disabled" });
        }
        Some("clear") => {
            // Clear screen (ANSI escape)
            print!("\x1B[2J\x1B[1;1H");
        }
        Some("history") => {
            show_history();
        }
        Some(cmd) => {
            eprintln!("Unknown command: :{}", cmd);
            eprintln!("Type :help for available commands");
        }
        None => {
            eprintln!("Empty command. Type :help for available commands");
        }
    }
    
    Ok(ProcessResult::Continue)
}

fn show_help(topic: Option<&str>) {
    match topic {
        None => {
            println!("Available commands:");
            println!("  :help [command]    Show help");
            println!("  :quit, :exit       Exit the REPL");
            println!();
            println!("Database:");
            println!("  :open <path>       Open a database file");
            println!("  :close             Close current database");
            println!("  :memory            Switch to in-memory database");
            println!("  :status            Show database statistics");
            println!();
            println!("Schema:");
            println!("  :schema            Show full schema");
            println!("  :labels            List vertex labels");
            println!("  :edge-labels       List edge labels");
            println!();
            println!("Mode:");
            println!("  :gql               Switch to GQL mode");
            println!("  :gremlin           Switch to Gremlin mode");
            println!();
            println!("Output:");
            println!("  :format <fmt>      Set format (table, json, csv)");
            println!("  :timing [on|off]   Toggle query timing");
            println!();
            println!("Session:");
            println!("  :history           Show command history");
            println!("  :clear             Clear screen");
        }
        Some("open") => {
            println!(":open <path>");
            println!("  Open a persistent database file.");
            println!("  Creates the file if it doesn't exist.");
        }
        // ... more help topics
        Some(cmd) => {
            eprintln!("No help available for: {}", cmd);
        }
    }
}
```

### 10.7 Query Execution

```rust
// src/cli/executor.rs

use intersteller::{Graph, GraphSnapshot, Value};
use intersteller::query::{QueryPlan, parse_gql, parse_gremlin};
use std::time::Instant;

pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub execution_time: std::time::Duration,
}

impl App {
    pub fn execute_query(&self, query: &str) -> anyhow::Result<QueryResult> {
        let start = Instant::now();
        
        // Parse based on mode
        let plan = match self.mode {
            QueryMode::Gql => parse_gql(query)?,
            QueryMode::Gremlin => parse_gremlin(query)?,
        };
        
        // Execute
        let snap = self.graph.snapshot();
        let results = plan.execute(&snap)?;
        
        let execution_time = start.elapsed();
        
        Ok(QueryResult {
            columns: results.columns,
            rows: results.rows,
            execution_time,
        })
    }
}
```

### 10.8 Output Formatting

```rust
// src/cli/output.rs

use comfy_table::{Table, ContentArrangement, presets::UTF8_FULL};
use intersteller::Value;

impl App {
    pub fn display_result(&self, result: &QueryResult) {
        match self.output.format {
            OutputFormat::Table => display_table(result, self.timing),
            OutputFormat::Json => display_json(result, self.timing),
            OutputFormat::Csv => display_csv(result),
        }
    }
}

fn display_table(result: &QueryResult, show_timing: bool) {
    if result.rows.is_empty() {
        println!("(no results)");
        if show_timing {
            println!("0 rows ({:.1} ms)", result.execution_time.as_secs_f64() * 1000.0);
        }
        return;
    }
    
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic);
    
    // Header
    table.set_header(&result.columns);
    
    // Rows
    for row in &result.rows {
        let cells: Vec<String> = row.iter().map(format_value).collect();
        table.add_row(cells);
    }
    
    println!("{}", table);
    
    if show_timing {
        println!(
            "{} row{} ({:.1} ms)",
            result.rows.len(),
            if result.rows.len() == 1 { "" } else { "s" },
            result.execution_time.as_secs_f64() * 1000.0
        );
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(n) => n.to_string(),
        Value::Float(f) => format!("{:.6}", f),
        Value::String(s) => s.clone(),
        Value::Vertex(id) => format!("v[{}]", id.0),
        Value::Edge(id) => format!("e[{}]", id.0),
        Value::List(items) => {
            let formatted: Vec<String> = items.iter().map(format_value).collect();
            format!("[{}]", formatted.join(", "))
        }
        Value::Map(map) => {
            let pairs: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}: {}", k, format_value(v)))
                .collect();
            format!("{{{}}}", pairs.join(", "))
        }
    }
}

fn display_json(result: &QueryResult, show_timing: bool) {
    let output = serde_json::json!({
        "columns": result.columns,
        "data": result.rows.iter().map(|row| {
            result.columns.iter().zip(row.iter())
                .map(|(col, val)| (col.clone(), value_to_json(val)))
                .collect::<serde_json::Map<String, serde_json::Value>>()
        }).collect::<Vec<_>>(),
        "stats": {
            "rows": result.rows.len(),
            "time_ms": result.execution_time.as_secs_f64() * 1000.0
        }
    });
    
    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}

fn display_csv(result: &QueryResult) {
    let mut wtr = csv::Writer::from_writer(std::io::stdout());
    
    // Header
    wtr.write_record(&result.columns).unwrap();
    
    // Rows
    for row in &result.rows {
        let cells: Vec<String> = row.iter().map(format_value).collect();
        wtr.write_record(&cells).unwrap();
    }
    
    wtr.flush().unwrap();
}
```

---

## 11. GQL Parser

The GQL parser is based on the grammar defined in `guilding-documents/gql.md`. We use `pest` for parsing.

### 11.1 Grammar File

Create `src/cli/parser/gql.pest`:

```pest
// gql.pest - GQL subset grammar for Intersteller CLI

WHITESPACE = _{ " " | "\t" | "\r" | "\n" }
COMMENT = _{ "--" ~ (!"\n" ~ ANY)* | "/*" ~ (!"*/" ~ ANY)* ~ "*/" }

// Keywords (case-insensitive)
MATCH    = { ^"match" }
WHERE    = { ^"where" }
RETURN   = { ^"return" }
ORDER    = { ^"order" }
BY       = { ^"by" }
LIMIT    = { ^"limit" }
OFFSET   = { ^"offset" }
AS       = { ^"as" }
AND      = { ^"and" }
OR       = { ^"or" }
NOT      = { ^"not" }
IN       = { ^"in" }
IS       = { ^"is" }
NULL     = { ^"null" }
TRUE     = { ^"true" }
FALSE    = { ^"false" }
DISTINCT = { ^"distinct" }
ASC      = { ^"asc" }
DESC     = { ^"desc" }
CONTAINS = { ^"contains" }
STARTS   = { ^"starts" }
ENDS     = { ^"ends" }
WITH     = { ^"with" }
COUNT    = { ^"count" }
SUM      = { ^"sum" }
AVG      = { ^"avg" }
MIN      = { ^"min" }
MAX      = { ^"max" }
COLLECT  = { ^"collect" }

// Entry point
query = { SOI ~ match_clause ~ where_clause? ~ return_clause ~ order_clause? ~ limit_clause? ~ EOI }

// MATCH clause
match_clause = { MATCH ~ pattern ~ ("," ~ pattern)* }

pattern = { node_pattern ~ (edge_pattern ~ node_pattern)* }

node_pattern = { "(" ~ variable? ~ label_filter? ~ property_filter? ~ ")" }

edge_pattern = { 
    left_arrow? ~ "-[" ~ variable? ~ label_filter? ~ quantifier? ~ property_filter? ~ "]-" ~ right_arrow?
}

left_arrow = { "<" }
right_arrow = { ">" }

variable = @{ ASCII_ALPHA ~ (ASCII_ALPHANUMERIC | "_")* }

label_filter = { (":" ~ identifier)+ }

identifier = @{ ASCII_ALPHA ~ (ASCII_ALPHANUMERIC | "_")* }

property_filter = { "{" ~ property ~ ("," ~ property)* ~ "}" }

property = { identifier ~ ":" ~ literal }

quantifier = { "*" ~ range? }

range = { integer? ~ ".." ~ integer? | integer }

// WHERE clause
where_clause = { WHERE ~ expression }

// Expressions with precedence
expression = { or_expr }

or_expr = { and_expr ~ (OR ~ and_expr)* }

and_expr = { not_expr ~ (AND ~ not_expr)* }

not_expr = { NOT? ~ comparison }

comparison = { additive ~ (comp_op ~ additive)? | is_null_expr | in_expr }

is_null_expr = { additive ~ IS ~ NOT? ~ NULL }

in_expr = { additive ~ NOT? ~ IN ~ "[" ~ expression ~ ("," ~ expression)* ~ "]" }

comp_op = { "<>" | "!=" | "<=" | ">=" | "=" | "<" | ">" | CONTAINS | starts_with | ends_with }

starts_with = { STARTS ~ WITH }
ends_with = { ENDS ~ WITH }

additive = { multiplicative ~ (("+"|"-") ~ multiplicative)* }

multiplicative = { unary ~ (("*"|"/"|"%") ~ unary)* }

unary = { "-"? ~ primary }

primary = { 
    literal
    | aggregate
    | function_call
    | property_access
    | variable
    | "(" ~ expression ~ ")"
    | list_expr
}

property_access = { variable ~ "." ~ identifier }

function_call = { identifier ~ "(" ~ (expression ~ ("," ~ expression)*)? ~ ")" }

aggregate = { agg_func ~ "(" ~ DISTINCT? ~ expression ~ ")" }

agg_func = { COUNT | SUM | AVG | MIN | MAX | COLLECT }

list_expr = { "[" ~ (expression ~ ("," ~ expression)*)? ~ "]" }

// RETURN clause
return_clause = { RETURN ~ return_item ~ ("," ~ return_item)* }

return_item = { expression ~ (AS ~ identifier)? }

// ORDER BY clause
order_clause = { ORDER ~ BY ~ order_item ~ ("," ~ order_item)* }

order_item = { expression ~ (ASC | DESC)? }

// LIMIT clause
limit_clause = { LIMIT ~ integer ~ (OFFSET ~ integer)? }

// Literals
literal = { string | float | integer | TRUE | FALSE | NULL }

string = ${ "'" ~ string_inner ~ "'" }
string_inner = @{ (!"'" ~ ANY | "''")* }

integer = @{ "-"? ~ ASCII_DIGIT+ }

float = @{ "-"? ~ ASCII_DIGIT+ ~ "." ~ ASCII_DIGIT+ }
```

### 11.2 Parser Implementation

```rust
// src/cli/parser/gql.rs

use pest::Parser;
use pest_derive::Parser;
use crate::cli::parser::ast::*;

#[derive(Parser)]
#[grammar = "cli/parser/gql.pest"]
pub struct GqlParser;

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("Syntax error: {0}")]
    Syntax(String),
    #[error("Missing MATCH clause")]
    MissingMatch,
    #[error("Missing RETURN clause")]
    MissingReturn,
    #[error("Invalid pattern")]
    InvalidPattern,
}

pub fn parse_gql(input: &str) -> Result<Query, ParseError> {
    let pairs = GqlParser::parse(Rule::query, input)
        .map_err(|e| ParseError::Syntax(format_pest_error(e)))?;
    
    let query_pair = pairs.into_iter().next().unwrap();
    build_query(query_pair)
}

fn format_pest_error(e: pest::error::Error<Rule>) -> String {
    match e.line_col {
        pest::error::LineColLocation::Pos((line, col)) => {
            format!("at line {}, column {}: {}", line, col, e.variant.message())
        }
        pest::error::LineColLocation::Span((l1, c1), (l2, c2)) => {
            format!("from ({}, {}) to ({}, {}): {}", l1, c1, l2, c2, e.variant.message())
        }
    }
}

fn build_query(pair: pest::iterators::Pair<Rule>) -> Result<Query, ParseError> {
    let mut match_clause = None;
    let mut where_clause = None;
    let mut return_clause = None;
    let mut order_clause = None;
    let mut limit_clause = None;
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::match_clause => match_clause = Some(build_match_clause(inner)?),
            Rule::where_clause => where_clause = Some(build_where_clause(inner)?),
            Rule::return_clause => return_clause = Some(build_return_clause(inner)?),
            Rule::order_clause => order_clause = Some(build_order_clause(inner)?),
            Rule::limit_clause => limit_clause = Some(build_limit_clause(inner)?),
            Rule::EOI => {}
            _ => {}
        }
    }
    
    Ok(Query {
        match_clause: match_clause.ok_or(ParseError::MissingMatch)?,
        where_clause,
        return_clause: return_clause.ok_or(ParseError::MissingReturn)?,
        order_clause,
        limit_clause,
    })
}

// Additional builder functions follow the same pattern as gql.md section 5.2
```

### 11.3 AST Module

```rust
// src/cli/parser/ast.rs

/// Complete query
#[derive(Debug, Clone)]
pub struct Query {
    pub match_clause: MatchClause,
    pub where_clause: Option<WhereClause>,
    pub return_clause: ReturnClause,
    pub order_clause: Option<OrderClause>,
    pub limit_clause: Option<LimitClause>,
}

/// MATCH clause with one or more patterns
#[derive(Debug, Clone)]
pub struct MatchClause {
    pub patterns: Vec<Pattern>,
}

/// A pattern is a path through the graph
#[derive(Debug, Clone)]
pub struct Pattern {
    pub elements: Vec<PatternElement>,
}

#[derive(Debug, Clone)]
pub enum PatternElement {
    Node(NodePattern),
    Edge(EdgePattern),
}

/// Node pattern: (variable:Label {prop: value})
#[derive(Debug, Clone)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: Vec<(String, Literal)>,
}

/// Edge pattern: -[variable:TYPE*1..3]->
#[derive(Debug, Clone)]
pub struct EdgePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub direction: EdgeDirection,
    pub quantifier: Option<PathQuantifier>,
    pub properties: Vec<(String, Literal)>,
}

#[derive(Debug, Clone, Copy)]
pub enum EdgeDirection {
    Outgoing,   // -->
    Incoming,   // <--
    Both,       // --
}

#[derive(Debug, Clone)]
pub struct PathQuantifier {
    pub min: Option<u32>,
    pub max: Option<u32>,
}

// ... (remaining AST types from gql.md section 4)
```

### 11.4 Query Compilation

The query compiler transforms the AST into executable traversals:

```rust
// src/cli/compiler.rs

use crate::cli::parser::ast::*;
use intersteller::{Graph, GraphSnapshot};

pub struct QueryCompiler<'g> {
    graph: &'g Graph,
}

impl<'g> QueryCompiler<'g> {
    pub fn new(graph: &'g Graph) -> Self {
        Self { graph }
    }
    
    pub fn compile(&self, query: Query) -> Result<CompiledQuery<'g>, CompileError> {
        // 1. Analyze patterns to determine optimal start point
        let start = self.choose_start_point(&query)?;
        
        // 2. Build traversal pipeline from patterns
        let traversal = self.build_traversal(&query, start)?;
        
        // 3. Add WHERE filter predicate
        let traversal = if let Some(where_clause) = &query.where_clause {
            self.add_where_filter(traversal, where_clause)?
        } else {
            traversal
        };
        
        // 4. Build projection from RETURN clause
        let projection = self.build_projection(&query.return_clause)?;
        
        // 5. Build ordering from ORDER BY clause
        let ordering = query.order_clause.as_ref()
            .map(|o| self.build_ordering(o))
            .transpose()?;
        
        // 6. Extract LIMIT/OFFSET
        let limit = query.limit_clause.as_ref()
            .map(|l| (l.limit, l.offset));
        
        Ok(CompiledQuery {
            traversal,
            projection,
            ordering,
            limit,
        })
    }
}
```

---

## 12. Testing Strategy

### 12.1 Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    mod parser_tests {
        use crate::cli::parser::gql::parse_gql;
        
        #[test]
        fn test_simple_match() {
            let query = parse_gql("MATCH (p:Person) RETURN p").unwrap();
            assert_eq!(query.match_clause.patterns.len(), 1);
            assert!(query.where_clause.is_none());
        }
        
        #[test]
        fn test_property_filter() {
            let query = parse_gql("MATCH (p:Person {name: 'Alice'}) RETURN p.name").unwrap();
            let pattern = &query.match_clause.patterns[0];
            if let PatternElement::Node(node) = &pattern.elements[0] {
                assert_eq!(node.properties.len(), 1);
                assert_eq!(node.properties[0].0, "name");
            } else {
                panic!("Expected node pattern");
            }
        }
        
        #[test]
        fn test_edge_pattern() {
            let query = parse_gql(
                "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"
            ).unwrap();
            let pattern = &query.match_clause.patterns[0];
            assert_eq!(pattern.elements.len(), 3); // node, edge, node
        }
        
        #[test]
        fn test_variable_length_path() {
            let query = parse_gql(
                "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN b.name"
            ).unwrap();
            let pattern = &query.match_clause.patterns[0];
            if let PatternElement::Edge(edge) = &pattern.elements[1] {
                let q = edge.quantifier.as_ref().unwrap();
                assert_eq!(q.min, Some(1));
                assert_eq!(q.max, Some(3));
            }
        }
        
        #[test]
        fn test_where_clause() {
            let query = parse_gql(
                "MATCH (p:Person) WHERE p.age > 30 AND p.city = 'NYC' RETURN p"
            ).unwrap();
            assert!(query.where_clause.is_some());
        }
        
        #[test]
        fn test_aggregation() {
            let query = parse_gql(
                "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, COUNT(f) AS friends"
            ).unwrap();
            assert_eq!(query.return_clause.items.len(), 2);
        }
        
        #[test]
        fn test_order_limit() {
            let query = parse_gql(
                "MATCH (p:Person) RETURN p.name ORDER BY p.age DESC LIMIT 10 OFFSET 5"
            ).unwrap();
            assert!(query.order_clause.is_some());
            let limit = query.limit_clause.as_ref().unwrap();
            assert_eq!(limit.limit, 10);
            assert_eq!(limit.offset, Some(5));
        }
        
        #[test]
        fn test_syntax_error() {
            let result = parse_gql("MATCH (p:Person RETURN p");
            assert!(result.is_err());
        }
    }
    
    mod command_tests {
        use crate::cli::commands::*;
        
        #[test]
        fn test_parse_meta_command() {
            assert!(parse_command(":help").is_ok());
            assert!(parse_command(":quit").is_ok());
            assert!(parse_command(":format json").is_ok());
            assert!(parse_command(":unknown").is_err());
        }
        
        #[test]
        fn test_format_command() {
            let mut app = App::new_in_memory();
            process_command(&mut app, ":format json").unwrap();
            assert_eq!(app.output.format, OutputFormat::Json);
        }
    }
    
    mod output_tests {
        use crate::cli::output::*;
        
        #[test]
        fn test_format_value() {
            assert_eq!(format_value(&Value::Int(42)), "42");
            assert_eq!(format_value(&Value::String("hello".into())), "hello");
            assert_eq!(format_value(&Value::Null), "null");
            assert_eq!(format_value(&Value::Bool(true)), "true");
        }
        
        #[test]
        fn test_table_rendering() {
            let result = QueryResult {
                columns: vec!["name".into(), "age".into()],
                rows: vec![
                    vec![Value::String("Alice".into()), Value::Int(30)],
                    vec![Value::String("Bob".into()), Value::Int(35)],
                ],
                execution_time: std::time::Duration::from_millis(5),
            };
            
            let output = render_table(&result);
            assert!(output.contains("Alice"));
            assert!(output.contains("Bob"));
            assert!(output.contains("name"));
        }
    }
}
```

### 12.2 Integration Tests

```rust
// tests/cli_integration.rs

use intersteller::cli::{App, run_query};

fn setup_test_graph() -> App {
    let mut app = App::new_in_memory();
    
    // Add test data
    app.execute_query("CREATE (a:Person {name: 'Alice', age: 30})").unwrap();
    app.execute_query("CREATE (b:Person {name: 'Bob', age: 35})").unwrap();
    app.execute_query("CREATE (c:Person {name: 'Carol', age: 28})").unwrap();
    app.execute_query("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)").unwrap();
    
    app
}

#[test]
fn test_simple_query() {
    let app = setup_test_graph();
    let result = app.execute_query("MATCH (p:Person) RETURN p.name").unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn test_filter_query() {
    let app = setup_test_graph();
    let result = app.execute_query("MATCH (p:Person) WHERE p.age > 29 RETURN p.name").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn test_relationship_query() {
    let app = setup_test_graph();
    let result = app.execute_query(
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN b.name"
    ).unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn test_aggregation_query() {
    let app = setup_test_graph();
    let result = app.execute_query(
        "MATCH (p:Person) RETURN COUNT(p) AS total"
    ).unwrap();
    assert_eq!(result.rows[0][0], Value::Int(3));
}

#[test]
fn test_order_limit() {
    let app = setup_test_graph();
    let result = app.execute_query(
        "MATCH (p:Person) RETURN p.name ORDER BY p.age DESC LIMIT 2"
    ).unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::String("Bob".into()));
}
```

### 12.3 End-to-End Tests

```rust
// tests/cli_e2e.rs

use std::process::{Command, Stdio};
use std::io::Write;

#[test]
fn test_cli_single_query() {
    let output = Command::new("cargo")
        .args(["run", "--", "-e", "MATCH (p:Person) RETURN p.name"])
        .output()
        .expect("Failed to execute CLI");
    
    assert!(output.status.success());
}

#[test]
fn test_cli_json_output() {
    let output = Command::new("cargo")
        .args(["run", "--", "-e", "MATCH (p:Person) RETURN p.name", "-o", "json"])
        .output()
        .expect("Failed to execute CLI");
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"columns\""));
    assert!(stdout.contains("\"data\""));
}

#[test]
fn test_cli_file_input() {
    // Create temp file with queries
    let mut file = tempfile::NamedTempFile::new().unwrap();
    writeln!(file, "MATCH (p:Person) RETURN p.name").unwrap();
    
    let output = Command::new("cargo")
        .args(["run", "--", "-f", file.path().to_str().unwrap()])
        .output()
        .expect("Failed to execute CLI");
    
    assert!(output.status.success());
}

#[test]
fn test_cli_stdin_input() {
    let mut child = Command::new("cargo")
        .args(["run", "--", "-q"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to spawn CLI");
    
    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(b"MATCH (p:Person) RETURN p.name\n").unwrap();
    }
    
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
}

#[test]
fn test_cli_syntax_error_exit_code() {
    let output = Command::new("cargo")
        .args(["run", "--", "-e", "MATCH (p:Person RETURN p"])
        .output()
        .expect("Failed to execute CLI");
    
    assert_eq!(output.status.code(), Some(1));
}
```

---

## 13. Acceptance Criteria

### 13.1 CLI Launch and Arguments

- [ ] `intersteller` launches interactive REPL with in-memory database
- [ ] `intersteller <path>` opens persistent database file
- [ ] `intersteller -e <query>` executes single query and exits
- [ ] `intersteller -f <file>` executes queries from file and exits
- [ ] `intersteller -o json` sets output format
- [ ] `intersteller -q` suppresses banner/prompts for scripting
- [ ] `intersteller --gremlin` starts in Gremlin mode
- [ ] Exit code 0 on success, 1 on query error, 2 on database error, 3 on argument error

### 13.2 REPL Functionality

- [ ] Displays startup banner with version and database info
- [ ] Prompt shows current mode (`gql>` or `gremlin>`)
- [ ] Multi-line input with `...>` continuation prompt
- [ ] Command history persisted to `~/.intersteller_history`
- [ ] Tab completion for keywords and meta-commands
- [ ] Ctrl+C cancels current input
- [ ] Ctrl+D exits REPL (if line empty)
- [ ] Ctrl+L clears screen

### 13.3 Meta-Commands

- [ ] `:help` shows available commands
- [ ] `:help <cmd>` shows help for specific command
- [ ] `:quit` / `:exit` exits the REPL
- [ ] `:open <path>` opens database file
- [ ] `:close` closes current database
- [ ] `:memory` switches to new in-memory database
- [ ] `:status` shows database statistics
- [ ] `:schema` shows vertex/edge labels and properties
- [ ] `:gql` / `:gremlin` switches query mode
- [ ] `:format <fmt>` sets output format
- [ ] `:timing on/off` toggles timing display

### 13.4 Query Execution

- [ ] GQL queries parse correctly
- [ ] MATCH patterns with labels and properties work
- [ ] WHERE clause filtering works
- [ ] RETURN projections and aliases work
- [ ] ORDER BY sorting works (ASC/DESC)
- [ ] LIMIT and OFFSET work
- [ ] Aggregations (COUNT, SUM, AVG, MIN, MAX, COLLECT) work
- [ ] Variable-length paths work (*n, *n..m)
- [ ] Multiple patterns in single MATCH work
- [ ] Gremlin syntax works (g.V(), out(), has(), etc.)

### 13.5 Output Formatting

- [ ] Table format displays correctly with borders
- [ ] JSON format is valid JSON with columns/data/stats
- [ ] CSV format is valid CSV with header row
- [ ] Vertices display as `v[id]` with properties
- [ ] Edges display as `e[id]` with properties
- [ ] Paths display as connected elements
- [ ] Timing shows milliseconds when enabled

### 13.6 Error Handling

- [ ] Syntax errors show line/column with caret pointing to error
- [ ] Unknown variable errors suggest similar names
- [ ] Execution errors show meaningful messages
- [ ] Database errors (I/O, corruption) handled gracefully
- [ ] No panics in normal operation

---

## 14. Implementation Phases

### Phase 1: CLI Skeleton (Week 1)

**Goal**: Basic CLI infrastructure without query execution

**Tasks**:
1. Set up `src/bin/intersteller.rs` with clap argument parsing
2. Implement `src/cli/mod.rs` with App state
3. Implement basic REPL loop with rustyline
4. Add meta-commands (:help, :quit, :status)
5. Add output formatting skeleton (table only)

**Deliverables**:
- `intersteller` launches and accepts input
- Meta-commands work
- `:status` shows placeholder stats
- Queries print "Not implemented"

### Phase 2: GQL Parser (Week 1-2)

**Goal**: Parse GQL queries into AST

**Tasks**:
1. Create pest grammar file (`gql.pest`)
2. Implement AST types in `src/cli/parser/ast.rs`
3. Implement parser in `src/cli/parser/gql.rs`
4. Add parser tests for all grammar constructs
5. Integrate parser with REPL (parse and print AST)

**Deliverables**:
- Parser passes all grammar tests
- REPL shows parsed AST for valid queries
- Syntax errors show line/column

### Phase 3: Query Execution (Week 2-3)

**Goal**: Execute parsed queries against the graph

**Tasks**:
1. Implement query compiler (`src/cli/compiler.rs`)
2. Pattern compilation to traversal steps
3. WHERE clause compilation to predicates
4. RETURN clause projection
5. ORDER BY, LIMIT, OFFSET
6. Integration tests with sample queries

**Deliverables**:
- Basic MATCH/WHERE/RETURN queries execute
- Results display in table format
- Timing information available

### Phase 4: Advanced Features (Week 3-4)

**Goal**: Complete feature set

**Tasks**:
1. Aggregation support (COUNT, SUM, etc.)
2. Variable-length path execution
3. JSON and CSV output formats
4. Gremlin parser (basic subset)
5. Configuration file support
6. Tab completion for labels/properties

**Deliverables**:
- All query features working
- Multiple output formats
- Both GQL and Gremlin modes

### Phase 5: Polish (Week 4)

**Goal**: Production readiness

**Tasks**:
1. Error message improvements
2. Performance optimization
3. Documentation (--help text, error messages)
4. End-to-end test suite
5. Edge case handling
6. Release preparation

**Deliverables**:
- All acceptance criteria met
- Test coverage > 80%
- No known bugs

---

## 15. Future Enhancements

### 15.1 Import/Export (Phase 2)

```
:import csv <file> [--label <label>]
:import json <file>
:export csv <query> <file>
:export json <query> <file>
```

### 15.2 Server Mode (Phase 3)

```bash
intersteller serve --port 8182
```

- WebSocket protocol compatible with Gremlin Server
- HTTP REST API for queries
- Multiple concurrent connections

### 15.3 Transactions (Phase 3)

```
:begin
CREATE (p:Person {name: 'Alice'})
:commit
```

Or:
```
:begin
... queries ...
:rollback
```

### 15.4 Plugins (Phase 4)

- Custom functions loadable at runtime
- User-defined aggregations
- Import/export format plugins

---

## 16. References

### 16.1 Internal Documentation

- `guilding-documents/gql.md` - GQL grammar and AST definition
- `guilding-documents/gremlin.md` - Gremlin API mapping
- `guilding-documents/overview.md` - Architecture overview
- `specs/spec-03-traversal-engine-core.md` - Traversal engine specification
- `specs/spec-05-ir.md` - Query IR specification

### 16.2 External Resources

- [pest.rs](https://pest.rs/) - Parser library documentation
- [rustyline](https://docs.rs/rustyline/) - Line editor documentation
- [clap](https://docs.rs/clap/) - CLI argument parser
- [comfy-table](https://docs.rs/comfy-table/) - Table formatting
- [ISO GQL Standard](https://www.gqlstandards.org/) - Official GQL specification

---

*End of CLI Specification*
