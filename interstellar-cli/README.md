# Interstellar CLI

> **Early Development Notice**
>
> Interstellar is in early development and is **not recommended for production use**. APIs may change without notice, and the project has not been audited for security or performance at scale.

A command-line interface for the [Interstellar](../interstellar/) graph database.

## Installation

```bash
# From source
cargo install --path .

# Or build locally
cargo build --release
```

## Quick Start

```bash
# Create a new database with sample data
interstellar create ./my-graph.db --with-sample marvel

# Query the database
interstellar query ./my-graph.db --gql "MATCH (n:Character) RETURN n.name, n.alias"

# Interactive REPL
interstellar query ./my-graph.db

# View statistics
interstellar stats ./my-graph.db
```

## Commands

### create

Create a new database.

```bash
interstellar create <path> [OPTIONS]
```

**Options:**
- `--force` - Overwrite existing database
- `--with-sample <name>` - Initialize with sample data (`marvel` or `british_royals`)
- `--no-repl` - Exit after creation (don't open REPL)

**Examples:**
```bash
# Create empty database
interstellar create ./social.db --no-repl

# Create with Marvel sample data
interstellar create ./heroes.db --with-sample marvel --no-repl

# Overwrite existing database
interstellar create ./my.db --force --no-repl
```

### query

Execute GQL queries against a database.

```bash
interstellar query <path> [OPTIONS] [QUERY]
```

**Options:**
- `--gql` - Use GQL parser (default)
- `--gremlin` - Use Gremlin mode
- `--file <path>` - Read query from file
- `--format <fmt>` - Output format: `table`, `json`, `csv`
- `--limit <n>` - Limit results (default: 100, 0 = unlimited)
- `--timing` - Show query execution time
- `--memory` - Use in-memory database (enables Gremlin scripting)
- `--readonly` - Open in read-only mode

**Examples:**
```bash
# Simple query
interstellar query ./my.db --gql "MATCH (n) RETURN n LIMIT 10"

# Query with JSON output
interstellar query ./my.db --gql "MATCH (n:Person) RETURN n.name" --format json

# Query from file
interstellar query ./my.db --file queries/find-friends.gql

# Show timing
interstellar query ./my.db --gql "MATCH (n) RETURN count(n)" --timing
```

### stats

Display database statistics.

```bash
interstellar stats <path> [OPTIONS]
```

**Options:**
- `--format <fmt>` - Output format: `table`, `json`, `csv`

**Example output:**
```
Database: ./my-graph.db
Storage: PersistentGraph (mmap)

Vertices:          7
Edges:             9

Vertex Labels:
  Character: 6
  Team: 1

Edge Labels:
  member_of: 6
  ally: 2
  mentored_by: 1
```

### import

Import data from GraphSON files.

```bash
interstellar import ./my.db data.json
```

### export

Export database to GraphSON file.

```bash
interstellar export ./my.db output.json
```

### saved-query

Manage saved queries (save, list, get, delete, run).

```bash
interstellar saved-query ./my.db list
interstellar saved-query ./my.db save --name find_people --query "MATCH (n:Person) RETURN n"
interstellar saved-query ./my.db run --name find_people
```

## Planned Commands

The following commands are planned for future releases:

### schema (Phase 5 - Planned)

Display inferred schema.

### serve (Phase 6 - Planned)

Start web UI server.

## GQL Query Language

Interstellar supports GQL (Graph Query Language) for declarative graph queries:

```sql
-- Find all people
MATCH (n:Person) RETURN n

-- Find relationships
MATCH (a:Character)-[:ally]->(b:Character)
RETURN a.name, b.name

-- Filter with WHERE
MATCH (n:Royal)
WHERE n.birth_year > 1950
RETURN n.name, n.title

-- Aggregation
MATCH (n:Character)-[:member_of]->(t:Team)
RETURN t.name, count(n) AS members

-- Shortest path between two vertices
MATCH (a), (b) WHERE id(a) = 0 AND id(b) = 5
CALL interstellar.shortestPath(a, b)
YIELD path AS p, distance AS d
RETURN p, d

-- Dijkstra weighted shortest path
MATCH (a), (b) WHERE id(a) = 0 AND id(b) = 5
CALL interstellar.dijkstra(a, b, 'weight')
YIELD path AS p, distance AS d
RETURN p, d

-- BFS traversal
MATCH (a) WHERE id(a) = 0
CALL interstellar.bfs(a)
YIELD node AS v, depth AS d
RETURN v, d
```

## Gremlin Scripting

Interstellar supports Gremlin-style traversals via a native parser. This mode is available for both **in-memory and persistent graphs**.

```bash
# Start REPL in Gremlin mode
interstellar query ./my.db --gremlin

# Or with in-memory database
interstellar query --memory --gremlin
```

### Example Gremlin Scripts

```javascript
// Get a traversal source
let g = graph.gremlin();

// Add vertices
let alice = g.add_v("person").property("name", "Alice").id().first();
let bob = g.add_v("person").property("name", "Bob").id().first();

// Add an edge
g.add_e("knows").from_v(alice).to_v(bob).first();

// Query vertices
g.v().has_label("person").values("name").to_list()

// Count vertices
g.v().count()

// Traverse relationships
g.v().has_label("person").out("knows").values("name").to_list()

// Filter with predicates
g.v().has_label("person").has_where("age", gt(30)).to_list()

// Anonymous traversals for branching
g.v().union([A.out("knows"), A.out("works_with")]).to_list()

// Shortest path (unweighted BFS)
g.V(0).shortestPath(5).next()

// Dijkstra weighted shortest path
g.V(0).shortestPath(5).by('distance').next()

// BFS traversal with depth limit
g.V(0).bfs().with('maxDepth', 3).toList()

// DFS traversal
g.V(0).dfs().toList()

// Inspect traversal plan without executing
g.V().hasLabel('person').out('knows').values('name').explain()
```

### Available Traversal Steps

**Source Steps:** `v()`, `e()`, `add_v()`, `add_e()`

**Filter Steps:** `has_label()`, `has()`, `has_where()`, `has_id()`, `where_()`, `filter()`, `dedup()`, `limit()`, `skip()`, `range()`, `is()`, `not()`, `and_()`, `or_()`

**Map Steps:** `out()`, `in_()`, `both()`, `out_e()`, `in_e()`, `both_e()`, `out_v()`, `in_v()`, `values()`, `value_map()`, `properties()`, `id()`, `label()`, `path()`, `select()`, `project()`, `by()`, `as_()`, `unfold()`, `fold()`, `constant()`, `identity()`

**Mutation Steps:** `add_v()`, `add_e()`, `property()`, `from_v()`, `to_v()`, `drop()`

**Branch Steps:** `union()`, `choose()`, `coalesce()`, `optional()`, `repeat()`, `until()`, `times()`, `emit()`, `local()`

**Terminal Steps:** `to_list()`, `first()`, `next()`, `count()`, `sum()`, `min()`, `max()`, `mean()`, `group()`, `group_count()`, `explain()`

**Algorithm Steps:** `shortestPath()`, `shortestPath().by('weight')`, `kShortestPaths()`, `bfs()`, `dfs()`, `bidirectionalBfs()`, `iddfs()`

**Modulators:** `by()`, `with()`

**Full-Text Search:** `searchTextV()`, `searchTextE()`, `textScore()`

**Predicates:** `eq()`, `neq()`, `lt()`, `lte()`, `gt()`, `gte()`, `between()`, `inside()`, `outside()`, `within()`, `without()`, `containing()`, `starting_with()`, `ending_with()`, `regex()`

## Interactive REPL

When running `interstellar query` without a query argument, you enter an interactive REPL:

```bash
# Open REPL for persistent database (GQL + Gremlin)
interstellar query ./my.db

# Open REPL for in-memory database (GQL + Gremlin)
interstellar query --memory
```

### REPL Commands

| Command | Description |
|---------|-------------|
| `.help` | Show available commands |
| `.schema` | Display database schema (vertex/edge labels) |
| `.stats` | Show vertex and edge counts |
| `.history` | Show query history |
| `.mode <lang>` | Switch mode: `gql` or `gremlin` |
| `.set <key> <val>` | Set option: `format`, `limit`, `timing` |
| `.clear` | Clear screen |
| `.read <file>` | Execute queries from file |
| `.output <file>` | Redirect output to file |
| `.quit` / `.exit` | Exit REPL |

### REPL Features

- **Syntax highlighting** for GQL and Gremlin keywords
- **Tab completion** for commands, labels, and Gremlin methods
- **Command history** with up/down arrows
- **Multi-line queries** (continue until semicolon for GQL)
- **Hints** from history

## Output Formats

### Table (default)

```
+-----------------+------------------+
| n.alias         | n.name           |
+-----------------+------------------+
| Iron Man        | Tony Stark       |
| Captain America | Steve Rogers     |
+-----------------+------------------+
2 rows
```

### JSON

```json
[
  {"n.alias": "Iron Man", "n.name": "Tony Stark"},
  {"n.alias": "Captain America", "n.name": "Steve Rogers"}
]
```

### CSV

```csv
n.alias,n.name
Iron Man,Tony Stark
Captain America,Steve Rogers
```

## Configuration

Create a config file at `~/.config/interstellar/config.toml`:

```toml
# Default output format
format = "table"  # table | json | csv

# Default result limit (0 = unlimited)
limit = 100

# Show query execution time
timing = false

# REPL settings (Phase 2)
[repl]
default_mode = "gql"  # gql | gremlin
highlight = true
history_size = 1000
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `INTERSTELLAR_DB` | Default database path |
| `INTERSTELLAR_FORMAT` | Output format |
| `INTERSTELLAR_LIMIT` | Result limit |

```bash
# Set default database
export INTERSTELLAR_DB=./my-graph.db

# Now these work without specifying path
interstellar stats
interstellar query --gql "MATCH (n) RETURN n LIMIT 5"
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Database error |
| 3 | Query syntax error |
| 4 | Query execution error |
| 5 | File I/O error |

## Sample Datasets

### Marvel Universe

Characters and teams from the Marvel universe.

```bash
interstellar create ./marvel.db --with-sample marvel --no-repl
```

**Vertices:**
- `Character` - name, alias, powers
- `Team` - name, founded

**Edges:**
- `member_of` - Character belongs to Team
- `ally` - Character allied with Character
- `mentored_by` - Character mentored by Character

### British Royals

British royal family tree.

```bash
interstellar create ./royals.db --with-sample british_royals --no-repl
```

**Vertices:**
- `Royal` - name, title, birth_year

**Edges:**
- `parent_of` - Parent to child relationship
- `married_to` - Marriage relationship
- `sibling_of` - Sibling relationship

## Development Status

- [x] Phase 1: Project Setup & Core Commands
- [x] Phase 2: Interactive REPL
- [x] Phase 3: Gremlin Mode
- [x] Phase 4: Import/Export
- [ ] Phase 5: Schema & Polish
- [ ] Phase 6: Web UI

## License

MIT

## Development Approach

This project uses **spec-driven development** with AI assistance. Most code is generated or reviewed by LLMs (primarily Claude Opus 4.5). While we aim for high quality and test coverage, this approach is experimental.
