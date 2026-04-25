# Examples

Interstellar includes several example programs demonstrating different features. This guide walks through what each example does and how to run it.

## Running Examples

Run examples with Cargo:

```bash
# Basic examples
cargo run --example quickstart_gremlin
cargo run --example quickstart_gql
cargo run --example quickstart_gremlin_script
cargo run --example algorithms
cargo run --example explain

# Examples requiring features
cargo run --example storage --features mmap
cargo run --example query_storage --features mmap
cargo run --example nba --features mmap
cargo run --example quickstart_text_search --features full-text
cargo run --example geo_cities --features geospatial
cargo run --example geo_text_search --features "full-text,geospatial"
cargo run --example reactive_queries --features reactive
cargo run --example reactive_geo --features "reactive,geospatial"

# All features
cargo run --example shortest_path_cities --all-features
cargo run --example graphson
cargo run --example marvel
```

---

## Quick Start Examples

### quickstart_gremlin

**File:** `examples/quickstart_gremlin.rs`

Demonstrates the Gremlin-style fluent traversal API with a simple social network.

```bash
cargo run --example quickstart_gremlin
```

**What it covers:**
- Creating an in-memory graph
- Adding vertices with properties
- Adding edges between vertices
- Basic traversal steps (`out`, `in_`, `values`, `has_label`)
- Filtering with predicates

### quickstart_gql

**File:** `examples/quickstart_gql.rs`

Demonstrates the GQL query language with the same social network data.

```bash
cargo run --example quickstart_gql
```

**What it covers:**
- Pattern matching with `MATCH`
- Filtering with `WHERE`
- Returning specific properties
- Creating data with GQL mutations

### quickstart_text_search

**File:** `examples/quickstart_text_search.rs`
**Requires:** `full-text` feature

Demonstrates Tantivy-backed full-text search over a small set of articles.

```bash
cargo run --example quickstart_text_search --features full-text
```

**What it covers:**
- Registering a text index on a vertex property
- Indexing documents via `add_v` (mutation hooks keep the index in sync)
- BM25-ranked free-text queries with `search_text`
- Structured queries (`TextQuery::Phrase`, boolean, prefix) with `search_text_query`
- Chaining search results with regular Gremlin steps (`has_label`, `values`)
- Updating a document and observing the index change

See also the [Full-Text Search guide](../guides/full-text-search.md).

### quickstart_gremlin_script

**File:** `examples/quickstart_gremlin_script.rs`

Demonstrates executing Gremlin queries as text strings, including variable assignment, multi-statement scripts, and error handling.

```bash
cargo run --example quickstart_gremlin_script
```

**What it covers:**
- `graph.query()` and `graph.mutate()` for string-based Gremlin execution
- Variable binding across statements
- Error handling for invalid queries

---

## Algorithm Examples

### algorithms

**File:** `examples/algorithms.rs`

Demonstrates all graph algorithms using the standalone Rust API, the Gremlin fluent API, and Gremlin text queries.

```bash
cargo run --example algorithms
```

**What it covers:**
- BFS and DFS iterators with depth limits and label filters
- Unweighted shortest path, Dijkstra, A*, k-shortest paths
- Bidirectional BFS and IDDFS
- Gremlin fluent API: `shortest_path_to()`, `dijkstra_to()`, `bfs_traversal()`, `dfs_traversal()`, etc.
- Gremlin text parser: `shortestPath()`, `bfs()`, `dfs()`, etc.
- Error handling for missing vertices and unreachable targets

### shortest_path_cities

**File:** `examples/shortest_path_cities.rs`
**Requires:** geospatial features

Geospatial pathfinding example with European cities: Dijkstra and A\* with haversine-based heuristic.

```bash
cargo run --example shortest_path_cities --all-features
```

**What it covers:**
- Building a geospatial city network
- Dijkstra with distance weights
- A\* with geographic (haversine) heuristic
- Combining geospatial queries with pathfinding

### explain

**File:** `examples/explain.rs`

Demonstrates the `explain()` terminal step that describes a traversal pipeline without executing it.

```bash
cargo run --example explain
```

**What it covers:**
- `explain()` for inspecting traversal structure
- Viewing step names, categories, and descriptions
- Debugging traversal pipelines

---

## Geospatial Examples

### geo_cities

**File:** `examples/geo_cities.rs`
**Requires:** `geospatial` feature

Demonstrates geospatial indexing and queries with city data.

```bash
cargo run --example geo_cities --features geospatial
```

**What it covers:**
- Point values on vertices
- R-tree spatial index
- `withinDistance`, `bbox`, `containedBy` queries

### geo_text_search

**File:** `examples/geo_text_search.rs`
**Requires:** `full-text` and `geospatial` features

Combines full-text search with geospatial queries.

```bash
cargo run --example geo_text_search --features "full-text,geospatial"
```

**What it covers:**
- Text index and R-tree index on the same label
- Chaining text search results with geospatial filters

---

## Reactive Examples

### reactive_queries

**File:** `examples/reactive_queries.rs`
**Requires:** `reactive` feature

Demonstrates reactive streaming subscriptions that push results as the graph changes.

```bash
cargo run --example reactive_queries --features reactive
```

**What it covers:**
- Subscribing to graph change events
- Streaming query results in real time

### reactive_geo

**File:** `examples/reactive_geo.rs`
**Requires:** `reactive` and `geospatial` features

Combines reactive subscriptions with geospatial queries.

```bash
cargo run --example reactive_geo --features "reactive,geospatial"
```

**What it covers:**
- Reactive geospatial event streaming

---

## Import/Export Examples

### graphson

**File:** `examples/graphson.rs`

Demonstrates GraphSON 3.0 import and export for graph data interchange.

```bash
cargo run --example graphson
```

**What it covers:**
- GraphSON 3.0 format serialization
- Round-trip import/export
- Schema metadata handling

---

## Storage Examples

### storage

**File:** `examples/storage.rs`  
**Requires:** `mmap` feature

Demonstrates persistent storage with `MmapGraph`.

```bash
cargo run --example storage --features mmap
```

**What it covers:**
- Opening/creating a persistent database
- Data durability across restarts
- Batch mode for high-performance writes
- When to use persistent vs. in-memory storage

### query_storage

**File:** `examples/query_storage.rs`
**Requires:** `mmap` feature

Demonstrates saving and managing named queries in persistent storage.

```bash
cargo run --example query_storage --features mmap
```

**What it covers:**
- Saving named query templates
- Reloading queries from persistent storage
- Parameterized query execution

---

## Domain Examples

### nba

**File:** `examples/nba.rs`  
**Requires:** `mmap` feature

A comprehensive example modeling NBA data: players, teams, and statistics.

```bash
cargo run --example nba --features mmap
```

**What it covers:**
- Loading data from fixtures
- Complex traversals (multi-hop paths)
- Aggregations (counting, grouping)
- Real-world query patterns

### marvel

**File:** `examples/marvel.rs`

Models the Marvel universe with characters and their relationships.

```bash
cargo run --example marvel
```

**What it covers:**
- Larger graph structures
- Relationship types (allies, enemies, teams)
- Path finding between characters
- Graph exploration patterns

---

## Example Data

Examples load data from `examples/fixtures/`:

```
examples/fixtures/
├── nba_players.json
└── nba_teams.json
```

---

## Creating Your Own Examples

Use this template to create a new example:

```rust
// examples/my_example.rs

use interstellar::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup
    let graph = Graph::new();
    
    // Add your data
    let v1 = graph.add_vertex("label", props! {
        "prop" => "value"
    });
    
    // Query the graph
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();
    
    // Your queries here
    let results = g.v().has_label("label").to_list();
    println!("Results: {:?}", results);
    
    Ok(())
}
```

Add to `Cargo.toml`:

```toml
[[example]]
name = "my_example"
path = "examples/my_example.rs"
```

Run:

```bash
cargo run --example my_example
```

---

## See Also

- [Quick Start](quick-start.md) - Step-by-step introduction
- [Gremlin API](../api/gremlin.md) - Complete API reference
- [GQL API](../api/gql.md) - GQL syntax reference
