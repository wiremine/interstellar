# Examples

Interstellar includes several example programs demonstrating different features. This guide walks through what each example does and how to run it.

## Running Examples

Run examples with Cargo:

```bash
# Basic examples
cargo run --example quickstart_gremlin
cargo run --example quickstart_gql

# Examples requiring features
cargo run --example storage --features mmap
cargo run --example nba --features mmap
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
