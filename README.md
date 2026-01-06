# RustGremlin

A high-performance Rust graph traversal library with Gremlin-style fluent API.

## Features

- **Gremlin-style Fluent API**: Chainable traversal steps with lazy evaluation
- **Dual Storage Backends**: In-memory (HashMap-based) and memory-mapped (persistent)
- **Anonymous Traversals**: Composable fragments via the `__` factory module
- **Zero-cost Abstractions**: Monomorphized traversal pipelines
- **Rich Predicate System**: `eq`, `neq`, `gt`, `lt`, `within`, `containing`, `regex`, and more
- **Path Tracking**: Full path history with `as_()` labels and `select()`
- **Thread-Safe**: All backends support concurrent read access

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
rustgremlin = "0.1"

# For persistent storage (memory-mapped files):
# rustgremlin = { version = "0.1", features = ["mmap"] }
```

### Basic Usage

```rust
use rustgremlin::graph::Graph;
use rustgremlin::storage::InMemoryGraph;
use rustgremlin::value::Value;
use std::collections::HashMap;
use std::sync::Arc;

fn main() {
    // Create an in-memory graph
    let mut storage = InMemoryGraph::new();

    // Add vertices
    let alice = storage.add_vertex("person", HashMap::from([
        ("name".to_string(), Value::String("Alice".to_string())),
        ("age".to_string(), Value::Int(30)),
    ]));

    let bob = storage.add_vertex("person", HashMap::from([
        ("name".to_string(), Value::String("Bob".to_string())),
        ("age".to_string(), Value::Int(25)),
    ]));

    // Add edge
    storage.add_edge(alice, bob, "knows", HashMap::new()).unwrap();

    // Wrap in Graph for traversal API
    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // Traverse: find all people Alice knows
    let friends = g.v_ids([alice])
        .out("knows")
        .values("name")
        .to_list();

    println!("Alice knows: {:?}", friends); // ["Bob"]
}
```

## Storage Backends

### InMemoryGraph (Default)

HashMap-based storage for development and small graphs:

```rust
use rustgremlin::storage::InMemoryGraph;

let graph = InMemoryGraph::new();
```

### MmapGraph (Persistent Storage)

Memory-mapped persistent storage with write-ahead logging. Enable with the `mmap` feature:

```toml
[dependencies]
rustgremlin = { version = "0.1", features = ["mmap"] }
```

```rust
use rustgremlin::storage::MmapGraph;
use std::collections::HashMap;

// Open or create a database
let graph = MmapGraph::open("my_graph.db").unwrap();

// Add data (each operation is durable by default)
let alice = graph.add_vertex("person", HashMap::from([
    ("name".to_string(), "Alice".into()),
])).unwrap();
```

#### Batch Mode (High-Performance Writes)

For bulk loading, use batch mode to defer fsync until commit (~500x faster):

```rust
use rustgremlin::storage::MmapGraph;
use std::collections::HashMap;

let graph = MmapGraph::open("my_graph.db").unwrap();

// Start batch mode
graph.begin_batch().unwrap();

// Add many vertices (no fsync per operation)
for i in 0..100_000 {
    let props = HashMap::from([("i".to_string(), (i as i64).into())]);
    graph.add_vertex("node", props).unwrap();
}

// Single fsync commits all operations atomically
graph.commit_batch().unwrap();
```

#### MmapGraph Features

- **Durability**: Write-ahead logging ensures crash recovery
- **Efficient Reads**: Memory-mapped access with zero-copy reads
- **Slot Reuse**: Deleted vertex/edge slots are reused
- **String Interning**: Compact storage for repeated strings
- **Label Indexes**: RoaringBitmap indexes for fast label filtering

## Traversal API

### Source Steps

```rust
g.v()                    // All vertices
g.e()                    // All edges
g.v_ids([id1, id2])      // Specific vertices by ID
g.e_ids([id1, id2])      // Specific edges by ID
g.inject([1, 2, 3])      // Inject arbitrary values
```

### Navigation Steps

```rust
.out("label")            // Outgoing edges (optional label filter)
.in_("label")            // Incoming edges
.both("label")           // Both directions
.out_e("label")          // Outgoing edge objects
.in_e("label")           // Incoming edge objects
.both_e("label")         // Both edge objects
.out_v()                 // Source vertex of edge
.in_v()                  // Target vertex of edge
.other_v()               // Opposite vertex of edge
```

### Filter Steps

```rust
.has("key")                        // Has property
.has_not("key")                    // Missing property
.has_value("key", value)           // Property equals value
.has_where("key", p::gt(30))       // Property matches predicate
.has_label("person")               // Filter by label
.has_id(id)                        // Filter by ID
.is_(p::gt(10))                    // Filter values by predicate
.is_eq(42)                         // Filter values equal to
.dedup()                           // Remove duplicates
.limit(10)                         // Take first N
.skip(5)                           // Skip first N
.range(5, 10)                      // Take elements 5-9
.filter(|v| ...)                   // Custom filter function
.simple_path()                     // Only non-cyclic paths
.cyclic_path()                     // Only cyclic paths
```

### Transform Steps

```rust
.values("name")                    // Extract property value
.values_multi(["name", "age"])     // Extract multiple properties
.label()                           // Get element label
.id()                              // Get element ID
.constant(42)                      // Replace with constant
.map(|v| ...)                      // Transform values
.flat_map(|v| ...)                 // Transform and flatten
.path()                            // Get traversal path
.value_map()                       // All properties as map
.value_map_selected(["name"])      // Selected properties as map
.element_map()                     // Properties + id + label
.unfold()                          // Flatten collections
```

### Branch Steps

```rust
.union([__::out("a"), __::out("b")])     // Merge multiple traversals
.coalesce([__::out("a"), __::out("b")])  // First non-empty traversal
.choose(cond, true_branch, false_branch) // Conditional branching
.optional(__::out("knows"))              // Include if exists
.and_([__::has("a"), __::has("b")])      // All conditions must match
.or_([__::has("a"), __::has("b")])       // Any condition matches
.not_(__::has("deleted"))                // Negation
.where_(__::out("knows").count())        // Filter by sub-traversal
.local(__::limit(1))                     // Apply per-element
```

### Repeat Steps

```rust
.repeat(__::out("parent"))
    .times(3)                      // Fixed iterations
    .until(__::has_label("root"))  // Stop condition
    .emit()                        // Emit intermediates
    .emit_if(__::has("important")) // Conditional emit
```

### Aggregation Steps

```rust
.group()                           // Group by identity
.group_by_label()                  // Group by element label
.group_count()                     // Count per group
.group_count_by(key)               // Count by property
```

### Path and Labels

```rust
.as_("a")                          // Label current position
.select("a")                       // Retrieve labeled value
.select_multi(["a", "b"])          // Multiple labels
.path()                            // Full traversal path
```

### Terminal Steps

```rust
.to_list()                         // Collect all results
.to_set()                          // Collect unique results
.count()                           // Count results
.sum()                             // Sum numeric values
.min()                             // Find minimum
.max()                             // Find maximum
.fold()                            // Collect into single list
.next()                            // First result (Option)
.one()                             // Exactly one result (Result)
.has_next()                        // Check if results exist
.iterate()                         // Consume without collecting
```

## Predicates

The `p` module provides rich predicates for filtering:

```rust
use rustgremlin::traversal::p;

p::eq(30)                          // Equals
p::neq(30)                         // Not equals
p::gt(30)                          // Greater than
p::gte(30)                         // Greater than or equal
p::lt(30)                          // Less than
p::lte(30)                         // Less than or equal
p::between(20, 40)                 // In range [20, 40)
p::inside(20, 40)                  // In range (20, 40)
p::outside(20, 40)                 // Outside range
p::within([1, 2, 3])               // In set
p::without([1, 2, 3])              // Not in set
p::containing("sub")               // String contains
p::starting_with("pre")            // String prefix
p::ending_with("suf")              // String suffix
p::regex(r"^\d+$")                 // Regex match
p::and_(p::gt(20), p::lt(40))      // Logical AND
p::or_(p::lt(20), p::gt(40))       // Logical OR
p::not_(p::eq(30))                 // Logical NOT
```

## Anonymous Traversals

The `__` module provides anonymous traversal fragments for composition:

```rust
use rustgremlin::traversal::__;

// Use in branch steps
g.v().union([
    __::out("knows"),
    __::out("created"),
]);

// Use in predicates
g.v().where_(__::out("knows").count().is_(p::gt(3)));

// Use in repeat
g.v().repeat(__::out("parent")).until(__::has_label("root"));
```

## Examples

Run the included examples:

```bash
cargo run --example basic_traversal
cargo run --example british_royals
cargo run --example filter_steps
cargo run --example navigation_steps
cargo run --example repeat_steps
cargo run --example branch_combinations

# MmapGraph example (requires mmap feature)
cargo run --example bench_writes --features mmap
```

## Building

```bash
cargo build                          # Debug build
cargo build --release                # Release build
cargo build --features mmap          # With mmap support
cargo build --features full-text     # With full-text search (planned)
```

## Testing

```bash
cargo test                           # Run all tests
cargo test --features mmap           # Include mmap tests
cargo clippy -- -D warnings          # Lint
cargo fmt --check                    # Check formatting
```

## Benchmarks

```bash
cargo bench                          # Run traversal benchmarks
cargo bench --features mmap          # Include mmap benchmarks
```

## Documentation

```bash
cargo doc --open                     # Build and view docs
```

## License

MIT
