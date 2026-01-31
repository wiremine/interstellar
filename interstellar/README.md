# Interstellar

> **Early Development Notice**
>
> Interstellar is in early development and is **not recommended for production use**. APIs may change without notice, and the project has not been audited for security or performance at scale.

A high-performance Rust graph database with dual query APIs: Gremlin-style fluent traversals and GQL (Graph Query Language).

## Features

- **Dual Query APIs**: Gremlin-style fluent API and SQL-like GQL syntax
- **Gremlin Text Parser**: Execute TinkerPop-compatible Gremlin query strings
- **GQL Schema & DDL**: Define vertex/edge types with validation (`CREATE NODE TYPE`, `CREATE EDGE TYPE`)
- **Dual Storage Backends**: In-memory (HashMap-based) and memory-mapped (persistent)
- **Anonymous Traversals**: Composable fragments via the `__` factory module
- **Zero-cost Abstractions**: Monomorphized traversal pipelines
- **Rich Predicate System**: `eq`, `neq`, `gt`, `lt`, `within`, `containing`, `regex`, and more
- **Path Tracking**: Full path history with `as_()` labels and `select()`
- **Thread-Safe**: All backends support concurrent read access
- **Formal Verification**: Critical code paths verified with Kani

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
interstellar = "0.1"

# With Gremlin text parser:
# interstellar = { version = "0.1", features = ["gremlin"] }

# With GQL query language:
# interstellar = { version = "0.1", features = ["gql"] }

# For persistent storage (memory-mapped files):
# interstellar = { version = "0.1", features = ["mmap"] }

# Everything enabled:
# interstellar = { version = "0.1", features = ["full"] }
```

### Basic Usage

```rust
use interstellar::prelude::*;

fn main() {
    // Create an in-memory graph
    let graph = Graph::new();

    // Add vertices using the props! macro
    let alice = graph.add_vertex("person", props! {
        "name" => "Alice",
        "age" => 30i64
    });

    let bob = graph.add_vertex("person", props! {
        "name" => "Bob",
        "age" => 25i64
    });

    // Add edge
    graph.add_edge(alice, bob, "knows", props! {}).unwrap();

    // Get a snapshot for traversal
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

### Graph (Default)

In-memory graph with copy-on-write snapshots. Uses interior mutability for thread-safe mutations:

```rust
use interstellar::prelude::*;

let graph = Graph::new();  // No mut needed - interior mutability
let id = graph.add_vertex("person", props! { "name" => "Alice" });
let snapshot = graph.snapshot();  // Immutable point-in-time view
```

### MmapGraph (Persistent Storage)

Memory-mapped persistent storage with write-ahead logging. Enable with the `mmap` feature:

```toml
[dependencies]
interstellar = { version = "0.1", features = ["mmap"] }
```

```rust
use interstellar::prelude::*;
use interstellar::storage::MmapGraph;

// Open or create a database
let graph = MmapGraph::open("my_graph.db").unwrap();

// Add data (each operation is durable by default)
let alice = graph.add_vertex("person", props! {
    "name" => "Alice"
}).unwrap();
```

#### Batch Mode (High-Performance Writes)

For bulk loading, use batch mode to defer fsync until commit (~500x faster):

```rust
use interstellar::prelude::*;
use interstellar::storage::MmapGraph;

let graph = MmapGraph::open("my_graph.db").unwrap();

// Start batch mode
graph.begin_batch().unwrap();

// Add many vertices (no fsync per operation)
for i in 0..100_000 {
    graph.add_vertex("node", props! { "i" => i as i64 }).unwrap();
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
.union([__.out("a"), __.out("b")])     // Merge multiple traversals
.coalesce([__.out("a"), __.out("b")])  // First non-empty traversal
.choose(cond, true_branch, false_branch) // Conditional branching
.optional(__.out("knows"))              // Include if exists
.and_([__.has("a"), __.has("b")])      // All conditions must match
.or_([__.has("a"), __.has("b")])       // Any condition matches
.not_(__.has("deleted"))                // Negation
.where_(__.out("knows").count())        // Filter by sub-traversal
.local(__.limit(1))                     // Apply per-element
```

### Repeat Steps

```rust
.repeat(__.out("parent"))
    .times(3)                      // Fixed iterations
    .until(__.has_label("root"))  // Stop condition
    .emit()                        // Emit intermediates
    .emit_if(__.has("important")) // Conditional emit
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
use interstellar::traversal::p;

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
use interstellar::traversal::__;

// Use in branch steps
g.v().union([
    __.out("knows"),
    __.out("created"),
]);

// Use in predicates
g.v().where_(__.out("knows").count().is_(p::gt(3)));

// Use in repeat
g.v().repeat(__.out("parent")).until(__.has_label("root"));
```

## GQL (Graph Query Language)

Interstellar includes a full GQL implementation with SQL-like syntax for querying and mutating graphs. Enable with the `gql` feature:

```toml
[dependencies]
interstellar = { version = "0.1", features = ["gql"] }
```

### Basic Queries

```rust
use interstellar::prelude::*;

let snapshot = graph.snapshot();

// Pattern matching with MATCH
let results = snapshot.gql("
    MATCH (p:Person)-[:KNOWS]->(friend:Person)
    WHERE p.age > 25
    RETURN p.name, friend.name
").unwrap();

// Aggregation
let results = snapshot.gql("
    MATCH (p:Person)
    RETURN p.city, COUNT(*) AS count
    GROUP BY p.city
    ORDER BY count DESC
").unwrap();
```

### Mutations

```rust
use interstellar::gql::execute_mutation;

// Create vertices and edges
execute_mutation(&mut storage, "
    CREATE (alice:Person {name: 'Alice', age: 30})
    CREATE (bob:Person {name: 'Bob', age: 25})
    CREATE (alice)-[:KNOWS {since: 2020}]->(bob)
").unwrap();

// Update properties
execute_mutation(&mut storage, "
    MATCH (p:Person {name: 'Alice'})
    SET p.age = 31
").unwrap();

// MERGE (create if not exists)
execute_mutation(&mut storage, "
    MERGE (p:Person {name: 'Charlie'})
    ON CREATE SET p.created = true
    ON MATCH SET p.updated = true
").unwrap();
```

### Schema Definition (DDL)

Define vertex and edge types with validation:

```sql
-- Create vertex type with property constraints
CREATE NODE TYPE Person (
    name STRING NOT NULL,
    age INT,
    email STRING,
    active BOOL DEFAULT true
)

-- Create edge type with endpoint constraints
CREATE EDGE TYPE WORKS_AT (
    since INT,
    role STRING
) FROM Person TO Company

-- Set validation mode
SET SCHEMA VALIDATION STRICT
```

### Advanced Features

- **UNION / UNION ALL**: Combine query results
- **OPTIONAL MATCH**: Left outer join semantics
- **EXISTS / NOT EXISTS**: Subquery predicates
- **CASE expressions**: Conditional logic
- **List comprehensions**: `[x IN list WHERE x > 0 | x * 2]`
- **Pattern comprehension**: `[(p)-[:FRIEND]->(f) | f.name]`
- **FOREACH**: Iterate and mutate: `FOREACH (n IN nodes(path) | SET n.visited = true)`
- **REDUCE**: Fold over lists: `REDUCE(sum = 0, x IN list | sum + x)`
- **CALL subqueries**: Correlated and uncorrelated subqueries

## Gremlin Text Parser

Interstellar includes a TinkerPop-compatible Gremlin query string parser. Enable with the `gremlin` feature:

```toml
[dependencies]
interstellar = { version = "0.1", features = ["gremlin"] }
```

### Basic Queries

```rust
use interstellar::prelude::*;

let graph = Graph::new();
// ... add vertices and edges ...

// Execute Gremlin query strings directly
let result = graph.query("g.V().hasLabel('person').values('name').toList()").unwrap();

// Or use a snapshot
let snapshot = graph.snapshot();
let result = snapshot.query("g.V().has('age', P.gt(25)).count()").unwrap();
```

### Supported Steps

The parser supports the full range of Gremlin steps:

```gremlin
// Navigation
g.V().out('knows').in('created').both('uses')
g.V().outE('knows').inV().otherV()

// Filtering
g.V().hasLabel('person').has('age', P.gt(30))
g.V().has('name', P.within('Alice', 'Bob'))
g.V().has('email', TextP.containing('@gmail'))
g.V().where(__.out('knows').count().is(P.gt(3)))

// Transform
g.V().values('name', 'age')
g.V().valueMap(true)  // Include id and label
g.V().project('name', 'friends').by('name').by(__.out('knows').count())

// Branching
g.V().union(__.out('knows'), __.out('created'))
g.V().choose(__.hasLabel('person'), __.out('knows'), __.out('uses'))
g.V().coalesce(__.out('preferred'), __.out('default'))

// Repeat
g.V().repeat(__.out('parent')).times(3)
g.V().repeat(__.out()).until(__.hasLabel('root')).emit()

// Aggregation
g.V().hasLabel('person').order().by('age', desc).limit(10)
g.V().group().by('city')
g.V().groupCount().by('label')

// Side effects
g.V().as('a').out().as('b').select('a', 'b')
g.V().aggregate('x').out().where(P.within('x'))

// Terminal
g.V().toList()
g.V().next()
g.V().count()
g.V().hasNext()
```

### Math Expressions

When both `gremlin` and `gql` features are enabled, mathematical expressions are supported:

```rust
// Requires features = ["gremlin", "gql"]
let result = graph.query("g.V().values('age').math('_ * 2 + 5').toList()").unwrap();
```

### Predicates

Full predicate support including:

- **Comparison**: `P.eq()`, `P.neq()`, `P.gt()`, `P.gte()`, `P.lt()`, `P.lte()`
- **Range**: `P.between()`, `P.inside()`, `P.outside()`
- **Membership**: `P.within()`, `P.without()`
- **Text**: `TextP.containing()`, `TextP.startingWith()`, `TextP.endingWith()`, `TextP.regex()`
- **Logical**: `P.and()`, `P.or()`, `P.not()`

### Mutations

> **Note**: The `graph.query()` and `snapshot.query()` methods are **read-only**. Mutation queries (addV, addE, property, drop) will parse and compile but return placeholder values instead of executing.

For mutations, use the Rust fluent API:

```rust
use interstellar::prelude::*;
use std::sync::Arc;

let graph = Arc::new(Graph::new());

// Use gremlin() with Arc for write access
let g = graph.gremlin(Arc::clone(&graph));

// Mutations execute immediately
let alice = g.add_v("Person").property("name", "Alice").next();
let bob = g.add_v("Person").property("name", "Bob").next();

// Create edge between them
if let (Some(a), Some(b)) = (alice, bob) {
    g.v_id(a.id()).add_e("knows").to_id(b.id()).iterate();
}

// Read queries work normally
let count = g.v().count();  // 2
```

## Examples

Run the included examples:

```bash
# Gremlin-style traversals
cargo run --example quickstart_gremlin
cargo run --example marvel
cargo run --example nba --features mmap,gql

# GQL queries (requires gql feature)
cargo run --example quickstart_gql --features gql

# Persistence (requires mmap feature)
cargo run --example storage --features mmap
```

## Features

| Feature | Description | Default |
|---------|-------------|---------|
| `graphson` | GraphSON import/export | Yes |
| `mmap` | Memory-mapped persistent storage | No |
| `gql` | GQL query language | No |
| `gremlin` | Gremlin text query parser | No |
| `full-text` | Full-text search (Tantivy) | No |
| `wasm` | WebAssembly/JavaScript bindings | No |
| `full` | Enable all features (except wasm) | No |

In-memory graph storage is always available as core functionality.

### WebAssembly Support

Enable the `wasm` feature to compile Interstellar for browsers and Node.js:

```toml
[dependencies]
interstellar = { version = "0.1", features = ["wasm"] }
```

Build with wasm-pack:

```bash
wasm-pack build --target web --features wasm
```

Usage in JavaScript/TypeScript:

```typescript
import init, { Graph, P, __ } from 'interstellar-graph';

await init();

const graph = new Graph();
const alice = graph.addVertex('person', { name: 'Alice', age: 30n });
const bob = graph.addVertex('person', { name: 'Bob', age: 25n });
graph.addEdge(alice, bob, 'knows', { since: 2020n });

// Gremlin-style traversal
const friends = graph.V([alice])
    .outLabels(['knows'])
    .values('name')
    .toList();
console.log(friends); // ['Bob']

// With predicates
const adults = graph.V()
    .hasLabel('person')
    .hasWhere('age', P.gte(18n))
    .values('name')
    .toList();

// With anonymous traversals
const withFriends = graph.V()
    .hasLabel('person')
    .where_(__.out())  // Has at least one outgoing edge
    .toList();
```

See [spec-45-wasm-bindgen.md](specs/spec-45-wasm-bindgen.md) for full API documentation.

## Building

```bash
cargo build                          # Debug build
cargo build --release                # Release build
cargo build --features gremlin       # With Gremlin text parser
cargo build --features gql           # With GQL query language
cargo build --features mmap          # With mmap support
cargo build --features full          # All features
```

## Testing

```bash
cargo test                                      # Run default tests
cargo test --features gremlin                   # Include Gremlin parser tests
cargo test --features gql                       # Include GQL tests
cargo test --features mmap                      # Include mmap tests
cargo test --features "gremlin,gql,mmap"        # Run most tests (recommended)
cargo clippy -- -D warnings                     # Lint
cargo fmt --check                               # Check formatting
```

> **Note**: The `full-text` feature (tantivy) has a known upstream dependency conflict and is excluded from the full test suite.

### WASM Testing

The `wasm` feature requires browser or Node.js testing via wasm-pack:

```bash
# Install wasm-pack (one-time setup)
curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh

# Run tests in headless browser
wasm-pack test --headless --firefox --features wasm
wasm-pack test --headless --chrome --features wasm

# Run tests in Node.js
wasm-pack test --node --features wasm
```

## Benchmarks

```bash
cargo bench                          # Run traversal benchmarks
cargo bench --features mmap          # Include mmap benchmarks
```

## Formal Verification

Interstellar uses [Kani](https://github.com/model-checking/kani) for formal verification of critical code paths. Kani exhaustively checks all possible inputs within defined bounds, providing mathematical proofs of correctness.

### Verified Properties

- **Packed struct layout**: All `#[repr(C, packed)]` structs match their size constants
- **Serialization roundtrips**: FileHeader, NodeRecord, EdgeRecord serialize/deserialize correctly
- **Type conversions**: Value type conversions preserve data within safe ranges
- **Offset calculations**: File offset arithmetic cannot overflow for reasonable capacities
- **Data structure invariants**: FreeList and ArenaAllocator maintain their contracts

### Running Verification

```bash
# Install Kani (one-time setup)
cargo install --locked kani-verifier
kani setup

# Run all proofs
cargo kani

# Run specific proof
cargo kani --harness verify_node_record_roundtrip

# Run with verbose output
cargo kani --verbose
```

### Proof Harnesses

| Category | Proof | Description |
|----------|-------|-------------|
| Records | `verify_struct_sizes_match_constants` | All packed struct sizes match constants |
| Records | `verify_file_header_roundtrip` | FileHeader serialization roundtrip |
| Records | `verify_node_record_roundtrip` | NodeRecord serialization roundtrip |
| Records | `verify_edge_record_roundtrip` | EdgeRecord serialization roundtrip |
| Value | `verify_u64_to_value_safe_range` | Safe u64 to Value conversion |
| Value | `verify_i64_to_value` | i64 to Value conversion |
| Value | `verify_bool_to_value` | bool to Value conversion |
| Offset | `verify_node_offset_no_overflow` | Node offset calculation safety |
| Offset | `verify_edge_offset_no_overflow` | Edge offset calculation safety |
| FreeList | `verify_freelist_push_pop` | FreeList LIFO behavior |
| Arena | `verify_arena_allocation_bounded` | Arena bounds checking |

## Documentation

```bash
cargo doc --open                     # Build and view docs
```

See the [docs/](docs/) directory for comprehensive documentation:

- [Getting Started](docs/getting-started/) - Installation, quick start, and examples
- [API Reference](docs/api/) - Gremlin, GQL, and predicate reference
- [Concepts](docs/concepts/) - Architecture, storage, traversal model
- [Guides](docs/guides/) - Graph modeling, querying, mutations, performance
- [Reference](docs/reference/) - Value types, error handling, feature flags, glossary

## License

MIT

## Development Approach

This project uses **spec-driven development** with AI assistance. Most code is generated or reviewed by LLMs (primarily Claude Opus 4.5). While we aim for high quality and test coverage, this approach is experimental.
