# Error Handling

This document describes Interstellar's error types and patterns for handling errors gracefully.

## Philosophy

Interstellar follows Rust conventions:
- All fallible operations return `Result` types
- The library **never panics** in normal operation
- Error conditions are communicated through return values

## Error Types

### StorageError

Errors from storage backend operations.

```rust
use interstellar::prelude::*;

pub enum StorageError {
    VertexNotFound(VertexId),    // Vertex ID doesn't exist
    EdgeNotFound(EdgeId),        // Edge ID doesn't exist
    Io(std::io::Error),          // File system error
    WalCorrupted(String),        // Write-ahead log damaged
    InvalidFormat,               // File isn't a valid database
    CorruptedData,               // Data integrity failure
    OutOfSpace,                  // Storage capacity exceeded
    IndexError(String),          // Index constraint violation
}
```

#### When StorageErrors Occur

| Operation | Possible Errors |
|-----------|-----------------|
| Creating edges | `VertexNotFound` if source/target doesn't exist |
| Looking up elements | `VertexNotFound`, `EdgeNotFound` |
| Opening database | `Io`, `InvalidFormat`, `WalCorrupted` |
| Writing data | `Io`, `OutOfSpace` |
| Index operations | `IndexError` |

### TraversalError

Errors during graph traversals.

```rust
pub enum TraversalError {
    NotOne(usize),               // Expected exactly one result
    Storage(StorageError),       // Underlying storage failed
    Mutation(MutationError),     // Mutation step failed
}
```

**Note:** Most traversal operations are **infallible**. They return empty results rather than errors when elements don't match. Errors only occur for:

- `.one()` with 0 or 2+ results
- Storage failure during traversal
- Mutation failure during traversal

### MutationError

Errors during mutation operations (`addV`, `addE`, `property`, `drop`).

```rust
pub enum MutationError {
    EdgeSourceNotFound(VertexId),      // 'from' vertex doesn't exist
    EdgeTargetNotFound(VertexId),      // 'to' vertex doesn't exist
    MissingEdgeEndpoint(&'static str), // 'from' or 'to' not specified
    EmptyTraversalEndpoint,            // Endpoint traversal returned nothing
    AmbiguousTraversalEndpoint,        // Endpoint traversal returned multiple
    StepLabelNotFound(String),         // Referenced label doesn't exist
    StepLabelNotVertex(String),        // Label references non-vertex
    Storage(StorageError),             // Underlying storage failed
}
```

## Recovery Patterns

### Pattern 1: Match on Specific Errors

Use pattern matching when you need to handle different error cases differently:

```rust
use interstellar::prelude::*;
use interstellar::storage::InMemoryGraph;
use std::collections::HashMap;

let mut storage = InMemoryGraph::new();

let result = storage.add_edge(
    VertexId(999),  // doesn't exist
    VertexId(888),  // doesn't exist
    "knows",
    HashMap::new(),
);

match result {
    Ok(edge_id) => println!("Created edge: {:?}", edge_id),
    Err(StorageError::VertexNotFound(id)) => {
        println!("Cannot create edge: vertex {:?} doesn't exist", id);
    }
    Err(e) => {
        println!("Storage error: {}", e);
    }
}
```

### Pattern 2: Use the `?` Operator

For functions that return `Result`, use `?` for concise error propagation:

```rust
use interstellar::prelude::*;
use std::collections::HashMap;

fn setup_graph() -> Result<Graph, StorageError> {
    let graph = Graph::new();
    
    let alice = graph.add_vertex("person", HashMap::from([
        ("name".to_string(), Value::from("Alice")),
    ]));
    
    let bob = graph.add_vertex("person", HashMap::from([
        ("name".to_string(), Value::from("Bob")),
    ]));
    
    // The `?` propagates any error up to the caller
    graph.add_edge(alice, bob, "knows", HashMap::new())?;
    
    Ok(graph)
}
```

### Pattern 3: Provide Defaults

When you have a sensible default for error cases:

```rust
use interstellar::prelude::*;

let graph = Graph::in_memory();
let snapshot = graph.snapshot();
let g = snapshot.gremlin();

// Use next() which returns Option, then provide a default
let name = g.v()
    .has_label("person")
    .values("name")
    .next()
    .unwrap_or(Value::String("Unknown".to_string()));
```

### Pattern 4: Handle `.one()` Cardinality Errors

The `.one()` terminal step requires exactly one result:

```rust
use interstellar::prelude::*;

let graph = Graph::in_memory();
let snapshot = graph.snapshot();
let g = snapshot.gremlin();

match g.v().has_label("person").one() {
    Ok(vertex) => println!("Found: {:?}", vertex),
    Err(TraversalError::NotOne(0)) => {
        println!("No people found");
    }
    Err(TraversalError::NotOne(count)) => {
        println!("Expected 1, found {}. Add more filters.", count);
    }
    Err(e) => println!("Unexpected error: {}", e),
}
```

**Alternatives to `.one()`:**

| Method | Returns | Use When |
|--------|---------|----------|
| `.next()` | `Option<Value>` | You want the first result, if any |
| `.first()` | `Option<Value>` | Same as `next()` |
| `.to_list()` | `Vec<Value>` | You want all results |
| `.count()` | `usize` | You just need the count |
| `.has_next()` | `bool` | You just need to know if any exist |

### Pattern 5: Retry Logic for I/O Errors

For persistent storage, I/O errors may be transient:

```rust
use interstellar::prelude::*;
use std::thread;
use std::time::Duration;

fn open_with_retry(path: &str, max_retries: u32) -> Result<MmapGraph, StorageError> {
    let mut attempts = 0;
    
    loop {
        match MmapGraph::open(path) {
            Ok(graph) => return Ok(graph),
            Err(StorageError::Io(ref e)) if attempts < max_retries => {
                attempts += 1;
                let delay = Duration::from_millis(100 * 2_u64.pow(attempts));
                eprintln!("I/O error (attempt {}): {}. Retrying...", attempts, e);
                thread::sleep(delay);
            }
            Err(e) => return Err(e),
        }
    }
}
```

### Pattern 6: Graceful Degradation

Return empty results instead of errors when appropriate:

```rust
use interstellar::prelude::*;

fn get_user_friends(graph: &Graph, user_id: VertexId) -> Vec<Value> {
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();
    
    // Traversals handle missing vertices gracefully
    g.v_ids([user_id])
        .out_labels(&["knows"])
        .values("name")
        .to_list()
}

let graph = Graph::in_memory();

// Even with an invalid ID, this returns an empty Vec
let friends = get_user_friends(&graph, VertexId(999));
assert!(friends.is_empty());
```

## Error Conversion

Errors automatically convert between types using the `From` trait:

```rust
use interstellar::prelude::*;

// StorageError -> MutationError
let storage_err = StorageError::VertexNotFound(VertexId(1));
let mutation_err: MutationError = storage_err.into();

// MutationError -> TraversalError
let traversal_err: TraversalError = mutation_err.into();

// Direct: StorageError -> TraversalError
fn example() -> Result<(), TraversalError> {
    let err = StorageError::VertexNotFound(VertexId(1));
    Err(err)?;  // Automatically converts
    Ok(())
}
```

## Display and Debug

All error types implement `Display` and `Debug`:

```rust
use interstellar::prelude::*;

let error = StorageError::VertexNotFound(VertexId(42));

// For user-facing messages (Display)
println!("Error: {}", error);
// Output: "Error: vertex not found: VertexId(42)"

// For debugging (Debug)
println!("Debug: {:?}", error);
// Output: "Debug: VertexNotFound(VertexId(42))"
```

## Quick Reference

### Terminal Steps That Return Result

| Step | Returns | Error Condition |
|------|---------|-----------------|
| `.one()` | `Result<Value, TraversalError>` | Count != 1 |

### Terminal Steps That Never Error

| Step | Returns | Notes |
|------|---------|-------|
| `.to_list()` | `Vec<Value>` | May be empty |
| `.to_set()` | `HashSet<Value>` | May be empty |
| `.count()` | `usize` | May be 0 |
| `.sum()` | `Value` | Returns 0 for empty |
| `.min()` | `Option<Value>` | None if empty |
| `.max()` | `Option<Value>` | None if empty |
| `.next()` | `Option<Value>` | None if empty |
| `.first()` | `Option<Value>` | None if empty |
| `.has_next()` | `bool` | false if empty |
| `.iterate()` | `()` | Consumes iterator |

### Storage Operations That Return Result

| Operation | Returns |
|-----------|---------|
| `add_edge()` | `Result<EdgeId, StorageError>` |
| `MmapGraph::open()` | `Result<MmapGraph, StorageError>` |
| `begin_batch()` | `Result<(), StorageError>` |
| `commit_batch()` | `Result<(), StorageError>` |

### Storage Operations That Never Error

| Operation | Returns |
|-----------|---------|
| `add_vertex()` | `VertexId` |
| `get_vertex()` | `Option<Vertex>` |
| `get_edge()` | `Option<Edge>` |
| `InMemoryGraph::new()` | `InMemoryGraph` |

## See Also

- [Value Types](value-types.md) - The Value enum and type system
- [Performance Guide](../guides/performance.md) - Avoiding common error-prone patterns
