# Mutations Guide

How to add, update, and delete data in Interstellar.

## Overview

Interstellar provides two ways to mutate graphs:

| Method | API | Best For |
|--------|-----|----------|
| Storage API | `storage.add_vertex()`, etc. | Bulk loading, programmatic access |
| GQL Mutations | `CREATE`, `SET`, `DELETE` | Declarative, pattern-based |

---

## Storage API

### Adding Vertices

Use the `props!` macro for concise property maps:

```rust
use interstellar::prelude::*;

let graph = Graph::new();

// Add a vertex with properties using the props! macro
let alice = graph.add_vertex("person", props! {
    "name" => "Alice",
    "age" => 30i64,
    "email" => "alice@example.com"
});

// Returns VertexId for later reference
println!("Created vertex: {:?}", alice);

// The props! macro automatically converts values to Value enum
// and keys to String. It's equivalent to:
// HashMap::from([
//     ("name".to_string(), Value::String("Alice".to_string())),
//     ("age".to_string(), Value::Int(30)),
//     ("email".to_string(), Value::String("alice@example.com".to_string())),
// ])

// For vertices without properties, use an empty props! or HashMap::new()
let anonymous = graph.add_vertex("person", props! {});
```

### Adding Edges

```rust
// Add edge with properties using props! macro
graph.add_edge(alice, bob, "knows", props! {
    "since" => 2020i64,
    "strength" => 0.9f64
})?;

// Add edge without properties
graph.add_edge(alice, project, "created", props! {})?;
```

### Updating Properties

```rust
// Update a vertex property
graph.set_vertex_property(alice, "age", Value::Int(31))?;

// Update an edge property
graph.set_edge_property(edge_id, "strength", Value::Float(0.95))?;
```

### Removing Properties

```rust
// Remove a property
graph.remove_vertex_property(alice, "temporary_field")?;
```

### Deleting Elements

```rust
// Delete an edge
graph.remove_edge(edge_id)?;

// Delete a vertex (must remove edges first)
graph.remove_vertex(vertex_id)?;

// Delete a vertex and all connected edges
graph.remove_vertex_with_edges(vertex_id)?;
```

---

## GQL Mutations

### CREATE

Create new vertices and edges:

```rust
use interstellar::gql::{parse_statement, execute_mutation};

// Create a vertex
let stmt = parse_statement("CREATE (n:person {name: 'Alice', age: 30})")?;
execute_mutation(&stmt, &mut storage)?;

// Create multiple vertices
let stmt = parse_statement("
    CREATE (a:person {name: 'Alice'}),
           (b:person {name: 'Bob'})
")?;
execute_mutation(&stmt, &mut storage)?;

// Create vertex and edge together
let stmt = parse_statement("
    CREATE (a:person {name: 'Alice'})-[:knows {since: 2020}]->(b:person {name: 'Bob'})
")?;
execute_mutation(&stmt, &mut storage)?;
```

### SET

Update properties on matched elements:

```rust
// Update a property
let stmt = parse_statement("
    MATCH (n:person {name: 'Alice'})
    SET n.age = 31
")?;
execute_mutation(&stmt, &mut storage)?;

// Update multiple properties
let stmt = parse_statement("
    MATCH (n:person {name: 'Alice'})
    SET n.age = 31, n.status = 'active'
")?;
execute_mutation(&stmt, &mut storage)?;

// Increment a value
let stmt = parse_statement("
    MATCH (n:person {name: 'Alice'})
    SET n.visit_count = n.visit_count + 1
")?;
execute_mutation(&stmt, &mut storage)?;
```

### REMOVE

Remove properties from elements:

```rust
let stmt = parse_statement("
    MATCH (n:person {name: 'Alice'})
    REMOVE n.temporary_field
")?;
execute_mutation(&stmt, &mut storage)?;
```

### DELETE

Delete matched elements:

```rust
// Delete edges
let stmt = parse_statement("
    MATCH (a:person)-[r:knows]->(b:person)
    WHERE r.strength < 0.5
    DELETE r
")?;
execute_mutation(&stmt, &mut storage)?;

// Delete vertices (must have no edges)
let stmt = parse_statement("
    MATCH (n:person {status: 'deleted'})
    DELETE n
")?;
execute_mutation(&stmt, &mut storage)?;
```

### DETACH DELETE

Delete vertices along with all connected edges:

```rust
let stmt = parse_statement("
    MATCH (n:person {name: 'Alice'})
    DETACH DELETE n
")?;
execute_mutation(&stmt, &mut storage)?;
```

### MERGE (Upsert)

Create if not exists, update if exists:

```rust
// Basic merge
let stmt = parse_statement("
    MERGE (n:person {email: 'alice@example.com'})
")?;
execute_mutation(&stmt, &mut storage)?;

// With ON CREATE / ON MATCH actions
let stmt = parse_statement("
    MERGE (n:person {email: 'alice@example.com'})
    ON CREATE SET n.created_at = 1234567890, n.visits = 1
    ON MATCH SET n.last_seen = 1234567890, n.visits = n.visits + 1
")?;
execute_mutation(&stmt, &mut storage)?;
```

---

## Bulk Loading

For loading large amounts of data, use batch mode:

### Graph (In-Memory)

```rust
let graph = Graph::new();

// Just add everything directly - it's already in-memory
for i in 0..100_000 {
    graph.add_vertex("node", props! {
        "index" => i as i64
    });
}
```

### MmapGraph (Batch Mode)

```rust
let graph = MmapGraph::open("data.db")?;

// Start batch mode (disables per-operation fsync)
graph.begin_batch()?;

for i in 0..100_000 {
    graph.add_vertex("node", props! {
        "index" => i as i64
    })?;
}

// Single fsync at the end
graph.commit_batch()?;
```

Batch mode provides ~500x speedup for bulk inserts.

---

## Transaction Patterns

### GraphMut API

For programmatic mutations with the graph wrapper:

```rust
let graph = Graph::new(Arc::new(storage));

// Acquire write lock
let mut gm = graph.mutate();

// Make changes
gm.add_v("person").property("name", "Alice").build();
gm.add_v("person").property("name", "Bob").build();

// Commit changes
gm.commit()?;
```

### Rollback

Discard uncommitted changes:

```rust
let mut gm = graph.mutate();
gm.add_v("person").property("name", "Oops").build();
gm.rollback();  // Changes discarded
```

Implicit rollback on drop without commit:

```rust
{
    let mut gm = graph.mutate();
    gm.add_v("person").build();
    // No commit called
}  // gm dropped, changes discarded
```

---

## Best Practices

### 1. Batch Related Changes

```rust
// Good: Single mutation with related changes
let stmt = parse_statement("
    CREATE (a:person {name: 'Alice'}),
           (b:person {name: 'Bob'}),
           (a)-[:knows]->(b)
")?;
execute_mutation(&stmt, &mut storage)?;

// Avoid: Multiple separate mutations
execute_mutation(&parse_statement("CREATE (a:person {name: 'Alice'})")?, &mut storage)?;
execute_mutation(&parse_statement("CREATE (b:person {name: 'Bob'})")?, &mut storage)?;
// Now how do we reference a and b for the edge?
```

### 2. Use MERGE for Idempotency

```rust
// Idempotent: Safe to run multiple times
"MERGE (n:person {email: 'alice@example.com'})"

// Not idempotent: Creates duplicates
"CREATE (n:person {email: 'alice@example.com'})"
```

### 3. Handle Missing Data

```rust
// MATCH returns nothing if no match, mutation doesn't execute
let stmt = parse_statement("
    MATCH (n:person {name: 'Nonexistent'})
    SET n.updated = true
")?;
// No error, just no changes made
execute_mutation(&stmt, &mut storage)?;
```

### 4. Validate Before Delete

```rust
// Use DETACH DELETE to avoid edge constraint errors
"MATCH (n) WHERE n.status = 'deleted' DETACH DELETE n"

// Or explicitly delete edges first
"MATCH (n)-[r]-() WHERE n.status = 'deleted' DELETE r"
"MATCH (n) WHERE n.status = 'deleted' DELETE n"
```

---

## Error Handling

### Common Errors

```rust
use interstellar::gql::MutationError;

match execute_mutation(&stmt, &mut storage) {
    Ok(results) => println!("Success: {:?}", results),
    Err(MutationError::VertexHasEdges(vid)) => {
        println!("Cannot delete {:?}: has edges. Use DETACH DELETE.", vid);
    }
    Err(MutationError::Storage(e)) => {
        println!("Storage error: {}", e);
    }
    Err(e) => println!("Error: {}", e),
}
```

### Vertex Not Found

```rust
// Attempting to add edge to non-existent vertex
storage.add_edge(
    VertexId(9999),  // Doesn't exist
    bob,
    "knows",
    HashMap::new(),
)?;  // Returns Err(StorageError::VertexNotFound)
```

---

## See Also

- [GQL API](../api/gql.md) - Complete GQL mutation syntax
- [Storage Backends](../concepts/storage-backends.md) - Batch mode details
- [Concurrency](../concepts/concurrency.md) - Thread-safe mutations
