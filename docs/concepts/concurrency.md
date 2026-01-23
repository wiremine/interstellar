# Concurrency

Interstellar provides thread-safe access to graphs with a reader-writer concurrency model.

## Overview

- **Multiple concurrent readers**: Many threads can query simultaneously
- **Single writer**: Only one thread can mutate at a time
- **Snapshot isolation**: Readers see a consistent view
- **No reader-writer blocking**: Readers don't block writers (and vice versa) beyond lock acquisition

---

## Core Types

### Graph

The main handle to a graph database:

```rust
use interstellar::graph::Graph;
use std::sync::Arc;

// Graph is Send + Sync, safe to share across threads
let graph = Arc::new(Graph::new(Arc::new(storage)));
```

### GraphSnapshot

A read-only view of the graph at a point in time:

```rust
let snapshot = graph.snapshot();  // Acquires read lock
let g = snapshot.traversal();

// Query the graph
let results = g.v().has_label("person").to_list();

// Snapshot is dropped here, releasing the lock
```

**Properties:**
- Holds a read lock for its lifetime
- Multiple snapshots can coexist
- Provides consistent view of graph state
- Cannot modify the graph

### GraphMut

Exclusive write access to the graph:

```rust
let mut gm = graph.mutate();  // Acquires write lock

// Add vertices
gm.add_v("person").property("name", "Alice").build();

// Commit changes
gm.commit()?;

// Lock released when gm is dropped
```

**Properties:**
- Holds a write lock for its lifetime
- Only one can exist at a time
- Blocks other writers
- Changes visible after commit

---

## Lock Semantics

### Read Lock (Snapshot)

```rust
// Thread 1
let snap1 = graph.snapshot();  // OK: First reader

// Thread 2 (concurrent)
let snap2 = graph.snapshot();  // OK: Multiple readers allowed

// Thread 3 (concurrent)
let snap3 = graph.snapshot();  // OK: Still allowed
```

### Write Lock (Mutate)

```rust
// Thread 1
let gm = graph.mutate();  // OK: Acquires write lock

// Thread 2 (concurrent)
let gm2 = graph.mutate();  // BLOCKS: Waits for Thread 1
```

### Non-Blocking Write Attempt

Use `try_mutate()` to attempt without blocking:

```rust
match graph.try_mutate() {
    Some(gm) => {
        // Got the lock
        gm.add_v("person").property("name", "Alice").build();
        gm.commit()?;
    }
    None => {
        // Lock held by another thread
        println!("Graph is busy, try again later");
    }
}
```

---

## Usage Patterns

### Read-Heavy Workload

For applications with many readers and occasional writes:

```rust
use std::sync::Arc;
use std::thread;

let graph = Arc::new(Graph::new(storage));

// Spawn many reader threads
let handles: Vec<_> = (0..10)
    .map(|i| {
        let g = Arc::clone(&graph);
        thread::spawn(move || {
            let snap = g.snapshot();
            let traversal = snap.traversal();
            traversal.v().has_label("person").count()
        })
    })
    .collect();

// All readers execute concurrently
for handle in handles {
    let count = handle.join().unwrap();
    println!("Count: {}", count);
}
```

### Writer Thread

Dedicate a thread or task to mutations:

```rust
use std::sync::mpsc;

let (tx, rx) = mpsc::channel::<Mutation>();
let graph = Arc::clone(&shared_graph);

// Writer thread
thread::spawn(move || {
    for mutation in rx {
        let gm = graph.mutate();
        match mutation {
            Mutation::AddVertex { label, props } => {
                gm.add_v(&label).properties(props).build();
            }
            // ... handle other mutations
        }
        gm.commit().unwrap();
    }
});

// Send mutations from other threads
tx.send(Mutation::AddVertex { 
    label: "person".into(), 
    props: HashMap::new() 
}).unwrap();
```

### Async Context

With async runtimes, use `spawn_blocking` for graph operations:

```rust
use tokio::task;

async fn query_graph(graph: Arc<Graph>) -> Vec<Value> {
    task::spawn_blocking(move || {
        let snap = graph.snapshot();
        let g = snap.traversal();
        g.v().has_label("person").to_list()
    })
    .await
    .unwrap()
}
```

---

## Snapshot Lifetime

Snapshots borrow the graph. Be mindful of lifetime:

```rust
// This works
fn query(graph: &Graph) -> Vec<Value> {
    let snap = graph.snapshot();
    let g = snap.traversal();
    g.v().to_list()  // Results owned, snapshot can be dropped
}

// This doesn't compile - snapshot can't outlive graph reference
fn bad_query<'a>(graph: &'a Graph) -> GraphSnapshot<'a> {
    graph.snapshot()  // Returned snapshot borrows graph
}
```

### Holding Snapshots

Avoid holding snapshots longer than necessary:

```rust
// Bad: Holds lock during I/O
let snap = graph.snapshot();
let results = snap.traversal().v().to_list();
expensive_network_call(&results)?;  // Still holding snapshot
drop(snap);  // Lock held too long

// Good: Release lock before I/O
let results = {
    let snap = graph.snapshot();
    snap.traversal().v().to_list()
};  // Snapshot dropped here
expensive_network_call(&results)?;  // No lock held
```

---

## Transaction Semantics

### Commit

Changes are visible after commit:

```rust
let gm = graph.mutate();
let id = gm.add_v("person").property("name", "Alice").build();
gm.commit()?;  // Changes now visible to new snapshots
```

### Rollback

Discard uncommitted changes:

```rust
let gm = graph.mutate();
gm.add_v("person").property("name", "Oops").build();
gm.rollback();  // Changes discarded
```

Implicit rollback on drop without commit:

```rust
{
    let gm = graph.mutate();
    gm.add_v("person").build();
    // No commit!
}  // gm dropped, changes discarded
```

---

## Thread Safety Summary

| Type | Send | Sync | Thread Usage |
|------|------|------|--------------|
| `Graph` | Yes | Yes | Share with `Arc` |
| `GraphSnapshot<'_>` | No | No | Use within one thread |
| `GraphMut<'_>` | No | No | Use within one thread |
| `Traversal` | No | No | Use within one thread |

### Sharing Pattern

```rust
// Create shared graph
let graph: Arc<Graph> = Arc::new(Graph::new(storage));

// Clone Arc for each thread
let g1 = Arc::clone(&graph);
let g2 = Arc::clone(&graph);

thread::spawn(move || {
    let snap = g1.snapshot();  // Create snapshot in thread
    // ... query
});

thread::spawn(move || {
    let snap = g2.snapshot();  // Create snapshot in thread
    // ... query
});
```

---

## MmapGraph Concurrency

MmapGraph has additional considerations:

### Process-Level Locking

MmapGraph uses file locks for cross-process safety:

```rust
// Process 1
let graph1 = MmapGraph::open("data.db")?;  // Acquires file lock

// Process 2
let graph2 = MmapGraph::open("data.db")?;  // Waits or fails
```

### Batch Mode

Batch mode is not thread-safe:

```rust
// Only use batch mode from a single thread
graph.begin_batch()?;
for i in 0..1000 {
    graph.add_vertex("node", props)?;
}
graph.commit_batch()?;
```

---

## Deadlock Prevention

Interstellar uses `parking_lot::RwLock` which prevents some deadlocks:

- Readers don't block readers
- Write lock requests don't starve (fair scheduling)
- `try_mutate()` available for non-blocking attempts

Avoid holding locks while waiting on external resources:

```rust
// Bad: potential deadlock with external systems
let snap = graph.snapshot();
external_service.wait_for_response()?;  // Holding lock!
let results = snap.traversal().v().to_list();

// Good: minimize lock duration
let data = {
    let snap = graph.snapshot();
    snap.traversal().v().to_list()
};
external_service.send(data)?;  // No lock held
```

---

## See Also

- [Architecture](architecture.md) - System overview
- [Storage Backends](storage-backends.md) - Backend-specific details
- [Performance Guide](../guides/performance.md) - Optimization tips
