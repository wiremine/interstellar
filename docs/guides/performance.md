# Performance Guide

Optimize your graph queries and storage for best performance.

## Query Optimization

### 1. Filter Early

Apply filters as early as possible to reduce the number of elements processed:

```rust
// Good: Filter before traversal
g.v()
    .has_label("person")      // Filter to persons first
    .has_value("active", true)
    .out("knows")
    .to_list()

// Slower: Filter after traversal
g.v()
    .out("knows")             // Traverse from ALL vertices
    .has_label("person")
    .has_value("active", true)
    .to_list()
```

### 2. Use Specific Start Points

Start from known IDs rather than scanning:

```rust
// Fast: Direct ID lookup O(1)
g.v_ids([known_id]).out("knows").to_list()

// Slow: Full scan O(n)
g.v().has_value("id", known_id).out("knows").to_list()
```

### 3. Limit Results

Stop traversal early when you only need some results:

```rust
// Get just the first 10
g.v().has_label("person").limit(10).to_list()

// Check if any exist (stops at first match)
g.v().has_label("admin").has_next()

// Get first match only
g.v().has_label("person").next()
```

### 4. Use Label Indexes

Labels are indexed. Filter by label first:

```rust
// Uses label index
g.v().has_label("person").has_value("name", "Alice")

// Scans all vertices
g.v().has_value("name", "Alice").has_label("person")
```

### 5. Avoid Unnecessary Dedup

`dedup()` must track all seen values. Only use when needed:

```rust
// Only dedup if you expect duplicates
g.v().out("knows").out("knows").dedup().to_list()

// No dedup needed for unique results
g.v().has_label("person").to_list()
```

### 6. Minimize Path Tracking

Path tracking has memory overhead. Only enable when needed:

```rust
// Enables path tracking (slower)
g.v().as_("a").out().as_("b").select(["a", "b"])

// No path tracking (faster)
g.v().out().to_list()
```

---

## Storage Optimization

### Batch Loading

For bulk inserts, use batch mode with MmapGraph:

```rust
// Without batch mode: ~1ms per insert (fsync each)
for i in 0..10_000 {
    graph.add_vertex("node", props)?;  // 10 seconds total
}

// With batch mode: ~100ns per insert + one fsync
graph.begin_batch()?;
for i in 0..10_000 {
    graph.add_vertex("node", props)?;  // ~1ms total
}
graph.commit_batch()?;  // ~1ms for final fsync
```

Batch mode provides **~500x speedup** for bulk operations.

### Choose the Right Backend

| Scenario | Recommended |
|----------|-------------|
| < 100K elements | InMemoryGraph |
| > 100K elements | MmapGraph |
| Need persistence | MmapGraph |
| Maximum query speed | InMemoryGraph |
| Limited RAM | MmapGraph |

### Compact MmapGraph

Periodically compact to reclaim space from deleted elements:

```rust
graph.compact()?;
```

---

## Memory Optimization

### Property Design

Keep properties small:

```rust
// Good: Small, typed values
props.insert("age".into(), Value::Int(30));

// Avoid: Large embedded data
props.insert("bio".into(), Value::String(/* 10KB text */));
```

Consider separate vertices for large content:

```rust
// Better: Reference large content
let bio_vertex = storage.add_vertex("content", HashMap::from([
    ("text".into(), large_text),
]));
storage.add_edge(person, bio_vertex, "has_bio", HashMap::new());
```

### String Interning

String labels are automatically interned. Use consistent label names:

```rust
// Good: Same label string reused
storage.add_vertex("person", props1);  // "person" interned once
storage.add_vertex("person", props2);  // Reuses interned string

// Avoid: Dynamic label generation
storage.add_vertex(&format!("person_{}", i), props);  // Each unique
```

---

## Concurrency Optimization

### Minimize Lock Duration

Release snapshots quickly:

```rust
// Good: Release lock before I/O
let results = {
    let snap = graph.snapshot();
    snap.traversal().v().to_list()
};  // Lock released
expensive_io_operation(&results)?;

// Avoid: Hold lock during I/O
let snap = graph.snapshot();
let results = snap.traversal().v().to_list();
expensive_io_operation(&results)?;  // Still holding lock!
```

### Parallel Reads

Multiple threads can query simultaneously:

```rust
let handles: Vec<_> = (0..num_threads)
    .map(|_| {
        let g = Arc::clone(&graph);
        thread::spawn(move || {
            let snap = g.snapshot();
            snap.traversal().v().count()
        })
    })
    .collect();
```

### Batch Writes

Combine mutations to reduce lock contention:

```rust
// Good: Single write lock, multiple changes
let mut gm = graph.mutate();
for data in items {
    gm.add_v("item").property("data", data).build();
}
gm.commit()?;

// Avoid: Acquire lock for each change
for data in items {
    let mut gm = graph.mutate();
    gm.add_v("item").property("data", data).build();
    gm.commit()?;
}
```

---

## Query Pattern Optimization

### Short-Circuit Patterns

```rust
// Stops at first match
g.v().has_label("admin").has_next()

// Better than
g.v().has_label("admin").count() > 0
```

### Avoid Cartesian Products

```rust
// Avoid: Combines all persons with all companies
g.v().has_label("person")
    .v().has_label("company")  // Cartesian product!
    .to_list()

// Better: Start from one, traverse to related
g.v().has_label("person")
    .out("works_at")
    .has_label("company")
    .to_list()
```

### Use Coalesce for Fallbacks

```rust
// Efficient: Stops at first non-empty
g.v().coalesce([
    __.has("preferred_name").values("preferred_name"),
    __.values("name"),
])
```

---

## Benchmarking

### Built-in Profiling

```rust
let profile = g.v()
    .has_label("person")
    .out("knows")
    .profile();
    
println!("{}", profile);
```

### Criterion Benchmarks

```bash
cargo bench --features mmap
```

### Simple Timing

```rust
use std::time::Instant;

let start = Instant::now();
let results = g.v().has_label("person").out("knows").to_list();
println!("Query took {:?}", start.elapsed());
```

---

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Vertex by ID | O(1) | Direct lookup |
| Edge by ID | O(1) | Direct lookup |
| Vertices by label | O(n) | n = vertices with label |
| Get all vertices | O(V) | Full scan |
| Traverse edge | O(1) | Direct lookup |
| Get neighbors | O(degree) | Adjacency list |
| Add vertex | O(1) amortized | May resize |
| Add edge | O(1) | List append |
| Delete edge | O(1) | Mark deleted |
| Delete vertex | O(degree) | Must update edges |

---

## Checklist

Before deploying to production:

- [ ] Filter by label before property filters
- [ ] Use specific start points (IDs) when possible
- [ ] Apply `limit()` for pagination
- [ ] Use batch mode for bulk loading
- [ ] Choose appropriate storage backend
- [ ] Test with production-sized data
- [ ] Profile slow queries
- [ ] Minimize snapshot hold time

---

## See Also

- [Storage Backends](../concepts/storage-backends.md) - Backend comparison
- [Traversal Model](../concepts/traversal-model.md) - How queries execute
- [Concurrency](../concepts/concurrency.md) - Thread safety
