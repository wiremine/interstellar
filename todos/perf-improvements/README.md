# Performance Improvement Plan for Interstellar Graph Database

This document outlines identified performance issues and a plan to benchmark and address them.

## Executive Summary

The traversal engine has a fundamental design issue: **eager collection after every step** defeats the documented "lazy evaluation" model. This cascades into many downstream performance problems.

## Critical Issues

### 1. Eager Collection Defeats Lazy Evaluation (CRITICAL)

**Location:** `src/traversal/source.rs:2864-2877`

```rust
// Collects after EACH step - defeats lazy evaluation
for step in &steps {
    current = step.apply(&ctx, Box::new(current.into_iter())).collect();
}
```

**Impact:** 
- O(N × steps) allocations for any traversal
- `v().out().out().count()` on 10K vertices with 50K edges creates millions of intermediate allocations
- Memory usage scales with traversal length, not result size

**Benchmark to add:**
```rust
fn bench_step_count_scaling(c: &mut Criterion) {
    let graph = create_benchmark_graph(1_000, 5_000);
    let mut group = c.benchmark_group("step_count_scaling");
    
    // Compare traversal time as we add more steps
    group.bench_function("v().count()", |b| ...);
    group.bench_function("v().out().count()", |b| ...);
    group.bench_function("v().out().out().count()", |b| ...);
    group.bench_function("v().out().out().out().count()", |b| ...);
}
```

---

### 2. Terminal Steps Don't Short-Circuit (HIGH)

**Location:** `src/traversal/source.rs:3006-3008`

```rust
pub fn count(self) -> u64 {
    self.execute().len() as u64  // Materializes everything just to count
}
```

**Also affects:** `sum()`, `min()`, `max()`, `mean()`, `first()`, `next()`

**Benchmark to add:**
```rust
fn bench_count_vs_direct(c: &mut Criterion) {
    let graph = create_benchmark_graph(100_000, 0);
    let mut group = c.benchmark_group("count_comparison");
    
    group.bench_function("g.v().count()", |b| {
        b.iter(|| graph.snapshot().gremlin().v().count())
    });
    
    group.bench_function("storage.vertex_count()", |b| {
        b.iter(|| graph.snapshot().vertex_count())
    });
}
```

**See:** `perf-improvements/001-count-step-optimization.md`

---

### 3. Label String Comparison Instead of ID Comparison (HIGH)

**Location:** `src/traversal/filter.rs:85-106`

```rust
self.labels.iter().any(|l| l == &vertex.label)  // String comparison
```

The interner exists to avoid this, but `HasLabelStep` compares strings instead of interned IDs.

**Fix:** Store label_ids in HasLabelStep, compare against vertex.label_id

**Benchmark to add:**
```rust
fn bench_has_label_scaling(c: &mut Criterion) {
    // Create graph with many unique labels
    let mut group = c.benchmark_group("has_label_comparison");
    
    for num_labels in [1, 10, 100] {
        group.bench_function(format!("has_label_{}_labels", num_labels), |b| ...);
    }
}
```

---

### 4. Boxed Iterator Overhead (HIGH)

**Location:** `src/storage/mod.rs:279-349`

```rust
fn out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_>;
fn all_vertices(&self) -> Box<dyn Iterator<Item = Vertex> + '_>;
// ... 8 more methods
```

Every storage call allocates a boxed iterator. Navigation steps compound this with nested boxes.

**Benchmark to add:**
```rust
fn bench_boxed_vs_concrete_iterator(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 50_000);
    let mut group = c.benchmark_group("iterator_overhead");
    
    // Direct iteration (if we add a concrete path)
    group.bench_function("direct_vertex_iteration", |b| ...);
    
    // Boxed (current)
    group.bench_function("boxed_vertex_iteration", |b| {
        b.iter(|| graph.snapshot().all_vertices().count())
    });
}
```

---

### 5. Label Vec Allocation in Navigation Steps (MEDIUM)

**Location:** `src/traversal/source.rs:1242, 1276, 1306, 1336, 1366, 1396`

```rust
let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
```

Every navigation step with labels allocates a new Vec of Strings.

**Fix:** Accept `&[&str]` and intern labels once at step creation.

---

### 6. Traverser Cloning in Repeat Step (MEDIUM)

**Location:** `src/traversal/repeat.rs:324, 354, 513`

```rust
let sub_input = Box::new(std::iter::once(traverser.clone()));
```

Every traverser is cloned when checking until/emit conditions.

**Fix:** Pass by reference for condition checking, only clone when emitting.

---

### 7. Lock Contention in Snapshot Operations (MEDIUM)

**Location:** `src/storage/cow.rs:578-579, 608-610`

```rust
let indexes = self.indexes.read();
let state = self.state.read();
// ... later in the same method:
let state = self.state.read();  // Acquires again
```

Multiple lock acquisitions within single methods.

**Benchmark to add:**
```rust
fn bench_concurrent_reads(c: &mut Criterion) {
    let graph = Arc::new(create_benchmark_graph(10_000, 50_000));
    
    // Single-threaded baseline
    c.bench_function("single_thread_reads", |b| ...);
    
    // Multi-threaded with contention
    c.bench_function("multi_thread_reads", |b| ...);
}
```

---

### 8. String Allocation in Edge/Vertex Retrieval (MEDIUM)

**Location:** `src/storage/cow.rs:2866, 2881`

```rust
let label = self.interner_snapshot.resolve(node.label_id)?.to_string();
```

Every vertex/edge lookup converts label_id to an owned String.

**Fix:** Return label_id and resolve lazily, or use `Cow<str>`.

---

## Benchmark Plan

### Phase 1: Baseline Measurements (Create `benches/perf_analysis.rs`)

```rust
// 1. Step scaling - how does time grow with step count?
fn bench_step_count_scaling(c: &mut Criterion);

// 2. Count optimization potential
fn bench_count_vs_direct(c: &mut Criterion);

// 3. Label comparison overhead
fn bench_has_label_scaling(c: &mut Criterion);

// 4. Iterator boxing overhead (if measurable)
fn bench_iterator_allocation(c: &mut Criterion);

// 5. Navigation step overhead
fn bench_navigation_step_overhead(c: &mut Criterion);

// 6. Concurrent access patterns
fn bench_concurrent_reads(c: &mut Criterion);
```

### Phase 2: Fix Critical Issues

1. **Implement CountStep** - Immediate 10-20x improvement for `count()`
2. **Fix HasLabelStep** - Use interned IDs instead of string comparison
3. **Lazy executor prototype** - Prove streaming is possible

### Phase 3: Structural Improvements

1. **Step fusion** - Detect and optimize common patterns
2. **Enum dispatch** - Replace Box<dyn AnyStep> for common steps
3. **Label ID caching** - Intern labels at step creation time

---

## Files to Modify

| File | Changes |
|------|---------|
| `src/traversal/source.rs` | Add CountStep, lazy execution path |
| `src/traversal/filter.rs` | Use label IDs in HasLabelStep |
| `src/traversal/step.rs` | Consider enum dispatch for hot steps |
| `src/traversal/navigation.rs` | Pre-intern label filters |
| `src/storage/mod.rs` | Consider concrete iterator types |
| `benches/perf_analysis.rs` | New benchmark file for analysis |

---

## Success Metrics

| Metric | Current | Target |
|--------|---------|--------|
| `v().count()` on 10K vertices | ~2.0 ms | < 0.2 ms |
| `v().has_label("X").count()` | ~2.5 ms | < 0.3 ms |
| `v().out().out().count()` | ~50 ms | < 10 ms |
| Step count scaling | O(steps × N) | O(N) |
| Memory for `count()` | O(N) | O(1) |
