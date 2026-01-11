# Spec 16: Side Effect Steps

**Phase 16 of RustGremlin Implementation**

## Overview

This specification defines the implementation of Gremlin side effect steps for RustGremlin. Side effect steps perform operations that don't change the traversal stream but produce side effects such as storing values in collections, building subgraphs, or capturing aggregated data.

Side effect steps are unique in that they:
1. Pass traversers through unchanged (transparent to the stream)
2. Accumulate data in the `ExecutionContext.side_effects` storage
3. Enable later retrieval via `cap()` or post-traversal access

**Duration**: 4-6 days  
**Priority**: Medium  
**Dependencies**: Phase 3 (Traversal Engine Core), existing `SideEffects` infrastructure in `context.rs`

---

## Goals

1. Implement `sideEffect(traversal)` - Execute a traversal for side effects only
2. Implement `aggregate(sideEffectKey)` - Lazily store all traverser values into a collection
3. Implement `store(sideEffectKey)` - Eagerly store each traverser value into a collection
4. Implement `cap(sideEffectKeys...)` - Retrieve accumulated side effect data
5. Implement `profile()` - Collect traversal profiling/timing information (optional)
6. Ensure all steps integrate with the anonymous traversal `__` module
7. Provide comprehensive test coverage

## Non-Goals

1. `subgraph()` - Complex graph construction requiring mutable storage access (future work)
2. `sack()` / `withSack()` - Traverser-local storage (requires traverser redesign)
3. `barrier()` - Explicit synchronization (implicit in reduce steps)

---

## TinkerPop Reference

### sideEffect() Step

The `sideEffect()` step executes a traversal for its side effects without affecting the main stream.

```groovy
// Gremlin
g.V().sideEffect(outE().count().store("edgeCounts")).values("name")
```

**Semantics:**
1. For each input traverser, execute the side-effect traversal
2. Discard the side-effect traversal's output
3. Pass the original traverser through unchanged
4. Side-effect traversal can modify `SideEffects` storage

### aggregate() Step

The `aggregate()` step collects all traverser values into a named collection (barrier step).

```groovy
// Gremlin
g.V().aggregate("x").out().where(within("x"))
```

**Semantics:**
1. **Barrier step**: Collects ALL input traversers before continuing
2. Stores all values in the named side-effect collection
3. Passes all traversers through (after collection is complete)
4. Enables forward references in traversal (e.g., `where(within("x"))`)

### store() Step

The `store()` step eagerly stores each traverser value into a named collection (lazy/streaming).

```groovy
// Gremlin
g.V().store("all").out().store("neighbors")
```

**Semantics:**
1. **Lazy step**: Stores each value as it passes through
2. Passes each traverser through immediately
3. Collection grows incrementally during traversal
4. Useful for accumulating values without blocking

### cap() Step

The `cap()` step retrieves accumulated side effect data, terminating the current stream.

```groovy
// Gremlin
g.V().aggregate("x").cap("x")           // Returns the collection
g.V().groupCount().by("name").cap()     // Cap implicit group
```

**Semantics:**
1. Terminates the current traverser stream
2. Retrieves specified side-effect collections
3. Returns `Value::List` for single key, `Value::Map` for multiple keys
4. Consumes the stream to ensure all side effects are populated

### profile() Step

The `profile()` step collects timing and metrics about traversal execution.

```groovy
// Gremlin
g.V().out().profile()
```

**Semantics:**
1. Collects metrics during traversal execution
2. Returns profiling data (step timings, counts, etc.)
3. Primarily a debugging/optimization tool

---

## Module Structure

This phase adds/modifies the following files:

| File | Description |
|------|-------------|
| `src/traversal/sideeffect.rs` | New module with `SideEffectStep`, `AggregateStep`, `StoreStep`, `CapStep`, `ProfileStep` |
| `src/traversal/source.rs` | Add builder methods for side effect steps |
| `src/traversal/mod.rs` | Module declaration and re-exports, `__` factory functions |
| `src/traversal/context.rs` | Potential enhancements to `SideEffects` |

---

## Deliverables

### 1. SideEffectStep

Executes a sub-traversal for side effects without affecting the main stream:

```rust
use crate::traversal::context::ExecutionContext;
use crate::traversal::step::{execute_traversal_from, AnyStep};
use crate::traversal::{Traversal, Traverser};
use crate::value::Value;

/// Execute a traversal for side effects only
/// 
/// The sub-traversal is executed for each input traverser, but its output
/// is discarded. The original traverser passes through unchanged.
/// 
/// # Example
/// 
/// ```ignore
/// // Store edge counts as a side effect
/// g.v().side_effect(__::out_e().count().store("edge_counts"))
///      .values("name")
///      .to_list()
/// ```
#[derive(Clone)]
pub struct SideEffectStep {
    /// Traversal to execute for side effects
    side_traversal: Traversal<Value, Value>,
}

impl SideEffectStep {
    /// Create a new SideEffectStep
    pub fn new(side_traversal: Traversal<Value, Value>) -> Self {
        Self { side_traversal }
    }
}

impl AnyStep for SideEffectStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let side_traversal = self.side_traversal.clone();
        
        Box::new(input.map(move |t| {
            // Execute side-effect traversal (discard results)
            let side_input = Box::new(std::iter::once(t.clone()));
            let _ = execute_traversal_from(ctx, &side_traversal, side_input)
                .for_each(|_| {}); // Consume iterator
            
            // Return original traverser unchanged
            t
        }))
    }
    
    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }
    
    fn name(&self) -> &'static str {
        "sideEffect"
    }
}
```

### 2. StoreStep

Eagerly stores each traverser value into a named collection:

```rust
/// Store each traverser value into a named side-effect collection (lazy)
/// 
/// Unlike `aggregate()`, `store()` is not a barrier - values are stored
/// as they pass through, and traversers continue immediately.
/// 
/// # Example
/// 
/// ```ignore
/// g.v().store("all")
///      .out().store("neighbors")
///      .to_list()
/// // Access stored values:
/// // ctx.side_effects.get("all")
/// // ctx.side_effects.get("neighbors")
/// ```
#[derive(Clone, Debug)]
pub struct StoreStep {
    /// The side-effect key to store values under
    key: String,
}

impl StoreStep {
    /// Create a new StoreStep
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl AnyStep for StoreStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let key = self.key.clone();
        
        Box::new(input.map(move |t| {
            // Store the current value
            ctx.side_effects.store(&key, t.value.clone());
            // Pass through unchanged
            t
        }))
    }
    
    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }
    
    fn name(&self) -> &'static str {
        "store"
    }
}
```

### 3. AggregateStep

Collects all traverser values into a named collection (barrier step):

```rust
/// Collect all traverser values into a named side-effect collection (barrier)
/// 
/// This is a **barrier step** - it collects ALL input traversers before
/// allowing any to continue. This enables forward references to the
/// collected values later in the traversal.
/// 
/// # Example
/// 
/// ```ignore
/// // Collect all start vertices, then find neighbors not in original set
/// g.v().has_label("person")
///      .aggregate("start")
///      .out("knows")
///      .where_(p::without(select("start")))
///      .to_list()
/// ```
#[derive(Clone, Debug)]
pub struct AggregateStep {
    /// The side-effect key to aggregate values under
    key: String,
}

impl AggregateStep {
    /// Create a new AggregateStep
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl AnyStep for AggregateStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Barrier: collect all traversers first
        let traversers: Vec<Traverser> = input.collect();
        
        // Store all values in the side-effect collection
        for t in &traversers {
            ctx.side_effects.store(&self.key, t.value.clone());
        }
        
        // Re-emit all traversers
        Box::new(traversers.into_iter())
    }
    
    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }
    
    fn name(&self) -> &'static str {
        "aggregate"
    }
}
```

### 4. CapStep

Retrieves accumulated side effect data:

```rust
/// Retrieve accumulated side-effect data
/// 
/// The `cap()` step terminates the current traverser stream and returns
/// the accumulated side-effect data for the specified keys.
/// 
/// # Behavior
/// 
/// - Single key: Returns `Value::List` containing all stored values
/// - Multiple keys: Returns `Value::Map` with keys mapped to their lists
/// - Non-existent keys return empty lists
/// 
/// # Example
/// 
/// ```ignore
/// // Get the aggregated collection
/// let collected = g.v().aggregate("x").cap("x").next();
/// // Returns: Value::List([...all vertex values...])
/// 
/// // Get multiple collections as a map
/// let data = g.v().store("a").out().store("b").cap_multi(&["a", "b"]).next();
/// // Returns: Value::Map({"a": [...], "b": [...]})
/// ```
#[derive(Clone, Debug)]
pub struct CapStep {
    /// The side-effect key(s) to retrieve
    keys: Vec<String>,
}

impl CapStep {
    /// Create a CapStep for a single key
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }
    
    /// Create a CapStep for multiple keys
    pub fn multi(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl AnyStep for CapStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Consume the input stream to ensure all side effects are populated
        input.for_each(|_| {});
        
        // Retrieve the side-effect data
        let result = if self.keys.len() == 1 {
            // Single key: return the list directly
            let values = ctx.side_effects.get(&self.keys[0])
                .unwrap_or_default();
            Value::List(values)
        } else {
            // Multiple keys: return a map
            let mut map = std::collections::HashMap::new();
            for key in &self.keys {
                let values = ctx.side_effects.get(key)
                    .unwrap_or_default();
                map.insert(key.clone(), Value::List(values));
            }
            Value::Map(map)
        };
        
        // Return single traverser with the result
        Box::new(std::iter::once(Traverser::new(result)))
    }
    
    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }
    
    fn name(&self) -> &'static str {
        "cap"
    }
}
```

### 5. ProfileStep (Optional)

Collects traversal profiling information:

```rust
use std::time::Instant;
use std::cell::Cell;

/// Collect traversal profiling information
/// 
/// The `profile()` step collects timing and count metrics for the traversal
/// and stores them in side effects. This is primarily a debugging tool.
/// 
/// # Metrics Collected
/// 
/// - `count`: Number of traversers processed
/// - `time_ms`: Time spent processing (milliseconds)
/// 
/// # Example
/// 
/// ```ignore
/// let profile_data = g.v().out().profile("step1").to_list();
/// let metrics = ctx.side_effects.get("step1_profile");
/// ```
#[derive(Clone, Debug)]
pub struct ProfileStep {
    /// Optional profile key (defaults to auto-generated)
    key: Option<String>,
}

impl ProfileStep {
    /// Create a new ProfileStep with auto-generated key
    pub fn new() -> Self {
        Self { key: None }
    }
    
    /// Create a ProfileStep with a specific key
    pub fn with_key(key: impl Into<String>) -> Self {
        Self { key: Some(key.into()) }
    }
}

impl Default for ProfileStep {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyStep for ProfileStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let key = self.key.clone()
            .unwrap_or_else(|| "profile".to_string());
        let count = Cell::new(0u64);
        let start = Instant::now();
        
        // Wrap the iterator to count and time
        let key_clone = key.clone();
        Box::new(ProfileIterator {
            inner: input,
            ctx,
            key: key_clone,
            count,
            start,
            finished: Cell::new(false),
        })
    }
    
    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }
    
    fn name(&self) -> &'static str {
        "profile"
    }
}

/// Iterator wrapper for profiling
struct ProfileIterator<'a, I> {
    inner: I,
    ctx: &'a ExecutionContext<'a>,
    key: String,
    count: Cell<u64>,
    start: Instant,
    finished: Cell<bool>,
}

impl<'a, I: Iterator<Item = Traverser>> Iterator for ProfileIterator<'a, I> {
    type Item = Traverser;
    
    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(t) => {
                self.count.set(self.count.get() + 1);
                Some(t)
            }
            None => {
                // Store profile data on completion
                if !self.finished.get() {
                    self.finished.set(true);
                    let elapsed = self.start.elapsed();
                    let profile_data = Value::Map({
                        let mut map = std::collections::HashMap::new();
                        map.insert("count".to_string(), Value::Int(self.count.get() as i64));
                        map.insert("time_ms".to_string(), Value::Float(elapsed.as_secs_f64() * 1000.0));
                        map
                    });
                    self.ctx.side_effects.store(&self.key, profile_data);
                }
                None
            }
        }
    }
}
```

### 6. Builder Methods on BoundTraversal

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Execute a traversal for side effects only
    /// 
    /// The sub-traversal is executed for each traverser, but its output
    /// is discarded. The original traversers pass through unchanged.
    /// 
    /// # Example
    /// 
    /// ```rust
    /// g.v().side_effect(__::out_e().count().store("edge_counts"))
    ///      .values("name")
    ///      .to_list()
    /// ```
    pub fn side_effect(self, traversal: Traversal<Value, Value>) -> BoundTraversal<'g, In, Out> {
        // ... add SideEffectStep
    }
    
    /// Store each traverser value into a named collection (lazy)
    /// 
    /// Values are stored as they pass through. This is not a barrier step.
    /// 
    /// # Example
    /// 
    /// ```rust
    /// g.v().store("vertices").out().store("neighbors").to_list()
    /// ```
    pub fn store(self, key: impl Into<String>) -> BoundTraversal<'g, In, Out> {
        // ... add StoreStep
    }
    
    /// Aggregate all traverser values into a named collection (barrier)
    /// 
    /// This is a barrier step - all traversers are collected before any
    /// continue. Enables forward references to the collected data.
    /// 
    /// # Example
    /// 
    /// ```rust
    /// g.v().aggregate("all").out().where_(p::without(select("all")))
    /// ```
    pub fn aggregate(self, key: impl Into<String>) -> BoundTraversal<'g, In, Out> {
        // ... add AggregateStep
    }
    
    /// Retrieve accumulated side-effect data (single key)
    /// 
    /// Terminates the current stream and returns the stored collection.
    /// 
    /// # Example
    /// 
    /// ```rust
    /// let collected = g.v().aggregate("x").cap("x").next();
    /// ```
    pub fn cap(self, key: impl Into<String>) -> BoundTraversal<'g, In, Value> {
        // ... add CapStep
    }
    
    /// Retrieve accumulated side-effect data (multiple keys)
    /// 
    /// Returns a map of key -> collection pairs.
    /// 
    /// # Example
    /// 
    /// ```rust
    /// let data = g.v().store("a").out().store("b").cap_multi(&["a", "b"]).next();
    /// ```
    pub fn cap_multi<I, S>(self, keys: I) -> BoundTraversal<'g, In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        // ... add CapStep::multi
    }
    
    /// Collect traversal profiling information
    /// 
    /// Stores timing and count metrics in side effects.
    pub fn profile(self) -> BoundTraversal<'g, In, Out> {
        // ... add ProfileStep
    }
    
    /// Collect traversal profiling information with a named key
    pub fn profile_as(self, key: impl Into<String>) -> BoundTraversal<'g, In, Out> {
        // ... add ProfileStep::with_key
    }
}
```

### 7. Anonymous Traversal Factory

```rust
// In src/traversal/mod.rs, within the __ module

/// Execute a traversal for side effects only
pub fn side_effect(traversal: Traversal<Value, Value>) -> Traversal<Value, Value> {
    Traversal::new().add_step(SideEffectStep::new(traversal))
}

/// Store each value into a named collection (lazy)
pub fn store(key: impl Into<String>) -> Traversal<Value, Value> {
    Traversal::new().add_step(StoreStep::new(key))
}

/// Aggregate all values into a named collection (barrier)
pub fn aggregate(key: impl Into<String>) -> Traversal<Value, Value> {
    Traversal::new().add_step(AggregateStep::new(key))
}

/// Retrieve accumulated side-effect data
pub fn cap(key: impl Into<String>) -> Traversal<Value, Value> {
    Traversal::new().add_step(CapStep::new(key))
}

/// Collect profiling information
pub fn profile() -> Traversal<Value, Value> {
    Traversal::new().add_step(ProfileStep::new())
}
```

---

## Execution Flow

### Example: store() vs aggregate()

```
Input: [Alice, Bob, Carol] (stream of vertices)

─────────────────────────────────────────────────────
store("x") - Lazy (streaming)
─────────────────────────────────────────────────────

Time 0: Alice arrives
  -> Store Alice in "x"
  -> Alice continues downstream immediately
  -> "x" = [Alice]

Time 1: Bob arrives
  -> Store Bob in "x"
  -> Bob continues downstream immediately
  -> "x" = [Alice, Bob]

Time 2: Carol arrives
  -> Store Carol in "x"
  -> Carol continues downstream immediately
  -> "x" = [Alice, Bob, Carol]

─────────────────────────────────────────────────────
aggregate("x") - Barrier (blocking)
─────────────────────────────────────────────────────

Time 0-2: Collect all traversers
  -> Buffer: [Alice, Bob, Carol]

Time 3: Barrier releases
  -> Store all in "x"
  -> "x" = [Alice, Bob, Carol]
  -> Re-emit [Alice, Bob, Carol] downstream

```

### Example: cap() Retrieval

```
g.v().aggregate("people").out("knows").aggregate("friends").cap_multi(&["people", "friends"])

Phase 1: Aggregate "people"
  -> Barrier collects [v1, v2, v3]
  -> SideEffects: {"people": [v1, v2, v3]}
  -> Re-emit all

Phase 2: Navigate out("knows")
  -> v1 -> [v4, v5]
  -> v2 -> [v5, v6]
  -> v3 -> [v7]

Phase 3: Aggregate "friends"
  -> Barrier collects [v4, v5, v5, v6, v7]
  -> SideEffects: {"people": [...], "friends": [v4, v5, v5, v6, v7]}
  -> Re-emit all

Phase 4: cap_multi(["people", "friends"])
  -> Consume remaining stream
  -> Return Value::Map({
       "people": [v1, v2, v3],
       "friends": [v4, v5, v5, v6, v7]
     })
```

---

## API Summary

### New Types

| Type | Description |
|------|-------------|
| `SideEffectStep` | Execute sub-traversal for side effects |
| `StoreStep` | Lazily store values in named collection |
| `AggregateStep` | Barrier that collects all values into named collection |
| `CapStep` | Retrieve accumulated side-effect data |
| `ProfileStep` | Collect traversal profiling metrics |

### New Methods on BoundTraversal

| Method | Returns | Description |
|--------|---------|-------------|
| `side_effect(traversal)` | `Self` | Execute traversal for side effects |
| `store(key)` | `Self` | Store each value lazily |
| `aggregate(key)` | `Self` | Aggregate all values (barrier) |
| `cap(key)` | `BoundTraversal<..., Value>` | Retrieve single collection |
| `cap_multi(keys)` | `BoundTraversal<..., Value>` | Retrieve multiple collections |
| `profile()` | `Self` | Add profiling |
| `profile_as(key)` | `Self` | Add profiling with named key |

### Anonymous Traversal (`__`)

| Function | Description |
|----------|-------------|
| `__::side_effect(t)` | Execute traversal for side effects |
| `__::store(key)` | Store each value lazily |
| `__::aggregate(key)` | Aggregate all values (barrier) |
| `__::cap(key)` | Retrieve side-effect data |
| `__::profile()` | Add profiling |

---

## Use Cases

### 1. Forward Reference with aggregate()

```rust
// Find vertices not connected to any starting vertex
let disconnected = g.v()
    .has_label("person")
    .aggregate("start")
    .out("knows")
    .where_(p::without_collection("start"))  // Not in starting set
    .to_list();
```

### 2. Collecting Statistics with store()

```rust
// Collect all edges traversed while navigating
g.v().has_value("name", "Alice")
    .out_e().store("edges")
    .in_v().out_e().store("edges")
    .to_list();

// Access collected edges
let all_edges = ctx.side_effects.get("edges");
```

### 3. Side-Effect Computation

```rust
// Store degree counts without affecting main traversal
let names = g.v()
    .side_effect(
        __::out_e().count().store("degree")
    )
    .values("name")
    .to_list();
```

### 4. Multiple Collections

```rust
let data = g.v()
    .has_label("person").store("people")
    .out("created").store("creations")
    .cap_multi(&["people", "creations"])
    .next();
// Returns: Map{"people": [...], "creations": [...]}
```

---

## Test Cases

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // ─────────────────────────────────────────────────────────────
    // StoreStep Tests
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn store_step_stores_values() {
        // Setup graph and context
        // Execute: g.v().store("all").to_list()
        // Verify: ctx.side_effects.get("all") contains all vertices
    }

    #[test]
    fn store_step_is_lazy() {
        // Verify values are stored incrementally, not as barrier
    }

    #[test]
    fn store_step_passes_through_unchanged() {
        // Verify traversers pass through unmodified
    }

    #[test]
    fn store_multiple_collections() {
        // g.v().store("a").out().store("b")
        // Verify both collections populated correctly
    }

    // ─────────────────────────────────────────────────────────────
    // AggregateStep Tests
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn aggregate_step_collects_all() {
        // Verify all values collected before continuing
    }

    #[test]
    fn aggregate_step_is_barrier() {
        // Verify barrier behavior (all collected before any released)
    }

    #[test]
    fn aggregate_step_enables_forward_reference() {
        // Use aggregated collection in downstream where()
    }

    // ─────────────────────────────────────────────────────────────
    // CapStep Tests
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn cap_returns_list_for_single_key() {
        // Verify Value::List returned
    }

    #[test]
    fn cap_returns_map_for_multiple_keys() {
        // Verify Value::Map returned
    }

    #[test]
    fn cap_returns_empty_for_missing_key() {
        // Verify empty list for non-existent key
    }

    #[test]
    fn cap_consumes_stream() {
        // Verify stream is consumed before returning
    }

    // ─────────────────────────────────────────────────────────────
    // SideEffectStep Tests
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn side_effect_executes_traversal() {
        // Verify sub-traversal is executed
    }

    #[test]
    fn side_effect_discards_output() {
        // Verify sub-traversal output is discarded
    }

    #[test]
    fn side_effect_passes_original() {
        // Verify original traversers pass through
    }

    // ─────────────────────────────────────────────────────────────
    // ProfileStep Tests
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn profile_collects_count() {
        // Verify count is recorded
    }

    #[test]
    fn profile_collects_timing() {
        // Verify time_ms is recorded
    }
}
```

### Integration Tests

```rust
// tests/sideeffect.rs

#[test]
fn test_store_and_cap() {
    let graph = create_test_graph();
    let g = graph.traversal();
    
    let result = g.v()
        .has_label("person")
        .store("people")
        .cap("people")
        .next()
        .unwrap();
    
    match result {
        Value::List(items) => {
            assert!(!items.is_empty());
            // All should be vertices
        }
        _ => panic!("Expected list"),
    }
}

#[test]
fn test_aggregate_forward_reference() {
    let graph = create_test_graph();
    let g = graph.traversal();
    
    // Find friends-of-friends not in original set
    let results = g.v()
        .has_label("person")
        .aggregate("start")
        .out("knows").out("knows")
        .where_(p::without_collection("start"))
        .dedup()
        .to_list();
}

#[test]
fn test_store_vs_aggregate_order() {
    // Verify store is lazy (order-preserving)
    // Verify aggregate is barrier (collects before releasing)
}

#[test]
fn test_multiple_cap() {
    let graph = create_test_graph();
    let g = graph.traversal();
    
    let result = g.v()
        .store("vertices")
        .out_e().store("edges")
        .cap_multi(&["vertices", "edges"])
        .next()
        .unwrap();
    
    match result {
        Value::Map(map) => {
            assert!(map.contains_key("vertices"));
            assert!(map.contains_key("edges"));
        }
        _ => panic!("Expected map"),
    }
}

#[test]
fn test_side_effect_nested() {
    let graph = create_test_graph();
    let g = graph.traversal();
    
    // Complex side effect with nested traversal
    let names = g.v()
        .side_effect(
            __::out_e()
                .group_count()
                .by_label()
                .build()
                .cap("edge_counts")
        )
        .values("name")
        .to_list();
}
```

---

## Error Handling

### Error Conditions

| Step | Error Condition | Behavior |
|------|-----------------|----------|
| `store()` | None | Always succeeds |
| `aggregate()` | None | Always succeeds |
| `cap()` | Missing key | Returns empty list |
| `side_effect()` | Sub-traversal error | Error propagates |
| `profile()` | None | Always succeeds |

### Design Decisions

1. **Missing cap() keys**: Return empty list instead of error (Gremlin behavior)
2. **Empty traversals**: All steps handle empty input gracefully
3. **Sub-traversal errors**: Propagate errors from side_effect() sub-traversals

---

## Performance Considerations

### Memory

- `store()`: O(n) memory for stored values
- `aggregate()`: O(n) memory (barrier holds all traversers)
- `cap()`: O(n) for result construction

### Execution

- `store()`: O(1) per traverser (append to vector)
- `aggregate()`: O(n) barrier + O(n) re-emission
- `cap()`: O(n) to consume stream + O(n) to build result

### Thread Safety

All side-effect steps use the existing thread-safe `SideEffects` storage with `RwLock`.

---

## Gremlin_api.md Updates

After implementation, update the Side Effect Steps table:

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `sideEffect()` | `side_effect(traversal)` | `traversal::sideeffect` |
| `aggregate()` | `aggregate(key)` | `traversal::sideeffect` |
| `store()` | `store(key)` | `traversal::sideeffect` |
| `cap()` | `cap(key)`, `cap_multi(keys)` | `traversal::sideeffect` |
| `profile()` | `profile()`, `profile_as(key)` | `traversal::sideeffect` |
| `subgraph()` | - | - |

---

## Success Criteria

1. `StoreStep` lazily stores values as they pass through
2. `AggregateStep` collects all values before releasing (barrier behavior)
3. `CapStep` retrieves stored collections correctly
4. `SideEffectStep` executes sub-traversals without affecting main stream
5. All steps pass through traversers unchanged (except `cap`)
6. Integration with existing `SideEffects` infrastructure works correctly
7. All tests pass with >90% branch coverage on new code
8. `Gremlin_api.md` updated accurately
9. Anonymous traversal `__` factory functions work correctly

---

## Future Work (Out of Scope)

- `subgraph()` - Requires mutable storage access and graph construction
- `sack()` / `withSack()` - Traverser-local storage mechanism
- `barrier()` - Explicit synchronization step
- `aggregate(key).by(traversal)` - Aggregate by computed value
- `store(key).by(traversal)` - Store computed values
- Named loop support for `aggregate()` scope
