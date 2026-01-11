# Plan 17: Implement Side Effect Steps

**Spec Reference:** `specs/spec-16-side-effect-steps.md`

**Goal:** Implement Gremlin side effect steps (`store()`, `aggregate()`, `cap()`, `sideEffect()`, `profile()`) that enable accumulating data during traversal without affecting the main stream.

**Estimated Duration:** 4-6 days

---

## Overview

Side effect steps are unique in Gremlin as they perform operations that don't change the traversal stream but produce side effects such as storing values in collections. This plan implements the core side effect steps, leveraging the existing `SideEffects` infrastructure in `context.rs`.

**Key Features:**
- `store(key)` - Lazily store each value as it passes through
- `aggregate(key)` - Barrier step that collects all values before continuing
- `cap(key)` / `cap_multi(keys)` - Retrieve accumulated side-effect data
- `side_effect(traversal)` - Execute a traversal for side effects only
- `profile()` - Collect traversal timing and count metrics

---

## Phase 1: Core Module Setup (Day 1)

### 1.1 Create Side Effect Module

**File:** `src/traversal/sideeffect.rs` (new file)

**Tasks:**
- [ ] Create new module file with imports
- [ ] Add module documentation
- [ ] Set up basic structure

**Implementation:**
```rust
//! Side effect steps for graph traversals.
//!
//! Side effect steps perform operations that don't change the traversal stream
//! but produce side effects such as storing values in collections.

use crate::traversal::context::ExecutionContext;
use crate::traversal::step::AnyStep;
use crate::traversal::{Traversal, Traverser};
use crate::value::Value;

use std::collections::HashMap;
```

### 1.2 Update Module Declarations

**File:** `src/traversal/mod.rs`

**Tasks:**
- [ ] Add `pub mod sideeffect;` declaration
- [ ] Add re-exports for new types:
  - `SideEffectStep`
  - `StoreStep`
  - `AggregateStep`
  - `CapStep`
  - `ProfileStep`

**Implementation:**
```rust
pub mod sideeffect;

pub use sideeffect::{
    AggregateStep, CapStep, ProfileStep, SideEffectStep, StoreStep,
};
```

---

## Phase 2: StoreStep Implementation (Day 1)

### 2.1 Implement StoreStep Struct

**File:** `src/traversal/sideeffect.rs`

**Tasks:**
- [ ] Define `StoreStep` struct with `key: String` field
- [ ] Implement `StoreStep::new(key: impl Into<String>)`
- [ ] Derive/implement `Clone`, `Debug`

**Implementation:**
```rust
/// Store each traverser value into a named side-effect collection (lazy)
///
/// Unlike `aggregate()`, `store()` is not a barrier - values are stored
/// as they pass through, and traversers continue immediately.
#[derive(Clone, Debug)]
pub struct StoreStep {
    key: String,
}

impl StoreStep {
    /// Create a new StoreStep
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}
```

### 2.2 Implement AnyStep for StoreStep

**File:** `src/traversal/sideeffect.rs`

**Tasks:**
- [ ] Implement `apply()` method that stores each value
- [ ] Implement `clone_box()`
- [ ] Implement `name()` returning "store"

**Implementation:**
```rust
impl AnyStep for StoreStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let key = self.key.clone();
        
        Box::new(input.map(move |t| {
            ctx.side_effects.store(&key, t.value.clone());
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

### 2.3 Unit Tests for StoreStep

**File:** `src/traversal/sideeffect.rs` (tests module)

**Tasks:**
- [ ] Test `StoreStep::new()` creates step with correct key
- [ ] Test step stores values in side effects
- [ ] Test step passes through traversers unchanged
- [ ] Test step handles empty input
- [ ] Test multiple values stored sequentially

---

## Phase 3: AggregateStep Implementation (Day 1-2)

### 3.1 Implement AggregateStep Struct

**File:** `src/traversal/sideeffect.rs`

**Tasks:**
- [ ] Define `AggregateStep` struct with `key: String` field
- [ ] Implement `AggregateStep::new(key: impl Into<String>)`
- [ ] Derive/implement `Clone`, `Debug`

**Implementation:**
```rust
/// Collect all traverser values into a named side-effect collection (barrier)
///
/// This is a **barrier step** - it collects ALL input traversers before
/// allowing any to continue.
#[derive(Clone, Debug)]
pub struct AggregateStep {
    key: String,
}

impl AggregateStep {
    /// Create a new AggregateStep
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}
```

### 3.2 Implement AnyStep for AggregateStep

**File:** `src/traversal/sideeffect.rs`

**Tasks:**
- [ ] Implement barrier `apply()` method:
  1. Collect all traversers into Vec
  2. Store all values in side effects
  3. Re-emit all traversers
- [ ] Implement `clone_box()`
- [ ] Implement `name()` returning "aggregate"

**Implementation:**
```rust
impl AnyStep for AggregateStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Barrier: collect all traversers first
        let traversers: Vec<Traverser> = input.collect();
        
        // Store all values
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

### 3.3 Unit Tests for AggregateStep

**File:** `src/traversal/sideeffect.rs` (tests module)

**Tasks:**
- [ ] Test step collects all values
- [ ] Test step is barrier (verifies collection before release)
- [ ] Test step passes through all traversers
- [ ] Test step handles empty input
- [ ] Test multiple values stored together

---

## Phase 4: CapStep Implementation (Day 2)

### 4.1 Implement CapStep Struct

**File:** `src/traversal/sideeffect.rs`

**Tasks:**
- [ ] Define `CapStep` struct with `keys: Vec<String>` field
- [ ] Implement `CapStep::new(key: impl Into<String>)` for single key
- [ ] Implement `CapStep::multi(keys)` for multiple keys
- [ ] Derive/implement `Clone`, `Debug`

**Implementation:**
```rust
/// Retrieve accumulated side-effect data
///
/// Single key returns `Value::List`, multiple keys return `Value::Map`.
#[derive(Clone, Debug)]
pub struct CapStep {
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
    pub fn multi<I, S>(keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}
```

### 4.2 Implement AnyStep for CapStep

**File:** `src/traversal/sideeffect.rs`

**Tasks:**
- [ ] Implement `apply()` method:
  1. Consume input stream
  2. Retrieve side-effect data
  3. Return single traverser with result
- [ ] Handle single key (return List)
- [ ] Handle multiple keys (return Map)
- [ ] Handle missing keys (return empty list)

**Implementation:**
```rust
impl AnyStep for CapStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Consume input to ensure all side effects populated
        input.for_each(|_| {});
        
        let result = if self.keys.len() == 1 {
            // Single key: return list
            let values = ctx.side_effects.get(&self.keys[0]).unwrap_or_default();
            Value::List(values)
        } else {
            // Multiple keys: return map
            let mut map = HashMap::new();
            for key in &self.keys {
                let values = ctx.side_effects.get(key).unwrap_or_default();
                map.insert(key.clone(), Value::List(values));
            }
            Value::Map(map)
        };
        
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

### 4.3 Unit Tests for CapStep

**File:** `src/traversal/sideeffect.rs` (tests module)

**Tasks:**
- [ ] Test single key returns List
- [ ] Test multiple keys returns Map
- [ ] Test missing key returns empty list
- [ ] Test stream is consumed before result
- [ ] Test empty input works correctly

---

## Phase 5: SideEffectStep Implementation (Day 2-3)

### 5.1 Implement SideEffectStep Struct

**File:** `src/traversal/sideeffect.rs`

**Tasks:**
- [ ] Define `SideEffectStep` struct with `side_traversal: Traversal<Value, Value>`
- [ ] Implement `SideEffectStep::new(traversal)`
- [ ] Implement `Clone`

**Implementation:**
```rust
use crate::traversal::step::execute_traversal_from;

/// Execute a traversal for side effects only
///
/// The sub-traversal is executed for each input traverser, but its output
/// is discarded. The original traverser passes through unchanged.
#[derive(Clone)]
pub struct SideEffectStep {
    side_traversal: Traversal<Value, Value>,
}

impl SideEffectStep {
    /// Create a new SideEffectStep
    pub fn new(side_traversal: Traversal<Value, Value>) -> Self {
        Self { side_traversal }
    }
}
```

### 5.2 Implement AnyStep for SideEffectStep

**File:** `src/traversal/sideeffect.rs`

**Tasks:**
- [ ] Implement `apply()` method:
  1. For each traverser, execute side-effect traversal
  2. Discard side-effect traversal output
  3. Pass original traverser through
- [ ] Implement `clone_box()`
- [ ] Implement `name()` returning "sideEffect"

**Implementation:**
```rust
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
                .for_each(|_| {});
            
            // Return original traverser
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

### 5.3 Unit Tests for SideEffectStep

**File:** `src/traversal/sideeffect.rs` (tests module)

**Tasks:**
- [ ] Test side-effect traversal is executed
- [ ] Test side-effect output is discarded
- [ ] Test original traversers pass through unchanged
- [ ] Test side effects from sub-traversal are recorded
- [ ] Test empty input works correctly

---

## Phase 6: ProfileStep Implementation (Day 3)

### 6.1 Implement ProfileStep Struct

**File:** `src/traversal/sideeffect.rs`

**Tasks:**
- [ ] Define `ProfileStep` struct with `key: Option<String>`
- [ ] Implement `ProfileStep::new()` for auto-generated key
- [ ] Implement `ProfileStep::with_key(key)` for named key
- [ ] Implement `Default`
- [ ] Derive `Clone`, `Debug`

**Implementation:**
```rust
use std::time::Instant;
use std::cell::Cell;

/// Collect traversal profiling information
#[derive(Clone, Debug)]
pub struct ProfileStep {
    key: Option<String>,
}

impl ProfileStep {
    /// Create with auto-generated key
    pub fn new() -> Self {
        Self { key: None }
    }
    
    /// Create with specific key
    pub fn with_key(key: impl Into<String>) -> Self {
        Self { key: Some(key.into()) }
    }
}

impl Default for ProfileStep {
    fn default() -> Self {
        Self::new()
    }
}
```

### 6.2 Implement AnyStep for ProfileStep

**File:** `src/traversal/sideeffect.rs`

**Tasks:**
- [ ] Create `ProfileIterator` wrapper struct
- [ ] Implement `Iterator` for `ProfileIterator`
- [ ] Track count and timing during iteration
- [ ] Store profile data on iterator completion
- [ ] Implement `apply()`, `clone_box()`, `name()`

**Implementation:**
```rust
impl AnyStep for ProfileStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let key = self.key.clone().unwrap_or_else(|| "profile".to_string());
        
        Box::new(ProfileIterator {
            inner: input,
            ctx,
            key,
            count: Cell::new(0),
            start: Instant::now(),
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
                if !self.finished.get() {
                    self.finished.set(true);
                    let elapsed = self.start.elapsed();
                    let profile = Value::Map({
                        let mut m = HashMap::new();
                        m.insert("count".to_string(), Value::Int(self.count.get() as i64));
                        m.insert("time_ms".to_string(), Value::Float(elapsed.as_secs_f64() * 1000.0));
                        m
                    });
                    self.ctx.side_effects.store(&self.key, profile);
                }
                None
            }
        }
    }
}
```

### 6.3 Unit Tests for ProfileStep

**File:** `src/traversal/sideeffect.rs` (tests module)

**Tasks:**
- [ ] Test count is recorded correctly
- [ ] Test time_ms is recorded (non-negative)
- [ ] Test profile data stored in side effects
- [ ] Test custom key works
- [ ] Test default key works
- [ ] Test empty input produces profile with count=0

---

## Phase 7: Builder Methods on BoundTraversal (Day 3-4)

### 7.1 Add store() Method

**File:** `src/traversal/source.rs`

**Tasks:**
- [ ] Add `store(key)` method returning `Self`
- [ ] Add documentation with example

**Implementation:**
```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Store each traverser value into a named collection (lazy)
    pub fn store(self, key: impl Into<String>) -> BoundTraversal<'g, In, Out> {
        let step = StoreStep::new(key);
        BoundTraversal {
            snapshot: self.snapshot,
            interner: self.interner,
            traversal: self.traversal.add_step(step),
        }
    }
}
```

### 7.2 Add aggregate() Method

**File:** `src/traversal/source.rs`

**Tasks:**
- [ ] Add `aggregate(key)` method returning `Self`
- [ ] Add documentation explaining barrier behavior

**Implementation:**
```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Aggregate all traverser values into a named collection (barrier)
    pub fn aggregate(self, key: impl Into<String>) -> BoundTraversal<'g, In, Out> {
        let step = AggregateStep::new(key);
        BoundTraversal {
            snapshot: self.snapshot,
            interner: self.interner,
            traversal: self.traversal.add_step(step),
        }
    }
}
```

### 7.3 Add cap() and cap_multi() Methods

**File:** `src/traversal/source.rs`

**Tasks:**
- [ ] Add `cap(key)` method returning `BoundTraversal<'g, In, Value>`
- [ ] Add `cap_multi(keys)` method returning `BoundTraversal<'g, In, Value>`
- [ ] Add documentation with examples

**Implementation:**
```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Retrieve accumulated side-effect data (single key)
    pub fn cap(self, key: impl Into<String>) -> BoundTraversal<'g, In, Value> {
        let step = CapStep::new(key);
        BoundTraversal {
            snapshot: self.snapshot,
            interner: self.interner,
            traversal: self.traversal.add_step(step),
        }
    }
    
    /// Retrieve accumulated side-effect data (multiple keys)
    pub fn cap_multi<I, S>(self, keys: I) -> BoundTraversal<'g, In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let step = CapStep::multi(keys);
        BoundTraversal {
            snapshot: self.snapshot,
            interner: self.interner,
            traversal: self.traversal.add_step(step),
        }
    }
}
```

### 7.4 Add side_effect() Method

**File:** `src/traversal/source.rs`

**Tasks:**
- [ ] Add `side_effect(traversal)` method returning `Self`
- [ ] Add documentation with example

**Implementation:**
```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Execute a traversal for side effects only
    pub fn side_effect(
        self,
        traversal: Traversal<Value, Value>,
    ) -> BoundTraversal<'g, In, Out> {
        let step = SideEffectStep::new(traversal);
        BoundTraversal {
            snapshot: self.snapshot,
            interner: self.interner,
            traversal: self.traversal.add_step(step),
        }
    }
}
```

### 7.5 Add profile() Methods

**File:** `src/traversal/source.rs`

**Tasks:**
- [ ] Add `profile()` method returning `Self`
- [ ] Add `profile_as(key)` method returning `Self`
- [ ] Add documentation

**Implementation:**
```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Collect traversal profiling information
    pub fn profile(self) -> BoundTraversal<'g, In, Out> {
        let step = ProfileStep::new();
        BoundTraversal {
            snapshot: self.snapshot,
            interner: self.interner,
            traversal: self.traversal.add_step(step),
        }
    }
    
    /// Collect traversal profiling information with named key
    pub fn profile_as(self, key: impl Into<String>) -> BoundTraversal<'g, In, Out> {
        let step = ProfileStep::with_key(key);
        BoundTraversal {
            snapshot: self.snapshot,
            interner: self.interner,
            traversal: self.traversal.add_step(step),
        }
    }
}
```

---

## Phase 8: Anonymous Traversal Methods (Day 4)

### 8.1 Add Methods to Traversal<In, Value>

**File:** `src/traversal/mod.rs`

**Tasks:**
- [ ] Add `store(key)` method
- [ ] Add `aggregate(key)` method
- [ ] Add `cap(key)` method
- [ ] Add `cap_multi(keys)` method
- [ ] Add `side_effect(traversal)` method
- [ ] Add `profile()` and `profile_as(key)` methods

**Implementation:**
```rust
impl<In> Traversal<In, Value> {
    /// Store each value into a named collection (lazy)
    pub fn store(self, key: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(sideeffect::StoreStep::new(key))
    }
    
    /// Aggregate all values into a named collection (barrier)
    pub fn aggregate(self, key: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(sideeffect::AggregateStep::new(key))
    }
    
    /// Retrieve side-effect data (single key)
    pub fn cap(self, key: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(sideeffect::CapStep::new(key))
    }
    
    /// Retrieve side-effect data (multiple keys)
    pub fn cap_multi<I, S>(self, keys: I) -> Traversal<In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.add_step(sideeffect::CapStep::multi(keys))
    }
    
    /// Execute traversal for side effects only
    pub fn side_effect(self, traversal: Traversal<Value, Value>) -> Traversal<In, Value> {
        self.add_step(sideeffect::SideEffectStep::new(traversal))
    }
    
    /// Collect profiling information
    pub fn profile(self) -> Traversal<In, Value> {
        self.add_step(sideeffect::ProfileStep::new())
    }
    
    /// Collect profiling information with named key
    pub fn profile_as(self, key: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(sideeffect::ProfileStep::with_key(key))
    }
}
```

### 8.2 Add Factory Functions to __ Module

**File:** `src/traversal/mod.rs` (within `__` module)

**Tasks:**
- [ ] Add `store(key)` factory function
- [ ] Add `aggregate(key)` factory function
- [ ] Add `cap(key)` factory function
- [ ] Add `side_effect(traversal)` factory function
- [ ] Add `profile()` factory function

**Implementation:**
```rust
pub mod __ {
    // ... existing functions ...
    
    /// Store each value into a named collection (lazy)
    pub fn store(key: impl Into<String>) -> Traversal<Value, Value> {
        Traversal::new().add_step(sideeffect::StoreStep::new(key))
    }
    
    /// Aggregate all values into a named collection (barrier)
    pub fn aggregate(key: impl Into<String>) -> Traversal<Value, Value> {
        Traversal::new().add_step(sideeffect::AggregateStep::new(key))
    }
    
    /// Retrieve side-effect data
    pub fn cap(key: impl Into<String>) -> Traversal<Value, Value> {
        Traversal::new().add_step(sideeffect::CapStep::new(key))
    }
    
    /// Execute traversal for side effects only
    pub fn side_effect(traversal: Traversal<Value, Value>) -> Traversal<Value, Value> {
        Traversal::new().add_step(sideeffect::SideEffectStep::new(traversal))
    }
    
    /// Collect profiling information
    pub fn profile() -> Traversal<Value, Value> {
        Traversal::new().add_step(sideeffect::ProfileStep::new())
    }
}
```

---

## Phase 9: Integration Tests (Day 4-5)

### 9.1 Create Integration Test File

**File:** `tests/sideeffect.rs`

**Tasks:**
- [ ] Set up test graph with vertices and edges
- [ ] Test `store()` basic functionality
- [ ] Test `aggregate()` barrier behavior
- [ ] Test `cap()` single key
- [ ] Test `cap_multi()` multiple keys
- [ ] Test `side_effect()` nested traversal
- [ ] Test `profile()` metrics collection
- [ ] Test store vs aggregate ordering differences

**Test Cases:**
```rust
use rustgremlin::prelude::*;

fn create_test_graph() -> Graph {
    // Create graph with person and software vertices
    // Add knows and created edges
}

#[test]
fn test_store_basic() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();
    
    // Store all vertices
    let _ = g.v().store("all").to_list();
    
    // Verify stored
    let stored = g.ctx().side_effects.get("all").unwrap();
    assert!(!stored.is_empty());
}

#[test]
fn test_store_is_lazy() {
    // Verify store() doesn't block - values available incrementally
}

#[test]
fn test_aggregate_barrier() {
    // Verify aggregate() collects all before continuing
}

#[test]
fn test_cap_single_key() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();
    
    let result = g.v().store("x").cap("x").next().unwrap();
    
    assert!(matches!(result, Value::List(_)));
}

#[test]
fn test_cap_multiple_keys() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();
    
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
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();
    
    // Store count as side effect
    let names = g.v()
        .side_effect(__::out_e().count().store("edge_count"))
        .values("name")
        .to_list();
    
    // Verify names returned and side effect stored
    assert!(!names.is_empty());
}

#[test]
fn test_profile_metrics() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();
    
    let _ = g.v().profile_as("step1").to_list();
    
    let profile = g.ctx().side_effects.get("step1").unwrap();
    assert!(!profile.is_empty());
    
    if let Value::Map(map) = &profile[0] {
        assert!(map.contains_key("count"));
        assert!(map.contains_key("time_ms"));
    }
}

#[test]
fn test_aggregate_forward_reference() {
    // Classic use case: find vertices not in starting set
    let graph = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();
    
    // This requires where_(p::without(...)) which may need additional work
}
```

---

## Phase 10: Documentation Updates (Day 5)

### 10.1 Update Gremlin_api.md

**File:** `Gremlin_api.md`

**Tasks:**
- [ ] Update Side Effect Steps table with implementations
- [ ] Update Implementation Summary counts
- [ ] Add examples if appropriate

**Changes:**
```markdown
## Side Effect Steps

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `sideEffect()` | `side_effect(traversal)` | `traversal::sideeffect` |
| `aggregate()` | `aggregate(key)` | `traversal::sideeffect` |
| `store()` | `store(key)` | `traversal::sideeffect` |
| `subgraph()` | - | - |
| `cap()` | `cap(key)`, `cap_multi(keys)` | `traversal::sideeffect` |
| `profile()` | `profile()`, `profile_as(key)` | `traversal::sideeffect` |
```

### 10.2 Create Example File

**File:** `examples/side_effects.rs`

**Tasks:**
- [ ] Demonstrate `store()` usage
- [ ] Demonstrate `aggregate()` usage
- [ ] Demonstrate `cap()` retrieval
- [ ] Demonstrate `side_effect()` nested traversal
- [ ] Demonstrate `profile()` metrics
- [ ] Add detailed comments

---

## Phase 11: Final Verification (Day 5-6)

### 11.1 Run Full Test Suite

**Tasks:**
- [ ] Run `cargo test` - all tests pass
- [ ] Run `cargo test --features mmap` - mmap tests pass
- [ ] Run `cargo clippy -- -D warnings` - no warnings
- [ ] Run `cargo fmt --check` - formatting correct

### 11.2 Code Coverage

**Tasks:**
- [ ] Run `cargo +nightly llvm-cov --branch --html`
- [ ] Verify >90% branch coverage on new code
- [ ] Add tests for any uncovered branches

### 11.3 Documentation Review

**Tasks:**
- [ ] Verify doc comments compile (`cargo doc`)
- [ ] Check example code compiles
- [ ] Verify Gremlin_api.md is accurate

---

## Testing Checklist

### Unit Tests

**StoreStep:**
- [ ] Creates step with correct key
- [ ] Stores values in side effects
- [ ] Passes traversers unchanged
- [ ] Handles empty input
- [ ] Stores multiple values sequentially

**AggregateStep:**
- [ ] Creates step with correct key
- [ ] Collects all values before releasing
- [ ] Passes all traversers through
- [ ] Handles empty input

**CapStep:**
- [ ] Single key returns List
- [ ] Multiple keys returns Map
- [ ] Missing key returns empty list
- [ ] Consumes input stream
- [ ] Handles empty input

**SideEffectStep:**
- [ ] Executes sub-traversal
- [ ] Discards sub-traversal output
- [ ] Passes original traversers
- [ ] Side effects from sub-traversal recorded

**ProfileStep:**
- [ ] Records count correctly
- [ ] Records time_ms (non-negative)
- [ ] Stores profile in side effects
- [ ] Custom key works
- [ ] Default key works

### Integration Tests

- [ ] store() basic functionality
- [ ] store() is lazy (not barrier)
- [ ] aggregate() is barrier
- [ ] cap() single key
- [ ] cap_multi() multiple keys
- [ ] side_effect() nested traversal
- [ ] profile() metrics collection
- [ ] Multiple stores same key
- [ ] store + aggregate interleaved
- [ ] Empty traversals work

---

## Dependencies

- Existing `SideEffects` in `context.rs`
- Existing `ExecutionContext`
- Existing `execute_traversal_from()` helper
- Existing `Traversal<In, Out>` and `BoundTraversal`

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Thread safety with SideEffects | Medium | Use existing RwLock implementation |
| Performance with large aggregates | Medium | Document barrier behavior |
| cap() timing with lazy evaluation | Low | Document stream consumption |
| ProfileStep timing accuracy | Low | Use std::time::Instant |

---

## Success Criteria

1. `StoreStep` lazily stores values as they pass through
2. `AggregateStep` collects all values before releasing (barrier)
3. `CapStep` retrieves stored collections correctly
4. `SideEffectStep` executes sub-traversals without affecting stream
5. `ProfileStep` records accurate count and timing
6. All steps pass through traversers unchanged (except cap)
7. Integration with existing `SideEffects` works correctly
8. All tests pass with >90% branch coverage on new code
9. `Gremlin_api.md` updated accurately
10. Example file demonstrates functionality

---

## File Changes Summary

| File | Changes |
|------|---------|
| `src/traversal/sideeffect.rs` | New module with all side effect steps |
| `src/traversal/mod.rs` | Module declaration, re-exports, `__` factory functions |
| `src/traversal/source.rs` | Builder methods on `BoundTraversal` |
| `tests/sideeffect.rs` | New integration test file |
| `examples/side_effects.rs` | New example file |
| `Gremlin_api.md` | Update documentation |

---

## Future Work (Out of Scope)

- `subgraph()` - Requires mutable storage access
- `sack()` / `withSack()` - Traverser-local storage
- `barrier()` - Explicit synchronization step
- `aggregate(key).by(traversal)` - Aggregate by computed value
- `store(key).by(traversal)` - Store computed values
- `aggregate()` scope in nested loops
- Forward reference predicates (`p::within_collection()`)
