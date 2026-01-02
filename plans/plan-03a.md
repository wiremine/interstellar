# Plan 03a: Path Tracking, AsStep, and SelectStep Implementation

**Addendum to Phase 3: Traversal Engine Core**

Based on: `specs/spec-03a-paths.md`

---

## Overview

This plan implements path tracking capabilities for the traversal engine. It adds:
- Opt-in automatic path tracking via `with_path()`
- Explicit path labeling via `as_()`
- Path value retrieval via `select()` and `select_one()`

**Total Duration**: 3-4 hours  
**Current State**: Path and Traverser types exist, PathStep implemented, but no path tracking or labeling

---

## Implementation Order

### Phase 1: AsStep Implementation
**File**: `src/traversal/transform.rs`  
**Duration**: 30-45 minutes

**Tasks**:
1. Add `AsStep` struct with `label: String` field
2. Implement `AsStep::new(label)` constructor
3. Implement `AsStep::label()` accessor
4. Implement `AnyStep` for `AsStep`:
   - Map over input, call `t.extend_path_labeled(&label)`, return traverser unchanged
5. Add unit tests for AsStep

**Code**:
```rust
#[derive(Clone, Debug)]
pub struct AsStep {
    label: String,
}

impl AsStep {
    pub fn new(label: impl Into<String>) -> Self {
        Self { label: label.into() }
    }

    pub fn label(&self) -> &str {
        &self.label
    }
}

impl AnyStep for AsStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let label = self.label.clone();
        Box::new(input.map(move |mut t| {
            t.extend_path_labeled(&label);
            t
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "as"
    }
}
```

**Tests**:
- `as_step_construction` - new(), label() accessor
- `as_step_identity_behavior` - value unchanged after step
- `as_step_adds_label_to_path` - path contains labeled element
- `as_step_multiple_labels` - multiple as_() calls accumulate
- `as_step_preserves_metadata` - loops, bulk, sack preserved

**Acceptance Criteria**:
- [ ] `AsStep::new("label")` compiles
- [ ] `AsStep` implements `AnyStep`
- [ ] Traverser value unchanged after AsStep
- [ ] Path contains element with label after AsStep
- [ ] All tests pass

---

### Phase 2: SelectStep Implementation
**File**: `src/traversal/transform.rs`  
**Duration**: 45-60 minutes

**Tasks**:
1. Add `SelectStep` struct with `labels: Vec<String>` field
2. Implement `SelectStep::new(labels)` constructor (multi-label)
3. Implement `SelectStep::single(label)` constructor (single-label)
4. Implement `SelectStep::labels()` and `SelectStep::is_single()` accessors
5. Implement `AnyStep` for `SelectStep`:
   - Single label: return value directly via `filter_map`
   - Multiple labels: return `Value::Map` via `filter_map`
   - Filter out traversers with no matching labels
6. Add unit tests for SelectStep

**Code**:
```rust
#[derive(Clone, Debug)]
pub struct SelectStep {
    labels: Vec<String>,
}

impl SelectStep {
    pub fn new(labels: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            labels: labels.into_iter().map(Into::into).collect(),
        }
    }

    pub fn single(label: impl Into<String>) -> Self {
        Self { labels: vec![label.into()] }
    }

    pub fn labels(&self) -> &[String] {
        &self.labels
    }

    pub fn is_single(&self) -> bool {
        self.labels.len() == 1
    }
}

impl AnyStep for SelectStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let labels = self.labels.clone();
        let is_single = self.labels.len() == 1;

        Box::new(input.filter_map(move |t| {
            if is_single {
                t.path
                    .get(&labels[0])
                    .and_then(|values| values.last().cloned())
                    .map(|pv| t.with_value(pv.to_value()))
            } else {
                let mut map = std::collections::HashMap::new();
                for label in &labels {
                    if let Some(values) = t.path.get(label) {
                        if let Some(last) = values.last() {
                            map.insert(label.clone(), last.to_value());
                        }
                    }
                }
                if map.is_empty() {
                    None
                } else {
                    Some(t.with_value(Value::Map(map)))
                }
            }
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "select"
    }
}
```

**Tests**:
- `select_step_single_construction` - single() constructor
- `select_step_multi_construction` - new() constructor
- `select_step_single_returns_value` - single label returns Value directly
- `select_step_multi_returns_map` - multiple labels return Value::Map
- `select_step_missing_label_filters` - no matching labels = filtered out
- `select_step_partial_match` - some labels exist, returns partial Map
- `select_step_last_value_for_duplicate` - multiple values for label, returns last
- `select_step_preserves_path` - path unchanged after select

**Acceptance Criteria**:
- [ ] `SelectStep::new(["a", "b"])` compiles
- [ ] `SelectStep::single("a")` compiles
- [ ] Single label returns `Value` directly
- [ ] Multiple labels return `Value::Map`
- [ ] Missing labels filter out traverser
- [ ] All tests pass

---

### Phase 3: Update Module Exports
**File**: `src/traversal/mod.rs`  
**Duration**: 10 minutes

**Tasks**:
1. Add `AsStep` and `SelectStep` to the `pub use transform::` line

**Code**:
```rust
pub use transform::{
    AsStep, ConstantStep, FlatMapStep, IdStep, LabelStep, 
    MapStep, PathStep, SelectStep, ValuesStep
};
```

**Acceptance Criteria**:
- [ ] `use crate::traversal::AsStep` compiles
- [ ] `use crate::traversal::SelectStep` compiles

---

### Phase 4: Add API Methods to BoundTraversal
**File**: `src/traversal/source.rs`  
**Duration**: 20-30 minutes

**Tasks**:
1. Add `as_(label: &str)` method to `BoundTraversal`
2. Add `select(labels: &[&str])` method to `BoundTraversal`
3. Add `select_one(label: &str)` method to `BoundTraversal`
4. Add doc comments with examples

**Code** (add after existing methods like `path()`):
```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Label the current traversal position for later select().
    ///
    /// # Example
    ///
    /// ```ignore
    /// g.v().as_("start").out().as_("end").select(&["start", "end"])
    /// ```
    pub fn as_(self, label: &str) -> BoundTraversal<'g, In, Out> {
        use crate::traversal::transform::AsStep;
        self.add_step(AsStep::new(label))
    }

    /// Select multiple labeled values from path as a Map.
    ///
    /// # Example
    ///
    /// ```ignore
    /// g.v().as_("a").out().as_("b").select(&["a", "b"])
    /// ```
    pub fn select(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::SelectStep;
        self.add_step(SelectStep::new(labels.iter().map(|s| s.to_string())))
    }

    /// Select a single labeled value from path.
    ///
    /// # Example
    ///
    /// ```ignore
    /// g.v().as_("x").out().select_one("x")
    /// ```
    pub fn select_one(self, label: &str) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::SelectStep;
        self.add_step(SelectStep::single(label))
    }
}
```

**Acceptance Criteria**:
- [ ] `g.v().as_("x")` compiles
- [ ] `g.v().as_("x").select_one("x")` compiles
- [ ] `g.v().as_("a").out().as_("b").select(&["a", "b"])` compiles

---

### Phase 5: Add API Methods to Anonymous Traversal
**File**: `src/traversal/mod.rs`  
**Duration**: 20-30 minutes

**Tasks**:
1. Add `as_(label: &str)` method to `Traversal<In, Out>`
2. Add `select(labels: &[&str])` method to `Traversal<In, Value>`
3. Add `select_one(label: &str)` method to `Traversal<In, Value>`

**Code** (add to appropriate impl block):
```rust
impl<In, Out> Traversal<In, Out> {
    /// Label the current traversal position (anonymous traversal).
    pub fn as_(self, label: &str) -> Traversal<In, Out> {
        use crate::traversal::transform::AsStep;
        self.add_step(AsStep::new(label))
    }
}

impl<In> Traversal<In, Value> {
    /// Select multiple labeled values (anonymous traversal).
    pub fn select(self, labels: &[&str]) -> Traversal<In, Value> {
        use crate::traversal::transform::SelectStep;
        self.add_step(SelectStep::new(labels.iter().map(|s| s.to_string())))
    }

    /// Select a single labeled value (anonymous traversal).
    pub fn select_one(self, label: &str) -> Traversal<In, Value> {
        use crate::traversal::transform::SelectStep;
        self.add_step(SelectStep::single(label))
    }
}
```

**Acceptance Criteria**:
- [ ] `Traversal::new().as_("x")` compiles
- [ ] `Traversal::new().as_("x").select_one("x")` compiles
- [ ] Anonymous traversals can use as_() and select()

---

### Phase 6: ExecutionContext Path Tracking
**File**: `src/traversal/context.rs`  
**Duration**: 20-30 minutes

**Tasks**:
1. Add `track_paths: bool` field to `ExecutionContext`
2. Update `new()` to set `track_paths: false`
3. Add `with_path_tracking()` constructor
4. Add `is_tracking_paths()` accessor
5. Add tests

**Code**:
```rust
pub struct ExecutionContext<'g> {
    snapshot: &'g GraphSnapshot<'g>,
    interner: &'g StringInterner,
    pub side_effects: SideEffects,
    pub track_paths: bool,
}

impl<'g> ExecutionContext<'g> {
    pub fn new(snapshot: &'g GraphSnapshot<'g>, interner: &'g StringInterner) -> Self {
        Self {
            snapshot,
            interner,
            side_effects: SideEffects::new(),
            track_paths: false,
        }
    }

    pub fn with_path_tracking(
        snapshot: &'g GraphSnapshot<'g>,
        interner: &'g StringInterner,
    ) -> Self {
        Self {
            snapshot,
            interner,
            side_effects: SideEffects::new(),
            track_paths: true,
        }
    }

    #[inline]
    pub fn is_tracking_paths(&self) -> bool {
        self.track_paths
    }
}
```

**Tests**:
- `execution_context_default_no_path_tracking`
- `execution_context_with_path_tracking`
- `is_tracking_paths_accessor`

**Acceptance Criteria**:
- [ ] `ExecutionContext::new()` has `track_paths: false`
- [ ] `ExecutionContext::with_path_tracking()` has `track_paths: true`
- [ ] `is_tracking_paths()` returns correct value

---

### Phase 7: BoundTraversal with_path() Method
**File**: `src/traversal/source.rs`  
**Duration**: 30-45 minutes

**Tasks**:
1. Add `track_paths: bool` field to `BoundTraversal`
2. Update constructors to initialize `track_paths: false`
3. Add `with_path()` method that returns new BoundTraversal with `track_paths: true`
4. Update terminal steps (`to_list`, `count`, etc.) to use appropriate ExecutionContext

**Code**:
```rust
pub struct BoundTraversal<'g, In, Out> {
    snapshot: &'g GraphSnapshot<'g>,
    interner: &'g StringInterner,
    traversal: Traversal<In, Out>,
    track_paths: bool,
}

impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    // Update existing constructors to set track_paths: false
    
    /// Enable automatic path tracking for this traversal.
    pub fn with_path(self) -> BoundTraversal<'g, In, Out> {
        BoundTraversal {
            snapshot: self.snapshot,
            interner: self.interner,
            traversal: self.traversal,
            track_paths: true,
        }
    }
    
    /// Check if path tracking is enabled.
    pub fn is_tracking_paths(&self) -> bool {
        self.track_paths
    }
}

// Update terminal steps to use track_paths
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    pub fn to_list(self) -> Vec<Value> {
        let ctx = if self.track_paths {
            ExecutionContext::with_path_tracking(self.snapshot, self.interner)
        } else {
            ExecutionContext::new(self.snapshot, self.interner)
        };
        // ... rest of implementation
    }
    // Similar for count(), next(), one(), has_next(), iterate(), etc.
}
```

**Acceptance Criteria**:
- [ ] `g.v().with_path()` compiles
- [ ] `with_path()` sets `track_paths: true`
- [ ] Terminal steps create correct ExecutionContext
- [ ] `is_tracking_paths()` accessor works

---

### Phase 8: Update Navigation Steps for Path Tracking
**File**: `src/traversal/navigation.rs`  
**Duration**: 45-60 minutes

**Tasks**:
1. Update `OutStep::expand()` to check `ctx.track_paths` and call `extend_path_unlabeled()`
2. Repeat for `InStep`, `BothStep`
3. Repeat for `OutEStep`, `InEStep`, `BothEStep`
4. Repeat for `OutVStep`, `InVStep`, `BothVStep`
5. Add tests for each step with path tracking enabled/disabled

**Pattern** (apply to all 9 steps):
```rust
fn expand<'a>(&self, ctx: &'a ExecutionContext<'a>, t: Traverser) -> Vec<Traverser> {
    // ... existing logic ...
    
    .filter_map(|edge| {
        // ... existing filter logic ...
        
        let mut new_t = t.split(Value::Vertex(edge.dst));
        if ctx.track_paths {
            new_t.extend_path_unlabeled();
        }
        Some(new_t)
    })
    .collect()
}
```

**Tests** (for each of 9 steps):
- `{step}_path_tracking_disabled` - path empty after step
- `{step}_path_tracking_enabled` - path contains new element

**Acceptance Criteria**:
- [ ] `OutStep` adds to path when tracking enabled
- [ ] `InStep` adds to path when tracking enabled
- [ ] `BothStep` adds to path when tracking enabled
- [ ] `OutEStep` adds to path when tracking enabled
- [ ] `InEStep` adds to path when tracking enabled
- [ ] `BothEStep` adds to path when tracking enabled
- [ ] `OutVStep` adds to path when tracking enabled
- [ ] `InVStep` adds to path when tracking enabled
- [ ] `BothVStep` adds to path when tracking enabled
- [ ] No path modification when tracking disabled

---

### Phase 9: Update StartStep for Path Tracking
**File**: `src/traversal/step.rs`  
**Duration**: 20-30 minutes

**Tasks**:
1. Update `StartStep::apply()` to check `ctx.track_paths`
2. If enabled, call `extend_path_unlabeled()` on new traversers
3. Add tests

**Code**:
```rust
impl AnyStep for StartStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        _input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let track_paths = ctx.track_paths;
        
        match &self.source {
            TraversalSource::AllVertices => {
                Box::new(
                    ctx.snapshot()
                        .storage()
                        .all_vertices()
                        .map(move |v| {
                            let mut t = Traverser::from_vertex(v.id);
                            if track_paths {
                                t.extend_path_unlabeled();
                            }
                            t
                        }),
                )
            }
            // ... similar for AllEdges, Vertices, Edges, Inject ...
        }
    }
}
```

**Tests**:
- `start_step_path_tracking_disabled` - traversers have empty path
- `start_step_path_tracking_enabled` - traversers have initial element in path

**Acceptance Criteria**:
- [ ] `g.v()` traversers have empty path (default)
- [ ] `g.v().with_path()` traversers have initial vertex in path
- [ ] Same for `e()`, `v(ids)`, `e(ids)`, `inject()`

---

### Phase 10: Integration Tests
**File**: `src/traversal/transform.rs` (or new test file)  
**Duration**: 30-45 minutes

**Tasks**:
1. Add integration tests combining all new features
2. Test as_() + select() combinations
3. Test with_path() + path() combinations
4. Test mixed as_() + with_path() + path()

**Tests**:
```rust
#[test]
fn integration_as_select_chain() {
    let graph = create_test_graph();
    let g = graph.traversal();
    
    let results = g.v()
        .as_("start")
        .out()
        .as_("end")
        .select(&["start", "end"])
        .to_list();
    
    assert!(!results.is_empty());
    // Verify results are Maps with "start" and "end" keys
}

#[test]
fn integration_with_path_tracking() {
    let graph = create_test_graph();
    let g = graph.traversal();
    
    // Without path tracking
    let paths_empty = g.v().out().path().to_list();
    assert!(paths_empty.iter().all(|p| matches!(p, Value::List(l) if l.is_empty())));
    
    // With path tracking
    let paths_full = g.v().with_path().out().path().to_list();
    assert!(paths_full.iter().all(|p| matches!(p, Value::List(l) if l.len() == 2)));
}

#[test]
fn integration_mixed_as_and_path() {
    let graph = create_test_graph();
    let g = graph.traversal();
    
    let results = g.v()
        .with_path()
        .as_("origin")
        .out()
        .out()
        .path()
        .to_list();
    
    // Path should have 3 elements (origin, intermediate, final)
    // "origin" label should be accessible via select
}
```

**Acceptance Criteria**:
- [ ] `g.v().as_("x").select_one("x")` returns original vertices
- [ ] `g.v().as_("a").out().as_("b").select(&["a", "b"])` returns Maps
- [ ] `g.v().with_path().out().path()` returns non-empty paths
- [ ] `g.v().out().path()` returns empty paths (no tracking)

---

## Summary

| Phase | File | Duration | Key Deliverable |
|-------|------|----------|-----------------|
| 1 | transform.rs | 30-45 min | AsStep |
| 2 | transform.rs | 45-60 min | SelectStep |
| 3 | mod.rs | 10 min | Exports |
| 4 | source.rs | 20-30 min | BoundTraversal methods |
| 5 | mod.rs | 20-30 min | Anonymous Traversal methods |
| 6 | context.rs | 20-30 min | ExecutionContext track_paths |
| 7 | source.rs | 30-45 min | with_path() method |
| 8 | navigation.rs | 45-60 min | 9 navigation steps updated |
| 9 | step.rs | 20-30 min | StartStep updated |
| 10 | transform.rs | 30-45 min | Integration tests |

**Total**: ~4.5-6 hours

---

## Verification Commands

```bash
# Run all tests
cargo test

# Run specific test modules
cargo test as_step
cargo test select_step
cargo test path_tracking

# Run with output
cargo test -- --nocapture

# Check for warnings
cargo clippy -- -D warnings

# Verify no regressions
cargo test 2>&1 | grep -E "^(test result|passed|failed)"
```

---

## Risk Mitigation

1. **Type signature complexity**: `as_()` returns same type, `select()` returns `Value`. Carefully test return type transitions.

2. **Performance regression**: Navigation steps now have conditional. Benchmark before/after to ensure < 5% impact when tracking disabled.

3. **Path clone overhead**: When tracking enabled, each split clones path. Already expected, but monitor memory usage.

4. **Breaking existing tests**: Navigation step behavior unchanged when tracking disabled. Run full test suite after each phase.
