# Plan 16: Implement Branch and Choose-Option Steps

**Spec Reference:** `specs/spec-15-branch-option-steps.md`

**Goal:** Implement multi-way branching via `branch()` and `choose(traversal).option()` patterns to enable switch/case-like control flow in graph traversals.

**Estimated Duration:** 3-5 days

---

## Overview

This plan implements value-based multi-way branching steps. Unlike the existing binary `choose(condition, if_true, if_false)` step, these new steps route traversers to multiple possible branches based on computed values.

**Key Features:**
- `branch(traversal)` evaluates a traversal to produce a key, then routes to matching option
- `.option(key, traversal)` defines branch for specific key
- `.option_none(traversal)` defines default/fallback branch
- `choose_by(traversal)` is an alias providing the `choose().option()` pattern

---

## Phase 1: Core Types (Day 1)

### 1.1 Implement OptionKey Enum

**File:** `src/traversal/branch.rs`

**Tasks:**
- [ ] Add `OptionKey` enum with `Value(Value)` and `None` variants
- [ ] Implement `OptionKey::value()` and `OptionKey::none()` constructors
- [ ] Implement `From<T>` for common types (String, &str, i64, etc.)
- [ ] Add `OptionKeyWrapper` for HashMap usage (needs Hash + Eq)

**Implementation:**
```rust
/// Key for matching option branches
#[derive(Clone, Debug, PartialEq)]
pub enum OptionKey {
    /// Match a specific value
    Value(Value),
    /// Default fallback (Pick.none)
    None,
}

impl OptionKey {
    pub fn value<T: Into<Value>>(v: T) -> Self {
        OptionKey::Value(v.into())
    }
    
    pub fn none() -> Self {
        OptionKey::None
    }
}

// From implementations for ergonomic API
impl From<&str> for OptionKey {
    fn from(s: &str) -> Self {
        OptionKey::Value(Value::String(s.to_string()))
    }
}

impl From<String> for OptionKey {
    fn from(s: String) -> Self {
        OptionKey::Value(Value::String(s))
    }
}

impl From<i64> for OptionKey {
    fn from(n: i64) -> Self {
        OptionKey::Value(Value::Int(n))
    }
}

impl From<i32> for OptionKey {
    fn from(n: i32) -> Self {
        OptionKey::Value(Value::Int(n as i64))
    }
}

impl From<bool> for OptionKey {
    fn from(b: bool) -> Self {
        OptionKey::Value(Value::Bool(b))
    }
}
```

### 1.2 Implement OptionKeyWrapper

**File:** `src/traversal/branch.rs`

**Tasks:**
- [ ] Create `OptionKeyWrapper(OptionKey)` newtype
- [ ] Implement `Hash` using Value's hash implementation
- [ ] Implement `PartialEq` and `Eq`
- [ ] Implement `Clone`

**Implementation:**
```rust
#[derive(Clone, Debug)]
pub(crate) struct OptionKeyWrapper(pub OptionKey);

impl std::hash::Hash for OptionKeyWrapper {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self.0 {
            OptionKey::Value(v) => {
                0u8.hash(state);
                v.hash(state);
            }
            OptionKey::None => {
                1u8.hash(state);
            }
        }
    }
}

impl PartialEq for OptionKeyWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for OptionKeyWrapper {}
```

### 1.3 Unit Tests for OptionKey

**File:** `src/traversal/branch.rs` (tests module)

**Tasks:**
- [ ] Test `OptionKey::value()` with string
- [ ] Test `OptionKey::value()` with integer
- [ ] Test `OptionKey::none()`
- [ ] Test `From` implementations
- [ ] Test `OptionKeyWrapper` hash consistency
- [ ] Test `OptionKeyWrapper` equality

---

## Phase 2: BranchStep Implementation (Day 1-2)

### 2.1 Implement BranchStep Struct

**File:** `src/traversal/branch.rs`

**Tasks:**
- [ ] Define `BranchStep` struct with fields:
  - `branch_traversal: Traversal<Value, Value>`
  - `options: HashMap<OptionKeyWrapper, Traversal<Value, Value>>`
  - `none_branch: Option<Traversal<Value, Value>>`
- [ ] Implement `BranchStep::new(branch_traversal)`
- [ ] Implement `add_option(key, branch)` method
- [ ] Implement `add_none_option(branch)` method
- [ ] Derive/implement `Clone`

**Implementation:**
```rust
#[derive(Clone)]
pub struct BranchStep {
    branch_traversal: Traversal<Value, Value>,
    options: HashMap<OptionKeyWrapper, Traversal<Value, Value>>,
    none_branch: Option<Traversal<Value, Value>>,
}

impl BranchStep {
    pub fn new(branch_traversal: Traversal<Value, Value>) -> Self {
        Self {
            branch_traversal,
            options: HashMap::new(),
            none_branch: None,
        }
    }
    
    pub fn add_option<K: Into<OptionKey>>(
        mut self,
        key: K,
        branch: Traversal<Value, Value>,
    ) -> Self {
        let key = key.into();
        match key {
            OptionKey::None => {
                self.none_branch = Some(branch);
            }
            OptionKey::Value(_) => {
                self.options.insert(OptionKeyWrapper(key), branch);
            }
        }
        self
    }
    
    pub fn add_none_option(mut self, branch: Traversal<Value, Value>) -> Self {
        self.none_branch = Some(branch);
        self
    }
}
```

### 2.2 Implement AnyStep for BranchStep

**File:** `src/traversal/branch.rs`

**Tasks:**
- [ ] Implement `apply()` method with branching logic:
  1. Evaluate branch traversal to get key
  2. Look up matching option in HashMap
  3. Fall back to none_branch if no match
  4. Filter if no match and no none_branch
- [ ] Implement `clone_box()`
- [ ] Implement `name()` returning "branch"

**Implementation:**
```rust
impl AnyStep for BranchStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let branch_traversal = self.branch_traversal.clone();
        let options = self.options.clone();
        let none_branch = self.none_branch.clone();
        
        Box::new(input.flat_map(move |t| {
            // Evaluate branch traversal to get the key
            let branch_input = Box::new(std::iter::once(t.clone()));
            let mut branch_results = execute_traversal_from(ctx, &branch_traversal, branch_input);
            
            // Get the first result as the branch key
            let key_value = match branch_results.next() {
                Some(key_traverser) => Some(key_traverser.value),
                None => None,
            };
            
            // Find matching option
            let branch = match key_value {
                Some(key) => {
                    let option_key = OptionKeyWrapper(OptionKey::Value(key));
                    options.get(&option_key).or(none_branch.as_ref())
                }
                None => none_branch.as_ref(),
            };
            
            match branch {
                Some(branch) => {
                    let sub_input = Box::new(std::iter::once(t));
                    execute_traversal_from(ctx, branch, sub_input)
                        .collect::<Vec<_>>()
                        .into_iter()
                }
                None => Vec::new().into_iter(),
            }
        }))
    }
    
    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }
    
    fn name(&self) -> &'static str {
        "branch"
    }
}
```

### 2.3 Unit Tests for BranchStep

**File:** `src/traversal/branch.rs` (tests module)

**Tasks:**
- [ ] Test `BranchStep::new()` creates empty options
- [ ] Test `add_option()` adds to options map
- [ ] Test `add_option()` with `OptionKey::None` sets none_branch
- [ ] Test `add_none_option()` sets none_branch
- [ ] Test step is clonable
- [ ] Test step implements AnyStep
- [ ] Test step name is "branch"

---

## Phase 3: BranchBuilder Implementation (Day 2-3)

### 3.1 Implement BranchBuilder Struct

**File:** `src/traversal/source.rs`

**Tasks:**
- [ ] Define `BranchBuilder<'g, In>` struct with fields:
  - `snapshot: &'g GraphSnapshot<'g>`
  - `interner: &'g StringInterner`
  - `base: Traversal<In, Value>`
  - `branch_traversal: Traversal<Value, Value>`
  - `options: HashMap<OptionKeyWrapper, Traversal<Value, Value>>`
  - `none_branch: Option<Traversal<Value, Value>>`
- [ ] Implement `new()` constructor

**Implementation:**
```rust
pub struct BranchBuilder<'g, In> {
    snapshot: &'g GraphSnapshot<'g>,
    interner: &'g StringInterner,
    base: Traversal<In, Value>,
    branch_traversal: Traversal<Value, Value>,
    options: HashMap<OptionKeyWrapper, Traversal<Value, Value>>,
    none_branch: Option<Traversal<Value, Value>>,
}

impl<'g, In> BranchBuilder<'g, In> {
    pub(crate) fn new(
        snapshot: &'g GraphSnapshot<'g>,
        interner: &'g StringInterner,
        base: Traversal<In, Value>,
        branch_traversal: Traversal<Value, Value>,
    ) -> Self {
        Self {
            snapshot,
            interner,
            base,
            branch_traversal,
            options: HashMap::new(),
            none_branch: None,
        }
    }
}
```

### 3.2 Implement Option Methods on BranchBuilder

**File:** `src/traversal/source.rs`

**Tasks:**
- [ ] Implement `option(key, traversal)` method
- [ ] Implement `option_none(traversal)` method
- [ ] Both should return `Self` for chaining

**Implementation:**
```rust
impl<'g, In> BranchBuilder<'g, In> {
    pub fn option<K: Into<OptionKey>>(
        mut self,
        key: K,
        branch: Traversal<Value, Value>,
    ) -> Self {
        let key = key.into();
        match key {
            OptionKey::None => {
                self.none_branch = Some(branch);
            }
            OptionKey::Value(_) => {
                self.options.insert(OptionKeyWrapper(key), branch);
            }
        }
        self
    }
    
    pub fn option_none(mut self, branch: Traversal<Value, Value>) -> Self {
        self.none_branch = Some(branch);
        self
    }
}
```

### 3.3 Implement Finalize Method

**File:** `src/traversal/source.rs`

**Tasks:**
- [ ] Implement private `finalize()` method
- [ ] Constructs `BranchStep` from builder state
- [ ] Returns `BoundTraversal<'g, In, Value>`

**Implementation:**
```rust
impl<'g, In> BranchBuilder<'g, In> {
    fn finalize(self) -> BoundTraversal<'g, In, Value> {
        let mut step = BranchStep::new(self.branch_traversal);
        step.options = self.options;
        step.none_branch = self.none_branch;
        
        BoundTraversal::new(
            self.snapshot,
            self.interner,
            self.base.add_step(step),
        )
    }
}
```

### 3.4 Implement Terminal Methods on BranchBuilder

**File:** `src/traversal/source.rs`

**Tasks:**
- [ ] Implement `to_list()` -> `Vec<Value>`
- [ ] Implement `count()` -> `u64`
- [ ] Implement `next()` -> `Option<Value>`
- [ ] Implement `one()` -> `Result<Value, TraversalError>`
- [ ] Implement `iterate()` -> `()`
- [ ] Implement `has_next()` -> `bool`

**Implementation:**
```rust
impl<'g, In> BranchBuilder<'g, In> {
    pub fn to_list(self) -> Vec<Value> {
        self.finalize().to_list()
    }
    
    pub fn count(self) -> u64 {
        self.finalize().count()
    }
    
    pub fn next(self) -> Option<Value> {
        self.finalize().next()
    }
    
    pub fn one(self) -> Result<Value, TraversalError> {
        self.finalize().one()
    }
    
    pub fn iterate(self) {
        self.finalize().iterate()
    }
    
    pub fn has_next(self) -> bool {
        self.finalize().has_next()
    }
}
```

### 3.5 Implement Continuation Methods on BranchBuilder

**File:** `src/traversal/source.rs`

**Tasks:**
- [ ] Implement common navigation methods: `out()`, `in_()`, `both()`, `out_e()`, `in_e()`, `both_e()`
- [ ] Implement common filter methods: `has_label()`, `has_label_any()`, `has()`, `has_value()`, `has_where()`, `dedup()`, `limit()`
- [ ] Implement common transform methods: `values()`, `id()`, `label()`
- [ ] All delegate to `self.finalize().method()`

**Implementation Pattern:**
```rust
impl<'g, In> BranchBuilder<'g, In> {
    pub fn out(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().out()
    }
    
    pub fn in_(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().in_()
    }
    
    pub fn has_label(self, label: &str) -> BoundTraversal<'g, In, Value> {
        self.finalize().has_label(label)
    }
    
    pub fn values(self, key: &str) -> BoundTraversal<'g, In, Value> {
        self.finalize().values(key)
    }
    
    // ... etc for other methods
}
```

---

## Phase 4: BoundTraversal Methods (Day 3)

### 4.1 Add branch() Method to BoundTraversal

**File:** `src/traversal/source.rs`

**Tasks:**
- [ ] Add `branch(traversal)` method returning `BranchBuilder`
- [ ] Add documentation with example

**Implementation:**
```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Start a branch step with the given branch traversal
    /// 
    /// The branch traversal is evaluated for each input traverser to
    /// produce a key. The key is then matched against option branches.
    /// 
    /// # Example
    /// ```rust
    /// use interstellar::traversal::__;
    /// 
    /// // Route based on vertex label
    /// let results = g.v()
    ///     .branch(__::label())
    ///     .option("person", __::out_labels(&["knows"]))
    ///     .option("software", __::in_labels(&["created"]))
    ///     .to_list();
    /// ```
    pub fn branch(
        self,
        branch_traversal: Traversal<Value, Value>,
    ) -> BranchBuilder<'g, In> {
        BranchBuilder::new(
            self.snapshot,
            self.interner,
            self.traversal,
            branch_traversal,
        )
    }
}
```

### 4.2 Add choose_by() Method to BoundTraversal

**File:** `src/traversal/source.rs`

**Tasks:**
- [ ] Add `choose_by(traversal)` method as alias for `branch()`
- [ ] Add documentation explaining relationship to `choose()`

**Implementation:**
```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Start a choose-option step (alias for branch)
    /// 
    /// When `choose_by()` is called with a traversal, it creates a
    /// multi-way branch based on computed values. This is distinct
    /// from `choose(condition, if_true, if_false)` which is binary.
    /// 
    /// # Example
    /// ```rust
    /// use interstellar::traversal::__;
    /// 
    /// let results = g.v()
    ///     .choose_by(__::values("type"))
    ///     .option("admin", __::out_labels(&["manages"]))
    ///     .option("user", __::out_labels(&["uses"]))
    ///     .to_list();
    /// ```
    pub fn choose_by(
        self,
        branch_traversal: Traversal<Value, Value>,
    ) -> BranchBuilder<'g, In> {
        self.branch(branch_traversal)
    }
}
```

---

## Phase 5: Anonymous Traversal Support (Day 3)

### 5.1 Add branch() to __ Module

**File:** `src/traversal/mod.rs` (within `__` module)

**Tasks:**
- [ ] Add `branch(traversal)` function returning `Traversal<Value, Value>`
- [ ] Note: This creates a basic BranchStep; options must be added separately

**Implementation:**
```rust
pub mod __ {
    // ... existing functions ...
    
    /// Create a branch step for anonymous traversals
    /// 
    /// Note: Returns a traversal with a BranchStep that has no options.
    /// Options must be added by the caller if needed.
    pub fn branch(branch_traversal: Traversal<Value, Value>) -> Traversal<Value, Value> {
        Traversal::new().add_step(BranchStep::new(branch_traversal))
    }
}
```

### 5.2 Update Module Exports

**File:** `src/traversal/mod.rs`

**Tasks:**
- [ ] Export `OptionKey` from branch module
- [ ] Export `BranchStep` from branch module
- [ ] Export `BranchBuilder` from source module

**File:** `src/lib.rs` (prelude)

**Tasks:**
- [ ] Add `OptionKey` to prelude if needed

---

## Phase 6: Integration Tests (Day 4)

### 6.1 Create Integration Test File

**File:** `tests/branch.rs`

**Tasks:**
- [ ] Set up test graph with mixed vertex types (person, software, company)
- [ ] Test `branch()` with label-based routing
- [ ] Test `branch()` with property-based routing
- [ ] Test `option_none()` default branch
- [ ] Test filtering when no match and no default
- [ ] Test `choose_by()` equivalence
- [ ] Test with various key types (string, int, bool)
- [ ] Test path preservation through branch
- [ ] Test nested branching

**Test Cases:**
```rust
#[test]
fn test_branch_routes_by_label() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();
    
    let results = g.v()
        .branch(__::label())
        .option("person", __::out_labels(&["knows"]))
        .option("software", __::in_labels(&["created"]))
        .values("name")
        .to_list();
    
    // Verify person vertices went through out("knows")
    // Verify software vertices went through in("created")
}

#[test]
fn test_branch_with_none_default() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();
    
    let results = g.v()
        .branch(__::label())
        .option("person", __::values("name"))
        .option_none(__::constant("other"))
        .to_list();
    
    // Non-person vertices should produce "other"
}

#[test]
fn test_branch_filters_unmatched_without_none() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();
    
    let results = g.v()
        .branch(__::label())
        .option("person", __::identity())
        // No option_none, so non-person vertices are filtered
        .count();
    
    // Count should only include person vertices
}

#[test]
fn test_branch_with_property_value_key() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();
    
    let results = g.v()
        .has_label("person")
        .branch(__::values("status"))
        .option("active", __::out())
        .option("inactive", __::identity())
        .to_list();
}

#[test]
fn test_branch_with_integer_key() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();
    
    let results = g.v()
        .branch(__::values("priority"))
        .option(1i64, __::out_labels(&["urgent"]))
        .option(2i64, __::out_labels(&["normal"]))
        .option_none(__::identity())
        .to_list();
}

#[test]
fn test_choose_by_equivalent_to_branch() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();
    
    let branch_results = g.v()
        .branch(__::label())
        .option("person", __::out())
        .to_list();
    
    let choose_results = g.v()
        .choose_by(__::label())
        .option("person", __::out())
        .to_list();
    
    assert_eq!(branch_results, choose_results);
}

#[test]
fn test_branch_preserves_path() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();
    
    let results = g.v()
        .as_("start")
        .branch(__::label())
        .option("person", __::out().as_("middle"))
        .path()
        .to_list();
    
    // Verify path contains both labeled steps
}
```

---

## Phase 7: Documentation (Day 4-5)

### 7.1 Update Gremlin_api.md

**File:** `Gremlin_api.md`

**Tasks:**
- [ ] Update Branch Steps table:
  - Change `branch()` from `-` to `branch(traversal).option()`
  - Change `choose(traversal).option()` from `-` to `choose_by(traversal).option()`
- [ ] Update Modulator Steps table:
  - Change `option()` from `-` to `.option()` on `BranchBuilder`
- [ ] Update Implementation Summary counts
- [ ] Add example in Branch Steps section

**Changes:**
```markdown
## Branch Steps

| Gremlin Function | Rust Function | Module |
|-----------------|---------------|--------|
| `branch()` | `branch(traversal).option()` | `traversal::branch` |
| `choose(cond, true, false)` | `choose(condition, if_true, if_false)` | `traversal::branch` |
| `choose(traversal).option()` | `choose_by(traversal).option()` | `traversal::branch` |
| `union(traversal...)` | `union(traversals)` | `traversal::branch` |
| `coalesce(traversal...)` | `coalesce(traversals)` | `traversal::branch` |
| `optional(traversal)` | `optional(traversal)` | `traversal::branch` |
| `local(traversal)` | `local(traversal)` | `traversal::branch` |
```

### 7.2 Add Example

**File:** `examples/branch_steps.rs`

**Tasks:**
- [ ] Create example demonstrating branch() step
- [ ] Show label-based routing
- [ ] Show property-based routing
- [ ] Show default branch with option_none()
- [ ] Show choose_by() alternative syntax
- [ ] Add detailed comments

---

## Phase 8: Final Verification (Day 5)

### 8.1 Run Full Test Suite

**Tasks:**
- [ ] Run `cargo test` - all tests pass
- [ ] Run `cargo test --features mmap` - mmap tests pass
- [ ] Run `cargo clippy -- -D warnings` - no warnings
- [ ] Run `cargo fmt --check` - formatting correct

### 8.2 Code Coverage

**Tasks:**
- [ ] Run `cargo +nightly llvm-cov --branch --html`
- [ ] Verify >90% branch coverage on new code
- [ ] Add tests for any uncovered branches

### 8.3 Documentation Review

**Tasks:**
- [ ] Verify doc comments compile (`cargo doc`)
- [ ] Check example code in docs compiles
- [ ] Verify Gremlin_api.md is accurate

---

## Testing Checklist

### Unit Tests

**OptionKey:**
- [ ] Create from string literal
- [ ] Create from String
- [ ] Create from i64
- [ ] Create from bool
- [ ] Create None variant
- [ ] Hash consistency for wrapper

**BranchStep:**
- [ ] Construction with branch traversal
- [ ] Add single option
- [ ] Add multiple options
- [ ] Add none option via add_option
- [ ] Add none option via add_none_option
- [ ] Clone preserves options
- [ ] AnyStep trait implementation

**BranchBuilder:**
- [ ] Construction
- [ ] Chain multiple options
- [ ] Chain option_none
- [ ] Finalize creates correct step
- [ ] Terminal methods work

### Integration Tests

- [ ] Label-based routing
- [ ] Property-based routing
- [ ] Integer keys
- [ ] String keys
- [ ] Boolean keys
- [ ] None default branch
- [ ] Filter without none branch
- [ ] choose_by equivalence
- [ ] Path preservation
- [ ] Nested branching
- [ ] Empty branch traversal result uses none

---

## Dependencies

- Existing `branch.rs` module with `ChooseStep`
- Existing `Traversal<In, Out>` and `BoundTraversal<'g, In, Out>`
- Existing `execute_traversal_from()` helper
- Existing `Value` enum with `Hash` implementation

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Value hash collisions | Low | Use existing tested Value hash impl |
| Complex builder lifetime | Medium | Follow RepeatBuilder pattern |
| Option key type conversions | Low | Implement common From traits |
| Performance with many options | Low | HashMap gives O(1) lookup |

---

## Success Criteria

1. `BranchStep` routes traversers to correct option branches
2. `BranchBuilder` provides fluent configuration API
3. `option_none()` provides default fallback
4. Unmatched keys without none branch are filtered
5. Both string and numeric keys work correctly
6. `choose_by()` works identically to `branch()`
7. All tests pass with >90% branch coverage on new code
8. `Gremlin_api.md` updated accurately
9. Example file demonstrates functionality

---

## File Changes Summary

| File | Changes |
|------|---------|
| `src/traversal/branch.rs` | Add `OptionKey`, `OptionKeyWrapper`, `BranchStep` |
| `src/traversal/source.rs` | Add `BranchBuilder`, `branch()`, `choose_by()` methods |
| `src/traversal/mod.rs` | Update exports, add `__::branch()` |
| `tests/branch.rs` | New integration test file |
| `examples/branch_steps.rs` | New example file |
| `Gremlin_api.md` | Update documentation |

---

## Future Work (Out of Scope)

- `Pick.any` for first-match semantics
- `option(traversal, traversal)` where key is computed
- GQL CASE-WHEN compilation to branch steps
- Multi-value option matching
- Parallel branch execution
