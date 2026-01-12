# Spec 15: Branch and Choose-Option Steps

**Phase 15 of Intersteller Implementation**

## Overview

This specification details the implementation of the `branch()` step and the `choose(traversal).option()` pattern - two powerful value-based branching mechanisms in Gremlin. These complement the existing `choose(condition, if_true, if_false)` step by enabling multi-way branching based on computed values rather than simple boolean conditions.

**Key Distinction:**
- **Existing `choose()`**: Binary branching based on whether a condition traversal produces results (boolean-like)
- **`branch()`**: Multi-way branching where a function traversal produces a value that selects which option branch to execute
- **`choose(traversal).option()`**: Multi-way branching with explicit value-to-branch mapping via `.option()` modifiers

Both patterns are essential for implementing switch/case-like control flow in graph traversals.

**Duration**: 3-5 days  
**Priority**: Medium  
**Dependencies**: Phase 4 (Anonymous Traversals), existing `ChooseStep` implementation

---

## Goals

1. Implement `BranchStep` with value-based multi-way branching
2. Implement `ChooseOptionStep` with `.option(value, traversal)` pattern
3. Support `Pick.none` for default/fallback branches
4. Ensure both steps integrate with the anonymous traversal `__` module
5. Add GQL support for CASE-WHEN expressions (optional, maps to choose-option)
6. Provide comprehensive test coverage

---

## TinkerPop Reference

### branch() Step

The `branch()` step evaluates a function traversal to produce a branch key, then routes the traverser to the matching option branch.

```groovy
// Gremlin
g.V().branch(values("name"))
     .option("alice", out("knows"))
     .option("bob", out("created"))
     .option(none, identity())
```

**Semantics:**
1. For each input traverser, evaluate the branch traversal to get a key
2. Route to the option branch matching that key
3. If no match and `none` option exists, use `none` branch
4. If no match and no `none` option, the traverser is filtered out

### choose(traversal).option() Pattern

An alternative syntax achieving the same result:

```groovy
// Gremlin
g.V().choose(values("type"))
     .option("person", out("knows"))
     .option("software", in("created"))
     .option(none, identity())
```

**Semantics:** Identical to `branch()` - the `choose(traversal)` without true/false branches creates an option-based selector.

### Pick Enum

TinkerPop defines `Pick` with two special values:
- `Pick.any` - Match any value (first-match semantics)
- `Pick.none` - Default fallback when no other option matches

Intersteller will implement `Pick::None` for default branches. `Pick::Any` is lower priority.

---

## Module Structure

This phase adds/modifies the following files:

| File | Description |
|------|-------------|
| `src/traversal/branch.rs` | Add `BranchStep`, `ChooseOptionStep`, `OptionKey` enum |
| `src/traversal/source.rs` | Add builder methods and `__` factory functions |
| `src/traversal/mod.rs` | Re-exports |

---

## Deliverables

### 5.1 OptionKey Enum

Represents keys used to match option branches:

```rust
/// Key for matching option branches
/// 
/// Represents either a specific value or a special Pick value.
#[derive(Clone, Debug, PartialEq)]
pub enum OptionKey {
    /// Match a specific value
    Value(Value),
    /// Default fallback (Pick.none)
    None,
    // Future: Any for first-match semantics
    // Any,
}

impl OptionKey {
    /// Create an option key from a value
    pub fn value<T: Into<Value>>(v: T) -> Self {
        OptionKey::Value(v.into())
    }
    
    /// Create the default/none option key
    pub fn none() -> Self {
        OptionKey::None
    }
}

impl<T: Into<Value>> From<T> for OptionKey {
    fn from(v: T) -> Self {
        OptionKey::Value(v.into())
    }
}
```

### 5.2 BranchStep

The core branching step that routes traversers based on a computed key:

```rust
use crate::traversal::context::ExecutionContext;
use crate::traversal::step::{execute_traversal_from, AnyStep};
use crate::traversal::{Traversal, Traverser};
use crate::value::Value;
use std::collections::HashMap;

/// Multi-way branching based on computed value
/// 
/// Evaluates the branch traversal to produce a key, then routes
/// the traverser to the matching option branch.
/// 
/// # Example
/// 
/// ```ignore
/// // Route based on vertex label
/// g.v().branch(__.label())
///      .option("person", __.out_labels(&["knows"]))
///      .option("software", __.in_labels(&["created"]))
///      .option_none(__.identity())
///      .to_list()
/// ```
#[derive(Clone)]
pub struct BranchStep {
    /// Traversal that produces the branch key
    branch_traversal: Traversal<Value, Value>,
    /// Map of option key to branch traversal
    options: HashMap<OptionKeyWrapper, Traversal<Value, Value>>,
    /// Default branch for unmatched keys (Pick.none)
    none_branch: Option<Traversal<Value, Value>>,
}

/// Wrapper for OptionKey to implement Hash/Eq for HashMap
/// 
/// Since Value may contain floats, we use a wrapper that
/// hashes based on the Value's canonical form.
#[derive(Clone, Debug)]
struct OptionKeyWrapper(OptionKey);

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

impl BranchStep {
    /// Create a new BranchStep with the given branch traversal
    pub fn new(branch_traversal: Traversal<Value, Value>) -> Self {
        Self {
            branch_traversal,
            options: HashMap::new(),
            none_branch: None,
        }
    }
    
    /// Add an option branch for a specific value
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
    
    /// Add the default/none option branch
    pub fn add_none_option(mut self, branch: Traversal<Value, Value>) -> Self {
        self.none_branch = Some(branch);
        self
    }
}

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
            let key = match branch_results.next() {
                Some(key_traverser) => key_traverser.value,
                None => {
                    // No key produced - use none branch if available
                    return match &none_branch {
                        Some(branch) => {
                            let sub_input = Box::new(std::iter::once(t));
                            execute_traversal_from(ctx, branch, sub_input)
                                .collect::<Vec<_>>()
                                .into_iter()
                        }
                        None => Vec::new().into_iter(),
                    };
                }
            };
            
            // Find matching option
            let option_key = OptionKeyWrapper(OptionKey::Value(key.clone()));
            let branch = options.get(&option_key).or(none_branch.as_ref());
            
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

### 5.3 ChooseOptionStep

Alternative syntax using `choose(traversal).option()`:

```rust
/// Multi-way branching using choose(traversal).option() pattern
/// 
/// Functionally identical to BranchStep, but constructed via
/// the choose().option() builder pattern.
/// 
/// # Example
/// 
/// ```ignore
/// g.v().choose(__.values("type"))
///      .option("person", __.out("knows"))
///      .option("software", __.in("created"))
///      .to_list()
/// ```
pub type ChooseOptionStep = BranchStep;

// The implementation is identical - we use a type alias.
// The distinction is in the builder API, not the step itself.
```

### 5.4 BranchBuilder Pattern

A builder for configuring branch/choose-option steps with fluent `.option()` calls:

```rust
/// Builder for branch step with options
/// 
/// Created by calling `.branch()` on a traversal, configured via
/// `.option()` calls, and finalized by continuing the traversal.
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
    
    /// Add an option branch for a specific value
    /// 
    /// # Example
    /// ```rust
    /// g.v().branch(__.label())
    ///      .option("person", __.out())
    ///      .option("software", __.in_())
    /// ```
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
    
    /// Add the default/none option branch
    /// 
    /// This branch is used when no other option matches.
    /// 
    /// # Example
    /// ```rust
    /// g.v().branch(__.label())
    ///      .option("person", __.out())
    ///      .option_none(__.identity())  // Default for non-person vertices
    /// ```
    pub fn option_none(mut self, branch: Traversal<Value, Value>) -> Self {
        self.none_branch = Some(branch);
        self
    }
    
    /// Finalize the branch configuration and return to normal traversal
    fn finalize(self) -> BoundTraversal<'g, In, Value> {
        let mut step = BranchStep::new(self.branch_traversal);
        for (key, branch) in self.options {
            step.options.insert(key, branch);
        }
        step.none_branch = self.none_branch;
        
        BoundTraversal::new(
            self.snapshot,
            self.interner,
            self.base.add_step(step),
        )
    }
}

// Terminal steps directly on BranchBuilder
impl<'g, In> BranchBuilder<'g, In> {
    /// Execute and collect results
    pub fn to_list(self) -> Vec<Value> {
        self.finalize().to_list()
    }
    
    /// Execute and count results
    pub fn count(self) -> u64 {
        self.finalize().count()
    }
    
    /// Get next result
    pub fn next(self) -> Option<Value> {
        self.finalize().next()
    }
    
    // Continue chaining steps after branch configuration
    pub fn has_label(self, label: &str) -> BoundTraversal<'g, In, Value> {
        self.finalize().has_label(label)
    }
    
    pub fn has_value(self, key: &str, value: impl Into<Value>) -> BoundTraversal<'g, In, Value> {
        self.finalize().has_value(key, value)
    }
    
    pub fn dedup(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().dedup()
    }
    
    pub fn values(self, key: &str) -> BoundTraversal<'g, In, Value> {
        self.finalize().values(key)
    }
    
    pub fn out(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().out()
    }
    
    pub fn out_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        self.finalize().out_labels(labels)
    }
    
    // ... other navigation/filter methods as needed
}
```

### 5.5 Builder Methods on BoundTraversal

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Start a branch step with the given branch traversal
    /// 
    /// The branch traversal is evaluated for each input traverser to
    /// produce a key. The key is then matched against option branches.
    /// 
    /// # Example
    /// ```rust
    /// g.v().branch(__.label())
    ///      .option("person", __.out_labels(&["knows"]))
    ///      .option("software", __.in_labels(&["created"]))
    ///      .to_list()
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
    
    /// Start a choose-option step (alias for branch)
    /// 
    /// When `choose()` is called with only a traversal (no true/false branches),
    /// it creates an option-based selector like `branch()`.
    /// 
    /// # Note
    /// This is distinct from `choose(condition, if_true, if_false)` which is
    /// binary branching. This version creates a multi-way branch.
    /// 
    /// # Example
    /// ```rust
    /// g.v().choose_by(__.values("type"))
    ///      .option("person", __.out())
    ///      .option("company", __.in_())
    ///      .to_list()
    /// ```
    pub fn choose_by(
        self,
        branch_traversal: Traversal<Value, Value>,
    ) -> BranchBuilder<'g, In> {
        // Functionally identical to branch()
        self.branch(branch_traversal)
    }
}
```

### 5.6 Anonymous Traversal Factory

```rust
// In src/traversal/mod.rs, within the __ module

/// Create a branch step for anonymous traversals
/// 
/// Note: Branch steps in anonymous traversals typically need to be
/// completed with options before being useful.
pub fn branch(branch_traversal: Traversal<Value, Value>) -> Traversal<Value, Value> {
    Traversal::new().add_step(BranchStep::new(branch_traversal))
}

// The __ module doesn't directly support the builder pattern,
// so branch steps in anonymous contexts use the direct step API
// or are constructed manually.
```

---

## Execution Flow

### Example: Label-based Routing

```
Input: [Alice(person), Linux(software), Bob(person)]

Branch traversal: __.label()

Options:
  "person"   -> __.out_labels(&["knows"])
  "software" -> __.in_labels(&["created"])
  none       -> __.identity()

Execution:

For Alice(person):
  Branch key: __.label() -> "person"
  Match: "person" -> __.out_labels(&["knows"])
  Result: [Bob, Carol]

For Linux(software):
  Branch key: __.label() -> "software"
  Match: "software" -> __.in_labels(&["created"])
  Result: [Marko, Josh]

For Bob(person):
  Branch key: __.label() -> "person"
  Match: "person" -> __.out_labels(&["knows"])
  Result: [Alice]

Output: [Bob, Carol, Marko, Josh, Alice]
```

### Example: With None Branch

```
Input: [Alice(person), Acme(company), Bob(person)]

Branch traversal: __.label()

Options:
  "person" -> __.out()
  none     -> __.constant("unknown")

Execution:

For Alice(person):
  Branch key: "person"
  Match: "person" -> __.out()
  Result: [Bob]

For Acme(company):
  Branch key: "company"
  No match for "company", use none -> __.constant("unknown")
  Result: ["unknown"]

For Bob(person):
  Branch key: "person"
  Match: "person" -> __.out()
  Result: [Alice]

Output: [Bob, "unknown", Alice]
```

---

## Test Cases

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // ─────────────────────────────────────────────────────────────
    // OptionKey Tests
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn test_option_key_from_string() {
        let key: OptionKey = "test".into();
        assert!(matches!(key, OptionKey::Value(Value::String(s)) if s == "test"));
    }

    #[test]
    fn test_option_key_from_int() {
        let key: OptionKey = 42i64.into();
        assert!(matches!(key, OptionKey::Value(Value::Int(42))));
    }

    #[test]
    fn test_option_key_none() {
        let key = OptionKey::none();
        assert!(matches!(key, OptionKey::None));
    }

    // ─────────────────────────────────────────────────────────────
    // BranchStep Tests
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn branch_step_compiles() {
        let branch_trav = Traversal::<Value, Value>::new();
        let step = BranchStep::new(branch_trav);
        assert_eq!(step.name(), "branch");
    }

    #[test]
    fn branch_step_add_option() {
        let branch_trav = Traversal::<Value, Value>::new();
        let option_trav = Traversal::<Value, Value>::new();
        
        let step = BranchStep::new(branch_trav)
            .add_option("test", option_trav);
        
        assert_eq!(step.options.len(), 1);
    }

    #[test]
    fn branch_step_add_none_option() {
        let branch_trav = Traversal::<Value, Value>::new();
        let none_trav = Traversal::<Value, Value>::new();
        
        let step = BranchStep::new(branch_trav)
            .add_none_option(none_trav);
        
        assert!(step.none_branch.is_some());
    }

    #[test]
    fn branch_step_is_clonable() {
        let branch_trav = Traversal::<Value, Value>::new();
        let step = BranchStep::new(branch_trav);
        let _cloned = step.clone();
    }

    // ─────────────────────────────────────────────────────────────
    // Integration Tests (require graph setup)
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn branch_routes_by_label() {
        // Setup: Graph with person and software vertices
        // Test: branch(__.label()).option("person", ...).option("software", ...)
        // Verify: Correct routing based on label
    }

    #[test]
    fn branch_uses_none_for_unmatched() {
        // Setup: Graph with vertices of various labels
        // Test: branch(__.label()).option("person", ...).option_none(...)
        // Verify: Unmatched labels use none branch
    }

    #[test]
    fn branch_filters_when_no_match_and_no_none() {
        // Setup: Graph with vertices of various labels
        // Test: branch(__.label()).option("person", ...) // no none
        // Verify: Non-person vertices are filtered out
    }

    #[test]
    fn branch_with_property_value() {
        // Setup: Graph with vertices having "type" property
        // Test: branch(__.values("type")).option("admin", ...).option("user", ...)
        // Verify: Routing based on property value
    }

    #[test]
    fn branch_with_numeric_keys() {
        // Setup: Graph with vertices having "priority" property (int)
        // Test: branch(__.values("priority")).option(1, ...).option(2, ...)
        // Verify: Routing works with integer keys
    }

    #[test]
    fn branch_preserves_path() {
        // Test that path metadata flows through branch steps
    }

    #[test]
    fn branch_with_empty_branch_traversal_result() {
        // When branch traversal produces no result, use none branch
    }

    #[test]
    fn choose_by_works_like_branch() {
        // Verify choose_by() is functionally equivalent to branch()
    }
}
```

### Integration Tests

```rust
// tests/branch.rs

#[test]
fn test_branch_label_routing() {
    let graph = setup_test_graph();
    let g = graph.traversal();
    
    // Route based on label
    let results = g.v()
        .branch(__::label())
        .option("person", __::out_labels(&["knows"]))
        .option("software", __::in_labels(&["created"]))
        .values("name")
        .to_list();
    
    // Verify expected results based on test graph structure
}

#[test]
fn test_branch_with_default() {
    let graph = setup_test_graph();
    let g = graph.traversal();
    
    let results = g.v()
        .branch(__::label())
        .option("person", __::values("name"))
        .option_none(__::constant("other"))
        .to_list();
    
    // Non-person vertices should produce "other"
}

#[test]
fn test_choose_by_pattern() {
    let graph = setup_test_graph();
    let g = graph.traversal();
    
    let results = g.v()
        .choose_by(__::values("type"))
        .option("primary", __::out())
        .option("secondary", __::in_())
        .to_list();
}

#[test]
fn test_nested_branch() {
    let graph = setup_test_graph();
    let g = graph.traversal();
    
    // Nested branching
    let results = g.v()
        .branch(__::label())
        .option("person", 
            __::branch(__::values("status"))
                .add_option("active", __::out())
                .add_option("inactive", __::identity())
        )
        .to_list();
}
```

---

## GQL Integration (Optional)

The `branch()` and `choose().option()` patterns map naturally to SQL-style CASE expressions:

```sql
-- GQL
MATCH (n)
RETURN CASE n.type
    WHEN 'person' THEN n.name
    WHEN 'company' THEN n.title
    ELSE 'unknown'
END
```

This could compile to:

```rust
g.v()
    .branch(__::values("type"))
    .option("person", __::values("name"))
    .option("company", __::values("title"))
    .option_none(__::constant("unknown"))
```

**Note:** GQL CASE support is optional for this phase. The primary deliverable is the Gremlin API.

---

## API Summary

### New Types

| Type | Description |
|------|-------------|
| `OptionKey` | Enum for option keys (`Value` or `None`) |
| `BranchStep` | Multi-way branching step |
| `BranchBuilder<'g, In>` | Builder for configuring branch options |

### New Methods on BoundTraversal

| Method | Returns | Description |
|--------|---------|-------------|
| `branch(traversal)` | `BranchBuilder` | Start multi-way branch |
| `choose_by(traversal)` | `BranchBuilder` | Alias for branch (choose-option pattern) |

### New Methods on BranchBuilder

| Method | Returns | Description |
|--------|---------|-------------|
| `option(key, traversal)` | `Self` | Add option for specific key |
| `option_none(traversal)` | `Self` | Add default branch |
| `to_list()` | `Vec<Value>` | Execute and collect |
| `count()` | `u64` | Execute and count |
| `next()` | `Option<Value>` | Get first result |
| (navigation/filter methods) | `BoundTraversal` | Continue traversal |

### Anonymous Traversal (`__`)

| Function | Description |
|----------|-------------|
| `__::branch(traversal)` | Create branch step |

---

## Comparison with Existing choose()

| Aspect | `choose(cond, true, false)` | `branch(trav).option()` |
|--------|----------------------------|------------------------|
| Branching | Binary (2-way) | Multi-way (N-way) |
| Condition | Existence check | Value equality |
| Syntax | `choose(c, t, f)` | `branch(t).option(k, b)...` |
| Default | `if_false` branch | `option_none()` |
| Use Case | Simple if/else | Switch/case |

---

## Implementation Notes

### Performance Considerations

1. **Option Lookup**: Use `HashMap` for O(1) option lookup by key
2. **Value Hashing**: Leverage existing `Value::hash()` implementation
3. **Lazy Evaluation**: Results collected per-traverser, not globally

### Thread Safety

- `BranchStep` is `Clone + Send + Sync`
- Options map is cloned per-execution (not shared)

### Error Handling

- No explicit errors; unmatched keys without `none` branch are filtered silently (TinkerPop behavior)

---

## Success Criteria

1. `BranchStep` routes traversers based on computed key
2. `BranchBuilder` provides fluent `.option()` configuration
3. `option_none()` handles unmatched keys
4. Unmatched keys without `none` branch are filtered (not errors)
5. Both string and numeric keys work correctly
6. Integration with existing traversal infrastructure
7. All tests pass with >90% branch coverage
8. `Gremlin_api.md` updated with new implementations

---

## Future Work (Out of Scope)

- `Pick.any` for first-match semantics
- `option(traversal, traversal)` pattern (key computed from traversal)
- `branch()` with multiple output values per option
- GQL CASE-WHEN compilation to branch steps
