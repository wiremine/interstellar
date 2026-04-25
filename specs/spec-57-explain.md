# Spec 57: `explain()` Terminal Method

## Status

Draft

## Summary

Add an `explain()` terminal method to `BoundTraversal` and `Traversal` that returns a structured description of the traversal pipeline **without executing it**. This is the Interstellar equivalent of TinkerPop's `explain()`.

## Motivation

- **Query debugging**: understand what steps a traversal contains before running it
- **Barrier detection**: identify barrier steps that prevent streaming execution
- **Step classification**: see at a glance which steps are filters, navigation, aggregation, etc.
- **Logging/docs**: produce a human-readable query plan string for logs or documentation

## Non-Goals

- This spec does **not** cover `profile()`. A side-effect `ProfileStep` already exists (`sideeffect.rs:548`) that collects inline timing metrics during execution. A future spec may add a terminal `profile()` that wraps every step in a `ProfilingStep` and returns a `TraversalProfile` — see `specs/explain_profile.md` Part 2 for that design.

## Relation to Existing Code

| What | Status |
|------|--------|
| `profile()` side-effect step | Implemented (`sideeffect::ProfileStep`) |
| `is_barrier()` on `DynStep` | Implemented (`step.rs:189`) |
| `dyn_name()` on `DynStep` | Implemented (`step.rs:186`) |
| `describe()` on `DynStep` | **Not implemented** — added by this spec |
| `category()` on `DynStep` | **Not implemented** — added by this spec |
| `TraversalExplanation` | **Not implemented** — added by this spec |
| `StepCategory` | **Not implemented** — added by this spec |

---

## Design

### 1. New Types

#### `StepCategory`

```rust
// src/traversal/explain.rs

/// Category of traversal step for classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepCategory {
    Source,
    Navigation,
    Filter,
    Transform,
    Aggregation,
    Branch,
    SideEffect,
    Modulator,
    Other,
}

impl fmt::Display for StepCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source => write!(f, "Source"),
            Self::Navigation => write!(f, "Navigation"),
            Self::Filter => write!(f, "Filter"),
            Self::Transform => write!(f, "Transform"),
            Self::Aggregation => write!(f, "Aggregation"),
            Self::Branch => write!(f, "Branch"),
            Self::SideEffect => write!(f, "SideEffect"),
            Self::Modulator => write!(f, "Modulator"),
            Self::Other => write!(f, "Other"),
        }
    }
}
```

#### `StepExplanation`

```rust
/// Explanation of a single traversal step.
#[derive(Debug, Clone)]
pub struct StepExplanation {
    /// Step name (from `dyn_name()`)
    pub name: &'static str,
    /// Zero-based index in the pipeline
    pub index: usize,
    /// Whether this step blocks streaming
    pub is_barrier: bool,
    /// Step category
    pub category: StepCategory,
    /// Optional human-readable description of step configuration
    pub description: Option<String>,
}
```

#### `TraversalExplanation`

```rust
/// Structured description of a traversal pipeline.
#[derive(Debug, Clone)]
pub struct TraversalExplanation {
    /// Source description (e.g., "V()", "V(1,2,3)", "E()")
    pub source: Option<String>,
    /// Ordered list of step explanations
    pub steps: Vec<StepExplanation>,
    /// Whether any step is a barrier
    pub has_barriers: bool,
    /// Total number of steps
    pub step_count: usize,
}
```

The `Display` impl produces:

```
Traversal Explanation
=====================
Source: V() [all vertices]
Barriers: No

Steps (3):
  [0] out        Navigation   labels: ["knows"]
  [1] hasLabel   Filter       labels: ["person"]
  [2] values     Transform    keys: ["name"]
```

### 2. Trait Extensions

Add two methods with defaults to `DynStep` (in `step.rs`):

```rust
pub trait DynStep: Send + Sync {
    // ... existing methods ...

    /// Human-readable description of step configuration.
    /// Default: None.
    fn describe(&self) -> Option<String> {
        None
    }

    /// Classification of this step.
    /// Default: StepCategory::Other.
    fn category(&self) -> StepCategory {
        StepCategory::Other
    }
}
```

The blanket `impl<S: Step> DynStep for S` forwards these to `Step` defaults:

```rust
fn describe(&self) -> Option<String> { Step::describe(self) }
fn category(&self) -> StepCategory { Step::category(self) }
```

Add corresponding defaults on `Step`:

```rust
pub trait Step: Clone + Send + Sync + 'static {
    // ... existing methods ...

    fn describe(&self) -> Option<String> { None }
    fn category(&self) -> StepCategory { StepCategory::Other }
}
```

### 3. API Surface

#### `BoundTraversal`

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Return a structured explanation of this traversal without executing it.
    ///
    /// Terminal method — consumes the traversal.
    pub fn explain(self) -> TraversalExplanation {
        TraversalExplanation::from_steps(
            self.traversal.source(),
            self.traversal.steps(),
        )
    }
}
```

#### `Traversal` (anonymous)

```rust
impl<In, Out> Traversal<In, Out> {
    /// Return a structured explanation of this anonymous traversal.
    pub fn explain(&self) -> TraversalExplanation {
        TraversalExplanation::from_steps(None, self.steps())
    }
}
```

### 4. Gremlin / GQL Integration

#### Gremlin

`explain()` should be recognized as a terminal step in the Gremlin parser:

```
g.V().out('knows').hasLabel('person').explain()
```

The compiler emits a `TraversalExplanation` value (not a list of traversers).

**Parser change**: Add `Explain` variant to `TerminalStep` in `gremlin/ast.rs`.  
**Compiler change**: In `gremlin/compiler.rs`, handle `TerminalStep::Explain` by calling `explain()` on the built traversal and returning the Display string as a `Value::String`.

#### GQL

Not applicable — `explain()` is a Gremlin-specific introspection method.

---

## Phase Plan

### Phase 1: Foundation

**Files**: `step.rs`

1. Add `describe() -> Option<String>` and `category() -> StepCategory` with defaults to `Step` trait
2. Add same to `DynStep` trait with defaults
3. Forward in blanket `impl<S: Step> DynStep for S`
4. Import `StepCategory` from the new `explain` module (created in Phase 2)

**Risk**: Adding methods to `Step` and `DynStep` is a breaking change for external implementors. Since this library is pre-1.0, this is acceptable. Both have default implementations so existing internal steps compile without changes.

### Phase 2: Explain Types and Display

**New file**: `src/traversal/explain.rs`

1. Define `StepCategory`, `StepExplanation`, `TraversalExplanation`
2. Implement `TraversalExplanation::from_steps(source, steps)` — iterates `&[Box<dyn DynStep>]`, calls `dyn_name()`, `is_barrier()`, `describe()`, `category()` on each
3. Implement `Display` for `TraversalExplanation` (tabular format shown above)
4. Implement `Display` for `StepCategory`
5. Export from `traversal/mod.rs`
6. Unit tests for empty traversal, step capture, barrier detection, display format

### Phase 3: API Surface

**Files**: `source.rs`, `pipeline.rs`

1. Add `explain()` terminal to `BoundTraversal`
2. Add `explain()` to `Traversal<In, Out>`
3. Export `TraversalExplanation`, `StepExplanation`, `StepCategory` from `lib.rs` prelude
4. Integration tests

### Phase 4: Step Descriptions

**Files**: `filter.rs`, `navigation.rs`, `transform/`, `aggregate.rs`, `branch.rs`, `sideeffect.rs`, `repeat.rs`

Implement `describe()` and `category()` overrides on each step. Priority:

**High** (commonly used, configuration-heavy):

| Step | `category()` | `describe()` |
|------|-------------|-------------|
| `HasLabelStep` | Filter | `labels: ["person"]` |
| `HasStep` | Filter | `key: "name"` |
| `HasValueStep` | Filter | `key: "age", value: 30` |
| `HasWhereStep` | Filter | `key: "age"` |
| `OutStep` / `InStep` / `BothStep` | Navigation | `labels: ["knows"]` |
| `OutEStep` / `InEStep` / `BothEStep` | Navigation | `labels: ["created"]` |
| `ValuesStep` | Transform | `keys: ["name", "age"]` |
| `LimitStep` | Filter | `limit: 10` |
| `RangeStep` | Filter | `range: 5..15` |
| `GroupStep` | Aggregation | (barrier) |
| `OrderStep` | Filter | (barrier) |
| `CountStep` | Aggregation | (barrier) |
| `FoldStep` | Aggregation | (barrier) |

**Medium**:

| Step | `category()` | `describe()` |
|------|-------------|-------------|
| `AsStep` | Modulator | `label: "a"` |
| `SelectStep` | Transform | `labels: ["a", "b"]` |
| `DedupStep` | Filter | — |
| `RepeatStep` | Branch | `times: 3` |
| `UnionStep` | Branch | — |
| `CoalesceStep` | Branch | — |
| `ChooseStep` | Branch | — |
| `ProfileStep` | SideEffect | — |
| `AggregateStep` | SideEffect | `key: "x"` |

**Low** (simple steps, `None` description):

| Step | `category()` |
|------|-------------|
| `IdentityStep` | Transform |
| `IdStep` | Transform |
| `LabelStep` | Transform |
| `PathStep` | Transform |
| `ConstantStep` | Transform |
| `ShortestPathStep` | Navigation |
| `DijkstraStep` | Navigation |

### Phase 5: Gremlin Parser Integration

**Files**: `gremlin/ast.rs`, `gremlin/parser.rs`, `gremlin/compiler.rs`

1. Add `Explain` to `TerminalStep` enum in AST
2. Parse `.explain()` as a terminal in the parser
3. Compile `TerminalStep::Explain` → call `explain()`, return `Display` string as `Value::String`
4. Tests for Gremlin string queries

### Phase 6: Documentation

1. Add doc comments to all public types
2. Update `docs/api/gremlin.md` to mark `explain()` as implemented
3. Optionally add an example: `examples/explain.rs`

---

## Testing Strategy

### Unit Tests (in `explain.rs`)

```rust
#[test]
fn explain_empty_traversal();          // 0 steps, no barriers
#[test]
fn explain_captures_step_names();      // out, hasLabel, values → correct names
#[test]
fn explain_detects_barriers();         // group step → has_barriers = true
#[test]
fn explain_step_categories();          // each step reports correct category
#[test]
fn explain_step_descriptions();        // hasLabel → "labels: [\"person\"]"
#[test]
fn explain_display_format();           // Display output contains expected strings
#[test]
fn explain_source_description();       // V() vs V(1,2) vs E()
```

### Integration Tests (in `tests/explain.rs`)

```rust
#[test]
fn explain_bound_traversal();          // g.v().out().has_label("person").explain()
#[test]
fn explain_anonymous_traversal();      // __.out().has_label("person").explain()
#[test]
fn explain_gremlin_string();           // "g.V().out('knows').explain()" via parser
#[test]
fn explain_complex_traversal();        // repeat, union, group — all steps captured
```

### Coverage Target

100% branch coverage on `explain.rs`. Each `StepCategory` variant and each `describe()` override must have at least one test.

---

## File Summary

### New Files

| File | Contents |
|------|----------|
| `src/traversal/explain.rs` | `StepCategory`, `StepExplanation`, `TraversalExplanation`, `Display` impls |
| `tests/explain.rs` | Integration tests |

### Modified Files

| File | Changes |
|------|---------|
| `src/traversal/step.rs` | Add `describe()`, `category()` to `Step` and `DynStep` traits |
| `src/traversal/mod.rs` | `pub mod explain;` |
| `src/traversal/source.rs` | `explain()` terminal on `BoundTraversal` |
| `src/traversal/pipeline.rs` | `explain()` on `Traversal<In, Out>` |
| `src/traversal/filter.rs` | `describe()` / `category()` overrides |
| `src/traversal/navigation.rs` | `describe()` / `category()` overrides |
| `src/traversal/transform/*.rs` | `describe()` / `category()` overrides |
| `src/traversal/aggregate.rs` | `describe()` / `category()` overrides |
| `src/traversal/branch.rs` | `describe()` / `category()` overrides |
| `src/traversal/sideeffect.rs` | `describe()` / `category()` overrides |
| `src/traversal/repeat.rs` | `describe()` / `category()` overrides |
| `src/lib.rs` | Prelude exports |
| `gremlin/ast.rs` | `TerminalStep::Explain` variant |
| `gremlin/parser.rs` | Parse `.explain()` |
| `gremlin/compiler.rs` | Compile `Explain` terminal |

---

## Performance

- `explain()` is O(n) in the number of steps — no graph access, no execution
- Zero runtime cost: no changes to the execution hot path
- `describe()` and `category()` are only called during `explain()`, never during normal traversal execution
