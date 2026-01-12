# Spec 04: Anonymous Traversals and Predicates

**Phase 4 of Intersteller Implementation**

## Overview

This specification details the implementation of anonymous traversals (`__` factory module) and the predicate system (`p::` module). Anonymous traversals are a fundamental concept enabling composable, reusable traversal fragments that can be embedded within parent traversals for filtering, branching, and complex control flow.

**Key Architectural Point**: Anonymous traversals use the **same `Traversal<In, Out>` type** as bound traversals. The difference is that anonymous traversals have no source—they're pure step pipelines that receive their `ExecutionContext` when spliced into a parent traversal.

**Duration**: 2-3 weeks  
**Priority**: High  
**Dependencies**: Phase 3 (Traversal Engine Core)

---

## Goals

1. Implement `__` factory module for anonymous traversal creation
2. Create `p::` predicate module with comparison, range, string, and logical predicates
3. Implement filter steps: `where_`, `not`, `and_`, `or_`
4. Implement branch steps: `union`, `coalesce`, `choose`, `optional`, `local`
5. Implement `repeat` step with `times`, `until`, `emit`, `emit_if` modifiers
6. Create `has_where(key, predicate)` for predicate-based property filtering
7. Ensure all steps support cloning for branching operations
8. Provide comprehensive test coverage for all step types

---

## Deliverables

| File | Description |
|------|-------------|
| `src/traversal/predicate.rs` | `Predicate` trait, `p::` module, and `HasWhereStep` |
| `src/traversal/branch.rs` | Filter and branch steps: `where_`, `not`, `union`, etc. |
| `src/traversal/repeat.rs` | `RepeatStep`, `RepeatConfig`, `RepeatTraversal` builder |

**Note**: The `__` factory module already exists in `src/traversal/mod.rs` from Phase 3.

---

## Module Structure

This phase adds the following new files:

| File | Description |
|------|-------------|
| `src/traversal/predicate.rs` | `Predicate` trait, `p::` module, `HasWhereStep` |
| `src/traversal/branch.rs` | Filter steps (`WhereStep`, `NotStep`, `AndStep`, `OrStep`) and branch steps (`UnionStep`, `CoalesceStep`, `ChooseStep`, `OptionalStep`, `LocalStep`) |
| `src/traversal/repeat.rs` | `RepeatStep`, `RepeatConfig`, `RepeatTraversal` builder |

**Note**: The `__` factory module and chainable `Traversal<In, Value>` methods are **already implemented** in `src/traversal/mod.rs` as part of Phase 3. This phase adds new step types that integrate with the existing infrastructure.

### 4.1 Existing Infrastructure (Already Implemented)

The following are already in place from Phase 3:

- **`__` module** (`src/traversal/mod.rs`): Factory functions for anonymous traversals (`__::out()`, `__::has_label()`, etc.)
- **`Traversal<In, Out>`**: Chainable methods for anonymous traversals (`.out()`, `.has_label()`, etc.)
- **`execute_traversal()`** and **`execute_traversal_from()`**: Helpers for sub-traversal execution (`src/traversal/step.rs`)
- **`AnyStep` trait**: Type-erased step interface with `apply()`, `clone_box()`, `name()`

This phase extends the existing `__` module and `Traversal` with new step methods after implementing the underlying steps.

### 4.2 Filter Steps with Anonymous Traversals (`src/traversal/branch.rs`)

Filter steps use anonymous traversals to determine whether to pass or reject input traversers.

#### WhereStep

Filters the current traverser by executing a sub-traversal. The traverser is emitted only if the sub-traversal produces at least one result.

```rust
use crate::traversal::{ExecutionContext, Traversal, Traverser};
use crate::traversal::step::{AnyStep, execute_traversal_from};
use crate::value::Value;

/// Filter by sub-traversal existence
/// 
/// Emits input traverser only if the sub-traversal produces results.
#[derive(Clone)]
pub struct WhereStep {
    sub: Traversal<Value, Value>,
}

impl WhereStep {
    pub fn new(sub: Traversal<Value, Value>) -> Self {
        Self { sub }
    }
}

impl AnyStep for WhereStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let sub = self.sub.clone();
        Box::new(input.filter(move |t| {
            // Execute sub-traversal with current traverser as input
            let sub_input = Box::new(std::iter::once(t.clone()));
            let mut results = execute_traversal_from(ctx, &sub, sub_input);
            results.next().is_some() // Pass if any results
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "where"
    }
}
```

**Execution Flow:**
```
Input: [Alice, Bob, Carol]

For Alice:
  Alice → __.out_labels(&["knows"]) → [Bob, Dave]
          → .has_value("name", "Bob") → [Bob]
  Result: non-empty → EMIT Alice

For Bob:
  Bob → __.out_labels(&["knows"]) → [Carol]
        → .has_value("name", "Bob") → []
  Result: empty → FILTER Bob

For Carol:
  Carol → __.out_labels(&["knows"]) → []
  Result: empty → FILTER Carol

Output: [Alice]
```

#### NotStep

Inverse of `where_()`. Emits the traverser only if the sub-traversal produces zero results.

```rust
/// Filter by sub-traversal non-existence
/// 
/// Emits input traverser only if the sub-traversal produces NO results.
#[derive(Clone)]
pub struct NotStep {
    sub: Traversal<Value, Value>,
}

impl NotStep {
    pub fn new(sub: Traversal<Value, Value>) -> Self {
        Self { sub }
    }
}

impl AnyStep for NotStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let sub = self.sub.clone();
        Box::new(input.filter(move |t| {
            let sub_input = Box::new(std::iter::once(t.clone()));
            let mut results = execute_traversal_from(ctx, &sub, sub_input);
            results.next().is_none() // Pass if NO results
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "not"
    }
}
```

#### AndStep

Emits the traverser only if *all* sub-traversals produce at least one result.

```rust
/// Filter by multiple sub-traversals (AND logic)
/// 
/// Emits input traverser only if ALL sub-traversals produce results.
#[derive(Clone)]
pub struct AndStep {
    subs: Vec<Traversal<Value, Value>>,
}

impl AndStep {
    pub fn new(subs: Vec<Traversal<Value, Value>>) -> Self {
        Self { subs }
    }
}

impl AnyStep for AndStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let subs = self.subs.clone();
        Box::new(input.filter(move |t| {
            // All sub-traversals must produce at least one result
            subs.iter().all(|sub| {
                let sub_input = Box::new(std::iter::once(t.clone()));
                let mut results = execute_traversal_from(ctx, sub, sub_input);
                results.next().is_some()
            })
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "and"
    }
}
```

#### OrStep

Emits the traverser if *at least one* sub-traversal produces results.

```rust
/// Filter by multiple sub-traversals (OR logic)
/// 
/// Emits input traverser if ANY sub-traversal produces results.
#[derive(Clone)]
pub struct OrStep {
    subs: Vec<Traversal<Value, Value>>,
}

impl OrStep {
    pub fn new(subs: Vec<Traversal<Value, Value>>) -> Self {
        Self { subs }
    }
}

impl AnyStep for OrStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let subs = self.subs.clone();
        Box::new(input.filter(move |t| {
            // At least one sub-traversal must produce a result
            subs.iter().any(|sub| {
                let sub_input = Box::new(std::iter::once(t.clone()));
                let mut results = execute_traversal_from(ctx, sub, sub_input);
                results.next().is_some()
            })
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "or"
    }
}
```

#### Builder Methods

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Filter by sub-traversal producing results
    pub fn where_(self, sub: Traversal<Value, Value>) -> BoundTraversal<'g, In, Out> {
        self.add_step(WhereStep::new(sub))
    }

    /// Filter by sub-traversal NOT producing results
    pub fn not(self, sub: Traversal<Value, Value>) -> BoundTraversal<'g, In, Out> {
        self.add_step(NotStep::new(sub))
    }

    /// Filter: all sub-traversals must produce results
    pub fn and_(self, subs: Vec<Traversal<Value, Value>>) -> BoundTraversal<'g, In, Out> {
        self.add_step(AndStep::new(subs))
    }

    /// Filter: at least one sub-traversal must produce results
    pub fn or_(self, subs: Vec<Traversal<Value, Value>>) -> BoundTraversal<'g, In, Out> {
        self.add_step(OrStep::new(subs))
    }
}

// Also implement on Traversal for anonymous chaining
impl<In, Out> Traversal<In, Out> {
    pub fn where_(self, sub: Traversal<Value, Value>) -> Traversal<In, Out> {
        self.add_step(WhereStep::new(sub))
    }

    pub fn not(self, sub: Traversal<Value, Value>) -> Traversal<In, Out> {
        self.add_step(NotStep::new(sub))
    }

    pub fn and_(self, subs: Vec<Traversal<Value, Value>>) -> Traversal<In, Out> {
        self.add_step(AndStep::new(subs))
    }

    pub fn or_(self, subs: Vec<Traversal<Value, Value>>) -> Traversal<In, Out> {
        self.add_step(OrStep::new(subs))
    }
}
```

---

### 4.3 Branch Steps (`src/traversal/branch.rs`)

Branch steps execute multiple sub-traversals and merge or select results.

#### UnionStep

Executes multiple sub-traversals and merges their results. All sub-traversals receive the same input traverser.

```rust
/// Execute multiple branches and merge results
/// 
/// All branches receive each input traverser; results are interleaved
/// in traverser-major order (all results from one input before next).
#[derive(Clone)]
pub struct UnionStep {
    branches: Vec<Traversal<Value, Value>>,
}

impl UnionStep {
    pub fn new(branches: Vec<Traversal<Value, Value>>) -> Self {
        Self { branches }
    }
}

impl AnyStep for UnionStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let branches = self.branches.clone();
        
        Box::new(input.flat_map(move |t| {
            // For each input traverser, execute all branches
            branches.iter().flat_map(|branch| {
                let sub_input = Box::new(std::iter::once(t.clone()));
                execute_traversal_from(ctx, branch, sub_input)
            }).collect::<Vec<_>>().into_iter()
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "union"
    }
}
```

**Execution Flow (Traverser-Major Order):**
```
Input: [Alice]

Branch 1: Alice → __.out("knows") → [Bob, Carol]
Branch 2: Alice → __.out("works_with") → [Dave]

Interleaving:
  Alice[branch1] → Bob
  Alice[branch1] → Carol
  Alice[branch2] → Dave

Output: [Bob, Carol, Dave]
```

#### CoalesceStep

Tries each sub-traversal in order until one produces results. Short-circuits on first success.

```rust
/// Try branches in order, return first non-empty result
/// 
/// Short-circuits: once a branch produces results, remaining branches
/// are not evaluated for that input traverser.
#[derive(Clone)]
pub struct CoalesceStep {
    branches: Vec<Traversal<Value, Value>>,
}

impl CoalesceStep {
    pub fn new(branches: Vec<Traversal<Value, Value>>) -> Self {
        Self { branches }
    }
}

impl AnyStep for CoalesceStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let branches = self.branches.clone();
        
        Box::new(input.flat_map(move |t| {
            // Try each branch in order
            for branch in branches.iter() {
                let sub_input = Box::new(std::iter::once(t.clone()));
                let results: Vec<_> = execute_traversal_from(ctx, branch, sub_input).collect();
                
                if !results.is_empty() {
                    return results.into_iter();
                }
            }
            // No branch produced results
            Vec::new().into_iter()
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "coalesce"
    }
}
```

**Execution Flow:**
```
Input: [Alice (has nickname), Bob (no nickname)]

For Alice:
  Try __.values("nickname") → ["Ally"]
  Result: non-empty → EMIT "Ally", skip remaining branches

For Bob:
  Try __.values("nickname") → []
  Try __.values("first_name") → ["Robert"]
  Result: non-empty → EMIT "Robert"

Output: ["Ally", "Robert"]
```

#### ChooseStep

Conditional branching based on a predicate traversal.

```rust
/// Conditional branching
/// 
/// Evaluates condition traversal; if it produces results, executes
/// if_true branch, otherwise executes if_false branch.
#[derive(Clone)]
pub struct ChooseStep {
    condition: Traversal<Value, Value>,
    if_true: Traversal<Value, Value>,
    if_false: Traversal<Value, Value>,
}

impl ChooseStep {
    pub fn new(
        condition: Traversal<Value, Value>,
        if_true: Traversal<Value, Value>,
        if_false: Traversal<Value, Value>,
    ) -> Self {
        Self { condition, if_true, if_false }
    }
}

impl AnyStep for ChooseStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let condition = self.condition.clone();
        let if_true = self.if_true.clone();
        let if_false = self.if_false.clone();
        
        Box::new(input.flat_map(move |t| {
            // Evaluate condition
            let cond_input = Box::new(std::iter::once(t.clone()));
            let mut cond_result = execute_traversal_from(ctx, &condition, cond_input);
            
            let branch = if cond_result.next().is_some() {
                &if_true
            } else {
                &if_false
            };
            
            let sub_input = Box::new(std::iter::once(t));
            execute_traversal_from(ctx, branch, sub_input).collect::<Vec<_>>().into_iter()
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "choose"
    }
}
```

#### OptionalStep

Tries a sub-traversal but keeps the original traverser if the sub-traversal produces no results.

```rust
/// Optional traversal with fallback to input
/// 
/// If sub-traversal produces results, emit those results.
/// If sub-traversal produces no results, emit the original input.
#[derive(Clone)]
pub struct OptionalStep {
    sub: Traversal<Value, Value>,
}

impl OptionalStep {
    pub fn new(sub: Traversal<Value, Value>) -> Self {
        Self { sub }
    }
}

impl AnyStep for OptionalStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let sub = self.sub.clone();
        
        Box::new(input.flat_map(move |t| {
            let sub_input = Box::new(std::iter::once(t.clone()));
            let results: Vec<_> = execute_traversal_from(ctx, &sub, sub_input).collect();
            
            if results.is_empty() {
                // No results, emit original
                vec![t].into_iter()
            } else {
                results.into_iter()
            }
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "optional"
    }
}
```

#### LocalStep

Executes a sub-traversal in an isolated scope. Aggregations operate per-traverser, not globally.

```rust
/// Execute sub-traversal in isolated scope
/// 
/// Aggregations (count, fold, etc.) in the sub-traversal operate
/// independently for each input traverser, not across all inputs.
#[derive(Clone)]
pub struct LocalStep {
    sub: Traversal<Value, Value>,
}

impl LocalStep {
    pub fn new(sub: Traversal<Value, Value>) -> Self {
        Self { sub }
    }
}

impl AnyStep for LocalStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let sub = self.sub.clone();
        
        Box::new(input.flat_map(move |t| {
            // Execute sub-traversal for this traverser in isolation
            let sub_input = Box::new(std::iter::once(t));
            execute_traversal_from(ctx, &sub, sub_input).collect::<Vec<_>>().into_iter()
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "local"
    }
}
```

#### Builder Methods for Branch Steps

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Execute multiple branches, merge results
    pub fn union(self, branches: Vec<Traversal<Value, Value>>) -> BoundTraversal<'g, In, Value> {
        self.add_step(UnionStep::new(branches))
    }

    /// Try branches in order, return first non-empty
    pub fn coalesce(self, branches: Vec<Traversal<Value, Value>>) -> BoundTraversal<'g, In, Value> {
        self.add_step(CoalesceStep::new(branches))
    }

    /// Conditional branching
    pub fn choose(
        self,
        condition: Traversal<Value, Value>,
        if_true: Traversal<Value, Value>,
        if_false: Traversal<Value, Value>,
    ) -> BoundTraversal<'g, In, Value> {
        self.add_step(ChooseStep::new(condition, if_true, if_false))
    }

    /// Try sub-traversal, keep original if no results
    pub fn optional(self, sub: Traversal<Value, Value>) -> BoundTraversal<'g, In, Value> {
        self.add_step(OptionalStep::new(sub))
    }

    /// Execute sub-traversal in isolated scope
    pub fn local(self, sub: Traversal<Value, Value>) -> BoundTraversal<'g, In, Value> {
        self.add_step(LocalStep::new(sub))
    }
}
```

---

### 4.4 Repeat Step (`src/traversal/repeat.rs`)


The `repeat()` step enables iterative graph exploration with fine-grained control over termination and emission. It processes the graph in breadth-first order, maintaining a frontier of traversers at the current depth.

#### RepeatStep

```rust
use std::collections::VecDeque;

/// Iterative graph exploration with configurable termination and emission
/// 
/// Executes a sub-traversal repeatedly until termination condition is met.
/// Supports BFS frontier processing for level-by-level traversal.
#[derive(Clone)]
pub struct RepeatStep {
    /// The sub-traversal to repeat
    sub: Traversal<Value, Value>,
    /// Termination configuration
    config: RepeatConfig,
}

#[derive(Clone, Default)]
pub struct RepeatConfig {
    /// Maximum number of iterations (None = unlimited)
    times: Option<usize>,
    /// Termination condition - stop when this produces results
    until: Option<Traversal<Value, Value>>,
    /// Emit all intermediate results (not just final)
    emit: bool,
    /// Conditional emission - emit only when this produces results
    emit_if: Option<Traversal<Value, Value>>,
    /// Emit the initial input before first iteration
    emit_first: bool,
}

impl RepeatStep {
    pub fn new(sub: Traversal<Value, Value>) -> Self {
        Self {
            sub,
            config: RepeatConfig::default(),
        }
    }

    pub fn with_config(sub: Traversal<Value, Value>, config: RepeatConfig) -> Self {
        Self { sub, config }
    }

    /// Check if a traverser satisfies the until condition
    fn satisfies_until(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        match &self.config.until {
            Some(until_trav) => {
                let sub_input = Box::new(std::iter::once(traverser.clone()));
                let mut results = execute_traversal_from(ctx, until_trav, sub_input);
                results.next().is_some()
            }
            None => false,
        }
    }

    /// Check if a traverser should be emitted
    fn should_emit(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        if !self.config.emit {
            return false;
        }
        match &self.config.emit_if {
            Some(emit_trav) => {
                let sub_input = Box::new(std::iter::once(traverser.clone()));
                let mut results = execute_traversal_from(ctx, emit_trav, sub_input);
                results.next().is_some()
            }
            None => true, // emit() without condition emits all
        }
    }
}

impl AnyStep for RepeatStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let sub = self.sub.clone();
        let config = self.config.clone();
        let step = self.clone();
        
        Box::new(RepeatIterator::new(ctx, input, sub, config, step))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "repeat"
    }
}

/// Iterator for RepeatStep - processes BFS frontiers
struct RepeatIterator<'a> {
    ctx: &'a ExecutionContext<'a>,
    /// Queue of (traverser, loop_count)
    frontier: VecDeque<(Traverser, usize)>,
    /// Sub-traversal to apply each iteration
    sub: Traversal<Value, Value>,
    /// Configuration
    config: RepeatConfig,
    /// Step reference for condition checks
    step: RepeatStep,
    /// Buffered results to emit
    emit_buffer: VecDeque<Traverser>,
    /// Whether we've processed initial input
    initialized: bool,
    /// Original input iterator
    input: Option<Box<dyn Iterator<Item = Traverser> + 'a>>,
}

impl<'a> RepeatIterator<'a> {
    fn new(
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
        sub: Traversal<Value, Value>,
        config: RepeatConfig,
        step: RepeatStep,
    ) -> Self {
        Self {
            ctx,
            frontier: VecDeque::new(),
            sub,
            config,
            step,
            emit_buffer: VecDeque::new(),
            initialized: false,
            input: Some(input),
        }
    }

    fn process_frontier(&mut self) {
        // Process one level of the BFS frontier
        let current_frontier: Vec<_> = self.frontier.drain(..).collect();
        
        for (traverser, loop_count) in current_frontier {
            // Check times limit
            if let Some(max_times) = self.config.times {
                if loop_count >= max_times {
                    // Emit final result if not using emit mode
                    if !self.config.emit {
                        self.emit_buffer.push_back(traverser);
                    }
                    continue;
                }
            }

            // Check until condition BEFORE iteration
            if self.step.satisfies_until(self.ctx, &traverser) {
                // Emit and stop iterating for this traverser
                self.emit_buffer.push_back(traverser);
                continue;
            }

            // Execute sub-traversal
            let sub_input = Box::new(std::iter::once(traverser.clone()));
            let results: Vec<_> = execute_traversal_from(self.ctx, &self.sub, sub_input).collect();

            if results.is_empty() {
                // No more results from this branch
                // Emit if not already emitted via emit mode
                if !self.config.emit {
                    self.emit_buffer.push_back(traverser);
                }
            } else {
                // Add results to next frontier
                for result in results {
                    // Increment loop count on the traverser using the method from Traverser
                    let mut new_traverser = result;
                    new_traverser.inc_loops();

                    // Emit intermediate if configured
                    if self.step.should_emit(self.ctx, &new_traverser) {
                        self.emit_buffer.push_back(new_traverser.clone());
                    }

                    // Add to frontier for next iteration (loops field already incremented)
                    self.frontier.push_back((new_traverser, loop_count + 1));
                }
            }
        }
    }
}

impl<'a> Iterator for RepeatIterator<'a> {
    type Item = Traverser;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Return buffered emissions first
            if let Some(t) = self.emit_buffer.pop_front() {
                return Some(t);
            }

            // Initialize from input on first call
            if !self.initialized {
                self.initialized = true;
                if let Some(input) = self.input.take() {
                    for t in input {
                        // Emit initial if configured
                        if self.config.emit_first && self.config.emit {
                            self.emit_buffer.push_back(t.clone());
                        }
                        self.frontier.push_back((t, 0));
                    }
                }
            }

            // Process frontier if we have work
            if !self.frontier.is_empty() {
                self.process_frontier();
                continue;
            }

            // No more work
            return None;
        }
    }
}
```

#### RepeatTraversal Builder

A builder pattern for configuring repeat behavior:

```rust
/// Builder for configuring repeat step behavior
/// 
/// Created by calling `.repeat()` on a traversal, configured via
/// chained methods, and finalized by continuing the traversal.
pub struct RepeatTraversal<'g, In> {
    snapshot: &'g GraphSnapshot<'g>,
    interner: &'g StringInterner,
    base: Traversal<In, Value>,
    sub: Traversal<Value, Value>,
    config: RepeatConfig,
}

impl<'g, In> RepeatTraversal<'g, In> {
    pub(crate) fn new(
        snapshot: &'g GraphSnapshot<'g>,
        interner: &'g StringInterner,
        base: Traversal<In, Value>,
        sub: Traversal<Value, Value>,
    ) -> Self {
        Self {
            snapshot,
            interner,
            base,
            sub,
            config: RepeatConfig::default(),
        }
    }

    /// Execute exactly n iterations
    /// 
    /// # Example
    /// ```rust
    /// // Get friends-of-friends (2 hops exactly)
    /// g.v().has_value("name", "Alice")
    ///     .repeat(__.out_labels(&["knows"]))
    ///     .times(2)
    /// ```
    pub fn times(mut self, n: usize) -> Self {
        self.config.times = Some(n);
        self
    }

    /// Continue until the condition traversal produces results
    /// 
    /// # Example
    /// ```rust
    /// // Traverse until reaching a company vertex
    /// g.v().has_value("name", "Alice")
    ///     .repeat(__.out())
    ///     .until(__.has_label("company"))
    /// ```
    pub fn until(mut self, condition: Traversal<Value, Value>) -> Self {
        self.config.until = Some(condition);
        self
    }

    /// Emit results from all iterations (not just final)
    /// 
    /// # Example
    /// ```rust
    /// // Get all vertices within 3 hops
    /// g.v().has_value("name", "Alice")
    ///     .repeat(__.out())
    ///     .times(3)
    ///     .emit()
    /// ```
    pub fn emit(mut self) -> Self {
        self.config.emit = true;
        self
    }

    /// Emit results that satisfy a condition
    /// 
    /// # Example
    /// ```rust
    /// // Traverse up to 5 hops, emit only people
    /// g.v().repeat(__.out())
    ///     .times(5)
    ///     .emit_if(__.has_label("person"))
    /// ```
    pub fn emit_if(mut self, condition: Traversal<Value, Value>) -> Self {
        self.config.emit = true;
        self.config.emit_if = Some(condition);
        self
    }

    /// Emit the initial input before the first iteration
    /// 
    /// # Example
    /// ```rust
    /// // Include starting vertex in results
    /// g.v().has_value("name", "Alice")
    ///     .repeat(__.out())
    ///     .times(2)
    ///     .emit()
    ///     .emit_first()
    /// ```
    pub fn emit_first(mut self) -> Self {
        self.config.emit_first = true;
        self
    }

    /// Finalize repeat configuration and return to normal traversal
    fn finalize(self) -> BoundTraversal<'g, In, Value> {
        let repeat_step = RepeatStep::with_config(self.sub, self.config);
        BoundTraversal::new(
            self.snapshot,
            self.interner,
            self.base.add_step(repeat_step),
        )
    }
}

// Terminal steps directly on RepeatTraversal
impl<'g, In> RepeatTraversal<'g, In> {
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

    // Continue chaining steps after repeat configuration
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

    // ... other navigation/filter methods
}
```

#### Builder Method on BoundTraversal

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Start a repeat loop with the given sub-traversal
    /// 
    /// Returns a `RepeatTraversal` builder for configuring termination
    /// and emission behavior.
    /// 
    /// # Example
    /// ```rust
    /// g.v().has_value("name", "Alice")
    ///     .repeat(__.out_labels(&["knows"]))
    ///     .times(2)
    ///     .dedup()
    ///     .to_list()
    /// ```
    pub fn repeat(self, sub: Traversal<Value, Value>) -> RepeatTraversal<'g, In> {
        RepeatTraversal::new(
            self.snapshot,
            self.interner,
            self.traversal,  // Convert Out -> Value implicitly
            sub,
        )
    }
}
```

**Execution Flow - times(2):**
```
Iteration 0: [Alice]
             ↓ __.out("knows")
Iteration 1: [Bob, Carol]
             ↓ __.out("knows")
Iteration 2: [Dave, Eve, Frank]
             (stop - times(2) reached)

Output: [Dave, Eve, Frank]
```

**Execution Flow - until() with emit():**
```
Graph: Alice → Bob → Acme(company)
                  ↘ Carol → BigCorp(company)

g.v("Alice").repeat(__.out()).until(__.has_label("company")).emit()

Iteration 0: [Alice]
  emit: Alice
  ↓ __.out()
Iteration 1: [Bob]
  emit: Bob
  ↓ __.out()
Iteration 2: [Acme, Carol]
  Acme: satisfies until → emit & stop
  Carol: emit
  ↓ __.out() (Carol only)
Iteration 3: [BigCorp]
  BigCorp: satisfies until → emit & stop

Output: [Alice, Bob, Acme, Carol, BigCorp]
```

---

### 4.5 Predicate Module (`src/traversal/predicate.rs`)

The predicate system provides declarative filtering with composable comparison, range, string, and logical operations. Predicates are used with `has_where()` to filter elements based on property values.

#### Predicate Trait

```rust
use crate::value::Value;

/// A predicate that tests a Value
/// 
/// Predicates are composable and can be used with `has_where()` to filter
/// traversers based on property values.
/// 
/// # Design Note
/// Like `AnyStep`, predicates use `clone_box()` for cloning trait objects.
/// This enables storing predicates as `Box<dyn Predicate>` while supporting
/// Clone via explicit method rather than the `Clone` trait bound.
pub trait Predicate: Send + Sync {
    /// Test if the predicate matches the given value
    fn test(&self, value: &Value) -> bool;
    
    /// Clone this predicate into a boxed trait object
    fn clone_box(&self) -> Box<dyn Predicate>;
}

// Enable cloning of Box<dyn Predicate>
impl Clone for Box<dyn Predicate> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
```

#### The `p` Module

```rust
/// Predicate factory module
/// 
/// Provides comparison, range, string, and logical predicates for use
/// with `has_where()`.
/// 
/// # Example
/// ```rust
/// use intersteller::prelude::*;
/// 
/// g.v().has_label("person")
///     .has_where("age", p::gte(18))
///     .has_where("name", p::starting_with("A"))
///     .to_list()
/// ```
pub mod p {
    use super::*;

    /// Helper macro to implement Predicate for simple structs
    macro_rules! impl_predicate {
        ($ty:ty) => {
            impl Predicate for $ty {
                fn clone_box(&self) -> Box<dyn Predicate> {
                    Box::new(self.clone())
                }
            }
        };
    }

    // ─────────────────────────────────────────────────────────────
    // Comparison Predicates
    // ─────────────────────────────────────────────────────────────

    /// Equal to
    #[derive(Clone)]
    pub struct Eq(Value);

    impl Predicate for Eq {
        fn test(&self, value: &Value) -> bool {
            value == &self.0
        }
        
        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    pub fn eq<T: Into<Value>>(value: T) -> impl Predicate {
        Eq(value.into())
    }

    /// Not equal to
    #[derive(Clone)]
    pub struct Neq(Value);

    impl Predicate for Neq {
        fn test(&self, value: &Value) -> bool {
            value != &self.0
        }
        
        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    pub fn neq<T: Into<Value>>(value: T) -> impl Predicate {
        Neq(value.into())
    }

    /// Less than
    #[derive(Clone)]
    pub struct Lt(Value);

    impl Predicate for Lt {
        fn test(&self, value: &Value) -> bool {
            match (value, &self.0) {
                (Value::Int(a), Value::Int(b)) => a < b,
                (Value::Float(a), Value::Float(b)) => a < b,
                (Value::Int(a), Value::Float(b)) => (*a as f64) < *b,
                (Value::Float(a), Value::Int(b)) => *a < (*b as f64),
                (Value::String(a), Value::String(b)) => a < b,
                _ => false,
            }
        }
        
        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    pub fn lt<T: Into<Value>>(value: T) -> impl Predicate {
        Lt(value.into())
    }

    /// Less than or equal to
    #[derive(Clone)]
    pub struct Lte(Value);

    impl Predicate for Lte {
        fn test(&self, value: &Value) -> bool {
            match (value, &self.0) {
                (Value::Int(a), Value::Int(b)) => a <= b,
                (Value::Float(a), Value::Float(b)) => a <= b,
                (Value::Int(a), Value::Float(b)) => (*a as f64) <= *b,
                (Value::Float(a), Value::Int(b)) => *a <= (*b as f64),
                (Value::String(a), Value::String(b)) => a <= b,
                _ => false,
            }
        }
        
        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    pub fn lte<T: Into<Value>>(value: T) -> impl Predicate {
        Lte(value.into())
    }

    /// Greater than
    #[derive(Clone)]
    pub struct Gt(Value);

    impl Predicate for Gt {
        fn test(&self, value: &Value) -> bool {
            match (value, &self.0) {
                (Value::Int(a), Value::Int(b)) => a > b,
                (Value::Float(a), Value::Float(b)) => a > b,
                (Value::Int(a), Value::Float(b)) => (*a as f64) > *b,
                (Value::Float(a), Value::Int(b)) => *a > (*b as f64),
                (Value::String(a), Value::String(b)) => a > b,
                _ => false,
            }
        }
        
        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    pub fn gt<T: Into<Value>>(value: T) -> impl Predicate {
        Gt(value.into())
    }

    /// Greater than or equal to
    #[derive(Clone)]
    pub struct Gte(Value);

    impl Predicate for Gte {
        fn test(&self, value: &Value) -> bool {
            match (value, &self.0) {
                (Value::Int(a), Value::Int(b)) => a >= b,
                (Value::Float(a), Value::Float(b)) => a >= b,
                (Value::Int(a), Value::Float(b)) => (*a as f64) >= *b,
                (Value::Float(a), Value::Int(b)) => *a >= (*b as f64),
                (Value::String(a), Value::String(b)) => a >= b,
                _ => false,
            }
        }
        
        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    pub fn gte<T: Into<Value>>(value: T) -> impl Predicate {
        Gte(value.into())
    }

    // ─────────────────────────────────────────────────────────────
    // Range Predicates
    // ─────────────────────────────────────────────────────────────

    /// Between (inclusive start, exclusive end): start <= value < end
    #[derive(Clone)]
    pub struct Between(Value, Value);

    impl Predicate for Between {
        fn test(&self, value: &Value) -> bool {
            Gte(self.0.clone()).test(value) && Lt(self.1.clone()).test(value)
        }
        
        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    pub fn between<T: Into<Value>>(start: T, end: T) -> impl Predicate {
        Between(start.into(), end.into())
    }

    /// Inside (exclusive both): start < value < end
    #[derive(Clone)]
    pub struct Inside(Value, Value);

    impl Predicate for Inside {
        fn test(&self, value: &Value) -> bool {
            Gt(self.0.clone()).test(value) && Lt(self.1.clone()).test(value)
        }
        
        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    pub fn inside<T: Into<Value>>(start: T, end: T) -> impl Predicate {
        Inside(start.into(), end.into())
    }

    /// Outside: value < start OR value > end
    #[derive(Clone)]
    pub struct Outside(Value, Value);

    impl Predicate for Outside {
        fn test(&self, value: &Value) -> bool {
            Lt(self.0.clone()).test(value) || Gt(self.1.clone()).test(value)
        }
        
        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    pub fn outside<T: Into<Value>>(start: T, end: T) -> impl Predicate {
        Outside(start.into(), end.into())
    }

    // ─────────────────────────────────────────────────────────────
    // Collection Predicates
    // ─────────────────────────────────────────────────────────────

    /// Value is in the given set
    #[derive(Clone)]
    pub struct Within(Vec<Value>);

    impl Predicate for Within {
        fn test(&self, value: &Value) -> bool {
            self.0.contains(value)
        }
        
        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    pub fn within<T, I>(values: I) -> impl Predicate
    where
        T: Into<Value>,
        I: IntoIterator<Item = T>,
    {
        Within(values.into_iter().map(Into::into).collect())
    }

    /// Value is NOT in the given set
    #[derive(Clone)]
    pub struct Without(Vec<Value>);

    impl Predicate for Without {
        fn test(&self, value: &Value) -> bool {
            !self.0.contains(value)
        }
        
        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    pub fn without<T, I>(values: I) -> impl Predicate
    where
        T: Into<Value>,
        I: IntoIterator<Item = T>,
    {
        Without(values.into_iter().map(Into::into).collect())
    }

    // ─────────────────────────────────────────────────────────────
    // String Predicates
    // ─────────────────────────────────────────────────────────────

    /// String contains substring
    #[derive(Clone)]
    pub struct Containing(String);

    impl Predicate for Containing {
        fn test(&self, value: &Value) -> bool {
            match value {
                Value::String(s) => s.contains(&self.0),
                _ => false,
            }
        }
        
        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    pub fn containing(substring: &str) -> impl Predicate {
        Containing(substring.to_string())
    }

    /// String starts with prefix
    #[derive(Clone)]
    pub struct StartingWith(String);

    impl Predicate for StartingWith {
        fn test(&self, value: &Value) -> bool {
            match value {
                Value::String(s) => s.starts_with(&self.0),
                _ => false,
            }
        }
        
        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    pub fn starting_with(prefix: &str) -> impl Predicate {
        StartingWith(prefix.to_string())
    }

    /// String ends with suffix
    #[derive(Clone)]
    pub struct EndingWith(String);

    impl Predicate for EndingWith {
        fn test(&self, value: &Value) -> bool {
            match value {
                Value::String(s) => s.ends_with(&self.0),
                _ => false,
            }
        }
        
        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    pub fn ending_with(suffix: &str) -> impl Predicate {
        EndingWith(suffix.to_string())
    }

    /// String matches regex pattern
    #[derive(Clone)]
    pub struct Regex {
        pattern: String,
        compiled: regex::Regex,
    }

    impl Predicate for Regex {
        fn test(&self, value: &Value) -> bool {
            match value {
                Value::String(s) => self.compiled.is_match(s),
                _ => false,
            }
        }
        
        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create a regex predicate
    /// 
    /// # Panics
    /// Panics if the pattern is invalid. Use `try_regex` for fallible creation.
    pub fn regex(pattern: &str) -> impl Predicate {
        Regex {
            pattern: pattern.to_string(),
            compiled: regex::Regex::new(pattern)
                .unwrap_or_else(|e| panic!("Invalid regex pattern '{}': {}", pattern, e)),
        }
    }

    /// Create a regex predicate, returning None if pattern is invalid
    pub fn try_regex(pattern: &str) -> Option<impl Predicate> {
        regex::Regex::new(pattern).ok().map(|compiled| Regex {
            pattern: pattern.to_string(),
            compiled,
        })
    }

    // ─────────────────────────────────────────────────────────────
    // Logical Composition
    // ─────────────────────────────────────────────────────────────

    /// Logical AND of two predicates
    #[derive(Clone)]
    pub struct And<P1, P2>(P1, P2);

    impl<P1: Predicate + Clone + 'static, P2: Predicate + Clone + 'static> Predicate for And<P1, P2> {
        fn test(&self, value: &Value) -> bool {
            self.0.test(value) && self.1.test(value)
        }
        
        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    pub fn and<P1: Predicate + Clone + 'static, P2: Predicate + Clone + 'static>(p1: P1, p2: P2) -> impl Predicate {
        And(p1, p2)
    }

    /// Logical OR of two predicates
    #[derive(Clone)]
    pub struct Or<P1, P2>(P1, P2);

    impl<P1: Predicate + Clone + 'static, P2: Predicate + Clone + 'static> Predicate for Or<P1, P2> {
        fn test(&self, value: &Value) -> bool {
            self.0.test(value) || self.1.test(value)
        }
        
        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    pub fn or<P1: Predicate + Clone + 'static, P2: Predicate + Clone + 'static>(p1: P1, p2: P2) -> impl Predicate {
        Or(p1, p2)
    }

    /// Logical NOT of a predicate
    #[derive(Clone)]
    pub struct Not<P>(P);

    impl<P: Predicate + Clone + 'static> Predicate for Not<P> {
        fn test(&self, value: &Value) -> bool {
            !self.0.test(value)
        }
        
        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    pub fn not<P: Predicate + Clone + 'static>(p: P) -> impl Predicate {
        Not(p)
    }
}
```

#### HasWhereStep

Filters traversers based on a property value matching a predicate:

```rust
/// Filter by property with predicate
/// 
/// Extracts the property value and tests against the predicate.
/// 
/// # Note
/// `P` must be `Clone` to support traversal cloning for branching operations.
#[derive(Clone)]
pub struct HasWhereStep<P: Predicate + Clone> {
    key: String,
    predicate: P,
}

impl<P: Predicate + Clone> HasWhereStep<P> {
    pub fn new(key: impl Into<String>, predicate: P) -> Self {
        Self {
            key: key.into(),
            predicate,
        }
    }

    fn matches(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        let prop_value = match &traverser.value {
            Value::Vertex(id) => {
                ctx.snapshot
                    .get_vertex(*id)
                    .and_then(|v| v.property(&self.key).cloned())
            }
            Value::Edge(id) => {
                ctx.snapshot
                    .get_edge(*id)
                    .and_then(|e| e.property(&self.key).cloned())
            }
            _ => None,
        };

        match prop_value {
            Some(value) => self.predicate.test(&value),
            None => false,
        }
    }
}

impl<P: Predicate + Clone + 'static> AnyStep for HasWhereStep<P> {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let step = self.clone();
        Box::new(input.filter(move |t| step.matches(ctx, t)))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "hasWhere"
    }
}
```

#### Type-Erased HasWhereStep

For dynamic predicate usage:

```rust
/// Type-erased version for dynamic predicates
/// 
/// Unlike `HasWhereStep<P>`, this stores a boxed predicate for cases
/// where the predicate type isn't known at compile time.
pub struct HasWhereStepDyn {
    key: String,
    predicate: Box<dyn Predicate>,
}

impl Clone for HasWhereStepDyn {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            predicate: self.predicate.clone_box(),
        }
    }
}

impl HasWhereStepDyn {
    pub fn new(key: impl Into<String>, predicate: impl Predicate + 'static) -> Self {
        Self {
            key: key.into(),
            predicate: Box::new(predicate),
        }
    }
    
    /// Create from a pre-boxed predicate
    pub fn from_boxed(key: impl Into<String>, predicate: Box<dyn Predicate>) -> Self {
        Self {
            key: key.into(),
            predicate,
        }
    }

    fn matches(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        let prop_value = match &traverser.value {
            Value::Vertex(id) => {
                ctx.snapshot
                    .get_vertex(*id)
                    .and_then(|v| v.property(&self.key).cloned())
            }
            Value::Edge(id) => {
                ctx.snapshot
                    .get_edge(*id)
                    .and_then(|e| e.property(&self.key).cloned())
            }
            _ => None,
        };

        match prop_value {
            Some(value) => self.predicate.test(&value),
            None => false,
        }
    }
}

impl AnyStep for HasWhereStepDyn {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let step = self.clone();
        Box::new(input.filter(move |t| step.matches(ctx, t)))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "hasWhere"
    }
}
```

#### Builder Methods

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Filter by property matching a predicate
    /// 
    /// # Example
    /// ```rust
    /// // Find adults (age >= 18)
    /// g.v().has_label("person")
    ///     .has_where("age", p::gte(18))
    ///     .to_list()
    /// 
    /// // Find people with names starting with "A" or "B"
    /// g.v().has_label("person")
    ///     .has_where("name", p::or(
    ///         p::starting_with("A"),
    ///         p::starting_with("B")
    ///     ))
    ///     .to_list()
    /// ```
    pub fn has_where<P: Predicate + Clone + 'static>(
        self,
        key: &str,
        predicate: P,
    ) -> BoundTraversal<'g, In, Out> {
        self.add_step(HasWhereStep::new(key, predicate))
    }
}

// Also on Traversal for anonymous chaining
impl<In, Out> Traversal<In, Out> {
    pub fn has_where<P: Predicate + Clone + 'static>(
        self,
        key: &str,
        predicate: P,
    ) -> Traversal<In, Out> {
        self.add_step(HasWhereStep::new(key, predicate))
    }
}
```

**Example Usage:**
```rust
use intersteller::prelude::*;

// Complex predicate composition
let adults_starting_with_a = g.v()
    .has_label("person")
    .has_where("age", p::gte(18))
    .has_where("name", p::starting_with("A"))
    .to_list();

// Range query
let middle_aged = g.v()
    .has_label("person")
    .has_where("age", p::between(30, 50))
    .to_list();

// Membership check
let specific_names = g.v()
    .has_label("person")
    .has_where("name", p::within(["Alice", "Bob", "Carol"]))
    .to_list();

// Regex matching
let emails = g.v()
    .has_label("person")
    .has_where("email", p::regex(r".*@company\.com$"))
    .to_list();

// Logical composition
let complex_filter = g.v()
    .has_label("person")
    .has_where("age", p::and(p::gte(18), p::lt(65)))
    .has_where("status", p::or(p::eq("active"), p::eq("pending")))
    .to_list();
```

---

### 4.6 Helper Functions: `execute_traversal` and `execute_traversal_from`

All branch and filter steps that use anonymous traversals rely on the helper functions defined in Phase 3 (`src/traversal/step.rs`). These functions execute a traversal's steps with a given context and input.

**Note**: These functions already exist in the codebase. The signatures below match the actual implementation.

```rust
/// Execute steps on provided input (low-level).
/// 
/// This is the core function used by branch/filter steps to evaluate
/// sub-traversals. It applies steps in sequence, building a lazy iterator chain.
/// 
/// # Arguments
/// * `ctx` - The execution context (provides graph access)
/// * `steps` - The steps to apply (extracted from a traversal via `.steps()`)
/// * `input` - Input traversers to feed into the traversal
/// 
/// # Returns
/// A boxed iterator over the output traversers
pub fn execute_traversal<'a>(
    ctx: &'a ExecutionContext<'a>,
    steps: &'a [Box<dyn AnyStep>],
    input: Box<dyn Iterator<Item = Traverser> + 'a>,
) -> Box<dyn Iterator<Item = Traverser> + 'a> {
    steps
        .iter()
        .fold(input, |current, step| step.apply(ctx, current))
}

/// Execute a traversal on provided input (convenience wrapper).
/// 
/// Accesses the traversal's steps via `.steps()` and calls `execute_traversal`.
/// The traversal's source (if any) is ignored.
/// 
/// # Arguments
/// * `ctx` - The execution context
/// * `traversal` - The traversal whose steps to execute (borrowed, not consumed)
/// * `input` - Iterator of input traversers
pub fn execute_traversal_from<'a, In, Out>(
    ctx: &'a ExecutionContext<'a>,
    traversal: &'a Traversal<In, Out>,
    input: Box<dyn Iterator<Item = Traverser> + 'a>,
) -> Box<dyn Iterator<Item = Traverser> + 'a> {
    execute_traversal(ctx, traversal.steps(), input)
}
```

**Key Points:**
- Anonymous traversals have no source (the source comes from the parent's current traverser)
- The `ExecutionContext` is shared between parent and sub-traversals
- Steps are applied lazily via the iterator chain (fold builds the iterator chain)
- Traversals are **borrowed** (not consumed), enabling reuse across multiple inputs
- The function returns a boxed iterator for uniform handling

**Usage in Steps:**
```rust
// In WhereStep - use execute_traversal_from for convenience
fn apply<'a>(&'a self, ctx: &'a ExecutionContext<'a>, input: ...) -> ... {
    let sub = self.sub.clone();
    Box::new(input.filter(move |t| {
        let sub_input = Box::new(std::iter::once(t.clone()));
        let mut results = execute_traversal_from(ctx, &sub, sub_input);
        results.next().is_some()
    }))
}

// In UnionStep
fn apply<'a>(&'a self, ctx: &'a ExecutionContext<'a>, input: ...) -> ... {
    let branches = self.branches.clone();
    Box::new(input.flat_map(move |t| {
        branches.iter().flat_map(|branch| {
            let sub_input = Box::new(std::iter::once(t.clone()));
            execute_traversal_from(ctx, branch, sub_input)
        }).collect::<Vec<_>>().into_iter()
    }))
}
```

---

## Test Cases

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // ─────────────────────────────────────────────────────────────
    // Predicate Tests
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn test_predicate_eq() {
        let pred = p::eq(42);
        assert!(pred.test(&Value::Int(42)));
        assert!(!pred.test(&Value::Int(41)));
        assert!(!pred.test(&Value::String("42".to_string())));
    }

    #[test]
    fn test_predicate_neq() {
        let pred = p::neq(42);
        assert!(!pred.test(&Value::Int(42)));
        assert!(pred.test(&Value::Int(41)));
    }

    #[test]
    fn test_predicate_comparison() {
        assert!(p::lt(50).test(&Value::Int(30)));
        assert!(!p::lt(50).test(&Value::Int(50)));
        assert!(p::lte(50).test(&Value::Int(50)));
        assert!(p::gt(50).test(&Value::Int(60)));
        assert!(p::gte(50).test(&Value::Int(50)));
    }

    #[test]
    fn test_predicate_between() {
        let pred = p::between(10, 20);
        assert!(pred.test(&Value::Int(10)));  // inclusive start
        assert!(pred.test(&Value::Int(15)));
        assert!(!pred.test(&Value::Int(20))); // exclusive end
        assert!(!pred.test(&Value::Int(5)));
    }

    #[test]
    fn test_predicate_within() {
        let pred = p::within([1, 2, 3]);
        assert!(pred.test(&Value::Int(1)));
        assert!(pred.test(&Value::Int(2)));
        assert!(!pred.test(&Value::Int(4)));
    }

    #[test]
    fn test_predicate_string() {
        assert!(p::containing("foo").test(&Value::String("foobar".to_string())));
        assert!(p::starting_with("foo").test(&Value::String("foobar".to_string())));
        assert!(p::ending_with("bar").test(&Value::String("foobar".to_string())));
        assert!(!p::containing("baz").test(&Value::String("foobar".to_string())));
    }

    #[test]
    fn test_predicate_regex() {
        let pred = p::regex(r"^\d{3}-\d{4}$");
        assert!(pred.test(&Value::String("123-4567".to_string())));
        assert!(!pred.test(&Value::String("12-4567".to_string())));
    }

    #[test]
    fn test_predicate_and() {
        let pred = p::and(p::gte(18), p::lt(65));
        assert!(pred.test(&Value::Int(30)));
        assert!(!pred.test(&Value::Int(10)));
        assert!(!pred.test(&Value::Int(70)));
    }

    #[test]
    fn test_predicate_or() {
        let pred = p::or(p::eq("active"), p::eq("pending"));
        assert!(pred.test(&Value::String("active".to_string())));
        assert!(pred.test(&Value::String("pending".to_string())));
        assert!(!pred.test(&Value::String("inactive".to_string())));
    }

    #[test]
    fn test_predicate_not() {
        let pred = p::not(p::eq(42));
        assert!(!pred.test(&Value::Int(42)));
        assert!(pred.test(&Value::Int(41)));
    }

    // ─────────────────────────────────────────────────────────────
    // Anonymous Traversal Factory Tests
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn test_anonymous_identity() {
        let anon = __::identity();
        // Should have 1 step (IdentityStep that passes through)
        assert_eq!(anon.step_count(), 1);
    }

    #[test]
    fn test_anonymous_out() {
        let anon = __::out();
        assert_eq!(anon.step_count(), 1);
    }

    #[test]
    fn test_anonymous_chaining() {
        let anon = __::out_labels(&["knows"])
            .has_label("person")
            .has_value("age", 30);
        assert_eq!(anon.step_count(), 3);
    }

    // ─────────────────────────────────────────────────────────────
    // Repeat Config Tests
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn test_repeat_config_times() {
        let config = RepeatConfig::default();
        assert!(config.times.is_none());
        
        let mut config = config;
        config.times = Some(5);
        assert_eq!(config.times, Some(5));
    }

    #[test]
    fn test_repeat_config_emit() {
        let config = RepeatConfig::default();
        assert!(!config.emit);
        assert!(!config.emit_first);
        assert!(config.emit_if.is_none());
    }
}
```

### Integration Tests

```rust
#[cfg(test)]
mod integration_tests {
    use super::*;

    fn create_test_graph() -> Graph {
        let graph = Graph::in_memory();
        
        {
            let mut g = graph.mutate();
            
            let alice = g.add_vertex("person", hashmap!{
                "name" => "Alice",
                "age" => 30
            });
            let bob = g.add_vertex("person", hashmap!{
                "name" => "Bob",
                "age" => 35
            });
            let carol = g.add_vertex("person", hashmap!{
                "name" => "Carol",
                "age" => 25
            });
            let dave = g.add_vertex("person", hashmap!{
                "name" => "Dave",
                "age" => 40
            });
            let acme = g.add_vertex("company", hashmap!{
                "name" => "Acme Corp"
            });
            
            // Alice -> Bob -> Dave
            // Alice -> Carol -> Dave
            g.add_edge(alice, bob, "knows", hashmap!{});
            g.add_edge(alice, carol, "knows", hashmap!{});
            g.add_edge(bob, dave, "knows", hashmap!{});
            g.add_edge(carol, dave, "knows", hashmap!{});
            g.add_edge(bob, acme, "works_at", hashmap!{});
            
            g.commit().unwrap();
        }
        
        graph
    }

    // ─────────────────────────────────────────────────────────────
    // Filter Step Tests
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn test_where_step() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        // Find people who know someone named "Dave"
        let results = g.v()
            .has_label("person")
            .where_(__.out_labels(&["knows"]).has_value("name", "Dave"))
            .values("name")
            .to_list();
        
        assert_eq!(results.len(), 2); // Bob and Carol know Dave
    }

    #[test]
    fn test_not_step() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        // Find people who DON'T know Dave
        let results = g.v()
            .has_label("person")
            .not(__.out_labels(&["knows"]).has_value("name", "Dave"))
            .values("name")
            .to_list();
        
        // Alice and Dave don't know Dave (Alice knows Bob/Carol, Dave doesn't have outgoing knows)
        assert!(results.len() >= 1);
    }

    #[test]
    fn test_and_step() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        // Find people who know someone AND work at Acme
        let results = g.v()
            .has_label("person")
            .and_(vec![
                __.out_labels(&["knows"]),
                __.out_labels(&["works_at"]).has_label("company")
            ])
            .values("name")
            .to_list();
        
        // Only Bob knows someone AND works at Acme
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_or_step() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        // Find people named Alice OR Bob
        let results = g.v()
            .has_label("person")
            .or_(vec![
                __.has_value("name", "Alice"),
                __.has_value("name", "Bob")
            ])
            .values("name")
            .to_list();
        
        assert_eq!(results.len(), 2);
    }

    // ─────────────────────────────────────────────────────────────
    // Branch Step Tests
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn test_union_step() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        // Get both friends and employer
        let results = g.v()
            .has_value("name", "Bob")
            .union(vec![
                __.out_labels(&["knows"]),
                __.out_labels(&["works_at"])
            ])
            .to_list();
        
        // Bob knows Dave, works at Acme
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_coalesce_step() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        // Try nickname first, fall back to name
        let results = g.v()
            .has_label("person")
            .coalesce(vec![
                __.values("nickname"),  // None have nickname
                __.values("name")
            ])
            .to_list();
        
        // Should get names since no nicknames exist
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_optional_step() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        // Try to get employer, keep person if no employer
        let results = g.v()
            .has_label("person")
            .optional(__.out_labels(&["works_at"]))
            .to_list();
        
        // 4 people: Bob gets Acme, others keep themselves
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_choose_step() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        // If age >= 35, return "senior", else "junior"
        let results = g.v()
            .has_label("person")
            .choose(
                __.has_where("age", p::gte(35)),
                __.constant("senior"),
                __.constant("junior")
            )
            .to_list();
        
        assert_eq!(results.len(), 4);
        // Bob (35) and Dave (40) are senior, Alice (30) and Carol (25) are junior
    }

    // ─────────────────────────────────────────────────────────────
    // Repeat Step Tests
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn test_repeat_times() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        // Friends of friends (2 hops from Alice)
        let results = g.v()
            .has_value("name", "Alice")
            .repeat(__.out_labels(&["knows"]))
            .times(2)
            .dedup()
            .values("name")
            .to_list();
        
        // Alice -> Bob,Carol -> Dave (deduplicated)
        assert_eq!(results.len(), 1); // Just Dave
    }

    #[test]
    fn test_repeat_until() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        // Traverse until finding a company
        let results = g.v()
            .has_value("name", "Alice")
            .repeat(__.out())
            .until(__.has_label("company"))
            .to_list();
        
        // Should find Acme via Alice -> Bob -> Acme
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_repeat_emit() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        // Get all reachable vertices up to 2 hops
        let results = g.v()
            .has_value("name", "Alice")
            .repeat(__.out_labels(&["knows"]))
            .times(2)
            .emit()
            .dedup()
            .to_list();
        
        // Should include Bob, Carol (hop 1) and Dave (hop 2)
        assert!(results.len() >= 3);
    }

    // ─────────────────────────────────────────────────────────────
    // Has Where Step Tests
    // ─────────────────────────────────────────────────────────────

    #[test]
    fn test_has_where_gte() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        let results = g.v()
            .has_label("person")
            .has_where("age", p::gte(35))
            .values("name")
            .to_list();
        
        assert_eq!(results.len(), 2); // Bob (35) and Dave (40)
    }

    #[test]
    fn test_has_where_between() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        let results = g.v()
            .has_label("person")
            .has_where("age", p::between(25, 35))
            .values("name")
            .to_list();
        
        assert_eq!(results.len(), 2); // Carol (25) and Alice (30)
    }

    #[test]
    fn test_has_where_within() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        let results = g.v()
            .has_label("person")
            .has_where("name", p::within(["Alice", "Bob"]))
            .to_list();
        
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_has_where_starting_with() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        let results = g.v()
            .has_label("person")
            .has_where("name", p::starting_with("A"))
            .values("name")
            .to_list();
        
        assert_eq!(results.len(), 1); // Alice
    }

    #[test]
    fn test_has_where_composed() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        // Age >= 25 AND < 35
        let results = g.v()
            .has_label("person")
            .has_where("age", p::and(p::gte(25), p::lt(35)))
            .values("name")
            .to_list();
        
        assert_eq!(results.len(), 2); // Carol (25) and Alice (30)
    }
}
```

### Benchmarks

```rust
// benches/anonymous_traversal.rs

use criterion::{criterion_group, criterion_main, Criterion};

fn create_benchmark_graph(n: u64) -> Graph {
    let graph = Graph::in_memory();
    {
        let mut g = graph.mutate();
        
        let mut ids = Vec::new();
        for i in 0..n {
            let id = g.add_vertex("person", hashmap!{
                "name" => format!("Person_{}", i),
                "age" => (i % 80) as i64
            });
            ids.push(id);
        }
        
        // Create random edges
        use rand::Rng;
        let mut rng = rand::thread_rng();
        for _ in 0..(n * 5) {
            let src = ids[rng.gen_range(0..ids.len())];
            let dst = ids[rng.gen_range(0..ids.len())];
            if src != dst {
                let _ = g.add_edge(src, dst, "knows", hashmap!{});
            }
        }
        
        g.commit().unwrap();
    }
    graph
}

fn bench_where_step(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000);
    
    c.bench_function("where_(__.out().has_label())", |b| {
        b.iter(|| {
            let snap = graph.snapshot();
            let g = snap.traversal();
            g.v()
                .has_label("person")
                .where_(__.out_labels(&["knows"]).has_label("person"))
                .count()
        })
    });
}

fn bench_union_step(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000);
    
    c.bench_function("union(__.out(), __.in_())", |b| {
        b.iter(|| {
            let snap = graph.snapshot();
            let g = snap.traversal();
            g.v()
                .has_label("person")
                .limit(100)
                .union(vec![
                    __.out_labels(&["knows"]),
                    __.in_labels(&["knows"])
                ])
                .count()
        })
    });
}

fn bench_repeat_times(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000);
    
    c.bench_function("repeat(__.out()).times(3)", |b| {
        b.iter(|| {
            let snap = graph.snapshot();
            let g = snap.traversal();
            g.v()
                .has_label("person")
                .limit(10)
                .repeat(__.out_labels(&["knows"]))
                .times(3)
                .dedup()
                .count()
        })
    });
}

fn bench_has_where(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000);
    
    c.bench_function("has_where(age, p::between())", |b| {
        b.iter(|| {
            let snap = graph.snapshot();
            let g = snap.traversal();
            g.v()
                .has_label("person")
                .has_where("age", p::between(18, 65))
                .count()
        })
    });
}

criterion_group!(
    benches,
    bench_where_step,
    bench_union_step,
    bench_repeat_times,
    bench_has_where,
);
criterion_main!(benches);
```

---

## Exit Criteria

- [ ] `__` module compiles with all factory methods
- [ ] Anonymous traversals chain: `__.out().has_label("person").values("name")`
- [ ] `Traversal<In, Out>` is cloneable for branching operations
- [ ] `execute_traversal()` helper works for sub-traversal execution
- [ ] **Filter steps work:**
  - [ ] `where_(sub)` - filter by sub-traversal producing results
  - [ ] `not(sub)` - filter by sub-traversal NOT producing results
  - [ ] `and_(subs)` - all sub-traversals must produce results
  - [ ] `or_(subs)` - at least one sub-traversal must produce results
- [ ] **Branch steps work:**
  - [ ] `union(branches)` - merge results from multiple branches
  - [ ] `coalesce(branches)` - first branch with results wins
  - [ ] `choose(cond, if_true, if_false)` - conditional branching
  - [ ] `optional(sub)` - try sub-traversal, keep original if empty
  - [ ] `local(sub)` - execute in isolated scope
- [ ] **Repeat step works:**
  - [ ] `repeat(sub).times(n)` - fixed iterations
  - [ ] `repeat(sub).until(cond)` - conditional termination
  - [ ] `repeat(sub).emit()` - emit intermediate results
  - [ ] `repeat(sub).emit_if(cond)` - conditional emission
  - [ ] BFS frontier processing for level-order traversal
- [ ] **Predicate system works:**
  - [ ] Comparison: `eq`, `neq`, `lt`, `lte`, `gt`, `gte`
  - [ ] Range: `between`, `inside`, `outside`
  - [ ] Collection: `within`, `without`
  - [ ] String: `containing`, `starting_with`, `ending_with`, `regex`
  - [ ] Logical: `and`, `or`, `not`
- [ ] `has_where(key, predicate)` filters by property with predicate
- [ ] All predicates are `Clone + Send + Sync`
- [ ] All unit tests pass
- [ ] All integration tests pass
- [ ] Benchmarks run successfully on 10K vertex graph

---

## Implementation Order

1. **Week 1**: Core infrastructure
   - `execute_traversal()` helper function
   - `Predicate` trait and `p::` module (comparison predicates)
   - `HasWhereStep` implementation
   - Unit tests for predicates

2. **Week 2**: Filter steps
   - `WhereStep`, `NotStep`, `AndStep`, `OrStep`
   - `__` factory module extensions
   - Filter step integration tests

3. **Week 3**: Branch steps and repeat
   - `UnionStep`, `CoalesceStep`, `ChooseStep`
   - `OptionalStep`, `LocalStep`
   - `RepeatStep` with `RepeatConfig` and `RepeatTraversal` builder
   - Branch step integration tests
   - Repeat step integration tests

4. **Week 3+**: Polish and optimization
   - Complete predicate system (string, regex, range)
   - Logical predicate composition
   - Performance benchmarks
   - Documentation
   - Edge case testing

---

## Notes

### Cloneability Requirement

All steps must be `Clone` because branching operations need to clone sub-traversals:

```rust
// UnionStep clones each branch for each input traverser
for branch in self.branches.iter() {
    let sub_input = std::iter::once(t.clone());
    execute_traversal(ctx, branch.clone(), sub_input)  // Clone!
}
```

### Iterator Lifetime Management

The type-erased architecture requires careful lifetime management:

```rust
// BAD: Lifetime of 'step' doesn't extend past apply
fn apply<'a>(&'a self, ctx: &'a ExecutionContext<'a>, input: ...) -> ... {
    let step = self;  // Borrows self
    Box::new(input.filter(move |t| step.matches(ctx, t)))  // Error!
}

// GOOD: Clone the step data needed
fn apply<'a>(&'a self, ctx: &'a ExecutionContext<'a>, input: ...) -> ... {
    let step = self.clone();  // Owned copy
    Box::new(input.filter(move |t| step.matches(ctx, t)))  // OK!
}
```

### Predicate Type Erasure

For maximum flexibility, predicates can be boxed:

```rust
// Static predicate (monomorphized)
pub fn has_where<P: Predicate + 'static>(self, key: &str, predicate: P) -> Self

// Dynamic predicate (when type isn't known at compile time)
pub fn has_where_dyn(self, key: &str, predicate: Box<dyn Predicate>) -> Self
```

### Dependency Note

The regex predicate requires adding `regex` to dependencies:

```toml
[dependencies]
regex = "1.10"
```

