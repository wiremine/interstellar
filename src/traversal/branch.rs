//! Branch and filter steps using anonymous traversals.
//!
//! This module provides steps that use anonymous traversals for:
//! - **Filter steps**: `WhereStep`, `NotStep`, `AndStep`, `OrStep`
//! - **Branch steps**: `UnionStep`, `CoalesceStep`, `ChooseStep`, `OptionalStep`, `LocalStep`
//!
//! These steps execute sub-traversals to determine filtering or branching behavior.
//! The sub-traversals receive the current traverser as input and their results
//! determine whether to emit, filter, or transform the traverser.
//!
//! # Example
//!
//! ```ignore
//! use rustgremlin::traversal::__;
//!
//! // Filter to vertices that have outgoing "knows" edges
//! g.v().where_(__.out_labels(&["knows"])).to_list()
//!
//! // Execute multiple branches and merge results
//! g.v().union(vec![__.out(), __.in_()]).to_list()
//! ```

use crate::traversal::context::ExecutionContext;
use crate::traversal::step::{execute_traversal_from, AnyStep};
use crate::traversal::{Traversal, Traverser};
use crate::value::Value;

// =============================================================================
// Filter Steps
// =============================================================================

/// Filter by sub-traversal existence.
///
/// Emits input traverser only if the sub-traversal produces at least one result.
/// This is the primary mechanism for filtering based on graph structure.
///
/// # Example
///
/// ```ignore
/// // Keep only vertices that have outgoing "knows" edges to someone named "Bob"
/// g.v().where_(__.out_labels(&["knows"]).has_value("name", "Bob")).to_list()
/// ```
#[derive(Clone)]
pub struct WhereStep {
    sub: Traversal<Value, Value>,
}

impl WhereStep {
    /// Create a new WhereStep with the given sub-traversal.
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

/// Filter by sub-traversal non-existence.
///
/// Emits input traverser only if the sub-traversal produces NO results.
/// This is the inverse of `WhereStep`.
///
/// # Example
///
/// ```ignore
/// // Keep only leaf vertices (no outgoing edges)
/// g.v().not(__.out()).to_list()
/// ```
#[derive(Clone)]
pub struct NotStep {
    sub: Traversal<Value, Value>,
}

impl NotStep {
    /// Create a new NotStep with the given sub-traversal.
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

/// Filter by multiple sub-traversals (AND logic).
///
/// Emits input traverser only if ALL sub-traversals produce at least one result.
/// Short-circuits on first failing condition.
///
/// # Example
///
/// ```ignore
/// // Keep vertices that have both outgoing AND incoming edges
/// g.v().and_(vec![__.out(), __.in_()]).to_list()
/// ```
#[derive(Clone)]
pub struct AndStep {
    subs: Vec<Traversal<Value, Value>>,
}

impl AndStep {
    /// Create a new AndStep with the given sub-traversals.
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

/// Filter by multiple sub-traversals (OR logic).
///
/// Emits input traverser if ANY sub-traversal produces at least one result.
/// Short-circuits on first successful condition.
///
/// # Example
///
/// ```ignore
/// // Keep vertices that are either "person" OR "software"
/// g.v().or_(vec![__.has_label("person"), __.has_label("software")]).to_list()
/// ```
#[derive(Clone)]
pub struct OrStep {
    subs: Vec<Traversal<Value, Value>>,
}

impl OrStep {
    /// Create a new OrStep with the given sub-traversals.
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

// =============================================================================
// Branch Steps
// =============================================================================

/// Execute multiple branches and merge results.
///
/// All branches receive each input traverser; results are interleaved
/// in traverser-major order (all results from one input before next).
///
/// # Example
///
/// ```ignore
/// // Get neighbors in both directions
/// g.v().union(vec![__.out(), __.in_()]).to_list()
/// ```
#[derive(Clone)]
pub struct UnionStep {
    branches: Vec<Traversal<Value, Value>>,
}

impl UnionStep {
    /// Create a new UnionStep with the given branches.
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
            // For each input traverser, execute all branches and collect results
            let mut results = Vec::new();
            for branch in branches.iter() {
                let sub_input = Box::new(std::iter::once(t.clone()));
                results.extend(execute_traversal_from(ctx, branch, sub_input));
            }
            results.into_iter()
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "union"
    }
}

/// Try branches in order, return first non-empty result.
///
/// Short-circuits: once a branch produces results, remaining branches
/// are not evaluated for that input traverser.
///
/// # Example
///
/// ```ignore
/// // Try to get nickname, fall back to name
/// g.v().coalesce(vec![__.values("nickname"), __.values("name")]).to_list()
/// ```
#[derive(Clone)]
pub struct CoalesceStep {
    branches: Vec<Traversal<Value, Value>>,
}

impl CoalesceStep {
    /// Create a new CoalesceStep with the given branches.
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

/// Conditional branching.
///
/// Evaluates condition traversal; if it produces results, executes
/// if_true branch, otherwise executes if_false branch.
///
/// # Example
///
/// ```ignore
/// // If person, get friends; otherwise get all neighbors
/// g.v().choose(__.has_label("person"), __.out_labels(&["knows"]), __.out()).to_list()
/// ```
#[derive(Clone)]
pub struct ChooseStep {
    condition: Traversal<Value, Value>,
    if_true: Traversal<Value, Value>,
    if_false: Traversal<Value, Value>,
}

impl ChooseStep {
    /// Create a new ChooseStep with condition and branches.
    pub fn new(
        condition: Traversal<Value, Value>,
        if_true: Traversal<Value, Value>,
        if_false: Traversal<Value, Value>,
    ) -> Self {
        Self {
            condition,
            if_true,
            if_false,
        }
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
            execute_traversal_from(ctx, branch, sub_input)
                .collect::<Vec<_>>()
                .into_iter()
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "choose"
    }
}

/// Optional traversal with fallback to input.
///
/// If sub-traversal produces results, emit those results.
/// If sub-traversal produces no results, emit the original input.
///
/// # Example
///
/// ```ignore
/// // Try to traverse to friends, keep original if none found
/// g.v().optional(__.out_labels(&["knows"])).to_list()
/// ```
#[derive(Clone)]
pub struct OptionalStep {
    sub: Traversal<Value, Value>,
}

impl OptionalStep {
    /// Create a new OptionalStep with the given sub-traversal.
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

/// Execute sub-traversal in isolated scope.
///
/// Aggregations (count, fold, etc.) in the sub-traversal operate
/// independently for each input traverser, not across all inputs.
///
/// # Example
///
/// ```ignore
/// // Count neighbors per vertex (not total neighbors)
/// g.v().local(__.out().count()).to_list()
/// ```
#[derive(Clone)]
pub struct LocalStep {
    sub: Traversal<Value, Value>,
}

impl LocalStep {
    /// Create a new LocalStep with the given sub-traversal.
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
            execute_traversal_from(ctx, &sub, sub_input)
                .collect::<Vec<_>>()
                .into_iter()
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "local"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Basic compilation tests - full integration tests will be in tests/traversal.rs

    #[test]
    fn where_step_compiles() {
        let sub = Traversal::<Value, Value>::new();
        let step = WhereStep::new(sub);
        assert_eq!(step.name(), "where");
    }

    #[test]
    fn not_step_compiles() {
        let sub = Traversal::<Value, Value>::new();
        let step = NotStep::new(sub);
        assert_eq!(step.name(), "not");
    }

    #[test]
    fn and_step_compiles() {
        let subs = vec![
            Traversal::<Value, Value>::new(),
            Traversal::<Value, Value>::new(),
        ];
        let step = AndStep::new(subs);
        assert_eq!(step.name(), "and");
    }

    #[test]
    fn or_step_compiles() {
        let subs = vec![
            Traversal::<Value, Value>::new(),
            Traversal::<Value, Value>::new(),
        ];
        let step = OrStep::new(subs);
        assert_eq!(step.name(), "or");
    }

    #[test]
    fn union_step_compiles() {
        let branches = vec![
            Traversal::<Value, Value>::new(),
            Traversal::<Value, Value>::new(),
        ];
        let step = UnionStep::new(branches);
        assert_eq!(step.name(), "union");
    }

    #[test]
    fn coalesce_step_compiles() {
        let branches = vec![
            Traversal::<Value, Value>::new(),
            Traversal::<Value, Value>::new(),
        ];
        let step = CoalesceStep::new(branches);
        assert_eq!(step.name(), "coalesce");
    }

    #[test]
    fn choose_step_compiles() {
        let condition = Traversal::<Value, Value>::new();
        let if_true = Traversal::<Value, Value>::new();
        let if_false = Traversal::<Value, Value>::new();
        let step = ChooseStep::new(condition, if_true, if_false);
        assert_eq!(step.name(), "choose");
    }

    #[test]
    fn optional_step_compiles() {
        let sub = Traversal::<Value, Value>::new();
        let step = OptionalStep::new(sub);
        assert_eq!(step.name(), "optional");
    }

    #[test]
    fn local_step_compiles() {
        let sub = Traversal::<Value, Value>::new();
        let step = LocalStep::new(sub);
        assert_eq!(step.name(), "local");
    }

    #[test]
    fn steps_are_clonable() {
        let sub = Traversal::<Value, Value>::new();

        let where_step = WhereStep::new(sub.clone());
        let _ = where_step.clone();

        let not_step = NotStep::new(sub.clone());
        let _ = not_step.clone();

        let and_step = AndStep::new(vec![sub.clone()]);
        let _ = and_step.clone();

        let or_step = OrStep::new(vec![sub.clone()]);
        let _ = or_step.clone();

        let union_step = UnionStep::new(vec![sub.clone()]);
        let _ = union_step.clone();

        let coalesce_step = CoalesceStep::new(vec![sub.clone()]);
        let _ = coalesce_step.clone();

        let choose_step = ChooseStep::new(sub.clone(), sub.clone(), sub.clone());
        let _ = choose_step.clone();

        let optional_step = OptionalStep::new(sub.clone());
        let _ = optional_step.clone();

        let local_step = LocalStep::new(sub);
        let _ = local_step.clone();
    }

    #[test]
    fn steps_implement_any_step() {
        let sub = Traversal::<Value, Value>::new();

        let _: Box<dyn AnyStep> = Box::new(WhereStep::new(sub.clone()));
        let _: Box<dyn AnyStep> = Box::new(NotStep::new(sub.clone()));
        let _: Box<dyn AnyStep> = Box::new(AndStep::new(vec![sub.clone()]));
        let _: Box<dyn AnyStep> = Box::new(OrStep::new(vec![sub.clone()]));
        let _: Box<dyn AnyStep> = Box::new(UnionStep::new(vec![sub.clone()]));
        let _: Box<dyn AnyStep> = Box::new(CoalesceStep::new(vec![sub.clone()]));
        let _: Box<dyn AnyStep> = Box::new(ChooseStep::new(sub.clone(), sub.clone(), sub.clone()));
        let _: Box<dyn AnyStep> = Box::new(OptionalStep::new(sub.clone()));
        let _: Box<dyn AnyStep> = Box::new(LocalStep::new(sub));
    }
}
