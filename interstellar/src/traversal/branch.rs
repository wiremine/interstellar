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
//! use interstellar::traversal::__;
//!
//! // Filter to vertices that have outgoing "knows" edges
//! g.v().where_(__.out_labels(&["knows"])).to_list()
//!
//! // Execute multiple branches and merge results
//! g.v().union(vec![__.out(), __.in_()]).to_list()
//! ```

use std::collections::HashMap;

use crate::traversal::context::ExecutionContext;
use crate::traversal::step::{execute_traversal_from, Step};
use crate::traversal::{Traversal, Traverser};
use crate::value::Value;

// =============================================================================
// Option Key Types for Multi-way Branching
// =============================================================================

/// Key for matching option branches in `BranchStep`.
///
/// `OptionKey` is used to route traversers to specific branches based on
/// computed values. It supports two variants:
///
/// - `Value(Value)` - Match a specific value
/// - `None` - Default fallback when no other option matches
///
/// # Example
///
/// ```rust
/// use interstellar::traversal::branch::OptionKey;
///
/// // Create keys from various types
/// let key1 = OptionKey::from("person");
/// let key2 = OptionKey::from(42i64);
/// let key3 = OptionKey::none();
/// ```
#[derive(Clone, Debug, PartialEq)]
pub enum OptionKey {
    /// Match a specific value.
    Value(Value),
    /// Default fallback (Pick.none in Gremlin).
    None,
}

impl OptionKey {
    /// Create an `OptionKey` from any type that can be converted to `Value`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::traversal::branch::OptionKey;
    ///
    /// let key = OptionKey::value("person");
    /// ```
    pub fn value<T: Into<Value>>(v: T) -> Self {
        OptionKey::Value(v.into())
    }

    /// Create a `None` option key (default fallback).
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::traversal::branch::OptionKey;
    ///
    /// let key = OptionKey::none();
    /// ```
    pub fn none() -> Self {
        OptionKey::None
    }
}

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

impl From<Value> for OptionKey {
    fn from(v: Value) -> Self {
        OptionKey::Value(v)
    }
}

/// Wrapper around `OptionKey` that implements `Hash` and `Eq` for use in `HashMap`.
///
/// This wrapper is necessary because `OptionKey` contains `Value`, and we need
/// consistent hashing behavior for use as HashMap keys.
#[derive(Clone, Debug)]
pub struct OptionKeyWrapper(pub OptionKey);

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

impl Step for WhereStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let sub = self.sub.clone();
        input.filter(move |t| {
            // Execute sub-traversal with current traverser as input
            let sub_input = Box::new(std::iter::once(t.clone()));
            let mut results = execute_traversal_from(ctx, &sub, sub_input);
            results.next().is_some() // Pass if any results
        })
    }

    fn name(&self) -> &'static str {
        "where"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Filter
    }

    fn describe(&self) -> Option<String> {
        use crate::traversal::explain::format_traversal_steps;
        Some(format_traversal_steps(self.sub.steps()))
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        use crate::traversal::step::execute_traversal_streaming;

        // Execute sub-traversal and check if it produces any result
        let mut results = execute_traversal_streaming(&ctx, &self.sub, input.clone());
        if results.next().is_some() {
            Box::new(std::iter::once(input))
        } else {
            Box::new(std::iter::empty())
        }
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

impl Step for NotStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let sub = self.sub.clone();
        input.filter(move |t| {
            let sub_input = Box::new(std::iter::once(t.clone()));
            let mut results = execute_traversal_from(ctx, &sub, sub_input);
            results.next().is_none() // Pass if NO results
        })
    }

    fn name(&self) -> &'static str {
        "not"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Filter
    }

    fn describe(&self) -> Option<String> {
        use crate::traversal::explain::format_traversal_steps;
        Some(format_traversal_steps(self.sub.steps()))
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        use crate::traversal::step::execute_traversal_streaming;

        // Execute sub-traversal and check if it produces NO results
        let mut results = execute_traversal_streaming(&ctx, &self.sub, input.clone());
        if results.next().is_none() {
            Box::new(std::iter::once(input))
        } else {
            Box::new(std::iter::empty())
        }
    }
}

/// Filter by comparing current value to a labeled path value (not equal).
///
/// Emits input traverser only if the current value is NOT equal to the value
/// stored at the specified path label.
///
/// # Example
///
/// ```ignore
/// // Find customers who are not Alice
/// g.v().as_("alice").out().in_().where_neq("alice").to_list()
/// ```
#[derive(Clone)]
pub struct WhereNeqStep {
    label: String,
}

impl WhereNeqStep {
    /// Create a new WhereNeqStep that compares to the given label.
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
        }
    }
}

impl Step for WhereNeqStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let label = self.label.clone();
        input.filter(move |t| {
            // Get the labeled value from the path
            if let Some(labeled_values) = t.path.get(&label) {
                // Compare current value to the first labeled value
                if let Some(labeled_value) = labeled_values.first() {
                    return t.value != labeled_value.to_value();
                }
            }
            // If label not found, pass through (conservative behavior)
            true
        })
    }

    fn name(&self) -> &'static str {
        "where_neq"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Filter
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // Get the labeled value from the path
        if let Some(labeled_values) = input.path.get(&self.label) {
            // Compare current value to the first labeled value
            if let Some(labeled_value) = labeled_values.first() {
                if input.value != labeled_value.to_value() {
                    return Box::new(std::iter::once(input));
                } else {
                    return Box::new(std::iter::empty());
                }
            }
        }
        // If label not found, pass through (conservative behavior)
        Box::new(std::iter::once(input))
    }
}

/// Filter by comparing current value to a labeled path value (equal).
///
/// Emits input traverser only if the current value IS equal to the value
/// stored at the specified path label.
///
/// # Example
///
/// ```ignore
/// // Find vertices that match a previously labeled vertex
/// g.v().as_("start").out().in_().where_eq("start").to_list()
/// ```
#[derive(Clone)]
pub struct WhereEqStep {
    label: String,
}

impl WhereEqStep {
    /// Create a new WhereEqStep that compares to the given label.
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
        }
    }
}

impl Step for WhereEqStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let label = self.label.clone();
        input.filter(move |t| {
            // Get the labeled value from the path
            if let Some(labeled_values) = t.path.get(&label) {
                // Compare current value to the first labeled value
                if let Some(labeled_value) = labeled_values.first() {
                    return t.value == labeled_value.to_value();
                }
            }
            // If label not found, filter out (conservative behavior for equality)
            false
        })
    }

    fn name(&self) -> &'static str {
        "where_eq"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Filter
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // Get the labeled value from the path
        if let Some(labeled_values) = input.path.get(&self.label) {
            // Compare current value to the first labeled value
            if let Some(labeled_value) = labeled_values.first() {
                if input.value == labeled_value.to_value() {
                    return Box::new(std::iter::once(input));
                }
            }
        }
        // If label not found or not equal, filter out
        Box::new(std::iter::empty())
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

impl Step for AndStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let subs = self.subs.clone();
        input.filter(move |t| {
            // All sub-traversals must produce at least one result
            subs.iter().all(|sub| {
                let sub_input = Box::new(std::iter::once(t.clone()));
                let mut results = execute_traversal_from(ctx, sub, sub_input);
                results.next().is_some()
            })
        })
    }

    fn name(&self) -> &'static str {
        "and"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Filter
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        use crate::traversal::step::execute_traversal_streaming;

        // All sub-traversals must produce at least one result
        for sub in &self.subs {
            let mut results = execute_traversal_streaming(&ctx, sub, input.clone());
            if results.next().is_none() {
                return Box::new(std::iter::empty());
            }
        }
        Box::new(std::iter::once(input))
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

impl Step for OrStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let subs = self.subs.clone();
        input.filter(move |t| {
            // At least one sub-traversal must produce a result
            subs.iter().any(|sub| {
                let sub_input = Box::new(std::iter::once(t.clone()));
                let mut results = execute_traversal_from(ctx, sub, sub_input);
                results.next().is_some()
            })
        })
    }

    fn name(&self) -> &'static str {
        "or"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Filter
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        use crate::traversal::step::execute_traversal_streaming;

        // At least one sub-traversal must produce a result
        for sub in &self.subs {
            let mut results = execute_traversal_streaming(&ctx, sub, input.clone());
            if results.next().is_some() {
                return Box::new(std::iter::once(input));
            }
        }
        Box::new(std::iter::empty())
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

impl Step for UnionStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let branches = self.branches.clone();

        input.flat_map(move |t| {
            // For each input traverser, execute all branches and collect results
            let mut results = Vec::new();
            for branch in branches.iter() {
                let sub_input = Box::new(std::iter::once(t.clone()));
                results.extend(execute_traversal_from(ctx, branch, sub_input));
            }
            results.into_iter()
        })
    }

    fn name(&self) -> &'static str {
        "union"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Branch
    }

    fn describe(&self) -> Option<String> {
        use crate::traversal::explain::format_traversal_steps;
        let branches: Vec<String> = self
            .branches
            .iter()
            .map(|b| format_traversal_steps(b.steps()))
            .collect();
        Some(format!("[{}]", branches.join(" | ")))
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        use crate::traversal::step::execute_traversal_streaming;

        // Execute all branches and collect results
        let mut results = Vec::new();
        for branch in &self.branches {
            results.extend(execute_traversal_streaming(&ctx, branch, input.clone()));
        }
        Box::new(results.into_iter())
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

impl Step for CoalesceStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let branches = self.branches.clone();

        input.flat_map(move |t| {
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
        })
    }

    fn name(&self) -> &'static str {
        "coalesce"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Branch
    }

    fn describe(&self) -> Option<String> {
        use crate::traversal::explain::format_traversal_steps;
        let branches: Vec<String> = self
            .branches
            .iter()
            .map(|b| format_traversal_steps(b.steps()))
            .collect();
        Some(format!("[{}]", branches.join(" | ")))
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        use crate::traversal::step::execute_traversal_streaming;

        // Try each branch in order, return first non-empty
        for branch in &self.branches {
            let results: Vec<_> =
                execute_traversal_streaming(&ctx, branch, input.clone()).collect();
            if !results.is_empty() {
                return Box::new(results.into_iter());
            }
        }
        Box::new(std::iter::empty())
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

impl Step for ChooseStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let condition = self.condition.clone();
        let if_true = self.if_true.clone();
        let if_false = self.if_false.clone();

        input.flat_map(move |t| {
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
        })
    }

    fn name(&self) -> &'static str {
        "choose"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Branch
    }

    fn describe(&self) -> Option<String> {
        use crate::traversal::explain::format_traversal_steps;
        Some(format!(
            "if: {}, then: {}, else: {}",
            format_traversal_steps(self.condition.steps()),
            format_traversal_steps(self.if_true.steps()),
            format_traversal_steps(self.if_false.steps()),
        ))
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        use crate::traversal::step::execute_traversal_streaming;

        // Evaluate condition traversal to determine which branch to take
        let mut cond_result = execute_traversal_streaming(&ctx, &self.condition, input.clone());

        // Select branch based on whether condition produced any results
        let branch = if cond_result.next().is_some() {
            &self.if_true
        } else {
            &self.if_false
        };

        // Execute selected branch with original input
        let results: Vec<_> = execute_traversal_streaming(&ctx, branch, input).collect();
        Box::new(results.into_iter())
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

impl Step for OptionalStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let sub = self.sub.clone();

        input.flat_map(move |t| {
            let sub_input = Box::new(std::iter::once(t.clone()));
            let results: Vec<_> = execute_traversal_from(ctx, &sub, sub_input).collect();

            if results.is_empty() {
                // No results, emit original
                vec![t].into_iter()
            } else {
                results.into_iter()
            }
        })
    }

    fn name(&self) -> &'static str {
        "optional"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Branch
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        use crate::traversal::step::execute_traversal_streaming;

        // Execute sub-traversal
        let results: Vec<_> = execute_traversal_streaming(&ctx, &self.sub, input.clone()).collect();

        if results.is_empty() {
            // No results from sub-traversal, emit original input as fallback
            Box::new(std::iter::once(input))
        } else {
            // Sub-traversal produced results, emit those
            Box::new(results.into_iter())
        }
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

impl Step for LocalStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let sub = self.sub.clone();

        input.flat_map(move |t| {
            // Execute sub-traversal for this traverser in isolation
            let sub_input = Box::new(std::iter::once(t));
            execute_traversal_from(ctx, &sub, sub_input)
                .collect::<Vec<_>>()
                .into_iter()
        })
    }

    fn name(&self) -> &'static str {
        "local"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Branch
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        use crate::traversal::step::execute_traversal_streaming;

        // Execute sub-traversal in local (isolated) scope for this input
        // Aggregations in the sub-traversal operate on just this input
        let results: Vec<_> = execute_traversal_streaming(&ctx, &self.sub, input).collect();
        Box::new(results.into_iter())
    }
}

// =============================================================================
// Multi-way Branch Step
// =============================================================================

/// Multi-way branching step that routes traversers based on computed values.
///
/// `BranchStep` evaluates a branch traversal for each input traverser to produce
/// a key value. The key is then matched against registered options to determine
/// which branch to execute. If no option matches and a `none_branch` is set,
/// that branch is executed. If no option matches and no `none_branch` exists,
/// the traverser is filtered out.
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::__;
///
/// // Route based on vertex label
/// g.v()
///     .branch(__.label())
///     .option("person", __.out_labels(&["knows"]))
///     .option("software", __.in_labels(&["created"]))
///     .option_none(__.identity())
///     .to_list()
/// ```
#[derive(Clone)]
pub struct BranchStep {
    /// The traversal that computes the branch key for each input traverser.
    branch_traversal: Traversal<Value, Value>,
    /// Map of option keys to their corresponding branch traversals.
    pub(crate) options: HashMap<OptionKeyWrapper, Traversal<Value, Value>>,
    /// Optional default branch when no option key matches.
    pub(crate) none_branch: Option<Traversal<Value, Value>>,
}

impl BranchStep {
    /// Create a new `BranchStep` with the given branch traversal.
    ///
    /// The branch traversal is evaluated for each input traverser to produce
    /// a key value that is used to select the appropriate option branch.
    ///
    /// # Arguments
    ///
    /// * `branch_traversal` - The traversal that computes the branch key
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::traversal::{Traversal, branch::BranchStep};
    /// use interstellar::value::Value;
    ///
    /// let label_traversal = Traversal::<Value, Value>::new();
    /// let step = BranchStep::new(label_traversal);
    /// ```
    pub fn new(branch_traversal: Traversal<Value, Value>) -> Self {
        Self {
            branch_traversal,
            options: HashMap::new(),
            none_branch: None,
        }
    }

    /// Add an option branch for a specific key.
    ///
    /// When the branch traversal produces a value matching `key`, the `branch`
    /// traversal will be executed for that traverser.
    ///
    /// If `key` is `OptionKey::None`, this sets the default branch instead.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to match against (can be converted from string, int, bool, etc.)
    /// * `branch` - The traversal to execute when the key matches
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::traversal::{Traversal, branch::BranchStep};
    /// use interstellar::value::Value;
    ///
    /// let label_traversal = Traversal::<Value, Value>::new();
    /// let step = BranchStep::new(label_traversal)
    ///     .add_option("person", Traversal::<Value, Value>::new())
    ///     .add_option("software", Traversal::<Value, Value>::new());
    /// ```
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

    /// Add a default branch for when no option key matches.
    ///
    /// This branch is executed when the branch traversal produces a value
    /// that doesn't match any registered option, or when it produces no value.
    ///
    /// # Arguments
    ///
    /// * `branch` - The traversal to execute as the default
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::traversal::{Traversal, branch::BranchStep};
    /// use interstellar::value::Value;
    ///
    /// let label_traversal = Traversal::<Value, Value>::new();
    /// let step = BranchStep::new(label_traversal)
    ///     .add_option("person", Traversal::<Value, Value>::new())
    ///     .add_none_option(Traversal::<Value, Value>::new());
    /// ```
    pub fn add_none_option(mut self, branch: Traversal<Value, Value>) -> Self {
        self.none_branch = Some(branch);
        self
    }
}

impl Step for BranchStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let branch_traversal = self.branch_traversal.clone();
        let options = self.options.clone();
        let none_branch = self.none_branch.clone();

        input.flat_map(move |t| {
            // Evaluate branch traversal to get the key
            let branch_input = Box::new(std::iter::once(t.clone()));
            let mut branch_results = execute_traversal_from(ctx, &branch_traversal, branch_input);

            // Get the first result as the branch key
            let key_value = branch_results
                .next()
                .map(|key_traverser| key_traverser.value);

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
        })
    }

    fn name(&self) -> &'static str {
        "branch"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Branch
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        use crate::traversal::step::execute_traversal_streaming;

        // Evaluate branch traversal to get the key value
        let mut branch_results =
            execute_traversal_streaming(&ctx, &self.branch_traversal, input.clone());

        // Get the first result as the branch key
        let key_value = branch_results.next().map(|t| t.value);

        // Find matching option branch
        let branch = match key_value {
            Some(key) => {
                let option_key = OptionKeyWrapper(OptionKey::Value(key));
                self.options.get(&option_key).or(self.none_branch.as_ref())
            }
            None => self.none_branch.as_ref(),
        };

        match branch {
            Some(branch) => {
                let results: Vec<_> = execute_traversal_streaming(&ctx, branch, input).collect();
                Box::new(results.into_iter())
            }
            None => Box::new(std::iter::empty()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traversal::step::DynStep;

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

        let _: Box<dyn DynStep> = Box::new(WhereStep::new(sub.clone()));
        let _: Box<dyn DynStep> = Box::new(NotStep::new(sub.clone()));
        let _: Box<dyn DynStep> = Box::new(AndStep::new(vec![sub.clone()]));
        let _: Box<dyn DynStep> = Box::new(OrStep::new(vec![sub.clone()]));
        let _: Box<dyn DynStep> = Box::new(UnionStep::new(vec![sub.clone()]));
        let _: Box<dyn DynStep> = Box::new(CoalesceStep::new(vec![sub.clone()]));
        let _: Box<dyn DynStep> = Box::new(ChooseStep::new(sub.clone(), sub.clone(), sub.clone()));
        let _: Box<dyn DynStep> = Box::new(OptionalStep::new(sub.clone()));
        let _: Box<dyn DynStep> = Box::new(LocalStep::new(sub));
    }

    // ==========================================================================
    // OptionKey Tests
    // ==========================================================================

    #[test]
    fn option_key_value_from_string() {
        let key = OptionKey::value("person");
        assert_eq!(key, OptionKey::Value(Value::String("person".to_string())));
    }

    #[test]
    fn option_key_value_from_integer() {
        let key = OptionKey::value(42i64);
        assert_eq!(key, OptionKey::Value(Value::Int(42)));
    }

    #[test]
    fn option_key_value_from_bool() {
        let key = OptionKey::value(true);
        assert_eq!(key, OptionKey::Value(Value::Bool(true)));
    }

    #[test]
    fn option_key_none_creates_none_variant() {
        let key = OptionKey::none();
        assert_eq!(key, OptionKey::None);
    }

    #[test]
    fn option_key_from_str() {
        let key: OptionKey = "person".into();
        assert_eq!(key, OptionKey::Value(Value::String("person".to_string())));
    }

    #[test]
    fn option_key_from_string() {
        let key: OptionKey = String::from("software").into();
        assert_eq!(key, OptionKey::Value(Value::String("software".to_string())));
    }

    #[test]
    fn option_key_from_i64() {
        let key: OptionKey = 42i64.into();
        assert_eq!(key, OptionKey::Value(Value::Int(42)));
    }

    #[test]
    fn option_key_from_i32() {
        let key: OptionKey = 42i32.into();
        assert_eq!(key, OptionKey::Value(Value::Int(42)));
    }

    #[test]
    fn option_key_from_bool() {
        let key: OptionKey = true.into();
        assert_eq!(key, OptionKey::Value(Value::Bool(true)));
    }

    #[test]
    fn option_key_from_value() {
        let key: OptionKey = Value::Float(3.15).into();
        assert_eq!(key, OptionKey::Value(Value::Float(3.15)));
    }

    // ==========================================================================
    // OptionKeyWrapper Tests
    // ==========================================================================

    #[test]
    fn option_key_wrapper_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_key(key: &OptionKeyWrapper) -> u64 {
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            hasher.finish()
        }

        // Same keys should produce same hash
        let key1 = OptionKeyWrapper(OptionKey::from("person"));
        let key2 = OptionKeyWrapper(OptionKey::from("person"));
        assert_eq!(hash_key(&key1), hash_key(&key2));

        // Different keys should (generally) produce different hashes
        let key3 = OptionKeyWrapper(OptionKey::from("software"));
        assert_ne!(hash_key(&key1), hash_key(&key3));

        // None keys should have consistent hash
        let none1 = OptionKeyWrapper(OptionKey::None);
        let none2 = OptionKeyWrapper(OptionKey::None);
        assert_eq!(hash_key(&none1), hash_key(&none2));

        // None should differ from value keys
        assert_ne!(hash_key(&key1), hash_key(&none1));
    }

    #[test]
    fn option_key_wrapper_equality() {
        let key1 = OptionKeyWrapper(OptionKey::from("person"));
        let key2 = OptionKeyWrapper(OptionKey::from("person"));
        let key3 = OptionKeyWrapper(OptionKey::from("software"));
        let none = OptionKeyWrapper(OptionKey::None);

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
        assert_ne!(key1, none);
    }

    #[test]
    fn option_key_wrapper_in_hashmap() {
        let mut map: HashMap<OptionKeyWrapper, &str> = HashMap::new();
        map.insert(OptionKeyWrapper(OptionKey::from("person")), "people branch");
        map.insert(
            OptionKeyWrapper(OptionKey::from("software")),
            "software branch",
        );
        map.insert(OptionKeyWrapper(OptionKey::None), "default branch");

        assert_eq!(
            map.get(&OptionKeyWrapper(OptionKey::from("person"))),
            Some(&"people branch")
        );
        assert_eq!(
            map.get(&OptionKeyWrapper(OptionKey::from("software"))),
            Some(&"software branch")
        );
        assert_eq!(
            map.get(&OptionKeyWrapper(OptionKey::None)),
            Some(&"default branch")
        );
        assert_eq!(map.get(&OptionKeyWrapper(OptionKey::from("unknown"))), None);
    }

    // ==========================================================================
    // BranchStep Tests
    // ==========================================================================

    #[test]
    fn branch_step_new_creates_empty_options() {
        let branch_traversal = Traversal::<Value, Value>::new();
        let step = BranchStep::new(branch_traversal);

        assert!(step.options.is_empty());
        assert!(step.none_branch.is_none());
    }

    #[test]
    fn branch_step_add_option_adds_to_map() {
        let branch_traversal = Traversal::<Value, Value>::new();
        let option_traversal = Traversal::<Value, Value>::new();

        let step = BranchStep::new(branch_traversal).add_option("person", option_traversal);

        assert_eq!(step.options.len(), 1);
        assert!(step
            .options
            .contains_key(&OptionKeyWrapper(OptionKey::from("person"))));
        assert!(step.none_branch.is_none());
    }

    #[test]
    fn branch_step_add_option_with_none_key_sets_none_branch() {
        let branch_traversal = Traversal::<Value, Value>::new();
        let option_traversal = Traversal::<Value, Value>::new();

        let step = BranchStep::new(branch_traversal).add_option(OptionKey::None, option_traversal);

        assert!(step.options.is_empty());
        assert!(step.none_branch.is_some());
    }

    #[test]
    fn branch_step_add_none_option_sets_none_branch() {
        let branch_traversal = Traversal::<Value, Value>::new();
        let option_traversal = Traversal::<Value, Value>::new();

        let step = BranchStep::new(branch_traversal).add_none_option(option_traversal);

        assert!(step.options.is_empty());
        assert!(step.none_branch.is_some());
    }

    #[test]
    fn branch_step_multiple_options() {
        let branch_traversal = Traversal::<Value, Value>::new();
        let option1 = Traversal::<Value, Value>::new();
        let option2 = Traversal::<Value, Value>::new();
        let default = Traversal::<Value, Value>::new();

        let step = BranchStep::new(branch_traversal)
            .add_option("person", option1)
            .add_option("software", option2)
            .add_none_option(default);

        assert_eq!(step.options.len(), 2);
        assert!(step
            .options
            .contains_key(&OptionKeyWrapper(OptionKey::from("person"))));
        assert!(step
            .options
            .contains_key(&OptionKeyWrapper(OptionKey::from("software"))));
        assert!(step.none_branch.is_some());
    }

    #[test]
    fn branch_step_compiles() {
        let branch_traversal = Traversal::<Value, Value>::new();
        let step = BranchStep::new(branch_traversal);
        assert_eq!(step.name(), "branch");
    }

    #[test]
    fn branch_step_is_clonable() {
        let branch_traversal = Traversal::<Value, Value>::new();
        let option_traversal = Traversal::<Value, Value>::new();

        let step = BranchStep::new(branch_traversal)
            .add_option("person", option_traversal.clone())
            .add_none_option(option_traversal);

        let cloned = step.clone();
        assert_eq!(cloned.options.len(), step.options.len());
        assert_eq!(cloned.none_branch.is_some(), step.none_branch.is_some());
    }

    #[test]
    fn branch_step_implements_any_step() {
        let branch_traversal = Traversal::<Value, Value>::new();
        let step = BranchStep::new(branch_traversal);
        let _: Box<dyn DynStep> = Box::new(step);
    }
}
