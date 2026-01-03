//! Repeat step for iterative graph exploration.
//!
//! The `repeat()` step enables iterative graph exploration with fine-grained
//! control over termination and emission. It processes the graph in breadth-first
//! order, maintaining a frontier of traversers at the current depth.
//!
//! # Configuration Options
//!
//! - `times(n)` - Execute exactly n iterations
//! - `until(condition)` - Stop when condition traversal produces results
//! - `emit()` - Emit results from all iterations (not just final)
//! - `emit_if(condition)` - Conditional emission based on traversal
//! - `emit_first()` - Emit the initial input before first iteration
//!
//! # Example
//!
//! ```ignore
//! use rustgremlin::prelude::*;
//!
//! // Get friends-of-friends (2 hops exactly)
//! let fof = g.v()
//!     .has_value("name", "Alice")
//!     .repeat(__.out_labels(&["knows"]))
//!     .times(2)
//!     .to_list();
//!
//! // Traverse until reaching a company vertex
//! let path_to_company = g.v()
//!     .has_value("name", "Alice")
//!     .repeat(__.out())
//!     .until(__.has_label("company"))
//!     .to_list();
//!
//! // Get all vertices within 3 hops, emitting intermediates
//! let all_reachable = g.v()
//!     .has_value("name", "Alice")
//!     .repeat(__.out())
//!     .times(3)
//!     .emit()
//!     .to_list();
//! ```

use crate::traversal::step::{execute_traversal_from, AnyStep};
use crate::traversal::{ExecutionContext, Traversal, Traverser};
use crate::value::Value;

/// Configuration for the repeat step.
///
/// This struct holds all the configuration options that control how the
/// repeat step behaves, including termination conditions and emission behavior.
///
/// # Fields
///
/// - `times` - Maximum number of iterations (None = unlimited)
/// - `until` - Termination condition traversal (stop when this produces results)
/// - `emit` - Whether to emit all intermediate results
/// - `emit_if` - Conditional emission traversal (emit when this produces results)
/// - `emit_first` - Whether to emit the initial input before first iteration
///
/// # Default
///
/// By default, all fields are None/false, which means:
/// - No iteration limit
/// - No termination condition (would loop infinitely if graph has cycles)
/// - Only emit final results
/// - Do not emit initial input
///
/// In practice, you should always set at least `times` or `until` to prevent
/// infinite loops.
#[derive(Clone, Default)]
pub struct RepeatConfig {
    /// Maximum number of iterations (None = unlimited).
    ///
    /// When set, the repeat step will stop after executing the sub-traversal
    /// this many times. A value of `times = Some(2)` means traverse 2 hops.
    pub times: Option<usize>,

    /// Termination condition - stop when this traversal produces results.
    ///
    /// Before each iteration, the until condition is evaluated on the current
    /// traverser. If it produces any results, the traverser is emitted and
    /// removed from the frontier (no more iterations for that path).
    pub until: Option<Traversal<Value, Value>>,

    /// Emit all intermediate results (not just final).
    ///
    /// When true, traversers are emitted after each iteration, not just when
    /// they reach a termination condition or exhaustion.
    pub emit: bool,

    /// Conditional emission - emit only when this traversal produces results.
    ///
    /// When set (and emit is true), only emit a traverser if this condition
    /// traversal produces at least one result. This allows selective emission
    /// of intermediate results.
    pub emit_if: Option<Traversal<Value, Value>>,

    /// Emit the initial input before the first iteration.
    ///
    /// When true (and emit is true), the starting traversers are emitted
    /// before any iterations occur. Useful for including the starting vertex
    /// in results like "all vertices reachable from X including X".
    pub emit_first: bool,
}

impl RepeatConfig {
    /// Create a new RepeatConfig with default values.
    ///
    /// All fields are initialized to None/false.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum number of iterations.
    ///
    /// # Arguments
    ///
    /// * `n` - The maximum number of times to execute the sub-traversal
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = RepeatConfig::new().with_times(3);
    /// assert_eq!(config.times, Some(3));
    /// ```
    pub fn with_times(mut self, n: usize) -> Self {
        self.times = Some(n);
        self
    }

    /// Set the termination condition traversal.
    ///
    /// # Arguments
    ///
    /// * `condition` - A traversal that determines when to stop iterating
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = RepeatConfig::new()
    ///     .with_until(__.has_label("company"));
    /// ```
    pub fn with_until(mut self, condition: Traversal<Value, Value>) -> Self {
        self.until = Some(condition);
        self
    }

    /// Enable emission of intermediate results.
    ///
    /// When enabled, traversers are emitted after each iteration,
    /// not just at the end.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = RepeatConfig::new()
    ///     .with_times(3)
    ///     .with_emit();
    /// assert!(config.emit);
    /// ```
    pub fn with_emit(mut self) -> Self {
        self.emit = true;
        self
    }

    /// Set a conditional emission traversal.
    ///
    /// Also enables emit mode. Only traversers that satisfy the condition
    /// will be emitted.
    ///
    /// # Arguments
    ///
    /// * `condition` - A traversal that determines which traversers to emit
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = RepeatConfig::new()
    ///     .with_times(5)
    ///     .with_emit_if(__.has_label("person"));
    /// assert!(config.emit);
    /// assert!(config.emit_if.is_some());
    /// ```
    pub fn with_emit_if(mut self, condition: Traversal<Value, Value>) -> Self {
        self.emit = true;
        self.emit_if = Some(condition);
        self
    }

    /// Enable emission of initial input before first iteration.
    ///
    /// Requires emit mode to be enabled (either via `with_emit()` or
    /// `with_emit_if()`).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = RepeatConfig::new()
    ///     .with_times(2)
    ///     .with_emit()
    ///     .with_emit_first();
    /// assert!(config.emit_first);
    /// ```
    pub fn with_emit_first(mut self) -> Self {
        self.emit_first = true;
        self
    }
}

impl std::fmt::Debug for RepeatConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RepeatConfig")
            .field("times", &self.times)
            .field("until", &self.until.is_some())
            .field("emit", &self.emit)
            .field("emit_if", &self.emit_if.is_some())
            .field("emit_first", &self.emit_first)
            .finish()
    }
}

// -----------------------------------------------------------------------------
// RepeatStep - Iterative graph exploration step
// -----------------------------------------------------------------------------

/// Iterative graph exploration with configurable termination and emission.
///
/// Executes a sub-traversal repeatedly until a termination condition is met.
/// The step supports:
/// - Fixed iteration counts (`times(n)`)
/// - Condition-based termination (`until(condition)`)
/// - Intermediate result emission (`emit()`, `emit_if()`)
/// - Initial value emission (`emit_first()`)
///
/// # Execution Model
///
/// RepeatStep processes traversers in breadth-first order, maintaining a frontier
/// of traversers at the current depth. This ensures level-by-level processing
/// for graph traversals.
///
/// # Example
///
/// ```ignore
/// // Get friends of friends (exactly 2 hops)
/// g.v().has_value("name", "Alice")
///     .repeat(__.out_labels(&["knows"]))
///     .times(2)
///     .to_list();
///
/// // Traverse until reaching a company vertex
/// g.v().has_value("name", "Alice")
///     .repeat(__.out())
///     .until(__.has_label("company"))
///     .to_list();
/// ```
#[derive(Clone)]
pub struct RepeatStep {
    /// The sub-traversal to repeat each iteration.
    sub: Traversal<Value, Value>,
    /// Configuration controlling termination and emission.
    config: RepeatConfig,
}

impl RepeatStep {
    /// Create a new RepeatStep with default configuration.
    ///
    /// The step will iterate indefinitely until the sub-traversal produces
    /// no more results (graph exhaustion). Use `with_config()` or builder
    /// methods on `RepeatTraversal` to set termination conditions.
    ///
    /// # Arguments
    ///
    /// * `sub` - The sub-traversal to execute each iteration
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = RepeatStep::new(__::out());
    /// // Will iterate until no more outgoing edges
    /// ```
    pub fn new(sub: Traversal<Value, Value>) -> Self {
        Self {
            sub,
            config: RepeatConfig::default(),
        }
    }

    /// Create a new RepeatStep with the specified configuration.
    ///
    /// # Arguments
    ///
    /// * `sub` - The sub-traversal to execute each iteration
    /// * `config` - Configuration controlling termination and emission
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = RepeatConfig::new().with_times(3).with_emit();
    /// let step = RepeatStep::with_config(__::out(), config);
    /// ```
    pub fn with_config(sub: Traversal<Value, Value>, config: RepeatConfig) -> Self {
        Self { sub, config }
    }

    /// Check if a traverser satisfies the until condition.
    ///
    /// Evaluates the `until` traversal on the given traverser. Returns `true`
    /// if the traversal produces at least one result, indicating the traverser
    /// has reached a termination state.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The execution context providing graph access
    /// * `traverser` - The traverser to check
    ///
    /// # Returns
    ///
    /// `true` if the until condition is satisfied (or no until is set returns `false`)
    #[allow(dead_code)] // Used in Phase 4.3 RepeatIterator
    pub(crate) fn satisfies_until(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        match &self.config.until {
            Some(until_trav) => {
                let sub_input = Box::new(std::iter::once(traverser.clone()));
                let mut results = execute_traversal_from(ctx, until_trav, sub_input);
                results.next().is_some()
            }
            None => false,
        }
    }

    /// Check if a traverser should be emitted as intermediate result.
    ///
    /// A traverser is emitted if:
    /// 1. `emit` is `true` AND
    /// 2. Either no `emit_if` condition is set, OR the `emit_if` traversal
    ///    produces at least one result
    ///
    /// # Arguments
    ///
    /// * `ctx` - The execution context providing graph access
    /// * `traverser` - The traverser to check
    ///
    /// # Returns
    ///
    /// `true` if the traverser should be emitted
    #[allow(dead_code)] // Used in Phase 4.3 RepeatIterator
    pub(crate) fn should_emit(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
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

    /// Get a reference to the sub-traversal.
    pub fn sub(&self) -> &Traversal<Value, Value> {
        &self.sub
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &RepeatConfig {
        &self.config
    }
}

impl std::fmt::Debug for RepeatStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RepeatStep")
            .field("sub_steps", &self.sub.step_count())
            .field("config", &self.config)
            .finish()
    }
}

impl AnyStep for RepeatStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Phase 4.2: Return empty iterator as skeleton
        // Phase 4.3 will implement the full RepeatIterator with BFS frontier processing
        let _ = input;
        Box::new(std::iter::empty())
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "repeat"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::storage::InMemoryGraph;
    use crate::traversal::filter::HasLabelStep;
    use crate::traversal::step::IdentityStep;
    use crate::value::VertexId;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_graph() -> Graph {
        let mut storage = InMemoryGraph::new();

        // Add vertices
        let v1 = storage.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props
        });
        let v2 = storage.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Bob".to_string()));
            props
        });
        let v3 = storage.add_vertex("company", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("TechCorp".to_string()));
            props
        });

        // Add edges
        storage.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
        storage
            .add_edge(v2, v3, "works_at", HashMap::new())
            .unwrap();

        Graph::new(Arc::new(storage))
    }

    // -------------------------------------------------------------------------
    // RepeatConfig Tests (existing tests)
    // -------------------------------------------------------------------------

    #[test]
    fn repeat_config_default_has_none_values() {
        let config = RepeatConfig::default();
        assert_eq!(config.times, None);
        assert!(config.until.is_none());
        assert!(!config.emit);
        assert!(config.emit_if.is_none());
        assert!(!config.emit_first);
    }

    #[test]
    fn repeat_config_new_equals_default() {
        let new_config = RepeatConfig::new();
        let default_config = RepeatConfig::default();

        assert_eq!(new_config.times, default_config.times);
        assert_eq!(new_config.emit, default_config.emit);
        assert_eq!(new_config.emit_first, default_config.emit_first);
    }

    #[test]
    fn repeat_config_with_times() {
        let config = RepeatConfig::new().with_times(5);
        assert_eq!(config.times, Some(5));
        assert!(!config.emit);
        assert!(config.until.is_none());
    }

    #[test]
    fn repeat_config_with_times_zero() {
        let config = RepeatConfig::new().with_times(0);
        assert_eq!(config.times, Some(0));
    }

    #[test]
    fn repeat_config_with_until() {
        let condition: Traversal<Value, Value> =
            Traversal::<Value, Value>::new().add_step(IdentityStep::new());
        let config = RepeatConfig::new().with_until(condition);
        assert!(config.until.is_some());
        assert_eq!(config.times, None);
    }

    #[test]
    fn repeat_config_with_emit() {
        let config = RepeatConfig::new().with_emit();
        assert!(config.emit);
        assert!(config.emit_if.is_none());
        assert!(!config.emit_first);
    }

    #[test]
    fn repeat_config_with_emit_if() {
        let condition: Traversal<Value, Value> =
            Traversal::<Value, Value>::new().add_step(IdentityStep::new());
        let config = RepeatConfig::new().with_emit_if(condition);
        assert!(config.emit); // emit_if also sets emit = true
        assert!(config.emit_if.is_some());
    }

    #[test]
    fn repeat_config_with_emit_first() {
        let config = RepeatConfig::new().with_emit().with_emit_first();
        assert!(config.emit);
        assert!(config.emit_first);
    }

    #[test]
    fn repeat_config_builder_chain() {
        let condition: Traversal<Value, Value> =
            Traversal::<Value, Value>::new().add_step(IdentityStep::new());

        let config = RepeatConfig::new()
            .with_times(3)
            .with_until(condition)
            .with_emit()
            .with_emit_first();

        assert_eq!(config.times, Some(3));
        assert!(config.until.is_some());
        assert!(config.emit);
        assert!(config.emit_first);
    }

    #[test]
    fn repeat_config_is_clone() {
        let config = RepeatConfig::new().with_times(2).with_emit();
        let cloned = config.clone();

        assert_eq!(cloned.times, Some(2));
        assert!(cloned.emit);
    }

    #[test]
    fn repeat_config_debug_format() {
        let config = RepeatConfig::new().with_times(3).with_emit();
        let debug_str = format!("{:?}", config);

        assert!(debug_str.contains("RepeatConfig"));
        assert!(debug_str.contains("times"));
        assert!(debug_str.contains("Some(3)"));
        assert!(debug_str.contains("emit"));
        assert!(debug_str.contains("true"));
    }

    #[test]
    fn repeat_config_times_can_be_accessed() {
        let config = RepeatConfig::new().with_times(10);

        if let Some(n) = config.times {
            assert_eq!(n, 10);
        } else {
            panic!("Expected times to be Some(10)");
        }
    }

    #[test]
    fn repeat_config_all_fields_accessible() {
        let until_cond: Traversal<Value, Value> =
            Traversal::<Value, Value>::new().add_step(IdentityStep::new());
        let emit_cond: Traversal<Value, Value> =
            Traversal::<Value, Value>::new().add_step(IdentityStep::new());

        let config = RepeatConfig {
            times: Some(5),
            until: Some(until_cond),
            emit: true,
            emit_if: Some(emit_cond),
            emit_first: true,
        };

        assert_eq!(config.times, Some(5));
        assert!(config.until.is_some());
        assert!(config.emit);
        assert!(config.emit_if.is_some());
        assert!(config.emit_first);
    }

    // -------------------------------------------------------------------------
    // RepeatStep Tests
    // -------------------------------------------------------------------------

    mod repeat_step_tests {
        use super::*;

        #[test]
        fn repeat_step_new_creates_with_default_config() {
            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let step = RepeatStep::new(sub);

            assert_eq!(step.config().times, None);
            assert!(!step.config().emit);
            assert!(!step.config().emit_first);
            assert!(step.config().until.is_none());
            assert!(step.config().emit_if.is_none());
        }

        #[test]
        fn repeat_step_with_config_stores_config() {
            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let config = RepeatConfig::new().with_times(5).with_emit();
            let step = RepeatStep::with_config(sub, config);

            assert_eq!(step.config().times, Some(5));
            assert!(step.config().emit);
        }

        #[test]
        fn repeat_step_sub_accessor() {
            let sub: Traversal<Value, Value> =
                Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let sub: Traversal<Value, Value> = sub.add_step(IdentityStep::new());
            let step = RepeatStep::new(sub);

            assert_eq!(step.sub().step_count(), 2);
        }

        #[test]
        fn repeat_step_config_accessor() {
            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let config = RepeatConfig::new().with_times(3);
            let step = RepeatStep::with_config(sub, config);

            assert_eq!(step.config().times, Some(3));
        }

        #[test]
        fn repeat_step_is_clone() {
            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let config = RepeatConfig::new().with_times(2).with_emit();
            let step = RepeatStep::with_config(sub, config);
            let cloned = step.clone();

            assert_eq!(cloned.config().times, Some(2));
            assert!(cloned.config().emit);
            assert_eq!(cloned.sub().step_count(), 1);
        }

        #[test]
        fn repeat_step_clone_box_returns_any_step() {
            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let step = RepeatStep::new(sub);
            let boxed = step.clone_box();

            assert_eq!(boxed.name(), "repeat");
        }

        #[test]
        fn repeat_step_name_is_repeat() {
            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let step = RepeatStep::new(sub);

            assert_eq!(step.name(), "repeat");
        }

        #[test]
        fn repeat_step_debug_format() {
            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let config = RepeatConfig::new().with_times(3);
            let step = RepeatStep::with_config(sub, config);
            let debug_str = format!("{:?}", step);

            assert!(debug_str.contains("RepeatStep"));
            assert!(debug_str.contains("sub_steps"));
            assert!(debug_str.contains("config"));
        }

        #[test]
        fn repeat_step_can_be_boxed_as_any_step() {
            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let step = RepeatStep::new(sub);
            let boxed: Box<dyn AnyStep> = Box::new(step);

            assert_eq!(boxed.name(), "repeat");
        }

        #[test]
        fn repeat_step_can_be_stored_in_vec() {
            let sub1 = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let sub2 = Traversal::<Value, Value>::new().add_step(IdentityStep::new());

            let steps: Vec<Box<dyn AnyStep>> = vec![
                Box::new(RepeatStep::new(sub1)),
                Box::new(RepeatStep::new(sub2)),
            ];

            assert_eq!(steps.len(), 2);
            assert_eq!(steps[0].name(), "repeat");
            assert_eq!(steps[1].name(), "repeat");
        }

        #[test]
        fn repeat_step_apply_returns_empty_iterator_skeleton() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let step = RepeatStep::new(sub);

            let input = vec![Traverser::from_vertex(VertexId(0))];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Phase 4.2 skeleton returns empty
            assert!(output.is_empty());
        }
    }

    // -------------------------------------------------------------------------
    // RepeatStep satisfies_until Tests
    // -------------------------------------------------------------------------

    mod satisfies_until_tests {
        use super::*;

        #[test]
        fn satisfies_until_returns_false_when_no_until_set() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let step = RepeatStep::new(sub);

            let traverser = Traverser::from_vertex(VertexId(0));
            assert!(!step.satisfies_until(&ctx, &traverser));
        }

        #[test]
        fn satisfies_until_returns_true_when_condition_produces_results() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Until condition: has_label("person") - vertex 0 is a person
            let until_cond =
                Traversal::<Value, Value>::new().add_step(HasLabelStep::single("person"));

            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let config = RepeatConfig::new().with_until(until_cond);
            let step = RepeatStep::with_config(sub, config);

            // Vertex 0 is "person" label, so until should be satisfied
            let traverser = Traverser::from_vertex(VertexId(0));
            assert!(step.satisfies_until(&ctx, &traverser));
        }

        #[test]
        fn satisfies_until_returns_false_when_condition_produces_no_results() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Until condition: has_label("company") - vertex 0 is NOT a company
            let until_cond =
                Traversal::<Value, Value>::new().add_step(HasLabelStep::single("company"));

            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let config = RepeatConfig::new().with_until(until_cond);
            let step = RepeatStep::with_config(sub, config);

            // Vertex 0 is "person" label, so until should NOT be satisfied
            let traverser = Traverser::from_vertex(VertexId(0));
            assert!(!step.satisfies_until(&ctx, &traverser));
        }

        #[test]
        fn satisfies_until_works_with_company_vertex() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Until condition: has_label("company")
            let until_cond =
                Traversal::<Value, Value>::new().add_step(HasLabelStep::single("company"));

            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let config = RepeatConfig::new().with_until(until_cond);
            let step = RepeatStep::with_config(sub, config);

            // Vertex 2 is "company" label
            let traverser = Traverser::from_vertex(VertexId(2));
            assert!(step.satisfies_until(&ctx, &traverser));
        }
    }

    // -------------------------------------------------------------------------
    // RepeatStep should_emit Tests
    // -------------------------------------------------------------------------

    mod should_emit_tests {
        use super::*;

        #[test]
        fn should_emit_returns_false_when_emit_not_set() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let step = RepeatStep::new(sub);

            let traverser = Traverser::from_vertex(VertexId(0));
            assert!(!step.should_emit(&ctx, &traverser));
        }

        #[test]
        fn should_emit_returns_true_when_emit_set_without_condition() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let config = RepeatConfig::new().with_emit();
            let step = RepeatStep::with_config(sub, config);

            let traverser = Traverser::from_vertex(VertexId(0));
            assert!(step.should_emit(&ctx, &traverser));
        }

        #[test]
        fn should_emit_returns_true_when_emit_if_condition_produces_results() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Emit if: has_label("person") - vertex 0 is a person
            let emit_if_cond =
                Traversal::<Value, Value>::new().add_step(HasLabelStep::single("person"));

            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let config = RepeatConfig::new().with_emit_if(emit_if_cond);
            let step = RepeatStep::with_config(sub, config);

            // Vertex 0 is "person" label
            let traverser = Traverser::from_vertex(VertexId(0));
            assert!(step.should_emit(&ctx, &traverser));
        }

        #[test]
        fn should_emit_returns_false_when_emit_if_condition_produces_no_results() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Emit if: has_label("company") - vertex 0 is NOT a company
            let emit_if_cond =
                Traversal::<Value, Value>::new().add_step(HasLabelStep::single("company"));

            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let config = RepeatConfig::new().with_emit_if(emit_if_cond);
            let step = RepeatStep::with_config(sub, config);

            // Vertex 0 is "person" label
            let traverser = Traverser::from_vertex(VertexId(0));
            assert!(!step.should_emit(&ctx, &traverser));
        }

        #[test]
        fn should_emit_works_with_emit_and_emit_if_together() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Emit if: has_label("company")
            let emit_if_cond =
                Traversal::<Value, Value>::new().add_step(HasLabelStep::single("company"));

            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            // Note: with_emit_if already sets emit = true
            let config = RepeatConfig::new().with_emit_if(emit_if_cond);
            let step = RepeatStep::with_config(sub, config);

            // Vertex 2 is "company" label - should emit
            let traverser = Traverser::from_vertex(VertexId(2));
            assert!(step.should_emit(&ctx, &traverser));

            // Vertex 0 is "person" label - should not emit
            let traverser = Traverser::from_vertex(VertexId(0));
            assert!(!step.should_emit(&ctx, &traverser));
        }

        #[test]
        fn should_emit_emits_all_when_emit_true_and_no_emit_if() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let config = RepeatConfig::new().with_emit();
            let step = RepeatStep::with_config(sub, config);

            // All vertices should emit
            for id in [VertexId(0), VertexId(1), VertexId(2)] {
                let traverser = Traverser::from_vertex(id);
                assert!(step.should_emit(&ctx, &traverser));
            }
        }
    }
}
