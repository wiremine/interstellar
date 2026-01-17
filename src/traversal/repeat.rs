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
//! use intersteller::prelude::*;
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
use std::collections::VecDeque;

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
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(RepeatIterator::new(
            ctx,
            input,
            self.sub.clone(),
            self.config.clone(),
            self.clone(),
        ))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "repeat"
    }
}

// -----------------------------------------------------------------------------
// RepeatIterator - BFS frontier processing for iterative graph exploration
// -----------------------------------------------------------------------------

/// Iterator for `RepeatStep` that processes traversers in BFS order.
///
/// Maintains a frontier queue of `(Traverser, loop_count)` pairs and processes
/// one level at a time. This ensures breadth-first traversal order.
///
/// # BFS Execution Model
///
/// 1. Initialize frontier from input traversers (all at loop_count = 0)
/// 2. For each level:
///    - Check termination conditions (`times`, `until`)
///    - Execute sub-traversal for surviving traversers
///    - Add results to next frontier with incremented loop_count
///    - Emit results based on configuration (`emit`, `emit_if`)
/// 3. Continue until frontier is empty
///
/// # Emission Modes
///
/// - Default (no emit): Only emit when traverser terminates (times/until/exhaustion)
/// - `emit()`: Emit after each iteration
/// - `emit_if(condition)`: Emit only when condition is satisfied
/// - `emit_first()`: Also emit the initial input before first iteration
struct RepeatIterator<'a> {
    /// Execution context providing graph access
    ctx: &'a ExecutionContext<'a>,
    /// BFS queue of (traverser, loop_count) pairs
    frontier: VecDeque<(Traverser, usize)>,
    /// Sub-traversal to apply each iteration
    sub: Traversal<Value, Value>,
    /// Configuration controlling termination and emission
    config: RepeatConfig,
    /// Step reference for condition checking methods
    step: RepeatStep,
    /// Buffered results to emit
    emit_buffer: VecDeque<Traverser>,
    /// Whether we've processed the initial input
    initialized: bool,
    /// Original input iterator (consumed during initialization)
    input: Option<Box<dyn Iterator<Item = Traverser> + 'a>>,
}

impl<'a> RepeatIterator<'a> {
    /// Create a new RepeatIterator.
    ///
    /// # Arguments
    ///
    /// * `ctx` - Execution context providing graph access
    /// * `input` - Input iterator of traversers
    /// * `sub` - Sub-traversal to repeat
    /// * `config` - Configuration for termination and emission
    /// * `step` - RepeatStep reference for condition checks
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

    /// Process one level of the BFS frontier.
    ///
    /// For each traverser in the current frontier:
    /// 1. Check if `times` limit is reached - if so, emit and stop
    /// 2. Check if `until` condition is satisfied - if so, emit and stop
    /// 3. Execute sub-traversal to get next-level results
    /// 4. Handle empty results (graph exhaustion) - emit if not in emit mode
    /// 5. Add results to next frontier with incremented loop count
    /// 6. Emit intermediate results if configured
    fn process_frontier(&mut self) {
        // Drain current frontier for processing
        let current_frontier: Vec<_> = self.frontier.drain(..).collect();

        for (traverser, loop_count) in current_frontier {
            // Check times limit BEFORE iteration
            if let Some(max_times) = self.config.times {
                if loop_count >= max_times {
                    // Reached iteration limit - emit final result if not using emit mode
                    // (emit mode already emitted this traverser)
                    if !self.config.emit {
                        self.emit_buffer.push_back(traverser);
                    }
                    continue;
                }
            }

            // Check until condition BEFORE iteration
            if self.step.satisfies_until(self.ctx, &traverser) {
                // Termination condition met - emit and stop iterating this path
                self.emit_buffer.push_back(traverser);
                continue;
            }

            // Execute sub-traversal for this traverser
            let sub_input = Box::new(std::iter::once(traverser.clone()));
            let results: Vec<_> = execute_traversal_from(self.ctx, &self.sub, sub_input).collect();

            if results.is_empty() {
                // No more results from this branch (graph exhaustion)
                // Emit if not already emitted via emit mode
                if !self.config.emit {
                    self.emit_buffer.push_back(traverser);
                }
            } else {
                // Add results to next frontier with incremented loop count
                for result in results {
                    let mut new_traverser = result;
                    new_traverser.inc_loops();

                    // Emit intermediate result if configured
                    if self.step.should_emit(self.ctx, &new_traverser) {
                        self.emit_buffer.push_back(new_traverser.clone());
                    }

                    // Add to frontier for next iteration
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
                        // Emit initial input if configured (emit_first requires emit mode)
                        if self.config.emit_first && self.config.emit {
                            self.emit_buffer.push_back(t.clone());
                        }
                        // Add to frontier with loop_count = 0
                        self.frontier.push_back((t, 0));
                    }
                }
                // Continue to check emit_buffer or process frontier
                continue;
            }

            // Process frontier if we have work to do
            if !self.frontier.is_empty() {
                self.process_frontier();
                continue;
            }

            // No more work - iteration complete
            return None;
        }
    }
}

// -----------------------------------------------------------------------------
// RepeatTraversal - Builder for configuring repeat behavior
// -----------------------------------------------------------------------------

use crate::graph::GraphSnapshot;
use crate::storage::interner::StringInterner;
use crate::traversal::source::BoundTraversal;

/// Builder for configuring repeat step behavior.
///
/// Created by calling `.repeat()` on a `BoundTraversal`, configured via
/// chained methods, and finalized by either:
/// - Calling a terminal step (`to_list()`, `count()`, `next()`)
/// - Calling a continuation step (`has_label()`, `out()`, `dedup()`, etc.)
///
/// # Example
///
/// ```ignore
/// use intersteller::prelude::*;
///
/// // Fixed iteration count
/// let fof = g.v().has_value("name", "Alice")
///     .repeat(__.out_labels(&["knows"]))
///     .times(2)
///     .to_list();
///
/// // Conditional termination
/// let path = g.v().has_value("name", "Alice")
///     .repeat(__.out())
///     .until(__.has_label("company"))
///     .to_list();
///
/// // With intermediate emission
/// let all = g.v().has_value("name", "Alice")
///     .repeat(__.out())
///     .times(3)
///     .emit()
///     .to_list();
/// ```
pub struct RepeatTraversal<'g, In> {
    /// Graph snapshot for execution
    snapshot: &'g GraphSnapshot<'g>,
    /// String interner for label resolution
    interner: &'g StringInterner,
    /// Base traversal before the repeat step
    base: Traversal<In, Value>,
    /// Sub-traversal to repeat each iteration
    sub: Traversal<Value, Value>,
    /// Configuration for termination and emission
    config: RepeatConfig,
    /// Whether to track paths
    track_paths: bool,
}

impl<'g, In> RepeatTraversal<'g, In> {
    /// Create a new RepeatTraversal builder.
    ///
    /// This is typically called via `BoundTraversal::repeat()`.
    ///
    /// # Arguments
    ///
    /// * `snapshot` - Graph snapshot for execution
    /// * `interner` - String interner for label resolution
    /// * `base` - The base traversal before the repeat step
    /// * `sub` - The sub-traversal to repeat
    /// * `track_paths` - Whether path tracking is enabled
    pub(crate) fn new(
        snapshot: &'g GraphSnapshot<'g>,
        interner: &'g StringInterner,
        base: Traversal<In, Value>,
        sub: Traversal<Value, Value>,
        track_paths: bool,
    ) -> Self {
        Self {
            snapshot,
            interner,
            base,
            sub,
            config: RepeatConfig::default(),
            track_paths,
        }
    }

    /// Execute exactly n iterations.
    ///
    /// The repeat loop will stop after exactly n iterations, regardless
    /// of whether `until` conditions are met.
    ///
    /// # Arguments
    ///
    /// * `n` - Number of iterations to execute
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get friends-of-friends (exactly 2 hops)
    /// g.v().has_value("name", "Alice")
    ///     .repeat(__.out_labels(&["knows"]))
    ///     .times(2)
    ///     .to_list()
    /// ```
    pub fn times(mut self, n: usize) -> Self {
        self.config.times = Some(n);
        self
    }

    /// Continue until the condition traversal produces results.
    ///
    /// Before each iteration, the condition is evaluated on each traverser.
    /// If it produces results, that traverser is emitted and removed from
    /// the iteration loop.
    ///
    /// # Arguments
    ///
    /// * `condition` - Traversal that determines when to stop
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Traverse until reaching a company vertex
    /// g.v().has_value("name", "Alice")
    ///     .repeat(__.out())
    ///     .until(__.has_label("company"))
    ///     .to_list()
    /// ```
    pub fn until(mut self, condition: Traversal<Value, Value>) -> Self {
        self.config.until = Some(condition);
        self
    }

    /// Emit results from all iterations (not just final).
    ///
    /// By default, only the final iteration's results are emitted.
    /// With `emit()`, intermediate results from each iteration are
    /// also included in the output.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get all vertices within 3 hops (including intermediates)
    /// g.v().has_value("name", "Alice")
    ///     .repeat(__.out())
    ///     .times(3)
    ///     .emit()
    ///     .to_list()
    /// ```
    pub fn emit(mut self) -> Self {
        self.config.emit = true;
        self
    }

    /// Emit results that satisfy a condition.
    ///
    /// Only intermediate results that satisfy the condition traversal
    /// will be emitted. This also enables emit mode.
    ///
    /// # Arguments
    ///
    /// * `condition` - Traversal that determines which results to emit
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Traverse up to 5 hops, emit only person vertices
    /// g.v().repeat(__.out())
    ///     .times(5)
    ///     .emit_if(__.has_label("person"))
    ///     .to_list()
    /// ```
    pub fn emit_if(mut self, condition: Traversal<Value, Value>) -> Self {
        self.config.emit = true;
        self.config.emit_if = Some(condition);
        self
    }

    /// Emit the initial input before the first iteration.
    ///
    /// When combined with `emit()`, this includes the starting traversers
    /// in the output before any iterations occur.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Include Alice in the results along with all reachable vertices
    /// g.v().has_value("name", "Alice")
    ///     .repeat(__.out())
    ///     .times(2)
    ///     .emit()
    ///     .emit_first()
    ///     .to_list()
    /// ```
    pub fn emit_first(mut self) -> Self {
        self.config.emit_first = true;
        self
    }

    /// Finalize repeat configuration and return to normal traversal.
    ///
    /// This is called internally when a terminal or continuation step
    /// is invoked on the builder.
    fn finalize(self) -> BoundTraversal<'g, In, Value> {
        let repeat_step = RepeatStep::with_config(self.sub, self.config);
        let mut bound = BoundTraversal::new(
            self.snapshot,
            self.interner,
            self.base.add_step(repeat_step),
        );
        if self.track_paths {
            bound = bound.with_path();
        }
        bound
    }
}

// -----------------------------------------------------------------------------
// Terminal steps on RepeatTraversal
// -----------------------------------------------------------------------------

impl<'g, In> RepeatTraversal<'g, In> {
    /// Execute and collect all values into a list.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = g.v().repeat(__.out()).times(2).to_list();
    /// ```
    pub fn to_list(self) -> Vec<Value> {
        self.finalize().to_list()
    }

    /// Execute and collect all unique values into a set.
    pub fn to_set(self) -> std::collections::HashSet<Value> {
        self.finalize().to_set()
    }

    /// Execute and return the first value, if any.
    pub fn next(self) -> Option<Value> {
        self.finalize().next()
    }

    /// Check if the traversal produces any results.
    pub fn has_next(self) -> bool {
        self.finalize().has_next()
    }

    /// Execute and count the number of results.
    pub fn count(self) -> u64 {
        self.finalize().count()
    }

    /// Execute and return the first n values.
    pub fn take(self, n: usize) -> Vec<Value> {
        self.finalize().take(n)
    }

    /// Execute and consume the traversal, discarding results.
    pub fn iterate(self) {
        self.finalize().iterate()
    }
}

// -----------------------------------------------------------------------------
// Continuation steps on RepeatTraversal (return to BoundTraversal)
// -----------------------------------------------------------------------------

impl<'g, In> RepeatTraversal<'g, In> {
    /// Filter elements by label.
    pub fn has_label(self, label: impl Into<String>) -> BoundTraversal<'g, In, Value> {
        self.finalize().has_label(label)
    }

    /// Filter elements by any of the given labels.
    pub fn has_label_any<I, S>(self, labels: I) -> BoundTraversal<'g, In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.finalize().has_label_any(labels)
    }

    /// Filter elements by property existence.
    pub fn has(self, key: impl Into<String>) -> BoundTraversal<'g, In, Value> {
        self.finalize().has(key)
    }

    /// Filter elements by property value equality.
    pub fn has_value(
        self,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> BoundTraversal<'g, In, Value> {
        self.finalize().has_value(key, value)
    }

    /// Deduplicate traversers by value.
    pub fn dedup(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().dedup()
    }

    /// Limit the number of traversers.
    pub fn limit(self, count: usize) -> BoundTraversal<'g, In, Value> {
        self.finalize().limit(count)
    }

    /// Skip the first n traversers.
    pub fn skip(self, count: usize) -> BoundTraversal<'g, In, Value> {
        self.finalize().skip(count)
    }

    /// Extract property values.
    pub fn values(self, key: impl Into<String>) -> BoundTraversal<'g, In, Value> {
        self.finalize().values(key)
    }

    /// Extract the ID from elements.
    pub fn id(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().id()
    }

    /// Extract the label from elements.
    pub fn label(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().label()
    }

    /// Traverse to outgoing adjacent vertices.
    pub fn out(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().out()
    }

    /// Traverse to outgoing adjacent vertices via edges with given labels.
    pub fn out_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        self.finalize().out_labels(labels)
    }

    /// Traverse to incoming adjacent vertices.
    pub fn in_(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().in_()
    }

    /// Traverse to incoming adjacent vertices via edges with given labels.
    pub fn in_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        self.finalize().in_labels(labels)
    }

    /// Traverse to adjacent vertices in both directions.
    pub fn both(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().both()
    }

    /// Traverse to adjacent vertices in both directions via edges with given labels.
    pub fn both_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        self.finalize().both_labels(labels)
    }

    /// Traverse to outgoing edges.
    pub fn out_e(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().out_e()
    }

    /// Traverse to incoming edges.
    pub fn in_e(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().in_e()
    }

    /// Traverse to all incident edges.
    pub fn both_e(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().both_e()
    }

    /// Convert the path to a Value::List.
    pub fn path(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().path()
    }

    /// Label the current position in the traversal path.
    pub fn as_(self, label: &str) -> BoundTraversal<'g, In, Value> {
        self.finalize().as_(label)
    }

    /// Select multiple labeled values from the path.
    pub fn select(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        self.finalize().select(labels)
    }

    /// Select a single labeled value from the path.
    pub fn select_one(self, label: &str) -> BoundTraversal<'g, In, Value> {
        self.finalize().select_one(label)
    }

    /// Filter by sub-traversal existence.
    pub fn where_(self, sub: Traversal<Value, Value>) -> BoundTraversal<'g, In, Value> {
        self.finalize().where_(sub)
    }

    /// Filter by sub-traversal non-existence.
    pub fn not(self, sub: Traversal<Value, Value>) -> BoundTraversal<'g, In, Value> {
        self.finalize().not(sub)
    }

    /// Execute multiple branches and merge results.
    pub fn union(self, branches: Vec<Traversal<Value, Value>>) -> BoundTraversal<'g, In, Value> {
        self.finalize().union(branches)
    }

    /// Try branches in order, return first non-empty result.
    pub fn coalesce(self, branches: Vec<Traversal<Value, Value>>) -> BoundTraversal<'g, In, Value> {
        self.finalize().coalesce(branches)
    }

    /// Conditional branching.
    pub fn choose(
        self,
        condition: Traversal<Value, Value>,
        if_true: Traversal<Value, Value>,
        if_false: Traversal<Value, Value>,
    ) -> BoundTraversal<'g, In, Value> {
        self.finalize().choose(condition, if_true, if_false)
    }

    /// Optional traversal with fallback to input.
    pub fn optional(self, sub: Traversal<Value, Value>) -> BoundTraversal<'g, In, Value> {
        self.finalize().optional(sub)
    }

    /// Execute sub-traversal in isolated scope.
    pub fn local(self, sub: Traversal<Value, Value>) -> BoundTraversal<'g, In, Value> {
        self.finalize().local(sub)
    }

    /// Convert to BoundTraversal without adding any additional step.
    ///
    /// This is useful when you need to continue the traversal chain but
    /// don't want to add any filter or transformation step.
    pub fn identity(self) -> BoundTraversal<'g, In, Value> {
        self.finalize()
    }

    /// Get the loop count for each traverser.
    ///
    /// Returns the number of times the repeat loop has iterated for each
    /// traverser. This is particularly useful for understanding how many
    /// hops were required to reach a particular vertex.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get loop counts for vertices reached in repeat
    /// g.v().has_value("name", "Alice")
    ///     .repeat(__.out_labels(&["knows"]))
    ///     .times(2)
    ///     .emit()
    ///     .loops()
    ///     .to_list()
    /// ```
    pub fn loops(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().loops()
    }
}

impl<'g, In> std::fmt::Debug for RepeatTraversal<'g, In> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RepeatTraversal")
            .field("base_steps", &self.base.step_count())
            .field("sub_steps", &self.sub.step_count())
            .field("config", &self.config)
            .field("track_paths", &self.track_paths)
            .finish()
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

        Graph::new(storage)
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
        fn repeat_step_apply_with_times_terminates() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Use identity step with times(2) - will loop twice then emit
            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let config = RepeatConfig::new().with_times(2);
            let step = RepeatStep::with_config(sub, config);

            let input = vec![Traverser::from_vertex(VertexId(0))];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Should emit final result after 2 iterations
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Vertex(VertexId(0)));
            // Loop count should be 2 (incremented each iteration)
            assert_eq!(output[0].loops, 2);
        }

        #[test]
        fn repeat_step_apply_with_times_zero_emits_immediately() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let config = RepeatConfig::new().with_times(0);
            let step = RepeatStep::with_config(sub, config);

            let input = vec![Traverser::from_vertex(VertexId(0))];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // times(0) means don't iterate at all - emit input immediately
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].loops, 0);
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

    // -------------------------------------------------------------------------
    // RepeatIterator Tests - BFS frontier processing
    // -------------------------------------------------------------------------

    mod repeat_iterator_tests {
        use super::*;
        use crate::traversal::navigation::OutStep;

        /// Create a test graph with a chain: Alice -> Bob -> TechCorp
        fn create_chain_graph() -> Graph {
            let mut storage = InMemoryGraph::new();

            // Add vertices
            let v0 = storage.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Alice".to_string()));
                props
            });
            let v1 = storage.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Bob".to_string()));
                props
            });
            let v2 = storage.add_vertex("company", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("TechCorp".to_string()));
                props
            });

            // Create chain: Alice -> Bob -> TechCorp
            storage.add_edge(v0, v1, "knows", HashMap::new()).unwrap();
            storage
                .add_edge(v1, v2, "works_at", HashMap::new())
                .unwrap();

            Graph::new(storage)
        }

        #[test]
        fn repeat_out_times_1_traverses_one_hop() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // repeat(out()).times(1) from Alice should reach Bob
            let sub = Traversal::<Value, Value>::new().add_step(OutStep::new());
            let config = RepeatConfig::new().with_times(1);
            let step = RepeatStep::with_config(sub, config);

            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Vertex(VertexId(1))); // Bob
            assert_eq!(output[0].loops, 1);
        }

        #[test]
        fn repeat_out_times_2_traverses_two_hops() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // repeat(out()).times(2) from Alice should reach TechCorp
            let sub = Traversal::<Value, Value>::new().add_step(OutStep::new());
            let config = RepeatConfig::new().with_times(2);
            let step = RepeatStep::with_config(sub, config);

            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Vertex(VertexId(2))); // TechCorp
            assert_eq!(output[0].loops, 2);
        }

        #[test]
        fn repeat_out_times_3_exhausts_at_leaf() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // repeat(out()).times(3) from Alice - only 2 hops exist, should stop at TechCorp
            let sub = Traversal::<Value, Value>::new().add_step(OutStep::new());
            let config = RepeatConfig::new().with_times(3);
            let step = RepeatStep::with_config(sub, config);

            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Vertex(VertexId(2))); // TechCorp (exhausted)
            assert_eq!(output[0].loops, 2); // Only reached 2 hops before exhaustion
        }

        #[test]
        fn repeat_out_until_company_terminates_correctly() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // repeat(out()).until(has_label("company")) from Alice
            let until_cond =
                Traversal::<Value, Value>::new().add_step(HasLabelStep::single("company"));
            let sub = Traversal::<Value, Value>::new().add_step(OutStep::new());
            let config = RepeatConfig::new().with_until(until_cond);
            let step = RepeatStep::with_config(sub, config);

            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Vertex(VertexId(2))); // TechCorp
        }

        #[test]
        fn repeat_out_emit_includes_intermediates() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // repeat(out()).times(2).emit() from Alice should emit Bob and TechCorp
            let sub = Traversal::<Value, Value>::new().add_step(OutStep::new());
            let config = RepeatConfig::new().with_times(2).with_emit();
            let step = RepeatStep::with_config(sub, config);

            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // With emit(), we get results from each iteration
            assert_eq!(output.len(), 2);
            // BFS order: Bob (loop 1), TechCorp (loop 2)
            assert_eq!(output[0].value, Value::Vertex(VertexId(1))); // Bob
            assert_eq!(output[0].loops, 1);
            assert_eq!(output[1].value, Value::Vertex(VertexId(2))); // TechCorp
            assert_eq!(output[1].loops, 2);
        }

        #[test]
        fn repeat_out_emit_first_includes_starting_vertex() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // repeat(out()).times(1).emit().emit_first() from Alice
            let sub = Traversal::<Value, Value>::new().add_step(OutStep::new());
            let config = RepeatConfig::new()
                .with_times(1)
                .with_emit()
                .with_emit_first();
            let step = RepeatStep::with_config(sub, config);

            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // emit_first emits Alice first, then emit() emits Bob
            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Vertex(VertexId(0))); // Alice (emit_first)
            assert_eq!(output[0].loops, 0);
            assert_eq!(output[1].value, Value::Vertex(VertexId(1))); // Bob (emit)
            assert_eq!(output[1].loops, 1);
        }

        #[test]
        fn repeat_out_emit_if_only_emits_matching() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // repeat(out()).times(2).emit_if(has_label("company")) from Alice
            // Should only emit TechCorp (company), not Bob (person)
            let emit_if_cond =
                Traversal::<Value, Value>::new().add_step(HasLabelStep::single("company"));
            let sub = Traversal::<Value, Value>::new().add_step(OutStep::new());
            let config = RepeatConfig::new().with_times(2).with_emit_if(emit_if_cond);
            let step = RepeatStep::with_config(sub, config);

            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only TechCorp matches emit_if condition
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Vertex(VertexId(2))); // TechCorp
        }

        #[test]
        fn repeat_handles_multiple_input_traversers() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // repeat(out()).times(1) from both Alice and Bob
            let sub = Traversal::<Value, Value>::new().add_step(OutStep::new());
            let config = RepeatConfig::new().with_times(1);
            let step = RepeatStep::with_config(sub, config);

            let input = vec![
                Traverser::from_vertex(VertexId(0)), // Alice
                Traverser::from_vertex(VertexId(1)), // Bob
            ];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Alice -> Bob, Bob -> TechCorp
            assert_eq!(output.len(), 2);
            let values: Vec<_> = output.iter().map(|t| &t.value).collect();
            assert!(values.contains(&&Value::Vertex(VertexId(1)))); // Bob from Alice
            assert!(values.contains(&&Value::Vertex(VertexId(2)))); // TechCorp from Bob
        }

        #[test]
        fn repeat_handles_empty_input() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let sub = Traversal::<Value, Value>::new().add_step(OutStep::new());
            let config = RepeatConfig::new().with_times(2);
            let step = RepeatStep::with_config(sub, config);

            let input: Vec<Traverser> = vec![];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn repeat_from_leaf_with_no_outgoing_edges() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // repeat(out()).times(2) from TechCorp (leaf node)
            let sub = Traversal::<Value, Value>::new().add_step(OutStep::new());
            let config = RepeatConfig::new().with_times(2);
            let step = RepeatStep::with_config(sub, config);

            let input = vec![Traverser::from_vertex(VertexId(2))]; // TechCorp (leaf)
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // TechCorp has no outgoing edges - emits immediately due to exhaustion
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Vertex(VertexId(2)));
            assert_eq!(output[0].loops, 0); // No iterations completed
        }
    }

    // -------------------------------------------------------------------------
    // RepeatTraversal Builder Tests
    // -------------------------------------------------------------------------

    mod repeat_traversal_builder_tests {
        use super::*;
        use crate::traversal::TraversalSource;

        /// Create a chain graph: Alice -> Bob -> TechCorp
        fn create_chain_graph() -> Graph {
            let mut storage = InMemoryGraph::new();

            let v0 = storage.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Alice".to_string()));
                props
            });
            let v1 = storage.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Bob".to_string()));
                props
            });
            let v2 = storage.add_vertex("company", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("TechCorp".to_string()));
                props
            });

            storage.add_edge(v0, v1, "knows", HashMap::new()).unwrap();
            storage
                .add_edge(v1, v2, "works_at", HashMap::new())
                .unwrap();

            Graph::new(storage)
        }

        #[test]
        fn repeat_traversal_builder_times_configures_correctly() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            // Create base traversal directly
            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // Configure with times(2)
            let builder = builder.times(2);
            assert_eq!(builder.config.times, Some(2));
        }

        #[test]
        fn repeat_traversal_builder_emit_configures_correctly() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // Configure with emit()
            let builder = builder.emit();
            assert!(builder.config.emit);
            assert!(!builder.config.emit_first);
        }

        #[test]
        fn repeat_traversal_builder_emit_first_configures_correctly() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            let builder = builder.emit().emit_first();
            assert!(builder.config.emit);
            assert!(builder.config.emit_first);
        }

        #[test]
        fn repeat_traversal_builder_until_configures_correctly() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let until_cond =
                Traversal::<Value, Value>::new().add_step(HasLabelStep::single("company"));

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            let builder = builder.until(until_cond);
            assert!(builder.config.until.is_some());
        }

        #[test]
        fn repeat_traversal_builder_emit_if_configures_correctly() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let emit_cond =
                Traversal::<Value, Value>::new().add_step(HasLabelStep::single("person"));

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            let builder = builder.emit_if(emit_cond);
            assert!(builder.config.emit);
            assert!(builder.config.emit_if.is_some());
        }

        #[test]
        fn repeat_traversal_builder_chained_config() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // Chain multiple configurations
            let builder = builder.times(3).emit().emit_first();
            assert_eq!(builder.config.times, Some(3));
            assert!(builder.config.emit);
            assert!(builder.config.emit_first);
        }

        #[test]
        fn repeat_traversal_builder_to_list_executes() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // times(1) from Alice should reach Bob
            let results = builder.times(1).to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(1))); // Bob
        }

        #[test]
        fn repeat_traversal_builder_count_executes() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // times(2) from Alice should reach TechCorp (1 result)
            let count = builder.times(2).count();
            assert_eq!(count, 1);
        }

        #[test]
        fn repeat_traversal_builder_next_executes() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            let result = builder.times(1).next();
            assert!(result.is_some());
            assert_eq!(result.unwrap(), Value::Vertex(VertexId(1))); // Bob
        }

        #[test]
        fn repeat_traversal_builder_continuation_has_label() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // times(2).has_label("company") - should match TechCorp
            let results = builder.times(2).has_label("company").to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(2)));
        }

        #[test]
        fn repeat_traversal_builder_continuation_dedup() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // times(2).dedup() - result is already unique
            let results = builder.times(2).dedup().to_list();
            assert_eq!(results.len(), 1);
        }

        #[test]
        fn repeat_traversal_builder_debug_format() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false)
                .times(2)
                .emit();

            let debug_str = format!("{:?}", builder);
            assert!(debug_str.contains("RepeatTraversal"));
            assert!(debug_str.contains("base_steps"));
            assert!(debug_str.contains("sub_steps"));
            assert!(debug_str.contains("config"));
        }

        // -------------------------------------------------------------------------
        // Terminal Step Coverage Tests
        // -------------------------------------------------------------------------

        #[test]
        fn repeat_traversal_builder_to_set_executes() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            let results = builder.times(2).emit().to_set();
            // Should contain Bob (VertexId(1)) and TechCorp (VertexId(2))
            assert_eq!(results.len(), 2);
            assert!(results.contains(&Value::Vertex(VertexId(1))));
            assert!(results.contains(&Value::Vertex(VertexId(2))));
        }

        #[test]
        fn repeat_traversal_builder_has_next_returns_true() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            assert!(builder.times(1).has_next());
        }

        #[test]
        fn repeat_traversal_builder_has_next_returns_false_for_empty() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            // Start from TechCorp (leaf) - no outgoing edges
            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(2)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            // Filter to non-existent label after repeat
            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            let result = builder.times(1).has_label("nonexistent").has_next();
            assert!(!result);
        }

        #[test]
        fn repeat_traversal_builder_take_executes() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // emit() produces 2 results, take(1) should return just 1
            let results = builder.times(2).emit().take(1);
            assert_eq!(results.len(), 1);
        }

        #[test]
        fn repeat_traversal_builder_iterate_consumes() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // iterate() should not panic and consume the traversal
            builder.times(2).iterate();
        }

        // -------------------------------------------------------------------------
        // Continuation Step Coverage Tests
        // -------------------------------------------------------------------------

        #[test]
        fn repeat_traversal_builder_continuation_has_label_any() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            let results = builder
                .times(2)
                .emit()
                .has_label_any(vec!["person", "company"])
                .to_list();
            // Both Bob (person) and TechCorp (company) match
            assert_eq!(results.len(), 2);
        }

        #[test]
        fn repeat_traversal_builder_continuation_has() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // All vertices have "name" property
            let results = builder.times(1).has("name").to_list();
            assert_eq!(results.len(), 1); // Bob has "name"
        }

        #[test]
        fn repeat_traversal_builder_continuation_has_value() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            let results = builder.times(1).has_value("name", "Bob").to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(1)));
        }

        #[test]
        fn repeat_traversal_builder_continuation_limit() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // emit() produces 2 results, limit(1) keeps only first
            let results = builder.times(2).emit().limit(1).to_list();
            assert_eq!(results.len(), 1);
        }

        #[test]
        fn repeat_traversal_builder_continuation_skip() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // emit() produces 2 results (Bob, TechCorp), skip(1) skips first
            let results = builder.times(2).emit().skip(1).to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(2))); // TechCorp
        }

        #[test]
        fn repeat_traversal_builder_continuation_id() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            let results = builder.times(1).id().to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Int(1)); // Bob's ID
        }

        #[test]
        fn repeat_traversal_builder_continuation_label() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            let results = builder.times(2).label().to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::String("company".to_string())); // TechCorp's label
        }

        // -------------------------------------------------------------------------
        // Navigation Continuation Step Coverage Tests
        // -------------------------------------------------------------------------

        #[test]
        fn repeat_traversal_builder_continuation_out() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            // Start from Alice
            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // times(1) -> Bob, then .out() -> TechCorp
            let results = builder.times(1).out().to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(2))); // TechCorp
        }

        #[test]
        fn repeat_traversal_builder_continuation_out_labels() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // times(1) -> Bob, then out_labels(["works_at"]) -> TechCorp
            let results = builder.times(1).out_labels(&["works_at"]).to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(2)));
        }

        #[test]
        fn repeat_traversal_builder_continuation_in() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            // Start from TechCorp
            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(2)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::InStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // times(1) -> Bob, then .in_() -> Alice
            let results = builder.times(1).in_().to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(0))); // Alice
        }

        #[test]
        fn repeat_traversal_builder_continuation_in_labels() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            // Start from TechCorp
            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(2)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::InStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // times(1) -> Bob, then in_labels(["knows"]) -> Alice
            let results = builder.times(1).in_labels(&["knows"]).to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(0)));
        }

        #[test]
        fn repeat_traversal_builder_continuation_both() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            // Start from Bob (middle of chain)
            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(1)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // times(1) -> TechCorp, then both() -> Bob (the only neighbor)
            let results = builder.times(1).both().to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(1))); // Bob
        }

        #[test]
        fn repeat_traversal_builder_continuation_both_labels() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            // Start from Bob
            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(1)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // times(1) -> TechCorp, then both_labels(["works_at"]) -> Bob
            let results = builder.times(1).both_labels(&["works_at"]).to_list();
            assert_eq!(results.len(), 1);
        }

        // -------------------------------------------------------------------------
        // Edge Navigation Continuation Step Coverage Tests
        // -------------------------------------------------------------------------

        #[test]
        fn repeat_traversal_builder_continuation_out_e() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // times(1) -> Bob, then out_e() -> edge Bob->TechCorp
            let results = builder.times(1).out_e().to_list();
            assert_eq!(results.len(), 1);
        }

        #[test]
        fn repeat_traversal_builder_continuation_in_e() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            // Start from TechCorp
            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(2)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::InStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // times(1) -> Bob, then in_e() -> edge Alice->Bob
            let results = builder.times(1).in_e().to_list();
            assert_eq!(results.len(), 1);
        }

        #[test]
        fn repeat_traversal_builder_continuation_both_e() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            // Start from Bob (middle vertex)
            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(1)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // times(1) -> TechCorp, then both_e() -> edge Bob->TechCorp
            let results = builder.times(1).both_e().to_list();
            assert_eq!(results.len(), 1);
        }

        // -------------------------------------------------------------------------
        // Path and Selection Continuation Step Coverage Tests
        // -------------------------------------------------------------------------

        #[test]
        fn repeat_traversal_builder_continuation_path() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            // Enable path tracking
            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, true);

            let results = builder.times(2).path().to_list();
            assert_eq!(results.len(), 1);
            // Path should be a list
            if let Value::List(path) = &results[0] {
                assert!(path.len() >= 2); // At least start and end
            } else {
                panic!("Expected path to be a List");
            }
        }

        #[test]
        fn repeat_traversal_builder_continuation_as_and_select() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, true);

            // times(1).as_("end").select(["end"])
            let results = builder.times(1).as_("end").select(&["end"]).to_list();
            assert_eq!(results.len(), 1);
        }

        #[test]
        fn repeat_traversal_builder_continuation_select_one() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, true);

            let results = builder
                .times(1)
                .as_("result")
                .select_one("result")
                .to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(1))); // Bob
        }

        // -------------------------------------------------------------------------
        // Filter Continuation Step Coverage Tests
        // -------------------------------------------------------------------------

        #[test]
        fn repeat_traversal_builder_continuation_where() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // where_ with a condition that filters to person label
            let where_cond =
                Traversal::<Value, Value>::new().add_step(HasLabelStep::single("person"));

            let results = builder.times(2).emit().where_(where_cond).to_list();
            // Only Bob (person) should pass, not TechCorp (company)
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(1)));
        }

        #[test]
        fn repeat_traversal_builder_continuation_not() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // not(has_label("person")) - should filter OUT persons, keep companies
            let not_cond =
                Traversal::<Value, Value>::new().add_step(HasLabelStep::single("person"));

            let results = builder.times(2).emit().not(not_cond).to_list();
            // Only TechCorp (company) should pass
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(2)));
        }

        // -------------------------------------------------------------------------
        // Branch Continuation Step Coverage Tests
        // -------------------------------------------------------------------------

        #[test]
        fn repeat_traversal_builder_continuation_union() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // union of id() and label() on Bob
            let id_branch = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::transform::metadata::IdStep::new());
            let label_branch = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::transform::metadata::LabelStep::new());

            let results = builder
                .times(1)
                .union(vec![id_branch, label_branch])
                .to_list();
            assert_eq!(results.len(), 2); // id and label
        }

        #[test]
        fn repeat_traversal_builder_continuation_coalesce() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // coalesce: try values("nonexistent"), then values("name")
            let first_branch = Traversal::<Value, Value>::new().add_step(
                crate::traversal::transform::values::ValuesStep::new("nonexistent"),
            );
            let fallback_branch = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::transform::values::ValuesStep::new("name"));

            let results = builder
                .times(1)
                .coalesce(vec![first_branch, fallback_branch])
                .to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::String("Bob".to_string()));
        }

        #[test]
        fn repeat_traversal_builder_continuation_choose() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // choose(has_label("person"), id(), label())
            let condition =
                Traversal::<Value, Value>::new().add_step(HasLabelStep::single("person"));
            let if_true = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::transform::metadata::IdStep::new());
            let if_false = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::transform::metadata::LabelStep::new());

            let results = builder
                .times(1)
                .choose(condition, if_true, if_false)
                .to_list();
            // Bob is a person, so id() is chosen -> 1
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Int(1));
        }

        #[test]
        fn repeat_traversal_builder_continuation_optional() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // optional(out()) from Bob - should go to TechCorp
            let optional_sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let results = builder.times(1).optional(optional_sub).to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(2))); // TechCorp
        }

        #[test]
        fn repeat_traversal_builder_continuation_local() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // local(identity()) - should just pass through
            let local_sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::step::IdentityStep::new());

            let results = builder.times(1).local(local_sub).to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(1))); // Bob
        }

        // -------------------------------------------------------------------------
        // Identity and Loops Continuation Step Coverage Tests
        // -------------------------------------------------------------------------

        #[test]
        fn repeat_traversal_builder_continuation_identity() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // identity() just passes through
            let results = builder.times(1).identity().to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(1))); // Bob
        }

        #[test]
        fn repeat_traversal_builder_continuation_loops() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            // loops() returns the loop count for each traverser
            let results = builder.times(2).emit().loops().to_list();
            // emit() produces traversers at loop 1 and loop 2
            assert_eq!(results.len(), 2);
            assert!(results.contains(&Value::Int(1)));
            assert!(results.contains(&Value::Int(2)));
        }

        #[test]
        fn repeat_traversal_builder_continuation_values() {
            let graph = create_chain_graph();
            let snapshot = graph.snapshot();

            let base: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(0)]));

            let sub = Traversal::<Value, Value>::new()
                .add_step(crate::traversal::navigation::OutStep::new());

            let builder = RepeatTraversal::new(&snapshot, snapshot.interner(), base, sub, false);

            let results = builder.times(1).values("name").to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::String("Bob".to_string()));
        }
    }
}
