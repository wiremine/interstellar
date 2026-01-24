//! Traversal struct and core methods.
//!
//! This module provides the main `Traversal` type which represents a traversal
//! pipeline. Traversals are type-erased internally but type-safe externally
//! through phantom type parameters.

use std::marker::PhantomData;

use crate::traversal::step::{DynStep, Step};
use crate::traversal::traverser::TraversalSource;

// -----------------------------------------------------------------------------
// Traversal - main traversal type with type erasure
// -----------------------------------------------------------------------------

/// Main traversal type - type-erased internally, type-safe externally.
///
/// # Type Parameters
///
/// - `In`: The input type this traversal expects (phantom)
/// - `Out`: The output type this traversal produces (phantom)
///
/// Both parameters are "phantom" - used only for compile-time checking.
/// Internally, all values flow as `Value` enum through `Box<dyn DynStep>`.
///
/// # Design Notes
///
/// - Same type for bound and anonymous traversals
/// - Steps are stored as `Vec<Box<dyn DynStep>>` for type erasure
/// - `In = ()` for traversals that start from a source (bound)
/// - `In = SomeType` for traversals that expect input (anonymous)
///
/// # Example
///
/// ```ignore
/// // Create an anonymous traversal
/// let anon: Traversal<Value, Value> = Traversal::new()
///     .add_step(HasLabelStep::single("person"));
///
/// // Anonymous traversals can be appended to bound traversals
/// let bound = g.v().append(anon);
/// ```
pub struct Traversal<In, Out> {
    /// The steps in this traversal (type-erased)
    pub(crate) steps: Vec<Box<dyn DynStep>>,
    /// Optional reference to source (for bound traversals)
    pub(crate) source: Option<TraversalSource>,
    /// Phantom data for input/output types
    pub(crate) _phantom: PhantomData<fn(In) -> Out>,
}

impl<In, Out> Clone for Traversal<In, Out> {
    fn clone(&self) -> Self {
        Self {
            steps: self.steps.iter().map(|s| s.clone_box()).collect(),
            source: self.source.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<In, Out> std::fmt::Debug for Traversal<In, Out> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Traversal")
            .field("source", &self.source)
            .field("steps_count", &self.steps.len())
            .field(
                "step_names",
                &self.steps.iter().map(|s| s.dyn_name()).collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl<In, Out> Default for Traversal<In, Out> {
    fn default() -> Self {
        Self::new()
    }
}

impl<In, Out> Traversal<In, Out> {
    /// Create a new empty traversal (for anonymous traversals).
    ///
    /// Anonymous traversals have no source - they expect input from
    /// the traversal they are appended to.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon: Traversal<Value, Value> = Traversal::new();
    /// ```
    pub fn new() -> Self {
        Self {
            steps: Vec::new(),
            source: None,
            _phantom: PhantomData,
        }
    }

    /// Create a traversal with a source (for bound traversals).
    ///
    /// This is typically called by `GraphTraversalSource` methods like
    /// `v()` and `e()`.
    pub(crate) fn with_source(source: TraversalSource) -> Self {
        Self {
            steps: Vec::new(),
            source: Some(source),
            _phantom: PhantomData,
        }
    }

    /// Add a step to the traversal, returning a new traversal with updated output type.
    ///
    /// This method consumes self and returns a new `Traversal` with the output
    /// type changed to `NewOut`. The phantom type parameters ensure compile-time
    /// safety even though the steps are type-erased internally.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let t: Traversal<(), Value> = Traversal::with_source(TraversalSource::AllVertices)
    ///     .add_step(HasLabelStep::single("person"));
    /// ```
    pub fn add_step<NewOut>(mut self, step: impl Step) -> Traversal<In, NewOut> {
        self.steps.push(Box::new(step));
        Traversal {
            steps: self.steps,
            source: self.source,
            _phantom: PhantomData,
        }
    }

    /// Append another traversal's steps to this one.
    ///
    /// This is used to merge anonymous traversals into bound traversals.
    /// The output type becomes the output type of the appended traversal.
    ///
    /// # Type Safety
    ///
    /// The type system ensures that `other` expects `Out` as input
    /// and produces `Mid` as output.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon: Traversal<Value, Value> = __.out().has_label("person");
    /// let bound = g.v().append(anon);
    /// ```
    pub fn append<Mid>(mut self, other: Traversal<Out, Mid>) -> Traversal<In, Mid> {
        self.steps.extend(other.steps);
        Traversal {
            steps: self.steps,
            source: self.source,
            _phantom: PhantomData,
        }
    }

    /// Get the steps for execution, consuming the traversal.
    ///
    /// Returns the optional source and the list of steps. This is used
    /// by `TraversalExecutor` to execute the traversal.
    #[allow(dead_code)] // Will be used by TraversalExecutor in upcoming phases
    pub(crate) fn into_steps(self) -> (Option<TraversalSource>, Vec<Box<dyn DynStep>>) {
        (self.source, self.steps)
    }

    /// Get the number of steps in this traversal (for testing/debugging).
    #[inline]
    pub fn step_count(&self) -> usize {
        self.steps.len()
    }

    /// Check if this traversal has a source.
    #[inline]
    pub fn has_source(&self) -> bool {
        self.source.is_some()
    }

    /// Get a reference to the source (for debugging/testing).
    pub fn source(&self) -> Option<&TraversalSource> {
        self.source.as_ref()
    }

    /// Get step names for debugging/profiling.
    pub fn step_names(&self) -> Vec<&'static str> {
        self.steps.iter().map(|s| s.dyn_name()).collect()
    }

    /// Get a reference to the steps (for sub-traversal execution).
    ///
    /// This method provides read-only access to the traversal's steps,
    /// enabling the `execute_traversal` helper to apply steps without
    /// consuming the traversal.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().out().has_label("person");
    /// let steps = anon.steps();
    ///
    /// // Use with execute_traversal
    /// let output = execute_traversal(&ctx, steps, input);
    /// ```
    #[inline]
    pub fn steps(&self) -> &[Box<dyn DynStep>] {
        &self.steps
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Graph;
    use crate::traversal::context::{ExecutionContext, SnapshotLike};
    use crate::traversal::step::IdentityStep;
    use crate::traversal::traverser::{TraversalSource, Traverser};
    use crate::value::{Value, VertexId};

    #[test]
    fn new_creates_empty_traversal() {
        let t: Traversal<Value, Value> = Traversal::new();
        assert_eq!(t.step_count(), 0);
        assert!(!t.has_source());
        assert!(t.source().is_none());
    }

    #[test]
    fn default_creates_empty_traversal() {
        let t: Traversal<Value, Value> = Traversal::default();
        assert_eq!(t.step_count(), 0);
        assert!(!t.has_source());
    }

    #[test]
    fn with_source_creates_sourced_traversal() {
        let t: Traversal<(), Value> = Traversal::with_source(TraversalSource::AllVertices);
        assert!(t.has_source());
        assert!(matches!(t.source(), Some(TraversalSource::AllVertices)));
        assert_eq!(t.step_count(), 0);
    }

    #[test]
    fn add_step_increments_count() {
        let t: Traversal<Value, Value> = Traversal::new();
        assert_eq!(t.step_count(), 0);

        let t: Traversal<Value, Value> = t.add_step(IdentityStep::new());
        assert_eq!(t.step_count(), 1);

        let t: Traversal<Value, Value> = t.add_step(IdentityStep::new());
        assert_eq!(t.step_count(), 2);
    }

    #[test]
    fn add_step_preserves_source() {
        let t: Traversal<(), Value> = Traversal::with_source(TraversalSource::AllVertices);
        let t: Traversal<(), Value> = t.add_step(IdentityStep::new());

        assert!(t.has_source());
        assert!(matches!(t.source(), Some(TraversalSource::AllVertices)));
    }

    #[test]
    fn step_names_returns_step_names() {
        let t: Traversal<Value, Value> =
            Traversal::<Value, Value>::new().add_step(IdentityStep::new());
        let t: Traversal<Value, Value> = t.add_step(IdentityStep::new());

        let names = t.step_names();
        assert_eq!(names.len(), 2);
        assert_eq!(names[0], "identity");
        assert_eq!(names[1], "identity");
    }

    #[test]
    fn append_merges_steps() {
        let t1: Traversal<(), Value> =
            Traversal::<(), Value>::with_source(TraversalSource::AllVertices)
                .add_step(IdentityStep::new());
        let t2: Traversal<Value, Value> =
            Traversal::<Value, Value>::new().add_step(IdentityStep::new());
        let t2: Traversal<Value, Value> = t2.add_step(IdentityStep::new());

        let merged = t1.append(t2);
        assert_eq!(merged.step_count(), 3);
        assert!(merged.has_source());
    }

    #[test]
    fn append_drops_second_source() {
        // Even if the second traversal has a source, it should be ignored
        // (anonymous traversals shouldn't have sources in normal usage)
        let t1: Traversal<(), Value> = Traversal::with_source(TraversalSource::AllVertices);
        let t2: Traversal<Value, Value> = Traversal::with_source(TraversalSource::AllEdges);

        // Note: this is unusual usage but the behavior should be defined
        let merged = t1.append(t2);
        assert!(merged.has_source());
        // Source should be from t1, not t2
        assert!(matches!(
            merged.source(),
            Some(TraversalSource::AllVertices)
        ));
    }

    #[test]
    fn clone_creates_independent_copy() {
        let t1: Traversal<Value, Value> =
            Traversal::<Value, Value>::new().add_step(IdentityStep::new());

        let t2 = t1.clone();

        // Both should have same step count
        assert_eq!(t1.step_count(), t2.step_count());

        // They should be independent (adding to one doesn't affect other)
        let t1_modified: Traversal<Value, Value> = t1.add_step(IdentityStep::new());
        assert_eq!(t1_modified.step_count(), 2);
        assert_eq!(t2.step_count(), 1);
    }

    #[test]
    fn clone_preserves_source() {
        let t1: Traversal<(), Value> =
            Traversal::with_source(TraversalSource::Vertices(vec![VertexId(1), VertexId(2)]));
        let t2 = t1.clone();

        assert!(t2.has_source());
        match t2.source() {
            Some(TraversalSource::Vertices(ids)) => {
                assert_eq!(ids.len(), 2);
                assert_eq!(ids[0], VertexId(1));
            }
            _ => panic!("Expected Vertices source"),
        }
    }

    #[test]
    fn into_steps_returns_source_and_steps() {
        let t: Traversal<(), Value> =
            Traversal::<(), Value>::with_source(TraversalSource::AllVertices)
                .add_step(IdentityStep::new());
        let t: Traversal<(), Value> = t.add_step(IdentityStep::new());

        let (source, steps) = t.into_steps();

        assert!(source.is_some());
        assert!(matches!(source, Some(TraversalSource::AllVertices)));
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].dyn_name(), "identity");
    }

    #[test]
    fn into_steps_returns_none_source_for_anonymous() {
        let t: Traversal<Value, Value> =
            Traversal::<Value, Value>::new().add_step(IdentityStep::new());

        let (source, steps) = t.into_steps();

        assert!(source.is_none());
        assert_eq!(steps.len(), 1);
    }

    #[test]
    fn debug_format_shows_info() {
        let t: Traversal<(), Value> =
            Traversal::<(), Value>::with_source(TraversalSource::AllVertices)
                .add_step(IdentityStep::new());

        let debug_str = format!("{:?}", t);
        assert!(debug_str.contains("Traversal"));
        assert!(debug_str.contains("steps_count"));
        assert!(debug_str.contains("step_names"));
    }

    #[test]
    fn steps_can_be_executed_from_into_steps() {
        let graph = Graph::new();
        let snapshot = graph.snapshot();
        let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

        // Create a simple traversal with identity step
        let t: Traversal<Value, Value> =
            Traversal::<Value, Value>::new().add_step(IdentityStep::new());

        let (_source, steps) = t.into_steps();

        // Execute the steps manually
        let input: Vec<Traverser> =
            vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];

        let mut current: Box<dyn Iterator<Item = Traverser> + '_> = Box::new(input.into_iter());
        for step in &steps {
            current = step.apply_dyn(&ctx, current);
        }

        let results: Vec<Traverser> = current.collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].value, Value::Int(1));
        assert_eq!(results[1].value, Value::Int(2));
    }
}
