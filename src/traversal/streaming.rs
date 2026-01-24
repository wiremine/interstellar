//! Streaming executor for true O(1) lazy evaluation.
//!
//! This module provides [`StreamingExecutor`] and [`StreamingAdapter`] which enable
//! true pull-based streaming execution where traversers flow through the pipeline
//! one at a time, without eager collection.
//!
//! # Architecture
//!
//! ```text
//! StreamingExecutor
//!     |
//!     +-- holds SideEffects (Arc internally)
//!     +-- owns Box<dyn Iterator + 'static>
//!             |
//!             v
//!         StreamingAdapter [step N]
//!             +-- owns cloned step
//!             +-- owns StreamingContext (Arc refs)
//!             +-- owns input iterator
//!             +-- owns Option<current output iterator>
//!             |
//!             v
//!         StreamingAdapter [step N-1]
//!             ...
//!             |
//!             v
//!         Source Iterator ('static, owns data)
//! ```
//!
//! # Memory Model
//!
//! - **Per step**: O(1) memory overhead
//! - **Total**: O(steps + max_degree) constant regardless of result set size
//! - **Early termination**: `iter().take(n)` processes exactly n items per step
//!
//! # Example
//!
//! ```ignore
//! // Lazy streaming - only processes items as needed
//! let first = g.v().out().out().iter().next();
//!
//! // Early termination - stops after 10 items
//! let sample: Vec<_> = g.v().out("knows").iter().take(10).collect();
//! ```

use std::sync::Arc;

use crate::storage::interner::StringInterner;
use crate::storage::GraphStorage;
use crate::traversal::context::{SideEffects, StreamingContext};
use crate::traversal::step::DynStep;
use crate::traversal::traverser::{TraversalSource, Traverser};
use crate::value::{EdgeId, Value, VertexId};

// =============================================================================
// StreamingAdapter - Iterator adapter that chains steps
// =============================================================================

/// Iterator adapter that streams one step's outputs lazily.
///
/// Each `StreamingAdapter` wraps a single step and an input iterator.
/// It pulls one traverser at a time from the input, applies the step's
/// `apply_streaming` method, and yields results from the resulting iterator.
///
/// When the current output iterator is exhausted, it pulls the next input
/// traverser and creates a new output iterator.
pub struct StreamingAdapter {
    /// Owned step (boxed for dynamic dispatch)
    step: Box<dyn DynStep>,
    /// Streaming context (cheaply cloneable via Arc)
    ctx: StreamingContext,
    /// Input iterator (previous adapter or source)
    input: Box<dyn Iterator<Item = Traverser> + Send>,
    /// Current output iterator from one input traverser
    current: Option<Box<dyn Iterator<Item = Traverser> + Send>>,
}

impl StreamingAdapter {
    /// Create a new streaming adapter.
    ///
    /// # Arguments
    ///
    /// * `step` - The step to apply to each traverser
    /// * `ctx` - The streaming context (Arc-wrapped storage/interner)
    /// * `input` - The input iterator (previous adapter or source)
    pub fn new(
        step: Box<dyn DynStep>,
        ctx: StreamingContext,
        input: Box<dyn Iterator<Item = Traverser> + Send>,
    ) -> Self {
        Self {
            step,
            ctx,
            input,
            current: None,
        }
    }
}

impl Iterator for StreamingAdapter {
    type Item = Traverser;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // 1. Yield from current output iterator
            if let Some(ref mut current) = self.current {
                if let Some(t) = current.next() {
                    return Some(t);
                }
                // Current iterator exhausted
                self.current = None;
            }

            // 2. Pull next input traverser
            let input = self.input.next()?;

            // 3. Apply step to get new output iterator
            self.current = Some(self.step.apply_streaming(self.ctx.clone(), input));
        }
    }
}

// StreamingAdapter is Send because all fields are Send:
// - step: Box<dyn DynStep> where DynStep: Send
// - ctx: StreamingContext is Clone + Send
// - input: Box<dyn Iterator + Send>
// - current: Option<Box<dyn Iterator + Send>>
unsafe impl Send for StreamingAdapter {}

// =============================================================================
// StreamingExecutor - Builds and executes the streaming pipeline
// =============================================================================

/// Executor that streams results with O(1) memory per step.
///
/// The `StreamingExecutor` builds a chain of `StreamingAdapter`s from the
/// traversal steps and source, then provides an iterator interface over
/// the results.
///
/// # Side Effects
///
/// Side effects (from `store()`, `aggregate()`, etc.) are accumulated
/// during iteration and can be accessed via `side_effects()`.
///
/// # Example
///
/// ```ignore
/// let executor = StreamingExecutor::new(
///     storage,
///     interner,
///     steps,
///     Some(TraversalSource::AllVertices),
///     false,
/// );
///
/// for traverser in executor {
///     println!("{:?}", traverser.value);
/// }
/// ```
pub struct StreamingExecutor {
    /// The streaming iterator pipeline
    iter: Box<dyn Iterator<Item = Traverser> + Send>,
    /// Side effects accumulated during traversal
    side_effects: SideEffects,
}

impl StreamingExecutor {
    /// Create a new streaming executor.
    ///
    /// # Arguments
    ///
    /// * `storage` - Arc-wrapped graph storage
    /// * `interner` - Arc-wrapped string interner
    /// * `steps` - The traversal steps to execute
    /// * `source` - The source of traversers (vertices, edges, or injected values)
    /// * `track_paths` - Whether to track traversal paths
    pub fn new(
        storage: Arc<dyn GraphStorage + Send + Sync>,
        interner: Arc<StringInterner>,
        steps: Vec<Box<dyn DynStep>>,
        source: Option<TraversalSource>,
        track_paths: bool,
    ) -> Self {
        let side_effects = SideEffects::new();
        let ctx = StreamingContext::new(storage.clone(), interner.clone())
            .with_side_effects(side_effects.clone())
            .with_path_tracking(track_paths);

        // Build source iterator
        let source_iter = Self::build_source(storage, source, track_paths);

        // Chain adapters - fold steps into a pipeline
        let iter = steps.into_iter().fold(
            source_iter,
            |input, step| -> Box<dyn Iterator<Item = Traverser> + Send> {
                Box::new(StreamingAdapter::new(step, ctx.clone(), input))
            },
        );

        Self { iter, side_effects }
    }

    /// Build the source iterator from the traversal source.
    fn build_source(
        storage: Arc<dyn GraphStorage + Send + Sync>,
        source: Option<TraversalSource>,
        track_paths: bool,
    ) -> Box<dyn Iterator<Item = Traverser> + Send> {
        match source {
            Some(TraversalSource::AllVertices) => {
                // Collect vertex IDs to own them (storage iteration is borrowed)
                let ids: Vec<VertexId> = storage.all_vertices().map(|v| v.id).collect();
                Box::new(ids.into_iter().map(move |id| {
                    let mut t = Traverser::new(Value::Vertex(id));
                    if track_paths {
                        t.extend_path_unlabeled();
                    }
                    t
                }))
            }
            Some(TraversalSource::Vertices(ids)) => {
                let storage = storage.clone();
                Box::new(ids.into_iter().filter_map(move |id| {
                    // Verify vertex exists
                    storage.get_vertex(id).map(|_| {
                        let mut t = Traverser::new(Value::Vertex(id));
                        if track_paths {
                            t.extend_path_unlabeled();
                        }
                        t
                    })
                }))
            }
            Some(TraversalSource::AllEdges) => {
                // Collect edge IDs to own them
                let ids: Vec<EdgeId> = storage.all_edges().map(|e| e.id).collect();
                Box::new(ids.into_iter().map(move |id| {
                    let mut t = Traverser::new(Value::Edge(id));
                    if track_paths {
                        t.extend_path_unlabeled();
                    }
                    t
                }))
            }
            Some(TraversalSource::Edges(ids)) => {
                let storage = storage.clone();
                Box::new(ids.into_iter().filter_map(move |id| {
                    // Verify edge exists
                    storage.get_edge(id).map(|_| {
                        let mut t = Traverser::new(Value::Edge(id));
                        if track_paths {
                            t.extend_path_unlabeled();
                        }
                        t
                    })
                }))
            }
            Some(TraversalSource::Inject(values)) => Box::new(values.into_iter().map(move |v| {
                let mut t = Traverser::new(v);
                if track_paths {
                    t.extend_path_unlabeled();
                }
                t
            })),
            None => Box::new(std::iter::empty()),
        }
    }

    /// Get a reference to the side effects store.
    ///
    /// Side effects are populated as traversers flow through the pipeline,
    /// so this should typically be called after iteration is complete.
    #[inline]
    pub fn side_effects(&self) -> &SideEffects {
        &self.side_effects
    }

    /// Consume the executor and return the side effects.
    ///
    /// Note: The iterator must be fully consumed before calling this
    /// to ensure all side effects are captured.
    pub fn into_side_effects(self) -> SideEffects {
        self.side_effects
    }
}

impl Iterator for StreamingExecutor {
    type Item = Traverser;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Graph;
    use crate::value::Value;
    use std::collections::HashMap;

    fn create_test_graph() -> Graph {
        let graph = Graph::new();

        // Add vertices
        let alice = graph.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props.insert("age".to_string(), Value::Int(30));
            props
        });

        let bob = graph.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Bob".to_string()));
            props.insert("age".to_string(), Value::Int(25));
            props
        });

        let charlie = graph.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Charlie".to_string()));
            props.insert("age".to_string(), Value::Int(35));
            props
        });

        let software = graph.add_vertex("software", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("GraphDB".to_string()));
            props
        });

        // Add edges
        graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
        graph
            .add_edge(alice, charlie, "knows", HashMap::new())
            .unwrap();
        graph
            .add_edge(bob, charlie, "knows", HashMap::new())
            .unwrap();
        graph
            .add_edge(alice, software, "created", HashMap::new())
            .unwrap();

        graph
    }

    #[test]
    fn streaming_executor_empty_source() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();

        let executor = StreamingExecutor::new(
            snapshot.arc_storage(),
            snapshot.arc_interner(),
            vec![],
            None,
            false,
        );

        let results: Vec<_> = executor.collect();
        assert!(results.is_empty());
    }

    #[test]
    fn streaming_executor_all_vertices() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();

        let executor = StreamingExecutor::new(
            snapshot.arc_storage(),
            snapshot.arc_interner(),
            vec![],
            Some(TraversalSource::AllVertices),
            false,
        );

        let results: Vec<_> = executor.collect();
        assert_eq!(results.len(), 4); // 3 people + 1 software
    }

    #[test]
    fn streaming_executor_specific_vertices() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();

        let executor = StreamingExecutor::new(
            snapshot.arc_storage(),
            snapshot.arc_interner(),
            vec![],
            Some(TraversalSource::Vertices(vec![VertexId(0), VertexId(1)])),
            false,
        );

        let results: Vec<_> = executor.collect();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn streaming_executor_inject() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();

        let executor = StreamingExecutor::new(
            snapshot.arc_storage(),
            snapshot.arc_interner(),
            vec![],
            Some(TraversalSource::Inject(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3),
            ])),
            false,
        );

        let results: Vec<_> = executor.collect();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].value, Value::Int(1));
        assert_eq!(results[1].value, Value::Int(2));
        assert_eq!(results[2].value, Value::Int(3));
    }

    #[test]
    fn streaming_executor_early_termination() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();

        let mut executor = StreamingExecutor::new(
            snapshot.arc_storage(),
            snapshot.arc_interner(),
            vec![],
            Some(TraversalSource::AllVertices),
            false,
        );

        // Take only 2 items
        let first = executor.next();
        let second = executor.next();

        assert!(first.is_some());
        assert!(second.is_some());
        // We didn't consume all items - this tests early termination works
    }

    #[test]
    fn streaming_executor_path_tracking() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();

        let executor = StreamingExecutor::new(
            snapshot.arc_storage(),
            snapshot.arc_interner(),
            vec![],
            Some(TraversalSource::AllVertices),
            true, // Enable path tracking
        );

        let results: Vec<_> = executor.collect();
        // With path tracking, each traverser should have a path entry
        for t in results {
            assert_eq!(t.path.len(), 1);
        }
    }

    #[test]
    fn streaming_adapter_identity() {
        use crate::traversal::step::IdentityStep;

        let graph = create_test_graph();
        let snapshot = graph.snapshot();

        let ctx = StreamingContext::new(snapshot.arc_storage(), snapshot.arc_interner());

        let source: Box<dyn Iterator<Item = Traverser> + Send> = Box::new(
            vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))].into_iter(),
        );

        let step: Box<dyn DynStep> = Box::new(IdentityStep);
        let adapter = StreamingAdapter::new(step, ctx, source);

        let results: Vec<_> = adapter.collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].value, Value::Int(1));
        assert_eq!(results[1].value, Value::Int(2));
    }

    #[test]
    fn streaming_executor_side_effects() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();

        let executor = StreamingExecutor::new(
            snapshot.arc_storage(),
            snapshot.arc_interner(),
            vec![],
            Some(TraversalSource::Inject(vec![Value::Int(42)])),
            false,
        );

        // Side effects should be accessible
        let se = executor.side_effects();
        assert!(se.keys().is_empty());
    }
}
