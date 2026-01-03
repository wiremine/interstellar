//! Graph traversal source and bound traversal types.
//!
//! This module provides:
//! - `GraphTraversalSource`: Entry point for all bound traversals
//! - `BoundTraversal`: A traversal bound to a graph source
//! - `TraversalExecutor`: Executes traversals and produces results
//!
//! # Example
//!
//! ```ignore
//! let graph = Graph::in_memory();
//! let snapshot = graph.snapshot();
//! let g = snapshot.traversal();
//!
//! // Start from all vertices
//! let results = g.v().to_list();
//!
//! // Start from specific vertices
//! let results = g.v_ids([VertexId(1), VertexId(2)]).to_list();
//!
//! // Start from all edges
//! let results = g.e().to_list();
//!
//! // Inject arbitrary values
//! let results = g.inject([1, 2, 3]).to_list();
//! ```

use std::marker::PhantomData;

use crate::graph::GraphSnapshot;
use crate::storage::interner::StringInterner;
use crate::traversal::step::{AnyStep, StartStep};
use crate::traversal::{ExecutionContext, Traversal, TraversalSource, Traverser};
use crate::value::{EdgeId, Value, VertexId};

// -----------------------------------------------------------------------------
// GraphTraversalSource - Entry point for bound traversals
// -----------------------------------------------------------------------------

/// Entry point for all bound traversals.
///
/// Created from a `GraphSnapshot` via `snapshot.traversal()`.
/// The source holds references needed to create an `ExecutionContext` at
/// execution time.
///
/// # Example
///
/// ```ignore
/// let graph = Graph::in_memory();
/// let snapshot = graph.snapshot();
/// let g = snapshot.traversal();
///
/// // All methods return BoundTraversal which can be chained
/// let count = g.v().count();
/// ```
pub struct GraphTraversalSource<'g> {
    snapshot: &'g GraphSnapshot<'g>,
    interner: &'g StringInterner,
}

impl<'g> GraphTraversalSource<'g> {
    /// Create a new traversal source from a snapshot.
    ///
    /// This is typically called via `snapshot.traversal()`.
    pub fn new(snapshot: &'g GraphSnapshot<'g>, interner: &'g StringInterner) -> Self {
        Self { snapshot, interner }
    }

    /// Start traversal from all vertices.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = g.v().to_list();
    /// ```
    pub fn v(&self) -> BoundTraversal<'g, (), Value> {
        BoundTraversal::new(
            self.snapshot,
            self.interner,
            Traversal::with_source(TraversalSource::AllVertices),
        )
    }

    /// Start traversal from specific vertex IDs.
    ///
    /// Non-existent vertex IDs are filtered out during execution.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = g.v_ids([VertexId(1), VertexId(2)]).to_list();
    /// ```
    pub fn v_ids<I>(&self, ids: I) -> BoundTraversal<'g, (), Value>
    where
        I: IntoIterator<Item = VertexId>,
    {
        BoundTraversal::new(
            self.snapshot,
            self.interner,
            Traversal::with_source(TraversalSource::Vertices(ids.into_iter().collect())),
        )
    }

    /// Start traversal from all edges.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = g.e().to_list();
    /// ```
    pub fn e(&self) -> BoundTraversal<'g, (), Value> {
        BoundTraversal::new(
            self.snapshot,
            self.interner,
            Traversal::with_source(TraversalSource::AllEdges),
        )
    }

    /// Start traversal from specific edge IDs.
    ///
    /// Non-existent edge IDs are filtered out during execution.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = g.e_ids([EdgeId(1), EdgeId(2)]).to_list();
    /// ```
    pub fn e_ids<I>(&self, ids: I) -> BoundTraversal<'g, (), Value>
    where
        I: IntoIterator<Item = EdgeId>,
    {
        BoundTraversal::new(
            self.snapshot,
            self.interner,
            Traversal::with_source(TraversalSource::Edges(ids.into_iter().collect())),
        )
    }

    /// Inject arbitrary values into the traversal.
    ///
    /// Creates traversers from the given values, allowing you to start
    /// a traversal from non-graph data.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = g.inject([1, 2, 3]).to_list();
    /// // results: [Value::Int(1), Value::Int(2), Value::Int(3)]
    /// ```
    pub fn inject<T, I>(&self, values: I) -> BoundTraversal<'g, (), Value>
    where
        I: IntoIterator<Item = T>,
        T: Into<Value>,
    {
        let values: Vec<Value> = values.into_iter().map(Into::into).collect();
        BoundTraversal::new(
            self.snapshot,
            self.interner,
            Traversal::with_source(TraversalSource::Inject(values)),
        )
    }

    /// Get the snapshot reference.
    #[inline]
    pub fn snapshot(&self) -> &'g GraphSnapshot<'g> {
        self.snapshot
    }

    /// Get the interner reference.
    #[inline]
    pub fn interner(&self) -> &'g StringInterner {
        self.interner
    }
}

// -----------------------------------------------------------------------------
// BoundTraversal - A traversal bound to a graph
// -----------------------------------------------------------------------------

/// A traversal bound to a graph source.
///
/// This wrapper holds both the traversal and the graph references
/// needed to create an `ExecutionContext` when terminal steps are called.
///
/// # Type Parameters
///
/// - `'g`: The lifetime of the graph snapshot
/// - `In`: The input type (usually `()` for bound traversals)
/// - `Out`: The output type of the traversal
///
/// # Example
///
/// ```ignore
/// let traversal: BoundTraversal<'_, (), Value> = g.v();
/// let results: Vec<Value> = traversal.to_list();
/// ```
pub struct BoundTraversal<'g, In, Out> {
    snapshot: &'g GraphSnapshot<'g>,
    interner: &'g StringInterner,
    traversal: Traversal<In, Out>,
    /// Whether to automatically track paths for navigation steps
    track_paths: bool,
}

impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Create a new bound traversal.
    pub(crate) fn new(
        snapshot: &'g GraphSnapshot<'g>,
        interner: &'g StringInterner,
        traversal: Traversal<In, Out>,
    ) -> Self {
        Self {
            snapshot,
            interner,
            traversal,
            track_paths: false,
        }
    }

    /// Enable automatic path tracking for this traversal.
    ///
    /// When path tracking is enabled, navigation steps automatically
    /// record visited elements to each traverser's path. This is required
    /// for the `path()` step to return meaningful results.
    ///
    /// Note: `as_()` labeled positions are always recorded regardless of
    /// whether `with_path()` is called.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get full traversal paths
    /// let paths = g.v().with_path().out().out().path().to_list();
    /// // Each result is Value::List([start_vertex, middle_vertex, end_vertex])
    /// ```
    pub fn with_path(mut self) -> Self {
        self.track_paths = true;
        self
    }

    /// Check if path tracking is enabled.
    #[inline]
    pub fn is_tracking_paths(&self) -> bool {
        self.track_paths
    }

    /// Add a step to the traversal.
    ///
    /// Returns a new `BoundTraversal` with the output type updated.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let traversal = g.v().add_step(MyCustomStep::new());
    /// ```
    pub fn add_step<NewOut>(self, step: impl AnyStep + 'static) -> BoundTraversal<'g, In, NewOut> {
        BoundTraversal {
            snapshot: self.snapshot,
            interner: self.interner,
            traversal: self.traversal.add_step(step),
            track_paths: self.track_paths,
        }
    }

    /// Append an anonymous traversal's steps to this traversal.
    ///
    /// This is used to merge anonymous traversals (created with `__::`)
    /// into bound traversals.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = __::out().has_label("person");
    /// let results = g.v().append(anon).to_list();
    /// ```
    pub fn append<Mid>(self, anon: Traversal<Out, Mid>) -> BoundTraversal<'g, In, Mid> {
        BoundTraversal {
            snapshot: self.snapshot,
            interner: self.interner,
            traversal: self.traversal.append(anon),
            track_paths: self.track_paths,
        }
    }

    /// Create an execution context for this traversal.
    #[allow(dead_code)] // Will be used in future phases for lazy execution
    fn create_context(&self) -> ExecutionContext<'g> {
        ExecutionContext::new(self.snapshot, self.interner)
    }

    /// Execute the traversal and return an executor that produces traversers.
    ///
    /// The executor owns the collected results and iterates over them.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let executor = g.v().execute();
    /// for traverser in executor {
    ///     println!("{:?}", traverser.value);
    /// }
    /// ```
    pub fn execute(self) -> TraversalExecutor<'g> {
        TraversalExecutor::new(
            self.snapshot,
            self.interner,
            self.traversal,
            self.track_paths,
        )
    }

    /// Get the underlying snapshot.
    #[inline]
    pub fn snapshot(&self) -> &'g GraphSnapshot<'g> {
        self.snapshot
    }

    /// Get the interner reference for label resolution.
    #[inline]
    pub fn interner(&self) -> &'g StringInterner {
        self.interner
    }

    /// Get the number of steps in the traversal.
    #[inline]
    pub fn step_count(&self) -> usize {
        self.traversal.step_count()
    }

    /// Get step names for debugging/profiling.
    pub fn step_names(&self) -> Vec<&'static str> {
        self.traversal.step_names()
    }
}

// -----------------------------------------------------------------------------
// Traversal Step Methods on BoundTraversal
// -----------------------------------------------------------------------------

impl<'g, In> BoundTraversal<'g, In, Value> {
    /// Filter elements by label.
    ///
    /// Keeps only vertices/edges whose label matches the given label.
    /// Non-element values (integers, strings, etc.) are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get all person vertices
    /// let people = g.v().has_label("person").to_list();
    /// ```
    pub fn has_label(self, label: impl Into<String>) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::HasLabelStep;
        self.add_step(HasLabelStep::single(label))
    }

    /// Filter elements by any of the given labels.
    ///
    /// Keeps only vertices/edges whose label matches any of the given labels.
    /// Non-element values (integers, strings, etc.) are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get all person or company vertices
    /// let entities = g.v().has_label_any(&["person", "company"]).to_list();
    /// ```
    pub fn has_label_any<I, S>(self, labels: I) -> BoundTraversal<'g, In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        use crate::traversal::filter::HasLabelStep;
        self.add_step(HasLabelStep::any(labels))
    }

    /// Filter elements by property existence.
    ///
    /// Keeps only vertices/edges that have the specified property.
    /// Non-element values (integers, strings, etc.) are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get all vertices that have an "age" property
    /// let with_age = g.v().has("age").to_list();
    /// ```
    pub fn has(self, key: impl Into<String>) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::HasStep;
        self.add_step(HasStep::new(key))
    }

    /// Filter elements by property value equality.
    ///
    /// Keeps only vertices/edges where the specified property equals the given value.
    /// Non-element values (integers, strings, etc.) are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get all vertices where name == "Alice"
    /// let alice = g.v().has_value("name", "Alice").to_list();
    ///
    /// // Get all vertices where age == 30
    /// let age_30 = g.v().has_value("age", 30i64).to_list();
    /// ```
    pub fn has_value(
        self,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::HasValueStep;
        self.add_step(HasValueStep::new(key, value))
    }

    /// Filter elements by property value using a predicate.
    ///
    /// Keeps only vertices/edges where the specified property satisfies the predicate.
    /// Non-element values (integers, strings, etc.) are filtered out.
    /// Elements without the specified property are also filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// // Get all vertices where age >= 18
    /// let adults = g.v().has_where("age", p::gte(18)).to_list();
    ///
    /// // Get all vertices where name starts with "A"
    /// let a_names = g.v().has_where("name", p::starting_with("A")).to_list();
    ///
    /// // Get vertices with age between 25 and 65
    /// let working_age = g.v().has_where("age", p::between(25, 65)).to_list();
    ///
    /// // Combine predicates with logical operators
    /// let adults_under_65 = g.v().has_where("age", p::and(p::gte(18), p::lt(65))).to_list();
    /// ```
    pub fn has_where(
        self,
        key: impl Into<String>,
        predicate: impl crate::traversal::predicate::Predicate + 'static,
    ) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::HasWhereStep;
        self.add_step(HasWhereStep::new(key, predicate))
    }

    /// Filter elements using a custom predicate.
    ///
    /// The predicate receives the execution context and the value, returning
    /// `true` to keep the traverser or `false` to filter it out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Filter to only positive integers
    /// let positives = g.inject([1i64, -2i64, 3i64])
    ///     .filter(|_ctx, v| matches!(v, Value::Int(n) if *n > 0))
    ///     .to_list();
    /// ```
    pub fn filter<F>(self, predicate: F) -> BoundTraversal<'g, In, Value>
    where
        F: Fn(&crate::traversal::ExecutionContext, &Value) -> bool + Clone + Send + Sync + 'static,
    {
        use crate::traversal::filter::FilterStep;
        self.add_step(FilterStep::new(predicate))
    }

    /// Deduplicate traversers by value.
    ///
    /// Removes duplicate values from the traversal, keeping only the first
    /// occurrence of each value. Uses `Value`'s `Hash` implementation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Remove duplicate vertices when traversing neighbors
    /// let unique_neighbors = g.v().out().dedup().to_list();
    ///
    /// // Dedup injected values
    /// let unique = g.inject([1i64, 2i64, 1i64, 3i64]).dedup().to_list();
    /// // Results: [1, 2, 3]
    /// ```
    pub fn dedup(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::DedupStep;
        self.add_step(DedupStep::new())
    }

    /// Limit the number of traversers passing through.
    ///
    /// Returns at most the specified number of traversers, stopping iteration
    /// after the limit is reached.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get only the first 5 vertices
    /// let first_five = g.v().limit(5).to_list();
    ///
    /// // Limit results after filtering
    /// let top_people = g.v().has_label("person").limit(10).to_list();
    /// ```
    pub fn limit(self, count: usize) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::LimitStep;
        self.add_step(LimitStep::new(count))
    }

    /// Skip the first n traversers.
    ///
    /// Discards the first n traversers and passes through all remaining ones.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Skip the first 10 vertices
    /// let after_skip = g.v().skip(10).to_list();
    ///
    /// // Pagination: skip first page
    /// let page_2 = g.v().has_label("person").skip(20).limit(20).to_list();
    /// ```
    pub fn skip(self, count: usize) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::SkipStep;
        self.add_step(SkipStep::new(count))
    }

    /// Select traversers within a given range.
    ///
    /// Equivalent to `skip(start).limit(end - start)`. Returns traversers
    /// from index `start` (inclusive) to index `end` (exclusive).
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get vertices 10-19 (for pagination)
    /// let page = g.v().range(10, 20).to_list();
    ///
    /// // Get elements 2, 3, 4
    /// let middle = g.inject([0i64, 1i64, 2i64, 3i64, 4i64, 5i64]).range(2, 5).to_list();
    /// // Results: [2, 3, 4]
    /// ```
    pub fn range(self, start: usize, end: usize) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::RangeStep;
        self.add_step(RangeStep::new(start, end))
    }

    /// Filter elements by a single ID.
    ///
    /// Keeps only vertices/edges whose ID matches the given ID.
    /// Non-element values (integers, strings, etc.) are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get a specific vertex by ID
    /// let vertex = g.v().has_id(VertexId(1)).to_list();
    ///
    /// // Get a specific edge by ID
    /// let edge = g.e().has_id(EdgeId(0)).to_list();
    /// ```
    pub fn has_id(self, id: impl Into<Value>) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::HasIdStep;
        self.add_step(HasIdStep::from_value(id))
    }

    /// Filter elements by multiple IDs.
    ///
    /// Keeps only vertices/edges whose ID matches any of the given IDs.
    /// Non-element values (integers, strings, etc.) are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get multiple vertices by ID
    /// let vertices = g.v().has_ids([VertexId(1), VertexId(2), VertexId(3)]).to_list();
    ///
    /// // Get multiple edges by ID
    /// let edges = g.e().has_ids([EdgeId(0), EdgeId(1)]).to_list();
    /// ```
    pub fn has_ids<I, T>(self, ids: I) -> BoundTraversal<'g, In, Value>
    where
        I: IntoIterator<Item = T>,
        T: Into<Value>,
    {
        use crate::traversal::filter::HasIdStep;
        self.add_step(HasIdStep::from_values(
            ids.into_iter().map(Into::into).collect(),
        ))
    }

    // -------------------------------------------------------------------------
    // Navigation steps
    // -------------------------------------------------------------------------

    /// Traverse to outgoing adjacent vertices.
    ///
    /// From each vertex traverser, follows all outgoing edges and returns
    /// the target vertices. Non-vertex traversers produce no output.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get all outgoing neighbors
    /// let neighbors = g.v_ids([VertexId(1)]).out().to_list();
    /// ```
    pub fn out(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::navigation::OutStep;
        self.add_step(OutStep::new())
    }

    /// Traverse to outgoing adjacent vertices via edges with given labels.
    ///
    /// Only edges with one of the specified labels are traversed.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get neighbors via "knows" edges
    /// let friends = g.v().out_labels(&["knows"]).to_list();
    /// ```
    pub fn out_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::navigation::OutStep;
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(OutStep::with_labels(labels))
    }

    /// Traverse to incoming adjacent vertices.
    ///
    /// From each vertex traverser, follows all incoming edges and returns
    /// the source vertices. Non-vertex traversers produce no output.
    ///
    /// Note: Named `in_` to avoid conflict with Rust's `in` keyword.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get all incoming neighbors
    /// let known_by = g.v_ids([VertexId(1)]).in_().to_list();
    /// ```
    pub fn in_(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::navigation::InStep;
        self.add_step(InStep::new())
    }

    /// Traverse to incoming adjacent vertices via edges with given labels.
    ///
    /// Only edges with one of the specified labels are traversed.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get people who know this person
    /// let known_by = g.v().in_labels(&["knows"]).to_list();
    /// ```
    pub fn in_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::navigation::InStep;
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(InStep::with_labels(labels))
    }

    /// Traverse to adjacent vertices in both directions.
    ///
    /// From each vertex traverser, follows all edges (both outgoing and
    /// incoming) and returns the adjacent vertices.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get all neighbors regardless of direction
    /// let neighbors = g.v().both().to_list();
    /// ```
    pub fn both(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::navigation::BothStep;
        self.add_step(BothStep::new())
    }

    /// Traverse to adjacent vertices in both directions via edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get all "knows" neighbors regardless of direction
    /// let connected = g.v().both_labels(&["knows"]).to_list();
    /// ```
    pub fn both_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::navigation::BothStep;
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(BothStep::with_labels(labels))
    }

    /// Traverse to outgoing edges.
    ///
    /// From each vertex traverser, returns all outgoing edges (as edge elements).
    /// Non-vertex traversers produce no output.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get all outgoing edges
    /// let edges = g.v().out_e().to_list();
    /// ```
    pub fn out_e(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::navigation::OutEStep;
        self.add_step(OutEStep::new())
    }

    /// Traverse to outgoing edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get all "knows" edges going out
    /// let knows_edges = g.v().out_e_labels(&["knows"]).to_list();
    /// ```
    pub fn out_e_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::navigation::OutEStep;
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(OutEStep::with_labels(labels))
    }

    /// Traverse to incoming edges.
    ///
    /// From each vertex traverser, returns all incoming edges (as edge elements).
    /// Non-vertex traversers produce no output.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get all incoming edges
    /// let edges = g.v().in_e().to_list();
    /// ```
    pub fn in_e(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::navigation::InEStep;
        self.add_step(InEStep::new())
    }

    /// Traverse to incoming edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get all "knows" edges coming in
    /// let known_by_edges = g.v().in_e_labels(&["knows"]).to_list();
    /// ```
    pub fn in_e_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::navigation::InEStep;
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(InEStep::with_labels(labels))
    }

    /// Traverse to all incident edges (both directions).
    ///
    /// From each vertex traverser, returns all incident edges (as edge elements).
    /// Non-vertex traversers produce no output.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get all incident edges
    /// let edges = g.v().both_e().to_list();
    /// ```
    pub fn both_e(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::navigation::BothEStep;
        self.add_step(BothEStep::new())
    }

    /// Traverse to all incident edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get all "knows" edges in either direction
    /// let knows_edges = g.v().both_e_labels(&["knows"]).to_list();
    /// ```
    pub fn both_e_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::navigation::BothEStep;
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(BothEStep::with_labels(labels))
    }

    /// Get the source (outgoing) vertex of an edge.
    ///
    /// From each edge traverser, returns the source vertex.
    /// Non-edge traversers produce no output.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get source vertices of all edges
    /// let sources = g.e().out_v().to_list();
    /// ```
    pub fn out_v(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::navigation::OutVStep;
        self.add_step(OutVStep::new())
    }

    /// Get the target (incoming) vertex of an edge.
    ///
    /// From each edge traverser, returns the target vertex.
    /// Non-edge traversers produce no output.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get target vertices of all edges
    /// let targets = g.e().in_v().to_list();
    /// ```
    pub fn in_v(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::navigation::InVStep;
        self.add_step(InVStep::new())
    }

    /// Get both vertices of an edge.
    ///
    /// From each edge traverser, returns both the source and target vertices
    /// (2 traversers per edge). Source is returned first, then target.
    /// Non-edge traversers produce no output.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get both vertices of all edges (2 results per edge)
    /// let vertices = g.e().both_v().to_list();
    /// ```
    pub fn both_v(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::navigation::BothVStep;
        self.add_step(BothVStep::new())
    }

    // -------------------------------------------------------------------------
    // Transform steps
    // -------------------------------------------------------------------------

    /// Extract property values from vertices/edges.
    ///
    /// For each input element, extracts the value of the specified property.
    /// Missing properties are silently skipped.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let names = g.v().has_label("person").values("name").to_list();
    /// ```
    pub fn values(self, key: impl Into<String>) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::ValuesStep;
        self.add_step(ValuesStep::new(key))
    }

    /// Extract multiple property values from vertices/edges.
    ///
    /// For each input element, extracts the values of the specified properties.
    /// Missing properties are silently skipped.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let data = g.v().values_multi(&["name", "age"]).to_list();
    /// ```
    pub fn values_multi<I, S>(self, keys: I) -> BoundTraversal<'g, In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        use crate::traversal::transform::ValuesStep;
        self.add_step(ValuesStep::from_keys(keys))
    }

    /// Extract the ID from vertices/edges.
    ///
    /// For each input element, extracts its ID as a `Value::Int`.
    /// Non-element values are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get IDs of all person vertices
    /// let ids = g.v().has_label("person").id().to_list();
    ///
    /// // Get IDs of all edges
    /// let edge_ids = g.e().id().to_list();
    /// ```
    pub fn id(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::IdStep;
        self.add_step(IdStep::new())
    }

    /// Extract the label from vertices/edges.
    ///
    /// For each input element, extracts its label as a `Value::String`.
    /// Non-element values are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get labels of all vertices
    /// let labels = g.v().label().to_list();
    ///
    /// // Get unique labels
    /// let unique_labels = g.v().label().dedup().to_list();
    /// ```
    pub fn label(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::LabelStep;
        self.add_step(LabelStep::new())
    }

    /// Transform each value using a closure.
    ///
    /// The closure receives the execution context and the current value,
    /// returning a new value. This is a 1:1 mapping.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Double all integer values
    /// let doubled = g.inject([1i64, 2i64, 3i64])
    ///     .map(|_ctx, v| {
    ///         if let Value::Int(n) = v {
    ///             Value::Int(n * 2)
    ///         } else {
    ///             v.clone()
    ///         }
    ///     })
    ///     .to_list();
    /// // Results: [2, 4, 6]
    /// ```
    pub fn map<F>(self, f: F) -> BoundTraversal<'g, In, Value>
    where
        F: Fn(&crate::traversal::ExecutionContext, &Value) -> Value + Clone + Send + Sync + 'static,
    {
        use crate::traversal::transform::MapStep;
        self.add_step(MapStep::new(f))
    }

    /// Transform each value to multiple values using a closure.
    ///
    /// The closure receives the execution context and the current value,
    /// returning a `Vec<Value>`. This is a 1:N mapping - each input can
    /// produce zero or more outputs.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Generate a range of values from each integer
    /// let expanded = g.inject([3i64, 5i64])
    ///     .flat_map(|_ctx, v| {
    ///         if let Value::Int(n) = v {
    ///             (0..*n).map(|i| Value::Int(i)).collect()
    ///         } else {
    ///             vec![]
    ///         }
    ///     })
    ///     .to_list();
    /// // Results: [0, 1, 2, 0, 1, 2, 3, 4]
    /// ```
    pub fn flat_map<F>(self, f: F) -> BoundTraversal<'g, In, Value>
    where
        F: Fn(&crate::traversal::ExecutionContext, &Value) -> Vec<Value>
            + Clone
            + Send
            + Sync
            + 'static,
    {
        use crate::traversal::transform::FlatMapStep;
        self.add_step(FlatMapStep::new(f))
    }

    /// Replace each traverser's value with a constant.
    ///
    /// For each input traverser, replaces the value with the specified constant.
    /// All traverser metadata (path, loops, bulk, sack) is preserved.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Replace all vertex values with "found"
    /// let results = g.v().constant("found").to_list();
    /// // All results will be Value::String("found")
    ///
    /// // Count vertices by replacing with 1 and summing
    /// let results = g.v().constant(1i64).to_list();
    /// ```
    pub fn constant(self, value: impl Into<Value>) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::ConstantStep;
        self.add_step(ConstantStep::new(value))
    }

    /// Convert the traverser's path to a Value::List.
    ///
    /// Replaces the traverser's value with a list containing all elements
    /// from its path history. Each path element is converted to its
    /// corresponding Value representation.
    ///
    /// # Note
    ///
    /// For the path to contain elements, you need to use path-tracking steps
    /// like `as()` or enable path tracking. Without path tracking, paths
    /// will be empty.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get the path of a multi-hop traversal
    /// let paths = g.v().out().out().path().to_list();
    /// // Each result is a Value::List of [vertex, vertex, vertex]
    ///
    /// // With labeled steps
    /// let paths = g.v().as("start").out().as("end").path().to_list();
    /// // Path labels are preserved in traverser.path
    /// ```
    pub fn path(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::PathStep;
        self.add_step(PathStep::new())
    }

    /// Label the current position in the traversal path.
    ///
    /// Records the current traverser's value in the path with the specified label.
    /// This enables later retrieval via `select()` or `select_one()`.
    ///
    /// Unlike automatic path tracking, `as_()` labels are always recorded
    /// regardless of whether `with_path()` was called.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Label positions for later selection
    /// let results = g.v().as_("start").out().as_("end")
    ///     .select(&["start", "end"]).to_list();
    ///
    /// // Single label selection
    /// let starts = g.v().as_("x").out().select_one("x").to_list();
    /// ```
    pub fn as_(self, label: &str) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::AsStep;
        self.add_step(AsStep::new(label))
    }

    /// Select multiple labeled values from the path.
    ///
    /// Retrieves values that were labeled with `as_()` and returns them as a Map.
    /// Traversers without any of the requested labels are filtered out.
    ///
    /// # Arguments
    ///
    /// * `labels` - The labels to select from the path
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Returns Map { "a" -> vertex1, "b" -> vertex2 }
    /// let results = g.v().as_("a").out().as_("b")
    ///     .select(&["a", "b"]).to_list();
    /// ```
    pub fn select(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::SelectStep;
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(SelectStep::new(labels))
    }

    /// Select a single labeled value from the path.
    ///
    /// Retrieves the value that was labeled with `as_()` and returns it directly
    /// (not wrapped in a Map). Traversers without the requested label are filtered out.
    ///
    /// # Arguments
    ///
    /// * `label` - The label to select from the path
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Returns the vertex directly (not a Map)
    /// let starts = g.v().as_("x").out().select_one("x").to_list();
    /// ```
    pub fn select_one(self, label: &str) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::SelectStep;
        self.add_step(SelectStep::single(label))
    }

    // -------------------------------------------------------------------------
    // Filter steps using anonymous traversals
    // -------------------------------------------------------------------------

    /// Filter by sub-traversal existence.
    ///
    /// Emits input traverser only if the sub-traversal produces at least one result.
    /// This is the primary mechanism for filtering based on graph structure.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Keep only vertices that have outgoing edges
    /// let with_out = g.v().where_(__.out()).to_list();
    ///
    /// // Keep only vertices that have outgoing "knows" edges to someone named "Bob"
    /// let knows_bob = g.v().where_(__.out_labels(&["knows"]).has_value("name", "Bob")).to_list();
    /// ```
    pub fn where_(
        self,
        sub: crate::traversal::Traversal<Value, Value>,
    ) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::branch::WhereStep;
        self.add_step(WhereStep::new(sub))
    }

    /// Filter by sub-traversal non-existence.
    ///
    /// Emits input traverser only if the sub-traversal produces NO results.
    /// This is the inverse of `where_`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Keep only leaf vertices (no outgoing edges)
    /// let leaves = g.v().not(__.out()).to_list();
    ///
    /// // Keep vertices that don't have a "name" property
    /// let unnamed = g.v().not(__.has("name")).to_list();
    /// ```
    pub fn not(
        self,
        sub: crate::traversal::Traversal<Value, Value>,
    ) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::branch::NotStep;
        self.add_step(NotStep::new(sub))
    }

    /// Filter by multiple sub-traversals (AND logic).
    ///
    /// Emits input traverser only if ALL sub-traversals produce at least one result.
    /// Short-circuits on first failing condition.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Keep vertices that have both outgoing AND incoming edges
    /// let connected = g.v().and_(vec![__.out(), __.in_()]).to_list();
    /// ```
    pub fn and_(
        self,
        subs: Vec<crate::traversal::Traversal<Value, Value>>,
    ) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::branch::AndStep;
        self.add_step(AndStep::new(subs))
    }

    /// Filter by multiple sub-traversals (OR logic).
    ///
    /// Emits input traverser if ANY sub-traversal produces at least one result.
    /// Short-circuits on first successful condition.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Keep vertices that are either "person" OR "software"
    /// let entities = g.v().or_(vec![__.has_label("person"), __.has_label("software")]).to_list();
    /// ```
    pub fn or_(
        self,
        subs: Vec<crate::traversal::Traversal<Value, Value>>,
    ) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::branch::OrStep;
        self.add_step(OrStep::new(subs))
    }

    // -------------------------------------------------------------------------
    // Branch steps using anonymous traversals
    // -------------------------------------------------------------------------

    /// Execute multiple branches and merge results.
    ///
    /// All branches receive each input traverser; results are merged
    /// in branch order.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Get neighbors in both directions
    /// let neighbors = g.v().union(vec![__.out(), __.in_()]).to_list();
    /// ```
    pub fn union(
        self,
        branches: Vec<crate::traversal::Traversal<Value, Value>>,
    ) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::branch::UnionStep;
        self.add_step(UnionStep::new(branches))
    }

    /// Try branches in order, return first non-empty result.
    ///
    /// Short-circuits: once a branch produces results, remaining branches
    /// are not evaluated for that input traverser.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Try to get nickname, fall back to name
    /// let names = g.v().coalesce(vec![__.values("nickname"), __.values("name")]).to_list();
    /// ```
    pub fn coalesce(
        self,
        branches: Vec<crate::traversal::Traversal<Value, Value>>,
    ) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::branch::CoalesceStep;
        self.add_step(CoalesceStep::new(branches))
    }

    /// Conditional branching.
    ///
    /// Evaluates condition traversal; if it produces results, executes
    /// if_true branch, otherwise executes if_false branch.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // If person, get friends; otherwise get all neighbors
    /// let results = g.v().choose(
    ///     __.has_label("person"),
    ///     __.out_labels(&["knows"]),
    ///     __.out()
    /// ).to_list();
    /// ```
    pub fn choose(
        self,
        condition: crate::traversal::Traversal<Value, Value>,
        if_true: crate::traversal::Traversal<Value, Value>,
        if_false: crate::traversal::Traversal<Value, Value>,
    ) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::branch::ChooseStep;
        self.add_step(ChooseStep::new(condition, if_true, if_false))
    }

    /// Optional traversal with fallback to input.
    ///
    /// If sub-traversal produces results, emit those results.
    /// If sub-traversal produces no results, emit the original input.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Try to traverse to friends, keep original if none found
    /// let results = g.v().optional(__.out_labels(&["knows"])).to_list();
    /// ```
    pub fn optional(
        self,
        sub: crate::traversal::Traversal<Value, Value>,
    ) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::branch::OptionalStep;
        self.add_step(OptionalStep::new(sub))
    }

    /// Execute sub-traversal in isolated scope.
    ///
    /// Aggregations (count, fold, etc.) in the sub-traversal operate
    /// independently for each input traverser, not across all inputs.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Count neighbors per vertex (not total neighbors)
    /// let neighbor_counts = g.v().local(__.out().count()).to_list();
    /// ```
    pub fn local(
        self,
        sub: crate::traversal::Traversal<Value, Value>,
    ) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::branch::LocalStep;
        self.add_step(LocalStep::new(sub))
    }

    // -------------------------------------------------------------------------
    // Repeat step
    // -------------------------------------------------------------------------

    /// Start a repeat loop with the given sub-traversal.
    ///
    /// The repeat step enables iterative graph exploration with fine-grained
    /// control over termination and emission. Returns a `RepeatTraversal`
    /// builder that allows configuration via chained methods:
    ///
    /// - `times(n)` - Execute exactly n iterations
    /// - `until(condition)` - Stop when condition traversal produces results
    /// - `emit()` - Emit results from all iterations (not just final)
    /// - `emit_if(condition)` - Conditional emission based on traversal
    /// - `emit_first()` - Emit the initial input before first iteration
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Get friends-of-friends (2 hops exactly)
    /// let fof = g.v()
    ///     .has_value("name", "Alice")
    ///     .repeat(__.out_labels(&["knows"]))
    ///     .times(2)
    ///     .to_list();
    ///
    /// // Traverse until reaching a company vertex
    /// let path_to_company = g.v()
    ///     .has_value("name", "Alice")
    ///     .repeat(__.out())
    ///     .until(__.has_label("company"))
    ///     .to_list();
    ///
    /// // Get all vertices within 3 hops, emitting intermediates
    /// let all_reachable = g.v()
    ///     .has_value("name", "Alice")
    ///     .repeat(__.out())
    ///     .times(3)
    ///     .emit()
    ///     .to_list();
    /// ```
    pub fn repeat(
        self,
        sub: crate::traversal::Traversal<Value, Value>,
    ) -> crate::traversal::repeat::RepeatTraversal<'g, In> {
        crate::traversal::repeat::RepeatTraversal::new(
            self.snapshot,
            self.interner,
            self.traversal,
            sub,
            self.track_paths,
        )
    }
}

impl<'g, In, Out> Clone for BoundTraversal<'g, In, Out> {
    fn clone(&self) -> Self {
        Self {
            snapshot: self.snapshot,
            interner: self.interner,
            traversal: self.traversal.clone(),
            track_paths: self.track_paths,
        }
    }
}

impl<'g, In, Out> std::fmt::Debug for BoundTraversal<'g, In, Out> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundTraversal")
            .field("step_count", &self.traversal.step_count())
            .field("step_names", &self.traversal.step_names())
            .field("track_paths", &self.track_paths)
            .finish()
    }
}

// -----------------------------------------------------------------------------
// TraversalExecutor - Executes traversals and produces results
// -----------------------------------------------------------------------------

/// Executor that owns the traversal state and produces results.
///
/// This struct solves the lifetime issue where `ExecutionContext` needs
/// to outlive the iterator it produces. By owning the context and
/// collecting results eagerly, we avoid complex self-referential
/// lifetime issues.
///
/// # Design Note
///
/// The current design collects results eagerly which is simpler and
/// sufficient for most use cases. Future optimization could introduce
/// streaming execution using crates like `ouroboros` if needed.
///
/// # Example
///
/// ```ignore
/// let executor = g.v().execute();
/// for traverser in executor {
///     println!("{:?}", traverser.value);
/// }
/// ```
pub struct TraversalExecutor<'g> {
    results: std::vec::IntoIter<Traverser>,
    _phantom: PhantomData<&'g ()>,
}

impl<'g> TraversalExecutor<'g> {
    /// Create a new executor and execute the traversal.
    fn new<In, Out>(
        snapshot: &'g GraphSnapshot<'g>,
        interner: &'g StringInterner,
        traversal: Traversal<In, Out>,
        track_paths: bool,
    ) -> Self {
        let ctx = if track_paths {
            ExecutionContext::with_path_tracking(snapshot, interner)
        } else {
            ExecutionContext::new(snapshot, interner)
        };
        let (source, steps) = traversal.into_steps();

        // Start with source traversers - collect immediately to avoid lifetime issues
        let mut current: Vec<Traverser> = match source {
            Some(src) => {
                let start_step = StartStep::new(src);
                start_step
                    .apply(&ctx, Box::new(std::iter::empty()))
                    .collect()
            }
            None => Vec::new(),
        };

        // Apply each step in sequence, collecting after each to avoid lifetime issues
        for step in &steps {
            current = step.apply(&ctx, Box::new(current.into_iter())).collect();
        }

        Self {
            results: current.into_iter(),
            _phantom: PhantomData,
        }
    }

    /// Get the number of results (if known).
    ///
    /// This is exact because results are eagerly collected.
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Check if there are no results.
    pub fn is_empty(&self) -> bool {
        self.results.len() == 0
    }
}

impl Iterator for TraversalExecutor<'_> {
    type Item = Traverser;

    fn next(&mut self) -> Option<Self::Item> {
        self.results.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.results.size_hint()
    }
}

impl ExactSizeIterator for TraversalExecutor<'_> {
    fn len(&self) -> usize {
        self.results.len()
    }
}

// -----------------------------------------------------------------------------
// Terminal step methods on BoundTraversal
// -----------------------------------------------------------------------------

impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Execute and collect all values into a list.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let values: Vec<Value> = g.v().to_list();
    /// ```
    pub fn to_list(self) -> Vec<Value> {
        self.execute().map(|t| t.value).collect()
    }

    /// Execute and collect all unique values into a set.
    ///
    /// Uses `Value`'s `Hash` implementation for deduplication.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let unique: HashSet<Value> = g.v().out().to_set();
    /// ```
    pub fn to_set(self) -> std::collections::HashSet<Value> {
        self.execute().map(|t| t.value).collect()
    }

    /// Execute and return the first value, if any.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let first: Option<Value> = g.v().next();
    /// ```
    pub fn next(self) -> Option<Value> {
        self.execute().next().map(|t| t.value)
    }

    /// Check if the traversal produces any results.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let has_vertices: bool = g.v().has_next();
    /// ```
    pub fn has_next(self) -> bool {
        !self.execute().is_empty()
    }

    /// Execute and return exactly one value, or error.
    ///
    /// Returns an error if there are zero or more than one results.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let vertex = g.v_ids([VertexId(1)]).one()?;
    /// ```
    pub fn one(self) -> Result<Value, crate::error::TraversalError> {
        let results: Vec<_> = self.execute().take(2).collect();
        match results.len() {
            1 => Ok(results.into_iter().next().unwrap().value),
            n => Err(crate::error::TraversalError::NotOne(n)),
        }
    }

    /// Execute and consume the traversal, discarding results.
    ///
    /// Useful for side-effect-only traversals.
    ///
    /// # Example
    ///
    /// ```ignore
    /// g.v().side_effect(|t| println!("{:?}", t)).iterate();
    /// ```
    pub fn iterate(self) {
        for _ in self.execute() {
            // Consume and discard
        }
    }

    /// Execute and count the number of results.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let count: u64 = g.v().count();
    /// ```
    pub fn count(self) -> u64 {
        self.execute().len() as u64
    }

    /// Execute and return the first n values.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let first_five: Vec<Value> = g.v().take(5);
    /// ```
    pub fn take(self, n: usize) -> Vec<Value> {
        self.execute().take(n).map(|t| t.value).collect()
    }

    /// Execute and return an iterator over values.
    ///
    /// This is useful for lazy processing of results.
    ///
    /// # Example
    ///
    /// ```ignore
    /// for value in g.v().iter() {
    ///     println!("{:?}", value);
    /// }
    /// ```
    pub fn iter(self) -> impl Iterator<Item = Value> + 'g {
        self.execute().map(|t| t.value)
    }

    /// Execute and return an iterator over traversers (with metadata).
    ///
    /// # Example
    ///
    /// ```ignore
    /// for traverser in g.v().traversers() {
    ///     println!("{:?}", traverser.path);
    /// }
    /// ```
    pub fn traversers(self) -> impl Iterator<Item = Traverser> + 'g {
        self.execute()
    }

    /// Fold all values using an accumulator function.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let sum = g.v().values("age").fold(0i64, |acc, v| {
    ///     acc + v.as_i64().unwrap_or(0)
    /// });
    /// ```
    pub fn fold<B, F>(self, init: B, f: F) -> B
    where
        F: FnMut(B, Value) -> B,
    {
        self.execute().map(|t| t.value).fold(init, f)
    }

    /// Sum all numeric values.
    ///
    /// Returns `Value::Int(0)` for empty traversals.
    /// Non-numeric values are skipped.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let total = g.v().values("count").sum();
    /// ```
    pub fn sum(self) -> Value {
        let mut int_sum: i64 = 0;
        let mut float_sum: f64 = 0.0;
        let mut has_float = false;

        for traverser in self.execute() {
            match traverser.value {
                Value::Int(n) => int_sum += n,
                Value::Float(f) => {
                    has_float = true;
                    float_sum += f;
                }
                _ => {}
            }
        }

        if has_float {
            Value::Float(int_sum as f64 + float_sum)
        } else {
            Value::Int(int_sum)
        }
    }

    /// Find the minimum value.
    ///
    /// Returns `None` for empty traversals.
    /// Uses `ComparableValue` for ordering.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let min = g.v().values("age").min();
    /// ```
    pub fn min(self) -> Option<Value> {
        self.execute()
            .map(|t| t.value)
            .min_by(|a, b| a.to_comparable().cmp(&b.to_comparable()))
    }

    /// Find the maximum value.
    ///
    /// Returns `None` for empty traversals.
    /// Uses `ComparableValue` for ordering.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let max = g.v().values("age").max();
    /// ```
    pub fn max(self) -> Option<Value> {
        self.execute()
            .map(|t| t.value)
            .max_by(|a, b| a.to_comparable().cmp(&b.to_comparable()))
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::storage::InMemoryGraph;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_empty_graph() -> Graph {
        Graph::new(Arc::new(InMemoryGraph::new()))
    }

    fn create_test_graph() -> Graph {
        let mut storage = InMemoryGraph::new();

        // Add vertices
        let v1 = storage.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props.insert("age".to_string(), Value::Int(30));
            props
        });

        let v2 = storage.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Bob".to_string()));
            props.insert("age".to_string(), Value::Int(25));
            props
        });

        let v3 = storage.add_vertex("software", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Graph DB".to_string()));
            props.insert("version".to_string(), Value::Float(1.0));
            props
        });

        let v4 = storage.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Charlie".to_string()));
            props.insert("age".to_string(), Value::Int(35));
            props
        });

        // Add edges
        storage.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
        storage.add_edge(v2, v3, "uses", HashMap::new()).unwrap();
        storage.add_edge(v1, v3, "uses", HashMap::new()).unwrap();
        storage.add_edge(v2, v4, "knows", HashMap::new()).unwrap();
        storage
            .add_edge(v4, v1, "knows", {
                let mut props = HashMap::new();
                props.insert("since".to_string(), Value::Int(2020));
                props
            })
            .unwrap();

        Graph::new(Arc::new(storage))
    }

    mod graph_traversal_source_tests {
        use super::*;

        #[test]
        fn new_creates_source() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            // Should be able to access references
            let _ = g.snapshot();
            let _ = g.interner();
        }

        #[test]
        fn v_creates_all_vertices_traversal() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let results = g.v().to_list();
            assert_eq!(results.len(), 4);

            // All should be vertices
            for value in &results {
                assert!(value.is_vertex());
            }
        }

        #[test]
        fn v_ids_creates_specific_vertices_traversal() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let results = g.v_ids([VertexId(0), VertexId(2)]).to_list();
            assert_eq!(results.len(), 2);

            let ids: Vec<VertexId> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
            assert!(ids.contains(&VertexId(0)));
            assert!(ids.contains(&VertexId(2)));
        }

        #[test]
        fn v_ids_filters_nonexistent() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let results = g.v_ids([VertexId(0), VertexId(999)]).to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn e_creates_all_edges_traversal() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let results = g.e().to_list();
            assert_eq!(results.len(), 5);

            // All should be edges
            for value in &results {
                assert!(value.is_edge());
            }
        }

        #[test]
        fn e_ids_creates_specific_edges_traversal() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let results = g.e_ids([EdgeId(0), EdgeId(1)]).to_list();
            assert_eq!(results.len(), 2);

            let ids: Vec<EdgeId> = results.iter().filter_map(|v| v.as_edge_id()).collect();
            assert!(ids.contains(&EdgeId(0)));
            assert!(ids.contains(&EdgeId(1)));
        }

        #[test]
        fn e_ids_filters_nonexistent() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let results = g.e_ids([EdgeId(0), EdgeId(999)]).to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].as_edge_id(), Some(EdgeId(0)));
        }

        #[test]
        fn inject_creates_value_traversal() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let results = g.inject([1i64, 2i64, 3i64]).to_list();
            assert_eq!(results.len(), 3);
            assert_eq!(results[0], Value::Int(1));
            assert_eq!(results[1], Value::Int(2));
            assert_eq!(results[2], Value::Int(3));
        }

        #[test]
        fn inject_with_mixed_types() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let values: Vec<Value> = vec![
                Value::Int(1),
                Value::String("hello".to_string()),
                Value::Bool(true),
            ];
            let results = g.inject(values).to_list();
            assert_eq!(results.len(), 3);
            assert_eq!(results[0], Value::Int(1));
            assert_eq!(results[1], Value::String("hello".to_string()));
            assert_eq!(results[2], Value::Bool(true));
        }

        #[test]
        fn empty_graph_returns_empty() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            assert_eq!(g.v().count(), 0);
            assert_eq!(g.e().count(), 0);
        }
    }

    mod bound_traversal_tests {
        use super::*;
        use crate::traversal::step::IdentityStep;

        #[test]
        fn add_step_chains_correctly() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let traversal: BoundTraversal<'_, (), Value> = g.v().add_step(IdentityStep::new());
            assert_eq!(traversal.step_count(), 1);
            assert_eq!(traversal.step_names(), vec!["identity"]);

            // Results should be unchanged by identity step
            let results = traversal.to_list();
            assert_eq!(results.len(), 4);
        }

        #[test]
        fn append_merges_traversals() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let anon: Traversal<Value, Value> =
                Traversal::<Value, Value>::new().add_step(IdentityStep::new());

            let traversal = g.v().append(anon);
            assert_eq!(traversal.step_count(), 1);

            let results = traversal.to_list();
            assert_eq!(results.len(), 4);
        }

        #[test]
        fn clone_creates_independent_copy() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let t1 = g.v();
            let t2 = t1.clone();

            // Both should produce same results
            let results1 = t1.to_list();
            let results2 = t2.to_list();
            assert_eq!(results1.len(), results2.len());
        }

        #[test]
        fn debug_format() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let traversal: BoundTraversal<'_, (), Value> = g.v().add_step(IdentityStep::new());
            let debug_str = format!("{:?}", traversal);
            assert!(debug_str.contains("BoundTraversal"));
            assert!(debug_str.contains("step_count"));
        }
    }

    mod terminal_step_tests {
        use super::*;

        #[test]
        fn to_list_collects_all() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let results = g.v().to_list();
            assert_eq!(results.len(), 4);
        }

        #[test]
        fn to_set_deduplicates() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let results = g.v().to_set();
            assert_eq!(results.len(), 4);
        }

        #[test]
        fn next_returns_first() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let result = g.v().next();
            assert!(result.is_some());
            assert!(result.unwrap().is_vertex());
        }

        #[test]
        fn next_returns_none_for_empty() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let result = g.v().next();
            assert!(result.is_none());
        }

        #[test]
        fn has_next_returns_true_for_nonempty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            assert!(g.v().has_next());
        }

        #[test]
        fn has_next_returns_false_for_empty() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            assert!(!g.v().has_next());
        }

        #[test]
        fn one_returns_single_value() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let result = g.v_ids([VertexId(0)]).one();
            assert!(result.is_ok());
            assert_eq!(result.unwrap().as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn one_errors_on_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let result = g.v_ids([VertexId(999)]).one();
            assert!(result.is_err());
            match result.unwrap_err() {
                crate::error::TraversalError::NotOne(n) => assert_eq!(n, 0),
                _ => panic!("Expected NotOne error"),
            }
        }

        #[test]
        fn one_errors_on_multiple() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let result = g.v().one();
            assert!(result.is_err());
            match result.unwrap_err() {
                crate::error::TraversalError::NotOne(n) => assert_eq!(n, 2),
                _ => panic!("Expected NotOne error"),
            }
        }

        #[test]
        fn iterate_consumes_without_collecting() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            // This should not panic or error
            g.v().iterate();
        }

        #[test]
        fn count_returns_correct_value() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            assert_eq!(g.v().count(), 4);
            assert_eq!(g.e().count(), 5);
        }

        #[test]
        fn count_returns_zero_for_empty() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            assert_eq!(g.v().count(), 0);
        }

        #[test]
        fn take_returns_first_n() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let results = g.v().take(2);
            assert_eq!(results.len(), 2);
        }

        #[test]
        fn take_returns_all_if_less_than_n() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let results = g.v().take(100);
            assert_eq!(results.len(), 4);
        }

        #[test]
        fn iter_produces_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let values: Vec<Value> = g.v().iter().collect();
            assert_eq!(values.len(), 4);
        }

        #[test]
        fn traversers_produces_traversers() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let traversers: Vec<Traverser> = g.v().traversers().collect();
            assert_eq!(traversers.len(), 4);

            // Each traverser should have a value
            for t in &traversers {
                assert!(t.is_vertex());
            }
        }

        #[test]
        fn fold_accumulates() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let count = g
                .inject([1i64, 2i64, 3i64])
                .fold(0i64, |acc, v| acc + v.as_i64().unwrap_or(0));
            assert_eq!(count, 6);
        }

        #[test]
        fn sum_adds_integers() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let result = g.inject([1i64, 2i64, 3i64]).sum();
            assert_eq!(result, Value::Int(6));
        }

        #[test]
        fn sum_handles_floats() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let values: Vec<Value> = vec![Value::Int(1), Value::Float(2.5), Value::Int(3)];
            let result = g.inject(values).sum();
            assert!(matches!(result, Value::Float(f) if (f - 6.5).abs() < 1e-10));
        }

        #[test]
        fn sum_empty_returns_zero() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let values: Vec<Value> = vec![];
            let result = g.inject(values).sum();
            assert_eq!(result, Value::Int(0));
        }

        #[test]
        fn min_finds_minimum() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let result = g.inject([3i64, 1i64, 2i64]).min();
            assert_eq!(result, Some(Value::Int(1)));
        }

        #[test]
        fn min_empty_returns_none() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let values: Vec<Value> = vec![];
            let result = g.inject(values).min();
            assert!(result.is_none());
        }

        #[test]
        fn max_finds_maximum() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let result = g.inject([3i64, 1i64, 2i64]).max();
            assert_eq!(result, Some(Value::Int(3)));
        }

        #[test]
        fn max_empty_returns_none() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let values: Vec<Value> = vec![];
            let result = g.inject(values).max();
            assert!(result.is_none());
        }
    }

    mod traversal_executor_tests {
        use super::*;

        #[test]
        fn executor_iterates_correctly() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let executor = g.v().execute();
            assert_eq!(executor.len(), 4);
            assert!(!executor.is_empty());

            let values: Vec<_> = executor.collect();
            assert_eq!(values.len(), 4);
        }

        #[test]
        fn executor_empty_for_empty_graph() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let executor = g.v().execute();
            assert_eq!(executor.len(), 0);
            assert!(executor.is_empty());
        }

        #[test]
        fn executor_size_hint_is_exact() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let executor = g.v().execute();
            let (lower, upper) = executor.size_hint();
            assert_eq!(lower, 4);
            assert_eq!(upper, Some(4));
        }
    }

    mod has_label_step_integration_tests {
        use super::*;

        #[test]
        fn has_label_filters_vertices() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let people = g.v().has_label("person").to_list();

            // Should return 3 person vertices (Alice, Bob, Charlie)
            assert_eq!(people.len(), 3);
            for v in &people {
                assert!(v.is_vertex());
            }
        }

        #[test]
        fn has_label_filters_to_software() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let software = g.v().has_label("software").to_list();

            // Should return 1 software vertex (Graph DB)
            assert_eq!(software.len(), 1);
        }

        #[test]
        fn has_label_returns_empty_for_nonexistent_label() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let unknown = g.v().has_label("unknown").to_list();
            assert!(unknown.is_empty());
        }

        #[test]
        fn has_label_filters_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let knows_edges = g.e().has_label("knows").to_list();

            // Should return 3 "knows" edges
            assert_eq!(knows_edges.len(), 3);
            for e in &knows_edges {
                assert!(e.is_edge());
            }
        }

        #[test]
        fn has_label_any_filters_multiple_labels() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let entities = g.v().has_label_any(["person", "software"]).to_list();

            // Should return 3 persons + 1 software = 4 vertices
            assert_eq!(entities.len(), 4);
        }

        #[test]
        fn has_label_any_works_with_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let edges = g.e().has_label_any(["knows", "uses"]).to_list();

            // Should return 3 "knows" + 2 "uses" = 5 edges
            assert_eq!(edges.len(), 5);
        }

        #[test]
        fn has_label_can_be_chained() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            // This shouldn't match anything since an element can't have two labels
            // (this tests that chaining works, even if the result is empty)
            let result = g.v().has_label("person").has_label("software").to_list();
            assert!(result.is_empty());
        }

        #[test]
        fn has_label_count_works() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            let count = g.v().has_label("person").count();
            assert_eq!(count, 3);
        }

        #[test]
        fn has_label_with_specific_vertex_ids() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            // Vertex IDs 0 and 1 are "person", vertex ID 2 is "software"
            let result = g
                .v_ids([VertexId(0), VertexId(2)])
                .has_label("person")
                .to_list();

            // Only vertex 0 should match
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].as_vertex_id(), Some(VertexId(0)));
        }
    }

    mod path_tracking_tests {
        use super::*;

        #[test]
        fn with_path_enables_path_tracking() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            // With path tracking enabled, paths should be recorded
            let traversers = g.v_ids([VertexId(0)]).with_path().out().traversers();
            for t in traversers {
                // Each traverser should have a path with 2 elements (start + out)
                assert_eq!(t.path.len(), 2);
            }
        }

        #[test]
        fn path_not_tracked_by_default() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            // Without with_path(), paths should be empty
            let traversers = g.v_ids([VertexId(0)]).out().traversers();
            for t in traversers {
                assert!(t.path.is_empty());
            }
        }

        #[test]
        fn path_step_returns_path_list() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            // Test path() step returns Value::List of path elements
            let paths = g.v_ids([VertexId(0)]).with_path().out().path().to_list();

            // Alice has 2 outgoing edges (knows->Bob, uses->GraphDB)
            assert_eq!(paths.len(), 2);

            // Each path should be a list
            for path in &paths {
                assert!(matches!(path, Value::List(_)));
                if let Value::List(elements) = path {
                    // 2 elements: starting vertex + destination vertex
                    assert_eq!(elements.len(), 2);
                }
            }
        }

        #[test]
        fn path_tracks_multi_hop_traversal() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            // Alice -> Bob -> Charlie or GraphDB (2 hops)
            let traversers = g
                .v_ids([VertexId(0)])
                .with_path()
                .out_labels(&["knows"])
                .out()
                .traversers();

            for t in traversers {
                // 3 elements: Alice -> Bob -> destination
                assert_eq!(t.path.len(), 3);
            }
        }

        #[test]
        fn as_step_labels_current_position() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            // Label Alice as "a"
            let traversers = g
                .v_ids([VertexId(0)])
                .as_("a")
                .out_labels(&["knows"])
                .traversers();

            for t in traversers {
                // Label should be stored even without path tracking
                assert!(t.path.has_label("a"));
            }
        }

        #[test]
        fn select_single_label_returns_value() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            // Label Alice as "a", then traverse to Bob, then select "a"
            let results = g
                .v_ids([VertexId(0)])
                .as_("a")
                .out_labels(&["knows"])
                .select_one("a")
                .to_list();

            // Should return Alice's vertex
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(0)));
        }

        #[test]
        fn select_multiple_labels_returns_map() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            // Label starting vertex as "a", destination as "b"
            let results = g
                .v_ids([VertexId(0)])
                .as_("a")
                .out_labels(&["knows"])
                .as_("b")
                .select(&["a", "b"])
                .to_list();

            assert_eq!(results.len(), 1);

            // Result should be a map
            match &results[0] {
                Value::Map(map) => {
                    assert!(map.contains_key("a"));
                    assert!(map.contains_key("b"));
                    assert_eq!(map.get("a"), Some(&Value::Vertex(VertexId(0))));
                    assert_eq!(map.get("b"), Some(&Value::Vertex(VertexId(1))));
                }
                _ => panic!("Expected Value::Map"),
            }
        }

        #[test]
        fn select_missing_label_filters_out() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            // Select a label that was never set
            let results = g
                .v_ids([VertexId(0)])
                .out()
                .select_one("nonexistent")
                .to_list();

            // Should be empty since no traverser has the label
            assert!(results.is_empty());
        }

        #[test]
        fn as_step_works_without_path_tracking() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            // as_() should work even without with_path() - it stores labels
            let results = g
                .v_ids([VertexId(0)])
                .as_("start")
                .out()
                .select_one("start")
                .to_list();

            // Should still be able to select the labeled value
            assert_eq!(results.len(), 2); // Alice has 2 outgoing edges
            for result in &results {
                assert_eq!(result, &Value::Vertex(VertexId(0)));
            }
        }

        #[test]
        fn path_with_edge_steps() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            // Traverse through edges with path tracking
            let traversers = g
                .v_ids([VertexId(0)])
                .with_path()
                .out_e()
                .in_v()
                .traversers();

            for t in traversers {
                // 3 elements: vertex -> edge -> vertex
                assert_eq!(t.path.len(), 3);
            }
        }

        #[test]
        fn with_path_is_opt_in() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            // Verify BoundTraversal starts with path tracking disabled
            let traversal = g.v();
            assert!(!traversal.is_tracking_paths());

            // After with_path(), it should be enabled
            let traversal = g.v().with_path();
            assert!(traversal.is_tracking_paths());
        }
    }
}
