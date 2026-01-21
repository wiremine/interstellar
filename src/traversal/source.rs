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
//! let g = snapshot.gremlin();
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

use crate::storage::interner::StringInterner;
use crate::storage::GraphStorage;
use crate::traversal::context::SnapshotLike;
use crate::traversal::step::{AnyStep, StartStep};
use crate::traversal::{ExecutionContext, Traversal, TraversalSource, Traverser};
use crate::value::{EdgeId, Value, VertexId};

// -----------------------------------------------------------------------------
// GraphTraversalSource - Entry point for bound traversals
// -----------------------------------------------------------------------------

/// Entry point for all bound traversals.
///
/// Created from a `GraphSnapshot` via `snapshot.gremlin()` or from any
/// type implementing `SnapshotLike` via `GraphTraversalSource::from_snapshot()`.
///
/// The source holds references needed to create an `ExecutionContext` at
/// execution time.
///
/// # Example
///
/// ```ignore
/// let graph = Graph::in_memory();
/// let snapshot = graph.snapshot();
/// let g = snapshot.gremlin();
///
/// // All methods return BoundTraversal which can be chained
/// let count = g.v().count();
/// ```
pub struct GraphTraversalSource<'g> {
    storage: &'g dyn GraphStorage,
    interner: &'g StringInterner,
}

impl<'g> GraphTraversalSource<'g> {
    /// Create a new traversal source from any type implementing `SnapshotLike`.
    ///
    /// This allows traversals to work with `GraphSnapshot`, `CowMmapSnapshot`,
    /// or any other snapshot type that implements the trait.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let graph = Graph::new();
    /// let snapshot = graph.snapshot();
    /// let g = GraphTraversalSource::from_snapshot(&snapshot);
    /// let count = g.v().count();
    /// ```
    pub fn from_snapshot<S: SnapshotLike + ?Sized>(snapshot: &'g S) -> Self {
        Self {
            storage: snapshot.storage(),
            interner: snapshot.interner(),
        }
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
            self.storage,
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
            self.storage,
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
            self.storage,
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
            self.storage,
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
            self.storage,
            self.interner,
            Traversal::with_source(TraversalSource::Inject(values)),
        )
    }

    /// Get the underlying graph storage reference.
    #[inline]
    pub fn storage(&self) -> &'g dyn GraphStorage {
        self.storage
    }

    /// Get the interner reference.
    #[inline]
    pub fn interner(&self) -> &'g StringInterner {
        self.interner
    }

    // -------------------------------------------------------------------------
    // Index-aware source steps
    // -------------------------------------------------------------------------

    /// Start traversal from vertices matching a property value.
    ///
    /// This method uses property indexes when available for O(log n) or O(1)
    /// lookups instead of O(n) full scans. If no applicable index exists,
    /// it falls back to a full scan with filtering.
    ///
    /// # Arguments
    ///
    /// * `label` - Optional label filter (None matches all labels)
    /// * `property` - The property key to match
    /// * `value` - The property value to find
    ///
    /// # Performance
    ///
    /// - With index: O(log n) for BTree, O(1) for Unique index
    /// - Without index: O(n) full scan
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Find all users with email "alice@example.com" (uses unique index if available)
    /// let alice = g.v_by_property(Some("user"), "email", "alice@example.com").next();
    ///
    /// // Find all vertices with age 30 (any label)
    /// let age_30 = g.v_by_property(None, "age", 30i64).to_list();
    /// ```
    pub fn v_by_property(
        &self,
        label: Option<&str>,
        property: &str,
        value: impl Into<Value>,
    ) -> BoundTraversal<'g, (), Value> {
        let value = value.into();
        // Use the storage's indexed lookup method
        let vertex_ids: Vec<VertexId> = self
            .storage
            .vertices_by_property(label, property, &value)
            .map(|v| v.id)
            .collect();

        BoundTraversal::new(
            self.storage,
            self.interner,
            Traversal::with_source(TraversalSource::Vertices(vertex_ids)),
        )
    }

    /// Start traversal from vertices matching a property range.
    ///
    /// This method uses BTree indexes when available for O(log n) range lookups.
    /// If no applicable index exists, it falls back to a full scan with filtering.
    ///
    /// # Arguments
    ///
    /// * `label` - Optional label filter (None matches all labels)
    /// * `property` - The property key to match
    /// * `start` - Start bound of the range
    /// * `end` - End bound of the range
    ///
    /// # Performance
    ///
    /// - With BTree index: O(log n) + O(k) where k is result count
    /// - Without index: O(n) full scan
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::ops::Bound;
    ///
    /// // Find people aged 25-35 (inclusive)
    /// let young_adults = g.v_by_property_range(
    ///     Some("person"),
    ///     "age",
    ///     Bound::Included(&Value::Int(25)),
    ///     Bound::Included(&Value::Int(35)),
    /// ).to_list();
    ///
    /// // Find events after timestamp 1000 (unbounded end)
    /// let recent = g.v_by_property_range(
    ///     Some("event"),
    ///     "timestamp",
    ///     Bound::Excluded(&Value::Int(1000)),
    ///     Bound::Unbounded,
    /// ).to_list();
    /// ```
    pub fn v_by_property_range(
        &self,
        label: Option<&str>,
        property: &str,
        start: std::ops::Bound<&Value>,
        end: std::ops::Bound<&Value>,
    ) -> BoundTraversal<'g, (), Value> {
        // Use the storage's indexed range lookup method
        let vertex_ids: Vec<VertexId> = self
            .storage
            .vertices_by_property_range(label, property, start, end)
            .map(|v| v.id)
            .collect();

        BoundTraversal::new(
            self.storage,
            self.interner,
            Traversal::with_source(TraversalSource::Vertices(vertex_ids)),
        )
    }

    /// Start traversal from edges matching a property value.
    ///
    /// This method uses property indexes when available for O(log n) or O(1)
    /// lookups instead of O(n) full scans. If no applicable index exists,
    /// it falls back to a full scan with filtering.
    ///
    /// # Arguments
    ///
    /// * `label` - Optional label filter (None matches all labels)
    /// * `property` - The property key to match
    /// * `value` - The property value to find
    ///
    /// # Performance
    ///
    /// - With index: O(log n) for BTree, O(1) for Unique index
    /// - Without index: O(n) full scan
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Find all "purchased" edges with amount 100
    /// let purchases = g.e_by_property(Some("purchased"), "amount", 100i64).to_list();
    ///
    /// // Find all edges with weight 1.0 (any label)
    /// let weight_one = g.e_by_property(None, "weight", 1.0f64).to_list();
    /// ```
    pub fn e_by_property(
        &self,
        label: Option<&str>,
        property: &str,
        value: impl Into<Value>,
    ) -> BoundTraversal<'g, (), Value> {
        let value = value.into();
        // Use the storage's indexed lookup method
        let edge_ids: Vec<EdgeId> = self
            .storage
            .edges_by_property(label, property, &value)
            .map(|e| e.id)
            .collect();

        BoundTraversal::new(
            self.storage,
            self.interner,
            Traversal::with_source(TraversalSource::Edges(edge_ids)),
        )
    }

    // -------------------------------------------------------------------------
    // Mutation steps (spawning traversals)
    // -------------------------------------------------------------------------

    /// Start a traversal that creates a new vertex.
    ///
    /// This is a **spawning step** - it produces a traverser for the newly
    /// created vertex. The actual vertex creation happens when a terminal
    /// step is called.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create a new person vertex
    /// let vertex = g.add_v("person")
    ///     .property("name", "Alice")
    ///     .property("age", 30)
    ///     .next();
    /// ```
    pub fn add_v(&self, label: impl Into<String>) -> BoundTraversal<'g, (), Value> {
        use crate::traversal::mutation::AddVStep;

        // Create a traversal that starts with add_v step
        let mut traversal = Traversal::<(), Value>::with_source(TraversalSource::Inject(vec![]));
        traversal = traversal.add_step(AddVStep::new(label));
        BoundTraversal::new(self.storage, self.interner, traversal)
    }

    /// Start a traversal that creates a new edge.
    ///
    /// This is a **spawning step** - it produces a traverser for the newly
    /// created edge. Both `from` and `to` endpoints must be specified before
    /// the terminal step is called.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an edge between two vertices
    /// let edge = g.add_e("knows")
    ///     .from_vertex(VertexId(1))
    ///     .to_vertex(VertexId(2))
    ///     .property("since", 2020)
    ///     .next();
    /// ```
    pub fn add_e(&self, label: impl Into<String>) -> AddEdgeBuilder<'g> {
        AddEdgeBuilder::new(self.storage, self.interner, label.into())
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
/// - `'g`: The lifetime of the graph storage/snapshot
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
    storage: &'g dyn GraphStorage,
    interner: &'g StringInterner,
    traversal: Traversal<In, Out>,
    /// Whether to automatically track paths for navigation steps
    track_paths: bool,
}

impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Create a new bound traversal.
    pub(crate) fn new(
        storage: &'g dyn GraphStorage,
        interner: &'g StringInterner,
        traversal: Traversal<In, Out>,
    ) -> Self {
        Self {
            storage,
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
            storage: self.storage,
            interner: self.interner,
            traversal: self.traversal.add_step(step),
            track_paths: self.track_paths,
        }
    }

    /// Append an anonymous traversal's steps to this traversal.
    ///
    /// This is used to merge anonymous traversals (created with `__.`)
    /// into bound traversals.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = __.out().has_label("person");
    /// let results = g.v().append(anon).to_list();
    /// ```
    pub fn append<Mid>(self, anon: Traversal<Out, Mid>) -> BoundTraversal<'g, In, Mid> {
        BoundTraversal {
            storage: self.storage,
            interner: self.interner,
            traversal: self.traversal.append(anon),
            track_paths: self.track_paths,
        }
    }

    /// Create an execution context for this traversal.
    #[allow(dead_code)] // Will be used in future phases for lazy execution
    fn create_context(&self) -> ExecutionContext<'g> {
        ExecutionContext::new(self.storage, self.interner)
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
            self.storage,
            self.interner,
            self.traversal,
            self.track_paths,
        )
    }

    /// Get the underlying graph storage reference.
    #[inline]
    pub fn storage(&self) -> &'g dyn GraphStorage {
        self.storage
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

    /// Filter elements by property absence.
    ///
    /// Keeps only vertices/edges that do NOT have the specified property.
    /// Non-element values (integers, strings, etc.) pass through since they
    /// don't have properties.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get all vertices that do NOT have an "email" property
    /// let without_email = g.v().has_not("email").to_list();
    /// ```
    pub fn has_not(self, key: impl Into<String>) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::HasNotStep;
        self.add_step(HasNotStep::new(key))
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
    /// use interstellar::traversal::p;
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

    /// Deduplicate traversers by property value.
    ///
    /// Removes duplicates based on a property value extracted from elements.
    /// Only the first occurrence of each unique property value passes through.
    /// Elements without the property use `Null` as the key.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to use for deduplication
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Keep only one person per age
    /// let unique_ages = g.v().has_label("person").dedup_by_key("age").to_list();
    ///
    /// // Keep only one edge per weight
    /// let unique_weights = g.e().dedup_by_key("weight").to_list();
    /// ```
    pub fn dedup_by_key(self, key: impl Into<String>) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::DedupByKeyStep;
        self.add_step(DedupByKeyStep::new(key))
    }

    /// Deduplicate traversers by element label.
    ///
    /// Removes duplicates based on element label. Only the first occurrence
    /// of each unique label passes through.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Keep only one vertex per label type
    /// let one_per_label = g.v().dedup_by_label().to_list();
    /// ```
    pub fn dedup_by_label(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::DedupByLabelStep;
        self.add_step(DedupByLabelStep::new())
    }

    /// Deduplicate traversers by sub-traversal result.
    ///
    /// Executes the given sub-traversal for each element and uses the first
    /// result as the deduplication key. If the sub-traversal produces no
    /// results, `Null` is used as the key.
    ///
    /// # Arguments
    ///
    /// * `sub` - The sub-traversal to execute for each element
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Keep only one vertex per out-degree
    /// let unique_outdegree = g.v()
    ///     .dedup_by(__.out().count())
    ///     .to_list();
    ///
    /// // Keep one person per first friend's name
    /// let unique_friend = g.v()
    ///     .has_label("person")
    ///     .dedup_by(__.out_labels(&["knows"]).limit(1).values("name"))
    ///     .to_list();
    /// ```
    pub fn dedup_by(
        self,
        sub: crate::traversal::Traversal<crate::value::Value, crate::value::Value>,
    ) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::DedupByTraversalStep;
        self.add_step(DedupByTraversalStep::new(sub))
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

    /// Filter the current value using a predicate.
    ///
    /// Unlike `has_where()` which filters by property, `is_()` tests the
    /// traverser's current value directly. Commonly used after `values()`
    /// to filter property values.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::traversal::p;
    ///
    /// // Filter to ages greater than 25
    /// let older = g.v().values("age").is_(p::gt(25)).to_list();
    ///
    /// // Filter using between predicate
    /// let working_age = g.v().values("age").is_(p::between(25, 65)).to_list();
    /// ```
    pub fn is_(
        self,
        predicate: impl crate::traversal::predicate::Predicate + 'static,
    ) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::IsStep;
        self.add_step(IsStep::new(predicate))
    }

    /// Filter the current value by equality.
    ///
    /// Convenience method for `is_(p::eq(value))`. Tests the traverser's
    /// current value for equality with the given value.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Filter to age exactly 30
    /// let age_30 = g.v().values("age").is_eq(30i64).to_list();
    ///
    /// // Filter to specific name
    /// let alice = g.v().values("name").is_eq("Alice").to_list();
    /// ```
    pub fn is_eq(self, value: impl Into<Value>) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::IsStep;
        self.add_step(IsStep::eq(value))
    }

    /// Filter to traversers with simple (non-cyclic) paths.
    ///
    /// A simple path contains no repeated elements. This is useful when
    /// traversing graphs to avoid cycles.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Find all simple paths from a vertex
    /// let simple = g.v()
    ///     .has("name", "marko")
    ///     .repeat(__.both())
    ///     .times(3)
    ///     .simple_path()
    ///     .path()
    ///     .to_list();
    /// ```
    pub fn simple_path(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::SimplePathStep;
        self.add_step(SimplePathStep::new())
    }

    /// Filter to traversers with cyclic paths.
    ///
    /// A cyclic path contains at least one repeated element. This is the
    /// inverse of `simple_path()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Find all paths that contain cycles
    /// let cycles = g.v()
    ///     .repeat(__.both())
    ///     .times(4)
    ///     .cyclic_path()
    ///     .path()
    ///     .to_list();
    /// ```
    pub fn cyclic_path(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::CyclicPathStep;
        self.add_step(CyclicPathStep::new())
    }

    /// Return only the last element from the traversal.
    ///
    /// This is a **barrier step** - it must collect all elements to determine
    /// which is the last. Equivalent to `tail_n(1)`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get the last vertex
    /// let last = g.v().tail().to_list();
    /// ```
    pub fn tail(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::TailStep;
        self.add_step(TailStep::last())
    }

    /// Return only the last n elements from the traversal.
    ///
    /// This is a **barrier step** - it must collect all elements to determine
    /// which are the last n. Elements are returned in their original order.
    ///
    /// # Arguments
    ///
    /// * `count` - Number of elements to return from the end
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get the last 5 vertices
    /// let last_five = g.v().tail_n(5).to_list();
    /// ```
    pub fn tail_n(self, count: usize) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::TailStep;
        self.add_step(TailStep::new(count))
    }

    /// Probabilistic filter using random coin flip.
    ///
    /// Each traverser has a probability `p` of passing through. Useful for
    /// random sampling or probabilistic traversals.
    ///
    /// # Arguments
    ///
    /// * `probability` - Probability of passing (0.0 to 1.0, clamped)
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Randomly sample approximately 10% of vertices
    /// let sample = g.v().coin(0.1).to_list();
    ///
    /// // Probabilistic filtering in a traversal
    /// let random_friends = g.v()
    ///     .has_label("person")
    ///     .out_labels(&["knows"])
    ///     .coin(0.5)
    ///     .to_list();
    /// ```
    ///
    /// # Note
    ///
    /// Results are non-deterministic. For reproducible results in tests,
    /// use statistical tolerances.
    pub fn coin(self, probability: f64) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::CoinStep;
        self.add_step(CoinStep::new(probability))
    }

    /// Randomly sample n elements using reservoir sampling.
    ///
    /// This is a **barrier step** that collects all input elements and returns
    /// a random sample of exactly n elements. If the input has fewer than n
    /// elements, all elements are returned.
    ///
    /// Uses reservoir sampling algorithm to ensure each element has equal
    /// probability of being selected, regardless of total input size.
    ///
    /// # Arguments
    ///
    /// * `count` - The number of elements to sample
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Sample 5 random vertices
    /// let sampled = g.v().sample(5).to_list();
    ///
    /// // Combined with filter
    /// let sampled_people = g.v().has_label("person").sample(3).to_list();
    /// ```
    ///
    /// # Note
    ///
    /// Results are non-deterministic. For reproducible results in tests,
    /// use statistical tolerances.
    pub fn sample(self, count: usize) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::SampleStep;
        self.add_step(SampleStep::new(count))
    }

    /// Filter property objects by key name.
    ///
    /// This step filters property maps (from `properties()`) to keep only those
    /// with a matching "key" field.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to filter for
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get only "name" properties
    /// let names = g.v().properties().has_key("name").to_list();
    /// ```
    pub fn has_key(self, key: impl Into<String>) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::HasKeyStep;
        self.add_step(HasKeyStep::new(key))
    }

    /// Filter property objects by any of the specified key names.
    ///
    /// This step filters property maps (from `properties()`) to keep only those
    /// with a "key" field matching any of the specified keys.
    ///
    /// # Arguments
    ///
    /// * `keys` - The property keys to filter for
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get "name" or "age" properties
    /// let props = g.v().properties().has_key_any(["name", "age"]).to_list();
    /// ```
    pub fn has_key_any<I, S>(self, keys: I) -> BoundTraversal<'g, In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        use crate::traversal::filter::HasKeyStep;
        self.add_step(HasKeyStep::any(keys))
    }

    /// Filter property objects by value.
    ///
    /// This step filters property maps (from `properties()`) to keep only those
    /// with a matching "value" field.
    ///
    /// # Arguments
    ///
    /// * `value` - The property value to filter for
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get properties with value "Alice"
    /// let alice_props = g.v().properties().has_prop_value("Alice").to_list();
    /// ```
    pub fn has_prop_value(self, value: impl Into<Value>) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::HasPropValueStep;
        self.add_step(HasPropValueStep::new(value))
    }

    /// Filter property objects by any of the specified values.
    ///
    /// This step filters property maps (from `properties()`) to keep only those
    /// with a "value" field matching any of the specified values.
    ///
    /// # Arguments
    ///
    /// * `values` - The property values to filter for
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get properties with value "Alice" or "Bob"
    /// let props = g.v().properties().has_prop_value_any(["Alice", "Bob"]).to_list();
    /// ```
    pub fn has_prop_value_any<I, V>(self, values: I) -> BoundTraversal<'g, In, Value>
    where
        I: IntoIterator<Item = V>,
        V: Into<Value>,
    {
        use crate::traversal::filter::HasPropValueStep;
        self.add_step(HasPropValueStep::any(values))
    }

    /// Filter traversers by testing their current value against a predicate.
    ///
    /// This step is the predicate-based variant of `where()`, complementing the
    /// traversal-based `where_(traversal)` step. It tests the current traverser
    /// value directly against the predicate.
    ///
    /// # Arguments
    ///
    /// * `predicate` - The predicate to test values against
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::traversal::p;
    ///
    /// // Filter values greater than 25
    /// let adults = g.v().values("age").where_p(p::gt(25)).to_list();
    ///
    /// // Filter values within a set
    /// let selected = g.v().values("name").where_p(p::within(["Alice", "Bob"])).to_list();
    ///
    /// // Combined predicates
    /// let range = g.v().values("age").where_p(p::and(p::gte(18), p::lt(65))).to_list();
    /// ```
    pub fn where_p(
        self,
        predicate: impl crate::traversal::predicate::Predicate + 'static,
    ) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::filter::WherePStep;
        self.add_step(WherePStep::new(predicate))
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

    /// Get the "other" vertex of an edge.
    ///
    /// When traversing from a vertex to an edge, `other_v()` returns the
    /// vertex at the opposite end from where the traverser came from.
    /// This requires path tracking to be enabled (via `.with_path()`).
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Navigate: vertex -> outgoing edges -> other vertex (the target)
    /// let others = g.with_path().v().has("name", "marko").out_e("knows").other_v().to_list();
    /// ```
    pub fn other_v(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::navigation::OtherVStep;
        self.add_step(OtherVStep::new())
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

    /// Extract the key from property map objects.
    ///
    /// For each input property map (from `properties()` step), extracts the "key" field.
    /// Non-map values and maps without a "key" field are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get all property keys for person vertices
    /// let keys = g.v().has_label("person").properties().key().to_list();
    ///
    /// // Get unique property keys
    /// let unique_keys = g.v().properties().key().dedup().to_list();
    /// ```
    pub fn key(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::KeyStep;
        self.add_step(KeyStep::new())
    }

    /// Extract the value from property map objects.
    ///
    /// For each input property map (from `properties()` step), extracts the "value" field.
    /// Non-map values and maps without a "value" field are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get all property values for person vertices
    /// let values = g.v().has_label("person").properties().value().to_list();
    ///
    /// // Get property values for specific keys
    /// let ages = g.v().properties_keys(&["age"]).value().to_list();
    /// ```
    pub fn value(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::ValueStep;
        self.add_step(ValueStep::new())
    }

    /// Extract the current loop depth from traversers.
    ///
    /// Returns the loop count stored in each traverser as `Value::Int`.
    /// Outside of a repeat loop, this returns 0.
    ///
    /// # Note
    ///
    /// Uses 0-based indexing (first iteration = 0), which differs from
    /// Gremlin's 1-based indexing.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get loop depth at each emit
    /// let depths = g.v()
    ///     .has_label("person")
    ///     .repeat(__.out())
    ///     .times(3)
    ///     .emit()
    ///     .loops()
    ///     .to_list();
    ///
    /// // Use in until condition
    /// let vertices = g.v()
    ///     .repeat(__.out())
    ///     .until(__.loops().is_(p::gte(3)))
    ///     .to_list();
    /// ```
    pub fn loops(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::LoopsStep;
        self.add_step(LoopsStep::new())
    }

    /// Annotate each element with its position index in the stream.
    ///
    /// Returns a `[value, index]` list for each input element, where index
    /// is the 0-based position in the stream.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get elements with their indices
    /// let indexed = g.v()
    ///     .index()
    ///     .to_list();
    /// // Returns: [[v[0], 0], [v[1], 1], [v[2], 2], ...]
    ///
    /// // Get names with indices
    /// let indexed_names = g.v()
    ///     .values("name")
    ///     .index()
    ///     .to_list();
    /// // Returns: [["Alice", 0], ["Bob", 1], ...]
    /// ```
    pub fn index(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::IndexStep;
        self.add_step(IndexStep::new())
    }

    /// Extract all properties from vertices/edges.
    ///
    /// For each input element, extracts all properties as Maps containing
    /// "key" and "value" entries. Non-element values produce no output.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let props = g.v().has_label("person").properties().to_list();
    /// // Results: [Map{"key": "name", "value": "marko"}, Map{"key": "age", "value": 29}, ...]
    /// ```
    pub fn properties(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::PropertiesStep;
        self.add_step(PropertiesStep::new())
    }

    /// Extract specific properties from vertices/edges.
    ///
    /// For each input element, extracts only the specified properties as Maps
    /// containing "key" and "value" entries. Missing properties are skipped.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let props = g.v().properties_keys(&["name", "age"]).to_list();
    /// // Results: [Map{"key": "name", "value": "marko"}, Map{"key": "age", "value": 29}, ...]
    /// ```
    pub fn properties_keys<I, S>(self, keys: I) -> BoundTraversal<'g, In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        use crate::traversal::transform::PropertiesStep;
        self.add_step(PropertiesStep::from_keys(keys))
    }

    /// Get all properties as a map with list-wrapped values.
    ///
    /// Transforms each element into a `Value::Map` containing all properties.
    /// Property values are wrapped in `Value::List` for multi-property compatibility
    /// (following Gremlin semantics).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let maps = g.v().has_label("person").value_map().to_list();
    /// // Returns: [{"name": ["Alice"], "age": [30]}, {"name": ["Bob"]}]
    /// ```
    pub fn value_map(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::ValueMapStep;
        self.add_step(ValueMapStep::new())
    }

    /// Get specific properties as a map with list-wrapped values.
    ///
    /// Transforms each element into a `Value::Map` containing only the
    /// specified properties. Property values are wrapped in `Value::List`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let maps = g.v().value_map_keys(&["name"]).to_list();
    /// // Returns: [{"name": ["Alice"]}, {"name": ["Bob"]}]
    /// ```
    pub fn value_map_keys<I, S>(self, keys: I) -> BoundTraversal<'g, In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        use crate::traversal::transform::ValueMapStep;
        self.add_step(ValueMapStep::from_keys(keys))
    }

    /// Get all properties as a map including id and label tokens.
    ///
    /// Like `value_map()`, but also includes "id" and "label" entries.
    /// The id and label are NOT wrapped in lists, but property values are.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let maps = g.v().value_map_with_tokens().to_list();
    /// // Returns: [{"id": 0, "label": "person", "name": ["Alice"], "age": [30]}]
    /// ```
    pub fn value_map_with_tokens(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::ValueMapStep;
        self.add_step(ValueMapStep::new().with_tokens())
    }

    /// Get complete element representation as a map.
    ///
    /// Transforms each element into a `Value::Map` with id, label, and all
    /// properties. Unlike `value_map()`, property values are NOT wrapped in lists.
    /// For edges, also includes "IN" and "OUT" vertex references.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Vertex representation
    /// let maps = g.v().element_map().to_list();
    /// // Returns: [{"id": 0, "label": "person", "name": "Alice", "age": 30}]
    ///
    /// // Edge representation
    /// let edges = g.e().element_map().to_list();
    /// // Returns: [{"id": 0, "label": "knows", "IN": {"id": 1, "label": "person"},
    /// //           "OUT": {"id": 0, "label": "person"}, "since": 2020}]
    /// ```
    pub fn element_map(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::ElementMapStep;
        self.add_step(ElementMapStep::new())
    }

    /// Get element representation with specific properties.
    ///
    /// Like `element_map()`, but includes only the specified properties
    /// along with the id, label, and (for edges) IN/OUT references.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let maps = g.v().element_map_keys(&["name"]).to_list();
    /// // Returns: [{"id": 0, "label": "person", "name": "Alice"}]
    /// ```
    pub fn element_map_keys<I, S>(self, keys: I) -> BoundTraversal<'g, In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        use crate::traversal::transform::ElementMapStep;
        self.add_step(ElementMapStep::from_keys(keys))
    }

    /// Get all properties as a map of property objects.
    ///
    /// Transforms each element into a `Value::Map` where keys are property names
    /// and values are lists of property objects (maps with "key" and "value" entries).
    ///
    /// # Difference from valueMap
    ///
    /// - `value_map()`: Returns `{name: ["Alice"], age: [30]}` (just values in lists)
    /// - `property_map()`: Returns `{name: [{key: "name", value: "Alice"}], age: [{key: "age", value: 30}]}` (property objects in lists)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let maps = g.v().property_map().to_list();
    /// // Returns: [{name: [{key: "name", value: "Alice"}], age: [{key: "age", value: 30}]}]
    /// ```
    pub fn property_map(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::PropertyMapStep;
        self.add_step(PropertyMapStep::new())
    }

    /// Get specific properties as a map of property objects.
    ///
    /// Like `property_map()`, but includes only the specified properties.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let maps = g.v().property_map_keys(&["name"]).to_list();
    /// // Returns: [{name: [{key: "name", value: "Alice"}]}]
    /// ```
    pub fn property_map_keys<I, S>(self, keys: I) -> BoundTraversal<'g, In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        use crate::traversal::transform::PropertyMapStep;
        self.add_step(PropertyMapStep::from_keys(keys))
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

    /// Unroll collections into individual elements.
    ///
    /// This is a flatMap operation that:
    /// - Expands `Value::List` items into separate traversers (one per element)
    /// - Expands `Value::Map` items into separate single-entry maps (one per key-value pair)
    /// - Passes through non-collection values unchanged
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Unfold a folded list back to individual vertices
    /// let vertices = g.v().fold().unfold().to_list();
    ///
    /// // Unfold map entries
    /// let entries = g.v().value_map().unfold().to_list();
    /// // Each entry is a single-key map like {"name": ["Alice"]}
    /// ```
    pub fn unfold(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::UnfoldStep;
        self.add_step(UnfoldStep::new())
    }

    /// Calculate the arithmetic mean of numeric values.
    ///
    /// This is a **barrier step** - it collects ALL input before calculating.
    /// Only numeric values (`Value::Integer`, `Value::Float`) are included
    /// in the calculation. Non-numeric values are ignored.
    ///
    /// Returns a single `Value::Float` with the mean, or no traversers if
    /// there are no numeric values.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Calculate average age
    /// let avg_age = g.v().values("age").mean().next();
    /// // Returns: Some(Value::Float(29.5))
    ///
    /// // Mean of mixed types (non-numeric ignored)
    /// let avg = g.inject([1i64, 2i64, "skip", 3i64]).mean().next();
    /// // Returns: Some(Value::Float(2.0))
    /// ```
    pub fn mean(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::MeanStep;
        self.add_step(MeanStep::new())
    }

    /// Sort traversers using a fluent builder.
    ///
    /// This is a **barrier step** - it collects ALL input before producing sorted output.
    /// Returns a `BoundOrderBuilder` that allows chaining multiple sort keys using `by` methods.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Sort by natural order ascending
    /// let sorted = g.v().values("name").order().build().to_list();
    ///
    /// // Sort by property descending
    /// let sorted = g.v().has_label("person")
    ///     .order().by_key_desc("age").build()
    ///     .to_list();
    ///
    /// // Multi-level sort
    /// let sorted = g.v().has_label("person")
    ///     .order()
    ///     .by_key_desc("age")
    ///     .by_key_asc("name")
    ///     .build()
    ///     .to_list();
    /// ```
    pub fn order(self) -> crate::traversal::transform::BoundOrderBuilder<'g, In> {
        use crate::traversal::transform::BoundOrderBuilder;

        // Extract the steps and source from the traversal
        let track_paths = self.track_paths;
        let (source, steps) = self.traversal.into_steps();

        // Create and return the builder with graph references
        BoundOrderBuilder::new(self.storage, self.interner, source, steps, track_paths)
    }

    /// Evaluate a mathematical expression.
    ///
    /// The expression can reference the current value using `_` and labeled
    /// path values using their label names. Use `by()` to specify which
    /// property to extract from labeled elements.
    ///
    /// Uses the `mathexpr` crate for full expression parsing and evaluation,
    /// supporting:
    /// - Operators: `+`, `-`, `*`, `/`, `%`, `^`
    /// - Functions: `sqrt`, `abs`, `sin`, `cos`, `tan`, `log`, `exp`, `pow`, `min`, `max`, etc.
    /// - Constants: `pi`, `e`
    /// - Parentheses for grouping
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Double the current value
    /// let doubled = g.v().values("age").math("_ * 2").build().to_list();
    ///
    /// // Calculate age difference between labeled vertices
    /// let diff = g.v().as_("a").out("knows").as_("b")
    ///     .math("a - b")
    ///     .by("a", "age")
    ///     .by("b", "age")
    ///     .build()
    ///     .to_list();
    ///
    /// // Complex expression with functions
    /// let sqrt = g.v().values("x").math("sqrt(_ ^ 2 + 1)").build().to_list();
    /// ```
    pub fn math(self, expression: &str) -> crate::traversal::transform::BoundMathBuilder<'g, In> {
        use crate::traversal::transform::BoundMathBuilder;

        // Extract the steps and source from the traversal
        let track_paths = self.track_paths;
        let (source, steps) = self.traversal.into_steps();

        // Create and return the builder with graph references
        BoundMathBuilder::new(
            self.storage,
            self.interner,
            source,
            steps,
            expression,
            track_paths,
        )
    }

    /// Create a projection with named keys.
    ///
    /// The `project()` step creates a map with specific named keys. Each key's value
    /// is defined by a `by()` modulator, which can extract a property or execute
    /// a sub-traversal.
    ///
    /// # Gremlin Equivalent
    ///
    /// ```groovy
    /// g.V().hasLabel('person')
    ///     .project('name', 'age', 'friends')
    ///     .by('name')
    ///     .by('age')
    ///     .by(out('knows').count())
    /// ```
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::traversal::__;
    ///
    /// let results = g.v().has_label("person")
    ///     .project(&["name", "friend_count"])
    ///     .by_key("name")
    ///     .by(__.out("knows").count())
    ///     .build()
    ///     .to_list();
    /// // Results: [{name: "Alice", friend_count: 2}, ...]
    /// ```
    ///
    /// # Arguments
    ///
    /// * `keys` - The keys for the projection map
    ///
    /// # Returns
    ///
    /// A `BoundProjectBuilder` that requires `by()` clauses to be added for each key.
    pub fn project(
        self,
        keys: &[&str],
    ) -> crate::traversal::transform::BoundProjectBuilder<'g, In> {
        use crate::traversal::transform::BoundProjectBuilder;

        // Extract the steps and source from the traversal
        let track_paths = self.track_paths;
        let (source, steps) = self.traversal.into_steps();

        // Convert keys to strings
        let key_strings: Vec<String> = keys.iter().map(|k| k.to_string()).collect();

        // Create and return the builder with graph references
        BoundProjectBuilder::new(
            self.storage,
            self.interner,
            source,
            steps,
            key_strings,
            track_paths,
        )
    }

    /// Group traversers by a key and collect values.
    ///
    /// The `group()` step is a **barrier step** that collects all input traversers,
    /// groups them by a key, and produces a single `Value::Map` output where:
    /// - Keys are the grouping keys (converted to strings)
    /// - Values are lists of collected values for each group
    ///
    /// # Gremlin Equivalent
    ///
    /// ```groovy
    /// g.V().group().by(label)  // Group by label
    /// g.V().group().by("age").by("name")  // Group by age, collect names
    /// g.V().group().by(label).by(out().count())  // Group by label, count outgoing
    /// ```
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::traversal::__;
    ///
    /// // Group vertices by label
    /// let groups = g.v()
    ///     .group().by_label().by_value().build()
    ///     .next();
    /// // Returns: Map { "person" -> [v1, v2], "software" -> [v3] }
    ///
    /// // Group by property, collect other property
    /// let groups = g.v().has_label("person")
    ///     .group().by_key("age").by_value_key("name").build()
    ///     .next();
    /// // Returns: Map { "29" -> ["Alice", "Bob"], "30" -> ["Charlie"] }
    /// ```
    ///
    /// # Returns
    ///
    /// A `BoundGroupBuilder` that allows configuring the grouping key and value collector.
    pub fn group(self) -> crate::traversal::aggregate::BoundGroupBuilder<'g, In> {
        use crate::traversal::aggregate::BoundGroupBuilder;

        // Extract the steps and source from the traversal
        let track_paths = self.track_paths;
        let (source, steps) = self.traversal.into_steps();

        // Create and return the builder with graph references
        BoundGroupBuilder::new(self.storage, self.interner, source, steps, track_paths)
    }

    /// Count traversers grouped by a key.
    ///
    /// Creates a `BoundGroupCountBuilder` that allows configuring how to group and count
    /// traversers. The result is a single `Value::Map` where keys are the grouping keys
    /// and values are integer counts (respecting traverser bulk).
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Count vertices by label
    /// let label_counts = g.v().group_count().by_label().build().next();
    /// // Returns: Map { "person" -> 3, "software" -> 1 }
    ///
    /// // Count vertices by property
    /// let age_counts = g.v().has_label("person")
    ///     .group_count().by_key("age").build()
    ///     .next();
    /// // Returns: Map { "29" -> 2, "30" -> 1 }
    /// ```
    ///
    /// # Returns
    ///
    /// A `BoundGroupCountBuilder` that allows configuring the grouping key.
    pub fn group_count(self) -> crate::traversal::aggregate::BoundGroupCountBuilder<'g, In> {
        use crate::traversal::aggregate::BoundGroupCountBuilder;

        // Extract the steps and source from the traversal
        let track_paths = self.track_paths;
        let (source, steps) = self.traversal.into_steps();

        // Create and return the builder with graph references
        BoundGroupCountBuilder::new(self.storage, self.interner, source, steps, track_paths)
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
    /// use interstellar::traversal::__;
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
    /// use interstellar::traversal::__;
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
    /// use interstellar::traversal::__;
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
    /// use interstellar::traversal::__;
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
    /// use interstellar::traversal::__;
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
    /// use interstellar::traversal::__;
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
    /// use interstellar::traversal::__;
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
    /// use interstellar::traversal::__;
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
    /// use interstellar::traversal::__;
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
    // Mutation steps
    // -------------------------------------------------------------------------

    /// Add a property to the current element (vertex or edge).
    ///
    /// This step adds or updates a property on the current element in the
    /// traversal. The actual property modification happens when a terminal
    /// step is called.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Add properties to a newly created vertex
    /// let v = g.add_v("person")
    ///     .property("name", "Alice")
    ///     .property("age", 30)
    ///     .next();
    ///
    /// // Update a property on an existing vertex
    /// g.v_id(VertexId(1))
    ///     .property("status", "active")
    ///     .iterate();
    /// ```
    pub fn property(
        self,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::mutation::PropertyStep;
        self.add_step(PropertyStep::new(key, value))
    }

    /// Delete the current element (vertex or edge).
    ///
    /// This step marks the current element for deletion. The actual deletion
    /// happens when a terminal step is called. When a vertex is deleted, all
    /// its incident edges are also deleted.
    ///
    /// # Behavior
    ///
    /// - Consumes the traverser (produces no output for non-mutation execution)
    /// - Vertex deletion cascades to edge deletion
    /// - Non-element values are silently ignored
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Delete a specific vertex
    /// g.v_id(VertexId(1)).drop().iterate();
    ///
    /// // Delete all edges of a certain type
    /// g.e().has_label("temp").drop().iterate();
    ///
    /// // Delete vertices matching a condition
    /// g.v().has_value("status", "deleted").drop().iterate();
    /// ```
    pub fn drop(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::mutation::DropStep;
        self.add_step(DropStep::new())
    }

    /// Create an edge from the current vertex.
    ///
    /// This step creates a new edge starting from the current traverser's
    /// vertex. The `to` endpoint must be specified using the builder methods.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an edge from the current vertex to a specific vertex
    /// let edge = g.v_id(VertexId(1))
    ///     .add_e("knows")
    ///     .to_vertex(VertexId(2))
    ///     .property("since", 2020)
    ///     .next();
    ///
    /// // Create an edge to a labeled step
    /// let edges = g.v()
    ///     .as_("a")
    ///     .out("knows")
    ///     .as_("b")
    ///     .add_e("friend_of_friend")
    ///     .from_label("a")
    ///     .to_label("b")
    ///     .to_list();
    /// ```
    pub fn add_e(self, label: impl Into<String>) -> BoundAddEdgeBuilder<'g, In> {
        BoundAddEdgeBuilder::from_traversal(self, label.into())
    }

    // -------------------------------------------------------------------------
    // Side effect steps
    // -------------------------------------------------------------------------

    /// Store each traverser value into a named side-effect collection (lazy).
    ///
    /// This is NOT a barrier step - values are stored as they pass through,
    /// and traversers continue immediately. Values are stored incrementally
    /// as the traversal proceeds.
    ///
    /// # Gremlin Equivalent
    ///
    /// ```groovy
    /// g.V().store("x").out().store("y")
    /// ```
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Store all visited vertices
    /// let _ = g.v().store("visited").out().store("neighbors").to_list();
    ///
    /// // Retrieve stored values from ExecutionContext's side_effects
    /// ```
    pub fn store(self, key: impl Into<String>) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::sideeffect::StoreStep;
        self.add_step(StoreStep::new(key))
    }

    /// Aggregate all traverser values into a named side-effect collection (barrier).
    ///
    /// This is a **barrier step** - it collects ALL input traversers before
    /// allowing any to continue. This is useful when you need all values to be
    /// stored before subsequent steps execute.
    ///
    /// # Gremlin Equivalent
    ///
    /// ```groovy
    /// g.V().aggregate("x").out().where(within("x"))
    /// ```
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Collect all starting vertices before continuing
    /// let result = g.v()
    ///     .has_label("person")
    ///     .aggregate("people")
    ///     .out("knows")
    ///     .to_list();
    /// ```
    pub fn aggregate(self, key: impl Into<String>) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::sideeffect::AggregateStep;
        self.add_step(AggregateStep::new(key))
    }

    /// Retrieve accumulated side-effect data (single key).
    ///
    /// Returns a `Value::List` containing all values stored under the given key.
    /// This step consumes the input stream first to ensure side effects are populated.
    ///
    /// # Gremlin Equivalent
    ///
    /// ```groovy
    /// g.V().store("x").cap("x")
    /// ```
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Store and retrieve vertices
    /// let stored = g.v().store("all").cap("all").next();
    /// // Returns Some(Value::List([...all vertices...]))
    /// ```
    pub fn cap(self, key: impl Into<String>) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::sideeffect::CapStep;
        self.add_step(CapStep::new(key))
    }

    /// Retrieve accumulated side-effect data (multiple keys).
    ///
    /// Returns a `Value::Map` containing the stored values for each key.
    /// Each key maps to a `Value::List` of stored values.
    ///
    /// # Gremlin Equivalent
    ///
    /// ```groovy
    /// g.V().store("x").store("y").cap("x", "y")
    /// ```
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Store and retrieve from multiple keys
    /// let stored = g.v()
    ///     .store("vertices")
    ///     .out_e().store("edges")
    ///     .cap_multi(&["vertices", "edges"])
    ///     .next();
    /// // Returns Some(Value::Map { "vertices" -> List, "edges" -> List })
    /// ```
    pub fn cap_multi<I, S>(self, keys: I) -> BoundTraversal<'g, In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        use crate::traversal::sideeffect::CapStep;
        self.add_step(CapStep::multi(keys))
    }

    /// Execute a traversal for side effects only.
    ///
    /// The sub-traversal is executed for each input traverser, but its output
    /// is discarded. The original traverser passes through unchanged.
    ///
    /// # Gremlin Equivalent
    ///
    /// ```groovy
    /// g.V().sideEffect(out().count().store("counts"))
    /// ```
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::traversal::__;
    ///
    /// // Store counts as side effect while traversing
    /// let names = g.v()
    ///     .side_effect(__.out_e().count().store("edge_counts"))
    ///     .values("name")
    ///     .to_list();
    /// ```
    pub fn side_effect(
        self,
        traversal: crate::traversal::Traversal<Value, Value>,
    ) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::sideeffect::SideEffectStep;
        self.add_step(SideEffectStep::new(traversal))
    }

    /// Collect traversal profiling information with default key "profile".
    ///
    /// This step records the count of traversers and elapsed time as they pass
    /// through. The profile data is stored in the side effects when the iterator
    /// is exhausted.
    ///
    /// # Gremlin Equivalent
    ///
    /// ```groovy
    /// g.V().out().profile()
    /// ```
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Profile the traversal
    /// let result = g.v().out().profile().to_list();
    ///
    /// // Profile data stored under "profile" key with {"count": n, "time_ms": t}
    /// ```
    pub fn profile(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::sideeffect::ProfileStep;
        self.add_step(ProfileStep::new())
    }

    /// Collect traversal profiling information with a named key.
    ///
    /// Like `profile()`, but stores the profile data under the specified key
    /// instead of the default "profile" key.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Profile with custom key
    /// let result = g.v().profile_as("step1").out().profile_as("step2").to_list();
    ///
    /// // Two profiles stored: "step1" and "step2"
    /// ```
    pub fn profile_as(self, key: impl Into<String>) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::sideeffect::ProfileStep;
        self.add_step(ProfileStep::with_key(key))
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
    /// use interstellar::traversal::__;
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
            self.storage,
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
            storage: self.storage,
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
        storage: &'g dyn GraphStorage,
        interner: &'g StringInterner,
        traversal: Traversal<In, Out>,
        track_paths: bool,
    ) -> Self {
        let ctx = if track_paths {
            ExecutionContext::with_path_tracking(storage, interner)
        } else {
            ExecutionContext::new(storage, interner)
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

    // -------------------------------------------------------------------------
    // Multi-way Branch Steps
    // -------------------------------------------------------------------------

    /// Start a branch step with the given branch traversal.
    ///
    /// The branch traversal is evaluated for each input traverser to produce
    /// a key. The key is then matched against option branches. Use `.option()`
    /// to define branches for specific keys and `.option_none()` for a default
    /// fallback.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::traversal::__;
    ///
    /// // Route based on vertex label
    /// let results = g.v()
    ///     .branch(__.label())
    ///     .option("person", __.out_labels(&["knows"]))
    ///     .option("software", __.in_labels(&["created"]))
    ///     .option_none(__.identity())
    ///     .to_list();
    /// ```
    ///
    /// # Returns
    ///
    /// A `BranchBuilder` that allows configuring option branches.
    pub fn branch(self, branch_traversal: Traversal<Value, Value>) -> BranchBuilder<'g, In> {
        let track_paths = self.track_paths;
        let (source, steps) = self.traversal.into_steps();

        BranchBuilder::new(
            self.storage,
            self.interner,
            source,
            steps,
            branch_traversal,
            track_paths,
        )
    }

    /// Start a choose-option step with the given branch traversal.
    ///
    /// This is an alias for `branch()` that provides the `choose().option()`
    /// pattern from Gremlin. It evaluates the branch traversal for each input
    /// traverser to produce a key, then routes to the matching option branch.
    ///
    /// Note: This is distinct from the binary `choose(condition, if_true, if_false)`
    /// which takes a condition traversal and two branches.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::traversal::__;
    ///
    /// // Route based on property value
    /// let results = g.v()
    ///     .choose_by(__.values("status"))
    ///     .option("active", __.out())
    ///     .option("inactive", __.identity())
    ///     .option_none(__.constant("unknown"))
    ///     .to_list();
    /// ```
    ///
    /// # Returns
    ///
    /// A `BranchBuilder` that allows configuring option branches.
    pub fn choose_by(self, branch_traversal: Traversal<Value, Value>) -> BranchBuilder<'g, In> {
        self.branch(branch_traversal)
    }
}

// -----------------------------------------------------------------------------
// BranchBuilder - Builder for multi-way branch steps
// -----------------------------------------------------------------------------

/// Fluent builder for creating multi-way branch steps in bound traversals.
///
/// This builder is returned from `BoundTraversal::branch()` or `choose_by()`
/// and allows configuring option branches before completing the traversal.
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::__;
///
/// // Route based on vertex label
/// let results = g.v()
///     .branch(__.label())
///     .option("person", __.out_labels(&["knows"]))
///     .option("software", __.in_labels(&["created"]))
///     .option_none(__.identity())
///     .to_list();
/// ```
pub struct BranchBuilder<'g, In> {
    storage: &'g dyn GraphStorage,
    interner: &'g StringInterner,
    source: Option<TraversalSource>,
    steps: Vec<Box<dyn AnyStep>>,
    branch_traversal: Traversal<Value, Value>,
    options: std::collections::HashMap<
        crate::traversal::branch::OptionKeyWrapper,
        Traversal<Value, Value>,
    >,
    none_branch: Option<Traversal<Value, Value>>,
    track_paths: bool,
    _phantom: PhantomData<In>,
}

impl<'g, In> BranchBuilder<'g, In> {
    /// Create a new BranchBuilder with existing steps and graph references.
    pub(crate) fn new(
        storage: &'g dyn GraphStorage,
        interner: &'g StringInterner,
        source: Option<TraversalSource>,
        steps: Vec<Box<dyn AnyStep>>,
        branch_traversal: Traversal<Value, Value>,
        track_paths: bool,
    ) -> Self {
        Self {
            storage,
            interner,
            source,
            steps,
            branch_traversal,
            options: std::collections::HashMap::new(),
            none_branch: None,
            track_paths,
            _phantom: PhantomData,
        }
    }

    /// Add an option branch for a specific key.
    ///
    /// When the branch traversal produces a value matching `key`, the `branch`
    /// traversal will be executed for that traverser.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to match against (can be string, int, bool, etc.)
    /// * `branch` - The traversal to execute when the key matches
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::traversal::__;
    ///
    /// let results = g.v()
    ///     .branch(__.label())
    ///     .option("person", __.values("name"))
    ///     .option("software", __.values("version"))
    ///     .to_list();
    /// ```
    pub fn option<K: Into<crate::traversal::branch::OptionKey>>(
        mut self,
        key: K,
        branch: Traversal<Value, Value>,
    ) -> Self {
        use crate::traversal::branch::{OptionKey, OptionKeyWrapper};

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
    /// ```ignore
    /// use interstellar::traversal::__;
    ///
    /// let results = g.v()
    ///     .branch(__.label())
    ///     .option("person", __.values("name"))
    ///     .option_none(__.constant("unknown"))
    ///     .to_list();
    /// ```
    pub fn option_none(mut self, branch: Traversal<Value, Value>) -> Self {
        self.none_branch = Some(branch);
        self
    }

    /// Finalize the builder and return a BoundTraversal.
    fn finalize(mut self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::branch::BranchStep;

        let mut step = BranchStep::new(self.branch_traversal);
        step.options = self.options;
        step.none_branch = self.none_branch;

        self.steps.push(Box::new(step));

        let traversal = Traversal {
            steps: self.steps,
            source: self.source,
            _phantom: PhantomData,
        };

        let mut bound = BoundTraversal::new(self.storage, self.interner, traversal);

        if self.track_paths {
            bound = bound.with_path();
        }

        bound
    }

    // -------------------------------------------------------------------------
    // Terminal Methods
    // -------------------------------------------------------------------------

    /// Execute and collect all results into a list.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = g.v()
    ///     .branch(__.label())
    ///     .option("person", __.values("name"))
    ///     .to_list();
    /// ```
    pub fn to_list(self) -> Vec<Value> {
        self.finalize().to_list()
    }

    /// Count the number of traversers.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let count = g.v()
    ///     .branch(__.label())
    ///     .option("person", __.identity())
    ///     .count();
    /// ```
    pub fn count(self) -> u64 {
        self.finalize().count()
    }

    /// Return the next result, if any.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let first = g.v()
    ///     .branch(__.label())
    ///     .option("person", __.values("name"))
    ///     .next();
    /// ```
    pub fn next(self) -> Option<Value> {
        self.finalize().next()
    }

    /// Return exactly one result, or an error if zero or multiple.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let single = g.v_id(VertexId(0))
    ///     .branch(__.label())
    ///     .option("person", __.values("name"))
    ///     .one();
    /// ```
    pub fn one(self) -> Result<Value, crate::error::TraversalError> {
        self.finalize().one()
    }

    /// Execute and discard all results.
    ///
    /// # Example
    ///
    /// ```ignore
    /// g.v()
    ///     .branch(__.label())
    ///     .option("person", __.drop())
    ///     .iterate();
    /// ```
    pub fn iterate(self) {
        self.finalize().iterate()
    }

    /// Check if there are any results.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let has_people = g.v()
    ///     .branch(__.label())
    ///     .option("person", __.identity())
    ///     .has_next();
    /// ```
    pub fn has_next(self) -> bool {
        self.finalize().has_next()
    }

    // -------------------------------------------------------------------------
    // Continuation Methods (delegate to finalize().method())
    // -------------------------------------------------------------------------

    /// Navigate to outgoing adjacent vertices.
    pub fn out(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().out()
    }

    /// Navigate to incoming adjacent vertices.
    pub fn in_(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().in_()
    }

    /// Navigate to adjacent vertices in both directions.
    pub fn both(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().both()
    }

    /// Navigate to outgoing edges.
    pub fn out_e(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().out_e()
    }

    /// Navigate to incoming edges.
    pub fn in_e(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().in_e()
    }

    /// Navigate to edges in both directions.
    pub fn both_e(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().both_e()
    }

    /// Filter by element label.
    pub fn has_label(self, label: &str) -> BoundTraversal<'g, In, Value> {
        self.finalize().has_label(label)
    }

    /// Filter by any of the given labels.
    pub fn has_label_any<I, S>(self, labels: I) -> BoundTraversal<'g, In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.finalize().has_label_any(labels)
    }

    /// Filter by property existence.
    pub fn has(self, key: &str) -> BoundTraversal<'g, In, Value> {
        self.finalize().has(key)
    }

    /// Filter by property value.
    pub fn has_value(self, key: &str, value: impl Into<Value>) -> BoundTraversal<'g, In, Value> {
        self.finalize().has_value(key, value)
    }

    /// Filter by predicate on property.
    pub fn has_where(
        self,
        key: impl Into<String>,
        predicate: impl crate::traversal::predicate::Predicate + 'static,
    ) -> BoundTraversal<'g, In, Value> {
        self.finalize().has_where(key, predicate)
    }

    /// Remove duplicate traversers.
    pub fn dedup(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().dedup()
    }

    /// Limit the number of traversers.
    pub fn limit(self, n: usize) -> BoundTraversal<'g, In, Value> {
        self.finalize().limit(n)
    }

    /// Extract property values.
    pub fn values(self, key: &str) -> BoundTraversal<'g, In, Value> {
        self.finalize().values(key)
    }

    /// Extract element ID.
    pub fn id(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().id()
    }

    /// Extract element label.
    pub fn label(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().label()
    }

    /// Label the current position for later reference.
    pub fn as_(self, label: &str) -> BoundTraversal<'g, In, Value> {
        self.finalize().as_(label)
    }

    /// Select labeled values from path.
    pub fn select_one(self, label: &str) -> BoundTraversal<'g, In, Value> {
        self.finalize().select_one(label)
    }

    /// Get the path of elements traversed.
    pub fn path(self) -> BoundTraversal<'g, In, Value> {
        self.finalize().path()
    }

    /// Replace with a constant value.
    pub fn constant(self, value: impl Into<Value>) -> BoundTraversal<'g, In, Value> {
        self.finalize().constant(value)
    }
}

// -----------------------------------------------------------------------------
// AddEdgeBuilder - Builder for creating edges from GraphTraversalSource
// -----------------------------------------------------------------------------

/// Builder for creating edges starting from `GraphTraversalSource`.
///
/// This builder is returned by `g.add_e(label)` and allows specifying both
/// the source (`from`) and target (`to`) vertices before creating the edge.
///
/// # Example
///
/// ```ignore
/// let edge = g.add_e("knows")
///     .from_vertex(VertexId(1))
///     .to_vertex(VertexId(2))
///     .property("since", 2020)
///     .next();
/// ```
pub struct AddEdgeBuilder<'g> {
    storage: &'g dyn GraphStorage,
    interner: &'g StringInterner,
    label: String,
    from: Option<crate::traversal::EdgeEndpoint>,
    to: Option<crate::traversal::EdgeEndpoint>,
    properties: std::collections::HashMap<String, Value>,
}

impl<'g> AddEdgeBuilder<'g> {
    /// Create a new edge builder.
    fn new(storage: &'g dyn GraphStorage, interner: &'g StringInterner, label: String) -> Self {
        Self {
            storage,
            interner,
            label,
            from: None,
            to: None,
            properties: std::collections::HashMap::new(),
        }
    }

    /// Set the source vertex by ID.
    pub fn from_vertex(mut self, id: VertexId) -> Self {
        self.from = Some(crate::traversal::EdgeEndpoint::VertexId(id));
        self
    }

    /// Set the source vertex from a step label.
    pub fn from_label(mut self, label: impl Into<String>) -> Self {
        self.from = Some(crate::traversal::EdgeEndpoint::StepLabel(label.into()));
        self
    }

    /// Set the target vertex by ID.
    pub fn to_vertex(mut self, id: VertexId) -> Self {
        self.to = Some(crate::traversal::EdgeEndpoint::VertexId(id));
        self
    }

    /// Set the target vertex from a step label.
    pub fn to_label(mut self, label: impl Into<String>) -> Self {
        self.to = Some(crate::traversal::EdgeEndpoint::StepLabel(label.into()));
        self
    }

    /// Add a property to the edge.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Build the traversal and return it.
    pub fn build(self) -> BoundTraversal<'g, (), Value> {
        use crate::traversal::mutation::AddEStep;

        let mut step = AddEStep::new(&self.label);

        // Set from endpoint
        if let Some(from) = self.from {
            step = match from {
                crate::traversal::EdgeEndpoint::VertexId(id) => step.from_vertex(id),
                crate::traversal::EdgeEndpoint::StepLabel(label) => step.from_label(label),
                crate::traversal::EdgeEndpoint::Traverser => step.from_traverser(),
            };
        }

        // Set to endpoint
        if let Some(to) = self.to {
            step = match to {
                crate::traversal::EdgeEndpoint::VertexId(id) => step.to_vertex(id),
                crate::traversal::EdgeEndpoint::StepLabel(label) => step.to_label(label),
                crate::traversal::EdgeEndpoint::Traverser => step.to_traverser(),
            };
        }

        // Add properties
        for (key, value) in self.properties {
            step = step.property(key, value);
        }

        // Create a traversal that starts with inject to provide input
        let mut traversal = Traversal::<(), Value>::with_source(TraversalSource::Inject(vec![]));
        traversal = traversal.add_step(step);
        BoundTraversal::new(self.storage, self.interner, traversal)
    }

    // Terminal methods

    /// Execute and return the first result.
    pub fn next(self) -> Option<Value> {
        self.build().next()
    }

    /// Execute and collect all results into a list.
    pub fn to_list(self) -> Vec<Value> {
        self.build().to_list()
    }

    /// Execute and consume the traversal.
    pub fn iterate(self) {
        self.build().iterate()
    }
}

// -----------------------------------------------------------------------------
// BoundAddEdgeBuilder - Builder for creating edges from a traversal position
// -----------------------------------------------------------------------------

/// Builder for creating edges starting from a vertex in an ongoing traversal.
///
/// This builder is returned by `traversal.add_e(label)` and allows specifying
/// the target (`to`) vertex. The source vertex is implicitly the current
/// traverser's vertex.
///
/// # Example
///
/// ```ignore
/// let edges = g.v_id(VertexId(1))
///     .add_e("knows")
///     .to_vertex(VertexId(2))
///     .property("since", 2020)
///     .to_list();
/// ```
pub struct BoundAddEdgeBuilder<'g, In> {
    storage: &'g dyn GraphStorage,
    interner: &'g StringInterner,
    traversal: Traversal<In, Value>,
    track_paths: bool,
    label: String,
    from: Option<crate::traversal::EdgeEndpoint>,
    to: Option<crate::traversal::EdgeEndpoint>,
    properties: std::collections::HashMap<String, Value>,
}

impl<'g, In> BoundAddEdgeBuilder<'g, In> {
    /// Create a builder from an existing traversal.
    fn from_traversal(bound: BoundTraversal<'g, In, Value>, label: String) -> Self {
        Self {
            storage: bound.storage,
            interner: bound.interner,
            traversal: bound.traversal,
            track_paths: bound.track_paths,
            label,
            from: Some(crate::traversal::EdgeEndpoint::Traverser), // Default from current traverser
            to: None,
            properties: std::collections::HashMap::new(),
        }
    }

    /// Set the source vertex by ID (overrides the default current traverser).
    pub fn from_vertex(mut self, id: VertexId) -> Self {
        self.from = Some(crate::traversal::EdgeEndpoint::VertexId(id));
        self
    }

    /// Set the source vertex from a step label (overrides the default current traverser).
    pub fn from_label(mut self, label: impl Into<String>) -> Self {
        self.from = Some(crate::traversal::EdgeEndpoint::StepLabel(label.into()));
        self
    }

    /// Set the target vertex by ID.
    pub fn to_vertex(mut self, id: VertexId) -> Self {
        self.to = Some(crate::traversal::EdgeEndpoint::VertexId(id));
        self
    }

    /// Set the target vertex from a step label.
    pub fn to_label(mut self, label: impl Into<String>) -> Self {
        self.to = Some(crate::traversal::EdgeEndpoint::StepLabel(label.into()));
        self
    }

    /// Add a property to the edge.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Build the traversal and return it.
    pub fn build(self) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::mutation::AddEStep;

        let mut step = AddEStep::new(&self.label);

        // Set from endpoint
        if let Some(from) = self.from {
            step = match from {
                crate::traversal::EdgeEndpoint::VertexId(id) => step.from_vertex(id),
                crate::traversal::EdgeEndpoint::StepLabel(label) => step.from_label(label),
                crate::traversal::EdgeEndpoint::Traverser => step.from_traverser(),
            };
        }

        // Set to endpoint
        if let Some(to) = self.to {
            step = match to {
                crate::traversal::EdgeEndpoint::VertexId(id) => step.to_vertex(id),
                crate::traversal::EdgeEndpoint::StepLabel(label) => step.to_label(label),
                crate::traversal::EdgeEndpoint::Traverser => step.to_traverser(),
            };
        }

        // Add properties
        for (key, value) in self.properties {
            step = step.property(key, value);
        }

        BoundTraversal {
            storage: self.storage,
            interner: self.interner,
            traversal: self.traversal.add_step(step),
            track_paths: self.track_paths,
        }
    }

    // Terminal methods

    /// Execute and return the first result.
    pub fn next(self) -> Option<Value> {
        self.build().next()
    }

    /// Execute and collect all results into a list.
    pub fn to_list(self) -> Vec<Value> {
        self.build().to_list()
    }

    /// Execute and consume the traversal.
    pub fn iterate(self) {
        self.build().iterate()
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Graph;
    use std::collections::HashMap;

    fn create_empty_graph() -> Graph {
        Graph::new()
    }

    fn create_test_graph() -> Graph {
        let graph = Graph::new();

        // Add vertices
        let v1 = graph.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props.insert("age".to_string(), Value::Int(30));
            props
        });

        let v2 = graph.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Bob".to_string()));
            props.insert("age".to_string(), Value::Int(25));
            props
        });

        let v3 = graph.add_vertex("software", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Graph DB".to_string()));
            props.insert("version".to_string(), Value::Float(1.0));
            props
        });

        let v4 = graph.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Charlie".to_string()));
            props.insert("age".to_string(), Value::Int(35));
            props
        });

        // Add edges
        graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
        graph.add_edge(v2, v3, "uses", HashMap::new()).unwrap();
        graph.add_edge(v1, v3, "uses", HashMap::new()).unwrap();
        graph.add_edge(v2, v4, "knows", HashMap::new()).unwrap();
        graph
            .add_edge(v4, v1, "knows", {
                let mut props = HashMap::new();
                props.insert("since".to_string(), Value::Int(2020));
                props
            })
            .unwrap();

        graph
    }

    mod graph_traversal_source_tests {
        use super::*;

        #[test]
        fn from_snapshot_creates_source() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            // Should be able to access references
            let _ = g.storage();
            let _ = g.interner();
        }

        #[test]
        fn v_creates_all_vertices_traversal() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let results = g.v_ids([VertexId(0), VertexId(999)]).to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn e_creates_all_edges_traversal() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let results = g.e_ids([EdgeId(0), EdgeId(999)]).to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].as_edge_id(), Some(EdgeId(0)));
        }

        #[test]
        fn inject_creates_value_traversal() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let results = g.v().to_list();
            assert_eq!(results.len(), 4);
        }

        #[test]
        fn to_set_deduplicates() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let results = g.v().to_set();
            assert_eq!(results.len(), 4);
        }

        #[test]
        fn next_returns_first() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let result = g.v().next();
            assert!(result.is_some());
            assert!(result.unwrap().is_vertex());
        }

        #[test]
        fn next_returns_none_for_empty() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let result = g.v().next();
            assert!(result.is_none());
        }

        #[test]
        fn has_next_returns_true_for_nonempty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            assert!(g.v().has_next());
        }

        #[test]
        fn has_next_returns_false_for_empty() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            assert!(!g.v().has_next());
        }

        #[test]
        fn one_returns_single_value() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let result = g.v_ids([VertexId(0)]).one();
            assert!(result.is_ok());
            assert_eq!(result.unwrap().as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn one_errors_on_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            // This should not panic or error
            g.v().iterate();
        }

        #[test]
        fn count_returns_correct_value() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            assert_eq!(g.v().count(), 4);
            assert_eq!(g.e().count(), 5);
        }

        #[test]
        fn count_returns_zero_for_empty() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            assert_eq!(g.v().count(), 0);
        }

        #[test]
        fn take_returns_first_n() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let results = g.v().take(2);
            assert_eq!(results.len(), 2);
        }

        #[test]
        fn take_returns_all_if_less_than_n() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let results = g.v().take(100);
            assert_eq!(results.len(), 4);
        }

        #[test]
        fn iter_produces_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let values: Vec<Value> = g.v().iter().collect();
            assert_eq!(values.len(), 4);
        }

        #[test]
        fn traversers_produces_traversers() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let count = g
                .inject([1i64, 2i64, 3i64])
                .fold(0i64, |acc, v| acc + v.as_i64().unwrap_or(0));
            assert_eq!(count, 6);
        }

        #[test]
        fn sum_adds_integers() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let result = g.inject([1i64, 2i64, 3i64]).sum();
            assert_eq!(result, Value::Int(6));
        }

        #[test]
        fn sum_handles_floats() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let values: Vec<Value> = vec![Value::Int(1), Value::Float(2.5), Value::Int(3)];
            let result = g.inject(values).sum();
            assert!(matches!(result, Value::Float(f) if (f - 6.5).abs() < 1e-10));
        }

        #[test]
        fn sum_empty_returns_zero() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let values: Vec<Value> = vec![];
            let result = g.inject(values).sum();
            assert_eq!(result, Value::Int(0));
        }

        #[test]
        fn min_finds_minimum() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let result = g.inject([3i64, 1i64, 2i64]).min();
            assert_eq!(result, Some(Value::Int(1)));
        }

        #[test]
        fn min_empty_returns_none() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let values: Vec<Value> = vec![];
            let result = g.inject(values).min();
            assert!(result.is_none());
        }

        #[test]
        fn max_finds_maximum() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let result = g.inject([3i64, 1i64, 2i64]).max();
            assert_eq!(result, Some(Value::Int(3)));
        }

        #[test]
        fn max_empty_returns_none() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let executor = g.v().execute();
            assert_eq!(executor.len(), 0);
            assert!(executor.is_empty());
        }

        #[test]
        fn executor_size_hint_is_exact() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let software = g.v().has_label("software").to_list();

            // Should return 1 software vertex (Graph DB)
            assert_eq!(software.len(), 1);
        }

        #[test]
        fn has_label_returns_empty_for_nonexistent_label() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let unknown = g.v().has_label("unknown").to_list();
            assert!(unknown.is_empty());
        }

        #[test]
        fn has_label_filters_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let entities = g.v().has_label_any(["person", "software"]).to_list();

            // Should return 3 persons + 1 software = 4 vertices
            assert_eq!(entities.len(), 4);
        }

        #[test]
        fn has_label_any_works_with_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let edges = g.e().has_label_any(["knows", "uses"]).to_list();

            // Should return 3 "knows" + 2 "uses" = 5 edges
            assert_eq!(edges.len(), 5);
        }

        #[test]
        fn has_label_can_be_chained() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            // This shouldn't match anything since an element can't have two labels
            // (this tests that chaining works, even if the result is empty)
            let result = g.v().has_label("person").has_label("software").to_list();
            assert!(result.is_empty());
        }

        #[test]
        fn has_label_count_works() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            let count = g.v().has_label("person").count();
            assert_eq!(count, 3);
        }

        #[test]
        fn has_label_with_specific_vertex_ids() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

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
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            // Verify BoundTraversal starts with path tracking disabled
            let traversal = g.v();
            assert!(!traversal.is_tracking_paths());

            // After with_path(), it should be enabled
            let traversal = g.v().with_path();
            assert!(traversal.is_tracking_paths());
        }
    }

    mod index_aware_source_tests {
        use super::*;
        use crate::index::IndexBuilder;
        use std::ops::Bound;

        fn create_indexed_graph() -> Graph {
            let graph = Graph::new();

            // Add vertices with various ages
            let _v1 = graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Alice".to_string()));
                props.insert("age".to_string(), Value::Int(25));
                props
            });

            let _v2 = graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Bob".to_string()));
                props.insert("age".to_string(), Value::Int(30));
                props
            });

            let _v3 = graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Charlie".to_string()));
                props.insert("age".to_string(), Value::Int(35));
                props
            });

            let _v4 = graph.add_vertex("company", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("TechCorp".to_string()));
                props.insert("size".to_string(), Value::Int(100));
                props
            });

            // Create an index on person.age
            let index_spec = IndexBuilder::vertex()
                .label("person")
                .property("age")
                .build()
                .unwrap();
            graph.create_index(index_spec).unwrap();

            graph
        }

        fn create_graph_with_edge_index() -> Graph {
            let graph = Graph::new();

            let v1 = graph.add_vertex("person", HashMap::new());
            let v2 = graph.add_vertex("person", HashMap::new());
            let v3 = graph.add_vertex("person", HashMap::new());

            // Add edges with weights
            graph
                .add_edge(v1, v2, "knows", {
                    let mut props = HashMap::new();
                    props.insert("weight".to_string(), Value::Float(1.0));
                    props
                })
                .unwrap();

            graph
                .add_edge(v2, v3, "knows", {
                    let mut props = HashMap::new();
                    props.insert("weight".to_string(), Value::Float(2.0));
                    props
                })
                .unwrap();

            graph
                .add_edge(v1, v3, "knows", {
                    let mut props = HashMap::new();
                    props.insert("weight".to_string(), Value::Float(1.0));
                    props
                })
                .unwrap();

            // Create an index on knows.weight
            let index_spec = IndexBuilder::edge()
                .label("knows")
                .property("weight")
                .build()
                .unwrap();
            graph.create_index(index_spec).unwrap();

            graph
        }

        #[test]
        fn v_by_property_finds_exact_match() {
            let graph = create_indexed_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            // Find person with age 30 (Bob)
            let results = g.v_by_property(Some("person"), "age", 30i64).to_list();

            assert_eq!(results.len(), 1);
            assert!(results[0].is_vertex());
        }

        #[test]
        fn v_by_property_returns_empty_when_no_match() {
            let graph = create_indexed_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            // No person with age 100
            let results = g.v_by_property(Some("person"), "age", 100i64).to_list();

            assert!(results.is_empty());
        }

        #[test]
        fn v_by_property_with_no_label_searches_all() {
            let graph = create_indexed_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            // Find any vertex with age 30 (no label filter)
            let results = g.v_by_property(None, "age", 30i64).to_list();

            assert_eq!(results.len(), 1);
        }

        #[test]
        fn v_by_property_works_without_index() {
            let graph = create_indexed_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            // No index on "size", but should still find TechCorp
            let results = g.v_by_property(Some("company"), "size", 100i64).to_list();

            assert_eq!(results.len(), 1);
        }

        #[test]
        fn v_by_property_range_finds_inclusive_range() {
            let graph = create_indexed_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            // Find people aged 25-30 (inclusive)
            let results = g
                .v_by_property_range(
                    Some("person"),
                    "age",
                    Bound::Included(&Value::Int(25)),
                    Bound::Included(&Value::Int(30)),
                )
                .to_list();

            // Should find Alice (25) and Bob (30)
            assert_eq!(results.len(), 2);
        }

        #[test]
        fn v_by_property_range_with_exclusive_bounds() {
            let graph = create_indexed_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            // Find people aged > 25 and < 35 (exclusive)
            let results = g
                .v_by_property_range(
                    Some("person"),
                    "age",
                    Bound::Excluded(&Value::Int(25)),
                    Bound::Excluded(&Value::Int(35)),
                )
                .to_list();

            // Should find only Bob (30)
            assert_eq!(results.len(), 1);
        }

        #[test]
        fn v_by_property_range_with_unbounded_end() {
            let graph = create_indexed_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            // Find people aged >= 30
            let results = g
                .v_by_property_range(
                    Some("person"),
                    "age",
                    Bound::Included(&Value::Int(30)),
                    Bound::Unbounded,
                )
                .to_list();

            // Should find Bob (30) and Charlie (35)
            assert_eq!(results.len(), 2);
        }

        #[test]
        fn v_by_property_range_with_unbounded_start() {
            let graph = create_indexed_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            // Find people aged <= 30
            let results = g
                .v_by_property_range(
                    Some("person"),
                    "age",
                    Bound::Unbounded,
                    Bound::Included(&Value::Int(30)),
                )
                .to_list();

            // Should find Alice (25) and Bob (30)
            assert_eq!(results.len(), 2);
        }

        #[test]
        fn v_by_property_range_returns_empty_for_no_match() {
            let graph = create_indexed_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            // No one aged 100-200
            let results = g
                .v_by_property_range(
                    Some("person"),
                    "age",
                    Bound::Included(&Value::Int(100)),
                    Bound::Included(&Value::Int(200)),
                )
                .to_list();

            assert!(results.is_empty());
        }

        #[test]
        fn e_by_property_finds_exact_match() {
            let graph = create_graph_with_edge_index();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            // Find edges with weight 1.0
            let results = g.e_by_property(Some("knows"), "weight", 1.0f64).to_list();

            // Should find 2 edges with weight 1.0
            assert_eq!(results.len(), 2);
            for result in &results {
                assert!(result.is_edge());
            }
        }

        #[test]
        fn e_by_property_returns_empty_when_no_match() {
            let graph = create_graph_with_edge_index();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            // No edge with weight 99.0
            let results = g.e_by_property(Some("knows"), "weight", 99.0f64).to_list();

            assert!(results.is_empty());
        }

        #[test]
        fn e_by_property_with_no_label_searches_all() {
            let graph = create_graph_with_edge_index();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            // Find any edge with weight 2.0 (no label filter)
            let results = g.e_by_property(None, "weight", 2.0f64).to_list();

            assert_eq!(results.len(), 1);
        }

        #[test]
        fn v_by_property_can_chain_steps() {
            let graph = create_indexed_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            // Find person with age 30, get their name
            let names = g
                .v_by_property(Some("person"), "age", 30i64)
                .values("name")
                .to_list();

            assert_eq!(names.len(), 1);
            assert_eq!(names[0], Value::String("Bob".to_string()));
        }

        #[test]
        fn v_by_property_range_can_chain_steps() {
            let graph = create_indexed_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            // Find people aged 25-35, count them
            let count = g
                .v_by_property_range(
                    Some("person"),
                    "age",
                    Bound::Included(&Value::Int(25)),
                    Bound::Included(&Value::Int(35)),
                )
                .count();

            assert_eq!(count, 3); // Alice, Bob, Charlie
        }

        #[test]
        fn e_by_property_can_chain_navigation() {
            let graph = create_graph_with_edge_index();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::from_snapshot(&snapshot);

            // Find edges with weight 1.0, get their target vertices
            let targets = g
                .e_by_property(Some("knows"), "weight", 1.0f64)
                .in_v()
                .to_list();

            assert_eq!(targets.len(), 2);
            for target in &targets {
                assert!(target.is_vertex());
            }
        }
    }
}
