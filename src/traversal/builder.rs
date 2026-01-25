//! Traversal step methods for anonymous traversals.
//!
//! This module provides the chainable step methods on `Traversal<In, Value>`.
//! These methods allow building anonymous traversal pipelines that can be
//! composed with bound traversals.

use crate::traversal::aggregate;
use crate::traversal::branch;
use crate::traversal::context;
use crate::traversal::filter;
use crate::traversal::mutation;
use crate::traversal::navigation;
use crate::traversal::pipeline::Traversal;
use crate::traversal::predicate;
use crate::traversal::sideeffect;
use crate::traversal::transform;
use crate::value::Value;

// -----------------------------------------------------------------------------
// Traversal Step Methods for Anonymous Traversals
// -----------------------------------------------------------------------------

impl<In> Traversal<In, Value> {
    /// Filter elements by label (for anonymous traversals).
    ///
    /// Keeps only vertices/edges whose label matches the given label.
    /// Non-element values (integers, strings, etc.) are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to person vertices
    /// let anon = __.has_label("person");
    /// let people = g.v().append(anon).to_list();
    /// ```
    pub fn has_label(self, label: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(filter::HasLabelStep::single(label))
    }

    /// Filter elements by any of the given labels (for anonymous traversals).
    ///
    /// Keeps only vertices/edges whose label matches any of the given labels.
    /// Non-element values (integers, strings, etc.) are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to person or company vertices
    /// let anon = __.has_label_any(&["person", "company"]);
    /// let entities = g.v().append(anon).to_list();
    /// ```
    pub fn has_label_any<I, S>(self, labels: I) -> Traversal<In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.add_step(filter::HasLabelStep::any(labels))
    }

    /// Filter elements by property existence (for anonymous traversals).
    ///
    /// Keeps only vertices/edges that have the specified property.
    /// Non-element values (integers, strings, etc.) are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to vertices with "age" property
    /// let anon = Traversal::<Value, Value>::new().has("age");
    /// let with_age = g.v().append(anon).to_list();
    /// ```
    pub fn has(self, key: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(filter::HasStep::new(key))
    }

    /// Filter elements by property absence (for anonymous traversals).
    ///
    /// Keeps only vertices/edges that do NOT have the specified property.
    /// Non-element values (integers, strings, etc.) pass through since they
    /// don't have properties.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to vertices without "email" property
    /// let anon = Traversal::<Value, Value>::new().has_not("email");
    /// let without_email = g.v().append(anon).to_list();
    /// ```
    pub fn has_not(self, key: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(filter::HasNotStep::new(key))
    }

    /// Filter elements by property value equality (for anonymous traversals).
    ///
    /// Keeps only vertices/edges where the specified property equals the given value.
    /// Non-element values (integers, strings, etc.) are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to vertices where name == "Alice"
    /// let anon = Traversal::<Value, Value>::new().has_value("name", "Alice");
    /// let alice = g.v().append(anon).to_list();
    /// ```
    pub fn has_value(
        self,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> Traversal<In, Value> {
        self.add_step(filter::HasValueStep::new(key, value))
    }

    /// Filter elements by property value using a predicate (for anonymous traversals).
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
    /// // Create an anonymous traversal that filters to adults
    /// let anon = Traversal::<Value, Value>::new().has_where("age", p::gte(18));
    /// let adults = g.v().append(anon).to_list();
    ///
    /// // With string predicates
    /// let anon = Traversal::<Value, Value>::new().has_where("name", p::starting_with("A"));
    /// let a_names = g.v().append(anon).to_list();
    /// ```
    pub fn has_where(
        self,
        key: impl Into<String>,
        predicate: impl predicate::Predicate + 'static,
    ) -> Traversal<In, Value> {
        self.add_step(filter::HasWhereStep::new(key, predicate))
    }

    /// Filter by testing the current value against a predicate (for anonymous traversals).
    ///
    /// Unlike `has_where()` which tests a property of vertices/edges, `is_()` tests
    /// the traverser's current value directly. This is useful after extracting
    /// property values with `values()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::traversal::p;
    ///
    /// // Filter ages greater than 25
    /// let anon = Traversal::<Value, Value>::new().is_(p::gt(25));
    /// let adults = g.v().values("age").append(anon).to_list();
    ///
    /// // Filter ages in a range
    /// let anon = Traversal::<Value, Value>::new().is_(p::between(20, 40));
    /// let in_range = g.v().values("age").append(anon).to_list();
    /// ```
    pub fn is_(self, predicate: impl predicate::Predicate + 'static) -> Traversal<In, Value> {
        self.add_step(filter::IsStep::new(predicate))
    }

    /// Filter by testing the current value for equality (for anonymous traversals).
    ///
    /// This is a convenience method equivalent to `is_(p::eq(value))`.
    /// Useful after extracting property values with `values()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Filter to ages equal to 29
    /// let anon = Traversal::<Value, Value>::new().is_eq(29);
    /// let age_29 = g.v().values("age").append(anon).to_list();
    ///
    /// // Filter to a specific name
    /// let anon = Traversal::<Value, Value>::new().is_eq("Alice");
    /// let alice = g.v().values("name").append(anon).to_list();
    /// ```
    pub fn is_eq(self, value: impl Into<Value>) -> Traversal<In, Value> {
        self.add_step(filter::IsStep::eq(value))
    }

    /// Filter elements using a custom predicate (for anonymous traversals).
    ///
    /// The predicate receives the execution context and the value, returning
    /// `true` to keep the traverser or `false` to filter it out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to positive integers
    /// let anon = Traversal::<Value, Value>::new()
    ///     .filter(|_ctx, v| matches!(v, Value::Int(n) if *n > 0));
    /// let positives = g.inject([1i64, -2i64, 3i64]).append(anon).to_list();
    /// ```
    pub fn filter<F>(self, predicate: F) -> Traversal<In, Value>
    where
        F: Fn(&context::ExecutionContext, &Value) -> bool + Clone + Send + Sync + 'static,
    {
        self.add_step(filter::FilterStep::new(predicate))
    }

    /// Deduplicate traversers by value (for anonymous traversals).
    ///
    /// Removes duplicate values from the traversal, keeping only the first
    /// occurrence of each value. Uses `Value`'s `Hash` implementation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that deduplicates values
    /// let anon = Traversal::<Value, Value>::new().dedup();
    /// let unique = g.v().out().append(anon).to_list();
    /// ```
    pub fn dedup(self) -> Traversal<In, Value> {
        self.add_step(filter::DedupStep::new())
    }

    /// Deduplicate traversers by property value (for anonymous traversals).
    ///
    /// Removes duplicates based on a property value extracted from elements.
    /// Only the first occurrence of each unique property value passes through.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to use for deduplication
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that deduplicates by age
    /// let anon = Traversal::<Value, Value>::new().dedup_by_key("age");
    /// let unique_ages = g.v().has_label("person").append(anon).to_list();
    /// ```
    pub fn dedup_by_key(self, key: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(filter::DedupByKeyStep::new(key))
    }

    /// Deduplicate traversers by element label (for anonymous traversals).
    ///
    /// Removes duplicates based on element label. Only the first occurrence
    /// of each unique label passes through.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that keeps one per label
    /// let anon = Traversal::<Value, Value>::new().dedup_by_label();
    /// let one_per_label = g.v().append(anon).to_list();
    /// ```
    pub fn dedup_by_label(self) -> Traversal<In, Value> {
        self.add_step(filter::DedupByLabelStep::new())
    }

    /// Deduplicate traversers by sub-traversal result (for anonymous traversals).
    ///
    /// Executes the given sub-traversal for each element and uses the first
    /// result as the deduplication key.
    ///
    /// # Arguments
    ///
    /// * `sub` - The sub-traversal to execute for each element
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that deduplicates by out-degree
    /// let anon = Traversal::<Value, Value>::new()
    ///     .dedup_by(__.out().count());
    /// let unique_outdegree = g.v().append(anon).to_list();
    /// ```
    pub fn dedup_by(self, sub: Traversal<Value, Value>) -> Traversal<In, Value> {
        self.add_step(filter::DedupByTraversalStep::new(sub))
    }

    /// Limit the number of traversers passing through (for anonymous traversals).
    ///
    /// Returns at most the specified number of traversers, stopping iteration
    /// after the limit is reached.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that limits to 5 elements
    /// let anon = Traversal::<Value, Value>::new().limit(5);
    /// let first_five = g.v().append(anon).to_list();
    /// ```
    pub fn limit(self, count: usize) -> Traversal<In, Value> {
        self.add_step(filter::LimitStep::new(count))
    }

    /// Skip the first n traversers (for anonymous traversals).
    ///
    /// Discards the first n traversers and passes through all remaining ones.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that skips 10 elements
    /// let anon = Traversal::<Value, Value>::new().skip(10);
    /// let after_skip = g.v().append(anon).to_list();
    /// ```
    pub fn skip(self, count: usize) -> Traversal<In, Value> {
        self.add_step(filter::SkipStep::new(count))
    }

    /// Select traversers within a given range (for anonymous traversals).
    ///
    /// Equivalent to `skip(start).limit(end - start)`. Returns traversers
    /// from index `start` (inclusive) to index `end` (exclusive).
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that selects elements 10-19
    /// let anon = Traversal::<Value, Value>::new().range(10, 20);
    /// let page = g.v().append(anon).to_list();
    /// ```
    pub fn range(self, start: usize, end: usize) -> Traversal<In, Value> {
        self.add_step(filter::RangeStep::new(start, end))
    }

    /// Filter to only paths with no repeated elements (simple paths).
    ///
    /// A simple path visits each element at most once. This is useful
    /// for preventing cycles during traversal and finding unique paths.
    ///
    /// # Gremlin Equivalent
    ///
    /// ```groovy
    /// g.V().repeat(out()).until(hasLabel("target")).simplePath()
    /// ```
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Find all simple paths of length 3
    /// let simple = g.v()
    ///     .repeat(__.out())
    ///     .times(3)
    ///     .simple_path()
    ///     .to_list();
    /// ```
    pub fn simple_path(self) -> Traversal<In, Value> {
        self.add_step(filter::SimplePathStep::new())
    }

    /// Filter to only paths with at least one repeated element (cyclic paths).
    ///
    /// A cyclic path contains at least one element that appears more than once.
    /// This is the inverse of `simple_path()`.
    ///
    /// # Gremlin Equivalent
    ///
    /// ```groovy
    /// g.V().repeat(out()).until(hasLabel("target")).cyclicPath()
    /// ```
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Find all cyclic paths
    /// let cyclic = g.v()
    ///     .repeat(__.out())
    ///     .times(4)
    ///     .cyclic_path()
    ///     .to_list();
    /// ```
    pub fn cyclic_path(self) -> Traversal<In, Value> {
        self.add_step(filter::CyclicPathStep::new())
    }

    /// Return only the last element (for anonymous traversals).
    ///
    /// This is a **barrier step** - it collects ALL input before returning
    /// only the last element. Equivalent to `tail_n(1)`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that returns only the last element
    /// let anon = Traversal::<Value, Value>::new().tail();
    /// let last = g.v().append(anon).to_list();
    /// ```
    pub fn tail(self) -> Traversal<In, Value> {
        self.add_step(filter::TailStep::last())
    }

    /// Return only the last n elements (for anonymous traversals).
    ///
    /// This is a **barrier step** - it collects ALL input before returning
    /// the last n elements. Elements are returned in their original order.
    ///
    /// # Behavior
    ///
    /// - If fewer than n elements exist, all elements are returned
    /// - Empty traversal returns empty result
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that returns the last 5 elements
    /// let anon = Traversal::<Value, Value>::new().tail_n(5);
    /// let last_five = g.v().append(anon).to_list();
    /// ```
    pub fn tail_n(self, count: usize) -> Traversal<In, Value> {
        self.add_step(filter::TailStep::new(count))
    }

    /// Probabilistic filter using random coin flip (for anonymous traversals).
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
    /// // Create an anonymous traversal that randomly samples ~50%
    /// let anon = Traversal::<Value, Value>::new().coin(0.5);
    /// let sample = g.v().append(anon).to_list();
    /// ```
    pub fn coin(self, probability: f64) -> Traversal<In, Value> {
        self.add_step(filter::CoinStep::new(probability))
    }

    /// Randomly sample n elements using reservoir sampling (for anonymous traversals).
    ///
    /// This is a **barrier step** that collects all input elements and returns
    /// a random sample of exactly n elements. If the input has fewer than n
    /// elements, all elements are returned.
    ///
    /// # Arguments
    ///
    /// * `count` - The number of elements to sample
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that samples 5 random elements
    /// let anon = Traversal::<Value, Value>::new().sample(5);
    /// let sampled = g.v().append(anon).to_list();
    /// ```
    ///
    /// # Note
    ///
    /// Results are non-deterministic. For reproducible results in tests,
    /// use statistical tolerances.
    pub fn sample(self, count: usize) -> Traversal<In, Value> {
        self.add_step(filter::SampleStep::new(count))
    }

    /// Filter property objects by key name (for anonymous traversals).
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
    /// // Create an anonymous traversal that filters to "name" properties
    /// let anon = Traversal::<Value, Value>::new().has_key("name");
    /// let names = g.v().properties().append(anon).to_list();
    /// ```
    pub fn has_key(self, key: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(filter::HasKeyStep::new(key))
    }

    /// Filter property objects by any of the specified key names (for anonymous traversals).
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
    /// // Create an anonymous traversal that filters to "name" or "age" properties
    /// let anon = Traversal::<Value, Value>::new().has_key_any(["name", "age"]);
    /// let props = g.v().properties().append(anon).to_list();
    /// ```
    pub fn has_key_any<I, S>(self, keys: I) -> Traversal<In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.add_step(filter::HasKeyStep::any(keys))
    }

    /// Filter property objects by value (for anonymous traversals).
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
    /// // Create an anonymous traversal that filters to properties with value "Alice"
    /// let anon = Traversal::<Value, Value>::new().has_prop_value("Alice");
    /// let alice_props = g.v().properties().append(anon).to_list();
    /// ```
    pub fn has_prop_value(self, value: impl Into<Value>) -> Traversal<In, Value> {
        self.add_step(filter::HasPropValueStep::new(value))
    }

    /// Filter property objects by any of the specified values (for anonymous traversals).
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
    /// // Create an anonymous traversal that filters to properties with value "Alice" or "Bob"
    /// let anon = Traversal::<Value, Value>::new().has_prop_value_any(["Alice", "Bob"]);
    /// let props = g.v().properties().append(anon).to_list();
    /// ```
    pub fn has_prop_value_any<I, V>(self, values: I) -> Traversal<In, Value>
    where
        I: IntoIterator<Item = V>,
        V: Into<Value>,
    {
        self.add_step(filter::HasPropValueStep::any(values))
    }

    /// Filter traversers by testing their current value against a predicate (for anonymous traversals).
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
    /// // Create an anonymous traversal that filters values > 25
    /// let anon = Traversal::<Value, Value>::new().where_p(p::gt(25));
    /// let adults = g.v().values("age").append(anon).to_list();
    ///
    /// // Filter to values within a set
    /// let anon = Traversal::<Value, Value>::new().where_p(p::within(["Alice", "Bob"]));
    /// ```
    pub fn where_p(
        self,
        predicate: impl crate::traversal::predicate::Predicate + 'static,
    ) -> Traversal<In, Value> {
        self.add_step(filter::WherePStep::new(predicate))
    }

    /// Filter elements by a single ID (for anonymous traversals).
    ///
    /// Keeps only vertices/edges whose ID matches the given ID.
    /// Non-element values (integers, strings, etc.) are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to a specific vertex
    /// let anon = Traversal::<Value, Value>::new().has_id(VertexId(1));
    /// let vertex = g.v().append(anon).to_list();
    /// ```
    pub fn has_id(self, id: impl Into<Value>) -> Traversal<In, Value> {
        self.add_step(filter::HasIdStep::from_value(id))
    }

    /// Filter elements by multiple IDs (for anonymous traversals).
    ///
    /// Keeps only vertices/edges whose ID matches any of the given IDs.
    /// Non-element values (integers, strings, etc.) are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to multiple vertices
    /// let anon = Traversal::<Value, Value>::new().has_ids([VertexId(1), VertexId(2)]);
    /// let vertices = g.v().append(anon).to_list();
    /// ```
    pub fn has_ids<I, T>(self, ids: I) -> Traversal<In, Value>
    where
        I: IntoIterator<Item = T>,
        T: Into<Value>,
    {
        self.add_step(filter::HasIdStep::from_values(
            ids.into_iter().map(Into::into).collect(),
        ))
    }

    // -------------------------------------------------------------------------
    // Navigation steps (for anonymous traversals)
    // -------------------------------------------------------------------------

    /// Traverse to outgoing adjacent vertices (for anonymous traversals).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().out();
    /// let neighbors = g.v().append(anon).to_list();
    /// ```
    pub fn out(self) -> Traversal<In, Value> {
        self.add_step(navigation::OutStep::new())
    }

    /// Traverse to outgoing adjacent vertices via edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().out_labels(&["knows"]);
    /// let friends = g.v().append(anon).to_list();
    /// ```
    pub fn out_labels(self, labels: &[&str]) -> Traversal<In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(navigation::OutStep::with_labels(labels))
    }

    /// Traverse to incoming adjacent vertices (for anonymous traversals).
    ///
    /// Note: Named `in_` to avoid conflict with Rust's `in` keyword.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().in_();
    /// let known_by = g.v().append(anon).to_list();
    /// ```
    pub fn in_(self) -> Traversal<In, Value> {
        self.add_step(navigation::InStep::new())
    }

    /// Traverse to incoming adjacent vertices via edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().in_labels(&["knows"]);
    /// let known_by = g.v().append(anon).to_list();
    /// ```
    pub fn in_labels(self, labels: &[&str]) -> Traversal<In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(navigation::InStep::with_labels(labels))
    }

    /// Traverse to adjacent vertices in both directions (for anonymous traversals).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().both();
    /// let neighbors = g.v().append(anon).to_list();
    /// ```
    pub fn both(self) -> Traversal<In, Value> {
        self.add_step(navigation::BothStep::new())
    }

    /// Traverse to adjacent vertices in both directions via edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().both_labels(&["knows"]);
    /// let connected = g.v().append(anon).to_list();
    /// ```
    pub fn both_labels(self, labels: &[&str]) -> Traversal<In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(navigation::BothStep::with_labels(labels))
    }

    /// Traverse to outgoing edges (for anonymous traversals).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().out_e();
    /// let edges = g.v().append(anon).to_list();
    /// ```
    pub fn out_e(self) -> Traversal<In, Value> {
        self.add_step(navigation::OutEStep::new())
    }

    /// Traverse to outgoing edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().out_e_labels(&["knows"]);
    /// let edges = g.v().append(anon).to_list();
    /// ```
    pub fn out_e_labels(self, labels: &[&str]) -> Traversal<In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(navigation::OutEStep::with_labels(labels))
    }

    /// Traverse to incoming edges (for anonymous traversals).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().in_e();
    /// let edges = g.v().append(anon).to_list();
    /// ```
    pub fn in_e(self) -> Traversal<In, Value> {
        self.add_step(navigation::InEStep::new())
    }

    /// Traverse to incoming edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().in_e_labels(&["knows"]);
    /// let edges = g.v().append(anon).to_list();
    /// ```
    pub fn in_e_labels(self, labels: &[&str]) -> Traversal<In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(navigation::InEStep::with_labels(labels))
    }

    /// Traverse to all incident edges (for anonymous traversals).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().both_e();
    /// let edges = g.v().append(anon).to_list();
    /// ```
    pub fn both_e(self) -> Traversal<In, Value> {
        self.add_step(navigation::BothEStep::new())
    }

    /// Traverse to all incident edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().both_e_labels(&["knows"]);
    /// let edges = g.v().append(anon).to_list();
    /// ```
    pub fn both_e_labels(self, labels: &[&str]) -> Traversal<In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(navigation::BothEStep::with_labels(labels))
    }

    /// Get the source vertex of an edge (for anonymous traversals).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().out_v();
    /// let sources = g.e().append(anon).to_list();
    /// ```
    pub fn out_v(self) -> Traversal<In, Value> {
        self.add_step(navigation::OutVStep::new())
    }

    /// Get the target vertex of an edge (for anonymous traversals).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().in_v();
    /// let targets = g.e().append(anon).to_list();
    /// ```
    pub fn in_v(self) -> Traversal<In, Value> {
        self.add_step(navigation::InVStep::new())
    }

    /// Get both vertices of an edge (for anonymous traversals).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().both_v();
    /// let vertices = g.e().append(anon).to_list();
    /// ```
    pub fn both_v(self) -> Traversal<In, Value> {
        self.add_step(navigation::BothVStep::new())
    }

    /// Get the "other" vertex of an edge (for anonymous traversals).
    ///
    /// When traversing from a vertex to an edge, `other_v()` returns the
    /// vertex at the opposite end from where the traverser came from.
    /// Requires path tracking to be enabled.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().other_v();
    /// let others = g.v().out_e().append(anon).to_list();
    /// ```
    pub fn other_v(self) -> Traversal<In, Value> {
        self.add_step(navigation::OtherVStep::new())
    }

    // -------------------------------------------------------------------------
    // Transform steps (for anonymous traversals)
    // -------------------------------------------------------------------------

    /// Extract property values from vertices/edges (for anonymous traversals).
    ///
    /// For each input element, extracts the value of the specified property.
    /// Missing properties are silently skipped.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().values("name");
    /// let names = g.v().has_label("person").append(anon).to_list();
    /// ```
    pub fn values(self, key: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(transform::ValuesStep::new(key))
    }

    /// Extract multiple property values from vertices/edges (for anonymous traversals).
    ///
    /// For each input element, extracts the values of the specified properties.
    /// Missing properties are silently skipped.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().values_multi(["name", "age"]);
    /// let data = g.v().append(anon).to_list();
    /// ```
    pub fn values_multi<I, S>(self, keys: I) -> Traversal<In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.add_step(transform::ValuesStep::from_keys(keys))
    }

    /// Extract all property objects from vertices/edges (for anonymous traversals).
    ///
    /// Unlike `values()` which returns just property values, `properties()` returns
    /// the full property including its key as a Map with "key" and "value" entries.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().properties();
    /// let props = g.v().has_label("person").append(anon).to_list();
    /// // Each result is Value::Map { "key": "name", "value": "Alice" } etc.
    /// ```
    pub fn properties(self) -> Traversal<In, Value> {
        self.add_step(transform::PropertiesStep::new())
    }

    /// Extract specific property objects from vertices/edges (for anonymous traversals).
    ///
    /// Unlike `values()` which returns just property values, `properties_keys()` returns
    /// the full property including its key as a Map with "key" and "value" entries.
    /// Only the specified property keys are extracted.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().properties_keys(&["name", "age"]);
    /// let props = g.v().append(anon).to_list();
    /// ```
    pub fn properties_keys(self, keys: &[&str]) -> Traversal<In, Value> {
        let keys: Vec<String> = keys.iter().map(|s| s.to_string()).collect();
        self.add_step(transform::PropertiesStep::with_keys(keys))
    }

    /// Get all properties as a map (for anonymous traversals).
    ///
    /// Transforms each element into a `Value::Map` containing all its properties.
    /// Property values are wrapped in `Value::List` for multi-property compatibility.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().value_map();
    /// let maps = g.v().append(anon).to_list();
    /// // Returns: [{"name": ["Alice"], "age": [30]}, ...]
    /// ```
    pub fn value_map(self) -> Traversal<In, Value> {
        self.add_step(transform::ValueMapStep::new())
    }

    /// Get specific properties as a map (for anonymous traversals).
    ///
    /// Transforms each element into a `Value::Map` containing only the
    /// specified properties. Property values are wrapped in `Value::List`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().value_map_keys(&["name"]);
    /// let maps = g.v().append(anon).to_list();
    /// // Returns: [{"name": ["Alice"]}, {"name": ["Bob"]}]
    /// ```
    pub fn value_map_keys<I, S>(self, keys: I) -> Traversal<In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.add_step(transform::ValueMapStep::from_keys(keys))
    }

    /// Get all properties as a map including id and label tokens (for anonymous traversals).
    ///
    /// Like `value_map()`, but also includes "id" and "label" entries.
    /// The id and label are NOT wrapped in lists, but property values are.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().value_map_with_tokens();
    /// let maps = g.v().append(anon).to_list();
    /// // Returns: [{"id": 0, "label": "person", "name": ["Alice"], "age": [30]}]
    /// ```
    pub fn value_map_with_tokens(self) -> Traversal<In, Value> {
        self.add_step(transform::ValueMapStep::new().with_tokens())
    }

    /// Get complete element representation as a map (for anonymous traversals).
    ///
    /// Transforms each element into a `Value::Map` with id, label, and all
    /// properties. Unlike `value_map()`, property values are NOT wrapped in lists.
    /// For edges, also includes "IN" and "OUT" vertex references.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().element_map();
    /// let maps = g.v().append(anon).to_list();
    /// // Returns: [{"id": 0, "label": "person", "name": "Alice", "age": 30}]
    /// ```
    pub fn element_map(self) -> Traversal<In, Value> {
        self.add_step(transform::ElementMapStep::new())
    }

    /// Get element representation with specific properties (for anonymous traversals).
    ///
    /// Like `element_map()`, but includes only the specified properties
    /// along with the id, label, and (for edges) IN/OUT references.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().element_map_keys(&["name"]);
    /// let maps = g.v().append(anon).to_list();
    /// // Returns: [{"id": 0, "label": "person", "name": "Alice"}]
    /// ```
    pub fn element_map_keys<I, S>(self, keys: I) -> Traversal<In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.add_step(transform::ElementMapStep::from_keys(keys))
    }

    /// Get all properties as a map of property objects (for anonymous traversals).
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
    /// let anon = Traversal::<Value, Value>::new().property_map();
    /// let maps = g.v().append(anon).to_list();
    /// // Returns: [{name: [{key: "name", value: "Alice"}], age: [{key: "age", value: 30}]}]
    /// ```
    pub fn property_map(self) -> Traversal<In, Value> {
        self.add_step(transform::PropertyMapStep::new())
    }

    /// Get specific properties as a map of property objects (for anonymous traversals).
    ///
    /// Like `property_map()`, but includes only the specified properties.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().property_map_keys(&["name"]);
    /// let maps = g.v().append(anon).to_list();
    /// // Returns: [{name: [{key: "name", value: "Alice"}]}]
    /// ```
    pub fn property_map_keys<I, S>(self, keys: I) -> Traversal<In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.add_step(transform::PropertyMapStep::from_keys(keys))
    }

    /// Unroll collections into individual elements (for anonymous traversals).
    ///
    /// This step expands `Value::List` and `Value::Map` into separate traversers:
    /// - `Value::List`: Each element becomes a separate traverser
    /// - `Value::Map`: Each key-value pair becomes a single-entry map traverser
    /// - Non-collection values pass through unchanged
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Unfold a list
    /// let anon = Traversal::<Value, Value>::new().unfold();
    /// let items = g.inject([Value::List(vec![Value::Int(1), Value::Int(2)])])
    ///     .append(anon)
    ///     .to_list();
    /// // Results: [Value::Int(1), Value::Int(2)]
    ///
    /// // Round-trip: fold then unfold
    /// let original = g.v().fold().unfold().to_list();
    /// // Returns original vertices
    /// ```
    pub fn unfold(self) -> Traversal<In, Value> {
        self.add_step(transform::UnfoldStep::new())
    }

    /// Calculate the arithmetic mean (average) of numeric values.
    ///
    /// This is a **barrier step** - it collects ALL input values before producing
    /// a single output. Only numeric values (`Value::Int` and `Value::Float`) are
    /// included in the calculation; non-numeric values are silently ignored.
    ///
    /// # Behavior
    ///
    /// - Collects all numeric values from input traversers
    /// - `Value::Int` values are converted to `f64` for calculation
    /// - `Value::Float` values are used directly
    /// - Non-numeric values (strings, booleans, vertices, etc.) are ignored
    /// - Returns `Value::Float` with the mean if any numeric values exist
    /// - Returns empty (no output) if no numeric values are found
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Calculate average age of all people
    /// let avg_age = g.v().has_label("person").values("age").mean().next();
    ///
    /// // Mixed values - non-numeric ignored
    /// let avg = g.inject(vec![Value::Int(1), Value::Int(2), Value::String("three".into())])
    ///     .mean().next(); // Returns Some(Value::Float(1.5))
    /// ```
    pub fn mean(self) -> Traversal<In, Value> {
        self.add_step(transform::MeanStep::new())
    }

    /// Sort traversers using a fluent builder.
    ///
    /// This is a **barrier step** - it collects ALL input before producing sorted output.
    /// Returns an `OrderBuilder` that allows chaining multiple sort keys using `by` methods.
    ///
    /// # Behavior
    ///
    /// - Collects all input traversers (barrier)
    /// - Sorts according to configured keys
    /// - Multiple `by` clauses create multi-level sorts
    /// - Supports sorting by:
    ///   - Natural order of current value
    ///   - Property values from vertices/edges
    ///   - Results of sub-traversals
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Sort by natural order ascending (default)
    /// let sorted = g.v().values("name").order().build().to_list();
    ///
    /// // Sort by property descending
    /// let sorted = g.v().has_label("person")
    ///     .order().by_key_desc("age").build()
    ///     .to_list();
    ///
    /// // Multi-level sort: by age desc, then name asc
    /// let sorted = g.v().has_label("person")
    ///     .order()
    ///     .by_key_desc("age")
    ///     .by_key_asc("name")
    ///     .build()
    ///     .to_list();
    /// ```
    pub fn order(self) -> transform::OrderBuilder<In> {
        let (_, steps) = self.into_steps();
        transform::OrderBuilder::new(steps)
    }

    /// Evaluate a mathematical expression (for anonymous traversals).
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
    /// // Double current values
    /// g.v().values("age").math("_ * 2").build()
    ///
    /// // Calculate difference between labeled values
    /// g.v().as_("a").out("knows").as_("b")
    ///     .math("a - b")
    ///     .by("a", "age")
    ///     .by("b", "age")
    ///     .build()
    ///
    /// // Complex expression with functions
    /// g.v().values("x").math("sqrt(_ ^ 2 + 1)").build()
    /// ```
    #[cfg(feature = "gql")]
    pub fn math(self, expression: &str) -> transform::MathBuilder<In> {
        let (_, steps) = self.into_steps();
        transform::MathBuilder::new(steps, expression)
    }

    /// Create a projection with named keys (for anonymous traversals).
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
    /// use __; // Anonymous traversal module
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
    /// A `ProjectBuilder` that requires `by()` clauses to be added for each key.
    pub fn project(self, keys: &[&str]) -> transform::ProjectBuilder<In> {
        let (_, steps) = self.into_steps();
        let key_strings: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        transform::ProjectBuilder::new(steps, key_strings)
    }

    /// Group traversers by a key and collect values (for anonymous traversals).
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
    /// use __; // Anonymous traversal module
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
    /// A `GroupBuilder` that allows configuring the grouping key and value collector.
    pub fn group(self) -> aggregate::GroupBuilder<In> {
        let (_, steps) = self.into_steps();
        aggregate::GroupBuilder::new(steps)
    }

    /// Count traversers grouped by a key (for anonymous traversals).
    ///
    /// Creates a `GroupCountBuilder` that allows specifying how to group and count
    /// the traversers. The result is a single `Value::Map` where keys are the
    /// grouping keys and values are integer counts.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::*;
    /// use interstellar::traversal::__;
    ///
    /// // Count vertices by label
    /// let t = __.v().group_count().by_label().build();
    ///
    /// // Count vertices by a property
    /// let t2 = __.v().group_count().by_key("age").build();
    /// ```
    ///
    /// # Returns
    ///
    /// A `GroupCountBuilder` that allows configuring the grouping key.
    pub fn group_count(self) -> aggregate::GroupCountBuilder<In> {
        let (_, steps) = self.into_steps();
        aggregate::GroupCountBuilder::new(steps)
    }

    /// Extract the ID from vertices/edges (for anonymous traversals).
    ///
    /// For each input element, extracts its ID as a `Value::Int`.
    /// Non-element values are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().id();
    /// let ids = g.v().has_label("person").append(anon).to_list();
    /// ```
    pub fn id(self) -> Traversal<In, Value> {
        self.add_step(transform::IdStep::new())
    }

    /// Extract the label from vertices/edges (for anonymous traversals).
    ///
    /// For each input element, extracts its label as a `Value::String`.
    /// Non-element values are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().label();
    /// let labels = g.v().append(anon).to_list();
    /// ```
    pub fn label(self) -> Traversal<In, Value> {
        self.add_step(transform::LabelStep::new())
    }

    /// Transform each value using a closure (for anonymous traversals).
    ///
    /// The closure receives the execution context and the current value,
    /// returning a new value. This is a 1:1 mapping.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that doubles integer values
    /// let anon = Traversal::<Value, Value>::new()
    ///     .map(|_ctx, v| {
    ///         if let Value::Int(n) = v {
    ///             Value::Int(n * 2)
    ///         } else {
    ///             v.clone()
    ///         }
    ///     });
    /// let doubled = g.inject([1i64, 2i64]).append(anon).to_list();
    /// ```
    pub fn map<F>(self, f: F) -> Traversal<In, Value>
    where
        F: Fn(&context::ExecutionContext, &Value) -> Value + Clone + Send + Sync + 'static,
    {
        self.add_step(transform::MapStep::new(f))
    }

    /// Transform each value to multiple values using a closure (for anonymous traversals).
    ///
    /// The closure receives the execution context and the current value,
    /// returning a `Vec<Value>`. This is a 1:N mapping - each input can
    /// produce zero or more outputs.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that generates ranges
    /// let anon = Traversal::<Value, Value>::new()
    ///     .flat_map(|_ctx, v| {
    ///         if let Value::Int(n) = v {
    ///             (0..*n).map(|i| Value::Int(i)).collect()
    ///         } else {
    ///             vec![]
    ///         }
    ///     });
    /// let expanded = g.inject([3i64]).append(anon).to_list();
    /// // Results: [0, 1, 2]
    /// ```
    pub fn flat_map<F>(self, f: F) -> Traversal<In, Value>
    where
        F: Fn(&context::ExecutionContext, &Value) -> Vec<Value> + Clone + Send + Sync + 'static,
    {
        self.add_step(transform::FlatMapStep::new(f))
    }

    /// Replace each traverser's value with a constant (for anonymous traversals).
    ///
    /// For each input traverser, replaces the value with the specified constant.
    /// All traverser metadata (path, loops, bulk, sack) is preserved.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that replaces values with "found"
    /// let anon = Traversal::<Value, Value>::new().constant("found");
    /// let results = g.v().append(anon).to_list();
    /// // All results: Value::String("found")
    ///
    /// // With numeric constant
    /// let anon = Traversal::<Value, Value>::new().constant(42i64);
    /// ```
    pub fn constant(self, value: impl Into<Value>) -> Traversal<In, Value> {
        self.add_step(transform::ConstantStep::new(value))
    }

    /// Convert the traverser's path to a Value::List (for anonymous traversals).
    ///
    /// Replaces the traverser's value with a list containing all elements
    /// from its path history. Each path element is converted to its
    /// corresponding Value representation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that returns the path
    /// let anon = Traversal::<Value, Value>::new().out().path();
    /// let paths = g.v().append(anon).to_list();
    /// // Each result is a Value::List of path elements
    /// ```
    pub fn path(self) -> Traversal<In, Value> {
        self.add_step(transform::PathStep::new())
    }

    /// Label the current position in the traversal path (for anonymous traversals).
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
    /// // Create an anonymous traversal with labeled positions
    /// let anon = Traversal::<Value, Value>::new()
    ///     .as_("start").out().as_("end").select(&["start", "end"]);
    /// let results = g.v().append(anon).to_list();
    /// ```
    pub fn as_(self, label: &str) -> Traversal<In, Value> {
        self.add_step(transform::AsStep::new(label))
    }

    /// Select multiple labeled values from the path (for anonymous traversals).
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
    /// // Create an anonymous traversal that selects labeled values
    /// let anon = Traversal::<Value, Value>::new()
    ///     .as_("a").out().as_("b").select(&["a", "b"]);
    /// let results = g.v().append(anon).to_list();
    /// // Returns Map { "a" -> vertex1, "b" -> vertex2 }
    /// ```
    pub fn select(self, labels: &[&str]) -> Traversal<In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(transform::SelectStep::new(labels))
    }

    /// Select a single labeled value from the path (for anonymous traversals).
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
    /// // Create an anonymous traversal that selects a single labeled value
    /// let anon = Traversal::<Value, Value>::new()
    ///     .as_("x").out().select_one("x");
    /// let results = g.v().append(anon).to_list();
    /// // Returns the labeled vertex directly (not a Map)
    /// ```
    pub fn select_one(self, label: &str) -> Traversal<In, Value> {
        self.add_step(transform::SelectStep::single(label))
    }

    // -------------------------------------------------------------------------
    // Filter steps using anonymous traversals
    // -------------------------------------------------------------------------

    /// Filter by sub-traversal existence (for anonymous traversals).
    ///
    /// Emits input traverser only if the sub-traversal produces at least one result.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters by sub-traversal
    /// let anon = Traversal::<Value, Value>::new()
    ///     .where_(__.out());
    /// let with_out = g.v().append(anon).to_list();
    /// ```
    pub fn where_(self, sub: Traversal<Value, Value>) -> Traversal<In, Value> {
        self.add_step(branch::WhereStep::new(sub))
    }

    /// Filter by sub-traversal non-existence (for anonymous traversals).
    ///
    /// Emits input traverser only if the sub-traversal produces NO results.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters out vertices with outgoing edges
    /// let anon = Traversal::<Value, Value>::new()
    ///     .not(__.out());
    /// let leaves = g.v().append(anon).to_list();
    /// ```
    pub fn not(self, sub: Traversal<Value, Value>) -> Traversal<In, Value> {
        self.add_step(branch::NotStep::new(sub))
    }

    /// Filter by multiple sub-traversals (AND logic) (for anonymous traversals).
    ///
    /// Emits input traverser only if ALL sub-traversals produce at least one result.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that requires both conditions
    /// let anon = Traversal::<Value, Value>::new()
    ///     .and_(vec![__.out(), __.in_()]);
    /// let connected = g.v().append(anon).to_list();
    /// ```
    pub fn and_(self, subs: Vec<Traversal<Value, Value>>) -> Traversal<In, Value> {
        self.add_step(branch::AndStep::new(subs))
    }

    /// Filter by multiple sub-traversals (OR logic) (for anonymous traversals).
    ///
    /// Emits input traverser if ANY sub-traversal produces at least one result.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that accepts either condition
    /// let anon = Traversal::<Value, Value>::new()
    ///     .or_(vec![__.has_label("person"), __.has_label("software")]);
    /// let entities = g.v().append(anon).to_list();
    /// ```
    pub fn or_(self, subs: Vec<Traversal<Value, Value>>) -> Traversal<In, Value> {
        self.add_step(branch::OrStep::new(subs))
    }

    // -------------------------------------------------------------------------
    // Branch steps using anonymous traversals
    // -------------------------------------------------------------------------

    /// Execute multiple branches and merge results (for anonymous traversals).
    ///
    /// All branches receive each input traverser; results are merged.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that executes multiple branches
    /// let anon = Traversal::<Value, Value>::new()
    ///     .union(vec![__.out(), __.in_()]);
    /// let neighbors = g.v().append(anon).to_list();
    /// ```
    pub fn union(self, branches: Vec<Traversal<Value, Value>>) -> Traversal<In, Value> {
        self.add_step(branch::UnionStep::new(branches))
    }

    /// Try branches in order, return first non-empty result (for anonymous traversals).
    ///
    /// Short-circuits on first successful branch.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that tries branches in order
    /// let anon = Traversal::<Value, Value>::new()
    ///     .coalesce(vec![__.values("nickname"), __.values("name")]);
    /// let names = g.v().append(anon).to_list();
    /// ```
    pub fn coalesce(self, branches: Vec<Traversal<Value, Value>>) -> Traversal<In, Value> {
        self.add_step(branch::CoalesceStep::new(branches))
    }

    /// Conditional branching (for anonymous traversals).
    ///
    /// Evaluates condition; if it produces results, executes if_true, otherwise if_false.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal with conditional branching
    /// let anon = Traversal::<Value, Value>::new()
    ///     .choose(__.has_label("person"), __.out_labels(&["knows"]), __.out());
    /// let results = g.v().append(anon).to_list();
    /// ```
    pub fn choose(
        self,
        condition: Traversal<Value, Value>,
        if_true: Traversal<Value, Value>,
        if_false: Traversal<Value, Value>,
    ) -> Traversal<In, Value> {
        self.add_step(branch::ChooseStep::new(condition, if_true, if_false))
    }

    /// Optional traversal with fallback to input (for anonymous traversals).
    ///
    /// If sub-traversal produces results, emit those; otherwise emit input.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal with optional step
    /// let anon = Traversal::<Value, Value>::new()
    ///     .optional(__.out_labels(&["knows"]));
    /// let results = g.v().append(anon).to_list();
    /// ```
    pub fn optional(self, sub: Traversal<Value, Value>) -> Traversal<In, Value> {
        self.add_step(branch::OptionalStep::new(sub))
    }

    /// Execute sub-traversal in isolated scope (for anonymous traversals).
    ///
    /// Aggregations operate independently for each input traverser.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal with local scope
    /// let anon = Traversal::<Value, Value>::new()
    ///     .local(__.out().limit(1));
    /// let results = g.v().append(anon).to_list();
    /// ```
    pub fn local(self, sub: Traversal<Value, Value>) -> Traversal<In, Value> {
        self.add_step(branch::LocalStep::new(sub))
    }

    // -------------------------------------------------------------------------
    // Mutation Steps
    // -------------------------------------------------------------------------

    /// Add or update a property on the current element.
    ///
    /// This step modifies the current traverser's element (vertex or edge)
    /// by setting a property value. For pending vertex/edge creations,
    /// the property is accumulated. For existing elements, a pending
    /// mutation is created.
    ///
    /// The actual property update happens when the traversal is executed
    /// via `MutationExecutor`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Chain properties after add_v
    /// let vertex = g.add_v("person")
    ///     .property("name", "Alice")
    ///     .property("age", 30);
    ///
    /// // Update properties on existing vertices
    /// let updated = g.v_id(id).property("status", "active");
    /// ```
    pub fn property(self, key: impl Into<String>, value: impl Into<Value>) -> Traversal<In, Value> {
        self.add_step(mutation::PropertyStep::new(key, value))
    }

    /// Delete the current element (vertex or edge).
    ///
    /// When a vertex is dropped, all its incident edges are also dropped.
    /// The actual deletion happens when the traversal is executed via
    /// `MutationExecutor`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Drop specific vertices
    /// let deleted = g.v_id(id).drop();
    ///
    /// // Drop vertices matching criteria
    /// let deleted = g.v().has_label("temp").drop();
    /// ```
    pub fn drop(self) -> Traversal<In, Value> {
        self.add_step(mutation::DropStep::new())
    }

    // -------------------------------------------------------------------------
    // Side Effect Steps (for anonymous traversals)
    // -------------------------------------------------------------------------

    /// Store traverser values in a side-effect collection (for anonymous traversals).
    ///
    /// This is a **lazy step** - values are stored as they pass through the iterator,
    /// not all at once. The traverser values pass through unchanged.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that stores values
    /// let anon = Traversal::<Value, Value>::new()
    ///     .out()
    ///     .store("neighbors");
    /// ```
    pub fn store(self, key: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(sideeffect::StoreStep::new(key))
    }

    /// Aggregate all traverser values into a side-effect collection (for anonymous traversals).
    ///
    /// This is a **barrier step** - it collects ALL values before continuing.
    /// All input traversers are collected, stored, then re-emitted.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that aggregates values
    /// let anon = Traversal::<Value, Value>::new()
    ///     .out()
    ///     .aggregate("all_neighbors");
    /// ```
    pub fn aggregate(self, key: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(sideeffect::AggregateStep::new(key))
    }

    /// Retrieve side-effect data by key (for anonymous traversals).
    ///
    /// For a single key, returns the collection as a `Value::List`.
    /// Consumes all input traversers before producing the result.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that retrieves stored data
    /// let anon = Traversal::<Value, Value>::new()
    ///     .store("x")
    ///     .cap("x");
    /// ```
    pub fn cap(self, key: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(sideeffect::CapStep::new(key))
    }

    /// Retrieve multiple side-effect collections as a map (for anonymous traversals).
    ///
    /// Returns a `Value::Map` with keys being the collection names
    /// and values being `Value::List` of the stored items.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that retrieves multiple collections
    /// let anon = Traversal::<Value, Value>::new()
    ///     .store("x")
    ///     .store("y")
    ///     .cap_multi(["x", "y"]);
    /// ```
    pub fn cap_multi<I, S>(self, keys: I) -> Traversal<In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.add_step(sideeffect::CapStep::multi(keys))
    }

    /// Execute a sub-traversal for its side effects (for anonymous traversals).
    ///
    /// The sub-traversal is executed for each input traverser, but its output
    /// is discarded. The original traverser passes through unchanged.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that executes a side effect
    /// let anon = Traversal::<Value, Value>::new()
    ///     .side_effect(__.out().store("neighbors"));
    /// ```
    pub fn side_effect(self, traversal: Traversal<Value, Value>) -> Traversal<In, Value> {
        self.add_step(sideeffect::SideEffectStep::new(traversal))
    }

    /// Profile the traversal step timing and counts (for anonymous traversals).
    ///
    /// Records the number of traversers and elapsed time in milliseconds
    /// to the side-effects under the default key "~profile".
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal with profiling
    /// let anon = Traversal::<Value, Value>::new()
    ///     .out()
    ///     .profile();
    /// ```
    pub fn profile(self) -> Traversal<In, Value> {
        self.add_step(sideeffect::ProfileStep::new())
    }

    /// Profile the traversal with a custom key (for anonymous traversals).
    ///
    /// Like `profile()`, but stores data under the specified key instead
    /// of the default "~profile".
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal with custom profile key
    /// let anon = Traversal::<Value, Value>::new()
    ///     .out()
    ///     .profile_as("out_step_profile");
    /// ```
    pub fn profile_as(self, key: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(sideeffect::ProfileStep::with_key(key))
    }
}
