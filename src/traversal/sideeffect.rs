//! Side effect steps for graph traversals.
//!
//! Side effect steps perform operations that don't change the traversal stream
//! but produce side effects such as storing values in collections.
//!
//! # Steps Provided
//!
//! - [`StoreStep`]: Lazily store each value as it passes through (not a barrier)
//! - [`AggregateStep`]: Collect all values before continuing (barrier step)
//! - [`CapStep`]: Retrieve accumulated side-effect data
//! - [`SideEffectStep`]: Execute a traversal for side effects only
//! - [`ProfileStep`]: Collect traversal timing and count metrics
//!
//! # Example
//!
//! ```ignore
//! // Store all visited vertices
//! let result = g.v()
//!     .store("visited")
//!     .out()
//!     .store("neighbors")
//!     .to_list();
//!
//! // Retrieve stored values
//! let visited = g.ctx().side_effects.get("visited");
//!
//! // Use aggregate (barrier) then cap to retrieve
//! let result = g.v().aggregate("all").cap("all").next();
//! ```

use std::cell::Cell;
use std::collections::HashMap;
use std::time::Instant;

use crate::traversal::context::ExecutionContext;
use crate::traversal::step::{execute_traversal_from, AnyStep};
use crate::traversal::{Traversal, Traverser};
use crate::value::Value;

// -----------------------------------------------------------------------------
// StoreStep - lazily store values as they pass through
// -----------------------------------------------------------------------------

/// Store each traverser value into a named side-effect collection (lazy).
///
/// Unlike `aggregate()` (when implemented), `store()` is not a barrier - values are stored
/// as they pass through, and traversers continue immediately. This means values
/// are stored incrementally as the traversal proceeds.
///
/// # Behavior
///
/// - Each traverser's value is stored in the named collection
/// - The traverser passes through unchanged
/// - Values are stored in the order they are encountered
/// - Multiple stores to the same key append to the collection
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
/// // Store vertex names as we traverse
/// let names = g.v()
///     .has_label("person")
///     .store("people")
///     .values("name")
///     .to_list();
///
/// // The "people" collection now contains all person vertices
/// let people = ctx.side_effects.get("people");
/// ```
#[derive(Clone, Debug)]
pub struct StoreStep {
    key: String,
}

impl StoreStep {
    /// Create a new StoreStep that stores values under the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The name of the side-effect collection to store values in
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = StoreStep::new("collected");
    /// ```
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }

    /// Get the key this step stores values under.
    #[inline]
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl AnyStep for StoreStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let key = self.key.clone();

        Box::new(input.inspect(move |t| {
            // Store the value in the side effects collection
            ctx.side_effects.store(&key, t.value.clone());
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "store"
    }
}

// -----------------------------------------------------------------------------
// AggregateStep - barrier step that collects all values
// -----------------------------------------------------------------------------

/// Collect all traverser values into a named side-effect collection (barrier).
///
/// This is a **barrier step** - it collects ALL input traversers before
/// allowing any to continue. This is useful when you need all values to be
/// stored before subsequent steps execute.
///
/// # Behavior
///
/// - Collects all input traversers into memory
/// - Stores all values in the named collection
/// - Re-emits all traversers in their original order
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
///
/// // All person vertices are now in "people" collection
/// ```
#[derive(Clone, Debug)]
pub struct AggregateStep {
    key: String,
}

impl AggregateStep {
    /// Create a new AggregateStep that stores values under the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The name of the side-effect collection to store values in
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = AggregateStep::new("collected");
    /// ```
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }

    /// Get the key this step stores values under.
    #[inline]
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl AnyStep for AggregateStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Barrier: collect all traversers first
        let traversers: Vec<Traverser> = input.collect();

        // Store all values
        for t in &traversers {
            ctx.side_effects.store(&self.key, t.value.clone());
        }

        // Re-emit all traversers
        Box::new(traversers.into_iter())
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "aggregate"
    }
}

// -----------------------------------------------------------------------------
// CapStep - retrieve accumulated side-effect data
// -----------------------------------------------------------------------------

/// Retrieve accumulated side-effect data.
///
/// Single key returns `Value::List`, multiple keys return `Value::Map`.
/// This step consumes the input stream and produces a single traverser
/// containing the side-effect data.
///
/// # Behavior
///
/// - Consumes all input traversers (to ensure side effects are populated)
/// - For single key: returns `Value::List` with stored values
/// - For multiple keys: returns `Value::Map` with key-to-list mappings
/// - Missing keys return empty lists
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().store("x").cap("x")
/// g.V().store("x").store("y").cap("x", "y")
/// ```
///
/// # Example
///
/// ```ignore
/// // Single key - returns List
/// let stored = g.v().store("all").cap("all").next();
///
/// // Multiple keys - returns Map
/// let stored = g.v()
///     .store("vertices")
///     .out_e().store("edges")
///     .cap_multi(&["vertices", "edges"])
///     .next();
/// ```
#[derive(Clone, Debug)]
pub struct CapStep {
    keys: Vec<String>,
}

impl CapStep {
    /// Create a CapStep for a single key.
    ///
    /// # Arguments
    ///
    /// * `key` - The side-effect collection key to retrieve
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = CapStep::new("stored");
    /// ```
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }

    /// Create a CapStep for multiple keys.
    ///
    /// When multiple keys are provided, the result is a `Value::Map`
    /// with each key mapping to its stored values.
    ///
    /// # Arguments
    ///
    /// * `keys` - The side-effect collection keys to retrieve
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = CapStep::multi(["vertices", "edges"]);
    /// ```
    pub fn multi<I, S>(keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }

    /// Get the keys this step retrieves.
    pub fn keys(&self) -> &[String] {
        &self.keys
    }
}

impl AnyStep for CapStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Consume input to ensure all side effects populated
        input.for_each(drop);

        let result = if self.keys.len() == 1 {
            // Single key: return list
            let values = ctx.side_effects.get(&self.keys[0]).unwrap_or_default();
            Value::List(values)
        } else {
            // Multiple keys: return map
            let mut map = HashMap::new();
            for key in &self.keys {
                let values = ctx.side_effects.get(key).unwrap_or_default();
                map.insert(key.clone(), Value::List(values));
            }
            Value::Map(map)
        };

        Box::new(std::iter::once(Traverser::new(result)))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "cap"
    }
}

// -----------------------------------------------------------------------------
// SideEffectStep - execute a traversal for side effects only
// -----------------------------------------------------------------------------

/// Execute a traversal for side effects only.
///
/// The sub-traversal is executed for each input traverser, but its output
/// is discarded. The original traverser passes through unchanged. This is
/// useful for executing operations that have side effects without changing
/// the main traversal stream.
///
/// # Behavior
///
/// - For each input traverser, executes the sub-traversal
/// - Discards all output from the sub-traversal
/// - Passes the original traverser through unchanged
/// - Side effects from the sub-traversal are recorded
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
/// // Store counts as side effect while traversing
/// let names = g.v()
///     .side_effect(__::out_e().count().store("edge_counts"))
///     .values("name")
///     .to_list();
/// ```
#[derive(Clone)]
pub struct SideEffectStep {
    side_traversal: Traversal<Value, Value>,
}

impl SideEffectStep {
    /// Create a new SideEffectStep with the given sub-traversal.
    ///
    /// # Arguments
    ///
    /// * `side_traversal` - The traversal to execute for side effects
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = SideEffectStep::new(__::out().count().store("x"));
    /// ```
    pub fn new(side_traversal: Traversal<Value, Value>) -> Self {
        Self { side_traversal }
    }
}

impl std::fmt::Debug for SideEffectStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SideEffectStep")
            .field(
                "side_traversal",
                &format!("<{} steps>", self.side_traversal.step_count()),
            )
            .finish()
    }
}

impl AnyStep for SideEffectStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let side_traversal = self.side_traversal.clone();

        Box::new(input.inspect(move |t| {
            // Execute side-effect traversal (discard results)
            let side_input = Box::new(std::iter::once(t.clone()));
            execute_traversal_from(ctx, &side_traversal, side_input).for_each(drop);
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "sideEffect"
    }
}

// -----------------------------------------------------------------------------
// ProfileStep - collect traversal profiling information
// -----------------------------------------------------------------------------

/// Collect traversal profiling information.
///
/// This step records the count of traversers and elapsed time as they pass
/// through. The profile data is stored in the side effects when the iterator
/// is exhausted.
///
/// # Behavior
///
/// - Traversers pass through unchanged
/// - Counts each traverser as it passes
/// - Records elapsed time when the iterator completes
/// - Stores a `Value::Map` with "count" and "time_ms" entries
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
/// let result = g.v().out().profile_as("step1").to_list();
///
/// // Retrieve profile data
/// let profile = ctx.side_effects.get("step1");
/// // Contains: {"count": 10, "time_ms": 1.5}
/// ```
#[derive(Clone, Debug, Default)]
pub struct ProfileStep {
    key: Option<String>,
}

impl ProfileStep {
    /// Create a ProfileStep with auto-generated key ("profile").
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = ProfileStep::new();
    /// ```
    pub fn new() -> Self {
        Self { key: None }
    }

    /// Create a ProfileStep with a specific key.
    ///
    /// # Arguments
    ///
    /// * `key` - The side-effect key to store profile data under
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = ProfileStep::with_key("my_profile");
    /// ```
    pub fn with_key(key: impl Into<String>) -> Self {
        Self {
            key: Some(key.into()),
        }
    }

    /// Get the key this step stores profile data under.
    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }
}

impl AnyStep for ProfileStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let key = self.key.clone().unwrap_or_else(|| "profile".to_string());

        Box::new(ProfileIterator {
            inner: input,
            ctx,
            key,
            count: Cell::new(0),
            start: Instant::now(),
            finished: Cell::new(false),
        })
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "profile"
    }
}

/// Iterator wrapper that collects profiling information.
struct ProfileIterator<'a, I> {
    inner: I,
    ctx: &'a ExecutionContext<'a>,
    key: String,
    count: Cell<u64>,
    start: Instant,
    finished: Cell<bool>,
}

impl<'a, I: Iterator<Item = Traverser>> Iterator for ProfileIterator<'a, I> {
    type Item = Traverser;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(t) => {
                self.count.set(self.count.get() + 1);
                Some(t)
            }
            None => {
                if !self.finished.get() {
                    self.finished.set(true);
                    let elapsed = self.start.elapsed();
                    let profile = Value::Map({
                        let mut m = HashMap::new();
                        m.insert("count".to_string(), Value::Int(self.count.get() as i64));
                        m.insert(
                            "time_ms".to_string(),
                            Value::Float(elapsed.as_secs_f64() * 1000.0),
                        );
                        m
                    });
                    self.ctx.side_effects.store(&self.key, profile);
                }
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::storage::InMemoryGraph;
    use crate::value::{Value, VertexId};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_graph() -> Graph {
        let mut storage = InMemoryGraph::new();

        storage.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props.insert("age".to_string(), Value::Int(30));
            props
        });
        storage.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Bob".to_string()));
            props.insert("age".to_string(), Value::Int(25));
            props
        });
        storage.add_vertex("software", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Graph DB".to_string()));
            props
        });

        Graph::new(storage)
    }

    mod store_step_tests {
        use super::*;

        #[test]
        fn store_step_new_creates_step_with_key() {
            let step = StoreStep::new("test_key");
            assert_eq!(step.key(), "test_key");
        }

        #[test]
        fn store_step_new_accepts_string() {
            let step = StoreStep::new(String::from("my_collection"));
            assert_eq!(step.key(), "my_collection");
        }

        #[test]
        fn store_step_name_is_store() {
            let step = StoreStep::new("x");
            assert_eq!(step.name(), "store");
        }

        #[test]
        fn store_step_is_cloneable() {
            let step = StoreStep::new("test");
            let cloned = step.clone();
            assert_eq!(cloned.key(), "test");
        }

        #[test]
        fn store_step_clone_box() {
            let step = StoreStep::new("test");
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "store");
        }

        #[test]
        fn store_step_debug_output() {
            let step = StoreStep::new("my_key");
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("StoreStep"));
            assert!(debug_str.contains("my_key"));
        }

        #[test]
        fn store_step_stores_values_in_side_effects() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = StoreStep::new("collected");

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            // Apply the step and consume the iterator
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Check values were stored
            let stored = ctx.side_effects.get("collected").unwrap();
            assert_eq!(stored.len(), 3);
            assert_eq!(stored[0], Value::Int(1));
            assert_eq!(stored[1], Value::Int(2));
            assert_eq!(stored[2], Value::Int(3));

            // Check traversers passed through
            assert_eq!(output.len(), 3);
        }

        #[test]
        fn store_step_passes_traversers_through_unchanged() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = StoreStep::new("x");

            // Create traverser with metadata
            let mut t = Traverser::from_vertex(VertexId(42));
            t.extend_path_labeled("start");
            t.loops = 5;
            t.bulk = 10;

            let input = vec![t];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Vertex(VertexId(42)));
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn store_step_handles_empty_input() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = StoreStep::new("empty");
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
            // Key should not exist (nothing was stored)
            assert!(ctx.side_effects.get("empty").is_none());
        }

        #[test]
        fn store_step_stores_multiple_values_sequentially() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = StoreStep::new("items");

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::String("first".to_string())),
                Traverser::new(Value::String("second".to_string())),
                Traverser::new(Value::String("third".to_string())),
            ];

            let _output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            let stored = ctx.side_effects.get("items").unwrap();
            assert_eq!(stored.len(), 3);
            // Verify order is preserved
            assert_eq!(stored[0], Value::String("first".to_string()));
            assert_eq!(stored[1], Value::String("second".to_string()));
            assert_eq!(stored[2], Value::String("third".to_string()));
        }

        #[test]
        fn store_step_stores_various_value_types() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = StoreStep::new("mixed");

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(42)),
                Traverser::new(Value::String("hello".to_string())),
                Traverser::new(Value::Bool(true)),
                Traverser::new(Value::Float(3.14)),
                Traverser::new(Value::Vertex(VertexId(1))),
                Traverser::new(Value::Null),
            ];

            let _output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            let stored = ctx.side_effects.get("mixed").unwrap();
            assert_eq!(stored.len(), 6);
            assert_eq!(stored[0], Value::Int(42));
            assert_eq!(stored[1], Value::String("hello".to_string()));
            assert_eq!(stored[2], Value::Bool(true));
            assert_eq!(stored[3], Value::Float(3.14));
            assert_eq!(stored[4], Value::Vertex(VertexId(1)));
            assert_eq!(stored[5], Value::Null);
        }

        #[test]
        fn store_step_multiple_stores_to_same_key_append() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // First store
            let step1 = StoreStep::new("data");
            let input1 = vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];
            let _: Vec<_> = step1.apply(&ctx, Box::new(input1.into_iter())).collect();

            // Second store (same key)
            let step2 = StoreStep::new("data");
            let input2 = vec![Traverser::new(Value::Int(3)), Traverser::new(Value::Int(4))];
            let _: Vec<_> = step2.apply(&ctx, Box::new(input2.into_iter())).collect();

            let stored = ctx.side_effects.get("data").unwrap();
            assert_eq!(stored.len(), 4);
            assert_eq!(stored[0], Value::Int(1));
            assert_eq!(stored[1], Value::Int(2));
            assert_eq!(stored[2], Value::Int(3));
            assert_eq!(stored[3], Value::Int(4));
        }

        #[test]
        fn store_step_is_lazy_not_barrier() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = StoreStep::new("lazy_test");
            let input = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            // Create iterator but only partially consume it
            let mut iter = step.apply(&ctx, Box::new(input.into_iter()));

            // Take first element
            let first = iter.next();
            assert!(first.is_some());

            // After consuming first element, only first value should be stored
            let stored_after_first = ctx.side_effects.get("lazy_test").unwrap();
            assert_eq!(stored_after_first.len(), 1);
            assert_eq!(stored_after_first[0], Value::Int(1));

            // Take second element
            let _ = iter.next();

            // Now two values should be stored
            let stored_after_second = ctx.side_effects.get("lazy_test").unwrap();
            assert_eq!(stored_after_second.len(), 2);

            // Consume rest
            let _ = iter.collect::<Vec<_>>();

            // All three should be stored
            let stored_final = ctx.side_effects.get("lazy_test").unwrap();
            assert_eq!(stored_final.len(), 3);
        }

        #[test]
        fn store_step_can_be_used_as_any_step() {
            let step: Box<dyn AnyStep> = Box::new(StoreStep::new("test"));
            assert_eq!(step.name(), "store");
        }

        #[test]
        fn store_step_can_be_stored_in_vec_with_other_steps() {
            use crate::traversal::step::IdentityStep;

            let steps: Vec<Box<dyn AnyStep>> = vec![
                Box::new(IdentityStep::new()),
                Box::new(StoreStep::new("collected")),
                Box::new(IdentityStep::new()),
            ];

            assert_eq!(steps.len(), 3);
            assert_eq!(steps[0].name(), "identity");
            assert_eq!(steps[1].name(), "store");
            assert_eq!(steps[2].name(), "identity");
        }

        #[test]
        fn store_step_different_keys_store_independently() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Store to key "a"
            let step_a = StoreStep::new("a");
            let input_a = vec![Traverser::new(Value::Int(1))];
            let _: Vec<_> = step_a.apply(&ctx, Box::new(input_a.into_iter())).collect();

            // Store to key "b"
            let step_b = StoreStep::new("b");
            let input_b = vec![Traverser::new(Value::Int(2))];
            let _: Vec<_> = step_b.apply(&ctx, Box::new(input_b.into_iter())).collect();

            let stored_a = ctx.side_effects.get("a").unwrap();
            let stored_b = ctx.side_effects.get("b").unwrap();

            assert_eq!(stored_a.len(), 1);
            assert_eq!(stored_a[0], Value::Int(1));
            assert_eq!(stored_b.len(), 1);
            assert_eq!(stored_b[0], Value::Int(2));
        }
    }

    mod aggregate_step_tests {
        use super::*;

        #[test]
        fn aggregate_step_new_creates_step_with_key() {
            let step = AggregateStep::new("test_key");
            assert_eq!(step.key(), "test_key");
        }

        #[test]
        fn aggregate_step_name_is_aggregate() {
            let step = AggregateStep::new("x");
            assert_eq!(step.name(), "aggregate");
        }

        #[test]
        fn aggregate_step_is_cloneable() {
            let step = AggregateStep::new("test");
            let cloned = step.clone();
            assert_eq!(cloned.key(), "test");
        }

        #[test]
        fn aggregate_step_clone_box() {
            let step = AggregateStep::new("test");
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "aggregate");
        }

        #[test]
        fn aggregate_step_stores_all_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = AggregateStep::new("collected");

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Check values were stored
            let stored = ctx.side_effects.get("collected").unwrap();
            assert_eq!(stored.len(), 3);
            assert_eq!(stored[0], Value::Int(1));
            assert_eq!(stored[1], Value::Int(2));
            assert_eq!(stored[2], Value::Int(3));

            // Check traversers passed through
            assert_eq!(output.len(), 3);
        }

        #[test]
        fn aggregate_step_is_barrier() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = AggregateStep::new("barrier_test");
            let input = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            // Create iterator
            let mut iter = step.apply(&ctx, Box::new(input.into_iter()));

            // Before consuming any output, all values should already be stored
            // because aggregate is a barrier that collects all input first
            let stored_before = ctx.side_effects.get("barrier_test").unwrap();
            assert_eq!(stored_before.len(), 3);

            // Now consume the iterator
            let _ = iter.next();
            let _ = iter.next();
            let _ = iter.next();

            // Still 3 values (no duplicates added)
            let stored_after = ctx.side_effects.get("barrier_test").unwrap();
            assert_eq!(stored_after.len(), 3);
        }

        #[test]
        fn aggregate_step_handles_empty_input() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = AggregateStep::new("empty");
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
            assert!(ctx.side_effects.get("empty").is_none());
        }

        #[test]
        fn aggregate_step_passes_traversers_through_unchanged() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = AggregateStep::new("x");

            let mut t = Traverser::from_vertex(VertexId(42));
            t.extend_path_labeled("start");
            t.loops = 5;
            t.bulk = 10;

            let input = vec![t];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Vertex(VertexId(42)));
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }
    }

    mod cap_step_tests {
        use super::*;

        #[test]
        fn cap_step_new_creates_single_key() {
            let step = CapStep::new("test");
            assert_eq!(step.keys(), &["test"]);
        }

        #[test]
        fn cap_step_multi_creates_multiple_keys() {
            let step = CapStep::multi(["a", "b", "c"]);
            assert_eq!(step.keys(), &["a", "b", "c"]);
        }

        #[test]
        fn cap_step_name_is_cap() {
            let step = CapStep::new("x");
            assert_eq!(step.name(), "cap");
        }

        #[test]
        fn cap_step_is_cloneable() {
            let step = CapStep::new("test");
            let cloned = step.clone();
            assert_eq!(cloned.keys(), &["test"]);
        }

        #[test]
        fn cap_step_clone_box() {
            let step = CapStep::new("test");
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "cap");
        }

        #[test]
        fn cap_step_single_key_returns_list() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Pre-store some values
            ctx.side_effects.store("items", Value::Int(1));
            ctx.side_effects.store("items", Value::Int(2));
            ctx.side_effects.store("items", Value::Int(3));

            let step = CapStep::new("items");
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            match &output[0].value {
                Value::List(values) => {
                    assert_eq!(values.len(), 3);
                    assert_eq!(values[0], Value::Int(1));
                    assert_eq!(values[1], Value::Int(2));
                    assert_eq!(values[2], Value::Int(3));
                }
                _ => panic!("Expected List"),
            }
        }

        #[test]
        fn cap_step_multiple_keys_returns_map() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Pre-store values in different keys
            ctx.side_effects.store("vertices", Value::Int(1));
            ctx.side_effects.store("vertices", Value::Int(2));
            ctx.side_effects.store("edges", Value::Int(10));

            let step = CapStep::multi(["vertices", "edges"]);
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            match &output[0].value {
                Value::Map(map) => {
                    assert!(map.contains_key("vertices"));
                    assert!(map.contains_key("edges"));

                    if let Value::List(v) = map.get("vertices").unwrap() {
                        assert_eq!(v.len(), 2);
                    } else {
                        panic!("Expected List for vertices");
                    }

                    if let Value::List(e) = map.get("edges").unwrap() {
                        assert_eq!(e.len(), 1);
                    } else {
                        panic!("Expected List for edges");
                    }
                }
                _ => panic!("Expected Map"),
            }
        }

        #[test]
        fn cap_step_missing_key_returns_empty_list() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = CapStep::new("nonexistent");
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            match &output[0].value {
                Value::List(values) => {
                    assert!(values.is_empty());
                }
                _ => panic!("Expected List"),
            }
        }

        #[test]
        fn cap_step_consumes_input_stream() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Create a store step and chain with cap
            let store_step = StoreStep::new("items");
            let cap_step = CapStep::new("items");

            let input = vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];

            // Apply store step
            let after_store = store_step.apply(&ctx, Box::new(input.into_iter()));

            // Apply cap step (should consume store output first)
            let output: Vec<Traverser> = cap_step.apply(&ctx, after_store).collect();

            // Cap should have consumed the store step output, triggering storage
            assert_eq!(output.len(), 1);
            match &output[0].value {
                Value::List(values) => {
                    assert_eq!(values.len(), 2);
                }
                _ => panic!("Expected List"),
            }
        }

        #[test]
        fn cap_step_with_empty_input_still_returns_result() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Pre-store values
            ctx.side_effects.store("x", Value::Int(42));

            let step = CapStep::new("x");
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            match &output[0].value {
                Value::List(values) => {
                    assert_eq!(values.len(), 1);
                    assert_eq!(values[0], Value::Int(42));
                }
                _ => panic!("Expected List"),
            }
        }
    }

    mod side_effect_step_tests {
        use super::*;

        #[test]
        fn side_effect_step_name_is_side_effect() {
            let step = SideEffectStep::new(Traversal::new());
            assert_eq!(step.name(), "sideEffect");
        }

        #[test]
        fn side_effect_step_is_cloneable() {
            let step = SideEffectStep::new(Traversal::new());
            let _cloned = step.clone();
        }

        #[test]
        fn side_effect_step_clone_box() {
            let step = SideEffectStep::new(Traversal::new());
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "sideEffect");
        }

        #[test]
        fn side_effect_step_passes_traversers_through() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Side effect that does nothing
            let step = SideEffectStep::new(Traversal::new());

            let input = vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(2));
        }

        #[test]
        fn side_effect_step_executes_sub_traversal() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Side effect that stores values
            let side_traversal =
                Traversal::<Value, Value>::new().add_step(StoreStep::new("side_stored"));
            let step = SideEffectStep::new(side_traversal);

            let input = vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Original traversers pass through
            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(2));

            // Side effect stored values
            let stored = ctx.side_effects.get("side_stored").unwrap();
            assert_eq!(stored.len(), 2);
            assert_eq!(stored[0], Value::Int(1));
            assert_eq!(stored[1], Value::Int(2));
        }

        #[test]
        fn side_effect_step_handles_empty_input() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let side_traversal =
                Traversal::<Value, Value>::new().add_step(StoreStep::new("empty_side"));
            let step = SideEffectStep::new(side_traversal);

            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
            assert!(ctx.side_effects.get("empty_side").is_none());
        }

        #[test]
        fn side_effect_step_preserves_traverser_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = SideEffectStep::new(Traversal::new());

            let mut t = Traverser::from_vertex(VertexId(42));
            t.extend_path_labeled("start");
            t.loops = 5;
            t.bulk = 10;

            let input = vec![t];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Vertex(VertexId(42)));
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn side_effect_step_debug_output() {
            let step = SideEffectStep::new(Traversal::new());
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("SideEffectStep"));
        }
    }

    mod profile_step_tests {
        use super::*;

        #[test]
        fn profile_step_new_uses_default_key() {
            let step = ProfileStep::new();
            assert_eq!(step.key(), None);
        }

        #[test]
        fn profile_step_with_key_uses_custom_key() {
            let step = ProfileStep::with_key("my_profile");
            assert_eq!(step.key(), Some("my_profile"));
        }

        #[test]
        fn profile_step_name_is_profile() {
            let step = ProfileStep::new();
            assert_eq!(step.name(), "profile");
        }

        #[test]
        fn profile_step_is_cloneable() {
            let step = ProfileStep::with_key("test");
            let cloned = step.clone();
            assert_eq!(cloned.key(), Some("test"));
        }

        #[test]
        fn profile_step_clone_box() {
            let step = ProfileStep::new();
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "profile");
        }

        #[test]
        fn profile_step_default_impl() {
            let step = ProfileStep::default();
            assert_eq!(step.key(), None);
        }

        #[test]
        fn profile_step_records_count() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ProfileStep::with_key("count_test");

            let input = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            // Consume all output
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);

            // Check profile data
            let profile = ctx.side_effects.get("count_test").unwrap();
            assert_eq!(profile.len(), 1);

            if let Value::Map(map) = &profile[0] {
                assert_eq!(map.get("count"), Some(&Value::Int(3)));
            } else {
                panic!("Expected Map");
            }
        }

        #[test]
        fn profile_step_records_time_ms() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ProfileStep::with_key("time_test");

            let input = vec![Traverser::new(Value::Int(1))];

            let _: Vec<_> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            let profile = ctx.side_effects.get("time_test").unwrap();

            if let Value::Map(map) = &profile[0] {
                if let Some(Value::Float(time_ms)) = map.get("time_ms") {
                    // Time should be non-negative
                    assert!(*time_ms >= 0.0);
                } else {
                    panic!("Expected time_ms to be Float");
                }
            } else {
                panic!("Expected Map");
            }
        }

        #[test]
        fn profile_step_uses_default_key_when_none_specified() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ProfileStep::new(); // Uses default key "profile"

            let input = vec![Traverser::new(Value::Int(1))];

            let _: Vec<_> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Should be stored under "profile"
            let profile = ctx.side_effects.get("profile").unwrap();
            assert_eq!(profile.len(), 1);
        }

        #[test]
        fn profile_step_handles_empty_input() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ProfileStep::with_key("empty_test");

            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());

            // Profile should still be recorded with count=0
            let profile = ctx.side_effects.get("empty_test").unwrap();
            assert_eq!(profile.len(), 1);

            if let Value::Map(map) = &profile[0] {
                assert_eq!(map.get("count"), Some(&Value::Int(0)));
            } else {
                panic!("Expected Map");
            }
        }

        #[test]
        fn profile_step_passes_traversers_through_unchanged() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ProfileStep::new();

            let mut t = Traverser::from_vertex(VertexId(42));
            t.extend_path_labeled("start");
            t.loops = 5;
            t.bulk = 10;

            let input = vec![t];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Vertex(VertexId(42)));
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn profile_step_only_records_once_on_exhaustion() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ProfileStep::with_key("once_test");

            let input = vec![Traverser::new(Value::Int(1))];

            let mut iter = step.apply(&ctx, Box::new(input.into_iter()));

            // Consume all elements
            let _ = iter.next();

            // Profile not recorded yet (still have to call next() to get None)
            // Actually the profile is recorded when we get None, let's call next again
            let _ = iter.next(); // This returns None and records profile

            // Call next multiple times after exhaustion
            let _ = iter.next();
            let _ = iter.next();

            // Profile should only be recorded once
            let profile = ctx.side_effects.get("once_test").unwrap();
            assert_eq!(profile.len(), 1);
        }

        #[test]
        fn profile_step_debug_output() {
            let step = ProfileStep::with_key("debug_test");
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("ProfileStep"));
            assert!(debug_str.contains("debug_test"));
        }
    }
}
