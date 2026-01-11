//! Side effect steps for graph traversals.
//!
//! Side effect steps perform operations that don't change the traversal stream
//! but produce side effects such as storing values in collections.
//!
//! # Steps Provided
//!
//! - [`StoreStep`]: Lazily store each value as it passes through (not a barrier)
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
//! ```

use crate::traversal::context::ExecutionContext;
use crate::traversal::step::AnyStep;
use crate::traversal::Traverser;

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

        Graph::new(Arc::new(storage))
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
}
