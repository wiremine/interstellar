use std::cmp::Ordering as CmpOrdering;
use std::marker::PhantomData;

use crate::traversal::step::{execute_traversal_from, AnyStep};
use crate::traversal::{ExecutionContext, Traversal, Traverser};
use crate::value::Value;

// -----------------------------------------------------------------------------
// Order - sort direction
// -----------------------------------------------------------------------------

/// Sort direction for ordering.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Order {
    /// Ascending order
    Asc,
    /// Descending order
    Desc,
}

// -----------------------------------------------------------------------------
// OrderKey - what to order by
// -----------------------------------------------------------------------------

/// Specification of what to order by.
#[derive(Clone)]
pub enum OrderKey {
    /// Order by the traverser's current value (natural order)
    Natural(Order),
    /// Order by a property value
    Property(String, Order),
    /// Order by the result of a sub-traversal
    Traversal(Traversal<Value, Value>, Order),
}

// -----------------------------------------------------------------------------
// OrderStep - barrier step that sorts traversers
// -----------------------------------------------------------------------------

/// Barrier step that sorts all input traversers.
///
/// This is a **barrier step** - it collects ALL input before producing sorted output.
/// Supports sorting by:
/// - Natural order of the current value
/// - Property values from vertices/edges
/// - Results of sub-traversals
///
/// Multiple sort keys can be specified for multi-level sorting.
///
/// # Memory Warning
///
/// **This step requires O(n) memory** where n is the total number of input
/// traversers. For very large traversals, this may cause significant memory
/// usage. Sorting inherently requires all elements to be collected before
/// any output can be produced. Consider using `limit()` before `order()` if
/// you only need the top N elements, or use range-based filtering to reduce
/// input size.
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().order().by("age", desc)  // Sort by age descending
/// g.V().values("name").order()   // Sort names ascending
/// g.V().order().by(out().count(), desc)  // Sort by out-degree
/// ```
///
/// # Example
///
/// ```ignore
/// // Sort vertices by age (ascending)
/// let sorted = g.v().has_label("person")
///     .order().by_key_asc("age").build()
///     .to_list();
///
/// // Sort by natural order (descending)
/// let sorted = g.v().values("name")
///     .order().by_desc().build()
///     .to_list();
/// ```
#[derive(Clone)]
pub struct OrderStep {
    keys: Vec<OrderKey>,
}

impl OrderStep {
    /// Create a new OrderStep with natural ascending order.
    pub fn new() -> Self {
        Self {
            keys: vec![OrderKey::Natural(Order::Asc)],
        }
    }

    /// Create an OrderStep with natural order.
    pub fn by_natural(order: Order) -> Self {
        Self {
            keys: vec![OrderKey::Natural(order)],
        }
    }

    /// Create an OrderStep sorting by a property.
    pub fn by_property(key: impl Into<String>, order: Order) -> Self {
        Self {
            keys: vec![OrderKey::Property(key.into(), order)],
        }
    }

    /// Create an OrderStep with custom keys.
    pub fn with_keys(keys: Vec<OrderKey>) -> Self {
        Self { keys }
    }

    /// Compare two traversers according to the configured sort keys.
    fn compare(&self, ctx: &ExecutionContext, a: &Traverser, b: &Traverser) -> CmpOrdering {
        for key in &self.keys {
            let ord = match key {
                OrderKey::Natural(order) => {
                    let cmp = Self::compare_values(&a.value, &b.value);
                    Self::apply_order(cmp, *order)
                }
                OrderKey::Property(prop, order) => {
                    let va = self.get_property(ctx, a, prop);
                    let vb = self.get_property(ctx, b, prop);
                    let cmp = Self::compare_option_values(&va, &vb);
                    Self::apply_order(cmp, *order)
                }
                OrderKey::Traversal(sub, order) => {
                    let va = self.execute_for_sort(ctx, a, sub);
                    let vb = self.execute_for_sort(ctx, b, sub);
                    let cmp = Self::compare_option_values(&va, &vb);
                    Self::apply_order(cmp, *order)
                }
            };

            if ord != CmpOrdering::Equal {
                return ord;
            }
        }

        CmpOrdering::Equal
    }

    /// Compare two values.
    #[inline]
    fn compare_values(a: &Value, b: &Value) -> CmpOrdering {
        match (a, b) {
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(CmpOrdering::Equal),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            // Mixed types - compare by discriminant
            (Value::Int(_), _) => CmpOrdering::Less,
            (_, Value::Int(_)) => CmpOrdering::Greater,
            (Value::Float(_), _) => CmpOrdering::Less,
            (_, Value::Float(_)) => CmpOrdering::Greater,
            (Value::String(_), _) => CmpOrdering::Less,
            (_, Value::String(_)) => CmpOrdering::Greater,
            (Value::Bool(_), _) => CmpOrdering::Less,
            (_, Value::Bool(_)) => CmpOrdering::Greater,
            _ => CmpOrdering::Equal,
        }
    }

    /// Compare two optional values.
    #[inline]
    fn compare_option_values(a: &Option<Value>, b: &Option<Value>) -> CmpOrdering {
        match (a, b) {
            (Some(a), Some(b)) => Self::compare_values(a, b),
            (Some(_), None) => CmpOrdering::Less, // Present values come first
            (None, Some(_)) => CmpOrdering::Greater,
            (None, None) => CmpOrdering::Equal,
        }
    }

    /// Apply sort order to a comparison result.
    #[inline]
    fn apply_order(ord: CmpOrdering, order: Order) -> CmpOrdering {
        match order {
            Order::Asc => ord,
            Order::Desc => ord.reverse(),
        }
    }

    /// Get a property value from a traverser.
    fn get_property(&self, ctx: &ExecutionContext, t: &Traverser, key: &str) -> Option<Value> {
        match &t.value {
            Value::Vertex(id) => ctx
                .snapshot()
                .storage()
                .get_vertex(*id)
                .and_then(|v| v.properties.get(key).cloned()),
            Value::Edge(id) => ctx
                .snapshot()
                .storage()
                .get_edge(*id)
                .and_then(|e| e.properties.get(key).cloned()),
            _ => None,
        }
    }

    /// Execute a sub-traversal for sorting.
    fn execute_for_sort(
        &self,
        ctx: &ExecutionContext,
        t: &Traverser,
        sub: &Traversal<Value, Value>,
    ) -> Option<Value> {
        execute_traversal_from(ctx, sub, Box::new(std::iter::once(t.clone())))
            .next()
            .map(|t| t.value)
    }
}

impl Default for OrderStep {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyStep for OrderStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Collect all input (barrier)
        let mut traversers: Vec<_> = input.collect();

        // Sort
        traversers.sort_by(|a, b| self.compare(ctx, a, b));

        Box::new(traversers.into_iter())
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "order"
    }
}

// -----------------------------------------------------------------------------
// OrderBuilder - fluent API for building OrderStep
// -----------------------------------------------------------------------------

/// Fluent builder for creating OrderStep with multiple sort keys.
///
/// The builder allows chaining multiple `by` clauses to create multi-level sorts.
///
/// # Example
///
/// ```ignore
/// // Sort by age descending, then by name ascending
/// let sorted = g.v().has_label("person")
///     .order()
///     .by_key_desc("age")
///     .by_key_asc("name")
///     .build()
///     .to_list();
/// ```
pub struct OrderBuilder<In> {
    steps: Vec<Box<dyn AnyStep>>,
    order_keys: Vec<OrderKey>,
    _phantom: PhantomData<In>,
}

impl<In> OrderBuilder<In> {
    /// Create a new OrderBuilder with existing steps.
    pub(crate) fn new(steps: Vec<Box<dyn AnyStep>>) -> Self {
        Self {
            steps,
            order_keys: vec![],
            _phantom: PhantomData,
        }
    }

    /// Sort by natural order ascending.
    pub fn by_asc(mut self) -> Self {
        self.order_keys.push(OrderKey::Natural(Order::Asc));
        self
    }

    /// Sort by natural order descending.
    pub fn by_desc(mut self) -> Self {
        self.order_keys.push(OrderKey::Natural(Order::Desc));
        self
    }

    /// Sort by property value ascending.
    pub fn by_key_asc(mut self, key: &str) -> Self {
        self.order_keys
            .push(OrderKey::Property(key.to_string(), Order::Asc));
        self
    }

    /// Sort by property value descending.
    pub fn by_key_desc(mut self, key: &str) -> Self {
        self.order_keys
            .push(OrderKey::Property(key.to_string(), Order::Desc));
        self
    }

    /// Sort by sub-traversal result.
    pub fn by_traversal(mut self, t: Traversal<Value, Value>, desc: bool) -> Self {
        let order = if desc { Order::Desc } else { Order::Asc };
        self.order_keys.push(OrderKey::Traversal(t, order));
        self
    }

    /// Build the final traversal with the OrderStep.
    pub fn build(mut self) -> Traversal<In, Value> {
        // If no order keys were specified, default to natural ascending
        if self.order_keys.is_empty() {
            self.order_keys.push(OrderKey::Natural(Order::Asc));
        }

        let order_step = OrderStep::with_keys(self.order_keys);
        self.steps.push(Box::new(order_step));

        Traversal {
            steps: self.steps,
            source: None,
            _phantom: PhantomData,
        }
    }
}

// -----------------------------------------------------------------------------
// BoundOrderBuilder - fluent API for bound traversals
// -----------------------------------------------------------------------------

/// Fluent builder for creating OrderStep with multiple sort keys for bound traversals.
///
/// This builder is returned from `BoundTraversal::order()` and allows chaining
/// multiple `by` clauses before calling `build()` to get back a `BoundTraversal`.
///
/// # Example
///
/// ```ignore
/// // Sort by age descending, then by name ascending
/// let sorted = g.v().has_label("person")
///     .order()
///     .by_key_desc("age")
///     .by_key_asc("name")
///     .build()
///     .to_list();
/// ```
pub struct BoundOrderBuilder<'g, In> {
    snapshot: &'g crate::graph::GraphSnapshot<'g>,
    interner: &'g crate::storage::interner::StringInterner,
    source: Option<crate::traversal::TraversalSource>,
    steps: Vec<Box<dyn AnyStep>>,
    order_keys: Vec<OrderKey>,
    track_paths: bool,
    _phantom: PhantomData<In>,
}

impl<'g, In> BoundOrderBuilder<'g, In> {
    /// Create a new BoundOrderBuilder with existing steps and graph references.
    pub(crate) fn new(
        snapshot: &'g crate::graph::GraphSnapshot<'g>,
        interner: &'g crate::storage::interner::StringInterner,
        source: Option<crate::traversal::TraversalSource>,
        steps: Vec<Box<dyn AnyStep>>,
        track_paths: bool,
    ) -> Self {
        Self {
            snapshot,
            interner,
            source,
            steps,
            order_keys: vec![],
            track_paths,
            _phantom: PhantomData,
        }
    }

    /// Sort by natural order ascending.
    pub fn by_asc(mut self) -> Self {
        self.order_keys.push(OrderKey::Natural(Order::Asc));
        self
    }

    /// Sort by natural order descending.
    pub fn by_desc(mut self) -> Self {
        self.order_keys.push(OrderKey::Natural(Order::Desc));
        self
    }

    /// Sort by property value ascending.
    pub fn by_key_asc(mut self, key: &str) -> Self {
        self.order_keys
            .push(OrderKey::Property(key.to_string(), Order::Asc));
        self
    }

    /// Sort by property value descending.
    pub fn by_key_desc(mut self, key: &str) -> Self {
        self.order_keys
            .push(OrderKey::Property(key.to_string(), Order::Desc));
        self
    }

    /// Sort by sub-traversal result.
    pub fn by_traversal(mut self, t: Traversal<Value, Value>, desc: bool) -> Self {
        let order = if desc { Order::Desc } else { Order::Asc };
        self.order_keys.push(OrderKey::Traversal(t, order));
        self
    }

    /// Build the final bound traversal with the OrderStep.
    pub fn build(mut self) -> crate::traversal::source::BoundTraversal<'g, In, Value> {
        // If no order keys were specified, default to natural ascending
        if self.order_keys.is_empty() {
            self.order_keys.push(OrderKey::Natural(Order::Asc));
        }

        let order_step = OrderStep::with_keys(self.order_keys);
        self.steps.push(Box::new(order_step));

        let traversal = Traversal {
            steps: self.steps,
            source: self.source, // Preserve the source!
            _phantom: PhantomData,
        };

        // We need to preserve track_paths, so we'll add a helper to BoundTraversal
        // For now, use the private field constructor directly since we're in the same crate
        let mut bound =
            crate::traversal::source::BoundTraversal::new(self.snapshot, self.interner, traversal);

        // Preserve track_paths by conditionally calling with_path()
        if self.track_paths {
            bound = bound.with_path();
        }

        bound
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::storage::InMemoryGraph;
    use crate::value::VertexId;
    use std::collections::HashMap;

    fn create_test_graph() -> Graph {
        let storage = InMemoryGraph::new();
        Graph::new(storage)
    }

    fn create_sorted_graph() -> Graph {
        let mut storage = InMemoryGraph::new();

        // Add vertices with ages and names
        let mut props1 = HashMap::new();
        props1.insert("name".to_string(), Value::String("Alice".to_string()));
        props1.insert("age".to_string(), Value::Int(30));
        storage.add_vertex("person", props1);

        let mut props2 = HashMap::new();
        props2.insert("name".to_string(), Value::String("Bob".to_string()));
        props2.insert("age".to_string(), Value::Int(25));
        storage.add_vertex("person", props2);

        let mut props3 = HashMap::new();
        props3.insert("name".to_string(), Value::String("Charlie".to_string()));
        props3.insert("age".to_string(), Value::Int(35));
        storage.add_vertex("person", props3);

        Graph::new(storage)
    }

    mod order_step_construction {
        use super::*;

        #[test]
        fn test_new() {
            let step = OrderStep::new();
            assert_eq!(step.name(), "order");
            assert_eq!(step.keys.len(), 1);
            assert!(matches!(step.keys[0], OrderKey::Natural(Order::Asc)));
        }

        #[test]
        fn test_by_natural_asc() {
            let step = OrderStep::by_natural(Order::Asc);
            assert_eq!(step.keys.len(), 1);
            assert!(matches!(step.keys[0], OrderKey::Natural(Order::Asc)));
        }

        #[test]
        fn test_by_natural_desc() {
            let step = OrderStep::by_natural(Order::Desc);
            assert_eq!(step.keys.len(), 1);
            assert!(matches!(step.keys[0], OrderKey::Natural(Order::Desc)));
        }

        #[test]
        fn test_by_property() {
            let step = OrderStep::by_property("age", Order::Desc);
            assert_eq!(step.keys.len(), 1);
            if let OrderKey::Property(key, order) = &step.keys[0] {
                assert_eq!(key, "age");
                assert_eq!(*order, Order::Desc);
            } else {
                panic!("Expected Property order key");
            }
        }
    }

    mod order_step_natural_sort {
        use super::*;

        #[test]
        fn sorts_integers_ascending() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = OrderStep::by_natural(Order::Asc);
            let input = vec![
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(2));
            assert_eq!(output[2].value, Value::Int(3));
        }

        #[test]
        fn sorts_integers_descending() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = OrderStep::by_natural(Order::Desc);
            let input = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(2)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(3));
            assert_eq!(output[1].value, Value::Int(2));
            assert_eq!(output[2].value, Value::Int(1));
        }

        #[test]
        fn sorts_strings_ascending() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = OrderStep::by_natural(Order::Asc);
            let input = vec![
                Traverser::new(Value::String("Charlie".to_string())),
                Traverser::new(Value::String("Alice".to_string())),
                Traverser::new(Value::String("Bob".to_string())),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::String("Alice".to_string()));
            assert_eq!(output[1].value, Value::String("Bob".to_string()));
            assert_eq!(output[2].value, Value::String("Charlie".to_string()));
        }

        #[test]
        fn sorts_floats_ascending() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = OrderStep::by_natural(Order::Asc);
            let input = vec![
                Traverser::new(Value::Float(3.5)),
                Traverser::new(Value::Float(1.2)),
                Traverser::new(Value::Float(2.7)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Float(1.2));
            assert_eq!(output[1].value, Value::Float(2.7));
            assert_eq!(output[2].value, Value::Float(3.5));
        }
    }

    mod order_step_property_sort {
        use super::*;

        #[test]
        fn sorts_by_property_ascending() {
            let graph = create_sorted_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = OrderStep::by_property("age", Order::Asc);
            let input = vec![
                Traverser::from_vertex(VertexId(0)), // Alice, 30
                Traverser::from_vertex(VertexId(1)), // Bob, 25
                Traverser::from_vertex(VertexId(2)), // Charlie, 35
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            // Should be sorted by age: Bob (25), Alice (30), Charlie (35)
            assert_eq!(output[0].value, Value::Vertex(VertexId(1)));
            assert_eq!(output[1].value, Value::Vertex(VertexId(0)));
            assert_eq!(output[2].value, Value::Vertex(VertexId(2)));
        }

        #[test]
        fn sorts_by_property_descending() {
            let graph = create_sorted_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = OrderStep::by_property("age", Order::Desc);
            let input = vec![
                Traverser::from_vertex(VertexId(0)), // Alice, 30
                Traverser::from_vertex(VertexId(1)), // Bob, 25
                Traverser::from_vertex(VertexId(2)), // Charlie, 35
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            // Should be sorted by age desc: Charlie (35), Alice (30), Bob (25)
            assert_eq!(output[0].value, Value::Vertex(VertexId(2)));
            assert_eq!(output[1].value, Value::Vertex(VertexId(0)));
            assert_eq!(output[2].value, Value::Vertex(VertexId(1)));
        }
    }

    mod order_step_empty {
        use super::*;

        #[test]
        fn handles_empty_input() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = OrderStep::new();
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn handles_single_element() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = OrderStep::new();
            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(42));
        }
    }

    mod order_step_metadata {
        use super::*;

        #[test]
        fn preserves_paths() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = OrderStep::by_natural(Order::Asc);

            let mut t1 = Traverser::new(Value::Int(3));
            t1.extend_path_labeled("t1");

            let mut t2 = Traverser::new(Value::Int(1));
            t2.extend_path_labeled("t2");

            let input = vec![t1, t2];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            // After sorting, t2 (value 1) should be first
            assert!(output[0].path.has_label("t2"));
            assert!(output[1].path.has_label("t1"));
        }

        #[test]
        fn preserves_bulk() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = OrderStep::by_natural(Order::Asc);

            let mut t1 = Traverser::new(Value::Int(2));
            t1.bulk = 5;

            let mut t2 = Traverser::new(Value::Int(1));
            t2.bulk = 10;

            let input = vec![t1, t2];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            // After sorting, t2 should be first with bulk 10
            assert_eq!(output[0].bulk, 10);
            assert_eq!(output[1].bulk, 5);
        }
    }

    mod bound_traversal_integration {
        use super::*;

        #[test]
        fn bound_order_natural_ascending() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.traversal();

            let results = g
                .inject([Value::Int(3), Value::Int(1), Value::Int(2)])
                .order()
                .build()
                .to_list();

            assert_eq!(results.len(), 3);
            assert_eq!(results[0], Value::Int(1));
            assert_eq!(results[1], Value::Int(2));
            assert_eq!(results[2], Value::Int(3));
        }

        #[test]
        fn bound_order_natural_descending() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.traversal();

            let results = g
                .inject([
                    Value::String("a".to_string()),
                    Value::String("c".to_string()),
                    Value::String("b".to_string()),
                ])
                .order()
                .by_desc()
                .build()
                .to_list();

            assert_eq!(results.len(), 3);
            assert_eq!(results[0], Value::String("c".to_string()));
            assert_eq!(results[1], Value::String("b".to_string()));
            assert_eq!(results[2], Value::String("a".to_string()));
        }

        #[test]
        fn bound_order_by_property_ascending() {
            let graph = create_sorted_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.traversal();

            let results = g
                .v()
                .has_label("person")
                .order()
                .by_key_asc("age")
                .build()
                .to_list();

            // Extract ages to verify sorting
            let ages: Vec<i64> = results
                .iter()
                .filter_map(|v| {
                    if let Value::Vertex(id) = v {
                        snapshot.graph.storage.get_vertex(*id).and_then(|vertex| {
                            vertex.properties.get("age").and_then(|age| {
                                if let Value::Int(n) = age {
                                    Some(*n)
                                } else {
                                    None
                                }
                            })
                        })
                    } else {
                        None
                    }
                })
                .collect();

            assert_eq!(ages.len(), 3);
            assert_eq!(ages[0], 25); // Bob
            assert_eq!(ages[1], 30); // Alice
            assert_eq!(ages[2], 35); // Charlie
        }

        #[test]
        fn bound_order_by_property_descending() {
            let graph = create_sorted_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.traversal();

            let results = g
                .v()
                .has_label("person")
                .order()
                .by_key_desc("age")
                .build()
                .to_list();

            // Extract ages to verify sorting
            let ages: Vec<i64> = results
                .iter()
                .filter_map(|v| {
                    if let Value::Vertex(id) = v {
                        snapshot.graph.storage.get_vertex(*id).and_then(|vertex| {
                            vertex.properties.get("age").and_then(|age| {
                                if let Value::Int(n) = age {
                                    Some(*n)
                                } else {
                                    None
                                }
                            })
                        })
                    } else {
                        None
                    }
                })
                .collect();

            assert_eq!(ages.len(), 3);
            assert_eq!(ages[0], 35); // Charlie
            assert_eq!(ages[1], 30); // Alice
            assert_eq!(ages[2], 25); // Bob
        }

        #[test]
        fn bound_order_multi_level() {
            let mut storage = InMemoryGraph::new();

            // Add vertices with same age but different names
            let mut props1 = HashMap::new();
            props1.insert("name".to_string(), Value::String("Zara".to_string()));
            props1.insert("age".to_string(), Value::Int(30));
            storage.add_vertex("person", props1);

            let mut props2 = HashMap::new();
            props2.insert("name".to_string(), Value::String("Alice".to_string()));
            props2.insert("age".to_string(), Value::Int(30));
            storage.add_vertex("person", props2);

            let mut props3 = HashMap::new();
            props3.insert("name".to_string(), Value::String("Bob".to_string()));
            props3.insert("age".to_string(), Value::Int(25));
            storage.add_vertex("person", props3);

            let graph = Graph::new(storage);
            let snapshot = graph.snapshot();
            let g = snapshot.traversal();

            let results = g
                .v()
                .has_label("person")
                .order()
                .by_key_asc("age") // First by age ascending
                .by_key_asc("name") // Then by name ascending
                .build()
                .to_list();

            // Extract names to verify sorting
            let names: Vec<String> = results
                .iter()
                .filter_map(|v| {
                    if let Value::Vertex(id) = v {
                        snapshot.graph.storage.get_vertex(*id).and_then(|vertex| {
                            vertex.properties.get("name").and_then(|name| {
                                if let Value::String(s) = name {
                                    Some(s.clone())
                                } else {
                                    None
                                }
                            })
                        })
                    } else {
                        None
                    }
                })
                .collect();

            assert_eq!(names.len(), 3);
            assert_eq!(names[0], "Bob"); // age 25
            assert_eq!(names[1], "Alice"); // age 30, name "Alice"
            assert_eq!(names[2], "Zara"); // age 30, name "Zara"
        }

        #[test]
        fn bound_order_preserves_path_tracking() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.traversal();

            // Test that path tracking is preserved through the builder
            let results = g
                .inject([Value::Int(3), Value::Int(1), Value::Int(2)])
                .with_path()
                .order()
                .by_asc()
                .build()
                .to_list();

            assert_eq!(results.len(), 3);
            assert_eq!(results[0], Value::Int(1));
            assert_eq!(results[1], Value::Int(2));
            assert_eq!(results[2], Value::Int(3));
        }
    }
}
