//! Execution context for traversal operations.
//!
//! The `ExecutionContext` provides graph access at execution time, decoupling
//! traversal construction from graph binding. This is key to supporting
//! anonymous traversals - graph access is provided when the traversal executes,
//! not when it's constructed.

use std::any::Any;
use std::collections::HashMap;

use parking_lot::RwLock;

use crate::graph::GraphSnapshot;
use crate::storage::interner::StringInterner;
use crate::value::Value;

/// Execution context passed to steps at runtime.
///
/// This is the key to supporting anonymous traversals - graph access
/// is provided when the traversal executes, not when it's constructed.
///
/// # Example
///
/// ```ignore
/// let ctx = ExecutionContext::new(&snapshot, snapshot.interner());
/// let label_id = ctx.resolve_label("person");
/// ```
pub struct ExecutionContext<'g> {
    /// Graph snapshot for consistent reads
    snapshot: &'g GraphSnapshot<'g>,
    /// String interner for label lookups
    interner: &'g StringInterner,
    /// Side effects storage (for store(), aggregate(), etc.)
    pub side_effects: SideEffects,
}

impl<'g> ExecutionContext<'g> {
    /// Create a new execution context.
    ///
    /// # Arguments
    ///
    /// * `snapshot` - Graph snapshot for consistent reads
    /// * `interner` - String interner for label resolution
    pub fn new(snapshot: &'g GraphSnapshot<'g>, interner: &'g StringInterner) -> Self {
        Self {
            snapshot,
            interner,
            side_effects: SideEffects::new(),
        }
    }

    /// Get the graph snapshot.
    #[inline]
    pub fn snapshot(&self) -> &'g GraphSnapshot<'g> {
        self.snapshot
    }

    /// Get the string interner.
    #[inline]
    pub fn interner(&self) -> &'g StringInterner {
        self.interner
    }

    /// Resolve a label string to its interned ID.
    ///
    /// Returns `None` if the label has not been interned (i.e., doesn't exist
    /// in the graph).
    #[inline]
    pub fn resolve_label(&self, label: &str) -> Option<u32> {
        self.interner.lookup(label)
    }

    /// Resolve multiple labels to their interned IDs.
    ///
    /// Labels that don't exist are filtered out.
    pub fn resolve_labels(&self, labels: &[&str]) -> Vec<u32> {
        labels
            .iter()
            .filter_map(|l| self.interner.lookup(l))
            .collect()
    }

    /// Get label string from ID.
    #[inline]
    pub fn get_label(&self, id: u32) -> Option<&str> {
        self.interner.resolve(id)
    }
}

/// Storage for traversal side effects.
///
/// Used by steps like `store()`, `aggregate()`, `sack()`, etc.
///
/// # Thread Safety
///
/// Uses interior mutability via `RwLock` to allow mutation through
/// shared references (since `ExecutionContext` is passed as `&'a`).
/// This enables side-effect steps to accumulate data during traversal.
///
/// # Example
///
/// ```ignore
/// let side_effects = SideEffects::new();
/// side_effects.store("collected", Value::Int(42));
/// let values = side_effects.get("collected");
/// ```
#[derive(Default)]
pub struct SideEffects {
    /// Named collections of values
    collections: RwLock<HashMap<String, Vec<Value>>>,
    /// Arbitrary side effect data
    data: RwLock<HashMap<String, Box<dyn Any + Send + Sync>>>,
}

impl SideEffects {
    /// Create a new empty side effects store.
    pub fn new() -> Self {
        Self {
            collections: RwLock::new(HashMap::new()),
            data: RwLock::new(HashMap::new()),
        }
    }

    /// Store a value in a named collection.
    ///
    /// Values are appended to the collection (multiple values per key).
    pub fn store(&self, key: &str, value: Value) {
        self.collections
            .write()
            .entry(key.to_string())
            .or_default()
            .push(value);
    }

    /// Get values from a named collection (returns a clone).
    ///
    /// Returns `None` if the key doesn't exist.
    pub fn get(&self, key: &str) -> Option<Vec<Value>> {
        self.collections.read().get(key).cloned()
    }

    /// Get values from a named collection by reference (for iteration).
    ///
    /// Returns a guard that holds the read lock.
    ///
    /// # Note
    ///
    /// The returned guard holds the read lock. Use sparingly and drop
    /// the guard as soon as possible to avoid blocking other operations.
    pub fn get_ref(&self, key: &str) -> Option<parking_lot::MappedRwLockReadGuard<'_, Vec<Value>>> {
        let guard = self.collections.read();
        if guard.contains_key(key) {
            Some(parking_lot::RwLockReadGuard::map(guard, |m| {
                m.get(key).unwrap()
            }))
        } else {
            None
        }
    }

    /// Store arbitrary typed data.
    ///
    /// Overwrites any existing value with the same key.
    pub fn set_data<T: Any + Send + Sync>(&self, key: &str, value: T) {
        self.data.write().insert(key.to_string(), Box::new(value));
    }

    /// Get arbitrary typed data (clones if `T: Clone`).
    ///
    /// Returns `None` if the key doesn't exist or the type doesn't match.
    pub fn get_data<T: Any + Clone>(&self, key: &str) -> Option<T> {
        self.data
            .read()
            .get(key)
            .and_then(|v| v.downcast_ref::<T>())
            .cloned()
    }

    /// Check if a collection key exists.
    pub fn contains_key(&self, key: &str) -> bool {
        self.collections.read().contains_key(key)
    }

    /// Get the number of values in a collection.
    pub fn collection_len(&self, key: &str) -> usize {
        self.collections
            .read()
            .get(key)
            .map(|v| v.len())
            .unwrap_or(0)
    }

    /// Clear all side effects.
    pub fn clear(&self) {
        self.collections.write().clear();
        self.data.write().clear();
    }

    /// Get all collection keys.
    pub fn keys(&self) -> Vec<String> {
        self.collections.read().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn side_effects_new_is_empty() {
        let se = SideEffects::new();
        assert!(se.keys().is_empty());
        assert_eq!(se.get("nonexistent"), None);
    }

    #[test]
    fn side_effects_store_and_get() {
        let se = SideEffects::new();

        se.store("numbers", Value::Int(1));
        se.store("numbers", Value::Int(2));
        se.store("numbers", Value::Int(3));

        let values = se.get("numbers").unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], Value::Int(1));
        assert_eq!(values[1], Value::Int(2));
        assert_eq!(values[2], Value::Int(3));
    }

    #[test]
    fn side_effects_get_ref() {
        let se = SideEffects::new();
        se.store("items", Value::String("hello".to_string()));
        se.store("items", Value::String("world".to_string()));

        {
            let guard = se.get_ref("items").unwrap();
            assert_eq!(guard.len(), 2);
            assert_eq!(guard[0], Value::String("hello".to_string()));
        }

        // Guard dropped, can access again
        assert_eq!(se.collection_len("items"), 2);
    }

    #[test]
    fn side_effects_get_ref_missing_key() {
        let se = SideEffects::new();
        assert!(se.get_ref("missing").is_none());
    }

    #[test]
    fn side_effects_set_and_get_data() {
        let se = SideEffects::new();

        se.set_data("count", 42i32);
        se.set_data("name", "Alice".to_string());

        assert_eq!(se.get_data::<i32>("count"), Some(42));
        assert_eq!(se.get_data::<String>("name"), Some("Alice".to_string()));
    }

    #[test]
    fn side_effects_get_data_wrong_type() {
        let se = SideEffects::new();
        se.set_data("count", 42i32);

        // Wrong type returns None
        assert_eq!(se.get_data::<String>("count"), None);
        assert_eq!(se.get_data::<i64>("count"), None);
    }

    #[test]
    fn side_effects_get_data_missing_key() {
        let se = SideEffects::new();
        assert_eq!(se.get_data::<i32>("missing"), None);
    }

    #[test]
    fn side_effects_contains_key() {
        let se = SideEffects::new();
        assert!(!se.contains_key("test"));

        se.store("test", Value::Null);
        assert!(se.contains_key("test"));
    }

    #[test]
    fn side_effects_collection_len() {
        let se = SideEffects::new();
        assert_eq!(se.collection_len("items"), 0);

        se.store("items", Value::Int(1));
        assert_eq!(se.collection_len("items"), 1);

        se.store("items", Value::Int(2));
        assert_eq!(se.collection_len("items"), 2);
    }

    #[test]
    fn side_effects_clear() {
        let se = SideEffects::new();
        se.store("a", Value::Int(1));
        se.store("b", Value::Int(2));
        se.set_data("c", 3i32);

        se.clear();

        assert!(se.keys().is_empty());
        assert_eq!(se.get("a"), None);
        assert_eq!(se.get("b"), None);
        assert_eq!(se.get_data::<i32>("c"), None);
    }

    #[test]
    fn side_effects_keys() {
        let se = SideEffects::new();
        se.store("alpha", Value::Int(1));
        se.store("beta", Value::Int(2));
        se.store("gamma", Value::Int(3));

        let mut keys = se.keys();
        keys.sort();

        assert_eq!(keys, vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn side_effects_multiple_stores_same_key() {
        let se = SideEffects::new();

        for i in 0..100 {
            se.store("many", Value::Int(i));
        }

        assert_eq!(se.collection_len("many"), 100);
        let values = se.get("many").unwrap();
        for (i, v) in values.iter().enumerate() {
            assert_eq!(*v, Value::Int(i as i64));
        }
    }

    #[test]
    fn side_effects_set_data_overwrites() {
        let se = SideEffects::new();

        se.set_data("key", 1i32);
        assert_eq!(se.get_data::<i32>("key"), Some(1));

        se.set_data("key", 2i32);
        assert_eq!(se.get_data::<i32>("key"), Some(2));
    }

    // Tests for ExecutionContext require integration with Graph
    mod execution_context_tests {
        use super::*;
        use crate::graph::Graph;
        use std::collections::HashMap;

        fn create_test_graph() -> Graph {
            use crate::storage::InMemoryGraph;
            use std::sync::Arc;

            let mut storage = InMemoryGraph::new();

            // Add vertices with different labels
            storage.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Alice".to_string()));
                props
            });
            storage.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Bob".to_string()));
                props
            });
            storage.add_vertex("software", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Graph DB".to_string()));
                props
            });

            Graph::new(Arc::new(storage))
        }

        #[test]
        fn execution_context_new_compiles() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let _ctx = ExecutionContext::new(&snapshot, snapshot.interner());
            // If this compiles and doesn't panic, the test passes
        }

        #[test]
        fn execution_context_resolve_label_existing() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // "person" label exists (added vertices with this label)
            let person_id = ctx.resolve_label("person");
            assert!(person_id.is_some());

            // "software" label exists
            let software_id = ctx.resolve_label("software");
            assert!(software_id.is_some());

            // Different labels should have different IDs
            assert_ne!(person_id, software_id);
        }

        #[test]
        fn execution_context_resolve_label_missing() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // "unknown" label was never added
            let unknown_id = ctx.resolve_label("unknown");
            assert!(unknown_id.is_none());
        }

        #[test]
        fn execution_context_resolve_labels_multiple() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Resolve multiple labels at once
            let ids = ctx.resolve_labels(&["person", "software", "unknown"]);

            // Should return 2 IDs (unknown is filtered out)
            assert_eq!(ids.len(), 2);
        }

        #[test]
        fn execution_context_resolve_labels_all_missing() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let ids = ctx.resolve_labels(&["unknown1", "unknown2"]);
            assert!(ids.is_empty());
        }

        #[test]
        fn execution_context_get_label() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // First resolve to get the ID
            let person_id = ctx.resolve_label("person").unwrap();

            // Then get the string back
            let label_str = ctx.get_label(person_id);
            assert_eq!(label_str, Some("person"));
        }

        #[test]
        fn execution_context_get_label_missing() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // ID that doesn't exist
            let label_str = ctx.get_label(999);
            assert!(label_str.is_none());
        }

        #[test]
        fn execution_context_snapshot_accessor() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Should be able to access the snapshot through the context
            let _snap = ctx.snapshot();
        }

        #[test]
        fn execution_context_interner_accessor() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Should be able to access the interner through the context
            let interner = ctx.interner();

            // Interner should have the same labels
            assert!(interner.lookup("person").is_some());
        }

        #[test]
        fn execution_context_side_effects_accessible() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Side effects should be accessible and usable
            ctx.side_effects.store("test", Value::Int(42));
            let values = ctx.side_effects.get("test");
            assert_eq!(values, Some(vec![Value::Int(42)]));
        }
    }
}
