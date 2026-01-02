use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::Arc;

use crate::storage::interner::StringInterner;
use crate::storage::{GraphStorage, InMemoryGraph};

pub struct Graph {
    pub(crate) storage: Arc<dyn GraphStorage>,
    pub(crate) lock: Arc<RwLock<()>>,
}

pub struct GraphSnapshot<'g> {
    pub graph: &'g Graph,
    pub version: u64,
    pub(crate) _guard: RwLockReadGuard<'g, ()>,
}

impl<'g> GraphSnapshot<'g> {
    pub fn traversal(&self) -> crate::traversal::GraphTraversalSource<'_> {
        crate::traversal::GraphTraversalSource::new(self, self.interner())
    }

    /// Get the underlying storage.
    #[inline]
    pub fn storage(&self) -> &dyn GraphStorage {
        self.graph.storage.as_ref()
    }

    /// Get the string interner for label resolution.
    ///
    /// This is used by the traversal engine to efficiently resolve
    /// label strings to interned IDs.
    #[inline]
    pub fn interner(&self) -> &StringInterner {
        self.graph.storage.interner()
    }
}

pub struct GraphMut<'g> {
    pub graph: &'g Graph,
    pub(crate) _guard: RwLockWriteGuard<'g, ()>,
}

impl Graph {
    pub fn new(storage: Arc<dyn GraphStorage>) -> Self {
        Graph {
            storage,
            lock: Arc::new(RwLock::new(())),
        }
    }

    pub fn snapshot(&self) -> GraphSnapshot<'_> {
        GraphSnapshot {
            graph: self,
            version: 0,
            _guard: self.lock.read(),
        }
    }

    pub fn mutate(&self) -> GraphMut<'_> {
        GraphMut {
            graph: self,
            _guard: self.lock.write(),
        }
    }

    /// Try to acquire write lock without blocking
    /// Returns None if lock is already held by another writer or reader
    pub fn try_mutate(&self) -> Option<GraphMut<'_>> {
        self.lock.try_write().map(|guard| GraphMut {
            graph: self,
            _guard: guard,
        })
    }

    /// Create a new in-memory graph (no persistence)
    pub fn in_memory() -> Self {
        Self::new(Arc::new(InMemoryGraph::new()))
    }

    /// Get the underlying storage (for advanced use cases)
    pub fn storage(&self) -> &Arc<dyn GraphStorage> {
        &self.storage
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_try_mutate_succeeds_when_unlocked() {
        let graph = Graph::in_memory();
        let mut_handle = graph.try_mutate();
        assert!(
            mut_handle.is_some(),
            "try_mutate should succeed when unlocked"
        );
    }

    #[test]
    fn test_try_mutate_fails_when_locked() {
        let graph = Graph::in_memory();
        let _mut1 = graph.mutate(); // Hold write lock
        let mut2 = graph.try_mutate();
        assert!(
            mut2.is_none(),
            "try_mutate should fail when write lock is held"
        );
    }

    #[test]
    fn test_try_mutate_fails_when_snapshot_held() {
        let graph = Graph::in_memory();
        let _snap = graph.snapshot(); // Hold read lock
        let mut_handle = graph.try_mutate();
        assert!(
            mut_handle.is_none(),
            "try_mutate should fail when read lock is held"
        );
    }

    #[test]
    fn test_try_mutate_succeeds_after_lock_released() {
        let graph = Graph::in_memory();
        {
            let _mut1 = graph.mutate(); // Acquire and release
        } // Lock released here
        let mut2 = graph.try_mutate();
        assert!(
            mut2.is_some(),
            "try_mutate should succeed after lock is released"
        );
    }

    #[test]
    fn test_multiple_snapshots_allowed() {
        let graph = Graph::in_memory();
        let _snap1 = graph.snapshot();
        let _snap2 = graph.snapshot();
        let _snap3 = graph.snapshot();
        // All snapshots should coexist (multiple readers allowed)
    }

    #[test]
    fn test_concurrent_try_mutate() {
        let graph = Arc::new(Graph::in_memory());
        let graph_clone = Arc::clone(&graph);

        // Thread 1 holds the lock
        let handle1 = thread::spawn(move || {
            let _mut = graph_clone.mutate();
            thread::sleep(Duration::from_millis(100));
        });

        // Give thread 1 time to acquire the lock
        thread::sleep(Duration::from_millis(20));

        // Thread 2 tries to acquire
        let result = graph.try_mutate();
        assert!(
            result.is_none(),
            "try_mutate should fail when another thread holds lock"
        );

        handle1.join().unwrap();

        // Now it should succeed
        let result = graph.try_mutate();
        assert!(
            result.is_some(),
            "try_mutate should succeed after other thread releases lock"
        );
    }

    #[test]
    fn test_traversal_holds_read_lock() {
        let graph = Arc::new(Graph::in_memory());
        let graph_clone = Arc::clone(&graph);

        // Thread 1 holds snapshot with traversal
        let handle1 = thread::spawn(move || {
            let snap = graph_clone.snapshot();
            let _g = snap.traversal();
            thread::sleep(Duration::from_millis(100));
            // Read lock held until snap drops
        });

        // Give thread 1 time to acquire the read lock
        thread::sleep(Duration::from_millis(20));

        // Thread 2 tries to get write lock - should fail
        let result = graph.try_mutate();
        assert!(
            result.is_none(),
            "try_mutate should fail when traversal holds read lock"
        );

        handle1.join().unwrap();

        // Now it should succeed
        let result = graph.try_mutate();
        assert!(
            result.is_some(),
            "try_mutate should succeed after traversal completes"
        );
    }

    #[test]
    fn test_multiple_traversals_concurrent() {
        let graph = Arc::new(Graph::in_memory());

        // Multiple readers can hold snapshots concurrently
        let snap1 = graph.snapshot();
        let snap2 = graph.snapshot();
        let snap3 = graph.snapshot();

        let _g1 = snap1.traversal();
        let _g2 = snap2.traversal();
        let _g3 = snap3.traversal();

        // All traversals should coexist (multiple readers allowed)
        // No assertion needed - this tests that it doesn't deadlock
    }

    #[test]
    fn test_traversal_from_single_snapshot() {
        let graph = Graph::in_memory();
        let snap = graph.snapshot();

        // Can create multiple traversals from the same snapshot
        let _g1 = snap.traversal();
        let _g2 = snap.traversal();

        // Both should work without issues
    }
}
