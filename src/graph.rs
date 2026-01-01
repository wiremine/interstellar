use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::Arc;

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

    pub fn traversal(&self) -> crate::traversal::GraphTraversalSource<'_> {
        crate::traversal::GraphTraversalSource { graph: self }
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

    /// Create a new in-memory graph (no persistence)
    pub fn in_memory() -> Self {
        Self::new(Arc::new(InMemoryGraph::new()))
    }

    /// Get the underlying storage (for advanced use cases)
    pub fn storage(&self) -> &Arc<dyn GraphStorage> {
        &self.storage
    }
}
