use std::sync::Arc;

use crate::storage::GraphStorage;

pub struct Graph {
    pub(crate) storage: Arc<dyn GraphStorage>,
}

pub struct GraphSnapshot<'g> {
    pub graph: &'g Graph,
    pub version: u64,
}

pub struct GraphMut<'g> {
    pub graph: &'g Graph,
}

impl Graph {
    pub fn new(storage: Arc<dyn GraphStorage>) -> Self {
        Graph { storage }
    }

    pub fn traversal(&self) -> crate::traversal::GraphTraversalSource<'_> {
        crate::traversal::GraphTraversalSource { graph: self }
    }

    pub fn mutate(&self) -> GraphMut<'_> {
        GraphMut { graph: self }
    }
}
