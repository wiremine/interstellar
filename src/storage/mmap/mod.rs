//! Memory-mapped persistent graph storage.
//!
//! This module implements the `GraphStorage` trait using memory-mapped files,
//! providing durable storage with write-ahead logging for crash recovery.

pub mod arena;
pub mod freelist;
pub mod records;
pub mod recovery;
pub mod wal;

/// Memory-mapped graph storage backend.
///
/// This backend provides persistent storage using memory-mapped files with
/// write-ahead logging for durability and crash recovery.
#[derive(Clone)]
pub struct MmapGraph {
    // Implementation will be added in future phases
}

impl MmapGraph {
    /// Create a new empty MmapGraph (stub implementation).
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for MmapGraph {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mmap_graph_can_be_constructed() {
        let _graph = MmapGraph::new();
    }

    #[test]
    fn test_mmap_graph_default() {
        let _graph = MmapGraph::default();
    }

    #[test]
    fn test_mmap_graph_clone() {
        let graph1 = MmapGraph::new();
        let _graph2 = graph1.clone();
    }
}
