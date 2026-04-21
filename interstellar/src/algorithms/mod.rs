//! Graph algorithms: traversal and pathfinding.
//!
//! This module provides graph traversal algorithms (BFS, DFS, bidirectional BFS, IDDFS)
//! and pathfinding algorithms (shortest path, Dijkstra, A*, Yen's k-shortest paths).
//!
//! All algorithms are generic over [`GraphAccess`](crate::graph_access::GraphAccess)
//! and work with both in-memory and memory-mapped storage backends.
//!
//! # Example
//!
//! ```rust
//! use interstellar::prelude::*;
//! use interstellar::algorithms::traversal::Bfs;
//! use interstellar::algorithms::common::Direction;
//! use std::sync::Arc;
//! use std::collections::HashMap;
//!
//! let graph = Arc::new(Graph::new());
//! let a = graph.add_vertex("person", HashMap::new());
//! let b = graph.add_vertex("person", HashMap::new());
//! graph.add_edge(a, b, "knows", HashMap::new()).unwrap();
//!
//! let snapshot = graph.snapshot();
//! let visited: Vec<_> = Bfs::new(snapshot, a).collect();
//! assert_eq!(visited.len(), 2);
//! ```

pub mod common;
pub mod pathfinding;
pub mod traversal;

pub use common::{
    AlgorithmError, Direction, NoopVisitor, PathResult, Visitor, WeightFn, property_weight,
    unit_weight,
};
pub use pathfinding::{astar, dijkstra, dijkstra_all, k_shortest_paths, shortest_path_unweighted};
pub use traversal::{bidirectional_bfs, iddfs, Bfs, Dfs};
