# Spec 53: Graph Algorithms — Traversal & Pathfinding

## Overview

Ship the first algorithms in the empty `interstellar/src/algorithms/` module, covering graph traversal and pathfinding. These are the highest-value algorithms: they compose with the existing Gremlin/GQL pipelines, enable real queries (shortest path, reachability), and establish the patterns that later algorithm families (centrality, community, similarity) will follow.

### Goals

1. **Traversal algorithms**: BFS, DFS, bidirectional BFS, iterative deepening DFS (IDDFS)
2. **Pathfinding algorithms**: Unweighted shortest path, Dijkstra, A*, k-shortest paths (Yen's algorithm)
3. **Generic over storage**: All algorithms operate on `GraphAccess`, working with both in-memory and mmap backends
4. **Iterator-based results**: Lazy `Iterator` return types where possible (BFS/DFS yield vertices on demand)
5. **Gremlin/GQL integration**: Expose as traversal steps and GQL `CALL` procedures
6. **Benchmarks**: Criterion benchmarks in `benches/algorithms.rs`
7. **100% branch coverage target**: Comprehensive unit and property-based tests

### Non-Goals

- Centrality algorithms (PageRank, betweenness, etc.) — separate spec
- Community detection (Louvain, label propagation) — separate spec
- Similarity metrics (Jaccard, cosine) — separate spec
- Parallel/rayon execution — future enhancement
- Approximate/streaming algorithms

---

## Architecture

```
interstellar/src/algorithms/
├── mod.rs              # Public re-exports, AlgorithmError type
├── traversal.rs        # BFS, DFS, IDDFS, bidirectional BFS
├── pathfinding.rs      # Shortest path, Dijkstra, A*, Yen's k-shortest
└── common.rs           # Shared types: PathResult, WeightFn, Visitor
```

### Core Types

```rust
// algorithms/common.rs

use crate::value::{VertexId, EdgeId, Value};
use crate::graph_access::GraphAccess;
use crate::error::StorageError;
use smallvec::SmallVec;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AlgorithmError {
    #[error("vertex not found: {0:?}")]
    VertexNotFound(VertexId),

    #[error("negative weight cycle detected")]
    NegativeWeightCycle,

    #[error("no path exists between {from:?} and {to:?}")]
    NoPath { from: VertexId, to: VertexId },

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("weight property '{0}' not found or not numeric")]
    InvalidWeight(String),

    #[error("depth limit exceeded: {0}")]
    DepthLimitExceeded(u32),
}

/// A discovered path through the graph.
#[derive(Debug, Clone, PartialEq)]
pub struct PathResult {
    /// Ordered vertex IDs from source to target.
    pub vertices: Vec<VertexId>,
    /// Edge IDs traversed (len = vertices.len() - 1).
    pub edges: Vec<EdgeId>,
    /// Total weight (0.0 for unweighted, sum of edge weights for weighted).
    pub weight: f64,
}

/// Direction filter for neighbor expansion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Out,
    In,
    Both,
}

/// Function that extracts a numeric weight from an edge's properties.
/// Returns `None` to skip the edge (acts as a filter).
pub type WeightFn = Box<dyn Fn(EdgeId, &HashMap<String, Value>) -> Option<f64> + Send + Sync>;

/// Constant weight of 1.0 for every edge (unweighted graphs).
pub fn unit_weight() -> WeightFn {
    Box::new(|_, _| Some(1.0))
}

/// Extract weight from a named property. Non-numeric or missing → error.
pub fn property_weight(key: String) -> WeightFn {
    Box::new(move |_, props| {
        props.get(&key).and_then(|v| match v {
            Value::Int(n) => Some(*n as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        })
    })
}

/// Optional visitor callback for traversal algorithms.
pub trait Visitor {
    /// Called when a vertex is first discovered. Return `false` to prune.
    fn discover(&mut self, _vertex: VertexId, _depth: u32) -> bool { true }
    /// Called when a vertex is fully processed.
    fn finish(&mut self, _vertex: VertexId, _depth: u32) {}
}

/// No-op visitor.
pub struct NoopVisitor;
impl Visitor for NoopVisitor {}
```

---

## Traversal Algorithms

### BFS

```rust
// algorithms/traversal.rs

use std::collections::{HashSet, VecDeque};

/// Breadth-first traversal yielding `(VertexId, depth)` lazily.
pub struct Bfs<G: GraphAccess> {
    graph: G,
    queue: VecDeque<(VertexId, u32)>,
    visited: HashSet<VertexId>,
    direction: Direction,
    max_depth: Option<u32>,
    label_filter: Option<Vec<String>>,
}

impl<G: GraphAccess> Bfs<G> {
    pub fn new(graph: G, start: VertexId) -> Self { /* ... */ }

    pub fn direction(mut self, dir: Direction) -> Self { /* ... */ }
    pub fn max_depth(mut self, depth: u32) -> Self { /* ... */ }
    pub fn label_filter(mut self, labels: Vec<String>) -> Self { /* ... */ }
}

impl<G: GraphAccess> Iterator for Bfs<G> {
    type Item = (VertexId, u32);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((vid, depth)) = self.queue.pop_front() {
            if let Some(max) = self.max_depth {
                if depth > max { continue; }
            }
            if !self.visited.insert(vid) { continue; }

            // Expand neighbors
            if self.max_depth.map_or(true, |m| depth < m) {
                let neighbors = self.expand(vid);
                for neighbor in neighbors {
                    if !self.visited.contains(&neighbor) {
                        self.queue.push_back((neighbor, depth + 1));
                    }
                }
            }

            return Some((vid, depth));
        }
        None
    }
}
```

### DFS

```rust
/// Depth-first traversal yielding `(VertexId, depth)` lazily.
/// Pre-order by default; configurable for post-order via `Visitor::finish`.
pub struct Dfs<G: GraphAccess> {
    graph: G,
    stack: Vec<(VertexId, u32)>,
    visited: HashSet<VertexId>,
    direction: Direction,
    max_depth: Option<u32>,
    label_filter: Option<Vec<String>>,
}

impl<G: GraphAccess> Iterator for Dfs<G> {
    type Item = (VertexId, u32);
    // Pops from stack, marks visited, pushes unvisited neighbors
}
```

### Bidirectional BFS

```rust
/// Bidirectional BFS for finding shortest unweighted path.
/// Alternates expansion from source and target frontiers.
/// Returns the meeting point and reconstructed path.
pub fn bidirectional_bfs<G: GraphAccess>(
    graph: &G,
    source: VertexId,
    target: VertexId,
    direction: Direction,
    label_filter: Option<&[String]>,
) -> Result<PathResult, AlgorithmError> {
    // Two frontiers: forward (Out from source) and backward (In from target)
    // Alternate expansion; when frontiers intersect, reconstruct path
    // Time: O(V + E), but in practice ~2 * O(b^{d/2}) vs O(b^d) for BFS
}
```

### Iterative Deepening DFS (IDDFS)

```rust
/// Iterative deepening: DFS with increasing depth limit.
/// Combines DFS space efficiency O(d) with BFS optimality.
pub fn iddfs<G: GraphAccess>(
    graph: &G,
    source: VertexId,
    target: VertexId,
    max_depth: u32,
    direction: Direction,
) -> Result<PathResult, AlgorithmError> {
    for depth_limit in 0..=max_depth {
        if let Some(path) = depth_limited_dfs(graph, source, target, depth_limit, direction)? {
            return Ok(path);
        }
    }
    Err(AlgorithmError::NoPath { from: source, to: target })
}
```

---

## Pathfinding Algorithms

### Unweighted Shortest Path

```rust
// algorithms/pathfinding.rs

/// Shortest path in an unweighted graph using BFS.
/// Returns the first shortest path found.
pub fn shortest_path_unweighted<G: GraphAccess>(
    graph: &G,
    source: VertexId,
    target: VertexId,
    direction: Direction,
    label_filter: Option<&[String]>,
) -> Result<PathResult, AlgorithmError> {
    // BFS from source, tracking predecessors
    // Reconstruct path when target is reached
    // Weight = number of hops
}
```

### Dijkstra

```rust
/// Dijkstra's shortest path algorithm for non-negative weighted graphs.
///
/// # Complexity
/// - Time: O((V + E) log V) with binary heap
/// - Space: O(V)
///
/// # Errors
/// - `AlgorithmError::InvalidWeight` if weight function returns `None`
/// - `AlgorithmError::NoPath` if target is unreachable
pub fn dijkstra<G: GraphAccess>(
    graph: &G,
    source: VertexId,
    target: VertexId,
    weight_fn: &WeightFn,
    direction: Direction,
) -> Result<PathResult, AlgorithmError> {
    // Standard binary-heap Dijkstra
    // dist: HashMap<VertexId, f64>
    // prev: HashMap<VertexId, (VertexId, EdgeId)>
    // heap: BinaryHeap<Reverse<(OrderedFloat<f64>, VertexId)>>
}

/// Single-source Dijkstra returning distances to ALL reachable vertices.
pub fn dijkstra_all<G: GraphAccess>(
    graph: &G,
    source: VertexId,
    weight_fn: &WeightFn,
    direction: Direction,
) -> Result<HashMap<VertexId, (f64, PathResult)>, AlgorithmError> { /* ... */ }
```

### A*

```rust
/// A* pathfinding with a user-supplied heuristic.
///
/// The heuristic `h(v)` must be admissible (never overestimates)
/// for the result to be optimal.
///
/// # Complexity
/// - Time: O((V + E) log V) worst case, typically much better with a good heuristic
/// - Space: O(V)
pub fn astar<G, H>(
    graph: &G,
    source: VertexId,
    target: VertexId,
    weight_fn: &WeightFn,
    heuristic: H,
    direction: Direction,
) -> Result<PathResult, AlgorithmError>
where
    G: GraphAccess,
    H: Fn(VertexId) -> f64,
{
    // f(v) = g(v) + h(v)
    // Otherwise identical to Dijkstra but prioritizes by f(v)
}
```

### Yen's K-Shortest Paths

```rust
/// Yen's algorithm for finding the K shortest loopless paths.
///
/// # Complexity
/// - Time: O(K * V * (V + E) log V) — K iterations of modified Dijkstra
/// - Space: O(K * V)
pub fn k_shortest_paths<G: GraphAccess>(
    graph: &G,
    source: VertexId,
    target: VertexId,
    k: usize,
    weight_fn: &WeightFn,
    direction: Direction,
) -> Result<Vec<PathResult>, AlgorithmError> {
    // 1. Find shortest path via Dijkstra → A[0]
    // 2. For i in 1..k:
    //    For each spur node in A[i-1]:
    //      Remove edges in A[0..i] that share the same root path
    //      Run Dijkstra from spur node to target
    //      Combine root path + spur path → candidate
    //    Pick shortest candidate → A[i]
}
```

---

## Gremlin Integration

Expose algorithms as traversal steps on `GraphTraversalSource` and `GraphTraversal`:

```rust
// In traversal source / traversal impl:

impl<G: GraphAccess> GraphTraversal<G> {
    /// Shortest path from current vertex to target.
    /// g.V(source).shortest_path_to(target)
    pub fn shortest_path_to(self, target: VertexId) -> GraphTraversal<G> { /* ... */ }

    /// Weighted shortest path.
    /// g.V(source).shortest_path_to(target).by("weight")
    pub fn dijkstra_to(self, target: VertexId, weight_property: &str) -> GraphTraversal<G> { /* ... */ }
}
```

## GQL Integration

```sql
-- Unweighted shortest path
CALL interstellar.shortestPath(source, target)
YIELD path, distance

-- Dijkstra
CALL interstellar.dijkstra(source, target, 'weight')
YIELD path, distance

-- K shortest paths
CALL interstellar.kShortestPaths(source, target, 3, 'weight')
YIELD path, distance, index

-- BFS
CALL interstellar.bfs(source, {maxDepth: 5, direction: 'OUT'})
YIELD node, depth
```

---

## Error Handling

All algorithms return `Result<T, AlgorithmError>`. No panics. Specific error variants:

| Scenario | Error |
|---|---|
| Source/target vertex doesn't exist | `VertexNotFound(id)` |
| No path between vertices | `NoPath { from, to }` |
| Edge missing required weight property | `InvalidWeight(property_name)` |
| Negative weight detected in Dijkstra | `NegativeWeightCycle` |
| IDDFS exceeds depth limit | `DepthLimitExceeded(max)` |
| Underlying storage failure | `Storage(StorageError)` |

---

## Implementation Phases

### Phase 1: Core Traversal (BFS + DFS)
- `common.rs`: `AlgorithmError`, `PathResult`, `Direction`, `WeightFn`, `Visitor`
- `traversal.rs`: `Bfs`, `Dfs` iterators
- Unit tests + proptest for traversal ordering invariants
- Criterion benchmarks on synthetic graphs (1K, 10K, 100K vertices)

### Phase 2: Pathfinding
- `pathfinding.rs`: `shortest_path_unweighted`, `dijkstra`, `dijkstra_all`
- Bidirectional BFS in `traversal.rs`
- IDDFS in `traversal.rs`
- Tests: known graphs with verified shortest paths, edge cases (disconnected, self-loops, parallel edges)

### Phase 3: Advanced Pathfinding
- A* with pluggable heuristic
- Yen's k-shortest paths
- Property-based tests: k-shortest paths are in non-decreasing weight order, shortest path ≤ all k-shortest paths

### Phase 4: Query Language Integration
- Gremlin step wrappers (`shortest_path_to`, `dijkstra_to`)
- GQL `CALL` procedure registration
- Integration tests via Gremlin text and GQL queries

---

## Testing Strategy

### Unit Tests (per algorithm)
- Empty graph → appropriate error
- Single vertex, no edges → NoPath or single-element result
- Linear chain → correct path
- Diamond/grid graphs → correct shortest path
- Disconnected components → NoPath
- Self-loops → handled correctly
- Parallel edges → selects minimum weight
- Large fan-out (star graph) → correct BFS/DFS order

### Property-Based Tests (proptest)
- BFS visits every reachable vertex exactly once
- BFS depth is monotonically non-decreasing
- DFS visits every reachable vertex exactly once
- Dijkstra result weight ≤ any other path weight (sample random paths)
- k-shortest: `results[0].weight <= results[1].weight <= ...`
- Bidirectional BFS result equals BFS shortest path
- Path vertices form a valid walk (each consecutive pair connected by an edge)

### Benchmarks (`benches/algorithms.rs`)
- BFS/DFS on random graphs: 1K, 10K, 100K vertices
- Dijkstra on weighted random graphs: 1K, 10K, 100K vertices
- Bidirectional BFS vs plain BFS on large diameter graphs
- k-shortest paths: k=1,5,10 on 10K vertex graph

---

## Dependencies

No new external crates required. Uses:
- `std::collections::{BinaryHeap, HashMap, HashSet, VecDeque}`
- `smallvec` (already in deps)
- `thiserror` (already in deps)
- `ordered-float` — **new dev/runtime dependency** for `BinaryHeap` key ordering in Dijkstra/A*

```toml
[dependencies]
ordered-float = "4.2"
```

---

## Open Questions

1. **Should `Bfs`/`Dfs` also yield edge IDs?** Current design yields `(VertexId, depth)`. Could yield `(VertexId, Option<EdgeId>, depth)` to include the edge that discovered each vertex. Slightly more overhead but useful for path reconstruction.

2. **All-pairs shortest paths?** Floyd-Warshall / Johnson's are O(V³) / O(V²logV + VE). Only practical for small graphs. Defer to a separate spec or add behind a `small_graph` guard?

3. **Should `WeightFn` be a trait instead of a boxed closure?** Trait would allow monomorphization but complicates the API. Boxed closure is simpler and the weight lookup is not on the hot path relative to heap operations.
