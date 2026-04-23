# Graph Algorithms

Interstellar ships traversal and pathfinding algorithms in the `algorithms` module. All algorithms are generic over `GraphAccess`, so they work with both in-memory (`Arc<Graph>`) and persistent (`Arc<CowMmapGraph>`) backends.

```rust
use interstellar::algorithms::*;
use interstellar::algorithms::common::{property_weight, unit_weight};
```

Run the full example: `cargo run --example algorithms`

---

## Common Types

### Direction

Controls which edges are followed during expansion:

```rust
use interstellar::algorithms::Direction;

Direction::Out   // outgoing edges only
Direction::In    // incoming edges only
Direction::Both  // both directions
```

### PathResult

Returned by all pathfinding algorithms:

```rust
pub struct PathResult {
    pub vertices: Vec<VertexId>,  // ordered source -> target
    pub edges: Vec<EdgeId>,       // edges traversed (len = vertices.len() - 1)
    pub weight: f64,              // total weight (hop count for unweighted)
}
```

### WeightFn

Extracts a numeric weight from an edge's properties. Two built-in helpers:

```rust
// Every edge has weight 1.0 (unweighted)
let wf = unit_weight();

// Read weight from a property (returns None if missing/non-numeric)
let wf = property_weight("distance".to_string());
```

You can also provide a custom closure:

```rust
let wf: WeightFn = Box::new(|_edge_id, props| {
    props.get("cost").and_then(|v| match v {
        Value::Float(f) => Some(*f),
        Value::Int(n) => Some(*n as f64),
        _ => None,
    })
});
```

### AlgorithmError

All algorithms return `Result<T, AlgorithmError>`. Variants:

| Variant | When |
|---------|------|
| `VertexNotFound(VertexId)` | Source or target doesn't exist |
| `NoPath { from, to }` | No path between vertices |
| `InvalidWeight(String)` | Weight function returned `None` |
| `NegativeWeightCycle` | Negative cycle detected |
| `DepthLimitExceeded(u32)` | IDDFS exceeded max depth |
| `Storage(StorageError)` | Underlying storage failure |

---

## Traversal Algorithms

### BFS (Breadth-First Search)

Lazy iterator yielding `(VertexId, depth)` in breadth-first order.

```rust
use interstellar::algorithms::{Bfs, Direction};

let graph = Arc::new(Graph::new());
// ... add vertices and edges ...

let results: Vec<_> = Bfs::new(Arc::clone(&graph), start_vertex)
    .direction(Direction::Out)
    .max_depth(3)                              // optional depth limit
    .label_filter(vec!["knows".to_string()])   // optional edge label filter
    .collect();

for (vertex_id, depth) in results {
    println!("depth {}: {:?}", depth, vertex_id);
}
```

**Properties:**
- Visits every reachable vertex exactly once
- Depth is monotonically non-decreasing
- Time: O(V + E), Space: O(V)

### DFS (Depth-First Search)

Lazy iterator yielding `(VertexId, depth)` in pre-order.

```rust
use interstellar::algorithms::{Dfs, Direction};

let results: Vec<_> = Dfs::new(Arc::clone(&graph), start_vertex)
    .direction(Direction::Out)
    .max_depth(5)
    .collect();
```

**Properties:**
- Visits every reachable vertex exactly once
- Pre-order traversal (parent before children)
- Time: O(V + E), Space: O(V)

### When to use BFS vs DFS

| Use case | Preferred |
|----------|-----------|
| Shortest path (unweighted) | BFS |
| Exploring all reachable vertices | Either |
| Finding any path quickly | DFS |
| Level-by-level exploration | BFS |
| Deep graph exploration with memory constraints | DFS |

### Bidirectional BFS

Expands frontiers from both source and target simultaneously. Finds the shortest unweighted path, often much faster than plain BFS on large-diameter graphs.

```rust
use interstellar::algorithms::{bidirectional_bfs, Direction};

let path = bidirectional_bfs(&graph, source, target, Direction::Out, None)?;
println!("Hops: {}", path.edges.len());
```

The optional last parameter is an edge label filter (`Option<&[String]>`).

**Complexity:** O(V + E) worst case, but typically ~O(b^{d/2}) vs O(b^d) for BFS where b is branching factor and d is path length.

### IDDFS (Iterative Deepening DFS)

Combines DFS space efficiency with BFS shortest-path optimality. Runs DFS repeatedly with increasing depth limits.

```rust
use interstellar::algorithms::{iddfs, Direction};

let path = iddfs(&graph, source, target, 10, Direction::Out)?;
```

**When to use:** When you need shortest paths but memory is constrained. Space is O(d) instead of O(V).

---

## Pathfinding Algorithms

### Unweighted Shortest Path

BFS-based shortest path. Returns the first shortest path found.

```rust
use interstellar::algorithms::{shortest_path_unweighted, Direction};

let path = shortest_path_unweighted(
    &graph, source, target,
    Direction::Out,
    None,  // optional edge label filter
)?;

println!("Path: {:?}", path.vertices);
println!("Hops: {}", path.edges.len());
```

### Dijkstra

Weighted shortest path for non-negative edge weights.

```rust
use interstellar::algorithms::{dijkstra, Direction};
use interstellar::algorithms::common::property_weight;

let wf = property_weight("distance".to_string());
let path = dijkstra(&graph, source, target, &wf, Direction::Out)?;

println!("Distance: {}", path.weight);
```

**Complexity:** O((V + E) log V) time, O(V) space.

### Dijkstra All (Single-Source)

Compute shortest distances from one vertex to all reachable vertices.

```rust
use interstellar::algorithms::{dijkstra_all, Direction};

let results = dijkstra_all(&graph, source, &wf, Direction::Out)?;

for (vid, (distance, path)) in &results {
    println!("{:?}: distance {}, hops {}", vid, distance, path.edges.len());
}
```

Returns `HashMap<VertexId, (f64, PathResult)>`.

### A*

Dijkstra with a heuristic function that guides the search toward the target. The heuristic must be **admissible** (never overestimate) for optimal results.

```rust
use interstellar::algorithms::{astar, Direction};

let path = astar(
    &graph,
    source,
    target,
    &wf,
    |vertex_id| estimated_distance_to_target(vertex_id),
    Direction::Out,
)?;
```

**When to use:** When you have domain knowledge to estimate remaining distance (e.g., geographic coordinates, grid positions). Falls back to Dijkstra behavior with `|_| 0.0`.

### Yen's K-Shortest Paths

Find the K shortest loopless paths between two vertices.

```rust
use interstellar::algorithms::{k_shortest_paths, Direction};

let paths = k_shortest_paths(
    &graph, source, target,
    3,     // find up to 3 shortest paths
    &wf,
    Direction::Out,
)?;

for (i, path) in paths.iter().enumerate() {
    println!("Path {}: distance {}", i + 1, path.weight);
}
```

**Properties:**
- Paths are returned in non-decreasing weight order
- Returns fewer than K paths if fewer exist
- Complexity: O(K * V * (V + E) log V)

---

## Choosing an Algorithm

| Need | Algorithm | Time Complexity |
|------|-----------|-----------------|
| Explore all reachable vertices | `Bfs` or `Dfs` | O(V + E) |
| Shortest path (unweighted) | `shortest_path_unweighted` | O(V + E) |
| Shortest path (unweighted, large graph) | `bidirectional_bfs` | ~O(b^{d/2}) |
| Shortest path (weighted, non-negative) | `dijkstra` | O((V+E) log V) |
| Shortest path (weighted, with heuristic) | `astar` | O((V+E) log V)* |
| Distances to all vertices | `dijkstra_all` | O((V+E) log V) |
| Multiple alternative paths | `k_shortest_paths` | O(KV(V+E) log V) |
| Shortest path, low memory | `iddfs` | O(b^d) time, O(d) space |

\* Typically much better with a good heuristic.

---

## Visitor Trait

BFS and DFS support a `Visitor` trait for custom callbacks during traversal. Implement `discover` to control pruning and `finish` for post-processing:

```rust
use interstellar::algorithms::common::Visitor;
use interstellar::value::VertexId;

struct DepthLogger;

impl Visitor for DepthLogger {
    fn discover(&mut self, vertex: VertexId, depth: u32) -> bool {
        println!("Discovered {:?} at depth {}", vertex, depth);
        true  // return false to prune this branch
    }

    fn finish(&mut self, vertex: VertexId, depth: u32) {
        println!("Finished {:?} at depth {}", vertex, depth);
    }
}
```

The `NoopVisitor` is a no-op implementation that accepts all vertices.

---

## Error Handling

All algorithms use `Result` types. Common patterns:

```rust
use interstellar::algorithms::AlgorithmError;

match dijkstra(&graph, source, target, &wf, Direction::Out) {
    Ok(path) => println!("Found path: {} hops", path.edges.len()),
    Err(AlgorithmError::VertexNotFound(id)) => {
        eprintln!("Vertex {:?} does not exist", id);
    }
    Err(AlgorithmError::NoPath { from, to }) => {
        eprintln!("No path from {:?} to {:?}", from, to);
    }
    Err(AlgorithmError::InvalidWeight(msg)) => {
        eprintln!("Bad edge weight: {}", msg);
    }
    Err(e) => eprintln!("Algorithm error: {}", e),
}
```

---

## Full Example

See [`examples/algorithms.rs`](../../examples/algorithms.rs) for a complete runnable example that builds a city road network and demonstrates every algorithm.

```bash
cargo run --example algorithms
```
