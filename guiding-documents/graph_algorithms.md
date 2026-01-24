# Algorithm Roadmap for an Embedded (SQLite‑Style) Graph Database

This document summarizes a **pragmatic, high‑leverage set of graph algorithms** suitable for a **single‑process, embedded graph database written in Rust**.

The guiding principles:

* Favor **predictable performance** over asymptotic optimality
* Prefer **composable primitives** over monolithic analytics jobs
* Optimize for **small → medium graphs** and **synchronous execution**
* Deliver features that *feel powerful* without exploding complexity

---

## Tier 0 — Core Foundations (Must‑Have)

These are the algorithms everything else builds on. If these are fast and correct, the database will feel fast and expressive.

### 1. Breadth‑First Search (BFS) / Reachability

**Purpose**

* Reachability queries
* Subgraph extraction
* Basis for shortest paths and many analytics

**Variants to support**

* Directed / undirected
* Depth‑limited
* Early‑exit / predicate‑based

> BFS is the single most important primitive in a graph engine.

---

### 2. Shortest Path

**Algorithms**

* Unweighted: BFS
* Weighted: Dijkstra

**Features**

* Return full paths, not just distances
* Support edge‑property‑based weights
* Optional cutoff limits

**Why it matters**

* Dependency analysis
* Routing
* Influence and blame chains

---

### 3. Connected Components

**Algorithms**

* Weakly Connected Components (WCC)
* Strongly Connected Components (Tarjan or Kosaraju)

**Use cases**

* Clustering
* Fraud rings
* Isolation and partition detection

Cheap to compute, extremely useful.

---

## Tier 1 — High Impact, Low Regret

These algorithms dramatically increase perceived power without heavy implementation cost.

### 4. Degree Metrics

**Metrics**

* In‑degree
* Out‑degree
* Total degree

**Notes**

* Trivial to compute
* Frequently queried
* Easy to cache

---

### 5. PageRank (Basic)

**Why include it**

* Well‑known and intuitive
* Demonstrates iterative global computation
* Useful for ranking and influence

**Recommended constraints**

* Fixed iteration count
* Simple damping factor
* Results written as vertex properties

---

### 6. Betweenness Centrality (Optional)

**Purpose**

* Identify bridges and chokepoints

**Caveats**

* Expensive (≈ O(V·E))
* Should support:

  * sampling
  * depth limits
  * small‑graph focus

Best treated as a power‑user feature.

---

## Tier 2 — Structural Insight Algorithms

These help users *understand* graph shape and correctness.

### 7. Cycle Detection

**Capabilities**

* Cycle existence check
* Optional bounded cycle enumeration

**Use cases**

* Dependency graphs
* Deadlock detection
* Build systems

---

### 8. Topological Sort

**Requirements**

* DAG detection
* Stable or deterministic ordering (nice to have)

**Use cases**

* Workflows
* Task graphs
* Pipelines

---

### 9. K‑Core Decomposition

**Why it’s useful**

* Identifies dense subgraphs
* Good approximation for communities
* Much cheaper than full community detection

Highly underrated for embedded systems.

---

## Tier 3 — Similarity & Recommendation (Optional)

Only add if this matches your target workloads.

### 10. Node Similarity

**Metrics**

* Jaccard similarity
* Cosine similarity

**Use cases**

* Recommendations
* Deduplication
* Entity resolution

---

### 11. Lightweight Link Prediction

**Heuristics**

* Common neighbors
* Adamic–Adar

Keep this explainable. Avoid ML initially.

---

## Algorithms to Defer or Avoid (Initially)

These tend to fight the embedded, single‑process model.

* Full Louvain / Leiden community detection
* All‑pairs shortest paths
* Spectral algorithms
* Distributed graph algorithms
* Native hypergraph analytics (unless hyperedges are first‑class)

---

## API Design Recommendations

Algorithms should feel like **graph primitives**, not bolt‑on utilities.

**Good patterns**

* Composable builders
* Property‑writing semantics
* Interruptible execution

Conceptual examples:

```text
graph.pagerank()
     .iterations(20)
     .write_property("rank")
```

```text
graph.bfs()
     .from(vertex)
     .depth_limit(3)
     .edge_filter(predicate)
```

---

## Performance Notes (SQLite‑Style Thinking)

* Favor contiguous adjacency storage
* Use stable vertex IDs
* Optimize for memory bandwidth
* Make transaction boundaries explicit

Most graph algorithms are **memory‑bound**, not CPU‑bound.

---

## Minimal “Killer” Algorithm Set

If you ship only these, the database is already compelling:

1. BFS / DFS
2. Shortest path (BFS + Dijkstra)
3. Connected components (WCC + SCC)
4. Degree metrics
5. PageRank
6. Cycle detection
7. Topological sort

---

**Bottom line:**
A small, well‑chosen algorithm set — deeply integrated and fast — will outperform a sprawling analytics catalog in an embedded graph database.
