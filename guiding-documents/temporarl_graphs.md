# Temporal Graphs Design for an Embedded Graph Database

This document outlines a practical approach to implementing **temporal graphs** in a single-process, SQLite-style graph database written in Rust.

---

## 1. Intuition

A **temporal graph** records **when vertices and edges exist**, not just whether they exist.

Example:

> “Alice depended on Bob from Jan 3 to Jan 10.”

This allows queries like:

* What did the graph look like at time T?
* How did relationships evolve over time?
* When did this path or connection stop existing?

---

## 2. Types of Time

### 1️⃣ Valid Time (Event Time)

* When the fact was true in the modeled world.
* Example: Service A depended on Service B from 10:02–10:05.

### 2️⃣ Transaction Time

* When the database learned about it.
* Example: Dependency discovered at 10:07.

> For v1, implementing **valid time only** is sufficient.

---

## 3. Temporal Graph Model

### Vertices

```text
Vertex {
  id: VertexId
  valid_from: Timestamp
  valid_to: Option<Timestamp>  // NULL means still active
}
```

### Edges

```text
Edge {
  from: VertexId
  to: VertexId
  label: EdgeLabel
  valid_from: Timestamp
  valid_to: Option<Timestamp>  // NULL means still active
}
```

No deletions—closing intervals marks removal.

---

## 4. Query Patterns

### Snapshot Queries

> “Graph at time T”

```text
MATCH (a)-[e]->(b)
WHERE e.valid_from <= T
  AND (e.valid_to IS NULL OR e.valid_to > T)
```

* Can expose syntax like `AT TIME '2026-01-12T10:03:00'`

### Temporal Traversals

* Path exists at a single instant
* Path exists continuously over a duration
* Path exists at any time

### Diff Queries

> “What changed between T1 and T2?”

* Edges added or removed
* Vertices activated or deactivated

---

## 5. Algorithms Over Time

Temporal graphs enable **time-aware algorithms**:

* Connected components at time T
* PageRank over a sliding window
* Shortest paths that existed for a minimum duration
* Evolution of centrality over time

---

## 6. Storage Implications

* Adjacency lists sorted by `valid_from`
* Optional secondary index on `(valid_from, valid_to)`
* Snapshot cursors = filtered iterators

> Interval trees or other complex structures are optional for v1.

---

## 7. Safe Deletion

Instead of removing edges or vertices:

```text
edge.valid_to = now()
```

Benefits:

* Time travel
* Undo capability
* Auditing
* Replay

---

## 8. Integration with Versioned Layouts

* Layout v1: Non-temporal
* Layout v2: Adds `valid_from` and `valid_to`
* Migration sets defaults (e.g., `valid_from = epoch`)

Ensures backward compatibility and safe upgrades.

---

## 9. Minimal Temporal Feature Set (High ROI)

1. Valid-time on edges
2. Snapshot queries (`AT TIME`)
3. No physical deletes (close intervals instead)
4. BFS / shortest-path respecting time

Even this small set is rare and highly valuable.

---

## 10. Benefits

* Replay history of graph changes
* Explain why relationships existed or failed
* Audit and debug temporal evolution
* Support causality analysis

---

## 11. TL;DR

A temporal graph adds **time intervals to vertices and edges** and enables **snapshot-aware traversal and algorithms**. For an embedded Rust graph DB, it is **natural, differentiating, and high-value**.
