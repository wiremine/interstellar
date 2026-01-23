# Traversal Model

This document explains how Interstellar's traversal engine executes queries.

## Overview

Interstellar uses a **pull-based iterator model** with **lazy evaluation**. Queries are built as chains of steps, but no work happens until results are consumed.

```rust
// This builds a query but doesn't execute anything yet
let query = g.v()
    .has_label("person")
    .out("knows")
    .values("name");

// Execution happens here when we consume results
let names = query.to_list();
```

---

## Core Concepts

### Traversers

A **Traverser** is a wrapper that carries a value through the traversal pipeline:

```rust
pub struct Traverser {
    pub value: Value,       // Current element (vertex, edge, or value)
    pub path: Path,         // History of visited elements
    pub loops: u32,         // Iteration count (for repeat)
    pub bulk: u64,          // Optimization for duplicates
}
```

Traversers flow through steps, being filtered, transformed, or multiplied.

### Steps

**Steps** are the building blocks of traversals. Each step takes traversers in and produces traversers out:

```
Input Traversers → Step → Output Traversers
```

Steps are categorized by their behavior:

| Category | Behavior | Examples |
|----------|----------|----------|
| **Source** | Produce initial traversers | `v()`, `e()`, `inject()` |
| **Filter** | Keep/discard traversers | `has()`, `dedup()`, `limit()` |
| **Map** | Transform 1:1 | `values()`, `id()`, `label()` |
| **FlatMap** | Transform 1:N | `out()`, `in_()`, `unfold()` |
| **Branch** | Split/merge paths | `union()`, `choose()` |
| **Reduce** | Aggregate many:1 | `count()`, `fold()`, `group()` |
| **Side Effect** | Store/track | `as_()`, `store()` |
| **Terminal** | Consume results | `to_list()`, `next()` |

### Lazy Evaluation

Steps don't execute when called. They build a pipeline:

```rust
// Phase 1: Build pipeline (fast, no I/O)
let pipeline = g.v()           // StartStep
    .has_label("person")       // HasLabelStep
    .out("knows")              // OutStep
    .values("name");           // ValuesStep

// Phase 2: Execute (traverses graph)
let results = pipeline.to_list();
```

Benefits:
- **Optimization**: Steps can be reordered/merged
- **Efficiency**: Only compute what's needed
- **Composition**: Build complex queries from parts

---

## Execution Flow

### Simple Query

```rust
g.v().has_label("person").values("name").to_list()
```

Execution:

```
┌─────────────┐
│  v() Step   │ ─── Produces all vertex traversers
└─────┬───────┘
      │ Traverser(Vertex(1)), Traverser(Vertex(2)), ...
      ▼
┌─────────────────┐
│ has_label Step  │ ─── Filters to "person" only
└─────┬───────────┘
      │ Traverser(Vertex(1)), Traverser(Vertex(3)), ...
      ▼
┌─────────────────┐
│  values Step    │ ─── Extracts "name" property
└─────┬───────────┘
      │ Traverser("Alice"), Traverser("Bob"), ...
      ▼
┌─────────────────┐
│  to_list()      │ ─── Collects into Vec
└─────────────────┘
      │
      ▼
    ["Alice", "Bob", ...]
```

### Navigation Query

```rust
g.v_ids([alice]).out("knows").out("knows").dedup().to_list()
```

"Friends of friends" execution:

```
alice
  │
  ▼ out("knows")
bob, carol
  │
  ▼ out("knows")  
dave, eve, bob (dave from bob, eve from carol, bob from carol)
  │
  ▼ dedup()
dave, eve, bob (duplicates removed)
```

---

## Path Tracking

Traversers maintain a **Path** of visited elements:

```rust
// Enable path tracking
g.v_ids([alice])
    .as_("a")               // Label this position
    .out("knows")
    .as_("b")
    .out("created")
    .as_("c")
    .select(["a", "b", "c"]) // Retrieve labeled elements
    .to_list()
```

Path tracking is enabled automatically when using:
- `as_()` labels
- `path()` step
- `select()` step

---

## Anonymous Traversals

**Anonymous traversals** are traversal fragments without a graph binding:

```rust
use interstellar::traversal::__;

// Anonymous traversal (no graph)
let friends = __::out("knows").has_label("person");

// Use within a bound traversal
g.v().where_(friends).to_list()
```

Anonymous traversals are used in:
- `where_()` - Filter by sub-traversal existence
- `union()` - Merge multiple paths
- `choose()` - Conditional branching
- `repeat()` - Iterative traversal
- `coalesce()` - First non-empty result

### How Anonymous Traversals Execute

When an anonymous traversal is spliced into a bound traversal:

1. Parent traversal reaches the splice point
2. Execution context (graph snapshot) is passed to anonymous traversal
3. Anonymous traversal executes with parent's current traverser
4. Results flow back to parent

```rust
g.v()
    .where_(
        __::out("knows")    // Receives each vertex
            .has_label("person")
    )
    .to_list()
```

---

## Branching and Merging

### Union

Merge results from multiple paths:

```rust
g.v_ids([alice])
    .union([
        __::out("knows"),    // Friends
        __::out("created"),  // Creations
    ])
    .to_list()
```

```
         alice
           │
     ┌─────┴─────┐
     ▼           ▼
 out("knows") out("created")
     │           │
     ▼           ▼
   bob         project1
   carol       project2
     │           │
     └─────┬─────┘
           ▼
   [bob, carol, project1, project2]
```

### Choose

Conditional branching:

```rust
g.v()
    .choose(
        __::has("premium"),           // Condition
        __::values("premium_name"),   // If true
        __::values("name"),           // If false
    )
    .to_list()
```

---

## Reduce Steps

Reduce steps aggregate traversers:

```rust
// Count: many traversers → one count
g.v().has_label("person").count()  // Returns: 42

// Fold: many traversers → one list
g.v().has_label("person").fold()   // Returns: [v1, v2, ...]

// Group: many traversers → map
g.v().group_count_by_label()       // Returns: {"person": 5, "company": 3}
```

Reduce steps are **barrier steps** - they must consume all input before producing output.

---

## Repeat Steps

Iterative traversal with `repeat()`:

```rust
// Fixed iterations
g.v_ids([alice])
    .repeat(__::out("knows"))
    .times(3)
    .to_list()

// Until condition
g.v_ids([start])
    .repeat(__::out("parent"))
    .until(__::has_label("root"))
    .to_list()

// Emit intermediate results
g.v_ids([alice])
    .repeat(__::out("knows"))
    .times(3)
    .emit()  // Include results from each iteration
    .to_list()
```

### Repeat Execution

```
repeat(__::out("knows")).times(2).emit()

Iteration 0: alice
            emit → [alice]
            
Iteration 1: out("knows") → bob, carol
            emit → [bob, carol]
            
Iteration 2: out("knows") → dave, eve, frank
            emit → [dave, eve, frank]
            
Result: [alice, bob, carol, dave, eve, frank]
```

---

## Optimization

### Short-Circuit Evaluation

Some steps can terminate early:

```rust
g.v().has_label("person").limit(1).next()  // Stops after first match
g.v().has_label("person").has_next()       // Stops after finding any
```

### Step Fusion

Adjacent compatible steps may be fused:

```rust
// These might be combined internally
g.v().has_label("person").has("age")
// → single filter: label == "person" AND has property "age"
```

### Index Usage

Filter steps use indexes when available:

- `has_label()` uses label indexes
- `has_id()` uses primary ID index
- `has_value()` uses property indexes (if created)

---

## Debugging

### Explain

See the query plan:

```rust
let plan = g.v().has_label("person").out("knows").explain();
println!("{:?}", plan);
```

### Profile

Measure step execution:

```rust
let profile = g.v().has_label("person").out("knows").profile();
println!("{}", profile);
```

---

## See Also

- [Gremlin API](../api/gremlin.md) - Step reference
- [Architecture](architecture.md) - System overview
- [Performance Guide](../guides/performance.md) - Optimization tips
