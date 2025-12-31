# Anonymous Traversals

## Overview

Anonymous traversals are a fundamental concept in graph traversal systems, enabling composable, reusable traversal fragments that can be embedded within parent traversals. Unlike bound traversals that start from a concrete graph source (e.g., `g.v()` or `g.e()`), anonymous traversals are unbound templates that receive their input at execution time.

### The `__` Convention

By convention, anonymous traversals are created using the double underscore `__` factory module. This syntactic marker clearly distinguishes anonymous traversal fragments from their bound counterparts:

```rust
// Bound traversal - starts from the graph
g.v().has_label("person").out_labels(&["knows"])

// Anonymous traversal - receives input at execution time
__.out_labels(&["knows"]).has_value("name", "Alice")
```

The `__` module provides the same rich API as bound traversals, but the resulting traversal object is detached from any specific graph instance until it's composed into a parent traversal.

### Purpose and Benefits

Anonymous traversals serve several critical purposes:

1. **Reusability**: Define traversal logic once and reuse it across multiple parent traversals
2. **Composability**: Build complex graph queries by combining simple, testable fragments
3. **Modularity**: Encapsulate traversal patterns for filtering, branching, and transformation
4. **Deferred Binding**: The traversal logic is defined independently of the data source
5. **Type Safety**: Preserve compile-time type checking while enabling dynamic composition

### Deferred Execution Model

Anonymous traversals are templates—they define *what* to do, but not *when* or *where*. The execution is deferred until:
- The anonymous traversal is embedded in a parent traversal
- The parent traversal begins evaluation
- Input traversers reach the step containing the anonymous traversal

This lazy evaluation model is consistent with the overall iterator-based execution pipeline described in algorithms.md, ensuring efficient memory usage and enabling short-circuit evaluation.

---

## Conceptual Overview

### Anonymous vs Bound Traversals

The fundamental distinction between bound and anonymous traversals lies in their starting point:

```
Bound Traversal:
┌─────────────┐
│   Graph G   │ ← Data source
└──────┬──────┘
       │ g.v()
       ▼
┌─────────────┐
│  Traversal  │ ← Bound to G
│   Pipeline  │
└─────────────┘

Anonymous Traversal:
┌─────────────┐
│      __     │ ← No data source
└──────┬──────┘
       │ __.out()
       ▼
┌─────────────┐
│  Traversal  │ ← Unbound template
│   Template  │
└─────────────┘
```

Bound traversals are connected to a specific graph instance from the start. They know where to pull initial vertices or edges from. Anonymous traversals, by contrast, are pure logic—they describe transformations without specifying the input source.

### Splicing Anonymous Traversals

When an anonymous traversal is used within a parent traversal step (like `where_()`, `union()`, or `repeat()`), it gets "spliced" into the execution pipeline:

```
Parent Traversal Execution:

g.v().has_label("person")
     .where_(__.out("knows").has_value("name", "Bob"))
     .values("name")

Execution Flow:
┌─────────┐
│  g.v()  │
└────┬────┘
     │ [v1, v2, v3, ...]
     ▼
┌─────────────────┐
│ has_label("...") │
└────┬────────────┘
     │ [v1, v3, ...]
     ▼
┌─────────────────────────────────────────┐
│ where_(anonymous_trav)                  │
│                                         │
│  For each input traverser:              │
│  ┌──────────────────────────┐           │
│  │ 1. Clone traverser       │           │
│  │ 2. Feed to __.out(...)   │           │
│  │ 3. Execute sub-traversal │           │
│  │ 4. Check if any results  │           │
│  │ 5. Emit or filter input  │           │
│  └──────────────────────────┘           │
└────┬────────────────────────────────────┘
     │ [v1 (passed), ...]
     ▼
┌─────────────┐
│ values(...) │
└─────────────┘
```

The parent traversal feeds each of its traversers into the anonymous traversal. The anonymous traversal executes independently for each input, producing results that determine filtering, branching, or transformation behavior.

### Relationship to the Lazy Iterator Pipeline

Anonymous traversals integrate seamlessly with the lazy pull-based iterator model. When a step like `where_()` needs to determine whether to emit a traverser:

1. The parent step pulls a traverser from its upstream source
2. The anonymous traversal receives this traverser as input
3. The anonymous traversal's iterator pipeline begins evaluation
4. Results are consumed just enough to make a decision (filtering, branching, etc.)
5. The parent step continues based on the anonymous traversal's output

This preserves the O(1) memory usage per active pipeline, as described in algorithms.md. Anonymous traversals don't materialize collections unless explicitly required by steps like `fold()` or `count()`.

### Template Instantiation

Each time a parent traversal executes an anonymous traversal step, it instantiates a fresh execution context:

```
Single Parent Traverser → Multiple Anonymous Executions

Parent:  v1 ──→ union([__.out("a"), __.out("b")])

Execution:
  v1 ──→ __.out("a") → [v2, v3]
  v1 ──→ __.out("b") → [v4]

Result: [v2, v3, v4]
```

For steps like `union()` or `coalesce()`, the anonymous traversal template may be executed multiple times for the same input traverser, but with independent state (unless using shared side effects or sacks).

---

## The `__` Factory Module

The `__` module is the entry point for creating anonymous traversals. While the exact Rust implementation may vary, conceptually it provides static methods that return unbound `Traversal` instances.

### Module Structure

```rust
// Conceptual representation
pub struct __ {}

impl __ {
    // Identity traversal - passes input through unchanged
    pub fn identity<E>() -> Traversal<E, E> {
        Traversal::new_anonymous(vec![Box::new(IdentityStep)])
    }
    
    // Constant emission - ignores input, emits constant value
    pub fn constant<E, V>(value: V) -> Traversal<E, V> {
        Traversal::new_anonymous(vec![Box::new(ConstantStep::new(value))])
    }
    
    // Navigation steps
    pub fn out<E>(labels: &[&str]) -> Traversal<Vertex, Vertex> {
        Traversal::new_anonymous(vec![Box::new(OutStep::new(labels))])
    }
    
    pub fn in_<E>(labels: &[&str]) -> Traversal<Vertex, Vertex> {
        Traversal::new_anonymous(vec![Box::new(InStep::new(labels))])
    }
    
    pub fn both<E>(labels: &[&str]) -> Traversal<Vertex, Vertex> {
        Traversal::new_anonymous(vec![Box::new(BothStep::new(labels))])
    }
    
    // Edge navigation
    pub fn out_e<E>(labels: &[&str]) -> Traversal<Vertex, Edge> {
        Traversal::new_anonymous(vec![Box::new(OutEStep::new(labels))])
    }
    
    pub fn in_e<E>(labels: &[&str]) -> Traversal<Vertex, Edge> {
        Traversal::new_anonymous(vec![Box::new(InEStep::new(labels))])
    }
    
    pub fn both_e<E>(labels: &[&str]) -> Traversal<Vertex, Edge> {
        Traversal::new_anonymous(vec![Box::new(BothEStep::new(labels))])
    }
    
    // Property access
    pub fn values<E>(keys: &[&str]) -> Traversal<Element, Value> {
        Traversal::new_anonymous(vec![Box::new(ValuesStep::new(keys))])
    }
    
    pub fn label<E>() -> Traversal<Element, String> {
        Traversal::new_anonymous(vec![Box::new(LabelStep)])
    }
    
    pub fn id<E>() -> Traversal<Element, ElementId> {
        Traversal::new_anonymous(vec![Box::new(IdStep)])
    }
    
    // Filtering
    pub fn has<E>(key: &str, value: Value) -> Traversal<Element, Element> {
        Traversal::new_anonymous(vec![Box::new(HasStep::new(key, value))])
    }
    
    pub fn has_label<E>(labels: &[&str]) -> Traversal<Element, Element> {
        Traversal::new_anonymous(vec![Box::new(HasLabelStep::new(labels))])
    }
}
```

### Chainable API

Once an anonymous traversal is created with a starting method, all subsequent steps can be chained exactly like bound traversals:

```rust
// Start anonymous, chain multiple steps
let complex_anon = __.out_labels(&["knows"])
    .has_value("age", 30)
    .out_labels(&["works_at"])
    .has_label("company")
    .values("name");

// Use within parent traversal
g.v().has_label("person")
    .where_(complex_anon)
```

The type system ensures that steps are compatible with their input/output types. For example, `out()` requires a `Vertex` input and produces `Vertex` outputs, while `values()` can accept any `Element` and produces `Value` outputs.

### Type Generics

Anonymous traversals are generic over their input and output types:

```rust
// Generic signature (simplified)
pub struct Traversal<In, Out> {
    steps: Vec<Box<dyn Step<?, ?>>>,
    phantom: PhantomData<(In, Out)>,
}
```

The `In` type parameter represents what the traversal expects to receive, and `Out` represents what it produces. This enables compile-time verification when embedding anonymous traversals into parent steps:

```rust
// Type-safe composition
fn where_<In, Out>(self, sub: Traversal<In, Out>) -> Traversal<In, In> {
    // sub must accept In (same as parent's current element type)
    // where_ always returns the same type it receives
}
```

---

## Steps That Accept Anonymous Traversals

Many traversal steps accept one or more anonymous traversals to enable filtering, branching, and complex control flow. This section details the most important steps.

### Filter Steps

#### `where_(sub)`

Filters the current traverser by executing a sub-traversal. The traverser is emitted only if the sub-traversal produces at least one result.

**Signature:**
```rust
fn where_<S>(self, sub: Traversal<E, S>) -> Traversal<In, E>
```

**Semantics:**
- Input traverser is cloned and fed to `sub`
- If `sub` yields any output, the original traverser is emitted
- If `sub` yields no output, the traverser is filtered out

**Example:**
```rust
// Find people who know someone named "Bob"
g.v().has_label("person")
    .where_(__.out_labels(&["knows"]).has_value("name", "Bob"))
    .values("name")
```

**ASCII Flow:**
```
Input: [Alice, Bob, Carol]

For Alice:
  Alice → __.out_labels(&["knows"]) → [Bob, Dave]
          → .has_value("name", "Bob") → [Bob]
  Result: non-empty → EMIT Alice

For Bob:
  Bob → __.out_labels(&["knows"]) → [Carol]
        → .has_value("name", "Bob") → []
  Result: empty → FILTER Bob

For Carol:
  Carol → __.out_labels(&["knows"]) → []
  Result: empty → FILTER Carol

Output: [Alice]
```

#### `not(sub)`

Inverse of `where_()`. Emits the traverser only if the sub-traversal produces zero results.

**Signature:**
```rust
fn not<S>(self, sub: Traversal<E, S>) -> Traversal<In, E>
```

**Example:**
```rust
// Find people who don't know anyone named "Bob"
g.v().has_label("person")
    .not(__.out_labels(&["knows"]).has_value("name", "Bob"))
```

#### `and_(subs...)`

Emits the traverser only if *all* sub-traversals produce at least one result.

**Signature:**
```rust
fn and_(self, subs: Vec<Traversal<E, ?>>) -> Traversal<In, E>
```

**Semantics:**
- Input traverser is cloned for each sub-traversal
- All sub-traversals are evaluated (no short-circuit)
- Traverser is emitted only if every sub-traversal yields results

**Example:**
```rust
// Find people who know both Bob AND work at Acme
g.v().has_label("person")
    .and_(vec![
        __.out_labels(&["knows"]).has_value("name", "Bob"),
        __.out_labels(&["works_at"]).has_value("name", "Acme")
    ])
```

#### `or_(subs...)`

Emits the traverser if *at least one* sub-traversal produces results.

**Signature:**
```rust
fn or_(self, subs: Vec<Traversal<E, ?>>) -> Traversal<In, E>
```

**Example:**
```rust
// Find people who know Bob OR work at Acme
g.v().has_label("person")
    .or_(vec![
        __.out_labels(&["knows"]).has_value("name", "Bob"),
        __.out_labels(&["works_at"]).has_value("name", "Acme")
    ])
```

---

### Branch Steps

#### `union(subs...)`

Executes multiple sub-traversals in parallel and merges their results. All sub-traversals receive the same input traverser.

**Signature:**
```rust
fn union<S>(self, subs: Vec<Traversal<E, S>>) -> Traversal<In, S>
```

**Semantics:**
- Each input traverser is cloned N times (where N = number of branches)
- All sub-traversals execute independently
- Results are interleaved in traverser-major order (see algorithms.md)

**Example:**
```rust
// Get both friends and colleagues of Alice
g.v().has_value("name", "Alice")
    .union(vec![
        __.out_labels(&["knows"]),
        __.out_labels(&["works_with"])
    ])
```

**ASCII Flow:**
```
Input: [Alice]

Branch 1: Alice → __.out("knows") → [Bob, Carol]
Branch 2: Alice → __.out("works_with") → [Dave]

Interleaving (traverser-major):
  Alice[branch1] → Bob
  Alice[branch2] → Dave
  Alice[branch1] → Carol

Output: [Bob, Dave, Carol]
```

**Note:** The exact interleaving order depends on the iterator implementation but follows traverser-major semantics—all results from Alice's branches before moving to the next input traverser.

#### `coalesce(subs...)`

Tries each sub-traversal in order until one produces results. Short-circuits on first success.

**Signature:**
```rust
fn coalesce<S>(self, subs: Vec<Traversal<E, S>>) -> Traversal<In, S>
```

**Semantics:**
- Sub-traversals are evaluated in order
- First sub-traversal that yields results "wins"
- Remaining sub-traversals are not evaluated
- If no sub-traversal yields results, the traverser is filtered out

**Example:**
```rust
// Prefer nickname, fall back to full name
g.v().has_label("person")
    .coalesce(vec![
        __.values("nickname"),
        __.values("first_name")
    ])
```

**ASCII Flow:**
```
Input: [Alice (has nickname), Bob (no nickname)]

For Alice:
  Try __.values("nickname") → ["Ally"]
  Result: non-empty → EMIT "Ally", skip remaining branches

For Bob:
  Try __.values("nickname") → []
  Try __.values("first_name") → ["Robert"]
  Result: non-empty → EMIT "Robert"

Output: ["Ally", "Robert"]
```

#### `choose(condition, if_true, if_false)`

Conditional branching based on a predicate traversal.

**Signature:**
```rust
fn choose<S>(
    self,
    condition: Traversal<E, ?>,
    if_true: Traversal<E, S>,
    if_false: Traversal<E, S>
) -> Traversal<In, S>
```

**Semantics:**
- Evaluates `condition` on input traverser
- If `condition` yields results → execute `if_true`
- If `condition` yields no results → execute `if_false`

**Example:**
```rust
// Different traversal based on vertex type
g.v().choose(
    __.has_label("person"),
    __.out("knows"),        // if person
    __.out("contains")      // if not person
)
```

#### `optional(sub)`

Tries a sub-traversal but keeps the original traverser if the sub-traversal produces no results.

**Signature:**
```rust
fn optional<S>(self, sub: Traversal<E, S>) -> Traversal<In, Either<E, S>>
```

**Semantics:**
- Execute `sub` on input traverser
- If `sub` yields results → emit those results
- If `sub` yields no results → emit the original traverser

**Example:**
```rust
// Get company name if person works somewhere, otherwise keep the person
g.v().has_label("person")
    .optional(__.out("works_at").values("name"))
```

---

### Repeat Step

The `repeat()` step enables iterative graph exploration with fine-grained control over termination and emission.

**Signature:**
```rust
fn repeat(self, sub: Traversal<E, E>) -> RepeatTraversal<In, E>
```

**Core Mechanism:**
```
Repeat Execution Flow:

Input → Repeat Loop → Output
         ↑       ↓
         └───────┘
         sub-trav

Pseudocode:
for each input traverser:
    loop_count = 0
    current = [input]
    frontier = []
    
    loop:
        loop_count += 1
        for each c in current:
            results = execute sub-traversal on c
            for each r in results:
                if should_emit(r, loop_count):
                    emit r
                if should_continue(r, loop_count):
                    frontier.add(r)
        
        if frontier is empty or should_break(loop_count):
            break
        
        current = frontier
        frontier = []
```

#### `.times(n)` - Fixed Iterations

Executes the sub-traversal exactly N times.

**Example:**
```rust
// Get friends-of-friends (2 hops)
g.v().has_value("name", "Alice")
    .repeat(__.out("knows"))
    .times(2)
```

**ASCII Diagram:**
```
Iteration 0: [Alice]
             ↓ __.out("knows")
Iteration 1: [Bob, Carol]
             ↓ __.out("knows")
Iteration 2: [Dave, Eve, Frank]
             (stop - times(2) reached)

Output: [Dave, Eve, Frank]
```

#### `.until(sub)` - Conditional Termination

Continues until the termination condition is met.

**Example:**
```rust
// Traverse until we find a company
g.v().has_value("name", "Alice")
    .repeat(__.out())
    .until(__.has_label("company"))
```

**Semantics:**
- Before each iteration, check `until` condition on current traverser
- If condition is satisfied, stop iterating for that traverser
- Can combine with `.emit()` to collect intermediate results

#### `.emit()` - Emit All Iterations

Emits traversers from every iteration (including initial input if specified).

**Example:**
```rust
// Get all vertices within 3 hops
g.v().has_value("name", "Alice")
    .repeat(__.out())
    .times(3)
    .emit()
```

**ASCII Diagram:**
```
Input:       [Alice]         (emit if .emit_first())
Iteration 1: [Bob, Carol]    (emit)
Iteration 2: [Dave, Eve]     (emit)
Iteration 3: [Frank]         (emit)

Output: [Alice?, Bob, Carol, Dave, Eve, Frank]
```

#### `.emit_if(sub)` - Conditional Emission

Emits traversers that satisfy a condition.

**Example:**
```rust
// Traverse up to 5 hops, emit only people
g.v().repeat(__.out())
    .times(5)
    .emit_if(__.has_label("person"))
```

**Combined Example:**
```rust
// Find all reachable vertices within 5 hops, avoiding duplicates
g.v_by_ids([start_id])
    .repeat(__.out())
    .times(5)
    .emit()
    .dedup()
```

**Breadth-First Frontier Processing:**

The `repeat()` step processes the graph in breadth-first order by default, maintaining a frontier of traversers at the current depth:

```
Graph:
  A → B → D
  A → C → E → F

Execution of .repeat(__.out()).times(3).emit():

Depth 0: [A]                  (emit A)
Depth 1: [B, C]               (emit B, C)
Depth 2: [D, E]               (emit D, E)
Depth 3: [F]                  (emit F)
```

---

### Scope Steps

#### `local(sub)`

Executes a sub-traversal in an isolated scope, preventing it from affecting the path or parent state.

**Signature:**
```rust
fn local<S>(self, sub: Traversal<E, S>) -> Traversal<In, S>
```

**Use Case:**
Aggregations or transformations that should operate independently:

```rust
// Get friend count for each person (not total across all people)
g.v().has_label("person")
    .local(__.out("knows").count())
```

Without `local()`, aggregation steps like `count()` might operate across all traversers in the pipeline. With `local()`, each input traverser gets its own independent count.

#### `map(sub)` - When Accepting Traversals

Transforms each traverser by applying a sub-traversal.

**Signature:**
```rust
fn map<S>(self, sub: Traversal<E, S>) -> Traversal<In, S>
```

**Example:**
```rust
// Get names of all friends
g.v().has_label("person")
    .map(__.out("knows").values("name"))
```

**Note:** `map()` can also accept closures/functions. When using anonymous traversals, it behaves similarly to chaining the traversal directly but can provide scoping semantics.

#### `flat_map(sub)`

Similar to `map()`, but flattens nested results.

**Signature:**
```rust
fn flat_map<S>(self, sub: Traversal<E, S>) -> Traversal<In, S>
```

**Example:**
```rust
// Get all hobbies of all friends
g.v().has_label("person")
    .flat_map(__.out("knows").values("hobbies"))
```

---

## Implementation Architecture

### Anonymous Traversal Type Structure

At the core, an anonymous traversal is a sequence of step instances without a bound graph:

```rust
// Simplified conceptual representation
pub struct Traversal<In, Out> {
    graph: Option<Arc<Graph>>,           // None for anonymous
    steps: Vec<Box<dyn Step<?, ?>>>,     // Step pipeline
    phantom: PhantomData<(In, Out)>,     // Type markers
}

impl<In, Out> Traversal<In, Out> {
    pub fn new_anonymous(steps: Vec<Box<dyn Step<?, ?>>>) -> Self {
        Self {
            graph: None,
            steps,
            phantom: PhantomData,
        }
    }
    
    pub fn new_bound(graph: Arc<Graph>, steps: Vec<Box<dyn Step<?, ?>>>) -> Self {
        Self {
            graph: Some(graph),
            steps,
            phantom: PhantomData,
        }
    }
}
```

The `graph` field distinguishes bound from anonymous traversals:
- `Some(graph)` → Bound traversal with a data source
- `None` → Anonymous traversal template

### Binding Mechanism

When an anonymous traversal is embedded in a parent step, it receives its input dynamically:

```
Parent Step Execution:

┌───────────────────────────────────┐
│ WhereStep {                       │
│   sub_traversal: AnonymousTraversal │
│ }                                 │
└───────────────────────────────────┘
                ↓
        next() called
                ↓
┌───────────────────────────────────┐
│ 1. Pull traverser from upstream   │
│    input_traverser = parent.next()│
└───────────────────────────────────┘
                ↓
┌───────────────────────────────────┐
│ 2. Create execution context       │
│    ctx = ExecutionContext {       │
│      input: input_traverser,      │
│      graph: parent.graph(),       │
│      path: input_traverser.path,  │
│      sack: input_traverser.sack   │
│    }                              │
└───────────────────────────────────┘
                ↓
┌───────────────────────────────────┐
│ 3. Bind anonymous traversal       │
│    bound = sub_traversal.bind(ctx)│
└───────────────────────────────────┘
                ↓
┌───────────────────────────────────┐
│ 4. Execute bound traversal        │
│    results = bound.collect()      │
└───────────────────────────────────┘
                ↓
┌───────────────────────────────────┐
│ 5. Make filtering decision        │
│    if !results.is_empty() {       │
│      emit input_traverser         │
│    }                              │
└───────────────────────────────────┘
```

**Key Points:**
- The anonymous traversal receives a clone of the input traverser
- The graph reference is inherited from the parent traversal
- Path, sack, and other context are preserved
- Each execution creates a fresh iterator pipeline

### Integration with Traverser State

Anonymous traversals must preserve and extend traverser state correctly:

**Path Tracking:**
When steps inside an anonymous traversal use `.as_("label")`, those labels extend the path:

```rust
g.v().as_("person")
    .where_(
        __.out("knows")
          .as_("friend")
          .has_value("age", 30)
    )
    .select(&["person", "friend"])
```

Even though `"friend"` is labeled inside an anonymous traversal, it's accessible in the parent's `.select()` because paths are preserved across the boundary.

**Path Preservation Logic:**
```rust
// Conceptual
struct Traverser {
    element: Element,
    path: Path,           // History of traversed elements
    sack: Option<Value>,  // Mutable side-effect carrier
    loops: usize,         // Loop counter for repeat()
}

// When binding anonymous traversal
fn bind_anonymous(sub: &Traversal, input: Traverser) -> BoundTraversal {
    let bound = sub.clone();
    bound.graph = input.graph;
    bound.initial_traverser = input.clone();  // Preserve path, sack, loops
    bound
}
```

**Sack Handling:**
Sacks (mutable values carried with traversers) are preserved:

```rust
g.v().has_label("person")
    .sack_init(|| 0)
    .repeat(
        __.out("knows")
          .sack_add_by(__.values("age"))
    )
    .times(2)
    .sack()
```

Each iteration of the anonymous traversal modifies the same sack value.

**Loop Counter Access:**
Inside `repeat()`, the loop counter is accessible to nested anonymous traversals:

```rust
g.v().repeat(
    __.out()
      .where_(__.loops().is(P::gt(2)))  // Access loop count
)
```

### Step Cloning for Multi-Branch

Steps like `union()` and `coalesce()` need to execute the same anonymous traversal multiple times. This requires cloning:

```rust
pub trait Step: Clone {
    fn next(&mut self) -> Option<Traverser>;
}

// Union step implementation (simplified)
struct UnionStep {
    branches: Vec<Traversal>,  // Cloned for each input traverser
}

impl Step for UnionStep {
    fn next(&mut self) -> Option<Traverser> {
        // For each input traverser:
        //   1. Clone all branch traversals
        //   2. Bind each to the input
        //   3. Interleave results
    }
}
```

**ReplayableIter Pattern:**

To avoid materialization, branches can use the `ReplayableIter` pattern from algorithms.md:

```rust
// Instead of collecting all results
let results: Vec<_> = sub_trav.collect();

// Use ReplayableIter to preserve laziness
let mut iter1 = sub_trav.iter();
let mut iter2 = sub_trav.iter();  // Independent iterator over same data
```

This pattern is crucial for memory efficiency when branches might produce large result sets.

---

## Execution Semantics

### Evaluation Model

Anonymous traversals preserve the pull-based lazy evaluation model:

```
Pull-Based Execution:

Consumer                    Anonymous Traversal         Upstream
   ↓                               ↓                        ↓
next() ──────────────→ next() ──────────────→ next() ──────────────→ Source
   ↑                               ↑                        ↑
return ←────────────── return ←─────────────── return ←───────────────
```

**Per-Traverser Execution:**

Each input traverser executes the anonymous traversal independently:

```rust
// Parent traversal with 3 input traversers
g.v().limit(3)                       // [v1, v2, v3]
    .where_(__.out().count().is(P::gt(5)))

Execution:
  v1 → __.out().count() → 7  → gt(5)? YES → EMIT v1
  v2 → __.out().count() → 3  → gt(5)? NO  → FILTER
  v3 → __.out().count() → 10 → gt(5)? YES → EMIT v3

Output: [v1, v3]
```

Each execution is isolated—no state leaks between traversers (except intentional side effects like sacks or global stores).

**ASCII Data Flow:**
```
Input Stream:    [t1, t2, t3, ...]
                  ↓   ↓   ↓
                ┌─────────────┐
                │ Parent Step │
                │  (where_)   │
                └─────────────┘
                  ↓   ↓   ↓
              ┌───┬───┬───┐
              │   │   │   │
      ┌───────┘   │   └───────┐
      ↓           ↓           ↓
  [t1 clone]  [t2 clone]  [t3 clone]
      ↓           ↓           ↓
  ┌───────┐   ┌───────┐   ┌───────┐
  │ Anon  │   │ Anon  │   │ Anon  │
  │ Exec  │   │ Exec  │   │ Exec  │
  └───────┘   └───────┘   └───────┘
      ↓           ↓           ↓
   [result]   [result]   [result]
      ↓           ↓           ↓
   (pass?)    (pass?)    (pass?)
      ↓           ↓           ↓
Output Stream:   [t1, t3] (t2 filtered)
```

### Ordering Guarantees

Different steps provide different ordering guarantees when executing anonymous traversals.

**`union()` - Traverser-Major Interleaving:**

Results from all branches of a single input traverser are emitted before processing the next input:

```rust
Input: [A, B]
union([__.out("x"), __.out("y")])

Execution:
  A → branch1 → [A1, A2]
  A → branch2 → [A3]
  B → branch1 → [B1]
  B → branch2 → [B2, B3]

Output order (traverser-major):
  [A1, A2, A3, B1, B2, B3]
  └── A results ──┘ └── B results ──┘
```

This is in contrast to step-major ordering, which would be `[A1, B1, A2, B2, A3, B3]`. Traverser-major ordering better preserves locality and enables pipeline optimizations.

**`coalesce()` - Short-Circuit Evaluation:**

Branches are evaluated in order, and evaluation stops at the first success:

```rust
Input: [A, B]
coalesce([__.out("x"), __.out("y"), __.out("z")])

For A:
  __.out("x") → []         (try branch 2)
  __.out("y") → [A1]       (success! skip branch 3)

For B:
  __.out("x") → [B1, B2]   (success! skip branches 2 & 3)

Output: [A1, B1, B2]
```

**`repeat()` - Breadth-First Frontier:**

By default, `repeat()` processes the graph in breadth-first order:

```rust
g.v_by_ids([v1])
    .repeat(__.out())
    .times(3)
    .emit()

Depth 0: [v1]           → emit
Depth 1: [v2, v3]       → emit
Depth 2: [v4, v5, v6]   → emit
Depth 3: [v7, v8]       → emit

Output order: [v1, v2, v3, v4, v5, v6, v7, v8]
```

### Path Tracking

Paths track the history of traversed elements and labels. Anonymous traversals extend paths seamlessly:

**Example:**
```rust
g.v().as_("start")
    .out("knows").as_("friend")
    .where_(
        __.out("works_at")
          .as_("company")
          .has_value("name", "Acme")
    )
    .select(&["start", "friend", "company"])
```

**Path Structure at `.select()`:**
```
Traverser path:
  [v1 (labeled "start"),
   v2 (labeled "friend"),
   v5 (labeled "company")]
```

Even though `"company"` was labeled inside the anonymous traversal within `where_()`, it's part of the path and accessible in `select()`.

**Path Extension Rules:**
- Labels applied inside anonymous traversals extend the current path
- Path is preserved when traversers enter and exit anonymous traversals
- Paths are cloned when traversers split (e.g., in `union()`)

**Path Isolation with `local()`:**
Some steps like `local()` can isolate path modifications:

```rust
g.v().as_("person")
    .local(
        __.out("knows").as_("friend")
    )
    .select(&["person"])  // "friend" not accessible here
```

---

## Usage Patterns & Examples

### Basic Filtering

**Find people who know someone named "Bob":**
```rust
g.v().has_label("person")
    .where_(__.out("knows").has_value("name", "Bob"))
    .values("name")
```

**Find people who don't work anywhere:**
```rust
g.v().has_label("person")
    .not(__.out("works_at"))
    .values("name")
```

**Find people who know Bob AND work at Acme:**
```rust
g.v().has_label("person")
    .and_(vec![
        __.out("knows").has_value("name", "Bob"),
        __.out("works_at").has_value("name", "Acme")
    ])
```

### Conditional Logic

**Different traversal based on vertex type:**
```rust
g.v().choose(
    __.has_label("person"),
    __.out("knows"),        // if person, get friends
    __.out("contains")      // otherwise, get children
)
```

**Preference-based selection:**
```rust
g.v().coalesce(vec![
    __.values("nickname"),       // prefer nickname
    __.values("first_name"),     // fall back to first name
    __.constant("Anonymous")     // ultimate fallback
])
```

### Multi-Path Exploration

**Get both friends and colleagues:**
```rust
g.v().has_value("name", "Alice")
    .union(vec![
        __.out("knows"),
        __.out("works_with")
    ])
    .dedup()
    .values("name")
```

**Get multiple properties:**
```rust
g.v().has_label("person")
    .union(vec![
        __.values("name"),
        __.values("email"),
        __.out("works_at").values("name")
    ])
```

### Fallback Patterns

**Prefer nickname, fall back to name:**
```rust
g.v().coalesce(vec![
    __.values("nickname"),
    __.values("name")
])
```

**Try multiple edge types:**
```rust
g.v().has_value("name", "Alice")
    .coalesce(vec![
        __.out("prefers"),      // try preferred connection
        __.out("knows"),        // fall back to knows
        __.out("colleague")     // final fallback
    ])
```

### Recursive Graph Exploration

**Find all reachable vertices within 5 hops:**
```rust
g.v_by_ids([start_id])
    .repeat(__.out())
    .times(5)
    .emit()
    .dedup()
```

**Traverse until finding a company:**
```rust
g.v().has_label("person")
    .repeat(__.out())
    .until(__.has_label("company"))
```

**Depth-limited exploration with filtering:**
```rust
g.v().has_value("name", "Alice")
    .repeat(__.out("knows"))
    .times(3)
    .emit_if(__.has_value("age", P::gt(25)))
    .values("name")
```

### Complex Real-World Examples

#### Recommendation Engine Pattern

Find friends-of-friends who share hobbies but aren't already friends:

```rust
g.v().has_value("name", "Alice")
    .as_("me")
    .out("knows").as_("friend")
    .out("knows").as_("fof")
    .where_(
        __.and_(vec![
            // Not already friends
            __.not(__.select("me").out("knows").where_(P::eq("fof"))),
            // Share at least one hobby
            __.select("me")
              .values("hobbies")
              .where_(P::within(__.select("fof").values("hobbies")))
        ])
    )
    .select(&["fof"])
    .dedup()
    .values("name")
```

#### Shortest Path with Constraints

Find shortest path from Alice to Bob, avoiding blocked vertices:

```rust
g.v().has_value("name", "Alice")
    .as_("start")
    .repeat(
        __.out()
          .not(__.has_value("blocked", true))
          .simplePath()  // Avoid cycles
    )
    .until(__.has_value("name", "Bob"))
    .limit(1)
    .path()
```

#### Hierarchical Data Traversal

Navigate organizational hierarchy, collecting all managers up to C-level:

```rust
g.v().has_value("name", "John")
    .repeat(__.out("reports_to"))
    .until(__.has_value("level", "C-level"))
    .emit()
    .values("name")
```

**Complex aggregation:**

Count teammates by department:

```rust
g.v().has_label("person")
    .group_by(
        __.out("works_in").values("department"),  // key
        __.out("works_with").dedup().count()      // value
    )
```

---

## Performance Considerations

### Overhead Analysis

**Anonymous Traversal Instantiation Cost:**

Creating an anonymous traversal is O(1)—it allocates a small struct and stores step references. No graph data is accessed or traversed.

```rust
// Negligible cost
let anon = __.out("knows").has_value("age", 30);
```

**Cloning Costs for Multi-Branch Steps:**

Steps like `union()` and `coalesce()` clone anonymous traversals for each input traverser:

```rust
// If input has 1000 traversers and union has 3 branches:
// → 3000 anonymous traversal clones
g.v().limit(1000)
    .union(vec![
        __.out("a"),
        __.out("b"),
        __.out("c")
    ])
```

**Mitigation:**
- Traversal cloning should be shallow (clone step references, not data)
- Use `Arc<Step>` for step sharing when possible
- Avoid wide unions with large fanout

**Memory Allocation Patterns:**

Each anonymous traversal execution creates:
- A fresh iterator pipeline (O(1) per step)
- Traverser clones (O(path length))
- Intermediate result buffers (only if materialization is required)

The pull-based model ensures memory usage is proportional to pipeline depth, not result set size.

### Optimization Opportunities

**Query Planner Pushing Filters:**

A query optimizer can push filters into anonymous traversals:

```rust
// Before optimization
g.v().union(vec![
    __.out("a"),
    __.out("b")
]).has_label("person")

// After optimization
g.v().union(vec![
    __.out("a").has_label("person"),
    __.out("b").has_label("person")
])
```

Early filtering reduces intermediate result sets.

**Index Utilization Within Anonymous Traversals:**

Steps inside anonymous traversals can leverage indexes:

```rust
g.v().where_(
    __.out("knows")
      .has_value("name", "Bob")  // Can use name index
)
```

The query planner should recognize index opportunities even within nested traversals.

**Early Termination in `coalesce()` and `until()`:**

Short-circuit evaluation saves work:

```rust
// If first branch succeeds, remaining branches aren't evaluated
coalesce(vec![
    __.values("preferred_name"),  // If this exists...
    __.values("legal_name")       // ...this never runs
])
```

Similarly, `repeat().until()` stops as soon as the condition is satisfied.

### Best Practices

**Prefer `where_()` over `filter()` with Nested Traversal:**

```rust
// Better (explicit filtering intent for optimizer)
.where_(__.out("knows").has_value("name", "Bob"))

// Less optimal (generic filter with closure)
.filter(|t| {
    let results = t.clone().out("knows").has_value("name", "Bob").collect();
    !results.is_empty()
})
```

**Use `limit()` Inside `repeat()` When Appropriate:**

```rust
// Limit each iteration's fanout
.repeat(__.out().limit(10))
.times(5)
```

This prevents exponential explosion in high-degree graphs.

**Avoid Deep Nesting When Possible:**

```rust
// Hard to optimize
.where_(
    __.where_(
        __.where_(
            __.out().has_value("x", 1)
        )
    )
)

// Better - flatten
.where_(__.out().has_value("x", 1))
```

**Use `dedup()` After Exploration:**

```rust
// Remove duplicate paths
.repeat(__.both()).times(3).emit()
.dedup()
```

**Leverage `simplePath()` to Avoid Cycles:**

```rust
// Prevent infinite loops in cyclic graphs
.repeat(__.out().simplePath())
.times(10)
```

---

## Comparison with Bound Traversals

### When to Use Anonymous vs Bound Traversals

**Use Anonymous Traversals When:**
- Defining reusable filtering or transformation logic
- Embedding traversal logic within parent steps (`where_`, `union`, `repeat`, etc.)
- Building modular, composable query components
- Testing traversal fragments in isolation

**Use Bound Traversals When:**
- Starting a query from a graph source
- Performing standalone graph exploration
- Directly iterating over results
- No need for embedding in parent steps

**Example:**

```rust
// Bound traversal - standalone query
let friends = g.v()
    .has_value("name", "Alice")
    .out("knows")
    .collect();

// Anonymous traversal - reusable filter
let knows_bob = __.out("knows").has_value("name", "Bob");

let people_who_know_bob = g.v()
    .has_label("person")
    .where_(knows_bob.clone())  // Reuse
    .collect();

let companies_with_bob_employees = g.v()
    .has_label("company")
    .where_(__.in_("works_at").where_(knows_bob))  // Reuse
    .collect();
```

### Composability Benefits

Anonymous traversals enable function-like composition:

```rust
// Define reusable fragments
fn adult_filter() -> Traversal<Vertex, Vertex> {
    __.has_value("age", P::gte(18))
}

fn employed_filter() -> Traversal<Vertex, Vertex> {
    __.out("works_at").has_label("company")
}

// Compose
g.v().has_label("person")
    .where_(adult_filter())
    .where_(employed_filter())
```

This is more maintainable than copy-pasting traversal logic.

### Testing Anonymous Traversals in Isolation

Anonymous traversals can be tested independently by binding them to test data:

```rust
#[test]
fn test_adult_filter() {
    let graph = create_test_graph();
    let traversal = __.has_value("age", P::gte(18));
    
    let adults = graph.v()
        .where_(traversal)
        .collect();
    
    assert_eq!(adults.len(), 5);
}
```

This promotes modular, testable query logic.

---

## Complexity Summary

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Anonymous traversal creation | O(1) | Lazy, no execution |
| `where_()` evaluation | O(k) per traverser | k = sub-traversal cost |
| `not()` evaluation | O(k) per traverser | k = sub-traversal cost |
| `and_()` with n branches | O(n × k) | All branches evaluated |
| `or_()` with n branches | O(n × k) worst case | No short-circuit in standard impl |
| `union()` with n branches | O(n × k) | All branches executed, results merged |
| `coalesce()` with n branches | O(1) to O(n × k) | Short-circuits on first success |
| `choose()` | O(k_cond + k_branch) | Condition + one branch |
| `optional()` | O(k) | Single sub-traversal |
| `repeat().times(m)` | O(m × k) | k = step cost per iteration |
| `repeat().until()` | O(d × k) | d = depth until condition met |
| `local()` | O(k) | k = sub-traversal cost |
| `map()` / `flat_map()` | O(k) per traverser | k = sub-traversal cost |

**Complexity Variables:**
- `n` = number of branches in multi-branch steps
- `k` = cost of executing the sub-traversal (depends on graph structure)
- `m` = number of iterations in `repeat().times(m)`
- `d` = depth reached in `repeat().until()`

**Notes:**
- Costs are per-traverser (each input traverser incurs the cost)
- Actual runtime depends on graph density and sub-traversal selectivity
- Index utilization can reduce `k` significantly
- Lazy evaluation ensures memory is proportional to pipeline depth, not result size

---

## Conclusion

Anonymous traversals are a powerful abstraction for building modular, composable graph queries. By decoupling traversal logic from graph sources, they enable:

- **Reusability**: Define once, use in multiple contexts
- **Composability**: Combine simple fragments into complex queries
- **Testability**: Isolate and test traversal logic independently
- **Optimization**: Enable query planners to reason about and optimize nested traversals

When used effectively with steps like `where_()`, `union()`, `repeat()`, and `coalesce()`, anonymous traversals unlock expressive and efficient graph exploration patterns. Understanding their execution semantics—lazy evaluation, per-traverser isolation, and path preservation—is key to writing performant graph queries.
