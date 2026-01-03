# Anonymous Traversals

## Overview

Anonymous traversals are a fundamental concept in graph traversal systems, enabling composable, reusable traversal fragments that can be embedded within parent traversals. Unlike bound traversals that start from a concrete graph source (e.g., `g.v()` or `g.e()`), anonymous traversals are unbound templates that receive their input at execution time.

**Key Architectural Point**: In the type-erased architecture, anonymous traversals use the **same `Traversal<In, Out>` type** as bound traversals. The difference is that anonymous traversals have no source—they're pure step pipelines that receive their `ExecutionContext` when spliced into a parent traversal.

### The `__` Convention

By convention, anonymous traversals are created using the double underscore `__` factory module. This syntactic marker clearly distinguishes anonymous traversal fragments from their bound counterparts:

```rust
// Bound traversal - starts from the graph, wrapped in BoundTraversal
g.v().has_label("person").out_labels(&["knows"])

// Anonymous traversal - same Traversal type, but no source
// Returns Traversal<Value, Value>
__.out_labels(&["knows"]).has_value("name", "Alice")
```

The `__` module provides the same rich API as bound traversals, but returns `Traversal<Value, Value>` (or other type combinations) instead of `BoundTraversal`. The resulting traversal object is detached from any specific graph instance until it's composed into a parent traversal.

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

The `__` module is the entry point for creating anonymous traversals. It provides static methods that return `Traversal<Value, Value>` instances—the same type used internally by bound traversals, but without a source.

### Module Structure

```rust
/// Anonymous traversal factory
/// 
/// Returns Traversal<Value, Value> for most methods.
/// These traversals have no source and receive ExecutionContext
/// when spliced into a parent traversal via append().
pub mod __ {
    use super::*;

    /// Identity traversal - passes input through unchanged
    pub fn identity() -> Traversal<Value, Value> {
        Traversal::new().add_step(IdentityStep)
    }
    
    /// Constant emission - ignores input, emits constant value
    pub fn constant(value: impl Into<Value>) -> Traversal<Value, Value> {
        Traversal::new().add_step(ConstantStep::new(value))
    }
    
    /// Navigation steps - traverse graph structure
    pub fn out() -> Traversal<Value, Value> {
        Traversal::new().add_step(OutStep::new())
    }
    
    pub fn out_labels(labels: &[&str]) -> Traversal<Value, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        Traversal::new().add_step(OutStep::with_labels(labels))
    }
    
    pub fn in_() -> Traversal<Value, Value> {
        Traversal::new().add_step(InStep::new())
    }
    
    pub fn in_labels(labels: &[&str]) -> Traversal<Value, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        Traversal::new().add_step(InStep::with_labels(labels))
    }
    
    pub fn both() -> Traversal<Value, Value> {
        Traversal::new().add_step(BothStep::new())
    }
    
    pub fn both_labels(labels: &[&str]) -> Traversal<Value, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        Traversal::new().add_step(BothStep::with_labels(labels))
    }
    
    /// Edge navigation
    pub fn out_e() -> Traversal<Value, Value> {
        Traversal::new().add_step(OutEStep::new())
    }
    
    pub fn out_e_labels(labels: &[&str]) -> Traversal<Value, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        Traversal::new().add_step(OutEStep::with_labels(labels))
    }
    
    pub fn in_e() -> Traversal<Value, Value> {
        Traversal::new().add_step(InEStep::new())
    }
    
    pub fn in_e_labels(labels: &[&str]) -> Traversal<Value, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        Traversal::new().add_step(InEStep::with_labels(labels))
    }
    
    /// Property access
    pub fn values(key: &str) -> Traversal<Value, Value> {
        Traversal::new().add_step(ValuesStep::new(key))
    }
    
    pub fn label() -> Traversal<Value, Value> {
        Traversal::new().add_step(LabelStep)
    }
    
    pub fn id() -> Traversal<Value, Value> {
        Traversal::new().add_step(IdStep)
    }
    
    /// Filtering
    pub fn has(key: &str) -> Traversal<Value, Value> {
        Traversal::new().add_step(HasStep::new(key))
    }
    
    pub fn has_value(key: &str, value: impl Into<Value>) -> Traversal<Value, Value> {
        Traversal::new().add_step(HasValueStep::new(key, value))
    }
    
    pub fn has_label(label: &str) -> Traversal<Value, Value> {
        Traversal::new().add_step(HasLabelStep::single(label))
    }
    
    pub fn has_label_any(labels: &[&str]) -> Traversal<Value, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        Traversal::new().add_step(HasLabelStep::new(labels))
    }
    
    pub fn has_id(id: impl Into<Value>) -> Traversal<Value, Value> {
        // HasIdStep filters by vertex/edge ID
        Traversal::new().add_step(HasIdStep::from_value(id.into()))
    }
    
    /// Control flow
    pub fn dedup() -> Traversal<Value, Value> {
        Traversal::new().add_step(DedupStep)
    }
    
    pub fn limit(n: usize) -> Traversal<Value, Value> {
        Traversal::new().add_step(LimitStep::new(n))
    }
}
```

### Chainable API

Once an anonymous traversal is created with a starting method, all subsequent steps can be chained exactly like bound traversals. The `Traversal<In, Out>` type provides the same fluent methods:

```rust
// Start anonymous, chain multiple steps
// Each step returns a new Traversal with potentially different Out type
let complex_anon: Traversal<Value, Value> = __.out_labels(&["knows"])
    .has_value("age", 30)
    .out_labels(&["works_at"])
    .has_label("company")
    .values("name");

// Use within parent traversal via append() or step methods
g.v().has_label("person")
    .where_(complex_anon)  // where_ accepts Traversal<Value, Value>
```

The type system ensures that steps are compatible. Since the type-erased architecture uses `Value` internally, most anonymous traversals are `Traversal<Value, Value>`.

### Type Parameters

Anonymous traversals use the same `Traversal<In, Out>` type as the internal representation:

```rust
/// Main traversal type - same for bound and anonymous
/// 
/// In = expected input type (phantom, for compile-time checking)
/// Out = produced output type (phantom, for compile-time checking)
/// 
/// Internally, all values flow as `Value` through `Box<dyn AnyStep>`
pub struct Traversal<In, Out> {
    steps: Vec<Box<dyn AnyStep>>,
    source: Option<TraversalSource>,  // None for anonymous
    _phantom: PhantomData<fn(In) -> Out>,
}
```

Most anonymous traversals are `Traversal<Value, Value>` because:
- They accept any `Value` as input (vertex, edge, property, etc.)
- They produce `Value` as output
- The phantom types provide API safety without runtime overhead

The key methods for composition:

```rust
impl<In, Out> Traversal<In, Out> {
    /// Add a step, returning traversal with new output type
    pub fn add_step<NewOut>(self, step: impl AnyStep + 'static) -> Traversal<In, NewOut>;
    
    /// Append another traversal's steps (for splicing)
    /// Out of self must match In of other
    pub fn append<Mid>(self, other: Traversal<Out, Mid>) -> Traversal<In, Mid>;
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

### Unified Traversal Type

In the type-erased architecture, both bound and anonymous traversals use the **same `Traversal<In, Out>` type**. The key difference is the presence of a source:

```rust
/// Main traversal type - unified for bound and anonymous
pub struct Traversal<In, Out> {
    /// Type-erased steps
    steps: Vec<Box<dyn AnyStep>>,
    /// Source (Some for bound, None for anonymous)
    source: Option<TraversalSource>,
    /// Phantom types for API safety
    _phantom: PhantomData<fn(In) -> Out>,
}

/// Bound traversals are wrapped with graph context
pub struct BoundTraversal<'g, In, Out> {
    snapshot: &'g GraphSnapshot<'g>,
    interner: &'g StringInterner,
    traversal: Traversal<In, Out>,  // Contains the steps
}
```

| Aspect | Bound Traversal | Anonymous Traversal |
|--------|-----------------|---------------------|
| Type | `BoundTraversal<'g, In, Out>` | `Traversal<In, Out>` |
| Created via | `g.v()`, `g.e()` | `__.out()`, `__.has_label()` |
| Has source? | Yes (via wrapper) | No (`source: None`) |
| Graph access | Via `BoundTraversal` wrapper | Via `ExecutionContext` at splice |
| `In` type | `()` (starts from nothing) | `Value` (input element type) |
| Execution | Direct (has context) | Must be spliced into parent |

### Binding Mechanism: The `append()` Method

When an anonymous traversal is used within a parent step, its steps are merged via `append()`:

```rust
impl<In, Out> Traversal<In, Out> {
    /// Append another traversal's steps
    /// 
    /// This is how anonymous traversals are "spliced" into parents.
    /// The anonymous traversal's steps are added to self's pipeline.
    pub fn append<Mid>(mut self, other: Traversal<Out, Mid>) -> Traversal<In, Mid> {
        self.steps.extend(other.steps);
        Traversal {
            steps: self.steps,
            source: self.source,  // Preserve parent's source
            _phantom: PhantomData,
        }
    }
}

impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Append anonymous traversal to bound traversal
    pub fn append<Mid>(self, anon: Traversal<Out, Mid>) -> BoundTraversal<'g, In, Mid> {
        BoundTraversal {
            snapshot: self.snapshot,
            interner: self.interner,
            traversal: self.traversal.append(anon),
        }
    }
}
```

**Execution Flow:**

```
Parent Step Execution (e.g., where_):

┌───────────────────────────────────┐
│ BoundTraversal contains:          │
│   snapshot: &GraphSnapshot        │
│   interner: &StringInterner       │
│   traversal: Traversal<In, Out>   │
│     └── steps: [HasLabelStep, ... │
│         ... + anonymous steps]    │
└───────────────────────────────────┘
                ↓
        Terminal step called (to_list, next, etc.)
                ↓
┌───────────────────────────────────┐
│ 1. Create ExecutionContext        │
│    ctx = ExecutionContext {       │
│      snapshot: self.snapshot,     │
│      interner: self.interner,     │
│      side_effects: SideEffects    │
│    }                              │
└───────────────────────────────────┘
                ↓
┌───────────────────────────────────┐
│ 2. Start with source traversers   │
│    if let Some(src) = source {    │
│      StartStep::apply(ctx, src)   │
│    }                              │
└───────────────────────────────────┘
                ↓
┌───────────────────────────────────┐
│ 3. Apply each step in sequence    │
│    for step in steps {            │
│      current = step.apply(ctx,    │
│                          current) │
│    }                              │
│    // Anonymous steps receive     │
│    // same ctx as parent steps!   │
└───────────────────────────────────┘
                ↓
┌───────────────────────────────────┐
│ 4. Collect results                │
│    current.collect() or iterate   │
└───────────────────────────────────┘
```

**Key Insight:** Anonymous traversal steps receive the **same `ExecutionContext`** as parent steps. This is the magic that makes them work—graph access is provided at execution time, not construction time.

### Integration with Traverser State

Anonymous traversals must preserve and extend traverser state correctly. The `Traverser` type carries metadata alongside the `Value`:

```rust
/// Non-generic Traverser - uses Value internally
#[derive(Clone)]
pub struct Traverser {
    pub value: Value,           // Current element (vertex, edge, property, etc.)
    pub path: Path,             // History of traversed elements
    pub loops: u32,             // Loop counter for repeat()
    pub sack: Option<Box<dyn CloneSack>>,  // Mutable side-effect carrier
    pub bulk: u64,              // Bulk count optimization
}
```

**Path Tracking:**
When steps inside an anonymous traversal use `.as_("label")`, those labels extend the path:

```rust
g.v().as_("person")
    .where_(
        __.out_labels(&["knows"])
          .as_("friend")  // Label added inside anonymous traversal
          .has_value("age", 30)
    )
    .select(&["person", "friend"])  // Both labels accessible!
```

Even though `"friend"` is labeled inside an anonymous traversal, it's accessible in the parent's `.select()` because paths are preserved. The `Traverser.path` is cloned when entering the anonymous traversal and modifications persist.

**Path Preservation via `split()`:**
```rust
impl Traverser {
    /// Split traverser for branching - preserves path and metadata
    pub fn split(&self, new_value: impl Into<Value>) -> Traverser {
        Traverser {
            value: new_value.into(),
            path: self.path.clone(),    // Path preserved!
            loops: self.loops,          // Loop count preserved
            sack: self.sack.clone(),    // Sack preserved
            bulk: self.bulk,
        }
    }
}
```

**Sack Handling:**
Sacks (mutable values carried with traversers) are preserved across anonymous traversal boundaries:

```rust
g.v().has_label("person")
    .sack_init(|| Value::Int(0))
    .repeat(
        __.out_labels(&["knows"])
          .sack_add_by(__.values("age"))  // Nested anonymous in sack_add_by
    )
    .times(2)
    .sack()  // Returns accumulated value
```

**Loop Counter Access:**
Inside `repeat()`, the loop counter is accessible via the traverser's `loops` field:

```rust
g.v().repeat(
    __.out()
      .filter(|ctx, v| {
          // Access loop count from traverser (via closure capture)
          // Note: actual API may differ
      })
)
```

### Step Cloning for Multi-Branch

Steps like `union()` and `coalesce()` need to execute the same anonymous traversal multiple times. The `AnyStep` trait requires `clone_box()` to enable this:

```rust
/// Type-erased step trait - all steps implement this
pub trait AnyStep: Send + Sync {
    /// Apply step to input, producing output
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a>;

    /// Clone into boxed trait object - enables traversal cloning
    fn clone_box(&self) -> Box<dyn AnyStep>;
    
    /// Step name for debugging
    fn name(&self) -> &'static str;
}

// Traversal cloning uses clone_box()
impl<In, Out> Clone for Traversal<In, Out> {
    fn clone(&self) -> Self {
        Self {
            steps: self.steps.iter().map(|s| s.clone_box()).collect(),
            source: self.source.clone(),
            _phantom: PhantomData,
        }
    }
}
```

**Union Step Implementation (simplified):**

```rust
#[derive(Clone)]
pub struct UnionStep {
    branches: Vec<Traversal<Value, Value>>,
}

impl AnyStep for UnionStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let branches = self.branches.clone();
        
        Box::new(input.flat_map(move |t| {
            // For each input traverser, execute all branches
            branches.iter().flat_map(|branch| {
                // Clone branch for this traverser
                let branch_steps = branch.clone();
                // Execute branch with traverser as input
                execute_steps(ctx, branch_steps, std::iter::once(t.clone()))
            }).collect::<Vec<_>>().into_iter()
        }))
    }
    
    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }
    
    fn name(&self) -> &'static str {
        "union"
    }
}
```

**Memory Efficiency:**

The lazy iterator model means branches only execute as results are pulled:

```rust
// Only branch1 executes until its results are consumed
// Then branch2 executes, etc.
g.v().union(vec![
    __.out_labels(&["a"]),  // Branch 1
    __.out_labels(&["b"]),  // Branch 2
    __.out_labels(&["c"]),  // Branch 3
])
.limit(10)  // May not need to execute all branches!
.to_list();
```

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

```rust
// Find people who know someone named "Bob"
// where_() accepts Traversal<Value, Value>
let results: Vec<Value> = g.v()
    .has_label("person")
    .where_(__.out_labels(&["knows"]).has_value("name", "Bob"))
    .values("name")
    .to_list();

// Find people who don't work anywhere
// not() inverts the traversal existence check
let unemployed: Vec<Value> = g.v()
    .has_label("person")
    .not(__.out_labels(&["works_at"]))
    .values("name")
    .to_list();

// Find people who know Bob AND work at Acme
// and_() requires all sub-traversals to produce results
let results: Vec<Value> = g.v()
    .has_label("person")
    .and_(vec![
        __.out_labels(&["knows"]).has_value("name", "Bob"),
        __.out_labels(&["works_at"]).has_value("name", "Acme")
    ])
    .to_list();
```

### Conditional Logic

```rust
// Different traversal based on vertex type
// choose() accepts three Traversal<Value, Value> arguments
let results: Vec<Value> = g.v()
    .choose(
        __.has_label("person"),    // condition
        __.out_labels(&["knows"]), // if person, get friends
        __.out_labels(&["contains"]) // otherwise, get children
    )
    .to_list();

// Preference-based selection with fallbacks
// coalesce() tries each branch in order, returns first with results
let names: Vec<Value> = g.v()
    .coalesce(vec![
        __.values("nickname"),       // prefer nickname
        __.values("first_name"),     // fall back to first name
        __.constant("Anonymous")     // ultimate fallback (always succeeds)
    ])
    .to_list();
```

### Multi-Path Exploration

```rust
// Get both friends and colleagues using union()
// All branches execute, results are merged
let connections: Vec<Value> = g.v()
    .has_value("name", "Alice")
    .union(vec![
        __.out_labels(&["knows"]),
        __.out_labels(&["works_with"])
    ])
    .dedup()
    .values("name")
    .to_list();

// Get multiple properties via union()
let properties: Vec<Value> = g.v()
    .has_label("person")
    .union(vec![
        __.values("name"),
        __.values("email"),
        __.out_labels(&["works_at"]).values("name")  // Company name
    ])
    .to_list();
```

### Fallback Patterns

```rust
// Prefer nickname, fall back to name
let display_names: Vec<Value> = g.v()
    .coalesce(vec![
        __.values("nickname"),
        __.values("name")
    ])
    .to_list();

// Try multiple edge types in priority order
let preferred_connections: Vec<Value> = g.v()
    .has_value("name", "Alice")
    .coalesce(vec![
        __.out_labels(&["prefers"]),   // try preferred connection
        __.out_labels(&["knows"]),     // fall back to knows
        __.out_labels(&["colleague"])  // final fallback
    ])
    .to_list();
```

### Recursive Graph Exploration

```rust
// Find all reachable vertices within 5 hops
// repeat() takes Traversal<Value, Value> for the iteration body
let reachable: Vec<Value> = g.v_ids([start_id])
    .repeat(__.out())     // Anonymous traversal as repeat body
    .times(5)             // Maximum 5 iterations
    .emit()               // Emit from each iteration
    .dedup()
    .to_list();

// Traverse until finding a company
// until() takes Traversal<Value, Value> as termination condition
let path_to_company: Option<Value> = g.v()
    .has_label("person")
    .repeat(__.out())
    .until(__.has_label("company"))  // Stop when condition matches
    .limit(1)
    .next();

// Depth-limited exploration with conditional emission
// emit_if() filters which iterations produce output
let young_friends: Vec<Value> = g.v()
    .has_value("name", "Alice")
    .repeat(__.out_labels(&["knows"]))
    .times(3)
    .emit_if(__.has_where("age", p::gt(25)))  // Only emit if age > 25
    .values("name")
    .to_list();
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

### Type Relationship

In the unified architecture, the relationship is:

```rust
// Anonymous traversal - pure step pipeline
Traversal<In, Out> {
    steps: Vec<Box<dyn AnyStep>>,
    source: None,  // No source!
}

// Bound traversal - wraps Traversal with graph context
BoundTraversal<'g, In, Out> {
    snapshot: &'g GraphSnapshot<'g>,
    interner: &'g StringInterner,
    traversal: Traversal<In, Out>,  // Steps live here
}
```

Both share the same step types (`Box<dyn AnyStep>`), the same `Traverser` type, and execute via the same `ExecutionContext`. The only difference is where graph access comes from.

### When to Use Anonymous vs Bound Traversals

**Use Anonymous Traversals (`Traversal<Value, Value>`) When:**
- Defining reusable filtering or transformation logic
- Embedding traversal logic within parent steps (`where_`, `union`, `repeat`, etc.)
- Building modular, composable query components
- Testing traversal fragments in isolation

**Use Bound Traversals (`BoundTraversal<'g, _, _>`) When:**
- Starting a query from a graph source (`g.v()`, `g.e()`)
- Performing standalone graph exploration
- Directly iterating over results (terminal steps)
- No need for embedding in parent steps

**Example showing both:**

```rust
// Define reusable anonymous traversal
fn knows_bob() -> Traversal<Value, Value> {
    __.out_labels(&["knows"]).has_value("name", "Bob")
}

// Use in bound traversal
let snap = graph.snapshot();
let g = snap.traversal();  // GraphTraversalSource

// Bound traversal with embedded anonymous
let people_who_know_bob: Vec<Value> = g.v()  // BoundTraversal
    .has_label("person")
    .where_(knows_bob())  // Splice anonymous traversal
    .to_list();           // Terminal step executes everything

// Reuse same anonymous traversal in different context
let companies_with_bob_employees: Vec<Value> = g.v()
    .has_label("company")
    .where_(__.in_labels(&["works_at"]).where_(knows_bob()))
    .to_list();
```

### Composability Benefits

Anonymous traversals enable function-like composition. Since they're just `Traversal<Value, Value>`, they can be passed around, stored, and combined:

```rust
// Define reusable fragments as functions
fn adult_filter() -> Traversal<Value, Value> {
    __.has_where("age", p::gte(18))
}

fn employed_filter() -> Traversal<Value, Value> {
    __.out_labels(&["works_at"]).has_label("company")
}

fn has_email() -> Traversal<Value, Value> {
    __.has("email")
}

// Compose via chaining (using append internally)
let contactable_adults: Vec<Value> = g.v()
    .has_label("person")
    .where_(adult_filter())
    .where_(employed_filter())
    .where_(has_email())
    .to_list();

// Or combine with and_()
let same_query: Vec<Value> = g.v()
    .has_label("person")
    .and_(vec![
        adult_filter(),
        employed_filter(),
        has_email(),
    ])
    .to_list();
```

This is more maintainable than copy-pasting traversal logic, and enables testing fragments in isolation.

### Testing Anonymous Traversals in Isolation

Anonymous traversals can be tested by embedding them in a minimal bound traversal:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn adult_filter() -> Traversal<Value, Value> {
        __.has_where("age", p::gte(18))
    }

    #[test]
    fn test_adult_filter() {
        let graph = create_test_graph();  // Helper to create test data
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        // Test the anonymous traversal via where_()
        let adults: Vec<Value> = g.v()
            .has_label("person")
            .where_(adult_filter())
            .to_list();
        
        assert_eq!(adults.len(), 5);
        
        // Verify all results are actually adults
        for v in adults {
            if let Value::Vertex(id) = v {
                let vertex = snap.get_vertex(id).unwrap();
                let age = vertex.property("age").unwrap();
                assert!(matches!(age, Value::Int(n) if *n >= 18));
            }
        }
    }
}
```

This promotes modular, testable query logic. Each anonymous traversal can be unit tested independently.

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

Anonymous traversals are a powerful abstraction for building modular, composable graph queries. In the type-erased architecture, they use the **same `Traversal<In, Out>` type** as bound traversals—the only difference is the absence of a source.

**Key architectural points:**

1. **Unified type**: `Traversal<In, Out>` works for both bound and anonymous traversals
2. **ExecutionContext at runtime**: Anonymous traversals receive graph access when spliced, not at construction
3. **Steps are type-erased**: `Vec<Box<dyn AnyStep>>` enables storing heterogeneous steps
4. **Clone-friendly**: `clone_box()` on `AnyStep` enables cloning for branching operations
5. **Same execution model**: All steps receive `ExecutionContext` uniformly

**Benefits:**

- **Reusability**: Define once, use in multiple contexts
- **Composability**: Combine simple fragments into complex queries
- **Testability**: Isolate and test traversal logic independently
- **Optimization**: Enable query planners to reason about and optimize nested traversals
- **Simplicity**: One traversal type to understand, not separate bound/anonymous types

When used effectively with steps like `where_()`, `union()`, `repeat()`, and `coalesce()`, anonymous traversals unlock expressive and efficient graph exploration patterns. Understanding their execution semantics—lazy evaluation, per-traverser isolation, and path preservation—is key to writing performant graph queries.
