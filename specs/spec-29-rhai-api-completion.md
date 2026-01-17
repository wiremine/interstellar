# Spec 29: Rhai API Completion

## Overview

This specification defines the remaining work to bring the Rhai scripting bindings to full parity with the core Gremlin-style traversal API. The current implementation covers approximately 55% of the API surface.

## Current State

### Fully Covered (100%)
- **Predicates**: All 20 predicates implemented (`eq`, `neq`, `lt`, `lte`, `gt`, `gte`, `between`, `inside`, `outside`, `within`, `without`, `containing`, `starting_with`, `ending_with`, `not_containing`, `not_starting_with`, `not_ending_with`, `regex`, `pred_not`, `pred_and`, `pred_or`)

### Partially Covered
- **Navigation** (80%): Missing `both_v()`, incomplete label variants
- **Source** (69%): Missing `add_v()`, `add_e()`
- **Terminal** (62%): Missing `to_set()`, `iterate()`, `take()`
- **Transform** (50%): Missing property accessors, builders
- **Filter** (48%): Missing traversal-based filters, sampling
- **Branch** (41%): Missing multi-way branching
- **Repeat** (30%): Missing emit/until modifiers

### Not Covered (0%)
- **Side Effect Steps**: `store()`, `aggregate()`, `cap()`, `side_effect()`
- **Mutation Steps**: `add_v()`, `add_e()`, `property()`, `drop()`

---

## Phase 1: Anonymous Traversal Parity

**Priority**: High  
**Estimated Effort**: Small  
**Files**: `src/rhai/traversal.rs`

The `RhaiAnonymousTraversal` is missing many steps that exist on `RhaiTraversal`. These should be added for consistency.

### 1.1 Missing Filter Steps

Add to `register_anonymous_factory()`:

```rust
// Skip and range
engine.register_fn("skip", |a: &mut RhaiAnonymousTraversal, n: i64| {
    a.clone().skip(n)
});
engine.register_fn("range", |a: &mut RhaiAnonymousTraversal, start: i64, end: i64| {
    a.clone().range(start, end)
});

// Predicate filters
engine.register_fn("is_", |a: &mut RhaiAnonymousTraversal, pred: RhaiPredicate| {
    a.clone().is_(pred)
});
engine.register_fn("is_eq", |a: &mut RhaiAnonymousTraversal, value: Dynamic| {
    a.clone().is_eq(dynamic_to_value(value))
});

// Path filters
engine.register_fn("simple_path", |a: &mut RhaiAnonymousTraversal| {
    a.clone().simple_path()
});
engine.register_fn("cyclic_path", |a: &mut RhaiAnonymousTraversal| {
    a.clone().cyclic_path()
});

// Has variants
engine.register_fn("has_not", |a: &mut RhaiAnonymousTraversal, key: ImmutableString| {
    a.clone().has_not(key.to_string())
});
engine.register_fn("has_where", |a: &mut RhaiAnonymousTraversal, key: ImmutableString, pred: RhaiPredicate| {
    a.clone().has_where(key.to_string(), pred)
});
engine.register_fn("has_id", |a: &mut RhaiAnonymousTraversal, id: i64| {
    a.clone().has_id(VertexId(id as u64))
});
engine.register_fn("has_label_any", |a: &mut RhaiAnonymousTraversal, labels: rhai::Array| {
    let labels: Vec<String> = labels.into_iter().filter_map(|d| d.into_string().ok()).collect();
    a.clone().has_label_any(labels)
});
```

### 1.2 Missing Transform Steps

```rust
// Element map
engine.register_fn("element_map", |a: &mut RhaiAnonymousTraversal| {
    a.clone().element_map()
});

// Multi-value access
engine.register_fn("values_multi", |a: &mut RhaiAnonymousTraversal, keys: rhai::Array| {
    let keys: Vec<String> = keys.into_iter().filter_map(|d| d.into_string().ok()).collect();
    a.clone().values_multi(keys)
});

// Aggregates
engine.register_fn("sum", |a: &mut RhaiAnonymousTraversal| a.clone().sum());
engine.register_fn("mean", |a: &mut RhaiAnonymousTraversal| a.clone().mean());
engine.register_fn("min", |a: &mut RhaiAnonymousTraversal| a.clone().min());
engine.register_fn("max", |a: &mut RhaiAnonymousTraversal| a.clone().max());
engine.register_fn("count", |a: &mut RhaiAnonymousTraversal| a.clone().count());
```

### 1.3 Missing Navigation Steps

```rust
// Label variants for both
engine.register_fn("both", |a: &mut RhaiAnonymousTraversal, label: ImmutableString| {
    a.clone().both_labels(vec![label.to_string()])
});

// Edge navigation with labels
engine.register_fn("out_e", |a: &mut RhaiAnonymousTraversal, label: ImmutableString| {
    a.clone().out_e_labels(vec![label.to_string()])
});
engine.register_fn("in_e", |a: &mut RhaiAnonymousTraversal, label: ImmutableString| {
    a.clone().in_e_labels(vec![label.to_string()])
});
engine.register_fn("both_e", |a: &mut RhaiAnonymousTraversal, label: ImmutableString| {
    a.clone().both_e_labels(vec![label.to_string()])
});
```

### 1.4 Missing Modulator Steps

```rust
// Select
engine.register_fn("select", |a: &mut RhaiAnonymousTraversal, labels: rhai::Array| {
    let labels: Vec<String> = labels.into_iter().filter_map(|d| d.into_string().ok()).collect();
    a.clone().select(labels)
});
engine.register_fn("select_one", |a: &mut RhaiAnonymousTraversal, label: ImmutableString| {
    a.clone().select_one(label.to_string())
});

// Order
engine.register_fn("order_asc", |a: &mut RhaiAnonymousTraversal| a.clone().order_asc());
engine.register_fn("order_desc", |a: &mut RhaiAnonymousTraversal| a.clone().order_desc());
```

### Implementation Notes

Before adding these registrations, verify that `RhaiAnonymousTraversal` has the corresponding methods. If not, they must be added to `src/rhai/anonymous.rs` first.

---

## Phase 2: Traversal-Based Filtering

**Priority**: High  
**Estimated Effort**: Medium  
**Files**: `src/rhai/traversal.rs`, `src/rhai/anonymous.rs`

These steps use anonymous traversals as filter conditions, enabling complex logic like "find people who have friends over 30".

### 2.1 Where Step

Filter traversers based on whether a sub-traversal produces results.

```rust
// In register_traversal_methods()
engine.register_fn("where_", |t: &mut RhaiTraversal, cond: RhaiAnonymousTraversal| {
    t.clone().where_(cond)
});

// In register_anonymous_factory()
engine.register_fn("where_", |a: &mut RhaiAnonymousTraversal, cond: RhaiAnonymousTraversal| {
    a.clone().where_(cond)
});
```

**Rhai Usage**:
```javascript
// Find people who have at least one friend
g.v().has_label("person").where_(A.out("knows")).values("name").to_list()
```

### 2.2 Not Step

Inverse of `where_` - filter traversers where sub-traversal produces NO results.

```rust
engine.register_fn("not_", |t: &mut RhaiTraversal, cond: RhaiAnonymousTraversal| {
    t.clone().not(cond)
});

engine.register_fn("not_", |a: &mut RhaiAnonymousTraversal, cond: RhaiAnonymousTraversal| {
    a.clone().not(cond)
});
```

**Rhai Usage**:
```javascript
// Find people with no outgoing "knows" edges
g.v().has_label("person").not_(A.out("knows")).values("name").to_list()
```

### 2.3 And Step

All sub-traversals must produce results.

```rust
engine.register_fn("and_", |t: &mut RhaiTraversal, conditions: rhai::Array| {
    let conds: Vec<RhaiAnonymousTraversal> = conditions
        .into_iter()
        .filter_map(|d| d.try_cast::<RhaiAnonymousTraversal>())
        .collect();
    t.clone().and_(conds)
});

engine.register_fn("and_", |a: &mut RhaiAnonymousTraversal, conditions: rhai::Array| {
    let conds: Vec<RhaiAnonymousTraversal> = conditions
        .into_iter()
        .filter_map(|d| d.try_cast::<RhaiAnonymousTraversal>())
        .collect();
    a.clone().and_(conds)
});
```

**Rhai Usage**:
```javascript
// Find people who know someone AND work at a company
g.v().has_label("person")
    .and_([A.out("knows"), A.out("works_at")])
    .values("name").to_list()
```

### 2.4 Or Step

At least one sub-traversal must produce results.

```rust
engine.register_fn("or_", |t: &mut RhaiTraversal, conditions: rhai::Array| {
    let conds: Vec<RhaiAnonymousTraversal> = conditions
        .into_iter()
        .filter_map(|d| d.try_cast::<RhaiAnonymousTraversal>())
        .collect();
    t.clone().or_(conds)
});

engine.register_fn("or_", |a: &mut RhaiAnonymousTraversal, conditions: rhai::Array| {
    let conds: Vec<RhaiAnonymousTraversal> = conditions
        .into_iter()
        .filter_map(|d| d.try_cast::<RhaiAnonymousTraversal>())
        .collect();
    a.clone().or_(conds)
});
```

**Rhai Usage**:
```javascript
// Find people who know someone OR work somewhere
g.v().has_label("person")
    .or_([A.out("knows"), A.out("works_at")])
    .values("name").to_list()
```

### 2.5 Required Core Methods

Ensure these methods exist on `RhaiTraversal` and `RhaiAnonymousTraversal`:

```rust
// In RhaiTraversal impl
pub fn where_(self, cond: RhaiAnonymousTraversal) -> Self { ... }
pub fn not(self, cond: RhaiAnonymousTraversal) -> Self { ... }
pub fn and_(self, conds: Vec<RhaiAnonymousTraversal>) -> Self { ... }
pub fn or_(self, conds: Vec<RhaiAnonymousTraversal>) -> Self { ... }

// In RhaiAnonymousTraversal impl
pub fn where_(&self, cond: RhaiAnonymousTraversal) -> RhaiAnonymousTraversal { ... }
pub fn not(&self, cond: RhaiAnonymousTraversal) -> RhaiAnonymousTraversal { ... }
pub fn and_(&self, conds: Vec<RhaiAnonymousTraversal>) -> RhaiAnonymousTraversal { ... }
pub fn or_(&self, conds: Vec<RhaiAnonymousTraversal>) -> RhaiAnonymousTraversal { ... }
```

---

## Phase 3: Navigation Completion

**Priority**: High  
**Estimated Effort**: Small  
**Files**: `src/rhai/traversal.rs`

### 3.1 Both Vertices Step

Navigate from an edge to both its source and target vertices.

```rust
// In register_traversal_methods()
engine.register_fn("both_v", |t: &mut RhaiTraversal| t.clone().both_v());

// In register_anonymous_factory()
engine.register_fn("both_v", |a: &mut RhaiAnonymousTraversal| a.clone().both_v());
```

**Rhai Usage**:
```javascript
// Get all vertices connected by "knows" edges
g.e().has_label("knows").both_v().dedup().values("name").to_list()
```

### 3.2 Required Core Method

Add to `RhaiTraversal` and `RhaiAnonymousTraversal`:

```rust
pub fn both_v(self) -> Self {
    self.add_step(RhaiStep::BothV)
}
```

---

## Phase 4: Repeat Step Completion

**Priority**: High  
**Estimated Effort**: Medium  
**Files**: `src/rhai/traversal.rs`, `src/rhai/anonymous.rs`

The current `repeat` implementation only supports `times(n)`. This phase adds `until()` and `emit()` modifiers.

### 4.1 Repeat Until

Repeat until a condition is met.

```rust
// Current implementation uses RepeatUntil enum - extend it
pub enum RepeatUntil {
    Times(i64),
    Until(RhaiAnonymousTraversal),
    UntilWithEmit(RhaiAnonymousTraversal),
    TimesWithEmit(i64),
}

// Registration
engine.register_fn("repeat_until", |t: &mut RhaiTraversal, body: RhaiAnonymousTraversal, until: RhaiAnonymousTraversal| {
    t.clone().repeat_until(body, until)
});
```

**Rhai Usage**:
```javascript
// Traverse until we find a "manager" vertex
g.v().has_value("name", "Alice")
    .repeat_until(A.out("reports_to"), A.has_label("manager"))
    .values("name").to_list()
```

### 4.2 Repeat Emit

Emit all intermediate results during iteration.

```rust
engine.register_fn("repeat_emit", |t: &mut RhaiTraversal, body: RhaiAnonymousTraversal, n: i64| {
    t.clone().repeat_emit(body, n)
});

engine.register_fn("repeat_emit_until", |t: &mut RhaiTraversal, body: RhaiAnonymousTraversal, until: RhaiAnonymousTraversal| {
    t.clone().repeat_emit_until(body, until)
});
```

**Rhai Usage**:
```javascript
// Get all people in the "knows" chain, including intermediates
g.v().has_value("name", "Alice")
    .repeat_emit(A.out("knows"), 3)
    .dedup().values("name").to_list()
```

### 4.3 Loops Step

Access the current loop depth within a repeat.

```rust
engine.register_fn("loops", |t: &mut RhaiTraversal| t.clone().loops());
engine.register_fn("loops", |a: &mut RhaiAnonymousTraversal| a.clone().loops());
```

**Rhai Usage**:
```javascript
// Get loop depth at each step
g.v().has_value("name", "Alice")
    .repeat_emit(A.out("knows"), 3)
    .project(["name", "depth"])
    .by(A.values("name"))
    .by(A.loops())
    .to_list()
```

---

## Phase 5: Side Effect Steps

**Priority**: Medium  
**Estimated Effort**: Large  
**Files**: `src/rhai/traversal.rs`, `src/rhai/anonymous.rs`

Side effect steps accumulate data during traversal for later retrieval. This requires maintaining a side-effect registry.

### 5.1 Side Effect Registry

Add a registry to track named side effects:

```rust
// In RhaiTraversal
pub struct RhaiTraversal {
    // ... existing fields
    side_effects: Arc<RwLock<HashMap<String, Vec<Value>>>>,
}
```

### 5.2 Store Step

Lazily store traversers into a named side effect (per-traverser, not barrier).

```rust
engine.register_fn("store", |t: &mut RhaiTraversal, key: ImmutableString| {
    t.clone().store(key.to_string())
});
```

**Rhai Usage**:
```javascript
g.v().has_label("person").store("people").out("knows").store("friends").cap("people")
```

### 5.3 Aggregate Step

Barrier step that collects all traversers before continuing.

```rust
engine.register_fn("aggregate", |t: &mut RhaiTraversal, key: ImmutableString| {
    t.clone().aggregate(key.to_string())
});
```

**Rhai Usage**:
```javascript
// Collect all people first, then get their friends
g.v().has_label("person").aggregate("all_people").out("knows").cap("all_people")
```

### 5.4 Cap Step

Retrieve accumulated side effect values.

```rust
engine.register_fn("cap", |t: &mut RhaiTraversal, key: ImmutableString| {
    t.clone().cap(key.to_string())
});

engine.register_fn("cap_multi", |t: &mut RhaiTraversal, keys: rhai::Array| {
    let keys: Vec<String> = keys.into_iter().filter_map(|d| d.into_string().ok()).collect();
    t.clone().cap_multi(keys)
});
```

### 5.5 SideEffect Step

Execute a sub-traversal for side effects only, without modifying the main traversal.

```rust
engine.register_fn("side_effect", |t: &mut RhaiTraversal, traversal: RhaiAnonymousTraversal| {
    t.clone().side_effect(traversal)
});
```

**Rhai Usage**:
```javascript
// Count friends while traversing
g.v().has_value("name", "Alice")
    .side_effect(A.out("knows").store("friends"))
    .out("works_at").values("name").to_list()
```

### Implementation Considerations

- Side effects must be thread-safe (`Arc<RwLock<...>>`)
- `store` is lazy (per-element), `aggregate` is a barrier
- `cap` terminates the current traversal and starts new one with side effect contents
- Consider using Rhai's built-in `Map` type for `cap_multi` results

---

## Phase 6: Mutation Steps

**Priority**: Medium  
**Estimated Effort**: Large  
**Files**: `src/rhai/traversal.rs`, `src/rhai/engine.rs`

Mutation steps modify the graph. These require a mutable graph reference.

### 6.1 Mutable Graph Access

Extend `RhaiEngine` to support mutable graph operations:

```rust
impl RhaiEngine {
    /// Evaluate a script with mutable graph access
    pub fn eval_with_graph_mut<T: Clone + 'static>(
        &self,
        graph: &mut Graph,
        script: &str,
    ) -> Result<T, RhaiError> {
        // Create mutable graph wrapper
        let mut scope = Scope::new();
        scope.push("graph", RhaiGraphMut::new(graph));
        // ...
    }
}

#[derive(Clone)]
pub struct RhaiGraphMut {
    inner: Arc<RwLock<&mut Graph>>,
}
```

### 6.2 Add Vertex Step

Create a new vertex.

```rust
engine.register_fn("add_v", |g: &mut RhaiGraphMut, label: ImmutableString| {
    g.add_vertex(label.to_string())
});

// On traversal - adds vertex and traverses to it
engine.register_fn("add_v", |t: &mut RhaiTraversal, label: ImmutableString| {
    t.clone().add_v(label.to_string())
});
```

**Rhai Usage**:
```javascript
// Create a new person vertex
let g = graph.traversal();
g.add_v("person").property("name", "Eve").property("age", 28).id()
```

### 6.3 Add Edge Step

Create a new edge between vertices.

```rust
engine.register_fn("add_e", |t: &mut RhaiTraversal, label: ImmutableString| {
    t.clone().add_e(label.to_string())
});

engine.register_fn("from_v", |t: &mut RhaiTraversal, label: ImmutableString| {
    t.clone().from_v(label.to_string())
});

engine.register_fn("to_v", |t: &mut RhaiTraversal, label: ImmutableString| {
    t.clone().to_v(label.to_string())
});
```

**Rhai Usage**:
```javascript
// Create edge from Alice to a new person
g.v().has_value("name", "Alice").as_("a")
    .add_v("person").property("name", "Frank").as_("b")
    .add_e("knows").from_v("a").to_v("b")
```

### 6.4 Property Step

Set a property on the current element.

```rust
engine.register_fn("property", |t: &mut RhaiTraversal, key: ImmutableString, value: Dynamic| {
    t.clone().property(key.to_string(), dynamic_to_value(value))
});
```

### 6.5 Drop Step

Delete the current element from the graph.

```rust
engine.register_fn("drop", |t: &mut RhaiTraversal| t.clone().drop());
```

**Rhai Usage**:
```javascript
// Delete all "temp" vertices
g.v().has_label("temp").drop().iterate()
```

### Implementation Considerations

- Mutations should be atomic or support transactions
- Consider whether to use deferred execution (collect mutations, apply at end)
- Drop must handle edge cleanup when deleting vertices
- Property updates on edges vs vertices may differ

---

## Phase 7: Advanced Filter Steps

**Priority**: Medium  
**Estimated Effort**: Medium  
**Files**: `src/rhai/traversal.rs`

### 7.1 Tail Steps

Get the last element(s) of the traversal.

```rust
engine.register_fn("tail", |t: &mut RhaiTraversal| t.clone().tail());
engine.register_fn("tail_n", |t: &mut RhaiTraversal, n: i64| t.clone().tail_n(n));

engine.register_fn("tail", |a: &mut RhaiAnonymousTraversal| a.clone().tail());
engine.register_fn("tail_n", |a: &mut RhaiAnonymousTraversal, n: i64| a.clone().tail_n(n));
```

**Rhai Usage**:
```javascript
// Get the last 3 people by age
g.v().has_label("person").order_asc().by("age").tail_n(3).values("name").to_list()
```

### 7.2 Sampling Steps

Random element selection.

```rust
engine.register_fn("coin", |t: &mut RhaiTraversal, probability: f64| {
    t.clone().coin(probability)
});

engine.register_fn("sample", |t: &mut RhaiTraversal, n: i64| {
    t.clone().sample(n)
});
```

**Rhai Usage**:
```javascript
// Random 50% of people
g.v().has_label("person").coin(0.5).values("name").to_list()

// Random sample of 3 people
g.v().has_label("person").sample(3).values("name").to_list()
```

### 7.3 Dedup Variants

Deduplication with custom keys.

```rust
engine.register_fn("dedup_by_key", |t: &mut RhaiTraversal, key: ImmutableString| {
    t.clone().dedup_by_key(key.to_string())
});

engine.register_fn("dedup_by_label", |t: &mut RhaiTraversal| {
    t.clone().dedup_by_label()
});

engine.register_fn("dedup_by", |t: &mut RhaiTraversal, traversal: RhaiAnonymousTraversal| {
    t.clone().dedup_by(traversal)
});
```

**Rhai Usage**:
```javascript
// One person per city
g.v().has_label("person").dedup_by_key("city").values("name").to_list()

// One vertex per label
g.v().dedup_by_label().label().to_list()
```

### 7.4 Has ID Variants

```rust
engine.register_fn("has_ids", |t: &mut RhaiTraversal, ids: rhai::Array| {
    let ids: Vec<VertexId> = ids
        .into_iter()
        .filter_map(|d| d.as_int().ok().map(|i| VertexId(i as u64)))
        .collect();
    t.clone().has_ids(ids)
});
```

---

## Phase 8: Advanced Transform Steps

**Priority**: Medium  
**Estimated Effort**: Large  
**Files**: `src/rhai/traversal.rs`

### 8.1 Property Object Steps

Access property objects (key-value pairs).

```rust
engine.register_fn("properties", |t: &mut RhaiTraversal| t.clone().properties());
engine.register_fn("properties_keys", |t: &mut RhaiTraversal, keys: rhai::Array| {
    let keys: Vec<String> = keys.into_iter().filter_map(|d| d.into_string().ok()).collect();
    t.clone().properties_keys(keys)
});

engine.register_fn("key", |t: &mut RhaiTraversal| t.clone().key());
engine.register_fn("value", |t: &mut RhaiTraversal| t.clone().value());
```

**Rhai Usage**:
```javascript
// Get all property keys and values
g.v().has_value("name", "Alice").properties().key().to_list()  // ["name", "age", "city"]
g.v().has_value("name", "Alice").properties().value().to_list() // ["Alice", 30, "New York"]
```

### 8.2 Value Map Variants

```rust
engine.register_fn("value_map_keys", |t: &mut RhaiTraversal, keys: rhai::Array| {
    let keys: Vec<String> = keys.into_iter().filter_map(|d| d.into_string().ok()).collect();
    t.clone().value_map_keys(keys)
});

engine.register_fn("value_map_with_tokens", |t: &mut RhaiTraversal| {
    t.clone().value_map_with_tokens()
});
```

### 8.3 Index Step

Add position index to each element.

```rust
engine.register_fn("index", |t: &mut RhaiTraversal| t.clone().index());
```

**Rhai Usage**:
```javascript
// Get people with their position
g.v().has_label("person").values("name").index().to_list()
// Returns: [[0, "Alice"], [1, "Bob"], ...]
```

### 8.4 Local Step

Execute sub-traversal in isolated scope.

```rust
engine.register_fn("local", |t: &mut RhaiTraversal, traversal: RhaiAnonymousTraversal| {
    t.clone().local(traversal)
});
```

**Rhai Usage**:
```javascript
// Get the oldest friend of each person (local scope)
g.v().has_label("person")
    .local(A.out("knows").order_desc().by("age").limit(1))
    .values("name").to_list()
```

---

## Phase 9: Branching Completion

**Priority**: Medium  
**Estimated Effort**: Medium  
**Files**: `src/rhai/traversal.rs`

### 9.1 Choose Step (Binary)

Simple if-then-else branching.

```rust
engine.register_fn(
    "choose_binary",
    |t: &mut RhaiTraversal, cond: RhaiAnonymousTraversal, true_branch: RhaiAnonymousTraversal, false_branch: RhaiAnonymousTraversal| {
        t.clone().choose(cond, true_branch, false_branch)
    }
);
```

**Rhai Usage**:
```javascript
// Different processing based on age
g.v().has_label("person")
    .choose_binary(
        A.has_where("age", gt(30)),
        A.constant("senior"),
        A.constant("junior")
    ).to_list()
```

### 9.2 Choose Step (Multi-way)

Pattern-matching style branching with options.

```rust
// This requires a builder pattern in Rhai
// Option 1: Use a map for options
engine.register_fn(
    "choose_options",
    |t: &mut RhaiTraversal, key_traversal: RhaiAnonymousTraversal, options: rhai::Map| {
        t.clone().choose_options(key_traversal, options)
    }
);
```

**Rhai Usage**:
```javascript
// Route based on label
g.v().choose_options(A.label(), #{
    "person": A.values("name"),
    "company": A.values("industry"),
    "_default": A.constant("unknown")
}).to_list()
```

### 9.3 Branch Step

Similar to choose but all matching branches execute.

```rust
engine.register_fn(
    "branch_options",
    |t: &mut RhaiTraversal, key_traversal: RhaiAnonymousTraversal, options: rhai::Map| {
        t.clone().branch_options(key_traversal, options)
    }
);
```

---

## Phase 10: Terminal Steps Completion

**Priority**: Low  
**Estimated Effort**: Small  
**Files**: `src/rhai/traversal.rs`

### 10.1 To Set

Collect unique results.

```rust
engine.register_fn("to_set", |t: &mut RhaiTraversal| -> rhai::Array {
    t.clone().to_set().into_iter().map(value_to_dynamic).collect()
});
```

### 10.2 Iterate

Consume traversal without collecting (useful for side effects).

```rust
engine.register_fn("iterate", |t: &mut RhaiTraversal| {
    t.clone().iterate();
});
```

### 10.3 Take

Get first N results.

```rust
engine.register_fn("take", |t: &mut RhaiTraversal, n: i64| -> rhai::Array {
    t.clone().take(n as usize).into_iter().map(value_to_dynamic).collect()
});
```

### 10.4 Explain

Get traversal execution plan (for debugging).

```rust
engine.register_fn("explain", |t: &mut RhaiTraversal| -> String {
    t.clone().explain()
});
```

**Rhai Usage**:
```javascript
// See execution plan
g.v().has_label("person").out("knows").values("name").explain()
```

---

## Phase 11: Builder Pattern Steps

**Priority**: Low  
**Estimated Effort**: Large  
**Files**: `src/rhai/traversal.rs`, `src/rhai/builders.rs` (new)

These steps use a builder pattern with `.by()` modifiers. They require special handling in Rhai.

### 11.1 Order By

```rust
// Option 1: Named methods for common cases
engine.register_fn("order_by", |t: &mut RhaiTraversal, key: ImmutableString| {
    t.clone().order_by(key.to_string(), Order::Asc)
});
engine.register_fn("order_by_desc", |t: &mut RhaiTraversal, key: ImmutableString| {
    t.clone().order_by(key.to_string(), Order::Desc)
});

// Option 2: Fluent builder (requires RhaiOrderBuilder type)
engine.register_fn("order", |t: &mut RhaiTraversal| {
    RhaiOrderBuilder::new(t.clone())
});
engine.register_fn("by", |b: &mut RhaiOrderBuilder, key: ImmutableString| {
    b.clone().by(key.to_string())
});
engine.register_fn("by_desc", |b: &mut RhaiOrderBuilder, key: ImmutableString| {
    b.clone().by_desc(key.to_string())
});
```

**Rhai Usage**:
```javascript
// Option 1
g.v().has_label("person").order_by("age").values("name").to_list()

// Option 2
g.v().has_label("person").order().by("age").by_desc("name").values("name").to_list()
```

### 11.2 Project

```rust
engine.register_fn("project", |t: &mut RhaiTraversal, keys: rhai::Array| {
    let keys: Vec<String> = keys.into_iter().filter_map(|d| d.into_string().ok()).collect();
    RhaiProjectBuilder::new(t.clone(), keys)
});

engine.register_fn("by", |b: &mut RhaiProjectBuilder, traversal: RhaiAnonymousTraversal| {
    b.clone().by(traversal)
});
```

**Rhai Usage**:
```javascript
g.v().has_label("person")
    .project(["name", "friend_count"])
    .by(A.values("name"))
    .by(A.out("knows").count())
    .to_list()
```

### 11.3 Group / GroupCount

```rust
engine.register_fn("group", |t: &mut RhaiTraversal| {
    RhaiGroupBuilder::new(t.clone())
});
engine.register_fn("group_count", |t: &mut RhaiTraversal| {
    RhaiGroupCountBuilder::new(t.clone())
});

// Builder methods
engine.register_fn("by_key", |b: &mut RhaiGroupBuilder, traversal: RhaiAnonymousTraversal| {
    b.clone().by_key(traversal)
});
engine.register_fn("by_value", |b: &mut RhaiGroupBuilder, traversal: RhaiAnonymousTraversal| {
    b.clone().by_value(traversal)
});
```

**Rhai Usage**:
```javascript
// Group people by city
g.v().has_label("person")
    .group()
    .by_key(A.values("city"))
    .by_value(A.values("name"))
    .to_list()

// Count by city
g.v().has_label("person")
    .group_count()
    .by_key(A.values("city"))
    .to_list()
```

### 11.4 Math

```rust
engine.register_fn("math", |t: &mut RhaiTraversal, expr: ImmutableString| {
    RhaiMathBuilder::new(t.clone(), expr.to_string())
});

engine.register_fn("by", |b: &mut RhaiMathBuilder, name: ImmutableString, traversal: RhaiAnonymousTraversal| {
    b.clone().by(name.to_string(), traversal)
});
```

**Rhai Usage**:
```javascript
// Calculate BMI
g.v().has_label("person")
    .math("weight / (height * height)")
    .by("weight", A.values("weight"))
    .by("height", A.values("height"))
    .to_list()
```

---

## Implementation Priority Summary

| Phase | Priority | Effort | Coverage Impact |
|-------|----------|--------|-----------------|
| 1. Anonymous Traversal Parity | High | Small | +5% |
| 2. Traversal-Based Filtering | High | Medium | +8% |
| 3. Navigation Completion | High | Small | +2% |
| 4. Repeat Completion | High | Medium | +4% |
| 5. Side Effect Steps | Medium | Large | +5% |
| 6. Mutation Steps | Medium | Large | +5% |
| 7. Advanced Filter Steps | Medium | Medium | +4% |
| 8. Advanced Transform Steps | Medium | Large | +6% |
| 9. Branching Completion | Medium | Medium | +3% |
| 10. Terminal Completion | Low | Small | +2% |
| 11. Builder Pattern Steps | Low | Large | +6% |

**Total potential coverage: ~100%**

---

## Testing Strategy

Each phase should include:

1. **Unit tests** in `src/rhai/*.rs` for each new method
2. **Integration tests** in `tests/rhai_integration/` for end-to-end scenarios
3. **Example updates** in `examples/rhai_scripting.rs` for documentation

### Test Categories Per Phase

- **Phase 1-4**: Add to existing test files
- **Phase 5**: New `tests/rhai_integration/side_effects.rs`
- **Phase 6**: New `tests/rhai_integration/mutations.rs`
- **Phase 7-8**: Add to `tests/rhai_integration/traversal.rs`
- **Phase 9**: New `tests/rhai_integration/branching.rs`
- **Phase 10-11**: Add to `tests/rhai_integration/traversal.rs`

---

## Acceptance Criteria

1. All 141 API steps covered in Rhai bindings
2. 100% of new code covered by tests
3. Documentation for each new step
4. Example script demonstrating each phase
5. No performance regression in existing functionality
