# Side-Effect Mutation Pattern Spec

## Overview

This spec addresses two related issues:

1. **Mutation chaining**: Currently `.property()` transforms `Value::Vertex(id)` into a `Value::Map` with pending mutation markers, breaking subsequent traversal steps like `.out()`.

2. **Vertex representation**: Currently `Value::Vertex(VertexId)` contains only the ID, not a richer vertex object like TinkerPop's `Vertex` interface.

The goal is to enable TinkerPop-style traversal chaining:

```rust
g.V().has("name", "Alice")
    .property("age", 31)
    .out("knows")
    .to_list()
```

## Current Architecture

### Value::Vertex

```rust
pub enum Value {
    Vertex(VertexId),  // Just the ID
    Edge(EdgeId),      // Just the ID
    // ...
}
```

### PropertyStep Current Behavior

When `.property(key, value)` is called on a `Value::Vertex(id)`:

```rust
// PropertyStep::apply() transforms the value:
Value::Vertex(id) => {
    t.value = Value::Map(HashMap::from([
        ("__pending_property_vertex".to_string(), Value::Bool(true)),
        ("id".to_string(), Value::Vertex(*id)),
        ("key".to_string(), Value::String(key)),
        ("value".to_string(), value),
    ]));
}
```

**Problem**: After `.property()`, the traverser no longer carries `Value::Vertex` - subsequent steps like `.out()` fail because they expect `Value::Vertex`.

### Mutation Execution

Mutations are executed **after** traversal completion in `to_list()`:

```rust
// In CowBoundTraversal::to_list()
for traverser in results {
    if let Some(mutation) = PendingMutation::from_value(&traverser.value) {
        Self::execute_mutation(&mut wrapper, mutation);
    }
}
```

This deferred execution is correct, but the value transformation during traversal breaks chaining.

---

## Proposed Solution

### Part 1: Side-Effect Pattern for Mutations

Mutation steps (`.property()`, `.drop()`) should be **side-effect steps** that:
1. Queue mutations in a side channel
2. Pass through the original traverser unchanged

#### 1.1 Add Mutation Queue to Traverser

Extend `Traverser` to carry pending mutations:

```rust
// src/traversal/traverser.rs

pub struct Traverser {
    pub value: Value,
    pub path: Path,
    pub loops: usize,
    pub sack: Option<Box<dyn CloneSack>>,
    pub bulk: u64,
    // NEW: Queue of pending mutations for this traverser
    pub pending_mutations: Vec<PendingMutation>,
}
```

The `PendingMutation` enum already exists in `src/traversal/mutation.rs`.

#### 1.2 Modify PropertyStep to Use Side-Effect Pattern

```rust
// src/traversal/mutation.rs

impl AnyStep for PropertyStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let key = self.key.clone();
        let value = self.value.clone();

        Box::new(input.map(move |mut t| {
            match &t.value {
                Value::Vertex(id) => {
                    // Queue mutation as side-effect
                    t.pending_mutations.push(PendingMutation::SetVertexProperty {
                        id: *id,
                        key: key.clone(),
                        value: value.clone(),
                    });
                    // PRESERVE original value - this is the key change
                }
                Value::Edge(id) => {
                    t.pending_mutations.push(PendingMutation::SetEdgeProperty {
                        id: *id,
                        key: key.clone(),
                        value: value.clone(),
                    });
                }
                Value::Map(map) if map.contains_key("__pending_add_v") => {
                    // For add_v(), add property to pending vertex (existing behavior)
                    // ... keep current logic for pending additions
                }
                _ => {
                    // Non-element values pass through unchanged
                }
            }
            t
        }))
    }
}
```

#### 1.3 Modify DropStep Similarly

```rust
impl AnyStep for DropStep {
    fn apply<'a>(...) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.filter_map(move |mut t| {
            match &t.value {
                Value::Vertex(id) => {
                    t.pending_mutations.push(PendingMutation::DropVertex { id: *id });
                    None  // Drop consumes the traverser
                }
                Value::Edge(id) => {
                    t.pending_mutations.push(PendingMutation::DropEdge { id: *id });
                    None
                }
                _ => Some(t),
            }
        }))
    }
}
```

#### 1.4 Modify Terminal Steps to Execute Pending Mutations

In `CowBoundTraversal::to_list()` and similar:

```rust
fn to_list(self) -> Vec<Value> {
    let results = self.execute();
    let mut wrapper = GraphMutWrapper { graph: self.graph };
    let mut final_results = Vec::new();

    for traverser in results {
        // Execute any pending mutations from this traverser
        for mutation in &traverser.pending_mutations {
            Self::execute_mutation(&mut wrapper, mutation.clone());
        }

        // Also check for legacy Value::Map-based mutations (for add_v/add_e)
        if let Some(mutation) = PendingMutation::from_value(&traverser.value) {
            if let Some(result) = Self::execute_mutation(&mut wrapper, mutation) {
                final_results.push(result);
            }
        } else {
            final_results.push(traverser.value);
        }
    }

    final_results
}
```

---

### Part 2: Richer Vertex/Edge Representation (Optional Enhancement)

This is orthogonal to Part 1 but addresses the original question about vertex representation.

#### Current State

```rust
// Value::Vertex contains only the ID
Value::Vertex(VertexId(42))

// To get label/properties, must call storage:
let vertex: storage::Vertex = storage.get_vertex(id)?;
```

#### Option A: Keep Current Design

Keep `Value::Vertex(VertexId)` as a lightweight reference. Users who need full vertex data use:

```rust
g.v().element_map().first()  // Returns Map with id, label, properties
```

**Recommendation**: This is the simpler path and matches the "traversal API" philosophy. The ID-based reference is efficient and allows lazy loading.

#### Option B: Embed Full Vertex Data

Change to:

```rust
pub enum Value {
    Vertex(Arc<VertexData>),  // Contains id, label, properties
    Edge(Arc<EdgeData>),
    // ...
}

pub struct VertexData {
    pub id: VertexId,
    pub label: String,
    pub properties: HashMap<String, Value>,
}
```

**Trade-offs**:
- Pro: `v.label`, `v.property("name")` work directly on the Value
- Con: Larger memory footprint, stale data if graph mutates, breaking change

#### Recommendation

**Start with Part 1 only**. The side-effect mutation pattern is the higher priority fix and unblocks the chaining use case. Part 2 can be revisited later if the ID-based approach proves too limiting.

---

## Implementation Plan

### Phase 1: Side-Effect Mutation Pattern

1. **Add `pending_mutations` field to `Traverser`**
   - File: `src/traversal/traverser.rs`
   - Add `pub pending_mutations: Vec<PendingMutation>`
   - Update `Traverser::new()`, `split()`, `with_value()` to initialize/clone mutations
   - Move `PendingMutation` to `traverser.rs` or create shared module

2. **Modify `PropertyStep` to use side-effect pattern**
   - File: `src/traversal/mutation.rs`
   - Queue mutation instead of transforming value
   - Preserve original `Value::Vertex`/`Value::Edge`

3. **Modify `DropStep` similarly**
   - File: `src/traversal/mutation.rs`

4. **Update terminal steps to execute pending mutations**
   - Files: `src/storage/cow.rs`, `src/storage/cow_mmap.rs`
   - Iterate `traverser.pending_mutations` before processing value

5. **Update Rhai integration**
   - File: `src/rhai/traversal.rs`
   - Ensure execution functions handle pending mutations

6. **Add tests**
   - Test: `.property().out()` chaining
   - Test: Multiple `.property()` calls
   - Test: `.property().property().out()` chaining
   - Test: Mutation happens at terminal step, not during traversal

### Phase 2: Backward Compatibility (if needed)

Keep `PendingMutation::from_value()` working for `add_v()`/`add_e()` which still use the `Value::Map` pattern for building up new elements with properties.

---

## Test Cases

```rust
#[test]
fn property_then_out_chaining() {
    let mut graph = Graph::new();
    let alice = graph.add_vertex("person", HashMap::from([
        ("name".to_string(), "Alice".into()),
    ]));
    let bob = graph.add_vertex("person", HashMap::from([
        ("name".to_string(), "Bob".into()),
    ]));
    graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();

    let g = Graph::from(graph).gremlin();

    // This should work after the fix
    let friends = g.v()
        .has_value("name", "Alice")
        .property("age", 31i64)
        .out_labels(&["knows"])
        .values("name")
        .to_list();

    assert_eq!(friends, vec![Value::String("Bob".to_string())]);

    // Verify the mutation was applied
    let alice_age = g.v_id(alice).values("age").next();
    assert_eq!(alice_age, Some(Value::Int(31)));
}

#[test]
fn multiple_property_mutations() {
    // Test: g.v().property("a", 1).property("b", 2).to_list()
    // Both properties should be set
}

#[test]
fn property_does_not_mutate_until_terminal() {
    // Create traversal but don't call terminal step
    // Verify no mutation occurred
    // Call terminal step
    // Verify mutation occurred
}
```

---

## Open Questions

1. **Mutation ordering**: If the same vertex appears multiple times in a traversal with different `.property()` calls, what order are they applied? (Proposal: order of traverser processing)

2. **Error handling**: What if a mutation fails (e.g., vertex was deleted)? (Proposal: skip silently, matching current behavior)

3. **Split traversers**: When a traverser splits (e.g., at `.out()`), should pending mutations be cloned or shared? (Proposal: clone, so each path independently applies its mutations)

---

## Migration Notes

This change should be **backward compatible** for most use cases:

- `add_v()` / `add_e()` continue to use `Value::Map` with pending markers (unchanged)
- `.property()` on new elements (after `add_v()`) continues to modify the pending map (unchanged)
- `.property()` on existing elements now uses side-effect pattern (changed, but more correct)
- `.drop()` behavior unchanged (already consumes traverser)

The main visible change is that `.property().out()` chains now work correctly.
