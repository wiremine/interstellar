# Spec 36: Mutation ID Extraction in Traversal Engine

This specification addresses a bug in the traversal engine where `IdStep` fails to extract IDs from mutation traversals because it executes before mutations are materialized.

---

## 1. Problem Statement

### 1.1 Current Behavior

When chaining `.id()` after mutation steps, the traversal returns no results:

```rust
// Rust API
let g = graph.gremlin();
let id = g.add_v("person").property("name", "Alice").id().next();
// id = None (expected: Some(Value::Int(...)))
```

```javascript
// Rhai API
let g = graph.gremlin();
let alice = g.add_v("person").property("name", "Alice").id().first();
// alice = () (expected: integer ID)

// This fails because alice is unit:
g.add_e("knows").from_v(alice).to_v(bob).iterate();
```

### 1.2 Expected Behavior

```rust
// Rust API
let id = g.add_v("person").property("name", "Alice").id().next();
// id = Some(Value::Int(0))  // The created vertex's ID as an integer
```

```javascript
// Rhai API
let alice = g.add_v("person").property("name", "Alice").id().first();
// alice = 0  // The created vertex's ID as an integer

g.add_e("knows").from_v(alice).to_v(bob).iterate();  // Works
```

### 1.3 Working Pattern (Current)

The Rust API works correctly if you skip `.id()` and use `.as_vertex_id()`:

```rust
// This works (see examples/quickstart_gremlin.rs)
let alice = g.add_v("Person")
    .property("name", "Alice")
    .next()
    .unwrap();  // Returns Value::Vertex(VertexId)

let alice_id = alice.as_vertex_id().unwrap();  // Extract VertexId
```

In Rhai, the equivalent pattern also works:

```javascript
let alice = g.add_v("person").property("name", "Alice").first();
// alice is a VertexId (value_to_dynamic unwraps Value::Vertex to VertexId)
g.add_e("knows").from_v(alice).to_v(bob).iterate();  // Works!
```

### 1.4 When the Bug Manifests

The bug only occurs when `.id()` is explicitly chained before the terminal step:

```rust
// This is broken
let id = g.add_v("Person").id().next();  // Returns None
```

```javascript
// This is broken  
let id = g.add_v("person").id().first();  // Returns ()
```

Users might use `.id()` expecting to get an integer ID directly, rather than a `VertexId` wrapper type.

### 1.5 Scope

This bug affects the core traversal engine (`src/storage/cow.rs`), not just the Rhai layer. The Rhai API correctly delegates to the engine, which has the underlying timing issue.

### 1.6 Impact Assessment

**Low priority.** The working pattern (without `.id()`) is already documented in examples and is the idiomatic approach. This bug only affects users who explicitly chain `.id()` expecting an integer result.

However, fixing this improves API consistency: `.id()` should work uniformly whether the traversal starts from existing elements (`g.v().id()`) or newly created ones (`g.add_v().id()`).

---

## 2. Root Cause Analysis

### 2.1 Execution Flow

The bug occurs in `CowBoundTraversal::execute_with_mutations()` (`src/storage/cow.rs:1779-1878`):

```rust
fn execute_with_mutations(self) -> Vec<Value> {
    // ... setup ...

    // Phase 1: Apply all steps in sequence
    for step in &steps {
        current = step.apply(&ctx, Box::new(current.into_iter())).collect();
    }

    // Phase 2: Execute pending mutations (AFTER all steps)
    for traverser in results {
        if let Some(mutation) = PendingMutation::from_value(&traverser.value) {
            if let Some(result) = Self::execute_mutation(&mut wrapper, mutation) {
                final_results.push(result);
            }
        } else {
            final_results.push(traverser.value);
        }
    }
}
```

### 2.2 Step-by-Step Trace

For `g.add_v("person").property("name", "Alice").id().next()`:

| Step | Input | Output |
|------|-------|--------|
| Start | `[Traverser(Null)]` | - |
| AddVStep | `[Traverser(Null)]` | `[Traverser(Map{__pending_add_v: true, label: "person", ...})]` |
| PropertyStep | `[Traverser(Map{...})]` | `[Traverser(Map{..., properties: {name: "Alice"}})]` |
| IdStep | `[Traverser(Map{...})]` | `[]` ← **Bug: Map filtered out** |
| Mutation execution | `[]` | `[]` |
| Return | - | `[]` |

### 2.3 IdStep Implementation

The current `IdStep` (`src/traversal/transform/metadata.rs:39-68`) only handles actual elements:

```rust
impl AnyStep for IdStep {
    fn apply<'a>(&'a self, _ctx: &'a ExecutionContext<'a>, input: ...) -> ... {
        Box::new(input.filter_map(|traverser| {
            match &traverser.value {
                Value::Vertex(id) => Some(traverser.split(Value::Int(id.0 as i64))),
                Value::Edge(id) => Some(traverser.split(Value::Int(id.0 as i64))),
                _ => None,  // ← Pending mutations (Map) are filtered out
            }
        }))
    }
}
```

### 2.4 Core Issue

**The mutation execution model is deferred**: Steps produce "pending mutation markers" (`Value::Map` with `__pending_add_v`), and actual mutations happen only at the terminal step. But `IdStep` runs during the step-application phase when only markers exist, not actual vertices.

---

## 3. Proposed Solution

### 3.1 Approach: Deferred ID Extraction

Modify `IdStep` to recognize pending mutation markers and annotate them for post-mutation ID extraction. The mutation execution phase then extracts the ID instead of returning the element.

### 3.2 Implementation Details

#### 3.2.1 Update IdStep to Handle Pending Mutations

**File:** `src/traversal/transform/metadata.rs`

```rust
impl AnyStep for IdStep {
    fn apply<'a>(&'a self, _ctx: &'a ExecutionContext<'a>, input: ...) -> ... {
        Box::new(input.filter_map(|traverser| {
            match &traverser.value {
                Value::Vertex(id) => Some(traverser.split(Value::Int(id.0 as i64))),
                Value::Edge(id) => Some(traverser.split(Value::Int(id.0 as i64))),
                
                // Handle pending mutations: mark for ID extraction after mutation
                Value::Map(map) if map.contains_key("__pending_add_v") 
                                || map.contains_key("__pending_add_e") => {
                    let mut new_map = map.clone();
                    new_map.insert("__extract_id".to_string(), Value::Bool(true));
                    Some(traverser.split(Value::Map(new_map)))
                }
                
                _ => None,
            }
        }))
    }
}
```

#### 3.2.2 Update Mutation Execution to Respect `__extract_id`

**File:** `src/storage/cow.rs` in `execute_with_mutations()`

```rust
for traverser in results {
    if let Some(mutation) = PendingMutation::from_value(&traverser.value) {
        // Check if ID extraction was requested
        let extract_id = traverser.value
            .as_map()
            .map(|m| m.contains_key("__extract_id"))
            .unwrap_or(false);
        
        if let Some(result) = Self::execute_mutation(&mut wrapper, mutation) {
            if extract_id {
                // Return the ID as an integer instead of the element
                let id_value = match result {
                    Value::Vertex(vid) => Value::Int(vid.0 as i64),
                    Value::Edge(eid) => Value::Int(eid.0 as i64),
                    other => other,
                };
                final_results.push(id_value);
            } else {
                final_results.push(result);
            }
        }
    } else {
        final_results.push(traverser.value);
    }
}
```

#### 3.2.3 Update PendingMutation::from_value

**File:** `src/traversal/mutation.rs`

The `from_value` function should ignore the `__extract_id` key when parsing (it's metadata, not mutation data). Current implementation already handles this correctly since it only checks for specific keys.

### 3.3 Execution Flow After Fix

For `g.add_v("person").property("name", "Alice").id().next()`:

| Step | Input | Output |
|------|-------|--------|
| Start | `[Traverser(Null)]` | - |
| AddVStep | `[Traverser(Null)]` | `[Traverser(Map{__pending_add_v: true, ...})]` |
| PropertyStep | `[Traverser(Map{...})]` | `[Traverser(Map{..., properties: {name: "Alice"}})]` |
| IdStep | `[Traverser(Map{...})]` | `[Traverser(Map{..., __extract_id: true})]` ← **Fixed** |
| Mutation execution | Sees `__extract_id` | Creates vertex, returns `Value::Int(id)` |
| Return | - | `[Value::Int(0)]` |

---

## 4. Alternative Approaches Considered

### 4.1 Execute Mutations Eagerly

Execute mutations during step application instead of deferring to terminal steps.

**Rejected because:**
- Breaks lazy evaluation model
- Mutations would execute even if traversal is never terminated
- Complicates error handling and rollback

### 4.2 Two-Phase Execution for Mutations

Detect mutation traversals and execute them in a special mode.

**Rejected because:**
- Adds complexity to determine "is this a mutation traversal"
- Traversals can mix reads and mutations (e.g., `g.v().property("updated", true)`)
- The proposed solution is simpler and more targeted

### 4.3 Add Separate `mutation_id()` Step

Create a new step specifically for mutation ID extraction.

**Rejected because:**
- Breaks Gremlin compatibility (`.id()` should work)
- Adds API surface without necessity
- Users would need to learn when to use which step

---

## 5. Test Plan

### 5.1 Unit Tests

**File:** `src/traversal/transform/metadata.rs` (add to existing tests)

```rust
#[test]
fn id_step_preserves_pending_add_v() {
    // IdStep should pass through pending add_v markers with __extract_id flag
    let pending = Value::Map(HashMap::from([
        ("__pending_add_v".to_string(), Value::Bool(true)),
        ("label".to_string(), Value::String("person".into())),
        ("properties".to_string(), Value::Map(HashMap::new())),
    ]));
    
    let traverser = Traverser::new(pending);
    let step = IdStep;
    let ctx = /* create test context */;
    
    let results: Vec<_> = step.apply(&ctx, Box::new(std::iter::once(traverser))).collect();
    
    assert_eq!(results.len(), 1);
    let map = results[0].value.as_map().unwrap();
    assert!(map.contains_key("__pending_add_v"));
    assert!(map.contains_key("__extract_id"));
}

#[test]
fn id_step_preserves_pending_add_e() {
    // Same as above but for edge mutations
}
```

### 5.2 Integration Tests

**File:** `tests/storage/cow.rs` (add new tests)

```rust
#[test]
fn cow_add_v_id_returns_integer() {
    let graph = Graph::new();
    let g = graph.gremlin();
    
    let result = g.add_v("Person").property("name", "Alice").id().next();
    
    assert!(result.is_some());
    match result.unwrap() {
        Value::Int(id) => assert!(id >= 0),
        other => panic!("Expected Int, got {:?}", other),
    }
    
    // Verify vertex was actually created
    assert_eq!(graph.vertex_count(), 1);
}

#[test]
fn cow_add_e_id_returns_integer() {
    let graph = Graph::new();
    let alice = graph.add_vertex("Person", HashMap::new());
    let bob = graph.add_vertex("Person", HashMap::new());
    
    let g = graph.gremlin();
    let result = g.add_e("KNOWS").from_id(alice).to_id(bob).id().next();
    
    assert!(result.is_some());
    match result.unwrap() {
        Value::Int(id) => assert!(id >= 0),
        other => panic!("Expected Int, got {:?}", other),
    }
}

#[test]
fn cow_add_v_without_id_returns_vertex() {
    // Ensure existing behavior is preserved: without .id(), returns Value::Vertex
    let graph = Graph::new();
    let g = graph.gremlin();
    
    let result = g.add_v("Person").next();
    
    assert!(matches!(result, Some(Value::Vertex(_))));
}
```

### 5.3 Rhai Integration Tests

**File:** `tests/rhai_integration/traversal.rs` (add new tests)

```rust
#[test]
fn test_rhai_add_v_id_first() {
    let engine = RhaiEngine::new();
    let graph = Arc::new(Graph::new());
    
    let result: i64 = engine.eval_with_graph(
        graph.clone(),
        r#"
            let g = graph.gremlin();
            g.add_v("person").property("name", "Alice").id().first()
        "#,
    ).unwrap();
    
    assert!(result >= 0);
    assert_eq!(graph.vertex_count(), 1);
}

#[test]
fn test_rhai_add_e_with_captured_ids() {
    let engine = RhaiEngine::new();
    let graph = Arc::new(Graph::new());
    
    let edge_count: i64 = engine.eval_with_graph(
        graph.clone(),
        r#"
            let g = graph.gremlin();
            let alice = g.add_v("person").property("name", "Alice").id().first();
            let bob = g.add_v("person").property("name", "Bob").id().first();
            g.add_e("knows").from_v(alice).to_v(bob).iterate();
            g.e().count()
        "#,
    ).unwrap();
    
    assert_eq!(edge_count, 1);
}

#[test]
fn test_rhai_add_v_first_returns_vertex_id() {
    // Without .id(), first() should return a VertexId (not integer)
    let engine = RhaiEngine::new();
    let graph = Arc::new(Graph::new());
    
    let result: rhai::Dynamic = engine.eval_with_graph(
        graph.clone(),
        r#"
            let g = graph.gremlin();
            let v = g.add_v("person").first();
            v.id  // Access the .id property of VertexId
        "#,
    ).unwrap();
    
    assert!(result.is::<i64>());
}
```

---

## 6. Migration & Compatibility

### 6.1 Breaking Changes

**None.** This fix only adds behavior for a previously-broken case.

### 6.2 Backward Compatibility

Existing working patterns are unaffected:

- `g.add_v("label").next()` continues to return `Value::Vertex(id)` (see `examples/quickstart_gremlin.rs`)
- `g.v().id().to_list()` continues to return `Vec<Value::Int>`
- Rhai: `g.add_v("label").first()` continues to return `VertexId`

Only the currently-broken pattern gains functionality:
- `g.add_v("label").id().next()` will now return `Some(Value::Int(id))`

---

## 7. Success Criteria

1. `g.add_v("label").property(...).id().next()` returns `Some(Value::Int(id))`
2. `g.add_e("label").from_id(a).to_id(b).id().next()` returns `Some(Value::Int(id))`
3. Rhai pattern `g.add_v("label").id().first()` returns an integer usable in `from_v()`/`to_v()`
4. All existing tests pass without modification
5. The `examples/scripting.rs` can build graphs entirely via Rhai scripts

---

## 8. Implementation Checklist

- [ ] Update `IdStep::apply()` to handle `__pending_add_v` and `__pending_add_e` markers
- [ ] Update `CowBoundTraversal::execute_with_mutations()` to check `__extract_id` flag
- [ ] Add unit tests for `IdStep` with pending mutations
- [ ] Add integration tests for Rust API
- [ ] Add integration tests for Rhai API
- [ ] Update `examples/scripting.rs` to demonstrate the pattern
- [ ] Run full test suite and benchmarks
