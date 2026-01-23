# Spec 38: Dynamic Property Values via Traversal

## Overview

This specification defines support for setting property values dynamically using a sub-traversal, enabling Gremlin patterns where the property value is computed from the current element rather than being a static constant.

## Motivation

Gremlin supports powerful patterns like:

```groovy
g.V().has('Person', 'name', 'Alice')
 .choose(
   has('age'),
   property('age', values('age').map{ it + 1 }),
   property('age', 1)
 )
 .values('age')
```

This pattern:
1. Find Alice
2. **If** she has an 'age' property → increment it by 1
3. **Else** → set age to 1
4. Return the new age

This is essential for:
- Conditional updates based on current state
- Computed property values (aggregations, transformations)
- Atomic read-modify-write patterns

## Goals

1. Support `property_from(key, traversal)` where the value comes from a sub-traversal
2. Integrate with existing `choose()`, `coalesce()`, and other branching steps
3. Maintain the deferred execution model for mutations
4. Provide fluent API consistent with existing patterns
5. Work with both in-memory and mmap storage backends

## Non-Goals

- Multi-value properties (cardinality) - future work
- Transaction semantics - handled at storage level
- Property removal via traversal (use `drop()` for that)

---

## 1. Current State Analysis

### 1.1 Existing PropertyStep

```rust
// Current implementation - static value only
pub struct PropertyStep {
    key: String,
    value: Value,  // Static constant
}
```

### 1.2 Related Steps Already Implemented

| Step | Status | Notes |
|------|--------|-------|
| `choose(cond, if_true, if_false)` | ✅ | Conditional branching |
| `has('key')` | ✅ | Property existence filter |
| `values('key')` | ✅ | Extract property values |
| `math("_ + 1")` | ✅ | Arithmetic expressions |
| `map(\|v\| ...)` | ✅ | Value transformation |
| `property('key', <static>)` | ✅ | Set static property value |
| `property('key', <traversal>)` | ❌ | **Not implemented** |

---

## 2. API Design

### 2.1 New Property Step Variant

Add a new method that accepts a sub-traversal for computing the value:

```rust
// New API: property with traversal-computed value
g.v().has_label("Person").has_value("name", "Alice")
    .property_from("age", __.values("age").math("_ + 1").build())
    .next_mut(&mut graph);
```

### 2.2 Complete Example with choose()

```rust
use interstellar::prelude::*;

fn increment_or_initialize_age(graph: &mut Graph) -> Result<(), MutationError> {
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();
    
    // Find Alice and update her age
    let result = g.v()
        .has("Person", "name", "Alice")
        .choose(
            __.has("age"),  // condition: has age property?
            // if true: increment age
            __.property_from("age", __.values("age").math("_ + 1").build()),
            // if false: set age to 1
            __.property("age", 1)
        )
        .values("age")
        .next_mut(&mut graph)?;
    
    println!("Alice's new age: {:?}", result);
    Ok(())
}
```

### 2.3 API Signatures

```rust
impl<In, Out> Traversal<In, Out> {
    /// Set a property value computed from a sub-traversal.
    ///
    /// The sub-traversal is executed with the current traverser as input,
    /// and its first result becomes the property value.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to set
    /// * `value_traversal` - A traversal that computes the value
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Increment age by 1
    /// g.v_id(alice_id)
    ///     .property_from("age", __.values("age").math("_ + 1").build())
    ///     .iterate_mut(&mut graph);
    ///
    /// // Set full_name from existing property
    /// g.v_id(id)
    ///     .property_from("display_name", __.coalesce(vec![
    ///         __.values("nickname"),
    ///         __.values("name"),
    ///     ]))
    ///     .iterate_mut(&mut graph);
    /// ```
    pub fn property_from(
        self,
        key: impl Into<String>,
        value_traversal: Traversal<Value, Value>,
    ) -> Traversal<In, Value>;
}
```

---

## 3. Implementation Design

### 3.1 PropertyFromTraversalStep

A new step that computes the property value dynamically:

```rust
/// Step that sets a property using a value computed from a sub-traversal.
///
/// The sub-traversal is executed with the current element as input,
/// and its first result becomes the property value.
#[derive(Clone)]
pub struct PropertyFromTraversalStep {
    key: String,
    value_traversal: Traversal<Value, Value>,
}

impl PropertyFromTraversalStep {
    /// Create a new step with the given key and value-computing traversal.
    pub fn new(key: impl Into<String>, value_traversal: Traversal<Value, Value>) -> Self {
        Self {
            key: key.into(),
            value_traversal,
        }
    }
    
    /// Get the property key.
    pub fn key(&self) -> &str {
        &self.key
    }
    
    /// Get the value traversal.
    pub fn value_traversal(&self) -> &Traversal<Value, Value> {
        &self.value_traversal
    }
}

impl AnyStep for PropertyFromTraversalStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let key = self.key.clone();
        let value_traversal = self.value_traversal.clone();
        
        Box::new(input.filter_map(move |t| {
            // Execute the value traversal with current traverser as input
            let value_input = Box::new(std::iter::once(t.clone()));
            let mut value_result = execute_traversal_from(ctx, &value_traversal, value_input);
            
            // Get the first result as the new property value
            let computed_value = value_result.next()?.value;
            
            // Create pending mutation marker
            match &t.value {
                Value::Vertex(id) => {
                    Some(Traverser::new(Value::Map(HashMap::from([
                        ("__pending_property_vertex".to_string(), Value::Bool(true)),
                        ("id".to_string(), Value::Vertex(*id)),
                        ("key".to_string(), Value::String(key.clone())),
                        ("value".to_string(), computed_value),
                    ]))))
                }
                Value::Edge(id) => {
                    Some(Traverser::new(Value::Map(HashMap::from([
                        ("__pending_property_edge".to_string(), Value::Bool(true)),
                        ("id".to_string(), Value::Edge(*id)),
                        ("key".to_string(), Value::String(key.clone())),
                        ("value".to_string(), computed_value),
                    ]))))
                }
                // Handle pending add_v/add_e
                Value::Map(map) if map.contains_key("__pending_add_v") => {
                    let mut new_t = t.clone();
                    if let Value::Map(ref mut m) = new_t.value {
                        if let Some(Value::Map(props)) = m.get_mut("properties") {
                            props.insert(key.clone(), computed_value);
                        }
                    }
                    Some(new_t)
                }
                Value::Map(map) if map.contains_key("__pending_add_e") => {
                    let mut new_t = t.clone();
                    if let Value::Map(ref mut m) = new_t.value {
                        if let Some(Value::Map(props)) = m.get_mut("properties") {
                            props.insert(key.clone(), computed_value);
                        }
                    }
                    Some(new_t)
                }
                _ => {
                    // Non-element values: pass through unchanged
                    Some(t)
                }
            }
        }))
    }
    
    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }
    
    fn name(&self) -> &'static str {
        "property(traversal)"
    }
}
```

### 3.2 Clone Implementation for Traversal

The `value_traversal` field requires `Traversal` to implement `Clone`. This should already be the case since steps are stored as `Vec<Box<dyn AnyStep>>` and `AnyStep` requires `clone_box()`.

```rust
impl<In, Out> Clone for Traversal<In, Out> {
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            steps: self.steps.iter().map(|s| s.clone_box()).collect(),
            _phantom: PhantomData,
        }
    }
}
```

---

## 4. Anonymous Traversal Integration

### 4.1 Add to `__` Module

```rust
/// Set a property value computed from a sub-traversal.
///
/// # Example
///
/// ```ignore
/// // In choose() branches
/// __.property_from("age", __.values("age").math("_ + 1").build())
/// ```
pub fn property_from(
    key: impl Into<String>,
    value_traversal: Traversal<Value, Value>,
) -> Traversal<Value, Value> {
    Traversal::<Value, Value>::new()
        .add_step(PropertyFromTraversalStep::new(key, value_traversal))
}
```

### 4.2 Usage in choose()

```rust
// The pattern from the user's query
g.v().has("Person", "name", "Alice")
    .choose(
        __.has("age"),
        __.property_from("age", __.values("age").math("_ + 1").build()),
        __.property("age", 1)
    )
    .values("age")
    .next_mut(&mut graph);
```

---

## 5. Execution Flow

### 5.1 Step-by-Step Execution

For the query:
```rust
g.v().has("Person", "name", "Alice")
    .choose(
        __.has("age"),
        __.property_from("age", __.values("age").math("_ + 1").build()),
        __.property("age", 1)
    )
    .values("age")
    .next_mut(&mut graph)
```

**Execution Flow:**

1. `g.v()` - Emit all vertices
2. `has("Person", "name", "Alice")` - Filter to Alice
3. `choose(...)` - For each traverser (Alice):
   a. Execute condition: `__.has("age")`
      - If Alice has age → produces result → condition is TRUE
      - If Alice lacks age → no result → condition is FALSE
   b. Based on condition, execute appropriate branch:
      - TRUE branch: `__.property_from("age", __.values("age").math("_ + 1").build())`
        - Execute value traversal: `values("age")` → 30, `math("_ + 1")` → 31.0
        - Create pending mutation: `{__pending_property_vertex: true, id: Alice, key: "age", value: 31.0}`
      - FALSE branch: `__.property("age", 1)`
        - Create pending mutation: `{__pending_property_vertex: true, id: Alice, key: "age", value: 1}`
4. `values("age")` - **Issue: traverser is now a pending mutation map, not a vertex**
5. `next_mut(&mut graph)` - Execute mutations

### 5.2 Design Decision: Return Element or Pending Mutation?

**Issue**: After `property_from()`, should the traverser contain:
- (A) The original vertex (so subsequent steps like `values()` work), OR
- (B) The pending mutation marker (current behavior of `PropertyStep`)

**Recommendation**: Option (A) - Return the original element, but also track the pending mutation via side-effects.

### 5.3 Proposed Solution: Mutation Side Effects

Enhance `ExecutionContext` to collect mutations:

```rust
pub struct ExecutionContext<'a> {
    storage: &'a dyn GraphStorage,
    interner: &'a StringInterner,
    side_effects: &'a SideEffects,
    track_paths: bool,
    /// Collected pending mutations (when in mutation mode)
    pending_mutations: RefCell<Vec<PendingMutation>>,
}

impl<'a> ExecutionContext<'a> {
    /// Record a pending mutation.
    pub fn record_mutation(&self, mutation: PendingMutation) {
        self.pending_mutations.borrow_mut().push(mutation);
    }
    
    /// Get all pending mutations.
    pub fn take_mutations(&self) -> Vec<PendingMutation> {
        self.pending_mutations.take()
    }
}
```

Then `PropertyFromTraversalStep` becomes:

```rust
impl AnyStep for PropertyFromTraversalStep {
    fn apply<'a>(&'a self, ctx: &'a ExecutionContext<'a>, input: ...) -> ... {
        Box::new(input.filter_map(move |t| {
            // Execute value traversal
            let computed_value = /* ... */;
            
            // Record the mutation as a side effect
            match &t.value {
                Value::Vertex(id) => {
                    ctx.record_mutation(PendingMutation::SetVertexProperty {
                        id: *id,
                        key: self.key.clone(),
                        value: computed_value,
                    });
                }
                // ... other cases
            }
            
            // Return the ORIGINAL traverser unchanged
            Some(t)
        }))
    }
}
```

This allows chaining:
```rust
g.v().property_from("age", ...).values("age")  // Works!
```

---

## 6. Error Handling

### 6.1 Error Cases

| Scenario | Behavior |
|----------|----------|
| Value traversal produces no results | Filter out traverser (no mutation) |
| Value traversal produces non-primitive | Use the value as-is |
| Applied to non-element | Pass through unchanged |
| Mutation fails at execution | Return error from `next_mut()` |

### 6.2 New Error Variants

```rust
#[derive(Debug, Error)]
pub enum MutationError {
    // ... existing variants ...
    
    #[error("property value traversal produced no results")]
    PropertyTraversalEmpty,
}
```

---

## 7. Implementation Plan

### Phase 1: Core Implementation
1. Add `PropertyFromTraversalStep` to `src/traversal/mutation.rs`
2. Implement `Clone` for `Traversal` if not already present
3. Add step application logic

### Phase 2: API Integration
1. Add `property_from()` method to `Traversal` in `src/traversal/builder.rs`
2. Add `property_from()` method to `BoundTraversal` in `src/traversal/source.rs`
3. Add `property_from()` factory to `__` module in `src/traversal/anonymous.rs`

### Phase 3: Execution Context Enhancement (Optional but Recommended)
1. Add `pending_mutations` field to `ExecutionContext`
2. Update `PropertyFromTraversalStep` to use side-effect recording
3. Update terminal methods to collect and execute mutations
4. This enables proper chaining with subsequent steps

### Phase 4: Testing
1. Unit tests for `PropertyFromTraversalStep`
2. Integration tests with `choose()`
3. Integration tests with `coalesce()`
4. Error case tests

---

## 8. Testing Requirements

### 8.1 Unit Tests

```rust
#[test]
fn property_from_increments_value() {
    let graph = Graph::new();
    let alice = graph.add_vertex("Person", [("name", "Alice".into()), ("age", 30.into())].into());
    
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();
    
    g.v_id(alice)
        .property_from("age", __.values("age").math("_ + 1").build())
        .iterate_mut(&mut graph)
        .unwrap();
    
    // Verify age was incremented
    let new_age = g.v_id(alice).values("age").next();
    assert_eq!(new_age, Some(Value::Float(31.0)));
}

#[test]
fn property_from_with_choose_increment_or_initialize() {
    let graph = Graph::new();
    
    // Alice has age
    let alice = graph.add_vertex("Person", [("name", "Alice".into()), ("age", 30.into())].into());
    // Bob does NOT have age
    let bob = graph.add_vertex("Person", [("name", "Bob".into())].into());
    
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();
    
    // Increment if has age, else set to 1
    g.v()
        .has_label("Person")
        .choose(
            __.has("age"),
            __.property_from("age", __.values("age").math("_ + 1").build()),
            __.property("age", 1)
        )
        .iterate_mut(&mut graph)
        .unwrap();
    
    // Alice: 30 + 1 = 31
    let alice_age = g.v_id(alice).values("age").next();
    assert_eq!(alice_age, Some(Value::Float(31.0)));
    
    // Bob: initialized to 1
    let bob_age = g.v_id(bob).values("age").next();
    assert_eq!(bob_age, Some(Value::Int(1)));
}

#[test]
fn property_from_empty_traversal_filters() {
    let graph = Graph::new();
    let alice = graph.add_vertex("Person", [("name", "Alice".into())].into());
    
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();
    
    // Try to set age from non-existent property - should filter
    let result = g.v_id(alice)
        .property_from("age", __.values("missing_property"))
        .to_list_mut(&mut graph)
        .unwrap();
    
    // No results since value traversal produced nothing
    assert!(result.is_empty());
    
    // Age should not be set
    let age = g.v_id(alice).values("age").next();
    assert!(age.is_none());
}
```

### 8.2 Integration Tests

```rust
#[test]
fn property_from_in_coalesce() {
    let graph = Graph::new();
    let v = graph.add_vertex("Person", [("name", "Alice".into())].into());
    
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();
    
    // Try to use nickname, fall back to name, set as display_name
    g.v_id(v)
        .property_from("display_name", 
            __.coalesce(vec![
                __.values("nickname"),
                __.values("name"),
            ])
        )
        .iterate_mut(&mut graph)
        .unwrap();
    
    let display_name = g.v_id(v).values("display_name").next();
    assert_eq!(display_name, Some(Value::String("Alice".into())));
}

#[test]
fn property_from_chained_with_values() {
    let graph = Graph::new();
    let alice = graph.add_vertex("Person", [("name", "Alice".into()), ("age", 30.into())].into());
    
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();
    
    // With side-effect recording, this should work
    let result = g.v_id(alice)
        .property_from("age", __.values("age").math("_ + 1").build())
        .values("age")
        .next_mut(&mut graph)
        .unwrap();
    
    // Should return the NEW age (31) after mutation
    assert_eq!(result, Some(Value::Float(31.0)));
}
```

---

## 9. Comparison with Gremlin

| Gremlin | Interstellar (Proposed) |
|---------|-------------------------|
| `property('age', values('age').map{ it + 1 })` | `property_from("age", __.values("age").math("_ + 1").build())` |
| `property('x', constant(5))` | `property("x", 5)` (static value) |
| `property('name', select('a').values('name'))` | `property_from("name", __.select("a").values("name"))` |

### Key Differences

1. **Method name**: `property_from()` vs overloaded `property()` to maintain type safety
2. **Traversal building**: Explicit `.build()` call needed for anonymous traversals
3. **Arithmetic**: `math("_ + 1")` instead of Groovy closure `{ it + 1 }`

---

## 10. Files to Modify

| File | Changes |
|------|---------|
| `src/traversal/mutation.rs` | Add `PropertyFromTraversalStep` |
| `src/traversal/builder.rs` | Add `property_from()` method to `Traversal` |
| `src/traversal/source.rs` | Add `property_from()` method to `BoundTraversal` |
| `src/traversal/anonymous.rs` | Add `property_from()` factory function |
| `src/traversal/context.rs` | (Optional) Add `pending_mutations` to `ExecutionContext` |
| `src/traversal/mod.rs` | Export `PropertyFromTraversalStep` |

---

## 11. Future Enhancements

1. **Closure-based API**: `property_computed("age", |t| t.values("age").math("_ + 1"))`
2. **Multi-value properties**: Support for property cardinality (list, set)
3. **Property deletion**: `property_drop("key")` step
4. **Bulk property updates**: `properties_from(map_traversal)`
5. **String operations**: Concatenation, formatting for computed string properties

---

## 12. Dependencies

This spec builds upon:
- **Spec 10**: Mutation Steps (existing `PropertyStep`)
- **Spec 07**: MathStep (for arithmetic in value traversals)
- **Spec 04**: Anonymous Traversals (for `__` factory usage)
- **Spec 15**: Branch/Option Steps (`choose()`, `coalesce()`)

---

## 13. Summary

To support the query:
```groovy
g.V().has('Person', 'name', 'Alice')
 .choose(has('age'), property('age', values('age').map{ it + 1 }), property('age', 1))
 .values('age')
```

We need to implement:

1. **`PropertyFromTraversalStep`** - New step that computes property value from sub-traversal
2. **`property_from(key, traversal)`** - API method on `Traversal` and `BoundTraversal`
3. **`__.property_from(key, traversal)`** - Anonymous traversal factory function
4. **Side-effect mutation collection** - Optional enhancement for proper chaining

The Rust equivalent will be:
```rust
g.v().has("Person", "name", "Alice")
    .choose(
        __.has("age"),
        __.property_from("age", __.values("age").math("_ + 1").build()),
        __.property("age", 1)
    )
    .values("age")
    .next_mut(&mut graph)
```
