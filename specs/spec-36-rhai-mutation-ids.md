# Spec 36: Rhai Mutation ID Capture

This specification addresses the gap in the Rhai scripting API where mutation steps (`add_v`, `add_e`) do not properly return element IDs for use in subsequent operations.

---

## 1. Problem Statement

### 1.1 Current Behavior

When attempting to create a graph entirely via Rhai scripts, the following pattern fails:

```javascript
let g = graph.gremlin();

// Create a vertex and capture its ID
let alice = g.add_v("person")
    .property("name", "Alice")
    .id()
    .first();  // Returns () instead of VertexId

// This fails: "Function not found: from_v (Traversal, ())"
g.add_e("knows").from_v(alice).to_v(bob).iterate();
```

The `id().first()` chain after `add_v()` returns unit `()` instead of the created vertex's `VertexId`.

### 1.2 Expected Behavior

```javascript
let alice = g.add_v("person").property("name", "Alice").id().first();
// alice should be a VertexId that can be used in from_v()/to_v()

g.add_e("knows").from_v(alice).to_v(bob).iterate();
// Should successfully create an edge
```

### 1.3 Impact

- Users cannot build graphs entirely via Rhai scripts
- The `scripting.rs` example must use Rust code for graph creation
- Limits the usefulness of Rhai for dynamic graph construction scenarios

---

## 2. Root Cause Analysis

### 2.1 Traversal Flow

The Rhai traversal execution path is:

1. `RhaiTraversalSource::add_v(label)` creates a `RhaiTraversal` with `RhaiStep::AddV`
2. `.property()` adds `RhaiStep::Property` steps
3. `.id()` adds `RhaiStep::Id`
4. `.first()` calls `to_list()` which:
   - Calls `graph.gremlin()` to get a `CowTraversalSource`
   - Builds the traversal via `apply_step_cow()`
   - Executes and returns results

### 2.2 Suspected Issues

1. **Source mismatch**: `RhaiTraversalSource::add_v()` creates a traversal with `TraversalSource::Inject(vec![])` as the source, but mutations need to flow through the mutation executor.

2. **ID extraction timing**: The `IdStep` may be applied before the vertex is actually created, resulting in no elements to extract IDs from.

3. **Mutation vs Query path**: The `CowTraversalSource` distinguishes between query traversals (starting with `v()`, `e()`) and mutation traversals (starting with `add_v()`, `add_e()`). The Rhai layer may not be correctly routing through the mutation path.

### 2.3 Investigation Points

Review these code locations:

- `src/rhai/traversal.rs:137-161` - `RhaiTraversalSource::add_v()` and `add_e()`
- `src/rhai/traversal.rs:375-396` - `RhaiTraversal::to_list()` execution
- `src/rhai/traversal.rs:1890-1909` - `apply_step_cow` handling of `AddV` and `AddE`
- `src/storage/cow.rs` - `CowTraversalSource` mutation handling

---

## 3. Proposed Solution

### 3.1 Option A: Fix the Execution Path

Modify `RhaiTraversal::to_list()` to detect mutation-starting traversals and route them through the correct execution path that returns created elements.

```rust
// In RhaiTraversal::to_list()
pub fn to_list(&self) -> Vec<Value> {
    let g = self.graph.gremlin();
    
    // Check if this is a mutation-starting traversal
    if self.is_mutation_start() {
        return self.execute_mutation(&g);
    }
    
    // Existing query path...
}

fn is_mutation_start(&self) -> bool {
    matches!(self.steps.first(), Some(RhaiStep::AddV(_)) | Some(RhaiStep::AddE { .. }))
}

fn execute_mutation(&self, g: &CowTraversalSource) -> Vec<Value> {
    // Route through mutation-aware execution
    // Return created element wrapped in Value
}
```

### 3.2 Option B: Dedicated Mutation Methods

Add explicit mutation methods to `RhaiTraversalSource` that return IDs directly:

```javascript
// New Rhai API
let alice = g.create_vertex("person", #{
    name: "Alice",
    age: 30
});  // Returns VertexId directly

let edge = g.create_edge("knows", alice, bob, #{
    since: 2020
});  // Returns EdgeId directly
```

Implementation:

```rust
// In RhaiTraversalSource
pub fn create_vertex(&self, label: String, properties: rhai::Map) -> VertexId {
    let props = map_to_hashmap(properties);
    self.graph.add_vertex(&label, props)
}

pub fn create_edge(&self, label: String, from: VertexId, to: VertexId, properties: rhai::Map) -> Result<EdgeId, ...> {
    let props = map_to_hashmap(properties);
    self.graph.add_edge(from, to, &label, props)
}
```

### 3.3 Option C: Hybrid Approach

Implement both:
1. Fix the traversal-based mutation flow (Option A) for Gremlin compatibility
2. Add convenience methods (Option B) for simpler scripting use cases

---

## 4. Recommended Approach

**Option C (Hybrid)** is recommended because:

1. **Gremlin compatibility**: Users familiar with Gremlin expect `add_v().property().id()` to work
2. **Scripting ergonomics**: Direct `create_vertex()` methods are more natural for scripts
3. **Flexibility**: Users can choose the pattern that fits their use case

---

## 5. Implementation Plan

### Phase 1: Investigate and Diagnose

1. Add test case that reproduces the issue
2. Add debug logging to trace the execution path
3. Identify exact point of failure

### Phase 2: Fix Traversal-Based Mutations

1. Modify `RhaiTraversal::to_list()` to handle mutation-starting traversals
2. Ensure `AddVStep` execution returns the created vertex
3. Ensure `IdStep` correctly extracts ID from mutation results
4. Add tests for `add_v().id().first()` pattern

### Phase 3: Add Convenience Methods

1. Add `create_vertex(label, properties)` to `RhaiTraversalSource`
2. Add `create_edge(label, from, to, properties)` to `RhaiTraversalSource`
3. Register these functions with the Rhai engine
4. Add tests for convenience methods

### Phase 4: Update Examples

1. Update `examples/scripting.rs` to build graph via scripts
2. Demonstrate both patterns (traversal-based and convenience methods)

---

## 6. Test Cases

### 6.1 Traversal-Based Mutation Tests

```rust
#[test]
fn test_rhai_add_v_returns_vertex_id() {
    let graph = Arc::new(Graph::new());
    let engine = RhaiEngine::new();
    
    let script = r#"
        let g = graph.gremlin();
        let id = g.add_v("person").property("name", "Alice").id().first();
        id
    "#;
    
    let result: VertexId = engine.eval_with_graph(graph.clone(), script).unwrap();
    assert!(graph.get_vertex(result).is_some());
}

#[test]
fn test_rhai_add_e_with_captured_ids() {
    let graph = Arc::new(Graph::new());
    let engine = RhaiEngine::new();
    
    let script = r#"
        let g = graph.gremlin();
        let alice = g.add_v("person").property("name", "Alice").id().first();
        let bob = g.add_v("person").property("name", "Bob").id().first();
        g.add_e("knows").from_v(alice).to_v(bob).iterate();
        g.e().count()
    "#;
    
    let count: i64 = engine.eval_with_graph(graph, script).unwrap();
    assert_eq!(count, 1);
}
```

### 6.2 Convenience Method Tests

```rust
#[test]
fn test_rhai_create_vertex() {
    let graph = Arc::new(Graph::new());
    let engine = RhaiEngine::new();
    
    let script = r#"
        let g = graph.gremlin();
        let id = g.create_vertex("person", #{ name: "Alice", age: 30 });
        id
    "#;
    
    let result: VertexId = engine.eval_with_graph(graph.clone(), script).unwrap();
    let vertex = graph.get_vertex(result).unwrap();
    assert_eq!(vertex.label(), "person");
}

#[test]
fn test_rhai_create_edge() {
    let graph = Arc::new(Graph::new());
    let engine = RhaiEngine::new();
    
    let script = r#"
        let g = graph.gremlin();
        let alice = g.create_vertex("person", #{ name: "Alice" });
        let bob = g.create_vertex("person", #{ name: "Bob" });
        let edge = g.create_edge("knows", alice, bob, #{ since: 2020 });
        edge
    "#;
    
    let result: EdgeId = engine.eval_with_graph(graph.clone(), script).unwrap();
    let edge = graph.get_edge(result).unwrap();
    assert_eq!(edge.label(), "knows");
}
```

---

## 7. Success Criteria

1. `g.add_v("label").property(...).id().first()` returns a valid `VertexId`
2. `g.add_e("label").from_v(id1).to_v(id2).id().first()` returns a valid `EdgeId`
3. `g.create_vertex(label, props)` returns a `VertexId`
4. `g.create_edge(label, from, to, props)` returns an `EdgeId`
5. `examples/scripting.rs` builds its graph entirely via Rhai scripts
6. All existing Rhai tests continue to pass

---

## 8. Open Questions

1. Should `create_vertex` / `create_edge` be on the graph object directly (`graph.create_vertex(...)`) or on the traversal source (`g.create_vertex(...)`)?

2. Should we support batch creation methods for performance?
   ```javascript
   g.create_vertices("person", [
       #{ name: "Alice" },
       #{ name: "Bob" }
   ])  // Returns array of VertexIds
   ```

3. Should failed mutations throw Rhai exceptions or return error values?
