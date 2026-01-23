# Spec 37: Nested Traversal Mutations

## Overview

This specification defines support for nested traversal mutations in edge creation, enabling Gremlin patterns where the target (or source) of an edge is a newly created vertex specified inline via a sub-traversal.

## Motivation

Gremlin supports powerful patterns like:

```groovy
g.V().has('Person', 'name', 'Bob')
 .addE('knows')
 .to(
     g.addV('Person').property('name', 'Alice')
 )
```

This pattern:
1. Finds existing vertex Bob
2. Creates a **new** vertex Alice inline
3. Creates a "knows" edge from Bob to the newly created Alice

This is ergonomic for graph construction workflows where you want to atomically create edges to new vertices without managing intermediate IDs.

## Goals

1. Support nested mutation traversals in `to()` and `from()` edge modulators
2. Use a fluent closure-based API that receives `GraphTraversalSource`
3. Execute nested mutations eagerly to obtain the created `VertexId`
4. Maintain type safety and composability
5. Work with both in-memory and mmap storage backends

## Non-Goals

- Transaction/rollback semantics (handled at storage level)
- Nested traversals that create multiple vertices (ambiguous endpoint)
- Deeply nested mutations (e.g., nested edge creation within nested vertex creation)

---

## 1. API Design

### 1.1 Closure-Based `to()` Modulator

The primary API uses a closure that receives the `GraphTraversalSource`:

```rust
// Pattern: Create edge to a new vertex
g.v().has_label("Person").has_value("name", "Bob")
    .add_e("knows")
    .to(|g| g.add_v("Person").property("name", "Alice"))
    .property("since", 2024)
    .next();
```

The closure signature:
```rust
FnOnce(&GraphTraversalSource<'g>) -> BoundTraversal<'g, (), Value>
```

### 1.2 Closure-Based `from()` Modulator

Similarly for the source vertex:

```rust
// Pattern: Create edge from a new vertex
g.v().has_label("Person").has_value("name", "Bob")
    .add_e("knows")
    .from(|g| g.add_v("Person").property("name", "Alice"))
    .next();
```

### 1.3 Complete Example

```rust
use interstellar::prelude::*;

fn example() -> Result<(), MutationError> {
    let graph = Graph::new();
    
    // Create initial vertex
    graph.add_vertex("Person", [("name", "Bob".into())].into());
    
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();
    
    // Find Bob, create Alice inline, connect them
    let edge = g.v()
        .has("Person", "name", "Bob")
        .add_e("knows")
        .to(|g| g.add_v("Person").property("name", "Alice"))
        .property("since", 2024)
        .next_mut(&mut graph)?;  // Uses mutable graph for mutations
    
    // Alice now exists in the graph
    let alice = g.v().has_value("name", "Alice").next();
    assert!(alice.is_some());
    
    Ok(())
}
```

### 1.4 Method Signatures

```rust
impl<'g, In> BoundAddEdgeBuilder<'g, In> {
    /// Set the target vertex from a nested traversal via closure.
    ///
    /// The closure receives the graph traversal source and should return
    /// a traversal that produces exactly one vertex (existing or new).
    ///
    /// # Example
    ///
    /// ```ignore
    /// g.v_id(bob_id)
    ///     .add_e("knows")
    ///     .to(|g| g.add_v("Person").property("name", "Alice"))
    ///     .next_mut(&mut graph);
    /// ```
    pub fn to<F>(self, f: F) -> Self
    where
        F: FnOnce(&GraphTraversalSource<'g>) -> BoundTraversal<'g, (), Value>;

    /// Set the source vertex from a nested traversal via closure.
    ///
    /// The closure receives the graph traversal source and should return
    /// a traversal that produces exactly one vertex (existing or new).
    pub fn from<F>(self, f: F) -> Self
    where
        F: FnOnce(&GraphTraversalSource<'g>) -> BoundTraversal<'g, (), Value>;
}
```

---

## 2. EdgeEndpoint Enhancement

### 2.1 New Variant

Add a `NestedTraversal` variant to store the boxed traversal:

```rust
/// Specifies the source or target vertex for an edge.
#[derive(Clone, Debug)]
pub enum EdgeEndpoint {
    /// A specific vertex ID.
    VertexId(VertexId),
    /// The current traverser (implicit from context).
    Traverser,
    /// A step label referencing a previously labeled vertex.
    StepLabel(String),
    /// A nested traversal that produces the target vertex.
    /// Executed eagerly during mutation execution.
    NestedTraversal(NestedMutationTraversal),
}
```

### 2.2 NestedMutationTraversal

A wrapper type that captures the traversal for later execution:

```rust
/// A captured traversal for nested mutation execution.
/// 
/// This type stores the traversal pipeline and is executed eagerly
/// when resolving edge endpoints during mutation execution.
#[derive(Clone, Debug)]
pub struct NestedMutationTraversal {
    /// The traversal to execute.
    traversal: Traversal<(), Value>,
}

impl NestedMutationTraversal {
    /// Create from a bound traversal (extracts the pipeline).
    pub fn from_bound<In>(bound: BoundTraversal<'_, In, Value>) -> Self {
        Self {
            traversal: bound.into_traversal(),
        }
    }
    
    /// Get the traversal for execution.
    pub fn traversal(&self) -> &Traversal<(), Value> {
        &self.traversal
    }
}
```

---

## 3. Execution Model

### 3.1 Two-Phase Execution

Nested mutation traversals require a two-phase execution model:

**Phase 1: Nested Traversal Execution**
1. When `AddEStep` encounters a `NestedTraversal` endpoint
2. Execute the nested traversal with `MutationContext`
3. If it contains pending mutations (e.g., `addV`), execute them immediately
4. Obtain the resulting `VertexId`

**Phase 2: Edge Creation**
1. Use the resolved `VertexId` as the endpoint
2. Create the edge as normal

### 3.2 MutationContext Enhancement

```rust
/// Context for executing mutations during traversal.
///
/// Provides mutable storage access for eager mutation execution,
/// enabling nested traversals to create vertices and return their IDs.
pub struct MutationContext<'s, S: GraphStorageMut> {
    storage: &'s mut S,
    interner: &'s StringInterner,
    /// Track created elements for potential rollback.
    created_vertices: Vec<VertexId>,
    created_edges: Vec<EdgeId>,
}

impl<'s, S: GraphStorageMut> MutationContext<'s, S> {
    /// Execute a nested traversal and return the first result.
    ///
    /// If the traversal contains pending mutations, they are executed
    /// immediately and the created element ID is returned.
    ///
    /// # Errors
    ///
    /// - `NestedTraversalEmpty` if the traversal produces no results
    /// - `NestedTraversalAmbiguous` if it produces multiple results
    /// - `NestedTraversalNotVertex` if the result is not a vertex
    pub fn execute_nested(
        &mut self,
        traversal: &Traversal<(), Value>,
    ) -> Result<VertexId, MutationError> {
        // Create execution context with current storage
        let ctx = ExecutionContext::new(self.storage, self.interner);
        
        // Execute the traversal
        let results: Vec<Traverser> = traversal.execute(&ctx).collect();
        
        // Must produce exactly one result
        if results.is_empty() {
            return Err(MutationError::NestedTraversalEmpty);
        }
        if results.len() > 1 {
            return Err(MutationError::NestedTraversalAmbiguous);
        }
        
        let value = &results[0].value;
        
        // Check if it's a pending mutation
        if let Some(mutation) = PendingMutation::from_value(value) {
            match mutation {
                PendingMutation::AddVertex { label, properties } => {
                    let id = self.storage.add_vertex(&label, properties);
                    self.created_vertices.push(id);
                    return Ok(id);
                }
                _ => {
                    return Err(MutationError::NestedTraversalNotVertex);
                }
            }
        }
        
        // Otherwise, it should be an existing vertex
        value.as_vertex_id()
            .ok_or(MutationError::NestedTraversalNotVertex)
    }
}
```

### 3.3 AddEStep Resolution

Update `resolve_endpoint` to handle nested traversals:

```rust
impl AddEStep {
    /// Resolve an endpoint to a vertex ID.
    fn resolve_endpoint(
        endpoint: &EdgeEndpoint,
        traverser: &Traverser,
        mutation_ctx: Option<&mut MutationContext<'_, impl GraphStorageMut>>,
    ) -> Result<VertexId, MutationError> {
        match endpoint {
            EdgeEndpoint::VertexId(id) => Ok(*id),
            
            EdgeEndpoint::Traverser => {
                traverser.as_vertex_id()
                    .ok_or(MutationError::MissingEdgeEndpoint("traverser is not a vertex"))
            }
            
            EdgeEndpoint::StepLabel(label) => {
                if let Some(values) = traverser.path.get(label) {
                    values.first()
                        .and_then(|pv| pv.as_vertex_id())
                        .ok_or_else(|| MutationError::StepLabelNotVertex(label.clone()))
                } else {
                    Err(MutationError::StepLabelNotFound(label.clone()))
                }
            }
            
            EdgeEndpoint::NestedTraversal(nested) => {
                let ctx = mutation_ctx
                    .ok_or(MutationError::NestedTraversalRequiresMutationContext)?;
                ctx.execute_nested(nested.traversal())
            }
        }
    }
}
```

---

## 4. Terminal Methods for Mutations

### 4.1 New Terminal Methods

To execute mutations, we need terminal methods that accept mutable storage:

```rust
impl<'g, In> BoundAddEdgeBuilder<'g, In> {
    /// Execute the traversal with mutation support and return the first result.
    ///
    /// This method accepts mutable storage to execute any nested mutations.
    pub fn next_mut<S: GraphStorageMut>(
        self,
        storage: &mut S,
    ) -> Result<Value, MutationError> {
        let bound = self.build();
        bound.next_mut(storage)
    }
    
    /// Execute the traversal with mutation support and collect all results.
    pub fn to_list_mut<S: GraphStorageMut>(
        self,
        storage: &mut S,
    ) -> Result<Vec<Value>, MutationError> {
        let bound = self.build();
        bound.to_list_mut(storage)
    }
    
    /// Execute the traversal with mutation support, consuming results.
    pub fn iterate_mut<S: GraphStorageMut>(
        self,
        storage: &mut S,
    ) -> Result<(), MutationError> {
        let bound = self.build();
        bound.iterate_mut(storage)
    }
}
```

### 4.2 BoundTraversal Extensions

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Execute with mutation support.
    pub fn next_mut<S: GraphStorageMut>(
        self,
        storage: &mut S,
    ) -> Result<Option<Value>, MutationError> {
        let mut ctx = MutationContext::new(storage, self.interner);
        let exec_ctx = ExecutionContext::new(ctx.storage(), self.interner);
        
        let traversers = self.traversal.execute(&exec_ctx);
        let mut executor = MutationExecutor::with_context(&mut ctx);
        let result = executor.execute(traversers);
        
        Ok(result.values.into_iter().next())
    }
}
```

---

## 5. Error Handling

### 5.1 New Error Variants

```rust
#[derive(Debug, Error)]
pub enum MutationError {
    // ... existing variants ...
    
    #[error("nested traversal produced no results")]
    NestedTraversalEmpty,
    
    #[error("nested traversal produced multiple results (expected exactly one)")]
    NestedTraversalAmbiguous,
    
    #[error("nested traversal did not produce a vertex")]
    NestedTraversalNotVertex,
    
    #[error("nested traversal requires mutation context (use next_mut/to_list_mut)")]
    NestedTraversalRequiresMutationContext,
}
```

### 5.2 Validation

- Nested traversal must produce exactly one result
- Result must be a vertex (existing or pending creation)
- Must use `*_mut()` terminal methods when nested traversals contain mutations

---

## 6. Implementation Plan

### Phase 1: Core Types

1. Add `NestedMutationTraversal` type to `mutation.rs`
2. Add `NestedTraversal` variant to `EdgeEndpoint`
3. Add new error variants to `MutationError`

### Phase 2: Builder Methods

1. Add `to()` closure method to `BoundAddEdgeBuilder`
2. Add `from()` closure method to `BoundAddEdgeBuilder`
3. Add `to()` and `from()` to `AddEdgeBuilder` (standalone g.add_e())

### Phase 3: Execution

1. Enhance `MutationContext` with `execute_nested()`
2. Update `AddEStep::resolve_endpoint()` for nested traversals
3. Add `*_mut()` terminal methods to builders

### Phase 4: Testing

1. Unit tests for `NestedMutationTraversal`
2. Unit tests for closure-based `to()`/`from()`
3. Integration tests for the full pattern
4. Error case tests

---

## 7. Testing Requirements

### 7.1 Unit Tests

```rust
#[test]
fn to_closure_creates_vertex_and_edge() {
    let graph = Graph::new();
    let bob = graph.add_vertex("Person", [("name", "Bob".into())].into());
    
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();
    
    let result = g.v_id(bob)
        .add_e("knows")
        .to(|g| g.add_v("Person").property("name", "Alice"))
        .next_mut(&mut graph);
    
    assert!(result.is_ok());
    
    // Verify Alice was created
    let alice = g.v().has_value("name", "Alice").next();
    assert!(alice.is_some());
    
    // Verify edge exists
    let edges = g.v_id(bob).out_labels(&["knows"]).to_list();
    assert_eq!(edges.len(), 1);
}

#[test]
fn to_closure_existing_vertex() {
    let graph = Graph::new();
    let bob = graph.add_vertex("Person", [("name", "Bob".into())].into());
    let alice = graph.add_vertex("Person", [("name", "Alice".into())].into());
    
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();
    
    // Use closure to find existing vertex
    let result = g.v_id(bob)
        .add_e("knows")
        .to(|g| g.v().has_value("name", "Alice"))
        .next_mut(&mut graph);
    
    assert!(result.is_ok());
}

#[test]
fn to_closure_empty_traversal_error() {
    let graph = Graph::new();
    let bob = graph.add_vertex("Person", [("name", "Bob".into())].into());
    
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();
    
    let result = g.v_id(bob)
        .add_e("knows")
        .to(|g| g.v().has_value("name", "NonExistent"))
        .next_mut(&mut graph);
    
    assert!(matches!(result, Err(MutationError::NestedTraversalEmpty)));
}

#[test]
fn to_closure_ambiguous_traversal_error() {
    let graph = Graph::new();
    let bob = graph.add_vertex("Person", [("name", "Bob".into())].into());
    graph.add_vertex("Person", [("name", "Alice".into())].into());
    graph.add_vertex("Person", [("name", "Alice".into())].into()); // Duplicate!
    
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();
    
    let result = g.v_id(bob)
        .add_e("knows")
        .to(|g| g.v().has_value("name", "Alice")) // Returns 2 vertices
        .next_mut(&mut graph);
    
    assert!(matches!(result, Err(MutationError::NestedTraversalAmbiguous)));
}
```

### 7.2 Integration Tests

```rust
#[test]
fn complex_graph_construction_with_nested_mutations() {
    let graph = Graph::new();
    
    // Create a social network in one traversal
    let bob = graph.add_vertex("Person", [("name", "Bob".into())].into());
    
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();
    
    // Bob knows Alice and Charlie (both created inline)
    g.v_id(bob)
        .add_e("knows")
        .to(|g| g.add_v("Person").property("name", "Alice"))
        .iterate_mut(&mut graph)
        .unwrap();
    
    g.v_id(bob)
        .add_e("knows")
        .to(|g| g.add_v("Person").property("name", "Charlie"))
        .iterate_mut(&mut graph)
        .unwrap();
    
    // Verify the graph structure
    let friends = g.v_id(bob)
        .out_labels(&["knows"])
        .values("name")
        .to_list();
    
    assert_eq!(friends.len(), 2);
    assert!(friends.contains(&Value::String("Alice".into())));
    assert!(friends.contains(&Value::String("Charlie".into())));
}
```

---

## 8. Future Enhancements

1. **Bi-directional edges**: `to(|g| ...).from(|g| ...)` where both are closures
2. **Multiple edge creation**: Create edges to all vertices returned by traversal (opt-in)
3. **Nested edge creation**: `g.add_v().add_e().to(|g| g.add_v())` patterns
4. **Deferred/batched execution**: Collect all nested mutations for batch execution
5. **Rollback support**: If edge creation fails, rollback nested vertex creation

---

## 9. Comparison with Gremlin

| Gremlin | Interstellar (Proposed) |
|---------|-------------------------|
| `g.V(1).addE('knows').to(g.addV('Person'))` | `g.v_id(id).add_e("knows").to(\|g\| g.add_v("Person"))` |
| `g.addE('knows').from(__.V(1)).to(__.V(2))` | `g.add_e("knows").from_vertex(id1).to_vertex(id2)` |
| `g.V(1).addE('knows').to(__.V().has('name','Bob'))` | `g.v_id(id).add_e("knows").to(\|g\| g.v().has_value("name", "Bob"))` |

The closure-based API provides:
- Full access to `GraphTraversalSource` for complex queries
- Type safety through Rust's closure type checking
- Familiar Gremlin-like semantics with idiomatic Rust patterns
