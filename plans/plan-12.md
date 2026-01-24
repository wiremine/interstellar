# Plan 12: Implement Mutation Steps

**Spec Reference:** `specs/spec-10-mutations.md`

**Goal:** Implement Gremlin mutation steps (`addV`, `addE`, `property`, `drop`) for creating, updating, and deleting graph elements.

**Estimated Duration:** 2-3 weeks

---

## Overview

This plan implements the mutation steps defined in Spec 10. The implementation is divided into phases, starting with storage layer support and building up to the full traversal API.

---

## Phase 1: Storage Layer Mutations (Week 1, Days 1-3)

### 1.1 Extend GraphStorage Trait

**File:** `src/storage/mod.rs`

Add mutation methods to the `GraphStorage` trait:

```rust
pub trait GraphStorage: Send + Sync {
    // ... existing methods ...
    
    // Mutation methods
    fn add_vertex(&self, label: &str, properties: HashMap<String, Value>) -> Result<VertexId, StorageError>;
    fn add_edge(&self, label: &str, from: VertexId, to: VertexId, properties: HashMap<String, Value>) -> Result<EdgeId, StorageError>;
    fn set_vertex_property(&self, id: VertexId, key: &str, value: Value) -> Result<(), StorageError>;
    fn set_edge_property(&self, id: EdgeId, key: &str, value: Value) -> Result<(), StorageError>;
    fn remove_vertex(&self, id: VertexId) -> Result<(), StorageError>;
    fn remove_edge(&self, id: EdgeId) -> Result<(), StorageError>;
}
```

**Tasks:**
- [ ] Add mutation method signatures to `GraphStorage` trait
- [ ] Add `MutationError` variants to `StorageError`
- [ ] Update trait documentation

### 1.2 Implement for Graph

**File:** `src/storage/inmemory.rs`

Implement mutation methods for the in-memory storage backend:

**Tasks:**
- [ ] Implement `add_vertex()` - generate ID, insert into vertices map
- [ ] Implement `add_edge()` - validate endpoints, generate ID, update adjacency lists
- [ ] Implement `set_vertex_property()` - find vertex, update property
- [ ] Implement `set_edge_property()` - find edge, update property
- [ ] Implement `remove_vertex()` - remove vertex and all incident edges
- [ ] Implement `remove_edge()` - remove edge, update adjacency lists
- [ ] Ensure thread-safety with `RwLock` for concurrent access
- [ ] Write unit tests for each method

### 1.3 Implement for MmapGraph

**File:** `src/storage/mmap/mod.rs`

Implement mutation methods for memory-mapped storage:

**Tasks:**
- [ ] Implement `add_vertex()` - append to vertex file
- [ ] Implement `add_edge()` - append to edge file
- [ ] Implement `set_vertex_property()` - append property record
- [ ] Implement `set_edge_property()` - append property record
- [ ] Implement `remove_vertex()` - mark as tombstone
- [ ] Implement `remove_edge()` - mark as tombstone
- [ ] Handle file growth and remapping
- [ ] Write unit tests for each method

---

## Phase 2: addV() Step (Week 1, Days 4-5)

### 2.1 AddVertexStep Implementation

**File:** `src/traversal/mutation.rs` (new file)

Create the mutation steps module:

**Tasks:**
- [ ] Create `src/traversal/mutation.rs`
- [ ] Add `mod mutation;` to `src/traversal/mod.rs`
- [ ] Implement `AddVertexStep` struct
- [ ] Implement `Step` trait for `AddVertexStep`

### 2.2 AddVertexBuilder

**Tasks:**
- [ ] Create `AddVertexBuilder` for fluent property chaining
- [ ] Implement `.property(key, value)` method
- [ ] Implement terminal methods (`.next()`, `.to_list()`, `.iterate()`)
- [ ] Handle property accumulation before execution

### 2.3 Traversal Integration

**Tasks:**
- [ ] Add `add_v(label)` method to `GraphTraversalSource`
- [ ] Add `add_v_with_props(label, props)` convenience method
- [ ] Write integration tests

---

## Phase 3: addE() Step (Week 2, Days 1-3)

### 3.1 AddEdgeStep Implementation

**File:** `src/traversal/mutation.rs`

**Tasks:**
- [ ] Implement `AddEdgeStep` struct
- [ ] Implement `Step` trait for `AddEdgeStep`
- [ ] Handle implicit `from` vertex from traverser context

### 3.2 AddEdgeBuilder

**Tasks:**
- [ ] Create `AddEdgeBuilder` with state machine pattern
- [ ] Implement `.from_vertex(id)` method
- [ ] Implement `.from_traversal(t)` method
- [ ] Implement `.to_vertex(id)` method
- [ ] Implement `.to_traversal(t)` method
- [ ] Implement `.property(key, value)` method
- [ ] Validate both endpoints are set before execution
- [ ] Implement terminal methods

### 3.3 Edge Endpoint Resolution

**Tasks:**
- [ ] Implement `EdgeEndpoint` enum (VertexId, Traversal, StepLabel)
- [ ] Implement resolution logic for traversal endpoints
- [ ] Handle error cases (empty traversal, multiple vertices)
- [ ] Support step label references via `as_()`

### 3.4 Traversal Integration

**Tasks:**
- [ ] Add `add_e(label)` method to `Traversal<Vertex>` (implicit from)
- [ ] Add `add_e(label)` method to `GraphTraversalSource` (explicit from/to)
- [ ] Write integration tests

---

## Phase 4: property() Step (Week 2, Days 4-5)

### 4.1 PropertyStep Implementation

**File:** `src/traversal/mutation.rs`

**Tasks:**
- [ ] Implement `PropertyStep` struct for vertices
- [ ] Implement `PropertyStep` struct for edges
- [ ] Implement `Step` trait for both

### 4.2 Traversal Integration

**Tasks:**
- [ ] Add `.property(key, value)` to `Traversal<Vertex>`
- [ ] Add `.property(key, value)` to `Traversal<Edge>`
- [ ] Ensure chaining works correctly
- [ ] Write unit tests

---

## Phase 5: drop() Step (Week 3, Days 1-2)

### 5.1 DropStep Implementation

**File:** `src/traversal/mutation.rs`

**Tasks:**
- [ ] Implement `DropStep` struct
- [ ] Handle vertex deletion with edge cascade
- [ ] Handle edge deletion
- [ ] Implement `Step` trait

### 5.2 Traversal Integration

**Tasks:**
- [ ] Add `.drop()` to `Traversal<Vertex>`
- [ ] Add `.drop()` to `Traversal<Edge>`
- [ ] Ensure it works with filtered traversals (e.g., `g.v().has(...).drop()`)
- [ ] Write unit tests

---

## Phase 6: Anonymous Traversal Support (Week 3, Day 3)

### 6.1 Add Mutation Methods to `__` Module

**File:** `src/traversal/mod.rs` (__ module section)

**Tasks:**
- [ ] Add `__::add_v(label)` factory function
- [ ] Add `__::add_e(label)` factory function
- [ ] Add `__::property(key, value)` factory function
- [ ] Add `__::drop()` factory function
- [ ] Write tests for anonymous mutation traversals

---

## Phase 7: Documentation and Examples (Week 3, Days 4-5)

### 7.1 Update API Documentation

**Tasks:**
- [ ] Update `Gremlin_api.md` with implemented mutation steps
- [ ] Add rustdoc comments to all new public APIs
- [ ] Update `README.md` if needed

### 7.2 Create Example

**File:** `examples/mutations.rs`

**Tasks:**
- [ ] Create comprehensive example demonstrating all mutation steps
- [ ] Show common patterns (create graph, update properties, delete elements)
- [ ] Include error handling examples

### 7.3 Integration Tests

**File:** `tests/mutations.rs`

**Tasks:**
- [ ] Create integration test file
- [ ] Test mutations with Graph
- [ ] Test mutations with MmapGraph
- [ ] Test complex traversals mixing reads and writes
- [ ] Test concurrent mutations (thread safety)

---

## Testing Checklist

### Unit Tests
- [ ] `Graph::add_vertex()` creates vertex correctly
- [ ] `Graph::add_edge()` creates edge correctly
- [ ] `Graph::add_edge()` fails if vertices don't exist
- [ ] `Graph::set_vertex_property()` updates property
- [ ] `Graph::set_edge_property()` updates property
- [ ] `Graph::remove_vertex()` removes vertex
- [ ] `Graph::remove_vertex()` cascades to edges
- [ ] `Graph::remove_edge()` removes edge
- [ ] Same tests for `MmapGraph`

### Integration Tests
- [ ] `g.add_v().next()` creates and returns vertex
- [ ] `g.add_v().property().property().next()` chains properties
- [ ] `g.v_id(x).add_e().to_vertex(y).next()` creates edge
- [ ] `g.add_e().from_vertex(x).to_vertex(y).next()` creates edge
- [ ] `g.v_id(x).property(k, v).iterate()` updates property
- [ ] `g.v_id(x).drop().iterate()` deletes vertex
- [ ] `g.e_id(x).drop().iterate()` deletes edge
- [ ] `g.v().has(...).drop().iterate()` bulk delete

### Edge Cases
- [ ] Adding edge to non-existent vertex fails gracefully
- [ ] Dropping already-deleted element is idempotent or errors
- [ ] Empty traversal endpoint for addE() returns error
- [ ] Property with null value removes property (or errors)

---

## Dependencies

- Existing storage layer (`src/storage/`)
- Existing traversal engine (`src/traversal/`)
- `thiserror` for error types
- `parking_lot` for `RwLock` (thread safety)

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Thread safety issues | High | Use `RwLock`, write concurrent tests |
| Mmap file growth complexity | Medium | Start with simple append-only, optimize later |
| Breaking existing read-only API | Medium | Ensure backward compatibility, feature flag if needed |
| Performance regression | Low | Benchmark before/after, optimize hot paths |

---

## Success Criteria

1. All mutation steps (`addV`, `addE`, `property`, `drop`) are implemented
2. Both storage backends (Graph, MmapGraph) support mutations
3. All tests pass with >90% branch coverage on new code
4. API is ergonomic and consistent with existing traversal API
5. Documentation is complete with examples
6. `Gremlin_api.md` updated to show mutations as implemented

---

## Future Work (Out of Scope)

- `mergeV()` / `mergeE()` upsert operations
- Property cardinality (single, list, set)
- Batch mutation optimization
- Transaction support with rollback
- Mutation event hooks
