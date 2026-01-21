# Interstellar: Optimistic Concurrency Control (OCC)

This document specifies Optimistic Concurrency Control for Interstellar, enabling concurrent readers to compute mutations offline and commit them atomically with conflict detection.

---

## 1. Overview and Motivation

### 1.1 Current State

Interstellar's COW architecture provides:
- O(1) snapshot creation via structural sharing
- Lock-free reads on immutable snapshots
- Single-writer serialization via `RwLock`

```
+------------------------------------------------------------------+
|              Current: Single-Writer Model                         |
+------------------------------------------------------------------+
|                                                                   |
|   Reader 1 ---> snapshot() ---> Compute offline (no lock)         |
|   Reader 2 ---> snapshot() ---> Compute offline (no lock)         |
|                                                                   |
|   Writer ----> state.write() ---> Exclusive access                |
|                (blocks other writers)                             |
|                                                                   |
|   Problem:                                                        |
|   - Readers cannot safely commit mutations                        |
|   - No way to detect if graph changed since snapshot              |
|   - Must hold write lock for entire computation                   |
|                                                                   |
+------------------------------------------------------------------+
```

### 1.2 Proposed Solution

Optimistic Concurrency Control allows readers to:
1. Take a snapshot and note its version
2. Compute mutations offline (no lock held)
3. Attempt atomic commit with version check
4. Retry if version changed

```
+------------------------------------------------------------------+
|              Proposed: Optimistic Concurrency Control             |
+------------------------------------------------------------------+
|                                                                   |
|   T=0: Reader takes snapshot at version N                         |
|        let snap = graph.snapshot();  // version = 5               |
|        let v = snap.version();                                    |
|                                                                   |
|   T=1: Reader computes mutations offline (no lock held)           |
|        let mutations = analyze_and_plan(&snap);                   |
|                                                                   |
|   T=2: Reader attempts commit with version check                  |
|        match graph.try_commit(v, mutations) {                     |
|            Ok(new_version) => { /* success, now at version 6 */ } |
|            Err(CommitError::VersionMismatch { .. }) => {          |
|                // Graph changed - retry from step T=0             |
|            }                                                      |
|        }                                                          |
|                                                                   |
|   Benefits:                                                       |
|   - No lock held during computation                               |
|   - Conflict detection at commit time                             |
|   - Automatic retry pattern for conflicts                         |
|   - Serializable commit ordering                                  |
|                                                                   |
+------------------------------------------------------------------+
```

### 1.3 Design Goals

| Goal | Description |
|------|-------------|
| Lock-free computation | Mutations computed without holding locks |
| Conflict detection | Detect if graph changed since snapshot |
| Atomic commit | All mutations succeed or none applied |
| Simple retry pattern | Clear API for handling conflicts |
| Backward compatible | Existing mutation API unchanged |
| Minimal overhead | Version check is O(1) |

### 1.4 Non-Goals

| Non-Goal | Rationale |
|----------|-----------|
| Automatic merge | Conflicts require manual retry; no three-way merge |
| Fine-grained conflict detection | Version-level only; no per-element tracking |
| Multi-statement transactions | OCC is single-commit; use `batch()` for multi-op |
| Distributed consensus | Single-node only |

---

## 2. Error Types

### 2.1 CommitError

Add a new error type for OCC commit failures:

```rust
// src/error.rs

/// Errors that can occur during optimistic commit operations.
///
/// `CommitError` represents failures when attempting to commit mutations
/// using optimistic concurrency control.
///
/// # Variants
///
/// | Variant | Cause | Recovery |
/// |---------|-------|----------|
/// | [`VersionMismatch`](Self::VersionMismatch) | Graph changed since snapshot | Retry with fresh snapshot |
/// | [`EmptyCommit`](Self::EmptyCommit) | No mutations to commit | Skip commit or add mutations |
/// | [`Mutation`](Self::Mutation) | Mutation validation failed | Fix mutation and retry |
/// | [`Storage`](Self::Storage) | Storage operation failed | See [`StorageError`] |
///
/// # Retry Pattern
///
/// The standard pattern for handling `VersionMismatch`:
///
/// ```rust
/// use interstellar::prelude::*;
/// use interstellar::error::CommitError;
///
/// fn update_with_retry(graph: &Graph, max_retries: u32) -> Result<(), CommitError> {
///     for attempt in 0..max_retries {
///         let snapshot = graph.snapshot();
///         let version = snapshot.version();
///         
///         // Compute mutations based on current state
///         let mutations = compute_mutations(&snapshot);
///         
///         match graph.try_commit(version, mutations) {
///             Ok(_) => return Ok(()),
///             Err(CommitError::VersionMismatch { .. }) => {
///                 // Graph changed, retry with fresh snapshot
///                 continue;
///             }
///             Err(e) => return Err(e),
///         }
///     }
///     
///     Err(CommitError::VersionMismatch {
///         expected: 0,
///         actual: 0,
///     })
/// }
/// ```
#[derive(Debug, thiserror::Error)]
pub enum CommitError {
    /// The graph version changed since the snapshot was taken.
    ///
    /// This indicates that another writer committed changes between
    /// when your snapshot was taken and when you attempted to commit.
    ///
    /// # Fields
    ///
    /// - `expected`: The version from your snapshot
    /// - `actual`: The current version of the graph
    ///
    /// # Recovery
    ///
    /// Take a new snapshot and recompute your mutations:
    ///
    /// ```ignore
    /// loop {
    ///     let snap = graph.snapshot();
    ///     let mutations = compute(&snap);
    ///     match graph.try_commit(snap.version(), mutations) {
    ///         Ok(_) => break,
    ///         Err(CommitError::VersionMismatch { .. }) => continue,
    ///         Err(e) => return Err(e),
    ///     }
    /// }
    /// ```
    #[error("version mismatch: expected {expected}, found {actual}")]
    VersionMismatch {
        /// The version the caller expected (from their snapshot)
        expected: u64,
        /// The actual current version of the graph
        actual: u64,
    },

    /// The commit contained no mutations.
    ///
    /// This is not necessarily an error, but indicates that `try_commit`
    /// was called with an empty mutation list.
    ///
    /// # Recovery
    ///
    /// Either skip the commit or ensure mutations are added.
    #[error("commit contained no mutations")]
    EmptyCommit,

    /// A mutation in the commit failed validation.
    ///
    /// This occurs when a mutation references invalid elements or
    /// violates schema constraints.
    #[error("mutation error: {0}")]
    Mutation(#[from] MutationError),

    /// A storage operation failed during commit.
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}
```

### 2.2 Error Conversion

Add conversion from `CommitError` to `TraversalError`:

```rust
// Add to TraversalError enum
#[derive(Debug, thiserror::Error)]
pub enum TraversalError {
    // ... existing variants ...
    
    /// An optimistic commit failed.
    #[error("commit error: {0}")]
    Commit(#[from] CommitError),
}
```

---

## 3. Mutation Representation

### 3.1 Using Existing PendingMutation

The existing `PendingMutation` enum in `traversal/mutation.rs` already represents mutations as data:

```rust
// src/traversal/mutation.rs (existing)

/// A mutation operation that can be applied to the graph.
#[derive(Debug, Clone)]
pub enum PendingMutation {
    /// Add a vertex with the given label and properties
    AddVertex {
        label: String,
        properties: HashMap<String, Value>,
    },
    
    /// Add an edge between two vertices
    AddEdge {
        src: VertexId,
        dst: VertexId,
        label: String,
        properties: HashMap<String, Value>,
    },
    
    /// Set a property on a vertex
    SetVertexProperty {
        id: VertexId,
        key: String,
        value: Value,
    },
    
    /// Set a property on an edge
    SetEdgeProperty {
        id: EdgeId,
        key: String,
        value: Value,
    },
    
    /// Remove a vertex (and its edges)
    RemoveVertex {
        id: VertexId,
    },
    
    /// Remove an edge
    RemoveEdge {
        id: EdgeId,
    },
    
    /// Remove a property from a vertex
    RemoveVertexProperty {
        id: VertexId,
        key: String,
    },
    
    /// Remove a property from an edge
    RemoveEdgeProperty {
        id: EdgeId,
        key: String,
    },
}
```

### 3.2 CommitResult

Define the result of a successful commit:

```rust
/// Result of a successful optimistic commit.
#[derive(Debug, Clone)]
pub struct CommitResult {
    /// The new version after commit
    pub version: u64,
    
    /// Number of vertices added
    pub vertices_added: usize,
    
    /// Number of edges added
    pub edges_added: usize,
    
    /// Number of vertices removed
    pub vertices_removed: usize,
    
    /// Number of edges removed
    pub edges_removed: usize,
    
    /// Number of properties set
    pub properties_set: usize,
    
    /// Number of properties removed
    pub properties_removed: usize,
    
    /// IDs of newly created vertices (in order of AddVertex mutations)
    pub new_vertex_ids: Vec<VertexId>,
    
    /// IDs of newly created edges (in order of AddEdge mutations)
    pub new_edge_ids: Vec<EdgeId>,
}
```

---

## 4. Core API

### 4.1 Graph::try_commit

The primary OCC method:

```rust
// src/storage/cow.rs

impl Graph {
    /// Attempt to commit mutations at a specific version.
    ///
    /// This is the core optimistic concurrency control method. It:
    /// 1. Acquires the write lock
    /// 2. Checks if current version matches expected version
    /// 3. If match: applies all mutations atomically
    /// 4. If mismatch: returns `VersionMismatch` error
    ///
    /// # Arguments
    ///
    /// * `expected_version` - The version from your snapshot (via `snapshot.version()`)
    /// * `mutations` - List of mutations to apply
    ///
    /// # Returns
    ///
    /// * `Ok(CommitResult)` - Commit succeeded, contains new version and IDs
    /// * `Err(CommitError::VersionMismatch)` - Graph changed, retry needed
    /// * `Err(CommitError::Mutation)` - A mutation was invalid
    /// * `Err(CommitError::Storage)` - Storage operation failed
    ///
    /// # Thread Safety
    ///
    /// This method acquires a write lock for the duration of the commit.
    /// The version check and mutation application are atomic.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::error::CommitError;
    /// use interstellar::traversal::mutation::PendingMutation;
    /// use std::collections::HashMap;
    ///
    /// let graph = Graph::new();
    ///
    /// // Take snapshot and note version
    /// let snapshot = graph.snapshot();
    /// let version = snapshot.version();
    ///
    /// // Compute mutations (no lock held)
    /// let mutations = vec![
    ///     PendingMutation::AddVertex {
    ///         label: "person".to_string(),
    ///         properties: HashMap::from([
    ///             ("name".to_string(), Value::from("Alice")),
    ///         ]),
    ///     },
    /// ];
    ///
    /// // Attempt commit
    /// match graph.try_commit(version, mutations) {
    ///     Ok(result) => {
    ///         println!("Committed at version {}", result.version);
    ///         println!("Created vertex: {:?}", result.new_vertex_ids[0]);
    ///     }
    ///     Err(CommitError::VersionMismatch { expected, actual }) => {
    ///         println!("Conflict: expected v{}, found v{}", expected, actual);
    ///         // Retry with fresh snapshot
    ///     }
    ///     Err(e) => println!("Error: {}", e),
    /// }
    /// ```
    pub fn try_commit(
        &self,
        expected_version: u64,
        mutations: Vec<PendingMutation>,
    ) -> Result<CommitResult, CommitError> {
        // Empty commit check
        if mutations.is_empty() {
            return Err(CommitError::EmptyCommit);
        }
        
        // Acquire write lock
        let mut state = self.state.write();
        
        // Version check (the "optimistic" part)
        if state.version != expected_version {
            return Err(CommitError::VersionMismatch {
                expected: expected_version,
                actual: state.version,
            });
        }
        
        // Apply mutations atomically
        let result = apply_mutations(&mut state, mutations, self.schema.read().as_ref())?;
        
        // Increment version
        state.version += 1;
        
        Ok(CommitResult {
            version: state.version,
            ..result
        })
    }
}
```

### 4.2 Graph::try_commit_with_validator

Extended version with custom validation:

```rust
impl Graph {
    /// Attempt to commit with a custom validator function.
    ///
    /// The validator is called after the version check but before mutations
    /// are applied. It receives the current state and can perform additional
    /// validation (e.g., checking preconditions).
    ///
    /// # Arguments
    ///
    /// * `expected_version` - The version from your snapshot
    /// * `mutations` - List of mutations to apply
    /// * `validator` - Function to validate current state before commit
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    ///
    /// // Only commit if vertex count is below threshold
    /// graph.try_commit_with_validator(version, mutations, |state| {
    ///     if state.vertex_count() >= 1000 {
    ///         Err(CommitError::Mutation(MutationError::Storage(
    ///             StorageError::OutOfSpace
    ///         )))
    ///     } else {
    ///         Ok(())
    ///     }
    /// })?;
    /// ```
    pub fn try_commit_with_validator<F>(
        &self,
        expected_version: u64,
        mutations: Vec<PendingMutation>,
        validator: F,
    ) -> Result<CommitResult, CommitError>
    where
        F: FnOnce(&GraphState) -> Result<(), CommitError>,
    {
        if mutations.is_empty() {
            return Err(CommitError::EmptyCommit);
        }
        
        let mut state = self.state.write();
        
        // Version check
        if state.version != expected_version {
            return Err(CommitError::VersionMismatch {
                expected: expected_version,
                actual: state.version,
            });
        }
        
        // Custom validation
        validator(&state)?;
        
        // Apply mutations
        let result = apply_mutations(&mut state, mutations, self.schema.read().as_ref())?;
        
        state.version += 1;
        
        Ok(CommitResult {
            version: state.version,
            ..result
        })
    }
}
```

### 4.3 Graph::commit_or_retry

Convenience method with automatic retry:

```rust
impl Graph {
    /// Commit mutations with automatic retry on version mismatch.
    ///
    /// This method repeatedly:
    /// 1. Takes a snapshot
    /// 2. Calls your function to compute mutations
    /// 3. Attempts to commit
    /// 4. Retries on version mismatch (up to max_retries)
    ///
    /// # Arguments
    ///
    /// * `max_retries` - Maximum number of retry attempts (0 = no retries)
    /// * `compute_mutations` - Function that computes mutations from a snapshot
    ///
    /// # Returns
    ///
    /// * `Ok(CommitResult)` - Commit succeeded
    /// * `Err(CommitError::VersionMismatch)` - Max retries exceeded
    /// * `Err(CommitError::*)` - Other error occurred
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    ///
    /// // Automatically retry up to 3 times
    /// let result = graph.commit_or_retry(3, |snapshot| {
    ///     // Compute mutations based on current state
    ///     let count = snapshot.vertex_count();
    ///     vec![
    ///         PendingMutation::AddVertex {
    ///             label: "node".to_string(),
    ///             properties: HashMap::from([
    ///                 ("index".to_string(), Value::from(count as i64)),
    ///             ]),
    ///         },
    ///     ]
    /// })?;
    ///
    /// println!("Committed after retries, new version: {}", result.version);
    /// ```
    pub fn commit_or_retry<F>(
        &self,
        max_retries: u32,
        mut compute_mutations: F,
    ) -> Result<CommitResult, CommitError>
    where
        F: FnMut(&GraphSnapshot) -> Vec<PendingMutation>,
    {
        let mut last_error = None;
        
        for _ in 0..=max_retries {
            let snapshot = self.snapshot();
            let version = snapshot.version();
            let mutations = compute_mutations(&snapshot);
            
            match self.try_commit(version, mutations) {
                Ok(result) => return Ok(result),
                Err(CommitError::VersionMismatch { expected, actual }) => {
                    last_error = Some(CommitError::VersionMismatch { expected, actual });
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        
        Err(last_error.unwrap_or(CommitError::VersionMismatch {
            expected: 0,
            actual: 0,
        }))
    }
}
```

---

## 5. Mutation Application

### 5.1 apply_mutations Function

Internal function to apply a list of mutations atomically:

```rust
// src/storage/cow.rs (internal)

/// Apply a list of mutations to the graph state.
///
/// This function is all-or-nothing: if any mutation fails validation,
/// no changes are applied (due to COW semantics).
fn apply_mutations(
    state: &mut GraphState,
    mutations: Vec<PendingMutation>,
    schema: Option<&GraphSchema>,
) -> Result<CommitResult, CommitError> {
    let mut result = CommitResult {
        version: 0, // Will be set by caller
        vertices_added: 0,
        edges_added: 0,
        vertices_removed: 0,
        edges_removed: 0,
        properties_set: 0,
        properties_removed: 0,
        new_vertex_ids: Vec::new(),
        new_edge_ids: Vec::new(),
    };
    
    // Phase 1: Validate all mutations
    for mutation in &mutations {
        validate_mutation(state, mutation, schema)?;
    }
    
    // Phase 2: Apply all mutations
    // Because we validated first, these should not fail
    for mutation in mutations {
        match mutation {
            PendingMutation::AddVertex { label, properties } => {
                let id = state.allocate_vertex_id();
                let label_id = state.interner.write().intern(&label);
                
                let node = Arc::new(NodeData {
                    id,
                    label_id,
                    properties,
                    out_edges: Vec::new(),
                    in_edges: Vec::new(),
                });
                
                state.vertices = state.vertices.update(id, node);
                update_vertex_label_index(state, label_id, id);
                
                result.vertices_added += 1;
                result.new_vertex_ids.push(id);
            }
            
            PendingMutation::AddEdge { src, dst, label, properties } => {
                let id = state.allocate_edge_id();
                let label_id = state.interner.write().intern(&label);
                
                let edge = Arc::new(EdgeData {
                    id,
                    label_id,
                    src,
                    dst,
                    properties,
                });
                
                state.edges = state.edges.update(id, edge);
                update_edge_label_index(state, label_id, id);
                
                // Update vertex adjacency lists
                update_vertex_out_edge(state, src, id);
                update_vertex_in_edge(state, dst, id);
                
                result.edges_added += 1;
                result.new_edge_ids.push(id);
            }
            
            PendingMutation::SetVertexProperty { id, key, value } => {
                let node = state.vertices.get(&id).unwrap();
                let mut new_node = (**node).clone();
                new_node.properties.insert(key, value);
                state.vertices = state.vertices.update(id, Arc::new(new_node));
                result.properties_set += 1;
            }
            
            PendingMutation::SetEdgeProperty { id, key, value } => {
                let edge = state.edges.get(&id).unwrap();
                let mut new_edge = (**edge).clone();
                new_edge.properties.insert(key, value);
                state.edges = state.edges.update(id, Arc::new(new_edge));
                result.properties_set += 1;
            }
            
            PendingMutation::RemoveVertex { id } => {
                // Remove all incident edges first
                if let Some(node) = state.vertices.get(&id) {
                    let out_edges = node.out_edges.clone();
                    let in_edges = node.in_edges.clone();
                    
                    for edge_id in out_edges.iter().chain(in_edges.iter()) {
                        remove_edge_internal(state, *edge_id);
                        result.edges_removed += 1;
                    }
                }
                
                // Remove vertex
                let node = state.vertices.get(&id).unwrap();
                remove_from_vertex_label_index(state, node.label_id, id);
                state.vertices = state.vertices.without(&id);
                result.vertices_removed += 1;
            }
            
            PendingMutation::RemoveEdge { id } => {
                remove_edge_internal(state, id);
                result.edges_removed += 1;
            }
            
            PendingMutation::RemoveVertexProperty { id, key } => {
                let node = state.vertices.get(&id).unwrap();
                let mut new_node = (**node).clone();
                new_node.properties.remove(&key);
                state.vertices = state.vertices.update(id, Arc::new(new_node));
                result.properties_removed += 1;
            }
            
            PendingMutation::RemoveEdgeProperty { id, key } => {
                let edge = state.edges.get(&id).unwrap();
                let mut new_edge = (**edge).clone();
                new_edge.properties.remove(&key);
                state.edges = state.edges.update(id, Arc::new(new_edge));
                result.properties_removed += 1;
            }
        }
    }
    
    Ok(result)
}
```

### 5.2 Validation Function

```rust
/// Validate a single mutation before applying.
fn validate_mutation(
    state: &GraphState,
    mutation: &PendingMutation,
    schema: Option<&GraphSchema>,
) -> Result<(), CommitError> {
    match mutation {
        PendingMutation::AddVertex { label, properties } => {
            // Validate against schema if present
            if let Some(s) = schema {
                s.validate_vertex(label, properties)
                    .map_err(|e| CommitError::Mutation(MutationError::Storage(e)))?;
            }
            Ok(())
        }
        
        PendingMutation::AddEdge { src, dst, label, properties } => {
            // Check source vertex exists
            if !state.vertices.contains_key(src) {
                return Err(CommitError::Mutation(
                    MutationError::EdgeSourceNotFound(*src)
                ));
            }
            
            // Check destination vertex exists
            if !state.vertices.contains_key(dst) {
                return Err(CommitError::Mutation(
                    MutationError::EdgeTargetNotFound(*dst)
                ));
            }
            
            // Validate against schema if present
            if let Some(s) = schema {
                s.validate_edge(label, *src, *dst, properties)
                    .map_err(|e| CommitError::Mutation(MutationError::Storage(e)))?;
            }
            
            Ok(())
        }
        
        PendingMutation::SetVertexProperty { id, key, value } => {
            if !state.vertices.contains_key(id) {
                return Err(CommitError::Storage(StorageError::VertexNotFound(*id)));
            }
            
            // Schema validation for property type
            if let Some(s) = schema {
                let node = state.vertices.get(id).unwrap();
                let label = state.interner.read().resolve(node.label_id)
                    .unwrap_or_default().to_string();
                s.validate_property(&label, key, value)
                    .map_err(|e| CommitError::Mutation(MutationError::Storage(e)))?;
            }
            
            Ok(())
        }
        
        PendingMutation::SetEdgeProperty { id, key, value } => {
            if !state.edges.contains_key(id) {
                return Err(CommitError::Storage(StorageError::EdgeNotFound(*id)));
            }
            
            // Schema validation could be added here
            let _ = (key, value); // Suppress unused warning
            
            Ok(())
        }
        
        PendingMutation::RemoveVertex { id } => {
            if !state.vertices.contains_key(id) {
                return Err(CommitError::Storage(StorageError::VertexNotFound(*id)));
            }
            Ok(())
        }
        
        PendingMutation::RemoveEdge { id } => {
            if !state.edges.contains_key(id) {
                return Err(CommitError::Storage(StorageError::EdgeNotFound(*id)));
            }
            Ok(())
        }
        
        PendingMutation::RemoveVertexProperty { id, .. } => {
            if !state.vertices.contains_key(id) {
                return Err(CommitError::Storage(StorageError::VertexNotFound(*id)));
            }
            Ok(())
        }
        
        PendingMutation::RemoveEdgeProperty { id, .. } => {
            if !state.edges.contains_key(id) {
                return Err(CommitError::Storage(StorageError::EdgeNotFound(*id)));
            }
            Ok(())
        }
    }
}
```

---

## 6. CowMmapGraph Integration

### 6.1 Persistent OCC

The `CowMmapGraph` should also support OCC with the same API:

```rust
// src/storage/cow_mmap.rs

impl CowMmapGraph {
    /// Attempt to commit mutations at a specific version.
    ///
    /// Same semantics as `Graph::try_commit`, but also persists
    /// mutations to the underlying mmap storage.
    pub fn try_commit(
        &self,
        expected_version: u64,
        mutations: Vec<PendingMutation>,
    ) -> Result<CommitResult, CommitError> {
        if mutations.is_empty() {
            return Err(CommitError::EmptyCommit);
        }
        
        let mut state = self.state.write();
        
        // Version check
        if state.version != expected_version {
            return Err(CommitError::VersionMismatch {
                expected: expected_version,
                actual: state.version,
            });
        }
        
        // Apply to COW state (validates and applies)
        let result = apply_mutations(&mut state, mutations.clone(), self.schema.read().as_ref())?;
        
        // Persist to mmap storage
        // This must happen within the write lock to ensure consistency
        self.persist_mutations(&mutations)?;
        
        state.version += 1;
        
        Ok(CommitResult {
            version: state.version,
            ..result
        })
    }
    
    /// Persist mutations to the underlying mmap storage.
    fn persist_mutations(&self, mutations: &[PendingMutation]) -> Result<(), CommitError> {
        // Apply each mutation to the mmap layer
        for mutation in mutations {
            match mutation {
                PendingMutation::AddVertex { label, properties } => {
                    self.mmap.add_vertex(label, properties.clone())
                        .map_err(CommitError::Storage)?;
                }
                // ... other mutation types
                _ => {
                    // Implement remaining mutation types
                }
            }
        }
        Ok(())
    }
}
```

---

## 7. Usage Patterns

### 7.1 Basic OCC Pattern

```rust
use interstellar::prelude::*;
use interstellar::error::CommitError;
use interstellar::traversal::mutation::PendingMutation;

fn add_person_optimistic(graph: &Graph, name: &str) -> Result<VertexId, CommitError> {
    loop {
        // 1. Take snapshot and note version
        let snapshot = graph.snapshot();
        let version = snapshot.version();
        
        // 2. Check preconditions (no lock held)
        let exists = snapshot.gremlin()
            .v()
            .has_label("person")
            .has_value("name", name)
            .has_next();
        
        if exists {
            // Person already exists - could return existing ID or error
            return Err(CommitError::Mutation(
                MutationError::Storage(StorageError::IndexError(
                    format!("person '{}' already exists", name)
                ))
            ));
        }
        
        // 3. Prepare mutations
        let mutations = vec![
            PendingMutation::AddVertex {
                label: "person".to_string(),
                properties: HashMap::from([
                    ("name".to_string(), Value::from(name)),
                ]),
            },
        ];
        
        // 4. Attempt commit
        match graph.try_commit(version, mutations) {
            Ok(result) => return Ok(result.new_vertex_ids[0]),
            Err(CommitError::VersionMismatch { .. }) => {
                // Another writer committed - retry
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}
```

### 7.2 Batch Mutation Pattern

```rust
/// Add multiple related entities atomically with OCC
fn add_team_optimistic(
    graph: &Graph,
    team_name: &str,
    members: &[&str],
) -> Result<CommitResult, CommitError> {
    graph.commit_or_retry(5, |snapshot| {
        let mut mutations = Vec::new();
        
        // Add team vertex
        mutations.push(PendingMutation::AddVertex {
            label: "team".to_string(),
            properties: HashMap::from([
                ("name".to_string(), Value::from(team_name)),
            ]),
        });
        
        // Find existing members or prepare to create them
        let g = snapshot.gremlin();
        
        for (i, member_name) in members.iter().enumerate() {
            // Check if member exists
            let existing = g.v()
                .has_label("person")
                .has_value("name", *member_name)
                .next();
            
            match existing {
                Some(Value::Vertex(v)) => {
                    // Member exists - we'll add edge after commit
                    // (edges to new vertices use placeholder IDs)
                }
                _ => {
                    // Create new member
                    mutations.push(PendingMutation::AddVertex {
                        label: "person".to_string(),
                        properties: HashMap::from([
                            ("name".to_string(), Value::from(*member_name)),
                        ]),
                    });
                }
            }
        }
        
        mutations
    })
}
```

### 7.3 Compare-and-Swap Pattern

```rust
/// Atomically update a counter using OCC
fn increment_counter(
    graph: &Graph,
    vertex_id: VertexId,
    property: &str,
) -> Result<i64, CommitError> {
    graph.commit_or_retry(10, |snapshot| {
        // Read current value
        let current = snapshot.gremlin()
            .v_id(vertex_id)
            .values(property)
            .next()
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        
        let new_value = current + 1;
        
        vec![
            PendingMutation::SetVertexProperty {
                id: vertex_id,
                key: property.to_string(),
                value: Value::from(new_value),
            },
        ]
    }).map(|_| {
        // Return the new value (we know it was incremented)
        // For precise value, re-read after commit
        0 // Placeholder
    })
}
```

### 7.4 Conditional Mutation Pattern

```rust
/// Only add edge if relationship doesn't exist
fn add_friendship_if_not_exists(
    graph: &Graph,
    person_a: VertexId,
    person_b: VertexId,
) -> Result<Option<EdgeId>, CommitError> {
    loop {
        let snapshot = graph.snapshot();
        let version = snapshot.version();
        
        // Check if friendship already exists
        let exists = snapshot.gremlin()
            .v_id(person_a)
            .out_labels(&["knows"])
            .has_id(person_b)
            .has_next();
        
        if exists {
            return Ok(None); // Already friends
        }
        
        let mutations = vec![
            PendingMutation::AddEdge {
                src: person_a,
                dst: person_b,
                label: "knows".to_string(),
                properties: HashMap::from([
                    ("since".to_string(), Value::from("2024-01-01")),
                ]),
            },
        ];
        
        match graph.try_commit(version, mutations) {
            Ok(result) => return Ok(Some(result.new_edge_ids[0])),
            Err(CommitError::VersionMismatch { .. }) => continue,
            Err(e) => return Err(e),
        }
    }
}
```

---

## 8. Thread Safety and Concurrency

### 8.1 Concurrency Guarantees

```
+------------------------------------------------------------------+
|                    OCC Concurrency Model                          |
+------------------------------------------------------------------+
|                                                                   |
|   Thread A                    Thread B                            |
|   --------                    --------                            |
|   snap_a = snapshot()         snap_b = snapshot()                 |
|   v_a = 5                     v_b = 5                             |
|   |                           |                                   |
|   | (compute)                 | (compute)                         |
|   |                           |                                   |
|   try_commit(5, [...])        try_commit(5, [...])                |
|   |                           |                                   |
|   +-- LOCK --+                +-- WAIT --+                        |
|   | check: 5 == 5 OK |        |          |                        |
|   | apply mutations  |        |          |                        |
|   | version = 6      |        |          |                        |
|   +-- UNLOCK --+              +-- LOCK --+                        |
|   |                           | check: 5 != 6 FAIL |              |
|   v                           +-- UNLOCK --+                      |
|   Ok(v6)                      |                                   |
|                               v                                   |
|                               Err(VersionMismatch)                |
|                               |                                   |
|                               v (retry)                           |
|                               snap_b' = snapshot()                |
|                               v_b' = 6                            |
|                               try_commit(6, [...])                |
|                               Ok(v7)                              |
|                                                                   |
+------------------------------------------------------------------+
```

### 8.2 Fairness and Starvation

OCC does not guarantee fairness. Under high contention:
- Fast writers may repeatedly win
- Slow writers may starve

Mitigation strategies:
1. Exponential backoff between retries
2. Maximum retry limits
3. Priority queuing (not implemented in this spec)

```rust
/// OCC with exponential backoff
pub fn commit_with_backoff<F>(
    graph: &Graph,
    max_retries: u32,
    mut compute: F,
) -> Result<CommitResult, CommitError>
where
    F: FnMut(&GraphSnapshot) -> Vec<PendingMutation>,
{
    use std::thread;
    use std::time::Duration;
    
    let mut backoff_ms = 1;
    
    for attempt in 0..=max_retries {
        let snapshot = graph.snapshot();
        let mutations = compute(&snapshot);
        
        match graph.try_commit(snapshot.version(), mutations) {
            Ok(result) => return Ok(result),
            Err(CommitError::VersionMismatch { .. }) if attempt < max_retries => {
                // Exponential backoff with jitter
                let jitter = rand::random::<u64>() % backoff_ms;
                thread::sleep(Duration::from_millis(backoff_ms + jitter));
                backoff_ms = (backoff_ms * 2).min(1000); // Cap at 1 second
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    
    Err(CommitError::VersionMismatch { expected: 0, actual: 0 })
}
```

---

## 9. Integration with Existing APIs

### 9.1 Relationship to batch()

The existing `batch()` API remains for cases where you don't need OCC:

| Use Case | API |
|----------|-----|
| Simple atomic mutations (no contention) | `graph.batch(\|ctx\| { ... })` |
| Mutations with conflict detection | `graph.try_commit(version, mutations)` |
| Auto-retry on conflict | `graph.commit_or_retry(n, \|snap\| { ... })` |

### 9.2 Relationship to Direct Mutations

Direct mutations (`add_vertex`, `add_edge`, etc.) remain unchanged:

```rust
// Direct mutation (always succeeds, no conflict detection)
let id = graph.add_vertex("person", props);

// OCC mutation (may fail with VersionMismatch)
let result = graph.try_commit(version, vec![
    PendingMutation::AddVertex { label: "person".into(), properties: props }
])?;
```

### 9.3 GQL Integration

OCC can be exposed via GQL using a version parameter:

```rust
// Potential future GQL syntax (not in this spec)
graph.execute_at_version(
    version,
    "CREATE (n:Person {name: $name}) RETURN n",
    params
)?;
```

---

## 10. Performance Considerations

### 10.1 Version Check Overhead

The version check is O(1):
- Single u64 comparison
- Already holding write lock

### 10.2 Mutation Validation

Validation is O(m) where m = number of mutations:
- Each mutation validated before any applied
- Hash lookups for vertex/edge existence

### 10.3 Structural Sharing

Mutations benefit from COW:
- Only modified paths are copied
- O(log n) per mutation, not O(n)

### 10.4 Contention Impact

Under high contention:
- More retries = more wasted computation
- Consider batching mutations to reduce commit frequency
- Consider sharding for independent subgraphs

---

## 11. Testing Strategy

### 11.1 Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_try_commit_success() {
        let graph = Graph::new();
        let v = graph.version();
        
        let mutations = vec![
            PendingMutation::AddVertex {
                label: "person".into(),
                properties: HashMap::new(),
            },
        ];
        
        let result = graph.try_commit(v, mutations).unwrap();
        assert_eq!(result.vertices_added, 1);
        assert_eq!(result.version, v + 1);
        assert_eq!(result.new_vertex_ids.len(), 1);
    }
    
    #[test]
    fn test_try_commit_version_mismatch() {
        let graph = Graph::new();
        let v = graph.version();
        
        // Modify graph to change version
        graph.add_vertex("other", HashMap::new());
        
        let mutations = vec![
            PendingMutation::AddVertex {
                label: "person".into(),
                properties: HashMap::new(),
            },
        ];
        
        let err = graph.try_commit(v, mutations).unwrap_err();
        assert!(matches!(err, CommitError::VersionMismatch { expected, actual }
            if expected == v && actual == v + 1));
    }
    
    #[test]
    fn test_try_commit_empty() {
        let graph = Graph::new();
        let err = graph.try_commit(0, vec![]).unwrap_err();
        assert!(matches!(err, CommitError::EmptyCommit));
    }
    
    #[test]
    fn test_try_commit_invalid_edge() {
        let graph = Graph::new();
        let v = graph.version();
        
        let mutations = vec![
            PendingMutation::AddEdge {
                src: VertexId(999),
                dst: VertexId(888),
                label: "knows".into(),
                properties: HashMap::new(),
            },
        ];
        
        let err = graph.try_commit(v, mutations).unwrap_err();
        assert!(matches!(err, CommitError::Mutation(MutationError::EdgeSourceNotFound(_))));
    }
    
    #[test]
    fn test_commit_or_retry_success() {
        let graph = Graph::new();
        
        let result = graph.commit_or_retry(3, |_| {
            vec![
                PendingMutation::AddVertex {
                    label: "test".into(),
                    properties: HashMap::new(),
                },
            ]
        }).unwrap();
        
        assert_eq!(result.vertices_added, 1);
    }
}
```

### 11.2 Concurrency Tests

```rust
#[test]
fn test_concurrent_commits() {
    use std::sync::Arc;
    use std::thread;
    
    let graph = Arc::new(Graph::new());
    let num_threads = 10;
    let mutations_per_thread = 100;
    
    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let g = Arc::clone(&graph);
            thread::spawn(move || {
                let mut successes = 0;
                let mut retries = 0;
                
                for i in 0..mutations_per_thread {
                    loop {
                        let snap = g.snapshot();
                        let mutations = vec![
                            PendingMutation::AddVertex {
                                label: format!("node_t{}_i{}", t, i),
                                properties: HashMap::new(),
                            },
                        ];
                        
                        match g.try_commit(snap.version(), mutations) {
                            Ok(_) => {
                                successes += 1;
                                break;
                            }
                            Err(CommitError::VersionMismatch { .. }) => {
                                retries += 1;
                                continue;
                            }
                            Err(e) => panic!("Unexpected error: {:?}", e),
                        }
                    }
                }
                
                (successes, retries)
            })
        })
        .collect();
    
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    
    let total_successes: usize = results.iter().map(|(s, _)| s).sum();
    let total_retries: usize = results.iter().map(|(_, r)| r).sum();
    
    assert_eq!(total_successes, num_threads * mutations_per_thread);
    assert_eq!(graph.snapshot().vertex_count(), (num_threads * mutations_per_thread) as u64);
    
    println!("Total retries due to contention: {}", total_retries);
}
```

### 11.3 Property-Based Tests

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn prop_version_always_increases(ops in prop::collection::vec(any::<bool>(), 1..100)) {
        let graph = Graph::new();
        let mut last_version = graph.version();
        
        for should_commit in ops {
            if should_commit {
                let v = graph.version();
                let mutations = vec![
                    PendingMutation::AddVertex {
                        label: "test".into(),
                        properties: HashMap::new(),
                    },
                ];
                
                if let Ok(result) = graph.try_commit(v, mutations) {
                    prop_assert!(result.version > last_version);
                    last_version = result.version;
                }
            }
        }
    }
}
```

---

## 12. Implementation Phases

### Phase 1: Core OCC (This Spec)

1. Add `CommitError` to `src/error.rs`
2. Add `CommitResult` struct
3. Implement `Graph::try_commit()`
4. Implement `apply_mutations()` and `validate_mutation()`
5. Add unit tests

### Phase 2: Convenience APIs

1. Implement `Graph::try_commit_with_validator()`
2. Implement `Graph::commit_or_retry()`
3. Add retry with backoff utility

### Phase 3: CowMmapGraph Integration

1. Implement `CowMmapGraph::try_commit()`
2. Ensure persistence consistency
3. Add integration tests

### Phase 4: Documentation and Examples

1. Update README with OCC patterns
2. Add examples/ showcasing OCC usage
3. Document performance characteristics

---

## 13. Future Extensions

### 13.1 Fine-Grained Conflict Detection

Track which elements were modified between versions:

```rust
pub struct ConflictInfo {
    pub modified_vertices: HashSet<VertexId>,
    pub modified_edges: HashSet<EdgeId>,
    pub added_vertices: usize,
    pub removed_vertices: usize,
}

// Could return more detailed conflict info
Err(CommitError::Conflict {
    expected: 5,
    actual: 6,
    info: ConflictInfo { ... },
})
```

### 13.2 Partial Retry

Only recompute mutations that conflict:

```rust
// Potential future API
graph.try_commit_partial(version, mutations, |conflicts| {
    // Recompute only conflicting mutations
    recompute_for_conflicts(conflicts)
})
```

### 13.3 Read-Your-Writes

Allow reading uncommitted changes during computation:

```rust
// Potential future API
let tx = graph.begin_transaction();
tx.add_vertex("person", props);
let id = tx.last_vertex_id();
tx.add_edge(id, other_id, "knows", props);
tx.commit()?;
```

This would require a full transaction API (out of scope for this spec).
