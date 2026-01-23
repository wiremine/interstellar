# Interstellar: Copy-on-Write Snapshots

This document specifies the Copy-on-Write (COW) snapshot implementation for Interstellar using persistent data structures, enabling lock-free reads with consistent snapshots.

---

## 1. Overview and Motivation

### 1.1 Current State

Interstellar Phase 1 uses `parking_lot::RwLock` for concurrency control:

```
┌─────────────────────────────────────────────────────────────────┐
│              Current: RwLock Concurrency                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   Reader 1 ──┐                                                  │
│   Reader 2 ──┼──▶ RwLock::read()  ──▶ Shared Access             │
│   Reader 3 ──┘                                                  │
│                                                                 │
│   Writer   ────▶ RwLock::write() ──▶ Exclusive Access           │
│                  (blocks all readers)                           │
│                                                                 │
│   Problems:                                                     │
│   • Writers block all readers                                   │
│   • Long traversals block writes                                │
│   • Snapshot lifetime tied to lock guard                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 1.2 Proposed Solution

Copy-on-Write snapshots using the `im` crate's persistent data structures:

```
┌─────────────────────────────────────────────────────────────────┐
│              Proposed: Copy-on-Write Snapshots                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   Snapshot 1 ──▶ Arc<State v1> ──▶ Reads frozen state           │
│   Snapshot 2 ──▶ Arc<State v2> ──▶ Reads different frozen state │
│                                                                 │
│   Writer ──▶ Clone-on-write ──▶ Creates new state version       │
│              (O(log n) structural sharing)                      │
│                                                                 │
│   Benefits:                                                     │
│   • Readers never block writers                                 │
│   • Writers never block readers                                 │
│   • Snapshots are owned, not borrowed                           │
│   • Automatic cleanup via Arc refcounting                       │
│   • No garbage collection needed                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 1.3 Design Goals

| Goal | Description |
|------|-------------|
| Lock-free reads | Snapshots don't hold locks |
| Owned snapshots | Snapshots can outlive the Graph reference |
| Structural sharing | Minimize memory copying via persistent structures |
| Backward compatible | Existing traversal API unchanged |
| Simple implementation | No manual garbage collection |
| Incremental adoption | Can coexist with current RwLock model |
| **Unified API** | Single entry point for reads AND mutations - users don't choose |
| **Statement-level atomicity** | Each GQL statement or Rhai script executes atomically |
| **Rhai mutation support** | Rhai scripts can execute mutations directly (not just pending markers) |

### 1.4 Non-Goals

| Non-Goal | Rationale |
|----------|-----------|
| Concurrent writers | Single-writer model retained; writes are serialized via RwLock |
| Multi-statement transactions | Statement-level atomicity only; no BEGIN/COMMIT/ROLLBACK |
| Historical queries | Only current + active snapshot states retained |
| Serializable isolation | Snapshot isolation only |
| Fine-grained rollback | Entire statement succeeds or fails; no savepoints |

---

## 2. Dependencies

### 2.1 The `im` Crate

Add the `im` crate for persistent/immutable data structures:

```toml
[dependencies]
im = "15.1"
```

The `im` crate provides:

- `im::HashMap<K, V>` - Persistent hash map with O(log₃₂ n) operations
- `im::Vector<T>` - Persistent vector with O(log₃₂ n) operations
- Structural sharing - Clones share unmodified subtrees
- `Clone` is O(1) - Just increments reference counts

### 2.2 Performance Characteristics

| Operation | `std::HashMap` | `im::HashMap` |
|-----------|----------------|---------------|
| Get | O(1) | O(log₃₂ n) ≈ O(1) |
| Insert | O(1) amortized | O(log₃₂ n) |
| Clone | O(n) | O(1) |
| Memory | Contiguous | Tree nodes |

For a graph with 1 million vertices:
- `log₃₂(1,000,000)` ≈ 4 operations per lookup
- Clone is instant regardless of size

---

## 3. Core Data Structures

### 3.1 CowGraphState

The immutable, shareable graph state:

```rust
use im::HashMap as ImHashMap;
use std::sync::Arc;

/// Immutable graph state that can be shared between snapshots
#[derive(Clone)]
pub struct CowGraphState {
    /// Vertex data: VertexId → NodeData
    /// Using Arc<NodeData> for cheap cloning of individual nodes
    pub(crate) vertices: ImHashMap<VertexId, Arc<NodeData>>,
    
    /// Edge data: EdgeId → EdgeData
    pub(crate) edges: ImHashMap<EdgeId, Arc<EdgeData>>,
    
    /// Label index: label_id → set of vertex IDs
    pub(crate) vertex_labels: ImHashMap<u32, Arc<RoaringBitmap>>,
    
    /// Label index: label_id → set of edge IDs
    pub(crate) edge_labels: ImHashMap<u32, Arc<RoaringBitmap>>,
    
    /// String interner (append-only, always shared)
    pub(crate) interner: Arc<StringInterner>,
    
    /// Monotonic version counter
    pub(crate) version: u64,
    
    /// Next vertex ID
    pub(crate) next_vertex_id: u64,
    
    /// Next edge ID
    pub(crate) next_edge_id: u64,
}
```

### 3.2 NodeData and EdgeData

Internal representations remain largely unchanged but wrapped in `Arc`:

```rust
/// Internal vertex representation (unchanged from current)
#[derive(Clone, Debug)]
pub(crate) struct NodeData {
    pub id: VertexId,
    pub label_id: u32,
    pub properties: HashMap<String, Value>,
    pub out_edges: Vec<EdgeId>,
    pub in_edges: Vec<EdgeId>,
}

/// Internal edge representation (unchanged from current)
#[derive(Clone, Debug)]
pub(crate) struct EdgeData {
    pub id: EdgeId,
    pub label_id: u32,
    pub src: VertexId,
    pub dst: VertexId,
    pub properties: HashMap<String, Value>,
}
```

### 3.3 CowGraph

The mutable graph container:

```rust
use parking_lot::RwLock;

/// Copy-on-Write graph with snapshot support
pub struct CowGraph {
    /// Current mutable state (protected by RwLock for thread safety)
    state: RwLock<CowGraphState>,
    
    /// Schema for validation (optional)
    schema: RwLock<Option<GraphSchema>>,
}
```

### 3.4 CowSnapshot

An owned, immutable snapshot:

```rust
/// An owned snapshot of the graph at a point in time.
/// 
/// Unlike the current `GraphSnapshot<'g>`, this snapshot:
/// - Does not hold any locks
/// - Can be sent across threads (`Send + Sync`)
/// - Can outlive the source `CowGraph`
/// - Is immutable and will never change
#[derive(Clone)]
pub struct CowSnapshot {
    /// Shared reference to frozen state
    state: Arc<CowGraphState>,
}
```

---

## 4. Operations

### 4.1 Snapshot Creation

Taking a snapshot is O(1) - just clone the Arc:

```rust
impl CowGraph {
    /// Create a snapshot of the current graph state.
    /// 
    /// This is an O(1) operation that creates a shared reference
    /// to the current state. The snapshot will not reflect any
    /// mutations made after this call.
    /// 
    /// # Thread Safety
    /// 
    /// This method briefly acquires a read lock to clone the state Arc.
    /// The returned snapshot does not hold any locks.
    pub fn snapshot(&self) -> CowSnapshot {
        let state = self.state.read();
        CowSnapshot {
            state: Arc::new((*state).clone()),
        }
    }
}
```

**Note:** The `im::HashMap::clone()` is O(1) due to structural sharing.

### 4.2 Read Operations

Snapshots implement `GraphStorage` for seamless integration:

```rust
impl GraphStorage for CowSnapshot {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        self.state.vertices.get(&id).map(|node| {
            let label = self.state.interner.resolve(node.label_id)
                .unwrap_or_default()
                .to_string();
            Vertex {
                id: node.id,
                label,
                properties: node.properties.clone(),
            }
        })
    }
    
    fn vertex_count(&self) -> u64 {
        self.state.vertices.len() as u64
    }
    
    fn out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        let node = match self.state.vertices.get(&vertex) {
            Some(n) => n,
            None => return Box::new(std::iter::empty()),
        };
        
        let edges: Vec<_> = node.out_edges.iter()
            .filter_map(|eid| self.get_edge(*eid))
            .collect();
        
        Box::new(edges.into_iter())
    }
    
    // ... other GraphStorage methods
}
```

### 4.3 Write Operations

Writes modify the mutable state, triggering copy-on-write:

```rust
impl CowGraph {
    /// Add a vertex to the graph.
    /// 
    /// This triggers copy-on-write: only the modified paths in the
    /// persistent data structure are copied. Existing snapshots
    /// continue to see the old state.
    pub fn add_vertex(
        &self,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> VertexId {
        let mut state = self.state.write();
        
        // Allocate ID
        let id = VertexId(state.next_vertex_id);
        state.next_vertex_id += 1;
        
        // Intern label
        let label_id = state.interner_mut().intern(label);
        
        // Create node
        let node = Arc::new(NodeData {
            id,
            label_id,
            properties,
            out_edges: Vec::new(),
            in_edges: Vec::new(),
        });
        
        // Insert into persistent map (O(log n), structural sharing)
        state.vertices = state.vertices.update(id, node);
        
        // Update label index
        let bitmap = state.vertex_labels
            .get(&label_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(RoaringBitmap::new()));
        let mut new_bitmap = (*bitmap).clone();
        new_bitmap.insert(id.0 as u32);
        state.vertex_labels = state.vertex_labels.update(label_id, Arc::new(new_bitmap));
        
        // Increment version
        state.version += 1;
        
        id
    }
    
    /// Update a vertex's properties.
    pub fn set_vertex_property(
        &self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write();
        
        let node = state.vertices.get(&id)
            .ok_or(StorageError::VertexNotFound(id))?;
        
        // Clone and modify the node
        let mut new_node = (**node).clone();
        new_node.properties.insert(key.to_string(), value);
        
        // Update in persistent map
        state.vertices = state.vertices.update(id, Arc::new(new_node));
        state.version += 1;
        
        Ok(())
    }
    
    /// Add an edge between two vertices.
    pub fn add_edge(
        &self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError> {
        let mut state = self.state.write();
        
        // Verify vertices exist
        if !state.vertices.contains_key(&src) {
            return Err(StorageError::VertexNotFound(src));
        }
        if !state.vertices.contains_key(&dst) {
            return Err(StorageError::VertexNotFound(dst));
        }
        
        // Allocate edge ID
        let edge_id = EdgeId(state.next_edge_id);
        state.next_edge_id += 1;
        
        // Intern label
        let label_id = state.interner_mut().intern(label);
        
        // Create edge
        let edge = Arc::new(EdgeData {
            id: edge_id,
            label_id,
            src,
            dst,
            properties,
        });
        
        // Insert edge
        state.edges = state.edges.update(edge_id, edge);
        
        // Update source vertex's out_edges
        if let Some(src_node) = state.vertices.get(&src) {
            let mut new_src = (**src_node).clone();
            new_src.out_edges.push(edge_id);
            state.vertices = state.vertices.update(src, Arc::new(new_src));
        }
        
        // Update destination vertex's in_edges
        if let Some(dst_node) = state.vertices.get(&dst) {
            let mut new_dst = (**dst_node).clone();
            new_dst.in_edges.push(edge_id);
            state.vertices = state.vertices.update(dst, Arc::new(new_dst));
        }
        
        // Update label index
        let bitmap = state.edge_labels
            .get(&label_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(RoaringBitmap::new()));
        let mut new_bitmap = (*bitmap).clone();
        new_bitmap.insert(edge_id.0 as u32);
        state.edge_labels = state.edge_labels.update(label_id, Arc::new(new_bitmap));
        
        state.version += 1;
        
        Ok(edge_id)
    }
}
```

### 4.4 Interner Handling

The `StringInterner` is append-only and can be shared:

```rust
impl CowGraphState {
    /// Get shared reference to interner (for reads)
    pub fn interner(&self) -> &StringInterner {
        &self.interner
    }
    
    /// Get mutable access to interner (for writes)
    /// Uses Arc::make_mut for copy-on-write semantics
    pub fn interner_mut(&mut self) -> &mut StringInterner {
        Arc::make_mut(&mut self.interner)
    }
}
```

---

## 5. Unified Query/Mutation API

A key benefit of COW is enabling a **unified API** where users don't need to decide upfront whether their query is a read or a mutation. The system analyzes the query and acquires appropriate locks automatically.

### 5.1 The Problem with Separate Paths

The current architecture has separate code paths:

```
┌─────────────────────────────────────────────────────────────────┐
│              Current: Separate Read/Write Paths                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Read Path:                                                     │
│  ──────────                                                     │
│    let snapshot = graph.snapshot();      // Read lock           │
│    let results = snapshot.gql("MATCH (n) RETURN n")?;           │
│                                                                 │
│  Write Path:                                                    │
│  ───────────                                                    │
│    let mut_handle = graph.mutate();      // Write lock          │
│    let results = mut_handle.gql("CREATE (n:Person)", &mut storage)?; │
│                                                                 │
│  Problems:                                                      │
│  • User must choose the right path upfront                      │
│  • GraphMut::gql() requires separate &mut storage argument      │
│  • Rhai scripts can only create "pending" mutations             │
│  • No way to mix reads and writes in single query               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 Unified API Design

With COW, `CowGraph` can provide a single entry point:

```rust
impl CowGraph {
    /// Execute any GQL statement - reads, mutations, or mixed.
    /// 
    /// The system automatically:
    /// 1. Parses the statement to determine if it contains mutations
    /// 2. Acquires the appropriate lock (read for queries, write for mutations)
    /// 3. Executes atomically and returns results
    /// 
    /// # Examples
    /// 
    /// ```rust
    /// let graph = CowGraph::new();
    /// 
    /// // Read query - uses snapshot internally
    /// let results = graph.execute("MATCH (n:Person) RETURN n.name")?;
    /// 
    /// // Mutation - acquires write lock, executes atomically
    /// let results = graph.execute("CREATE (n:Person {name: 'Alice'}) RETURN n")?;
    /// 
    /// // Mixed read/write - acquires write lock, reads see consistent state
    /// let results = graph.execute("
    ///     MATCH (a:Person {name: 'Alice'})
    ///     CREATE (b:Person {name: 'Bob'})
    ///     CREATE (a)-[:KNOWS]->(b)
    ///     RETURN a, b
    /// ")?;
    /// ```
    pub fn execute(&self, gql: &str) -> Result<Vec<Value>, GqlError> {
        let stmt = parse_statement(gql)?;
        
        if stmt.is_read_only() {
            // Lock-free read via snapshot
            let snap = self.snapshot();
            compile_statement(&stmt, &snap)
        } else {
            // Acquire write lock for mutations
            let mut state = self.state.write();
            execute_mutation_atomic(&stmt, &mut state, self.schema.read().as_ref())
        }
    }
    
    /// Execute with parameters
    pub fn execute_with_params(
        &self,
        gql: &str,
        params: &Parameters,
    ) -> Result<Vec<Value>, GqlError> {
        let stmt = parse_statement(gql)?;
        
        if stmt.is_read_only() {
            let snap = self.snapshot();
            compile_statement_with_params(&stmt, &snap, params)
        } else {
            let mut state = self.state.write();
            execute_mutation_with_params_atomic(&stmt, &mut state, self.schema.read().as_ref(), params)
        }
    }
}
```

### 5.3 Statement Classification

The parser determines statement type by analyzing the AST:

```rust
/// Classify a statement as read-only or mutation
impl Statement {
    pub fn is_read_only(&self) -> bool {
        match self {
            Statement::Query(q) => q.is_read_only(),
            Statement::Union { queries, .. } => queries.iter().all(|q| q.is_read_only()),
            Statement::Mutation(_) => false,
            Statement::Ddl(_) => false,
        }
    }
}

impl Query {
    pub fn is_read_only(&self) -> bool {
        // A query is read-only if it has no mutation clauses
        self.clauses.iter().all(|clause| match clause {
            Clause::Match(_) => true,
            Clause::Where(_) => true,
            Clause::Return(_) => true,
            Clause::With(_) => true,
            Clause::OrderBy(_) => true,
            Clause::Skip(_) => true,
            Clause::Limit(_) => true,
            Clause::Call(_) => true,  // CALL {} subqueries need deeper analysis
            Clause::Create(_) => false,
            Clause::Merge(_) => false,
            Clause::Set(_) => false,
            Clause::Delete(_) => false,
            Clause::Remove(_) => false,
            Clause::ForEach(_) => false,
        })
    }
}
```

### 5.4 Gremlin-Style Unified Traversal

For Gremlin-style traversals, `CowGraph` provides a unified traversal source:

```rust
impl CowGraph {
    /// Create a traversal source that can execute both reads and mutations.
    /// 
    /// Unlike the snapshot-based `GraphTraversalSource`, this source can
    /// execute mutation steps like `addV()`, `addE()`, `drop()`, etc.
    pub fn traversal(&self) -> CowTraversalSource<'_> {
        CowTraversalSource { graph: self }
    }
}

pub struct CowTraversalSource<'g> {
    graph: &'g CowGraph,
}

impl<'g> CowTraversalSource<'g> {
    /// Start traversal from all vertices (read operation)
    pub fn v(&self) -> CowTraversal<'g> {
        CowTraversal {
            graph: self.graph,
            source: TraversalSource::AllVertices,
            steps: Vec::new(),
            has_mutations: false,
        }
    }
    
    /// Add a new vertex (mutation operation)
    pub fn add_v(&self, label: &str) -> CowTraversal<'g> {
        CowTraversal {
            graph: self.graph,
            source: TraversalSource::Empty,
            steps: vec![Step::AddV(label.to_string())],
            has_mutations: true,
        }
    }
    
    // ... other source steps
}

impl<'g> CowTraversal<'g> {
    /// Execute the traversal and return results.
    /// 
    /// Automatically determines if mutations are involved:
    /// - Read-only: Uses a snapshot (lock-free)
    /// - Has mutations: Acquires write lock, executes atomically
    pub fn to_list(self) -> Vec<Value> {
        if self.has_mutations {
            // Acquire write lock and execute
            let mut state = self.graph.state.write();
            self.execute_with_mutations(&mut state)
        } else {
            // Lock-free read via snapshot
            let snap = self.graph.snapshot();
            self.execute_read_only(&snap)
        }
    }
}
```

---

## 6. Statement-Level Atomicity

Each GQL statement or Gremlin traversal executes atomically - either all mutations succeed or none do.

### 6.1 Atomicity Guarantees

```
┌─────────────────────────────────────────────────────────────────┐
│              Statement-Level Atomicity                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Single Statement (atomic):                                     │
│  ─────────────────────────                                      │
│    CREATE (a:Person {name: 'Alice'}),                           │
│           (b:Person {name: 'Bob'}),                             │
│           (a)-[:KNOWS]->(b)                                     │
│    ──▶ All three elements created, or none if error             │
│                                                                 │
│  Multiple Statements (each atomic independently):               │
│  ───────────────────────────────────────────────                │
│    graph.execute("CREATE (a:Person {name: 'Alice'})")?;  // ✓   │
│    graph.execute("CREATE (b:Person {name: 'Bob'})")?;    // ✓   │
│    graph.execute("CREATE (a)-[:KNOWS]->(b)")?;           // ✗   │
│    ──▶ Alice and Bob exist, but edge fails (variables lost)    │
│                                                                 │
│  For multi-statement atomicity, use batch():                    │
│  ────────────────────────────────────────────                   │
│    graph.batch(|g| {                                            │
│        let a = g.add_v("Person").property("name", "Alice");     │
│        let b = g.add_v("Person").property("name", "Bob");       │
│        g.add_e("knows").from(a).to(b);                          │
│        Ok(())                                                   │
│    })?;                                                         │
│    ──▶ All succeed or all rolled back                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 6.2 Implementation: Two-Phase Mutation Execution

Mutations are executed in two phases to ensure atomicity:

```rust
/// Execute a mutation statement atomically
fn execute_mutation_atomic(
    stmt: &Statement,
    state: &mut CowGraphState,
    schema: Option<&GraphSchema>,
) -> Result<Vec<Value>, GqlError> {
    // Phase 1: Validate and plan
    // ─────────────────────────
    // - Parse and validate the statement
    // - Check schema constraints (if schema exists)
    // - Build a plan of mutations to execute
    // - If any validation fails, return error without modifying state
    
    let plan = plan_mutations(stmt, state, schema)?;
    
    // Phase 2: Execute
    // ────────────────
    // - Apply all mutations to the state
    // - Since we hold the write lock and COW provides structural sharing,
    //   if we panic mid-execution, the original state is preserved
    // - Only after all mutations succeed do we increment version
    
    let results = execute_plan(plan, state)?;
    
    state.version += 1;
    
    Ok(results)
}

/// A planned mutation operation
enum MutationOp {
    CreateVertex { label: String, properties: HashMap<String, Value> },
    CreateEdge { src: VertexId, dst: VertexId, label: String, properties: HashMap<String, Value> },
    SetProperty { target: ElementId, key: String, value: Value },
    DeleteVertex { id: VertexId },
    DeleteEdge { id: EdgeId },
}

/// Plan mutations without executing them
fn plan_mutations(
    stmt: &Statement,
    state: &CowGraphState,
    schema: Option<&GraphSchema>,
) -> Result<Vec<MutationOp>, GqlError> {
    let mut ops = Vec::new();
    
    // Walk the statement and collect operations
    // Validate each operation against schema
    // Return error if any validation fails
    
    for clause in stmt.clauses() {
        match clause {
            Clause::Create(create) => {
                for pattern in &create.patterns {
                    validate_and_plan_create(pattern, state, schema, &mut ops)?;
                }
            }
            // ... other clauses
        }
    }
    
    Ok(ops)
}
```

### 6.3 Batch Execution for Multi-Statement Atomicity

For cases requiring multiple statements to be atomic:

```rust
impl CowGraph {
    /// Execute multiple operations atomically.
    /// 
    /// The closure receives a `BatchContext` that buffers all mutations.
    /// Only when the closure returns `Ok(())` are all mutations applied.
    /// If the closure returns `Err` or panics, no mutations are applied.
    /// 
    /// # Example
    /// 
    /// ```rust
    /// graph.batch(|ctx| {
    ///     let alice = ctx.add_vertex("Person", props("name", "Alice"))?;
    ///     let bob = ctx.add_vertex("Person", props("name", "Bob"))?;
    ///     ctx.add_edge(alice, bob, "knows", HashMap::new())?;
    ///     Ok(())
    /// })?;
    /// ```
    pub fn batch<F, T>(&self, f: F) -> Result<T, BatchError>
    where
        F: FnOnce(&mut BatchContext) -> Result<T, BatchError>,
    {
        // Take a snapshot of current state (for reads during batch)
        let read_snapshot = self.snapshot();
        
        // Create batch context that buffers writes
        let mut ctx = BatchContext {
            snapshot: &read_snapshot,
            pending_ops: Vec::new(),
            next_temp_vertex_id: 0,
            next_temp_edge_id: 0,
        };
        
        // Execute user function
        let result = f(&mut ctx)?;
        
        // If successful, apply all pending operations atomically
        let mut state = self.state.write();
        for op in ctx.pending_ops {
            apply_op(&mut state, op)?;
        }
        state.version += 1;
        
        Ok(result)
    }
}

pub struct BatchContext<'a> {
    snapshot: &'a CowSnapshot,
    pending_ops: Vec<MutationOp>,
    next_temp_vertex_id: u64,
    next_temp_edge_id: u64,
}

impl<'a> BatchContext<'a> {
    /// Add a vertex (buffered, not yet committed)
    pub fn add_vertex(
        &mut self,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<TempVertexId, BatchError> {
        let temp_id = TempVertexId(self.next_temp_vertex_id);
        self.next_temp_vertex_id += 1;
        
        self.pending_ops.push(MutationOp::CreateVertex {
            temp_id,
            label: label.to_string(),
            properties,
        });
        
        Ok(temp_id)
    }
    
    /// Read from the snapshot (sees state before batch started)
    pub fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        self.snapshot.get_vertex(id)
    }
    
    // ... other methods
}
```

### 6.4 Error Handling and Rollback

Since COW uses structural sharing, rollback is implicit:

```rust
// If execution fails partway through, the original state is unchanged
// because we only modify a copy of the path to the modified nodes

impl CowGraph {
    pub fn execute(&self, gql: &str) -> Result<Vec<Value>, GqlError> {
        let stmt = parse_statement(gql)?;
        
        if stmt.is_read_only() {
            let snap = self.snapshot();
            compile_statement(&stmt, &snap)
        } else {
            let mut state = self.state.write();
            
            // Clone state for modification (O(1) due to structural sharing)
            let mut working_state = (*state).clone();
            
            // Execute mutations on the working copy
            let results = execute_mutation_atomic(&stmt, &mut working_state, self.schema.read().as_ref())?;
            
            // Only on success: replace the current state
            *state = working_state;
            
            Ok(results)
        }
    }
}
```

---

## 7. Rhai Integration

With the unified API, Rhai scripts can execute mutations directly instead of creating pending markers.

### 7.1 Current Limitation

The current Rhai integration has a fundamental limitation:

```rust
// Current: Rhai creates "pending" mutations that can't be executed
impl RhaiTraversal {
    pub fn add_v(&self, label: String) -> RhaiTraversal {
        // This just adds a step to the traversal
        // The actual mutation never happens!
        RhaiTraversal {
            steps: vec![RhaiStep::AddV(label)],
            ..
        }
    }
    
    pub fn to_list(&self) -> Vec<Value> {
        // Returns Value::Map with "__pending_add_v" markers
        // User must manually execute via MutationExecutor
        // But there's no way to get &mut storage in Rhai!
    }
}
```

### 7.2 COW-Based Rhai Integration

With COW, Rhai can execute mutations through `&self`:

```rust
/// Rhai-compatible graph wrapper using COW
#[derive(Clone)]
pub struct RhaiCowGraph {
    inner: Arc<CowGraph>,
}

impl RhaiCowGraph {
    pub fn new(graph: CowGraph) -> Self {
        Self { inner: Arc::new(graph) }
    }
    
    /// Execute a GQL query or mutation
    /// 
    /// Rhai example:
    /// ```rhai
    /// let results = graph.execute("CREATE (n:Person {name: 'Alice'}) RETURN n");
    /// ```
    pub fn execute(&self, gql: &str) -> Result<Vec<Dynamic>, Box<EvalAltResult>> {
        self.inner.execute(gql)
            .map(|values| values.into_iter().map(value_to_dynamic).collect())
            .map_err(|e| e.to_string().into())
    }
    
    /// Create a traversal source
    pub fn traversal(&self) -> RhaiCowTraversalSource {
        RhaiCowTraversalSource {
            graph: Arc::clone(&self.inner),
        }
    }
}

#[derive(Clone)]
pub struct RhaiCowTraversalSource {
    graph: Arc<CowGraph>,
}

impl RhaiCowTraversalSource {
    /// Start from all vertices
    pub fn v(&self) -> RhaiCowTraversal {
        RhaiCowTraversal {
            graph: Arc::clone(&self.graph),
            source: TraversalSource::AllVertices,
            steps: Vec::new(),
        }
    }
    
    /// Add a new vertex - mutation is executed when to_list() is called
    pub fn add_v(&self, label: String) -> RhaiCowTraversal {
        RhaiCowTraversal {
            graph: Arc::clone(&self.graph),
            source: TraversalSource::Empty,
            steps: vec![RhaiStep::AddV(label)],
        }
    }
}

#[derive(Clone)]
pub struct RhaiCowTraversal {
    graph: Arc<CowGraph>,
    source: TraversalSource,
    steps: Vec<RhaiStep>,
}

impl RhaiCowTraversal {
    /// Execute the traversal and return results.
    /// 
    /// If the traversal contains mutations, they are executed atomically.
    pub fn to_list(&self) -> Vec<Dynamic> {
        let has_mutations = self.steps.iter().any(|s| s.is_mutation());
        
        if has_mutations {
            // Execute with write lock
            self.execute_with_mutations()
        } else {
            // Execute read-only via snapshot
            self.execute_read_only()
        }
    }
    
    fn execute_with_mutations(&self) -> Vec<Dynamic> {
        let mut state = self.graph.state.write();
        
        // Execute each step, accumulating results
        let mut results = Vec::new();
        
        for step in &self.steps {
            match step {
                RhaiStep::AddV(label) => {
                    let id = allocate_vertex_id(&mut state);
                    let node = Arc::new(NodeData {
                        id,
                        label_id: state.interner_mut().intern(label),
                        properties: HashMap::new(),
                        out_edges: Vec::new(),
                        in_edges: Vec::new(),
                    });
                    state.vertices = state.vertices.update(id, node);
                    results.push(value_to_dynamic(Value::VertexId(id)));
                }
                // ... other mutation steps
            }
        }
        
        state.version += 1;
        results
    }
}
```

### 7.3 Rhai Script Examples

With COW-based Rhai integration:

```rhai
// Create a person and their friend
let g = graph.traversal();
let alice = g.add_v("Person").property("name", "Alice").next();
let bob = g.add_v("Person").property("name", "Bob").next();
g.v_id(alice).add_e("knows").to(bob).next();

// Query using GQL
let friends = graph.execute("
    MATCH (a:Person {name: 'Alice'})-[:knows]->(b)
    RETURN b.name
");

// Mixed traversal - reads and writes in same script
let g = graph.traversal();
let lonely = g.v().has_label("Person").not(__.out("knows")).to_list();
for person in lonely {
    // Give them a friend
    let friend = g.add_v("Person").property("name", "Friend").next();
    g.v_id(person).add_e("knows").to(friend).next();
}
```

### 7.4 Rhai Engine Registration

```rust
pub fn register_cow_graph_api(engine: &mut Engine) {
    // Register types
    engine.register_type::<RhaiCowGraph>()
        .register_fn("execute", RhaiCowGraph::execute)
        .register_fn("traversal", RhaiCowGraph::traversal)
        .register_fn("snapshot", RhaiCowGraph::snapshot)
        .register_fn("batch", RhaiCowGraph::batch);
    
    engine.register_type::<RhaiCowTraversalSource>()
        .register_fn("v", RhaiCowTraversalSource::v)
        .register_fn("v_id", RhaiCowTraversalSource::v_id)
        .register_fn("e", RhaiCowTraversalSource::e)
        .register_fn("add_v", RhaiCowTraversalSource::add_v)
        .register_fn("add_e", RhaiCowTraversalSource::add_e);
    
    engine.register_type::<RhaiCowTraversal>()
        .register_fn("out", RhaiCowTraversal::out)
        .register_fn("in_", RhaiCowTraversal::in_)
        .register_fn("has", RhaiCowTraversal::has)
        .register_fn("has_label", RhaiCowTraversal::has_label)
        .register_fn("property", RhaiCowTraversal::property)
        .register_fn("drop", RhaiCowTraversal::drop)
        .register_fn("to_list", RhaiCowTraversal::to_list)
        .register_fn("next", RhaiCowTraversal::next)
        .register_fn("count", RhaiCowTraversal::count);
}
```

### 7.5 Script-Level Atomicity

For Rhai scripts that need all-or-nothing semantics:

```rust
impl RhaiCowGraph {
    /// Execute a Rhai script atomically.
    /// 
    /// All mutations in the script are buffered and only applied
    /// if the script completes successfully.
    /// 
    /// Rhai example:
    /// ```rhai
    /// graph.atomic(|| {
    ///     let a = g.add_v("Person").property("name", "Alice").next();
    ///     let b = g.add_v("Person").property("name", "Bob").next();
    ///     g.v_id(a).add_e("knows").to(b).next();
    ///     // If any step fails, none of the above are committed
    /// });
    /// ```
    pub fn atomic<F>(&self, f: F) -> Result<Dynamic, Box<EvalAltResult>>
    where
        F: FnOnce() -> Result<Dynamic, Box<EvalAltResult>>,
    {
        self.inner.batch(|_ctx| {
            // Within batch context, all mutations are buffered
            f()
        })
    }
}
```

---

## 8. Integration with Existing API

### 8.1 Graph Wrapper

Maintain backward compatibility with the existing `Graph` type:

```rust
/// The main Graph type - now backed by CowGraph internally
pub struct Graph {
    inner: CowGraph,
}

impl Graph {
    /// Create a new in-memory graph
    pub fn in_memory() -> Self {
        Self {
            inner: CowGraph::new(),
        }
    }
    
    /// Create a snapshot for read-only traversals
    /// 
    /// The returned snapshot is owned and does not hold any locks.
    pub fn snapshot(&self) -> GraphSnapshot {
        GraphSnapshot {
            inner: self.inner.snapshot(),
        }
    }
    
    /// Get mutable access for writes
    /// 
    /// Note: For backward compatibility, this still uses a guard pattern,
    /// but internally delegates to CowGraph's write methods.
    pub fn mutate(&self) -> GraphMut<'_> {
        GraphMut {
            graph: &self.inner,
        }
    }
}
```

### 8.2 GraphSnapshot Wrapper

```rust
/// A snapshot of the graph for read-only traversals.
/// 
/// This is now an owned type that does not borrow from Graph.
#[derive(Clone)]
pub struct GraphSnapshot {
    inner: CowSnapshot,
}

impl GraphSnapshot {
    /// Create a traversal source
    pub fn traversal(&self) -> GraphTraversalSource<'_> {
        GraphTraversalSource::new(self, self.inner.interner())
    }
    
    /// Get the snapshot version
    pub fn version(&self) -> u64 {
        self.inner.state.version
    }
}

impl GraphStorage for GraphSnapshot {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        self.inner.get_vertex(id)
    }
    
    // ... delegate all methods to inner
}
```

### 8.3 Traversal Integration

The traversal engine needs minimal changes:

```rust
impl<'g> GraphTraversalSource<'g> {
    /// Create traversal source from a snapshot
    pub fn new(snapshot: &'g GraphSnapshot, interner: &'g StringInterner) -> Self {
        Self {
            storage: snapshot,
            interner,
        }
    }
    
    // All existing methods work unchanged since GraphSnapshot
    // implements GraphStorage
}
```

---

## 9. Memory Management

### 9.1 Automatic Cleanup

Memory is automatically reclaimed via `Arc` reference counting:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Memory Lifecycle                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  T=0: Graph created                                             │
│       State v1 (refcount = 1, owned by Graph)                   │
│                                                                 │
│  T=1: Snapshot A taken                                          │
│       State v1 (refcount = 2: Graph + Snapshot A)               │
│                                                                 │
│  T=2: Write occurs, new state created                           │
│       State v1 (refcount = 1: Snapshot A only)                  │
│       State v2 (refcount = 1: Graph only)                       │
│       [Structural sharing: unchanged nodes shared]              │
│                                                                 │
│  T=3: Snapshot A dropped                                        │
│       State v1 (refcount = 0, FREED)                            │
│       State v2 (refcount = 1: Graph only)                       │
│       [Only nodes unique to v1 are freed]                       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 9.2 Structural Sharing

The `im` crate uses structural sharing to minimize copying:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Structural Sharing                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Before mutation (State v1):                                    │
│                                                                 │
│         Root                                                    │
│        /    \                                                   │
│     Node A   Node B                                             │
│      / \       |                                                │
│    V1  V2     V3                                                │
│                                                                 │
│  After updating V1 (State v2):                                  │
│                                                                 │
│    Root'              Root (v1, still valid)                    │
│    /    \            /    \                                     │
│ Node A'  Node B   Node A   Node B                               │
│   / \      |        / \      |                                  │
│ V1'  V2   V3      V1  V2    V3                                  │
│       ↑    ↑           ↑     ↑                                  │
│       └────┴───────────┴─────┘                                  │
│              (shared)                                           │
│                                                                 │
│  Memory: Only Root', Node A', V1' are new allocations           │
│  Cost: O(log₃₂ n) nodes copied, not O(n)                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 9.3 Memory Overhead

| Component | Current | COW |
|-----------|---------|-----|
| HashMap entry | 1 pointer | 1 pointer + tree overhead |
| Node/Edge | Direct | Arc wrapper (16 bytes) |
| Snapshot | Lock guard | Arc (16 bytes) |
| Overall | Lower | ~20-30% higher base |

Trade-off: Higher base memory for O(1) snapshots and no lock contention.

---

## 10. Thread Safety

### 10.1 Guarantees

| Type | Send | Sync | Notes |
|------|------|------|-------|
| `CowGraph` | Yes | Yes | RwLock provides synchronization |
| `CowSnapshot` | Yes | Yes | Immutable, no synchronization needed |
| `CowGraphState` | Yes | Yes | Immutable after creation |
| `Arc<NodeData>` | Yes | Yes | Shared ownership |

### 10.2 Concurrency Model

```rust
// Multiple readers - no blocking
let snap1 = graph.snapshot();  // O(1)
let snap2 = graph.snapshot();  // O(1)

// Can use snapshots across threads
std::thread::spawn(move || {
    let count = snap1.vertex_count();
});

std::thread::spawn(move || {
    let count = snap2.vertex_count();
});

// Writer doesn't block readers
graph.add_vertex("person", props);  // Creates new state
// snap1 and snap2 still see old state
```

### 10.3 Write Serialization

Writes are still serialized via `RwLock::write()`:

```rust
// These will be serialized (one blocks the other)
thread::spawn(|| graph.add_vertex("a", props1));
thread::spawn(|| graph.add_vertex("b", props2));
```

For concurrent writes, external coordination is needed (out of scope for this spec).

---

## 11. Performance Characteristics

### 11.1 Operation Complexity

| Operation | Current | COW | Notes |
|-----------|---------|-----|-------|
| `snapshot()` | O(1) | O(1) | Arc clone |
| `get_vertex()` | O(1) | O(log₃₂ n) | Tree traversal |
| `add_vertex()` | O(1)* | O(log₃₂ n) | Path copy |
| `add_edge()` | O(1)* | O(log₃₂ n) | Path copy |
| `vertex_count()` | O(1) | O(1) | Cached |
| Clone graph | O(n) | O(1) | Structural sharing |

*Amortized for HashMap resizing

### 11.2 Benchmarks to Implement

```rust
#[bench]
fn bench_snapshot_creation(b: &mut Bencher) {
    let graph = build_graph(1_000_000);  // 1M vertices
    b.iter(|| graph.snapshot());
    // Target: < 100ns
}

#[bench]
fn bench_vertex_lookup(b: &mut Bencher) {
    let graph = build_graph(1_000_000);
    let snap = graph.snapshot();
    b.iter(|| snap.get_vertex(VertexId(500_000)));
    // Target: < 200ns
}

#[bench]
fn bench_write_during_read(b: &mut Bencher) {
    let graph = Arc::new(build_graph(1_000_000));
    let snap = graph.snapshot();
    
    // Measure write latency while snapshot exists
    b.iter(|| graph.add_vertex("test", HashMap::new()));
    // Target: No degradation vs. no active snapshot
}

#[bench]
fn bench_concurrent_reads(b: &mut Bencher) {
    let graph = Arc::new(build_graph(1_000_000));
    
    // Multiple threads taking snapshots and reading
    b.iter(|| {
        let handles: Vec<_> = (0..8).map(|_| {
            let g = Arc::clone(&graph);
            thread::spawn(move || {
                let snap = g.snapshot();
                snap.vertex_count()
            })
        }).collect();
        
        handles.into_iter().map(|h| h.join().unwrap()).sum::<u64>()
    });
    // Target: Linear scaling with threads
}
```

---

## 12. Migration Path

### 12.1 Phase 1: Add CowGraph (Non-Breaking)

1. Add `im` dependency
2. Create `src/storage/cow.rs` with `CowGraph`, `CowSnapshot`
3. Implement `GraphStorage` for `CowSnapshot`
4. Add comprehensive tests

```rust
// New module structure
src/storage/
├── mod.rs
├── inmemory.rs      // Existing - unchanged
├── cow.rs           // New - CowGraph implementation
└── interner.rs      // Existing - unchanged
```

### 12.2 Phase 2: Integrate with Graph (Optional Breaking)

Option A: New constructors (non-breaking):
```rust
impl Graph {
    pub fn in_memory() -> Self { /* current impl */ }
    pub fn in_memory_cow() -> Self { /* new COW impl */ }
}
```

Option B: Replace default (breaking):
```rust
impl Graph {
    pub fn in_memory() -> Self { /* new COW impl */ }
    pub fn in_memory_legacy() -> Self { /* old impl */ }
}
```

### 12.3 Phase 3: Update Documentation

- Update README with new concurrency model
- Add migration guide for users
- Update benchmarks

---

## 13. Testing Strategy

### 13.1 Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_snapshot_isolation() {
        let graph = CowGraph::new();
        let v1 = graph.add_vertex("person", props("name", "Alice"));
        
        // Take snapshot
        let snap = graph.snapshot();
        
        // Modify graph
        graph.set_vertex_property(v1, "name", "Alicia".into()).unwrap();
        
        // Snapshot still sees old value
        let vertex = snap.get_vertex(v1).unwrap();
        assert_eq!(vertex.properties.get("name"), Some(&Value::from("Alice")));
        
        // New snapshot sees new value
        let snap2 = graph.snapshot();
        let vertex2 = snap2.get_vertex(v1).unwrap();
        assert_eq!(vertex2.properties.get("name"), Some(&Value::from("Alicia")));
    }
    
    #[test]
    fn test_snapshot_survives_graph_modification() {
        let graph = CowGraph::new();
        for i in 0..1000 {
            graph.add_vertex("node", props("id", i));
        }
        
        let snap = graph.snapshot();
        assert_eq!(snap.vertex_count(), 1000);
        
        // Add more vertices
        for i in 1000..2000 {
            graph.add_vertex("node", props("id", i));
        }
        
        // Snapshot unchanged
        assert_eq!(snap.vertex_count(), 1000);
        
        // New snapshot sees all
        assert_eq!(graph.snapshot().vertex_count(), 2000);
    }
    
    #[test]
    fn test_snapshot_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CowSnapshot>();
        assert_send_sync::<CowGraph>();
    }
    
    #[test]
    fn test_snapshot_can_outlive_scope() {
        let snap = {
            let graph = CowGraph::new();
            graph.add_vertex("person", HashMap::new());
            graph.snapshot()
        };  // graph dropped here
        
        // Snapshot still valid
        assert_eq!(snap.vertex_count(), 1);
    }
    
    #[test]
    fn test_concurrent_snapshots() {
        let graph = Arc::new(CowGraph::new());
        for i in 0..100 {
            graph.add_vertex("node", props("id", i));
        }
        
        let handles: Vec<_> = (0..10).map(|_| {
            let g = Arc::clone(&graph);
            thread::spawn(move || {
                let snap = g.snapshot();
                snap.vertex_count()
            })
        }).collect();
        
        for handle in handles {
            assert_eq!(handle.join().unwrap(), 100);
        }
    }
}
```

### 13.2 Property-Based Tests

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn snapshot_always_consistent(ops in prop::collection::vec(graph_op(), 0..100)) {
        let graph = CowGraph::new();
        let mut snapshots = Vec::new();
        
        for op in ops {
            match op {
                GraphOp::AddVertex(label) => {
                    graph.add_vertex(&label, HashMap::new());
                }
                GraphOp::TakeSnapshot => {
                    snapshots.push((graph.snapshot(), graph.vertex_count()));
                }
            }
        }
        
        // All snapshots should still have their original counts
        for (snap, expected_count) in snapshots {
            assert_eq!(snap.vertex_count(), expected_count);
        }
    }
}
```

### 13.3 Integration Tests

```rust
#[test]
fn test_traversal_on_cow_snapshot() {
    let graph = CowGraph::new();
    
    let alice = graph.add_vertex("person", props("name", "Alice"));
    let bob = graph.add_vertex("person", props("name", "Bob"));
    graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    
    let snap = graph.snapshot();
    let g = GraphTraversalSource::new(&snap, snap.interner());
    
    let friends: Vec<_> = g.v_id(alice)
        .out("knows")
        .values("name")
        .to_list();
    
    assert_eq!(friends, vec![Value::from("Bob")]);
}
```

---

## 14. File Structure

```
src/
├── storage/
│   ├── mod.rs              # Add: pub mod cow;
│   ├── cow.rs              # New: CowGraph, CowGraphState, CowSnapshot
│   ├── inmemory.rs         # Unchanged (or deprecated later)
│   └── interner.rs         # Unchanged
├── graph.rs                # Update: Use CowGraph internally (Phase 2)
└── lib.rs                  # Update: Re-export CowGraph

tests/
└── storage/
    └── cow.rs              # New: COW-specific tests
```

---

## 15. API Summary

### 15.1 Public Types

| Type | Description |
|------|-------------|
| `CowGraph` | Mutable graph with COW semantics and unified query API |
| `CowSnapshot` | Immutable, owned snapshot for read-only access |
| `CowTraversalSource` | Unified traversal source supporting reads and mutations |
| `CowTraversal` | Traversal that can include mutation steps |
| `BatchContext` | Context for multi-operation atomic batches |
| `RhaiCowGraph` | Rhai-compatible wrapper with mutation support |

### 15.2 Core Methods

```rust
impl CowGraph {
    // Construction
    pub fn new() -> Self;
    pub fn with_schema(schema: GraphSchema) -> Self;
    
    // Unified Query API (NEW)
    pub fn execute(&self, gql: &str) -> Result<Vec<Value>, GqlError>;
    pub fn execute_with_params(&self, gql: &str, params: &Parameters) -> Result<Vec<Value>, GqlError>;
    
    // Unified Traversal API (NEW)
    pub fn traversal(&self) -> CowTraversalSource<'_>;
    
    // Batch Operations (NEW)
    pub fn batch<F, T>(&self, f: F) -> Result<T, BatchError>
    where F: FnOnce(&mut BatchContext) -> Result<T, BatchError>;
    
    // Snapshots
    pub fn snapshot(&self) -> CowSnapshot;
    
    // Direct Mutations (still available for simple cases)
    pub fn add_vertex(&self, label: &str, properties: HashMap<String, Value>) -> VertexId;
    pub fn add_edge(&self, src: VertexId, dst: VertexId, label: &str, properties: HashMap<String, Value>) -> Result<EdgeId, StorageError>;
    pub fn set_vertex_property(&self, id: VertexId, key: &str, value: Value) -> Result<(), StorageError>;
    pub fn set_edge_property(&self, id: EdgeId, key: &str, value: Value) -> Result<(), StorageError>;
    pub fn remove_vertex(&self, id: VertexId) -> Result<(), StorageError>;
    pub fn remove_edge(&self, id: EdgeId) -> Result<(), StorageError>;
    
    // Queries
    pub fn vertex_count(&self) -> u64;
    pub fn edge_count(&self) -> u64;
}

impl CowSnapshot {
    pub fn version(&self) -> u64;
    pub fn interner(&self) -> &StringInterner;
    pub fn traversal(&self) -> GraphTraversalSource<'_>;  // Read-only
    pub fn gql(&self, query: &str) -> Result<Vec<Value>, GqlError>;  // Read-only
}

impl GraphStorage for CowSnapshot {
    // All GraphStorage methods for read access
}

impl Clone for CowSnapshot {
    // O(1) clone via Arc
}
```

### 15.3 Traversal API

```rust
impl<'g> CowTraversalSource<'g> {
    // Read sources
    pub fn v(&self) -> CowTraversal<'g>;
    pub fn v_id(&self, id: VertexId) -> CowTraversal<'g>;
    pub fn e(&self) -> CowTraversal<'g>;
    pub fn inject(&self, values: Vec<Value>) -> CowTraversal<'g>;
    
    // Mutation sources (NEW)
    pub fn add_v(&self, label: &str) -> CowTraversal<'g>;
    pub fn add_e(&self, label: &str) -> CowTraversal<'g>;
}

impl<'g> CowTraversal<'g> {
    // Steps work for both reads and mutations
    pub fn out(&self, label: &str) -> Self;
    pub fn in_(&self, label: &str) -> Self;
    pub fn has(&self, key: &str, value: Value) -> Self;
    pub fn property(&self, key: &str, value: Value) -> Self;
    pub fn drop(&self) -> Self;
    
    // Terminal steps - execute atomically
    pub fn to_list(self) -> Vec<Value>;
    pub fn next(self) -> Option<Value>;
    pub fn count(self) -> u64;
}
```

### 15.4 Rhai API

```rust
impl RhaiCowGraph {
    pub fn new(graph: CowGraph) -> Self;
    pub fn execute(&self, gql: &str) -> Result<Vec<Dynamic>, Box<EvalAltResult>>;
    pub fn traversal(&self) -> RhaiCowTraversalSource;
    pub fn snapshot(&self) -> RhaiCowSnapshot;
    pub fn atomic<F>(&self, f: F) -> Result<Dynamic, Box<EvalAltResult>>;
}
```

---

## 16. Success Criteria

| Criterion | Target |
|-----------|--------|
| Snapshot creation | < 100ns |
| Vertex lookup | < 500ns (vs ~50ns current) |
| No lock contention | Readers never block |
| Memory overhead | < 50% increase |
| All existing tests pass | 100% |
| Thread-safe snapshots | Send + Sync |
| Backward compatible API | Minimal breaking changes |
| **Unified execute() API** | Single entry point for GQL reads and mutations |
| **Statement atomicity** | All mutations in a statement succeed or fail together |
| **Rhai mutation support** | Rhai scripts can create/modify/delete graph elements |
| **Batch atomicity** | batch() executes multiple operations atomically |
| **No pending markers** | Mutations execute immediately (no two-phase manual execution) |

---

## 17. Future Considerations

### 17.1 Potential Enhancements

1. **Snapshot compaction** - Merge old snapshots to reduce memory
2. **Persistent storage** - Serialize COW state to disk
3. **Write coalescing** - Buffer rapid writes
4. **Concurrent writers** - Row-level locking for parallel mutations

### 17.2 Path to Full MVCC

COW with statement-level atomicity is a stepping stone to full MVCC:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Evolution Path                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Phase 1 (Current)     Phase 2 (This Spec)     Phase 3 (Future) │
│  ─────────────────     ───────────────────     ──────────────── │
│  RwLock                COW + Unified API       Full MVCC         │
│  Blocking reads        Lock-free reads         Lock-free reads   │
│  Separate paths        Single execute()        Single execute()  │
│  Manual mutations      Statement atomicity     Transactions      │
│  No history            Current + snapshots     Historical queries│
│  Single writer         Single writer           Concurrent writers│
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 17.3 What Full MVCC Would Add

If concurrent writers or multi-statement transactions are needed:

| Feature | COW + Statement Atomicity | Full MVCC |
|---------|---------------------------|-----------|
| Lock-free reads | Yes | Yes |
| Statement atomicity | Yes | Yes |
| Multi-statement transactions | No (use batch()) | Yes (BEGIN/COMMIT) |
| Concurrent writers | No (serialized) | Yes (row-level locks) |
| Conflict detection | N/A | Write-write conflicts |
| Rollback | Implicit (on error) | Explicit (ROLLBACK) |
| Savepoints | No | Yes |
| Historical queries | No | Optional |

### 17.4 Migration Notes

When/if migrating to full MVCC:

1. **API Compatibility** - The `execute()` and `batch()` APIs will remain
2. **Transaction Extension** - Add `begin_transaction()` returning a `Transaction` handle
3. **Conflict Handling** - Add error types for write-write conflicts
4. **Garbage Collection** - Will need to track oldest active transaction for cleanup
