# Spec 39: Rhai Generic Storage Backend Support

**Phase: Scripting & Extensibility**

## 1. Overview

This spec extends the Rhai scripting integration to support both in-memory (`Graph`) and memory-mapped persistent (`CowMmapGraph`) storage backends. Currently, the Rhai integration is hardcoded to work only with `Graph`, preventing users from using scripts with persistent databases.

### 1.1 Goals

1. **Generic storage support**: Rhai scripts work with any storage backend that implements `GraphStorage`
2. **Unified API**: Script syntax remains identical regardless of storage backend
3. **Type safety**: Compile-time verification that the correct graph type is used
4. **Backward compatibility**: Existing `eval_with_graph(Arc<Graph>, ...)` API continues to work
5. **Feature-gated**: mmap support requires the `mmap` feature flag

### 1.2 Non-Goals

1. Switching storage backends within a single script
2. Cross-graph queries (joining data from multiple graphs)
3. Schema validation at the Rhai layer (handled by underlying storage)
4. Async script execution

### 1.3 Current Limitation

The Rhai integration is hardcoded to `Graph`:

```rust
// src/rhai/engine.rs - Current
pub fn eval_with_graph<T>(&self, graph: Arc<Graph>, script: &str) -> RhaiResult<T>

// src/rhai/traversal.rs - Current
pub struct RhaiGraph {
    inner: Arc<Graph>,  // Only accepts in-memory Graph
}
```

Users cannot pass `Arc<CowMmapGraph>` to the Rhai engine.

---

## 2. Architecture

### 2.1 Current State

```
┌─────────────────────────────────────────────────────────────────┐
│                         RhaiEngine                               │
├─────────────────────────────────────────────────────────────────┤
│  eval_with_graph(Arc<Graph>, script)  ◄── Only Graph supported  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         RhaiGraph                                │
│  inner: Arc<Graph>  ◄────────────────── Hardcoded to Graph      │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Target State

```
┌─────────────────────────────────────────────────────────────────┐
│                         RhaiEngine                               │
├─────────────────────────────────────────────────────────────────┤
│  eval_with_graph(Arc<Graph>, script)       ◄── In-memory        │
│  eval_with_mmap_graph(Arc<CowMmapGraph>, script) ◄── Persistent │
│  eval_with_storage(storage, script)        ◄── Generic          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    RhaiStorageAdapter                            │
│  Enum variant for each supported storage backend:               │
│  - InMemory(Arc<Graph>)                                          │
│  - Mmap(Arc<CowMmapGraph>)                                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Unified Traversal API                       │
│  RhaiTraversal delegates to underlying storage                  │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 Design Approach

Use an **enum-based adapter pattern** instead of generics. This approach:
- Avoids monomorphization complexity in Rhai type registration
- Keeps a single `RhaiGraph` type registered with Rhai
- Uses dynamic dispatch internally (negligible overhead for scripting)
- Allows conditional compilation for mmap support

---

## 3. API Design

### 3.1 RhaiEngine Extensions

```rust
// src/rhai/engine.rs

impl RhaiEngine {
    // === Existing API (unchanged) ===
    
    /// Evaluate a script with an in-memory graph.
    pub fn eval_with_graph<T>(&self, graph: Arc<Graph>, script: &str) -> RhaiResult<T>
    where
        T: Clone + Send + Sync + 'static;
    
    // === New APIs ===
    
    /// Evaluate a script with a persistent mmap-backed graph.
    #[cfg(feature = "mmap")]
    pub fn eval_with_mmap_graph<T>(
        &self,
        graph: Arc<CowMmapGraph>,
        script: &str,
    ) -> RhaiResult<T>
    where
        T: Clone + Send + Sync + 'static;
    
    /// Evaluate a pre-compiled AST with a persistent graph.
    #[cfg(feature = "mmap")]
    pub fn eval_ast_with_mmap_graph<T>(
        &self,
        graph: Arc<CowMmapGraph>,
        ast: &AST,
    ) -> RhaiResult<T>
    where
        T: Clone + Send + Sync + 'static;
    
    /// Run a script with a persistent graph (ignoring return value).
    #[cfg(feature = "mmap")]
    pub fn run_with_mmap_graph(
        &self,
        graph: Arc<CowMmapGraph>,
        script: &str,
    ) -> RhaiResult<()>;
    
    /// Evaluate a script with dynamic return type.
    #[cfg(feature = "mmap")]
    pub fn eval_with_mmap_graph_dynamic(
        &self,
        graph: Arc<CowMmapGraph>,
        script: &str,
    ) -> RhaiResult<Dynamic>;
}
```

### 3.2 Storage Adapter Enum

```rust
// src/rhai/traversal.rs

/// Adapter enum that wraps different storage backends.
/// 
/// This allows a single `RhaiGraph` type to work with multiple storage
/// implementations without requiring generic type parameters in Rhai.
#[derive(Clone)]
pub(crate) enum StorageAdapter {
    /// In-memory COW graph
    InMemory(Arc<Graph>),
    
    /// Persistent mmap-backed graph
    #[cfg(feature = "mmap")]
    Mmap(Arc<CowMmapGraph>),
}

impl StorageAdapter {
    /// Create a Gremlin-style traversal source.
    pub fn gremlin(&self) -> RhaiTraversalSource {
        RhaiTraversalSource {
            storage: self.clone(),
        }
    }
    
    /// Execute a traversal and return results.
    pub(crate) fn execute_traversal(
        &self,
        source: &TraversalSource,
        steps: &[RhaiStep],
        track_paths: bool,
    ) -> Vec<Value> {
        match self {
            StorageAdapter::InMemory(graph) => {
                execute_with_cow_graph(graph, source, steps, track_paths)
            }
            #[cfg(feature = "mmap")]
            StorageAdapter::Mmap(graph) => {
                execute_with_mmap_graph(graph, source, steps, track_paths)
            }
        }
    }
}
```

### 3.3 Updated RhaiGraph

```rust
// src/rhai/traversal.rs

/// A wrapper around a graph that can be passed to Rhai scripts.
///
/// This wrapper supports multiple storage backends via internal dispatch.
/// The script API is identical regardless of the underlying storage.
#[derive(Clone)]
pub struct RhaiGraph {
    storage: StorageAdapter,
}

impl RhaiGraph {
    /// Create from an in-memory graph.
    pub fn from_graph(graph: Arc<Graph>) -> Self {
        RhaiGraph {
            storage: StorageAdapter::InMemory(graph),
        }
    }
    
    /// Create from an Arc<Graph> (alias for backward compatibility).
    pub fn from_arc(graph: Arc<Graph>) -> Self {
        Self::from_graph(graph)
    }
    
    /// Create from a persistent mmap-backed graph.
    #[cfg(feature = "mmap")]
    pub fn from_mmap_graph(graph: Arc<CowMmapGraph>) -> Self {
        RhaiGraph {
            storage: StorageAdapter::Mmap(graph),
        }
    }
    
    /// Create a Gremlin-style traversal source.
    pub fn gremlin(&self) -> RhaiTraversalSource {
        self.storage.gremlin()
    }
}
```

### 3.4 Updated RhaiTraversalSource and RhaiTraversal

```rust
// src/rhai/traversal.rs

#[derive(Clone)]
pub struct RhaiTraversalSource {
    storage: StorageAdapter,
}

impl RhaiTraversalSource {
    pub fn v(&self) -> RhaiTraversal {
        RhaiTraversal {
            storage: self.storage.clone(),
            source: TraversalSource::AllVertices,
            steps: Vec::new(),
            track_paths: false,
        }
    }
    
    // ... other source steps unchanged, just use storage instead of graph
}

#[derive(Clone)]
pub struct RhaiTraversal {
    storage: StorageAdapter,  // Changed from Arc<Graph>
    source: TraversalSource,
    steps: Vec<RhaiStep>,
    track_paths: bool,
}

impl RhaiTraversal {
    pub fn to_list(&self) -> Vec<Value> {
        self.storage.execute_traversal(&self.source, &self.steps, self.track_paths)
    }
    
    // ... other methods delegate to storage adapter
}
```

---

## 4. Implementation Details

### 4.1 Traversal Execution Dispatch

The key implementation detail is how traversals are executed against different backends:

```rust
// src/rhai/traversal.rs

/// Execute traversal against in-memory COW graph.
fn execute_with_cow_graph(
    graph: &Arc<Graph>,
    source: &TraversalSource,
    steps: &[RhaiStep],
    track_paths: bool,
) -> Vec<Value> {
    let g = graph.gremlin();
    
    let mut bound = match source {
        TraversalSource::AllVertices => g.v(),
        TraversalSource::Vertices(ids) => g.v_ids(ids.clone()),
        TraversalSource::AllEdges => g.e(),
        TraversalSource::Edges(ids) => g.e_ids(ids.clone()),
        TraversalSource::Inject(values) => g.inject(values.clone()),
    };
    
    if track_paths {
        bound = bound.with_path();
    }
    
    for step in steps {
        bound = apply_step_cow(bound, step);
    }
    
    bound.to_list()
}

/// Execute traversal against persistent mmap graph.
#[cfg(feature = "mmap")]
fn execute_with_mmap_graph(
    graph: &Arc<CowMmapGraph>,
    source: &TraversalSource,
    steps: &[RhaiStep],
    track_paths: bool,
) -> Vec<Value> {
    // Take a snapshot for consistent reads
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();
    
    let mut bound = match source {
        TraversalSource::AllVertices => g.v(),
        TraversalSource::Vertices(ids) => g.v_ids(ids.clone()),
        TraversalSource::AllEdges => g.e(),
        TraversalSource::Edges(ids) => g.e_ids(ids.clone()),
        TraversalSource::Inject(values) => g.inject(values.clone()),
    };
    
    if track_paths {
        bound = bound.with_path();
    }
    
    for step in steps {
        bound = apply_step_mmap(bound, step);
    }
    
    bound.to_list()
}
```

### 4.2 Mutation Handling

Mutations need special handling since `CowMmapGraph` mutations can fail:

```rust
// src/rhai/traversal.rs

impl StorageAdapter {
    /// Add a vertex to the graph.
    pub(crate) fn add_vertex(
        &self,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<VertexId, RhaiError> {
        match self {
            StorageAdapter::InMemory(graph) => {
                Ok(graph.add_vertex(label, properties))
            }
            #[cfg(feature = "mmap")]
            StorageAdapter::Mmap(graph) => {
                graph.add_vertex(label, properties)
                    .map_err(|e| RhaiError::Storage(e))
            }
        }
    }
    
    /// Add an edge to the graph.
    pub(crate) fn add_edge(
        &self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, RhaiError> {
        match self {
            StorageAdapter::InMemory(graph) => {
                graph.add_edge(src, dst, label, properties)
                    .map_err(|e| RhaiError::Storage(e))
            }
            #[cfg(feature = "mmap")]
            StorageAdapter::Mmap(graph) => {
                graph.add_edge(src, dst, label, properties)
                    .map_err(|e| RhaiError::Storage(e))
            }
        }
    }
    
    // ... other mutation methods
}
```

### 4.3 Engine Scope Creation

```rust
// src/rhai/engine.rs

impl RhaiEngine {
    /// Create a scope with the in-memory graph bound.
    fn create_graph_scope(&self, graph: Arc<Graph>) -> Scope<'static> {
        let mut scope = Scope::new();
        let rhai_graph = RhaiGraph::from_graph(graph);
        scope.push("graph", rhai_graph);
        scope.push("A", create_anonymous_factory());
        scope
    }
    
    /// Create a scope with the mmap graph bound.
    #[cfg(feature = "mmap")]
    fn create_mmap_graph_scope(&self, graph: Arc<CowMmapGraph>) -> Scope<'static> {
        let mut scope = Scope::new();
        let rhai_graph = RhaiGraph::from_mmap_graph(graph);
        scope.push("graph", rhai_graph);
        scope.push("A", create_anonymous_factory());
        scope
    }
    
    #[cfg(feature = "mmap")]
    pub fn eval_with_mmap_graph<T>(&self, graph: Arc<CowMmapGraph>, script: &str) -> RhaiResult<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        let mut scope = self.create_mmap_graph_scope(graph);
        self.engine
            .eval_with_scope(&mut scope, script)
            .map_err(RhaiError::from)
    }
}
```

---

## 5. Step Implementation Strategy

### 5.1 Shared Step Application

Most steps can share implementation between backends via a trait:

```rust
// src/rhai/traversal.rs

/// Trait for applying steps to bound traversals.
/// 
/// Both `CowBoundTraversal` and `MmapBoundTraversal` implement this,
/// allowing step application logic to be shared.
trait StepApplicable: Sized {
    fn apply_out(self, labels: Vec<String>) -> Self;
    fn apply_in(self, labels: Vec<String>) -> Self;
    fn apply_has_label(self, labels: Vec<String>) -> Self;
    fn apply_has_value(self, key: String, value: Value) -> Self;
    // ... other steps
}

fn apply_step<T: StepApplicable>(bound: T, step: &RhaiStep) -> T {
    match step {
        RhaiStep::Out(labels) => bound.apply_out(labels.clone()),
        RhaiStep::In(labels) => bound.apply_in(labels.clone()),
        RhaiStep::HasLabel(labels) => bound.apply_has_label(labels.clone()),
        // ... other steps
    }
}
```

### 5.2 Backend-Specific Step Functions

For cases where the traversal types are incompatible, use separate functions:

```rust
// src/rhai/steps_cow.rs
pub(crate) fn apply_step_cow(
    bound: CowBoundTraversal,
    step: &RhaiStep,
) -> CowBoundTraversal {
    // Apply step to in-memory traversal
}

// src/rhai/steps_mmap.rs (behind #[cfg(feature = "mmap")])
pub(crate) fn apply_step_mmap(
    bound: MmapBoundTraversal,
    step: &RhaiStep,
) -> MmapBoundTraversal {
    // Apply step to mmap traversal
}
```

---

## 6. Feature Flags

### 6.1 Conditional Compilation

```toml
# Cargo.toml

[features]
default = []
mmap = ["memmap2"]
rhai = ["dep:rhai"]
# Rhai + mmap support requires both features
rhai-mmap = ["rhai", "mmap"]
```

### 6.2 Module Structure

```rust
// src/rhai/mod.rs

#[cfg(feature = "mmap")]
mod steps_mmap;

pub use engine::RhaiEngine;
pub use traversal::{RhaiGraph, RhaiTraversal, RhaiTraversalSource};

// Re-export mmap-specific types when enabled
#[cfg(feature = "mmap")]
pub use traversal::StorageAdapter;
```

---

## 7. Error Handling

### 7.1 Error Type Extensions

```rust
// src/rhai/error.rs

#[derive(Debug, Error)]
pub enum RhaiError {
    // ... existing variants ...
    
    /// Storage backend error (for mmap operations)
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    
    /// Feature not available
    #[error("feature not available: {0}")]
    FeatureNotAvailable(String),
}
```

### 7.2 Mmap-Specific Errors

```rust
impl RhaiError {
    #[cfg(not(feature = "mmap"))]
    pub fn mmap_not_available() -> Self {
        RhaiError::FeatureNotAvailable(
            "mmap support requires the 'mmap' feature flag".to_string()
        )
    }
}
```

---

## 8. Testing Strategy

### 8.1 Unit Tests

```rust
// tests/rhai/storage_backends.rs

#[cfg(feature = "rhai")]
mod tests {
    use interstellar::rhai::RhaiEngine;
    use interstellar::storage::Graph;
    use std::sync::Arc;
    
    #[test]
    fn test_inmemory_graph_traversal() {
        let graph = Arc::new(Graph::new());
        graph.add_vertex("person", [("name".into(), "Alice".into())].into());
        
        let engine = RhaiEngine::new();
        let count: i64 = engine.eval_with_graph(graph, r#"
            let g = graph.gremlin();
            g.v().has_label("person").count()
        "#).unwrap();
        
        assert_eq!(count, 1);
    }
    
    #[cfg(feature = "mmap")]
    #[test]
    fn test_mmap_graph_traversal() {
        use interstellar::storage::cow_mmap::CowMmapGraph;
        use tempfile::tempdir;
        
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        
        let graph = Arc::new(CowMmapGraph::open(&path).unwrap());
        graph.add_vertex("person", [("name".into(), "Alice".into())].into()).unwrap();
        
        let engine = RhaiEngine::new();
        let count: i64 = engine.eval_with_mmap_graph(graph, r#"
            let g = graph.gremlin();
            g.v().has_label("person").count()
        "#).unwrap();
        
        assert_eq!(count, 1);
    }
    
    #[cfg(feature = "mmap")]
    #[test]
    fn test_mmap_mutations_via_script() {
        use interstellar::storage::cow_mmap::CowMmapGraph;
        use tempfile::tempdir;
        
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let graph = Arc::new(CowMmapGraph::open(&path).unwrap());
        
        let engine = RhaiEngine::new();
        
        // Create vertices via script
        engine.run_with_mmap_graph(graph.clone(), r#"
            let g = graph.gremlin();
            g.add_v("person").property("name", "Alice").iterate();
            g.add_v("person").property("name", "Bob").iterate();
        "#).unwrap();
        
        // Verify persistence
        drop(graph);
        let graph = Arc::new(CowMmapGraph::open(&path).unwrap());
        
        let count: i64 = engine.eval_with_mmap_graph(graph, r#"
            let g = graph.gremlin();
            g.v().has_label("person").count()
        "#).unwrap();
        
        assert_eq!(count, 2);
    }
}
```

### 8.2 Integration Tests

```rust
// tests/rhai/mmap_integration.rs

#[cfg(all(feature = "rhai", feature = "mmap"))]
mod tests {
    use interstellar::rhai::RhaiEngine;
    use interstellar::storage::cow_mmap::CowMmapGraph;
    use std::sync::Arc;
    use tempfile::tempdir;
    
    #[test]
    fn test_same_script_both_backends() {
        let script = r#"
            let g = graph.gremlin();
            g.v().has_label("person").values("name").to_list()
        "#;
        
        let engine = RhaiEngine::new();
        
        // Test with in-memory
        let inmem_graph = Arc::new(interstellar::storage::Graph::new());
        inmem_graph.add_vertex("person", [("name".into(), "Alice".into())].into());
        let inmem_result: Vec<String> = engine.eval_with_graph(inmem_graph, script).unwrap();
        
        // Test with mmap
        let dir = tempdir().unwrap();
        let mmap_graph = Arc::new(CowMmapGraph::open(dir.path().join("test.db")).unwrap());
        mmap_graph.add_vertex("person", [("name".into(), "Alice".into())].into()).unwrap();
        let mmap_result: Vec<String> = engine.eval_with_mmap_graph(mmap_graph, script).unwrap();
        
        // Same script, same results
        assert_eq!(inmem_result, mmap_result);
    }
    
    #[test]
    fn test_complex_traversal_mmap() {
        let dir = tempdir().unwrap();
        let graph = Arc::new(CowMmapGraph::open(dir.path().join("test.db")).unwrap());
        
        // Build a social graph
        let alice = graph.add_vertex("person", [("name".into(), "Alice".into())].into()).unwrap();
        let bob = graph.add_vertex("person", [("name".into(), "Bob".into())].into()).unwrap();
        let carol = graph.add_vertex("person", [("name".into(), "Carol".into())].into()).unwrap();
        
        graph.add_edge(alice, bob, "knows", Default::default()).unwrap();
        graph.add_edge(bob, carol, "knows", Default::default()).unwrap();
        
        let engine = RhaiEngine::new();
        
        // Friends of friends
        let result: Vec<String> = engine.eval_with_mmap_graph(graph, r#"
            let g = graph.gremlin();
            g.v().has_value("name", "Alice")
                .out("knows")
                .out("knows")
                .values("name")
                .to_list()
        "#).unwrap();
        
        assert_eq!(result, vec!["Carol"]);
    }
}
```

### 8.3 Parity Tests

Ensure all traversal steps work identically on both backends:

```rust
// tests/rhai/backend_parity.rs

#[cfg(all(feature = "rhai", feature = "mmap"))]
mod tests {
    use interstellar::rhai::RhaiEngine;
    use interstellar::storage::{Graph, cow_mmap::CowMmapGraph};
    use std::sync::Arc;
    use tempfile::tempdir;
    
    macro_rules! test_parity {
        ($name:ident, $script:expr) => {
            #[test]
            fn $name() {
                let engine = RhaiEngine::new();
                
                // Setup in-memory graph
                let inmem = Arc::new(Graph::new());
                setup_test_graph(&inmem);
                
                // Setup mmap graph
                let dir = tempdir().unwrap();
                let mmap = Arc::new(CowMmapGraph::open(dir.path().join("test.db")).unwrap());
                setup_test_mmap_graph(&mmap);
                
                // Execute same script on both
                let inmem_result: rhai::Dynamic = engine
                    .eval_with_graph(inmem, $script)
                    .unwrap();
                let mmap_result: rhai::Dynamic = engine
                    .eval_with_mmap_graph(mmap, $script)
                    .unwrap();
                
                // Results must match
                assert_eq!(
                    format!("{:?}", inmem_result),
                    format!("{:?}", mmap_result),
                    "Script: {}",
                    $script
                );
            }
        };
    }
    
    test_parity!(parity_v_count, "graph.gremlin().v().count()");
    test_parity!(parity_e_count, "graph.gremlin().e().count()");
    test_parity!(parity_has_label, "graph.gremlin().v().has_label(\"person\").count()");
    test_parity!(parity_out, "graph.gremlin().v().out().count()");
    test_parity!(parity_values, "graph.gremlin().v().values(\"name\").to_list()");
    // ... more parity tests
}
```

---

## 9. Example Usage

### 9.1 In-Memory Graph (Existing API)

```rust
use interstellar::rhai::RhaiEngine;
use interstellar::storage::Graph;
use std::sync::Arc;

let graph = Arc::new(Graph::new());
graph.add_vertex("person", [("name".into(), "Alice".into())].into());

let engine = RhaiEngine::new();
let names: Vec<String> = engine.eval_with_graph(graph, r#"
    let g = graph.gremlin();
    g.v().has_label("person").values("name").to_list()
"#)?;

println!("Names: {:?}", names);  // ["Alice"]
```

### 9.2 Persistent Mmap Graph (New API)

```rust
use interstellar::rhai::RhaiEngine;
use interstellar::storage::cow_mmap::CowMmapGraph;
use std::sync::Arc;

// Open or create a persistent database
let graph = Arc::new(CowMmapGraph::open("my_graph.db")?);

// Add data (persisted to disk)
graph.add_vertex("person", [("name".into(), "Alice".into())].into())?;

let engine = RhaiEngine::new();

// Execute script - same syntax as in-memory!
let names: Vec<String> = engine.eval_with_mmap_graph(graph, r#"
    let g = graph.gremlin();
    g.v().has_label("person").values("name").to_list()
"#)?;

println!("Names: {:?}", names);  // ["Alice"]
```

### 9.3 Script Reuse Across Backends

```rust
use interstellar::rhai::RhaiEngine;

let engine = RhaiEngine::new();

// Pre-compile a script once
let ast = engine.compile(r#"
    let g = graph.gremlin();
    g.v().has_label("person")
        .has_where("age", gt(25))
        .values("name")
        .to_list()
"#)?;

// Execute against in-memory graph
let inmem_graph = Arc::new(Graph::new());
// ... populate ...
let inmem_result = engine.eval_ast_with_graph(inmem_graph, &ast)?;

// Execute same compiled script against persistent graph
let mmap_graph = Arc::new(CowMmapGraph::open("data.db")?);
let mmap_result = engine.eval_ast_with_mmap_graph(mmap_graph, &ast)?;
```

---

## 10. File Structure

```
src/rhai/
├── mod.rs              # Module root, exports
├── engine.rs           # RhaiEngine with new mmap methods
├── error.rs            # Error types (add Storage variant)
├── types.rs            # Type registrations (unchanged)
├── predicates.rs       # Predicate bindings (unchanged)
├── anonymous.rs        # Anonymous traversal factory (unchanged)
├── traversal.rs        # RhaiGraph, StorageAdapter, updated types
├── steps_cow.rs        # Step application for COW graph (extracted)
└── steps_mmap.rs       # Step application for mmap graph (new, cfg-gated)

tests/rhai/
├── mod.rs
├── types.rs            # (existing)
├── predicates.rs       # (existing)
├── traversal.rs        # (existing)
├── storage_backends.rs # NEW: Backend-specific tests
├── mmap_integration.rs # NEW: Mmap integration tests
└── backend_parity.rs   # NEW: Parity tests
```

---

## 11. Implementation Phases

### Phase 1: Core Adapter Pattern (Day 1)
1. Create `StorageAdapter` enum in `traversal.rs`
2. Update `RhaiGraph` to use `StorageAdapter`
3. Add `from_graph()` and `from_mmap_graph()` constructors
4. Extract traversal execution into `execute_with_cow_graph()`

### Phase 2: Mmap Execution (Day 1-2)
1. Add `execute_with_mmap_graph()` behind `#[cfg(feature = "mmap")]`
2. Implement `apply_step_mmap()` for all traversal steps
3. Handle mutation differences (Result return types)

### Phase 3: Engine API Extensions (Day 2)
1. Add `eval_with_mmap_graph()` and related methods
2. Add `create_mmap_graph_scope()` helper
3. Update error types for storage errors

### Phase 4: Testing & Documentation (Day 2-3)
1. Write unit tests for both backends
2. Write parity tests
3. Write integration tests
4. Update examples/scripting.rs with mmap examples
5. Update module documentation

---

## 12. Success Criteria

| Criterion | Target |
|-----------|--------|
| Existing in-memory tests pass | 100% |
| Mmap graph traversal works | All steps supported |
| Script syntax identical between backends | Verified by parity tests |
| Mutations persist to disk | Verified by close/reopen test |
| Feature-gated compilation | `--features mmap` only |
| No breaking changes to existing API | Backward compatible |

---

## 13. Migration Guide

### 13.1 Existing Code (No Changes Required)

```rust
// This continues to work unchanged
let engine = RhaiEngine::new();
let graph = Arc::new(Graph::new());
engine.eval_with_graph(graph, script)?;
```

### 13.2 Adding Mmap Support

```rust
// Enable features in Cargo.toml
// interstellar = { version = "...", features = ["rhai", "mmap"] }

use interstellar::storage::cow_mmap::CowMmapGraph;

let engine = RhaiEngine::new();
let graph = Arc::new(CowMmapGraph::open("data.db")?);
engine.eval_with_mmap_graph(graph, script)?;  // New method
```

---

## 14. Future Considerations

### 14.1 Generic Storage Trait

A future enhancement could provide a fully generic API:

```rust
pub trait RhaiCompatibleStorage: Send + Sync + 'static {
    fn create_rhai_graph(self: Arc<Self>) -> RhaiGraph;
}

impl RhaiEngine {
    pub fn eval_with_storage<S: RhaiCompatibleStorage, T>(
        &self,
        storage: Arc<S>,
        script: &str,
    ) -> RhaiResult<T>;
}
```

### 14.2 Additional Backends

The adapter pattern makes it easy to add new backends:

```rust
enum StorageAdapter {
    InMemory(Arc<Graph>),
    #[cfg(feature = "mmap")]
    Mmap(Arc<CowMmapGraph>),
    #[cfg(feature = "remote")]
    Remote(Arc<RemoteGraph>),  // Future: distributed graph
}
```

### 14.3 Unified Example

Update `examples/scripting.rs` to demonstrate both backends:

```rust
fn main() {
    // In-memory example
    run_inmemory_example();
    
    // Persistent example
    #[cfg(feature = "mmap")]
    run_mmap_example();
}
```
