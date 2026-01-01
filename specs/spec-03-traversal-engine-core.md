# Spec 03: Traversal Engine Core

**Phase 3 of RustGremlin Implementation**

## Overview

This specification details the implementation of the core traversal engine - the heart of RustGremlin's Gremlin-style fluent API. Phase 3 builds on the completed Phase 1 (Core Foundation) and Phase 2 (In-Memory Storage) to deliver a functional graph query system.

The architecture uses **type-erased steps** (`Box<dyn AnyStep>`) internally while maintaining **compile-time type safety** at API boundaries through `Traversal<In, Out>`. This design enables:
- **Unified traversal type**: Same `Traversal` type for both bound and anonymous traversals
- **ExecutionContext**: Graph access passed at execution time, not construction time
- **Phase 4 compatibility**: Anonymous traversals (`__`) work seamlessly
- **Pragmatic performance**: Start with boxing, migrate hot paths to monomorphization later

**Duration**: 4-5 weeks  
**Priority**: Critical  
**Dependencies**: Phase 1 (complete), Phase 2 (complete)

---

## Goals

1. Implement core types: `Traversal<In, Out>`, `Traverser`, `Path`, `ExecutionContext`
2. Create the `AnyStep` trait with type erasure (`Box<dyn AnyStep>`)
3. Build `GraphTraversalSource` with `v()` and `e()` starting points
4. Implement essential navigation steps (`out`, `in_`, `both`, etc.)
5. Implement core filter steps (`has_label`, `has`, `has_value`, `filter`, `dedup`, `limit`)
6. Implement terminal steps (`to_list`, `next`, `count`, `iterate`)
7. Ensure lazy, pull-based evaluation throughout
8. Design for Phase 4 anonymous traversal compatibility

---

## Architecture

### Design Principles

1. **Type erasure internally, type safety externally**: Steps are stored as `Box<dyn AnyStep>` but `Traversal<In, Out>` provides compile-time checking at API boundaries
2. **ExecutionContext at runtime**: Graph access is provided when the traversal executes, not when it's constructed - this enables anonymous traversals
3. **Unified Value type**: Internally, traversers carry `Value` enum; type parameters are "phantoms" for API safety
4. **Clone-friendly steps**: Steps must be cloneable for branching operations (union, coalesce, etc.)

### Module Structure

```
src/traversal/
├── mod.rs              # Core types: Traversal, Traverser, Path, re-exports
├── context.rs          # ExecutionContext, SideEffects
├── step.rs             # AnyStep trait, step implementations
├── source.rs           # GraphTraversalSource, StartStep
├── filter.rs           # Filter steps: has_label, has, dedup, limit, etc.
├── navigation.rs       # Navigation steps: out, in_, both, outE, etc.
├── transform.rs        # Transform steps: values, id, label, map, etc.
├── terminal.rs         # Terminal steps: to_list, next, count, etc.
└── value.rs            # TraversalValue type conversions
```

### Type Relationships

```
GraphSnapshot<'g>
     │
     │ .traversal()
     ▼
GraphTraversalSource<'g>                    Anonymous Factory
     │                                            │
     │ .v() / .e()                          __::out() / __::has_label()
     ▼                                            ▼
Traversal<(), Vertex>                      Traversal<Vertex, Vertex>
     │                                            │
     │ .has_label() / .out() / etc.              │ (same Traversal type!)
     ▼                                            │
Traversal<(), Value>  ◄───────────────────────────┘
     │
     │ .to_list() / .next() (creates ExecutionContext)
     ▼
ExecutionContext<'g>
     │
     │ executes steps
     ▼
Vec<Value> / Option<Value> / etc.
```

### Key Insight: Bound vs Anonymous Traversals

Both use the **same `Traversal<In, Out>` type**. The difference:

| Aspect | Bound Traversal | Anonymous Traversal |
|--------|-----------------|---------------------|
| Creation | `g.v()` | `__.out()` |
| Has source? | Yes (`GraphTraversalSource`) | No |
| Graph access | Via source reference | Via `ExecutionContext` at execution |
| `In` type | `()` (starts from nothing) | Input element type |
| Execution | Direct (has context) | Must be spliced into parent |

---

## Deliverables

### 3.1 ExecutionContext (`src/traversal/context.rs`)

The `ExecutionContext` provides graph access at execution time, decoupling traversal construction from graph binding.

```rust
use crate::graph::GraphSnapshot;
use crate::storage::interner::StringInterner;
use std::collections::HashMap;
use std::any::Any;
use std::sync::Arc;
use parking_lot::RwLock;

/// Execution context passed to steps at runtime
/// 
/// This is the key to supporting anonymous traversals - graph access
/// is provided when the traversal executes, not when it's constructed.
pub struct ExecutionContext<'g> {
    /// Graph snapshot for consistent reads
    pub snapshot: &'g GraphSnapshot<'g>,
    /// String interner for label lookups
    pub interner: &'g StringInterner,
    /// Side effects storage (for store(), aggregate(), etc.)
    pub side_effects: SideEffects,
}

impl<'g> ExecutionContext<'g> {
    /// Create a new execution context
    pub fn new(snapshot: &'g GraphSnapshot<'g>, interner: &'g StringInterner) -> Self {
        Self {
            snapshot,
            interner,
            side_effects: SideEffects::new(),
        }
    }

    /// Resolve a label string to its interned ID
    pub fn resolve_label(&self, label: &str) -> Option<u32> {
        self.interner.get_id(label)
    }

    /// Resolve multiple labels to their interned IDs
    pub fn resolve_labels(&self, labels: &[&str]) -> Vec<u32> {
        labels
            .iter()
            .filter_map(|l| self.interner.get_id(l))
            .collect()
    }

    /// Get label string from ID
    pub fn get_label(&self, id: u32) -> Option<&str> {
        self.interner.get_str(id)
    }
}

/// Storage for traversal side effects
/// 
/// Used by steps like store(), aggregate(), sack(), etc.
#[derive(Default)]
pub struct SideEffects {
    /// Named collections of values
    collections: HashMap<String, Vec<Value>>,
    /// Arbitrary side effect data
    data: HashMap<String, Box<dyn Any + Send + Sync>>,
}

impl SideEffects {
    pub fn new() -> Self {
        Self::default()
    }

    /// Store a value in a named collection
    pub fn store(&mut self, key: &str, value: Value) {
        self.collections.entry(key.to_string()).or_default().push(value);
    }

    /// Get values from a named collection
    pub fn get(&self, key: &str) -> Option<&Vec<Value>> {
        self.collections.get(key)
    }

    /// Store arbitrary data
    pub fn set_data<T: Any + Send + Sync>(&mut self, key: &str, value: T) {
        self.data.insert(key.to_string(), Box::new(value));
    }

    /// Get arbitrary data
    pub fn get_data<T: Any>(&self, key: &str) -> Option<&T> {
        self.data.get(key).and_then(|v| v.downcast_ref())
    }
}
```

### 3.2 Core Types (`src/traversal/mod.rs`)

#### Traverser

Carries a `Value` through the pipeline with metadata. Note: internally all traversers use `Value`, not generic `E`.

```rust
use std::any::Any;
use smallvec::SmallVec;

/// Traverser carries a Value through the pipeline with metadata
/// 
/// Unlike the monomorphic design, we use a single concrete type
/// with `Value` to enable type erasure in steps.
#[derive(Clone)]
pub struct Traverser {
    /// The current element (always a Value)
    pub value: Value,
    /// Path history
    pub path: Path,
    /// Loop counter for repeat()
    pub loops: u32,
    /// Optional sack value (for future use)
    pub sack: Option<Box<dyn CloneSack>>,
    /// Bulk count (optimization for identical traversers)
    pub bulk: u64,
}

/// Trait for clonable sack values
pub trait CloneSack: Any + Send {
    fn clone_box(&self) -> Box<dyn CloneSack>;
    fn as_any(&self) -> &dyn Any;
}

impl<T: Clone + Any + Send> CloneSack for T {
    fn clone_box(&self) -> Box<dyn CloneSack> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Clone for Box<dyn CloneSack> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl Traverser {
    /// Create a new traverser with default metadata
    pub fn new(value: impl Into<Value>) -> Self {
        Self {
            value: value.into(),
            path: Path::default(),
            loops: 0,
            sack: None,
            bulk: 1,
        }
    }

    /// Create traverser for a vertex
    pub fn from_vertex(id: VertexId) -> Self {
        Self::new(Value::Vertex(id))
    }

    /// Create traverser for an edge
    pub fn from_edge(id: EdgeId) -> Self {
        Self::new(Value::Edge(id))
    }

    /// Split traverser for branching (preserves path and metadata)
    pub fn split(&self, new_value: impl Into<Value>) -> Traverser {
        Traverser {
            value: new_value.into(),
            path: self.path.clone(),
            loops: self.loops,
            sack: self.sack.clone(),
            bulk: self.bulk,
        }
    }

    /// Replace the value while preserving metadata
    pub fn with_value(self, new_value: impl Into<Value>) -> Traverser {
        Traverser {
            value: new_value.into(),
            path: self.path,
            loops: self.loops,
            sack: self.sack,
            bulk: self.bulk,
        }
    }

    /// Increment loop counter
    pub fn inc_loops(&mut self) {
        self.loops += 1;
    }

    /// Extend path with current value
    pub fn extend_path(&mut self, labels: &[String]) {
        let path_value = PathValue::from(&self.value);
        self.path.push(path_value, labels);
    }

    /// Get the value as a vertex ID (if it is one)
    pub fn as_vertex_id(&self) -> Option<VertexId> {
        match &self.value {
            Value::Vertex(id) => Some(*id),
            _ => None,
        }
    }

    /// Get the value as an edge ID (if it is one)
    pub fn as_edge_id(&self) -> Option<EdgeId> {
        match &self.value {
            Value::Edge(id) => Some(*id),
            _ => None,
        }
    }
}
```

#### Path

Tracks traversal history (unchanged from original design):

```rust
use std::collections::HashMap;

/// Path tracks traversal history
#[derive(Clone, Default, Debug)]
pub struct Path {
    /// Ordered list of path elements
    objects: Vec<PathElement>,
    /// Label to indices mapping
    labels: HashMap<String, Vec<usize>>,
}

/// A single element in the path
#[derive(Clone, Debug)]
pub struct PathElement {
    pub value: PathValue,
    pub labels: SmallVec<[String; 2]>,
}

/// Values that can be stored in a path
#[derive(Clone, Debug)]
pub enum PathValue {
    Vertex(VertexId),
    Edge(EdgeId),
    Property(Value),
}

impl From<&Value> for PathValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::Vertex(id) => PathValue::Vertex(*id),
            Value::Edge(id) => PathValue::Edge(*id),
            other => PathValue::Property(other.clone()),
        }
    }
}

impl Path {
    /// Push a new element onto the path
    pub fn push(&mut self, value: PathValue, labels: &[String]) {
        let idx = self.objects.len();
        for label in labels {
            self.labels
                .entry(label.clone())
                .or_default()
                .push(idx);
        }
        self.objects.push(PathElement {
            value,
            labels: labels.iter().cloned().collect(),
        });
    }

    /// Get elements by label
    pub fn get(&self, label: &str) -> Option<Vec<&PathValue>> {
        self.labels.get(label).map(|indices| {
            indices.iter().map(|&i| &self.objects[i].value).collect()
        })
    }

    /// Get all objects in order
    pub fn objects(&self) -> impl Iterator<Item = &PathValue> {
        self.objects.iter().map(|e| &e.value)
    }

    /// Check if path contains a vertex (for cycle detection)
    pub fn contains_vertex(&self, id: VertexId) -> bool {
        self.objects.iter().any(|e| matches!(&e.value, PathValue::Vertex(v) if *v == id))
    }

    /// Length of the path
    pub fn len(&self) -> usize {
        self.objects.len()
    }

    /// Check if path is empty
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }
}
```

#### Traversal (Type-Erased Design)

The `Traversal` struct uses `Box<dyn AnyStep>` internally but maintains compile-time type safety through phantom type parameters:

```rust
use std::marker::PhantomData;

/// Main traversal type - type-erased internally, type-safe externally
/// 
/// # Type Parameters
/// - `In`: The input type this traversal expects (phantom)
/// - `Out`: The output type this traversal produces (phantom)
/// 
/// Both parameters are "phantom" - used only for compile-time checking.
/// Internally, all values flow as `Value` enum through `Box<dyn AnyStep>`.
/// 
/// # Design Notes
/// - Same type for bound and anonymous traversals
/// - Steps are stored as `Vec<Box<dyn AnyStep>>` for type erasure
/// - `In = ()` for traversals that start from a source (bound)
/// - `In = SomeType` for traversals that expect input (anonymous)
pub struct Traversal<In, Out> {
    /// The steps in this traversal (type-erased)
    steps: Vec<Box<dyn AnyStep>>,
    /// Optional reference to source (for bound traversals)
    source: Option<TraversalSource>,
    /// Phantom data for input/output types
    _phantom: PhantomData<fn(In) -> Out>,
}

/// Source information for bound traversals
#[derive(Clone)]
pub(crate) enum TraversalSource {
    /// Start from all vertices
    AllVertices,
    /// Start from specific vertex IDs
    Vertices(Vec<VertexId>),
    /// Start from all edges
    AllEdges,
    /// Start from specific edge IDs
    Edges(Vec<EdgeId>),
    /// Inject arbitrary values
    Inject(Vec<Value>),
}

impl<In, Out> Clone for Traversal<In, Out> {
    fn clone(&self) -> Self {
        Self {
            steps: self.steps.iter().map(|s| s.clone_box()).collect(),
            source: self.source.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<In, Out> Traversal<In, Out> {
    /// Create a new empty traversal (for anonymous traversals)
    pub fn new() -> Self {
        Self {
            steps: Vec::new(),
            source: None,
            _phantom: PhantomData,
        }
    }

    /// Create a traversal with a source (for bound traversals)
    pub(crate) fn with_source(source: TraversalSource) -> Self {
        Self {
            steps: Vec::new(),
            source: Some(source),
            _phantom: PhantomData,
        }
    }

    /// Add a step to the traversal, returning a new traversal with updated output type
    pub fn add_step<NewOut>(mut self, step: impl AnyStep + 'static) -> Traversal<In, NewOut> {
        self.steps.push(Box::new(step));
        Traversal {
            steps: self.steps,
            source: self.source,
            _phantom: PhantomData,
        }
    }

    /// Append another traversal's steps to this one
    pub fn append<Mid>(mut self, other: Traversal<Out, Mid>) -> Traversal<In, Mid> {
        self.steps.extend(other.steps);
        Traversal {
            steps: self.steps,
            source: self.source,
            _phantom: PhantomData,
        }
    }

    /// Get the steps (for execution)
    pub(crate) fn into_steps(self) -> (Option<TraversalSource>, Vec<Box<dyn AnyStep>>) {
        (self.source, self.steps)
    }
}

impl<In, Out> Default for Traversal<In, Out> {
    fn default() -> Self {
        Self::new()
    }
}
```
```

### 3.3 AnyStep Trait (`src/traversal/step.rs`)

The `AnyStep` trait provides type-erased step execution. All steps implement this trait.

```rust
use crate::traversal::{ExecutionContext, Traverser};

/// Type-erased step trait
/// 
/// This is the core abstraction that enables:
/// - Storing heterogeneous steps in `Vec<Box<dyn AnyStep>>`
/// - Anonymous traversals without graph binding at construction
/// - Cloning traversals for branching operations
/// 
/// # Design Notes
/// - Input and output are both `Iterator<Item = Traverser>` (using Value internally)
/// - Steps receive `ExecutionContext` to access graph data
/// - Steps must be cloneable (`clone_box`) for traversal cloning
pub trait AnyStep: Send + Sync {
    /// Apply this step to input traversers, producing output traversers
    /// 
    /// The returned iterator is boxed to enable type erasure.
    /// Steps that need graph access use the provided `ExecutionContext`.
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a>;

    /// Clone this step into a boxed trait object
    fn clone_box(&self) -> Box<dyn AnyStep>;

    /// Get step name for debugging
    fn name(&self) -> &'static str;
}

// Enable cloning of Box<dyn AnyStep>
impl Clone for Box<dyn AnyStep> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Helper macro to implement AnyStep for simple filter steps
#[macro_export]
macro_rules! impl_filter_step {
    ($step:ty, $name:literal) => {
        impl AnyStep for $step {
            fn apply<'a>(
                &'a self,
                ctx: &'a ExecutionContext<'a>,
                input: Box<dyn Iterator<Item = Traverser> + 'a>,
            ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
                let step = self.clone();
                Box::new(input.filter(move |t| step.matches(ctx, t)))
            }

            fn clone_box(&self) -> Box<dyn AnyStep> {
                Box::new(self.clone())
            }

            fn name(&self) -> &'static str {
                $name
            }
        }
    };
}

/// Helper macro to implement AnyStep for flatmap steps (1:N mappings)
#[macro_export]
macro_rules! impl_flatmap_step {
    ($step:ty, $name:literal) => {
        impl AnyStep for $step {
            fn apply<'a>(
                &'a self,
                ctx: &'a ExecutionContext<'a>,
                input: Box<dyn Iterator<Item = Traverser> + 'a>,
            ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
                let step = self.clone();
                Box::new(input.flat_map(move |t| step.expand(ctx, t)))
            }

            fn clone_box(&self) -> Box<dyn AnyStep> {
                Box::new(self.clone())
            }

            fn name(&self) -> &'static str {
                $name
            }
        }
    };
}
```

#### Example Step Implementations

```rust
/// Identity step - passes input through unchanged
#[derive(Clone, Copy)]
pub struct IdentityStep;

impl AnyStep for IdentityStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        input // Pass through unchanged
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(*self)
    }

    fn name(&self) -> &'static str {
        "identity"
    }
}

/// Start step - produces initial traversers from source
#[derive(Clone)]
pub struct StartStep {
    pub source: TraversalSource,
}

impl AnyStep for StartStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        _input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        match &self.source {
            TraversalSource::AllVertices => {
                Box::new(
                    ctx.snapshot
                        .vertices()
                        .map(|v| Traverser::from_vertex(v.id()))
                )
            }
            TraversalSource::Vertices(ids) => {
                let ids = ids.clone();
                Box::new(
                    ids.into_iter()
                        .filter_map(|id| ctx.snapshot.get_vertex(id).map(|_| Traverser::from_vertex(id)))
                )
            }
            TraversalSource::AllEdges => {
                Box::new(
                    ctx.snapshot
                        .edges()
                        .map(|e| Traverser::from_edge(e.id()))
                )
            }
            TraversalSource::Edges(ids) => {
                let ids = ids.clone();
                Box::new(
                    ids.into_iter()
                        .filter_map(|id| ctx.snapshot.get_edge(id).map(|_| Traverser::from_edge(id)))
                )
            }
            TraversalSource::Inject(values) => {
                let values = values.clone();
                Box::new(values.into_iter().map(Traverser::new))
            }
        }
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "start"
    }
}
```
```

### 3.4 GraphTraversalSource (`src/traversal/source.rs`)

The entry point for bound traversals. Holds a reference to the graph and creates traversals with source information.

```rust
use crate::graph::GraphSnapshot;
use crate::storage::interner::StringInterner;
use crate::value::{VertexId, EdgeId, Value};

/// Entry point for all bound traversals
/// 
/// Created from a GraphSnapshot via `snapshot.traversal()`.
/// The source holds references needed to create ExecutionContext at execution time.
pub struct GraphTraversalSource<'g> {
    snapshot: &'g GraphSnapshot<'g>,
    interner: &'g StringInterner,
}

impl<'g> GraphTraversalSource<'g> {
    /// Create a new traversal source from a snapshot
    pub fn new(snapshot: &'g GraphSnapshot<'g>, interner: &'g StringInterner) -> Self {
        Self { snapshot, interner }
    }

    /// Start traversal from all vertices
    pub fn v(&self) -> BoundTraversal<'g, (), Value> {
        BoundTraversal::new(
            self.snapshot,
            self.interner,
            Traversal::with_source(TraversalSource::AllVertices),
        )
    }

    /// Start traversal from specific vertex IDs
    pub fn v_ids<I>(&self, ids: I) -> BoundTraversal<'g, (), Value>
    where
        I: IntoIterator<Item = VertexId>,
    {
        BoundTraversal::new(
            self.snapshot,
            self.interner,
            Traversal::with_source(TraversalSource::Vertices(ids.into_iter().collect())),
        )
    }

    /// Start traversal from all edges
    pub fn e(&self) -> BoundTraversal<'g, (), Value> {
        BoundTraversal::new(
            self.snapshot,
            self.interner,
            Traversal::with_source(TraversalSource::AllEdges),
        )
    }

    /// Start traversal from specific edge IDs
    pub fn e_ids<I>(&self, ids: I) -> BoundTraversal<'g, (), Value>
    where
        I: IntoIterator<Item = EdgeId>,
    {
        BoundTraversal::new(
            self.snapshot,
            self.interner,
            Traversal::with_source(TraversalSource::Edges(ids.into_iter().collect())),
        )
    }

    /// Inject arbitrary values into traversal
    pub fn inject<T, I>(&self, values: I) -> BoundTraversal<'g, (), Value>
    where
        I: IntoIterator<Item = T>,
        T: Into<Value>,
    {
        let values: Vec<Value> = values.into_iter().map(Into::into).collect();
        BoundTraversal::new(
            self.snapshot,
            self.interner,
            Traversal::with_source(TraversalSource::Inject(values)),
        )
    }
}

/// A traversal bound to a graph source
/// 
/// This wrapper holds both the traversal and the graph references
/// needed to create an ExecutionContext when terminal steps are called.
pub struct BoundTraversal<'g, In, Out> {
    snapshot: &'g GraphSnapshot<'g>,
    interner: &'g StringInterner,
    traversal: Traversal<In, Out>,
}

impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    pub(crate) fn new(
        snapshot: &'g GraphSnapshot<'g>,
        interner: &'g StringInterner,
        traversal: Traversal<In, Out>,
    ) -> Self {
        Self {
            snapshot,
            interner,
            traversal,
        }
    }

    /// Add a step to the traversal
    pub fn add_step<NewOut>(self, step: impl AnyStep + 'static) -> BoundTraversal<'g, In, NewOut> {
        BoundTraversal {
            snapshot: self.snapshot,
            interner: self.interner,
            traversal: self.traversal.add_step(step),
        }
    }

    /// Append an anonymous traversal
    pub fn append<Mid>(self, anon: Traversal<Out, Mid>) -> BoundTraversal<'g, In, Mid> {
        BoundTraversal {
            snapshot: self.snapshot,
            interner: self.interner,
            traversal: self.traversal.append(anon),
        }
    }

    /// Create execution context for this traversal
    fn create_context(&self) -> ExecutionContext<'g> {
        ExecutionContext::new(self.snapshot, self.interner)
    }

    /// Execute the traversal and return an iterator
    pub fn execute(self) -> impl Iterator<Item = Traverser> + 'g {
        let ctx = self.create_context();
        let (source, steps) = self.traversal.into_steps();
        
        // Start with source traversers
        let mut current: Box<dyn Iterator<Item = Traverser> + 'g> = match source {
            Some(src) => {
                let start_step = StartStep { source: src };
                start_step.apply(&ctx, Box::new(std::iter::empty()))
            }
            None => Box::new(std::iter::empty()),
        };

        // Apply each step in sequence
        for step in steps {
            current = step.apply(&ctx, current);
        }

        current
    }

    /// Get reference to interner for label resolution
    pub(crate) fn interner(&self) -> &StringInterner {
        self.interner
    }
}

impl<'g, In, Out: Clone> Clone for BoundTraversal<'g, In, Out> {
    fn clone(&self) -> Self {
        Self {
            snapshot: self.snapshot,
            interner: self.interner,
            traversal: self.traversal.clone(),
        }
    }
}
```

#### Integration with GraphSnapshot

```rust
impl<'g> GraphSnapshot<'g> {
    /// Create a traversal source for this snapshot
    pub fn traversal(&self) -> GraphTraversalSource<'_> {
        GraphTraversalSource::new(self, self.interner())
    }
}
```

### 3.5 Filter Steps (`src/traversal/filter.rs`)

Filter steps check conditions and pass through or reject traversers.

```rust
use std::collections::HashSet;
use std::hash::Hash;

/// Filter by vertex/edge label
#[derive(Clone)]
pub struct HasLabelStep {
    /// Labels to match (empty = match all)
    labels: Vec<String>,
}

impl HasLabelStep {
    pub fn new(labels: Vec<String>) -> Self {
        Self { labels }
    }

    pub fn single(label: impl Into<String>) -> Self {
        Self { labels: vec![label.into()] }
    }

    fn matches(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        if self.labels.is_empty() {
            return true;
        }

        // Resolve label IDs at execution time
        let label_ids: Vec<u32> = self.labels
            .iter()
            .filter_map(|l| ctx.resolve_label(l))
            .collect();

        match &traverser.value {
            Value::Vertex(id) => {
                if let Some(v) = ctx.snapshot.get_vertex(*id) {
                    label_ids.contains(&v.label_id())
                } else {
                    false
                }
            }
            Value::Edge(id) => {
                if let Some(e) = ctx.snapshot.get_edge(*id) {
                    label_ids.contains(&e.label_id())
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

impl AnyStep for HasLabelStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let step = self.clone();
        Box::new(input.filter(move |t| step.matches(ctx, t)))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "hasLabel"
    }
}

/// Filter by property existence
#[derive(Clone)]
pub struct HasStep {
    key: String,
}

impl HasStep {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }

    fn matches(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Vertex(id) => {
                ctx.snapshot
                    .get_vertex(*id)
                    .map(|v| v.property(&self.key).is_some())
                    .unwrap_or(false)
            }
            Value::Edge(id) => {
                ctx.snapshot
                    .get_edge(*id)
                    .map(|e| e.property(&self.key).is_some())
                    .unwrap_or(false)
            }
            _ => false,
        }
    }
}

impl_filter_step!(HasStep, "has");

/// Filter by property value equality
#[derive(Clone)]
pub struct HasValueStep {
    key: String,
    value: Value,
}

impl HasValueStep {
    pub fn new(key: impl Into<String>, value: impl Into<Value>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    fn matches(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Vertex(id) => {
                ctx.snapshot
                    .get_vertex(*id)
                    .and_then(|v| v.property(&self.key))
                    .map(|pv| pv == &self.value)
                    .unwrap_or(false)
            }
            Value::Edge(id) => {
                ctx.snapshot
                    .get_edge(*id)
                    .and_then(|e| e.property(&self.key))
                    .map(|pv| pv == &self.value)
                    .unwrap_or(false)
            }
            _ => false,
        }
    }
}

impl_filter_step!(HasValueStep, "has");

/// Filter by arbitrary predicate on Value
#[derive(Clone)]
pub struct FilterStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> bool + Clone + Send + Sync,
{
    predicate: F,
}

impl<F> FilterStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> bool + Clone + Send + Sync,
{
    pub fn new(predicate: F) -> Self {
        Self { predicate }
    }
}

impl<F> AnyStep for FilterStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> bool + Clone + Send + Sync + 'static,
{
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let predicate = self.predicate.clone();
        Box::new(input.filter(move |t| predicate(ctx, &t.value)))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "filter"
    }
}

/// Deduplicate traversers by value
#[derive(Clone)]
pub struct DedupStep;

impl AnyStep for DedupStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Use a stateful iterator with HashSet
        let mut seen = HashSet::new();
        Box::new(input.filter(move |t| {
            // Hash based on Value
            let key = t.value.clone();
            seen.insert(key)
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(DedupStep)
    }

    fn name(&self) -> &'static str {
        "dedup"
    }
}

/// Limit number of results
#[derive(Clone, Copy)]
pub struct LimitStep {
    n: usize,
}

impl LimitStep {
    pub fn new(n: usize) -> Self {
        Self { n }
    }
}

impl AnyStep for LimitStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.take(self.n))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(*self)
    }

    fn name(&self) -> &'static str {
        "limit"
    }
}

/// Skip first n results
#[derive(Clone, Copy)]
pub struct SkipStep {
    n: usize,
}

impl SkipStep {
    pub fn new(n: usize) -> Self {
        Self { n }
    }
}

impl AnyStep for SkipStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.skip(self.n))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(*self)
    }

    fn name(&self) -> &'static str {
        "skip"
    }
}

/// Range of results [start, end)
#[derive(Clone, Copy)]
pub struct RangeStep {
    start: usize,
    end: usize,
}

impl RangeStep {
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }
}

impl AnyStep for RangeStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.skip(self.start).take(self.end - self.start))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(*self)
    }

    fn name(&self) -> &'static str {
        "range"
    }
}

/// Filter by vertex/edge ID
#[derive(Clone)]
pub struct HasIdStep {
    ids: Vec<Value>, // Can be VertexId or EdgeId wrapped in Value
}

impl HasIdStep {
    pub fn vertex(id: VertexId) -> Self {
        Self { ids: vec![Value::Vertex(id)] }
    }

    pub fn edge(id: EdgeId) -> Self {
        Self { ids: vec![Value::Edge(id)] }
    }

    pub fn vertices(ids: Vec<VertexId>) -> Self {
        Self { ids: ids.into_iter().map(Value::Vertex).collect() }
    }

    fn matches(&self, _ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        self.ids.contains(&traverser.value)
    }
}

impl_filter_step!(HasIdStep, "hasId");
```

#### Traversal Builder Methods for Filters

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Filter vertices/edges by label
    pub fn has_label(self, label: &str) -> BoundTraversal<'g, In, Out> {
        self.add_step(HasLabelStep::single(label))
    }

    /// Filter vertices/edges by any of the given labels
    pub fn has_label_any(self, labels: &[&str]) -> BoundTraversal<'g, In, Out> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(HasLabelStep::new(labels))
    }

    /// Filter by property existence
    pub fn has(self, key: &str) -> BoundTraversal<'g, In, Out> {
        self.add_step(HasStep::new(key))
    }

    /// Filter by property value
    pub fn has_value(self, key: &str, value: impl Into<Value>) -> BoundTraversal<'g, In, Out> {
        self.add_step(HasValueStep::new(key, value))
    }

    /// Filter by arbitrary predicate
    pub fn filter<F>(self, predicate: F) -> BoundTraversal<'g, In, Out>
    where
        F: Fn(&ExecutionContext, &Value) -> bool + Clone + Send + Sync + 'static,
    {
        self.add_step(FilterStep::new(predicate))
    }

    /// Deduplicate by value
    pub fn dedup(self) -> BoundTraversal<'g, In, Out> {
        self.add_step(DedupStep)
    }

    /// Limit number of results
    pub fn limit(self, n: usize) -> BoundTraversal<'g, In, Out> {
        self.add_step(LimitStep::new(n))
    }

    /// Skip first n results
    pub fn skip(self, n: usize) -> BoundTraversal<'g, In, Out> {
        self.add_step(SkipStep::new(n))
    }

    /// Get results in range [start, end)
    pub fn range(self, start: usize, end: usize) -> BoundTraversal<'g, In, Out> {
        self.add_step(RangeStep::new(start, end))
    }
}
```

### 3.6 Navigation Steps (`src/traversal/navigation.rs`)

Navigation steps traverse the graph structure, expanding from vertices to edges or adjacent vertices.

```rust
/// Traverse to outgoing adjacent vertices
#[derive(Clone)]
pub struct OutStep {
    /// Optional edge labels to filter by
    labels: Vec<String>,
}

impl OutStep {
    pub fn new() -> Self {
        Self { labels: Vec::new() }
    }

    pub fn with_labels(labels: Vec<String>) -> Self {
        Self { labels }
    }

    fn expand<'a>(&self, ctx: &'a ExecutionContext<'a>, traverser: Traverser) -> impl Iterator<Item = Traverser> + 'a {
        let vertex_id = match traverser.as_vertex_id() {
            Some(id) => id,
            None => return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Traverser>>,
        };

        let label_ids: Vec<u32> = if self.labels.is_empty() {
            Vec::new()
        } else {
            ctx.resolve_labels(&self.labels.iter().map(|s| s.as_str()).collect::<Vec<_>>())
        };

        let edges = ctx.snapshot.out_edges(vertex_id);
        let traverser = traverser.clone();
        
        Box::new(edges.filter_map(move |edge| {
            // Filter by label if specified
            if !label_ids.is_empty() && !label_ids.contains(&edge.label_id()) {
                return None;
            }
            // Get target vertex
            let dst_id = edge.dst();
            Some(traverser.split(Value::Vertex(dst_id)))
        }))
    }
}

impl AnyStep for OutStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let step = self.clone();
        Box::new(input.flat_map(move |t| step.expand(ctx, t)))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "out"
    }
}

/// Traverse to incoming adjacent vertices
#[derive(Clone)]
pub struct InStep {
    labels: Vec<String>,
}

impl InStep {
    pub fn new() -> Self {
        Self { labels: Vec::new() }
    }

    pub fn with_labels(labels: Vec<String>) -> Self {
        Self { labels }
    }

    fn expand<'a>(&self, ctx: &'a ExecutionContext<'a>, traverser: Traverser) -> impl Iterator<Item = Traverser> + 'a {
        let vertex_id = match traverser.as_vertex_id() {
            Some(id) => id,
            None => return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Traverser>>,
        };

        let label_ids: Vec<u32> = if self.labels.is_empty() {
            Vec::new()
        } else {
            ctx.resolve_labels(&self.labels.iter().map(|s| s.as_str()).collect::<Vec<_>>())
        };

        let edges = ctx.snapshot.in_edges(vertex_id);
        let traverser = traverser.clone();
        
        Box::new(edges.filter_map(move |edge| {
            if !label_ids.is_empty() && !label_ids.contains(&edge.label_id()) {
                return None;
            }
            let src_id = edge.src();
            Some(traverser.split(Value::Vertex(src_id)))
        }))
    }
}

impl AnyStep for InStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let step = self.clone();
        Box::new(input.flat_map(move |t| step.expand(ctx, t)))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "in"
    }
}

/// Traverse both directions
#[derive(Clone)]
pub struct BothStep {
    labels: Vec<String>,
}

impl BothStep {
    pub fn new() -> Self {
        Self { labels: Vec::new() }
    }

    pub fn with_labels(labels: Vec<String>) -> Self {
        Self { labels }
    }
}

impl AnyStep for BothStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Combine out and in steps
        let out_step = OutStep::with_labels(self.labels.clone());
        let in_step = InStep::with_labels(self.labels.clone());
        
        Box::new(input.flat_map(move |t| {
            let out_iter = out_step.expand(ctx, t.clone());
            let in_iter = in_step.expand(ctx, t);
            out_iter.chain(in_iter)
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "both"
    }
}

/// Traverse to outgoing edges
#[derive(Clone)]
pub struct OutEStep {
    labels: Vec<String>,
}

impl OutEStep {
    pub fn new() -> Self {
        Self { labels: Vec::new() }
    }

    pub fn with_labels(labels: Vec<String>) -> Self {
        Self { labels }
    }
}

impl AnyStep for OutEStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let labels = self.labels.clone();
        Box::new(input.flat_map(move |t| {
            let vertex_id = match t.as_vertex_id() {
                Some(id) => id,
                None => return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Traverser>>,
            };

            let label_ids: Vec<u32> = if labels.is_empty() {
                Vec::new()
            } else {
                ctx.resolve_labels(&labels.iter().map(|s| s.as_str()).collect::<Vec<_>>())
            };

            let edges = ctx.snapshot.out_edges(vertex_id);
            Box::new(edges.filter_map(move |edge| {
                if !label_ids.is_empty() && !label_ids.contains(&edge.label_id()) {
                    return None;
                }
                Some(t.split(Value::Edge(edge.id())))
            }))
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "outE"
    }
}

/// Traverse to incoming edges
#[derive(Clone)]
pub struct InEStep {
    labels: Vec<String>,
}

impl InEStep {
    pub fn new() -> Self {
        Self { labels: Vec::new() }
    }

    pub fn with_labels(labels: Vec<String>) -> Self {
        Self { labels }
    }
}

impl AnyStep for InEStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let labels = self.labels.clone();
        Box::new(input.flat_map(move |t| {
            let vertex_id = match t.as_vertex_id() {
                Some(id) => id,
                None => return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Traverser>>,
            };

            let label_ids: Vec<u32> = if labels.is_empty() {
                Vec::new()
            } else {
                ctx.resolve_labels(&labels.iter().map(|s| s.as_str()).collect::<Vec<_>>())
            };

            let edges = ctx.snapshot.in_edges(vertex_id);
            Box::new(edges.filter_map(move |edge| {
                if !label_ids.is_empty() && !label_ids.contains(&edge.label_id()) {
                    return None;
                }
                Some(t.split(Value::Edge(edge.id())))
            }))
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "inE"
    }
}

/// Traverse to both incident edges
#[derive(Clone)]
pub struct BothEStep {
    labels: Vec<String>,
}

impl BothEStep {
    pub fn new() -> Self {
        Self { labels: Vec::new() }
    }

    pub fn with_labels(labels: Vec<String>) -> Self {
        Self { labels }
    }
}

/// Get source vertex of edge
#[derive(Clone, Copy)]
pub struct OutVStep;

impl AnyStep for OutVStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.filter_map(move |t| {
            let edge_id = t.as_edge_id()?;
            let edge = ctx.snapshot.get_edge(edge_id)?;
            Some(t.split(Value::Vertex(edge.src())))
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(*self)
    }

    fn name(&self) -> &'static str {
        "outV"
    }
}

/// Get target vertex of edge
#[derive(Clone, Copy)]
pub struct InVStep;

impl AnyStep for InVStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.filter_map(move |t| {
            let edge_id = t.as_edge_id()?;
            let edge = ctx.snapshot.get_edge(edge_id)?;
            Some(t.split(Value::Vertex(edge.dst())))
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(*self)
    }

    fn name(&self) -> &'static str {
        "inV"
    }
}

/// Get both vertices of edge
#[derive(Clone, Copy)]
pub struct BothVStep;

impl AnyStep for BothVStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.flat_map(move |t| {
            let edge_id = match t.as_edge_id() {
                Some(id) => id,
                None => return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Traverser>>,
            };
            
            match ctx.snapshot.get_edge(edge_id) {
                Some(edge) => {
                    let src = t.split(Value::Vertex(edge.src()));
                    let dst = t.split(Value::Vertex(edge.dst()));
                    Box::new([src, dst].into_iter()) as Box<dyn Iterator<Item = Traverser>>
                }
                None => Box::new(std::iter::empty()),
            }
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(*self)
    }

    fn name(&self) -> &'static str {
        "bothV"
    }
}
```

#### Traversal Builder Methods for Navigation

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Traverse to outgoing adjacent vertices
    pub fn out(self) -> BoundTraversal<'g, In, Value> {
        self.add_step(OutStep::new())
    }

    /// Traverse to outgoing adjacent vertices via edges with given labels
    pub fn out_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(OutStep::with_labels(labels))
    }

    /// Traverse to incoming adjacent vertices
    pub fn in_(self) -> BoundTraversal<'g, In, Value> {
        self.add_step(InStep::new())
    }

    /// Traverse to incoming adjacent vertices via edges with given labels
    pub fn in_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(InStep::with_labels(labels))
    }

    /// Traverse both directions
    pub fn both(self) -> BoundTraversal<'g, In, Value> {
        self.add_step(BothStep::new())
    }

    /// Traverse both directions via edges with given labels
    pub fn both_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(BothStep::with_labels(labels))
    }

    /// Traverse to outgoing edges
    pub fn out_e(self) -> BoundTraversal<'g, In, Value> {
        self.add_step(OutEStep::new())
    }

    /// Traverse to outgoing edges with given labels
    pub fn out_e_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(OutEStep::with_labels(labels))
    }

    /// Traverse to incoming edges
    pub fn in_e(self) -> BoundTraversal<'g, In, Value> {
        self.add_step(InEStep::new())
    }

    /// Traverse to incoming edges with given labels
    pub fn in_e_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(InEStep::with_labels(labels))
    }

    /// Traverse to all incident edges
    pub fn both_e(self) -> BoundTraversal<'g, In, Value> {
        self.add_step(BothEStep::new())
    }

    /// Traverse to all incident edges with given labels
    pub fn both_e_labels(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(BothEStep::with_labels(labels))
    }

    /// Get source vertex of edge
    pub fn out_v(self) -> BoundTraversal<'g, In, Value> {
        self.add_step(OutVStep)
    }

    /// Get target vertex of edge
    pub fn in_v(self) -> BoundTraversal<'g, In, Value> {
        self.add_step(InVStep)
    }

    /// Get both vertices of edge
    pub fn both_v(self) -> BoundTraversal<'g, In, Value> {
        self.add_step(BothVStep)
    }
}
```

### 3.7 Transform Steps (`src/traversal/transform.rs`)

Transform steps map values to different types.

```rust
/// Extract property value(s)
#[derive(Clone)]
pub struct ValuesStep {
    keys: Vec<String>,
}

impl ValuesStep {
    pub fn new(key: impl Into<String>) -> Self {
        Self { keys: vec![key.into()] }
    }

    pub fn multi(keys: Vec<String>) -> Self {
        Self { keys }
    }
}

impl AnyStep for ValuesStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let keys = self.keys.clone();
        Box::new(input.flat_map(move |t| {
            let props: Vec<Value> = match &t.value {
                Value::Vertex(id) => {
                    ctx.snapshot
                        .get_vertex(*id)
                        .map(|v| {
                            keys.iter()
                                .filter_map(|k| v.property(k).cloned())
                                .collect()
                        })
                        .unwrap_or_default()
                }
                Value::Edge(id) => {
                    ctx.snapshot
                        .get_edge(*id)
                        .map(|e| {
                            keys.iter()
                                .filter_map(|k| e.property(k).cloned())
                                .collect()
                        })
                        .unwrap_or_default()
                }
                _ => Vec::new(),
            };
            props.into_iter().map(move |v| t.split(v))
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "values"
    }
}

/// Get element ID
#[derive(Clone, Copy)]
pub struct IdStep;

impl AnyStep for IdStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.map(|t| {
            let id_value = match &t.value {
                Value::Vertex(id) => Value::Int(id.0 as i64),
                Value::Edge(id) => Value::Int(id.0 as i64),
                _ => t.value.clone(),
            };
            t.with_value(id_value)
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(*self)
    }

    fn name(&self) -> &'static str {
        "id"
    }
}

/// Get element label
#[derive(Clone, Copy)]
pub struct LabelStep;

impl AnyStep for LabelStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.filter_map(move |t| {
            let label = match &t.value {
                Value::Vertex(id) => {
                    let v = ctx.snapshot.get_vertex(*id)?;
                    ctx.get_label(v.label_id())?
                }
                Value::Edge(id) => {
                    let e = ctx.snapshot.get_edge(*id)?;
                    ctx.get_label(e.label_id())?
                }
                _ => return None,
            };
            Some(t.with_value(Value::String(label.to_string())))
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(*self)
    }

    fn name(&self) -> &'static str {
        "label"
    }
}

/// Map with closure
#[derive(Clone)]
pub struct MapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Value + Clone + Send + Sync,
{
    f: F,
}

impl<F> MapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Value + Clone + Send + Sync,
{
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F> AnyStep for MapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Value + Clone + Send + Sync + 'static,
{
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let f = self.f.clone();
        Box::new(input.map(move |t| {
            let new_value = f(ctx, &t.value);
            t.with_value(new_value)
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "map"
    }
}

/// FlatMap with closure
#[derive(Clone)]
pub struct FlatMapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Vec<Value> + Clone + Send + Sync,
{
    f: F,
}

impl<F> FlatMapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Vec<Value> + Clone + Send + Sync,
{
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F> AnyStep for FlatMapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Vec<Value> + Clone + Send + Sync + 'static,
{
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let f = self.f.clone();
        Box::new(input.flat_map(move |t| {
            let values = f(ctx, &t.value);
            values.into_iter().map(move |v| t.split(v))
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "flatMap"
    }
}

/// Emit constant value
#[derive(Clone)]
pub struct ConstantStep {
    value: Value,
}

impl ConstantStep {
    pub fn new(value: impl Into<Value>) -> Self {
        Self { value: value.into() }
    }
}

impl AnyStep for ConstantStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let value = self.value.clone();
        Box::new(input.map(move |t| t.with_value(value.clone())))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "constant"
    }
}

/// Get traversal path
#[derive(Clone, Copy)]
pub struct PathStep;

impl AnyStep for PathStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.map(|t| {
            // Convert path to a Value (list of path values)
            let path_values: Vec<Value> = t.path
                .objects()
                .map(|pv| match pv {
                    PathValue::Vertex(id) => Value::Vertex(*id),
                    PathValue::Edge(id) => Value::Edge(*id),
                    PathValue::Property(v) => v.clone(),
                })
                .collect();
            t.with_value(Value::List(path_values))
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(*self)
    }

    fn name(&self) -> &'static str {
        "path"
    }
}
```

#### Traversal Builder Methods for Transforms

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Extract property value
    pub fn values(self, key: &str) -> BoundTraversal<'g, In, Value> {
        self.add_step(ValuesStep::new(key))
    }

    /// Extract multiple property values
    pub fn values_multi(self, keys: &[&str]) -> BoundTraversal<'g, In, Value> {
        let keys: Vec<String> = keys.iter().map(|s| s.to_string()).collect();
        self.add_step(ValuesStep::multi(keys))
    }

    /// Get element ID
    pub fn id(self) -> BoundTraversal<'g, In, Value> {
        self.add_step(IdStep)
    }

    /// Get element label
    pub fn label(self) -> BoundTraversal<'g, In, Value> {
        self.add_step(LabelStep)
    }

    /// Map with closure
    pub fn map<F>(self, f: F) -> BoundTraversal<'g, In, Value>
    where
        F: Fn(&ExecutionContext, &Value) -> Value + Clone + Send + Sync + 'static,
    {
        self.add_step(MapStep::new(f))
    }

    /// FlatMap with closure
    pub fn flat_map<F>(self, f: F) -> BoundTraversal<'g, In, Value>
    where
        F: Fn(&ExecutionContext, &Value) -> Vec<Value> + Clone + Send + Sync + 'static,
    {
        self.add_step(FlatMapStep::new(f))
    }

    /// Emit constant value
    pub fn constant(self, value: impl Into<Value>) -> BoundTraversal<'g, In, Value> {
        self.add_step(ConstantStep::new(value))
    }

    /// Get traversal path
    pub fn path(self) -> BoundTraversal<'g, In, Value> {
        self.add_step(PathStep)
    }
}
```

### 3.8 Terminal Steps (`src/traversal/terminal.rs`)

Terminal steps consume the traversal and produce a result. They trigger execution.

```rust
use crate::error::TraversalError;
use std::collections::HashSet;

impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Execute and collect all results to a Vec
    pub fn to_list(self) -> Vec<Value> {
        self.execute().map(|t| t.value).collect()
    }

    /// Execute and collect to a HashSet (deduplicates)
    pub fn to_set(self) -> HashSet<Value> {
        self.execute().map(|t| t.value).collect()
    }

    /// Get next result
    pub fn next(self) -> Option<Value> {
        self.execute().next().map(|t| t.value)
    }

    /// Get exactly one result (error if 0 or 2+)
    pub fn one(self) -> Result<Value, TraversalError> {
        let mut iter = self.execute();
        match iter.next() {
            None => Err(TraversalError::NotOne(0)),
            Some(first) => {
                if iter.next().is_some() {
                    let remaining = iter.count();
                    Err(TraversalError::NotOne(remaining + 2))
                } else {
                    Ok(first.value)
                }
            }
        }
    }

    /// Check if any results exist
    pub fn has_next(self) -> bool {
        self.execute().next().is_some()
    }

    /// Execute for side effects only
    pub fn iterate(self) {
        for _ in self.execute() {}
    }

    /// Get first n results
    pub fn take(self, n: usize) -> Vec<Value> {
        self.execute().take(n).map(|t| t.value).collect()
    }

    /// Count results
    pub fn count(self) -> u64 {
        self.execute().count() as u64
    }

    /// Fold/reduce results
    pub fn fold<B, F>(self, init: B, f: F) -> B
    where
        F: FnMut(B, Value) -> B,
    {
        self.execute().map(|t| t.value).fold(init, f)
    }

    /// Sum numeric values
    pub fn sum(self) -> Value {
        let mut total: f64 = 0.0;
        for t in self.execute() {
            match t.value {
                Value::Int(n) => total += n as f64,
                Value::Float(n) => total += n,
                _ => {}
            }
        }
        Value::Float(total)
    }

    /// Get min value
    pub fn min(self) -> Option<Value> {
        self.execute()
            .map(|t| t.value)
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
    }

    /// Get max value
    pub fn max(self) -> Option<Value> {
        self.execute()
            .map(|t| t.value)
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
    }

    /// Get results as iterator (for advanced usage)
    pub fn iter(self) -> impl Iterator<Item = Value> + 'g {
        self.execute().map(|t| t.value)
    }

    /// Get traversers as iterator (includes metadata)
    pub fn traversers(self) -> impl Iterator<Item = Traverser> + 'g {
        self.execute()
    }
}
```

---

## 3.9 Anonymous Traversals (Phase 4 Preview)

The new architecture fully supports anonymous traversals which will be implemented in Phase 4. Here's how they work:

### The `__` Factory Module

```rust
/// Anonymous traversal factory
/// 
/// Anonymous traversals are traversal fragments without a graph binding.
/// They receive their ExecutionContext when spliced into a parent traversal.
pub mod __ {
    use super::*;

    /// Create anonymous traversal starting with out step
    pub fn out() -> Traversal<Value, Value> {
        Traversal::new().add_step(OutStep::new())
    }

    /// Create anonymous traversal starting with out step (with labels)
    pub fn out_labels(labels: &[&str]) -> Traversal<Value, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        Traversal::new().add_step(OutStep::with_labels(labels))
    }

    /// Create anonymous traversal starting with in step
    pub fn in_() -> Traversal<Value, Value> {
        Traversal::new().add_step(InStep::new())
    }

    /// Create anonymous traversal starting with has_label filter
    pub fn has_label(label: &str) -> Traversal<Value, Value> {
        Traversal::new().add_step(HasLabelStep::single(label))
    }

    /// Create anonymous traversal starting with has_value filter
    pub fn has_value(key: &str, value: impl Into<Value>) -> Traversal<Value, Value> {
        Traversal::new().add_step(HasValueStep::new(key, value))
    }

    /// Create anonymous traversal starting with values step
    pub fn values(key: &str) -> Traversal<Value, Value> {
        Traversal::new().add_step(ValuesStep::new(key))
    }

    /// Create anonymous traversal starting with identity (pass-through)
    pub fn identity() -> Traversal<Value, Value> {
        Traversal::new().add_step(IdentityStep)
    }

    /// Create anonymous traversal starting with constant
    pub fn constant(value: impl Into<Value>) -> Traversal<Value, Value> {
        Traversal::new().add_step(ConstantStep::new(value))
    }
}
```

### Usage in Branching Steps (Phase 4)

Anonymous traversals will be used with branching steps:

```rust
// Union - merge results from multiple traversals
g.v().has_label("person")
    .union(
        __.out_labels(&["knows"]),
        __.out_labels(&["works_at"]),
    )
    .to_list();

// Coalesce - first traversal with results wins
g.v().has_label("person")
    .coalesce(
        __.values("nickname"),
        __.values("name"),
    )
    .to_list();

// Choose - conditional branching
g.v().has_label("person")
    .choose(
        __.has_value("age", Value::Int(30)),
        __.constant("thirty"),
        __.constant("not thirty"),
    )
    .to_list();
```

### How Anonymous Traversals Execute

When an anonymous traversal is used:

1. **At construction**: Steps are added to `Vec<Box<dyn AnyStep>>` without graph access
2. **At splice point**: Parent traversal's `append()` method merges the steps
3. **At execution**: `ExecutionContext` is passed through all steps uniformly

```rust
// Both bound and anonymous traversals use the same step types
// The difference is only in how they're constructed

// Bound traversal - has source
let bound: BoundTraversal<'_, (), Value> = g.v().out();

// Anonymous traversal - no source
let anon: Traversal<Value, Value> = __.out();

// When combined:
let combined = bound.append(anon);  // Steps are merged
combined.to_list();  // ExecutionContext flows through all steps
```

---

## Test Cases

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_traverser_new() {
        let t = Traverser::new(Value::String("hello".to_string()));
        assert_eq!(t.loops, 0);
        assert_eq!(t.bulk, 1);
        assert!(t.path.is_empty());
    }

    #[test]
    fn test_traverser_split() {
        let t1 = Traverser::new(Value::String("hello".to_string()));
        let t2 = t1.split(Value::String("world".to_string()));
        assert_eq!(t2.loops, 0);
        assert!(t2.path.is_empty()); // Path preserved from parent
    }

    #[test]
    fn test_path_push_and_get() {
        let mut path = Path::default();
        path.push(PathValue::Vertex(VertexId(1)), &["start".to_string()]);
        path.push(PathValue::Vertex(VertexId(2)), &["middle".to_string()]);
        
        assert_eq!(path.len(), 2);
        
        let start = path.get("start").unwrap();
        assert_eq!(start.len(), 1);
    }

    #[test]
    fn test_path_contains_vertex() {
        let mut path = Path::default();
        path.push(PathValue::Vertex(VertexId(1)), &[]);
        path.push(PathValue::Vertex(VertexId(2)), &[]);
        
        assert!(path.contains_vertex(VertexId(1)));
        assert!(path.contains_vertex(VertexId(2)));
        assert!(!path.contains_vertex(VertexId(3)));
    }

    #[test]
    fn test_traversal_clone() {
        let t: Traversal<Value, Value> = Traversal::new()
            .add_step(HasLabelStep::single("person"))
            .add_step(OutStep::new());
        
        let t2 = t.clone();
        // Both should have same steps
    }
}
```

### Integration Tests

```rust
#[cfg(test)]
mod integration_tests {
    use super::*;

    fn create_test_graph() -> Graph {
        let graph = Graph::in_memory();
        
        {
            let mut g = graph.mutate();
            
            // Add vertices
            let alice = g.add_vertex("person", hashmap!{
                "name" => "Alice",
                "age" => 30
            });
            let bob = g.add_vertex("person", hashmap!{
                "name" => "Bob",
                "age" => 35
            });
            let carol = g.add_vertex("person", hashmap!{
                "name" => "Carol",
                "age" => 25
            });
            let acme = g.add_vertex("company", hashmap!{
                "name" => "Acme Corp"
            });
            
            // Add edges
            g.add_edge(alice, bob, "knows", hashmap!{});
            g.add_edge(alice, carol, "knows", hashmap!{});
            g.add_edge(bob, carol, "knows", hashmap!{});
            g.add_edge(alice, acme, "works_at", hashmap!{});
            g.add_edge(bob, acme, "works_at", hashmap!{});
            
            g.commit().unwrap();
        }
        
        graph
    }

    #[test]
    fn test_v_all() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        let count = g.v().count();
        assert_eq!(count, 4);
    }

    #[test]
    fn test_has_label() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        let people_count = g.v().has_label("person").count();
        assert_eq!(people_count, 3);
        
        let company_count = g.v().has_label("company").count();
        assert_eq!(company_count, 1);
    }

    #[test]
    fn test_has_value() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        let alice_count = g.v()
            .has_label("person")
            .has_value("name", "Alice")
            .count();
        assert_eq!(alice_count, 1);
    }

    #[test]
    fn test_out() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        // Alice knows Bob and Carol
        let friends_count = g.v()
            .has_value("name", "Alice")
            .out_labels(&["knows"])
            .count();
        assert_eq!(friends_count, 2);
    }

    #[test]
    fn test_in() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        // Who knows Carol? Alice and Bob
        let knowers_count = g.v()
            .has_value("name", "Carol")
            .in_labels(&["knows"])
            .count();
        assert_eq!(knowers_count, 2);
    }

    #[test]
    fn test_dedup() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        // Without dedup: would have duplicates
        // With dedup: unique results
        let count = g.v()
            .has_value("name", "Alice")
            .out_labels(&["knows"])
            .out_labels(&["knows"])
            .dedup()
            .count();
        
        // Carol reachable via Bob
        assert!(count <= 2);
    }

    #[test]
    fn test_limit() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        let count = g.v().limit(2).count();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_values() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        let names = g.v()
            .has_label("person")
            .values("name")
            .to_list();
        assert_eq!(names.len(), 3);
    }

    #[test]
    fn test_one_success() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        let alice = g.v()
            .has_value("name", "Alice")
            .one();
        assert!(alice.is_ok());
    }

    #[test]
    fn test_one_too_many() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        let result = g.v()
            .has_label("person")
            .one();
        assert!(matches!(result, Err(TraversalError::NotOne(3))));
    }

    #[test]
    fn test_one_none() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        let result = g.v()
            .has_value("name", "Nobody")
            .one();
        assert!(matches!(result, Err(TraversalError::NotOne(0))));
    }

    #[test]
    fn test_append_anonymous() {
        let graph = create_test_graph();
        let snap = graph.snapshot();
        let g = snap.traversal();
        
        // Use anonymous traversal
        let anon = Traversal::<Value, Value>::new()
            .add_step(OutStep::new())
            .add_step(HasLabelStep::single("person"));
        
        let count = g.v()
            .has_value("name", "Alice")
            .append(anon)
            .count();
        
        // Alice -> Bob, Carol (both persons via knows)
        assert_eq!(count, 2);
    }
}
```

### Benchmarks

```rust
// benches/traversal.rs

use criterion::{criterion_group, criterion_main, Criterion};

fn create_benchmark_graph(vertex_count: u64, edge_count: u64) -> Graph {
    let graph = Graph::in_memory();
    
    {
        let mut g = graph.mutate();
        
        let mut vertex_ids = Vec::with_capacity(vertex_count as usize);
        for i in 0..vertex_count {
            let label = if i % 2 == 0 { "person" } else { "company" };
            let id = g.add_vertex(label, hashmap!{
                "name" => format!("Entity_{}", i),
                "value" => i as i64
            });
            vertex_ids.push(id);
        }
        
        use rand::Rng;
        let mut rng = rand::thread_rng();
        for _ in 0..edge_count {
            let src = vertex_ids[rng.gen_range(0..vertex_ids.len())];
            let dst = vertex_ids[rng.gen_range(0..vertex_ids.len())];
            if src != dst {
                let _ = g.add_edge(src, dst, "connects", hashmap!{});
            }
        }
        
        g.commit().unwrap();
    }
    
    graph
}

fn bench_simple_traversal(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);
    
    c.bench_function("v().has_label().count()", |b| {
        b.iter(|| {
            let snap = graph.snapshot();
            let g = snap.traversal();
            g.v().has_label("person").count()
        })
    });
}

fn bench_navigation(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);
    
    c.bench_function("v().out().limit().count()", |b| {
        b.iter(|| {
            let snap = graph.snapshot();
            let g = snap.traversal();
            g.v().has_label("person").out().limit(1000).count()
        })
    });
}

fn bench_dedup(c: &mut Criterion) {
    let graph = create_benchmark_graph(10_000, 100_000);
    
    c.bench_function("v().out().out().dedup().count()", |b| {
        b.iter(|| {
            let snap = graph.snapshot();
            let g = snap.traversal();
            g.v().limit(100).out().out().dedup().count()
        })
    });
}

criterion_group!(
    benches,
    bench_simple_traversal,
    bench_navigation,
    bench_dedup,
);
criterion_main!(benches);
```

---

## Exit Criteria

- [ ] All core types compile (`Traversal`, `Traverser`, `Path`, `ExecutionContext`)
- [ ] `AnyStep` trait works with type erasure
- [ ] `GraphTraversalSource` with `v()` and `e()` starting points
- [ ] `BoundTraversal` wrapper correctly manages execution context
- [ ] Navigation steps work: `out()`, `in_()`, `both()`, `out_e()`, `in_e()`, `out_v()`, `in_v()`
- [ ] Filter steps work: `has_label()`, `has()`, `has_value()`, `filter()`, `dedup()`, `limit()`, `skip()`, `range()`
- [ ] Transform steps work: `values()`, `id()`, `label()`, `map()`, `flat_map()`, `constant()`, `path()`
- [ ] Terminal steps work: `to_list()`, `to_set()`, `next()`, `one()`, `has_next()`, `iterate()`, `count()`, `sum()`, `min()`, `max()`
- [ ] Lazy evaluation verified (no work until terminal step)
- [ ] Path tracking works correctly
- [ ] Label resolution works via ExecutionContext
- [ ] Anonymous traversals can be appended to bound traversals
- [ ] All unit tests pass
- [ ] All integration tests pass with 10K vertex, 100K edge graph
- [ ] Benchmarks run successfully

---

## Implementation Order

1. **Week 1**: Core types and infrastructure
   - `ExecutionContext`, `SideEffects`
   - `Traverser` (non-generic, uses Value)
   - `Path`, `PathElement`, `PathValue`
   - `AnyStep` trait with `clone_box()`
   - `Traversal<In, Out>` with type-erased steps
   - Basic unit tests

2. **Week 2**: Source and terminal steps
   - `GraphTraversalSource`
   - `BoundTraversal` wrapper
   - `StartStep` for source expansion
   - Terminal steps: `to_list()`, `next()`, `count()`, `iterate()`
   - Integration with `GraphSnapshot`

3. **Week 3**: Filter steps
   - `HasLabelStep`, `HasStep`, `HasValueStep`
   - `FilterStep`, `DedupStep`, `LimitStep`, `SkipStep`, `RangeStep`
   - `HasIdStep`
   - Filter integration tests

4. **Week 4**: Navigation steps
   - `OutStep`, `InStep`, `BothStep`
   - `OutEStep`, `InEStep`, `BothEStep`
   - `OutVStep`, `InVStep`, `BothVStep`
   - Navigation integration tests

5. **Week 5**: Transform steps and polish
   - `ValuesStep`, `IdStep`, `LabelStep`
   - `MapStep`, `FlatMapStep`, `ConstantStep`
   - `PathStep`
   - Anonymous traversal `append()` support
   - Benchmarks
   - Documentation
   - Final integration testing

---

## Notes

### Type Erasure Trade-offs

The new architecture uses `Box<dyn AnyStep>` instead of monomorphization:

**Pros:**
- Unified type for bound and anonymous traversals
- Simpler API (no complex generic bounds)
- Steps can be stored in collections
- Easier to clone traversals for branching

**Cons:**
- Virtual dispatch overhead (one indirect call per step per traverser)
- Dynamic allocation for each step
- No compile-time step optimization

**Mitigation:**
- Hot paths can be re-implemented with monomorphization later
- The overhead is typically dwarfed by actual graph traversal I/O
- Boxing enables features (anonymous traversals) that would be very complex otherwise

### Memory Model

```
BoundTraversal<'g, In, Out>
    ├── snapshot: &'g GraphSnapshot
    ├── interner: &'g StringInterner  
    └── traversal: Traversal<In, Out>
            └── steps: Vec<Box<dyn AnyStep>>
                       ├── Box<HasLabelStep>
                       ├── Box<OutStep>
                       └── Box<DedupStep>
```

At execution time:
```
ExecutionContext<'g>
    ├── snapshot: &'g GraphSnapshot
    ├── interner: &'g StringInterner
    └── side_effects: SideEffects

Traversers flow: Box<dyn Iterator<Item = Traverser>>
    → step1.apply(ctx, input)
    → step2.apply(ctx, output1)
    → step3.apply(ctx, output2)
    → terminal step collects
```

### Thread Safety

- `AnyStep: Send + Sync` enables future parallel execution
- `ExecutionContext` holds shared references (read-only)
- `SideEffects` may need interior mutability for Phase 4 (store/aggregate)
- `GraphSnapshot` provides consistent reads

### Phase 4 Compatibility

This architecture directly supports Phase 4 features:

| Feature | How Supported |
|---------|---------------|
| Anonymous traversals (`__`) | Same `Traversal` type, no source |
| `union()` | Clone steps, merge iterators |
| `coalesce()` | Try traversals in order |
| `choose()` | Conditional step selection |
| `repeat()` | Loop with cloned steps |
| `local()` | Scoped step execution |
| `store()`/`aggregate()` | Via `SideEffects` in context |
