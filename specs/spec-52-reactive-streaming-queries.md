# Spec 52: Reactive Streaming Queries

## Overview

Reactive streaming queries enable users to subscribe to a traversal pattern and receive push-based notifications whenever graph mutations produce new matching results, cause previously-matching elements to stop matching, or update properties on matching elements. This is the foundational building block for real-time graph applications — live dashboards, alerting, incremental view maintenance, and event-driven architectures.

### Goals

1. **Push-based reactivity**: Subscribers receive events as mutations occur, without polling
2. **Runtime-agnostic**: No async runtime dependency. Pure `std::sync` + `parking_lot`. Users can wrap the provided `Receiver` in tokio, async-std, smol, or any other runtime
3. **Removal tracking**: Subscribers are notified when previously-matching elements no longer match (due to deletion or property changes)
4. **Both storage backends**: Works with in-memory `Graph` and persistent `CowMmapGraph`
5. **Zero cost when unused**: When no subscriptions exist, mutation overhead is a single atomic load
6. **Configurable backpressure**: Bounded channel with configurable capacity (default 1024)
7. **Lazy thread lifecycle**: Dispatcher thread spawns on first subscription, shuts down when last subscription is dropped
8. **Batch-aware**: Batch mutations emit a single composite event, preserving atomicity semantics

### Non-Goals

- Async runtime integration (users wrap `std::sync::mpsc::Receiver` themselves)
- WASM support (excluded, same as `mmap` — `std::thread::spawn` unavailable)
- Distributed/remote subscriptions
- Persistent subscription state across process restarts
- Complex Event Processing (CEP) patterns (windowing, temporal joins)
- Subscription to schema/index changes

## Architecture

```
  ┌─────────────────────────────────────────────────────────────┐
  │                      User Code                              │
  │                                                             │
  │  let sub = g.v().has_label("person")                        │
  │              .has_where("age", p::gt(30))                   │
  │              .subscribe();                                  │
  │                                                             │
  │  for event in sub {                                         │
  │      match event.event_type {                               │
  │          Added => ...,                                      │
  │          Removed => ...,                                    │
  │          Updated => ...,                                    │
  │      }                                                      │
  │  }                                                          │
  └────────────────────────────┬────────────────────────────────┘
                               │ mpsc::Receiver<SubscriptionEvent>
                               │
  ┌────────────────────────────┴────────────────────────────────┐
  │                  SubscriptionManager                         │
  │                  (dispatcher thread)                         │
  │                                                             │
  │  loop {                                                     │
  │    event = event_rx.recv()                                  │
  │    for sub in active_subscriptions {                        │
  │      if sub.matcher.might_match(&event) {       ← O(1)     │
  │        let snapshot = (sub.snapshot_fn)()                   │
  │        let result = sub.matcher.evaluate(snapshot, &event)  │
  │        sub.update_matched_set(result)           ← Removed  │
  │        sub.tx.send(subscription_event)                      │
  │      }                                                      │
  │    }                                                        │
  │  }                                                          │
  └────────────────────────────┬────────────────────────────────┘
                               │ mpsc::Receiver<GraphEvent>
                               │
  ┌────────────────────────────┴────────────────────────────────┐
  │                       EventBus                               │
  │              (embedded in Graph / CowMmapGraph)              │
  │                                                             │
  │  subscriber_count: AtomicUsize  ← fast-path zero check     │
  │  subscribers: Mutex<Vec<Sender<GraphEvent>>>                │
  └────────────────────────────┬────────────────────────────────┘
                               │ emit() called after each mutation
                               │
  ┌────────────────────────────┴────────────────────────────────┐
  │                  Graph / CowMmapGraph                        │
  │                                                             │
  │  add_vertex()          → emit(VertexAdded { ... })          │
  │  add_edge()            → emit(EdgeAdded { ... })            │
  │  set_vertex_property() → emit(VertexPropertyChanged { ... })│
  │  set_edge_property()   → emit(EdgePropertyChanged { ... })  │
  │  remove_vertex()       → emit(VertexRemoved { ... })        │
  │  remove_edge()         → emit(EdgeRemoved { ... })          │
  │  batch()               → emit(Batch(collected_events))      │
  └─────────────────────────────────────────────────────────────┘
```

## Module Structure

```
interstellar/src/
├── storage/
│   ├── events.rs           # NEW: GraphEvent, EventBus
│   ├── mod.rs              # MODIFIED: pub mod events (behind feature flag)
│   ├── cow.rs              # MODIFIED: EventBus field, emit calls
│   └── cow_mmap.rs         # MODIFIED: EventBus field, emit calls
│
├── traversal/
│   ├── reactive.rs         # NEW: QueryMatcher, Subscription, SubscriptionManager
│   ├── mod.rs              # MODIFIED: pub mod reactive, re-exports
│   └── source.rs           # MODIFIED: .subscribe() on BoundTraversal
│
├── lib.rs                  # MODIFIED: feature flag, prelude additions
└── error.rs                # MODIFIED: SubscriptionError variants
```

## Feature Flag

```toml
# In Cargo.toml [features]
reactive = []  # No new dependencies — uses std::sync, parking_lot (already present)
full = ["mmap", "graphson", "gql", "gremlin", "full-text", "reactive"]
```

No new crate dependencies. The implementation uses:
- `std::sync::mpsc` — channels for event delivery
- `std::sync::atomic::AtomicUsize` — fast-path subscriber count
- `std::thread` — dispatcher thread
- `parking_lot::Mutex` / `parking_lot::RwLock` — already in dependencies
- `hashbrown::HashSet` — already in dependencies, for `matched_set`

The `reactive` feature is excluded from WASM targets. In `lib.rs`:

```rust
#[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
pub mod reactive_reexports { ... }
```

---

## Layer 1: GraphEvent

**File**: `interstellar/src/storage/events.rs`

### GraphEvent Enum

```rust
use std::collections::HashMap;
use crate::value::{EdgeId, Value, VertexId};

/// A single graph mutation event.
///
/// Events are emitted after each successful mutation. They capture enough
/// context for downstream subscribers to determine whether the mutation
/// affects their query without requiring a full graph scan.
///
/// # Design Notes
///
/// - Labels are stored as `String` (not interned IDs) for consumer ergonomics.
///   The interner is an internal detail of the storage layer.
/// - `VertexRemoved` / `EdgeRemoved` capture the label and endpoint IDs
///   *before* deletion, so subscribers can determine relevance.
/// - `PropertyChanged` captures the old value for removal detection.
/// - `Batch` wraps events from `batch()` closures, emitted atomically
///   on successful commit.
#[derive(Clone, Debug, PartialEq)]
pub enum GraphEvent {
    /// A new vertex was added to the graph.
    VertexAdded {
        id: VertexId,
        label: String,
        properties: HashMap<String, Value>,
    },

    /// A vertex was removed from the graph.
    ///
    /// The label is captured before deletion for subscriber filtering.
    /// Incident edges are removed separately and emit their own
    /// `EdgeRemoved` events.
    VertexRemoved {
        id: VertexId,
        label: String,
    },

    /// A vertex property was added or updated.
    VertexPropertyChanged {
        id: VertexId,
        key: String,
        /// `None` if the property was newly added.
        old_value: Option<Value>,
        new_value: Value,
    },

    /// A new edge was added to the graph.
    EdgeAdded {
        id: EdgeId,
        src: VertexId,
        dst: VertexId,
        label: String,
        properties: HashMap<String, Value>,
    },

    /// An edge was removed from the graph.
    ///
    /// Endpoint IDs and label are captured before deletion.
    EdgeRemoved {
        id: EdgeId,
        src: VertexId,
        dst: VertexId,
        label: String,
    },

    /// An edge property was added or updated.
    EdgePropertyChanged {
        id: EdgeId,
        key: String,
        /// `None` if the property was newly added.
        old_value: Option<Value>,
        new_value: Value,
    },

    /// A batch of events committed atomically.
    ///
    /// Emitted by `Graph::batch()` and `CowMmapGraph::batch()` when the
    /// batch closure returns `Ok`. The inner events are in mutation order.
    ///
    /// Subscribers should process all inner events as a single logical
    /// unit — the `matched_set` should be updated after all inner events
    /// are processed, not after each one.
    Batch(Vec<GraphEvent>),
}

impl GraphEvent {
    /// Returns the affected vertex ID, if this is a vertex event.
    pub fn vertex_id(&self) -> Option<VertexId> {
        match self {
            Self::VertexAdded { id, .. }
            | Self::VertexRemoved { id, .. }
            | Self::VertexPropertyChanged { id, .. } => Some(*id),
            _ => None,
        }
    }

    /// Returns the affected edge ID, if this is an edge event.
    pub fn edge_id(&self) -> Option<EdgeId> {
        match self {
            Self::EdgeAdded { id, .. }
            | Self::EdgeRemoved { id, .. }
            | Self::EdgePropertyChanged { id, .. } => Some(*id),
            _ => None,
        }
    }

    /// Returns the label associated with this event.
    pub fn label(&self) -> Option<&str> {
        match self {
            Self::VertexAdded { label, .. }
            | Self::VertexRemoved { label, .. }
            | Self::EdgeAdded { label, .. }
            | Self::EdgeRemoved { label, .. } => Some(label),
            _ => None,
        }
    }

    /// Returns the property key if this is a property change event.
    pub fn property_key(&self) -> Option<&str> {
        match self {
            Self::VertexPropertyChanged { key, .. }
            | Self::EdgePropertyChanged { key, .. } => Some(key),
            _ => None,
        }
    }

    /// Returns `true` if this is a vertex event (added, removed, or property changed).
    pub fn is_vertex_event(&self) -> bool {
        matches!(
            self,
            Self::VertexAdded { .. }
                | Self::VertexRemoved { .. }
                | Self::VertexPropertyChanged { .. }
        )
    }

    /// Returns `true` if this is an edge event (added, removed, or property changed).
    pub fn is_edge_event(&self) -> bool {
        matches!(
            self,
            Self::EdgeAdded { .. }
                | Self::EdgeRemoved { .. }
                | Self::EdgePropertyChanged { .. }
        )
    }

    /// Returns `true` if this is a removal event.
    pub fn is_removal(&self) -> bool {
        matches!(
            self,
            Self::VertexRemoved { .. } | Self::EdgeRemoved { .. }
        )
    }

    /// Returns `true` if this is a batch event.
    pub fn is_batch(&self) -> bool {
        matches!(self, Self::Batch(_))
    }

    /// Flattens a batch event into individual events.
    /// Non-batch events return a single-element vec.
    pub fn flatten(self) -> Vec<GraphEvent> {
        match self {
            Self::Batch(events) => events
                .into_iter()
                .flat_map(|e| e.flatten())
                .collect(),
            other => vec![other],
        }
    }
}
```

### EventBus

```rust
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use parking_lot::Mutex;

/// Zero-overhead broadcast event bus for graph mutation events.
///
/// When no subscribers exist, `emit()` performs a single atomic load
/// and returns immediately — no allocation, no locking.
///
/// # Thread Safety
///
/// `EventBus` is `Send + Sync`. Multiple threads can emit events
/// concurrently (the internal `Mutex` serializes subscriber list access
/// only when there are active subscribers).
///
/// # Subscriber Lifecycle
///
/// Subscribers are added via `subscribe()` which returns a
/// `mpsc::Receiver<GraphEvent>`. When the receiver is dropped,
/// the next `emit()` call detects the closed channel and removes
/// the dead subscriber automatically.
pub struct EventBus {
    /// Subscriber channels. Protected by Mutex for thread-safe access.
    subscribers: Mutex<Vec<mpsc::SyncSender<GraphEvent>>>,
    /// Fast-path: avoids locking when no subscribers exist.
    subscriber_count: AtomicUsize,
}

impl EventBus {
    /// Default channel capacity per subscriber.
    pub const DEFAULT_CAPACITY: usize = 1024;

    /// Create a new empty event bus.
    pub fn new() -> Self {
        Self {
            subscribers: Mutex::new(Vec::new()),
            subscriber_count: AtomicUsize::new(0),
        }
    }

    /// Subscribe to events with the default channel capacity (1024).
    ///
    /// Returns a `Receiver` that will receive all future events.
    /// When the receiver is dropped, the subscription is automatically
    /// cleaned up on the next `emit()` call.
    pub fn subscribe(&self) -> mpsc::Receiver<GraphEvent> {
        self.subscribe_with_capacity(Self::DEFAULT_CAPACITY)
    }

    /// Subscribe to events with a custom channel capacity.
    ///
    /// The capacity controls backpressure behavior: when the channel
    /// is full, events for this subscriber are dropped (not blocking
    /// the mutation path).
    ///
    /// # Arguments
    ///
    /// * `capacity` - Maximum number of events buffered before dropping
    pub fn subscribe_with_capacity(&self, capacity: usize) -> mpsc::Receiver<GraphEvent> {
        let (tx, rx) = mpsc::sync_channel(capacity);
        let mut subs = self.subscribers.lock();
        subs.push(tx);
        self.subscriber_count.store(subs.len(), Ordering::Release);
        rx
    }

    /// Emit an event to all subscribers.
    ///
    /// - If no subscribers exist: returns immediately (atomic load only).
    /// - Dead subscribers (dropped receivers) are pruned automatically.
    /// - If a subscriber's channel is full, the event is dropped for
    ///   that subscriber (non-blocking — mutations are never slowed).
    ///
    /// # Arguments
    ///
    /// * `event` - The event to broadcast
    pub fn emit(&self, event: GraphEvent) {
        // Fast path: no subscribers
        if self.subscriber_count.load(Ordering::Acquire) == 0 {
            return;
        }

        let mut subs = self.subscribers.lock();

        // Send to all subscribers, tracking which are still alive
        subs.retain(|tx| {
            // try_send: non-blocking. If full, drop the event for this subscriber.
            // If disconnected, remove the subscriber.
            match tx.try_send(event.clone()) {
                Ok(()) => true,
                Err(mpsc::TrySendError::Full(_)) => true,  // subscriber is slow, keep it
                Err(mpsc::TrySendError::Disconnected(_)) => false,  // subscriber dropped
            }
        });

        self.subscriber_count.store(subs.len(), Ordering::Release);
    }

    /// Returns the number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.subscriber_count.load(Ordering::Relaxed)
    }

    /// Returns `true` if there are no active subscribers.
    pub fn is_empty(&self) -> bool {
        self.subscriber_count() == 0
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}
```

### Design Decisions — EventBus

| Decision | Choice | Rationale |
|----------|--------|-----------|
| `SyncSender` vs `Sender` | `SyncSender` (bounded) | Prevents unbounded memory growth from slow subscribers. Default capacity 1024. |
| Full channel behavior | Drop events (via `try_send`) | Mutations must never block on a slow subscriber. This is a design invariant. |
| Dead subscriber cleanup | Lazy (on next `emit`) | Avoids needing a background reaper. Cleanup happens naturally. |
| Event cloning | `event.clone()` per subscriber | Required for broadcast semantics. `GraphEvent` is cheap to clone (small strings + HashMap). |
| AtomicUsize fast path | `Acquire`/`Release` ordering | Sufficient for publisher/subscriber coordination. No `SeqCst` needed. |

---

## Layer 2: Graph / CowMmapGraph Integration

### Graph (cow.rs) Changes

#### New Field

```rust
pub struct Graph {
    state: RwLock<GraphState>,
    schema: RwLock<Option<GraphSchema>>,

    #[cfg(feature = "reactive")]
    event_bus: EventBus,
}
```

The `Graph::new()` constructor initializes `event_bus: EventBus::new()`.

#### New Public Methods

```rust
#[cfg(feature = "reactive")]
impl Graph {
    /// Get a reference to the event bus for subscribing to mutation events.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let graph = Graph::new();
    /// let rx = graph.event_bus().subscribe();
    ///
    /// // In another thread
    /// graph.add_vertex("person", props! { "name" => "Alice" });
    ///
    /// // Receive the event
    /// let event = rx.recv().unwrap();
    /// assert!(matches!(event, GraphEvent::VertexAdded { .. }));
    /// ```
    pub fn event_bus(&self) -> &EventBus {
        &self.event_bus
    }
}
```

#### Mutation Method Instrumentation

Each mutation method gets event emission added after the mutation succeeds. The pattern is:

1. Capture any pre-mutation state needed for the event (e.g., old property value)
2. Perform the mutation (existing code, unchanged)
3. Emit the event (behind `#[cfg(feature = "reactive")]`)

##### `add_vertex` (cow.rs:1089)

```rust
pub fn add_vertex(&self, label: &str, properties: HashMap<String, Value>) -> VertexId {
    // ... existing mutation logic ...
    // id = newly assigned VertexId

    #[cfg(feature = "reactive")]
    self.event_bus.emit(GraphEvent::VertexAdded {
        id,
        label: label.to_string(),
        properties: properties.clone(),
    });

    id
}
```

**Note**: `properties` must be cloned for the event since the original is moved into `NodeData`. The clone only happens when there are subscribers (the `emit` fast-path skips the clone if `subscriber_count == 0`). To avoid the clone when there are no subscribers:

```rust
#[cfg(feature = "reactive")]
if self.event_bus.subscriber_count() > 0 {
    self.event_bus.emit(GraphEvent::VertexAdded {
        id,
        label: label.to_string(),
        properties: properties_clone, // cloned before moving into NodeData
    });
}
```

This optimization requires capturing `properties.clone()` before the existing code moves `properties` into `NodeData`. The exact insertion point depends on the existing code structure — the properties should be cloned (or the event built) between the point where `properties` is still available and before it's consumed.

##### `add_edge` (cow.rs:1157)

```rust
pub fn add_edge(
    &self, src: VertexId, dst: VertexId, label: &str,
    properties: HashMap<String, Value>,
) -> Result<EdgeId, StorageError> {
    // ... existing mutation logic ...
    // id = newly assigned EdgeId

    #[cfg(feature = "reactive")]
    self.event_bus.emit(GraphEvent::EdgeAdded {
        id,
        src,
        dst,
        label: label.to_string(),
        properties: properties_clone,
    });

    Ok(id)
}
```

##### `set_vertex_property` (cow.rs:1233)

```rust
pub fn set_vertex_property(
    &self, id: VertexId, key: &str, value: Value,
) -> Result<(), StorageError> {
    // Capture old value BEFORE mutation (inside the write lock)
    #[cfg(feature = "reactive")]
    let old_value = {
        let state = self.state.read();
        state.vertices.get(&id)
            .and_then(|node| node.properties.get(key).cloned())
    };

    // ... existing mutation logic ...

    #[cfg(feature = "reactive")]
    self.event_bus.emit(GraphEvent::VertexPropertyChanged {
        id,
        key: key.to_string(),
        old_value,
        new_value: value.clone(),
    });

    Ok(())
}
```

**Important**: The old value must be captured *before* the mutation. This requires reading the current property value in a read lock, then performing the mutation in a write lock. The existing code likely takes a write lock directly. Two options:

1. **Capture inside the write lock** (simpler): Read old value from the state inside the already-held write lock, before mutating.
2. **Separate read then write** (existing pattern may not support): Would require restructuring.

Option 1 is recommended — after acquiring the write lock but before modifying the `NodeData`, read the old property value.

##### `set_edge_property` (cow.rs:1277)

Same pattern as `set_vertex_property` but for edges.

##### `remove_vertex` (cow.rs:1318)

```rust
pub fn remove_vertex(&self, id: VertexId) -> Result<(), StorageError> {
    // Capture label and incident edge info BEFORE deletion (inside write lock)
    #[cfg(feature = "reactive")]
    let (label, incident_edges) = {
        let state = self.state.read();
        let node = state.vertices.get(&id)
            .ok_or(StorageError::VertexNotFound(id))?;
        let label = state.interner.resolve(node.label_id)
            .unwrap_or_default().to_string();
        let edges: Vec<(EdgeId, VertexId, VertexId, String)> = node.out_edges.iter()
            .chain(node.in_edges.iter())
            .filter_map(|eid| {
                state.edges.get(eid).map(|e| {
                    let elabel = state.interner.resolve(e.label_id)
                        .unwrap_or_default().to_string();
                    (*eid, e.src, e.dst, elabel)
                })
            })
            .collect();
        (label, edges)
    };

    // ... existing deletion logic (removes vertex + incident edges) ...

    #[cfg(feature = "reactive")]
    {
        // Emit EdgeRemoved for each incident edge
        for (eid, src, dst, elabel) in incident_edges {
            self.event_bus.emit(GraphEvent::EdgeRemoved {
                id: eid, src, dst, label: elabel,
            });
        }
        // Emit VertexRemoved
        self.event_bus.emit(GraphEvent::VertexRemoved { id, label });
    }

    Ok(())
}
```

**Order**: Edge removals are emitted before the vertex removal, matching the actual deletion order (incident edges are removed first).

##### `remove_edge` (cow.rs:1396)

```rust
pub fn remove_edge(&self, id: EdgeId) -> Result<(), StorageError> {
    // Capture edge info BEFORE deletion
    #[cfg(feature = "reactive")]
    let (src, dst, label) = {
        let state = self.state.read();
        let edge = state.edges.get(&id)
            .ok_or(StorageError::EdgeNotFound(id))?;
        let label = state.interner.resolve(edge.label_id)
            .unwrap_or_default().to_string();
        (edge.src, edge.dst, label)
    };

    // ... existing deletion logic ...

    #[cfg(feature = "reactive")]
    self.event_bus.emit(GraphEvent::EdgeRemoved { id, src, dst, label });

    Ok(())
}
```

#### BatchContext Integration

`BatchContext` (cow.rs:4003) operates on a cloned `GraphState` directly, bypassing `Graph` methods. Events must be collected during the batch and emitted after successful commit.

```rust
pub struct BatchContext<'a> {
    state: &'a mut GraphState,

    #[cfg(feature = "reactive")]
    pending_events: Vec<GraphEvent>,
}
```

Each `BatchContext` mutation method (`add_vertex`, `add_edge`) appends to `pending_events`.

In `Graph::batch()` (cow.rs:1637), after the batch closure succeeds and the new state is swapped in:

```rust
pub fn batch<F, T>(&self, f: F) -> Result<T, BatchError>
where
    F: FnOnce(&mut BatchContext) -> Result<T, BatchError>,
{
    let mut state = self.state.read().clone();
    let mut ctx = BatchContext {
        state: &mut state,
        #[cfg(feature = "reactive")]
        pending_events: Vec::new(),
    };

    let result = f(&mut ctx)?;

    #[cfg(feature = "reactive")]
    let events = std::mem::take(&mut ctx.pending_events);

    // Swap in new state
    *self.state.write() = state;

    // Emit batch event AFTER successful commit
    #[cfg(feature = "reactive")]
    if !events.is_empty() {
        self.event_bus.emit(GraphEvent::Batch(events));
    }

    Ok(result)
}
```

### CowMmapGraph (cow_mmap.rs) Changes

Identical pattern to `Graph`:

- Add `EventBus` field to `CowMmapGraph`
- Add `event_bus()` accessor
- Instrument all 6 mutation methods
- Instrument `CowMmapBatchContext` (which has all 6 mutation methods, unlike `BatchContext`)

The `CowMmapBatchContext` collects events in a `Vec<GraphEvent>`, and `CowMmapGraph::batch()` emits them as `GraphEvent::Batch(events)` after successful WAL commit.

---

## Layer 3: QueryMatcher

**File**: `interstellar/src/traversal/reactive.rs`

The `QueryMatcher` provides two-phase evaluation: fast rejection followed by full traversal re-evaluation.

### Step Introspection

The matcher examines traversal steps at subscription time to extract static filters. This is done via `dyn_name()` matching and downcasting where possible.

```rust
use std::collections::HashSet;
use crate::storage::events::GraphEvent;
use crate::traversal::step::DynStep;
use crate::traversal::{TraversalSource, Traverser};
use crate::traversal::context::SnapshotLike;
use crate::value::{Value, VertexId, EdgeId};

/// Compiled query filter for fast event matching.
///
/// Created from a traversal's steps and source. Provides O(1) fast
/// rejection for events that cannot possibly affect the query results,
/// followed by full traversal re-evaluation for events that pass.
///
/// # Compilation
///
/// The `compile` method walks the step list and extracts:
/// - Label filters from `HasLabelStep` steps
/// - Property keys from `HasStep`, `HasWhereStep`, `ValuesStep`
/// - Source type (vertex-only, edge-only, or both)
///
/// Steps that cannot be introspected are ignored (conservative — they
/// don't narrow the filter).
pub struct QueryMatcher {
    /// If Some, only events with these labels can match.
    label_filter: Option<HashSet<String>>,

    /// Property keys referenced by the traversal. Events changing
    /// other properties can be fast-rejected if no navigation steps
    /// exist (the property change can't affect the query).
    property_keys: HashSet<String>,

    /// True if the traversal only operates on vertices (source is V()).
    vertex_only: bool,

    /// True if the traversal only operates on edges (source is E()).
    edge_only: bool,

    /// True if the traversal contains navigation steps (out, in, both).
    /// Navigation means property changes on *any* vertex could affect
    /// results, since a neighbor might enter/leave the result set.
    has_navigation: bool,

    /// The original traversal steps, cloned for re-evaluation.
    steps: Vec<Box<dyn DynStep>>,

    /// The original traversal source.
    source: Option<TraversalSource>,
}
```

### Compilation

```rust
impl QueryMatcher {
    /// Compile a matcher from traversal steps and source.
    ///
    /// Extracts static filters by inspecting step names and, where
    /// possible, downcasting to concrete step types.
    pub fn compile(
        steps: &[Box<dyn DynStep>],
        source: &Option<TraversalSource>,
    ) -> Self {
        let mut label_filter: Option<HashSet<String>> = None;
        let mut property_keys = HashSet::new();
        let mut has_navigation = false;

        let vertex_only = matches!(
            source,
            Some(TraversalSource::AllVertices) | Some(TraversalSource::Vertices(_))
        );
        let edge_only = matches!(
            source,
            Some(TraversalSource::AllEdges) | Some(TraversalSource::Edges(_))
        );

        for step in steps {
            match step.dyn_name() {
                "hasLabel" => {
                    // Extract labels via StepIntrospect trait (see below)
                    if let Some(introspect) = step.as_introspectable() {
                        if let Some(labels) = introspect.label_constraints() {
                            let set = label_filter.get_or_insert_with(HashSet::new);
                            set.extend(labels);
                        }
                    }
                }
                "has" | "hasValue" | "hasWhere" | "hasNot" | "hasKey" => {
                    if let Some(introspect) = step.as_introspectable() {
                        if let Some(keys) = introspect.property_constraints() {
                            property_keys.extend(keys);
                        }
                    }
                }
                "values" | "properties" | "valueMap" | "elementMap" | "propertyMap" => {
                    if let Some(introspect) = step.as_introspectable() {
                        if let Some(keys) = introspect.property_constraints() {
                            property_keys.extend(keys);
                        }
                    }
                }
                "out" | "in" | "both" | "outE" | "inE" | "bothE"
                | "outV" | "inV" | "bothV" | "otherV" => {
                    has_navigation = true;
                }
                _ => {
                    // Unknown step — can't narrow the filter.
                    // This is conservative: we won't miss events.
                }
            }
        }

        Self {
            label_filter,
            property_keys,
            vertex_only,
            edge_only,
            has_navigation,
            steps: steps.iter().map(|s| s.clone_box()).collect(),
            source: source.clone(),
        }
    }
}
```

### StepIntrospect Trait

A new trait that concrete step types can implement to expose their filter constraints:

```rust
/// Trait for steps that can expose their filter constraints to the
/// reactive query matcher.
///
/// This is an optional opt-in trait. Steps that don't implement it
/// are treated conservatively (no filter narrowing).
pub trait StepIntrospect {
    /// Returns label constraints, if this step filters by label.
    fn label_constraints(&self) -> Option<Vec<String>> { None }

    /// Returns property key constraints, if this step references properties.
    fn property_constraints(&self) -> Option<Vec<String>> { None }
}
```

Added to `DynStep`:

```rust
pub trait DynStep: Send + Sync {
    // ... existing methods ...

    /// Downcast to StepIntrospect for reactive query optimization.
    /// Returns None by default.
    fn as_introspectable(&self) -> Option<&dyn StepIntrospect> { None }
}
```

Implemented on concrete steps:

```rust
// In filter.rs
impl StepIntrospect for HasLabelStep {
    fn label_constraints(&self) -> Option<Vec<String>> {
        Some(self.labels.clone())
    }
}

impl StepIntrospect for HasStep {
    fn property_constraints(&self) -> Option<Vec<String>> {
        Some(vec![self.key.clone()])
    }
}

// etc. for HasValueStep, HasWhereStep, ValuesStep, ...
```

### Fast Rejection

```rust
impl QueryMatcher {
    /// O(1) fast rejection: can this event possibly affect query results?
    ///
    /// Returns `false` if the event can be safely ignored.
    /// Returns `true` if the event *might* affect results (requires
    /// full evaluation to confirm).
    ///
    /// # Fast Rejection Rules
    ///
    /// 1. Vertex-only query + edge event (without navigation) → reject
    /// 2. Edge-only query + vertex event (without navigation) → reject
    /// 3. Label filter + event label not in filter → reject
    /// 4. Property change + key not in referenced keys (without navigation) → reject
    pub fn might_match(&self, event: &GraphEvent) -> bool {
        match event {
            GraphEvent::Batch(events) => {
                events.iter().any(|e| self.might_match(e))
            }

            // Vertex events
            GraphEvent::VertexAdded { label, .. }
            | GraphEvent::VertexRemoved { label, .. } => {
                if self.edge_only && !self.has_navigation {
                    return false;
                }
                if let Some(ref filter) = self.label_filter {
                    if !self.has_navigation && !filter.contains(label.as_str()) {
                        return false;
                    }
                }
                true
            }

            GraphEvent::VertexPropertyChanged { key, .. } => {
                if self.edge_only && !self.has_navigation {
                    return false;
                }
                // If we track specific property keys and this key isn't one of them,
                // AND there's no navigation (which could make any vertex relevant)
                if !self.has_navigation
                    && !self.property_keys.is_empty()
                    && !self.property_keys.contains(key.as_str())
                {
                    return false;
                }
                true
            }

            // Edge events
            GraphEvent::EdgeAdded { label, .. }
            | GraphEvent::EdgeRemoved { label, .. } => {
                if self.vertex_only && !self.has_navigation {
                    return false;
                }
                if let Some(ref filter) = self.label_filter {
                    if !self.has_navigation && !filter.contains(label.as_str()) {
                        return false;
                    }
                }
                true
            }

            GraphEvent::EdgePropertyChanged { key, .. } => {
                if self.vertex_only && !self.has_navigation {
                    return false;
                }
                if !self.has_navigation
                    && !self.property_keys.is_empty()
                    && !self.property_keys.contains(key.as_str())
                {
                    return false;
                }
                true
            }
        }
    }
}
```

### Full Evaluation

After `might_match` passes, the matcher re-evaluates the traversal against the affected element(s):

```rust
/// Result of evaluating a query against a graph event.
#[derive(Debug)]
pub struct EvalResult {
    /// Elements that now match the query (were not in matched_set before).
    pub added: Vec<Value>,
    /// Element IDs that no longer match (were in matched_set before).
    pub removed: Vec<ElementId>,
}

/// Identifies a vertex or edge.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ElementId {
    Vertex(VertexId),
    Edge(EdgeId),
}

impl QueryMatcher {
    /// Full re-evaluation of the traversal for the affected element(s).
    ///
    /// This takes a snapshot and runs the traversal, then compares
    /// results against the subscription's `matched_set`.
    ///
    /// # Strategy by Event Type
    ///
    /// - **VertexAdded / EdgeAdded**: Run the traversal starting from the
    ///   new element. If it produces results, they're new matches.
    ///
    /// - **VertexRemoved / EdgeRemoved**: The element is gone. If it was
    ///   in the `matched_set`, it's a removal.
    ///
    /// - **PropertyChanged**: Run the traversal starting from the affected
    ///   element. Compare with `matched_set` to detect additions/removals.
    ///
    /// - **Navigation-heavy queries**: For queries with `out()`, `in()`, etc.,
    ///   a property change on vertex A could cause vertex B to enter/leave
    ///   the result set. In this case, we must re-run the full traversal
    ///   (not just from the affected element). This is the expensive path.
    pub fn evaluate(
        &self,
        snapshot: &dyn SnapshotLike,
        event: &GraphEvent,
        matched_set: &HashSet<ElementId>,
    ) -> EvalResult {
        match event {
            GraphEvent::VertexRemoved { id, .. } => {
                let eid = ElementId::Vertex(*id);
                if matched_set.contains(&eid) {
                    EvalResult {
                        added: vec![],
                        removed: vec![eid],
                    }
                } else {
                    EvalResult::empty()
                }
            }

            GraphEvent::EdgeRemoved { id, .. } => {
                let eid = ElementId::Edge(*id);
                if matched_set.contains(&eid) {
                    EvalResult {
                        added: vec![],
                        removed: vec![eid],
                    }
                } else {
                    EvalResult::empty()
                }
            }

            GraphEvent::VertexAdded { id, .. }
            | GraphEvent::VertexPropertyChanged { id, .. } => {
                if self.has_navigation {
                    self.full_reevaluate(snapshot, matched_set)
                } else {
                    self.evaluate_from_vertex(snapshot, *id, matched_set)
                }
            }

            GraphEvent::EdgeAdded { id, .. }
            | GraphEvent::EdgePropertyChanged { id, .. } => {
                if self.has_navigation {
                    self.full_reevaluate(snapshot, matched_set)
                } else {
                    self.evaluate_from_edge(snapshot, *id, matched_set)
                }
            }

            GraphEvent::Batch(events) => {
                // For batches, do a single full re-evaluation
                // rather than per-event evaluation
                self.full_reevaluate(snapshot, matched_set)
            }
        }
    }

    /// Re-run the full traversal and diff against matched_set.
    fn full_reevaluate(
        &self,
        snapshot: &dyn SnapshotLike,
        matched_set: &HashSet<ElementId>,
    ) -> EvalResult {
        let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());
        let results = execute_full_traversal(&ctx, &self.source, &self.steps);

        let mut current_matches = HashSet::new();
        let mut added = Vec::new();

        for value in &results {
            if let Some(eid) = value_to_element_id(value) {
                current_matches.insert(eid.clone());
                if !matched_set.contains(&eid) {
                    added.push(value.clone());
                }
            }
        }

        let removed: Vec<ElementId> = matched_set
            .iter()
            .filter(|eid| !current_matches.contains(eid))
            .cloned()
            .collect();

        EvalResult { added, removed }
    }

    /// Evaluate from a specific vertex (non-navigation queries).
    fn evaluate_from_vertex(
        &self,
        snapshot: &dyn SnapshotLike,
        id: VertexId,
        matched_set: &HashSet<ElementId>,
    ) -> EvalResult {
        let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

        // Run the traversal starting from just this vertex
        let source = TraversalSource::Vertices(vec![id]);
        let results = execute_full_traversal(&ctx, &Some(source), &self.steps);

        let eid = ElementId::Vertex(id);

        if results.is_empty() {
            // Vertex doesn't match. If it was in matched_set, it's removed.
            if matched_set.contains(&eid) {
                EvalResult { added: vec![], removed: vec![eid] }
            } else {
                EvalResult::empty()
            }
        } else {
            // Vertex matches. If it wasn't in matched_set, it's added.
            if matched_set.contains(&eid) {
                EvalResult::empty() // Already matched — could be Updated
            } else {
                EvalResult { added: results, removed: vec![] }
            }
        }
    }

    /// Evaluate from a specific edge (non-navigation queries).
    fn evaluate_from_edge(
        &self,
        snapshot: &dyn SnapshotLike,
        id: EdgeId,
        matched_set: &HashSet<ElementId>,
    ) -> EvalResult {
        // Same pattern as evaluate_from_vertex but for edges
        let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());
        let source = TraversalSource::Edges(vec![id]);
        let results = execute_full_traversal(&ctx, &Some(source), &self.steps);

        let eid = ElementId::Edge(id);

        if results.is_empty() {
            if matched_set.contains(&eid) {
                EvalResult { added: vec![], removed: vec![eid] }
            } else {
                EvalResult::empty()
            }
        } else {
            if matched_set.contains(&eid) {
                EvalResult::empty()
            } else {
                EvalResult { added: results, removed: vec![] }
            }
        }
    }
}

impl EvalResult {
    pub fn empty() -> Self {
        Self { added: vec![], removed: vec![] }
    }

    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }
}

/// Helper: extract ElementId from a Value.
fn value_to_element_id(value: &Value) -> Option<ElementId> {
    match value {
        Value::Vertex(id) => Some(ElementId::Vertex(*id)),
        Value::Edge(id) => Some(ElementId::Edge(*id)),
        _ => None,
    }
}
```

---

## Layer 4: SubscriptionManager and Subscription

### SubscriptionId

```rust
use std::sync::atomic::{AtomicU64, Ordering};

/// Unique identifier for a subscription.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SubscriptionId(u64);

static NEXT_SUBSCRIPTION_ID: AtomicU64 = AtomicU64::new(0);

impl SubscriptionId {
    fn next() -> Self {
        Self(NEXT_SUBSCRIPTION_ID.fetch_add(1, Ordering::Relaxed))
    }
}
```

### SubscriptionEvent

```rust
/// Event delivered to subscribers.
///
/// Contains the matched values, the type of change, and a reference
/// to the source `GraphEvent` that triggered the match.
#[derive(Clone, Debug)]
pub struct SubscriptionEvent {
    /// What kind of change occurred relative to the subscription.
    pub event_type: SubscriptionEventType,

    /// The matched values from the traversal.
    ///
    /// - For `Added`: the newly matching values
    /// - For `Removed`: the element IDs that no longer match (as `Value::Vertex` or `Value::Edge`)
    /// - For `Updated`: the current values of the still-matching elements
    pub values: Vec<Value>,

    /// The source graph mutation that triggered this subscription event.
    pub source_event: GraphEvent,
}

/// The type of change relative to a subscription's result set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubscriptionEventType {
    /// New elements entered the result set.
    Added,
    /// Previously matching elements left the result set.
    Removed,
    /// Matching elements were modified but still match.
    Updated,
}
```

### SubscribeOptions

```rust
/// Configuration for a subscription.
///
/// Use the builder pattern to customize subscription behavior.
///
/// # Example
///
/// ```ignore
/// let sub = g.v().has_label("person")
///     .subscribe_with(
///         SubscribeOptions::new()
///             .capacity(4096)
///             .include_initial(true)
///     );
/// ```
#[derive(Clone, Debug)]
pub struct SubscribeOptions {
    /// Channel capacity (default: 1024).
    pub capacity: usize,
    /// If true, run the traversal immediately and emit `Added` events
    /// for all currently matching elements before streaming live events.
    pub include_initial: bool,
}

impl SubscribeOptions {
    pub fn new() -> Self {
        Self {
            capacity: EventBus::DEFAULT_CAPACITY,
            include_initial: false,
        }
    }

    pub fn capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    pub fn include_initial(mut self, include: bool) -> Self {
        self.include_initial = include;
        self
    }
}

impl Default for SubscribeOptions {
    fn default() -> Self {
        Self::new()
    }
}
```

### Subscription

```rust
/// A live subscription to a reactive query.
///
/// Receives `SubscriptionEvent`s as graph mutations match the
/// subscribed traversal pattern. Implements `Iterator` for
/// synchronous consumption.
///
/// # Cancellation
///
/// Drop the `Subscription` or call `cancel()` to unsubscribe.
/// The dispatcher thread detects the closed channel and cleans up.
///
/// # Async Integration
///
/// For async runtimes, use `into_receiver()` to get the raw
/// `mpsc::Receiver` and wrap it in your runtime's async channel:
///
/// ```ignore
/// // tokio example
/// let rx = subscription.into_receiver();
/// tokio::task::spawn_blocking(move || {
///     while let Ok(event) = rx.recv() {
///         // process event
///     }
/// });
/// ```
pub struct Subscription {
    /// Subscription identifier.
    id: SubscriptionId,
    /// Receiver for subscription events.
    rx: std::sync::mpsc::Receiver<SubscriptionEvent>,
    /// Shared handle to signal cancellation.
    cancel_flag: Arc<AtomicBool>,
}

impl Subscription {
    /// Get the subscription's unique identifier.
    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    /// Blocking receive. Blocks until the next event is available
    /// or the subscription is cancelled.
    pub fn recv(&self) -> Option<SubscriptionEvent> {
        self.rx.recv().ok()
    }

    /// Non-blocking try_recv. Returns immediately.
    ///
    /// Returns `Ok(event)` if an event is available, `Err(TryRecvError::Empty)`
    /// if no events are pending, or `Err(TryRecvError::Disconnected)` if
    /// the subscription has been cancelled.
    pub fn try_recv(&self) -> Result<SubscriptionEvent, std::sync::mpsc::TryRecvError> {
        self.rx.try_recv()
    }

    /// Consume the subscription and return the raw receiver.
    ///
    /// Use this for integration with async runtimes.
    pub fn into_receiver(self) -> std::sync::mpsc::Receiver<SubscriptionEvent> {
        self.rx
    }

    /// Cancel the subscription and clean up resources.
    ///
    /// After cancellation, no more events will be received.
    /// This is equivalent to dropping the `Subscription`.
    pub fn cancel(self) {
        self.cancel_flag.store(true, Ordering::Release);
        // drop self — receiver is closed
    }
}

impl Iterator for Subscription {
    type Item = SubscriptionEvent;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.cancel_flag.store(true, Ordering::Release);
    }
}
```

### ActiveSubscription (internal)

```rust
/// Internal representation of an active subscription within
/// the dispatcher thread.
struct ActiveSubscription {
    id: SubscriptionId,
    matcher: QueryMatcher,
    tx: std::sync::mpsc::SyncSender<SubscriptionEvent>,
    cancel_flag: Arc<AtomicBool>,

    /// Tracks which elements currently match this subscription's query.
    /// Used for detecting `Removed` and `Updated` events.
    matched_set: HashSet<ElementId>,

    /// Function to create a snapshot for re-evaluation.
    /// Returns a boxed SnapshotLike.
    snapshot_fn: Arc<dyn Fn() -> Box<dyn SnapshotLike + Send> + Send + Sync>,
}

impl ActiveSubscription {
    /// Returns true if the subscription has been cancelled.
    fn is_cancelled(&self) -> bool {
        self.cancel_flag.load(Ordering::Acquire)
    }

    /// Process a graph event, potentially sending subscription events.
    ///
    /// Returns `false` if the subscription should be removed (cancelled
    /// or receiver dropped).
    fn process_event(&mut self, event: &GraphEvent) -> bool {
        if self.is_cancelled() {
            return false;
        }

        if !self.matcher.might_match(event) {
            return true;
        }

        let snapshot = (self.snapshot_fn)();
        let result = self.matcher.evaluate(
            snapshot.as_ref(),
            event,
            &self.matched_set,
        );

        if result.is_empty() {
            // Check if this is a property change on a matched element
            // If so, emit Updated
            if let Some(eid) = event_to_element_id(event) {
                if self.matched_set.contains(&eid) {
                    let sub_event = SubscriptionEvent {
                        event_type: SubscriptionEventType::Updated,
                        values: vec![element_id_to_value(&eid)],
                        source_event: event.clone(),
                    };
                    return self.send(sub_event);
                }
            }
            return true;
        }

        // Process additions
        if !result.added.is_empty() {
            for value in &result.added {
                if let Some(eid) = value_to_element_id(value) {
                    self.matched_set.insert(eid);
                }
            }
            let sub_event = SubscriptionEvent {
                event_type: SubscriptionEventType::Added,
                values: result.added,
                source_event: event.clone(),
            };
            if !self.send(sub_event) {
                return false;
            }
        }

        // Process removals
        if !result.removed.is_empty() {
            let values: Vec<Value> = result.removed.iter()
                .map(element_id_to_value)
                .collect();
            for eid in &result.removed {
                self.matched_set.remove(eid);
            }
            let sub_event = SubscriptionEvent {
                event_type: SubscriptionEventType::Removed,
                values,
                source_event: event.clone(),
            };
            if !self.send(sub_event) {
                return false;
            }
        }

        true
    }

    /// Send a subscription event. Returns false if the receiver is disconnected.
    fn send(&self, event: SubscriptionEvent) -> bool {
        match self.tx.try_send(event) {
            Ok(()) => true,
            Err(std::sync::mpsc::TrySendError::Full(_)) => true,  // drop event, keep sub
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => false,  // remove sub
        }
    }
}

fn event_to_element_id(event: &GraphEvent) -> Option<ElementId> {
    match event {
        GraphEvent::VertexPropertyChanged { id, .. } => Some(ElementId::Vertex(*id)),
        GraphEvent::EdgePropertyChanged { id, .. } => Some(ElementId::Edge(*id)),
        _ => None,
    }
}

fn element_id_to_value(eid: &ElementId) -> Value {
    match eid {
        ElementId::Vertex(id) => Value::Vertex(*id),
        ElementId::Edge(id) => Value::Edge(*id),
    }
}
```

### SubscriptionManager

```rust
/// Manages active subscriptions and dispatches graph events.
///
/// The manager lazily spawns a background `std::thread` when the first
/// subscription is created. The thread blocks on the event channel and
/// dispatches events to matching subscriptions. When the last subscription
/// is dropped, the thread shuts down.
///
/// # Thread Safety
///
/// New subscriptions can be registered from any thread via `subscribe()`.
/// The dispatcher thread is the only reader of the event channel and
/// the only writer of `ActiveSubscription::matched_set`.
pub struct SubscriptionManager {
    /// Channel for registering new subscriptions with the dispatcher.
    register_tx: parking_lot::Mutex<Option<std::sync::mpsc::Sender<ActiveSubscription>>>,

    /// Handle to the event bus (for subscribing to graph events).
    event_bus: Arc<EventBus>,

    /// Snapshot factory for the dispatcher thread.
    snapshot_fn: Arc<dyn Fn() -> Box<dyn SnapshotLike + Send> + Send + Sync>,

    /// Handle to the dispatcher thread.
    thread_handle: parking_lot::Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl SubscriptionManager {
    /// Create a new subscription manager.
    ///
    /// The dispatcher thread is NOT started yet — it's lazily spawned
    /// on the first `subscribe()` call.
    pub fn new(
        event_bus: Arc<EventBus>,
        snapshot_fn: Arc<dyn Fn() -> Box<dyn SnapshotLike + Send> + Send + Sync>,
    ) -> Self {
        Self {
            register_tx: parking_lot::Mutex::new(None),
            event_bus,
            snapshot_fn,
            thread_handle: parking_lot::Mutex::new(None),
        }
    }

    /// Subscribe with a compiled matcher and options.
    pub fn subscribe(
        &self,
        matcher: QueryMatcher,
        opts: SubscribeOptions,
    ) -> Subscription {
        let id = SubscriptionId::next();
        let (sub_tx, sub_rx) = std::sync::mpsc::sync_channel(opts.capacity);
        let cancel_flag = Arc::new(AtomicBool::new(false));

        let active = ActiveSubscription {
            id,
            matcher,
            tx: sub_tx,
            cancel_flag: cancel_flag.clone(),
            matched_set: HashSet::new(),
            snapshot_fn: self.snapshot_fn.clone(),
        };

        // Ensure dispatcher thread is running
        self.ensure_dispatcher();

        // Register with dispatcher
        if let Some(ref tx) = *self.register_tx.lock() {
            let _ = tx.send(active);
        }

        // If include_initial, run the traversal now and populate matched_set
        // (handled by the dispatcher after registration)

        Subscription {
            id,
            rx: sub_rx,
            cancel_flag,
        }
    }

    /// Ensure the dispatcher thread is running.
    fn ensure_dispatcher(&self) {
        let mut handle = self.thread_handle.lock();
        if handle.is_some() {
            return; // Already running
        }

        let (register_tx, register_rx) = std::sync::mpsc::channel::<ActiveSubscription>();
        *self.register_tx.lock() = Some(register_tx);

        let event_rx = self.event_bus.subscribe();

        let thread = std::thread::Builder::new()
            .name("interstellar-reactive-dispatcher".to_string())
            .spawn(move || {
                Self::dispatcher_loop(event_rx, register_rx);
            })
            .expect("failed to spawn reactive dispatcher thread");

        *handle = Some(thread);
    }

    /// Main dispatcher loop running on the background thread.
    fn dispatcher_loop(
        event_rx: std::sync::mpsc::Receiver<GraphEvent>,
        register_rx: std::sync::mpsc::Receiver<ActiveSubscription>,
    ) {
        let mut subscriptions: Vec<ActiveSubscription> = Vec::new();

        loop {
            // Drain any newly registered subscriptions (non-blocking)
            while let Ok(sub) = register_rx.try_recv() {
                subscriptions.push(sub);
            }

            // Block until next event (or channel closed)
            match event_rx.recv() {
                Ok(event) => {
                    // Dispatch to all active subscriptions
                    subscriptions.retain_mut(|sub| sub.process_event(&event));

                    // If no subscriptions left, exit the thread
                    // (it will be re-spawned on next subscribe())
                    if subscriptions.is_empty() {
                        // Drain register_rx one more time
                        while let Ok(sub) = register_rx.try_recv() {
                            subscriptions.push(sub);
                        }
                        if subscriptions.is_empty() {
                            break;
                        }
                    }
                }
                Err(_) => {
                    // Event bus channel closed (Graph dropped)
                    break;
                }
            }
        }
    }
}

impl Drop for SubscriptionManager {
    fn drop(&mut self) {
        // Close the register channel
        *self.register_tx.lock() = None;
        // The dispatcher thread will exit when both channels are closed
        if let Some(handle) = self.thread_handle.lock().take() {
            let _ = handle.join();
        }
    }
}
```

### Dispatcher Thread Lifecycle

```
First .subscribe() call
        │
        ▼
  ensure_dispatcher()
        │
        ├── Already running? → return
        │
        └── Spawn thread:
              │
              ▼
         dispatcher_loop()
              │
              ├── Block on event_rx.recv()
              │     │
              │     ├── Got event → dispatch to all subscriptions
              │     │     │
              │     │     └── subscriptions empty? → drain register_rx → still empty? → exit
              │     │
              │     └── Channel closed (Graph dropped) → exit
              │
              └── Check register_rx for new subscriptions (non-blocking)

  Thread exits
        │
        ▼
  Next .subscribe() call re-spawns
```

---

## Layer 5: API Surface

### BoundTraversal Extensions (source.rs)

```rust
#[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
impl<'g, Out: OutputMarker> BoundTraversal<'g, Out> {
    /// Subscribe to this traversal pattern reactively.
    ///
    /// Returns a `Subscription` that yields `SubscriptionEvent`s whenever
    /// graph mutations cause elements to match or stop matching this
    /// traversal pattern.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::prelude::*;
    ///
    /// let graph = Graph::new();
    /// let snapshot = graph.snapshot();
    /// let g = snapshot.gremlin();
    ///
    /// // Subscribe to all people over 30
    /// let sub = g.v()
    ///     .has_label("person")
    ///     .has_where("age", p::gt(30))
    ///     .subscribe();
    ///
    /// // Add a matching vertex in another thread
    /// graph.add_vertex("person", props! { "name" => "Alice", "age" => 35i64 });
    ///
    /// // Receive the event
    /// let event = sub.recv().unwrap();
    /// assert_eq!(event.event_type, SubscriptionEventType::Added);
    /// ```
    ///
    /// # Backpressure
    ///
    /// Uses a bounded channel with capacity 1024. Events are dropped
    /// (not blocking mutations) when the subscriber falls behind.
    /// Use `subscribe_with` for custom capacity.
    pub fn subscribe(&self) -> Subscription {
        self.subscribe_with(SubscribeOptions::default())
    }

    /// Subscribe with custom options.
    ///
    /// See [`SubscribeOptions`] for available configuration.
    pub fn subscribe_with(&self, opts: SubscribeOptions) -> Subscription {
        let matcher = QueryMatcher::compile(
            self.traversal.steps(),
            &self.traversal.source(),
        );
        self.snapshot
            .subscription_manager()
            .subscribe(matcher, opts)
    }
}
```

### SnapshotLike Trait Extension

The `SnapshotLike` trait (in `traversal/context.rs`) needs a method to access the `SubscriptionManager`:

```rust
#[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
pub trait ReactiveSnapshotLike {
    /// Get the subscription manager for creating reactive subscriptions.
    fn subscription_manager(&self) -> &SubscriptionManager;
}
```

Implemented on `GraphSnapshot`:

```rust
#[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
impl ReactiveSnapshotLike for GraphSnapshot {
    fn subscription_manager(&self) -> &SubscriptionManager {
        self.state.subscription_manager()
    }
}
```

The `SubscriptionManager` is stored in the `Graph`, not the snapshot. The snapshot holds an `Arc` reference back to the graph's `SubscriptionManager`.

### Graph Extensions

```rust
#[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
impl Graph {
    /// Get the subscription manager.
    pub fn subscription_manager(&self) -> &SubscriptionManager {
        &self.subscription_manager
    }
}
```

The `Graph` struct gets an additional field:

```rust
pub struct Graph {
    state: RwLock<GraphState>,
    schema: RwLock<Option<GraphSchema>>,
    #[cfg(feature = "reactive")]
    event_bus: EventBus,
    #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
    subscription_manager: SubscriptionManager,
}
```

### Prelude Additions

```rust
// In lib.rs prelude
#[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
pub use crate::storage::events::GraphEvent;
#[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
pub use crate::traversal::reactive::{
    ElementId, EvalResult, QueryMatcher, SubscribeOptions,
    Subscription, SubscriptionEvent, SubscriptionEventType,
    SubscriptionId, SubscriptionManager,
};
```

---

## Error Handling

New error variants in `error.rs`:

```rust
#[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
#[derive(Debug, Error)]
pub enum SubscriptionError {
    /// The subscription was cancelled.
    #[error("subscription cancelled")]
    Cancelled,

    /// The graph was dropped while the subscription was active.
    #[error("graph dropped")]
    GraphDropped,

    /// The subscription's event channel is full and events are being dropped.
    #[error("subscription channel full (capacity: {capacity})")]
    ChannelFull { capacity: usize },
}
```

---

## Implementation Phases

### Phase 1: Event Infrastructure
**Files**: `storage/events.rs`, `storage/mod.rs`, `Cargo.toml`

- [ ] Define `GraphEvent` enum with all variants
- [ ] Implement `EventBus` with `subscribe()`, `subscribe_with_capacity()`, `emit()`
- [ ] Add `reactive` feature flag to `Cargo.toml`
- [ ] Add `pub mod events` to `storage/mod.rs` behind feature gate
- [ ] Unit tests for `GraphEvent` helper methods (`vertex_id`, `label`, `flatten`, etc.)
- [ ] Unit tests for `EventBus` (subscribe, emit, dead subscriber cleanup, fast path)

### Phase 2: Graph Event Emission (In-Memory)
**Files**: `storage/cow.rs`

- [ ] Add `EventBus` field to `Graph`
- [ ] Add `event_bus()` accessor
- [ ] Instrument `add_vertex` (line 1089)
- [ ] Instrument `add_edge` (line 1157)
- [ ] Instrument `set_vertex_property` (line 1233) — capture old value
- [ ] Instrument `set_edge_property` (line 1277) — capture old value
- [ ] Instrument `remove_vertex` (line 1318) — capture label + incident edges before deletion
- [ ] Instrument `remove_edge` (line 1396) — capture endpoints + label before deletion
- [ ] Instrument `BatchContext` to collect events
- [ ] Instrument `Graph::batch()` to emit `Batch` event on success
- [ ] Integration tests: verify events emitted for each mutation type
- [ ] Integration tests: verify no events on failed mutations (e.g., VertexNotFound)
- [ ] Integration tests: verify batch events
- [ ] Benchmark: measure mutation overhead with 0 subscribers

### Phase 3: CowMmapGraph Event Emission
**Files**: `storage/cow_mmap.rs`

- [ ] Add `EventBus` field to `CowMmapGraph`
- [ ] Instrument all 6 mutation methods (same pattern as Phase 2)
- [ ] Instrument `CowMmapBatchContext` (all 6 mutation methods)
- [ ] Instrument `CowMmapGraph::batch()` to emit `Batch` event
- [ ] Integration tests mirroring Phase 2 for mmap backend

### Phase 4: QueryMatcher
**Files**: `traversal/reactive.rs`, `traversal/mod.rs`

- [ ] Define `StepIntrospect` trait
- [ ] Add `as_introspectable()` to `DynStep`
- [ ] Implement `StepIntrospect` on `HasLabelStep`, `HasStep`, `HasValueStep`, `HasWhereStep`, `ValuesStep`
- [ ] Implement `QueryMatcher::compile()`
- [ ] Implement `QueryMatcher::might_match()`
- [ ] Implement `QueryMatcher::evaluate()` and internal helpers
- [ ] Define `EvalResult`, `ElementId`
- [ ] Unit tests for `compile()` — verify extracted labels, property keys, navigation detection
- [ ] Unit tests for `might_match()` — verify fast rejection for all event/query combinations
- [ ] Integration tests for `evaluate()` — verify correct added/removed detection

### Phase 5: Subscription Infrastructure
**Files**: `traversal/reactive.rs`

- [ ] Define `SubscriptionId`, `SubscriptionEvent`, `SubscriptionEventType`
- [ ] Define `SubscribeOptions`
- [ ] Implement `Subscription` (recv, try_recv, into_receiver, cancel, Iterator, Drop)
- [ ] Implement `ActiveSubscription` with `matched_set` tracking
- [ ] Implement `SubscriptionManager` with lazy thread spawning
- [ ] Implement `dispatcher_loop`
- [ ] Unit tests: subscription lifecycle (create, receive, cancel)
- [ ] Unit tests: matched_set tracking (Added → Updated → Removed)
- [ ] Integration tests: multi-subscription dispatch
- [ ] Integration tests: thread shutdown when last subscription dropped

### Phase 6: API Surface
**Files**: `traversal/source.rs`, `traversal/context.rs`, `lib.rs`, `error.rs`

- [ ] Add `subscribe()` and `subscribe_with()` to `BoundTraversal`
- [ ] Add `ReactiveSnapshotLike` trait
- [ ] Implement for `GraphSnapshot` and `CowMmapSnapshot`
- [ ] Add `SubscriptionManager` field to `Graph` and `CowMmapGraph`
- [ ] Wire snapshot → subscription manager access
- [ ] Add prelude exports
- [ ] Add `SubscriptionError` variants
- [ ] End-to-end integration test:
  ```rust
  let graph = Graph::new();
  let snapshot = graph.snapshot();
  let g = snapshot.gremlin();
  let sub = g.v().has_label("person").has_where("age", p::gt(30)).subscribe();
  graph.add_vertex("person", props! { "age" => 35i64 });
  let event = sub.recv().unwrap();
  assert_eq!(event.event_type, SubscriptionEventType::Added);
  ```

### Phase 7: Polish and Documentation
- [ ] Doc comments on all public types
- [ ] Example program: `examples/reactive_queries.rs`
- [ ] Benchmark: dispatch latency (mutation → subscriber receives event)
- [ ] Benchmark: throughput with N subscriptions
- [ ] Update `lib.rs` module overview table
- [ ] Update `AGENTS.md` if needed
- [ ] `include_initial` option implementation (initial snapshot evaluation)

---

## Testing Strategy

### Unit Tests

| Component | Test Cases |
|-----------|------------|
| `GraphEvent` | Helper methods, flatten, equality, clone |
| `EventBus` | Zero subscribers fast path, single/multi subscriber, dead subscriber cleanup, capacity overflow |
| `QueryMatcher::compile` | Label extraction, property key extraction, navigation detection, empty steps |
| `QueryMatcher::might_match` | All combinations of event type × query type (vertex-only, edge-only, with/without navigation, with/without label filter) |
| `QueryMatcher::evaluate` | Add/remove/update detection, navigation queries, batch events |
| `ActiveSubscription` | matched_set lifecycle, cancellation, channel full behavior |
| `Subscription` | Iterator impl, recv/try_recv, cancel/drop |

### Integration Tests

```
tests/
└── reactive/
    ├── mod.rs
    ├── event_bus_tests.rs       # EventBus with real Graph mutations
    ├── subscription_tests.rs    # End-to-end subscribe → mutate → receive
    ├── removal_tests.rs         # Removal detection scenarios
    ├── batch_tests.rs           # Batch mutation events
    ├── navigation_tests.rs      # Queries with out()/in() steps
    ├── multi_sub_tests.rs       # Multiple concurrent subscriptions
    ├── backpressure_tests.rs    # Channel full behavior
    └── mmap_tests.rs            # Same tests against CowMmapGraph
```

### Key Test Scenarios

1. **Basic addition**: Subscribe to `g.v().has_label("person")`, add a person vertex → receive `Added`
2. **Non-matching addition**: Subscribe to same, add a "software" vertex → receive nothing
3. **Property-triggered match**: Subscribe to `g.v().has_where("age", p::gt(30))`, add vertex with age=25, then update to age=35 → receive `Added`
4. **Property-triggered removal**: Subscribe to same, vertex has age=35, update to age=25 → receive `Removed`
5. **Vertex deletion**: Subscribe, matching vertex is removed → receive `Removed`
6. **Navigation query**: Subscribe to `g.v().has_label("person").out("knows")`, add an edge from person to another → receive `Added` for the target
7. **Batch**: Subscribe, batch adds 3 matching vertices → receive events for all 3
8. **Multiple subscriptions**: Two different subscriptions on the same graph, same mutation matches one but not the other
9. **Backpressure**: Subscribe with capacity=1, emit 100 events → subscriber gets 1, rest are dropped, no mutation blocking
10. **Thread lifecycle**: Subscribe → unsubscribe → subscribe again → verify dispatcher re-spawns

---

## Performance Considerations

### Mutation Path Overhead

| Subscribers | Overhead per mutation |
|-------------|----------------------|
| 0 | ~1ns (atomic load) |
| 1+ | ~200ns (Mutex lock + `try_send` + event clone) |

The `AtomicUsize` fast path ensures zero cost when reactive features are enabled but no subscriptions are active.

### Dispatcher Throughput

The dispatcher thread processes events sequentially. For each event:

1. `might_match()`: O(1) — HashSet lookups
2. `evaluate()` (if needed): O(traversal complexity) — takes a snapshot and runs the traversal

For non-navigation queries, evaluation targets a single element (O(steps)). For navigation queries, full re-evaluation is O(V + E) in the worst case.

### Optimization Opportunities (Future)

- **Incremental evaluation**: For navigation queries, track which vertices are "boundary" vertices and only re-evaluate from those
- **Parallel dispatch**: Use a thread pool for evaluating multiple subscriptions concurrently
- **Event coalescing**: Batch rapid mutations into a single evaluation pass
- **Index-accelerated matching**: Use property indexes to speed up `evaluate()`
- **Subscription sharing**: Multiple subscriptions with the same traversal can share evaluation results

---

## Open Design Notes

### Why `std::sync::mpsc` Instead of Crossbeam

`std::sync::mpsc::sync_channel` provides bounded channels without adding a new dependency. The `SyncSender::try_send` gives us non-blocking behavior. If benchmarks show contention is a bottleneck, switching to `crossbeam-channel` is a drop-in replacement.

### Why a Dedicated Thread Instead of Inline Dispatch

Dispatching inline (in the mutation method) would block the mutation path on potentially expensive `evaluate()` calls. The dedicated thread decouples mutation latency from subscription evaluation cost.

### Why `matched_set` Over Differential Evaluation

Maintaining a `HashSet<ElementId>` per subscription is simple and correct. Differential dataflow or incremental view maintenance would be more efficient for complex queries but dramatically increases implementation complexity. The `matched_set` approach can be optimized later without API changes.

### Batch Event Atomicity

When `Graph::batch()` commits, we emit a single `GraphEvent::Batch(events)`. The dispatcher processes the entire batch before updating `matched_set`, so subscribers see a consistent view. They don't see intermediate states where some mutations in the batch are applied but not others.
