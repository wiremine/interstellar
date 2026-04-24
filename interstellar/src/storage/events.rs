//! Graph mutation events and event bus for reactive streaming queries.
//!
//! This module provides the [`GraphEvent`] enum representing graph mutations
//! and the [`EventBus`] for broadcasting events to subscribers.
//!
//! # Zero-Cost When Unused
//!
//! When no subscribers exist, [`EventBus::emit`] performs a single atomic load
//! and returns immediately — no allocation, no locking.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;

use parking_lot::Mutex;

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
    VertexRemoved { id: VertexId, label: String },

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

    /// Returns `true` if this is a vertex event.
    pub fn is_vertex_event(&self) -> bool {
        matches!(
            self,
            Self::VertexAdded { .. }
                | Self::VertexRemoved { .. }
                | Self::VertexPropertyChanged { .. }
        )
    }

    /// Returns `true` if this is an edge event.
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
            Self::Batch(events) => events.into_iter().flat_map(|e| e.flatten()).collect(),
            other => vec![other],
        }
    }
}

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
    pub fn emit(&self, event: GraphEvent) {
        // Fast path: no subscribers
        if self.subscriber_count.load(Ordering::Acquire) == 0 {
            return;
        }

        let mut subs = self.subscribers.lock();

        // Send to all subscribers, tracking which are still alive
        subs.retain(|tx| {
            match tx.try_send(event.clone()) {
                Ok(()) => true,
                Err(mpsc::TrySendError::Full(_)) => true, // subscriber is slow, keep it
                Err(mpsc::TrySendError::Disconnected(_)) => false, // subscriber dropped
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

impl std::fmt::Debug for EventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventBus")
            .field("subscriber_count", &self.subscriber_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_graph_event_vertex_id() {
        let event = GraphEvent::VertexAdded {
            id: VertexId(1),
            label: "person".to_string(),
            properties: HashMap::new(),
        };
        assert_eq!(event.vertex_id(), Some(VertexId(1)));
        assert_eq!(event.edge_id(), None);
    }

    #[test]
    fn test_graph_event_edge_id() {
        let event = GraphEvent::EdgeAdded {
            id: EdgeId(10),
            src: VertexId(1),
            dst: VertexId(2),
            label: "knows".to_string(),
            properties: HashMap::new(),
        };
        assert_eq!(event.edge_id(), Some(EdgeId(10)));
        assert_eq!(event.vertex_id(), None);
    }

    #[test]
    fn test_graph_event_label() {
        let event = GraphEvent::VertexAdded {
            id: VertexId(1),
            label: "person".to_string(),
            properties: HashMap::new(),
        };
        assert_eq!(event.label(), Some("person"));

        let prop_event = GraphEvent::VertexPropertyChanged {
            id: VertexId(1),
            key: "age".to_string(),
            old_value: None,
            new_value: Value::Int(30),
        };
        assert_eq!(prop_event.label(), None);
        assert_eq!(prop_event.property_key(), Some("age"));
    }

    #[test]
    fn test_graph_event_is_methods() {
        let vertex_event = GraphEvent::VertexAdded {
            id: VertexId(1),
            label: "person".to_string(),
            properties: HashMap::new(),
        };
        assert!(vertex_event.is_vertex_event());
        assert!(!vertex_event.is_edge_event());
        assert!(!vertex_event.is_removal());
        assert!(!vertex_event.is_batch());

        let removal = GraphEvent::VertexRemoved {
            id: VertexId(1),
            label: "person".to_string(),
        };
        assert!(removal.is_removal());

        let batch = GraphEvent::Batch(vec![]);
        assert!(batch.is_batch());
    }

    #[test]
    fn test_graph_event_flatten() {
        let batch = GraphEvent::Batch(vec![
            GraphEvent::VertexAdded {
                id: VertexId(1),
                label: "a".to_string(),
                properties: HashMap::new(),
            },
            GraphEvent::Batch(vec![GraphEvent::VertexAdded {
                id: VertexId(2),
                label: "b".to_string(),
                properties: HashMap::new(),
            }]),
        ]);
        let flat = batch.flatten();
        assert_eq!(flat.len(), 2);
        assert_eq!(flat[0].vertex_id(), Some(VertexId(1)));
        assert_eq!(flat[1].vertex_id(), Some(VertexId(2)));
    }

    #[test]
    fn test_event_bus_no_subscribers_fast_path() {
        let bus = EventBus::new();
        assert!(bus.is_empty());
        assert_eq!(bus.subscriber_count(), 0);

        // Emit with no subscribers — should not panic
        bus.emit(GraphEvent::VertexAdded {
            id: VertexId(1),
            label: "test".to_string(),
            properties: HashMap::new(),
        });
    }

    #[test]
    fn test_event_bus_single_subscriber() {
        let bus = EventBus::new();
        let rx = bus.subscribe();

        assert_eq!(bus.subscriber_count(), 1);

        bus.emit(GraphEvent::VertexAdded {
            id: VertexId(1),
            label: "person".to_string(),
            properties: HashMap::new(),
        });

        let event = rx.try_recv().unwrap();
        assert_eq!(event.vertex_id(), Some(VertexId(1)));
    }

    #[test]
    fn test_event_bus_multiple_subscribers() {
        let bus = EventBus::new();
        let rx1 = bus.subscribe();
        let rx2 = bus.subscribe();

        assert_eq!(bus.subscriber_count(), 2);

        bus.emit(GraphEvent::VertexRemoved {
            id: VertexId(5),
            label: "test".to_string(),
        });

        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }

    #[test]
    fn test_event_bus_dead_subscriber_cleanup() {
        let bus = EventBus::new();
        let rx1 = bus.subscribe();
        let _rx2 = bus.subscribe();

        assert_eq!(bus.subscriber_count(), 2);

        // Drop rx1
        drop(rx1);

        // Next emit should clean up the dead subscriber
        bus.emit(GraphEvent::VertexAdded {
            id: VertexId(1),
            label: "test".to_string(),
            properties: HashMap::new(),
        });

        assert_eq!(bus.subscriber_count(), 1);
    }

    #[test]
    fn test_event_bus_capacity_overflow() {
        let bus = EventBus::new();
        let rx = bus.subscribe_with_capacity(2);

        // Fill the channel
        for i in 0..5 {
            bus.emit(GraphEvent::VertexAdded {
                id: VertexId(i),
                label: "test".to_string(),
                properties: HashMap::new(),
            });
        }

        // Should get first 2 (capacity), rest dropped
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok());
        // Channel might have some buffered, but not all 5
        // The subscriber should still be alive
        assert_eq!(bus.subscriber_count(), 1);
    }

    #[test]
    fn test_event_bus_all_subscribers_dropped() {
        let bus = EventBus::new();
        let rx = bus.subscribe();
        drop(rx);

        bus.emit(GraphEvent::VertexAdded {
            id: VertexId(1),
            label: "test".to_string(),
            properties: HashMap::new(),
        });

        assert_eq!(bus.subscriber_count(), 0);
        assert!(bus.is_empty());
    }
}
