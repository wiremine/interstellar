//! Reactive streaming query support: QueryMatcher, Subscription, SubscriptionManager.
//!
//! This module provides the core matching infrastructure for reactive
//! streaming queries. The `QueryMatcher` compiles a traversal into a
//! fast-rejection filter plus full re-evaluation capability.
//!
//! # Two-Phase Evaluation
//!
//! 1. **Fast rejection** (`might_match`): O(1) check using extracted label
//!    and property filters. Most events are rejected here.
//! 2. **Full evaluation** (`evaluate`): Re-runs the traversal against the
//!    affected element(s) and diffs against the current matched set.
//!
//! # Subscription Infrastructure
//!
//! - [`SubscriptionManager`]: Manages active subscriptions and lazily spawns
//!   a background dispatcher thread.
//! - [`Subscription`]: A live subscription handle that receives
//!   [`SubscriptionEvent`]s via an `mpsc` channel.
//! - [`SubscribeOptions`]: Builder for configuring subscription capacity
//!   and initial snapshot behavior.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::storage::events::{EventBus, GraphEvent};
use crate::storage::GraphStorage;
use crate::traversal::context::{ExecutionContext, SnapshotLike};
use crate::traversal::step::{execute_traversal, DynStep};
use crate::traversal::traverser::{TraversalSource, Traverser};
use crate::value::{EdgeId, Value, VertexId};

// =============================================================================
// StepIntrospect Trait
// =============================================================================

/// Trait for steps that can expose their filter constraints to the
/// reactive query matcher.
///
/// This is an optional opt-in trait. Steps that don't implement it
/// are treated conservatively (no filter narrowing).
pub trait StepIntrospect {
    /// Returns label constraints, if this step filters by label.
    fn label_constraints(&self) -> Option<Vec<String>> {
        None
    }

    /// Returns property key constraints, if this step references properties.
    fn property_constraints(&self) -> Option<Vec<String>> {
        None
    }
}

// =============================================================================
// ElementId
// =============================================================================

/// Identifies a vertex or edge for matched-set tracking.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ElementId {
    Vertex(VertexId),
    Edge(EdgeId),
}

// =============================================================================
// EvalResult
// =============================================================================

/// Result of evaluating a query against a graph event.
#[derive(Debug)]
pub struct EvalResult {
    /// Elements that now match the query (were not in matched_set before).
    pub added: Vec<Value>,
    /// Element IDs that no longer match (were in matched_set before).
    pub removed: Vec<ElementId>,
}

impl EvalResult {
    /// Create an empty result (no additions or removals).
    pub fn empty() -> Self {
        Self {
            added: vec![],
            removed: vec![],
        }
    }

    /// Returns true if there are no additions or removals.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }
}

// =============================================================================
// QueryMatcher
// =============================================================================

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

impl QueryMatcher {
    /// Compile a matcher from traversal steps and source.
    ///
    /// Extracts static filters by inspecting step names and downcasting
    /// to concrete step types via `as_any()` to read their fields.
    pub fn compile(steps: &[Box<dyn DynStep>], source: Option<&TraversalSource>) -> Self {
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
                    if let Some(introspect) = introspect_step(step.as_ref()) {
                        if let Some(labels) = introspect.label_constraints() {
                            let set = label_filter.get_or_insert_with(HashSet::new);
                            set.extend(labels);
                        }
                    }
                }
                "has" | "hasValue" | "hasNot" | "hasKey" => {
                    if let Some(introspect) = introspect_step(step.as_ref()) {
                        if let Some(keys) = introspect.property_constraints() {
                            property_keys.extend(keys);
                        }
                    }
                }
                "values" | "properties" | "valueMap" | "elementMap" | "propertyMap" => {
                    if let Some(introspect) = introspect_step(step.as_ref()) {
                        if let Some(keys) = introspect.property_constraints() {
                            property_keys.extend(keys);
                        }
                    }
                }
                "out" | "in" | "both" | "outE" | "inE" | "bothE" | "outV" | "inV" | "bothV"
                | "otherV" => {
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
            source: source.cloned(),
        }
    }

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
            GraphEvent::Batch(events) => events.iter().any(|e| self.might_match(e)),

            // Vertex events
            GraphEvent::VertexAdded { label, .. } | GraphEvent::VertexRemoved { label, .. } => {
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
                if !self.has_navigation
                    && !self.property_keys.is_empty()
                    && !self.property_keys.contains(key.as_str())
                {
                    return false;
                }
                true
            }

            // Edge events
            GraphEvent::EdgeAdded { label, .. } | GraphEvent::EdgeRemoved { label, .. } => {
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

    /// Full re-evaluation of the traversal for the affected element(s).
    ///
    /// This takes a graph storage reference and runs the traversal, then
    /// compares results against the subscription's `matched_set`.
    ///
    /// # Strategy by Event Type
    ///
    /// - **VertexAdded / EdgeAdded**: Run the traversal starting from the
    ///   new element. If it produces results, they're new matches.
    /// - **VertexRemoved / EdgeRemoved**: The element is gone. If it was
    ///   in the `matched_set`, it's a removal.
    /// - **PropertyChanged**: Run the traversal starting from the affected
    ///   element. Compare with `matched_set` to detect additions/removals.
    /// - **Navigation-heavy queries**: For queries with `out()`, `in()`, etc.,
    ///   a property change on vertex A could cause vertex B to enter/leave
    ///   the result set. In this case, we must re-run the full traversal
    ///   (not just from the affected element). This is the expensive path.
    /// - **Batch**: Do a single full re-evaluation rather than per-event.
    pub fn evaluate(
        &self,
        storage: &dyn GraphStorage,
        interner: &crate::storage::interner::StringInterner,
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

            GraphEvent::VertexAdded { id, .. } | GraphEvent::VertexPropertyChanged { id, .. } => {
                if self.has_navigation {
                    self.full_reevaluate(storage, interner, matched_set)
                } else {
                    self.evaluate_from_vertex(storage, interner, *id, matched_set)
                }
            }

            GraphEvent::EdgeAdded { id, .. } | GraphEvent::EdgePropertyChanged { id, .. } => {
                if self.has_navigation {
                    self.full_reevaluate(storage, interner, matched_set)
                } else {
                    self.evaluate_from_edge(storage, interner, *id, matched_set)
                }
            }

            GraphEvent::Batch(_) => self.full_reevaluate(storage, interner, matched_set),
        }
    }

    /// Re-run the full traversal and diff against matched_set.
    fn full_reevaluate(
        &self,
        storage: &dyn GraphStorage,
        interner: &crate::storage::interner::StringInterner,
        matched_set: &HashSet<ElementId>,
    ) -> EvalResult {
        let ctx = ExecutionContext::new(storage, interner);
        let input = build_source_iterator(&ctx, &self.source);
        let results: Vec<Traverser> = execute_traversal(&ctx, &self.steps, input).collect();

        let mut current_matches = HashSet::new();
        let mut added = Vec::new();

        for traverser in &results {
            if let Some(eid) = value_to_element_id(&traverser.value) {
                current_matches.insert(eid.clone());
                if !matched_set.contains(&eid) {
                    added.push(traverser.value.clone());
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
        storage: &dyn GraphStorage,
        interner: &crate::storage::interner::StringInterner,
        id: VertexId,
        matched_set: &HashSet<ElementId>,
    ) -> EvalResult {
        let ctx = ExecutionContext::new(storage, interner);
        let source = TraversalSource::Vertices(vec![id]);
        let input = build_source_iterator(&ctx, &Some(source));
        let results: Vec<Traverser> = execute_traversal(&ctx, &self.steps, input).collect();

        let eid = ElementId::Vertex(id);

        if results.is_empty() {
            if matched_set.contains(&eid) {
                EvalResult {
                    added: vec![],
                    removed: vec![eid],
                }
            } else {
                EvalResult::empty()
            }
        } else if matched_set.contains(&eid) {
            EvalResult::empty() // Already matched
        } else {
            EvalResult {
                added: results.into_iter().map(|t| t.value).collect(),
                removed: vec![],
            }
        }
    }

    /// Evaluate from a specific edge (non-navigation queries).
    fn evaluate_from_edge(
        &self,
        storage: &dyn GraphStorage,
        interner: &crate::storage::interner::StringInterner,
        id: EdgeId,
        matched_set: &HashSet<ElementId>,
    ) -> EvalResult {
        let ctx = ExecutionContext::new(storage, interner);
        let source = TraversalSource::Edges(vec![id]);
        let input = build_source_iterator(&ctx, &Some(source));
        let results: Vec<Traverser> = execute_traversal(&ctx, &self.steps, input).collect();

        let eid = ElementId::Edge(id);

        if results.is_empty() {
            if matched_set.contains(&eid) {
                EvalResult {
                    added: vec![],
                    removed: vec![eid],
                }
            } else {
                EvalResult::empty()
            }
        } else if matched_set.contains(&eid) {
            EvalResult::empty()
        } else {
            EvalResult {
                added: results.into_iter().map(|t| t.value).collect(),
                removed: vec![],
            }
        }
    }

    /// Run the full traversal and return all current matches.
    ///
    /// Used for `include_initial` to populate the matched set and emit
    /// initial `Added` events at subscription time.
    pub(crate) fn initial_evaluate(
        &self,
        storage: &dyn GraphStorage,
        interner: &crate::storage::interner::StringInterner,
    ) -> (Vec<Value>, HashSet<ElementId>) {
        let ctx = ExecutionContext::new(storage, interner);
        let input = build_source_iterator(&ctx, &self.source);
        let results: Vec<Traverser> = execute_traversal(&ctx, &self.steps, input).collect();

        let mut matched_set = HashSet::new();
        let mut values = Vec::new();

        for traverser in results {
            if let Some(eid) = value_to_element_id(&traverser.value) {
                if matched_set.insert(eid) {
                    values.push(traverser.value);
                }
            }
        }

        (values, matched_set)
    }

    // =========================================================================
    // Accessors for testing
    // =========================================================================

    /// Returns the label filter, if any.
    #[cfg(test)]
    pub(crate) fn label_filter(&self) -> Option<&HashSet<String>> {
        self.label_filter.as_ref()
    }

    /// Returns the property keys referenced by the traversal.
    #[cfg(test)]
    pub(crate) fn property_keys(&self) -> &HashSet<String> {
        &self.property_keys
    }

    /// Returns true if the traversal only operates on vertices.
    #[cfg(test)]
    pub(crate) fn is_vertex_only(&self) -> bool {
        self.vertex_only
    }

    /// Returns true if the traversal only operates on edges.
    #[cfg(test)]
    pub(crate) fn is_edge_only(&self) -> bool {
        self.edge_only
    }

    /// Returns true if the traversal contains navigation steps.
    #[cfg(test)]
    pub(crate) fn has_navigation(&self) -> bool {
        self.has_navigation
    }
}

// =============================================================================
// Helper functions
// =============================================================================

/// Build a source iterator from a `TraversalSource`, mirroring the logic in
/// `StreamingExecutor::build_streaming_source` but returning a non-`Send`
/// iterator tied to the `ExecutionContext` lifetime.
fn build_source_iterator<'a>(
    ctx: &'a ExecutionContext<'a>,
    source: &Option<TraversalSource>,
) -> Box<dyn Iterator<Item = Traverser> + 'a> {
    match source {
        Some(TraversalSource::AllVertices) => {
            Box::new(ctx.storage().all_vertices().map(|v| Traverser::new(Value::Vertex(v.id))))
        }
        Some(TraversalSource::Vertices(ids)) => {
            let ids = ids.clone();
            Box::new(ids.into_iter().filter_map(move |id| {
                ctx.storage()
                    .get_vertex(id)
                    .map(|_| Traverser::new(Value::Vertex(id)))
            }))
        }
        Some(TraversalSource::AllEdges) => {
            Box::new(ctx.storage().all_edges().map(|e| Traverser::new(Value::Edge(e.id))))
        }
        Some(TraversalSource::Edges(ids)) => {
            let ids = ids.clone();
            Box::new(ids.into_iter().filter_map(move |id| {
                ctx.storage()
                    .get_edge(id)
                    .map(|_| Traverser::new(Value::Edge(id)))
            }))
        }
        Some(TraversalSource::Inject(values)) => {
            let values = values.clone();
            Box::new(values.into_iter().map(Traverser::new))
        }
        #[cfg(feature = "full-text")]
        Some(TraversalSource::VerticesWithTextScore(hits)) => {
            let hits = hits.clone();
            Box::new(hits.into_iter().filter_map(move |(id, score)| {
                ctx.storage().get_vertex(id).map(|_| {
                    let mut t = Traverser::new(Value::Vertex(id));
                    t.set_sack(score);
                    t
                })
            }))
        }
        #[cfg(feature = "full-text")]
        Some(TraversalSource::EdgesWithTextScore(hits)) => {
            let hits = hits.clone();
            Box::new(hits.into_iter().filter_map(move |(id, score)| {
                ctx.storage().get_edge(id).map(|_| {
                    let mut t = Traverser::new(Value::Edge(id));
                    t.set_sack(score);
                    t
                })
            }))
        }
        None => Box::new(std::iter::empty()),
    }
}

/// Extract an `ElementId` from a `Value`.
fn value_to_element_id(value: &Value) -> Option<ElementId> {
    match value {
        Value::Vertex(id) => Some(ElementId::Vertex(*id)),
        Value::Edge(id) => Some(ElementId::Edge(*id)),
        _ => None,
    }
}

/// Try to downcast a `DynStep` to a `&dyn StepIntrospect` via `as_any()`.
///
/// Attempts to downcast to each known introspectable step type in turn.
fn introspect_step(step: &dyn DynStep) -> Option<&dyn StepIntrospect> {
    use crate::traversal::filter::{HasLabelStep, HasStep, HasValueStep, HasWhereStep};
    use crate::traversal::transform::ValuesStep;

    let any = step.as_any();

    if let Some(s) = any.downcast_ref::<HasLabelStep>() {
        return Some(s);
    }
    if let Some(s) = any.downcast_ref::<HasStep>() {
        return Some(s);
    }
    if let Some(s) = any.downcast_ref::<HasValueStep>() {
        return Some(s);
    }
    if let Some(s) = any.downcast_ref::<HasWhereStep>() {
        return Some(s);
    }
    if let Some(s) = any.downcast_ref::<ValuesStep>() {
        return Some(s);
    }

    None
}

// =============================================================================
// SubscriptionId
// =============================================================================

static NEXT_SUBSCRIPTION_ID: AtomicU64 = AtomicU64::new(0);

/// Unique identifier for a subscription.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SubscriptionId(u64);

impl SubscriptionId {
    fn next() -> Self {
        Self(NEXT_SUBSCRIPTION_ID.fetch_add(1, Ordering::Relaxed))
    }

    /// Returns the raw numeric ID.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

// =============================================================================
// SubscriptionEvent
// =============================================================================

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

// =============================================================================
// SubscribeOptions
// =============================================================================

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

// =============================================================================
// Subscription
// =============================================================================

/// A live subscription to a reactive query.
///
/// Receives [`SubscriptionEvent`]s as graph mutations match the
/// subscribed traversal pattern. Implements [`Iterator`] for
/// synchronous consumption.
///
/// # Cancellation
///
/// Drop the `Subscription` or call [`cancel()`](Subscription::cancel)
/// to unsubscribe. The dispatcher thread detects the closed channel
/// and cleans up.
///
/// # Async Integration
///
/// For async runtimes, use [`into_receiver()`](Subscription::into_receiver)
/// to get the raw `mpsc::Receiver` and wrap it in your runtime's async channel.
pub struct Subscription {
    /// Subscription identifier.
    id: SubscriptionId,
    /// Receiver for subscription events. `Option` so `into_receiver` can take it.
    rx: Option<std::sync::mpsc::Receiver<SubscriptionEvent>>,
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
        self.rx.as_ref()?.recv().ok()
    }

    /// Non-blocking try_recv. Returns immediately.
    pub fn try_recv(&self) -> Result<SubscriptionEvent, std::sync::mpsc::TryRecvError> {
        self.rx
            .as_ref()
            .ok_or(std::sync::mpsc::TryRecvError::Disconnected)?
            .try_recv()
    }

    /// Consume the subscription and return the raw receiver.
    ///
    /// The subscription remains active — the dispatcher will continue
    /// sending events until the receiver is dropped.
    pub fn into_receiver(mut self) -> std::sync::mpsc::Receiver<SubscriptionEvent> {
        let rx = self.rx.take().expect("receiver already taken");
        // Prevent cancel_flag from being set in drop — the subscription
        // stays alive as long as the receiver is alive.
        self.cancel_flag = Arc::new(AtomicBool::new(false));
        rx
    }

    /// Cancel the subscription and clean up resources.
    pub fn cancel(self) {
        self.cancel_flag.store(true, Ordering::Release);
        // drop self — receiver is closed
    }
}

impl Iterator for Subscription {
    type Item = SubscriptionEvent;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.as_ref()?.recv().ok()
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.cancel_flag.store(true, Ordering::Release);
    }
}

// =============================================================================
// ActiveSubscription (internal)
// =============================================================================

/// Internal representation of an active subscription within
/// the dispatcher thread.
struct ActiveSubscription {
    #[allow(dead_code)]
    id: SubscriptionId,
    matcher: QueryMatcher,
    tx: std::sync::mpsc::SyncSender<SubscriptionEvent>,
    cancel_flag: Arc<AtomicBool>,

    /// Tracks which elements currently match this subscription's query.
    matched_set: HashSet<ElementId>,

    /// Function to create a snapshot for re-evaluation.
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
            snapshot.storage(),
            snapshot.interner(),
            event,
            &self.matched_set,
        );

        if result.is_empty() {
            // Check if this is a property change on a matched element —
            // if so, emit Updated.
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
            let values: Vec<Value> = result.removed.iter().map(element_id_to_value).collect();
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
            Err(std::sync::mpsc::TrySendError::Full(_)) => true, // drop event, keep sub
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => false, // remove sub
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

// =============================================================================
// SubscriptionManager
// =============================================================================

/// Manages active subscriptions and dispatches graph events.
///
/// Lazily spawns a background `std::thread` when the first subscription
/// is created. The thread blocks on the event channel and dispatches
/// events to matching subscriptions. When the last subscription is
/// dropped, the thread shuts down.
///
/// # Thread Safety
///
/// New subscriptions can be registered from any thread via `subscribe()`.
/// The dispatcher thread is the only reader of the event channel and
/// the only writer of `ActiveSubscription::matched_set`.
pub struct SubscriptionManager {
    /// Channel for registering new subscriptions with the dispatcher.
    register_tx: parking_lot::Mutex<Option<std::sync::mpsc::Sender<ActiveSubscription>>>,

    /// Factory that creates a new event-bus subscriber receiver.
    /// Called once when the dispatcher thread is spawned.
    event_subscribe_fn: Arc<dyn Fn() -> std::sync::mpsc::Receiver<GraphEvent> + Send + Sync>,

    /// Shutdown signal. Setting to true tells the dispatcher to exit.
    shutdown: Arc<AtomicBool>,

    /// Handle to the dispatcher thread.
    thread_handle: parking_lot::Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl SubscriptionManager {
    /// Create a new subscription manager.
    ///
    /// The dispatcher thread is NOT started yet — it's lazily spawned
    /// on the first `subscribe()` call.
    ///
    /// # Arguments
    ///
    /// * `event_subscribe_fn` - Factory that creates a new `Receiver<GraphEvent>`
    ///   by subscribing to the graph's `EventBus`.
    pub fn new(
        event_subscribe_fn: Arc<dyn Fn() -> std::sync::mpsc::Receiver<GraphEvent> + Send + Sync>,
    ) -> Self {
        Self {
            register_tx: parking_lot::Mutex::new(None),
            event_subscribe_fn,
            shutdown: Arc::new(AtomicBool::new(false)),
            thread_handle: parking_lot::Mutex::new(None),
        }
    }

    /// Create a dummy/placeholder subscription manager.
    /// Used internally for snapshots created for re-evaluation only.
    pub(crate) fn placeholder() -> Self {
        Self {
            register_tx: parking_lot::Mutex::new(None),
            event_subscribe_fn: Arc::new(|| {
                let (_tx, rx) = std::sync::mpsc::sync_channel(1);
                rx
            }),
            shutdown: Arc::new(AtomicBool::new(true)),
            thread_handle: parking_lot::Mutex::new(None),
        }
    }

    /// Subscribe with a compiled matcher, options, and snapshot factory.
    pub fn subscribe(
        &self,
        matcher: QueryMatcher,
        opts: SubscribeOptions,
        snapshot_fn: Arc<dyn Fn() -> Box<dyn SnapshotLike + Send> + Send + Sync>,
    ) -> Subscription {
        let id = SubscriptionId::next();
        let (sub_tx, sub_rx) = std::sync::mpsc::sync_channel(opts.capacity);
        let cancel_flag = Arc::new(AtomicBool::new(false));

        // If include_initial, run the traversal now and send initial matches
        let matched_set = if opts.include_initial {
            let snapshot = (snapshot_fn)();
            let (values, initial_matched) =
                matcher.initial_evaluate(snapshot.storage(), snapshot.interner());
            if !values.is_empty() {
                let event = SubscriptionEvent {
                    event_type: SubscriptionEventType::Added,
                    values,
                    source_event: GraphEvent::Batch(vec![]),
                };
                // Use try_send to respect backpressure; drop if full
                let _ = sub_tx.try_send(event);
            }
            initial_matched
        } else {
            HashSet::new()
        };

        let active = ActiveSubscription {
            id,
            matcher,
            tx: sub_tx,
            cancel_flag: cancel_flag.clone(),
            matched_set,
            snapshot_fn,
        };

        // Ensure dispatcher thread is running
        self.ensure_dispatcher();

        // Register with dispatcher
        if let Some(ref tx) = *self.register_tx.lock() {
            let _ = tx.send(active);
        }

        Subscription {
            id,
            rx: Some(sub_rx),
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

        let event_rx = (self.event_subscribe_fn)();
        let shutdown = self.shutdown.clone();

        let thread = std::thread::Builder::new()
            .name("interstellar-reactive-dispatcher".to_string())
            .spawn(move || {
                Self::dispatcher_loop(event_rx, register_rx, shutdown);
            })
            .expect("failed to spawn reactive dispatcher thread");

        *handle = Some(thread);
    }

    /// Main dispatcher loop running on the background thread.
    fn dispatcher_loop(
        event_rx: std::sync::mpsc::Receiver<GraphEvent>,
        register_rx: std::sync::mpsc::Receiver<ActiveSubscription>,
        shutdown: Arc<AtomicBool>,
    ) {
        let mut subscriptions: Vec<ActiveSubscription> = Vec::new();

        loop {
            if shutdown.load(Ordering::Acquire) {
                break;
            }

            // Use recv_timeout to periodically check shutdown flag
            match event_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(event) => {
                    // Drain any newly registered subscriptions BEFORE dispatching
                    while let Ok(sub) = register_rx.try_recv() {
                        subscriptions.push(sub);
                    }

                    // Dispatch to all active subscriptions
                    subscriptions.retain_mut(|sub| sub.process_event(&event));

                    // If no subscriptions left, check for new ones then exit
                    if subscriptions.is_empty() {
                        while let Ok(sub) = register_rx.try_recv() {
                            subscriptions.push(sub);
                        }
                        if subscriptions.is_empty() && !shutdown.load(Ordering::Acquire) {
                            // No subscriptions — thread will be re-spawned on next subscribe
                            break;
                        }
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    // Check for new subscriptions during idle periods
                    while let Ok(sub) = register_rx.try_recv() {
                        subscriptions.push(sub);
                    }
                    continue;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    // Event bus channel closed (Graph dropped)
                    break;
                }
            }
        }
    }
}

impl Drop for SubscriptionManager {
    fn drop(&mut self) {
        // Signal shutdown
        self.shutdown.store(true, Ordering::Release);
        // Close the register channel
        *self.register_tx.lock() = None;
        // Wait for the dispatcher thread to exit (it checks shutdown every 100ms)
        if let Some(handle) = self.thread_handle.lock().take() {
            let _ = handle.join();
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::events::GraphEvent;
    use crate::traversal::context::SnapshotLike;
    use crate::traversal::filter::{HasLabelStep, HasStep, HasValueStep, HasWhereStep};
    use crate::traversal::navigation::OutStep;
    use crate::traversal::predicate;
    use crate::traversal::step::DynStep;
    use crate::traversal::transform::ValuesStep;
    use crate::value::Value;

    // =========================================================================
    // compile() tests
    // =========================================================================

    #[test]
    fn test_compile_empty_steps() {
        let steps: Vec<Box<dyn DynStep>> = vec![];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        assert!(matcher.label_filter().is_none());
        assert!(matcher.property_keys().is_empty());
        assert!(matcher.is_vertex_only());
        assert!(!matcher.is_edge_only());
        assert!(!matcher.has_navigation());
    }

    #[test]
    fn test_compile_has_label() {
        let steps: Vec<Box<dyn DynStep>> = vec![Box::new(HasLabelStep::new(vec![
            "person".to_string(),
            "company".to_string(),
        ]))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        let labels = matcher.label_filter().unwrap();
        assert!(labels.contains("person"));
        assert!(labels.contains("company"));
        assert_eq!(labels.len(), 2);
    }

    #[test]
    fn test_compile_has_step() {
        let steps: Vec<Box<dyn DynStep>> = vec![Box::new(HasStep::new("age"))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        assert!(matcher.property_keys().contains("age"));
        assert_eq!(matcher.property_keys().len(), 1);
    }

    #[test]
    fn test_compile_has_value_step() {
        let steps: Vec<Box<dyn DynStep>> =
            vec![Box::new(HasValueStep::new("name", Value::from("Alice")))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        assert!(matcher.property_keys().contains("name"));
    }

    #[test]
    fn test_compile_has_where_step() {
        let steps: Vec<Box<dyn DynStep>> =
            vec![Box::new(HasWhereStep::new("age", predicate::p::gt(30)))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        assert!(matcher.property_keys().contains("age"));
    }

    #[test]
    fn test_compile_values_step() {
        let steps: Vec<Box<dyn DynStep>> = vec![Box::new(ValuesStep::new("name"))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        assert!(matcher.property_keys().contains("name"));
    }

    #[test]
    fn test_compile_navigation_step() {
        let steps: Vec<Box<dyn DynStep>> = vec![Box::new(OutStep::new())];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        assert!(matcher.has_navigation());
    }

    #[test]
    fn test_compile_edge_source() {
        let steps: Vec<Box<dyn DynStep>> = vec![];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllEdges));

        assert!(!matcher.is_vertex_only());
        assert!(matcher.is_edge_only());
    }

    #[test]
    fn test_compile_no_source() {
        let steps: Vec<Box<dyn DynStep>> = vec![];
        let matcher = QueryMatcher::compile(&steps, None);

        assert!(!matcher.is_vertex_only());
        assert!(!matcher.is_edge_only());
    }

    #[test]
    fn test_compile_multiple_has_labels_merged() {
        let steps: Vec<Box<dyn DynStep>> = vec![
            Box::new(HasLabelStep::new(vec!["person".to_string()])),
            Box::new(HasLabelStep::new(vec!["company".to_string()])),
        ];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        let labels = matcher.label_filter().unwrap();
        assert!(labels.contains("person"));
        assert!(labels.contains("company"));
        assert_eq!(labels.len(), 2);
    }

    // =========================================================================
    // might_match() tests
    // =========================================================================

    #[test]
    fn test_might_match_vertex_only_rejects_edge_events() {
        let steps: Vec<Box<dyn DynStep>> = vec![];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        let edge_event = GraphEvent::EdgeAdded {
            id: EdgeId(1),
            label: "knows".into(),
            src: VertexId(1),
            dst: VertexId(2),
            properties: Default::default(),
        };
        assert!(!matcher.might_match(&edge_event));
    }

    #[test]
    fn test_might_match_edge_only_rejects_vertex_events() {
        let steps: Vec<Box<dyn DynStep>> = vec![];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllEdges));

        let vertex_event = GraphEvent::VertexAdded {
            id: VertexId(1),
            label: "person".into(),
            properties: Default::default(),
        };
        assert!(!matcher.might_match(&vertex_event));
    }

    #[test]
    fn test_might_match_label_filter_rejects_wrong_label() {
        let steps: Vec<Box<dyn DynStep>> =
            vec![Box::new(HasLabelStep::new(vec!["person".to_string()]))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        let wrong_label = GraphEvent::VertexAdded {
            id: VertexId(1),
            label: "company".into(),
            properties: Default::default(),
        };
        assert!(!matcher.might_match(&wrong_label));

        let right_label = GraphEvent::VertexAdded {
            id: VertexId(2),
            label: "person".into(),
            properties: Default::default(),
        };
        assert!(matcher.might_match(&right_label));
    }

    #[test]
    fn test_might_match_property_filter_rejects_irrelevant_key() {
        let steps: Vec<Box<dyn DynStep>> = vec![Box::new(HasStep::new("age"))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        let irrelevant = GraphEvent::VertexPropertyChanged {
            id: VertexId(1),
            key: "name".into(),
            old_value: None,
            new_value: Value::from("Alice"),
        };
        assert!(!matcher.might_match(&irrelevant));

        let relevant = GraphEvent::VertexPropertyChanged {
            id: VertexId(1),
            key: "age".into(),
            old_value: None,
            new_value: Value::from(30),
        };
        assert!(matcher.might_match(&relevant));
    }

    #[test]
    fn test_might_match_navigation_disables_type_rejection() {
        let steps: Vec<Box<dyn DynStep>> = vec![Box::new(OutStep::new())];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        // With navigation, edge events can still affect vertex-only queries
        let edge_event = GraphEvent::EdgeAdded {
            id: EdgeId(1),
            label: "knows".into(),
            src: VertexId(1),
            dst: VertexId(2),
            properties: Default::default(),
        };
        assert!(matcher.might_match(&edge_event));
    }

    #[test]
    fn test_might_match_navigation_disables_label_rejection() {
        let steps: Vec<Box<dyn DynStep>> = vec![
            Box::new(HasLabelStep::new(vec!["person".to_string()])),
            Box::new(OutStep::new()),
        ];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        // With navigation, even wrong-label vertices might affect results
        let wrong_label = GraphEvent::VertexAdded {
            id: VertexId(1),
            label: "company".into(),
            properties: Default::default(),
        };
        assert!(matcher.might_match(&wrong_label));
    }

    #[test]
    fn test_might_match_navigation_disables_property_rejection() {
        let steps: Vec<Box<dyn DynStep>> = vec![
            Box::new(HasStep::new("age")),
            Box::new(OutStep::new()),
        ];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        let irrelevant = GraphEvent::VertexPropertyChanged {
            id: VertexId(1),
            key: "name".into(),
            old_value: None,
            new_value: Value::from("Alice"),
        };
        assert!(matcher.might_match(&irrelevant));
    }

    #[test]
    fn test_might_match_batch_any_sub_event_matches() {
        let steps: Vec<Box<dyn DynStep>> =
            vec![Box::new(HasLabelStep::new(vec!["person".to_string()]))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        let batch = GraphEvent::Batch(vec![
            GraphEvent::VertexAdded {
                id: VertexId(1),
                label: "company".into(),
                properties: Default::default(),
            },
            GraphEvent::VertexAdded {
                id: VertexId(2),
                label: "person".into(),
                properties: Default::default(),
            },
        ]);
        assert!(matcher.might_match(&batch));
    }

    #[test]
    fn test_might_match_batch_no_sub_event_matches() {
        let steps: Vec<Box<dyn DynStep>> =
            vec![Box::new(HasLabelStep::new(vec!["person".to_string()]))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        let batch = GraphEvent::Batch(vec![GraphEvent::VertexAdded {
            id: VertexId(1),
            label: "company".into(),
            properties: Default::default(),
        }]);
        assert!(!matcher.might_match(&batch));
    }

    #[test]
    fn test_might_match_edge_property_vertex_only() {
        let steps: Vec<Box<dyn DynStep>> = vec![Box::new(HasStep::new("weight"))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        let event = GraphEvent::EdgePropertyChanged {
            id: EdgeId(1),
            key: "weight".into(),
            old_value: None,
            new_value: Value::from(1.0),
        };
        assert!(!matcher.might_match(&event));
    }

    #[test]
    fn test_might_match_vertex_event_accepts_matching_label() {
        let steps: Vec<Box<dyn DynStep>> =
            vec![Box::new(HasLabelStep::new(vec!["person".to_string()]))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        let event = GraphEvent::VertexRemoved {
            id: VertexId(1),
            label: "person".into(),
        };
        assert!(matcher.might_match(&event));
    }

    #[test]
    fn test_might_match_no_property_keys_accepts_property_change() {
        // When no property keys are tracked, all property changes pass
        let steps: Vec<Box<dyn DynStep>> = vec![];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        let event = GraphEvent::VertexPropertyChanged {
            id: VertexId(1),
            key: "anything".into(),
            old_value: None,
            new_value: Value::from(42),
        };
        assert!(matcher.might_match(&event));
    }

    // =========================================================================
    // evaluate() tests
    // =========================================================================

    #[test]
    fn test_evaluate_vertex_removed_in_matched_set() {
        let steps: Vec<Box<dyn DynStep>> = vec![];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        let mut matched = HashSet::new();
        matched.insert(ElementId::Vertex(VertexId(1)));

        let graph = crate::Graph::new();
        let snapshot = graph.snapshot();
        let event = GraphEvent::VertexRemoved {
            id: VertexId(1),
            label: "person".into(),
        };

        let result = matcher.evaluate(snapshot.storage(), snapshot.interner(), &event, &matched);
        assert_eq!(result.removed.len(), 1);
        assert_eq!(result.removed[0], ElementId::Vertex(VertexId(1)));
        assert!(result.added.is_empty());
    }

    #[test]
    fn test_evaluate_vertex_removed_not_in_matched_set() {
        let steps: Vec<Box<dyn DynStep>> = vec![];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        let matched = HashSet::new();
        let graph = crate::Graph::new();
        let snapshot = graph.snapshot();
        let event = GraphEvent::VertexRemoved {
            id: VertexId(1),
            label: "person".into(),
        };

        let result = matcher.evaluate(snapshot.storage(), snapshot.interner(), &event, &matched);
        assert!(result.is_empty());
    }

    #[test]
    fn test_evaluate_edge_removed_in_matched_set() {
        let steps: Vec<Box<dyn DynStep>> = vec![];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllEdges));

        let mut matched = HashSet::new();
        matched.insert(ElementId::Edge(EdgeId(1)));

        let graph = crate::Graph::new();
        let snapshot = graph.snapshot();
        let event = GraphEvent::EdgeRemoved {
            id: EdgeId(1),
            label: "knows".into(),
            src: VertexId(1),
            dst: VertexId(2),
        };

        let result = matcher.evaluate(snapshot.storage(), snapshot.interner(), &event, &matched);
        assert_eq!(result.removed.len(), 1);
        assert_eq!(result.removed[0], ElementId::Edge(EdgeId(1)));
    }

    #[test]
    fn test_evaluate_vertex_added_matches() {
        use std::collections::HashMap;
        let graph = crate::Graph::new();
        let id = graph.add_vertex("person", HashMap::new());
        let snapshot = graph.snapshot();

        // Traversal: V().hasLabel("person")
        let steps: Vec<Box<dyn DynStep>> =
            vec![Box::new(HasLabelStep::new(vec!["person".to_string()]))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        let matched = HashSet::new();
        let event = GraphEvent::VertexAdded {
            id,
            label: "person".into(),
            properties: Default::default(),
        };

        let result = matcher.evaluate(snapshot.storage(), snapshot.interner(), &event, &matched);
        assert_eq!(result.added.len(), 1);
        assert_eq!(result.added[0], Value::Vertex(id));
        assert!(result.removed.is_empty());
    }

    #[test]
    fn test_evaluate_vertex_added_no_match() {
        use std::collections::HashMap;
        let graph = crate::Graph::new();
        let id = graph.add_vertex("company", HashMap::new());
        let snapshot = graph.snapshot();

        // Traversal: V().hasLabel("person") — company won't match
        let steps: Vec<Box<dyn DynStep>> =
            vec![Box::new(HasLabelStep::new(vec!["person".to_string()]))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        let matched = HashSet::new();
        let event = GraphEvent::VertexAdded {
            id,
            label: "company".into(),
            properties: Default::default(),
        };

        let result = matcher.evaluate(snapshot.storage(), snapshot.interner(), &event, &matched);
        assert!(result.is_empty());
    }

    #[test]
    fn test_evaluate_vertex_already_matched() {
        use std::collections::HashMap;
        let graph = crate::Graph::new();
        let id = graph.add_vertex("person", HashMap::new());
        let snapshot = graph.snapshot();

        let steps: Vec<Box<dyn DynStep>> =
            vec![Box::new(HasLabelStep::new(vec!["person".to_string()]))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        let mut matched = HashSet::new();
        matched.insert(ElementId::Vertex(id));

        let event = GraphEvent::VertexPropertyChanged {
            id,
            key: "name".into(),
            old_value: None,
            new_value: Value::from("Alice"),
        };

        let result = matcher.evaluate(snapshot.storage(), snapshot.interner(), &event, &matched);
        // Already in matched set and still matches → empty (could be Updated in future)
        assert!(result.is_empty());
    }

    #[test]
    fn test_evaluate_full_reevaluate_on_batch() {
        use std::collections::HashMap;
        let graph = crate::Graph::new();
        let id1 = graph.add_vertex("person", HashMap::new());
        let id2 = graph.add_vertex("person", HashMap::new());
        let snapshot = graph.snapshot();

        let steps: Vec<Box<dyn DynStep>> =
            vec![Box::new(HasLabelStep::new(vec!["person".to_string()]))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));

        // Only id1 was previously matched
        let mut matched = HashSet::new();
        matched.insert(ElementId::Vertex(id1));

        let event = GraphEvent::Batch(vec![GraphEvent::VertexAdded {
            id: id2,
            label: "person".into(),
            properties: Default::default(),
        }]);

        let result = matcher.evaluate(snapshot.storage(), snapshot.interner(), &event, &matched);
        // id2 should be added
        assert!(result.added.contains(&Value::Vertex(id2)));
        assert!(result.removed.is_empty());
    }

    #[test]
    fn test_eval_result_empty() {
        let result = EvalResult::empty();
        assert!(result.is_empty());
    }

    #[test]
    fn test_eval_result_not_empty_with_added() {
        let result = EvalResult {
            added: vec![Value::Vertex(VertexId(1))],
            removed: vec![],
        };
        assert!(!result.is_empty());
    }

    #[test]
    fn test_eval_result_not_empty_with_removed() {
        let result = EvalResult {
            added: vec![],
            removed: vec![ElementId::Vertex(VertexId(1))],
        };
        assert!(!result.is_empty());
    }

    #[test]
    fn test_value_to_element_id_vertex() {
        let eid = value_to_element_id(&Value::Vertex(VertexId(42)));
        assert_eq!(eid, Some(ElementId::Vertex(VertexId(42))));
    }

    #[test]
    fn test_value_to_element_id_edge() {
        let eid = value_to_element_id(&Value::Edge(EdgeId(7)));
        assert_eq!(eid, Some(ElementId::Edge(EdgeId(7))));
    }

    #[test]
    fn test_value_to_element_id_other() {
        let eid = value_to_element_id(&Value::from(42));
        assert_eq!(eid, None);
    }

    #[test]
    fn test_element_id_equality() {
        assert_eq!(
            ElementId::Vertex(VertexId(1)),
            ElementId::Vertex(VertexId(1))
        );
        assert_ne!(
            ElementId::Vertex(VertexId(1)),
            ElementId::Vertex(VertexId(2))
        );
        assert_ne!(
            ElementId::Vertex(VertexId(1)),
            ElementId::Edge(EdgeId(1))
        );
    }

    // =========================================================================
    // Subscription infrastructure tests
    // =========================================================================

    #[test]
    fn test_subscription_id_uniqueness() {
        let id1 = SubscriptionId::next();
        let id2 = SubscriptionId::next();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_subscribe_options_defaults() {
        let opts = SubscribeOptions::default();
        assert_eq!(opts.capacity, EventBus::DEFAULT_CAPACITY);
        assert!(!opts.include_initial);
    }

    #[test]
    fn test_subscribe_options_builder() {
        let opts = SubscribeOptions::new().capacity(42).include_initial(true);
        assert_eq!(opts.capacity, 42);
        assert!(opts.include_initial);
    }

    #[test]
    fn test_subscription_event_types() {
        let event = SubscriptionEvent {
            event_type: SubscriptionEventType::Added,
            values: vec![Value::Vertex(VertexId(1))],
            source_event: GraphEvent::VertexAdded {
                id: VertexId(1),
                label: "person".into(),
                properties: Default::default(),
            },
        };
        assert_eq!(event.event_type, SubscriptionEventType::Added);
        assert_eq!(event.values.len(), 1);
    }

    #[test]
    fn test_subscription_manager_basic_lifecycle() {
        use std::collections::HashMap;
        let graph = Arc::new(crate::Graph::new());

        let g1 = graph.clone();
        let event_sub_fn: Arc<dyn Fn() -> std::sync::mpsc::Receiver<GraphEvent> + Send + Sync> =
            Arc::new(move || g1.event_bus().subscribe());
        let g2 = graph.clone();
        let snapshot_fn: Arc<dyn Fn() -> Box<dyn SnapshotLike + Send> + Send + Sync> =
            Arc::new(move || Box::new(g2.snapshot()));

        let manager = SubscriptionManager::new(event_sub_fn);

        // Create a subscription for V().hasLabel("person")
        let steps: Vec<Box<dyn DynStep>> =
            vec![Box::new(HasLabelStep::new(vec!["person".to_string()]))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));
        let sub = manager.subscribe(matcher, SubscribeOptions::default(), snapshot_fn);

        // Add a matching vertex
        graph.add_vertex("person", HashMap::new());

        // Should receive Added event
        let event = sub.recv().unwrap();
        assert_eq!(event.event_type, SubscriptionEventType::Added);
        assert_eq!(event.values.len(), 1);

        // Cancel and clean up
        sub.cancel();
        drop(manager);
    }

    #[test]
    fn test_subscription_non_matching_ignored() {
        use std::collections::HashMap;
        let graph = Arc::new(crate::Graph::new());

        let g1 = graph.clone();
        let event_sub_fn: Arc<dyn Fn() -> std::sync::mpsc::Receiver<GraphEvent> + Send + Sync> =
            Arc::new(move || g1.event_bus().subscribe());
        let g2 = graph.clone();
        let snapshot_fn: Arc<dyn Fn() -> Box<dyn SnapshotLike + Send> + Send + Sync> =
            Arc::new(move || Box::new(g2.snapshot()));

        let manager = SubscriptionManager::new(event_sub_fn);

        let steps: Vec<Box<dyn DynStep>> =
            vec![Box::new(HasLabelStep::new(vec!["person".to_string()]))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));
        let sub = manager.subscribe(matcher, SubscribeOptions::default(), snapshot_fn);

        // Add a non-matching vertex
        graph.add_vertex("company", HashMap::new());

        // Add a matching vertex so the dispatcher processes events
        graph.add_vertex("person", HashMap::new());

        // First event should be Added for the person
        let event = sub.recv().unwrap();
        assert_eq!(event.event_type, SubscriptionEventType::Added);

        // Try non-blocking — should be empty (company was filtered)
        assert!(sub.try_recv().is_err());

        sub.cancel();
        drop(manager);
    }

    #[test]
    fn test_subscription_removal_detection() {
        use std::collections::HashMap;
        let graph = Arc::new(crate::Graph::new());

        let g1 = graph.clone();
        let event_sub_fn: Arc<dyn Fn() -> std::sync::mpsc::Receiver<GraphEvent> + Send + Sync> =
            Arc::new(move || g1.event_bus().subscribe());
        let g2 = graph.clone();
        let snapshot_fn: Arc<dyn Fn() -> Box<dyn SnapshotLike + Send> + Send + Sync> =
            Arc::new(move || Box::new(g2.snapshot()));

        let manager = SubscriptionManager::new(event_sub_fn);

        let steps: Vec<Box<dyn DynStep>> =
            vec![Box::new(HasLabelStep::new(vec!["person".to_string()]))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));
        let sub = manager.subscribe(matcher, SubscribeOptions::default(), snapshot_fn);

        // Add then remove
        let id = graph.add_vertex("person", HashMap::new());
        let event = sub.recv().unwrap();
        assert_eq!(event.event_type, SubscriptionEventType::Added);

        graph.remove_vertex(id).unwrap();
        let event = sub.recv().unwrap();
        assert_eq!(event.event_type, SubscriptionEventType::Removed);

        sub.cancel();
        drop(manager);
    }

    #[test]
    fn test_subscription_try_recv_empty() {
        let graph = Arc::new(crate::Graph::new());

        let g1 = graph.clone();
        let event_sub_fn: Arc<dyn Fn() -> std::sync::mpsc::Receiver<GraphEvent> + Send + Sync> =
            Arc::new(move || g1.event_bus().subscribe());
        let g2 = graph.clone();
        let snapshot_fn: Arc<dyn Fn() -> Box<dyn SnapshotLike + Send> + Send + Sync> =
            Arc::new(move || Box::new(g2.snapshot()));

        let manager = SubscriptionManager::new(event_sub_fn);

        let steps: Vec<Box<dyn DynStep>> = vec![];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));
        let sub = manager.subscribe(matcher, SubscribeOptions::default(), snapshot_fn);

        // No events yet
        assert!(sub.try_recv().is_err());

        sub.cancel();
        drop(manager);
    }

    #[test]
    fn test_subscription_cancel_via_drop() {
        use std::collections::HashMap;
        let graph = Arc::new(crate::Graph::new());

        let g1 = graph.clone();
        let event_sub_fn: Arc<dyn Fn() -> std::sync::mpsc::Receiver<GraphEvent> + Send + Sync> =
            Arc::new(move || g1.event_bus().subscribe());
        let g2 = graph.clone();
        let snapshot_fn: Arc<dyn Fn() -> Box<dyn SnapshotLike + Send> + Send + Sync> =
            Arc::new(move || Box::new(g2.snapshot()));

        let manager = SubscriptionManager::new(event_sub_fn);

        let steps: Vec<Box<dyn DynStep>> =
            vec![Box::new(HasLabelStep::new(vec!["person".to_string()]))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));
        let sub = manager.subscribe(matcher, SubscribeOptions::default(), snapshot_fn);

        // Drop the subscription
        drop(sub);

        // Add a vertex — should not panic or block
        graph.add_vertex("person", HashMap::new());

        // Give dispatcher time to process
        std::thread::sleep(std::time::Duration::from_millis(50));

        drop(manager);
    }

    #[test]
    fn test_multiple_subscriptions() {
        use std::collections::HashMap;
        let graph = Arc::new(crate::Graph::new());

        let g1 = graph.clone();
        let event_sub_fn: Arc<dyn Fn() -> std::sync::mpsc::Receiver<GraphEvent> + Send + Sync> =
            Arc::new(move || g1.event_bus().subscribe());
        let g2 = graph.clone();
        let snapshot_fn: Arc<dyn Fn() -> Box<dyn SnapshotLike + Send> + Send + Sync> =
            Arc::new(move || Box::new(g2.snapshot()));

        let manager = SubscriptionManager::new(event_sub_fn);

        // Sub 1: person vertices
        let steps1: Vec<Box<dyn DynStep>> =
            vec![Box::new(HasLabelStep::new(vec!["person".to_string()]))];
        let matcher1 = QueryMatcher::compile(&steps1, Some(&TraversalSource::AllVertices));
        let sub1 = manager.subscribe(matcher1, SubscribeOptions::default(), snapshot_fn.clone());

        // Sub 2: company vertices
        let steps2: Vec<Box<dyn DynStep>> =
            vec![Box::new(HasLabelStep::new(vec!["company".to_string()]))];
        let matcher2 = QueryMatcher::compile(&steps2, Some(&TraversalSource::AllVertices));
        let sub2 = manager.subscribe(matcher2, SubscribeOptions::default(), snapshot_fn);

        // Add a person — only sub1 should get it
        graph.add_vertex("person", HashMap::new());
        let event = sub1.recv().unwrap();
        assert_eq!(event.event_type, SubscriptionEventType::Added);

        // Add a company — only sub2 should get it
        graph.add_vertex("company", HashMap::new());
        let event = sub2.recv().unwrap();
        assert_eq!(event.event_type, SubscriptionEventType::Added);

        sub1.cancel();
        sub2.cancel();
        drop(manager);
    }

    #[test]
    fn test_backpressure_does_not_block_mutations() {
        use std::collections::HashMap;
        let graph = Arc::new(crate::Graph::new());

        let g1 = graph.clone();
        let event_sub_fn: Arc<dyn Fn() -> std::sync::mpsc::Receiver<GraphEvent> + Send + Sync> =
            Arc::new(move || g1.event_bus().subscribe());
        let g2 = graph.clone();
        let snapshot_fn: Arc<dyn Fn() -> Box<dyn SnapshotLike + Send> + Send + Sync> =
            Arc::new(move || Box::new(g2.snapshot()));

        let manager = SubscriptionManager::new(event_sub_fn);

        // Tiny capacity
        let steps: Vec<Box<dyn DynStep>> =
            vec![Box::new(HasLabelStep::new(vec!["person".to_string()]))];
        let matcher = QueryMatcher::compile(&steps, Some(&TraversalSource::AllVertices));
        let sub = manager.subscribe(matcher, SubscribeOptions::new().capacity(1), snapshot_fn);

        // Flood with mutations — should not block
        for _ in 0..100 {
            graph.add_vertex("person", HashMap::new());
        }

        // Give dispatcher time to process
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Should be able to receive at least one event
        assert!(sub.recv().is_some());

        sub.cancel();
        drop(manager);
    }

    #[test]
    fn test_element_id_to_value_roundtrip() {
        let eid = ElementId::Vertex(VertexId(42));
        let value = element_id_to_value(&eid);
        assert_eq!(value, Value::Vertex(VertexId(42)));

        let eid = ElementId::Edge(EdgeId(7));
        let value = element_id_to_value(&eid);
        assert_eq!(value, Value::Edge(EdgeId(7)));
    }

    #[test]
    fn test_event_to_element_id_property_changes() {
        let event = GraphEvent::VertexPropertyChanged {
            id: VertexId(1),
            key: "age".into(),
            old_value: None,
            new_value: Value::from(30),
        };
        assert_eq!(
            event_to_element_id(&event),
            Some(ElementId::Vertex(VertexId(1)))
        );

        let event = GraphEvent::EdgePropertyChanged {
            id: EdgeId(2),
            key: "weight".into(),
            old_value: None,
            new_value: Value::from(1.0),
        };
        assert_eq!(
            event_to_element_id(&event),
            Some(ElementId::Edge(EdgeId(2)))
        );

        let event = GraphEvent::VertexAdded {
            id: VertexId(1),
            label: "person".into(),
            properties: Default::default(),
        };
        assert_eq!(event_to_element_id(&event), None);
    }
}
