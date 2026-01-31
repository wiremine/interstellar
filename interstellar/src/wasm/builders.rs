//! Builder wrappers for WASM bindings.
//!
//! Provides JavaScript-friendly builder patterns for steps like order(), group(), etc.
//! These wrap the WASM Traversal and accumulate configuration before building the final step.

use std::sync::Arc;

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsError;

use crate::storage::cow::Graph as InnerGraph;
use crate::traversal::aggregate::{GroupKey, GroupValue};
use crate::traversal::repeat::RepeatConfig;
use crate::traversal::transform::functional::Projection;
use crate::traversal::transform::order::{Order, OrderKey};
use crate::value::Value;
use crate::wasm::traversal::{Traversal, TraversalType};
use crate::wasm::types::js_to_u64;

// =============================================================================
// OrderBuilder
// =============================================================================

/// Builder for order() step configuration.
///
/// @example
/// ```typescript
/// graph.V()
///     .order()
///     .byKeyAsc('name')
///     .byKeyDesc('age')
///     .build()
///     .toList();
/// ```
#[wasm_bindgen]
pub struct OrderBuilder {
    graph: Arc<InnerGraph>,
    steps: Vec<Box<dyn crate::traversal::DynStep>>,
    output_type: TraversalType,
    keys: Vec<OrderKey>,
}

impl OrderBuilder {
    pub(crate) fn new(traversal: Traversal) -> Self {
        Self {
            graph: traversal.graph.clone(),
            steps: traversal.steps,
            output_type: traversal.output_type,
            keys: Vec::new(),
        }
    }
}

#[wasm_bindgen]
impl OrderBuilder {
    /// Order by natural value (ascending).
    #[wasm_bindgen(js_name = "byAsc")]
    pub fn by_asc(mut self) -> OrderBuilder {
        self.keys.push(OrderKey::Natural(Order::Asc));
        self
    }

    /// Order by natural value (descending).
    #[wasm_bindgen(js_name = "byDesc")]
    pub fn by_desc(mut self) -> OrderBuilder {
        self.keys.push(OrderKey::Natural(Order::Desc));
        self
    }

    /// Order by a property key (ascending).
    ///
    /// @param key - Property name
    #[wasm_bindgen(js_name = "byKeyAsc")]
    pub fn by_key_asc(mut self, key: &str) -> OrderBuilder {
        self.keys
            .push(OrderKey::Property(key.to_string(), Order::Asc));
        self
    }

    /// Order by a property key (descending).
    ///
    /// @param key - Property name
    #[wasm_bindgen(js_name = "byKeyDesc")]
    pub fn by_key_desc(mut self, key: &str) -> OrderBuilder {
        self.keys
            .push(OrderKey::Property(key.to_string(), Order::Desc));
        self
    }

    /// Order by the result of a traversal (ascending).
    ///
    /// @param traversal - Anonymous traversal
    #[wasm_bindgen(js_name = "byTraversalAsc")]
    pub fn by_traversal_asc(mut self, traversal: Traversal) -> OrderBuilder {
        let core_traversal = traversal.into_core_traversal();
        self.keys
            .push(OrderKey::Traversal(core_traversal, Order::Asc));
        self
    }

    /// Order by the result of a traversal (descending).
    ///
    /// @param traversal - Anonymous traversal
    #[wasm_bindgen(js_name = "byTraversalDesc")]
    pub fn by_traversal_desc(mut self, traversal: Traversal) -> OrderBuilder {
        let core_traversal = traversal.into_core_traversal();
        self.keys
            .push(OrderKey::Traversal(core_traversal, Order::Desc));
        self
    }

    /// Finalize the order step and return to traversal.
    pub fn build(mut self) -> Traversal {
        // Default to ascending natural order if no keys specified
        if self.keys.is_empty() {
            self.keys.push(OrderKey::Natural(Order::Asc));
        }

        let step = crate::traversal::OrderStep::with_keys(self.keys);
        self.steps.push(Box::new(step));

        Traversal {
            graph: self.graph,
            source: crate::wasm::traversal::TraversalSource::Anonymous, // Will be fixed by caller
            steps: self.steps,
            output_type: self.output_type,
        }
    }
}

// =============================================================================
// ProjectBuilder
// =============================================================================

/// Builder for project() step configuration.
///
/// @example
/// ```typescript
/// graph.V()
///     .project('name', 'friendCount')
///     .byKey('name', 'name')
///     .byTraversal('friendCount', __.out('knows').count())
///     .build()
///     .toList();
/// ```
#[wasm_bindgen]
pub struct ProjectBuilder {
    graph: Arc<InnerGraph>,
    steps: Vec<Box<dyn crate::traversal::DynStep>>,
    #[allow(dead_code)]
    output_type: TraversalType,
    keys: Vec<String>,
    projections: Vec<Projection>,
}

impl ProjectBuilder {
    pub(crate) fn new(traversal: Traversal, keys: Vec<String>) -> Self {
        Self {
            graph: traversal.graph.clone(),
            steps: traversal.steps,
            output_type: traversal.output_type,
            keys,
            projections: Vec::new(),
        }
    }
}

#[wasm_bindgen]
impl ProjectBuilder {
    /// Project a key using a property value.
    ///
    /// @param propertyKey - Property to extract
    #[wasm_bindgen(js_name = "byKey")]
    pub fn by_key(mut self, property_key: &str) -> ProjectBuilder {
        self.projections
            .push(Projection::Key(property_key.to_string()));
        self
    }

    /// Project a key using a traversal result.
    ///
    /// @param traversal - Anonymous traversal
    #[wasm_bindgen(js_name = "byTraversal")]
    pub fn by_traversal(mut self, traversal: Traversal) -> ProjectBuilder {
        let core_traversal = traversal.into_core_traversal();
        self.projections.push(Projection::Traversal(core_traversal));
        self
    }

    /// Finalize the project step and return to traversal.
    pub fn build(mut self) -> Traversal {
        // Pad projections with identity if not enough were specified
        while self.projections.len() < self.keys.len() {
            // Default to identity - not directly available, use Key("") as placeholder
            // Better: we should default to extracting the key with same name
            let key = &self.keys[self.projections.len()];
            self.projections.push(Projection::Key(key.clone()));
        }

        let step = crate::traversal::ProjectStep::new(self.keys, self.projections);
        self.steps.push(Box::new(step));

        Traversal {
            graph: self.graph,
            source: crate::wasm::traversal::TraversalSource::Anonymous,
            steps: self.steps,
            output_type: TraversalType::Value,
        }
    }
}

// =============================================================================
// GroupBuilder
// =============================================================================

/// Builder for group() step configuration.
///
/// @example
/// ```typescript
/// graph.V()
///     .group()
///     .byKey('age')
///     .valuesByTraversal(__.values('name'))
///     .build()
///     .toList();
/// ```
#[wasm_bindgen]
pub struct GroupBuilder {
    graph: Arc<InnerGraph>,
    steps: Vec<Box<dyn crate::traversal::DynStep>>,
    #[allow(dead_code)]
    output_type: TraversalType,
    key_selector: Option<GroupKey>,
    value_collector: Option<GroupValue>,
}

impl GroupBuilder {
    pub(crate) fn new(traversal: Traversal) -> Self {
        Self {
            graph: traversal.graph.clone(),
            steps: traversal.steps,
            output_type: traversal.output_type,
            key_selector: None,
            value_collector: None,
        }
    }
}

#[wasm_bindgen]
impl GroupBuilder {
    /// Group by element label.
    #[wasm_bindgen(js_name = "byLabel")]
    pub fn by_label(mut self) -> GroupBuilder {
        self.key_selector = Some(GroupKey::Label);
        self
    }

    /// Group by a property key.
    ///
    /// @param key - Property name
    #[wasm_bindgen(js_name = "byKey")]
    pub fn by_key(mut self, key: &str) -> GroupBuilder {
        self.key_selector = Some(GroupKey::Property(key.to_string()));
        self
    }

    /// Group by the result of a traversal.
    ///
    /// @param traversal - Anonymous traversal
    #[wasm_bindgen(js_name = "byTraversal")]
    pub fn by_traversal(mut self, traversal: Traversal) -> GroupBuilder {
        let core_traversal = traversal.into_core_traversal();
        self.key_selector = Some(GroupKey::Traversal(Box::new(core_traversal)));
        self
    }

    /// Aggregate values using a traversal.
    ///
    /// @param traversal - Anonymous traversal for values
    #[wasm_bindgen(js_name = "valuesByTraversal")]
    pub fn values_by_traversal(mut self, traversal: Traversal) -> GroupBuilder {
        let core_traversal = traversal.into_core_traversal();
        self.value_collector = Some(GroupValue::Traversal(Box::new(core_traversal)));
        self
    }

    /// Aggregate values using fold (collect into list).
    #[wasm_bindgen(js_name = "valuesFold")]
    pub fn values_fold(mut self) -> GroupBuilder {
        // Identity means the value itself is collected (which fold then collects into list)
        self.value_collector = Some(GroupValue::Identity);
        self
    }

    /// Aggregate values by extracting a property.
    ///
    /// @param key - Property name
    #[wasm_bindgen(js_name = "valuesByKey")]
    pub fn values_by_key(mut self, key: &str) -> GroupBuilder {
        self.value_collector = Some(GroupValue::Property(key.to_string()));
        self
    }

    /// Finalize the group step and return to traversal.
    pub fn build(mut self) -> Traversal {
        let key_selector = self.key_selector.unwrap_or(GroupKey::Label);
        let value_collector = self.value_collector.unwrap_or(GroupValue::Identity);

        let step = crate::traversal::GroupStep::with_selectors(key_selector, value_collector);
        self.steps.push(Box::new(step));

        Traversal {
            graph: self.graph,
            source: crate::wasm::traversal::TraversalSource::Anonymous,
            steps: self.steps,
            output_type: TraversalType::Value,
        }
    }
}

// =============================================================================
// GroupCountBuilder
// =============================================================================

/// Builder for groupCount() step configuration.
///
/// @example
/// ```typescript
/// graph.V()
///     .groupCount()
///     .byKey('age')
///     .build()
///     .toList();
/// ```
#[wasm_bindgen]
pub struct GroupCountBuilder {
    graph: Arc<InnerGraph>,
    steps: Vec<Box<dyn crate::traversal::DynStep>>,
    #[allow(dead_code)]
    output_type: TraversalType,
    key_selector: Option<GroupKey>,
}

impl GroupCountBuilder {
    pub(crate) fn new(traversal: Traversal) -> Self {
        Self {
            graph: traversal.graph.clone(),
            steps: traversal.steps,
            output_type: traversal.output_type,
            key_selector: None,
        }
    }
}

#[wasm_bindgen]
impl GroupCountBuilder {
    /// Count by element label.
    #[wasm_bindgen(js_name = "byLabel")]
    pub fn by_label(mut self) -> GroupCountBuilder {
        self.key_selector = Some(GroupKey::Label);
        self
    }

    /// Count by a property key.
    ///
    /// @param key - Property name
    #[wasm_bindgen(js_name = "byKey")]
    pub fn by_key(mut self, key: &str) -> GroupCountBuilder {
        self.key_selector = Some(GroupKey::Property(key.to_string()));
        self
    }

    /// Count by the result of a traversal.
    ///
    /// @param traversal - Anonymous traversal
    #[wasm_bindgen(js_name = "byTraversal")]
    pub fn by_traversal(mut self, traversal: Traversal) -> GroupCountBuilder {
        let core_traversal = traversal.into_core_traversal();
        self.key_selector = Some(GroupKey::Traversal(Box::new(core_traversal)));
        self
    }

    /// Finalize the groupCount step and return to traversal.
    pub fn build(mut self) -> Traversal {
        let key_selector = self.key_selector.unwrap_or(GroupKey::Label);

        let step = crate::traversal::GroupCountStep::new(key_selector);
        self.steps.push(Box::new(step));

        Traversal {
            graph: self.graph,
            source: crate::wasm::traversal::TraversalSource::Anonymous,
            steps: self.steps,
            output_type: TraversalType::Value,
        }
    }
}

// =============================================================================
// RepeatBuilder
// =============================================================================

/// Builder for repeat() step configuration.
///
/// @example
/// ```typescript
/// graph.V_(startId)
///     .repeat(__.out('knows'))
///     .times(3n)
///     .build()
///     .toList();
/// ```
#[wasm_bindgen]
pub struct RepeatBuilder {
    graph: Arc<InnerGraph>,
    steps: Vec<Box<dyn crate::traversal::DynStep>>,
    output_type: TraversalType,
    sub_traversal: crate::traversal::Traversal<Value, Value>,
    config: RepeatConfig,
}

impl RepeatBuilder {
    pub(crate) fn new(traversal: Traversal, sub: Traversal) -> Self {
        Self {
            graph: traversal.graph.clone(),
            steps: traversal.steps,
            output_type: traversal.output_type,
            sub_traversal: sub.into_core_traversal(),
            config: RepeatConfig::new(),
        }
    }
}

#[wasm_bindgen]
impl RepeatBuilder {
    /// Repeat a fixed number of times.
    ///
    /// @param n - Number of iterations
    pub fn times(mut self, n: JsValue) -> Result<RepeatBuilder, JsError> {
        let num = js_to_u64(n)?;
        self.config = self.config.with_times(num as usize);
        Ok(self)
    }

    /// Repeat until a condition is met.
    ///
    /// @param condition - Anonymous traversal that determines when to stop
    pub fn until(mut self, condition: Traversal) -> RepeatBuilder {
        let core_traversal = condition.into_core_traversal();
        self.config = self.config.with_until(core_traversal);
        self
    }

    /// Emit elements during iteration.
    pub fn emit(mut self) -> RepeatBuilder {
        self.config = self.config.with_emit();
        self
    }

    /// Emit elements that match a condition.
    ///
    /// @param condition - Anonymous traversal that determines when to emit
    #[wasm_bindgen(js_name = "emitIf")]
    pub fn emit_if(mut self, condition: Traversal) -> RepeatBuilder {
        let core_traversal = condition.into_core_traversal();
        self.config = self.config.with_emit_if(core_traversal);
        self
    }

    /// Finalize the repeat step and return to traversal.
    pub fn build(mut self) -> Traversal {
        let step = crate::traversal::RepeatStep::with_config(self.sub_traversal, self.config);
        self.steps.push(Box::new(step));

        Traversal {
            graph: self.graph,
            source: crate::wasm::traversal::TraversalSource::Anonymous,
            steps: self.steps,
            output_type: self.output_type,
        }
    }
}
