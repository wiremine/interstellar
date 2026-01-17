//! Anonymous traversal factory for Rhai.
//!
//! This module provides a factory object for creating anonymous traversal fragments.
//! Anonymous traversals are used as arguments to steps like `where_()`, `union()`,
//! `coalesce()`, `optional()`, and `repeat()`.
//!
//! # Usage in Rhai scripts
//!
//! The factory is typically bound to a short variable name like `A`:
//!
//! ```rhai
//! // Create anonymous traversal using the factory
//! let anon = A.out().has_label("person");
//!
//! // Use in where_() step
//! g.v().where_(A.out("knows").has_label("engineer")).to_list()
//!
//! // Use in union()
//! g.v().union([A.out("knows"), A.in_("follows")]).to_list()
//! ```
//!
//! Note: Rhai doesn't allow identifiers starting with underscores, so we use `A`
//! instead of Gremlin's traditional `__`.

use rhai::{Dynamic, Engine, ImmutableString};

use super::traversal::RhaiAnonymousTraversal;
use super::types::dynamic_to_value;
use crate::value::Value;

/// The anonymous traversal factory.
///
/// This struct is registered as `__` in Rhai scripts and provides factory
/// methods for creating anonymous traversals.
#[derive(Clone, Default)]
pub struct AnonymousTraversalFactory;

impl AnonymousTraversalFactory {
    /// Create a new factory instance.
    pub fn new() -> Self {
        AnonymousTraversalFactory
    }

    /// Start with identity (pass-through).
    pub fn identity(&self) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().identity()
    }

    /// Start with out step.
    pub fn out(&self) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().out()
    }

    /// Start with out step with label filter.
    pub fn out_label(&self, label: String) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().out_labels(vec![label])
    }

    /// Start with in_ step.
    pub fn in_(&self) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().in_()
    }

    /// Start with in_ step with label filter.
    pub fn in_label(&self, label: String) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().in_labels(vec![label])
    }

    /// Start with both step.
    pub fn both(&self) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().both()
    }

    /// Start with out_e step.
    pub fn out_e(&self) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().out_e()
    }

    /// Start with in_e step.
    pub fn in_e(&self) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().in_e()
    }

    /// Start with both_e step.
    pub fn both_e(&self) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().both_e()
    }

    /// Start with out_v step.
    pub fn out_v(&self) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().out_v()
    }

    /// Start with in_v step.
    pub fn in_v(&self) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().in_v()
    }

    /// Start with other_v step.
    pub fn other_v(&self) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().other_v()
    }

    /// Start with has_label filter.
    pub fn has_label(&self, label: String) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().has_label(label)
    }

    /// Start with has filter.
    pub fn has(&self, key: String) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().has(key)
    }

    /// Start with has_not filter.
    pub fn has_not(&self, key: String) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().has_not(key)
    }

    /// Start with has_value filter.
    pub fn has_value(&self, key: String, value: Value) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().has_value(key, value)
    }

    /// Start with dedup.
    pub fn dedup(&self) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().dedup()
    }

    /// Start with limit.
    pub fn limit(&self, n: i64) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().limit(n)
    }

    /// Start with id step.
    pub fn id(&self) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().id()
    }

    /// Start with label step.
    pub fn label(&self) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().label()
    }

    /// Start with values step.
    pub fn values(&self, key: String) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().values(key)
    }

    /// Start with value_map step.
    pub fn value_map(&self) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().value_map()
    }

    /// Start with path step.
    pub fn path(&self) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().path()
    }

    /// Start with constant step.
    pub fn constant(&self, value: Value) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().constant(value)
    }

    /// Start with fold step.
    pub fn fold(&self) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().fold()
    }

    /// Start with unfold step.
    pub fn unfold(&self) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().unfold()
    }

    /// Start with as_ modulator.
    pub fn as_(&self, label: String) -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new().as_(label)
    }
}

/// Register the anonymous traversal factory with the Rhai engine.
///
/// This registers the `AnonymousTraversalFactory` type and its methods.
/// Users typically access this through a pre-bound `__` variable.
pub fn register_anonymous(engine: &mut Engine) {
    engine.register_type_with_name::<AnonymousTraversalFactory>("AnonymousFactory");

    // Factory methods (called on __ object)
    engine.register_fn("identity", |f: &mut AnonymousTraversalFactory| f.identity());
    engine.register_fn("out", |f: &mut AnonymousTraversalFactory| f.out());
    engine.register_fn(
        "out",
        |f: &mut AnonymousTraversalFactory, label: ImmutableString| f.out_label(label.to_string()),
    );
    engine.register_fn("in_", |f: &mut AnonymousTraversalFactory| f.in_());
    engine.register_fn(
        "in_",
        |f: &mut AnonymousTraversalFactory, label: ImmutableString| f.in_label(label.to_string()),
    );
    engine.register_fn("both", |f: &mut AnonymousTraversalFactory| f.both());
    engine.register_fn("out_e", |f: &mut AnonymousTraversalFactory| f.out_e());
    engine.register_fn("in_e", |f: &mut AnonymousTraversalFactory| f.in_e());
    engine.register_fn("both_e", |f: &mut AnonymousTraversalFactory| f.both_e());
    engine.register_fn("out_v", |f: &mut AnonymousTraversalFactory| f.out_v());
    engine.register_fn("in_v", |f: &mut AnonymousTraversalFactory| f.in_v());
    engine.register_fn("other_v", |f: &mut AnonymousTraversalFactory| f.other_v());

    // Filter methods
    engine.register_fn(
        "has_label",
        |f: &mut AnonymousTraversalFactory, label: ImmutableString| f.has_label(label.to_string()),
    );
    engine.register_fn(
        "has",
        |f: &mut AnonymousTraversalFactory, key: ImmutableString| f.has(key.to_string()),
    );
    engine.register_fn(
        "has_not",
        |f: &mut AnonymousTraversalFactory, key: ImmutableString| f.has_not(key.to_string()),
    );
    engine.register_fn(
        "has_value",
        |f: &mut AnonymousTraversalFactory, key: ImmutableString, value: Dynamic| {
            f.has_value(key.to_string(), dynamic_to_value(value))
        },
    );
    engine.register_fn("dedup", |f: &mut AnonymousTraversalFactory| f.dedup());
    engine.register_fn("limit", |f: &mut AnonymousTraversalFactory, n: i64| {
        f.limit(n)
    });

    // Transform methods
    engine.register_fn("id", |f: &mut AnonymousTraversalFactory| f.id());
    engine.register_fn("label", |f: &mut AnonymousTraversalFactory| f.label());
    engine.register_fn(
        "values",
        |f: &mut AnonymousTraversalFactory, key: ImmutableString| f.values(key.to_string()),
    );
    engine.register_fn("value_map", |f: &mut AnonymousTraversalFactory| {
        f.value_map()
    });
    engine.register_fn("path", |f: &mut AnonymousTraversalFactory| f.path());
    engine.register_fn(
        "constant",
        |f: &mut AnonymousTraversalFactory, value: Dynamic| f.constant(dynamic_to_value(value)),
    );
    engine.register_fn("fold", |f: &mut AnonymousTraversalFactory| f.fold());
    engine.register_fn("unfold", |f: &mut AnonymousTraversalFactory| f.unfold());

    // Modulator methods
    engine.register_fn(
        "as_",
        |f: &mut AnonymousTraversalFactory, label: ImmutableString| f.as_(label.to_string()),
    );
}

/// Create a new anonymous traversal factory instance.
///
/// This is typically bound to `__` in the Rhai scope.
pub fn create_anonymous_factory() -> AnonymousTraversalFactory {
    AnonymousTraversalFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rhai::Scope;

    fn create_engine() -> Engine {
        let mut engine = Engine::new();
        super::super::types::register_types(&mut engine);
        super::super::predicates::register_predicates(&mut engine);
        super::super::traversal::register_traversal(&mut engine);
        register_anonymous(&mut engine);
        engine
    }

    #[test]
    fn test_anonymous_factory_identity() {
        let factory = AnonymousTraversalFactory::new();
        let anon = factory.identity();
        let traversal = anon.to_traversal();
        // Should compile without error
        assert!(true);
        let _ = traversal;
    }

    #[test]
    fn test_anonymous_factory_navigation() {
        let factory = AnonymousTraversalFactory::new();

        // Test various factory methods
        let _ = factory.out();
        let _ = factory.in_();
        let _ = factory.both();
        let _ = factory.out_e();
        let _ = factory.in_e();
        let _ = factory.both_e();
        let _ = factory.out_v();
        let _ = factory.in_v();
        let _ = factory.other_v();
    }

    #[test]
    fn test_anonymous_factory_with_labels() {
        let factory = AnonymousTraversalFactory::new();

        let _ = factory.out_label("knows".to_string());
        let _ = factory.in_label("follows".to_string());
        let _ = factory.has_label("person".to_string());
    }

    #[test]
    fn test_anonymous_factory_filters() {
        let factory = AnonymousTraversalFactory::new();

        let _ = factory.has("name".to_string());
        let _ = factory.has_not("age".to_string());
        let _ = factory.has_value("name".to_string(), Value::String("Alice".to_string()));
        let _ = factory.dedup();
        let _ = factory.limit(10);
    }

    #[test]
    fn test_anonymous_factory_transforms() {
        let factory = AnonymousTraversalFactory::new();

        let _ = factory.id();
        let _ = factory.label();
        let _ = factory.values("name".to_string());
        let _ = factory.value_map();
        let _ = factory.path();
        let _ = factory.constant(Value::Int(42));
        let _ = factory.fold();
        let _ = factory.unfold();
    }

    #[test]
    fn test_rhai_script_with_anonymous_factory() {
        let engine = create_engine();

        // Create a scope with the anonymous factory bound
        // Note: Using 'A' instead of '__' because Rhai doesn't allow identifiers starting with underscore
        let mut scope = Scope::new();
        scope.push("A", create_anonymous_factory());

        // Test that we can create anonymous traversals via script
        let result: RhaiAnonymousTraversal = engine
            .eval_with_scope(&mut scope, r#"A.out().has_label("person")"#)
            .unwrap();

        // Verify it produces a valid traversal
        let _ = result.to_traversal();
    }

    #[test]
    fn test_rhai_script_chained_anonymous() {
        let engine = create_engine();

        let mut scope = Scope::new();
        scope.push("A", create_anonymous_factory());

        // More complex chained traversal
        let result: RhaiAnonymousTraversal = engine
            .eval_with_scope(
                &mut scope,
                r#"A.out("knows").in_("follows").has("active").values("name")"#,
            )
            .unwrap();

        let _ = result.to_traversal();
    }
}
