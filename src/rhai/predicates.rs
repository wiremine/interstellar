//! Predicate bindings for Rhai.
//!
//! This module registers predicate functions (`eq`, `gt`, `within`, etc.) as global
//! functions in Rhai, and provides a `RhaiPredicate` wrapper that can be passed
//! between scripts and used with traversal filtering steps.

use rhai::{Dynamic, Engine, ImmutableString};
use std::sync::Arc;

use crate::traversal::predicate::{p, Predicate};
use crate::value::Value;

use super::types::dynamic_to_value;

/// A thread-safe wrapper around a predicate for use in Rhai scripts.
///
/// `RhaiPredicate` wraps a `Box<dyn Predicate>` in an `Arc` to make it cloneable
/// and shareable between Rhai scripts. This is necessary because Rhai requires
/// all registered types to be `Clone`.
#[derive(Clone)]
pub struct RhaiPredicate {
    inner: Arc<Box<dyn Predicate>>,
}

impl std::fmt::Debug for RhaiPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RhaiPredicate").finish_non_exhaustive()
    }
}

impl RhaiPredicate {
    /// Create a new RhaiPredicate from a predicate.
    pub fn new<P: Predicate + 'static>(predicate: P) -> Self {
        RhaiPredicate {
            inner: Arc::new(Box::new(predicate)),
        }
    }

    /// Create a RhaiPredicate from a boxed predicate.
    pub fn from_boxed(predicate: Box<dyn Predicate>) -> Self {
        RhaiPredicate {
            inner: Arc::new(predicate),
        }
    }

    /// Test if the predicate matches the given value.
    pub fn test(&self, value: &Value) -> bool {
        self.inner.test(value)
    }

    /// Get the inner predicate as a boxed trait object.
    ///
    /// This clones the predicate using `clone_box()`.
    pub fn to_boxed(&self) -> Box<dyn Predicate> {
        self.inner.clone_box()
    }
}

/// Implement the Predicate trait for RhaiPredicate so it can be used directly.
impl Predicate for RhaiPredicate {
    fn test(&self, value: &Value) -> bool {
        self.inner.test(value)
    }

    fn clone_box(&self) -> Box<dyn Predicate> {
        // Clone the wrapper, which shares the Arc
        Box::new(self.clone())
    }
}

/// Registers all predicate functions with the Rhai engine.
///
/// This registers:
/// - Comparison predicates: `eq`, `neq`, `lt`, `lte`, `gt`, `gte`
/// - Range predicates: `between`, `inside`, `outside`
/// - Collection predicates: `within`, `without`
/// - String predicates: `containing`, `starting_with`, `ending_with`, `regex`
/// - Logical combinators: `pred_not`, `pred_and`, `pred_or`
pub fn register_predicates(engine: &mut Engine) {
    // Register the RhaiPredicate type
    engine.register_type_with_name::<RhaiPredicate>("Predicate");

    // Test method
    engine.register_fn("test", |pred: &mut RhaiPredicate, value: Value| {
        pred.test(&value)
    });

    // Also allow testing with a Dynamic (converts to Value first)
    engine.register_fn(
        "test_dynamic",
        |pred: &mut RhaiPredicate, value: Dynamic| pred.test(&dynamic_to_value(value)),
    );

    register_comparison_predicates(engine);
    register_range_predicates(engine);
    register_collection_predicates(engine);
    register_string_predicates(engine);
    register_logical_predicates(engine);
}

/// Registers comparison predicates (eq, neq, lt, lte, gt, gte).
fn register_comparison_predicates(engine: &mut Engine) {
    // eq(value) - for various types
    engine.register_fn("eq", |value: i64| RhaiPredicate::new(p::eq(value)));
    engine.register_fn("eq", |value: f64| RhaiPredicate::new(p::eq(value)));
    engine.register_fn("eq", |value: bool| RhaiPredicate::new(p::eq(value)));
    engine.register_fn("eq", |value: ImmutableString| {
        RhaiPredicate::new(p::eq(value.to_string()))
    });
    engine.register_fn("eq", |value: Value| RhaiPredicate::new(p::eq(value)));

    // neq(value)
    engine.register_fn("neq", |value: i64| RhaiPredicate::new(p::neq(value)));
    engine.register_fn("neq", |value: f64| RhaiPredicate::new(p::neq(value)));
    engine.register_fn("neq", |value: bool| RhaiPredicate::new(p::neq(value)));
    engine.register_fn("neq", |value: ImmutableString| {
        RhaiPredicate::new(p::neq(value.to_string()))
    });
    engine.register_fn("neq", |value: Value| RhaiPredicate::new(p::neq(value)));

    // lt(value)
    engine.register_fn("lt", |value: i64| RhaiPredicate::new(p::lt(value)));
    engine.register_fn("lt", |value: f64| RhaiPredicate::new(p::lt(value)));
    engine.register_fn("lt", |value: ImmutableString| {
        RhaiPredicate::new(p::lt(value.to_string()))
    });

    // lte(value)
    engine.register_fn("lte", |value: i64| RhaiPredicate::new(p::lte(value)));
    engine.register_fn("lte", |value: f64| RhaiPredicate::new(p::lte(value)));
    engine.register_fn("lte", |value: ImmutableString| {
        RhaiPredicate::new(p::lte(value.to_string()))
    });

    // gt(value)
    engine.register_fn("gt", |value: i64| RhaiPredicate::new(p::gt(value)));
    engine.register_fn("gt", |value: f64| RhaiPredicate::new(p::gt(value)));
    engine.register_fn("gt", |value: ImmutableString| {
        RhaiPredicate::new(p::gt(value.to_string()))
    });

    // gte(value)
    engine.register_fn("gte", |value: i64| RhaiPredicate::new(p::gte(value)));
    engine.register_fn("gte", |value: f64| RhaiPredicate::new(p::gte(value)));
    engine.register_fn("gte", |value: ImmutableString| {
        RhaiPredicate::new(p::gte(value.to_string()))
    });
}

/// Registers range predicates (between, inside, outside).
fn register_range_predicates(engine: &mut Engine) {
    // between(start, end) - [start, end)
    engine.register_fn("between", |start: i64, end: i64| {
        RhaiPredicate::new(p::between(start, end))
    });
    engine.register_fn("between", |start: f64, end: f64| {
        RhaiPredicate::new(p::between(start, end))
    });
    engine.register_fn("between", |start: ImmutableString, end: ImmutableString| {
        RhaiPredicate::new(p::between(start.to_string(), end.to_string()))
    });

    // inside(start, end) - (start, end)
    engine.register_fn("inside", |start: i64, end: i64| {
        RhaiPredicate::new(p::inside(start, end))
    });
    engine.register_fn("inside", |start: f64, end: f64| {
        RhaiPredicate::new(p::inside(start, end))
    });
    engine.register_fn("inside", |start: ImmutableString, end: ImmutableString| {
        RhaiPredicate::new(p::inside(start.to_string(), end.to_string()))
    });

    // outside(start, end) - value < start OR value > end
    engine.register_fn("outside", |start: i64, end: i64| {
        RhaiPredicate::new(p::outside(start, end))
    });
    engine.register_fn("outside", |start: f64, end: f64| {
        RhaiPredicate::new(p::outside(start, end))
    });
    engine.register_fn("outside", |start: ImmutableString, end: ImmutableString| {
        RhaiPredicate::new(p::outside(start.to_string(), end.to_string()))
    });
}

/// Registers collection predicates (within, without).
fn register_collection_predicates(engine: &mut Engine) {
    // within([values]) - value is in set
    engine.register_fn("within", |values: rhai::Array| {
        let value_vec: Vec<Value> = values.into_iter().map(dynamic_to_value).collect();
        RhaiPredicate::new(p::within(value_vec))
    });

    // without([values]) - value is NOT in set
    engine.register_fn("without", |values: rhai::Array| {
        let value_vec: Vec<Value> = values.into_iter().map(dynamic_to_value).collect();
        RhaiPredicate::new(p::without(value_vec))
    });
}

/// Registers string predicates (containing, starting_with, ending_with, regex).
fn register_string_predicates(engine: &mut Engine) {
    // containing(substring)
    engine.register_fn("containing", |s: ImmutableString| {
        RhaiPredicate::new(p::containing(s.to_string()))
    });

    // starting_with(prefix)
    engine.register_fn("starting_with", |s: ImmutableString| {
        RhaiPredicate::new(p::starting_with(s.to_string()))
    });

    // ending_with(suffix)
    engine.register_fn("ending_with", |s: ImmutableString| {
        RhaiPredicate::new(p::ending_with(s.to_string()))
    });

    // regex(pattern) - returns Result to handle invalid patterns
    engine.register_fn(
        "regex",
        |pattern: ImmutableString| -> Result<RhaiPredicate, Box<rhai::EvalAltResult>> {
            match p::try_regex(&pattern) {
                Some(regex_pred) => Ok(RhaiPredicate::new(regex_pred)),
                None => Err(format!("Invalid regex pattern: {}", pattern).into()),
            }
        },
    );

    // not_containing(substring)
    engine.register_fn("not_containing", |s: ImmutableString| {
        RhaiPredicate::new(p::not_containing(s.to_string()))
    });

    // not_starting_with(prefix)
    engine.register_fn("not_starting_with", |s: ImmutableString| {
        RhaiPredicate::new(p::not_starting_with(s.to_string()))
    });

    // not_ending_with(suffix)
    engine.register_fn("not_ending_with", |s: ImmutableString| {
        RhaiPredicate::new(p::not_ending_with(s.to_string()))
    });
}

/// Registers logical predicates (pred_not, pred_and, pred_or).
///
/// These use the `pred_` prefix to avoid conflicts with Rhai keywords (`and`, `or`, `not`).
fn register_logical_predicates(engine: &mut Engine) {
    // pred_not(predicate) - negate a predicate
    engine.register_fn("pred_not", |pred: RhaiPredicate| {
        RhaiPredicate::new(p::not(pred))
    });

    // pred_and(p1, p2) - both must be true
    engine.register_fn("pred_and", |p1: RhaiPredicate, p2: RhaiPredicate| {
        RhaiPredicate::new(p::and(p1, p2))
    });

    // pred_or(p1, p2) - either must be true
    engine.register_fn("pred_or", |p1: RhaiPredicate, p2: RhaiPredicate| {
        RhaiPredicate::new(p::or(p1, p2))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_engine() -> Engine {
        let mut engine = Engine::new();
        super::super::types::register_types(&mut engine);
        register_predicates(&mut engine);
        engine
    }

    #[test]
    fn test_eq_predicates() {
        let engine = create_engine();

        // Integer equality
        let pred: RhaiPredicate = engine.eval("eq(42)").unwrap();
        assert!(pred.test(&Value::Int(42)));
        assert!(!pred.test(&Value::Int(41)));

        // String equality
        let pred: RhaiPredicate = engine.eval("eq(\"hello\")").unwrap();
        assert!(pred.test(&Value::String("hello".to_string())));
        assert!(!pred.test(&Value::String("world".to_string())));

        // Boolean equality
        let pred: RhaiPredicate = engine.eval("eq(true)").unwrap();
        assert!(pred.test(&Value::Bool(true)));
        assert!(!pred.test(&Value::Bool(false)));
    }

    #[test]
    fn test_neq_predicates() {
        let engine = create_engine();

        let pred: RhaiPredicate = engine.eval("neq(42)").unwrap();
        assert!(!pred.test(&Value::Int(42)));
        assert!(pred.test(&Value::Int(41)));
    }

    #[test]
    fn test_comparison_predicates() {
        let engine = create_engine();

        // lt
        let pred: RhaiPredicate = engine.eval("lt(50)").unwrap();
        assert!(pred.test(&Value::Int(30)));
        assert!(!pred.test(&Value::Int(50)));

        // lte
        let pred: RhaiPredicate = engine.eval("lte(50)").unwrap();
        assert!(pred.test(&Value::Int(50)));
        assert!(!pred.test(&Value::Int(51)));

        // gt
        let pred: RhaiPredicate = engine.eval("gt(50)").unwrap();
        assert!(pred.test(&Value::Int(60)));
        assert!(!pred.test(&Value::Int(50)));

        // gte
        let pred: RhaiPredicate = engine.eval("gte(50)").unwrap();
        assert!(pred.test(&Value::Int(50)));
        assert!(!pred.test(&Value::Int(49)));
    }

    #[test]
    fn test_range_predicates() {
        let engine = create_engine();

        // between [10, 20)
        let pred: RhaiPredicate = engine.eval("between(10, 20)").unwrap();
        assert!(pred.test(&Value::Int(10)));
        assert!(pred.test(&Value::Int(15)));
        assert!(!pred.test(&Value::Int(20)));
        assert!(!pred.test(&Value::Int(5)));

        // inside (10, 20)
        let pred: RhaiPredicate = engine.eval("inside(10, 20)").unwrap();
        assert!(!pred.test(&Value::Int(10)));
        assert!(pred.test(&Value::Int(15)));
        assert!(!pred.test(&Value::Int(20)));

        // outside - value < 10 OR value > 20
        let pred: RhaiPredicate = engine.eval("outside(10, 20)").unwrap();
        assert!(pred.test(&Value::Int(5)));
        assert!(pred.test(&Value::Int(25)));
        assert!(!pred.test(&Value::Int(15)));
    }

    #[test]
    fn test_collection_predicates() {
        let engine = create_engine();

        // within
        let pred: RhaiPredicate = engine.eval("within([1, 2, 3])").unwrap();
        assert!(pred.test(&Value::Int(2)));
        assert!(!pred.test(&Value::Int(4)));

        // without
        let pred: RhaiPredicate = engine.eval("without([1, 2, 3])").unwrap();
        assert!(!pred.test(&Value::Int(2)));
        assert!(pred.test(&Value::Int(4)));
    }

    #[test]
    fn test_string_predicates() {
        let engine = create_engine();

        // containing
        let pred: RhaiPredicate = engine.eval("containing(\"foo\")").unwrap();
        assert!(pred.test(&Value::String("foobar".to_string())));
        assert!(!pred.test(&Value::String("bar".to_string())));

        // starting_with
        let pred: RhaiPredicate = engine.eval("starting_with(\"foo\")").unwrap();
        assert!(pred.test(&Value::String("foobar".to_string())));
        assert!(!pred.test(&Value::String("barfoo".to_string())));

        // ending_with
        let pred: RhaiPredicate = engine.eval("ending_with(\"bar\")").unwrap();
        assert!(pred.test(&Value::String("foobar".to_string())));
        assert!(!pred.test(&Value::String("barfoo".to_string())));
    }

    #[test]
    fn test_regex_predicate() {
        let engine = create_engine();

        // Valid regex
        let pred: RhaiPredicate = engine.eval("regex(\"^\\\\d+$\")").unwrap();
        assert!(pred.test(&Value::String("123".to_string())));
        assert!(!pred.test(&Value::String("abc".to_string())));

        // Invalid regex should error
        let result: Result<RhaiPredicate, _> = engine.eval("regex(\"[\")");
        assert!(result.is_err());
    }

    #[test]
    fn test_logical_predicates() {
        let engine = create_engine();

        // pred_not
        let pred: RhaiPredicate = engine.eval("pred_not(eq(42))").unwrap();
        assert!(!pred.test(&Value::Int(42)));
        assert!(pred.test(&Value::Int(41)));

        // pred_and
        let pred: RhaiPredicate = engine.eval("pred_and(gte(10), lt(20))").unwrap();
        assert!(pred.test(&Value::Int(15)));
        assert!(!pred.test(&Value::Int(5)));
        assert!(!pred.test(&Value::Int(25)));

        // pred_or
        let pred: RhaiPredicate = engine.eval("pred_or(eq(1), eq(2))").unwrap();
        assert!(pred.test(&Value::Int(1)));
        assert!(pred.test(&Value::Int(2)));
        assert!(!pred.test(&Value::Int(3)));
    }

    #[test]
    fn test_nested_logical_predicates() {
        let engine = create_engine();

        // Complex nested predicate: (x >= 10 AND x < 20) OR x == 100
        let pred: RhaiPredicate = engine
            .eval("pred_or(pred_and(gte(10), lt(20)), eq(100))")
            .unwrap();
        assert!(pred.test(&Value::Int(15)));
        assert!(pred.test(&Value::Int(100)));
        assert!(!pred.test(&Value::Int(5)));
        assert!(!pred.test(&Value::Int(50)));
    }

    #[test]
    fn test_predicate_test_method() {
        let engine = create_engine();

        // Test method should work
        let result: bool = engine
            .eval(
                r#"
                let p = eq(42);
                p.test(value_int(42))
            "#,
            )
            .unwrap();
        assert!(result);
    }
}
