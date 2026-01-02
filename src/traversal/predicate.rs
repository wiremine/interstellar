//! Predicate system for value-based filtering.
//!
//! This module provides the `Predicate` trait and the `p` module containing
//! comparison, range, collection, string, and logical predicates. Predicates
//! are used with `has_where()` to filter traversers based on property values.
//!
//! # Architecture
//!
//! The predicate system uses a trait object pattern similar to `AnyStep`:
//! - `Predicate` trait defines `test()` and `clone_box()` methods
//! - `Box<dyn Predicate>` is clonable via the `clone_box()` pattern
//! - All predicates are `Send + Sync` for thread safety
//!
//! # Usage
//!
//! ```ignore
//! use rustgremlin::traversal::p;
//!
//! // Comparison predicates
//! g.v().has_where("age", p::gte(18));
//!
//! // Range predicates
//! g.v().has_where("age", p::between(25, 65));
//!
//! // String predicates
//! g.v().has_where("name", p::starting_with("A"));
//!
//! // Logical composition
//! g.v().has_where("age", p::and(p::gte(18), p::lt(65)));
//! ```

use crate::value::Value;

// -----------------------------------------------------------------------------
// Predicate Trait
// -----------------------------------------------------------------------------

/// A predicate that tests a Value.
///
/// Predicates are composable and can be used with `has_where()` to filter
/// traversers based on property values.
///
/// # Design Note
///
/// Like `AnyStep`, predicates use `clone_box()` for cloning trait objects.
/// This enables storing predicates as `Box<dyn Predicate>` while supporting
/// Clone via explicit method rather than the `Clone` trait bound.
///
/// # Example
///
/// ```ignore
/// use rustgremlin::traversal::predicate::{Predicate, p};
///
/// // Using a comparison predicate
/// let pred = p::eq(42);
/// assert!(pred.test(&Value::Int(42)));
/// assert!(!pred.test(&Value::Int(41)));
/// ```
pub trait Predicate: Send + Sync {
    /// Test if the predicate matches the given value.
    ///
    /// Returns `true` if the value satisfies the predicate, `false` otherwise.
    fn test(&self, value: &Value) -> bool;

    /// Clone this predicate into a boxed trait object.
    ///
    /// This is required for cloning traversals that contain predicates
    /// (e.g., for branching operations like `union()` or `coalesce()`).
    fn clone_box(&self) -> Box<dyn Predicate>;
}

// Enable cloning of Box<dyn Predicate>
impl Clone for Box<dyn Predicate> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

// -----------------------------------------------------------------------------
// Predicate Factory Module (p::) - Stub for Phase 1.1
// -----------------------------------------------------------------------------

/// Predicate factory module.
///
/// Provides factory functions for creating predicates used with `has_where()`.
/// All factory functions return types that implement `Predicate`.
///
/// # Example
///
/// ```ignore
/// use rustgremlin::traversal::p;
///
/// // Comparison predicates (Phase 1.2)
/// g.v().has_where("age", p::gte(18));
///
/// // Range predicates (Phase 1.3)
/// g.v().has_where("age", p::between(25, 65));
///
/// // String predicates (Phase 1.5)
/// g.v().has_where("name", p::starting_with("A"));
///
/// // Logical composition (Phase 1.7)
/// g.v().has_where("age", p::and(p::gte(18), p::lt(65)));
/// ```
pub mod p {
    use super::Predicate;
    use crate::value::Value;

    // -------------------------------------------------------------------------
    // Comparison Predicates (Phase 1.2)
    // -------------------------------------------------------------------------

    /// Equal to predicate.
    ///
    /// Tests if the value equals the target value using `PartialEq`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// let pred = p::eq(42);
    /// assert!(pred.test(&Value::Int(42)));
    /// assert!(!pred.test(&Value::Int(41)));
    /// ```
    #[derive(Clone)]
    pub struct Eq(Value);

    impl Predicate for Eq {
        fn test(&self, value: &Value) -> bool {
            value == &self.0
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create an equality predicate.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to compare against (can be any type that implements `Into<Value>`)
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// // Integer equality
    /// let pred = p::eq(42);
    ///
    /// // String equality
    /// let pred = p::eq("Alice");
    ///
    /// // Boolean equality
    /// let pred = p::eq(true);
    /// ```
    pub fn eq<T: Into<Value>>(value: T) -> Eq {
        Eq(value.into())
    }

    /// Not equal to predicate.
    ///
    /// Tests if the value does not equal the target value using `PartialEq`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// let pred = p::neq(42);
    /// assert!(!pred.test(&Value::Int(42)));
    /// assert!(pred.test(&Value::Int(41)));
    /// ```
    #[derive(Clone)]
    pub struct Neq(Value);

    impl Predicate for Neq {
        fn test(&self, value: &Value) -> bool {
            value != &self.0
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create a not-equal predicate.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to compare against (can be any type that implements `Into<Value>`)
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// let pred = p::neq(42);
    /// assert!(pred.test(&Value::Int(41)));
    /// ```
    pub fn neq<T: Into<Value>>(value: T) -> Neq {
        Neq(value.into())
    }

    /// Less than predicate.
    ///
    /// Tests if the value is less than the target value.
    /// Supports cross-type numeric comparison (Int vs Float).
    /// String comparison uses lexicographic ordering.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// let pred = p::lt(50);
    /// assert!(pred.test(&Value::Int(30)));
    /// assert!(!pred.test(&Value::Int(50)));
    /// assert!(pred.test(&Value::Float(30.0))); // Cross-type comparison
    /// ```
    #[derive(Clone)]
    pub struct Lt(Value);

    impl Predicate for Lt {
        fn test(&self, value: &Value) -> bool {
            match (value, &self.0) {
                (Value::Int(a), Value::Int(b)) => a < b,
                (Value::Float(a), Value::Float(b)) => a < b,
                (Value::Int(a), Value::Float(b)) => (*a as f64) < *b,
                (Value::Float(a), Value::Int(b)) => *a < (*b as f64),
                (Value::String(a), Value::String(b)) => a < b,
                _ => false,
            }
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create a less-than predicate.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to compare against (can be any type that implements `Into<Value>`)
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// // Filter for values less than 50
    /// let pred = p::lt(50);
    /// ```
    pub fn lt<T: Into<Value>>(value: T) -> Lt {
        Lt(value.into())
    }

    /// Less than or equal to predicate.
    ///
    /// Tests if the value is less than or equal to the target value.
    /// Supports cross-type numeric comparison (Int vs Float).
    /// String comparison uses lexicographic ordering.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// let pred = p::lte(50);
    /// assert!(pred.test(&Value::Int(30)));
    /// assert!(pred.test(&Value::Int(50))); // Equal case
    /// assert!(!pred.test(&Value::Int(51)));
    /// ```
    #[derive(Clone)]
    pub struct Lte(Value);

    impl Predicate for Lte {
        fn test(&self, value: &Value) -> bool {
            match (value, &self.0) {
                (Value::Int(a), Value::Int(b)) => a <= b,
                (Value::Float(a), Value::Float(b)) => a <= b,
                (Value::Int(a), Value::Float(b)) => (*a as f64) <= *b,
                (Value::Float(a), Value::Int(b)) => *a <= (*b as f64),
                (Value::String(a), Value::String(b)) => a <= b,
                _ => false,
            }
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create a less-than-or-equal predicate.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to compare against (can be any type that implements `Into<Value>`)
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// // Filter for values less than or equal to 50
    /// let pred = p::lte(50);
    /// ```
    pub fn lte<T: Into<Value>>(value: T) -> Lte {
        Lte(value.into())
    }

    /// Greater than predicate.
    ///
    /// Tests if the value is greater than the target value.
    /// Supports cross-type numeric comparison (Int vs Float).
    /// String comparison uses lexicographic ordering.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// let pred = p::gt(50);
    /// assert!(pred.test(&Value::Int(60)));
    /// assert!(!pred.test(&Value::Int(50)));
    /// assert!(pred.test(&Value::Float(60.0))); // Cross-type comparison
    /// ```
    #[derive(Clone)]
    pub struct Gt(Value);

    impl Predicate for Gt {
        fn test(&self, value: &Value) -> bool {
            match (value, &self.0) {
                (Value::Int(a), Value::Int(b)) => a > b,
                (Value::Float(a), Value::Float(b)) => a > b,
                (Value::Int(a), Value::Float(b)) => (*a as f64) > *b,
                (Value::Float(a), Value::Int(b)) => *a > (*b as f64),
                (Value::String(a), Value::String(b)) => a > b,
                _ => false,
            }
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create a greater-than predicate.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to compare against (can be any type that implements `Into<Value>`)
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// // Filter for values greater than 50
    /// let pred = p::gt(50);
    /// ```
    pub fn gt<T: Into<Value>>(value: T) -> Gt {
        Gt(value.into())
    }

    /// Greater than or equal to predicate.
    ///
    /// Tests if the value is greater than or equal to the target value.
    /// Supports cross-type numeric comparison (Int vs Float).
    /// String comparison uses lexicographic ordering.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// let pred = p::gte(50);
    /// assert!(pred.test(&Value::Int(60)));
    /// assert!(pred.test(&Value::Int(50))); // Equal case
    /// assert!(!pred.test(&Value::Int(49)));
    /// ```
    #[derive(Clone)]
    pub struct Gte(Value);

    impl Predicate for Gte {
        fn test(&self, value: &Value) -> bool {
            match (value, &self.0) {
                (Value::Int(a), Value::Int(b)) => a >= b,
                (Value::Float(a), Value::Float(b)) => a >= b,
                (Value::Int(a), Value::Float(b)) => (*a as f64) >= *b,
                (Value::Float(a), Value::Int(b)) => *a >= (*b as f64),
                (Value::String(a), Value::String(b)) => a >= b,
                _ => false,
            }
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create a greater-than-or-equal predicate.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to compare against (can be any type that implements `Into<Value>`)
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// // Filter for values greater than or equal to 18
    /// let pred = p::gte(18);
    /// ```
    pub fn gte<T: Into<Value>>(value: T) -> Gte {
        Gte(value.into())
    }

    // Predicates will be added in subsequent phases:
    // - Phase 1.3: between, inside, outside
    // - Phase 1.4: within, without
    // - Phase 1.5: containing, starting_with, ending_with
    // - Phase 1.6: regex
    // - Phase 1.7: and, or, not
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // A simple test predicate for verifying the trait works
    #[derive(Clone)]
    struct IsPositive;

    impl Predicate for IsPositive {
        fn test(&self, value: &Value) -> bool {
            match value {
                Value::Int(n) => *n > 0,
                Value::Float(f) => *f > 0.0,
                _ => false,
            }
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    #[test]
    fn predicate_trait_compiles() {
        let pred = IsPositive;
        assert!(pred.test(&Value::Int(42)));
        assert!(!pred.test(&Value::Int(-1)));
        assert!(!pred.test(&Value::Int(0)));
    }

    #[test]
    fn predicate_test_works_with_different_value_types() {
        let pred = IsPositive;

        // Int
        assert!(pred.test(&Value::Int(1)));
        assert!(!pred.test(&Value::Int(-1)));

        // Float
        assert!(pred.test(&Value::Float(0.1)));
        assert!(!pred.test(&Value::Float(-0.1)));

        // Non-numeric types return false
        assert!(!pred.test(&Value::String("positive".to_string())));
        assert!(!pred.test(&Value::Bool(true)));
        assert!(!pred.test(&Value::Null));
    }

    #[test]
    fn box_dyn_predicate_is_clonable() {
        let pred: Box<dyn Predicate> = Box::new(IsPositive);
        let cloned = pred.clone();

        // Both should work identically
        assert!(pred.test(&Value::Int(5)));
        assert!(cloned.test(&Value::Int(5)));
        assert!(!pred.test(&Value::Int(-5)));
        assert!(!cloned.test(&Value::Int(-5)));
    }

    #[test]
    fn predicate_clone_box_works() {
        let pred = IsPositive;
        let boxed = pred.clone_box();

        assert!(boxed.test(&Value::Int(100)));
        assert!(!boxed.test(&Value::Int(-100)));
    }

    #[test]
    fn predicates_can_be_stored_in_vec() {
        let predicates: Vec<Box<dyn Predicate>> = vec![
            Box::new(IsPositive),
            Box::new(IsPositive),
            Box::new(IsPositive),
        ];

        assert_eq!(predicates.len(), 3);

        // All should work
        for pred in &predicates {
            assert!(pred.test(&Value::Int(1)));
        }
    }

    #[test]
    fn vec_of_predicates_is_clonable() {
        let predicates: Vec<Box<dyn Predicate>> = vec![Box::new(IsPositive), Box::new(IsPositive)];

        let cloned: Vec<Box<dyn Predicate>> = predicates.iter().map(|p| p.clone_box()).collect();

        assert_eq!(cloned.len(), 2);
        for pred in &cloned {
            assert!(pred.test(&Value::Int(42)));
        }
    }

    #[test]
    fn predicate_is_send_sync() {
        // Verify that predicates can be sent across threads
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Box<dyn Predicate>>();
    }

    // Test a more complex predicate to ensure the pattern works
    #[derive(Clone)]
    struct InRange {
        min: i64,
        max: i64,
    }

    impl Predicate for InRange {
        fn test(&self, value: &Value) -> bool {
            match value {
                Value::Int(n) => *n >= self.min && *n <= self.max,
                _ => false,
            }
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    #[test]
    fn complex_predicate_works() {
        let pred = InRange { min: 10, max: 20 };

        assert!(!pred.test(&Value::Int(5)));
        assert!(pred.test(&Value::Int(10)));
        assert!(pred.test(&Value::Int(15)));
        assert!(pred.test(&Value::Int(20)));
        assert!(!pred.test(&Value::Int(25)));
    }

    #[test]
    fn predicates_with_state_clone_correctly() {
        let pred: Box<dyn Predicate> = Box::new(InRange { min: 0, max: 100 });
        let cloned = pred.clone();

        // Both should have the same state
        assert!(pred.test(&Value::Int(50)));
        assert!(cloned.test(&Value::Int(50)));
        assert!(!pred.test(&Value::Int(150)));
        assert!(!cloned.test(&Value::Int(150)));
    }

    // -------------------------------------------------------------------------
    // Phase 1.2: Comparison Predicates Tests
    // -------------------------------------------------------------------------

    mod comparison_predicates {
        use super::*;

        // -------------------------------------------------------------------
        // eq() tests
        // -------------------------------------------------------------------

        #[test]
        fn eq_matches_equal_int() {
            let pred = p::eq(42);
            assert!(pred.test(&Value::Int(42)));
        }

        #[test]
        fn eq_does_not_match_unequal_int() {
            let pred = p::eq(42);
            assert!(!pred.test(&Value::Int(41)));
            assert!(!pred.test(&Value::Int(43)));
        }

        #[test]
        fn eq_matches_equal_string() {
            let pred = p::eq("Alice");
            assert!(pred.test(&Value::String("Alice".to_string())));
        }

        #[test]
        fn eq_does_not_match_unequal_string() {
            let pred = p::eq("Alice");
            assert!(!pred.test(&Value::String("Bob".to_string())));
        }

        #[test]
        fn eq_matches_equal_float() {
            let pred = p::eq(3.14f64);
            assert!(pred.test(&Value::Float(3.14)));
        }

        #[test]
        fn eq_does_not_match_different_types() {
            let pred = p::eq(42);
            // Int(42) != String("42")
            assert!(!pred.test(&Value::String("42".to_string())));
            // Int(42) != Float(42.0) (different Value variants)
            assert!(!pred.test(&Value::Float(42.0)));
        }

        #[test]
        fn eq_matches_equal_bool() {
            let pred = p::eq(true);
            assert!(pred.test(&Value::Bool(true)));
            assert!(!pred.test(&Value::Bool(false)));
        }

        #[test]
        fn eq_is_clonable() {
            let pred = p::eq(42);
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::Int(42)));
        }

        // -------------------------------------------------------------------
        // neq() tests
        // -------------------------------------------------------------------

        #[test]
        fn neq_matches_unequal_int() {
            let pred = p::neq(42);
            assert!(pred.test(&Value::Int(41)));
            assert!(pred.test(&Value::Int(43)));
        }

        #[test]
        fn neq_does_not_match_equal_int() {
            let pred = p::neq(42);
            assert!(!pred.test(&Value::Int(42)));
        }

        #[test]
        fn neq_matches_different_types() {
            let pred = p::neq(42);
            // Int(42) != String("42") is true
            assert!(pred.test(&Value::String("42".to_string())));
        }

        #[test]
        fn neq_is_clonable() {
            let pred = p::neq(42);
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::Int(41)));
        }

        // -------------------------------------------------------------------
        // lt() tests
        // -------------------------------------------------------------------

        #[test]
        fn lt_with_int() {
            let pred = p::lt(50);
            assert!(pred.test(&Value::Int(30)));
            assert!(pred.test(&Value::Int(49)));
            assert!(!pred.test(&Value::Int(50)));
            assert!(!pred.test(&Value::Int(51)));
        }

        #[test]
        fn lt_with_float() {
            let pred = p::lt(50.0f64);
            assert!(pred.test(&Value::Float(30.0)));
            assert!(pred.test(&Value::Float(49.9)));
            assert!(!pred.test(&Value::Float(50.0)));
            assert!(!pred.test(&Value::Float(50.1)));
        }

        #[test]
        fn lt_cross_type_int_to_float() {
            // value (Int) < target (Float)
            let pred = p::lt(50.5f64);
            assert!(pred.test(&Value::Int(50)));
            assert!(!pred.test(&Value::Int(51)));
        }

        #[test]
        fn lt_cross_type_float_to_int() {
            // value (Float) < target (Int)
            let pred = p::lt(50);
            assert!(pred.test(&Value::Float(49.9)));
            assert!(!pred.test(&Value::Float(50.0)));
            assert!(!pred.test(&Value::Float(50.1)));
        }

        #[test]
        fn lt_with_string() {
            let pred = p::lt("bob");
            assert!(pred.test(&Value::String("alice".to_string())));
            assert!(pred.test(&Value::String("bbb".to_string())));
            assert!(!pred.test(&Value::String("bob".to_string())));
            assert!(!pred.test(&Value::String("charlie".to_string())));
        }

        #[test]
        fn lt_returns_false_for_incompatible_types() {
            let pred = p::lt(50);
            assert!(!pred.test(&Value::String("30".to_string())));
            assert!(!pred.test(&Value::Bool(true)));
            assert!(!pred.test(&Value::Null));
        }

        #[test]
        fn lt_is_clonable() {
            let pred = p::lt(50);
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::Int(30)));
        }

        // -------------------------------------------------------------------
        // lte() tests
        // -------------------------------------------------------------------

        #[test]
        fn lte_with_int() {
            let pred = p::lte(50);
            assert!(pred.test(&Value::Int(30)));
            assert!(pred.test(&Value::Int(50)));
            assert!(!pred.test(&Value::Int(51)));
        }

        #[test]
        fn lte_with_float() {
            let pred = p::lte(50.0f64);
            assert!(pred.test(&Value::Float(30.0)));
            assert!(pred.test(&Value::Float(50.0)));
            assert!(!pred.test(&Value::Float(50.1)));
        }

        #[test]
        fn lte_cross_type_int_to_float() {
            let pred = p::lte(50.0f64);
            assert!(pred.test(&Value::Int(49)));
            assert!(pred.test(&Value::Int(50)));
            assert!(!pred.test(&Value::Int(51)));
        }

        #[test]
        fn lte_cross_type_float_to_int() {
            let pred = p::lte(50);
            assert!(pred.test(&Value::Float(49.9)));
            assert!(pred.test(&Value::Float(50.0)));
            assert!(!pred.test(&Value::Float(50.1)));
        }

        #[test]
        fn lte_with_string() {
            let pred = p::lte("bob");
            assert!(pred.test(&Value::String("alice".to_string())));
            assert!(pred.test(&Value::String("bob".to_string())));
            assert!(!pred.test(&Value::String("charlie".to_string())));
        }

        #[test]
        fn lte_is_clonable() {
            let pred = p::lte(50);
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::Int(50)));
        }

        // -------------------------------------------------------------------
        // gt() tests
        // -------------------------------------------------------------------

        #[test]
        fn gt_with_int() {
            let pred = p::gt(50);
            assert!(pred.test(&Value::Int(60)));
            assert!(pred.test(&Value::Int(51)));
            assert!(!pred.test(&Value::Int(50)));
            assert!(!pred.test(&Value::Int(49)));
        }

        #[test]
        fn gt_with_float() {
            let pred = p::gt(50.0f64);
            assert!(pred.test(&Value::Float(60.0)));
            assert!(pred.test(&Value::Float(50.1)));
            assert!(!pred.test(&Value::Float(50.0)));
            assert!(!pred.test(&Value::Float(49.9)));
        }

        #[test]
        fn gt_cross_type_int_to_float() {
            let pred = p::gt(50.0f64);
            assert!(pred.test(&Value::Int(51)));
            assert!(!pred.test(&Value::Int(50)));
            assert!(!pred.test(&Value::Int(49)));
        }

        #[test]
        fn gt_cross_type_float_to_int() {
            let pred = p::gt(50);
            assert!(pred.test(&Value::Float(60.0)));
            assert!(pred.test(&Value::Float(50.1)));
            assert!(!pred.test(&Value::Float(50.0)));
            assert!(!pred.test(&Value::Float(49.9)));
        }

        #[test]
        fn gt_with_string() {
            let pred = p::gt("bob");
            assert!(pred.test(&Value::String("charlie".to_string())));
            assert!(!pred.test(&Value::String("bob".to_string())));
            assert!(!pred.test(&Value::String("alice".to_string())));
        }

        #[test]
        fn gt_returns_false_for_incompatible_types() {
            let pred = p::gt(50);
            assert!(!pred.test(&Value::String("60".to_string())));
            assert!(!pred.test(&Value::Bool(true)));
            assert!(!pred.test(&Value::Null));
        }

        #[test]
        fn gt_is_clonable() {
            let pred = p::gt(50);
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::Int(60)));
        }

        // -------------------------------------------------------------------
        // gte() tests
        // -------------------------------------------------------------------

        #[test]
        fn gte_with_int() {
            let pred = p::gte(50);
            assert!(pred.test(&Value::Int(60)));
            assert!(pred.test(&Value::Int(50)));
            assert!(!pred.test(&Value::Int(49)));
        }

        #[test]
        fn gte_with_float() {
            let pred = p::gte(50.0f64);
            assert!(pred.test(&Value::Float(60.0)));
            assert!(pred.test(&Value::Float(50.0)));
            assert!(!pred.test(&Value::Float(49.9)));
        }

        #[test]
        fn gte_cross_type_int_to_float() {
            let pred = p::gte(50.0f64);
            assert!(pred.test(&Value::Int(51)));
            assert!(pred.test(&Value::Int(50)));
            assert!(!pred.test(&Value::Int(49)));
        }

        #[test]
        fn gte_cross_type_float_to_int() {
            let pred = p::gte(50);
            assert!(pred.test(&Value::Float(60.0)));
            assert!(pred.test(&Value::Float(50.0)));
            assert!(!pred.test(&Value::Float(49.9)));
        }

        #[test]
        fn gte_with_string() {
            let pred = p::gte("bob");
            assert!(pred.test(&Value::String("charlie".to_string())));
            assert!(pred.test(&Value::String("bob".to_string())));
            assert!(!pred.test(&Value::String("alice".to_string())));
        }

        #[test]
        fn gte_is_clonable() {
            let pred = p::gte(50);
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::Int(50)));
        }

        // -------------------------------------------------------------------
        // Combined/edge case tests
        // -------------------------------------------------------------------

        #[test]
        fn comparison_predicates_implement_predicate_trait() {
            // Verify all can be used as Box<dyn Predicate>
            let predicates: Vec<Box<dyn Predicate>> = vec![
                Box::new(p::eq(42)),
                Box::new(p::neq(42)),
                Box::new(p::lt(42)),
                Box::new(p::lte(42)),
                Box::new(p::gt(42)),
                Box::new(p::gte(42)),
            ];

            // All should implement Predicate
            assert_eq!(predicates.len(), 6);
            for pred in &predicates {
                // Just verify test() can be called
                let _ = pred.test(&Value::Int(42));
            }
        }

        #[test]
        fn comparison_predicates_are_send_sync() {
            fn assert_send_sync<T: Send + Sync>() {}
            assert_send_sync::<p::Eq>();
            assert_send_sync::<p::Neq>();
            assert_send_sync::<p::Lt>();
            assert_send_sync::<p::Lte>();
            assert_send_sync::<p::Gt>();
            assert_send_sync::<p::Gte>();
        }

        #[test]
        fn negative_numbers() {
            let pred = p::lt(0);
            assert!(pred.test(&Value::Int(-1)));
            assert!(pred.test(&Value::Int(-100)));
            assert!(!pred.test(&Value::Int(0)));
            assert!(!pred.test(&Value::Int(1)));

            let pred = p::gt(-10);
            assert!(pred.test(&Value::Int(-5)));
            assert!(pred.test(&Value::Int(0)));
            assert!(!pred.test(&Value::Int(-10)));
            assert!(!pred.test(&Value::Int(-15)));
        }

        #[test]
        fn float_edge_cases() {
            // Very small differences
            let pred = p::lt(1.0000001f64);
            assert!(pred.test(&Value::Float(1.0)));
            assert!(!pred.test(&Value::Float(1.0000001)));

            // Zero
            let pred = p::gte(0.0f64);
            assert!(pred.test(&Value::Float(0.0)));
            assert!(pred.test(&Value::Float(0.1)));
            assert!(!pred.test(&Value::Float(-0.1)));
        }
    }
}
