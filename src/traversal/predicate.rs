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

    // -------------------------------------------------------------------------
    // Range Predicates (Phase 1.3)
    // -------------------------------------------------------------------------

    /// Between predicate (inclusive start, exclusive end).
    ///
    /// Tests if the value is within the range [start, end).
    /// Uses the existing Gte and Lt predicates for comparison.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// let pred = p::between(10, 20);
    /// assert!(pred.test(&Value::Int(10)));  // inclusive start
    /// assert!(pred.test(&Value::Int(15)));  // in range
    /// assert!(!pred.test(&Value::Int(20))); // exclusive end
    /// assert!(!pred.test(&Value::Int(5)));  // below range
    /// ```
    #[derive(Clone)]
    pub struct Between(Value, Value);

    impl Predicate for Between {
        fn test(&self, value: &Value) -> bool {
            // value >= start && value < end
            Gte(self.0.clone()).test(value) && Lt(self.1.clone()).test(value)
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create a between predicate (inclusive start, exclusive end).
    ///
    /// Tests if value is in the range [start, end).
    ///
    /// # Arguments
    ///
    /// * `start` - The inclusive lower bound
    /// * `end` - The exclusive upper bound
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// // Filter for ages 18-65 (exclusive of 65)
    /// let pred = p::between(18, 65);
    /// ```
    pub fn between<T: Into<Value>>(start: T, end: T) -> Between {
        Between(start.into(), end.into())
    }

    /// Inside predicate (exclusive both ends).
    ///
    /// Tests if the value is strictly inside the range (start, end).
    /// Both endpoints are excluded.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// let pred = p::inside(10, 20);
    /// assert!(!pred.test(&Value::Int(10))); // exclusive start
    /// assert!(pred.test(&Value::Int(15)));  // in range
    /// assert!(!pred.test(&Value::Int(20))); // exclusive end
    /// ```
    #[derive(Clone)]
    pub struct Inside(Value, Value);

    impl Predicate for Inside {
        fn test(&self, value: &Value) -> bool {
            // value > start && value < end
            Gt(self.0.clone()).test(value) && Lt(self.1.clone()).test(value)
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create an inside predicate (exclusive both ends).
    ///
    /// Tests if value is in the range (start, end), excluding both endpoints.
    ///
    /// # Arguments
    ///
    /// * `start` - The exclusive lower bound
    /// * `end` - The exclusive upper bound
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// // Filter for values strictly between 0 and 100
    /// let pred = p::inside(0, 100);
    /// ```
    pub fn inside<T: Into<Value>>(start: T, end: T) -> Inside {
        Inside(start.into(), end.into())
    }

    /// Outside predicate.
    ///
    /// Tests if the value is outside the range [start, end].
    /// Returns true if value < start OR value > end.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// let pred = p::outside(10, 20);
    /// assert!(pred.test(&Value::Int(5)));   // below range
    /// assert!(!pred.test(&Value::Int(10))); // at start (inside)
    /// assert!(!pred.test(&Value::Int(15))); // in range
    /// assert!(!pred.test(&Value::Int(20))); // at end (inside)
    /// assert!(pred.test(&Value::Int(25)));  // above range
    /// ```
    #[derive(Clone)]
    pub struct Outside(Value, Value);

    impl Predicate for Outside {
        fn test(&self, value: &Value) -> bool {
            // value < start || value > end
            Lt(self.0.clone()).test(value) || Gt(self.1.clone()).test(value)
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create an outside predicate.
    ///
    /// Tests if value is outside the range [start, end].
    /// Returns true if value < start OR value > end.
    ///
    /// # Arguments
    ///
    /// * `start` - Values less than this are outside
    /// * `end` - Values greater than this are outside
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// // Filter for values outside the normal range
    /// let pred = p::outside(0, 100);
    /// ```
    pub fn outside<T: Into<Value>>(start: T, end: T) -> Outside {
        Outside(start.into(), end.into())
    }

    // -------------------------------------------------------------------------
    // Collection Predicates (Phase 1.4)
    // -------------------------------------------------------------------------

    /// Within predicate (value is in set).
    ///
    /// Tests if the value is contained in the given set of values.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// let pred = p::within([1, 2, 3]);
    /// assert!(pred.test(&Value::Int(2)));
    /// assert!(!pred.test(&Value::Int(4)));
    /// ```
    #[derive(Clone)]
    pub struct Within(Vec<Value>);

    impl Predicate for Within {
        fn test(&self, value: &Value) -> bool {
            self.0.contains(value)
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create a within predicate (value is in set).
    ///
    /// Tests if value is contained in the given collection of values.
    ///
    /// # Arguments
    ///
    /// * `values` - An iterable of values to check membership against
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// // Filter for specific names
    /// let pred = p::within(["Alice", "Bob", "Carol"]);
    ///
    /// // Filter for specific numbers
    /// let pred = p::within([1, 2, 3, 5, 8, 13]);
    ///
    /// // Using a vec
    /// let names = vec!["Alice", "Bob"];
    /// let pred = p::within(names);
    /// ```
    pub fn within<T, I>(values: I) -> Within
    where
        T: Into<Value>,
        I: IntoIterator<Item = T>,
    {
        Within(values.into_iter().map(Into::into).collect())
    }

    /// Without predicate (value is NOT in set).
    ///
    /// Tests if the value is NOT contained in the given set of values.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// let pred = p::without([1, 2, 3]);
    /// assert!(pred.test(&Value::Int(4)));
    /// assert!(!pred.test(&Value::Int(2)));
    /// ```
    #[derive(Clone)]
    pub struct Without(Vec<Value>);

    impl Predicate for Without {
        fn test(&self, value: &Value) -> bool {
            !self.0.contains(value)
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create a without predicate (value is NOT in set).
    ///
    /// Tests if value is NOT contained in the given collection of values.
    ///
    /// # Arguments
    ///
    /// * `values` - An iterable of values to check membership against
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// // Exclude specific statuses
    /// let pred = p::without(["deleted", "archived"]);
    ///
    /// // Exclude specific IDs
    /// let pred = p::without([0, -1]);
    /// ```
    pub fn without<T, I>(values: I) -> Without
    where
        T: Into<Value>,
        I: IntoIterator<Item = T>,
    {
        Without(values.into_iter().map(Into::into).collect())
    }

    // Predicates will be added in subsequent phases:
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

    // -------------------------------------------------------------------------
    // Phase 1.3: Range Predicates Tests
    // -------------------------------------------------------------------------

    mod range_predicates {
        use super::*;

        // -------------------------------------------------------------------
        // between() tests
        // -------------------------------------------------------------------

        #[test]
        fn between_inclusive_start() {
            let pred = p::between(10, 20);
            assert!(pred.test(&Value::Int(10))); // inclusive start
        }

        #[test]
        fn between_exclusive_end() {
            let pred = p::between(10, 20);
            assert!(!pred.test(&Value::Int(20))); // exclusive end
        }

        #[test]
        fn between_in_range() {
            let pred = p::between(10, 20);
            assert!(pred.test(&Value::Int(15)));
            assert!(pred.test(&Value::Int(11)));
            assert!(pred.test(&Value::Int(19)));
        }

        #[test]
        fn between_below_range() {
            let pred = p::between(10, 20);
            assert!(!pred.test(&Value::Int(5)));
            assert!(!pred.test(&Value::Int(9)));
        }

        #[test]
        fn between_above_range() {
            let pred = p::between(10, 20);
            assert!(!pred.test(&Value::Int(21)));
            assert!(!pred.test(&Value::Int(100)));
        }

        #[test]
        fn between_with_floats() {
            let pred = p::between(10.0f64, 20.0f64);
            assert!(pred.test(&Value::Float(10.0))); // inclusive start
            assert!(pred.test(&Value::Float(15.5)));
            assert!(!pred.test(&Value::Float(20.0))); // exclusive end
            assert!(!pred.test(&Value::Float(9.9)));
            assert!(!pred.test(&Value::Float(20.1)));
        }

        #[test]
        fn between_cross_type() {
            // Int range, float value
            let pred = p::between(10, 20);
            assert!(pred.test(&Value::Float(10.0)));
            assert!(pred.test(&Value::Float(15.5)));
            assert!(!pred.test(&Value::Float(20.0)));
            assert!(!pred.test(&Value::Float(9.9)));
        }

        #[test]
        fn between_with_strings() {
            let pred = p::between("b", "d");
            assert!(pred.test(&Value::String("b".to_string()))); // inclusive start
            assert!(pred.test(&Value::String("c".to_string())));
            assert!(!pred.test(&Value::String("d".to_string()))); // exclusive end
            assert!(!pred.test(&Value::String("a".to_string())));
            assert!(!pred.test(&Value::String("e".to_string())));
        }

        #[test]
        fn between_returns_false_for_incompatible_types() {
            let pred = p::between(10, 20);
            assert!(!pred.test(&Value::String("15".to_string())));
            assert!(!pred.test(&Value::Bool(true)));
            assert!(!pred.test(&Value::Null));
        }

        #[test]
        fn between_is_clonable() {
            let pred = p::between(10, 20);
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::Int(15)));
            assert!(!cloned.test(&Value::Int(5)));
        }

        #[test]
        fn between_with_negative_range() {
            let pred = p::between(-10, 10);
            assert!(pred.test(&Value::Int(-10))); // inclusive start
            assert!(pred.test(&Value::Int(0)));
            assert!(pred.test(&Value::Int(9)));
            assert!(!pred.test(&Value::Int(10))); // exclusive end
            assert!(!pred.test(&Value::Int(-11)));
        }

        // -------------------------------------------------------------------
        // inside() tests
        // -------------------------------------------------------------------

        #[test]
        fn inside_exclusive_start() {
            let pred = p::inside(10, 20);
            assert!(!pred.test(&Value::Int(10))); // exclusive start
        }

        #[test]
        fn inside_exclusive_end() {
            let pred = p::inside(10, 20);
            assert!(!pred.test(&Value::Int(20))); // exclusive end
        }

        #[test]
        fn inside_in_range() {
            let pred = p::inside(10, 20);
            assert!(pred.test(&Value::Int(11)));
            assert!(pred.test(&Value::Int(15)));
            assert!(pred.test(&Value::Int(19)));
        }

        #[test]
        fn inside_below_range() {
            let pred = p::inside(10, 20);
            assert!(!pred.test(&Value::Int(5)));
            assert!(!pred.test(&Value::Int(9)));
        }

        #[test]
        fn inside_above_range() {
            let pred = p::inside(10, 20);
            assert!(!pred.test(&Value::Int(21)));
            assert!(!pred.test(&Value::Int(100)));
        }

        #[test]
        fn inside_with_floats() {
            let pred = p::inside(10.0f64, 20.0f64);
            assert!(!pred.test(&Value::Float(10.0))); // exclusive start
            assert!(pred.test(&Value::Float(10.1)));
            assert!(pred.test(&Value::Float(15.5)));
            assert!(pred.test(&Value::Float(19.9)));
            assert!(!pred.test(&Value::Float(20.0))); // exclusive end
        }

        #[test]
        fn inside_cross_type() {
            // Int range, float value
            let pred = p::inside(10, 20);
            assert!(!pred.test(&Value::Float(10.0)));
            assert!(pred.test(&Value::Float(10.5)));
            assert!(pred.test(&Value::Float(15.0)));
            assert!(pred.test(&Value::Float(19.5)));
            assert!(!pred.test(&Value::Float(20.0)));
        }

        #[test]
        fn inside_with_strings() {
            let pred = p::inside("b", "d");
            assert!(!pred.test(&Value::String("b".to_string()))); // exclusive start
            assert!(pred.test(&Value::String("c".to_string())));
            assert!(!pred.test(&Value::String("d".to_string()))); // exclusive end
            assert!(!pred.test(&Value::String("a".to_string())));
        }

        #[test]
        fn inside_returns_false_for_incompatible_types() {
            let pred = p::inside(10, 20);
            assert!(!pred.test(&Value::String("15".to_string())));
            assert!(!pred.test(&Value::Bool(true)));
            assert!(!pred.test(&Value::Null));
        }

        #[test]
        fn inside_is_clonable() {
            let pred = p::inside(10, 20);
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::Int(15)));
            assert!(!cloned.test(&Value::Int(10)));
        }

        #[test]
        fn inside_with_negative_range() {
            let pred = p::inside(-10, 10);
            assert!(!pred.test(&Value::Int(-10))); // exclusive start
            assert!(pred.test(&Value::Int(-9)));
            assert!(pred.test(&Value::Int(0)));
            assert!(pred.test(&Value::Int(9)));
            assert!(!pred.test(&Value::Int(10))); // exclusive end
        }

        // -------------------------------------------------------------------
        // outside() tests
        // -------------------------------------------------------------------

        #[test]
        fn outside_below_range() {
            let pred = p::outside(10, 20);
            assert!(pred.test(&Value::Int(5)));
            assert!(pred.test(&Value::Int(9)));
        }

        #[test]
        fn outside_above_range() {
            let pred = p::outside(10, 20);
            assert!(pred.test(&Value::Int(21)));
            assert!(pred.test(&Value::Int(100)));
        }

        #[test]
        fn outside_at_start_boundary() {
            let pred = p::outside(10, 20);
            assert!(!pred.test(&Value::Int(10))); // at start (inside)
        }

        #[test]
        fn outside_at_end_boundary() {
            let pred = p::outside(10, 20);
            assert!(!pred.test(&Value::Int(20))); // at end (inside)
        }

        #[test]
        fn outside_in_range() {
            let pred = p::outside(10, 20);
            assert!(!pred.test(&Value::Int(15)));
            assert!(!pred.test(&Value::Int(11)));
            assert!(!pred.test(&Value::Int(19)));
        }

        #[test]
        fn outside_with_floats() {
            let pred = p::outside(10.0f64, 20.0f64);
            assert!(pred.test(&Value::Float(9.9)));
            assert!(pred.test(&Value::Float(20.1)));
            assert!(!pred.test(&Value::Float(10.0)));
            assert!(!pred.test(&Value::Float(15.0)));
            assert!(!pred.test(&Value::Float(20.0)));
        }

        #[test]
        fn outside_cross_type() {
            // Int range, float value
            let pred = p::outside(10, 20);
            assert!(pred.test(&Value::Float(9.9)));
            assert!(pred.test(&Value::Float(20.1)));
            assert!(!pred.test(&Value::Float(10.0)));
            assert!(!pred.test(&Value::Float(15.0)));
        }

        #[test]
        fn outside_with_strings() {
            let pred = p::outside("b", "d");
            assert!(pred.test(&Value::String("a".to_string())));
            assert!(pred.test(&Value::String("e".to_string())));
            assert!(!pred.test(&Value::String("b".to_string())));
            assert!(!pred.test(&Value::String("c".to_string())));
            assert!(!pred.test(&Value::String("d".to_string())));
        }

        #[test]
        fn outside_returns_false_for_incompatible_types() {
            // Incompatible types return false for both Lt and Gt,
            // so outside (Lt || Gt) returns false
            let pred = p::outside(10, 20);
            assert!(!pred.test(&Value::String("5".to_string())));
            assert!(!pred.test(&Value::Bool(true)));
            assert!(!pred.test(&Value::Null));
        }

        #[test]
        fn outside_is_clonable() {
            let pred = p::outside(10, 20);
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::Int(5)));
            assert!(!cloned.test(&Value::Int(15)));
        }

        #[test]
        fn outside_with_negative_range() {
            let pred = p::outside(-10, 10);
            assert!(pred.test(&Value::Int(-11)));
            assert!(pred.test(&Value::Int(11)));
            assert!(!pred.test(&Value::Int(-10)));
            assert!(!pred.test(&Value::Int(0)));
            assert!(!pred.test(&Value::Int(10)));
        }

        // -------------------------------------------------------------------
        // Combined/edge case tests
        // -------------------------------------------------------------------

        #[test]
        fn range_predicates_implement_predicate_trait() {
            // Verify all can be used as Box<dyn Predicate>
            let predicates: Vec<Box<dyn Predicate>> = vec![
                Box::new(p::between(10, 20)),
                Box::new(p::inside(10, 20)),
                Box::new(p::outside(10, 20)),
            ];

            assert_eq!(predicates.len(), 3);
            for pred in &predicates {
                let _ = pred.test(&Value::Int(15));
            }
        }

        #[test]
        fn range_predicates_are_send_sync() {
            fn assert_send_sync<T: Send + Sync>() {}
            assert_send_sync::<p::Between>();
            assert_send_sync::<p::Inside>();
            assert_send_sync::<p::Outside>();
        }

        #[test]
        fn single_value_range() {
            // between(10, 11) should only match 10
            let pred = p::between(10, 11);
            assert!(pred.test(&Value::Int(10)));
            assert!(!pred.test(&Value::Int(11)));
            assert!(!pred.test(&Value::Int(9)));

            // inside(10, 11) should match nothing (no integers strictly between 10 and 11)
            let pred = p::inside(10, 11);
            assert!(!pred.test(&Value::Int(10)));
            assert!(!pred.test(&Value::Int(11)));
            // But floats can be inside
            assert!(pred.test(&Value::Float(10.5)));
        }

        #[test]
        fn empty_range() {
            // between(10, 10) should match nothing (start >= end)
            let pred = p::between(10, 10);
            assert!(!pred.test(&Value::Int(10)));
            assert!(!pred.test(&Value::Int(9)));
            assert!(!pred.test(&Value::Int(11)));
        }

        #[test]
        fn inverted_range() {
            // between(20, 10) should match nothing (start > end)
            let pred = p::between(20, 10);
            assert!(!pred.test(&Value::Int(15)));
            assert!(!pred.test(&Value::Int(10)));
            assert!(!pred.test(&Value::Int(20)));
        }
    }

    // -------------------------------------------------------------------------
    // Phase 1.4: Collection Predicates Tests
    // -------------------------------------------------------------------------

    mod collection_predicates {
        use super::*;

        // -------------------------------------------------------------------
        // within() tests
        // -------------------------------------------------------------------

        #[test]
        fn within_matches_value_in_set() {
            let pred = p::within([1, 2, 3]);
            assert!(pred.test(&Value::Int(1)));
            assert!(pred.test(&Value::Int(2)));
            assert!(pred.test(&Value::Int(3)));
        }

        #[test]
        fn within_does_not_match_value_not_in_set() {
            let pred = p::within([1, 2, 3]);
            assert!(!pred.test(&Value::Int(0)));
            assert!(!pred.test(&Value::Int(4)));
            assert!(!pred.test(&Value::Int(-1)));
        }

        #[test]
        fn within_with_strings() {
            let pred = p::within(["Alice", "Bob", "Carol"]);
            assert!(pred.test(&Value::String("Alice".to_string())));
            assert!(pred.test(&Value::String("Bob".to_string())));
            assert!(pred.test(&Value::String("Carol".to_string())));
            assert!(!pred.test(&Value::String("Dave".to_string())));
            assert!(!pred.test(&Value::String("alice".to_string()))); // case sensitive
        }

        #[test]
        fn within_with_floats() {
            let pred = p::within([1.0f64, 2.5f64, 3.14f64]);
            assert!(pred.test(&Value::Float(1.0)));
            assert!(pred.test(&Value::Float(2.5)));
            assert!(pred.test(&Value::Float(3.14)));
            assert!(!pred.test(&Value::Float(1.1)));
        }

        #[test]
        fn within_with_booleans() {
            let pred = p::within([true]);
            assert!(pred.test(&Value::Bool(true)));
            assert!(!pred.test(&Value::Bool(false)));
        }

        #[test]
        fn within_empty_set() {
            let pred = p::within::<i64, _>([]);
            assert!(!pred.test(&Value::Int(1)));
            assert!(!pred.test(&Value::String("any".to_string())));
        }

        #[test]
        fn within_single_element() {
            let pred = p::within([42]);
            assert!(pred.test(&Value::Int(42)));
            assert!(!pred.test(&Value::Int(41)));
            assert!(!pred.test(&Value::Int(43)));
        }

        #[test]
        fn within_does_not_match_different_types() {
            let pred = p::within([1, 2, 3]);
            // "1" is String, not Int
            assert!(!pred.test(&Value::String("1".to_string())));
            // 1.0 is Float, not Int
            assert!(!pred.test(&Value::Float(1.0)));
        }

        #[test]
        fn within_with_vec() {
            let values = vec![10, 20, 30];
            let pred = p::within(values);
            assert!(pred.test(&Value::Int(10)));
            assert!(pred.test(&Value::Int(20)));
            assert!(pred.test(&Value::Int(30)));
            assert!(!pred.test(&Value::Int(15)));
        }

        #[test]
        fn within_is_clonable() {
            let pred = p::within([1, 2, 3]);
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::Int(2)));
            assert!(!cloned.test(&Value::Int(4)));
        }

        #[test]
        fn within_with_negative_numbers() {
            let pred = p::within([-1, 0, 1]);
            assert!(pred.test(&Value::Int(-1)));
            assert!(pred.test(&Value::Int(0)));
            assert!(pred.test(&Value::Int(1)));
            assert!(!pred.test(&Value::Int(-2)));
        }

        // -------------------------------------------------------------------
        // without() tests
        // -------------------------------------------------------------------

        #[test]
        fn without_matches_value_not_in_set() {
            let pred = p::without([1, 2, 3]);
            assert!(pred.test(&Value::Int(0)));
            assert!(pred.test(&Value::Int(4)));
            assert!(pred.test(&Value::Int(-1)));
        }

        #[test]
        fn without_does_not_match_value_in_set() {
            let pred = p::without([1, 2, 3]);
            assert!(!pred.test(&Value::Int(1)));
            assert!(!pred.test(&Value::Int(2)));
            assert!(!pred.test(&Value::Int(3)));
        }

        #[test]
        fn without_with_strings() {
            let pred = p::without(["deleted", "archived"]);
            assert!(pred.test(&Value::String("active".to_string())));
            assert!(pred.test(&Value::String("pending".to_string())));
            assert!(!pred.test(&Value::String("deleted".to_string())));
            assert!(!pred.test(&Value::String("archived".to_string())));
        }

        #[test]
        fn without_with_floats() {
            let pred = p::without([0.0f64, -0.0f64]);
            assert!(pred.test(&Value::Float(1.0)));
            assert!(pred.test(&Value::Float(-1.0)));
            assert!(!pred.test(&Value::Float(0.0)));
        }

        #[test]
        fn without_empty_set() {
            let pred = p::without::<i64, _>([]);
            // Everything should match when set is empty
            assert!(pred.test(&Value::Int(1)));
            assert!(pred.test(&Value::Int(0)));
            assert!(pred.test(&Value::String("anything".to_string())));
        }

        #[test]
        fn without_single_element() {
            let pred = p::without([42]);
            assert!(!pred.test(&Value::Int(42)));
            assert!(pred.test(&Value::Int(41)));
            assert!(pred.test(&Value::Int(43)));
        }

        #[test]
        fn without_matches_different_types() {
            let pred = p::without([1, 2, 3]);
            // Different types are "not in" the int set
            assert!(pred.test(&Value::String("1".to_string())));
            assert!(pred.test(&Value::Float(1.0)));
            assert!(pred.test(&Value::Bool(true)));
        }

        #[test]
        fn without_with_vec() {
            let blacklist = vec!["spam", "blocked"];
            let pred = p::without(blacklist);
            assert!(pred.test(&Value::String("hello".to_string())));
            assert!(!pred.test(&Value::String("spam".to_string())));
            assert!(!pred.test(&Value::String("blocked".to_string())));
        }

        #[test]
        fn without_is_clonable() {
            let pred = p::without([1, 2, 3]);
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::Int(4)));
            assert!(!cloned.test(&Value::Int(2)));
        }

        #[test]
        fn without_with_negative_numbers() {
            let pred = p::without([-1, 0, 1]);
            assert!(!pred.test(&Value::Int(-1)));
            assert!(!pred.test(&Value::Int(0)));
            assert!(!pred.test(&Value::Int(1)));
            assert!(pred.test(&Value::Int(-2)));
            assert!(pred.test(&Value::Int(2)));
        }

        // -------------------------------------------------------------------
        // Combined/edge case tests
        // -------------------------------------------------------------------

        #[test]
        fn collection_predicates_implement_predicate_trait() {
            // Verify both can be used as Box<dyn Predicate>
            let predicates: Vec<Box<dyn Predicate>> = vec![
                Box::new(p::within([1, 2, 3])),
                Box::new(p::without([1, 2, 3])),
            ];

            assert_eq!(predicates.len(), 2);
            for pred in &predicates {
                let _ = pred.test(&Value::Int(2));
            }
        }

        #[test]
        fn collection_predicates_are_send_sync() {
            fn assert_send_sync<T: Send + Sync>() {}
            assert_send_sync::<p::Within>();
            assert_send_sync::<p::Without>();
        }

        #[test]
        fn within_and_without_are_complementary() {
            // For the same set, within and without should be complementary
            let set = [1, 2, 3];
            let within_pred = p::within(set.clone());
            let without_pred = p::without(set);

            // For any value, exactly one should match
            let test_values = [
                Value::Int(1),
                Value::Int(2),
                Value::Int(3),
                Value::Int(4),
                Value::Int(0),
            ];

            for value in &test_values {
                assert_ne!(
                    within_pred.test(value),
                    without_pred.test(value),
                    "within and without should be complementary for {:?}",
                    value
                );
            }
        }

        #[test]
        fn within_with_duplicates() {
            // Duplicates in the set should not affect behavior
            let pred = p::within([1, 1, 2, 2, 3]);
            assert!(pred.test(&Value::Int(1)));
            assert!(pred.test(&Value::Int(2)));
            assert!(pred.test(&Value::Int(3)));
            assert!(!pred.test(&Value::Int(4)));
        }

        #[test]
        fn within_null_value() {
            // Null should not match an int set
            let pred = p::within([1, 2, 3]);
            assert!(!pred.test(&Value::Null));
        }

        #[test]
        fn without_null_value() {
            // Null should match "not in" an int set
            let pred = p::without([1, 2, 3]);
            assert!(pred.test(&Value::Null));
        }

        #[test]
        fn large_set_performance() {
            // Test with a larger set to ensure no issues
            let large_set: Vec<i64> = (0..1000).collect();
            let pred = p::within(large_set);

            assert!(pred.test(&Value::Int(0)));
            assert!(pred.test(&Value::Int(500)));
            assert!(pred.test(&Value::Int(999)));
            assert!(!pred.test(&Value::Int(1000)));
            assert!(!pred.test(&Value::Int(-1)));
        }
    }
}
