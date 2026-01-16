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
//! use intersteller::traversal::p;
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
/// use intersteller::traversal::predicate::{Predicate, p};
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
/// use intersteller::traversal::p;
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
    // Internal comparison helper functions
    // -------------------------------------------------------------------------
    // These are used by range predicates to avoid cloning on every test() call.
    // They perform the same comparison logic as Lt, Gt, Lte, Gte predicates.

    /// Less-than comparison without allocating a predicate.
    #[inline]
    fn lt_cmp(value: &Value, bound: &Value) -> bool {
        match (value, bound) {
            (Value::Int(a), Value::Int(b)) => a < b,
            (Value::Float(a), Value::Float(b)) => a < b,
            (Value::Int(a), Value::Float(b)) => (*a as f64) < *b,
            (Value::Float(a), Value::Int(b)) => *a < (*b as f64),
            (Value::String(a), Value::String(b)) => a < b,
            _ => false,
        }
    }

    /// Greater-than comparison without allocating a predicate.
    #[inline]
    fn gt_cmp(value: &Value, bound: &Value) -> bool {
        match (value, bound) {
            (Value::Int(a), Value::Int(b)) => a > b,
            (Value::Float(a), Value::Float(b)) => a > b,
            (Value::Int(a), Value::Float(b)) => (*a as f64) > *b,
            (Value::Float(a), Value::Int(b)) => *a > (*b as f64),
            (Value::String(a), Value::String(b)) => a > b,
            _ => false,
        }
    }

    /// Less-than-or-equal comparison without allocating a predicate.
    #[inline]
    fn lte_cmp(value: &Value, bound: &Value) -> bool {
        match (value, bound) {
            (Value::Int(a), Value::Int(b)) => a <= b,
            (Value::Float(a), Value::Float(b)) => a <= b,
            (Value::Int(a), Value::Float(b)) => (*a as f64) <= *b,
            (Value::Float(a), Value::Int(b)) => *a <= (*b as f64),
            (Value::String(a), Value::String(b)) => a <= b,
            _ => false,
        }
    }

    /// Greater-than-or-equal comparison without allocating a predicate.
    #[inline]
    fn gte_cmp(value: &Value, bound: &Value) -> bool {
        match (value, bound) {
            (Value::Int(a), Value::Int(b)) => a >= b,
            (Value::Float(a), Value::Float(b)) => a >= b,
            (Value::Int(a), Value::Float(b)) => (*a as f64) >= *b,
            (Value::Float(a), Value::Int(b)) => *a >= (*b as f64),
            (Value::String(a), Value::String(b)) => a >= b,
            _ => false,
        }
    }

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
    /// use intersteller::traversal::p;
    ///
    /// let pred = p::eq(42);
    /// assert!(pred.test(&Value::Int(42)));
    /// assert!(!pred.test(&Value::Int(41)));
    /// ```
    #[derive(Clone)]
    pub struct Eq(Value);

    impl Predicate for Eq {
        #[inline]
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
    /// use intersteller::traversal::p;
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
    /// use intersteller::traversal::p;
    ///
    /// let pred = p::neq(42);
    /// assert!(!pred.test(&Value::Int(42)));
    /// assert!(pred.test(&Value::Int(41)));
    /// ```
    #[derive(Clone)]
    pub struct Neq(Value);

    impl Predicate for Neq {
        #[inline]
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
    /// use intersteller::traversal::p;
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
    /// use intersteller::traversal::p;
    ///
    /// let pred = p::lt(50);
    /// assert!(pred.test(&Value::Int(30)));
    /// assert!(!pred.test(&Value::Int(50)));
    /// assert!(pred.test(&Value::Float(30.0))); // Cross-type comparison
    /// ```
    #[derive(Clone)]
    pub struct Lt(Value);

    impl Predicate for Lt {
        #[inline]
        fn test(&self, value: &Value) -> bool {
            lt_cmp(value, &self.0)
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
    /// use intersteller::traversal::p;
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
    /// use intersteller::traversal::p;
    ///
    /// let pred = p::lte(50);
    /// assert!(pred.test(&Value::Int(30)));
    /// assert!(pred.test(&Value::Int(50))); // Equal case
    /// assert!(!pred.test(&Value::Int(51)));
    /// ```
    #[derive(Clone)]
    pub struct Lte(Value);

    impl Predicate for Lte {
        #[inline]
        fn test(&self, value: &Value) -> bool {
            lte_cmp(value, &self.0)
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
    /// use intersteller::traversal::p;
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
    /// use intersteller::traversal::p;
    ///
    /// let pred = p::gt(50);
    /// assert!(pred.test(&Value::Int(60)));
    /// assert!(!pred.test(&Value::Int(50)));
    /// assert!(pred.test(&Value::Float(60.0))); // Cross-type comparison
    /// ```
    #[derive(Clone)]
    pub struct Gt(Value);

    impl Predicate for Gt {
        #[inline]
        fn test(&self, value: &Value) -> bool {
            gt_cmp(value, &self.0)
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
    /// use intersteller::traversal::p;
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
    /// use intersteller::traversal::p;
    ///
    /// let pred = p::gte(50);
    /// assert!(pred.test(&Value::Int(60)));
    /// assert!(pred.test(&Value::Int(50))); // Equal case
    /// assert!(!pred.test(&Value::Int(49)));
    /// ```
    #[derive(Clone)]
    pub struct Gte(Value);

    impl Predicate for Gte {
        #[inline]
        fn test(&self, value: &Value) -> bool {
            gte_cmp(value, &self.0)
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
    /// use intersteller::traversal::p;
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
    /// Comparison is performed directly without intermediate predicate construction.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// let pred = p::between(10, 20);
    /// assert!(pred.test(&Value::Int(10)));  // inclusive start
    /// assert!(pred.test(&Value::Int(15)));  // in range
    /// assert!(!pred.test(&Value::Int(20))); // exclusive end
    /// assert!(!pred.test(&Value::Int(5)));  // below range
    /// ```
    #[derive(Clone)]
    pub struct Between {
        start: Value,
        end: Value,
    }

    impl Predicate for Between {
        #[inline]
        fn test(&self, value: &Value) -> bool {
            // value >= start && value < end
            // Inline the comparison logic to avoid per-test cloning
            gte_cmp(value, &self.start) && lt_cmp(value, &self.end)
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
    /// use intersteller::traversal::p;
    ///
    /// // Filter for ages 18-65 (exclusive of 65)
    /// let pred = p::between(18, 65);
    /// ```
    pub fn between<T: Into<Value>>(start: T, end: T) -> Between {
        Between {
            start: start.into(),
            end: end.into(),
        }
    }

    /// Inside predicate (exclusive both ends).
    ///
    /// Tests if the value is strictly inside the range (start, end).
    /// Both endpoints are excluded.
    /// Comparison is performed directly without intermediate predicate construction.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// let pred = p::inside(10, 20);
    /// assert!(!pred.test(&Value::Int(10))); // exclusive start
    /// assert!(pred.test(&Value::Int(15)));  // in range
    /// assert!(!pred.test(&Value::Int(20))); // exclusive end
    /// ```
    #[derive(Clone)]
    pub struct Inside {
        start: Value,
        end: Value,
    }

    impl Predicate for Inside {
        #[inline]
        fn test(&self, value: &Value) -> bool {
            // value > start && value < end
            // Inline the comparison logic to avoid per-test cloning
            gt_cmp(value, &self.start) && lt_cmp(value, &self.end)
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
    /// use intersteller::traversal::p;
    ///
    /// // Filter for values strictly between 0 and 100
    /// let pred = p::inside(0, 100);
    /// ```
    pub fn inside<T: Into<Value>>(start: T, end: T) -> Inside {
        Inside {
            start: start.into(),
            end: end.into(),
        }
    }

    /// Outside predicate.
    ///
    /// Tests if the value is outside the range [start, end].
    /// Returns true if value < start OR value > end.
    /// Comparison is performed directly without intermediate predicate construction.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// let pred = p::outside(10, 20);
    /// assert!(pred.test(&Value::Int(5)));   // below range
    /// assert!(!pred.test(&Value::Int(10))); // at start (inside)
    /// assert!(!pred.test(&Value::Int(15))); // in range
    /// assert!(!pred.test(&Value::Int(20))); // at end (inside)
    /// assert!(pred.test(&Value::Int(25)));  // above range
    /// ```
    #[derive(Clone)]
    pub struct Outside {
        start: Value,
        end: Value,
    }

    impl Predicate for Outside {
        #[inline]
        fn test(&self, value: &Value) -> bool {
            // value < start || value > end
            // Inline the comparison logic to avoid per-test cloning
            lt_cmp(value, &self.start) || gt_cmp(value, &self.end)
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
    /// use intersteller::traversal::p;
    ///
    /// // Filter for values outside the normal range
    /// let pred = p::outside(0, 100);
    /// ```
    pub fn outside<T: Into<Value>>(start: T, end: T) -> Outside {
        Outside {
            start: start.into(),
            end: end.into(),
        }
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
    /// use intersteller::traversal::p;
    ///
    /// let pred = p::within([1, 2, 3]);
    /// assert!(pred.test(&Value::Int(2)));
    /// assert!(!pred.test(&Value::Int(4)));
    /// ```
    #[derive(Clone)]
    pub struct Within(Vec<Value>);

    impl Predicate for Within {
        #[inline]
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
    /// use intersteller::traversal::p;
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
    /// use intersteller::traversal::p;
    ///
    /// let pred = p::without([1, 2, 3]);
    /// assert!(pred.test(&Value::Int(4)));
    /// assert!(!pred.test(&Value::Int(2)));
    /// ```
    #[derive(Clone)]
    pub struct Without(Vec<Value>);

    impl Predicate for Without {
        #[inline]
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
    /// use intersteller::traversal::p;
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

    // -------------------------------------------------------------------------
    // String Predicates (Phase 1.5)
    // -------------------------------------------------------------------------

    /// String contains substring predicate.
    ///
    /// Tests if the value is a string that contains the given substring.
    /// Returns false for non-string values.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// let pred = p::containing("foo");
    /// assert!(pred.test(&Value::String("foobar".to_string())));
    /// assert!(pred.test(&Value::String("barfoo".to_string())));
    /// assert!(!pred.test(&Value::String("bar".to_string())));
    /// assert!(!pred.test(&Value::Int(42))); // Non-string returns false
    /// ```
    #[derive(Clone)]
    pub struct Containing(String);

    impl Predicate for Containing {
        #[inline]
        fn test(&self, value: &Value) -> bool {
            match value {
                Value::String(s) => s.contains(&self.0),
                _ => false,
            }
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create a string contains predicate.
    ///
    /// Tests if the value is a string containing the given substring.
    /// Non-string values always return false.
    ///
    /// # Arguments
    ///
    /// * `substring` - The substring to search for
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// // Filter for names containing "son"
    /// g.v().has_label("person")
    ///     .has_where("name", p::containing("son"))
    ///     .to_list();
    /// ```
    pub fn containing(substring: impl Into<String>) -> Containing {
        Containing(substring.into())
    }

    /// String starts with prefix predicate.
    ///
    /// Tests if the value is a string that starts with the given prefix.
    /// Returns false for non-string values.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// let pred = p::starting_with("foo");
    /// assert!(pred.test(&Value::String("foobar".to_string())));
    /// assert!(!pred.test(&Value::String("barfoo".to_string())));
    /// assert!(!pred.test(&Value::Int(42))); // Non-string returns false
    /// ```
    #[derive(Clone)]
    pub struct StartingWith(String);

    impl Predicate for StartingWith {
        #[inline]
        fn test(&self, value: &Value) -> bool {
            match value {
                Value::String(s) => s.starts_with(&self.0),
                _ => false,
            }
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create a string starts-with predicate.
    ///
    /// Tests if the value is a string starting with the given prefix.
    /// Non-string values always return false.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix to check for
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// // Filter for names starting with "A"
    /// g.v().has_label("person")
    ///     .has_where("name", p::starting_with("A"))
    ///     .to_list();
    /// ```
    pub fn starting_with(prefix: impl Into<String>) -> StartingWith {
        StartingWith(prefix.into())
    }

    /// String ends with suffix predicate.
    ///
    /// Tests if the value is a string that ends with the given suffix.
    /// Returns false for non-string values.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// let pred = p::ending_with("bar");
    /// assert!(pred.test(&Value::String("foobar".to_string())));
    /// assert!(!pred.test(&Value::String("barfoo".to_string())));
    /// assert!(!pred.test(&Value::Int(42))); // Non-string returns false
    /// ```
    #[derive(Clone)]
    pub struct EndingWith(String);

    impl Predicate for EndingWith {
        #[inline]
        fn test(&self, value: &Value) -> bool {
            match value {
                Value::String(s) => s.ends_with(&self.0),
                _ => false,
            }
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create a string ends-with predicate.
    ///
    /// Tests if the value is a string ending with the given suffix.
    /// Non-string values always return false.
    ///
    /// # Arguments
    ///
    /// * `suffix` - The suffix to check for
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// // Filter for email addresses ending with "@company.com"
    /// g.v().has_label("person")
    ///     .has_where("email", p::ending_with("@company.com"))
    ///     .to_list();
    /// ```
    pub fn ending_with(suffix: impl Into<String>) -> EndingWith {
        EndingWith(suffix.into())
    }

    /// String does NOT contain substring predicate.
    ///
    /// Tests if the value is a string that does NOT contain the given substring.
    /// Returns false for non-string values.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// let pred = p::not_containing("spam");
    /// assert!(pred.test(&Value::String("hello world".to_string())));
    /// assert!(!pred.test(&Value::String("this is spam".to_string())));
    /// assert!(!pred.test(&Value::Int(42))); // Non-string returns false
    /// ```
    #[derive(Clone)]
    pub struct NotContaining(String);

    impl Predicate for NotContaining {
        #[inline]
        fn test(&self, value: &Value) -> bool {
            match value {
                Value::String(s) => !s.contains(&self.0),
                _ => false,
            }
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create a string NOT contains predicate.
    ///
    /// Tests if the value is a string that does NOT contain the given substring.
    /// Non-string values always return false.
    ///
    /// # Arguments
    ///
    /// * `substring` - The substring to check for absence
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// // Filter for messages without spam keywords
    /// g.v().has_label("message")
    ///     .has_where("content", p::not_containing("spam"))
    ///     .to_list();
    /// ```
    pub fn not_containing(substring: impl Into<String>) -> NotContaining {
        NotContaining(substring.into())
    }

    /// String does NOT start with prefix predicate.
    ///
    /// Tests if the value is a string that does NOT start with the given prefix.
    /// Returns false for non-string values.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// let pred = p::not_starting_with("test_");
    /// assert!(pred.test(&Value::String("production_data".to_string())));
    /// assert!(!pred.test(&Value::String("test_data".to_string())));
    /// assert!(!pred.test(&Value::Int(42))); // Non-string returns false
    /// ```
    #[derive(Clone)]
    pub struct NotStartingWith(String);

    impl Predicate for NotStartingWith {
        #[inline]
        fn test(&self, value: &Value) -> bool {
            match value {
                Value::String(s) => !s.starts_with(&self.0),
                _ => false,
            }
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create a string NOT starts-with predicate.
    ///
    /// Tests if the value is a string that does NOT start with the given prefix.
    /// Non-string values always return false.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix to check for absence
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// // Filter for non-test items
    /// g.v().has_label("item")
    ///     .has_where("name", p::not_starting_with("test_"))
    ///     .to_list();
    /// ```
    pub fn not_starting_with(prefix: impl Into<String>) -> NotStartingWith {
        NotStartingWith(prefix.into())
    }

    /// String does NOT end with suffix predicate.
    ///
    /// Tests if the value is a string that does NOT end with the given suffix.
    /// Returns false for non-string values.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// let pred = p::not_ending_with(".tmp");
    /// assert!(pred.test(&Value::String("document.pdf".to_string())));
    /// assert!(!pred.test(&Value::String("cache.tmp".to_string())));
    /// assert!(!pred.test(&Value::Int(42))); // Non-string returns false
    /// ```
    #[derive(Clone)]
    pub struct NotEndingWith(String);

    impl Predicate for NotEndingWith {
        #[inline]
        fn test(&self, value: &Value) -> bool {
            match value {
                Value::String(s) => !s.ends_with(&self.0),
                _ => false,
            }
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create a string NOT ends-with predicate.
    ///
    /// Tests if the value is a string that does NOT end with the given suffix.
    /// Non-string values always return false.
    ///
    /// # Arguments
    ///
    /// * `suffix` - The suffix to check for absence
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// // Filter for non-temporary files
    /// g.v().has_label("file")
    ///     .has_where("name", p::not_ending_with(".tmp"))
    ///     .to_list();
    /// ```
    pub fn not_ending_with(suffix: impl Into<String>) -> NotEndingWith {
        NotEndingWith(suffix.into())
    }

    // -------------------------------------------------------------------------
    // Regex Predicate (Phase 1.6)
    // -------------------------------------------------------------------------

    /// Regex pattern matching predicate.
    ///
    /// Tests if the value is a string that matches the given regular expression.
    /// Returns false for non-string values.
    ///
    /// The regex is compiled once at construction time and reused for all tests.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// let pred = p::regex(r"^\d{3}-\d{4}$");
    /// assert!(pred.test(&Value::String("123-4567".to_string())));
    /// assert!(!pred.test(&Value::String("12-3456".to_string())));
    /// assert!(!pred.test(&Value::Int(42))); // Non-string returns false
    /// ```
    #[derive(Clone)]
    pub struct Regex(regex::Regex);

    impl Predicate for Regex {
        #[inline]
        fn test(&self, value: &Value) -> bool {
            match value {
                Value::String(s) => self.0.is_match(s),
                _ => false,
            }
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create a regex pattern matching predicate.
    ///
    /// Tests if the value is a string that matches the given regular expression.
    /// Non-string values always return false.
    ///
    /// # Arguments
    ///
    /// * `pattern` - A regular expression pattern string
    ///
    /// # Panics
    ///
    /// Panics if the pattern is not a valid regular expression. Use `try_regex()`
    /// if you need to handle invalid patterns gracefully.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// // Match phone numbers
    /// g.v().has_label("person")
    ///     .has_where("phone", p::regex(r"^\d{3}-\d{3}-\d{4}$"))
    ///     .to_list();
    ///
    /// // Match email addresses (simplified)
    /// g.v().has_label("person")
    ///     .has_where("email", p::regex(r"^[\w.]+@[\w.]+\.\w+$"))
    ///     .to_list();
    /// ```
    pub fn regex(pattern: &str) -> Regex {
        Regex(regex::Regex::new(pattern).expect("invalid regex pattern"))
    }

    /// Try to create a regex pattern matching predicate.
    ///
    /// Tests if the value is a string that matches the given regular expression.
    /// Non-string values always return false.
    ///
    /// Unlike `regex()`, this function returns `None` instead of panicking
    /// if the pattern is invalid.
    ///
    /// # Arguments
    ///
    /// * `pattern` - A regular expression pattern string
    ///
    /// # Returns
    ///
    /// * `Some(Regex)` if the pattern is valid
    /// * `None` if the pattern is invalid
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// // Valid pattern
    /// let pred = p::try_regex(r"^\d+$");
    /// assert!(pred.is_some());
    ///
    /// // Invalid pattern (unmatched bracket)
    /// let pred = p::try_regex(r"[invalid");
    /// assert!(pred.is_none());
    /// ```
    pub fn try_regex(pattern: &str) -> Option<Regex> {
        regex::Regex::new(pattern).ok().map(Regex)
    }

    // -------------------------------------------------------------------------
    // Logical Composition Predicates (Phase 1.7)
    // -------------------------------------------------------------------------

    /// Logical AND predicate.
    ///
    /// Tests if the value satisfies BOTH inner predicates.
    /// Short-circuits: if the first predicate returns false, the second is not evaluated.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// // Match ages between 18 and 65
    /// let pred = p::and(p::gte(18), p::lt(65));
    /// assert!(pred.test(&Value::Int(30)));  // 30 >= 18 && 30 < 65
    /// assert!(!pred.test(&Value::Int(10))); // 10 >= 18 is false
    /// assert!(!pred.test(&Value::Int(70))); // 70 < 65 is false
    /// ```
    #[derive(Clone)]
    pub struct And<P1, P2>(P1, P2);

    impl<P1, P2> Predicate for And<P1, P2>
    where
        P1: Predicate + Clone + 'static,
        P2: Predicate + Clone + 'static,
    {
        #[inline]
        fn test(&self, value: &Value) -> bool {
            self.0.test(value) && self.1.test(value)
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create a logical AND predicate.
    ///
    /// Returns a predicate that tests if the value satisfies BOTH predicates.
    ///
    /// # Arguments
    ///
    /// * `p1` - The first predicate
    /// * `p2` - The second predicate
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// // Filter for working-age adults
    /// g.v().has_label("person")
    ///     .has_where("age", p::and(p::gte(18), p::lt(65)))
    ///     .to_list();
    ///
    /// // Combine string predicates
    /// g.v().has_where("email", p::and(
    ///     p::containing("@"),
    ///     p::ending_with(".com")
    /// )).to_list();
    /// ```
    pub fn and<P1, P2>(p1: P1, p2: P2) -> And<P1, P2>
    where
        P1: Predicate + Clone + 'static,
        P2: Predicate + Clone + 'static,
    {
        And(p1, p2)
    }

    /// Logical OR predicate.
    ///
    /// Tests if the value satisfies EITHER inner predicate.
    /// Short-circuits: if the first predicate returns true, the second is not evaluated.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// // Match either "active" or "pending" status
    /// let pred = p::or(p::eq("active"), p::eq("pending"));
    /// assert!(pred.test(&Value::String("active".to_string())));
    /// assert!(pred.test(&Value::String("pending".to_string())));
    /// assert!(!pred.test(&Value::String("inactive".to_string())));
    /// ```
    #[derive(Clone)]
    pub struct Or<P1, P2>(P1, P2);

    impl<P1, P2> Predicate for Or<P1, P2>
    where
        P1: Predicate + Clone + 'static,
        P2: Predicate + Clone + 'static,
    {
        #[inline]
        fn test(&self, value: &Value) -> bool {
            self.0.test(value) || self.1.test(value)
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create a logical OR predicate.
    ///
    /// Returns a predicate that tests if the value satisfies EITHER predicate.
    ///
    /// # Arguments
    ///
    /// * `p1` - The first predicate
    /// * `p2` - The second predicate
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// // Filter for specific statuses
    /// g.v().has_label("task")
    ///     .has_where("status", p::or(p::eq("open"), p::eq("in_progress")))
    ///     .to_list();
    ///
    /// // Names starting with A or B
    /// g.v().has_where("name", p::or(
    ///     p::starting_with("A"),
    ///     p::starting_with("B")
    /// )).to_list();
    /// ```
    pub fn or<P1, P2>(p1: P1, p2: P2) -> Or<P1, P2>
    where
        P1: Predicate + Clone + 'static,
        P2: Predicate + Clone + 'static,
    {
        Or(p1, p2)
    }

    /// Logical NOT predicate.
    ///
    /// Tests if the value does NOT satisfy the inner predicate.
    /// Returns the boolean negation of the inner predicate's result.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// // Match any value that is NOT 42
    /// let pred = p::not(p::eq(42));
    /// assert!(!pred.test(&Value::Int(42)));
    /// assert!(pred.test(&Value::Int(41)));
    /// assert!(pred.test(&Value::Int(43)));
    /// ```
    #[derive(Clone)]
    pub struct Not<P>(P);

    impl<P> Predicate for Not<P>
    where
        P: Predicate + Clone + 'static,
    {
        #[inline]
        fn test(&self, value: &Value) -> bool {
            !self.0.test(value)
        }

        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }

    /// Create a logical NOT predicate.
    ///
    /// Returns a predicate that tests if the value does NOT satisfy the inner predicate.
    ///
    /// # Arguments
    ///
    /// * `p` - The predicate to negate
    ///
    /// # Example
    ///
    /// ```ignore
    /// use intersteller::traversal::p;
    ///
    /// // Filter for non-admin users
    /// g.v().has_label("user")
    ///     .has_where("role", p::not(p::eq("admin")))
    ///     .to_list();
    ///
    /// // Values outside a range
    /// g.v().has_where("score", p::not(p::between(0, 100)))
    ///     .to_list();
    /// ```
    pub fn not<P>(p: P) -> Not<P>
    where
        P: Predicate + Clone + 'static,
    {
        Not(p)
    }
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
            let pred = p::eq(3.15f64);
            assert!(pred.test(&Value::Float(3.15)));
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
            let pred = p::within([1.0f64, 2.5f64, 3.15f64]);
            assert!(pred.test(&Value::Float(1.0)));
            assert!(pred.test(&Value::Float(2.5)));
            assert!(pred.test(&Value::Float(3.15)));
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
            let within_pred = p::within(set);
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

    // -------------------------------------------------------------------------
    // Phase 1.5: String Predicates Tests
    // -------------------------------------------------------------------------

    mod string_predicates {
        use super::*;

        // -------------------------------------------------------------------
        // containing() tests
        // -------------------------------------------------------------------

        #[test]
        fn containing_matches_substring() {
            let pred = p::containing("foo");
            assert!(pred.test(&Value::String("foobar".to_string())));
            assert!(pred.test(&Value::String("barfoo".to_string())));
            assert!(pred.test(&Value::String("barfoobar".to_string())));
            assert!(pred.test(&Value::String("foo".to_string())));
        }

        #[test]
        fn containing_does_not_match_missing_substring() {
            let pred = p::containing("foo");
            assert!(!pred.test(&Value::String("bar".to_string())));
            assert!(!pred.test(&Value::String("baz".to_string())));
            assert!(!pred.test(&Value::String("".to_string())));
        }

        #[test]
        fn containing_is_case_sensitive() {
            let pred = p::containing("foo");
            assert!(!pred.test(&Value::String("FOO".to_string())));
            assert!(!pred.test(&Value::String("Foo".to_string())));
            assert!(!pred.test(&Value::String("FOOBAR".to_string())));
        }

        #[test]
        fn containing_empty_substring_matches_all_strings() {
            let pred = p::containing("");
            assert!(pred.test(&Value::String("anything".to_string())));
            assert!(pred.test(&Value::String("".to_string())));
            assert!(pred.test(&Value::String("x".to_string())));
        }

        #[test]
        fn containing_returns_false_for_non_string() {
            let pred = p::containing("foo");
            assert!(!pred.test(&Value::Int(42)));
            assert!(!pred.test(&Value::Float(42.0)));
            assert!(!pred.test(&Value::Bool(true)));
            assert!(!pred.test(&Value::Null));
        }

        #[test]
        fn containing_with_special_characters() {
            let pred = p::containing("@");
            assert!(pred.test(&Value::String("user@example.com".to_string())));
            assert!(!pred.test(&Value::String("user.example.com".to_string())));

            let pred = p::containing(".");
            assert!(pred.test(&Value::String("file.txt".to_string())));
            assert!(!pred.test(&Value::String("file_txt".to_string())));
        }

        #[test]
        fn containing_is_clonable() {
            let pred = p::containing("foo");
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::String("foobar".to_string())));
            assert!(!cloned.test(&Value::String("bar".to_string())));
        }

        // -------------------------------------------------------------------
        // starting_with() tests
        // -------------------------------------------------------------------

        #[test]
        fn starting_with_matches_prefix() {
            let pred = p::starting_with("foo");
            assert!(pred.test(&Value::String("foobar".to_string())));
            assert!(pred.test(&Value::String("foo".to_string())));
            assert!(pred.test(&Value::String("foo123".to_string())));
        }

        #[test]
        fn starting_with_does_not_match_non_prefix() {
            let pred = p::starting_with("foo");
            assert!(!pred.test(&Value::String("barfoo".to_string())));
            assert!(!pred.test(&Value::String("bar".to_string())));
            assert!(!pred.test(&Value::String("afoo".to_string())));
        }

        #[test]
        fn starting_with_is_case_sensitive() {
            let pred = p::starting_with("foo");
            assert!(!pred.test(&Value::String("FOObar".to_string())));
            assert!(!pred.test(&Value::String("Foobar".to_string())));
        }

        #[test]
        fn starting_with_empty_prefix_matches_all_strings() {
            let pred = p::starting_with("");
            assert!(pred.test(&Value::String("anything".to_string())));
            assert!(pred.test(&Value::String("".to_string())));
            assert!(pred.test(&Value::String("x".to_string())));
        }

        #[test]
        fn starting_with_returns_false_for_non_string() {
            let pred = p::starting_with("foo");
            assert!(!pred.test(&Value::Int(42)));
            assert!(!pred.test(&Value::Float(42.0)));
            assert!(!pred.test(&Value::Bool(true)));
            assert!(!pred.test(&Value::Null));
        }

        #[test]
        fn starting_with_exact_match() {
            let pred = p::starting_with("exact");
            assert!(pred.test(&Value::String("exact".to_string())));
        }

        #[test]
        fn starting_with_longer_prefix_than_string() {
            let pred = p::starting_with("foobar");
            assert!(!pred.test(&Value::String("foo".to_string())));
        }

        #[test]
        fn starting_with_is_clonable() {
            let pred = p::starting_with("foo");
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::String("foobar".to_string())));
            assert!(!cloned.test(&Value::String("barfoo".to_string())));
        }

        // -------------------------------------------------------------------
        // ending_with() tests
        // -------------------------------------------------------------------

        #[test]
        fn ending_with_matches_suffix() {
            let pred = p::ending_with("bar");
            assert!(pred.test(&Value::String("foobar".to_string())));
            assert!(pred.test(&Value::String("bar".to_string())));
            assert!(pred.test(&Value::String("123bar".to_string())));
        }

        #[test]
        fn ending_with_does_not_match_non_suffix() {
            let pred = p::ending_with("bar");
            assert!(!pred.test(&Value::String("barfoo".to_string())));
            assert!(!pred.test(&Value::String("foo".to_string())));
            assert!(!pred.test(&Value::String("bara".to_string())));
        }

        #[test]
        fn ending_with_is_case_sensitive() {
            let pred = p::ending_with("bar");
            assert!(!pred.test(&Value::String("fooBAR".to_string())));
            assert!(!pred.test(&Value::String("fooBar".to_string())));
        }

        #[test]
        fn ending_with_empty_suffix_matches_all_strings() {
            let pred = p::ending_with("");
            assert!(pred.test(&Value::String("anything".to_string())));
            assert!(pred.test(&Value::String("".to_string())));
            assert!(pred.test(&Value::String("x".to_string())));
        }

        #[test]
        fn ending_with_returns_false_for_non_string() {
            let pred = p::ending_with("bar");
            assert!(!pred.test(&Value::Int(42)));
            assert!(!pred.test(&Value::Float(42.0)));
            assert!(!pred.test(&Value::Bool(true)));
            assert!(!pred.test(&Value::Null));
        }

        #[test]
        fn ending_with_exact_match() {
            let pred = p::ending_with("exact");
            assert!(pred.test(&Value::String("exact".to_string())));
        }

        #[test]
        fn ending_with_longer_suffix_than_string() {
            let pred = p::ending_with("foobar");
            assert!(!pred.test(&Value::String("bar".to_string())));
        }

        #[test]
        fn ending_with_is_clonable() {
            let pred = p::ending_with("bar");
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::String("foobar".to_string())));
            assert!(!cloned.test(&Value::String("barfoo".to_string())));
        }

        // -------------------------------------------------------------------
        // Combined/edge case tests
        // -------------------------------------------------------------------

        #[test]
        fn string_predicates_implement_predicate_trait() {
            // Verify all can be used as Box<dyn Predicate>
            let predicates: Vec<Box<dyn Predicate>> = vec![
                Box::new(p::containing("foo")),
                Box::new(p::starting_with("foo")),
                Box::new(p::ending_with("foo")),
                Box::new(p::not_containing("foo")),
                Box::new(p::not_starting_with("foo")),
                Box::new(p::not_ending_with("foo")),
            ];

            assert_eq!(predicates.len(), 6);
            for pred in &predicates {
                let _ = pred.test(&Value::String("foo".to_string()));
            }
        }

        #[test]
        fn string_predicates_are_send_sync() {
            fn assert_send_sync<T: Send + Sync>() {}
            assert_send_sync::<p::Containing>();
            assert_send_sync::<p::StartingWith>();
            assert_send_sync::<p::EndingWith>();
            assert_send_sync::<p::NotContaining>();
            assert_send_sync::<p::NotStartingWith>();
            assert_send_sync::<p::NotEndingWith>();
        }

        #[test]
        fn string_predicates_with_whitespace() {
            let pred = p::containing(" ");
            assert!(pred.test(&Value::String("hello world".to_string())));
            assert!(!pred.test(&Value::String("helloworld".to_string())));

            let pred = p::starting_with("  ");
            assert!(pred.test(&Value::String("  indented".to_string())));
            assert!(!pred.test(&Value::String(" single".to_string())));

            let pred = p::ending_with("\n");
            assert!(pred.test(&Value::String("line\n".to_string())));
            assert!(!pred.test(&Value::String("line".to_string())));
        }

        #[test]
        fn string_predicates_with_unicode() {
            let pred = p::containing("日本");
            assert!(pred.test(&Value::String("日本語".to_string())));
            assert!(!pred.test(&Value::String("中文".to_string())));

            let pred = p::starting_with("🚀");
            assert!(pred.test(&Value::String("🚀 launch".to_string())));
            assert!(!pred.test(&Value::String("launch 🚀".to_string())));

            let pred = p::ending_with("😊");
            assert!(pred.test(&Value::String("hello 😊".to_string())));
            assert!(!pred.test(&Value::String("😊 hello".to_string())));
        }

        #[test]
        fn string_predicates_relationships() {
            // If a string starts with "foo", it contains "foo"
            let s = Value::String("foobar".to_string());
            assert!(p::starting_with("foo").test(&s));
            assert!(p::containing("foo").test(&s));

            // If a string ends with "bar", it contains "bar"
            assert!(p::ending_with("bar").test(&s));
            assert!(p::containing("bar").test(&s));

            // But not vice versa
            let s = Value::String("xfoox".to_string());
            assert!(p::containing("foo").test(&s));
            assert!(!p::starting_with("foo").test(&s));
            assert!(!p::ending_with("foo").test(&s));
        }

        #[test]
        fn string_predicates_with_empty_string_value() {
            let empty = Value::String("".to_string());

            // Empty string contains empty string
            assert!(p::containing("").test(&empty));
            assert!(p::starting_with("").test(&empty));
            assert!(p::ending_with("").test(&empty));

            // But doesn't contain non-empty strings
            assert!(!p::containing("x").test(&empty));
            assert!(!p::starting_with("x").test(&empty));
            assert!(!p::ending_with("x").test(&empty));
        }
    }

    // -------------------------------------------------------------------------
    // Phase 1.5.1: Negated String Predicates Tests
    // -------------------------------------------------------------------------

    mod negated_string_predicates {
        use super::*;

        // -------------------------------------------------------------------
        // not_containing() tests
        // -------------------------------------------------------------------

        #[test]
        fn not_containing_matches_missing_substring() {
            let pred = p::not_containing("spam");
            assert!(pred.test(&Value::String("hello world".to_string())));
            assert!(pred.test(&Value::String("legitimate message".to_string())));
            assert!(pred.test(&Value::String("".to_string())));
        }

        #[test]
        fn not_containing_does_not_match_present_substring() {
            let pred = p::not_containing("spam");
            assert!(!pred.test(&Value::String("this is spam".to_string())));
            assert!(!pred.test(&Value::String("spam message".to_string())));
            assert!(!pred.test(&Value::String("message spam".to_string())));
            assert!(!pred.test(&Value::String("spam".to_string())));
        }

        #[test]
        fn not_containing_is_case_sensitive() {
            let pred = p::not_containing("foo");
            assert!(pred.test(&Value::String("FOO".to_string())));
            assert!(pred.test(&Value::String("Foo".to_string())));
            assert!(pred.test(&Value::String("FOOBAR".to_string())));
        }

        #[test]
        fn not_containing_empty_substring_matches_nothing() {
            let pred = p::not_containing("");
            // Empty string is contained in every string, so NOT containing it matches nothing
            assert!(!pred.test(&Value::String("anything".to_string())));
            assert!(!pred.test(&Value::String("".to_string())));
        }

        #[test]
        fn not_containing_returns_false_for_non_string() {
            let pred = p::not_containing("foo");
            assert!(!pred.test(&Value::Int(42)));
            assert!(!pred.test(&Value::Float(42.0)));
            assert!(!pred.test(&Value::Bool(true)));
            assert!(!pred.test(&Value::Null));
        }

        #[test]
        fn not_containing_with_special_characters() {
            let pred = p::not_containing("@");
            assert!(pred.test(&Value::String("user.example.com".to_string())));
            assert!(!pred.test(&Value::String("user@example.com".to_string())));
        }

        #[test]
        fn not_containing_is_clonable() {
            let pred = p::not_containing("foo");
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::String("bar".to_string())));
            assert!(!cloned.test(&Value::String("foobar".to_string())));
        }

        // -------------------------------------------------------------------
        // not_starting_with() tests
        // -------------------------------------------------------------------

        #[test]
        fn not_starting_with_matches_non_prefix() {
            let pred = p::not_starting_with("test_");
            assert!(pred.test(&Value::String("production_data".to_string())));
            assert!(pred.test(&Value::String("data_test".to_string())));
            assert!(pred.test(&Value::String("".to_string())));
        }

        #[test]
        fn not_starting_with_does_not_match_prefix() {
            let pred = p::not_starting_with("test_");
            assert!(!pred.test(&Value::String("test_data".to_string())));
            assert!(!pred.test(&Value::String("test_".to_string())));
            assert!(!pred.test(&Value::String("test_foo_bar".to_string())));
        }

        #[test]
        fn not_starting_with_is_case_sensitive() {
            let pred = p::not_starting_with("foo");
            assert!(pred.test(&Value::String("FOObar".to_string())));
            assert!(pred.test(&Value::String("Foobar".to_string())));
        }

        #[test]
        fn not_starting_with_empty_prefix_matches_nothing() {
            let pred = p::not_starting_with("");
            // Every string starts with empty string, so NOT starting with it matches nothing
            assert!(!pred.test(&Value::String("anything".to_string())));
            assert!(!pred.test(&Value::String("".to_string())));
        }

        #[test]
        fn not_starting_with_returns_false_for_non_string() {
            let pred = p::not_starting_with("foo");
            assert!(!pred.test(&Value::Int(42)));
            assert!(!pred.test(&Value::Float(42.0)));
            assert!(!pred.test(&Value::Bool(true)));
            assert!(!pred.test(&Value::Null));
        }

        #[test]
        fn not_starting_with_longer_prefix_than_string() {
            let pred = p::not_starting_with("foobar");
            // "foo" doesn't start with "foobar", so NOT starting with returns true
            assert!(pred.test(&Value::String("foo".to_string())));
        }

        #[test]
        fn not_starting_with_is_clonable() {
            let pred = p::not_starting_with("foo");
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::String("barfoo".to_string())));
            assert!(!cloned.test(&Value::String("foobar".to_string())));
        }

        // -------------------------------------------------------------------
        // not_ending_with() tests
        // -------------------------------------------------------------------

        #[test]
        fn not_ending_with_matches_non_suffix() {
            let pred = p::not_ending_with(".tmp");
            assert!(pred.test(&Value::String("document.pdf".to_string())));
            assert!(pred.test(&Value::String("tmp.file".to_string())));
            assert!(pred.test(&Value::String("".to_string())));
        }

        #[test]
        fn not_ending_with_does_not_match_suffix() {
            let pred = p::not_ending_with(".tmp");
            assert!(!pred.test(&Value::String("cache.tmp".to_string())));
            assert!(!pred.test(&Value::String(".tmp".to_string())));
            assert!(!pred.test(&Value::String("foo.bar.tmp".to_string())));
        }

        #[test]
        fn not_ending_with_is_case_sensitive() {
            let pred = p::not_ending_with("bar");
            assert!(pred.test(&Value::String("fooBAR".to_string())));
            assert!(pred.test(&Value::String("fooBar".to_string())));
        }

        #[test]
        fn not_ending_with_empty_suffix_matches_nothing() {
            let pred = p::not_ending_with("");
            // Every string ends with empty string, so NOT ending with it matches nothing
            assert!(!pred.test(&Value::String("anything".to_string())));
            assert!(!pred.test(&Value::String("".to_string())));
        }

        #[test]
        fn not_ending_with_returns_false_for_non_string() {
            let pred = p::not_ending_with("bar");
            assert!(!pred.test(&Value::Int(42)));
            assert!(!pred.test(&Value::Float(42.0)));
            assert!(!pred.test(&Value::Bool(true)));
            assert!(!pred.test(&Value::Null));
        }

        #[test]
        fn not_ending_with_longer_suffix_than_string() {
            let pred = p::not_ending_with("foobar");
            // "bar" doesn't end with "foobar", so NOT ending with returns true
            assert!(pred.test(&Value::String("bar".to_string())));
        }

        #[test]
        fn not_ending_with_is_clonable() {
            let pred = p::not_ending_with("bar");
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::String("barfoo".to_string())));
            assert!(!cloned.test(&Value::String("foobar".to_string())));
        }

        // -------------------------------------------------------------------
        // Combined/edge case tests
        // -------------------------------------------------------------------

        #[test]
        fn negated_string_predicates_implement_predicate_trait() {
            let predicates: Vec<Box<dyn Predicate>> = vec![
                Box::new(p::not_containing("foo")),
                Box::new(p::not_starting_with("foo")),
                Box::new(p::not_ending_with("foo")),
            ];

            assert_eq!(predicates.len(), 3);
            for pred in &predicates {
                let _ = pred.test(&Value::String("bar".to_string()));
            }
        }

        #[test]
        fn negated_string_predicates_are_send_sync() {
            fn assert_send_sync<T: Send + Sync>() {}
            assert_send_sync::<p::NotContaining>();
            assert_send_sync::<p::NotStartingWith>();
            assert_send_sync::<p::NotEndingWith>();
        }

        #[test]
        fn negated_predicates_are_complementary_to_positive() {
            // For any string value, exactly one of containing/not_containing should match
            let test_values = [
                Value::String("foobar".to_string()),
                Value::String("bar".to_string()),
                Value::String("xfoox".to_string()),
                Value::String("".to_string()),
            ];

            for value in &test_values {
                assert_ne!(
                    p::containing("foo").test(value),
                    p::not_containing("foo").test(value),
                    "containing and not_containing should be complementary for {:?}",
                    value
                );
                assert_ne!(
                    p::starting_with("foo").test(value),
                    p::not_starting_with("foo").test(value),
                    "starting_with and not_starting_with should be complementary for {:?}",
                    value
                );
                assert_ne!(
                    p::ending_with("foo").test(value),
                    p::not_ending_with("foo").test(value),
                    "ending_with and not_ending_with should be complementary for {:?}",
                    value
                );
            }
        }

        #[test]
        fn negated_predicates_with_unicode() {
            let pred = p::not_containing("日本");
            assert!(pred.test(&Value::String("中文".to_string())));
            assert!(!pred.test(&Value::String("日本語".to_string())));

            let pred = p::not_starting_with("🚀");
            assert!(pred.test(&Value::String("launch 🚀".to_string())));
            assert!(!pred.test(&Value::String("🚀 launch".to_string())));

            let pred = p::not_ending_with("😊");
            assert!(pred.test(&Value::String("😊 hello".to_string())));
            assert!(!pred.test(&Value::String("hello 😊".to_string())));
        }

        #[test]
        fn negated_predicates_with_whitespace() {
            let pred = p::not_containing(" ");
            assert!(pred.test(&Value::String("helloworld".to_string())));
            assert!(!pred.test(&Value::String("hello world".to_string())));

            let pred = p::not_starting_with("  ");
            assert!(pred.test(&Value::String(" single".to_string())));
            assert!(!pred.test(&Value::String("  indented".to_string())));

            let pred = p::not_ending_with("\n");
            assert!(pred.test(&Value::String("line".to_string())));
            assert!(!pred.test(&Value::String("line\n".to_string())));
        }

        #[test]
        fn negated_predicates_logical_consistency() {
            // Test that p::not(p::containing("x")) behaves equivalently to p::not_containing("x")
            // for string values (non-string values behave differently due to implementation)
            let test_values = [
                Value::String("hello".to_string()),
                Value::String("xhello".to_string()),
                Value::String("hellox".to_string()),
                Value::String("x".to_string()),
                Value::String("".to_string()),
            ];

            for value in &test_values {
                assert_eq!(
                    p::not(p::containing("x")).test(value),
                    p::not_containing("x").test(value),
                    "not(containing) should equal not_containing for {:?}",
                    value
                );
                assert_eq!(
                    p::not(p::starting_with("x")).test(value),
                    p::not_starting_with("x").test(value),
                    "not(starting_with) should equal not_starting_with for {:?}",
                    value
                );
                assert_eq!(
                    p::not(p::ending_with("x")).test(value),
                    p::not_ending_with("x").test(value),
                    "not(ending_with) should equal not_ending_with for {:?}",
                    value
                );
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 1.6: Regex Predicates Tests
    // -------------------------------------------------------------------------

    mod regex_predicates {
        use super::*;

        // -------------------------------------------------------------------
        // regex() tests
        // -------------------------------------------------------------------

        #[test]
        fn regex_matches_pattern() {
            let pred = p::regex(r"^\d{3}-\d{4}$");
            assert!(pred.test(&Value::String("123-4567".to_string())));
            assert!(pred.test(&Value::String("000-0000".to_string())));
            assert!(pred.test(&Value::String("999-9999".to_string())));
        }

        #[test]
        fn regex_does_not_match_non_matching_pattern() {
            let pred = p::regex(r"^\d{3}-\d{4}$");
            assert!(!pred.test(&Value::String("12-3456".to_string())));
            assert!(!pred.test(&Value::String("1234-567".to_string())));
            assert!(!pred.test(&Value::String("abc-defg".to_string())));
            assert!(!pred.test(&Value::String("123-45678".to_string())));
            assert!(!pred.test(&Value::String("".to_string())));
        }

        #[test]
        fn regex_returns_false_for_non_string() {
            let pred = p::regex(r"\d+");
            assert!(!pred.test(&Value::Int(42)));
            assert!(!pred.test(&Value::Float(42.0)));
            assert!(!pred.test(&Value::Bool(true)));
            assert!(!pred.test(&Value::Null));
        }

        #[test]
        fn regex_simple_patterns() {
            // Match any digits
            let pred = p::regex(r"\d+");
            assert!(pred.test(&Value::String("123".to_string())));
            assert!(pred.test(&Value::String("abc123xyz".to_string())));
            assert!(!pred.test(&Value::String("abc".to_string())));

            // Match word characters
            let pred = p::regex(r"^\w+$");
            assert!(pred.test(&Value::String("hello".to_string())));
            assert!(pred.test(&Value::String("hello123".to_string())));
            assert!(!pred.test(&Value::String("hello world".to_string())));
        }

        #[test]
        fn regex_anchored_patterns() {
            // Start anchor
            let pred = p::regex(r"^foo");
            assert!(pred.test(&Value::String("foobar".to_string())));
            assert!(!pred.test(&Value::String("barfoo".to_string())));

            // End anchor
            let pred = p::regex(r"bar$");
            assert!(pred.test(&Value::String("foobar".to_string())));
            assert!(!pred.test(&Value::String("barfoo".to_string())));

            // Both anchors
            let pred = p::regex(r"^exact$");
            assert!(pred.test(&Value::String("exact".to_string())));
            assert!(!pred.test(&Value::String("exactly".to_string())));
            assert!(!pred.test(&Value::String("not exact".to_string())));
        }

        #[test]
        fn regex_character_classes() {
            // Digit class
            let pred = p::regex(r"[0-9]+");
            assert!(pred.test(&Value::String("42".to_string())));
            assert!(!pred.test(&Value::String("abc".to_string())));

            // Letter class
            let pred = p::regex(r"[a-zA-Z]+");
            assert!(pred.test(&Value::String("Hello".to_string())));
            assert!(!pred.test(&Value::String("123".to_string())));

            // Custom class
            let pred = p::regex(r"[aeiou]");
            assert!(pred.test(&Value::String("hello".to_string())));
            assert!(!pred.test(&Value::String("rhythm".to_string())));
        }

        #[test]
        fn regex_quantifiers() {
            // Zero or more
            let pred = p::regex(r"^a*$");
            assert!(pred.test(&Value::String("".to_string())));
            assert!(pred.test(&Value::String("a".to_string())));
            assert!(pred.test(&Value::String("aaa".to_string())));
            assert!(!pred.test(&Value::String("ab".to_string())));

            // One or more
            let pred = p::regex(r"^a+$");
            assert!(!pred.test(&Value::String("".to_string())));
            assert!(pred.test(&Value::String("a".to_string())));
            assert!(pred.test(&Value::String("aaa".to_string())));

            // Optional
            let pred = p::regex(r"^colou?r$");
            assert!(pred.test(&Value::String("color".to_string())));
            assert!(pred.test(&Value::String("colour".to_string())));
            assert!(!pred.test(&Value::String("colouur".to_string())));

            // Exact count
            let pred = p::regex(r"^a{3}$");
            assert!(pred.test(&Value::String("aaa".to_string())));
            assert!(!pred.test(&Value::String("aa".to_string())));
            assert!(!pred.test(&Value::String("aaaa".to_string())));

            // Range count
            let pred = p::regex(r"^a{2,4}$");
            assert!(!pred.test(&Value::String("a".to_string())));
            assert!(pred.test(&Value::String("aa".to_string())));
            assert!(pred.test(&Value::String("aaa".to_string())));
            assert!(pred.test(&Value::String("aaaa".to_string())));
            assert!(!pred.test(&Value::String("aaaaa".to_string())));
        }

        #[test]
        fn regex_alternation() {
            let pred = p::regex(r"^(cat|dog|bird)$");
            assert!(pred.test(&Value::String("cat".to_string())));
            assert!(pred.test(&Value::String("dog".to_string())));
            assert!(pred.test(&Value::String("bird".to_string())));
            assert!(!pred.test(&Value::String("fish".to_string())));
            assert!(!pred.test(&Value::String("cats".to_string())));
        }

        #[test]
        fn regex_groups() {
            // Capturing group
            let pred = p::regex(r"^(\d{3})-(\d{4})$");
            assert!(pred.test(&Value::String("123-4567".to_string())));

            // Non-capturing group
            let pred = p::regex(r"^(?:Mr|Ms|Mrs)\. \w+$");
            assert!(pred.test(&Value::String("Mr. Smith".to_string())));
            assert!(pred.test(&Value::String("Ms. Jones".to_string())));
            assert!(pred.test(&Value::String("Mrs. Brown".to_string())));
            assert!(!pred.test(&Value::String("Dr. Who".to_string())));
        }

        #[test]
        fn regex_special_characters() {
            // Escaped special characters
            let pred = p::regex(r"\.\*\+\?");
            assert!(pred.test(&Value::String(".*+?".to_string())));
            assert!(!pred.test(&Value::String("abcd".to_string())));

            // Dot matches any
            let pred = p::regex(r"^a.c$");
            assert!(pred.test(&Value::String("abc".to_string())));
            assert!(pred.test(&Value::String("a c".to_string())));
            assert!(pred.test(&Value::String("a1c".to_string())));
            assert!(!pred.test(&Value::String("ac".to_string())));
        }

        #[test]
        fn regex_case_sensitive_by_default() {
            let pred = p::regex(r"^hello$");
            assert!(pred.test(&Value::String("hello".to_string())));
            assert!(!pred.test(&Value::String("Hello".to_string())));
            assert!(!pred.test(&Value::String("HELLO".to_string())));
        }

        #[test]
        fn regex_case_insensitive_flag() {
            let pred = p::regex(r"(?i)^hello$");
            assert!(pred.test(&Value::String("hello".to_string())));
            assert!(pred.test(&Value::String("Hello".to_string())));
            assert!(pred.test(&Value::String("HELLO".to_string())));
            assert!(pred.test(&Value::String("hElLo".to_string())));
        }

        #[test]
        fn regex_email_pattern() {
            // Simplified email pattern
            let pred = p::regex(r"^[\w.+-]+@[\w.-]+\.\w{2,}$");
            assert!(pred.test(&Value::String("user@example.com".to_string())));
            assert!(pred.test(&Value::String("user.name@example.co.uk".to_string())));
            assert!(pred.test(&Value::String("user+tag@example.org".to_string())));
            assert!(!pred.test(&Value::String("invalid".to_string())));
            assert!(!pred.test(&Value::String("@example.com".to_string())));
            assert!(!pred.test(&Value::String("user@".to_string())));
        }

        #[test]
        fn regex_is_clonable() {
            let pred = p::regex(r"^\d+$");
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::String("123".to_string())));
            assert!(!cloned.test(&Value::String("abc".to_string())));
        }

        #[test]
        fn regex_is_send_sync() {
            fn assert_send_sync<T: Send + Sync>() {}
            assert_send_sync::<p::Regex>();
        }

        // -------------------------------------------------------------------
        // try_regex() tests
        // -------------------------------------------------------------------

        #[test]
        fn try_regex_valid_pattern_returns_some() {
            let result = p::try_regex(r"^\d+$");
            assert!(result.is_some());

            let pred = result.unwrap();
            assert!(pred.test(&Value::String("123".to_string())));
        }

        #[test]
        fn try_regex_invalid_pattern_returns_none() {
            // Unmatched bracket
            assert!(p::try_regex(r"[invalid").is_none());

            // Unmatched parenthesis
            assert!(p::try_regex(r"(unclosed").is_none());

            // Invalid escape sequence
            assert!(p::try_regex(r"\").is_none());

            // Invalid repetition
            assert!(p::try_regex(r"a{2,1}").is_none());
        }

        #[test]
        fn try_regex_empty_pattern_is_valid() {
            let result = p::try_regex(r"");
            assert!(result.is_some());

            let pred = result.unwrap();
            // Empty pattern matches any string
            assert!(pred.test(&Value::String("anything".to_string())));
            assert!(pred.test(&Value::String("".to_string())));
        }

        #[test]
        fn try_regex_complex_valid_pattern() {
            let result = p::try_regex(r"^(?:[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})$");
            assert!(result.is_some());
        }

        // -------------------------------------------------------------------
        // regex() panic test
        // -------------------------------------------------------------------

        #[test]
        #[should_panic(expected = "invalid regex pattern")]
        fn regex_panics_on_invalid_pattern() {
            let _ = p::regex(r"[invalid");
        }

        // -------------------------------------------------------------------
        // Edge cases and integration
        // -------------------------------------------------------------------

        #[test]
        fn regex_with_empty_string_value() {
            let pred = p::regex(r"^$");
            assert!(pred.test(&Value::String("".to_string())));
            assert!(!pred.test(&Value::String("x".to_string())));

            let pred = p::regex(r"^.*$");
            assert!(pred.test(&Value::String("".to_string())));
            assert!(pred.test(&Value::String("anything".to_string())));
        }

        #[test]
        fn regex_with_unicode() {
            let pred = p::regex(r"日本");
            assert!(pred.test(&Value::String("日本語".to_string())));
            assert!(!pred.test(&Value::String("中文".to_string())));

            // Unicode property (if supported)
            let pred = p::regex(r"\p{L}+");
            assert!(pred.test(&Value::String("hello".to_string())));
            assert!(pred.test(&Value::String("日本語".to_string())));
        }

        #[test]
        fn regex_with_whitespace_patterns() {
            let pred = p::regex(r"^\s+$");
            assert!(pred.test(&Value::String("   ".to_string())));
            assert!(pred.test(&Value::String("\t\n".to_string())));
            assert!(!pred.test(&Value::String("abc".to_string())));

            let pred = p::regex(r"\S+");
            assert!(pred.test(&Value::String("abc".to_string())));
            assert!(!pred.test(&Value::String("   ".to_string())));
        }

        #[test]
        fn regex_predicate_implements_predicate_trait() {
            let predicates: Vec<Box<dyn Predicate>> = vec![
                Box::new(p::regex(r"^\d+$")),
                Box::new(p::regex(r"^[a-z]+$")),
            ];

            assert_eq!(predicates.len(), 2);
            for pred in &predicates {
                let _ = pred.test(&Value::String("test".to_string()));
            }
        }

        #[test]
        fn regex_multiline_flag() {
            // By default, ^ and $ match start/end of string
            let pred = p::regex(r"^line$");
            assert!(!pred.test(&Value::String("line\nline".to_string())));

            // With multiline flag, ^ and $ match start/end of lines
            let pred = p::regex(r"(?m)^line$");
            assert!(pred.test(&Value::String("line\nline".to_string())));
            assert!(pred.test(&Value::String("line".to_string())));
        }

        #[test]
        fn regex_word_boundaries() {
            let pred = p::regex(r"\bword\b");
            assert!(pred.test(&Value::String("a word here".to_string())));
            assert!(pred.test(&Value::String("word".to_string())));
            assert!(!pred.test(&Value::String("wording".to_string())));
            assert!(!pred.test(&Value::String("sword".to_string())));
        }
    }

    // -------------------------------------------------------------------------
    // Phase 1.7: Logical Composition Predicates Tests
    // -------------------------------------------------------------------------

    mod logical_predicates {
        use super::*;

        // -------------------------------------------------------------------
        // and() tests
        // -------------------------------------------------------------------

        #[test]
        fn and_both_true() {
            let pred = p::and(p::gte(18), p::lt(65));
            assert!(pred.test(&Value::Int(30)));
            assert!(pred.test(&Value::Int(18)));
            assert!(pred.test(&Value::Int(64)));
        }

        #[test]
        fn and_first_false() {
            let pred = p::and(p::gte(18), p::lt(65));
            assert!(!pred.test(&Value::Int(10)));
            assert!(!pred.test(&Value::Int(17)));
            assert!(!pred.test(&Value::Int(0)));
        }

        #[test]
        fn and_second_false() {
            let pred = p::and(p::gte(18), p::lt(65));
            assert!(!pred.test(&Value::Int(65)));
            assert!(!pred.test(&Value::Int(70)));
            assert!(!pred.test(&Value::Int(100)));
        }

        #[test]
        fn and_both_false() {
            // Create a predicate that will be false for both conditions at boundary
            let pred = p::and(p::gt(100), p::lt(50));
            assert!(!pred.test(&Value::Int(75)));
            assert!(!pred.test(&Value::Int(30)));
            assert!(!pred.test(&Value::Int(150)));
        }

        #[test]
        fn and_with_string_predicates() {
            let pred = p::and(p::containing("@"), p::ending_with(".com"));
            assert!(pred.test(&Value::String("user@example.com".to_string())));
            assert!(!pred.test(&Value::String("user@example.org".to_string())));
            assert!(!pred.test(&Value::String("no-at.com".to_string())));
        }

        #[test]
        fn and_with_eq_predicates() {
            // This creates a predicate that can only match one specific value
            let pred = p::and(p::gte(42), p::lte(42));
            assert!(pred.test(&Value::Int(42)));
            assert!(!pred.test(&Value::Int(41)));
            assert!(!pred.test(&Value::Int(43)));
        }

        #[test]
        fn and_with_mixed_types() {
            // Test that incompatible types return false
            let pred = p::and(p::gte(0), p::lt(100));
            assert!(!pred.test(&Value::String("50".to_string())));
            assert!(!pred.test(&Value::Bool(true)));
        }

        #[test]
        fn and_is_clonable() {
            let pred = p::and(p::gte(18), p::lt(65));
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::Int(30)));
            assert!(!cloned.test(&Value::Int(10)));
        }

        #[test]
        fn and_is_send_sync() {
            fn assert_send_sync<T: Send + Sync>() {}
            assert_send_sync::<p::And<p::Gte, p::Lt>>();
        }

        #[test]
        fn and_nested() {
            // (age >= 18 AND age < 65) AND status == "active"
            let age_range = p::and(p::gte(18), p::lt(65));
            // Note: We can't easily combine predicates of different types in nested and
            // But we can verify the age_range predicate works
            assert!(age_range.test(&Value::Int(30)));
            assert!(!age_range.test(&Value::Int(10)));
        }

        #[test]
        fn and_with_floats() {
            let pred = p::and(p::gte(0.0f64), p::lt(1.0f64));
            assert!(pred.test(&Value::Float(0.0)));
            assert!(pred.test(&Value::Float(0.5)));
            assert!(pred.test(&Value::Float(0.999)));
            assert!(!pred.test(&Value::Float(1.0)));
            assert!(!pred.test(&Value::Float(-0.1)));
        }

        // -------------------------------------------------------------------
        // or() tests
        // -------------------------------------------------------------------

        #[test]
        fn or_first_true() {
            let pred = p::or(p::eq("active"), p::eq("pending"));
            assert!(pred.test(&Value::String("active".to_string())));
        }

        #[test]
        fn or_second_true() {
            let pred = p::or(p::eq("active"), p::eq("pending"));
            assert!(pred.test(&Value::String("pending".to_string())));
        }

        #[test]
        fn or_both_false() {
            let pred = p::or(p::eq("active"), p::eq("pending"));
            assert!(!pred.test(&Value::String("inactive".to_string())));
            assert!(!pred.test(&Value::String("deleted".to_string())));
        }

        #[test]
        fn or_both_true() {
            // When testing with a value that matches both
            let pred = p::or(p::gte(0), p::lte(100));
            assert!(pred.test(&Value::Int(50))); // Matches both
        }

        #[test]
        fn or_with_string_predicates() {
            let pred = p::or(p::starting_with("A"), p::starting_with("B"));
            assert!(pred.test(&Value::String("Alice".to_string())));
            assert!(pred.test(&Value::String("Bob".to_string())));
            assert!(!pred.test(&Value::String("Carol".to_string())));
        }

        #[test]
        fn or_with_numeric_ranges() {
            // Outside of [10, 20] - either less than 10 OR greater than 20
            let pred = p::or(p::lt(10), p::gt(20));
            assert!(pred.test(&Value::Int(5)));
            assert!(pred.test(&Value::Int(25)));
            assert!(!pred.test(&Value::Int(15)));
            assert!(!pred.test(&Value::Int(10)));
            assert!(!pred.test(&Value::Int(20)));
        }

        #[test]
        fn or_with_mixed_predicate_types() {
            // Match integers 42 OR strings containing "foo"
            // Note: Since predicates return false for wrong types, this works
            let pred = p::or(p::eq(42), p::containing("foo"));
            assert!(pred.test(&Value::Int(42)));
            assert!(pred.test(&Value::String("foobar".to_string())));
            assert!(!pred.test(&Value::Int(43)));
            assert!(!pred.test(&Value::String("bar".to_string())));
        }

        #[test]
        fn or_is_clonable() {
            let pred = p::or(p::eq("a"), p::eq("b"));
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::String("a".to_string())));
            assert!(cloned.test(&Value::String("b".to_string())));
            assert!(!cloned.test(&Value::String("c".to_string())));
        }

        #[test]
        fn or_is_send_sync() {
            fn assert_send_sync<T: Send + Sync>() {}
            assert_send_sync::<p::Or<p::Eq, p::Eq>>();
        }

        #[test]
        fn or_with_floats() {
            let pred = p::or(p::lt(0.0f64), p::gt(1.0f64));
            assert!(pred.test(&Value::Float(-0.5)));
            assert!(pred.test(&Value::Float(1.5)));
            assert!(!pred.test(&Value::Float(0.5)));
        }

        // -------------------------------------------------------------------
        // not() tests
        // -------------------------------------------------------------------

        #[test]
        fn not_negates_true() {
            let pred = p::not(p::eq(42));
            assert!(!pred.test(&Value::Int(42)));
        }

        #[test]
        fn not_negates_false() {
            let pred = p::not(p::eq(42));
            assert!(pred.test(&Value::Int(41)));
            assert!(pred.test(&Value::Int(43)));
            assert!(pred.test(&Value::Int(0)));
        }

        #[test]
        fn not_with_string_predicate() {
            let pred = p::not(p::containing("spam"));
            assert!(pred.test(&Value::String("hello world".to_string())));
            assert!(!pred.test(&Value::String("this is spam".to_string())));
        }

        #[test]
        fn not_with_range_predicate() {
            // Not between 10 and 20
            let pred = p::not(p::between(10, 20));
            assert!(pred.test(&Value::Int(5)));
            assert!(pred.test(&Value::Int(20)));
            assert!(pred.test(&Value::Int(25)));
            assert!(!pred.test(&Value::Int(10)));
            assert!(!pred.test(&Value::Int(15)));
        }

        #[test]
        fn not_with_within_predicate() {
            let pred = p::not(p::within([1, 2, 3]));
            assert!(pred.test(&Value::Int(0)));
            assert!(pred.test(&Value::Int(4)));
            assert!(!pred.test(&Value::Int(1)));
            assert!(!pred.test(&Value::Int(2)));
            assert!(!pred.test(&Value::Int(3)));
        }

        #[test]
        fn not_double_negation() {
            // not(not(eq(42))) should be equivalent to eq(42)
            let pred = p::not(p::not(p::eq(42)));
            assert!(pred.test(&Value::Int(42)));
            assert!(!pred.test(&Value::Int(41)));
        }

        #[test]
        fn not_is_clonable() {
            let pred = p::not(p::eq(42));
            let boxed: Box<dyn Predicate> = Box::new(pred);
            let cloned = boxed.clone();
            assert!(cloned.test(&Value::Int(41)));
            assert!(!cloned.test(&Value::Int(42)));
        }

        #[test]
        fn not_is_send_sync() {
            fn assert_send_sync<T: Send + Sync>() {}
            assert_send_sync::<p::Not<p::Eq>>();
        }

        #[test]
        fn not_with_different_value_types() {
            let pred = p::not(p::gt(50));
            assert!(pred.test(&Value::Int(50)));
            assert!(pred.test(&Value::Int(30)));
            assert!(!pred.test(&Value::Int(51)));
            // Non-numeric returns false from gt(), so not() returns true
            assert!(pred.test(&Value::String("60".to_string())));
        }

        // -------------------------------------------------------------------
        // Complex compositions
        // -------------------------------------------------------------------

        #[test]
        fn complex_and_or_composition() {
            // (age >= 18 AND age < 25) OR age >= 65
            // Matches: young adults (18-24) OR seniors (65+)
            let young_adult = p::and(p::gte(18), p::lt(25));
            let senior = p::gte(65);
            let pred = p::or(young_adult, senior);

            // Young adults
            assert!(pred.test(&Value::Int(18)));
            assert!(pred.test(&Value::Int(20)));
            assert!(pred.test(&Value::Int(24)));

            // Seniors
            assert!(pred.test(&Value::Int(65)));
            assert!(pred.test(&Value::Int(80)));

            // Neither (middle-aged)
            assert!(!pred.test(&Value::Int(17)));
            assert!(!pred.test(&Value::Int(25)));
            assert!(!pred.test(&Value::Int(40)));
            assert!(!pred.test(&Value::Int(64)));
        }

        #[test]
        fn complex_not_and_composition() {
            // NOT (age >= 18 AND age < 65) - people NOT in working age
            let working_age = p::and(p::gte(18), p::lt(65));
            let pred = p::not(working_age);

            // Minors
            assert!(pred.test(&Value::Int(10)));
            assert!(pred.test(&Value::Int(17)));

            // Seniors
            assert!(pred.test(&Value::Int(65)));
            assert!(pred.test(&Value::Int(80)));

            // Working age - should NOT match
            assert!(!pred.test(&Value::Int(18)));
            assert!(!pred.test(&Value::Int(30)));
            assert!(!pred.test(&Value::Int(64)));
        }

        #[test]
        fn complex_not_or_composition() {
            // NOT (status == "deleted" OR status == "archived")
            // Matches: anything except deleted or archived
            let excluded = p::or(p::eq("deleted"), p::eq("archived"));
            let pred = p::not(excluded);

            assert!(pred.test(&Value::String("active".to_string())));
            assert!(pred.test(&Value::String("pending".to_string())));
            assert!(!pred.test(&Value::String("deleted".to_string())));
            assert!(!pred.test(&Value::String("archived".to_string())));
        }

        #[test]
        fn deeply_nested_composition() {
            // ((a AND b) OR (c AND d)) - complex business rule
            let ab = p::and(p::gte(0), p::lt(10));
            let cd = p::and(p::gte(20), p::lt(30));
            let pred = p::or(ab, cd);

            // In [0, 10)
            assert!(pred.test(&Value::Int(0)));
            assert!(pred.test(&Value::Int(5)));
            assert!(!pred.test(&Value::Int(10)));

            // In [20, 30)
            assert!(pred.test(&Value::Int(20)));
            assert!(pred.test(&Value::Int(25)));
            assert!(!pred.test(&Value::Int(30)));

            // Neither
            assert!(!pred.test(&Value::Int(15)));
            assert!(!pred.test(&Value::Int(-5)));
            assert!(!pred.test(&Value::Int(35)));
        }

        #[test]
        fn logical_predicates_implement_predicate_trait() {
            // Verify all can be used as Box<dyn Predicate>
            let predicates: Vec<Box<dyn Predicate>> = vec![
                Box::new(p::and(p::gte(0), p::lt(100))),
                Box::new(p::or(p::eq("a"), p::eq("b"))),
                Box::new(p::not(p::eq(42))),
            ];

            assert_eq!(predicates.len(), 3);
            for pred in &predicates {
                let _ = pred.test(&Value::Int(50));
            }
        }

        #[test]
        fn logical_predicates_short_circuit_and() {
            // AND should short-circuit on first false
            // We can't easily test short-circuiting directly, but we can verify behavior
            let pred = p::and(p::lt(0), p::gt(100)); // Impossible condition
            assert!(!pred.test(&Value::Int(50)));
        }

        #[test]
        fn logical_predicates_short_circuit_or() {
            // OR should short-circuit on first true
            let pred = p::or(p::gt(0), p::lt(0)); // First succeeds for positive
            assert!(pred.test(&Value::Int(50)));
        }

        #[test]
        fn de_morgans_law_and_to_or() {
            // NOT (A AND B) == (NOT A) OR (NOT B)
            let a = p::gte(10);
            let b = p::lt(20);

            // NOT (A AND B)
            let not_and = p::not(p::and(a.clone(), b.clone()));

            // Testing with values
            // Value 5: A=false, B=true -> NOT(false AND true) = NOT(false) = true
            assert!(not_and.test(&Value::Int(5)));
            // Value 25: A=true, B=false -> NOT(true AND false) = NOT(false) = true
            assert!(not_and.test(&Value::Int(25)));
            // Value 15: A=true, B=true -> NOT(true AND true) = NOT(true) = false
            assert!(!not_and.test(&Value::Int(15)));
        }

        #[test]
        fn de_morgans_law_or_to_and() {
            // NOT (A OR B) == (NOT A) AND (NOT B)
            let a = p::eq(1);
            let b = p::eq(2);

            // NOT (A OR B)
            let not_or = p::not(p::or(a.clone(), b.clone()));

            // Value 1: A=true, B=false -> NOT(true OR false) = NOT(true) = false
            assert!(!not_or.test(&Value::Int(1)));
            // Value 2: A=false, B=true -> NOT(false OR true) = NOT(true) = false
            assert!(!not_or.test(&Value::Int(2)));
            // Value 3: A=false, B=false -> NOT(false OR false) = NOT(false) = true
            assert!(not_or.test(&Value::Int(3)));
        }
    }
}
