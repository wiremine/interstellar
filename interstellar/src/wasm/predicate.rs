//! Predicate system for WASM bindings.
//!
//! Provides the `P` namespace with factory functions for creating predicates.

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsError;

use crate::traversal::predicate;
use crate::traversal::predicate::p as rust_p;
use crate::wasm::types::js_to_value;

/// Internal representation of a predicate that can be used in traversal steps.
///
/// This wraps a boxed Rust predicate for use with wasm-bindgen.
#[wasm_bindgen]
pub struct Predicate {
    pub(crate) inner: Box<dyn predicate::Predicate>,
}

impl Predicate {
    pub(crate) fn new(pred: impl predicate::Predicate + 'static) -> Self {
        Self {
            inner: Box::new(pred),
        }
    }

    pub(crate) fn into_inner(self) -> Box<dyn predicate::Predicate> {
        self.inner
    }
}

impl Clone for Predicate {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone_box(),
        }
    }
}

/// Predicate factory functions.
///
/// @example
/// ```typescript
/// graph.V()
///     .hasWhere('age', P.gte(18n))
///     .hasWhere('name', P.startingWith('A'))
///     .toList();
/// ```
#[wasm_bindgen]
pub struct P;

#[wasm_bindgen]
impl P {
    // =========================================================================
    // Comparison Predicates
    // =========================================================================

    /// Equals comparison.
    ///
    /// @param value - Value to compare against
    pub fn eq(value: JsValue) -> Result<Predicate, JsError> {
        let v = js_to_value(value)?;
        Ok(Predicate::new(rust_p::eq(v)))
    }

    /// Not equals comparison.
    ///
    /// @param value - Value to compare against
    pub fn neq(value: JsValue) -> Result<Predicate, JsError> {
        let v = js_to_value(value)?;
        Ok(Predicate::new(rust_p::neq(v)))
    }

    /// Less than comparison.
    ///
    /// @param value - Value to compare against
    pub fn lt(value: JsValue) -> Result<Predicate, JsError> {
        let v = js_to_value(value)?;
        Ok(Predicate::new(rust_p::lt(v)))
    }

    /// Less than or equal comparison.
    ///
    /// @param value - Value to compare against
    pub fn lte(value: JsValue) -> Result<Predicate, JsError> {
        let v = js_to_value(value)?;
        Ok(Predicate::new(rust_p::lte(v)))
    }

    /// Greater than comparison.
    ///
    /// @param value - Value to compare against
    pub fn gt(value: JsValue) -> Result<Predicate, JsError> {
        let v = js_to_value(value)?;
        Ok(Predicate::new(rust_p::gt(v)))
    }

    /// Greater than or equal comparison.
    ///
    /// @param value - Value to compare against
    pub fn gte(value: JsValue) -> Result<Predicate, JsError> {
        let v = js_to_value(value)?;
        Ok(Predicate::new(rust_p::gte(v)))
    }

    // =========================================================================
    // Range Predicates
    // =========================================================================

    /// Value is between start and end (inclusive start, exclusive end).
    ///
    /// @param start - Range start
    /// @param end - Range end
    pub fn between(start: JsValue, end: JsValue) -> Result<Predicate, JsError> {
        let s = js_to_value(start)?;
        let e = js_to_value(end)?;
        Ok(Predicate::new(rust_p::between(s, e)))
    }

    /// Value is strictly inside range (exclusive).
    ///
    /// @param start - Range start
    /// @param end - Range end
    pub fn inside(start: JsValue, end: JsValue) -> Result<Predicate, JsError> {
        let s = js_to_value(start)?;
        let e = js_to_value(end)?;
        Ok(Predicate::new(rust_p::inside(s, e)))
    }

    /// Value is outside range.
    ///
    /// @param start - Range start
    /// @param end - Range end
    pub fn outside(start: JsValue, end: JsValue) -> Result<Predicate, JsError> {
        let s = js_to_value(start)?;
        let e = js_to_value(end)?;
        Ok(Predicate::new(rust_p::outside(s, e)))
    }

    // =========================================================================
    // Collection Predicates
    // =========================================================================

    /// Value is within the given set.
    ///
    /// @param values - Array of values to check membership
    pub fn within(values: JsValue) -> Result<Predicate, JsError> {
        let vals = crate::wasm::types::js_array_to_values(values)?;
        Ok(Predicate::new(rust_p::within(vals)))
    }

    /// Value is NOT within the given set.
    ///
    /// @param values - Array of values to exclude
    pub fn without(values: JsValue) -> Result<Predicate, JsError> {
        let vals = crate::wasm::types::js_array_to_values(values)?;
        Ok(Predicate::new(rust_p::without(vals)))
    }

    // =========================================================================
    // String Predicates
    // =========================================================================

    /// String contains substring.
    ///
    /// @param substring - Substring to find
    pub fn containing(substring: &str) -> Predicate {
        Predicate::new(rust_p::containing(substring))
    }

    /// String does NOT contain substring.
    ///
    /// @param substring - Substring that must be absent
    #[wasm_bindgen(js_name = "notContaining")]
    pub fn not_containing(substring: &str) -> Predicate {
        Predicate::new(rust_p::not_containing(substring))
    }

    /// String starts with prefix.
    ///
    /// @param prefix - Required prefix
    #[wasm_bindgen(js_name = "startingWith")]
    pub fn starting_with(prefix: &str) -> Predicate {
        Predicate::new(rust_p::starting_with(prefix))
    }

    /// String does NOT start with prefix.
    ///
    /// @param prefix - Forbidden prefix
    #[wasm_bindgen(js_name = "notStartingWith")]
    pub fn not_starting_with(prefix: &str) -> Predicate {
        Predicate::new(rust_p::not_starting_with(prefix))
    }

    /// String ends with suffix.
    ///
    /// @param suffix - Required suffix
    #[wasm_bindgen(js_name = "endingWith")]
    pub fn ending_with(suffix: &str) -> Predicate {
        Predicate::new(rust_p::ending_with(suffix))
    }

    /// String does NOT end with suffix.
    ///
    /// @param suffix - Forbidden suffix
    #[wasm_bindgen(js_name = "notEndingWith")]
    pub fn not_ending_with(suffix: &str) -> Predicate {
        Predicate::new(rust_p::not_ending_with(suffix))
    }

    /// String matches regular expression.
    ///
    /// @param pattern - Regex pattern
    pub fn regex(pattern: &str) -> Result<Predicate, JsError> {
        // Note: rust_p::regex returns the predicate directly
        Ok(Predicate::new(rust_p::regex(pattern)))
    }

    // =========================================================================
    // Logical Predicates
    // =========================================================================

    /// Logical AND of two predicates.
    ///
    /// @param p1 - First predicate
    /// @param p2 - Second predicate
    #[wasm_bindgen(js_name = "and")]
    pub fn and_(p1: Predicate, p2: Predicate) -> Predicate {
        // Use boxed versions for dynamic composition
        Predicate {
            inner: rust_p::and_pred(p1.inner, p2.inner),
        }
    }

    /// Logical OR of two predicates.
    ///
    /// @param p1 - First predicate
    /// @param p2 - Second predicate
    #[wasm_bindgen(js_name = "or")]
    pub fn or_(p1: Predicate, p2: Predicate) -> Predicate {
        // Use boxed versions for dynamic composition
        Predicate {
            inner: rust_p::or_pred(p1.inner, p2.inner),
        }
    }

    /// Logical NOT of a predicate.
    ///
    /// @param p - Predicate to negate
    #[wasm_bindgen(js_name = "not")]
    pub fn not_(pred: Predicate) -> Predicate {
        // Use boxed version for dynamic composition
        Predicate {
            inner: rust_p::not_pred(pred.inner),
        }
    }

    // =========================================================================
    // Geospatial Predicates
    // =========================================================================

    /// Check if a geo point property is within a given distance from a center point.
    ///
    /// @param lon - Center longitude
    /// @param lat - Center latitude
    /// @param distanceKm - Radius in kilometers
    ///
    /// @example
    /// ```typescript
    /// graph.V()
    ///     .hasWhere('location', P.withinDistance(-122.4, 37.7, 5.0))
    ///     .values('name')
    ///     .toList();
    /// ```
    #[wasm_bindgen(js_name = "withinDistance")]
    pub fn within_distance(lon: f64, lat: f64, distance_km: f64) -> Predicate {
        use crate::geo::{Distance, Point};
        let center = Point { lon, lat };
        let radius = Distance::Kilometers(distance_km);
        Predicate::new(rust_p::within_distance(center, radius))
    }
}
