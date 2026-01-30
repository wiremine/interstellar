//! Predicate system for napi-rs bindings.
//!
//! Provides the `P` namespace with factory functions for creating predicates.

use napi::bindgen_prelude::*;
use napi::JsUnknown;
use napi_derive::napi;

use interstellar::traversal::predicate;
use interstellar::traversal::predicate::p as rust_p;

use crate::value::js_to_value;

/// Internal representation of a predicate that can be used in traversal steps.
///
/// This wraps a boxed Rust predicate for use with napi-rs.
#[napi(js_name = "Predicate")]
pub struct JsPredicate {
    pub(crate) inner: Box<dyn predicate::Predicate>,
}

impl JsPredicate {
    pub(crate) fn new(pred: impl predicate::Predicate + 'static) -> Self {
        Self {
            inner: Box::new(pred),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn into_inner(self) -> Box<dyn predicate::Predicate> {
        self.inner
    }
}

impl Clone for JsPredicate {
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
/// import { Graph, P } from '@interstellar/node';
///
/// graph.V()
///     .hasWhere('age', P.gte(18))
///     .hasWhere('name', P.startingWith('A'))
///     .toList();
/// ```
#[napi]
pub struct P;

#[napi]
impl P {
    // =========================================================================
    // Comparison Predicates
    // =========================================================================

    /// Equals comparison.
    ///
    /// @param value - Value to compare against
    #[napi]
    pub fn eq(env: Env, value: JsUnknown) -> Result<JsPredicate> {
        let v = js_to_value(env, value)?;
        Ok(JsPredicate::new(rust_p::eq(v)))
    }

    /// Not equals comparison.
    ///
    /// @param value - Value to compare against
    #[napi]
    pub fn neq(env: Env, value: JsUnknown) -> Result<JsPredicate> {
        let v = js_to_value(env, value)?;
        Ok(JsPredicate::new(rust_p::neq(v)))
    }

    /// Less than comparison.
    ///
    /// @param value - Value to compare against
    #[napi]
    pub fn lt(env: Env, value: JsUnknown) -> Result<JsPredicate> {
        let v = js_to_value(env, value)?;
        Ok(JsPredicate::new(rust_p::lt(v)))
    }

    /// Less than or equal comparison.
    ///
    /// @param value - Value to compare against
    #[napi]
    pub fn lte(env: Env, value: JsUnknown) -> Result<JsPredicate> {
        let v = js_to_value(env, value)?;
        Ok(JsPredicate::new(rust_p::lte(v)))
    }

    /// Greater than comparison.
    ///
    /// @param value - Value to compare against
    #[napi]
    pub fn gt(env: Env, value: JsUnknown) -> Result<JsPredicate> {
        let v = js_to_value(env, value)?;
        Ok(JsPredicate::new(rust_p::gt(v)))
    }

    /// Greater than or equal comparison.
    ///
    /// @param value - Value to compare against
    #[napi]
    pub fn gte(env: Env, value: JsUnknown) -> Result<JsPredicate> {
        let v = js_to_value(env, value)?;
        Ok(JsPredicate::new(rust_p::gte(v)))
    }

    // =========================================================================
    // Range Predicates
    // =========================================================================

    /// Value is between start and end (inclusive start, exclusive end).
    ///
    /// @param start - Range start
    /// @param end - Range end
    #[napi]
    pub fn between(env: Env, start: JsUnknown, end: JsUnknown) -> Result<JsPredicate> {
        let s = js_to_value(env, start)?;
        let e = js_to_value(env, end)?;
        Ok(JsPredicate::new(rust_p::between(s, e)))
    }

    /// Value is strictly inside range (exclusive).
    ///
    /// @param start - Range start
    /// @param end - Range end
    #[napi]
    pub fn inside(env: Env, start: JsUnknown, end: JsUnknown) -> Result<JsPredicate> {
        let s = js_to_value(env, start)?;
        let e = js_to_value(env, end)?;
        Ok(JsPredicate::new(rust_p::inside(s, e)))
    }

    /// Value is outside range.
    ///
    /// @param start - Range start
    /// @param end - Range end
    #[napi]
    pub fn outside(env: Env, start: JsUnknown, end: JsUnknown) -> Result<JsPredicate> {
        let s = js_to_value(env, start)?;
        let e = js_to_value(env, end)?;
        Ok(JsPredicate::new(rust_p::outside(s, e)))
    }

    // =========================================================================
    // Collection Predicates
    // =========================================================================

    /// Value is within the given set.
    ///
    /// @param values - Array of values to check membership
    #[napi]
    pub fn within(env: Env, values: JsUnknown) -> Result<JsPredicate> {
        let vals = crate::value::js_array_to_values(env, values)?;
        Ok(JsPredicate::new(rust_p::within(vals)))
    }

    /// Value is NOT within the given set.
    ///
    /// @param values - Array of values to exclude
    #[napi]
    pub fn without(env: Env, values: JsUnknown) -> Result<JsPredicate> {
        let vals = crate::value::js_array_to_values(env, values)?;
        Ok(JsPredicate::new(rust_p::without(vals)))
    }

    // =========================================================================
    // String Predicates
    // =========================================================================

    /// String contains substring.
    ///
    /// @param substring - Substring to find
    #[napi]
    pub fn containing(substring: String) -> JsPredicate {
        JsPredicate::new(rust_p::containing(&substring))
    }

    /// String does NOT contain substring.
    ///
    /// @param substring - Substring that must be absent
    #[napi(js_name = "notContaining")]
    pub fn not_containing(substring: String) -> JsPredicate {
        JsPredicate::new(rust_p::not_containing(&substring))
    }

    /// String starts with prefix.
    ///
    /// @param prefix - Required prefix
    #[napi(js_name = "startingWith")]
    pub fn starting_with(prefix: String) -> JsPredicate {
        JsPredicate::new(rust_p::starting_with(&prefix))
    }

    /// String does NOT start with prefix.
    ///
    /// @param prefix - Forbidden prefix
    #[napi(js_name = "notStartingWith")]
    pub fn not_starting_with(prefix: String) -> JsPredicate {
        JsPredicate::new(rust_p::not_starting_with(&prefix))
    }

    /// String ends with suffix.
    ///
    /// @param suffix - Required suffix
    #[napi(js_name = "endingWith")]
    pub fn ending_with(suffix: String) -> JsPredicate {
        JsPredicate::new(rust_p::ending_with(&suffix))
    }

    /// String does NOT end with suffix.
    ///
    /// @param suffix - Forbidden suffix
    #[napi(js_name = "notEndingWith")]
    pub fn not_ending_with(suffix: String) -> JsPredicate {
        JsPredicate::new(rust_p::not_ending_with(&suffix))
    }

    /// String matches regular expression.
    ///
    /// @param pattern - Regex pattern
    #[napi]
    pub fn regex(pattern: String) -> JsPredicate {
        JsPredicate::new(rust_p::regex(&pattern))
    }

    // =========================================================================
    // Logical Predicates
    // =========================================================================

    /// Logical AND of two predicates.
    ///
    /// @param p1 - First predicate
    /// @param p2 - Second predicate
    #[napi(js_name = "and")]
    pub fn and_(p1: &JsPredicate, p2: &JsPredicate) -> JsPredicate {
        JsPredicate {
            inner: rust_p::and_pred(p1.inner.clone_box(), p2.inner.clone_box()),
        }
    }

    /// Logical OR of two predicates.
    ///
    /// @param p1 - First predicate
    /// @param p2 - Second predicate
    #[napi(js_name = "or")]
    pub fn or_(p1: &JsPredicate, p2: &JsPredicate) -> JsPredicate {
        JsPredicate {
            inner: rust_p::or_pred(p1.inner.clone_box(), p2.inner.clone_box()),
        }
    }

    /// Logical NOT of a predicate.
    ///
    /// @param p - Predicate to negate
    #[napi(js_name = "not")]
    pub fn not_(pred: &JsPredicate) -> JsPredicate {
        JsPredicate {
            inner: rust_p::not_pred(pred.inner.clone_box()),
        }
    }
}
