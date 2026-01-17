//! Helper types and functions for the GQL compiler.
//!
//! This module contains:
//! - `ComparableValue`: A wrapper for `Value` that implements `Eq` and `Hash` for grouping
//! - Comparison and binary operation functions
//! - Inline WHERE expression evaluation utilities

use std::collections::HashMap;

use crate::gql::ast::{BinaryOperator, Expression, UnaryOperator};
use crate::graph::GraphSnapshot;
use crate::value::Value;

use super::Parameters;

// =============================================================================
// Helper Types for Aggregation
// =============================================================================

/// A comparable wrapper for Value that implements Eq and Hash for grouping.
#[derive(Debug, Clone)]
pub(super) struct ComparableValue(pub(super) Value);

impl PartialEq for ComparableValue {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => {
                // Handle NaN and compare floats bitwise
                a.to_bits() == b.to_bits()
            }
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Vertex(a), Value::Vertex(b)) => a == b,
            (Value::Edge(a), Value::Edge(b)) => a == b,
            (Value::List(a), Value::List(b)) => {
                if a.len() != b.len() {
                    return false;
                }
                a.iter()
                    .zip(b.iter())
                    .all(|(x, y)| ComparableValue(x.clone()) == ComparableValue(y.clone()))
            }
            (Value::Map(a), Value::Map(b)) => {
                if a.len() != b.len() {
                    return false;
                }
                a.iter().all(|(k, v)| {
                    b.get(k)
                        .map(|bv| ComparableValue(v.clone()) == ComparableValue(bv.clone()))
                        .unwrap_or(false)
                })
            }
            _ => false,
        }
    }
}

impl Eq for ComparableValue {}

impl std::hash::Hash for ComparableValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(&self.0).hash(state);
        match &self.0 {
            Value::Null => {}
            Value::Bool(b) => b.hash(state),
            Value::Int(n) => n.hash(state),
            Value::Float(f) => f.to_bits().hash(state),
            Value::String(s) => s.hash(state),
            Value::Vertex(id) => id.0.hash(state),
            Value::Edge(id) => id.0.hash(state),
            Value::List(items) => {
                items.len().hash(state);
                for item in items {
                    ComparableValue(item.clone()).hash(state);
                }
            }
            Value::Map(map) => {
                map.len().hash(state);
                // Note: HashMap order is not deterministic, but we still hash for consistency
                for (k, v) in map {
                    k.hash(state);
                    ComparableValue(v.clone()).hash(state);
                }
            }
        }
    }
}

impl From<Value> for ComparableValue {
    fn from(v: Value) -> Self {
        ComparableValue(v)
    }
}

impl From<ComparableValue> for Value {
    fn from(cv: ComparableValue) -> Self {
        cv.0
    }
}

// =============================================================================
// Helper Functions for Expression Evaluation
// =============================================================================

/// Apply a comparison operator to two values.
pub(super) fn apply_comparison(op: BinaryOperator, left: &Value, right: &Value) -> bool {
    match op {
        BinaryOperator::Eq => left == right,
        BinaryOperator::Neq => left != right,
        BinaryOperator::Lt => compare_values(left, right) == std::cmp::Ordering::Less,
        BinaryOperator::Lte => {
            matches!(
                compare_values(left, right),
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal
            )
        }
        BinaryOperator::Gt => compare_values(left, right) == std::cmp::Ordering::Greater,
        BinaryOperator::Gte => {
            matches!(
                compare_values(left, right),
                std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
            )
        }
        BinaryOperator::And => value_to_bool(left) && value_to_bool(right),
        BinaryOperator::Or => value_to_bool(left) || value_to_bool(right),
        BinaryOperator::Contains => match (left, right) {
            (Value::String(s), Value::String(sub)) => s.contains(sub.as_str()),
            _ => false,
        },
        BinaryOperator::StartsWith => match (left, right) {
            (Value::String(s), Value::String(prefix)) => s.starts_with(prefix.as_str()),
            _ => false,
        },
        BinaryOperator::EndsWith => match (left, right) {
            (Value::String(s), Value::String(suffix)) => s.ends_with(suffix.as_str()),
            _ => false,
        },
        BinaryOperator::RegexMatch => match (left, right) {
            (Value::String(s), Value::String(pattern)) => {
                // Compile and match the regex pattern
                match regex::Regex::new(pattern) {
                    Ok(re) => re.is_match(s),
                    Err(_) => false, // Invalid regex pattern returns false
                }
            }
            // NULL operands return false (not a match)
            (Value::Null, _) | (_, Value::Null) => false,
            // Non-string operands return false
            _ => false,
        },
        // Arithmetic operators don't return bool, but we handle them for completeness
        _ => false,
    }
}

/// Apply a binary operator and return the result as a Value.
pub(super) fn apply_binary_op(op: BinaryOperator, left: Value, right: Value) -> Value {
    match op {
        BinaryOperator::Add => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a + b),
            (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
            (Value::Int(a), Value::Float(b)) => Value::Float(a as f64 + b),
            (Value::Float(a), Value::Int(b)) => Value::Float(a + b as f64),
            (Value::String(a), Value::String(b)) => Value::String(a + b.as_str()),
            _ => Value::Null,
        },
        BinaryOperator::Sub => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a - b),
            (Value::Float(a), Value::Float(b)) => Value::Float(a - b),
            (Value::Int(a), Value::Float(b)) => Value::Float(a as f64 - b),
            (Value::Float(a), Value::Int(b)) => Value::Float(a - b as f64),
            _ => Value::Null,
        },
        BinaryOperator::Mul => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a * b),
            (Value::Float(a), Value::Float(b)) => Value::Float(a * b),
            (Value::Int(a), Value::Float(b)) => Value::Float(a as f64 * b),
            (Value::Float(a), Value::Int(b)) => Value::Float(a * b as f64),
            _ => Value::Null,
        },
        BinaryOperator::Div => match (left, right) {
            (Value::Int(a), Value::Int(b)) if b != 0 => Value::Int(a / b),
            (Value::Float(a), Value::Float(b)) if b != 0.0 => Value::Float(a / b),
            (Value::Int(a), Value::Float(b)) if b != 0.0 => Value::Float(a as f64 / b),
            (Value::Float(a), Value::Int(b)) if b != 0 => Value::Float(a / b as f64),
            _ => Value::Null,
        },
        BinaryOperator::Mod => match (left, right) {
            (Value::Int(a), Value::Int(b)) if b != 0 => Value::Int(a % b),
            _ => Value::Null,
        },
        BinaryOperator::Pow => match (left, right) {
            // Integer to non-negative integer power
            (Value::Int(a), Value::Int(b)) if b >= 0 => Value::Int(a.pow(b as u32)),
            // Integer to negative power becomes float
            (Value::Int(a), Value::Int(b)) => Value::Float((a as f64).powi(b as i32)),
            // Float to integer power
            (Value::Float(a), Value::Int(b)) => Value::Float(a.powi(b as i32)),
            // Float to float power
            (Value::Float(a), Value::Float(b)) => Value::Float(a.powf(b)),
            // Integer base with float exponent
            (Value::Int(a), Value::Float(b)) => Value::Float((a as f64).powf(b)),
            _ => Value::Null,
        },
        // String concatenation operator
        BinaryOperator::Concat => match (&left, &right) {
            (Value::Null, _) | (_, Value::Null) => Value::Null,
            _ => {
                let left_str = value_to_string(&left);
                let right_str = value_to_string(&right);
                Value::String(format!("{}{}", left_str, right_str))
            }
        },
        // Comparison operators return Bool
        op => Value::Bool(apply_comparison(op, &left, &right)),
    }
}

/// Compare two values, returning Ordering.
pub(super) fn compare_values(left: &Value, right: &Value) -> std::cmp::Ordering {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Int(a), Value::Float(b)) => (*a as f64)
            .partial_cmp(b)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::Float(a), Value::Int(b)) => a
            .partial_cmp(&(*b as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        // Null is less than everything except Null
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        // Incompatible types - default to equal
        _ => std::cmp::Ordering::Equal,
    }
}

/// Convert a Value to a boolean for truthiness checks.
pub(super) fn value_to_bool(val: &Value) -> bool {
    match val {
        Value::Bool(b) => *b,
        Value::Null => false,
        Value::Int(n) => *n != 0,
        Value::Float(f) => *f != 0.0,
        Value::String(s) => !s.is_empty(),
        _ => true,
    }
}

/// Convert a Value to a string representation for concatenation.
pub(super) fn value_to_string(val: &Value) -> String {
    match val {
        Value::String(s) => s.clone(),
        Value::Int(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::List(items) => {
            let inner: Vec<String> = items.iter().map(value_to_string).collect();
            format!("[{}]", inner.join(", "))
        }
        Value::Map(map) => {
            let inner: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}: {}", k, value_to_string(v)))
                .collect();
            format!("{{{}}}", inner.join(", "))
        }
        Value::Vertex(vid) => format!("Vertex({})", vid.0),
        Value::Edge(eid) => format!("Edge({})", eid.0),
    }
}

// =============================================================================
// Inline WHERE Expression Evaluation
// =============================================================================
//
// These functions evaluate expressions for inline WHERE clauses in patterns.
// They are designed to be called from within filter closures, taking a
// GraphSnapshot reference and an element Value.

/// Extract a property value from a vertex or edge using a snapshot.
///
/// This is a standalone version of `Compiler::extract_property` for use in closures.
pub(super) fn extract_property_from_snapshot<'g>(
    snapshot: &GraphSnapshot<'g>,
    element: &Value,
    property: &str,
) -> Option<Value> {
    match element {
        Value::Vertex(id) => {
            let vertex = snapshot.storage().get_vertex(*id)?;
            vertex.properties.get(property).cloned()
        }
        Value::Edge(id) => {
            let edge = snapshot.storage().get_edge(*id)?;
            edge.properties.get(property).cloned()
        }
        Value::Null => Some(Value::Null),
        _ => None,
    }
}

/// Evaluate an expression to a Value for inline WHERE clauses.
///
/// This is a standalone version of `Compiler::evaluate_value` that takes a
/// snapshot reference, suitable for use within filter closures.
pub(super) fn eval_inline_value<'g>(
    snapshot: &GraphSnapshot<'g>,
    expr: &Expression,
    element: &Value,
    params: &Parameters,
) -> Value {
    match expr {
        Expression::Literal(lit) => lit.clone().into(),
        Expression::Variable(_) => {
            // Return the element itself when referencing a variable
            element.clone()
        }
        Expression::Parameter(name) => {
            // Resolve parameter value
            params.get(name).cloned().unwrap_or(Value::Null)
        }
        Expression::Property { property, .. } => {
            // Extract property from the element
            extract_property_from_snapshot(snapshot, element, property).unwrap_or(Value::Null)
        }
        Expression::BinaryOp { left, op, right } => {
            let left_val = eval_inline_value(snapshot, left, element, params);
            let right_val = eval_inline_value(snapshot, right, element, params);
            apply_binary_op(*op, left_val, right_val)
        }
        Expression::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => {
                let val = eval_inline_value(snapshot, expr, element, params);
                match val {
                    Value::Bool(b) => Value::Bool(!b),
                    _ => Value::Null,
                }
            }
            UnaryOperator::Neg => {
                let val = eval_inline_value(snapshot, expr, element, params);
                match val {
                    Value::Int(n) => Value::Int(-n),
                    Value::Float(f) => Value::Float(-f),
                    _ => Value::Null,
                }
            }
        },
        Expression::IsNull { expr, negated } => {
            let val = eval_inline_value(snapshot, expr, element, params);
            let is_null = matches!(val, Value::Null);
            Value::Bool(if *negated { !is_null } else { is_null })
        }
        Expression::InList {
            expr,
            list,
            negated,
        } => {
            let val = eval_inline_value(snapshot, expr, element, params);
            let in_list = list.iter().any(|item| {
                let item_val = eval_inline_value(snapshot, item, element, params);
                val == item_val
            });
            Value::Bool(if *negated { !in_list } else { in_list })
        }
        Expression::List(items) => {
            let values: Vec<Value> = items
                .iter()
                .map(|item| eval_inline_value(snapshot, item, element, params))
                .collect();
            Value::List(values)
        }
        Expression::Map(entries) => {
            let map: HashMap<String, Value> = entries
                .iter()
                .map(|(key, value_expr)| {
                    let value = eval_inline_value(snapshot, value_expr, element, params);
                    (key.clone(), value)
                })
                .collect();
            Value::Map(map)
        }
        // For inline WHERE, we don't support complex expressions like EXISTS, CASE, or function calls
        // These would require additional context or be expensive to evaluate per-element
        _ => Value::Null,
    }
}

/// Evaluate a predicate expression for inline WHERE clauses.
///
/// This is a standalone version of `Compiler::evaluate_predicate` that takes a
/// snapshot reference, suitable for use within filter closures.
pub(super) fn eval_inline_predicate<'g>(
    snapshot: &GraphSnapshot<'g>,
    expr: &Expression,
    element: &Value,
    params: &Parameters,
) -> bool {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            match op {
                // Logical operators
                BinaryOperator::And => {
                    eval_inline_predicate(snapshot, left, element, params)
                        && eval_inline_predicate(snapshot, right, element, params)
                }
                BinaryOperator::Or => {
                    eval_inline_predicate(snapshot, left, element, params)
                        || eval_inline_predicate(snapshot, right, element, params)
                }
                // Comparison and other operators
                _ => {
                    let left_val = eval_inline_value(snapshot, left, element, params);
                    let right_val = eval_inline_value(snapshot, right, element, params);
                    apply_comparison(*op, &left_val, &right_val)
                }
            }
        }
        Expression::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => !eval_inline_predicate(snapshot, expr, element, params),
            UnaryOperator::Neg => {
                // Negation of a value - treat non-zero as true
                match eval_inline_value(snapshot, expr, element, params) {
                    Value::Int(n) => n == 0,
                    Value::Float(f) => f == 0.0,
                    Value::Bool(b) => !b,
                    Value::Null => true,
                    _ => false,
                }
            }
        },
        Expression::IsNull { expr, negated } => {
            let val = eval_inline_value(snapshot, expr, element, params);
            let is_null = matches!(val, Value::Null);
            if *negated {
                !is_null
            } else {
                is_null
            }
        }
        Expression::InList {
            expr,
            list,
            negated,
        } => {
            let val = eval_inline_value(snapshot, expr, element, params);
            let in_list = list.iter().any(|item| {
                let item_val = eval_inline_value(snapshot, item, element, params);
                val == item_val
            });
            if *negated {
                !in_list
            } else {
                in_list
            }
        }
        // For other expressions, evaluate and check truthiness
        _ => {
            let val = eval_inline_value(snapshot, expr, element, params);
            value_to_bool(&val)
        }
    }
}
