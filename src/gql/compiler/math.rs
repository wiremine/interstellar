//! Math expression evaluation for the GQL compiler.
//!
//! This module contains the MATH() function implementation that allows
//! embedding mathexpr expressions within GQL queries.
//!
//! # Syntax
//!
//! `MATH(expression_string, arg1, arg2, ...)`
//!
//! Where:
//! - `expression_string`: A mathexpr expression as a string
//! - `arg1, arg2, ...`: Values to bind to variables `a`, `b`, `c`, etc.
//!
//! If the expression uses `_`, the first argument is bound to the current value.

use mathexpr::Expression as MathExpr;

use crate::value::Value;

/// Internal helper to evaluate a mathexpr expression with given values.
///
/// Variable bindings:
/// - First value is bound to `_` (current value in mathexpr) if expression uses `_`
/// - Additional values (or all values if no `_`) are bound to `a`, `b`, `c`, etc.
pub(super) fn evaluate_math_expr_internal(expr_string: &str, var_values: &[f64]) -> Value {
    // Parse the expression first to check if it uses current value
    let parsed = match MathExpr::parse(expr_string) {
        Ok(p) => p,
        Err(_) => return Value::Null,
    };

    // Determine variable names based on whether expression uses `_`
    static VAR_NAMES: &[&str] = &["a", "b", "c", "d", "e", "f", "g", "h"];

    // Compile with empty var names first to check if it uses current value
    // If it does, the first arg is `_` and the rest are named variables
    let uses_current = {
        // Try compiling with no vars to see if only `_` is used
        let test_compile = parsed.clone().compile(&[]);
        test_compile.is_ok()
            && test_compile
                .as_ref()
                .map(|c| c.uses_current_value())
                .unwrap_or(false)
            && var_values.len() <= 1
    };

    if uses_current && !var_values.is_empty() {
        // Expression uses `_` and we have a single value for it
        let compiled = match parsed.compile(&[]) {
            Ok(c) => c,
            Err(_) => return Value::Null,
        };

        let result = compiled.eval_with_current(var_values[0], &[]);
        return match result {
            Ok(r) if !r.is_nan() && !r.is_infinite() => Value::Float(r),
            _ => Value::Null,
        };
    }

    // Expression uses named variables (a, b, c, ...) or a mix of `_` and named vars
    // Build variable names based on count
    let var_names: Vec<&str> = VAR_NAMES.iter().take(var_values.len()).copied().collect();

    let compiled = match parsed.compile(&var_names) {
        Ok(c) => c,
        Err(_) => return Value::Null,
    };

    // Evaluate the expression
    let result = if var_values.is_empty() {
        // No arguments - just evaluate the expression (constants only)
        compiled.eval(&[])
    } else if compiled.uses_current_value() {
        // Expression uses both `_` and named variables
        // First arg is current, rest are named
        compiled.eval_with_current(var_values[0], &var_values[1..])
    } else {
        // Expression uses only named variables
        compiled.eval(var_values)
    };

    match result {
        Ok(r) if !r.is_nan() && !r.is_infinite() => Value::Float(r),
        _ => Value::Null,
    }
}
