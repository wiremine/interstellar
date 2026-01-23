# Spec 09-Math-GQL: Math Expression Integration for GQL

**Phase 4 Feature: Mathematical Expressions in GQL Queries**

## Overview

This specification details the integration of the `MathStep` mathematical expression evaluator into the GQL query language. This enables users to perform complex mathematical calculations directly within GQL queries using a `MATH()` function and inline arithmetic expressions.

**Duration**: 1-2 days  
**Priority**: Medium  
**Dependencies**: 
- MathStep implementation (complete - `src/traversal/transform/functional.rs`)
- GQL Parser and Compiler (complete - `src/gql/`)
- `mathexpr` crate (v0.1.1)

---

## Current State

### MathStep Capabilities (Already Implemented)

The `MathStep` in the traversal API supports:

- **Operators**: `+`, `-`, `*`, `/`, `%`, `^` (power)
- **Functions**: `sqrt`, `abs`, `sin`, `cos`, `tan`, `log`, `exp`, `pow`, `min`, `max`, `floor`, `ceil`, `round`
- **Constants**: `pi`, `e`
- **Special variable**: `_` represents the current traverser value
- **Path variables**: Labeled values from `as_()` steps via `.by(variable, key)` bindings
- **Output**: Always `Value::Float`
- **Error handling**: Non-numeric values and domain errors (NaN, Infinity) filter out the traverser

### Current GQL Math Support

GQL already supports basic inline arithmetic:
- `+`, `-`, `*`, `/`, `%` operators in expressions
- `ABS()`, `CEIL()`, `FLOOR()`, `ROUND()`, `SIGN()` functions

**Gap**: No support for:
- Power operator (`^`)
- Trigonometric functions (`sin`, `cos`, `tan`)
- Logarithmic functions (`log`, `exp`)
- Multi-argument math functions (`pow`, `min`, `max`)
- Square root (`sqrt`)
- Mathematical constants (`pi`, `e`)
- Complex expressions with the `_` current value placeholder

---

## Goals

1. Add a `MATH()` function to GQL that evaluates mathematical expressions using `mathexpr`
2. Support the `_` placeholder for referencing the current expression context value
3. Support variable references from bound pattern variables
4. Extend GQL's built-in function set with additional math functions
5. Maintain backward compatibility with existing arithmetic expressions

---

## Architecture

### Design Options

**Option A: MATH() Function Only**
Add a `MATH(expression_string)` function that parses and evaluates the expression:
```sql
MATCH (p:player) 
RETURN p.name, MATH('sqrt(p.ppg ^ 2 + p.rpg ^ 2)') AS stat_magnitude
```

**Option B: Extended Expression Syntax**
Add new operators and functions directly to the GQL grammar:
```sql
MATCH (p:player) 
RETURN p.name, sqrt(p.ppg ^ 2 + p.rpg ^ 2) AS stat_magnitude
```

**Option C: Hybrid Approach (Recommended)**
- Add commonly-used math functions directly to GQL (`sqrt`, `pow`, `log`, `exp`, `sin`, `cos`, `tan`)
- Add the `^` power operator to the grammar
- Add `MATH()` function for complex expressions with `_` placeholder
- Add `pi()` and `e()` as zero-argument functions

**Chosen Approach**: Option C (Hybrid)

This provides the best user experience:
- Simple expressions use natural GQL syntax: `sqrt(p.ppg)`
- Complex expressions use MATH(): `MATH('sin(_ * pi / 180)')` 
- Full mathexpr power available when needed

---

## Implementation Phases

### Phase 1: Power Operator and Basic Math Functions (2-3 hours)

#### 1.1 Grammar Updates

**File**: `src/gql/grammar.pest`

Add power operator with correct precedence (higher than multiplication):

```pest
// Update multiplicative to include power
multiplicative = { power ~ (mul_op ~ power)* }
mul_op = { "*" | "/" | "%" }

// Add power expression (right-associative, highest arithmetic precedence)
power = { unary ~ (pow_op ~ unary)* }
pow_op = { "^" }

// Existing unary remains unchanged
unary = { neg_op? ~ primary }
```

**Acceptance Criteria**:
- [ ] `p.age ^ 2` parses correctly
- [ ] `2 ^ 3 ^ 2` parses as `2 ^ (3 ^ 2)` (right-associative)
- [ ] Precedence: `2 + 3 ^ 2` = `2 + 9` = `11`

#### 1.2 AST Updates

**File**: `src/gql/ast.rs`

Add power operator to `BinaryOperator`:

```rust
/// Binary operators for expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BinaryOperator {
    // ... existing operators ...
    
    // Arithmetic operators
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    /// Power/exponentiation: `^`
    Pow,  // NEW
    
    // ... rest unchanged ...
}
```

#### 1.3 Parser Updates

**File**: `src/gql/parser.rs`

Update `build_binary_op` to handle the power operator:

```rust
fn build_binary_op(pair: pest::iterators::Pair<Rule>) -> Result<BinaryOperator, ParseError> {
    match pair.as_rule() {
        // ... existing cases ...
        Rule::pow_op => Ok(BinaryOperator::Pow),
        // ...
    }
}
```

Update the expression building to handle the new `power` rule.

#### 1.4 Compiler Updates

**File**: `src/gql/compiler.rs`

Update `apply_binary_op` to handle power:

```rust
fn apply_binary_op(op: BinaryOperator, left: Value, right: Value) -> Value {
    match op {
        // ... existing cases ...
        BinaryOperator::Pow => {
            match (left, right) {
                (Value::Int(a), Value::Int(b)) => {
                    if b >= 0 {
                        Value::Int(a.pow(b as u32))
                    } else {
                        Value::Float((a as f64).powf(b as f64))
                    }
                }
                (Value::Float(a), Value::Int(b)) => Value::Float(a.powi(b as i32)),
                (Value::Int(a), Value::Float(b)) => Value::Float((a as f64).powf(b)),
                (Value::Float(a), Value::Float(b)) => Value::Float(a.powf(b)),
                _ => Value::Null,
            }
        }
        // ...
    }
}
```

---

### Phase 2: Extended Math Functions (2-3 hours)

#### 2.1 Add Math Functions to Compiler

**File**: `src/gql/compiler.rs`

Extend `evaluate_function_call_from_path` with new math functions:

```rust
fn evaluate_function_call_from_path(
    &self,
    name: &str,
    args: &[Expression],
    traverser: &crate::traversal::Traverser,
) -> Value {
    match name.to_uppercase().as_str() {
        // ... existing functions ...
        
        // Square root
        "SQRT" => {
            if let Some(arg) = args.first() {
                match self.evaluate_value_from_path(arg, traverser) {
                    Value::Int(n) if n >= 0 => Value::Float((n as f64).sqrt()),
                    Value::Float(f) if f >= 0.0 => Value::Float(f.sqrt()),
                    _ => Value::Null, // Domain error or non-numeric
                }
            } else {
                Value::Null
            }
        }
        
        // Power function (alternative to ^ operator)
        "POW" | "POWER" => {
            if args.len() >= 2 {
                let base = self.evaluate_value_from_path(&args[0], traverser);
                let exp = self.evaluate_value_from_path(&args[1], traverser);
                apply_binary_op(BinaryOperator::Pow, base, exp)
            } else {
                Value::Null
            }
        }
        
        // Logarithm (natural log)
        "LOG" | "LN" => {
            if let Some(arg) = args.first() {
                match self.evaluate_value_from_path(arg, traverser) {
                    Value::Int(n) if n > 0 => Value::Float((n as f64).ln()),
                    Value::Float(f) if f > 0.0 => Value::Float(f.ln()),
                    _ => Value::Null,
                }
            } else {
                Value::Null
            }
        }
        
        // Log base 10
        "LOG10" => {
            if let Some(arg) = args.first() {
                match self.evaluate_value_from_path(arg, traverser) {
                    Value::Int(n) if n > 0 => Value::Float((n as f64).log10()),
                    Value::Float(f) if f > 0.0 => Value::Float(f.log10()),
                    _ => Value::Null,
                }
            } else {
                Value::Null
            }
        }
        
        // Exponential (e^x)
        "EXP" => {
            if let Some(arg) = args.first() {
                match self.evaluate_value_from_path(arg, traverser) {
                    Value::Int(n) => Value::Float((n as f64).exp()),
                    Value::Float(f) => Value::Float(f.exp()),
                    _ => Value::Null,
                }
            } else {
                Value::Null
            }
        }
        
        // Trigonometric functions (input in radians)
        "SIN" => {
            if let Some(arg) = args.first() {
                match self.evaluate_value_from_path(arg, traverser) {
                    Value::Int(n) => Value::Float((n as f64).sin()),
                    Value::Float(f) => Value::Float(f.sin()),
                    _ => Value::Null,
                }
            } else {
                Value::Null
            }
        }
        
        "COS" => {
            if let Some(arg) = args.first() {
                match self.evaluate_value_from_path(arg, traverser) {
                    Value::Int(n) => Value::Float((n as f64).cos()),
                    Value::Float(f) => Value::Float(f.cos()),
                    _ => Value::Null,
                }
            } else {
                Value::Null
            }
        }
        
        "TAN" => {
            if let Some(arg) = args.first() {
                match self.evaluate_value_from_path(arg, traverser) {
                    Value::Int(n) => Value::Float((n as f64).tan()),
                    Value::Float(f) => Value::Float(f.tan()),
                    _ => Value::Null,
                }
            } else {
                Value::Null
            }
        }
        
        // Inverse trigonometric functions
        "ASIN" => {
            if let Some(arg) = args.first() {
                match self.evaluate_value_from_path(arg, traverser) {
                    Value::Int(n) => {
                        let f = n as f64;
                        if (-1.0..=1.0).contains(&f) {
                            Value::Float(f.asin())
                        } else {
                            Value::Null
                        }
                    }
                    Value::Float(f) if (-1.0..=1.0).contains(&f) => Value::Float(f.asin()),
                    _ => Value::Null,
                }
            } else {
                Value::Null
            }
        }
        
        "ACOS" => {
            if let Some(arg) = args.first() {
                match self.evaluate_value_from_path(arg, traverser) {
                    Value::Int(n) => {
                        let f = n as f64;
                        if (-1.0..=1.0).contains(&f) {
                            Value::Float(f.acos())
                        } else {
                            Value::Null
                        }
                    }
                    Value::Float(f) if (-1.0..=1.0).contains(&f) => Value::Float(f.acos()),
                    _ => Value::Null,
                }
            } else {
                Value::Null
            }
        }
        
        "ATAN" => {
            if let Some(arg) = args.first() {
                match self.evaluate_value_from_path(arg, traverser) {
                    Value::Int(n) => Value::Float((n as f64).atan()),
                    Value::Float(f) => Value::Float(f.atan()),
                    _ => Value::Null,
                }
            } else {
                Value::Null
            }
        }
        
        // Two-argument arctangent (atan2)
        "ATAN2" => {
            if args.len() >= 2 {
                let y = self.evaluate_value_from_path(&args[0], traverser);
                let x = self.evaluate_value_from_path(&args[1], traverser);
                match (y, x) {
                    (Value::Int(y), Value::Int(x)) => {
                        Value::Float((y as f64).atan2(x as f64))
                    }
                    (Value::Float(y), Value::Float(x)) => Value::Float(y.atan2(x)),
                    (Value::Int(y), Value::Float(x)) => Value::Float((y as f64).atan2(x)),
                    (Value::Float(y), Value::Int(x)) => Value::Float(y.atan2(x as f64)),
                    _ => Value::Null,
                }
            } else {
                Value::Null
            }
        }
        
        // Degree/radian conversion
        "RADIANS" | "TORADIANS" => {
            if let Some(arg) = args.first() {
                match self.evaluate_value_from_path(arg, traverser) {
                    Value::Int(n) => Value::Float((n as f64).to_radians()),
                    Value::Float(f) => Value::Float(f.to_radians()),
                    _ => Value::Null,
                }
            } else {
                Value::Null
            }
        }
        
        "DEGREES" | "TODEGREES" => {
            if let Some(arg) = args.first() {
                match self.evaluate_value_from_path(arg, traverser) {
                    Value::Int(n) => Value::Float((n as f64).to_degrees()),
                    Value::Float(f) => Value::Float(f.to_degrees()),
                    _ => Value::Null,
                }
            } else {
                Value::Null
            }
        }
        
        // Mathematical constants
        "PI" => Value::Float(std::f64::consts::PI),
        "E" => Value::Float(std::f64::consts::E),
        
        // Multi-argument min/max
        "MIN" => {
            // Existing MIN might be aggregate - check if used as scalar
            if args.len() >= 2 {
                // Scalar min of multiple arguments
                let values: Vec<f64> = args
                    .iter()
                    .filter_map(|arg| {
                        match self.evaluate_value_from_path(arg, traverser) {
                            Value::Int(n) => Some(n as f64),
                            Value::Float(f) => Some(f),
                            _ => None,
                        }
                    })
                    .collect();
                
                if values.len() == args.len() {
                    values.into_iter()
                        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                        .map(Value::Float)
                        .unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            } else {
                // Single-arg MIN is aggregate (handled elsewhere)
                Value::Null
            }
        }
        
        "MAX" => {
            if args.len() >= 2 {
                // Scalar max of multiple arguments
                let values: Vec<f64> = args
                    .iter()
                    .filter_map(|arg| {
                        match self.evaluate_value_from_path(arg, traverser) {
                            Value::Int(n) => Some(n as f64),
                            Value::Float(f) => Some(f),
                            _ => None,
                        }
                    })
                    .collect();
                
                if values.len() == args.len() {
                    values.into_iter()
                        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                        .map(Value::Float)
                        .unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            } else {
                // Single-arg MAX is aggregate (handled elsewhere)
                Value::Null
            }
        }
        
        // ... existing functions ...
    }
}
```

**Acceptance Criteria**:
- [ ] `sqrt(16)` returns `4.0`
- [ ] `pow(2, 10)` returns `1024.0`
- [ ] `log(e())` returns `1.0`
- [ ] `sin(pi() / 2)` returns `1.0`
- [ ] `degrees(pi())` returns `180.0`
- [ ] `min(3, 1, 4, 1, 5)` returns `1.0`

---

### Phase 3: MATH() Function with mathexpr Integration (3-4 hours)

#### 3.1 MATH() Function Concept

The `MATH()` function provides full mathexpr expression power within GQL:

```sql
-- Current value placeholder (_) bound to the expression context
MATCH (p:player)
RETURN p.name, MATH('sqrt(_ * 2)', p.ppg) AS adjusted_score

-- Multiple variable bindings  
MATCH (p:player)
RETURN p.name, MATH('sqrt(x^2 + y^2)', p.ppg, p.rpg) AS stat_magnitude

-- Using labeled variables
MATCH (p1:player)-[:teammate]->(p2:player)
RETURN MATH('a - b', p1.ppg, p2.ppg) AS ppg_difference
```

#### 3.2 AST Updates

**File**: `src/gql/ast.rs`

The existing `FunctionCall` expression type can be used for `MATH()`. No AST changes needed - it parses as a regular function call.

#### 3.3 Compiler Implementation

**File**: `src/gql/compiler.rs`

Add MATH function handler:

```rust
fn evaluate_function_call_from_path(
    &self,
    name: &str,
    args: &[Expression],
    traverser: &crate::traversal::Traverser,
) -> Value {
    match name.to_uppercase().as_str() {
        // ... existing functions ...
        
        "MATH" => {
            self.evaluate_math_expression(args, traverser)
        }
        
        // ...
    }
}

/// Evaluate a MATH() expression using the mathexpr crate.
/// 
/// Syntax: MATH(expression_string, var1, var2, ...)
/// 
/// The expression string can reference:
/// - `_` for the first variable (current context value)
/// - `a`, `b`, `c`, ... for subsequent variables (positional)
/// - Or named: `x`, `y`, `z` for the first three variables
/// 
/// # Examples
/// 
/// ```sql
/// MATH('_ * 2', p.age)           -- Double age
/// MATH('sqrt(x^2 + y^2)', a, b)  -- Pythagorean distance
/// MATH('sin(_ * pi / 180)', deg) -- Convert degrees to sin
/// ```
fn evaluate_math_expression(
    &self,
    args: &[Expression],
    traverser: &crate::traversal::Traverser,
) -> Value {
    use mathexpr::Expression as MathExpr;
    
    // First argument must be the expression string
    if args.is_empty() {
        return Value::Null;
    }
    
    let expr_string = match self.evaluate_value_from_path(&args[0], traverser) {
        Value::String(s) => s,
        _ => return Value::Null, // Expression must be a string literal
    };
    
    // Evaluate remaining arguments as numeric values
    let mut var_values: Vec<f64> = Vec::new();
    for arg in args.iter().skip(1) {
        let val = self.evaluate_value_from_path(arg, traverser);
        match val {
            Value::Int(n) => var_values.push(n as f64),
            Value::Float(f) => var_values.push(f),
            Value::Null => return Value::Null, // Can't compute with nulls
            _ => return Value::Null, // Non-numeric value
        }
    }
    
    // Build variable names based on count
    // Convention: first value is bound to `_` (current), rest are a, b, c, ... or x, y, z
    let var_names: Vec<&str> = match var_values.len() {
        0 => vec![],
        1 => vec![],  // Single value uses `_` (current value in mathexpr)
        2 => vec!["a", "b"],
        3 => vec!["a", "b", "c"],
        n => {
            // Generate alphabetic names: a, b, c, ..., z, aa, ab, ...
            (0..n).map(|i| {
                // This is a simplification - for > 26 vars we'd need more complex naming
                static NAMES: &[&str] = &[
                    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
                    "k", "l", "m", "n", "o", "p", "q", "r", "s", "t",
                    "u", "v", "w", "x", "y", "z"
                ];
                if i < NAMES.len() { NAMES[i] } else { "v" }
            }).collect()
        }
    };
    
    // Parse and compile the expression
    let parsed = match MathExpr::parse(&expr_string) {
        Ok(p) => p,
        Err(_) => return Value::Null, // Parse error
    };
    
    let compiled = match parsed.compile(&var_names) {
        Ok(c) => c,
        Err(_) => return Value::Null, // Compile error (unknown variable)
    };
    
    // Evaluate
    let result = if var_values.len() == 1 && compiled.uses_current_value() {
        // Single argument uses the `_` current value
        compiled.eval_with_current(var_values[0], &[])
    } else if var_values.len() == 1 {
        // Single argument but expression doesn't use `_`
        compiled.eval(&var_values)
    } else {
        // Multiple arguments
        if compiled.uses_current_value() && !var_values.is_empty() {
            // First argument is `_`, rest are named variables
            let current = var_values[0];
            let rest = &var_values[1..];
            compiled.eval_with_current(current, rest)
        } else {
            compiled.eval(&var_values)
        }
    };
    
    match result {
        Ok(r) if !r.is_nan() && !r.is_infinite() => Value::Float(r),
        _ => Value::Null, // Domain error or invalid result
    }
}
```

**Acceptance Criteria**:
- [ ] `MATH('_ * 2', 21)` returns `42.0`
- [ ] `MATH('sqrt(a^2 + b^2)', 3, 4)` returns `5.0`
- [ ] `MATH('sin(_ * pi / 180)', 90)` returns `1.0`
- [ ] `MATH('log(_)', 0)` returns `NULL` (domain error)
- [ ] `MATH('invalid', 1)` returns `NULL` (parse error)

---

### Phase 4: Integration with Property Expressions (2-3 hours)

#### 4.1 Property-Based Math in RETURN

Enable using property expressions directly in math functions:

```sql
-- Calculate BMI from weight and height properties
MATCH (p:person)
RETURN p.name, p.weight / (p.height ^ 2) AS bmi

-- Using MATH function with properties
MATCH (p:player)  
RETURN p.name, MATH('sqrt(a^2 + b^2)', p.ppg, p.rpg) AS stat_magnitude

-- Complex calculations
MATCH (p:player)
RETURN p.name, 
       sqrt(pow(p.ppg - 20, 2) + pow(p.rpg - 10, 2)) AS distance_from_avg
```

#### 4.2 Math in WHERE Clauses

Support math expressions in predicates:

```sql
-- Find players with high stat magnitude
MATCH (p:player)
WHERE sqrt(p.ppg ^ 2 + p.rpg ^ 2) > 30
RETURN p.name

-- Using MATH in WHERE
MATCH (p:player)
WHERE MATH('sqrt(a^2 + b^2)', p.ppg, p.rpg) > 30
RETURN p.name
```

#### 4.3 Math in ORDER BY

Support sorting by calculated values:

```sql
MATCH (p:player)
RETURN p.name, sqrt(p.ppg ^ 2 + p.rpg ^ 2) AS magnitude
ORDER BY magnitude DESC
```

---

### Phase 5: Tests and Documentation (2-3 hours)

#### 5.1 Unit Tests

**File**: `src/gql/compiler.rs` (add to existing tests module)

```rust
#[cfg(test)]
mod math_tests {
    use super::*;
    
    #[test]
    fn test_power_operator() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        
        let results = snapshot.gql("MATCH (n) RETURN 2 ^ 3").unwrap();
        assert_eq!(results[0], Value::Float(8.0));
    }
    
    #[test]
    fn test_power_operator_with_property() {
        let graph = create_test_graph_with_numbers();
        let snapshot = graph.snapshot();
        
        let results = snapshot.gql("MATCH (n {value: 3}) RETURN n.value ^ 2").unwrap();
        assert_eq!(results[0], Value::Float(9.0));
    }
    
    #[test]
    fn test_sqrt_function() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        
        let results = snapshot.gql("MATCH (n) RETURN sqrt(16)").unwrap();
        assert_eq!(results[0], Value::Float(4.0));
    }
    
    #[test]
    fn test_sqrt_negative_returns_null() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        
        let results = snapshot.gql("MATCH (n) RETURN sqrt(-1)").unwrap();
        assert_eq!(results[0], Value::Null);
    }
    
    #[test]
    fn test_trig_functions() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        
        // sin(pi/2) = 1
        let results = snapshot.gql("MATCH (n) RETURN sin(pi() / 2)").unwrap();
        if let Value::Float(f) = &results[0] {
            assert!((f - 1.0).abs() < 0.0001);
        } else {
            panic!("Expected float");
        }
        
        // cos(0) = 1
        let results = snapshot.gql("MATCH (n) RETURN cos(0)").unwrap();
        assert_eq!(results[0], Value::Float(1.0));
    }
    
    #[test]
    fn test_math_function_basic() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        
        let results = snapshot.gql("MATCH (n) RETURN MATH('_ * 2', 21)").unwrap();
        assert_eq!(results[0], Value::Float(42.0));
    }
    
    #[test]
    fn test_math_function_with_variables() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        
        let results = snapshot.gql("MATCH (n) RETURN MATH('sqrt(a^2 + b^2)', 3, 4)").unwrap();
        assert_eq!(results[0], Value::Float(5.0));
    }
    
    #[test]
    fn test_math_function_with_properties() {
        let graph = create_test_graph_with_numbers();
        let snapshot = graph.snapshot();
        
        // Graph has nodes with x=3 and y=4
        let results = snapshot.gql(r#"
            MATCH (n {x: 3, y: 4}) 
            RETURN MATH('sqrt(a^2 + b^2)', n.x, n.y)
        "#).unwrap();
        assert_eq!(results[0], Value::Float(5.0));
    }
    
    #[test]
    fn test_math_in_where_clause() {
        let graph = create_test_graph_with_numbers();
        let snapshot = graph.snapshot();
        
        let results = snapshot.gql(r#"
            MATCH (n)
            WHERE sqrt(n.value ^ 2) > 5
            RETURN n.value
        "#).unwrap();
        
        for result in &results {
            if let Value::Int(v) = result {
                assert!(*v > 5 || *v < -5);
            }
        }
    }
    
    #[test]
    fn test_math_constants() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        
        let results = snapshot.gql("MATCH (n) RETURN pi()").unwrap();
        if let Value::Float(f) = &results[0] {
            assert!((f - std::f64::consts::PI).abs() < 0.0001);
        }
        
        let results = snapshot.gql("MATCH (n) RETURN e()").unwrap();
        if let Value::Float(f) = &results[0] {
            assert!((f - std::f64::consts::E).abs() < 0.0001);
        }
    }
    
    #[test]
    fn test_radians_degrees() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        
        let results = snapshot.gql("MATCH (n) RETURN degrees(pi())").unwrap();
        if let Value::Float(f) = &results[0] {
            assert!((f - 180.0).abs() < 0.0001);
        }
        
        let results = snapshot.gql("MATCH (n) RETURN radians(180)").unwrap();
        if let Value::Float(f) = &results[0] {
            assert!((f - std::f64::consts::PI).abs() < 0.0001);
        }
    }
}
```

#### 5.2 Integration Tests

**File**: `tests/gql.rs`

```rust
#[test]
fn test_gql_math_pythagorean() {
    let graph = create_nba_test_graph();
    let snapshot = graph.snapshot();
    
    // Calculate "stat magnitude" for players
    let results = snapshot.gql(r#"
        MATCH (p:player)
        RETURN p.name, sqrt(p.points_per_game ^ 2 + p.rebounds_per_game ^ 2) AS magnitude
        ORDER BY magnitude DESC
        LIMIT 5
    "#).unwrap();
    
    assert_eq!(results.len(), 5);
}

#[test]
fn test_gql_math_function_complex() {
    let graph = create_nba_test_graph();
    let snapshot = graph.snapshot();
    
    // Using MATH function for complex calculation
    let results = snapshot.gql(r#"
        MATCH (p:player)
        RETURN p.name, 
               MATH('log(a + 1) * b / 10', p.points_per_game, p.assists_per_game) AS score
        ORDER BY score DESC
        LIMIT 3
    "#).unwrap();
    
    assert_eq!(results.len(), 3);
}
```

#### 5.3 Snapshot Tests

**File**: `tests/gql_snapshots.rs`

```rust
#[test]
fn test_parse_power_operator_snapshot() {
    let ast = parse("MATCH (n) RETURN n.value ^ 2").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_sqrt_function_snapshot() {
    let ast = parse("MATCH (n) RETURN sqrt(n.x ^ 2 + n.y ^ 2)").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_math_function_snapshot() {
    let ast = parse("MATCH (n) RETURN MATH('_ * 2 + 1', n.value)").unwrap();
    assert_yaml_snapshot!(ast);
}
```

---

## API Reference

### New Operators

| Operator | Description | Example | Result Type |
|----------|-------------|---------|-------------|
| `^` | Power/exponentiation | `2 ^ 3` | Float |

### New Functions

| Function | Arguments | Description | Example |
|----------|-----------|-------------|---------|
| `sqrt(x)` | 1 numeric | Square root | `sqrt(16)` → `4.0` |
| `pow(base, exp)` | 2 numeric | Power | `pow(2, 10)` → `1024.0` |
| `log(x)` / `ln(x)` | 1 numeric (>0) | Natural logarithm | `log(e())` → `1.0` |
| `log10(x)` | 1 numeric (>0) | Base-10 logarithm | `log10(100)` → `2.0` |
| `exp(x)` | 1 numeric | e^x | `exp(1)` → `2.718...` |
| `sin(x)` | 1 numeric (radians) | Sine | `sin(pi()/2)` → `1.0` |
| `cos(x)` | 1 numeric (radians) | Cosine | `cos(0)` → `1.0` |
| `tan(x)` | 1 numeric (radians) | Tangent | `tan(0)` → `0.0` |
| `asin(x)` | 1 numeric (-1..1) | Arc sine | `asin(1)` → `1.57...` |
| `acos(x)` | 1 numeric (-1..1) | Arc cosine | `acos(1)` → `0.0` |
| `atan(x)` | 1 numeric | Arc tangent | `atan(1)` → `0.785...` |
| `atan2(y, x)` | 2 numeric | Two-argument arctangent | `atan2(1, 1)` → `0.785...` |
| `radians(deg)` | 1 numeric | Degrees to radians | `radians(180)` → `3.14...` |
| `degrees(rad)` | 1 numeric | Radians to degrees | `degrees(pi())` → `180.0` |
| `pi()` | 0 | Pi constant | `pi()` → `3.14159...` |
| `e()` | 0 | Euler's number | `e()` → `2.71828...` |
| `min(a, b, ...)` | 2+ numeric | Minimum of values | `min(3, 1, 4)` → `1.0` |
| `max(a, b, ...)` | 2+ numeric | Maximum of values | `max(3, 1, 4)` → `4.0` |
| `MATH(expr, ...)` | 1 string + N numeric | Evaluate mathexpr | `MATH('_ * 2', 21)` → `42.0` |

### MATH() Function Details

The `MATH()` function evaluates a mathematical expression string using the `mathexpr` crate.

**Syntax**: `MATH(expression_string, arg1, arg2, ...)`

**Variable Binding**:
- First argument after expression: bound to `_` (current value)
- Subsequent arguments: bound to `a`, `b`, `c`, ... in order

**Supported in Expression**:
- All operators: `+`, `-`, `*`, `/`, `%`, `^`
- All functions: `sqrt`, `sin`, `cos`, `tan`, `log`, `exp`, `pow`, `min`, `max`, `abs`, `floor`, `ceil`, `round`
- Constants: `pi`, `e`
- Parentheses for grouping

**Error Handling**:
- Parse errors → `NULL`
- Domain errors (sqrt of negative, log of non-positive) → `NULL`
- Non-numeric input → `NULL`
- NaN/Infinity results → `NULL`

---

## Example Queries

### Basic Math

```sql
-- Power operator
MATCH (p:player) 
RETURN p.name, p.ppg ^ 2 AS ppg_squared

-- Square root
MATCH (p:player)
RETURN p.name, sqrt(p.ppg) AS sqrt_ppg

-- Trigonometry (calculate angle)
MATCH (p:player)
RETURN p.name, degrees(atan2(p.rpg, p.ppg)) AS stat_angle
```

### Complex Calculations

```sql
-- Euclidean distance from average stats (20 PPG, 10 RPG)
MATCH (p:player)
RETURN p.name, 
       sqrt(pow(p.ppg - 20, 2) + pow(p.rpg - 10, 2)) AS distance_from_avg
ORDER BY distance_from_avg

-- Composite score using logarithmic scaling
MATCH (p:player)
RETURN p.name,
       log(p.ppg + 1) * 10 + sqrt(p.rpg) * 5 AS composite_score
ORDER BY composite_score DESC
```

### MATH() Function Usage

```sql
-- Simple calculation
MATCH (p:player)
RETURN MATH('_ * 1.1', p.ppg) AS adjusted_ppg

-- Multiple variables
MATCH (p:player)
RETURN MATH('sqrt(a^2 + b^2 + c^2)', p.ppg, p.rpg, p.apg) AS stat_magnitude

-- Complex formula
MATCH (p:player)
RETURN MATH('100 * (1 - exp(-_ / 30))', p.ppg) AS efficiency_score
```

### Filtering with Math

```sql
-- Find players with high "stat magnitude"
MATCH (p:player)
WHERE sqrt(p.ppg ^ 2 + p.rpg ^ 2) > 30
RETURN p.name

-- Filter by calculated efficiency
MATCH (p:player)
WHERE MATH('a / (b + 1)', p.ppg, p.minutes) > 0.5
RETURN p.name
```

---

## Exit Criteria

### Grammar Updates
- [ ] Power operator (`^`) added to grammar
- [ ] Precedence is correct (higher than `*`, `/`)
- [ ] All existing tests pass

### AST Updates
- [ ] `BinaryOperator::Pow` variant added
- [ ] Serialization works correctly

### Compiler Updates  
- [ ] Power operator evaluates correctly
- [ ] All new math functions implemented
- [ ] MATH() function works with mathexpr
- [ ] Domain errors return NULL
- [ ] Non-numeric inputs return NULL

### Testing
- [ ] Unit tests for all new operators/functions
- [ ] Integration tests with real queries
- [ ] Snapshot tests for new AST structures
- [ ] Edge cases (NULL, domain errors) tested

### Documentation
- [ ] Function reference in this spec
- [ ] Example queries documented
- [ ] Doc comments on new code

---

## File Summary

**Modified files**:
- `src/gql/grammar.pest` - Add `^` operator, power rule
- `src/gql/ast.rs` - Add `BinaryOperator::Pow`
- `src/gql/parser.rs` - Parse power expressions
- `src/gql/compiler.rs` - Evaluate power, add math functions, add MATH()

**Test files**:
- `src/gql/compiler.rs` - Unit tests in `mod math_tests`
- `tests/gql.rs` - Integration tests
- `tests/gql_snapshots.rs` - Snapshot tests

---

## References

- `specs/spec-07-math.md` - MathStep specification
- `src/traversal/transform/functional.rs` - MathStep implementation
- `mathexpr` crate documentation - Expression syntax
- `src/gql/compiler.rs` - Existing function implementations
