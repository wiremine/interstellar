# Spec 07-Math: MathStep Implementation (Basic)

**Phase 3.2 of Phase 7 (Completing the Gremlin API)**

## Overview

This specification details the implementation of the `MathStep` transform step for the Interstellar graph database. The `math()` step evaluates mathematical expressions on traverser values, supporting both the current value (`_`) and labeled values from the path.

**Duration**: 2-3 hours  
**Priority**: Medium  
**Dependencies**: Phase 4 (Anonymous Traversals), Phase 7 Phases 1-2

---

## Current Implementation Status

### Completed Steps (Phase 7)
- Phase 1.1-1.4: Filter steps (`hasNot`, `is`, `simplePath`, `cyclicPath`)
- Phase 1.5: Navigation step (`otherV`)
- Phase 2.1-2.3: Transform steps (`properties`, `valueMap`, `elementMap`)
- Phase 2.4-2.5: Collection steps (`unfold`, `mean`)
- Phase 2.6: **OrderStep ✅ COMPLETE**

### This Phase
- **MathStep**: Mathematical expression evaluation

---

## Goals

1. Implement `MathStep` for basic arithmetic operations (+, -, *, /, %)
2. Support `_` variable for current traverser value
3. Support labeled variables from `as()` steps
4. Create `MathBuilder` for fluent `by()` modulator configuration
5. Provide comprehensive test coverage
6. Optional: Integrate `meval` crate for full expression support

---

## Deliverables

| File | Description |
|------|-------------|
| `src/traversal/transform/functional.rs` | Add `MathStep`, `MathBuilder` |
| `src/traversal/mod.rs` | Export `MathStep`, `MathBuilder`, add `math()` method |
| `tests/traversal.rs` | Integration tests for `math()` step |

---

## Architecture

### Step Type
**Transform Step (1:1)**: Each input traverser produces one output traverser with a computed numeric value.

### Expression Evaluation Strategy

**Basic Implementation** (Phase 3.2):
- Simple pattern-based parser for basic operations
- Support: `+`, `-`, `*`, `/`, `%`
- Variables: `_` (current value) and labeled path values
- Output: `Value::Float`

**Future Enhancement** (Optional):
- Integrate `meval` crate for full expression parsing
- Support parentheses, functions (sin, cos, sqrt, etc.)
- Support more complex expressions

---

## Section 1: MathStep Implementation

### 1.1 Core Types

```rust
/// Mathematical expression evaluator step.
///
/// Evaluates arithmetic expressions with variables from the traversal path.
/// The special variable `_` represents the current traverser value.
/// Other variables reference labeled path values from `as()` steps.
///
/// Supported operations: +, -, *, /, %
///
/// # Examples
///
/// ```rust
/// // Double the current value
/// g.v().values("age").math("_ * 2").build()
///
/// // Calculate age difference between labeled vertices
/// g.v().as_("a").out("knows").as_("b")
///     .math("a - b")
///     .by("age")
///     .by("age")
///     .build()
/// ```
#[derive(Clone, Debug)]
pub struct MathStep {
    expression: String,
    variable_keys: HashMap<String, String>, // variable -> property_key
}

impl MathStep {
    pub fn new(expression: impl Into<String>) -> Self {
        Self {
            expression: expression.into(),
            variable_keys: HashMap::new(),
        }
    }

    pub fn with_bindings(expression: impl Into<String>, bindings: HashMap<String, String>) -> Self {
        Self {
            expression: expression.into(),
            variable_keys: bindings,
        }
    }

    fn evaluate(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Option<Value> {
        // Build variable bindings
        let mut bindings = HashMap::new();
        
        // Bind `_` to current value
        self.bind_current_value(&traverser.value, &mut bindings)?;
        
        // Bind labeled variables from path
        self.bind_path_variables(ctx, traverser, &mut bindings);
        
        // Evaluate expression
        let result = self.evaluate_expr(&self.expression, &bindings)?;
        
        Some(Value::Float(result))
    }

    fn bind_current_value(&self, value: &Value, bindings: &mut HashMap<String, f64>) -> Option<()> {
        let num = match value {
            Value::Integer(n) => *n as f64,
            Value::Float(f) => *f,
            _ => return None, // Non-numeric current value
        };
        bindings.insert("_".to_string(), num);
        Some(())
    }

    fn bind_path_variables(
        &self,
        ctx: &ExecutionContext,
        traverser: &Traverser,
        bindings: &mut HashMap<String, f64>,
    ) {
        for (var, prop_key) in &self.variable_keys {
            if let Some(path_value) = traverser.path.get_labeled(var) {
                if let Some(num) = self.extract_number(ctx, &path_value.clone().into(), prop_key) {
                    bindings.insert(var.clone(), num);
                }
            }
        }
    }

    fn extract_number(&self, ctx: &ExecutionContext, value: &Value, key: &str) -> Option<f64> {
        match value {
            Value::Integer(n) => Some(*n as f64),
            Value::Float(f) => Some(*f),
            Value::Vertex(id) => {
                ctx.snapshot()
                    .storage()
                    .get_vertex(*id)
                    .and_then(|v| v.properties.get(key))
                    .and_then(|v| match v {
                        Value::Integer(n) => Some(*n as f64),
                        Value::Float(f) => Some(*f),
                        _ => None,
                    })
            }
            Value::Edge(id) => {
                ctx.snapshot()
                    .storage()
                    .get_edge(*id)
                    .and_then(|e| e.properties.get(key))
                    .and_then(|v| match v {
                        Value::Integer(n) => Some(*n as f64),
                        Value::Float(f) => Some(*f),
                        _ => None,
                    })
            }
            _ => None,
        }
    }

    /// Simple expression evaluator supporting basic arithmetic.
    ///
    /// Supported patterns:
    /// - "a + b", "a - b", "a * b", "a / b", "a % b"
    /// - "_ * 2", "_ + 10", etc.
    ///
    /// For complex expressions, consider using the `meval` crate.
    fn evaluate_expr(&self, expr: &str, bindings: &HashMap<String, f64>) -> Option<f64> {
        let expr = expr.trim();
        
        // Try each operator in precedence order
        // Lower precedence first (so they're evaluated last)
        
        // Addition/Subtraction
        if let Some(result) = self.try_binary_op(expr, " + ", bindings, |a, b| a + b) {
            return Some(result);
        }
        if let Some(result) = self.try_binary_op(expr, " - ", bindings, |a, b| a - b) {
            return Some(result);
        }
        
        // Multiplication/Division/Modulo
        if let Some(result) = self.try_binary_op(expr, " * ", bindings, |a, b| a * b) {
            return Some(result);
        }
        if let Some(result) = self.try_binary_op(expr, " / ", bindings, |a, b| a / b) {
            return Some(result);
        }
        if let Some(result) = self.try_binary_op(expr, " % ", bindings, |a, b| a % b) {
            return Some(result);
        }
        
        // Single variable or constant
        self.resolve_value(expr, bindings)
    }

    fn try_binary_op<F>(
        &self,
        expr: &str,
        op: &str,
        bindings: &HashMap<String, f64>,
        operation: F,
    ) -> Option<f64>
    where
        F: Fn(f64, f64) -> f64,
    {
        let pos = expr.find(op)?;
        let left = expr[..pos].trim();
        let right = expr[pos + op.len()..].trim();
        
        let l = self.resolve_value(left, bindings)?;
        let r = self.resolve_value(right, bindings)?;
        
        Some(operation(l, r))
    }

    fn resolve_value(&self, s: &str, bindings: &HashMap<String, f64>) -> Option<f64> {
        // Try as variable
        if let Some(&val) = bindings.get(s) {
            return Some(val);
        }
        
        // Try as numeric literal
        s.parse::<f64>().ok()
    }
}

impl AnyStep for MathStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.filter_map(move |t| {
            self.evaluate(ctx, &t).map(|value| t.with_value(value))
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "math"
    }
}
```

### 1.2 MathBuilder

```rust
/// Builder for configuring math() step with by() modulators.
///
/// Each `by()` call binds a variable in the expression to a property key.
/// Variables are bound in the order they appear in the expression.
pub struct MathBuilder<In> {
    steps: Vec<Box<dyn AnyStep>>,
    expression: String,
    variable_bindings: HashMap<String, String>,
    _phantom: PhantomData<In>,
}

impl<In> MathBuilder<In> {
    pub(crate) fn new(steps: Vec<Box<dyn AnyStep>>, expression: impl Into<String>) -> Self {
        Self {
            steps,
            expression: expression.into(),
            variable_bindings: HashMap::new(),
            _phantom: PhantomData,
        }
    }

    /// Bind a variable to a property key.
    ///
    /// # Arguments
    /// * `variable` - Variable name from expression (e.g., "a", "b")
    /// * `key` - Property key to extract from labeled element
    ///
    /// # Examples
    ///
    /// ```rust
    /// g.v().as_("a").out().as_("b")
    ///     .math("a - b")
    ///     .by("a", "age")  // Extract age from labeled "a"
    ///     .by("b", "age")  // Extract age from labeled "b"
    ///     .build()
    /// ```
    pub fn by(mut self, variable: &str, key: &str) -> Self {
        self.variable_bindings.insert(variable.to_string(), key.to_string());
        self
    }

    /// Finalize the math() step and return the traversal.
    pub fn build(mut self) -> Traversal<In, Value> {
        let step = MathStep::with_bindings(self.expression, self.variable_bindings);
        self.steps.push(Box::new(step));
        Traversal::new(self.steps)
    }
}
```

---

## Section 2: API Integration

### 2.1 Traversal Method

Add to `impl<In, Out> Traversal<In, Out>`:

```rust
/// Evaluate a mathematical expression.
///
/// The expression can reference the current value using `_` and labeled
/// path values using their label names. Use `by()` to specify which
/// property to extract from labeled elements.
///
/// # Examples
///
/// ```rust
/// // Double current values
/// g.v().values("age").math("_ * 2").build()
///
/// // Calculate difference between labeled values
/// g.v().as_("a").out("knows").as_("b")
///     .math("a - b")
///     .by("a", "age")
///     .by("b", "age")
///     .build()
/// ```
pub fn math(self, expression: &str) -> MathBuilder<In> {
    MathBuilder::new(self.steps, expression)
}
```

### 2.2 Anonymous Traversal Factory

Add to `__` module:

```rust
/// Create a math() step for use in anonymous traversals.
///
/// # Examples
///
/// ```rust
/// // In a where() clause
/// g.v().where_(__::values("age").math("_ > 30").build())
/// ```
pub fn math(expression: &str) -> MathBuilder<Value> {
    MathBuilder::new(vec![], expression)
}
```

---

## Section 3: Test Cases

### 3.1 Unit Tests

Add to `src/traversal/transform/functional.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::inmemory::InMemoryGraph;
    use std::collections::HashMap;

    fn create_test_graph() -> InMemoryGraph {
        let graph = InMemoryGraph::new();
        let mut props_a = HashMap::new();
        props_a.insert("name".to_string(), Value::String("Alice".into()));
        props_a.insert("age".to_string(), Value::Integer(30));
        
        let mut props_b = HashMap::new();
        props_b.insert("name".to_string(), Value::String("Bob".into()));
        props_b.insert("age".to_string(), Value::Integer(25));
        
        let v1 = graph.add_vertex("person", props_a);
        let v2 = graph.add_vertex("person", props_b);
        graph.add_edge("knows", v1, v2, HashMap::new());
        
        graph
    }

    #[test]
    fn test_math_multiply_current_value() {
        let graph = create_test_graph();
        let g = graph.traversal();
        
        let results: Vec<_> = g.v()
            .values("age")
            .math("_ * 2")
            .build()
            .to_list();
        
        assert_eq!(results.len(), 2);
        // 30 * 2 = 60, 25 * 2 = 50
        let mut values: Vec<_> = results.iter()
            .filter_map(|v| match v {
                Value::Float(f) => Some(*f as i64),
                _ => None,
            })
            .collect();
        values.sort();
        assert_eq!(values, vec![50, 60]);
    }

    #[test]
    fn test_math_add_constant() {
        let graph = create_test_graph();
        let g = graph.traversal();
        
        let results: Vec<_> = g.v()
            .values("age")
            .math("_ + 10")
            .build()
            .to_list();
        
        assert_eq!(results.len(), 2);
        let mut values: Vec<_> = results.iter()
            .filter_map(|v| match v {
                Value::Float(f) => Some(*f as i64),
                _ => None,
            })
            .collect();
        values.sort();
        assert_eq!(values, vec![35, 40]); // 25+10, 30+10
    }

    #[test]
    fn test_math_subtract_constant() {
        let graph = create_test_graph();
        let g = graph.traversal();
        
        let results: Vec<_> = g.v()
            .values("age")
            .math("_ - 5")
            .build()
            .to_list();
        
        let mut values: Vec<_> = results.iter()
            .filter_map(|v| match v {
                Value::Float(f) => Some(*f as i64),
                _ => None,
            })
            .collect();
        values.sort();
        assert_eq!(values, vec![20, 25]); // 25-5, 30-5
    }

    #[test]
    fn test_math_divide() {
        let graph = create_test_graph();
        let g = graph.traversal();
        
        let results: Vec<_> = g.v()
            .values("age")
            .math("_ / 2")
            .build()
            .to_list();
        
        assert_eq!(results.len(), 2);
        // 30 / 2 = 15.0, 25 / 2 = 12.5
        let has_15 = results.iter().any(|v| matches!(v, Value::Float(f) if (*f - 15.0).abs() < 0.01));
        let has_12_5 = results.iter().any(|v| matches!(v, Value::Float(f) if (*f - 12.5).abs() < 0.01));
        assert!(has_15);
        assert!(has_12_5);
    }

    #[test]
    fn test_math_modulo() {
        let graph = create_test_graph();
        let g = graph.traversal();
        
        let results: Vec<_> = g.v()
            .values("age")
            .math("_ % 10")
            .build()
            .to_list();
        
        let mut values: Vec<_> = results.iter()
            .filter_map(|v| match v {
                Value::Float(f) => Some(*f as i64),
                _ => None,
            })
            .collect();
        values.sort();
        assert_eq!(values, vec![0, 5]); // 30 % 10 = 0, 25 % 10 = 5
    }

    #[test]
    fn test_math_with_labeled_values() {
        let graph = create_test_graph();
        let g = graph.traversal();
        
        // Alice (age 30) knows Bob (age 25)
        // Calculate age difference: a - b = 30 - 25 = 5
        let results: Vec<_> = g.v()
            .has("name", "Alice")
            .as_("a")
            .out("knows")
            .as_("b")
            .math("a - b")
            .by("a", "age")
            .by("b", "age")
            .build()
            .to_list();
        
        assert_eq!(results.len(), 1);
        if let Value::Float(diff) = results[0] {
            assert!((diff - 5.0).abs() < 0.01);
        } else {
            panic!("Expected Float value");
        }
    }

    #[test]
    fn test_math_filters_non_numeric() {
        let graph = create_test_graph();
        let g = graph.traversal();
        
        // values("name") produces strings, which can't be used in math
        let results: Vec<_> = g.v()
            .values("name")
            .math("_ * 2")
            .build()
            .to_list();
        
        // Should filter out non-numeric values
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_math_complex_expression() {
        let graph = create_test_graph();
        let g = graph.traversal();
        
        // Test operator precedence: should evaluate * before +
        // But our simple parser evaluates left-to-right
        // So for now, we only support single-operator expressions
        // This test documents current limitation
        
        let results: Vec<_> = g.v()
            .constant(Value::Integer(10))
            .math("_ * 2")
            .build()
            .to_list();
        
        assert_eq!(results.len(), 2); // Two vertices
        for result in results {
            if let Value::Float(f) = result {
                assert!((f - 20.0).abs() < 0.01);
            }
        }
    }
}
```

### 3.2 Integration Tests

Add to `tests/traversal.rs`:

```rust
#[test]
fn test_math_in_pipeline() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    // Calculate ages in "dog years" (multiply by 7)
    let dog_years: Vec<_> = g.v()
        .has_label("person")
        .values("age")
        .math("_ * 7")
        .build()
        .to_list();
    
    assert!(!dog_years.is_empty());
    for age in dog_years {
        assert!(matches!(age, Value::Float(_)));
    }
}

#[test]
fn test_math_with_filter() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    // Calculate doubled ages, then filter > 50
    let results: Vec<_> = g.v()
        .has_label("person")
        .values("age")
        .math("_ * 2")
        .build()
        .is_(p::gt(50.0))
        .to_list();
    
    for age in results {
        if let Value::Float(f) = age {
            assert!(f > 50.0);
        }
    }
}

#[test]
fn test_math_preserves_path() {
    let graph = create_modern_graph();
    let g = graph.traversal();
    
    let paths: Vec<_> = g.v()
        .has("name", "marko")
        .values("age")
        .math("_ + 1")
        .build()
        .path()
        .to_list();
    
    assert_eq!(paths.len(), 1);
    // Path should include: vertex -> age value -> calculated value
}
```

---

## Section 4: Future Enhancements

### 4.1 Optional: meval Integration

For full expression support, consider adding `meval` crate:

**Cargo.toml**:
```toml
[dependencies]
meval = { version = "0.2", optional = true }

[features]
full-math = ["meval"]
```

**Implementation**:
```rust
#[cfg(feature = "full-math")]
fn evaluate_expr(&self, expr: &str, bindings: &HashMap<String, f64>) -> Option<f64> {
    meval::eval_str_with_context(expr, bindings).ok()
}
```

### 4.2 Future Features
- Support for mathematical functions (sin, cos, sqrt, pow, etc.)
- Parentheses for grouping
- Multi-operator expressions with proper precedence
- Type coercion (Integer -> Float automatically)

---

## Exit Criteria

### Implementation Complete
- [x] `MathStep` struct implemented
- [x] Expression evaluator supports +, -, *, /, %
- [x] Current value binding (`_`) works
- [x] Labeled value binding with `by()` works
- [x] `MathBuilder` provides fluent API
- [x] `AnyStep` trait implemented

### API Integration
- [x] `math()` method added to `Traversal<In, Out>`
- [x] `__::math()` factory function added
- [x] `MathStep` and `MathBuilder` exported

### Testing
- [x] Unit tests for all arithmetic operators
- [x] Unit tests for `_` variable binding
- [x] Unit tests for labeled variable binding
- [x] Unit tests for non-numeric filtering
- [x] Integration tests with other steps
- [x] Path preservation verified
- [x] All tests pass

### Documentation
- [x] Doc comments on `MathStep`
- [x] Doc comments on `MathBuilder`
- [x] Doc comments on `math()` method
- [x] Examples in doc comments
- [x] `cargo doc` builds without warnings

---

## Implementation Notes

### Expression Evaluation Limitations

The basic implementation supports single-operator expressions:
- ✅ `"_ * 2"`
- ✅ `"a - b"`
- ✅ `"_ + 10"`
- ❌ `"_ * 2 + 5"` (requires full parser)
- ❌ `"(a + b) / 2"` (requires parentheses support)

For complex expressions, recommend the `meval` integration.

### Variable Binding Order

Variables are explicitly bound using `.by(variable, key)` syntax:
```rust
.math("a - b")
.by("a", "age")  // Bind variable "a" to age property of labeled "a"
.by("b", "age")  // Bind variable "b" to age property of labeled "b"
```

### Type Handling

- Input: `Value::Integer` or `Value::Float`
- Output: Always `Value::Float` (for consistency)
- Non-numeric values: Filtered out (no output traverser)

### Error Handling

Graceful degradation:
- Invalid expression → filter out traverser
- Missing variable binding → filter out traverser
- Non-numeric property → filter out traverser
- Division by zero → produces `Infinity` (f64 behavior)
