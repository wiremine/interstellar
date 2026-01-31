# Plan 07-Math: MathStep Implementation (Basic)

**Phase 3.2 of Phase 7 (Completing the Gremlin API)**

Based on: `specs/spec-07-math.md`

---

## Overview

This plan provides a granular, step-by-step implementation guide for the `MathStep` transform step. This is a focused subset of Phase 7, implementing mathematical expression evaluation with basic arithmetic operations.

**Total Duration**: 2-3 hours  
**Current State**: OrderStep complete (Phase 2.6). Ready to implement MathStep.

---

## Implementation Order

### Phase 1: Core MathStep Implementation (1.5 hours)

#### Task 1.1: Create MathStep Struct and Basic Evaluation
**File**: `src/traversal/transform/functional.rs` (or create `src/traversal/transform/math.rs`)  
**Duration**: 30 minutes

**Implementation Steps**:
1. Add necessary imports (HashMap, ExecutionContext, Traverser, Value)
2. Define `MathStep` struct with `expression: String` and `variable_keys: HashMap<String, String>`
3. Implement `MathStep::new()` constructor
4. Implement `MathStep::with_bindings()` constructor
5. Stub out `evaluate()` method signature

**Code to Write**:
```rust
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
        // To be implemented
        todo!()
    }
}
```

**Acceptance Criteria**:
- [x] `MathStep` struct compiles
- [x] Constructors work correctly
- [x] Struct has proper Debug and Clone derives

---

#### Task 1.2: Implement Variable Binding
**File**: `src/traversal/transform/functional.rs`  
**Duration**: 30 minutes

**Implementation Steps**:
1. Implement `bind_current_value()` - extract numeric value from current traverser value
2. Implement `bind_path_variables()` - extract labeled values from path
3. Implement `extract_number()` - get numeric value from Value, handling Vertex/Edge properties
4. Wire up `evaluate()` to call binding methods

**Code to Write**:
```rust
impl MathStep {
    fn bind_current_value(&self, value: &Value, bindings: &mut HashMap<String, f64>) -> Option<()> {
        let num = match value {
            Value::Integer(n) => *n as f64,
            Value::Float(f) => *f,
            _ => return None,
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
        // Implementation from spec
    }
}
```

**Acceptance Criteria**:
- [x] `_` variable binds to Integer values
- [x] `_` variable binds to Float values
- [x] Non-numeric current values return None
- [x] Labeled vertex properties are extracted correctly
- [x] Labeled edge properties are extracted correctly

---

#### Task 1.3: Implement Expression Evaluator
**File**: `src/traversal/transform/functional.rs`  
**Duration**: 30 minutes

**Implementation Steps**:
1. Implement `evaluate_expr()` method
2. Implement `try_binary_op()` helper for parsing binary operations
3. Implement `resolve_value()` helper for variable/literal lookup
4. Handle operators: +, -, *, /, %
5. Complete `evaluate()` method to call expression evaluator

**Code to Write**:
```rust
impl MathStep {
    fn evaluate_expr(&self, expr: &str, bindings: &HashMap<String, f64>) -> Option<f64> {
        let expr = expr.trim();
        
        // Try operators (order matters for precedence)
        if let Some(result) = self.try_binary_op(expr, " + ", bindings, |a, b| a + b) {
            return Some(result);
        }
        if let Some(result) = self.try_binary_op(expr, " - ", bindings, |a, b| a - b) {
            return Some(result);
        }
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
```

**Acceptance Criteria**:
- [x] Addition works: `"_ + 10"`
- [x] Subtraction works: `"_ - 5"`
- [x] Multiplication works: `"_ * 2"`
- [x] Division works: `"_ / 2"`
- [x] Modulo works: `"_ % 10"`
- [x] Variable references work: `"a - b"`
- [x] Numeric literals work: `"10 + 5"`

---

### Phase 2: AnyStep Implementation and Builder (45 minutes)

#### Task 2.1: Implement AnyStep Trait
**File**: `src/traversal/transform/functional.rs`  
**Duration**: 15 minutes

**Implementation Steps**:
1. Implement `AnyStep::apply()` for MathStep
2. Use `filter_map` to handle Option<Value> from evaluate()
3. Implement `clone_box()` method
4. Implement `name()` method returning "math"

**Code to Write**:
```rust
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

**Acceptance Criteria**:
- [x] `AnyStep` trait compiles
- [x] Iterator returns Value::Float results
- [x] Non-numeric values are filtered out
- [x] Step is clonable

---

#### Task 2.2: Implement MathBuilder
**File**: `src/traversal/transform/functional.rs`  
**Duration**: 30 minutes

**Implementation Steps**:
1. Define `MathBuilder<In>` struct with PhantomData
2. Implement `MathBuilder::new()` internal constructor
3. Implement `.by(variable, key)` method to add variable bindings
4. Implement `.build()` method to create MathStep and return Traversal
5. Add doc comments with examples

**Code to Write**:
```rust
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

    pub fn by(mut self, variable: &str, key: &str) -> Self {
        self.variable_bindings.insert(variable.to_string(), key.to_string());
        self
    }

    pub fn build(mut self) -> Traversal<In, Value> {
        let step = MathStep::with_bindings(self.expression, self.variable_bindings);
        self.steps.push(Box::new(step));
        Traversal::new(self.steps)
    }
}
```

**Acceptance Criteria**:
- [x] `MathBuilder::new()` creates builder
- [x] `.by()` method chains correctly
- [x] `.build()` produces valid Traversal
- [x] Variable bindings are passed to MathStep
- [x] Type inference works with PhantomData

---

### Phase 3: API Integration and Testing (45 minutes)

#### Task 3.1: Add Traversal Method
**File**: `src/traversal/mod.rs`  
**Duration**: 10 minutes

**Implementation Steps**:
1. Add `math()` method to `impl<In, Out> Traversal<In, Out>`
2. Add doc comments with examples
3. Return `MathBuilder<In>`

**Code to Write**:
```rust
impl<In, Out> Traversal<In, Out> {
    // ... existing methods ...

    /// Evaluate a mathematical expression.
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
}
```

**Acceptance Criteria**:
- [x] Method compiles
- [x] Type signature is correct
- [x] Doc examples are valid
- [x] Method is accessible on Traversal

---

#### Task 3.2: Add Anonymous Traversal Factory
**File**: `src/traversal/mod.rs`  
**Duration**: 5 minutes

**Implementation Steps**:
1. Add `math()` function to `__` module
2. Return `MathBuilder<Value>`

**Code to Write**:
```rust
pub mod __ {
    // ... existing functions ...

    pub fn math(expression: &str) -> MathBuilder<Value> {
        MathBuilder::new(vec![], expression)
    }
}
```

**Acceptance Criteria**:
- [x] Function compiles
- [x] Returns correct type
- [x] Can be used in anonymous traversals

---

#### Task 3.3: Update Module Exports
**File**: `src/traversal/mod.rs` and `src/traversal/transform/mod.rs`  
**Duration**: 5 minutes

**Implementation Steps**:
1. Export `MathStep` from `transform/functional.rs`
2. Export `MathBuilder` from `transform/functional.rs`
3. Re-export from `traversal/mod.rs`

**Acceptance Criteria**:
- [x] `MathStep` is publicly accessible
- [x] `MathBuilder` is publicly accessible
- [x] `cargo build` succeeds

---

#### Task 3.4: Write Unit Tests
**File**: `src/traversal/transform/functional.rs` (tests module)  
**Duration**: 15 minutes

**Implementation Steps**:
1. Create test helper to build simple graph
2. Test `_ * 2` - multiply by constant
3. Test `_ + 10` - add constant
4. Test `_ - 5` - subtract constant
5. Test `_ / 2` - division
6. Test `_ % 10` - modulo
7. Test labeled variables: `a - b`
8. Test non-numeric filtering

**Acceptance Criteria**:
- [x] All 8+ unit tests pass
- [x] Tests cover all operators
- [x] Tests cover both `_` and labeled variables
- [x] Tests verify non-numeric values are filtered

---

#### Task 3.5: Write Integration Tests
**File**: `tests/traversal.rs`  
**Duration**: 10 minutes

**Implementation Steps**:
1. Test math in pipeline with other steps
2. Test math with filter step (is_)
3. Test path preservation through math
4. Test math with anonymous traversal

**Acceptance Criteria**:
- [x] Integration tests pass
- [x] Math works with other steps
- [x] Path tracking preserved

---

## Exit Criteria Checklist

### Core Implementation
- [x] `MathStep` struct defined with expression and variable_keys
- [x] Variable binding for `_` (current value) works
- [x] Variable binding for labeled values works
- [x] Expression evaluator supports +, -, *, /, %
- [x] `AnyStep` trait implemented correctly
- [x] Non-numeric values filtered gracefully

### Builder Pattern
- [x] `MathBuilder<In>` struct defined
- [x] `.by(variable, key)` method works
- [x] `.build()` method produces Traversal
- [x] Builder chains correctly

### API Integration
- [x] `math()` method on `Traversal<In, Out>`
- [x] `__::math()` factory function
- [x] `MathStep` and `MathBuilder` exported
- [x] Module structure correct

### Testing
- [x] All unit tests pass (8+ tests)
- [x] All integration tests pass (3+ tests)
- [x] Test coverage for all operators
- [x] Test coverage for both binding modes
- [x] Edge cases tested (non-numeric, missing labels)

### Documentation
- [x] Doc comments on `MathStep`
- [x] Doc comments on `MathBuilder`
- [x] Doc comments on `math()` method
- [x] Examples in doc comments
- [x] `cargo doc` builds without warnings

---

## Implementation Notes

### File Organization

**Option 1**: Add to existing `src/traversal/transform/functional.rs`
- Pros: Keeps related transform steps together
- Cons: File may get large

**Option 2**: Create new `src/traversal/transform/math.rs`
- Pros: Cleaner separation of concerns
- Cons: More files to manage

**Recommendation**: Start with Option 1 (add to functional.rs). If the file grows beyond ~1000 lines, split into separate file.

### Expression Parsing Limitations

This basic implementation has known limitations:
- ❌ No operator precedence (single-operator expressions only)
- ❌ No parentheses support
- ❌ No function calls (sin, cos, sqrt, etc.)
- ✅ Simple binary operations work
- ✅ Variable references work
- ✅ Numeric literals work

Document these limitations in the doc comments.

### Testing Strategy

**Unit Tests** (in implementation file):
- Test each operator independently
- Test variable binding
- Test edge cases

**Integration Tests** (in tests/traversal.rs):
- Test in real graph traversals
- Test with other steps
- Test path preservation

### Common Pitfalls

1. **Spaces in expressions**: Require spaces around operators (`"_ * 2"` not `"_*2"`)
2. **Type coercion**: Always return Float, even for Integer inputs
3. **Missing bindings**: Filter out traversers when variables can't be resolved
4. **Division by zero**: f64 naturally handles this (returns Infinity)

---

## Optional Future Work

### meval Integration (Not in this phase)

For users who need complex expressions:

```toml
[dependencies]
meval = { version = "0.2", optional = true }

[features]
full-math = ["meval"]
```

```rust
#[cfg(feature = "full-math")]
fn evaluate_expr(&self, expr: &str, bindings: &HashMap<String, f64>) -> Option<f64> {
    meval::eval_str_with_context(expr, bindings).ok()
}

#[cfg(not(feature = "full-math"))]
fn evaluate_expr(&self, expr: &str, bindings: &HashMap<String, f64>) -> Option<f64> {
    // Current basic implementation
}
```

This can be added in a future phase without breaking existing code.

---

## Validation

Before marking this phase complete:

1. Run `cargo test` - all tests pass
2. Run `cargo clippy -- -D warnings` - no warnings
3. Run `cargo fmt --check` - code is formatted
4. Run `cargo doc --open` - documentation builds and looks correct
5. Manually test example from spec:
   ```rust
   g.v().values("age").math("_ * 2").build().to_list()
   ```
6. Verify labeled variables work:
   ```rust
   g.v().as_("a").out().as_("b")
       .math("a - b")
       .by("a", "age")
       .by("b", "age")
       .build()
       .to_list()
   ```

---

## Time Budget

| Task | Estimated | Actual |
|------|-----------|--------|
| 1.1: MathStep struct | 30 min | ___ |
| 1.2: Variable binding | 30 min | ___ |
| 1.3: Expression evaluator | 30 min | ___ |
| 2.1: AnyStep impl | 15 min | ___ |
| 2.2: MathBuilder | 30 min | ___ |
| 3.1: Traversal method | 10 min | ___ |
| 3.2: Factory function | 5 min | ___ |
| 3.3: Exports | 5 min | ___ |
| 3.4: Unit tests | 15 min | ___ |
| 3.5: Integration tests | 10 min | ___ |
| **Total** | **2h 30m** | ___ |

Buffer: 30 minutes for debugging and polish

---

## Success Criteria

This phase is complete when:

1. ✅ All code compiles without warnings
2. ✅ All tests pass (unit + integration)
3. ✅ Documentation is complete and accurate
4. ✅ `cargo clippy` shows no warnings
5. ✅ Manual testing of examples succeeds
6. ✅ Code follows project style guidelines
7. ✅ Coverage of new code is 100%

Upon completion, update `plans/plan-07.md` to mark Phase 3.2 as complete.
