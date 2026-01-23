# Spec 27: GQL Compiler Refactoring

## Overview

The `src/gql/compiler.rs` file has grown to **9,306 lines** with **166 methods**, making it difficult to navigate, maintain, and extend. This specification outlines a plan to refactor the compiler into a well-organized module structure while maintaining full backward compatibility and 100% test pass rate.

## Current State Analysis

### File Statistics
- **Total lines**: 9,306
- **Methods in `impl Compiler`**: ~130
- **Standalone functions**: ~12
- **Test functions**: ~60 (embedded in file)
- **Structs/Enums**: 4 (`Compiler`, `BindingInfo`, `ListPredicateKind`, `ComparableValue`)

### Logical Groupings

After analyzing the codebase, methods can be grouped into these categories:

| Category | Lines (approx) | Method Count | Description |
|----------|---------------|--------------|-------------|
| Public API | 193-416 | 6 | `compile()`, `compile_with_params()`, etc. |
| Core Compiler | 417-720 | 8 | `Compiler` struct, `new()`, `compile()`, variable tracking |
| Clause Execution | 721-1700 | 15 | UNWIND, LET, WITH clause handling |
| CALL Subquery | 1700-2158 | 12 | Correlated/uncorrelated CALL execution |
| Row-based Expression Eval | 2159-3187 | 12 | `evaluate_*_from_row()` methods |
| Pattern Compilation | 3188-3546 | 8 | `compile_pattern()`, node/edge compilation |
| Return Execution | 3547-3646 | 3 | `execute_return()`, `execute_multi_var_return()` |
| Optional Match | 3647-3952 | 5 | OPTIONAL MATCH handling |
| Path-based Expression Eval | 3953-4923 | 12 | `evaluate_*_from_path()` methods |
| Element-based Expression Eval | 4924-6116 | 18 | `evaluate_*()` single-element methods |
| Exists & Filters | 6117-6210 | 3 | EXISTS patterns, node/edge filters |
| Variable Validation | 6211-6405 | 1 | `validate_expression_variables()` |
| Aggregation & GROUP BY | 6406-7809 | 25 | All aggregation, grouping, HAVING |
| Math Evaluation | 7810-7968 | 4 | Math expression handling |
| Helper Types & Functions | 7969-8425 | 10 | `ComparableValue`, `apply_binary_op()`, etc. |
| Tests | 8426-9306 | ~60 | Embedded unit tests |

---

## Proposed Module Structure

```
src/gql/
├── compiler/
│   ├── mod.rs              # Re-exports, Compiler struct, public API
│   ├── core.rs             # Core compile logic, variable tracking
│   ├── clauses.rs          # UNWIND, LET, WITH clause execution
│   ├── call.rs             # CALL subquery handling
│   ├── pattern.rs          # Pattern compilation (nodes, edges, quantifiers)
│   ├── optional.rs         # OPTIONAL MATCH handling
│   ├── aggregation.rs      # GROUP BY, aggregates, HAVING
│   ├── expression/
│   │   ├── mod.rs          # Expression evaluation traits/common code
│   │   ├── row.rs          # Row-based evaluation (evaluate_*_from_row)
│   │   ├── path.rs         # Path-based evaluation (evaluate_*_from_path)
│   │   └── element.rs      # Single-element evaluation (evaluate_*)
│   ├── functions.rs        # Function call evaluation (string, math, type)
│   ├── math.rs             # Math expression evaluation
│   └── helpers.rs          # ComparableValue, binary ops, comparisons
├── compiler.rs             # Backward compatibility re-exports (thin wrapper)
└── ... (existing files)
```

---

## Implementation Phases

### Phase 1: Infrastructure Setup (Low Risk)

**Goal**: Create module structure without moving any code.

1. Create `src/gql/compiler/` directory
2. Create `mod.rs` with re-exports from current `compiler.rs`
3. Verify all tests pass
4. Update `src/gql/mod.rs` to use new module path

**Files to create**:
```rust
// src/gql/compiler/mod.rs
//! GQL Compiler module - transforms GQL AST to traversal execution.

// Re-export everything from the monolithic file during migration
#[path = "../compiler_legacy.rs"]
mod legacy;

pub use legacy::*;
```

**Verification**: `cargo test` passes, no API changes

---

### Phase 2: Extract Helper Types & Functions (Low Risk)

**Goal**: Move standalone types and functions that have no internal dependencies.

**Target file**: `src/gql/compiler/helpers.rs`

**Items to move**:
```rust
// Types
struct ComparableValue(Value);
impl PartialEq for ComparableValue { ... }
impl Eq for ComparableValue {}
impl std::hash::Hash for ComparableValue { ... }
impl From<Value> for ComparableValue { ... }
impl From<ComparableValue> for Value { ... }

// Functions
fn apply_comparison(op: BinaryOperator, left: &Value, right: &Value) -> bool;
fn apply_binary_op(op: BinaryOperator, left: Value, right: Value) -> Value;
fn compare_values(left: &Value, right: &Value) -> std::cmp::Ordering;
fn value_to_bool(val: &Value) -> bool;
fn value_to_string(val: &Value) -> String;
fn extract_property_from_snapshot<'g>(...) -> Value;
fn eval_inline_value<'g>(...) -> Value;
fn eval_inline_predicate<'g>(...) -> bool;
```

**Lines**: ~7969-8425 (~456 lines)

**Verification**: `cargo test` passes

---

### Phase 3: Extract Math Evaluation (Low Risk)

**Goal**: Move math expression evaluation to dedicated module.

**Target file**: `src/gql/compiler/math.rs`

**Items to move**:
```rust
impl Compiler {
    fn evaluate_math_from_row(&self, args: &[Expression], row: &HashMap<String, Value>) -> Value;
    fn evaluate_math_from_path(&self, args: &[Expression], path: &[Value], ...) -> Value;
    fn evaluate_math(&self, args: &[Expression], element: &Value) -> Value;
    fn evaluate_math_expr_internal(&self, expr_string: &str, var_values: &[f64]) -> Value;
}
```

**Lines**: ~7810-7968 (~158 lines)

**Approach**: Create trait or use `impl Compiler` extension pattern with `pub(super)` visibility.

---

### Phase 4: Extract Pattern Compilation (Medium Risk)

**Goal**: Move pattern matching logic to dedicated module.

**Target file**: `src/gql/compiler/pattern.rs`

**Items to move**:
```rust
impl Compiler {
    fn compile_pattern(&mut self, pattern: &Pattern, ...) -> Result<...>;
    fn compile_node(&mut self, node: &NodePattern, ...) -> Result<...>;
    fn compile_edge(&mut self, edge: &EdgePattern, ...) -> Result<...>;
    fn compile_edge_with_variable(&mut self, ...) -> Result<...>;
    fn build_edge_sub_traversal(&mut self, ...) -> BoundTraversal<'g>;
    fn compile_edge_with_quantifier(&mut self, ...) -> Result<...>;
    fn bind_pattern_variables_from_match(&mut self, ...);
    fn register_optional_pattern_variables(&mut self, ...);
}
```

**Lines**: ~3009-3546 + 3148-3187 (~600 lines)

**Dependencies**: Needs access to `Compiler.snapshot`, `Compiler.bindings`

---

### Phase 5: Extract Expression Evaluation (Medium Risk)

**Goal**: Split the three expression evaluation contexts into separate files.

**Target directory**: `src/gql/compiler/expression/`

#### 5.1: Row-based Evaluation (`expression/row.rs`)
```rust
impl Compiler {
    fn evaluate_expression_from_row(&self, expr: &Expression, row: &HashMap<String, Value>) -> Value;
    fn evaluate_function_call_from_row(&self, name: &str, args: &[Expression], row: &HashMap<String, Value>) -> Value;
    fn evaluate_list_comprehension_from_row(&self, ...) -> Value;
    fn evaluate_reduce_from_row(&self, ...) -> Value;
    fn evaluate_list_predicate_from_row(&self, ...) -> Value;
    fn evaluate_pattern_comprehension_from_row(&self, ...) -> Value;
    fn evaluate_case_from_row(&self, ...) -> Value;
    fn evaluate_predicate_from_row(&self, ...) -> bool;
    fn evaluate_return_for_row(&self, ...) -> Value;
}
```
**Lines**: ~2159-3187 (~1028 lines)

#### 5.2: Path-based Evaluation (`expression/path.rs`)
```rust
impl Compiler {
    fn evaluate_predicate_from_path(&self, ...) -> bool;
    fn evaluate_value_from_path(&self, ...) -> Value;
    fn evaluate_reduce_from_path(&self, ...) -> Value;
    fn evaluate_list_predicate_from_path(&self, ...) -> Value;
    fn evaluate_list_comprehension_from_path(&self, ...) -> Value;
    fn evaluate_function_call_from_path(&self, ...) -> Value;
    fn evaluate_case_from_path(&self, ...) -> Value;
    fn get_variable_value_from_path(&self, ...) -> Value;
    fn evaluate_return_for_traverser(&self, ...) -> Value;
    fn evaluate_expression_from_path(&self, ...) -> Value;
}
```
**Lines**: ~3953-4923 (~970 lines)

#### 5.3: Element-based Evaluation (`expression/element.rs`)
```rust
impl Compiler {
    fn deduplicate_results(&self, ...) -> Vec<Value>;
    fn evaluate_return_for_element(&self, ...) -> Option<Value>;
    fn get_return_item_key(&self, ...) -> String;
    fn evaluate_expression(&self, ...) -> Option<Value>;
    fn extract_property(&self, ...) -> Option<Value>;
    fn evaluate_predicate(&self, ...) -> bool;
    fn evaluate_value(&self, ...) -> Value;
    fn evaluate_reduce(&self, ...) -> Value;
    fn evaluate_list_predicate(&self, ...) -> Value;
    fn evaluate_list_comprehension(&self, ...) -> Value;
    fn evaluate_index(&self, ...) -> Value;
    fn evaluate_slice(&self, ...) -> Value;
    fn evaluate_function_call(&self, ...) -> Value;
    fn evaluate_case(&self, ...) -> Value;
    fn evaluate_exists_pattern(&self, ...) -> bool;
}
```
**Lines**: ~4924-6210 (~1286 lines)

---

### Phase 6: Extract Clause Handling (Medium Risk)

**Goal**: Move clause execution logic to dedicated module.

**Target file**: `src/gql/compiler/clauses.rs`

**Items to move**:
```rust
impl Compiler {
    // UNWIND
    fn execute_with_unwind(&mut self, ...) -> Result<...>;
    fn apply_unwind(&self, ...) -> Vec<HashMap<String, Value>>;
    
    // LET
    fn execute_with_let(&mut self, ...) -> Result<...>;
    fn apply_let_clauses(&self, ...) -> Vec<HashMap<String, Value>>;
    fn apply_single_let_clause(&self, ...) -> HashMap<String, Value>;
    fn compute_let_aggregate(&self, ...) -> Value;
    
    // WITH
    fn execute_with_with_clauses(&mut self, ...) -> Result<...>;
    fn apply_with_clause(&self, ...) -> Vec<HashMap<String, Value>>;
    fn apply_with_projection(&self, ...) -> HashMap<String, Value>;
    fn apply_with_aggregation(&self, ...) -> Vec<HashMap<String, Value>>;
    fn compute_aggregate_over_rows(&self, ...) -> Value;
    fn compute_sum(&self, ...) -> Value;
    fn compute_avg(&self, ...) -> Value;
    fn compute_min(&self, ...) -> Value;
    fn compute_max(&self, ...) -> Value;
    fn deduplicate_rows(&self, ...) -> Vec<HashMap<String, Value>>;
    fn row_to_comparable_key(&self, ...) -> String;
    fn apply_order_by_to_rows(&self, ...) -> Vec<HashMap<String, Value>>;
    fn apply_limit_to_rows(&self, ...) -> Vec<HashMap<String, Value>>;
    fn expression_to_key(expr: &Expression) -> String;
}
```
**Lines**: ~721-1600 (~880 lines)

---

### Phase 7: Extract CALL Subquery (Medium Risk)

**Goal**: Move CALL subquery handling to dedicated module.

**Target file**: `src/gql/compiler/call.rs`

**Items to move**:
```rust
impl Compiler {
    fn validate_call_clause(&self, ...) -> Result<(), CompileError>;
    fn validate_call_query(&self, ...) -> Result<(), CompileError>;
    fn register_call_clause_variables(&mut self, ...);
    fn execute_with_call_clauses(&mut self, ...) -> Result<...>;
    fn execute_call_clause(&mut self, ...) -> Result<...>;
    fn execute_correlated_call(&mut self, ...) -> Result<...>;
    fn execute_uncorrelated_call(&mut self, ...) -> Result<...>;
    fn execute_call_body_with_context(&mut self, ...) -> Result<...>;
    fn execute_call_query_with_context(&mut self, ...) -> Result<...>;
    fn execute_match_for_call(&mut self, ...) -> Result<...>;
    fn compile_remaining_pattern_for_call(&mut self, ...) -> Result<...>;
    fn compile_pattern_elements_for_comprehension(&mut self, ...) -> Option<...>;
}
```
**Lines**: ~1604-2158 (~554 lines)

---

### Phase 8: Extract Optional Match (Low Risk)

**Goal**: Move OPTIONAL MATCH handling to dedicated module.

**Target file**: `src/gql/compiler/optional.rs`

**Items to move**:
```rust
impl Compiler {
    fn execute_with_optional_match(&mut self, ...) -> Result<...>;
    fn try_optional_match(&mut self, ...) -> Result<...>;
    fn add_null_optional_vars(&self, ...) -> Vec<HashMap<String, Value>>;
    fn merge_paths(&self, ...) -> Vec<Value>;
}
```
**Lines**: ~3647-3952 (~305 lines)

---

### Phase 9: Extract Aggregation (Medium Risk)

**Goal**: Move aggregation and GROUP BY logic to dedicated module.

**Target file**: `src/gql/compiler/aggregation.rs`

**Items to move**:
```rust
impl Compiler {
    fn has_aggregates(&self, ...) -> bool;
    fn expression_has_aggregate(&self, ...) -> bool;
    fn expr_has_aggregate(expr: &Expression) -> bool;
    fn execute_aggregated_return(&mut self, ...) -> Result<...>;
    fn execute_group_by_query(&mut self, ...) -> Result<...>;
    fn execute_group_by_query_multi_var(&mut self, ...) -> Result<...>;
    fn compute_group_result_multi_var(&self, ...) -> Value;
    fn evaluate_group_expression_multi_var(&self, ...) -> Value;
    fn compute_aggregate_multi_var(&self, ...) -> Value;
    fn expression_in_group_by(&self, ...) -> bool;
    fn expressions_match(&self, ...) -> bool;
    fn expression_to_string(&self, ...) -> String;
    fn compute_group_result(&self, ...) -> Value;
    fn evaluate_group_expression(&self, ...) -> Value;
    fn evaluate_having_predicate(&self, ...) -> bool;
    fn evaluate_having_value(&self, ...) -> Value;
    fn evaluate_having_predicate_multi_var(&self, ...) -> bool;
    fn evaluate_having_value_multi_var(&self, ...) -> Value;
    fn execute_global_aggregates(&mut self, ...) -> Result<...>;
    fn execute_grouped_aggregates(&mut self, ...) -> Result<...>;
    fn compute_aggregate(&self, ...) -> Value;
    fn apply_order_by(&self, ...) -> Vec<Value>;
    fn extract_order_key(&self, ...) -> Value;
    fn apply_limit(&self, ...) -> Vec<Value>;
}
```
**Lines**: ~6406-7809 (~1403 lines)

---

### Phase 10: Extract Tests (Low Risk)

**Goal**: Move embedded tests to dedicated test file.

**Target file**: `tests/gql/compiler_unit.rs`

**Lines to move**: ~8426-9306 (~880 lines)

This allows the tests to remain as unit tests but live in the test directory for consistency.

---

### Phase 11: Finalize Core Module

**Goal**: Clean up remaining code in core module.

**Target file**: `src/gql/compiler/core.rs` (or keep in `mod.rs`)

**Remaining items**:
```rust
// Public API functions
pub fn compile<'g>(...) -> Result<...>;
pub fn compile_with_params<'g>(...) -> Result<...>;
pub fn compile_statement<'g>(...) -> Result<...>;
pub fn compile_statement_with_params<'g>(...) -> Result<...>;
fn compile_union<'g>(...) -> Result<...>;
fn compile_union_with_params<'g>(...) -> Result<...>;

// Compiler struct and core methods
pub struct Compiler<'a, 'g> { ... }
struct BindingInfo { ... }
enum ListPredicateKind { ... }

impl Compiler {
    fn new(...) -> Self;
    fn resolve_parameter(&self, ...) -> Result<...>;
    fn count_pattern_variables(...) -> usize;
    fn return_uses_path_function(&self, ...) -> bool;
    fn expression_uses_path_function(...) -> bool;
    fn has_edge_variable(...) -> bool;
    fn compile(&mut self, ...) -> Result<...>;
    fn execute_return(&mut self, ...) -> Result<...>;
    fn execute_multi_var_return(&mut self, ...) -> Result<...>;
    fn apply_node_filters(...) -> ...;
    fn apply_edge_navigation(...) -> ...;
    fn validate_expression_variables(&self, ...) -> Result<...>;
}
```
**Lines**: ~193-720, 3547-3646, 6117-6210 (~600 lines)

---

## Backward Compatibility

### Public API Preservation

The following public items MUST remain accessible from `interstellar::gql`:

```rust
// Types
pub type Parameters = HashMap<String, Value>;

// Functions
pub fn compile<'g>(query: &Query, snapshot: &GraphSnapshot<'g>) -> Result<Vec<Value>, CompileError>;
pub fn compile_with_params<'g>(...) -> Result<Vec<Value>, CompileError>;
pub fn compile_statement<'g>(...) -> Result<Vec<Value>, CompileError>;
pub fn compile_statement_with_params<'g>(...) -> Result<Vec<Value>, CompileError>;
```

### Re-export Strategy

```rust
// src/gql/mod.rs
pub mod compiler;
pub use compiler::{compile, compile_with_params, compile_statement, compile_statement_with_params, Parameters};
```

---

## Testing Strategy

### Verification at Each Phase

1. **Run full test suite**: `cargo test`
2. **Run GQL-specific tests**: `cargo test gql`
3. **Check for regressions**: `cargo test --test gql`
4. **Verify coverage**: `cargo +nightly llvm-cov --branch 2>&1 | grep compiler`

### Test Distribution After Refactor

| File | Test Location |
|------|---------------|
| `compiler/mod.rs` | `tests/gql/compiler_unit.rs` |
| `compiler/helpers.rs` | `tests/gql/compiler_helpers.rs` |
| `compiler/pattern.rs` | `tests/gql/pattern.rs` |
| `compiler/expression/*` | `tests/gql/expression.rs` |
| `compiler/aggregation.rs` | `tests/gql/aggregation.rs` |

---

## Risk Mitigation

### Low-Risk Phases (Do First)
- Phase 1: Infrastructure setup
- Phase 2: Helper types/functions
- Phase 3: Math evaluation
- Phase 8: Optional Match
- Phase 10: Tests extraction

### Medium-Risk Phases (Careful Testing)
- Phase 4: Pattern compilation
- Phase 5: Expression evaluation
- Phase 6: Clause handling
- Phase 7: CALL subquery
- Phase 9: Aggregation

### Rollback Strategy

Each phase should be a separate commit, allowing easy revert:

```bash
git revert HEAD~1  # Revert last phase if issues found
```

---

## Success Criteria

1. **All tests pass**: `cargo test` shows 0 failures
2. **No API changes**: Public interface unchanged
3. **Coverage maintained**: No decrease in test coverage
4. **Improved maintainability**:
   - No file exceeds 1,500 lines
   - Clear separation of concerns
   - Easy to locate specific functionality

---

## Estimated Effort

| Phase | Lines | Effort | Risk |
|-------|-------|--------|------|
| Phase 1 | 0 | 30 min | Low |
| Phase 2 | 456 | 1 hour | Low |
| Phase 3 | 158 | 30 min | Low |
| Phase 4 | 600 | 2 hours | Medium |
| Phase 5 | 3,284 | 4 hours | Medium |
| Phase 6 | 880 | 2 hours | Medium |
| Phase 7 | 554 | 2 hours | Medium |
| Phase 8 | 305 | 1 hour | Low |
| Phase 9 | 1,403 | 3 hours | Medium |
| Phase 10 | 880 | 1 hour | Low |
| Phase 11 | 600 | 1 hour | Low |

**Total Estimated Effort**: ~18 hours

---

## Implementation Notes

### Module Pattern for Method Extraction

Since Rust requires methods to be in the same crate as the struct, we'll use partial `impl` blocks across modules:

```rust
// src/gql/compiler/mod.rs
pub struct Compiler<'a, 'g> {
    pub(super) snapshot: &'a GraphSnapshot<'g>,
    pub(super) parameters: &'a Parameters,
    pub(super) bindings: HashMap<String, BindingInfo>,
}

// src/gql/compiler/pattern.rs
impl<'a: 'g, 'g> Compiler<'a, 'g> {
    pub(super) fn compile_pattern(&mut self, ...) -> Result<...> {
        // Implementation
    }
}
```

### Visibility Rules

- Struct fields: `pub(super)` or `pub(crate)`
- Internal methods: `pub(super)` 
- Public API: `pub` with re-exports in `mod.rs`

---

## References

- Original file: `src/gql/compiler.rs` (9,306 lines)
- GQL module: `src/gql/mod.rs`
- Tests: `tests/gql/*.rs`
- Coverage spec: `specs/spec-26-test-coverage-improvements.md`
