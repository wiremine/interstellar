# Plan 18: Implement Query Language Enhancements

**Spec Reference:** `specs/spec-17-query-enhancements.md`

**Goal:** Implement WITH clause, REDUCE function, regex predicates, HAVING clause, SKIP alias, and ALL/ANY/NONE/SINGLE predicates to extend GQL query capabilities.

**Estimated Duration:** 2-3 weeks

---

## Overview

This plan implements the query language enhancements defined in Spec 17. The features are ordered by dependency and complexity, starting with simpler additions and building toward the more complex WITH clause.

**Implementation Order:**
1. SKIP alias (trivial)
2. HAVING clause (low complexity)
3. Regular expressions (medium complexity)
4. REDUCE function (medium complexity)
5. ALL/ANY/NONE/SINGLE predicates (medium complexity)
6. WITH clause (high complexity)

---

## Phase 1: SKIP Alias (Day 1)

### 1.1 Add SKIP Keyword to Grammar

**File:** `src/gql/grammar.pest`

**Tasks:**
- [ ] Add `SKIP` keyword definition
- [ ] Update keyword list to include SKIP
- [ ] Update `limit_clause` rule to accept SKIP or OFFSET

**Grammar Changes:**
```pest
SKIP = @{ ^"skip" ~ !ASCII_ALPHANUMERIC }

// Update keyword list
keyword = { ... | SKIP | ... }

// Update limit_clause
limit_clause = { 
    LIMIT ~ integer ~ ((OFFSET | SKIP) ~ integer)?
    | (OFFSET | SKIP) ~ integer ~ (LIMIT ~ integer)?
}
```

### 1.2 Update Parser

**File:** `src/gql/parser.rs`

**Tasks:**
- [ ] Handle SKIP as alias for OFFSET in `parse_limit_clause`
- [ ] Ensure SKIP and OFFSET produce identical AST output

### 1.3 Add Tests

**File:** `tests/gql_snapshots.rs` or new test file

**Tasks:**
- [ ] Test `LIMIT 10 SKIP 5` parses correctly
- [ ] Test `SKIP 5 LIMIT 10` parses correctly
- [ ] Test `SKIP 5` without LIMIT works
- [ ] Verify SKIP and OFFSET produce identical results

---

## Phase 2: HAVING Clause (Days 2-3)

### 2.1 Add HAVING to Grammar

**File:** `src/gql/grammar.pest`

**Tasks:**
- [ ] Add `HAVING` keyword definition
- [ ] Add `having_clause` rule
- [ ] Update query rule to include optional having_clause after group_by_clause

**Grammar Changes:**
```pest
HAVING = @{ ^"having" ~ !ASCII_ALPHANUMERIC }

having_clause = { HAVING ~ expression }

// Update query structure
query = { 
    match_clause ~ ... ~ 
    group_by_clause? ~ 
    having_clause? ~     // NEW
    order_clause? ~ 
    limit_clause? 
}
```

### 2.2 Add AST Types

**File:** `src/gql/ast.rs`

**Tasks:**
- [ ] Add `HavingClause` struct
- [ ] Add `having_clause: Option<HavingClause>` to `Query` struct

```rust
#[derive(Debug, Clone, Serialize)]
pub struct HavingClause {
    pub expression: Expression,
}
```

### 2.3 Update Parser

**File:** `src/gql/parser.rs`

**Tasks:**
- [ ] Implement `parse_having_clause` function
- [ ] Call from main query parsing
- [ ] Handle span tracking

### 2.4 Update Compiler

**File:** `src/gql/compiler.rs`

**Tasks:**
- [ ] Add HAVING evaluation after GROUP BY aggregation
- [ ] Validate HAVING expressions reference available columns/aliases
- [ ] Add error for HAVING without GROUP BY (optional - could allow it)

**Compilation Logic:**
```rust
// After computing aggregated groups:
if let Some(having) = &query.having_clause {
    groups.retain(|group| {
        let value = self.evaluate_expression(&having.expression, &group.context);
        matches!(value, Value::Bool(true))
    });
}
```

### 2.5 Add Tests

**Tasks:**
- [ ] Test `HAVING COUNT(*) > 5` filters groups correctly
- [ ] Test HAVING with alias reference
- [ ] Test HAVING with complex expression (AND, OR)
- [ ] Test HAVING without GROUP BY (error or allow)
- [ ] Snapshot test for parsed HAVING clause

---

## Phase 3: Regular Expression Predicates (Days 4-6)

### 3.1 Add Regex Dependency

**File:** `Cargo.toml`

**Tasks:**
- [ ] Add `regex = "1.10"` to dependencies
- [ ] Run `cargo build` to verify

### 3.2 Add Regex Operator to Grammar

**File:** `src/gql/grammar.pest`

**Tasks:**
- [ ] Add `regex_op` rule for `=~`
- [ ] Update comparison expression to handle regex

**Grammar Changes:**
```pest
regex_op = { "=~" }

// Option A: Add regex as comparison variant
comparison = { 
    concat_expr ~ comp_op ~ concat_expr
    | concat_expr ~ regex_op ~ primary   // regex match
    | is_null_expr 
    | in_expr 
}
```

### 3.3 Update AST

**File:** `src/gql/ast.rs`

**Tasks:**
- [ ] Add `RegexMatch` variant to `BinaryOperator` enum

```rust
pub enum BinaryOperator {
    // ... existing
    RegexMatch,  // =~
}
```

### 3.4 Update Parser

**File:** `src/gql/parser.rs`

**Tasks:**
- [ ] Parse `=~` operator
- [ ] Create BinaryOp expression with RegexMatch operator

### 3.5 Update Compiler

**File:** `src/gql/compiler.rs`

**Tasks:**
- [ ] Import `regex::Regex`
- [ ] Handle `BinaryOperator::RegexMatch` in expression evaluation
- [ ] Consider caching compiled regexes for performance
- [ ] Handle NULL values appropriately

```rust
BinaryOperator::RegexMatch => {
    match (left, right) {
        (Value::String(s), Value::String(pattern)) => {
            match Regex::new(&pattern) {
                Ok(re) => Value::Bool(re.is_match(&s)),
                Err(e) => {
                    // Could return error or NULL
                    Value::Null
                }
            }
        }
        (Value::Null, _) | (_, Value::Null) => Value::Null,
        _ => Value::Null,
    }
}
```

### 3.6 Add Error Type

**File:** `src/gql/error.rs`

**Tasks:**
- [ ] Add `InvalidRegex(String)` error variant
- [ ] Decide: compile-time error vs runtime NULL

### 3.7 Add Tests

**Tasks:**
- [ ] Test basic regex match
- [ ] Test regex no match
- [ ] Test case-insensitive with `(?i)`
- [ ] Test NULL handling
- [ ] Test invalid regex pattern
- [ ] Test complex patterns (anchors, groups, etc.)
- [ ] Snapshot test for parsed regex expression

---

## Phase 4: REDUCE Function (Days 7-9)

### 4.1 Add REDUCE to Grammar

**File:** `src/gql/grammar.pest`

**Tasks:**
- [ ] Add `reduce_expr` rule
- [ ] Update `primary` to include reduce_expr

**Grammar Changes:**
```pest
reduce_expr = { 
    ^"reduce" ~ "(" ~ 
    identifier ~ "=" ~ expression ~ "," ~   // accumulator = initial
    identifier ~ IN ~ expression ~ "|" ~    // variable IN list
    expression ~                            // body expression
    ")"
}

primary = { 
    case_expr
    | exists_expr
    | reduce_expr     // NEW - before function_call
    | parameter
    | ...
}
```

### 4.2 Update AST

**File:** `src/gql/ast.rs`

**Tasks:**
- [ ] Add `Reduce` variant to `Expression` enum

```rust
pub enum Expression {
    // ... existing variants
    
    Reduce {
        accumulator: String,
        initial: Box<Expression>,
        variable: String,
        list: Box<Expression>,
        expression: Box<Expression>,
    },
}
```

### 4.3 Update Parser

**File:** `src/gql/parser.rs`

**Tasks:**
- [ ] Implement `parse_reduce_expr` function
- [ ] Call from primary expression parsing
- [ ] Handle all components (accumulator, initial, variable, list, expression)

### 4.4 Update Compiler

**File:** `src/gql/compiler.rs`

**Tasks:**
- [ ] Handle `Expression::Reduce` in expression evaluation
- [ ] Implement accumulation loop
- [ ] Handle NULL list (return NULL)
- [ ] Handle empty list (return initial)

```rust
Expression::Reduce { accumulator, initial, variable, list, expression } => {
    let mut acc = self.evaluate_expression(initial, context);
    
    let items = match self.evaluate_expression(list, context) {
        Value::List(items) => items,
        Value::Null => return Value::Null,
        _ => return Value::Null,
    };
    
    for item in items {
        let mut iter_ctx = context.clone();
        iter_ctx.bindings.insert(accumulator.clone(), acc.clone());
        iter_ctx.bindings.insert(variable.clone(), item);
        acc = self.evaluate_expression(expression, &iter_ctx);
    }
    
    acc
}
```

### 4.5 Add Tests

**Tasks:**
- [ ] Test sum: `REDUCE(t=0, x IN [1,2,3] | t+x)` → 6
- [ ] Test concat: `REDUCE(s='', x IN ['a','b'] | s||x)` → "ab"
- [ ] Test with property access
- [ ] Test empty list returns initial
- [ ] Test NULL list returns NULL
- [ ] Test complex accumulator (map)
- [ ] Snapshot test for parsed REDUCE

---

## Phase 5: ALL/ANY/NONE/SINGLE Predicates (Days 10-12)

### 5.1 Add Predicate Rules to Grammar

**File:** `src/gql/grammar.pest`

**Tasks:**
- [ ] Add `all_predicate`, `any_predicate`, `none_predicate`, `single_predicate` rules
- [ ] Update `primary` to include these predicates

**Grammar Changes:**
```pest
all_predicate = { ^"all" ~ "(" ~ identifier ~ IN ~ expression ~ WHERE ~ expression ~ ")" }
any_predicate = { ^"any" ~ "(" ~ identifier ~ IN ~ expression ~ WHERE ~ expression ~ ")" }
none_predicate = { ^"none" ~ "(" ~ identifier ~ IN ~ expression ~ WHERE ~ expression ~ ")" }
single_predicate = { ^"single" ~ "(" ~ identifier ~ IN ~ expression ~ WHERE ~ expression ~ ")" }

primary = { 
    case_expr
    | exists_expr
    | reduce_expr
    | all_predicate      // NEW
    | any_predicate      // NEW  
    | none_predicate     // NEW
    | single_predicate   // NEW
    | parameter
    | ...
}
```

### 5.2 Update AST

**File:** `src/gql/ast.rs`

**Tasks:**
- [ ] Add `All`, `Any`, `None`, `Single` variants to `Expression` enum

```rust
pub enum Expression {
    // ... existing variants
    
    All {
        variable: String,
        list: Box<Expression>,
        condition: Box<Expression>,
    },
    Any {
        variable: String,
        list: Box<Expression>,
        condition: Box<Expression>,
    },
    None {
        variable: String,
        list: Box<Expression>,
        condition: Box<Expression>,
    },
    Single {
        variable: String,
        list: Box<Expression>,
        condition: Box<Expression>,
    },
}
```

### 5.3 Update Parser

**File:** `src/gql/parser.rs`

**Tasks:**
- [ ] Implement `parse_all_predicate`, `parse_any_predicate`, etc.
- [ ] Call from primary expression parsing
- [ ] Share common parsing logic where possible

### 5.4 Update Compiler

**File:** `src/gql/compiler.rs`

**Tasks:**
- [ ] Handle all four predicate variants
- [ ] Implement common evaluation logic with predicate-specific condition

```rust
fn evaluate_list_predicate(
    &self,
    kind: ListPredicateKind,
    variable: &str,
    list_expr: &Expression,
    condition: &Expression,
    context: &EvalContext,
) -> Value {
    let items = match self.evaluate_expression(list_expr, context) {
        Value::List(items) => items,
        Value::Null => return Value::Null,
        _ => return Value::Null,
    };
    
    let mut match_count = 0;
    for item in &items {
        let mut ctx = context.clone();
        ctx.bindings.insert(variable.to_string(), item.clone());
        if matches!(self.evaluate_expression(condition, &ctx), Value::Bool(true)) {
            match_count += 1;
        }
    }
    
    Value::Bool(match kind {
        ListPredicateKind::All => match_count == items.len(),
        ListPredicateKind::Any => match_count > 0,
        ListPredicateKind::None => match_count == 0,
        ListPredicateKind::Single => match_count == 1,
    })
}
```

### 5.5 Add Tests

**Tasks:**
- [ ] ALL: all match → true
- [ ] ALL: one fails → false
- [ ] ALL: empty list → true
- [ ] ANY: one matches → true
- [ ] ANY: none match → false
- [ ] ANY: empty list → false
- [ ] NONE: none match → true
- [ ] NONE: one matches → false
- [ ] NONE: empty list → true
- [ ] SINGLE: exactly one → true
- [ ] SINGLE: zero match → false
- [ ] SINGLE: multiple match → false
- [ ] Test NULL list returns NULL
- [ ] Snapshot tests for each predicate

---

## Phase 6: WITH Clause (Days 13-20)

This is the most complex feature, requiring significant restructuring.

### 6.1 Restructure Query AST

**File:** `src/gql/ast.rs`

**Tasks:**
- [ ] Add `WithClause` struct
- [ ] Add `QueryPart` struct
- [ ] Add `Projection` enum (With vs Return)
- [ ] Update `Query` to contain `Vec<QueryPart>`
- [ ] Keep backward compatibility during transition

```rust
#[derive(Debug, Clone, Serialize)]
pub struct WithClause {
    pub distinct: bool,
    pub items: Vec<ReturnItem>,
    pub order_clause: Option<OrderClause>,
    pub limit_clause: Option<LimitClause>,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryPart {
    pub match_clause: Option<MatchClause>,
    pub optional_match_clauses: Vec<OptionalMatchClause>,
    pub where_clause: Option<WhereClause>,
    pub with_path_clause: Option<WithPathClause>,
    pub unwind_clauses: Vec<UnwindClause>,
    pub let_clauses: Vec<LetClause>,
    pub projection: Projection,
}

#[derive(Debug, Clone, Serialize)]
pub enum Projection {
    With(WithClause),
    Return {
        clause: ReturnClause,
        group_by: Option<GroupByClause>,
        having: Option<HavingClause>,
        order: Option<OrderClause>,
        limit: Option<LimitClause>,
    },
}
```

### 6.2 Update Grammar for WITH

**File:** `src/gql/grammar.pest`

**Tasks:**
- [ ] Ensure WITH keyword doesn't conflict with WITH PATH
- [ ] Add `with_clause` rule
- [ ] Update query structure for multiple parts

```pest
// WITH clause (distinct from WITH PATH)
with_clause = { 
    WITH ~ DISTINCT? ~ return_item ~ ("," ~ return_item)* ~
    order_clause? ~ limit_clause?
}

// Query with multiple parts
query = { query_part+ }

query_part = {
    match_clause? ~ 
    optional_match_clause* ~
    with_path_clause? ~
    unwind_clause* ~
    where_clause? ~
    let_clause* ~
    (with_clause | return_clause_final)
}
```

### 6.3 Update Parser for WITH

**File:** `src/gql/parser.rs`

**Tasks:**
- [ ] Implement `parse_with_clause` function
- [ ] Update main query parsing for multi-part structure
- [ ] Handle WHERE after WITH correctly
- [ ] Distinguish WITH from WITH PATH

### 6.4 Restructure Compiler for Multi-Part Queries

**File:** `src/gql/compiler.rs`

**Tasks:**
- [ ] Implement multi-stage execution
- [ ] Handle variable scope transitions between parts
- [ ] Support aggregations in WITH
- [ ] Support WHERE after WITH
- [ ] Support MATCH after WITH (referencing WITH variables)

**Key Implementation:**
```rust
fn compile_query(&self, query: &Query) -> Result<Vec<Value>, CompileError> {
    let mut context = EvalContext::new();
    let mut current_rows: Vec<Row> = vec![Row::empty()];
    
    for (i, part) in query.parts.iter().enumerate() {
        // Execute this query part
        current_rows = self.execute_query_part(part, current_rows, &context)?;
        
        // Update context with WITH/RETURN variables
        match &part.projection {
            Projection::With(with_clause) => {
                // Reset context to only WITH variables
                context = self.build_with_context(&with_clause, &current_rows);
            }
            Projection::Return { .. } => {
                // Final part - return results
                return self.project_results(&part.projection, current_rows);
            }
        }
    }
    
    Err(CompileError::MissingReturn)
}
```

### 6.5 Handle Variable Scope

**Tasks:**
- [ ] After WITH, only projected variables are available
- [ ] Aliases from WITH become the new variable names
- [ ] Previous MATCH variables are not accessible unless projected
- [ ] Error on undefined variable reference after WITH

### 6.6 Handle WHERE After WITH

**Tasks:**
- [ ] Detect WHERE following WITH
- [ ] Filter on WITH output (not original MATCH)
- [ ] Support aggregated values in WHERE after WITH

### 6.7 Handle MATCH After WITH

**Tasks:**
- [ ] Allow MATCH to use WITH variables in patterns
- [ ] Constrain pattern matching based on WITH values
- [ ] Support `MATCH (n) WHERE n.id = withVar.id` pattern

### 6.8 Add Comprehensive Tests

**Tasks:**
- [ ] Basic WITH projection
- [ ] WITH with aggregation
- [ ] WITH DISTINCT
- [ ] WHERE after WITH
- [ ] Multiple WITH clauses
- [ ] MATCH after WITH
- [ ] Variable scope isolation
- [ ] ORDER BY and LIMIT in WITH
- [ ] Complex multi-part queries
- [ ] Snapshot tests for WITH AST

---

## Phase 7: Integration and Documentation (Days 21-22)

### 7.1 Integration Tests

**File:** `tests/gql.rs` or new file

**Tasks:**
- [ ] Test combining multiple new features
- [ ] Test WITH + HAVING together
- [ ] Test REDUCE inside WITH
- [ ] Test list predicates in WHERE after WITH
- [ ] Test regex in complex queries

### 7.2 Update gql_api.md

**File:** `gql_api.md`

**Tasks:**
- [ ] Add WITH clause documentation
- [ ] Add REDUCE function documentation
- [ ] Add regex predicate documentation
- [ ] Add HAVING clause documentation
- [ ] Add SKIP documentation
- [ ] Add ALL/ANY/NONE/SINGLE documentation
- [ ] Update Limitations section
- [ ] Update Query Structure section

### 7.3 Update Limitations

**File:** `gql_api.md`

**Tasks:**
- [ ] Remove WITH from "Not Supported"
- [ ] Remove REDUCE from "Future work"
- [ ] Remove regex from "Future work"
- [ ] Update partial support notes

### 7.4 Add Example

**File:** `examples/advanced_gql.rs` (update existing or create new)

**Tasks:**
- [ ] Add examples using new features
- [ ] Demonstrate WITH clause usage
- [ ] Demonstrate REDUCE function
- [ ] Demonstrate list predicates

---

## Testing Checklist

### Unit Tests

**SKIP:**
- [ ] `SKIP 10` parses correctly
- [ ] `LIMIT 10 SKIP 5` works
- [ ] `SKIP 5 LIMIT 10` works
- [ ] SKIP and OFFSET are equivalent

**HAVING:**
- [ ] `HAVING COUNT(*) > 5` filters groups
- [ ] HAVING with alias reference
- [ ] HAVING with AND/OR
- [ ] HAVING without GROUP BY handling

**Regex:**
- [ ] Basic match
- [ ] No match returns false
- [ ] Case insensitive `(?i)`
- [ ] NULL handling
- [ ] Invalid pattern handling

**REDUCE:**
- [ ] Sum accumulation
- [ ] String concatenation
- [ ] Empty list → initial
- [ ] NULL list → NULL
- [ ] Complex accumulator type

**List Predicates:**
- [ ] ALL with all matching
- [ ] ALL with none matching
- [ ] ALL with empty list
- [ ] ANY with one matching
- [ ] ANY with none matching
- [ ] NONE with none matching
- [ ] NONE with one matching
- [ ] SINGLE with exactly one
- [ ] SINGLE with zero/multiple

**WITH:**
- [ ] Basic projection
- [ ] With aggregation
- [ ] DISTINCT
- [ ] WHERE after WITH
- [ ] Multiple WITH clauses
- [ ] Variable scope isolation
- [ ] MATCH after WITH

### Integration Tests
- [ ] Complex queries combining features
- [ ] Performance with large datasets
- [ ] Error handling edge cases

---

## Dependencies

- Existing GQL module (`src/gql/`)
- `regex` crate (new dependency for Phase 3)

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| WITH restructuring breaks existing queries | High | Keep backward-compatible Query struct during transition |
| Regex performance on large datasets | Medium | Consider caching compiled regexes |
| Variable scope complexity in WITH | Medium | Clear documentation, comprehensive tests |
| Grammar ambiguity (WITH vs WITH PATH) | Medium | Careful grammar ordering, lookahead if needed |
| AST changes require updating all tests | Medium | Incremental changes, run tests frequently |

---

## Success Criteria

1. All six features are implemented and tested
2. Tests pass with good branch coverage (>90% on new code)
3. Backward compatibility maintained for existing queries
4. Documentation updated with examples
5. `gql_api.md` reflects new capabilities
6. No performance regression on existing queries

---

## Future Work (Out of Scope)

- EXISTS subqueries with correlated variables
- COUNT/COLLECT subqueries
- Pattern comprehensions `[(n)-->(m) | m.name]`
- UNWIND with index
- Date/Time functions
- Schema operations (CREATE INDEX, CREATE CONSTRAINT)
