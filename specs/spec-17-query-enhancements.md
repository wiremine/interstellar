# Spec 17: Query Language Enhancements

## Overview

This specification defines the implementation of query language enhancements to extend GQL capabilities. These features address gaps identified in the current implementation and bring Intersteller closer to full GQL/Cypher compatibility.

The target is to support queries like:

```sql
-- Multi-stage query with WITH clause
MATCH (p:Person)-[:KNOWS]->(friend)
WITH p, COUNT(friend) AS friendCount
WHERE friendCount > 5
MATCH (p)-[:LIVES_IN]->(city)
RETURN p.name, friendCount, city.name

-- REDUCE for accumulation
MATCH (p:Person)-[:PURCHASED]->(item)
RETURN p.name, REDUCE(total = 0, i IN COLLECT(item.price) | total + i) AS totalSpent

-- Regex matching
MATCH (p:Person) WHERE p.email =~ '.*@gmail\\.com$' RETURN p

-- HAVING for post-aggregation filtering
MATCH (p:Player)-[:plays_for]->(t:Team)
RETURN t.name, COUNT(*) AS playerCount
GROUP BY t.name
HAVING playerCount > 10

-- List quantifier predicates
MATCH (s:Student) WHERE ALL(score IN s.scores WHERE score >= 60) RETURN s
```

## Goals

1. **WITH Clause** - Pipe results between query parts for multi-stage processing
2. **REDUCE Function** - Fold/accumulate over lists
3. **Regular Expression Predicates** - Pattern matching with `=~` operator
4. **HAVING Clause** - Filter aggregated results post-GROUP BY
5. **SKIP Alias** - Alternative syntax for OFFSET (Cypher compatibility)
6. **ALL/ANY/NONE/SINGLE Predicates** - List quantifier expressions

## Non-Goals

- Subqueries (CALL { ... })
- LOAD CSV or data import features
- CREATE INDEX / CREATE CONSTRAINT (schema features)
- Date/Time functions (separate spec)
- Pattern comprehensions (separate spec)

---

## 1. WITH Clause

### 1.1 Description

The WITH clause pipes the results of one query part to the next, enabling multi-stage query processing. It acts as both a projection (like RETURN) and a connector between query parts.

**Syntax:**
```sql
MATCH (p:Person)-[:KNOWS]->(friend)
WITH p, COUNT(friend) AS friendCount
WHERE friendCount > 5
RETURN p.name, friendCount

-- Multiple WITH clauses
MATCH (p:Person)
WITH p, SIZE((p)-[:KNOWS]->()) AS degree
WHERE degree > 10
WITH p, degree, p.age AS age
WHERE age > 30
RETURN p.name, degree

-- WITH DISTINCT
MATCH (p:Person)-[:KNOWS]->(friend)
WITH DISTINCT friend.city AS city
RETURN city
```

### 1.2 Semantics

- WITH terminates the current query part and starts a new one
- Only variables explicitly listed in WITH are available in subsequent clauses
- WITH can include aggregations (like RETURN)
- WHERE after WITH filters on the WITH output (not on the original MATCH)
- WITH can use DISTINCT to deduplicate
- ORDER BY, LIMIT can be used with WITH for sorting/pagination mid-query
- Later MATCH clauses can reference WITH variables to constrain patterns

**Execution Model:**
```
MATCH -> [WHERE] -> WITH -> [WHERE] -> [MATCH] -> ... -> RETURN
        ^query part 1^    ^query part 2^
```

### 1.3 Grammar Changes

```pest
// WITH keyword
WITH = @{ ^"with" ~ !ASCII_ALPHANUMERIC }

// Update keyword list
keyword = {
    MATCH | RETURN | WHERE | ORDER | BY | GROUP | LIMIT | OFFSET |
    AS | AND | OR | NOT | TRUE | FALSE | NULL | ASC | DESC |
    IN | IS | CONTAINS | STARTS | ENDS | WITH | DISTINCT | EXISTS |
    CASE | WHEN | THEN | ELSE | END | UNION | ALL | OPTIONAL |
    PATH | UNWIND | CREATE | SET | REMOVE | DELETE | DETACH | MERGE | ON |
    LET | HAVING | SKIP  // Added HAVING and SKIP
}

// WITH clause
with_clause = { 
    WITH ~ DISTINCT? ~ return_item ~ ("," ~ return_item)* ~
    order_clause? ~ limit_clause?
}

// Updated query structure to support multiple query parts
query = { query_part+ }

query_part = {
    match_clause ~ 
    optional_match_clause* ~ 
    where_clause? ~ 
    (with_clause | return_clause_final)
}

return_clause_final = {
    return_clause ~ 
    group_by_clause? ~
    having_clause? ~
    order_clause? ~ 
    limit_clause?
}
```

### 1.4 AST Changes

```rust
/// A WITH clause for piping results between query parts.
#[derive(Debug, Clone, Serialize)]
pub struct WithClause {
    /// Whether to apply DISTINCT.
    pub distinct: bool,
    /// Items to project forward.
    pub items: Vec<ReturnItem>,
    /// Optional ORDER BY within WITH.
    pub order_clause: Option<OrderClause>,
    /// Optional LIMIT within WITH.
    pub limit_clause: Option<LimitClause>,
}

/// A complete query with potentially multiple parts connected by WITH.
#[derive(Debug, Clone, Serialize)]
pub struct Query {
    /// The query parts (MATCH...WITH or MATCH...RETURN).
    pub parts: Vec<QueryPart>,
}

/// A single part of a query (MATCH to WITH or MATCH to RETURN).
#[derive(Debug, Clone, Serialize)]
pub struct QueryPart {
    pub match_clause: Option<MatchClause>,
    pub optional_match_clauses: Vec<OptionalMatchClause>,
    pub where_clause: Option<WhereClause>,
    pub with_path_clause: Option<WithPathClause>,
    pub unwind_clauses: Vec<UnwindClause>,
    pub let_clauses: Vec<LetClause>,
    /// Either WITH (intermediate) or RETURN (final).
    pub projection: Projection,
}

/// Projection type: WITH (continues query) or RETURN (terminates).
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

### 1.5 Compilation

WITH clause compilation requires multi-stage execution:

1. Execute first query part (MATCH...WHERE)
2. Project results through WITH (like RETURN)
3. Use WITH output as input context for next part
4. Continue until RETURN is reached

```rust
// MATCH (p:Person)-[:KNOWS]->(f) WITH p, COUNT(f) AS cnt WHERE cnt > 5 RETURN p.name
// Compiles to:
// Stage 1: Match pattern, collect into groups
// Stage 2: Aggregate (COUNT)
// Stage 3: Filter (cnt > 5)
// Stage 4: Project (p.name)
```

**Key Implementation Details:**
- WITH resets the variable scope - only explicitly projected vars are available
- Aggregations in WITH require implicit grouping by non-aggregated expressions
- WHERE after WITH filters the WITH output rows
- Subsequent MATCH uses WITH variables to constrain patterns

---

## 2. REDUCE Function

### 2.1 Description

REDUCE accumulates a value by iterating over a list, similar to fold/reduce in functional programming.

**Syntax:**
```sql
-- Basic syntax
REDUCE(accumulator = initial, variable IN list | expression)

-- Sum prices
MATCH (p:Person)-[:PURCHASED]->(item)
LET prices = COLLECT(item.price)
RETURN REDUCE(total = 0, x IN prices | total + x) AS totalSpent

-- Concatenate strings
MATCH (p:Person)
RETURN REDUCE(s = '', name IN p.nicknames | s || name || ', ') AS allNames

-- Complex accumulation
MATCH (p:Person)-[:KNOWS]->(friend)
LET ages = COLLECT(friend.age)
RETURN REDUCE(stats = {sum: 0, count: 0}, a IN ages | {
    sum: stats.sum + a,
    count: stats.count + 1
}) AS ageStats
```

### 2.2 Semantics

- `accumulator` is the variable holding the running result
- `initial` is the starting value of the accumulator
- `variable` is bound to each element of the list in turn
- `expression` computes the new accumulator value
- Returns `initial` if list is empty
- Returns NULL if list is NULL

**Evaluation:**
```
REDUCE(acc = init, x IN [a, b, c] | expr)
→ let acc = init
→ let x = a; acc = expr
→ let x = b; acc = expr  
→ let x = c; acc = expr
→ return acc
```

### 2.3 Grammar Changes

```pest
// REDUCE function in primary expressions
reduce_expr = { 
    ^"reduce" ~ "(" ~ 
    identifier ~ "=" ~ expression ~ "," ~
    identifier ~ IN ~ expression ~ "|" ~ 
    expression ~ 
    ")"
}

// Update primary to include reduce_expr
primary = { 
    case_expr
    | exists_expr
    | reduce_expr     // NEW
    | parameter
    | literal
    | function_call
    | list_comprehension
    | property_access
    | variable
    | paren_expr
    | list_expr
    | map_expr
}
```

### 2.4 AST Changes

```rust
/// An expression that can be evaluated to produce a value.
#[derive(Debug, Clone, Serialize)]
pub enum Expression {
    // ... existing variants ...
    
    /// REDUCE expression: `REDUCE(acc = init, x IN list | expr)`
    ///
    /// Accumulates a value over a list.
    Reduce {
        /// Accumulator variable name.
        accumulator: String,
        /// Initial value for accumulator.
        initial: Box<Expression>,
        /// Variable bound to each list element.
        variable: String,
        /// The list to iterate over.
        list: Box<Expression>,
        /// Expression computing next accumulator value.
        expression: Box<Expression>,
    },
}
```

### 2.5 Compilation

```rust
fn evaluate_reduce(
    &self,
    accumulator: &str,
    initial: &Expression,
    variable: &str,
    list_expr: &Expression,
    expr: &Expression,
    context: &EvalContext,
) -> Value {
    // Evaluate initial value
    let mut acc_value = self.evaluate_expression(initial, context);
    
    // Evaluate list
    let list = match self.evaluate_expression(list_expr, context) {
        Value::List(items) => items,
        Value::Null => return Value::Null,
        _ => return Value::Null, // Non-list returns null
    };
    
    // Iterate and accumulate
    for item in list {
        let mut iter_context = context.clone();
        iter_context.bindings.insert(accumulator.to_string(), acc_value.clone());
        iter_context.bindings.insert(variable.to_string(), item);
        acc_value = self.evaluate_expression(expr, &iter_context);
    }
    
    acc_value
}
```

---

## 3. Regular Expression Predicates

### 3.1 Description

Support regex pattern matching using the `=~` operator for string filtering.

**Syntax:**
```sql
-- Basic regex match
MATCH (p:Person) WHERE p.email =~ '.*@gmail\\.com$' RETURN p

-- Case-insensitive match (using (?i) flag)
MATCH (p:Person) WHERE p.name =~ '(?i)^john' RETURN p

-- Negated regex
MATCH (p:Person) WHERE NOT (p.email =~ '.*@spam\\.com$') RETURN p

-- In property filter context
MATCH (p:Person) WHERE p.phone =~ '^\\+1-\\d{3}-\\d{3}-\\d{4}$' RETURN p
```

### 3.2 Semantics

- `string =~ pattern` returns true if string matches the regex pattern
- Uses Rust `regex` crate syntax (similar to PCRE)
- NULL on either side returns NULL
- Non-string left operand returns NULL
- Invalid regex pattern results in compile error
- Full match is required (pattern anchored to full string)

**Common Patterns:**
| Pattern | Matches |
|---------|---------|
| `.*@gmail\\.com$` | Strings ending with @gmail.com |
| `^\\d{3}-\\d{4}$` | Phone number format XXX-XXXX |
| `(?i)^john` | Starts with "john" (case-insensitive) |
| `.*error.*` | Contains "error" anywhere |

### 3.3 Grammar Changes

```pest
// Add regex match operator
regex_op = { "=~" }

// Add to comparison expression
comparison = { 
    concat_expr ~ (comp_op ~ concat_expr)? 
    | concat_expr ~ regex_op ~ string   // NEW: regex comparison
    | is_null_expr 
    | in_expr 
}
```

### 3.4 AST Changes

```rust
/// Binary operators for expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BinaryOperator {
    // ... existing variants ...
    
    /// Regex match: `=~`
    RegexMatch,
}
```

### 3.5 Compilation

```rust
fn evaluate_regex_match(&self, left: &Value, pattern: &str) -> Value {
    match left {
        Value::String(s) => {
            // Compile regex (could cache for performance)
            match regex::Regex::new(pattern) {
                Ok(re) => Value::Bool(re.is_match(s)),
                Err(_) => Value::Null, // Or could return error
            }
        }
        Value::Null => Value::Null,
        _ => Value::Null, // Non-string returns null
    }
}
```

### 3.6 Dependencies

Add `regex` crate to Cargo.toml:

```toml
[dependencies]
regex = "1.10"
```

---

## 4. HAVING Clause

### 4.1 Description

HAVING filters results after GROUP BY aggregation, allowing conditions on aggregate values.

**Syntax:**
```sql
-- Filter by aggregate
MATCH (p:Player)-[:plays_for]->(t:Team)
RETURN t.name, COUNT(*) AS playerCount
GROUP BY t.name
HAVING playerCount > 10

-- Multiple conditions
MATCH (e:Employee)-[:works_in]->(d:Department)
RETURN d.name, AVG(e.salary) AS avgSalary, COUNT(*) AS headcount
GROUP BY d.name
HAVING avgSalary > 50000 AND headcount >= 5

-- With ORDER BY and LIMIT
MATCH (p:Product)-[:sold_in]->(s:Store)
RETURN s.name, SUM(p.revenue) AS totalRevenue
GROUP BY s.name
HAVING totalRevenue > 100000
ORDER BY totalRevenue DESC
LIMIT 10
```

### 4.2 Semantics

- HAVING filters groups after GROUP BY aggregation
- Can only reference expressions that appear in RETURN
- Can reference aliases defined in RETURN
- Applied after GROUP BY but before ORDER BY/LIMIT
- Equivalent to WHERE for aggregated results

**Clause Ordering:**
```
MATCH -> WHERE -> GROUP BY -> HAVING -> ORDER BY -> LIMIT
         ^filters rows^  ^filters groups^
```

**Difference from WHERE:**
```sql
-- WHERE filters rows BEFORE aggregation
MATCH (p:Player) WHERE p.age > 25
RETURN p.position, COUNT(*) GROUP BY p.position

-- HAVING filters groups AFTER aggregation  
MATCH (p:Player)
RETURN p.position, COUNT(*) AS cnt
GROUP BY p.position
HAVING cnt > 5
```

### 4.3 Grammar Changes

```pest
// HAVING keyword
HAVING = @{ ^"having" ~ !ASCII_ALPHANUMERIC }

// HAVING clause
having_clause = { HAVING ~ expression }

// Update query structure
query = { 
    match_clause ~ 
    optional_match_clause* ~ 
    with_path_clause? ~ 
    unwind_clause* ~ 
    where_clause? ~ 
    let_clause* ~
    return_clause ~ 
    group_by_clause? ~ 
    having_clause? ~    // NEW: after GROUP BY
    order_clause? ~ 
    limit_clause? 
}
```

### 4.4 AST Changes

```rust
/// HAVING clause for filtering aggregated results.
#[derive(Debug, Clone, Serialize)]
pub struct HavingClause {
    /// The filter expression (evaluated after GROUP BY).
    pub expression: Expression,
}

/// Updated Query struct
#[derive(Debug, Clone, Serialize)]
pub struct Query {
    // ... existing fields ...
    pub having_clause: Option<HavingClause>,  // NEW
}
```

### 4.5 Compilation

HAVING is evaluated after aggregation:

```rust
// 1. Execute MATCH/WHERE
// 2. Group by GROUP BY keys
// 3. Compute aggregates for each group
// 4. Filter groups by HAVING expression
// 5. Apply ORDER BY/LIMIT
// 6. Return results
```

---

## 5. SKIP Alias for OFFSET

### 5.1 Description

SKIP is an alias for OFFSET, providing Cypher-compatible syntax for pagination.

**Syntax:**
```sql
-- SKIP is equivalent to OFFSET
MATCH (p:Person) RETURN p LIMIT 10 SKIP 20
MATCH (p:Person) RETURN p LIMIT 10 OFFSET 20  -- Same result

-- SKIP can appear before or after LIMIT (both valid)
MATCH (p:Person) RETURN p SKIP 20 LIMIT 10
MATCH (p:Person) RETURN p LIMIT 10 SKIP 20
```

### 5.2 Semantics

- `SKIP n` and `OFFSET n` are interchangeable
- Both skip the first n results
- Can be used with or without LIMIT
- If both SKIP and OFFSET appear, it's a parse error

### 5.3 Grammar Changes

```pest
// SKIP keyword
SKIP = @{ ^"skip" ~ !ASCII_ALPHANUMERIC }

// Update limit clause to accept SKIP or OFFSET
limit_clause = { 
    LIMIT ~ integer ~ (OFFSET | SKIP)? ~ integer?
    | (OFFSET | SKIP) ~ integer ~ LIMIT? ~ integer?
}
```

### 5.4 AST Changes

No AST changes needed - both SKIP and OFFSET produce the same `LimitClause` struct:

```rust
/// Limit and offset clause (unchanged)
#[derive(Debug, Clone, Serialize)]
pub struct LimitClause {
    pub limit: Option<u64>,
    pub offset: Option<u64>,  // Populated by either SKIP or OFFSET
}
```

### 5.5 Compilation

No compilation changes needed - SKIP and OFFSET are parsed identically.

---

## 6. ALL/ANY/NONE/SINGLE Predicates

### 6.1 Description

List quantifier predicates test conditions across list elements.

**Syntax:**
```sql
-- ALL: every element satisfies condition
MATCH (s:Student) WHERE ALL(score IN s.scores WHERE score >= 60) RETURN s

-- ANY: at least one element satisfies condition  
MATCH (p:Person) WHERE ANY(tag IN p.tags WHERE tag = 'vip') RETURN p

-- NONE: no element satisfies condition
MATCH (p:Product) WHERE NONE(review IN p.reviews WHERE review.rating < 3) RETURN p

-- SINGLE: exactly one element satisfies condition
MATCH (t:Team) WHERE SINGLE(p IN t.players WHERE p.captain = true) RETURN t
```

### 6.2 Semantics

| Predicate | Returns TRUE when |
|-----------|-------------------|
| `ALL(x IN list WHERE cond)` | All elements satisfy cond, or list is empty |
| `ANY(x IN list WHERE cond)` | At least one element satisfies cond |
| `NONE(x IN list WHERE cond)` | No element satisfies cond (empty list → true) |
| `SINGLE(x IN list WHERE cond)` | Exactly one element satisfies cond |

**NULL Handling:**
- If list is NULL, all predicates return NULL
- If condition evaluates to NULL for an element, that element is considered non-matching
- Empty list: ALL → true, ANY → false, NONE → true, SINGLE → false

### 6.3 Grammar Changes

```pest
// List quantifier predicates
all_predicate = { ^"all" ~ "(" ~ identifier ~ IN ~ expression ~ WHERE ~ expression ~ ")" }
any_predicate = { ^"any" ~ "(" ~ identifier ~ IN ~ expression ~ WHERE ~ expression ~ ")" }
none_predicate = { ^"none" ~ "(" ~ identifier ~ IN ~ expression ~ WHERE ~ expression ~ ")" }
single_predicate = { ^"single" ~ "(" ~ identifier ~ IN ~ expression ~ WHERE ~ expression ~ ")" }

// Update primary
primary = { 
    case_expr
    | exists_expr
    | reduce_expr
    | all_predicate      // NEW
    | any_predicate      // NEW
    | none_predicate     // NEW
    | single_predicate   // NEW
    | parameter
    | literal
    | function_call
    | list_comprehension
    | property_access
    | variable
    | paren_expr
    | list_expr
    | map_expr
}
```

### 6.4 AST Changes

```rust
/// An expression that can be evaluated to produce a value.
#[derive(Debug, Clone, Serialize)]
pub enum Expression {
    // ... existing variants ...
    
    /// ALL predicate: `ALL(x IN list WHERE cond)`
    All {
        variable: String,
        list: Box<Expression>,
        condition: Box<Expression>,
    },
    
    /// ANY predicate: `ANY(x IN list WHERE cond)`
    Any {
        variable: String,
        list: Box<Expression>,
        condition: Box<Expression>,
    },
    
    /// NONE predicate: `NONE(x IN list WHERE cond)`
    None {
        variable: String,
        list: Box<Expression>,
        condition: Box<Expression>,
    },
    
    /// SINGLE predicate: `SINGLE(x IN list WHERE cond)`
    Single {
        variable: String,
        list: Box<Expression>,
        condition: Box<Expression>,
    },
}
```

### 6.5 Compilation

```rust
fn evaluate_list_predicate(
    &self,
    predicate: ListPredicateType,
    variable: &str,
    list_expr: &Expression,
    condition: &Expression,
    context: &EvalContext,
) -> Value {
    let list = match self.evaluate_expression(list_expr, context) {
        Value::List(items) => items,
        Value::Null => return Value::Null,
        _ => return Value::Null,
    };
    
    let mut match_count = 0;
    
    for item in &list {
        let mut item_context = context.clone();
        item_context.bindings.insert(variable.to_string(), item.clone());
        
        match self.evaluate_expression(condition, &item_context) {
            Value::Bool(true) => match_count += 1,
            Value::Bool(false) => {}
            _ => {} // NULL/other treated as non-match
        }
    }
    
    Value::Bool(match predicate {
        ListPredicateType::All => match_count == list.len(),
        ListPredicateType::Any => match_count > 0,
        ListPredicateType::None => match_count == 0,
        ListPredicateType::Single => match_count == 1,
    })
}
```

---

## 7. Implementation Plan

### Phase 1: SKIP Alias (Low effort, ~30 lines)
1. Add SKIP keyword to grammar
2. Update limit_clause parsing
3. Add tests

### Phase 2: HAVING Clause (Low-Medium effort, ~100 lines)
1. Add HAVING keyword and clause to grammar
2. Add HavingClause to AST
3. Update parser
4. Implement HAVING evaluation in compiler (after GROUP BY)
5. Add tests

### Phase 3: Regular Expression Predicates (Medium effort, ~150 lines)
1. Add `regex` crate dependency
2. Add `=~` operator to grammar
3. Add BinaryOperator::RegexMatch
4. Update parser
5. Implement regex evaluation
6. Add tests (including edge cases)

### Phase 4: REDUCE Function (Medium effort, ~180 lines)
1. Add reduce_expr to grammar
2. Add Expression::Reduce variant
3. Update parser
4. Implement REDUCE evaluation
5. Add tests

### Phase 5: ALL/ANY/NONE/SINGLE Predicates (Medium effort, ~250 lines)
1. Add predicate rules to grammar
2. Add Expression variants for each predicate
3. Update parser
4. Implement predicate evaluation
5. Add tests for each predicate type

### Phase 6: WITH Clause (High effort, ~500 lines)
1. Restructure Query AST for multiple query parts
2. Add WITH keyword and clause to grammar
3. Add WithClause and QueryPart to AST
4. Update parser for multi-part queries
5. Implement multi-stage query execution
6. Handle variable scope transitions
7. Add comprehensive tests

---

## 8. Testing Requirements

### 8.1 Parser Tests

```rust
#[test] fn test_parse_skip() { }
#[test] fn test_parse_having_simple() { }
#[test] fn test_parse_having_complex() { }
#[test] fn test_parse_regex_match() { }
#[test] fn test_parse_reduce_basic() { }
#[test] fn test_parse_reduce_complex() { }
#[test] fn test_parse_all_predicate() { }
#[test] fn test_parse_any_predicate() { }
#[test] fn test_parse_none_predicate() { }
#[test] fn test_parse_single_predicate() { }
#[test] fn test_parse_with_simple() { }
#[test] fn test_parse_with_aggregation() { }
#[test] fn test_parse_with_multiple() { }
#[test] fn test_parse_with_distinct() { }
```

### 8.2 Compiler Tests

```rust
#[test] fn test_compile_skip_offset_equivalent() { }
#[test] fn test_compile_having_filters_groups() { }
#[test] fn test_compile_having_with_alias() { }
#[test] fn test_compile_regex_basic_match() { }
#[test] fn test_compile_regex_no_match() { }
#[test] fn test_compile_regex_null_handling() { }
#[test] fn test_compile_reduce_sum() { }
#[test] fn test_compile_reduce_concat() { }
#[test] fn test_compile_reduce_empty_list() { }
#[test] fn test_compile_all_true() { }
#[test] fn test_compile_all_false() { }
#[test] fn test_compile_any_true() { }
#[test] fn test_compile_any_false() { }
#[test] fn test_compile_none_true() { }
#[test] fn test_compile_none_false() { }
#[test] fn test_compile_single_true() { }
#[test] fn test_compile_single_false() { }
#[test] fn test_compile_with_basic() { }
#[test] fn test_compile_with_aggregation() { }
#[test] fn test_compile_with_where_after() { }
#[test] fn test_compile_with_multiple_parts() { }
```

### 8.3 Integration Tests

```rust
#[test]
fn test_with_count_and_filter() {
    // MATCH (p:Person)-[:KNOWS]->(f) WITH p, COUNT(f) AS cnt WHERE cnt > 5 RETURN p
}

#[test]
fn test_reduce_total_price() {
    // MATCH (p)-[:PURCHASED]->(i) RETURN REDUCE(t=0, x IN COLLECT(i.price) | t+x)
}

#[test]
fn test_having_with_group_by() {
    // MATCH (p:Player)-[:plays_for]->(t) RETURN t.name, COUNT(*) GROUP BY t HAVING COUNT(*) > 5
}

#[test]
fn test_regex_email_filter() {
    // MATCH (p:Person) WHERE p.email =~ '.*@gmail\\.com$' RETURN p
}

#[test]
fn test_all_scores_passing() {
    // MATCH (s:Student) WHERE ALL(x IN s.scores WHERE x >= 60) RETURN s
}
```

### 8.4 Snapshot Tests

```rust
#[test] fn parse_skip_snapshot() { }
#[test] fn parse_having_snapshot() { }
#[test] fn parse_regex_snapshot() { }
#[test] fn parse_reduce_snapshot() { }
#[test] fn parse_all_predicate_snapshot() { }
#[test] fn parse_any_predicate_snapshot() { }
#[test] fn parse_none_predicate_snapshot() { }
#[test] fn parse_single_predicate_snapshot() { }
#[test] fn parse_with_clause_snapshot() { }
```

---

## 9. Error Handling

### 9.1 New Error Types

```rust
#[derive(Debug, Error)]
pub enum GqlError {
    // ... existing variants ...
    
    /// Invalid regex pattern
    #[error("invalid regex pattern: {0}")]
    InvalidRegex(String),
    
    /// HAVING without GROUP BY
    #[error("HAVING clause requires GROUP BY")]
    HavingWithoutGroupBy,
    
    /// HAVING references undefined alias
    #[error("HAVING references undefined column: {0}")]
    HavingUndefinedColumn(String),
    
    /// WITH variable not available
    #[error("variable '{0}' not available after WITH clause")]
    VariableNotInWith(String),
    
    /// REDUCE accumulator shadows existing variable
    #[error("REDUCE accumulator '{0}' shadows existing variable")]
    ReduceAccumulatorShadows(String),
}
```

---

## 10. Example Usage

### 10.1 Multi-Stage Query with WITH

```rust
let results = execute(
    &graph,
    "MATCH (p:Person)-[:KNOWS]->(friend)
     WITH p, COUNT(friend) AS friendCount, COLLECT(friend.name) AS friendNames
     WHERE friendCount > 5
     MATCH (p)-[:LIVES_IN]->(city)
     RETURN p.name, friendCount, friendNames, city.name
     ORDER BY friendCount DESC"
)?;
```

### 10.2 REDUCE for Aggregation

```rust
let results = execute(
    &graph,
    "MATCH (order:Order)-[:CONTAINS]->(item:Item)
     LET items = COLLECT({name: item.name, price: item.price, qty: item.quantity})
     RETURN order.id,
            REDUCE(total = 0, i IN items | total + i.price * i.qty) AS orderTotal,
            SIZE(items) AS itemCount"
)?;
```

### 10.3 Regex Filtering

```rust
let results = execute(
    &graph,
    "MATCH (p:Person)
     WHERE p.email =~ '.*@(gmail|yahoo)\\.com$'
       AND p.phone =~ '^\\+1-\\d{3}-\\d{3}-\\d{4}$'
     RETURN p.name, p.email, p.phone"
)?;
```

### 10.4 HAVING with Aggregation

```rust
let results = execute(
    &graph,
    "MATCH (e:Employee)-[:WORKS_IN]->(d:Department)
     RETURN d.name AS department,
            COUNT(*) AS headcount,
            AVG(e.salary) AS avgSalary
     GROUP BY d.name
     HAVING headcount >= 10 AND avgSalary > 60000
     ORDER BY avgSalary DESC"
)?;
```

### 10.5 List Predicates

```rust
let results = execute(
    &graph,
    "MATCH (s:Student)
     WHERE ALL(score IN s.testScores WHERE score >= 70)
       AND ANY(activity IN s.activities WHERE activity = 'sports')
       AND NONE(flag IN s.flags WHERE flag = 'probation')
     RETURN s.name, s.gpa"
)?;
```

---

## 11. Future Enhancements

After this spec is implemented, potential future work includes:

- **EXISTS subqueries**: `WHERE EXISTS { MATCH (n)-->(m) WHERE m.x = outer.x }`
- **COUNT subqueries**: `RETURN (COUNT { MATCH (n)-->() }) AS degree`
- **COLLECT subqueries**: Pattern comprehensions `[(n)-->(m) | m.name]`
- **UNWIND with index**: `UNWIND list AS item, idx`
- **ALL/ANY on paths**: `ALL(n IN nodes(path) WHERE n.valid)`
