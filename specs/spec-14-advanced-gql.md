# Spec 14: Advanced GQL Features

## Overview

This specification defines the implementation of advanced GQL features needed to support complex analytical queries. These features bring Interstellar closer to full GQL/Cypher compatibility, enabling sophisticated pattern matching and data transformation scenarios.

The target is to support queries like:

```sql
MATCH (person:Person WHERE person.id = $personId)
      -[:PARTICIPATED_IN WHERE role = "child"]->(personEvent)
      <-[:PARTICIPATED_IN]-(parent:Person),
      (parent)-[:PARTICIPATED_IN]->(siblingEvent)
      <-[:PARTICIPATED_IN WHERE role = "child"]-(sibling:Person)
WHERE (personEvent:Birth OR personEvent:Adoption)
  AND (siblingEvent:Birth OR siblingEvent:Adoption)
  AND sibling <> person
LET connections = COLLECT({
      parent: parent,
      personEventType: labels(personEvent)[1],
      siblingEventType: labels(siblingEvent)[1]
    })
RETURN sibling,
       SIZE(connections) AS sharedParentCount,
       [c IN connections | c.personEventType || "/" || c.siblingEventType] AS relationshipTypes
GROUP BY sibling
```

## Goals

1. **Inline WHERE in Patterns** - Filter nodes/edges within pattern syntax
2. **Query Parameters** - Support `$paramName` syntax for parameterized queries
3. **LET Clause** - Bind intermediate computed values to variables
4. **List Comprehensions** - Transform lists with `[x IN list | expr]` syntax
5. **String Concatenation** - Support `||` operator for string operations
6. **Map Literals in Expressions** - Support `{key: value}` in COLLECT and RETURN

## Non-Goals

- Full Cypher procedure support (CALL)
- Subqueries in expressions (covered separately)
- LOAD CSV or data import features
- Regular expression predicates (future work)
- REDUCE function (future work)

---

## 1. Inline WHERE in Patterns

### 1.1 Description

Allow WHERE clauses directly within node and edge patterns to filter during pattern matching, rather than requiring a separate WHERE clause.

**Syntax:**
```sql
-- Node with inline WHERE
(n:Person WHERE n.age > 21)

-- Edge with inline WHERE
-[r:KNOWS WHERE r.since > 2020]->

-- Combined
MATCH (a:Person WHERE a.status = 'active')-[r:FOLLOWS WHERE r.weight > 0.5]->(b)
RETURN a, b
```

### 1.2 Semantics

- Inline WHERE is evaluated during pattern matching, not after
- The variable being filtered must be the one defined in that pattern element
- Inline WHERE can reference properties of the current element only (not other pattern variables)
- Inline WHERE combines with label filters (both must match)

**Equivalent queries:**
```sql
-- These are semantically equivalent:
MATCH (n:Person WHERE n.age > 21) RETURN n
MATCH (n:Person) WHERE n.age > 21 RETURN n

-- But inline WHERE is required for edge filtering in complex patterns
MATCH (a)-[r:KNOWS WHERE r.weight > 0.5]->(b)-[s:WORKS_AT]->(c)
-- Cannot easily express r.weight filter in outer WHERE when pattern is complex
```

### 1.3 Grammar Changes

```pest
// Updated node_pattern with optional inline WHERE
node_pattern = { 
    "(" ~ variable? ~ label_filter? ~ property_filter? ~ inline_where? ~ ")" 
}

// Updated edge_pattern with optional inline WHERE
edge_pattern = { 
    left_arrow? ~ "-[" ~ variable? ~ label_filter? ~ quantifier? ~ property_filter? ~ inline_where? ~ "]-" ~ right_arrow?
}

// Inline WHERE clause (distinct from main WHERE)
inline_where = { WHERE ~ expression }
```

### 1.4 AST Changes

```rust
/// A node pattern with optional inline filter
#[derive(Debug, Clone, Serialize)]
pub struct NodePattern {
    /// Optional variable name to bind the matched vertex.
    pub variable: Option<String>,
    /// Labels that the vertex must have.
    pub labels: Vec<String>,
    /// Property constraints as (key, value) pairs.
    pub properties: Vec<(String, Literal)>,
    /// Optional inline WHERE expression.
    pub where_clause: Option<Expression>,
}

/// An edge pattern with optional inline filter
#[derive(Debug, Clone, Serialize)]
pub struct EdgePattern {
    /// Optional variable name to bind the matched edge.
    pub variable: Option<String>,
    /// Relationship types that the edge must have.
    pub labels: Vec<String>,
    /// Direction of the edge in the pattern.
    pub direction: EdgeDirection,
    /// Optional quantifier for variable-length paths.
    pub quantifier: Option<PathQuantifier>,
    /// Property constraints as (key, value) pairs.
    pub properties: Vec<(String, Literal)>,
    /// Optional inline WHERE expression.
    pub where_clause: Option<Expression>,
}
```

### 1.5 Compilation

During pattern matching compilation:

1. If `where_clause` is present on a node/edge pattern, add a filter step immediately after matching that element
2. The filter evaluates the expression with the current element in scope
3. For edges, the filter is applied after matching the edge but before traversing to the next node

```rust
// MATCH (n:Person WHERE n.age > 21)
// Compiles to:
g.v()
    .has_label("Person")
    .filter(|v| v.property("age").map(|a| a > 21).unwrap_or(false))
```

---

## 2. Query Parameters

### 2.1 Description

Support parameterized queries using `$paramName` syntax, allowing safe value injection and query reuse.

**Syntax:**
```sql
-- Parameter in property filter
MATCH (n:Person {id: $personId}) RETURN n

-- Parameter in WHERE clause
MATCH (n:Person) WHERE n.age > $minAge RETURN n

-- Parameter in expression
MATCH (n) RETURN n.value * $multiplier AS scaled

-- Multiple parameters
MATCH (a:Person {id: $fromId})-[:KNOWS]->(b:Person {id: $toId})
RETURN a, b
```

### 2.2 Semantics

- Parameters start with `$` followed by an identifier
- Parameters are resolved at compile time from a provided parameter map
- Unresolved parameters result in a compile error
- Parameters can be used anywhere a literal value is valid
- Parameter values support all `Value` types (String, Int, Float, Bool, List, Map, Null)

### 2.3 Grammar Changes

```pest
// Add parameter as a primary expression
primary = { 
    case_expr
    | exists_expr
    | parameter        // NEW
    | literal
    | function_call
    | property_access
    | variable
    | paren_expr
    | list_expr
    | map_expr         // NEW (see Section 6)
}

// Parameter syntax: $identifier
parameter = @{ "$" ~ identifier }
```

### 2.4 AST Changes

```rust
/// An expression that can be evaluated to produce a value.
#[derive(Debug, Clone, Serialize)]
pub enum Expression {
    // ... existing variants ...
    
    /// Parameter reference: `$paramName`
    ///
    /// References a parameter that will be resolved at compile/execute time.
    Parameter(String),
}
```

### 2.5 Compilation

```rust
/// Parameters passed to query execution
pub type Parameters = HashMap<String, Value>;

/// Compile a query with parameters
pub fn compile_with_params(
    query: &str, 
    params: &Parameters
) -> Result<CompiledQuery, GqlError>;

/// Execute with parameters
pub fn execute_with_params<G: Graph>(
    graph: &G,
    query: &str,
    params: &Parameters,
) -> Result<Vec<Value>, GqlError>;
```

During compilation, parameter references are resolved:

```rust
// When evaluating Expression::Parameter(name):
fn resolve_parameter(&self, name: &str) -> Result<Value, GqlError> {
    self.parameters
        .get(name)
        .cloned()
        .ok_or_else(|| GqlError::UnboundParameter(name.to_string()))
}
```

### 2.6 Error Handling

```rust
#[derive(Debug, Error)]
pub enum GqlError {
    // ... existing variants ...
    
    #[error("unbound parameter: ${0}")]
    UnboundParameter(String),
    
    #[error("parameter type mismatch for ${0}: expected {1}, got {2}")]
    ParameterTypeMismatch(String, String, String),
}
```

---

## 3. LET Clause

### 3.1 Description

The LET clause binds the result of an expression to a variable for use in subsequent clauses. This enables computing intermediate values, especially aggregations, that can be referenced later.

**Syntax:**
```sql
-- Basic LET
MATCH (p:Person)-[:FRIEND]->(f)
LET friendCount = COUNT(f)
RETURN p.name, friendCount

-- LET with COLLECT
MATCH (p:Person)-[:PURCHASED]->(item)
LET purchases = COLLECT(item)
LET totalSpent = SUM(item.price)
RETURN p.name, purchases, totalSpent

-- LET with complex expression
MATCH (p:Person)
LET ageCategory = CASE 
    WHEN p.age < 18 THEN 'minor'
    WHEN p.age < 65 THEN 'adult'
    ELSE 'senior'
END
RETURN p.name, ageCategory

-- Multiple LET clauses
MATCH (person)-[:WORKS_AT]->(company)
LET colleagues = COLLECT(person)
LET companySize = SIZE(colleagues)
LET avgSalary = AVG(person.salary)
RETURN company.name, companySize, avgSalary
```

### 3.2 Semantics

- LET binds an expression result to a new variable
- The variable is available in all subsequent clauses (WHERE, LET, RETURN, ORDER BY)
- LET is evaluated after MATCH/WHERE but before RETURN
- Multiple LET clauses are evaluated in order (later LETs can reference earlier ones)
- LET expressions can include aggregations (COUNT, SUM, COLLECT, etc.)
- LET does not change the cardinality of results (unlike UNWIND)

**Clause ordering:**
```
MATCH -> OPTIONAL MATCH -> WHERE -> LET -> RETURN -> GROUP BY -> ORDER BY -> LIMIT
```

### 3.3 Grammar Changes

```pest
// Add LET keyword
LET = @{ ^"let" ~ !ASCII_ALPHANUMERIC }

// Update keyword list
keyword = {
    MATCH | RETURN | WHERE | ORDER | BY | GROUP | LIMIT | OFFSET |
    AS | AND | OR | NOT | TRUE | FALSE | NULL | ASC | DESC |
    IN | IS | CONTAINS | STARTS | ENDS | WITH | DISTINCT | EXISTS |
    CASE | WHEN | THEN | ELSE | END | UNION | ALL | OPTIONAL |
    PATH | UNWIND | CREATE | SET | REMOVE | DELETE | DETACH | MERGE | ON |
    LET  // NEW
}

// LET clause
let_clause = { LET ~ identifier ~ "=" ~ expression }

// Update query structure
query = { 
    match_clause ~ 
    optional_match_clause* ~ 
    with_path_clause? ~ 
    unwind_clause* ~ 
    where_clause? ~ 
    let_clause* ~           // NEW: zero or more LET clauses
    return_clause ~ 
    group_by_clause? ~ 
    order_clause? ~ 
    limit_clause? 
}
```

### 3.4 AST Changes

```rust
/// The LET clause for binding computed values to variables.
///
/// # Example
/// ```text
/// MATCH (p:Person)-[:FRIEND]->(f)
/// LET friendCount = COUNT(f)
/// RETURN p.name, friendCount
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct LetClause {
    /// The variable name to bind.
    pub variable: String,
    /// The expression to evaluate and bind.
    pub expression: Expression,
}

/// Updated Query struct
#[derive(Debug, Clone, Serialize)]
pub struct Query {
    pub match_clause: MatchClause,
    pub optional_match_clauses: Vec<OptionalMatchClause>,
    pub with_path_clause: Option<WithPathClause>,
    pub unwind_clauses: Vec<UnwindClause>,
    pub where_clause: Option<WhereClause>,
    pub let_clauses: Vec<LetClause>,  // NEW
    pub return_clause: ReturnClause,
    pub group_by_clause: Option<GroupByClause>,
    pub order_clause: Option<OrderClause>,
    pub limit_clause: Option<LimitClause>,
}
```

### 3.5 Compilation

LET clauses are compiled into the evaluation context:

1. After pattern matching and WHERE filtering
2. Each LET expression is evaluated and stored in the variable bindings
3. Subsequent clauses can reference LET variables

```rust
// Compilation pseudocode
fn compile_let_clauses(&mut self, let_clauses: &[LetClause]) {
    for let_clause in let_clauses {
        // Evaluate the expression
        let value = self.evaluate_expression(&let_clause.expression);
        // Bind to variable name
        self.bindings.insert(let_clause.variable.clone(), value);
    }
}
```

For aggregating LET expressions (COUNT, SUM, COLLECT, etc.), the compiler must:

1. Detect that the expression contains an aggregate function
2. Collect all matching rows first
3. Compute the aggregate
4. Bind the single aggregate result to the variable

---

## 4. List Comprehensions

### 4.1 Description

List comprehensions allow transforming and filtering lists in a concise syntax, similar to Python list comprehensions or functional map/filter operations.

**Syntax:**
```sql
-- Basic transformation
[x IN list | x.name]

-- With filter
[x IN list WHERE x.active | x.name]

-- Nested property access
[c IN connections | c.parent.name]

-- Expression transformation
[n IN numbers | n * 2]

-- String building
[t IN types | t.category || ': ' || t.name]

-- Complex expressions
[p IN people | CASE WHEN p.age > 18 THEN 'adult' ELSE 'minor' END]
```

### 4.2 Semantics

- `[variable IN list | expression]` - transforms each element
- `[variable IN list WHERE condition | expression]` - filter then transform
- The variable is scoped to the comprehension only
- Returns a new list with transformed elements
- If the input is NULL or not a list, returns NULL
- Empty list input returns empty list

**Examples:**
```sql
-- Get names from list of people
LET names = [p IN people | p.name]
-- Input: [{name: 'Alice'}, {name: 'Bob'}]
-- Output: ['Alice', 'Bob']

-- Filter and transform
LET adultNames = [p IN people WHERE p.age >= 18 | p.name]
-- Input: [{name: 'Alice', age: 25}, {name: 'Bob', age: 15}]
-- Output: ['Alice']

-- Build formatted strings
LET labels = [t IN types | t.category || '/' || t.name]
-- Input: [{category: 'A', name: 'foo'}, {category: 'B', name: 'bar'}]
-- Output: ['A/foo', 'B/bar']
```

### 4.3 Grammar Changes

```pest
// List comprehension (distinct from list_expr literal)
list_comprehension = { 
    "[" ~ identifier ~ IN ~ expression ~ list_comp_where? ~ "|" ~ expression ~ "]" 
}

// Optional WHERE filter in comprehension
list_comp_where = { WHERE ~ expression }

// Update primary to include list_comprehension
primary = { 
    case_expr
    | exists_expr
    | parameter
    | literal
    | function_call
    | list_comprehension  // NEW - before list_expr to take precedence
    | property_access
    | variable
    | paren_expr
    | list_expr
    | map_expr
}
```

### 4.4 AST Changes

```rust
/// An expression that can be evaluated to produce a value.
#[derive(Debug, Clone, Serialize)]
pub enum Expression {
    // ... existing variants ...
    
    /// List comprehension: `[x IN list WHERE cond | expr]`
    ///
    /// Transforms and optionally filters a list.
    ListComprehension {
        /// Variable name bound to each element.
        variable: String,
        /// The list expression to iterate over.
        list: Box<Expression>,
        /// Optional filter condition.
        filter: Option<Box<Expression>>,
        /// Transformation expression applied to each element.
        transform: Box<Expression>,
    },
}
```

### 4.5 Compilation

```rust
fn evaluate_list_comprehension(
    &self,
    variable: &str,
    list_expr: &Expression,
    filter: Option<&Expression>,
    transform: &Expression,
    context: &EvalContext,
) -> Value {
    // Evaluate the list expression
    let list = match self.evaluate_expression(list_expr, context) {
        Value::List(items) => items,
        Value::Null => return Value::Null,
        _ => return Value::Null, // Non-list returns null
    };
    
    let mut results = Vec::new();
    
    for item in list {
        // Create new context with variable bound to current item
        let mut item_context = context.clone();
        item_context.bindings.insert(variable.to_string(), item.clone());
        
        // Apply filter if present
        if let Some(filter_expr) = filter {
            match self.evaluate_expression(filter_expr, &item_context) {
                Value::Bool(true) => {}
                _ => continue, // Skip items that don't pass filter
            }
        }
        
        // Apply transformation
        let transformed = self.evaluate_expression(transform, &item_context);
        results.push(transformed);
    }
    
    Value::List(results)
}
```

---

## 5. String Concatenation Operator

### 5.1 Description

The `||` operator concatenates strings, following SQL/GQL standard.

**Syntax:**
```sql
-- Basic concatenation
'Hello' || ' ' || 'World'
-- Result: 'Hello World'

-- With properties
p.firstName || ' ' || p.lastName

-- In expressions
RETURN n.type || '/' || n.subtype AS fullType

-- With COALESCE for null handling
COALESCE(p.nickname, p.firstName) || ' ' || p.lastName
```

### 5.2 Semantics

- Concatenates two string values
- If either operand is NULL, result is NULL (SQL standard)
- Non-string operands are converted to strings:
  - Int/Float: Decimal representation
  - Bool: "true" / "false"
  - List: "[elem1, elem2, ...]"
  - Map: "{key1: val1, key2: val2}"
  - Null: propagates NULL
- Lower precedence than arithmetic operators, higher than comparison

**Precedence (lowest to highest):**
```
OR < AND < NOT < comparison < concat (||) < additive (+,-) < multiplicative (*,/,%) < power (^) < unary
```

### 5.3 Grammar Changes

```pest
// Add concatenation level between comparison and additive
// Updated expression precedence chain:
expression = { or_expr }
or_expr = { and_expr ~ (OR ~ and_expr)* }
and_expr = { not_expr ~ (AND ~ not_expr)* }
not_expr = { NOT* ~ comparison }
comparison = { concat_expr ~ (comp_op ~ concat_expr)? | is_null_expr | in_expr }

// NEW: Concatenation operator (between comparison and additive)
concat_expr = { additive ~ (concat_op ~ additive)* }
concat_op = { "||" }

// Rest of precedence chain unchanged
additive = { multiplicative ~ (add_op ~ multiplicative)* }
// ...
```

### 5.4 AST Changes

```rust
/// Binary operators for expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BinaryOperator {
    // ... existing variants ...
    
    /// String concatenation: `||`
    Concat,
}
```

### 5.5 Compilation

```rust
fn evaluate_binary_op(&self, left: &Value, op: BinaryOperator, right: &Value) -> Value {
    match op {
        BinaryOperator::Concat => {
            match (left, right) {
                (Value::Null, _) | (_, Value::Null) => Value::Null,
                (l, r) => {
                    let left_str = self.value_to_string(l);
                    let right_str = self.value_to_string(r);
                    Value::String(format!("{}{}", left_str, right_str))
                }
            }
        }
        // ... other operators
    }
}

fn value_to_string(&self, value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Int(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::List(items) => {
            let inner: Vec<String> = items.iter()
                .map(|v| self.value_to_string(v))
                .collect();
            format!("[{}]", inner.join(", "))
        }
        Value::Map(m) => {
            let inner: Vec<String> = m.iter()
                .map(|(k, v)| format!("{}: {}", k, self.value_to_string(v)))
                .collect();
            format!("{{{}}}", inner.join(", "))
        }
        Value::Vertex(id) => format!("vertex({:?})", id),
        Value::Edge(id) => format!("edge({:?})", id),
    }
}
```

---

## 6. Map Literals in Expressions

### 6.1 Description

Support map/object literals in expressions, particularly for use with COLLECT and in RETURN clauses.

**Syntax:**
```sql
-- Map literal
{name: 'Alice', age: 30}

-- Map with property references
{personName: p.name, personAge: p.age}

-- In COLLECT
LET data = COLLECT({parent: parent, type: event.type})

-- In RETURN
RETURN {
    name: p.name,
    stats: {
        friends: friendCount,
        posts: postCount
    }
} AS profile
```

### 6.2 Semantics

- Map literals create `Value::Map` instances
- Keys must be identifiers (unquoted) or string literals
- Values can be any expression
- Maps are ordered (preserve insertion order)
- Nested maps are supported

### 6.3 Grammar Changes

```pest
// Map literal expression
map_expr = { "{" ~ (map_entry ~ ("," ~ map_entry)*)? ~ "}" }
map_entry = { map_key ~ ":" ~ expression }
map_key = { identifier | string }

// Update primary to include map_expr
primary = { 
    case_expr
    | exists_expr
    | parameter
    | literal
    | function_call
    | list_comprehension
    | property_access
    | variable
    | paren_expr
    | list_expr
    | map_expr  // NEW
}
```

**Note:** Map syntax `{key: value}` conflicts with property filter syntax in patterns. The parser must distinguish based on context:
- In patterns: `{key: literal}` is a property filter
- In expressions: `{key: expression}` is a map literal

### 6.4 AST Changes

```rust
/// An expression that can be evaluated to produce a value.
#[derive(Debug, Clone, Serialize)]
pub enum Expression {
    // ... existing variants ...
    
    /// Map literal: `{key: value, ...}`
    ///
    /// Creates a map/object value.
    Map(Vec<(String, Expression)>),
}
```

### 6.5 Compilation

```rust
fn evaluate_map_literal(
    &self,
    entries: &[(String, Expression)],
    context: &EvalContext,
) -> Value {
    let mut map = IndexMap::new();
    
    for (key, value_expr) in entries {
        let value = self.evaluate_expression(value_expr, context);
        map.insert(key.clone(), value);
    }
    
    Value::Map(map)
}
```

---

## 7. Implementation Plan

### Phase 1: String Concatenation (Low effort)

1. Add `concat_op` rule to grammar
2. Add `BinaryOperator::Concat` variant
3. Update parser to handle `||`
4. Implement concatenation in compiler
5. Add tests

**Estimated: ~50 lines of code**

### Phase 2: Query Parameters (Low-Medium effort)

1. Add `parameter` rule to grammar
2. Add `Expression::Parameter` variant
3. Update parser
4. Add `Parameters` type and `compile_with_params` API
5. Implement parameter resolution in compiler
6. Add error handling for unbound parameters
7. Add tests

**Estimated: ~120 lines of code**

### Phase 3: Map Literals (Medium effort)

1. Add `map_expr` and `map_entry` rules to grammar
2. Add `Expression::Map` variant
3. Update parser (handle context disambiguation)
4. Implement map evaluation in compiler
5. Add tests

**Estimated: ~100 lines of code**

### Phase 4: Inline WHERE in Patterns (Medium effort)

1. Add `inline_where` rule to grammar
2. Update `NodePattern` and `EdgePattern` structs
3. Update parser for node/edge patterns
4. Modify pattern compilation to add filter steps
5. Add tests

**Estimated: ~180 lines of code**

### Phase 5: LET Clause (Medium-High effort)

1. Add `LET` keyword and `let_clause` rule
2. Add `LetClause` struct
3. Update `Query` struct
4. Update parser
5. Implement LET evaluation in compiler
6. Handle aggregating expressions in LET
7. Add tests

**Estimated: ~280 lines of code**

### Phase 6: List Comprehensions (Medium-High effort)

1. Add `list_comprehension` rule to grammar
2. Add `Expression::ListComprehension` variant
3. Update parser
4. Implement comprehension evaluation
5. Handle nested comprehensions
6. Add tests

**Estimated: ~200 lines of code**

---

## 8. Testing Requirements

### 8.1 Parser Tests

```rust
#[test] fn test_parse_inline_where_node() { }
#[test] fn test_parse_inline_where_edge() { }
#[test] fn test_parse_inline_where_complex_pattern() { }
#[test] fn test_parse_parameter_in_property() { }
#[test] fn test_parse_parameter_in_where() { }
#[test] fn test_parse_parameter_in_expression() { }
#[test] fn test_parse_let_simple() { }
#[test] fn test_parse_let_with_aggregate() { }
#[test] fn test_parse_let_multiple() { }
#[test] fn test_parse_list_comprehension_basic() { }
#[test] fn test_parse_list_comprehension_with_filter() { }
#[test] fn test_parse_list_comprehension_nested() { }
#[test] fn test_parse_concat_strings() { }
#[test] fn test_parse_concat_with_properties() { }
#[test] fn test_parse_map_literal() { }
#[test] fn test_parse_map_nested() { }
#[test] fn test_parse_map_in_collect() { }
```

### 8.2 Compiler Tests

```rust
#[test] fn test_compile_inline_where_filters_during_match() { }
#[test] fn test_compile_parameter_resolution() { }
#[test] fn test_compile_parameter_unbound_error() { }
#[test] fn test_compile_let_binds_value() { }
#[test] fn test_compile_let_aggregate_collect() { }
#[test] fn test_compile_let_references_previous_let() { }
#[test] fn test_compile_list_comprehension_transform() { }
#[test] fn test_compile_list_comprehension_filter() { }
#[test] fn test_compile_concat_strings() { }
#[test] fn test_compile_concat_null_propagation() { }
#[test] fn test_compile_concat_type_coercion() { }
#[test] fn test_compile_map_literal() { }
#[test] fn test_compile_map_with_expressions() { }
```

### 8.3 Integration Tests

```rust
#[test]
fn test_complex_family_query() {
    // The motivating query from the overview
    let query = r#"
        MATCH (person:Person WHERE person.id = $personId)
              -[:PARTICIPATED_IN WHERE role = 'child']->(personEvent)
        ...
    "#;
    // Execute and verify results
}

#[test]
fn test_parameterized_query_reuse() {
    // Same query, different parameters
}

#[test]
fn test_let_with_group_by() {
    // Verify LET works correctly with aggregation
}

#[test]
fn test_list_comprehension_string_building() {
    // Build strings from list elements
}
```

### 8.4 Snapshot Tests

Add insta snapshots for AST structure:

```rust
#[test] fn parse_inline_where_snapshot() { }
#[test] fn parse_parameter_snapshot() { }
#[test] fn parse_let_clause_snapshot() { }
#[test] fn parse_list_comprehension_snapshot() { }
#[test] fn parse_concat_snapshot() { }
#[test] fn parse_map_literal_snapshot() { }
```

---

## 9. Error Handling

### 9.1 New Error Types

```rust
#[derive(Debug, Error)]
pub enum GqlError {
    // ... existing variants ...
    
    /// Unbound parameter reference
    #[error("unbound parameter: ${0}")]
    UnboundParameter(String),
    
    /// Invalid inline WHERE - references wrong variable
    #[error("inline WHERE in pattern for '{0}' cannot reference variable '{1}'")]
    InvalidInlineWhereReference(String, String),
    
    /// LET variable shadows existing binding
    #[error("LET variable '{0}' shadows existing binding")]
    LetVariableShadows(String),
    
    /// Invalid list comprehension - not iterating over list
    #[error("list comprehension requires list, got {0}")]
    ListComprehensionNotList(String),
    
    /// Concat type error
    #[error("cannot concatenate {0} and {1}")]
    ConcatTypeError(String, String),
}
```

---

## 10. Example Usage

### 10.1 Parameterized Query

```rust
use interstellar::gql::{execute_with_params, Parameters};
use interstellar::Value;

let mut params = Parameters::new();
params.insert("personId".to_string(), Value::Int(123));
params.insert("minAge".to_string(), Value::Int(18));

let results = execute_with_params(
    &graph,
    "MATCH (p:Person {id: $personId})-[:FRIEND]->(f) 
     WHERE f.age >= $minAge 
     RETURN f.name",
    &params,
)?;
```

### 10.2 LET with Aggregation

```rust
let results = execute(
    &graph,
    "MATCH (company:Company)<-[:WORKS_AT]-(employee:Person)
     LET employeeCount = COUNT(employee)
     LET avgSalary = AVG(employee.salary)
     LET topEarners = COLLECT(employee.name)
     RETURN company.name, employeeCount, avgSalary, topEarners
     GROUP BY company",
)?;
```

### 10.3 List Comprehension

```rust
let results = execute(
    &graph,
    "MATCH (p:Person)-[:PURCHASED]->(item:Product)
     LET purchases = COLLECT({name: item.name, price: item.price})
     LET itemNames = [i IN purchases | i.name]
     LET formattedPrices = [i IN purchases | '$' || toString(i.price)]
     RETURN p.name, itemNames, formattedPrices",
)?;
```

### 10.4 Complex Analytical Query

```rust
let results = execute_with_params(
    &graph,
    "MATCH (person:Person WHERE person.id = $personId)
           -[r1:PARTICIPATED_IN WHERE r1.role = 'child']->(birthEvent:Birth)
           <-[r2:PARTICIPATED_IN WHERE r2.role = 'parent']-(parent:Person),
           (parent)-[:PARTICIPATED_IN]->(otherBirth:Birth)
           <-[r3:PARTICIPATED_IN WHERE r3.role = 'child']-(sibling:Person)
     WHERE sibling <> person
     LET siblingInfo = COLLECT({
         sibling: sibling,
         parent: parent,
         sharedEvent: birthEvent
     })
     RETURN sibling.name,
            SIZE(siblingInfo) AS connectionCount,
            [s IN siblingInfo | s.parent.name] AS sharedParents
     GROUP BY sibling",
    &params,
)?;
```

---

## 11. Future Enhancements

After this spec is implemented, potential future work includes:

- **REDUCE function**: `REDUCE(s = '', x IN list | s || x)`
- **Pattern comprehensions**: `[(p)-[:KNOWS]->(f) | f.name]`
- **Regular expression predicates**: `WHERE n.name =~ 'A.*'`
- **Existential subqueries in expressions**: `WHERE EXISTS { MATCH ... }`
- **FOREACH clause**: `FOREACH (x IN list | SET x.prop = val)`
- **Type predicates**: `WHERE n IS Person`
- **Quantified path patterns**: `MATCH (a)((n)-[:KNOWS]->(m)){2,5}(b)`
