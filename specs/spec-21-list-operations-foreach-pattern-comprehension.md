# Spec 21: FOREACH Clause, List Operations, and Pattern Comprehension

## Overview

This specification defines three GQL features that enhance list handling and iteration capabilities in Intersteller:

1. **FOREACH Clause** - Apply mutations to each element during iteration
2. **List Slicing and Indexing** - Access list elements by index and extract sublists
3. **Pattern Comprehension** - Inline pattern matching in expressions

These features address key gaps in GQL completeness and enable more expressive queries.

### Target Queries

```sql
-- FOREACH: Mark all nodes in a path as visited
MATCH p = (start:Person {name: 'Alice'})-[*]->(end)
FOREACH (n IN nodes(p) | SET n.visited = true)
RETURN end.name

-- List indexing and slicing
MATCH (p:Person)
RETURN p.tags[0] AS firstTag,           -- Index access
       p.scores[-1] AS lastScore,        -- Negative indexing
       p.history[1..3] AS recentHistory  -- Slice

-- Pattern comprehension
MATCH (p:Person)
RETURN p.name, [(p)-[:FRIEND]->(f) | f.name] AS friendNames
```

## Goals

1. **FOREACH Clause** - Iterate over collections and apply mutations to each element
2. **List Index Access** - Access elements by positive/negative index (`list[2]`, `list[-1]`)
3. **List Slicing** - Extract sublists with range syntax (`list[1..3]`, `list[..5]`, `list[2..]`)
4. **Pattern Comprehension** - Inline pattern matching that returns a list of transformed results

## Non-Goals

- FOREACH with nested FOREACH (future work)
- Pattern comprehension with WHERE clause (may be added later)
- Multi-dimensional list slicing
- Step/stride in slicing (`list[::2]`)

---

## 1. FOREACH Clause

### 1.1 Description

FOREACH iterates over a collection and applies mutation operations to each element. It is used for side effects within a query, typically to update properties on multiple elements.

**Syntax:**
```sql
-- Basic FOREACH
FOREACH (variable IN expression | mutation_operations)

-- Mark nodes visited
MATCH p = (a:Person)-[*]->(b)
FOREACH (n IN nodes(p) | SET n.visited = true)

-- Update properties on collected items
MATCH (team:Team)<-[:PLAYS_FOR]-(player:Player)
WITH team, COLLECT(player) AS players
FOREACH (p IN players | SET p.teamName = team.name)

-- Multiple mutations per iteration
MATCH (order:Order)-[:CONTAINS]->(item:Item)
WITH order, COLLECT(item) AS items
FOREACH (i IN items | 
    SET i.processed = true
    SET i.processedAt = $timestamp
    REMOVE i.pending
)

-- Nested FOREACH (future work, not in this spec)
-- FOREACH (x IN outerList |
--     FOREACH (y IN x.innerList | SET y.prop = value)
-- )
```

### 1.2 Semantics

- FOREACH iterates over a list expression
- For each element, the variable is bound and mutation operations are executed
- FOREACH does NOT produce rows - it only performs side effects
- FOREACH does NOT change query cardinality
- Mutations inside FOREACH can be: SET, REMOVE, DELETE, DETACH DELETE, CREATE
- FOREACH is executed after MATCH/WHERE/WITH but can appear multiple times
- The variable is only in scope within the FOREACH body
- If the list is NULL or empty, no iterations occur

**Execution Model:**
```
MATCH -> WHERE -> WITH -> FOREACH -> FOREACH -> ... -> RETURN
                         ^side effects only^
```

**Error Conditions:**
- Expression must evaluate to a list (error if not)
- Mutations must be valid (e.g., can't SET on null)
- Variable name cannot shadow outer scope variables

### 1.3 Grammar Changes

```pest
// Add FOREACH keyword
FOREACH = @{ ^"foreach" ~ !ASCII_ALPHANUMERIC }

// Update keyword list
keyword = {
    // ... existing keywords ...
    FOREACH  // NEW
}

// FOREACH clause
foreach_clause = { 
    FOREACH ~ "(" ~ identifier ~ IN ~ expression ~ "|" ~ foreach_body ~ ")" 
}

// Body contains one or more mutation operations
foreach_body = { foreach_mutation+ }

// Mutations allowed in FOREACH (subset of mutation_clause)
foreach_mutation = { 
    set_clause 
    | remove_clause 
    | delete_clause 
    | detach_delete_clause 
    | create_clause 
}

// Update query structure to include FOREACH
query = { 
    match_clause ~ 
    optional_match_clause* ~ 
    with_path_clause? ~ 
    unwind_clause* ~ 
    where_clause? ~ 
    call_clause* ~
    let_clause* ~ 
    with_clause* ~
    foreach_clause* ~        // NEW: zero or more FOREACH clauses
    return_clause ~ 
    group_by_clause? ~ 
    having_clause? ~ 
    order_clause? ~ 
    limit_clause? 
}

// Also allow FOREACH in mutation statements
match_mutation_statement = {
    match_clause ~
    optional_match_clause* ~
    where_clause? ~
    foreach_clause* ~       // NEW
    mutation_clause+ ~
    return_clause?
}
```

### 1.4 AST Changes

```rust
/// FOREACH clause for iterating with side effects.
///
/// FOREACH iterates over a list and applies mutations to each element.
/// It does not produce rows or change query cardinality.
///
/// # Examples
///
/// ```text
/// // Mark all nodes in path as visited
/// MATCH p = (a)-[*]->(b)
/// FOREACH (n IN nodes(p) | SET n.visited = true)
///
/// // Update multiple properties
/// MATCH (team:Team)<-[:PLAYS_FOR]-(player)
/// WITH team, COLLECT(player) AS players
/// FOREACH (p IN players | 
///     SET p.teamName = team.name
///     SET p.updated = true
/// )
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct ForeachClause {
    /// Variable name bound to each element during iteration.
    pub variable: String,
    /// Expression that produces a list to iterate over.
    pub list: Expression,
    /// Mutations to apply for each element.
    pub mutations: Vec<ForeachMutation>,
}

/// Mutations allowed inside FOREACH.
///
/// A subset of MutationClause that makes sense in iteration context.
#[derive(Debug, Clone, Serialize)]
pub enum ForeachMutation {
    /// SET clause - update properties.
    Set(SetClause),
    /// REMOVE clause - remove properties.
    Remove(RemoveClause),
    /// DELETE clause - delete elements.
    Delete(DeleteClause),
    /// DETACH DELETE clause - delete with edge removal.
    DetachDelete(DetachDeleteClause),
    /// CREATE clause - create new elements.
    Create(CreateClause),
}

/// Update Query struct
#[derive(Debug, Clone, Serialize)]
pub struct Query {
    pub match_clause: MatchClause,
    pub optional_match_clauses: Vec<OptionalMatchClause>,
    pub with_path_clause: Option<WithPathClause>,
    pub unwind_clauses: Vec<UnwindClause>,
    pub where_clause: Option<WhereClause>,
    pub call_clauses: Vec<CallClause>,
    pub let_clauses: Vec<LetClause>,
    pub with_clauses: Vec<WithClause>,
    pub foreach_clauses: Vec<ForeachClause>,  // NEW
    pub return_clause: ReturnClause,
    pub group_by_clause: Option<GroupByClause>,
    pub having_clause: Option<HavingClause>,
    pub order_clause: Option<OrderClause>,
    pub limit_clause: Option<LimitClause>,
}
```

### 1.5 Compilation

FOREACH executes as a side-effect loop during query execution:

```rust
fn execute_foreach<G: GraphMut>(
    &mut self,
    graph: &mut G,
    foreach_clause: &ForeachClause,
    context: &EvalContext,
) -> Result<(), GqlError> {
    // Evaluate the list expression
    let list = match self.evaluate_expression(&foreach_clause.list, context)? {
        Value::List(items) => items,
        Value::Null => return Ok(()), // NULL list = no iterations
        other => return Err(GqlError::ForeachNotList(type_name(&other))),
    };
    
    // Iterate over each element
    for item in list {
        // Create iteration context with variable bound
        let mut iter_context = context.clone();
        iter_context.bindings.insert(
            foreach_clause.variable.clone(), 
            item
        );
        
        // Execute each mutation
        for mutation in &foreach_clause.mutations {
            self.execute_foreach_mutation(graph, mutation, &iter_context)?;
        }
    }
    
    Ok(())
}

fn execute_foreach_mutation<G: GraphMut>(
    &mut self,
    graph: &mut G,
    mutation: &ForeachMutation,
    context: &EvalContext,
) -> Result<(), GqlError> {
    match mutation {
        ForeachMutation::Set(set_clause) => {
            self.execute_set(graph, set_clause, context)
        }
        ForeachMutation::Remove(remove_clause) => {
            self.execute_remove(graph, remove_clause, context)
        }
        ForeachMutation::Delete(delete_clause) => {
            self.execute_delete(graph, delete_clause, context)
        }
        ForeachMutation::DetachDelete(detach_clause) => {
            self.execute_detach_delete(graph, detach_clause, context)
        }
        ForeachMutation::Create(create_clause) => {
            self.execute_create(graph, create_clause, context)
        }
    }
}
```

---

## 2. List Indexing and Slicing

### 2.1 Description

Support accessing list elements by index and extracting sublists using slice syntax.

**Syntax:**
```sql
-- Positive index (0-based)
list[0]      -- First element
list[2]      -- Third element

-- Negative index (from end)
list[-1]     -- Last element
list[-2]     -- Second to last

-- Slicing (start..end, end exclusive)
list[1..3]   -- Elements at index 1, 2 (not 3)
list[..3]    -- First 3 elements (0, 1, 2)
list[2..]    -- From index 2 to end
list[..]     -- Copy of entire list

-- Negative indices in slices
list[-3..]   -- Last 3 elements
list[..-1]   -- All but last element
list[-3..-1] -- Third-to-last and second-to-last

-- Practical examples
MATCH (p:Person)
RETURN p.name,
       p.scores[0] AS firstScore,
       p.scores[-1] AS lastScore,
       p.tags[0..3] AS topThreeTags,
       p.history[-5..] AS recentHistory
```

### 2.2 Semantics

**Index Access:**
- `list[i]` returns element at index `i`
- Indices are 0-based
- Negative indices count from end: `-1` is last, `-2` is second-to-last
- Out-of-bounds index returns NULL
- Index on NULL list returns NULL
- Index on non-list returns NULL

**Slicing:**
- `list[start..end]` returns elements from `start` (inclusive) to `end` (exclusive)
- Omitted `start` defaults to 0: `list[..3]` = `list[0..3]`
- Omitted `end` defaults to list length: `list[2..]` = rest of list
- Negative indices work in slices
- Out-of-bounds slice bounds are clamped (no error)
- Slice on NULL list returns NULL
- Empty slice returns empty list

**Index Resolution:**
```
list = [a, b, c, d, e]  (length 5)

Positive:  0  1  2  3  4
Negative: -5 -4 -3 -2 -1

list[1]   = b
list[-1]  = e
list[-2]  = d
list[1..3] = [b, c]
list[-2..] = [d, e]
list[..-1] = [a, b, c, d]
```

### 2.3 Grammar Changes

```pest
// Index/slice access is a postfix operation on primary expressions
// This requires restructuring to handle postfix operators

// New: postfix expression wraps primary and allows [index] or [slice]
postfix_expr = { primary ~ index_access* }

// Index access: [expr] or [start..end]
index_access = { "[" ~ (slice_range | expression) ~ "]" }

// Slice range: start..end, ..end, start.., or ..
slice_range = { 
    expression? ~ ".." ~ expression? 
}

// Update unary to use postfix_expr instead of primary
unary = { neg_op? ~ postfix_expr }
```

### 2.4 AST Changes

```rust
/// An expression that can be evaluated to produce a value.
#[derive(Debug, Clone, Serialize)]
pub enum Expression {
    // ... existing variants ...
    
    /// Index access: `list[index]`
    ///
    /// Accesses a single element from a list by index.
    /// Negative indices count from the end.
    ///
    /// # Examples
    ///
    /// ```text
    /// list[0]   -- First element
    /// list[-1]  -- Last element
    /// list[2]   -- Third element
    /// ```
    Index {
        /// The list expression to index into.
        list: Box<Expression>,
        /// The index expression (should evaluate to integer).
        index: Box<Expression>,
    },
    
    /// Slice access: `list[start..end]`
    ///
    /// Extracts a sublist from start (inclusive) to end (exclusive).
    /// Omitted bounds default to start of list or end of list.
    ///
    /// # Examples
    ///
    /// ```text
    /// list[1..3]   -- Elements 1 and 2
    /// list[..3]    -- First 3 elements  
    /// list[2..]    -- From element 2 to end
    /// list[-3..]   -- Last 3 elements
    /// ```
    Slice {
        /// The list expression to slice.
        list: Box<Expression>,
        /// Start index (None = beginning of list).
        start: Option<Box<Expression>>,
        /// End index (None = end of list).
        end: Option<Box<Expression>>,
    },
}
```

### 2.5 Compilation

```rust
fn evaluate_index(
    &self,
    list_expr: &Expression,
    index_expr: &Expression,
    context: &EvalContext,
) -> Value {
    let list = match self.evaluate_expression(list_expr, context) {
        Value::List(items) => items,
        Value::Null => return Value::Null,
        _ => return Value::Null, // Non-list returns null
    };
    
    let index = match self.evaluate_expression(index_expr, context) {
        Value::Int(i) => i,
        Value::Null => return Value::Null,
        _ => return Value::Null, // Non-integer index returns null
    };
    
    // Resolve negative index
    let len = list.len() as i64;
    let resolved_index = if index < 0 {
        len + index
    } else {
        index
    };
    
    // Bounds check
    if resolved_index < 0 || resolved_index >= len {
        return Value::Null; // Out of bounds returns null
    }
    
    list[resolved_index as usize].clone()
}

fn evaluate_slice(
    &self,
    list_expr: &Expression,
    start_expr: Option<&Expression>,
    end_expr: Option<&Expression>,
    context: &EvalContext,
) -> Value {
    let list = match self.evaluate_expression(list_expr, context) {
        Value::List(items) => items,
        Value::Null => return Value::Null,
        _ => return Value::Null,
    };
    
    let len = list.len() as i64;
    
    // Resolve start index (default to 0)
    let start = match start_expr {
        Some(expr) => match self.evaluate_expression(expr, context) {
            Value::Int(i) => self.resolve_slice_index(i, len),
            _ => 0,
        },
        None => 0,
    };
    
    // Resolve end index (default to len)
    let end = match end_expr {
        Some(expr) => match self.evaluate_expression(expr, context) {
            Value::Int(i) => self.resolve_slice_index(i, len),
            _ => len as usize,
        },
        None => len as usize,
    };
    
    // Clamp and extract slice
    let start = start.min(list.len());
    let end = end.min(list.len()).max(start);
    
    Value::List(list[start..end].to_vec())
}

/// Resolve a slice index, handling negative values.
/// Clamps result to [0, len].
fn resolve_slice_index(&self, index: i64, len: i64) -> usize {
    let resolved = if index < 0 {
        (len + index).max(0)
    } else {
        index.min(len)
    };
    resolved as usize
}
```

---

## 3. Pattern Comprehension

### 3.1 Description

Pattern comprehension allows inline pattern matching within expressions, returning a list of transformed results. It's similar to list comprehension but uses a graph pattern as the source.

**Syntax:**
```sql
-- Basic pattern comprehension
[(pattern) | transform_expression]

-- Get friend names
MATCH (p:Person)
RETURN p.name, [(p)-[:FRIEND]->(f) | f.name] AS friendNames

-- Get relationship properties
MATCH (p:Person)
RETURN p.name, [(p)-[r:KNOWS]->(other) | r.since] AS knowsSince

-- Complex transform
MATCH (company:Company)
RETURN company.name,
       [(company)<-[:WORKS_AT]-(emp) | {name: emp.name, role: emp.role}] AS employees

-- With labels and properties in pattern
MATCH (p:Person)
RETURN p.name,
       [(p)-[:PURCHASED]->(item:Product {category: 'electronics'}) | item.name] AS electronics

-- Multiple hops
MATCH (p:Person)
RETURN p.name,
       [(p)-[:FRIEND]->()-[:FRIEND]->(fof) | fof.name] AS friendsOfFriends
```

### 3.2 Semantics

- Pattern comprehension matches a pattern starting from current context
- For each match, the transform expression is evaluated
- Results are collected into a list
- If pattern matches nothing, returns empty list
- Variables in pattern are scoped to the comprehension
- Outer variables can be referenced in the pattern (for correlation)
- The pattern must be connected to at least one outer variable

**Execution Model:**
1. For each row in the current result set
2. Execute the pattern match starting from referenced outer variables
3. For each pattern match, evaluate the transform expression
4. Collect all transform results into a list
5. Continue with the list as the expression result

**Example Execution:**
```sql
MATCH (p:Person)
RETURN p.name, [(p)-[:FRIEND]->(f) | f.name] AS friendNames

-- For person Alice with friends Bob, Carol:
-- 1. Match (Alice)-[:FRIEND]->(Bob) -> transform: "Bob"
-- 2. Match (Alice)-[:FRIEND]->(Carol) -> transform: "Carol"  
-- 3. Result: ["Bob", "Carol"]
```

### 3.3 Grammar Changes

```pest
// Pattern comprehension in primary expressions
pattern_comprehension = { 
    "[" ~ pattern ~ pipe_token ~ expression ~ "]" 
}

// Update primary to include pattern_comprehension
// Note: Must come before list_expr to avoid ambiguity
primary = { 
    case_expr
    | exists_expr
    | reduce_expr
    | all_predicate
    | any_predicate
    | none_predicate
    | single_predicate
    | parameter
    | literal
    | function_call
    | list_comprehension
    | pattern_comprehension   // NEW - before list_expr
    | property_access
    | variable
    | paren_expr
    | list_expr
    | map_expr
}
```

**Parser Disambiguation:**

Pattern comprehension and list comprehension both start with `[`. The parser distinguishes by:
- List comprehension: `[identifier IN ...]`
- Pattern comprehension: `[(pattern) ...]` or `[pattern_starting_with_node ...]`

Since node patterns start with `(`, and list comprehensions start with an identifier followed by `IN`, the grammar is unambiguous.

### 3.4 AST Changes

```rust
/// An expression that can be evaluated to produce a value.
#[derive(Debug, Clone, Serialize)]
pub enum Expression {
    // ... existing variants ...
    
    /// Pattern comprehension: `[(pattern) | expression]`
    ///
    /// Matches a pattern and transforms each match into a list element.
    /// The pattern typically references variables from the outer scope.
    ///
    /// # Examples
    ///
    /// ```text
    /// // Get friend names
    /// [(p)-[:FRIEND]->(f) | f.name]
    ///
    /// // Get relationship data
    /// [(p)-[r:KNOWS]->(other) | {person: other.name, since: r.since}]
    ///
    /// // Multi-hop pattern
    /// [(p)-[:FRIEND]->()-[:FRIEND]->(fof) | fof.name]
    /// ```
    PatternComprehension {
        /// The pattern to match.
        pattern: Pattern,
        /// Expression to evaluate for each match.
        transform: Box<Expression>,
    },
}
```

### 3.5 Compilation

Pattern comprehension requires executing a nested pattern match:

```rust
fn evaluate_pattern_comprehension(
    &self,
    pattern: &Pattern,
    transform: &Expression,
    context: &EvalContext,
) -> Result<Value, GqlError> {
    // Identify outer variables referenced in pattern
    let outer_refs = self.find_outer_references(pattern, context);
    
    if outer_refs.is_empty() {
        return Err(GqlError::PatternComprehensionNotCorrelated);
    }
    
    // Execute pattern match from outer context
    let matches = self.execute_pattern_match(pattern, context)?;
    
    // Transform each match
    let mut results = Vec::new();
    for match_bindings in matches {
        // Merge outer context with match bindings
        let mut transform_context = context.clone();
        transform_context.bindings.extend(match_bindings);
        
        // Evaluate transform expression
        let value = self.evaluate_expression(transform, &transform_context)?;
        results.push(value);
    }
    
    Ok(Value::List(results))
}

fn find_outer_references(
    &self, 
    pattern: &Pattern, 
    context: &EvalContext
) -> Vec<String> {
    // Scan pattern for variables that exist in outer context
    let pattern_vars = self.extract_pattern_variables(pattern);
    
    pattern_vars
        .into_iter()
        .filter(|v| context.bindings.contains_key(v))
        .collect()
}

fn extract_pattern_variables(&self, pattern: &Pattern) -> HashSet<String> {
    let mut vars = HashSet::new();
    
    for element in &pattern.elements {
        match element {
            PatternElement::Node(node) => {
                if let Some(ref var) = node.variable {
                    vars.insert(var.clone());
                }
            }
            PatternElement::Edge(edge) => {
                if let Some(ref var) = edge.variable {
                    vars.insert(var.clone());
                }
            }
        }
    }
    
    vars
}
```

### 3.6 Pattern Comprehension vs Existing Features

| Feature | Syntax | Use Case |
|---------|--------|----------|
| List Comprehension | `[x IN list \| expr]` | Transform existing list |
| Pattern Comprehension | `[(pattern) \| expr]` | Match pattern, collect results |
| COLLECT aggregate | `COLLECT(expr)` | Aggregate across rows |
| CALL subquery | `CALL { ... RETURN }` | Complex nested queries |

Pattern comprehension is most useful when:
- You need to match a pattern and collect results inline
- The pattern is simple (no WHERE, ORDER BY, LIMIT needed)
- You want a list result without changing row cardinality

---

## 4. Implementation Plan

### Phase 1: List Indexing (Medium effort, ~150 lines)

1. Add `index_access` and postfix expression rules to grammar
2. Add `Expression::Index` variant
3. Update parser for postfix index access
4. Implement index evaluation with negative index support
5. Add tests for edge cases (out of bounds, NULL, non-list)

**Estimated: ~150 lines**

### Phase 2: List Slicing (Medium effort, ~180 lines)

1. Add `slice_range` rule to grammar
2. Add `Expression::Slice` variant
3. Update parser to distinguish index from slice
4. Implement slice evaluation with negative indices
5. Implement bounds clamping
6. Add tests for all slice variants

**Estimated: ~180 lines**

### Phase 3: Pattern Comprehension (High effort, ~300 lines)

1. Add `pattern_comprehension` rule to grammar
2. Add `Expression::PatternComprehension` variant
3. Update parser (handle disambiguation from list comprehension)
4. Implement pattern match execution within expression context
5. Implement transform evaluation for each match
6. Add tests for correlated patterns, empty results, complex transforms

**Estimated: ~300 lines**

### Phase 4: FOREACH Clause (High effort, ~350 lines)

1. Add `FOREACH` keyword and `foreach_clause` rule to grammar
2. Add `ForeachClause` and `ForeachMutation` types
3. Update `Query` struct
4. Update parser for FOREACH in queries and mutations
5. Implement FOREACH execution loop
6. Implement each mutation type within FOREACH
7. Add tests for various mutation combinations
8. Add error handling for invalid FOREACH usage

**Estimated: ~350 lines**

---

## 5. Testing Requirements

### 5.1 Parser Tests

```rust
// List indexing
#[test] fn test_parse_list_index_positive() { }
#[test] fn test_parse_list_index_negative() { }
#[test] fn test_parse_list_index_expression() { }
#[test] fn test_parse_list_index_chained() { }

// List slicing
#[test] fn test_parse_slice_full_range() { }
#[test] fn test_parse_slice_start_only() { }
#[test] fn test_parse_slice_end_only() { }
#[test] fn test_parse_slice_negative_indices() { }
#[test] fn test_parse_slice_empty() { }

// Pattern comprehension
#[test] fn test_parse_pattern_comprehension_simple() { }
#[test] fn test_parse_pattern_comprehension_with_labels() { }
#[test] fn test_parse_pattern_comprehension_multi_hop() { }
#[test] fn test_parse_pattern_comprehension_complex_transform() { }

// FOREACH
#[test] fn test_parse_foreach_single_set() { }
#[test] fn test_parse_foreach_multiple_mutations() { }
#[test] fn test_parse_foreach_with_nodes_function() { }
#[test] fn test_parse_foreach_in_mutation_statement() { }
```

### 5.2 Compiler Tests

```rust
// List indexing
#[test] fn test_compile_index_positive() { }
#[test] fn test_compile_index_negative() { }
#[test] fn test_compile_index_out_of_bounds() { }
#[test] fn test_compile_index_null_list() { }
#[test] fn test_compile_index_non_list() { }
#[test] fn test_compile_index_null_index() { }

// List slicing
#[test] fn test_compile_slice_full_range() { }
#[test] fn test_compile_slice_start_omitted() { }
#[test] fn test_compile_slice_end_omitted() { }
#[test] fn test_compile_slice_negative_start() { }
#[test] fn test_compile_slice_negative_end() { }
#[test] fn test_compile_slice_out_of_bounds_clamped() { }
#[test] fn test_compile_slice_null_list() { }
#[test] fn test_compile_slice_empty_result() { }

// Pattern comprehension
#[test] fn test_compile_pattern_comprehension_basic() { }
#[test] fn test_compile_pattern_comprehension_empty_matches() { }
#[test] fn test_compile_pattern_comprehension_with_properties() { }
#[test] fn test_compile_pattern_comprehension_map_transform() { }
#[test] fn test_compile_pattern_comprehension_multi_hop() { }
#[test] fn test_compile_pattern_comprehension_uncorrelated_error() { }

// FOREACH
#[test] fn test_compile_foreach_set_property() { }
#[test] fn test_compile_foreach_remove_property() { }
#[test] fn test_compile_foreach_multiple_mutations() { }
#[test] fn test_compile_foreach_empty_list() { }
#[test] fn test_compile_foreach_null_list() { }
#[test] fn test_compile_foreach_non_list_error() { }
#[test] fn test_compile_foreach_variable_scope() { }
```

### 5.3 Integration Tests

```rust
#[test]
fn test_foreach_mark_path_nodes_visited() {
    // MATCH p = (a)-[*]->(b) FOREACH (n IN nodes(p) | SET n.visited = true)
}

#[test]
fn test_foreach_propagate_property() {
    // MATCH (parent)-[:CHILD]->(child)
    // WITH parent, COLLECT(child) AS children
    // FOREACH (c IN children | SET c.parentName = parent.name)
}

#[test]
fn test_list_index_in_return() {
    // MATCH (p:Person) RETURN p.tags[0], p.scores[-1]
}

#[test]
fn test_list_slice_recent_items() {
    // MATCH (p:Person) RETURN p.history[-5..] AS recent
}

#[test]
fn test_pattern_comprehension_friends() {
    // MATCH (p:Person) RETURN p.name, [(p)-[:FRIEND]->(f) | f.name] AS friends
}

#[test]
fn test_pattern_comprehension_with_edge_properties() {
    // MATCH (p:Person) RETURN [(p)-[r:RATED]->(m:Movie) | {title: m.title, rating: r.stars}]
}

#[test]
fn test_combined_features() {
    // Complex query using multiple new features
    // MATCH (team:Team)
    // FOREACH (p IN [(team)<-[:PLAYS_FOR]-(player) | player] | SET p.active = true)
    // RETURN team.name, [(team)<-[:PLAYS_FOR]-(p) | p.name][0..5] AS topFive
}
```

### 5.4 Snapshot Tests

```rust
#[test] fn parse_list_index_snapshot() { }
#[test] fn parse_list_slice_snapshot() { }
#[test] fn parse_pattern_comprehension_snapshot() { }
#[test] fn parse_foreach_simple_snapshot() { }
#[test] fn parse_foreach_multiple_mutations_snapshot() { }
```

---

## 6. Error Handling

### 6.1 New Error Types

```rust
#[derive(Debug, Error)]
pub enum GqlError {
    // ... existing variants ...
    
    /// FOREACH requires a list expression
    #[error("FOREACH expression must be a list, got {0}")]
    ForeachNotList(String),
    
    /// FOREACH variable shadows outer scope
    #[error("FOREACH variable '{0}' shadows variable in outer scope")]
    ForeachVariableShadows(String),
    
    /// Index is not an integer
    #[error("list index must be an integer, got {0}")]
    IndexNotInteger(String),
    
    /// Cannot index into non-list
    #[error("cannot index into {0}, expected list")]
    IndexNotList(String),
    
    /// Slice bounds are not integers
    #[error("slice bound must be an integer, got {0}")]
    SliceBoundNotInteger(String),
    
    /// Pattern comprehension not correlated to outer query
    #[error("pattern comprehension must reference at least one outer variable")]
    PatternComprehensionNotCorrelated,
    
    /// Invalid pattern in comprehension
    #[error("invalid pattern in comprehension: {0}")]
    InvalidPatternComprehension(String),
}
```

---

## 7. Example Usage

### 7.1 FOREACH Examples

```rust
// Mark all nodes in a path as visited
let results = execute_mut(
    &mut graph,
    "MATCH p = (start:Person {name: 'Alice'})-[*1..5]->(end)
     FOREACH (n IN nodes(p) | SET n.visited = true)
     RETURN end.name"
)?;

// Propagate department info to employees
let results = execute_mut(
    &mut graph,
    "MATCH (dept:Department)<-[:WORKS_IN]-(emp:Employee)
     WITH dept, COLLECT(emp) AS employees
     FOREACH (e IN employees | 
         SET e.deptName = dept.name
         SET e.deptCode = dept.code
     )
     RETURN dept.name, SIZE(employees) AS employeeCount"
)?;

// Create audit trail
let results = execute_mut(
    &mut graph,
    "MATCH (order:Order)-[:CONTAINS]->(item:Item)
     WHERE order.status = 'shipped'
     WITH order, COLLECT(item) AS items
     FOREACH (i IN items |
         SET i.shippedAt = $timestamp
         SET i.trackingId = order.trackingId
     )
     RETURN order.id"
)?;
```

### 7.2 List Indexing and Slicing Examples

```rust
// Access first and last elements
let results = execute(
    &graph,
    "MATCH (p:Person)
     RETURN p.name,
            p.scores[0] AS firstScore,
            p.scores[-1] AS lastScore,
            p.tags[0] AS primaryTag"
)?;

// Get recent history items
let results = execute(
    &graph,
    "MATCH (user:User)
     RETURN user.name,
            user.loginHistory[-10..] AS recentLogins,
            user.purchases[-5..] AS recentPurchases"
)?;

// Pagination-like slicing
let results = execute(
    &graph,
    "MATCH (p:Product)
     WITH p ORDER BY p.rating DESC
     WITH COLLECT(p.name) AS sortedProducts
     RETURN sortedProducts[0..10] AS topTen,
            sortedProducts[10..20] AS nextTen"
)?;
```

### 7.3 Pattern Comprehension Examples

```rust
// Get friend names inline
let results = execute(
    &graph,
    "MATCH (p:Person)
     RETURN p.name, 
            [(p)-[:FRIEND]->(f) | f.name] AS friendNames,
            SIZE([(p)-[:FRIEND]->(f) | f]) AS friendCount"
)?;

// Get movie ratings
let results = execute(
    &graph,
    "MATCH (user:User)
     RETURN user.name,
            [(user)-[r:RATED]->(m:Movie) | {
                title: m.title,
                rating: r.stars,
                date: r.date
            }] AS ratings"
)?;

// Multi-hop pattern
let results = execute(
    &graph,
    "MATCH (p:Person)
     RETURN p.name,
            [(p)-[:FRIEND]->()-[:FRIEND]->(fof) WHERE fof <> p | fof.name] AS friendsOfFriends"
)?;

// Combined with slicing
let results = execute(
    &graph,
    "MATCH (author:Author)
     RETURN author.name,
            [(author)-[:WROTE]->(book:Book) | book.title][0..5] AS topFiveBooks"
)?;
```

---

## 8. Performance Considerations

### 8.1 FOREACH

- Executes mutations in a loop - O(n) mutations for n elements
- Each mutation may trigger index updates
- Consider batching for large lists
- Memory usage is constant (no intermediate collections)

### 8.2 List Slicing

- Slice creates a new list (copy) - O(slice_length)
- Chained slices create intermediate lists
- For large lists, consider using LIMIT/OFFSET instead of collecting and slicing

### 8.3 Pattern Comprehension

- Executes a pattern match for each outer row
- Can be expensive for complex patterns
- Results are materialized as a list
- Consider using CALL subquery for very complex nested queries

**Optimization Opportunities (Future):**
- Cache pattern match results for repeated comprehensions
- Lazy evaluation for pattern comprehensions
- Batch execution of FOREACH mutations

---

## 9. Compatibility Notes

### 9.1 Cypher/GQL Standard Compliance

| Feature | Cypher | ISO GQL | This Spec |
|---------|--------|---------|-----------|
| FOREACH | Yes | Yes | Yes |
| List indexing | Yes | Yes | Yes |
| Negative indexing | Yes | Yes | Yes |
| List slicing | Yes (`[a..b]`) | Yes | Yes |
| Pattern comprehension | Yes | Yes | Yes |

### 9.2 Differences from Cypher

- Slice syntax uses `[start..end]` not `[start..end]` (same as Cypher)
- Pattern comprehension supports same syntax as Cypher
- FOREACH supports same mutation types as Cypher

---

## 10. Future Extensions

After this spec is implemented, potential future work includes:

1. **FOREACH with nested FOREACH** - Allow nested iteration
2. **Pattern comprehension with WHERE** - Filter within pattern comprehension
3. **Pattern comprehension with ORDER BY/LIMIT** - Control match order and count
4. **List concatenation** - `list1 + list2` syntax
5. **List contains operator** - `element IN list` as operator
6. **Multi-dimensional indexing** - `matrix[i][j]` syntax
7. **Slice with step** - `list[start..end..step]` for stride access

---

## 11. References

- ISO/IEC 39075:2024 (GQL Standard) - List operations, FOREACH
- Neo4j Cypher Manual - List operators, FOREACH clause, pattern comprehensions
- openCypher Specification - Expression evaluation, list handling
