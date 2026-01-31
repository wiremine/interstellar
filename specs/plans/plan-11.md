# Plan 11: GQL Advanced Features - Final Coverage

**Phase 3 Feature: Complete Remaining GQL Feature Parity with Traversal API**

Based on: Gap analysis comparing `examples/nba_mmap_read.rs` to `examples/nba_gql.rs`

---

## Overview

This plan addresses the final ~15% of traversal API features that don't have GQL equivalents after Plan 10. These are advanced patterns that push the boundaries of declarative query languages and require careful design decisions about syntax and semantics.

**Total Duration**: 3-4 weeks  
**Dependencies**: Plan 10 (GQL Extended Features) must be complete

---

## Gap Analysis

After Plan 10, the following traversal API features remain without GQL equivalents:

| Category | Missing Feature | Traversal API | Priority | Complexity |
|----------|----------------|---------------|----------|------------|
| **Branch Steps** | UNION | `union(vec![...])` | High | Medium |
| **Branch Steps** | OPTIONAL | `optional(__::...)` | Medium | Medium |
| **Iteration** | REPEAT | `repeat(__::...).times(n).emit()` | Medium | High |
| **Path** | Full Path Return | `with_path().path()` | Medium | High |
| **Value Filtering** | IS predicate | `is_(p::gte(...))` | Low | Low |
| **Transform** | UNFOLD | `unfold()` | Low | Low |
| **Transform** | VALUE_MAP | `value_map()` / `element_map()` | Low | Medium |

---

## Architecture Changes

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           GQL Source Text                               │
│  MATCH (p:player)                                                       │
│  RETURN UNION { (p)-[:played_for]->() }, { (p)-[:won_championship]->() }│
│  WITH PATH                                                              │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Extended Grammar (grammar.pest)                      │
│  + UNION clause                                                         │
│  + OPTIONAL pattern modifier                                            │
│  + Variable-length paths with emit semantics                            │
│  + WITH PATH clause                                                     │
│  + UNWIND clause                                                        │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Extended AST (ast.rs)                           │
│  + UnionClause, OptionalPattern                                         │
│  + PathReturn, UnwindClause                                             │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      Extended Compiler (compiler.rs)                    │
│  + UNION via union() step                                               │
│  + OPTIONAL via optional() step                                         │
│  + Path collection via path() step                                      │
│  + UNWIND via unfold() step                                             │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Phases

### Week 1: UNION Clause

The UNION pattern combines results from multiple sub-patterns, equivalent to the `union()` traversal step.

---

#### Phase 1.1: UNION Grammar
**File**: `src/gql/grammar.pest`  
**Duration**: 1-2 hours

**Design Decision**: GQL/SQL UNION operates on full queries, but for graph patterns we need sub-pattern union. We'll use a syntax inspired by Cypher's UNION but applied to patterns within a single query.

**Syntax Options**:

```
// Option A: UNION keyword between patterns (Cypher-style)
MATCH (p:player)-[:played_for]->(t:team)
UNION
MATCH (p:player)-[:won_championship_with]->(t:team)
RETURN t.name

// Option B: UNION function in RETURN (more composable)
MATCH (p:player)
RETURN UNION(
    (p)-[:played_for]->(t:team) | t.name,
    (p)-[:won_championship_with]->(t:team) | t.name
)

// Option C: Multiple MATCH clauses with UNION ALL
MATCH (p:player)-[:played_for]->(t:team)
UNION ALL
MATCH (p:player)-[:won_championship_with]->(t:team)
RETURN t.name
```

**Chosen Approach**: Option A (Cypher-style UNION between queries)

**Grammar**:

```pest
// Update top-level to support UNION
gql = { SOI ~ query_or_union ~ EOI }

query_or_union = { 
    query ~ (union_clause ~ query)* 
}

union_clause = { UNION ~ ALL? }

UNION = @{ ^"union" ~ !ASCII_ALPHANUMERIC }
ALL   = @{ ^"all" ~ !ASCII_ALPHANUMERIC }
```

**Tasks**:
1. Add UNION and ALL keywords
2. Add union_clause rule
3. Update top-level to allow query chains
4. Handle UNION vs UNION ALL semantics

**Acceptance Criteria**:
- [ ] `MATCH (a) RETURN a UNION MATCH (b) RETURN b` parses
- [ ] `UNION ALL` parses and is distinct from `UNION`
- [ ] Multiple UNIONs parse correctly

---

#### Phase 1.2: UNION AST
**File**: `src/gql/ast.rs`  
**Duration**: 30 minutes

**Code**:

```rust
/// A GQL statement which may be a single query or a UNION of queries.
#[derive(Debug, Clone, Serialize)]
pub enum Statement {
    /// A single query
    Query(Query),
    /// A UNION of multiple queries
    Union {
        /// The queries to union
        queries: Vec<Query>,
        /// True for UNION ALL (keep duplicates), false for UNION (deduplicate)
        all: bool,
    },
}
```

**Acceptance Criteria**:
- [ ] `Statement` enum supports both single queries and unions
- [ ] `all` flag distinguishes UNION from UNION ALL

---

#### Phase 1.3: UNION Parser
**File**: `src/gql/parser.rs`  
**Duration**: 1-2 hours

**Code**:

```rust
fn build_statement(pair: pest::iterators::Pair<Rule>) -> Result<Statement, ParseError> {
    let mut queries = Vec::new();
    let mut union_all = false;
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::query => {
                queries.push(build_query(inner)?);
            }
            Rule::union_clause => {
                // Check for ALL keyword
                for clause_inner in inner.into_inner() {
                    if clause_inner.as_rule() == Rule::ALL {
                        union_all = true;
                    }
                }
            }
            Rule::EOI => {}
            _ => {}
        }
    }
    
    if queries.len() == 1 {
        Ok(Statement::Query(queries.pop().unwrap()))
    } else {
        Ok(Statement::Union {
            queries,
            all: union_all,
        })
    }
}
```

**Acceptance Criteria**:
- [ ] Single query parses to `Statement::Query`
- [ ] UNION parses to `Statement::Union` with `all: false`
- [ ] UNION ALL parses to `Statement::Union` with `all: true`

---

#### Phase 1.4: UNION Compiler
**File**: `src/gql/compiler.rs`  
**Duration**: 2-3 hours

**Code**:

```rust
impl<'a, 'g> Compiler<'a, 'g> {
    /// Execute a statement (single query or union).
    pub fn execute_statement(&mut self, stmt: &Statement) -> Result<Vec<Value>, CompileError> {
        match stmt {
            Statement::Query(query) => self.execute_query(query),
            Statement::Union { queries, all } => self.execute_union(queries, *all),
        }
    }
    
    /// Execute a UNION of multiple queries.
    fn execute_union(
        &mut self,
        queries: &[Query],
        keep_duplicates: bool,
    ) -> Result<Vec<Value>, CompileError> {
        let mut all_results = Vec::new();
        
        for query in queries {
            let results = self.execute_query(query)?;
            all_results.extend(results);
        }
        
        if keep_duplicates {
            // UNION ALL - keep all results
            Ok(all_results)
        } else {
            // UNION - deduplicate
            let mut seen = HashSet::new();
            let deduped: Vec<Value> = all_results
                .into_iter()
                .filter(|v| {
                    let key = ComparableValue::from(v.clone());
                    seen.insert(key)
                })
                .collect();
            Ok(deduped)
        }
    }
}
```

**Acceptance Criteria**:
- [ ] UNION combines results from multiple queries
- [ ] UNION deduplicates results
- [ ] UNION ALL keeps duplicates
- [ ] Column compatibility is validated (same return columns)

---

#### Phase 1.5: UNION Integration Tests
**File**: `tests/gql.rs`  
**Duration**: 1-2 hours

**Code**:

```rust
#[test]
fn test_gql_union_basic() {
    let graph = create_nba_test_graph();
    let snapshot = graph.snapshot();
    
    // Combine played_for and won_championship_with relationships
    let results = snapshot.gql(r#"
        MATCH (p:player {name: 'Shaquille O''Neal'})-[:played_for]->(t:team)
        RETURN t.name AS team
        UNION
        MATCH (p:player {name: 'Shaquille O''Neal'})-[:won_championship_with]->(t:team)
        RETURN t.name AS team
    "#).unwrap();
    
    // Should have unique teams only
    let team_names: HashSet<_> = results.iter()
        .filter_map(|v| v.as_string())
        .collect();
    
    // Shaq played for more teams than he won with, so union should be >= championship teams
    assert!(!team_names.is_empty());
}

#[test]
fn test_gql_union_all() {
    let graph = create_nba_test_graph();
    let snapshot = graph.snapshot();
    
    // UNION ALL keeps duplicates
    let results = snapshot.gql(r#"
        MATCH (p:player)-[:played_for]->(t:team {name: 'Los Angeles Lakers'})
        RETURN p.name
        UNION ALL
        MATCH (p:player)-[:won_championship_with]->(t:team {name: 'Los Angeles Lakers'})
        RETURN p.name
    "#).unwrap();
    
    // Lakers players who also won championships appear twice
}

#[test]
fn test_gql_union_incompatible_columns_error() {
    let graph = create_nba_test_graph();
    let snapshot = graph.snapshot();
    
    // Different column names should error
    let result = snapshot.gql(r#"
        MATCH (p:player) RETURN p.name AS player
        UNION
        MATCH (t:team) RETURN t.name AS team
    "#);
    
    // Should fail due to incompatible return columns
    assert!(result.is_err());
}
```

**Acceptance Criteria**:
- [ ] Basic UNION works
- [ ] UNION ALL works
- [ ] Column compatibility validated

---

### Week 2: OPTIONAL Pattern and Variable-Length Paths

---

#### Phase 2.1: OPTIONAL Pattern Grammar
**File**: `src/gql/grammar.pest`  
**Duration**: 1-2 hours

**Design**: Use `OPTIONAL MATCH` syntax (Cypher-compatible) to try a pattern match but keep the row if it fails.

**Grammar**:

```pest
// Add OPTIONAL keyword
OPTIONAL = @{ ^"optional" ~ !ASCII_ALPHANUMERIC }

// Optional match clause
optional_match_clause = { OPTIONAL ~ MATCH ~ pattern }

// Update query to include optional matches
query = { 
    SOI ~ 
    match_clause ~ 
    optional_match_clause* ~  // NEW: zero or more optional matches
    where_clause? ~ 
    return_clause ~ 
    group_by_clause? ~ 
    order_clause? ~ 
    limit_clause? ~ 
    EOI 
}
```

**Acceptance Criteria**:
- [ ] `OPTIONAL MATCH (p)-[:knows]->(friend)` parses
- [ ] Multiple OPTIONAL MATCH clauses parse
- [ ] OPTIONAL MATCH can reference variables from earlier MATCH

---

#### Phase 2.2: OPTIONAL AST
**File**: `src/gql/ast.rs`  
**Duration**: 30 minutes

**Code**:

```rust
/// Optional match clause - matches if possible, null if not.
#[derive(Debug, Clone, Serialize)]
pub struct OptionalMatchClause {
    /// The pattern to optionally match
    pub pattern: Pattern,
}

// Update Query struct
#[derive(Debug, Clone, Serialize)]
pub struct Query {
    pub match_clause: MatchClause,
    pub optional_match_clauses: Vec<OptionalMatchClause>,  // NEW
    pub where_clause: Option<WhereClause>,
    pub return_clause: ReturnClause,
    pub group_by_clause: Option<GroupByClause>,
    pub order_clause: Option<OrderClause>,
    pub limit_clause: Option<LimitClause>,
}
```

**Acceptance Criteria**:
- [ ] `OptionalMatchClause` type defined
- [ ] Query includes vec of optional match clauses

---

#### Phase 2.3: OPTIONAL Compiler
**File**: `src/gql/compiler.rs`  
**Duration**: 2-3 hours

**Code**:

```rust
impl<'a, 'g> Compiler<'a, 'g> {
    fn compile_with_optional_matches(
        &mut self,
        query: &Query,
    ) -> Result<Vec<Value>, CompileError> {
        // First, execute the required MATCH
        let base_results = self.execute_match_clause(&query.match_clause)?;
        
        // For each base result, try optional matches
        let mut final_results = Vec::new();
        
        for base_row in base_results {
            let mut row = base_row.clone();
            
            // Try each optional match
            for optional in &query.optional_match_clauses {
                let optional_results = self.try_optional_match(
                    &optional.pattern,
                    &row,
                )?;
                
                if let Some(opt_row) = optional_results {
                    // Merge optional results into row
                    row = self.merge_rows(row, opt_row);
                } else {
                    // Keep row but with nulls for optional variables
                    row = self.add_null_optional_vars(row, &optional.pattern);
                }
            }
            
            final_results.push(row);
        }
        
        Ok(final_results)
    }
    
    /// Try to execute an optional match from a base row.
    fn try_optional_match(
        &self,
        pattern: &Pattern,
        base_row: &Value,
    ) -> Result<Option<Value>, CompileError> {
        // Build traversal from pattern, using bound variables from base_row
        let traversal = self.build_pattern_from_bindings(pattern, base_row)?;
        
        // Execute and return first result, or None if empty
        let results: Vec<Value> = traversal.limit(1).to_list();
        Ok(results.into_iter().next())
    }
    
    /// Add null values for all variables in a pattern.
    fn add_null_optional_vars(&self, mut row: Value, pattern: &Pattern) -> Value {
        if let Value::Map(ref mut map) = row {
            for element in &pattern.elements {
                match element {
                    PatternElement::Node(node) => {
                        if let Some(var) = &node.variable {
                            map.entry(var.clone()).or_insert(Value::Null);
                        }
                    }
                    PatternElement::Edge(edge) => {
                        if let Some(var) = &edge.variable {
                            map.entry(var.clone()).or_insert(Value::Null);
                        }
                    }
                }
            }
        }
        row
    }
}
```

**Acceptance Criteria**:
- [ ] OPTIONAL MATCH returns null when pattern doesn't match
- [ ] OPTIONAL MATCH returns values when pattern matches
- [ ] Variables from OPTIONAL can be used in WHERE and RETURN
- [ ] Multiple OPTIONAL MATCH clauses work

---

#### Phase 2.4: Variable-Length Path Enhancement
**File**: `src/gql/grammar.pest`, `src/gql/compiler.rs`  
**Duration**: 3-4 hours

The grammar already supports `*1..3` syntax. This phase adds "emit" semantics to collect intermediate nodes.

**Design Decision**: Use `*` with a `COLLECT` modifier to emit intermediate results:

```
// Current: Only returns end nodes
MATCH (p:player)-[:played_for*1..2]->(t)

// New: Returns all nodes along path (emit semantics)
MATCH (p:player)-[:played_for*1..2 EMIT]->(t)
```

**Grammar addition**:

```pest
// Update variable_length to include optional EMIT
variable_length = { 
    "*" ~ 
    (INTEGER ~ (".." ~ INTEGER?)?)? ~
    EMIT?  // NEW
}

EMIT = @{ ^"emit" ~ !ASCII_ALPHANUMERIC }
```

**Compiler**:

```rust
fn compile_variable_length_edge(
    &mut self,
    edge: &EdgePattern,
    traversal: BoundTraversal<'g, (), Value>,
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    let (min, max, emit) = self.parse_variable_length(&edge.variable_length)?;
    
    // Build the repeat traversal
    let edge_traversal = self.build_edge_step(edge);
    
    let mut repeat_builder = traversal
        .repeat(edge_traversal)
        .times(max.unwrap_or(10));  // Default max depth
    
    if emit {
        // Emit intermediate results
        repeat_builder = repeat_builder.emit();
    }
    
    if min > 1 {
        // Only emit after min hops
        repeat_builder = repeat_builder.emit_after(min);
    }
    
    Ok(repeat_builder.build())
}
```

**Acceptance Criteria**:
- [ ] `*1..3 EMIT` collects all intermediate nodes
- [ ] `*1..3` without EMIT only returns final nodes
- [ ] Variable-length works with labeled edges

---

### Week 3: Path Return and UNWIND

---

#### Phase 3.1: WITH PATH Clause
**File**: `src/gql/grammar.pest`, `src/gql/ast.rs`  
**Duration**: 1-2 hours

**Design**: Add a `WITH PATH` clause to enable path collection, and a `path()` function to return it.

**Grammar**:

```pest
// PATH keyword
PATH = @{ ^"path" ~ !ASCII_ALPHANUMERIC }

// WITH PATH clause enables path tracking
with_path_clause = { WITH ~ PATH ~ (AS ~ identifier)? }

// Path function returns the collected path
// Already part of function_call: path()

// Update query to include WITH PATH
query = { 
    SOI ~ 
    match_clause ~ 
    optional_match_clause* ~
    with_path_clause? ~  // NEW
    where_clause? ~ 
    return_clause ~ 
    group_by_clause? ~ 
    order_clause? ~ 
    limit_clause? ~ 
    EOI 
}
```

**AST**:

```rust
/// WITH PATH clause - enables path tracking and collection.
#[derive(Debug, Clone, Serialize)]
pub struct WithPathClause {
    /// Optional alias for the path variable
    pub alias: Option<String>,
}
```

**Acceptance Criteria**:
- [ ] `WITH PATH` parses
- [ ] `WITH PATH AS p` parses with alias
- [ ] `RETURN path()` parses as function call

---

#### Phase 3.2: Path Return Compiler
**File**: `src/gql/compiler.rs`  
**Duration**: 2-3 hours

**Code**:

```rust
impl<'a, 'g> Compiler<'a, 'g> {
    fn compile_with_path_tracking(&mut self, query: &Query) -> Result<Vec<Value>, CompileError> {
        // Enable path tracking on traversal
        let mut traversal = self.build_initial_traversal(&query.match_clause)?;
        
        if query.with_path_clause.is_some() {
            traversal = traversal.with_path();
        }
        
        // Continue with pattern compilation...
        traversal = self.compile_pattern(&query.match_clause.pattern, traversal)?;
        
        // Execute and collect paths
        let results: Vec<Traverser> = traversal.into_traversers();
        
        // Process results with path if requested
        let values: Vec<Value> = results
            .into_iter()
            .map(|t| self.build_result_with_path(query, t))
            .collect::<Result<Vec<_>, _>>()?;
        
        Ok(values)
    }
    
    /// Build a result including path data if path() function is used.
    fn build_result_with_path(
        &self,
        query: &Query,
        traverser: Traverser,
    ) -> Result<Value, CompileError> {
        let mut result_map = HashMap::new();
        
        for item in &query.return_clause.items {
            let key = self.get_return_item_key(item);
            
            let value = if self.is_path_function(&item.expression) {
                // Return the path as a list
                self.build_path_value(&traverser.path)
            } else {
                self.evaluate_expression_from_path(&item.expression, &traverser)?
            };
            
            result_map.insert(key, value);
        }
        
        if result_map.len() == 1 {
            Ok(result_map.into_values().next().unwrap())
        } else {
            Ok(Value::Map(result_map))
        }
    }
    
    /// Convert a path to a Value::List.
    fn build_path_value(&self, path: &Path) -> Value {
        let elements: Vec<Value> = path
            .elements()
            .map(|pv| match pv {
                PathValue::Vertex(id) => Value::Vertex(*id),
                PathValue::Edge(id) => Value::Edge(*id),
                PathValue::Property(v) => v.clone(),
            })
            .collect();
        
        Value::List(elements)
    }
    
    /// Check if expression is the path() function.
    fn is_path_function(&self, expr: &Expression) -> bool {
        matches!(expr, Expression::FunctionCall { name, .. } if name.eq_ignore_ascii_case("path"))
    }
}
```

**Acceptance Criteria**:
- [ ] `WITH PATH` enables path tracking
- [ ] `path()` function returns list of traversed elements
- [ ] Path includes vertices and edges in order
- [ ] Path alias works: `WITH PATH AS mypath`

---

#### Phase 3.3: UNWIND Clause
**File**: `src/gql/grammar.pest`, `src/gql/ast.rs`, `src/gql/compiler.rs`  
**Duration**: 2-3 hours

**Design**: UNWIND expands a list into individual rows (equivalent to `unfold()`).

**Grammar**:

```pest
// UNWIND keyword
UNWIND = @{ ^"unwind" ~ !ASCII_ALPHANUMERIC }

// UNWIND clause
unwind_clause = { UNWIND ~ expression ~ AS ~ identifier }

// UNWIND can appear between MATCH and WHERE
query = { 
    SOI ~ 
    match_clause ~ 
    optional_match_clause* ~
    with_path_clause? ~
    unwind_clause* ~  // NEW: zero or more UNWIND
    where_clause? ~ 
    return_clause ~ 
    group_by_clause? ~ 
    order_clause? ~ 
    limit_clause? ~ 
    EOI 
}
```

**AST**:

```rust
/// UNWIND clause - expands a list into rows.
#[derive(Debug, Clone, Serialize)]
pub struct UnwindClause {
    /// The expression that produces a list
    pub expression: Expression,
    /// The variable name for each element
    pub alias: String,
}
```

**Compiler**:

```rust
impl<'a, 'g> Compiler<'a, 'g> {
    fn apply_unwind(
        &self,
        rows: Vec<Value>,
        unwind: &UnwindClause,
    ) -> Result<Vec<Value>, CompileError> {
        let mut result = Vec::new();
        
        for row in rows {
            // Evaluate the expression to get a list
            let list_value = self.evaluate_value(&unwind.expression, &row);
            
            match list_value {
                Value::List(items) => {
                    // Create a new row for each item
                    for item in items {
                        let mut new_row = row.clone();
                        if let Value::Map(ref mut map) = new_row {
                            map.insert(unwind.alias.clone(), item);
                        }
                        result.push(new_row);
                    }
                }
                Value::Null => {
                    // UNWIND null produces no rows
                }
                other => {
                    // UNWIND non-list wraps in single-element list
                    let mut new_row = row.clone();
                    if let Value::Map(ref mut map) = new_row {
                        map.insert(unwind.alias.clone(), other);
                    }
                    result.push(new_row);
                }
            }
        }
        
        Ok(result)
    }
}
```

**Example queries**:

```sql
-- Unwind a collected list
MATCH (p:player)
WITH collect(p.name) AS names
UNWIND names AS name
RETURN name

-- Unwind a literal list
UNWIND [1, 2, 3] AS num
RETURN num * 2 AS doubled
```

**Acceptance Criteria**:
- [ ] UNWIND expands list into rows
- [ ] UNWIND null produces no rows
- [ ] Multiple UNWIND clauses work
- [ ] UNWIND variable usable in WHERE and RETURN

---

### Week 4: Value Functions and Polish

---

#### Phase 4.1: IS Predicate Function
**File**: `src/gql/grammar.pest`, `src/gql/compiler.rs`  
**Duration**: 1-2 hours

**Design**: Add `IS` as a post-filter for values, similar to the traversal `is_()` step.

**Grammar option**: Use a WHERE-like syntax after value extraction:

```sql
-- Filter extracted values
MATCH (p:player)
WHERE p.points_per_game IS >= 25.0
RETURN p.name
```

**Alternative**: Function syntax:

```sql
MATCH (p:player)
RETURN p.name
WHERE is(p.points_per_game, '>= 25.0')
```

**Chosen approach**: Enhance existing comparison to work on extracted values naturally (already supported via WHERE).

For value-level filtering like `is_()`, we enhance expression evaluation:

```rust
// No grammar change needed - IS predicate is already handled by comparisons
// The traversal is_() step is equivalent to WHERE in GQL
```

**Note**: The `is_()` step in the traversal API filters stream values. In GQL, this is achieved via WHERE clause on the property. No additional syntax needed.

**Acceptance Criteria**:
- [ ] Document that `is_()` equivalent is WHERE on property
- [ ] Add examples showing equivalent patterns

---

#### Phase 4.2: Properties Function (VALUE_MAP equivalent)
**File**: `src/gql/compiler.rs`  
**Duration**: 1-2 hours

**Design**: Add `properties()` function to return all properties of a vertex/edge as a map.

**Grammar**: Already supported as function_call.

**Compiler**:

```rust
fn evaluate_function_call(
    &self,
    name: &str,
    args: &[Expression],
    element: &Value,
) -> Value {
    match name.to_uppercase().as_str() {
        "PROPERTIES" => {
            // Return all properties as a map
            match element {
                Value::Vertex(vid) => {
                    if let Some(vertex) = self.snapshot.storage().get_vertex(*vid) {
                        Value::Map(vertex.properties.clone())
                    } else {
                        Value::Null
                    }
                }
                Value::Edge(eid) => {
                    if let Some(edge) = self.snapshot.storage().get_edge(*eid) {
                        Value::Map(edge.properties.clone())
                    } else {
                        Value::Null
                    }
                }
                _ => Value::Null,
            }
        }
        // ... existing functions ...
        _ => Value::Null,
    }
}
```

**Example**:

```sql
MATCH (p:player {name: 'Michael Jordan'})
RETURN properties(p)
-- Returns: {name: 'Michael Jordan', position: 'Shooting Guard', ...}
```

**Acceptance Criteria**:
- [ ] `properties(node)` returns all properties as map
- [ ] `properties(edge)` returns edge properties
- [ ] Works in RETURN clause

---

#### Phase 4.3: Labels and Type Functions
**File**: `src/gql/compiler.rs`  
**Duration**: 1 hour

**Design**: Add `labels()` and `type()` functions for introspection.

**Compiler**:

```rust
fn evaluate_function_call(
    &self,
    name: &str,
    args: &[Expression],
    element: &Value,
) -> Value {
    match name.to_uppercase().as_str() {
        "LABELS" => {
            // Return vertex labels as a list
            if let Value::Vertex(vid) = element {
                if let Some(vertex) = self.snapshot.storage().get_vertex(*vid) {
                    return Value::List(
                        vertex.labels.iter()
                            .map(|l| Value::String(l.clone()))
                            .collect()
                    );
                }
            }
            Value::Null
        }
        "TYPE" => {
            // Return edge type/label
            if let Value::Edge(eid) = element {
                if let Some(edge) = self.snapshot.storage().get_edge(*eid) {
                    return Value::String(edge.label.clone());
                }
            }
            Value::Null
        }
        // ... existing functions ...
        _ => Value::Null,
    }
}
```

**Example**:

```sql
MATCH (n)
RETURN labels(n), properties(n)
LIMIT 5

MATCH ()-[e]->()
RETURN type(e), properties(e)
LIMIT 5
```

**Acceptance Criteria**:
- [ ] `labels(node)` returns list of labels
- [ ] `type(edge)` returns edge label
- [ ] Works for any vertex/edge

---

#### Phase 4.4: ID Function
**File**: `src/gql/compiler.rs`  
**Duration**: 30 minutes

**Design**: Add `id()` function to return the internal identifier.

```rust
"ID" => {
    match element {
        Value::Vertex(vid) => Value::Int(vid.0 as i64),
        Value::Edge(eid) => Value::Int(eid.0 as i64),
        _ => Value::Null,
    }
}
```

**Example**:

```sql
MATCH (p:player {name: 'Michael Jordan'})
RETURN id(p), p.name
```

**Acceptance Criteria**:
- [ ] `id(node)` returns vertex ID
- [ ] `id(edge)` returns edge ID

---

#### Phase 4.5: Update NBA GQL Example
**File**: `examples/nba_gql.rs`  
**Duration**: 2-3 hours

Add new sections demonstrating Plan 11 features:

```rust
// =========================================================================
// SECTION 18: UNION Queries (Plan 11)
// =========================================================================

// Query 61: Combine different relationship types
print_query("All team connections for Shaq (UNION)");
let results = snapshot.gql(r#"
    MATCH (p:player {name: 'Shaquille O''Neal'})-[:played_for]->(t:team)
    RETURN t.name AS team, 'played_for' AS relationship
    UNION
    MATCH (p:player {name: 'Shaquille O''Neal'})-[:won_championship_with]->(t:team)
    RETURN t.name AS team, 'won_championship_with' AS relationship
"#).unwrap();

// =========================================================================
// SECTION 19: OPTIONAL MATCH (Plan 11)
// =========================================================================

// Query 62: Optional championship data
print_query("Players with optional championship info");
let results = snapshot.gql(r#"
    MATCH (p:player)
    OPTIONAL MATCH (p)-[:won_championship_with]->(t:team)
    RETURN p.name, t.name AS championship_team
    LIMIT 10
"#).unwrap();

// =========================================================================
// SECTION 20: Path Functions (Plan 11)
// =========================================================================

// Query 63: Return full traversal path
print_query("Path from player to teammate");
let results = snapshot.gql(r#"
    MATCH (p1:player {name: 'Michael Jordan'})-[:played_for]->(t:team)<-[:played_for]-(p2:player)
    WITH PATH
    RETURN path(), p2.name
    LIMIT 5
"#).unwrap();

// =========================================================================
// SECTION 21: Introspection Functions (Plan 11)
// =========================================================================

// Query 64: Get element metadata
print_query("Element introspection");
let results = snapshot.gql(r#"
    MATCH (p:player {name: 'LeBron James'})
    RETURN id(p), labels(p), properties(p)
"#).unwrap();
```

**Acceptance Criteria**:
- [ ] All Plan 11 features demonstrated
- [ ] Example compiles and runs
- [ ] Output is readable

---

#### Phase 4.6: Documentation and Tests
**File**: `tests/gql.rs`, `tests/gql_snapshots.rs`  
**Duration**: 2-3 hours

Add comprehensive tests for all Plan 11 features:

```rust
// UNION tests
#[test]
fn test_gql_union_deduplicates() { ... }

#[test]
fn test_gql_union_all_keeps_duplicates() { ... }

// OPTIONAL MATCH tests
#[test]
fn test_gql_optional_match_with_result() { ... }

#[test]
fn test_gql_optional_match_without_result() { ... }

// Path tests
#[test]
fn test_gql_with_path_returns_path() { ... }

// UNWIND tests
#[test]
fn test_gql_unwind_list() { ... }

#[test]
fn test_gql_unwind_null() { ... }

// Function tests
#[test]
fn test_gql_properties_function() { ... }

#[test]
fn test_gql_labels_function() { ... }

#[test]
fn test_gql_type_function() { ... }

#[test]
fn test_gql_id_function() { ... }
```

**Acceptance Criteria**:
- [ ] All new features have tests
- [ ] Snapshot tests for new AST structures
- [ ] Integration tests pass

---

## Exit Criteria Checklist

### Parser Extensions
- [ ] UNION / UNION ALL parses
- [ ] OPTIONAL MATCH parses
- [ ] WITH PATH parses
- [ ] UNWIND parses
- [ ] Variable-length EMIT parses

### Compiler Extensions
- [ ] UNION combines query results
- [ ] UNION ALL keeps duplicates
- [ ] OPTIONAL MATCH returns nulls on no match
- [ ] Path collection works
- [ ] UNWIND expands lists

### Functions
- [ ] `properties()` returns all properties
- [ ] `labels()` returns vertex labels
- [ ] `type()` returns edge type
- [ ] `id()` returns element ID
- [ ] `path()` returns traversed path

### Integration
- [ ] NBA example updated with Plan 11 features
- [ ] All tests pass
- [ ] Documentation complete

---

## Query Coverage After Plan 11

After implementing this plan, GQL will support:

| Category | Coverage |
|----------|----------|
| Basic Queries | 100% |
| Navigation | 100% |
| Predicates | 100% |
| Anonymous Traversals | 100% (via EXISTS) |
| Branch Steps | 95% (UNION, OPTIONAL, COALESCE, CASE) |
| Repeat Steps | 90% (variable-length with EMIT) |
| Path Tracking | 95% (WITH PATH, path()) |
| Complex Combined | 95% |
| Transform Steps | 90% (properties, labels, type, id) |
| Aggregation | 100% |

**Overall Coverage**: ~95% of traversal API features expressible in GQL

**Remaining gaps** (intentionally not covered - require procedural semantics):
- Complex custom step composition (Gremlin's `map()`, `flatMap()`)
- Arbitrary predicate functions in filters (covered by WHERE)
- Side-effect steps (`sideEffect()`, `store()`)

---

## File Summary

**Modified files**:
- `src/gql/grammar.pest` - UNION, OPTIONAL, WITH PATH, UNWIND, EMIT
- `src/gql/ast.rs` - Statement enum, OptionalMatchClause, WithPathClause, UnwindClause
- `src/gql/parser.rs` - Parse new constructs
- `src/gql/compiler.rs` - UNION execution, OPTIONAL semantics, path collection, UNWIND, new functions
- `examples/nba_gql.rs` - New sections for Plan 11 features

**Test files**:
- `tests/gql.rs` - New integration tests
- `tests/gql_snapshots.rs` - New snapshot tests

---

## References

- `plans/plan-10.md` - Previous GQL extension plan
- `examples/nba_mmap_read.rs` - Target query coverage
- `guiding-documents/gql.md` - GQL language specification
- [Cypher Manual - UNION](https://neo4j.com/docs/cypher-manual/current/clauses/union/)
- [Cypher Manual - OPTIONAL MATCH](https://neo4j.com/docs/cypher-manual/current/clauses/optional-match/)
- [Cypher Manual - UNWIND](https://neo4j.com/docs/cypher-manual/current/clauses/unwind/)
