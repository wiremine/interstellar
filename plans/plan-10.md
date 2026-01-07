# Plan 10: GQL Extended Features

**Phase 3 Feature: Complete GQL Feature Parity with Traversal API**

Based on: Analysis of `examples/nba_mmap_read.rs` coverage gaps

---

## Overview

This plan extends the GQL implementation from Plan 09 to support advanced query patterns that are currently only available through the Rust traversal API. The goal is to enable ~90% of the NBA example queries to be expressible in GQL.

**Total Duration**: 3-4 weeks  
**Dependencies**: Plan 09 (GQL Parser and Runtime) must be complete

---

## Gap Analysis

From analyzing `examples/nba_mmap_read.rs`, the following features are missing from GQL:

| Category | Missing Feature | Priority | Complexity |
|----------|----------------|----------|------------|
| **Subqueries** | EXISTS subquery in WHERE | High | Medium |
| **Subqueries** | NOT EXISTS pattern | High | Medium |
| **Aggregation** | GROUP BY clause | High | High |
| **Projection** | Multiple variable binding | Medium | Medium |
| **Navigation** | Edge property access | Medium | Medium |
| **Functions** | COALESCE() | Medium | Low |
| **Functions** | CASE expressions | Low | Medium |
| **Path** | Path return (collect path) | Low | High |

---

## Architecture Changes

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           GQL Source Text                               │
│  MATCH (p:player) WHERE EXISTS { (p)-[:won_championship_with]->() }    │
│  RETURN p.name, count(*) GROUP BY p.position                           │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Extended Grammar (grammar.pest)                      │
│  + EXISTS { pattern }                                                   │
│  + GROUP BY clause                                                      │
│  + COALESCE(), CASE expressions                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Extended AST (ast.rs)                           │
│  + ExistsExpression, GroupByClause                                      │
│  + CoalesceExpression, CaseExpression                                   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      Extended Compiler (compiler.rs)                    │
│  + Subquery evaluation via where_() / not()                             │
│  + GROUP BY via group() step                                            │
│  + Multi-variable path tracking via as_() / select()                    │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Phases

### Week 1: EXISTS Subqueries

The most impactful missing feature. Enables patterns like:
- "Find players who have won championships"
- "Find players who have NOT won championships"

---

#### Phase 1.1: EXISTS Grammar
**File**: `src/gql/grammar.pest`  
**Duration**: 1 hour

**Tasks**:
1. Add EXISTS keyword
2. Add EXISTS { pattern } expression syntax
3. Add NOT EXISTS { pattern } syntax

**Grammar additions**:

```pest
// Add to keywords
EXISTS   = @{ ^"exists" ~ !ASCII_ALPHANUMERIC }

// Add to primary expression (inside comparison or as standalone)
primary = { 
    exists_expr       // NEW
    | literal
    | function_call
    | property_access
    | variable
    | paren_expr
    | list_expr
}

// EXISTS expression with embedded pattern
exists_expr = { NOT? ~ EXISTS ~ "{" ~ pattern ~ "}" }
```

**Acceptance Criteria**:
- [ ] `WHERE EXISTS { (p)-[:KNOWS]->() }` parses
- [ ] `WHERE NOT EXISTS { (p)-[:won_championship_with]->() }` parses
- [ ] `EXISTS` without braces fails with clear error

---

#### Phase 1.2: EXISTS AST Types
**File**: `src/gql/ast.rs`  
**Duration**: 30 minutes

**Tasks**:
1. Add `ExistsExpression` variant to `Expression` enum
2. Include pattern and negation flag

**Code**:

```rust
/// Expression types - add new variant
#[derive(Debug, Clone, Serialize)]
pub enum Expression {
    // ... existing variants ...
    
    /// EXISTS subquery: EXISTS { (a)-[:KNOWS]->(b) }
    /// 
    /// Evaluates to true if the embedded pattern matches at least one path
    /// starting from the current element. The pattern can reference variables
    /// bound in the outer MATCH clause.
    Exists {
        /// The pattern to check for existence
        pattern: Pattern,
        /// True for NOT EXISTS, false for EXISTS
        negated: bool,
    },
}
```

**Acceptance Criteria**:
- [ ] `Expression::Exists` variant compiles
- [ ] Pattern can be stored in the expression

---

#### Phase 1.3: EXISTS Parser
**File**: `src/gql/parser.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Parse EXISTS expression into AST
2. Handle NOT EXISTS variant
3. Reuse existing pattern parsing

**Code**:

```rust
fn build_exists_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let mut negated = false;
    let mut pattern = None;
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::NOT => negated = true,
            Rule::pattern => pattern = Some(build_pattern(inner)?),
            _ => {}
        }
    }
    
    Ok(Expression::Exists {
        pattern: pattern.ok_or(ParseError::MissingClause("pattern in EXISTS"))?,
        negated,
    })
}
```

**Acceptance Criteria**:
- [ ] EXISTS expressions parse to correct AST
- [ ] NOT flag is captured correctly
- [ ] Pattern elements are parsed

---

#### Phase 1.4: EXISTS Compiler
**File**: `src/gql/compiler.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Compile EXISTS to `where_()` step with sub-traversal
2. Compile NOT EXISTS to `not()` step
3. Handle variable references from outer scope

**Code**:

```rust
impl<'a, 'g> Compiler<'a, 'g> {
    /// Evaluate an EXISTS expression against an element.
    /// 
    /// EXISTS { (p)-[:KNOWS]->(friend) } checks if there's at least one
    /// path matching the pattern starting from the current element.
    fn evaluate_exists(&self, pattern: &Pattern, negated: bool, element: &Value) -> bool {
        // Build a sub-traversal from the pattern
        let sub_traversal = self.build_pattern_traversal(pattern, element);
        
        // Check if any results exist
        let has_results = sub_traversal.map(|t| t.count() > 0).unwrap_or(false);
        
        if negated {
            !has_results
        } else {
            has_results
        }
    }
    
    /// Build a traversal from a pattern, starting from the given element.
    fn build_pattern_traversal(
        &self,
        pattern: &Pattern,
        start_element: &Value,
    ) -> Option<impl Iterator<Item = Traverser>> {
        // Get vertex ID from element
        let vid = start_element.as_vertex_id()?;
        
        // Start traversal from this vertex
        let g = self.snapshot.traversal();
        let mut traversal = g.v_ids([vid]);
        
        // Apply pattern elements (skip first node, apply edges and subsequent nodes)
        for (i, element) in pattern.elements.iter().enumerate() {
            if i == 0 {
                // First element is the starting node - apply filters only
                if let PatternElement::Node(node) = element {
                    if !node.labels.is_empty() {
                        let labels: Vec<&str> = node.labels.iter().map(|s| s.as_str()).collect();
                        traversal = traversal.has_label_any(labels);
                    }
                    for (key, value) in &node.properties {
                        let val: Value = value.clone().into();
                        traversal = traversal.has_value(key.as_str(), val);
                    }
                }
                continue;
            }
            
            match element {
                PatternElement::Edge(edge) => {
                    traversal = self.apply_edge_to_bound_traversal(edge, traversal);
                }
                PatternElement::Node(node) => {
                    traversal = self.apply_node_filters_to_bound_traversal(node, traversal);
                }
            }
        }
        
        Some(traversal.into_iter())
    }
}
```

**Acceptance Criteria**:
- [ ] `WHERE EXISTS { (p)-[:won_championship_with]->() }` finds championship winners
- [ ] `WHERE NOT EXISTS { ... }` finds non-championship winners
- [ ] Nested EXISTS works correctly
- [ ] Performance is reasonable (no full graph scan per element)

---

#### Phase 1.5: EXISTS Integration Tests
**File**: `tests/gql.rs`  
**Duration**: 1-2 hours

**Code**:

```rust
#[test]
fn test_gql_exists_basic() {
    let graph = create_nba_test_graph();
    let snapshot = graph.snapshot();
    
    // Find players who have won championships
    let results = snapshot.gql(r#"
        MATCH (p:player)
        WHERE EXISTS { (p)-[:won_championship_with]->() }
        RETURN p.name
    "#).unwrap();
    
    // Should find MJ, Kobe, etc.
    assert!(!results.is_empty());
    assert!(results.iter().any(|v| v == &Value::String("Michael Jordan".to_string())));
}

#[test]
fn test_gql_not_exists() {
    let graph = create_nba_test_graph();
    let snapshot = graph.snapshot();
    
    // Find players who have NOT won championships
    let results = snapshot.gql(r#"
        MATCH (p:player)
        WHERE NOT EXISTS { (p)-[:won_championship_with]->() }
        RETURN p.name
    "#).unwrap();
    
    // Should find Barkley, etc.
    assert!(results.iter().any(|v| v == &Value::String("Charles Barkley".to_string())));
}

#[test]
fn test_gql_exists_with_filters() {
    let graph = create_nba_test_graph();
    let snapshot = graph.snapshot();
    
    // Find players who played for teams with 5+ championships
    let results = snapshot.gql(r#"
        MATCH (p:player)
        WHERE EXISTS { (p)-[:played_for]->(t:team {championship_count: 5}) }
        RETURN p.name
    "#).unwrap();
    
    // Note: This tests property filter inside EXISTS pattern
}

#[test]
fn test_gql_exists_combined_with_and() {
    let graph = create_nba_test_graph();
    let snapshot = graph.snapshot();
    
    // Find MVP players who also won championships
    let results = snapshot.gql(r#"
        MATCH (p:player)
        WHERE p.mvp_count >= 1 AND EXISTS { (p)-[:won_championship_with]->() }
        RETURN p.name
    "#).unwrap();
}
```

**Acceptance Criteria**:
- [ ] All EXISTS tests pass
- [ ] NOT EXISTS tests pass
- [ ] Combined conditions work

---

### Week 2: GROUP BY Clause

Enables grouping and aggregation patterns like:
- "Count players by position"
- "Average PPG by team"

---

#### Phase 2.1: GROUP BY Grammar
**File**: `src/gql/grammar.pest`  
**Duration**: 1 hour

**Tasks**:
1. Add GROUP BY keywords
2. Add group_by_clause rule
3. Update query structure

**Grammar**:

```pest
// Update query to include GROUP BY between WHERE and RETURN
query = { 
    SOI ~ 
    match_clause ~ 
    where_clause? ~ 
    return_clause ~ 
    group_by_clause? ~   // NEW - after RETURN for GQL-style
    order_clause? ~ 
    limit_clause? ~ 
    EOI 
}

// Alternative: Cypher-style (GROUP BY before RETURN)
// We'll use GQL/SQL style where GROUP BY comes after RETURN

// GROUP BY clause
group_by_clause = { GROUP ~ BY ~ expression ~ ("," ~ expression)* }

// Add GROUP keyword
GROUP = @{ ^"group" ~ !ASCII_ALPHANUMERIC }
```

**Note**: GQL/ISO standard places grouping specification in RETURN clause semantics, but for familiarity we'll support explicit `GROUP BY`.

**Acceptance Criteria**:
- [ ] `RETURN p.position, count(*) GROUP BY p.position` parses
- [ ] Multiple GROUP BY expressions parse
- [ ] GROUP BY with aggregates in RETURN parses

---

#### Phase 2.2: GROUP BY AST
**File**: `src/gql/ast.rs`  
**Duration**: 30 minutes

**Code**:

```rust
/// GROUP BY clause for aggregation grouping.
///
/// Specifies which expressions to group by when using aggregate functions.
/// Non-aggregated expressions in RETURN should appear in GROUP BY.
///
/// # Example
///
/// ```text
/// MATCH (p:player) 
/// RETURN p.position, count(*), avg(p.points_per_game)
/// GROUP BY p.position
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct GroupByClause {
    /// Expressions to group by
    pub expressions: Vec<Expression>,
}

// Update Query struct
#[derive(Debug, Clone, Serialize)]
pub struct Query {
    pub match_clause: MatchClause,
    pub where_clause: Option<WhereClause>,
    pub return_clause: ReturnClause,
    pub group_by_clause: Option<GroupByClause>,  // NEW
    pub order_clause: Option<OrderClause>,
    pub limit_clause: Option<LimitClause>,
}
```

**Acceptance Criteria**:
- [ ] `GroupByClause` type defined
- [ ] Query includes optional group_by_clause

---

#### Phase 2.3: GROUP BY Parser
**File**: `src/gql/parser.rs`  
**Duration**: 1 hour

**Code**:

```rust
fn build_group_by_clause(pair: pest::iterators::Pair<Rule>) -> Result<GroupByClause, ParseError> {
    let mut expressions = Vec::new();
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::expression => {
                expressions.push(build_expression(inner)?);
            }
            _ => {}
        }
    }
    
    if expressions.is_empty() {
        return Err(ParseError::MissingClause("GROUP BY expression"));
    }
    
    Ok(GroupByClause { expressions })
}

// Update build_query to parse GROUP BY
fn build_query(pair: pest::iterators::Pair<Rule>) -> Result<Query, ParseError> {
    let mut match_clause = None;
    let mut where_clause = None;
    let mut return_clause = None;
    let mut group_by_clause = None;  // NEW
    let mut order_clause = None;
    let mut limit_clause = None;
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::match_clause => match_clause = Some(build_match_clause(inner)?),
            Rule::where_clause => where_clause = Some(build_where_clause(inner)?),
            Rule::return_clause => return_clause = Some(build_return_clause(inner)?),
            Rule::group_by_clause => group_by_clause = Some(build_group_by_clause(inner)?),  // NEW
            Rule::order_clause => order_clause = Some(build_order_clause(inner)?),
            Rule::limit_clause => limit_clause = Some(build_limit_clause(inner)?),
            Rule::EOI => {}
            _ => {}
        }
    }
    
    Ok(Query {
        match_clause: match_clause.ok_or(ParseError::MissingClause("MATCH"))?,
        where_clause,
        return_clause: return_clause.ok_or(ParseError::MissingClause("RETURN"))?,
        group_by_clause,
        order_clause,
        limit_clause,
    })
}
```

**Acceptance Criteria**:
- [ ] GROUP BY clause parses correctly
- [ ] Multiple expressions supported
- [ ] Missing GROUP BY keyword handled

---

#### Phase 2.4: GROUP BY Compiler
**File**: `src/gql/compiler.rs`  
**Duration**: 3-4 hours

**Tasks**:
1. Detect GROUP BY clause
2. Group results by specified expressions
3. Apply aggregates per group
4. Return grouped results

**Code**:

```rust
impl<'a, 'g> Compiler<'a, 'g> {
    fn compile(&mut self, query: &Query) -> Result<Vec<Value>, CompileError> {
        // ... existing pattern compilation ...
        
        // Check for GROUP BY
        if let Some(group_by) = &query.group_by_clause {
            return self.execute_grouped_query(
                query,
                &query.return_clause,
                &query.where_clause,
                group_by,
                traversal,
            );
        }
        
        // ... existing non-grouped execution ...
    }
    
    /// Execute a query with GROUP BY clause.
    fn execute_grouped_query(
        &self,
        query: &Query,
        return_clause: &ReturnClause,
        where_clause: &Option<WhereClause>,
        group_by: &GroupByClause,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<Vec<Value>, CompileError> {
        // Collect all matched elements
        let matched_elements: Vec<Value> = traversal.to_list();
        
        // Apply WHERE filter
        let filtered_elements: Vec<Value> = if let Some(where_cl) = where_clause {
            matched_elements
                .into_iter()
                .filter(|element| self.evaluate_predicate(&where_cl.expression, element))
                .collect()
        } else {
            matched_elements
        };
        
        // Group elements by GROUP BY expressions
        let mut groups: HashMap<Vec<ComparableValue>, Vec<Value>> = HashMap::new();
        
        for element in filtered_elements {
            let group_key: Vec<ComparableValue> = group_by.expressions
                .iter()
                .map(|expr| {
                    let val = self.evaluate_value(expr, &element);
                    ComparableValue::from(val)
                })
                .collect();
            
            groups.entry(group_key).or_default().push(element);
        }
        
        // For each group, compute the RETURN clause
        let mut results = Vec::new();
        
        for (group_key, group_elements) in groups {
            let result = self.compute_group_result(
                return_clause,
                group_by,
                &group_key,
                &group_elements,
            )?;
            results.push(result);
        }
        
        Ok(results)
    }
    
    /// Compute the result for a single group.
    fn compute_group_result(
        &self,
        return_clause: &ReturnClause,
        group_by: &GroupByClause,
        group_key: &[ComparableValue],
        group_elements: &[Value],
    ) -> Result<Value, CompileError> {
        let mut result_map = HashMap::new();
        
        for (i, item) in return_clause.items.iter().enumerate() {
            let key = self.get_return_item_key(item);
            
            let value = if Self::expr_has_aggregate(&item.expression) {
                // Compute aggregate over group
                self.compute_aggregate_for_group(&item.expression, group_elements)?
            } else {
                // Non-aggregate: should be a GROUP BY expression
                // Use the group key value
                self.find_group_key_value(&item.expression, group_by, group_key, i)?
            };
            
            result_map.insert(key, value);
        }
        
        // If single item, return value directly; otherwise return map
        if return_clause.items.len() == 1 {
            Ok(result_map.into_values().next().unwrap())
        } else {
            Ok(Value::Map(result_map))
        }
    }
    
    /// Find the group key value for a non-aggregate expression.
    fn find_group_key_value(
        &self,
        expr: &Expression,
        group_by: &GroupByClause,
        group_key: &[ComparableValue],
        _item_index: usize,
    ) -> Result<Value, CompileError> {
        // Find which group_by expression matches this return expression
        for (i, group_expr) in group_by.expressions.iter().enumerate() {
            if self.expressions_match(expr, group_expr) {
                return Ok(group_key[i].clone().into());
            }
        }
        
        // Expression not in GROUP BY - this is a semantic error
        Err(CompileError::ExpressionNotInGroupBy)
    }
    
    /// Check if two expressions are equivalent.
    fn expressions_match(&self, a: &Expression, b: &Expression) -> bool {
        // Simple structural comparison
        match (a, b) {
            (Expression::Variable(va), Expression::Variable(vb)) => va == vb,
            (
                Expression::Property { variable: va, property: pa },
                Expression::Property { variable: vb, property: pb },
            ) => va == vb && pa == pb,
            _ => false,
        }
    }
    
    /// Compute an aggregate expression for a group.
    fn compute_aggregate_for_group(
        &self,
        expr: &Expression,
        group_elements: &[Value],
    ) -> Result<Value, CompileError> {
        match expr {
            Expression::Aggregate { func, distinct, expr: inner } => {
                self.compute_aggregate(*func, *distinct, inner, group_elements)
            }
            _ => {
                // Non-aggregate in aggregate context - evaluate on first element
                group_elements.first()
                    .map(|e| self.evaluate_value(expr, e))
                    .ok_or(CompileError::EmptyPattern)
            }
        }
    }
}

// Add new error variant
#[derive(Debug, Error)]
pub enum CompileError {
    // ... existing variants ...
    
    #[error("Non-aggregated expression must appear in GROUP BY clause")]
    ExpressionNotInGroupBy,
}
```

**Acceptance Criteria**:
- [ ] `GROUP BY p.position` groups correctly
- [ ] Aggregates computed per group
- [ ] Non-aggregate expressions validated against GROUP BY
- [ ] Multiple GROUP BY expressions work

---

#### Phase 2.5: GROUP BY Integration Tests
**File**: `tests/gql.rs`  
**Duration**: 1-2 hours

**Code**:

```rust
#[test]
fn test_gql_group_by_single() {
    let graph = create_nba_test_graph();
    let snapshot = graph.snapshot();
    
    // Count players by position
    let results = snapshot.gql(r#"
        MATCH (p:player)
        RETURN p.position, count(*) AS player_count
        GROUP BY p.position
    "#).unwrap();
    
    // Should have groups for each position
    assert!(!results.is_empty());
    
    // Each result should be a map with position and count
    for result in &results {
        if let Value::Map(map) = result {
            assert!(map.contains_key("p.position") || map.contains_key("position"));
            assert!(map.contains_key("player_count"));
        }
    }
}

#[test]
fn test_gql_group_by_with_avg() {
    let graph = create_nba_test_graph();
    let snapshot = graph.snapshot();
    
    // Average PPG by position
    let results = snapshot.gql(r#"
        MATCH (p:player)
        RETURN p.position, avg(p.points_per_game) AS avg_ppg
        GROUP BY p.position
    "#).unwrap();
    
    assert!(!results.is_empty());
}

#[test]
fn test_gql_group_by_multiple() {
    let graph = create_nba_test_graph();
    let snapshot = graph.snapshot();
    
    // Group by multiple expressions
    let results = snapshot.gql(r#"
        MATCH (p:player)-[:played_for]->(t:team)
        RETURN t.conference, p.position, count(*) AS cnt
        GROUP BY t.conference, p.position
    "#).unwrap();
    
    // Should have groups for each conference/position combination
}

#[test]
fn test_gql_group_by_validation_error() {
    let graph = create_nba_test_graph();
    let snapshot = graph.snapshot();
    
    // This should fail - p.name not in GROUP BY
    let result = snapshot.gql(r#"
        MATCH (p:player)
        RETURN p.position, p.name, count(*)
        GROUP BY p.position
    "#);
    
    assert!(result.is_err());
}
```

**Acceptance Criteria**:
- [ ] Single GROUP BY works
- [ ] Multiple GROUP BY expressions work
- [ ] AVG, COUNT, SUM work per group
- [ ] Validation error for missing GROUP BY expression

---

### Week 3: Multi-Variable Patterns and Edge Properties

Enable queries that bind and return multiple variables from a pattern, and access edge properties.

---

#### Phase 3.1: Multi-Variable Binding
**File**: `src/gql/compiler.rs`  
**Duration**: 3-4 hours

**Current Limitation**: The compiler only tracks the "current" element. Pattern variables like `(a)-[:KNOWS]->(b)` don't preserve `a` when traversing to `b`.

**Tasks**:
1. Track all bound variables during pattern compilation
2. Use `as_()` step to label positions in path
3. Enable `select()` in RETURN clause to retrieve multiple variables

**Code**:

```rust
impl<'a, 'g> Compiler<'a, 'g> {
    /// Compile a pattern with variable tracking.
    fn compile_pattern_with_bindings(
        &mut self,
        pattern: &Pattern,
        mut traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
        // Enable path tracking for multi-variable patterns
        let has_multiple_vars = self.count_pattern_variables(pattern) > 1;
        if has_multiple_vars {
            traversal = traversal.with_path();
        }
        
        for (element_index, element) in pattern.elements.iter().enumerate() {
            match element {
                PatternElement::Node(node) => {
                    traversal = self.compile_node(node, traversal, element_index)?;
                    
                    // Add as_() step for variable binding
                    if let Some(var) = &node.variable {
                        traversal = traversal.as_(var);
                    }
                }
                PatternElement::Edge(edge) => {
                    traversal = self.compile_edge(edge, traversal)?;
                    
                    // Add as_() step for edge variable binding
                    if let Some(var) = &edge.variable {
                        traversal = traversal.as_(var);
                    }
                }
            }
        }
        
        Ok(traversal)
    }
    
    /// Count the number of variables in a pattern.
    fn count_pattern_variables(&self, pattern: &Pattern) -> usize {
        pattern.elements.iter().filter(|e| {
            match e {
                PatternElement::Node(n) => n.variable.is_some(),
                PatternElement::Edge(e) => e.variable.is_some(),
            }
        }).count()
    }
    
    /// Evaluate a return item that may reference multiple variables.
    fn evaluate_return_for_multi_var(
        &self,
        items: &[ReturnItem],
        traverser: &Traverser,
    ) -> Option<Value> {
        if items.len() == 1 {
            self.evaluate_expression_from_path(&items[0].expression, traverser)
        } else {
            let mut map = HashMap::new();
            for item in items {
                let key = self.get_return_item_key(item);
                let value = self.evaluate_expression_from_path(&item.expression, traverser)?;
                map.insert(key, value);
            }
            Some(Value::Map(map))
        }
    }
    
    /// Evaluate an expression using the traverser's path for variable lookup.
    fn evaluate_expression_from_path(
        &self,
        expr: &Expression,
        traverser: &Traverser,
    ) -> Option<Value> {
        match expr {
            Expression::Variable(var) => {
                // Look up variable in path
                traverser.path.get(var)
                    .and_then(|values| values.first())
                    .map(|pv| pv.to_value())
            }
            Expression::Property { variable, property } => {
                // Look up variable in path, then extract property
                traverser.path.get(variable)
                    .and_then(|values| values.first())
                    .and_then(|pv| {
                        match pv {
                            PathValue::Vertex(id) => {
                                self.snapshot.storage().get_vertex(*id)
                                    .and_then(|v| v.properties.get(property).cloned())
                            }
                            PathValue::Edge(id) => {
                                self.snapshot.storage().get_edge(*id)
                                    .and_then(|e| e.properties.get(property).cloned())
                            }
                            _ => None,
                        }
                    })
            }
            Expression::Literal(lit) => Some(lit.clone().into()),
            _ => None,
        }
    }
}
```

**Acceptance Criteria**:
- [ ] `MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name` works
- [ ] `MATCH (a)-[e:KNOWS]->(b) RETURN a, e, b` returns all three
- [ ] Path tracking enabled automatically when needed

---

#### Phase 3.2: Edge Variable and Property Access
**File**: `src/gql/compiler.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Compile edge variables with `out_e()` / `in_e()` patterns
2. Enable property access on edge variables
3. Handle edge-to-vertex navigation with `other_v()`

**Code**:

```rust
impl<'a, 'g> Compiler<'a, 'g> {
    /// Compile an edge pattern that needs variable binding.
    /// 
    /// When an edge has a variable, we need to:
    /// 1. Navigate to the edge (out_e/in_e)
    /// 2. Bind the edge with as_()
    /// 3. Navigate to the target vertex (in_v/out_v)
    fn compile_edge_with_variable(
        &mut self,
        edge: &EdgePattern,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
        let labels: Vec<&str> = edge.labels.iter().map(|s| s.as_str()).collect();
        
        // Step 1: Navigate to edge
        let traversal = match edge.direction {
            EdgeDirection::Outgoing => {
                if labels.is_empty() {
                    traversal.out_e()
                } else {
                    traversal.out_e_labels(&labels)
                }
            }
            EdgeDirection::Incoming => {
                if labels.is_empty() {
                    traversal.in_e()
                } else {
                    traversal.in_e_labels(&labels)
                }
            }
            EdgeDirection::Both => {
                if labels.is_empty() {
                    traversal.both_e()
                } else {
                    traversal.both_e_labels(&labels)
                }
            }
        };
        
        // Step 2: Bind edge variable
        let traversal = if let Some(var) = &edge.variable {
            self.bindings.insert(var.clone(), BindingInfo {
                pattern_index: 0,
                is_node: false,
            });
            traversal.as_(var)
        } else {
            traversal
        };
        
        // Step 3: Apply property filters on edge
        let mut traversal = traversal;
        for (key, value) in &edge.properties {
            let val: Value = value.clone().into();
            traversal = traversal.has_value(key.as_str(), val);
        }
        
        // Step 4: Navigate to target vertex
        let traversal = match edge.direction {
            EdgeDirection::Outgoing => traversal.in_v(),
            EdgeDirection::Incoming => traversal.out_v(),
            EdgeDirection::Both => traversal.other_v(),
        };
        
        Ok(traversal)
    }
}
```

**Acceptance Criteria**:
- [ ] `MATCH (a)-[e:played_for]->(t) RETURN e.years` works
- [ ] Edge properties can be filtered: `{since: 2020}`
- [ ] Edge variable can be returned in RETURN clause

---

#### Phase 3.3: Edge Property Access in Expressions
**File**: `src/gql/compiler.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Handle edge variable property access in WHERE
2. Handle edge variable property access in RETURN
3. Handle edge variable property access in ORDER BY

**Code**:

```rust
impl<'a, 'g> Compiler<'a, 'g> {
    /// Extract a property from an element (vertex or edge) in the path.
    fn extract_property_from_binding(
        &self,
        variable: &str,
        property: &str,
        traverser: &Traverser,
    ) -> Value {
        // Check if this is bound in the path
        if let Some(values) = traverser.path.get(variable) {
            if let Some(path_value) = values.first() {
                return match path_value {
                    PathValue::Vertex(id) => {
                        self.snapshot.storage()
                            .get_vertex(*id)
                            .and_then(|v| v.properties.get(property).cloned())
                            .unwrap_or(Value::Null)
                    }
                    PathValue::Edge(id) => {
                        self.snapshot.storage()
                            .get_edge(*id)
                            .and_then(|e| e.properties.get(property).cloned())
                            .unwrap_or(Value::Null)
                    }
                    PathValue::Property(v) => {
                        // Can't get property of a property
                        Value::Null
                    }
                };
            }
        }
        Value::Null
    }
}
```

**Acceptance Criteria**:
- [ ] `WHERE e.ring_count > 1` filters on edge property
- [ ] `RETURN e.years` returns edge property value
- [ ] `ORDER BY e.start_year` sorts by edge property

---

#### Phase 3.4: Multi-Variable Pattern Integration Tests
**File**: `tests/gql.rs`  
**Duration**: 1-2 hours

**Code**:

```rust
#[test]
fn test_gql_multi_variable_return() {
    let graph = create_nba_test_graph();
    let snapshot = graph.snapshot();
    
    // Return properties from both ends of a relationship
    let results = snapshot.gql(r#"
        MATCH (p:player)-[:played_for]->(t:team)
        RETURN p.name AS player, t.name AS team
    "#).unwrap();
    
    assert!(!results.is_empty());
    for result in &results {
        if let Value::Map(map) = result {
            assert!(map.contains_key("player"));
            assert!(map.contains_key("team"));
        }
    }
}

#[test]
fn test_gql_edge_variable_and_property() {
    let graph = create_nba_test_graph();
    let snapshot = graph.snapshot();
    
    // Access edge properties
    let results = snapshot.gql(r#"
        MATCH (p:player)-[e:won_championship_with]->(t:team)
        RETURN p.name, t.name, e.ring_count
    "#).unwrap();
    
    // Verify edge property is returned
    for result in &results {
        if let Value::Map(map) = result {
            assert!(map.contains_key("e.ring_count"));
        }
    }
}

#[test]
fn test_gql_edge_property_filter() {
    let graph = create_nba_test_graph();
    let snapshot = graph.snapshot();
    
    // Filter by edge property
    let results = snapshot.gql(r#"
        MATCH (p:player)-[e:won_championship_with]->(t:team)
        WHERE e.ring_count >= 3
        RETURN p.name, t.name
    "#).unwrap();
    
    // Should find players with 3+ rings with a single team
}

#[test]
fn test_gql_three_node_pattern() {
    let graph = create_nba_test_graph();
    let snapshot = graph.snapshot();
    
    // Three-node pattern: player -> team <- player (teammates)
    let results = snapshot.gql(r#"
        MATCH (p1:player)-[:played_for]->(t:team)<-[:played_for]-(p2:player)
        WHERE p1.name <> p2.name
        RETURN p1.name AS player1, t.name AS team, p2.name AS player2
        LIMIT 10
    "#).unwrap();
    
    assert!(!results.is_empty());
}
```

**Acceptance Criteria**:
- [ ] Multi-variable RETURN works
- [ ] Edge variable binding works
- [ ] Edge property access works
- [ ] Three-node patterns work

---

### Week 4: Functions and Polish

Add common functions and polish the implementation.

---

#### Phase 4.1: COALESCE Function
**File**: `src/gql/grammar.pest`, `src/gql/ast.rs`, `src/gql/compiler.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Add COALESCE to grammar as a function
2. Parse COALESCE(expr, expr, ...) to AST
3. Compile to first non-null value evaluation

**Grammar**:

```pest
// COALESCE is parsed as a regular function call
// No grammar changes needed - function_call already handles it
```

**Compiler**:

```rust
impl<'a, 'g> Compiler<'a, 'g> {
    /// Evaluate a function call expression.
    fn evaluate_function_call(
        &self,
        name: &str,
        args: &[Expression],
        element: &Value,
    ) -> Value {
        match name.to_uppercase().as_str() {
            "COALESCE" => {
                // Return first non-null argument
                for arg in args {
                    let val = self.evaluate_value(arg, element);
                    if !matches!(val, Value::Null) {
                        return val;
                    }
                }
                Value::Null
            }
            "TOUPPER" | "UPPER" => {
                if let Some(arg) = args.first() {
                    if let Value::String(s) = self.evaluate_value(arg, element) {
                        return Value::String(s.to_uppercase());
                    }
                }
                Value::Null
            }
            "TOLOWER" | "LOWER" => {
                if let Some(arg) = args.first() {
                    if let Value::String(s) = self.evaluate_value(arg, element) {
                        return Value::String(s.to_lowercase());
                    }
                }
                Value::Null
            }
            "SIZE" | "LENGTH" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::String(s) => Value::Int(s.len() as i64),
                        Value::List(l) => Value::Int(l.len() as i64),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "ABS" => {
                if let Some(arg) = args.first() {
                    match self.evaluate_value(arg, element) {
                        Value::Int(n) => Value::Int(n.abs()),
                        Value::Float(f) => Value::Float(f.abs()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            _ => {
                // Unknown function
                Value::Null
            }
        }
    }
}
```

**Acceptance Criteria**:
- [ ] `COALESCE(p.nickname, p.name)` returns first non-null
- [ ] `COALESCE(null, null, 'default')` returns 'default'
- [ ] Works in RETURN and WHERE clauses

---

#### Phase 4.2: CASE Expression
**File**: `src/gql/grammar.pest`, `src/gql/ast.rs`, `src/gql/compiler.rs`  
**Duration**: 3-4 hours

**Grammar**:

```pest
// Add CASE expression
case_expr = { 
    CASE ~ 
    (case_when_clause)+ ~ 
    case_else_clause? ~ 
    END 
}

case_when_clause = { WHEN ~ expression ~ THEN ~ expression }
case_else_clause = { ELSE ~ expression }

CASE = @{ ^"case" ~ !ASCII_ALPHANUMERIC }
WHEN = @{ ^"when" ~ !ASCII_ALPHANUMERIC }
THEN = @{ ^"then" ~ !ASCII_ALPHANUMERIC }
ELSE = @{ ^"else" ~ !ASCII_ALPHANUMERIC }
END  = @{ ^"end" ~ !ASCII_ALPHANUMERIC }

// Add to primary
primary = { 
    case_expr     // NEW
    | exists_expr
    | literal
    | function_call
    | property_access
    | variable
    | paren_expr
    | list_expr
}
```

**AST**:

```rust
/// CASE expression with WHEN/THEN/ELSE branches.
#[derive(Debug, Clone, Serialize)]
pub struct CaseExpression {
    /// WHEN condition THEN result pairs
    pub when_clauses: Vec<(Expression, Expression)>,
    /// Optional ELSE result
    pub else_clause: Option<Box<Expression>>,
}

// Add to Expression enum
#[derive(Debug, Clone, Serialize)]
pub enum Expression {
    // ... existing variants ...
    
    /// CASE WHEN ... THEN ... ELSE ... END
    Case(CaseExpression),
}
```

**Compiler**:

```rust
impl<'a, 'g> Compiler<'a, 'g> {
    fn evaluate_case(&self, case: &CaseExpression, element: &Value) -> Value {
        // Evaluate each WHEN clause
        for (condition, result) in &case.when_clauses {
            if self.evaluate_predicate(condition, element) {
                return self.evaluate_value(result, element);
            }
        }
        
        // No WHEN matched, evaluate ELSE or return null
        if let Some(else_expr) = &case.else_clause {
            self.evaluate_value(else_expr, element)
        } else {
            Value::Null
        }
    }
}
```

**Acceptance Criteria**:
- [ ] Simple CASE works: `CASE WHEN p.age > 30 THEN 'Senior' ELSE 'Junior' END`
- [ ] Multiple WHEN clauses work
- [ ] ELSE is optional (returns null if no match)
- [ ] CASE in RETURN clause works

---

#### Phase 4.3: Type Conversion Functions
**File**: `src/gql/compiler.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Add type conversion functions to function_call handler
2. Support: `toString()`, `toInteger()`, `toFloat()`, `toBoolean()`

**Code**:

```rust
impl<'a, 'g> Compiler<'a, 'g> {
    fn evaluate_function_call(
        &self,
        name: &str,
        args: &[Expression],
        element: &Value,
    ) -> Value {
        match name.to_uppercase().as_str() {
            // ... existing functions ...
            
            "TOSTRING" => {
                if let Some(arg) = args.first() {
                    let val = self.evaluate_value(arg, element);
                    match val {
                        Value::String(s) => Value::String(s),
                        Value::Int(n) => Value::String(n.to_string()),
                        Value::Float(f) => Value::String(f.to_string()),
                        Value::Bool(b) => Value::String(b.to_string()),
                        Value::Null => Value::Null,
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "TOINTEGER" | "TOINT" => {
                if let Some(arg) = args.first() {
                    let val = self.evaluate_value(arg, element);
                    match val {
                        Value::Int(n) => Value::Int(n),
                        Value::Float(f) => Value::Int(f as i64),
                        Value::String(s) => s.parse::<i64>().ok()
                            .map(Value::Int)
                            .unwrap_or(Value::Null),
                        Value::Bool(b) => Value::Int(if b { 1 } else { 0 }),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "TOFLOAT" => {
                if let Some(arg) = args.first() {
                    let val = self.evaluate_value(arg, element);
                    match val {
                        Value::Float(f) => Value::Float(f),
                        Value::Int(n) => Value::Float(n as f64),
                        Value::String(s) => s.parse::<f64>().ok()
                            .map(Value::Float)
                            .unwrap_or(Value::Null),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            "TOBOOLEAN" | "TOBOOL" => {
                if let Some(arg) = args.first() {
                    let val = self.evaluate_value(arg, element);
                    match val {
                        Value::Bool(b) => Value::Bool(b),
                        Value::String(s) => match s.to_lowercase().as_str() {
                            "true" | "yes" | "1" => Value::Bool(true),
                            "false" | "no" | "0" => Value::Bool(false),
                            _ => Value::Null,
                        },
                        Value::Int(n) => Value::Bool(n != 0),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            
            _ => Value::Null,
        }
    }
}
```

**Acceptance Criteria**:
- [ ] `toString(42)` returns `"42"`
- [ ] `toInteger("123")` returns `123`
- [ ] `toFloat(10)` returns `10.0`
- [ ] Invalid conversions return null

---

#### Phase 4.4: Create GQL Example
**File**: `examples/nba_gql.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Create new example demonstrating GQL queries
2. Port applicable queries from `nba_mmap_read.rs`
3. Show side-by-side comparison with traversal API

**Code structure**:

```rust
//! NBA Graph Database - GQL Query Example
//!
//! This example demonstrates querying the NBA graph using GQL,
//! comparing equivalent queries to the traversal API.

use rustgremlin::graph::Graph;
use rustgremlin::storage::mmap::MmapGraph;
use std::sync::Arc;

fn main() {
    // Open the persistent database
    let storage = MmapGraph::open("examples/data/nba_graph.db").expect("...");
    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();
    
    println!("=== GQL Query Examples ===\n");
    
    // Section 1: Basic Queries
    demonstrate_basic_queries(&snapshot);
    
    // Section 2: Navigation
    demonstrate_navigation(&snapshot);
    
    // Section 3: Filtering with WHERE
    demonstrate_filtering(&snapshot);
    
    // Section 4: Aggregation with GROUP BY
    demonstrate_aggregation(&snapshot);
    
    // Section 5: EXISTS patterns
    demonstrate_exists(&snapshot);
    
    // Section 6: Multi-variable patterns
    demonstrate_multi_variable(&snapshot);
}

fn demonstrate_basic_queries(snapshot: &GraphSnapshot) {
    println!("--- Basic Queries ---\n");
    
    // Find all players
    let results = snapshot.gql("MATCH (p:player) RETURN p.name").unwrap();
    println!("Players: {} found", results.len());
    
    // Find Point Guards
    let results = snapshot.gql(r#"
        MATCH (p:player {position: 'Point Guard'})
        RETURN p.name
    "#).unwrap();
    println!("Point Guards: {:?}", results);
}

fn demonstrate_navigation(snapshot: &GraphSnapshot) {
    println!("\n--- Navigation Queries ---\n");
    
    // Teams Michael Jordan played for
    let results = snapshot.gql(r#"
        MATCH (p:player {name: 'Michael Jordan'})-[:played_for]->(t:team)
        RETURN t.name
    "#).unwrap();
    println!("MJ's teams: {:?}", results);
    
    // Players who played for Lakers
    let results = snapshot.gql(r#"
        MATCH (p:player)-[:played_for]->(t:team {name: 'Los Angeles Lakers'})
        RETURN p.name
    "#).unwrap();
    println!("Lakers players: {:?}", results);
}

fn demonstrate_filtering(snapshot: &GraphSnapshot) {
    println!("\n--- Filtering Queries ---\n");
    
    // Elite scorers (25+ PPG)
    let results = snapshot.gql(r#"
        MATCH (p:player)
        WHERE p.points_per_game >= 25.0
        RETURN p.name, p.points_per_game
        ORDER BY p.points_per_game DESC
    "#).unwrap();
    println!("Elite scorers: {:?}", results);
    
    // Guards (using IN)
    let results = snapshot.gql(r#"
        MATCH (p:player)
        WHERE p.position IN ['Point Guard', 'Shooting Guard']
        RETURN p.name, p.position
    "#).unwrap();
    println!("Guards: {} found", results.len());
}

fn demonstrate_aggregation(snapshot: &GraphSnapshot) {
    println!("\n--- Aggregation Queries ---\n");
    
    // Count players by position
    let results = snapshot.gql(r#"
        MATCH (p:player)
        RETURN p.position, count(*) AS count
        GROUP BY p.position
    "#).unwrap();
    println!("Players by position: {:?}", results);
    
    // Average PPG by position
    let results = snapshot.gql(r#"
        MATCH (p:player)
        RETURN p.position, avg(p.points_per_game) AS avg_ppg
        GROUP BY p.position
    "#).unwrap();
    println!("Avg PPG by position: {:?}", results);
}

fn demonstrate_exists(snapshot: &GraphSnapshot) {
    println!("\n--- EXISTS Queries ---\n");
    
    // Championship winners
    let results = snapshot.gql(r#"
        MATCH (p:player)
        WHERE EXISTS { (p)-[:won_championship_with]->() }
        RETURN p.name
    "#).unwrap();
    println!("Championship winners: {:?}", results);
    
    // Players WITHOUT championships
    let results = snapshot.gql(r#"
        MATCH (p:player)
        WHERE NOT EXISTS { (p)-[:won_championship_with]->() }
        RETURN p.name
    "#).unwrap();
    println!("No rings: {:?}", results);
}

fn demonstrate_multi_variable(snapshot: &GraphSnapshot) {
    println!("\n--- Multi-Variable Queries ---\n");
    
    // Player to team relationships
    let results = snapshot.gql(r#"
        MATCH (p:player)-[:played_for]->(t:team)
        RETURN p.name AS player, t.name AS team
        LIMIT 10
    "#).unwrap();
    println!("Player-Team pairs (first 10): {:?}", results);
    
    // Edge property access
    let results = snapshot.gql(r#"
        MATCH (p:player)-[e:won_championship_with]->(t:team)
        RETURN p.name, t.name, e.ring_count
        LIMIT 5
    "#).unwrap();
    println!("Championship details: {:?}", results);
}
```

**Acceptance Criteria**:
- [ ] Example compiles and runs
- [ ] Demonstrates all major GQL features
- [ ] Output is readable and matches traversal API results

---

#### Phase 4.5: Snapshot Tests for New Features
**File**: `tests/gql_snapshots.rs`  
**Duration**: 1-2 hours

**Code**:

```rust
use insta::assert_yaml_snapshot;
use rustgremlin::gql::parse;

#[test]
fn test_parse_exists_snapshot() {
    let ast = parse(r#"
        MATCH (p:player)
        WHERE EXISTS { (p)-[:won_championship_with]->() }
        RETURN p
    "#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_not_exists_snapshot() {
    let ast = parse(r#"
        MATCH (p:player)
        WHERE NOT EXISTS { (p)-[:knows]->(:player {active: true}) }
        RETURN p.name
    "#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_group_by_snapshot() {
    let ast = parse(r#"
        MATCH (p:player)
        RETURN p.position, count(*) AS cnt, avg(p.ppg) AS avg
        GROUP BY p.position
    "#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_case_expression_snapshot() {
    let ast = parse(r#"
        MATCH (p:player)
        RETURN p.name, 
            CASE 
                WHEN p.age > 35 THEN 'Veteran'
                WHEN p.age > 28 THEN 'Prime'
                ELSE 'Young'
            END AS category
    "#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_multi_variable_pattern_snapshot() {
    let ast = parse(r#"
        MATCH (a:player)-[e:played_for]->(t:team)<-[:played_for]-(b:player)
        WHERE a.name <> b.name
        RETURN a.name, t.name, b.name
    "#).unwrap();
    assert_yaml_snapshot!(ast);
}
```

**Acceptance Criteria**:
- [ ] Snapshot tests created for all new features
- [ ] Snapshots are stable and reviewed

---

## Exit Criteria Checklist

### Parser Extensions
- [ ] EXISTS expression parses
- [ ] NOT EXISTS expression parses
- [ ] GROUP BY clause parses
- [ ] CASE expression parses
- [ ] Edge variable in pattern parses

### Compiler Extensions
- [ ] EXISTS compiles to sub-pattern evaluation
- [ ] GROUP BY compiles to grouped aggregation
- [ ] Multi-variable patterns track all bindings
- [ ] Edge properties accessible in expressions
- [ ] COALESCE and CASE functions work
- [ ] Type conversion functions work

### Integration
- [ ] NBA example queries work in GQL
- [ ] New `examples/nba_gql.rs` demonstrates features
- [ ] All tests pass
- [ ] Snapshot tests stable

### Documentation
- [ ] Module docs updated for new features
- [ ] Examples in doc comments
- [ ] Error messages helpful

---

## Query Coverage After Plan 10

After implementing this plan, the following NBA example queries will be expressible in GQL:

| Category | Queries | Coverage |
|----------|---------|----------|
| Basic Queries | 1-6 | 100% |
| Navigation | 7-12 | 100% |
| Predicates | 13-21 | 95% |
| Anonymous Traversals | 22-27 | 100% (via EXISTS) |
| Branch Steps | 28-31 | 50% (COALESCE, CASE) |
| Repeat Steps | 32-35 | 80% (via variable-length paths) |
| Path Tracking | 36-38 | 75% (multi-var, no full path) |
| Complex Combined | 39-45 | 85% |
| Transform Steps | 46-54 | 70% |
| Aggregation | 55-60 | 100% (via GROUP BY) |

**Overall Coverage**: ~85% of NBA example queries expressible in GQL

---

## File Summary

**Modified files**:
- `src/gql/grammar.pest` - EXISTS, GROUP BY, CASE syntax
- `src/gql/ast.rs` - New expression types, GroupByClause
- `src/gql/parser.rs` - Parse new constructs
- `src/gql/compiler.rs` - Compile new features, multi-var binding
- `src/gql/error.rs` - New error types

**New files**:
- `examples/nba_gql.rs` - GQL query demonstration

**Test files**:
- `tests/gql.rs` - New integration tests
- `tests/gql_snapshots.rs` - New snapshot tests

---

## References

- `plans/plan-09.md` - Original GQL implementation
- `examples/nba_mmap_read.rs` - Target query coverage
- `guiding-documents/gql.md` - GQL language specification
- [ISO GQL Standard](https://www.iso.org/standard/76120.html) - Reference specification
- [openCypher](https://opencypher.org/) - Related query language
