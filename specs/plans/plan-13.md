# Plan 13: Implement GQL Mutation Clauses

**Spec Reference:** `specs/spec-11-gql-mutations.md`

**Goal:** Implement GQL mutation clauses (CREATE, SET, REMOVE, DELETE, DETACH DELETE, MERGE) that compile to existing Gremlin-style mutation steps.

**Estimated Duration:** 4-5 weeks

---

## Overview

This plan implements the GQL mutation clauses defined in Spec 11. The implementation builds on the existing Gremlin mutation steps (Spec 10 / Plan 12) and extends the GQL parser and compiler to support declarative mutations.

The implementation is divided into phases:
1. Grammar extensions for mutation syntax
2. AST types for mutation clauses
3. Parser implementation
4. Compiler implementation for each clause type
5. Integration testing

---

## Phase 1: Grammar Extensions (Week 1, Days 1-2)

### 1.1 Add Mutation Keywords

**File:** `src/gql/grammar.pest`

Add new keywords for mutation clauses:

```pest
// Add after existing keywords (line ~51)
CREATE   = @{ ^"create" ~ !ASCII_ALPHANUMERIC }
SET      = @{ ^"set" ~ !ASCII_ALPHANUMERIC }
REMOVE   = @{ ^"remove" ~ !ASCII_ALPHANUMERIC }
DELETE   = @{ ^"delete" ~ !ASCII_ALPHANUMERIC }
DETACH   = @{ ^"detach" ~ !ASCII_ALPHANUMERIC }
MERGE    = @{ ^"merge" ~ !ASCII_ALPHANUMERIC }
ON       = @{ ^"on" ~ !ASCII_ALPHANUMERIC }
```

**Tasks:**
- [ ] Add CREATE, SET, REMOVE, DELETE, DETACH, MERGE, ON keywords
- [ ] Update `keyword` rule to include new keywords
- [ ] Test keywords don't conflict with identifiers

### 1.2 Add Mutation Statement Grammar

**File:** `src/gql/grammar.pest`

Update the statement rule to support mutations:

```pest
// Updated entry point - supports both read queries and mutations
statement = { SOI ~ (mutation_statement | read_statement) ~ EOI }

// Read-only statement (existing query structure)
read_statement = { query ~ (union_clause ~ query)* }

// Mutation statement
mutation_statement = { 
    // CREATE-only (no MATCH required)
    create_only_statement
    // MATCH with mutations
    | match_mutation_statement
    // MERGE statement
    | merge_statement
}

// CREATE without preceding MATCH
create_only_statement = { create_clause+ ~ return_clause? }

// MATCH followed by mutation clauses
match_mutation_statement = { 
    match_clause ~ 
    optional_match_clause* ~ 
    where_clause? ~ 
    mutation_clause+ ~ 
    return_clause? 
}

// MERGE statement
merge_statement = { merge_clause ~ merge_action* ~ return_clause? }
```

**Tasks:**
- [ ] Update `statement` rule for mutation support
- [ ] Add `mutation_statement`, `create_only_statement`, `match_mutation_statement`
- [ ] Ensure existing read queries still parse correctly

### 1.3 Add Mutation Clause Grammar

**File:** `src/gql/grammar.pest`

```pest
// Mutation clauses
mutation_clause = { create_clause | set_clause | remove_clause | delete_clause | detach_delete_clause }

// CREATE clause - creates new patterns
create_clause = { CREATE ~ pattern ~ ("," ~ pattern)* }

// SET clause - updates properties  
set_clause = { SET ~ set_item ~ ("," ~ set_item)* }
set_item = { property_access ~ "=" ~ expression }

// REMOVE clause - removes properties
remove_clause = { REMOVE ~ remove_item ~ ("," ~ remove_item)* }
remove_item = { property_access }

// DELETE clause - deletes elements (fails if vertex has edges)
delete_clause = { DELETE ~ variable ~ ("," ~ variable)* }

// DETACH DELETE clause - deletes vertices with edge cascade
detach_delete_clause = { DETACH ~ DELETE ~ variable ~ ("," ~ variable)* }

// MERGE clause - upsert pattern
merge_clause = { MERGE ~ pattern }
merge_action = { on_create_action | on_match_action }
on_create_action = { ON ~ CREATE ~ SET ~ set_item ~ ("," ~ set_item)* }
on_match_action = { ON ~ MATCH ~ SET ~ set_item ~ ("," ~ set_item)* }
```

**Tasks:**
- [ ] Add `mutation_clause` rule with all variants
- [ ] Add `create_clause` rule
- [ ] Add `set_clause` and `set_item` rules
- [ ] Add `remove_clause` and `remove_item` rules
- [ ] Add `delete_clause` rule
- [ ] Add `detach_delete_clause` rule
- [ ] Add `merge_clause` with `merge_action` rules
- [ ] Test grammar with sample mutation queries

---

## Phase 2: AST Extensions (Week 1, Days 3-5)

### 2.1 Add Statement Enum Variant

**File:** `src/gql/ast.rs`

Extend the Statement enum:

```rust
/// A GQL statement - read query, union, or mutation
#[derive(Debug, Clone, Serialize)]
pub enum Statement {
    /// A single read query
    Query(Box<Query>),
    /// A UNION of multiple queries
    Union { queries: Vec<Query>, all: bool },
    /// A mutation query (CREATE, SET, DELETE, etc.)
    Mutation(Box<MutationQuery>),
}
```

**Tasks:**
- [ ] Add `Mutation` variant to `Statement` enum
- [ ] Add rustdoc for the new variant

### 2.2 Add MutationQuery Type

**File:** `src/gql/ast.rs`

```rust
/// A mutation query (CREATE, SET, DELETE, MERGE, etc.)
///
/// Represents a GQL statement that modifies the graph.
///
/// # Examples
///
/// ```text
/// CREATE (n:Person {name: 'Alice'})
///
/// MATCH (n:Person {name: 'Alice'}) SET n.age = 31 RETURN n
///
/// MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = 123
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct MutationQuery {
    /// Optional MATCH clause for pattern-based mutations
    pub match_clause: Option<MatchClause>,
    /// Optional MATCH clauses
    pub optional_match_clauses: Vec<OptionalMatchClause>,
    /// Optional WHERE clause for filtering
    pub where_clause: Option<WhereClause>,
    /// List of mutation clauses (CREATE, SET, DELETE, etc.)
    pub mutations: Vec<MutationClause>,
    /// Optional RETURN clause
    pub return_clause: Option<ReturnClause>,
}
```

**Tasks:**
- [ ] Add `MutationQuery` struct
- [ ] Add comprehensive rustdoc

### 2.3 Add Mutation Clause Types

**File:** `src/gql/ast.rs`

```rust
/// A mutation clause (CREATE, SET, DELETE, etc.)
#[derive(Debug, Clone, Serialize)]
pub enum MutationClause {
    /// CREATE clause - creates new vertices and edges
    Create(CreateClause),
    /// SET clause - updates properties
    Set(SetClause),
    /// REMOVE clause - removes properties
    Remove(RemoveClause),
    /// DELETE clause - deletes elements
    Delete(DeleteClause),
    /// DETACH DELETE clause - deletes vertices with edge cascade
    DetachDelete(DetachDeleteClause),
    /// MERGE clause - upsert operation
    Merge(MergeClause),
}

/// CREATE clause - creates new vertices and edges
#[derive(Debug, Clone, Serialize)]
pub struct CreateClause {
    /// Patterns to create
    pub patterns: Vec<Pattern>,
}

/// SET clause - updates properties
#[derive(Debug, Clone, Serialize)]
pub struct SetClause {
    /// Property assignments
    pub items: Vec<SetItem>,
}

/// A single SET assignment (e.g., n.age = 31)
#[derive(Debug, Clone, Serialize)]
pub struct SetItem {
    /// The property to set (variable.property)
    pub target: PropertyRef,
    /// The value expression
    pub value: Expression,
}

/// Reference to a property (variable.property)
#[derive(Debug, Clone, Serialize)]
pub struct PropertyRef {
    /// Variable name
    pub variable: String,
    /// Property name
    pub property: String,
}

/// REMOVE clause - removes properties
#[derive(Debug, Clone, Serialize)]
pub struct RemoveClause {
    /// Properties to remove
    pub properties: Vec<PropertyRef>,
}

/// DELETE clause - deletes elements (fails if vertex has edges)
#[derive(Debug, Clone, Serialize)]
pub struct DeleteClause {
    /// Variables referencing elements to delete
    pub variables: Vec<String>,
}

/// DETACH DELETE clause - deletes vertices with automatic edge removal
#[derive(Debug, Clone, Serialize)]
pub struct DetachDeleteClause {
    /// Variables referencing vertices to delete
    pub variables: Vec<String>,
}

/// MERGE clause - upsert operation
#[derive(Debug, Clone, Serialize)]
pub struct MergeClause {
    /// Pattern to merge (match or create)
    pub pattern: Pattern,
    /// Actions to perform when creating
    pub on_create: Option<Vec<SetItem>>,
    /// Actions to perform when matching
    pub on_match: Option<Vec<SetItem>>,
}
```

**Tasks:**
- [ ] Add `MutationClause` enum
- [ ] Add `CreateClause` struct
- [ ] Add `SetClause` and `SetItem` structs
- [ ] Add `PropertyRef` struct
- [ ] Add `RemoveClause` struct
- [ ] Add `DeleteClause` struct
- [ ] Add `DetachDeleteClause` struct
- [ ] Add `MergeClause` struct
- [ ] Add rustdoc for all types

---

## Phase 3: Parser Implementation (Week 2)

### 3.1 Parse Mutation Statement

**File:** `src/gql/parser.rs`

Add parsing logic for the statement entry point:

```rust
/// Parse a GQL statement (query or mutation)
pub fn parse(input: &str) -> Result<Statement, ParseError> {
    let pairs = GqlParser::parse(Rule::statement, input)?;
    
    for pair in pairs {
        match pair.as_rule() {
            Rule::statement => {
                let inner = pair.into_inner().next().unwrap();
                match inner.as_rule() {
                    Rule::read_statement => parse_read_statement(inner),
                    Rule::mutation_statement => parse_mutation_statement(inner),
                    _ => unreachable!(),
                }
            }
            _ => continue,
        }
    }
    
    unreachable!()
}

fn parse_mutation_statement(pair: Pair<Rule>) -> Result<Statement, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::create_only_statement => parse_create_only(inner),
        Rule::match_mutation_statement => parse_match_mutation(inner),
        Rule::merge_statement => parse_merge_statement(inner),
        _ => unreachable!(),
    }
}
```

**Tasks:**
- [ ] Update `parse()` to dispatch to mutation parser
- [ ] Implement `parse_mutation_statement()`
- [ ] Handle all three mutation statement types

### 3.2 Parse CREATE Clause

**File:** `src/gql/parser.rs`

```rust
fn parse_create_clause(pair: Pair<Rule>) -> Result<CreateClause, ParseError> {
    let mut patterns = Vec::new();
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::pattern => {
                patterns.push(parse_pattern(inner)?);
            }
            _ => {}
        }
    }
    
    Ok(CreateClause { patterns })
}

fn parse_create_only(pair: Pair<Rule>) -> Result<Statement, ParseError> {
    let mut mutations = Vec::new();
    let mut return_clause = None;
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::create_clause => {
                mutations.push(MutationClause::Create(parse_create_clause(inner)?));
            }
            Rule::return_clause => {
                return_clause = Some(parse_return_clause(inner)?);
            }
            _ => {}
        }
    }
    
    Ok(Statement::Mutation(Box::new(MutationQuery {
        match_clause: None,
        optional_match_clauses: vec![],
        where_clause: None,
        mutations,
        return_clause,
    })))
}
```

**Tasks:**
- [ ] Implement `parse_create_clause()`
- [ ] Implement `parse_create_only()`
- [ ] Reuse existing `parse_pattern()` function
- [ ] Write unit tests for CREATE parsing

### 3.3 Parse SET and REMOVE Clauses

**File:** `src/gql/parser.rs`

```rust
fn parse_set_clause(pair: Pair<Rule>) -> Result<SetClause, ParseError> {
    let mut items = Vec::new();
    
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::set_item {
            items.push(parse_set_item(inner)?);
        }
    }
    
    Ok(SetClause { items })
}

fn parse_set_item(pair: Pair<Rule>) -> Result<SetItem, ParseError> {
    let mut inner = pair.into_inner();
    
    let prop_access = inner.next().unwrap();
    let target = parse_property_ref(prop_access)?;
    
    let value_expr = inner.next().unwrap();
    let value = parse_expression(value_expr)?;
    
    Ok(SetItem { target, value })
}

fn parse_property_ref(pair: Pair<Rule>) -> Result<PropertyRef, ParseError> {
    let mut inner = pair.into_inner();
    let variable = inner.next().unwrap().as_str().to_string();
    let property = inner.next().unwrap().as_str().to_string();
    
    Ok(PropertyRef { variable, property })
}

fn parse_remove_clause(pair: Pair<Rule>) -> Result<RemoveClause, ParseError> {
    let mut properties = Vec::new();
    
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::remove_item {
            let prop = inner.into_inner().next().unwrap();
            properties.push(parse_property_ref(prop)?);
        }
    }
    
    Ok(RemoveClause { properties })
}
```

**Tasks:**
- [ ] Implement `parse_set_clause()`
- [ ] Implement `parse_set_item()`
- [ ] Implement `parse_property_ref()`
- [ ] Implement `parse_remove_clause()`
- [ ] Write unit tests for SET and REMOVE parsing

### 3.4 Parse DELETE Clauses

**File:** `src/gql/parser.rs`

```rust
fn parse_delete_clause(pair: Pair<Rule>) -> Result<DeleteClause, ParseError> {
    let mut variables = Vec::new();
    
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::variable {
            variables.push(inner.as_str().to_string());
        }
    }
    
    Ok(DeleteClause { variables })
}

fn parse_detach_delete_clause(pair: Pair<Rule>) -> Result<DetachDeleteClause, ParseError> {
    let mut variables = Vec::new();
    
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::variable {
            variables.push(inner.as_str().to_string());
        }
    }
    
    Ok(DetachDeleteClause { variables })
}
```

**Tasks:**
- [ ] Implement `parse_delete_clause()`
- [ ] Implement `parse_detach_delete_clause()`
- [ ] Write unit tests for DELETE parsing

### 3.5 Parse MERGE Clause

**File:** `src/gql/parser.rs`

```rust
fn parse_merge_statement(pair: Pair<Rule>) -> Result<Statement, ParseError> {
    let mut merge_clause = None;
    let mut on_create = None;
    let mut on_match = None;
    let mut return_clause = None;
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::merge_clause => {
                let pattern = inner.into_inner()
                    .find(|p| p.as_rule() == Rule::pattern)
                    .map(|p| parse_pattern(p))
                    .transpose()?
                    .unwrap();
                merge_clause = Some(pattern);
            }
            Rule::merge_action => {
                let action = inner.into_inner().next().unwrap();
                match action.as_rule() {
                    Rule::on_create_action => {
                        on_create = Some(parse_set_items(action)?);
                    }
                    Rule::on_match_action => {
                        on_match = Some(parse_set_items(action)?);
                    }
                    _ => {}
                }
            }
            Rule::return_clause => {
                return_clause = Some(parse_return_clause(inner)?);
            }
            _ => {}
        }
    }
    
    let pattern = merge_clause.ok_or_else(|| ParseError::MissingPattern)?;
    
    Ok(Statement::Mutation(Box::new(MutationQuery {
        match_clause: None,
        optional_match_clauses: vec![],
        where_clause: None,
        mutations: vec![MutationClause::Merge(MergeClause {
            pattern,
            on_create,
            on_match,
        })],
        return_clause,
    })))
}

fn parse_set_items(pair: Pair<Rule>) -> Result<Vec<SetItem>, ParseError> {
    let mut items = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::set_item {
            items.push(parse_set_item(inner)?);
        }
    }
    Ok(items)
}
```

**Tasks:**
- [ ] Implement `parse_merge_statement()`
- [ ] Implement `parse_set_items()` helper
- [ ] Handle ON CREATE and ON MATCH actions
- [ ] Write unit tests for MERGE parsing

### 3.6 Parse Combined MATCH + Mutation

**File:** `src/gql/parser.rs`

```rust
fn parse_match_mutation(pair: Pair<Rule>) -> Result<Statement, ParseError> {
    let mut match_clause = None;
    let mut optional_match_clauses = Vec::new();
    let mut where_clause = None;
    let mut mutations = Vec::new();
    let mut return_clause = None;
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::match_clause => {
                match_clause = Some(parse_match_clause(inner)?);
            }
            Rule::optional_match_clause => {
                optional_match_clauses.push(parse_optional_match_clause(inner)?);
            }
            Rule::where_clause => {
                where_clause = Some(parse_where_clause(inner)?);
            }
            Rule::mutation_clause => {
                mutations.push(parse_mutation_clause(inner)?);
            }
            Rule::return_clause => {
                return_clause = Some(parse_return_clause(inner)?);
            }
            _ => {}
        }
    }
    
    Ok(Statement::Mutation(Box::new(MutationQuery {
        match_clause,
        optional_match_clauses,
        where_clause,
        mutations,
        return_clause,
    })))
}

fn parse_mutation_clause(pair: Pair<Rule>) -> Result<MutationClause, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::create_clause => Ok(MutationClause::Create(parse_create_clause(inner)?)),
        Rule::set_clause => Ok(MutationClause::Set(parse_set_clause(inner)?)),
        Rule::remove_clause => Ok(MutationClause::Remove(parse_remove_clause(inner)?)),
        Rule::delete_clause => Ok(MutationClause::Delete(parse_delete_clause(inner)?)),
        Rule::detach_delete_clause => Ok(MutationClause::DetachDelete(parse_detach_delete_clause(inner)?)),
        _ => unreachable!(),
    }
}
```

**Tasks:**
- [ ] Implement `parse_match_mutation()`
- [ ] Implement `parse_mutation_clause()` dispatcher
- [ ] Ensure all existing parsing functions are reused
- [ ] Write integration tests for combined queries

---

## Phase 4: Compiler Implementation - CREATE (Week 3, Days 1-2)

### 4.1 Compile CREATE Clause

**File:** `src/gql/compiler.rs`

Add mutation compilation support:

```rust
impl<S: GraphStorage + GraphStorageMut> Compiler<S> {
    /// Compile a mutation query
    pub fn compile_mutation(
        &self,
        query: &MutationQuery,
    ) -> Result<MutationPlan<S>, CompileError> {
        let mut plan = MutationPlan::new(self.graph);
        
        // If there's a MATCH clause, compile it first
        if let Some(ref match_clause) = query.match_clause {
            plan.set_match(self.compile_match(match_clause)?);
        }
        
        // Apply WHERE filter
        if let Some(ref where_clause) = query.where_clause {
            plan.set_filter(self.compile_where(where_clause)?);
        }
        
        // Compile each mutation clause
        for mutation in &query.mutations {
            match mutation {
                MutationClause::Create(create) => {
                    plan.add_step(self.compile_create(create)?);
                }
                MutationClause::Set(set) => {
                    plan.add_step(self.compile_set(set)?);
                }
                MutationClause::Remove(remove) => {
                    plan.add_step(self.compile_remove(remove)?);
                }
                MutationClause::Delete(delete) => {
                    plan.add_step(self.compile_delete(delete)?);
                }
                MutationClause::DetachDelete(detach) => {
                    plan.add_step(self.compile_detach_delete(detach)?);
                }
                MutationClause::Merge(merge) => {
                    plan.add_step(self.compile_merge(merge)?);
                }
            }
        }
        
        // Compile RETURN if present
        if let Some(ref return_clause) = query.return_clause {
            plan.set_return(self.compile_return(return_clause)?);
        }
        
        Ok(plan)
    }
}
```

**Tasks:**
- [ ] Add `compile_mutation()` method
- [ ] Create `MutationPlan` struct to hold compiled steps
- [ ] Handle optional MATCH clause
- [ ] Dispatch to clause-specific compilation

### 4.2 Compile CREATE Patterns

**File:** `src/gql/compiler.rs`

```rust
fn compile_create(&self, create: &CreateClause) -> Result<CreateStep<S>, CompileError> {
    let mut operations = Vec::new();
    
    for pattern in &create.patterns {
        operations.extend(self.compile_pattern_for_create(pattern)?);
    }
    
    Ok(CreateStep { operations })
}

fn compile_pattern_for_create(
    &self,
    pattern: &Pattern,
) -> Result<Vec<CreateOperation>, CompileError> {
    let mut operations = Vec::new();
    let mut prev_var: Option<String> = None;
    
    for element in &pattern.elements {
        match element {
            PatternElement::Node(node) => {
                // Create add_v operation
                let label = node.labels.first()
                    .ok_or(CompileError::MissingLabel)?
                    .clone();
                    
                let props: HashMap<String, Value> = node.properties
                    .iter()
                    .map(|(k, v)| (k.clone(), self.literal_to_value(v)))
                    .collect();
                
                operations.push(CreateOperation::AddVertex {
                    variable: node.variable.clone(),
                    label,
                    properties: props,
                });
                
                prev_var = node.variable.clone();
            }
            PatternElement::Edge(edge) => {
                // Edge must be between two nodes
                let from_var = prev_var.clone()
                    .ok_or(CompileError::EdgeWithoutSource)?;
                
                // Next element should be a node
                // Store edge info for when we process the next node
                operations.push(CreateOperation::AddEdge {
                    variable: edge.variable.clone(),
                    label: edge.labels.first().cloned()
                        .ok_or(CompileError::MissingEdgeLabel)?,
                    from_variable: from_var,
                    to_variable: None, // Will be filled by next node
                    direction: edge.direction.clone(),
                    properties: edge.properties.iter()
                        .map(|(k, v)| (k.clone(), self.literal_to_value(v)))
                        .collect(),
                });
            }
        }
    }
    
    // Link edges to their target nodes
    self.link_edge_targets(&mut operations)?;
    
    Ok(operations)
}
```

**Tasks:**
- [ ] Implement `compile_create()`
- [ ] Implement `compile_pattern_for_create()`
- [ ] Handle vertex creation with labels and properties
- [ ] Handle edge creation between vertices
- [ ] Link edges to their target nodes
- [ ] Write unit tests

---

## Phase 5: Compiler Implementation - SET & REMOVE (Week 3, Days 3-4)

### 5.1 Compile SET Clause

**File:** `src/gql/compiler.rs`

```rust
fn compile_set(&self, set: &SetClause) -> Result<SetStep<S>, CompileError> {
    let mut assignments = Vec::new();
    
    for item in &set.items {
        let variable = item.target.variable.clone();
        let property = item.target.property.clone();
        let value_expr = self.compile_expression(&item.value)?;
        
        assignments.push(PropertyAssignment {
            variable,
            property,
            value: value_expr,
        });
    }
    
    Ok(SetStep { assignments })
}

/// Compiled SET step
struct SetStep<S> {
    assignments: Vec<PropertyAssignment>,
}

struct PropertyAssignment {
    variable: String,
    property: String,
    value: CompiledExpression,
}

impl<S: GraphStorageMut> SetStep<S> {
    fn execute(&self, context: &mut MutationContext<S>) -> Result<(), MutationError> {
        for assignment in &self.assignments {
            let element = context.get_element(&assignment.variable)?;
            let value = assignment.value.evaluate(context)?;
            
            match element {
                Element::Vertex(id) => {
                    context.storage.set_vertex_property(id, &assignment.property, value)?;
                }
                Element::Edge(id) => {
                    context.storage.set_edge_property(id, &assignment.property, value)?;
                }
            }
        }
        Ok(())
    }
}
```

**Tasks:**
- [ ] Implement `compile_set()`
- [ ] Create `SetStep` struct
- [ ] Create `PropertyAssignment` struct
- [ ] Implement execution for vertices and edges
- [ ] Write unit tests

### 5.2 Compile REMOVE Clause

**File:** `src/gql/compiler.rs`

```rust
fn compile_remove(&self, remove: &RemoveClause) -> Result<RemoveStep<S>, CompileError> {
    let mut removals = Vec::new();
    
    for prop in &remove.properties {
        removals.push(PropertyRemoval {
            variable: prop.variable.clone(),
            property: prop.property.clone(),
        });
    }
    
    Ok(RemoveStep { removals })
}

/// Compiled REMOVE step
struct RemoveStep<S> {
    removals: Vec<PropertyRemoval>,
}

struct PropertyRemoval {
    variable: String,
    property: String,
}

impl<S: GraphStorageMut> RemoveStep<S> {
    fn execute(&self, context: &mut MutationContext<S>) -> Result<(), MutationError> {
        for removal in &self.removals {
            let element = context.get_element(&removal.variable)?;
            
            match element {
                Element::Vertex(id) => {
                    context.storage.remove_vertex_property(id, &removal.property)?;
                }
                Element::Edge(id) => {
                    context.storage.remove_edge_property(id, &removal.property)?;
                }
            }
        }
        Ok(())
    }
}
```

**Tasks:**
- [ ] Implement `compile_remove()`
- [ ] Create `RemoveStep` struct
- [ ] Add `remove_vertex_property` and `remove_edge_property` to storage trait if needed
- [ ] Write unit tests

---

## Phase 6: Compiler Implementation - DELETE (Week 3, Day 5)

### 6.1 Compile DELETE Clause

**File:** `src/gql/compiler.rs`

```rust
fn compile_delete(&self, delete: &DeleteClause) -> Result<DeleteStep<S>, CompileError> {
    Ok(DeleteStep {
        variables: delete.variables.clone(),
        detach: false,
    })
}

fn compile_detach_delete(&self, delete: &DetachDeleteClause) -> Result<DeleteStep<S>, CompileError> {
    Ok(DeleteStep {
        variables: delete.variables.clone(),
        detach: true,
    })
}

/// Compiled DELETE step
struct DeleteStep<S> {
    variables: Vec<String>,
    detach: bool,
}

impl<S: GraphStorageMut> DeleteStep<S> {
    fn execute(&self, context: &mut MutationContext<S>) -> Result<(), MutationError> {
        for var in &self.variables {
            let element = context.get_element(var)?;
            
            match element {
                Element::Vertex(id) => {
                    if self.detach {
                        // Remove all connected edges first
                        context.storage.remove_vertex(id)?;
                    } else {
                        // Check for existing edges
                        if context.storage.vertex_has_edges(id)? {
                            return Err(MutationError::VertexHasEdges(id));
                        }
                        context.storage.remove_vertex(id)?;
                    }
                }
                Element::Edge(id) => {
                    context.storage.remove_edge(id)?;
                }
            }
        }
        Ok(())
    }
}
```

**Tasks:**
- [ ] Implement `compile_delete()`
- [ ] Implement `compile_detach_delete()`
- [ ] Create `DeleteStep` struct with `detach` flag
- [ ] Implement edge check for non-detach DELETE
- [ ] Write unit tests

---

## Phase 7: Compiler Implementation - MERGE (Week 4, Days 1-3)

### 7.1 Compile MERGE Clause

**File:** `src/gql/compiler.rs`

```rust
fn compile_merge(&self, merge: &MergeClause) -> Result<MergeStep<S>, CompileError> {
    // Compile the match pattern
    let match_pattern = self.compile_pattern_for_match(&merge.pattern)?;
    
    // Compile the create pattern
    let create_pattern = self.compile_pattern_for_create(&merge.pattern)?;
    
    // Compile ON CREATE actions
    let on_create = merge.on_create.as_ref()
        .map(|items| self.compile_set_items(items))
        .transpose()?;
    
    // Compile ON MATCH actions
    let on_match = merge.on_match.as_ref()
        .map(|items| self.compile_set_items(items))
        .transpose()?;
    
    Ok(MergeStep {
        match_pattern,
        create_pattern,
        on_create,
        on_match,
    })
}

/// Compiled MERGE step
struct MergeStep<S> {
    match_pattern: CompiledPattern,
    create_pattern: Vec<CreateOperation>,
    on_create: Option<Vec<PropertyAssignment>>,
    on_match: Option<Vec<PropertyAssignment>>,
}

impl<S: GraphStorageMut> MergeStep<S> {
    fn execute(&self, context: &mut MutationContext<S>) -> Result<(), MutationError> {
        // Try to match the pattern
        let matches = self.match_pattern.execute(context)?;
        
        if matches.is_empty() {
            // Create new elements
            for op in &self.create_pattern {
                op.execute(context)?;
            }
            
            // Apply ON CREATE actions
            if let Some(ref actions) = self.on_create {
                for action in actions {
                    action.execute(context)?;
                }
            }
        } else {
            // Bind matched elements to context
            context.bind_matches(matches)?;
            
            // Apply ON MATCH actions
            if let Some(ref actions) = self.on_match {
                for action in actions {
                    action.execute(context)?;
                }
            }
        }
        
        Ok(())
    }
}
```

**Tasks:**
- [ ] Implement `compile_merge()`
- [ ] Create `MergeStep` struct
- [ ] Implement match-or-create logic
- [ ] Implement ON CREATE action execution
- [ ] Implement ON MATCH action execution
- [ ] Write unit tests

---

## Phase 8: Execution Framework (Week 4, Days 4-5)

### 8.1 MutationContext

**File:** `src/gql/compiler.rs`

```rust
/// Context for executing mutations
pub struct MutationContext<'g, S: GraphStorage> {
    pub storage: &'g S,
    /// Bound variables from MATCH or CREATE
    variables: HashMap<String, Element>,
    /// Newly created vertices (variable -> id)
    created_vertices: HashMap<String, VertexId>,
    /// Newly created edges (variable -> id)
    created_edges: HashMap<String, EdgeId>,
}

impl<'g, S: GraphStorageMut> MutationContext<'g, S> {
    pub fn new(storage: &'g S) -> Self {
        Self {
            storage,
            variables: HashMap::new(),
            created_vertices: HashMap::new(),
            created_edges: HashMap::new(),
        }
    }
    
    pub fn bind(&mut self, variable: &str, element: Element) {
        self.variables.insert(variable.to_string(), element);
    }
    
    pub fn get_element(&self, variable: &str) -> Result<Element, MutationError> {
        self.variables.get(variable)
            .cloned()
            .or_else(|| self.created_vertices.get(variable).map(|id| Element::Vertex(*id)))
            .or_else(|| self.created_edges.get(variable).map(|id| Element::Edge(*id)))
            .ok_or_else(|| MutationError::UnboundVariable(variable.to_string()))
    }
}

#[derive(Debug, Clone)]
pub enum Element {
    Vertex(VertexId),
    Edge(EdgeId),
}
```

**Tasks:**
- [ ] Create `MutationContext` struct
- [ ] Implement variable binding
- [ ] Track created elements
- [ ] Implement element lookup

### 8.2 MutationPlan Execution

**File:** `src/gql/compiler.rs`

```rust
/// A compiled mutation query plan
pub struct MutationPlan<'g, S: GraphStorage> {
    graph: &'g S,
    match_step: Option<CompiledMatch>,
    filter: Option<CompiledFilter>,
    mutations: Vec<Box<dyn MutationStep<S>>>,
    return_step: Option<CompiledReturn>,
}

impl<'g, S: GraphStorageMut> MutationPlan<'g, S> {
    /// Execute the mutation plan
    pub fn execute(&self) -> Result<Vec<HashMap<String, Value>>, MutationError> {
        let mut context = MutationContext::new(self.graph);
        
        // Execute MATCH if present
        if let Some(ref match_step) = self.match_step {
            let matches = match_step.execute(self.graph)?;
            
            // Apply filter
            let matches = if let Some(ref filter) = self.filter {
                matches.into_iter()
                    .filter(|m| filter.evaluate(m))
                    .collect()
            } else {
                matches
            };
            
            // For each match, execute mutations
            for match_result in matches {
                context.bind_all(&match_result);
                
                for mutation in &self.mutations {
                    mutation.execute(&mut context)?;
                }
            }
        } else {
            // No MATCH - just execute mutations
            for mutation in &self.mutations {
                mutation.execute(&mut context)?;
            }
        }
        
        // Execute RETURN if present
        if let Some(ref return_step) = self.return_step {
            return_step.execute(&context)
        } else {
            Ok(vec![])
        }
    }
}
```

**Tasks:**
- [ ] Create `MutationPlan` struct
- [ ] Implement `execute()` method
- [ ] Handle MATCH iteration
- [ ] Handle mutations without MATCH
- [ ] Implement RETURN evaluation

---

## Phase 9: Integration and Testing (Week 5)

### 9.1 Parser Snapshot Tests

**File:** `tests/gql_snapshots.rs`

Add snapshot tests for mutation parsing:

```rust
#[test]
fn parse_create_vertex_snapshot() {
    let query = "CREATE (n:Person {name: 'Alice', age: 30})";
    let result = parse(query).unwrap();
    insta::assert_debug_snapshot!(result);
}

#[test]
fn parse_create_edge_snapshot() {
    let query = "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)";
    let result = parse(query).unwrap();
    insta::assert_debug_snapshot!(result);
}

#[test]
fn parse_set_snapshot() {
    let query = "MATCH (n:Person {name: 'Alice'}) SET n.age = 31 RETURN n";
    let result = parse(query).unwrap();
    insta::assert_debug_snapshot!(result);
}

#[test]
fn parse_delete_snapshot() {
    let query = "MATCH (n:Person {status: 'inactive'}) DELETE n";
    let result = parse(query).unwrap();
    insta::assert_debug_snapshot!(result);
}

#[test]
fn parse_detach_delete_snapshot() {
    let query = "MATCH (n:Person {name: 'Alice'}) DETACH DELETE n";
    let result = parse(query).unwrap();
    insta::assert_debug_snapshot!(result);
}

#[test]
fn parse_merge_snapshot() {
    let query = "MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = 123 ON MATCH SET n.updated = 456";
    let result = parse(query).unwrap();
    insta::assert_debug_snapshot!(result);
}
```

**Tasks:**
- [ ] Add CREATE parsing snapshots
- [ ] Add SET parsing snapshots
- [ ] Add REMOVE parsing snapshots
- [ ] Add DELETE parsing snapshots
- [ ] Add MERGE parsing snapshots
- [ ] Add combined query snapshots

### 9.2 Integration Tests

**File:** `tests/gql_mutations.rs`

```rust
use interstellar::gql::{parse, compile, execute};
use interstellar::Graph;

#[test]
fn test_create_vertex() {
    let graph = Graph::new();
    
    execute(&graph, "CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    
    // Verify vertex exists
    let results = execute(&graph, "MATCH (n:Person {name: 'Alice'}) RETURN n.age").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["n.age"], Value::Int(30));
}

#[test]
fn test_create_edge() {
    let graph = Graph::new();
    
    execute(&graph, "CREATE (a:Person {name: 'Alice'})").unwrap();
    execute(&graph, "CREATE (b:Person {name: 'Bob'})").unwrap();
    execute(&graph, "
        MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
        CREATE (a)-[:KNOWS {since: 2020}]->(b)
    ").unwrap();
    
    // Verify edge exists
    let results = execute(&graph, "
        MATCH (a:Person)-[r:KNOWS]->(b:Person)
        RETURN a.name, b.name, r.since
    ").unwrap();
    
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["a.name"], Value::String("Alice".into()));
    assert_eq!(results[0]["b.name"], Value::String("Bob".into()));
}

#[test]
fn test_set_property() {
    let graph = Graph::new();
    
    execute(&graph, "CREATE (n:Person {name: 'Alice', age: 30})").unwrap();
    execute(&graph, "MATCH (n:Person {name: 'Alice'}) SET n.age = 31").unwrap();
    
    let results = execute(&graph, "MATCH (n:Person {name: 'Alice'}) RETURN n.age").unwrap();
    assert_eq!(results[0]["n.age"], Value::Int(31));
}

#[test]
fn test_delete_fails_with_edges() {
    let graph = Graph::new();
    
    execute(&graph, "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();
    
    // DELETE should fail
    let result = execute(&graph, "MATCH (n:Person {name: 'Alice'}) DELETE n");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), MutationError::VertexHasEdges(_)));
}

#[test]
fn test_detach_delete_succeeds() {
    let graph = Graph::new();
    
    execute(&graph, "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})").unwrap();
    execute(&graph, "MATCH (n:Person {name: 'Alice'}) DETACH DELETE n").unwrap();
    
    // Verify Alice is gone
    let results = execute(&graph, "MATCH (n:Person {name: 'Alice'}) RETURN n").unwrap();
    assert!(results.is_empty());
    
    // Verify Bob still exists
    let results = execute(&graph, "MATCH (n:Person {name: 'Bob'}) RETURN n").unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn test_merge_creates_when_not_exists() {
    let graph = Graph::new();
    
    execute(&graph, "MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = 123").unwrap();
    
    let results = execute(&graph, "MATCH (n:Person {name: 'Alice'}) RETURN n.created").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["n.created"], Value::Int(123));
}

#[test]
fn test_merge_matches_when_exists() {
    let graph = Graph::new();
    
    execute(&graph, "CREATE (n:Person {name: 'Alice', created: 100})").unwrap();
    execute(&graph, "MERGE (n:Person {name: 'Alice'}) ON MATCH SET n.updated = 200").unwrap();
    
    let results = execute(&graph, "MATCH (n:Person {name: 'Alice'}) RETURN n.created, n.updated").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["n.created"], Value::Int(100));
    assert_eq!(results[0]["n.updated"], Value::Int(200));
}
```

**Tasks:**
- [ ] Create `tests/gql_mutations.rs`
- [ ] Test CREATE vertex
- [ ] Test CREATE edge
- [ ] Test SET property
- [ ] Test REMOVE property
- [ ] Test DELETE with edge check
- [ ] Test DETACH DELETE
- [ ] Test MERGE create path
- [ ] Test MERGE match path

### 9.3 Error Case Tests

**File:** `tests/gql_mutations.rs`

```rust
#[test]
fn test_set_without_match_fails() {
    let graph = Graph::new();
    // SET requires MATCH - this should be a parse error
    let result = parse("SET n.age = 31");
    assert!(result.is_err());
}

#[test]
fn test_delete_unbound_variable_fails() {
    let graph = Graph::new();
    execute(&graph, "CREATE (n:Person {name: 'Alice'})").unwrap();
    
    let result = execute(&graph, "MATCH (n:Person) DELETE m");
    assert!(matches!(result.unwrap_err(), MutationError::UnboundVariable(_)));
}

#[test]
fn test_create_edge_to_nonexistent_fails() {
    let graph = Graph::new();
    execute(&graph, "CREATE (a:Person {name: 'Alice'})").unwrap();
    
    // b doesn't exist
    let result = execute(&graph, "
        MATCH (a:Person {name: 'Alice'})
        CREATE (a)-[:KNOWS]->(b:Person {name: 'Bob'})
    ");
    // This should work - CREATE creates Bob
    assert!(result.is_ok());
}
```

**Tasks:**
- [ ] Test SET without MATCH
- [ ] Test DELETE unbound variable
- [ ] Test REMOVE unbound variable
- [ ] Test CREATE edge with inline node creation

---

## Phase 10: Documentation (Week 5, Days 4-5)

### 10.1 Update API Documentation

**Tasks:**
- [ ] Add rustdoc to all new AST types
- [ ] Add rustdoc to all compiler functions
- [ ] Add examples to key functions
- [ ] Update module-level documentation

### 10.2 Update Gremlin_api.md

**Tasks:**
- [ ] Add GQL Mutations section
- [ ] Document CREATE syntax and examples
- [ ] Document SET syntax and examples
- [ ] Document DELETE/DETACH DELETE syntax
- [ ] Document MERGE syntax and examples
- [ ] Show equivalence to Gremlin mutation steps

### 10.3 Create Example

**File:** `examples/gql_mutations.rs`

**Tasks:**
- [ ] Create comprehensive GQL mutations example
- [ ] Demonstrate CREATE, SET, DELETE, MERGE
- [ ] Show error handling
- [ ] Show combined read/write queries

---

## Testing Checklist

### Parser Tests
- [ ] CREATE single vertex parses
- [ ] CREATE vertex with properties parses
- [ ] CREATE multiple vertices parses
- [ ] CREATE edge pattern parses
- [ ] SET single property parses
- [ ] SET multiple properties parses
- [ ] REMOVE single property parses
- [ ] DELETE single variable parses
- [ ] DELETE multiple variables parses
- [ ] DETACH DELETE parses
- [ ] MERGE without actions parses
- [ ] MERGE with ON CREATE parses
- [ ] MERGE with ON MATCH parses
- [ ] MERGE with both actions parses
- [ ] MATCH + CREATE parses
- [ ] MATCH + SET + RETURN parses
- [ ] Multiple mutation clauses parse

### Compiler Tests
- [ ] CREATE vertex compiles to add_v
- [ ] CREATE edge compiles to add_e
- [ ] SET compiles to property step
- [ ] DELETE compiles to drop step
- [ ] DETACH DELETE compiles to drop step
- [ ] MERGE compiles to match-or-create logic

### Integration Tests
- [ ] End-to-end CREATE vertex
- [ ] End-to-end CREATE edge
- [ ] End-to-end SET property
- [ ] End-to-end REMOVE property
- [ ] End-to-end DELETE edge
- [ ] End-to-end DETACH DELETE vertex
- [ ] End-to-end MERGE create path
- [ ] End-to-end MERGE match path

### Error Cases
- [ ] DELETE vertex with edges fails
- [ ] SET on unbound variable fails
- [ ] DELETE on unbound variable fails

---

## Dependencies

- Existing GQL parser (`src/gql/parser.rs`)
- Existing GQL compiler (`src/gql/compiler.rs`)
- Existing mutation steps (`src/traversal/mutation.rs`)
- `GraphStorageMut` trait (`src/storage/mod.rs`)

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Grammar conflicts with existing rules | High | Test incrementally, use grammar testing |
| Parser complexity increases | Medium | Keep parsing functions modular |
| MERGE logic complexity | Medium | Implement as separate, testable function |
| Variable binding across clauses | Medium | Clear context management with MutationContext |

---

## Success Criteria

1. All mutation clauses (CREATE, SET, REMOVE, DELETE, DETACH DELETE, MERGE) parse correctly
2. Mutations compile to existing traversal mutation steps
3. All tests pass with >90% branch coverage on new code
4. Backward compatibility with existing read-only queries maintained
5. Documentation complete with examples
6. Error messages are clear and helpful

---

## Future Work (Out of Scope)

- Label mutations (`SET n:Label`, `REMOVE n:Label`)
- Map property assignment (`SET n += {key: value}`)
- FOREACH clause for iterative mutations
- Schema constraints and indexes
- CALL procedures
- Subquery mutations
