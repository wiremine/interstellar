# Plan 09: GQL Parser and Runtime Implementation

**Phase 2 Feature: Graph Query Language Support**

Based on: `guiding-documents/gql.md` and `guiding-documents/gql-to-ir-pipeline.md`

---

## Overview

This plan implements GQL (Graph Query Language) support for RustGremlin. The implementation follows a **spike-first approach** where Phase 1 delivers a minimal end-to-end working query before building out the full feature set.

**Total Duration**: 4-5 weeks  
**Dependencies**: Traversal engine (Plan 03) must be complete

---

## Architecture Summary

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           GQL Source Text                               │
│              MATCH (p:Person {name: 'Alice'}) RETURN p.name             │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         pest Parser (grammar.pest)                      │
│  - Tokenization (implicit)                                              │
│  - Parse tree generation                                                │
│  - Syntax error detection                                               │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              Typed AST                                  │
│  - Query, MatchClause, Pattern, Expression                              │
│  - Source spans for error reporting                                     │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Compiler (compiler.rs)                          │
│  - AST → Traversal API calls                                            │
│  - Variable binding via as_() / select()                                │
│  - Pattern → navigation steps                                           │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        BoundTraversal Execution                         │
│  g.v().has_label("Person").has_value("name", "Alice").values("name")    │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## File Structure

```
src/
├── gql/
│   ├── mod.rs              # Public API: parse(), compile(), execute()
│   ├── grammar.pest        # pest grammar definition
│   ├── ast.rs              # AST types (Query, Pattern, Expression, etc.)
│   ├── parser.rs           # pest Pairs → AST conversion
│   ├── compiler.rs         # AST → BoundTraversal compilation
│   └── error.rs            # GqlError, ParseError, CompileError
├── lib.rs                  # Add `pub mod gql;`
```

---

## Dependencies

Add to `Cargo.toml`:

```toml
[dependencies]
pest = "2.7"
pest_derive = "2.7"

[dev-dependencies]
insta = { version = "1.34", features = ["yaml"] }  # Snapshot testing
```

---

## Implementation Phases

### Week 1: End-to-End Spike

The spike proves the entire pipeline works before investing in full grammar coverage.

---

#### Phase 1.1: Project Setup and Dependencies
**Duration**: 30 minutes

**Tasks**:
1. Add `pest` and `pest_derive` to `Cargo.toml`
2. Create `src/gql/mod.rs` with module structure
3. Add `pub mod gql;` to `src/lib.rs`

**Code Changes**:

```toml
# Cargo.toml
[dependencies]
pest = "2.7"
pest_derive = "2.7"
```

```rust
// src/gql/mod.rs
mod ast;
mod compiler;
mod error;
mod parser;

pub use ast::*;
pub use compiler::compile;
pub use error::{CompileError, GqlError, ParseError};
pub use parser::parse;
```

**Acceptance Criteria**:
- [ ] `cargo build` succeeds with new dependencies
- [ ] `src/gql/mod.rs` exists and compiles

---

#### Phase 1.2: Minimal Grammar (Spike)
**File**: `src/gql/grammar.pest`  
**Duration**: 1-2 hours

**Tasks**:
1. Create minimal grammar supporting only: `MATCH (n:Label) RETURN n`
2. Define rules for: query, match_clause, return_clause, node_pattern, variable, label_filter

**Code**:

```pest
// src/gql/grammar.pest - Minimal spike grammar

WHITESPACE = _{ " " | "\t" | "\r" | "\n" }

// Keywords (case-insensitive)
MATCH  = { ^"match" }
RETURN = { ^"return" }

// Entry point - minimal query
query = { SOI ~ match_clause ~ return_clause ~ EOI }

// MATCH clause - single pattern only
match_clause = { MATCH ~ pattern }

pattern = { node_pattern }

node_pattern = { "(" ~ variable? ~ label_filter? ~ ")" }

variable = @{ ASCII_ALPHA ~ (ASCII_ALPHANUMERIC | "_")* }

label_filter = { ":" ~ identifier }

identifier = @{ ASCII_ALPHA ~ (ASCII_ALPHANUMERIC | "_")* }

// RETURN clause - single variable only
return_clause = { RETURN ~ variable }
```

**Acceptance Criteria**:
- [ ] Grammar file parses without pest errors
- [ ] `MATCH (n:Person) RETURN n` parses successfully
- [ ] `MATCH (n) RETURN n` parses successfully (no label)
- [ ] `MATCH (:Person) RETURN n` fails (missing variable in RETURN)

---

#### Phase 1.3: Minimal AST Types (Spike)
**File**: `src/gql/ast.rs`  
**Duration**: 1 hour

**Tasks**:
1. Define minimal AST types for spike query
2. Keep types simple - expand in later phases

**Code**:

```rust
// src/gql/ast.rs

/// Complete GQL query (minimal spike version)
#[derive(Debug, Clone)]
pub struct Query {
    pub match_clause: MatchClause,
    pub where_clause: Option<WhereClause>,
    pub return_clause: ReturnClause,
    pub order_clause: Option<OrderClause>,
    pub limit_clause: Option<LimitClause>,
}

/// MATCH clause with patterns
#[derive(Debug, Clone)]
pub struct MatchClause {
    pub patterns: Vec<Pattern>,
}

/// A pattern is a path through the graph
#[derive(Debug, Clone)]
pub struct Pattern {
    pub elements: Vec<PatternElement>,
}

#[derive(Debug, Clone)]
pub enum PatternElement {
    Node(NodePattern),
    Edge(EdgePattern),
}

/// Node pattern: (variable:Label {prop: value})
#[derive(Debug, Clone)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: Vec<(String, Literal)>,
}

/// Edge pattern: -[variable:TYPE]->
#[derive(Debug, Clone)]
pub struct EdgePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub direction: EdgeDirection,
    pub quantifier: Option<PathQuantifier>,
    pub properties: Vec<(String, Literal)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeDirection {
    Outgoing,  // -->
    Incoming,  // <--
    Both,      // --
}

#[derive(Debug, Clone)]
pub struct PathQuantifier {
    pub min: Option<u32>,
    pub max: Option<u32>,
}

/// WHERE clause (stub for spike)
#[derive(Debug, Clone)]
pub struct WhereClause {
    pub expression: Expression,
}

/// RETURN clause
#[derive(Debug, Clone)]
pub struct ReturnClause {
    pub items: Vec<ReturnItem>,
}

#[derive(Debug, Clone)]
pub struct ReturnItem {
    pub expression: Expression,
    pub alias: Option<String>,
}

/// ORDER BY clause (stub for spike)
#[derive(Debug, Clone)]
pub struct OrderClause {
    pub items: Vec<OrderItem>,
}

#[derive(Debug, Clone)]
pub struct OrderItem {
    pub expression: Expression,
    pub descending: bool,
}

/// LIMIT clause (stub for spike)
#[derive(Debug, Clone)]
pub struct LimitClause {
    pub limit: u64,
    pub offset: Option<u64>,
}

/// Expression types
#[derive(Debug, Clone)]
pub enum Expression {
    /// Variable reference: `n`
    Variable(String),
    
    /// Property access: `n.name`
    Property { variable: String, property: String },
    
    /// Literal value
    Literal(Literal),
    
    /// Binary operation (stub for spike)
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    
    /// Aggregate function (stub for spike)
    Aggregate {
        func: AggregateFunc,
        distinct: bool,
        expr: Box<Expression>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    // Comparison
    Eq, Neq, Lt, Lte, Gt, Gte,
    // Logical
    And, Or,
    // Arithmetic
    Add, Sub, Mul, Div, Mod,
    // String
    Contains, StartsWith, EndsWith,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunc {
    Count, Sum, Avg, Min, Max, Collect,
}

/// Literal values
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

impl From<Literal> for crate::value::Value {
    fn from(lit: Literal) -> Self {
        match lit {
            Literal::Null => crate::value::Value::Null,
            Literal::Bool(b) => crate::value::Value::Bool(b),
            Literal::Int(n) => crate::value::Value::Int(n),
            Literal::Float(f) => crate::value::Value::Float(f),
            Literal::String(s) => crate::value::Value::String(s),
        }
    }
}
```

**Acceptance Criteria**:
- [ ] All AST types compile
- [ ] `Query`, `MatchClause`, `Pattern`, `NodePattern` are defined
- [ ] `Literal` converts to `Value`

---

#### Phase 1.4: Minimal Parser (Spike)
**File**: `src/gql/parser.rs`  
**Duration**: 2 hours

**Tasks**:
1. Create pest parser struct with `#[grammar = "gql/grammar.pest"]`
2. Implement `parse()` function returning `Result<Query, ParseError>`
3. Build AST from pest pairs for minimal grammar

**Code**:

```rust
// src/gql/parser.rs

use pest::Parser;
use pest_derive::Parser;

use crate::gql::ast::*;
use crate::gql::error::ParseError;

#[derive(Parser)]
#[grammar = "gql/grammar.pest"]
struct GqlParser;

/// Parse a GQL query string into an AST.
pub fn parse(input: &str) -> Result<Query, ParseError> {
    let pairs = GqlParser::parse(Rule::query, input)
        .map_err(|e| ParseError::Syntax(e.to_string()))?;
    
    let query_pair = pairs.into_iter().next()
        .ok_or_else(|| ParseError::Empty)?;
    
    build_query(query_pair)
}

fn build_query(pair: pest::iterators::Pair<Rule>) -> Result<Query, ParseError> {
    let mut match_clause = None;
    let mut return_clause = None;
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::match_clause => match_clause = Some(build_match_clause(inner)?),
            Rule::return_clause => return_clause = Some(build_return_clause(inner)?),
            Rule::EOI => {}
            _ => {}
        }
    }
    
    Ok(Query {
        match_clause: match_clause.ok_or(ParseError::MissingClause("MATCH"))?,
        where_clause: None,
        return_clause: return_clause.ok_or(ParseError::MissingClause("RETURN"))?,
        order_clause: None,
        limit_clause: None,
    })
}

fn build_match_clause(pair: pest::iterators::Pair<Rule>) -> Result<MatchClause, ParseError> {
    let mut patterns = Vec::new();
    
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::pattern {
            patterns.push(build_pattern(inner)?);
        }
    }
    
    Ok(MatchClause { patterns })
}

fn build_pattern(pair: pest::iterators::Pair<Rule>) -> Result<Pattern, ParseError> {
    let mut elements = Vec::new();
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::node_pattern => {
                elements.push(PatternElement::Node(build_node_pattern(inner)?));
            }
            _ => {}
        }
    }
    
    Ok(Pattern { elements })
}

fn build_node_pattern(pair: pest::iterators::Pair<Rule>) -> Result<NodePattern, ParseError> {
    let mut variable = None;
    let mut labels = Vec::new();
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::variable => variable = Some(inner.as_str().to_string()),
            Rule::label_filter => {
                for label_inner in inner.into_inner() {
                    if label_inner.as_rule() == Rule::identifier {
                        labels.push(label_inner.as_str().to_string());
                    }
                }
            }
            _ => {}
        }
    }
    
    Ok(NodePattern {
        variable,
        labels,
        properties: Vec::new(),
    })
}

fn build_return_clause(pair: pest::iterators::Pair<Rule>) -> Result<ReturnClause, ParseError> {
    let mut items = Vec::new();
    
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::variable {
            items.push(ReturnItem {
                expression: Expression::Variable(inner.as_str().to_string()),
                alias: None,
            });
        }
    }
    
    Ok(ReturnClause { items })
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_simple_match() {
        let query = parse("MATCH (n:Person) RETURN n").unwrap();
        assert_eq!(query.match_clause.patterns.len(), 1);
        
        let pattern = &query.match_clause.patterns[0];
        assert_eq!(pattern.elements.len(), 1);
        
        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert_eq!(node.variable, Some("n".to_string()));
            assert_eq!(node.labels, vec!["Person".to_string()]);
        } else {
            panic!("Expected node pattern");
        }
        
        assert_eq!(query.return_clause.items.len(), 1);
        if let Expression::Variable(v) = &query.return_clause.items[0].expression {
            assert_eq!(v, "n");
        } else {
            panic!("Expected variable expression");
        }
    }
    
    #[test]
    fn test_parse_no_label() {
        let query = parse("MATCH (n) RETURN n").unwrap();
        if let PatternElement::Node(node) = &query.match_clause.patterns[0].elements[0] {
            assert_eq!(node.variable, Some("n".to_string()));
            assert!(node.labels.is_empty());
        }
    }
}
```

**Acceptance Criteria**:
- [ ] `parse("MATCH (n:Person) RETURN n")` returns valid AST
- [ ] Parser tests pass
- [ ] Invalid syntax returns `ParseError`

---

#### Phase 1.5: Error Types
**File**: `src/gql/error.rs`  
**Duration**: 30 minutes

**Tasks**:
1. Define `GqlError` enum wrapping parse and compile errors
2. Define `ParseError` for syntax errors
3. Define `CompileError` for semantic errors

**Code**:

```rust
// src/gql/error.rs

use thiserror::Error;

/// Top-level GQL error type
#[derive(Debug, Error)]
pub enum GqlError {
    #[error("Parse error: {0}")]
    Parse(#[from] ParseError),
    
    #[error("Compile error: {0}")]
    Compile(#[from] CompileError),
}

/// Errors during parsing
#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Syntax error: {0}")]
    Syntax(String),
    
    #[error("Empty input")]
    Empty,
    
    #[error("Missing {0} clause")]
    MissingClause(&'static str),
    
    #[error("Invalid literal: {0}")]
    InvalidLiteral(String),
}

/// Errors during compilation to traversal
#[derive(Debug, Error)]
pub enum CompileError {
    #[error("Undefined variable: {0}")]
    UndefinedVariable(String),
    
    #[error("Variable already defined: {0}")]
    DuplicateVariable(String),
    
    #[error("Empty pattern")]
    EmptyPattern,
    
    #[error("Pattern must start with a node")]
    PatternMustStartWithNode,
    
    #[error("Unsupported expression in context")]
    UnsupportedExpression,
    
    #[error("Aggregates not allowed in WHERE clause")]
    AggregateInWhere,
}
```

**Acceptance Criteria**:
- [ ] Error types compile
- [ ] Errors implement `std::error::Error`
- [ ] Errors have useful messages

---

#### Phase 1.6: Minimal Compiler (Spike)
**File**: `src/gql/compiler.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Create `compile()` function that takes AST and `GraphSnapshot`
2. Compile `MATCH (n:Label) RETURN n` to traversal
3. Return results as `Vec<Value>`

**Code**:

```rust
// src/gql/compiler.rs

use std::collections::HashMap;

use crate::gql::ast::*;
use crate::gql::error::CompileError;
use crate::graph::GraphSnapshot;
use crate::value::Value;

/// Compile and execute a GQL query against a graph snapshot.
pub fn compile<'g>(
    query: &Query,
    snapshot: &'g GraphSnapshot<'g>,
) -> Result<Vec<Value>, CompileError> {
    let mut compiler = Compiler::new(snapshot);
    compiler.compile(query)
}

struct Compiler<'a, 'g> {
    snapshot: &'a GraphSnapshot<'g>,
    bindings: HashMap<String, BindingInfo>,
}

#[derive(Debug, Clone)]
struct BindingInfo {
    /// Index in the pattern where this variable was bound
    pattern_index: usize,
    /// Whether this is a node or edge binding
    is_node: bool,
}

impl<'a, 'g> Compiler<'a, 'g> {
    fn new(snapshot: &'a GraphSnapshot<'g>) -> Self {
        Self {
            snapshot,
            bindings: HashMap::new(),
        }
    }
    
    fn compile(&mut self, query: &Query) -> Result<Vec<Value>, CompileError> {
        // For spike: support only single pattern with single node
        if query.match_clause.patterns.is_empty() {
            return Err(CompileError::EmptyPattern);
        }
        
        let pattern = &query.match_clause.patterns[0];
        if pattern.elements.is_empty() {
            return Err(CompileError::EmptyPattern);
        }
        
        // Get the first node pattern
        let node = match &pattern.elements[0] {
            PatternElement::Node(n) => n,
            PatternElement::Edge(_) => return Err(CompileError::PatternMustStartWithNode),
        };
        
        // Build traversal
        let g = self.snapshot.traversal();
        let mut traversal = g.v();
        
        // Apply label filter
        if !node.labels.is_empty() {
            let labels: Vec<&str> = node.labels.iter().map(|s| s.as_str()).collect();
            traversal = traversal.has_label_any(&labels);
        }
        
        // Register binding
        if let Some(var) = &node.variable {
            self.bindings.insert(var.clone(), BindingInfo {
                pattern_index: 0,
                is_node: true,
            });
        }
        
        // Execute and collect results based on RETURN clause
        let results = self.execute_return(&query.return_clause, traversal)?;
        
        Ok(results)
    }
    
    fn execute_return<In, Out>(
        &self,
        return_clause: &ReturnClause,
        traversal: crate::traversal::BoundTraversal<'g, In, Out>,
    ) -> Result<Vec<Value>, CompileError>
    where
        Out: Clone + 'static,
    {
        // For spike: support only returning the matched node
        // This returns the vertex itself as a Value::Vertex
        
        // Verify all referenced variables are bound
        for item in &return_clause.items {
            if let Expression::Variable(var) = &item.expression {
                if !self.bindings.contains_key(var) {
                    return Err(CompileError::UndefinedVariable(var.clone()));
                }
            }
        }
        
        // Collect results - the traversal yields Value::Vertex for each match
        let results: Vec<Value> = traversal.to_list();
        
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gql::parser::parse;
    use crate::Graph;
    
    #[test]
    fn test_compile_simple_match() {
        let graph = Graph::new();
        
        // Add test data
        {
            let mut writer = graph.write();
            writer.add_vertex("Person", [("name", "Alice")]);
            writer.add_vertex("Person", [("name", "Bob")]);
            writer.add_vertex("Company", [("name", "Acme")]);
            writer.commit();
        }
        
        let snapshot = graph.read();
        let query = parse("MATCH (n:Person) RETURN n").unwrap();
        let results = compile(&query, &snapshot).unwrap();
        
        // Should find 2 Person vertices
        assert_eq!(results.len(), 2);
    }
}
```

**Acceptance Criteria**:
- [ ] `compile()` executes simple MATCH query
- [ ] Returns `Vec<Value>` with matched vertices
- [ ] Undefined variable in RETURN produces error
- [ ] Integration test passes

---

#### Phase 1.7: Public API and Integration Test
**File**: `src/gql/mod.rs`, `tests/gql.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Add `gql()` method to `GraphSnapshot`
2. Create integration test proving end-to-end spike works
3. Add documentation

**Code**:

```rust
// Add to src/graph.rs

impl<'g> GraphSnapshot<'g> {
    /// Execute a GQL query against this snapshot.
    ///
    /// # Example
    /// ```
    /// let graph = Graph::new();
    /// // ... add data ...
    /// let snapshot = graph.read();
    /// let results = snapshot.gql("MATCH (n:Person) RETURN n")?;
    /// ```
    pub fn gql(&self, query: &str) -> Result<Vec<crate::value::Value>, crate::gql::GqlError> {
        let ast = crate::gql::parse(query)?;
        let results = crate::gql::compile(&ast, self)?;
        Ok(results)
    }
}
```

```rust
// tests/gql.rs

use rustgremlin::Graph;

#[test]
fn test_gql_spike_end_to_end() {
    let graph = Graph::new();
    
    // Setup test data
    {
        let mut writer = graph.write();
        writer.add_vertex("Person", [("name", "Alice"), ("age", 30)]);
        writer.add_vertex("Person", [("name", "Bob"), ("age", 25)]);
        writer.add_vertex("Person", [("name", "Carol"), ("age", 35)]);
        writer.add_vertex("Company", [("name", "Acme")]);
        writer.commit();
    }
    
    let snapshot = graph.read();
    
    // Test: Find all Person vertices
    let results = snapshot.gql("MATCH (p:Person) RETURN p").unwrap();
    assert_eq!(results.len(), 3, "Should find 3 Person vertices");
    
    // Test: Find all vertices (no label filter)
    let results = snapshot.gql("MATCH (n) RETURN n").unwrap();
    assert_eq!(results.len(), 4, "Should find 4 total vertices");
}

#[test]
fn test_gql_parse_error() {
    let graph = Graph::new();
    let snapshot = graph.read();
    
    // Invalid syntax
    let result = snapshot.gql("MATCH (n:Person RETURN n");
    assert!(result.is_err());
}

#[test]
fn test_gql_undefined_variable() {
    let graph = Graph::new();
    let snapshot = graph.read();
    
    // Variable 'x' not defined in MATCH
    let result = snapshot.gql("MATCH (n:Person) RETURN x");
    assert!(result.is_err());
}
```

**Acceptance Criteria**:
- [ ] `snapshot.gql("MATCH (n:Person) RETURN n")` works end-to-end
- [ ] All integration tests pass
- [ ] Spike is complete - full pipeline from GQL text to results

---

### Spike Complete Checkpoint

At this point, we have:
- ✅ pest grammar parsing GQL
- ✅ AST types representing queries
- ✅ Parser converting text → AST
- ✅ Compiler converting AST → traversal → results
- ✅ Public API on `GraphSnapshot`
- ✅ Integration tests proving it works

**Next phases expand the grammar and compiler to support the full GQL subset.**

---

### Week 2: Full Pattern Grammar

---

#### Phase 2.1: Extended Grammar - Edge Patterns
**File**: `src/gql/grammar.pest`  
**Duration**: 2 hours

**Tasks**:
1. Add edge pattern rules with direction support
2. Add property filter syntax `{key: value}`
3. Support multiple patterns in MATCH

**Code** (additions to grammar.pest):

```pest
// Extended grammar.pest

// Keywords
WHERE    = { ^"where" }
ORDER    = { ^"order" }
BY       = { ^"by" }
LIMIT    = { ^"limit" }
OFFSET   = { ^"offset" }
AS       = { ^"as" }
AND      = { ^"and" }
OR       = { ^"or" }
NOT      = { ^"not" }
TRUE     = { ^"true" }
FALSE    = { ^"false" }
NULL     = { ^"null" }
ASC      = { ^"asc" }
DESC     = { ^"desc" }

// Full query structure
query = { SOI ~ match_clause ~ where_clause? ~ return_clause ~ order_clause? ~ limit_clause? ~ EOI }

// MATCH clause - multiple patterns
match_clause = { MATCH ~ pattern ~ ("," ~ pattern)* }

// Pattern with edges
pattern = { node_pattern ~ (edge_pattern ~ node_pattern)* }

// Node pattern with properties
node_pattern = { "(" ~ variable? ~ label_filter? ~ property_filter? ~ ")" }

// Edge pattern with direction
edge_pattern = { 
    left_arrow? ~ "-[" ~ variable? ~ label_filter? ~ quantifier? ~ property_filter? ~ "]-" ~ right_arrow?
}

left_arrow = { "<" }
right_arrow = { ">" }

// Labels: :Person or :Person:Employee (multiple)
label_filter = { (":" ~ identifier)+ }

// Properties: {name: 'Alice', age: 30}
property_filter = { "{" ~ property ~ ("," ~ property)* ~ "}" }
property = { identifier ~ ":" ~ literal }

// Path quantifier for variable-length: *1..3
quantifier = { "*" ~ range? }
range = { integer? ~ ".." ~ integer? | integer }

// Literals
literal = { string | float | integer | TRUE | FALSE | NULL }
string = ${ "'" ~ string_inner ~ "'" }
string_inner = @{ (!"'" ~ ANY | "''")* }
integer = @{ "-"? ~ ASCII_DIGIT+ }
float = @{ "-"? ~ ASCII_DIGIT+ ~ "." ~ ASCII_DIGIT+ }

variable = @{ ASCII_ALPHA ~ (ASCII_ALPHANUMERIC | "_")* }
identifier = @{ ASCII_ALPHA ~ (ASCII_ALPHANUMERIC | "_")* }
```

**Acceptance Criteria**:
- [ ] `(a)-[:KNOWS]->(b)` parses with Outgoing direction
- [ ] `(a)<-[:KNOWS]-(b)` parses with Incoming direction
- [ ] `(a)-[:KNOWS]-(b)` parses with Both direction
- [ ] `(n:Person {name: 'Alice'})` parses with properties
- [ ] Multiple patterns `(a), (b)` parse correctly

---

#### Phase 2.2: Parser - Edge Pattern Building
**File**: `src/gql/parser.rs`  
**Duration**: 2 hours

**Tasks**:
1. Implement `build_edge_pattern()` function
2. Handle direction from left/right arrows
3. Parse property filters
4. Parse path quantifiers

**Code**:

```rust
fn build_edge_pattern(pair: pest::iterators::Pair<Rule>) -> Result<EdgePattern, ParseError> {
    let mut variable = None;
    let mut labels = Vec::new();
    let mut direction = EdgeDirection::Both;
    let mut quantifier = None;
    let mut properties = Vec::new();
    
    let mut has_left = false;
    let mut has_right = false;
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::left_arrow => has_left = true,
            Rule::right_arrow => has_right = true,
            Rule::variable => variable = Some(inner.as_str().to_string()),
            Rule::label_filter => {
                for label in inner.into_inner() {
                    if label.as_rule() == Rule::identifier {
                        labels.push(label.as_str().to_string());
                    }
                }
            }
            Rule::quantifier => quantifier = Some(build_quantifier(inner)?),
            Rule::property_filter => properties = build_properties(inner)?,
            _ => {}
        }
    }
    
    direction = match (has_left, has_right) {
        (false, true) => EdgeDirection::Outgoing,   // -[]->
        (true, false) => EdgeDirection::Incoming,   // <-[]-
        _ => EdgeDirection::Both,                    // -[]-
    };
    
    Ok(EdgePattern {
        variable,
        labels,
        direction,
        quantifier,
        properties,
    })
}

fn build_quantifier(pair: pest::iterators::Pair<Rule>) -> Result<PathQuantifier, ParseError> {
    let mut min = None;
    let mut max = None;
    
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::range {
            let range_str = inner.as_str();
            if range_str.contains("..") {
                let parts: Vec<&str> = range_str.split("..").collect();
                if !parts[0].is_empty() {
                    min = Some(parts[0].parse().map_err(|_| ParseError::InvalidLiteral(parts[0].to_string()))?);
                }
                if parts.len() > 1 && !parts[1].is_empty() {
                    max = Some(parts[1].parse().map_err(|_| ParseError::InvalidLiteral(parts[1].to_string()))?);
                }
            } else {
                let n: u32 = range_str.parse().map_err(|_| ParseError::InvalidLiteral(range_str.to_string()))?;
                min = Some(n);
                max = Some(n);
            }
        }
    }
    
    Ok(PathQuantifier { min, max })
}

fn build_properties(pair: pest::iterators::Pair<Rule>) -> Result<Vec<(String, Literal)>, ParseError> {
    let mut properties = Vec::new();
    
    for prop in pair.into_inner() {
        if prop.as_rule() == Rule::property {
            let mut key = None;
            let mut value = None;
            
            for inner in prop.into_inner() {
                match inner.as_rule() {
                    Rule::identifier => key = Some(inner.as_str().to_string()),
                    Rule::literal => value = Some(build_literal(inner)?),
                    _ => {}
                }
            }
            
            if let (Some(k), Some(v)) = (key, value) {
                properties.push((k, v));
            }
        }
    }
    
    Ok(properties)
}

fn build_literal(pair: pest::iterators::Pair<Rule>) -> Result<Literal, ParseError> {
    let inner = pair.into_inner().next()
        .ok_or_else(|| ParseError::InvalidLiteral("empty".to_string()))?;
    
    match inner.as_rule() {
        Rule::string => {
            let s = inner.as_str();
            // Remove surrounding quotes and unescape ''
            let content = &s[1..s.len()-1];
            let unescaped = content.replace("''", "'");
            Ok(Literal::String(unescaped))
        }
        Rule::integer => {
            let n: i64 = inner.as_str().parse()
                .map_err(|_| ParseError::InvalidLiteral(inner.as_str().to_string()))?;
            Ok(Literal::Int(n))
        }
        Rule::float => {
            let f: f64 = inner.as_str().parse()
                .map_err(|_| ParseError::InvalidLiteral(inner.as_str().to_string()))?;
            Ok(Literal::Float(f))
        }
        Rule::TRUE => Ok(Literal::Bool(true)),
        Rule::FALSE => Ok(Literal::Bool(false)),
        Rule::NULL => Ok(Literal::Null),
        _ => Err(ParseError::InvalidLiteral(inner.as_str().to_string())),
    }
}
```

**Acceptance Criteria**:
- [ ] Edge patterns parse with correct direction
- [ ] Properties parse to `Vec<(String, Literal)>`
- [ ] String literals handle escaping (`''` → `'`)
- [ ] Quantifiers parse min/max bounds correctly

---

#### Phase 2.3: Parser - RETURN Clause Extensions
**File**: `src/gql/parser.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Support property access in RETURN: `n.name`
2. Support AS aliases: `n.name AS personName`
3. Support multiple return items: `a.name, b.name`

**Grammar additions**:

```pest
// RETURN clause - multiple items with aliases
return_clause = { RETURN ~ return_item ~ ("," ~ return_item)* }
return_item = { expression ~ (AS ~ identifier)? }

// Expression - start simple
expression = { property_access | variable | literal }
property_access = { variable ~ "." ~ identifier }
```

**Parser code**:

```rust
fn build_return_clause(pair: pest::iterators::Pair<Rule>) -> Result<ReturnClause, ParseError> {
    let mut items = Vec::new();
    
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::return_item {
            items.push(build_return_item(inner)?);
        }
    }
    
    Ok(ReturnClause { items })
}

fn build_return_item(pair: pest::iterators::Pair<Rule>) -> Result<ReturnItem, ParseError> {
    let mut expression = None;
    let mut alias = None;
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::expression => expression = Some(build_expression(inner)?),
            Rule::identifier => alias = Some(inner.as_str().to_string()),
            _ => {}
        }
    }
    
    Ok(ReturnItem {
        expression: expression.ok_or(ParseError::MissingClause("expression"))?,
        alias,
    })
}

fn build_expression(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let inner = pair.into_inner().next()
        .ok_or_else(|| ParseError::MissingClause("expression"))?;
    
    match inner.as_rule() {
        Rule::property_access => {
            let mut parts = inner.into_inner();
            let variable = parts.next()
                .ok_or(ParseError::MissingClause("variable"))?
                .as_str().to_string();
            let property = parts.next()
                .ok_or(ParseError::MissingClause("property"))?
                .as_str().to_string();
            Ok(Expression::Property { variable, property })
        }
        Rule::variable => {
            Ok(Expression::Variable(inner.as_str().to_string()))
        }
        Rule::literal => {
            Ok(Expression::Literal(build_literal(inner)?))
        }
        _ => Err(ParseError::InvalidLiteral(inner.as_str().to_string())),
    }
}
```

**Acceptance Criteria**:
- [ ] `RETURN n.name` parses to `Expression::Property`
- [ ] `RETURN n.name AS personName` includes alias
- [ ] `RETURN a.name, b.age` parses multiple items

---

#### Phase 2.4: Compiler - Edge Traversal
**File**: `src/gql/compiler.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Compile edge patterns to `out()`, `in_()`, `both()` calls
2. Apply edge label filters with `out_labels()`, etc.
3. Apply property filters with `has_value()`
4. Handle multi-hop patterns

**Code**:

```rust
impl<'a, 'g> Compiler<'a, 'g> {
    fn compile_pattern(
        &mut self,
        pattern: &Pattern,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
        let mut current = traversal;
        let mut element_index = 0;
        
        for element in &pattern.elements {
            match element {
                PatternElement::Node(node) => {
                    current = self.compile_node(node, current, element_index)?;
                }
                PatternElement::Edge(edge) => {
                    current = self.compile_edge(edge, current)?;
                }
            }
            element_index += 1;
        }
        
        Ok(current)
    }
    
    fn compile_node(
        &mut self,
        node: &NodePattern,
        traversal: BoundTraversal<'g, (), Value>,
        index: usize,
    ) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
        let mut current = traversal;
        
        // Apply label filter
        if !node.labels.is_empty() {
            let labels: Vec<&str> = node.labels.iter().map(|s| s.as_str()).collect();
            current = current.has_label_any(&labels);
        }
        
        // Apply property filters
        for (key, value) in &node.properties {
            let val: Value = value.clone().into();
            current = current.has_value(key.clone(), val);
        }
        
        // Register binding
        if let Some(var) = &node.variable {
            if self.bindings.contains_key(var) {
                return Err(CompileError::DuplicateVariable(var.clone()));
            }
            self.bindings.insert(var.clone(), BindingInfo {
                pattern_index: index,
                is_node: true,
            });
            current = current.as_(var.clone());
        }
        
        Ok(current)
    }
    
    fn compile_edge(
        &mut self,
        edge: &EdgePattern,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
        let labels: Vec<&str> = edge.labels.iter().map(|s| s.as_str()).collect();
        
        // Navigate based on direction
        let current = match edge.direction {
            EdgeDirection::Outgoing => {
                if labels.is_empty() {
                    traversal.out()
                } else {
                    traversal.out_labels(&labels)
                }
            }
            EdgeDirection::Incoming => {
                if labels.is_empty() {
                    traversal.in_()
                } else {
                    traversal.in_labels(&labels)
                }
            }
            EdgeDirection::Both => {
                if labels.is_empty() {
                    traversal.both()
                } else {
                    traversal.both_labels(&labels)
                }
            }
        };
        
        // TODO: Handle edge variable binding (requires outE/inE approach)
        // TODO: Handle property filters on edges
        // TODO: Handle quantifiers (variable-length paths)
        
        Ok(current)
    }
}
```

**Acceptance Criteria**:
- [ ] `(a)-[:KNOWS]->(b)` compiles to `out_labels(&["KNOWS"])`
- [ ] `(a)<-[:KNOWS]-(b)` compiles to `in_labels(&["KNOWS"])`
- [ ] `(a:Person {name: 'Alice'})` applies `has_value()`
- [ ] Multi-hop patterns work: `(a)-[:KNOWS]->(b)-[:KNOWS]->(c)`

---

#### Phase 2.5: Compiler - Property Access in RETURN
**File**: `src/gql/compiler.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Handle `Expression::Property` in RETURN clause
2. Use `values()` step or project with `by_key()`
3. Return property values instead of vertices

**Code**:

```rust
fn execute_return(
    &self,
    return_clause: &ReturnClause,
    traversal: BoundTraversal<'g, (), Value>,
) -> Result<Vec<Value>, CompileError> {
    // Validate variables
    for item in &return_clause.items {
        self.validate_expression_variables(&item.expression)?;
    }
    
    // Simple case: single return item
    if return_clause.items.len() == 1 {
        let item = &return_clause.items[0];
        match &item.expression {
            Expression::Variable(var) => {
                // Return the vertex/edge itself
                // Use select() if bound with as_()
                let results = traversal.select(&[var.as_str()]).to_list();
                Ok(results)
            }
            Expression::Property { variable, property } => {
                // Return the property value
                let results = traversal
                    .select(&[variable.as_str()])
                    .values(property.as_str())
                    .to_list();
                Ok(results)
            }
            Expression::Literal(lit) => {
                // Return constant value for each traverser
                let val: Value = lit.clone().into();
                let results = traversal.constant(val.clone()).to_list();
                Ok(results)
            }
            _ => Err(CompileError::UnsupportedExpression),
        }
    } else {
        // Multiple return items - use project()
        self.execute_multi_return(return_clause, traversal)
    }
}

fn execute_multi_return(
    &self,
    return_clause: &ReturnClause,
    traversal: BoundTraversal<'g, (), Value>,
) -> Result<Vec<Value>, CompileError> {
    // Build projection keys
    let keys: Vec<String> = return_clause.items.iter()
        .enumerate()
        .map(|(i, item)| {
            item.alias.clone().unwrap_or_else(|| format!("_{}", i))
        })
        .collect();
    
    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
    let mut project = traversal.project(&key_refs);
    
    // Add by() for each item
    for item in &return_clause.items {
        project = match &item.expression {
            Expression::Variable(var) => {
                project.by_select(var.as_str())
            }
            Expression::Property { variable: _, property } => {
                project.by_key(property.as_str())
            }
            Expression::Literal(lit) => {
                let val: Value = lit.clone().into();
                project.by_constant(val)
            }
            _ => return Err(CompileError::UnsupportedExpression),
        };
    }
    
    Ok(project.to_list())
}

fn validate_expression_variables(&self, expr: &Expression) -> Result<(), CompileError> {
    match expr {
        Expression::Variable(var) => {
            if !self.bindings.contains_key(var) {
                return Err(CompileError::UndefinedVariable(var.clone()));
            }
        }
        Expression::Property { variable, .. } => {
            if !self.bindings.contains_key(variable) {
                return Err(CompileError::UndefinedVariable(variable.clone()));
            }
        }
        _ => {}
    }
    Ok(())
}
```

**Acceptance Criteria**:
- [ ] `RETURN n.name` returns property values
- [ ] `RETURN n.name, n.age` returns maps with both properties
- [ ] `RETURN n.name AS personName` uses alias in result
- [ ] Undefined variable in property access produces error

---

#### Phase 2.6: Integration Tests - Patterns
**File**: `tests/gql.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Add tests for edge traversal
2. Add tests for property filters
3. Add tests for property access in RETURN

**Code**:

```rust
#[test]
fn test_gql_edge_traversal() {
    let graph = Graph::new();
    
    {
        let mut writer = graph.write();
        let alice = writer.add_vertex("Person", [("name", "Alice")]);
        let bob = writer.add_vertex("Person", [("name", "Bob")]);
        let carol = writer.add_vertex("Person", [("name", "Carol")]);
        
        writer.add_edge(alice, bob, "KNOWS", []);
        writer.add_edge(alice, carol, "KNOWS", []);
        writer.add_edge(bob, carol, "WORKS_WITH", []);
        writer.commit();
    }
    
    let snapshot = graph.read();
    
    // Test outgoing edge
    let results = snapshot.gql(
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(friend) RETURN friend"
    ).unwrap();
    assert_eq!(results.len(), 2);
    
    // Test incoming edge
    let results = snapshot.gql(
        "MATCH (b:Person {name: 'Bob'})<-[:KNOWS]-(source) RETURN source"
    ).unwrap();
    assert_eq!(results.len(), 1);
    
    // Test bidirectional
    let results = snapshot.gql(
        "MATCH (b:Person {name: 'Bob'})-[:KNOWS]-(connected) RETURN connected"
    ).unwrap();
    assert_eq!(results.len(), 2); // Alice (incoming) and any outgoing KNOWS
}

#[test]
fn test_gql_property_return() {
    let graph = Graph::new();
    
    {
        let mut writer = graph.write();
        writer.add_vertex("Person", [("name", "Alice"), ("age", 30)]);
        writer.add_vertex("Person", [("name", "Bob"), ("age", 25)]);
        writer.commit();
    }
    
    let snapshot = graph.read();
    
    // Return single property
    let results = snapshot.gql("MATCH (p:Person) RETURN p.name").unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.contains(&Value::String("Alice".to_string())));
    assert!(results.contains(&Value::String("Bob".to_string())));
}

#[test]
fn test_gql_multi_hop() {
    let graph = Graph::new();
    
    {
        let mut writer = graph.write();
        let a = writer.add_vertex("Person", [("name", "Alice")]);
        let b = writer.add_vertex("Person", [("name", "Bob")]);
        let c = writer.add_vertex("Person", [("name", "Carol")]);
        
        writer.add_edge(a, b, "KNOWS", []);
        writer.add_edge(b, c, "KNOWS", []);
        writer.commit();
    }
    
    let snapshot = graph.read();
    
    // Two-hop traversal
    let results = snapshot.gql(
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.name"
    ).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Carol".to_string()));
}
```

**Acceptance Criteria**:
- [ ] All pattern traversal tests pass
- [ ] Property return tests pass
- [ ] Multi-hop tests pass

---

### Week 3: WHERE Clause and Expressions

---

#### Phase 3.1: Expression Grammar
**File**: `src/gql/grammar.pest`  
**Duration**: 2 hours

**Tasks**:
1. Full expression grammar with precedence
2. Comparison operators
3. Logical operators (AND, OR, NOT)
4. Arithmetic operators

**Grammar**:

```pest
// WHERE clause
where_clause = { WHERE ~ expression }

// Expressions with correct precedence
expression = { or_expr }

or_expr = { and_expr ~ (OR ~ and_expr)* }

and_expr = { not_expr ~ (AND ~ not_expr)* }

not_expr = { NOT? ~ comparison }

comparison = { additive ~ (comp_op ~ additive)? | is_null_expr | in_expr }

is_null_expr = { additive ~ IS ~ NOT? ~ NULL }

in_expr = { additive ~ NOT? ~ IN ~ list_expr }

comp_op = { 
    "<>" | "!=" | "<=" | ">=" | "=" | "<" | ">" 
    | CONTAINS | starts_with | ends_with 
}

starts_with = { STARTS ~ WITH }
ends_with = { ENDS ~ WITH }
CONTAINS = { ^"contains" }
STARTS = { ^"starts" }
ENDS = { ^"ends" }
WITH = { ^"with" }
IN = { ^"in" }
IS = { ^"is" }

additive = { multiplicative ~ (("+"|"-") ~ multiplicative)* }

multiplicative = { unary ~ (("*"|"/"|"%") ~ unary)* }

unary = { "-"? ~ primary }

primary = { 
    literal
    | function_call
    | property_access
    | variable
    | "(" ~ expression ~ ")"
    | list_expr
}

function_call = { identifier ~ "(" ~ (expression ~ ("," ~ expression)*)? ~ ")" }

list_expr = { "[" ~ (expression ~ ("," ~ expression)*)? ~ "]" }
```

**Acceptance Criteria**:
- [ ] `WHERE p.age > 30` parses
- [ ] `WHERE p.age > 30 AND p.name = 'Alice'` parses with correct precedence
- [ ] `WHERE NOT p.active` parses
- [ ] `WHERE p.name IS NULL` parses
- [ ] `WHERE p.status IN ['active', 'pending']` parses

---

#### Phase 3.2: Parser - Expression Building
**File**: `src/gql/parser.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Implement recursive expression parser
2. Build `Expression` AST from pest pairs
3. Handle operator precedence (already in grammar)

**Code**:

```rust
fn build_expression(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    build_or_expr(pair.into_inner().next().unwrap())
}

fn build_or_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let mut iter = pair.into_inner();
    let mut left = build_and_expr(iter.next().unwrap())?;
    
    while let Some(right_pair) = iter.next() {
        let right = build_and_expr(right_pair)?;
        left = Expression::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Or,
            right: Box::new(right),
        };
    }
    
    Ok(left)
}

fn build_and_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let mut iter = pair.into_inner();
    let mut left = build_not_expr(iter.next().unwrap())?;
    
    while let Some(right_pair) = iter.next() {
        let right = build_not_expr(right_pair)?;
        left = Expression::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        };
    }
    
    Ok(left)
}

fn build_not_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let mut iter = pair.into_inner();
    let mut negated = false;
    
    // Check for NOT keyword
    let first = iter.next().unwrap();
    let comparison_pair = if first.as_rule() == Rule::NOT {
        negated = true;
        iter.next().unwrap()
    } else {
        first
    };
    
    let expr = build_comparison(comparison_pair)?;
    
    if negated {
        Ok(Expression::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(expr),
        })
    } else {
        Ok(expr)
    }
}

fn build_comparison(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    match pair.as_rule() {
        Rule::comparison => {
            let mut iter = pair.into_inner();
            let left = build_additive(iter.next().unwrap())?;
            
            if let Some(op_pair) = iter.next() {
                let op = parse_comp_op(op_pair)?;
                let right = build_additive(iter.next().unwrap())?;
                Ok(Expression::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                })
            } else {
                Ok(left)
            }
        }
        Rule::is_null_expr => {
            let mut iter = pair.into_inner();
            let expr = build_additive(iter.next().unwrap())?;
            let negated = iter.any(|p| p.as_rule() == Rule::NOT);
            Ok(Expression::IsNull {
                expr: Box::new(expr),
                negated,
            })
        }
        Rule::in_expr => {
            let mut iter = pair.into_inner();
            let expr = build_additive(iter.next().unwrap())?;
            let negated = iter.clone().any(|p| p.as_rule() == Rule::NOT);
            let list_pair = iter.find(|p| p.as_rule() == Rule::list_expr).unwrap();
            let list = build_list_expr(list_pair)?;
            Ok(Expression::InList {
                expr: Box::new(expr),
                list,
                negated,
            })
        }
        _ => build_additive(pair),
    }
}

fn parse_comp_op(pair: pest::iterators::Pair<Rule>) -> Result<BinaryOperator, ParseError> {
    let op_str = pair.as_str().to_uppercase();
    match op_str.as_str() {
        "=" => Ok(BinaryOperator::Eq),
        "<>" | "!=" => Ok(BinaryOperator::Neq),
        "<" => Ok(BinaryOperator::Lt),
        "<=" => Ok(BinaryOperator::Lte),
        ">" => Ok(BinaryOperator::Gt),
        ">=" => Ok(BinaryOperator::Gte),
        _ if op_str.contains("CONTAINS") => Ok(BinaryOperator::Contains),
        _ if op_str.contains("STARTS") => Ok(BinaryOperator::StartsWith),
        _ if op_str.contains("ENDS") => Ok(BinaryOperator::EndsWith),
        _ => Err(ParseError::InvalidLiteral(op_str)),
    }
}

fn build_additive(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let mut iter = pair.into_inner();
    let mut left = build_multiplicative(iter.next().unwrap())?;
    
    while let Some(op_pair) = iter.next() {
        let op = match op_pair.as_str() {
            "+" => BinaryOperator::Add,
            "-" => BinaryOperator::Sub,
            _ => return Err(ParseError::InvalidLiteral(op_pair.as_str().to_string())),
        };
        let right = build_multiplicative(iter.next().unwrap())?;
        left = Expression::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
    }
    
    Ok(left)
}

// Similar implementations for build_multiplicative, build_unary, build_primary...
```

**Acceptance Criteria**:
- [ ] Complex expressions parse correctly
- [ ] Operator precedence is correct (NOT > AND > OR)
- [ ] Arithmetic expressions work
- [ ] All expression tests pass

---

#### Phase 3.3: Add Missing AST Types
**File**: `src/gql/ast.rs`  
**Duration**: 30 minutes

**Tasks**:
1. Add `UnaryOp` variant to `Expression`
2. Add `IsNull` variant
3. Add `InList` variant
4. Add `UnaryOperator` enum

**Code**:

```rust
#[derive(Debug, Clone)]
pub enum Expression {
    // ... existing variants ...
    
    /// Unary operation: NOT, -
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
    
    /// IS NULL / IS NOT NULL
    IsNull {
        expr: Box<Expression>,
        negated: bool,  // true for IS NOT NULL
    },
    
    /// IN list check
    InList {
        expr: Box<Expression>,
        list: Vec<Expression>,
        negated: bool,  // true for NOT IN
    },
    
    /// List literal: [1, 2, 3]
    List(Vec<Expression>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    Not,
    Neg,
}
```

**Acceptance Criteria**:
- [ ] All expression variants defined
- [ ] AST can represent all supported expressions

---

#### Phase 3.4: Compiler - WHERE Clause
**File**: `src/gql/compiler.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Compile WHERE expression to filter predicate
2. Evaluate expressions at runtime
3. Support comparison, logical, and null checks

**Code**:

```rust
impl<'a, 'g> Compiler<'a, 'g> {
    fn compile_where(
        &self,
        where_clause: &WhereClause,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
        // Compile expression to a filter closure
        let predicate = self.compile_predicate(&where_clause.expression)?;
        
        // Apply filter to traversal
        Ok(traversal.has_where(predicate))
    }
    
    fn compile_predicate(
        &self,
        expr: &Expression,
    ) -> Result<Box<dyn Fn(&ExecutionContext, &Traverser) -> bool + Send + Sync>, CompileError> {
        match expr {
            Expression::BinaryOp { left, op, right } => {
                self.compile_binary_predicate(left, *op, right)
            }
            Expression::UnaryOp { op: UnaryOperator::Not, expr } => {
                let inner = self.compile_predicate(expr)?;
                Ok(Box::new(move |ctx, t| !inner(ctx, t)))
            }
            Expression::IsNull { expr, negated } => {
                let eval = self.compile_value_expr(expr)?;
                let neg = *negated;
                Ok(Box::new(move |ctx, t| {
                    let val = eval(ctx, t);
                    let is_null = matches!(val, Value::Null);
                    if neg { !is_null } else { is_null }
                }))
            }
            Expression::InList { expr, list, negated } => {
                let eval = self.compile_value_expr(expr)?;
                let list_evals: Vec<_> = list.iter()
                    .map(|e| self.compile_value_expr(e))
                    .collect::<Result<_, _>>()?;
                let neg = *negated;
                Ok(Box::new(move |ctx, t| {
                    let val = eval(ctx, t);
                    let in_list = list_evals.iter().any(|e| e(ctx, t) == val);
                    if neg { !in_list } else { in_list }
                }))
            }
            _ => {
                // For non-boolean expressions, evaluate and check truthiness
                let eval = self.compile_value_expr(expr)?;
                Ok(Box::new(move |ctx, t| {
                    match eval(ctx, t) {
                        Value::Bool(b) => b,
                        Value::Null => false,
                        _ => true,  // Non-null values are truthy
                    }
                }))
            }
        }
    }
    
    fn compile_binary_predicate(
        &self,
        left: &Expression,
        op: BinaryOperator,
        right: &Expression,
    ) -> Result<Box<dyn Fn(&ExecutionContext, &Traverser) -> bool + Send + Sync>, CompileError> {
        let left_eval = self.compile_value_expr(left)?;
        let right_eval = self.compile_value_expr(right)?;
        
        Ok(Box::new(move |ctx, t| {
            let l = left_eval(ctx, t);
            let r = right_eval(ctx, t);
            apply_comparison(op, &l, &r)
        }))
    }
    
    fn compile_value_expr(
        &self,
        expr: &Expression,
    ) -> Result<Box<dyn Fn(&ExecutionContext, &Traverser) -> Value + Send + Sync>, CompileError> {
        match expr {
            Expression::Literal(lit) => {
                let val: Value = lit.clone().into();
                Ok(Box::new(move |_, _| val.clone()))
            }
            Expression::Variable(var) => {
                // Look up from path using select semantics
                let var = var.clone();
                if !self.bindings.contains_key(&var) {
                    return Err(CompileError::UndefinedVariable(var));
                }
                Ok(Box::new(move |ctx, t| {
                    t.path().get(&var)
                        .map(|e| e.value().clone())
                        .unwrap_or(Value::Null)
                }))
            }
            Expression::Property { variable, property } => {
                let var = variable.clone();
                let prop = property.clone();
                if !self.bindings.contains_key(&var) {
                    return Err(CompileError::UndefinedVariable(var));
                }
                Ok(Box::new(move |ctx, t| {
                    // Get vertex from path, then property
                    t.path().get(&var)
                        .and_then(|e| {
                            if let Value::Vertex(vid) = e.value() {
                                ctx.snapshot.get_vertex(*vid)
                                    .and_then(|v| v.properties().get(&prop).cloned())
                            } else {
                                None
                            }
                        })
                        .unwrap_or(Value::Null)
                }))
            }
            Expression::BinaryOp { left, op, right } => {
                let left_eval = self.compile_value_expr(left)?;
                let right_eval = self.compile_value_expr(right)?;
                let op = *op;
                Ok(Box::new(move |ctx, t| {
                    let l = left_eval(ctx, t);
                    let r = right_eval(ctx, t);
                    apply_binary_op(op, l, r)
                }))
            }
            _ => Err(CompileError::UnsupportedExpression),
        }
    }
}

fn apply_comparison(op: BinaryOperator, left: &Value, right: &Value) -> bool {
    match op {
        BinaryOperator::Eq => left == right,
        BinaryOperator::Neq => left != right,
        BinaryOperator::Lt => left.to_comparable() < right.to_comparable(),
        BinaryOperator::Lte => left.to_comparable() <= right.to_comparable(),
        BinaryOperator::Gt => left.to_comparable() > right.to_comparable(),
        BinaryOperator::Gte => left.to_comparable() >= right.to_comparable(),
        BinaryOperator::And => {
            left.as_bool().unwrap_or(false) && right.as_bool().unwrap_or(false)
        }
        BinaryOperator::Or => {
            left.as_bool().unwrap_or(false) || right.as_bool().unwrap_or(false)
        }
        BinaryOperator::Contains => {
            match (left, right) {
                (Value::String(s), Value::String(sub)) => s.contains(sub.as_str()),
                _ => false,
            }
        }
        BinaryOperator::StartsWith => {
            match (left, right) {
                (Value::String(s), Value::String(prefix)) => s.starts_with(prefix.as_str()),
                _ => false,
            }
        }
        BinaryOperator::EndsWith => {
            match (left, right) {
                (Value::String(s), Value::String(suffix)) => s.ends_with(suffix.as_str()),
                _ => false,
            }
        }
        _ => false,
    }
}

fn apply_binary_op(op: BinaryOperator, left: Value, right: Value) -> Value {
    match op {
        BinaryOperator::Add => {
            match (left, right) {
                (Value::Int(a), Value::Int(b)) => Value::Int(a + b),
                (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
                (Value::String(a), Value::String(b)) => Value::String(a + &b),
                _ => Value::Null,
            }
        }
        BinaryOperator::Sub => {
            match (left, right) {
                (Value::Int(a), Value::Int(b)) => Value::Int(a - b),
                (Value::Float(a), Value::Float(b)) => Value::Float(a - b),
                _ => Value::Null,
            }
        }
        BinaryOperator::Mul => {
            match (left, right) {
                (Value::Int(a), Value::Int(b)) => Value::Int(a * b),
                (Value::Float(a), Value::Float(b)) => Value::Float(a * b),
                _ => Value::Null,
            }
        }
        BinaryOperator::Div => {
            match (left, right) {
                (Value::Int(a), Value::Int(b)) if b != 0 => Value::Int(a / b),
                (Value::Float(a), Value::Float(b)) => Value::Float(a / b),
                _ => Value::Null,
            }
        }
        // Comparison ops return Bool
        op => Value::Bool(apply_comparison(op, &left, &right)),
    }
}
```

**Acceptance Criteria**:
- [ ] `WHERE p.age > 30` filters correctly
- [ ] `WHERE p.age > 30 AND p.name = 'Alice'` combines predicates
- [ ] `WHERE NOT p.active` negates predicate
- [ ] `WHERE p.status IN ['active', 'pending']` checks list membership
- [ ] `WHERE p.email IS NOT NULL` checks for non-null

---

#### Phase 3.5: Integration Tests - WHERE Clause
**File**: `tests/gql.rs`  
**Duration**: 1-2 hours

**Code**:

```rust
#[test]
fn test_gql_where_comparison() {
    let graph = Graph::new();
    
    {
        let mut writer = graph.write();
        writer.add_vertex("Person", [("name", "Alice"), ("age", 30)]);
        writer.add_vertex("Person", [("name", "Bob"), ("age", 25)]);
        writer.add_vertex("Person", [("name", "Carol"), ("age", 35)]);
        writer.commit();
    }
    
    let snapshot = graph.read();
    
    // Greater than
    let results = snapshot.gql(
        "MATCH (p:Person) WHERE p.age > 28 RETURN p.name"
    ).unwrap();
    assert_eq!(results.len(), 2);  // Alice (30), Carol (35)
    
    // Equality
    let results = snapshot.gql(
        "MATCH (p:Person) WHERE p.name = 'Bob' RETURN p"
    ).unwrap();
    assert_eq!(results.len(), 1);
    
    // Combined with AND
    let results = snapshot.gql(
        "MATCH (p:Person) WHERE p.age >= 25 AND p.age < 35 RETURN p.name"
    ).unwrap();
    assert_eq!(results.len(), 2);  // Alice (30), Bob (25)
}

#[test]
fn test_gql_where_string_ops() {
    let graph = Graph::new();
    
    {
        let mut writer = graph.write();
        writer.add_vertex("Person", [("name", "Alice Anderson")]);
        writer.add_vertex("Person", [("name", "Bob Brown")]);
        writer.add_vertex("Person", [("name", "Carol Anderson")]);
        writer.commit();
    }
    
    let snapshot = graph.read();
    
    // CONTAINS
    let results = snapshot.gql(
        "MATCH (p:Person) WHERE p.name CONTAINS 'Anderson' RETURN p"
    ).unwrap();
    assert_eq!(results.len(), 2);
    
    // STARTS WITH
    let results = snapshot.gql(
        "MATCH (p:Person) WHERE p.name STARTS WITH 'Bob' RETURN p"
    ).unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn test_gql_where_in_list() {
    let graph = Graph::new();
    
    {
        let mut writer = graph.write();
        writer.add_vertex("Person", [("name", "Alice"), ("status", "active")]);
        writer.add_vertex("Person", [("name", "Bob"), ("status", "pending")]);
        writer.add_vertex("Person", [("name", "Carol"), ("status", "inactive")]);
        writer.commit();
    }
    
    let snapshot = graph.read();
    
    let results = snapshot.gql(
        "MATCH (p:Person) WHERE p.status IN ['active', 'pending'] RETURN p.name"
    ).unwrap();
    assert_eq!(results.len(), 2);  // Alice, Bob
}

#[test]
fn test_gql_where_null_check() {
    let graph = Graph::new();
    
    {
        let mut writer = graph.write();
        writer.add_vertex("Person", [("name", "Alice"), ("email", "alice@example.com")]);
        writer.add_vertex("Person", [("name", "Bob")]);  // No email
        writer.commit();
    }
    
    let snapshot = graph.read();
    
    // IS NOT NULL
    let results = snapshot.gql(
        "MATCH (p:Person) WHERE p.email IS NOT NULL RETURN p.name"
    ).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice".to_string()));
    
    // IS NULL
    let results = snapshot.gql(
        "MATCH (p:Person) WHERE p.email IS NULL RETURN p.name"
    ).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Bob".to_string()));
}
```

**Acceptance Criteria**:
- [ ] All WHERE clause tests pass
- [ ] Comparison operators work correctly
- [ ] String operations work
- [ ] IN list membership works
- [ ] NULL checks work

---

### Week 4: ORDER BY, LIMIT, and Aggregations

---

#### Phase 4.1: ORDER BY Grammar and Parser
**File**: `src/gql/grammar.pest`, `src/gql/parser.rs`  
**Duration**: 1-2 hours

**Grammar**:

```pest
// ORDER BY clause
order_clause = { ORDER ~ BY ~ order_item ~ ("," ~ order_item)* }
order_item = { expression ~ (ASC | DESC)? }
```

**Parser code**:

```rust
fn build_order_clause(pair: pest::iterators::Pair<Rule>) -> Result<OrderClause, ParseError> {
    let mut items = Vec::new();
    
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::order_item {
            items.push(build_order_item(inner)?);
        }
    }
    
    Ok(OrderClause { items })
}

fn build_order_item(pair: pest::iterators::Pair<Rule>) -> Result<OrderItem, ParseError> {
    let mut expression = None;
    let mut descending = false;
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::expression => expression = Some(build_expression(inner)?),
            Rule::DESC => descending = true,
            Rule::ASC => descending = false,
            _ => {}
        }
    }
    
    Ok(OrderItem {
        expression: expression.ok_or(ParseError::MissingClause("expression"))?,
        descending,
    })
}
```

**Acceptance Criteria**:
- [ ] `ORDER BY p.age` parses (default ASC)
- [ ] `ORDER BY p.age DESC` parses
- [ ] `ORDER BY p.age DESC, p.name ASC` parses multiple

---

#### Phase 4.2: LIMIT Grammar and Parser
**File**: `src/gql/grammar.pest`, `src/gql/parser.rs`  
**Duration**: 30 minutes

**Grammar**:

```pest
// LIMIT clause
limit_clause = { LIMIT ~ integer ~ (OFFSET ~ integer)? }
```

**Parser code**:

```rust
fn build_limit_clause(pair: pest::iterators::Pair<Rule>) -> Result<LimitClause, ParseError> {
    let mut limit = 0;
    let mut offset = None;
    let mut seen_limit = false;
    
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::integer {
            let n: u64 = inner.as_str().parse()
                .map_err(|_| ParseError::InvalidLiteral(inner.as_str().to_string()))?;
            if !seen_limit {
                limit = n;
                seen_limit = true;
            } else {
                offset = Some(n);
            }
        }
    }
    
    Ok(LimitClause { limit, offset })
}
```

**Acceptance Criteria**:
- [ ] `LIMIT 10` parses
- [ ] `LIMIT 10 OFFSET 5` parses

---

#### Phase 4.3: Compiler - ORDER BY and LIMIT
**File**: `src/gql/compiler.rs`  
**Duration**: 2 hours

**Tasks**:
1. Apply ORDER BY using traversal `order()` step
2. Apply LIMIT using `limit()` step
3. Apply OFFSET using `skip()` step

**Code**:

```rust
impl<'a, 'g> Compiler<'a, 'g> {
    fn apply_order_by(
        &self,
        order_clause: &OrderClause,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
        let mut order_builder = traversal.order();
        
        for item in &order_clause.items {
            let direction = if item.descending {
                crate::traversal::Order::Desc
            } else {
                crate::traversal::Order::Asc
            };
            
            match &item.expression {
                Expression::Property { variable: _, property } => {
                    order_builder = order_builder.by_key(property.as_str(), direction);
                }
                Expression::Variable(_) => {
                    // Order by the value itself
                    order_builder = order_builder.by_value(direction);
                }
                _ => return Err(CompileError::UnsupportedExpression),
            }
        }
        
        Ok(order_builder.build())
    }
    
    fn apply_limit(
        &self,
        limit_clause: &LimitClause,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> BoundTraversal<'g, (), Value> {
        let mut result = traversal;
        
        // Apply offset first (skip)
        if let Some(offset) = limit_clause.offset {
            result = result.skip(offset as usize);
        }
        
        // Apply limit
        result = result.limit(limit_clause.limit as usize);
        
        result
    }
}
```

**Acceptance Criteria**:
- [ ] `ORDER BY p.age` sorts ascending
- [ ] `ORDER BY p.age DESC` sorts descending
- [ ] `LIMIT 10` returns at most 10 results
- [ ] `LIMIT 10 OFFSET 5` skips 5, returns next 10

---

#### Phase 4.4: Aggregation Grammar
**File**: `src/gql/grammar.pest`  
**Duration**: 1 hour

**Grammar**:

```pest
// Aggregate functions
aggregate = { agg_func ~ "(" ~ DISTINCT? ~ (expression | "*")? ~ ")" }
agg_func = { COUNT | SUM | AVG | MIN | MAX | COLLECT }

COUNT = { ^"count" }
SUM = { ^"sum" }
AVG = { ^"avg" }
MIN = { ^"min" }
MAX = { ^"max" }
COLLECT = { ^"collect" }
DISTINCT = { ^"distinct" }

// Update primary to include aggregate
primary = { 
    aggregate
    | literal
    | function_call
    | property_access
    | variable
    | "(" ~ expression ~ ")"
    | list_expr
}
```

**Acceptance Criteria**:
- [ ] `COUNT(*)` parses
- [ ] `COUNT(DISTINCT p.city)` parses
- [ ] `SUM(p.age)` parses
- [ ] `COLLECT(friend.name)` parses

---

#### Phase 4.5: Aggregation Parser
**File**: `src/gql/parser.rs`  
**Duration**: 1 hour

**Code**:

```rust
fn build_aggregate(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let mut func = None;
    let mut distinct = false;
    let mut expr = None;
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::agg_func => {
                func = Some(parse_agg_func(inner)?);
            }
            Rule::DISTINCT => {
                distinct = true;
            }
            Rule::expression => {
                expr = Some(build_expression(inner)?);
            }
            _ => {
                // Handle COUNT(*)
                if inner.as_str() == "*" {
                    expr = Some(Expression::Variable("*".to_string()));
                }
            }
        }
    }
    
    Ok(Expression::Aggregate {
        func: func.ok_or(ParseError::MissingClause("aggregate function"))?,
        distinct,
        expr: Box::new(expr.unwrap_or(Expression::Variable("*".to_string()))),
    })
}

fn parse_agg_func(pair: pest::iterators::Pair<Rule>) -> Result<AggregateFunc, ParseError> {
    let func_str = pair.as_str().to_uppercase();
    match func_str.as_str() {
        "COUNT" => Ok(AggregateFunc::Count),
        "SUM" => Ok(AggregateFunc::Sum),
        "AVG" => Ok(AggregateFunc::Avg),
        "MIN" => Ok(AggregateFunc::Min),
        "MAX" => Ok(AggregateFunc::Max),
        "COLLECT" => Ok(AggregateFunc::Collect),
        _ => Err(ParseError::InvalidLiteral(func_str)),
    }
}
```

**Acceptance Criteria**:
- [ ] Aggregate expressions parse correctly
- [ ] DISTINCT flag is captured
- [ ] COUNT(*) works

---

#### Phase 4.6: Compiler - Aggregations
**File**: `src/gql/compiler.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Detect aggregates in RETURN clause
2. Use group() step for grouping
3. Apply aggregate functions

**Code**:

```rust
impl<'a, 'g> Compiler<'a, 'g> {
    fn has_aggregates(&self, return_clause: &ReturnClause) -> bool {
        return_clause.items.iter().any(|item| {
            matches!(&item.expression, Expression::Aggregate { .. })
        })
    }
    
    fn execute_aggregated_return(
        &self,
        return_clause: &ReturnClause,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<Vec<Value>, CompileError> {
        // Separate group-by expressions from aggregates
        let mut group_by_exprs = Vec::new();
        let mut aggregates = Vec::new();
        
        for item in &return_clause.items {
            match &item.expression {
                Expression::Aggregate { func, distinct, expr } => {
                    aggregates.push((item.alias.clone(), *func, *distinct, expr.as_ref()));
                }
                _ => {
                    group_by_exprs.push((&item.expression, item.alias.clone()));
                }
            }
        }
        
        if group_by_exprs.is_empty() {
            // No grouping - aggregate over all results
            self.execute_global_aggregates(aggregates, traversal)
        } else {
            // Group by non-aggregate expressions
            self.execute_grouped_aggregates(group_by_exprs, aggregates, traversal)
        }
    }
    
    fn execute_global_aggregates(
        &self,
        aggregates: Vec<(Option<String>, AggregateFunc, bool, &Expression)>,
        traversal: BoundTraversal<'g, (), Value>,
    ) -> Result<Vec<Value>, CompileError> {
        // Collect all results first
        let results = traversal.to_list();
        
        // Apply each aggregate
        let mut output = HashMap::new();
        
        for (alias, func, distinct, expr) in aggregates {
            let key = alias.unwrap_or_else(|| format!("{:?}", func));
            let value = self.compute_aggregate(func, distinct, expr, &results)?;
            output.insert(key, value);
        }
        
        Ok(vec![Value::Map(output)])
    }
    
    fn compute_aggregate(
        &self,
        func: AggregateFunc,
        distinct: bool,
        expr: &Expression,
        results: &[Value],
    ) -> Result<Value, CompileError> {
        // Extract values to aggregate
        let mut values: Vec<Value> = results.iter()
            .filter_map(|v| self.evaluate_expr_on_value(expr, v))
            .collect();
        
        if distinct {
            values.sort_by(|a, b| a.to_comparable().cmp(&b.to_comparable()));
            values.dedup();
        }
        
        match func {
            AggregateFunc::Count => Ok(Value::Int(values.len() as i64)),
            AggregateFunc::Sum => {
                let sum = values.iter()
                    .filter_map(|v| v.as_int().or_else(|| v.as_float().map(|f| f as i64)))
                    .sum::<i64>();
                Ok(Value::Int(sum))
            }
            AggregateFunc::Avg => {
                let sum: f64 = values.iter()
                    .filter_map(|v| v.as_float().or_else(|| v.as_int().map(|i| i as f64)))
                    .sum();
                let count = values.len() as f64;
                if count > 0.0 {
                    Ok(Value::Float(sum / count))
                } else {
                    Ok(Value::Null)
                }
            }
            AggregateFunc::Min => {
                values.into_iter()
                    .min_by(|a, b| a.to_comparable().cmp(&b.to_comparable()))
                    .ok_or(CompileError::EmptyPattern)
                    .or(Ok(Value::Null))
            }
            AggregateFunc::Max => {
                values.into_iter()
                    .max_by(|a, b| a.to_comparable().cmp(&b.to_comparable()))
                    .ok_or(CompileError::EmptyPattern)
                    .or(Ok(Value::Null))
            }
            AggregateFunc::Collect => {
                Ok(Value::List(values))
            }
        }
    }
}
```

**Acceptance Criteria**:
- [ ] `COUNT(*)` counts all matches
- [ ] `SUM(p.age)` sums numeric property
- [ ] `AVG(p.age)` computes average
- [ ] `COLLECT(friend.name)` collects into list
- [ ] `COUNT(DISTINCT p.city)` counts unique values

---

#### Phase 4.7: Integration Tests - ORDER BY, LIMIT, Aggregations
**File**: `tests/gql.rs`  
**Duration**: 1-2 hours

**Code**:

```rust
#[test]
fn test_gql_order_by() {
    let graph = Graph::new();
    
    {
        let mut writer = graph.write();
        writer.add_vertex("Person", [("name", "Alice"), ("age", 30)]);
        writer.add_vertex("Person", [("name", "Bob"), ("age", 25)]);
        writer.add_vertex("Person", [("name", "Carol"), ("age", 35)]);
        writer.commit();
    }
    
    let snapshot = graph.read();
    
    // Order by age ascending
    let results = snapshot.gql(
        "MATCH (p:Person) RETURN p.name ORDER BY p.age"
    ).unwrap();
    assert_eq!(results[0], Value::String("Bob".to_string()));
    assert_eq!(results[2], Value::String("Carol".to_string()));
    
    // Order by age descending
    let results = snapshot.gql(
        "MATCH (p:Person) RETURN p.name ORDER BY p.age DESC"
    ).unwrap();
    assert_eq!(results[0], Value::String("Carol".to_string()));
    assert_eq!(results[2], Value::String("Bob".to_string()));
}

#[test]
fn test_gql_limit_offset() {
    let graph = Graph::new();
    
    {
        let mut writer = graph.write();
        for i in 0..10 {
            writer.add_vertex("Person", [("name", format!("Person{}", i)), ("order", i as i64)]);
        }
        writer.commit();
    }
    
    let snapshot = graph.read();
    
    // Simple limit
    let results = snapshot.gql(
        "MATCH (p:Person) RETURN p ORDER BY p.order LIMIT 3"
    ).unwrap();
    assert_eq!(results.len(), 3);
    
    // Limit with offset
    let results = snapshot.gql(
        "MATCH (p:Person) RETURN p.name ORDER BY p.order LIMIT 3 OFFSET 5"
    ).unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn test_gql_aggregations() {
    let graph = Graph::new();
    
    {
        let mut writer = graph.write();
        writer.add_vertex("Person", [("name", "Alice"), ("age", 30), ("city", "NYC")]);
        writer.add_vertex("Person", [("name", "Bob"), ("age", 25), ("city", "LA")]);
        writer.add_vertex("Person", [("name", "Carol"), ("age", 35), ("city", "NYC")]);
        writer.commit();
    }
    
    let snapshot = graph.read();
    
    // COUNT
    let results = snapshot.gql(
        "MATCH (p:Person) RETURN COUNT(*)"
    ).unwrap();
    // Should return map with count
    if let Value::Map(m) = &results[0] {
        assert_eq!(m.get("COUNT").unwrap(), &Value::Int(3));
    }
    
    // SUM
    let results = snapshot.gql(
        "MATCH (p:Person) RETURN SUM(p.age)"
    ).unwrap();
    if let Value::Map(m) = &results[0] {
        assert_eq!(m.get("SUM").unwrap(), &Value::Int(90));
    }
    
    // COLLECT
    let results = snapshot.gql(
        "MATCH (p:Person) WHERE p.city = 'NYC' RETURN COLLECT(p.name)"
    ).unwrap();
    if let Value::Map(m) = &results[0] {
        if let Value::List(names) = m.get("COLLECT").unwrap() {
            assert_eq!(names.len(), 2);
        }
    }
}
```

**Acceptance Criteria**:
- [ ] ORDER BY tests pass
- [ ] LIMIT/OFFSET tests pass
- [ ] Aggregation tests pass

---

### Week 5: Variable-Length Paths and Polish

---

#### Phase 5.1: Variable-Length Path Compiler
**File**: `src/gql/compiler.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Compile path quantifiers to `repeat()` step
2. Handle `*` (any length), `*1..3` (range), `*2` (exact)
3. Use emit() for collecting intermediate results

**Code**:

```rust
fn compile_edge_with_quantifier(
    &mut self,
    edge: &EdgePattern,
    traversal: BoundTraversal<'g, (), Value>,
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    use crate::traversal::__;
    
    let quantifier = edge.quantifier.as_ref()
        .ok_or(CompileError::UnsupportedExpression)?;
    
    let labels: Vec<&str> = edge.labels.iter().map(|s| s.as_str()).collect();
    
    // Build anonymous traversal for repeat body
    let step = match edge.direction {
        EdgeDirection::Outgoing => {
            if labels.is_empty() { __::out() } else { __::out_labels(&labels) }
        }
        EdgeDirection::Incoming => {
            if labels.is_empty() { __::in_() } else { __::in_labels(&labels) }
        }
        EdgeDirection::Both => {
            if labels.is_empty() { __::both() } else { __::both_labels(&labels) }
        }
    };
    
    let min = quantifier.min.unwrap_or(1) as usize;
    let max = quantifier.max;
    
    // Build repeat with bounds
    let mut repeat_builder = traversal.repeat(step);
    
    // Set max iterations
    if let Some(max_val) = max {
        repeat_builder = repeat_builder.times(max_val as usize);
    }
    
    // Configure emit behavior based on min
    if min == 0 {
        // *0.. - emit from start
        repeat_builder = repeat_builder.emit();
    } else if min > 1 {
        // *2.. - emit after min iterations
        repeat_builder = repeat_builder.emit_after(min);
    }
    
    // If max is not set, need until condition (or default max to prevent infinite loops)
    if max.is_none() {
        // Default to max 10 iterations to prevent infinite loops
        repeat_builder = repeat_builder.times(10);
    }
    
    Ok(repeat_builder.build())
}
```

**Acceptance Criteria**:
- [ ] `*` compiles to repeat with default bounds
- [ ] `*2` compiles to exactly 2 iterations
- [ ] `*1..3` compiles to 1-3 iterations
- [ ] `*..5` compiles to 0-5 iterations

---

#### Phase 5.2: Integration Tests - Variable-Length Paths
**File**: `tests/gql.rs`  
**Duration**: 1-2 hours

**Code**:

```rust
#[test]
fn test_gql_variable_length_path() {
    let graph = Graph::new();
    
    {
        let mut writer = graph.write();
        let a = writer.add_vertex("Person", [("name", "Alice")]);
        let b = writer.add_vertex("Person", [("name", "Bob")]);
        let c = writer.add_vertex("Person", [("name", "Carol")]);
        let d = writer.add_vertex("Person", [("name", "Dave")]);
        
        writer.add_edge(a, b, "KNOWS", []);
        writer.add_edge(b, c, "KNOWS", []);
        writer.add_edge(c, d, "KNOWS", []);
        writer.commit();
    }
    
    let snapshot = graph.read();
    
    // Exactly 2 hops
    let results = snapshot.gql(
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS*2]->(target) RETURN target.name"
    ).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Carol".to_string()));
    
    // 1 to 3 hops
    let results = snapshot.gql(
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(target) RETURN DISTINCT target.name"
    ).unwrap();
    assert_eq!(results.len(), 3);  // Bob, Carol, Dave
    
    // Any number of hops (with implicit limit)
    let results = snapshot.gql(
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS*]->(target) RETURN DISTINCT target.name"
    ).unwrap();
    assert!(results.len() >= 3);
}

#[test]
fn test_gql_friends_of_friends() {
    let graph = Graph::new();
    
    {
        let mut writer = graph.write();
        let alice = writer.add_vertex("Person", [("name", "Alice")]);
        let bob = writer.add_vertex("Person", [("name", "Bob")]);
        let carol = writer.add_vertex("Person", [("name", "Carol")]);
        let dave = writer.add_vertex("Person", [("name", "Dave")]);
        
        // Alice knows Bob
        writer.add_edge(alice, bob, "KNOWS", []);
        // Bob knows Carol and Dave
        writer.add_edge(bob, carol, "KNOWS", []);
        writer.add_edge(bob, dave, "KNOWS", []);
        writer.commit();
    }
    
    let snapshot = graph.read();
    
    // Friends of friends (excluding direct friends)
    let results = snapshot.gql(
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS*2]->(fof:Person) \
         WHERE NOT (a)-[:KNOWS]->(fof) \
         RETURN DISTINCT fof.name"
    ).unwrap();
    // Should find Carol and Dave (friends of Bob, not direct friends of Alice)
    assert_eq!(results.len(), 2);
}
```

**Acceptance Criteria**:
- [ ] Exact hop count works
- [ ] Range bounds work
- [ ] Friends-of-friends pattern works

---

#### Phase 5.3: DISTINCT Support
**File**: `src/gql/compiler.rs`  
**Duration**: 1 hour

**Tasks**:
1. Parse DISTINCT keyword in RETURN
2. Apply dedup() step when DISTINCT is used

**Code**:

```rust
// Add to ReturnClause AST
#[derive(Debug, Clone)]
pub struct ReturnClause {
    pub distinct: bool,
    pub items: Vec<ReturnItem>,
}

// Update compiler
fn execute_return(
    &self,
    return_clause: &ReturnClause,
    traversal: BoundTraversal<'g, (), Value>,
) -> Result<Vec<Value>, CompileError> {
    let mut current = traversal;
    
    // Apply DISTINCT if specified
    if return_clause.distinct {
        current = current.dedup();
    }
    
    // ... rest of return handling
}
```

**Acceptance Criteria**:
- [ ] `RETURN DISTINCT p.city` deduplicates results
- [ ] Works with variable-length paths

---

#### Phase 5.4: Error Messages Improvement
**File**: `src/gql/error.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Add source spans to errors
2. Improve error messages with context
3. Add suggestions for common mistakes

**Code**:

```rust
#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Syntax error at position {position}: {message}")]
    SyntaxAt {
        position: usize,
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    
    #[error("Unexpected token '{found}' at position {position}, expected {expected}")]
    UnexpectedToken {
        position: usize,
        found: String,
        expected: String,
    },
    
    // ... other variants
}

#[derive(Debug, Error)]
pub enum CompileError {
    #[error("Undefined variable '{name}'. Did you forget to bind it in MATCH?")]
    UndefinedVariable { name: String },
    
    #[error("Variable '{name}' is already defined at position {original_position}")]
    DuplicateVariable {
        name: String,
        original_position: usize,
    },
    
    #[error("Aggregates like {func}() cannot be used in WHERE clause")]
    AggregateInWhere { func: String },
    
    // ... other variants
}
```

**Acceptance Criteria**:
- [ ] Errors include position information
- [ ] Error messages are helpful and actionable
- [ ] Common mistakes have suggestions

---

#### Phase 5.5: Documentation
**File**: `src/gql/mod.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Add module-level documentation
2. Add examples to public functions
3. Document supported GQL features and limitations

**Code**:

```rust
//! # GQL Parser and Compiler
//!
//! This module provides GQL (Graph Query Language) support for RustGremlin.
//!
//! ## Supported Features
//!
//! - **MATCH**: Pattern matching with nodes and edges
//!   - Node patterns: `(n:Person {name: 'Alice'})`
//!   - Edge patterns: `-[:KNOWS]->`, `<-[:KNOWS]-`, `-[:KNOWS]-`
//!   - Variable-length paths: `*`, `*2`, `*1..3`
//!
//! - **WHERE**: Filtering with expressions
//!   - Comparisons: `=`, `<>`, `<`, `<=`, `>`, `>=`
//!   - Logical: `AND`, `OR`, `NOT`
//!   - String: `CONTAINS`, `STARTS WITH`, `ENDS WITH`
//!   - Null checks: `IS NULL`, `IS NOT NULL`
//!   - List membership: `IN [...]`
//!
//! - **RETURN**: Result projection
//!   - Variables: `RETURN n`
//!   - Properties: `RETURN n.name`
//!   - Aliases: `RETURN n.name AS personName`
//!   - Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COLLECT`
//!
//! - **ORDER BY**: Sorting with `ASC`/`DESC`
//!
//! - **LIMIT/OFFSET**: Pagination
//!
//! ## Example
//!
//! ```rust
//! use rustgremlin::Graph;
//!
//! let graph = Graph::new();
//! // ... add data ...
//!
//! let snapshot = graph.read();
//! let results = snapshot.gql(r#"
//!     MATCH (p:Person)-[:KNOWS]->(friend:Person)
//!     WHERE p.age > 25
//!     RETURN p.name, friend.name
//!     ORDER BY p.age DESC
//!     LIMIT 10
//! "#)?;
//! ```
//!
//! ## Limitations
//!
//! - No CREATE/DELETE/SET mutations (use Rust API)
//! - No OPTIONAL MATCH
//! - No UNWIND, CASE, subqueries
//! - Single graph only (no multiple graph references)
```

**Acceptance Criteria**:
- [ ] Module documentation is complete
- [ ] All public functions have doc comments
- [ ] Examples compile and work

---

#### Phase 5.6: Comprehensive Test Suite
**File**: `tests/gql.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Add edge case tests
2. Add error handling tests
3. Add performance/stress tests

**Code**:

```rust
mod gql_comprehensive_tests {
    use super::*;
    
    // Edge cases
    
    #[test]
    fn test_empty_result() {
        let graph = Graph::new();
        let snapshot = graph.read();
        
        let results = snapshot.gql("MATCH (n:NonExistent) RETURN n").unwrap();
        assert!(results.is_empty());
    }
    
    #[test]
    fn test_case_insensitive_keywords() {
        let graph = Graph::new();
        {
            let mut w = graph.write();
            w.add_vertex("Person", [("name", "Test")]);
            w.commit();
        }
        let snapshot = graph.read();
        
        // All these should work
        let _ = snapshot.gql("MATCH (n:Person) RETURN n").unwrap();
        let _ = snapshot.gql("match (n:Person) return n").unwrap();
        let _ = snapshot.gql("Match (n:Person) Return n").unwrap();
    }
    
    #[test]
    fn test_multiple_labels() {
        let graph = Graph::new();
        {
            let mut w = graph.write();
            w.add_vertex("Person", [("name", "Alice")]);
            w.commit();
        }
        let snapshot = graph.read();
        
        // This tests label filtering (when supported)
        let results = snapshot.gql("MATCH (n:Person) RETURN n").unwrap();
        assert_eq!(results.len(), 1);
    }
    
    // Error cases
    
    #[test]
    fn test_syntax_error_missing_return() {
        let graph = Graph::new();
        let snapshot = graph.read();
        
        let result = snapshot.gql("MATCH (n:Person)");
        assert!(result.is_err());
    }
    
    #[test]
    fn test_semantic_error_undefined_var() {
        let graph = Graph::new();
        let snapshot = graph.read();
        
        let result = snapshot.gql("MATCH (n:Person) RETURN x");
        assert!(matches!(result, Err(GqlError::Compile(CompileError::UndefinedVariable(_)))));
    }
    
    // Complex queries
    
    #[test]
    fn test_complex_social_network_query() {
        let graph = create_social_network_graph();
        let snapshot = graph.read();
        
        let results = snapshot.gql(r#"
            MATCH (p:Person {city: 'NYC'})-[:KNOWS*1..2]->(friend:Person)
            WHERE friend.age >= 25 AND friend.age <= 35
            RETURN DISTINCT friend.name, friend.age
            ORDER BY friend.age DESC
            LIMIT 5
        "#).unwrap();
        
        assert!(results.len() <= 5);
        // Verify ordering
        // ...
    }
    
    fn create_social_network_graph() -> Graph {
        let graph = Graph::new();
        {
            let mut w = graph.write();
            // Create 20+ vertices with various properties
            // Create 50+ edges forming a social network
            // ...
            w.commit();
        }
        graph
    }
}
```

**Acceptance Criteria**:
- [ ] All edge case tests pass
- [ ] All error case tests pass
- [ ] Complex query tests pass
- [ ] Test coverage is comprehensive

---

#### Phase 5.7: Snapshot Tests
**File**: `tests/gql_snapshots.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Add insta snapshot tests for parser output
2. Add snapshot tests for error messages
3. Ensure stable test output

**Code**:

```rust
use insta::assert_yaml_snapshot;
use rustgremlin::gql::parse;

#[test]
fn test_parse_simple_match_snapshot() {
    let ast = parse("MATCH (n:Person) RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_complex_query_snapshot() {
    let ast = parse(r#"
        MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(friend:Person)
        WHERE friend.age > 25 AND friend.city = 'NYC'
        RETURN friend.name, friend.age
        ORDER BY friend.age DESC
        LIMIT 10
    "#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_error_snapshot() {
    let err = parse("MATCH (n:Person").unwrap_err();
    assert_yaml_snapshot!(format!("{}", err));
}
```

**Acceptance Criteria**:
- [ ] Snapshot tests created
- [ ] AST snapshots are stable
- [ ] Error message snapshots are stable

---

## Exit Criteria Checklist

### Parser
- [ ] Grammar parses all supported GQL constructs
- [ ] Parser builds correct AST
- [ ] Error messages include position information
- [ ] Case-insensitive keywords work

### Compiler
- [ ] Node patterns compile to `has_label()`, `has_value()`
- [ ] Edge patterns compile to `out()`, `in_()`, `both()`
- [ ] WHERE clause compiles to filter predicates
- [ ] RETURN clause handles variables, properties, aggregates
- [ ] ORDER BY compiles to `order()` step
- [ ] LIMIT/OFFSET compile to `limit()`, `skip()` steps
- [ ] Variable-length paths compile to `repeat()` step

### Public API
- [ ] `GraphSnapshot::gql()` method works
- [ ] Errors are properly typed and informative
- [ ] Documentation is complete

### Testing
- [ ] Unit tests for parser
- [ ] Unit tests for compiler
- [ ] Integration tests for full queries
- [ ] Snapshot tests for AST and errors
- [ ] Edge case tests pass

---

## File Summary

**New files**:
- `src/gql/mod.rs` - Module root and public API
- `src/gql/grammar.pest` - pest grammar
- `src/gql/ast.rs` - AST types
- `src/gql/parser.rs` - pest → AST conversion
- `src/gql/compiler.rs` - AST → traversal compilation
- `src/gql/error.rs` - Error types
- `tests/gql.rs` - Integration tests
- `tests/gql_snapshots.rs` - Snapshot tests

**Modified files**:
- `Cargo.toml` - Add pest, pest_derive, insta dependencies
- `src/lib.rs` - Add `pub mod gql;`
- `src/graph.rs` - Add `GraphSnapshot::gql()` method

---

## References

- `guiding-documents/gql.md` - GQL subset specification
- `guiding-documents/gql-to-ir-pipeline.md` - Architecture details
- `plans/plan-03.md` - Traversal engine (dependency)
- [pest book](https://pest.rs/book/) - Grammar reference
- [ISO GQL](https://www.iso.org/standard/76120.html) - Full specification

