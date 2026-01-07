# RustGremlin: GQL Subset Implementation

**Note**: This document describes a **Phase 2 feature**. The initial Phase 1 implementation focuses on the core Gremlin-style fluent API with dual storage architecture (in-memory + memory-mapped), simple RwLock-based concurrency, and essential traversal operations. GQL support will be added after the core traversal engine is stable and well-tested. See the [Roadmap](./overview.md#5-roadmap) section in overview.md for the complete development timeline.

---

A practical subset of ISO GQL (Graph Query Language) that maps cleanly to our traversal engine.

---

## 1. Why a Subset?

Full GQL is a 600+ page ISO standard with features we don't need:

| Feature | Full GQL | Our Subset | Reason |
|---------|----------|------------|--------|
| Basic pattern matching | ✅ | ✅ | Core functionality |
| Variable-length paths | ✅ | ✅ | Common use case |
| WHERE clauses | ✅ | ✅ | Essential filtering |
| RETURN projections | ✅ | ✅ | Essential output |
| Aggregations | ✅ | ✅ | Analytics |
| ORDER BY / LIMIT | ✅ | ✅ | Pagination |
| Graph construction | ✅ | ❌ | Rarely needed |
| Multiple graphs | ✅ | ❌ | Single graph for now |
| Graph types | ✅ | ❌ | Complex type system |
| Procedures | ✅ | ❌ | Can add later |
| Temporal features | ✅ | ❌ | Specialized |

**Goal**: Support the 80% of queries users actually write with 20% of the spec.

---

## 2. Supported Grammar

### 2.1 Query Structure

```
query         → match_clause where_clause? return_clause order_clause? limit_clause?

match_clause  → "MATCH" pattern ("," pattern)*
where_clause  → "WHERE" expression
return_clause → "RETURN" return_item ("," return_item)*
order_clause  → "ORDER" "BY" order_item ("," order_item)*
limit_clause  → "LIMIT" integer ("OFFSET" integer)?

return_item   → expression ("AS" identifier)?
order_item    → expression ("ASC" | "DESC")?
```

### 2.2 Pattern Syntax

```
pattern       → path_pattern
path_pattern  → node_pattern (edge_pattern node_pattern)*

node_pattern  → "(" variable? label_filter? property_filter? ")"
edge_pattern  → left_arrow? "-[" variable? label_filter? quantifier? "]-" right_arrow?

left_arrow    → "<"
right_arrow   → ">"

label_filter  → ":" identifier (":" identifier)*
property_filter → "{" property ("," property)* "}"
property      → identifier ":" literal

quantifier    → "*" range?
range         → integer? ".." integer?
              | integer
```

### 2.3 Expressions

```
expression    → or_expr
or_expr       → and_expr ("OR" and_expr)*
and_expr      → not_expr ("AND" not_expr)*
not_expr      → "NOT"? comparison
comparison    → term (comp_op term)?
term          → factor (("+" | "-") factor)*
factor        → unary (("*" | "/") unary)*
unary         → "-"? primary

primary       → literal
              | variable
              | property_access
              | function_call
              | "(" expression ")"
              | list_literal
              | aggregate

property_access → variable "." identifier
function_call   → identifier "(" (expression ("," expression)*)? ")"
aggregate       → agg_func "(" "DISTINCT"? expression ")"
agg_func        → "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "COLLECT"

comp_op       → "=" | "<>" | "!=" | "<" | "<=" | ">" | ">=" 
              | "CONTAINS" | "STARTS WITH" | "ENDS WITH"
              | "IN" | "IS NULL" | "IS NOT NULL"

literal       → string | integer | float | "TRUE" | "FALSE" | "NULL"
list_literal  → "[" (expression ("," expression)*)? "]"
```

---

## 3. Example Queries

### Simple Patterns

```sql
-- Find all people
MATCH (p:Person)
RETURN p.name, p.age

-- Find Alice
MATCH (p:Person {name: 'Alice'})
RETURN p

-- Find Alice's friends
MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(friend:Person)
RETURN friend.name
```

### Filtering

```sql
-- People over 30
MATCH (p:Person)
WHERE p.age > 30
RETURN p.name, p.age
ORDER BY p.age DESC

-- Multiple conditions
MATCH (p:Person)-[:LIVES_IN]->(c:City)
WHERE p.age >= 25 AND p.age <= 40 AND c.name = 'NYC'
RETURN p.name, c.name

-- Pattern in WHERE
MATCH (p:Person)
WHERE (p)-[:KNOWS]->(:Person {name: 'Alice'})
RETURN p.name
```

### Variable-Length Paths

```sql
-- Friends of friends (exactly 2 hops)
MATCH (a:Person {name: 'Alice'})-[:KNOWS*2]->(fof:Person)
RETURN DISTINCT fof.name

-- Reachable within 1-3 hops
MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(reachable:Person)
RETURN DISTINCT reachable.name

-- Any path length
MATCH (a:Person {name: 'Alice'})-[:KNOWS*]->(connected:Person)
RETURN DISTINCT connected.name
```

### Aggregations

```sql
-- Count friends per person
MATCH (p:Person)-[:KNOWS]->(friend:Person)
RETURN p.name, COUNT(friend) AS friend_count
ORDER BY friend_count DESC
LIMIT 10

-- Average age by city
MATCH (p:Person)-[:LIVES_IN]->(c:City)
RETURN c.name, AVG(p.age) AS avg_age, COUNT(p) AS population

-- Collect friends into list
MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(friend:Person)
RETURN p.name, COLLECT(friend.name) AS friends
```

### Multiple Patterns

```sql
-- Mutual friends
MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(mutual:Person),
      (b:Person {name: 'Bob'})-[:KNOWS]->(mutual)
RETURN mutual.name

-- Triangle pattern
MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(a)
RETURN a.name, b.name, c.name
```

### Edge Properties

```sql
-- Relationships with properties
MATCH (a:Person)-[k:KNOWS {since: 2020}]->(b:Person)
RETURN a.name, b.name, k.since

-- Filter on edge property
MATCH (a:Person)-[k:KNOWS]->(b:Person)
WHERE k.since >= 2015
RETURN a.name, b.name, k.since
```

---

## 4. AST Definition

```rust
pub mod ast {
    /// Complete query
    #[derive(Debug, Clone)]
    pub struct Query {
        pub match_clause: MatchClause,
        pub where_clause: Option<WhereClause>,
        pub return_clause: ReturnClause,
        pub order_clause: Option<OrderClause>,
        pub limit_clause: Option<LimitClause>,
    }
    
    /// MATCH clause with one or more patterns
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
    
    /// Edge pattern: -[variable:TYPE*1..3]->
    #[derive(Debug, Clone)]
    pub struct EdgePattern {
        pub variable: Option<String>,
        pub labels: Vec<String>,
        pub direction: EdgeDirection,
        pub quantifier: Option<PathQuantifier>,
        pub properties: Vec<(String, Literal)>,
    }
    
    #[derive(Debug, Clone, Copy)]
    pub enum EdgeDirection {
        Outgoing,   // -->
        Incoming,   // <--
        Both,       // --
    }
    
    #[derive(Debug, Clone)]
    pub struct PathQuantifier {
        pub min: Option<u32>,
        pub max: Option<u32>,
    }
    
    /// WHERE clause
    #[derive(Debug, Clone)]
    pub struct WhereClause {
        pub expression: Expression,
    }
    
    /// Expression types
    #[derive(Debug, Clone)]
    pub enum Expression {
        // Literals
        Literal(Literal),
        
        // Variable reference
        Variable(String),
        
        // Property access: a.name
        Property {
            variable: String,
            property: String,
        },
        
        // Binary operations
        BinaryOp {
            left: Box<Expression>,
            op: BinaryOperator,
            right: Box<Expression>,
        },
        
        // Unary operations
        UnaryOp {
            op: UnaryOperator,
            expr: Box<Expression>,
        },
        
        // Function call: func(args)
        FunctionCall {
            name: String,
            args: Vec<Expression>,
        },
        
        // Aggregate: COUNT(DISTINCT x)
        Aggregate {
            func: AggregateFunc,
            distinct: bool,
            expr: Box<Expression>,
        },
        
        // List: [1, 2, 3]
        List(Vec<Expression>),
        
        // Pattern existence: (a)-[:KNOWS]->(b)
        PatternExpr(Pattern),
        
        // IS NULL / IS NOT NULL
        IsNull {
            expr: Box<Expression>,
            negated: bool,
        },
        
        // IN list
        InList {
            expr: Box<Expression>,
            list: Vec<Expression>,
            negated: bool,
        },
    }
    
    #[derive(Debug, Clone, Copy)]
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
    
    #[derive(Debug, Clone, Copy)]
    pub enum UnaryOperator {
        Not,
        Neg,
    }
    
    #[derive(Debug, Clone, Copy)]
    pub enum AggregateFunc {
        Count,
        Sum,
        Avg,
        Min,
        Max,
        Collect,
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
    
    /// ORDER BY clause
    #[derive(Debug, Clone)]
    pub struct OrderClause {
        pub items: Vec<OrderItem>,
    }
    
    #[derive(Debug, Clone)]
    pub struct OrderItem {
        pub expression: Expression,
        pub descending: bool,
    }
    
    /// LIMIT clause
    #[derive(Debug, Clone)]
    pub struct LimitClause {
        pub limit: u64,
        pub offset: Option<u64>,
    }
    
    /// Literal values for GQL AST.
    ///
    /// Note: This mirrors `crate::value::Value` but is limited to the subset
    /// of types that can appear as literals in GQL queries. During compilation,
    /// these are converted to `Value` instances for use in the traversal engine.
    #[derive(Debug, Clone)]
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
}
```

---

## 5. Parser Implementation

Using `pest` for the parser (cleaner grammar definition):

### 5.1 Grammar File (gql.pest)

```pest
// gql.pest - GQL subset grammar

WHITESPACE = _{ " " | "\t" | "\r" | "\n" }
COMMENT = _{ "//" ~ (!"\n" ~ ANY)* | "/*" ~ (!"*/" ~ ANY)* ~ "*/" }

// Keywords (case-insensitive)
MATCH    = { ^"match" }
WHERE    = { ^"where" }
RETURN   = { ^"return" }
ORDER    = { ^"order" }
BY       = { ^"by" }
LIMIT    = { ^"limit" }
OFFSET   = { ^"offset" }
AS       = { ^"as" }
AND      = { ^"and" }
OR       = { ^"or" }
NOT      = { ^"not" }
IN       = { ^"in" }
IS       = { ^"is" }
NULL     = { ^"null" }
TRUE     = { ^"true" }
FALSE    = { ^"false" }
DISTINCT = { ^"distinct" }
ASC      = { ^"asc" }
DESC     = { ^"desc" }
CONTAINS = { ^"contains" }
STARTS   = { ^"starts" }
ENDS     = { ^"ends" }
WITH     = { ^"with" }
COUNT    = { ^"count" }
SUM      = { ^"sum" }
AVG      = { ^"avg" }
MIN      = { ^"min" }
MAX      = { ^"max" }
COLLECT  = { ^"collect" }

// Entry point
query = { SOI ~ match_clause ~ where_clause? ~ return_clause ~ order_clause? ~ limit_clause? ~ EOI }

// MATCH clause
match_clause = { MATCH ~ pattern ~ ("," ~ pattern)* }

pattern = { node_pattern ~ (edge_pattern ~ node_pattern)* }

node_pattern = { "(" ~ variable? ~ label_filter? ~ property_filter? ~ ")" }

edge_pattern = { 
    left_arrow? ~ "-[" ~ variable? ~ label_filter? ~ quantifier? ~ property_filter? ~ "]-" ~ right_arrow?
}

left_arrow = { "<" }
right_arrow = { ">" }

variable = @{ ASCII_ALPHA ~ (ASCII_ALPHANUMERIC | "_")* }

label_filter = { (":" ~ identifier)+ }

identifier = @{ ASCII_ALPHA ~ (ASCII_ALPHANUMERIC | "_")* }

property_filter = { "{" ~ property ~ ("," ~ property)* ~ "}" }

property = { identifier ~ ":" ~ literal }

quantifier = { "*" ~ range? }

range = { integer? ~ ".." ~ integer? | integer }

// WHERE clause
where_clause = { WHERE ~ expression }

// Expressions with precedence
expression = { or_expr }

or_expr = { and_expr ~ (OR ~ and_expr)* }

and_expr = { not_expr ~ (AND ~ not_expr)* }

not_expr = { NOT? ~ comparison }

comparison = { additive ~ (comp_op ~ additive)? | is_null_expr | in_expr }

is_null_expr = { additive ~ IS ~ NOT? ~ NULL }

in_expr = { additive ~ NOT? ~ IN ~ "[" ~ expression ~ ("," ~ expression)* ~ "]" }

comp_op = { "<>" | "!=" | "<=" | ">=" | "=" | "<" | ">" | CONTAINS | starts_with | ends_with }

starts_with = { STARTS ~ WITH }
ends_with = { ENDS ~ WITH }

additive = { multiplicative ~ (("+"|"-") ~ multiplicative)* }

multiplicative = { unary ~ (("*"|"/"|"%") ~ unary)* }

unary = { "-"? ~ primary }

primary = { 
    literal
    | aggregate
    | function_call
    | property_access
    | variable
    | "(" ~ expression ~ ")"
    | list_expr
    | pattern_expr
}

property_access = { variable ~ "." ~ identifier }

function_call = { identifier ~ "(" ~ (expression ~ ("," ~ expression)*)? ~ ")" }

aggregate = { agg_func ~ "(" ~ DISTINCT? ~ expression ~ ")" }

agg_func = { COUNT | SUM | AVG | MIN | MAX | COLLECT }

list_expr = { "[" ~ (expression ~ ("," ~ expression)*)? ~ "]" }

pattern_expr = { "(" ~ pattern ~ ")" }

// RETURN clause
return_clause = { RETURN ~ return_item ~ ("," ~ return_item)* }

return_item = { expression ~ (AS ~ identifier)? }

// ORDER BY clause
order_clause = { ORDER ~ BY ~ order_item ~ ("," ~ order_item)* }

order_item = { expression ~ (ASC | DESC)? }

// LIMIT clause
limit_clause = { LIMIT ~ integer ~ (OFFSET ~ integer)? }

// Literals
literal = { string | float | integer | TRUE | FALSE | NULL }

string = ${ "'" ~ string_inner ~ "'" }
string_inner = @{ (!"'" ~ ANY | "''")* }

integer = @{ "-"? ~ ASCII_DIGIT+ }

float = @{ "-"? ~ ASCII_DIGIT+ ~ "." ~ ASCII_DIGIT+ }
```

### 5.2 Parser Code

```rust
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "gql.pest"]
pub struct GqlParser;

pub fn parse(input: &str) -> Result<Query, ParseError> {
    let pairs = GqlParser::parse(Rule::query, input)
        .map_err(|e| ParseError::Syntax(e.to_string()))?;
    
    let query_pair = pairs.into_iter().next().unwrap();
    build_query(query_pair)
}

fn build_query(pair: pest::iterators::Pair<Rule>) -> Result<Query, ParseError> {
    let mut match_clause = None;
    let mut where_clause = None;
    let mut return_clause = None;
    let mut order_clause = None;
    let mut limit_clause = None;
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::match_clause => match_clause = Some(build_match_clause(inner)?),
            Rule::where_clause => where_clause = Some(build_where_clause(inner)?),
            Rule::return_clause => return_clause = Some(build_return_clause(inner)?),
            Rule::order_clause => order_clause = Some(build_order_clause(inner)?),
            Rule::limit_clause => limit_clause = Some(build_limit_clause(inner)?),
            Rule::EOI => {}
            _ => {}
        }
    }
    
    Ok(Query {
        match_clause: match_clause.ok_or(ParseError::MissingMatch)?,
        where_clause,
        return_clause: return_clause.ok_or(ParseError::MissingReturn)?,
        order_clause,
        limit_clause,
    })
}

fn build_match_clause(pair: pest::iterators::Pair<Rule>) -> Result<MatchClause, ParseError> {
    let patterns = pair.into_inner()
        .filter(|p| p.as_rule() == Rule::pattern)
        .map(build_pattern)
        .collect::<Result<Vec<_>, _>>()?;
    
    Ok(MatchClause { patterns })
}

fn build_pattern(pair: pest::iterators::Pair<Rule>) -> Result<Pattern, ParseError> {
    let mut elements = Vec::new();
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::node_pattern => elements.push(PatternElement::Node(build_node_pattern(inner)?)),
            Rule::edge_pattern => elements.push(PatternElement::Edge(build_edge_pattern(inner)?)),
            _ => {}
        }
    }
    
    Ok(Pattern { elements })
}

fn build_node_pattern(pair: pest::iterators::Pair<Rule>) -> Result<NodePattern, ParseError> {
    let mut variable = None;
    let mut labels = Vec::new();
    let mut properties = Vec::new();
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::variable => variable = Some(inner.as_str().to_string()),
            Rule::label_filter => {
                for label in inner.into_inner() {
                    if label.as_rule() == Rule::identifier {
                        labels.push(label.as_str().to_string());
                    }
                }
            }
            Rule::property_filter => {
                properties = build_properties(inner)?;
            }
            _ => {}
        }
    }
    
    Ok(NodePattern { variable, labels, properties })
}

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
        (false, true) => EdgeDirection::Outgoing,
        (true, false) => EdgeDirection::Incoming,
        _ => EdgeDirection::Both,
    };
    
    Ok(EdgePattern { variable, labels, direction, quantifier, properties })
}

fn build_quantifier(pair: pest::iterators::Pair<Rule>) -> Result<PathQuantifier, ParseError> {
    let mut min = None;
    let mut max = None;
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::range => {
                let range_str = inner.as_str();
                if range_str.contains("..") {
                    let parts: Vec<&str> = range_str.split("..").collect();
                    if !parts[0].is_empty() {
                        min = Some(parts[0].parse().unwrap());
                    }
                    if parts.len() > 1 && !parts[1].is_empty() {
                        max = Some(parts[1].parse().unwrap());
                    }
                } else {
                    let n: u32 = range_str.parse().unwrap();
                    min = Some(n);
                    max = Some(n);
                }
            }
            Rule::integer => {
                let n: u32 = inner.as_str().parse().unwrap();
                min = Some(n);
                max = Some(n);
            }
            _ => {}
        }
    }
    
    Ok(PathQuantifier { min, max })
}

// ... more builder functions for expressions, etc.
```

---

## 6. Compilation to Traversal

### 6.1 Query Planner

The query planner compiles GQL queries to RustGremlin's traversal API. The
actual implementation will use `BoundTraversal` which provides the fluent API.

**Key API mappings:**
- `has_label_any(labels)` - Filter by multiple labels (takes `&[&str]`)
- `has_value(key, value)` - Filter by property value (key: `impl Into<String>`, value: `impl Into<Value>`)
- `out()` / `out_labels(&[&str])` - Navigate outgoing edges
- `in_()` / `in_labels(&[&str])` - Navigate incoming edges
- `both()` / `both_labels(&[&str])` - Navigate both directions
- `as_(label)` - Bind current position to a label for later reference
- `select(labels)` - Retrieve bound values by label

```rust
pub struct QueryPlanner<'g> {
    snapshot: &'g GraphSnapshot<'g>,
}

impl<'g> QueryPlanner<'g> {
    pub fn compile(&self, query: Query) -> Result<CompiledQuery<'g>, CompileError> {
        // 1. Analyze patterns to determine start point
        let start = self.choose_start_point(&query)?;
        
        // 2. Build traversal from patterns
        let traversal = self.build_traversal(&query, start)?;
        
        // 3. Add WHERE filter
        let traversal = self.add_where_filter(traversal, &query.where_clause)?;
        
        // 4. Build projection
        let projection = self.build_projection(&query.return_clause)?;
        
        // 5. Add ORDER BY
        let ordering = self.build_ordering(&query.order_clause)?;
        
        // 6. Add LIMIT
        let limit = query.limit_clause.as_ref().map(|l| (l.limit, l.offset));
        
        Ok(CompiledQuery {
            traversal,
            projection,
            ordering,
            limit,
        })
    }
    
    fn choose_start_point(&self, query: &Query) -> Result<StartPoint, CompileError> {
        // Find the most selective starting pattern
        // Priority:
        // 1. Node with property filter that has an index
        // 2. Node with label filter
        // 3. First node in pattern
        
        for pattern in &query.match_clause.patterns {
            for element in &pattern.elements {
                if let PatternElement::Node(node) = element {
                    // Check for indexed property (future feature)
                    // for (key, value) in &node.properties {
                    //     if self.snapshot.has_index(&node.labels, key) { ... }
                    // }
                    
                    // Check for label
                    if !node.labels.is_empty() {
                        return Ok(StartPoint::LabelScan {
                            labels: node.labels.clone(),
                            variable: node.variable.clone(),
                        });
                    }
                }
            }
        }
        
        // Fall back to full scan
        Ok(StartPoint::FullScan)
    }
    
    fn build_traversal(
        &self,
        query: &Query,
        start: StartPoint,
    ) -> Result<CompiledTraversal<'g>, CompileError> {
        let g = self.snapshot.traversal();
        
        // Start traversal based on start point
        // Note: BoundTraversal<'g, In, Out> is the actual type
        let mut traversal = match start {
            StartPoint::LabelScan { ref labels, .. } => {
                let label_refs: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                g.v().has_label_any(&label_refs)
            }
            StartPoint::FullScan => {
                g.v()
            }
        };
        
        // Build pattern matching
        let mut bindings: HashMap<String, BindingType> = HashMap::new();
        
        for pattern in &query.match_clause.patterns {
            // Pattern compilation modifies traversal in place
            // ... compile_pattern logic here
        }
        
        Ok(CompiledTraversal { /* ... */ })
    }
}

// Example pattern compilation (pseudocode showing actual API usage):
fn compile_pattern_example<'g>(
    traversal: BoundTraversal<'g, (), Value>,
    node: &NodePattern,
    edge: &EdgePattern,
    next_node: &NodePattern,
) -> BoundTraversal<'g, (), Value> {
    // Filter by node labels
    let label_refs: Vec<&str> = node.labels.iter().map(|s| s.as_str()).collect();
    let t = if !label_refs.is_empty() {
        traversal.has_label_any(&label_refs)
    } else {
        traversal
    };
    
    // Apply property filters using has_value (key, value)
    let t = node.properties.iter().fold(t, |acc, (key, value)| {
        // value.clone().into() converts Literal -> Value
        acc.has_value(key.clone(), value.clone())
    });
    
    // Bind variable with as_()
    let t = if let Some(var) = &node.variable {
        t.as_(var.clone())
    } else {
        t
    };
    
    // Navigate based on edge direction
    let edge_labels: Vec<&str> = edge.labels.iter().map(|s| s.as_str()).collect();
    let t = match edge.direction {
        EdgeDirection::Outgoing => {
            if edge_labels.is_empty() {
                t.out()
            } else {
                t.out_labels(&edge_labels)
            }
        }
        EdgeDirection::Incoming => {
            if edge_labels.is_empty() {
                t.in_()
            } else {
                t.in_labels(&edge_labels)
            }
        }
        EdgeDirection::Both => {
            if edge_labels.is_empty() {
                t.both()
            } else {
                t.both_labels(&edge_labels)
            }
        }
    };
    
    // Apply next node filters
    let next_label_refs: Vec<&str> = next_node.labels.iter().map(|s| s.as_str()).collect();
    let t = if !next_label_refs.is_empty() {
        t.has_label_any(&next_label_refs)
    } else {
        t
    };
    
    // Bind next node variable
    if let Some(var) = &next_node.variable {
        t.as_(var.clone())
    } else {
        t
    }
}

// Variable-length path compilation using repeat():
fn compile_variable_path_example<'g>(
    traversal: BoundTraversal<'g, (), Value>,
    edge: &EdgePattern,
    quantifier: &PathQuantifier,
) -> BoundTraversal<'g, (), Value> {
    use crate::traversal::__;  // Anonymous traversal factory
    
    let edge_labels: Vec<&str> = edge.labels.iter().map(|s| s.as_str()).collect();
    
    // Build the anonymous traversal for the repeat body
    let step = match edge.direction {
        EdgeDirection::Outgoing => {
            if edge_labels.is_empty() {
                __::out()
            } else {
                __::out_labels(&edge_labels)
            }
        }
        EdgeDirection::Incoming => {
            if edge_labels.is_empty() {
                __::in_()
            } else {
                __::in_labels(&edge_labels)
            }
        }
        EdgeDirection::Both => {
            if edge_labels.is_empty() {
                __::both()
            } else {
                __::both_labels(&edge_labels)
            }
        }
    };
    
    // Apply repeat with bounds using the builder pattern
    let min = quantifier.min.unwrap_or(1) as usize;
    let max = quantifier.max.map(|m| m as usize);
    
    let repeat_builder = traversal.repeat(step);
    
    // Configure repeat bounds
    let repeat_builder = if let Some(max) = max {
        repeat_builder.times(max)
    } else {
        repeat_builder
    };
    
    // Configure emit behavior
    if min > 0 {
        repeat_builder.emit_after(min)
    } else {
        repeat_builder.emit()
    }
}
```

### 6.2 Expression Compiler

The expression compiler converts GQL expressions to Rust closures that operate
on traverser context. This integrates with the existing `Value` type system.

```rust
use crate::value::Value;

impl<'g> QueryPlanner<'g> {
    /// Compile a GQL expression to a closure that evaluates against traverser context.
    fn compile_expression(
        &self,
        expr: &Expression,
        bindings: &HashMap<String, BindingType>,
    ) -> Result<Box<dyn Fn(&TraverserContext) -> Value>, CompileError> {
        match expr {
            Expression::Literal(lit) => {
                // Convert AST Literal to Value
                let value: Value = lit.clone().into();
                Ok(Box::new(move |_| value.clone()))
            }
            
            Expression::Variable(name) => {
                let name = name.clone();
                Ok(Box::new(move |ctx| {
                    // Use select() semantics to retrieve bound value
                    ctx.get_binding(&name)
                        .map(|v| v.clone())
                        .unwrap_or(Value::Null)
                }))
            }
            
            Expression::Property { variable, property } => {
                let var = variable.clone();
                let prop = property.clone();
                Ok(Box::new(move |ctx| {
                    ctx.get_binding(&var)
                        .and_then(|element| ctx.get_property(element, &prop))
                        .unwrap_or(Value::Null)
                }))
            }
            
            Expression::BinaryOp { left, op, right } => {
                let left_fn = self.compile_expression(left, bindings)?;
                let right_fn = self.compile_expression(right, bindings)?;
                let op = *op;
                
                Ok(Box::new(move |ctx| {
                    let l = left_fn(ctx);
                    let r = right_fn(ctx);
                    apply_binary_op(op, l, r)
                }))
            }
            
            Expression::Aggregate { .. } => {
                // Aggregates are handled separately during projection
                Err(CompileError::AggregateInWhere)
            }
            
            // ... more cases
            _ => Err(CompileError::UnsupportedExpression),
        }
    }
    
    /// Compile a WHERE clause to a filter predicate.
    ///
    /// The resulting predicate can be passed to traversal.filter().
    fn compile_where_predicate(
        &self,
        expr: &Expression,
        bindings: &HashMap<String, BindingType>,
    ) -> Result<Box<dyn Fn(&TraverserContext) -> bool>, CompileError> {
        let eval = self.compile_expression(expr, bindings)?;
        Ok(Box::new(move |ctx| {
            match eval(ctx) {
                Value::Bool(b) => b,
                _ => false,
            }
        }))
    }
}

/// Apply a binary operator to two Values.
///
/// Uses the existing Value type's semantics for comparisons.
fn apply_binary_op(op: BinaryOperator, left: Value, right: Value) -> Value {
    match op {
        BinaryOperator::Eq => Value::Bool(left == right),
        BinaryOperator::Neq => Value::Bool(left != right),
        BinaryOperator::Lt => {
            // Use ComparableValue for ordering
            let l = left.to_comparable();
            let r = right.to_comparable();
            Value::Bool(l < r)
        }
        BinaryOperator::Lte => {
            let l = left.to_comparable();
            let r = right.to_comparable();
            Value::Bool(l <= r)
        }
        BinaryOperator::Gt => {
            let l = left.to_comparable();
            let r = right.to_comparable();
            Value::Bool(l > r)
        }
        BinaryOperator::Gte => {
            let l = left.to_comparable();
            let r = right.to_comparable();
            Value::Bool(l >= r)
        }
        BinaryOperator::And => {
            Value::Bool(left.as_bool().unwrap_or(false) && right.as_bool().unwrap_or(false))
        }
        BinaryOperator::Or => {
            Value::Bool(left.as_bool().unwrap_or(false) || right.as_bool().unwrap_or(false))
        }
        BinaryOperator::Add => {
            match (left, right) {
                (Value::Int(a), Value::Int(b)) => Value::Int(a + b),
                (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
                (Value::Int(a), Value::Float(b)) => Value::Float(a as f64 + b),
                (Value::Float(a), Value::Int(b)) => Value::Float(a + b as f64),
                (Value::String(a), Value::String(b)) => Value::String(a + &b),
                _ => Value::Null,
            }
        }
        BinaryOperator::Sub => {
            match (left, right) {
                (Value::Int(a), Value::Int(b)) => Value::Int(a - b),
                (Value::Float(a), Value::Float(b)) => Value::Float(a - b),
                (Value::Int(a), Value::Float(b)) => Value::Float(a as f64 - b),
                (Value::Float(a), Value::Int(b)) => Value::Float(a - b as f64),
                _ => Value::Null,
            }
        }
        BinaryOperator::Mul => {
            match (left, right) {
                (Value::Int(a), Value::Int(b)) => Value::Int(a * b),
                (Value::Float(a), Value::Float(b)) => Value::Float(a * b),
                (Value::Int(a), Value::Float(b)) => Value::Float(a as f64 * b),
                (Value::Float(a), Value::Int(b)) => Value::Float(a * b as f64),
                _ => Value::Null,
            }
        }
        BinaryOperator::Div => {
            match (left, right) {
                (Value::Int(a), Value::Int(b)) if b != 0 => Value::Int(a / b),
                (Value::Float(a), Value::Float(b)) => Value::Float(a / b),
                (Value::Int(a), Value::Float(b)) => Value::Float(a as f64 / b),
                (Value::Float(a), Value::Int(b)) => Value::Float(a / b as f64),
                _ => Value::Null,
            }
        }
        BinaryOperator::Mod => {
            match (left, right) {
                (Value::Int(a), Value::Int(b)) if b != 0 => Value::Int(a % b),
                _ => Value::Null,
            }
        }
        BinaryOperator::Contains => {
            match (left, right) {
                (Value::String(haystack), Value::String(needle)) => {
                    Value::Bool(haystack.contains(&needle))
                }
                (Value::List(items), needle) => {
                    Value::Bool(items.contains(&needle))
                }
                _ => Value::Bool(false),
            }
        }
        BinaryOperator::StartsWith => {
            match (left, right) {
                (Value::String(s), Value::String(prefix)) => {
                    Value::Bool(s.starts_with(&prefix))
                }
                _ => Value::Bool(false),
            }
        }
        BinaryOperator::EndsWith => {
            match (left, right) {
                (Value::String(s), Value::String(suffix)) => {
                    Value::Bool(s.ends_with(&suffix))
                }
                _ => Value::Bool(false),
            }
        }
    }
}
```

---

## 7. Query Execution

```rust
pub struct CompiledQuery<'g> {
    traversal: Box<dyn Traversal + 'g>,
    projection: Projection,
    ordering: Option<Ordering>,
    limit: Option<(u64, Option<u64>)>,
}

impl<'g> CompiledQuery<'g> {
    pub fn execute(self) -> QueryResult {
        let mut results: Vec<Row> = Vec::new();
        
        // Execute traversal and collect bindings
        for traverser in self.traversal {
            let row = self.projection.project(&traverser);
            results.push(row);
        }
        
        // Apply ordering
        if let Some(ordering) = &self.ordering {
            results.sort_by(|a, b| ordering.compare(a, b));
        }
        
        // Apply limit/offset
        if let Some((limit, offset)) = self.limit {
            let start = offset.unwrap_or(0) as usize;
            let end = (start + limit as usize).min(results.len());
            results = results[start..end].to_vec();
        }
        
        QueryResult { rows: results }
    }
}

pub struct Projection {
    items: Vec<ProjectionItem>,
}

pub struct ProjectionItem {
    eval: Box<dyn Fn(&TraverserContext) -> Value>,
    alias: String,
}

impl Projection {
    fn project(&self, traverser: &Traverser) -> Row {
        let ctx = TraverserContext::from(traverser);
        let values: Vec<(String, Value)> = self.items
            .iter()
            .map(|item| (item.alias.clone(), (item.eval)(&ctx)))
            .collect();
        Row { values }
    }
}
```

---

## 8. Usage Example

```rust
use rustgremlin::gql;

fn main() -> Result<(), Box<dyn Error>> {
    let graph = Graph::open("social.db")?;
    
    // Parse and execute GQL query
    let results = graph.gql(r#"
        MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(friend:Person)
        WHERE friend.age > 25
        RETURN friend.name, friend.age
        ORDER BY friend.age DESC
        LIMIT 10
    "#)?;
    
    for row in results {
        println!("{}: {}", row.get("name"), row.get("age"));
    }
    
    // Parameterized query
    let results = graph.gql_with_params(
        r#"
            MATCH (p:Person)-[:KNOWS*1..3]->(friend:Person)
            WHERE p.name = $name AND friend.city = $city
            RETURN DISTINCT friend.name
        "#,
        params! {
            "name" => "Alice",
            "city" => "NYC"
        }
    )?;
    
    // Prepared statement for repeated execution
    let stmt = graph.prepare_gql(r#"
        MATCH (p:Person {name: $name})-[:KNOWS]->(f:Person)
        RETURN f.name, f.age
    "#)?;
    
    for name in ["Alice", "Bob", "Carol"] {
        let results = stmt.execute(params! { "name" => name })?;
        println!("{}: {} friends", name, results.len());
    }
    
    Ok(())
}
```

---

## 9. Existing Traversal Features

The GQL implementation can leverage these already-implemented traversal features:

### Path Tracking (`as_()` / `select()`)
Variable binding in GQL maps directly to path tracking:
- `(p:Person)` with variable `p` → `traversal.as_("p")`
- Accessing bound variable in WHERE/RETURN → `traversal.select(["p"])`

### Filtering Steps
- `has_label()` / `has_label_any()` - Label filtering
- `has_value()` - Property value matching
- `has_where()` - Predicate-based filtering
- `is_()` - Value predicates (`p::eq`, `p::gt`, `p::between`, etc.)

### Navigation
- `out()` / `in_()` / `both()` - Edge traversal
- `out_labels()` / `in_labels()` / `both_labels()` - Filtered edge traversal
- `out_e()` / `in_e()` / `both_e()` - Edge element access

### Transform Steps
- `values()` - Extract property values
- `value_map()` / `element_map()` - Property maps
- `project()` - Multi-value projection with `ProjectBuilder`
- `order()` - Sorting with `OrderBuilder`
- `group()` / `group_count()` - Aggregation

### Predicate System (`p::` module)
- Comparison: `p::eq`, `p::neq`, `p::lt`, `p::lte`, `p::gt`, `p::gte`
- Range: `p::between`, `p::within`, `p::without`
- String: `p::starting_with`, `p::ending_with`, `p::containing`

### Repeat Step (Variable-length paths)
- `repeat(traversal)` - Repeated traversal
- `.times(n)` - Fixed iteration count
- `.until(predicate)` - Conditional termination
- `.emit()` / `.emit_after(n)` - Result emission control

---

## 10. Implementation Effort

| Component | Effort | Notes |
|-----------|--------|-------|
| Grammar definition | 2-3 days | pest grammar file |
| AST types | 1-2 days | Straightforward structs |
| Parser | 3-4 days | Building AST from pest pairs |
| Query planner | 4-5 days | Pattern analysis, start point selection |
| Pattern compiler | 3-4 days | Converting patterns to traversals (leverages existing API) |
| Expression compiler | 2-3 days | WHERE clause, projections (uses existing `Value`) |
| Aggregation support | 3-4 days | GROUP BY semantics (uses existing `group()` step) |
| ORDER BY / LIMIT | 1-2 days | Post-processing (uses existing `order()`, `limit()`) |
| Testing | 3-4 days | Parser tests, execution tests |
| **Total** | **~4-5 weeks** | |

---

## 11. Limitations of Subset

| Feature | Status | Workaround |
|---------|--------|------------|
| CREATE/DELETE/SET | Not supported | Use Rust API for mutations |
| MERGE | Not supported | Use Rust API |
| UNWIND | Not supported | Use COLLECT in reverse |
| CALL procedures | Not supported | Use Rust API |
| Graph construction | Not supported | Not planned |
| Multiple graphs | Not supported | Single graph only |
| OPTIONAL MATCH | Not supported | Use `optional()` step via Rust API |
| CASE expressions | Not supported | Use application logic |
| List comprehensions | Not supported | Use COLLECT + app logic |

---

## 12. Future Extensions

### Phase 1: Read-only subset (this document)
- Pattern matching
- WHERE, RETURN, ORDER BY, LIMIT
- Aggregations
- Variable-length paths

### Phase 2: Mutations
```sql
CREATE (p:Person {name: 'Alice', age: 30})

MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS {since: 2024}]->(b)

MATCH (p:Person {name: 'Alice'})
SET p.age = 31

MATCH (p:Person {name: 'Alice'})
DELETE p
```

### Phase 3: Advanced Features
```sql
-- OPTIONAL MATCH
MATCH (p:Person {name: 'Alice'})
OPTIONAL MATCH (p)-[:KNOWS]->(f:Person)
RETURN p.name, f.name

-- UNWIND
UNWIND [1, 2, 3] AS x
RETURN x * 2

-- CASE
MATCH (p:Person)
RETURN p.name, 
       CASE WHEN p.age < 30 THEN 'young' ELSE 'mature' END AS category

-- Subqueries
MATCH (p:Person)
WHERE EXISTS {
    MATCH (p)-[:KNOWS]->(:Person {name: 'Alice'})
}
RETURN p.name
```

---

## 13. Summary

**Yes, we can support a useful GQL subset now.** The key is being strategic about what to include:

| Include | Exclude |
|---------|---------|
| Basic MATCH patterns | Graph construction |
| WHERE filtering | Mutations (Phase 2) |
| RETURN projections | Procedures |
| Aggregations | Multiple graphs |
| Variable-length paths | OPTIONAL MATCH |
| ORDER BY / LIMIT | UNWIND, CASE |

**Implementation path:**
1. Define grammar subset (~3 days)
2. Build parser with pest (~4 days)
3. Compile patterns to traversals (~5 days)
4. Expression evaluation (~3 days)
5. Aggregation support (~4 days)
6. Testing and polish (~4 days)

**Total: ~4-5 weeks** for a production-quality GQL subset.

This gives users a familiar, SQL-like interface while keeping implementation manageable. The subset covers the most common query patterns and can be extended incrementally.