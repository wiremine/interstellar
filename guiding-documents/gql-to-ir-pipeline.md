# GQL Parser Project Plan

A Rust-based ISO GQL parser that compiles to an intermediate representation (IR) targeting a Gremlin-style API.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Project Structure](#project-structure)
4. [ISO GQL Grammar Subset](#iso-gql-grammar-subset)
5. [AST Design](#ast-design)
6. [IR Design](#ir-design)
7. [Code Generation Strategy](#code-generation-strategy)
8. [Error Handling Strategy](#error-handling-strategy)
9. [Implementation Phases](#implementation-phases)
10. [Testing Strategy](#testing-strategy)
11. [Dependencies](#dependencies)
12. [Open Questions & Decisions](#open-questions--decisions)

---

## Project Overview

### Goals

- Parse ISO GQL queries into a typed Abstract Syntax Tree (AST)
- Lower the AST to an optimizable Intermediate Representation (IR)
- Generate Rust code targeting a Gremlin-style traversal API
- Provide excellent error messages with source spans and recovery

### Non-Goals (Initial Scope)

- Full ISO GQL compliance (we'll implement incrementally)
- Query optimization passes (future work)
- Runtime execution (the Gremlin API handles this)

### Technology Choices

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Language | Rust | Performance, safety, target API is Rust |
| Parser | pest | Declarative grammar, good errors, spec alignment |
| Error Reporting | miette or ariadne | Beautiful diagnostics with source spans |
| Code Generation | quote + proc-macro2 | Type-safe Rust code generation |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           GQL Source Text                               │
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
│                      AST Construction (parser.rs)                       │
│  - Parse tree → typed AST                                               │
│  - Span preservation for errors                                         │
│  - Semantic validation (basic)                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              Typed AST                                  │
│  - Statements, Clauses, Patterns, Expressions                           │
│  - Source spans attached to all nodes                                   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Lowering (lowering.rs)                          │
│  - AST → IR transformation                                              │
│  - Name resolution                                                      │
│  - Type inference (if applicable)                                       │
│  - Semantic analysis                                                    │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Intermediate Representation                     │
│  - Graph operation primitives                                           │
│  - Suitable for optimization                                            │
│  - Maps cleanly to Gremlin steps                                        │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      Code Generation (codegen.rs)                       │
│  - IR → Rust/Gremlin API calls                                          │
│  - Uses quote for code generation                                       │
│  - Outputs TokenStream or String                                        │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        Generated Rust Code                              │
│  g.V().has_label("Person").has("name", "Alice").out("KNOWS")...         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
gql-parser/
├── Cargo.toml
├── README.md
├── src/
│   ├── lib.rs                 # Public API, re-exports
│   │
│   ├── grammar.pest           # ISO GQL grammar definition
│   │
│   ├── ast/
│   │   ├── mod.rs             # AST module root
│   │   ├── statement.rs       # Top-level statements
│   │   ├── clause.rs          # MATCH, RETURN, WHERE, etc.
│   │   ├── pattern.rs         # Graph patterns (nodes, edges)
│   │   ├── expr.rs            # Expressions
│   │   ├── literal.rs         # Literal values
│   │   ├── types.rs           # Type annotations
│   │   └── span.rs            # Source span wrapper
│   │
│   ├── parser/
│   │   ├── mod.rs             # Parser module root
│   │   ├── convert.rs         # pest Pairs → AST conversion
│   │   └── helpers.rs         # Parsing utilities
│   │
│   ├── ir/
│   │   ├── mod.rs             # IR module root
│   │   ├── ops.rs             # IR operations
│   │   ├── expr.rs            # IR expressions
│   │   └── symbol.rs          # Symbol/binding representation
│   │
│   ├── lowering/
│   │   ├── mod.rs             # Lowering module root
│   │   ├── context.rs         # Lowering context (symbol tables, etc.)
│   │   ├── pattern.rs         # Pattern lowering
│   │   ├── expr.rs            # Expression lowering
│   │   └── clause.rs          # Clause lowering
│   │
│   ├── runtime/
│   │   ├── mod.rs             # Runtime module root
│   │   ├── value.rs           # Runtime Value type
│   │   ├── predicate.rs       # Runtime Predicate type
│   │   ├── traits.rs          # TraversalBuilder, Traversal, GraphSource traits
│   │   ├── executor.rs        # IR interpreter / executor
│   │   └── result.rs          # QueryResult type and iterators
│   │
│   ├── repl/
│   │   ├── mod.rs             # REPL module root
│   │   ├── commands.rs        # REPL command handlers
│   │   ├── display.rs         # Result formatting (table, JSON, CSV)
│   │   ├── history.rs         # Command history
│   │   └── settings.rs        # REPL configuration
│   │
│   └── error/
│       ├── mod.rs             # Error module root
│       ├── diagnostic.rs      # Diagnostic types
│       ├── syntax.rs          # Syntax errors
│       ├── semantic.rs        # Semantic errors
│       └── runtime.rs         # Execution errors
│
├── tests/
│   ├── parsing/               # Parser tests
│   │   ├── match_clause.rs
│   │   ├── return_clause.rs
│   │   ├── expressions.rs
│   │   └── patterns.rs
│   │
│   ├── lowering/              # AST → IR tests
│   │
│   ├── execution/             # Runtime execution tests
│   │   ├── mock_graph.rs      # Mock graph implementation for tests
│   │   └── queries.rs         # End-to-end query tests
│   │
│   └── integration/           # Full integration tests
│       └── snapshots/         # insta snapshot files
│
├── examples/
│   ├── repl.rs                # Standalone REPL binary
│   ├── execute_query.rs       # Single query execution
│   └── mock_backend.rs        # Example backend implementation
│
└── benches/
    ├── parser_bench.rs        # Parsing performance
    └── executor_bench.rs      # Execution performance
```

---

## ISO GQL Grammar Subset

We'll implement GQL incrementally, starting with core features.

### Phase 1: Core Query Structure

```
// Statements
<GQL-query> ::= <match-statement> <return-statement>

// Match
<match-statement> ::= MATCH <graph-pattern> [<where-clause>]

// Return  
<return-statement> ::= RETURN <return-item-list>
<return-item> ::= <expression> [AS <identifier>]

// Where
<where-clause> ::= WHERE <expression>
```

### Phase 2: Graph Patterns

```
// Patterns
<graph-pattern> ::= <path-pattern> { ',' <path-pattern> }*
<path-pattern> ::= <node-pattern> { <edge-pattern> <node-pattern> }*

// Node patterns
<node-pattern> ::= '(' [<variable>] [<label-expression>] [<property-specification>] ')'
<label-expression> ::= ':' <label> { '|' <label> }*

// Edge patterns
<edge-pattern> ::= <left-arrow>? '-' '[' [<variable>] [<label-expression>] [<property-specification>] ']' '-' <right-arrow>?
<left-arrow> ::= '<'
<right-arrow> ::= '>'

// Properties
<property-specification> ::= '{' <property-key-value-pair-list> '}'
<property-key-value-pair> ::= <property-key> ':' <expression>
```

### Phase 3: Expressions

```
// Expressions
<expression> ::= <or-expression>
<or-expression> ::= <and-expression> { OR <and-expression> }*
<and-expression> ::= <not-expression> { AND <not-expression> }*
<not-expression> ::= [NOT] <comparison-expression>
<comparison-expression> ::= <additive-expression> [<comparison-operator> <additive-expression>]
<comparison-operator> ::= '=' | '<>' | '<' | '>' | '<=' | '>='

// Arithmetic
<additive-expression> ::= <multiplicative-expression> { ('+' | '-') <multiplicative-expression> }*
<multiplicative-expression> ::= <unary-expression> { ('*' | '/' | '%') <unary-expression> }*
<unary-expression> ::= ['-'] <primary-expression>

// Primary
<primary-expression> ::= <literal>
                      | <variable>
                      | <property-reference>
                      | <function-call>
                      | '(' <expression> ')'

// Property access
<property-reference> ::= <variable> '.' <property-key>

// Functions
<function-call> ::= <function-name> '(' [<expression-list>] ')'
```

### Phase 4: Extended Clauses

```
// Create
<create-statement> ::= CREATE <graph-pattern>

// Set/Remove
<set-clause> ::= SET <set-item-list>
<remove-clause> ::= REMOVE <remove-item-list>

// Delete
<delete-clause> ::= [DETACH] DELETE <variable-list>

// With (pipelining)
<with-clause> ::= WITH <return-item-list> [<where-clause>]

// Order/Skip/Limit
<order-clause> ::= ORDER BY <sort-item-list>
<skip-clause> ::= SKIP <expression>
<limit-clause> ::= LIMIT <expression>
```

### Phase 5: Advanced Features

```
// Optional match
<optional-match> ::= OPTIONAL MATCH <graph-pattern>

// Aggregation
<aggregation-function> ::= COUNT | SUM | AVG | MIN | MAX | COLLECT

// List comprehensions
<list-comprehension> ::= '[' <expression> FOR <variable> IN <expression> [WHERE <expression>] ']'

// Case expressions
<case-expression> ::= CASE [<expression>] <when-clause>+ [ELSE <expression>] END

// Path patterns (quantified)
<quantified-path-pattern> ::= <path-pattern> <quantifier>
<quantifier> ::= '*' | '+' | '?' | '{' <integer> ',' [<integer>] '}'
```

---

## AST Design

### Core Principles

1. **Span-annotated**: Every node carries source location for error reporting
2. **Immutable**: AST is built once and never mutated
3. **Type-safe**: No stringly-typed fields where enums/structs work
4. **Faithful**: Represents the source accurately (not a desugared form)

### Node Structure

```
Spanned<T>
├── node: T           # The actual AST node
├── span: Span        # Source location (start, end, source_id)
```

### Statement Types

```
Statement
├── Query(QueryStatement)
│   ├── match_clause: MatchClause
│   ├── with_clauses: Vec<WithClause>
│   ├── return_clause: ReturnClause
│   └── modifiers: QueryModifiers (ORDER BY, SKIP, LIMIT)
│
├── Create(CreateStatement)
│   └── pattern: GraphPattern
│
├── Merge(MergeStatement)
│   ├── pattern: GraphPattern
│   ├── on_create: Option<SetClause>
│   └── on_match: Option<SetClause>
│
└── Delete(DeleteStatement)
    ├── detach: bool
    └── variables: Vec<Variable>
```

### Clause Types

```
MatchClause
├── optional: bool
├── pattern: GraphPattern
└── where_clause: Option<WhereClause>

ReturnClause
├── distinct: bool
├── items: Vec<ReturnItem>
└── asterisk: bool  # RETURN *

ReturnItem
├── expression: Expression
└── alias: Option<Identifier>

WhereClause
└── condition: Expression

SetClause
└── items: Vec<SetItem>

SetItem
├── PropertySet { target: PropertyRef, value: Expression }
├── LabelSet { variable: Variable, labels: Vec<Label> }
└── PropertiesSet { variable: Variable, expression: Expression }
```

### Pattern Types

```
GraphPattern
└── paths: Vec<PathPattern>

PathPattern
└── elements: Vec<PathElement>

PathElement
├── Node(NodePattern)
└── Edge(EdgePattern)

NodePattern
├── variable: Option<Variable>
├── labels: Vec<Label>
├── properties: Option<PropertyMap>
└── where_clause: Option<Expression>  # Inline WHERE

EdgePattern
├── variable: Option<Variable>
├── labels: Vec<Label>
├── properties: Option<PropertyMap>
├── direction: EdgeDirection
├── quantifier: Option<PathQuantifier>
└── where_clause: Option<Expression>

EdgeDirection
├── Left        # <-[]-
├── Right       # -[]->
├── Both        # -[]-
└── Undirected  # ~[]~

PathQuantifier
├── ZeroOrMore         # *
├── OneOrMore          # +
├── Optional           # ?
├── Exactly(u32)       # {3}
├── Range(u32, u32)    # {2,5}
└── AtLeast(u32)       # {3,}
```

### Expression Types

```
Expression
├── Literal(Literal)
├── Variable(Variable)
├── PropertyAccess { base: Box<Expression>, property: Identifier }
├── Parameter(Parameter)  # $param
│
├── Binary { left: Box<Expression>, op: BinaryOp, right: Box<Expression> }
├── Unary { op: UnaryOp, operand: Box<Expression> }
│
├── FunctionCall { name: Identifier, args: Vec<Expression>, distinct: bool }
├── AggregateCall { function: AggregateFunction, arg: Option<Box<Expression>>, distinct: bool }
│
├── Case { operand: Option<Box<Expression>>, when_clauses: Vec<WhenClause>, else_clause: Option<Box<Expression>> }
│
├── List(Vec<Expression>)
├── Map(Vec<(Identifier, Expression)>)
│
├── ListComprehension { ... }
├── PatternComprehension { ... }
│
├── IsNull(Box<Expression>)
├── IsNotNull(Box<Expression>)
│
├── In { item: Box<Expression>, list: Box<Expression> }
├── Between { value: Box<Expression>, low: Box<Expression>, high: Box<Expression> }
│
└── Subquery(Box<QueryStatement>)

BinaryOp
├── Arithmetic: Add, Sub, Mul, Div, Mod, Pow
├── Comparison: Eq, Neq, Lt, Lte, Gt, Gte
├── Logical: And, Or, Xor
├── String: Concat, StartsWith, EndsWith, Contains, Regex
└── Other: In, Is

UnaryOp
├── Not
├── Neg
└── IsNull

Literal
├── Null
├── Boolean(bool)
├── Integer(i64)
├── Float(f64)
├── String(String)
├── List(Vec<Literal>)
└── Map(Vec<(String, Literal)>)
```

### Supporting Types

```
Variable
├── name: String
└── span: Span

Identifier
├── name: String
├── quoted: bool  # "quoted identifier" vs regular
└── span: Span

Label
├── name: String
└── span: Span

PropertyMap
└── entries: Vec<(Identifier, Expression)>

Parameter
├── name: String        # For $name
└── position: Option<u32>  # For $1, $2, etc.
```

---

## IR Design

### Design Goals

1. **Lower-level than AST**: Patterns decomposed into primitive operations
2. **Explicit bindings**: All variables resolved to symbols
3. **Optimization-friendly**: Easy to analyze and transform
4. **Gremlin-aligned**: Operations map naturally to Gremlin steps

### IR Program Structure

```
IrProgram
├── steps: Vec<IrStep>
└── result: ResultProjection

ResultProjection
├── columns: Vec<(Symbol, String)>  # (symbol, output_name)
└── distinct: bool
```

### IR Operations (Steps)

```
IrStep
│
├── Scan { binding: Symbol, constraint: ScanConstraint }
│   # Start traversal: g.V() or g.V().hasLabel("Person")
│
├── Expand { 
│       from: Symbol, 
│       to: Symbol, 
│       edge_binding: Option<Symbol>,
│       direction: Direction, 
│       edge_labels: Vec<String> 
│   }
│   # Traverse edges: .out("KNOWS"), .inE("WORKS_AT").outV()
│
├── Filter { predicate: IrPredicate }
│   # Filter current traversers: .has("age", gt(30)), .where(...)
│
├── Project { bindings: Vec<(Symbol, IrExpr)> }
│   # Compute new values: .project("a", "b").by(...).by(...)
│
├── Aggregate { 
│       group_by: Vec<Symbol>, 
│       aggregations: Vec<(Symbol, Aggregation)> 
│   }
│   # Grouping: .group().by(...).by(count())
│
├── Order { sort_keys: Vec<(IrExpr, SortDirection)> }
│   # Sorting: .order().by("name", asc)
│
├── Limit { count: IrExpr }
│   # Limiting: .limit(10)
│
├── Skip { count: IrExpr }
│   # Skipping: .skip(5)
│
├── Dedup { by: Option<Vec<Symbol>> }
│   # Deduplication: .dedup()
│
├── Optional { steps: Vec<IrStep> }
│   # Optional traversal: .optional(...)
│
├── Coalesce { alternatives: Vec<Vec<IrStep>> }
│   # First match: .coalesce(...)
│
├── Union { branches: Vec<Vec<IrStep>> }
│   # Multiple paths: .union(...)
│
├── CreateNode { binding: Symbol, labels: Vec<String>, properties: Vec<(String, IrExpr)> }
│   # Add vertex: .addV("Person").property("name", "Alice")
│
├── CreateEdge { 
│       binding: Option<Symbol>, 
│       from: Symbol, 
│       to: Symbol, 
│       label: String, 
│       properties: Vec<(String, IrExpr)> 
│   }
│   # Add edge: .addE("KNOWS").from(a).to(b)
│
├── SetProperty { target: Symbol, key: String, value: IrExpr }
│   # Set property: .property("age", 30)
│
├── RemoveProperty { target: Symbol, key: String }
│   # Remove property: .properties("temp").drop()
│
└── Delete { target: Symbol, detach: bool }
    # Delete: .drop()

ScanConstraint
├── AllVertices
├── HasLabel(Vec<String>)
├── HasProperty(String, IrExpr)
└── ById(IrExpr)

Direction
├── Out
├── In
├── Both
```

### IR Expressions

```
IrExpr
├── Const(IrValue)
├── Symbol(Symbol)
├── Property { base: Symbol, key: String }
├── Parameter { name: String }
│
├── Binary { left: Box<IrExpr>, op: IrBinaryOp, right: Box<IrExpr> }
├── Unary { op: IrUnaryOp, operand: Box<IrExpr> }
│
├── FunctionCall { name: String, args: Vec<IrExpr> }
│
├── List(Vec<IrExpr>)
├── Map(Vec<(String, IrExpr)>)
│
├── Conditional { condition: Box<IrExpr>, then_expr: Box<IrExpr>, else_expr: Box<IrExpr> }
│
└── Coalesce(Vec<IrExpr>)

IrPredicate
├── Expr(IrExpr)  # Evaluate as boolean
├── And(Vec<IrPredicate>)
├── Or(Vec<IrPredicate>)
├── Not(Box<IrPredicate>)
├── Comparison { left: IrExpr, op: CompareOp, right: IrExpr }
├── HasLabel { binding: Symbol, labels: Vec<String> }
├── HasProperty { binding: Symbol, key: String }
├── PropertyEquals { binding: Symbol, key: String, value: IrExpr }
└── InList { value: IrExpr, list: IrExpr }

IrValue
├── Null
├── Bool(bool)
├── Int(i64)
├── Float(f64)
├── String(String)
├── List(Vec<IrValue>)
└── Map(BTreeMap<String, IrValue>)

Symbol
├── id: u32
├── name: String  # Original variable name (for debugging)
└── kind: SymbolKind

SymbolKind
├── Vertex
├── Edge
├── Path
├── Value
└── List
```

### Aggregation Types

```
Aggregation
├── Count { distinct: bool }
├── Sum { expr: IrExpr }
├── Avg { expr: IrExpr }
├── Min { expr: IrExpr }
├── Max { expr: IrExpr }
├── Collect { expr: IrExpr, distinct: bool }
├── First { expr: IrExpr }
└── Last { expr: IrExpr }
```

---

## Code Generation Strategy

### Runtime Execution Model

Since we're building a REPL where queries are entered and executed dynamically, we need a **runtime traversal builder** rather than compile-time code generation. The IR gets interpreted at runtime to build and execute traversals.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              REPL Loop                                  │
│                                                                         │
│   gql> MATCH (p:Person)-[:KNOWS]->(f) RETURN f.name                    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Parse + Lower to IR                             │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      IR Interpreter / Executor                          │
│  - Walks IR steps                                                       │
│  - Builds traversal dynamically via trait methods                       │
│  - Executes against graph backend                                       │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           Query Results                                 │
│                                                                         │
│   ┌─────────┐                                                          │
│   │  name   │                                                          │
│   ├─────────┤                                                          │
│   │ "Bob"   │                                                          │
│   │ "Carol" │                                                          │
│   └─────────┘                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Traversal Builder Trait

The key abstraction is a trait that your Gremlin-style API implements. The IR executor calls these methods dynamically:

```rust
/// Runtime value that can be passed to traversal steps
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<Value>),
    Map(HashMap<String, Value>),
    Vertex(VertexId),
    Edge(EdgeId),
}

/// Predicate for filtering
#[derive(Debug, Clone)]
pub enum Predicate {
    Eq(Value),
    Neq(Value),
    Lt(Value),
    Lte(Value),
    Gt(Value),
    Gte(Value),
    Within(Vec<Value>),
    Without(Vec<Value>),
    Between(Value, Value),
    StartsWith(String),
    EndsWith(String),
    Contains(String),
    Regex(String),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
}

/// Sort direction for ordering
#[derive(Debug, Clone, Copy)]
pub enum Order {
    Asc,
    Desc,
}

/// The core trait your Gremlin API must implement
pub trait TraversalBuilder: Sized {
    type Traversal: Traversal;
    type AnonymousTraversal: TraversalBuilder;
    
    // === Start Steps ===
    fn v(self) -> Self;
    fn v_by_ids(self, ids: &[Value]) -> Self;
    fn e(self) -> Self;
    fn e_by_ids(self, ids: &[Value]) -> Self;
    
    // === Filter Steps ===
    fn has_label(self, label: &str) -> Self;
    fn has_labels(self, labels: &[&str]) -> Self;
    fn has(self, key: &str, predicate: Predicate) -> Self;
    fn has_key(self, key: &str) -> Self;
    fn has_not(self, key: &str) -> Self;
    fn filter(self, traversal: Self::AnonymousTraversal) -> Self;
    fn where_(self, traversal: Self::AnonymousTraversal) -> Self;
    fn where_predicate(self, start: &str, predicate: Predicate, end: &str) -> Self;
    fn dedup(self) -> Self;
    fn dedup_by(self, labels: &[&str]) -> Self;
    fn is(self, predicate: Predicate) -> Self;
    
    // === Navigate Steps ===
    fn out(self, labels: &[&str]) -> Self;
    fn in_(self, labels: &[&str]) -> Self;
    fn both(self, labels: &[&str]) -> Self;
    fn out_e(self, labels: &[&str]) -> Self;
    fn in_e(self, labels: &[&str]) -> Self;
    fn both_e(self, labels: &[&str]) -> Self;
    fn out_v(self) -> Self;
    fn in_v(self) -> Self;
    fn other_v(self) -> Self;
    
    // === Map Steps ===
    fn id(self) -> Self;
    fn label(self) -> Self;
    fn values(self, keys: &[&str]) -> Self;
    fn value_map(self, keys: &[&str]) -> Self;
    fn element_map(self) -> Self;
    fn select(self, labels: &[&str]) -> Self;
    fn select_one(self, label: &str) -> Self;
    fn project(self, keys: &[&str]) -> Self;
    fn by_key(self, key: &str) -> Self;
    fn by_traversal(self, traversal: Self::AnonymousTraversal) -> Self;
    fn as_(self, label: &str) -> Self;
    fn path(self) -> Self;
    fn constant(self, value: Value) -> Self;
    fn property_value(self, key: &str) -> Self;
    
    // === Math Steps ===
    fn math(self, expression: &str) -> Self;
    
    // === Branch Steps ===
    fn optional(self, traversal: Self::AnonymousTraversal) -> Self;
    fn coalesce(self, traversals: Vec<Self::AnonymousTraversal>) -> Self;
    fn union(self, traversals: Vec<Self::AnonymousTraversal>) -> Self;
    fn choose_if_then_else(
        self, 
        predicate: Self::AnonymousTraversal,
        then_branch: Self::AnonymousTraversal,
        else_branch: Self::AnonymousTraversal,
    ) -> Self;
    fn choose_by_map(
        self,
        traversal: Self::AnonymousTraversal,
        branches: HashMap<Value, Self::AnonymousTraversal>,
    ) -> Self;
    fn repeat(self, traversal: Self::AnonymousTraversal) -> Self;
    fn until(self, predicate: Self::AnonymousTraversal) -> Self;
    fn emit(self) -> Self;
    fn emit_predicate(self, predicate: Self::AnonymousTraversal) -> Self;
    fn times(self, n: usize) -> Self;
    fn loops(self) -> Self;
    
    // === Aggregate Steps ===
    fn count(self) -> Self;
    fn sum(self) -> Self;
    fn min(self) -> Self;
    fn max(self) -> Self;
    fn mean(self) -> Self;
    fn group(self) -> Self;
    fn group_count(self) -> Self;
    fn fold(self) -> Self;
    fn unfold(self) -> Self;
    fn aggregate(self, label: &str) -> Self;
    fn store(self, label: &str) -> Self;
    
    // === Order/Limit Steps ===
    fn order(self) -> Self;
    fn by_order(self, key: &str, order: Order) -> Self;
    fn limit(self, n: usize) -> Self;
    fn skip(self, n: usize) -> Self;
    fn range(self, low: usize, high: usize) -> Self;
    fn tail(self, n: usize) -> Self;
    fn sample(self, n: usize) -> Self;
    
    // === Mutation Steps ===
    fn add_v(self, label: &str) -> Self;
    fn add_e(self, label: &str) -> Self;
    fn from_vertex(self, label: &str) -> Self;
    fn from_traversal(self, traversal: Self::AnonymousTraversal) -> Self;
    fn to_vertex(self, label: &str) -> Self;
    fn to_traversal(self, traversal: Self::AnonymousTraversal) -> Self;
    fn property(self, key: &str, value: Value) -> Self;
    fn property_with_cardinality(self, cardinality: Cardinality, key: &str, value: Value) -> Self;
    fn drop(self) -> Self;
    
    // === Side Effect Steps ===
    fn side_effect(self, traversal: Self::AnonymousTraversal) -> Self;
    
    // === Barrier Steps ===
    fn barrier(self) -> Self;
    
    // === Build/Execute ===
    fn build(self) -> Self::Traversal;
}

/// Executable traversal
pub trait Traversal {
    type Result: Iterator<Item = Value>;
    type Error;
    
    fn execute(self) -> Result<Self::Result, Self::Error>;
    fn to_list(self) -> Result<Vec<Value>, Self::Error>;
    fn next(self) -> Result<Option<Value>, Self::Error>;
    fn has_next(&mut self) -> Result<bool, Self::Error>;
}

/// Graph source - entry point for traversals
pub trait GraphSource {
    type Builder: TraversalBuilder;
    
    fn traversal(&self) -> Self::Builder;
    fn anonymous(&self) -> <Self::Builder as TraversalBuilder>::AnonymousTraversal;
}
```

### IR Executor

The executor walks the IR and calls trait methods dynamically:

```rust
pub struct Executor<G: GraphSource> {
    graph: G,
    bindings: HashMap<Symbol, Value>,
}

impl<G: GraphSource> Executor<G> {
    pub fn new(graph: G) -> Self {
        Self {
            graph,
            bindings: HashMap::new(),
        }
    }
    
    pub fn execute(&mut self, program: &IrProgram) -> Result<QueryResult, ExecuteError> {
        let mut traversal = self.graph.traversal();
        
        for step in &program.steps {
            traversal = self.execute_step(traversal, step)?;
        }
        
        self.apply_projection(traversal, &program.result)
    }
    
    fn execute_step<T: TraversalBuilder>(
        &mut self,
        traversal: T,
        step: &IrStep,
    ) -> Result<T, ExecuteError> {
        match step {
            IrStep::Scan { binding, constraint } => {
                let t = match constraint {
                    ScanConstraint::AllVertices => traversal.v(),
                    ScanConstraint::HasLabel(labels) => {
                        let labels: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                        traversal.v().has_labels(&labels)
                    }
                    ScanConstraint::HasProperty(key, value) => {
                        let val = self.eval_expr(value)?;
                        traversal.v().has(key, Predicate::Eq(val))
                    }
                    ScanConstraint::ById(id_expr) => {
                        let id = self.eval_expr(id_expr)?;
                        traversal.v_by_ids(&[id])
                    }
                };
                // Store binding for later reference
                Ok(t.as_(binding.name()))
            }
            
            IrStep::Expand { from, to, edge_binding, direction, edge_labels } => {
                let labels: Vec<&str> = edge_labels.iter().map(|s| s.as_str()).collect();
                let t = match direction {
                    Direction::Out => traversal.out(&labels),
                    Direction::In => traversal.in_(&labels),
                    Direction::Both => traversal.both(&labels),
                };
                Ok(t.as_(to.name()))
            }
            
            IrStep::Filter { predicate } => {
                self.apply_predicate(traversal, predicate)
            }
            
            IrStep::Project { bindings } => {
                let keys: Vec<&str> = bindings.iter().map(|(s, _)| s.name()).collect();
                let mut t = traversal.project(&keys);
                for (_, expr) in bindings {
                    t = self.apply_by_expr(t, expr)?;
                }
                Ok(t)
            }
            
            IrStep::Order { sort_keys } => {
                let mut t = traversal.order();
                for (expr, direction) in sort_keys {
                    let order = match direction {
                        SortDirection::Asc => Order::Asc,
                        SortDirection::Desc => Order::Desc,
                    };
                    // Assuming expr is a simple property reference
                    if let IrExpr::Property { key, .. } = expr {
                        t = t.by_order(key, order);
                    }
                }
                Ok(t)
            }
            
            IrStep::Limit { count } => {
                let n = self.eval_expr_as_usize(count)?;
                Ok(traversal.limit(n))
            }
            
            IrStep::Skip { count } => {
                let n = self.eval_expr_as_usize(count)?;
                Ok(traversal.skip(n))
            }
            
            IrStep::Dedup { by } => {
                match by {
                    Some(symbols) => {
                        let labels: Vec<&str> = symbols.iter().map(|s| s.name()).collect();
                        Ok(traversal.dedup_by(&labels))
                    }
                    None => Ok(traversal.dedup()),
                }
            }
            
            IrStep::CreateNode { binding, labels, properties } => {
                let label = labels.first().map(|s| s.as_str()).unwrap_or("vertex");
                let mut t = traversal.add_v(label);
                for additional_label in labels.iter().skip(1) {
                    t = t.property("__label__", Value::String(additional_label.clone()));
                }
                for (key, expr) in properties {
                    let val = self.eval_expr(expr)?;
                    t = t.property(key, val);
                }
                Ok(t.as_(binding.name()))
            }
            
            IrStep::CreateEdge { binding, from, to, label, properties } => {
                let mut t = traversal
                    .add_e(label)
                    .from_vertex(from.name())
                    .to_vertex(to.name());
                for (key, expr) in properties {
                    let val = self.eval_expr(expr)?;
                    t = t.property(key, val);
                }
                if let Some(b) = binding {
                    t = t.as_(b.name());
                }
                Ok(t)
            }
            
            IrStep::Delete { target, detach } => {
                let t = traversal.select_one(target.name());
                // If detach, we need to drop edges first
                // This depends on your backend's semantics
                Ok(t.drop())
            }
            
            IrStep::Optional { steps } => {
                let anon = self.build_anonymous_traversal(steps)?;
                Ok(traversal.optional(anon))
            }
            
            IrStep::Union { branches } => {
                let anon_branches: Vec<_> = branches
                    .iter()
                    .map(|b| self.build_anonymous_traversal(b))
                    .collect::<Result<_, _>>()?;
                Ok(traversal.union(anon_branches))
            }
            
            // ... more step handlers
        }
    }
    
    fn apply_predicate<T: TraversalBuilder>(
        &self,
        traversal: T,
        predicate: &IrPredicate,
    ) -> Result<T, ExecuteError> {
        match predicate {
            IrPredicate::PropertyEquals { binding, key, value } => {
                let val = self.eval_expr(value)?;
                Ok(traversal.has(key, Predicate::Eq(val)))
            }
            IrPredicate::Comparison { left, op, right } => {
                // Build appropriate has() or where() based on expression shape
                // ...
                todo!()
            }
            IrPredicate::And(predicates) => {
                let mut t = traversal;
                for p in predicates {
                    t = self.apply_predicate(t, p)?;
                }
                Ok(t)
            }
            IrPredicate::Or(predicates) => {
                // Need to use union or or() step
                // ...
                todo!()
            }
            IrPredicate::Not(inner) => {
                // Use not() step
                // ...
                todo!()
            }
            // ... more predicate handlers
        }
    }
    
    fn eval_expr(&self, expr: &IrExpr) -> Result<Value, ExecuteError> {
        match expr {
            IrExpr::Const(v) => Ok(v.clone().into()),
            IrExpr::Symbol(s) => self.bindings.get(s)
                .cloned()
                .ok_or_else(|| ExecuteError::UnboundSymbol(s.clone())),
            IrExpr::Parameter { name } => {
                // Look up in parameter map
                todo!()
            }
            // ... more expression evaluation
        }
    }
}
```

### IR to Gremlin Mapping

| IR Operation | Gremlin Step(s) |
|--------------|-----------------|
| `Scan(AllVertices)` | `.v()` |
| `Scan(HasLabel(["Person"]))` | `.v().has_labels(&["Person"])` |
| `Scan(HasProperty("id", val))` | `.v().has("id", Predicate::Eq(val))` |
| `Expand(Out, ["KNOWS"])` | `.out(&["KNOWS"])` |
| `Expand(In, ["KNOWS"])` | `.in_(&["KNOWS"])` |
| `Expand(Both, [])` | `.both(&[])` |
| `Filter(PropertyEquals(...))` | `.has(key, Predicate::Eq(val))` |
| `Filter(Comparison(...))` | `.where_(...)` or `.filter(...)` |
| `Project([...])` | `.project(&[...]).by_key(...).by_key(...)` |
| `Aggregate(group, aggs)` | `.group().by_key(...).by_traversal(...)` |
| `Order([...])` | `.order().by_order(key, Order::Asc)` |
| `Limit(n)` | `.limit(n)` |
| `Skip(n)` | `.skip(n)` |
| `Dedup` | `.dedup()` |
| `Optional(steps)` | `.optional(anonymous_traversal)` |
| `CreateNode(...)` | `.add_v("Label").property(...)` |
| `CreateEdge(...)` | `.add_e("Label").from_vertex(...).to_vertex(...)` |
| `Delete(v)` | `.select_one(...).drop()` |

### REPL Implementation

```rust
use std::io::{self, Write, BufRead};

pub struct Repl<G: GraphSource> {
    graph: G,
    history: Vec<String>,
    settings: ReplSettings,
}

pub struct ReplSettings {
    pub show_ir: bool,
    pub show_timing: bool,
    pub output_format: OutputFormat,
    pub limit: Option<usize>,
}

pub enum OutputFormat {
    Table,
    Json,
    Csv,
}

impl<G: GraphSource> Repl<G> {
    pub fn new(graph: G) -> Self {
        Self {
            graph,
            history: Vec::new(),
            settings: ReplSettings::default(),
        }
    }
    
    pub fn run(&mut self) -> io::Result<()> {
        let stdin = io::stdin();
        let mut stdout = io::stdout();
        
        println!("GQL REPL v0.1.0");
        println!("Type :help for commands, :quit to exit\n");
        
        loop {
            print!("gql> ");
            stdout.flush()?;
            
            let mut input = String::new();
            stdin.lock().read_line(&mut input)?;
            let input = input.trim();
            
            if input.is_empty() {
                continue;
            }
            
            // Handle REPL commands
            if input.starts_with(':') {
                match self.handle_command(input) {
                    CommandResult::Continue => continue,
                    CommandResult::Quit => break,
                    CommandResult::Error(e) => {
                        eprintln!("Error: {}", e);
                        continue;
                    }
                }
            }
            
            // Handle multi-line input (ends with semicolon)
            let query = if input.ends_with(';') {
                input.trim_end_matches(';').to_string()
            } else {
                let mut full_query = input.to_string();
                loop {
                    print!("  -> ");
                    stdout.flush()?;
                    let mut line = String::new();
                    stdin.lock().read_line(&mut line)?;
                    let line = line.trim();
                    if line.ends_with(';') {
                        full_query.push(' ');
                        full_query.push_str(line.trim_end_matches(';'));
                        break;
                    }
                    full_query.push(' ');
                    full_query.push_str(line);
                }
                full_query
            };
            
            self.history.push(query.clone());
            self.execute_query(&query);
        }
        
        println!("Goodbye!");
        Ok(())
    }
    
    fn execute_query(&mut self, query: &str) {
        let start = std::time::Instant::now();
        
        // Parse
        let ast = match parse(query) {
            Ok(ast) => ast,
            Err(errors) => {
                for error in errors {
                    eprintln!("{:?}", error);
                }
                return;
            }
        };
        
        // Lower to IR
        let ir = match lower(&ast) {
            Ok(ir) => {
                if self.settings.show_ir {
                    println!("\n--- IR ---");
                    println!("{:#?}", ir);
                    println!("----------\n");
                }
                ir
            }
            Err(errors) => {
                for error in errors {
                    eprintln!("{:?}", error);
                }
                return;
            }
        };
        
        // Execute
        let mut executor = Executor::new(&self.graph);
        match executor.execute(&ir) {
            Ok(results) => {
                let elapsed = start.elapsed();
                
                self.display_results(results);
                
                if self.settings.show_timing {
                    println!("\nExecuted in {:?}", elapsed);
                }
            }
            Err(e) => {
                eprintln!("Execution error: {:?}", e);
            }
        }
    }
    
    fn display_results(&self, results: QueryResult) {
        match self.settings.output_format {
            OutputFormat::Table => self.display_table(results),
            OutputFormat::Json => self.display_json(results),
            OutputFormat::Csv => self.display_csv(results),
        }
    }
    
    fn display_table(&self, results: QueryResult) {
        // Pretty table output
        // ...
    }
    
    fn handle_command(&mut self, cmd: &str) -> CommandResult {
        let parts: Vec<&str> = cmd.split_whitespace().collect();
        match parts.get(0).map(|s| *s) {
            Some(":quit") | Some(":q") => CommandResult::Quit,
            Some(":help") | Some(":h") => {
                self.print_help();
                CommandResult::Continue
            }
            Some(":ir") => {
                self.settings.show_ir = !self.settings.show_ir;
                println!("Show IR: {}", self.settings.show_ir);
                CommandResult::Continue
            }
            Some(":timing") => {
                self.settings.show_timing = !self.settings.show_timing;
                println!("Show timing: {}", self.settings.show_timing);
                CommandResult::Continue
            }
            Some(":format") => {
                match parts.get(1) {
                    Some(&"table") => self.settings.output_format = OutputFormat::Table,
                    Some(&"json") => self.settings.output_format = OutputFormat::Json,
                    Some(&"csv") => self.settings.output_format = OutputFormat::Csv,
                    _ => return CommandResult::Error("Usage: :format table|json|csv".into()),
                }
                CommandResult::Continue
            }
            Some(":history") => {
                for (i, q) in self.history.iter().enumerate() {
                    println!("{}: {}", i + 1, q);
                }
                CommandResult::Continue
            }
            Some(":schema") => {
                // Show graph schema if available
                println!("Schema display not implemented");
                CommandResult::Continue
            }
            _ => CommandResult::Error(format!("Unknown command: {}", cmd)),
        }
    }
    
    fn print_help(&self) {
        println!("GQL REPL Commands:");
        println!("  :help, :h      Show this help");
        println!("  :quit, :q      Exit the REPL");
        println!("  :ir            Toggle IR display");
        println!("  :timing        Toggle timing display");
        println!("  :format <fmt>  Set output format (table, json, csv)");
        println!("  :history       Show query history");
        println!("  :schema        Show graph schema");
        println!();
        println!("Enter GQL queries ending with ; to execute");
        println!("Multi-line queries supported");
    }
}

enum CommandResult {
    Continue,
    Quit,
    Error(String),
}
```

### Example REPL Session

```
GQL REPL v0.1.0
Type :help for commands, :quit to exit

gql> MATCH (p:Person {name: "Alice"}) RETURN p.name, p.age;

┌─────────┬─────┐
│ p.name  │ p.age│
├─────────┼─────┤
│ "Alice" │ 30  │
└─────────┴─────┘
1 row returned

gql> :ir
Show IR: true

gql> MATCH (p:Person)-[:KNOWS]->(f:Person)
  -> WHERE f.age > 25
  -> RETURN f.name;

--- IR ---
IrProgram {
    steps: [
        Scan { binding: $0, constraint: HasLabel(["Person"]) },
        Expand { from: $0, to: $1, direction: Out, edge_labels: ["KNOWS"] },
        Filter { predicate: HasLabel($1, ["Person"]) },
        Filter { predicate: Comparison(Property($1, "age"), Gt, Const(25)) },
    ],
    result: ResultProjection {
        columns: [($1, "f.name")],
        distinct: false,
    },
}
----------

┌─────────┐
│ f.name  │
├─────────┤
│ "Bob"   │
│ "Carol" │
│ "David" │
└─────────┘
3 rows returned

gql> :format json
gql> MATCH (p:Person) RETURN p LIMIT 2;
[
  {"name": "Alice", "age": 30, "city": "NYC"},
  {"name": "Bob", "age": 28, "city": "LA"}
]

gql> :quit
Goodbye!
```

### Output Modes

1. **Interactive REPL**: Pretty-printed tables, timing, IR inspection
2. **Script mode**: Execute .gql files, output JSON/CSV for piping
3. **Embedded mode**: Library API for running queries from Rust code

```rust
// Embedded usage
let graph = MyGraph::connect("localhost:8182")?;
let result = gql::execute(&graph, r#"
    MATCH (p:Person)-[:KNOWS]->(f)
    RETURN f.name
"#)?;

for row in result {
    println!("{}", row.get::<String>("f.name")?);
}
```

---

## Error Handling Strategy

### Error Categories

1. **Syntax Errors**: Invalid GQL syntax (pest handles initial detection)
2. **Semantic Errors**: Valid syntax but invalid meaning (undefined variables, type mismatches)
3. **Lowering Errors**: AST→IR issues (unsupported features, pattern complexity)
4. **Codegen Errors**: IR→Gremlin issues (API limitations)

### Error Type Design

```
GqlError
├── syntax: Vec<SyntaxError>
├── semantic: Vec<SemanticError>
└── codegen: Vec<CodegenError>

SyntaxError
├── kind: SyntaxErrorKind
├── span: Span
├── message: String
└── help: Option<String>

SyntaxErrorKind
├── UnexpectedToken { expected: Vec<String>, found: String }
├── UnterminatedString
├── InvalidNumber
├── InvalidEscape
├── MissingClause { clause: &'static str }
└── UnexpectedEof

SemanticError
├── kind: SemanticErrorKind
├── span: Span
├── message: String
├── related: Vec<RelatedInfo>
└── help: Option<String>

SemanticErrorKind
├── UndefinedVariable { name: String }
├── DuplicateVariable { name: String, original: Span }
├── TypeMismatch { expected: String, found: String }
├── InvalidPropertyAccess { type_name: String }
├── UnknownFunction { name: String, suggestions: Vec<String> }
├── WrongArgumentCount { function: String, expected: usize, found: usize }
├── AmbiguousReference { name: String, candidates: Vec<String> }
└── PathPatternError { reason: String }

RelatedInfo
├── span: Span
└── message: String
```

### Error Recovery Strategy

**In pest grammar:**
- Define recovery rules for common errors
- Use atomic rules to prevent backtracking issues
- Mark synchronization points (statement boundaries)

**In AST construction:**
- Continue parsing after errors when possible
- Collect multiple errors per parse
- Use sentinel values for missing nodes

**Example error output:**
```
error[E0001]: undefined variable `frend`
  --> query.gql:3:8
   |
 2 | MATCH (p:Person)-[:KNOWS]->(friend:Person)
   |                            ------ `friend` defined here
 3 | RETURN frend.name
   |        ^^^^^ did you mean `friend`?
   |
help: a variable with a similar name exists
   |
 3 | RETURN friend.name
   |        ~~~~~~
```

### Diagnostic Library Integration

Using `miette` for rich error display:

```rust
#[derive(Debug, Diagnostic, Error)]
#[error("undefined variable `{name}`")]
#[diagnostic(
    code(gql::semantic::undefined_variable),
    help("a variable with a similar name exists: `{suggestion}`")
)]
struct UndefinedVariable {
    name: String,
    suggestion: String,
    #[label("variable `{name}` used here")]
    span: SourceSpan,
    #[label("did you mean this?")]
    definition: Option<SourceSpan>,
    #[source_code]
    source: NamedSource<String>,
}
```

---

## Implementation Phases

### Phase 1: Foundation (Week 1-2)

**Goals:** Basic parsing infrastructure, minimal query support

**Tasks:**
- [ ] Set up project structure and dependencies
- [ ] Implement core pest grammar (MATCH, RETURN, basic patterns)
- [ ] Define core AST types with spans
- [ ] Implement pest → AST conversion
- [ ] Basic error types and reporting
- [ ] Unit tests for parser

**Deliverable:** Can parse `MATCH (n:Label) RETURN n`

### Phase 2: Patterns & Expressions (Week 3-4)

**Goals:** Full pattern syntax, expression evaluation

**Tasks:**
- [ ] Extended pattern syntax (edge directions, properties, variables)
- [ ] Full expression grammar (arithmetic, comparison, logical)
- [ ] Property access and function calls
- [ ] WHERE clause support
- [ ] Comprehensive pattern tests

**Deliverable:** Can parse complex patterns and expressions

### Phase 3: IR & Lowering (Week 5-6)

**Goals:** AST → IR transformation

**Tasks:**
- [ ] Define IR types
- [ ] Implement symbol table / binding context
- [ ] Pattern lowering (node/edge → Scan/Expand)
- [ ] Expression lowering
- [ ] Predicate extraction and optimization
- [ ] Semantic validation during lowering

**Deliverable:** Can lower parsed queries to IR

### Phase 4: Runtime & Executor (Week 7-8)

**Goals:** IR → Gremlin API execution at runtime

**Tasks:**
- [ ] Define TraversalBuilder trait and related types
- [ ] Define Value, Predicate, Order runtime types
- [ ] Implement Executor that walks IR and calls trait methods
- [ ] Handle variable bindings and scoping
- [ ] Implement expression evaluation
- [ ] Create mock GraphSource implementation for testing

**Deliverable:** Can execute basic queries against mock backend

### Phase 5: REPL & Output (Week 9-10)

**Goals:** Interactive query environment

**Tasks:**
- [ ] Implement REPL loop with rustyline
- [ ] Add REPL commands (:help, :ir, :timing, :format, etc.)
- [ ] Implement table output format (comfy-table)
- [ ] Implement JSON output format
- [ ] Implement CSV output format
- [ ] Add multi-line input support
- [ ] Add query history

**Deliverable:** Functional REPL that can execute and display query results

### Phase 6: Extended Clauses (Week 11-12)

**Goals:** More GQL features

**Tasks:**
- [ ] CREATE statement
- [ ] SET/REMOVE clauses
- [ ] DELETE statement
- [ ] WITH clause (pipelining)
- [ ] ORDER BY, SKIP, LIMIT
- [ ] OPTIONAL MATCH

**Deliverable:** Support for mutation queries and query modifiers

### Phase 7: Advanced Features (Week 13-14)

**Goals:** Complex GQL constructs

**Tasks:**
- [ ] Aggregation functions
- [ ] CASE expressions
- [ ] List comprehensions
- [ ] Subqueries
- [ ] Path quantifiers (variable-length paths)
- [ ] UNION/INTERSECT

**Deliverable:** Near-complete GQL support

### Phase 8: Polish & Production (Week 15+)

**Goals:** Production readiness

**Tasks:**
- [ ] Error message improvements
- [ ] Performance optimization (executor hot paths)
- [ ] REPL tab completion for labels/properties
- [ ] Script mode (execute .gql files)
- [ ] Documentation
- [ ] More comprehensive test suite
- [ ] Benchmarks
- [ ] Example backend implementations

---

## Testing Strategy

### Test Categories

1. **Unit Tests**: Individual functions and modules
2. **Parser Tests**: Grammar coverage, error cases
3. **Lowering Tests**: AST → IR correctness
4. **Codegen Tests**: IR → Gremlin correctness
5. **Integration Tests**: End-to-end compilation
6. **Snapshot Tests**: Golden file comparison (using `insta`)

### Parser Testing Approach

```rust
#[test]
fn test_simple_match() {
    let input = "MATCH (n:Person) RETURN n";
    let ast = parse(input).unwrap();
    
    assert_matches!(ast, Statement::Query(q) => {
        assert_eq!(q.match_clause.pattern.paths.len(), 1);
        // ...
    });
}

#[test]
fn test_parse_error_recovery() {
    let input = "MATCH (n:Person RETURN n";  // Missing )
    let result = parse(input);
    
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert_eq!(errors.len(), 1);
    assert_matches!(errors[0].kind, SyntaxErrorKind::UnexpectedToken { .. });
}
```

### Snapshot Testing (insta)

```rust
#[test]
fn test_codegen_snapshot() {
    let input = r#"
        MATCH (p:Person {name: "Alice"})-[:KNOWS]->(friend)
        WHERE friend.age > 30
        RETURN friend.name
    "#;
    
    let code = compile(input).unwrap();
    insta::assert_snapshot!(code);
}
```

### Test File Organization

```
tests/
├── parsing/
│   ├── valid/           # Valid GQL files
│   │   ├── match_simple.gql
│   │   ├── match_complex.gql
│   │   └── ...
│   └── invalid/         # Invalid GQL (expected errors)
│       ├── missing_return.gql
│       └── ...
│
├── snapshots/           # insta snapshots
│   ├── parser__tests__*.snap
│   ├── codegen__tests__*.snap
│   └── ...
```

### Property-Based Testing (proptest)

```rust
proptest! {
    #[test]
    fn roundtrip_literal(lit: ArbitraryLiteral) {
        let ast = Literal::from(lit);
        let printed = ast.to_string();
        let reparsed = parse_literal(&printed).unwrap();
        assert_eq!(ast, reparsed);
    }
}
```

---

## Dependencies

### Cargo.toml

```toml
[package]
name = "gql-parser"
version = "0.1.0"
edition = "2021"
rust-version = "1.75"

[dependencies]
# Parsing
pest = "2.7"
pest_derive = "2.7"

# Error handling
thiserror = "1.0"
miette = { version = "7.0", features = ["fancy"] }

# Runtime values
ordered-float = "4.2"     # Hashable floats for Value type

# Utilities
smol_str = "0.2"          # Small string optimization
indexmap = "2.0"          # Ordered maps
rustc-hash = "1.1"        # Fast hashing

# REPL (optional feature)
rustyline = { version = "14.0", optional = true }  # Line editing, history
comfy-table = { version = "7.1", optional = true } # Pretty table output
serde = { version = "1.0", features = ["derive"], optional = true }
serde_json = { version = "1.0", optional = true }
csv = { version = "1.3", optional = true }

[dev-dependencies]
# Testing
insta = { version = "1.34", features = ["yaml"] }
proptest = "1.4"
test-case = "3.3"
pretty_assertions = "1.4"

# Benchmarking
criterion = "0.5"

[features]
default = ["repl"]
repl = ["rustyline", "comfy-table", "serde", "serde_json", "csv"]

[[bin]]
name = "gql-repl"
path = "src/bin/repl.rs"
required-features = ["repl"]

[[bench]]
name = "parser"
harness = false

[[bench]]
name = "executor"
harness = false
```

### Dependency Rationale

| Dependency | Purpose |
|------------|---------|
| `pest` | PEG parser generator with good error reporting |
| `pest_derive` | Derive macro for pest grammars |
| `thiserror` | Ergonomic error type derivation |
| `miette` | Beautiful error diagnostics |
| `ordered-float` | Hashable floats for runtime Value comparisons |
| `smol_str` | Efficient small strings (identifiers) |
| `indexmap` | Preserve insertion order (properties) |
| `rustyline` | REPL line editing, history, completion |
| `comfy-table` | Pretty-printed result tables |
| `serde`/`serde_json` | JSON output format |
| `csv` | CSV output format |
| `insta` | Snapshot testing |
| `proptest` | Property-based testing |

---

## Open Questions & Decisions

### Grammar Decisions

1. **Case sensitivity**: GQL keywords are case-insensitive. How strict for identifiers?
   - Recommendation: Keywords case-insensitive, identifiers case-sensitive

2. **Unicode identifiers**: Support non-ASCII identifiers?
   - Recommendation: Start with ASCII, add Unicode later

3. **Comments**: Support `//` and `/* */` comments?
   - Recommendation: Yes, both styles

### IR Decisions

4. **Path representation**: How to represent variable-length paths in IR?
   - Option A: Expand to loop constructs
   - Option B: Keep as first-class PathScan operation
   - Recommendation: Option B for now (maps to Gremlin repeat())

5. **Optimization passes**: Should IR support optimization?
   - Recommendation: Design for it, implement later

### Codegen Decisions

6. **Generic vs specific API**: Generate against trait or concrete type?
   - Recommendation: Trait-based for flexibility

7. **Error handling in generated code**: Result types throughout?
   - Recommendation: Yes, propagate errors

### API Decisions

8. **Parse API**: Return AST or errors, or both?
   - Recommendation: Return `Result<Ast, Errors>` where Errors contains partial AST

9. **Compilation modes**: Separate parse/lower/codegen or unified?
   - Recommendation: Expose both unified `compile()` and individual stages

### Outstanding Research

- [ ] Review ISO GQL spec for edge cases
- [ ] Evaluate Gremlin API completeness for GQL features
- [ ] Investigate incremental parsing for IDE support
- [ ] Consider WASM compilation for browser use

---

## References

- [ISO GQL Specification](https://www.iso.org/standard/76120.html)
- [GQL Wikipedia](https://en.wikipedia.org/wiki/Graph_Query_Language)
- [pest Book](https://pest.rs/book/)
- [Apache TinkerPop Gremlin](https://tinkerpop.apache.org/gremlin.html)
- [miette Documentation](https://docs.rs/miette)
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)

---

## Appendix A: Example Queries

### Basic Queries

```gql
-- Simple node match
MATCH (n:Person)
RETURN n.name

-- With relationship
MATCH (a:Person)-[:KNOWS]->(b:Person)
RETURN a.name, b.name

-- Bidirectional
MATCH (a:Person)-[:KNOWS]-(b:Person)
RETURN a.name, b.name

-- Multiple hops
MATCH (a:Person)-[:KNOWS]->()-[:KNOWS]->(c:Person)
WHERE a <> c
RETURN DISTINCT a.name, c.name
```

### Filtering

```gql
-- Property filter
MATCH (p:Person)
WHERE p.age >= 21 AND p.city = "NYC"
RETURN p

-- Pattern predicate
MATCH (p:Person)
WHERE (p)-[:WORKS_AT]->(:Company {name: "Acme"})
RETURN p.name

-- List membership
MATCH (p:Person)
WHERE p.status IN ["active", "pending"]
RETURN p
```

### Aggregation

```gql
-- Count
MATCH (p:Person)-[:PURCHASED]->(item)
RETURN p.name, count(item) AS purchases

-- Group and aggregate
MATCH (p:Person)-[:LIVES_IN]->(city:City)
RETURN city.name, count(p) AS population, avg(p.age) AS avg_age
ORDER BY population DESC

-- Collect into list
MATCH (p:Person)-[:KNOWS]->(friend)
RETURN p.name, collect(friend.name) AS friends
```

### Mutations

```gql
-- Create node
CREATE (p:Person {name: "Alice", age: 30})
RETURN p

-- Create relationship
MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
CREATE (a)-[:KNOWS {since: 2020}]->(b)

-- Update
MATCH (p:Person {name: "Alice"})
SET p.age = 31, p.updated = datetime()
RETURN p

-- Delete
MATCH (p:Person {name: "Alice"})
DETACH DELETE p
```

### Advanced

```gql
-- Variable-length path
MATCH path = (a:Person)-[:KNOWS*1..5]->(b:Person)
WHERE a.name = "Alice" AND b.name = "Eve"
RETURN path, length(path)

-- Optional match
MATCH (p:Person)
OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
RETURN p.name, c.name AS company

-- With pipelining
MATCH (p:Person)-[:PURCHASED]->(item)
WITH p, count(item) AS total
WHERE total > 10
MATCH (p)-[:LIVES_IN]->(city)
RETURN p.name, total, city.name

-- Subquery
MATCH (p:Person)
WHERE EXISTS {
    MATCH (p)-[:PURCHASED]->(:Product {category: "Electronics"})
}
RETURN p.name
```

---

## Appendix B: Backend Implementation Guide

### Implementing a Graph Backend

To use the GQL parser with your graph database, implement the `GraphSource` and `TraversalBuilder` traits. Here's a guide:

### Minimal Implementation Example

```rust
use gql_parser::runtime::{
    GraphSource, TraversalBuilder, Traversal, Value, Predicate, Order,
};

/// Your graph connection
pub struct MyGraph {
    connection: MyConnection,
}

/// Builder that accumulates traversal steps
pub struct MyTraversalBuilder {
    graph: Arc<MyGraph>,
    steps: Vec<MyStep>,
}

/// Anonymous traversal (for nested traversals)
pub struct MyAnonymousTraversal {
    steps: Vec<MyStep>,
}

/// Executable traversal
pub struct MyTraversal {
    graph: Arc<MyGraph>,
    steps: Vec<MyStep>,
}

impl GraphSource for MyGraph {
    type Builder = MyTraversalBuilder;
    
    fn traversal(&self) -> Self::Builder {
        MyTraversalBuilder {
            graph: Arc::new(self.clone()),
            steps: vec![],
        }
    }
    
    fn anonymous(&self) -> <Self::Builder as TraversalBuilder>::AnonymousTraversal {
        MyAnonymousTraversal { steps: vec![] }
    }
}

impl TraversalBuilder for MyTraversalBuilder {
    type Traversal = MyTraversal;
    type AnonymousTraversal = MyAnonymousTraversal;
    
    fn v(mut self) -> Self {
        self.steps.push(MyStep::V);
        self
    }
    
    fn has_label(mut self, label: &str) -> Self {
        self.steps.push(MyStep::HasLabel(label.to_string()));
        self
    }
    
    fn has(mut self, key: &str, predicate: Predicate) -> Self {
        self.steps.push(MyStep::Has(key.to_string(), predicate));
        self
    }
    
    fn out(mut self, labels: &[&str]) -> Self {
        self.steps.push(MyStep::Out(labels.iter().map(|s| s.to_string()).collect()));
        self
    }
    
    // ... implement remaining trait methods
    
    fn build(self) -> Self::Traversal {
        MyTraversal {
            graph: self.graph,
            steps: self.steps,
        }
    }
}

impl Traversal for MyTraversal {
    type Result = MyResultIterator;
    type Error = MyError;
    
    fn execute(self) -> Result<Self::Result, Self::Error> {
        // Convert steps to your backend's query format
        // Execute against the graph
        // Return iterator over results
        todo!()
    }
    
    fn to_list(self) -> Result<Vec<Value>, Self::Error> {
        self.execute()?.collect()
    }
    
    fn next(self) -> Result<Option<Value>, Self::Error> {
        self.execute()?.next().transpose()
    }
}
```

### Integration Testing Your Backend

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use gql_parser::{parse, lower, Executor};
    
    fn setup_test_graph() -> MyGraph {
        let graph = MyGraph::new();
        // Add test vertices and edges
        graph
    }
    
    #[test]
    fn test_simple_query() {
        let graph = setup_test_graph();
        let mut executor = Executor::new(&graph);
        
        let query = "MATCH (p:Person {name: 'Alice'}) RETURN p.age";
        let ast = parse(query).unwrap();
        let ir = lower(&ast).unwrap();
        let result = executor.execute(&ir).unwrap();
        
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.get(0, "p.age"), Some(Value::Int(30)));
    }
}
```

### Backend Checklist

Minimum viable implementation:
- [ ] `v()` - Start traversal at vertices
- [ ] `has_label()` - Filter by vertex label
- [ ] `has()` - Filter by property value
- [ ] `out()` / `in_()` / `both()` - Traverse edges
- [ ] `values()` - Extract property values
- [ ] `as_()` / `select()` - Label and retrieve traversers
- [ ] `build()` / `execute()` - Execute the traversal

For mutations:
- [ ] `add_v()` - Create vertices
- [ ] `add_e()` - Create edges
- [ ] `property()` - Set properties
- [ ] `drop()` - Delete elements

For advanced queries:
- [ ] `project()` / `by_key()` - Projections
- [ ] `group()` - Aggregations
- [ ] `order()` / `by_order()` - Sorting
- [ ] `limit()` / `skip()` - Pagination
- [ ] `optional()` / `coalesce()` - Branching
- [ ] `repeat()` / `until()` / `times()` - Loops

---

*Document Version: 1.1*  
*Last Updated: January 2025*