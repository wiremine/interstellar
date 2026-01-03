# RustGremlin: Shared Query IR Design

**Status**: Phase 2 Feature - Shared infrastructure for Gremlin and GQL query interfaces.

---

## 1. Overview

### 1.1 Purpose

This document describes the shared Intermediate Representation (IR) and type system used by both the Gremlin and GQL query interfaces. By sharing these foundational types, we:

1. **Reduce code duplication** between Gremlin and GQL implementations
2. **Enable consistent semantics** for equivalent operations
3. **Simplify the compilation pipeline** with a unified IR
4. **Allow future optimizations** to benefit both query languages

### 1.2 Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Query Interface Layer                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────┐              ┌─────────────────────┐           │
│  │   Gremlin Parser    │              │    GQL Parser       │           │
│  │   (Text/Bytecode)   │              │    (Text)           │           │
│  └──────────┬──────────┘              └──────────┬──────────┘           │
│             │                                    │                       │
│             │   Bytecode                         │   AST                 │
│             ▼                                    ▼                       │
│  ┌─────────────────────┐              ┌─────────────────────┐           │
│  │  Gremlin Compiler   │              │   GQL Compiler      │           │
│  │  bytecode → IR      │              │   AST → IR          │           │
│  └──────────┬──────────┘              └──────────┬──────────┘           │
│             │                                    │                       │
│             └────────────────┬───────────────────┘                       │
│                              │                                           │
│                              ▼                                           │
│                    ┌─────────────────────┐                               │
│                    │   Shared Query IR   │ ◄── This document             │
│                    │   (QueryPlan)       │                               │
│                    └──────────┬──────────┘                               │
│                              │                                           │
│                              ▼                                           │
│                    ┌─────────────────────┐                               │
│                    │   IR Compiler       │                               │
│                    │   IR → Steps        │                               │
│                    └──────────┬──────────┘                               │
│                              │                                           │
│                              ▼                                           │
│                    ┌─────────────────────┐                               │
│                    │  Vec<Box<AnyStep>>  │                               │
│                    │  (Executable)       │                               │
│                    └─────────────────────┘                               │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.3 Design Principles

1. **Step-Oriented IR**: The IR is a sequence of operations that map directly to traversal steps
2. **Value-Based**: All literals and predicates work with the existing `Value` type
3. **Composable**: Anonymous traversals are represented as nested IR
4. **Extensible**: New operations can be added without breaking existing code

---

## 2. Shared Type System

### 2.1 Module Structure

```
src/query/
├── mod.rs              # Module entry, re-exports
├── types.rs            # Core enums (Direction, Scope, SortOrder, etc.)
├── predicate.rs        # Predicate enum with evaluate() method
└── ir.rs               # QueryOp enum and QueryPlan (Phase 2)
```

### 2.2 Core Enums (`types.rs`)

These enums are used by both Gremlin bytecode arguments and GQL expression evaluation.

```rust
/// Direction for edge traversal
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// Outgoing edges (source → target)
    Out,
    /// Incoming edges (target ← source)
    In,
    /// Both directions
    Both,
}

/// Sort order for ORDER BY / order() step
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SortOrder {
    /// Ascending (smallest first)
    Asc,
    /// Descending (largest first)
    Desc,
    /// Random shuffle
    Shuffle,
}

/// Scope for aggregation operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scope {
    /// Operate on local collection (within each traverser)
    Local,
    /// Operate on global stream (across all traversers)
    Global,
}

/// Column selection for map operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Column {
    /// Select keys from a map
    Keys,
    /// Select values from a map
    Values,
}

/// Token for accessing element metadata
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum T {
    /// Element ID
    Id,
    /// Element label
    Label,
    /// Property key
    Key,
    /// Property value
    Value,
}

/// Property cardinality (for schema/mutation operations)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Cardinality {
    /// Single value per property key
    Single,
    /// List of values (ordered, allows duplicates)
    List,
    /// Set of values (unordered, no duplicates)
    Set,
}
```

### 2.3 Mapping to Query Languages

| Enum | Gremlin Usage | GQL Usage |
|------|---------------|-----------|
| `Direction` | `out()`, `in()`, `both()` steps | `->`, `<-`, `-` edge patterns |
| `SortOrder` | `order().by(x, asc)` | `ORDER BY x ASC` |
| `Scope` | `dedup(local)`, `count(global)` | Implicit in aggregation context |
| `Column` | `select(keys)`, `select(values)` | N/A (use property access) |
| `T` | `T.id`, `T.label` | `id(x)`, `labels(x)` functions |
| `Cardinality` | `property(single, k, v)` | N/A (schema-defined) |

---

## 3. Predicate System

### 3.1 Overview

The predicate system provides a unified way to express filtering conditions. Both Gremlin's `P.*` predicates and GQL's WHERE expressions compile to the same `Predicate` enum.

### 3.2 Predicate Enum (`predicate.rs`)

```rust
use crate::value::Value;

/// A predicate for filtering values.
///
/// Predicates can be evaluated against any `Value` and return a boolean result.
/// They support comparison, range, collection membership, string operations,
/// and logical combination.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    // === Comparison Predicates ===
    
    /// Equal: value == target
    Eq(Value),
    /// Not equal: value != target
    Neq(Value),
    /// Less than: value < target
    Lt(Value),
    /// Less than or equal: value <= target
    Lte(Value),
    /// Greater than: value > target
    Gt(Value),
    /// Greater than or equal: value >= target
    Gte(Value),
    
    // === Range Predicates ===
    
    /// Between (inclusive): low <= value <= high
    Between(Value, Value),
    /// Inside (exclusive): low < value < high
    Inside(Value, Value),
    /// Outside: value < low OR value > high
    Outside(Value, Value),
    
    // === Collection Predicates ===
    
    /// Within: value is in the collection
    Within(Vec<Value>),
    /// Without: value is not in the collection
    Without(Vec<Value>),
    
    // === String Predicates ===
    
    /// Contains substring
    Containing(String),
    /// Starts with prefix
    StartingWith(String),
    /// Ends with suffix
    EndingWith(String),
    /// Matches regex pattern
    Regex(String),
    
    // === Logical Predicates ===
    
    /// Logical AND: both predicates must match
    And(Box<Predicate>, Box<Predicate>),
    /// Logical OR: at least one predicate must match
    Or(Box<Predicate>, Box<Predicate>),
    /// Logical NOT: predicate must not match
    Not(Box<Predicate>),
}
```

### 3.3 Evaluation Semantics

#### Type Coercion Rules

Predicates follow these type coercion rules when comparing values:

1. **Same types**: Direct comparison using Rust's `PartialOrd`/`PartialEq`
2. **Int vs Float**: Int is promoted to Float for comparison
3. **Null comparisons**: 
   - `Eq(Null)` matches only `Null`
   - `Neq(Null)` matches any non-null value
   - Ordering predicates (`Lt`, `Gt`, etc.) return `false` for `Null`
4. **Type mismatches**: Return `false` (no implicit coercion beyond Int/Float)

#### String Predicate Behavior

| Predicate | Behavior | Case Sensitive |
|-----------|----------|----------------|
| `Containing(s)` | `value.contains(s)` | Yes |
| `StartingWith(s)` | `value.starts_with(s)` | Yes |
| `EndingWith(s)` | `value.ends_with(s)` | Yes |
| `Regex(pattern)` | `Regex::new(pattern).is_match(value)` | Depends on pattern |

#### Range Predicate Semantics

| Predicate | Mathematical | SQL Equivalent |
|-----------|--------------|----------------|
| `Between(a, b)` | `a <= x <= b` | `x BETWEEN a AND b` |
| `Inside(a, b)` | `a < x < b` | `x > a AND x < b` |
| `Outside(a, b)` | `x < a OR x > b` | `x < a OR x > b` |

### 3.4 Mapping to Query Languages

| Predicate | Gremlin | GQL |
|-----------|---------|-----|
| `Eq(v)` | `P.eq(v)` | `= v` |
| `Neq(v)` | `P.neq(v)` | `<> v` or `!= v` |
| `Lt(v)` | `P.lt(v)` | `< v` |
| `Lte(v)` | `P.lte(v)` | `<= v` |
| `Gt(v)` | `P.gt(v)` | `> v` |
| `Gte(v)` | `P.gte(v)` | `>= v` |
| `Between(a, b)` | `P.between(a, b)` | `BETWEEN a AND b` |
| `Inside(a, b)` | `P.inside(a, b)` | `> a AND < b` |
| `Outside(a, b)` | `P.outside(a, b)` | `< a OR > b` |
| `Within([...])` | `P.within(...)` | `IN [...]` |
| `Without([...])` | `P.without(...)` | `NOT IN [...]` |
| `Containing(s)` | `TextP.containing(s)` | `CONTAINS s` |
| `StartingWith(s)` | `TextP.startingWith(s)` | `STARTS WITH s` |
| `EndingWith(s)` | `TextP.endingWith(s)` | `ENDS WITH s` |
| `Regex(p)` | `TextP.regex(p)` | `=~ p` (extension) |
| `And(a, b)` | `P.and(a, b)` or `a.and(b)` | `a AND b` |
| `Or(a, b)` | `P.or(a, b)` or `a.or(b)` | `a OR b` |
| `Not(p)` | `P.not(p)` | `NOT p` |

### 3.5 Builder API (`p` module)

For ergonomic predicate construction in the fluent API:

```rust
/// Predicate builder module (Gremlin P.* style)
pub mod p {
    use super::Predicate;
    use crate::value::Value;

    pub fn eq(value: impl Into<Value>) -> Predicate {
        Predicate::Eq(value.into())
    }
    
    pub fn neq(value: impl Into<Value>) -> Predicate {
        Predicate::Neq(value.into())
    }
    
    pub fn lt(value: impl Into<Value>) -> Predicate {
        Predicate::Lt(value.into())
    }
    
    pub fn lte(value: impl Into<Value>) -> Predicate {
        Predicate::Lte(value.into())
    }
    
    pub fn gt(value: impl Into<Value>) -> Predicate {
        Predicate::Gt(value.into())
    }
    
    pub fn gte(value: impl Into<Value>) -> Predicate {
        Predicate::Gte(value.into())
    }
    
    pub fn between(low: impl Into<Value>, high: impl Into<Value>) -> Predicate {
        Predicate::Between(low.into(), high.into())
    }
    
    pub fn inside(low: impl Into<Value>, high: impl Into<Value>) -> Predicate {
        Predicate::Inside(low.into(), high.into())
    }
    
    pub fn outside(low: impl Into<Value>, high: impl Into<Value>) -> Predicate {
        Predicate::Outside(low.into(), high.into())
    }
    
    pub fn within<I, V>(values: I) -> Predicate
    where
        I: IntoIterator<Item = V>,
        V: Into<Value>,
    {
        Predicate::Within(values.into_iter().map(Into::into).collect())
    }
    
    pub fn without<I, V>(values: I) -> Predicate
    where
        I: IntoIterator<Item = V>,
        V: Into<Value>,
    {
        Predicate::Without(values.into_iter().map(Into::into).collect())
    }
    
    pub fn containing(substring: impl Into<String>) -> Predicate {
        Predicate::Containing(substring.into())
    }
    
    pub fn starting_with(prefix: impl Into<String>) -> Predicate {
        Predicate::StartingWith(prefix.into())
    }
    
    pub fn ending_with(suffix: impl Into<String>) -> Predicate {
        Predicate::EndingWith(suffix.into())
    }
    
    pub fn regex(pattern: impl Into<String>) -> Predicate {
        Predicate::Regex(pattern.into())
    }
    
    pub fn not(predicate: Predicate) -> Predicate {
        Predicate::Not(Box::new(predicate))
    }
}
```

### 3.6 Usage Examples

```rust
use rustgremlin::prelude::*;

// Simple equality (existing behavior)
let alice = g.v().has_value("name", "Alice").to_list();

// Using predicates for richer filtering
let adults = g.v()
    .has_label("person")
    .has("age", p::gte(18))
    .to_list();

// Range query
let middle_aged = g.v()
    .has_label("person")
    .has("age", p::between(30, 50))
    .to_list();

// String matching
let a_names = g.v()
    .has_label("person")
    .has("name", p::starting_with("A"))
    .to_list();

// Collection membership
let target_ages = g.v()
    .has_label("person")
    .has("age", p::within([25, 30, 35, 40]))
    .to_list();

// Combining predicates
let query = g.v()
    .has_label("person")
    .has("age", p::gte(18).and(p::lt(65)))
    .to_list();
```

---

## 4. Query IR (Phase 2)

### 4.1 Overview

The Query IR represents a traversal as a sequence of operations. Both Gremlin bytecode and GQL AST compile to this IR, which then compiles to executable steps.

### 4.2 QueryOp Enum

```rust
use crate::value::{Value, VertexId, EdgeId};
use super::{Direction, Predicate, SortOrder, Scope};

/// A single operation in the query IR.
///
/// Each operation corresponds to one or more traversal steps.
#[derive(Debug, Clone)]
pub enum QueryOp {
    // === Source Operations ===
    
    /// Start from all vertices
    AllVertices,
    /// Start from specific vertices by ID
    Vertices(Vec<VertexId>),
    /// Start from all edges
    AllEdges,
    /// Start from specific edges by ID
    Edges(Vec<EdgeId>),
    /// Inject literal values into the traversal
    Inject(Vec<Value>),
    
    // === Navigation Operations ===
    
    /// Traverse to adjacent vertices
    ToVertex {
        direction: Direction,
        labels: Vec<String>,
    },
    /// Traverse to incident edges
    ToEdge {
        direction: Direction,
        labels: Vec<String>,
    },
    /// Get the source vertex of an edge
    OutVertex,
    /// Get the target vertex of an edge
    InVertex,
    /// Get both vertices of an edge
    BothVertices,
    
    // === Filter Operations ===
    
    /// Filter by element label
    HasLabel(Vec<String>),
    /// Filter by property existence
    HasProperty(String),
    /// Filter by property value with predicate
    HasValue {
        key: String,
        predicate: Predicate,
    },
    /// Filter by element ID
    HasId(Vec<Value>),
    /// Generic filter with nested traversal (returns true if traversal produces results)
    Filter(Box<QueryPlan>),
    /// Deduplicate by value
    Dedup,
    /// Deduplicate by projected value
    DedupBy(Box<QueryPlan>),
    /// Take first n elements
    Limit(usize),
    /// Skip first n elements
    Skip(usize),
    /// Take elements in range [start, end)
    Range(usize, usize),
    /// Logical NOT filter
    Not(Box<QueryPlan>),
    /// Logical AND filter
    And(Vec<QueryPlan>),
    /// Logical OR filter  
    Or(Vec<QueryPlan>),
    /// Where filter with pattern matching
    Where(Box<QueryPlan>),
    /// Is predicate (filter current value)
    Is(Predicate),
    
    // === Transform Operations ===
    
    /// Extract property values
    Values(Vec<String>),
    /// Get element ID
    Id,
    /// Get element label
    Label,
    /// Replace value with constant
    Constant(Value),
    /// Get traversal path
    Path,
    /// Label current position in path
    As(String),
    /// Select labeled values from path
    Select(Vec<String>),
    /// Project to map with named traversals
    Project {
        keys: Vec<String>,
        traversals: Vec<QueryPlan>,
    },
    /// Custom map transformation (closure-based, embedded API only)
    Map(String),  // Placeholder for closure reference
    /// Custom flat-map transformation
    FlatMap(String),
    /// Unfold collections
    Unfold,
    /// Fold into collection
    Fold,
    /// Group by key
    GroupBy {
        key_traversal: Box<QueryPlan>,
        value_traversal: Option<Box<QueryPlan>>,
    },
    /// Order results
    Order {
        comparators: Vec<(QueryPlan, SortOrder)>,
    },
    
    // === Branching Operations ===
    
    /// Union of multiple traversals
    Union(Vec<QueryPlan>),
    /// First traversal that produces results
    Coalesce(Vec<QueryPlan>),
    /// Optional traversal (identity if empty)
    Optional(Box<QueryPlan>),
    /// Repeat traversal
    Repeat {
        traversal: Box<QueryPlan>,
        until: Option<Box<QueryPlan>>,
        emit: Option<Box<QueryPlan>>,
        times: Option<usize>,
    },
    /// Local scope operation
    Local(Box<QueryPlan>),
    
    // === Side Effect Operations ===
    
    /// Store values in side effect
    Store(String),
    /// Aggregate values in side effect
    Aggregate(String),
    /// Inject side effect values
    Cap(String),
    
    // === Terminal Operations (metadata only, execution handled separately) ===
    
    /// Count results
    Count,
    /// Sum numeric values
    Sum,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Average value
    Mean,
}

/// A query plan is a sequence of operations.
#[derive(Debug, Clone)]
pub struct QueryPlan {
    /// The operations to execute in order
    pub ops: Vec<QueryOp>,
}

impl QueryPlan {
    /// Create an empty query plan
    pub fn new() -> Self {
        Self { ops: vec![] }
    }
    
    /// Add an operation to the plan
    pub fn push(&mut self, op: QueryOp) {
        self.ops.push(op);
    }
    
    /// Check if the plan is empty
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}
```

### 4.3 IR Compilation

The IR compiler transforms a `QueryPlan` into executable steps:

```rust
impl QueryPlan {
    /// Compile this query plan into executable steps.
    pub fn compile(&self) -> Result<Vec<Box<dyn AnyStep>>, CompileError> {
        let mut steps: Vec<Box<dyn AnyStep>> = Vec::new();
        
        for op in &self.ops {
            match op {
                QueryOp::AllVertices => {
                    steps.push(Box::new(VertexStep::all()));
                }
                QueryOp::Vertices(ids) => {
                    steps.push(Box::new(VertexStep::by_ids(ids.clone())));
                }
                QueryOp::HasLabel(labels) => {
                    steps.push(Box::new(HasLabelStep::new(labels.clone())));
                }
                QueryOp::HasValue { key, predicate } => {
                    steps.push(Box::new(HasPredicateStep::new(
                        key.clone(),
                        predicate.clone(),
                    )));
                }
                QueryOp::ToVertex { direction, labels } => {
                    steps.push(Box::new(match direction {
                        Direction::Out => {
                            if labels.is_empty() {
                                OutStep::new()
                            } else {
                                OutStep::with_labels(labels.clone())
                            }
                        }
                        Direction::In => {
                            if labels.is_empty() {
                                InStep::new()
                            } else {
                                InStep::with_labels(labels.clone())
                            }
                        }
                        Direction::Both => {
                            if labels.is_empty() {
                                BothStep::new()
                            } else {
                                BothStep::with_labels(labels.clone())
                            }
                        }
                    }));
                }
                // ... more cases
                _ => return Err(CompileError::UnsupportedOp(format!("{:?}", op))),
            }
        }
        
        Ok(steps)
    }
}
```

### 4.4 Gremlin to IR Example

Gremlin query:
```groovy
g.V().has('person', 'name', 'Alice').out('knows').values('name')
```

Bytecode:
```rust
Bytecode {
    step_instructions: vec![
        Instruction { operator: "V", arguments: [] },
        Instruction { 
            operator: "has", 
            arguments: [String("person"), String("name"), String("Alice")] 
        },
        Instruction { operator: "out", arguments: [String("knows")] },
        Instruction { operator: "values", arguments: [String("name")] },
    ],
}
```

Compiled IR:
```rust
QueryPlan {
    ops: vec![
        QueryOp::AllVertices,
        QueryOp::HasLabel(vec!["person".to_string()]),
        QueryOp::HasValue {
            key: "name".to_string(),
            predicate: Predicate::Eq(Value::String("Alice".to_string())),
        },
        QueryOp::ToVertex {
            direction: Direction::Out,
            labels: vec!["knows".to_string()],
        },
        QueryOp::Values(vec!["name".to_string()]),
    ],
}
```

### 4.5 GQL to IR Example

GQL query:
```sql
MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(friend:Person)
RETURN friend.name
```

Compiled IR:
```rust
QueryPlan {
    ops: vec![
        QueryOp::AllVertices,
        QueryOp::HasLabel(vec!["Person".to_string()]),
        QueryOp::HasValue {
            key: "name".to_string(),
            predicate: Predicate::Eq(Value::String("Alice".to_string())),
        },
        QueryOp::As("p".to_string()),
        QueryOp::ToEdge {
            direction: Direction::Out,
            labels: vec!["KNOWS".to_string()],
        },
        QueryOp::InVertex,
        QueryOp::HasLabel(vec!["Person".to_string()]),
        QueryOp::As("friend".to_string()),
        QueryOp::Select(vec!["friend".to_string()]),
        QueryOp::Values(vec!["name".to_string()]),
    ],
}
```

---

## 5. Integration with Traversal Engine

### 5.1 HasPredicateStep

A new filter step that uses predicates instead of equality:

```rust
/// Filter step that keeps elements where a property matches a predicate.
#[derive(Clone, Debug)]
pub struct HasPredicateStep {
    key: String,
    predicate: Predicate,
}

impl HasPredicateStep {
    pub fn new(key: impl Into<String>, predicate: Predicate) -> Self {
        Self {
            key: key.into(),
            predicate,
        }
    }
    
    fn matches(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Vertex(id) => {
                ctx.snapshot()
                    .storage()
                    .get_vertex(*id)
                    .and_then(|v| v.properties.get(&self.key))
                    .map(|pv| self.predicate.evaluate(pv))
                    .unwrap_or(false)
            }
            Value::Edge(id) => {
                ctx.snapshot()
                    .storage()
                    .get_edge(*id)
                    .and_then(|e| e.properties.get(&self.key))
                    .map(|pv| self.predicate.evaluate(pv))
                    .unwrap_or(false)
            }
            _ => false,
        }
    }
}

impl_filter_step!(HasPredicateStep, "has");
```

### 5.2 Fluent API Extension

Add `has` method overload that accepts predicates:

```rust
impl<S, E> Traversal<S, E> {
    /// Filter elements by property value matching a predicate.
    pub fn has(self, key: impl Into<String>, predicate: Predicate) -> Traversal<S, E> {
        self.add_step(HasPredicateStep::new(key, predicate))
    }
}
```

### 5.3 IsStep

Filter current value against a predicate:

```rust
/// Filter step that keeps values matching a predicate.
#[derive(Clone, Debug)]
pub struct IsStep {
    predicate: Predicate,
}

impl IsStep {
    pub fn new(predicate: Predicate) -> Self {
        Self { predicate }
    }
}

impl AnyStep for IsStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let predicate = self.predicate.clone();
        Box::new(input.filter(move |t| predicate.evaluate(&t.value)))
    }
    
    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }
    
    fn name(&self) -> &'static str {
        "is"
    }
}
```

---

## 6. Error Handling

### 6.1 Query Errors

```rust
use thiserror::Error;

/// Errors that can occur during query parsing and compilation.
#[derive(Debug, Error)]
pub enum QueryError {
    /// Parse error in query text
    #[error("parse error at position {position}: {message}")]
    Parse { position: usize, message: String },
    
    /// Unknown step/operator
    #[error("unknown operator: {0}")]
    UnknownOperator(String),
    
    /// Invalid argument type
    #[error("invalid argument type for {operator}: expected {expected}, got {actual}")]
    InvalidArgument {
        operator: String,
        expected: String,
        actual: String,
    },
    
    /// Missing required argument
    #[error("missing argument for {operator}: {argument}")]
    MissingArgument { operator: String, argument: String },
    
    /// Unsupported feature
    #[error("unsupported feature: {0}")]
    Unsupported(String),
    
    /// Invalid regex pattern
    #[error("invalid regex pattern: {0}")]
    InvalidRegex(String),
    
    /// Type error during evaluation
    #[error("type error: {0}")]
    TypeError(String),
}
```

---

## 7. Implementation Phases

### Phase 1: Foundation (Current)
- [ ] `src/query/mod.rs` - Module structure
- [ ] `src/query/types.rs` - Core enums
- [ ] `src/query/predicate.rs` - Predicate enum with `evaluate()`
- [ ] `p` module - Predicate builders
- [ ] `HasPredicateStep` - Predicate-based filtering
- [ ] Unit tests for all predicates

### Phase 2: IR Layer
- [ ] `src/query/ir.rs` - QueryOp and QueryPlan
- [ ] IR compiler to steps
- [ ] `IsStep` - Value predicate filter
- [ ] Integration tests

### Phase 3: Gremlin Interface
- [ ] `src/query/gremlin/bytecode.rs` - Bytecode structures
- [ ] `src/query/gremlin/parser.rs` - Text parser
- [ ] `src/query/gremlin/compiler.rs` - Bytecode → IR

### Phase 4: GQL Interface
- [ ] `src/query/gql/ast.rs` - AST structures
- [ ] `src/query/gql/parser.rs` - Text parser
- [ ] `src/query/gql/compiler.rs` - AST → IR

---

## 8. Testing Strategy

### 8.1 Predicate Tests

```rust
#[cfg(test)]
mod predicate_tests {
    use super::*;

    #[test]
    fn eq_matches_same_value() {
        let pred = Predicate::Eq(Value::Int(42));
        assert!(pred.evaluate(&Value::Int(42)));
        assert!(!pred.evaluate(&Value::Int(43)));
    }
    
    #[test]
    fn gt_compares_numerics() {
        let pred = Predicate::Gt(Value::Int(10));
        assert!(pred.evaluate(&Value::Int(20)));
        assert!(!pred.evaluate(&Value::Int(10)));
        assert!(!pred.evaluate(&Value::Int(5)));
    }
    
    #[test]
    fn gt_promotes_int_to_float() {
        let pred = Predicate::Gt(Value::Float(10.5));
        assert!(pred.evaluate(&Value::Int(11)));
        assert!(!pred.evaluate(&Value::Int(10)));
    }
    
    #[test]
    fn between_inclusive_bounds() {
        let pred = Predicate::Between(Value::Int(10), Value::Int(20));
        assert!(pred.evaluate(&Value::Int(10)));  // inclusive lower
        assert!(pred.evaluate(&Value::Int(15)));
        assert!(pred.evaluate(&Value::Int(20)));  // inclusive upper
        assert!(!pred.evaluate(&Value::Int(9)));
        assert!(!pred.evaluate(&Value::Int(21)));
    }
    
    #[test]
    fn within_checks_membership() {
        let pred = Predicate::Within(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
        ]);
        assert!(pred.evaluate(&Value::Int(2)));
        assert!(!pred.evaluate(&Value::Int(4)));
    }
    
    #[test]
    fn containing_matches_substring() {
        let pred = Predicate::Containing("ello".to_string());
        assert!(pred.evaluate(&Value::String("Hello".to_string())));
        assert!(!pred.evaluate(&Value::String("Hi".to_string())));
    }
    
    #[test]
    fn and_requires_both() {
        let pred = Predicate::And(
            Box::new(Predicate::Gt(Value::Int(10))),
            Box::new(Predicate::Lt(Value::Int(20))),
        );
        assert!(pred.evaluate(&Value::Int(15)));
        assert!(!pred.evaluate(&Value::Int(5)));
        assert!(!pred.evaluate(&Value::Int(25)));
    }
    
    #[test]
    fn not_inverts_result() {
        let pred = Predicate::Not(Box::new(Predicate::Eq(Value::Int(42))));
        assert!(!pred.evaluate(&Value::Int(42)));
        assert!(pred.evaluate(&Value::Int(43)));
    }
}
```

### 8.2 Integration Tests

```rust
#[test]
fn has_predicate_filters_vertices() {
    let graph = create_test_graph();
    let g = graph.traversal();
    
    // Find people over 30
    let results = g.v()
        .has_label("person")
        .has("age", p::gt(30))
        .values("name")
        .to_list();
    
    assert!(results.contains(&Value::String("Bob".to_string())));
    assert!(!results.contains(&Value::String("Alice".to_string()))); // age 25
}

#[test]
fn is_step_filters_values() {
    let graph = create_test_graph();
    let g = graph.traversal();
    
    // Get ages over 30
    let results = g.v()
        .has_label("person")
        .values("age")
        .is(p::gt(30))
        .to_list();
    
    for result in &results {
        if let Value::Int(age) = result {
            assert!(*age > 30);
        }
    }
}
```

---

## 9. Performance Considerations

### 9.1 Predicate Evaluation

- **Avoid allocations**: Predicates should evaluate without allocating
- **Short-circuit logical ops**: `And` should fail fast on first false, `Or` on first true
- **Compile regex once**: For `Regex` predicates, compile the pattern on construction

### 9.2 IR Compilation

- **Batch similar ops**: Consecutive `HasLabel` ops can be merged
- **Reorder filters**: Push filters before navigation for early elimination
- **Detect dead paths**: Contradictory predicates can be eliminated

### 9.3 Step Fusion (Future)

Some operations can be fused for efficiency:
- `V().has('label', x)` → direct label index lookup
- `out().has('label', x)` → filtered adjacency traversal
- `values('x').is(p)` → property predicate without materialization

---

## 10. References

- [Gremlin Interface Design](./gremlin.md)
- [GQL Subset Design](./gql.md)
- [TinkerPop Predicates](https://tinkerpop.apache.org/docs/current/reference/#a-note-on-predicates)
- [ISO GQL Specification](https://www.iso.org/standard/76120.html)
