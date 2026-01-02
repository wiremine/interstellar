# Spec 05: Query IR and Predicate System

**Status**: Phase 2 Feature  
**Prerequisites**: spec-03 (Traversal Engine Core), spec-03a (Paths)  
**Estimated Effort**: 3-4 days

---

## 1. Overview

### 1.1 Purpose

This specification defines the implementation of a shared Query Intermediate Representation (IR) that enables both Gremlin bytecode and GQL AST to compile to a common format. The IR then compiles to executable traversal steps.

Key benefits:
1. **Unified execution**: Both query languages share the same execution engine
2. **Consistent semantics**: Equivalent operations behave identically
3. **Future optimization**: IR-level optimizations benefit both languages
4. **Reduced duplication**: Shared predicate evaluation, type coercion

### 1.2 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Query Interface Layer                         │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐              ┌──────────────────┐         │
│  │  Gremlin Parser  │              │   GQL Parser     │         │
│  │  (Text/Bytecode) │              │   (Text)         │         │
│  └────────┬─────────┘              └────────┬─────────┘         │
│           │                                  │                   │
│           ▼                                  ▼                   │
│  ┌──────────────────┐              ┌──────────────────┐         │
│  │ Gremlin Compiler │              │  GQL Compiler    │         │
│  │ bytecode → IR    │              │  AST → IR        │         │
│  └────────┬─────────┘              └────────┬─────────┘         │
│           │                                  │                   │
│           └───────────────┬──────────────────┘                   │
│                           ▼                                      │
│                 ┌──────────────────┐                             │
│                 │  Shared Query IR │  ◄── This spec              │
│                 │  (QueryPlan)     │                             │
│                 └────────┬─────────┘                             │
│                          ▼                                       │
│                 ┌──────────────────┐                             │
│                 │   IR Compiler    │                             │
│                 │   IR → Steps     │                             │
│                 └────────┬─────────┘                             │
│                          ▼                                       │
│                 ┌──────────────────┐                             │
│                 │ Vec<Box<AnyStep>>│                             │
│                 │  (Executable)    │                             │
│                 └──────────────────┘                             │
└─────────────────────────────────────────────────────────────────┘
```

### 1.3 Module Structure

```
src/query/
├── mod.rs              # Module entry, re-exports
├── types.rs            # Core enums (Direction, Scope, SortOrder)
├── predicate.rs        # Predicate enum with evaluate() method
├── ir.rs               # QueryOp enum and QueryPlan
└── compiler.rs         # IR → Steps compilation
```

### 1.4 Design Principles

1. **Step-oriented IR**: Operations map directly to traversal steps
2. **Value-based**: All literals use the existing `Value` type
3. **Composable**: Nested traversals represented as nested `QueryPlan`
4. **Extensible**: New operations can be added without breaking changes

---

## 2. Core Types (`types.rs`)

These enums are shared between Gremlin and GQL compilation.

### 2.1 Direction

```rust
/// Direction for edge traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// Outgoing edges (source → target)
    Out,
    /// Incoming edges (target ← source)
    In,
    /// Both directions
    Both,
}
```

### 2.2 SortOrder

```rust
/// Sort order for ORDER BY / order() step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SortOrder {
    /// Ascending (smallest first)
    Asc,
    /// Descending (largest first)
    Desc,
    /// Random shuffle
    Shuffle,
}
```

### 2.3 Scope

```rust
/// Scope for aggregation operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scope {
    /// Operate on local collection (within each traverser)
    Local,
    /// Operate on global stream (across all traversers)
    Global,
}
```

### 2.4 Token (T)

```rust
/// Token for accessing element metadata.
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
```

### 2.5 Query Language Mapping

| Enum | Gremlin Usage | GQL Usage |
|------|---------------|-----------|
| `Direction` | `out()`, `in()`, `both()` | `->`, `<-`, `-` patterns |
| `SortOrder` | `order().by(x, asc)` | `ORDER BY x ASC` |
| `Scope` | `dedup(local)`, `count(global)` | Implicit in context |
| `T` | `T.id`, `T.label` | `id(x)`, `labels(x)` |

---

## 3. Predicate System (`predicate.rs`)

### 3.1 Overview

The predicate system provides unified filtering conditions for both Gremlin's `P.*` predicates and GQL's WHERE expressions.

### 3.2 Predicate Enum

```rust
use crate::value::Value;

/// A predicate for filtering values.
///
/// Predicates evaluate against any `Value` and return a boolean result.
/// They support comparison, range, collection, string, and logical operations.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    // === Comparison ===
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
    
    // === Range ===
    /// Between (inclusive): low <= value <= high
    Between(Value, Value),
    /// Inside (exclusive): low < value < high
    Inside(Value, Value),
    /// Outside: value < low OR value > high
    Outside(Value, Value),
    
    // === Collection ===
    /// Within: value is in the collection
    Within(Vec<Value>),
    /// Without: value is not in the collection
    Without(Vec<Value>),
    
    // === String ===
    /// Contains substring
    Containing(String),
    /// Starts with prefix
    StartingWith(String),
    /// Ends with suffix
    EndingWith(String),
    /// Matches regex pattern
    Regex(String),
    
    // === Logical ===
    /// Logical AND: both predicates must match
    And(Box<Predicate>, Box<Predicate>),
    /// Logical OR: at least one predicate must match
    Or(Box<Predicate>, Box<Predicate>),
    /// Logical NOT: predicate must not match
    Not(Box<Predicate>),
}
```

### 3.3 Predicate Evaluation

```rust
impl Predicate {
    /// Evaluate this predicate against a value.
    ///
    /// Returns `true` if the value matches the predicate.
    pub fn evaluate(&self, value: &Value) -> bool {
        match self {
            // Comparison predicates
            Predicate::Eq(target) => value_eq(value, target),
            Predicate::Neq(target) => !value_eq(value, target),
            Predicate::Lt(target) => value_cmp(value, target) == Some(Ordering::Less),
            Predicate::Lte(target) => {
                matches!(value_cmp(value, target), Some(Ordering::Less | Ordering::Equal))
            }
            Predicate::Gt(target) => value_cmp(value, target) == Some(Ordering::Greater),
            Predicate::Gte(target) => {
                matches!(value_cmp(value, target), Some(Ordering::Greater | Ordering::Equal))
            }
            
            // Range predicates
            Predicate::Between(low, high) => {
                matches!(value_cmp(value, low), Some(Ordering::Greater | Ordering::Equal))
                    && matches!(value_cmp(value, high), Some(Ordering::Less | Ordering::Equal))
            }
            Predicate::Inside(low, high) => {
                value_cmp(value, low) == Some(Ordering::Greater)
                    && value_cmp(value, high) == Some(Ordering::Less)
            }
            Predicate::Outside(low, high) => {
                value_cmp(value, low) == Some(Ordering::Less)
                    || value_cmp(value, high) == Some(Ordering::Greater)
            }
            
            // Collection predicates
            Predicate::Within(values) => values.iter().any(|v| value_eq(value, v)),
            Predicate::Without(values) => !values.iter().any(|v| value_eq(value, v)),
            
            // String predicates
            Predicate::Containing(substr) => {
                if let Value::String(s) = value {
                    s.contains(substr)
                } else {
                    false
                }
            }
            Predicate::StartingWith(prefix) => {
                if let Value::String(s) = value {
                    s.starts_with(prefix)
                } else {
                    false
                }
            }
            Predicate::EndingWith(suffix) => {
                if let Value::String(s) = value {
                    s.ends_with(suffix)
                } else {
                    false
                }
            }
            Predicate::Regex(pattern) => {
                if let Value::String(s) = value {
                    // Use regex crate - pattern compiled at evaluation time
                    // Consider caching compiled regex for performance
                    regex::Regex::new(pattern)
                        .map(|re| re.is_match(s))
                        .unwrap_or(false)
                } else {
                    false
                }
            }
            
            // Logical predicates (short-circuit evaluation)
            Predicate::And(a, b) => a.evaluate(value) && b.evaluate(value),
            Predicate::Or(a, b) => a.evaluate(value) || b.evaluate(value),
            Predicate::Not(p) => !p.evaluate(value),
        }
    }
}
```

### 3.4 Type Coercion Rules

The helper functions `value_eq` and `value_cmp` implement these coercion rules:

```rust
use std::cmp::Ordering;

/// Compare two values for equality with type coercion.
fn value_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        // Same types: direct comparison
        (Value::Null, Value::Null) => true,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Int(x), Value::Int(y)) => x == y,
        (Value::Float(x), Value::Float(y)) => x == y,
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Vertex(x), Value::Vertex(y)) => x == y,
        (Value::Edge(x), Value::Edge(y)) => x == y,
        
        // Int/Float coercion
        (Value::Int(i), Value::Float(f)) | (Value::Float(f), Value::Int(i)) => {
            (*i as f64) == *f
        }
        
        // Type mismatches: not equal
        _ => false,
    }
}

/// Compare two values with ordering and type coercion.
/// Returns None for incomparable types.
fn value_cmp(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        // Null comparisons: only equality is meaningful
        (Value::Null, _) | (_, Value::Null) => None,
        
        // Same numeric types
        (Value::Int(x), Value::Int(y)) => Some(x.cmp(y)),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y),
        
        // Int/Float coercion: promote Int to Float
        (Value::Int(i), Value::Float(f)) => (*i as f64).partial_cmp(f),
        (Value::Float(f), Value::Int(i)) => f.partial_cmp(&(*i as f64)),
        
        // String comparison
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        
        // Bool: false < true
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        
        // Type mismatches: incomparable
        _ => None,
    }
}
```

### 3.5 Type Coercion Summary

| Comparison | Type A | Type B | Behavior |
|------------|--------|--------|----------|
| Any | Same | Same | Direct comparison |
| Any | Int | Float | Int promoted to Float |
| `Eq`/`Neq` | Null | Null | Equal |
| `Eq`/`Neq` | Null | Other | Not equal |
| `Lt`/`Gt`/etc | Null | Any | Returns `false` |
| Any | Mismatched | Mismatched | Returns `false` |

### 3.6 Query Language Mapping

| Predicate | Gremlin | GQL |
|-----------|---------|-----|
| `Eq(v)` | `P.eq(v)` | `= v` |
| `Neq(v)` | `P.neq(v)` | `<> v` |
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
| `Regex(p)` | `TextP.regex(p)` | `=~ p` |
| `And(a, b)` | `a.and(b)` | `a AND b` |
| `Or(a, b)` | `a.or(b)` | `a OR b` |
| `Not(p)` | `P.not(p)` | `NOT p` |

### 3.7 Builder Module (`p`)

Ergonomic predicate construction matching Gremlin's `P.*` API:

```rust
/// Predicate builder module (Gremlin P.* style).
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
    
    pub fn containing(substr: impl Into<String>) -> Predicate {
        Predicate::Containing(substr.into())
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

// Extension trait for combining predicates
impl Predicate {
    pub fn and(self, other: Predicate) -> Predicate {
        Predicate::And(Box::new(self), Box::new(other))
    }
    
    pub fn or(self, other: Predicate) -> Predicate {
        Predicate::Or(Box::new(self), Box::new(other))
    }
}
```

---

## 4. Query IR (`ir.rs`)

### 4.1 QueryOp Enum

Each `QueryOp` represents a single operation in the traversal pipeline:

```rust
use crate::value::{Value, VertexId, EdgeId};
use super::{Direction, Predicate, SortOrder, Scope};

/// A single operation in the query IR.
///
/// Each operation maps to one or more traversal steps during compilation.
#[derive(Debug, Clone)]
pub enum QueryOp {
    // === Source Operations ===
    
    /// Start from all vertices: g.V()
    AllVertices,
    /// Start from specific vertices: g.V(id1, id2, ...)
    Vertices(Vec<VertexId>),
    /// Start from all edges: g.E()
    AllEdges,
    /// Start from specific edges: g.E(id1, id2, ...)
    Edges(Vec<EdgeId>),
    /// Inject literal values: g.inject(v1, v2, ...)
    Inject(Vec<Value>),
    
    // === Navigation Operations ===
    
    /// Traverse to adjacent vertices: out(), in(), both()
    ToVertex {
        direction: Direction,
        labels: Vec<String>,
    },
    /// Traverse to incident edges: outE(), inE(), bothE()
    ToEdge {
        direction: Direction,
        labels: Vec<String>,
    },
    /// Get source vertex of edge: outV()
    OutVertex,
    /// Get target vertex of edge: inV()
    InVertex,
    /// Get both vertices of edge: bothV()
    BothVertices,
    
    // === Filter Operations ===
    
    /// Filter by element label: hasLabel("person")
    HasLabel(Vec<String>),
    /// Filter by property existence: has("age")
    HasProperty(String),
    /// Filter by property value with predicate: has("age", gt(30))
    HasValue {
        key: String,
        predicate: Predicate,
    },
    /// Filter by element ID: hasId(1, 2, 3)
    HasId(Vec<Value>),
    /// Generic filter with sub-traversal: filter(__.out().count().is(gt(0)))
    Filter(Box<QueryPlan>),
    /// Deduplicate by value: dedup()
    Dedup,
    /// Deduplicate by projected value: dedup().by("name")
    DedupBy(Box<QueryPlan>),
    /// Take first n elements: limit(10)
    Limit(usize),
    /// Skip first n elements: skip(10)
    Skip(usize),
    /// Take elements in range: range(10, 20)
    Range(usize, usize),
    /// Logical NOT filter: not(__.out())
    Not(Box<QueryPlan>),
    /// Logical AND filter: and(__.has("x"), __.has("y"))
    And(Vec<QueryPlan>),
    /// Logical OR filter: or(__.has("x"), __.has("y"))
    Or(Vec<QueryPlan>),
    /// Where filter: where(__.out().has("name", "Bob"))
    Where(Box<QueryPlan>),
    /// Filter current value by predicate: is(gt(10))
    Is(Predicate),
    
    // === Transform Operations ===
    
    /// Extract property values: values("name", "age")
    Values(Vec<String>),
    /// Get element ID: id()
    Id,
    /// Get element label: label()
    Label,
    /// Replace with constant: constant("found")
    Constant(Value),
    /// Get traversal path: path()
    Path,
    /// Label current position: as("a")
    As(String),
    /// Select labeled values: select("a", "b")
    Select(Vec<String>),
    /// Project to map: project("name", "age").by("name").by("age")
    Project {
        keys: Vec<String>,
        traversals: Vec<QueryPlan>,
    },
    /// Unfold collections: unfold()
    Unfold,
    /// Fold into list: fold()
    Fold,
    /// Group by key: group().by("label").by(count())
    GroupBy {
        key_traversal: Box<QueryPlan>,
        value_traversal: Option<Box<QueryPlan>>,
    },
    /// Order results: order().by("name", asc)
    Order {
        comparators: Vec<(QueryPlan, SortOrder)>,
    },
    
    // === Branching Operations ===
    
    /// Union of traversals: union(__.out(), __.in())
    Union(Vec<QueryPlan>),
    /// First non-empty traversal: coalesce(__.out("a"), __.out("b"))
    Coalesce(Vec<QueryPlan>),
    /// Optional traversal (identity if empty): optional(__.out())
    Optional(Box<QueryPlan>),
    /// Repeat traversal: repeat(__.out()).times(3)
    Repeat {
        traversal: Box<QueryPlan>,
        until: Option<Box<QueryPlan>>,
        emit: Option<Box<QueryPlan>>,
        times: Option<usize>,
    },
    /// Local scope: local(__.limit(1))
    Local(Box<QueryPlan>),
    
    // === Side Effect Operations ===
    
    /// Store values lazily: store("x")
    Store(String),
    /// Aggregate values eagerly: aggregate("x")
    Aggregate(String),
    /// Emit side effect collection: cap("x")
    Cap(String),
    
    // === Terminal Operations (reduce traversers to single result) ===
    
    /// Count results: count()
    Count,
    /// Sum numeric values: sum()
    Sum,
    /// Minimum value: min()
    Min,
    /// Maximum value: max()
    Max,
    /// Average value: mean()
    Mean,
}
```

### 4.2 QueryPlan Structure

```rust
/// A query plan is a sequence of operations to execute.
#[derive(Debug, Clone, Default)]
pub struct QueryPlan {
    /// Operations to execute in order
    pub ops: Vec<QueryOp>,
}

impl QueryPlan {
    /// Create an empty query plan.
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }
    
    /// Create a plan with a single operation.
    pub fn single(op: QueryOp) -> Self {
        Self { ops: vec![op] }
    }
    
    /// Add an operation to the plan.
    pub fn push(&mut self, op: QueryOp) {
        self.ops.push(op);
    }
    
    /// Chain an operation, returning self for fluent building.
    pub fn with(mut self, op: QueryOp) -> Self {
        self.ops.push(op);
        self
    }
    
    /// Check if the plan is empty.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
    
    /// Get the number of operations.
    pub fn len(&self) -> usize {
        self.ops.len()
    }
}
```

### 4.3 IR Examples

**Gremlin**: `g.V().has("person", "name", "Alice").out("knows").values("name")`

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

**GQL**: `MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(friend) RETURN friend.name`

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
        QueryOp::As("friend".to_string()),
        QueryOp::Select(vec!["friend".to_string()]),
        QueryOp::Values(vec!["name".to_string()]),
    ],
}
```

---

## 5. IR Compiler (`compiler.rs`)

### 5.1 Overview

The IR compiler transforms a `QueryPlan` into executable `Vec<Box<dyn AnyStep>>`. It maps each `QueryOp` to one or more existing steps, and handles nested traversals recursively.

### 5.2 Compilation Errors

```rust
use thiserror::Error;

/// Errors that can occur during IR compilation.
#[derive(Debug, Error)]
pub enum CompileError {
    /// Operation not yet supported
    #[error("unsupported operation: {0}")]
    UnsupportedOp(String),
    
    /// Invalid regex pattern in predicate
    #[error("invalid regex pattern: {pattern}")]
    InvalidRegex {
        pattern: String,
        #[source]
        source: regex::Error,
    },
    
    /// Empty sub-traversal where one is required
    #[error("empty traversal in {context}")]
    EmptyTraversal { context: String },
    
    /// Invalid operation combination
    #[error("invalid operation: {0}")]
    InvalidOp(String),
}
```

### 5.3 Compiler Implementation

```rust
use crate::traversal::step::AnyStep;
use crate::traversal::{
    StartStep, IdentityStep,
    HasLabelStep, HasStep, HasValueStep, HasIdStep,
    DedupStep, LimitStep, SkipStep, RangeStep,
    OutStep, InStep, BothStep,
    OutEStep, InEStep, BothEStep,
    OutVStep, InVStep, BothVStep,
    ValuesStep, IdStep, LabelStep, ConstantStep,
    PathStep, AsStep, SelectStep,
};

impl QueryPlan {
    /// Compile this query plan into executable steps.
    ///
    /// Returns a vector of type-erased steps that can be executed
    /// by the traversal engine.
    pub fn compile(&self) -> Result<Vec<Box<dyn AnyStep>>, CompileError> {
        let mut steps: Vec<Box<dyn AnyStep>> = Vec::new();
        
        for op in &self.ops {
            self.compile_op(op, &mut steps)?;
        }
        
        Ok(steps)
    }
    
    /// Compile a single operation, potentially adding multiple steps.
    fn compile_op(
        &self,
        op: &QueryOp,
        steps: &mut Vec<Box<dyn AnyStep>>,
    ) -> Result<(), CompileError> {
        match op {
            // === Source Operations ===
            QueryOp::AllVertices => {
                steps.push(Box::new(StartStep::all_vertices()));
            }
            QueryOp::Vertices(ids) => {
                steps.push(Box::new(StartStep::vertices(ids.clone())));
            }
            QueryOp::AllEdges => {
                steps.push(Box::new(StartStep::all_edges()));
            }
            QueryOp::Edges(ids) => {
                steps.push(Box::new(StartStep::edges(ids.clone())));
            }
            QueryOp::Inject(values) => {
                steps.push(Box::new(StartStep::inject(values.clone())));
            }
            
            // === Navigation Operations ===
            QueryOp::ToVertex { direction, labels } => {
                let step: Box<dyn AnyStep> = match direction {
                    Direction::Out => {
                        if labels.is_empty() {
                            Box::new(OutStep::new())
                        } else {
                            Box::new(OutStep::with_labels(labels.clone()))
                        }
                    }
                    Direction::In => {
                        if labels.is_empty() {
                            Box::new(InStep::new())
                        } else {
                            Box::new(InStep::with_labels(labels.clone()))
                        }
                    }
                    Direction::Both => {
                        if labels.is_empty() {
                            Box::new(BothStep::new())
                        } else {
                            Box::new(BothStep::with_labels(labels.clone()))
                        }
                    }
                };
                steps.push(step);
            }
            QueryOp::ToEdge { direction, labels } => {
                let step: Box<dyn AnyStep> = match direction {
                    Direction::Out => {
                        if labels.is_empty() {
                            Box::new(OutEStep::new())
                        } else {
                            Box::new(OutEStep::with_labels(labels.clone()))
                        }
                    }
                    Direction::In => {
                        if labels.is_empty() {
                            Box::new(InEStep::new())
                        } else {
                            Box::new(InEStep::with_labels(labels.clone()))
                        }
                    }
                    Direction::Both => {
                        if labels.is_empty() {
                            Box::new(BothEStep::new())
                        } else {
                            Box::new(BothEStep::with_labels(labels.clone()))
                        }
                    }
                };
                steps.push(step);
            }
            QueryOp::OutVertex => {
                steps.push(Box::new(OutVStep::new()));
            }
            QueryOp::InVertex => {
                steps.push(Box::new(InVStep::new()));
            }
            QueryOp::BothVertices => {
                steps.push(Box::new(BothVStep::new()));
            }
            
            // === Filter Operations ===
            QueryOp::HasLabel(labels) => {
                steps.push(Box::new(HasLabelStep::new(labels.clone())));
            }
            QueryOp::HasProperty(key) => {
                steps.push(Box::new(HasStep::new(key.clone())));
            }
            QueryOp::HasValue { key, predicate } => {
                // Use HasPredicateStep (new step, see section 6)
                steps.push(Box::new(HasPredicateStep::new(
                    key.clone(),
                    predicate.clone(),
                )));
            }
            QueryOp::HasId(ids) => {
                steps.push(Box::new(HasIdStep::from_values(ids.clone())));
            }
            QueryOp::Dedup => {
                steps.push(Box::new(DedupStep::new()));
            }
            QueryOp::Limit(n) => {
                steps.push(Box::new(LimitStep::new(*n)));
            }
            QueryOp::Skip(n) => {
                steps.push(Box::new(SkipStep::new(*n)));
            }
            QueryOp::Range(start, end) => {
                steps.push(Box::new(RangeStep::new(*start, *end)));
            }
            QueryOp::Is(predicate) => {
                // Use IsStep (new step, see section 6)
                steps.push(Box::new(IsStep::new(predicate.clone())));
            }
            QueryOp::Filter(sub_plan) => {
                // Use FilterTraversalStep (new step, see section 6)
                let sub_steps = sub_plan.compile()?;
                steps.push(Box::new(FilterTraversalStep::new(sub_steps)));
            }
            QueryOp::Not(sub_plan) => {
                // Use NotStep (new step, see section 6)
                let sub_steps = sub_plan.compile()?;
                steps.push(Box::new(NotStep::new(sub_steps)));
            }
            QueryOp::And(plans) => {
                // Use AndStep (new step, see section 6)
                let sub_traversals: Vec<_> = plans
                    .iter()
                    .map(|p| p.compile())
                    .collect::<Result<_, _>>()?;
                steps.push(Box::new(AndStep::new(sub_traversals)));
            }
            QueryOp::Or(plans) => {
                // Use OrStep (new step, see section 6)
                let sub_traversals: Vec<_> = plans
                    .iter()
                    .map(|p| p.compile())
                    .collect::<Result<_, _>>()?;
                steps.push(Box::new(OrStep::new(sub_traversals)));
            }
            QueryOp::Where(sub_plan) => {
                // Where is semantically equivalent to Filter
                let sub_steps = sub_plan.compile()?;
                steps.push(Box::new(FilterTraversalStep::new(sub_steps)));
            }
            
            // === Transform Operations ===
            QueryOp::Values(keys) => {
                if keys.len() == 1 {
                    steps.push(Box::new(ValuesStep::new(keys[0].clone())));
                } else {
                    steps.push(Box::new(ValuesStep::from_keys(keys.clone())));
                }
            }
            QueryOp::Id => {
                steps.push(Box::new(IdStep::new()));
            }
            QueryOp::Label => {
                steps.push(Box::new(LabelStep::new()));
            }
            QueryOp::Constant(value) => {
                steps.push(Box::new(ConstantStep::new(value.clone())));
            }
            QueryOp::Path => {
                steps.push(Box::new(PathStep::new()));
            }
            QueryOp::As(label) => {
                steps.push(Box::new(AsStep::new(label.clone())));
            }
            QueryOp::Select(labels) => {
                if labels.len() == 1 {
                    steps.push(Box::new(SelectStep::single(&labels[0])));
                } else {
                    steps.push(Box::new(SelectStep::new(labels.clone())));
                }
            }
            
            // === Branching Operations ===
            QueryOp::Union(plans) => {
                let sub_traversals: Vec<_> = plans
                    .iter()
                    .map(|p| p.compile())
                    .collect::<Result<_, _>>()?;
                steps.push(Box::new(UnionStep::new(sub_traversals)));
            }
            QueryOp::Coalesce(plans) => {
                let sub_traversals: Vec<_> = plans
                    .iter()
                    .map(|p| p.compile())
                    .collect::<Result<_, _>>()?;
                steps.push(Box::new(CoalesceStep::new(sub_traversals)));
            }
            QueryOp::Optional(sub_plan) => {
                let sub_steps = sub_plan.compile()?;
                steps.push(Box::new(OptionalStep::new(sub_steps)));
            }
            QueryOp::Local(sub_plan) => {
                let sub_steps = sub_plan.compile()?;
                steps.push(Box::new(LocalStep::new(sub_steps)));
            }
            
            // === Operations requiring new steps (Phase 2+) ===
            QueryOp::DedupBy(_) |
            QueryOp::Project { .. } |
            QueryOp::Unfold |
            QueryOp::Fold |
            QueryOp::GroupBy { .. } |
            QueryOp::Order { .. } |
            QueryOp::Repeat { .. } |
            QueryOp::Store(_) |
            QueryOp::Aggregate(_) |
            QueryOp::Cap(_) |
            QueryOp::Count |
            QueryOp::Sum |
            QueryOp::Min |
            QueryOp::Max |
            QueryOp::Mean => {
                return Err(CompileError::UnsupportedOp(format!("{:?}", op)));
            }
        }
        
        Ok(())
    }
}
```

### 5.4 QueryOp to Step Mapping

| QueryOp | Existing Step | New Step Needed |
|---------|--------------|-----------------|
| `AllVertices` | `StartStep::all_vertices()` | - |
| `Vertices(ids)` | `StartStep::vertices(ids)` | - |
| `AllEdges` | `StartStep::all_edges()` | - |
| `Edges(ids)` | `StartStep::edges(ids)` | - |
| `Inject(values)` | `StartStep::inject(values)` | - |
| `ToVertex{Out}` | `OutStep` | - |
| `ToVertex{In}` | `InStep` | - |
| `ToVertex{Both}` | `BothStep` | - |
| `ToEdge{Out}` | `OutEStep` | - |
| `ToEdge{In}` | `InEStep` | - |
| `ToEdge{Both}` | `BothEStep` | - |
| `OutVertex` | `OutVStep` | - |
| `InVertex` | `InVStep` | - |
| `BothVertices` | `BothVStep` | - |
| `HasLabel` | `HasLabelStep` | - |
| `HasProperty` | `HasStep` | - |
| `HasValue` | - | `HasPredicateStep` |
| `HasId` | `HasIdStep` | - |
| `Dedup` | `DedupStep` | - |
| `Limit` | `LimitStep` | - |
| `Skip` | `SkipStep` | - |
| `Range` | `RangeStep` | - |
| `Is` | - | `IsStep` |
| `Filter` | - | `FilterTraversalStep` |
| `Not` | - | `NotStep` |
| `And` | - | `AndStep` |
| `Or` | - | `OrStep` |
| `Where` | - | `FilterTraversalStep` (reuse) |
| `Values` | `ValuesStep` | - |
| `Id` | `IdStep` | - |
| `Label` | `LabelStep` | - |
| `Constant` | `ConstantStep` | - |
| `Path` | `PathStep` | - |
| `As` | `AsStep` | - |
| `Select` | `SelectStep` | - |
| `Union` | - | `UnionStep` |
| `Coalesce` | - | `CoalesceStep` |
| `Optional` | - | `OptionalStep` |
| `Local` | - | `LocalStep` |
| `DedupBy` | - | Phase 2 |
| `Project` | - | Phase 2 |
| `Unfold` | - | Phase 2 |
| `Fold` | - | Phase 2 |
| `GroupBy` | - | Phase 2 |
| `Order` | - | Phase 2 |
| `Repeat` | - | Phase 2 |
| `Store` | - | Phase 2 |
| `Aggregate` | - | Phase 2 |
| `Cap` | - | Phase 2 |
| `Count` | - | Phase 2 |
| `Sum` | - | Phase 2 |
| `Min` | - | Phase 2 |
| `Max` | - | Phase 2 |
| `Mean` | - | Phase 2 |

---

## 6. New Steps Required

This section defines the new steps needed to support IR compilation.

### 6.1 HasPredicateStep

Filter elements by property value using a `Predicate`:

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

### 6.2 IsStep

Filter current traverser value against a predicate:

```rust
/// Filter step that keeps traversers whose value matches a predicate.
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

### 6.3 FilterTraversalStep

Filter based on whether a sub-traversal produces any results:

```rust
/// Filter step that keeps traversers where a sub-traversal produces results.
#[derive(Clone)]
pub struct FilterTraversalStep {
    sub_steps: Vec<Box<dyn AnyStep>>,
}

impl FilterTraversalStep {
    pub fn new(sub_steps: Vec<Box<dyn AnyStep>>) -> Self {
        Self { sub_steps }
    }
}

impl AnyStep for FilterTraversalStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let steps = self.sub_steps.clone();
        Box::new(input.filter(move |t| {
            // Execute sub-traversal with this traverser as input
            let sub_input: Box<dyn Iterator<Item = Traverser>> = 
                Box::new(std::iter::once(t.clone()));
            let mut sub_output = execute_traversal(ctx, &steps, sub_input);
            // Keep traverser if sub-traversal produces at least one result
            sub_output.next().is_some()
        }))
    }
    
    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }
    
    fn name(&self) -> &'static str {
        "filter"
    }
}
```

### 6.4 NotStep

Logical NOT - keeps traversers where sub-traversal produces NO results:

```rust
/// Filter step that keeps traversers where a sub-traversal produces NO results.
#[derive(Clone)]
pub struct NotStep {
    sub_steps: Vec<Box<dyn AnyStep>>,
}

impl NotStep {
    pub fn new(sub_steps: Vec<Box<dyn AnyStep>>) -> Self {
        Self { sub_steps }
    }
}

impl AnyStep for NotStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let steps = self.sub_steps.clone();
        Box::new(input.filter(move |t| {
            let sub_input: Box<dyn Iterator<Item = Traverser>> = 
                Box::new(std::iter::once(t.clone()));
            let mut sub_output = execute_traversal(ctx, &steps, sub_input);
            // Keep traverser if sub-traversal produces NO results
            sub_output.next().is_none()
        }))
    }
    
    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }
    
    fn name(&self) -> &'static str {
        "not"
    }
}
```

### 6.5 AndStep

Logical AND - keeps traversers where ALL sub-traversals produce results:

```rust
/// Filter step that keeps traversers where all sub-traversals produce results.
#[derive(Clone)]
pub struct AndStep {
    sub_traversals: Vec<Vec<Box<dyn AnyStep>>>,
}

impl AndStep {
    pub fn new(sub_traversals: Vec<Vec<Box<dyn AnyStep>>>) -> Self {
        Self { sub_traversals }
    }
}

impl AnyStep for AndStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let traversals = self.sub_traversals.clone();
        Box::new(input.filter(move |t| {
            // All sub-traversals must produce at least one result
            traversals.iter().all(|steps| {
                let sub_input: Box<dyn Iterator<Item = Traverser>> = 
                    Box::new(std::iter::once(t.clone()));
                let mut sub_output = execute_traversal(ctx, steps, sub_input);
                sub_output.next().is_some()
            })
        }))
    }
    
    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }
    
    fn name(&self) -> &'static str {
        "and"
    }
}
```

### 6.6 OrStep

Logical OR - keeps traversers where ANY sub-traversal produces results:

```rust
/// Filter step that keeps traversers where at least one sub-traversal produces results.
#[derive(Clone)]
pub struct OrStep {
    sub_traversals: Vec<Vec<Box<dyn AnyStep>>>,
}

impl OrStep {
    pub fn new(sub_traversals: Vec<Vec<Box<dyn AnyStep>>>) -> Self {
        Self { sub_traversals }
    }
}

impl AnyStep for OrStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let traversals = self.sub_traversals.clone();
        Box::new(input.filter(move |t| {
            // At least one sub-traversal must produce a result
            traversals.iter().any(|steps| {
                let sub_input: Box<dyn Iterator<Item = Traverser>> = 
                    Box::new(std::iter::once(t.clone()));
                let mut sub_output = execute_traversal(ctx, steps, sub_input);
                sub_output.next().is_some()
            })
        }))
    }
    
    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }
    
    fn name(&self) -> &'static str {
        "or"
    }
}
```

### 6.7 UnionStep

Execute multiple traversals and merge results:

```rust
/// Branching step that executes multiple traversals and merges all results.
#[derive(Clone)]
pub struct UnionStep {
    sub_traversals: Vec<Vec<Box<dyn AnyStep>>>,
}

impl UnionStep {
    pub fn new(sub_traversals: Vec<Vec<Box<dyn AnyStep>>>) -> Self {
        Self { sub_traversals }
    }
}

impl AnyStep for UnionStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Collect input since we need to process it multiple times
        let input_vec: Vec<Traverser> = input.collect();
        let traversals = self.sub_traversals.clone();
        
        Box::new(traversals.into_iter().flat_map(move |steps| {
            let sub_input: Box<dyn Iterator<Item = Traverser>> = 
                Box::new(input_vec.clone().into_iter());
            execute_traversal(ctx, &steps, sub_input).collect::<Vec<_>>()
        }))
    }
    
    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }
    
    fn name(&self) -> &'static str {
        "union"
    }
}
```

### 6.8 CoalesceStep

Execute traversals in order, return first non-empty:

```rust
/// Branching step that returns results from first non-empty traversal.
#[derive(Clone)]
pub struct CoalesceStep {
    sub_traversals: Vec<Vec<Box<dyn AnyStep>>>,
}

impl CoalesceStep {
    pub fn new(sub_traversals: Vec<Vec<Box<dyn AnyStep>>>) -> Self {
        Self { sub_traversals }
    }
}

impl AnyStep for CoalesceStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let traversals = self.sub_traversals.clone();
        
        Box::new(input.flat_map(move |t| {
            // Try each traversal in order
            for steps in &traversals {
                let sub_input: Box<dyn Iterator<Item = Traverser>> = 
                    Box::new(std::iter::once(t.clone()));
                let results: Vec<_> = execute_traversal(ctx, steps, sub_input).collect();
                if !results.is_empty() {
                    return results;
                }
            }
            // No traversal produced results
            Vec::new()
        }))
    }
    
    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }
    
    fn name(&self) -> &'static str {
        "coalesce"
    }
}
```

### 6.9 OptionalStep

Execute traversal, return identity if empty:

```rust
/// Branching step that returns traversal results or identity if empty.
#[derive(Clone)]
pub struct OptionalStep {
    sub_steps: Vec<Box<dyn AnyStep>>,
}

impl OptionalStep {
    pub fn new(sub_steps: Vec<Box<dyn AnyStep>>) -> Self {
        Self { sub_steps }
    }
}

impl AnyStep for OptionalStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let steps = self.sub_steps.clone();
        
        Box::new(input.flat_map(move |t| {
            let sub_input: Box<dyn Iterator<Item = Traverser>> = 
                Box::new(std::iter::once(t.clone()));
            let results: Vec<_> = execute_traversal(ctx, &steps, sub_input).collect();
            if results.is_empty() {
                // Return identity (the original traverser)
                vec![t]
            } else {
                results
            }
        }))
    }
    
    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }
    
    fn name(&self) -> &'static str {
        "optional"
    }
}
```

### 6.10 LocalStep

Execute traversal in local scope (per-traverser collection operations):

```rust
/// Step that executes a sub-traversal in local scope.
#[derive(Clone)]
pub struct LocalStep {
    sub_steps: Vec<Box<dyn AnyStep>>,
}

impl LocalStep {
    pub fn new(sub_steps: Vec<Box<dyn AnyStep>>) -> Self {
        Self { sub_steps }
    }
}

impl AnyStep for LocalStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let steps = self.sub_steps.clone();
        
        Box::new(input.flat_map(move |t| {
            let sub_input: Box<dyn Iterator<Item = Traverser>> = 
                Box::new(std::iter::once(t));
            execute_traversal(ctx, &steps, sub_input).collect::<Vec<_>>()
        }))
    }
    
    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }
    
    fn name(&self) -> &'static str {
        "local"
    }
}
```

---

## 7. Testing Strategy

### 7.1 Predicate Unit Tests

```rust
#[cfg(test)]
mod predicate_tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn eq_matches_same_value() {
        let pred = Predicate::Eq(Value::Int(42));
        assert!(pred.evaluate(&Value::Int(42)));
        assert!(!pred.evaluate(&Value::Int(43)));
    }
    
    #[test]
    fn eq_matches_null() {
        let pred = Predicate::Eq(Value::Null);
        assert!(pred.evaluate(&Value::Null));
        assert!(!pred.evaluate(&Value::Int(0)));
    }
    
    #[test]
    fn neq_inverts_eq() {
        let pred = Predicate::Neq(Value::Int(42));
        assert!(!pred.evaluate(&Value::Int(42)));
        assert!(pred.evaluate(&Value::Int(43)));
    }
    
    #[test]
    fn gt_compares_integers() {
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
    fn lt_returns_false_for_null() {
        let pred = Predicate::Lt(Value::Int(10));
        assert!(!pred.evaluate(&Value::Null));
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
    fn inside_exclusive_bounds() {
        let pred = Predicate::Inside(Value::Int(10), Value::Int(20));
        assert!(!pred.evaluate(&Value::Int(10))); // exclusive lower
        assert!(pred.evaluate(&Value::Int(15)));
        assert!(!pred.evaluate(&Value::Int(20))); // exclusive upper
    }
    
    #[test]
    fn outside_range() {
        let pred = Predicate::Outside(Value::Int(10), Value::Int(20));
        assert!(pred.evaluate(&Value::Int(5)));
        assert!(pred.evaluate(&Value::Int(25)));
        assert!(!pred.evaluate(&Value::Int(15)));
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
    fn without_checks_non_membership() {
        let pred = Predicate::Without(vec![Value::Int(1), Value::Int(2)]);
        assert!(!pred.evaluate(&Value::Int(1)));
        assert!(pred.evaluate(&Value::Int(3)));
    }
    
    #[test]
    fn containing_matches_substring() {
        let pred = Predicate::Containing("ello".to_string());
        assert!(pred.evaluate(&Value::String("Hello".to_string())));
        assert!(!pred.evaluate(&Value::String("Hi".to_string())));
        assert!(!pred.evaluate(&Value::Int(42))); // non-string
    }
    
    #[test]
    fn starting_with_matches_prefix() {
        let pred = Predicate::StartingWith("He".to_string());
        assert!(pred.evaluate(&Value::String("Hello".to_string())));
        assert!(!pred.evaluate(&Value::String("hello".to_string()))); // case-sensitive
    }
    
    #[test]
    fn ending_with_matches_suffix() {
        let pred = Predicate::EndingWith("llo".to_string());
        assert!(pred.evaluate(&Value::String("Hello".to_string())));
        assert!(!pred.evaluate(&Value::String("Help".to_string())));
    }
    
    #[test]
    fn regex_matches_pattern() {
        let pred = Predicate::Regex(r"^\d{3}-\d{4}$".to_string());
        assert!(pred.evaluate(&Value::String("123-4567".to_string())));
        assert!(!pred.evaluate(&Value::String("12-34567".to_string())));
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
    fn or_requires_either() {
        let pred = Predicate::Or(
            Box::new(Predicate::Lt(Value::Int(10))),
            Box::new(Predicate::Gt(Value::Int(20))),
        );
        assert!(pred.evaluate(&Value::Int(5)));
        assert!(pred.evaluate(&Value::Int(25)));
        assert!(!pred.evaluate(&Value::Int(15)));
    }
    
    #[test]
    fn not_inverts_result() {
        let pred = Predicate::Not(Box::new(Predicate::Eq(Value::Int(42))));
        assert!(!pred.evaluate(&Value::Int(42)));
        assert!(pred.evaluate(&Value::Int(43)));
    }
    
    #[test]
    fn type_mismatch_returns_false() {
        let pred = Predicate::Gt(Value::Int(10));
        assert!(!pred.evaluate(&Value::String("hello".to_string())));
        assert!(!pred.evaluate(&Value::Bool(true)));
    }
}
```

### 7.2 IR Compilation Tests

```rust
#[cfg(test)]
mod ir_compile_tests {
    use super::*;
    
    #[test]
    fn compile_empty_plan() {
        let plan = QueryPlan::new();
        let steps = plan.compile().unwrap();
        assert!(steps.is_empty());
    }
    
    #[test]
    fn compile_all_vertices() {
        let plan = QueryPlan::single(QueryOp::AllVertices);
        let steps = plan.compile().unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].name(), "start");
    }
    
    #[test]
    fn compile_navigation_chain() {
        let plan = QueryPlan::new()
            .with(QueryOp::AllVertices)
            .with(QueryOp::HasLabel(vec!["person".to_string()]))
            .with(QueryOp::ToVertex {
                direction: Direction::Out,
                labels: vec!["knows".to_string()],
            })
            .with(QueryOp::Values(vec!["name".to_string()]));
        
        let steps = plan.compile().unwrap();
        assert_eq!(steps.len(), 4);
        assert_eq!(steps[0].name(), "start");
        assert_eq!(steps[1].name(), "hasLabel");
        assert_eq!(steps[2].name(), "out");
        assert_eq!(steps[3].name(), "values");
    }
    
    #[test]
    fn compile_predicate_filter() {
        let plan = QueryPlan::new()
            .with(QueryOp::AllVertices)
            .with(QueryOp::HasValue {
                key: "age".to_string(),
                predicate: Predicate::Gte(Value::Int(18)),
            });
        
        let steps = plan.compile().unwrap();
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[1].name(), "has");
    }
    
    #[test]
    fn compile_nested_filter() {
        let sub_plan = QueryPlan::new()
            .with(QueryOp::ToVertex {
                direction: Direction::Out,
                labels: vec![],
            });
        
        let plan = QueryPlan::new()
            .with(QueryOp::AllVertices)
            .with(QueryOp::Filter(Box::new(sub_plan)));
        
        let steps = plan.compile().unwrap();
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[1].name(), "filter");
    }
    
    #[test]
    fn compile_unsupported_op_returns_error() {
        let plan = QueryPlan::single(QueryOp::Count);
        let result = plan.compile();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CompileError::UnsupportedOp(_)));
    }
}
```

### 7.3 Integration Tests

```rust
#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::graph::Graph;
    use crate::storage::InMemoryGraph;
    
    fn create_test_graph() -> Graph {
        let mut storage = InMemoryGraph::new();
        
        // Create people
        let alice = storage.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props.insert("age".to_string(), Value::Int(30));
            props
        });
        let bob = storage.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Bob".to_string()));
            props.insert("age".to_string(), Value::Int(25));
            props
        });
        let charlie = storage.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Charlie".to_string()));
            props.insert("age".to_string(), Value::Int(35));
            props
        });
        
        // Create edges
        storage.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
        storage.add_edge(bob, charlie, "knows", HashMap::new()).unwrap();
        
        Graph::new(Arc::new(storage))
    }
    
    #[test]
    fn execute_compiled_query_plan() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let ctx = ExecutionContext::new(&snapshot, snapshot.interner());
        
        // Build IR: g.V().hasLabel("person").has("age", gte(30)).values("name")
        let plan = QueryPlan::new()
            .with(QueryOp::AllVertices)
            .with(QueryOp::HasLabel(vec!["person".to_string()]))
            .with(QueryOp::HasValue {
                key: "age".to_string(),
                predicate: Predicate::Gte(Value::Int(30)),
            })
            .with(QueryOp::Values(vec!["name".to_string()]));
        
        // Compile to steps
        let steps = plan.compile().unwrap();
        
        // Execute
        let input: Box<dyn Iterator<Item = Traverser>> = Box::new(std::iter::empty());
        let results: Vec<Value> = execute_traversal(&ctx, &steps, input)
            .map(|t| t.value)
            .collect();
        
        // Should return Alice and Charlie (age >= 30)
        assert_eq!(results.len(), 2);
        assert!(results.contains(&Value::String("Alice".to_string())));
        assert!(results.contains(&Value::String("Charlie".to_string())));
    }
    
    #[test]
    fn predicate_builder_api() {
        use crate::query::p;
        
        // g.V().has("age", p.between(25, 35))
        let pred = p::between(25i64, 35i64);
        
        assert!(pred.evaluate(&Value::Int(25)));
        assert!(pred.evaluate(&Value::Int(30)));
        assert!(pred.evaluate(&Value::Int(35)));
        assert!(!pred.evaluate(&Value::Int(20)));
        assert!(!pred.evaluate(&Value::Int(40)));
    }
    
    #[test]
    fn combined_predicates() {
        use crate::query::p;
        
        // age >= 18 AND age < 65
        let pred = p::gte(18i64).and(p::lt(65i64));
        
        assert!(pred.evaluate(&Value::Int(30)));
        assert!(pred.evaluate(&Value::Int(18)));
        assert!(!pred.evaluate(&Value::Int(17)));
        assert!(!pred.evaluate(&Value::Int(65)));
    }
}
```

---

## 8. Implementation Phases

### Phase 1: Foundation (This Spec)

**Goal**: Core types, predicates, and IR structure.

- [ ] `src/query/mod.rs` - Module structure, re-exports
- [ ] `src/query/types.rs` - Direction, SortOrder, Scope, T enums
- [ ] `src/query/predicate.rs` - Predicate enum with `evaluate()`
- [ ] `src/query/predicate.rs` - `p` builder module
- [ ] Unit tests for all predicates

**Estimated**: 1 day

### Phase 2: IR and Compiler

**Goal**: QueryOp, QueryPlan, and compilation to existing steps.

- [ ] `src/query/ir.rs` - QueryOp enum, QueryPlan struct
- [ ] `src/query/compiler.rs` - CompileError, QueryPlan::compile()
- [ ] `src/traversal/filter.rs` - HasPredicateStep, IsStep
- [ ] `src/traversal/filter.rs` - FilterTraversalStep, NotStep, AndStep, OrStep
- [ ] Integration with existing steps

**Estimated**: 1.5 days

### Phase 3: Branching Steps

**Goal**: Union, coalesce, optional, local steps.

- [ ] `src/traversal/branch.rs` - New module for branching steps
- [ ] UnionStep, CoalesceStep, OptionalStep, LocalStep
- [ ] Integration tests

**Estimated**: 1 day

### Phase 4: Fluent API Integration

**Goal**: Extend BoundTraversal to use predicates.

- [ ] Add `has(key, predicate)` method to BoundTraversal
- [ ] Add `is(predicate)` method
- [ ] Add `where_(traversal)`, `not_(traversal)`, `and_(...)`, `or_(...)`
- [ ] Add `union(...)`, `coalesce(...)`, `optional(...)`
- [ ] Documentation and examples

**Estimated**: 0.5 day

---

## 9. Dependencies

### New Crate Dependencies

```toml
[dependencies]
regex = "1.10"  # For Predicate::Regex
```

### Module Dependencies

```
src/query/
├── mod.rs         → re-exports from types, predicate, ir, compiler
├── types.rs       → standalone
├── predicate.rs   → depends on value.rs
├── ir.rs          → depends on types, predicate, value
└── compiler.rs    → depends on ir, traversal/step
```

---

## 10. Acceptance Criteria

### Predicates

- [ ] All 17 predicate variants implemented with `evaluate()`
- [ ] Int/Float type coercion works correctly
- [ ] Null handling follows documented semantics
- [ ] Logical predicates short-circuit correctly
- [ ] Builder module `p` provides ergonomic construction
- [ ] 100% branch coverage on predicate evaluation

### IR

- [ ] All QueryOp variants defined
- [ ] QueryPlan supports fluent building
- [ ] Compilation maps to existing steps where possible
- [ ] Nested traversals compile recursively
- [ ] Unsupported ops return clear errors

### New Steps

- [ ] HasPredicateStep filters by property predicate
- [ ] IsStep filters current value by predicate
- [ ] FilterTraversalStep/NotStep/AndStep/OrStep handle sub-traversals
- [ ] UnionStep merges multiple traversal results
- [ ] CoalesceStep returns first non-empty
- [ ] OptionalStep returns identity when empty
- [ ] All new steps are cloneable and thread-safe

### Integration

- [ ] Compiled QueryPlan executes correctly
- [ ] Results match expected Gremlin semantics
- [ ] Performance comparable to direct step construction

---

## 11. References

- [Gremlin Interface Design](../guilding-documents/gremlin.md)
- [GQL Subset Design](../guilding-documents/gql.md)
- [IR Query Plan Design](../guilding-documents/ir-query-plan.md)
- [TinkerPop Predicates](https://tinkerpop.apache.org/docs/current/reference/#a-note-on-predicates)
