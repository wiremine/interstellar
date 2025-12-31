# RustGremlin: Gremlin Interface Implementation

**Note**: This document describes a **Phase 2 feature**. The initial Phase 1 implementation focuses on the core Rust fluent API. The Gremlin interface enables external clients to submit queries using standard Gremlin bytecode or text format, which get compiled to the internal traversal engine. See the [Roadmap](./overview.md#5-roadmap) section in overview.md for the complete development timeline.

---

## 1. Overview

### 1.1 Purpose

The Gremlin interface provides external access to RustGremlin's traversal engine through:

1. **Bytecode Interface**: Accept TinkerPop-compatible Gremlin bytecode and execute it
2. **Text Parser**: Parse Gremlin query strings (e.g., `g.V().has('name','Alice').out('knows')`)
3. **Server Protocol**: Optional WebSocket server implementing the Gremlin Server protocol

This enables interoperability with existing Gremlin clients (Python, JavaScript, Java, etc.) while leveraging RustGremlin's high-performance traversal engine.

### 1.2 Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Gremlin Interface Layer                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐     │
│  │  Text Parser    │    │ Bytecode Decoder│    │ WebSocket Server│     │
│  │  (Gremlin DSL)  │    │ (GraphBinary)   │    │ (Optional)      │     │
│  └────────┬────────┘    └────────┬────────┘    └────────┬────────┘     │
│           │                      │                      │               │
│           └──────────────────────┼──────────────────────┘               │
│                                  ▼                                      │
│                    ┌─────────────────────────┐                          │
│                    │   Bytecode Interpreter  │                          │
│                    │   (Step Compilation)    │                          │
│                    └────────────┬────────────┘                          │
│                                 │                                       │
│                                 ▼                                       │
│                    ┌─────────────────────────┐                          │
│                    │   Internal Traversal    │                          │
│                    │   Traversal<S, E, T>    │                          │
│                    └────────────┬────────────┘                          │
│                                 │                                       │
│                                 ▼                                       │
│                    ┌─────────────────────────┐                          │
│                    │   Execution Engine      │                          │
│                    │   (Iterator Pipeline)   │                          │
│                    └────────────┬────────────┘                          │
│                                 │                                       │
│                                 ▼                                       │
│                    ┌─────────────────────────┐                          │
│                    │   Result Serializer     │                          │
│                    │   (GraphSON/GraphBinary)│                          │
│                    └─────────────────────────┘                          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.3 Design Principles

1. **Subset Approach**: Support the most commonly used 80% of Gremlin steps
2. **Zero-Copy Where Possible**: Minimize allocations during bytecode interpretation
3. **Type Safety**: Preserve Rust's type safety through careful step compilation
4. **Lazy Evaluation**: Maintain the pull-based iterator model from the internal API
5. **Compatibility**: Follow TinkerPop conventions for bytecode and serialization formats

---

## 2. Supported Gremlin Subset

### 2.1 Step Coverage Matrix

| Step Category | Step | Supported | Internal Mapping | Notes |
|--------------|------|-----------|------------------|-------|
| **Source** | `V()` | ✅ | `v()` | All vertices or by ID |
| | `E()` | ✅ | `e()` | All edges or by ID |
| | `addV()` | ✅ | `add_v()` | Requires mutation context |
| | `addE()` | ✅ | `add_e()` | Requires mutation context |
| | `inject()` | ✅ | `inject()` | |
| **Filter** | `has()` | ✅ | `has()`, `has_value()`, `has_where()` | Multiple overloads |
| | `hasLabel()` | ✅ | `has_label()` | |
| | `hasId()` | ✅ | `has_id()` | |
| | `hasNot()` | ✅ | `has().not()` | |
| | `filter()` | ✅ | `filter()` | Lambda or traversal |
| | `where()` | ✅ | `where_()` | |
| | `not()` | ✅ | `not()` | |
| | `and()` | ✅ | `and_()` | |
| | `or()` | ✅ | `or_()` | |
| | `is()` | ✅ | `filter()` with predicate | |
| | `dedup()` | ✅ | `dedup()` | |
| | `limit()` | ✅ | `limit()` | |
| | `skip()` | ✅ | `skip()` | |
| | `range()` | ✅ | `range()` | |
| | `coin()` | ✅ | `coin()` | |
| | `sample()` | ✅ | `sample()` | |
| | `simplePath()` | ✅ | `simple_path()` | |
| | `cyclicPath()` | ✅ | `cyclic_path()` | |
| **Map** | `out()` | ✅ | `out()`, `out_labels()` | |
| | `in()` | ✅ | `in_()`, `in_labels()` | |
| | `both()` | ✅ | `both()`, `both_labels()` | |
| | `outE()` | ✅ | `out_e()` | |
| | `inE()` | ✅ | `in_e()` | |
| | `bothE()` | ✅ | `both_e()` | |
| | `outV()` | ✅ | `out_v()` | |
| | `inV()` | ✅ | `in_v()` | |
| | `bothV()` | ✅ | `both_v()` | |
| | `otherV()` | ✅ | `other_v()` | |
| | `values()` | ✅ | `values()` | |
| | `properties()` | ✅ | `properties()` | |
| | `valueMap()` | ✅ | `value_map()` | |
| | `elementMap()` | ✅ | `element_map()` | |
| | `id()` | ✅ | `id()` | |
| | `label()` | ✅ | `label()` | |
| | `map()` | ✅ | `map()` | Traversal-based |
| | `flatMap()` | ✅ | `flat_map()` | |
| | `unfold()` | ✅ | `unfold()` | |
| | `fold()` | ✅ | `fold()` | |
| | `path()` | ✅ | `path()` | |
| | `select()` | ✅ | `select()` | |
| | `project()` | ✅ | Custom projection | |
| | `constant()` | ✅ | `constant()` | |
| | `math()` | ✅ | `math()` | |
| | `order()` | ✅ | `order()` | |
| | `count()` | ✅ | `count()` | |
| | `sum()` | ✅ | `sum()` | |
| | `mean()` | ✅ | `mean()` | |
| | `min()` | ✅ | `min()` | |
| | `max()` | ✅ | `max()` | |
| | `group()` | ✅ | `group()` | |
| | `groupCount()` | ✅ | `group_count()` | |
| **Branch** | `union()` | ✅ | `union()` | |
| | `coalesce()` | ✅ | `coalesce()` | |
| | `choose()` | ✅ | `choose()` | |
| | `optional()` | ✅ | `optional()` | |
| | `repeat()` | ✅ | `repeat()` | |
| | `times()` | ✅ | `.times()` | |
| | `until()` | ✅ | `.until()` | |
| | `emit()` | ✅ | `.emit()` | |
| | `local()` | ✅ | `local()` | |
| **Side Effect** | `as()` | ✅ | `as_()` | |
| | `store()` | ✅ | `store()` | |
| | `aggregate()` | ✅ | `aggregate()` | |
| | `sideEffect()` | ✅ | `side_effect()` | |
| | `property()` | ✅ | `property()` | Mutation |
| | `drop()` | ✅ | `drop()` | Mutation |
| **Terminal** | `toList()` | ✅ | `to_list()` | |
| | `toSet()` | ✅ | `to_set()` | |
| | `next()` | ✅ | `next()` | |
| | `hasNext()` | ✅ | `has_next()` | |
| | `iterate()` | ✅ | `iterate()` | |
| | `explain()` | ✅ | `explain()` | |
| | `profile()` | ✅ | `profile()` | |

### 2.2 Unsupported Steps

| Step | Reason | Workaround |
|------|--------|------------|
| `subgraph()` | Complex graph construction | Use multiple queries |
| `tree()` | Specialized data structure | Use `path()` + post-processing |
| `sack()` | Requires stateful traverser | Use `store()`/`aggregate()` |
| `withSack()` | Requires stateful traverser | Use side effects |
| `barrier()` | Explicit synchronization | Implicit in reduce steps |
| `cap()` | Side effect capture | Use explicit aggregation |
| `by()` modulators | Partially supported | Supported for common cases |
| Lambda steps | Security/portability | Use built-in predicates |
| `match()` | Complex pattern matching | Use explicit path patterns |
| `program()` | VertexProgram execution | Not planned |
| `io()` | Graph I/O | Use native import/export |
| `call()` | Procedure calls | Not planned |

### 2.3 Predicate Support

| Predicate | Supported | Example |
|-----------|-----------|---------|
| `eq(value)` | ✅ | `has('age', eq(30))` |
| `neq(value)` | ✅ | `has('status', neq('inactive'))` |
| `lt(value)` | ✅ | `has('age', lt(30))` |
| `lte(value)` | ✅ | `has('age', lte(30))` |
| `gt(value)` | ✅ | `has('age', gt(30))` |
| `gte(value)` | ✅ | `has('age', gte(30))` |
| `between(start, end)` | ✅ | `has('age', between(20, 40))` |
| `inside(start, end)` | ✅ | `has('age', inside(20, 40))` |
| `outside(start, end)` | ✅ | `has('age', outside(20, 40))` |
| `within(values...)` | ✅ | `has('status', within('active', 'pending'))` |
| `without(values...)` | ✅ | `has('status', without('deleted'))` |
| `containing(str)` | ✅ | `has('name', containing('bob'))` |
| `startingWith(str)` | ✅ | `has('name', startingWith('A'))` |
| `endingWith(str)` | ✅ | `has('name', endingWith('son'))` |
| `regex(pattern)` | ✅ | `has('email', regex('.*@acme.com'))` |
| `P.and(p1, p2)` | ✅ | `has('age', and(gt(20), lt(40)))` |
| `P.or(p1, p2)` | ✅ | `has('status', or(eq('a'), eq('b')))` |
| `P.not(p)` | ✅ | `has('age', not(eq(0)))` |

---

## 3. Bytecode Format

### 3.1 Bytecode Structure

Gremlin bytecode represents a traversal as a sequence of instructions. Each instruction contains:
- **Operator**: The step name (e.g., "V", "has", "out")
- **Arguments**: Step parameters (values, predicates, or nested bytecode)

```rust
/// Gremlin bytecode representation
#[derive(Debug, Clone)]
pub struct Bytecode {
    /// Source instructions (traversal source steps)
    pub source_instructions: Vec<Instruction>,
    /// Step instructions (traversal steps)
    pub step_instructions: Vec<Instruction>,
}

/// A single bytecode instruction
#[derive(Debug, Clone)]
pub struct Instruction {
    /// Operator name (e.g., "V", "has", "out")
    pub operator: String,
    /// Arguments to the operator
    pub arguments: Vec<Argument>,
}

/// Argument types in bytecode
#[derive(Debug, Clone)]
pub enum Argument {
    /// Null value
    Null,
    /// Boolean value
    Bool(bool),
    /// Integer value (i64)
    Int(i64),
    /// Float value (f64)
    Float(f64),
    /// String value
    String(String),
    /// List of arguments
    List(Vec<Argument>),
    /// Map of string keys to arguments
    Map(Vec<(String, Argument)>),
    /// Vertex ID reference
    VertexId(u64),
    /// Edge ID reference
    EdgeId(u64),
    /// Predicate (P.eq, P.gt, etc.)
    Predicate(Predicate),
    /// Nested bytecode (for anonymous traversals)
    Bytecode(Box<Bytecode>),
    /// Direction enum (OUT, IN, BOTH)
    Direction(Direction),
    /// Order enum (ASC, DESC)
    Order(SortOrder),
    /// Scope enum (local, global)
    Scope(Scope),
    /// Column enum (keys, values)
    Column(Column),
    /// T enum (id, label, key, value)
    T(T),
    /// Cardinality enum (single, list, set)
    Cardinality(Cardinality),
    /// Lambda placeholder (limited support)
    Lambda(String),
}

/// Predicate representation
#[derive(Debug, Clone)]
pub struct Predicate {
    pub operator: PredicateOperator,
    pub value: Box<Argument>,
    pub other: Option<Box<Argument>>,  // For between, inside, etc.
}

#[derive(Debug, Clone, Copy)]
pub enum PredicateOperator {
    Eq, Neq, Lt, Lte, Gt, Gte,
    Between, Inside, Outside,
    Within, Without,
    Containing, StartingWith, EndingWith, Regex,
    And, Or, Not,
}

#[derive(Debug, Clone, Copy)]
pub enum Direction { Out, In, Both }

#[derive(Debug, Clone, Copy)]
pub enum SortOrder { Asc, Desc, Shuffle }

#[derive(Debug, Clone, Copy)]
pub enum Scope { Local, Global }

#[derive(Debug, Clone, Copy)]
pub enum Column { Keys, Values }

#[derive(Debug, Clone, Copy)]
pub enum T { Id, Label, Key, Value }

#[derive(Debug, Clone, Copy)]
pub enum Cardinality { Single, List, Set }
```

### 3.2 Example Bytecode

For the query `g.V().has('name', 'Alice').out('knows').values('name')`:

```rust
Bytecode {
    source_instructions: vec![],
    step_instructions: vec![
        Instruction {
            operator: "V".to_string(),
            arguments: vec![],
        },
        Instruction {
            operator: "has".to_string(),
            arguments: vec![
                Argument::String("name".to_string()),
                Argument::String("Alice".to_string()),
            ],
        },
        Instruction {
            operator: "out".to_string(),
            arguments: vec![
                Argument::String("knows".to_string()),
            ],
        },
        Instruction {
            operator: "values".to_string(),
            arguments: vec![
                Argument::String("name".to_string()),
            ],
        },
    ],
}
```

### 3.3 GraphBinary Serialization

GraphBinary is TinkerPop's efficient binary serialization format. We implement a subset for bytecode exchange.

```rust
/// GraphBinary type codes
pub mod type_codes {
    pub const INT: u8 = 0x01;
    pub const LONG: u8 = 0x02;
    pub const STRING: u8 = 0x03;
    pub const LIST: u8 = 0x09;
    pub const MAP: u8 = 0x0A;
    pub const UUID: u8 = 0x0C;
    pub const BYTECODE: u8 = 0x15;
    pub const P: u8 = 0x1E;
    pub const LAMBDA: u8 = 0x1F;
    pub const TRAVERSER: u8 = 0x21;
    pub const VERTEX: u8 = 0x11;
    pub const EDGE: u8 = 0x12;
    pub const PROPERTY: u8 = 0x13;
    pub const VERTEX_PROPERTY: u8 = 0x14;
    pub const PATH: u8 = 0x16;
    // ... more type codes
}

/// GraphBinary deserializer
pub struct GraphBinaryReader<'a> {
    buffer: &'a [u8],
    position: usize,
}

impl<'a> GraphBinaryReader<'a> {
    pub fn new(buffer: &'a [u8]) -> Self {
        Self { buffer, position: 0 }
    }
    
    pub fn read_bytecode(&mut self) -> Result<Bytecode, DecodeError> {
        let type_code = self.read_byte()?;
        if type_code != type_codes::BYTECODE {
            return Err(DecodeError::UnexpectedType(type_code));
        }
        
        // Read nullable flag
        let nullable = self.read_byte()?;
        if nullable == 0x01 {
            return Err(DecodeError::NullBytecode);
        }
        
        // Read source instructions
        let source_count = self.read_int()? as usize;
        let mut source_instructions = Vec::with_capacity(source_count);
        for _ in 0..source_count {
            source_instructions.push(self.read_instruction()?);
        }
        
        // Read step instructions
        let step_count = self.read_int()? as usize;
        let mut step_instructions = Vec::with_capacity(step_count);
        for _ in 0..step_count {
            step_instructions.push(self.read_instruction()?);
        }
        
        Ok(Bytecode {
            source_instructions,
            step_instructions,
        })
    }
    
    fn read_instruction(&mut self) -> Result<Instruction, DecodeError> {
        let operator = self.read_string()?;
        let arg_count = self.read_int()? as usize;
        
        let mut arguments = Vec::with_capacity(arg_count);
        for _ in 0..arg_count {
            arguments.push(self.read_argument()?);
        }
        
        Ok(Instruction { operator, arguments })
    }
    
    fn read_argument(&mut self) -> Result<Argument, DecodeError> {
        let type_code = self.read_byte()?;
        
        match type_code {
            0xFE => Ok(Argument::Null),  // Null marker
            type_codes::INT => Ok(Argument::Int(self.read_int()? as i64)),
            type_codes::LONG => Ok(Argument::Int(self.read_long()?)),
            type_codes::STRING => Ok(Argument::String(self.read_string()?)),
            type_codes::LIST => self.read_list_argument(),
            type_codes::MAP => self.read_map_argument(),
            type_codes::BYTECODE => {
                // Rewind and read full bytecode
                self.position -= 1;
                Ok(Argument::Bytecode(Box::new(self.read_bytecode()?)))
            }
            type_codes::P => self.read_predicate(),
            _ => Err(DecodeError::UnsupportedType(type_code)),
        }
    }
    
    fn read_predicate(&mut self) -> Result<Argument, DecodeError> {
        let _nullable = self.read_byte()?;
        let operator_name = self.read_string()?;
        let value_count = self.read_int()? as usize;
        
        let operator = match operator_name.as_str() {
            "eq" => PredicateOperator::Eq,
            "neq" => PredicateOperator::Neq,
            "lt" => PredicateOperator::Lt,
            "lte" => PredicateOperator::Lte,
            "gt" => PredicateOperator::Gt,
            "gte" => PredicateOperator::Gte,
            "between" => PredicateOperator::Between,
            "inside" => PredicateOperator::Inside,
            "outside" => PredicateOperator::Outside,
            "within" => PredicateOperator::Within,
            "without" => PredicateOperator::Without,
            "containing" => PredicateOperator::Containing,
            "startingWith" => PredicateOperator::StartingWith,
            "endingWith" => PredicateOperator::EndingWith,
            "regex" => PredicateOperator::Regex,
            "and" => PredicateOperator::And,
            "or" => PredicateOperator::Or,
            "not" => PredicateOperator::Not,
            _ => return Err(DecodeError::UnknownPredicate(operator_name)),
        };
        
        let value = Box::new(self.read_argument()?);
        let other = if value_count > 1 {
            Some(Box::new(self.read_argument()?))
        } else {
            None
        };
        
        Ok(Argument::Predicate(Predicate { operator, value, other }))
    }
    
    // Helper methods
    fn read_byte(&mut self) -> Result<u8, DecodeError> {
        if self.position >= self.buffer.len() {
            return Err(DecodeError::UnexpectedEof);
        }
        let b = self.buffer[self.position];
        self.position += 1;
        Ok(b)
    }
    
    fn read_int(&mut self) -> Result<i32, DecodeError> {
        if self.position + 4 > self.buffer.len() {
            return Err(DecodeError::UnexpectedEof);
        }
        let bytes: [u8; 4] = self.buffer[self.position..self.position + 4]
            .try_into()
            .unwrap();
        self.position += 4;
        Ok(i32::from_be_bytes(bytes))
    }
    
    fn read_long(&mut self) -> Result<i64, DecodeError> {
        if self.position + 8 > self.buffer.len() {
            return Err(DecodeError::UnexpectedEof);
        }
        let bytes: [u8; 8] = self.buffer[self.position..self.position + 8]
            .try_into()
            .unwrap();
        self.position += 8;
        Ok(i64::from_be_bytes(bytes))
    }
    
    fn read_string(&mut self) -> Result<String, DecodeError> {
        let _nullable = self.read_byte()?;
        let len = self.read_int()? as usize;
        if self.position + len > self.buffer.len() {
            return Err(DecodeError::UnexpectedEof);
        }
        let s = String::from_utf8(self.buffer[self.position..self.position + len].to_vec())
            .map_err(|_| DecodeError::InvalidUtf8)?;
        self.position += len;
        Ok(s)
    }
    
    fn read_list_argument(&mut self) -> Result<Argument, DecodeError> {
        let _nullable = self.read_byte()?;
        let len = self.read_int()? as usize;
        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            items.push(self.read_argument()?);
        }
        Ok(Argument::List(items))
    }
    
    fn read_map_argument(&mut self) -> Result<Argument, DecodeError> {
        let _nullable = self.read_byte()?;
        let len = self.read_int()? as usize;
        let mut entries = Vec::with_capacity(len);
        for _ in 0..len {
            let key = self.read_string()?;
            let value = self.read_argument()?;
            entries.push((key, value));
        }
        Ok(Argument::Map(entries))
    }
}

#[derive(Debug)]
pub enum DecodeError {
    UnexpectedEof,
    UnexpectedType(u8),
    UnsupportedType(u8),
    NullBytecode,
    UnknownPredicate(String),
    InvalidUtf8,
}
```

---

## 4. Text Parser

### 4.1 Grammar Definition

The Gremlin text parser accepts standard Gremlin-Groovy syntax. We use a PEG (Parsing Expression Grammar) approach for clarity and maintainability.

```pest
// gremlin.pest - Gremlin text grammar

WHITESPACE = _{ " " | "\t" | "\r" | "\n" }
COMMENT = _{ "//" ~ (!"\n" ~ ANY)* | "/*" ~ (!"*/" ~ ANY)* ~ "*/" }

// Entry point
query = { SOI ~ traversal_source ~ step* ~ terminal_step? ~ EOI }

// Traversal source: g.V(), g.E(), g.addV(), etc.
traversal_source = { "g" ~ "." ~ source_step }

source_step = {
    v_step
    | e_step
    | add_v_step
    | add_e_step
    | inject_step
}

v_step = { "V" ~ "(" ~ id_list? ~ ")" }
e_step = { "E" ~ "(" ~ id_list? ~ ")" }
add_v_step = { "addV" ~ "(" ~ string_literal? ~ ")" }
add_e_step = { "addE" ~ "(" ~ string_literal ~ ")" }
inject_step = { "inject" ~ "(" ~ arg_list ~ ")" }

id_list = { (integer | string_literal) ~ ("," ~ (integer | string_literal))* }

// Steps
step = { "." ~ step_name ~ "(" ~ arg_list? ~ ")" ~ modulator* }

step_name = {
    // Filter steps
    "has" | "hasLabel" | "hasId" | "hasNot" | "hasKey" | "hasValue"
    | "filter" | "where" | "not" | "and" | "or" | "is"
    | "dedup" | "limit" | "skip" | "range" | "tail"
    | "coin" | "sample" | "simplePath" | "cyclicPath"
    // Map steps
    | "out" | "in" | "both" | "outE" | "inE" | "bothE"
    | "outV" | "inV" | "bothV" | "otherV"
    | "values" | "properties" | "valueMap" | "elementMap" | "propertyMap"
    | "id" | "label" | "key" | "value"
    | "map" | "flatMap" | "unfold" | "fold"
    | "path" | "select" | "project" | "constant" | "math"
    | "order" | "count" | "sum" | "mean" | "min" | "max"
    | "group" | "groupCount"
    // Branch steps
    | "union" | "coalesce" | "choose" | "optional"
    | "repeat" | "times" | "until" | "emit" | "loops"
    | "local" | "identity"
    // Side effect steps
    | "as" | "store" | "aggregate" | "sideEffect"
    | "property" | "drop"
}

// Modulators (by, from, to, etc.)
modulator = { "." ~ modulator_name ~ "(" ~ arg_list? ~ ")" }

modulator_name = { "by" | "from" | "to" | "with" | "option" }

// Terminal steps
terminal_step = { "." ~ terminal_name ~ "(" ~ ")" }

terminal_name = {
    "toList" | "toSet" | "toBulkSet"
    | "next" | "hasNext" | "tryNext"
    | "iterate" | "explain" | "profile"
}

// Arguments
arg_list = { argument ~ ("," ~ argument)* }

argument = {
    anonymous_traversal
    | predicate
    | direction
    | order_enum
    | scope_enum
    | t_enum
    | cardinality
    | column_enum
    | pop_enum
    | list_literal
    | map_literal
    | string_literal
    | number
    | boolean
    | null
}

// Anonymous traversal: __. or just chained steps starting with __
anonymous_traversal = { "__" ~ step* }

// Predicates: P.eq(x), P.gt(x), etc.
predicate = { predicate_p | text_predicate }

predicate_p = { "P" ~ "." ~ predicate_name ~ "(" ~ predicate_args ~ ")" }
text_predicate = { "TextP" ~ "." ~ text_predicate_name ~ "(" ~ string_literal ~ ")" }

predicate_name = {
    "eq" | "neq" | "lt" | "lte" | "gt" | "gte"
    | "between" | "inside" | "outside"
    | "within" | "without"
    | "and" | "or" | "not"
}

text_predicate_name = {
    "containing" | "startingWith" | "endingWith"
    | "notContaining" | "notStartingWith" | "notEndingWith"
    | "regex" | "notRegex"
}

predicate_args = { argument ~ ("," ~ argument)* }

// Enums
direction = { "Direction" ~ "." ~ ("OUT" | "IN" | "BOTH") }
order_enum = { "Order" ~ "." ~ ("asc" | "desc" | "shuffle") }
scope_enum = { "Scope" ~ "." ~ ("local" | "global") }
t_enum = { "T" ~ "." ~ ("id" | "label" | "key" | "value") }
cardinality = { "Cardinality" ~ "." ~ ("single" | "list" | "set") }
column_enum = { "Column" ~ "." ~ ("keys" | "values") }
pop_enum = { "Pop" ~ "." ~ ("first" | "last" | "all" | "mixed") }

// Literals
list_literal = { "[" ~ (argument ~ ("," ~ argument)*)? ~ "]" }
map_literal = { "[" ~ (map_entry ~ ("," ~ map_entry)*)? ~ "]" }
map_entry = { argument ~ ":" ~ argument }

string_literal = @{ "'" ~ (!"'" ~ ANY | "''")* ~ "'" | "\"" ~ (!"\"" ~ ANY | "\\\"")* ~ "\"" }
number = @{ "-"? ~ ASCII_DIGIT+ ~ ("." ~ ASCII_DIGIT+)? ~ (("e" | "E") ~ ("-" | "+")? ~ ASCII_DIGIT+)? }
integer = @{ "-"? ~ ASCII_DIGIT+ }
boolean = { "true" | "false" }
null = { "null" }
```

### 4.2 Parser Implementation

```rust
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "gremlin.pest"]
pub struct GremlinParser;

/// Parse a Gremlin query string into bytecode
pub fn parse_gremlin(input: &str) -> Result<Bytecode, ParseError> {
    let pairs = GremlinParser::parse(Rule::query, input)
        .map_err(|e| ParseError::Syntax(e.to_string()))?;
    
    let query_pair = pairs.into_iter().next().unwrap();
    build_bytecode(query_pair)
}

fn build_bytecode(pair: pest::iterators::Pair<Rule>) -> Result<Bytecode, ParseError> {
    let mut source_instructions = Vec::new();
    let mut step_instructions = Vec::new();
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::traversal_source => {
                // Parse source step (V, E, addV, etc.)
                let source = parse_traversal_source(inner)?;
                step_instructions.push(source);
            }
            Rule::step => {
                step_instructions.push(parse_step(inner)?);
            }
            Rule::terminal_step => {
                step_instructions.push(parse_terminal_step(inner)?);
            }
            Rule::EOI => {}
            _ => {}
        }
    }
    
    Ok(Bytecode {
        source_instructions,
        step_instructions,
    })
}

fn parse_traversal_source(pair: pest::iterators::Pair<Rule>) -> Result<Instruction, ParseError> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::source_step {
            return parse_source_step(inner);
        }
    }
    Err(ParseError::MissingSourceStep)
}

fn parse_source_step(pair: pest::iterators::Pair<Rule>) -> Result<Instruction, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    
    match inner.as_rule() {
        Rule::v_step => {
            let mut args = Vec::new();
            for id_pair in inner.into_inner() {
                if id_pair.as_rule() == Rule::id_list {
                    for id in id_pair.into_inner() {
                        args.push(parse_argument_literal(id)?);
                    }
                }
            }
            Ok(Instruction {
                operator: "V".to_string(),
                arguments: args,
            })
        }
        Rule::e_step => {
            let mut args = Vec::new();
            for id_pair in inner.into_inner() {
                if id_pair.as_rule() == Rule::id_list {
                    for id in id_pair.into_inner() {
                        args.push(parse_argument_literal(id)?);
                    }
                }
            }
            Ok(Instruction {
                operator: "E".to_string(),
                arguments: args,
            })
        }
        Rule::add_v_step => {
            let mut args = Vec::new();
            for label_pair in inner.into_inner() {
                if label_pair.as_rule() == Rule::string_literal {
                    args.push(parse_string_literal(label_pair));
                }
            }
            Ok(Instruction {
                operator: "addV".to_string(),
                arguments: args,
            })
        }
        Rule::add_e_step => {
            let label = inner.into_inner()
                .find(|p| p.as_rule() == Rule::string_literal)
                .map(parse_string_literal)
                .ok_or(ParseError::MissingLabel)?;
            Ok(Instruction {
                operator: "addE".to_string(),
                arguments: vec![label],
            })
        }
        Rule::inject_step => {
            let args = parse_arg_list(inner)?;
            Ok(Instruction {
                operator: "inject".to_string(),
                arguments: args,
            })
        }
        _ => Err(ParseError::UnknownSourceStep),
    }
}

fn parse_step(pair: pest::iterators::Pair<Rule>) -> Result<Instruction, ParseError> {
    let mut operator = String::new();
    let mut arguments = Vec::new();
    let mut modulators = Vec::new();
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::step_name => {
                operator = inner.as_str().to_string();
            }
            Rule::arg_list => {
                arguments = parse_arg_list_pair(inner)?;
            }
            Rule::modulator => {
                modulators.push(parse_modulator(inner)?);
            }
            _ => {}
        }
    }
    
    // Append modulators as nested instructions (simplified)
    // In full implementation, modulators modify the step behavior
    for modulator in modulators {
        arguments.push(Argument::Map(vec![
            ("modulator".to_string(), Argument::String(modulator.operator)),
            ("args".to_string(), Argument::List(modulator.arguments)),
        ]));
    }
    
    Ok(Instruction { operator, arguments })
}

fn parse_terminal_step(pair: pest::iterators::Pair<Rule>) -> Result<Instruction, ParseError> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::terminal_name {
            return Ok(Instruction {
                operator: inner.as_str().to_string(),
                arguments: vec![],
            });
        }
    }
    Err(ParseError::MissingTerminalStep)
}

fn parse_arg_list(pair: pest::iterators::Pair<Rule>) -> Result<Vec<Argument>, ParseError> {
    let mut args = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::arg_list {
            return parse_arg_list_pair(inner);
        }
    }
    Ok(args)
}

fn parse_arg_list_pair(pair: pest::iterators::Pair<Rule>) -> Result<Vec<Argument>, ParseError> {
    let mut args = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::argument {
            args.push(parse_argument(inner)?);
        }
    }
    Ok(args)
}

fn parse_argument(pair: pest::iterators::Pair<Rule>) -> Result<Argument, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    
    match inner.as_rule() {
        Rule::anonymous_traversal => {
            let bytecode = parse_anonymous_traversal(inner)?;
            Ok(Argument::Bytecode(Box::new(bytecode)))
        }
        Rule::predicate => parse_predicate(inner),
        Rule::string_literal => Ok(parse_string_literal(inner)),
        Rule::number => parse_number(inner),
        Rule::integer => {
            let s = inner.as_str();
            let n: i64 = s.parse().map_err(|_| ParseError::InvalidNumber)?;
            Ok(Argument::Int(n))
        }
        Rule::boolean => {
            let b = inner.as_str() == "true";
            Ok(Argument::Bool(b))
        }
        Rule::null => Ok(Argument::Null),
        Rule::list_literal => parse_list_literal(inner),
        Rule::map_literal => parse_map_literal(inner),
        Rule::direction => parse_direction(inner),
        Rule::order_enum => parse_order_enum(inner),
        Rule::scope_enum => parse_scope_enum(inner),
        Rule::t_enum => parse_t_enum(inner),
        _ => Err(ParseError::UnknownArgument),
    }
}

fn parse_anonymous_traversal(pair: pest::iterators::Pair<Rule>) -> Result<Bytecode, ParseError> {
    let mut step_instructions = Vec::new();
    
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::step {
            step_instructions.push(parse_step(inner)?);
        }
    }
    
    Ok(Bytecode {
        source_instructions: vec![],
        step_instructions,
    })
}

fn parse_predicate(pair: pest::iterators::Pair<Rule>) -> Result<Argument, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    
    match inner.as_rule() {
        Rule::predicate_p => {
            let mut operator = PredicateOperator::Eq;
            let mut args = Vec::new();
            
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::predicate_name => {
                        operator = match p.as_str() {
                            "eq" => PredicateOperator::Eq,
                            "neq" => PredicateOperator::Neq,
                            "lt" => PredicateOperator::Lt,
                            "lte" => PredicateOperator::Lte,
                            "gt" => PredicateOperator::Gt,
                            "gte" => PredicateOperator::Gte,
                            "between" => PredicateOperator::Between,
                            "inside" => PredicateOperator::Inside,
                            "outside" => PredicateOperator::Outside,
                            "within" => PredicateOperator::Within,
                            "without" => PredicateOperator::Without,
                            "and" => PredicateOperator::And,
                            "or" => PredicateOperator::Or,
                            "not" => PredicateOperator::Not,
                            _ => return Err(ParseError::UnknownPredicate),
                        };
                    }
                    Rule::predicate_args => {
                        for arg in p.into_inner() {
                            if arg.as_rule() == Rule::argument {
                                args.push(parse_argument(arg)?);
                            }
                        }
                    }
                    _ => {}
                }
            }
            
            let value = args.get(0).cloned().unwrap_or(Argument::Null);
            let other = args.get(1).cloned();
            
            Ok(Argument::Predicate(Predicate {
                operator,
                value: Box::new(value),
                other: other.map(Box::new),
            }))
        }
        Rule::text_predicate => {
            let mut operator = PredicateOperator::Containing;
            let mut value = Argument::Null;
            
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::text_predicate_name => {
                        operator = match p.as_str() {
                            "containing" | "notContaining" => PredicateOperator::Containing,
                            "startingWith" | "notStartingWith" => PredicateOperator::StartingWith,
                            "endingWith" | "notEndingWith" => PredicateOperator::EndingWith,
                            "regex" | "notRegex" => PredicateOperator::Regex,
                            _ => return Err(ParseError::UnknownPredicate),
                        };
                    }
                    Rule::string_literal => {
                        value = parse_string_literal(p);
                    }
                    _ => {}
                }
            }
            
            Ok(Argument::Predicate(Predicate {
                operator,
                value: Box::new(value),
                other: None,
            }))
        }
        _ => Err(ParseError::UnknownPredicate),
    }
}

fn parse_string_literal(pair: pest::iterators::Pair<Rule>) -> Argument {
    let s = pair.as_str();
    // Remove quotes and unescape
    let inner = if s.starts_with('\'') {
        s[1..s.len()-1].replace("''", "'")
    } else {
        s[1..s.len()-1].replace("\\\"", "\"")
    };
    Argument::String(inner)
}

fn parse_number(pair: pest::iterators::Pair<Rule>) -> Result<Argument, ParseError> {
    let s = pair.as_str();
    if s.contains('.') || s.contains('e') || s.contains('E') {
        let f: f64 = s.parse().map_err(|_| ParseError::InvalidNumber)?;
        Ok(Argument::Float(f))
    } else {
        let n: i64 = s.parse().map_err(|_| ParseError::InvalidNumber)?;
        Ok(Argument::Int(n))
    }
}

fn parse_argument_literal(pair: pest::iterators::Pair<Rule>) -> Result<Argument, ParseError> {
    match pair.as_rule() {
        Rule::string_literal => Ok(parse_string_literal(pair)),
        Rule::integer => {
            let n: i64 = pair.as_str().parse().map_err(|_| ParseError::InvalidNumber)?;
            Ok(Argument::Int(n))
        }
        _ => Err(ParseError::UnknownArgument),
    }
}

fn parse_list_literal(pair: pest::iterators::Pair<Rule>) -> Result<Argument, ParseError> {
    let mut items = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::argument {
            items.push(parse_argument(inner)?);
        }
    }
    Ok(Argument::List(items))
}

fn parse_map_literal(pair: pest::iterators::Pair<Rule>) -> Result<Argument, ParseError> {
    let mut entries = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::map_entry {
            let mut key = String::new();
            let mut value = Argument::Null;
            for entry_part in inner.into_inner() {
                if entry_part.as_rule() == Rule::argument {
                    if key.is_empty() {
                        if let Argument::String(s) = parse_argument(entry_part.clone())? {
                            key = s;
                        }
                    } else {
                        value = parse_argument(entry_part)?;
                    }
                }
            }
            entries.push((key, value));
        }
    }
    Ok(Argument::Map(entries))
}

fn parse_direction(pair: pest::iterators::Pair<Rule>) -> Result<Argument, ParseError> {
    let s = pair.as_str();
    let dir = if s.contains("OUT") {
        Direction::Out
    } else if s.contains("IN") {
        Direction::In
    } else {
        Direction::Both
    };
    Ok(Argument::Direction(dir))
}

fn parse_order_enum(pair: pest::iterators::Pair<Rule>) -> Result<Argument, ParseError> {
    let s = pair.as_str();
    let order = if s.contains("asc") {
        SortOrder::Asc
    } else if s.contains("desc") {
        SortOrder::Desc
    } else {
        SortOrder::Shuffle
    };
    Ok(Argument::Order(order))
}

fn parse_scope_enum(pair: pest::iterators::Pair<Rule>) -> Result<Argument, ParseError> {
    let s = pair.as_str();
    let scope = if s.contains("local") {
        Scope::Local
    } else {
        Scope::Global
    };
    Ok(Argument::Scope(scope))
}

fn parse_t_enum(pair: pest::iterators::Pair<Rule>) -> Result<Argument, ParseError> {
    let s = pair.as_str();
    let t = if s.contains("id") {
        T::Id
    } else if s.contains("label") {
        T::Label
    } else if s.contains("key") {
        T::Key
    } else {
        T::Value
    };
    Ok(Argument::T(t))
}

fn parse_modulator(pair: pest::iterators::Pair<Rule>) -> Result<Instruction, ParseError> {
    let mut operator = String::new();
    let mut arguments = Vec::new();
    
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::modulator_name => {
                operator = inner.as_str().to_string();
            }
            Rule::arg_list => {
                arguments = parse_arg_list_pair(inner)?;
            }
            _ => {}
        }
    }
    
    Ok(Instruction { operator, arguments })
}

#[derive(Debug)]
pub enum ParseError {
    Syntax(String),
    MissingSourceStep,
    MissingLabel,
    MissingTerminalStep,
    UnknownSourceStep,
    UnknownArgument,
    UnknownPredicate,
    InvalidNumber,
}
```

### 4.3 Usage Example

```rust
// Parse a Gremlin query string
let query = "g.V().has('name', 'Alice').out('knows').values('name')";
let bytecode = parse_gremlin(query)?;

// The bytecode can now be passed to the interpreter
let results = interpreter.execute(bytecode)?;
```

---

## 5. Bytecode Interpreter

The bytecode interpreter converts Gremlin bytecode into RustGremlin's internal traversal representation and executes it.

### 5.1 Interpreter Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     Bytecode Interpreter Flow                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Bytecode                                                               │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │ [V(), has('name','Alice'), out('knows'), values('name')]       │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                          │                                              │
│                          ▼                                              │
│  Step Compilation                                                       │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │ For each instruction:                                          │    │
│  │   1. Look up step compiler by operator name                    │    │
│  │   2. Validate argument types and count                         │    │
│  │   3. Convert arguments to internal Value types                 │    │
│  │   4. Create internal Step instance                             │    │
│  │   5. Chain to traversal pipeline                               │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                          │                                              │
│                          ▼                                              │
│  Internal Traversal                                                     │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │ Traversal<GraphTraversalSource, Value, DynamicTraverser>       │    │
│  │   steps: [VStep, HasStep, OutStep, ValuesStep]                 │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                          │                                              │
│                          ▼                                              │
│  Execution (Pull-Based Iterator)                                        │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │ while let Some(traverser) = traversal.next() {                 │    │
│  │     results.push(traverser.get());                             │    │
│  │ }                                                              │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Core Interpreter Implementation

```rust
use std::collections::HashMap;
use std::sync::Arc;

/// The main bytecode interpreter
pub struct GremlinInterpreter<'g> {
    graph: &'g Graph,
    step_compilers: HashMap<&'static str, Box<dyn StepCompiler>>,
}

/// Trait for compiling a single step
pub trait StepCompiler: Send + Sync {
    fn compile(
        &self,
        args: &[Argument],
        context: &mut CompilationContext,
    ) -> Result<Box<dyn DynamicStep>, CompileError>;
}

/// Context maintained during compilation
pub struct CompilationContext<'g> {
    graph: &'g Graph,
    /// Current element type in the traversal (Vertex, Edge, Value, etc.)
    current_type: ElementType,
    /// Named step bindings (from as() steps)
    bindings: HashMap<String, ElementType>,
    /// Side effect keys
    side_effects: HashMap<String, SideEffectType>,
}

#[derive(Clone, Copy, Debug)]
pub enum ElementType {
    Vertex,
    Edge,
    VertexProperty,
    Property,
    Value,
    Path,
    Map,
    List,
    Unknown,
}

/// Dynamic step that works with runtime-typed values
pub trait DynamicStep: Send + Sync {
    fn process(&mut self, traverser: DynamicTraverser) -> DynamicStepResult;
    fn reset(&mut self);
    fn clone_box(&self) -> Box<dyn DynamicStep>;
}

pub enum DynamicStepResult {
    /// Emit a single traverser
    Emit(DynamicTraverser),
    /// Emit multiple traversers
    EmitMany(Vec<DynamicTraverser>),
    /// Filter out (no emission)
    Filter,
    /// Pull from upstream (barrier step needs more input)
    Pull,
    /// Done processing
    Done,
}

/// Runtime-typed traverser
#[derive(Clone)]
pub struct DynamicTraverser {
    pub value: DynamicValue,
    pub path: Path,
    pub loops: usize,
    pub bulk: u64,
    pub sack: Option<DynamicValue>,
}

/// Runtime-typed value
#[derive(Clone, Debug)]
pub enum DynamicValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Vertex(VertexId),
    Edge(EdgeId),
    Property { key: String, value: Box<DynamicValue> },
    List(Vec<DynamicValue>),
    Map(Vec<(String, DynamicValue)>),
    Path(Path),
}

impl<'g> GremlinInterpreter<'g> {
    pub fn new(graph: &'g Graph) -> Self {
        let mut interpreter = Self {
            graph,
            step_compilers: HashMap::new(),
        };
        interpreter.register_default_steps();
        interpreter
    }
    
    fn register_default_steps(&mut self) {
        // Source steps
        self.register("V", Box::new(VStepCompiler));
        self.register("E", Box::new(EStepCompiler));
        self.register("addV", Box::new(AddVStepCompiler));
        self.register("addE", Box::new(AddEStepCompiler));
        self.register("inject", Box::new(InjectStepCompiler));
        
        // Filter steps
        self.register("has", Box::new(HasStepCompiler));
        self.register("hasLabel", Box::new(HasLabelStepCompiler));
        self.register("hasId", Box::new(HasIdStepCompiler));
        self.register("hasNot", Box::new(HasNotStepCompiler));
        self.register("filter", Box::new(FilterStepCompiler));
        self.register("where", Box::new(WhereStepCompiler));
        self.register("not", Box::new(NotStepCompiler));
        self.register("and", Box::new(AndStepCompiler));
        self.register("or", Box::new(OrStepCompiler));
        self.register("is", Box::new(IsStepCompiler));
        self.register("dedup", Box::new(DedupStepCompiler));
        self.register("limit", Box::new(LimitStepCompiler));
        self.register("skip", Box::new(SkipStepCompiler));
        self.register("range", Box::new(RangeStepCompiler));
        self.register("coin", Box::new(CoinStepCompiler));
        self.register("sample", Box::new(SampleStepCompiler));
        self.register("simplePath", Box::new(SimplePathStepCompiler));
        self.register("cyclicPath", Box::new(CyclicPathStepCompiler));
        
        // Map steps
        self.register("out", Box::new(OutStepCompiler));
        self.register("in", Box::new(InStepCompiler));
        self.register("both", Box::new(BothStepCompiler));
        self.register("outE", Box::new(OutEStepCompiler));
        self.register("inE", Box::new(InEStepCompiler));
        self.register("bothE", Box::new(BothEStepCompiler));
        self.register("outV", Box::new(OutVStepCompiler));
        self.register("inV", Box::new(InVStepCompiler));
        self.register("bothV", Box::new(BothVStepCompiler));
        self.register("otherV", Box::new(OtherVStepCompiler));
        self.register("values", Box::new(ValuesStepCompiler));
        self.register("properties", Box::new(PropertiesStepCompiler));
        self.register("valueMap", Box::new(ValueMapStepCompiler));
        self.register("elementMap", Box::new(ElementMapStepCompiler));
        self.register("id", Box::new(IdStepCompiler));
        self.register("label", Box::new(LabelStepCompiler));
        self.register("map", Box::new(MapStepCompiler));
        self.register("flatMap", Box::new(FlatMapStepCompiler));
        self.register("unfold", Box::new(UnfoldStepCompiler));
        self.register("fold", Box::new(FoldStepCompiler));
        self.register("path", Box::new(PathStepCompiler));
        self.register("select", Box::new(SelectStepCompiler));
        self.register("project", Box::new(ProjectStepCompiler));
        self.register("constant", Box::new(ConstantStepCompiler));
        self.register("math", Box::new(MathStepCompiler));
        self.register("order", Box::new(OrderStepCompiler));
        self.register("count", Box::new(CountStepCompiler));
        self.register("sum", Box::new(SumStepCompiler));
        self.register("mean", Box::new(MeanStepCompiler));
        self.register("min", Box::new(MinStepCompiler));
        self.register("max", Box::new(MaxStepCompiler));
        self.register("group", Box::new(GroupStepCompiler));
        self.register("groupCount", Box::new(GroupCountStepCompiler));
        
        // Branch steps
        self.register("union", Box::new(UnionStepCompiler));
        self.register("coalesce", Box::new(CoalesceStepCompiler));
        self.register("choose", Box::new(ChooseStepCompiler));
        self.register("optional", Box::new(OptionalStepCompiler));
        self.register("repeat", Box::new(RepeatStepCompiler));
        self.register("local", Box::new(LocalStepCompiler));
        self.register("identity", Box::new(IdentityStepCompiler));
        
        // Side effect steps
        self.register("as", Box::new(AsStepCompiler));
        self.register("store", Box::new(StoreStepCompiler));
        self.register("aggregate", Box::new(AggregateStepCompiler));
        self.register("sideEffect", Box::new(SideEffectStepCompiler));
        self.register("property", Box::new(PropertyStepCompiler));
        self.register("drop", Box::new(DropStepCompiler));
    }
    
    fn register(&mut self, name: &'static str, compiler: Box<dyn StepCompiler>) {
        self.step_compilers.insert(name, compiler);
    }
    
    /// Execute bytecode and return results
    pub fn execute(&self, bytecode: Bytecode) -> Result<Vec<DynamicValue>, ExecutionError> {
        let traversal = self.compile(bytecode)?;
        self.run(traversal)
    }
    
    /// Compile bytecode into a traversal pipeline
    pub fn compile(&self, bytecode: Bytecode) -> Result<DynamicTraversal, CompileError> {
        let mut context = CompilationContext {
            graph: self.graph,
            current_type: ElementType::Unknown,
            bindings: HashMap::new(),
            side_effects: HashMap::new(),
        };
        
        let mut steps: Vec<Box<dyn DynamicStep>> = Vec::new();
        
        // Compile all instructions
        for instruction in bytecode.step_instructions {
            let compiler = self.step_compilers.get(instruction.operator.as_str())
                .ok_or_else(|| CompileError::UnknownStep(instruction.operator.clone()))?;
            
            let step = compiler.compile(&instruction.arguments, &mut context)?;
            steps.push(step);
        }
        
        Ok(DynamicTraversal {
            graph: self.graph,
            steps,
            current_step: 0,
            side_effects: HashMap::new(),
        })
    }
    
    /// Run a compiled traversal
    pub fn run(&self, mut traversal: DynamicTraversal) -> Result<Vec<DynamicValue>, ExecutionError> {
        let mut results = Vec::new();
        
        while let Some(traverser) = traversal.next()? {
            results.push(traverser.value);
        }
        
        Ok(results)
    }
}

/// A compiled dynamic traversal
pub struct DynamicTraversal<'g> {
    graph: &'g Graph,
    steps: Vec<Box<dyn DynamicStep>>,
    current_step: usize,
    side_effects: HashMap<String, Vec<DynamicValue>>,
}

impl<'g> DynamicTraversal<'g> {
    pub fn next(&mut self) -> Result<Option<DynamicTraverser>, ExecutionError> {
        // Pull-based iteration through the step pipeline
        // This is a simplified version - real implementation would be more sophisticated
        
        if self.steps.is_empty() {
            return Ok(None);
        }
        
        // Start from the last step and pull backwards
        loop {
            let last_idx = self.steps.len() - 1;
            
            match self.pull_from_step(last_idx)? {
                Some(traverser) => return Ok(Some(traverser)),
                None => return Ok(None),
            }
        }
    }
    
    fn pull_from_step(&mut self, step_idx: usize) -> Result<Option<DynamicTraverser>, ExecutionError> {
        // Recursive pull through the pipeline
        // Real implementation would maintain step-local state
        
        if step_idx == 0 {
            // First step (source step) - generates initial traversers
            // This would be implemented by the source step itself
            return Ok(None);
        }
        
        // Pull from upstream and process
        // ... (implementation details)
        
        Ok(None)
    }
}
```

### 5.3 Step Compiler Examples

```rust
/// Compiler for V() step
struct VStepCompiler;

impl StepCompiler for VStepCompiler {
    fn compile(
        &self,
        args: &[Argument],
        context: &mut CompilationContext,
    ) -> Result<Box<dyn DynamicStep>, CompileError> {
        let ids: Vec<VertexId> = args.iter()
            .filter_map(|arg| match arg {
                Argument::Int(id) => Some(VertexId(*id as u64)),
                Argument::VertexId(id) => Some(VertexId(*id)),
                _ => None,
            })
            .collect();
        
        context.current_type = ElementType::Vertex;
        
        Ok(Box::new(VStep { ids, position: 0 }))
    }
}

struct VStep {
    ids: Vec<VertexId>,
    position: usize,
}

impl DynamicStep for VStep {
    fn process(&mut self, _traverser: DynamicTraverser) -> DynamicStepResult {
        // Source step ignores input traverser
        if self.ids.is_empty() {
            // Scan all vertices - would integrate with graph storage
            DynamicStepResult::Done
        } else {
            if self.position < self.ids.len() {
                let id = self.ids[self.position];
                self.position += 1;
                DynamicStepResult::Emit(DynamicTraverser {
                    value: DynamicValue::Vertex(id),
                    path: Path::new(),
                    loops: 0,
                    bulk: 1,
                    sack: None,
                })
            } else {
                DynamicStepResult::Done
            }
        }
    }
    
    fn reset(&mut self) {
        self.position = 0;
    }
    
    fn clone_box(&self) -> Box<dyn DynamicStep> {
        Box::new(VStep {
            ids: self.ids.clone(),
            position: 0,
        })
    }
}

/// Compiler for has() step - handles multiple overloads
struct HasStepCompiler;

impl StepCompiler for HasStepCompiler {
    fn compile(
        &self,
        args: &[Argument],
        context: &mut CompilationContext,
    ) -> Result<Box<dyn DynamicStep>, CompileError> {
        match args.len() {
            // has(key) - property existence
            1 => {
                let key = extract_string(&args[0])?;
                Ok(Box::new(HasKeyStep { key }))
            }
            // has(key, value) or has(key, predicate)
            2 => {
                let key = extract_string(&args[0])?;
                match &args[1] {
                    Argument::Predicate(pred) => {
                        let predicate = compile_predicate(pred)?;
                        Ok(Box::new(HasPredicateStep { key, predicate }))
                    }
                    value => {
                        let value = argument_to_value(value)?;
                        Ok(Box::new(HasValueStep { key, value }))
                    }
                }
            }
            // has(label, key, value)
            3 => {
                let label = extract_string(&args[0])?;
                let key = extract_string(&args[1])?;
                let value = argument_to_value(&args[2])?;
                Ok(Box::new(HasLabelKeyValueStep { label, key, value }))
            }
            _ => Err(CompileError::InvalidArgumentCount("has", args.len())),
        }
    }
}

struct HasValueStep {
    key: String,
    value: DynamicValue,
}

impl DynamicStep for HasValueStep {
    fn process(&mut self, traverser: DynamicTraverser) -> DynamicStepResult {
        // Check if element has property with matching value
        // Would integrate with graph storage to read properties
        match &traverser.value {
            DynamicValue::Vertex(id) => {
                // graph.get_vertex(*id).property(&self.key) == self.value
                // Simplified: assume it passes
                DynamicStepResult::Emit(traverser)
            }
            DynamicValue::Edge(id) => {
                // Similar for edges
                DynamicStepResult::Emit(traverser)
            }
            _ => DynamicStepResult::Filter,
        }
    }
    
    fn reset(&mut self) {}
    
    fn clone_box(&self) -> Box<dyn DynamicStep> {
        Box::new(HasValueStep {
            key: self.key.clone(),
            value: self.value.clone(),
        })
    }
}

/// Compiler for out() step
struct OutStepCompiler;

impl StepCompiler for OutStepCompiler {
    fn compile(
        &self,
        args: &[Argument],
        context: &mut CompilationContext,
    ) -> Result<Box<dyn DynamicStep>, CompileError> {
        // Validate input type
        if context.current_type != ElementType::Vertex {
            return Err(CompileError::InvalidInputType("out", context.current_type));
        }
        
        let labels: Vec<String> = args.iter()
            .filter_map(|arg| {
                if let Argument::String(s) = arg {
                    Some(s.clone())
                } else {
                    None
                }
            })
            .collect();
        
        // Output type is still Vertex
        context.current_type = ElementType::Vertex;
        
        Ok(Box::new(OutStep { labels }))
    }
}

struct OutStep {
    labels: Vec<String>,
}

impl DynamicStep for OutStep {
    fn process(&mut self, traverser: DynamicTraverser) -> DynamicStepResult {
        match &traverser.value {
            DynamicValue::Vertex(id) => {
                // Would iterate outgoing edges and return adjacent vertices
                // graph.out_edges(*id).filter(|e| labels.contains(e.label())).map(|e| e.target())
                
                // Simplified: return empty for now
                DynamicStepResult::Filter
            }
            _ => DynamicStepResult::Filter,
        }
    }
    
    fn reset(&mut self) {}
    
    fn clone_box(&self) -> Box<dyn DynamicStep> {
        Box::new(OutStep {
            labels: self.labels.clone(),
        })
    }
}

/// Compiler for repeat() step
struct RepeatStepCompiler;

impl StepCompiler for RepeatStepCompiler {
    fn compile(
        &self,
        args: &[Argument],
        context: &mut CompilationContext,
    ) -> Result<Box<dyn DynamicStep>, CompileError> {
        // First argument should be a nested bytecode (anonymous traversal)
        let sub_bytecode = match args.get(0) {
            Some(Argument::Bytecode(bc)) => bc.as_ref().clone(),
            _ => return Err(CompileError::InvalidArgument("repeat", 0)),
        };
        
        // Look for modulator arguments (times, until, emit)
        let mut times = None;
        let mut until_bytecode = None;
        let mut emit = false;
        let mut emit_bytecode = None;
        
        for arg in args.iter().skip(1) {
            if let Argument::Map(entries) = arg {
                for (key, value) in entries {
                    match key.as_str() {
                        "times" => {
                            if let Argument::Int(n) = value {
                                times = Some(*n as usize);
                            }
                        }
                        "until" => {
                            if let Argument::Bytecode(bc) = value {
                                until_bytecode = Some(bc.as_ref().clone());
                            }
                        }
                        "emit" => {
                            emit = true;
                            if let Argument::Bytecode(bc) = value {
                                emit_bytecode = Some(bc.as_ref().clone());
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        
        Ok(Box::new(RepeatStep {
            sub_bytecode,
            times,
            until_bytecode,
            emit,
            emit_bytecode,
        }))
    }
}

struct RepeatStep {
    sub_bytecode: Bytecode,
    times: Option<usize>,
    until_bytecode: Option<Bytecode>,
    emit: bool,
    emit_bytecode: Option<Bytecode>,
}

impl DynamicStep for RepeatStep {
    fn process(&mut self, traverser: DynamicTraverser) -> DynamicStepResult {
        // Repeat step implementation would:
        // 1. Compile sub_bytecode to nested traversal
        // 2. Execute iteratively, checking times/until/emit conditions
        // 3. Maintain frontier for BFS-style traversal
        
        // This is a simplified placeholder
        DynamicStepResult::Emit(traverser)
    }
    
    fn reset(&mut self) {}
    
    fn clone_box(&self) -> Box<dyn DynamicStep> {
        Box::new(RepeatStep {
            sub_bytecode: self.sub_bytecode.clone(),
            times: self.times,
            until_bytecode: self.until_bytecode.clone(),
            emit: self.emit,
            emit_bytecode: self.emit_bytecode.clone(),
        })
    }
}

// Helper functions

fn extract_string(arg: &Argument) -> Result<String, CompileError> {
    match arg {
        Argument::String(s) => Ok(s.clone()),
        _ => Err(CompileError::ExpectedString),
    }
}

fn argument_to_value(arg: &Argument) -> Result<DynamicValue, CompileError> {
    match arg {
        Argument::Null => Ok(DynamicValue::Null),
        Argument::Bool(b) => Ok(DynamicValue::Bool(*b)),
        Argument::Int(n) => Ok(DynamicValue::Int(*n)),
        Argument::Float(f) => Ok(DynamicValue::Float(*f)),
        Argument::String(s) => Ok(DynamicValue::String(s.clone())),
        Argument::List(items) => {
            let values: Result<Vec<_>, _> = items.iter()
                .map(argument_to_value)
                .collect();
            Ok(DynamicValue::List(values?))
        }
        Argument::Map(entries) => {
            let values: Result<Vec<_>, _> = entries.iter()
                .map(|(k, v)| argument_to_value(v).map(|v| (k.clone(), v)))
                .collect();
            Ok(DynamicValue::Map(values?))
        }
        _ => Err(CompileError::UnsupportedArgumentType),
    }
}

fn compile_predicate(pred: &Predicate) -> Result<Box<dyn Fn(&DynamicValue) -> bool + Send + Sync>, CompileError> {
    let value = argument_to_value(&pred.value)?;
    let other = pred.other.as_ref().map(|o| argument_to_value(o)).transpose()?;
    
    match pred.operator {
        PredicateOperator::Eq => {
            Ok(Box::new(move |v| v == &value))
        }
        PredicateOperator::Neq => {
            Ok(Box::new(move |v| v != &value))
        }
        PredicateOperator::Lt => {
            Ok(Box::new(move |v| compare_values(v, &value) == Some(std::cmp::Ordering::Less)))
        }
        PredicateOperator::Lte => {
            Ok(Box::new(move |v| {
                matches!(compare_values(v, &value), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal))
            }))
        }
        PredicateOperator::Gt => {
            Ok(Box::new(move |v| compare_values(v, &value) == Some(std::cmp::Ordering::Greater)))
        }
        PredicateOperator::Gte => {
            Ok(Box::new(move |v| {
                matches!(compare_values(v, &value), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
            }))
        }
        PredicateOperator::Between => {
            let end = other.ok_or(CompileError::MissingPredicateArgument)?;
            Ok(Box::new(move |v| {
                matches!(compare_values(v, &value), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
                && matches!(compare_values(v, &end), Some(std::cmp::Ordering::Less))
            }))
        }
        PredicateOperator::Within => {
            if let DynamicValue::List(items) = value {
                Ok(Box::new(move |v| items.contains(v)))
            } else {
                Err(CompileError::ExpectedList)
            }
        }
        PredicateOperator::Without => {
            if let DynamicValue::List(items) = value {
                Ok(Box::new(move |v| !items.contains(v)))
            } else {
                Err(CompileError::ExpectedList)
            }
        }
        PredicateOperator::Containing => {
            if let DynamicValue::String(pattern) = value {
                Ok(Box::new(move |v| {
                    if let DynamicValue::String(s) = v {
                        s.contains(&pattern)
                    } else {
                        false
                    }
                }))
            } else {
                Err(CompileError::ExpectedString)
            }
        }
        PredicateOperator::StartingWith => {
            if let DynamicValue::String(pattern) = value {
                Ok(Box::new(move |v| {
                    if let DynamicValue::String(s) = v {
                        s.starts_with(&pattern)
                    } else {
                        false
                    }
                }))
            } else {
                Err(CompileError::ExpectedString)
            }
        }
        PredicateOperator::EndingWith => {
            if let DynamicValue::String(pattern) = value {
                Ok(Box::new(move |v| {
                    if let DynamicValue::String(s) = v {
                        s.ends_with(&pattern)
                    } else {
                        false
                    }
                }))
            } else {
                Err(CompileError::ExpectedString)
            }
        }
        _ => Err(CompileError::UnsupportedPredicate),
    }
}

fn compare_values(a: &DynamicValue, b: &DynamicValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (DynamicValue::Int(a), DynamicValue::Int(b)) => Some(a.cmp(b)),
        (DynamicValue::Float(a), DynamicValue::Float(b)) => a.partial_cmp(b),
        (DynamicValue::Int(a), DynamicValue::Float(b)) => (*a as f64).partial_cmp(b),
        (DynamicValue::Float(a), DynamicValue::Int(b)) => a.partial_cmp(&(*b as f64)),
        (DynamicValue::String(a), DynamicValue::String(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

#[derive(Debug)]
pub enum CompileError {
    UnknownStep(String),
    InvalidArgumentCount(&'static str, usize),
    InvalidArgument(&'static str, usize),
    InvalidInputType(&'static str, ElementType),
    ExpectedString,
    ExpectedList,
    UnsupportedArgumentType,
    UnsupportedPredicate,
    MissingPredicateArgument,
}

#[derive(Debug)]
pub enum ExecutionError {
    GraphError(String),
    TypeMismatch(String),
    PropertyNotFound(String),
}
```

---

## 6. Server Protocol (Optional)

### 6.1 WebSocket Server Overview

The optional Gremlin Server provides network access to RustGremlin using the TinkerPop WebSocket protocol.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     Gremlin Server Architecture                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Client (gremlin-python, gremlin-javascript, etc.)                      │
│       │                                                                 │
│       │ WebSocket (ws://host:8182/gremlin)                             │
│       ▼                                                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    WebSocket Handler                            │   │
│  │  • Connection management                                        │   │
│  │  • Session tracking                                             │   │
│  │  • Request/response framing                                     │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│       │                                                                 │
│       ▼                                                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Request Processor                            │   │
│  │  • Deserialize GraphBinary request                              │   │
│  │  • Extract bytecode                                             │   │
│  │  • Validate authentication (optional)                           │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│       │                                                                 │
│       ▼                                                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Bytecode Interpreter                         │   │
│  │  • Compile bytecode to traversal                                │   │
│  │  • Execute traversal                                            │   │
│  │  • Stream results                                               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│       │                                                                 │
│       ▼                                                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Response Serializer                          │   │
│  │  • Serialize results to GraphBinary                             │   │
│  │  • Handle pagination (batch size)                               │   │
│  │  • Send response frames                                         │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Request/Response Format

```rust
/// Gremlin Server request message
#[derive(Debug)]
pub struct RequestMessage {
    pub request_id: Uuid,
    pub op: String,           // "bytecode", "eval", "authentication", etc.
    pub processor: String,    // "traversal", "session", ""
    pub args: HashMap<String, Argument>,
}

/// Gremlin Server response message
#[derive(Debug)]
pub struct ResponseMessage {
    pub request_id: Uuid,
    pub status: ResponseStatus,
    pub result: ResponseResult,
}

#[derive(Debug)]
pub struct ResponseStatus {
    pub code: u16,
    pub message: String,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug)]
pub struct ResponseResult {
    pub data: Vec<DynamicValue>,
    pub meta: HashMap<String, String>,
}

// Status codes
pub mod status_codes {
    pub const SUCCESS: u16 = 200;
    pub const NO_CONTENT: u16 = 204;
    pub const PARTIAL_CONTENT: u16 = 206;
    pub const UNAUTHORIZED: u16 = 401;
    pub const FORBIDDEN: u16 = 403;
    pub const REQUEST_ERROR: u16 = 498;
    pub const SERVER_ERROR: u16 = 500;
    pub const SCRIPT_EVALUATION_ERROR: u16 = 597;
    pub const SERVER_TIMEOUT: u16 = 598;
    pub const SERVER_SERIALIZATION_ERROR: u16 = 599;
}
```

### 6.3 Server Implementation

```rust
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use futures::{StreamExt, SinkExt};

/// Gremlin WebSocket server
pub struct GremlinServer {
    graph: Arc<Graph>,
    config: ServerConfig,
}

pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
    pub request_timeout: Duration,
    pub result_batch_size: usize,
    pub authentication: Option<AuthConfig>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8182,
            max_connections: 100,
            request_timeout: Duration::from_secs(30),
            result_batch_size: 64,
            authentication: None,
        }
    }
}

impl GremlinServer {
    pub fn new(graph: Arc<Graph>, config: ServerConfig) -> Self {
        Self { graph, config }
    }
    
    pub async fn run(&self) -> Result<(), ServerError> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        
        println!("Gremlin Server listening on ws://{}/gremlin", addr);
        
        while let Ok((stream, peer_addr)) = listener.accept().await {
            let graph = self.graph.clone();
            let config = self.config.clone();
            
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, graph, config, peer_addr).await {
                    eprintln!("Connection error: {:?}", e);
                }
            });
        }
        
        Ok(())
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    graph: Arc<Graph>,
    config: ServerConfig,
    peer_addr: std::net::SocketAddr,
) -> Result<(), ServerError> {
    let ws_stream = accept_async(stream).await?;
    let (mut write, mut read) = ws_stream.split();
    
    let interpreter = GremlinInterpreter::new(&graph);
    
    while let Some(msg) = read.next().await {
        let msg = msg?;
        
        if msg.is_binary() {
            let request = decode_request(msg.into_data())?;
            let response = process_request(&interpreter, request, &config).await;
            let response_bytes = encode_response(response)?;
            write.send(tokio_tungstenite::tungstenite::Message::Binary(response_bytes)).await?;
        }
    }
    
    Ok(())
}

fn decode_request(data: Vec<u8>) -> Result<RequestMessage, ServerError> {
    let mut reader = GraphBinaryReader::new(&data);
    
    // Read request ID
    let request_id = reader.read_uuid()?;
    
    // Read op
    let op = reader.read_string()?;
    
    // Read processor
    let processor = reader.read_string()?;
    
    // Read args map
    let args = reader.read_map_args()?;
    
    Ok(RequestMessage {
        request_id,
        op,
        processor,
        args,
    })
}

async fn process_request(
    interpreter: &GremlinInterpreter<'_>,
    request: RequestMessage,
    config: &ServerConfig,
) -> ResponseMessage {
    match request.op.as_str() {
        "bytecode" => {
            // Extract bytecode from args
            let bytecode = match request.args.get("gremlin") {
                Some(Argument::Bytecode(bc)) => bc.as_ref().clone(),
                _ => return error_response(request.request_id, 498, "Missing bytecode"),
            };
            
            // Execute with timeout
            match tokio::time::timeout(
                config.request_timeout,
                tokio::task::spawn_blocking(move || interpreter.execute(bytecode))
            ).await {
                Ok(Ok(Ok(results))) => {
                    ResponseMessage {
                        request_id: request.request_id,
                        status: ResponseStatus {
                            code: 200,
                            message: "OK".to_string(),
                            attributes: HashMap::new(),
                        },
                        result: ResponseResult {
                            data: results,
                            meta: HashMap::new(),
                        },
                    }
                }
                Ok(Ok(Err(e))) => error_response(request.request_id, 597, &format!("{:?}", e)),
                Ok(Err(e)) => error_response(request.request_id, 500, &format!("{:?}", e)),
                Err(_) => error_response(request.request_id, 598, "Request timeout"),
            }
        }
        "authentication" => {
            // Handle authentication request
            ResponseMessage {
                request_id: request.request_id,
                status: ResponseStatus {
                    code: 200,
                    message: "OK".to_string(),
                    attributes: HashMap::new(),
                },
                result: ResponseResult {
                    data: vec![],
                    meta: HashMap::new(),
                },
            }
        }
        _ => error_response(request.request_id, 498, &format!("Unknown op: {}", request.op)),
    }
}

fn error_response(request_id: Uuid, code: u16, message: &str) -> ResponseMessage {
    ResponseMessage {
        request_id,
        status: ResponseStatus {
            code,
            message: message.to_string(),
            attributes: HashMap::new(),
        },
        result: ResponseResult {
            data: vec![],
            meta: HashMap::new(),
        },
    }
}

fn encode_response(response: ResponseMessage) -> Result<Vec<u8>, ServerError> {
    let mut writer = GraphBinaryWriter::new();
    
    // Write response format
    writer.write_byte(0x81)?;  // Version
    writer.write_uuid(&response.request_id)?;
    writer.write_int(response.status.code as i32)?;
    writer.write_string(&response.status.message)?;
    writer.write_map(&response.status.attributes)?;
    
    // Write result data
    writer.write_list(&response.result.data)?;
    writer.write_map(&response.result.meta)?;
    
    Ok(writer.into_bytes())
}
```

### 6.4 Client Connection Example

```python
# Python client example using gremlinpython
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal

# Connect to RustGremlin server
connection = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')
g = traversal().withRemote(connection)

# Execute queries
friends = g.V().has('name', 'Alice').out('knows').values('name').toList()
print(friends)

# Close connection
connection.close()
```

```javascript
// JavaScript client example using gremlin-javascript
const gremlin = require('gremlin');
const DriverRemoteConnection = gremlin.driver.DriverRemoteConnection;
const Graph = gremlin.structure.Graph;

const connection = new DriverRemoteConnection('ws://localhost:8182/gremlin');
const g = new Graph().traversal().withRemote(connection);

// Execute queries
const friends = await g.V().has('name', 'Alice').out('knows').values('name').toList();
console.log(friends);

await connection.close();
```

---

## 7. Module Structure

```
rustgremlin/
├── src/
│   ├── lib.rs                 # Public API exports
│   ├── graph.rs               # Core Graph type
│   ├── storage/               # Storage backends (see storage.md)
│   ├── traversal/             # Fluent API (see overview.md)
│   │
│   ├── gremlin/               # Gremlin interface module
│   │   ├── mod.rs             # Module exports
│   │   │
│   │   ├── bytecode/          # Bytecode handling
│   │   │   ├── mod.rs
│   │   │   ├── types.rs       # Bytecode, Instruction, Argument
│   │   │   ├── reader.rs      # GraphBinary deserializer
│   │   │   └── writer.rs      # GraphBinary serializer
│   │   │
│   │   ├── parser/            # Text parser
│   │   │   ├── mod.rs
│   │   │   ├── grammar.pest   # PEG grammar definition
│   │   │   └── builder.rs     # AST to bytecode builder
│   │   │
│   │   ├── interpreter/       # Bytecode interpreter
│   │   │   ├── mod.rs
│   │   │   ├── compiler.rs    # Bytecode to traversal compiler
│   │   │   ├── steps/         # Step compilers
│   │   │   │   ├── mod.rs
│   │   │   │   ├── source.rs  # V, E, addV, addE, inject
│   │   │   │   ├── filter.rs  # has, where, not, and, or, etc.
│   │   │   │   ├── map.rs     # out, in, values, etc.
│   │   │   │   ├── branch.rs  # union, coalesce, choose, repeat
│   │   │   │   ├── reduce.rs  # count, sum, fold, group
│   │   │   │   └── sideeffect.rs # as, store, aggregate
│   │   │   ├── dynamic.rs     # DynamicTraverser, DynamicValue
│   │   │   └── predicate.rs   # Predicate compilation
│   │   │
│   │   └── server/            # Optional WebSocket server
│   │       ├── mod.rs
│   │       ├── handler.rs     # Connection handler
│   │       ├── protocol.rs    # Request/response types
│   │       └── auth.rs        # Authentication (optional)
│   │
│   └── error.rs               # Error types
│
├── Cargo.toml
└── tests/
    ├── gremlin_parser_tests.rs
    ├── gremlin_interpreter_tests.rs
    └── gremlin_server_tests.rs
```

---

## 8. Usage Examples

### 8.1 Programmatic API (Bytecode)

```rust
use rustgremlin::prelude::*;
use rustgremlin::gremlin::{Bytecode, Instruction, Argument, GremlinInterpreter};

fn main() -> Result<(), Box<dyn Error>> {
    let graph = Graph::open("social.db")?;
    let interpreter = GremlinInterpreter::new(&graph);
    
    // Build bytecode programmatically
    let bytecode = Bytecode {
        source_instructions: vec![],
        step_instructions: vec![
            Instruction {
                operator: "V".to_string(),
                arguments: vec![],
            },
            Instruction {
                operator: "has".to_string(),
                arguments: vec![
                    Argument::String("name".to_string()),
                    Argument::String("Alice".to_string()),
                ],
            },
            Instruction {
                operator: "out".to_string(),
                arguments: vec![Argument::String("knows".to_string())],
            },
            Instruction {
                operator: "values".to_string(),
                arguments: vec![Argument::String("name".to_string())],
            },
        ],
    };
    
    let results = interpreter.execute(bytecode)?;
    
    for value in results {
        println!("{:?}", value);
    }
    
    Ok(())
}
```

### 8.2 Text Query API

```rust
use rustgremlin::prelude::*;
use rustgremlin::gremlin::{parse_gremlin, GremlinInterpreter};

fn main() -> Result<(), Box<dyn Error>> {
    let graph = Graph::open("social.db")?;
    let interpreter = GremlinInterpreter::new(&graph);
    
    // Parse and execute text query
    let query = r#"
        g.V().has('person', 'name', 'Alice')
             .out('knows')
             .has('age', P.gt(25))
             .values('name')
    "#;
    
    let bytecode = parse_gremlin(query)?;
    let results = interpreter.execute(bytecode)?;
    
    for value in results {
        println!("{:?}", value);
    }
    
    Ok(())
}
```

### 8.3 Running the Server

```rust
use rustgremlin::prelude::*;
use rustgremlin::gremlin::server::{GremlinServer, ServerConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let graph = Arc::new(Graph::open("social.db")?);
    
    let config = ServerConfig {
        host: "0.0.0.0".to_string(),
        port: 8182,
        max_connections: 100,
        request_timeout: Duration::from_secs(30),
        result_batch_size: 64,
        authentication: None,
    };
    
    let server = GremlinServer::new(graph, config);
    server.run().await?;
    
    Ok(())
}
```

### 8.4 Complex Query Examples

```rust
// Friends of friends (2 hops)
let query = r#"
    g.V().has('name', 'Alice')
         .repeat(__.out('knows')).times(2)
         .dedup()
         .values('name')
"#;

// Shortest path between two people
let query = r#"
    g.V().has('name', 'Alice')
         .repeat(__.out('knows').simplePath())
         .until(__.has('name', 'Bob'))
         .path()
         .limit(1)
"#;

// People who work at the same company as Alice
let query = r#"
    g.V().has('name', 'Alice')
         .out('works_at').as('company')
         .in('works_at')
         .where(P.neq('Alice')).by('name')
         .values('name')
"#;

// Group people by city and count
let query = r#"
    g.V().hasLabel('person')
         .group()
           .by(__.out('lives_in').values('name'))
           .by(__.count())
"#;

// Recommendation: friends of friends who share interests
let query = r#"
    g.V().has('name', 'Alice').as('me')
         .out('knows').out('knows').as('fof')
         .where(P.neq('me'))
         .not(__.select('me').out('knows'))
         .where(
             __.out('interested_in')
               .where(P.within(__.select('me').out('interested_in')))
         )
         .dedup()
         .values('name')
"#;
```

---

## 9. Implementation Effort

| Component | Effort | Dependencies | Notes |
|-----------|--------|--------------|-------|
| **Bytecode types** | 1-2 days | None | Straightforward structs |
| **GraphBinary reader** | 3-4 days | Bytecode types | Binary parsing, type codes |
| **GraphBinary writer** | 2-3 days | Bytecode types | Serialization for responses |
| **Text parser grammar** | 3-4 days | pest | Grammar definition |
| **Text parser builder** | 3-4 days | Parser grammar | AST to bytecode |
| **Interpreter core** | 3-4 days | Bytecode types | Step compiler framework |
| **Source steps** | 2-3 days | Interpreter | V, E, addV, addE, inject |
| **Filter steps** | 4-5 days | Interpreter | has, where, not, and, or, etc. |
| **Map steps** | 4-5 days | Interpreter | out, in, values, etc. |
| **Branch steps** | 5-6 days | Interpreter | union, coalesce, repeat (complex) |
| **Reduce steps** | 3-4 days | Interpreter | count, sum, group, fold |
| **Side effect steps** | 2-3 days | Interpreter | as, store, aggregate |
| **WebSocket server** | 4-5 days | tokio, tungstenite | Connection handling, protocol |
| **Result serialization** | 2-3 days | GraphBinary writer | Vertex, Edge, Path serialization |
| **Testing** | 5-6 days | All components | Unit tests, integration tests |
| **Documentation** | 2-3 days | All components | API docs, examples |
| **Total** | **~8-10 weeks** | | Full Gremlin interface |

### 9.1 Phased Implementation

**Phase 2a: Core Gremlin (4-5 weeks)**
- Bytecode types and GraphBinary reader
- Text parser
- Interpreter with essential steps (V, E, has, out, in, values, limit)
- Basic filter and map steps

**Phase 2b: Advanced Steps (2-3 weeks)**
- Branch steps (union, coalesce, repeat)
- Reduce steps (group, fold, count)
- Side effect steps (as, store)
- Complex predicates

**Phase 2c: Server (2-3 weeks)**
- WebSocket server
- GraphBinary writer
- Result serialization
- Client compatibility testing

---

## 10. Limitations

### 10.1 Unsupported Features

| Feature | Status | Reason |
|---------|--------|--------|
| Lambda/Closure steps | Not supported | Security, portability |
| Custom vertex programs | Not supported | Complex, specialized |
| `match()` step | Not supported | Complex pattern matching |
| `subgraph()` step | Not supported | Graph construction |
| `tree()` step | Not supported | Specialized output |
| Transactions | Partial | Simple commit/rollback only |
| Multi-graph | Not supported | Single graph per instance |
| Schema validation | Not supported | Schema-free design |
| Full-text search | Limited | Basic `containing()` only |

### 10.2 Known Differences from TinkerPop

| Behavior | TinkerPop | RustGremlin |
|----------|-----------|-------------|
| ID types | Object (any) | u64 only |
| Properties | Multi-valued | Single-valued (Phase 1) |
| Vertex properties | First-class | Simplified |
| Edge properties | Full support | Full support |
| Null handling | Explicit nulls | Filtered by default |
| Type coercion | Groovy-style | Strict Rust types |

### 10.3 Performance Considerations

- **Bytecode vs Text**: Text parsing adds ~5-10% overhead; prefer bytecode for high-throughput
- **Dynamic typing**: ~20-30% overhead vs native Rust API due to runtime type checks
- **Server protocol**: Network overhead; use batching for bulk operations
- **Memory**: Dynamic values require heap allocation; native API is more efficient

---

## 11. Testing Strategy

### 11.1 Compatibility Testing

Use the TinkerPop test suite where applicable:

```rust
#[cfg(test)]
mod gremlin_tests {
    use super::*;
    
    fn create_modern_graph() -> Graph {
        // Create the standard "modern" toy graph from TinkerPop
        let graph = Graph::in_memory();
        let mut g = graph.mutate();
        
        let marko = g.add_v("person").property("name", "marko").property("age", 29).build();
        let vadas = g.add_v("person").property("name", "vadas").property("age", 27).build();
        let josh = g.add_v("person").property("name", "josh").property("age", 32).build();
        let peter = g.add_v("person").property("name", "peter").property("age", 35).build();
        let lop = g.add_v("software").property("name", "lop").property("lang", "java").build();
        let ripple = g.add_v("software").property("name", "ripple").property("lang", "java").build();
        
        g.add_e("knows").from(marko).to(vadas).property("weight", 0.5).build();
        g.add_e("knows").from(marko).to(josh).property("weight", 1.0).build();
        g.add_e("created").from(marko).to(lop).property("weight", 0.4).build();
        g.add_e("created").from(josh).to(ripple).property("weight", 1.0).build();
        g.add_e("created").from(josh).to(lop).property("weight", 0.4).build();
        g.add_e("created").from(peter).to(lop).property("weight", 0.2).build();
        
        g.commit().unwrap();
        graph
    }
    
    #[test]
    fn test_v_all() {
        let graph = create_modern_graph();
        let interpreter = GremlinInterpreter::new(&graph);
        let bytecode = parse_gremlin("g.V()").unwrap();
        let results = interpreter.execute(bytecode).unwrap();
        assert_eq!(results.len(), 6);
    }
    
    #[test]
    fn test_has_name() {
        let graph = create_modern_graph();
        let interpreter = GremlinInterpreter::new(&graph);
        let bytecode = parse_gremlin("g.V().has('name', 'marko')").unwrap();
        let results = interpreter.execute(bytecode).unwrap();
        assert_eq!(results.len(), 1);
    }
    
    #[test]
    fn test_out_knows() {
        let graph = create_modern_graph();
        let interpreter = GremlinInterpreter::new(&graph);
        let bytecode = parse_gremlin(
            "g.V().has('name', 'marko').out('knows').values('name')"
        ).unwrap();
        let results = interpreter.execute(bytecode).unwrap();
        assert_eq!(results.len(), 2);
        // Should contain "vadas" and "josh"
    }
    
    #[test]
    fn test_repeat_times() {
        let graph = create_modern_graph();
        let interpreter = GremlinInterpreter::new(&graph);
        let bytecode = parse_gremlin(
            "g.V().has('name', 'marko').repeat(__.out()).times(2).values('name')"
        ).unwrap();
        let results = interpreter.execute(bytecode).unwrap();
        // Should reach software created by josh
    }
    
    #[test]
    fn test_group_count() {
        let graph = create_modern_graph();
        let interpreter = GremlinInterpreter::new(&graph);
        let bytecode = parse_gremlin(
            "g.V().hasLabel('person').groupCount().by('age')"
        ).unwrap();
        let results = interpreter.execute(bytecode).unwrap();
        assert_eq!(results.len(), 1);
        // Should be a map with age counts
    }
}
```

### 11.2 Parser Tests

```rust
#[test]
fn test_parse_simple_query() {
    let bytecode = parse_gremlin("g.V().has('name', 'Alice')").unwrap();
    assert_eq!(bytecode.step_instructions.len(), 2);
    assert_eq!(bytecode.step_instructions[0].operator, "V");
    assert_eq!(bytecode.step_instructions[1].operator, "has");
}

#[test]
fn test_parse_predicate() {
    let bytecode = parse_gremlin("g.V().has('age', P.gt(30))").unwrap();
    let has_step = &bytecode.step_instructions[1];
    assert!(matches!(&has_step.arguments[1], Argument::Predicate(_)));
}

#[test]
fn test_parse_anonymous_traversal() {
    let bytecode = parse_gremlin(
        "g.V().where(__.out('knows').has('name', 'Bob'))"
    ).unwrap();
    let where_step = &bytecode.step_instructions[1];
    assert!(matches!(&where_step.arguments[0], Argument::Bytecode(_)));
}
```

---

## 12. Summary

The Gremlin interface provides RustGremlin with standard TinkerPop-compatible query capabilities:

| Component | Purpose | Status |
|-----------|---------|--------|
| **Bytecode Types** | Standard instruction representation | Phase 2 |
| **GraphBinary** | Binary serialization format | Phase 2 |
| **Text Parser** | Parse Gremlin query strings | Phase 2 |
| **Interpreter** | Compile and execute bytecode | Phase 2 |
| **WebSocket Server** | Network access for clients | Phase 2c |

**Key benefits:**
- Interoperability with existing Gremlin clients (Python, JavaScript, Java, .NET)
- Familiar query syntax for users coming from other graph databases
- Standard protocol enables tooling and visualization support
- Separation of query interface from storage implementation

**Implementation priority:**
1. Bytecode interpreter (enables programmatic bytecode submission)
2. Text parser (enables string-based queries)
3. WebSocket server (enables remote client access)

The Gremlin interface, combined with the native Rust fluent API, provides users with flexible query options while maintaining the performance benefits of RustGremlin's optimized traversal engine.
