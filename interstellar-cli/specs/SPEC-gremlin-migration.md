# Interstellar CLI: Rhai to Native Gremlin Migration Spec

## Overview

This specification outlines the migration of the Interstellar CLI from the Rhai-based Gremlin scripting engine to the new native Gremlin parser/compiler introduced in the core `interstellar` package.

### Current State

The CLI currently uses **Rhai** as the scripting engine for Gremlin-style queries:
- Rhai scripts use a Gremlin-like method-chaining API
- Syntax: `g.v().has_label("Person").values("name").to_list()` (Rhai/Rust style)
- Results are returned as `rhai::Dynamic` values
- Full Rhai scripting capabilities (variables, control flow, functions)

### Target State

The CLI will use the new **native Gremlin parser/compiler**:
- TinkerPop-compatible Gremlin syntax parsed directly
- Syntax: `g.V().hasLabel('person').values('name').toList()` (standard Gremlin)
- Results are returned as `interstellar::gremlin::ExecutionResult`
- Full mutation support via traversal API (`addV`, `addE`, `drop`, `property`)

---

## Architecture Changes

### Dependencies

**Before (`Cargo.toml`):**
```toml
interstellar = { path = "../interstellar", features = ["mmap", "rhai"] }
rhai = { version = "1", features = ["sync"] }
```

**After (`Cargo.toml`):**
```toml
interstellar = { path = "../interstellar", features = ["mmap", "gremlin"] }
```

The `rhai` dependency is completely removed.

### Module Structure

The `src/gremlin/mod.rs` module will be completely rewritten:

**Before:**
```
src/gremlin/mod.rs
  - GremlinEngine (wraps RhaiEngine + Graph)
  - PersistentGremlinEngine (wraps RhaiEngine + PersistentGraph)
  - format_dynamic() - formats rhai::Dynamic values
  - format_value() - formats interstellar::Value
  - format_graph_vertex/edge() - formats graph elements
```

**After:**
```
src/gremlin/mod.rs
  - GremlinEngine (wraps Graph + provides parse/compile/execute)
  - PersistentGremlinEngine (wraps PersistentGraph)
  - format_result() - formats ExecutionResult
  - format_value() - formats interstellar::Value (retained)
```

---

## Detailed Changes

### 1. `src/gremlin/mod.rs` - Complete Rewrite

#### Current Implementation (Rhai-based)

```rust
use interstellar::rhai::{create_anonymous_factory, RhaiEngine, RhaiGraph};
use rhai::{Dynamic, Scope};

pub struct GremlinEngine {
    engine: RhaiEngine,
    graph: Arc<Graph>,
}

impl GremlinEngine {
    pub fn execute(&self, script: &str) -> Result<Dynamic> {
        let rhai_graph = RhaiGraph::from_arc(Arc::clone(&self.graph));
        let g = rhai_graph.gremlin();
        let mut scope = Scope::new();
        scope.push("g", g);
        scope.push("graph", rhai_graph);
        scope.push("A", create_anonymous_factory());
        self.engine.engine().eval_with_scope(&mut scope, script)
    }
}
```

#### New Implementation (Native Gremlin)

```rust
use std::sync::Arc;

use interstellar::gremlin::{self, ExecutionResult, CompileError, ParseError};
use interstellar::storage::Graph;
use interstellar::Value;

use crate::error::{CliError, Result};

/// Gremlin execution engine using native parser/compiler.
pub struct GremlinEngine {
    graph: Arc<Graph>,
}

impl GremlinEngine {
    /// Create a new Gremlin engine for the given graph.
    pub fn new(graph: Graph) -> Self {
        Self::with_arc(Arc::new(graph))
    }

    /// Create a new Gremlin engine with a shared graph reference.
    pub fn with_arc(graph: Arc<Graph>) -> Self {
        Self { graph }
    }

    /// Get a reference to the underlying graph.
    pub fn graph(&self) -> &Graph {
        &self.graph
    }

    /// Execute a Gremlin query string and return the result.
    pub fn execute(&self, query: &str) -> Result<ExecutionResult> {
        // Parse the query string into an AST
        let ast = gremlin::parse(query)
            .map_err(|e| self.map_parse_error(e))?;
        
        // Get a snapshot for traversal execution
        let snapshot = self.graph.snapshot();
        let g = snapshot.gremlin();
        
        // Compile the AST into a traversal
        let compiled = gremlin::compile(&ast, &g)
            .map_err(|e| self.map_compile_error(e))?;
        
        // Execute and return results
        Ok(compiled.execute())
    }

    fn map_parse_error(&self, error: ParseError) -> CliError {
        CliError::query_syntax(error.to_string())
    }

    fn map_compile_error(&self, error: CompileError) -> CliError {
        match &error {
            CompileError::UnsupportedStep { .. } => {
                CliError::query_execution(error.to_string())
            }
            _ => CliError::query_syntax(error.to_string())
        }
    }
}
```

#### PersistentGremlinEngine

```rust
use interstellar::storage::PersistentGraph;

/// Gremlin execution engine for persistent (mmap-backed) graphs.
pub struct PersistentGremlinEngine {
    graph: Arc<PersistentGraph>,
}

impl PersistentGremlinEngine {
    /// Create a new Gremlin engine for the given persistent graph.
    pub fn new(graph: PersistentGraph) -> Self {
        Self::with_arc(Arc::new(graph))
    }

    /// Create a new Gremlin engine with a shared graph reference.
    pub fn with_arc(graph: Arc<PersistentGraph>) -> Self {
        Self { graph }
    }

    /// Get a reference to the underlying graph.
    pub fn graph(&self) -> &PersistentGraph {
        &self.graph
    }

    /// Execute a Gremlin query string and return the result.
    pub fn execute(&self, query: &str) -> Result<ExecutionResult> {
        let ast = gremlin::parse(query)
            .map_err(|e| self.map_parse_error(e))?;
        
        let snapshot = self.graph.snapshot();
        let g = snapshot.gremlin();
        
        let compiled = gremlin::compile(&ast, &g)
            .map_err(|e| self.map_compile_error(e))?;
        
        Ok(compiled.execute())
    }

    fn map_parse_error(&self, error: ParseError) -> CliError {
        CliError::query_syntax(error.to_string())
    }

    fn map_compile_error(&self, error: CompileError) -> CliError {
        match &error {
            CompileError::UnsupportedStep { .. } => {
                CliError::query_execution(error.to_string())
            }
            _ => CliError::query_syntax(error.to_string())
        }
    }
}
```

### 2. Result Formatting

#### Current: `format_dynamic()`

Handles `rhai::Dynamic` values with complex type detection and casting.

#### New: `format_result()`

Handles `ExecutionResult` enum directly - much simpler:

```rust
use std::collections::HashSet;
use interstellar::gremlin::ExecutionResult;
use interstellar::Value;

/// Format an ExecutionResult for human-readable output.
pub fn format_result(result: &ExecutionResult) -> String {
    match result {
        ExecutionResult::List(values) => format_value_list(values),
        ExecutionResult::Single(Some(value)) => format_value(value),
        ExecutionResult::Single(None) => "(no result)".to_string(),
        ExecutionResult::Set(values) => format_value_set(values),
        ExecutionResult::Bool(b) => b.to_string(),
        ExecutionResult::Unit => "(executed)".to_string(),
    }
}

fn format_value_list(values: &[Value]) -> String {
    if values.is_empty() {
        return "[]".to_string();
    }
    let items: Vec<String> = values.iter().map(format_value).collect();
    format!("[\n  {}\n]", items.join(",\n  "))
}

fn format_value_set(values: &HashSet<Value>) -> String {
    if values.is_empty() {
        return "{}".to_string();
    }
    let items: Vec<String> = values.iter().map(format_value).collect();
    format!("{{{}}}", items.join(", "))
}

/// Format an Interstellar Value for display.
fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => format!("{:.2}", f),
        Value::String(s) => s.clone(),
        Value::List(items) => {
            if items.is_empty() {
                "[]".to_string()
            } else {
                let formatted: Vec<String> = items.iter().map(format_value).collect();
                format!("[{}]", formatted.join(", "))
            }
        }
        Value::Map(map) => {
            if map.is_empty() {
                "{}".to_string()
            } else {
                let formatted: Vec<String> = map
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, format_value(v)))
                    .collect();
                format!("{{{}}}", formatted.join(", "))
            }
        }
        Value::Vertex(id) => format!("v[{}]", id.0),
        Value::Edge(id) => format!("e[{}]", id.0),
    }
}
```

### 3. REPL Changes (`src/repl/mod.rs`)

#### Import Changes

**Before:**
```rust
use crate::gremlin::{format_dynamic, GremlinEngine, PersistentGremlinEngine};
```

**After:**
```rust
use crate::gremlin::{format_result, GremlinEngine, PersistentGremlinEngine};
```

#### Query Execution

**Before:**
```rust
QueryMode::Gremlin => {
    let result = self.gremlin_engine.execute(query)?;
    let output = format_dynamic(&result);
    println!("{}", output);
}
```

**After:**
```rust
QueryMode::Gremlin => {
    let result = self.gremlin_engine.execute(query)?;
    let output = format_result(&result);
    println!("{}", output);
}
```

#### Query Completion Detection

The `is_query_complete()` function needs updating for standard Gremlin syntax:

**Before:**
```rust
QueryMode::Gremlin => {
    // Rhai: ends with ) or ] or ;
    trimmed.ends_with(')') || trimmed.ends_with(']') || trimmed.ends_with(';')
}
```

**After:**
```rust
QueryMode::Gremlin => {
    // Standard Gremlin: must end with ) for complete traversal
    // Terminal steps like toList(), next(), iterate() end with )
    // Non-terminal queries also end with ) and will default to toList() behavior
    trimmed.ends_with(')')
}
```

### 4. Syntax Highlighting (`src/repl/highlighter.rs`)

Update the Gremlin keywords/methods to match TinkerPop standard:

**Before (Rhai-style):**
```rust
const GREMLIN_METHODS: &[&str] = &[
    "v", "e", "add_v", "add_e",
    "out", "in_", "both", "out_e", "in_e", "both_e",
    "has", "has_label", "has_not", "has_value", "has_where",
    "values", "value_map", "to_list", "first", "count",
    // ... snake_case methods
];
```

**After (TinkerPop-style):**
```rust
const GREMLIN_METHODS: &[&str] = &[
    // Source steps
    "V", "E", "addV", "addE", "inject",
    // Navigation
    "out", "in", "both", "outE", "inE", "bothE", "outV", "inV", "bothV", "otherV",
    // Filter
    "has", "hasLabel", "hasId", "hasNot", "hasKey", "hasValue", "where", "is",
    "and", "or", "not", "dedup", "limit", "skip", "range", "tail", "coin", "sample",
    "simplePath", "cyclicPath",
    // Transform  
    "values", "valueMap", "elementMap", "propertyMap", "id", "label", "key", "value",
    "path", "select", "project", "by", "unfold", "fold", "count", "sum", "max", "min",
    "mean", "order", "math", "constant", "identity", "index", "loops",
    // Branch
    "choose", "union", "coalesce", "optional", "local", "branch", "option",
    // Repeat
    "repeat", "times", "until", "emit",
    // Side effect
    "as", "aggregate", "store", "cap", "sideEffect", "profile",
    // Mutation
    "property", "from", "to", "drop",
    // Terminal
    "toList", "toSet", "next", "iterate", "hasNext",
];

const GREMLIN_PREDICATES: &[&str] = &[
    // P predicates
    "P.eq", "P.neq", "P.lt", "P.lte", "P.gt", "P.gte",
    "P.between", "P.inside", "P.outside",
    "P.within", "P.without",
    "P.and", "P.or", "P.not",
    // TextP predicates
    "TextP.containing", "TextP.notContaining",
    "TextP.startingWith", "TextP.notStartingWith",
    "TextP.endingWith", "TextP.notEndingWith",
    "TextP.regex",
];
```

### 5. Tab Completion (`src/repl/completer.rs`)

Update completion candidates to match TinkerPop syntax:

```rust
const GREMLIN_COMPLETIONS: &[&str] = &[
    // Source
    "g.V()", "g.E()", "g.addV('", "g.addE('", "g.inject(",
    // Navigation
    ".out()", ".out('", ".in()", ".in('", ".both()", ".both('",
    ".outE()", ".outE('", ".inE()", ".inE('", ".bothE()", ".bothE('",
    ".outV()", ".inV()", ".bothV()", ".otherV()",
    // Filter
    ".has('", ".hasLabel('", ".hasId(", ".hasNot('", ".hasKey('", ".hasValue(",
    ".where(", ".is(", ".and(", ".or(", ".not(",
    ".dedup()", ".limit(", ".skip(", ".range(", ".tail(",
    ".simplePath()", ".cyclicPath()",
    // Transform
    ".values('", ".valueMap()", ".elementMap()", ".propertyMap()",
    ".id()", ".label()", ".key()", ".value()",
    ".path()", ".select('", ".project('", ".by('", ".by(",
    ".unfold()", ".fold()", ".count()", ".sum()", ".max()", ".min()", ".mean()",
    ".order()", ".constant(",
    // Branch
    ".choose(", ".union(", ".coalesce(", ".optional(", ".local(",
    // Repeat  
    ".repeat(", ".times(", ".until(", ".emit(",
    // Side effect
    ".as('", ".aggregate('", ".store('", ".cap('", ".sideEffect(",
    // Mutation
    ".addV('", ".addE('", ".property('", ".from('", ".to('", ".drop()",
    // Terminal
    ".toList()", ".toSet()", ".next()", ".next(", ".iterate()", ".hasNext()",
    // Predicates
    "P.eq(", "P.neq(", "P.lt(", "P.lte(", "P.gt(", "P.gte(",
    "P.between(", "P.inside(", "P.outside(",
    "P.within(", "P.without(",
    "TextP.containing('", "TextP.startingWith('", "TextP.endingWith('", "TextP.regex('",
    // Anonymous traversals
    "__(", "__.out(", "__.in(", "__.has(", "__.hasLabel(", "__.values(",
];
```

---

## Mutation Support

The native Gremlin compiler fully supports mutations through the traversal API:

### Add Vertices

```gremlin
// Create a vertex with label
g.addV('person')

// Create a vertex with properties
g.addV('person').property('name', 'Alice').property('age', 30)
```

### Add Edges

```gremlin
// Using as() labels for endpoints
g.V().has('name', 'Alice').as('a').V().has('name', 'Bob').as('b').addE('knows').from('a').to('b')

// With edge properties
g.addE('knows').from('a').to('b').property('since', 2020)

// Using vertex IDs directly
g.addE('knows').from(1).to(2)
```

### Update Properties

```gremlin
// Add/update property on existing elements
g.V().has('name', 'Alice').property('age', 31)
```

### Delete Elements

```gremlin
// Delete vertices matching a condition
g.V().hasLabel('temp').drop()

// Delete specific edges
g.E().hasLabel('deprecated').drop()

// Use iterate() to execute without returning results
g.V().hasLabel('temp').drop().iterate()
```

---

## Syntax Migration Guide

Users will need to update their Gremlin queries from Rhai-style to TinkerPop-style:

| Rhai Syntax | TinkerPop Syntax |
|-------------|------------------|
| `g.v()` | `g.V()` |
| `g.e()` | `g.E()` |
| `g.add_v("label")` | `g.addV('label')` |
| `g.add_e("label")` | `g.addE('label')` |
| `.has_label("Person")` | `.hasLabel('person')` |
| `.has_value("name", "Alice")` | `.has('name', 'Alice')` |
| `.has_where("age", p::gt(30))` | `.has('age', P.gt(30))` |
| `.has_not("deleted")` | `.hasNot('deleted')` |
| `.has_id(1)` | `.hasId(1)` |
| `.out_labels(&["knows"])` | `.out('knows')` |
| `.in_()` | `.in()` |
| `.out_e()` | `.outE()` |
| `.in_e()` | `.inE()` |
| `.out_v()` | `.outV()` |
| `.in_v()` | `.inV()` |
| `.to_list()` | `.toList()` |
| `.to_set()` | `.toSet()` |
| `.first()` | `.next()` |
| `.value_map()` | `.valueMap()` |
| `.element_map()` | `.elementMap()` |
| `A.out()` | `__.out()` |

**String literals:** Single quotes preferred (`'label'`), double quotes also supported (`"label"`)

---

## Breaking Changes

1. **Syntax changes** - Rhai-style method names (snake_case) no longer work; use TinkerPop camelCase
2. **Return types** - `rhai::Dynamic` replaced by `ExecutionResult` enum
3. **Scripting removed** - No more Rhai variables, control flow, or functions in Gremlin mode
4. **Anonymous traversal syntax** - `A.` prefix replaced by `__.` (double underscore)

---

## Files to Modify

| File | Change Type | Description |
|------|-------------|-------------|
| `Cargo.toml` | Modify | Remove `rhai` dep, change feature `rhai` → `gremlin` |
| `src/gremlin/mod.rs` | Rewrite | Replace Rhai engine with native parser/compiler |
| `src/repl/mod.rs` | Modify | Update imports, change `format_dynamic` → `format_result` |
| `src/repl/highlighter.rs` | Modify | Update method/predicate lists for TinkerPop syntax |
| `src/repl/completer.rs` | Modify | Update completion candidates for TinkerPop syntax |

---

## Migration Steps

### Phase 1: Core Engine Migration
1. Update `Cargo.toml` - remove `rhai`, add `gremlin` feature
2. Rewrite `src/gremlin/mod.rs`:
   - Remove all Rhai imports and types
   - Implement `GremlinEngine` using `gremlin::parse()` and `gremlin::compile()`
   - Implement `PersistentGremlinEngine` similarly
   - Replace `format_dynamic()` with `format_result()`
   - Keep `format_value()` (already handles `interstellar::Value`)

### Phase 2: REPL Integration
1. Update imports in `src/repl/mod.rs`
2. Change `format_dynamic(&result)` to `format_result(&result)`
3. Update `is_query_complete()` for TinkerPop syntax
4. Test basic read queries

### Phase 3: UX Improvements
1. Update `src/repl/highlighter.rs` with TinkerPop method names
2. Update `src/repl/completer.rs` with TinkerPop completions
3. Test syntax highlighting and tab completion

### Phase 4: Testing
1. Update existing tests for new syntax
2. Add tests for mutation queries (`addV`, `addE`, `drop`)
3. Add tests for predicates (`P.gt`, `P.within`, `TextP.containing`)
4. Test error handling for invalid syntax

---

## Testing Strategy

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use interstellar::gremlin::ExecutionResult;

    fn test_graph() -> Graph {
        let graph = Graph::new();
        let alice = graph.add_vertex("person", [
            ("name".to_string(), Value::String("Alice".to_string())),
            ("age".to_string(), Value::Int(30)),
        ].into_iter().collect());
        let bob = graph.add_vertex("person", [
            ("name".to_string(), Value::String("Bob".to_string())),
            ("age".to_string(), Value::Int(25)),
        ].into_iter().collect());
        graph.add_edge(alice, bob, "knows", Default::default()).unwrap();
        graph
    }

    #[test]
    fn test_basic_query() {
        let graph = test_graph();
        let engine = GremlinEngine::new(graph);
        
        let result = engine.execute("g.V().hasLabel('person').toList()").unwrap();
        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 2);
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_values_query() {
        let graph = test_graph();
        let engine = GremlinEngine::new(graph);
        
        let result = engine.execute("g.V().values('name').toList()").unwrap();
        if let ExecutionResult::List(values) = result {
            assert!(values.contains(&Value::String("Alice".to_string())));
            assert!(values.contains(&Value::String("Bob".to_string())));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_predicate_query() {
        let graph = test_graph();
        let engine = GremlinEngine::new(graph);
        
        let result = engine.execute("g.V().has('age', P.gt(27)).values('name').toList()").unwrap();
        if let ExecutionResult::List(values) = result {
            assert_eq!(values, vec![Value::String("Alice".to_string())]);
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_navigation_query() {
        let graph = test_graph();
        let engine = GremlinEngine::new(graph);
        
        let result = engine.execute("g.V().has('name', 'Alice').out('knows').values('name').toList()").unwrap();
        if let ExecutionResult::List(values) = result {
            assert_eq!(values, vec![Value::String("Bob".to_string())]);
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_add_vertex() {
        let graph = Graph::new();
        let engine = GremlinEngine::new(graph);
        
        let result = engine.execute("g.addV('test').property('name', 'New')").unwrap();
        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 1);
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_terminal_next() {
        let graph = test_graph();
        let engine = GremlinEngine::new(graph);
        
        let result = engine.execute("g.V().hasLabel('person').next()").unwrap();
        assert!(matches!(result, ExecutionResult::Single(Some(_))));
    }

    #[test]
    fn test_terminal_has_next() {
        let graph = test_graph();
        let engine = GremlinEngine::new(graph);
        
        let result = engine.execute("g.V().hasLabel('person').hasNext()").unwrap();
        assert!(matches!(result, ExecutionResult::Bool(true)));
    }

    #[test]
    fn test_parse_error() {
        let graph = test_graph();
        let engine = GremlinEngine::new(graph);
        
        let result = engine.execute("g.V(.invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_format_result_list() {
        let result = ExecutionResult::List(vec![
            Value::String("Alice".to_string()),
            Value::String("Bob".to_string()),
        ]);
        let output = format_result(&result);
        assert!(output.contains("Alice"));
        assert!(output.contains("Bob"));
    }

    #[test]
    fn test_format_result_single() {
        let result = ExecutionResult::Single(Some(Value::Int(42)));
        assert_eq!(format_result(&result), "42");
    }

    #[test]
    fn test_format_result_unit() {
        let result = ExecutionResult::Unit;
        assert_eq!(format_result(&result), "(executed)");
    }
}
```

---

## Timeline Estimate

- **Phase 1**: 1-2 days (core engine rewrite)
- **Phase 2**: 0.5 day (REPL integration)
- **Phase 3**: 0.5 day (UX improvements)
- **Phase 4**: 1 day (testing)

**Total**: ~3-4 days

---

## Appendix: Supported Gremlin Steps

The native parser supports the following TinkerPop Gremlin steps:

### Source Steps
- `g.V()`, `g.V(id)`, `g.V(id, id, ...)`
- `g.E()`, `g.E(id)`
- `g.addV('label')`
- `g.addE('label').from(...).to(...)`
- `g.inject(values...)`

### Navigation Steps
- `out()`, `out('label')`, `out('l1', 'l2')`
- `in()`, `in('label')`
- `both()`, `both('label')`
- `outE()`, `inE()`, `bothE()`
- `outV()`, `inV()`, `bothV()`, `otherV()`

### Filter Steps
- `has('key')`, `has('key', value)`, `has('key', P.predicate)`, `has('label', 'key', value)`
- `hasLabel('label')`, `hasLabel('l1', 'l2')`
- `hasId(id)`, `hasId(id1, id2)`
- `hasNot('key')`
- `hasKey('key')`, `hasValue(value)`
- `where(traversal)`, `where(P.predicate)`
- `is(value)`, `is(P.predicate)`
- `and(t1, t2)`, `or(t1, t2)`, `not(t)`
- `dedup()`, `dedup('key')`
- `limit(n)`, `skip(n)`, `range(start, end)`, `tail()`, `tail(n)`
- `coin(probability)`, `sample(n)`
- `simplePath()`, `cyclicPath()`

### Transform Steps
- `values('key')`, `values('k1', 'k2')`
- `valueMap()`, `valueMap(true)`, `valueMap('k1', 'k2')`
- `elementMap()`, `propertyMap()`, `properties()`
- `id()`, `label()`, `key()`, `value()`
- `path()`
- `select('label')`, `select('l1', 'l2')`
- `project('k1', 'k2').by(...).by(...)`
- `by('key')`, `by(traversal)`, `by(asc)`, `by(desc)`
- `unfold()`, `fold()`
- `count()`, `sum()`, `max()`, `min()`, `mean()`
- `order().by(...)`, `math('expression')`
- `constant(value)`, `identity()`, `index()`, `loops()`

### Branch Steps
- `choose(cond, true_trav, false_trav)`
- `union(t1, t2, ...)`
- `coalesce(t1, t2, ...)`
- `optional(t)`
- `local(t)`

### Repeat Steps
- `repeat(t).times(n)`
- `repeat(t).until(cond)`
- `emit()`, `emit(cond)`

### Side Effect Steps
- `as('label')`
- `aggregate('key')`, `store('key')`, `cap('key')`
- `sideEffect(t)`, `profile()`

### Mutation Steps
- `addV('label')` (source or inline)
- `addE('label').from('a').to('b')` (source or inline)
- `addE('label').from(vertexId).to(vertexId)`
- `property('key', value)`
- `drop()`

### Terminal Steps
- `toList()` (default if no terminal specified)
- `toSet()`
- `next()`, `next(n)`
- `iterate()`
- `hasNext()`

### Predicates
- `P.eq(v)`, `P.neq(v)`, `P.lt(v)`, `P.lte(v)`, `P.gt(v)`, `P.gte(v)`
- `P.between(start, end)`, `P.inside(start, end)`, `P.outside(start, end)`
- `P.within(v1, v2, ...)`, `P.without(v1, v2, ...)`
- `P.and(p1, p2)`, `P.or(p1, p2)`, `P.not(p)`
- `TextP.containing(s)`, `TextP.notContaining(s)`
- `TextP.startingWith(s)`, `TextP.notStartingWith(s)`
- `TextP.endingWith(s)`, `TextP.notEndingWith(s)`
- `TextP.regex(pattern)`

### Anonymous Traversals
- `__.out()`, `__.in()`, `__.both()`
- `__.has('key')`, `__.hasLabel('label')`
- `__.values('key')`, `__.id()`, `__.label()`
- `__.select('label')`, `__.constant(value)`
- (Most steps available in anonymous form)
