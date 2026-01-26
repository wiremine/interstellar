# Spec 51: Gremlin Variable Assignment Support

## Summary

Enable Gremlin-style variable assignment for storing and reusing vertex/edge references across multiple traversal operations.

**Target Pattern:**
```gremlin
// Create vertices and store references
alice = g.addV('person').property('name', 'Alice').property('age', 30).next()
bob = g.addV('person').property('name', 'Bob').property('age', 25).next()
charlie = g.addV('person').property('name', 'Charlie').property('age', 35).next()
acme = g.addV('company').property('name', 'Acme Corp').next()

// Create edges using the stored references
g.addE('knows').from(alice).to(bob).property('since', 2020).next()
g.addE('knows').from(bob).to(charlie).property('since', 2021).next()
g.addE('works_at').from(alice).to(acme).next()
g.addE('works_at').from(bob).to(acme).next()

// Query using variables
g.V(alice).out('knows').values('name').toList()
```

## Current State

### What Works Today

The Rust fluent API already supports this workflow, but with more verbose syntax:

```rust
let graph = Arc::new(Graph::new());
let g = graph.gremlin(Arc::clone(&graph));

// Create vertices - returns Option<GraphVertex>
let alice = g.add_v("person").property("name", "Alice").next().unwrap();
let bob = g.add_v("person").property("name", "Bob").next().unwrap();

// Create edge - requires explicit ID extraction
g.add_e("knows")
    .from_id(alice.id())    // Must call .id() to get VertexId
    .to_id(bob.id())
    .property("since", 2020)
    .next();

// Query - requires explicit ID extraction
g.v_id(alice.id()).out_labels(&["knows"]).values("name").to_list();
```

### Gaps

| Feature | Rust API | Gremlin Parser |
|---------|----------|----------------|
| Store vertex reference | `let alice = g.add_v(...).next()` | Not supported |
| `from(vertex)` | `from_id(vertex.id())` | Not supported |
| `to(vertex)` | `to_id(vertex.id())` | Not supported |
| `g.V(vertex)` | `g.v_id(vertex.id())` | Not supported |
| Multi-statement scripts | N/A | Not supported |
| Variable binding | N/A | Not supported |

---

## Phased Implementation

### Phase 1: Rust API Ergonomics (Core Library)

**Goal:** Make the Rust fluent API accept vertex objects directly, eliminating `.id()` calls.

**Target Syntax:**
```rust
let alice = g.add_v("person").property("name", "Alice").next().unwrap();
let bob = g.add_v("person").property("name", "Bob").next().unwrap();

// New: Accept GraphVertex directly
g.add_e("knows").from(&alice).to(&bob).next();

// New: Accept GraphVertex in source step
g.v(&alice).out_labels(&["knows"]).values("name").to_list();
```

#### Implementation

##### 1. Create `IntoVertexId` Trait

**File:** `src/value.rs`

```rust
/// Trait for types that can be converted to a VertexId.
/// 
/// This enables ergonomic APIs that accept vertex references in multiple forms.
pub trait IntoVertexId {
    fn into_vertex_id(self) -> VertexId;
}

impl IntoVertexId for VertexId {
    #[inline]
    fn into_vertex_id(self) -> VertexId {
        self
    }
}

impl IntoVertexId for u64 {
    #[inline]
    fn into_vertex_id(self) -> VertexId {
        VertexId(self)
    }
}

impl IntoVertexId for &VertexId {
    #[inline]
    fn into_vertex_id(self) -> VertexId {
        *self
    }
}
```

##### 2. Implement for GraphVertex

**File:** `src/graph_elements.rs`

```rust
impl IntoVertexId for &InMemoryVertex {
    #[inline]
    fn into_vertex_id(self) -> VertexId {
        self.id()
    }
}

impl IntoVertexId for InMemoryVertex {
    #[inline]
    fn into_vertex_id(self) -> VertexId {
        self.id()
    }
}
```

##### 3. Add `from()` / `to()` Methods to CowAddEdgeBuilder

**File:** `src/storage/cow.rs`

```rust
impl<'g> CowAddEdgeBuilder<'g> {
    /// Set the source vertex using any type that can be converted to VertexId.
    ///
    /// Accepts:
    /// - `VertexId` directly
    /// - `&GraphVertex` (reference to a vertex object)
    /// - `GraphVertex` (owned vertex object)
    /// - `u64` (raw ID value)
    ///
    /// # Example
    ///
    /// ```rust
    /// let alice = g.add_v("person").next().unwrap();
    /// let bob = g.add_v("person").next().unwrap();
    /// 
    /// // All of these work:
    /// g.add_e("knows").from(&alice).to(&bob).next();
    /// g.add_e("knows").from(alice.id()).to(bob.id()).next();
    /// g.add_e("knows").from(VertexId(0)).to(VertexId(1)).next();
    /// ```
    pub fn from(self, vertex: impl IntoVertexId) -> Self {
        self.from_id(vertex.into_vertex_id())
    }

    /// Set the target vertex using any type that can be converted to VertexId.
    pub fn to(self, vertex: impl IntoVertexId) -> Self {
        self.to_id(vertex.into_vertex_id())
    }
}
```

##### 4. Add `v()` Method That Accepts Vertex References

**File:** `src/storage/cow.rs`

```rust
impl<'g> CowTraversalSource<'g> {
    /// Start traversal from a vertex reference.
    ///
    /// Accepts any type implementing `IntoVertexId`:
    /// - `VertexId` 
    /// - `&GraphVertex`
    /// - `GraphVertex`
    /// - `u64`
    ///
    /// # Example
    ///
    /// ```rust
    /// let alice = g.add_v("person").next().unwrap();
    /// let names = g.v(&alice).out_labels(&["knows"]).values("name").to_list();
    /// ```
    pub fn v_ref(&self, vertex: impl IntoVertexId) -> CowBoundTraversal<'g, (), Value, VertexMarker> {
        self.v_id(vertex.into_vertex_id())
    }
}
```

##### 5. Add Similar Methods to Read-Only API

**File:** `src/traversal/source.rs`

Add corresponding methods to `GraphTraversalSource` and `AddEdgeBuilder` for consistency.

#### Files Changed (Phase 1)

| File | Changes |
|------|---------|
| `src/value.rs` | Add `IntoVertexId` trait |
| `src/graph_elements.rs` | Implement `IntoVertexId` for `GraphVertex` types |
| `src/storage/cow.rs` | Add `from()`, `to()`, `v_ref()` methods |
| `src/traversal/source.rs` | Add `from()`, `to()` to read-only `AddEdgeBuilder` |

#### Effort Estimate

**~2-3 hours** including tests and documentation.

---

### Phase 2: Gremlin Parser Variable Binding (Scripting Engine)

**Goal:** Enable multi-statement Gremlin scripts with variable assignment in the text parser.

**Target Syntax:**
```gremlin
alice = g.addV('person').property('name', 'Alice').next()
bob = g.addV('person').property('name', 'Bob').next()
g.addE('knows').from(alice).to(bob).next()
g.V(alice).out('knows').values('name').toList()
```

#### Implementation

##### 1. Grammar Changes

**File:** `src/gremlin/grammar.pest`

```pest
// Add at top level - scripts can contain multiple statements
script = { SOI ~ statement* ~ EOI }

// A statement is either an assignment or a standalone traversal
statement = { assignment | traversal_statement }

// Assignment binds a traversal result to a variable
assignment = { identifier ~ "=" ~ traversal_with_terminal }

// Standalone traversal (may or may not have terminal)
traversal_statement = { traversal ~ terminal_step? }

// Variable reference (for use in from/to/V/E)
variable_ref = { identifier }

// Update vertex_source to accept variables
vertex_source = { 
    "g" ~ "." ~ "V" ~ "(" ~ (vertex_id_list | variable_ref)? ~ ")" 
}

// Update edge_source to accept variables  
edge_source = { 
    "g" ~ "." ~ "E" ~ "(" ~ (edge_id_list | variable_ref)? ~ ")" 
}

// Update from/to modulators to accept variables
from_modulator = { 
    ".from" ~ "(" ~ (vertex_traversal | vertex_id | variable_ref | step_label) ~ ")" 
}

to_modulator = { 
    ".to" ~ "(" ~ (vertex_traversal | vertex_id | variable_ref | step_label) ~ ")" 
}

// Identifier for variable names (must not conflict with keywords)
identifier = @{ 
    !keyword ~ ASCII_ALPHA ~ (ASCII_ALPHANUMERIC | "_")* 
}

// Reserved keywords that can't be used as variable names
keyword = { 
    "g" | "true" | "false" | "null" | "P" | "TextP" | "__" 
}
```

##### 2. AST Changes

**File:** `src/gremlin/ast.rs`

```rust
/// A complete Gremlin script containing one or more statements.
#[derive(Debug, Clone, PartialEq)]
pub struct Script {
    pub statements: Vec<Statement>,
}

/// A single statement in a Gremlin script.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// Variable assignment: `name = traversal`
    Assignment {
        name: String,
        traversal: Traversal,
    },
    /// Standalone traversal execution
    Traversal(Traversal),
}

/// Edge endpoint specification - extended to support variables.
#[derive(Debug, Clone, PartialEq)]
pub enum EdgeEndpoint {
    /// Explicit vertex ID
    VertexId(u64),
    /// Reference to a variable
    Variable(String),
    /// Step label reference
    StepLabel(String),
    /// Sub-traversal that produces a vertex
    Traversal(Box<Traversal>),
}

/// Source step vertex specification - extended to support variables.
#[derive(Debug, Clone, PartialEq)]
pub enum VertexSource {
    /// All vertices
    All,
    /// Specific vertex IDs
    Ids(Vec<u64>),
    /// Variable reference
    Variable(String),
}
```

##### 3. Parser Changes

**File:** `src/gremlin/parser.rs`

```rust
/// Parse a multi-statement Gremlin script.
pub fn parse_script(input: &str) -> Result<Script, GremlinError> {
    let pairs = GremlinParser::parse(Rule::script, input)
        .map_err(|e| GremlinError::Parse(format!("{}", e)))?;
    
    let mut statements = Vec::new();
    for pair in pairs {
        match pair.as_rule() {
            Rule::statement => {
                statements.push(parse_statement_inner(pair)?);
            }
            Rule::EOI => {}
            _ => {}
        }
    }
    
    Ok(Script { statements })
}

fn parse_statement_inner(pair: Pair<Rule>) -> Result<Statement, GremlinError> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::assignment => {
            let mut parts = inner.into_inner();
            let name = parts.next().unwrap().as_str().to_string();
            let traversal = parse_traversal(parts.next().unwrap())?;
            Ok(Statement::Assignment { name, traversal })
        }
        Rule::traversal_statement => {
            let traversal = parse_traversal(inner)?;
            Ok(Statement::Traversal(traversal))
        }
        _ => unreachable!(),
    }
}

fn parse_edge_endpoint(pair: Pair<Rule>) -> Result<EdgeEndpoint, GremlinError> {
    match pair.as_rule() {
        Rule::vertex_id => {
            let id = pair.as_str().parse::<u64>()
                .map_err(|_| GremlinError::Parse("invalid vertex id".into()))?;
            Ok(EdgeEndpoint::VertexId(id))
        }
        Rule::variable_ref => {
            Ok(EdgeEndpoint::Variable(pair.as_str().to_string()))
        }
        Rule::step_label => {
            Ok(EdgeEndpoint::StepLabel(pair.as_str().to_string()))
        }
        Rule::vertex_traversal => {
            let traversal = parse_traversal(pair)?;
            Ok(EdgeEndpoint::Traversal(Box::new(traversal)))
        }
        _ => Err(GremlinError::Parse("invalid edge endpoint".into())),
    }
}
```

##### 4. Compiler Changes

**File:** `src/gremlin/compiler.rs`

```rust
use std::collections::HashMap;

/// Variable context for tracking bindings during script execution.
#[derive(Debug, Default)]
pub struct VariableContext {
    /// Maps variable names to their bound values
    bindings: HashMap<String, Value>,
}

impl VariableContext {
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Bind a value to a variable name.
    pub fn bind(&mut self, name: String, value: Value) {
        self.bindings.insert(name, value);
    }
    
    /// Look up a variable's value.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.bindings.get(name)
    }
    
    /// Get a vertex ID from a variable.
    pub fn get_vertex_id(&self, name: &str) -> Option<VertexId> {
        self.get(name).and_then(|v| v.as_vertex_id())
    }
}

/// Compile and execute a multi-statement script.
pub fn execute_script<'a>(
    script: &Script,
    graph: &Graph,
    graph_arc: Arc<Graph>,
) -> Result<ExecutionResult, GremlinError> {
    let mut ctx = VariableContext::new();
    let mut last_result = ExecutionResult::Unit;
    
    for statement in &script.statements {
        match statement {
            Statement::Assignment { name, traversal } => {
                // Compile and execute the traversal
                let g = graph.gremlin(Arc::clone(&graph_arc));
                let compiled = compile_traversal_with_vars(traversal, &g, &ctx)?;
                let result = compiled.execute();
                
                // Bind the result to the variable
                match result {
                    ExecutionResult::Single(Some(value)) => {
                        ctx.bind(name.clone(), value);
                    }
                    ExecutionResult::List(values) if values.len() == 1 => {
                        ctx.bind(name.clone(), values.into_iter().next().unwrap());
                    }
                    _ => {
                        return Err(GremlinError::Compile(
                            format!("assignment requires single value, got {:?}", result)
                        ));
                    }
                }
                last_result = ExecutionResult::Unit;
            }
            Statement::Traversal(traversal) => {
                let g = graph.gremlin(Arc::clone(&graph_arc));
                let compiled = compile_traversal_with_vars(traversal, &g, &ctx)?;
                last_result = compiled.execute();
            }
        }
    }
    
    Ok(last_result)
}

/// Compile a traversal with variable context for resolving references.
fn compile_traversal_with_vars<'a>(
    traversal: &Traversal,
    g: &CowTraversalSource<'a>,
    ctx: &VariableContext,
) -> Result<CompiledTraversal<'a>, GremlinError> {
    // ... compilation logic that resolves variable references using ctx
}
```

##### 5. New Public API

**File:** `src/storage/cow.rs`

```rust
impl Graph {
    /// Execute a multi-statement Gremlin script.
    ///
    /// Supports variable assignment and reference:
    ///
    /// ```gremlin
    /// alice = g.addV('person').property('name', 'Alice').next()
    /// bob = g.addV('person').property('name', 'Bob').next()
    /// g.addE('knows').from(alice).to(bob).next()
    /// g.V(alice).out('knows').values('name').toList()
    /// ```
    ///
    /// # Example
    ///
    /// ```rust
    /// let graph = Arc::new(Graph::new());
    /// 
    /// let result = graph.execute_script(r#"
    ///     alice = g.addV('person').property('name', 'Alice').next()
    ///     bob = g.addV('person').property('name', 'Bob').next()
    ///     g.addE('knows').from(alice).to(bob).next()
    ///     g.V(alice).out('knows').values('name').toList()
    /// "#)?;
    /// ```
    pub fn execute_script(&self, script: &str) -> Result<ExecutionResult, GremlinError> {
        let parsed = gremlin::parse_script(script)?;
        gremlin::execute_script(&parsed, self, Arc::new(self.clone()))
    }
}
```

**File:** `src/gremlin/mod.rs`

```rust
// Add to public exports
pub use parser::parse_script;
pub use compiler::{execute_script, VariableContext};
```

#### Files Changed (Phase 2)

| File | Changes |
|------|---------|
| `src/gremlin/grammar.pest` | Add script, statement, assignment, variable_ref rules |
| `src/gremlin/ast.rs` | Add `Script`, `Statement`, update `EdgeEndpoint` |
| `src/gremlin/parser.rs` | Add `parse_script()`, variable parsing |
| `src/gremlin/compiler.rs` | Add `VariableContext`, `execute_script()` |
| `src/gremlin/mod.rs` | Export new types and functions |
| `src/storage/cow.rs` | Add `Graph::execute_script()` |

#### Effort Estimate

**~6-8 hours** including tests, error handling, and documentation.

---

## Testing Strategy

### Phase 1 Tests

```rust
#[test]
fn test_from_to_accept_graph_vertex() {
    let graph = Arc::new(Graph::new());
    let g = graph.gremlin(Arc::clone(&graph));
    
    let alice = g.add_v("person").property("name", "Alice").next().unwrap();
    let bob = g.add_v("person").property("name", "Bob").next().unwrap();
    
    // Test from/to with GraphVertex reference
    let edge = g.add_e("knows").from(&alice).to(&bob).next();
    assert!(edge.is_some());
    
    // Test v_ref with GraphVertex reference
    let names: Vec<String> = g.v_ref(&alice)
        .out_labels(&["knows"])
        .values("name")
        .to_list()
        .into_iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();
    assert_eq!(names, vec!["Bob"]);
}

#[test]
fn test_from_to_accept_vertex_id() {
    let graph = Arc::new(Graph::new());
    let g = graph.gremlin(Arc::clone(&graph));
    
    let alice = g.add_v("person").next().unwrap();
    let bob = g.add_v("person").next().unwrap();
    
    // Test from/to with VertexId
    let edge = g.add_e("knows").from(alice.id()).to(bob.id()).next();
    assert!(edge.is_some());
}
```

### Phase 2 Tests

```rust
#[test]
fn test_script_variable_assignment() {
    let graph = Arc::new(Graph::new());
    
    let result = graph.execute_script(r#"
        alice = g.addV('person').property('name', 'Alice').next()
        bob = g.addV('person').property('name', 'Bob').next()
        g.addE('knows').from(alice).to(bob).next()
        g.V(alice).out('knows').values('name').toList()
    "#).unwrap();
    
    match result {
        ExecutionResult::List(values) => {
            assert_eq!(values.len(), 1);
            assert_eq!(values[0], Value::String("Bob".into()));
        }
        _ => panic!("Expected list result"),
    }
}

#[test]
fn test_script_multiple_edges() {
    let graph = Arc::new(Graph::new());
    
    graph.execute_script(r#"
        alice = g.addV('person').property('name', 'Alice').next()
        bob = g.addV('person').property('name', 'Bob').next()
        charlie = g.addV('person').property('name', 'Charlie').next()
        
        g.addE('knows').from(alice).to(bob).property('since', 2020).next()
        g.addE('knows').from(bob).to(charlie).property('since', 2021).next()
        g.addE('knows').from(alice).to(charlie).property('since', 2022).next()
    "#).unwrap();
    
    assert_eq!(graph.vertex_count(), 3);
    assert_eq!(graph.edge_count(), 3);
}

#[test]
fn test_script_variable_not_found() {
    let graph = Arc::new(Graph::new());
    
    let result = graph.execute_script(r#"
        g.addE('knows').from(undefined_var).to(bob).next()
    "#);
    
    assert!(result.is_err());
}
```

---

## Migration Guide

### Before (Current)

```rust
let g = graph.gremlin(Arc::clone(&graph));

let alice = g.add_v("person").property("name", "Alice").next().unwrap();
let bob = g.add_v("person").property("name", "Bob").next().unwrap();

g.add_e("knows").from_id(alice.id()).to_id(bob.id()).next();
g.v_id(alice.id()).out_labels(&["knows"]).values("name").to_list();
```

### After Phase 1 (Rust API)

```rust
let g = graph.gremlin(Arc::clone(&graph));

let alice = g.add_v("person").property("name", "Alice").next().unwrap();
let bob = g.add_v("person").property("name", "Bob").next().unwrap();

// New ergonomic methods - no .id() needed
g.add_e("knows").from(&alice).to(&bob).next();
g.v_ref(&alice).out_labels(&["knows"]).values("name").to_list();
```

### After Phase 2 (Gremlin Scripts)

```rust
let result = graph.execute_script(r#"
    alice = g.addV('person').property('name', 'Alice').next()
    bob = g.addV('person').property('name', 'Bob').next()
    g.addE('knows').from(alice).to(bob).next()
    g.V(alice).out('knows').values('name').toList()
"#)?;
```

---

## Open Questions

1. **Variable Scope:** Should variables persist across multiple `execute_script()` calls, or be isolated per call?
   - **Recommendation:** Isolated per call (stateless). Users can chain statements in a single script.

2. **Type Safety:** Should we validate that variables hold vertex values before using in `from()`/`to()`?
   - **Recommendation:** Yes, return clear error messages like "variable 'x' is not a vertex".

3. **Shadowing:** Should re-assignment to an existing variable be allowed?
   - **Recommendation:** Yes, like most scripting languages.

4. **Return Value:** What should `execute_script()` return when there are multiple statements?
   - **Recommendation:** Return the result of the last statement (like a REPL).

---

## Timeline

| Phase | Task | Estimate |
|-------|------|----------|
| 1.1 | Add `IntoVertexId` trait | 30 min |
| 1.2 | Implement for GraphVertex types | 30 min |
| 1.3 | Add `from()`/`to()` methods | 1 hour |
| 1.4 | Add `v_ref()` method | 30 min |
| 1.5 | Tests and docs | 1 hour |
| **Phase 1 Total** | | **~3 hours** |
| 2.1 | Grammar changes | 2 hours |
| 2.2 | AST changes | 1 hour |
| 2.3 | Parser changes | 2 hours |
| 2.4 | Compiler changes | 2 hours |
| 2.5 | Public API | 30 min |
| 2.6 | Tests and docs | 2 hours |
| **Phase 2 Total** | | **~8 hours** |
| **Grand Total** | | **~11 hours** |

---

## References

- [TinkerPop Gremlin Documentation](https://tinkerpop.apache.org/docs/current/reference/)
- [Gremlin Language Variants](https://tinkerpop.apache.org/docs/current/tutorials/gremlin-language-variants/)
- Current implementation: `src/gremlin/`, `src/storage/cow.rs`
- Gremlin API docs: `docs/api/gremlin.md`
