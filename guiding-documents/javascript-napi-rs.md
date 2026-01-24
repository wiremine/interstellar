# Native Node.js Bindings via napi-rs

This document outlines the plan for creating native Node.js bindings for Interstellar using [napi-rs](https://napi.rs/), providing better performance than WASM for server-side JavaScript applications.

## Goals

1. **Native performance** - No WASM boundary overhead, full threading support
2. **Full traversal API** exposed to JavaScript with Gremlin-style fluent interface
3. **Graph construction/mutation** APIs for building graphs
4. **TypeScript support** - Auto-generated type definitions
5. **Cross-platform distribution** - Prebuilt binaries for all major platforms

## Comparison: napi-rs vs WASM for Node.js

| Aspect | napi-rs (Native) | WASM |
|--------|------------------|------|
| Performance | Near-native | ~80-90% native |
| Boundary overhead | Lower | Higher (serialization) |
| Threading | Full (`parking_lot`, `worker_threads`) | Single-threaded |
| mmap support | Yes | No |
| Package size | Larger (per-platform) | Smaller (universal) |
| Distribution | Platform-specific binaries | Universal |
| Debugging | Better stack traces | Harder to debug |

**Recommendation**: Use napi-rs for Node.js (primary), keep WASM for browser and as Node.js fallback.

## Crate Structure

```
interstellar/
├── src/                          # Core library (existing)
├── interstellar-wasm/             # Browser WASM bindings (see wasm.md)
└── interstellar-node/             # Native Node.js bindings
    ├── Cargo.toml
    ├── package.json
    ├── build.rs
    ├── src/
    │   ├── lib.rs                # napi exports, init
    │   ├── graph.rs              # JsGraph wrapper
    │   ├── traversal.rs          # JsTraversal builder
    │   ├── value.rs              # Value <-> JsValue conversion
    │   └── predicate.rs          # P predicate factory
    ├── index.js                  # Auto-generated loader
    ├── index.d.ts                # Auto-generated TypeScript types
    └── npm/                      # Platform packages (auto-generated)
        ├── darwin-arm64/
        ├── darwin-x64/
        ├── linux-x64-gnu/
        ├── linux-arm64-gnu/
        ├── linux-x64-musl/
        ├── win32-x64-msvc/
        └── ...
```

## Configuration Files

### Cargo.toml

```toml
[package]
name = "interstellar-node"
version = "0.1.0"
edition = "2021"
description = "Native Node.js bindings for Interstellar graph database"

[lib]
crate-type = ["cdylib"]

[dependencies]
interstellar = { path = "..", default-features = true }
napi = { version = "3", features = ["async", "serde-json"] }
napi-derive = "3"

[build-dependencies]
napi-build = "2"

[profile.release]
lto = true
strip = "symbols"
opt-level = 3
```

### build.rs

```rust
fn main() {
    napi_build::setup();
}
```

### package.json

```json
{
  "name": "@interstellar/node",
  "version": "0.1.0",
  "description": "High-performance graph traversal library for Node.js",
  "main": "index.js",
  "types": "index.d.ts",
  "files": ["index.js", "index.d.ts"],
  "napi": {
    "binaryName": "interstellar",
    "targets": [
      "x86_64-apple-darwin",
      "aarch64-apple-darwin",
      "x86_64-unknown-linux-gnu",
      "aarch64-unknown-linux-gnu",
      "x86_64-unknown-linux-musl",
      "aarch64-unknown-linux-musl",
      "x86_64-pc-windows-msvc"
    ]
  },
  "optionalDependencies": {
    "@interstellar/node-darwin-arm64": "0.1.0",
    "@interstellar/node-darwin-x64": "0.1.0",
    "@interstellar/node-linux-x64-gnu": "0.1.0",
    "@interstellar/node-linux-arm64-gnu": "0.1.0",
    "@interstellar/node-linux-x64-musl": "0.1.0",
    "@interstellar/node-linux-arm64-musl": "0.1.0",
    "@interstellar/node-win32-x64-msvc": "0.1.0"
  },
  "scripts": {
    "build": "napi build --release",
    "build:debug": "napi build",
    "test": "node test.js"
  },
  "devDependencies": {
    "@napi-rs/cli": "^3.0.0-alpha"
  },
  "engines": {
    "node": ">= 16"
  },
  "license": "MIT",
  "repository": {
    "type": "git",
    "url": "https://github.com/your-org/interstellar"
  },
  "keywords": ["graph", "database", "gremlin", "traversal", "native"]
}
```

## JavaScript API Design

The API mirrors the WASM design for consistency between browser and Node.js.

### Graph Construction

```javascript
const { Graph, P } = require('@interstellar/node');

const graph = new Graph();

// Add vertices - returns numeric ID
const alice = graph.addVertex('person', { name: 'Alice', age: 30 });
const bob = graph.addVertex('person', { name: 'Bob', age: 25 });
const company = graph.addVertex('company', { name: 'Acme Corp' });

// Add edges - returns numeric ID
graph.addEdge(alice, bob, 'knows', { since: 2020 });
graph.addEdge(alice, company, 'worksAt', { role: 'Engineer' });

// Bulk load from JSON
graph.loadJson({
  vertices: [
    { id: 1, label: 'person', properties: { name: 'Charlie' } },
    // ...
  ],
  edges: [
    { id: 1, label: 'knows', from: 1, to: 2, properties: {} },
    // ...
  ]
});

// Export to JSON
const data = graph.toJson();

// Stats
console.log(`Vertices: ${graph.vertexCount}, Edges: ${graph.edgeCount}`);
```

### Traversal API

```javascript
// Get traversal source
const g = graph.traversal();

// Basic traversal - synchronous execution
const friends = g.V()
  .has('person', 'name', 'Alice')
  .out('knows')
  .values('name')
  .toList();
// => ['Bob']

// With predicates
const adults = g.V()
  .hasLabel('person')
  .has('age', P.gte(18))
  .toList();

// Path tracking
const paths = g.V()
  .has('name', 'Alice')
  .out('knows')
  .out('knows')
  .path()
  .toList();

// Select with labels
const connections = g.V()
  .hasLabel('person')
  .as('a')
  .out('knows')
  .as('b')
  .select('a', 'b')
  .toList();
// => [{ a: {id: 1, ...}, b: {id: 2, ...} }, ...]

// Branching
const neighbors = g.V(alice)
  .union(
    g.__.out('knows'),
    g.__.in_('knows')
  )
  .toList();
```

### Predicates

```javascript
const { P } = require('@interstellar/node');

// Comparison
P.eq(value)      // equals
P.neq(value)     // not equals
P.lt(value)      // less than
P.lte(value)     // less than or equal
P.gt(value)      // greater than
P.gte(value)     // greater than or equal

// Range
P.between(min, max)  // min <= x < max
P.within([a, b, c])  // x in [a, b, c]
P.without([a, b])    // x not in [a, b]

// String
P.startingWith('prefix')
P.endingWith('suffix')
P.containing('substring')
P.regex('^A.*')

// Logical
P.and(P.gte(18), P.lt(65))  // both conditions
P.or(P.lt(18), P.gte(65))   // either condition
P.not(P.eq(0))              // negation
```

## Implementation

### Phase 1: Core Types & Graph Operations

**`src/lib.rs`**:

```rust
use napi::bindgen_prelude::*;
use napi_derive::napi;

mod graph;
mod traversal;
mod value;
mod predicate;

pub use graph::JsGraph;
pub use traversal::{JsTraversalSource, JsTraversal};
pub use predicate::P;

/// Initialize the module (optional setup)
#[napi]
pub fn init() {
    // Could set up panic hooks, logging, etc.
}
```

**`src/graph.rs`**:

```rust
use napi::bindgen_prelude::*;
use napi_derive::napi;
use interstellar::{Graph, Value, VertexId, EdgeId};
use interstellar::storage::Graph;
use std::sync::Arc;
use std::collections::HashMap;

use crate::value::{js_to_value, value_to_js, js_to_properties};
use crate::traversal::JsTraversalSource;

#[napi(js_name = "Graph")]
pub struct JsGraph {
    inner: Graph,
}

#[napi]
impl JsGraph {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self {
            inner: Graph::in_memory(),
        }
    }

    /// Add a vertex with label and properties
    #[napi(js_name = "addVertex")]
    pub fn add_vertex(&self, label: String, properties: Object) -> Result<i64> {
        let props = js_to_properties(properties)?;
        
        // Get mutable access
        let storage = self.inner.storage();
        let storage = storage
            .as_any()
            .downcast_ref::<Graph>()
            .ok_or_else(|| Error::new(Status::GenericFailure, "Invalid storage type"))?;
        
        // Note: This requires Graph to have interior mutability
        // or we need a different approach
        let id = storage.add_vertex(&label, props);
        Ok(id.0 as i64)
    }

    /// Add an edge between vertices
    #[napi(js_name = "addEdge")]
    pub fn add_edge(
        &self,
        from: i64,
        to: i64,
        label: String,
        properties: Object,
    ) -> Result<i64> {
        let props = js_to_properties(properties)?;
        let from_id = VertexId(from as u64);
        let to_id = VertexId(to as u64);
        
        let storage = self.inner.storage();
        let storage = storage
            .as_any()
            .downcast_ref::<Graph>()
            .ok_or_else(|| Error::new(Status::GenericFailure, "Invalid storage type"))?;
        
        let id = storage
            .add_edge(from_id, to_id, &label, props)
            .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
        Ok(id.0 as i64)
    }

    /// Get a traversal source
    #[napi]
    pub fn traversal(&self) -> JsTraversalSource {
        JsTraversalSource::new(&self.inner)
    }

    /// Get vertex count
    #[napi(getter, js_name = "vertexCount")]
    pub fn vertex_count(&self) -> i64 {
        self.inner.storage().vertex_count() as i64
    }

    /// Get edge count
    #[napi(getter, js_name = "edgeCount")]
    pub fn edge_count(&self) -> i64 {
        self.inner.storage().edge_count() as i64
    }

    /// Load graph from JSON
    #[napi(js_name = "loadJson")]
    pub fn load_json(&self, data: Object) -> Result<()> {
        // Parse vertices array
        // Parse edges array
        // Add to graph
        todo!("Implement JSON loading")
    }

    /// Export graph to JSON
    #[napi(js_name = "toJson")]
    pub fn to_json(&self, env: Env) -> Result<Object> {
        // Serialize vertices
        // Serialize edges
        // Return { vertices: [...], edges: [...] }
        todo!("Implement JSON export")
    }
}
```

**`src/value.rs`**:

```rust
use napi::bindgen_prelude::*;
use interstellar::Value;
use std::collections::HashMap;

/// Convert JavaScript value to Rust Value
pub fn js_to_value(env: Env, js: JsUnknown) -> Result<Value> {
    let value_type = js.get_type()?;
    
    match value_type {
        ValueType::Null | ValueType::Undefined => Ok(Value::Null),
        ValueType::Boolean => {
            let b: bool = js.coerce_to_bool()?.get_value()?;
            Ok(Value::Bool(b))
        }
        ValueType::Number => {
            let n: f64 = js.coerce_to_number()?.get_double()?;
            // Detect integer vs float
            if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
                Ok(Value::Int(n as i64))
            } else {
                Ok(Value::Float(n))
            }
        }
        ValueType::String => {
            let s: String = js.coerce_to_string()?.into_utf8()?.into_owned()?;
            Ok(Value::String(s))
        }
        ValueType::Object => {
            let obj = js.coerce_to_object()?;
            
            // Check if it's an array
            if obj.is_array()? {
                let len = obj.get_array_length()?;
                let mut items = Vec::with_capacity(len as usize);
                for i in 0..len {
                    let item: JsUnknown = obj.get_element(i)?;
                    items.push(js_to_value(env, item)?);
                }
                Ok(Value::List(items))
            } else {
                // Regular object -> Map
                let keys = obj.get_property_names()?;
                let len = keys.get_array_length()?;
                let mut map = HashMap::with_capacity(len as usize);
                for i in 0..len {
                    let key: JsString = keys.get_element(i)?;
                    let key_str = key.into_utf8()?.into_owned()?;
                    let val: JsUnknown = obj.get_named_property(&key_str)?;
                    map.insert(key_str, js_to_value(env, val)?);
                }
                Ok(Value::Map(map))
            }
        }
        _ => Err(Error::new(
            Status::InvalidArg,
            format!("Unsupported value type: {:?}", value_type),
        )),
    }
}

/// Convert Rust Value to JavaScript value
pub fn value_to_js(env: Env, value: &Value) -> Result<JsUnknown> {
    match value {
        Value::Null => Ok(env.get_null()?.into_unknown()),
        Value::Bool(b) => Ok(env.get_boolean(*b)?.into_unknown()),
        Value::Int(n) => Ok(env.create_int64(*n)?.into_unknown()),
        Value::Float(f) => Ok(env.create_double(*f)?.into_unknown()),
        Value::String(s) => Ok(env.create_string(s)?.into_unknown()),
        Value::List(items) => {
            let mut arr = env.create_array(items.len() as u32)?;
            for (i, item) in items.iter().enumerate() {
                arr.set_element(i as u32, value_to_js(env, item)?)?;
            }
            Ok(arr.into_unknown())
        }
        Value::Map(map) => {
            let mut obj = env.create_object()?;
            for (k, v) in map {
                obj.set_named_property(k, value_to_js(env, v)?)?;
            }
            Ok(obj.into_unknown())
        }
        Value::Vertex(id) => {
            // Return as object with id field, or just the ID?
            // For simplicity, return the raw ID
            Ok(env.create_int64(id.0 as i64)?.into_unknown())
        }
        Value::Edge(id) => {
            Ok(env.create_int64(id.0 as i64)?.into_unknown())
        }
    }
}

/// Convert JavaScript object to properties HashMap
pub fn js_to_properties(obj: Object) -> Result<HashMap<String, Value>> {
    let env = obj.env;
    let keys = obj.get_property_names()?;
    let len = keys.get_array_length()?;
    let mut map = HashMap::with_capacity(len as usize);
    
    for i in 0..len {
        let key: JsString = keys.get_element(i)?;
        let key_str = key.into_utf8()?.into_owned()?;
        let val: JsUnknown = obj.get_named_property(&key_str)?;
        map.insert(key_str, js_to_value(env, val)?);
    }
    
    Ok(map)
}
```

### Phase 2: Traversal API

**`src/traversal.rs`**:

```rust
use napi::bindgen_prelude::*;
use napi_derive::napi;
use interstellar::{Graph, Value, VertexId};
use interstellar::traversal::{Traversal, TraversalSource, __};

use crate::value::value_to_js;
use crate::predicate::JsPredicate;

/// Traversal source - entry point for queries
#[napi(js_name = "TraversalSource")]
pub struct JsTraversalSource {
    // Store a reference or clone of the graph
    // For simplicity, we'll need to handle this carefully
    graph: Graph,
}

impl JsTraversalSource {
    pub fn new(graph: &Graph) -> Self {
        // Clone the graph handle (it's Arc-based internally)
        Self {
            graph: graph.clone(),
        }
    }
}

#[napi]
impl JsTraversalSource {
    /// Start traversal from all vertices or specific IDs
    #[napi(js_name = "V")]
    pub fn v(&self, ids: Option<Vec<i64>>) -> JsTraversal {
        JsTraversal::from_vertices(&self.graph, ids)
    }

    /// Start traversal from all edges or specific IDs
    #[napi(js_name = "E")]
    pub fn e(&self, ids: Option<Vec<i64>>) -> JsTraversal {
        JsTraversal::from_edges(&self.graph, ids)
    }

    /// Anonymous traversal factory
    #[napi(getter)]
    pub fn __(&self) -> JsAnonymousTraversal {
        JsAnonymousTraversal {}
    }
}

/// Traversal builder - accumulates steps and executes
#[napi(js_name = "Traversal")]
pub struct JsTraversal {
    graph: Graph,
    steps: Vec<TraversalStep>,
    source: Option<TraversalSourceType>,
}

#[derive(Clone)]
enum TraversalSourceType {
    AllVertices,
    Vertices(Vec<VertexId>),
    AllEdges,
    Edges(Vec<EdgeId>),
}

#[derive(Clone)]
enum TraversalStep {
    Out(Vec<String>),
    In(Vec<String>),
    Both(Vec<String>),
    OutE(Vec<String>),
    InE(Vec<String>),
    BothE(Vec<String>),
    OutV,
    InV,
    BothV,
    HasLabel(Vec<String>),
    Has(String),
    HasValue(String, Value),
    HasWhere(String, PredicateConfig),
    HasId(Vec<i64>),
    Filter(/* ... */),
    Dedup,
    Limit(usize),
    Skip(usize),
    Range(usize, usize),
    Values(Vec<String>),
    Id,
    Label,
    Path,
    As(String),
    Select(Vec<String>),
    // ... more steps
}

#[napi]
impl JsTraversal {
    fn from_vertices(graph: &Graph, ids: Option<Vec<i64>>) -> Self {
        let source = match ids {
            Some(ids) => TraversalSourceType::Vertices(
                ids.into_iter().map(|id| VertexId(id as u64)).collect()
            ),
            None => TraversalSourceType::AllVertices,
        };
        Self {
            graph: graph.clone(),
            steps: Vec::new(),
            source: Some(source),
        }
    }

    fn from_edges(graph: &Graph, ids: Option<Vec<i64>>) -> Self {
        let source = match ids {
            Some(ids) => TraversalSourceType::Edges(
                ids.into_iter().map(|id| EdgeId(id as u64)).collect()
            ),
            None => TraversalSourceType::AllEdges,
        };
        Self {
            graph: graph.clone(),
            steps: Vec::new(),
            source: Some(source),
        }
    }

    // -------------------------------------------------------------------------
    // Navigation Steps
    // -------------------------------------------------------------------------

    #[napi]
    pub fn out(&mut self, labels: Option<Vec<String>>) -> &Self {
        self.steps.push(TraversalStep::Out(labels.unwrap_or_default()));
        self
    }

    #[napi(js_name = "in")]
    pub fn in_(&mut self, labels: Option<Vec<String>>) -> &Self {
        self.steps.push(TraversalStep::In(labels.unwrap_or_default()));
        self
    }

    #[napi]
    pub fn both(&mut self, labels: Option<Vec<String>>) -> &Self {
        self.steps.push(TraversalStep::Both(labels.unwrap_or_default()));
        self
    }

    #[napi(js_name = "outE")]
    pub fn out_e(&mut self, labels: Option<Vec<String>>) -> &Self {
        self.steps.push(TraversalStep::OutE(labels.unwrap_or_default()));
        self
    }

    #[napi(js_name = "inE")]
    pub fn in_e(&mut self, labels: Option<Vec<String>>) -> &Self {
        self.steps.push(TraversalStep::InE(labels.unwrap_or_default()));
        self
    }

    #[napi(js_name = "bothE")]
    pub fn both_e(&mut self, labels: Option<Vec<String>>) -> &Self {
        self.steps.push(TraversalStep::BothE(labels.unwrap_or_default()));
        self
    }

    #[napi(js_name = "outV")]
    pub fn out_v(&mut self) -> &Self {
        self.steps.push(TraversalStep::OutV);
        self
    }

    #[napi(js_name = "inV")]
    pub fn in_v(&mut self) -> &Self {
        self.steps.push(TraversalStep::InV);
        self
    }

    #[napi(js_name = "bothV")]
    pub fn both_v(&mut self) -> &Self {
        self.steps.push(TraversalStep::BothV);
        self
    }

    // -------------------------------------------------------------------------
    // Filter Steps
    // -------------------------------------------------------------------------

    #[napi(js_name = "hasLabel")]
    pub fn has_label(&mut self, labels: Either<String, Vec<String>>) -> &Self {
        let labels = match labels {
            Either::A(s) => vec![s],
            Either::B(v) => v,
        };
        self.steps.push(TraversalStep::HasLabel(labels));
        self
    }

    #[napi]
    pub fn has(&mut self, key: String, value: Option<JsUnknown>) -> Result<&Self> {
        match value {
            None => {
                self.steps.push(TraversalStep::Has(key));
            }
            Some(v) => {
                let val = js_to_value(v)?;
                self.steps.push(TraversalStep::HasValue(key, val));
            }
        }
        Ok(self)
    }

    #[napi(js_name = "hasId")]
    pub fn has_id(&mut self, ids: Either<i64, Vec<i64>>) -> &Self {
        let ids = match ids {
            Either::A(id) => vec![id],
            Either::B(ids) => ids,
        };
        self.steps.push(TraversalStep::HasId(ids));
        self
    }

    #[napi]
    pub fn dedup(&mut self) -> &Self {
        self.steps.push(TraversalStep::Dedup);
        self
    }

    #[napi]
    pub fn limit(&mut self, count: u32) -> &Self {
        self.steps.push(TraversalStep::Limit(count as usize));
        self
    }

    #[napi]
    pub fn skip(&mut self, count: u32) -> &Self {
        self.steps.push(TraversalStep::Skip(count as usize));
        self
    }

    #[napi]
    pub fn range(&mut self, start: u32, end: u32) -> &Self {
        self.steps.push(TraversalStep::Range(start as usize, end as usize));
        self
    }

    // -------------------------------------------------------------------------
    // Transform Steps
    // -------------------------------------------------------------------------

    #[napi]
    pub fn values(&mut self, keys: Either<String, Vec<String>>) -> &Self {
        let keys = match keys {
            Either::A(s) => vec![s],
            Either::B(v) => v,
        };
        self.steps.push(TraversalStep::Values(keys));
        self
    }

    #[napi]
    pub fn id(&mut self) -> &Self {
        self.steps.push(TraversalStep::Id);
        self
    }

    #[napi]
    pub fn label(&mut self) -> &Self {
        self.steps.push(TraversalStep::Label);
        self
    }

    #[napi]
    pub fn path(&mut self) -> &Self {
        self.steps.push(TraversalStep::Path);
        self
    }

    #[napi(js_name = "as")]
    pub fn as_(&mut self, label: String) -> &Self {
        self.steps.push(TraversalStep::As(label));
        self
    }

    #[napi]
    pub fn select(&mut self, labels: Either<String, Vec<String>>) -> &Self {
        let labels = match labels {
            Either::A(s) => vec![s],
            Either::B(v) => v,
        };
        self.steps.push(TraversalStep::Select(labels));
        self
    }

    // -------------------------------------------------------------------------
    // Terminal Steps
    // -------------------------------------------------------------------------

    /// Execute traversal and return all results as a list
    #[napi(js_name = "toList")]
    pub fn to_list(&self, env: Env) -> Result<Vec<JsUnknown>> {
        let results = self.execute()?;
        results
            .into_iter()
            .map(|v| value_to_js(env, &v))
            .collect()
    }

    /// Execute traversal and return first result
    #[napi]
    pub fn next(&self, env: Env) -> Result<Option<JsUnknown>> {
        let mut results = self.execute()?;
        match results.pop() {
            Some(v) => Ok(Some(value_to_js(env, &v)?)),
            None => Ok(None),
        }
    }

    /// Execute traversal and return count
    #[napi]
    pub fn count(&self) -> Result<i64> {
        let results = self.execute()?;
        Ok(results.len() as i64)
    }

    /// Check if traversal has any results
    #[napi(js_name = "hasNext")]
    pub fn has_next(&self) -> Result<bool> {
        let results = self.execute()?;
        Ok(!results.is_empty())
    }

    // -------------------------------------------------------------------------
    // Internal Execution
    // -------------------------------------------------------------------------

    fn execute(&self) -> Result<Vec<Value>> {
        // Build the actual Rust traversal from recorded steps
        // Execute against the graph
        // Return results
        
        let snapshot = self.graph.snapshot();
        let g = snapshot.traversal();
        
        // Start with source
        let traversal = match &self.source {
            Some(TraversalSourceType::AllVertices) => g.v(),
            Some(TraversalSourceType::Vertices(ids)) => g.v_from_ids(ids.clone()),
            Some(TraversalSourceType::AllEdges) => g.e(),
            Some(TraversalSourceType::Edges(ids)) => g.e_from_ids(ids.clone()),
            None => return Err(Error::new(Status::GenericFailure, "No traversal source")),
        };
        
        // Apply steps (this is simplified - actual implementation needs type handling)
        // ...
        
        // Execute and collect
        let results = traversal.to_list();
        Ok(results)
    }
}

/// Anonymous traversal factory for use in sub-traversals
#[napi(js_name = "AnonymousTraversal")]
pub struct JsAnonymousTraversal {}

#[napi]
impl JsAnonymousTraversal {
    #[napi]
    pub fn out(&self, labels: Option<Vec<String>>) -> JsTraversal {
        // Return anonymous traversal fragment
        todo!()
    }

    // ... other methods
}
```

### Phase 3: Predicates

**`src/predicate.rs`**:

```rust
use napi::bindgen_prelude::*;
use napi_derive::napi;
use interstellar::Value;

use crate::value::js_to_value;

/// Predicate configuration for deferred evaluation
#[derive(Clone)]
pub enum PredicateConfig {
    Eq(Value),
    Neq(Value),
    Lt(Value),
    Lte(Value),
    Gt(Value),
    Gte(Value),
    Between(Value, Value),
    Within(Vec<Value>),
    Without(Vec<Value>),
    StartingWith(String),
    EndingWith(String),
    Containing(String),
    Regex(String),
    And(Box<PredicateConfig>, Box<PredicateConfig>),
    Or(Box<PredicateConfig>, Box<PredicateConfig>),
    Not(Box<PredicateConfig>),
}

/// Predicate factory - exposed to JavaScript as `P`
#[napi(js_name = "P")]
pub struct P;

#[napi]
impl P {
    #[napi]
    pub fn eq(value: JsUnknown) -> Result<JsPredicate> {
        let val = js_to_value(value)?;
        Ok(JsPredicate {
            config: PredicateConfig::Eq(val),
        })
    }

    #[napi]
    pub fn neq(value: JsUnknown) -> Result<JsPredicate> {
        let val = js_to_value(value)?;
        Ok(JsPredicate {
            config: PredicateConfig::Neq(val),
        })
    }

    #[napi]
    pub fn lt(value: JsUnknown) -> Result<JsPredicate> {
        let val = js_to_value(value)?;
        Ok(JsPredicate {
            config: PredicateConfig::Lt(val),
        })
    }

    #[napi]
    pub fn lte(value: JsUnknown) -> Result<JsPredicate> {
        let val = js_to_value(value)?;
        Ok(JsPredicate {
            config: PredicateConfig::Lte(val),
        })
    }

    #[napi]
    pub fn gt(value: JsUnknown) -> Result<JsPredicate> {
        let val = js_to_value(value)?;
        Ok(JsPredicate {
            config: PredicateConfig::Gt(val),
        })
    }

    #[napi]
    pub fn gte(value: JsUnknown) -> Result<JsPredicate> {
        let val = js_to_value(value)?;
        Ok(JsPredicate {
            config: PredicateConfig::Gte(val),
        })
    }

    #[napi]
    pub fn between(min: JsUnknown, max: JsUnknown) -> Result<JsPredicate> {
        let min_val = js_to_value(min)?;
        let max_val = js_to_value(max)?;
        Ok(JsPredicate {
            config: PredicateConfig::Between(min_val, max_val),
        })
    }

    #[napi]
    pub fn within(values: Vec<JsUnknown>) -> Result<JsPredicate> {
        let vals: Result<Vec<Value>> = values.into_iter().map(js_to_value).collect();
        Ok(JsPredicate {
            config: PredicateConfig::Within(vals?),
        })
    }

    #[napi]
    pub fn without(values: Vec<JsUnknown>) -> Result<JsPredicate> {
        let vals: Result<Vec<Value>> = values.into_iter().map(js_to_value).collect();
        Ok(JsPredicate {
            config: PredicateConfig::Without(vals?),
        })
    }

    #[napi(js_name = "startingWith")]
    pub fn starting_with(prefix: String) -> JsPredicate {
        JsPredicate {
            config: PredicateConfig::StartingWith(prefix),
        }
    }

    #[napi(js_name = "endingWith")]
    pub fn ending_with(suffix: String) -> JsPredicate {
        JsPredicate {
            config: PredicateConfig::EndingWith(suffix),
        }
    }

    #[napi]
    pub fn containing(substring: String) -> JsPredicate {
        JsPredicate {
            config: PredicateConfig::Containing(substring),
        }
    }

    #[napi]
    pub fn regex(pattern: String) -> JsPredicate {
        JsPredicate {
            config: PredicateConfig::Regex(pattern),
        }
    }

    #[napi]
    pub fn and(a: &JsPredicate, b: &JsPredicate) -> JsPredicate {
        JsPredicate {
            config: PredicateConfig::And(
                Box::new(a.config.clone()),
                Box::new(b.config.clone()),
            ),
        }
    }

    #[napi]
    pub fn or(a: &JsPredicate, b: &JsPredicate) -> JsPredicate {
        JsPredicate {
            config: PredicateConfig::Or(
                Box::new(a.config.clone()),
                Box::new(b.config.clone()),
            ),
        }
    }

    #[napi]
    pub fn not(p: &JsPredicate) -> JsPredicate {
        JsPredicate {
            config: PredicateConfig::Not(Box::new(p.config.clone())),
        }
    }
}

/// Predicate instance - passed to has() steps
#[napi]
pub struct JsPredicate {
    pub(crate) config: PredicateConfig,
}
```

## Build & Distribution

### GitHub Actions Workflow

```yaml
name: Build and Publish

on:
  push:
    tags:
      - 'v*'
  workflow_dispatch:

jobs:
  build:
    strategy:
      matrix:
        include:
          - os: macos-latest
            target: x86_64-apple-darwin
          - os: macos-latest
            target: aarch64-apple-darwin
          - os: ubuntu-latest
            target: x86_64-unknown-linux-gnu
          - os: ubuntu-latest
            target: aarch64-unknown-linux-gnu
          - os: ubuntu-latest
            target: x86_64-unknown-linux-musl
          - os: windows-latest
            target: x86_64-pc-windows-msvc

    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: 20
          
      - name: Install Rust
        uses: dtolnay/rust-toolchain@stable
        with:
          targets: ${{ matrix.target }}
          
      - name: Install dependencies
        run: npm install
        working-directory: interstellar-node
        
      - name: Build
        run: npm run build -- --target ${{ matrix.target }}
        working-directory: interstellar-node
        
      - name: Upload artifact
        uses: actions/upload-artifact@v4
        with:
          name: bindings-${{ matrix.target }}
          path: interstellar-node/*.node

  publish:
    needs: build
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: 20
          registry-url: 'https://registry.npmjs.org'
          
      - name: Download all artifacts
        uses: actions/download-artifact@v4
        with:
          path: artifacts
          
      - name: Move artifacts
        run: napi artifacts
        working-directory: interstellar-node
        
      - name: Publish
        run: |
          npm publish --access public
        working-directory: interstellar-node
        env:
          NODE_AUTH_TOKEN: ${{ secrets.NPM_TOKEN }}
```

### CLI Commands

```bash
# Development
cd interstellar-node
npm install
npm run build:debug    # Fast debug build
npm run build          # Release build
npm test               # Run tests

# Cross-compilation (requires additional setup)
npm run build -- --target aarch64-apple-darwin
npm run build -- --target x86_64-unknown-linux-musl

# Publishing
napi artifacts         # Collect built binaries
napi prepublish        # Prepare npm packages
npm publish            # Publish to npm
```

## Testing

### Unit Tests (Rust)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_conversion() {
        // Test js_to_value and value_to_js roundtrips
    }

    #[test]
    fn test_graph_operations() {
        // Test add_vertex, add_edge
    }
}
```

### Integration Tests (JavaScript)

```javascript
// test.js
const { Graph, P } = require('./index.js');
const assert = require('assert');

// Test graph construction
const graph = new Graph();
const alice = graph.addVertex('person', { name: 'Alice', age: 30 });
const bob = graph.addVertex('person', { name: 'Bob', age: 25 });
graph.addEdge(alice, bob, 'knows', { since: 2020 });

assert.strictEqual(graph.vertexCount, 2);
assert.strictEqual(graph.edgeCount, 1);

// Test traversal
const g = graph.traversal();
const names = g.V().hasLabel('person').values('name').toList();
assert.deepStrictEqual(names.sort(), ['Alice', 'Bob']);

// Test predicates
const adults = g.V().has('age', P.gte(18)).values('name').toList();
assert.deepStrictEqual(adults.sort(), ['Alice', 'Bob']);

console.log('All tests passed!');
```

## Future Enhancements

1. **Async API**: Add async variants for long-running traversals
2. **Streaming**: Return async iterators for large result sets
3. **mmap Support**: Expose persistent storage via optional config
4. **Worker Threads**: Parallel traversal execution
5. **GraphQL Integration**: Direct GraphQL resolver support
6. **Visualization**: Integration with graph visualization libraries

## References

- [napi-rs Documentation](https://napi.rs/)
- [napi-rs GitHub](https://github.com/napi-rs/napi-rs)
- [Node.js N-API](https://nodejs.org/api/n-api.html)
- [Apache TinkerPop Gremlin](https://tinkerpop.apache.org/gremlin.html)
