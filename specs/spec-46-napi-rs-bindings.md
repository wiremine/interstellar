# Spec 46: Native Node.js Bindings via NAPI-RS

This specification defines the native Node.js bindings for Interstellar using [napi-rs](https://napi.rs/), providing high-performance server-side JavaScript integration with full threading support.

**Prerequisite**: Core library implementation (Phases 1-6) must be complete.

---

## 1. Overview

### 1.1 Motivation

While Spec 45 provides WASM bindings for browser and universal JavaScript environments, native Node.js bindings via napi-rs offer significant advantages for server-side applications:

| Aspect | napi-rs (Native) | WASM |
|--------|------------------|------|
| Performance | Near-native | ~80-90% native |
| Boundary overhead | Lower (direct FFI) | Higher (serialization) |
| Threading | Full (`parking_lot`, `worker_threads`) | Single-threaded |
| mmap support | Yes | No |
| Async I/O | Native Node.js integration | Limited |
| Package size | Larger (per-platform) | Smaller (universal) |
| Distribution | Platform-specific binaries | Universal |
| Debugging | Better stack traces | Harder to debug |

**Primary use cases for napi-rs bindings:**
- High-throughput graph analytics servers
- Real-time recommendation engines
- Knowledge graph applications
- ETL pipelines with large datasets
- Applications requiring persistent storage (mmap)

### 1.2 Scope

This specification covers:

- Separate `interstellar-node` crate structure
- napi-rs facade types: `Graph`, `Traversal`, `Vertex`, `Edge`
- Full traversal API with method chaining (matching WASM API)
- Predicate system (`P.eq()`, `P.gt()`, etc.)
- Anonymous traversal factory (`__`)
- Async API variants for long-running operations
- Cross-platform build and distribution via GitHub Actions
- TypeScript type definitions

### 1.3 Non-Goals

| Non-Goal | Rationale |
|----------|-----------|
| Electron main process bindings | Use WASM for renderer, native for main - future spec |
| Native ES modules (`.mjs`) | CommonJS primary, ESM wrapper possible later |
| Deno/Bun support | Different FFI mechanisms - separate specs |
| GraphQL server integration | Application-level concern |
| Clustering/distributed mode | Separate architectural concern |

### 1.4 Design Principles

| Principle | Description |
|-----------|-------------|
| **API Parity** | Same API surface as WASM bindings for code portability |
| **Native Performance** | Leverage Rust's zero-cost abstractions fully |
| **Thread Safety** | Safe concurrent access from multiple JS contexts |
| **Ergonomic TypeScript** | First-class TypeScript support with full type inference |
| **Minimal Dependencies** | Only napi-rs core, no heavy runtime dependencies |

---

## 2. Architecture

### 2.1 Crate Structure

```
interstellar/
├── Cargo.toml                    # Workspace root
├── src/                          # Core library
└── interstellar-node/            # Native Node.js bindings
    ├── Cargo.toml
    ├── build.rs                  # napi-build setup
    ├── package.json              # npm package config
    ├── src/
    │   ├── lib.rs                # Module exports, init
    │   ├── graph.rs              # JsGraph wrapper
    │   ├── traversal.rs          # JsTraversal builder
    │   ├── value.rs              # Value <-> JsValue conversion
    │   ├── predicate.rs          # P predicate factory
    │   ├── anonymous.rs          # __ anonymous traversal factory
    │   ├── builders.rs           # OrderBuilder, GroupBuilder, etc.
    │   ├── error.rs              # Error conversion
    │   └── async_ops.rs          # Async operation wrappers
    ├── index.js                  # Auto-generated native loader
    ├── index.d.ts                # Auto-generated TypeScript types
    ├── __test__/                 # JavaScript tests
    │   ├── graph.spec.ts
    │   ├── traversal.spec.ts
    │   └── predicates.spec.ts
    └── npm/                      # Platform packages (generated)
        ├── darwin-arm64/
        ├── darwin-x64/
        ├── linux-x64-gnu/
        ├── linux-arm64-gnu/
        ├── linux-x64-musl/
        ├── linux-arm64-musl/
        └── win32-x64-msvc/
```

### 2.2 Dependency Graph

```
┌─────────────────────────────────────────────────────────────────┐
│                         Node.js                                  │
│   const { Graph, P, __ } = require('@interstellar/node');       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    interstellar-node                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │ graph.rs │  │traversal │  │predicate │  │ value.rs │        │
│  │          │  │   .rs    │  │   .rs    │  │          │        │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘        │
│       │             │             │             │                │
│       └─────────────┴─────────────┴─────────────┘                │
│                              │                                   │
│                     napi-rs bindings                             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      interstellar (core)                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │  Graph   │  │ Traversal│  │ Predicate│  │  Value   │        │
│  │ Snapshot │  │  Steps   │  │  System  │  │  Types   │        │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘        │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 Threading Model

napi-rs provides safe concurrent access patterns:

```
┌─────────────────────────────────────────────────────────────────┐
│                     Node.js Main Thread                          │
│                                                                  │
│   const graph = new Graph();  // JsGraph created                │
│   graph.addVertex(...);       // Sync mutation                  │
│   graph.V().toList();         // Sync traversal                 │
│   await graph.loadAsync();    // Async on thread pool           │
└─────────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          │                   │                   │
          ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  Worker Thread  │ │  Worker Thread  │ │  Worker Thread  │
│    (libuv)      │ │    (libuv)      │ │    (libuv)      │
│                 │ │                 │ │                 │
│  Async ops:     │ │  Async ops:     │ │  Async ops:     │
│  - File I/O     │ │  - Large loads  │ │  - Batch ops    │
│  - mmap init    │ │  - GraphSON     │ │  - Algorithms   │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

**Key threading considerations:**

1. `JsGraph` wraps `Arc<Graph>` for safe sharing
2. Sync methods acquire appropriate locks (read for queries, write for mutations)
3. Async methods spawn work on libuv thread pool via `napi::Task`
4. `GraphSnapshot` provides lock-free reads for traversals

---

## 3. Configuration Files

### 3.1 Cargo.toml

```toml
[package]
name = "interstellar-node"
version = "0.1.0"
edition = "2021"
description = "Native Node.js bindings for Interstellar graph database"
license = "MIT OR Apache-2.0"
repository = "https://github.com/anthropic/interstellar"
readme = "README.md"
keywords = ["graph", "database", "gremlin", "nodejs", "napi"]
categories = ["database", "api-bindings"]

[lib]
crate-type = ["cdylib"]

[dependencies]
interstellar = { path = "..", features = ["graphson", "gql"] }
napi = { version = "2", default-features = false, features = [
    "napi8",
    "async",
    "serde-json",
    "error_anyhow"
] }
napi-derive = "2"

[build-dependencies]
napi-build = "2"

[features]
default = []
mmap = ["interstellar/mmap"]
full-text = ["interstellar/full-text"]

[profile.release]
lto = true
strip = "symbols"
opt-level = 3
codegen-units = 1
```

### 3.2 build.rs

```rust
extern crate napi_build;

fn main() {
    napi_build::setup();
}
```

### 3.3 package.json

```json
{
  "name": "@interstellar/node",
  "version": "0.1.0",
  "description": "High-performance graph database for Node.js",
  "main": "index.js",
  "types": "index.d.ts",
  "author": "Interstellar Contributors",
  "license": "MIT OR Apache-2.0",
  "repository": {
    "type": "git",
    "url": "https://github.com/anthropic/interstellar"
  },
  "keywords": [
    "graph",
    "database",
    "gremlin",
    "traversal",
    "native",
    "performance"
  ],
  "files": [
    "index.js",
    "index.d.ts"
  ],
  "napi": {
    "name": "interstellar",
    "triples": {
      "defaults": true,
      "additional": [
        "x86_64-unknown-linux-musl",
        "aarch64-unknown-linux-gnu",
        "aarch64-unknown-linux-musl",
        "aarch64-apple-darwin",
        "armv7-unknown-linux-gnueabihf"
      ]
    }
  },
  "engines": {
    "node": ">= 16"
  },
  "scripts": {
    "artifacts": "napi artifacts",
    "build": "napi build --platform --release",
    "build:debug": "napi build --platform",
    "prepublishOnly": "napi prepublish -t npm",
    "test": "vitest run",
    "test:watch": "vitest",
    "universal": "napi universal",
    "version": "napi version"
  },
  "devDependencies": {
    "@napi-rs/cli": "^2.18.0",
    "@types/node": "^20.10.0",
    "typescript": "^5.3.0",
    "vitest": "^1.0.0"
  },
  "optionalDependencies": {
    "@interstellar/node-win32-x64-msvc": "0.1.0",
    "@interstellar/node-darwin-x64": "0.1.0",
    "@interstellar/node-darwin-arm64": "0.1.0",
    "@interstellar/node-linux-x64-gnu": "0.1.0",
    "@interstellar/node-linux-x64-musl": "0.1.0",
    "@interstellar/node-linux-arm64-gnu": "0.1.0",
    "@interstellar/node-linux-arm64-musl": "0.1.0",
    "@interstellar/node-linux-arm-gnueabihf": "0.1.0"
  }
}
```

---

## 4. Core Type Implementations

### 4.1 Module Entry Point (lib.rs)

```rust
#![deny(clippy::all)]

use napi_derive::napi;

mod anonymous;
mod builders;
mod error;
mod graph;
mod predicate;
mod traversal;
mod value;

pub use anonymous::AnonymousTraversal;
pub use builders::{GroupBuilder, GroupCountBuilder, OrderBuilder, ProjectBuilder, RepeatBuilder};
pub use graph::JsGraph;
pub use predicate::P;
pub use traversal::JsTraversal;

/// Initialize the module (called automatically by Node.js)
#[napi]
pub fn init() -> napi::Result<()> {
    // Optional: set up panic hooks, logging, etc.
    Ok(())
}
```

### 4.2 Value Conversion (value.rs)

```rust
use napi::bindgen_prelude::*;
use napi::{Env, JsUnknown, ValueType};
use interstellar::Value;
use std::collections::HashMap;

/// Convert a JavaScript value to a Rust Value
pub fn js_to_value(env: Env, js: JsUnknown) -> Result<Value> {
    match js.get_type()? {
        ValueType::Null | ValueType::Undefined => Ok(Value::Null),
        
        ValueType::Boolean => {
            let b = js.coerce_to_bool()?.get_value()?;
            Ok(Value::Bool(b))
        }
        
        ValueType::Number => {
            let n = js.coerce_to_number()?.get_double()?;
            // Detect integer vs float
            if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
                Ok(Value::Int(n as i64))
            } else {
                Ok(Value::Float(n))
            }
        }
        
        ValueType::BigInt => {
            let (value, _lossless) = js.coerce_to_number()?.get_int64()?;
            Ok(Value::Int(value))
        }
        
        ValueType::String => {
            let s = js.coerce_to_string()?.into_utf8()?.into_owned()?;
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
                    let key: String = keys.get_element::<String>(i)?;
                    let val: JsUnknown = obj.get_named_property(&key)?;
                    map.insert(key, js_to_value(env, val)?);
                }
                Ok(Value::Map(map))
            }
        }
        
        _ => Err(Error::new(
            Status::InvalidArg,
            "Unsupported value type",
        )),
    }
}

/// Convert a Rust Value to a JavaScript value
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
            // Return as bigint ID
            Ok(env.create_int64(id.0 as i64)?.into_unknown())
        }
        Value::Edge(id) => {
            Ok(env.create_int64(id.0 as i64)?.into_unknown())
        }
    }
}

/// Convert JavaScript object to properties HashMap
pub fn js_to_properties(env: Env, obj: Object) -> Result<HashMap<String, Value>> {
    let keys = obj.get_property_names()?;
    let len = keys.get_array_length()?;
    let mut map = HashMap::with_capacity(len as usize);
    
    for i in 0..len {
        let key: String = keys.get_element(i)?;
        let val: JsUnknown = obj.get_named_property(&key)?;
        map.insert(key, js_to_value(env, val)?);
    }
    
    Ok(map)
}

/// Convert Rust properties to JavaScript object
pub fn properties_to_js(env: Env, props: &HashMap<String, Value>) -> Result<Object> {
    let mut obj = env.create_object()?;
    for (k, v) in props {
        obj.set_named_property(k, value_to_js(env, v)?)?;
    }
    Ok(obj)
}
```

### 4.3 Error Handling (error.rs)

```rust
use napi::bindgen_prelude::*;
use interstellar::{StorageError, TraversalError, MutationError};

/// Convert Interstellar errors to napi errors
pub trait IntoNapiError {
    fn into_napi_error(self) -> Error;
}

impl IntoNapiError for StorageError {
    fn into_napi_error(self) -> Error {
        match self {
            StorageError::VertexNotFound(id) => {
                Error::new(Status::GenericFailure, format!("Vertex not found: {:?}", id))
            }
            StorageError::EdgeNotFound(id) => {
                Error::new(Status::GenericFailure, format!("Edge not found: {:?}", id))
            }
            StorageError::Io(e) => {
                Error::new(Status::GenericFailure, format!("I/O error: {}", e))
            }
            StorageError::InvalidFormat => {
                Error::new(Status::GenericFailure, "Invalid data format")
            }
            StorageError::CorruptedData => {
                Error::new(Status::GenericFailure, "Corrupted data detected")
            }
            StorageError::OutOfSpace => {
                Error::new(Status::GenericFailure, "Storage out of space")
            }
            StorageError::IndexError(msg) => {
                Error::new(Status::GenericFailure, format!("Index error: {}", msg))
            }
            _ => Error::new(Status::GenericFailure, self.to_string()),
        }
    }
}

impl IntoNapiError for TraversalError {
    fn into_napi_error(self) -> Error {
        match self {
            TraversalError::NotOne(count) => {
                Error::new(
                    Status::GenericFailure,
                    format!("Expected exactly one result, got {}", count),
                )
            }
            TraversalError::Storage(e) => e.into_napi_error(),
            TraversalError::Mutation(e) => e.into_napi_error(),
        }
    }
}

impl IntoNapiError for MutationError {
    fn into_napi_error(self) -> Error {
        Error::new(Status::GenericFailure, self.to_string())
    }
}

/// Extension trait for Result types
pub trait ResultExt<T> {
    fn to_napi(self) -> Result<T>;
}

impl<T, E: IntoNapiError> ResultExt<T> for std::result::Result<T, E> {
    fn to_napi(self) -> Result<T> {
        self.map_err(|e| e.into_napi_error())
    }
}
```

---

## 5. Graph Implementation (graph.rs)

```rust
use napi::bindgen_prelude::*;
use napi_derive::napi;
use interstellar::{Graph, Value, VertexId, EdgeId};
use std::sync::Arc;
use std::collections::HashMap;

use crate::error::ResultExt;
use crate::traversal::JsTraversal;
use crate::value::{js_to_value, js_to_properties, value_to_js, properties_to_js};

/// A high-performance in-memory graph database.
/// 
/// @example
/// ```javascript
/// const { Graph } = require('@interstellar/node');
/// 
/// const graph = new Graph();
/// const alice = graph.addVertex('person', { name: 'Alice', age: 30 });
/// const bob = graph.addVertex('person', { name: 'Bob', age: 25 });
/// graph.addEdge(alice, bob, 'knows', { since: 2020 });
/// ```
#[napi(js_name = "Graph")]
pub struct JsGraph {
    inner: Arc<Graph>,
}

#[napi]
impl JsGraph {
    /// Create a new empty in-memory graph.
    #[napi(constructor)]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Graph::new()),
        }
    }

    // -------------------------------------------------------------------------
    // Vertex Operations
    // -------------------------------------------------------------------------

    /// Add a vertex with a label and optional properties.
    /// 
    /// @param label - The vertex label (e.g., 'person', 'product')
    /// @param properties - Optional key-value properties
    /// @returns The new vertex's ID as a number
    #[napi(js_name = "addVertex")]
    pub fn add_vertex(
        &self,
        env: Env,
        label: String,
        properties: Option<Object>,
    ) -> Result<i64> {
        let props = match properties {
            Some(obj) => js_to_properties(env, obj)?,
            None => HashMap::new(),
        };
        
        let id = self.inner.add_vertex(&label, props);
        Ok(id.0 as i64)
    }

    /// Get a vertex by ID.
    /// 
    /// @param id - The vertex ID
    /// @returns The vertex object, or undefined if not found
    #[napi(js_name = "getVertex")]
    pub fn get_vertex(&self, env: Env, id: i64) -> Result<Option<Object>> {
        let snapshot = self.inner.snapshot();
        match snapshot.get_vertex(VertexId(id as u64)) {
            Some(vertex) => {
                let mut obj = env.create_object()?;
                obj.set_named_property("id", env.create_int64(vertex.id.0 as i64)?)?;
                obj.set_named_property("label", env.create_string(&vertex.label)?)?;
                obj.set_named_property("properties", properties_to_js(env, &vertex.properties)?)?;
                Ok(Some(obj))
            }
            None => Ok(None),
        }
    }

    /// Remove a vertex and all its incident edges.
    /// 
    /// @param id - The vertex ID to remove
    /// @returns true if removed, false if not found
    #[napi(js_name = "removeVertex")]
    pub fn remove_vertex(&self, id: i64) -> Result<bool> {
        match self.inner.remove_vertex(VertexId(id as u64)) {
            Ok(()) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    /// Set a property on a vertex.
    /// 
    /// @param id - The vertex ID
    /// @param key - Property name
    /// @param value - Property value
    #[napi(js_name = "setVertexProperty")]
    pub fn set_vertex_property(
        &self,
        env: Env,
        id: i64,
        key: String,
        value: JsUnknown,
    ) -> Result<()> {
        let val = js_to_value(env, value)?;
        self.inner
            .set_vertex_property(VertexId(id as u64), &key, val)
            .to_napi()
    }

    // -------------------------------------------------------------------------
    // Edge Operations
    // -------------------------------------------------------------------------

    /// Add an edge between two vertices.
    /// 
    /// @param from - Source vertex ID
    /// @param to - Target vertex ID
    /// @param label - The edge label (e.g., 'knows', 'purchased')
    /// @param properties - Optional key-value properties
    /// @returns The new edge's ID
    #[napi(js_name = "addEdge")]
    pub fn add_edge(
        &self,
        env: Env,
        from: i64,
        to: i64,
        label: String,
        properties: Option<Object>,
    ) -> Result<i64> {
        let props = match properties {
            Some(obj) => js_to_properties(env, obj)?,
            None => HashMap::new(),
        };
        
        let id = self.inner
            .add_edge(VertexId(from as u64), VertexId(to as u64), &label, props)
            .to_napi()?;
        Ok(id.0 as i64)
    }

    /// Get an edge by ID.
    /// 
    /// @param id - The edge ID
    /// @returns The edge object, or undefined if not found
    #[napi(js_name = "getEdge")]
    pub fn get_edge(&self, env: Env, id: i64) -> Result<Option<Object>> {
        let snapshot = self.inner.snapshot();
        match snapshot.get_edge(EdgeId(id as u64)) {
            Some(edge) => {
                let mut obj = env.create_object()?;
                obj.set_named_property("id", env.create_int64(edge.id.0 as i64)?)?;
                obj.set_named_property("label", env.create_string(&edge.label)?)?;
                obj.set_named_property("from", env.create_int64(edge.out_v.0 as i64)?)?;
                obj.set_named_property("to", env.create_int64(edge.in_v.0 as i64)?)?;
                obj.set_named_property("properties", properties_to_js(env, &edge.properties)?)?;
                Ok(Some(obj))
            }
            None => Ok(None),
        }
    }

    /// Remove an edge.
    /// 
    /// @param id - The edge ID to remove
    /// @returns true if removed, false if not found
    #[napi(js_name = "removeEdge")]
    pub fn remove_edge(&self, id: i64) -> Result<bool> {
        match self.inner.remove_edge(EdgeId(id as u64)) {
            Ok(()) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    /// Set a property on an edge.
    /// 
    /// @param id - The edge ID
    /// @param key - Property name
    /// @param value - Property value
    #[napi(js_name = "setEdgeProperty")]
    pub fn set_edge_property(
        &self,
        env: Env,
        id: i64,
        key: String,
        value: JsUnknown,
    ) -> Result<()> {
        let val = js_to_value(env, value)?;
        self.inner
            .set_edge_property(EdgeId(id as u64), &key, val)
            .to_napi()
    }

    // -------------------------------------------------------------------------
    // Graph Statistics
    // -------------------------------------------------------------------------

    /// Get the total number of vertices.
    #[napi(getter, js_name = "vertexCount")]
    pub fn vertex_count(&self) -> i64 {
        self.inner.vertex_count() as i64
    }

    /// Get the total number of edges.
    #[napi(getter, js_name = "edgeCount")]
    pub fn edge_count(&self) -> i64 {
        self.inner.edge_count() as i64
    }

    /// Get the current version/transaction ID.
    #[napi(getter)]
    pub fn version(&self) -> i64 {
        self.inner.version() as i64
    }

    // -------------------------------------------------------------------------
    // Traversal Entry Points
    // -------------------------------------------------------------------------

    /// Start a traversal from all vertices.
    /// 
    /// @returns A new traversal starting from all vertices
    /// 
    /// @example
    /// ```javascript
    /// const names = graph.V()
    ///     .hasLabel('person')
    ///     .values('name')
    ///     .toList();
    /// ```
    #[napi(js_name = "V")]
    pub fn v(&self) -> JsTraversal {
        JsTraversal::from_all_vertices(Arc::clone(&self.inner))
    }

    /// Start a traversal from specific vertex IDs.
    /// 
    /// @param ids - Vertex IDs to start from
    #[napi(js_name = "V_")]
    pub fn v_ids(&self, ids: Vec<i64>) -> JsTraversal {
        let vertex_ids: Vec<VertexId> = ids.into_iter().map(|id| VertexId(id as u64)).collect();
        JsTraversal::from_vertex_ids(Arc::clone(&self.inner), vertex_ids)
    }

    /// Start a traversal from all edges.
    #[napi(js_name = "E")]
    pub fn e(&self) -> JsTraversal {
        JsTraversal::from_all_edges(Arc::clone(&self.inner))
    }

    /// Start a traversal from specific edge IDs.
    /// 
    /// @param ids - Edge IDs to start from
    #[napi(js_name = "E_")]
    pub fn e_ids(&self, ids: Vec<i64>) -> JsTraversal {
        let edge_ids: Vec<EdgeId> = ids.into_iter().map(|id| EdgeId(id as u64)).collect();
        JsTraversal::from_edge_ids(Arc::clone(&self.inner), edge_ids)
    }

    // -------------------------------------------------------------------------
    // Serialization
    // -------------------------------------------------------------------------

    /// Export the graph to a GraphSON JSON string.
    /// 
    /// @returns GraphSON 3.0 formatted JSON string
    #[napi(js_name = "toGraphSON")]
    pub fn to_graphson(&self) -> Result<String> {
        let snapshot = self.inner.snapshot();
        interstellar::graphson::to_graphson_string(&snapshot)
            .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))
    }

    /// Import graph data from a GraphSON JSON string.
    /// 
    /// @param json - GraphSON 3.0 formatted JSON string
    #[napi(js_name = "fromGraphSON")]
    pub fn from_graphson(&self, json: String) -> Result<()> {
        interstellar::graphson::from_graphson_string(&self.inner, &json)
            .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))
    }

    /// Clear all vertices and edges from the graph.
    #[napi]
    pub fn clear(&self) {
        // Note: Requires Graph to support clear() or recreate
        // For now, this would need internal implementation
    }

    // -------------------------------------------------------------------------
    // GQL Query Language
    // -------------------------------------------------------------------------

    /// Execute a GQL query string.
    /// 
    /// @param query - GQL query string
    /// @returns Query results as an array
    /// 
    /// @example
    /// ```javascript
    /// const results = graph.gql(`
    ///     MATCH (p:person)-[:knows]->(friend)
    ///     WHERE p.name = 'Alice'
    ///     RETURN friend.name
    /// `);
    /// ```
    #[napi]
    pub fn gql(&self, env: Env, query: String) -> Result<Vec<JsUnknown>> {
        let results = self.inner
            .gql(&query)
            .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
        
        results
            .iter()
            .map(|v| value_to_js(env, v))
            .collect()
    }
}

impl Default for JsGraph {
    fn default() -> Self {
        Self::new()
    }
}
```

---

## 6. Traversal Implementation (traversal.rs)

```rust
use napi::bindgen_prelude::*;
use napi_derive::napi;
use interstellar::{Graph, Value, VertexId, EdgeId};
use interstellar::storage::GraphSnapshot;
use interstellar::traversal::p;
use std::sync::Arc;

use crate::error::ResultExt;
use crate::value::{js_to_value, value_to_js};
use crate::predicate::JsPredicate;
use crate::builders::{OrderBuilder, ProjectBuilder, GroupBuilder, GroupCountBuilder, RepeatBuilder};

/// Internal representation of traversal steps
#[derive(Clone)]
pub(crate) enum TraversalStep {
    // Source
    AllVertices,
    VertexIds(Vec<VertexId>),
    AllEdges,
    EdgeIds(Vec<EdgeId>),
    
    // Navigation
    Out(Vec<String>),
    In(Vec<String>),
    Both(Vec<String>),
    OutE(Vec<String>),
    InE(Vec<String>),
    BothE(Vec<String>),
    OutV,
    InV,
    BothV,
    OtherV,
    
    // Filter
    HasLabel(Vec<String>),
    Has(String),
    HasValue(String, Value),
    HasPredicate(String, PredicateConfig),
    HasNot(String),
    HasId(Vec<i64>),
    Dedup,
    DedupByKey(String),
    Limit(usize),
    Skip(usize),
    Range(usize, usize),
    Where(Box<Vec<TraversalStep>>),
    Not(Box<Vec<TraversalStep>>),
    And(Vec<Vec<TraversalStep>>),
    Or(Vec<Vec<TraversalStep>>),
    SimplePath,
    CyclicPath,
    
    // Transform
    Values(Vec<String>),
    Id,
    Label,
    Properties(Vec<String>),
    ValueMap(Vec<String>, bool), // keys, with_tokens
    ElementMap(Vec<String>),
    Constant(Value),
    Unfold,
    Fold,
    Path,
    As(String),
    Select(Vec<String>),
    Count,
    Sum,
    Mean,
    Min,
    Max,
    
    // Branch
    Union(Vec<Vec<TraversalStep>>),
    Coalesce(Vec<Vec<TraversalStep>>),
    Optional(Box<Vec<TraversalStep>>),
    Local(Box<Vec<TraversalStep>>),
    
    // Order (simplified)
    OrderAsc,
    OrderDesc,
    OrderByKeyAsc(String),
    OrderByKeyDesc(String),
    
    // Group (simplified)
    GroupByLabel,
    GroupByKey(String),
    GroupCount,
    GroupCountByKey(String),
    
    // Mutation
    AddV(String),
    AddE(String),
    Property(String, Value),
    From(String),
    FromId(i64),
    To(String),
    ToId(i64),
    Drop,
}

/// Predicate configuration for filter steps
#[derive(Clone)]
pub(crate) enum PredicateConfig {
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

/// A graph traversal that can be chained with various steps.
/// 
/// Traversals are lazy - they only execute when a terminal step is called.
#[napi(js_name = "Traversal")]
#[derive(Clone)]
pub struct JsTraversal {
    graph: Arc<Graph>,
    steps: Vec<TraversalStep>,
}

impl JsTraversal {
    pub(crate) fn from_all_vertices(graph: Arc<Graph>) -> Self {
        Self {
            graph,
            steps: vec![TraversalStep::AllVertices],
        }
    }

    pub(crate) fn from_vertex_ids(graph: Arc<Graph>, ids: Vec<VertexId>) -> Self {
        Self {
            graph,
            steps: vec![TraversalStep::VertexIds(ids)],
        }
    }

    pub(crate) fn from_all_edges(graph: Arc<Graph>) -> Self {
        Self {
            graph,
            steps: vec![TraversalStep::AllEdges],
        }
    }

    pub(crate) fn from_edge_ids(graph: Arc<Graph>, ids: Vec<EdgeId>) -> Self {
        Self {
            graph,
            steps: vec![TraversalStep::EdgeIds(ids)],
        }
    }

    fn with_step(&self, step: TraversalStep) -> Self {
        let mut new_steps = self.steps.clone();
        new_steps.push(step);
        Self {
            graph: Arc::clone(&self.graph),
            steps: new_steps,
        }
    }
}

#[napi]
impl JsTraversal {
    // -------------------------------------------------------------------------
    // Navigation Steps
    // -------------------------------------------------------------------------

    /// Navigate to outgoing adjacent vertices.
    /// 
    /// @param labels - Optional edge labels to traverse
    #[napi]
    pub fn out(&self, labels: Option<Vec<String>>) -> JsTraversal {
        self.with_step(TraversalStep::Out(labels.unwrap_or_default()))
    }

    /// Navigate to incoming adjacent vertices.
    /// 
    /// @param labels - Optional edge labels to traverse
    #[napi(js_name = "in")]
    pub fn in_(&self, labels: Option<Vec<String>>) -> JsTraversal {
        self.with_step(TraversalStep::In(labels.unwrap_or_default()))
    }

    /// Navigate to adjacent vertices in both directions.
    /// 
    /// @param labels - Optional edge labels to traverse
    #[napi]
    pub fn both(&self, labels: Option<Vec<String>>) -> JsTraversal {
        self.with_step(TraversalStep::Both(labels.unwrap_or_default()))
    }

    /// Navigate to outgoing edges.
    /// 
    /// @param labels - Optional edge labels to match
    #[napi(js_name = "outE")]
    pub fn out_e(&self, labels: Option<Vec<String>>) -> JsTraversal {
        self.with_step(TraversalStep::OutE(labels.unwrap_or_default()))
    }

    /// Navigate to incoming edges.
    /// 
    /// @param labels - Optional edge labels to match
    #[napi(js_name = "inE")]
    pub fn in_e(&self, labels: Option<Vec<String>>) -> JsTraversal {
        self.with_step(TraversalStep::InE(labels.unwrap_or_default()))
    }

    /// Navigate to edges in both directions.
    /// 
    /// @param labels - Optional edge labels to match
    #[napi(js_name = "bothE")]
    pub fn both_e(&self, labels: Option<Vec<String>>) -> JsTraversal {
        self.with_step(TraversalStep::BothE(labels.unwrap_or_default()))
    }

    /// Navigate from an edge to its source vertex.
    #[napi(js_name = "outV")]
    pub fn out_v(&self) -> JsTraversal {
        self.with_step(TraversalStep::OutV)
    }

    /// Navigate from an edge to its target vertex.
    #[napi(js_name = "inV")]
    pub fn in_v(&self) -> JsTraversal {
        self.with_step(TraversalStep::InV)
    }

    /// Navigate from an edge to both endpoints.
    #[napi(js_name = "bothV")]
    pub fn both_v(&self) -> JsTraversal {
        self.with_step(TraversalStep::BothV)
    }

    /// Navigate to the vertex that was NOT the previous step.
    #[napi(js_name = "otherV")]
    pub fn other_v(&self) -> JsTraversal {
        self.with_step(TraversalStep::OtherV)
    }

    // -------------------------------------------------------------------------
    // Filter Steps
    // -------------------------------------------------------------------------

    /// Filter to elements with a specific label.
    /// 
    /// @param label - The label to match
    #[napi(js_name = "hasLabel")]
    pub fn has_label(&self, label: String) -> JsTraversal {
        self.with_step(TraversalStep::HasLabel(vec![label]))
    }

    /// Filter to elements with any of the specified labels.
    /// 
    /// @param labels - Labels to match (OR logic)
    #[napi(js_name = "hasLabelAny")]
    pub fn has_label_any(&self, labels: Vec<String>) -> JsTraversal {
        self.with_step(TraversalStep::HasLabel(labels))
    }

    /// Filter to elements that have a property (any value).
    /// 
    /// @param key - Property name
    #[napi]
    pub fn has(&self, key: String) -> JsTraversal {
        self.with_step(TraversalStep::Has(key))
    }

    /// Filter to elements that have a property with a specific value.
    /// 
    /// @param key - Property name
    /// @param value - Exact value to match
    #[napi(js_name = "hasValue")]
    pub fn has_value(&self, env: Env, key: String, value: JsUnknown) -> Result<JsTraversal> {
        let val = js_to_value(env, value)?;
        Ok(self.with_step(TraversalStep::HasValue(key, val)))
    }

    /// Filter to elements where property matches a predicate.
    /// 
    /// @param key - Property name
    /// @param predicate - Predicate to test (e.g., P.gt(10))
    #[napi(js_name = "hasWhere")]
    pub fn has_where(&self, key: String, predicate: &JsPredicate) -> JsTraversal {
        self.with_step(TraversalStep::HasPredicate(key, predicate.config.clone()))
    }

    /// Filter to elements that do NOT have a property.
    /// 
    /// @param key - Property name that must be absent
    #[napi(js_name = "hasNot")]
    pub fn has_not(&self, key: String) -> JsTraversal {
        self.with_step(TraversalStep::HasNot(key))
    }

    /// Filter to elements with specific IDs.
    /// 
    /// @param ids - Element IDs to match
    #[napi(js_name = "hasId")]
    pub fn has_id(&self, ids: Vec<i64>) -> JsTraversal {
        self.with_step(TraversalStep::HasId(ids))
    }

    /// Remove duplicate elements from the traversal.
    #[napi]
    pub fn dedup(&self) -> JsTraversal {
        self.with_step(TraversalStep::Dedup)
    }

    /// Remove duplicates based on a property key.
    /// 
    /// @param key - Property to deduplicate by
    #[napi(js_name = "dedupByKey")]
    pub fn dedup_by_key(&self, key: String) -> JsTraversal {
        self.with_step(TraversalStep::DedupByKey(key))
    }

    /// Limit results to the first n elements.
    /// 
    /// @param n - Maximum number of elements
    #[napi]
    pub fn limit(&self, n: i64) -> JsTraversal {
        self.with_step(TraversalStep::Limit(n as usize))
    }

    /// Skip the first n elements.
    /// 
    /// @param n - Number of elements to skip
    #[napi]
    pub fn skip(&self, n: i64) -> JsTraversal {
        self.with_step(TraversalStep::Skip(n as usize))
    }

    /// Take elements in a range [start, end).
    /// 
    /// @param start - Start index (inclusive)
    /// @param end - End index (exclusive)
    #[napi]
    pub fn range(&self, start: i64, end: i64) -> JsTraversal {
        self.with_step(TraversalStep::Range(start as usize, end as usize))
    }

    /// Filter to paths that don't repeat vertices.
    #[napi(js_name = "simplePath")]
    pub fn simple_path(&self) -> JsTraversal {
        self.with_step(TraversalStep::SimplePath)
    }

    /// Filter to paths that do repeat vertices.
    #[napi(js_name = "cyclicPath")]
    pub fn cyclic_path(&self) -> JsTraversal {
        self.with_step(TraversalStep::CyclicPath)
    }

    // -------------------------------------------------------------------------
    // Transform Steps
    // -------------------------------------------------------------------------

    /// Extract property values.
    /// 
    /// @param keys - Property names to extract
    #[napi]
    pub fn values(&self, keys: Vec<String>) -> JsTraversal {
        self.with_step(TraversalStep::Values(keys))
    }

    /// Extract the element ID.
    #[napi]
    pub fn id(&self) -> JsTraversal {
        self.with_step(TraversalStep::Id)
    }

    /// Extract the element label.
    #[napi]
    pub fn label(&self) -> JsTraversal {
        self.with_step(TraversalStep::Label)
    }

    /// Get a map of property name to value.
    /// 
    /// @param keys - Optional specific keys to include
    #[napi(js_name = "valueMap")]
    pub fn value_map(&self, keys: Option<Vec<String>>) -> JsTraversal {
        self.with_step(TraversalStep::ValueMap(keys.unwrap_or_default(), false))
    }

    /// Get a value map including id and label tokens.
    #[napi(js_name = "valueMapWithTokens")]
    pub fn value_map_with_tokens(&self, keys: Option<Vec<String>>) -> JsTraversal {
        self.with_step(TraversalStep::ValueMap(keys.unwrap_or_default(), true))
    }

    /// Get a complete element map (id, label, and all properties).
    /// 
    /// @param keys - Optional specific property keys to include
    #[napi(js_name = "elementMap")]
    pub fn element_map(&self, keys: Option<Vec<String>>) -> JsTraversal {
        self.with_step(TraversalStep::ElementMap(keys.unwrap_or_default()))
    }

    /// Replace each element with a constant value.
    /// 
    /// @param value - Constant value to emit
    #[napi]
    pub fn constant(&self, env: Env, value: JsUnknown) -> Result<JsTraversal> {
        let val = js_to_value(env, value)?;
        Ok(self.with_step(TraversalStep::Constant(val)))
    }

    /// Flatten lists in the stream.
    #[napi]
    pub fn unfold(&self) -> JsTraversal {
        self.with_step(TraversalStep::Unfold)
    }

    /// Collect all elements into a single list.
    #[napi]
    pub fn fold(&self) -> JsTraversal {
        self.with_step(TraversalStep::Fold)
    }

    /// Get the traversal path (history of elements visited).
    #[napi]
    pub fn path(&self) -> JsTraversal {
        self.with_step(TraversalStep::Path)
    }

    /// Label the current step for later reference.
    /// 
    /// @param label - Step label
    #[napi(js_name = "as")]
    pub fn as_(&self, label: String) -> JsTraversal {
        self.with_step(TraversalStep::As(label))
    }

    /// Select labeled steps from the path.
    /// 
    /// @param labels - Step labels to select
    #[napi]
    pub fn select(&self, labels: Vec<String>) -> JsTraversal {
        self.with_step(TraversalStep::Select(labels))
    }

    /// Count the number of elements.
    #[napi]
    pub fn count(&self) -> JsTraversal {
        self.with_step(TraversalStep::Count)
    }

    /// Calculate the sum of numeric values.
    #[napi]
    pub fn sum(&self) -> JsTraversal {
        self.with_step(TraversalStep::Sum)
    }

    /// Calculate the arithmetic mean of numeric values.
    #[napi]
    pub fn mean(&self) -> JsTraversal {
        self.with_step(TraversalStep::Mean)
    }

    /// Get the minimum value.
    #[napi]
    pub fn min(&self) -> JsTraversal {
        self.with_step(TraversalStep::Min)
    }

    /// Get the maximum value.
    #[napi]
    pub fn max(&self) -> JsTraversal {
        self.with_step(TraversalStep::Max)
    }

    // -------------------------------------------------------------------------
    // Order Steps (Simplified)
    // -------------------------------------------------------------------------

    /// Order by natural value (ascending).
    #[napi(js_name = "orderAsc")]
    pub fn order_asc(&self) -> JsTraversal {
        self.with_step(TraversalStep::OrderAsc)
    }

    /// Order by natural value (descending).
    #[napi(js_name = "orderDesc")]
    pub fn order_desc(&self) -> JsTraversal {
        self.with_step(TraversalStep::OrderDesc)
    }

    /// Order by a property key (ascending).
    /// 
    /// @param key - Property name
    #[napi(js_name = "orderByKeyAsc")]
    pub fn order_by_key_asc(&self, key: String) -> JsTraversal {
        self.with_step(TraversalStep::OrderByKeyAsc(key))
    }

    /// Order by a property key (descending).
    /// 
    /// @param key - Property name
    #[napi(js_name = "orderByKeyDesc")]
    pub fn order_by_key_desc(&self, key: String) -> JsTraversal {
        self.with_step(TraversalStep::OrderByKeyDesc(key))
    }

    // -------------------------------------------------------------------------
    // Group Steps (Simplified)
    // -------------------------------------------------------------------------

    /// Group elements by label.
    #[napi(js_name = "groupByLabel")]
    pub fn group_by_label(&self) -> JsTraversal {
        self.with_step(TraversalStep::GroupByLabel)
    }

    /// Group elements by a property key.
    /// 
    /// @param key - Property name
    #[napi(js_name = "groupByKey")]
    pub fn group_by_key(&self, key: String) -> JsTraversal {
        self.with_step(TraversalStep::GroupByKey(key))
    }

    /// Count elements by group (label).
    #[napi(js_name = "groupCount")]
    pub fn group_count(&self) -> JsTraversal {
        self.with_step(TraversalStep::GroupCount)
    }

    /// Count elements by a property key.
    /// 
    /// @param key - Property name
    #[napi(js_name = "groupCountByKey")]
    pub fn group_count_by_key(&self, key: String) -> JsTraversal {
        self.with_step(TraversalStep::GroupCountByKey(key))
    }

    // -------------------------------------------------------------------------
    // Branch Steps
    // -------------------------------------------------------------------------

    /// Execute multiple traversals and combine results.
    /// 
    /// @param traversals - Traversals to execute in parallel
    #[napi]
    pub fn union(&self, traversals: Vec<&JsTraversal>) -> JsTraversal {
        let step_lists: Vec<Vec<TraversalStep>> = traversals
            .into_iter()
            .map(|t| t.steps.clone())
            .collect();
        self.with_step(TraversalStep::Union(step_lists))
    }

    /// Return the result of the first traversal that produces output.
    /// 
    /// @param traversals - Traversals to try in order
    #[napi]
    pub fn coalesce(&self, traversals: Vec<&JsTraversal>) -> JsTraversal {
        let step_lists: Vec<Vec<TraversalStep>> = traversals
            .into_iter()
            .map(|t| t.steps.clone())
            .collect();
        self.with_step(TraversalStep::Coalesce(step_lists))
    }

    /// Execute traversal, but pass through original if no results.
    /// 
    /// @param traversal - Optional traversal
    #[napi]
    pub fn optional(&self, traversal: &JsTraversal) -> JsTraversal {
        self.with_step(TraversalStep::Optional(Box::new(traversal.steps.clone())))
    }

    /// Execute traversal in local scope (per element).
    /// 
    /// @param traversal - Traversal to execute locally
    #[napi]
    pub fn local(&self, traversal: &JsTraversal) -> JsTraversal {
        self.with_step(TraversalStep::Local(Box::new(traversal.steps.clone())))
    }

    // -------------------------------------------------------------------------
    // Mutation Steps
    // -------------------------------------------------------------------------

    /// Set a property on the current element.
    /// 
    /// @param key - Property name
    /// @param value - Property value
    #[napi]
    pub fn property(&self, env: Env, key: String, value: JsUnknown) -> Result<JsTraversal> {
        let val = js_to_value(env, value)?;
        Ok(self.with_step(TraversalStep::Property(key, val)))
    }

    /// Remove the current element from the graph.
    #[napi]
    pub fn drop(&self) -> JsTraversal {
        self.with_step(TraversalStep::Drop)
    }

    // -------------------------------------------------------------------------
    // Terminal Steps
    // -------------------------------------------------------------------------

    /// Execute the traversal and return all results as an array.
    /// 
    /// @returns Array of results
    #[napi(js_name = "toList")]
    pub fn to_list(&self, env: Env) -> Result<Vec<JsUnknown>> {
        let results = self.execute()?;
        results
            .into_iter()
            .map(|v| value_to_js(env, &v))
            .collect()
    }

    /// Execute and return the first result, or undefined.
    /// 
    /// @returns First result or undefined
    #[napi]
    pub fn first(&self, env: Env) -> Result<Option<JsUnknown>> {
        let results = self.execute()?;
        match results.into_iter().next() {
            Some(v) => Ok(Some(value_to_js(env, &v)?)),
            None => Ok(None),
        }
    }

    /// Execute and return exactly one result.
    /// 
    /// @throws If zero or more than one result
    #[napi]
    pub fn one(&self, env: Env) -> Result<JsUnknown> {
        let results = self.execute()?;
        if results.len() != 1 {
            return Err(Error::new(
                Status::GenericFailure,
                format!("Expected exactly one result, got {}", results.len()),
            ));
        }
        value_to_js(env, &results[0])
    }

    /// Check if the traversal has any results.
    /// 
    /// @returns true if at least one result exists
    #[napi(js_name = "hasNext")]
    pub fn has_next(&self) -> Result<bool> {
        let results = self.execute()?;
        Ok(!results.is_empty())
    }

    /// Execute and return the count of results.
    /// 
    /// @returns Number of results
    #[napi(js_name = "toCount")]
    pub fn to_count(&self) -> Result<i64> {
        let results = self.execute()?;
        Ok(results.len() as i64)
    }

    /// Iterate through all results (for side effects like drop).
    #[napi]
    pub fn iterate(&self) -> Result<()> {
        self.execute()?;
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Internal Execution
    // -------------------------------------------------------------------------

    fn execute(&self) -> Result<Vec<Value>> {
        // Build and execute the actual Rust traversal from recorded steps
        // This is a simplified implementation - the real version would
        // translate each step to the corresponding Rust traversal API
        
        let snapshot = self.graph.snapshot();
        let g = snapshot.gremlin();
        
        // Start traversal based on first step
        // Then apply remaining steps
        // This would require a more sophisticated step compiler
        
        // Placeholder implementation
        Ok(Vec::new())
    }
}
```

---

## 7. Predicate System (predicate.rs)

```rust
use napi::bindgen_prelude::*;
use napi_derive::napi;
use interstellar::Value;

use crate::value::js_to_value;
use crate::traversal::PredicateConfig;

/// A predicate for filtering values.
/// 
/// Predicates are created via the P namespace factory functions.
#[napi]
pub struct JsPredicate {
    pub(crate) config: PredicateConfig,
}

/// Predicate factory functions.
/// 
/// @example
/// ```javascript
/// const { P } = require('@interstellar/node');
/// 
/// graph.V()
///     .has('age', P.gte(18))
///     .has('name', P.startingWith('A'))
///     .toList();
/// ```
#[napi(js_name = "P")]
pub struct P;

#[napi]
impl P {
    // -------------------------------------------------------------------------
    // Comparison Predicates
    // -------------------------------------------------------------------------

    /// Equals comparison.
    /// 
    /// @param value - Value to compare against
    #[napi]
    pub fn eq(env: Env, value: JsUnknown) -> Result<JsPredicate> {
        let val = js_to_value(env, value)?;
        Ok(JsPredicate {
            config: PredicateConfig::Eq(val),
        })
    }

    /// Not equals comparison.
    /// 
    /// @param value - Value to compare against
    #[napi]
    pub fn neq(env: Env, value: JsUnknown) -> Result<JsPredicate> {
        let val = js_to_value(env, value)?;
        Ok(JsPredicate {
            config: PredicateConfig::Neq(val),
        })
    }

    /// Less than comparison.
    /// 
    /// @param value - Value to compare against
    #[napi]
    pub fn lt(env: Env, value: JsUnknown) -> Result<JsPredicate> {
        let val = js_to_value(env, value)?;
        Ok(JsPredicate {
            config: PredicateConfig::Lt(val),
        })
    }

    /// Less than or equal comparison.
    /// 
    /// @param value - Value to compare against
    #[napi]
    pub fn lte(env: Env, value: JsUnknown) -> Result<JsPredicate> {
        let val = js_to_value(env, value)?;
        Ok(JsPredicate {
            config: PredicateConfig::Lte(val),
        })
    }

    /// Greater than comparison.
    /// 
    /// @param value - Value to compare against
    #[napi]
    pub fn gt(env: Env, value: JsUnknown) -> Result<JsPredicate> {
        let val = js_to_value(env, value)?;
        Ok(JsPredicate {
            config: PredicateConfig::Gt(val),
        })
    }

    /// Greater than or equal comparison.
    /// 
    /// @param value - Value to compare against
    #[napi]
    pub fn gte(env: Env, value: JsUnknown) -> Result<JsPredicate> {
        let val = js_to_value(env, value)?;
        Ok(JsPredicate {
            config: PredicateConfig::Gte(val),
        })
    }

    // -------------------------------------------------------------------------
    // Range Predicates
    // -------------------------------------------------------------------------

    /// Value is between start and end (inclusive).
    /// 
    /// @param start - Range start
    /// @param end - Range end
    #[napi]
    pub fn between(env: Env, start: JsUnknown, end: JsUnknown) -> Result<JsPredicate> {
        let start_val = js_to_value(env, start)?;
        let end_val = js_to_value(env, end)?;
        Ok(JsPredicate {
            config: PredicateConfig::Between(start_val, end_val),
        })
    }

    // -------------------------------------------------------------------------
    // Collection Predicates
    // -------------------------------------------------------------------------

    /// Value is within the given set.
    /// 
    /// @param values - Values to check membership
    #[napi]
    pub fn within(env: Env, values: Vec<JsUnknown>) -> Result<JsPredicate> {
        let vals: Result<Vec<Value>> = values
            .into_iter()
            .map(|v| js_to_value(env, v))
            .collect();
        Ok(JsPredicate {
            config: PredicateConfig::Within(vals?),
        })
    }

    /// Value is NOT within the given set.
    /// 
    /// @param values - Values to exclude
    #[napi]
    pub fn without(env: Env, values: Vec<JsUnknown>) -> Result<JsPredicate> {
        let vals: Result<Vec<Value>> = values
            .into_iter()
            .map(|v| js_to_value(env, v))
            .collect();
        Ok(JsPredicate {
            config: PredicateConfig::Without(vals?),
        })
    }

    // -------------------------------------------------------------------------
    // String Predicates
    // -------------------------------------------------------------------------

    /// String starts with prefix.
    /// 
    /// @param prefix - Required prefix
    #[napi(js_name = "startingWith")]
    pub fn starting_with(prefix: String) -> JsPredicate {
        JsPredicate {
            config: PredicateConfig::StartingWith(prefix),
        }
    }

    /// String ends with suffix.
    /// 
    /// @param suffix - Required suffix
    #[napi(js_name = "endingWith")]
    pub fn ending_with(suffix: String) -> JsPredicate {
        JsPredicate {
            config: PredicateConfig::EndingWith(suffix),
        }
    }

    /// String contains substring.
    /// 
    /// @param substring - Substring to find
    #[napi]
    pub fn containing(substring: String) -> JsPredicate {
        JsPredicate {
            config: PredicateConfig::Containing(substring),
        }
    }

    /// String matches regular expression.
    /// 
    /// @param pattern - Regex pattern
    #[napi]
    pub fn regex(pattern: String) -> JsPredicate {
        JsPredicate {
            config: PredicateConfig::Regex(pattern),
        }
    }

    // -------------------------------------------------------------------------
    // Logical Predicates
    // -------------------------------------------------------------------------

    /// Logical AND of two predicates.
    /// 
    /// @param p1 - First predicate
    /// @param p2 - Second predicate
    #[napi]
    pub fn and(p1: &JsPredicate, p2: &JsPredicate) -> JsPredicate {
        JsPredicate {
            config: PredicateConfig::And(
                Box::new(p1.config.clone()),
                Box::new(p2.config.clone()),
            ),
        }
    }

    /// Logical OR of two predicates.
    /// 
    /// @param p1 - First predicate
    /// @param p2 - Second predicate
    #[napi]
    pub fn or(p1: &JsPredicate, p2: &JsPredicate) -> JsPredicate {
        JsPredicate {
            config: PredicateConfig::Or(
                Box::new(p1.config.clone()),
                Box::new(p2.config.clone()),
            ),
        }
    }

    /// Logical NOT of a predicate.
    /// 
    /// @param p - Predicate to negate
    #[napi]
    pub fn not(p: &JsPredicate) -> JsPredicate {
        JsPredicate {
            config: PredicateConfig::Not(Box::new(p.config.clone())),
        }
    }
}
```

---

## 8. Anonymous Traversal Factory (anonymous.rs)

```rust
use napi::bindgen_prelude::*;
use napi_derive::napi;
use interstellar::{Graph, Value};
use std::sync::Arc;

use crate::value::js_to_value;
use crate::traversal::{JsTraversal, TraversalStep, PredicateConfig};
use crate::predicate::JsPredicate;

/// Anonymous traversal factory for use in sub-traversals.
/// 
/// Creates traversal fragments for use with steps like `where()`, `union()`, `repeat()`.
/// 
/// @example
/// ```javascript
/// const { Graph, __ } = require('@interstellar/node');
/// 
/// // Find friends of friends
/// graph.V()
///     .hasLabel('person')
///     .repeat(__.out('knows'))
///     .times(2)
///     .dedup()
///     .toList();
/// ```
#[napi(js_name = "__")]
pub struct AnonymousTraversal;

#[napi]
impl AnonymousTraversal {
    /// Create a new anonymous traversal (identity).
    #[napi]
    pub fn identity() -> JsTraversal {
        // Anonymous traversals start with an empty step list
        // They get their source from the parent traversal context
        JsTraversal::anonymous()
    }

    // -------------------------------------------------------------------------
    // Navigation
    // -------------------------------------------------------------------------

    #[napi]
    pub fn out(labels: Option<Vec<String>>) -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::Out(labels.unwrap_or_default()))
    }

    #[napi(js_name = "in")]
    pub fn in_(labels: Option<Vec<String>>) -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::In(labels.unwrap_or_default()))
    }

    #[napi]
    pub fn both(labels: Option<Vec<String>>) -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::Both(labels.unwrap_or_default()))
    }

    #[napi(js_name = "outE")]
    pub fn out_e(labels: Option<Vec<String>>) -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::OutE(labels.unwrap_or_default()))
    }

    #[napi(js_name = "inE")]
    pub fn in_e(labels: Option<Vec<String>>) -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::InE(labels.unwrap_or_default()))
    }

    #[napi(js_name = "bothE")]
    pub fn both_e(labels: Option<Vec<String>>) -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::BothE(labels.unwrap_or_default()))
    }

    #[napi(js_name = "outV")]
    pub fn out_v() -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::OutV)
    }

    #[napi(js_name = "inV")]
    pub fn in_v() -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::InV)
    }

    #[napi(js_name = "bothV")]
    pub fn both_v() -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::BothV)
    }

    // -------------------------------------------------------------------------
    // Filter
    // -------------------------------------------------------------------------

    #[napi(js_name = "hasLabel")]
    pub fn has_label(label: String) -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::HasLabel(vec![label]))
    }

    #[napi]
    pub fn has(key: String) -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::Has(key))
    }

    #[napi(js_name = "hasValue")]
    pub fn has_value(env: Env, key: String, value: JsUnknown) -> Result<JsTraversal> {
        let val = js_to_value(env, value)?;
        Ok(JsTraversal::anonymous_with_step(TraversalStep::HasValue(key, val)))
    }

    #[napi(js_name = "hasWhere")]
    pub fn has_where(key: String, predicate: &JsPredicate) -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::HasPredicate(key, predicate.config.clone()))
    }

    #[napi]
    pub fn dedup() -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::Dedup)
    }

    #[napi]
    pub fn limit(n: i64) -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::Limit(n as usize))
    }

    // -------------------------------------------------------------------------
    // Transform
    // -------------------------------------------------------------------------

    #[napi]
    pub fn values(keys: Vec<String>) -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::Values(keys))
    }

    #[napi]
    pub fn id() -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::Id)
    }

    #[napi]
    pub fn label() -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::Label)
    }

    #[napi]
    pub fn count() -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::Count)
    }

    #[napi]
    pub fn fold() -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::Fold)
    }

    #[napi]
    pub fn unfold() -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::Unfold)
    }

    #[napi]
    pub fn constant(env: Env, value: JsUnknown) -> Result<JsTraversal> {
        let val = js_to_value(env, value)?;
        Ok(JsTraversal::anonymous_with_step(TraversalStep::Constant(val)))
    }

    // -------------------------------------------------------------------------
    // Path/Select
    // -------------------------------------------------------------------------

    #[napi(js_name = "as")]
    pub fn as_(step_label: String) -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::As(step_label))
    }

    #[napi]
    pub fn select(labels: Vec<String>) -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::Select(labels))
    }

    #[napi]
    pub fn path() -> JsTraversal {
        JsTraversal::anonymous_with_step(TraversalStep::Path)
    }
}

// Add helper methods to JsTraversal for anonymous creation
impl JsTraversal {
    pub(crate) fn anonymous() -> Self {
        Self {
            graph: Arc::new(Graph::new()), // Placeholder, will be replaced by parent
            steps: vec![],
        }
    }

    pub(crate) fn anonymous_with_step(step: TraversalStep) -> Self {
        Self {
            graph: Arc::new(Graph::new()), // Placeholder
            steps: vec![step],
        }
    }
}
```

---

## 9. Build & Distribution

### 9.1 GitHub Actions Workflow

Create `.github/workflows/node-bindings.yml`:

```yaml
name: Node.js Bindings CI

on:
  push:
    branches: [main]
    tags: ['v*']
  pull_request:
    branches: [main]
    paths:
      - 'interstellar-node/**'
      - 'src/**'
      - 'Cargo.toml'

env:
  DEBUG: napi:*
  MACOSX_DEPLOYMENT_TARGET: '10.13'

jobs:
  build:
    strategy:
      fail-fast: false
      matrix:
        settings:
          - host: macos-latest
            target: x86_64-apple-darwin
            build: |
              npm run build
              strip -x *.node
          - host: macos-latest
            target: aarch64-apple-darwin
            build: |
              npm run build -- --target aarch64-apple-darwin
              strip -x *.node
          - host: ubuntu-latest
            target: x86_64-unknown-linux-gnu
            docker: ghcr.io/napi-rs/napi-rs/nodejs-rust:lts-debian
            build: |
              npm run build -- --target x86_64-unknown-linux-gnu
              strip *.node
          - host: ubuntu-latest
            target: x86_64-unknown-linux-musl
            docker: ghcr.io/napi-rs/napi-rs/nodejs-rust:lts-alpine
            build: |
              npm run build -- --target x86_64-unknown-linux-musl
              strip *.node
          - host: ubuntu-latest
            target: aarch64-unknown-linux-gnu
            docker: ghcr.io/napi-rs/napi-rs/nodejs-rust:lts-debian-aarch64
            build: |
              npm run build -- --target aarch64-unknown-linux-gnu
              aarch64-linux-gnu-strip *.node
          - host: ubuntu-latest
            target: aarch64-unknown-linux-musl
            docker: ghcr.io/napi-rs/napi-rs/nodejs-rust:lts-alpine
            build: |
              rustup target add aarch64-unknown-linux-musl
              npm run build -- --target aarch64-unknown-linux-musl
          - host: windows-latest
            target: x86_64-pc-windows-msvc
            build: npm run build

    name: Build - ${{ matrix.settings.target }}
    runs-on: ${{ matrix.settings.host }}
    
    defaults:
      run:
        working-directory: interstellar-node

    steps:
      - uses: actions/checkout@v4

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: 20
          cache: npm
          cache-dependency-path: interstellar-node/package-lock.json

      - name: Install Rust
        uses: dtolnay/rust-toolchain@stable
        if: ${{ !matrix.settings.docker }}
        with:
          targets: ${{ matrix.settings.target }}

      - name: Install dependencies
        run: npm ci

      - name: Build in Docker
        uses: addnab/docker-run-action@v3
        if: ${{ matrix.settings.docker }}
        with:
          image: ${{ matrix.settings.docker }}
          options: '-v ${{ github.workspace }}:/build -w /build/interstellar-node'
          run: ${{ matrix.settings.build }}

      - name: Build
        if: ${{ !matrix.settings.docker }}
        run: ${{ matrix.settings.build }}

      - name: Upload artifact
        uses: actions/upload-artifact@v4
        with:
          name: bindings-${{ matrix.settings.target }}
          path: interstellar-node/*.node
          if-no-files-found: error

  test:
    name: Test
    needs: build
    runs-on: ubuntu-latest
    
    defaults:
      run:
        working-directory: interstellar-node

    steps:
      - uses: actions/checkout@v4

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: 20
          cache: npm
          cache-dependency-path: interstellar-node/package-lock.json

      - name: Install dependencies
        run: npm ci

      - name: Download artifact
        uses: actions/download-artifact@v4
        with:
          name: bindings-x86_64-unknown-linux-gnu
          path: interstellar-node

      - name: Run tests
        run: npm test

  publish:
    name: Publish to npm
    if: startsWith(github.ref, 'refs/tags/v')
    needs: [build, test]
    runs-on: ubuntu-latest
    
    defaults:
      run:
        working-directory: interstellar-node

    steps:
      - uses: actions/checkout@v4

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: 20
          registry-url: 'https://registry.npmjs.org'

      - name: Install dependencies
        run: npm ci

      - name: Download all artifacts
        uses: actions/download-artifact@v4
        with:
          path: interstellar-node/artifacts

      - name: Move artifacts
        run: npm run artifacts

      - name: Publish
        run: |
          npm run prepublishOnly
          npm publish --access public
        env:
          NODE_AUTH_TOKEN: ${{ secrets.NPM_TOKEN }}
```

### 9.2 Local Development Commands

```bash
# Setup
cd interstellar-node
npm install

# Development build (faster, with debug symbols)
npm run build:debug

# Release build (optimized)
npm run build

# Run tests
npm test
npm run test:watch

# Build for specific target
npm run build -- --target aarch64-apple-darwin

# Prepare for publishing
npm run prepublishOnly

# Local linking for testing
npm link
cd ../my-app
npm link @interstellar/node
```

---

## 10. TypeScript Type Definitions

napi-rs auto-generates `index.d.ts`. Enhance with custom sections in Rust:

```rust
// In lib.rs, add custom TypeScript type definitions
#[napi(custom_ts)]
const CUSTOM_TYPES: &str = r#"
/**
 * A property value type.
 * 
 * Note: Large integers use `number` (safe up to 2^53-1).
 * For full 64-bit precision, use bigint in JavaScript.
 */
export type Value = null | boolean | number | string | Value[] | Record<string, Value>;

/**
 * Vertex object returned from graph queries.
 */
export interface Vertex {
    /** Unique vertex identifier */
    readonly id: number;
    /** Vertex label (e.g., 'person', 'product') */
    readonly label: string;
    /** Vertex properties */
    readonly properties: Record<string, Value>;
}

/**
 * Edge object returned from graph queries.
 */
export interface Edge {
    /** Unique edge identifier */
    readonly id: number;
    /** Edge label (e.g., 'knows', 'purchased') */
    readonly label: string;
    /** Source vertex ID */
    readonly from: number;
    /** Target vertex ID */
    readonly to: number;
    /** Edge properties */
    readonly properties: Record<string, Value>;
}

/**
 * Import statistics from GraphSON loading.
 */
export interface ImportResult {
    verticesImported: number;
    edgesImported: number;
    warnings: string[];
}
"#;
```

---

## 11. Testing

### 11.1 Test File Structure

```
interstellar-node/
└── __test__/
    ├── graph.spec.ts        # Graph CRUD operations
    ├── traversal.spec.ts    # Traversal API tests
    ├── predicates.spec.ts   # Predicate system tests
    ├── anonymous.spec.ts    # Anonymous traversal tests
    └── integration.spec.ts  # End-to-end scenarios
```

### 11.2 Example Tests (vitest)

**`__test__/graph.spec.ts`**:

```typescript
import { describe, it, expect, beforeEach } from 'vitest';
import { Graph, P, __ } from '../index.js';

describe('Graph', () => {
    let graph: Graph;

    beforeEach(() => {
        graph = new Graph();
    });

    describe('vertex operations', () => {
        it('should add a vertex with properties', () => {
            const id = graph.addVertex('person', { name: 'Alice', age: 30 });
            
            expect(typeof id).toBe('number');
            expect(graph.vertexCount).toBe(1);
            
            const vertex = graph.getVertex(id);
            expect(vertex).toBeDefined();
            expect(vertex?.label).toBe('person');
            expect(vertex?.properties.name).toBe('Alice');
            expect(vertex?.properties.age).toBe(30);
        });

        it('should return undefined for non-existent vertex', () => {
            expect(graph.getVertex(999)).toBeUndefined();
        });

        it('should remove a vertex', () => {
            const id = graph.addVertex('person', { name: 'Alice' });
            expect(graph.removeVertex(id)).toBe(true);
            expect(graph.vertexCount).toBe(0);
            expect(graph.removeVertex(id)).toBe(false);
        });

        it('should set vertex property', () => {
            const id = graph.addVertex('person', { name: 'Alice' });
            graph.setVertexProperty(id, 'age', 30);
            
            const vertex = graph.getVertex(id);
            expect(vertex?.properties.age).toBe(30);
        });
    });

    describe('edge operations', () => {
        it('should add an edge between vertices', () => {
            const alice = graph.addVertex('person', { name: 'Alice' });
            const bob = graph.addVertex('person', { name: 'Bob' });
            
            const edgeId = graph.addEdge(alice, bob, 'knows', { since: 2020 });
            
            expect(typeof edgeId).toBe('number');
            expect(graph.edgeCount).toBe(1);
            
            const edge = graph.getEdge(edgeId);
            expect(edge?.label).toBe('knows');
            expect(edge?.from).toBe(alice);
            expect(edge?.to).toBe(bob);
            expect(edge?.properties.since).toBe(2020);
        });

        it('should throw when adding edge with invalid vertices', () => {
            const alice = graph.addVertex('person', { name: 'Alice' });
            
            expect(() => {
                graph.addEdge(alice, 999, 'knows', {});
            }).toThrow('Vertex not found');
        });
    });
});
```

**`__test__/traversal.spec.ts`**:

```typescript
import { describe, it, expect, beforeEach } from 'vitest';
import { Graph, P, __ } from '../index.js';

describe('Traversal', () => {
    let graph: Graph;
    let alice: number, bob: number, charlie: number;

    beforeEach(() => {
        graph = new Graph();
        
        // Create test data
        alice = graph.addVertex('person', { name: 'Alice', age: 30 });
        bob = graph.addVertex('person', { name: 'Bob', age: 25 });
        charlie = graph.addVertex('person', { name: 'Charlie', age: 35 });
        
        graph.addEdge(alice, bob, 'knows', { since: 2020 });
        graph.addEdge(alice, charlie, 'knows', { since: 2019 });
        graph.addEdge(bob, charlie, 'knows', { since: 2021 });
    });

    describe('source steps', () => {
        it('V() should return all vertices', () => {
            const results = graph.V().toList();
            expect(results.length).toBe(3);
        });

        it('V_(ids) should return specific vertices', () => {
            const results = graph.V_([alice, bob]).toList();
            expect(results.length).toBe(2);
        });

        it('E() should return all edges', () => {
            const results = graph.E().toList();
            expect(results.length).toBe(3);
        });
    });

    describe('filter steps', () => {
        it('hasLabel should filter by label', () => {
            const results = graph.V().hasLabel('person').toList();
            expect(results.length).toBe(3);
        });

        it('hasValue should filter by property value', () => {
            const results = graph.V()
                .hasLabel('person')
                .hasValue('name', 'Alice')
                .toList();
            expect(results.length).toBe(1);
        });

        it('hasWhere with predicate should filter correctly', () => {
            const results = graph.V()
                .hasLabel('person')
                .hasWhere('age', P.gte(30))
                .values(['name'])
                .toList();
            
            expect(results).toContain('Alice');
            expect(results).toContain('Charlie');
            expect(results).not.toContain('Bob');
        });

        it('limit should restrict results', () => {
            const results = graph.V().limit(2).toList();
            expect(results.length).toBe(2);
        });

        it('dedup should remove duplicates', () => {
            const results = graph.V()
                .out(['knows'])
                .out(['knows'])
                .dedup()
                .toList();
            
            // Each vertex should appear only once
            const ids = results.map((v: any) => v.id);
            expect(new Set(ids).size).toBe(ids.length);
        });
    });

    describe('navigation steps', () => {
        it('out should navigate to adjacent vertices', () => {
            const friends = graph.V_([alice])
                .out(['knows'])
                .values(['name'])
                .toList();
            
            expect(friends).toContain('Bob');
            expect(friends).toContain('Charlie');
        });

        it('in should navigate to incoming vertices', () => {
            const knownBy = graph.V_([bob])
                .in(['knows'])
                .values(['name'])
                .toList();
            
            expect(knownBy).toContain('Alice');
        });

        it('both should navigate in both directions', () => {
            const connections = graph.V_([bob])
                .both(['knows'])
                .dedup()
                .values(['name'])
                .toList();
            
            expect(connections).toContain('Alice');
            expect(connections).toContain('Charlie');
        });
    });

    describe('transform steps', () => {
        it('values should extract property values', () => {
            const names = graph.V()
                .hasLabel('person')
                .values(['name'])
                .toList();
            
            expect(names).toEqual(['Alice', 'Bob', 'Charlie'].sort());
        });

        it('valueMap should return property map', () => {
            const result = graph.V_([alice]).valueMap().first();
            
            expect(result).toHaveProperty('name', 'Alice');
            expect(result).toHaveProperty('age', 30);
        });

        it('count should return element count', () => {
            const count = graph.V().hasLabel('person').toCount();
            expect(count).toBe(3);
        });
    });

    describe('terminal steps', () => {
        it('toList should return array', () => {
            const results = graph.V().toList();
            expect(Array.isArray(results)).toBe(true);
        });

        it('first should return first or undefined', () => {
            const result = graph.V().hasLabel('person').first();
            expect(result).toBeDefined();
            
            const empty = graph.V().hasLabel('nonexistent').first();
            expect(empty).toBeUndefined();
        });

        it('one should return exactly one or throw', () => {
            const result = graph.V_([alice]).one();
            expect(result).toBeDefined();
            
            expect(() => {
                graph.V().hasLabel('person').one();
            }).toThrow('Expected exactly one');
        });

        it('hasNext should check for results', () => {
            expect(graph.V().hasLabel('person').hasNext()).toBe(true);
            expect(graph.V().hasLabel('nonexistent').hasNext()).toBe(false);
        });
    });
});
```

**`__test__/predicates.spec.ts`**:

```typescript
import { describe, it, expect } from 'vitest';
import { Graph, P } from '../index.js';

describe('Predicates', () => {
    let graph: Graph;

    beforeEach(() => {
        graph = new Graph();
        graph.addVertex('person', { name: 'Alice', age: 30 });
        graph.addVertex('person', { name: 'Bob', age: 25 });
        graph.addVertex('person', { name: 'Charlie', age: 35 });
    });

    describe('comparison predicates', () => {
        it('P.eq should match equal values', () => {
            const results = graph.V()
                .hasWhere('age', P.eq(30))
                .values(['name'])
                .toList();
            expect(results).toEqual(['Alice']);
        });

        it('P.neq should match non-equal values', () => {
            const results = graph.V()
                .hasWhere('age', P.neq(30))
                .values(['name'])
                .toList();
            expect(results.sort()).toEqual(['Bob', 'Charlie']);
        });

        it('P.gt should match greater values', () => {
            const results = graph.V()
                .hasWhere('age', P.gt(30))
                .values(['name'])
                .toList();
            expect(results).toEqual(['Charlie']);
        });

        it('P.gte should match greater or equal values', () => {
            const results = graph.V()
                .hasWhere('age', P.gte(30))
                .values(['name'])
                .toList();
            expect(results.sort()).toEqual(['Alice', 'Charlie']);
        });

        it('P.lt should match lesser values', () => {
            const results = graph.V()
                .hasWhere('age', P.lt(30))
                .values(['name'])
                .toList();
            expect(results).toEqual(['Bob']);
        });

        it('P.lte should match lesser or equal values', () => {
            const results = graph.V()
                .hasWhere('age', P.lte(30))
                .values(['name'])
                .toList();
            expect(results.sort()).toEqual(['Alice', 'Bob']);
        });
    });

    describe('range predicates', () => {
        it('P.between should match values in range', () => {
            const results = graph.V()
                .hasWhere('age', P.between(26, 34))
                .values(['name'])
                .toList();
            expect(results).toEqual(['Alice']);
        });

        it('P.within should match values in set', () => {
            const results = graph.V()
                .hasWhere('age', P.within([25, 35]))
                .values(['name'])
                .toList();
            expect(results.sort()).toEqual(['Bob', 'Charlie']);
        });

        it('P.without should exclude values in set', () => {
            const results = graph.V()
                .hasWhere('age', P.without([25, 35]))
                .values(['name'])
                .toList();
            expect(results).toEqual(['Alice']);
        });
    });

    describe('string predicates', () => {
        it('P.startingWith should match prefix', () => {
            const results = graph.V()
                .hasWhere('name', P.startingWith('A'))
                .values(['name'])
                .toList();
            expect(results).toEqual(['Alice']);
        });

        it('P.endingWith should match suffix', () => {
            const results = graph.V()
                .hasWhere('name', P.endingWith('e'))
                .values(['name'])
                .toList();
            expect(results.sort()).toEqual(['Alice', 'Charlie']);
        });

        it('P.containing should match substring', () => {
            const results = graph.V()
                .hasWhere('name', P.containing('li'))
                .values(['name'])
                .toList();
            expect(results.sort()).toEqual(['Alice', 'Charlie']);
        });

        it('P.regex should match pattern', () => {
            const results = graph.V()
                .hasWhere('name', P.regex('^[AB].*'))
                .values(['name'])
                .toList();
            expect(results.sort()).toEqual(['Alice', 'Bob']);
        });
    });

    describe('logical predicates', () => {
        it('P.and should combine predicates', () => {
            const results = graph.V()
                .hasWhere('age', P.and(P.gte(25), P.lte(30)))
                .values(['name'])
                .toList();
            expect(results.sort()).toEqual(['Alice', 'Bob']);
        });

        it('P.or should match either predicate', () => {
            const results = graph.V()
                .hasWhere('age', P.or(P.lt(26), P.gt(34)))
                .values(['name'])
                .toList();
            expect(results.sort()).toEqual(['Bob', 'Charlie']);
        });

        it('P.not should negate predicate', () => {
            const results = graph.V()
                .hasWhere('age', P.not(P.eq(30)))
                .values(['name'])
                .toList();
            expect(results.sort()).toEqual(['Bob', 'Charlie']);
        });
    });
});
```

---

## 12. Usage Examples

### 12.1 Basic Usage

```javascript
const { Graph, P, __ } = require('@interstellar/node');

// Create a graph
const graph = new Graph();

// Add vertices
const alice = graph.addVertex('person', { name: 'Alice', age: 30 });
const bob = graph.addVertex('person', { name: 'Bob', age: 25 });
const acme = graph.addVertex('company', { name: 'Acme Corp' });

// Add edges
graph.addEdge(alice, bob, 'knows', { since: 2020 });
graph.addEdge(alice, acme, 'worksAt', { role: 'Engineer' });
graph.addEdge(bob, acme, 'worksAt', { role: 'Designer' });

// Query: Find all people Alice knows
const friends = graph.V_([alice])
    .out(['knows'])
    .values(['name'])
    .toList();
console.log('Alice knows:', friends); // ['Bob']

// Query: Find adults (age >= 18) who work at Acme
const employees = graph.V()
    .hasLabel('person')
    .hasWhere('age', P.gte(18))
    .where(__.out(['worksAt']).hasLabel('company').hasValue('name', 'Acme Corp'))
    .values(['name'])
    .toList();
console.log('Acme employees:', employees); // ['Alice', 'Bob']

// Query: Group people by age range
const byAge = graph.V()
    .hasLabel('person')
    .groupByKey('age')
    .toList();
console.log('By age:', byAge);

// GQL query
const results = graph.gql(`
    MATCH (p:person)-[:knows]->(friend)
    WHERE p.name = 'Alice'
    RETURN friend.name
`);
console.log('GQL results:', results);
```

### 12.2 Advanced Patterns

```javascript
// Find friends of friends (excluding direct friends)
const foaf = graph.V_([alice])
    .as('start')
    .out(['knows'])
    .as('friend')
    .out(['knows'])
    .where(__.not(__.as('start')))
    .where(__.not(__.as('friend')))
    .dedup()
    .values(['name'])
    .toList();

// Find shortest path between two people
const path = graph.V_([alice])
    .repeat(__.both(['knows']))
    .until(__.hasId([charlie]))
    .path()
    .limit(1)
    .first();

// Aggregate statistics
const stats = graph.V()
    .hasLabel('person')
    .groupByLabel()
    .toList();
```

---

## 13. Implementation Phases

### Phase 1: Core Foundation (2 weeks)
- [ ] Create `interstellar-node` crate structure
- [ ] Implement `value.rs` - JavaScript value conversion
- [ ] Implement `error.rs` - Error type mapping
- [ ] Implement basic `graph.rs` - Graph construction and CRUD

### Phase 2: Traversal API (2 weeks)
- [ ] Implement `traversal.rs` - Step accumulation pattern
- [ ] Navigation steps (out, in, both, outE, inE, etc.)
- [ ] Filter steps (hasLabel, has, hasWhere, dedup, limit)
- [ ] Transform steps (values, valueMap, elementMap, id, label)
- [ ] Terminal steps (toList, first, one, count)

### Phase 3: Predicates & Anonymous Traversals (1 week)
- [ ] Implement `predicate.rs` - P namespace
- [ ] Implement `anonymous.rs` - __ factory
- [ ] Branch steps (union, coalesce, optional)
- [ ] Order and group steps

### Phase 4: Build & Distribution (1 week)
- [ ] GitHub Actions workflow
- [ ] Cross-platform testing
- [ ] npm publishing setup
- [ ] TypeScript type definitions

### Phase 5: Testing & Documentation (1 week)
- [ ] Comprehensive test suite
- [ ] Performance benchmarks
- [ ] API documentation
- [ ] Usage examples

---

## 14. References

- [napi-rs Documentation](https://napi.rs/)
- [napi-rs GitHub](https://github.com/napi-rs/napi-rs)
- [Node.js N-API](https://nodejs.org/api/n-api.html)
- [Apache TinkerPop Gremlin](https://tinkerpop.apache.org/gremlin.html)
- [Spec 45: WASM Bindings](./spec-45-wasm-bindgen.md)
