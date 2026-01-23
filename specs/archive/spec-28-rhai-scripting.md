# Spec 28: Rhai Scripting Integration

**Phase: Scripting & Extensibility**

## Overview

This spec defines the integration of the [Rhai](https://rhai.rs/) embedded scripting language with Interstellar's Gremlin-style traversal API. Rhai is a simple, fast, and safe embedded scripting language for Rust that compiles to bytecode and provides excellent Rust interoperability.

The integration will expose the full Gremlin API to Rhai scripts, enabling:
- Interactive graph exploration via a future REPL
- User-defined query scripts without Rust compilation
- Third-party projects to embed Interstellar with scripting support
- Dynamic query construction at runtime

**Duration**: 3-4 days  
**Priority**: Medium  
**Dependencies**: 
- Spec 03: Traversal Engine Core (implemented)
- Spec 07: Math Expressions (implemented)
- Spec 10: Mutations (implemented)

## Goals

1. Expose the complete Gremlin traversal API to Rhai scripts
2. Provide a reusable `RhaiEngine` module that other projects can import
3. Register all core types (`Value`, `VertexId`, `EdgeId`, predicates) with Rhai
4. Support anonymous traversal composition via `__` factory functions
5. Enable script-based graph mutations
6. Provide clear error messages that map Rhai errors to traversal errors
7. Add as an optional feature (`rhai`) to avoid bloating the core library

## Non-Goals

1. Building a full REPL in this spec (future work)
2. Rhai-to-GQL translation
3. Sandboxing or security restrictions (users control their own scripts)
4. Hot-reloading of scripts
5. Async/concurrent script execution

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      User Script (Rhai)                         │
│  let g = graph.traversal();                                     │
│  let results = g.v().has_label("person").values("name").list(); │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      RhaiEngine                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐   │
│  │ Type Registry│  │ Step Bindings│  │ Predicate Bindings   │   │
│  │ - Value      │  │ - v()        │  │ - eq()               │   │
│  │ - VertexId   │  │ - out()      │  │ - gt()               │   │
│  │ - EdgeId     │  │ - has()      │  │ - within()           │   │
│  │ - Traversal  │  │ - ...        │  │ - ...                │   │
│  └──────────────┘  └──────────────┘  └──────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Interstellar Core                              │
│  GraphTraversalSource → Traversal Steps → Terminal Execution    │
└─────────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

1. **Method Chaining via Rhai's Object Model**: Rhai supports method syntax on registered types, enabling fluent `g.v().out().has_label("person")` chains.

2. **Anonymous Traversals as First-Class Values**: The `__` object's methods return `Traversal` objects that can be passed to steps like `where_()`, `repeat()`, etc.

3. **Lazy Evaluation Preserved**: Traversals remain lazy until a terminal step is called from Rhai.

4. **Shared Immutable Snapshots**: Scripts operate on `GraphSnapshot` which is `Send + Sync`, allowing safe concurrent script execution on the same snapshot.

## Module Structure

| File | Description |
|------|-------------|
| `src/rhai/mod.rs` | Module root, public API exports |
| `src/rhai/engine.rs` | `RhaiEngine` builder and configuration |
| `src/rhai/types.rs` | Type registrations (`Value`, `VertexId`, `EdgeId`) |
| `src/rhai/traversal.rs` | Traversal step method bindings |
| `src/rhai/predicates.rs` | Predicate function bindings (global) |
| `src/rhai/anonymous.rs` | Anonymous traversal factory (`__.*`) |
| `src/rhai/error.rs` | Error type conversions |

## Deliverables

### 4.1 Feature Flag and Dependencies

Add Rhai as an optional dependency:

```toml
# Cargo.toml

[features]
default = ["inmemory"]
inmemory = []
mmap = ["memmap2"]
full-text = ["tantivy"]
rhai = ["dep:rhai"]  # New feature

[dependencies]
rhai = { version = "1.19", optional = true, features = ["sync"] }
```

The `sync` feature is required because `GraphSnapshot` is `Send + Sync` and we want thread-safe script execution.

### 4.2 Error Types

```rust
// src/rhai/error.rs

use rhai::EvalAltResult;
use thiserror::Error;

use crate::error::{StorageError, TraversalError};

/// Errors that can occur during Rhai script execution
#[derive(Debug, Error)]
pub enum RhaiError {
    /// Error during script compilation
    #[error("script compilation failed: {0}")]
    Compile(String),

    /// Error during script execution
    #[error("script execution failed: {0}")]
    Execution(String),

    /// Error from traversal operations
    #[error("traversal error: {0}")]
    Traversal(#[from] TraversalError),

    /// Error from storage operations
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    /// Type conversion error
    #[error("type error: expected {expected}, got {actual}")]
    Type { expected: String, actual: String },

    /// Missing required argument
    #[error("missing argument: {0}")]
    MissingArgument(String),
}

impl From<Box<EvalAltResult>> for RhaiError {
    fn from(err: Box<EvalAltResult>) -> Self {
        RhaiError::Execution(err.to_string())
    }
}

impl From<rhai::ParseError> for RhaiError {
    fn from(err: rhai::ParseError) -> Self {
        RhaiError::Compile(err.to_string())
    }
}

/// Result type for Rhai operations
pub type RhaiResult<T> = Result<T, RhaiError>;
```

### 4.3 Core Type Registrations

```rust
// src/rhai/types.rs

use rhai::{CustomType, Engine, TypeBuilder};

use crate::value::{EdgeId, Value, VertexId};

/// Register all core Interstellar types with the Rhai engine
pub fn register_types(engine: &mut Engine) {
    register_value(engine);
    register_vertex_id(engine);
    register_edge_id(engine);
}

fn register_vertex_id(engine: &mut Engine) {
    engine
        .register_type_with_name::<VertexId>("VertexId")
        .register_fn("vertex_id", |id: i64| VertexId(id as u64))
        .register_fn("to_string", |id: &mut VertexId| format!("{:?}", id))
        .register_get("id", |v: &mut VertexId| v.0 as i64);
}

fn register_edge_id(engine: &mut Engine) {
    engine
        .register_type_with_name::<EdgeId>("EdgeId")
        .register_fn("edge_id", |id: i64| EdgeId(id as u64))
        .register_fn("to_string", |id: &mut EdgeId| format!("{:?}", id))
        .register_get("id", |e: &mut EdgeId| e.0 as i64);
}

fn register_value(engine: &mut Engine) {
    engine
        .register_type_with_name::<Value>("Value")
        // Constructors
        .register_fn("value_int", |i: i64| Value::Int(i))
        .register_fn("value_float", |f: f64| Value::Float(f))
        .register_fn("value_string", |s: String| Value::String(s))
        .register_fn("value_bool", |b: bool| Value::Bool(b))
        .register_fn("value_null", || Value::Null)
        // Type checking
        .register_fn("is_int", |v: &mut Value| matches!(v, Value::Int(_)))
        .register_fn("is_float", |v: &mut Value| matches!(v, Value::Float(_)))
        .register_fn("is_string", |v: &mut Value| matches!(v, Value::String(_)))
        .register_fn("is_bool", |v: &mut Value| matches!(v, Value::Bool(_)))
        .register_fn("is_null", |v: &mut Value| matches!(v, Value::Null))
        .register_fn("is_list", |v: &mut Value| matches!(v, Value::List(_)))
        .register_fn("is_map", |v: &mut Value| matches!(v, Value::Map(_)))
        // Extraction (returns Dynamic for Rhai compatibility)
        .register_fn("as_int", |v: &mut Value| match v {
            Value::Int(i) => Ok(*i),
            _ => Err("not an integer".into()),
        })
        .register_fn("as_float", |v: &mut Value| match v {
            Value::Float(f) => Ok(*f),
            Value::Int(i) => Ok(*i as f64),
            _ => Err("not a float".into()),
        })
        .register_fn("as_string", |v: &mut Value| match v {
            Value::String(s) => Ok(s.clone()),
            _ => Err("not a string".into()),
        })
        .register_fn("as_bool", |v: &mut Value| match v {
            Value::Bool(b) => Ok(*b),
            _ => Err("not a boolean".into()),
        })
        .register_fn("to_string", |v: &mut Value| format!("{:?}", v));
}
```

### 4.4 Predicate Bindings

```rust
// src/rhai/predicates.rs

use rhai::{Dynamic, Engine, Module};

use crate::traversal::p::{self, Predicate};

/// Register predicate functions as global functions.
///
/// Predicates are registered globally (not under a namespace) for cleaner syntax:
/// ```rhai
/// g.v().has_where("age", gt(30))
/// g.v().has_where("name", within(["Alice", "Bob"]))
/// ```
pub fn register_predicates(engine: &mut Engine) {
    // Comparison predicates
    engine.register_fn("eq", |v: Dynamic| -> Predicate {
        p::eq(dynamic_to_value(v))
    });
    engine.register_fn("neq", |v: Dynamic| -> Predicate {
        p::neq(dynamic_to_value(v))
    });
    engine.register_fn("lt", |v: Dynamic| -> Predicate {
        p::lt(dynamic_to_value(v))
    });
    engine.register_fn("lte", |v: Dynamic| -> Predicate {
        p::lte(dynamic_to_value(v))
    });
    engine.register_fn("gt", |v: Dynamic| -> Predicate {
        p::gt(dynamic_to_value(v))
    });
    engine.register_fn("gte", |v: Dynamic| -> Predicate {
        p::gte(dynamic_to_value(v))
    });

    // Range predicates
    engine.register_fn("between", |low: Dynamic, high: Dynamic| -> Predicate {
        p::between(dynamic_to_value(low), dynamic_to_value(high))
    });
    engine.register_fn("inside", |low: Dynamic, high: Dynamic| -> Predicate {
        p::inside(dynamic_to_value(low), dynamic_to_value(high))
    });
    engine.register_fn("outside", |low: Dynamic, high: Dynamic| -> Predicate {
        p::outside(dynamic_to_value(low), dynamic_to_value(high))
    });

    // Collection predicates
    engine.register_fn("within", |values: rhai::Array| -> Predicate {
        let vals: Vec<_> = values.into_iter().map(dynamic_to_value).collect();
        p::within(vals)
    });
    engine.register_fn("without", |values: rhai::Array| -> Predicate {
        let vals: Vec<_> = values.into_iter().map(dynamic_to_value).collect();
        p::without(vals)
    });

    // Text predicates
    engine.register_fn("containing", |s: String| -> Predicate {
        p::containing(s)
    });
    engine.register_fn("starting_with", |s: String| -> Predicate {
        p::starting_with(s)
    });
    engine.register_fn("ending_with", |s: String| -> Predicate {
        p::ending_with(s)
    });
    engine.register_fn("regex", |pattern: String| -> Predicate {
        p::regex(&pattern)
    });

    // Logical combinators - using pred_not/pred_and/pred_or to avoid
    // conflicts with Rhai's built-in logical operators
    engine.register_fn("pred_not", |pred: Predicate| -> Predicate {
        p::not(pred)
    });
    engine.register_fn("pred_and", |p1: Predicate, p2: Predicate| -> Predicate {
        p::and(p1, p2)
    });
    engine.register_fn("pred_or", |p1: Predicate, p2: Predicate| -> Predicate {
        p::or(p1, p2)
    });
}

/// Convert Rhai Dynamic to Interstellar Value
fn dynamic_to_value(d: Dynamic) -> crate::value::Value {
    use crate::value::Value;
    
    if d.is_int() {
        Value::Int(d.as_int().unwrap())
    } else if d.is_float() {
        Value::Float(d.as_float().unwrap())
    } else if d.is_string() {
        Value::String(d.into_string().unwrap())
    } else if d.is_bool() {
        Value::Bool(d.as_bool().unwrap())
    } else if d.is_unit() {
        Value::Null
    } else if d.is_array() {
        let arr: rhai::Array = d.cast();
        Value::List(arr.into_iter().map(dynamic_to_value).collect())
    } else if d.is_map() {
        let map: rhai::Map = d.cast();
        Value::Map(
            map.into_iter()
                .map(|(k, v)| (k.to_string(), dynamic_to_value(v)))
                .collect(),
        )
    } else {
        Value::Null
    }
}
```

### 4.5 Traversal Step Bindings

The traversal bindings are the core of the integration. We need to wrap `BoundTraversal` to make it work with Rhai's type system.

```rust
// src/rhai/traversal.rs

use std::sync::Arc;

use rhai::{Dynamic, Engine, EvalAltResult};

use crate::graph::GraphSnapshot;
use crate::storage::GraphStorage;
use crate::traversal::{p::Predicate, BoundTraversal, GraphTraversalSource};
use crate::value::{Value, VertexId};

/// Wrapper around BoundTraversal for Rhai compatibility
/// 
/// This wrapper is necessary because Rhai requires `Clone` on all registered
/// types, and BoundTraversal contains non-Clone iterators. We use Arc to
/// share the underlying traversal state.
#[derive(Clone)]
pub struct RhaiTraversal {
    inner: Arc<parking_lot::Mutex<Option<BoundTraversal<Value>>>>,
}

impl RhaiTraversal {
    pub fn new(traversal: BoundTraversal<Value>) -> Self {
        Self {
            inner: Arc::new(parking_lot::Mutex::new(Some(traversal))),
        }
    }

    fn take(&self) -> Result<BoundTraversal<Value>, Box<EvalAltResult>> {
        self.inner
            .lock()
            .take()
            .ok_or_else(|| "traversal already consumed".into())
    }
}

/// Wrapper around GraphTraversalSource for Rhai
#[derive(Clone)]
pub struct RhaiTraversalSource<S: GraphStorage> {
    snapshot: Arc<GraphSnapshot<S>>,
}

impl<S: GraphStorage + 'static> RhaiTraversalSource<S> {
    pub fn new(snapshot: Arc<GraphSnapshot<S>>) -> Self {
        Self { snapshot }
    }

    fn source(&self) -> GraphTraversalSource<S> {
        // Note: This requires GraphSnapshot to be clonable or provide
        // a method to create a new traversal source
        self.snapshot.traversal()
    }
}

/// Register traversal source and step methods
pub fn register_traversal<S: GraphStorage + Clone + Send + Sync + 'static>(
    engine: &mut Engine,
) {
    // Register the wrapper types
    engine.register_type_with_name::<RhaiTraversal>("Traversal");
    engine.register_type_with_name::<RhaiTraversalSource<S>>("TraversalSource");

    // === Source Steps ===
    
    engine.register_fn("v", |source: &mut RhaiTraversalSource<S>| {
        RhaiTraversal::new(source.source().v().into())
    });
    
    engine.register_fn("v", |source: &mut RhaiTraversalSource<S>, id: i64| {
        RhaiTraversal::new(source.source().v_ids(&[VertexId(id as u64)]).into())
    });
    
    engine.register_fn("v_ids", |source: &mut RhaiTraversalSource<S>, ids: rhai::Array| {
        let vertex_ids: Vec<VertexId> = ids
            .into_iter()
            .filter_map(|d| d.as_int().ok().map(|i| VertexId(i as u64)))
            .collect();
        RhaiTraversal::new(source.source().v_ids(&vertex_ids).into())
    });

    engine.register_fn("e", |source: &mut RhaiTraversalSource<S>| {
        RhaiTraversal::new(source.source().e().into())
    });

    // === Navigation Steps ===

    engine.register_fn("out", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.out())
    });

    engine.register_fn("out", |t: &mut RhaiTraversal, label: String| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.out_labels(&[&label]))
    });

    engine.register_fn("in_", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.in_())
    });

    engine.register_fn("in_", |t: &mut RhaiTraversal, label: String| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.in_labels(&[&label]))
    });

    engine.register_fn("both", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.both())
    });

    engine.register_fn("both", |t: &mut RhaiTraversal, label: String| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.both_labels(&[&label]))
    });

    engine.register_fn("out_e", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.out_e())
    });

    engine.register_fn("in_e", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.in_e())
    });

    engine.register_fn("both_e", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.both_e())
    });

    engine.register_fn("out_v", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.out_v())
    });

    engine.register_fn("in_v", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.in_v())
    });

    engine.register_fn("other_v", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.other_v())
    });

    // === Filter Steps ===

    engine.register_fn("has_label", |t: &mut RhaiTraversal, label: String| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.has_label(&label))
    });

    engine.register_fn("has_label_any", |t: &mut RhaiTraversal, labels: rhai::Array| {
        let traversal = t.take().unwrap();
        let labels: Vec<String> = labels
            .into_iter()
            .filter_map(|d| d.into_string().ok())
            .collect();
        let label_refs: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
        RhaiTraversal::new(traversal.has_label_any(&label_refs))
    });

    engine.register_fn("has", |t: &mut RhaiTraversal, key: String| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.has(&key))
    });

    engine.register_fn("has_value", |t: &mut RhaiTraversal, key: String, value: Dynamic| {
        let traversal = t.take().unwrap();
        let val = super::predicates::dynamic_to_value(value);
        RhaiTraversal::new(traversal.has_value(&key, val))
    });

    engine.register_fn("has_where", |t: &mut RhaiTraversal, key: String, pred: Predicate| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.has_where(&key, pred))
    });

    engine.register_fn("has_not", |t: &mut RhaiTraversal, key: String| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.has_not(&key))
    });

    engine.register_fn("has_id", |t: &mut RhaiTraversal, id: i64| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.has_id(VertexId(id as u64)))
    });

    engine.register_fn("dedup", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.dedup())
    });

    engine.register_fn("limit", |t: &mut RhaiTraversal, n: i64| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.limit(n as usize))
    });

    engine.register_fn("skip", |t: &mut RhaiTraversal, n: i64| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.skip(n as usize))
    });

    engine.register_fn("range", |t: &mut RhaiTraversal, start: i64, end: i64| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.range(start as usize, end as usize))
    });

    engine.register_fn("is_eq", |t: &mut RhaiTraversal, value: Dynamic| {
        let traversal = t.take().unwrap();
        let val = super::predicates::dynamic_to_value(value);
        RhaiTraversal::new(traversal.is_eq(val))
    });

    engine.register_fn("is_", |t: &mut RhaiTraversal, pred: Predicate| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.is_(pred))
    });

    engine.register_fn("simple_path", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.simple_path())
    });

    engine.register_fn("cyclic_path", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.cyclic_path())
    });

    // === Transform Steps ===

    engine.register_fn("id", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.id())
    });

    engine.register_fn("label", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.label())
    });

    engine.register_fn("values", |t: &mut RhaiTraversal, key: String| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.values(&key))
    });

    engine.register_fn("values_multi", |t: &mut RhaiTraversal, keys: rhai::Array| {
        let traversal = t.take().unwrap();
        let keys: Vec<String> = keys
            .into_iter()
            .filter_map(|d| d.into_string().ok())
            .collect();
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        RhaiTraversal::new(traversal.values_multi(&key_refs))
    });

    engine.register_fn("value_map", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.value_map())
    });

    engine.register_fn("element_map", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.element_map())
    });

    engine.register_fn("path", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.path())
    });

    engine.register_fn("constant", |t: &mut RhaiTraversal, value: Dynamic| {
        let traversal = t.take().unwrap();
        let val = super::predicates::dynamic_to_value(value);
        RhaiTraversal::new(traversal.constant(val))
    });

    engine.register_fn("identity", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.identity())
    });

    engine.register_fn("fold", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.fold())
    });

    engine.register_fn("unfold", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.unfold())
    });

    // === Modulator Steps ===

    engine.register_fn("as_", |t: &mut RhaiTraversal, label: String| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.as_(&label))
    });

    engine.register_fn("select", |t: &mut RhaiTraversal, labels: rhai::Array| {
        let traversal = t.take().unwrap();
        let labels: Vec<String> = labels
            .into_iter()
            .filter_map(|d| d.into_string().ok())
            .collect();
        let label_refs: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
        RhaiTraversal::new(traversal.select(&label_refs))
    });

    engine.register_fn("select_one", |t: &mut RhaiTraversal, label: String| {
        let traversal = t.take().unwrap();
        RhaiTraversal::new(traversal.select_one(&label))
    });

    // === Terminal Steps ===

    engine.register_fn("to_list", |t: &mut RhaiTraversal| -> rhai::Array {
        let traversal = t.take().unwrap();
        traversal
            .to_list()
            .into_iter()
            .map(value_to_dynamic)
            .collect()
    });

    engine.register_fn("list", |t: &mut RhaiTraversal| -> rhai::Array {
        // Alias for to_list
        let traversal = t.take().unwrap();
        traversal
            .to_list()
            .into_iter()
            .map(value_to_dynamic)
            .collect()
    });

    engine.register_fn("next", |t: &mut RhaiTraversal| -> Dynamic {
        let traversal = t.take().unwrap();
        match traversal.next() {
            Some(v) => value_to_dynamic(v),
            None => Dynamic::UNIT,
        }
    });

    engine.register_fn("one", |t: &mut RhaiTraversal| -> Dynamic {
        let traversal = t.take().unwrap();
        match traversal.one() {
            Ok(Some(v)) => value_to_dynamic(v),
            _ => Dynamic::UNIT,
        }
    });

    engine.register_fn("count", |t: &mut RhaiTraversal| -> i64 {
        let traversal = t.take().unwrap();
        traversal.count() as i64
    });

    engine.register_fn("has_next", |t: &mut RhaiTraversal| -> bool {
        let traversal = t.take().unwrap();
        traversal.has_next()
    });

    engine.register_fn("iterate", |t: &mut RhaiTraversal| {
        let traversal = t.take().unwrap();
        traversal.iterate();
    });
}

/// Convert Interstellar Value to Rhai Dynamic
fn value_to_dynamic(v: Value) -> Dynamic {
    match v {
        Value::Int(i) => Dynamic::from(i),
        Value::Float(f) => Dynamic::from(f),
        Value::String(s) => Dynamic::from(s),
        Value::Bool(b) => Dynamic::from(b),
        Value::Null => Dynamic::UNIT,
        Value::List(arr) => {
            let rhai_arr: rhai::Array = arr.into_iter().map(value_to_dynamic).collect();
            Dynamic::from(rhai_arr)
        }
        Value::Map(map) => {
            let rhai_map: rhai::Map = map
                .into_iter()
                .map(|(k, v)| (k.into(), value_to_dynamic(v)))
                .collect();
            Dynamic::from(rhai_map)
        }
        Value::Vertex(v) => Dynamic::from(format!("v[{}]", v.id.0)),
        Value::Edge(e) => Dynamic::from(format!("e[{}]", e.id.0)),
        Value::VertexId(id) => Dynamic::from(id.0 as i64),
        Value::EdgeId(id) => Dynamic::from(id.0 as i64),
        Value::Path(p) => Dynamic::from(format!("{:?}", p)),
        Value::Property(p) => value_to_dynamic(p.value),
    }
}
```

### 4.6 Anonymous Traversal Factory

```rust
// src/rhai/anonymous.rs

use rhai::{Dynamic, Engine};

use crate::traversal::__;
use crate::value::Value;

/// Factory object for creating anonymous traversals.
///
/// This is exposed as a global `__` variable in Rhai scripts, allowing
/// method-style syntax: `__.out()`, `__.has_label("person")`, etc.
#[derive(Clone)]
pub struct AnonymousTraversalFactory;

/// Wrapper for anonymous traversals that can be passed to steps like where_(), repeat(), etc.
#[derive(Clone)]
pub struct RhaiAnonymousTraversal {
    // Store a factory function that creates the traversal when needed
    pub(crate) factory: std::sync::Arc<dyn Fn() -> crate::traversal::Traversal<Value, Value> + Send + Sync>,
}

impl RhaiAnonymousTraversal {
    pub fn new<F>(f: F) -> Self
    where
        F: Fn() -> crate::traversal::Traversal<Value, Value> + Send + Sync + 'static,
    {
        Self {
            factory: std::sync::Arc::new(f),
        }
    }

    pub fn build(&self) -> crate::traversal::Traversal<Value, Value> {
        (self.factory)()
    }
}

/// Register the `__` factory object and its methods
pub fn register_anonymous_traversals(engine: &mut Engine) {
    engine.register_type_with_name::<AnonymousTraversalFactory>("AnonymousTraversalFactory");
    engine.register_type_with_name::<RhaiAnonymousTraversal>("AnonymousTraversal");

    // === Navigation Steps ===

    engine.register_fn("out", |_: &mut AnonymousTraversalFactory| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(|| __::out())
    });

    engine.register_fn("out", |_: &mut AnonymousTraversalFactory, label: String| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(move || __::out_labels(&[&label]))
    });

    engine.register_fn("in_", |_: &mut AnonymousTraversalFactory| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(|| __::in_())
    });

    engine.register_fn("in_", |_: &mut AnonymousTraversalFactory, label: String| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(move || __::in_labels(&[&label]))
    });

    engine.register_fn("both", |_: &mut AnonymousTraversalFactory| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(|| __::both())
    });

    engine.register_fn("out_e", |_: &mut AnonymousTraversalFactory| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(|| __::out_e())
    });

    engine.register_fn("in_e", |_: &mut AnonymousTraversalFactory| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(|| __::in_e())
    });

    engine.register_fn("out_v", |_: &mut AnonymousTraversalFactory| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(|| __::out_v())
    });

    engine.register_fn("in_v", |_: &mut AnonymousTraversalFactory| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(|| __::in_v())
    });

    // === Filter Steps ===

    engine.register_fn("has_label", |_: &mut AnonymousTraversalFactory, label: String| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(move || __::has_label(&label))
    });

    engine.register_fn("has", |_: &mut AnonymousTraversalFactory, key: String| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(move || __::has(&key))
    });

    engine.register_fn("has_value", |_: &mut AnonymousTraversalFactory, key: String, value: Dynamic| -> RhaiAnonymousTraversal {
        let val = super::predicates::dynamic_to_value(value);
        RhaiAnonymousTraversal::new(move || __::has_value(&key, val.clone()))
    });

    engine.register_fn("has_not", |_: &mut AnonymousTraversalFactory, key: String| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(move || __::has_not(&key))
    });

    engine.register_fn("limit", |_: &mut AnonymousTraversalFactory, n: i64| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(move || __::limit(n as usize))
    });

    engine.register_fn("dedup", |_: &mut AnonymousTraversalFactory| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(|| __::dedup())
    });

    // === Transform Steps ===

    engine.register_fn("id", |_: &mut AnonymousTraversalFactory| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(|| __::id())
    });

    engine.register_fn("label", |_: &mut AnonymousTraversalFactory| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(|| __::label())
    });

    engine.register_fn("values", |_: &mut AnonymousTraversalFactory, key: String| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(move || __::values(&key))
    });

    engine.register_fn("identity", |_: &mut AnonymousTraversalFactory| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(|| __::identity())
    });

    engine.register_fn("constant", |_: &mut AnonymousTraversalFactory, value: Dynamic| -> RhaiAnonymousTraversal {
        let val = super::predicates::dynamic_to_value(value);
        RhaiAnonymousTraversal::new(move || __::constant(val.clone()))
    });

    engine.register_fn("path", |_: &mut AnonymousTraversalFactory| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(|| __::path())
    });

    engine.register_fn("fold", |_: &mut AnonymousTraversalFactory| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(|| __::fold())
    });

    engine.register_fn("unfold", |_: &mut AnonymousTraversalFactory| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(|| __::unfold())
    });

    // === Modulator Steps ===

    engine.register_fn("as_", |_: &mut AnonymousTraversalFactory, label: String| -> RhaiAnonymousTraversal {
        RhaiAnonymousTraversal::new(move || __::as_(&label))
    });
}

/// Create the `__` factory instance to be added to the scope
pub fn create_anonymous_factory() -> AnonymousTraversalFactory {
    AnonymousTraversalFactory
}
```

### 4.7 RhaiEngine Builder

```rust
// src/rhai/engine.rs

use std::sync::Arc;

use rhai::{Dynamic, Engine, Scope, AST};

use crate::graph::{Graph, GraphSnapshot};
use crate::storage::GraphStorage;

use super::error::{RhaiError, RhaiResult};
use super::traversal::RhaiTraversalSource;

/// A configured Rhai engine for executing graph traversal scripts.
///
/// # Example
///
/// ```rust
/// use interstellar::prelude::*;
/// use interstellar::rhai::RhaiEngine;
///
/// let graph = Graph::in_memory();
/// // ... add vertices and edges ...
///
/// let engine = RhaiEngine::new();
/// let snapshot = graph.snapshot();
///
/// let result = engine.eval_with_graph(
///     &snapshot,
///     r#"
///         let g = graph.traversal();
///         g.v().has_label("person").values("name").list()
///     "#
/// )?;
/// ```
pub struct RhaiEngine {
    engine: Engine,
}

impl RhaiEngine {
    /// Create a new RhaiEngine with all Interstellar bindings registered.
    pub fn new() -> Self {
        let mut engine = Engine::new();

        // Register core types
        super::types::register_types(&mut engine);

        // Register predicates as global functions
        super::predicates::register_predicates(&mut engine);

        // Register anonymous traversals under `__::` namespace
        super::anonymous::register_anonymous_traversals(&mut engine);

        Self { engine }
    }

    /// Create a RhaiEngine with custom configuration.
    pub fn with_engine(engine: Engine) -> Self {
        let mut engine = engine;

        super::types::register_types(&mut engine);
        super::predicates::register_predicates(&mut engine);
        super::anonymous::register_anonymous_traversals(&mut engine);

        Self { engine }
    }

    /// Get a reference to the underlying Rhai engine for custom configuration.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Get a mutable reference to the underlying Rhai engine.
    pub fn engine_mut(&mut self) -> &mut Engine {
        &mut self.engine
    }

    /// Compile a script to an AST for repeated execution.
    pub fn compile(&self, script: &str) -> RhaiResult<AST> {
        self.engine.compile(script).map_err(RhaiError::from)
    }

    /// Evaluate a script with a graph snapshot available as `graph`.
    ///
    /// The snapshot is exposed to the script as a variable named `graph`,
    /// which can be used to create traversal sources via `graph.traversal()`.
    /// The `__` anonymous traversal factory is also available.
    pub fn eval_with_graph<S: GraphStorage + Clone + Send + Sync + 'static>(
        &self,
        snapshot: &GraphSnapshot<S>,
        script: &str,
    ) -> RhaiResult<Dynamic> {
        // Register storage-specific traversal bindings
        let mut engine = self.engine.clone();
        super::traversal::register_traversal::<S>(&mut engine);

        // Create scope with graph and __ bindings
        let mut scope = Scope::new();
        let source = RhaiTraversalSource::new(Arc::new(snapshot.clone()));
        scope.push("graph", source);
        scope.push("__", super::anonymous::create_anonymous_factory());

        engine.eval_with_scope(&mut scope, script).map_err(RhaiError::from)
    }

    /// Evaluate a pre-compiled AST with a graph snapshot.
    pub fn eval_ast_with_graph<S: GraphStorage + Clone + Send + Sync + 'static>(
        &self,
        snapshot: &GraphSnapshot<S>,
        ast: &AST,
    ) -> RhaiResult<Dynamic> {
        let mut engine = self.engine.clone();
        super::traversal::register_traversal::<S>(&mut engine);

        let mut scope = Scope::new();
        let source = RhaiTraversalSource::new(Arc::new(snapshot.clone()));
        scope.push("graph", source);
        scope.push("__", super::anonymous::create_anonymous_factory());

        engine.eval_ast_with_scope(&mut scope, ast).map_err(RhaiError::from)
    }

    /// Evaluate a script without a graph (for testing predicates, etc.)
    pub fn eval(&self, script: &str) -> RhaiResult<Dynamic> {
        self.engine.eval(script).map_err(RhaiError::from)
    }
}

impl Default for RhaiEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for RhaiEngine {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
        }
    }
}
```

### 4.8 Module Root

```rust
// src/rhai/mod.rs

//! Rhai scripting integration for Interstellar.
//!
//! This module provides a way to execute Gremlin-style traversals using
//! the Rhai embedded scripting language. It enables interactive exploration,
//! dynamic query construction, and embedding in applications that want to
//! offer user-defined queries.
//!
//! # Quick Start
//!
//! ```rust
//! use interstellar::prelude::*;
//! use interstellar::rhai::RhaiEngine;
//!
//! // Create a graph
//! let graph = Graph::in_memory();
//! {
//!     let mut g = graph.mutate();
//!     g.add_vertex("person", props! { "name" => "Alice", "age" => 30 });
//!     g.add_vertex("person", props! { "name" => "Bob", "age" => 25 });
//! }
//!
//! // Create engine and execute script
//! let engine = RhaiEngine::new();
//! let snapshot = graph.snapshot();
//!
//! let names = engine.eval_with_graph(&snapshot, r#"
//!     let g = graph.traversal();
//!     g.v().has_label("person").values("name").list()
//! "#)?;
//!
//! println!("Names: {:?}", names); // ["Alice", "Bob"]
//! ```
//!
//! # Predicates
//!
//! Predicates are registered as global functions for cleaner syntax:
//!
//! ```rhai
//! // Filter by age > 25
//! g.v().has_where("age", gt(25)).list()
//!
//! // Filter by name in list
//! g.v().has_where("name", within(["Alice", "Bob"])).list()
//!
//! // Logical combinators use pred_ prefix to avoid conflicts
//! g.v().has_where("age", pred_and(gt(20), lt(40))).list()
//! ```
//!
//! # Anonymous Traversals
//!
//! Use the `__` object for anonymous traversals:
//!
//! ```rhai
//! // Find friends of friends
//! g.v().has_label("person").out("knows").where_(__.out("knows")).list()
//! ```

mod anonymous;
mod engine;
mod error;
mod predicates;
mod traversal;
mod types;

pub use engine::RhaiEngine;
pub use error::{RhaiError, RhaiResult};
pub use traversal::{RhaiTraversal, RhaiTraversalSource};
pub use anonymous::RhaiAnonymousTraversal;
```

## API Integration

### Exposing via lib.rs

Add conditional compilation for the rhai module:

```rust
// src/lib.rs

#[cfg(feature = "rhai")]
pub mod rhai;
```

### Prelude Extension (Optional)

Users who frequently use the Rhai integration can import directly:

```rust
// User code
use interstellar::rhai::{RhaiEngine, RhaiError, RhaiResult};
```

## Test Cases

### 5.1 Unit Tests for Type Conversions

```rust
// tests/rhai/types.rs

#[cfg(feature = "rhai")]
mod tests {
    use interstellar::rhai::RhaiEngine;

    #[test]
    fn test_value_int_creation() {
        let engine = RhaiEngine::new();
        let result: i64 = engine.eval("value_int(42).as_int()").unwrap().cast();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_value_string_creation() {
        let engine = RhaiEngine::new();
        let result: String = engine.eval(r#"value_string("hello").as_string()"#).unwrap().cast();
        assert_eq!(result, "hello");
    }

    #[test]
    fn test_value_type_checks() {
        let engine = RhaiEngine::new();
        assert!(engine.eval("value_int(1).is_int()").unwrap().cast::<bool>());
        assert!(engine.eval(r#"value_string("x").is_string()"#).unwrap().cast::<bool>());
        assert!(engine.eval("value_bool(true).is_bool()").unwrap().cast::<bool>());
        assert!(engine.eval("value_null().is_null()").unwrap().cast::<bool>());
    }

    #[test]
    fn test_vertex_id_creation() {
        let engine = RhaiEngine::new();
        let result: i64 = engine.eval("vertex_id(123).id").unwrap().cast();
        assert_eq!(result, 123);
    }
}
```

### 5.2 Unit Tests for Predicates

```rust
// tests/rhai/predicates.rs

#[cfg(feature = "rhai")]
mod tests {
    use interstellar::prelude::*;
    use interstellar::rhai::RhaiEngine;

    fn create_test_graph() -> Graph<InMemoryGraph> {
        let graph = Graph::in_memory();
        {
            let mut g = graph.mutate();
            g.add_vertex("person", props! { "name" => "Alice", "age" => 30 });
            g.add_vertex("person", props! { "name" => "Bob", "age" => 25 });
            g.add_vertex("person", props! { "name" => "Charlie", "age" => 35 });
        }
        graph
    }

    #[test]
    fn test_predicate_eq() {
        let graph = create_test_graph();
        let engine = RhaiEngine::new();
        let snapshot = graph.snapshot();

        let result = engine.eval_with_graph(&snapshot, r#"
            let g = graph.traversal();
            g.v().has_where("age", eq(30)).values("name").list()
        "#).unwrap();

        let names: Vec<String> = result.cast();
        assert_eq!(names, vec!["Alice"]);
    }

    #[test]
    fn test_predicate_gt() {
        let graph = create_test_graph();
        let engine = RhaiEngine::new();
        let snapshot = graph.snapshot();

        let result = engine.eval_with_graph(&snapshot, r#"
            let g = graph.traversal();
            g.v().has_where("age", gt(28)).count()
        "#).unwrap();

        assert_eq!(result.cast::<i64>(), 2); // Alice (30) and Charlie (35)
    }

    #[test]
    fn test_predicate_within() {
        let graph = create_test_graph();
        let engine = RhaiEngine::new();
        let snapshot = graph.snapshot();

        let result = engine.eval_with_graph(&snapshot, r#"
            let g = graph.traversal();
            g.v().has_where("name", within(["Alice", "Charlie"])).count()
        "#).unwrap();

        assert_eq!(result.cast::<i64>(), 2);
    }

    #[test]
    fn test_predicate_between() {
        let graph = create_test_graph();
        let engine = RhaiEngine::new();
        let snapshot = graph.snapshot();

        let result = engine.eval_with_graph(&snapshot, r#"
            let g = graph.traversal();
            g.v().has_where("age", between(26, 34)).values("name").list()
        "#).unwrap();

        let names: Vec<String> = result.cast();
        assert_eq!(names, vec!["Alice"]); // Only Alice (30) is in [26, 34)
    }

    #[test]
    fn test_predicate_logical_and() {
        let graph = create_test_graph();
        let engine = RhaiEngine::new();
        let snapshot = graph.snapshot();

        let result = engine.eval_with_graph(&snapshot, r#"
            let g = graph.traversal();
            g.v().has_where("age", pred_and(gt(25), lt(35))).count()
        "#).unwrap();

        assert_eq!(result.cast::<i64>(), 1); // Only Alice (30)
    }
}
```

### 5.3 Integration Tests for Traversals

```rust
// tests/rhai/traversal.rs

#[cfg(feature = "rhai")]
mod tests {
    use interstellar::prelude::*;
    use interstellar::rhai::RhaiEngine;

    fn create_social_graph() -> Graph<InMemoryGraph> {
        let graph = Graph::in_memory();
        {
            let mut g = graph.mutate();
            let alice = g.add_vertex("person", props! { "name" => "Alice" });
            let bob = g.add_vertex("person", props! { "name" => "Bob" });
            let charlie = g.add_vertex("person", props! { "name" => "Charlie" });

            g.add_edge(alice, bob, "knows", props! {});
            g.add_edge(bob, charlie, "knows", props! {});
            g.add_edge(alice, charlie, "knows", props! {});
        }
        graph
    }

    #[test]
    fn test_basic_vertex_traversal() {
        let graph = create_social_graph();
        let engine = RhaiEngine::new();
        let snapshot = graph.snapshot();

        let result = engine.eval_with_graph(&snapshot, r#"
            let g = graph.traversal();
            g.v().has_label("person").count()
        "#).unwrap();

        assert_eq!(result.cast::<i64>(), 3);
    }

    #[test]
    fn test_navigation_out() {
        let graph = create_social_graph();
        let engine = RhaiEngine::new();
        let snapshot = graph.snapshot();

        let result = engine.eval_with_graph(&snapshot, r#"
            let g = graph.traversal();
            g.v().has_value("name", "Alice").out("knows").values("name").list()
        "#).unwrap();

        let names: Vec<String> = result.cast();
        assert_eq!(names.len(), 2); // Bob and Charlie
        assert!(names.contains(&"Bob".to_string()));
        assert!(names.contains(&"Charlie".to_string()));
    }

    #[test]
    fn test_navigation_in() {
        let graph = create_social_graph();
        let engine = RhaiEngine::new();
        let snapshot = graph.snapshot();

        let result = engine.eval_with_graph(&snapshot, r#"
            let g = graph.traversal();
            g.v().has_value("name", "Charlie").in_("knows").values("name").list()
        "#).unwrap();

        let names: Vec<String> = result.cast();
        assert_eq!(names.len(), 2); // Alice and Bob
    }

    #[test]
    fn test_dedup() {
        let graph = create_social_graph();
        let engine = RhaiEngine::new();
        let snapshot = graph.snapshot();

        let result = engine.eval_with_graph(&snapshot, r#"
            let g = graph.traversal();
            g.v().out("knows").out("knows").dedup().count()
        "#).unwrap();

        // All paths lead to Charlie, dedup should give 1
        assert!(result.cast::<i64>() >= 1);
    }

    #[test]
    fn test_limit_and_skip() {
        let graph = create_social_graph();
        let engine = RhaiEngine::new();
        let snapshot = graph.snapshot();

        let result = engine.eval_with_graph(&snapshot, r#"
            let g = graph.traversal();
            g.v().has_label("person").limit(2).count()
        "#).unwrap();

        assert_eq!(result.cast::<i64>(), 2);

        let result = engine.eval_with_graph(&snapshot, r#"
            let g = graph.traversal();
            g.v().has_label("person").skip(1).count()
        "#).unwrap();

        assert_eq!(result.cast::<i64>(), 2);
    }

    #[test]
    fn test_value_map() {
        let graph = Graph::in_memory();
        {
            let mut g = graph.mutate();
            g.add_vertex("person", props! { "name" => "Alice", "age" => 30 });
        }

        let engine = RhaiEngine::new();
        let snapshot = graph.snapshot();

        let result = engine.eval_with_graph(&snapshot, r#"
            let g = graph.traversal();
            g.v().value_map().next()
        "#).unwrap();

        // Result should be a map with name and age
        assert!(!result.is_unit());
    }

    #[test]
    fn test_path() {
        let graph = create_social_graph();
        let engine = RhaiEngine::new();
        let snapshot = graph.snapshot();

        let result = engine.eval_with_graph(&snapshot, r#"
            let g = graph.traversal();
            g.v().has_value("name", "Alice").out("knows").path().list()
        "#).unwrap();

        let paths: rhai::Array = result.cast();
        assert_eq!(paths.len(), 2); // Two paths from Alice
    }
}
```

### 5.4 Tests for Anonymous Traversals

```rust
// tests/rhai/anonymous.rs

#[cfg(feature = "rhai")]
mod tests {
    use interstellar::prelude::*;
    use interstellar::rhai::RhaiEngine;

    #[test]
    fn test_anonymous_out() {
        let graph = Graph::in_memory();
        {
            let mut g = graph.mutate();
            let a = g.add_vertex("person", props! { "name" => "Alice" });
            let b = g.add_vertex("person", props! { "name" => "Bob" });
            let c = g.add_vertex("person", props! { "name" => "Charlie" });
            g.add_edge(a, b, "knows", props! {});
            g.add_edge(b, c, "knows", props! {});
        }

        let engine = RhaiEngine::new();
        let snapshot = graph.snapshot();

        // Find people who know someone
        let result = engine.eval_with_graph(&snapshot, r#"
            let g = graph.traversal();
            g.v().where_(__.out("knows")).values("name").list()
        "#).unwrap();

        let names: Vec<String> = result.cast();
        assert!(names.contains(&"Alice".to_string()));
        assert!(names.contains(&"Bob".to_string()));
        assert!(!names.contains(&"Charlie".to_string())); // Charlie knows no one
    }

    #[test]
    fn test_anonymous_has_label() {
        let graph = Graph::in_memory();
        {
            let mut g = graph.mutate();
            g.add_vertex("person", props! { "name" => "Alice" });
            g.add_vertex("company", props! { "name" => "ACME" });
        }

        let engine = RhaiEngine::new();
        let snapshot = graph.snapshot();

        let result = engine.eval_with_graph(&snapshot, r#"
            let g = graph.traversal();
            g.v().where_(__.has_label("person")).count()
        "#).unwrap();

        assert_eq!(result.cast::<i64>(), 1);
    }
}
```

### 5.5 Error Handling Tests

```rust
// tests/rhai/errors.rs

#[cfg(feature = "rhai")]
mod tests {
    use interstellar::rhai::{RhaiEngine, RhaiError};

    #[test]
    fn test_compile_error() {
        let engine = RhaiEngine::new();
        let result = engine.compile("let x = ");

        assert!(matches!(result, Err(RhaiError::Compile(_))));
    }

    #[test]
    fn test_runtime_error() {
        let engine = RhaiEngine::new();
        let result = engine.eval("undefined_function()");

        assert!(matches!(result, Err(RhaiError::Execution(_))));
    }

    #[test]
    fn test_type_error_in_value() {
        let engine = RhaiEngine::new();
        let result: Result<i64, _> = engine.eval(r#"value_string("hello").as_int()"#);

        // This should return an error because "hello" is not an int
        assert!(result.is_err());
    }

    #[test]
    fn test_traversal_consumed_error() {
        let graph = interstellar::prelude::Graph::in_memory();
        let engine = RhaiEngine::new();
        let snapshot = graph.snapshot();

        // Trying to use a traversal after it's been consumed should error
        let result = engine.eval_with_graph(&snapshot, r#"
            let g = graph.traversal();
            let t = g.v();
            t.count();  // Consumes the traversal
            t.count();  // Should error
        "#);

        assert!(result.is_err());
    }
}
```

## Example Usage

### Basic Script Execution

```rust
use interstellar::prelude::*;
use interstellar::rhai::RhaiEngine;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build a graph
    let graph = Graph::in_memory();
    {
        let mut g = graph.mutate();

        let alice = g.add_vertex("person", props! {
            "name" => "Alice",
            "age" => 30
        });

        let bob = g.add_vertex("person", props! {
            "name" => "Bob",
            "age" => 25
        });

        let company = g.add_vertex("company", props! {
            "name" => "TechCorp"
        });

        g.add_edge(alice, bob, "knows", props! { "since" => 2020 });
        g.add_edge(alice, company, "works_at", props! {});
        g.add_edge(bob, company, "works_at", props! {});
    }

    // Create engine
    let engine = RhaiEngine::new();
    let snapshot = graph.snapshot();

    // Execute various queries
    let people_count = engine.eval_with_graph(&snapshot, r#"
        let g = graph.traversal();
        g.v().has_label("person").count()
    "#)?;
    println!("People count: {}", people_count);

    let coworkers = engine.eval_with_graph(&snapshot, r#"
        let g = graph.traversal();
        g.v()
            .has_value("name", "Alice")
            .out("works_at")
            .in_("works_at")
            .has_where("name", neq("Alice"))
            .values("name")
            .list()
    "#)?;
    println!("Alice's coworkers: {:?}", coworkers);

    let older_than_25 = engine.eval_with_graph(&snapshot, r#"
        let g = graph.traversal();
        g.v()
            .has_label("person")
            .has_where("age", gt(25))
            .values("name")
            .list()
    "#)?;
    println!("People older than 25: {:?}", older_than_25);

    Ok(())
}
```

### Pre-compiled Scripts for Performance

```rust
use interstellar::prelude::*;
use interstellar::rhai::RhaiEngine;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = RhaiEngine::new();

    // Compile once
    let ast = engine.compile(r#"
        let g = graph.traversal();
        g.v().has_label("person").count()
    "#)?;

    // Execute multiple times with different snapshots
    let graph = Graph::in_memory();
    // ... populate graph ...

    for _ in 0..1000 {
        let snapshot = graph.snapshot();
        let count = engine.eval_ast_with_graph(&snapshot, &ast)?;
        // ... use count ...
    }

    Ok(())
}
```

### Embedding in a Third-Party Application

```rust
use interstellar::prelude::*;
use interstellar::rhai::RhaiEngine;

/// A query service that executes user-provided scripts
pub struct QueryService {
    engine: RhaiEngine,
    graph: Graph<InMemoryGraph>,
}

impl QueryService {
    pub fn new() -> Self {
        Self {
            engine: RhaiEngine::new(),
            graph: Graph::in_memory(),
        }
    }

    /// Execute a user-provided query script
    pub fn execute(&self, script: &str) -> Result<String, String> {
        let snapshot = self.graph.snapshot();

        self.engine
            .eval_with_graph(&snapshot, script)
            .map(|result| format!("{:?}", result))
            .map_err(|e| e.to_string())
    }

    /// Compile and validate a script without executing
    pub fn validate(&self, script: &str) -> Result<(), String> {
        self.engine
            .compile(script)
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}
```

## Success Criteria

- [ ] Feature flag `rhai` compiles and tests pass
- [ ] All core types registered: `Value`, `VertexId`, `EdgeId`
- [ ] All predicates available as global functions (`eq`, `gt`, `within`, etc.)
- [ ] All anonymous traversal factories available via `__` object (`__.out()`, `__.has_label()`, etc.)
- [ ] Navigation steps work: `out`, `in_`, `both`, `out_e`, `in_e`, etc.
- [ ] Filter steps work: `has_label`, `has`, `has_where`, `has_not`, `dedup`, `limit`, `skip`, `range`
- [ ] Transform steps work: `id`, `label`, `values`, `value_map`, `element_map`, `path`, `fold`, `unfold`
- [ ] Terminal steps work: `to_list`/`list`, `next`, `one`, `count`, `has_next`, `iterate`
- [ ] Modulator steps work: `as_`, `select`, `select_one`
- [ ] Error messages are clear and actionable
- [ ] Documentation includes examples for common use cases
- [ ] Third-party embedding example compiles and works
- [ ] Test coverage ≥ 80% for the rhai module

## Future Enhancements (Out of Scope)

1. **REPL Implementation**: Build an interactive REPL using `rustyline` (similar to `mathexpr/examples/repl.rs`)
2. **Mutation Support**: Enable `add_v()`, `add_e()`, `property()`, `drop()` from scripts
3. **Custom Functions**: Allow users to register their own Rhai functions
4. **Script Caching**: LRU cache for compiled ASTs
5. **Progress Callbacks**: Hook for long-running traversals to report progress
6. **Debugger Integration**: Step-through debugging for scripts
7. **GQL Interop**: Execute GQL queries from within Rhai scripts via `graph.gql("...")`
8. **Batch Execution**: Execute multiple scripts in parallel on the same snapshot

## Implementation Notes

### Rhai Limitations to Consider

1. **No Native Closures with Rust Types**: Rhai closures can't directly capture Rust types. Use anonymous traversal factories instead of inline closures for steps like `filter()` and `map()`.

2. **Method Chaining Ownership**: Rhai passes `&mut` to methods, but our traversal model consumes the traversal. The `RhaiTraversal` wrapper with `Arc<Mutex<Option<...>>>` handles this.

3. **No Generics in Bindings**: We can't expose generic types directly. The bindings use `Value` as the universal type.

4. **Integer Size**: Rhai uses `i64` for all integers; we convert to/from `u64` for IDs.

### Performance Considerations

1. **Compile Once, Execute Many**: For repeated query execution, use `compile()` + `eval_ast_with_graph()`.

2. **Clone Cost**: `RhaiEngine::eval_with_graph` clones the engine to register storage-specific bindings. For hot paths, consider caching configured engines per storage type.

3. **Dynamic Dispatch**: Rhai uses dynamic typing, adding overhead compared to native Rust traversals. For performance-critical code, use the native API directly.

### Thread Safety

- `RhaiEngine` is `Clone` and `Send + Sync`
- `GraphSnapshot` is `Send + Sync`
- Multiple threads can execute scripts concurrently on the same snapshot
- Each script execution is isolated (no shared mutable state)
