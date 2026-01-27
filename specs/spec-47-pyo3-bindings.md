# Spec 47: Python Bindings via PyO3 and maturin

This specification defines the Python bindings for Interstellar using [PyO3](https://pyo3.rs/) and [maturin](https://www.maturin.rs/), providing high-performance Python integration with full typing support.

**Prerequisite**: Core library implementation (Phases 1-6) must be complete.

---

## 1. Overview

### 1.1 Motivation

Python is the dominant language for data science, machine learning, and graph analytics. Native Python bindings via PyO3 enable:

| Aspect | Benefit |
|--------|---------|
| **Data Science Integration** | Works with NumPy, Pandas, NetworkX, PyTorch Geometric |
| **Performance** | Near-native speed vs pure Python graph libraries |
| **Type Safety** | Full type hints for IDE support and mypy |
| **Ecosystem** | PyPI distribution, conda packaging |
| **Memory Efficiency** | Zero-copy where possible, efficient iterator protocol |

**Primary use cases for Python bindings:**
- Graph analytics and machine learning pipelines
- Knowledge graph construction and querying
- Social network analysis
- Recommendation system development
- ETL pipelines with graph transformations
- Jupyter notebook exploration

### 1.2 Scope

This specification covers:

- Separate `interstellar-py` crate structure
- PyO3 wrapper types: `Graph`, `Traversal`, `Vertex`, `Edge`
- Full traversal API with method chaining (matching Rust/JS APIs)
- Predicate system (`P.eq()`, `P.gt()`, etc.)
- Anonymous traversal factory (`__`)
- Python iterator protocol support
- Type stub generation (`.pyi` files)
- maturin build and PyPI publishing

### 1.3 Non-Goals

| Non-Goal | Rationale |
|----------|-----------|
| Async/await support | Python's async model differs from Rust - future spec |
| NetworkX compatibility layer | Separate adapter package - future enhancement |
| Pandas DataFrame integration | Application-level concern - examples provided |
| Jupyter magic commands | IDE-specific - future enhancement |
| Distributed graph support | Separate architectural concern |

### 1.4 Design Principles

| Principle | Description |
|-----------|-------------|
| **Pythonic API** | Use snake_case, Python idioms, context managers |
| **API Parity** | Same method names and semantics as Rust/JS APIs |
| **Type Safety** | Complete type stubs with no `Any` types where avoidable |
| **Zero-Copy** | Minimize data copying at Rust-Python boundary |
| **Iterator Protocol** | Support `for` loops, list comprehensions, generators |
| **GIL-aware** | Release GIL during long computations |

---

## 2. Architecture

### 2.1 Crate Structure

```
interstellar/
├── Cargo.toml                    # Workspace root
├── src/                          # Core library
└── interstellar-py/              # Python bindings
    ├── Cargo.toml
    ├── pyproject.toml            # maturin/PEP 517 config
    ├── src/
    │   ├── lib.rs                # Module exports, PyModule init
    │   ├── graph.rs              # PyGraph wrapper
    │   ├── traversal.rs          # PyTraversal builder
    │   ├── value.rs              # Value <-> PyObject conversion
    │   ├── predicate.rs          # P predicate factory
    │   ├── anonymous.rs          # __ anonymous traversal factory
    │   ├── builders.rs           # OrderBuilder, GroupBuilder, etc.
    │   ├── error.rs              # Error conversion to Python exceptions
    │   └── iterators.rs          # Python iterator protocol
    ├── interstellar/             # Python package directory
    │   ├── __init__.py           # Re-exports, version
    │   ├── __init__.pyi          # Type stubs
    │   ├── py.typed              # PEP 561 marker
    │   └── _interstellar.pyi     # Native module stubs
    ├── tests/                    # Python tests
    │   ├── test_graph.py
    │   ├── test_traversal.py
    │   └── test_predicates.py
    └── examples/                 # Usage examples
        ├── basic_usage.py
        ├── social_network.py
        └── pandas_integration.py
```

### 2.2 Dependency Graph

```
┌─────────────────────────────────────────────────────────────────┐
│                          Python                                  │
│   from interstellar import Graph, P, __                          │
│   g = Graph()                                                    │
│   g.add_vertex("person", {"name": "Alice"})                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    interstellar-py                               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │ graph.rs │  │traversal │  │predicate │  │ value.rs │        │
│  │          │  │   .rs    │  │   .rs    │  │          │        │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘        │
│       │             │             │             │                │
│       └─────────────┴─────────────┴─────────────┘                │
│                              │                                   │
│                     PyO3 bindings                                │
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

### 2.3 GIL Management

PyO3 requires careful GIL management for performance:

```
┌─────────────────────────────────────────────────────────────────┐
│                     Python Main Thread                           │
│                                                                  │
│   graph = Graph()           # GIL held (fast)                   │
│   graph.add_vertex(...)     # GIL held (fast mutation)          │
│   graph.V().to_list()       # GIL released during traversal     │
│   graph.load_graphson(...)  # GIL released during I/O           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼ (GIL released)
┌─────────────────────────────────────────────────────────────────┐
│                    Rust Execution                                │
│                                                                  │
│   py.allow_threads(|| {                                          │
│       // Pure Rust computation                                   │
│       // Can use multiple threads                                │
│   })                                                             │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Key GIL considerations:**

1. Short operations (add_vertex, get_vertex): Keep GIL, avoid overhead
2. Long traversals: Release GIL with `py.allow_threads()`
3. Iteration: Reacquire GIL for each Python object creation
4. Batch operations: Release GIL, return Vec, convert at end

---

## 3. Configuration Files

### 3.1 Cargo.toml

```toml
[package]
name = "interstellar-py"
version = "0.1.0"
edition = "2021"
description = "Python bindings for Interstellar graph database"
license = "MIT OR Apache-2.0"
repository = "https://github.com/anthropic/interstellar"
readme = "README.md"
keywords = ["graph", "database", "gremlin", "python", "pyo3"]
categories = ["database", "api-bindings"]

[lib]
name = "_interstellar"
crate-type = ["cdylib"]

[dependencies]
interstellar = { path = "..", features = ["graphson", "gql"] }
pyo3 = { version = "0.22", features = ["extension-module", "abi3-py39"] }

[build-dependencies]
pyo3-build-config = "0.22"

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

### 3.2 pyproject.toml

```toml
[build-system]
requires = ["maturin>=1.4,<2.0"]
build-backend = "maturin"

[project]
name = "interstellar-graph"
version = "0.1.0"
description = "High-performance graph database for Python"
readme = "README.md"
license = { text = "MIT OR Apache-2.0" }
authors = [
    { name = "Interstellar Contributors" }
]
keywords = ["graph", "database", "gremlin", "traversal", "analytics"]
classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "Intended Audience :: Science/Research",
    "License :: OSI Approved :: MIT License",
    "License :: OSI Approved :: Apache Software License",
    "Operating System :: OS Independent",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Programming Language :: Rust",
    "Topic :: Database",
    "Topic :: Scientific/Engineering",
    "Typing :: Typed",
]
requires-python = ">=3.9"

[project.urls]
Homepage = "https://github.com/anthropic/interstellar"
Documentation = "https://interstellar.readthedocs.io"
Repository = "https://github.com/anthropic/interstellar"
Changelog = "https://github.com/anthropic/interstellar/blob/main/CHANGELOG.md"

[project.optional-dependencies]
dev = [
    "pytest>=7.0",
    "pytest-benchmark>=4.0",
    "mypy>=1.0",
    "ruff>=0.1",
]
docs = [
    "sphinx>=7.0",
    "sphinx-rtd-theme>=2.0",
    "myst-parser>=2.0",
]

[tool.maturin]
features = ["pyo3/extension-module"]
python-source = "interstellar"
module-name = "interstellar._interstellar"
strip = true

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]

[tool.mypy]
python_version = "3.9"
strict = true
warn_return_any = true
warn_unused_configs = true

[tool.ruff]
line-length = 88
target-version = "py39"

[tool.ruff.lint]
select = ["E", "F", "I", "N", "W", "UP"]
```

---

## 4. Core Type Implementations

### 4.1 Module Entry Point (lib.rs)

```rust
use pyo3::prelude::*;

mod anonymous;
mod builders;
mod error;
mod graph;
mod iterators;
mod predicate;
mod traversal;
mod value;

pub use anonymous::AnonymousTraversal;
pub use builders::{GroupBuilder, GroupCountBuilder, OrderBuilder, ProjectBuilder, RepeatBuilder};
pub use graph::PyGraph;
pub use predicate::PyP;
pub use traversal::PyTraversal;

/// Interstellar: A high-performance graph database for Python.
///
/// Example:
///     >>> from interstellar import Graph, P, __
///     >>> g = Graph()
///     >>> alice = g.add_vertex("person", {"name": "Alice", "age": 30})
///     >>> bob = g.add_vertex("person", {"name": "Bob", "age": 25})
///     >>> g.add_edge(alice, bob, "knows", {"since": 2020})
///     >>> names = g.V().has_label("person").out("knows").values("name").to_list()
#[pymodule]
fn _interstellar(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyGraph>()?;
    m.add_class::<PyTraversal>()?;
    m.add_class::<predicate::PyPredicate>()?;
    m.add_class::<OrderBuilder>()?;
    m.add_class::<ProjectBuilder>()?;
    m.add_class::<GroupBuilder>()?;
    m.add_class::<GroupCountBuilder>()?;
    m.add_class::<RepeatBuilder>()?;
    
    // Add P namespace as a submodule
    let p_module = PyModule::new(m.py(), "P")?;
    predicate::register_p_module(&p_module)?;
    m.add_submodule(&p_module)?;
    
    // Add __ namespace as a submodule  
    let anon_module = PyModule::new(m.py(), "__")?;
    anonymous::register_anon_module(&anon_module)?;
    m.add_submodule(&anon_module)?;
    
    // Module metadata
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    
    Ok(())
}
```

### 4.2 Value Conversion (value.rs)

```rust
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyNone, PyString};
use interstellar::Value;
use std::collections::HashMap;

/// Convert a Python object to a Rust Value
pub fn py_to_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        Ok(Value::Null)
    } else if let Ok(b) = obj.downcast::<PyBool>() {
        Ok(Value::Bool(b.is_true()))
    } else if let Ok(i) = obj.downcast::<PyInt>() {
        Ok(Value::Int(i.extract::<i64>()?))
    } else if let Ok(f) = obj.downcast::<PyFloat>() {
        Ok(Value::Float(f.extract::<f64>()?))
    } else if let Ok(s) = obj.downcast::<PyString>() {
        Ok(Value::String(s.to_str()?.to_string()))
    } else if let Ok(list) = obj.downcast::<PyList>() {
        let items: PyResult<Vec<Value>> = list.iter().map(|item| py_to_value(&item)).collect();
        Ok(Value::List(items?))
    } else if let Ok(dict) = obj.downcast::<PyDict>() {
        let mut map = HashMap::new();
        for (key, val) in dict.iter() {
            let k: String = key.extract()?;
            let v = py_to_value(&val)?;
            map.insert(k, v);
        }
        Ok(Value::Map(map))
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            format!("Unsupported type: {}", obj.get_type().name()?),
        ))
    }
}

/// Convert a Rust Value to a Python object
pub fn value_to_py(py: Python<'_>, value: &Value) -> PyResult<PyObject> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(b) => Ok(b.into_pyobject(py)?.into_any().unbind()),
        Value::Int(n) => Ok(n.into_pyobject(py)?.into_any().unbind()),
        Value::Float(f) => Ok(f.into_pyobject(py)?.into_any().unbind()),
        Value::String(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
        Value::List(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(value_to_py(py, item)?)?;
            }
            Ok(list.into())
        }
        Value::Map(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                dict.set_item(k, value_to_py(py, v)?)?;
            }
            Ok(dict.into())
        }
        Value::Vertex(id) => Ok(id.0.into_pyobject(py)?.into_any().unbind()),
        Value::Edge(id) => Ok(id.0.into_pyobject(py)?.into_any().unbind()),
    }
}

/// Convert Python dict to properties HashMap
pub fn py_to_properties(dict: Option<&Bound<'_, PyDict>>) -> PyResult<HashMap<String, Value>> {
    match dict {
        Some(d) => {
            let mut map = HashMap::new();
            for (key, val) in d.iter() {
                let k: String = key.extract()?;
                let v = py_to_value(&val)?;
                map.insert(k, v);
            }
            Ok(map)
        }
        None => Ok(HashMap::new()),
    }
}

/// Convert Rust properties to Python dict
pub fn properties_to_py(py: Python<'_>, props: &HashMap<String, Value>) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    for (k, v) in props {
        dict.set_item(k, value_to_py(py, v)?)?;
    }
    Ok(dict.into())
}
```

### 4.3 Error Handling (error.rs)

```rust
use pyo3::prelude::*;
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyValueError};
use interstellar::{MutationError, StorageError, TraversalError};

/// Convert Interstellar errors to Python exceptions
pub trait IntoPyErr {
    fn into_py_err(self) -> PyErr;
}

impl IntoPyErr for StorageError {
    fn into_py_err(self) -> PyErr {
        match self {
            StorageError::VertexNotFound(id) => {
                PyKeyError::new_err(format!("Vertex not found: {:?}", id))
            }
            StorageError::EdgeNotFound(id) => {
                PyKeyError::new_err(format!("Edge not found: {:?}", id))
            }
            StorageError::Io(e) => PyRuntimeError::new_err(format!("I/O error: {}", e)),
            StorageError::InvalidFormat => PyValueError::new_err("Invalid data format"),
            StorageError::CorruptedData => PyRuntimeError::new_err("Corrupted data detected"),
            StorageError::OutOfSpace => PyRuntimeError::new_err("Storage out of space"),
            StorageError::IndexError(msg) => PyRuntimeError::new_err(format!("Index error: {}", msg)),
            _ => PyRuntimeError::new_err(self.to_string()),
        }
    }
}

impl IntoPyErr for TraversalError {
    fn into_py_err(self) -> PyErr {
        match self {
            TraversalError::NotOne(count) => {
                PyValueError::new_err(format!("Expected exactly one result, got {}", count))
            }
            TraversalError::Storage(e) => e.into_py_err(),
            TraversalError::Mutation(e) => e.into_py_err(),
        }
    }
}

impl IntoPyErr for MutationError {
    fn into_py_err(self) -> PyErr {
        PyRuntimeError::new_err(self.to_string())
    }
}

/// Extension trait for Result types
pub trait ResultExt<T> {
    fn to_py(self) -> PyResult<T>;
}

impl<T, E: IntoPyErr> ResultExt<T> for std::result::Result<T, E> {
    fn to_py(self) -> PyResult<T> {
        self.map_err(|e| e.into_py_err())
    }
}
```

---

## 5. Graph Implementation (graph.rs)

```rust
use pyo3::prelude::*;
use pyo3::types::PyDict;
use interstellar::{Graph, Value, VertexId, EdgeId};
use std::sync::Arc;

use crate::error::ResultExt;
use crate::traversal::PyTraversal;
use crate::value::{py_to_value, py_to_properties, value_to_py, properties_to_py};

/// A high-performance in-memory graph database.
///
/// Example:
///     >>> from interstellar import Graph
///     >>> g = Graph()
///     >>> alice = g.add_vertex("person", {"name": "Alice", "age": 30})
///     >>> bob = g.add_vertex("person", {"name": "Bob", "age": 25})
///     >>> g.add_edge(alice, bob, "knows", {"since": 2020})
#[pyclass(name = "Graph")]
pub struct PyGraph {
    inner: Arc<Graph>,
}

#[pymethods]
impl PyGraph {
    /// Create a new empty in-memory graph.
    #[new]
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
    /// Args:
    ///     label: The vertex label (e.g., "person", "product")
    ///     properties: Optional dict of key-value properties
    ///
    /// Returns:
    ///     The new vertex's ID as an integer
    ///
    /// Example:
    ///     >>> g = Graph()
    ///     >>> alice = g.add_vertex("person", {"name": "Alice", "age": 30})
    #[pyo3(signature = (label, properties=None))]
    pub fn add_vertex(
        &self,
        label: &str,
        properties: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<u64> {
        let props = py_to_properties(properties)?;
        let id = self.inner.add_vertex(label, props);
        Ok(id.0)
    }

    /// Get a vertex by ID.
    ///
    /// Args:
    ///     id: The vertex ID
    ///
    /// Returns:
    ///     A dict with id, label, and properties, or None if not found
    pub fn get_vertex(&self, py: Python<'_>, id: u64) -> PyResult<Option<PyObject>> {
        let snapshot = self.inner.snapshot();
        match snapshot.get_vertex(VertexId(id)) {
            Some(vertex) => {
                let dict = PyDict::new(py);
                dict.set_item("id", vertex.id.0)?;
                dict.set_item("label", &vertex.label)?;
                dict.set_item("properties", properties_to_py(py, &vertex.properties)?)?;
                Ok(Some(dict.into()))
            }
            None => Ok(None),
        }
    }

    /// Remove a vertex and all its incident edges.
    ///
    /// Args:
    ///     id: The vertex ID to remove
    ///
    /// Returns:
    ///     True if removed, False if not found
    pub fn remove_vertex(&self, id: u64) -> PyResult<bool> {
        match self.inner.remove_vertex(VertexId(id)) {
            Ok(()) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    /// Set a property on a vertex.
    ///
    /// Args:
    ///     id: The vertex ID
    ///     key: Property name
    ///     value: Property value
    ///
    /// Raises:
    ///     KeyError: If vertex not found
    pub fn set_vertex_property(
        &self,
        id: u64,
        key: &str,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let val = py_to_value(value)?;
        self.inner
            .set_vertex_property(VertexId(id), key, val)
            .to_py()
    }

    // -------------------------------------------------------------------------
    // Edge Operations
    // -------------------------------------------------------------------------

    /// Add an edge between two vertices.
    ///
    /// Args:
    ///     from_id: Source vertex ID
    ///     to_id: Target vertex ID
    ///     label: The edge label (e.g., "knows", "purchased")
    ///     properties: Optional dict of key-value properties
    ///
    /// Returns:
    ///     The new edge's ID
    ///
    /// Raises:
    ///     KeyError: If source or target vertex not found
    #[pyo3(signature = (from_id, to_id, label, properties=None))]
    pub fn add_edge(
        &self,
        from_id: u64,
        to_id: u64,
        label: &str,
        properties: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<u64> {
        let props = py_to_properties(properties)?;
        let id = self.inner
            .add_edge(VertexId(from_id), VertexId(to_id), label, props)
            .to_py()?;
        Ok(id.0)
    }

    /// Get an edge by ID.
    ///
    /// Args:
    ///     id: The edge ID
    ///
    /// Returns:
    ///     A dict with id, label, from_id, to_id, and properties, or None
    pub fn get_edge(&self, py: Python<'_>, id: u64) -> PyResult<Option<PyObject>> {
        let snapshot = self.inner.snapshot();
        match snapshot.get_edge(EdgeId(id)) {
            Some(edge) => {
                let dict = PyDict::new(py);
                dict.set_item("id", edge.id.0)?;
                dict.set_item("label", &edge.label)?;
                dict.set_item("from_id", edge.out_v.0)?;
                dict.set_item("to_id", edge.in_v.0)?;
                dict.set_item("properties", properties_to_py(py, &edge.properties)?)?;
                Ok(Some(dict.into()))
            }
            None => Ok(None),
        }
    }

    /// Remove an edge.
    ///
    /// Args:
    ///     id: The edge ID to remove
    ///
    /// Returns:
    ///     True if removed, False if not found
    pub fn remove_edge(&self, id: u64) -> PyResult<bool> {
        match self.inner.remove_edge(EdgeId(id)) {
            Ok(()) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    /// Set a property on an edge.
    ///
    /// Args:
    ///     id: The edge ID
    ///     key: Property name
    ///     value: Property value
    ///
    /// Raises:
    ///     KeyError: If edge not found
    pub fn set_edge_property(
        &self,
        id: u64,
        key: &str,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let val = py_to_value(value)?;
        self.inner
            .set_edge_property(EdgeId(id), key, val)
            .to_py()
    }

    // -------------------------------------------------------------------------
    // Graph Statistics
    // -------------------------------------------------------------------------

    /// Get the total number of vertices.
    #[getter]
    pub fn vertex_count(&self) -> u64 {
        self.inner.vertex_count() as u64
    }

    /// Get the total number of edges.
    #[getter]
    pub fn edge_count(&self) -> u64 {
        self.inner.edge_count() as u64
    }

    /// Get the current version/transaction ID.
    #[getter]
    pub fn version(&self) -> u64 {
        self.inner.version()
    }

    // -------------------------------------------------------------------------
    // Traversal Entry Points
    // -------------------------------------------------------------------------

    /// Start a traversal from all vertices.
    ///
    /// Returns:
    ///     A new traversal starting from all vertices
    ///
    /// Example:
    ///     >>> names = g.V().has_label("person").values("name").to_list()
    #[pyo3(name = "V")]
    pub fn v(&self) -> PyTraversal {
        PyTraversal::from_all_vertices(Arc::clone(&self.inner))
    }

    /// Start a traversal from specific vertex IDs.
    ///
    /// Args:
    ///     *ids: Vertex IDs to start from
    #[pyo3(name = "V_", signature = (*ids))]
    pub fn v_ids(&self, ids: Vec<u64>) -> PyTraversal {
        let vertex_ids: Vec<VertexId> = ids.into_iter().map(VertexId).collect();
        PyTraversal::from_vertex_ids(Arc::clone(&self.inner), vertex_ids)
    }

    /// Start a traversal from all edges.
    #[pyo3(name = "E")]
    pub fn e(&self) -> PyTraversal {
        PyTraversal::from_all_edges(Arc::clone(&self.inner))
    }

    /// Start a traversal from specific edge IDs.
    ///
    /// Args:
    ///     *ids: Edge IDs to start from
    #[pyo3(name = "E_", signature = (*ids))]
    pub fn e_ids(&self, ids: Vec<u64>) -> PyTraversal {
        let edge_ids: Vec<EdgeId> = ids.into_iter().map(EdgeId).collect();
        PyTraversal::from_edge_ids(Arc::clone(&self.inner), edge_ids)
    }

    // -------------------------------------------------------------------------
    // Serialization
    // -------------------------------------------------------------------------

    /// Export the graph to a GraphSON JSON string.
    ///
    /// Returns:
    ///     GraphSON 3.0 formatted JSON string
    pub fn to_graphson(&self) -> PyResult<String> {
        let snapshot = self.inner.snapshot();
        interstellar::graphson::to_graphson_string(&snapshot)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    /// Import graph data from a GraphSON JSON string.
    ///
    /// Args:
    ///     json: GraphSON 3.0 formatted JSON string
    pub fn from_graphson(&self, json: &str) -> PyResult<()> {
        interstellar::graphson::from_graphson_string(&self.inner, json)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    /// Clear all vertices and edges from the graph.
    pub fn clear(&self) {
        self.inner.clear();
    }

    // -------------------------------------------------------------------------
    // GQL Query Language
    // -------------------------------------------------------------------------

    /// Execute a GQL query string.
    ///
    /// Args:
    ///     query: GQL query string
    ///
    /// Returns:
    ///     Query results as a list
    ///
    /// Example:
    ///     >>> results = g.gql('''
    ///     ...     MATCH (p:person)-[:knows]->(friend)
    ///     ...     WHERE p.name = 'Alice'
    ///     ...     RETURN friend.name
    ///     ... ''')
    pub fn gql(&self, py: Python<'_>, query: &str) -> PyResult<Vec<PyObject>> {
        let results = self.inner
            .gql(query)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        
        results
            .iter()
            .map(|v| value_to_py(py, v))
            .collect()
    }

    // -------------------------------------------------------------------------
    // Python Protocol Methods
    // -------------------------------------------------------------------------

    fn __repr__(&self) -> String {
        format!(
            "Graph(vertices={}, edges={}, version={})",
            self.inner.vertex_count(),
            self.inner.edge_count(),
            self.inner.version()
        )
    }

    fn __len__(&self) -> usize {
        self.inner.vertex_count()
    }

    /// Support: `vertex_id in graph`
    fn __contains__(&self, id: u64) -> bool {
        let snapshot = self.inner.snapshot();
        snapshot.get_vertex(VertexId(id)).is_some()
    }
}

impl Default for PyGraph {
    fn default() -> Self {
        Self::new()
    }
}
```

---

## 6. Traversal Implementation (traversal.rs)

```rust
use pyo3::prelude::*;
use pyo3::types::PyList;
use interstellar::{Graph, Value, VertexId, EdgeId};
use std::sync::Arc;

use crate::error::ResultExt;
use crate::value::{py_to_value, value_to_py};
use crate::predicate::PyPredicate;
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
    HasId(Vec<u64>),
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
    ValueMap(Vec<String>, bool),
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
    
    // Order
    OrderAsc,
    OrderDesc,
    OrderByKeyAsc(String),
    OrderByKeyDesc(String),
    
    // Group
    GroupByLabel,
    GroupByKey(String),
    GroupCount,
    GroupCountByKey(String),
    
    // Mutation
    AddV(String),
    AddE(String),
    Property(String, Value),
    From(String),
    FromId(u64),
    To(String),
    ToId(u64),
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
#[pyclass(name = "Traversal")]
#[derive(Clone)]
pub struct PyTraversal {
    graph: Arc<Graph>,
    steps: Vec<TraversalStep>,
}

impl PyTraversal {
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

#[pymethods]
impl PyTraversal {
    // -------------------------------------------------------------------------
    // Navigation Steps
    // -------------------------------------------------------------------------

    /// Navigate to outgoing adjacent vertices.
    ///
    /// Args:
    ///     *labels: Optional edge labels to traverse
    #[pyo3(signature = (*labels))]
    pub fn out(&self, labels: Vec<String>) -> PyTraversal {
        self.with_step(TraversalStep::Out(labels))
    }

    /// Navigate to incoming adjacent vertices.
    ///
    /// Args:
    ///     *labels: Optional edge labels to traverse
    #[pyo3(name = "in_", signature = (*labels))]
    pub fn in_(&self, labels: Vec<String>) -> PyTraversal {
        self.with_step(TraversalStep::In(labels))
    }

    /// Navigate to adjacent vertices in both directions.
    ///
    /// Args:
    ///     *labels: Optional edge labels to traverse
    #[pyo3(signature = (*labels))]
    pub fn both(&self, labels: Vec<String>) -> PyTraversal {
        self.with_step(TraversalStep::Both(labels))
    }

    /// Navigate to outgoing edges.
    ///
    /// Args:
    ///     *labels: Optional edge labels to match
    #[pyo3(name = "outE", signature = (*labels))]
    pub fn out_e(&self, labels: Vec<String>) -> PyTraversal {
        self.with_step(TraversalStep::OutE(labels))
    }

    /// Navigate to incoming edges.
    ///
    /// Args:
    ///     *labels: Optional edge labels to match
    #[pyo3(name = "inE", signature = (*labels))]
    pub fn in_e(&self, labels: Vec<String>) -> PyTraversal {
        self.with_step(TraversalStep::InE(labels))
    }

    /// Navigate to edges in both directions.
    ///
    /// Args:
    ///     *labels: Optional edge labels to match
    #[pyo3(name = "bothE", signature = (*labels))]
    pub fn both_e(&self, labels: Vec<String>) -> PyTraversal {
        self.with_step(TraversalStep::BothE(labels))
    }

    /// Navigate from an edge to its source vertex.
    #[pyo3(name = "outV")]
    pub fn out_v(&self) -> PyTraversal {
        self.with_step(TraversalStep::OutV)
    }

    /// Navigate from an edge to its target vertex.
    #[pyo3(name = "inV")]
    pub fn in_v(&self) -> PyTraversal {
        self.with_step(TraversalStep::InV)
    }

    /// Navigate from an edge to both endpoints.
    #[pyo3(name = "bothV")]
    pub fn both_v(&self) -> PyTraversal {
        self.with_step(TraversalStep::BothV)
    }

    /// Navigate to the vertex that was NOT the previous step.
    #[pyo3(name = "otherV")]
    pub fn other_v(&self) -> PyTraversal {
        self.with_step(TraversalStep::OtherV)
    }

    // -------------------------------------------------------------------------
    // Filter Steps
    // -------------------------------------------------------------------------

    /// Filter to elements with a specific label.
    ///
    /// Args:
    ///     label: The label to match
    #[pyo3(name = "has_label")]
    pub fn has_label(&self, label: &str) -> PyTraversal {
        self.with_step(TraversalStep::HasLabel(vec![label.to_string()]))
    }

    /// Filter to elements with any of the specified labels.
    ///
    /// Args:
    ///     *labels: Labels to match (OR logic)
    #[pyo3(name = "has_label_any", signature = (*labels))]
    pub fn has_label_any(&self, labels: Vec<String>) -> PyTraversal {
        self.with_step(TraversalStep::HasLabel(labels))
    }

    /// Filter to elements that have a property (any value).
    ///
    /// Args:
    ///     key: Property name
    pub fn has(&self, key: &str) -> PyTraversal {
        self.with_step(TraversalStep::Has(key.to_string()))
    }

    /// Filter to elements that have a property with a specific value.
    ///
    /// Args:
    ///     key: Property name
    ///     value: Exact value to match
    #[pyo3(name = "has_value")]
    pub fn has_value(&self, key: &str, value: &Bound<'_, PyAny>) -> PyResult<PyTraversal> {
        let val = py_to_value(value)?;
        Ok(self.with_step(TraversalStep::HasValue(key.to_string(), val)))
    }

    /// Filter to elements where property matches a predicate.
    ///
    /// Args:
    ///     key: Property name
    ///     predicate: Predicate to test (e.g., P.gt(10))
    #[pyo3(name = "has_where")]
    pub fn has_where(&self, key: &str, predicate: &PyPredicate) -> PyTraversal {
        self.with_step(TraversalStep::HasPredicate(key.to_string(), predicate.config.clone()))
    }

    /// Filter to elements that do NOT have a property.
    ///
    /// Args:
    ///     key: Property name that must be absent
    #[pyo3(name = "has_not")]
    pub fn has_not(&self, key: &str) -> PyTraversal {
        self.with_step(TraversalStep::HasNot(key.to_string()))
    }

    /// Filter to elements with specific IDs.
    ///
    /// Args:
    ///     *ids: Element IDs to match
    #[pyo3(name = "has_id", signature = (*ids))]
    pub fn has_id(&self, ids: Vec<u64>) -> PyTraversal {
        self.with_step(TraversalStep::HasId(ids))
    }

    /// Remove duplicate elements from the traversal.
    pub fn dedup(&self) -> PyTraversal {
        self.with_step(TraversalStep::Dedup)
    }

    /// Remove duplicates based on a property key.
    ///
    /// Args:
    ///     key: Property to deduplicate by
    #[pyo3(name = "dedup_by_key")]
    pub fn dedup_by_key(&self, key: &str) -> PyTraversal {
        self.with_step(TraversalStep::DedupByKey(key.to_string()))
    }

    /// Limit results to the first n elements.
    ///
    /// Args:
    ///     n: Maximum number of elements
    pub fn limit(&self, n: usize) -> PyTraversal {
        self.with_step(TraversalStep::Limit(n))
    }

    /// Skip the first n elements.
    ///
    /// Args:
    ///     n: Number of elements to skip
    pub fn skip(&self, n: usize) -> PyTraversal {
        self.with_step(TraversalStep::Skip(n))
    }

    /// Take elements in a range [start, end).
    ///
    /// Args:
    ///     start: Start index (inclusive)
    ///     end: End index (exclusive)
    pub fn range(&self, start: usize, end: usize) -> PyTraversal {
        self.with_step(TraversalStep::Range(start, end))
    }

    /// Filter to paths that don't repeat vertices.
    #[pyo3(name = "simple_path")]
    pub fn simple_path(&self) -> PyTraversal {
        self.with_step(TraversalStep::SimplePath)
    }

    /// Filter to paths that do repeat vertices.
    #[pyo3(name = "cyclic_path")]
    pub fn cyclic_path(&self) -> PyTraversal {
        self.with_step(TraversalStep::CyclicPath)
    }

    // -------------------------------------------------------------------------
    // Transform Steps
    // -------------------------------------------------------------------------

    /// Extract property values.
    ///
    /// Args:
    ///     *keys: Property names to extract
    #[pyo3(signature = (*keys))]
    pub fn values(&self, keys: Vec<String>) -> PyTraversal {
        self.with_step(TraversalStep::Values(keys))
    }

    /// Extract the element ID.
    pub fn id(&self) -> PyTraversal {
        self.with_step(TraversalStep::Id)
    }

    /// Extract the element label.
    pub fn label(&self) -> PyTraversal {
        self.with_step(TraversalStep::Label)
    }

    /// Get a map of property name to value.
    ///
    /// Args:
    ///     *keys: Optional specific keys to include
    #[pyo3(name = "value_map", signature = (*keys))]
    pub fn value_map(&self, keys: Vec<String>) -> PyTraversal {
        self.with_step(TraversalStep::ValueMap(keys, false))
    }

    /// Get a value map including id and label tokens.
    #[pyo3(name = "value_map_with_tokens", signature = (*keys))]
    pub fn value_map_with_tokens(&self, keys: Vec<String>) -> PyTraversal {
        self.with_step(TraversalStep::ValueMap(keys, true))
    }

    /// Get a complete element map (id, label, and all properties).
    ///
    /// Args:
    ///     *keys: Optional specific property keys to include
    #[pyo3(name = "element_map", signature = (*keys))]
    pub fn element_map(&self, keys: Vec<String>) -> PyTraversal {
        self.with_step(TraversalStep::ElementMap(keys))
    }

    /// Replace each element with a constant value.
    ///
    /// Args:
    ///     value: Constant value to emit
    pub fn constant(&self, value: &Bound<'_, PyAny>) -> PyResult<PyTraversal> {
        let val = py_to_value(value)?;
        Ok(self.with_step(TraversalStep::Constant(val)))
    }

    /// Flatten lists in the stream.
    pub fn unfold(&self) -> PyTraversal {
        self.with_step(TraversalStep::Unfold)
    }

    /// Collect all elements into a single list.
    pub fn fold(&self) -> PyTraversal {
        self.with_step(TraversalStep::Fold)
    }

    /// Get the traversal path (history of elements visited).
    pub fn path(&self) -> PyTraversal {
        self.with_step(TraversalStep::Path)
    }

    /// Label the current step for later reference.
    ///
    /// Args:
    ///     label: Step label
    #[pyo3(name = "as_")]
    pub fn as_(&self, label: &str) -> PyTraversal {
        self.with_step(TraversalStep::As(label.to_string()))
    }

    /// Select labeled steps from the path.
    ///
    /// Args:
    ///     *labels: Step labels to select
    #[pyo3(signature = (*labels))]
    pub fn select(&self, labels: Vec<String>) -> PyTraversal {
        self.with_step(TraversalStep::Select(labels))
    }

    /// Count the number of elements.
    pub fn count(&self) -> PyTraversal {
        self.with_step(TraversalStep::Count)
    }

    /// Calculate the sum of numeric values.
    pub fn sum(&self) -> PyTraversal {
        self.with_step(TraversalStep::Sum)
    }

    /// Calculate the arithmetic mean of numeric values.
    pub fn mean(&self) -> PyTraversal {
        self.with_step(TraversalStep::Mean)
    }

    /// Get the minimum value.
    pub fn min(&self) -> PyTraversal {
        self.with_step(TraversalStep::Min)
    }

    /// Get the maximum value.
    pub fn max(&self) -> PyTraversal {
        self.with_step(TraversalStep::Max)
    }

    // -------------------------------------------------------------------------
    // Order Steps
    // -------------------------------------------------------------------------

    /// Order by natural value (ascending).
    #[pyo3(name = "order_asc")]
    pub fn order_asc(&self) -> PyTraversal {
        self.with_step(TraversalStep::OrderAsc)
    }

    /// Order by natural value (descending).
    #[pyo3(name = "order_desc")]
    pub fn order_desc(&self) -> PyTraversal {
        self.with_step(TraversalStep::OrderDesc)
    }

    /// Order by a property key (ascending).
    ///
    /// Args:
    ///     key: Property name
    #[pyo3(name = "order_by_key_asc")]
    pub fn order_by_key_asc(&self, key: &str) -> PyTraversal {
        self.with_step(TraversalStep::OrderByKeyAsc(key.to_string()))
    }

    /// Order by a property key (descending).
    ///
    /// Args:
    ///     key: Property name
    #[pyo3(name = "order_by_key_desc")]
    pub fn order_by_key_desc(&self, key: &str) -> PyTraversal {
        self.with_step(TraversalStep::OrderByKeyDesc(key.to_string()))
    }

    // -------------------------------------------------------------------------
    // Group Steps
    // -------------------------------------------------------------------------

    /// Group elements by label.
    #[pyo3(name = "group_by_label")]
    pub fn group_by_label(&self) -> PyTraversal {
        self.with_step(TraversalStep::GroupByLabel)
    }

    /// Group elements by a property key.
    ///
    /// Args:
    ///     key: Property name
    #[pyo3(name = "group_by_key")]
    pub fn group_by_key(&self, key: &str) -> PyTraversal {
        self.with_step(TraversalStep::GroupByKey(key.to_string()))
    }

    /// Count elements by group (label).
    #[pyo3(name = "group_count")]
    pub fn group_count(&self) -> PyTraversal {
        self.with_step(TraversalStep::GroupCount)
    }

    /// Count elements by a property key.
    ///
    /// Args:
    ///     key: Property name
    #[pyo3(name = "group_count_by_key")]
    pub fn group_count_by_key(&self, key: &str) -> PyTraversal {
        self.with_step(TraversalStep::GroupCountByKey(key.to_string()))
    }

    // -------------------------------------------------------------------------
    // Branch Steps
    // -------------------------------------------------------------------------

    /// Execute multiple traversals and combine results.
    ///
    /// Args:
    ///     *traversals: Traversals to execute in parallel
    #[pyo3(signature = (*traversals))]
    pub fn union(&self, traversals: Vec<PyTraversal>) -> PyTraversal {
        let step_lists: Vec<Vec<TraversalStep>> = traversals
            .into_iter()
            .map(|t| t.steps)
            .collect();
        self.with_step(TraversalStep::Union(step_lists))
    }

    /// Return the result of the first traversal that produces output.
    ///
    /// Args:
    ///     *traversals: Traversals to try in order
    #[pyo3(signature = (*traversals))]
    pub fn coalesce(&self, traversals: Vec<PyTraversal>) -> PyTraversal {
        let step_lists: Vec<Vec<TraversalStep>> = traversals
            .into_iter()
            .map(|t| t.steps)
            .collect();
        self.with_step(TraversalStep::Coalesce(step_lists))
    }

    /// Execute traversal, but pass through original if no results.
    ///
    /// Args:
    ///     traversal: Optional traversal
    pub fn optional(&self, traversal: &PyTraversal) -> PyTraversal {
        self.with_step(TraversalStep::Optional(Box::new(traversal.steps.clone())))
    }

    /// Execute traversal in local scope (per element).
    ///
    /// Args:
    ///     traversal: Traversal to execute locally
    pub fn local(&self, traversal: &PyTraversal) -> PyTraversal {
        self.with_step(TraversalStep::Local(Box::new(traversal.steps.clone())))
    }

    // -------------------------------------------------------------------------
    // Mutation Steps
    // -------------------------------------------------------------------------

    /// Set a property on the current element.
    ///
    /// Args:
    ///     key: Property name
    ///     value: Property value
    pub fn property(&self, key: &str, value: &Bound<'_, PyAny>) -> PyResult<PyTraversal> {
        let val = py_to_value(value)?;
        Ok(self.with_step(TraversalStep::Property(key.to_string(), val)))
    }

    /// Remove the current element from the graph.
    pub fn drop(&self) -> PyTraversal {
        self.with_step(TraversalStep::Drop)
    }

    // -------------------------------------------------------------------------
    // Terminal Steps
    // -------------------------------------------------------------------------

    /// Execute the traversal and return all results as a list.
    ///
    /// Returns:
    ///     List of results
    #[pyo3(name = "to_list")]
    pub fn to_list(&self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        // Release GIL during execution for long traversals
        let results = py.allow_threads(|| self.execute())?;
        results
            .into_iter()
            .map(|v| value_to_py(py, &v))
            .collect()
    }

    /// Execute and return the first result, or None.
    ///
    /// Returns:
    ///     First result or None
    pub fn first(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let results = py.allow_threads(|| self.execute())?;
        match results.into_iter().next() {
            Some(v) => Ok(Some(value_to_py(py, &v)?)),
            None => Ok(None),
        }
    }

    /// Execute and return exactly one result.
    ///
    /// Raises:
    ///     ValueError: If zero or more than one result
    pub fn one(&self, py: Python<'_>) -> PyResult<PyObject> {
        let results = py.allow_threads(|| self.execute())?;
        if results.len() != 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Expected exactly one result, got {}",
                results.len()
            )));
        }
        value_to_py(py, &results[0])
    }

    /// Check if the traversal has any results.
    ///
    /// Returns:
    ///     True if at least one result exists
    #[pyo3(name = "has_next")]
    pub fn has_next(&self, py: Python<'_>) -> PyResult<bool> {
        let results = py.allow_threads(|| self.execute())?;
        Ok(!results.is_empty())
    }

    /// Execute and return the count of results.
    ///
    /// Returns:
    ///     Number of results
    #[pyo3(name = "to_count")]
    pub fn to_count(&self, py: Python<'_>) -> PyResult<u64> {
        let results = py.allow_threads(|| self.execute())?;
        Ok(results.len() as u64)
    }

    /// Iterate through all results (for side effects like drop).
    pub fn iterate(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| self.execute())?;
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Python Protocol Methods
    // -------------------------------------------------------------------------

    fn __repr__(&self) -> String {
        format!("Traversal(steps={})", self.steps.len())
    }

    /// Support: `for item in traversal`
    fn __iter__(slf: PyRef<'_, Self>) -> PyResult<PyTraversalIterator> {
        let results = slf.execute()?;
        Ok(PyTraversalIterator {
            results,
            index: 0,
        })
    }

    /// Support: `len(traversal)` - executes traversal
    fn __len__(&self, py: Python<'_>) -> PyResult<usize> {
        let results = py.allow_threads(|| self.execute())?;
        Ok(results.len())
    }

    // -------------------------------------------------------------------------
    // Internal Execution
    // -------------------------------------------------------------------------

    fn execute(&self) -> PyResult<Vec<Value>> {
        // Build and execute the actual Rust traversal from recorded steps
        // This is a simplified implementation - the real version would
        // translate each step to the corresponding Rust traversal API
        
        // For now, return a placeholder that shows the pattern
        // The actual implementation would build a BoundTraversal
        // and execute it against the graph snapshot
        
        todo!("Implement traversal execution from steps")
    }
}

/// Iterator for traversal results
#[pyclass]
pub struct PyTraversalIterator {
    results: Vec<Value>,
    index: usize,
}

#[pymethods]
impl PyTraversalIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        if self.index < self.results.len() {
            let value = &self.results[self.index];
            self.index += 1;
            Ok(Some(value_to_py(py, value)?))
        } else {
            Ok(None)
        }
    }
}
```

---

## 7. Predicate System (predicate.rs)

```rust
use pyo3::prelude::*;
use interstellar::Value;

use crate::traversal::PredicateConfig;
use crate::value::py_to_value;

/// A predicate for filtering values.
#[pyclass(name = "Predicate")]
#[derive(Clone)]
pub struct PyPredicate {
    pub(crate) config: PredicateConfig,
}

impl PyPredicate {
    pub fn new(config: PredicateConfig) -> Self {
        Self { config }
    }
}

#[pymethods]
impl PyPredicate {
    fn __repr__(&self) -> String {
        "Predicate(...)".to_string()
    }

    /// Combine predicates with AND.
    pub fn and_(&self, other: &PyPredicate) -> PyPredicate {
        PyPredicate::new(PredicateConfig::And(
            Box::new(self.config.clone()),
            Box::new(other.config.clone()),
        ))
    }

    /// Combine predicates with OR.
    pub fn or_(&self, other: &PyPredicate) -> PyPredicate {
        PyPredicate::new(PredicateConfig::Or(
            Box::new(self.config.clone()),
            Box::new(other.config.clone()),
        ))
    }

    /// Negate this predicate.
    pub fn not_(&self) -> PyPredicate {
        PyPredicate::new(PredicateConfig::Not(Box::new(self.config.clone())))
    }
}

/// P namespace - predicate factory functions
#[pyclass(name = "P")]
pub struct PyP;

/// Register P module functions
pub fn register_p_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    /// Equals comparison.
    #[pyfunction]
    fn eq(value: &Bound<'_, PyAny>) -> PyResult<PyPredicate> {
        let val = py_to_value(value)?;
        Ok(PyPredicate::new(PredicateConfig::Eq(val)))
    }

    /// Not equals comparison.
    #[pyfunction]
    fn neq(value: &Bound<'_, PyAny>) -> PyResult<PyPredicate> {
        let val = py_to_value(value)?;
        Ok(PyPredicate::new(PredicateConfig::Neq(val)))
    }

    /// Less than comparison.
    #[pyfunction]
    fn lt(value: &Bound<'_, PyAny>) -> PyResult<PyPredicate> {
        let val = py_to_value(value)?;
        Ok(PyPredicate::new(PredicateConfig::Lt(val)))
    }

    /// Less than or equal comparison.
    #[pyfunction]
    fn lte(value: &Bound<'_, PyAny>) -> PyResult<PyPredicate> {
        let val = py_to_value(value)?;
        Ok(PyPredicate::new(PredicateConfig::Lte(val)))
    }

    /// Greater than comparison.
    #[pyfunction]
    fn gt(value: &Bound<'_, PyAny>) -> PyResult<PyPredicate> {
        let val = py_to_value(value)?;
        Ok(PyPredicate::new(PredicateConfig::Gt(val)))
    }

    /// Greater than or equal comparison.
    #[pyfunction]
    fn gte(value: &Bound<'_, PyAny>) -> PyResult<PyPredicate> {
        let val = py_to_value(value)?;
        Ok(PyPredicate::new(PredicateConfig::Gte(val)))
    }

    /// Value is between start and end (inclusive).
    #[pyfunction]
    fn between(start: &Bound<'_, PyAny>, end: &Bound<'_, PyAny>) -> PyResult<PyPredicate> {
        let s = py_to_value(start)?;
        let e = py_to_value(end)?;
        Ok(PyPredicate::new(PredicateConfig::Between(s, e)))
    }

    /// Value is within the given set.
    #[pyfunction]
    #[pyo3(signature = (*values))]
    fn within(values: Vec<Bound<'_, PyAny>>) -> PyResult<PyPredicate> {
        let vals: PyResult<Vec<Value>> = values.iter().map(|v| py_to_value(v)).collect();
        Ok(PyPredicate::new(PredicateConfig::Within(vals?)))
    }

    /// Value is NOT within the given set.
    #[pyfunction]
    #[pyo3(signature = (*values))]
    fn without(values: Vec<Bound<'_, PyAny>>) -> PyResult<PyPredicate> {
        let vals: PyResult<Vec<Value>> = values.iter().map(|v| py_to_value(v)).collect();
        Ok(PyPredicate::new(PredicateConfig::Without(vals?)))
    }

    /// String contains substring.
    #[pyfunction]
    fn containing(substring: &str) -> PyPredicate {
        PyPredicate::new(PredicateConfig::Containing(substring.to_string()))
    }

    /// String starts with prefix.
    #[pyfunction]
    fn starting_with(prefix: &str) -> PyPredicate {
        PyPredicate::new(PredicateConfig::StartingWith(prefix.to_string()))
    }

    /// String ends with suffix.
    #[pyfunction]
    fn ending_with(suffix: &str) -> PyPredicate {
        PyPredicate::new(PredicateConfig::EndingWith(suffix.to_string()))
    }

    /// String matches regular expression.
    #[pyfunction]
    fn regex(pattern: &str) -> PyPredicate {
        PyPredicate::new(PredicateConfig::Regex(pattern.to_string()))
    }

    m.add_function(wrap_pyfunction!(eq, m)?)?;
    m.add_function(wrap_pyfunction!(neq, m)?)?;
    m.add_function(wrap_pyfunction!(lt, m)?)?;
    m.add_function(wrap_pyfunction!(lte, m)?)?;
    m.add_function(wrap_pyfunction!(gt, m)?)?;
    m.add_function(wrap_pyfunction!(gte, m)?)?;
    m.add_function(wrap_pyfunction!(between, m)?)?;
    m.add_function(wrap_pyfunction!(within, m)?)?;
    m.add_function(wrap_pyfunction!(without, m)?)?;
    m.add_function(wrap_pyfunction!(containing, m)?)?;
    m.add_function(wrap_pyfunction!(starting_with, m)?)?;
    m.add_function(wrap_pyfunction!(ending_with, m)?)?;
    m.add_function(wrap_pyfunction!(regex, m)?)?;

    Ok(())
}
```

---

## 8. Anonymous Traversals (anonymous.rs)

```rust
use pyo3::prelude::*;
use interstellar::{Graph, VertexId, EdgeId};
use std::sync::Arc;

use crate::traversal::{PyTraversal, TraversalStep};
use crate::predicate::PyPredicate;
use crate::value::py_to_value;

/// Register __ module functions (anonymous traversals)
pub fn register_anon_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    /// Start an anonymous traversal (identity).
    #[pyfunction]
    fn start() -> PyTraversal {
        PyTraversal::anonymous(vec![])
    }

    /// Navigate to outgoing adjacent vertices.
    #[pyfunction]
    #[pyo3(signature = (*labels))]
    fn out(labels: Vec<String>) -> PyTraversal {
        PyTraversal::anonymous(vec![TraversalStep::Out(labels)])
    }

    /// Navigate to incoming adjacent vertices.
    #[pyfunction]
    #[pyo3(name = "in_", signature = (*labels))]
    fn in_(labels: Vec<String>) -> PyTraversal {
        PyTraversal::anonymous(vec![TraversalStep::In(labels)])
    }

    /// Navigate to adjacent vertices in both directions.
    #[pyfunction]
    #[pyo3(signature = (*labels))]
    fn both(labels: Vec<String>) -> PyTraversal {
        PyTraversal::anonymous(vec![TraversalStep::Both(labels)])
    }

    /// Navigate to outgoing edges.
    #[pyfunction]
    #[pyo3(name = "outE", signature = (*labels))]
    fn out_e(labels: Vec<String>) -> PyTraversal {
        PyTraversal::anonymous(vec![TraversalStep::OutE(labels)])
    }

    /// Navigate to incoming edges.
    #[pyfunction]
    #[pyo3(name = "inE", signature = (*labels))]
    fn in_e(labels: Vec<String>) -> PyTraversal {
        PyTraversal::anonymous(vec![TraversalStep::InE(labels)])
    }

    /// Navigate to edges in both directions.
    #[pyfunction]
    #[pyo3(name = "bothE", signature = (*labels))]
    fn both_e(labels: Vec<String>) -> PyTraversal {
        PyTraversal::anonymous(vec![TraversalStep::BothE(labels)])
    }

    /// Navigate from an edge to its source vertex.
    #[pyfunction]
    #[pyo3(name = "outV")]
    fn out_v() -> PyTraversal {
        PyTraversal::anonymous(vec![TraversalStep::OutV])
    }

    /// Navigate from an edge to its target vertex.
    #[pyfunction]
    #[pyo3(name = "inV")]
    fn in_v() -> PyTraversal {
        PyTraversal::anonymous(vec![TraversalStep::InV])
    }

    /// Filter to elements with a specific label.
    #[pyfunction]
    #[pyo3(name = "has_label")]
    fn has_label(label: &str) -> PyTraversal {
        PyTraversal::anonymous(vec![TraversalStep::HasLabel(vec![label.to_string()])])
    }

    /// Filter to elements that have a property with a specific value.
    #[pyfunction]
    #[pyo3(name = "has_value")]
    fn has_value(key: &str, value: &Bound<'_, PyAny>) -> PyResult<PyTraversal> {
        let val = py_to_value(value)?;
        Ok(PyTraversal::anonymous(vec![TraversalStep::HasValue(key.to_string(), val)]))
    }

    /// Filter to elements where property matches a predicate.
    #[pyfunction]
    #[pyo3(name = "has_where")]
    fn has_where(key: &str, predicate: &PyPredicate) -> PyTraversal {
        PyTraversal::anonymous(vec![TraversalStep::HasPredicate(key.to_string(), predicate.config.clone())])
    }

    /// Extract property values.
    #[pyfunction]
    #[pyo3(signature = (*keys))]
    fn values(keys: Vec<String>) -> PyTraversal {
        PyTraversal::anonymous(vec![TraversalStep::Values(keys)])
    }

    /// Extract the element ID.
    #[pyfunction]
    fn id() -> PyTraversal {
        PyTraversal::anonymous(vec![TraversalStep::Id])
    }

    /// Extract the element label.
    #[pyfunction]
    fn label() -> PyTraversal {
        PyTraversal::anonymous(vec![TraversalStep::Label])
    }

    /// Remove duplicate elements.
    #[pyfunction]
    fn dedup() -> PyTraversal {
        PyTraversal::anonymous(vec![TraversalStep::Dedup])
    }

    /// Limit results.
    #[pyfunction]
    fn limit(n: usize) -> PyTraversal {
        PyTraversal::anonymous(vec![TraversalStep::Limit(n)])
    }

    /// Count the number of elements.
    #[pyfunction]
    fn count() -> PyTraversal {
        PyTraversal::anonymous(vec![TraversalStep::Count])
    }

    /// Flatten lists in the stream.
    #[pyfunction]
    fn unfold() -> PyTraversal {
        PyTraversal::anonymous(vec![TraversalStep::Unfold])
    }

    /// Collect all elements into a single list.
    #[pyfunction]
    fn fold() -> PyTraversal {
        PyTraversal::anonymous(vec![TraversalStep::Fold])
    }

    /// Filter to paths that don't repeat vertices.
    #[pyfunction]
    #[pyo3(name = "simple_path")]
    fn simple_path() -> PyTraversal {
        PyTraversal::anonymous(vec![TraversalStep::SimplePath])
    }

    m.add_function(wrap_pyfunction!(start, m)?)?;
    m.add_function(wrap_pyfunction!(out, m)?)?;
    m.add_function(wrap_pyfunction!(in_, m)?)?;
    m.add_function(wrap_pyfunction!(both, m)?)?;
    m.add_function(wrap_pyfunction!(out_e, m)?)?;
    m.add_function(wrap_pyfunction!(in_e, m)?)?;
    m.add_function(wrap_pyfunction!(both_e, m)?)?;
    m.add_function(wrap_pyfunction!(out_v, m)?)?;
    m.add_function(wrap_pyfunction!(in_v, m)?)?;
    m.add_function(wrap_pyfunction!(has_label, m)?)?;
    m.add_function(wrap_pyfunction!(has_value, m)?)?;
    m.add_function(wrap_pyfunction!(has_where, m)?)?;
    m.add_function(wrap_pyfunction!(values, m)?)?;
    m.add_function(wrap_pyfunction!(id, m)?)?;
    m.add_function(wrap_pyfunction!(label, m)?)?;
    m.add_function(wrap_pyfunction!(dedup, m)?)?;
    m.add_function(wrap_pyfunction!(limit, m)?)?;
    m.add_function(wrap_pyfunction!(count, m)?)?;
    m.add_function(wrap_pyfunction!(unfold, m)?)?;
    m.add_function(wrap_pyfunction!(fold, m)?)?;
    m.add_function(wrap_pyfunction!(simple_path, m)?)?;

    Ok(())
}
```

---

## 9. Type Stubs (interstellar/__init__.pyi)

```python
"""Type stubs for the interstellar-graph package."""

from typing import Any, Dict, Iterator, List, Optional, Sequence, Union

# Type aliases
Value = Union[None, bool, int, float, str, List["Value"], Dict[str, "Value"]]
Properties = Dict[str, Value]

class Predicate:
    """A predicate for filtering values."""
    
    def and_(self, other: "Predicate") -> "Predicate":
        """Combine predicates with AND."""
        ...
    
    def or_(self, other: "Predicate") -> "Predicate":
        """Combine predicates with OR."""
        ...
    
    def not_(self) -> "Predicate":
        """Negate this predicate."""
        ...

class P:
    """Predicate factory functions."""
    
    @staticmethod
    def eq(value: Value) -> Predicate:
        """Equals comparison."""
        ...
    
    @staticmethod
    def neq(value: Value) -> Predicate:
        """Not equals comparison."""
        ...
    
    @staticmethod
    def lt(value: Value) -> Predicate:
        """Less than comparison."""
        ...
    
    @staticmethod
    def lte(value: Value) -> Predicate:
        """Less than or equal comparison."""
        ...
    
    @staticmethod
    def gt(value: Value) -> Predicate:
        """Greater than comparison."""
        ...
    
    @staticmethod
    def gte(value: Value) -> Predicate:
        """Greater than or equal comparison."""
        ...
    
    @staticmethod
    def between(start: Value, end: Value) -> Predicate:
        """Value is between start and end (inclusive)."""
        ...
    
    @staticmethod
    def within(*values: Value) -> Predicate:
        """Value is within the given set."""
        ...
    
    @staticmethod
    def without(*values: Value) -> Predicate:
        """Value is NOT within the given set."""
        ...
    
    @staticmethod
    def containing(substring: str) -> Predicate:
        """String contains substring."""
        ...
    
    @staticmethod
    def starting_with(prefix: str) -> Predicate:
        """String starts with prefix."""
        ...
    
    @staticmethod
    def ending_with(suffix: str) -> Predicate:
        """String ends with suffix."""
        ...
    
    @staticmethod
    def regex(pattern: str) -> Predicate:
        """String matches regular expression."""
        ...

class Traversal:
    """A graph traversal that can be chained with various steps."""
    
    # Navigation
    def out(self, *labels: str) -> "Traversal": ...
    def in_(self, *labels: str) -> "Traversal": ...
    def both(self, *labels: str) -> "Traversal": ...
    def outE(self, *labels: str) -> "Traversal": ...
    def inE(self, *labels: str) -> "Traversal": ...
    def bothE(self, *labels: str) -> "Traversal": ...
    def outV(self) -> "Traversal": ...
    def inV(self) -> "Traversal": ...
    def bothV(self) -> "Traversal": ...
    def otherV(self) -> "Traversal": ...
    
    # Filter
    def has_label(self, label: str) -> "Traversal": ...
    def has_label_any(self, *labels: str) -> "Traversal": ...
    def has(self, key: str) -> "Traversal": ...
    def has_value(self, key: str, value: Value) -> "Traversal": ...
    def has_where(self, key: str, predicate: Predicate) -> "Traversal": ...
    def has_not(self, key: str) -> "Traversal": ...
    def has_id(self, *ids: int) -> "Traversal": ...
    def dedup(self) -> "Traversal": ...
    def dedup_by_key(self, key: str) -> "Traversal": ...
    def limit(self, n: int) -> "Traversal": ...
    def skip(self, n: int) -> "Traversal": ...
    def range(self, start: int, end: int) -> "Traversal": ...
    def simple_path(self) -> "Traversal": ...
    def cyclic_path(self) -> "Traversal": ...
    
    # Transform
    def values(self, *keys: str) -> "Traversal": ...
    def id(self) -> "Traversal": ...
    def label(self) -> "Traversal": ...
    def value_map(self, *keys: str) -> "Traversal": ...
    def value_map_with_tokens(self, *keys: str) -> "Traversal": ...
    def element_map(self, *keys: str) -> "Traversal": ...
    def constant(self, value: Value) -> "Traversal": ...
    def unfold(self) -> "Traversal": ...
    def fold(self) -> "Traversal": ...
    def path(self) -> "Traversal": ...
    def as_(self, label: str) -> "Traversal": ...
    def select(self, *labels: str) -> "Traversal": ...
    def count(self) -> "Traversal": ...
    def sum(self) -> "Traversal": ...
    def mean(self) -> "Traversal": ...
    def min(self) -> "Traversal": ...
    def max(self) -> "Traversal": ...
    
    # Order
    def order_asc(self) -> "Traversal": ...
    def order_desc(self) -> "Traversal": ...
    def order_by_key_asc(self, key: str) -> "Traversal": ...
    def order_by_key_desc(self, key: str) -> "Traversal": ...
    
    # Group
    def group_by_label(self) -> "Traversal": ...
    def group_by_key(self, key: str) -> "Traversal": ...
    def group_count(self) -> "Traversal": ...
    def group_count_by_key(self, key: str) -> "Traversal": ...
    
    # Branch
    def union(self, *traversals: "Traversal") -> "Traversal": ...
    def coalesce(self, *traversals: "Traversal") -> "Traversal": ...
    def optional(self, traversal: "Traversal") -> "Traversal": ...
    def local(self, traversal: "Traversal") -> "Traversal": ...
    
    # Mutation
    def property(self, key: str, value: Value) -> "Traversal": ...
    def drop(self) -> "Traversal": ...
    
    # Terminal
    def to_list(self) -> List[Value]: ...
    def first(self) -> Optional[Value]: ...
    def one(self) -> Value: ...
    def has_next(self) -> bool: ...
    def to_count(self) -> int: ...
    def iterate(self) -> None: ...
    
    # Python protocols
    def __iter__(self) -> Iterator[Value]: ...
    def __len__(self) -> int: ...

class __:
    """Anonymous traversal factory."""
    
    @staticmethod
    def start() -> Traversal: ...
    @staticmethod
    def out(*labels: str) -> Traversal: ...
    @staticmethod
    def in_(*labels: str) -> Traversal: ...
    @staticmethod
    def both(*labels: str) -> Traversal: ...
    @staticmethod
    def outE(*labels: str) -> Traversal: ...
    @staticmethod
    def inE(*labels: str) -> Traversal: ...
    @staticmethod
    def bothE(*labels: str) -> Traversal: ...
    @staticmethod
    def outV() -> Traversal: ...
    @staticmethod
    def inV() -> Traversal: ...
    @staticmethod
    def has_label(label: str) -> Traversal: ...
    @staticmethod
    def has_value(key: str, value: Value) -> Traversal: ...
    @staticmethod
    def has_where(key: str, predicate: Predicate) -> Traversal: ...
    @staticmethod
    def values(*keys: str) -> Traversal: ...
    @staticmethod
    def id() -> Traversal: ...
    @staticmethod
    def label() -> Traversal: ...
    @staticmethod
    def dedup() -> Traversal: ...
    @staticmethod
    def limit(n: int) -> Traversal: ...
    @staticmethod
    def count() -> Traversal: ...
    @staticmethod
    def unfold() -> Traversal: ...
    @staticmethod
    def fold() -> Traversal: ...
    @staticmethod
    def simple_path() -> Traversal: ...

class Graph:
    """A high-performance in-memory graph database."""
    
    def __init__(self) -> None:
        """Create a new empty in-memory graph."""
        ...
    
    # Vertex operations
    def add_vertex(self, label: str, properties: Optional[Properties] = None) -> int:
        """Add a vertex with a label and optional properties."""
        ...
    
    def get_vertex(self, id: int) -> Optional[Dict[str, Any]]:
        """Get a vertex by ID."""
        ...
    
    def remove_vertex(self, id: int) -> bool:
        """Remove a vertex and all its incident edges."""
        ...
    
    def set_vertex_property(self, id: int, key: str, value: Value) -> None:
        """Set a property on a vertex."""
        ...
    
    # Edge operations
    def add_edge(
        self,
        from_id: int,
        to_id: int,
        label: str,
        properties: Optional[Properties] = None,
    ) -> int:
        """Add an edge between two vertices."""
        ...
    
    def get_edge(self, id: int) -> Optional[Dict[str, Any]]:
        """Get an edge by ID."""
        ...
    
    def remove_edge(self, id: int) -> bool:
        """Remove an edge."""
        ...
    
    def set_edge_property(self, id: int, key: str, value: Value) -> None:
        """Set a property on an edge."""
        ...
    
    # Properties
    @property
    def vertex_count(self) -> int:
        """Get the total number of vertices."""
        ...
    
    @property
    def edge_count(self) -> int:
        """Get the total number of edges."""
        ...
    
    @property
    def version(self) -> int:
        """Get the current version/transaction ID."""
        ...
    
    # Traversal
    def V(self) -> Traversal:
        """Start a traversal from all vertices."""
        ...
    
    def V_(self, *ids: int) -> Traversal:
        """Start a traversal from specific vertex IDs."""
        ...
    
    def E(self) -> Traversal:
        """Start a traversal from all edges."""
        ...
    
    def E_(self, *ids: int) -> Traversal:
        """Start a traversal from specific edge IDs."""
        ...
    
    # Serialization
    def to_graphson(self) -> str:
        """Export the graph to a GraphSON JSON string."""
        ...
    
    def from_graphson(self, json: str) -> None:
        """Import graph data from a GraphSON JSON string."""
        ...
    
    def clear(self) -> None:
        """Clear all vertices and edges from the graph."""
        ...
    
    # GQL
    def gql(self, query: str) -> List[Value]:
        """Execute a GQL query string."""
        ...
    
    # Python protocols
    def __repr__(self) -> str: ...
    def __len__(self) -> int: ...
    def __contains__(self, id: int) -> bool: ...

__version__: str
```

---

## 10. Usage Examples

### 10.1 Basic Usage

```python
from interstellar import Graph, P, __

# Create a graph
g = Graph()

# Add vertices
alice = g.add_vertex("person", {"name": "Alice", "age": 30})
bob = g.add_vertex("person", {"name": "Bob", "age": 25})
company = g.add_vertex("company", {"name": "TechCorp"})

# Add edges
g.add_edge(alice, bob, "knows", {"since": 2020})
g.add_edge(alice, company, "works_at", {"role": "Engineer"})
g.add_edge(bob, company, "works_at", {"role": "Designer"})

# Simple query
names = g.V().has_label("person").values("name").to_list()
print(names)  # ['Alice', 'Bob']

# Query with predicate
adults = g.V().has_label("person").has_where("age", P.gte(30)).to_list()
print(len(adults))  # 1

# Graph traversal
friends_of_alice = (
    g.V_(alice)
    .out("knows")
    .values("name")
    .to_list()
)
print(friends_of_alice)  # ['Bob']
```

### 10.2 Complex Queries with Anonymous Traversals

```python
from interstellar import Graph, P, __

g = Graph()
# ... populate graph ...

# Find people who know someone over 30
result = (
    g.V()
    .has_label("person")
    .where(__.out("knows").has_where("age", P.gt(30)))
    .values("name")
    .to_list()
)

# Find people connected through multiple paths
connected = (
    g.V()
    .has_value("name", "Alice")
    .union(
        __.out("knows").out("knows"),  # Friends of friends
        __.out("works_at").in_("works_at"),  # Coworkers
    )
    .dedup()
    .values("name")
    .to_list()
)

# Path finding with repeat
paths = (
    g.V_(start_id)
    .repeat(__.out().simple_path())
    .until(__.has_id(target_id))
    .path()
    .limit(5)
    .to_list()
)
```

### 10.3 Iteration Support

```python
from interstellar import Graph

g = Graph()
# ... populate graph ...

# Use in for loops
for person in g.V().has_label("person"):
    print(person)

# Use with list comprehensions
names = [v for v in g.V().has_label("person").values("name")]

# Check length
count = len(g.V().has_label("person"))

# Membership testing
vertex_id = 42
if vertex_id in g:
    print(f"Vertex {vertex_id} exists")
```

### 10.4 GQL Queries

```python
from interstellar import Graph

g = Graph()
# ... populate graph ...

# Execute GQL query
results = g.gql("""
    MATCH (p:person)-[:knows]->(friend)
    WHERE p.name = 'Alice'
    RETURN friend.name, friend.age
""")

for row in results:
    print(row)
```

---

## 11. Build and Distribution

### 11.1 Development Build

```bash
# Install maturin
pip install maturin

# Development build (editable install)
cd interstellar-py
maturin develop

# Development build with release optimizations
maturin develop --release

# Run tests
pytest tests/
```

### 11.2 Wheel Build

```bash
# Build wheel for current platform
maturin build --release

# Build wheels for multiple Python versions
maturin build --release --interpreter python3.9 python3.10 python3.11 python3.12

# Build manylinux wheels (for Linux distribution)
maturin build --release --manylinux 2_28
```

### 11.3 PyPI Publishing

```bash
# Build and upload to PyPI
maturin publish --username __token__ --password $PYPI_TOKEN

# Build and upload to TestPyPI first
maturin publish --repository testpypi
```

### 11.4 GitHub Actions CI/CD

```yaml
# .github/workflows/python.yml
name: Python Wheels

on:
  push:
    tags:
      - 'v*'
  pull_request:
    paths:
      - 'interstellar-py/**'
      - '.github/workflows/python.yml'

jobs:
  linux:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        target: [x86_64, aarch64]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Build wheels
        uses: PyO3/maturin-action@v1
        with:
          target: ${{ matrix.target }}
          args: --release --out dist -m interstellar-py/Cargo.toml
          manylinux: auto
      - uses: actions/upload-artifact@v4
        with:
          name: wheels-linux-${{ matrix.target }}
          path: dist

  macos:
    runs-on: macos-latest
    strategy:
      matrix:
        target: [x86_64, aarch64]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Build wheels
        uses: PyO3/maturin-action@v1
        with:
          target: ${{ matrix.target }}-apple-darwin
          args: --release --out dist -m interstellar-py/Cargo.toml
      - uses: actions/upload-artifact@v4
        with:
          name: wheels-macos-${{ matrix.target }}
          path: dist

  windows:
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Build wheels
        uses: PyO3/maturin-action@v1
        with:
          args: --release --out dist -m interstellar-py/Cargo.toml
      - uses: actions/upload-artifact@v4
        with:
          name: wheels-windows
          path: dist

  sdist:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Build sdist
        uses: PyO3/maturin-action@v1
        with:
          command: sdist
          args: --out dist -m interstellar-py/Cargo.toml
      - uses: actions/upload-artifact@v4
        with:
          name: wheels-sdist
          path: dist

  publish:
    runs-on: ubuntu-latest
    needs: [linux, macos, windows, sdist]
    if: github.event_name == 'push' && startsWith(github.ref, 'refs/tags/')
    steps:
      - uses: actions/download-artifact@v4
        with:
          pattern: wheels-*
          merge-multiple: true
          path: dist
      - name: Publish to PyPI
        uses: pypa/gh-action-pypi-publish@release/v1
        with:
          password: ${{ secrets.PYPI_TOKEN }}
```

---

## 12. Testing Strategy

### 12.1 Python Tests

```python
# tests/test_graph.py
import pytest
from interstellar import Graph, P

class TestGraph:
    def test_create_empty_graph(self):
        g = Graph()
        assert g.vertex_count == 0
        assert g.edge_count == 0

    def test_add_vertex(self):
        g = Graph()
        v = g.add_vertex("person", {"name": "Alice"})
        assert isinstance(v, int)
        assert g.vertex_count == 1

    def test_add_edge(self):
        g = Graph()
        a = g.add_vertex("person", {"name": "Alice"})
        b = g.add_vertex("person", {"name": "Bob"})
        e = g.add_edge(a, b, "knows", {"since": 2020})
        assert isinstance(e, int)
        assert g.edge_count == 1

    def test_get_vertex(self):
        g = Graph()
        v = g.add_vertex("person", {"name": "Alice", "age": 30})
        vertex = g.get_vertex(v)
        assert vertex is not None
        assert vertex["label"] == "person"
        assert vertex["properties"]["name"] == "Alice"

    def test_vertex_not_found(self):
        g = Graph()
        assert g.get_vertex(999) is None

    def test_contains(self):
        g = Graph()
        v = g.add_vertex("person", {})
        assert v in g
        assert 999 not in g


class TestTraversal:
    @pytest.fixture
    def social_graph(self):
        g = Graph()
        alice = g.add_vertex("person", {"name": "Alice", "age": 30})
        bob = g.add_vertex("person", {"name": "Bob", "age": 25})
        charlie = g.add_vertex("person", {"name": "Charlie", "age": 35})
        g.add_edge(alice, bob, "knows", {"since": 2020})
        g.add_edge(bob, charlie, "knows", {"since": 2021})
        return g

    def test_v_to_list(self, social_graph):
        result = social_graph.V().to_list()
        assert len(result) == 3

    def test_has_label(self, social_graph):
        result = social_graph.V().has_label("person").to_list()
        assert len(result) == 3

    def test_values(self, social_graph):
        names = social_graph.V().values("name").to_list()
        assert set(names) == {"Alice", "Bob", "Charlie"}

    def test_has_where(self, social_graph):
        adults = social_graph.V().has_where("age", P.gte(30)).to_list()
        assert len(adults) == 2

    def test_out(self, social_graph):
        friends = social_graph.V().has_value("name", "Alice").out("knows").to_list()
        assert len(friends) == 1

    def test_iteration(self, social_graph):
        names = [v for v in social_graph.V().values("name")]
        assert len(names) == 3

    def test_len(self, social_graph):
        count = len(social_graph.V().has_label("person"))
        assert count == 3
```

### 12.2 Benchmark Tests

```python
# tests/test_benchmark.py
import pytest
from interstellar import Graph

@pytest.fixture
def large_graph():
    g = Graph()
    vertices = []
    for i in range(10000):
        v = g.add_vertex("node", {"index": i})
        vertices.append(v)
    for i in range(0, len(vertices) - 1, 2):
        g.add_edge(vertices[i], vertices[i + 1], "connects", {})
    return g

def test_vertex_lookup(benchmark, large_graph):
    result = benchmark(lambda: large_graph.get_vertex(5000))
    assert result is not None

def test_traversal_count(benchmark, large_graph):
    result = benchmark(lambda: large_graph.V().to_count())
    assert result == 10000

def test_filter_traversal(benchmark, large_graph):
    result = benchmark(
        lambda: large_graph.V().has_where("index", P.lt(1000)).to_count()
    )
    assert result == 1000
```

---

## 13. Exit Criteria

- [ ] `maturin develop` builds successfully
- [ ] All basic Graph operations work (add/get/remove vertex/edge)
- [ ] Traversal API matches Rust/JS equivalents
- [ ] Type stubs pass mypy validation
- [ ] Python iterator protocol works (`for`, list comprehensions)
- [ ] GIL is released during long computations
- [ ] Wheels build for Linux (x86_64, aarch64), macOS (x86_64, arm64), Windows
- [ ] Package installs from PyPI
- [ ] All tests pass
- [ ] Documentation examples compile and run

---

## 14. Future Enhancements

| Enhancement | Description | Priority |
|-------------|-------------|----------|
| Async support | `asyncio` compatible API | Medium |
| NetworkX adapter | Convert to/from NetworkX graphs | Medium |
| NumPy integration | Adjacency matrix export | Low |
| Pandas integration | DataFrame import/export | Low |
| Jupyter widgets | Interactive graph visualization | Low |
| Type narrowing | More specific return types in stubs | Low |
