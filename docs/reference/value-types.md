# Value Types

This document describes the core value types used throughout Interstellar for representing graph data.

## Overview

Interstellar uses a dynamic type system for property values, similar to JSON but extended with graph-specific types. The `Value` enum can hold any property value or traversal result.

## Element Identifiers

### VertexId

A unique identifier for a vertex in the graph.

```rust
use interstellar::prelude::*;

let id = VertexId(42);

// IDs are copyable, hashable, and orderable
let mut ids = vec![VertexId(3), VertexId(1), VertexId(2)];
ids.sort();  // [VertexId(1), VertexId(2), VertexId(3)]
```

**Properties:**
- `Copy`, `Clone`, `Eq`, `PartialEq`, `Hash`, `Ord`, `PartialOrd`
- Assigned by storage backend when vertices are created
- Stable within a session (not guaranteed across restarts for some backends)

### EdgeId

A unique identifier for an edge in the graph.

```rust
use interstellar::prelude::*;

let id = EdgeId(99);
```

**Properties:**
- Same traits as `VertexId`
- Distinct namespace from vertex IDs

### ElementId

A union type for either vertex or edge identifiers.

```rust
use interstellar::prelude::*;

let vertex_elem = ElementId::Vertex(VertexId(1));
let edge_elem = ElementId::Edge(EdgeId(2));

match vertex_elem {
    ElementId::Vertex(vid) => println!("Vertex: {:?}", vid),
    ElementId::Edge(eid) => println!("Edge: {:?}", eid),
}
```

## Value Enum

The `Value` enum represents any property value or traversal result.

### Variants

| Variant | Rust Type | Description | Example |
|---------|-----------|-------------|---------|
| `Null` | - | Absence of a value | `Value::Null` |
| `Bool` | `bool` | Boolean true/false | `Value::Bool(true)` |
| `Int` | `i64` | 64-bit signed integer | `Value::Int(42)` |
| `Float` | `f64` | 64-bit floating point | `Value::Float(3.14)` |
| `String` | `String` | UTF-8 text | `Value::String("hello".into())` |
| `List` | `Vec<Value>` | Ordered collection | `Value::List(vec![...])` |
| `Map` | `HashMap<String, Value>` | Key-value pairs | `Value::Map(map)` |
| `Vertex` | `VertexId` | Reference to a vertex | `Value::Vertex(VertexId(1))` |
| `Edge` | `EdgeId` | Reference to an edge | `Value::Edge(EdgeId(2))` |

### Type Conversions

`Value` implements `From` for common Rust types:

```rust
use interstellar::prelude::*;

// Primitives
let bool_val: Value = true.into();
let int_val: Value = 42i64.into();
let float_val: Value = 3.14f64.into();
let str_val: Value = "hello".into();

// Smaller integers are promoted
let i32_val: Value = 42i32.into();  // becomes Int(42)
let u32_val: Value = 7u32.into();   // becomes Int(7)
let f32_val: Value = 3.14f32.into(); // becomes Float(3.14...)

// Graph elements
let vertex_val: Value = VertexId(1).into();
let edge_val: Value = EdgeId(2).into();

// Collections
let list_val: Value = vec![Value::Int(1), Value::Int(2)].into();
let map_val: Value = HashMap::from([
    ("name".to_string(), Value::String("Alice".into())),
]).into();
```

### Type Checking

Check the type of a value:

```rust
use interstellar::prelude::*;

let val = Value::Int(42);

// Type predicates
assert!(!val.is_null());
assert!(!val.is_vertex());
assert!(!val.is_edge());
```

### Type Extraction

Safely extract typed values with `as_*` methods:

```rust
use interstellar::prelude::*;

let val: Value = 42i64.into();

// Returns Option<T>
if let Some(n) = val.as_i64() {
    println!("Integer: {}", n);
}

// All extraction methods
val.as_bool();       // Option<bool>
val.as_i64();        // Option<i64>
val.as_f64();        // Option<f64>
val.as_str();        // Option<&str>
val.as_list();       // Option<&Vec<Value>>
val.as_map();        // Option<&HashMap<String, Value>>
val.as_vertex_id();  // Option<VertexId>
val.as_edge_id();    // Option<EdgeId>
```

## ComparableValue

A version of `Value` that implements `Ord` for sorting and ordered collections.

```rust
use interstellar::prelude::*;

let values = vec![
    Value::Int(3),
    Value::Int(1),
    Value::Int(2),
];

// Convert to comparable for sorting
let mut comparable: Vec<_> = values.iter()
    .map(Value::to_comparable)
    .collect();
comparable.sort();  // Now sorted: [Int(1), Int(2), Int(3)]
```

### Float Ordering

Standard Rust floats don't implement `Ord` due to NaN. `ComparableValue::Float` uses `OrderedFloat` which provides total ordering:

- NaN values are ordered (greater than all other values)
- Negative zero equals positive zero for comparison
- All values have a consistent sort order

## Hashing

`Value` implements `Hash`, allowing use in `HashSet` and as `HashMap` keys:

```rust
use interstellar::prelude::*;
use std::collections::HashSet;

let mut seen: HashSet<Value> = HashSet::new();
seen.insert(42i64.into());
seen.insert("hello".into());
seen.insert(VertexId(1).into());
```

**Note:** Map values are hashed in sorted key order for consistency regardless of insertion order.

## Serialization

Values can be serialized to a compact binary format:

```rust
use interstellar::prelude::*;

let value = Value::Int(42);

// Serialize
let mut buf = Vec::new();
value.serialize(&mut buf);

// Deserialize
let mut pos = 0;
let parsed = Value::deserialize(&buf, &mut pos).unwrap();
assert_eq!(parsed, value);
```

### Binary Format

| Tag | Type | Data |
|-----|------|------|
| 0x00 | Null | (none) |
| 0x01 | Bool(false) | (none) |
| 0x02 | Bool(true) | (none) |
| 0x03 | Int | 8 bytes (little-endian i64) |
| 0x04 | Float | 8 bytes (little-endian f64) |
| 0x05 | String | 4-byte length + UTF-8 bytes |
| 0x06 | List | 4-byte count + serialized items |
| 0x07 | Map | 4-byte count + (key, value) pairs |
| 0x08 | Vertex | 8 bytes (little-endian u64) |
| 0x09 | Edge | 8 bytes (little-endian u64) |

## Type Discriminant

Get the type tag for a value (matches serialization format):

```rust
use interstellar::prelude::*;

assert_eq!(Value::Null.discriminant(), 0x00);
assert_eq!(Value::Bool(false).discriminant(), 0x01);
assert_eq!(Value::Bool(true).discriminant(), 0x02);
assert_eq!(Value::Int(42).discriminant(), 0x03);
assert_eq!(Value::Float(3.14).discriminant(), 0x04);
assert_eq!(Value::String("x".into()).discriminant(), 0x05);
assert_eq!(Value::List(vec![]).discriminant(), 0x06);
assert_eq!(Value::Map(HashMap::new()).discriminant(), 0x07);
assert_eq!(Value::Vertex(VertexId(1)).discriminant(), 0x08);
assert_eq!(Value::Edge(EdgeId(1)).discriminant(), 0x09);
```

## Working with Properties

Properties are stored as `HashMap<String, Value>`:

```rust
use interstellar::prelude::*;
use std::collections::HashMap;

let props: HashMap<String, Value> = HashMap::from([
    ("name".to_string(), "Alice".into()),
    ("age".to_string(), 30i64.into()),
    ("active".to_string(), true.into()),
]);

// Add vertex with properties
let graph = Graph::new();
let alice = graph.add_vertex("person", props);
```

## See Also

- [Error Handling](error-handling.md) - Error types for operations on values
- [Predicates](../api/predicates.md) - Filtering values in traversals
