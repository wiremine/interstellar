//! Value types and element identifiers for graph data.
//!
//! This module provides the core data types used throughout Interstellar:
//!
//! - [`VertexId`] / [`EdgeId`] - Unique identifiers for graph elements
//! - [`ElementId`] - Union type for either vertex or edge IDs
//! - [`Value`] - Dynamic property value type (similar to JSON)
//! - [`ComparableValue`] - An ordered version of `Value` for sorting/dedup
//!
//! # Value System
//!
//! Interstellar uses a dynamic type system for property values, similar to JSON
//! but extended with graph-specific types for vertices and edges. The [`Value`]
//! enum can hold:
//!
//! - Primitives: `Null`, `Bool`, `Int` (i64), `Float` (f64), `String`
//! - Collections: `List`, `Map`
//! - Graph references: `Vertex`, `Edge`
//!
//! # Type Conversions
//!
//! [`Value`] implements [`From`] for common Rust types, making it easy to
//! construct values:
//!
//! ```rust
//! use interstellar::prelude::*;
//!
//! let int_val: Value = 42i64.into();
//! let str_val: Value = "hello".into();
//! let bool_val: Value = true.into();
//! let float_val: Value = 3.14f64.into();
//! ```
//!
//! # Serialization
//!
//! Values can be serialized to and from a compact binary format using
//! [`Value::serialize`] and [`Value::deserialize`], useful for persistence.
//!
//! # Example
//!
//! ```rust
//! use interstellar::prelude::*;
//! use std::collections::HashMap;
//!
//! // Create values from Rust types
//! let name: Value = "Alice".into();
//! let age: Value = 30i64.into();
//!
//! // Check types
//! assert!(name.as_str().is_some());
//! assert!(age.as_i64().is_some());
//!
//! // Work with vertex IDs
//! let vid = VertexId(42);
//! let vertex_val: Value = vid.into();
//! assert!(vertex_val.is_vertex());
//! assert_eq!(vertex_val.as_vertex_id(), Some(VertexId(42)));
//! ```

use std::collections::{BTreeMap, HashMap};

/// An order-preserving map type used for [`Value::Map`] payloads.
///
/// Backed by [`indexmap::IndexMap`], this preserves insertion order so that
/// query result rows surface columns in a deterministic order matching the
/// order they were produced (e.g. the `RETURN` clause of a GQL query).
pub type ValueMap = indexmap::IndexMap<String, Value>;

/// Helper trait for ergonomic conversion of map-like collections into a
/// [`ValueMap`] for use with [`Value::Map`].
pub trait IntoValueMap {
    fn into_value_map(self) -> ValueMap;
}

impl IntoValueMap for HashMap<String, Value> {
    fn into_value_map(self) -> ValueMap {
        self.into_iter().collect()
    }
}

impl IntoValueMap for ValueMap {
    fn into_value_map(self) -> ValueMap {
        self
    }
}

impl IntoValueMap for BTreeMap<String, Value> {
    fn into_value_map(self) -> ValueMap {
        self.into_iter().collect()
    }
}

/// A unique identifier for a vertex in the graph.
///
/// `VertexId` is a lightweight, copy-able handle that uniquely identifies
/// a vertex within a graph. IDs are assigned by the storage backend when
/// vertices are created.
///
/// # Ordering
///
/// Vertex IDs implement [`Ord`], allowing them to be sorted and used in
/// ordered collections like [`BTreeMap`].
///
/// # Example
///
/// ```rust
/// use interstellar::prelude::*;
/// use interstellar::storage::Graph;
/// use std::collections::HashMap;
///
/// let graph = Graph::new();
///
/// // Adding a vertex returns its ID
/// let alice_id = graph.add_vertex("person", HashMap::new());
/// let bob_id = graph.add_vertex("person", HashMap::new());
///
/// // IDs can be compared and sorted
/// assert_ne!(alice_id, bob_id);
/// let mut ids = vec![bob_id, alice_id];
/// ids.sort();
/// ```
#[derive(
    Copy, Clone, Eq, PartialEq, Hash, Debug, Ord, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub struct VertexId(pub u64);

/// Trait for types that can be converted to a VertexId.
///
/// This enables ergonomic APIs that accept vertex references in multiple forms,
/// eliminating the need for explicit `.id()` calls in common patterns.
///
/// # Implemented For
///
/// - `VertexId` - Returns itself
/// - `&VertexId` - Dereferences and returns the ID
/// - `u64` - Wraps in `VertexId`
/// - `GraphVertex<G>` and `&GraphVertex<G>` - Extracts the vertex ID
///
/// # Example
///
/// ```rust
/// use interstellar::prelude::*;
/// use interstellar::value::IntoVertexId;
/// use std::sync::Arc;
/// use std::collections::HashMap;
///
/// let graph = Arc::new(Graph::new());
/// let alice_id = graph.add_vertex("person", HashMap::new());
///
/// // All of these implement IntoVertexId:
/// assert_eq!(alice_id.into_vertex_id(), alice_id);
/// assert_eq!((&alice_id).into_vertex_id(), alice_id);
/// assert_eq!(42u64.into_vertex_id(), VertexId(42));
/// ```
pub trait IntoVertexId {
    /// Convert this value into a VertexId.
    fn into_vertex_id(self) -> VertexId;
}

impl IntoVertexId for VertexId {
    #[inline]
    fn into_vertex_id(self) -> VertexId {
        self
    }
}

impl IntoVertexId for &VertexId {
    #[inline]
    fn into_vertex_id(self) -> VertexId {
        *self
    }
}

impl IntoVertexId for u64 {
    #[inline]
    fn into_vertex_id(self) -> VertexId {
        VertexId(self)
    }
}

/// A unique identifier for an edge in the graph.
///
/// `EdgeId` is a lightweight, copy-able handle that uniquely identifies
/// an edge within a graph. IDs are assigned by the storage backend when
/// edges are created.
///
/// # Ordering
///
/// Edge IDs implement [`Ord`], allowing them to be sorted and used in
/// ordered collections like [`BTreeMap`].
///
/// # Example
///
/// ```rust
/// use interstellar::prelude::*;
/// use interstellar::storage::Graph;
/// use std::collections::HashMap;
///
/// let graph = Graph::new();
/// let alice = graph.add_vertex("person", HashMap::new());
/// let bob = graph.add_vertex("person", HashMap::new());
///
/// // Adding an edge returns its ID
/// let edge_id = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
///
/// // Edge IDs are distinct from vertex IDs
/// println!("Edge {:?} connects {:?} to {:?}", edge_id, alice, bob);
/// ```
#[derive(
    Copy, Clone, Eq, PartialEq, Hash, Debug, Ord, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub struct EdgeId(pub u64);

/// A union type representing either a vertex or edge identifier.
///
/// `ElementId` is useful when working with APIs that can accept either
/// type of graph element, such as generic property accessors.
///
/// # Example
///
/// ```rust
/// use interstellar::prelude::*;
///
/// let vertex_elem = ElementId::Vertex(VertexId(1));
/// let edge_elem = ElementId::Edge(EdgeId(2));
///
/// match vertex_elem {
///     ElementId::Vertex(vid) => println!("Vertex: {:?}", vid),
///     ElementId::Edge(eid) => println!("Edge: {:?}", eid),
/// }
/// ```
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ElementId {
    /// A vertex identifier.
    Vertex(VertexId),
    /// An edge identifier.
    Edge(EdgeId),
}

/// A dynamic value type for graph properties and traversal results.
///
/// `Value` is Interstellar's universal data type, capable of representing
/// any property value or traversal result. It's similar to JSON but extended
/// with graph-specific types for vertex and edge references.
///
/// # Variants
///
/// | Variant | Rust Type | Description |
/// |---------|-----------|-------------|
/// | `Null` | - | Absence of a value |
/// | `Bool` | `bool` | Boolean true/false |
/// | `Int` | `i64` | 64-bit signed integer |
/// | `Float` | `f64` | 64-bit floating point |
/// | `String` | `String` | UTF-8 text |
/// | `List` | `Vec<Value>` | Ordered collection |
/// | `Map` | `HashMap<String, Value>` | Key-value pairs |
/// | `Vertex` | [`VertexId`] | Reference to a vertex |
/// | `Edge` | [`EdgeId`] | Reference to an edge |
///
/// # Type Conversions
///
/// `Value` implements [`From`] for many common types:
///
/// ```rust
/// use interstellar::prelude::*;
///
/// // Primitives
/// let _: Value = true.into();
/// let _: Value = 42i64.into();
/// let _: Value = 3.14f64.into();
/// let _: Value = "hello".into();
///
/// // Graph elements
/// let _: Value = VertexId(1).into();
/// let _: Value = EdgeId(2).into();
/// ```
///
/// # Type Checking and Extraction
///
/// Use the `as_*` methods to safely extract typed values:
///
/// ```rust
/// use interstellar::prelude::*;
///
/// let val: Value = 42i64.into();
///
/// // Type-safe extraction
/// if let Some(n) = val.as_i64() {
///     println!("Got integer: {}", n);
/// }
///
/// // Type checking
/// assert!(!val.is_null());
/// assert!(!val.is_vertex());
/// ```
///
/// # Hashing
///
/// `Value` implements [`Hash`], allowing it to be used in hash-based
/// collections. Map values are hashed in sorted key order to ensure
/// consistent hashing regardless of insertion order.
///
/// ```rust
/// use interstellar::prelude::*;
/// use std::collections::HashSet;
///
/// let mut seen: HashSet<Value> = HashSet::new();
/// seen.insert(42i64.into());
/// seen.insert("hello".into());
/// ```
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Value {
    /// The null/absent value.
    Null,
    /// A boolean value.
    Bool(bool),
    /// A 64-bit signed integer.
    Int(i64),
    /// A 64-bit floating-point number.
    Float(f64),
    /// A UTF-8 string.
    String(String),
    /// An ordered list of values.
    List(Vec<Value>),
    /// A map of string keys to values. Iteration order is the insertion order.
    Map(ValueMap),
    /// A vertex reference (for traversal).
    Vertex(VertexId),
    /// An edge reference (for traversal).
    Edge(EdgeId),
}

/// A comparable version of [`Value`] that implements [`Ord`].
///
/// `ComparableValue` mirrors the structure of [`Value`] but provides
/// total ordering, making it suitable for:
///
/// - Sorting traversal results
/// - Using values as keys in [`BTreeMap`]
/// - Deduplication with ordered iteration
///
/// # Float Ordering
///
/// Unlike standard Rust floats, [`ComparableValue::Float`] uses total
/// ordering via [`OrderedFloat`], where NaN values are ordered consistently.
///
/// # Conversion
///
/// Use [`Value::to_comparable`] to convert a `Value`:
///
/// ```rust
/// use interstellar::prelude::*;
///
/// let val = Value::Float(3.14);
/// let comparable = val.to_comparable();
///
/// // Now it can be sorted
/// let mut values = vec![
///     Value::Float(2.0).to_comparable(),
///     Value::Float(1.0).to_comparable(),
///     Value::Float(3.0).to_comparable(),
/// ];
/// values.sort();
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ComparableValue {
    /// The null/absent value.
    Null,
    /// A boolean value.
    Bool(bool),
    /// A 64-bit signed integer.
    Int(i64),
    /// A 64-bit floating-point with total ordering.
    Float(OrderedFloat),
    /// A UTF-8 string.
    String(String),
    /// An ordered list of comparable values.
    List(Vec<ComparableValue>),
    /// A sorted map of string keys to comparable values.
    Map(BTreeMap<String, ComparableValue>),
    /// A vertex reference.
    Vertex(VertexId),
    /// An edge reference.
    Edge(EdgeId),
}

/// A floating-point wrapper that provides total ordering.
///
/// Standard Rust `f64` only implements [`PartialOrd`] because NaN values
/// don't have a defined ordering. `OrderedFloat` wraps an `f64` and uses
/// [`f64::total_cmp`] to provide a total ordering where:
///
/// - NaN values are ordered (greater than all other values)
/// - Negative zero equals positive zero for comparison
/// - All values have a consistent sort order
///
/// # Example
///
/// ```rust
/// use interstellar::value::OrderedFloat;
///
/// let a = OrderedFloat(1.0);
/// let b = OrderedFloat(2.0);
/// let nan = OrderedFloat(f64::NAN);
///
/// assert!(a < b);
/// assert!(b < nan); // NaN is ordered last
/// ```
#[derive(Copy, Clone, Debug)]
pub struct OrderedFloat(pub f64);

impl PartialEq for OrderedFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedFloat {}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl std::hash::Hash for OrderedFloat {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash the discriminant first for type safety
        std::mem::discriminant(self).hash(state);

        match self {
            Value::Null => {}
            Value::Bool(b) => b.hash(state),
            Value::Int(n) => n.hash(state),
            Value::Float(f) => f.to_bits().hash(state),
            Value::String(s) => s.hash(state),
            Value::List(items) => items.hash(state),
            Value::Map(map) => {
                // Hash map entries in sorted order for consistency
                let mut entries: Vec<_> = map.iter().collect();
                entries.sort_by_key(|(k, _)| *k);
                for (k, v) in entries {
                    k.hash(state);
                    v.hash(state);
                }
            }
            Value::Vertex(id) => id.hash(state),
            Value::Edge(id) => id.hash(state),
        }
    }
}

impl Eq for Value {}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Int(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Value::Int(value as i64)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Value::Int(value as i64)
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Value::Int(value as i64)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Float(value)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Value::Float(value as f64)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.to_owned())
    }
}

impl From<Vec<Value>> for Value {
    fn from(values: Vec<Value>) -> Self {
        Value::List(values)
    }
}

impl From<HashMap<String, Value>> for Value {
    fn from(map: HashMap<String, Value>) -> Self {
        Value::Map(map.into_iter().collect())
    }
}

impl From<ValueMap> for Value {
    fn from(map: ValueMap) -> Self {
        Value::Map(map)
    }
}

impl From<VertexId> for Value {
    fn from(id: VertexId) -> Self {
        Value::Vertex(id)
    }
}

impl From<EdgeId> for Value {
    fn from(id: EdgeId) -> Self {
        Value::Edge(id)
    }
}

impl Value {
    /// Serialize this value to a compact binary format.
    ///
    /// The binary format uses a type tag byte followed by the value data.
    /// This format is suitable for persistence and network transmission.
    ///
    /// # Format
    ///
    /// | Tag | Type | Data |
    /// |-----|------|------|
    /// | 0x00 | Null | (none) |
    /// | 0x01 | Bool(false) | (none) |
    /// | 0x02 | Bool(true) | (none) |
    /// | 0x03 | Int | 8 bytes (little-endian i64) |
    /// | 0x04 | Float | 8 bytes (little-endian f64) |
    /// | 0x05 | String | 4-byte length + UTF-8 bytes |
    /// | 0x06 | List | 4-byte count + serialized items |
    /// | 0x07 | Map | 4-byte count + (key, value) pairs |
    /// | 0x08 | Vertex | 8 bytes (little-endian u64) |
    /// | 0x09 | Edge | 8 bytes (little-endian u64) |
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    ///
    /// let value = Value::Int(42);
    /// let mut buf = Vec::new();
    /// value.serialize(&mut buf);
    ///
    /// // Deserialize it back
    /// let mut pos = 0;
    /// let parsed = Value::deserialize(&buf, &mut pos).unwrap();
    /// assert_eq!(parsed, value);
    /// ```
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            Value::Null => buf.push(0x00),
            Value::Bool(false) => buf.push(0x01),
            Value::Bool(true) => buf.push(0x02),
            Value::Int(n) => {
                buf.push(0x03);
                buf.extend_from_slice(&n.to_le_bytes());
            }
            Value::Float(f) => {
                buf.push(0x04);
                buf.extend_from_slice(&f.to_le_bytes());
            }
            Value::String(s) => {
                buf.push(0x05);
                let len = s.len() as u32;
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            Value::List(items) => {
                buf.push(0x06);
                let len = items.len() as u32;
                buf.extend_from_slice(&len.to_le_bytes());
                for item in items {
                    item.serialize(buf);
                }
            }
            Value::Map(map) => {
                buf.push(0x07);
                let len = map.len() as u32;
                buf.extend_from_slice(&len.to_le_bytes());
                for (k, v) in map {
                    Value::String(k.clone()).serialize(buf);
                    v.serialize(buf);
                }
            }
            Value::Vertex(id) => {
                buf.push(0x08);
                buf.extend_from_slice(&id.0.to_le_bytes());
            }
            Value::Edge(id) => {
                buf.push(0x09);
                buf.extend_from_slice(&id.0.to_le_bytes());
            }
        }
    }

    /// Deserialize a value from a binary buffer.
    ///
    /// Reads a value starting at position `pos` in the buffer, advancing
    /// `pos` past the consumed bytes. Returns `None` if the buffer is
    /// malformed or truncated.
    ///
    /// # Arguments
    ///
    /// * `buf` - The byte buffer to read from
    /// * `pos` - Mutable position indicator, updated to point past the read value
    ///
    /// # Returns
    ///
    /// `Some(Value)` if deserialization succeeds, `None` if the data is invalid.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    ///
    /// // Serialize a value
    /// let original = Value::String("hello".to_string());
    /// let mut buf = Vec::new();
    /// original.serialize(&mut buf);
    ///
    /// // Deserialize it
    /// let mut pos = 0;
    /// let parsed = Value::deserialize(&buf, &mut pos).unwrap();
    /// assert_eq!(parsed, original);
    /// assert_eq!(pos, buf.len()); // Position advanced to end
    /// ```
    pub fn deserialize(buf: &[u8], pos: &mut usize) -> Option<Value> {
        let tag = *buf.get(*pos)?;
        *pos += 1;

        match tag {
            0x00 => Some(Value::Null),
            0x01 => Some(Value::Bool(false)),
            0x02 => Some(Value::Bool(true)),
            0x03 => {
                let n = i64::from_le_bytes(buf.get(*pos..*pos + 8)?.try_into().ok()?);
                *pos += 8;
                Some(Value::Int(n))
            }
            0x04 => {
                let f = f64::from_le_bytes(buf.get(*pos..*pos + 8)?.try_into().ok()?);
                *pos += 8;
                Some(Value::Float(f))
            }
            0x05 => {
                let len = u32::from_le_bytes(buf.get(*pos..*pos + 4)?.try_into().ok()?) as usize;
                *pos += 4;
                let slice = buf.get(*pos..*pos + len)?;
                *pos += len;
                let s = std::str::from_utf8(slice).ok()?;
                Some(Value::String(s.to_owned()))
            }
            0x06 => {
                let len = u32::from_le_bytes(buf.get(*pos..*pos + 4)?.try_into().ok()?) as usize;
                *pos += 4;
                let mut items = Vec::with_capacity(len);
                for _ in 0..len {
                    let item = Value::deserialize(buf, pos)?;
                    items.push(item);
                }
                Some(Value::List(items))
            }
            0x07 => {
                let len = u32::from_le_bytes(buf.get(*pos..*pos + 4)?.try_into().ok()?) as usize;
                *pos += 4;
                let mut map = ValueMap::with_capacity(len);
                for _ in 0..len {
                    let key = match Value::deserialize(buf, pos)? {
                        Value::String(s) => s,
                        _ => return None,
                    };
                    let value = Value::deserialize(buf, pos)?;
                    map.insert(key, value);
                }
                Some(Value::Map(map))
            }
            0x08 => {
                let id = u64::from_le_bytes(buf.get(*pos..*pos + 8)?.try_into().ok()?);
                *pos += 8;
                Some(Value::Vertex(VertexId(id)))
            }
            0x09 => {
                let id = u64::from_le_bytes(buf.get(*pos..*pos + 8)?.try_into().ok()?);
                *pos += 8;
                Some(Value::Edge(EdgeId(id)))
            }
            _ => None,
        }
    }

    /// Convert this value to a comparable version with total ordering.
    ///
    /// Returns a [`ComparableValue`] that mirrors this value but implements
    /// [`Ord`], enabling sorting and use in ordered collections.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    ///
    /// let values = vec![
    ///     Value::Int(3),
    ///     Value::Int(1),
    ///     Value::Int(2),
    /// ];
    ///
    /// let mut comparable: Vec<_> = values.iter()
    ///     .map(Value::to_comparable)
    ///     .collect();
    /// comparable.sort();
    ///
    /// // Now sorted: [Int(1), Int(2), Int(3)]
    /// ```
    pub fn to_comparable(&self) -> ComparableValue {
        match self {
            Value::Null => ComparableValue::Null,
            Value::Bool(b) => ComparableValue::Bool(*b),
            Value::Int(n) => ComparableValue::Int(*n),
            Value::Float(f) => ComparableValue::Float(OrderedFloat(*f)),
            Value::String(s) => ComparableValue::String(s.clone()),
            Value::List(items) => {
                ComparableValue::List(items.iter().map(Value::to_comparable).collect())
            }
            Value::Map(map) => {
                let mut ordered = BTreeMap::new();
                for (k, v) in map {
                    ordered.insert(k.clone(), v.to_comparable());
                }
                ComparableValue::Map(ordered)
            }
            Value::Vertex(id) => ComparableValue::Vertex(*id),
            Value::Edge(id) => ComparableValue::Edge(*id),
        }
    }

    /// Extract the value as a boolean, if it is one.
    ///
    /// Returns `Some(bool)` if this is a `Bool` variant, `None` otherwise.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    ///
    /// assert_eq!(Value::Bool(true).as_bool(), Some(true));
    /// assert_eq!(Value::Int(1).as_bool(), None);
    /// ```
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Extract the value as an i64 integer, if it is one.
    ///
    /// Returns `Some(i64)` if this is an `Int` variant, `None` otherwise.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    ///
    /// assert_eq!(Value::Int(42).as_i64(), Some(42));
    /// assert_eq!(Value::Float(42.0).as_i64(), None); // Type mismatch
    /// ```
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(n) => Some(*n),
            _ => None,
        }
    }

    /// Extract the value as an f64 float, if it is one.
    ///
    /// Returns `Some(f64)` if this is a `Float` variant, `None` otherwise.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    ///
    /// assert_eq!(Value::Float(3.14).as_f64(), Some(3.14));
    /// assert_eq!(Value::Int(3).as_f64(), None); // Type mismatch
    /// ```
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Extract the value as a string slice, if it is one.
    ///
    /// Returns `Some(&str)` if this is a `String` variant, `None` otherwise.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    ///
    /// let val = Value::String("hello".to_string());
    /// assert_eq!(val.as_str(), Some("hello"));
    /// assert_eq!(Value::Int(42).as_str(), None);
    /// ```
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Extract the value as a list reference, if it is one.
    ///
    /// Returns `Some(&Vec<Value>)` if this is a `List` variant, `None` otherwise.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    ///
    /// let val = Value::List(vec![Value::Int(1), Value::Int(2)]);
    /// if let Some(items) = val.as_list() {
    ///     assert_eq!(items.len(), 2);
    /// }
    /// ```
    pub fn as_list(&self) -> Option<&Vec<Value>> {
        match self {
            Value::List(items) => Some(items),
            _ => None,
        }
    }

    /// Extract the value as a map reference, if it is one.
    ///
    /// Returns `Some(&ValueMap)` if this is a `Map` variant,
    /// `None` otherwise. The returned map preserves insertion order.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::value::ValueMap;
    ///
    /// let mut map = ValueMap::new();
    /// map.insert("name".to_string(), Value::String("Alice".to_string()));
    /// let val = Value::Map(map);
    ///
    /// if let Some(m) = val.as_map() {
    ///     assert!(m.contains_key("name"));
    /// }
    /// ```
    pub fn as_map(&self) -> Option<&ValueMap> {
        match self {
            Value::Map(map) => Some(map),
            _ => None,
        }
    }

    /// Check if this value is null.
    ///
    /// Returns `true` if this is the `Null` variant, `false` otherwise.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    ///
    /// assert!(Value::Null.is_null());
    /// assert!(!Value::Int(0).is_null());
    /// assert!(!Value::String("".to_string()).is_null());
    /// ```
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Extract the value as a vertex ID, if it is one.
    ///
    /// Returns `Some(VertexId)` if this is a `Vertex` variant, `None` otherwise.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    ///
    /// let val = Value::Vertex(VertexId(42));
    /// assert_eq!(val.as_vertex_id(), Some(VertexId(42)));
    /// assert_eq!(Value::Edge(EdgeId(42)).as_vertex_id(), None);
    /// ```
    #[inline]
    pub fn as_vertex_id(&self) -> Option<VertexId> {
        match self {
            Value::Vertex(id) => Some(*id),
            _ => None,
        }
    }

    /// Extract the value as an edge ID, if it is one.
    ///
    /// Returns `Some(EdgeId)` if this is an `Edge` variant, `None` otherwise.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    ///
    /// let val = Value::Edge(EdgeId(99));
    /// assert_eq!(val.as_edge_id(), Some(EdgeId(99)));
    /// assert_eq!(Value::Vertex(VertexId(99)).as_edge_id(), None);
    /// ```
    #[inline]
    pub fn as_edge_id(&self) -> Option<EdgeId> {
        match self {
            Value::Edge(id) => Some(*id),
            _ => None,
        }
    }

    /// Check if this value is a vertex reference.
    ///
    /// Returns `true` if this is a `Vertex` variant, `false` otherwise.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    ///
    /// assert!(Value::Vertex(VertexId(1)).is_vertex());
    /// assert!(!Value::Edge(EdgeId(1)).is_vertex());
    /// assert!(!Value::Int(1).is_vertex());
    /// ```
    #[inline]
    pub fn is_vertex(&self) -> bool {
        matches!(self, Value::Vertex(_))
    }

    /// Check if this value is an edge reference.
    ///
    /// Returns `true` if this is an `Edge` variant, `false` otherwise.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    ///
    /// assert!(Value::Edge(EdgeId(1)).is_edge());
    /// assert!(!Value::Vertex(VertexId(1)).is_edge());
    /// assert!(!Value::Int(1).is_edge());
    /// ```
    #[inline]
    pub fn is_edge(&self) -> bool {
        matches!(self, Value::Edge(_))
    }

    /// Returns the type discriminant (tag) for this value.
    ///
    /// The discriminant is the type tag byte used in the binary serialization
    /// format. This is useful for storage backends that need to know the value
    /// type without fully deserializing it.
    ///
    /// # Discriminant Values
    ///
    /// | Discriminant | Type |
    /// |--------------|------|
    /// | 0x00 | Null |
    /// | 0x01 | Bool(false) |
    /// | 0x02 | Bool(true) |
    /// | 0x03 | Int |
    /// | 0x04 | Float |
    /// | 0x05 | String |
    /// | 0x06 | List |
    /// | 0x07 | Map |
    /// | 0x08 | Vertex |
    /// | 0x09 | Edge |
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    ///
    /// assert_eq!(Value::Null.discriminant(), 0x00);
    /// assert_eq!(Value::Bool(false).discriminant(), 0x01);
    /// assert_eq!(Value::Bool(true).discriminant(), 0x02);
    /// assert_eq!(Value::Int(42).discriminant(), 0x03);
    /// assert_eq!(Value::Float(3.14).discriminant(), 0x04);
    /// assert_eq!(Value::String("hello".to_string()).discriminant(), 0x05);
    /// assert_eq!(Value::List(vec![]).discriminant(), 0x06);
    /// ```
    pub fn discriminant(&self) -> u8 {
        match self {
            Value::Null => 0x00,
            Value::Bool(false) => 0x01,
            Value::Bool(true) => 0x02,
            Value::Int(_) => 0x03,
            Value::Float(_) => 0x04,
            Value::String(_) => 0x05,
            Value::List(_) => 0x06,
            Value::Map(_) => 0x07,
            Value::Vertex(_) => 0x08,
            Value::Edge(_) => 0x09,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn converts_primitives_into_value() {
        let bool_v: Value = true.into();
        let int_v: Value = 42i32.into();
        let uint_v: Value = 7u32.into();
        let float_v: Value = 3.15f32.into(); // Use non-PI value to avoid clippy warning
        let double_v: Value = 6.30f64.into(); // Use non-TAU value to avoid clippy warning
        let string_v: Value = "hello".into();

        assert_eq!(bool_v, Value::Bool(true));
        assert_eq!(int_v, Value::Int(42));
        assert_eq!(uint_v, Value::Int(7));
        assert!(matches!(float_v, Value::Float(v) if (v - 3.15f64).abs() < 1e-6));
        assert!(matches!(double_v, Value::Float(v) if (v - 6.30f64).abs() < 1e-12));
        assert_eq!(string_v, Value::String("hello".to_string()));
    }

    #[test]
    fn converts_collections_into_value() {
        let list_v: Value = vec![Value::Int(1), Value::Bool(false)].into();

        let mut map = crate::value::ValueMap::new();
        map.insert("a".to_string(), Value::Int(1));
        map.insert("b".to_string(), Value::Bool(true));
        let map_v: Value = map.clone().into();

        assert_eq!(list_v, Value::List(vec![Value::Int(1), Value::Bool(false)]));
        assert_eq!(map_v, Value::Map(map));
    }

    #[test]
    fn orders_and_compares_ids() {
        let v1 = VertexId(1);
        let v2 = VertexId(2);
        let e1 = EdgeId(1);
        let e2 = EdgeId(2);

        assert!(v1 < v2);
        assert!(e1 < e2);
        assert_eq!(ElementId::Vertex(v1), ElementId::Vertex(VertexId(1)));
        assert_eq!(ElementId::Edge(e2), ElementId::Edge(EdgeId(2)));
    }

    #[test]
    fn serializes_and_deserializes_roundtrip() {
        let mut original_map = crate::value::ValueMap::new();
        original_map.insert("name".to_string(), Value::String("Alice".to_string()));
        original_map.insert("age".to_string(), Value::Int(30));
        let value = Value::List(vec![
            Value::Null,
            Value::Bool(true),
            Value::Int(-7),
            Value::Float(3.5),
            Value::String("hello".to_string()),
            Value::Map(original_map.clone()),
        ]);

        let mut buf = Vec::new();
        value.serialize(&mut buf);
        let mut pos = 0;
        let parsed = Value::deserialize(&buf, &mut pos).expect("deserialize");
        assert_eq!(pos, buf.len());
        assert_eq!(parsed, value);
    }

    #[test]
    fn comparable_value_orders_consistently() {
        let a = Value::String("a".to_string()).to_comparable();
        let b = Value::String("b".to_string()).to_comparable();
        assert!(a < b);

        let list_small = Value::List(vec![Value::Int(1)]).to_comparable();
        let list_large = Value::List(vec![Value::Int(1), Value::Int(2)]).to_comparable();
        assert!(list_small < list_large);
    }

    #[test]
    fn as_bool_extracts_boolean_values() {
        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::Bool(false).as_bool(), Some(false));
        assert_eq!(Value::Int(1).as_bool(), None);
        assert_eq!(Value::Null.as_bool(), None);
    }

    #[test]
    fn as_i64_extracts_integer_values() {
        assert_eq!(Value::Int(42).as_i64(), Some(42));
        assert_eq!(Value::Int(-7).as_i64(), Some(-7));
        assert_eq!(Value::Bool(true).as_i64(), None);
        assert_eq!(Value::Float(42.0).as_i64(), None);
    }

    #[test]
    fn as_f64_extracts_float_values() {
        assert_eq!(Value::Float(3.15).as_f64(), Some(3.15));
        assert_eq!(Value::Float(-2.5).as_f64(), Some(-2.5));
        assert_eq!(Value::Int(42).as_f64(), None);
        assert_eq!(Value::Null.as_f64(), None);
    }

    #[test]
    fn as_str_extracts_string_values() {
        let s = Value::String("hello".to_string());
        assert_eq!(s.as_str(), Some("hello"));
        assert_eq!(Value::Int(42).as_str(), None);
        assert_eq!(Value::Null.as_str(), None);
    }

    #[test]
    fn as_list_extracts_list_values() {
        let list = Value::List(vec![Value::Int(1), Value::Bool(true)]);
        let extracted = list.as_list();
        assert!(extracted.is_some());
        assert_eq!(extracted.unwrap().len(), 2);
        assert_eq!(extracted.unwrap()[0], Value::Int(1));

        assert_eq!(Value::Null.as_list(), None);
        assert_eq!(Value::Int(42).as_list(), None);
    }

    #[test]
    fn as_map_extracts_map_values() {
        let mut map = crate::value::ValueMap::new();
        map.insert("key".to_string(), Value::Int(42));
        let value = Value::Map(map.clone());

        let extracted = value.as_map();
        assert!(extracted.is_some());
        assert_eq!(extracted.unwrap().get("key"), Some(&Value::Int(42)));

        assert_eq!(Value::Null.as_map(), None);
        assert_eq!(Value::String("test".to_string()).as_map(), None);
    }

    #[test]
    fn is_null_detects_null_values() {
        assert!(Value::Null.is_null());
        assert!(!Value::Bool(false).is_null());
        assert!(!Value::Int(0).is_null());
        assert!(!Value::String("".to_string()).is_null());
    }

    #[test]
    fn accessor_methods_work_with_conversions() {
        let v: Value = 42i32.into();
        assert_eq!(v.as_i64(), Some(42));

        let s: Value = "test".into();
        assert_eq!(s.as_str(), Some("test"));

        let b: Value = true.into();
        assert_eq!(b.as_bool(), Some(true));

        let f: Value = 3.15f64.into();
        assert_eq!(f.as_f64(), Some(3.15));
    }

    #[test]
    fn value_vertex_variant_compiles_and_pattern_matches() {
        let v = Value::Vertex(VertexId(1));
        assert!(matches!(v, Value::Vertex(VertexId(1))));

        let v2 = Value::Vertex(VertexId(42));
        match v2 {
            Value::Vertex(id) => assert_eq!(id, VertexId(42)),
            _ => panic!("Expected Vertex variant"),
        }
    }

    #[test]
    fn value_edge_variant_compiles_and_pattern_matches() {
        let e = Value::Edge(EdgeId(1));
        assert!(matches!(e, Value::Edge(EdgeId(1))));

        let e2 = Value::Edge(EdgeId(99));
        match e2 {
            Value::Edge(id) => assert_eq!(id, EdgeId(99)),
            _ => panic!("Expected Edge variant"),
        }
    }

    #[test]
    fn vertex_and_edge_to_comparable() {
        let v = Value::Vertex(VertexId(123));
        let cv = v.to_comparable();
        assert_eq!(cv, ComparableValue::Vertex(VertexId(123)));

        let e = Value::Edge(EdgeId(456));
        let ce = e.to_comparable();
        assert_eq!(ce, ComparableValue::Edge(EdgeId(456)));
    }

    #[test]
    fn vertex_and_edge_serialize_roundtrip() {
        let vertex = Value::Vertex(VertexId(12345));
        let mut buf = Vec::new();
        vertex.serialize(&mut buf);
        assert_eq!(buf[0], 0x08); // Vertex tag
        let mut pos = 0;
        let parsed = Value::deserialize(&buf, &mut pos).expect("deserialize vertex");
        assert_eq!(parsed, vertex);
        assert_eq!(pos, buf.len());

        let edge = Value::Edge(EdgeId(67890));
        let mut buf = Vec::new();
        edge.serialize(&mut buf);
        assert_eq!(buf[0], 0x09); // Edge tag
        let mut pos = 0;
        let parsed = Value::deserialize(&buf, &mut pos).expect("deserialize edge");
        assert_eq!(parsed, edge);
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn value_can_be_used_as_hashmap_key() {
        use std::collections::HashMap;

        let mut map: HashMap<Value, i32> = HashMap::new();
        map.insert(Value::Int(42), 1);
        map.insert(Value::String("hello".to_string()), 2);
        map.insert(Value::Bool(true), 3);
        map.insert(Value::Null, 4);
        map.insert(Value::Float(3.15), 5);
        map.insert(Value::Vertex(VertexId(100)), 6);
        map.insert(Value::Edge(EdgeId(200)), 7);
        map.insert(Value::List(vec![Value::Int(1), Value::Int(2)]), 8);

        assert_eq!(map.get(&Value::Int(42)), Some(&1));
        assert_eq!(map.get(&Value::String("hello".to_string())), Some(&2));
        assert_eq!(map.get(&Value::Bool(true)), Some(&3));
        assert_eq!(map.get(&Value::Null), Some(&4));
        assert_eq!(map.get(&Value::Float(3.15)), Some(&5));
        assert_eq!(map.get(&Value::Vertex(VertexId(100))), Some(&6));
        assert_eq!(map.get(&Value::Edge(EdgeId(200))), Some(&7));
        assert_eq!(
            map.get(&Value::List(vec![Value::Int(1), Value::Int(2)])),
            Some(&8)
        );
    }

    #[test]
    fn value_can_be_inserted_into_hashset() {
        use std::collections::HashSet;

        let mut set: HashSet<Value> = HashSet::new();
        set.insert(Value::Int(1));
        set.insert(Value::Int(2));
        set.insert(Value::Int(1)); // Duplicate
        set.insert(Value::String("test".to_string()));
        set.insert(Value::Vertex(VertexId(42)));
        set.insert(Value::Edge(EdgeId(99)));

        assert_eq!(set.len(), 5); // 1, 2, "test", Vertex(42), Edge(99)
        assert!(set.contains(&Value::Int(1)));
        assert!(set.contains(&Value::Int(2)));
        assert!(set.contains(&Value::String("test".to_string())));
        assert!(set.contains(&Value::Vertex(VertexId(42))));
        assert!(set.contains(&Value::Edge(EdgeId(99))));
    }

    #[test]
    fn value_hash_is_consistent() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_value(v: &Value) -> u64 {
            let mut hasher = DefaultHasher::new();
            v.hash(&mut hasher);
            hasher.finish()
        }

        // Same values should produce same hash
        assert_eq!(hash_value(&Value::Int(42)), hash_value(&Value::Int(42)));
        assert_eq!(
            hash_value(&Value::String("hello".to_string())),
            hash_value(&Value::String("hello".to_string()))
        );
        assert_eq!(
            hash_value(&Value::Float(3.15)),
            hash_value(&Value::Float(3.15))
        );
        assert_eq!(hash_value(&Value::Null), hash_value(&Value::Null));
        assert_eq!(
            hash_value(&Value::Bool(true)),
            hash_value(&Value::Bool(true))
        );
        assert_eq!(
            hash_value(&Value::Vertex(VertexId(100))),
            hash_value(&Value::Vertex(VertexId(100)))
        );
        assert_eq!(
            hash_value(&Value::Edge(EdgeId(200))),
            hash_value(&Value::Edge(EdgeId(200)))
        );

        // Different values should (generally) produce different hashes
        assert_ne!(hash_value(&Value::Int(1)), hash_value(&Value::Int(2)));
        assert_ne!(
            hash_value(&Value::String("a".to_string())),
            hash_value(&Value::String("b".to_string()))
        );
        // Different types with same-ish values should produce different hashes
        assert_ne!(hash_value(&Value::Int(42)), hash_value(&Value::Float(42.0)));
        assert_ne!(
            hash_value(&Value::Vertex(VertexId(1))),
            hash_value(&Value::Edge(EdgeId(1)))
        );
    }

    #[test]
    fn value_map_hash_is_order_independent() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_value(v: &Value) -> u64 {
            let mut hasher = DefaultHasher::new();
            v.hash(&mut hasher);
            hasher.finish()
        }

        // Create two maps with same content but potentially different insertion order
        let mut map1 = crate::value::ValueMap::new();
        map1.insert("a".to_string(), Value::Int(1));
        map1.insert("b".to_string(), Value::Int(2));
        map1.insert("c".to_string(), Value::Int(3));

        let mut map2 = crate::value::ValueMap::new();
        map2.insert("c".to_string(), Value::Int(3));
        map2.insert("a".to_string(), Value::Int(1));
        map2.insert("b".to_string(), Value::Int(2));

        let v1 = Value::Map(map1);
        let v2 = Value::Map(map2);

        // Maps with same content should have same hash
        assert_eq!(hash_value(&v1), hash_value(&v2));
        assert_eq!(v1, v2);
    }

    #[test]
    fn from_vertex_id_for_value() {
        let id = VertexId(42);
        let v: Value = id.into();
        assert_eq!(v, Value::Vertex(VertexId(42)));

        // Also test Value::from directly
        let v2 = Value::from(VertexId(123));
        assert_eq!(v2, Value::Vertex(VertexId(123)));
    }

    #[test]
    fn from_edge_id_for_value() {
        let id = EdgeId(99);
        let v: Value = id.into();
        assert_eq!(v, Value::Edge(EdgeId(99)));

        // Also test Value::from directly
        let v2 = Value::from(EdgeId(456));
        assert_eq!(v2, Value::Edge(EdgeId(456)));
    }

    #[test]
    fn as_vertex_id_extracts_vertex_id() {
        let v = Value::Vertex(VertexId(42));
        assert_eq!(v.as_vertex_id(), Some(VertexId(42)));

        // Non-vertex values should return None
        assert_eq!(Value::Int(42).as_vertex_id(), None);
        assert_eq!(Value::Edge(EdgeId(42)).as_vertex_id(), None);
        assert_eq!(Value::Null.as_vertex_id(), None);
        assert_eq!(Value::String("vertex".to_string()).as_vertex_id(), None);
    }

    #[test]
    fn as_edge_id_extracts_edge_id() {
        let e = Value::Edge(EdgeId(99));
        assert_eq!(e.as_edge_id(), Some(EdgeId(99)));

        // Non-edge values should return None
        assert_eq!(Value::Int(99).as_edge_id(), None);
        assert_eq!(Value::Vertex(VertexId(99)).as_edge_id(), None);
        assert_eq!(Value::Null.as_edge_id(), None);
        assert_eq!(Value::String("edge".to_string()).as_edge_id(), None);
    }

    #[test]
    fn is_vertex_detects_vertex_values() {
        assert!(Value::Vertex(VertexId(1)).is_vertex());
        assert!(Value::Vertex(VertexId(0)).is_vertex());
        assert!(Value::Vertex(VertexId(u64::MAX)).is_vertex());

        // Non-vertex values
        assert!(!Value::Edge(EdgeId(1)).is_vertex());
        assert!(!Value::Int(1).is_vertex());
        assert!(!Value::Null.is_vertex());
        assert!(!Value::Bool(true).is_vertex());
        assert!(!Value::String("vertex".to_string()).is_vertex());
    }

    #[test]
    fn is_edge_detects_edge_values() {
        assert!(Value::Edge(EdgeId(1)).is_edge());
        assert!(Value::Edge(EdgeId(0)).is_edge());
        assert!(Value::Edge(EdgeId(u64::MAX)).is_edge());

        // Non-edge values
        assert!(!Value::Vertex(VertexId(1)).is_edge());
        assert!(!Value::Int(1).is_edge());
        assert!(!Value::Null.is_edge());
        assert!(!Value::Bool(true).is_edge());
        assert!(!Value::String("edge".to_string()).is_edge());
    }

    fn arb_value() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(Value::Int),
            any::<f64>().prop_map(Value::Float),
            "[a-zA-Z0-9]{0,8}".prop_map(Value::String),
            any::<u64>().prop_map(|n| Value::Vertex(VertexId(n))),
            any::<u64>().prop_map(|n| Value::Edge(EdgeId(n))),
        ];

        leaf.prop_recursive(4, 64, 8, |inner| {
            let list = prop::collection::vec(inner.clone(), 0..4).prop_map(Value::List);
            let map = prop::collection::hash_map("[a-zA-Z0-9]{0,6}", inner, 0..4)
                .prop_map(|m| Value::Map(m.into_iter().collect()));
            prop_oneof![list, map]
        })
    }

    #[test]
    fn discriminant_matches_serialization_tag() {
        // Null
        assert_eq!(Value::Null.discriminant(), 0x00);
        let mut buf = Vec::new();
        Value::Null.serialize(&mut buf);
        assert_eq!(buf[0], 0x00);

        // Bool(false)
        assert_eq!(Value::Bool(false).discriminant(), 0x01);
        let mut buf = Vec::new();
        Value::Bool(false).serialize(&mut buf);
        assert_eq!(buf[0], 0x01);

        // Bool(true)
        assert_eq!(Value::Bool(true).discriminant(), 0x02);
        let mut buf = Vec::new();
        Value::Bool(true).serialize(&mut buf);
        assert_eq!(buf[0], 0x02);

        // Int
        assert_eq!(Value::Int(42).discriminant(), 0x03);
        let mut buf = Vec::new();
        Value::Int(42).serialize(&mut buf);
        assert_eq!(buf[0], 0x03);

        // Float
        assert_eq!(Value::Float(3.15).discriminant(), 0x04);
        let mut buf = Vec::new();
        Value::Float(3.15).serialize(&mut buf);
        assert_eq!(buf[0], 0x04);

        // String
        assert_eq!(Value::String("test".to_string()).discriminant(), 0x05);
        let mut buf = Vec::new();
        Value::String("test".to_string()).serialize(&mut buf);
        assert_eq!(buf[0], 0x05);

        // List
        assert_eq!(Value::List(vec![]).discriminant(), 0x06);
        let mut buf = Vec::new();
        Value::List(vec![]).serialize(&mut buf);
        assert_eq!(buf[0], 0x06);

        // Map
        assert_eq!(Value::Map(crate::value::ValueMap::new()).discriminant(), 0x07);
        let mut buf = Vec::new();
        Value::Map(crate::value::ValueMap::new()).serialize(&mut buf);
        assert_eq!(buf[0], 0x07);

        // Vertex
        assert_eq!(Value::Vertex(VertexId(1)).discriminant(), 0x08);
        let mut buf = Vec::new();
        Value::Vertex(VertexId(1)).serialize(&mut buf);
        assert_eq!(buf[0], 0x08);

        // Edge
        assert_eq!(Value::Edge(EdgeId(1)).discriminant(), 0x09);
        let mut buf = Vec::new();
        Value::Edge(EdgeId(1)).serialize(&mut buf);
        assert_eq!(buf[0], 0x09);
    }

    #[test]
    fn discriminant_is_unique_for_each_type() {
        let values = vec![
            Value::Null,
            Value::Bool(false),
            Value::Bool(true),
            Value::Int(0),
            Value::Float(0.0),
            Value::String("".to_string()),
            Value::List(vec![]),
            Value::Map(crate::value::ValueMap::new()),
            Value::Vertex(VertexId(0)),
            Value::Edge(EdgeId(0)),
        ];

        let discriminants: Vec<u8> = values.iter().map(|v| v.discriminant()).collect();

        // All discriminants should be unique
        for i in 0..discriminants.len() {
            for j in (i + 1)..discriminants.len() {
                assert_ne!(
                    discriminants[i], discriminants[j],
                    "Discriminants not unique: {} vs {}",
                    discriminants[i], discriminants[j]
                );
            }
        }
    }

    #[test]
    fn complex_value_serialization_roundtrip() {
        // Test nested structures with all types
        let mut map = crate::value::ValueMap::new();
        map.insert("null".to_string(), Value::Null);
        map.insert("bool".to_string(), Value::Bool(true));
        map.insert("int".to_string(), Value::Int(-42));
        map.insert("float".to_string(), Value::Float(2.72)); // Use non-E value
        map.insert("string".to_string(), Value::String("nested".to_string()));
        map.insert(
            "list".to_string(),
            Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
        );
        map.insert("vertex".to_string(), Value::Vertex(VertexId(100)));
        map.insert("edge".to_string(), Value::Edge(EdgeId(200)));

        let complex = Value::Map(map);

        let mut buf = Vec::new();
        complex.serialize(&mut buf);
        let mut pos = 0;
        let parsed = Value::deserialize(&buf, &mut pos).expect("deserialize");
        assert_eq!(parsed, complex);
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn deeply_nested_list_roundtrip() {
        // Test deeply nested lists
        let inner = Value::List(vec![Value::Int(1), Value::Int(2)]);
        let middle = Value::List(vec![inner.clone(), inner.clone()]);
        let outer = Value::List(vec![middle.clone(), middle]);

        let mut buf = Vec::new();
        outer.serialize(&mut buf);
        let mut pos = 0;
        let parsed = Value::deserialize(&buf, &mut pos).expect("deserialize");
        assert_eq!(parsed, outer);
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn empty_collections_roundtrip() {
        let empty_list = Value::List(vec![]);
        let mut buf = Vec::new();
        empty_list.serialize(&mut buf);
        let mut pos = 0;
        let parsed = Value::deserialize(&buf, &mut pos).expect("deserialize");
        assert_eq!(parsed, empty_list);

        let empty_map = Value::Map(crate::value::ValueMap::new());
        let mut buf = Vec::new();
        empty_map.serialize(&mut buf);
        let mut pos = 0;
        let parsed = Value::deserialize(&buf, &mut pos).expect("deserialize");
        assert_eq!(parsed, empty_map);
    }

    #[test]
    fn large_string_roundtrip() {
        // Test string larger than 256 bytes
        let large_string = "a".repeat(1000);
        let value = Value::String(large_string.clone());

        let mut buf = Vec::new();
        value.serialize(&mut buf);
        let mut pos = 0;
        let parsed = Value::deserialize(&buf, &mut pos).expect("deserialize");
        assert_eq!(parsed, value);
        assert_eq!(parsed.as_str(), Some(large_string.as_str()));
    }

    #[test]
    fn special_float_values_roundtrip() {
        // Test NaN, infinity, and negative zero
        let values = vec![
            Value::Float(f64::NAN),
            Value::Float(f64::INFINITY),
            Value::Float(f64::NEG_INFINITY),
            Value::Float(-0.0),
            Value::Float(0.0),
        ];

        for val in values {
            let mut buf = Vec::new();
            val.serialize(&mut buf);
            let mut pos = 0;
            let parsed = Value::deserialize(&buf, &mut pos).expect("deserialize");

            // For NaN, check that both are NaN (can't use equality)
            match (&val, &parsed) {
                (Value::Float(f1), Value::Float(f2)) if f1.is_nan() => {
                    assert!(f2.is_nan(), "NaN did not roundtrip correctly");
                }
                _ => {
                    assert_eq!(parsed, val, "Value did not roundtrip correctly");
                }
            }
        }
    }

    proptest! {
        #[test]
        fn value_roundtrips_through_serialization(v in arb_value()) {
            let mut buf = Vec::new();
            v.serialize(&mut buf);
            let mut pos = 0;
            let parsed = Value::deserialize(&buf, &mut pos).expect("deserialize");
            prop_assert_eq!(parsed, v);
            prop_assert_eq!(pos, buf.len());
        }

        #[test]
        fn discriminant_matches_first_byte_of_serialization(v in arb_value()) {
            let mut buf = Vec::new();
            v.serialize(&mut buf);
            prop_assert!(!buf.is_empty());
            prop_assert_eq!(buf[0], v.discriminant());
        }
    }

    // =========================================================================
    // IntoVertexId Tests
    // =========================================================================

    #[test]
    fn into_vertex_id_from_vertex_id() {
        use super::IntoVertexId;

        let id = VertexId(42);
        assert_eq!(id.into_vertex_id(), VertexId(42));
    }

    #[test]
    fn into_vertex_id_from_ref_vertex_id() {
        use super::IntoVertexId;

        let id = VertexId(42);
        assert_eq!((&id).into_vertex_id(), VertexId(42));
    }

    #[test]
    fn into_vertex_id_from_u64() {
        use super::IntoVertexId;

        assert_eq!(42u64.into_vertex_id(), VertexId(42));
        assert_eq!(0u64.into_vertex_id(), VertexId(0));
        assert_eq!(u64::MAX.into_vertex_id(), VertexId(u64::MAX));
    }
}
