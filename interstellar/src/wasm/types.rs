//! Type conversion between Rust and JavaScript.
//!
//! This module handles bidirectional conversion between:
//! - `crate::value::Value` ↔ `JsValue`
//! - `crate::storage::Vertex` → JS Vertex object
//! - `crate::storage::Edge` → JS Edge object

use std::collections::HashMap;

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsError;

use crate::value::{EdgeId, Value, VertexId};

/// Convert a Rust Value to a JsValue.
///
/// # Type Mapping
///
/// | Rust Type | JavaScript Type |
/// |-----------|-----------------|
/// | `Value::Null` | `null` |
/// | `Value::Bool` | `boolean` |
/// | `Value::Int` | `bigint` |
/// | `Value::Float` | `number` |
/// | `Value::String` | `string` |
/// | `Value::List` | `Array` |
/// | `Value::Map` | `Object` |
/// | `Value::Vertex` | `bigint` (vertex ID) |
/// | `Value::Edge` | `bigint` (edge ID) |
pub fn value_to_js(value: &Value) -> Result<JsValue, JsError> {
    match value {
        Value::Null => Ok(JsValue::NULL),
        Value::Bool(b) => Ok(JsValue::from(*b)),
        Value::Int(n) => Ok(js_sys::BigInt::from(*n).into()),
        Value::Float(f) => Ok(JsValue::from(*f)),
        Value::String(s) => Ok(JsValue::from_str(s)),
        Value::List(list) => {
            let array = js_sys::Array::new();
            for item in list {
                array.push(&value_to_js(item)?);
            }
            Ok(array.into())
        }
        Value::Map(map) => {
            let obj = js_sys::Object::new();
            for (key, val) in map {
                js_sys::Reflect::set(&obj, &JsValue::from_str(key), &value_to_js(val)?)
                    .map_err(|_| JsError::new("Failed to set object property"))?;
            }
            Ok(obj.into())
        }
        Value::Vertex(id) => Ok(js_sys::BigInt::from(id.0).into()),
        Value::Edge(id) => Ok(js_sys::BigInt::from(id.0).into()),
    }
}

/// Convert a JsValue to a Rust Value.
///
/// # Type Mapping
///
/// | JavaScript Type | Rust Type |
/// |-----------------|-----------|
/// | `null`/`undefined` | `Value::Null` |
/// | `boolean` | `Value::Bool` |
/// | `bigint` | `Value::Int` |
/// | `number` (integer) | `Value::Int` |
/// | `number` (float) | `Value::Float` |
/// | `string` | `Value::String` |
/// | `Array` | `Value::List` |
/// | `Object` | `Value::Map` |
pub fn js_to_value(js: JsValue) -> Result<Value, JsError> {
    js_to_value_impl(&js)
}

/// Internal implementation of js_to_value that takes a reference.
fn js_to_value_impl(js: &JsValue) -> Result<Value, JsError> {
    // Handle null/undefined
    if js.is_null() || js.is_undefined() {
        return Ok(Value::Null);
    }

    // Handle boolean
    if let Some(b) = js.as_bool() {
        return Ok(Value::Bool(b));
    }

    // Handle BigInt (must check before number since BigInt is not a number)
    if let Some(bigint) = js.dyn_ref::<js_sys::BigInt>() {
        let s = bigint
            .to_string(10)
            .map_err(|_| JsError::new("Failed to convert BigInt"))?;
        let s: String = s.into();
        let n: i64 = s
            .parse()
            .map_err(|_| JsError::new("BigInt value out of i64 range"))?;
        return Ok(Value::Int(n));
    }

    // Handle number
    if let Some(n) = js.as_f64() {
        // Check if it's an integer
        if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
            return Ok(Value::Int(n as i64));
        }
        return Ok(Value::Float(n));
    }

    // Handle string
    if let Some(s) = js.as_string() {
        return Ok(Value::String(s));
    }

    // Handle Array
    if js_sys::Array::is_array(js) {
        let array = js_sys::Array::from(js);
        let mut values = Vec::with_capacity(array.length() as usize);
        for i in 0..array.length() {
            let item = array.get(i);
            values.push(js_to_value_impl(&item)?);
        }
        return Ok(Value::List(values));
    }

    // Handle Object (must be last since Array and others are also objects)
    if js.is_object() {
        let obj = js_sys::Object::from(js.clone());
        let entries = js_sys::Object::entries(&obj);
        let mut map = HashMap::new();
        for i in 0..entries.length() {
            let entry = js_sys::Array::from(&entries.get(i));
            let key = entry
                .get(0)
                .as_string()
                .ok_or_else(|| JsError::new("Object key must be a string"))?;
            let val = entry.get(1);
            map.insert(key, js_to_value_impl(&val)?);
        }
        return Ok(Value::Map(map));
    }

    Err(JsError::new("Unsupported JavaScript type"))
}

/// Convert a JsValue to a HashMap<String, Value> for properties.
pub fn js_to_properties(js: JsValue) -> Result<HashMap<String, Value>, JsError> {
    if js.is_undefined() || js.is_null() {
        return Ok(HashMap::new());
    }

    if !js.is_object() {
        return Err(JsError::new("Properties must be an object"));
    }

    let obj = js_sys::Object::from(js);
    let entries = js_sys::Object::entries(&obj);
    let mut map = HashMap::new();

    for i in 0..entries.length() {
        let entry = js_sys::Array::from(&entries.get(i));
        let key = entry
            .get(0)
            .as_string()
            .ok_or_else(|| JsError::new("Property key must be a string"))?;
        let val = entry.get(1);
        map.insert(key, js_to_value_impl(&val)?);
    }

    Ok(map)
}

/// Convert a HashMap<String, Value> to JsValue.
pub fn properties_to_js(props: &HashMap<String, Value>) -> Result<JsValue, JsError> {
    let obj = js_sys::Object::new();
    for (key, val) in props {
        js_sys::Reflect::set(&obj, &JsValue::from_str(key), &value_to_js(val)?)
            .map_err(|_| JsError::new("Failed to set property"))?;
    }
    Ok(obj.into())
}

/// Convert a Vec<Value> to a JavaScript Array.
pub fn values_to_js_array(values: Vec<Value>) -> Result<JsValue, JsError> {
    let array = js_sys::Array::new();
    for val in values {
        array.push(&value_to_js(&val)?);
    }
    Ok(array.into())
}

/// Create a JavaScript Vertex object from Rust data.
///
/// Returns an object matching the TypeScript `Vertex` interface:
/// ```typescript
/// {
///     id: bigint,
///     label: string,
///     properties: Record<string, Value>
/// }
/// ```
pub fn create_vertex_js(
    id: VertexId,
    label: &str,
    properties: &HashMap<String, Value>,
) -> Result<JsValue, JsError> {
    let obj = js_sys::Object::new();

    // Set id as BigInt
    js_sys::Reflect::set(&obj, &"id".into(), &js_sys::BigInt::from(id.0).into())
        .map_err(|_| JsError::new("Failed to set vertex id"))?;

    // Set label
    js_sys::Reflect::set(&obj, &"label".into(), &JsValue::from_str(label))
        .map_err(|_| JsError::new("Failed to set vertex label"))?;

    // Set properties
    let props_js = properties_to_js(properties)?;
    js_sys::Reflect::set(&obj, &"properties".into(), &props_js)
        .map_err(|_| JsError::new("Failed to set vertex properties"))?;

    Ok(obj.into())
}

/// Create a JavaScript Edge object from Rust data.
///
/// Returns an object matching the TypeScript `Edge` interface:
/// ```typescript
/// {
///     id: bigint,
///     label: string,
///     from: bigint,
///     to: bigint,
///     properties: Record<string, Value>
/// }
/// ```
pub fn create_edge_js(
    id: EdgeId,
    label: &str,
    from: VertexId,
    to: VertexId,
    properties: &HashMap<String, Value>,
) -> Result<JsValue, JsError> {
    let obj = js_sys::Object::new();

    // Set id as BigInt
    js_sys::Reflect::set(&obj, &"id".into(), &js_sys::BigInt::from(id.0).into())
        .map_err(|_| JsError::new("Failed to set edge id"))?;

    // Set label
    js_sys::Reflect::set(&obj, &"label".into(), &JsValue::from_str(label))
        .map_err(|_| JsError::new("Failed to set edge label"))?;

    // Set from (source vertex ID)
    js_sys::Reflect::set(&obj, &"from".into(), &js_sys::BigInt::from(from.0).into())
        .map_err(|_| JsError::new("Failed to set edge from"))?;

    // Set to (target vertex ID)
    js_sys::Reflect::set(&obj, &"to".into(), &js_sys::BigInt::from(to.0).into())
        .map_err(|_| JsError::new("Failed to set edge to"))?;

    // Set properties
    let props_js = properties_to_js(properties)?;
    js_sys::Reflect::set(&obj, &"properties".into(), &props_js)
        .map_err(|_| JsError::new("Failed to set edge properties"))?;

    Ok(obj.into())
}

/// Parse a bigint from JsValue to u64.
pub fn js_to_vertex_id(js: JsValue) -> Result<VertexId, JsError> {
    let id = js_to_u64(js)?;
    Ok(VertexId(id))
}

/// Parse a bigint from JsValue to EdgeId.
pub fn js_to_edge_id(js: JsValue) -> Result<EdgeId, JsError> {
    let id = js_to_u64(js)?;
    Ok(EdgeId(id))
}

/// Parse a bigint or number from JsValue to u64.
pub fn js_to_u64(js: JsValue) -> Result<u64, JsError> {
    if let Some(bigint) = js.dyn_ref::<js_sys::BigInt>() {
        // Convert BigInt to u64 using as_f64 (loses precision for very large values)
        // For values that fit in u64, this is safe
        // js_sys::BigInt doesn't have a direct TryInto<u64>, so we use string parsing
        let s = bigint
            .to_string(10)
            .map_err(|_| JsError::new("Failed to convert BigInt"))?;
        let s: String = s.into();
        s.parse::<u64>()
            .map_err(|_| JsError::new("BigInt value out of u64 range or invalid"))
    } else if let Some(num) = js.as_f64() {
        // Handle regular numbers (common in JS)
        if num >= 0.0 && num <= u64::MAX as f64 && num.fract() == 0.0 {
            Ok(num as u64)
        } else {
            Err(JsError::new("Number must be a non-negative integer"))
        }
    } else {
        Err(JsError::new("Expected bigint or number"))
    }
}

/// Parse a JsValue (single ID or array of IDs) to Vec<VertexId>.
///
/// Accepts either:
/// - A single vertex ID (bigint or number)
/// - An array of vertex IDs
///
/// This allows both `graph.V_(id)` and `graph.V_([id1, id2])` syntax.
pub fn js_array_to_vertex_ids(js: JsValue) -> Result<Vec<VertexId>, JsError> {
    if js.is_undefined() || js.is_null() {
        return Ok(Vec::new());
    }

    // Handle single ID (BigInt or number) - allows graph.V_(id) syntax
    if js.dyn_ref::<js_sys::BigInt>().is_some() || js.as_f64().is_some() {
        return Ok(vec![js_to_vertex_id(js)?]);
    }

    // Handle array of IDs - allows graph.V_([id1, id2]) syntax
    if js_sys::Array::is_array(&js) {
        let array = js_sys::Array::from(&js);
        let mut ids = Vec::with_capacity(array.length() as usize);

        for i in 0..array.length() {
            let item = array.get(i);
            ids.push(js_to_vertex_id(item)?);
        }

        return Ok(ids);
    }

    Err(JsError::new(
        "Expected vertex ID (bigint/number) or array of IDs",
    ))
}

/// Parse a JsValue (single ID or array of IDs) to Vec<EdgeId>.
///
/// Accepts either:
/// - A single edge ID (bigint or number)
/// - An array of edge IDs
///
/// This allows both `graph.E_(id)` and `graph.E_([id1, id2])` syntax.
pub fn js_array_to_edge_ids(js: JsValue) -> Result<Vec<EdgeId>, JsError> {
    if js.is_undefined() || js.is_null() {
        return Ok(Vec::new());
    }

    // Handle single ID (BigInt or number) - allows graph.E_(id) syntax
    if js.dyn_ref::<js_sys::BigInt>().is_some() || js.as_f64().is_some() {
        return Ok(vec![js_to_edge_id(js)?]);
    }

    // Handle array of IDs - allows graph.E_([id1, id2]) syntax
    if js_sys::Array::is_array(&js) {
        let array = js_sys::Array::from(&js);
        let mut ids = Vec::with_capacity(array.length() as usize);

        for i in 0..array.length() {
            let item = array.get(i);
            ids.push(js_to_edge_id(item)?);
        }

        return Ok(ids);
    }

    Err(JsError::new(
        "Expected edge ID (bigint/number) or array of IDs",
    ))
}

/// Parse a JsValue array to Vec<Value>.
pub fn js_array_to_values(js: JsValue) -> Result<Vec<Value>, JsError> {
    if js.is_undefined() || js.is_null() {
        return Ok(Vec::new());
    }

    let array = js_sys::Array::from(&js);
    let mut values = Vec::with_capacity(array.length() as usize);

    for i in 0..array.length() {
        let item = array.get(i);
        values.push(js_to_value_impl(&item)?);
    }

    Ok(values)
}

/// Parse a JsValue array to Vec<String>.
pub fn js_array_to_strings(js: JsValue) -> Result<Vec<String>, JsError> {
    if js.is_undefined() || js.is_null() {
        return Ok(Vec::new());
    }

    let array = js_sys::Array::from(&js);
    let mut strings = Vec::with_capacity(array.length() as usize);

    for i in 0..array.length() {
        let item = array.get(i);
        if let Some(s) = item.as_string() {
            strings.push(s);
        } else {
            return Err(JsError::new("Expected array of strings"));
        }
    }

    Ok(strings)
}
