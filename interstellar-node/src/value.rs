//! Type conversion between Rust and JavaScript via napi-rs.
//!
//! This module handles bidirectional conversion between:
//! - `interstellar::Value` ↔ napi types
//! - `interstellar::storage::Vertex` → JS Vertex object
//! - `interstellar::storage::Edge` → JS Edge object

use std::collections::HashMap;

use napi::bindgen_prelude::*;
use napi::{Env, JsObject, JsUnknown, ValueType};

use interstellar::value::{EdgeId, Value, VertexId};

/// Convert a JavaScript value to a Rust Value.
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
pub fn js_to_value(env: Env, js: JsUnknown) -> Result<Value> {
    match js.get_type()? {
        ValueType::Null | ValueType::Undefined => Ok(Value::Null),

        ValueType::Boolean => {
            let b = js.coerce_to_bool()?.get_value()?;
            Ok(Value::Bool(b))
        }

        ValueType::Number => {
            let n = js.coerce_to_number()?.get_double()?;
            // Check if it's an integer
            if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
                Ok(Value::Int(n as i64))
            } else {
                Ok(Value::Float(n))
            }
        }

        ValueType::BigInt => {
            let bigint = unsafe { js.cast::<napi::JsBigInt>() };
            let (value, _lossless) = bigint.get_i64()?;
            Ok(Value::Int(value))
        }

        ValueType::String => {
            let s: String = js.coerce_to_string()?.into_utf8()?.as_str()?.to_string();
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
                    let key_js: JsUnknown = keys.get_element(i)?;
                    let key: String = key_js
                        .coerce_to_string()?
                        .into_utf8()?
                        .as_str()?
                        .to_string();
                    let val: JsUnknown = obj.get_named_property(&key)?;
                    map.insert(key, js_to_value(env, val)?);
                }
                Ok(Value::Map(map.into_iter().collect()))
            }
        }

        _ => Err(Error::new(Status::InvalidArg, "Unsupported value type")),
    }
}

/// Convert a Rust Value to a JavaScript value.
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
pub fn value_to_js(env: Env, value: &Value) -> Result<JsUnknown> {
    match value {
        Value::Null => {
            let null = env.get_null()?;
            Ok(null.into_unknown())
        }
        Value::Bool(b) => {
            let boolean = env.get_boolean(*b)?;
            Ok(boolean.into_unknown())
        }
        Value::Int(n) => {
            let bigint = env.create_bigint_from_i64(*n)?;
            Ok(bigint.into_unknown()?)
        }
        Value::Float(f) => {
            let number = env.create_double(*f)?;
            Ok(number.into_unknown())
        }
        Value::String(s) => {
            let string = env.create_string(s)?;
            Ok(string.into_unknown())
        }
        Value::List(items) => {
            let mut arr = env.create_array(items.len() as u32)?;
            for (i, item) in items.iter().enumerate() {
                arr.set(i as u32, value_to_js(env, item)?)?;
            }
            let obj = arr.coerce_to_object()?;
            Ok(obj.into_unknown())
        }
        Value::Map(map) => {
            let mut obj = env.create_object()?;
            for (k, v) in map {
                obj.set_named_property(k, value_to_js(env, v)?)?;
            }
            Ok(obj.into_unknown())
        }
        Value::Vertex(id) => {
            let bigint = env.create_bigint_from_u64(id.0)?;
            Ok(bigint.into_unknown()?)
        }
        Value::Edge(id) => {
            let bigint = env.create_bigint_from_u64(id.0)?;
            Ok(bigint.into_unknown()?)
        }
        Value::Point(p) => {
            let mut obj = env.create_object()?;
            obj.set_named_property("type", env.create_string("Point")?)?;
            let mut coords = env.create_array(2)?;
            coords.set(0, env.create_double(p.lon)?)?;
            coords.set(1, env.create_double(p.lat)?)?;
            obj.set_named_property("coordinates", coords)?;
            Ok(obj.into_unknown())
        }
        Value::Polygon(p) => {
            let mut obj = env.create_object()?;
            obj.set_named_property("type", env.create_string("Polygon")?)?;
            let mut ring = env.create_array(p.ring.len() as u32)?;
            for (i, &(lon, lat)) in p.ring.iter().enumerate() {
                let mut coord = env.create_array(2)?;
                coord.set(0, env.create_double(lon)?)?;
                coord.set(1, env.create_double(lat)?)?;
                ring.set(i as u32, coord)?;
            }
            let mut coords = env.create_array(1)?;
            coords.set(0u32, ring)?;
            obj.set_named_property("coordinates", coords)?;
            Ok(obj.into_unknown())
        }
    }
}

/// Convert JavaScript object to properties HashMap.
pub fn js_to_properties(env: Env, obj: Option<Object>) -> Result<HashMap<String, Value>> {
    match obj {
        None => Ok(HashMap::new()),
        Some(obj) => {
            let keys = obj.get_property_names()?;
            let len = keys.get_array_length()?;
            let mut map = HashMap::with_capacity(len as usize);

            for i in 0..len {
                let key_js: JsUnknown = keys.get_element(i)?;
                let key: String = key_js
                    .coerce_to_string()?
                    .into_utf8()?
                    .as_str()?
                    .to_string();
                let val: JsUnknown = obj.get_named_property(&key)?;
                map.insert(key, js_to_value(env, val)?);
            }

            Ok(map)
        }
    }
}

/// Convert Rust properties to JavaScript object.
pub fn properties_to_js(env: Env, props: &HashMap<String, Value>) -> Result<Object> {
    let mut obj = env.create_object()?;
    for (k, v) in props {
        obj.set_named_property(k, value_to_js(env, v)?)?;
    }
    Ok(obj)
}

/// Create a JavaScript Vertex object from Rust data.
pub fn create_vertex_js(
    env: Env,
    id: VertexId,
    label: &str,
    properties: &HashMap<String, Value>,
) -> Result<Object> {
    let mut obj = env.create_object()?;

    // Set id as BigInt
    obj.set_named_property("id", env.create_bigint_from_u64(id.0)?)?;

    // Set label
    obj.set_named_property("label", env.create_string(label)?)?;

    // Set properties
    let props_js = properties_to_js(env, properties)?;
    obj.set_named_property("properties", props_js)?;

    Ok(obj)
}

/// Create a JavaScript Edge object from Rust data.
pub fn create_edge_js(
    env: Env,
    id: EdgeId,
    label: &str,
    from: VertexId,
    to: VertexId,
    properties: &HashMap<String, Value>,
) -> Result<Object> {
    let mut obj = env.create_object()?;

    // Set id as BigInt
    obj.set_named_property("id", env.create_bigint_from_u64(id.0)?)?;

    // Set label
    obj.set_named_property("label", env.create_string(label)?)?;

    // Set from (source vertex ID)
    obj.set_named_property("from", env.create_bigint_from_u64(from.0)?)?;

    // Set to (target vertex ID)
    obj.set_named_property("to", env.create_bigint_from_u64(to.0)?)?;

    // Set properties
    let props_js = properties_to_js(env, properties)?;
    obj.set_named_property("properties", props_js)?;

    Ok(obj)
}

/// Parse a JsValue to VertexId.
pub fn js_to_vertex_id(env: Env, js: JsUnknown) -> Result<VertexId> {
    let id = js_to_u64(env, js)?;
    Ok(VertexId(id))
}

/// Parse a JsValue to EdgeId.
pub fn js_to_edge_id(env: Env, js: JsUnknown) -> Result<EdgeId> {
    let id = js_to_u64(env, js)?;
    Ok(EdgeId(id))
}

/// Parse a bigint or number from JsValue to u64.
pub fn js_to_u64(_env: Env, js: JsUnknown) -> Result<u64> {
    match js.get_type()? {
        ValueType::BigInt => {
            let bigint = unsafe { js.cast::<napi::JsBigInt>() };
            let (value, lossless) = bigint.get_u64()?;
            if !lossless {
                return Err(Error::new(
                    Status::InvalidArg,
                    "BigInt value out of u64 range",
                ));
            }
            Ok(value)
        }
        ValueType::Number => {
            let num = js.coerce_to_number()?.get_double()?;
            if num >= 0.0 && num <= u64::MAX as f64 && num.fract() == 0.0 {
                Ok(num as u64)
            } else {
                Err(Error::new(
                    Status::InvalidArg,
                    "Number must be a non-negative integer",
                ))
            }
        }
        _ => Err(Error::new(Status::InvalidArg, "Expected bigint or number")),
    }
}

/// Parse a JsValue (single ID or array of IDs) to Vec<VertexId>.
pub fn js_array_to_vertex_ids(env: Env, js: JsUnknown) -> Result<Vec<VertexId>> {
    match js.get_type()? {
        ValueType::Null | ValueType::Undefined => Ok(Vec::new()),
        ValueType::BigInt | ValueType::Number => Ok(vec![js_to_vertex_id(env, js)?]),
        ValueType::Object => {
            let obj = js.coerce_to_object()?;
            if obj.is_array()? {
                let len = obj.get_array_length()?;
                let mut ids = Vec::with_capacity(len as usize);
                for i in 0..len {
                    let item: JsUnknown = obj.get_element(i)?;
                    ids.push(js_to_vertex_id(env, item)?);
                }
                Ok(ids)
            } else {
                Err(Error::new(
                    Status::InvalidArg,
                    "Expected vertex ID or array of IDs",
                ))
            }
        }
        _ => Err(Error::new(
            Status::InvalidArg,
            "Expected vertex ID (bigint/number) or array of IDs",
        )),
    }
}

/// Parse a JsValue (single ID or array of IDs) to Vec<EdgeId>.
pub fn js_array_to_edge_ids(env: Env, js: JsUnknown) -> Result<Vec<EdgeId>> {
    match js.get_type()? {
        ValueType::Null | ValueType::Undefined => Ok(Vec::new()),
        ValueType::BigInt | ValueType::Number => Ok(vec![js_to_edge_id(env, js)?]),
        ValueType::Object => {
            let obj = js.coerce_to_object()?;
            if obj.is_array()? {
                let len = obj.get_array_length()?;
                let mut ids = Vec::with_capacity(len as usize);
                for i in 0..len {
                    let item: JsUnknown = obj.get_element(i)?;
                    ids.push(js_to_edge_id(env, item)?);
                }
                Ok(ids)
            } else {
                Err(Error::new(
                    Status::InvalidArg,
                    "Expected edge ID or array of IDs",
                ))
            }
        }
        _ => Err(Error::new(
            Status::InvalidArg,
            "Expected edge ID (bigint/number) or array of IDs",
        )),
    }
}

/// Parse a JsValue array to Vec<String>.
pub fn js_array_to_strings(_env: Env, js: Option<JsUnknown>) -> Result<Vec<String>> {
    match js {
        None => Ok(Vec::new()),
        Some(js) => match js.get_type()? {
            ValueType::Null | ValueType::Undefined => Ok(Vec::new()),
            ValueType::String => {
                let s: String = js.coerce_to_string()?.into_utf8()?.as_str()?.to_string();
                Ok(vec![s])
            }
            ValueType::Object => {
                let obj = js.coerce_to_object()?;
                if obj.is_array()? {
                    let len = obj.get_array_length()?;
                    let mut strings = Vec::with_capacity(len as usize);
                    for i in 0..len {
                        let item: JsUnknown = obj.get_element(i)?;
                        let s: String = item.coerce_to_string()?.into_utf8()?.as_str()?.to_string();
                        strings.push(s);
                    }
                    Ok(strings)
                } else {
                    Err(Error::new(Status::InvalidArg, "Expected array of strings"))
                }
            }
            _ => Err(Error::new(
                Status::InvalidArg,
                "Expected string or array of strings",
            )),
        },
    }
}

/// Convert a Vec<Value> to a JavaScript Array.
pub fn values_to_js_array(env: Env, values: Vec<Value>) -> Result<JsUnknown> {
    let mut arr = env.create_array_with_length(values.len())?;
    for (i, val) in values.iter().enumerate() {
        arr.set_element(i as u32, value_to_js(env, val)?)?;
    }
    Ok(arr.into_unknown())
}

/// Parse a JsValue array to Vec<Value>.
pub fn js_array_to_values(env: Env, js: JsUnknown) -> Result<Vec<Value>> {
    match js.get_type()? {
        ValueType::Null | ValueType::Undefined => Ok(Vec::new()),
        ValueType::Object => {
            let obj = unsafe { js.cast::<JsObject>() };
            if obj.is_array()? {
                let len = obj.get_array_length()?;
                let mut values = Vec::with_capacity(len as usize);
                for i in 0..len {
                    let item: JsUnknown = obj.get_element(i)?;
                    values.push(js_to_value(env, item)?);
                }
                Ok(values)
            } else {
                // Single object value
                Ok(vec![js_to_value(env, obj.into_unknown())?])
            }
        }
        _ => {
            // Single value - wrap in vec
            Ok(vec![js_to_value(env, js)?])
        }
    }
}
