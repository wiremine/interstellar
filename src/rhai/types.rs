//! Type registrations for Rhai.
//!
//! This module registers `Value`, `VertexId`, and `EdgeId` with Rhai's type system,
//! and provides conversion functions between Rhai's `Dynamic` and Intersteller's `Value`.

use rhai::{Dynamic, Engine, ImmutableString, Map as RhaiMap};
use std::collections::HashMap;

use crate::value::{EdgeId, Value, VertexId};

/// Converts a Rhai `Dynamic` value to an Intersteller `Value`.
///
/// This function handles conversion from Rhai's dynamic type system to
/// Intersteller's `Value` enum. Native Rhai types are converted to their
/// corresponding `Value` variants.
///
/// # Type Mapping
///
/// | Rhai Type | Value Variant |
/// |-----------|---------------|
/// | `()` (unit) | `Value::Null` |
/// | `bool` | `Value::Bool` |
/// | `i64` | `Value::Int` |
/// | `f64` | `Value::Float` |
/// | `String`/`ImmutableString` | `Value::String` |
/// | `Array` | `Value::List` |
/// | `Map` | `Value::Map` |
/// | `VertexId` | `Value::Vertex` |
/// | `EdgeId` | `Value::Edge` |
/// | `Value` | passthrough |
///
/// Unknown types are converted to `Value::Null`.
pub fn dynamic_to_value(d: Dynamic) -> Value {
    if d.is_unit() {
        return Value::Null;
    }

    if d.is::<bool>() {
        return Value::Bool(d.cast::<bool>());
    }

    if d.is::<i64>() {
        return Value::Int(d.cast::<i64>());
    }

    if d.is::<f64>() {
        return Value::Float(d.cast::<f64>());
    }

    if d.is::<ImmutableString>() {
        return Value::String(d.cast::<ImmutableString>().to_string());
    }

    if d.is::<String>() {
        return Value::String(d.cast::<String>());
    }

    if d.is_array() {
        let arr = d.cast::<rhai::Array>();
        let values: Vec<Value> = arr.into_iter().map(dynamic_to_value).collect();
        return Value::List(values);
    }

    if d.is_map() {
        let map = d.cast::<RhaiMap>();
        let values: HashMap<String, Value> = map
            .into_iter()
            .map(|(k, v)| (k.to_string(), dynamic_to_value(v)))
            .collect();
        return Value::Map(values);
    }

    // Handle our custom types
    if d.is::<VertexId>() {
        return Value::Vertex(d.cast::<VertexId>());
    }

    if d.is::<EdgeId>() {
        return Value::Edge(d.cast::<EdgeId>());
    }

    if d.is::<Value>() {
        return d.cast::<Value>();
    }

    // Unknown type - return null
    Value::Null
}

/// Converts an Intersteller `Value` to a Rhai `Dynamic`.
///
/// This function handles conversion from Intersteller's `Value` enum to
/// Rhai's dynamic type system.
///
/// # Type Mapping
///
/// | Value Variant | Rhai Type |
/// |---------------|-----------|
/// | `Value::Null` | `()` (unit) |
/// | `Value::Bool` | `bool` |
/// | `Value::Int` | `i64` |
/// | `Value::Float` | `f64` |
/// | `Value::String` | `ImmutableString` |
/// | `Value::List` | `Array` |
/// | `Value::Map` | `Map` |
/// | `Value::Vertex` | `VertexId` |
/// | `Value::Edge` | `EdgeId` |
pub fn value_to_dynamic(v: Value) -> Dynamic {
    match v {
        Value::Null => Dynamic::UNIT,
        Value::Bool(b) => Dynamic::from(b),
        Value::Int(i) => Dynamic::from(i),
        Value::Float(f) => Dynamic::from(f),
        Value::String(s) => Dynamic::from(s),
        Value::List(list) => {
            let arr: rhai::Array = list.into_iter().map(value_to_dynamic).collect();
            Dynamic::from(arr)
        }
        Value::Map(map) => {
            let rhai_map: RhaiMap = map
                .into_iter()
                .map(|(k, v)| (k.into(), value_to_dynamic(v)))
                .collect();
            Dynamic::from(rhai_map)
        }
        Value::Vertex(vid) => Dynamic::from(vid),
        Value::Edge(eid) => Dynamic::from(eid),
    }
}

/// Registers all core types with the Rhai engine.
///
/// This function registers:
/// - `VertexId` with constructor, getter, and display
/// - `EdgeId` with constructor, getter, and display
/// - `Value` with constructors, type checks, and extractors
pub fn register_types(engine: &mut Engine) {
    register_vertex_id(engine);
    register_edge_id(engine);
    register_value(engine);
}

/// Registers `VertexId` with the Rhai engine.
fn register_vertex_id(engine: &mut Engine) {
    // Register the type
    engine.register_type_with_name::<VertexId>("VertexId");

    // Constructor: vertex_id(123)
    engine.register_fn("vertex_id", |id: i64| VertexId(id as u64));

    // Getter: .id
    engine.register_get("id", |vid: &mut VertexId| vid.0 as i64);

    // Display: to_string()
    engine.register_fn("to_string", |vid: &mut VertexId| {
        format!("VertexId({})", vid.0)
    });

    // Debug representation
    engine.register_fn("to_debug", |vid: &mut VertexId| format!("{:?}", vid));
}

/// Registers `EdgeId` with the Rhai engine.
fn register_edge_id(engine: &mut Engine) {
    // Register the type
    engine.register_type_with_name::<EdgeId>("EdgeId");

    // Constructor: edge_id(123)
    engine.register_fn("edge_id", |id: i64| EdgeId(id as u64));

    // Getter: .id
    engine.register_get("id", |eid: &mut EdgeId| eid.0 as i64);

    // Display: to_string()
    engine.register_fn("to_string", |eid: &mut EdgeId| format!("EdgeId({})", eid.0));

    // Debug representation
    engine.register_fn("to_debug", |eid: &mut EdgeId| format!("{:?}", eid));
}

/// Registers `Value` with the Rhai engine.
fn register_value(engine: &mut Engine) {
    // Register the type
    engine.register_type_with_name::<Value>("Value");

    // === Constructors ===

    // value_int(42)
    engine.register_fn("value_int", |i: i64| Value::Int(i));

    // value_float(3.14)
    engine.register_fn("value_float", |f: f64| Value::Float(f));

    // value_string("hello")
    engine.register_fn("value_string", |s: ImmutableString| {
        Value::String(s.to_string())
    });

    // value_bool(true)
    engine.register_fn("value_bool", |b: bool| Value::Bool(b));

    // value_null()
    engine.register_fn("value_null", || Value::Null);

    // value_list([1, 2, 3])
    engine.register_fn("value_list", |arr: rhai::Array| {
        Value::List(arr.into_iter().map(dynamic_to_value).collect())
    });

    // value_map(#{ a: 1, b: 2 })
    engine.register_fn("value_map", |map: RhaiMap| {
        let values: HashMap<String, Value> = map
            .into_iter()
            .map(|(k, v)| (k.to_string(), dynamic_to_value(v)))
            .collect();
        Value::Map(values)
    });

    // === Type Checks ===

    engine.register_fn("is_null", |v: &mut Value| matches!(v, Value::Null));
    engine.register_fn("is_bool", |v: &mut Value| matches!(v, Value::Bool(_)));
    engine.register_fn("is_int", |v: &mut Value| matches!(v, Value::Int(_)));
    engine.register_fn("is_float", |v: &mut Value| matches!(v, Value::Float(_)));
    engine.register_fn("is_string", |v: &mut Value| matches!(v, Value::String(_)));
    engine.register_fn("is_list", |v: &mut Value| matches!(v, Value::List(_)));
    engine.register_fn("is_map", |v: &mut Value| matches!(v, Value::Map(_)));
    engine.register_fn("is_vertex", |v: &mut Value| matches!(v, Value::Vertex(_)));
    engine.register_fn("is_edge", |v: &mut Value| matches!(v, Value::Edge(_)));

    // === Extractors ===

    // as_int() -> i64 or throws
    engine.register_fn(
        "as_int",
        |v: &mut Value| -> Result<i64, Box<rhai::EvalAltResult>> {
            match v {
                Value::Int(i) => Ok(*i),
                Value::Float(f) => Ok(*f as i64),
                _ => Err(format!("Cannot convert {:?} to int", v).into()),
            }
        },
    );

    // as_float() -> f64 or throws
    engine.register_fn(
        "as_float",
        |v: &mut Value| -> Result<f64, Box<rhai::EvalAltResult>> {
            match v {
                Value::Float(f) => Ok(*f),
                Value::Int(i) => Ok(*i as f64),
                _ => Err(format!("Cannot convert {:?} to float", v).into()),
            }
        },
    );

    // as_string() -> String or throws
    engine.register_fn(
        "as_string",
        |v: &mut Value| -> Result<ImmutableString, Box<rhai::EvalAltResult>> {
            match v {
                Value::String(s) => Ok(s.clone().into()),
                _ => Err(format!("Cannot convert {:?} to string", v).into()),
            }
        },
    );

    // as_bool() -> bool or throws
    engine.register_fn(
        "as_bool",
        |v: &mut Value| -> Result<bool, Box<rhai::EvalAltResult>> {
            match v {
                Value::Bool(b) => Ok(*b),
                _ => Err(format!("Cannot convert {:?} to bool", v).into()),
            }
        },
    );

    // as_list() -> Array or throws
    engine.register_fn(
        "as_list",
        |v: &mut Value| -> Result<rhai::Array, Box<rhai::EvalAltResult>> {
            match v {
                Value::List(list) => Ok(list.iter().cloned().map(value_to_dynamic).collect()),
                _ => Err(format!("Cannot convert {:?} to list", v).into()),
            }
        },
    );

    // as_map() -> Map or throws
    engine.register_fn(
        "as_map",
        |v: &mut Value| -> Result<RhaiMap, Box<rhai::EvalAltResult>> {
            match v {
                Value::Map(map) => Ok(map
                    .iter()
                    .map(|(k, v)| (k.clone().into(), value_to_dynamic(v.clone())))
                    .collect()),
                _ => Err(format!("Cannot convert {:?} to map", v).into()),
            }
        },
    );

    // as_vertex_id() -> VertexId or throws
    engine.register_fn(
        "as_vertex_id",
        |v: &mut Value| -> Result<VertexId, Box<rhai::EvalAltResult>> {
            match v {
                Value::Vertex(vid) => Ok(*vid),
                _ => Err(format!("Cannot convert {:?} to VertexId", v).into()),
            }
        },
    );

    // as_edge_id() -> EdgeId or throws
    engine.register_fn(
        "as_edge_id",
        |v: &mut Value| -> Result<EdgeId, Box<rhai::EvalAltResult>> {
            match v {
                Value::Edge(eid) => Ok(*eid),
                _ => Err(format!("Cannot convert {:?} to EdgeId", v).into()),
            }
        },
    );

    // === Display ===

    engine.register_fn("to_string", |v: &mut Value| format!("{:?}", v));
    engine.register_fn("to_debug", |v: &mut Value| format!("{:?}", v));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dynamic_to_value_primitives() {
        assert_eq!(dynamic_to_value(Dynamic::UNIT), Value::Null);
        assert_eq!(dynamic_to_value(Dynamic::from(true)), Value::Bool(true));
        assert_eq!(dynamic_to_value(Dynamic::from(42i64)), Value::Int(42));
        assert_eq!(dynamic_to_value(Dynamic::from(3.14f64)), Value::Float(3.14));
        assert_eq!(
            dynamic_to_value(Dynamic::from("hello")),
            Value::String("hello".to_string())
        );
    }

    #[test]
    fn test_dynamic_to_value_collections() {
        // Array
        let arr: rhai::Array = vec![Dynamic::from(1i64), Dynamic::from(2i64)];
        assert_eq!(
            dynamic_to_value(Dynamic::from(arr)),
            Value::List(vec![Value::Int(1), Value::Int(2)])
        );

        // Map
        let mut map = RhaiMap::new();
        map.insert("a".into(), Dynamic::from(1i64));
        assert_eq!(
            dynamic_to_value(Dynamic::from(map)),
            Value::Map([("a".to_string(), Value::Int(1))].into_iter().collect())
        );
    }

    #[test]
    fn test_dynamic_to_value_graph_types() {
        assert_eq!(
            dynamic_to_value(Dynamic::from(VertexId(42))),
            Value::Vertex(VertexId(42))
        );
        assert_eq!(
            dynamic_to_value(Dynamic::from(EdgeId(99))),
            Value::Edge(EdgeId(99))
        );
    }

    #[test]
    fn test_value_to_dynamic_primitives() {
        assert!(value_to_dynamic(Value::Null).is_unit());
        assert_eq!(value_to_dynamic(Value::Bool(true)).cast::<bool>(), true);
        assert_eq!(value_to_dynamic(Value::Int(42)).cast::<i64>(), 42);
        assert_eq!(value_to_dynamic(Value::Float(3.14)).cast::<f64>(), 3.14);
        assert_eq!(
            value_to_dynamic(Value::String("hello".to_string()))
                .into_string()
                .unwrap(),
            "hello"
        );
    }

    #[test]
    fn test_value_to_dynamic_collections() {
        // List
        let list = Value::List(vec![Value::Int(1), Value::Int(2)]);
        let dyn_list = value_to_dynamic(list);
        assert!(dyn_list.is_array());
        let arr = dyn_list.cast::<rhai::Array>();
        assert_eq!(arr.len(), 2);

        // Map
        let map = Value::Map([("a".to_string(), Value::Int(1))].into_iter().collect());
        let dyn_map = value_to_dynamic(map);
        assert!(dyn_map.is_map());
    }

    #[test]
    fn test_value_to_dynamic_graph_types() {
        let vid = value_to_dynamic(Value::Vertex(VertexId(42)));
        assert!(vid.is::<VertexId>());
        assert_eq!(vid.cast::<VertexId>(), VertexId(42));

        let eid = value_to_dynamic(Value::Edge(EdgeId(99)));
        assert!(eid.is::<EdgeId>());
        assert_eq!(eid.cast::<EdgeId>(), EdgeId(99));
    }

    #[test]
    fn test_roundtrip() {
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int(42),
            Value::Float(3.14),
            Value::String("hello".to_string()),
            Value::Vertex(VertexId(1)),
            Value::Edge(EdgeId(2)),
            Value::List(vec![Value::Int(1), Value::Int(2)]),
        ];

        for v in values {
            let d = value_to_dynamic(v.clone());
            let v2 = dynamic_to_value(d);
            assert_eq!(v, v2, "Roundtrip failed for {:?}", v);
        }
    }

    #[test]
    fn test_register_types() {
        let mut engine = Engine::new();
        register_types(&mut engine);

        // Test VertexId constructor
        let result: VertexId = engine.eval("vertex_id(42)").unwrap();
        assert_eq!(result, VertexId(42));

        // Test VertexId getter
        let id: i64 = engine.eval("vertex_id(42).id").unwrap();
        assert_eq!(id, 42);

        // Test EdgeId constructor
        let result: EdgeId = engine.eval("edge_id(99)").unwrap();
        assert_eq!(result, EdgeId(99));

        // Test Value constructors
        let val: Value = engine.eval("value_int(42)").unwrap();
        assert_eq!(val, Value::Int(42));

        let val: Value = engine.eval("value_string(\"hello\")").unwrap();
        assert_eq!(val, Value::String("hello".to_string()));

        let val: Value = engine.eval("value_null()").unwrap();
        assert_eq!(val, Value::Null);
    }

    #[test]
    fn test_value_type_checks() {
        let mut engine = Engine::new();
        register_types(&mut engine);

        let is_int: bool = engine.eval("value_int(42).is_int()").unwrap();
        assert!(is_int);

        let is_string: bool = engine.eval("value_int(42).is_string()").unwrap();
        assert!(!is_string);

        let is_null: bool = engine.eval("value_null().is_null()").unwrap();
        assert!(is_null);
    }

    #[test]
    fn test_value_extractors() {
        let mut engine = Engine::new();
        register_types(&mut engine);

        let val: i64 = engine.eval("value_int(42).as_int()").unwrap();
        assert_eq!(val, 42);

        let val: f64 = engine.eval("value_float(3.14).as_float()").unwrap();
        assert!((val - 3.14).abs() < 0.001);

        // Test error on wrong type
        let result: Result<bool, _> = engine.eval("value_int(42).as_bool()");
        assert!(result.is_err());
    }
}
