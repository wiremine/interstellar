use std::collections::HashMap;

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Ord, PartialOrd)]
pub struct VertexId(pub(crate) u64);

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Ord, PartialOrd)]
pub struct EdgeId(pub(crate) u64);

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ElementId {
    Vertex(VertexId),
    Edge(EdgeId),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<Value>),
    Map(HashMap<String, Value>),
}

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
        Value::Map(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_primitives_into_value() {
        let bool_v: Value = true.into();
        let int_v: Value = 42i32.into();
        let uint_v: Value = 7u32.into();
        let float_v: Value = 3.14f32.into();
        let double_v: Value = 6.28f64.into();
        let string_v: Value = "hello".into();

        assert_eq!(bool_v, Value::Bool(true));
        assert_eq!(int_v, Value::Int(42));
        assert_eq!(uint_v, Value::Int(7));
        assert!(matches!(float_v, Value::Float(v) if (v - 3.14f64).abs() < 1e-6));
        assert!(matches!(double_v, Value::Float(v) if (v - 6.28f64).abs() < 1e-12));
        assert_eq!(string_v, Value::String("hello".to_string()));
    }

    #[test]
    fn converts_collections_into_value() {
        let list_v: Value = vec![Value::Int(1), Value::Bool(false)].into();

        let mut map = HashMap::new();
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
}
