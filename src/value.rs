use std::collections::{BTreeMap, HashMap};

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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ComparableValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(OrderedFloat),
    String(String),
    List(Vec<ComparableValue>),
    Map(BTreeMap<String, ComparableValue>),
}

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

impl Value {
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
        }
    }

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
                let mut map = HashMap::with_capacity(len);
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
            _ => None,
        }
    }

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

    #[test]
    fn serializes_and_deserializes_roundtrip() {
        let mut original_map = HashMap::new();
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

    fn arb_value() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(Value::Int),
            any::<f64>().prop_map(Value::Float),
            "[a-zA-Z0-9]{0,8}".prop_map(|s| Value::String(s)),
        ];

        leaf.prop_recursive(4, 64, 8, |inner| {
            let list = prop::collection::vec(inner.clone(), 0..4).prop_map(Value::List);
            let map =
                prop::collection::hash_map("[a-zA-Z0-9]{0,6}", inner, 0..4).prop_map(Value::Map);
            prop_oneof![list, map]
        })
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
    }
}
