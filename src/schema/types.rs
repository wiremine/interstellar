//! Schema type definitions.
//!
//! This module defines the core schema types used to describe graph structure:
//!
//! - [`VertexSchema`] - Defines allowed properties and constraints for vertices with a label
//! - [`EdgeSchema`] - Defines endpoint constraints and properties for edges with a label
//! - [`PropertyDef`] - Definition of a single property including type and constraints
//! - [`PropertyType`] - The expected type of a property value

use std::collections::HashMap;

use crate::value::Value;

/// Schema for vertices with a specific label.
///
/// Defines the property constraints for vertices. When a schema is defined
/// for a label, mutations can validate that vertices with that label
/// conform to the schema.
///
/// # Example
///
/// ```
/// use intersteller::schema::{VertexSchema, PropertyDef, PropertyType};
/// use std::collections::HashMap;
///
/// let mut properties = HashMap::new();
/// properties.insert("name".to_string(), PropertyDef {
///     key: "name".to_string(),
///     value_type: PropertyType::String,
///     required: true,
///     default: None,
/// });
///
/// let schema = VertexSchema {
///     label: "Person".to_string(),
///     properties,
///     additional_properties: false,
/// };
///
/// assert!(schema.properties.get("name").unwrap().required);
/// ```
#[derive(Clone, Debug)]
pub struct VertexSchema {
    /// The vertex label this schema applies to.
    pub label: String,

    /// Property definitions keyed by property name.
    pub properties: HashMap<String, PropertyDef>,

    /// Whether to allow properties not defined in the schema.
    ///
    /// If `false` (default), any property not in the schema will cause
    /// validation to fail in STRICT or CLOSED mode.
    pub additional_properties: bool,
}

impl VertexSchema {
    /// Get all required property keys.
    ///
    /// Returns an iterator over property keys that are marked as required.
    pub fn required_properties(&self) -> impl Iterator<Item = &str> {
        self.properties
            .iter()
            .filter(|(_, def)| def.required)
            .map(|(key, _)| key.as_str())
    }

    /// Get all optional property keys.
    ///
    /// Returns an iterator over property keys that are not required.
    pub fn optional_properties(&self) -> impl Iterator<Item = &str> {
        self.properties
            .iter()
            .filter(|(_, def)| !def.required)
            .map(|(key, _)| key.as_str())
    }

    /// Get type for a property.
    ///
    /// Returns the expected type if the property is defined, `None` otherwise.
    pub fn property_type(&self, key: &str) -> Option<&PropertyType> {
        self.properties.get(key).map(|def| &def.value_type)
    }

    /// Get default value for a property.
    ///
    /// Returns the default value if one is defined, `None` otherwise.
    pub fn property_default(&self, key: &str) -> Option<&Value> {
        self.properties
            .get(key)
            .and_then(|def| def.default.as_ref())
    }
}

/// Schema for edges with a specific label.
///
/// Defines endpoint constraints (which vertex types can be connected)
/// and property constraints for edges.
///
/// # Example
///
/// ```
/// use intersteller::schema::{EdgeSchema, PropertyDef, PropertyType};
/// use std::collections::HashMap;
///
/// let schema = EdgeSchema {
///     label: "KNOWS".to_string(),
///     from_labels: vec!["Person".to_string()],
///     to_labels: vec!["Person".to_string()],
///     properties: HashMap::new(),
///     additional_properties: false,
/// };
///
/// assert!(schema.allows_from("Person"));
/// assert!(!schema.allows_from("Company"));
/// ```
#[derive(Clone, Debug)]
pub struct EdgeSchema {
    /// The edge label this schema applies to.
    pub label: String,

    /// Allowed source vertex labels.
    ///
    /// The source vertex of an edge with this label must have one of these labels.
    pub from_labels: Vec<String>,

    /// Allowed target vertex labels.
    ///
    /// The target vertex of an edge with this label must have one of these labels.
    pub to_labels: Vec<String>,

    /// Property definitions keyed by property name.
    pub properties: HashMap<String, PropertyDef>,

    /// Whether to allow properties not defined in the schema.
    pub additional_properties: bool,
}

impl EdgeSchema {
    /// Check if a source label is allowed.
    ///
    /// Returns `true` if the given label is in the list of allowed source labels.
    pub fn allows_from(&self, label: &str) -> bool {
        self.from_labels.iter().any(|l| l == label)
    }

    /// Check if a target label is allowed.
    ///
    /// Returns `true` if the given label is in the list of allowed target labels.
    pub fn allows_to(&self, label: &str) -> bool {
        self.to_labels.iter().any(|l| l == label)
    }

    /// Get all required property keys.
    pub fn required_properties(&self) -> impl Iterator<Item = &str> {
        self.properties
            .iter()
            .filter(|(_, def)| def.required)
            .map(|(key, _)| key.as_str())
    }

    /// Get type for a property.
    pub fn property_type(&self, key: &str) -> Option<&PropertyType> {
        self.properties.get(key).map(|def| &def.value_type)
    }

    /// Get default value for a property.
    pub fn property_default(&self, key: &str) -> Option<&Value> {
        self.properties
            .get(key)
            .and_then(|def| def.default.as_ref())
    }
}

/// Definition of a single property.
///
/// Describes the expected type, whether the property is required,
/// and an optional default value.
#[derive(Clone, Debug)]
pub struct PropertyDef {
    /// Property key name.
    pub key: String,

    /// Expected value type.
    pub value_type: PropertyType,

    /// Is this property required?
    ///
    /// If `true`, the property must be present and non-null on creation.
    pub required: bool,

    /// Default value if not provided (applied at query time).
    ///
    /// Defaults are not stored physically; they are applied when reading
    /// properties if the value is missing.
    pub default: Option<Value>,
}

/// Property types that can be validated.
///
/// Maps to [`Value`] variants for type checking during validation.
///
/// # Example
///
/// ```
/// use intersteller::schema::PropertyType;
/// use intersteller::value::Value;
///
/// let pt = PropertyType::Int;
/// assert!(pt.matches(&Value::Int(42)));
/// assert!(!pt.matches(&Value::String("42".to_string())));
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PropertyType {
    /// Any type allowed.
    Any,
    /// `Value::Bool`
    Bool,
    /// `Value::Int`
    Int,
    /// `Value::Float`
    Float,
    /// `Value::String`
    String,
    /// `Value::List` with optional element type.
    List(Option<Box<PropertyType>>),
    /// `Value::Map` with optional value type.
    Map(Option<Box<PropertyType>>),
}

impl PropertyType {
    /// Check if a Value matches this type.
    ///
    /// Returns `true` if the value is compatible with this type definition.
    /// Note that `Value::Null` always returns `false` here; null handling
    /// is done separately based on the `required` flag.
    ///
    /// # Examples
    ///
    /// ```
    /// use intersteller::schema::PropertyType;
    /// use intersteller::value::Value;
    ///
    /// // Simple types
    /// assert!(PropertyType::Bool.matches(&Value::Bool(true)));
    /// assert!(PropertyType::Int.matches(&Value::Int(42)));
    /// assert!(!PropertyType::Int.matches(&Value::String("42".to_string())));
    ///
    /// // Any matches everything
    /// assert!(PropertyType::Any.matches(&Value::Int(42)));
    /// assert!(PropertyType::Any.matches(&Value::String("test".to_string())));
    ///
    /// // List with element type
    /// let list_of_ints = PropertyType::List(Some(Box::new(PropertyType::Int)));
    /// assert!(list_of_ints.matches(&Value::List(vec![Value::Int(1), Value::Int(2)])));
    /// assert!(!list_of_ints.matches(&Value::List(vec![Value::String("a".to_string())])));
    /// ```
    pub fn matches(&self, value: &Value) -> bool {
        match (self, value) {
            // Any matches everything except Null (null handling is separate)
            (PropertyType::Any, Value::Null) => false,
            (PropertyType::Any, _) => true,

            // Direct type matches
            (PropertyType::Bool, Value::Bool(_)) => true,
            (PropertyType::Int, Value::Int(_)) => true,
            (PropertyType::Float, Value::Float(_)) => true,
            (PropertyType::String, Value::String(_)) => true,

            // List with optional element type
            (PropertyType::List(None), Value::List(_)) => true,
            (PropertyType::List(Some(elem_type)), Value::List(items)) => {
                items.iter().all(|item| elem_type.matches(item))
            }

            // Map with optional value type
            (PropertyType::Map(None), Value::Map(_)) => true,
            (PropertyType::Map(Some(val_type)), Value::Map(map)) => {
                map.values().all(|v| val_type.matches(v))
            }

            // Null is handled separately based on required flag
            (_, Value::Null) => false,

            _ => false,
        }
    }

    /// Get a human-readable name for this type.
    pub fn type_name(&self) -> &'static str {
        match self {
            PropertyType::Any => "ANY",
            PropertyType::Bool => "BOOL",
            PropertyType::Int => "INT",
            PropertyType::Float => "FLOAT",
            PropertyType::String => "STRING",
            PropertyType::List(_) => "LIST",
            PropertyType::Map(_) => "MAP",
        }
    }
}

impl std::fmt::Display for PropertyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PropertyType::Any => write!(f, "ANY"),
            PropertyType::Bool => write!(f, "BOOL"),
            PropertyType::Int => write!(f, "INT"),
            PropertyType::Float => write!(f, "FLOAT"),
            PropertyType::String => write!(f, "STRING"),
            PropertyType::List(None) => write!(f, "LIST"),
            PropertyType::List(Some(t)) => write!(f, "LIST<{}>", t),
            PropertyType::Map(None) => write!(f, "MAP"),
            PropertyType::Map(Some(t)) => write!(f, "MAP<{}>", t),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn property_type_matches_bool() {
        assert!(PropertyType::Bool.matches(&Value::Bool(true)));
        assert!(PropertyType::Bool.matches(&Value::Bool(false)));
        assert!(!PropertyType::Bool.matches(&Value::Int(1)));
        assert!(!PropertyType::Bool.matches(&Value::String("true".to_string())));
    }

    #[test]
    fn property_type_matches_int() {
        assert!(PropertyType::Int.matches(&Value::Int(42)));
        assert!(PropertyType::Int.matches(&Value::Int(-1)));
        assert!(!PropertyType::Int.matches(&Value::Float(42.0)));
        assert!(!PropertyType::Int.matches(&Value::Bool(true)));
    }

    #[test]
    fn property_type_matches_float() {
        assert!(PropertyType::Float.matches(&Value::Float(3.14)));
        assert!(PropertyType::Float.matches(&Value::Float(-0.5)));
        assert!(!PropertyType::Float.matches(&Value::Int(42)));
        assert!(!PropertyType::Float.matches(&Value::String("3.14".to_string())));
    }

    #[test]
    fn property_type_matches_string() {
        assert!(PropertyType::String.matches(&Value::String("hello".to_string())));
        assert!(PropertyType::String.matches(&Value::String("".to_string())));
        assert!(!PropertyType::String.matches(&Value::Int(42)));
        assert!(!PropertyType::String.matches(&Value::Bool(true)));
    }

    #[test]
    fn property_type_matches_list_untyped() {
        let pt = PropertyType::List(None);
        assert!(pt.matches(&Value::List(vec![
            Value::Int(1),
            Value::String("a".to_string())
        ])));
        assert!(pt.matches(&Value::List(vec![])));
        assert!(!pt.matches(&Value::Int(1)));
    }

    #[test]
    fn property_type_matches_list_with_element_type() {
        let list_of_ints = PropertyType::List(Some(Box::new(PropertyType::Int)));
        assert!(list_of_ints.matches(&Value::List(vec![Value::Int(1), Value::Int(2)])));
        assert!(list_of_ints.matches(&Value::List(vec![]))); // Empty list matches
        assert!(!list_of_ints.matches(&Value::List(vec![Value::String("a".to_string())])));
        assert!(!list_of_ints.matches(&Value::List(vec![
            Value::Int(1),
            Value::String("a".to_string())
        ])));
    }

    #[test]
    fn property_type_matches_map_untyped() {
        let pt = PropertyType::Map(None);
        let mut map = HashMap::new();
        map.insert("a".to_string(), Value::Int(1));
        map.insert("b".to_string(), Value::String("x".to_string()));
        assert!(pt.matches(&Value::Map(map)));
        assert!(pt.matches(&Value::Map(HashMap::new())));
        assert!(!pt.matches(&Value::List(vec![])));
    }

    #[test]
    fn property_type_matches_map_with_value_type() {
        let map_of_ints = PropertyType::Map(Some(Box::new(PropertyType::Int)));

        let mut valid_map = HashMap::new();
        valid_map.insert("a".to_string(), Value::Int(1));
        valid_map.insert("b".to_string(), Value::Int(2));
        assert!(map_of_ints.matches(&Value::Map(valid_map)));

        let mut invalid_map = HashMap::new();
        invalid_map.insert("a".to_string(), Value::String("x".to_string()));
        assert!(!map_of_ints.matches(&Value::Map(invalid_map)));
    }

    #[test]
    fn property_type_any_matches_everything_except_null() {
        assert!(PropertyType::Any.matches(&Value::Bool(true)));
        assert!(PropertyType::Any.matches(&Value::Int(42)));
        assert!(PropertyType::Any.matches(&Value::Float(3.14)));
        assert!(PropertyType::Any.matches(&Value::String("hello".to_string())));
        assert!(PropertyType::Any.matches(&Value::List(vec![])));
        assert!(PropertyType::Any.matches(&Value::Map(HashMap::new())));
        assert!(!PropertyType::Any.matches(&Value::Null)); // Null is special
    }

    #[test]
    fn property_type_null_always_fails_match() {
        // Null handling is done separately via required flag
        assert!(!PropertyType::Bool.matches(&Value::Null));
        assert!(!PropertyType::Int.matches(&Value::Null));
        assert!(!PropertyType::Float.matches(&Value::Null));
        assert!(!PropertyType::String.matches(&Value::Null));
        assert!(!PropertyType::List(None).matches(&Value::Null));
        assert!(!PropertyType::Map(None).matches(&Value::Null));
        assert!(!PropertyType::Any.matches(&Value::Null));
    }

    #[test]
    fn property_type_display() {
        assert_eq!(format!("{}", PropertyType::String), "STRING");
        assert_eq!(format!("{}", PropertyType::Int), "INT");
        assert_eq!(format!("{}", PropertyType::Float), "FLOAT");
        assert_eq!(format!("{}", PropertyType::Bool), "BOOL");
        assert_eq!(format!("{}", PropertyType::Any), "ANY");
        assert_eq!(format!("{}", PropertyType::List(None)), "LIST");
        assert_eq!(format!("{}", PropertyType::Map(None)), "MAP");
        assert_eq!(
            format!("{}", PropertyType::List(Some(Box::new(PropertyType::Int)))),
            "LIST<INT>"
        );
        assert_eq!(
            format!(
                "{}",
                PropertyType::Map(Some(Box::new(PropertyType::String)))
            ),
            "MAP<STRING>"
        );
    }

    #[test]
    fn property_type_type_name() {
        assert_eq!(PropertyType::Any.type_name(), "ANY");
        assert_eq!(PropertyType::Bool.type_name(), "BOOL");
        assert_eq!(PropertyType::Int.type_name(), "INT");
        assert_eq!(PropertyType::Float.type_name(), "FLOAT");
        assert_eq!(PropertyType::String.type_name(), "STRING");
        assert_eq!(PropertyType::List(None).type_name(), "LIST");
        assert_eq!(PropertyType::Map(None).type_name(), "MAP");
    }

    #[test]
    fn vertex_schema_required_properties() {
        let mut properties = HashMap::new();
        properties.insert(
            "name".to_string(),
            PropertyDef {
                key: "name".to_string(),
                value_type: PropertyType::String,
                required: true,
                default: None,
            },
        );
        properties.insert(
            "age".to_string(),
            PropertyDef {
                key: "age".to_string(),
                value_type: PropertyType::Int,
                required: false,
                default: None,
            },
        );
        properties.insert(
            "email".to_string(),
            PropertyDef {
                key: "email".to_string(),
                value_type: PropertyType::String,
                required: true,
                default: None,
            },
        );

        let schema = VertexSchema {
            label: "Person".to_string(),
            properties,
            additional_properties: false,
        };

        let required: Vec<_> = schema.required_properties().collect();
        assert_eq!(required.len(), 2);
        assert!(required.contains(&"name"));
        assert!(required.contains(&"email"));

        let optional: Vec<_> = schema.optional_properties().collect();
        assert_eq!(optional.len(), 1);
        assert!(optional.contains(&"age"));
    }

    #[test]
    fn vertex_schema_property_type_and_default() {
        let mut properties = HashMap::new();
        properties.insert(
            "active".to_string(),
            PropertyDef {
                key: "active".to_string(),
                value_type: PropertyType::Bool,
                required: false,
                default: Some(Value::Bool(true)),
            },
        );

        let schema = VertexSchema {
            label: "Person".to_string(),
            properties,
            additional_properties: false,
        };

        assert_eq!(schema.property_type("active"), Some(&PropertyType::Bool));
        assert_eq!(schema.property_default("active"), Some(&Value::Bool(true)));
        assert_eq!(schema.property_type("nonexistent"), None);
        assert_eq!(schema.property_default("nonexistent"), None);
    }

    #[test]
    fn edge_schema_allows_from_to() {
        let schema = EdgeSchema {
            label: "WORKS_AT".to_string(),
            from_labels: vec!["Person".to_string(), "Employee".to_string()],
            to_labels: vec!["Company".to_string()],
            properties: HashMap::new(),
            additional_properties: false,
        };

        assert!(schema.allows_from("Person"));
        assert!(schema.allows_from("Employee"));
        assert!(!schema.allows_from("Company"));
        assert!(!schema.allows_from("Unknown"));

        assert!(schema.allows_to("Company"));
        assert!(!schema.allows_to("Person"));
        assert!(!schema.allows_to("Unknown"));
    }

    #[test]
    fn edge_schema_properties() {
        let mut properties = HashMap::new();
        properties.insert(
            "since".to_string(),
            PropertyDef {
                key: "since".to_string(),
                value_type: PropertyType::Int,
                required: true,
                default: None,
            },
        );
        properties.insert(
            "weight".to_string(),
            PropertyDef {
                key: "weight".to_string(),
                value_type: PropertyType::Float,
                required: false,
                default: Some(Value::Float(1.0)),
            },
        );

        let schema = EdgeSchema {
            label: "KNOWS".to_string(),
            from_labels: vec!["Person".to_string()],
            to_labels: vec!["Person".to_string()],
            properties,
            additional_properties: false,
        };

        let required: Vec<_> = schema.required_properties().collect();
        assert_eq!(required.len(), 1);
        assert!(required.contains(&"since"));

        assert_eq!(schema.property_type("weight"), Some(&PropertyType::Float));
        assert_eq!(schema.property_default("weight"), Some(&Value::Float(1.0)));
    }
}
