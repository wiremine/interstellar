//! Schema extension for GraphSON.
//!
//! Provides serialization of Interstellar schema to GraphSON format
//! using the custom `interstellar:Schema` type.

use crate::schema::{
    EdgeSchema, GraphSchema, PropertyDef, PropertyType, ValidationMode, VertexSchema,
};
use crate::value::Value;
use serde::{Deserialize, Serialize};

/// GraphSON representation of a property type.
fn property_type_to_string(pt: &PropertyType) -> String {
    match pt {
        PropertyType::Any => "ANY".to_string(),
        PropertyType::Bool => "BOOL".to_string(),
        PropertyType::Int => "INT".to_string(),
        PropertyType::Float => "FLOAT".to_string(),
        PropertyType::String => "STRING".to_string(),
        PropertyType::List(None) => "LIST".to_string(),
        PropertyType::List(Some(inner)) => format!("LIST<{}>", property_type_to_string(inner)),
        PropertyType::Map(None) => "MAP".to_string(),
        PropertyType::Map(Some(inner)) => format!("MAP<{}>", property_type_to_string(inner)),
    }
}

/// Parse a property type string.
pub fn string_to_property_type(s: &str) -> Option<PropertyType> {
    match s {
        "ANY" => Some(PropertyType::Any),
        "BOOL" => Some(PropertyType::Bool),
        "INT" => Some(PropertyType::Int),
        "FLOAT" => Some(PropertyType::Float),
        "STRING" => Some(PropertyType::String),
        "LIST" => Some(PropertyType::List(None)),
        "MAP" => Some(PropertyType::Map(None)),
        s if s.starts_with("LIST<") && s.ends_with('>') => {
            let inner = &s[5..s.len() - 1];
            string_to_property_type(inner).map(|t| PropertyType::List(Some(Box::new(t))))
        }
        s if s.starts_with("MAP<") && s.ends_with('>') => {
            let inner = &s[4..s.len() - 1];
            string_to_property_type(inner).map(|t| PropertyType::Map(Some(Box::new(t))))
        }
        _ => None,
    }
}

/// JSON-serializable schema representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphSONSchema {
    /// Validation mode
    pub mode: String,
    /// Vertex schemas
    pub vertex_schemas: Vec<GraphSONVertexSchema>,
    /// Edge schemas
    pub edge_schemas: Vec<GraphSONEdgeSchema>,
}

/// JSON-serializable vertex schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphSONVertexSchema {
    /// Vertex label
    pub label: String,
    /// Whether additional properties are allowed
    pub additional_properties: bool,
    /// Property definitions
    pub properties: Vec<GraphSONPropertyDef>,
}

/// JSON-serializable edge schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphSONEdgeSchema {
    /// Edge label
    pub label: String,
    /// Allowed source vertex labels
    pub from_labels: Vec<String>,
    /// Allowed target vertex labels
    pub to_labels: Vec<String>,
    /// Whether additional properties are allowed
    pub additional_properties: bool,
    /// Property definitions
    pub properties: Vec<GraphSONPropertyDef>,
}

/// JSON-serializable property definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSONPropertyDef {
    /// Property key
    pub key: String,
    /// Property type
    #[serde(rename = "type")]
    pub value_type: String,
    /// Whether property is required
    pub required: bool,
    /// Default value (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
}

/// Convert a GraphSchema to GraphSON format.
pub fn schema_to_graphson(schema: &GraphSchema) -> serde_json::Value {
    let mode = match schema.mode {
        ValidationMode::None => "none",
        ValidationMode::Warn => "warn",
        ValidationMode::Strict => "strict",
        ValidationMode::Closed => "closed",
    };

    let vertex_schemas: Vec<GraphSONVertexSchema> = schema
        .vertex_schemas
        .values()
        .map(|vs| GraphSONVertexSchema {
            label: vs.label.clone(),
            additional_properties: vs.additional_properties,
            properties: vs
                .properties
                .values()
                .map(property_def_to_graphson)
                .collect(),
        })
        .collect();

    let edge_schemas: Vec<GraphSONEdgeSchema> = schema
        .edge_schemas
        .values()
        .map(|es| GraphSONEdgeSchema {
            label: es.label.clone(),
            from_labels: es.from_labels.clone(),
            to_labels: es.to_labels.clone(),
            additional_properties: es.additional_properties,
            properties: es
                .properties
                .values()
                .map(property_def_to_graphson)
                .collect(),
        })
        .collect();

    let gs_schema = GraphSONSchema {
        mode: mode.to_string(),
        vertex_schemas,
        edge_schemas,
    };

    serde_json::json!({
        "@type": "interstellar:Schema",
        "@value": gs_schema
    })
}

/// Convert a PropertyDef to GraphSON format.
fn property_def_to_graphson(prop: &PropertyDef) -> GraphSONPropertyDef {
    GraphSONPropertyDef {
        key: prop.key.clone(),
        value_type: property_type_to_string(&prop.value_type),
        required: prop.required,
        default: prop.default.as_ref().map(value_to_json),
    }
}

/// Convert an Interstellar Value to JSON for default values.
fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(n) => serde_json::json!({"@type": "g:Int64", "@value": n}),
        Value::Float(f) => serde_json::json!({"@type": "g:Double", "@value": f}),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::List(items) => {
            serde_json::json!({
                "@type": "g:List",
                "@value": items.iter().map(value_to_json).collect::<Vec<_>>()
            })
        }
        Value::Map(map) => {
            let pairs: Vec<serde_json::Value> = map
                .iter()
                .flat_map(|(k, v)| vec![serde_json::Value::String(k.clone()), value_to_json(v)])
                .collect();
            serde_json::json!({"@type": "g:Map", "@value": pairs})
        }
        Value::Vertex(id) => serde_json::json!({"@type": "g:Int64", "@value": id.0}),
        Value::Edge(id) => serde_json::json!({"@type": "g:Int64", "@value": id.0}),
        Value::Point(p) => {
            serde_json::json!({"@type": "g:Point", "@value": {"longitude": p.lon, "latitude": p.lat}})
        }
        Value::Polygon(p) => {
            serde_json::json!({"@type": "is:Polygon", "@value": {"ring": p.ring.iter().map(|&(lon, lat)| vec![lon, lat]).collect::<Vec<_>>()}})
        }
    }
}

/// Convert a GraphSONSchema to a GraphSchema.
pub fn graphson_to_schema(gs_schema: &GraphSONSchema) -> GraphSchema {
    let mode = match gs_schema.mode.as_str() {
        "none" => ValidationMode::None,
        "warn" => ValidationMode::Warn,
        "strict" => ValidationMode::Strict,
        "closed" => ValidationMode::Closed,
        _ => ValidationMode::None,
    };

    let mut vertex_schemas = std::collections::HashMap::new();
    for vs in &gs_schema.vertex_schemas {
        let mut properties = std::collections::HashMap::new();
        for prop in &vs.properties {
            let value_type = string_to_property_type(&prop.value_type).unwrap_or(PropertyType::Any);
            properties.insert(
                prop.key.clone(),
                PropertyDef {
                    key: prop.key.clone(),
                    value_type,
                    required: prop.required,
                    default: None, // TODO: Parse default values
                },
            );
        }
        vertex_schemas.insert(
            vs.label.clone(),
            VertexSchema {
                label: vs.label.clone(),
                properties,
                additional_properties: vs.additional_properties,
            },
        );
    }

    let mut edge_schemas = std::collections::HashMap::new();
    for es in &gs_schema.edge_schemas {
        let mut properties = std::collections::HashMap::new();
        for prop in &es.properties {
            let value_type = string_to_property_type(&prop.value_type).unwrap_or(PropertyType::Any);
            properties.insert(
                prop.key.clone(),
                PropertyDef {
                    key: prop.key.clone(),
                    value_type,
                    required: prop.required,
                    default: None,
                },
            );
        }
        edge_schemas.insert(
            es.label.clone(),
            EdgeSchema {
                label: es.label.clone(),
                from_labels: es.from_labels.clone(),
                to_labels: es.to_labels.clone(),
                properties,
                additional_properties: es.additional_properties,
            },
        );
    }

    GraphSchema {
        vertex_schemas,
        edge_schemas,
        mode,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaBuilder;

    #[test]
    fn test_property_type_to_string() {
        assert_eq!(property_type_to_string(&PropertyType::Any), "ANY");
        assert_eq!(property_type_to_string(&PropertyType::Bool), "BOOL");
        assert_eq!(property_type_to_string(&PropertyType::Int), "INT");
        assert_eq!(property_type_to_string(&PropertyType::Float), "FLOAT");
        assert_eq!(property_type_to_string(&PropertyType::String), "STRING");
        assert_eq!(property_type_to_string(&PropertyType::List(None)), "LIST");
        assert_eq!(property_type_to_string(&PropertyType::Map(None)), "MAP");
        assert_eq!(
            property_type_to_string(&PropertyType::List(Some(Box::new(PropertyType::Int)))),
            "LIST<INT>"
        );
        assert_eq!(
            property_type_to_string(&PropertyType::Map(Some(Box::new(PropertyType::String)))),
            "MAP<STRING>"
        );
    }

    #[test]
    fn test_string_to_property_type() {
        assert_eq!(string_to_property_type("ANY"), Some(PropertyType::Any));
        assert_eq!(string_to_property_type("BOOL"), Some(PropertyType::Bool));
        assert_eq!(string_to_property_type("INT"), Some(PropertyType::Int));
        assert_eq!(string_to_property_type("FLOAT"), Some(PropertyType::Float));
        assert_eq!(
            string_to_property_type("STRING"),
            Some(PropertyType::String)
        );
        assert_eq!(
            string_to_property_type("LIST"),
            Some(PropertyType::List(None))
        );
        assert_eq!(
            string_to_property_type("MAP"),
            Some(PropertyType::Map(None))
        );
        assert_eq!(
            string_to_property_type("LIST<INT>"),
            Some(PropertyType::List(Some(Box::new(PropertyType::Int))))
        );
        assert_eq!(
            string_to_property_type("MAP<STRING>"),
            Some(PropertyType::Map(Some(Box::new(PropertyType::String))))
        );
        assert_eq!(string_to_property_type("UNKNOWN"), None);
    }

    #[test]
    fn test_schema_to_graphson() {
        let schema = SchemaBuilder::new()
            .mode(ValidationMode::Strict)
            .vertex("Person")
            .property("name", PropertyType::String)
            .optional("age", PropertyType::Int)
            .done()
            .edge("KNOWS")
            .from(&["Person"])
            .to(&["Person"])
            .optional("since", PropertyType::Int)
            .done()
            .build();

        let json = schema_to_graphson(&schema);

        // Check type tag
        assert_eq!(json["@type"], "interstellar:Schema");

        // Check mode
        assert_eq!(json["@value"]["mode"], "strict");

        // Check vertex schemas exist
        let vertex_schemas = json["@value"]["vertexSchemas"].as_array().unwrap();
        assert_eq!(vertex_schemas.len(), 1);
        assert_eq!(vertex_schemas[0]["label"], "Person");

        // Check edge schemas exist
        let edge_schemas = json["@value"]["edgeSchemas"].as_array().unwrap();
        assert_eq!(edge_schemas.len(), 1);
        assert_eq!(edge_schemas[0]["label"], "KNOWS");
    }

    #[test]
    fn test_graphson_to_schema_roundtrip() {
        let original = SchemaBuilder::new()
            .mode(ValidationMode::Strict)
            .vertex("Person")
            .property("name", PropertyType::String)
            .done()
            .edge("KNOWS")
            .from(&["Person"])
            .to(&["Person"])
            .done()
            .build();

        let json = schema_to_graphson(&original);
        let gs_schema: GraphSONSchema = serde_json::from_value(json["@value"].clone()).unwrap();
        let recovered = graphson_to_schema(&gs_schema);

        assert_eq!(recovered.mode, original.mode);
        assert!(recovered.vertex_schemas.contains_key("Person"));
        assert!(recovered.edge_schemas.contains_key("KNOWS"));
    }

    #[test]
    fn test_value_to_json() {
        assert_eq!(value_to_json(&Value::Null), serde_json::Value::Null);
        assert_eq!(
            value_to_json(&Value::Bool(true)),
            serde_json::Value::Bool(true)
        );
        assert_eq!(
            value_to_json(&Value::String("test".to_string())),
            serde_json::Value::String("test".to_string())
        );

        let int_json = value_to_json(&Value::Int(42));
        assert_eq!(int_json["@type"], "g:Int64");
        assert_eq!(int_json["@value"], 42);

        let float_json = value_to_json(&Value::Float(3.14));
        assert_eq!(float_json["@type"], "g:Double");
    }
}
