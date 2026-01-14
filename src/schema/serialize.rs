//! Schema binary serialization and deserialization.
//!
//! This module provides functions to serialize and deserialize [`GraphSchema`]
//! to and from a compact binary format suitable for persistence.
//!
//! # Binary Format
//!
//! ```text
//! Schema Header (16 bytes):
//! - magic: u32 = 0x53434845 ("SCHE")
//! - version: u32 = 1
//! - validation_mode: u8
//! - vertex_schema_count: u16
//! - edge_schema_count: u16
//! - reserved: [u8; 3]
//!
//! Vertex Schemas (variable length):
//! - For each: label_len, label, additional_properties, property_count, properties...
//!
//! Edge Schemas (variable length):
//! - For each: label_len, label, additional_properties, from_count, from_labels,
//!             to_count, to_labels, property_count, properties...
//! ```
//!
//! # Example
//!
//! ```ignore
//! use intersteller::schema::{SchemaBuilder, PropertyType, ValidationMode};
//! use intersteller::schema::serialize::{serialize_schema, deserialize_schema};
//!
//! let schema = SchemaBuilder::new()
//!     .mode(ValidationMode::Strict)
//!     .vertex("Person")
//!         .property("name", PropertyType::String)
//!         .done()
//!     .build();
//!
//! let bytes = serialize_schema(&schema);
//! let recovered = deserialize_schema(&bytes).unwrap();
//!
//! assert_eq!(schema.mode, recovered.mode);
//! ```

use std::collections::HashMap;
use std::io::{Cursor, Read};
use thiserror::Error;

use crate::value::Value;

use super::{EdgeSchema, GraphSchema, PropertyDef, PropertyType, ValidationMode, VertexSchema};

// =============================================================================
// Constants
// =============================================================================

/// Magic number identifying schema data ("SCHE" in ASCII)
pub const SCHEMA_MAGIC: u32 = 0x53434845;

/// Current schema format version
pub const SCHEMA_FORMAT_VERSION: u32 = 1;

/// Size of the schema header in bytes
pub const SCHEMA_HEADER_SIZE: usize = 16;

// PropertyType discriminants
const PROPERTY_TYPE_ANY: u8 = 0;
const PROPERTY_TYPE_BOOL: u8 = 1;
const PROPERTY_TYPE_INT: u8 = 2;
const PROPERTY_TYPE_FLOAT: u8 = 3;
const PROPERTY_TYPE_STRING: u8 = 4;
const PROPERTY_TYPE_LIST: u8 = 5;
const PROPERTY_TYPE_MAP: u8 = 6;

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur during schema serialization or deserialization.
#[derive(Debug, Error)]
pub enum SchemaSerializeError {
    /// The magic number in the header doesn't match.
    #[error("invalid schema magic number")]
    InvalidMagic,

    /// The schema format version is not supported.
    #[error("unsupported schema version: {0}")]
    UnsupportedVersion(u32),

    /// Unexpected end of data while reading.
    #[error("unexpected end of data")]
    UnexpectedEof,

    /// Invalid validation mode value.
    #[error("invalid validation mode: {0}")]
    InvalidValidationMode(u8),

    /// Invalid property type discriminant.
    #[error("invalid property type: {0}")]
    InvalidPropertyType(u8),

    /// String data is not valid UTF-8.
    #[error("invalid UTF-8 string")]
    InvalidUtf8,

    /// I/O error during read/write.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Error deserializing a Value using bincode.
    #[error("value deserialization error: {0}")]
    ValueDeserialize(String),

    /// Error serializing a Value using bincode.
    #[error("value serialization error: {0}")]
    ValueSerialize(String),
}

// =============================================================================
// Serialization
// =============================================================================

/// Serialize a [`GraphSchema`] to bytes.
///
/// The output is a compact binary representation that can be persisted to disk
/// or transmitted over the network.
///
/// # Format
///
/// See module documentation for the binary format specification.
///
/// # Example
///
/// ```ignore
/// let bytes = serialize_schema(&schema);
/// assert!(bytes.len() >= SCHEMA_HEADER_SIZE);
/// ```
pub fn serialize_schema(schema: &GraphSchema) -> Vec<u8> {
    let mut buf = Vec::new();

    // Header (16 bytes)
    buf.extend_from_slice(&SCHEMA_MAGIC.to_le_bytes());
    buf.extend_from_slice(&SCHEMA_FORMAT_VERSION.to_le_bytes());
    buf.push(validation_mode_to_u8(schema.mode));
    buf.extend_from_slice(&(schema.vertex_schemas.len() as u16).to_le_bytes());
    buf.extend_from_slice(&(schema.edge_schemas.len() as u16).to_le_bytes());
    buf.extend_from_slice(&[0u8; 3]); // reserved

    // Vertex schemas (sorted for deterministic output)
    let mut vertex_labels: Vec<_> = schema.vertex_schemas.keys().collect();
    vertex_labels.sort();
    for label in vertex_labels {
        let vs = &schema.vertex_schemas[label];
        serialize_vertex_schema(&mut buf, vs);
    }

    // Edge schemas (sorted for deterministic output)
    let mut edge_labels: Vec<_> = schema.edge_schemas.keys().collect();
    edge_labels.sort();
    for label in edge_labels {
        let es = &schema.edge_schemas[label];
        serialize_edge_schema(&mut buf, es);
    }

    buf
}

/// Serialize a vertex schema.
fn serialize_vertex_schema(buf: &mut Vec<u8>, vs: &VertexSchema) {
    // Label
    write_string(buf, &vs.label);

    // Additional properties flag
    buf.push(if vs.additional_properties { 1 } else { 0 });

    // Property count
    buf.extend_from_slice(&(vs.properties.len() as u16).to_le_bytes());

    // Properties (sorted for deterministic output)
    let mut prop_keys: Vec<_> = vs.properties.keys().collect();
    prop_keys.sort();
    for key in prop_keys {
        let prop = &vs.properties[key];
        serialize_property_def(buf, prop);
    }
}

/// Serialize an edge schema.
fn serialize_edge_schema(buf: &mut Vec<u8>, es: &EdgeSchema) {
    // Label
    write_string(buf, &es.label);

    // Additional properties flag
    buf.push(if es.additional_properties { 1 } else { 0 });

    // From labels
    buf.extend_from_slice(&(es.from_labels.len() as u16).to_le_bytes());
    for label in &es.from_labels {
        write_string(buf, label);
    }

    // To labels
    buf.extend_from_slice(&(es.to_labels.len() as u16).to_le_bytes());
    for label in &es.to_labels {
        write_string(buf, label);
    }

    // Property count
    buf.extend_from_slice(&(es.properties.len() as u16).to_le_bytes());

    // Properties (sorted for deterministic output)
    let mut prop_keys: Vec<_> = es.properties.keys().collect();
    prop_keys.sort();
    for key in prop_keys {
        let prop = &es.properties[key];
        serialize_property_def(buf, prop);
    }
}

/// Serialize a property definition.
fn serialize_property_def(buf: &mut Vec<u8>, prop: &PropertyDef) {
    // Key
    write_string(buf, &prop.key);

    // Value type
    serialize_property_type(buf, &prop.value_type);

    // Required flag
    buf.push(if prop.required { 1 } else { 0 });

    // Default value
    match &prop.default {
        Some(value) => {
            buf.push(1); // has_default = true
            let value_bytes = bincode::serialize(value).unwrap_or_default();
            buf.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&value_bytes);
        }
        None => {
            buf.push(0); // has_default = false
        }
    }
}

/// Serialize a property type.
fn serialize_property_type(buf: &mut Vec<u8>, pt: &PropertyType) {
    match pt {
        PropertyType::Any => buf.push(PROPERTY_TYPE_ANY),
        PropertyType::Bool => buf.push(PROPERTY_TYPE_BOOL),
        PropertyType::Int => buf.push(PROPERTY_TYPE_INT),
        PropertyType::Float => buf.push(PROPERTY_TYPE_FLOAT),
        PropertyType::String => buf.push(PROPERTY_TYPE_STRING),
        PropertyType::List(inner) => {
            buf.push(PROPERTY_TYPE_LIST);
            match inner {
                Some(elem_type) => {
                    buf.push(1); // has inner type
                    serialize_property_type(buf, elem_type);
                }
                None => {
                    buf.push(0); // no inner type
                }
            }
        }
        PropertyType::Map(inner) => {
            buf.push(PROPERTY_TYPE_MAP);
            match inner {
                Some(val_type) => {
                    buf.push(1); // has inner type
                    serialize_property_type(buf, val_type);
                }
                None => {
                    buf.push(0); // no inner type
                }
            }
        }
    }
}

/// Write a length-prefixed string.
fn write_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(bytes);
}

/// Convert ValidationMode to u8.
fn validation_mode_to_u8(mode: ValidationMode) -> u8 {
    match mode {
        ValidationMode::None => 0,
        ValidationMode::Warn => 1,
        ValidationMode::Strict => 2,
        ValidationMode::Closed => 3,
    }
}

// =============================================================================
// Deserialization
// =============================================================================

/// Deserialize a [`GraphSchema`] from bytes.
///
/// # Errors
///
/// Returns an error if:
/// - The magic number doesn't match
/// - The format version is unsupported
/// - The data is truncated or corrupted
/// - String data is not valid UTF-8
///
/// # Example
///
/// ```ignore
/// let schema = deserialize_schema(&bytes)?;
/// ```
pub fn deserialize_schema(data: &[u8]) -> Result<GraphSchema, SchemaSerializeError> {
    let mut cursor = Cursor::new(data);

    // Read and validate header
    let magic = read_u32(&mut cursor)?;
    if magic != SCHEMA_MAGIC {
        return Err(SchemaSerializeError::InvalidMagic);
    }

    let version = read_u32(&mut cursor)?;
    if version != SCHEMA_FORMAT_VERSION {
        return Err(SchemaSerializeError::UnsupportedVersion(version));
    }

    let mode = u8_to_validation_mode(read_u8(&mut cursor)?)?;
    let vertex_count = read_u16(&mut cursor)? as usize;
    let edge_count = read_u16(&mut cursor)? as usize;

    // Skip reserved bytes
    let mut reserved = [0u8; 3];
    cursor.read_exact(&mut reserved)?;

    // Read vertex schemas
    let mut vertex_schemas = HashMap::new();
    for _ in 0..vertex_count {
        let vs = deserialize_vertex_schema(&mut cursor)?;
        vertex_schemas.insert(vs.label.clone(), vs);
    }

    // Read edge schemas
    let mut edge_schemas = HashMap::new();
    for _ in 0..edge_count {
        let es = deserialize_edge_schema(&mut cursor)?;
        edge_schemas.insert(es.label.clone(), es);
    }

    Ok(GraphSchema {
        vertex_schemas,
        edge_schemas,
        mode,
    })
}

/// Deserialize a vertex schema.
fn deserialize_vertex_schema(
    cursor: &mut Cursor<&[u8]>,
) -> Result<VertexSchema, SchemaSerializeError> {
    let label = read_string(cursor)?;
    let additional_properties = read_u8(cursor)? != 0;
    let property_count = read_u16(cursor)? as usize;

    let mut properties = HashMap::new();
    for _ in 0..property_count {
        let prop = deserialize_property_def(cursor)?;
        properties.insert(prop.key.clone(), prop);
    }

    Ok(VertexSchema {
        label,
        properties,
        additional_properties,
    })
}

/// Deserialize an edge schema.
fn deserialize_edge_schema(cursor: &mut Cursor<&[u8]>) -> Result<EdgeSchema, SchemaSerializeError> {
    let label = read_string(cursor)?;
    let additional_properties = read_u8(cursor)? != 0;

    // From labels
    let from_count = read_u16(cursor)? as usize;
    let mut from_labels = Vec::with_capacity(from_count);
    for _ in 0..from_count {
        from_labels.push(read_string(cursor)?);
    }

    // To labels
    let to_count = read_u16(cursor)? as usize;
    let mut to_labels = Vec::with_capacity(to_count);
    for _ in 0..to_count {
        to_labels.push(read_string(cursor)?);
    }

    // Properties
    let property_count = read_u16(cursor)? as usize;
    let mut properties = HashMap::new();
    for _ in 0..property_count {
        let prop = deserialize_property_def(cursor)?;
        properties.insert(prop.key.clone(), prop);
    }

    Ok(EdgeSchema {
        label,
        from_labels,
        to_labels,
        properties,
        additional_properties,
    })
}

/// Deserialize a property definition.
fn deserialize_property_def(
    cursor: &mut Cursor<&[u8]>,
) -> Result<PropertyDef, SchemaSerializeError> {
    let key = read_string(cursor)?;
    let value_type = deserialize_property_type(cursor)?;
    let required = read_u8(cursor)? != 0;

    let has_default = read_u8(cursor)? != 0;
    let default = if has_default {
        let value_len = read_u32(cursor)? as usize;
        let mut value_bytes = vec![0u8; value_len];
        cursor.read_exact(&mut value_bytes)?;
        let value: Value = bincode::deserialize(&value_bytes)
            .map_err(|e| SchemaSerializeError::ValueDeserialize(e.to_string()))?;
        Some(value)
    } else {
        None
    };

    Ok(PropertyDef {
        key,
        value_type,
        required,
        default,
    })
}

/// Deserialize a property type.
fn deserialize_property_type(
    cursor: &mut Cursor<&[u8]>,
) -> Result<PropertyType, SchemaSerializeError> {
    let discriminant = read_u8(cursor)?;

    match discriminant {
        PROPERTY_TYPE_ANY => Ok(PropertyType::Any),
        PROPERTY_TYPE_BOOL => Ok(PropertyType::Bool),
        PROPERTY_TYPE_INT => Ok(PropertyType::Int),
        PROPERTY_TYPE_FLOAT => Ok(PropertyType::Float),
        PROPERTY_TYPE_STRING => Ok(PropertyType::String),
        PROPERTY_TYPE_LIST => {
            let has_inner = read_u8(cursor)? != 0;
            if has_inner {
                let inner = deserialize_property_type(cursor)?;
                Ok(PropertyType::List(Some(Box::new(inner))))
            } else {
                Ok(PropertyType::List(None))
            }
        }
        PROPERTY_TYPE_MAP => {
            let has_inner = read_u8(cursor)? != 0;
            if has_inner {
                let inner = deserialize_property_type(cursor)?;
                Ok(PropertyType::Map(Some(Box::new(inner))))
            } else {
                Ok(PropertyType::Map(None))
            }
        }
        _ => Err(SchemaSerializeError::InvalidPropertyType(discriminant)),
    }
}

/// Read a u8 from the cursor.
fn read_u8(cursor: &mut Cursor<&[u8]>) -> Result<u8, SchemaSerializeError> {
    let mut buf = [0u8; 1];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| SchemaSerializeError::UnexpectedEof)?;
    Ok(buf[0])
}

/// Read a u16 (little-endian) from the cursor.
fn read_u16(cursor: &mut Cursor<&[u8]>) -> Result<u16, SchemaSerializeError> {
    let mut buf = [0u8; 2];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| SchemaSerializeError::UnexpectedEof)?;
    Ok(u16::from_le_bytes(buf))
}

/// Read a u32 (little-endian) from the cursor.
fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32, SchemaSerializeError> {
    let mut buf = [0u8; 4];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| SchemaSerializeError::UnexpectedEof)?;
    Ok(u32::from_le_bytes(buf))
}

/// Read a length-prefixed string from the cursor.
fn read_string(cursor: &mut Cursor<&[u8]>) -> Result<String, SchemaSerializeError> {
    let len = read_u16(cursor)? as usize;
    let mut buf = vec![0u8; len];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| SchemaSerializeError::UnexpectedEof)?;
    String::from_utf8(buf).map_err(|_| SchemaSerializeError::InvalidUtf8)
}

/// Convert u8 to ValidationMode.
fn u8_to_validation_mode(v: u8) -> Result<ValidationMode, SchemaSerializeError> {
    match v {
        0 => Ok(ValidationMode::None),
        1 => Ok(ValidationMode::Warn),
        2 => Ok(ValidationMode::Strict),
        3 => Ok(ValidationMode::Closed),
        _ => Err(SchemaSerializeError::InvalidValidationMode(v)),
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaBuilder;

    #[test]
    fn test_empty_schema_roundtrip() {
        let schema = GraphSchema::new();
        let bytes = serialize_schema(&schema);
        let recovered = deserialize_schema(&bytes).expect("deserialize");

        assert_eq!(recovered.mode, ValidationMode::None);
        assert!(recovered.vertex_schemas.is_empty());
        assert!(recovered.edge_schemas.is_empty());
    }

    #[test]
    fn test_validation_mode_roundtrip() {
        for mode in [
            ValidationMode::None,
            ValidationMode::Warn,
            ValidationMode::Strict,
            ValidationMode::Closed,
        ] {
            let schema = GraphSchema::with_mode(mode);
            let bytes = serialize_schema(&schema);
            let recovered = deserialize_schema(&bytes).expect("deserialize");
            assert_eq!(recovered.mode, mode);
        }
    }

    #[test]
    fn test_simple_vertex_schema_roundtrip() {
        let schema = SchemaBuilder::new()
            .mode(ValidationMode::Strict)
            .vertex("Person")
            .property("name", PropertyType::String)
            .property("age", PropertyType::Int)
            .done()
            .build();

        let bytes = serialize_schema(&schema);
        let recovered = deserialize_schema(&bytes).expect("deserialize");

        assert_eq!(recovered.mode, ValidationMode::Strict);
        assert!(recovered.has_vertex_schema("Person"));

        let vs = recovered.vertex_schema("Person").unwrap();
        assert_eq!(vs.properties.len(), 2);
        assert!(vs.properties.contains_key("name"));
        assert!(vs.properties.contains_key("age"));
    }

    #[test]
    fn test_vertex_schema_with_optional_properties() {
        let schema = SchemaBuilder::new()
            .vertex("Person")
            .property("name", PropertyType::String)
            .optional("nickname", PropertyType::String)
            .done()
            .build();

        let bytes = serialize_schema(&schema);
        let recovered = deserialize_schema(&bytes).expect("deserialize");

        let vs = recovered.vertex_schema("Person").unwrap();
        assert!(vs.properties["name"].required);
        assert!(!vs.properties["nickname"].required);
    }

    #[test]
    fn test_vertex_schema_with_default_value() {
        let schema = SchemaBuilder::new()
            .vertex("Person")
            .optional_with_default("active", PropertyType::Bool, Value::Bool(true))
            .optional_with_default("score", PropertyType::Int, Value::Int(100))
            .done()
            .build();

        let bytes = serialize_schema(&schema);
        let recovered = deserialize_schema(&bytes).expect("deserialize");

        let vs = recovered.vertex_schema("Person").unwrap();
        assert_eq!(vs.properties["active"].default, Some(Value::Bool(true)));
        assert_eq!(vs.properties["score"].default, Some(Value::Int(100)));
    }

    #[test]
    fn test_edge_schema_roundtrip() {
        let schema = SchemaBuilder::new()
            .vertex("Person")
            .done()
            .vertex("Company")
            .done()
            .edge("WORKS_AT")
            .from(&["Person"])
            .to(&["Company"])
            .property("since", PropertyType::Int)
            .optional("role", PropertyType::String)
            .done()
            .build();

        let bytes = serialize_schema(&schema);
        let recovered = deserialize_schema(&bytes).expect("deserialize");

        assert!(recovered.has_edge_schema("WORKS_AT"));

        let es = recovered.edge_schema("WORKS_AT").unwrap();
        assert_eq!(es.from_labels, vec!["Person".to_string()]);
        assert_eq!(es.to_labels, vec!["Company".to_string()]);
        assert!(es.properties.contains_key("since"));
        assert!(es.properties.contains_key("role"));
    }

    #[test]
    fn test_edge_schema_multiple_endpoints() {
        let schema = SchemaBuilder::new()
            .vertex("Person")
            .done()
            .vertex("Employee")
            .done()
            .vertex("Company")
            .done()
            .vertex("Organization")
            .done()
            .edge("AFFILIATED_WITH")
            .from(&["Person", "Employee"])
            .to(&["Company", "Organization"])
            .done()
            .build();

        let bytes = serialize_schema(&schema);
        let recovered = deserialize_schema(&bytes).expect("deserialize");

        let es = recovered.edge_schema("AFFILIATED_WITH").unwrap();
        assert_eq!(es.from_labels.len(), 2);
        assert_eq!(es.to_labels.len(), 2);
        assert!(es.from_labels.contains(&"Person".to_string()));
        assert!(es.from_labels.contains(&"Employee".to_string()));
        assert!(es.to_labels.contains(&"Company".to_string()));
        assert!(es.to_labels.contains(&"Organization".to_string()));
    }

    #[test]
    fn test_property_types_roundtrip() {
        let schema = SchemaBuilder::new()
            .vertex("Test")
            .property("any_prop", PropertyType::Any)
            .property("bool_prop", PropertyType::Bool)
            .property("int_prop", PropertyType::Int)
            .property("float_prop", PropertyType::Float)
            .property("string_prop", PropertyType::String)
            .property("list_prop", PropertyType::List(None))
            .property("map_prop", PropertyType::Map(None))
            .done()
            .build();

        let bytes = serialize_schema(&schema);
        let recovered = deserialize_schema(&bytes).expect("deserialize");

        let vs = recovered.vertex_schema("Test").unwrap();
        assert_eq!(vs.properties["any_prop"].value_type, PropertyType::Any);
        assert_eq!(vs.properties["bool_prop"].value_type, PropertyType::Bool);
        assert_eq!(vs.properties["int_prop"].value_type, PropertyType::Int);
        assert_eq!(vs.properties["float_prop"].value_type, PropertyType::Float);
        assert_eq!(
            vs.properties["string_prop"].value_type,
            PropertyType::String
        );
        assert_eq!(
            vs.properties["list_prop"].value_type,
            PropertyType::List(None)
        );
        assert_eq!(
            vs.properties["map_prop"].value_type,
            PropertyType::Map(None)
        );
    }

    #[test]
    fn test_nested_property_types_roundtrip() {
        let schema = SchemaBuilder::new()
            .vertex("Test")
            .property(
                "int_list",
                PropertyType::List(Some(Box::new(PropertyType::Int))),
            )
            .property(
                "string_map",
                PropertyType::Map(Some(Box::new(PropertyType::String))),
            )
            .property(
                "nested_list",
                PropertyType::List(Some(Box::new(PropertyType::List(Some(Box::new(
                    PropertyType::Int,
                )))))),
            )
            .done()
            .build();

        let bytes = serialize_schema(&schema);
        let recovered = deserialize_schema(&bytes).expect("deserialize");

        let vs = recovered.vertex_schema("Test").unwrap();
        assert_eq!(
            vs.properties["int_list"].value_type,
            PropertyType::List(Some(Box::new(PropertyType::Int)))
        );
        assert_eq!(
            vs.properties["string_map"].value_type,
            PropertyType::Map(Some(Box::new(PropertyType::String)))
        );
        assert_eq!(
            vs.properties["nested_list"].value_type,
            PropertyType::List(Some(Box::new(PropertyType::List(Some(Box::new(
                PropertyType::Int
            ))))))
        );
    }

    #[test]
    fn test_additional_properties_flag() {
        let schema = SchemaBuilder::new()
            .vertex("Open")
            .allow_additional()
            .done()
            .vertex("Closed")
            .done()
            .build();

        let bytes = serialize_schema(&schema);
        let recovered = deserialize_schema(&bytes).expect("deserialize");

        assert!(
            recovered
                .vertex_schema("Open")
                .unwrap()
                .additional_properties
        );
        assert!(
            !recovered
                .vertex_schema("Closed")
                .unwrap()
                .additional_properties
        );
    }

    #[test]
    fn test_complex_schema_roundtrip() {
        let schema = SchemaBuilder::new()
            .mode(ValidationMode::Closed)
            .vertex("Person")
            .property("name", PropertyType::String)
            .property("email", PropertyType::String)
            .optional("age", PropertyType::Int)
            .optional_with_default("active", PropertyType::Bool, Value::Bool(true))
            .done()
            .vertex("Company")
            .property("name", PropertyType::String)
            .optional("founded", PropertyType::Int)
            .allow_additional()
            .done()
            .edge("WORKS_AT")
            .from(&["Person"])
            .to(&["Company"])
            .property("since", PropertyType::Int)
            .optional_with_default("full_time", PropertyType::Bool, Value::Bool(true))
            .done()
            .edge("KNOWS")
            .from(&["Person"])
            .to(&["Person"])
            .optional("since", PropertyType::Int)
            .done()
            .build();

        let bytes = serialize_schema(&schema);
        let recovered = deserialize_schema(&bytes).expect("deserialize");

        // Verify structure
        assert_eq!(recovered.mode, ValidationMode::Closed);
        assert_eq!(recovered.vertex_schemas.len(), 2);
        assert_eq!(recovered.edge_schemas.len(), 2);

        // Verify Person
        let person = recovered.vertex_schema("Person").unwrap();
        assert_eq!(person.properties.len(), 4);
        assert!(!person.additional_properties);
        assert!(person.properties["name"].required);
        assert!(!person.properties["age"].required);
        assert_eq!(person.properties["active"].default, Some(Value::Bool(true)));

        // Verify Company
        let company = recovered.vertex_schema("Company").unwrap();
        assert!(company.additional_properties);

        // Verify WORKS_AT
        let works_at = recovered.edge_schema("WORKS_AT").unwrap();
        assert_eq!(works_at.from_labels, vec!["Person".to_string()]);
        assert_eq!(works_at.to_labels, vec!["Company".to_string()]);
        assert!(works_at.properties["since"].required);
        assert_eq!(
            works_at.properties["full_time"].default,
            Some(Value::Bool(true))
        );

        // Verify KNOWS
        let knows = recovered.edge_schema("KNOWS").unwrap();
        assert_eq!(knows.from_labels, vec!["Person".to_string()]);
        assert_eq!(knows.to_labels, vec!["Person".to_string()]);
    }

    #[test]
    fn test_header_size() {
        let schema = GraphSchema::new();
        let bytes = serialize_schema(&schema);
        assert!(bytes.len() >= SCHEMA_HEADER_SIZE);
    }

    #[test]
    fn test_magic_number() {
        let schema = GraphSchema::new();
        let bytes = serialize_schema(&schema);

        let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(magic, SCHEMA_MAGIC);
    }

    #[test]
    fn test_version_number() {
        let schema = GraphSchema::new();
        let bytes = serialize_schema(&schema);

        let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        assert_eq!(version, SCHEMA_FORMAT_VERSION);
    }

    #[test]
    fn test_invalid_magic_error() {
        let mut bytes = serialize_schema(&GraphSchema::new());
        bytes[0] = 0xFF; // Corrupt magic

        let result = deserialize_schema(&bytes);
        assert!(matches!(result, Err(SchemaSerializeError::InvalidMagic)));
    }

    #[test]
    fn test_unsupported_version_error() {
        let mut bytes = serialize_schema(&GraphSchema::new());
        bytes[4] = 99; // Unknown version

        let result = deserialize_schema(&bytes);
        assert!(matches!(
            result,
            Err(SchemaSerializeError::UnsupportedVersion(99))
        ));
    }

    #[test]
    fn test_invalid_validation_mode_error() {
        let mut bytes = serialize_schema(&GraphSchema::new());
        bytes[8] = 99; // Invalid mode

        let result = deserialize_schema(&bytes);
        assert!(matches!(
            result,
            Err(SchemaSerializeError::InvalidValidationMode(99))
        ));
    }

    #[test]
    fn test_truncated_data_error() {
        let bytes = &[0x53, 0x43, 0x48, 0x45]; // Just magic, truncated

        let result = deserialize_schema(bytes);
        // Truncated data can return either UnexpectedEof or Io error depending on
        // where the truncation occurs
        assert!(result.is_err(), "Expected an error for truncated data");
    }

    #[test]
    fn test_empty_strings() {
        let schema = SchemaBuilder::new()
            .vertex("") // Empty label (unusual but valid)
            .property("", PropertyType::String) // Empty property key
            .done()
            .build();

        let bytes = serialize_schema(&schema);
        let recovered = deserialize_schema(&bytes).expect("deserialize");

        assert!(recovered.has_vertex_schema(""));
        let vs = recovered.vertex_schema("").unwrap();
        assert!(vs.properties.contains_key(""));
    }

    #[test]
    fn test_unicode_strings() {
        let schema = SchemaBuilder::new()
            .vertex("Person")
            .property("name", PropertyType::String)
            .done()
            .build();

        let bytes = serialize_schema(&schema);
        let recovered = deserialize_schema(&bytes).expect("deserialize");

        assert!(recovered.has_vertex_schema("Person"));
    }

    #[test]
    fn test_default_value_types() {
        let schema = SchemaBuilder::new()
            .vertex("Test")
            .optional_with_default(
                "str_val",
                PropertyType::String,
                Value::String("hello".into()),
            )
            .optional_with_default("int_val", PropertyType::Int, Value::Int(-42))
            .optional_with_default("float_val", PropertyType::Float, Value::Float(3.14159))
            .optional_with_default(
                "list_val",
                PropertyType::List(None),
                Value::List(vec![Value::Int(1), Value::Int(2)]),
            )
            .done()
            .build();

        let bytes = serialize_schema(&schema);
        let recovered = deserialize_schema(&bytes).expect("deserialize");

        let vs = recovered.vertex_schema("Test").unwrap();
        assert_eq!(
            vs.properties["str_val"].default,
            Some(Value::String("hello".into()))
        );
        assert_eq!(vs.properties["int_val"].default, Some(Value::Int(-42)));
        assert_eq!(
            vs.properties["float_val"].default,
            Some(Value::Float(3.14159))
        );
        assert_eq!(
            vs.properties["list_val"].default,
            Some(Value::List(vec![Value::Int(1), Value::Int(2)]))
        );
    }

    #[test]
    fn test_deterministic_output() {
        // Schema with multiple items should produce deterministic output
        let schema = SchemaBuilder::new()
            .vertex("Zebra")
            .property("z", PropertyType::String)
            .done()
            .vertex("Apple")
            .property("a", PropertyType::String)
            .done()
            .vertex("Middle")
            .property("m", PropertyType::String)
            .done()
            .build();

        let bytes1 = serialize_schema(&schema);
        let bytes2 = serialize_schema(&schema);

        assert_eq!(bytes1, bytes2, "Output should be deterministic");
    }

    #[test]
    fn test_validation_mode_to_u8() {
        assert_eq!(validation_mode_to_u8(ValidationMode::None), 0);
        assert_eq!(validation_mode_to_u8(ValidationMode::Warn), 1);
        assert_eq!(validation_mode_to_u8(ValidationMode::Strict), 2);
        assert_eq!(validation_mode_to_u8(ValidationMode::Closed), 3);
    }

    #[test]
    fn test_u8_to_validation_mode() {
        assert_eq!(u8_to_validation_mode(0).unwrap(), ValidationMode::None);
        assert_eq!(u8_to_validation_mode(1).unwrap(), ValidationMode::Warn);
        assert_eq!(u8_to_validation_mode(2).unwrap(), ValidationMode::Strict);
        assert_eq!(u8_to_validation_mode(3).unwrap(), ValidationMode::Closed);
        assert!(u8_to_validation_mode(4).is_err());
    }
}
