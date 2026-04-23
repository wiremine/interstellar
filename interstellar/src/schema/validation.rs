//! Schema validation functions.
//!
//! This module provides functions for validating vertices and edges against
//! a graph schema. Validation is called during mutation operations (CREATE, SET, MERGE)
//! when a schema is present.
//!
//! # Validation Modes
//!
//! The behavior of validation depends on the [`ValidationMode`]:
//!
//! | Mode | Unknown Label | Schema Violation | Additional Properties |
//! |------|---------------|------------------|----------------------|
//! | `None` | Allowed | Allowed | Allowed |
//! | `Warn` | Warning | Warning | Warning |
//! | `Strict` | Allowed | Error | Error (unless allowed) |
//! | `Closed` | Error | Error | Error (unless allowed) |
//!
//! # Example
//!
//! ```
//! use interstellar::schema::{
//!     SchemaBuilder, PropertyType, ValidationMode,
//!     validate_vertex, ValidationResult,
//! };
//! use interstellar::value::Value;
//! use std::collections::HashMap;
//!
//! let schema = SchemaBuilder::new()
//!     .mode(ValidationMode::Strict)
//!     .vertex("Person")
//!         .property("name", PropertyType::String)
//!         .optional("age", PropertyType::Int)
//!         .done()
//!     .build();
//!
//! // Valid vertex
//! let mut props = HashMap::new();
//! props.insert("name".to_string(), Value::String("Alice".to_string()));
//! let results = validate_vertex(&schema, "Person", &props).unwrap();
//! assert!(results.iter().all(|r| matches!(r, ValidationResult::Ok)));
//!
//! // Missing required property
//! let props = HashMap::new();
//! let result = validate_vertex(&schema, "Person", &props);
//! assert!(result.is_err()); // Missing 'name'
//! ```

use std::collections::HashMap;

use crate::value::Value;

use super::{GraphSchema, PropertyDef, SchemaError, SchemaResult, ValidationMode};

/// Result of validating a single aspect of a vertex or edge.
///
/// Validation can succeed, produce a warning, or produce an error depending
/// on the validation mode and the nature of the violation.
#[derive(Debug, Clone)]
pub enum ValidationResult {
    /// Validation passed.
    Ok,
    /// Validation produced a warning (data is allowed but may be incorrect).
    Warning(SchemaError),
    /// Validation failed with an error.
    Error(SchemaError),
}

impl ValidationResult {
    /// Returns `true` if this is an `Ok` result.
    pub fn is_ok(&self) -> bool {
        matches!(self, ValidationResult::Ok)
    }

    /// Returns `true` if this is a `Warning` result.
    pub fn is_warning(&self) -> bool {
        matches!(self, ValidationResult::Warning(_))
    }

    /// Returns `true` if this is an `Error` result.
    pub fn is_error(&self) -> bool {
        matches!(self, ValidationResult::Error(_))
    }

    /// Extract the error if this is a `Warning` or `Error`, or `None` if `Ok`.
    pub fn into_error(self) -> Option<SchemaError> {
        match self {
            ValidationResult::Ok => None,
            ValidationResult::Warning(e) | ValidationResult::Error(e) => Some(e),
        }
    }
}

/// Validate a vertex against the schema.
///
/// Checks that:
/// 1. The vertex label is known (in CLOSED mode)
/// 2. All required properties are present and non-null
/// 3. All property values have the correct type
/// 4. No unexpected properties are present (unless additional properties are allowed)
///
/// # Arguments
///
/// * `schema` - The graph schema to validate against
/// * `label` - The vertex label
/// * `properties` - The vertex properties
///
/// # Returns
///
/// A list of validation results on success. In STRICT/CLOSED mode, returns an error
/// immediately if validation fails.
///
/// # Example
///
/// ```
/// use interstellar::schema::{SchemaBuilder, PropertyType, ValidationMode, validate_vertex};
/// use interstellar::value::Value;
/// use std::collections::HashMap;
///
/// let schema = SchemaBuilder::new()
///     .mode(ValidationMode::Strict)
///     .vertex("Person")
///         .property("name", PropertyType::String)
///         .done()
///     .build();
///
/// let mut props = HashMap::new();
/// props.insert("name".to_string(), Value::String("Alice".to_string()));
///
/// let results = validate_vertex(&schema, "Person", &props).unwrap();
/// ```
pub fn validate_vertex(
    schema: &GraphSchema,
    label: &str,
    properties: &HashMap<String, Value>,
) -> SchemaResult<Vec<ValidationResult>> {
    let mut results = Vec::new();

    // Check if label is known
    let vertex_schema = match schema.vertex_schemas.get(label) {
        Some(vs) => vs,
        None => {
            match schema.mode {
                ValidationMode::None => return Ok(results),
                ValidationMode::Warn => {
                    results.push(ValidationResult::Warning(SchemaError::UnknownVertexLabel {
                        label: label.to_string(),
                    }));
                    return Ok(results);
                }
                ValidationMode::Strict => return Ok(results), // Unknown labels allowed
                ValidationMode::Closed => {
                    return Err(SchemaError::UnknownVertexLabel {
                        label: label.to_string(),
                    });
                }
            }
        }
    };

    // Validate properties
    validate_properties(
        &mut results,
        schema.mode,
        "vertex",
        label,
        vertex_schema.additional_properties,
        &vertex_schema.properties,
        properties,
    )?;

    Ok(results)
}

/// Validate an edge against the schema.
///
/// Checks that:
/// 1. The edge label is known (in CLOSED mode)
/// 2. The source vertex label is allowed for this edge type
/// 3. The target vertex label is allowed for this edge type
/// 4. All required properties are present and non-null
/// 5. All property values have the correct type
/// 6. No unexpected properties are present (unless additional properties are allowed)
///
/// # Arguments
///
/// * `schema` - The graph schema to validate against
/// * `label` - The edge label
/// * `from_label` - The source vertex label
/// * `to_label` - The target vertex label
/// * `properties` - The edge properties
///
/// # Returns
///
/// A list of validation results on success. In STRICT/CLOSED mode, returns an error
/// immediately if validation fails.
pub fn validate_edge(
    schema: &GraphSchema,
    label: &str,
    from_label: &str,
    to_label: &str,
    properties: &HashMap<String, Value>,
) -> SchemaResult<Vec<ValidationResult>> {
    let mut results = Vec::new();

    // Check if label is known
    let edge_schema = match schema.edge_schemas.get(label) {
        Some(es) => es,
        None => {
            match schema.mode {
                ValidationMode::None => return Ok(results),
                ValidationMode::Warn => {
                    results.push(ValidationResult::Warning(SchemaError::UnknownEdgeLabel {
                        label: label.to_string(),
                    }));
                    return Ok(results);
                }
                ValidationMode::Strict => return Ok(results), // Unknown labels allowed
                ValidationMode::Closed => {
                    return Err(SchemaError::UnknownEdgeLabel {
                        label: label.to_string(),
                    });
                }
            }
        }
    };

    // Validate source endpoint
    if !edge_schema.allows_from(from_label) {
        let err = SchemaError::InvalidSourceLabel {
            edge_label: label.to_string(),
            from_label: from_label.to_string(),
            allowed: edge_schema.from_labels.clone(),
        };
        match schema.mode {
            ValidationMode::None => {}
            ValidationMode::Warn => results.push(ValidationResult::Warning(err)),
            ValidationMode::Strict | ValidationMode::Closed => return Err(err),
        }
    }

    // Validate target endpoint
    if !edge_schema.allows_to(to_label) {
        let err = SchemaError::InvalidTargetLabel {
            edge_label: label.to_string(),
            to_label: to_label.to_string(),
            allowed: edge_schema.to_labels.clone(),
        };
        match schema.mode {
            ValidationMode::None => {}
            ValidationMode::Warn => results.push(ValidationResult::Warning(err)),
            ValidationMode::Strict | ValidationMode::Closed => return Err(err),
        }
    }

    // Validate properties
    validate_properties(
        &mut results,
        schema.mode,
        "edge",
        label,
        edge_schema.additional_properties,
        &edge_schema.properties,
        properties,
    )?;

    Ok(results)
}

/// Validate property updates for an existing element.
///
/// This is a lighter validation used for SET operations where we're only
/// updating specific properties, not creating a new element.
///
/// # Arguments
///
/// * `schema` - The graph schema to validate against
/// * `label` - The element label
/// * `property_key` - The property being set
/// * `value` - The new value
/// * `is_vertex` - Whether this is a vertex (true) or edge (false)
///
/// # Returns
///
/// A list of validation results on success.
pub fn validate_property_update(
    schema: &GraphSchema,
    label: &str,
    property_key: &str,
    value: &Value,
    is_vertex: bool,
) -> SchemaResult<Vec<ValidationResult>> {
    let mut results = Vec::new();

    let (schema_props, additional_allowed) = if is_vertex {
        match schema.vertex_schemas.get(label) {
            Some(vs) => (&vs.properties, vs.additional_properties),
            None => {
                // Unknown label - behavior depends on mode
                match schema.mode {
                    ValidationMode::None | ValidationMode::Strict => return Ok(results),
                    ValidationMode::Warn => {
                        results.push(ValidationResult::Warning(SchemaError::UnknownVertexLabel {
                            label: label.to_string(),
                        }));
                        return Ok(results);
                    }
                    ValidationMode::Closed => {
                        return Err(SchemaError::UnknownVertexLabel {
                            label: label.to_string(),
                        });
                    }
                }
            }
        }
    } else {
        match schema.edge_schemas.get(label) {
            Some(es) => (&es.properties, es.additional_properties),
            None => match schema.mode {
                ValidationMode::None | ValidationMode::Strict => return Ok(results),
                ValidationMode::Warn => {
                    results.push(ValidationResult::Warning(SchemaError::UnknownEdgeLabel {
                        label: label.to_string(),
                    }));
                    return Ok(results);
                }
                ValidationMode::Closed => {
                    return Err(SchemaError::UnknownEdgeLabel {
                        label: label.to_string(),
                    });
                }
            },
        }
    };

    let element_type = if is_vertex { "vertex" } else { "edge" };

    // Check if property is defined
    if let Some(prop_def) = schema_props.get(property_key) {
        // Check for null on required property
        if prop_def.required && matches!(value, Value::Null) {
            let err = SchemaError::NullRequired {
                element_type,
                label: label.to_string(),
                property: property_key.to_string(),
            };
            match schema.mode {
                ValidationMode::None => {}
                ValidationMode::Warn => results.push(ValidationResult::Warning(err)),
                ValidationMode::Strict | ValidationMode::Closed => return Err(err),
            }
        }

        // Check type (if not null)
        if !matches!(value, Value::Null) && !prop_def.value_type.matches(value) {
            let err = SchemaError::TypeMismatch {
                property: property_key.to_string(),
                expected: prop_def.value_type.clone(),
                actual: value_type_name(value).to_string(),
            };
            match schema.mode {
                ValidationMode::None => {}
                ValidationMode::Warn => results.push(ValidationResult::Warning(err)),
                ValidationMode::Strict | ValidationMode::Closed => return Err(err),
            }
        }
    } else if !additional_allowed {
        // Property not in schema and additional properties not allowed
        let err = SchemaError::UnexpectedProperty {
            element_type,
            label: label.to_string(),
            property: property_key.to_string(),
        };
        match schema.mode {
            ValidationMode::None => {}
            ValidationMode::Warn => results.push(ValidationResult::Warning(err)),
            ValidationMode::Strict | ValidationMode::Closed => return Err(err),
        }
    }

    Ok(results)
}

/// Apply default values to properties based on schema.
///
/// This function returns a new property map with default values filled in
/// for any optional properties that have defaults but are not present in
/// the input properties.
///
/// Default values are applied at query time, not stored physically.
///
/// # Arguments
///
/// * `schema` - The graph schema
/// * `label` - The element label
/// * `properties` - The existing properties
/// * `is_vertex` - Whether this is a vertex (true) or edge (false)
///
/// # Returns
///
/// A new property map with defaults applied.
pub fn apply_defaults(
    schema: &GraphSchema,
    label: &str,
    properties: &HashMap<String, Value>,
    is_vertex: bool,
) -> HashMap<String, Value> {
    let schema_props = if is_vertex {
        schema.vertex_schemas.get(label).map(|vs| &vs.properties)
    } else {
        schema.edge_schemas.get(label).map(|es| &es.properties)
    };

    let mut result = properties.clone();

    if let Some(props) = schema_props {
        for (key, def) in props {
            if !result.contains_key(key) {
                if let Some(default) = &def.default {
                    result.insert(key.clone(), default.clone());
                }
            }
        }
    }

    result
}

/// Internal function to validate properties against a schema definition.
fn validate_properties(
    results: &mut Vec<ValidationResult>,
    mode: ValidationMode,
    element_type: &'static str,
    label: &str,
    additional_properties: bool,
    schema_props: &HashMap<String, PropertyDef>,
    properties: &HashMap<String, Value>,
) -> SchemaResult<()> {
    // Check required properties
    for (key, def) in schema_props {
        if def.required {
            match properties.get(key) {
                None => {
                    let err = SchemaError::MissingRequired {
                        element_type,
                        label: label.to_string(),
                        property: key.clone(),
                    };
                    match mode {
                        ValidationMode::None => {}
                        ValidationMode::Warn => results.push(ValidationResult::Warning(err)),
                        ValidationMode::Strict | ValidationMode::Closed => return Err(err),
                    }
                }
                Some(Value::Null) => {
                    let err = SchemaError::NullRequired {
                        element_type,
                        label: label.to_string(),
                        property: key.clone(),
                    };
                    match mode {
                        ValidationMode::None => {}
                        ValidationMode::Warn => results.push(ValidationResult::Warning(err)),
                        ValidationMode::Strict | ValidationMode::Closed => return Err(err),
                    }
                }
                Some(_) => {} // Present and non-null, will check type below
            }
        }
    }

    // Check types and unexpected properties
    for (key, value) in properties {
        if let Some(def) = schema_props.get(key) {
            // Check type (skip null values as they're handled above)
            if !matches!(value, Value::Null) && !def.value_type.matches(value) {
                let err = SchemaError::TypeMismatch {
                    property: key.clone(),
                    expected: def.value_type.clone(),
                    actual: value_type_name(value).to_string(),
                };
                match mode {
                    ValidationMode::None => {}
                    ValidationMode::Warn => results.push(ValidationResult::Warning(err)),
                    ValidationMode::Strict | ValidationMode::Closed => return Err(err),
                }
            }
        } else if !additional_properties {
            // Property not in schema and additional properties not allowed
            let err = SchemaError::UnexpectedProperty {
                element_type,
                label: label.to_string(),
                property: key.clone(),
            };
            match mode {
                ValidationMode::None => {}
                ValidationMode::Warn => results.push(ValidationResult::Warning(err)),
                ValidationMode::Strict | ValidationMode::Closed => return Err(err),
            }
        }
    }

    Ok(())
}

/// Get the type name for a Value variant (for error messages).
fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "NULL",
        Value::Bool(_) => "BOOL",
        Value::Int(_) => "INT",
        Value::Float(_) => "FLOAT",
        Value::String(_) => "STRING",
        Value::List(_) => "LIST",
        Value::Map(_) => "MAP",
        Value::Vertex(_) => "VERTEX",
        Value::Edge(_) => "EDGE",
        Value::Point(_) => "POINT",
        Value::Polygon(_) => "POLYGON",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{PropertyType, SchemaBuilder};

    fn make_test_schema(mode: ValidationMode) -> GraphSchema {
        SchemaBuilder::new()
            .mode(mode)
            .vertex("Person")
            .property("name", PropertyType::String)
            .optional("age", PropertyType::Int)
            .optional_with_default("active", PropertyType::Bool, Value::Bool(true))
            .done()
            .vertex("Flexible")
            .property("type", PropertyType::String)
            .allow_additional()
            .done()
            .edge("KNOWS")
            .from(&["Person"])
            .to(&["Person"])
            .optional("since", PropertyType::Int)
            .done()
            .edge("WORKS_AT")
            .from(&["Person"])
            .to(&["Company"])
            .property("role", PropertyType::String)
            .done()
            .build()
    }

    #[test]
    fn validate_vertex_valid() {
        let schema = make_test_schema(ValidationMode::Strict);

        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));

        let results = validate_vertex(&schema, "Person", &props).unwrap();
        assert!(results.iter().all(|r| r.is_ok()));
    }

    #[test]
    fn validate_vertex_missing_required_strict() {
        let schema = make_test_schema(ValidationMode::Strict);
        let props = HashMap::new();

        let result = validate_vertex(&schema, "Person", &props);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::MissingRequired { .. }
        ));
    }

    #[test]
    fn validate_vertex_missing_required_warn() {
        let schema = make_test_schema(ValidationMode::Warn);
        let props = HashMap::new();

        let results = validate_vertex(&schema, "Person", &props).unwrap();
        assert!(results.iter().any(|r| r.is_warning()));
    }

    #[test]
    fn validate_vertex_null_required() {
        let schema = make_test_schema(ValidationMode::Strict);
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::Null);

        let result = validate_vertex(&schema, "Person", &props);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::NullRequired { .. }
        ));
    }

    #[test]
    fn validate_vertex_type_mismatch() {
        let schema = make_test_schema(ValidationMode::Strict);
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::Int(42)); // Should be String

        let result = validate_vertex(&schema, "Person", &props);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::TypeMismatch { .. }
        ));
    }

    #[test]
    fn validate_vertex_unexpected_property() {
        let schema = make_test_schema(ValidationMode::Strict);
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("unknown".to_string(), Value::String("value".to_string()));

        let result = validate_vertex(&schema, "Person", &props);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::UnexpectedProperty { .. }
        ));
    }

    #[test]
    fn validate_vertex_additional_properties_allowed() {
        let schema = make_test_schema(ValidationMode::Strict);
        let mut props = HashMap::new();
        props.insert("type".to_string(), Value::String("test".to_string()));
        props.insert("extra".to_string(), Value::Int(42));

        let results = validate_vertex(&schema, "Flexible", &props).unwrap();
        assert!(results.iter().all(|r| r.is_ok()));
    }

    #[test]
    fn validate_vertex_unknown_label_strict() {
        let schema = make_test_schema(ValidationMode::Strict);
        let props = HashMap::new();

        // Unknown labels are allowed in Strict mode
        let results = validate_vertex(&schema, "Unknown", &props).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn validate_vertex_unknown_label_closed() {
        let schema = make_test_schema(ValidationMode::Closed);
        let props = HashMap::new();

        let result = validate_vertex(&schema, "Unknown", &props);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::UnknownVertexLabel { .. }
        ));
    }

    #[test]
    fn validate_edge_valid() {
        let schema = make_test_schema(ValidationMode::Strict);
        let mut props = HashMap::new();
        props.insert("since".to_string(), Value::Int(2020));

        let results = validate_edge(&schema, "KNOWS", "Person", "Person", &props).unwrap();
        assert!(results.iter().all(|r| r.is_ok()));
    }

    #[test]
    fn validate_edge_invalid_source() {
        let schema = make_test_schema(ValidationMode::Strict);
        let mut props = HashMap::new();
        props.insert("role".to_string(), Value::String("Manager".to_string()));

        let result = validate_edge(&schema, "WORKS_AT", "Company", "Company", &props);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::InvalidSourceLabel { .. }
        ));
    }

    #[test]
    fn validate_edge_invalid_target() {
        let schema = make_test_schema(ValidationMode::Strict);
        let mut props = HashMap::new();
        props.insert("role".to_string(), Value::String("Manager".to_string()));

        let result = validate_edge(&schema, "WORKS_AT", "Person", "Person", &props);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::InvalidTargetLabel { .. }
        ));
    }

    #[test]
    fn validate_edge_missing_required() {
        let schema = make_test_schema(ValidationMode::Strict);
        let props = HashMap::new();

        // WORKS_AT requires 'role' property
        let result = validate_edge(&schema, "WORKS_AT", "Person", "Company", &props);
        assert!(result.is_err());
    }

    #[test]
    fn validate_property_update_valid() {
        let schema = make_test_schema(ValidationMode::Strict);

        let results =
            validate_property_update(&schema, "Person", "age", &Value::Int(31), true).unwrap();
        assert!(results.iter().all(|r| r.is_ok()));
    }

    #[test]
    fn validate_property_update_type_mismatch() {
        let schema = make_test_schema(ValidationMode::Strict);

        let result = validate_property_update(
            &schema,
            "Person",
            "age",
            &Value::String("thirty".to_string()),
            true,
        );
        assert!(result.is_err());
    }

    #[test]
    fn validate_property_update_null_required() {
        let schema = make_test_schema(ValidationMode::Strict);

        let result = validate_property_update(&schema, "Person", "name", &Value::Null, true);
        assert!(result.is_err());
    }

    #[test]
    fn apply_defaults_fills_missing() {
        let schema = make_test_schema(ValidationMode::Strict);

        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));

        let with_defaults = apply_defaults(&schema, "Person", &props, true);

        assert_eq!(
            with_defaults.get("name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(with_defaults.get("active"), Some(&Value::Bool(true)));
    }

    #[test]
    fn apply_defaults_does_not_overwrite() {
        let schema = make_test_schema(ValidationMode::Strict);

        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("active".to_string(), Value::Bool(false));

        let with_defaults = apply_defaults(&schema, "Person", &props, true);

        assert_eq!(with_defaults.get("active"), Some(&Value::Bool(false)));
    }

    #[test]
    fn apply_defaults_unknown_label() {
        let schema = make_test_schema(ValidationMode::Strict);
        let props = HashMap::new();

        let with_defaults = apply_defaults(&schema, "Unknown", &props, true);
        assert!(with_defaults.is_empty()); // No defaults for unknown label
    }

    #[test]
    fn validation_result_methods() {
        let ok = ValidationResult::Ok;
        assert!(ok.is_ok());
        assert!(!ok.is_warning());
        assert!(!ok.is_error());
        assert!(ok.into_error().is_none());

        let warning = ValidationResult::Warning(SchemaError::UnknownVertexLabel {
            label: "Test".to_string(),
        });
        assert!(!warning.is_ok());
        assert!(warning.is_warning());
        assert!(!warning.is_error());
        assert!(warning.into_error().is_some());

        let error = ValidationResult::Error(SchemaError::UnknownEdgeLabel {
            label: "Test".to_string(),
        });
        assert!(!error.is_ok());
        assert!(!error.is_warning());
        assert!(error.is_error());
        assert!(error.into_error().is_some());
    }

    #[test]
    fn validate_mode_none_allows_everything() {
        let schema = make_test_schema(ValidationMode::None);

        // Missing required property - should pass
        let props = HashMap::new();
        let results = validate_vertex(&schema, "Person", &props).unwrap();
        assert!(results.iter().all(|r| r.is_ok()));

        // Unknown label - should pass
        let results = validate_vertex(&schema, "Unknown", &props).unwrap();
        assert!(results.is_empty());

        // Type mismatch - should pass
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::Int(42));
        let results = validate_vertex(&schema, "Person", &props).unwrap();
        assert!(results.iter().all(|r| r.is_ok()));
    }

    #[test]
    fn validate_edge_for_edge_property() {
        let schema = make_test_schema(ValidationMode::Strict);

        let results =
            validate_property_update(&schema, "KNOWS", "since", &Value::Int(2020), false).unwrap();
        assert!(results.iter().all(|r| r.is_ok()));
    }
}
