//! Schema builder API.
//!
//! This module provides a fluent builder pattern for constructing graph schemas
//! programmatically. This is an alternative to using GQL DDL statements.
//!
//! # Example
//!
//! ```
//! use interstellar::schema::{SchemaBuilder, PropertyType, ValidationMode};
//! use interstellar::value::Value;
//!
//! let schema = SchemaBuilder::new()
//!     .mode(ValidationMode::Strict)
//!     .vertex("Person")
//!         .property("name", PropertyType::String)
//!         .optional("age", PropertyType::Int)
//!         .optional_with_default("active", PropertyType::Bool, Value::Bool(true))
//!         .done()
//!     .vertex("Company")
//!         .property("name", PropertyType::String)
//!         .allow_additional()
//!         .done()
//!     .edge("WORKS_AT")
//!         .from(&["Person"])
//!         .to(&["Company"])
//!         .property("role", PropertyType::String)
//!         .optional("since", PropertyType::Int)
//!         .done()
//!     .edge("KNOWS")
//!         .from(&["Person"])
//!         .to(&["Person"])
//!         .optional_with_default("weight", PropertyType::Float, Value::Float(1.0))
//!         .done()
//!     .build();
//!
//! assert!(schema.has_vertex_schema("Person"));
//! assert!(schema.has_edge_schema("WORKS_AT"));
//! ```

use std::collections::HashMap;

use crate::value::Value;

use super::{EdgeSchema, GraphSchema, PropertyDef, PropertyType, ValidationMode, VertexSchema};

/// Builder for constructing graph schemas fluently.
///
/// Use [`SchemaBuilder::new()`] to create a new builder, then chain methods
/// to add vertex and edge schemas.
///
/// # Example
///
/// ```
/// use interstellar::schema::{SchemaBuilder, PropertyType, ValidationMode};
///
/// let schema = SchemaBuilder::new()
///     .mode(ValidationMode::Strict)
///     .vertex("Person")
///         .property("name", PropertyType::String)
///         .done()
///     .build();
/// ```
pub struct SchemaBuilder {
    schema: GraphSchema,
}

impl SchemaBuilder {
    /// Create a new schema builder with default settings.
    ///
    /// The default validation mode is [`ValidationMode::None`].
    pub fn new() -> Self {
        Self {
            schema: GraphSchema::default(),
        }
    }

    /// Set the validation mode for the schema.
    ///
    /// See [`ValidationMode`] for details on each mode.
    pub fn mode(mut self, mode: ValidationMode) -> Self {
        self.schema.mode = mode;
        self
    }

    /// Begin defining a vertex schema for the given label.
    ///
    /// Returns a [`VertexSchemaBuilder`] for fluent property definitions.
    /// Call [`.done()`](VertexSchemaBuilder::done) to finish the vertex schema
    /// and return to the parent builder.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::schema::{SchemaBuilder, PropertyType};
    ///
    /// let schema = SchemaBuilder::new()
    ///     .vertex("Person")
    ///         .property("name", PropertyType::String)
    ///         .optional("age", PropertyType::Int)
    ///         .done()
    ///     .build();
    /// ```
    pub fn vertex(self, label: &str) -> VertexSchemaBuilder {
        VertexSchemaBuilder {
            parent: self,
            schema: VertexSchema {
                label: label.to_string(),
                properties: HashMap::new(),
                additional_properties: false,
            },
        }
    }

    /// Begin defining an edge schema for the given label.
    ///
    /// Returns an [`EdgeSchemaBuilder`] for fluent endpoint and property definitions.
    /// Call [`.done()`](EdgeSchemaBuilder::done) to finish the edge schema
    /// and return to the parent builder.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::schema::{SchemaBuilder, PropertyType};
    ///
    /// let schema = SchemaBuilder::new()
    ///     .vertex("Person").done()
    ///     .edge("KNOWS")
    ///         .from(&["Person"])
    ///         .to(&["Person"])
    ///         .optional("since", PropertyType::Int)
    ///         .done()
    ///     .build();
    /// ```
    pub fn edge(self, label: &str) -> EdgeSchemaBuilder {
        EdgeSchemaBuilder {
            parent: self,
            schema: EdgeSchema {
                label: label.to_string(),
                from_labels: Vec::new(),
                to_labels: Vec::new(),
                properties: HashMap::new(),
                additional_properties: false,
            },
        }
    }

    /// Build and return the final [`GraphSchema`].
    pub fn build(self) -> GraphSchema {
        self.schema
    }
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for vertex schema definitions.
///
/// Created by [`SchemaBuilder::vertex()`]. Use method chaining to add
/// properties, then call [`.done()`](Self::done) to return to the parent builder.
pub struct VertexSchemaBuilder {
    parent: SchemaBuilder,
    schema: VertexSchema,
}

impl VertexSchemaBuilder {
    /// Add a required property to the vertex schema.
    ///
    /// Required properties must be present and non-null when creating
    /// vertices with this label (in STRICT or CLOSED validation mode).
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::schema::{SchemaBuilder, PropertyType};
    ///
    /// let schema = SchemaBuilder::new()
    ///     .vertex("Person")
    ///         .property("name", PropertyType::String)  // Required
    ///         .done()
    ///     .build();
    /// ```
    pub fn property(mut self, key: &str, value_type: PropertyType) -> Self {
        self.schema.properties.insert(
            key.to_string(),
            PropertyDef {
                key: key.to_string(),
                value_type,
                required: true,
                default: None,
            },
        );
        self
    }

    /// Add an optional property to the vertex schema.
    ///
    /// Optional properties may be omitted when creating vertices.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::schema::{SchemaBuilder, PropertyType};
    ///
    /// let schema = SchemaBuilder::new()
    ///     .vertex("Person")
    ///         .optional("nickname", PropertyType::String)  // Optional
    ///         .done()
    ///     .build();
    /// ```
    pub fn optional(mut self, key: &str, value_type: PropertyType) -> Self {
        self.schema.properties.insert(
            key.to_string(),
            PropertyDef {
                key: key.to_string(),
                value_type,
                required: false,
                default: None,
            },
        );
        self
    }

    /// Add an optional property with a default value.
    ///
    /// The default value is applied at query time if the property is missing,
    /// not stored physically.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::schema::{SchemaBuilder, PropertyType};
    /// use interstellar::value::Value;
    ///
    /// let schema = SchemaBuilder::new()
    ///     .vertex("Person")
    ///         .optional_with_default("active", PropertyType::Bool, Value::Bool(true))
    ///         .done()
    ///     .build();
    /// ```
    pub fn optional_with_default(
        mut self,
        key: &str,
        value_type: PropertyType,
        default: Value,
    ) -> Self {
        self.schema.properties.insert(
            key.to_string(),
            PropertyDef {
                key: key.to_string(),
                value_type,
                required: false,
                default: Some(default),
            },
        );
        self
    }

    /// Allow properties not defined in the schema.
    ///
    /// By default, vertices with this label can only have properties defined
    /// in the schema (in STRICT or CLOSED mode). Call this method to allow
    /// arbitrary additional properties.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::schema::{SchemaBuilder, PropertyType};
    ///
    /// let schema = SchemaBuilder::new()
    ///     .vertex("Metadata")
    ///         .property("type", PropertyType::String)
    ///         .allow_additional()  // Allow any other properties
    ///         .done()
    ///     .build();
    /// ```
    pub fn allow_additional(mut self) -> Self {
        self.schema.additional_properties = true;
        self
    }

    /// Finish the vertex schema and return to the parent builder.
    ///
    /// The vertex schema is added to the parent [`GraphSchema`].
    pub fn done(mut self) -> SchemaBuilder {
        self.parent
            .schema
            .vertex_schemas
            .insert(self.schema.label.clone(), self.schema);
        self.parent
    }
}

/// Builder for edge schema definitions.
///
/// Created by [`SchemaBuilder::edge()`]. Use method chaining to set
/// endpoints and add properties, then call [`.done()`](Self::done) to
/// return to the parent builder.
pub struct EdgeSchemaBuilder {
    parent: SchemaBuilder,
    schema: EdgeSchema,
}

impl EdgeSchemaBuilder {
    /// Set the allowed source vertex labels.
    ///
    /// Edges with this label can only originate from vertices with one
    /// of the specified labels.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::schema::SchemaBuilder;
    ///
    /// let schema = SchemaBuilder::new()
    ///     .vertex("Person").done()
    ///     .vertex("Employee").done()
    ///     .edge("WORKS_AT")
    ///         .from(&["Person", "Employee"])
    ///         .to(&["Company"])
    ///         .done()
    ///     .build();
    /// ```
    pub fn from(mut self, labels: &[&str]) -> Self {
        self.schema.from_labels = labels.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Set the allowed target vertex labels.
    ///
    /// Edges with this label can only point to vertices with one
    /// of the specified labels.
    pub fn to(mut self, labels: &[&str]) -> Self {
        self.schema.to_labels = labels.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Add a required property to the edge schema.
    ///
    /// Required properties must be present and non-null when creating
    /// edges with this label (in STRICT or CLOSED validation mode).
    pub fn property(mut self, key: &str, value_type: PropertyType) -> Self {
        self.schema.properties.insert(
            key.to_string(),
            PropertyDef {
                key: key.to_string(),
                value_type,
                required: true,
                default: None,
            },
        );
        self
    }

    /// Add an optional property to the edge schema.
    ///
    /// Optional properties may be omitted when creating edges.
    pub fn optional(mut self, key: &str, value_type: PropertyType) -> Self {
        self.schema.properties.insert(
            key.to_string(),
            PropertyDef {
                key: key.to_string(),
                value_type,
                required: false,
                default: None,
            },
        );
        self
    }

    /// Add an optional property with a default value.
    ///
    /// The default value is applied at query time if the property is missing,
    /// not stored physically.
    pub fn optional_with_default(
        mut self,
        key: &str,
        value_type: PropertyType,
        default: Value,
    ) -> Self {
        self.schema.properties.insert(
            key.to_string(),
            PropertyDef {
                key: key.to_string(),
                value_type,
                required: false,
                default: Some(default),
            },
        );
        self
    }

    /// Allow properties not defined in the schema.
    ///
    /// By default, edges with this label can only have properties defined
    /// in the schema (in STRICT or CLOSED mode). Call this method to allow
    /// arbitrary additional properties.
    pub fn allow_additional(mut self) -> Self {
        self.schema.additional_properties = true;
        self
    }

    /// Finish the edge schema and return to the parent builder.
    ///
    /// The edge schema is added to the parent [`GraphSchema`].
    pub fn done(mut self) -> SchemaBuilder {
        self.parent
            .schema
            .edge_schemas
            .insert(self.schema.label.clone(), self.schema);
        self.parent
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_empty_schema() {
        let schema = SchemaBuilder::new().build();
        assert_eq!(schema.mode, ValidationMode::None);
        assert!(schema.vertex_schemas.is_empty());
        assert!(schema.edge_schemas.is_empty());
    }

    #[test]
    fn build_schema_with_mode() {
        let schema = SchemaBuilder::new().mode(ValidationMode::Strict).build();
        assert_eq!(schema.mode, ValidationMode::Strict);
    }

    #[test]
    fn build_vertex_schema() {
        let schema = SchemaBuilder::new()
            .vertex("Person")
            .property("name", PropertyType::String)
            .optional("age", PropertyType::Int)
            .optional_with_default("active", PropertyType::Bool, Value::Bool(true))
            .allow_additional()
            .done()
            .build();

        assert!(schema.has_vertex_schema("Person"));
        let vs = schema.vertex_schema("Person").unwrap();

        assert_eq!(vs.label, "Person");
        assert!(vs.additional_properties);

        // Check required property
        let name_def = vs.properties.get("name").unwrap();
        assert!(name_def.required);
        assert_eq!(name_def.value_type, PropertyType::String);
        assert!(name_def.default.is_none());

        // Check optional property
        let age_def = vs.properties.get("age").unwrap();
        assert!(!age_def.required);
        assert_eq!(age_def.value_type, PropertyType::Int);
        assert!(age_def.default.is_none());

        // Check optional with default
        let active_def = vs.properties.get("active").unwrap();
        assert!(!active_def.required);
        assert_eq!(active_def.value_type, PropertyType::Bool);
        assert_eq!(active_def.default, Some(Value::Bool(true)));
    }

    #[test]
    fn build_edge_schema() {
        let schema = SchemaBuilder::new()
            .vertex("Person")
            .done()
            .vertex("Company")
            .done()
            .edge("WORKS_AT")
            .from(&["Person"])
            .to(&["Company"])
            .property("role", PropertyType::String)
            .optional("since", PropertyType::Int)
            .done()
            .build();

        assert!(schema.has_edge_schema("WORKS_AT"));
        let es = schema.edge_schema("WORKS_AT").unwrap();

        assert_eq!(es.label, "WORKS_AT");
        assert_eq!(es.from_labels, vec!["Person"]);
        assert_eq!(es.to_labels, vec!["Company"]);
        assert!(!es.additional_properties);

        // Check properties
        let role_def = es.properties.get("role").unwrap();
        assert!(role_def.required);

        let since_def = es.properties.get("since").unwrap();
        assert!(!since_def.required);
    }

    #[test]
    fn build_edge_with_multiple_endpoints() {
        let schema = SchemaBuilder::new()
            .vertex("Person")
            .done()
            .vertex("Employee")
            .done()
            .vertex("Company")
            .done()
            .vertex("Startup")
            .done()
            .edge("WORKS_AT")
            .from(&["Person", "Employee"])
            .to(&["Company", "Startup"])
            .done()
            .build();

        let es = schema.edge_schema("WORKS_AT").unwrap();
        assert!(es.allows_from("Person"));
        assert!(es.allows_from("Employee"));
        assert!(!es.allows_from("Company"));
        assert!(es.allows_to("Company"));
        assert!(es.allows_to("Startup"));
        assert!(!es.allows_to("Person"));
    }

    #[test]
    fn build_complex_schema() {
        let schema = SchemaBuilder::new()
            .mode(ValidationMode::Closed)
            .vertex("Person")
            .property("name", PropertyType::String)
            .optional("age", PropertyType::Int)
            .done()
            .vertex("Company")
            .property("name", PropertyType::String)
            .optional("founded", PropertyType::Int)
            .done()
            .vertex("Product")
            .property("name", PropertyType::String)
            .optional(
                "tags",
                PropertyType::List(Some(Box::new(PropertyType::String))),
            )
            .allow_additional()
            .done()
            .edge("WORKS_AT")
            .from(&["Person"])
            .to(&["Company"])
            .property("role", PropertyType::String)
            .optional_with_default("weight", PropertyType::Float, Value::Float(1.0))
            .done()
            .edge("KNOWS")
            .from(&["Person"])
            .to(&["Person"])
            .done()
            .edge("PRODUCES")
            .from(&["Company"])
            .to(&["Product"])
            .done()
            .build();

        assert_eq!(schema.mode, ValidationMode::Closed);

        // Check vertex schemas
        assert_eq!(schema.vertex_schemas.len(), 3);
        assert!(schema.has_vertex_schema("Person"));
        assert!(schema.has_vertex_schema("Company"));
        assert!(schema.has_vertex_schema("Product"));

        // Check edge schemas
        assert_eq!(schema.edge_schemas.len(), 3);
        assert!(schema.has_edge_schema("WORKS_AT"));
        assert!(schema.has_edge_schema("KNOWS"));
        assert!(schema.has_edge_schema("PRODUCES"));

        // Check Product allows additional properties
        let product = schema.vertex_schema("Product").unwrap();
        assert!(product.additional_properties);

        // Check WORKS_AT has default weight
        let works_at = schema.edge_schema("WORKS_AT").unwrap();
        assert_eq!(
            works_at.property_default("weight"),
            Some(&Value::Float(1.0))
        );
    }

    #[test]
    fn schema_builder_default() {
        let builder = SchemaBuilder::default();
        let schema = builder.build();
        assert_eq!(schema.mode, ValidationMode::None);
    }

    #[test]
    fn vertex_schema_iterators() {
        let schema = SchemaBuilder::new()
            .vertex("Person")
            .property("name", PropertyType::String)
            .property("email", PropertyType::String)
            .optional("age", PropertyType::Int)
            .optional("bio", PropertyType::String)
            .done()
            .build();

        let vs = schema.vertex_schema("Person").unwrap();

        let required: Vec<_> = vs.required_properties().collect();
        assert_eq!(required.len(), 2);
        assert!(required.contains(&"name"));
        assert!(required.contains(&"email"));

        let optional: Vec<_> = vs.optional_properties().collect();
        assert_eq!(optional.len(), 2);
        assert!(optional.contains(&"age"));
        assert!(optional.contains(&"bio"));
    }
}
