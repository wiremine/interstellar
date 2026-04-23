//! DDL (Data Definition Language) execution for GQL schema management.
//!
//! This module executes DDL statements to modify the graph schema.
//! DDL statements include CREATE TYPE, ALTER TYPE, DROP TYPE, and
//! SET SCHEMA VALIDATION.
//!
//! # Usage
//!
//! ```
//! use interstellar::gql::{parse_statement, execute_ddl, Statement};
//! use interstellar::schema::GraphSchema;
//!
//! let mut schema = GraphSchema::new();
//!
//! // Parse and execute DDL
//! let stmt = parse_statement("CREATE NODE TYPE Person (name STRING NOT NULL)").unwrap();
//! if let Statement::Ddl(ddl) = stmt {
//!     execute_ddl(&mut schema, &ddl).unwrap();
//! }
//!
//! assert!(schema.has_vertex_schema("Person"));
//! ```
//!
//! # Index DDL
//!
//! Index DDL statements (CREATE INDEX, DROP INDEX) cannot be executed via
//! [`execute_ddl`] because they require graph storage access. Instead, use
//! the helper function [`create_index_spec`] to convert a parsed `CreateIndex`
//! to an [`IndexSpec`](crate::index::IndexSpec), then call
//! [`Graph::create_index()`](crate::storage::Graph::create_index).
//!
//! ```ignore
//! use interstellar::gql::{parse_statement, create_index_spec, Statement, DdlStatement};
//! use interstellar::storage::Graph;
//!
//! let graph = Graph::new();
//!
//! let stmt = parse_statement("CREATE INDEX idx_age ON :Person(age)").unwrap();
//! if let Statement::Ddl(ddl) = stmt {
//!     if let DdlStatement::CreateIndex(create) = *ddl {
//!         let spec = create_index_spec(&create)?;
//!         graph.create_index(spec)?;
//!     }
//! }
//! ```

use crate::gql::ast::{
    AlterEdgeType, AlterNodeType, AlterTypeAction, CreateEdgeType, CreateIndex, CreateNodeType,
    DdlStatement, DropType, Literal, PropertyDefinition, PropertyTypeAst, SetValidation,
    ValidationModeAst,
};
use crate::index::{ElementType, IndexBuilder, IndexSpec};
use crate::schema::{
    EdgeSchema, GraphSchema, PropertyDef, PropertyType, SchemaError, SchemaResult, ValidationMode,
    VertexSchema,
};
use crate::value::Value;
use std::collections::HashMap;

/// Execute a DDL statement, modifying the schema in place.
///
/// # Arguments
///
/// * `schema` - The schema to modify
/// * `stmt` - The DDL statement to execute
///
/// # Returns
///
/// `Ok(())` on success, or a `SchemaError` if the operation fails.
///
/// # Errors
///
/// - `TypeAlreadyExists` - When creating a type that already exists
/// - `TypeNotFound` - When altering/dropping a type that doesn't exist
/// - `PropertyAlreadyExists` - When adding a property that already exists
/// - `PropertyNotFound` - When dropping a property that doesn't exist
/// - `MissingEndpointConstraints` - When creating an edge type without FROM/TO
///
/// # Example
///
/// ```
/// use interstellar::gql::{parse_statement, execute_ddl, Statement};
/// use interstellar::schema::{GraphSchema, ValidationMode};
///
/// let mut schema = GraphSchema::new();
///
/// // Create a node type
/// let stmt = parse_statement("CREATE NODE TYPE Person (name STRING NOT NULL, age INT)").unwrap();
/// if let Statement::Ddl(ddl) = stmt {
///     execute_ddl(&mut schema, &ddl).unwrap();
/// }
///
/// // Set validation mode
/// let stmt = parse_statement("SET SCHEMA VALIDATION STRICT").unwrap();
/// if let Statement::Ddl(ddl) = stmt {
///     execute_ddl(&mut schema, &ddl).unwrap();
/// }
///
/// assert!(schema.has_vertex_schema("Person"));
/// assert_eq!(schema.mode, ValidationMode::Strict);
/// ```
pub fn execute_ddl(schema: &mut GraphSchema, stmt: &DdlStatement) -> SchemaResult<()> {
    match stmt {
        DdlStatement::CreateNodeType(create) => execute_create_node_type(schema, create),
        DdlStatement::CreateEdgeType(create) => execute_create_edge_type(schema, create),
        DdlStatement::AlterNodeType(alter) => execute_alter_node_type(schema, alter),
        DdlStatement::AlterEdgeType(alter) => execute_alter_edge_type(schema, alter),
        DdlStatement::DropNodeType(drop) => execute_drop_node_type(schema, drop),
        DdlStatement::DropEdgeType(drop) => execute_drop_edge_type(schema, drop),
        DdlStatement::SetValidation(set) => execute_set_validation(schema, set),
        DdlStatement::CreateIndex(_) | DdlStatement::DropIndex(_) => {
            // Index DDL operates on graph storage, not schema.
            // Use Graph::create_index() / drop_index() directly.
            Err(SchemaError::IndexDdlNotSupported)
        }
    }
}

// =============================================================================
// Index DDL Helpers
// =============================================================================

/// Convert a parsed [`CreateIndex`] AST node to an [`IndexSpec`].
///
/// This function allows you to use the GQL parser to define indexes,
/// then create them using [`Graph::create_index()`](crate::storage::Graph::create_index).
///
/// # GQL Syntax Limitation
///
/// The GQL `CREATE INDEX` syntax doesn't distinguish between vertex and edge indexes.
/// This function defaults to [`ElementType::Vertex`]. For edge indexes, either:
/// - Use [`create_index_spec_for_edge()`] instead
/// - Create the [`IndexSpec`] directly using [`IndexBuilder::edge()`]
///
/// # Example
///
/// ```rust,ignore
/// use interstellar::gql::{parse_statement, create_index_spec, Statement, DdlStatement};
/// use interstellar::storage::Graph;
///
/// let graph = Graph::new();
///
/// let stmt = parse_statement("CREATE INDEX idx_age ON :Person(age)").unwrap();
/// if let Statement::Ddl(ddl) = stmt {
///     if let DdlStatement::CreateIndex(create) = *ddl {
///         let spec = create_index_spec(&create).unwrap();
///         graph.create_index(spec).unwrap();
///     }
/// }
/// ```
///
/// # Errors
///
/// Returns [`IndexError::MissingProperty`] if the property field is empty
/// (which should not happen with a valid parsed AST).
pub fn create_index_spec(create: &CreateIndex) -> Result<IndexSpec, crate::index::IndexError> {
    create_index_spec_impl(create, ElementType::Vertex)
}

/// Convert a parsed [`CreateIndex`] AST node to an [`IndexSpec`] for edges.
///
/// This is the edge-specific variant of [`create_index_spec()`].
///
/// # Example
///
/// ```rust,ignore
/// use interstellar::gql::{parse_statement, create_index_spec_for_edge, Statement, DdlStatement};
/// use interstellar::storage::Graph;
///
/// let graph = Graph::new();
///
/// let stmt = parse_statement("CREATE INDEX idx_since ON :KNOWS(since)").unwrap();
/// if let Statement::Ddl(ddl) = stmt {
///     if let DdlStatement::CreateIndex(create) = *ddl {
///         let spec = create_index_spec_for_edge(&create).unwrap();
///         graph.create_index(spec).unwrap();
///     }
/// }
/// ```
pub fn create_index_spec_for_edge(
    create: &CreateIndex,
) -> Result<IndexSpec, crate::index::IndexError> {
    create_index_spec_impl(create, ElementType::Edge)
}

/// Internal implementation for creating IndexSpec from CreateIndex AST.
fn create_index_spec_impl(
    create: &CreateIndex,
    element_type: ElementType,
) -> Result<IndexSpec, crate::index::IndexError> {
    let mut builder = match element_type {
        ElementType::Vertex => IndexBuilder::vertex(),
        ElementType::Edge => IndexBuilder::edge(),
    };

    // Set the index name
    builder = builder.name(&create.name);

    // Set the property to index
    builder = builder.property(&create.property);

    // Set label filter if specified
    if let Some(ref label) = create.label {
        builder = builder.label(label);
    }

    // Set unique if specified
    if create.unique {
        builder = builder.unique();
    }

    // Set rtree if specified (spec-56)
    if create.rtree {
        builder = builder.rtree();
    }

    builder.build()
}

// =============================================================================
// DDL Execution Functions
// =============================================================================

fn execute_create_node_type(schema: &mut GraphSchema, stmt: &CreateNodeType) -> SchemaResult<()> {
    if schema.vertex_schemas.contains_key(&stmt.name) {
        return Err(SchemaError::TypeAlreadyExists {
            name: stmt.name.clone(),
        });
    }

    let vertex_schema = VertexSchema {
        label: stmt.name.clone(),
        properties: convert_properties(&stmt.properties),
        additional_properties: false,
    };

    schema
        .vertex_schemas
        .insert(stmt.name.clone(), vertex_schema);
    Ok(())
}

fn execute_create_edge_type(schema: &mut GraphSchema, stmt: &CreateEdgeType) -> SchemaResult<()> {
    if schema.edge_schemas.contains_key(&stmt.name) {
        return Err(SchemaError::TypeAlreadyExists {
            name: stmt.name.clone(),
        });
    }

    if stmt.from_types.is_empty() || stmt.to_types.is_empty() {
        return Err(SchemaError::MissingEndpointConstraints);
    }

    let edge_schema = EdgeSchema {
        label: stmt.name.clone(),
        from_labels: stmt.from_types.clone(),
        to_labels: stmt.to_types.clone(),
        properties: convert_properties(&stmt.properties),
        additional_properties: false,
    };

    schema.edge_schemas.insert(stmt.name.clone(), edge_schema);
    Ok(())
}

fn execute_alter_node_type(schema: &mut GraphSchema, stmt: &AlterNodeType) -> SchemaResult<()> {
    let vertex_schema =
        schema
            .vertex_schemas
            .get_mut(&stmt.name)
            .ok_or_else(|| SchemaError::TypeNotFound {
                name: stmt.name.clone(),
            })?;

    apply_vertex_alter_action(vertex_schema, &stmt.name, &stmt.action)
}

fn execute_alter_edge_type(schema: &mut GraphSchema, stmt: &AlterEdgeType) -> SchemaResult<()> {
    let edge_schema =
        schema
            .edge_schemas
            .get_mut(&stmt.name)
            .ok_or_else(|| SchemaError::TypeNotFound {
                name: stmt.name.clone(),
            })?;

    apply_edge_alter_action(edge_schema, &stmt.name, &stmt.action)
}

fn apply_vertex_alter_action(
    vertex_schema: &mut VertexSchema,
    type_name: &str,
    action: &AlterTypeAction,
) -> SchemaResult<()> {
    match action {
        AlterTypeAction::AllowAdditionalProperties => {
            vertex_schema.additional_properties = true;
            Ok(())
        }
        AlterTypeAction::AddProperty(prop) => {
            if vertex_schema.properties.contains_key(&prop.name) {
                return Err(SchemaError::PropertyAlreadyExists {
                    type_name: type_name.to_string(),
                    property: prop.name.clone(),
                });
            }
            vertex_schema
                .properties
                .insert(prop.name.clone(), convert_property(prop));
            Ok(())
        }
        AlterTypeAction::DropProperty(prop_name) => {
            if vertex_schema.properties.remove(prop_name).is_none() {
                return Err(SchemaError::PropertyNotFound {
                    type_name: type_name.to_string(),
                    property: prop_name.clone(),
                });
            }
            Ok(())
        }
    }
}

fn apply_edge_alter_action(
    edge_schema: &mut EdgeSchema,
    type_name: &str,
    action: &AlterTypeAction,
) -> SchemaResult<()> {
    match action {
        AlterTypeAction::AllowAdditionalProperties => {
            edge_schema.additional_properties = true;
            Ok(())
        }
        AlterTypeAction::AddProperty(prop) => {
            if edge_schema.properties.contains_key(&prop.name) {
                return Err(SchemaError::PropertyAlreadyExists {
                    type_name: type_name.to_string(),
                    property: prop.name.clone(),
                });
            }
            edge_schema
                .properties
                .insert(prop.name.clone(), convert_property(prop));
            Ok(())
        }
        AlterTypeAction::DropProperty(prop_name) => {
            if edge_schema.properties.remove(prop_name).is_none() {
                return Err(SchemaError::PropertyNotFound {
                    type_name: type_name.to_string(),
                    property: prop_name.clone(),
                });
            }
            Ok(())
        }
    }
}

fn execute_drop_node_type(schema: &mut GraphSchema, stmt: &DropType) -> SchemaResult<()> {
    if schema.vertex_schemas.remove(&stmt.name).is_none() {
        return Err(SchemaError::TypeNotFound {
            name: stmt.name.clone(),
        });
    }
    Ok(())
}

fn execute_drop_edge_type(schema: &mut GraphSchema, stmt: &DropType) -> SchemaResult<()> {
    if schema.edge_schemas.remove(&stmt.name).is_none() {
        return Err(SchemaError::TypeNotFound {
            name: stmt.name.clone(),
        });
    }
    Ok(())
}

fn execute_set_validation(schema: &mut GraphSchema, stmt: &SetValidation) -> SchemaResult<()> {
    schema.mode = convert_validation_mode(stmt.mode);
    Ok(())
}

// =============================================================================
// Conversion Functions
// =============================================================================

fn convert_properties(props: &[PropertyDefinition]) -> HashMap<String, PropertyDef> {
    props
        .iter()
        .map(|p| (p.name.clone(), convert_property(p)))
        .collect()
}

fn convert_property(prop: &PropertyDefinition) -> PropertyDef {
    PropertyDef {
        key: prop.name.clone(),
        value_type: convert_property_type(&prop.prop_type),
        required: prop.required,
        default: prop.default.as_ref().map(literal_to_value),
    }
}

fn convert_property_type(ast: &PropertyTypeAst) -> PropertyType {
    match ast {
        PropertyTypeAst::String => PropertyType::String,
        PropertyTypeAst::Int => PropertyType::Int,
        PropertyTypeAst::Float => PropertyType::Float,
        PropertyTypeAst::Bool => PropertyType::Bool,
        PropertyTypeAst::Any => PropertyType::Any,
        PropertyTypeAst::List(None) => PropertyType::List(None),
        PropertyTypeAst::List(Some(inner)) => {
            PropertyType::List(Some(Box::new(convert_property_type(inner))))
        }
        PropertyTypeAst::Map(None) => PropertyType::Map(None),
        PropertyTypeAst::Map(Some(inner)) => {
            PropertyType::Map(Some(Box::new(convert_property_type(inner))))
        }
    }
}

fn convert_validation_mode(ast: ValidationModeAst) -> ValidationMode {
    match ast {
        ValidationModeAst::None => ValidationMode::None,
        ValidationModeAst::Warn => ValidationMode::Warn,
        ValidationModeAst::Strict => ValidationMode::Strict,
        ValidationModeAst::Closed => ValidationMode::Closed,
    }
}

fn literal_to_value(lit: &Literal) -> Value {
    lit.clone().into()
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gql::{parse_statement, Statement};

    /// Helper to parse and execute a DDL statement.
    fn exec_ddl(schema: &mut GraphSchema, query: &str) -> SchemaResult<()> {
        let stmt = parse_statement(query).expect("Failed to parse DDL");
        match stmt {
            Statement::Ddl(ddl) => execute_ddl(schema, &ddl),
            _ => panic!("Expected DDL statement, got {:?}", stmt),
        }
    }

    // =========================================================================
    // CREATE NODE TYPE Tests
    // =========================================================================

    #[test]
    fn create_node_type_basic() {
        let mut schema = GraphSchema::new();
        exec_ddl(
            &mut schema,
            "CREATE NODE TYPE Person (name STRING NOT NULL, age INT)",
        )
        .unwrap();

        assert!(schema.has_vertex_schema("Person"));
        let vs = schema.vertex_schema("Person").unwrap();
        assert_eq!(vs.label, "Person");
        assert_eq!(vs.properties.len(), 2);

        let name_prop = vs.properties.get("name").unwrap();
        assert_eq!(name_prop.value_type, PropertyType::String);
        assert!(name_prop.required);

        let age_prop = vs.properties.get("age").unwrap();
        assert_eq!(age_prop.value_type, PropertyType::Int);
        assert!(!age_prop.required);
    }

    #[test]
    fn create_node_type_empty() {
        let mut schema = GraphSchema::new();
        exec_ddl(&mut schema, "CREATE NODE TYPE Tag ()").unwrap();

        assert!(schema.has_vertex_schema("Tag"));
        let vs = schema.vertex_schema("Tag").unwrap();
        assert!(vs.properties.is_empty());
    }

    #[test]
    fn create_node_type_with_default() {
        let mut schema = GraphSchema::new();
        exec_ddl(
            &mut schema,
            "CREATE NODE TYPE Person (active BOOL DEFAULT true)",
        )
        .unwrap();

        let vs = schema.vertex_schema("Person").unwrap();
        let prop = vs.properties.get("active").unwrap();
        assert_eq!(prop.default, Some(Value::Bool(true)));
    }

    #[test]
    fn create_node_type_all_types() {
        let mut schema = GraphSchema::new();
        exec_ddl(
            &mut schema,
            "CREATE NODE TYPE AllTypes (
                s STRING,
                i INT,
                f FLOAT,
                b BOOL,
                l LIST,
                m MAP,
                a ANY
            )",
        )
        .unwrap();

        let vs = schema.vertex_schema("AllTypes").unwrap();
        assert_eq!(
            vs.properties.get("s").unwrap().value_type,
            PropertyType::String
        );
        assert_eq!(
            vs.properties.get("i").unwrap().value_type,
            PropertyType::Int
        );
        assert_eq!(
            vs.properties.get("f").unwrap().value_type,
            PropertyType::Float
        );
        assert_eq!(
            vs.properties.get("b").unwrap().value_type,
            PropertyType::Bool
        );
        assert_eq!(
            vs.properties.get("l").unwrap().value_type,
            PropertyType::List(None)
        );
        assert_eq!(
            vs.properties.get("m").unwrap().value_type,
            PropertyType::Map(None)
        );
        assert_eq!(
            vs.properties.get("a").unwrap().value_type,
            PropertyType::Any
        );
    }

    #[test]
    fn create_node_type_typed_list() {
        let mut schema = GraphSchema::new();
        exec_ddl(&mut schema, "CREATE NODE TYPE Data (tags LIST<STRING>)").unwrap();

        let vs = schema.vertex_schema("Data").unwrap();
        assert_eq!(
            vs.properties.get("tags").unwrap().value_type,
            PropertyType::List(Some(Box::new(PropertyType::String)))
        );
    }

    #[test]
    fn create_node_type_typed_map() {
        let mut schema = GraphSchema::new();
        exec_ddl(&mut schema, "CREATE NODE TYPE Data (scores MAP<INT>)").unwrap();

        let vs = schema.vertex_schema("Data").unwrap();
        assert_eq!(
            vs.properties.get("scores").unwrap().value_type,
            PropertyType::Map(Some(Box::new(PropertyType::Int)))
        );
    }

    #[test]
    fn create_node_type_duplicate_error() {
        let mut schema = GraphSchema::new();
        exec_ddl(&mut schema, "CREATE NODE TYPE Person ()").unwrap();

        let err = exec_ddl(&mut schema, "CREATE NODE TYPE Person ()").unwrap_err();
        assert!(matches!(err, SchemaError::TypeAlreadyExists { name } if name == "Person"));
    }

    // =========================================================================
    // CREATE EDGE TYPE Tests
    // =========================================================================

    #[test]
    fn create_edge_type_basic() {
        let mut schema = GraphSchema::new();
        exec_ddl(
            &mut schema,
            "CREATE EDGE TYPE KNOWS (since INT) FROM Person TO Person",
        )
        .unwrap();

        assert!(schema.has_edge_schema("KNOWS"));
        let es = schema.edge_schema("KNOWS").unwrap();
        assert_eq!(es.label, "KNOWS");
        assert_eq!(es.from_labels, vec!["Person"]);
        assert_eq!(es.to_labels, vec!["Person"]);
        assert_eq!(es.properties.len(), 1);

        let since_prop = es.properties.get("since").unwrap();
        assert_eq!(since_prop.value_type, PropertyType::Int);
    }

    #[test]
    fn create_edge_type_empty_props() {
        let mut schema = GraphSchema::new();
        exec_ddl(
            &mut schema,
            "CREATE EDGE TYPE FOLLOWS () FROM Person TO Person",
        )
        .unwrap();

        let es = schema.edge_schema("FOLLOWS").unwrap();
        assert!(es.properties.is_empty());
    }

    #[test]
    fn create_edge_type_multiple_endpoints() {
        let mut schema = GraphSchema::new();
        exec_ddl(
            &mut schema,
            "CREATE EDGE TYPE TAGGED () FROM Post, Comment, Photo TO Tag",
        )
        .unwrap();

        let es = schema.edge_schema("TAGGED").unwrap();
        assert_eq!(es.from_labels, vec!["Post", "Comment", "Photo"]);
        assert_eq!(es.to_labels, vec!["Tag"]);
    }

    #[test]
    fn create_edge_type_different_endpoints() {
        let mut schema = GraphSchema::new();
        exec_ddl(
            &mut schema,
            "CREATE EDGE TYPE WORKS_AT (role STRING NOT NULL) FROM Person TO Company",
        )
        .unwrap();

        let es = schema.edge_schema("WORKS_AT").unwrap();
        assert_eq!(es.from_labels, vec!["Person"]);
        assert_eq!(es.to_labels, vec!["Company"]);

        let role_prop = es.properties.get("role").unwrap();
        assert!(role_prop.required);
    }

    #[test]
    fn create_edge_type_with_default() {
        let mut schema = GraphSchema::new();
        exec_ddl(
            &mut schema,
            "CREATE EDGE TYPE KNOWS (weight FLOAT DEFAULT 1.0) FROM Person TO Person",
        )
        .unwrap();

        let es = schema.edge_schema("KNOWS").unwrap();
        let prop = es.properties.get("weight").unwrap();
        assert_eq!(prop.default, Some(Value::Float(1.0.into())));
    }

    #[test]
    fn create_edge_type_duplicate_error() {
        let mut schema = GraphSchema::new();
        exec_ddl(
            &mut schema,
            "CREATE EDGE TYPE KNOWS () FROM Person TO Person",
        )
        .unwrap();

        let err = exec_ddl(
            &mut schema,
            "CREATE EDGE TYPE KNOWS () FROM Person TO Person",
        )
        .unwrap_err();
        assert!(matches!(err, SchemaError::TypeAlreadyExists { name } if name == "KNOWS"));
    }

    // =========================================================================
    // ALTER NODE TYPE Tests
    // =========================================================================

    #[test]
    fn alter_node_type_allow_additional() {
        let mut schema = GraphSchema::new();
        exec_ddl(&mut schema, "CREATE NODE TYPE Person (name STRING)").unwrap();

        assert!(
            !schema
                .vertex_schema("Person")
                .unwrap()
                .additional_properties
        );

        exec_ddl(
            &mut schema,
            "ALTER NODE TYPE Person ALLOW ADDITIONAL PROPERTIES",
        )
        .unwrap();

        assert!(
            schema
                .vertex_schema("Person")
                .unwrap()
                .additional_properties
        );
    }

    #[test]
    fn alter_node_type_add_property() {
        let mut schema = GraphSchema::new();
        exec_ddl(&mut schema, "CREATE NODE TYPE Person (name STRING)").unwrap();

        exec_ddl(&mut schema, "ALTER NODE TYPE Person ADD bio STRING").unwrap();

        let vs = schema.vertex_schema("Person").unwrap();
        assert_eq!(vs.properties.len(), 2);
        assert!(vs.properties.contains_key("bio"));
    }

    #[test]
    fn alter_node_type_add_property_with_default() {
        let mut schema = GraphSchema::new();
        exec_ddl(&mut schema, "CREATE NODE TYPE Person ()").unwrap();

        exec_ddl(
            &mut schema,
            "ALTER NODE TYPE Person ADD verified BOOL DEFAULT false",
        )
        .unwrap();

        let vs = schema.vertex_schema("Person").unwrap();
        let prop = vs.properties.get("verified").unwrap();
        assert_eq!(prop.default, Some(Value::Bool(false)));
    }

    #[test]
    fn alter_node_type_drop_property() {
        let mut schema = GraphSchema::new();
        exec_ddl(
            &mut schema,
            "CREATE NODE TYPE Person (name STRING, bio STRING)",
        )
        .unwrap();

        exec_ddl(&mut schema, "ALTER NODE TYPE Person DROP bio").unwrap();

        let vs = schema.vertex_schema("Person").unwrap();
        assert_eq!(vs.properties.len(), 1);
        assert!(!vs.properties.contains_key("bio"));
    }

    #[test]
    fn alter_node_type_not_found() {
        let mut schema = GraphSchema::new();

        let err = exec_ddl(
            &mut schema,
            "ALTER NODE TYPE Unknown ALLOW ADDITIONAL PROPERTIES",
        )
        .unwrap_err();
        assert!(matches!(err, SchemaError::TypeNotFound { name } if name == "Unknown"));
    }

    #[test]
    fn alter_node_type_add_duplicate_property() {
        let mut schema = GraphSchema::new();
        exec_ddl(&mut schema, "CREATE NODE TYPE Person (name STRING)").unwrap();

        let err = exec_ddl(&mut schema, "ALTER NODE TYPE Person ADD name STRING").unwrap_err();
        assert!(matches!(
            err,
            SchemaError::PropertyAlreadyExists { type_name, property }
            if type_name == "Person" && property == "name"
        ));
    }

    #[test]
    fn alter_node_type_drop_nonexistent_property() {
        let mut schema = GraphSchema::new();
        exec_ddl(&mut schema, "CREATE NODE TYPE Person ()").unwrap();

        let err = exec_ddl(&mut schema, "ALTER NODE TYPE Person DROP bio").unwrap_err();
        assert!(matches!(
            err,
            SchemaError::PropertyNotFound { type_name, property }
            if type_name == "Person" && property == "bio"
        ));
    }

    // =========================================================================
    // ALTER EDGE TYPE Tests
    // =========================================================================

    #[test]
    fn alter_edge_type_allow_additional() {
        let mut schema = GraphSchema::new();
        exec_ddl(
            &mut schema,
            "CREATE EDGE TYPE KNOWS () FROM Person TO Person",
        )
        .unwrap();

        assert!(!schema.edge_schema("KNOWS").unwrap().additional_properties);

        exec_ddl(
            &mut schema,
            "ALTER EDGE TYPE KNOWS ALLOW ADDITIONAL PROPERTIES",
        )
        .unwrap();

        assert!(schema.edge_schema("KNOWS").unwrap().additional_properties);
    }

    #[test]
    fn alter_edge_type_add_property() {
        let mut schema = GraphSchema::new();
        exec_ddl(
            &mut schema,
            "CREATE EDGE TYPE KNOWS () FROM Person TO Person",
        )
        .unwrap();

        exec_ddl(&mut schema, "ALTER EDGE TYPE KNOWS ADD notes STRING").unwrap();

        let es = schema.edge_schema("KNOWS").unwrap();
        assert!(es.properties.contains_key("notes"));
    }

    #[test]
    fn alter_edge_type_drop_property() {
        let mut schema = GraphSchema::new();
        exec_ddl(
            &mut schema,
            "CREATE EDGE TYPE KNOWS (notes STRING) FROM Person TO Person",
        )
        .unwrap();

        exec_ddl(&mut schema, "ALTER EDGE TYPE KNOWS DROP notes").unwrap();

        let es = schema.edge_schema("KNOWS").unwrap();
        assert!(!es.properties.contains_key("notes"));
    }

    #[test]
    fn alter_edge_type_not_found() {
        let mut schema = GraphSchema::new();

        let err = exec_ddl(
            &mut schema,
            "ALTER EDGE TYPE Unknown ALLOW ADDITIONAL PROPERTIES",
        )
        .unwrap_err();
        assert!(matches!(err, SchemaError::TypeNotFound { name } if name == "Unknown"));
    }

    // =========================================================================
    // DROP TYPE Tests
    // =========================================================================

    #[test]
    fn drop_node_type() {
        let mut schema = GraphSchema::new();
        exec_ddl(&mut schema, "CREATE NODE TYPE Person ()").unwrap();

        assert!(schema.has_vertex_schema("Person"));

        exec_ddl(&mut schema, "DROP NODE TYPE Person").unwrap();

        assert!(!schema.has_vertex_schema("Person"));
    }

    #[test]
    fn drop_node_type_not_found() {
        let mut schema = GraphSchema::new();

        let err = exec_ddl(&mut schema, "DROP NODE TYPE Unknown").unwrap_err();
        assert!(matches!(err, SchemaError::TypeNotFound { name } if name == "Unknown"));
    }

    #[test]
    fn drop_edge_type() {
        let mut schema = GraphSchema::new();
        exec_ddl(
            &mut schema,
            "CREATE EDGE TYPE KNOWS () FROM Person TO Person",
        )
        .unwrap();

        assert!(schema.has_edge_schema("KNOWS"));

        exec_ddl(&mut schema, "DROP EDGE TYPE KNOWS").unwrap();

        assert!(!schema.has_edge_schema("KNOWS"));
    }

    #[test]
    fn drop_edge_type_not_found() {
        let mut schema = GraphSchema::new();

        let err = exec_ddl(&mut schema, "DROP EDGE TYPE Unknown").unwrap_err();
        assert!(matches!(err, SchemaError::TypeNotFound { name } if name == "Unknown"));
    }

    // =========================================================================
    // SET SCHEMA VALIDATION Tests
    // =========================================================================

    #[test]
    fn set_validation_none() {
        let mut schema = GraphSchema::with_mode(ValidationMode::Strict);

        exec_ddl(&mut schema, "SET SCHEMA VALIDATION NONE").unwrap();

        assert_eq!(schema.mode, ValidationMode::None);
    }

    #[test]
    fn set_validation_warn() {
        let mut schema = GraphSchema::new();

        exec_ddl(&mut schema, "SET SCHEMA VALIDATION WARN").unwrap();

        assert_eq!(schema.mode, ValidationMode::Warn);
    }

    #[test]
    fn set_validation_strict() {
        let mut schema = GraphSchema::new();

        exec_ddl(&mut schema, "SET SCHEMA VALIDATION STRICT").unwrap();

        assert_eq!(schema.mode, ValidationMode::Strict);
    }

    #[test]
    fn set_validation_closed() {
        let mut schema = GraphSchema::new();

        exec_ddl(&mut schema, "SET SCHEMA VALIDATION CLOSED").unwrap();

        assert_eq!(schema.mode, ValidationMode::Closed);
    }

    // =========================================================================
    // Integration Tests
    // =========================================================================

    #[test]
    fn full_schema_workflow() {
        let mut schema = GraphSchema::new();

        // Create vertex types
        exec_ddl(
            &mut schema,
            "CREATE NODE TYPE Person (name STRING NOT NULL, age INT)",
        )
        .unwrap();
        exec_ddl(
            &mut schema,
            "CREATE NODE TYPE Company (name STRING NOT NULL)",
        )
        .unwrap();

        // Create edge types
        exec_ddl(
            &mut schema,
            "CREATE EDGE TYPE KNOWS (since INT) FROM Person TO Person",
        )
        .unwrap();
        exec_ddl(
            &mut schema,
            "CREATE EDGE TYPE WORKS_AT (role STRING NOT NULL) FROM Person TO Company",
        )
        .unwrap();

        // Set validation mode
        exec_ddl(&mut schema, "SET SCHEMA VALIDATION STRICT").unwrap();

        // Verify
        assert_eq!(schema.mode, ValidationMode::Strict);
        assert_eq!(schema.type_count(), 4);
        assert!(schema.has_vertex_schema("Person"));
        assert!(schema.has_vertex_schema("Company"));
        assert!(schema.has_edge_schema("KNOWS"));
        assert!(schema.has_edge_schema("WORKS_AT"));

        // Modify types
        exec_ddl(
            &mut schema,
            "ALTER NODE TYPE Person ALLOW ADDITIONAL PROPERTIES",
        )
        .unwrap();
        exec_ddl(&mut schema, "ALTER NODE TYPE Person ADD email STRING").unwrap();

        let person = schema.vertex_schema("Person").unwrap();
        assert!(person.additional_properties);
        assert_eq!(person.properties.len(), 3);

        // Drop a type
        exec_ddl(&mut schema, "DROP EDGE TYPE KNOWS").unwrap();
        assert!(!schema.has_edge_schema("KNOWS"));
        assert_eq!(schema.type_count(), 3);
    }

    #[test]
    fn case_insensitive_keywords() {
        let mut schema = GraphSchema::new();

        // Test case insensitivity
        exec_ddl(
            &mut schema,
            "create node type Person (name string not null)",
        )
        .unwrap();
        exec_ddl(
            &mut schema,
            "CREATE EDGE TYPE knows () from Person to Person",
        )
        .unwrap();
        exec_ddl(&mut schema, "Set Schema Validation Strict").unwrap();

        assert!(schema.has_vertex_schema("Person"));
        assert!(schema.has_edge_schema("knows"));
        assert_eq!(schema.mode, ValidationMode::Strict);
    }

    // =========================================================================
    // Index DDL Tests
    // =========================================================================

    #[test]
    fn parse_create_index_basic() {
        let stmt = parse_statement("CREATE INDEX idx_age ON :Person(age)").unwrap();
        match stmt {
            Statement::Ddl(ddl) => match *ddl {
                DdlStatement::CreateIndex(create) => {
                    assert_eq!(create.name, "idx_age");
                    assert_eq!(create.label, Some("Person".to_string()));
                    assert_eq!(create.property, "age");
                    assert!(!create.unique);
                }
                _ => panic!("Expected CreateIndex"),
            },
            _ => panic!("Expected DDL statement"),
        }
    }

    #[test]
    fn parse_create_index_unique() {
        let stmt = parse_statement("CREATE UNIQUE INDEX idx_email ON :User(email)").unwrap();
        match stmt {
            Statement::Ddl(ddl) => match *ddl {
                DdlStatement::CreateIndex(create) => {
                    assert_eq!(create.name, "idx_email");
                    assert_eq!(create.label, Some("User".to_string()));
                    assert_eq!(create.property, "email");
                    assert!(create.unique);
                }
                _ => panic!("Expected CreateIndex"),
            },
            _ => panic!("Expected DDL statement"),
        }
    }

    #[test]
    fn parse_create_index_no_label() {
        let stmt = parse_statement("CREATE INDEX idx_created ON (created_at)").unwrap();
        match stmt {
            Statement::Ddl(ddl) => match *ddl {
                DdlStatement::CreateIndex(create) => {
                    assert_eq!(create.name, "idx_created");
                    assert_eq!(create.label, None);
                    assert_eq!(create.property, "created_at");
                    assert!(!create.unique);
                }
                _ => panic!("Expected CreateIndex"),
            },
            _ => panic!("Expected DDL statement"),
        }
    }

    #[test]
    fn parse_drop_index() {
        let stmt = parse_statement("DROP INDEX idx_age").unwrap();
        match stmt {
            Statement::Ddl(ddl) => match *ddl {
                DdlStatement::DropIndex(drop) => {
                    assert_eq!(drop.name, "idx_age");
                }
                _ => panic!("Expected DropIndex"),
            },
            _ => panic!("Expected DDL statement"),
        }
    }

    #[test]
    fn execute_ddl_returns_error_for_create_index() {
        let mut schema = GraphSchema::new();
        let err = exec_ddl(&mut schema, "CREATE INDEX idx_age ON :Person(age)").unwrap_err();
        assert!(matches!(err, SchemaError::IndexDdlNotSupported));
    }

    #[test]
    fn execute_ddl_returns_error_for_drop_index() {
        let mut schema = GraphSchema::new();
        let err = exec_ddl(&mut schema, "DROP INDEX idx_age").unwrap_err();
        assert!(matches!(err, SchemaError::IndexDdlNotSupported));
    }

    #[test]
    fn create_index_spec_basic() {
        use super::create_index_spec;
        use crate::index::{ElementType, IndexType};

        let stmt = parse_statement("CREATE INDEX idx_age ON :Person(age)").unwrap();
        let create = match stmt {
            Statement::Ddl(ddl) => match *ddl {
                DdlStatement::CreateIndex(c) => c,
                _ => panic!("Expected CreateIndex"),
            },
            _ => panic!("Expected DDL statement"),
        };

        let spec = create_index_spec(&create).unwrap();
        assert_eq!(spec.name, "idx_age");
        assert_eq!(spec.element_type, ElementType::Vertex);
        assert_eq!(spec.label, Some("Person".to_string()));
        assert_eq!(spec.property, "age");
        assert_eq!(spec.index_type, IndexType::BTree);
    }

    #[test]
    fn create_index_spec_unique() {
        use super::create_index_spec;
        use crate::index::{ElementType, IndexType};

        let stmt = parse_statement("CREATE UNIQUE INDEX uniq_email ON :User(email)").unwrap();
        let create = match stmt {
            Statement::Ddl(ddl) => match *ddl {
                DdlStatement::CreateIndex(c) => c,
                _ => panic!("Expected CreateIndex"),
            },
            _ => panic!("Expected DDL statement"),
        };

        let spec = create_index_spec(&create).unwrap();
        assert_eq!(spec.name, "uniq_email");
        assert_eq!(spec.element_type, ElementType::Vertex);
        assert_eq!(spec.label, Some("User".to_string()));
        assert_eq!(spec.property, "email");
        assert_eq!(spec.index_type, IndexType::Unique);
    }

    #[test]
    fn create_index_spec_no_label() {
        use super::create_index_spec;
        use crate::index::ElementType;

        let stmt = parse_statement("CREATE INDEX idx_ts ON (timestamp)").unwrap();
        let create = match stmt {
            Statement::Ddl(ddl) => match *ddl {
                DdlStatement::CreateIndex(c) => c,
                _ => panic!("Expected CreateIndex"),
            },
            _ => panic!("Expected DDL statement"),
        };

        let spec = create_index_spec(&create).unwrap();
        assert_eq!(spec.name, "idx_ts");
        assert_eq!(spec.element_type, ElementType::Vertex);
        assert_eq!(spec.label, None);
        assert_eq!(spec.property, "timestamp");
    }

    #[test]
    fn create_index_spec_for_edge() {
        use super::create_index_spec_for_edge;
        use crate::index::{ElementType, IndexType};

        let stmt = parse_statement("CREATE INDEX idx_since ON :KNOWS(since)").unwrap();
        let create = match stmt {
            Statement::Ddl(ddl) => match *ddl {
                DdlStatement::CreateIndex(c) => c,
                _ => panic!("Expected CreateIndex"),
            },
            _ => panic!("Expected DDL statement"),
        };

        let spec = create_index_spec_for_edge(&create).unwrap();
        assert_eq!(spec.name, "idx_since");
        assert_eq!(spec.element_type, ElementType::Edge);
        assert_eq!(spec.label, Some("KNOWS".to_string()));
        assert_eq!(spec.property, "since");
        assert_eq!(spec.index_type, IndexType::BTree);
    }

    #[test]
    fn create_index_case_insensitive() {
        // Test that keywords are case-insensitive
        let stmt = parse_statement("create unique index Idx_Name on :Label(prop)").unwrap();
        match stmt {
            Statement::Ddl(ddl) => match *ddl {
                DdlStatement::CreateIndex(create) => {
                    assert_eq!(create.name, "Idx_Name");
                    assert_eq!(create.label, Some("Label".to_string()));
                    assert_eq!(create.property, "prop");
                    assert!(create.unique);
                }
                _ => panic!("Expected CreateIndex"),
            },
            _ => panic!("Expected DDL statement"),
        }
    }

    #[test]
    fn drop_index_case_insensitive() {
        let stmt = parse_statement("drop index My_Index").unwrap();
        match stmt {
            Statement::Ddl(ddl) => match *ddl {
                DdlStatement::DropIndex(drop) => {
                    assert_eq!(drop.name, "My_Index");
                }
                _ => panic!("Expected DropIndex"),
            },
            _ => panic!("Expected DDL statement"),
        }
    }

    // =========================================================================
    // Geospatial DDL tests (spec-56)
    // =========================================================================

    #[test]
    fn parse_create_rtree_index() {
        let stmt = parse_statement("CREATE RTREE INDEX idx_loc ON :Person(home)").unwrap();
        match stmt {
            Statement::Ddl(ddl) => match *ddl {
                DdlStatement::CreateIndex(create) => {
                    assert_eq!(create.name, "idx_loc");
                    assert!(create.rtree);
                    assert!(!create.unique);
                    assert_eq!(create.label, Some("Person".to_string()));
                    assert_eq!(create.property, "home");
                }
                _ => panic!("Expected CreateIndex"),
            },
            _ => panic!("Expected DDL statement"),
        }
    }

    #[test]
    fn create_rtree_index_spec() {
        use crate::index::IndexType;
        let stmt = parse_statement("CREATE RTREE INDEX idx_loc ON :Person(home)").unwrap();
        if let Statement::Ddl(ddl) = stmt {
            if let DdlStatement::CreateIndex(create) = *ddl {
                let spec = super::create_index_spec(&create).unwrap();
                assert_eq!(spec.index_type, IndexType::RTree);
                assert_eq!(spec.property, "home");
            }
        }
    }
}
