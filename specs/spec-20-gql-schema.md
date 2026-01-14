# Spec-20: GQL Schema and DDL Support

This specification defines the GQL Data Definition Language (DDL) for Intersteller, enabling users to define graph schemas using ISO GQL-inspired syntax with automatic validation of mutations.

---

## Table of Contents

1. [Overview](#1-overview)
2. [ISO GQL DDL Syntax](#2-iso-gql-ddl-syntax)
3. [Grammar Changes](#3-grammar-changes)
4. [AST Changes](#4-ast-changes)
5. [Schema Module](#5-schema-module)
6. [DDL Execution](#6-ddl-execution)
7. [Mutation Validation](#7-mutation-validation)
8. [Persistence](#8-persistence)
9. [Implementation Plan](#9-implementation-plan)
10. [Testing Strategy](#10-testing-strategy)

---

## 1. Overview

### 1.1 Motivation

Schema-free graphs provide flexibility but can lead to:
- Inconsistent property names (typos like `"nmae"` vs `"name"`)
- Type mismatches (`"30"` string vs `30` integer for age)
- Missing required data discovered only at query time
- Invalid edge connections (e.g., `Person -[:WORKS_AT]-> Person` when it should be `Person -[:WORKS_AT]-> Company`)

GQL DDL support enables users to define schemas declaratively using familiar SQL-like syntax, with automatic validation at mutation time.

### 1.2 Design Principles

1. **ISO GQL Alignment**: Use ISO GQL-style DDL syntax (`CREATE NODE TYPE`, `CREATE EDGE TYPE`)
2. **Opt-in Validation**: Schemas are optional; unschemaed graphs work exactly as before
3. **Fail-fast**: Validation errors surface at mutation time, not query time
4. **Query-time Defaults**: Default values are applied at query time, not stored physically
5. **Required Endpoints**: Edge types must specify source and target vertex type constraints

### 1.3 Scope

This specification covers:
- DDL statements: `CREATE NODE TYPE`, `CREATE EDGE TYPE`, `ALTER TYPE`, `DROP TYPE`, `SET SCHEMA VALIDATION`
- Schema validation on mutations (CREATE, SET, MERGE)
- Schema persistence for mmap backend

Deferred to future phases:
- Schema versioning and migrations
- Index creation via DDL
- Constraint expressions (numeric bounds, regex patterns)

---

## 2. ISO GQL DDL Syntax

### 2.1 CREATE NODE TYPE

Creates a vertex type definition with property constraints.

```sql
CREATE NODE TYPE TypeName (
    property_name TYPE [NOT NULL] [DEFAULT value],
    ...
)
```

**Examples:**

```sql
-- Basic vertex type with required and optional properties
CREATE NODE TYPE Person (
    name STRING NOT NULL,
    age INT,
    email STRING,
    active BOOL DEFAULT true
)

-- Vertex type with list and map properties
CREATE NODE TYPE Product (
    name STRING NOT NULL,
    tags LIST<STRING>,
    metadata MAP<ANY>
)

-- Minimal vertex type (no properties defined, but label is known)
CREATE NODE TYPE Tag ()
```

### 2.2 CREATE EDGE TYPE

Creates an edge type definition with endpoint constraints and property constraints.

```sql
CREATE EDGE TYPE TypeName (
    property_name TYPE [NOT NULL] [DEFAULT value],
    ...
) FROM SourceType1, SourceType2 TO TargetType1, TargetType2
```

**Examples:**

```sql
-- Edge between same vertex types
CREATE EDGE TYPE KNOWS (
    since INT,
    weight FLOAT DEFAULT 1.0
) FROM Person TO Person

-- Edge between different vertex types
CREATE EDGE TYPE WORKS_AT (
    role STRING NOT NULL,
    start_date INT,
    end_date INT
) FROM Person TO Company

-- Edge with multiple valid source/target types
CREATE EDGE TYPE TAGGED (
    created_at INT NOT NULL
) FROM Post, Comment, Photo TO Tag

-- Edge with no properties
CREATE EDGE TYPE FOLLOWS () FROM Person TO Person
```

### 2.3 Property Types

| GQL Type | Rust PropertyType | Value Variant | Example |
|----------|-------------------|---------------|---------|
| `STRING` | `PropertyType::String` | `Value::String` | `'hello'` |
| `INT` | `PropertyType::Int` | `Value::Int` | `42` |
| `FLOAT` | `PropertyType::Float` | `Value::Float` | `3.14` |
| `BOOL` | `PropertyType::Bool` | `Value::Bool` | `true` |
| `LIST` | `PropertyType::List(None)` | `Value::List` | `[1, 2, 3]` |
| `LIST<T>` | `PropertyType::List(Some(T))` | `Value::List` of T | `['a', 'b']` |
| `MAP` | `PropertyType::Map(None)` | `Value::Map` | `{k: v}` |
| `MAP<T>` | `PropertyType::Map(Some(T))` | `Value::Map` with T values | `{k: 42}` |
| `ANY` | `PropertyType::Any` | Any variant | any value |

### 2.4 Property Modifiers

| Modifier | Description | Default |
|----------|-------------|---------|
| `NOT NULL` | Property is required; cannot be null or missing | Optional (nullable) |
| `DEFAULT value` | Default value applied at query time if missing | No default |

**Modifier Rules:**
- `NOT NULL` properties must be provided on CREATE
- `DEFAULT` values are applied at query time, not stored physically
- `NOT NULL DEFAULT value` means required on CREATE, default only applies if explicitly set to NULL later

### 2.5 ALTER TYPE

Modifies an existing type definition.

```sql
-- Allow additional properties not in schema
ALTER NODE TYPE Person ALLOW ADDITIONAL PROPERTIES
ALTER EDGE TYPE KNOWS ALLOW ADDITIONAL PROPERTIES

-- Add a new property to a type
ALTER NODE TYPE Person ADD bio STRING

-- Drop a property from a type (existing data keeps property)
ALTER NODE TYPE Person DROP bio
```

### 2.6 DROP TYPE

Removes a type definition from the schema.

```sql
DROP NODE TYPE Person
DROP EDGE TYPE KNOWS
```

**Behavior:**
- Dropping a type does NOT delete existing vertices/edges with that label
- Existing data becomes "unschemaed" (no validation for that label)
- If validation mode is CLOSED, operations on dropped types will fail

### 2.7 SET SCHEMA VALIDATION

Sets the schema validation mode.

```sql
SET SCHEMA VALIDATION NONE    -- No validation (schema is documentation only)
SET SCHEMA VALIDATION WARN    -- Log warnings but allow invalid data
SET SCHEMA VALIDATION STRICT  -- Validate types with schemas, allow unknown types
SET SCHEMA VALIDATION CLOSED  -- All types must have schemas defined
```

**Validation Mode Behaviors:**

| Mode | Unknown Label | Schema Violation | Additional Properties |
|------|---------------|------------------|----------------------|
| `NONE` | Allowed | Allowed | Allowed |
| `WARN` | Allowed (log) | Allowed (log) | Allowed (log) |
| `STRICT` | Allowed | Rejected | Rejected (unless allowed) |
| `CLOSED` | Rejected | Rejected | Rejected (unless allowed) |

---

## 3. Grammar Changes

### 3.1 New Keywords

Add the following keywords to `grammar.pest`:

```pest
// Schema/DDL keywords
TYPE       = @{ ^"type" ~ !ASCII_ALPHANUMERIC }
NODE       = @{ ^"node" ~ !ASCII_ALPHANUMERIC }
EDGE       = @{ ^"edge" ~ !ASCII_ALPHANUMERIC }
GRAPH      = @{ ^"graph" ~ !ASCII_ALPHANUMERIC }
FROM_KW    = @{ ^"from" ~ !ASCII_ALPHANUMERIC }
TO_KW      = @{ ^"to" ~ !ASCII_ALPHANUMERIC }
ALTER      = @{ ^"alter" ~ !ASCII_ALPHANUMERIC }
DROP       = @{ ^"drop" ~ !ASCII_ALPHANUMERIC }
ADD        = @{ ^"add" ~ !ASCII_ALPHANUMERIC }
ALLOW      = @{ ^"allow" ~ !ASCII_ALPHANUMERIC }
ADDITIONAL = @{ ^"additional" ~ !ASCII_ALPHANUMERIC }
PROPERTIES = @{ ^"properties" ~ !ASCII_ALPHANUMERIC }
VALIDATION = @{ ^"validation" ~ !ASCII_ALPHANUMERIC }
SCHEMA     = @{ ^"schema" ~ !ASCII_ALPHANUMERIC }
STRICT     = @{ ^"strict" ~ !ASCII_ALPHANUMERIC }
CLOSED     = @{ ^"closed" ~ !ASCII_ALPHANUMERIC }
WARN_KW    = @{ ^"warn" ~ !ASCII_ALPHANUMERIC }
DEFAULT    = @{ ^"default" ~ !ASCII_ALPHANUMERIC }

// Property type keywords
STRING_TYPE = @{ ^"string" ~ !ASCII_ALPHANUMERIC }
INT_TYPE    = @{ ^"int" ~ !ASCII_ALPHANUMERIC }
FLOAT_TYPE  = @{ ^"float" ~ !ASCII_ALPHANUMERIC }
BOOL_TYPE   = @{ ^"bool" ~ !ASCII_ALPHANUMERIC }
LIST_TYPE   = @{ ^"list" ~ !ASCII_ALPHANUMERIC }
MAP_TYPE    = @{ ^"map" ~ !ASCII_ALPHANUMERIC }
ANY_TYPE    = @{ ^"any" ~ !ASCII_ALPHANUMERIC }
```

### 3.2 Statement Entry Point Update

Modify the `statement` rule to include DDL:

```pest
// Entry point - statement which may be a read query, UNION, mutation, or DDL
statement = { SOI ~ (ddl_statement | mutation_statement | read_statement) ~ EOI }
```

### 3.3 DDL Statement Rules

```pest
// =============================================================================
// DDL Statements (Schema Definition)
// =============================================================================

ddl_statement = {
    create_node_type
    | create_edge_type
    | alter_node_type
    | alter_edge_type
    | drop_node_type
    | drop_edge_type
    | set_schema_validation
}

// CREATE NODE TYPE Person (name STRING NOT NULL, age INT)
create_node_type = {
    CREATE ~ NODE ~ TYPE ~ identifier ~
    "(" ~ property_def_list? ~ ")"
}

// CREATE EDGE TYPE KNOWS (since INT) FROM Person TO Person
create_edge_type = {
    CREATE ~ EDGE ~ TYPE ~ identifier ~
    "(" ~ property_def_list? ~ ")" ~
    edge_endpoint_clause
}

// FROM Person, Employee TO Company, Startup
edge_endpoint_clause = {
    FROM_KW ~ type_name_list ~ TO_KW ~ type_name_list
}

// Comma-separated list of type names
type_name_list = { identifier ~ ("," ~ identifier)* }

// Property definitions
property_def_list = { property_def ~ ("," ~ property_def)* }

// name STRING NOT NULL DEFAULT 'unknown'
property_def = {
    identifier ~ property_type ~ not_null_modifier? ~ default_modifier?
}

// NOT NULL modifier
not_null_modifier = { NOT ~ NULL }

// DEFAULT literal
default_modifier = { DEFAULT ~ literal }

// Property types
property_type = {
    list_type
    | map_type
    | STRING_TYPE
    | INT_TYPE
    | FLOAT_TYPE
    | BOOL_TYPE
    | ANY_TYPE
}

// LIST or LIST<STRING>
list_type = { LIST_TYPE ~ ("<" ~ property_type ~ ">")? }

// MAP or MAP<INT>
map_type = { MAP_TYPE ~ ("<" ~ property_type ~ ">")? }

// ALTER NODE TYPE Person ALLOW ADDITIONAL PROPERTIES
// ALTER NODE TYPE Person ADD email STRING
// ALTER NODE TYPE Person DROP email
alter_node_type = {
    ALTER ~ NODE ~ TYPE ~ identifier ~ alter_type_action
}

// ALTER EDGE TYPE KNOWS ALLOW ADDITIONAL PROPERTIES
alter_edge_type = {
    ALTER ~ EDGE ~ TYPE ~ identifier ~ alter_type_action
}

alter_type_action = {
    allow_additional_properties
    | add_property_action
    | drop_property_action
}

allow_additional_properties = { ALLOW ~ ADDITIONAL ~ PROPERTIES }

add_property_action = { ADD ~ property_def }

drop_property_action = { DROP ~ identifier }

// DROP NODE TYPE Person
drop_node_type = { DROP ~ NODE ~ TYPE ~ identifier }

// DROP EDGE TYPE KNOWS
drop_edge_type = { DROP ~ EDGE ~ TYPE ~ identifier }

// SET SCHEMA VALIDATION STRICT
set_schema_validation = { SET ~ SCHEMA ~ VALIDATION ~ validation_mode }

validation_mode = { STRICT | CLOSED | WARN_KW | NONE_KW }
```

### 3.4 Keyword List Update

Add the new keywords to the `keyword` rule to prevent them from being used as identifiers:

```pest
keyword = {
    MATCH | RETURN | WHERE | ORDER | BY | GROUP | LIMIT | OFFSET | SKIP | HAVING |
    AS | AND | OR | NOT | TRUE | FALSE | NULL | ASC | DESC |
    IN | IS | CONTAINS | STARTS | ENDS | WITH | DISTINCT | EXISTS |
    CASE | WHEN | THEN | ELSE | END | UNION | ALL | OPTIONAL |
    PATH | UNWIND | CREATE | SET | REMOVE | DELETE | DETACH | MERGE | ON | LET |
    REDUCE | ANY_KW | NONE_KW | SINGLE | CALL |
    // DDL keywords
    TYPE | NODE | EDGE | GRAPH | FROM_KW | TO_KW | ALTER | DROP | ADD |
    ALLOW | ADDITIONAL | PROPERTIES | VALIDATION | SCHEMA | STRICT | CLOSED | WARN_KW | DEFAULT |
    STRING_TYPE | INT_TYPE | FLOAT_TYPE | BOOL_TYPE | LIST_TYPE | MAP_TYPE | ANY_TYPE
}
```

---

## 4. AST Changes

### 4.1 Statement Enum Extension

Extend the `Statement` enum in `ast.rs`:

```rust
/// A GQL statement which may be a query, UNION, mutation, or DDL.
#[derive(Debug, Clone, Serialize)]
pub enum Statement {
    /// A single query (boxed to reduce enum variant size).
    Query(Box<Query>),
    /// A UNION of multiple queries.
    Union {
        queries: Vec<Query>,
        all: bool,
    },
    /// A mutation statement (CREATE, SET, DELETE, MERGE, etc.)
    Mutation(Box<MutationQuery>),
    /// A DDL statement (schema definition)
    Ddl(Box<DdlStatement>),
}
```

### 4.2 DDL AST Types

Add new DDL-specific AST types:

```rust
// =============================================================================
// DDL Statement Types (Schema Definition)
// =============================================================================

/// A DDL (Data Definition Language) statement for schema management.
///
/// DDL statements define the structure of the graph schema, including
/// vertex types, edge types, property constraints, and validation modes.
#[derive(Debug, Clone, Serialize)]
pub enum DdlStatement {
    /// CREATE NODE TYPE statement
    CreateNodeType(CreateNodeType),
    /// CREATE EDGE TYPE statement
    CreateEdgeType(CreateEdgeType),
    /// ALTER NODE TYPE statement
    AlterNodeType(AlterNodeType),
    /// ALTER EDGE TYPE statement
    AlterEdgeType(AlterEdgeType),
    /// DROP NODE TYPE statement
    DropNodeType(DropType),
    /// DROP EDGE TYPE statement
    DropEdgeType(DropType),
    /// SET SCHEMA VALIDATION statement
    SetValidation(SetValidation),
}

/// CREATE NODE TYPE statement.
///
/// Defines a vertex type with optional property constraints.
///
/// # Example
///
/// ```text
/// CREATE NODE TYPE Person (
///     name STRING NOT NULL,
///     age INT,
///     email STRING DEFAULT 'unknown'
/// )
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct CreateNodeType {
    /// The name of the node type (becomes the vertex label)
    pub name: String,
    /// Property definitions for this type
    pub properties: Vec<PropertyDefinition>,
}

/// CREATE EDGE TYPE statement.
///
/// Defines an edge type with endpoint constraints and optional property constraints.
///
/// # Example
///
/// ```text
/// CREATE EDGE TYPE KNOWS (
///     since INT,
///     weight FLOAT DEFAULT 1.0
/// ) FROM Person TO Person
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct CreateEdgeType {
    /// The name of the edge type (becomes the edge label)
    pub name: String,
    /// Property definitions for this type
    pub properties: Vec<PropertyDefinition>,
    /// Allowed source vertex labels
    pub from_types: Vec<String>,
    /// Allowed target vertex labels
    pub to_types: Vec<String>,
}

/// ALTER NODE TYPE statement.
///
/// Modifies an existing node type definition.
#[derive(Debug, Clone, Serialize)]
pub struct AlterNodeType {
    /// The name of the node type to alter
    pub name: String,
    /// The alteration to apply
    pub action: AlterTypeAction,
}

/// ALTER EDGE TYPE statement.
///
/// Modifies an existing edge type definition.
#[derive(Debug, Clone, Serialize)]
pub struct AlterEdgeType {
    /// The name of the edge type to alter
    pub name: String,
    /// The alteration to apply
    pub action: AlterTypeAction,
}

/// Actions that can be performed in an ALTER TYPE statement.
#[derive(Debug, Clone, Serialize)]
pub enum AlterTypeAction {
    /// Allow properties not defined in the schema
    AllowAdditionalProperties,
    /// Add a new property definition
    AddProperty(PropertyDefinition),
    /// Drop a property definition (by name)
    DropProperty(String),
}

/// DROP TYPE statement (for both node and edge types).
#[derive(Debug, Clone, Serialize)]
pub struct DropType {
    /// The name of the type to drop
    pub name: String,
}

/// SET SCHEMA VALIDATION statement.
///
/// Sets the validation mode for the graph schema.
#[derive(Debug, Clone, Serialize)]
pub struct SetValidation {
    /// The validation mode to set
    pub mode: ValidationModeAst,
}

/// Validation modes for schema enforcement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ValidationModeAst {
    /// No validation (schema is documentation only)
    None,
    /// Log warnings but allow invalid data
    Warn,
    /// Validate types with schemas, allow unknown types
    Strict,
    /// All types must have schemas defined
    Closed,
}

/// A property definition in a type declaration.
///
/// # Example
///
/// ```text
/// name STRING NOT NULL DEFAULT 'unknown'
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct PropertyDefinition {
    /// The property key name
    pub name: String,
    /// The expected value type
    pub prop_type: PropertyTypeAst,
    /// Whether this property is required (NOT NULL)
    pub required: bool,
    /// Default value if not provided (query-time application)
    pub default: Option<Literal>,
}

/// Property types in DDL statements.
///
/// Maps to `PropertyType` in the schema module at execution time.
#[derive(Debug, Clone, Serialize)]
pub enum PropertyTypeAst {
    /// STRING type
    String,
    /// INT type (i64)
    Int,
    /// FLOAT type (f64)
    Float,
    /// BOOL type
    Bool,
    /// LIST type with optional element type
    List(Option<Box<PropertyTypeAst>>),
    /// MAP type with optional value type
    Map(Option<Box<PropertyTypeAst>>),
    /// ANY type (accepts any value)
    Any,
}
```

---

## 5. Schema Module

Create a new schema module at `src/schema/mod.rs` that implements the runtime schema types.

### 5.1 Module Structure

```
src/schema/
├── mod.rs          # Module exports, GraphSchema, ValidationMode
├── types.rs        # VertexSchema, EdgeSchema, PropertyDef, PropertyType
├── builder.rs      # SchemaBuilder fluent API
├── validation.rs   # Validation functions
└── error.rs        # SchemaError types
```

### 5.2 Core Types

```rust
// src/schema/mod.rs

use std::collections::HashMap;
use crate::value::Value;

pub mod types;
pub mod builder;
pub mod validation;
pub mod error;

pub use types::*;
pub use builder::*;
pub use validation::*;
pub use error::*;

/// Complete schema for a graph.
///
/// Contains type definitions for vertices and edges, along with
/// the validation mode that determines how strictly the schema is enforced.
#[derive(Clone, Debug, Default)]
pub struct GraphSchema {
    /// Vertex schemas keyed by label
    pub vertex_schemas: HashMap<String, VertexSchema>,
    
    /// Edge schemas keyed by label  
    pub edge_schemas: HashMap<String, EdgeSchema>,
    
    /// Validation mode
    pub mode: ValidationMode,
}

impl GraphSchema {
    /// Create a new empty schema with default validation mode (None).
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Create a new schema with the specified validation mode.
    pub fn with_mode(mode: ValidationMode) -> Self {
        Self {
            mode,
            ..Default::default()
        }
    }
    
    /// Get all defined vertex labels.
    pub fn vertex_labels(&self) -> impl Iterator<Item = &str> {
        self.vertex_schemas.keys().map(|s| s.as_str())
    }
    
    /// Get all defined edge labels.
    pub fn edge_labels(&self) -> impl Iterator<Item = &str> {
        self.edge_schemas.keys().map(|s| s.as_str())
    }
    
    /// Get schema for a vertex label.
    pub fn vertex_schema(&self, label: &str) -> Option<&VertexSchema> {
        self.vertex_schemas.get(label)
    }
    
    /// Get schema for an edge label.
    pub fn edge_schema(&self, label: &str) -> Option<&EdgeSchema> {
        self.edge_schemas.get(label)
    }
    
    /// Check if a vertex label has a schema defined.
    pub fn has_vertex_schema(&self, label: &str) -> bool {
        self.vertex_schemas.contains_key(label)
    }
    
    /// Check if an edge label has a schema defined.
    pub fn has_edge_schema(&self, label: &str) -> bool {
        self.edge_schemas.contains_key(label)
    }
    
    /// Get all edge labels that can connect from a vertex label.
    pub fn edges_from(&self, vertex_label: &str) -> Vec<&str> {
        self.edge_schemas
            .iter()
            .filter(|(_, schema)| {
                schema.from_labels.is_empty() || 
                schema.from_labels.iter().any(|l| l == vertex_label)
            })
            .map(|(label, _)| label.as_str())
            .collect()
    }
    
    /// Get all edge labels that can connect to a vertex label.
    pub fn edges_to(&self, vertex_label: &str) -> Vec<&str> {
        self.edge_schemas
            .iter()
            .filter(|(_, schema)| {
                schema.to_labels.is_empty() ||
                schema.to_labels.iter().any(|l| l == vertex_label)
            })
            .map(|(label, _)| label.as_str())
            .collect()
    }
}

/// How strictly to enforce schema validation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ValidationMode {
    /// No validation (schema is documentation only)
    #[default]
    None,
    
    /// Log warnings but allow invalid data
    Warn,
    
    /// Validate labels with schemas, allow unknown labels
    Strict,
    
    /// Require all labels to have schemas defined
    Closed,
}
```

### 5.3 Type Definitions

```rust
// src/schema/types.rs

use std::collections::HashMap;
use crate::value::Value;

/// Schema for vertices with a specific label.
#[derive(Clone, Debug)]
pub struct VertexSchema {
    /// The vertex label this schema applies to
    pub label: String,
    
    /// Property definitions
    pub properties: HashMap<String, PropertyDef>,
    
    /// Allow properties not defined in schema?
    pub additional_properties: bool,
}

impl VertexSchema {
    /// Get all required property keys.
    pub fn required_properties(&self) -> impl Iterator<Item = &str> {
        self.properties
            .iter()
            .filter(|(_, def)| def.required)
            .map(|(key, _)| key.as_str())
    }
    
    /// Get all optional property keys.
    pub fn optional_properties(&self) -> impl Iterator<Item = &str> {
        self.properties
            .iter()
            .filter(|(_, def)| !def.required)
            .map(|(key, _)| key.as_str())
    }
    
    /// Get type for a property.
    pub fn property_type(&self, key: &str) -> Option<&PropertyType> {
        self.properties.get(key).map(|def| &def.value_type)
    }
    
    /// Get default value for a property.
    pub fn property_default(&self, key: &str) -> Option<&Value> {
        self.properties.get(key).and_then(|def| def.default.as_ref())
    }
}

/// Schema for edges with a specific label.
#[derive(Clone, Debug)]
pub struct EdgeSchema {
    /// The edge label this schema applies to
    pub label: String,
    
    /// Allowed source vertex labels (required, cannot be empty)
    pub from_labels: Vec<String>,
    
    /// Allowed target vertex labels (required, cannot be empty)
    pub to_labels: Vec<String>,
    
    /// Property definitions
    pub properties: HashMap<String, PropertyDef>,
    
    /// Allow properties not defined in schema?
    pub additional_properties: bool,
}

impl EdgeSchema {
    /// Check if a source label is allowed.
    pub fn allows_from(&self, label: &str) -> bool {
        self.from_labels.iter().any(|l| l == label)
    }
    
    /// Check if a target label is allowed.
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
        self.properties.get(key).and_then(|def| def.default.as_ref())
    }
}

/// Definition of a single property.
#[derive(Clone, Debug)]
pub struct PropertyDef {
    /// Property key name
    pub key: String,
    
    /// Expected value type
    pub value_type: PropertyType,
    
    /// Is this property required?
    pub required: bool,
    
    /// Default value if not provided (applied at query time)
    pub default: Option<Value>,
}

/// Property types (maps to Value variants).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PropertyType {
    /// Any type allowed
    Any,
    /// Value::Bool
    Bool,
    /// Value::Int
    Int,
    /// Value::Float  
    Float,
    /// Value::String
    String,
    /// Value::List with optional element type
    List(Option<Box<PropertyType>>),
    /// Value::Map with optional value type
    Map(Option<Box<PropertyType>>),
}

impl PropertyType {
    /// Check if a Value matches this type.
    pub fn matches(&self, value: &Value) -> bool {
        match (self, value) {
            // Any matches everything except Null (unless optional)
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
```

### 5.4 Schema Builder

```rust
// src/schema/builder.rs

use std::collections::HashMap;
use crate::value::Value;
use super::{GraphSchema, ValidationMode, VertexSchema, EdgeSchema, PropertyDef, PropertyType};

/// Builder for constructing schemas fluently.
pub struct SchemaBuilder {
    schema: GraphSchema,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self {
            schema: GraphSchema::default(),
        }
    }
    
    /// Set validation mode.
    pub fn mode(mut self, mode: ValidationMode) -> Self {
        self.schema.mode = mode;
        self
    }
    
    /// Add a vertex schema.
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
    
    /// Add an edge schema.
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
    
    /// Build the final schema.
    pub fn build(self) -> GraphSchema {
        self.schema
    }
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for vertex schemas.
pub struct VertexSchemaBuilder {
    parent: SchemaBuilder,
    schema: VertexSchema,
}

impl VertexSchemaBuilder {
    /// Add a required property.
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
    
    /// Add an optional property.
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
    
    /// Add an optional property with default value.
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
    
    /// Allow properties not defined in schema.
    pub fn allow_additional(mut self) -> Self {
        self.schema.additional_properties = true;
        self
    }
    
    /// Finish vertex schema, return to parent builder.
    pub fn done(mut self) -> SchemaBuilder {
        self.parent
            .schema
            .vertex_schemas
            .insert(self.schema.label.clone(), self.schema);
        self.parent
    }
}

/// Builder for edge schemas.
pub struct EdgeSchemaBuilder {
    parent: SchemaBuilder,
    schema: EdgeSchema,
}

impl EdgeSchemaBuilder {
    /// Set allowed source vertex labels (required).
    pub fn from(mut self, labels: &[&str]) -> Self {
        self.schema.from_labels = labels.iter().map(|s| s.to_string()).collect();
        self
    }
    
    /// Set allowed target vertex labels (required).
    pub fn to(mut self, labels: &[&str]) -> Self {
        self.schema.to_labels = labels.iter().map(|s| s.to_string()).collect();
        self
    }
    
    /// Add a required property.
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
    
    /// Add an optional property.
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
    
    /// Add an optional property with default value.
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
    
    /// Allow properties not defined in schema.
    pub fn allow_additional(mut self) -> Self {
        self.schema.additional_properties = true;
        self
    }
    
    /// Finish edge schema, return to parent builder.
    pub fn done(mut self) -> SchemaBuilder {
        self.parent
            .schema
            .edge_schemas
            .insert(self.schema.label.clone(), self.schema);
        self.parent
    }
}
```

### 5.5 Schema Errors

```rust
// src/schema/error.rs

use thiserror::Error;
use super::PropertyType;

/// Errors that can occur during schema validation.
#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("unknown vertex label: {label}")]
    UnknownVertexLabel { label: String },
    
    #[error("unknown edge label: {label}")]
    UnknownEdgeLabel { label: String },
    
    #[error("missing required property '{property}' on {element_type} '{label}'")]
    MissingRequired {
        element_type: &'static str,  // "vertex" or "edge"
        label: String,
        property: String,
    },
    
    #[error("type mismatch for property '{property}': expected {expected}, got {actual}")]
    TypeMismatch {
        property: String,
        expected: PropertyType,
        actual: String,  // Value variant name
    },
    
    #[error("unexpected property '{property}' on {element_type} '{label}'")]
    UnexpectedProperty {
        element_type: &'static str,
        label: String,
        property: String,
    },
    
    #[error("invalid edge endpoint: '{edge_label}' cannot connect from '{from_label}' (allowed: {allowed:?})")]
    InvalidSourceLabel {
        edge_label: String,
        from_label: String,
        allowed: Vec<String>,
    },
    
    #[error("invalid edge endpoint: '{edge_label}' cannot connect to '{to_label}' (allowed: {allowed:?})")]
    InvalidTargetLabel {
        edge_label: String,
        to_label: String,
        allowed: Vec<String>,
    },
    
    #[error("null value for required property '{property}' on {element_type} '{label}'")]
    NullRequired {
        element_type: &'static str,
        label: String,
        property: String,
    },
    
    #[error("type '{name}' already exists")]
    TypeAlreadyExists { name: String },
    
    #[error("type '{name}' not found")]
    TypeNotFound { name: String },
    
    #[error("property '{property}' already exists on type '{type_name}'")]
    PropertyAlreadyExists { type_name: String, property: String },
    
    #[error("property '{property}' not found on type '{type_name}'")]
    PropertyNotFound { type_name: String, property: String },
    
    #[error("edge type must specify FROM and TO endpoint constraints")]
    MissingEndpointConstraints,
}

/// Result type for schema operations.
pub type SchemaResult<T> = Result<T, SchemaError>;
```

---

## 6. DDL Execution

### 6.1 DDL Executor

Create `src/gql/ddl.rs` to execute DDL statements and modify the schema:

```rust
// src/gql/ddl.rs

use crate::gql::ast::{
    DdlStatement, CreateNodeType, CreateEdgeType, AlterNodeType, AlterEdgeType,
    DropType, SetValidation, AlterTypeAction, PropertyDefinition, PropertyTypeAst,
    ValidationModeAst,
};
use crate::schema::{
    GraphSchema, ValidationMode, VertexSchema, EdgeSchema, PropertyDef, PropertyType,
    SchemaError, SchemaResult,
};
use crate::value::Value;
use std::collections::HashMap;

/// Execute a DDL statement, modifying the schema in place.
pub fn execute_ddl(schema: &mut GraphSchema, stmt: &DdlStatement) -> SchemaResult<()> {
    match stmt {
        DdlStatement::CreateNodeType(create) => execute_create_node_type(schema, create),
        DdlStatement::CreateEdgeType(create) => execute_create_edge_type(schema, create),
        DdlStatement::AlterNodeType(alter) => execute_alter_node_type(schema, alter),
        DdlStatement::AlterEdgeType(alter) => execute_alter_edge_type(schema, alter),
        DdlStatement::DropNodeType(drop) => execute_drop_node_type(schema, drop),
        DdlStatement::DropEdgeType(drop) => execute_drop_edge_type(schema, drop),
        DdlStatement::SetValidation(set) => execute_set_validation(schema, set),
    }
}

fn execute_create_node_type(schema: &mut GraphSchema, stmt: &CreateNodeType) -> SchemaResult<()> {
    if schema.vertex_schemas.contains_key(&stmt.name) {
        return Err(SchemaError::TypeAlreadyExists { name: stmt.name.clone() });
    }
    
    let vertex_schema = VertexSchema {
        label: stmt.name.clone(),
        properties: convert_properties(&stmt.properties),
        additional_properties: false,
    };
    
    schema.vertex_schemas.insert(stmt.name.clone(), vertex_schema);
    Ok(())
}

fn execute_create_edge_type(schema: &mut GraphSchema, stmt: &CreateEdgeType) -> SchemaResult<()> {
    if schema.edge_schemas.contains_key(&stmt.name) {
        return Err(SchemaError::TypeAlreadyExists { name: stmt.name.clone() });
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
    let vertex_schema = schema.vertex_schemas.get_mut(&stmt.name)
        .ok_or_else(|| SchemaError::TypeNotFound { name: stmt.name.clone() })?;
    
    apply_alter_action(vertex_schema, &stmt.name, &stmt.action)
}

fn execute_alter_edge_type(schema: &mut GraphSchema, stmt: &AlterEdgeType) -> SchemaResult<()> {
    let edge_schema = schema.edge_schemas.get_mut(&stmt.name)
        .ok_or_else(|| SchemaError::TypeNotFound { name: stmt.name.clone() })?;
    
    match &stmt.action {
        AlterTypeAction::AllowAdditionalProperties => {
            edge_schema.additional_properties = true;
            Ok(())
        }
        AlterTypeAction::AddProperty(prop) => {
            if edge_schema.properties.contains_key(&prop.name) {
                return Err(SchemaError::PropertyAlreadyExists {
                    type_name: stmt.name.clone(),
                    property: prop.name.clone(),
                });
            }
            edge_schema.properties.insert(prop.name.clone(), convert_property(prop));
            Ok(())
        }
        AlterTypeAction::DropProperty(prop_name) => {
            if edge_schema.properties.remove(prop_name).is_none() {
                return Err(SchemaError::PropertyNotFound {
                    type_name: stmt.name.clone(),
                    property: prop_name.clone(),
                });
            }
            Ok(())
        }
    }
}

fn apply_alter_action(
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
            vertex_schema.properties.insert(prop.name.clone(), convert_property(prop));
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

fn execute_drop_node_type(schema: &mut GraphSchema, stmt: &DropType) -> SchemaResult<()> {
    if schema.vertex_schemas.remove(&stmt.name).is_none() {
        return Err(SchemaError::TypeNotFound { name: stmt.name.clone() });
    }
    Ok(())
}

fn execute_drop_edge_type(schema: &mut GraphSchema, stmt: &DropType) -> SchemaResult<()> {
    if schema.edge_schemas.remove(&stmt.name).is_none() {
        return Err(SchemaError::TypeNotFound { name: stmt.name.clone() });
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
    props.iter()
        .map(|p| (p.name.clone(), convert_property(p)))
        .collect()
}

fn convert_property(prop: &PropertyDefinition) -> PropertyDef {
    PropertyDef {
        key: prop.name.clone(),
        value_type: convert_property_type(&prop.prop_type),
        required: prop.required,
        default: prop.default.as_ref().map(|lit| literal_to_value(lit)),
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

fn literal_to_value(lit: &crate::gql::ast::Literal) -> Value {
    lit.clone().into()
}
```

---

## 8. Persistence

This section describes how schemas are persisted in the mmap backend for durable storage.

### 8.1 Overview

Schema persistence enables:
- **Durability**: Schema survives database restarts
- **Consistency**: Schema is always available when database opens
- **Atomicity**: Schema changes are part of the WAL transaction system

### 8.2 File Layout

The schema is stored in a dedicated region within the mmap file:

```
┌─────────────────────────────────────────────────────────────────────┐
│ File Layout                                                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  [Header (80 bytes)]                                                 │
│  [Node Table]                                                        │
│  [Edge Table]                                                        │
│  [Property Arena]                                                    │
│  [Schema Region]  ← NEW                                              │
│  [String Table]                                                      │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 8.3 Header Changes

Add new fields to `FileHeader` in `src/storage/mmap/records.rs`:

```rust
#[repr(C, packed)]
pub struct FileHeader {
    // ... existing fields ...
    
    /// Offset to schema region (0 = no schema)
    pub schema_offset: u64,
    
    /// Size of schema data in bytes
    pub schema_size: u64,
    
    /// Schema version for compatibility checking
    pub schema_version: u32,
    
    /// Reserved for future use
    pub _schema_reserved: [u8; 12],
}
```

Update `HEADER_SIZE` to accommodate new fields (add 32 bytes).

### 8.4 Schema Serialization Format

The schema is serialized using a compact binary format:

```
Schema Binary Format:
┌──────────────────────────────────────────────────────────────────┐
│ Schema Header (16 bytes)                                          │
│ ├── magic: u32 = 0x53434845 ("SCHE")                             │
│ ├── version: u32 = 1                                              │
│ ├── validation_mode: u8                                           │
│ ├── vertex_schema_count: u16                                      │
│ ├── edge_schema_count: u16                                        │
│ └── reserved: [u8; 3]                                             │
├──────────────────────────────────────────────────────────────────┤
│ Vertex Schemas (variable length)                                  │
│ ├── For each vertex schema:                                       │
│ │   ├── label_len: u16                                           │
│ │   ├── label: [u8; label_len]                                   │
│ │   ├── additional_properties: u8 (bool)                         │
│ │   ├── property_count: u16                                      │
│ │   └── properties: [PropertyDef; property_count]                │
├──────────────────────────────────────────────────────────────────┤
│ Edge Schemas (variable length)                                    │
│ ├── For each edge schema:                                         │
│ │   ├── label_len: u16                                           │
│ │   ├── label: [u8; label_len]                                   │
│ │   ├── additional_properties: u8 (bool)                         │
│ │   ├── from_count: u16                                          │
│ │   ├── from_labels: [LabelRef; from_count]                      │
│ │   ├── to_count: u16                                            │
│ │   ├── to_labels: [LabelRef; to_count]                          │
│ │   ├── property_count: u16                                      │
│ │   └── properties: [PropertyDef; property_count]                │
└──────────────────────────────────────────────────────────────────┘

PropertyDef Format:
├── key_len: u16
├── key: [u8; key_len]
├── value_type: u8 (PropertyType discriminant)
├── type_param: Option<u8> (for List/Map element types)
├── required: u8 (bool)
├── has_default: u8 (bool)
└── default_value: [u8; variable] (if has_default, serialized Value)

LabelRef Format:
├── len: u16
└── label: [u8; len]
```

### 8.5 Serialization Module

Create `src/schema/serialize.rs`:

```rust
// src/schema/serialize.rs

use std::io::{Read, Write, Cursor};
use crate::value::Value;
use super::{GraphSchema, ValidationMode, VertexSchema, EdgeSchema, PropertyDef, PropertyType};

const SCHEMA_MAGIC: u32 = 0x53434845; // "SCHE"
const SCHEMA_FORMAT_VERSION: u32 = 1;

/// Serialize a GraphSchema to bytes.
pub fn serialize_schema(schema: &GraphSchema) -> Vec<u8> {
    let mut buf = Vec::new();
    
    // Header
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

/// Deserialize a GraphSchema from bytes.
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
    cursor.set_position(cursor.position() + 3); // skip reserved
    
    // Read vertex schemas
    let mut vertex_schemas = std::collections::HashMap::new();
    for _ in 0..vertex_count {
        let vs = deserialize_vertex_schema(&mut cursor)?;
        vertex_schemas.insert(vs.label.clone(), vs);
    }
    
    // Read edge schemas
    let mut edge_schemas = std::collections::HashMap::new();
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

#[derive(Debug, thiserror::Error)]
pub enum SchemaSerializeError {
    #[error("invalid schema magic number")]
    InvalidMagic,
    #[error("unsupported schema version: {0}")]
    UnsupportedVersion(u32),
    #[error("unexpected end of data")]
    UnexpectedEof,
    #[error("invalid validation mode: {0}")]
    InvalidValidationMode(u8),
    #[error("invalid property type: {0}")]
    InvalidPropertyType(u8),
    #[error("invalid UTF-8 string")]
    InvalidUtf8,
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

// Helper functions for reading/writing primitives
fn read_u8(cursor: &mut Cursor<&[u8]>) -> Result<u8, SchemaSerializeError> { /* ... */ }
fn read_u16(cursor: &mut Cursor<&[u8]>) -> Result<u16, SchemaSerializeError> { /* ... */ }
fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32, SchemaSerializeError> { /* ... */ }
fn read_string(cursor: &mut Cursor<&[u8]>) -> Result<String, SchemaSerializeError> { /* ... */ }

fn validation_mode_to_u8(mode: ValidationMode) -> u8 {
    match mode {
        ValidationMode::None => 0,
        ValidationMode::Warn => 1,
        ValidationMode::Strict => 2,
        ValidationMode::Closed => 3,
    }
}

fn u8_to_validation_mode(v: u8) -> Result<ValidationMode, SchemaSerializeError> {
    match v {
        0 => Ok(ValidationMode::None),
        1 => Ok(ValidationMode::Warn),
        2 => Ok(ValidationMode::Strict),
        3 => Ok(ValidationMode::Closed),
        _ => Err(SchemaSerializeError::InvalidValidationMode(v)),
    }
}

// Additional serialization helpers for vertex/edge schemas, properties, etc.
fn serialize_vertex_schema(buf: &mut Vec<u8>, vs: &VertexSchema) { /* ... */ }
fn serialize_edge_schema(buf: &mut Vec<u8>, es: &EdgeSchema) { /* ... */ }
fn serialize_property_def(buf: &mut Vec<u8>, prop: &PropertyDef) { /* ... */ }
fn serialize_property_type(buf: &mut Vec<u8>, pt: &PropertyType) { /* ... */ }

fn deserialize_vertex_schema(cursor: &mut Cursor<&[u8]>) -> Result<VertexSchema, SchemaSerializeError> { /* ... */ }
fn deserialize_edge_schema(cursor: &mut Cursor<&[u8]>) -> Result<EdgeSchema, SchemaSerializeError> { /* ... */ }
fn deserialize_property_def(cursor: &mut Cursor<&[u8]>) -> Result<PropertyDef, SchemaSerializeError> { /* ... */ }
fn deserialize_property_type(cursor: &mut Cursor<&[u8]>) -> Result<PropertyType, SchemaSerializeError> { /* ... */ }
```

### 8.6 MmapGraph Integration

Add schema persistence methods to `MmapGraph`:

```rust
// In src/storage/mmap/mod.rs

impl MmapGraph {
    /// Load schema from the database file.
    ///
    /// Called automatically during `open()` if schema data exists.
    pub fn load_schema(&self) -> Result<Option<GraphSchema>, StorageError> {
        let header = self.get_header();
        
        if header.schema_offset == 0 || header.schema_size == 0 {
            return Ok(None);
        }
        
        let mmap = self.mmap.read();
        let start = header.schema_offset as usize;
        let end = start + header.schema_size as usize;
        
        if end > mmap.len() {
            return Err(StorageError::CorruptedData);
        }
        
        let schema_data = &mmap[start..end];
        let schema = deserialize_schema(schema_data)
            .map_err(|_| StorageError::CorruptedData)?;
        
        Ok(Some(schema))
    }
    
    /// Persist schema to the database file.
    ///
    /// This allocates space in the schema region and writes the serialized
    /// schema data. The operation is logged to WAL for durability.
    pub fn save_schema(&self, schema: &GraphSchema) -> Result<(), StorageError> {
        let data = serialize_schema(schema);
        
        // Allocate space in schema region (may need to grow file)
        let offset = self.allocate_schema_space(data.len())?;
        
        // Write schema data
        self.write_schema_data(offset, &data)?;
        
        // Update header
        self.update_schema_header(offset, data.len())?;
        
        // Log to WAL for durability
        if !self.is_batch_mode() {
            let mut wal = self.wal.write();
            wal.log(WalEntry::SchemaUpdate {
                offset,
                data: data.clone(),
            })?;
            wal.sync()?;
        }
        
        Ok(())
    }
    
    /// Clear the schema from the database.
    pub fn clear_schema(&self) -> Result<(), StorageError> {
        self.update_schema_header(0, 0)?;
        Ok(())
    }
    
    // Private helpers
    fn allocate_schema_space(&self, size: usize) -> Result<u64, StorageError> { /* ... */ }
    fn write_schema_data(&self, offset: u64, data: &[u8]) -> Result<(), StorageError> { /* ... */ }
    fn update_schema_header(&self, offset: u64, size: usize) -> Result<(), StorageError> { /* ... */ }
}
```

### 8.7 WAL Entry for Schema

Add a new WAL entry type for schema updates:

```rust
// In src/storage/mmap/wal.rs

#[derive(Clone, Debug)]
pub enum WalEntry {
    // ... existing entries ...
    
    /// Schema update entry
    SchemaUpdate {
        /// Offset where schema data was written
        offset: u64,
        /// Serialized schema data
        data: Vec<u8>,
    },
}
```

### 8.8 Schema Recovery

During crash recovery, schema updates are replayed:

```rust
// In src/storage/mmap/recovery.rs

fn replay_entry(entry: &WalEntry, file: &File, header: &FileHeader) -> Result<(), StorageError> {
    match entry {
        // ... existing entries ...
        
        WalEntry::SchemaUpdate { offset, data } => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                file.write_all_at(data, *offset)?;
            }
            Ok(())
        }
    }
}
```

### 8.9 In-Memory Backend

For `InMemoryGraph`, schema is stored as a field on the graph struct:

```rust
// In src/storage/inmemory.rs

pub struct InMemoryGraph {
    // ... existing fields ...
    
    /// Optional schema for validation
    schema: Option<GraphSchema>,
}

impl InMemoryGraph {
    pub fn schema(&self) -> Option<&GraphSchema> {
        self.schema.as_ref()
    }
    
    pub fn set_schema(&mut self, schema: GraphSchema) {
        self.schema = Some(schema);
    }
    
    pub fn clear_schema(&mut self) {
        self.schema = None;
    }
}
```

---

## 7. Mutation Validation

### 7.1 Integration with Mutation Execution

Modify `src/gql/mutation.rs` to integrate schema validation:

```rust
// Add to MutationContext:
pub struct MutationContext<'a> {
    // ... existing fields ...
    
    /// Optional schema for validation
    pub schema: Option<&'a GraphSchema>,
}

// Add validation calls in execute_mutation:
impl<'a> MutationContext<'a> {
    /// Execute a mutation query with optional schema validation.
    pub fn execute_mutation(
        &mut self,
        mutation: &MutationQuery,
        schema: Option<&GraphSchema>,
    ) -> Result<Vec<Value>, MutationError> {
        self.schema = schema;
        
        // ... existing code for pattern matching ...
        
        for clause in &mutation.mutations {
            match clause {
                MutationClause::Create(create) => {
                    self.execute_create_with_validation(create)?;
                }
                MutationClause::Set(set) => {
                    self.execute_set_with_validation(set)?;
                }
                // ... other clauses ...
            }
        }
        
        // ... existing code ...
    }
    
    fn execute_create_with_validation(&mut self, create: &CreateClause) -> Result<(), MutationError> {
        // For each pattern in create.patterns:
        // 1. Extract label and properties
        // 2. If schema exists and has vertex/edge schema for this label:
        //    - Call validate_vertex/validate_edge
        // 3. If schema exists and mode is CLOSED and no schema for label:
        //    - Return error
        // 4. Create the vertex/edge
        
        // ... implementation ...
    }
    
    fn execute_set_with_validation(&mut self, set: &SetClause) -> Result<(), MutationError> {
        // For each set item:
        // 1. Get the element being modified
        // 2. If schema exists and has schema for element's label:
        //    - Validate the property type
        // 3. Set the property
        
        // ... implementation ...
    }
}
```

### 7.2 Validation Functions

```rust
// src/schema/validation.rs

use std::collections::HashMap;
use crate::value::Value;
use super::{GraphSchema, ValidationMode, VertexSchema, EdgeSchema, PropertyType, SchemaError, SchemaResult};

/// Validation result that can be an error or a warning.
#[derive(Debug)]
pub enum ValidationResult {
    Ok,
    Warning(SchemaError),
    Error(SchemaError),
}

/// Validate a vertex against the schema.
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
                    results.push(ValidationResult::Warning(
                        SchemaError::UnknownVertexLabel { label: label.to_string() }
                    ));
                    return Ok(results);
                }
                ValidationMode::Strict => return Ok(results), // Unknown labels allowed
                ValidationMode::Closed => {
                    return Err(SchemaError::UnknownVertexLabel { label: label.to_string() });
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
                    results.push(ValidationResult::Warning(
                        SchemaError::UnknownEdgeLabel { label: label.to_string() }
                    ));
                    return Ok(results);
                }
                ValidationMode::Strict => return Ok(results),
                ValidationMode::Closed => {
                    return Err(SchemaError::UnknownEdgeLabel { label: label.to_string() });
                }
            }
        }
    };
    
    // Validate endpoints
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

fn validate_properties(
    results: &mut Vec<ValidationResult>,
    mode: ValidationMode,
    element_type: &'static str,
    label: &str,
    additional_allowed: bool,
    schema_props: &HashMap<String, super::PropertyDef>,
    actual_props: &HashMap<String, Value>,
) -> SchemaResult<()> {
    // Check for missing required properties
    for (key, def) in schema_props {
        if def.required && !actual_props.contains_key(key) {
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
    }
    
    // Check property types and unexpected properties
    for (key, value) in actual_props {
        match schema_props.get(key) {
            Some(def) => {
                // Check for null on required property
                if def.required && matches!(value, Value::Null) {
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
                
                // Check type (skip for null on optional properties)
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
            }
            None if !additional_allowed => {
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
            None => {} // Additional properties allowed
        }
    }
    
    Ok(())
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "NULL",
        Value::Bool(_) => "BOOL",
        Value::Int(_) => "INT",
        Value::Float(_) => "FLOAT",
        Value::String(_) => "STRING",
        Value::List(_) => "LIST",
        Value::Map(_) => "MAP",
    }
}

/// Apply default values to properties at query time.
///
/// Returns a new HashMap with defaults applied for missing optional properties.
pub fn apply_defaults(
    schema: &VertexSchema,
    properties: &HashMap<String, Value>,
) -> HashMap<String, Value> {
    let mut result = properties.clone();
    
    for (key, def) in &schema.properties {
        if !result.contains_key(key) {
            if let Some(default) = &def.default {
                result.insert(key.clone(), default.clone());
            }
        }
    }
    
    result
}
```

---

## 9. Implementation Plan

This section outlines a phased approach to implementing GQL DDL support.

### 9.1 Phase Overview

| Phase | Description | Duration | Dependencies |
|-------|-------------|----------|--------------|
| **Phase 1** | Schema Module (Rust API) | 3-4 days | None |
| **Phase 2** | Grammar & Parser | 2-3 days | Phase 1 |
| **Phase 3** | DDL Execution | 2-3 days | Phase 2 |
| **Phase 4** | Mutation Validation | 2-3 days | Phase 3 |
| **Phase 5** | Persistence | 2-3 days | Phase 4 |
| **Phase 6** | Integration & Polish | 2-3 days | Phase 5 |

**Total estimated time**: 13-19 days

### 9.2 Phase 1: Schema Module (Rust API)

Create the schema module with core types and builder API.

**Files to Create:**

| File | Description |
|------|-------------|
| `src/schema/mod.rs` | Module exports, `GraphSchema`, `ValidationMode` |
| `src/schema/types.rs` | `VertexSchema`, `EdgeSchema`, `PropertyDef`, `PropertyType` |
| `src/schema/builder.rs` | `SchemaBuilder`, `VertexSchemaBuilder`, `EdgeSchemaBuilder` |
| `src/schema/error.rs` | `SchemaError`, `SchemaResult` |
| `src/schema/validation.rs` | `validate_vertex`, `validate_edge`, `apply_defaults` |

**Files to Modify:**

| File | Changes |
|------|---------|
| `src/lib.rs` | Add `pub mod schema;` and re-exports |

**Deliverables:**
- [ ] `GraphSchema` struct with vertex/edge schema maps
- [ ] `ValidationMode` enum (None, Warn, Strict, Closed)
- [ ] `VertexSchema` and `EdgeSchema` types
- [ ] `PropertyDef` and `PropertyType` types
- [ ] `PropertyType::matches(&Value)` implementation
- [ ] Fluent builder API for constructing schemas
- [ ] `SchemaError` error types
- [ ] `validate_vertex` and `validate_edge` functions
- [ ] `apply_defaults` for query-time default application
- [ ] Unit tests for all types and validation logic

**Exit Criteria:**
```rust
// This code compiles and works:
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

let result = validate_vertex(&schema, "Person", &props);
```

### 9.3 Phase 2: Grammar & Parser

Extend the GQL grammar to support DDL statements.

**Files to Modify:**

| File | Changes |
|------|---------|
| `src/gql/grammar.pest` | Add DDL keywords and statement rules |
| `src/gql/ast.rs` | Add `DdlStatement` and related AST types |
| `src/gql/parser.rs` | Add DDL parsing logic |

**Grammar Additions:**
- [ ] DDL keywords: `TYPE`, `NODE`, `EDGE`, `FROM_KW`, `TO_KW`, `ALTER`, `DROP`, `ADD`, `ALLOW`, `ADDITIONAL`, `PROPERTIES`, `VALIDATION`, `SCHEMA`, `STRICT`, `CLOSED`, `WARN_KW`, `DEFAULT`
- [ ] Property type keywords: `STRING_TYPE`, `INT_TYPE`, `FLOAT_TYPE`, `BOOL_TYPE`, `LIST_TYPE`, `MAP_TYPE`, `ANY_TYPE`
- [ ] `ddl_statement` rule with all DDL variants
- [ ] `create_node_type` and `create_edge_type` rules
- [ ] `property_def` and `property_type` rules
- [ ] `alter_node_type` and `alter_edge_type` rules
- [ ] `drop_node_type` and `drop_edge_type` rules
- [ ] `set_schema_validation` rule

**AST Additions:**
- [ ] `Statement::Ddl(Box<DdlStatement>)` variant
- [ ] `DdlStatement` enum
- [ ] `CreateNodeType`, `CreateEdgeType` structs
- [ ] `AlterNodeType`, `AlterEdgeType` structs
- [ ] `AlterTypeAction` enum
- [ ] `DropType`, `SetValidation` structs
- [ ] `PropertyDefinition`, `PropertyTypeAst` types
- [ ] `ValidationModeAst` enum

**Deliverables:**
- [ ] Grammar parses all DDL statement types
- [ ] Parser builds correct AST for DDL statements
- [ ] Snapshot tests for DDL parsing
- [ ] Error messages for DDL syntax errors

**Exit Criteria:**
```rust
// These parse successfully:
parse("CREATE NODE TYPE Person (name STRING NOT NULL, age INT)")?;
parse("CREATE EDGE TYPE KNOWS (since INT) FROM Person TO Person")?;
parse("ALTER NODE TYPE Person ADD email STRING")?;
parse("DROP NODE TYPE Person")?;
parse("SET SCHEMA VALIDATION STRICT")?;
```

### 9.4 Phase 3: DDL Execution

Implement DDL statement execution to modify the schema.

**Files to Create:**

| File | Description |
|------|-------------|
| `src/gql/ddl.rs` | DDL execution functions |

**Files to Modify:**

| File | Changes |
|------|---------|
| `src/gql/mod.rs` | Add `pub mod ddl;` |
| `src/gql/compiler.rs` | Route DDL statements to executor |

**Deliverables:**
- [ ] `execute_ddl(schema: &mut GraphSchema, stmt: &DdlStatement)` function
- [ ] `execute_create_node_type` implementation
- [ ] `execute_create_edge_type` implementation
- [ ] `execute_alter_node_type` implementation
- [ ] `execute_alter_edge_type` implementation
- [ ] `execute_drop_node_type` implementation
- [ ] `execute_drop_edge_type` implementation
- [ ] `execute_set_validation` implementation
- [ ] AST-to-schema conversion functions
- [ ] Error handling for type conflicts, missing types, etc.

**Exit Criteria:**
```rust
let mut schema = GraphSchema::new();
let stmt = parse("CREATE NODE TYPE Person (name STRING NOT NULL)")?;
execute_ddl(&mut schema, &stmt.as_ddl())?;
assert!(schema.vertex_schemas.contains_key("Person"));
```

### 9.5 Phase 4: Mutation Validation

Integrate schema validation with GQL mutations.

**Files to Modify:**

| File | Changes |
|------|---------|
| `src/gql/mutation.rs` | Add schema field to context, validation calls |
| `src/gql/compiler.rs` | Pass schema to mutation execution |

**Deliverables:**
- [ ] `MutationContext.schema` field
- [ ] Validation in `execute_create` for vertices and edges
- [ ] Validation in `execute_set` for property updates
- [ ] Validation in `execute_merge` for upserts
- [ ] Error propagation from validation to mutation errors
- [ ] Warning collection for `ValidationMode::Warn`

**Exit Criteria:**
```rust
// With strict schema requiring "name" property:
// This succeeds:
execute("CREATE (p:Person {name: 'Alice'})", Some(&schema))?;

// This fails:
let err = execute("CREATE (p:Person {age: 30})", Some(&schema)).unwrap_err();
assert!(matches!(err, MutationError::Schema(SchemaError::MissingRequired { .. })));
```

### 9.6 Phase 5: Persistence

Implement schema persistence for the mmap backend.

**Files to Create:**

| File | Description |
|------|-------------|
| `src/schema/serialize.rs` | Schema serialization/deserialization |

**Files to Modify:**

| File | Changes |
|------|---------|
| `src/schema/mod.rs` | Add `pub mod serialize;` |
| `src/storage/mmap/records.rs` | Add schema fields to `FileHeader` |
| `src/storage/mmap/mod.rs` | Add `load_schema`, `save_schema` methods |
| `src/storage/mmap/wal.rs` | Add `SchemaUpdate` entry type |
| `src/storage/mmap/recovery.rs` | Handle schema recovery |
| `src/storage/inmemory.rs` | Add schema field |

**Deliverables:**
- [ ] `serialize_schema` and `deserialize_schema` functions
- [ ] Binary serialization format for schema
- [ ] `FileHeader` schema offset/size fields
- [ ] `MmapGraph::load_schema` method
- [ ] `MmapGraph::save_schema` method
- [ ] WAL entry for schema updates
- [ ] Schema recovery during crash recovery
- [ ] `InMemoryGraph` schema field and methods

**Exit Criteria:**
```rust
// Schema persists across reopens:
{
    let graph = MmapGraph::open("test.db")?;
    let schema = SchemaBuilder::new()
        .vertex("Person").property("name", PropertyType::String).done()
        .build();
    graph.save_schema(&schema)?;
}
{
    let graph = MmapGraph::open("test.db")?;
    let schema = graph.load_schema()?.unwrap();
    assert!(schema.vertex_schemas.contains_key("Person"));
}
```

### 9.7 Phase 6: Integration & Polish

Final integration, documentation, and polish.

**Files to Modify:**

| File | Changes |
|------|---------|
| `src/graph.rs` | Add schema-aware graph methods |
| `src/lib.rs` | Public API exports |
| `examples/` | Add DDL example |

**Deliverables:**
- [ ] `Graph::with_schema` constructor
- [ ] `Graph::schema()` accessor
- [ ] `Graph::execute_ddl` method for DDL execution
- [ ] Public API documentation
- [ ] Example: `examples/gql_schema.rs`
- [ ] Update README with schema documentation
- [ ] Integration tests for full DDL → validation workflow
- [ ] Performance benchmarks for validation overhead

**Exit Criteria:**
- All tests pass
- Example runs successfully
- Documentation complete
- No clippy warnings

### 9.8 File Summary

**New Files (10):**
```
src/schema/
├── mod.rs           # Module exports, GraphSchema, ValidationMode
├── types.rs         # VertexSchema, EdgeSchema, PropertyDef, PropertyType
├── builder.rs       # SchemaBuilder fluent API
├── error.rs         # SchemaError types
├── validation.rs    # Validation functions
└── serialize.rs     # Binary serialization

src/gql/ddl.rs       # DDL execution

tests/schema.rs      # Schema unit tests
tests/ddl.rs         # DDL parsing and execution tests

examples/gql_schema.rs  # Usage example
```

**Modified Files (12):**
```
src/lib.rs                    # Add schema module export
src/graph.rs                  # Schema integration
src/gql/mod.rs                # Add ddl module
src/gql/grammar.pest          # DDL grammar rules
src/gql/ast.rs                # DDL AST types
src/gql/parser.rs             # DDL parsing
src/gql/compiler.rs           # Route DDL to executor
src/gql/mutation.rs           # Validation integration
src/storage/mmap/mod.rs       # Schema persistence
src/storage/mmap/records.rs   # Header changes
src/storage/mmap/wal.rs       # Schema WAL entry
src/storage/mmap/recovery.rs  # Schema recovery
src/storage/inmemory.rs       # Schema field
```

---

## 10. Testing Strategy

### 10.1 Test Categories

| Category | Purpose | Location |
|----------|---------|----------|
| Unit Tests | Test individual functions/types | `src/schema/*.rs` (inline) |
| Parser Snapshots | Verify DDL parsing | `tests/gql_snapshots.rs` |
| Integration Tests | End-to-end DDL workflows | `tests/schema.rs`, `tests/ddl.rs` |
| Validation Tests | Schema validation behavior | `tests/schema.rs` |
| Persistence Tests | Schema save/load | `tests/mmap.rs` |

### 10.2 Unit Tests

Located in each module with `#[cfg(test)]` blocks.

**Schema Types (`src/schema/types.rs`):**
```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn property_type_matches_bool() {
        assert!(PropertyType::Bool.matches(&Value::Bool(true)));
        assert!(!PropertyType::Bool.matches(&Value::Int(1)));
    }
    
    #[test]
    fn property_type_matches_list_with_element_type() {
        let list_of_ints = PropertyType::List(Some(Box::new(PropertyType::Int)));
        assert!(list_of_ints.matches(&Value::List(vec![Value::Int(1), Value::Int(2)])));
        assert!(!list_of_ints.matches(&Value::List(vec![Value::String("a".into())])));
    }
    
    #[test]
    fn property_type_any_matches_everything() {
        assert!(PropertyType::Any.matches(&Value::Bool(true)));
        assert!(PropertyType::Any.matches(&Value::Int(42)));
        assert!(PropertyType::Any.matches(&Value::String("hello".into())));
        assert!(PropertyType::Any.matches(&Value::Null));
    }
    
    #[test]
    fn property_type_display() {
        assert_eq!(format!("{}", PropertyType::String), "STRING");
        assert_eq!(format!("{}", PropertyType::List(None)), "LIST");
        assert_eq!(
            format!("{}", PropertyType::List(Some(Box::new(PropertyType::Int)))),
            "LIST<INT>"
        );
    }
}
```

**Schema Builder (`src/schema/builder.rs`):**
```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn build_empty_schema() {
        let schema = SchemaBuilder::new().build();
        assert!(schema.vertex_schemas.is_empty());
        assert!(schema.edge_schemas.is_empty());
        assert_eq!(schema.mode, ValidationMode::None);
    }
    
    #[test]
    fn build_schema_with_vertex() {
        let schema = SchemaBuilder::new()
            .vertex("Person")
                .property("name", PropertyType::String)
                .optional("age", PropertyType::Int)
                .done()
            .build();
        
        let vs = schema.vertex_schemas.get("Person").unwrap();
        assert!(vs.properties.get("name").unwrap().required);
        assert!(!vs.properties.get("age").unwrap().required);
    }
    
    #[test]
    fn build_schema_with_edge() {
        let schema = SchemaBuilder::new()
            .vertex("Person").done()
            .vertex("Company").done()
            .edge("WORKS_AT")
                .from(&["Person"])
                .to(&["Company"])
                .property("role", PropertyType::String)
                .done()
            .build();
        
        let es = schema.edge_schemas.get("WORKS_AT").unwrap();
        assert_eq!(es.from_labels, vec!["Person"]);
        assert_eq!(es.to_labels, vec!["Company"]);
    }
}
```

**Validation (`src/schema/validation.rs`):**
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    
    fn test_schema() -> GraphSchema {
        SchemaBuilder::new()
            .mode(ValidationMode::Strict)
            .vertex("Person")
                .property("name", PropertyType::String)
                .optional("age", PropertyType::Int)
                .done()
            .edge("KNOWS")
                .from(&["Person"])
                .to(&["Person"])
                .done()
            .build()
    }
    
    #[test]
    fn validate_vertex_success() {
        let schema = test_schema();
        let props = HashMap::from([("name".to_string(), Value::String("Alice".into()))]);
        let result = validate_vertex(&schema, "Person", &props);
        assert!(result.is_ok());
    }
    
    #[test]
    fn validate_vertex_missing_required() {
        let schema = test_schema();
        let props = HashMap::from([("age".to_string(), Value::Int(30))]);
        let result = validate_vertex(&schema, "Person", &props);
        assert!(matches!(result, Err(SchemaError::MissingRequired { .. })));
    }
    
    #[test]
    fn validate_vertex_type_mismatch() {
        let schema = test_schema();
        let props = HashMap::from([("name".to_string(), Value::Int(42))]);
        let result = validate_vertex(&schema, "Person", &props);
        assert!(matches!(result, Err(SchemaError::TypeMismatch { .. })));
    }
    
    #[test]
    fn validate_edge_invalid_source() {
        let schema = test_schema();
        let props = HashMap::new();
        let result = validate_edge(&schema, "KNOWS", "Company", "Person", &props);
        assert!(matches!(result, Err(SchemaError::InvalidSourceLabel { .. })));
    }
    
    #[test]
    fn validate_unknown_label_strict_mode() {
        let schema = test_schema();
        let props = HashMap::new();
        // In Strict mode, unknown labels are allowed
        let result = validate_vertex(&schema, "Unknown", &props);
        assert!(result.is_ok());
    }
    
    #[test]
    fn validate_unknown_label_closed_mode() {
        let mut schema = test_schema();
        schema.mode = ValidationMode::Closed;
        let props = HashMap::new();
        let result = validate_vertex(&schema, "Unknown", &props);
        assert!(matches!(result, Err(SchemaError::UnknownVertexLabel { .. })));
    }
}
```

### 10.3 Parser Snapshot Tests

Add DDL snapshot tests in `tests/gql_snapshots.rs`:

```rust
// tests/gql_snapshots.rs

#[test]
fn parse_create_node_type_snapshot() {
    let input = "CREATE NODE TYPE Person (name STRING NOT NULL, age INT, active BOOL DEFAULT true)";
    let stmt = parse(input).unwrap();
    insta::assert_debug_snapshot!(stmt);
}

#[test]
fn parse_create_edge_type_snapshot() {
    let input = "CREATE EDGE TYPE KNOWS (since INT, weight FLOAT DEFAULT 1.0) FROM Person TO Person";
    let stmt = parse(input).unwrap();
    insta::assert_debug_snapshot!(stmt);
}

#[test]
fn parse_create_edge_type_multiple_endpoints_snapshot() {
    let input = "CREATE EDGE TYPE TAGGED () FROM Post, Comment, Photo TO Tag";
    let stmt = parse(input).unwrap();
    insta::assert_debug_snapshot!(stmt);
}

#[test]
fn parse_alter_node_type_allow_additional_snapshot() {
    let input = "ALTER NODE TYPE Person ALLOW ADDITIONAL PROPERTIES";
    let stmt = parse(input).unwrap();
    insta::assert_debug_snapshot!(stmt);
}

#[test]
fn parse_alter_node_type_add_property_snapshot() {
    let input = "ALTER NODE TYPE Person ADD email STRING";
    let stmt = parse(input).unwrap();
    insta::assert_debug_snapshot!(stmt);
}

#[test]
fn parse_alter_node_type_drop_property_snapshot() {
    let input = "ALTER NODE TYPE Person DROP email";
    let stmt = parse(input).unwrap();
    insta::assert_debug_snapshot!(stmt);
}

#[test]
fn parse_drop_node_type_snapshot() {
    let input = "DROP NODE TYPE Person";
    let stmt = parse(input).unwrap();
    insta::assert_debug_snapshot!(stmt);
}

#[test]
fn parse_drop_edge_type_snapshot() {
    let input = "DROP EDGE TYPE KNOWS";
    let stmt = parse(input).unwrap();
    insta::assert_debug_snapshot!(stmt);
}

#[test]
fn parse_set_validation_strict_snapshot() {
    let input = "SET SCHEMA VALIDATION STRICT";
    let stmt = parse(input).unwrap();
    insta::assert_debug_snapshot!(stmt);
}

#[test]
fn parse_set_validation_closed_snapshot() {
    let input = "SET SCHEMA VALIDATION CLOSED";
    let stmt = parse(input).unwrap();
    insta::assert_debug_snapshot!(stmt);
}

#[test]
fn parse_list_type_snapshot() {
    let input = "CREATE NODE TYPE Product (tags LIST<STRING>, ids LIST<INT>)";
    let stmt = parse(input).unwrap();
    insta::assert_debug_snapshot!(stmt);
}

#[test]
fn parse_map_type_snapshot() {
    let input = "CREATE NODE TYPE Config (metadata MAP<ANY>, counts MAP<INT>)";
    let stmt = parse(input).unwrap();
    insta::assert_debug_snapshot!(stmt);
}

// Error snapshots
#[test]
fn error_missing_from_clause_snapshot() {
    let input = "CREATE EDGE TYPE KNOWS () TO Person";
    let err = parse(input).unwrap_err();
    insta::assert_debug_snapshot!(err);
}

#[test]
fn error_missing_property_type_snapshot() {
    let input = "CREATE NODE TYPE Person (name)";
    let err = parse(input).unwrap_err();
    insta::assert_debug_snapshot!(err);
}
```

### 10.4 Integration Tests

Create `tests/schema.rs` for end-to-end schema tests:

```rust
// tests/schema.rs

use intersteller::prelude::*;
use intersteller::schema::*;
use std::collections::HashMap;

#[test]
fn test_schema_builder_integration() {
    let schema = SchemaBuilder::new()
        .mode(ValidationMode::Strict)
        .vertex("Person")
            .property("name", PropertyType::String)
            .optional("age", PropertyType::Int)
            .optional_with_default("active", PropertyType::Bool, Value::Bool(true))
            .done()
        .vertex("Software")
            .property("name", PropertyType::String)
            .optional("lang", PropertyType::String)
            .done()
        .edge("KNOWS")
            .from(&["Person"])
            .to(&["Person"])
            .optional("since", PropertyType::Int)
            .done()
        .edge("CREATED")
            .from(&["Person"])
            .to(&["Software"])
            .optional("weight", PropertyType::Float)
            .done()
        .build();
    
    // Verify structure
    assert_eq!(schema.vertex_schemas.len(), 2);
    assert_eq!(schema.edge_schemas.len(), 2);
    
    // Verify Person schema
    let person = schema.vertex_schema("Person").unwrap();
    assert!(person.properties.get("name").unwrap().required);
    assert!(!person.properties.get("age").unwrap().required);
    
    // Verify KNOWS schema
    let knows = schema.edge_schema("KNOWS").unwrap();
    assert!(knows.allows_from("Person"));
    assert!(!knows.allows_from("Software"));
}

#[test]
fn test_ddl_create_and_validate() {
    let graph = Graph::in_memory();
    
    // Execute DDL to create schema
    graph.execute("CREATE NODE TYPE Person (name STRING NOT NULL, age INT)").unwrap();
    graph.execute("CREATE EDGE TYPE KNOWS () FROM Person TO Person").unwrap();
    graph.execute("SET SCHEMA VALIDATION STRICT").unwrap();
    
    // Valid mutation
    graph.execute("CREATE (p:Person {name: 'Alice', age: 30})").unwrap();
    
    // Invalid mutation - missing required property
    let err = graph.execute("CREATE (p:Person {age: 25})").unwrap_err();
    assert!(err.to_string().contains("missing required property"));
}

#[test]
fn test_validation_modes() {
    // Test NONE mode
    let mut schema = SchemaBuilder::new()
        .mode(ValidationMode::None)
        .vertex("Person").property("name", PropertyType::String).done()
        .build();
    
    let props = HashMap::new();  // Missing required "name"
    assert!(validate_vertex(&schema, "Person", &props).is_ok());
    
    // Test WARN mode
    schema.mode = ValidationMode::Warn;
    let result = validate_vertex(&schema, "Person", &props).unwrap();
    assert!(result.iter().any(|r| matches!(r, ValidationResult::Warning(_))));
    
    // Test STRICT mode
    schema.mode = ValidationMode::Strict;
    assert!(validate_vertex(&schema, "Person", &props).is_err());
    
    // Test CLOSED mode with unknown label
    schema.mode = ValidationMode::Closed;
    assert!(validate_vertex(&schema, "Unknown", &HashMap::new()).is_err());
}

#[test]
fn test_edge_endpoint_validation() {
    let schema = SchemaBuilder::new()
        .mode(ValidationMode::Strict)
        .vertex("Person").done()
        .vertex("Company").done()
        .edge("WORKS_AT")
            .from(&["Person"])
            .to(&["Company"])
            .done()
        .build();
    
    // Valid edge
    assert!(validate_edge(&schema, "WORKS_AT", "Person", "Company", &HashMap::new()).is_ok());
    
    // Invalid source
    assert!(validate_edge(&schema, "WORKS_AT", "Company", "Company", &HashMap::new()).is_err());
    
    // Invalid target
    assert!(validate_edge(&schema, "WORKS_AT", "Person", "Person", &HashMap::new()).is_err());
}

#[test]
fn test_default_value_application() {
    let schema = SchemaBuilder::new()
        .vertex("Person")
            .property("name", PropertyType::String)
            .optional_with_default("active", PropertyType::Bool, Value::Bool(true))
            .optional_with_default("score", PropertyType::Int, Value::Int(0))
            .done()
        .build();
    
    let vs = schema.vertex_schema("Person").unwrap();
    
    let props = HashMap::from([("name".to_string(), Value::String("Alice".into()))]);
    let with_defaults = apply_defaults(vs, &props);
    
    assert_eq!(with_defaults.get("name"), Some(&Value::String("Alice".into())));
    assert_eq!(with_defaults.get("active"), Some(&Value::Bool(true)));
    assert_eq!(with_defaults.get("score"), Some(&Value::Int(0)));
}

#[test]
fn test_alter_type_operations() {
    let mut schema = GraphSchema::new();
    
    // Create type
    execute_ddl(&mut schema, &parse_ddl("CREATE NODE TYPE Person (name STRING NOT NULL)")).unwrap();
    assert!(schema.vertex_schemas.contains_key("Person"));
    
    // Add property
    execute_ddl(&mut schema, &parse_ddl("ALTER NODE TYPE Person ADD email STRING")).unwrap();
    let person = schema.vertex_schema("Person").unwrap();
    assert!(person.properties.contains_key("email"));
    
    // Allow additional properties
    execute_ddl(&mut schema, &parse_ddl("ALTER NODE TYPE Person ALLOW ADDITIONAL PROPERTIES")).unwrap();
    assert!(schema.vertex_schema("Person").unwrap().additional_properties);
    
    // Drop property
    execute_ddl(&mut schema, &parse_ddl("ALTER NODE TYPE Person DROP email")).unwrap();
    assert!(!schema.vertex_schema("Person").unwrap().properties.contains_key("email"));
    
    // Drop type
    execute_ddl(&mut schema, &parse_ddl("DROP NODE TYPE Person")).unwrap();
    assert!(!schema.vertex_schemas.contains_key("Person"));
}
```

### 10.5 Persistence Tests

Add schema persistence tests to `tests/mmap.rs`:

```rust
// tests/mmap.rs (add these tests)

#[test]
fn test_schema_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    
    // Create and save schema
    {
        let graph = MmapGraph::open(&path).unwrap();
        let schema = SchemaBuilder::new()
            .mode(ValidationMode::Strict)
            .vertex("Person")
                .property("name", PropertyType::String)
                .optional("age", PropertyType::Int)
                .done()
            .edge("KNOWS")
                .from(&["Person"])
                .to(&["Person"])
                .done()
            .build();
        graph.save_schema(&schema).unwrap();
    }
    
    // Reopen and verify schema
    {
        let graph = MmapGraph::open(&path).unwrap();
        let schema = graph.load_schema().unwrap().unwrap();
        
        assert_eq!(schema.mode, ValidationMode::Strict);
        assert!(schema.vertex_schemas.contains_key("Person"));
        assert!(schema.edge_schemas.contains_key("KNOWS"));
        
        let person = schema.vertex_schema("Person").unwrap();
        assert!(person.properties.get("name").unwrap().required);
    }
}

#[test]
fn test_schema_recovery_after_crash() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    
    // Simulate crash: save schema without checkpoint
    {
        let graph = MmapGraph::open(&path).unwrap();
        graph.begin_batch().unwrap();
        
        let schema = SchemaBuilder::new()
            .vertex("Person").property("name", PropertyType::String).done()
            .build();
        graph.save_schema(&schema).unwrap();
        
        // Don't commit - simulate crash
        // graph.commit_batch().unwrap();
    }
    
    // Reopen - should recover schema from WAL
    {
        let graph = MmapGraph::open(&path).unwrap();
        let schema = graph.load_schema().unwrap();
        
        // Schema should be recovered
        assert!(schema.is_some());
        assert!(schema.unwrap().vertex_schemas.contains_key("Person"));
    }
}

#[test]
fn test_schema_serialization_roundtrip() {
    let original = SchemaBuilder::new()
        .mode(ValidationMode::Closed)
        .vertex("Person")
            .property("name", PropertyType::String)
            .optional("age", PropertyType::Int)
            .optional_with_default("active", PropertyType::Bool, Value::Bool(true))
            .allow_additional()
            .done()
        .vertex("Product")
            .property("id", PropertyType::Int)
            .optional("tags", PropertyType::List(Some(Box::new(PropertyType::String))))
            .optional("metadata", PropertyType::Map(None))
            .done()
        .edge("PURCHASED")
            .from(&["Person"])
            .to(&["Product"])
            .property("quantity", PropertyType::Int)
            .optional("price", PropertyType::Float)
            .done()
        .build();
    
    let bytes = serialize_schema(&original);
    let deserialized = deserialize_schema(&bytes).unwrap();
    
    // Verify vertex schemas
    assert_eq!(deserialized.mode, ValidationMode::Closed);
    assert_eq!(deserialized.vertex_schemas.len(), 2);
    
    let person = deserialized.vertex_schema("Person").unwrap();
    assert!(person.additional_properties);
    assert!(person.properties.get("name").unwrap().required);
    assert!(!person.properties.get("age").unwrap().required);
    
    let product = deserialized.vertex_schema("Product").unwrap();
    assert_eq!(
        product.properties.get("tags").unwrap().value_type,
        PropertyType::List(Some(Box::new(PropertyType::String)))
    );
    
    // Verify edge schemas
    let purchased = deserialized.edge_schema("PURCHASED").unwrap();
    assert_eq!(purchased.from_labels, vec!["Person"]);
    assert_eq!(purchased.to_labels, vec!["Product"]);
}
```

### 10.6 Test Coverage Goals

| Module | Target Coverage |
|--------|-----------------|
| `src/schema/types.rs` | 100% |
| `src/schema/builder.rs` | 100% |
| `src/schema/validation.rs` | 100% |
| `src/schema/error.rs` | 100% |
| `src/schema/serialize.rs` | 95%+ |
| `src/gql/ddl.rs` | 100% |
| DDL grammar rules | 100% (via snapshots) |

### 10.7 Running Tests

```bash
# Run all schema tests
cargo test schema

# Run DDL tests
cargo test ddl

# Run snapshot tests (and update if needed)
cargo insta test
cargo insta review

# Run with coverage
cargo +nightly llvm-cov --branch --html

# Run specific test
cargo test test_validation_modes -- --exact
```

---
