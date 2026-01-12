# Intersteller: Optional Schema Support

This document outlines the optional schema system for describing the shape of vertices and edges in a Intersteller graph. Schemas are **opt-in** — graphs function without them, but when provided, schemas enable validation, documentation, and tooling support.

---

## 1. Overview

### 1.1 Motivation

Schema-free graphs provide flexibility but can lead to:
- Inconsistent property names (typos like `"nmae"` vs `"name"`)
- Type mismatches (`"30"` string vs `30` integer for age)
- Missing required data discovered only at query time
- Difficulty understanding graph structure without inspecting data

Optional schemas address these issues while preserving flexibility for use cases that don't need them.

### 1.2 Design Principles

1. **Opt-in**: Schemas are never required — unschemaed graphs work exactly as today
2. **Label-centric**: Schemas are defined per vertex/edge label
3. **Fail-fast**: Validation errors surface at mutation time, not query time
4. **Simple types**: Property types map directly to `Value` variants
5. **Composable**: Edge schemas reference vertex schemas by label

---

## 2. Schema Model

### 2.1 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Graph Schema                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Vertex Schemas: HashMap<String, VertexSchema>                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ "person" → VertexSchema {                                │  │
│  │   properties: {                                          │  │
│  │     "name": PropertyDef { type: String, required }       │  │
│  │     "age":  PropertyDef { type: Int, optional }          │  │
│  │   }                                                      │  │
│  │ }                                                        │  │
│  │                                                          │  │
│  │ "software" → VertexSchema {                              │  │
│  │   properties: {                                          │  │
│  │     "name": PropertyDef { type: String, required }       │  │
│  │     "lang": PropertyDef { type: String, optional }       │  │
│  │   }                                                      │  │
│  │ }                                                        │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Edge Schemas: HashMap<String, EdgeSchema>                      │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ "knows" → EdgeSchema {                                   │  │
│  │   from_labels: ["person"],                               │  │
│  │   to_labels: ["person"],                                 │  │
│  │   properties: {                                          │  │
│  │     "since": PropertyDef { type: Int, optional }         │  │
│  │   }                                                      │  │
│  │ }                                                        │  │
│  │                                                          │  │
│  │ "created" → EdgeSchema {                                 │  │
│  │   from_labels: ["person"],                               │  │
│  │   to_labels: ["software"],                               │  │
│  │   properties: {                                          │  │
│  │     "weight": PropertyDef { type: Float, optional }      │  │
│  │   }                                                      │  │
│  │ }                                                        │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Core Types

```rust
/// Complete schema for a graph
#[derive(Clone, Debug, Default)]
pub struct GraphSchema {
    /// Vertex schemas keyed by label
    pub vertex_schemas: HashMap<String, VertexSchema>,
    
    /// Edge schemas keyed by label  
    pub edge_schemas: HashMap<String, EdgeSchema>,
    
    /// Validation mode
    pub mode: ValidationMode,
}

/// Schema for vertices with a specific label
#[derive(Clone, Debug)]
pub struct VertexSchema {
    /// The vertex label this schema applies to
    pub label: String,
    
    /// Property definitions
    pub properties: HashMap<String, PropertyDef>,
    
    /// Allow properties not defined in schema?
    pub additional_properties: bool,
}

/// Schema for edges with a specific label
#[derive(Clone, Debug)]
pub struct EdgeSchema {
    /// The edge label this schema applies to
    pub label: String,
    
    /// Allowed source vertex labels (empty = any)
    pub from_labels: Vec<String>,
    
    /// Allowed target vertex labels (empty = any)
    pub to_labels: Vec<String>,
    
    /// Property definitions
    pub properties: HashMap<String, PropertyDef>,
    
    /// Allow properties not defined in schema?
    pub additional_properties: bool,
}

/// Definition of a single property
#[derive(Clone, Debug)]
pub struct PropertyDef {
    /// Property key name
    pub key: String,
    
    /// Expected value type
    pub value_type: PropertyType,
    
    /// Is this property required?
    pub required: bool,
    
    /// Default value if not provided (must match value_type)
    /// See Section 8: Open Questions
    pub default: Option<Value>,
}

/// Property types (maps to Value variants)
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
```

---

## 3. Type System

### 3.1 Type Mapping

Property types map directly to `Value` variants:

| PropertyType | Value Variant | Example |
|--------------|---------------|---------|
| `Any` | Any variant | `Value::Int(42)`, `Value::String("x")` |
| `Bool` | `Value::Bool` | `Value::Bool(true)` |
| `Int` | `Value::Int` | `Value::Int(42)` |
| `Float` | `Value::Float` | `Value::Float(3.14)` |
| `String` | `Value::String` | `Value::String("Alice")` |
| `List(None)` | `Value::List` | `Value::List(vec![...])` |
| `List(Some(Int))` | `Value::List` of `Int` | `Value::List(vec![Value::Int(1), ...])` |
| `Map(None)` | `Value::Map` | `Value::Map(hashmap!{...})` |
| `Map(Some(String))` | `Value::Map` with `String` values | `Value::Map(hashmap!{"k" => Value::String(...)})` |

### 3.2 Type Compatibility

```rust
impl PropertyType {
    /// Check if a Value matches this type
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
            
            _ => false,
        }
    }
}
```

### 3.3 Null Handling

`Value::Null` is handled specially:
- Required properties **cannot** be `Null`
- Optional properties **can** be `Null` (equivalent to absent)
- `PropertyType::Any` matches `Null`

---

## 4. Validation

### 4.1 Validation Modes

```rust
/// How strictly to enforce schema
#[derive(Clone, Debug, Default)]
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

**Mode behaviors:**

| Mode | Unknown Label | Schema Violation |
|------|---------------|------------------|
| `None` | Allowed | Allowed |
| `Warn` | Allowed (log) | Allowed (log) |
| `Strict` | Allowed | Rejected |
| `Closed` | Rejected | Rejected |

### 4.2 Validation Points

Validation occurs at mutation time:

```rust
impl Graph {
    pub fn add_vertex(
        &mut self,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<VertexId, SchemaError> {
        // 1. Check if schema exists for this label
        if let Some(schema) = self.schema.vertex_schemas.get(label) {
            // 2. Validate properties against schema
            self.validate_vertex_properties(schema, &properties)?;
        } else if self.schema.mode == ValidationMode::Closed {
            // 3. In Closed mode, unknown labels are rejected
            return Err(SchemaError::UnknownVertexLabel { 
                label: label.to_string() 
            });
        }
        // 4. Proceed with insertion
        self.storage.add_vertex(label, properties)
    }
    
    pub fn add_edge(
        &mut self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, SchemaError> {
        // 1. Check if schema exists for this label
        if let Some(schema) = self.schema.edge_schemas.get(label) {
            // 2. Validate endpoint labels
            self.validate_edge_endpoints(schema, src, dst)?;
            
            // 3. Validate properties against schema
            self.validate_edge_properties(schema, &properties)?;
        } else if self.schema.mode == ValidationMode::Closed {
            // 4. In Closed mode, unknown labels are rejected
            return Err(SchemaError::UnknownEdgeLabel { 
                label: label.to_string() 
            });
        }
        // 5. Proceed with insertion
        self.storage.add_edge(src, dst, label, properties)
    }
}
```

### 4.3 Validation Errors

```rust
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
}
```

---

## 5. Schema Builder API

### 5.1 Fluent Builder

```rust
/// Builder for constructing schemas
pub struct SchemaBuilder {
    schema: GraphSchema,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self {
            schema: GraphSchema::default(),
        }
    }
    
    /// Set validation mode
    pub fn mode(mut self, mode: ValidationMode) -> Self {
        self.schema.mode = mode;
        self
    }
    
    /// Add a vertex schema
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
    
    /// Add an edge schema
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
    
    /// Build the final schema
    pub fn build(self) -> GraphSchema {
        self.schema
    }
}

/// Builder for vertex schemas
pub struct VertexSchemaBuilder {
    parent: SchemaBuilder,
    schema: VertexSchema,
}

impl VertexSchemaBuilder {
    /// Add a required property
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
    
    /// Add an optional property
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
    
    /// Add an optional property with default value
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
    
    /// Allow properties not defined in schema
    pub fn allow_additional(mut self) -> Self {
        self.schema.additional_properties = true;
        self
    }
    
    /// Finish vertex schema, return to parent builder
    pub fn done(mut self) -> SchemaBuilder {
        self.parent
            .schema
            .vertex_schemas
            .insert(self.schema.label.clone(), self.schema);
        self.parent
    }
}

/// Builder for edge schemas
pub struct EdgeSchemaBuilder {
    parent: SchemaBuilder,
    schema: EdgeSchema,
}

impl EdgeSchemaBuilder {
    /// Set allowed source vertex labels
    pub fn from(mut self, labels: &[&str]) -> Self {
        self.schema.from_labels = labels.iter().map(|s| s.to_string()).collect();
        self
    }
    
    /// Set allowed target vertex labels
    pub fn to(mut self, labels: &[&str]) -> Self {
        self.schema.to_labels = labels.iter().map(|s| s.to_string()).collect();
        self
    }
    
    /// Add a required property
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
    
    /// Add an optional property
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
    
    /// Allow properties not defined in schema
    pub fn allow_additional(mut self) -> Self {
        self.schema.additional_properties = true;
        self
    }
    
    /// Finish edge schema, return to parent builder
    pub fn done(mut self) -> SchemaBuilder {
        self.parent
            .schema
            .edge_schemas
            .insert(self.schema.label.clone(), self.schema);
        self.parent
    }
}
```

### 5.2 Usage Example

```rust
use intersteller::schema::*;

// Define schema for a social network graph
let schema = SchemaBuilder::new()
    .mode(ValidationMode::Strict)
    
    // Person vertex
    .vertex("person")
        .property("name", PropertyType::String)
        .optional("age", PropertyType::Int)
        .optional("email", PropertyType::String)
        .done()
    
    // Software vertex
    .vertex("software")
        .property("name", PropertyType::String)
        .optional("lang", PropertyType::String)
        .optional("version", PropertyType::String)
        .done()
    
    // Knows edge (person -> person)
    .edge("knows")
        .from(&["person"])
        .to(&["person"])
        .optional("since", PropertyType::Int)
        .optional("weight", PropertyType::Float)
        .done()
    
    // Created edge (person -> software)
    .edge("created")
        .from(&["person"])
        .to(&["software"])
        .optional("weight", PropertyType::Float)
        .done()
    
    .build();

// Create graph with schema
let graph = Graph::in_memory_with_schema(schema);
```

---

## 6. Open vs Closed Schemas

### 6.1 Closed Schema (Default)

By default, schemas are **closed** — only defined properties are allowed:

```rust
// Schema only defines "name" and "age"
let schema = SchemaBuilder::new()
    .mode(ValidationMode::Strict)
    .vertex("person")
        .property("name", PropertyType::String)
        .optional("age", PropertyType::Int)
        .done()
    .build();

let graph = Graph::in_memory_with_schema(schema);

// This fails — "email" is not in schema
graph.add_vertex("person", hashmap!{
    "name" => "Alice".into(),
    "email" => "alice@example.com".into(),  // Error: unexpected property
});
```

### 6.2 Open Schema

Use `allow_additional()` to permit extra properties:

```rust
let schema = SchemaBuilder::new()
    .mode(ValidationMode::Strict)
    .vertex("person")
        .property("name", PropertyType::String)  // Required
        .optional("age", PropertyType::Int)      // Optional, typed
        .allow_additional()                       // Allow any other properties
        .done()
    .build();

// Now "email" is allowed (as any type)
graph.add_vertex("person", hashmap!{
    "name" => "Alice".into(),
    "email" => "alice@example.com".into(),  // OK — additional property
});
```

### 6.3 Schema-Free Labels

In `None`, `Warn`, or `Strict` modes, labels without schemas have no restrictions:

```rust
let schema = SchemaBuilder::new()
    .mode(ValidationMode::Strict)
    .vertex("person")
        .property("name", PropertyType::String)
        .done()
    .build();

// "person" is validated
graph.add_vertex("person", hashmap!{"name" => "Alice".into()});

// "company" has no schema — anything goes
graph.add_vertex("company", hashmap!{
    "anything" => 123.into(),
    "goes" => "here".into(),
});
```

In `Closed` mode, all labels must have schemas defined.

---

## 7. Schema Introspection

### 7.1 Querying Schema

```rust
impl GraphSchema {
    /// Get all defined vertex labels
    pub fn vertex_labels(&self) -> impl Iterator<Item = &str> {
        self.vertex_schemas.keys().map(|s| s.as_str())
    }
    
    /// Get all defined edge labels
    pub fn edge_labels(&self) -> impl Iterator<Item = &str> {
        self.edge_schemas.keys().map(|s| s.as_str())
    }
    
    /// Get schema for a vertex label
    pub fn vertex_schema(&self, label: &str) -> Option<&VertexSchema> {
        self.vertex_schemas.get(label)
    }
    
    /// Get schema for an edge label
    pub fn edge_schema(&self, label: &str) -> Option<&EdgeSchema> {
        self.edge_schemas.get(label)
    }
    
    /// Check if a label has a schema defined
    pub fn has_vertex_schema(&self, label: &str) -> bool {
        self.vertex_schemas.contains_key(label)
    }
    
    /// Get all edge labels that can connect from a vertex label
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
    
    /// Get all edge labels that can connect to a vertex label
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
```

### 7.2 Property Introspection

```rust
impl VertexSchema {
    /// Get all required property keys
    pub fn required_properties(&self) -> impl Iterator<Item = &str> {
        self.properties
            .iter()
            .filter(|(_, def)| def.required)
            .map(|(key, _)| key.as_str())
    }
    
    /// Get all optional property keys
    pub fn optional_properties(&self) -> impl Iterator<Item = &str> {
        self.properties
            .iter()
            .filter(|(_, def)| !def.required)
            .map(|(key, _)| key.as_str())
    }
    
    /// Get type for a property
    pub fn property_type(&self, key: &str) -> Option<&PropertyType> {
        self.properties.get(key).map(|def| &def.value_type)
    }
}
```

---

## 8. Open Questions

### 8.1 Default Value Behavior

The `PropertyDef::default` field is defined but its runtime behavior is not yet specified. Options:

1. **Auto-apply**: When a property with a default is missing during insertion, automatically insert the default value into the stored data.

2. **Query-time**: Store data without the property; default is applied when reading/querying (virtual property).

3. **Documentation only**: Default values exist only for schema documentation and tooling; no runtime behavior.

**Decision deferred** to implementation phase.

---

## 9. Integration with Graph

### 9.1 Creating Graphs with Schemas

```rust
impl Graph {
    /// Create in-memory graph with no schema (current behavior)
    pub fn in_memory() -> Self {
        Self {
            storage: Box::new(InMemoryGraph::new()),
            schema: GraphSchema::default(),
        }
    }
    
    /// Create in-memory graph with schema
    pub fn in_memory_with_schema(schema: GraphSchema) -> Self {
        Self {
            storage: Box::new(InMemoryGraph::new()),
            schema,
        }
    }
    
    /// Get reference to schema
    pub fn schema(&self) -> &GraphSchema {
        &self.schema
    }
    
    /// Update schema (validation behavior TBD)
    pub fn set_schema(&mut self, schema: GraphSchema) -> Result<(), SchemaError> {
        self.schema = schema;
        Ok(())
    }
}
```

---

## 10. Future Considerations

### 10.1 Schema Evolution

Future work may include:
- **Migrations**: Tools to update existing data when schema changes
- **Versioning**: Track schema versions for compatibility
- **Backwards compatibility checks**: Detect breaking changes

### 10.2 Advanced Constraints (Not in Initial Implementation)

Potential future enhancements:
- Numeric bounds (`age >= 0 AND age <= 150`)
- String patterns (regex validation)
- Uniqueness constraints (`email` must be unique)
- Foreign key constraints (property references another vertex)
- Computed/derived properties

### 10.3 Schema Serialization (Future Phase)

Schema will be serializable for:
- Storage alongside graph data
- Import/export to JSON, YAML, or custom format
- Schema sharing between applications

---

## 11. Summary

Intersteller's optional schema system provides:

| Feature | Description |
|---------|-------------|
| **Opt-in** | Schemas are optional; unschemaed graphs work as before |
| **Label-based** | Each vertex/edge label can have its own schema |
| **Type-safe** | Properties validated against `Value` variant types |
| **Edge constraints** | Control which vertex labels can be connected |
| **Flexible modes** | None, Warn, Strict, Closed validation levels |
| **Open/closed** | Allow or disallow extra properties per label |
| **Introspection** | Query schema structure at runtime |

The schema system balances flexibility with data integrity, allowing users to choose the level of strictness appropriate for their use case.
