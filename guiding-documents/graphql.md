# RustGremlin: GraphQL Interface

This document outlines the GraphQL API for exposing RustGremlin graph data over HTTP. The interface provides query and mutation operations for vertices, edges, and schema introspection.

---

## 1. Overview

### 1.1 Goals

- **Read access**: Query vertices and edges by ID, label, or full scan with pagination
- **Write access**: Create, update, and delete vertices and edges
- **Schema introspection**: Expose graph schema definitions (placeholder until schema system is implemented)
- **Type safety**: Leverage GraphQL's type system to provide clear API contracts
- **Performance**: Efficient resolvers that minimize unnecessary data fetching

### 1.2 Technology Stack

| Component | Choice | Rationale |
|-----------|--------|-----------|
| GraphQL Library | `async-graphql` | Most actively maintained Rust GraphQL library, excellent async support |
| HTTP Server | `axum` | Modern, tokio-based, ergonomic API, pairs well with async-graphql |
| Runtime | `tokio` | Standard async runtime for Rust |

### 1.3 Feature Flag

The GraphQL interface is gated behind the `graphql` feature flag to keep dependencies optional:

```toml
[features]
graphql = ["async-graphql", "axum", "tokio"]
```

---

## 2. Architecture

### 2.1 Module Structure

```
src/
├── graphql/
│   ├── mod.rs           # Public API, re-exports
│   ├── types.rs         # GraphQL type definitions (GqlVertex, GqlEdge, etc.)
│   ├── query.rs         # Query resolvers
│   ├── mutation.rs      # Mutation resolvers
│   ├── schema.rs        # Schema introspection types
│   ├── context.rs       # Request context (graph reference)
│   └── server.rs        # Axum server setup
└── lib.rs               # Conditional module inclusion
```

### 2.2 Data Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           HTTP Request                                   │
│                    POST /graphql { query: "..." }                        │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           Axum Router                                    │
│                    axum::Router with GraphQL handler                     │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       async-graphql Executor                             │
│              Parses query, validates, executes resolvers                 │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           Resolvers                                      │
│         Query/Mutation handlers that interact with Graph                 │
│                                                                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                      │
│  │   Query     │  │  Mutation   │  │   Schema    │                      │
│  │  Resolvers  │  │  Resolvers  │  │ Introspect  │                      │
│  └─────────────┘  └─────────────┘  └─────────────┘                      │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          Graph (Arc<Graph>)                              │
│              GraphSnapshot for reads, GraphMut for writes                │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.3 Context Design

The GraphQL context carries a reference to the graph:

```rust
use std::sync::Arc;
use crate::graph::Graph;

/// GraphQL request context
pub struct GqlContext {
    /// Shared reference to the graph
    pub graph: Arc<Graph>,
}

impl GqlContext {
    pub fn new(graph: Arc<Graph>) -> Self {
        Self { graph }
    }
}
```

---

## 3. GraphQL Schema Types

### 3.1 SDL Overview

The complete GraphQL schema in SDL (Schema Definition Language):

```graphql
# Scalar for flexible property values
scalar JSON

# Core vertex type
type Vertex {
  id: ID!
  label: String!
  properties: JSON!
  
  # Navigation
  outEdges(label: String): [Edge!]!
  inEdges(label: String): [Edge!]!
  out(label: String): [Vertex!]!
  in(label: String): [Vertex!]!
}

# Core edge type
type Edge {
  id: ID!
  label: String!
  properties: JSON!
  
  # Endpoints
  source: Vertex!
  target: Vertex!
}

# Pagination info
type PageInfo {
  hasNextPage: Boolean!
  hasPreviousPage: Boolean!
  startCursor: String
  endCursor: String
  totalCount: Int!
}

# Vertex connection (paginated list)
type VertexConnection {
  edges: [VertexEdge!]!
  pageInfo: PageInfo!
}

type VertexEdge {
  cursor: String!
  node: Vertex!
}

# Edge connection (paginated list)  
type EdgeConnection {
  edges: [EdgeEdge!]!
  pageInfo: PageInfo!
}

type EdgeEdge {
  cursor: String!
  node: Edge!
}

# Schema introspection types (placeholder)
type GraphSchema {
  vertexLabels: [String!]!
  edgeLabels: [String!]!
  vertexSchema(label: String!): VertexSchema
  edgeSchema(label: String!): EdgeSchema
}

type VertexSchema {
  label: String!
  properties: [PropertyDef!]!
  additionalProperties: Boolean!
}

type EdgeSchema {
  label: String!
  fromLabels: [String!]!
  toLabels: [String!]!
  properties: [PropertyDef!]!
  additionalProperties: Boolean!
}

type PropertyDef {
  key: String!
  valueType: String!
  required: Boolean!
}

# Input types for mutations
input PropertyInput {
  key: String!
  value: JSON!
}

input VertexInput {
  label: String!
  properties: [PropertyInput!]
}

input EdgeInput {
  sourceId: ID!
  targetId: ID!
  label: String!
  properties: [PropertyInput!]
}

input UpdateVertexInput {
  properties: [PropertyInput!]!
}

input UpdateEdgeInput {
  properties: [PropertyInput!]!
}

# Root query type
type Query {
  # Single item lookups
  vertex(id: ID!): Vertex
  edge(id: ID!): Edge
  
  # List queries with pagination and filtering
  vertices(
    first: Int
    after: String
    last: Int
    before: String
    label: String
  ): VertexConnection!
  
  edges(
    first: Int
    after: String
    last: Int
    before: String
    label: String
  ): EdgeConnection!
  
  # Counts
  vertexCount: Int!
  edgeCount: Int!
  
  # Schema introspection
  schema: GraphSchema!
}

# Root mutation type
type Mutation {
  # Vertex mutations
  addVertex(input: VertexInput!): Vertex!
  updateVertex(id: ID!, input: UpdateVertexInput!): Vertex
  removeVertex(id: ID!): Boolean!
  
  # Edge mutations
  addEdge(input: EdgeInput!): Edge!
  updateEdge(id: ID!, input: UpdateEdgeInput!): Edge
  removeEdge(id: ID!): Boolean!
}
```

### 3.2 Rust Type Definitions

#### 3.2.1 Core Types

```rust
use async_graphql::{Object, Context, Result, ID, Json};
use std::collections::HashMap;
use crate::value::{VertexId, EdgeId, Value};
use crate::storage::Vertex as StorageVertex;
use crate::storage::Edge as StorageEdge;

/// GraphQL representation of a Vertex
pub struct GqlVertex {
    pub id: VertexId,
    pub label: String,
    pub properties: HashMap<String, Value>,
}

impl From<StorageVertex> for GqlVertex {
    fn from(v: StorageVertex) -> Self {
        Self {
            id: v.id,
            label: v.label,
            properties: v.properties,
        }
    }
}

#[Object]
impl GqlVertex {
    /// Unique vertex identifier
    async fn id(&self) -> ID {
        ID(self.id.0.to_string())
    }
    
    /// Vertex label
    async fn label(&self) -> &str {
        &self.label
    }
    
    /// Vertex properties as JSON
    async fn properties(&self) -> Json<HashMap<String, Value>> {
        Json(self.properties.clone())
    }
    
    /// Outgoing edges, optionally filtered by label
    async fn out_edges(
        &self,
        ctx: &Context<'_>,
        label: Option<String>,
    ) -> Result<Vec<GqlEdge>> {
        let context = ctx.data::<GqlContext>()?;
        let snapshot = context.graph.snapshot();
        let storage = snapshot.storage();
        
        let edges: Vec<GqlEdge> = storage
            .out_edges(self.id)
            .filter(|e| label.as_ref().map_or(true, |l| &e.label == l))
            .map(GqlEdge::from)
            .collect();
        
        Ok(edges)
    }
    
    /// Incoming edges, optionally filtered by label
    async fn in_edges(
        &self,
        ctx: &Context<'_>,
        label: Option<String>,
    ) -> Result<Vec<GqlEdge>> {
        let context = ctx.data::<GqlContext>()?;
        let snapshot = context.graph.snapshot();
        let storage = snapshot.storage();
        
        let edges: Vec<GqlEdge> = storage
            .in_edges(self.id)
            .filter(|e| label.as_ref().map_or(true, |l| &e.label == l))
            .map(GqlEdge::from)
            .collect();
        
        Ok(edges)
    }
    
    /// Adjacent vertices via outgoing edges
    async fn out(
        &self,
        ctx: &Context<'_>,
        label: Option<String>,
    ) -> Result<Vec<GqlVertex>> {
        let context = ctx.data::<GqlContext>()?;
        let snapshot = context.graph.snapshot();
        let storage = snapshot.storage();
        
        let vertices: Vec<GqlVertex> = storage
            .out_edges(self.id)
            .filter(|e| label.as_ref().map_or(true, |l| &e.label == l))
            .filter_map(|e| storage.get_vertex(e.dst))
            .map(GqlVertex::from)
            .collect();
        
        Ok(vertices)
    }
    
    /// Adjacent vertices via incoming edges
    async fn in_(
        &self,
        ctx: &Context<'_>,
        label: Option<String>,
    ) -> Result<Vec<GqlVertex>> {
        let context = ctx.data::<GqlContext>()?;
        let snapshot = context.graph.snapshot();
        let storage = snapshot.storage();
        
        let vertices: Vec<GqlVertex> = storage
            .in_edges(self.id)
            .filter(|e| label.as_ref().map_or(true, |l| &e.label == l))
            .filter_map(|e| storage.get_vertex(e.src))
            .map(GqlVertex::from)
            .collect();
        
        Ok(vertices)
    }
}

/// GraphQL representation of an Edge
pub struct GqlEdge {
    pub id: EdgeId,
    pub label: String,
    pub src: VertexId,
    pub dst: VertexId,
    pub properties: HashMap<String, Value>,
}

impl From<StorageEdge> for GqlEdge {
    fn from(e: StorageEdge) -> Self {
        Self {
            id: e.id,
            label: e.label,
            src: e.src,
            dst: e.dst,
            properties: e.properties,
        }
    }
}

#[Object]
impl GqlEdge {
    /// Unique edge identifier
    async fn id(&self) -> ID {
        ID(self.id.0.to_string())
    }
    
    /// Edge label
    async fn label(&self) -> &str {
        &self.label
    }
    
    /// Edge properties as JSON
    async fn properties(&self) -> Json<HashMap<String, Value>> {
        Json(self.properties.clone())
    }
    
    /// Source vertex
    async fn source(&self, ctx: &Context<'_>) -> Result<Option<GqlVertex>> {
        let context = ctx.data::<GqlContext>()?;
        let snapshot = context.graph.snapshot();
        let storage = snapshot.storage();
        
        Ok(storage.get_vertex(self.src).map(GqlVertex::from))
    }
    
    /// Target vertex
    async fn target(&self, ctx: &Context<'_>) -> Result<Option<GqlVertex>> {
        let context = ctx.data::<GqlContext>()?;
        let snapshot = context.graph.snapshot();
        let storage = snapshot.storage();
        
        Ok(storage.get_vertex(self.dst).map(GqlVertex::from))
    }
}
```

#### 3.2.2 Pagination Types

```rust
use async_graphql::SimpleObject;

/// Pagination metadata
#[derive(SimpleObject)]
pub struct PageInfo {
    pub has_next_page: bool,
    pub has_previous_page: bool,
    pub start_cursor: Option<String>,
    pub end_cursor: Option<String>,
    pub total_count: i32,
}

/// Paginated vertex list
#[derive(SimpleObject)]
pub struct VertexConnection {
    pub edges: Vec<VertexEdgeNode>,
    pub page_info: PageInfo,
}

#[derive(SimpleObject)]
pub struct VertexEdgeNode {
    pub cursor: String,
    pub node: GqlVertex,
}

/// Paginated edge list
#[derive(SimpleObject)]
pub struct EdgeConnection {
    pub edges: Vec<EdgeEdgeNode>,
    pub page_info: PageInfo,
}

#[derive(SimpleObject)]
pub struct EdgeEdgeNode {
    pub cursor: String,
    pub node: GqlEdge,
}
```

#### 3.2.3 Input Types

```rust
use async_graphql::InputObject;

#[derive(InputObject)]
pub struct PropertyInput {
    pub key: String,
    pub value: Json<Value>,
}

#[derive(InputObject)]
pub struct VertexInput {
    pub label: String,
    pub properties: Option<Vec<PropertyInput>>,
}

#[derive(InputObject)]
pub struct EdgeInput {
    pub source_id: ID,
    pub target_id: ID,
    pub label: String,
    pub properties: Option<Vec<PropertyInput>>,
}

#[derive(InputObject)]
pub struct UpdateVertexInput {
    pub properties: Vec<PropertyInput>,
}

#[derive(InputObject)]
pub struct UpdateEdgeInput {
    pub properties: Vec<PropertyInput>,
}
```

---

## 4. Query Resolvers

### 4.1 Query Root

```rust
use async_graphql::{Object, Context, Result, ID};

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Get a single vertex by ID
    async fn vertex(&self, ctx: &Context<'_>, id: ID) -> Result<Option<GqlVertex>> {
        let context = ctx.data::<GqlContext>()?;
        let vertex_id = parse_vertex_id(&id)?;
        
        let snapshot = context.graph.snapshot();
        let storage = snapshot.storage();
        
        Ok(storage.get_vertex(vertex_id).map(GqlVertex::from))
    }
    
    /// Get a single edge by ID
    async fn edge(&self, ctx: &Context<'_>, id: ID) -> Result<Option<GqlEdge>> {
        let context = ctx.data::<GqlContext>()?;
        let edge_id = parse_edge_id(&id)?;
        
        let snapshot = context.graph.snapshot();
        let storage = snapshot.storage();
        
        Ok(storage.get_edge(edge_id).map(GqlEdge::from))
    }
    
    /// List vertices with pagination and optional label filter
    async fn vertices(
        &self,
        ctx: &Context<'_>,
        first: Option<i32>,
        after: Option<String>,
        last: Option<i32>,
        before: Option<String>,
        label: Option<String>,
    ) -> Result<VertexConnection> {
        let context = ctx.data::<GqlContext>()?;
        let snapshot = context.graph.snapshot();
        let storage = snapshot.storage();
        
        // Get all vertices (filtered by label if provided)
        let all_vertices: Vec<_> = match &label {
            Some(l) => storage.vertices_with_label(l).collect(),
            None => storage.all_vertices().collect(),
        };
        
        let total_count = all_vertices.len() as i32;
        
        // Apply cursor-based pagination
        let (vertices, page_info) = paginate(
            all_vertices,
            first,
            after,
            last,
            before,
            |v| v.id.0.to_string(),
        )?;
        
        let edges: Vec<VertexEdgeNode> = vertices
            .into_iter()
            .map(|v| VertexEdgeNode {
                cursor: v.id.0.to_string(),
                node: GqlVertex::from(v),
            })
            .collect();
        
        Ok(VertexConnection {
            edges,
            page_info: PageInfo {
                has_next_page: page_info.has_next_page,
                has_previous_page: page_info.has_previous_page,
                start_cursor: page_info.start_cursor,
                end_cursor: page_info.end_cursor,
                total_count,
            },
        })
    }
    
    /// List edges with pagination and optional label filter
    async fn edges(
        &self,
        ctx: &Context<'_>,
        first: Option<i32>,
        after: Option<String>,
        last: Option<i32>,
        before: Option<String>,
        label: Option<String>,
    ) -> Result<EdgeConnection> {
        let context = ctx.data::<GqlContext>()?;
        let snapshot = context.graph.snapshot();
        let storage = snapshot.storage();
        
        // Get all edges (filtered by label if provided)
        let all_edges: Vec<_> = match &label {
            Some(l) => storage.edges_with_label(l).collect(),
            None => storage.all_edges().collect(),
        };
        
        let total_count = all_edges.len() as i32;
        
        // Apply cursor-based pagination
        let (edges_data, page_info) = paginate(
            all_edges,
            first,
            after,
            last,
            before,
            |e| e.id.0.to_string(),
        )?;
        
        let edges: Vec<EdgeEdgeNode> = edges_data
            .into_iter()
            .map(|e| EdgeEdgeNode {
                cursor: e.id.0.to_string(),
                node: GqlEdge::from(e),
            })
            .collect();
        
        Ok(EdgeConnection {
            edges,
            page_info: PageInfo {
                has_next_page: page_info.has_next_page,
                has_previous_page: page_info.has_previous_page,
                start_cursor: page_info.start_cursor,
                end_cursor: page_info.end_cursor,
                total_count,
            },
        })
    }
    
    /// Get total vertex count
    async fn vertex_count(&self, ctx: &Context<'_>) -> Result<i32> {
        let context = ctx.data::<GqlContext>()?;
        let snapshot = context.graph.snapshot();
        let storage = snapshot.storage();
        
        Ok(storage.vertex_count() as i32)
    }
    
    /// Get total edge count
    async fn edge_count(&self, ctx: &Context<'_>) -> Result<i32> {
        let context = ctx.data::<GqlContext>()?;
        let snapshot = context.graph.snapshot();
        let storage = snapshot.storage();
        
        Ok(storage.edge_count() as i32)
    }
    
    /// Get graph schema (placeholder implementation)
    async fn schema(&self, ctx: &Context<'_>) -> Result<GqlGraphSchema> {
        let context = ctx.data::<GqlContext>()?;
        let snapshot = context.graph.snapshot();
        let storage = snapshot.storage();
        
        // Placeholder: derive schema from existing data
        Ok(GqlGraphSchema::from_storage(storage))
    }
}
```

### 4.2 Pagination Helper

```rust
/// Pagination result metadata
struct PaginationInfo {
    has_next_page: bool,
    has_previous_page: bool,
    start_cursor: Option<String>,
    end_cursor: Option<String>,
}

/// Apply cursor-based pagination to a collection
fn paginate<T, F>(
    items: Vec<T>,
    first: Option<i32>,
    after: Option<String>,
    last: Option<i32>,
    before: Option<String>,
    cursor_fn: F,
) -> Result<(Vec<T>, PaginationInfo)>
where
    F: Fn(&T) -> String,
{
    let mut result = items;
    let original_len = result.len();
    
    // Apply 'after' cursor
    if let Some(after_cursor) = &after {
        if let Some(pos) = result.iter().position(|item| cursor_fn(item) == *after_cursor) {
            result = result.into_iter().skip(pos + 1).collect();
        }
    }
    
    // Apply 'before' cursor
    if let Some(before_cursor) = &before {
        if let Some(pos) = result.iter().position(|item| cursor_fn(item) == *before_cursor) {
            result = result.into_iter().take(pos).collect();
        }
    }
    
    let after_cursors_len = result.len();
    
    // Apply 'first' limit
    let has_next = if let Some(first) = first {
        let first = first.max(0) as usize;
        if result.len() > first {
            result.truncate(first);
            true
        } else {
            false
        }
    } else {
        false
    };
    
    // Apply 'last' limit (from the end)
    let has_prev = if let Some(last) = last {
        let last = last.max(0) as usize;
        if result.len() > last {
            let skip = result.len() - last;
            result = result.into_iter().skip(skip).collect();
            true
        } else {
            after.is_some() // has_previous if we skipped via 'after'
        }
    } else {
        after.is_some()
    };
    
    let start_cursor = result.first().map(|item| cursor_fn(item));
    let end_cursor = result.last().map(|item| cursor_fn(item));
    
    Ok((
        result,
        PaginationInfo {
            has_next_page: has_next || (before.is_some() && after_cursors_len < original_len),
            has_previous_page: has_prev,
            start_cursor,
            end_cursor,
        },
    ))
}
```

### 4.3 ID Parsing Helpers

```rust
use async_graphql::Error;
use crate::value::{VertexId, EdgeId};

fn parse_vertex_id(id: &ID) -> Result<VertexId> {
    id.parse::<u64>()
        .map(VertexId)
        .map_err(|_| Error::new(format!("Invalid vertex ID: {}", id)))
}

fn parse_edge_id(id: &ID) -> Result<EdgeId> {
    id.parse::<u64>()
        .map(EdgeId)
        .map_err(|_| Error::new(format!("Invalid edge ID: {}", id)))
}
```

---

## 5. Mutation Resolvers

### 5.1 Mutation Root

```rust
use async_graphql::{Object, Context, Result, ID};
use std::collections::HashMap;

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Add a new vertex to the graph
    async fn add_vertex(
        &self,
        ctx: &Context<'_>,
        input: VertexInput,
    ) -> Result<GqlVertex> {
        let context = ctx.data::<GqlContext>()?;
        
        // Convert properties from input format
        let properties = convert_properties(input.properties)?;
        
        // Acquire mutable access to the graph
        let mut graph_mut = context.graph.mutate();
        let storage = graph_mut.storage_mut();
        
        let vertex_id = storage.add_vertex(&input.label, properties);
        
        // Fetch the created vertex to return
        let vertex = storage
            .get_vertex(vertex_id)
            .ok_or_else(|| Error::new("Failed to retrieve created vertex"))?;
        
        Ok(GqlVertex::from(vertex))
    }
    
    /// Update an existing vertex's properties
    async fn update_vertex(
        &self,
        ctx: &Context<'_>,
        id: ID,
        input: UpdateVertexInput,
    ) -> Result<Option<GqlVertex>> {
        let context = ctx.data::<GqlContext>()?;
        let vertex_id = parse_vertex_id(&id)?;
        
        let mut graph_mut = context.graph.mutate();
        let storage = graph_mut.storage_mut();
        
        // Check if vertex exists
        let existing = match storage.get_vertex(vertex_id) {
            Some(v) => v,
            None => return Ok(None),
        };
        
        // Merge new properties with existing
        let mut properties = existing.properties;
        for prop in input.properties {
            let value = convert_json_to_value(prop.value)?;
            properties.insert(prop.key, value);
        }
        
        // Update the vertex (implementation depends on storage API)
        storage.update_vertex_properties(vertex_id, properties)?;
        
        // Return updated vertex
        Ok(storage.get_vertex(vertex_id).map(GqlVertex::from))
    }
    
    /// Remove a vertex and all its connected edges
    async fn remove_vertex(
        &self,
        ctx: &Context<'_>,
        id: ID,
    ) -> Result<bool> {
        let context = ctx.data::<GqlContext>()?;
        let vertex_id = parse_vertex_id(&id)?;
        
        let mut graph_mut = context.graph.mutate();
        let storage = graph_mut.storage_mut();
        
        // Check if vertex exists before removal
        if storage.get_vertex(vertex_id).is_none() {
            return Ok(false);
        }
        
        storage.remove_vertex(vertex_id);
        Ok(true)
    }
    
    /// Add a new edge between two vertices
    async fn add_edge(
        &self,
        ctx: &Context<'_>,
        input: EdgeInput,
    ) -> Result<GqlEdge> {
        let context = ctx.data::<GqlContext>()?;
        
        let source_id = parse_vertex_id(&input.source_id)?;
        let target_id = parse_vertex_id(&input.target_id)?;
        
        // Convert properties from input format
        let properties = convert_properties(input.properties)?;
        
        let mut graph_mut = context.graph.mutate();
        let storage = graph_mut.storage_mut();
        
        // Validate that both vertices exist
        if storage.get_vertex(source_id).is_none() {
            return Err(Error::new(format!(
                "Source vertex not found: {}",
                input.source_id
            )));
        }
        if storage.get_vertex(target_id).is_none() {
            return Err(Error::new(format!(
                "Target vertex not found: {}",
                input.target_id
            )));
        }
        
        let edge_id = storage.add_edge(source_id, target_id, &input.label, properties);
        
        // Fetch the created edge to return
        let edge = storage
            .get_edge(edge_id)
            .ok_or_else(|| Error::new("Failed to retrieve created edge"))?;
        
        Ok(GqlEdge::from(edge))
    }
    
    /// Update an existing edge's properties
    async fn update_edge(
        &self,
        ctx: &Context<'_>,
        id: ID,
        input: UpdateEdgeInput,
    ) -> Result<Option<GqlEdge>> {
        let context = ctx.data::<GqlContext>()?;
        let edge_id = parse_edge_id(&id)?;
        
        let mut graph_mut = context.graph.mutate();
        let storage = graph_mut.storage_mut();
        
        // Check if edge exists
        let existing = match storage.get_edge(edge_id) {
            Some(e) => e,
            None => return Ok(None),
        };
        
        // Merge new properties with existing
        let mut properties = existing.properties;
        for prop in input.properties {
            let value = convert_json_to_value(prop.value)?;
            properties.insert(prop.key, value);
        }
        
        // Update the edge
        storage.update_edge_properties(edge_id, properties)?;
        
        // Return updated edge
        Ok(storage.get_edge(edge_id).map(GqlEdge::from))
    }
    
    /// Remove an edge
    async fn remove_edge(
        &self,
        ctx: &Context<'_>,
        id: ID,
    ) -> Result<bool> {
        let context = ctx.data::<GqlContext>()?;
        let edge_id = parse_edge_id(&id)?;
        
        let mut graph_mut = context.graph.mutate();
        let storage = graph_mut.storage_mut();
        
        // Check if edge exists before removal
        if storage.get_edge(edge_id).is_none() {
            return Ok(false);
        }
        
        storage.remove_edge(edge_id);
        Ok(true)
    }
}
```

### 5.2 Property Conversion Helpers

```rust
use async_graphql::Json;
use crate::value::Value;

/// Convert property inputs to HashMap
fn convert_properties(
    inputs: Option<Vec<PropertyInput>>,
) -> Result<HashMap<String, Value>> {
    let mut properties = HashMap::new();
    
    if let Some(props) = inputs {
        for prop in props {
            let value = convert_json_to_value(prop.value)?;
            properties.insert(prop.key, value);
        }
    }
    
    Ok(properties)
}

/// Convert JSON value to internal Value type
fn convert_json_to_value(json: Json<serde_json::Value>) -> Result<Value> {
    let inner = json.0;
    json_to_value(inner)
}

fn json_to_value(json: serde_json::Value) -> Result<Value> {
    match json {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Bool(b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float(f))
            } else {
                Err(Error::new("Invalid number value"))
            }
        }
        serde_json::Value::String(s) => Ok(Value::String(s)),
        serde_json::Value::Array(arr) => {
            let values: Result<Vec<Value>, _> = arr
                .into_iter()
                .map(json_to_value)
                .collect();
            Ok(Value::List(values?))
        }
        serde_json::Value::Object(obj) => {
            let map: Result<HashMap<String, Value>, _> = obj
                .into_iter()
                .map(|(k, v)| json_to_value(v).map(|val| (k, val)))
                .collect();
            Ok(Value::Map(map?))
        }
    }
}
```

---

## 6. Schema Introspection (Placeholder)

### 6.1 Schema Types

Until the full schema system is implemented (see `schema.md`), we provide a placeholder that derives schema information from existing data:

```rust
use async_graphql::{Object, SimpleObject};
use std::collections::HashSet;

/// GraphQL representation of graph schema
pub struct GqlGraphSchema {
    vertex_labels: Vec<String>,
    edge_labels: Vec<String>,
}

impl GqlGraphSchema {
    /// Derive schema from storage by scanning existing labels
    pub fn from_storage(storage: &dyn GraphStorage) -> Self {
        let mut vertex_labels: HashSet<String> = HashSet::new();
        let mut edge_labels: HashSet<String> = HashSet::new();
        
        // Scan all vertices to collect labels
        for vertex in storage.all_vertices() {
            vertex_labels.insert(vertex.label);
        }
        
        // Scan all edges to collect labels
        for edge in storage.all_edges() {
            edge_labels.insert(edge.label);
        }
        
        Self {
            vertex_labels: vertex_labels.into_iter().collect(),
            edge_labels: edge_labels.into_iter().collect(),
        }
    }
}

#[Object]
impl GqlGraphSchema {
    /// All vertex labels in the graph
    async fn vertex_labels(&self) -> &[String] {
        &self.vertex_labels
    }
    
    /// All edge labels in the graph
    async fn edge_labels(&self) -> &[String] {
        &self.edge_labels
    }
    
    /// Get schema for a specific vertex label (placeholder)
    async fn vertex_schema(&self, label: String) -> Option<GqlVertexSchema> {
        if self.vertex_labels.contains(&label) {
            Some(GqlVertexSchema::placeholder(label))
        } else {
            None
        }
    }
    
    /// Get schema for a specific edge label (placeholder)
    async fn edge_schema(&self, label: String) -> Option<GqlEdgeSchema> {
        if self.edge_labels.contains(&label) {
            Some(GqlEdgeSchema::placeholder(label))
        } else {
            None
        }
    }
}

/// Placeholder vertex schema
#[derive(SimpleObject)]
pub struct GqlVertexSchema {
    pub label: String,
    pub properties: Vec<GqlPropertyDef>,
    pub additional_properties: bool,
}

impl GqlVertexSchema {
    fn placeholder(label: String) -> Self {
        Self {
            label,
            properties: vec![], // No property definitions until schema system is implemented
            additional_properties: true, // Allow any properties
        }
    }
}

/// Placeholder edge schema
#[derive(SimpleObject)]
pub struct GqlEdgeSchema {
    pub label: String,
    pub from_labels: Vec<String>,
    pub to_labels: Vec<String>,
    pub properties: Vec<GqlPropertyDef>,
    pub additional_properties: bool,
}

impl GqlEdgeSchema {
    fn placeholder(label: String) -> Self {
        Self {
            label,
            from_labels: vec![], // Any source vertex allowed
            to_labels: vec![],   // Any target vertex allowed
            properties: vec![],
            additional_properties: true,
        }
    }
}

/// Property definition
#[derive(SimpleObject)]
pub struct GqlPropertyDef {
    pub key: String,
    pub value_type: String,
    pub required: bool,
}
```

### 6.2 Future Schema Integration

When the full schema system is implemented, `GqlGraphSchema::from_storage` will be replaced with:

```rust
impl GqlGraphSchema {
    pub fn from_graph_schema(schema: &GraphSchema) -> Self {
        Self {
            vertex_labels: schema.vertex_labels().map(String::from).collect(),
            edge_labels: schema.edge_labels().map(String::from).collect(),
            // Store full schema reference for property definitions
        }
    }
}
```

---

## 7. Server Setup

### 7.1 Building the Schema

```rust
use async_graphql::{Schema, EmptySubscription};

pub type GqlSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;

/// Build the GraphQL schema
pub fn build_schema(graph: Arc<Graph>) -> GqlSchema {
    let context = GqlContext::new(graph);
    
    Schema::build(QueryRoot, MutationRoot, EmptySubscription)
        .data(context)
        .finish()
}
```

### 7.2 Axum Integration

```rust
use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use std::sync::Arc;

/// Application state
#[derive(Clone)]
pub struct AppState {
    pub schema: GqlSchema,
}

/// GraphQL handler
async fn graphql_handler(
    State(state): State<AppState>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    state.schema.execute(req.into_inner()).await.into()
}

/// GraphQL Playground handler (development only)
async fn playground_handler() -> impl IntoResponse {
    Html(playground_source(GraphQLPlaygroundConfig::new("/graphql")))
}

/// Health check endpoint
async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

/// Build the Axum router
pub fn build_router(graph: Arc<Graph>) -> Router {
    let schema = build_schema(graph);
    let state = AppState { schema };
    
    Router::new()
        .route("/graphql", post(graphql_handler))
        .route("/playground", get(playground_handler))
        .route("/health", get(health_handler))
        .with_state(state)
}
```

### 7.3 Server Entry Point

```rust
use std::net::SocketAddr;
use tokio::net::TcpListener;

/// GraphQL server configuration
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
        }
    }
}

/// Start the GraphQL server
pub async fn serve(graph: Arc<Graph>, config: ServerConfig) -> Result<(), std::io::Error> {
    let router = build_router(graph);
    
    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .expect("Invalid server address");
    
    println!("GraphQL server listening on http://{}", addr);
    println!("GraphQL Playground: http://{}/playground", addr);
    
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, router).await
}
```

### 7.4 Usage Example

```rust
use rustgremlin::prelude::*;
use rustgremlin::graphql::{serve, ServerConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // Create graph
    let graph = Arc::new(Graph::in_memory());
    
    // Add some data
    {
        let mut g = graph.mutate();
        let storage = g.storage_mut();
        
        let alice = storage.add_vertex("person", hashmap!{
            "name" => "Alice".into(),
            "age" => 30.into(),
        });
        let bob = storage.add_vertex("person", hashmap!{
            "name" => "Bob".into(),
            "age" => 25.into(),
        });
        storage.add_edge(alice, bob, "knows", hashmap!{
            "since" => 2020.into(),
        });
    }
    
    // Start server
    let config = ServerConfig {
        host: "0.0.0.0".to_string(),
        port: 8080,
    };
    
    serve(graph, config).await.unwrap();
}
```

---

## 8. Dependencies

### 8.1 Cargo.toml Additions

```toml
[features]
default = ["inmemory"]
inmemory = []
mmap = ["memmap2"]
full-text = ["tantivy"]
graphql = ["async-graphql", "axum", "tokio", "async-graphql-axum", "tower-http"]

[dependencies]
# Existing dependencies...
thiserror = "1.0"
hashbrown = "0.14"
smallvec = "1.11"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"  # Move from dev-dependencies if using graphql
parking_lot = "0.12"
roaring = "0.10"
regex = "1.10"

# GraphQL dependencies (optional)
async-graphql = { version = "7.0", optional = true }
async-graphql-axum = { version = "7.0", optional = true }
axum = { version = "0.7", optional = true }
tokio = { version = "1.0", features = ["full"], optional = true }
tower-http = { version = "0.5", features = ["cors"], optional = true }
```

---

## 9. Testing

### 9.1 Unit Tests

```rust
#[cfg(test)]
#[cfg(feature = "graphql")]
mod tests {
    use super::*;
    use async_graphql::Request;
    
    fn setup_test_graph() -> Arc<Graph> {
        let graph = Arc::new(Graph::in_memory());
        {
            let mut g = graph.mutate();
            let storage = g.storage_mut();
            
            let alice = storage.add_vertex("person", hashmap!{
                "name" => "Alice".into(),
                "age" => 30.into(),
            });
            let bob = storage.add_vertex("person", hashmap!{
                "name" => "Bob".into(),
            });
            storage.add_edge(alice, bob, "knows", hashmap!{});
        }
        graph
    }
    
    #[tokio::test]
    async fn test_query_vertex_by_id() {
        let graph = setup_test_graph();
        let schema = build_schema(graph);
        
        let query = r#"
            query {
                vertex(id: "0") {
                    id
                    label
                    properties
                }
            }
        "#;
        
        let result = schema.execute(Request::new(query)).await;
        assert!(result.errors.is_empty());
        
        let data = result.data.into_json().unwrap();
        assert_eq!(data["vertex"]["label"], "person");
    }
    
    #[tokio::test]
    async fn test_query_vertices_with_pagination() {
        let graph = setup_test_graph();
        let schema = build_schema(graph);
        
        let query = r#"
            query {
                vertices(first: 1) {
                    edges {
                        cursor
                        node {
                            id
                            label
                        }
                    }
                    pageInfo {
                        hasNextPage
                        totalCount
                    }
                }
            }
        "#;
        
        let result = schema.execute(Request::new(query)).await;
        assert!(result.errors.is_empty());
        
        let data = result.data.into_json().unwrap();
        assert_eq!(data["vertices"]["pageInfo"]["totalCount"], 2);
        assert_eq!(data["vertices"]["pageInfo"]["hasNextPage"], true);
    }
    
    #[tokio::test]
    async fn test_mutation_add_vertex() {
        let graph = setup_test_graph();
        let schema = build_schema(graph);
        
        let mutation = r#"
            mutation {
                addVertex(input: {
                    label: "software"
                    properties: [
                        { key: "name", value: "GraphDB" }
                    ]
                }) {
                    id
                    label
                    properties
                }
            }
        "#;
        
        let result = schema.execute(Request::new(mutation)).await;
        assert!(result.errors.is_empty());
        
        let data = result.data.into_json().unwrap();
        assert_eq!(data["addVertex"]["label"], "software");
    }
    
    #[tokio::test]
    async fn test_vertex_navigation() {
        let graph = setup_test_graph();
        let schema = build_schema(graph);
        
        let query = r#"
            query {
                vertex(id: "0") {
                    label
                    out {
                        id
                        label
                    }
                    outEdges {
                        label
                        target {
                            label
                        }
                    }
                }
            }
        "#;
        
        let result = schema.execute(Request::new(query)).await;
        assert!(result.errors.is_empty());
    }
}
```

### 9.2 Integration Tests

```rust
#[cfg(test)]
#[cfg(feature = "graphql")]
mod integration_tests {
    use super::*;
    use axum::http::StatusCode;
    use axum_test::TestServer;
    
    #[tokio::test]
    async fn test_graphql_endpoint() {
        let graph = Arc::new(Graph::in_memory());
        let router = build_router(graph);
        let server = TestServer::new(router).unwrap();
        
        let response = server
            .post("/graphql")
            .json(&serde_json::json!({
                "query": "{ vertexCount edgeCount }"
            }))
            .await;
        
        assert_eq!(response.status_code(), StatusCode::OK);
    }
    
    #[tokio::test]
    async fn test_health_endpoint() {
        let graph = Arc::new(Graph::in_memory());
        let router = build_router(graph);
        let server = TestServer::new(router).unwrap();
        
        let response = server.get("/health").await;
        assert_eq!(response.status_code(), StatusCode::OK);
    }
}
```

---

## 10. Example Queries

### 10.1 Querying Vertices

```graphql
# Get all vertices
query {
  vertices {
    edges {
      node {
        id
        label
        properties
      }
    }
    pageInfo {
      totalCount
    }
  }
}

# Get vertices with pagination
query {
  vertices(first: 10, after: "cursor123", label: "person") {
    edges {
      cursor
      node {
        id
        label
        properties
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}

# Get single vertex with navigation
query {
  vertex(id: "1") {
    id
    label
    properties
    out(label: "knows") {
      id
      label
    }
    outEdges(label: "knows") {
      id
      label
      properties
      target {
        id
        label
      }
    }
  }
}
```

### 10.2 Querying Edges

```graphql
# Get all edges
query {
  edges {
    edges {
      node {
        id
        label
        source { id label }
        target { id label }
      }
    }
  }
}

# Get edges by label
query {
  edges(label: "knows") {
    edges {
      node {
        id
        properties
      }
    }
  }
}
```

### 10.3 Mutations

```graphql
# Add a vertex
mutation {
  addVertex(input: {
    label: "person"
    properties: [
      { key: "name", value: "Charlie" }
      { key: "age", value: 35 }
    ]
  }) {
    id
    label
    properties
  }
}

# Add an edge
mutation {
  addEdge(input: {
    sourceId: "1"
    targetId: "2"
    label: "knows"
    properties: [
      { key: "since", value: 2024 }
    ]
  }) {
    id
    label
    source { label }
    target { label }
  }
}

# Update vertex properties
mutation {
  updateVertex(id: "1", input: {
    properties: [
      { key: "age", value: 31 }
    ]
  }) {
    id
    properties
  }
}

# Remove a vertex
mutation {
  removeVertex(id: "1")
}
```

### 10.4 Schema Introspection

```graphql
query {
  schema {
    vertexLabels
    edgeLabels
    vertexSchema(label: "person") {
      label
      properties {
        key
        valueType
        required
      }
    }
  }
}
```

---

## 11. Implementation Plan

### Phase 1: Core Types and Queries (2-3 days)

1. Add dependencies to `Cargo.toml` with `graphql` feature
2. Create `src/graphql/mod.rs` with module structure
3. Implement `types.rs`: `GqlVertex`, `GqlEdge`, pagination types
4. Implement `query.rs`: Single item queries, list queries with pagination
5. Implement `context.rs`: `GqlContext`
6. Write unit tests for queries

### Phase 2: Mutations (1-2 days)

1. Implement `mutation.rs`: Add/update/remove for vertices and edges
2. Implement property conversion helpers
3. Write unit tests for mutations
4. Handle edge cases (missing vertices, invalid IDs)

### Phase 3: Schema Introspection (1 day)

1. Implement `schema.rs`: Placeholder schema types
2. Derive labels from existing data
3. Write tests for schema queries

### Phase 4: Server Integration (1 day)

1. Implement `server.rs`: Axum router, handlers
2. Add GraphQL Playground support
3. Add health check endpoint
4. Write integration tests

### Phase 5: Documentation & Polish (1 day)

1. Add rustdoc comments
2. Create example binary
3. Update main README
4. Performance testing

---

## 12. Future Enhancements

### 12.1 Subscriptions

Real-time updates when graph data changes:

```graphql
subscription {
  vertexAdded(label: "person") {
    id
    label
    properties
  }
}
```

### 12.2 Batch Operations

Efficient bulk mutations:

```graphql
mutation {
  addVertices(inputs: [...]) {
    id
  }
  addEdges(inputs: [...]) {
    id
  }
}
```

### 12.3 Traversal Queries

Expose the Gremlin-style traversal API via GraphQL:

```graphql
query {
  traverse {
    v(ids: ["1"])
    out(label: "knows")
    has(key: "age", predicate: GT, value: 25)
    values(keys: ["name"])
  }
}
```

### 12.4 DataLoader Integration

Use async-graphql's DataLoader for N+1 query optimization:

```rust
use async_graphql::dataloader::{DataLoader, Loader};

pub struct VertexLoader {
    graph: Arc<Graph>,
}

impl Loader<VertexId> for VertexLoader {
    type Value = GqlVertex;
    type Error = Error;
    
    async fn load(&self, keys: &[VertexId]) -> Result<HashMap<VertexId, Self::Value>> {
        // Batch load vertices
    }
}
```

---

## 13. Summary

This GraphQL interface provides:

| Feature | Description |
|---------|-------------|
| **Query vertices** | By ID, with pagination, filtered by label |
| **Query edges** | By ID, with pagination, filtered by label |
| **Navigation** | Traverse graph via `out`, `in`, `outEdges`, `inEdges` |
| **Mutations** | Add, update, remove vertices and edges |
| **Schema introspection** | Placeholder for label discovery |
| **Playground** | Interactive GraphQL IDE for development |
| **Type safety** | Full GraphQL type system with JSON properties |
| **Pagination** | Cursor-based Relay-style pagination |

The implementation is designed to be:
- **Optional**: Behind a feature flag
- **Extensible**: Easy to add subscriptions, batch ops, traversals
- **Performant**: Direct storage access, no unnecessary copies
- **Testable**: Comprehensive unit and integration tests
