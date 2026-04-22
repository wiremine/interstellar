//! Graph facade for napi-rs bindings.
//!
//! Provides a unified JavaScript-friendly wrapper around graph storage backends:
//! - `Graph.inMemory()` - In-memory graph (fast, non-persistent)
//! - `Graph.open(path)` - Memory-mapped graph (persistent, disk-backed)

use napi::bindgen_prelude::*;
use napi::JsUnknown;
use napi_derive::napi;

use crate::error::ResultExt;
use crate::traversal::{GraphBackend, JsTraversal};
use crate::value::{
    create_edge_js, create_vertex_js, js_array_to_edge_ids, js_array_to_vertex_ids,
    js_to_properties, js_to_value,
};

/// A high-performance graph database.
///
/// Supports both in-memory and persistent (disk-backed) storage modes.
///
/// @example
/// ```javascript
/// const { Graph } = require('@interstellar/node');
///
/// // Create an in-memory graph (fast, non-persistent)
/// const memGraph = Graph.inMemory();
///
/// // Open a persistent graph (data survives restarts)
/// const diskGraph = Graph.open('./my_graph.db');
///
/// // Both use the same API
/// const alice = memGraph.addVertex('person', { name: 'Alice', age: 30n });
/// const bob = memGraph.addVertex('person', { name: 'Bob', age: 25n });
/// memGraph.addEdge(alice, bob, 'knows', { since: 2020n });
/// ```
#[napi(js_name = "Graph")]
pub struct JsGraph {
    pub(crate) backend: GraphBackend,
}

#[napi]
impl JsGraph {
    /// Create a new empty in-memory graph.
    ///
    /// This is a convenience constructor equivalent to `Graph.inMemory()`.
    #[napi(constructor)]
    pub fn new() -> Self {
        Self::in_memory()
    }

    /// Create a new empty in-memory graph.
    ///
    /// In-memory graphs are fast but data is lost when the process exits.
    ///
    /// @returns A new in-memory Graph instance
    ///
    /// @example
    /// ```javascript
    /// const graph = Graph.inMemory();
    /// graph.addVertex('person', { name: 'Alice' });
    /// // Data is lost when process exits
    /// ```
    #[napi(factory, js_name = "inMemory")]
    pub fn in_memory() -> Self {
        use interstellar::storage::cow::Graph as InnerGraph;
        use std::sync::Arc;
        Self {
            backend: GraphBackend::InMemory(Arc::new(InnerGraph::new())),
        }
    }

    /// Open or create a persistent graph database at the given path.
    ///
    /// Persistent graphs use memory-mapped files for efficient storage.
    /// Data survives process restarts.
    ///
    /// @param path - Path to the database file
    /// @returns A new persistent Graph instance
    ///
    /// @example
    /// ```javascript
    /// const graph = Graph.open('./my_graph.db');
    /// graph.addVertex('person', { name: 'Alice' });
    /// // Data is automatically persisted to disk
    /// ```
    #[cfg(feature = "mmap")]
    #[napi(factory)]
    pub fn open(path: String) -> Result<Self> {
        use interstellar::storage::cow_mmap::CowMmapGraph;
        use std::sync::Arc;
        let graph = CowMmapGraph::open(&path).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Failed to open graph: {}", e),
            )
        })?;
        Ok(Self {
            backend: GraphBackend::Mmap(Arc::new(graph)),
        })
    }

    /// Check if this graph is persistent (disk-backed).
    ///
    /// @returns true if persistent, false if in-memory
    #[napi(getter, js_name = "isPersistent")]
    pub fn is_persistent(&self) -> bool {
        match &self.backend {
            GraphBackend::InMemory(_) => false,
            #[cfg(feature = "mmap")]
            GraphBackend::Mmap(_) => true,
        }
    }

    // =========================================================================
    // Vertex Operations
    // =========================================================================

    /// Add a vertex with a label and optional properties.
    ///
    /// @param label - The vertex label (e.g., 'person', 'product')
    /// @param properties - Optional key-value properties
    /// @returns The new vertex's ID as a bigint
    #[napi(js_name = "addVertex")]
    pub fn add_vertex(&self, env: Env, label: String, properties: Option<Object>) -> Result<u64> {
        let props = js_to_properties(env, properties)?;
        let id = self.backend.add_vertex(&label, props);
        Ok(id.0)
    }

    /// Get a vertex by ID.
    ///
    /// @param id - The vertex ID
    /// @returns The vertex object, or undefined if not found
    #[napi(js_name = "getVertex")]
    pub fn get_vertex(&self, env: Env, id: JsUnknown) -> Result<Option<Object>> {
        let vertex_id = crate::value::js_to_vertex_id(env, id)?;
        self.backend
            .with_snapshot(|snapshot| match snapshot.storage().get_vertex(vertex_id) {
                Some(v) => Ok(Some(create_vertex_js(env, v.id, &v.label, &v.properties)?)),
                None => Ok(None),
            })
    }

    /// Remove a vertex and all its incident edges.
    ///
    /// @param id - The vertex ID to remove
    /// @returns true if removed, false if not found
    #[napi(js_name = "removeVertex")]
    pub fn remove_vertex(&self, env: Env, id: JsUnknown) -> Result<bool> {
        let vertex_id = crate::value::js_to_vertex_id(env, id)?;
        match self.backend.remove_vertex(vertex_id) {
            Ok(()) => Ok(true),
            Err(interstellar::error::StorageError::VertexNotFound(_)) => Ok(false),
            Err(e) => Err(crate::error::IntoNapiError::into_napi_error(e)),
        }
    }

    /// Set a property on a vertex.
    ///
    /// @param id - The vertex ID
    /// @param key - Property name
    /// @param value - Property value
    #[napi(js_name = "setVertexProperty")]
    pub fn set_vertex_property(
        &self,
        env: Env,
        id: JsUnknown,
        key: String,
        value: JsUnknown,
    ) -> Result<()> {
        let vertex_id = crate::value::js_to_vertex_id(env, id)?;
        let val = js_to_value(env, value)?;
        self.backend
            .set_vertex_property(vertex_id, &key, val)
            .to_napi()
    }

    // =========================================================================
    // Edge Operations
    // =========================================================================

    /// Add an edge between two vertices.
    ///
    /// @param from - Source vertex ID
    /// @param to - Target vertex ID
    /// @param label - The edge label (e.g., 'knows', 'purchased')
    /// @param properties - Optional key-value properties
    /// @returns The new edge's ID as a bigint
    #[napi(js_name = "addEdge")]
    pub fn add_edge(
        &self,
        env: Env,
        from: JsUnknown,
        to: JsUnknown,
        label: String,
        properties: Option<Object>,
    ) -> Result<u64> {
        let from_id = crate::value::js_to_vertex_id(env, from)?;
        let to_id = crate::value::js_to_vertex_id(env, to)?;
        let props = js_to_properties(env, properties)?;

        self.backend
            .add_edge(from_id, to_id, &label, props)
            .map(|id| id.0)
            .to_napi()
    }

    /// Get an edge by ID.
    ///
    /// @param id - The edge ID
    /// @returns The edge object, or undefined if not found
    #[napi(js_name = "getEdge")]
    pub fn get_edge(&self, env: Env, id: JsUnknown) -> Result<Option<Object>> {
        let edge_id = crate::value::js_to_edge_id(env, id)?;
        self.backend
            .with_snapshot(|snapshot| match snapshot.storage().get_edge(edge_id) {
                Some(e) => Ok(Some(create_edge_js(
                    env,
                    e.id,
                    &e.label,
                    e.src,
                    e.dst,
                    &e.properties,
                )?)),
                None => Ok(None),
            })
    }

    /// Remove an edge.
    ///
    /// @param id - The edge ID to remove
    /// @returns true if removed, false if not found
    #[napi(js_name = "removeEdge")]
    pub fn remove_edge(&self, env: Env, id: JsUnknown) -> Result<bool> {
        let edge_id = crate::value::js_to_edge_id(env, id)?;
        match self.backend.remove_edge(edge_id) {
            Ok(()) => Ok(true),
            Err(interstellar::error::StorageError::EdgeNotFound(_)) => Ok(false),
            Err(e) => Err(crate::error::IntoNapiError::into_napi_error(e)),
        }
    }

    /// Set a property on an edge.
    ///
    /// @param id - The edge ID
    /// @param key - Property name
    /// @param value - Property value
    #[napi(js_name = "setEdgeProperty")]
    pub fn set_edge_property(
        &self,
        env: Env,
        id: JsUnknown,
        key: String,
        value: JsUnknown,
    ) -> Result<()> {
        let edge_id = crate::value::js_to_edge_id(env, id)?;
        let val = js_to_value(env, value)?;
        self.backend.set_edge_property(edge_id, &key, val).to_napi()
    }

    // =========================================================================
    // Graph Statistics
    // =========================================================================

    /// Get the total number of vertices.
    #[napi(getter, js_name = "vertexCount")]
    pub fn vertex_count(&self) -> u32 {
        self.backend.vertex_count() as u32
    }

    /// Get the total number of edges.
    #[napi(getter, js_name = "edgeCount")]
    pub fn edge_count(&self) -> u32 {
        self.backend.edge_count() as u32
    }

    /// Get the current version/transaction ID.
    #[napi(getter)]
    pub fn version(&self) -> u64 {
        self.backend.version()
    }

    // =========================================================================
    // Traversal Entry Points
    // =========================================================================

    /// Start a traversal from all vertices, or specific vertices by ID.
    ///
    /// @param ids - Optional vertex IDs to start from (single ID or array)
    /// @returns A new traversal starting from all vertices or specific vertices
    ///
    /// @example
    /// ```javascript
    /// // All vertices
    /// const names = graph.V()
    ///     .hasLabel('person')
    ///     .values('name')
    ///     .toList();
    ///
    /// // Specific vertices
    /// const alice = graph.V(aliceId).values('name').first();
    /// ```
    #[napi(js_name = "V")]
    pub fn v(&self, env: Env, ids: Option<JsUnknown>) -> Result<JsTraversal> {
        match ids {
            Some(js_ids) => {
                let vertex_ids = js_array_to_vertex_ids(env, js_ids)?;
                Ok(JsTraversal::from_vertex_ids_backend(
                    self.backend.clone(),
                    vertex_ids,
                ))
            }
            None => Ok(JsTraversal::from_all_vertices_backend(self.backend.clone())),
        }
    }

    /// Start a traversal from all edges, or specific edges by ID.
    ///
    /// @param ids - Optional edge IDs to start from (single ID or array)
    #[napi(js_name = "E")]
    pub fn e(&self, env: Env, ids: Option<JsUnknown>) -> Result<JsTraversal> {
        match ids {
            Some(js_ids) => {
                let edge_ids = js_array_to_edge_ids(env, js_ids)?;
                Ok(JsTraversal::from_edge_ids_backend(
                    self.backend.clone(),
                    edge_ids,
                ))
            }
            None => Ok(JsTraversal::from_all_edges_backend(self.backend.clone())),
        }
    }
}

// ============================================================================
// Feature-gated functionality
// ============================================================================

#[cfg(feature = "graphson")]
#[napi]
impl JsGraph {
    /// Export the graph to a GraphSON JSON string.
    ///
    /// @returns GraphSON 3.0 formatted JSON string
    #[napi(js_name = "toGraphSON")]
    pub fn to_graphson(&self) -> Result<String> {
        self.backend.to_graphson_string()
    }

    /// Import graph data from a GraphSON JSON string.
    ///
    /// @param json - GraphSON 3.0 formatted JSON string
    #[napi(js_name = "fromGraphSON")]
    pub fn from_graphson(&self, json: String) -> Result<()> {
        self.backend
            .from_graphson(&json)
            .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))
    }
}

#[cfg(feature = "gql")]
#[napi]
impl JsGraph {
    /// Execute a GQL query string.
    ///
    /// @param query - GQL query string
    /// @returns Query results as an array
    ///
    /// @example
    /// ```javascript
    /// const results = graph.gql(`
    ///     MATCH (p:person)-[:knows]->(friend)
    ///     WHERE p.name = 'Alice'
    ///     RETURN friend.name
    /// `);
    /// ```
    #[napi]
    pub fn gql(&self, env: Env, query: String) -> Result<JsUnknown> {
        self.backend.with_snapshot(|snapshot| {
            // Parse the GQL statement
            let stmt = interstellar::gql::parse_statement(&query)
                .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;

            // Compile and execute the statement against the snapshot
            let results = interstellar::gql::compile_statement(&stmt, snapshot)
                .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;

            crate::value::values_to_js_array(env, results)
        })
    }
}

// ============================================================================
// Saved Query Methods (mmap-backed persistent graphs only)
// ============================================================================

#[cfg(feature = "mmap")]
#[napi]
impl JsGraph {
    /// Save a query to the database for later execution.
    ///
    /// Only available for persistent (mmap-backed) graphs.
    ///
    /// @param name - Unique name for the query
    /// @param queryText - The query string (Gremlin or GQL)
    /// @param queryType - Query language: "gremlin" or "gql" (default: "gql")
    /// @param description - Optional description of the query
    /// @returns The query ID
    ///
    /// @example
    /// ```javascript
    /// const graph = Graph.open('./my_graph.db');
    /// const id = graph.saveQuery('find_alice', 'g.V().has("name", "Alice")', 'gremlin', 'Find Alice');
    /// ```
    #[napi(js_name = "saveQuery")]
    pub fn save_query(
        &self,
        name: String,
        query_text: String,
        query_type: Option<String>,
        description: Option<String>,
    ) -> Result<u32> {
        use interstellar::query::QueryType;

        match &self.backend {
            GraphBackend::Mmap(graph) => {
                let qt = match query_type.as_deref() {
                    Some("gremlin") | Some("Gremlin") => QueryType::Gremlin,
                    _ => QueryType::Gql,
                };
                let desc = description.unwrap_or_default();
                graph
                    .save_query(&name, qt, &desc, &query_text)
                    .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))
            }
            GraphBackend::InMemory(_) => Err(Error::new(
                Status::GenericFailure,
                "Saved queries are only supported for persistent (mmap-backed) graphs",
            )),
        }
    }

    /// Get a saved query by name.
    ///
    /// @param name - The query name
    /// @returns The saved query object, or null if not found
    ///
    /// @example
    /// ```javascript
    /// const query = graph.getQuery('find_alice');
    /// if (query) {
    ///     console.log(query.queryText);
    /// }
    /// ```
    #[napi(js_name = "getQuery")]
    pub fn get_query(&self, env: Env, name: String) -> Result<Option<Object>> {
        match &self.backend {
            GraphBackend::Mmap(graph) => match graph.get_query(&name) {
                Some(q) => Ok(Some(saved_query_to_js(env, &q)?)),
                None => Ok(None),
            },
            GraphBackend::InMemory(_) => Err(Error::new(
                Status::GenericFailure,
                "Saved queries are only supported for persistent (mmap-backed) graphs",
            )),
        }
    }

    /// Get a saved query by ID.
    ///
    /// @param id - The query ID
    /// @returns The saved query object, or null if not found
    #[napi(js_name = "getQueryById")]
    pub fn get_query_by_id(&self, env: Env, id: u32) -> Result<Option<Object>> {
        match &self.backend {
            GraphBackend::Mmap(graph) => match graph.get_query_by_id(id) {
                Some(q) => Ok(Some(saved_query_to_js(env, &q)?)),
                None => Ok(None),
            },
            GraphBackend::InMemory(_) => Err(Error::new(
                Status::GenericFailure,
                "Saved queries are only supported for persistent (mmap-backed) graphs",
            )),
        }
    }

    /// List all saved queries.
    ///
    /// @returns Array of saved query objects
    ///
    /// @example
    /// ```javascript
    /// const queries = graph.listQueries();
    /// for (const q of queries) {
    ///     console.log(`${q.name}: ${q.description}`);
    /// }
    /// ```
    #[napi(js_name = "listQueries")]
    pub fn list_queries(&self, env: Env) -> Result<Vec<Object>> {
        match &self.backend {
            GraphBackend::Mmap(graph) => {
                let queries = graph.list_queries();
                queries.iter().map(|q| saved_query_to_js(env, q)).collect()
            }
            GraphBackend::InMemory(_) => Err(Error::new(
                Status::GenericFailure,
                "Saved queries are only supported for persistent (mmap-backed) graphs",
            )),
        }
    }

    /// Delete a saved query by name.
    ///
    /// @param name - The query name to delete
    ///
    /// @example
    /// ```javascript
    /// graph.deleteQuery('find_alice');
    /// ```
    #[napi(js_name = "deleteQuery")]
    pub fn delete_query(&self, name: String) -> Result<()> {
        match &self.backend {
            GraphBackend::Mmap(graph) => graph
                .delete_query(&name)
                .map_err(|e| Error::new(Status::GenericFailure, e.to_string())),
            GraphBackend::InMemory(_) => Err(Error::new(
                Status::GenericFailure,
                "Saved queries are only supported for persistent (mmap-backed) graphs",
            )),
        }
    }
}

/// Convert a SavedQuery to a JavaScript object.
#[cfg(feature = "mmap")]
fn saved_query_to_js(env: Env, query: &interstellar::query::SavedQuery) -> Result<Object> {
    let mut obj = env.create_object()?;

    obj.set("id", query.id)?;
    obj.set("name", query.name.as_str())?;
    obj.set("queryType", format!("{}", query.query_type))?;
    obj.set("description", query.description.as_str())?;
    obj.set("queryText", query.query.as_str())?;

    // Convert parameters to array of objects
    let mut params = env.create_array(query.parameters.len() as u32)?;
    for (i, param) in query.parameters.iter().enumerate() {
        let mut param_obj = env.create_object()?;
        param_obj.set("name", param.name.as_str())?;
        param_obj.set("paramType", format!("{:?}", param.param_type))?;
        params.set(i as u32, param_obj)?;
    }
    obj.set("parameters", params)?;

    Ok(obj)
}

impl Default for JsGraph {
    fn default() -> Self {
        Self::new()
    }
}
