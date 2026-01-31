//! Graph facade for WASM bindings.
//!
//! Provides a JavaScript-friendly wrapper around the core `crate::storage::Graph`.

use std::sync::Arc;

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsError;

use crate::storage::cow::Graph as InnerGraph;
use crate::storage::GraphStorage;
use crate::wasm::traversal::Traversal;
use crate::wasm::types::{
    create_edge_js, create_vertex_js, js_array_to_vertex_ids, js_to_edge_id, js_to_properties,
    js_to_value, js_to_vertex_id,
};

#[cfg(feature = "gql")]
use crate::wasm::types::values_to_js_array;

/// An in-memory property graph database.
///
/// @example
/// ```typescript
/// const graph = new Graph();
/// const alice = graph.addVertex('person', { name: 'Alice', age: 30n });
/// const bob = graph.addVertex('person', { name: 'Bob', age: 25n });
/// graph.addEdge(alice, bob, 'knows', { since: 2020n });
/// ```
#[wasm_bindgen]
pub struct Graph {
    inner: Arc<InnerGraph>,
}

#[wasm_bindgen]
impl Graph {
    /// Create a new empty graph.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(InnerGraph::new()),
        }
    }

    // =========================================================================
    // Vertex Operations
    // =========================================================================

    /// Add a vertex with a label and properties.
    ///
    /// @param label - The vertex label (e.g., 'person', 'product')
    /// @param properties - Key-value properties (optional)
    /// @returns The new vertex's ID as bigint
    #[wasm_bindgen(js_name = "addVertex")]
    pub fn add_vertex(&self, label: &str, properties: JsValue) -> Result<u64, JsError> {
        let props = js_to_properties(properties)?;
        let id = self.inner.add_vertex(label, props);
        Ok(id.0)
    }

    /// Get a vertex by ID.
    ///
    /// @param id - The vertex ID
    /// @returns The vertex object, or undefined if not found
    #[wasm_bindgen(js_name = "getVertex")]
    pub fn get_vertex(&self, id: JsValue) -> Result<JsValue, JsError> {
        let vertex_id = js_to_vertex_id(id)?;
        let snapshot = self.inner.snapshot();

        match snapshot.get_vertex(vertex_id) {
            Some(v) => create_vertex_js(v.id, &v.label, &v.properties),
            None => Ok(JsValue::UNDEFINED),
        }
    }

    /// Remove a vertex and all its edges.
    ///
    /// @param id - The vertex ID to remove
    /// @returns true if removed, false if not found
    #[wasm_bindgen(js_name = "removeVertex")]
    pub fn remove_vertex(&self, id: JsValue) -> Result<bool, JsError> {
        let vertex_id = js_to_vertex_id(id)?;
        match self.inner.remove_vertex(vertex_id) {
            Ok(_) => Ok(true),
            Err(crate::error::StorageError::VertexNotFound(_)) => Ok(false),
            Err(e) => Err(JsError::from(e)),
        }
    }

    /// Set a property on a vertex.
    ///
    /// @param id - The vertex ID
    /// @param key - Property name
    /// @param value - Property value
    /// @throws If vertex not found
    #[wasm_bindgen(js_name = "setVertexProperty")]
    pub fn set_vertex_property(
        &self,
        id: JsValue,
        key: &str,
        value: JsValue,
    ) -> Result<(), JsError> {
        let vertex_id = js_to_vertex_id(id)?;
        let val = js_to_value(value)?;
        self.inner
            .set_vertex_property(vertex_id, key, val)
            .map_err(JsError::from)
    }

    /// Remove a property from a vertex.
    ///
    /// @param id - The vertex ID
    /// @param key - Property name to remove
    /// @returns true if removed, false if property didn't exist or vertex not found
    #[wasm_bindgen(js_name = "removeVertexProperty")]
    pub fn remove_vertex_property(&self, id: JsValue, key: &str) -> Result<bool, JsError> {
        let vertex_id = js_to_vertex_id(id)?;
        // Get snapshot and check if vertex exists with the property
        let snapshot = self.inner.snapshot();
        if let Some(vertex) = snapshot.get_vertex(vertex_id) {
            if vertex.properties.contains_key(key) {
                // Set to null to "remove" - alternatively we could add a proper remove method
                // For now, this is a workaround since remove_vertex_property doesn't exist
                self.inner
                    .set_vertex_property(vertex_id, key, crate::value::Value::Null)
                    .map_err(|e| JsError::new(&e.to_string()))?;
                Ok(true)
            } else {
                Ok(false) // Property doesn't exist
            }
        } else {
            Err(JsError::new(&format!("Vertex not found: {}", vertex_id.0)))
        }
    }

    // =========================================================================
    // Edge Operations
    // =========================================================================

    /// Add an edge between two vertices.
    ///
    /// @param from - Source vertex ID
    /// @param to - Target vertex ID
    /// @param label - The edge label (e.g., 'knows', 'purchased')
    /// @param properties - Key-value properties (optional)
    /// @returns The new edge's ID as bigint
    /// @throws If source or target vertex not found
    #[wasm_bindgen(js_name = "addEdge")]
    pub fn add_edge(
        &self,
        from: JsValue,
        to: JsValue,
        label: &str,
        properties: JsValue,
    ) -> Result<u64, JsError> {
        let from_id = js_to_vertex_id(from)?;
        let to_id = js_to_vertex_id(to)?;
        let props = js_to_properties(properties)?;

        self.inner
            .add_edge(from_id, to_id, label, props)
            .map(|id| id.0)
            .map_err(JsError::from)
    }

    /// Get an edge by ID.
    ///
    /// @param id - The edge ID
    /// @returns The edge object, or undefined if not found
    #[wasm_bindgen(js_name = "getEdge")]
    pub fn get_edge(&self, id: JsValue) -> Result<JsValue, JsError> {
        let edge_id = js_to_edge_id(id)?;
        let snapshot = self.inner.snapshot();

        match snapshot.get_edge(edge_id) {
            Some(e) => create_edge_js(e.id, &e.label, e.src, e.dst, &e.properties),
            None => Ok(JsValue::UNDEFINED),
        }
    }

    /// Remove an edge.
    ///
    /// @param id - The edge ID to remove
    /// @returns true if removed, false if not found
    #[wasm_bindgen(js_name = "removeEdge")]
    pub fn remove_edge(&self, id: JsValue) -> Result<bool, JsError> {
        let edge_id = js_to_edge_id(id)?;
        match self.inner.remove_edge(edge_id) {
            Ok(_) => Ok(true),
            Err(crate::error::StorageError::EdgeNotFound(_)) => Ok(false),
            Err(e) => Err(JsError::from(e)),
        }
    }

    /// Set a property on an edge.
    ///
    /// @param id - The edge ID
    /// @param key - Property name
    /// @param value - Property value
    /// @throws If edge not found
    #[wasm_bindgen(js_name = "setEdgeProperty")]
    pub fn set_edge_property(&self, id: JsValue, key: &str, value: JsValue) -> Result<(), JsError> {
        let edge_id = js_to_edge_id(id)?;
        let val = js_to_value(value)?;
        self.inner
            .set_edge_property(edge_id, key, val)
            .map_err(JsError::from)
    }

    /// Remove a property from an edge.
    ///
    /// @param id - The edge ID
    /// @param key - Property name to remove
    /// @returns true if removed, false if property didn't exist or edge not found
    #[wasm_bindgen(js_name = "removeEdgeProperty")]
    pub fn remove_edge_property(&self, id: JsValue, key: &str) -> Result<bool, JsError> {
        let edge_id = js_to_edge_id(id)?;
        // Get snapshot and check if edge exists with the property
        let snapshot = self.inner.snapshot();
        if let Some(edge) = snapshot.get_edge(edge_id) {
            if edge.properties.contains_key(key) {
                // Set to null to "remove" - alternatively we could add a proper remove method
                // For now, this is a workaround since remove_edge_property doesn't exist
                self.inner
                    .set_edge_property(edge_id, key, crate::value::Value::Null)
                    .map_err(|e| JsError::new(&e.to_string()))?;
                Ok(true)
            } else {
                Ok(false) // Property doesn't exist
            }
        } else {
            Err(JsError::new(&format!("Edge not found: {}", edge_id.0)))
        }
    }

    // =========================================================================
    // Graph Statistics
    // =========================================================================

    /// Get the total number of vertices.
    #[wasm_bindgen(js_name = "vertexCount")]
    pub fn vertex_count(&self) -> u64 {
        self.inner.vertex_count()
    }

    /// Get the total number of edges.
    #[wasm_bindgen(js_name = "edgeCount")]
    pub fn edge_count(&self) -> u64 {
        self.inner.edge_count()
    }

    /// Clear all vertices and edges from the graph.
    ///
    /// Note: This is a no-op placeholder - true clear is not yet implemented.
    pub fn clear(&self) {
        // COW graph doesn't have a clear method yet
        // For now, this is a no-op. Users should create a new Graph instead.
        // TODO: Implement clear() in the inner graph
    }

    // =========================================================================
    // Traversal Source Steps
    // =========================================================================

    /// Start a traversal from all vertices.
    ///
    /// @returns A traversal over all vertices
    ///
    /// @example
    /// ```typescript
    /// graph.V().hasLabel('person').values('name').toList();
    /// ```
    #[wasm_bindgen(js_name = "V")]
    pub fn v(&self) -> Traversal {
        Traversal::from_all_vertices(Arc::clone(&self.inner))
    }

    /// Start a traversal from specific vertex IDs.
    ///
    /// @param ids - Vertex IDs to start from (as bigint array)
    #[wasm_bindgen(js_name = "V_")]
    pub fn v_ids(&self, ids: JsValue) -> Result<Traversal, JsError> {
        let vertex_ids = js_array_to_vertex_ids(ids)?;
        Ok(Traversal::from_vertex_ids(
            Arc::clone(&self.inner),
            vertex_ids,
        ))
    }

    /// Start a traversal over all edges.
    #[wasm_bindgen(js_name = "E")]
    pub fn e(&self) -> Traversal {
        Traversal::from_all_edges(Arc::clone(&self.inner))
    }

    /// Start a traversal from specific edge IDs.
    ///
    /// @param ids - Edge IDs to start from (as bigint array)
    #[wasm_bindgen(js_name = "E_")]
    pub fn e_ids(&self, ids: JsValue) -> Result<Traversal, JsError> {
        let edge_ids = crate::wasm::types::js_array_to_edge_ids(ids)?;
        Ok(Traversal::from_edge_ids(Arc::clone(&self.inner), edge_ids))
    }

    /// Inject values into a traversal.
    ///
    /// @param values - Values to inject (as array)
    pub fn inject(&self, values: JsValue) -> Result<Traversal, JsError> {
        let vals = crate::wasm::types::js_array_to_values(values)?;
        Ok(Traversal::from_injected_values(
            Arc::clone(&self.inner),
            vals,
        ))
    }

    // =========================================================================
    // Serialization
    // =========================================================================

    /// Export the graph to a GraphSON JSON string.
    ///
    /// @returns GraphSON 3.0 formatted JSON string
    #[cfg(feature = "graphson")]
    #[wasm_bindgen(js_name = "toGraphSON")]
    pub fn to_graphson(&self) -> Result<String, JsError> {
        let snapshot = self.inner.snapshot();
        crate::graphson::to_string(&snapshot)
            .map_err(|e| JsError::new(&format!("GraphSON export error: {}", e)))
    }

    /// Import graph data from a GraphSON JSON string.
    ///
    /// @param json - GraphSON 3.0 formatted JSON string
    /// @returns Import statistics
    #[cfg(feature = "graphson")]
    #[wasm_bindgen(js_name = "fromGraphSON")]
    pub fn from_graphson(&self, json: &str) -> Result<JsValue, JsError> {
        // Parse the GraphSON and import into a new graph
        let imported = crate::graphson::from_str(json)
            .map_err(|e| JsError::new(&format!("GraphSON import error: {}", e)))?;

        // Copy vertices and edges from imported graph to this graph
        let imported_snap = imported.snapshot();
        let mut vertices_imported: u64 = 0;
        let mut edges_imported: u64 = 0;

        // Import vertices
        for v in imported_snap.all_vertices() {
            self.inner.add_vertex(&v.label, v.properties.clone());
            vertices_imported += 1;
        }

        // Import edges
        for e in imported_snap.all_edges() {
            if self
                .inner
                .add_edge(e.src, e.dst, &e.label, e.properties.clone())
                .is_ok()
            {
                edges_imported += 1;
            }
        }

        // Create result object
        let result = js_sys::Object::new();
        js_sys::Reflect::set(
            &result,
            &"verticesImported".into(),
            &js_sys::BigInt::from(vertices_imported).into(),
        )
        .map_err(|_| JsError::new("Failed to set verticesImported"))?;
        js_sys::Reflect::set(
            &result,
            &"edgesImported".into(),
            &js_sys::BigInt::from(edges_imported).into(),
        )
        .map_err(|_| JsError::new("Failed to set edgesImported"))?;
        js_sys::Reflect::set(&result, &"warnings".into(), &js_sys::Array::new().into())
            .map_err(|_| JsError::new("Failed to set warnings"))?;

        Ok(result.into())
    }

    // =========================================================================
    // GQL Query Language
    // =========================================================================

    /// Execute a GQL query string.
    ///
    /// @param query - GQL query string
    /// @returns Query results as an array
    /// @throws If query parsing or execution fails
    ///
    /// @example
    /// ```typescript
    /// const results = graph.gql(`
    ///     MATCH (p:person)-[:knows]->(friend)
    ///     WHERE p.name = 'Alice'
    ///     RETURN friend.name
    /// `);
    /// ```
    #[cfg(feature = "gql")]
    pub fn gql(&self, query: &str) -> Result<JsValue, JsError> {
        let results = self
            .inner
            .gql(query)
            .map_err(|e| JsError::new(&format!("GQL error: {}", e)))?;

        values_to_js_array(results)
    }
}

impl Default for Graph {
    fn default() -> Self {
        Self::new()
    }
}

// Internal accessor for the Arc<InnerGraph>
impl Graph {
    /// Get the inner graph (for internal use).
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> &Arc<InnerGraph> {
        &self.inner
    }
}
