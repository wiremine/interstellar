//! Reusable test graph fixtures.
//!
//! Provides standardized test graphs used across integration tests:
//! - `create_small_graph()` - 4 vertices (3 person, 1 software) with 5 edges
//! - `create_medium_graph()` - 5 vertices (adds Redis software)
//! - `create_social_graph()` - Larger graph for complex traversal tests
//! - `create_gql_test_graph()` - Graph for GQL tests with Person/Company vertices
//! - `TestGraphBuilder` - Builder pattern for custom test graphs
//!
//! ## Unified API (Spec 33)
//!
//! All graphs now use the unified COW-based API:
//! - `Graph` - In-memory graph with COW semantics and O(1) snapshots
//! - `GraphSnapshot` - Immutable snapshot for read operations

use std::collections::HashMap;

use interstellar::storage::{Graph, GraphSnapshot, GraphStorage};
use interstellar::value::{EdgeId, Value, VertexId};

/// Standard test graph with vertices and their IDs for assertions.
///
/// The graph structure allows testing most traversal patterns including:
/// - Navigation (out, in, both)
/// - Filtering (by label, properties)
/// - Paths and cycles
/// - Aggregations
#[allow(dead_code)]
pub struct TestGraph {
    pub graph: Graph,
    // Person vertices
    pub alice: VertexId,
    pub bob: VertexId,
    pub charlie: VertexId,
    // Software vertices
    pub graphdb: VertexId,
    // Optional vertices for extended graphs
    pub redis: Option<VertexId>,
    pub eve: Option<VertexId>,
    // Edge IDs (commonly used in tests)
    pub alice_knows_bob: EdgeId,
    pub bob_knows_charlie: EdgeId,
    pub alice_uses_graphdb: Option<EdgeId>,
    pub bob_uses_graphdb: Option<EdgeId>,
    pub charlie_knows_alice: Option<EdgeId>,
}

impl TestGraph {
    /// Access the graph traversal source via a snapshot.
    ///
    /// # Example
    /// ```ignore
    /// let tg = create_small_graph();
    /// let snapshot = tg.graph.snapshot();
    /// let results = snapshot.gremlin().v().has_label("person").to_list();
    /// ```
    pub fn snapshot(&self) -> GraphSnapshot {
        self.graph.snapshot()
    }
}

/// Builder for creating custom test graphs with specific configurations.
///
/// # Example
/// ```ignore
/// let tg = TestGraphBuilder::new()
///     .add_person("alice", 30)
///     .add_person("bob", 25)
///     .add_software("graphdb", "rust")
///     .add_edge(0, 1, "knows")
///     .add_edge(0, 2, "created")
///     .build();
/// ```
pub struct TestGraphBuilder {
    graph: Graph,
    vertices: Vec<VertexId>,
}

#[allow(dead_code)]
impl TestGraphBuilder {
    pub fn new() -> Self {
        TestGraphBuilder {
            graph: Graph::new(),
            vertices: Vec::new(),
        }
    }

    /// Add a person vertex with name and age properties.
    pub fn add_person(mut self, name: &str, age: i64) -> Self {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String(name.to_string()));
        props.insert("age".to_string(), Value::Int(age));
        let id = self.graph.add_vertex("person", props);
        self.vertices.push(id);
        self
    }

    /// Add a person vertex with name, age, and additional status property.
    pub fn add_person_with_status(mut self, name: &str, age: i64, status: &str) -> Self {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String(name.to_string()));
        props.insert("age".to_string(), Value::Int(age));
        props.insert("status".to_string(), Value::String(status.to_string()));
        let id = self.graph.add_vertex("person", props);
        self.vertices.push(id);
        self
    }

    /// Add a software vertex with name and language properties.
    pub fn add_software(mut self, name: &str, lang: &str) -> Self {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String(name.to_string()));
        props.insert("lang".to_string(), Value::String(lang.to_string()));
        let id = self.graph.add_vertex("software", props);
        self.vertices.push(id);
        self
    }

    /// Add a software vertex with name and version properties.
    #[allow(dead_code)]
    pub fn add_software_with_version(mut self, name: &str, version: f64) -> Self {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String(name.to_string()));
        props.insert("version".to_string(), Value::Float(version));
        let id = self.graph.add_vertex("software", props);
        self.vertices.push(id);
        self
    }

    /// Add an edge between vertices by their builder index.
    pub fn add_edge(mut self, from_idx: usize, to_idx: usize, label: &str) -> Self {
        let from = self.vertices[from_idx];
        let to = self.vertices[to_idx];
        self.graph
            .add_edge(from, to, label, HashMap::new())
            .unwrap();
        self
    }

    /// Add an edge with properties between vertices by their builder index.
    #[allow(dead_code)]
    pub fn add_edge_with_props(
        mut self,
        from_idx: usize,
        to_idx: usize,
        label: &str,
        props: HashMap<String, Value>,
    ) -> Self {
        let from = self.vertices[from_idx];
        let to = self.vertices[to_idx];
        self.graph.add_edge(from, to, label, props).unwrap();
        self
    }

    /// Build the graph and return it.
    pub fn build(self) -> Graph {
        self.graph
    }

    /// Get the vertex ID at a specific index (for assertions).
    #[allow(dead_code)]
    pub fn vertex_id(&self, idx: usize) -> VertexId {
        self.vertices[idx]
    }

    /// Get all vertex IDs.
    #[allow(dead_code)]
    pub fn vertex_ids(&self) -> &[VertexId] {
        &self.vertices
    }
}

impl Default for TestGraphBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Creates an empty graph for testing edge cases.
pub fn create_empty_graph() -> Graph {
    Graph::new()
}

/// Creates a small test graph with 4 vertices and 5 edges.
///
/// Graph structure:
/// ```text
///     Alice ----knows----> Bob ----knows----> Charlie
///       |                   |                   |
///       |                   |                   |
///      uses                uses              knows
///       |                   |                   |
///       v                   v                   |
///     GraphDB <-------------+                   |
///       ^                                       |
///       |                                       |
///       +---------------------------------------+
///                    (Charlie knows Alice)
/// ```
///
/// Vertices:
/// - Alice (person): name="Alice", age=30
/// - Bob (person): name="Bob", age=25
/// - Charlie (person): name="Charlie", age=35
/// - GraphDB (software): name="GraphDB", version=1.0
///
/// Edges:
/// - Alice -[knows]-> Bob (since=2020)
/// - Bob -[knows]-> Charlie (since=2021)
/// - Alice -[uses]-> GraphDB (skill="expert")
/// - Bob -[uses]-> GraphDB (skill="beginner")
/// - Charlie -[knows]-> Alice (since=2019) - creates cycle
pub fn create_small_graph() -> TestGraph {
    let graph = Graph::new();

    // Add vertices with properties
    let alice = graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props
    });

    let bob = graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        props.insert("age".to_string(), Value::Int(25));
        props
    });

    let charlie = graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Charlie".to_string()));
        props.insert("age".to_string(), Value::Int(35));
        props
    });

    let graphdb = graph.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("GraphDB".to_string()));
        props.insert("version".to_string(), Value::Float(1.0));
        props
    });

    // Add edges with properties
    let alice_knows_bob = graph
        .add_edge(alice, bob, "knows", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2020));
            props
        })
        .unwrap();

    let bob_knows_charlie = graph
        .add_edge(bob, charlie, "knows", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2021));
            props
        })
        .unwrap();

    let alice_uses_graphdb = graph
        .add_edge(alice, graphdb, "uses", {
            let mut props = HashMap::new();
            props.insert("skill".to_string(), Value::String("expert".to_string()));
            props
        })
        .unwrap();

    let bob_uses_graphdb = graph
        .add_edge(bob, graphdb, "uses", {
            let mut props = HashMap::new();
            props.insert("skill".to_string(), Value::String("beginner".to_string()));
            props
        })
        .unwrap();

    let charlie_knows_alice = graph
        .add_edge(charlie, alice, "knows", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2019));
            props
        })
        .unwrap();

    TestGraph {
        graph,
        alice,
        bob,
        charlie,
        graphdb,
        redis: None,
        eve: None,
        alice_knows_bob,
        bob_knows_charlie,
        alice_uses_graphdb: Some(alice_uses_graphdb),
        bob_uses_graphdb: Some(bob_uses_graphdb),
        charlie_knows_alice: Some(charlie_knows_alice),
    }
}

/// Creates a medium test graph with 5 vertices (adds Redis software).
///
/// Extends `create_small_graph()` with:
/// - Redis (software): name="Redis", version=7.0
/// - Charlie -[created]-> Redis edge
pub fn create_medium_graph() -> TestGraph {
    let graph = Graph::new();

    // Add person vertices with status property (used by branch tests)
    let alice = graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props.insert("status".to_string(), Value::String("active".to_string()));
        props.insert("priority".to_string(), Value::Int(1));
        props
    });

    let bob = graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        props.insert("age".to_string(), Value::Int(25));
        props.insert("status".to_string(), Value::String("inactive".to_string()));
        props.insert("priority".to_string(), Value::Int(2));
        props
    });

    let charlie = graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Charlie".to_string()));
        props.insert("age".to_string(), Value::Int(35));
        props.insert("status".to_string(), Value::String("active".to_string()));
        props.insert("priority".to_string(), Value::Int(1));
        props
    });

    // Add software vertices
    let graphdb = graph.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("GraphDB".to_string()));
        props.insert("version".to_string(), Value::Float(2.0));
        props.insert("priority".to_string(), Value::Int(3));
        props
    });

    let redis = graph.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Redis".to_string()));
        props.insert("version".to_string(), Value::Float(7.0));
        props.insert("priority".to_string(), Value::Int(2));
        props
    });

    // Add edges
    let alice_knows_bob = graph
        .add_edge(alice, bob, "knows", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2020));
            props
        })
        .unwrap();

    let bob_knows_charlie = graph
        .add_edge(bob, charlie, "knows", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2021));
            props
        })
        .unwrap();

    // Alice created GraphDB
    graph
        .add_edge(alice, graphdb, "created", HashMap::new())
        .unwrap();

    // Bob created Redis
    graph
        .add_edge(bob, redis, "created", HashMap::new())
        .unwrap();

    TestGraph {
        graph,
        alice,
        bob,
        charlie,
        graphdb,
        redis: Some(redis),
        eve: None,
        alice_knows_bob,
        bob_knows_charlie,
        alice_uses_graphdb: None,
        bob_uses_graphdb: None,
        charlie_knows_alice: None,
    }
}

/// Creates a social network graph with more vertices for complex path tests.
///
/// Graph structure includes:
/// - 5 people: Alice, Bob, Charlie, Diana, Eve
/// - 2 software: GraphDB, Redis
/// - Multiple relationship types: knows, created, uses
pub fn create_social_graph() -> TestGraph {
    let graph = Graph::new();

    // People
    let alice = graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props.insert("city".to_string(), Value::String("NYC".to_string()));
        props
    });

    let bob = graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        props.insert("age".to_string(), Value::Int(25));
        props.insert("city".to_string(), Value::String("SF".to_string()));
        props
    });

    let charlie = graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Charlie".to_string()));
        props.insert("age".to_string(), Value::Int(35));
        props.insert("city".to_string(), Value::String("NYC".to_string()));
        props
    });

    let _diana = graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Diana".to_string()));
        props.insert("age".to_string(), Value::Int(28));
        props.insert("city".to_string(), Value::String("LA".to_string()));
        props
    });

    let eve = graph.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Eve".to_string()));
        props.insert("age".to_string(), Value::Int(32));
        props.insert("city".to_string(), Value::String("SF".to_string()));
        props
    });

    // Software
    let graphdb = graph.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("GraphDB".to_string()));
        props.insert("version".to_string(), Value::Float(2.0));
        props.insert("lang".to_string(), Value::String("rust".to_string()));
        props
    });

    let redis = graph.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Redis".to_string()));
        props.insert("version".to_string(), Value::Float(7.0));
        props.insert("lang".to_string(), Value::String("c".to_string()));
        props
    });

    // Edges - knows relationships
    let alice_knows_bob = graph
        .add_edge(alice, bob, "knows", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2020));
            props
        })
        .unwrap();

    let bob_knows_charlie = graph
        .add_edge(bob, charlie, "knows", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2021));
            props
        })
        .unwrap();

    graph
        .add_edge(charlie, alice, "knows", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2019));
            props
        })
        .unwrap();

    // Edges - created relationships
    graph
        .add_edge(alice, graphdb, "created", HashMap::new())
        .unwrap();

    graph
        .add_edge(bob, redis, "created", HashMap::new())
        .unwrap();

    // Edges - uses relationships
    graph
        .add_edge(charlie, graphdb, "uses", HashMap::new())
        .unwrap();

    graph.add_edge(eve, redis, "uses", HashMap::new()).unwrap();

    TestGraph {
        graph,
        alice,
        bob,
        charlie,
        graphdb,
        redis: Some(redis),
        eve: Some(eve),
        alice_knows_bob,
        bob_knows_charlie,
        alice_uses_graphdb: None,
        bob_uses_graphdb: None,
        charlie_knows_alice: None,
    }
}

/// Creates a test graph optimized for GQL tests.
///
/// Uses PascalCase labels (Person, Company) matching GQL conventions.
/// Includes Person and Company vertices for label filtering tests.
pub fn create_gql_test_graph() -> Graph {
    let graph = Graph::new();

    // Create Person vertices
    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    alice_props.insert("age".to_string(), Value::from(30i64));
    graph.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    bob_props.insert("age".to_string(), Value::from(25i64));
    graph.add_vertex("Person", bob_props);

    let mut charlie_props = HashMap::new();
    charlie_props.insert("name".to_string(), Value::from("Charlie"));
    charlie_props.insert("age".to_string(), Value::from(35i64));
    graph.add_vertex("Person", charlie_props);

    // Create Company vertices
    let mut acme_props = HashMap::new();
    acme_props.insert("name".to_string(), Value::from("Acme Corp"));
    graph.add_vertex("Company", acme_props);

    let mut globex_props = HashMap::new();
    globex_props.insert("name".to_string(), Value::from("Globex"));
    graph.add_vertex("Company", globex_props);

    graph
}

// =============================================================================
// Deprecated: Legacy COW Type Aliases
// =============================================================================
//
// These types are now deprecated. Use the unified types instead:
// - `Graph` instead of `CowGraph`
// - `GraphSnapshot` instead of `CowSnapshot`
// - `TestGraph` instead of `CowTestGraph`

/// Deprecated: Use `TestGraph` instead.
///
/// COW-based test graph with vertices and their IDs for assertions.
/// Now that `Graph` uses COW semantics by default, this is an alias for `TestGraph`.
#[deprecated(note = "Use TestGraph instead - Graph now uses COW semantics by default")]
#[allow(dead_code)]
pub type CowTestGraph = TestGraph;

/// Creates an empty COW graph for testing edge cases.
///
/// Deprecated: Use `create_empty_graph()` instead - now uses COW semantics.
#[deprecated(note = "Use create_empty_graph() instead")]
#[allow(dead_code)]
pub fn create_empty_cow_graph() -> Graph {
    Graph::new()
}

/// Creates a small COW test graph with 4 vertices and 5 edges.
///
/// Deprecated: Use `create_small_graph()` instead - now uses COW semantics.
#[deprecated(note = "Use create_small_graph() instead")]
#[allow(dead_code)]
pub fn create_small_cow_graph() -> TestGraph {
    create_small_graph()
}

/// Creates a COW test graph optimized for GQL tests.
///
/// Deprecated: Use `create_gql_test_graph()` instead - now uses COW semantics.
#[deprecated(note = "Use create_gql_test_graph() instead")]
#[allow(dead_code)]
pub fn create_gql_cow_test_graph() -> Graph {
    create_gql_test_graph()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_graph_has_expected_structure() {
        let tg = create_small_graph();
        let snapshot = tg.snapshot();
        let g = snapshot.gremlin();

        assert_eq!(g.v().to_list().len(), 4);
        assert_eq!(g.e().to_list().len(), 5);
        assert_eq!(g.v().has_label("person").to_list().len(), 3);
        assert_eq!(g.v().has_label("software").to_list().len(), 1);
    }

    #[test]
    fn medium_graph_has_expected_structure() {
        let tg = create_medium_graph();
        let snapshot = tg.snapshot();
        let g = snapshot.gremlin();

        assert_eq!(g.v().to_list().len(), 5);
        assert_eq!(g.v().has_label("person").to_list().len(), 3);
        assert_eq!(g.v().has_label("software").to_list().len(), 2);
        assert!(tg.redis.is_some());
    }

    #[test]
    fn social_graph_has_expected_structure() {
        let tg = create_social_graph();
        let snapshot = tg.snapshot();
        let g = snapshot.gremlin();

        assert_eq!(g.v().to_list().len(), 7); // 5 people + 2 software
        assert_eq!(g.v().has_label("person").to_list().len(), 5);
        assert_eq!(g.v().has_label("software").to_list().len(), 2);
    }

    #[test]
    fn gql_test_graph_has_expected_structure() {
        let graph = create_gql_test_graph();
        let snapshot = graph.snapshot();

        let results = graph.gql("MATCH (n:Person) RETURN n").unwrap();
        assert_eq!(results.len(), 3);

        let results = graph.gql("MATCH (c:Company) RETURN c").unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn builder_creates_custom_graph() {
        let graph = TestGraphBuilder::new()
            .add_person("Alice", 30)
            .add_person("Bob", 25)
            .add_software("GraphDB", "rust")
            .add_edge(0, 1, "knows")
            .add_edge(0, 2, "created")
            .build();

        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        assert_eq!(g.v().to_list().len(), 3);
        assert_eq!(g.e().to_list().len(), 2);
    }

    #[test]
    fn snapshot_is_owned_and_independent() {
        let graph = Graph::new();
        let alice = graph.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props
        });

        // Take a snapshot
        let snap1 = graph.snapshot();

        // Mutate the graph after snapshot
        graph
            .set_vertex_property(alice, "age", Value::Int(30))
            .unwrap();

        // Original snapshot doesn't see the change
        let v1 = snap1.get_vertex(alice).unwrap();
        assert!(v1.properties.get("age").is_none());

        // New snapshot sees the change
        let snap2 = graph.snapshot();
        let v2 = snap2.get_vertex(alice).unwrap();
        assert_eq!(v2.properties.get("age"), Some(&Value::Int(30)));
    }
}
