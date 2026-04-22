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
use std::sync::Arc;

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
    pub graph: Arc<Graph>,
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
    graph: Arc<Graph>,
    vertices: Vec<VertexId>,
}

#[allow(dead_code)]
impl TestGraphBuilder {
    pub fn new() -> Self {
        TestGraphBuilder {
            graph: Arc::new(Graph::new()),
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
    pub fn add_edge(self, from_idx: usize, to_idx: usize, label: &str) -> Self {
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
        self,
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
    pub fn build(self) -> Arc<Graph> {
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
#[allow(dead_code)]
pub fn create_empty_graph() -> Arc<Graph> {
    Arc::new(Graph::new())
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
    let graph = Arc::new(Graph::new());

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
    let graph = Arc::new(Graph::new());

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
    let graph = Arc::new(Graph::new());

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
pub fn create_gql_test_graph() -> Arc<Graph> {
    let graph = Arc::new(Graph::new());

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
// Phase 6: Additional Test Fixtures
// =============================================================================

/// Organizational hierarchy test graph for recursive pattern testing.
///
/// Structure:
/// ```text
///   CEO (level=0)
///   ├── CTO (level=1)
///   │   ├── Eng Manager 1 (level=2)
///   │   │   ├── Developer 1 (level=3)
///   │   │   └── Developer 2 (level=3)
///   │   └── Eng Manager 2 (level=2)
///   │       └── Developer 3 (level=3)
///   └── CFO (level=1)
///       └── Finance Manager (level=2)
///           └── Accountant (level=3)
/// ```
///
/// Edges use "reports_to" label (child -> parent direction).
#[allow(dead_code)]
pub struct OrgTestGraph {
    pub graph: Arc<Graph>,
    pub ceo: VertexId,
    pub cto: VertexId,
    pub cfo: VertexId,
    pub eng_mgr1: VertexId,
    pub eng_mgr2: VertexId,
    pub fin_mgr: VertexId,
    pub dev1: VertexId,
    pub dev2: VertexId,
    pub dev3: VertexId,
    pub accountant: VertexId,
}

impl OrgTestGraph {
    /// Access the graph traversal source via a snapshot.
    ///
    /// # Example
    /// ```ignore
    /// let org = create_org_graph();
    /// let snapshot = org.snapshot();
    /// let results = snapshot.gremlin().v().has_label("employee").to_list();
    /// ```
    pub fn snapshot(&self) -> GraphSnapshot {
        self.graph.snapshot()
    }
}

/// Creates an organizational hierarchy for testing recursive patterns.
#[allow(dead_code)]
pub fn create_org_graph() -> OrgTestGraph {
    let graph = Arc::new(Graph::new());

    // Level 0: CEO
    let ceo = graph.add_vertex("employee", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("CEO".to_string()));
        props.insert("level".to_string(), Value::Int(0));
        props.insert(
            "title".to_string(),
            Value::String("Chief Executive Officer".to_string()),
        );
        props.insert("salary".to_string(), Value::Int(500000));
        props
    });

    // Level 1: CTO and CFO
    let cto = graph.add_vertex("employee", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("CTO".to_string()));
        props.insert("level".to_string(), Value::Int(1));
        props.insert(
            "title".to_string(),
            Value::String("Chief Technology Officer".to_string()),
        );
        props.insert("salary".to_string(), Value::Int(350000));
        props
    });

    let cfo = graph.add_vertex("employee", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("CFO".to_string()));
        props.insert("level".to_string(), Value::Int(1));
        props.insert(
            "title".to_string(),
            Value::String("Chief Financial Officer".to_string()),
        );
        props.insert("salary".to_string(), Value::Int(350000));
        props
    });

    // Level 2: Managers
    let eng_mgr1 = graph.add_vertex("employee", {
        let mut props = HashMap::new();
        props.insert(
            "name".to_string(),
            Value::String("Eng Manager 1".to_string()),
        );
        props.insert("level".to_string(), Value::Int(2));
        props.insert(
            "title".to_string(),
            Value::String("Engineering Manager".to_string()),
        );
        props.insert("salary".to_string(), Value::Int(200000));
        props
    });

    let eng_mgr2 = graph.add_vertex("employee", {
        let mut props = HashMap::new();
        props.insert(
            "name".to_string(),
            Value::String("Eng Manager 2".to_string()),
        );
        props.insert("level".to_string(), Value::Int(2));
        props.insert(
            "title".to_string(),
            Value::String("Engineering Manager".to_string()),
        );
        props.insert("salary".to_string(), Value::Int(200000));
        props
    });

    let fin_mgr = graph.add_vertex("employee", {
        let mut props = HashMap::new();
        props.insert(
            "name".to_string(),
            Value::String("Finance Manager".to_string()),
        );
        props.insert("level".to_string(), Value::Int(2));
        props.insert(
            "title".to_string(),
            Value::String("Finance Manager".to_string()),
        );
        props.insert("salary".to_string(), Value::Int(180000));
        props
    });

    // Level 3: Individual contributors
    let dev1 = graph.add_vertex("employee", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Developer 1".to_string()));
        props.insert("level".to_string(), Value::Int(3));
        props.insert(
            "title".to_string(),
            Value::String("Senior Developer".to_string()),
        );
        props.insert("salary".to_string(), Value::Int(150000));
        props
    });

    let dev2 = graph.add_vertex("employee", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Developer 2".to_string()));
        props.insert("level".to_string(), Value::Int(3));
        props.insert("title".to_string(), Value::String("Developer".to_string()));
        props.insert("salary".to_string(), Value::Int(120000));
        props
    });

    let dev3 = graph.add_vertex("employee", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Developer 3".to_string()));
        props.insert("level".to_string(), Value::Int(3));
        props.insert("title".to_string(), Value::String("Developer".to_string()));
        props.insert("salary".to_string(), Value::Int(120000));
        props
    });

    let accountant = graph.add_vertex("employee", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Accountant".to_string()));
        props.insert("level".to_string(), Value::Int(3));
        props.insert(
            "title".to_string(),
            Value::String("Staff Accountant".to_string()),
        );
        props.insert("salary".to_string(), Value::Int(90000));
        props
    });

    // reports_to edges (child -> parent)
    graph
        .add_edge(cto, ceo, "reports_to", HashMap::new())
        .unwrap();
    graph
        .add_edge(cfo, ceo, "reports_to", HashMap::new())
        .unwrap();
    graph
        .add_edge(eng_mgr1, cto, "reports_to", HashMap::new())
        .unwrap();
    graph
        .add_edge(eng_mgr2, cto, "reports_to", HashMap::new())
        .unwrap();
    graph
        .add_edge(fin_mgr, cfo, "reports_to", HashMap::new())
        .unwrap();
    graph
        .add_edge(dev1, eng_mgr1, "reports_to", HashMap::new())
        .unwrap();
    graph
        .add_edge(dev2, eng_mgr1, "reports_to", HashMap::new())
        .unwrap();
    graph
        .add_edge(dev3, eng_mgr2, "reports_to", HashMap::new())
        .unwrap();
    graph
        .add_edge(accountant, fin_mgr, "reports_to", HashMap::new())
        .unwrap();

    OrgTestGraph {
        graph,
        ceo,
        cto,
        cfo,
        eng_mgr1,
        eng_mgr2,
        fin_mgr,
        dev1,
        dev2,
        dev3,
        accountant,
    }
}

/// Creates a densely connected graph for stress testing.
///
/// - `vertex_count` vertices labeled "node"
/// - Each vertex connects to approximately `edge_probability * vertex_count` other vertices
/// - Uses deterministic edge creation based on modular arithmetic
///
/// # Example
/// ```ignore
/// let graph = create_dense_graph(100, 0.2); // 100 vertices, ~20 edges each
/// ```
#[allow(dead_code)]
pub fn create_dense_graph(vertex_count: usize, edges_per_vertex: usize) -> Arc<Graph> {
    let graph = Arc::new(Graph::new());
    let mut ids = Vec::with_capacity(vertex_count);

    // Create vertices
    for i in 0..vertex_count {
        let mut props = HashMap::new();
        props.insert("index".to_string(), Value::Int(i as i64));
        props.insert("name".to_string(), Value::String(format!("node_{}", i)));
        let id = graph.add_vertex("node", props);
        ids.push(id);
    }

    // Create edges deterministically
    for i in 0..vertex_count {
        for j in 1..=edges_per_vertex {
            let target = (i + j) % vertex_count;
            if target != i {
                let mut props = HashMap::new();
                props.insert(
                    "weight".to_string(),
                    Value::Float((j as f64) / (edges_per_vertex as f64)),
                );
                let _ = graph.add_edge(ids[i], ids[target], "connects", props);
            }
        }
    }

    graph
}

/// Creates a graph with diverse property types for type handling tests.
///
/// Includes vertices with:
/// - String, Integer, Float, Boolean properties
/// - Null values
/// - Missing properties (some vertices lack certain keys)
/// - Lists and Maps as property values
#[allow(dead_code)]
pub fn create_property_test_graph() -> Arc<Graph> {
    let graph = Arc::new(Graph::new());

    // Vertex with all property types
    graph.add_vertex("complete", {
        let mut props = HashMap::new();
        props.insert(
            "string_prop".to_string(),
            Value::String("hello".to_string()),
        );
        props.insert("int_prop".to_string(), Value::Int(42));
        props.insert("float_prop".to_string(), Value::Float(2.718));
        props.insert("bool_prop".to_string(), Value::Bool(true));
        props.insert("null_prop".to_string(), Value::Null);
        props.insert(
            "list_prop".to_string(),
            Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
        );
        props.insert(
            "map_prop".to_string(),
            Value::Map({
                let mut map = interstellar::value::ValueMap::new();
                map.insert(
                    "nested_key".to_string(),
                    Value::String("nested_value".to_string()),
                );
                map
            }),
        );
        props
    });

    // Vertex with only string properties
    graph.add_vertex("strings_only", {
        let mut props = HashMap::new();
        props.insert(
            "name".to_string(),
            Value::String("StringVertex".to_string()),
        );
        props.insert(
            "description".to_string(),
            Value::String("A vertex with only strings".to_string()),
        );
        props.insert("empty_string".to_string(), Value::String("".to_string()));
        props.insert(
            "unicode".to_string(),
            Value::String("こんにちは 🌍".to_string()),
        );
        props
    });

    // Vertex with only numeric properties
    graph.add_vertex("numbers_only", {
        let mut props = HashMap::new();
        props.insert("positive_int".to_string(), Value::Int(100));
        props.insert("negative_int".to_string(), Value::Int(-50));
        props.insert("zero".to_string(), Value::Int(0));
        props.insert("large_int".to_string(), Value::Int(i64::MAX));
        props.insert("small_int".to_string(), Value::Int(i64::MIN));
        props.insert("positive_float".to_string(), Value::Float(123.456));
        props.insert("negative_float".to_string(), Value::Float(-789.012));
        props.insert("tiny_float".to_string(), Value::Float(0.000001));
        props
    });

    // Vertex with boolean properties
    graph.add_vertex("booleans", {
        let mut props = HashMap::new();
        props.insert("is_active".to_string(), Value::Bool(true));
        props.insert("is_deleted".to_string(), Value::Bool(false));
        props.insert("flag".to_string(), Value::Bool(true));
        props
    });

    // Vertex with null/missing properties
    graph.add_vertex("sparse", {
        let mut props = HashMap::new();
        props.insert("only_one".to_string(), Value::String("value".to_string()));
        props.insert("null_value".to_string(), Value::Null);
        props
    });

    // Vertex with nested structures
    graph.add_vertex("nested", {
        let mut props = HashMap::new();
        props.insert(
            "deep_list".to_string(),
            Value::List(vec![
                Value::List(vec![Value::Int(1), Value::Int(2)]),
                Value::List(vec![Value::Int(3), Value::Int(4)]),
            ]),
        );
        props.insert(
            "deep_map".to_string(),
            Value::Map({
                let mut outer = interstellar::value::ValueMap::new();
                outer.insert(
                    "inner".to_string(),
                    Value::Map({
                        let mut inner = interstellar::value::ValueMap::new();
                        inner.insert("value".to_string(), Value::Int(999));
                        inner
                    }),
                );
                outer
            }),
        );
        props
    });

    // Add edges between vertices
    let vertices: Vec<_> = graph.snapshot().all_vertices().map(|v| v.id).collect();
    for i in 0..vertices.len() {
        for j in (i + 1)..vertices.len() {
            let mut props = HashMap::new();
            props.insert("edge_index".to_string(), Value::Int((i * 10 + j) as i64));
            let _ = graph.add_edge(vertices[i], vertices[j], "relates_to", props);
        }
    }

    graph
}

/// Creates a parameterized large graph for performance testing.
///
/// # Arguments
/// * `vertex_count` - Number of vertices to create
/// * `edges_per_vertex` - Average number of outgoing edges per vertex
///
/// # Returns
/// A graph with the specified number of vertices and edges.
///
/// # Example
/// ```ignore
/// let graph = create_large_graph(10_000, 5); // 10k vertices, ~50k edges
/// ```
#[allow(dead_code)]
pub fn create_large_graph(vertex_count: usize, edges_per_vertex: usize) -> Arc<Graph> {
    let graph = Arc::new(Graph::new());
    let mut ids = Vec::with_capacity(vertex_count);

    // Create vertices with varied properties
    for i in 0..vertex_count {
        let mut props = HashMap::new();
        props.insert("index".to_string(), Value::Int(i as i64));
        props.insert("name".to_string(), Value::String(format!("vertex_{}", i)));
        props.insert(
            "group".to_string(),
            Value::String(format!("group_{}", i % 10)),
        );
        props.insert("priority".to_string(), Value::Int((i % 5) as i64));
        props.insert("score".to_string(), Value::Float((i as f64) * 0.1));
        props.insert("active".to_string(), Value::Bool(i % 2 == 0));

        let label = match i % 3 {
            0 => "type_a",
            1 => "type_b",
            _ => "type_c",
        };
        let id = graph.add_vertex(label, props);
        ids.push(id);
    }

    // Create edges with deterministic pattern
    for i in 0..vertex_count {
        for j in 1..=edges_per_vertex {
            let target = (i + j * 7) % vertex_count; // Prime multiplier for better distribution
            if target != i {
                let mut props = HashMap::new();
                props.insert("weight".to_string(), Value::Float((j as f64) / 10.0));

                let label = match j % 3 {
                    0 => "edge_a",
                    1 => "edge_b",
                    _ => "edge_c",
                };
                let _ = graph.add_edge(ids[i], ids[target], label, props);
            }
        }
    }

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
pub fn create_empty_cow_graph() -> Arc<Graph> {
    Arc::new(Graph::new())
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
pub fn create_gql_cow_test_graph() -> Arc<Graph> {
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
    #[cfg(feature = "gql")]
    fn gql_test_graph_has_expected_structure() {
        let graph = create_gql_test_graph();
        let _snapshot = graph.snapshot();

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
        let graph = Arc::new(Graph::new());
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
        assert!(!v1.properties.contains_key("age"));

        // New snapshot sees the change
        let snap2 = graph.snapshot();
        let v2 = snap2.get_vertex(alice).unwrap();
        assert_eq!(v2.properties.get("age"), Some(&Value::Int(30)));
    }

    // =========================================================================
    // Phase 6: Additional Test Fixture Tests
    // =========================================================================

    #[test]
    fn org_graph_has_expected_structure() {
        let org = create_org_graph();
        let snapshot = org.snapshot();
        let g = snapshot.gremlin();

        // Verify vertex count: 10 employees total
        assert_eq!(g.v().to_list().len(), 10);
        assert_eq!(g.v().has_label("employee").to_list().len(), 10);

        // Verify edge count: 9 reports_to edges (tree structure)
        assert_eq!(g.e().to_list().len(), 9);
        assert_eq!(g.e().has_label("reports_to").to_list().len(), 9);
    }

    #[test]
    fn org_graph_has_correct_hierarchy_levels() {
        let org = create_org_graph();
        let snapshot = org.snapshot();
        let g = snapshot.gremlin();

        // Level 0: 1 CEO
        let level0 = g.v().has_value("level", 0i64).to_list();
        assert_eq!(level0.len(), 1);

        // Level 1: 2 executives (CTO, CFO)
        let level1 = g.v().has_value("level", 1i64).to_list();
        assert_eq!(level1.len(), 2);

        // Level 2: 3 managers
        let level2 = g.v().has_value("level", 2i64).to_list();
        assert_eq!(level2.len(), 3);

        // Level 3: 4 individual contributors
        let level3 = g.v().has_value("level", 3i64).to_list();
        assert_eq!(level3.len(), 4);
    }

    #[test]
    fn org_graph_reports_to_edges_correct() {
        let org = create_org_graph();
        let snapshot = org.snapshot();
        let g = snapshot.gremlin();

        // CTO reports to CEO
        let cto_reports = g.v_ids([org.cto]).out_labels(&["reports_to"]).to_list();
        assert_eq!(cto_reports.len(), 1);
        assert_eq!(cto_reports[0].as_vertex_id(), Some(org.ceo));

        // Developers report to managers
        let dev1_reports = g.v_ids([org.dev1]).out_labels(&["reports_to"]).to_list();
        assert_eq!(dev1_reports.len(), 1);
        assert_eq!(dev1_reports[0].as_vertex_id(), Some(org.eng_mgr1));
    }

    #[test]
    fn org_graph_recursive_traversal() {
        let org = create_org_graph();
        let snapshot = org.snapshot();
        let g = snapshot.gremlin();

        // Developer 1 -> Eng Manager 1 -> CTO -> CEO (3 hops)
        let chain = g
            .v_ids([org.dev1])
            .out_labels(&["reports_to"])
            .out_labels(&["reports_to"])
            .out_labels(&["reports_to"])
            .to_list();
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].as_vertex_id(), Some(org.ceo));
    }

    #[test]
    fn dense_graph_has_expected_structure() {
        let vertex_count = 50;
        let edges_per_vertex = 5;
        let graph = create_dense_graph(vertex_count, edges_per_vertex);
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Verify vertex count
        assert_eq!(g.v().to_list().len(), vertex_count);
        assert_eq!(g.v().has_label("node").to_list().len(), vertex_count);

        // Verify edge count: each vertex has edges_per_vertex outgoing edges
        // (assuming no self-loops eliminated)
        let edge_count = g.e().to_list().len();
        assert!(edge_count > 0);
        assert!(edge_count <= vertex_count * edges_per_vertex);
    }

    #[test]
    fn dense_graph_vertices_have_properties() {
        let graph = create_dense_graph(10, 3);
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // All vertices should have index and name properties
        let indices = g.v().values("index").to_list();
        assert_eq!(indices.len(), 10);

        let names = g.v().values("name").to_list();
        assert_eq!(names.len(), 10);

        // Verify index values are 0..10
        let mut index_vals: Vec<i64> = indices.iter().filter_map(|v| v.as_i64()).collect();
        index_vals.sort();
        assert_eq!(index_vals, (0..10).collect::<Vec<i64>>());
    }

    #[test]
    fn dense_graph_edges_have_weights() {
        let graph = create_dense_graph(10, 3);
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // All edges should have weight properties
        let weights = g.e().values("weight").to_list();
        let edge_count = g.e().to_list().len();
        assert_eq!(weights.len(), edge_count);

        // Weights should be floats between 0 and 1
        for weight in &weights {
            let w = weight.as_f64().expect("weight should be float");
            assert!(w > 0.0 && w <= 1.0, "weight {} out of range", w);
        }
    }

    #[test]
    fn property_test_graph_has_expected_structure() {
        let graph = create_property_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // 6 vertices with different property configurations
        assert_eq!(g.v().to_list().len(), 6);

        // Verify label diversity
        assert_eq!(g.v().has_label("complete").to_list().len(), 1);
        assert_eq!(g.v().has_label("strings_only").to_list().len(), 1);
        assert_eq!(g.v().has_label("numbers_only").to_list().len(), 1);
        assert_eq!(g.v().has_label("booleans").to_list().len(), 1);
        assert_eq!(g.v().has_label("sparse").to_list().len(), 1);
        assert_eq!(g.v().has_label("nested").to_list().len(), 1);
    }

    #[test]
    fn property_test_graph_complete_vertex_has_all_types() {
        let graph = create_property_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Get the complete vertex
        let complete = g.v().has_label("complete").to_list();
        assert_eq!(complete.len(), 1);

        let vid = complete[0].as_vertex_id().unwrap();
        let vertex = snapshot.get_vertex(vid).unwrap();

        // Verify all property types present
        assert!(matches!(
            vertex.properties.get("string_prop"),
            Some(Value::String(_))
        ));
        assert!(matches!(
            vertex.properties.get("int_prop"),
            Some(Value::Int(_))
        ));
        assert!(matches!(
            vertex.properties.get("float_prop"),
            Some(Value::Float(_))
        ));
        assert!(matches!(
            vertex.properties.get("bool_prop"),
            Some(Value::Bool(_))
        ));
        assert!(matches!(
            vertex.properties.get("null_prop"),
            Some(Value::Null)
        ));
        assert!(matches!(
            vertex.properties.get("list_prop"),
            Some(Value::List(_))
        ));
        assert!(matches!(
            vertex.properties.get("map_prop"),
            Some(Value::Map(_))
        ));
    }

    #[test]
    fn property_test_graph_numbers_vertex_has_edge_cases() {
        let graph = create_property_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let numbers = g.v().has_label("numbers_only").to_list();
        assert_eq!(numbers.len(), 1);

        let vid = numbers[0].as_vertex_id().unwrap();
        let vertex = snapshot.get_vertex(vid).unwrap();

        // Verify numeric edge cases
        assert_eq!(
            vertex.properties.get("positive_int"),
            Some(&Value::Int(100))
        );
        assert_eq!(
            vertex.properties.get("negative_int"),
            Some(&Value::Int(-50))
        );
        assert_eq!(vertex.properties.get("zero"), Some(&Value::Int(0)));
        assert_eq!(
            vertex.properties.get("large_int"),
            Some(&Value::Int(i64::MAX))
        );
        assert_eq!(
            vertex.properties.get("small_int"),
            Some(&Value::Int(i64::MIN))
        );
    }

    #[test]
    fn property_test_graph_sparse_vertex_has_missing_properties() {
        let graph = create_property_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let sparse = g.v().has_label("sparse").to_list();
        assert_eq!(sparse.len(), 1);

        let vid = sparse[0].as_vertex_id().unwrap();
        let vertex = snapshot.get_vertex(vid).unwrap();

        // Should have only 2 properties
        assert_eq!(vertex.properties.len(), 2);
        assert!(vertex.properties.contains_key("only_one"));
        assert!(vertex.properties.contains_key("null_value"));
    }

    #[test]
    fn large_graph_has_expected_structure() {
        let vertex_count = 100;
        let edges_per_vertex = 3;
        let graph = create_large_graph(vertex_count, edges_per_vertex);
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Verify vertex count
        assert_eq!(g.v().to_list().len(), vertex_count);

        // Verify label distribution (3 types evenly distributed)
        let type_a = g.v().has_label("type_a").to_list().len();
        let type_b = g.v().has_label("type_b").to_list().len();
        let type_c = g.v().has_label("type_c").to_list().len();
        assert_eq!(type_a + type_b + type_c, vertex_count);

        // Each type should have roughly 1/3 of vertices
        assert!(type_a >= vertex_count / 4);
        assert!(type_b >= vertex_count / 4);
        assert!(type_c >= vertex_count / 4);
    }

    #[test]
    fn large_graph_vertices_have_varied_properties() {
        let graph = create_large_graph(50, 2);
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // All vertices should have these properties
        assert_eq!(g.v().values("index").to_list().len(), 50);
        assert_eq!(g.v().values("name").to_list().len(), 50);
        assert_eq!(g.v().values("group").to_list().len(), 50);
        assert_eq!(g.v().values("priority").to_list().len(), 50);
        assert_eq!(g.v().values("score").to_list().len(), 50);
        assert_eq!(g.v().values("active").to_list().len(), 50);

        // Verify priority distribution (0-4)
        for p in 0..5 {
            let count = g.v().has_value("priority", p as i64).to_list().len();
            assert_eq!(count, 10, "Expected 10 vertices with priority {}", p);
        }
    }

    #[test]
    fn large_graph_edge_labels_distributed() {
        let graph = create_large_graph(30, 3);
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Should have 3 different edge labels
        let edge_a = g.e().has_label("edge_a").to_list().len();
        let edge_b = g.e().has_label("edge_b").to_list().len();
        let edge_c = g.e().has_label("edge_c").to_list().len();

        let total = edge_a + edge_b + edge_c;
        assert!(total > 0);
        assert_eq!(total, g.e().to_list().len());
    }

    #[test]
    fn large_graph_scales_correctly() {
        // Test with different sizes to ensure scaling works
        for (v_count, e_per_v) in [(10, 2), (100, 5), (500, 10)] {
            let graph = create_large_graph(v_count, e_per_v);
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            assert_eq!(
                g.v().to_list().len(),
                v_count,
                "Vertex count mismatch for size {}",
                v_count
            );

            let edge_count = g.e().to_list().len();
            // Edge count should be approximately v_count * e_per_v
            // (may be less due to self-loop prevention)
            assert!(
                edge_count > 0 && edge_count <= v_count * e_per_v,
                "Edge count {} out of expected range for {} vertices with {} edges each",
                edge_count,
                v_count,
                e_per_v
            );
        }
    }
}
