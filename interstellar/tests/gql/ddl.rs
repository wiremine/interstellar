//! GQL DDL (Data Definition Language) integration tests.
//!
//! Tests for schema DDL (CREATE/ALTER/DROP TYPE) and index DDL (CREATE/DROP INDEX).

#![allow(unused_variables)]
use interstellar::gql::{
    create_index_spec, create_index_spec_for_edge, execute_ddl, parse_statement, DdlStatement,
    Statement,
};
use interstellar::index::{ElementType, IndexBuilder, IndexType};
use interstellar::schema::{GraphSchema, SchemaError, ValidationMode};
use interstellar::storage::Graph;
use std::sync::Arc;
use std::collections::HashMap;

// =============================================================================
// Helper Functions
// =============================================================================

/// Parse a DDL statement and extract the DdlStatement.
fn parse_ddl(query: &str) -> Box<DdlStatement> {
    let stmt = parse_statement(query).expect("Failed to parse DDL");
    match stmt {
        Statement::Ddl(ddl) => ddl,
        _ => panic!("Expected DDL statement, got {:?}", stmt),
    }
}

// =============================================================================
// CREATE INDEX Parsing Tests
// =============================================================================

#[test]
fn parse_create_index_with_label() {
    let ddl = parse_ddl("CREATE INDEX idx_age ON :Person(age)");
    match *ddl {
        DdlStatement::CreateIndex(create) => {
            assert_eq!(create.name, "idx_age");
            assert_eq!(create.label, Some("Person".to_string()));
            assert_eq!(create.property, "age");
            assert!(!create.unique);
        }
        _ => panic!("Expected CreateIndex"),
    }
}

#[test]
fn parse_create_index_without_label() {
    let ddl = parse_ddl("CREATE INDEX idx_created ON (created_at)");
    match *ddl {
        DdlStatement::CreateIndex(create) => {
            assert_eq!(create.name, "idx_created");
            assert_eq!(create.label, None);
            assert_eq!(create.property, "created_at");
            assert!(!create.unique);
        }
        _ => panic!("Expected CreateIndex"),
    }
}

#[test]
fn parse_create_unique_index() {
    let ddl = parse_ddl("CREATE UNIQUE INDEX idx_email ON :User(email)");
    match *ddl {
        DdlStatement::CreateIndex(create) => {
            assert_eq!(create.name, "idx_email");
            assert_eq!(create.label, Some("User".to_string()));
            assert_eq!(create.property, "email");
            assert!(create.unique);
        }
        _ => panic!("Expected CreateIndex"),
    }
}

#[test]
fn parse_create_index_case_insensitive() {
    let ddl = parse_ddl("create unique index Idx ON :Label(prop)");
    match *ddl {
        DdlStatement::CreateIndex(create) => {
            assert_eq!(create.name, "Idx");
            assert_eq!(create.label, Some("Label".to_string()));
            assert_eq!(create.property, "prop");
            assert!(create.unique);
        }
        _ => panic!("Expected CreateIndex"),
    }
}

// =============================================================================
// DROP INDEX Parsing Tests
// =============================================================================

#[test]
fn parse_drop_index() {
    let ddl = parse_ddl("DROP INDEX idx_age");
    match *ddl {
        DdlStatement::DropIndex(drop) => {
            assert_eq!(drop.name, "idx_age");
        }
        _ => panic!("Expected DropIndex"),
    }
}

#[test]
fn parse_drop_index_case_insensitive() {
    let ddl = parse_ddl("drop index My_Index");
    match *ddl {
        DdlStatement::DropIndex(drop) => {
            assert_eq!(drop.name, "My_Index");
        }
        _ => panic!("Expected DropIndex"),
    }
}

// =============================================================================
// Index DDL Execution Tests
// =============================================================================

#[test]
fn execute_ddl_rejects_create_index() {
    let mut schema = GraphSchema::new();
    let ddl = parse_ddl("CREATE INDEX idx_age ON :Person(age)");
    let result = execute_ddl(&mut schema, &ddl);
    assert!(matches!(result, Err(SchemaError::IndexDdlNotSupported)));
}

#[test]
fn execute_ddl_rejects_drop_index() {
    let mut schema = GraphSchema::new();
    let ddl = parse_ddl("DROP INDEX idx_age");
    let result = execute_ddl(&mut schema, &ddl);
    assert!(matches!(result, Err(SchemaError::IndexDdlNotSupported)));
}

// =============================================================================
// create_index_spec() Conversion Tests
// =============================================================================

#[test]
fn create_index_spec_basic() {
    let ddl = parse_ddl("CREATE INDEX idx_age ON :Person(age)");
    let create = match *ddl {
        DdlStatement::CreateIndex(c) => c,
        _ => panic!("Expected CreateIndex"),
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
    let ddl = parse_ddl("CREATE UNIQUE INDEX uniq_email ON :User(email)");
    let create = match *ddl {
        DdlStatement::CreateIndex(c) => c,
        _ => panic!("Expected CreateIndex"),
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
    let ddl = parse_ddl("CREATE INDEX idx_ts ON (timestamp)");
    let create = match *ddl {
        DdlStatement::CreateIndex(c) => c,
        _ => panic!("Expected CreateIndex"),
    };

    let spec = create_index_spec(&create).unwrap();
    assert_eq!(spec.name, "idx_ts");
    assert_eq!(spec.element_type, ElementType::Vertex);
    assert_eq!(spec.label, None);
    assert_eq!(spec.property, "timestamp");
}

#[test]
fn create_index_spec_for_edge_basic() {
    let ddl = parse_ddl("CREATE INDEX idx_since ON :KNOWS(since)");
    let create = match *ddl {
        DdlStatement::CreateIndex(c) => c,
        _ => panic!("Expected CreateIndex"),
    };

    let spec = create_index_spec_for_edge(&create).unwrap();
    assert_eq!(spec.name, "idx_since");
    assert_eq!(spec.element_type, ElementType::Edge);
    assert_eq!(spec.label, Some("KNOWS".to_string()));
    assert_eq!(spec.property, "since");
    assert_eq!(spec.index_type, IndexType::BTree);
}

// =============================================================================
// End-to-End Index Creation via GQL DDL
// =============================================================================

#[test]
fn create_vertex_index_via_gql_ddl() {
    let graph = Arc::new(Graph::new());

    // Add some data first
    for age in 20..30 {
        let mut props = HashMap::new();
        props.insert("age".to_string(), interstellar::value::Value::Int(age));
        graph.add_vertex("person", props);
    }

    // Parse GQL DDL and create index
    let ddl = parse_ddl("CREATE INDEX idx_person_age ON :person(age)");
    match *ddl {
        DdlStatement::CreateIndex(create) => {
            let spec = create_index_spec(&create).unwrap();
            graph.create_index(spec).unwrap();
        }
        _ => panic!("Expected CreateIndex"),
    }

    // Verify index exists
    assert!(graph.has_index("idx_person_age"));

    // Verify index works
    let results: Vec<_> = graph
        .vertices_by_property(Some("person"), "age", &interstellar::value::Value::Int(25))
        .collect();
    assert_eq!(results.len(), 1);
}

#[test]
fn create_unique_index_via_gql_ddl() {
    let graph = Arc::new(Graph::new());

    // Parse GQL DDL and create unique index first (before data)
    let ddl = parse_ddl("CREATE UNIQUE INDEX uniq_user_email ON :user(email)");
    match *ddl {
        DdlStatement::CreateIndex(create) => {
            let spec = create_index_spec(&create).unwrap();
            graph.create_index(spec).unwrap();
        }
        _ => panic!("Expected CreateIndex"),
    }

    // Add users with unique emails
    for i in 1..=5 {
        let mut props = HashMap::new();
        props.insert(
            "email".to_string(),
            interstellar::value::Value::String(format!("user{}@example.com", i)),
        );
        graph.add_vertex("user", props);
    }

    // Verify unique index works
    let results: Vec<_> = graph
        .vertices_by_property(
            Some("user"),
            "email",
            &interstellar::value::Value::String("user3@example.com".into()),
        )
        .collect();
    assert_eq!(results.len(), 1);
}

#[test]
fn create_edge_index_via_gql_ddl() {
    let graph = Arc::new(Graph::new());

    // Add vertices and edges
    let v1 = graph.add_vertex("person", HashMap::new());
    let v2 = graph.add_vertex("person", HashMap::new());

    for weight in 1..=5 {
        let mut props = HashMap::new();
        props.insert(
            "weight".to_string(),
            interstellar::value::Value::Int(weight),
        );
        graph.add_edge(v1, v2, "knows", props).unwrap();
    }

    // Parse GQL DDL and create edge index
    let ddl = parse_ddl("CREATE INDEX idx_knows_weight ON :knows(weight)");
    match *ddl {
        DdlStatement::CreateIndex(create) => {
            let spec = create_index_spec_for_edge(&create).unwrap();
            graph.create_index(spec).unwrap();
        }
        _ => panic!("Expected CreateIndex"),
    }

    // Verify edge index works
    let results: Vec<_> = graph
        .edges_by_property(Some("knows"), "weight", &interstellar::value::Value::Int(3))
        .collect();
    assert_eq!(results.len(), 1);
}

#[test]
fn drop_index_via_gql_ddl() {
    let graph = Arc::new(Graph::new());

    // Create an index using the builder API
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("person")
                .property("age")
                .name("idx_to_drop")
                .build()
                .unwrap(),
        )
        .unwrap();

    assert!(graph.has_index("idx_to_drop"));

    // Parse DROP INDEX DDL and drop the index
    let ddl = parse_ddl("DROP INDEX idx_to_drop");
    match *ddl {
        DdlStatement::DropIndex(drop) => {
            graph.drop_index(&drop.name).unwrap();
        }
        _ => panic!("Expected DropIndex"),
    }

    assert!(!graph.has_index("idx_to_drop"));
}

// =============================================================================
// Schema DDL Tests (existing functionality)
// =============================================================================

#[test]
fn create_node_type_via_gql_ddl() {
    let mut schema = GraphSchema::new();
    let ddl = parse_ddl("CREATE NODE TYPE Person (name STRING NOT NULL, age INT)");
    execute_ddl(&mut schema, &ddl).unwrap();

    assert!(schema.has_vertex_schema("Person"));
    let vs = schema.vertex_schema("Person").unwrap();
    assert_eq!(vs.properties.len(), 2);
}

#[test]
fn create_edge_type_via_gql_ddl() {
    let mut schema = GraphSchema::new();
    let ddl = parse_ddl("CREATE EDGE TYPE KNOWS (since INT) FROM Person TO Person");
    execute_ddl(&mut schema, &ddl).unwrap();

    assert!(schema.has_edge_schema("KNOWS"));
    let es = schema.edge_schema("KNOWS").unwrap();
    assert_eq!(es.from_labels, vec!["Person"]);
    assert_eq!(es.to_labels, vec!["Person"]);
}

#[test]
fn set_validation_mode_via_gql_ddl() {
    let mut schema = GraphSchema::new();
    let ddl = parse_ddl("SET SCHEMA VALIDATION STRICT");
    execute_ddl(&mut schema, &ddl).unwrap();

    assert_eq!(schema.mode, ValidationMode::Strict);
}
