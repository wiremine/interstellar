# RDF Store Extension

This document describes the modifications needed to use Interstellar as an RDF (Resource Description Framework) triple/quad store.

## Overview

RDF is a W3C standard for representing knowledge as a graph of triples in the form:

```
(Subject, Predicate, Object)
```

Where:
- **Subject**: A resource (URI/IRI) or blank node
- **Predicate**: A property (always a URI/IRI)
- **Object**: A resource, blank node, or literal value

Interstellar's property graph model has structural similarities but key differences that must be addressed.

## Current State

### Structural Mapping

| RDF Concept | Interstellar Equivalent | Location |
|-------------|----------------------|----------|
| Subject | `edge.src` (VertexId) | `src/storage/mod.rs:149` |
| Predicate | `edge.label` (String) | `src/storage/mod.rs:147` |
| Object (resource) | `edge.dst` (VertexId) | `src/storage/mod.rs:151` |
| Object (literal) | ❌ Not supported | - |
| URI/IRI | ❌ Not supported | - |
| Blank node | Anonymous vertex | `src/storage/inmemory.rs` |
| Named graph | ❌ Not supported | - |
| Datatype | `Value` enum (partial) | `src/value.rs:223-242` |

### Fundamental Gaps

1. **Literal objects** - RDF allows edges to point to literal values; Interstellar requires vertex destinations
2. **URI/IRI type** - No first-class support for URIs as identifiers
3. **RDF datatypes** - Missing XSD type system (xsd:date, xsd:decimal, etc.)
4. **Named graphs** - No quad (context) support for RDF datasets
5. **Language tags** - No support for `"hello"@en` style literals
6. **Query language** - SPARQL not supported; only Gremlin traversals

## Required Modifications

### Phase 1: URI/IRI Support

Add first-class URI support to the value system.

#### 1.1 Extend Value Enum

```rust
// src/value.rs

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<Value>),
    Map(HashMap<String, Value>),
    Vertex(VertexId),
    Edge(EdgeId),
    
    // New RDF-specific variants
    /// IRI (Internationalized Resource Identifier)
    Iri(Iri),
    /// Blank node identifier
    BlankNode(BlankNodeId),
    /// Typed literal with datatype IRI
    TypedLiteral {
        value: String,
        datatype: Iri,
    },
    /// Language-tagged string
    LangString {
        value: String,
        language: String,  // e.g., "en", "fr", "de"
    },
}
```

#### 1.2 IRI Type

```rust
/// Internationalized Resource Identifier
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Iri {
    /// The full IRI string
    value: String,
}

impl Iri {
    pub fn new(value: impl Into<String>) -> Result<Self, IriError> {
        let value = value.into();
        // Validate IRI syntax per RFC 3987
        Self::validate(&value)?;
        Ok(Self { value })
    }
    
    /// Create from prefix and local name
    /// e.g., ("http://example.org/", "Person") -> "http://example.org/Person"
    pub fn from_prefixed(prefix: &str, local: &str) -> Self;
    
    /// Get the IRI string
    pub fn as_str(&self) -> &str {
        &self.value
    }
}

/// Well-known IRI prefixes
pub mod prefix {
    pub const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    pub const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";
    pub const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
    pub const OWL: &str = "http://www.w3.org/2002/07/owl#";
    pub const FOAF: &str = "http://xmlns.com/foaf/0.1/";
    pub const DC: &str = "http://purl.org/dc/elements/1.1/";
}
```

#### 1.3 Blank Node ID

```rust
/// Blank node identifier (anonymous resource)
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BlankNodeId(String);

impl BlankNodeId {
    pub fn new() -> Self {
        // Generate unique ID like "_:b1", "_:b2", etc.
        Self(format!("_:b{}", uuid::Uuid::new_v4()))
    }
    
    pub fn from_label(label: &str) -> Self {
        Self(format!("_:{}", label))
    }
}
```

### Phase 2: RDF Datatype System

Implement XSD datatypes for typed literals.

#### 2.1 XSD Types

```rust
pub mod xsd {
    use super::Iri;
    
    lazy_static! {
        pub static ref STRING: Iri = Iri::new("http://www.w3.org/2001/XMLSchema#string").unwrap();
        pub static ref BOOLEAN: Iri = Iri::new("http://www.w3.org/2001/XMLSchema#boolean").unwrap();
        pub static ref INTEGER: Iri = Iri::new("http://www.w3.org/2001/XMLSchema#integer").unwrap();
        pub static ref DECIMAL: Iri = Iri::new("http://www.w3.org/2001/XMLSchema#decimal").unwrap();
        pub static ref FLOAT: Iri = Iri::new("http://www.w3.org/2001/XMLSchema#float").unwrap();
        pub static ref DOUBLE: Iri = Iri::new("http://www.w3.org/2001/XMLSchema#double").unwrap();
        pub static ref DATE: Iri = Iri::new("http://www.w3.org/2001/XMLSchema#date").unwrap();
        pub static ref DATETIME: Iri = Iri::new("http://www.w3.org/2001/XMLSchema#dateTime").unwrap();
        pub static ref TIME: Iri = Iri::new("http://www.w3.org/2001/XMLSchema#time").unwrap();
        pub static ref DURATION: Iri = Iri::new("http://www.w3.org/2001/XMLSchema#duration").unwrap();
        pub static ref ANY_URI: Iri = Iri::new("http://www.w3.org/2001/XMLSchema#anyURI").unwrap();
    }
}
```

#### 2.2 Literal Validation and Conversion

```rust
impl Value {
    /// Create a typed literal with validation
    pub fn typed_literal(value: &str, datatype: &Iri) -> Result<Self, DatatypeError> {
        // Validate value against datatype
        validate_datatype(value, datatype)?;
        
        Ok(Value::TypedLiteral {
            value: value.to_string(),
            datatype: datatype.clone(),
        })
    }
    
    /// Convert typed literal to native Rust type
    pub fn to_native<T: FromRdfLiteral>(&self) -> Result<T, ConversionError> {
        T::from_rdf_literal(self)
    }
}

pub trait FromRdfLiteral: Sized {
    fn from_rdf_literal(value: &Value) -> Result<Self, ConversionError>;
}

impl FromRdfLiteral for i64 {
    fn from_rdf_literal(value: &Value) -> Result<Self, ConversionError> {
        match value {
            Value::Int(n) => Ok(*n),
            Value::TypedLiteral { value, datatype } if *datatype == *xsd::INTEGER => {
                value.parse().map_err(|_| ConversionError::InvalidFormat)
            }
            _ => Err(ConversionError::TypeMismatch),
        }
    }
}
```

### Phase 3: Triple Store Model

Create an RDF-native interface on top of the property graph.

#### 3.1 Triple Representation

Two approaches are possible:

**Approach A: Edge-based (recommended)**

Map triples directly to edges where both subject and object are vertices:

```rust
pub struct Triple {
    pub subject: RdfNode,
    pub predicate: Iri,
    pub object: RdfNode,
}

pub enum RdfNode {
    /// IRI resource (stored as vertex with IRI property)
    Iri(Iri),
    /// Blank node (stored as vertex)
    BlankNode(BlankNodeId),
    /// Literal value (stored as vertex with special label)
    Literal(Value),
}
```

Internal storage:
- IRIs and blank nodes → vertices with `__rdf_iri` or `__rdf_blank` labels
- Literals → vertices with `__rdf_literal` label
- Predicates → edge labels (interned IRIs)

**Approach B: Reification**

Model each triple as a vertex with subject/predicate/object properties:

```rust
// Each triple becomes a vertex
{
    label: "__rdf_triple",
    properties: {
        "subject": Iri("http://example.org/Alice"),
        "predicate": Iri("http://example.org/knows"),
        "object": Iri("http://example.org/Bob"),
    }
}
```

This is less efficient but simplifies literal objects.

#### 3.2 RDF Storage Trait

```rust
pub trait RdfStorage: GraphStorage {
    /// Add a triple to the store
    fn add_triple(&mut self, triple: &Triple) -> Result<(), StorageError>;
    
    /// Remove a triple from the store
    fn remove_triple(&mut self, triple: &Triple) -> Result<bool, StorageError>;
    
    /// Check if triple exists
    fn contains_triple(&self, triple: &Triple) -> bool;
    
    /// Query triples by pattern (None = wildcard)
    fn match_triples(
        &self,
        subject: Option<&RdfNode>,
        predicate: Option<&Iri>,
        object: Option<&RdfNode>,
    ) -> Box<dyn Iterator<Item = Triple> + '_>;
    
    /// Get all triples
    fn triples(&self) -> Box<dyn Iterator<Item = Triple> + '_>;
    
    /// Triple count
    fn triple_count(&self) -> u64;
}
```

#### 3.3 Index Structure

Efficient triple pattern matching requires multiple indexes:

```rust
pub struct TripleIndexes {
    /// Subject -> (Predicate -> Objects)
    spo: HashMap<RdfNodeId, HashMap<IriId, HashSet<RdfNodeId>>>,
    /// Predicate -> (Subject -> Objects)
    pos: HashMap<IriId, HashMap<RdfNodeId, HashSet<RdfNodeId>>>,
    /// Object -> (Subject -> Predicates)
    osp: HashMap<RdfNodeId, HashMap<RdfNodeId, HashSet<IriId>>>,
}
```

This enables O(1) lookup for any triple pattern:
- `(s, p, o)` - Check specific triple
- `(s, p, ?)` - Objects for subject-predicate
- `(s, ?, ?)` - All triples with subject
- `(?, p, ?)` - All triples with predicate
- `(?, ?, o)` - All triples with object
- `(?, ?, ?)` - All triples

### Phase 4: Named Graphs (Quads)

Extend to RDF datasets with named graphs.

#### 4.1 Quad Model

```rust
pub struct Quad {
    pub subject: RdfNode,
    pub predicate: Iri,
    pub object: RdfNode,
    pub graph: Option<Iri>,  // None = default graph
}

pub struct RdfDataset {
    /// The default (unnamed) graph
    default_graph: RdfGraph,
    /// Named graphs
    named_graphs: HashMap<Iri, RdfGraph>,
}
```

#### 4.2 Quad Storage Trait

```rust
pub trait QuadStorage: RdfStorage {
    /// Add quad to a specific graph
    fn add_quad(&mut self, quad: &Quad) -> Result<(), StorageError>;
    
    /// Query with graph pattern
    fn match_quads(
        &self,
        subject: Option<&RdfNode>,
        predicate: Option<&Iri>,
        object: Option<&RdfNode>,
        graph: Option<&Iri>,
    ) -> Box<dyn Iterator<Item = Quad> + '_>;
    
    /// List all named graphs
    fn graphs(&self) -> Box<dyn Iterator<Item = &Iri> + '_>;
    
    /// Get a specific named graph
    fn graph(&self, name: &Iri) -> Option<&dyn RdfStorage>;
}
```

#### 4.3 Implementation Options

**Option A: Graph partitioning**

Store each named graph as a separate `Graph` instance:

```rust
pub struct RdfDatasetStorage {
    default_graph: Graph,
    named_graphs: HashMap<Iri, Graph>,
}
```

**Option B: Quad indexing**

Add graph dimension to all indexes:

```rust
pub struct QuadIndexes {
    /// Graph -> Subject -> Predicate -> Objects
    gspo: HashMap<Option<IriId>, HashMap<RdfNodeId, HashMap<IriId, HashSet<RdfNodeId>>>>,
    // ... additional indexes
}
```

### Phase 5: RDF Serialization Formats

Support standard RDF serialization formats.

#### 5.1 Supported Formats

| Format | Extension | MIME Type | Priority |
|--------|-----------|-----------|----------|
| N-Triples | `.nt` | `application/n-triples` | **P0** |
| Turtle | `.ttl` | `text/turtle` | **P0** |
| N-Quads | `.nq` | `application/n-quads` | **P1** |
| TriG | `.trig` | `application/trig` | **P1** |
| RDF/XML | `.rdf` | `application/rdf+xml` | **P2** |
| JSON-LD | `.jsonld` | `application/ld+json` | **P2** |

#### 5.2 Parser/Serializer Traits

```rust
pub trait RdfParser {
    type Error: std::error::Error;
    
    /// Parse RDF from a reader
    fn parse<R: Read>(&self, reader: R) -> Result<impl Iterator<Item = Triple>, Self::Error>;
    
    /// Parse with base IRI
    fn parse_with_base<R: Read>(
        &self,
        reader: R,
        base: &Iri,
    ) -> Result<impl Iterator<Item = Triple>, Self::Error>;
}

pub trait RdfSerializer {
    type Error: std::error::Error;
    
    /// Serialize triples to a writer
    fn serialize<W: Write, I>(&self, writer: W, triples: I) -> Result<(), Self::Error>
    where
        I: Iterator<Item = Triple>;
    
    /// Serialize with prefix declarations
    fn serialize_with_prefixes<W: Write, I>(
        &self,
        writer: W,
        triples: I,
        prefixes: &PrefixMap,
    ) -> Result<(), Self::Error>
    where
        I: Iterator<Item = Triple>;
}
```

#### 5.3 Prefix Management

```rust
pub struct PrefixMap {
    prefixes: HashMap<String, Iri>,
}

impl PrefixMap {
    pub fn new() -> Self {
        let mut map = Self { prefixes: HashMap::new() };
        // Add common prefixes
        map.insert("rdf", prefix::RDF);
        map.insert("rdfs", prefix::RDFS);
        map.insert("xsd", prefix::XSD);
        map.insert("owl", prefix::OWL);
        map
    }
    
    /// Expand prefixed name to full IRI
    /// "foaf:Person" -> "http://xmlns.com/foaf/0.1/Person"
    pub fn expand(&self, prefixed: &str) -> Option<Iri>;
    
    /// Compact IRI to prefixed name
    /// "http://xmlns.com/foaf/0.1/Person" -> "foaf:Person"
    pub fn compact(&self, iri: &Iri) -> Option<String>;
}
```

#### 5.4 Example: Turtle Parser

```rust
pub struct TurtleParser {
    base: Option<Iri>,
    prefixes: PrefixMap,
}

impl RdfParser for TurtleParser {
    type Error = TurtleParseError;
    
    fn parse<R: Read>(&self, reader: R) -> Result<impl Iterator<Item = Triple>, Self::Error> {
        // Use rio_turtle or similar crate
        todo!()
    }
}
```

### Phase 6: SPARQL Support (Optional)

Add SPARQL query language support.

#### 6.1 SPARQL to Gremlin Translation

```rust
pub struct SparqlCompiler {
    prefixes: PrefixMap,
}

impl SparqlCompiler {
    /// Compile SPARQL query to Gremlin traversal
    pub fn compile(&self, sparql: &str) -> Result<CompiledQuery, SparqlError>;
}

pub struct CompiledQuery {
    /// The Gremlin traversal to execute
    traversal: Box<dyn Fn(&GraphTraversalSource<...>) -> ...>,
    /// Variable bindings to extract
    variables: Vec<String>,
}
```

#### 6.2 Supported SPARQL Features

| Feature | Priority | Complexity |
|---------|----------|------------|
| SELECT queries | **P0** | Medium |
| Basic graph patterns | **P0** | Low |
| FILTER expressions | **P0** | Medium |
| OPTIONAL | **P1** | Medium |
| UNION | **P1** | Medium |
| ORDER BY, LIMIT, OFFSET | **P1** | Low |
| DISTINCT, REDUCED | **P1** | Low |
| CONSTRUCT queries | **P2** | Medium |
| ASK queries | **P2** | Low |
| DESCRIBE queries | **P2** | High |
| Property paths | **P2** | High |
| Aggregates (COUNT, SUM, etc.) | **P2** | Medium |
| Subqueries | **P3** | High |
| BIND, VALUES | **P3** | Medium |
| Federated queries (SERVICE) | **P3** | Very High |

#### 6.3 Example Translation

SPARQL:
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?name ?email
WHERE {
    ?person a foaf:Person .
    ?person foaf:name ?name .
    ?person foaf:mbox ?email .
    FILTER (CONTAINS(?name, "Alice"))
}
LIMIT 10
```

Gremlin equivalent:
```rust
g.V()
    .has_label("http://xmlns.com/foaf/0.1/Person")
    .as_("person")
    .out("http://xmlns.com/foaf/0.1/name")
    .filter(|v| v.string_value().contains("Alice"))
    .as_("name")
    .select("person")
    .out("http://xmlns.com/foaf/0.1/mbox")
    .as_("email")
    .select(["name", "email"])
    .limit(10)
```

### Phase 7: RDFS/OWL Reasoning (Optional)

Add inference capabilities for RDF Schema and OWL.

#### 7.1 Inference Rules

```rust
pub enum InferenceLevel {
    /// No inference
    None,
    /// RDFS entailment (subclass, subproperty, domain, range)
    Rdfs,
    /// OWL 2 RL profile
    OwlRl,
    /// Custom rules
    Custom(Vec<Rule>),
}

pub struct Rule {
    /// Antecedent pattern
    body: Vec<TriplePattern>,
    /// Consequent pattern
    head: Vec<TriplePattern>,
}
```

#### 7.2 Materialization vs. Query-time

**Materialization**: Pre-compute all inferred triples

```rust
impl RdfStorage {
    /// Materialize all inferred triples
    pub fn materialize(&mut self, level: InferenceLevel) -> Result<usize, StorageError>;
}
```

**Query-time reasoning**: Expand queries to include inference

```rust
impl SparqlCompiler {
    /// Compile with inference expansion
    pub fn compile_with_inference(
        &self,
        sparql: &str,
        level: InferenceLevel,
    ) -> Result<CompiledQuery, SparqlError>;
}
```

## Implementation Priority

| Phase | Feature | Effort | Value | Priority |
|-------|---------|--------|-------|----------|
| 1 | URI/IRI Support | Low | Critical | **P0** |
| 2 | RDF Datatype System | Medium | High | **P0** |
| 3 | Triple Store Model | High | Critical | **P0** |
| 4 | Named Graphs (Quads) | Medium | Medium | **P1** |
| 5 | RDF Serialization | Medium | High | **P1** |
| 6 | SPARQL Support | Very High | High | **P2** |
| 7 | RDFS/OWL Reasoning | Very High | Medium | **P3** |

## Example Usage

### Creating and Querying RDF Data

```rust
use interstellar::prelude::*;
use interstellar::rdf::*;

let mut store = InMemoryRdfStore::new();

// Define prefixes
let mut prefixes = PrefixMap::new();
prefixes.insert("ex", "http://example.org/");
prefixes.insert("foaf", prefix::FOAF);

// Add triples
store.add_triple(&Triple {
    subject: RdfNode::Iri(iri!("http://example.org/Alice")),
    predicate: iri!("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
    object: RdfNode::Iri(iri!("http://xmlns.com/foaf/0.1/Person")),
})?;

store.add_triple(&Triple {
    subject: RdfNode::Iri(iri!("http://example.org/Alice")),
    predicate: iri!("http://xmlns.com/foaf/0.1/name"),
    object: RdfNode::Literal(Value::LangString {
        value: "Alice".to_string(),
        language: "en".to_string(),
    }),
})?;

store.add_triple(&Triple {
    subject: RdfNode::Iri(iri!("http://example.org/Alice")),
    predicate: iri!("http://xmlns.com/foaf/0.1/age"),
    object: RdfNode::Literal(Value::typed_literal("30", &xsd::INTEGER)?),
})?;

store.add_triple(&Triple {
    subject: RdfNode::Iri(iri!("http://example.org/Alice")),
    predicate: iri!("http://xmlns.com/foaf/0.1/knows"),
    object: RdfNode::Iri(iri!("http://example.org/Bob")),
})?;

// Query by pattern
let alice_triples = store.match_triples(
    Some(&RdfNode::Iri(iri!("http://example.org/Alice"))),
    None,  // Any predicate
    None,  // Any object
);

for triple in alice_triples {
    println!("{:?}", triple);
}

// Serialize to Turtle
let mut output = Vec::new();
TurtleSerializer::new()
    .serialize_with_prefixes(&mut output, store.triples(), &prefixes)?;
println!("{}", String::from_utf8(output)?);
// Output:
// @prefix ex: <http://example.org/> .
// @prefix foaf: <http://xmlns.com/foaf/0.1/> .
//
// ex:Alice a foaf:Person ;
//     foaf:name "Alice"@en ;
//     foaf:age "30"^^xsd:integer ;
//     foaf:knows ex:Bob .
```

### Loading RDF from File

```rust
// Load from Turtle file
let file = File::open("data.ttl")?;
let parser = TurtleParser::new();

for triple in parser.parse(file)? {
    store.add_triple(&triple)?;
}

// Load from N-Triples
let nt_file = File::open("data.nt")?;
let nt_parser = NTriplesParser::new();

for triple in nt_parser.parse(nt_file)? {
    store.add_triple(&triple)?;
}
```

### SPARQL Queries (Phase 6)

```rust
let compiler = SparqlCompiler::new();

let query = r#"
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    
    SELECT ?name ?friend
    WHERE {
        ?person foaf:name ?name .
        ?person foaf:knows ?friend .
    }
"#;

let results = compiler.execute(&store, query)?;

for row in results {
    println!("Name: {}, Friend: {}", row["name"], row["friend"]);
}
```

## Comparison with Other RDF Stores

| Feature | Apache Jena | Oxigraph | Interstellar (proposed) |
|---------|-------------|----------|------------------------|
| Triple storage | ✅ | ✅ | ✅ (Phase 3) |
| Named graphs | ✅ | ✅ | ✅ (Phase 4) |
| N-Triples/Turtle | ✅ | ✅ | ✅ (Phase 5) |
| RDF/XML | ✅ | ✅ | ⚠️ Optional |
| JSON-LD | ✅ | ✅ | ⚠️ Optional |
| SPARQL | ✅ Full | ✅ Full | ⚠️ Partial (Phase 6) |
| RDFS inference | ✅ | ✅ | ⚠️ Optional (Phase 7) |
| OWL reasoning | ✅ | ❌ | ⚠️ Optional (Phase 7) |
| Property graph | ❌ | ❌ | ✅ Native |
| Gremlin traversal | ❌ | ❌ | ✅ Native |
| Memory-mapped storage | ⚠️ TDB | ✅ | ⚠️ Planned |

## Dependencies

```toml
[dependencies]
# IRI parsing and validation
iri-string = "0.7"

# RDF parsing (choose one or both)
rio_turtle = "0.8"    # Turtle/N-Triples parser
rio_xml = "0.8"       # RDF/XML parser
oxrdf = "0.1"         # RDF data model (alternative)

# SPARQL parsing (Phase 6)
spargebra = "0.2"     # SPARQL algebra
oxrdfio = "0.1"       # RDF I/O utilities

# Date/time handling for XSD types
chrono = "0.4"

[features]
rdf = ["iri-string", "rio_turtle"]
rdf-xml = ["rdf", "rio_xml"]
sparql = ["rdf", "spargebra"]
```

## Design Decisions

### 1. Literal Objects as Vertices

Since edges must connect vertices, literal objects are stored as special vertices:

```rust
// Triple: ex:Alice foaf:age "30"^^xsd:integer

// Stored as:
// Vertex 1: { label: "__rdf_iri", iri: "http://example.org/Alice" }
// Vertex 2: { label: "__rdf_literal", value: "30", datatype: "xsd:integer" }
// Edge: Vertex1 --[foaf:age]--> Vertex2
```

**Trade-offs:**
- ✅ Uniform graph model
- ✅ Works with existing traversal engine
- ❌ More vertices than pure triple stores
- ❌ Memory overhead for simple literals

### 2. IRI Interning

Store IRIs efficiently using the existing `StringInterner`:

```rust
pub struct IriId(u32);  // Interned IRI reference

impl RdfStore {
    fn intern_iri(&mut self, iri: &Iri) -> IriId {
        self.interner.intern(iri.as_str())
    }
}
```

### 3. Hybrid Querying

Support both RDF patterns and Gremlin traversals:

```rust
// RDF pattern matching
let triples = store.match_triples(Some(&alice), Some(&knows), None);

// Gremlin traversal on same data
let friends = g.V()
    .has("__rdf_iri", "http://example.org/Alice")
    .out("http://xmlns.com/foaf/0.1/knows")
    .values("__rdf_iri")
    .to_list()?;
```

## Conclusion

Adding RDF support to Interstellar requires significant extensions, primarily:

1. **Value system changes** - URI/IRI type, typed literals, language tags
2. **Storage layer** - Triple indexing, named graphs
3. **Serialization** - Standard RDF formats (Turtle, N-Triples, etc.)
4. **Query language** - SPARQL compiler (optional but valuable)

The result would be a unique hybrid store supporting both property graph (Gremlin) and RDF (SPARQL) paradigms, useful for:

- Knowledge graph applications needing both query styles
- Integrating RDF data sources with property graph analytics
- Semantic web applications requiring graph traversal capabilities

This positions Interstellar as a bridge between the property graph and semantic web worlds.
