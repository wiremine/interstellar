# RustGremlin Web UI Specification

**Status**: Draft  
**Dependencies**: Phase 3 (Traversal Engine), Phase 6 (CLI)  
**Target Users**: Developers debugging queries, non-technical users exploring data

---

## 1. Overview

A simple, embedded web UI that ships with the RustGremlin binary. The UI provides graph exploration, query execution, and data management capabilities through a browser interface.

### 1.1 Goals

1. **Embedded**: Single binary deployment via `rustgremlin serve`
2. **Read/Write**: Full CRUD operations on graph data
3. **Local-first**: No authentication required (assumes trusted local network)
4. **Dual audience**: Usable by both developers and non-technical users

### 1.2 Non-Goals (Initial Release)

- Remote/multi-user authentication
- Real-time collaboration
- Query optimization recommendations
- Clustering/distributed deployment

---

## 2. Architecture

### 2.1 Component Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Web UI Application                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                    HTTP Server (axum)                     │  │
│  │  • Static file serving (embedded)                         │  │
│  │  • REST API endpoints                                     │  │
│  │  • WebSocket for live updates (future)                    │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              │                                   │
│         ┌────────────────────┼────────────────────┐             │
│         ▼                    ▼                    ▼             │
│  ┌────────────┐       ┌────────────┐       ┌────────────┐      │
│  │   Query    │       │   Schema   │       │    CRUD    │      │
│  │  Executor  │       │  Explorer  │       │  Operations│      │
│  └─────┬──────┘       └─────┬──────┘       └─────┬──────┘      │
│        │                    │                    │              │
│        └────────────────────┼────────────────────┘              │
│                             ▼                                   │
│                 ┌──────────────────────┐                        │
│                 │   RustGremlin Core   │                        │
│                 │  (Graph, Traversal)  │                        │
│                 └──────────────────────┘                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Tech Stack

| Component | Technology | Rationale |
|-----------|------------|-----------|
| HTTP Server | `axum` | Clean async API, tower middleware ecosystem |
| Async Runtime | `tokio` | Required by axum, industry standard |
| Static Files | `rust-embed` | Compile frontend into binary |
| Frontend | Vanilla JS + htmx | Simple, no build step required |
| Graph Viz | Cytoscape.js | Feature-rich, good documentation |
| Styling | Pico CSS or similar | Minimal, classless CSS |

### 2.3 Proposed Dependencies

```toml
[dependencies]
axum = "0.7"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
tower-http = { version = "0.5", features = ["fs", "cors"] }
rust-embed = "8"
serde_json = "1.0"
```

---

## 3. CLI Integration

### 3.1 Command Structure

```bash
rustgremlin serve [OPTIONS]

Options:
  -p, --port <PORT>     HTTP port [default: 8080]
  -H, --host <HOST>     Bind address [default: 127.0.0.1]
  -d, --db <PATH>       Database path (omit for in-memory)
  --open                Auto-open browser after starting
  --readonly            Disable mutation endpoints
```

### 3.2 Example Usage

```bash
# Start with in-memory database
rustgremlin serve

# Start with persistent database
rustgremlin serve --db ./my-graph.db

# Start on custom port, auto-open browser
rustgremlin serve --port 3000 --open

# Read-only mode for production data
rustgremlin serve --db ./production.db --readonly
```

---

## 4. Feature Specifications

### 4.1 Schema Explorer

| Feature | Description | Priority |
|---------|-------------|----------|
| Vertex Label List | All labels with counts (uses existing label indexes) | High |
| Edge Label List | All edge labels with counts, shows source→target patterns | High |
| Property Discovery | For each label, show property keys and inferred types | High |
| Schema Graph | Interactive visualization: labels as nodes, edge types as connections | Medium |
| Property Statistics | Min/max/avg for numeric properties, cardinality for strings | Low |

**API Endpoints:**
```
GET /api/schema/vertex-labels    → [{ label, count }]
GET /api/schema/edge-labels      → [{ label, count, patterns: [{ from, to }] }]
GET /api/schema/properties/:label → [{ key, type, nullable, sample_values }]
```

---

### 4.2 Query Interface

| Feature | Description | Priority |
|---------|-------------|----------|
| Query Editor | Text area with syntax highlighting | High |
| Execute Button | Run query, show results | High |
| Query History | Recent queries with timestamps, re-run capability | High |
| Saved Queries | Name and save frequently used queries (localStorage) | Medium |
| Query Templates | Pre-built patterns: find by label, neighbors, shortest path | Medium |
| Auto-complete | Step names, labels, property keys from schema | Low |

**Query Language (Initial):**

A simplified text DSL that mirrors the Rust API:
```
g.v()                                    // All vertices
g.v().has_label("person")                // Filter by label
g.v().has_value("name", "Alice")         // Filter by property
g.v().out("knows").values("name")        // Traversal + projection
g.v().has_label("person").count()        // Aggregation
```

**API Endpoints:**
```
POST /api/query                  → { query: string } → { results, timing_ms, step_names }
GET  /api/query/history          → [{ query, timestamp, result_count }]
POST /api/query/save             → { name, query }
GET  /api/query/saved            → [{ name, query }]
GET  /api/query/templates        → [{ name, query, description }]
```

---

### 4.3 Results Display

| Feature | Description | Priority |
|---------|-------------|----------|
| Table View | Tabular display with sortable columns, property expansion | High |
| JSON View | Raw Value output (uses existing serde derive) | High |
| Graph View | Interactive visualization of vertex/edge results | High |
| Path View | Visualize traversal paths when using `path()` step | Medium |
| Pagination | Handle large result sets (client-side initially) | Medium |
| Export Results | Download as JSON/CSV | Medium |

**Result Format:**
```json
{
  "results": [
    { "type": "vertex", "id": 1, "label": "person", "properties": {...} },
    { "type": "edge", "id": 0, "label": "knows", "from": 1, "to": 2, "properties": {...} },
    { "type": "value", "value": "Alice" }
  ],
  "timing_ms": 12,
  "step_names": ["v", "hasLabel", "values"],
  "total_count": 42
}
```

---

### 4.4 Graph Statistics Dashboard

| Feature | Description | Priority |
|---------|-------------|----------|
| Overview | Total vertices, total edges, storage backend type | High |
| Label Distribution | Bar chart of counts by label | High |
| Degree Distribution | Histogram of vertex connectivity | Medium |
| Memory Usage | For in-memory graphs | Low |

**API Endpoints:**
```
GET /api/stats                   → { vertex_count, edge_count, storage_type }
GET /api/stats/labels            → { vertices: [...], edges: [...] }
GET /api/stats/degrees           → { distribution: [...] }
```

---

### 4.5 Data Management (CRUD)

| Feature | Description | Priority |
|---------|-------------|----------|
| Add Vertex | Form: label selection, property key-value pairs | High |
| Add Edge | Form: source/target vertex selection, label, properties | High |
| Edit Properties | Click to modify properties on selected element | Medium |
| Delete Vertex/Edge | With confirmation | Medium |
| Bulk Import | JSON format (compatible with existing fixtures) | Medium |
| Export Graph | Full graph to JSON | Medium |
| Load Sample Data | One-click load of marvel.json, british_royals.json | Low |

**API Endpoints:**
```
POST   /api/vertices             → { label, properties } → { id }
GET    /api/vertices/:id         → { id, label, properties }
PATCH  /api/vertices/:id         → { properties }
DELETE /api/vertices/:id

POST   /api/edges                → { from, to, label, properties } → { id }
GET    /api/edges/:id            → { id, label, from, to, properties }
PATCH  /api/edges/:id            → { properties }
DELETE /api/edges/:id

POST   /api/import               → { vertices: [...], edges: [...] }
GET    /api/export               → { vertices: [...], edges: [...] }
POST   /api/samples/:name        → Load sample dataset (marvel, british_royals)
```

---

### 4.6 Query Analysis

| Feature | Description | Priority |
|---------|-------------|----------|
| Step Names | Show `step_names()` for executed query | High |
| Result Count | Show count before/during loading | High |
| Execution Time | Basic timing in milliseconds | High |
| Explain Plan | When `explain()` is implemented | Low |
| Step-by-step Mode | Execute one step at a time (debugging) | Low |

---

## 5. UI Layout

### 5.1 Main Layout

```
┌─────────────────────────────────────────────────────────────────┐
│  RustGremlin                              [Stats] [Schema] [?]  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ Query Editor                                    [▶ Run]   │ │
│  │ g.v().has_label("person").values("name")                  │ │
│  │                                                           │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌─────────────────────────┬─────────────────────────────────┐ │
│  │ Results (42)            │ [Table] [Graph] [JSON]          │ │
│  ├─────────────────────────┴─────────────────────────────────┤ │
│  │                                                           │ │
│  │  ┌─────────┬─────────┬──────────────────────────────────┐│ │
│  │  │ ID      │ Label   │ Properties                       ││ │
│  │  ├─────────┼─────────┼──────────────────────────────────┤│ │
│  │  │ v[1]    │ person  │ { name: "Alice", age: 30 }       ││ │
│  │  │ v[2]    │ person  │ { name: "Bob", age: 25 }         ││ │
│  │  │ ...     │ ...     │ ...                              ││ │
│  │  └─────────┴─────────┴──────────────────────────────────┘│ │
│  │                                                           │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  Steps: v → hasLabel → values    Time: 12ms                     │
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 Schema Modal

```
┌─────────────────────────────────────────────────────────────────┐
│  Schema Explorer                                          [×]   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Vertex Labels                      Edge Labels                 │
│  ┌────────────────────────┐        ┌────────────────────────┐  │
│  │ person (150)           │        │ knows (320)            │  │
│  │ company (45)           │        │ works_at (150)         │  │
│  │ software (28)          │        │ uses (89)              │  │
│  └────────────────────────┘        └────────────────────────┘  │
│                                                                 │
│  Properties for "person":                                       │
│  ┌────────────────────────────────────────────────────────────┐│
│  │ name     │ String  │ required │ "Alice", "Bob", ...       ││
│  │ age      │ Int     │ optional │ 25, 30, 35, ...           ││
│  │ email    │ String  │ optional │ "alice@...", ...          ││
│  └────────────────────────────────────────────────────────────┘│
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 6. Implementation Phases

### Phase 1: Foundation (MVP)
- [ ] axum server setup with embedded static files
- [ ] Basic query execution endpoint
- [ ] Simple query parser for text DSL
- [ ] Table view for results
- [ ] Vertex/edge counts endpoint

### Phase 2: Schema & Stats
- [ ] Schema explorer endpoints
- [ ] Statistics dashboard
- [ ] Label distribution charts
- [ ] Property discovery

### Phase 3: Graph Visualization
- [ ] Cytoscape.js integration
- [ ] Graph view for query results
- [ ] Click-to-explore interaction
- [ ] Path visualization

### Phase 4: CRUD Operations
- [ ] Add vertex/edge forms
- [ ] Edit properties inline
- [ ] Delete with confirmation
- [ ] Import/export JSON

### Phase 5: Polish
- [ ] Query history (localStorage)
- [ ] Saved queries
- [ ] Query templates
- [ ] Auto-complete
- [ ] Keyboard shortcuts

---

## 7. Design Decisions

### 7.1 Query Language: Gremlin Text Parser (Shared with CLI)

**Decision**: Reuse the Gremlin text parser from the CLI specification (`spec-06-cli.md` section 6.2).

**Rationale**:
- The CLI already defines a Gremlin text parser that mirrors our Rust fluent API
- Gremlin syntax (`g.V().hasLabel('person').out('knows')`) is intuitive for web forms
- Avoids duplicating parser infrastructure
- Matches our existing examples and documentation
- Users familiar with Gremlin (from TinkerPop) can use their existing knowledge

**Query Syntax Examples**:
```
g.v()                                    // All vertices
g.v().has_label("person")                // Filter by label
g.v().has_value("name", "Alice")         // Filter by property
g.v().out("knows").values("name")        // Traversal + projection
g.v().has_label("person").count()        // Aggregation
g.v(42)                                  // Vertex by ID
g.e()                                    // All edges
```

### 7.2 Graph Visualization: Cytoscape.js

**Decision**: Use Cytoscape.js for graph visualization.

**Rationale**:
- Feature-rich with built-in layouts (force-directed, hierarchical, circle, grid)
- ~400KB is acceptable for a local tool (not optimizing for slow networks)
- Excellent documentation and active community
- Built-in support for click/hover events, zoom, pan
- Can export to PNG/JSON
- Used by many graph tools (Neo4j Bloom uses a fork)

**CDN URL**: `https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.28.1/cytoscape.min.js`

### 7.3 Frontend Stack: Vanilla JS + htmx

**Decision**: Vanilla JavaScript with htmx for AJAX interactions.

**Rationale**:
- No build step required (simplifies development and rust-embed integration)
- htmx handles form submissions and partial page updates with minimal code
- Keeps embedded binary size small
- Sufficient complexity for our use case (not a complex SPA)
- Can add Alpine.js later if reactivity needs increase

**Stack**:
- **htmx** (~14KB): AJAX requests via HTML attributes
- **Pico CSS** (~10KB): Classless CSS framework for clean defaults
- **Cytoscape.js** (~400KB): Graph visualization
- **CodeMirror 6** (~150KB, optional): Syntax highlighting for query editor

### 7.4 Real-time Updates: Manual Refresh (Deferred)

**Decision**: Start with manual refresh; defer WebSocket support to future phase.

**Rationale**:
- Simplifies initial implementation significantly
- Local single-user use case doesn't require real-time collaboration
- Users can click "Refresh" or re-run queries to see changes
- WebSocket support can be added in Phase 5+ if users request it

---

## 8. References

- [spec-06-cli.md](../specs/spec-06-cli.md) - CLI specification
- [overview.md](./overview.md) - Core architecture
- [examples/](../examples/) - Example datasets (marvel.json, british_royals.json)
