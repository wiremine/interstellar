# Graph Modeling

Design effective graph schemas for your domain. This guide covers best practices for modeling vertices, edges, and properties.

## Core Concepts

### Vertices (Nodes)

Vertices represent **entities** in your domain:

- People, users, accounts
- Products, orders, transactions
- Documents, files, resources
- Locations, events, concepts

### Edges (Relationships)

Edges represent **connections** between entities:

- "knows", "follows", "friends_with"
- "purchased", "created", "owns"
- "located_in", "part_of", "depends_on"

### Properties

Properties store **attributes** on vertices and edges:

- Names, dates, quantities
- Status flags, metadata
- Computed or derived values

---

## Modeling Guidelines

### 1. Use Descriptive Labels

Labels should clearly indicate what an entity or relationship represents:

```rust
// Good: Clear, specific labels
storage.add_vertex("person", props);
storage.add_vertex("company", props);
storage.add_edge(person, company, "works_at", props);

// Avoid: Generic or ambiguous labels
storage.add_vertex("node", props);
storage.add_edge(a, b, "link", props);
```

### 2. Model Relationships as Edges, Not Properties

If entities have meaningful relationships, use edges:

```rust
// Good: Relationship as edge
storage.add_edge(employee, manager, "reports_to", HashMap::new());

// Avoid: Relationship as property
storage.add_vertex("employee", HashMap::from([
    ("manager_id".into(), Value::Int(42)),  // Loses graph structure
]));
```

Benefits of edges:
- Traversable in both directions
- Can have their own properties
- Indexed for fast lookup
- Queryable as first-class entities

### 3. Consider Edge Direction

Edge direction should reflect the natural relationship:

```rust
// Direction matters semantically
alice.add_edge("follows", bob);    // Alice follows Bob
alice.add_edge("created", post);   // Alice created Post
order.add_edge("contains", item);  // Order contains Item
```

Use `in_()` and `out()` to traverse based on direction:

```rust
// Who does Alice follow?
g.v_ids([alice]).out("follows").to_list()

// Who follows Alice?
g.v_ids([alice]).in_("follows").to_list()
```

### 4. Use Properties for Attributes

Store scalar values as properties:

```rust
storage.add_vertex("person", HashMap::from([
    ("name".into(), Value::String("Alice".into())),
    ("age".into(), Value::Int(30)),
    ("email".into(), Value::String("alice@example.com".into())),
    ("active".into(), Value::Bool(true)),
]));

storage.add_edge(alice, bob, "knows", HashMap::from([
    ("since".into(), Value::Int(2020)),
    ("strength".into(), Value::Float(0.8)),
]));
```

### 5. Avoid Deep Nesting

Flatten structures when possible:

```rust
// Avoid: Deeply nested properties
storage.add_vertex("person", HashMap::from([
    ("address".into(), Value::Map(HashMap::from([
        ("street".into(), Value::Map(/* ... */)),
    ]))),
]));

// Better: Separate address vertex with relationship
let address = storage.add_vertex("address", HashMap::from([
    ("street".into(), Value::String("123 Main St".into())),
    ("city".into(), Value::String("Springfield".into())),
]));
storage.add_edge(person, address, "lives_at", HashMap::new());
```

---

## Common Patterns

### Social Network

```
[Person] --follows--> [Person]
[Person] --friends_with-- [Person]
[Person] --posted--> [Post]
[Person] --liked--> [Post]
[Post] --tagged--> [Topic]
```

```rust
// Create users
let alice = storage.add_vertex("person", HashMap::from([
    ("name".into(), "Alice".into()),
    ("username".into(), "@alice".into()),
]));

let bob = storage.add_vertex("person", HashMap::from([
    ("name".into(), "Bob".into()),
]));

// Create relationships
storage.add_edge(alice, bob, "follows", HashMap::from([
    ("since".into(), Value::Int(2023)),
]));
```

### E-Commerce

```
[Customer] --placed--> [Order]
[Order] --contains--> [OrderItem]
[OrderItem] --refers_to--> [Product]
[Product] --belongs_to--> [Category]
[Customer] --reviewed--> [Product]
```

```rust
let customer = storage.add_vertex("customer", HashMap::from([
    ("email".into(), "customer@example.com".into()),
]));

let order = storage.add_vertex("order", HashMap::from([
    ("date".into(), "2024-01-15".into()),
    ("total".into(), Value::Float(99.99)),
]));

storage.add_edge(customer, order, "placed", HashMap::new());
```

### Organizational Hierarchy

```
[Employee] --reports_to--> [Employee]
[Employee] --works_in--> [Department]
[Department] --part_of--> [Division]
[Employee] --has_role--> [Role]
```

```rust
// Create hierarchy
let ceo = storage.add_vertex("employee", HashMap::from([
    ("name".into(), "Jane CEO".into()),
    ("title".into(), "CEO".into()),
]));

let vp = storage.add_vertex("employee", HashMap::from([
    ("name".into(), "John VP".into()),
    ("title".into(), "VP Engineering".into()),
]));

storage.add_edge(vp, ceo, "reports_to", HashMap::new());
```

### Knowledge Graph

```
[Concept] --related_to--> [Concept]
[Concept] --is_a--> [Concept]
[Document] --mentions--> [Concept]
[Document] --cites--> [Document]
```

---

## Anti-Patterns to Avoid

### 1. Storing Lists as Properties

```rust
// Avoid: List of IDs in a property
storage.add_vertex("person", HashMap::from([
    ("friend_ids".into(), Value::List(vec![
        Value::Int(1), Value::Int(2), Value::Int(3)
    ])),
]));

// Better: Use edges
storage.add_edge(person, friend1, "friends_with", HashMap::new());
storage.add_edge(person, friend2, "friends_with", HashMap::new());
```

### 2. Using Edges for Non-Relationships

```rust
// Avoid: Edge for a simple attribute
storage.add_edge(person, age_vertex, "has_age", HashMap::new());

// Better: Property on the vertex
storage.add_vertex("person", HashMap::from([
    ("age".into(), Value::Int(30)),
]));
```

### 3. One Giant Vertex

```rust
// Avoid: Everything in one vertex
storage.add_vertex("data", HashMap::from([
    ("type".into(), "order".into()),
    ("customer_name".into(), "...".into()),
    ("product_name".into(), "...".into()),
    // ... 50 more properties
]));

// Better: Separate entities with relationships
```

### 4. Missing Labels

```rust
// Avoid: Unlabeled or generically labeled vertices
storage.add_vertex("entity", props);
storage.add_vertex("node", props);

// Better: Specific labels
storage.add_vertex("customer", props);
storage.add_vertex("product", props);
```

---

## Schema Evolution

### Adding Properties

Adding new properties is safe:

```rust
// Old vertices don't have "created_at"
// New vertices do - queries handle both
g.v().has_label("person")
    .values("created_at")  // Returns null for old vertices
    .to_list()
```

### Renaming Labels

Requires migration:

```rust
// Option 1: Add new label, migrate, remove old
// Option 2: Query both labels
g.v().has_label_any(&["user", "person"])  // Support both during transition
```

### Adding Relationship Types

Safe - new edges don't affect existing queries:

```rust
// Old queries still work
g.v().out("knows").to_list()

// New queries use new edge type
g.v().out("follows").to_list()
```

---

## See Also

- [Quick Start](../getting-started/quick-start.md) - Creating your first graph
- [Querying Guide](querying.md) - Query patterns for your model
- [GQL API](../api/gql.md) - Schema DDL syntax
