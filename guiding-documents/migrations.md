# RustGremlin: Schema Versioning and Migrations

This document outlines the schema versioning system and migration framework for evolving graph schemas over time. Migrations enable safe, controlled changes to graph structure while preserving existing data.

---

## 1. Overview

### 1.1 Motivation

Graph schemas evolve as applications change:
- New vertex/edge types are added
- Properties are added, removed, or renamed
- Property types change (e.g., `String` to `Int`)
- Edge constraints are tightened or relaxed

Without migrations, schema changes require:
- Manual data transformation scripts
- Downtime during updates
- Risk of data loss or corruption
- No rollback capability

### 1.2 Design Principles

1. **Versioned**: Every schema has an explicit version number
2. **Incremental**: Migrations define transitions between adjacent versions
3. **Reversible**: Migrations can be rolled back (when possible)
4. **Safe**: Validate migrations before applying to production data
5. **Auditable**: Track migration history in the graph

---

## 2. Schema Versioning

### 2.1 Version Model

```rust
/// Schema with version information
#[derive(Clone, Debug)]
pub struct VersionedSchema {
    /// The schema definition
    pub schema: GraphSchema,
    
    /// Schema version (monotonically increasing)
    pub version: u64,
    
    /// Human-readable version name (optional)
    pub name: Option<String>,
    
    /// Timestamp when this version was created
    pub created_at: Option<u64>,
    
    /// Description of changes in this version
    pub description: Option<String>,
}

impl VersionedSchema {
    pub fn new(schema: GraphSchema, version: u64) -> Self {
        Self {
            schema,
            version,
            name: None,
            created_at: None,
            description: None,
        }
    }
    
    pub fn with_name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }
    
    pub fn with_description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }
}
```

### 2.2 Version Storage

The current schema version is stored with the graph:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Graph with Versioned Schema                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Schema Metadata                                                │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ current_version: 3                                       │  │
│  │ schema: GraphSchema { ... }                              │  │
│  │ migration_history: [                                     │  │
│  │   { version: 1, applied_at: 1704067200, ... },          │  │
│  │   { version: 2, applied_at: 1704153600, ... },          │  │
│  │   { version: 3, applied_at: 1704240000, ... },          │  │
│  │ ]                                                        │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Graph Data                                                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ vertices: [...]                                          │  │
│  │ edges: [...]                                             │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 Migration History

```rust
/// Record of an applied migration
#[derive(Clone, Debug)]
pub struct MigrationRecord {
    /// Version after migration
    pub version: u64,
    
    /// When the migration was applied (Unix timestamp)
    pub applied_at: u64,
    
    /// Duration of migration in milliseconds
    pub duration_ms: u64,
    
    /// Number of vertices modified
    pub vertices_modified: u64,
    
    /// Number of edges modified
    pub edges_modified: u64,
    
    /// Was this a rollback?
    pub is_rollback: bool,
}
```

---

## 3. Migration Definition

### 3.1 Migration Trait

```rust
/// A migration between two schema versions
pub trait Migration: Send + Sync {
    /// Source version (before migration)
    fn from_version(&self) -> u64;
    
    /// Target version (after migration)
    fn to_version(&self) -> u64;
    
    /// Human-readable description
    fn description(&self) -> &str;
    
    /// Apply migration (forward)
    fn up(&self, ctx: &mut MigrationContext) -> Result<(), MigrationError>;
    
    /// Reverse migration (rollback)
    /// Returns None if migration is not reversible
    fn down(&self, ctx: &mut MigrationContext) -> Option<Result<(), MigrationError>>;
    
    /// Validate migration can be applied
    fn validate(&self, ctx: &MigrationContext) -> Result<(), MigrationError> {
        Ok(()) // Default: no validation
    }
}

/// Context provided to migrations
pub struct MigrationContext<'g> {
    /// Mutable access to graph
    pub graph: &'g mut Graph,
    
    /// Source schema (before migration)
    pub from_schema: &'g GraphSchema,
    
    /// Target schema (after migration)
    pub to_schema: &'g GraphSchema,
    
    /// Progress reporting callback
    pub on_progress: Option<Box<dyn Fn(MigrationProgress) + Send>>,
}

/// Progress information during migration
#[derive(Clone, Debug)]
pub struct MigrationProgress {
    pub phase: MigrationPhase,
    pub total_items: u64,
    pub processed_items: u64,
    pub current_label: Option<String>,
}

#[derive(Clone, Debug)]
pub enum MigrationPhase {
    Validating,
    MigratingVertices,
    MigratingEdges,
    UpdatingIndexes,
    Finalizing,
}
```

### 3.2 Migration Operations

```rust
/// Operations available during migration
impl<'g> MigrationContext<'g> {
    // === Vertex Operations ===
    
    /// Iterate all vertices with a label
    pub fn vertices_with_label(&self, label: &str) -> impl Iterator<Item = Vertex> + '_;
    
    /// Update a vertex's properties
    pub fn update_vertex(
        &mut self,
        id: VertexId,
        properties: HashMap<String, Value>,
    ) -> Result<(), MigrationError>;
    
    /// Change a vertex's label
    pub fn relabel_vertex(
        &mut self,
        id: VertexId,
        new_label: &str,
    ) -> Result<(), MigrationError>;
    
    /// Delete a vertex and its incident edges
    pub fn delete_vertex(&mut self, id: VertexId) -> Result<(), MigrationError>;
    
    // === Edge Operations ===
    
    /// Iterate all edges with a label
    pub fn edges_with_label(&self, label: &str) -> impl Iterator<Item = Edge> + '_;
    
    /// Update an edge's properties
    pub fn update_edge(
        &mut self,
        id: EdgeId,
        properties: HashMap<String, Value>,
    ) -> Result<(), MigrationError>;
    
    /// Change an edge's label
    pub fn relabel_edge(
        &mut self,
        id: EdgeId,
        new_label: &str,
    ) -> Result<(), MigrationError>;
    
    /// Delete an edge
    pub fn delete_edge(&mut self, id: EdgeId) -> Result<(), MigrationError>;
    
    // === Batch Operations ===
    
    /// Add a property to all vertices with a label
    pub fn add_vertex_property(
        &mut self,
        label: &str,
        key: &str,
        default: Value,
    ) -> Result<u64, MigrationError>;
    
    /// Remove a property from all vertices with a label
    pub fn remove_vertex_property(
        &mut self,
        label: &str,
        key: &str,
    ) -> Result<u64, MigrationError>;
    
    /// Rename a property on all vertices with a label
    pub fn rename_vertex_property(
        &mut self,
        label: &str,
        old_key: &str,
        new_key: &str,
    ) -> Result<u64, MigrationError>;
    
    /// Transform a property value on all vertices with a label
    pub fn transform_vertex_property<F>(
        &mut self,
        label: &str,
        key: &str,
        transform: F,
    ) -> Result<u64, MigrationError>
    where
        F: Fn(&Value) -> Value;
    
    // === Same operations for edges ===
    
    pub fn add_edge_property(
        &mut self,
        label: &str,
        key: &str,
        default: Value,
    ) -> Result<u64, MigrationError>;
    
    pub fn remove_edge_property(
        &mut self,
        label: &str,
        key: &str,
    ) -> Result<u64, MigrationError>;
    
    pub fn rename_edge_property(
        &mut self,
        label: &str,
        old_key: &str,
        new_key: &str,
    ) -> Result<u64, MigrationError>;
    
    pub fn transform_edge_property<F>(
        &mut self,
        label: &str,
        key: &str,
        transform: F,
    ) -> Result<u64, MigrationError>
    where
        F: Fn(&Value) -> Value;
}
```

### 3.3 Migration Errors

```rust
#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("migration validation failed: {reason}")]
    ValidationFailed { reason: String },
    
    #[error("version mismatch: expected {expected}, found {found}")]
    VersionMismatch { expected: u64, found: u64 },
    
    #[error("migration not reversible")]
    NotReversible,
    
    #[error("data transformation failed for {element_type} {id}: {reason}")]
    TransformFailed {
        element_type: &'static str,
        id: String,
        reason: String,
    },
    
    #[error("missing required data: {description}")]
    MissingData { description: String },
    
    #[error("migration aborted: {reason}")]
    Aborted { reason: String },
    
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    
    #[error("schema error: {0}")]
    Schema(#[from] SchemaError),
}
```

---

## 4. Common Migration Patterns

### 4.1 Add Property with Default

```rust
/// Migration: Add "created_at" property to all "person" vertices
pub struct AddCreatedAtMigration;

impl Migration for AddCreatedAtMigration {
    fn from_version(&self) -> u64 { 1 }
    fn to_version(&self) -> u64 { 2 }
    fn description(&self) -> &str { "Add created_at timestamp to person vertices" }
    
    fn up(&self, ctx: &mut MigrationContext) -> Result<(), MigrationError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        
        ctx.add_vertex_property("person", "created_at", Value::Int(now))?;
        Ok(())
    }
    
    fn down(&self, ctx: &mut MigrationContext) -> Option<Result<(), MigrationError>> {
        Some(ctx.remove_vertex_property("person", "created_at").map(|_| ()))
    }
}
```

### 4.2 Rename Property

```rust
/// Migration: Rename "name" to "full_name" on person vertices
pub struct RenameNameMigration;

impl Migration for RenameNameMigration {
    fn from_version(&self) -> u64 { 2 }
    fn to_version(&self) -> u64 { 3 }
    fn description(&self) -> &str { "Rename person.name to person.full_name" }
    
    fn up(&self, ctx: &mut MigrationContext) -> Result<(), MigrationError> {
        ctx.rename_vertex_property("person", "name", "full_name")?;
        Ok(())
    }
    
    fn down(&self, ctx: &mut MigrationContext) -> Option<Result<(), MigrationError>> {
        Some(ctx.rename_vertex_property("person", "full_name", "name").map(|_| ()))
    }
}
```

### 4.3 Change Property Type

```rust
/// Migration: Convert age from String to Int
pub struct ConvertAgeMigration;

impl Migration for ConvertAgeMigration {
    fn from_version(&self) -> u64 { 3 }
    fn to_version(&self) -> u64 { 4 }
    fn description(&self) -> &str { "Convert person.age from String to Int" }
    
    fn validate(&self, ctx: &MigrationContext) -> Result<(), MigrationError> {
        // Ensure all age values can be parsed as integers
        for vertex in ctx.graph.vertices_with_label("person") {
            if let Some(Value::String(age_str)) = vertex.properties.get("age") {
                if age_str.parse::<i64>().is_err() {
                    return Err(MigrationError::ValidationFailed {
                        reason: format!(
                            "Cannot convert age '{}' to integer for vertex {:?}",
                            age_str, vertex.id
                        ),
                    });
                }
            }
        }
        Ok(())
    }
    
    fn up(&self, ctx: &mut MigrationContext) -> Result<(), MigrationError> {
        ctx.transform_vertex_property("person", "age", |value| {
            match value {
                Value::String(s) => {
                    Value::Int(s.parse().unwrap_or(0))
                }
                other => other.clone(),
            }
        })?;
        Ok(())
    }
    
    fn down(&self, ctx: &mut MigrationContext) -> Option<Result<(), MigrationError>> {
        Some(ctx.transform_vertex_property("person", "age", |value| {
            match value {
                Value::Int(n) => Value::String(n.to_string()),
                other => other.clone(),
            }
        }).map(|_| ()))
    }
}
```

### 4.4 Split Vertex Label

```rust
/// Migration: Split "user" into "admin" and "member" based on role property
pub struct SplitUserMigration;

impl Migration for SplitUserMigration {
    fn from_version(&self) -> u64 { 4 }
    fn to_version(&self) -> u64 { 5 }
    fn description(&self) -> &str { "Split user vertices into admin and member labels" }
    
    fn up(&self, ctx: &mut MigrationContext) -> Result<(), MigrationError> {
        let users: Vec<_> = ctx.vertices_with_label("user").collect();
        
        for user in users {
            let new_label = match user.properties.get("role") {
                Some(Value::String(role)) if role == "admin" => "admin",
                _ => "member",
            };
            ctx.relabel_vertex(user.id, new_label)?;
        }
        
        // Remove the now-redundant role property
        ctx.remove_vertex_property("admin", "role")?;
        ctx.remove_vertex_property("member", "role")?;
        
        Ok(())
    }
    
    fn down(&self, ctx: &mut MigrationContext) -> Option<Result<(), MigrationError>> {
        // Reversible: merge back into "user" with role property
        Some((|| {
            // Add role property based on current label
            for vertex in ctx.vertices_with_label("admin").collect::<Vec<_>>() {
                let mut props = vertex.properties.clone();
                props.insert("role".to_string(), Value::String("admin".to_string()));
                ctx.update_vertex(vertex.id, props)?;
                ctx.relabel_vertex(vertex.id, "user")?;
            }
            
            for vertex in ctx.vertices_with_label("member").collect::<Vec<_>>() {
                let mut props = vertex.properties.clone();
                props.insert("role".to_string(), Value::String("member".to_string()));
                ctx.update_vertex(vertex.id, props)?;
                ctx.relabel_vertex(vertex.id, "user")?;
            }
            
            Ok(())
        })())
    }
}
```

### 4.5 Add Edge Type with Computed Data

```rust
/// Migration: Create "colleague" edges between people at the same company
pub struct AddColleagueEdgesMigration;

impl Migration for AddColleagueEdgesMigration {
    fn from_version(&self) -> u64 { 5 }
    fn to_version(&self) -> u64 { 6 }
    fn description(&self) -> &str { "Add colleague edges between coworkers" }
    
    fn up(&self, ctx: &mut MigrationContext) -> Result<(), MigrationError> {
        // Group people by company
        let mut by_company: HashMap<String, Vec<VertexId>> = HashMap::new();
        
        for person in ctx.vertices_with_label("person") {
            if let Some(Value::String(company)) = person.properties.get("company") {
                by_company
                    .entry(company.clone())
                    .or_default()
                    .push(person.id);
            }
        }
        
        // Create colleague edges within each company
        for (_company, people) in by_company {
            for i in 0..people.len() {
                for j in (i + 1)..people.len() {
                    ctx.graph.add_edge(
                        people[i],
                        people[j],
                        "colleague",
                        HashMap::new(),
                    )?;
                }
            }
        }
        
        Ok(())
    }
    
    fn down(&self, ctx: &mut MigrationContext) -> Option<Result<(), MigrationError>> {
        // Delete all colleague edges
        Some((|| {
            let edges: Vec<_> = ctx.edges_with_label("colleague")
                .map(|e| e.id)
                .collect();
            
            for edge_id in edges {
                ctx.delete_edge(edge_id)?;
            }
            Ok(())
        })())
    }
}
```

---

## 5. Migration Runner

### 5.1 Runner API

```rust
/// Manages and executes migrations
pub struct MigrationRunner {
    migrations: Vec<Box<dyn Migration>>,
}

impl MigrationRunner {
    pub fn new() -> Self {
        Self {
            migrations: Vec::new(),
        }
    }
    
    /// Register a migration
    pub fn add<M: Migration + 'static>(mut self, migration: M) -> Self {
        self.migrations.push(Box::new(migration));
        self
    }
    
    /// Get current schema version from graph
    pub fn current_version(&self, graph: &Graph) -> u64 {
        graph.schema_version()
    }
    
    /// Get latest available version
    pub fn latest_version(&self) -> u64 {
        self.migrations
            .iter()
            .map(|m| m.to_version())
            .max()
            .unwrap_or(0)
    }
    
    /// List pending migrations
    pub fn pending(&self, graph: &Graph) -> Vec<&dyn Migration> {
        let current = self.current_version(graph);
        self.migrations
            .iter()
            .filter(|m| m.from_version() >= current)
            .map(|m| m.as_ref())
            .collect()
    }
    
    /// Migrate to a specific version
    pub fn migrate_to(
        &self,
        graph: &mut Graph,
        target_version: u64,
    ) -> Result<MigrationResult, MigrationError> {
        let current = self.current_version(graph);
        
        if target_version == current {
            return Ok(MigrationResult::NoChange);
        }
        
        if target_version > current {
            self.migrate_up(graph, target_version)
        } else {
            self.migrate_down(graph, target_version)
        }
    }
    
    /// Migrate to latest version
    pub fn migrate_to_latest(
        &self,
        graph: &mut Graph,
    ) -> Result<MigrationResult, MigrationError> {
        self.migrate_to(graph, self.latest_version())
    }
    
    /// Rollback last migration
    pub fn rollback(&self, graph: &mut Graph) -> Result<MigrationResult, MigrationError> {
        let current = self.current_version(graph);
        if current == 0 {
            return Ok(MigrationResult::NoChange);
        }
        self.migrate_to(graph, current - 1)
    }
    
    /// Dry run: validate migrations without applying
    pub fn dry_run(
        &self,
        graph: &Graph,
        target_version: u64,
    ) -> Result<MigrationPlan, MigrationError> {
        // Build and validate migration plan without executing
        todo!()
    }
}

/// Result of migration execution
#[derive(Debug)]
pub enum MigrationResult {
    /// No migrations were needed
    NoChange,
    
    /// Migrations were applied successfully
    Success {
        from_version: u64,
        to_version: u64,
        migrations_applied: usize,
        duration_ms: u64,
    },
}

/// Plan for pending migrations (for dry run)
#[derive(Debug)]
pub struct MigrationPlan {
    pub current_version: u64,
    pub target_version: u64,
    pub steps: Vec<MigrationStep>,
}

#[derive(Debug)]
pub struct MigrationStep {
    pub from_version: u64,
    pub to_version: u64,
    pub description: String,
    pub is_reversible: bool,
}
```

### 5.2 Usage Example

```rust
use rustgremlin::migration::*;

// Define migrations
let runner = MigrationRunner::new()
    .add(AddCreatedAtMigration)
    .add(RenameNameMigration)
    .add(ConvertAgeMigration)
    .add(SplitUserMigration);

// Check current state
println!("Current version: {}", runner.current_version(&graph));
println!("Latest version: {}", runner.latest_version());
println!("Pending migrations: {}", runner.pending(&graph).len());

// Dry run to see what would happen
let plan = runner.dry_run(&graph, runner.latest_version())?;
for step in &plan.steps {
    println!(
        "  {} -> {}: {} (reversible: {})",
        step.from_version,
        step.to_version,
        step.description,
        step.is_reversible
    );
}

// Apply all pending migrations
match runner.migrate_to_latest(&mut graph)? {
    MigrationResult::NoChange => println!("Already up to date"),
    MigrationResult::Success { from_version, to_version, .. } => {
        println!("Migrated from v{} to v{}", from_version, to_version);
    }
}

// Rollback if needed
runner.rollback(&mut graph)?;
```

---

## 6. Migration Safety

### 6.1 Validation Phase

Before applying migrations, the runner validates:

1. **Version continuity**: Migrations form a continuous chain
2. **Schema compatibility**: Target schema is valid
3. **Data compatibility**: Existing data can be transformed
4. **Reversibility**: Warn if rollback is not possible

```rust
impl MigrationRunner {
    fn validate_migration_chain(&self) -> Result<(), MigrationError> {
        // Sort by from_version
        let mut sorted: Vec<_> = self.migrations.iter().collect();
        sorted.sort_by_key(|m| m.from_version());
        
        // Check for gaps
        for window in sorted.windows(2) {
            let prev = window[0];
            let next = window[1];
            
            if prev.to_version() != next.from_version() {
                return Err(MigrationError::ValidationFailed {
                    reason: format!(
                        "Gap in migration chain: v{} -> v{} missing",
                        prev.to_version(),
                        next.from_version()
                    ),
                });
            }
        }
        
        Ok(())
    }
}
```

### 6.2 Transactional Execution

Migrations execute within a transaction:

```rust
impl MigrationRunner {
    fn apply_migration(
        &self,
        graph: &mut Graph,
        migration: &dyn Migration,
    ) -> Result<MigrationRecord, MigrationError> {
        let start = std::time::Instant::now();
        
        // Begin transaction
        let mut tx = graph.begin_transaction()?;
        
        // Create migration context
        let mut ctx = MigrationContext {
            graph: &mut tx,
            from_schema: migration.from_schema(),
            to_schema: migration.to_schema(),
            on_progress: None,
        };
        
        // Validate
        migration.validate(&ctx)?;
        
        // Apply
        match migration.up(&mut ctx) {
            Ok(()) => {
                // Update schema version
                tx.set_schema_version(migration.to_version())?;
                
                // Commit transaction
                tx.commit()?;
                
                Ok(MigrationRecord {
                    version: migration.to_version(),
                    applied_at: current_timestamp(),
                    duration_ms: start.elapsed().as_millis() as u64,
                    vertices_modified: ctx.vertices_modified,
                    edges_modified: ctx.edges_modified,
                    is_rollback: false,
                })
            }
            Err(e) => {
                // Rollback transaction
                tx.rollback();
                Err(e)
            }
        }
    }
}
```

### 6.3 Backup Recommendations

For production migrations:

```rust
/// Migration options
pub struct MigrationOptions {
    /// Create backup before migrating
    pub backup: bool,
    
    /// Backup path (if backup is true)
    pub backup_path: Option<PathBuf>,
    
    /// Stop on first error
    pub fail_fast: bool,
    
    /// Progress callback
    pub on_progress: Option<Box<dyn Fn(MigrationProgress) + Send>>,
    
    /// Dry run only (validate without applying)
    pub dry_run: bool,
}

impl Default for MigrationOptions {
    fn default() -> Self {
        Self {
            backup: true,
            backup_path: None,
            fail_fast: true,
            on_progress: None,
            dry_run: false,
        }
    }
}
```

---

## 7. Declarative Migrations

### 7.1 Schema Diff

For simple changes, migrations can be auto-generated from schema diffs:

```rust
/// Generate migration from schema difference
pub fn diff_schemas(
    from: &GraphSchema,
    to: &GraphSchema,
) -> Vec<SchemaChange> {
    let mut changes = Vec::new();
    
    // Detect vertex schema changes
    for (label, to_schema) in &to.vertex_schemas {
        match from.vertex_schemas.get(label) {
            None => {
                changes.push(SchemaChange::AddVertexLabel {
                    label: label.clone(),
                    schema: to_schema.clone(),
                });
            }
            Some(from_schema) => {
                // Detect property changes
                changes.extend(diff_properties(
                    label,
                    ElementType::Vertex,
                    &from_schema.properties,
                    &to_schema.properties,
                ));
            }
        }
    }
    
    // Detect removed vertex labels
    for label in from.vertex_schemas.keys() {
        if !to.vertex_schemas.contains_key(label) {
            changes.push(SchemaChange::RemoveVertexLabel {
                label: label.clone(),
            });
        }
    }
    
    // Similar for edge schemas...
    
    changes
}

#[derive(Debug, Clone)]
pub enum SchemaChange {
    AddVertexLabel { label: String, schema: VertexSchema },
    RemoveVertexLabel { label: String },
    AddEdgeLabel { label: String, schema: EdgeSchema },
    RemoveEdgeLabel { label: String },
    AddProperty { label: String, element_type: ElementType, property: PropertyDef },
    RemoveProperty { label: String, element_type: ElementType, key: String },
    ChangePropertyType { label: String, element_type: ElementType, key: String, from: PropertyType, to: PropertyType },
    RenameProperty { label: String, element_type: ElementType, from_key: String, to_key: String },
}

#[derive(Debug, Clone, Copy)]
pub enum ElementType {
    Vertex,
    Edge,
}
```

### 7.2 Auto-Generated Migration

```rust
/// Migration generated from schema diff
pub struct AutoMigration {
    from_version: u64,
    to_version: u64,
    changes: Vec<SchemaChange>,
}

impl Migration for AutoMigration {
    fn from_version(&self) -> u64 { self.from_version }
    fn to_version(&self) -> u64 { self.to_version }
    
    fn description(&self) -> &str {
        "Auto-generated migration"
    }
    
    fn up(&self, ctx: &mut MigrationContext) -> Result<(), MigrationError> {
        for change in &self.changes {
            match change {
                SchemaChange::AddProperty { label, element_type, property } => {
                    if let Some(default) = &property.default {
                        match element_type {
                            ElementType::Vertex => {
                                ctx.add_vertex_property(label, &property.key, default.clone())?;
                            }
                            ElementType::Edge => {
                                ctx.add_edge_property(label, &property.key, default.clone())?;
                            }
                        }
                    }
                }
                SchemaChange::RemoveProperty { label, element_type, key } => {
                    match element_type {
                        ElementType::Vertex => ctx.remove_vertex_property(label, key)?,
                        ElementType::Edge => ctx.remove_edge_property(label, key)?,
                    };
                }
                // Handle other changes...
                _ => {}
            }
        }
        Ok(())
    }
    
    fn down(&self, ctx: &mut MigrationContext) -> Option<Result<(), MigrationError>> {
        // Auto-migrations may not be reversible for all change types
        // Return None for destructive changes
        None
    }
}
```

---

## 8. CLI Integration

### 8.1 Migration Commands

```
rustgremlin migrate status         # Show current version and pending migrations
rustgremlin migrate up             # Apply all pending migrations
rustgremlin migrate up --to=5      # Migrate to specific version
rustgremlin migrate down           # Rollback last migration
rustgremlin migrate down --to=3    # Rollback to specific version
rustgremlin migrate plan           # Show what migrations would run (dry run)
rustgremlin migrate history        # Show migration history
rustgremlin migrate create <name>  # Create new migration template
```

### 8.2 Example Output

```
$ rustgremlin migrate status
Database: my_graph.db
Current version: 3
Latest version: 6

Pending migrations:
  4: Convert person.age from String to Int
  5: Split user vertices into admin and member labels  
  6: Add colleague edges between coworkers

$ rustgremlin migrate plan
Planning migration from v3 to v6...

Step 1: v3 -> v4
  Description: Convert person.age from String to Int
  Reversible: Yes
  Affected: ~1,000 person vertices

Step 2: v4 -> v5
  Description: Split user vertices into admin and member labels
  Reversible: Yes
  Affected: ~500 user vertices

Step 3: v5 -> v6
  Description: Add colleague edges between coworkers
  Reversible: Yes
  Affected: Creates ~2,500 new edges

Run 'rustgremlin migrate up' to apply these migrations.

$ rustgremlin migrate up
Backing up to my_graph.db.backup.20240102...
Applying migration v3 -> v4... done (125ms, 1000 vertices)
Applying migration v4 -> v5... done (89ms, 500 vertices)
Applying migration v5 -> v6... done (342ms, 2500 edges created)

Successfully migrated from v3 to v6.
```

---

## 9. Best Practices

### 9.1 Writing Migrations

1. **Keep migrations small**: One logical change per migration
2. **Make migrations reversible**: Always implement `down()` when possible
3. **Validate before transforming**: Use `validate()` to catch issues early
4. **Use batch operations**: Prefer `add_vertex_property()` over iterating manually
5. **Test migrations**: Run against a copy of production data before deploying

### 9.2 Handling Irreversible Changes

Some changes cannot be reversed:
- Removing a property (data is lost)
- Narrowing a type (e.g., `Float` to `Int` loses precision)
- Deleting vertices/edges

For these cases:
- Return `None` from `down()`
- Document in migration description
- Consider keeping old data in a renamed property

### 9.3 Large Dataset Migrations

For graphs with millions of elements:

```rust
impl Migration for LargeMigration {
    fn up(&self, ctx: &mut MigrationContext) -> Result<(), MigrationError> {
        // Process in batches
        let batch_size = 10_000;
        let total = ctx.graph.vertex_count_with_label("person");
        let mut processed = 0;
        
        loop {
            let batch: Vec<_> = ctx.vertices_with_label("person")
                .skip(processed)
                .take(batch_size)
                .collect();
            
            if batch.is_empty() {
                break;
            }
            
            for vertex in batch {
                // Transform vertex...
            }
            
            processed += batch_size;
            
            // Report progress
            if let Some(ref callback) = ctx.on_progress {
                callback(MigrationProgress {
                    phase: MigrationPhase::MigratingVertices,
                    total_items: total,
                    processed_items: processed as u64,
                    current_label: Some("person".to_string()),
                });
            }
        }
        
        Ok(())
    }
}
```

---

## 10. Summary

RustGremlin's migration system provides:

| Feature | Description |
|---------|-------------|
| **Versioned schemas** | Every schema has an explicit version number |
| **Incremental migrations** | Define transitions between adjacent versions |
| **Reversible changes** | Roll back migrations when possible |
| **Validation** | Verify migrations before applying |
| **Transactional** | Migrations are atomic — all or nothing |
| **Progress tracking** | Monitor long-running migrations |
| **Declarative option** | Auto-generate migrations from schema diffs |
| **CLI integration** | Manage migrations from command line |

The migration framework enables safe schema evolution while maintaining data integrity across versions.
