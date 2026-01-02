# Spec 03a: Path Tracking, AsStep, and SelectStep

**Addendum to Phase 3: Traversal Engine Core**

## Overview

This specification extends the traversal engine with path tracking capabilities, enabling traversers to record their journey through the graph. It introduces:

1. **Automatic path tracking** via `with_path()` - opt-in recording of all traversed elements
2. **`AsStep`** - explicit labeling of positions in the traversal path
3. **`SelectStep`** - retrieval of labeled values from the path

These features enable powerful graph query patterns like finding paths between vertices, tracking traversal history, and correlating values from different points in a traversal.

**Duration**: 3-4 hours  
**Priority**: High  
**Dependencies**: Spec 03 (Traversal Engine Core) - complete

---

## Goals

1. Implement opt-in automatic path tracking via `with_path()` method
2. Implement `AsStep` for explicit path labeling
3. Implement `SelectStep` for retrieving labeled values
4. Ensure path tracking integrates with existing `PathStep` (`path()` method)
5. Maintain zero overhead when path tracking is disabled (default)

---

## Architecture

### Design Principles

1. **Opt-in path tracking**: Path tracking is disabled by default for performance. Users explicitly enable it with `with_path()`.
2. **Zero-cost when disabled**: No memory allocation or CPU overhead when path tracking is off.
3. **Explicit labels always work**: `as_()` labels are always recorded regardless of `with_path()` setting.
4. **Gremlin compatibility**: API matches TinkerPop Gremlin semantics where applicable.

### Path Tracking Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| **Disabled (default)** | Path only contains explicitly labeled elements via `as_()` | Performance-critical queries |
| **Enabled (`with_path()`)** | Path contains all traversed elements (vertices, edges, values) | Path discovery, debugging |

### How It Works

```
                    ┌─────────────────────────────────────────────────┐
                    │           ExecutionContext                      │
                    │  ┌─────────────────────────────────────────┐   │
                    │  │  track_paths: bool                      │   │
                    │  │  (checked by navigation steps)          │   │
                    │  └─────────────────────────────────────────┘   │
                    └─────────────────────────────────────────────────┘
                                          │
                                          ▼
    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
    │  v()     │───▶│  out()   │───▶│  out()   │───▶│  path()  │
    │          │    │          │    │          │    │          │
    │ Start    │    │ Navigate │    │ Navigate │    │ Collect  │
    └──────────┘    └──────────┘    └──────────┘    └──────────┘
         │               │               │               │
         ▼               ▼               ▼               ▼
    Traverser:      Traverser:      Traverser:      Traverser:
    value: V0       value: V1       value: V2       value: [V0,V1,V2]
    path: []        path: [V0]      path: [V0,V1]   path: [V0,V1,V2]
                    (if tracking)   (if tracking)
```

---

## Detailed Specifications

### 1. ExecutionContext Changes

**File**: `src/traversal/context.rs`

Add a `track_paths` field to control automatic path recording:

```rust
pub struct ExecutionContext<'g> {
    /// Graph snapshot for consistent reads
    snapshot: &'g GraphSnapshot<'g>,
    /// String interner for label lookups
    interner: &'g StringInterner,
    /// Side effects storage (for store(), aggregate(), etc.)
    pub side_effects: SideEffects,
    /// Whether to automatically track paths through navigation steps
    pub track_paths: bool,  // NEW
}

impl<'g> ExecutionContext<'g> {
    /// Create a new execution context with path tracking disabled.
    pub fn new(snapshot: &'g GraphSnapshot<'g>, interner: &'g StringInterner) -> Self {
        Self {
            snapshot,
            interner,
            side_effects: SideEffects::new(),
            track_paths: false,  // Default: disabled
        }
    }

    /// Create a new execution context with path tracking enabled.
    pub fn with_path_tracking(
        snapshot: &'g GraphSnapshot<'g>, 
        interner: &'g StringInterner
    ) -> Self {
        Self {
            snapshot,
            interner,
            side_effects: SideEffects::new(),
            track_paths: true,
        }
    }

    /// Check if path tracking is enabled.
    #[inline]
    pub fn is_tracking_paths(&self) -> bool {
        self.track_paths
    }
}
```

### 2. BoundTraversal Changes

**File**: `src/traversal/source.rs`

Add `with_path()` method and track path setting:

```rust
pub struct BoundTraversal<'g, In, Out> {
    snapshot: &'g GraphSnapshot<'g>,
    interner: &'g StringInterner,
    traversal: Traversal<In, Out>,
    track_paths: bool,  // NEW
}

impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Enable automatic path tracking for this traversal.
    ///
    /// When enabled, all navigation steps (out, in_, both, etc.) will
    /// automatically add traversed elements to the path. This enables
    /// `path()` to return the complete traversal history.
    ///
    /// # Performance Note
    ///
    /// Path tracking adds memory overhead for storing path history.
    /// Only enable when you need path information.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Without path tracking - path() returns empty lists
    /// let paths = g.v().out().out().path().to_list();
    /// // paths = [[], [], ...]  (empty paths)
    ///
    /// // With path tracking - path() returns full history
    /// let paths = g.v().with_path().out().out().path().to_list();
    /// // paths = [[v0, v1, v2], [v0, v1, v3], ...]
    /// ```
    pub fn with_path(self) -> BoundTraversal<'g, In, Out> {
        BoundTraversal {
            snapshot: self.snapshot,
            interner: self.interner,
            traversal: self.traversal,
            track_paths: true,
        }
    }
}
```

Update terminal steps to pass `track_paths` to `ExecutionContext`:

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    pub fn to_list(self) -> Vec<Value> {
        let ctx = if self.track_paths {
            ExecutionContext::with_path_tracking(self.snapshot, self.interner)
        } else {
            ExecutionContext::new(self.snapshot, self.interner)
        };
        // ... execute traversal with ctx
    }
}
```

### 3. Navigation Steps Changes

**File**: `src/traversal/navigation.rs`

Update all navigation steps to check `ctx.track_paths` and extend path when enabled.

**Pattern for all navigation steps** (OutStep, InStep, BothStep, OutEStep, InEStep, BothEStep, OutVStep, InVStep, BothVStep):

```rust
impl OutStep {
    fn expand<'a>(&self, ctx: &'a ExecutionContext<'a>, t: Traverser) -> Vec<Traverser> {
        let vertex_id = match t.as_vertex_id() {
            Some(id) => id,
            None => return Vec::new(),
        };

        // ... existing label resolution logic ...

        ctx.snapshot()
            .storage()
            .out_edges(vertex_id)
            .filter_map(|edge| {
                // ... existing label filter logic ...
                
                // Create new traverser, optionally extending path
                let mut new_t = t.split(Value::Vertex(edge.dst));
                if ctx.track_paths {
                    new_t.extend_path_unlabeled();
                }
                Some(new_t)
            })
            .collect()
    }
}
```

### 4. StartStep Changes

**File**: `src/traversal/step.rs`

Update `StartStep` to add initial element to path when tracking is enabled:

```rust
impl AnyStep for StartStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        _input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let track_paths = ctx.track_paths;
        
        match &self.source {
            TraversalSource::AllVertices => {
                Box::new(
                    ctx.snapshot()
                        .storage()
                        .all_vertices()
                        .map(move |v| {
                            let mut t = Traverser::from_vertex(v.id);
                            if track_paths {
                                t.extend_path_unlabeled();
                            }
                            t
                        }),
                )
            }
            // ... similar for other sources ...
        }
    }
}
```

### 5. AsStep Implementation

**File**: `src/traversal/transform.rs`

```rust
/// Step that labels the current position in the traversal path.
///
/// The `as_()` step records the current traverser's value in the path
/// with the specified label. This enables later retrieval via `select()`.
///
/// Unlike automatic path tracking, `as_()` labels are always recorded
/// regardless of whether `with_path()` was called.
///
/// # Behavior
///
/// - Passes traversers through unchanged (identity behavior)
/// - Adds the current value to the path with the specified label
/// - Multiple `as_()` calls with the same label create multiple entries
///
/// # Example
///
/// ```ignore
/// // Label positions for later selection
/// g.v().as_("start").out().as_("end").select(&["start", "end"])
///
/// // Multiple labels at same position
/// g.v().as_("a").as_("b").select(&["a", "b"])  // Both return same vertex
/// ```
#[derive(Clone, Debug)]
pub struct AsStep {
    label: String,
}

impl AsStep {
    /// Create a new AsStep with the given label.
    pub fn new(label: impl Into<String>) -> Self {
        Self { label: label.into() }
    }

    /// Get the label for this step.
    pub fn label(&self) -> &str {
        &self.label
    }
}

impl AnyStep for AsStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let label = self.label.clone();
        Box::new(input.map(move |mut t| {
            // Always add to path with label (regardless of track_paths setting)
            t.extend_path_labeled(&label);
            t
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "as"
    }
}
```

### 6. SelectStep Implementation

**File**: `src/traversal/transform.rs`

```rust
/// Step that retrieves labeled values from the traversal path.
///
/// The `select()` step looks up values in the path by their labels
/// (assigned via `as_()` steps) and returns them.
///
/// # Behavior
///
/// - **Single label**: Returns the value directly
/// - **Multiple labels**: Returns a `Value::Map` with label keys
/// - **Missing labels**: Traversers with no matching labels are filtered out
/// - **Multiple values per label**: Returns the *last* value for each label
///
/// # Example
///
/// ```ignore
/// // Single label - returns value directly
/// g.v().as_("x").out().select_one("x")  // Returns vertices
///
/// // Multiple labels - returns Map
/// g.v().as_("a").out().as_("b").select(&["a", "b"])
/// // Returns Map { "a" -> vertex1, "b" -> vertex2 }
///
/// // Missing label - filtered out
/// g.v().as_("x").select_one("y")  // Returns nothing (no "y" label)
/// ```
#[derive(Clone, Debug)]
pub struct SelectStep {
    labels: Vec<String>,
}

impl SelectStep {
    /// Create a SelectStep for multiple labels.
    ///
    /// Returns a `Value::Map` with the labeled values.
    pub fn new(labels: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            labels: labels.into_iter().map(Into::into).collect(),
        }
    }

    /// Create a SelectStep for a single label.
    ///
    /// Returns the value directly (not wrapped in a Map).
    pub fn single(label: impl Into<String>) -> Self {
        Self {
            labels: vec![label.into()],
        }
    }

    /// Get the labels for this step.
    pub fn labels(&self) -> &[String] {
        &self.labels
    }

    /// Check if this is a single-label select.
    pub fn is_single(&self) -> bool {
        self.labels.len() == 1
    }
}

impl AnyStep for SelectStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let labels = self.labels.clone();
        let is_single = self.labels.len() == 1;

        Box::new(input.filter_map(move |t| {
            if is_single {
                // Single label: return value directly
                let label = &labels[0];
                t.path
                    .get(label)
                    .and_then(|values| values.last().cloned())
                    .map(|pv| t.with_value(pv.to_value()))
            } else {
                // Multiple labels: return Map
                let mut map = std::collections::HashMap::new();
                for label in &labels {
                    if let Some(values) = t.path.get(label) {
                        if let Some(last) = values.last() {
                            map.insert(label.clone(), last.to_value());
                        }
                    }
                }
                if map.is_empty() {
                    None // No labels found, filter out traverser
                } else {
                    Some(t.with_value(Value::Map(map)))
                }
            }
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "select"
    }
}
```

### 7. API Methods

**File**: `src/traversal/source.rs` - BoundTraversal methods

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Label the current traversal position.
    ///
    /// Records the current value in the path with the given label,
    /// enabling later retrieval via `select()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// g.v().as_("start").out().as_("end").select(&["start", "end"])
    /// ```
    pub fn as_(self, label: &str) -> BoundTraversal<'g, In, Out> {
        use crate::traversal::transform::AsStep;
        self.add_step(AsStep::new(label))
    }

    /// Select multiple labeled values from path.
    ///
    /// Returns a `Value::Map` with the labeled values.
    /// Traversers with no matching labels are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// g.v().as_("a").out().as_("b").select(&["a", "b"])
    /// // Returns Map { "a" -> v1, "b" -> v2 }
    /// ```
    pub fn select(self, labels: &[&str]) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::SelectStep;
        self.add_step(SelectStep::new(labels.iter().map(|s| s.to_string())))
    }

    /// Select a single labeled value from path.
    ///
    /// Returns the value directly (not wrapped in a Map).
    /// Traversers without the label are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// g.v().as_("x").out().out().select_one("x")
    /// // Returns the original vertices
    /// ```
    pub fn select_one(self, label: &str) -> BoundTraversal<'g, In, Value> {
        use crate::traversal::transform::SelectStep;
        self.add_step(SelectStep::single(label))
    }
}
```

**File**: `src/traversal/mod.rs` - Anonymous Traversal methods

```rust
impl<In, Out> Traversal<In, Out> {
    /// Label the current traversal position (anonymous traversal).
    pub fn as_(self, label: &str) -> Traversal<In, Out> {
        use crate::traversal::transform::AsStep;
        self.add_step(AsStep::new(label))
    }

    /// Select multiple labeled values (anonymous traversal).
    pub fn select(self, labels: &[&str]) -> Traversal<In, Value> {
        use crate::traversal::transform::SelectStep;
        self.add_step(SelectStep::new(labels.iter().map(|s| s.to_string())))
    }

    /// Select a single labeled value (anonymous traversal).
    pub fn select_one(self, label: &str) -> Traversal<In, Value> {
        use crate::traversal::transform::SelectStep;
        self.add_step(SelectStep::single(label))
    }
}
```

### 8. Module Exports

**File**: `src/traversal/mod.rs`

Update the export line:

```rust
pub use transform::{
    AsStep, ConstantStep, FlatMapStep, IdStep, LabelStep, 
    MapStep, PathStep, SelectStep, ValuesStep
};
```

---

## Usage Examples

### Basic Path Tracking

```rust
// Without path tracking (default) - path() returns empty
let paths = g.v().out().out().path().to_list();
// paths = [Value::List([]), Value::List([]), ...]

// With path tracking - path() returns full history  
let paths = g.v().with_path().out().out().path().to_list();
// paths = [Value::List([V0, V1, V2]), Value::List([V0, V1, V3]), ...]
```

### Explicit Labels with as_() and select()

```rust
// Label specific positions
let results = g.v()
    .as_("start")
    .out()
    .as_("middle") 
    .out()
    .as_("end")
    .select(&["start", "middle", "end"])
    .to_list();
// results = [Map{"start": V0, "middle": V1, "end": V2}, ...]

// Single label selection
let origins = g.v()
    .as_("origin")
    .out().out().out()
    .select_one("origin")
    .to_list();
// origins = [V0, V0, V0, ...]  (original vertices)
```

### Mixed: Path Tracking + Labels

```rust
// Both automatic tracking and explicit labels
let results = g.v()
    .with_path()
    .as_("start")
    .out()
    .out()
    .as_("end")
    .to_list();

// Each traverser has:
// - path: [V0, V1, V2] (automatic tracking)
// - path labels: "start" -> V0, "end" -> V2 (explicit)
```

### Finding Paths Between Vertices

```rust
// Find all paths from Alice to any software
let paths = g.v()
    .has("name", "Alice")
    .with_path()
    .repeat(__.out())
    .until(__.has_label("software"))
    .path()
    .to_list();
```

---

## Exit Criteria

### ExecutionContext
- [ ] `track_paths` field added to `ExecutionContext`
- [ ] `with_path_tracking()` constructor works
- [ ] `is_tracking_paths()` accessor works

### BoundTraversal
- [ ] `with_path()` method enables path tracking
- [ ] `track_paths` setting passed to `ExecutionContext` at execution time
- [ ] Default is `track_paths: false`

### Navigation Steps (9 steps)
- [ ] `OutStep` adds to path when tracking enabled
- [ ] `InStep` adds to path when tracking enabled
- [ ] `BothStep` adds to path when tracking enabled
- [ ] `OutEStep` adds to path when tracking enabled
- [ ] `InEStep` adds to path when tracking enabled
- [ ] `BothEStep` adds to path when tracking enabled
- [ ] `OutVStep` adds to path when tracking enabled
- [ ] `InVStep` adds to path when tracking enabled
- [ ] `BothVStep` adds to path when tracking enabled

### StartStep
- [ ] Adds initial element to path when tracking enabled

### AsStep
- [ ] `AsStep::new(label)` creates step
- [ ] Passes traversers through unchanged
- [ ] Adds current value to path with label
- [ ] Works regardless of `track_paths` setting

### SelectStep
- [ ] `SelectStep::new(labels)` creates multi-label step
- [ ] `SelectStep::single(label)` creates single-label step
- [ ] Single label returns value directly
- [ ] Multiple labels return `Value::Map`
- [ ] Missing labels filter out traverser
- [ ] Returns last value when multiple values exist for a label

### API Methods
- [ ] `BoundTraversal::with_path()` works
- [ ] `BoundTraversal::as_()` works
- [ ] `BoundTraversal::select()` works
- [ ] `BoundTraversal::select_one()` works
- [ ] `Traversal::as_()` works (anonymous)
- [ ] `Traversal::select()` works (anonymous)
- [ ] `Traversal::select_one()` works (anonymous)

### Integration
- [ ] `g.v().with_path().out().path()` returns non-empty paths
- [ ] `g.v().as_("x").select_one("x")` returns labeled values
- [ ] `g.v().as_("a").out().as_("b").select(&["a","b"])` returns Map
- [ ] Path labels preserved through anonymous traversals

### Performance
- [ ] No overhead when `with_path()` not called
- [ ] Benchmark confirms path tracking adds < 20% overhead

---

## Test Cases

### Unit Tests

1. **ExecutionContext tests**
   - `new()` has `track_paths: false`
   - `with_path_tracking()` has `track_paths: true`

2. **AsStep tests**
   - Construction with various label strings
   - Identity behavior (value unchanged)
   - Path extended with label
   - Multiple labels on same traverser

3. **SelectStep tests**
   - Single label returns value directly
   - Multiple labels return Map
   - Missing label filters traverser
   - Partial match (some labels exist) returns partial Map
   - Last value returned for duplicate labels

4. **Navigation step path tracking tests**
   - Each navigation step with tracking enabled
   - Each navigation step with tracking disabled
   - Verify path contains correct elements

### Integration Tests

1. **Full traversal with path tracking**
   ```rust
   g.v().with_path().out().out().path().to_list()
   ```

2. **as_() and select() chain**
   ```rust
   g.v().as_("start").out().as_("end").select(&["start", "end"]).to_list()
   ```

3. **Mixed tracking and labels**
   ```rust
   g.v().with_path().as_("x").out().path().to_list()
   ```

4. **Anonymous traversal with labels**
   ```rust
   g.v().where_(__.as_("inner").out()).select_one("inner").to_list()
   ```
