# Specification: `explain()` and `profile()` Terminal Methods

This specification defines the implementation of two terminal methods for the Interstellar traversal API that provide introspection and performance analysis capabilities.

## Overview

| Method | Purpose | Executes Traversal | Returns |
|--------|---------|-------------------|---------|
| `explain()` | Describe traversal plan | No | `TraversalExplanation` |
| `profile()` | Execute with metrics | Yes | `TraversalProfile` |

### Gremlin Compatibility

These methods align with TinkerPop 3.x semantics:

```groovy
// TinkerPop Gremlin
g.V().out('knows').hasLabel('person').explain()
g.V().out('knows').hasLabel('person').profile()
```

```rust
// Interstellar Rust
g.v().out_labels(&["knows"]).has_label("person").explain()
g.v().out_labels(&["knows"]).has_label("person").profile()
```

---

## Part 1: `explain()` Method

### 1.1 Purpose

Returns a structured description of the traversal pipeline without executing it. Useful for:

- Understanding query plans
- Debugging traversal construction
- Identifying barrier steps that prevent streaming
- Documentation and logging

### 1.2 Output Types

#### `TraversalExplanation`

```rust
/// Complete explanation of a traversal pipeline.
#[derive(Debug, Clone)]
pub struct TraversalExplanation {
    /// Human-readable representation of the original traversal
    pub original: String,
    
    /// Source description (e.g., "V()", "V(1,2,3)", "E()")
    pub source: Option<String>,
    
    /// Ordered list of step explanations
    pub steps: Vec<StepExplanation>,
    
    /// Whether traversal contains any barrier steps
    pub has_barriers: bool,
    
    /// Whether path tracking is enabled
    pub tracks_paths: bool,
    
    /// Total number of steps (excluding source)
    pub step_count: usize,
}
```

#### `StepExplanation`

```rust
/// Explanation of a single traversal step.
#[derive(Debug, Clone)]
pub struct StepExplanation {
    /// Step name (e.g., "hasLabel", "out", "group")
    pub name: &'static str,
    
    /// Zero-based index in the pipeline
    pub index: usize,
    
    /// Whether this step is a barrier (blocks streaming)
    pub is_barrier: bool,
    
    /// Step category for grouping
    pub category: StepCategory,
    
    /// Optional configuration description
    pub description: Option<String>,
}
```

#### `StepCategory`

```rust
/// Category of traversal step for classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepCategory {
    /// Source steps: V(), E(), inject()
    Source,
    /// Navigation: out(), in_(), both(), outE(), etc.
    Navigation,
    /// Filter: has(), hasLabel(), where_(), limit(), etc.
    Filter,
    /// Transform: values(), valueMap(), id(), label(), etc.
    Transform,
    /// Aggregation: group(), groupCount(), fold(), etc.
    Aggregation,
    /// Branch: union(), coalesce(), choose(), etc.
    Branch,
    /// Side Effect: aggregate(), store(), sideEffect(), etc.
    SideEffect,
    /// Modulator: as_(), by(), etc.
    Modulator,
    /// Unknown/custom steps
    Other,
}
```

### 1.3 Display Format

The `Display` implementation produces human-readable output:

```
Traversal Explanation
=====================
Original: g.v().out_labels(["knows"]).has_label("person").values("name")
Source: V() [all vertices]
Barriers: No
Path Tracking: No

Steps (4):
  [0] out        Navigation   labels: ["knows"]
  [1] hasLabel   Filter       labels: ["person"]
  [2] values     Transform    key: "name"
  [3] identity   Transform    
```

For traversals with barriers:

```
Traversal Explanation
=====================
Original: g.v().group().by_label().build()
Source: V() [all vertices]
Barriers: Yes (streaming disabled)
Path Tracking: No

Steps (1):
  [0] group     Aggregation  BARRIER  by: label
```

### 1.4 API Surface

#### On `Traversal<In, Out>` (anonymous traversals)

```rust
impl<In, Out> Traversal<In, Out> {
    /// Returns an explanation of this traversal without executing it.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::traversal::__;
    ///
    /// let explanation = __.out().has_label("person").explain();
    /// println!("{}", explanation);
    /// ```
    pub fn explain(&self) -> TraversalExplanation;
}
```

#### On `BoundTraversal<'g, In, Out>` (bound traversals)

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Returns an explanation of this traversal without executing it.
    ///
    /// This is a terminal method that consumes the traversal.
    ///
    /// # Example
    ///
    /// ```rust
    /// let g = graph.gremlin();
    /// let explanation = g.v().out().has_label("person").explain();
    /// println!("{}", explanation);
    /// // Traversal is consumed, cannot call to_list() after explain()
    /// ```
    pub fn explain(self) -> TraversalExplanation;
}
```

### 1.5 Step Trait Extension

Add optional `describe()` and `category()` methods to the `Step` trait:

```rust
pub trait Step: Clone + Send + Sync + 'static {
    // ... existing methods ...

    /// Returns an optional description of step configuration.
    ///
    /// Override this to provide useful debugging information about
    /// the step's parameters. Returns `None` by default.
    ///
    /// # Example
    ///
    /// ```rust
    /// fn describe(&self) -> Option<String> {
    ///     Some(format!("labels: {:?}", self.labels))
    /// }
    /// ```
    fn describe(&self) -> Option<String> {
        None
    }

    /// Returns the category of this step.
    ///
    /// Used for classification in explain() output.
    /// Defaults to `StepCategory::Other`.
    fn category(&self) -> StepCategory {
        StepCategory::Other
    }
}
```

Update `DynStep` trait to include these methods:

```rust
pub trait DynStep: Send + Sync {
    // ... existing methods ...
    
    fn describe(&self) -> Option<String>;
    fn category(&self) -> StepCategory;
}
```

---

## Part 2: `profile()` Method

### 2.1 Purpose

Executes the traversal while collecting detailed performance metrics for each step. Useful for:

- Performance optimization
- Identifying bottlenecks
- Understanding traversal cost
- Capacity planning

### 2.2 Output Types

#### `TraversalProfile`

```rust
/// Complete profiling results for a traversal execution.
#[derive(Debug, Clone)]
pub struct TraversalProfile {
    /// Total wall-clock execution time
    pub total_duration: Duration,
    
    /// Total traversers produced (respecting bulk)
    pub total_traversers: u64,
    
    /// Per-step profiling metrics
    pub steps: Vec<StepProfile>,
    
    /// Side effects captured during execution
    pub side_effects: HashMap<String, Vec<Value>>,
    
    /// Whether path tracking was enabled
    pub tracked_paths: bool,
    
    /// Execution mode used
    pub execution_mode: ExecutionMode,
}
```

#### `StepProfile`

```rust
/// Profiling metrics for a single step.
#[derive(Debug, Clone)]
pub struct StepProfile {
    /// Step name
    pub name: &'static str,
    
    /// Zero-based index in the pipeline
    pub index: usize,
    
    /// Time spent executing this step
    pub duration: Duration,
    
    /// Percentage of total time
    pub duration_pct: f64,
    
    /// Number of traversers that entered this step
    pub traversers_in: u64,
    
    /// Number of traversers that exited this step
    pub traversers_out: u64,
    
    /// Expansion/reduction ratio (out/in)
    pub ratio: f64,
    
    /// Whether this is a barrier step
    pub is_barrier: bool,
    
    /// Step category
    pub category: StepCategory,
    
    /// Step description (from describe())
    pub description: Option<String>,
}
```

#### `ExecutionMode`

```rust
/// How the traversal was executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    /// Lazy/streaming execution (O(1) memory per step)
    Streaming,
    /// Eager execution (barriers present)
    Eager,
    /// Profiling mode (wrapped steps)
    Profiling,
}
```

### 2.3 Display Format

```
Traversal Profile
=================
Total Time: 12.45ms
Traversers: 1,523
Execution: Profiling
Path Tracking: No

Step Metrics:
  #   Step        Category     Time      %     In    Out   Ratio
  --- ----------- ------------ --------- ----- ----- ----- ------
  [0] start       Source       0.12ms    1.0%     0  1000  -
  [1] out         Navigation   8.23ms   66.1%  1000  2500  2.50x
  [2] hasLabel    Filter       2.10ms   16.9%  2500  1523  0.61x
  [3] values      Transform    2.00ms   16.1%  1523  1523  1.00x

Side Effects: (none)
```

With barriers:

```
Traversal Profile
=================
Total Time: 45.67ms
Traversers: 3
Execution: Profiling (eager - barriers present)

Step Metrics:
  #   Step        Category     Time      %     In    Out   Ratio   Notes
  --- ----------- ------------ --------- ----- ----- ----- ------- -------
  [0] start       Source       0.15ms    0.3%     0  1000  -
  [1] out         Navigation  12.34ms   27.0%  1000  5000  5.00x
  [2] group       Aggregation 30.00ms   65.7%  5000     3  0.00x   BARRIER
  [3] unfold      Transform    3.18ms    7.0%     3    15  5.00x

Side Effects:
  "myKey": [Value::Int(42), Value::Int(100)]
```

### 2.4 API Surface

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Execute the traversal with profiling, returning detailed metrics.
    ///
    /// This terminal method:
    /// 1. Wraps each step in a profiling wrapper
    /// 2. Executes the traversal to completion
    /// 3. Collects timing and count metrics per step
    /// 4. Returns a `TraversalProfile` with all metrics
    ///
    /// # Note
    ///
    /// Profiling adds overhead. The reported times include this overhead,
    /// so actual execution without profiling will be faster.
    ///
    /// # Example
    ///
    /// ```rust
    /// let profile = g.v()
    ///     .out_labels(&["knows"])
    ///     .has_label("person")
    ///     .values("name")
    ///     .profile();
    ///
    /// println!("{}", profile);
    /// println!("Slowest step: {:?}", profile.slowest_step());
    /// ```
    pub fn profile(self) -> TraversalProfile;
}
```

### 2.5 Implementation: `ProfilingStep<S>`

A wrapper step that captures metrics:

```rust
/// Wrapper step that captures profiling metrics.
#[derive(Clone)]
pub struct ProfilingStep<S> {
    /// The wrapped step
    inner: S,
    /// Shared metrics storage
    metrics: Arc<Mutex<StepMetrics>>,
    /// Step index for identification
    index: usize,
}

/// Mutable metrics accumulated during execution.
#[derive(Debug, Default)]
struct StepMetrics {
    /// Total time spent in apply()
    total_duration: Duration,
    /// Count of traversers entering
    traversers_in: u64,
    /// Count of traversers exiting
    traversers_out: u64,
}
```

The `ProfilingStep` implements `Step` by:

1. Recording start time before calling inner step
2. Counting input traversers
3. Wrapping output iterator to count outputs and accumulate time
4. Storing metrics in shared `Arc<Mutex<_>>`

```rust
impl<S: Step> Step for ProfilingStep<S> {
    type Iter<'a> = ProfilingIterator<'a, S::Iter<'a>> where Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let start = Instant::now();
        
        // Count inputs via passthrough iterator
        let counting_input = CountingIterator::new(input, self.metrics.clone(), true);
        
        // Apply inner step
        let inner_iter = self.inner.apply(ctx, Box::new(counting_input));
        
        // Wrap output to count and time
        ProfilingIterator::new(inner_iter, self.metrics.clone(), start)
    }

    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn describe(&self) -> Option<String> {
        self.inner.describe()
    }

    fn category(&self) -> StepCategory {
        self.inner.category()
    }

    fn is_barrier(&self) -> bool {
        self.inner.is_barrier()
    }
}
```

### 2.6 Helper Methods on `TraversalProfile`

```rust
impl TraversalProfile {
    /// Returns the step that took the most time.
    pub fn slowest_step(&self) -> Option<&StepProfile> {
        self.steps.iter().max_by_key(|s| s.duration)
    }

    /// Returns steps sorted by duration (descending).
    pub fn steps_by_duration(&self) -> Vec<&StepProfile> {
        let mut sorted: Vec<_> = self.steps.iter().collect();
        sorted.sort_by(|a, b| b.duration.cmp(&a.duration));
        sorted
    }

    /// Returns the step with the highest expansion ratio.
    pub fn most_expanding_step(&self) -> Option<&StepProfile> {
        self.steps.iter()
            .filter(|s| s.traversers_in > 0)
            .max_by(|a, b| a.ratio.partial_cmp(&b.ratio).unwrap_or(Ordering::Equal))
    }

    /// Returns the step with the lowest pass-through ratio (most filtering).
    pub fn most_filtering_step(&self) -> Option<&StepProfile> {
        self.steps.iter()
            .filter(|s| s.traversers_in > 0 && s.ratio < 1.0)
            .min_by(|a, b| a.ratio.partial_cmp(&b.ratio).unwrap_or(Ordering::Equal))
    }

    /// Returns total time spent in barrier steps.
    pub fn barrier_time(&self) -> Duration {
        self.steps.iter()
            .filter(|s| s.is_barrier)
            .map(|s| s.duration)
            .sum()
    }

    /// Returns percentage of time spent in barriers.
    pub fn barrier_time_pct(&self) -> f64 {
        if self.total_duration.is_zero() {
            0.0
        } else {
            self.barrier_time().as_secs_f64() / self.total_duration.as_secs_f64() * 100.0
        }
    }
}
```

---

## Part 3: Step `describe()` Implementations

Each step should implement `describe()` to provide useful configuration info. Priority order:

### High Priority (commonly used, configuration-heavy)

| Step | `describe()` Output |
|------|---------------------|
| `HasLabelStep` | `labels: ["person", "software"]` |
| `OutStep` / `InStep` / `BothStep` | `labels: ["knows"]` or `labels: [all]` |
| `OutEStep` / `InEStep` / `BothEStep` | `labels: ["created"]` |
| `HasValueStep` | `key: "age", value: 30` |
| `HasStep` | `key: "name"` |
| `HasWhereStep` | `key: "age", predicate: gt(25)` |
| `LimitStep` | `limit: 10` |
| `RangeStep` | `range: 5..15` |
| `ValuesStep` | `keys: ["name", "age"]` |
| `GroupStep` | `by: label` or `by: key("type")` |
| `OrderStep` | `by: key("age"), direction: DESC` |

### Medium Priority

| Step | `describe()` Output |
|------|---------------------|
| `AsStep` | `label: "a"` |
| `SelectStep` | `labels: ["a", "b"]` |
| `WhereStep` | `predicate: eq("a")` |
| `IsStep` | `predicate: gt(10)` |
| `DedupStep` | `by: key("id")` or `global` |
| `CoinStep` | `probability: 0.5` |
| `SampleStep` | `size: 100` |
| `RepeatStep` | `times: 3` or `until: hasLabel("end")` |

### Low Priority (simple/obvious)

| Step | `describe()` Output |
|------|---------------------|
| `IdentityStep` | `None` |
| `IdStep` | `None` |
| `LabelStep` | `None` |
| `CountStep` | `None` |
| `FoldStep` | `None` |

---

## Part 4: File Structure

### New Files

```
src/traversal/
├── explain.rs      # TraversalExplanation, StepExplanation, StepCategory
├── profile.rs      # TraversalProfile, StepProfile, ProfilingStep, ExecutionMode
```

### Modified Files

```
src/traversal/
├── mod.rs          # Export new modules
├── step.rs         # Add describe(), category() to Step and DynStep traits
├── pipeline.rs     # Add explain() to Traversal<In, Out>
├── source.rs       # Add explain(), profile() terminals to BoundTraversal
├── filter.rs       # Implement describe() for filter steps
├── navigation.rs   # Implement describe() for navigation steps
├── transform.rs    # Implement describe() for transform steps
├── aggregation.rs  # Implement describe() for aggregation steps
├── branch.rs       # Implement describe() for branch steps
├── sideeffect.rs   # Implement describe() for side effect steps
```

### Documentation Updates

```
docs/api/gremlin.md  # Update tables to show explain/profile as implemented
```

---

## Part 5: Testing Strategy

### Unit Tests

#### `explain.rs` tests

```rust
#[test]
fn explain_empty_traversal() {
    let t: Traversal<Value, Value> = Traversal::new();
    let exp = t.explain();
    assert_eq!(exp.step_count, 0);
    assert!(!exp.has_barriers);
}

#[test]
fn explain_captures_step_names() {
    let t = __.out().has_label("person").values("name");
    let exp = t.explain();
    assert_eq!(exp.steps.len(), 3);
    assert_eq!(exp.steps[0].name, "out");
    assert_eq!(exp.steps[1].name, "hasLabel");
    assert_eq!(exp.steps[2].name, "values");
}

#[test]
fn explain_detects_barriers() {
    let t = __.out().group().by_label().build();
    let exp = t.explain();
    assert!(exp.has_barriers);
    assert!(exp.steps.iter().any(|s| s.is_barrier));
}

#[test]
fn explain_display_format() {
    let t = __.out().has_label("person");
    let exp = t.explain();
    let display = format!("{}", exp);
    assert!(display.contains("out"));
    assert!(display.contains("hasLabel"));
}
```

#### `profile.rs` tests

```rust
#[test]
fn profile_captures_total_time() {
    let g = test_graph().gremlin();
    let profile = g.v().out().to_list_profile();
    assert!(profile.total_duration > Duration::ZERO);
}

#[test]
fn profile_counts_traversers() {
    let g = test_graph().gremlin(); // Graph with known structure
    let profile = g.v().profile();
    assert_eq!(profile.total_traversers, expected_vertex_count);
}

#[test]
fn profile_per_step_metrics() {
    let g = test_graph().gremlin();
    let profile = g.v().out().has_label("person").profile();
    
    assert_eq!(profile.steps.len(), 3); // start, out, hasLabel
    
    // Each step should have timing
    for step in &profile.steps {
        assert!(step.duration <= profile.total_duration);
    }
    
    // Traverser counts should chain
    assert!(profile.steps[1].traversers_in > 0);
}

#[test]
fn profile_barrier_detection() {
    let g = test_graph().gremlin();
    let profile = g.v().group().by_label().build().profile();
    
    let barrier_step = profile.steps.iter().find(|s| s.name == "group").unwrap();
    assert!(barrier_step.is_barrier);
}

#[test]
fn profile_display_format() {
    let g = test_graph().gremlin();
    let profile = g.v().out().profile();
    let display = format!("{}", profile);
    
    assert!(display.contains("Total Time:"));
    assert!(display.contains("Traversers:"));
    assert!(display.contains("out"));
}

#[test]
fn profile_helper_methods() {
    let g = test_graph().gremlin();
    let profile = g.v().out().has_label("person").profile();
    
    assert!(profile.slowest_step().is_some());
    assert!(!profile.steps_by_duration().is_empty());
}
```

### Integration Tests

```rust
#[test]
fn explain_matches_execution() {
    let g = test_graph().gremlin();
    
    // Get explanation
    let traversal = g.v().out_labels(&["knows"]).has_label("person");
    let explanation = traversal.clone().explain();
    
    // Execute
    let results = traversal.to_list();
    
    // Explanation step count should match
    assert_eq!(explanation.step_count, 2); // out, hasLabel (source not counted)
}

#[test]
fn profile_results_match_normal_execution() {
    let g = test_graph().gremlin();
    
    // Normal execution
    let normal_results = g.v().out().has_label("person").to_list();
    
    // Profiled execution
    let profile = g.v().out().has_label("person").profile();
    
    // Should produce same count
    assert_eq!(profile.total_traversers, normal_results.len() as u64);
}
```

---

## Part 6: Implementation Order

### Phase 1: Foundation (Step trait changes)
1. Add `describe()` and `category()` to `Step` trait with defaults
2. Add corresponding methods to `DynStep` trait
3. Update blanket impl

### Phase 2: Explain
1. Create `src/traversal/explain.rs` with types
2. Implement `Display` for `TraversalExplanation`
3. Add `explain()` to `Traversal<In, Out>`
4. Add `explain()` terminal to `BoundTraversal`
5. Add tests

### Phase 3: Profile
1. Create `src/traversal/profile.rs` with types
2. Implement `ProfilingStep` wrapper
3. Implement `ProfilingIterator` for timing
4. Add `profile()` terminal to `BoundTraversal`
5. Implement `Display` for `TraversalProfile`
6. Add helper methods
7. Add tests

### Phase 4: Step Descriptions
1. Implement `describe()` for high-priority steps
2. Implement `category()` for all steps
3. Add tests for describe output

### Phase 5: Documentation
1. Update `docs/api/gremlin.md`
2. Add examples to doc comments
3. Update AGENTS.md if needed

---

## Part 7: Error Handling

### `explain()` - No errors possible

`explain()` only inspects the traversal structure and cannot fail.

### `profile()` - Same errors as normal execution

`profile()` executes the traversal, so it can encounter the same errors:
- Storage errors (vertex/edge not found)
- Type errors in predicates
- Arithmetic errors in math expressions

The `profile()` method should return `TraversalProfile` (not `Result`), capturing any errors that occur. If execution fails partway through, the profile should contain partial metrics up to the failure point.

---

## Part 8: Performance Considerations

### `explain()` Overhead
- O(n) where n = number of steps
- No graph access, no execution
- Negligible overhead

### `profile()` Overhead
- Adds timing calls around each step
- Atomic operations for counters
- Estimated 5-15% overhead vs normal execution
- Memory: O(steps) for metrics storage

### Recommendations
- Use `explain()` freely in development/debugging
- Use `profile()` for performance analysis, not production hot paths
- Document that profile times include profiling overhead

---

## Appendix A: Example Outputs

### explain() Output

```
Traversal Explanation
=====================
Original: g.v().out_labels(["knows"]).has_label("person").values("name").limit(10)
Source: V() [all vertices]
Barriers: No
Path Tracking: No

Steps (4):
  [0] out        Navigation   labels: ["knows"]
  [1] hasLabel   Filter       labels: ["person"]
  [2] values     Transform    keys: ["name"]
  [3] limit      Filter       limit: 10
```

### profile() Output

```
Traversal Profile
=================
Total Time: 15.23ms
Traversers: 847
Execution: Profiling
Path Tracking: No

Step Metrics:
  #   Step        Category     Time      %      In    Out   Ratio
  --- ----------- ------------ --------- ------ ----- ----- ------
  [0] start       Source       0.45ms    3.0%      0  1000  -
  [1] out         Navigation   9.12ms   59.9%   1000  3500  3.50x
  [2] hasLabel    Filter       3.21ms   21.1%   3500   847  0.24x
  [3] values      Transform    2.45ms   16.1%    847   847  1.00x

Side Effects: (none)

Analysis:
  Slowest step: out (59.9%)
  Most filtering: hasLabel (76% filtered)
  Barrier time: 0ms (0%)
```

---

## Appendix B: Future Enhancements

Not in scope for initial implementation, but potential future work:

1. **Cardinality Estimation**: Add estimated output cardinality to `explain()`
2. **Query Plan Optimization**: Use explain data to suggest optimizations
3. **Memory Profiling**: Track peak memory per step
4. **Async Profiling**: Support for async traversal execution
5. **Flame Graph Export**: Export profile data for visualization tools
