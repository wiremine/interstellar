# Plan 20: CALL Subquery Implementation

## Overview

This plan implements CALL subqueries as specified in `specs/spec-19-call-subquery.md`. CALL subqueries enable correlated computations, nested aggregations, and query composition within GQL queries.

**Estimated effort**: 4-5 days  
**Complexity**: High (touches parser, AST, compiler, and execution engine)  
**Dependencies**: None (builds on existing row-based execution infrastructure)

## Implementation Phases

### Phase 1: Grammar Extension (4-6 hours)

**Goal**: Add CALL subquery syntax to the pest grammar.

#### Tasks

1. **Add CALL keyword** to keyword list in `grammar.pest`
   ```pest
   CALL = @{ ^"call" ~ !ASCII_ALPHANUMERIC }
   ```

2. **Add call_clause rule**
   ```pest
   call_clause = { CALL ~ "{" ~ call_body ~ "}" }
   ```

3. **Add call_body rule** (supports UNION inside CALL)
   ```pest
   call_body = { call_query ~ (union_clause ~ call_query)* }
   ```

4. **Add call_query rule** (the query inside CALL)
   ```pest
   call_query = { 
       importing_with? ~ 
       match_clause? ~ 
       optional_match_clause* ~ 
       where_clause? ~ 
       call_clause* ~
       with_clause* ~
       return_clause 
   }
   ```

5. **Add importing_with rule**
   ```pest
   importing_with = { WITH ~ !PATH ~ return_item ~ ("," ~ return_item)* }
   ```

6. **Update main query rule** to include call_clause
   ```pest
   query = { 
       match_clause ~ 
       optional_match_clause* ~ 
       with_path_clause? ~ 
       unwind_clause* ~ 
       where_clause? ~ 
       call_clause* ~        // NEW
       let_clause* ~ 
       with_clause* ~ 
       return_clause ~ 
       group_by_clause? ~ 
       having_clause? ~ 
       order_clause? ~ 
       limit_clause? 
   }
   ```

7. **Update keyword list** to include CALL
   ```pest
   keyword = {
       MATCH | RETURN | ... | CALL | ...
   }
   ```

#### Files Modified
- `src/gql/grammar.pest`

#### Verification
- Grammar compiles without errors
- Simple CALL queries parse without errors (manual pest playground test)

---

### Phase 2: AST Types (2-3 hours)

**Goal**: Define AST types for CALL subqueries.

#### Tasks

1. **Add CallClause struct** to `ast.rs`
   ```rust
   #[derive(Debug, Clone, Serialize)]
   pub struct CallClause {
       pub body: CallBody,
   }
   ```

2. **Add CallBody enum**
   ```rust
   #[derive(Debug, Clone, Serialize)]
   pub enum CallBody {
       Single(Box<CallQuery>),
       Union { queries: Vec<CallQuery>, all: bool },
   }
   ```

3. **Add CallQuery struct**
   ```rust
   #[derive(Debug, Clone, Serialize)]
   pub struct CallQuery {
       pub importing_with: Option<ImportingWith>,
       pub match_clause: Option<MatchClause>,
       pub optional_match_clauses: Vec<OptionalMatchClause>,
       pub where_clause: Option<WhereClause>,
       pub call_clauses: Vec<CallClause>,
       pub with_clauses: Vec<WithClause>,
       pub return_clause: ReturnClause,
   }
   ```

4. **Add ImportingWith struct**
   ```rust
   #[derive(Debug, Clone, Serialize)]
   pub struct ImportingWith {
       pub items: Vec<ReturnItem>,
   }
   ```

5. **Update Query struct** to include `call_clauses: Vec<CallClause>`

6. **Export new types** in `mod.rs`

#### Files Modified
- `src/gql/ast.rs`
- `src/gql/mod.rs`

#### Verification
- `cargo check` passes
- Types can be instantiated in tests

---

### Phase 3: Parser Implementation (4-6 hours)

**Goal**: Parse CALL syntax into AST types.

#### Tasks

1. **Add build_call_clause function**
   ```rust
   fn build_call_clause(pair: Pair<Rule>) -> Result<CallClause, ParseError> {
       let mut inner = pair.into_inner();
       let body_pair = inner.next().ok_or(ParseError::MissingCallBody)?;
       let body = build_call_body(body_pair)?;
       Ok(CallClause { body })
   }
   ```

2. **Add build_call_body function**
   - Parse single query or UNION of queries
   - Handle `union_clause` between queries

3. **Add build_call_query function**
   - Parse optional importing_with first
   - Parse optional match_clause
   - Parse optional_match_clauses
   - Parse optional where_clause
   - Parse nested call_clauses (recursive)
   - Parse with_clauses
   - Parse required return_clause

4. **Add build_importing_with function**
   - Reuse existing `build_return_item` for items

5. **Update build_query function**
   - Add loop to collect `call_clause` rules
   - Store in `Query.call_clauses`

6. **Add new ParseError variants**
   ```rust
   MissingCallBody,
   MissingCallReturn,
   ```

#### Files Modified
- `src/gql/parser.rs`
- `src/gql/error.rs`

#### Verification
- Parser tests for basic CALL queries
- Parser tests for CALL with UNION
- Parser tests for nested CALL
- Snapshot tests for AST structure

---

### Phase 4: Semantic Validation (3-4 hours)

**Goal**: Validate CALL subquery semantics at compile time.

#### Tasks

1. **Add validate_call_clause function** in compiler
   ```rust
   fn validate_call_clause(
       &self,
       call_clause: &CallClause,
       outer_scope: &HashSet<String>,
   ) -> Result<HashSet<String>, CompileError> {
       // Returns the set of variables exported by this CALL
   }
   ```

2. **Validate importing WITH**
   - All imported variables must exist in outer scope
   - Imported variables become available in subquery scope

3. **Validate RETURN exports**
   - Exported variables must not shadow outer scope variables
   - Exception: if same variable is imported and returned unchanged

4. **Validate nested CALL clauses** (recursive validation)

5. **Add CompileError variants**
   ```rust
   UnboundImport { variable: String },
   ShadowingExport { variable: String },
   MissingCallReturn,
   ```

6. **Integrate validation into main compile flow**
   - Call validation after processing MATCH/WHERE
   - Track exported variables for subsequent clauses

#### Files Modified
- `src/gql/compiler.rs`
- `src/gql/error.rs`

#### Verification
- Test: importing non-existent variable fails
- Test: exporting duplicate variable fails
- Test: valid imports/exports succeed

---

### Phase 5: Compiler Execution - Correlated (6-8 hours)

**Goal**: Execute correlated CALL subqueries (with importing WITH).

#### Tasks

1. **Add execute_call_clause method**
   ```rust
   fn execute_call_clause(
       &self,
       rows: Vec<HashMap<String, Value>>,
       call_clause: &CallClause,
   ) -> Result<Vec<HashMap<String, Value>>, CompileError> {
       if call_clause.is_correlated() {
           self.execute_correlated_call(rows, call_clause)
       } else {
           self.execute_uncorrelated_call(rows, call_clause)
       }
   }
   ```

2. **Add is_correlated helper**
   ```rust
   impl CallClause {
       fn is_correlated(&self) -> bool {
           match &self.body {
               CallBody::Single(q) => q.importing_with.is_some(),
               CallBody::Union { queries, .. } => {
                   queries.iter().any(|q| q.importing_with.is_some())
               }
           }
       }
   }
   ```

3. **Implement execute_correlated_call**
   ```rust
   fn execute_correlated_call(
       &self,
       outer_rows: Vec<HashMap<String, Value>>,
       call_clause: &CallClause,
   ) -> Result<Vec<HashMap<String, Value>>, CompileError> {
       let mut result = Vec::new();
       
       for outer_row in outer_rows {
           // Create subquery scope with imported variables
           let subquery_scope = self.create_subquery_scope(&outer_row, call_clause)?;
           
           // Execute subquery
           let subquery_results = self.execute_call_body(&call_clause.body, &subquery_scope)?;
           
           // Merge each subquery result with outer row
           for sub_row in subquery_results {
               let mut combined = outer_row.clone();
               combined.extend(sub_row);
               result.push(combined);
           }
       }
       
       Ok(result)
   }
   ```

4. **Implement create_subquery_scope**
   - Extract imported variables from outer row
   - Create HashMap with only imported variables

5. **Implement execute_call_body**
   - Handle Single vs Union variants
   - For Union: execute each query, combine results (dedupe if not ALL)

6. **Implement execute_call_query**
   - Similar to main compile() but:
     - Starts with imported scope instead of empty bindings
     - MATCH is optional (can just transform imported vars)
     - Uses row-based execution path

7. **Integrate into main compile flow**
   ```rust
   // In compile(), after UNWIND processing:
   for call_clause in &query.call_clauses {
       current_rows = self.execute_call_clause(current_rows, call_clause)?;
   }
   ```

#### Files Modified
- `src/gql/compiler.rs`

#### Verification
- Test: correlated count (count friends per person)
- Test: correlated with filter (top N per group)
- Test: empty subquery results exclude outer rows
- Test: multiple CALL clauses in sequence

---

### Phase 6: Compiler Execution - Uncorrelated (2-3 hours)

**Goal**: Execute uncorrelated CALL subqueries (no importing WITH).

#### Tasks

1. **Implement execute_uncorrelated_call**
   ```rust
   fn execute_uncorrelated_call(
       &self,
       outer_rows: Vec<HashMap<String, Value>>,
       call_clause: &CallClause,
   ) -> Result<Vec<HashMap<String, Value>>, CompileError> {
       // Execute subquery once with empty scope
       let subquery_results = self.execute_call_body(&call_clause.body, &HashMap::new())?;
       
       // Cross-join with outer rows
       let mut result = Vec::new();
       for outer_row in outer_rows {
           for sub_row in &subquery_results {
               let mut combined = outer_row.clone();
               combined.extend(sub_row.clone());
               result.push(combined);
           }
       }
       
       Ok(result)
   }
   ```

2. **Optimize: cache uncorrelated results**
   - If same CALL appears multiple times, reuse results
   - (Can be deferred to future optimization pass)

#### Files Modified
- `src/gql/compiler.rs`

#### Verification
- Test: uncorrelated global count
- Test: uncorrelated cross-join with multiple outer rows

---

### Phase 7: UNION Inside CALL (2-3 hours)

**Goal**: Support UNION/UNION ALL inside CALL subqueries.

#### Tasks

1. **Update execute_call_body for Union**
   ```rust
   fn execute_call_body(
       &self,
       body: &CallBody,
       scope: &HashMap<String, Value>,
   ) -> Result<Vec<HashMap<String, Value>>, CompileError> {
       match body {
           CallBody::Single(query) => self.execute_call_query(query, scope),
           CallBody::Union { queries, all } => {
               let mut results = Vec::new();
               for query in queries {
                   let query_results = self.execute_call_query(query, scope)?;
                   results.extend(query_results);
               }
               if !all {
                   results = self.deduplicate_rows(results);
               }
               Ok(results)
           }
       }
   }
   ```

2. **Handle correlated UNION**
   - Each query in UNION may have its own importing WITH
   - All should import from same outer scope

#### Files Modified
- `src/gql/compiler.rs`

#### Verification
- Test: UNION inside CALL (deduplicated)
- Test: UNION ALL inside CALL (keeps duplicates)
- Test: correlated UNION (imports variable in each branch)

---

### Phase 8: Nested CALL (2-3 hours)

**Goal**: Support CALL subqueries inside CALL subqueries.

#### Tasks

1. **Recursive execution in execute_call_query**
   - After processing MATCH/WHERE, process nested call_clauses
   - Pass current subquery scope to nested CALL

2. **Validate nested scopes**
   - Nested CALL can import from parent CALL's scope
   - Export validation applies at each level

#### Files Modified
- `src/gql/compiler.rs`

#### Verification
- Test: two-level nested CALL
- Test: nested CALL with aggregation at each level

---

### Phase 9: Integration & Edge Cases (3-4 hours)

**Goal**: Handle edge cases and integrate with existing features.

#### Tasks

1. **CALL with OPTIONAL MATCH inside**
   - Subquery can use OPTIONAL MATCH
   - Should not exclude outer row if optional part is null

2. **CALL with aggregation + GROUP BY**
   - Aggregation inside CALL groups per execution
   - GROUP BY in outer query groups CALL results

3. **CALL interaction with LET**
   - LET can reference CALL exports
   - CALL can reference LET-bound variables if LET comes first

4. **CALL interaction with UNWIND**
   - CALL after UNWIND sees unwound rows
   - UNWIND inside CALL works per subquery execution

5. **Order of operations documentation**
   - Document: MATCH → WHERE → UNWIND → CALL → LET → WITH → RETURN

6. **Error messages**
   - Clear error for common mistakes (missing RETURN, bad imports)

#### Files Modified
- `src/gql/compiler.rs`
- `src/gql/error.rs`

#### Verification
- Test: CALL with OPTIONAL MATCH preserving outer rows
- Test: CALL before and after LET
- Test: CALL with UNWIND interaction

---

### Phase 10: Testing & Documentation (4-6 hours)

**Goal**: Comprehensive test coverage and documentation.

#### Tasks

1. **Parser snapshot tests**
   - `tests/snapshots/gql_snapshots__parse_call_basic.snap`
   - `tests/snapshots/gql_snapshots__parse_call_union.snap`
   - `tests/snapshots/gql_snapshots__parse_call_nested.snap`
   - `tests/snapshots/gql_snapshots__parse_call_correlated.snap`

2. **Integration tests** in `tests/gql.rs`
   ```rust
   #[test]
   fn test_call_correlated_count() { ... }
   
   #[test]
   fn test_call_uncorrelated_global() { ... }
   
   #[test]
   fn test_call_union_inside() { ... }
   
   #[test]
   fn test_call_nested() { ... }
   
   #[test]
   fn test_call_with_optional_match() { ... }
   
   #[test]
   fn test_call_top_n_per_group() { ... }
   ```

3. **Error case tests**
   ```rust
   #[test]
   fn test_call_unbound_import_error() { ... }
   
   #[test]
   fn test_call_shadowing_export_error() { ... }
   
   #[test]
   fn test_call_missing_return_error() { ... }
   ```

4. **Documentation**
   - Add CALL examples to `gql_api.md`
   - Rustdoc for new AST types
   - Rustdoc for compiler functions

5. **Example file**
   - Create `examples/call_subqueries.rs` with working examples

#### Files Modified/Created
- `tests/gql.rs`
- `tests/gql_snapshots.rs`
- `tests/snapshots/*.snap`
- `gql_api.md`
- `examples/call_subqueries.rs`

---

## Test Plan

### Unit Tests

| Test | Description |
|------|-------------|
| `parse_call_basic` | Simple `CALL { MATCH ... RETURN }` |
| `parse_call_with_importing_with` | `CALL { WITH p MATCH ... RETURN }` |
| `parse_call_union` | `CALL { ... UNION ... }` |
| `parse_call_union_all` | `CALL { ... UNION ALL ... }` |
| `parse_call_nested` | CALL inside CALL |
| `parse_call_multiple` | Multiple CALL clauses in one query |

### Integration Tests

| Test | Description | Expected |
|------|-------------|----------|
| `correlated_count` | Count friends per person | Per-person counts |
| `correlated_collect` | Collect items per group | Lists per group |
| `uncorrelated_global` | Global count cross-joined | Same count for all |
| `union_in_call` | UNION inside CALL | Combined results |
| `nested_call` | Two-level nesting | Correct aggregation |
| `empty_subquery` | Subquery returns nothing | Outer rows excluded |
| `call_optional_match` | OPTIONAL inside CALL | Nulls preserved |
| `top_n_per_group` | LIMIT inside correlated CALL | Top N per group |

### Error Tests

| Test | Description | Expected Error |
|------|-------------|----------------|
| `unbound_import` | Import non-existent var | `UnboundImport` |
| `shadowing_export` | Export existing var | `ShadowingExport` |
| `missing_return` | CALL without RETURN | `MissingCallReturn` |

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Grammar conflicts | Medium | High | Test grammar thoroughly in isolation |
| Performance issues | Medium | Medium | Add warnings for expensive patterns |
| Scope complexity | Medium | High | Extensive unit tests for scoping |
| Recursive execution bugs | Low | High | Careful base case handling |

---

## Rollout Plan

1. **Phase 1-3**: Grammar, AST, Parser (feature flag: `--cfg feature="call_subquery"`)
2. **Phase 4-6**: Basic execution (still behind feature flag)
3. **Phase 7-8**: Advanced features (UNION, nesting)
4. **Phase 9-10**: Integration, testing, documentation
5. **Release**: Remove feature flag, document in changelog

---

## Success Criteria

- [ ] All parser tests pass
- [ ] All integration tests pass (correlated, uncorrelated, UNION, nested)
- [ ] Error cases produce clear error messages
- [ ] No performance regression in existing queries (benchmark)
- [ ] Documentation complete with examples
- [ ] Example file runs successfully

---

## Future Work (Out of Scope)

- **CALL YIELD** for procedures: `CALL db.labels() YIELD label`
- **Scalar subqueries**: Subqueries guaranteed to return one row/value
- **Query plan caching**: Cache uncorrelated subquery results
- **Decorrelation optimization**: Transform correlated to joins where possible
