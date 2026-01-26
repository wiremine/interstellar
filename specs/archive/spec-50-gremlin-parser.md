# Spec 50: Gremlin Text Parser

## Overview

This specification defines the implementation of a TinkerPop-compatible Gremlin text parser for Interstellar. The parser will allow users to write Gremlin queries as strings and have them parsed, compiled, and executed against the graph database.

### Goals

1. **TinkerPop Compatibility**: Match standard Gremlin syntax closely (`g.V().hasLabel('person').out('knows')`)
2. **Full Mutation Support**: Support `addV()`, `addE()`, `property()`, `drop()` for complete write capability
3. **Consistent Architecture**: Follow the proven GQL module pattern (pest parser -> AST -> compiler -> traversal)

### Non-Goals

- Groovy expression evaluation (lambdas, closures)
- Remote execution protocol (GLV - Gremlin Language Variants)
- Bytecode serialization
- Full TinkerPop server compatibility
- Interactive REPL (separate spec)

## Architecture

```
┌─────────────────┐     ┌─────────────┐     ┌───────────────┐     ┌────────────┐
│  Gremlin Text   │ ──► │   Parser    │ ──► │  Gremlin AST  │ ──► │  Compiler  │
│  Query String   │     │  (pest)     │     │               │     │            │
└─────────────────┘     └─────────────┘     └───────────────┘     └─────┬──────┘
                                                                        │
                                                                        ▼
┌─────────────────┐     ┌─────────────┐     ┌───────────────┐     ┌────────────┐
│    Results      │ ◄── │  Execution  │ ◄── │   Traversal   │ ◄── │ Traversal  │
│   Vec<Value>    │     │   Engine    │     │   Pipeline    │     │  Builder   │
└─────────────────┘     └─────────────┘     └───────────────┘     └────────────┘
```

### Module Structure

```
src/gremlin/
├── mod.rs              # Public API, module docs, re-exports
├── grammar.pest        # PEG grammar definition (~600-800 lines)
├── parser.rs           # pest-based parser, AST construction
├── ast.rs              # Abstract Syntax Tree type definitions
├── compiler.rs         # AST → Traversal compilation
└── error.rs            # Error types with span information

tests/gremlin/
├── mod.rs              # Test module
├── parser_tests.rs     # Parser unit tests
├── compiler_tests.rs   # Compiler integration tests
└── snapshots/          # Snapshot tests for parser output
```

## Grammar Specification

The grammar uses pest (PEG - Parsing Expression Grammar), consistent with the existing GQL module.

### Lexical Elements

```pest
// Whitespace and comments (silently consumed)
WHITESPACE = _{ " " | "\t" | "\r" | "\n" }
COMMENT = _{ "//" ~ (!"\n" ~ ANY)* | "/*" ~ (!"*/" ~ ANY)* ~ "*/" }

// String literals (both quote styles for TinkerPop compatibility)
string = ${ single_quoted | double_quoted }
single_quoted = ${ "'" ~ single_inner ~ "'" }
single_inner = @{ (!"'" ~ ("\\\\" | "\\'" | ANY))* }
double_quoted = ${ "\"" ~ double_inner ~ "\"" }
double_inner = @{ (!"\"" ~ ("\\\\" | "\\\"" | ANY))* }

// Numeric literals
integer = @{ "-"? ~ ASCII_DIGIT+ }
float = @{ "-"? ~ ASCII_DIGIT+ ~ "." ~ ASCII_DIGIT+ ~ (^"e" ~ "-"? ~ ASCII_DIGIT+)? }

// Boolean and null
boolean = { "true" | "false" }
null = { "null" }

// Identifiers
identifier = @{ ASCII_ALPHA ~ (ASCII_ALPHANUMERIC | "_")* }
```

### Top-Level Structure

```pest
// Entry point: g.V().step1().step2()...
traversal = { SOI ~ graph_source ~ step* ~ terminal_step? ~ EOI }

// Graph source: g
graph_source = { "g" ~ "." ~ source_step }

// Source steps
source_step = {
    v_step           // V(), V(id), V(id, id, ...)
    | e_step         // E(), E(id), E(id, id, ...)
    | add_v_step     // addV(label)
    | add_e_step     // addE(label)
    | inject_step    // inject(value, value, ...)
}

v_step = { "V" ~ "(" ~ id_list? ~ ")" }
e_step = { "E" ~ "(" ~ id_list? ~ ")" }
add_v_step = { "addV" ~ "(" ~ string ~ ")" }
add_e_step = { "addE" ~ "(" ~ string ~ ")" }
inject_step = { "inject" ~ "(" ~ value_list? ~ ")" }

id_list = { value ~ ("," ~ value)* }
value_list = { value ~ ("," ~ value)* }
```

### Step Definitions

```pest
// Main step dispatcher
step = { "." ~ (
    // Navigation (vertex to vertex)
    out_step | in_step | both_step |
    // Navigation (vertex to edge)
    out_e_step | in_e_step | both_e_step |
    // Navigation (edge to vertex)
    out_v_step | in_v_step | both_v_step | other_v_step |
    // Filter
    has_step | has_label_step | has_id_step | has_not_step | has_key_step | has_value_step |
    where_step | is_step | and_step | or_step | not_step |
    dedup_step | limit_step | skip_step | range_step | tail_step |
    coin_step | sample_step | simple_path_step | cyclic_path_step |
    // Transform
    values_step | properties_step | value_map_step | element_map_step | property_map_step |
    id_step | label_step | key_step | value_step |
    path_step | select_step | project_step | by_step |
    unfold_step | fold_step |
    count_step | sum_step | max_step | min_step | mean_step |
    order_step | math_step | constant_step | identity_step | index_step | loops_step |
    // Branch
    choose_step | union_step | coalesce_step | optional_step | local_step | branch_step | option_step |
    // Repeat
    repeat_step | times_step | until_step | emit_step |
    // Side effect
    as_step | aggregate_step | store_step | cap_step | side_effect_step | profile_step |
    // Mutation
    add_v_inline_step | add_e_inline_step | property_step | from_step | to_step | drop_step
)}

// ============================================================
// Navigation Steps
// ============================================================

out_step = { "out" ~ "(" ~ label_list? ~ ")" }
in_step = { "in" ~ "(" ~ label_list? ~ ")" }
both_step = { "both" ~ "(" ~ label_list? ~ ")" }
out_e_step = { "outE" ~ "(" ~ label_list? ~ ")" }
in_e_step = { "inE" ~ "(" ~ label_list? ~ ")" }
both_e_step = { "bothE" ~ "(" ~ label_list? ~ ")" }
out_v_step = { "outV" ~ "(" ~ ")" }
in_v_step = { "inV" ~ "(" ~ ")" }
both_v_step = { "bothV" ~ "(" ~ ")" }
other_v_step = { "otherV" ~ "(" ~ ")" }

label_list = { string ~ ("," ~ string)* }

// ============================================================
// Filter Steps
// ============================================================

// has() - multiple overloads
has_step = {
    "has" ~ "(" ~ (
        has_key_predicate         // has(key, predicate)
        | has_key_value           // has(key, value)
        | has_label_key_value     // has(label, key, value)
        | has_key_only            // has(key)
    ) ~ ")"
}
has_key_only = { string }
has_key_value = { string ~ "," ~ value }
has_key_predicate = { string ~ "," ~ predicate }
has_label_key_value = { string ~ "," ~ string ~ "," ~ value }

has_label_step = { "hasLabel" ~ "(" ~ string ~ ("," ~ string)* ~ ")" }
has_id_step = { "hasId" ~ "(" ~ value ~ ("," ~ value)* ~ ")" }
has_not_step = { "hasNot" ~ "(" ~ string ~ ")" }
has_key_step = { "hasKey" ~ "(" ~ string ~ ("," ~ string)* ~ ")" }
has_value_step = { "hasValue" ~ "(" ~ value ~ ("," ~ value)* ~ ")" }

// where() - traversal or predicate
where_step = {
    "where" ~ "(" ~ (
        where_predicate           // where(P.eq('a'))
        | where_traversal         // where(__.out())
    ) ~ ")"
}
where_predicate = { predicate }
where_traversal = { anonymous_traversal }

// is() - value or predicate
is_step = { "is" ~ "(" ~ (predicate | value) ~ ")" }

// Boolean combinators
and_step = { "and" ~ "(" ~ anonymous_traversal ~ ("," ~ anonymous_traversal)* ~ ")" }
or_step = { "or" ~ "(" ~ anonymous_traversal ~ ("," ~ anonymous_traversal)* ~ ")" }
not_step = { "not" ~ "(" ~ anonymous_traversal ~ ")" }

// Limiting steps
dedup_step = { "dedup" ~ "(" ~ string? ~ ")" }
limit_step = { "limit" ~ "(" ~ integer ~ ")" }
skip_step = { "skip" ~ "(" ~ integer ~ ")" }
range_step = { "range" ~ "(" ~ integer ~ "," ~ integer ~ ")" }
tail_step = { "tail" ~ "(" ~ integer? ~ ")" }
coin_step = { "coin" ~ "(" ~ float ~ ")" }
sample_step = { "sample" ~ "(" ~ integer ~ ")" }
simple_path_step = { "simplePath" ~ "(" ~ ")" }
cyclic_path_step = { "cyclicPath" ~ "(" ~ ")" }

// ============================================================
// Transform Steps
// ============================================================

values_step = { "values" ~ "(" ~ (string ~ ("," ~ string)*)? ~ ")" }
properties_step = { "properties" ~ "(" ~ (string ~ ("," ~ string)*)? ~ ")" }
value_map_step = { "valueMap" ~ "(" ~ value_map_args? ~ ")" }
value_map_args = { boolean ~ ("," ~ string)* | string ~ ("," ~ string)* }
element_map_step = { "elementMap" ~ "(" ~ (string ~ ("," ~ string)*)? ~ ")" }
property_map_step = { "propertyMap" ~ "(" ~ (string ~ ("," ~ string)*)? ~ ")" }
id_step = { "id" ~ "(" ~ ")" }
label_step = { "label" ~ "(" ~ ")" }
key_step = { "key" ~ "(" ~ ")" }
value_step = { "value" ~ "(" ~ ")" }
path_step = { "path" ~ "(" ~ ")" }

select_step = { "select" ~ "(" ~ string ~ ("," ~ string)* ~ ")" }
project_step = { "project" ~ "(" ~ string ~ ("," ~ string)* ~ ")" }
by_step = { "by" ~ "(" ~ by_arg? ~ ")" }
by_arg = { 
    order_direction               // by(asc) or by(desc)
    | by_key_direction            // by('name', asc)
    | anonymous_traversal         // by(__.values('name'))
    | string                      // by('name')
}
order_direction = { "asc" | "desc" | "Order.asc" | "Order.desc" | "Order.shuffle" }
by_key_direction = { string ~ "," ~ order_direction }

unfold_step = { "unfold" ~ "(" ~ ")" }
fold_step = { "fold" ~ "(" ~ ")" }
count_step = { "count" ~ "(" ~ ")" }
sum_step = { "sum" ~ "(" ~ ")" }
max_step = { "max" ~ "(" ~ ")" }
min_step = { "min" ~ "(" ~ ")" }
mean_step = { "mean" ~ "(" ~ ")" }
order_step = { "order" ~ "(" ~ ")" }
math_step = { "math" ~ "(" ~ string ~ ")" }
constant_step = { "constant" ~ "(" ~ value ~ ")" }
identity_step = { "identity" ~ "(" ~ ")" }
index_step = { "index" ~ "(" ~ ")" }
loops_step = { "loops" ~ "(" ~ ")" }

// ============================================================
// Branch Steps
// ============================================================

// choose() - multiple forms
choose_step = {
    "choose" ~ "(" ~ (
        choose_if_then_else       // choose(cond, true_trav, false_trav)
        | choose_by_traversal     // choose(__.values('type'))
        | choose_predicate        // choose(P.gt(25))
    ) ~ ")"
}
choose_if_then_else = { anonymous_traversal ~ "," ~ anonymous_traversal ~ "," ~ anonymous_traversal }
choose_by_traversal = { anonymous_traversal }
choose_predicate = { predicate }

union_step = { "union" ~ "(" ~ anonymous_traversal ~ ("," ~ anonymous_traversal)* ~ ")" }
coalesce_step = { "coalesce" ~ "(" ~ anonymous_traversal ~ ("," ~ anonymous_traversal)* ~ ")" }
optional_step = { "optional" ~ "(" ~ anonymous_traversal ~ ")" }
local_step = { "local" ~ "(" ~ anonymous_traversal ~ ")" }
branch_step = { "branch" ~ "(" ~ anonymous_traversal ~ ")" }
option_step = { "option" ~ "(" ~ option_args ~ ")" }
option_args = { 
    option_none                   // option(none, __.identity())
    | option_key_value            // option('a', __.out())
}
option_none = { "none" ~ "," ~ anonymous_traversal }
option_key_value = { value ~ "," ~ anonymous_traversal }

// ============================================================
// Repeat Steps
// ============================================================

repeat_step = { "repeat" ~ "(" ~ anonymous_traversal ~ ")" }
times_step = { "times" ~ "(" ~ integer ~ ")" }
until_step = { "until" ~ "(" ~ anonymous_traversal ~ ")" }
emit_step = { "emit" ~ "(" ~ anonymous_traversal? ~ ")" }

// ============================================================
// Side Effect Steps
// ============================================================

as_step = { "as" ~ "(" ~ string ~ ")" }
aggregate_step = { "aggregate" ~ "(" ~ string ~ ")" }
store_step = { "store" ~ "(" ~ string ~ ")" }
cap_step = { "cap" ~ "(" ~ string ~ ("," ~ string)* ~ ")" }
side_effect_step = { "sideEffect" ~ "(" ~ anonymous_traversal ~ ")" }
profile_step = { "profile" ~ "(" ~ string? ~ ")" }

// ============================================================
// Mutation Steps
// ============================================================

add_v_inline_step = { "addV" ~ "(" ~ string ~ ")" }
add_e_inline_step = { "addE" ~ "(" ~ string ~ ")" }
property_step = { "property" ~ "(" ~ property_args ~ ")" }
property_args = {
    property_cardinality          // property(Cardinality.single, 'key', value)
    | property_key_value          // property('key', value)
}
property_cardinality = { cardinality ~ "," ~ string ~ "," ~ value }
property_key_value = { string ~ "," ~ value }
cardinality = { "Cardinality.single" | "Cardinality.list" | "Cardinality.set" | "single" | "list" | "set" }

from_step = { "from" ~ "(" ~ from_to_arg ~ ")" }
to_step = { "to" ~ "(" ~ from_to_arg ~ ")" }
from_to_arg = { anonymous_traversal | string | value }
drop_step = { "drop" ~ "(" ~ ")" }

// ============================================================
// Terminal Steps
// ============================================================

terminal_step = { "." ~ (
    next_step | to_list_step | to_set_step | iterate_step | has_next_step
)}

next_step = { "next" ~ "(" ~ integer? ~ ")" }
to_list_step = { "toList" ~ "(" ~ ")" }
to_set_step = { "toSet" ~ "(" ~ ")" }
iterate_step = { "iterate" ~ "(" ~ ")" }
has_next_step = { "hasNext" ~ "(" ~ ")" }

// ============================================================
// Predicates
// ============================================================

predicate = { p_predicate | text_p_predicate }

// P.eq(), P.neq(), P.lt(), etc.
p_predicate = { "P" ~ "." ~ p_method }
p_method = {
    p_eq | p_neq | p_lt | p_lte | p_gt | p_gte |
    p_between | p_inside | p_outside |
    p_within | p_without |
    p_and | p_or | p_not
}

p_eq = { "eq" ~ "(" ~ value ~ ")" }
p_neq = { "neq" ~ "(" ~ value ~ ")" }
p_lt = { "lt" ~ "(" ~ value ~ ")" }
p_lte = { "lte" ~ "(" ~ value ~ ")" }
p_gt = { "gt" ~ "(" ~ value ~ ")" }
p_gte = { "gte" ~ "(" ~ value ~ ")" }
p_between = { "between" ~ "(" ~ value ~ "," ~ value ~ ")" }
p_inside = { "inside" ~ "(" ~ value ~ "," ~ value ~ ")" }
p_outside = { "outside" ~ "(" ~ value ~ "," ~ value ~ ")" }
p_within = { "within" ~ "(" ~ value_list ~ ")" }
p_without = { "without" ~ "(" ~ value_list ~ ")" }
p_and = { "and" ~ "(" ~ predicate ~ "," ~ predicate ~ ")" }
p_or = { "or" ~ "(" ~ predicate ~ "," ~ predicate ~ ")" }
p_not = { "not" ~ "(" ~ predicate ~ ")" }

// TextP.containing(), TextP.startingWith(), etc.
text_p_predicate = { "TextP" ~ "." ~ text_p_method }
text_p_method = {
    text_containing | text_not_containing |
    text_starting_with | text_not_starting_with |
    text_ending_with | text_not_ending_with |
    text_regex
}

text_containing = { "containing" ~ "(" ~ string ~ ")" }
text_not_containing = { "notContaining" ~ "(" ~ string ~ ")" }
text_starting_with = { "startingWith" ~ "(" ~ string ~ ")" }
text_not_starting_with = { "notStartingWith" ~ "(" ~ string ~ ")" }
text_ending_with = { "endingWith" ~ "(" ~ string ~ ")" }
text_not_ending_with = { "notEndingWith" ~ "(" ~ string ~ ")" }
text_regex = { "regex" ~ "(" ~ string ~ ")" }

// ============================================================
// Anonymous Traversal
// ============================================================

// __.out(), __.in(), __.identity(), etc.
// Note: step* allows zero steps (e.g., __ used as identity)
anonymous_traversal = { "__" ~ step* }

// ============================================================
// Values
// ============================================================

value = { float | integer | string | boolean | null | list_value | map_value }
list_value = { "[" ~ (value ~ ("," ~ value)*)? ~ "]" }
// Maps use [:] for empty, and key:value syntax (distinct from list)
map_value = { "[" ~ ":" ~ "]" | "[" ~ map_entry ~ ("," ~ map_entry)* ~ "]" }
map_entry = { (string | identifier) ~ ":" ~ value }
```

## AST Specification

### Core Types

```rust
/// A complete Gremlin traversal query
#[derive(Debug, Clone, PartialEq)]
pub struct GremlinTraversal {
    /// The source step (g.V(), g.E(), etc.)
    pub source: SourceStep,
    /// The chain of traversal steps
    pub steps: Vec<Step>,
    /// Optional terminal step (toList, next, etc.)
    pub terminal: Option<TerminalStep>,
    /// Source span for error reporting
    pub span: Span,
}

/// An anonymous traversal (__.out(), __.values(), etc.)
/// Unlike GremlinTraversal, has no source step or terminal
#[derive(Debug, Clone, PartialEq)]
pub struct AnonymousTraversal {
    /// The chain of traversal steps (may be empty for identity)
    pub steps: Vec<Step>,
    /// Source span for error reporting
    pub span: Span,
}

/// Source steps that initiate a traversal
#[derive(Debug, Clone, PartialEq)]
pub enum SourceStep {
    /// g.V() - all vertices, g.V(id) - vertex by id, g.V(id, id, ...) - multiple
    V { ids: Vec<Literal>, span: Span },
    /// g.E() - all edges, g.E(id) - edge by id
    E { ids: Vec<Literal>, span: Span },
    /// g.addV('label') - create vertex
    AddV { label: String, span: Span },
    /// g.addE('label') - create edge
    AddE { label: String, span: Span },
    /// g.inject(values...) - inject values into traversal
    Inject { values: Vec<Literal>, span: Span },
}

/// Terminal steps that execute the traversal and return results
#[derive(Debug, Clone, PartialEq)]
pub enum TerminalStep {
    /// .next() or .next(n)
    Next { count: Option<u64>, span: Span },
    /// .toList()
    ToList { span: Span },
    /// .toSet()
    ToSet { span: Span },
    /// .iterate() - execute without collecting
    Iterate { span: Span },
    /// .hasNext()
    HasNext { span: Span },
}
```

### Step Enumeration

```rust
/// Individual traversal steps
#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    // ========== Navigation Steps ==========
    
    /// out(), out('label'), out('label1', 'label2')
    Out { labels: Vec<String>, span: Span },
    /// in(), in('label')
    In { labels: Vec<String>, span: Span },
    /// both(), both('label')
    Both { labels: Vec<String>, span: Span },
    /// outE(), outE('label')
    OutE { labels: Vec<String>, span: Span },
    /// inE(), inE('label')
    InE { labels: Vec<String>, span: Span },
    /// bothE(), bothE('label')
    BothE { labels: Vec<String>, span: Span },
    /// outV()
    OutV { span: Span },
    /// inV()
    InV { span: Span },
    /// bothV()
    BothV { span: Span },
    /// otherV()
    OtherV { span: Span },

    // ========== Filter Steps ==========
    
    /// has('key'), has('key', value), has('key', P.gt(x)), has('label', 'key', value)
    Has { args: HasArgs, span: Span },
    /// hasLabel('label'), hasLabel('l1', 'l2')
    HasLabel { labels: Vec<String>, span: Span },
    /// hasId(id), hasId(id1, id2)
    HasId { ids: Vec<Literal>, span: Span },
    /// hasNot('key')
    HasNot { key: String, span: Span },
    /// hasKey('key'), hasKey('k1', 'k2')
    HasKey { keys: Vec<String>, span: Span },
    /// hasValue(value), hasValue(v1, v2)
    HasValue { values: Vec<Literal>, span: Span },
    /// where(__.out()), where(P.gt(25))
    Where { args: WhereArgs, span: Span },
    /// is(value), is(P.gt(25))
    Is { args: IsArgs, span: Span },
    /// and(__.out(), __.in())
    And { traversals: Vec<AnonymousTraversal>, span: Span },
    /// or(__.out(), __.in())
    Or { traversals: Vec<AnonymousTraversal>, span: Span },
    /// not(__.out())
    Not { traversal: Box<AnonymousTraversal>, span: Span },
    /// dedup(), dedup('label')
    Dedup { by_label: Option<String>, span: Span },
    /// limit(n)
    Limit { count: u64, span: Span },
    /// skip(n)
    Skip { count: u64, span: Span },
    /// range(start, end)
    Range { start: u64, end: u64, span: Span },
    /// tail(), tail(n)
    Tail { count: Option<u64>, span: Span },
    /// coin(probability)
    Coin { probability: f64, span: Span },
    /// sample(n)
    Sample { count: u64, span: Span },
    /// simplePath()
    SimplePath { span: Span },
    /// cyclicPath()
    CyclicPath { span: Span },

    // ========== Transform Steps ==========
    
    /// values('key'), values('k1', 'k2')
    Values { keys: Vec<String>, span: Span },
    /// properties(), properties('key')
    Properties { keys: Vec<String>, span: Span },
    /// valueMap(), valueMap(true), valueMap('k1', 'k2')
    ValueMap { args: ValueMapArgs, span: Span },
    /// elementMap(), elementMap('k1', 'k2')
    ElementMap { keys: Vec<String>, span: Span },
    /// propertyMap(), propertyMap('k1')
    PropertyMap { keys: Vec<String>, span: Span },
    /// id()
    Id { span: Span },
    /// label()
    Label { span: Span },
    /// key()
    Key { span: Span },
    /// value()
    Value { span: Span },
    /// path()
    Path { span: Span },
    /// select('label'), select('l1', 'l2')
    Select { labels: Vec<String>, span: Span },
    /// project('k1', 'k2')
    Project { keys: Vec<String>, span: Span },
    /// by('key'), by(__.values('x')), by(asc)
    By { args: ByArgs, span: Span },
    /// unfold()
    Unfold { span: Span },
    /// fold()
    Fold { span: Span },
    /// count()
    Count { span: Span },
    /// sum()
    Sum { span: Span },
    /// max()
    Max { span: Span },
    /// min()
    Min { span: Span },
    /// mean()
    Mean { span: Span },
    /// order()
    Order { span: Span },
    /// math('a + b')
    Math { expression: String, span: Span },
    /// constant(value)
    Constant { value: Literal, span: Span },
    /// identity()
    Identity { span: Span },
    /// index()
    Index { span: Span },
    /// loops()
    Loops { span: Span },

    // ========== Branch Steps ==========
    
    /// choose(cond, true_trav, false_trav), choose(__.values('type'))
    Choose { args: ChooseArgs, span: Span },
    /// union(__.out(), __.in())
    Union { traversals: Vec<AnonymousTraversal>, span: Span },
    /// coalesce(__.out(), __.in())
    Coalesce { traversals: Vec<AnonymousTraversal>, span: Span },
    /// optional(__.out())
    Optional { traversal: Box<AnonymousTraversal>, span: Span },
    /// local(__.out())
    Local { traversal: Box<AnonymousTraversal>, span: Span },
    /// branch(__.values('type'))
    Branch { traversal: Box<AnonymousTraversal>, span: Span },
    /// option('key', __.out()), option(none, __.identity())
    Option { args: OptionArgs, span: Span },

    // ========== Repeat Steps ==========
    
    /// repeat(__.out())
    Repeat { traversal: Box<AnonymousTraversal>, span: Span },
    /// times(n)
    Times { count: u32, span: Span },
    /// until(__.hasLabel('target'))
    Until { traversal: Box<AnonymousTraversal>, span: Span },
    /// emit(), emit(__.hasLabel('person'))
    Emit { traversal: Option<Box<AnonymousTraversal>>, span: Span },

    // ========== Side Effect Steps ==========
    
    /// as('label')
    As { label: String, span: Span },
    /// aggregate('x')
    Aggregate { key: String, span: Span },
    /// store('x')
    Store { key: String, span: Span },
    /// cap('x'), cap('x', 'y')
    Cap { keys: Vec<String>, span: Span },
    /// sideEffect(__.out())
    SideEffect { traversal: Box<AnonymousTraversal>, span: Span },
    /// profile(), profile('metrics')
    Profile { key: Option<String>, span: Span },

    // ========== Mutation Steps ==========
    
    /// addV('label') - inline (not source)
    AddV { label: String, span: Span },
    /// addE('label') - inline (not source)
    AddE { label: String, span: Span },
    /// property('key', value), property(Cardinality.single, 'key', value)
    Property { args: PropertyArgs, span: Span },
    /// from('label'), from(__.select('a'))
    From { args: FromToArgs, span: Span },
    /// to('label'), to(__.select('b'))
    To { args: FromToArgs, span: Span },
    /// drop()
    Drop { span: Span },
}
```

### Supporting Types

```rust
/// Arguments for has() step
#[derive(Debug, Clone, PartialEq)]
pub enum HasArgs {
    /// has('key') - key existence
    Key(String),
    /// has('key', value) - key equals value
    KeyValue { key: String, value: Literal },
    /// has('key', P.gt(x)) - key matches predicate
    KeyPredicate { key: String, predicate: Predicate },
    /// has('label', 'key', value) - label + key + value
    LabelKeyValue { label: String, key: String, value: Literal },
}

/// Arguments for where() step
#[derive(Debug, Clone, PartialEq)]
pub enum WhereArgs {
    /// where(__.out())
    Traversal(Box<AnonymousTraversal>),
    /// where(P.eq('value'))
    Predicate(Predicate),
}

/// Arguments for is() step
#[derive(Debug, Clone, PartialEq)]
pub enum IsArgs {
    /// is(value)
    Value(Literal),
    /// is(P.gt(x))
    Predicate(Predicate),
}

/// Arguments for valueMap() step
#[derive(Debug, Clone, PartialEq)]
pub struct ValueMapArgs {
    /// Include id and label tokens (valueMap(true))
    pub include_tokens: bool,
    /// Specific keys to include
    pub keys: Vec<String>,
}

/// Arguments for by() modulator
#[derive(Debug, Clone, PartialEq)]
pub enum ByArgs {
    /// by() - identity
    Identity,
    /// by('key')
    Key(String),
    /// by(__.values('name'))
    Traversal(Box<AnonymousTraversal>),
    /// by(asc), by(desc)
    Order(OrderDirection),
    /// by('key', asc)
    KeyOrder { key: String, order: OrderDirection },
    /// by(__.values('x'), asc)
    TraversalOrder { traversal: Box<AnonymousTraversal>, order: OrderDirection },
}

/// Order direction for sorting
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
    Shuffle,
}

/// Arguments for choose() step
#[derive(Debug, Clone, PartialEq)]
pub enum ChooseArgs {
    /// choose(cond, true_trav, false_trav)
    IfThenElse {
        condition: Box<AnonymousTraversal>,
        if_true: Box<AnonymousTraversal>,
        if_false: Box<AnonymousTraversal>,
    },
    /// choose(__.values('type')) - for use with option()
    ByTraversal(Box<AnonymousTraversal>),
    /// choose(P.gt(25))
    ByPredicate(Predicate),
}

/// Arguments for option() step
#[derive(Debug, Clone, PartialEq)]
pub enum OptionArgs {
    /// option('key', __.out())
    KeyValue { key: Literal, traversal: Box<AnonymousTraversal> },
    /// option(none, __.identity())
    None { traversal: Box<AnonymousTraversal> },
}

/// Arguments for property() step
#[derive(Debug, Clone, PartialEq)]
pub struct PropertyArgs {
    /// Optional cardinality (single, list, set)
    pub cardinality: Option<Cardinality>,
    /// Property key
    pub key: String,
    /// Property value
    pub value: Literal,
}

/// Property cardinality
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Cardinality {
    Single,
    List,
    Set,
}

/// Arguments for from()/to() steps
#[derive(Debug, Clone, PartialEq)]
pub enum FromToArgs {
    /// from('label') - select by as() label
    Label(String),
    /// from(__.select('a'))
    Traversal(Box<AnonymousTraversal>),
    /// from(vertexId)
    Id(Literal),
}

/// Predicate for filtering
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    // Comparison predicates
    Eq(Literal),
    Neq(Literal),
    Lt(Literal),
    Lte(Literal),
    Gt(Literal),
    Gte(Literal),
    
    // Range predicates
    Between { start: Literal, end: Literal },
    Inside { start: Literal, end: Literal },
    Outside { start: Literal, end: Literal },
    
    // Collection predicates
    Within(Vec<Literal>),
    Without(Vec<Literal>),
    
    // Logical predicates
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
    
    // Text predicates
    Containing(String),
    NotContaining(String),
    StartingWith(String),
    NotStartingWith(String),
    EndingWith(String),
    NotEndingWith(String),
    Regex(String),
}

/// Literal values
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Null,
    List(Vec<Literal>),
    Map(Vec<(String, Literal)>),
}

/// Source span for error reporting
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}
```

## Compiler Specification

The compiler transforms the Gremlin AST into the existing Interstellar traversal pipeline.

### Compilation Strategy

```rust
use crate::traversal::{BoundTraversal, GraphTraversalSource, Traversal, __};
use crate::gremlin::ast::*;
use crate::gremlin::error::CompileError;

/// Compile a Gremlin AST into a bound traversal
pub fn compile<'g, S: SnapshotLike>(
    ast: &GremlinTraversal,
    g: &GraphTraversalSource<'g, S>,
) -> Result<CompiledTraversal<'g>, CompileError> {
    // 1. Compile source step
    let mut traversal = compile_source(&ast.source, g)?;
    
    // 2. Compile each step in sequence
    for step in &ast.steps {
        traversal = compile_step(step, traversal)?;
    }
    
    // 3. Return with optional terminal info
    Ok(CompiledTraversal {
        traversal,
        terminal: ast.terminal.clone(),
    })
}

/// Result of compilation - traversal with terminal step info
pub struct CompiledTraversal<'g> {
    pub traversal: BoundTraversal<'g, (), Value>,
    pub terminal: Option<TerminalStep>,
}

impl<'g> CompiledTraversal<'g> {
    /// Execute the traversal and return results
    pub fn execute(self) -> Result<ExecutionResult, CompileError> {
        match self.terminal {
            None | Some(TerminalStep::ToList { .. }) => {
                Ok(ExecutionResult::List(self.traversal.to_list()))
            }
            Some(TerminalStep::Next { count: None, .. }) => {
                Ok(ExecutionResult::Single(self.traversal.next()))
            }
            Some(TerminalStep::Next { count: Some(n), .. }) => {
                Ok(ExecutionResult::List(self.traversal.take(n as usize)))
            }
            Some(TerminalStep::ToSet { .. }) => {
                Ok(ExecutionResult::Set(self.traversal.to_set()))
            }
            Some(TerminalStep::Iterate { .. }) => {
                self.traversal.iterate();
                Ok(ExecutionResult::Unit)
            }
            Some(TerminalStep::HasNext { .. }) => {
                Ok(ExecutionResult::Bool(self.traversal.has_next()))
            }
        }
    }
}
```

### Step Compilation Mapping

| Gremlin Step | Interstellar Method |
|--------------|---------------------|
| `V()` | `g.v()` |
| `V(id)` | `g.v_by_id(id)` |
| `V(id, id, ...)` | `g.v_ids(&[ids])` |
| `E()` | `g.e()` |
| `addV(label)` | `g.add_v(label)` |
| `addE(label)` | `g.add_e(label)` |
| `out()` | `.out()` |
| `out('label')` | `.out_labels(&["label"])` |
| `in()` | `.in_()` |
| `hasLabel('label')` | `.has_label("label")` |
| `hasLabel('l1', 'l2')` | `.has_label_any(&["l1", "l2"])` |
| `has('key')` | `.has("key")` |
| `has('key', value)` | `.has_value("key", value)` |
| `has('key', P.gt(x))` | `.has_where("key", p::gt(x))` |
| `values('key')` | `.values("key")` |
| `where(__.out())` | `.where_(__.out())` |
| `is(P.gt(x))` | `.is_(p::gt(x))` |
| `and(t1, t2)` | `.and_(&[t1, t2])` |
| `or(t1, t2)` | `.or_(&[t1, t2])` |
| `not(t)` | `.not(t)` |
| `dedup()` | `.dedup()` |
| `limit(n)` | `.limit(n)` |
| `skip(n)` | `.skip(n)` |
| `order().by('key')` | `.order().by_key_asc("key").build()` |
| `union(t1, t2)` | `.union(&[t1, t2])` |
| `coalesce(t1, t2)` | `.coalesce(&[t1, t2])` |
| `choose(c, t, f)` | `.choose(c, t, f)` |
| `repeat(t).times(n)` | `.repeat(t).times(n)` |
| `as('label')` | `.as_("label")` |
| `select('l1', 'l2')` | `.select(&["l1", "l2"])` |
| `property('k', v)` | `.property("k", v)` |
| `from('label')` | `.from_label("label")` |
| `to('label')` | `.to_label("label")` |
| `drop()` | `.drop()` |

### Predicate Compilation

```rust
fn compile_predicate(pred: &Predicate) -> crate::traversal::Predicate {
    match pred {
        Predicate::Eq(v) => p::eq(literal_to_value(v)),
        Predicate::Neq(v) => p::neq(literal_to_value(v)),
        Predicate::Lt(v) => p::lt(literal_to_value(v)),
        Predicate::Lte(v) => p::lte(literal_to_value(v)),
        Predicate::Gt(v) => p::gt(literal_to_value(v)),
        Predicate::Gte(v) => p::gte(literal_to_value(v)),
        Predicate::Between { start, end } => {
            p::between(literal_to_value(start), literal_to_value(end))
        }
        Predicate::Inside { start, end } => {
            p::inside(literal_to_value(start), literal_to_value(end))
        }
        Predicate::Outside { start, end } => {
            p::outside(literal_to_value(start), literal_to_value(end))
        }
        Predicate::Within(values) => {
            p::within(&values.iter().map(literal_to_value).collect::<Vec<_>>())
        }
        Predicate::Without(values) => {
            p::without(&values.iter().map(literal_to_value).collect::<Vec<_>>())
        }
        Predicate::And(p1, p2) => {
            p::and(compile_predicate(p1), compile_predicate(p2))
        }
        Predicate::Or(p1, p2) => {
            p::or(compile_predicate(p1), compile_predicate(p2))
        }
        Predicate::Not(p) => p::not(compile_predicate(p)),
        Predicate::Containing(s) => p::containing(s),
        Predicate::StartingWith(s) => p::starting_with(s),
        Predicate::EndingWith(s) => p::ending_with(s),
        Predicate::NotContaining(s) => p::not_containing(s),
        Predicate::NotStartingWith(s) => p::not_starting_with(s),
        Predicate::NotEndingWith(s) => p::not_ending_with(s),
        Predicate::Regex(s) => p::regex(s),
    }
}
```

## Error Handling

### Error Types

```rust
use thiserror::Error;

/// Top-level Gremlin error
#[derive(Debug, Error)]
pub enum GremlinError {
    #[error("Parse error: {0}")]
    Parse(#[from] ParseError),
    
    #[error("Compile error: {0}")]
    Compile(#[from] CompileError),
    
    #[error("Execution error: {0}")]
    Execution(String),
}

/// Parse errors with source location
#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Syntax error at position {span:?}: {message}")]
    SyntaxAt { span: Span, message: String },
    
    #[error("Syntax error: {0}")]
    Syntax(String),
    
    #[error("Empty query")]
    Empty,
    
    #[error("Invalid literal '{value}' at {span:?}: {reason}")]
    InvalidLiteral { value: String, span: Span, reason: &'static str },
    
    #[error("Unexpected token at {span:?}: found '{found}', expected {expected}")]
    UnexpectedToken { span: Span, found: String, expected: String },
    
    #[error("Missing source step (query must start with g.V(), g.E(), etc.)")]
    MissingSource,
    
    #[error("Invalid step '{step}' at {span:?}: {reason}")]
    InvalidStep { step: String, span: Span, reason: String },
}

/// Compile errors
#[derive(Debug, Error)]
pub enum CompileError {
    #[error("Unsupported step: {step}")]
    UnsupportedStep { step: String },
    
    #[error("Invalid arguments for {step}: {message}")]
    InvalidArguments { step: String, message: String },
    
    #[error("Type mismatch: {message}")]
    TypeMismatch { message: String },
    
    #[error("Undefined label: '{label}'")]
    UndefinedLabel { label: String },
    
    #[error("Invalid predicate: {message}")]
    InvalidPredicate { message: String },
    
    #[error("Step '{step}' requires preceding '{required}'")]
    MissingPrecedingStep { step: String, required: String },
}
```

### Error Formatting

Errors should include context for interactive use:

```
Parse error at position 15:
g.V().hasLabel('person').outX('knows')
                         ^^^^
Unexpected token: found 'outX', expected step name (out, in, has, etc.)
```

## Public API

### Module Exports

```rust
// src/gremlin/mod.rs

//! # Gremlin Text Parser
//!
//! TinkerPop-compatible Gremlin query parsing for Interstellar.
//!
//! ## Quick Start
//!
//! ```rust
//! use interstellar::prelude::*;
//!
//! let graph = Graph::new();
//! // ... populate graph ...
//!
//! // Execute a Gremlin query
//! let results = graph.gremlin("g.V().hasLabel('person').values('name')")?;
//! ```

mod ast;
mod compiler;
mod error;
mod parser;

pub use ast::*;
pub use compiler::{compile, CompiledTraversal, ExecutionResult};
pub use error::{CompileError, GremlinError, ParseError};
pub use parser::parse;
```

### Graph Integration

```rust
// Add to src/storage/graph.rs or src/lib.rs

impl Graph {
    /// Execute a Gremlin query string and return results
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    ///
    /// let graph = Graph::new();
    /// // ... populate ...
    ///
    /// let names = graph.gremlin("g.V().hasLabel('person').values('name')")?;
    /// ```
    pub fn gremlin(&self, query: &str) -> Result<Vec<Value>, GremlinError> {
        let ast = gremlin::parse(query)?;
        let snapshot = self.snapshot();
        let g = snapshot.gremlin();
        let compiled = gremlin::compile(&ast, &g)?;
        
        match compiled.execute()? {
            ExecutionResult::List(values) => Ok(values),
            ExecutionResult::Set(values) => Ok(values.into_iter().collect()),
            ExecutionResult::Single(Some(value)) => Ok(vec![value]),
            ExecutionResult::Single(None) => Ok(vec![]),
            ExecutionResult::Bool(b) => Ok(vec![Value::Bool(b)]),
            ExecutionResult::Unit => Ok(vec![]),
        }
    }
}

impl<'g> GraphSnapshot<'g> {
    /// Execute a Gremlin query string on this snapshot
    pub fn gremlin(&self, query: &str) -> Result<Vec<Value>, GremlinError> {
        let ast = gremlin::parse(query)?;
        let g = self.traversal();
        let compiled = gremlin::compile(&ast, &g)?;
        
        match compiled.execute()? {
            ExecutionResult::List(values) => Ok(values),
            ExecutionResult::Set(values) => Ok(values.into_iter().collect()),
            ExecutionResult::Single(Some(value)) => Ok(vec![value]),
            ExecutionResult::Single(None) => Ok(vec![]),
            ExecutionResult::Bool(b) => Ok(vec![Value::Bool(b)]),
            ExecutionResult::Unit => Ok(vec![]),
        }
    }
}
```

## Testing Strategy

### Parser Tests

```rust
#[cfg(test)]
mod parser_tests {
    use super::*;

    // ========== Source Step Tests ==========

    #[test]
    fn test_v_all() {
        let ast = parse("g.V()").unwrap();
        assert!(matches!(ast.source, SourceStep::V { ids, .. } if ids.is_empty()));
        assert!(ast.steps.is_empty());
    }

    #[test]
    fn test_v_single_id() {
        let ast = parse("g.V(1)").unwrap();
        assert!(matches!(&ast.source, SourceStep::V { ids, .. } if ids.len() == 1));
    }

    #[test]
    fn test_v_multiple_ids() {
        let ast = parse("g.V(1, 2, 3)").unwrap();
        assert!(matches!(&ast.source, SourceStep::V { ids, .. } if ids.len() == 3));
    }

    #[test]
    fn test_e_all() {
        let ast = parse("g.E()").unwrap();
        assert!(matches!(ast.source, SourceStep::E { ids, .. } if ids.is_empty()));
    }

    #[test]
    fn test_add_v() {
        let ast = parse("g.addV('person')").unwrap();
        assert!(matches!(&ast.source, SourceStep::AddV { label, .. } if label == "person"));
    }

    #[test]
    fn test_add_e() {
        let ast = parse("g.addE('knows')").unwrap();
        assert!(matches!(&ast.source, SourceStep::AddE { label, .. } if label == "knows"));
    }

    #[test]
    fn test_inject() {
        let ast = parse("g.inject(1, 2, 3)").unwrap();
        assert!(matches!(&ast.source, SourceStep::Inject { values, .. } if values.len() == 3));
    }

    // ========== Navigation Tests ==========

    #[test]
    fn test_out_no_label() {
        let ast = parse("g.V().out()").unwrap();
        assert_eq!(ast.steps.len(), 1);
        assert!(matches!(&ast.steps[0], Step::Out { labels, .. } if labels.is_empty()));
    }

    #[test]
    fn test_out_with_label() {
        let ast = parse("g.V().out('knows')").unwrap();
        assert!(matches!(&ast.steps[0], Step::Out { labels, .. } if labels == &["knows"]));
    }

    #[test]
    fn test_out_multiple_labels() {
        let ast = parse("g.V().out('knows', 'created')").unwrap();
        assert!(matches!(&ast.steps[0], Step::Out { labels, .. } if labels.len() == 2));
    }

    #[test]
    fn test_in_step() {
        let ast = parse("g.V().in('knows')").unwrap();
        assert!(matches!(&ast.steps[0], Step::In { labels, .. } if labels == &["knows"]));
    }

    #[test]
    fn test_both() {
        let ast = parse("g.V().both()").unwrap();
        assert!(matches!(&ast.steps[0], Step::Both { .. }));
    }

    #[test]
    fn test_edge_navigation() {
        let ast = parse("g.V().outE('knows').inV()").unwrap();
        assert_eq!(ast.steps.len(), 2);
        assert!(matches!(&ast.steps[0], Step::OutE { .. }));
        assert!(matches!(&ast.steps[1], Step::InV { .. }));
    }

    // ========== Filter Tests ==========

    #[test]
    fn test_has_key_only() {
        let ast = parse("g.V().has('name')").unwrap();
        assert!(matches!(&ast.steps[0], Step::Has { args: HasArgs::Key(k), .. } if k == "name"));
    }

    #[test]
    fn test_has_key_value() {
        let ast = parse("g.V().has('name', 'alice')").unwrap();
        assert!(matches!(&ast.steps[0], 
            Step::Has { args: HasArgs::KeyValue { key, value: Literal::String(v) }, .. } 
            if key == "name" && v == "alice"
        ));
    }

    #[test]
    fn test_has_key_predicate() {
        let ast = parse("g.V().has('age', P.gt(25))").unwrap();
        assert!(matches!(&ast.steps[0], 
            Step::Has { args: HasArgs::KeyPredicate { key, predicate: Predicate::Gt(_) }, .. } 
            if key == "age"
        ));
    }

    #[test]
    fn test_has_label() {
        let ast = parse("g.V().hasLabel('person')").unwrap();
        assert!(matches!(&ast.steps[0], 
            Step::HasLabel { labels, .. } if labels == &["person"]
        ));
    }

    #[test]
    fn test_has_label_multiple() {
        let ast = parse("g.V().hasLabel('person', 'software')").unwrap();
        assert!(matches!(&ast.steps[0], 
            Step::HasLabel { labels, .. } if labels.len() == 2
        ));
    }

    #[test]
    fn test_where_traversal() {
        let ast = parse("g.V().where(__.out('knows'))").unwrap();
        assert!(matches!(&ast.steps[0], Step::Where { args: WhereArgs::Traversal(_), .. }));
    }

    #[test]
    fn test_where_predicate() {
        let ast = parse("g.V().where(P.gt(25))").unwrap();
        assert!(matches!(&ast.steps[0], Step::Where { args: WhereArgs::Predicate(_), .. }));
    }

    #[test]
    fn test_and_step() {
        let ast = parse("g.V().and(__.out('knows'), __.has('age', P.gt(25)))").unwrap();
        assert!(matches!(&ast.steps[0], Step::And { traversals, .. } if traversals.len() == 2));
    }

    #[test]
    fn test_or_step() {
        let ast = parse("g.V().or(__.hasLabel('person'), __.hasLabel('software'))").unwrap();
        assert!(matches!(&ast.steps[0], Step::Or { traversals, .. } if traversals.len() == 2));
    }

    #[test]
    fn test_not_step() {
        let ast = parse("g.V().not(__.out('knows'))").unwrap();
        assert!(matches!(&ast.steps[0], Step::Not { .. }));
    }

    #[test]
    fn test_limit() {
        let ast = parse("g.V().limit(10)").unwrap();
        assert!(matches!(&ast.steps[0], Step::Limit { count: 10, .. }));
    }

    #[test]
    fn test_range() {
        let ast = parse("g.V().range(5, 10)").unwrap();
        assert!(matches!(&ast.steps[0], Step::Range { start: 5, end: 10, .. }));
    }

    // ========== Transform Tests ==========

    #[test]
    fn test_values_single() {
        let ast = parse("g.V().values('name')").unwrap();
        assert!(matches!(&ast.steps[0], Step::Values { keys, .. } if keys == &["name"]));
    }

    #[test]
    fn test_values_multiple() {
        let ast = parse("g.V().values('name', 'age')").unwrap();
        assert!(matches!(&ast.steps[0], Step::Values { keys, .. } if keys.len() == 2));
    }

    #[test]
    fn test_value_map() {
        let ast = parse("g.V().valueMap()").unwrap();
        assert!(matches!(&ast.steps[0], Step::ValueMap { .. }));
    }

    #[test]
    fn test_value_map_with_tokens() {
        let ast = parse("g.V().valueMap(true)").unwrap();
        assert!(matches!(&ast.steps[0], 
            Step::ValueMap { args: ValueMapArgs { include_tokens: true, .. }, .. }
        ));
    }

    #[test]
    fn test_select_single() {
        let ast = parse("g.V().as('a').out().select('a')").unwrap();
        assert!(matches!(&ast.steps[2], Step::Select { labels, .. } if labels == &["a"]));
    }

    #[test]
    fn test_select_multiple() {
        let ast = parse("g.V().as('a').out().as('b').select('a', 'b')").unwrap();
        assert!(matches!(&ast.steps[4], Step::Select { labels, .. } if labels.len() == 2));
    }

    #[test]
    fn test_order_by() {
        let ast = parse("g.V().order().by('name')").unwrap();
        assert!(matches!(&ast.steps[0], Step::Order { .. }));
        assert!(matches!(&ast.steps[1], Step::By { args: ByArgs::Key(k), .. } if k == "name"));
    }

    #[test]
    fn test_order_by_desc() {
        let ast = parse("g.V().order().by('age', desc)").unwrap();
        assert!(matches!(&ast.steps[1], 
            Step::By { args: ByArgs::KeyOrder { order: OrderDirection::Desc, .. }, .. }
        ));
    }

    // ========== Branch Tests ==========

    #[test]
    fn test_union() {
        let ast = parse("g.V().union(__.out('knows'), __.out('created'))").unwrap();
        assert!(matches!(&ast.steps[0], Step::Union { traversals, .. } if traversals.len() == 2));
    }

    #[test]
    fn test_coalesce() {
        let ast = parse("g.V().coalesce(__.values('nickname'), __.values('name'))").unwrap();
        assert!(matches!(&ast.steps[0], Step::Coalesce { traversals, .. } if traversals.len() == 2));
    }

    #[test]
    fn test_choose_if_then_else() {
        let ast = parse("g.V().choose(__.hasLabel('person'), __.out('knows'), __.out('created'))").unwrap();
        assert!(matches!(&ast.steps[0], Step::Choose { args: ChooseArgs::IfThenElse { .. }, .. }));
    }

    #[test]
    fn test_optional() {
        let ast = parse("g.V().optional(__.out('knows'))").unwrap();
        assert!(matches!(&ast.steps[0], Step::Optional { .. }));
    }

    // ========== Repeat Tests ==========

    #[test]
    fn test_repeat_times() {
        let ast = parse("g.V().repeat(__.out()).times(3)").unwrap();
        assert_eq!(ast.steps.len(), 2);
        assert!(matches!(&ast.steps[0], Step::Repeat { .. }));
        assert!(matches!(&ast.steps[1], Step::Times { count: 3, .. }));
    }

    #[test]
    fn test_repeat_until() {
        let ast = parse("g.V().repeat(__.out()).until(__.hasLabel('company'))").unwrap();
        assert!(matches!(&ast.steps[1], Step::Until { .. }));
    }

    #[test]
    fn test_repeat_emit() {
        let ast = parse("g.V().repeat(__.out()).times(5).emit()").unwrap();
        assert!(matches!(&ast.steps[2], Step::Emit { traversal: None, .. }));
    }

    // ========== Side Effect Tests ==========

    #[test]
    fn test_as_step() {
        let ast = parse("g.V().as('a')").unwrap();
        assert!(matches!(&ast.steps[0], Step::As { label, .. } if label == "a"));
    }

    #[test]
    fn test_aggregate() {
        let ast = parse("g.V().aggregate('x')").unwrap();
        assert!(matches!(&ast.steps[0], Step::Aggregate { key, .. } if key == "x"));
    }

    #[test]
    fn test_store() {
        let ast = parse("g.V().store('x')").unwrap();
        assert!(matches!(&ast.steps[0], Step::Store { key, .. } if key == "x"));
    }

    #[test]
    fn test_cap() {
        let ast = parse("g.V().store('x').cap('x')").unwrap();
        assert!(matches!(&ast.steps[1], Step::Cap { keys, .. } if keys == &["x"]));
    }

    // ========== Mutation Tests ==========

    #[test]
    fn test_property() {
        let ast = parse("g.V().property('name', 'alice')").unwrap();
        assert!(matches!(&ast.steps[0], 
            Step::Property { args: PropertyArgs { key, value: Literal::String(v), .. }, .. }
            if key == "name" && v == "alice"
        ));
    }

    #[test]
    fn test_property_with_cardinality() {
        let ast = parse("g.V().property(single, 'name', 'alice')").unwrap();
        assert!(matches!(&ast.steps[0], 
            Step::Property { args: PropertyArgs { cardinality: Some(Cardinality::Single), .. }, .. }
        ));
    }

    #[test]
    fn test_from_label() {
        let ast = parse("g.addE('knows').from('a').to('b')").unwrap();
        assert!(matches!(&ast.steps[0], Step::From { args: FromToArgs::Label(l), .. } if l == "a"));
        assert!(matches!(&ast.steps[1], Step::To { args: FromToArgs::Label(l), .. } if l == "b"));
    }

    #[test]
    fn test_drop() {
        let ast = parse("g.V().hasLabel('temp').drop()").unwrap();
        assert!(matches!(&ast.steps[1], Step::Drop { .. }));
    }

    // ========== Terminal Tests ==========

    #[test]
    fn test_to_list() {
        let ast = parse("g.V().toList()").unwrap();
        assert!(matches!(ast.terminal, Some(TerminalStep::ToList { .. })));
    }

    #[test]
    fn test_next() {
        let ast = parse("g.V().next()").unwrap();
        assert!(matches!(ast.terminal, Some(TerminalStep::Next { count: None, .. })));
    }

    #[test]
    fn test_next_with_count() {
        let ast = parse("g.V().next(5)").unwrap();
        assert!(matches!(ast.terminal, Some(TerminalStep::Next { count: Some(5), .. })));
    }

    // ========== Predicate Tests ==========

    #[test]
    fn test_predicate_eq() {
        let ast = parse("g.V().has('age', P.eq(30))").unwrap();
        assert!(matches!(&ast.steps[0], 
            Step::Has { args: HasArgs::KeyPredicate { predicate: Predicate::Eq(_), .. }, .. }
        ));
    }

    #[test]
    fn test_predicate_between() {
        let ast = parse("g.V().has('age', P.between(20, 30))").unwrap();
        assert!(matches!(&ast.steps[0], 
            Step::Has { args: HasArgs::KeyPredicate { predicate: Predicate::Between { .. }, .. }, .. }
        ));
    }

    #[test]
    fn test_predicate_within() {
        let ast = parse("g.V().has('status', P.within('active', 'pending'))").unwrap();
        assert!(matches!(&ast.steps[0], 
            Step::Has { args: HasArgs::KeyPredicate { predicate: Predicate::Within(_), .. }, .. }
        ));
    }

    #[test]
    fn test_predicate_and() {
        let ast = parse("g.V().has('age', P.and(P.gte(18), P.lt(65)))").unwrap();
        assert!(matches!(&ast.steps[0], 
            Step::Has { args: HasArgs::KeyPredicate { predicate: Predicate::And(_, _), .. }, .. }
        ));
    }

    #[test]
    fn test_text_predicate_containing() {
        let ast = parse("g.V().has('name', TextP.containing('alice'))").unwrap();
        assert!(matches!(&ast.steps[0], 
            Step::Has { args: HasArgs::KeyPredicate { predicate: Predicate::Containing(_), .. }, .. }
        ));
    }

    #[test]
    fn test_text_predicate_regex() {
        let ast = parse("g.V().has('email', TextP.regex('.*@example\\.com'))").unwrap();
        assert!(matches!(&ast.steps[0], 
            Step::Has { args: HasArgs::KeyPredicate { predicate: Predicate::Regex(_), .. }, .. }
        ));
    }

    // ========== String Escape Tests ==========

    #[test]
    fn test_escaped_single_quote() {
        let ast = parse("g.V().has('name', 'O\\'Brien')").unwrap();
        assert!(matches!(&ast.steps[0], 
            Step::Has { args: HasArgs::KeyValue { value: Literal::String(v), .. }, .. }
            if v == "O'Brien"
        ));
    }

    #[test]
    fn test_escaped_double_quote() {
        let ast = parse("g.V().has('quote', \"He said \\\"hello\\\"\")").unwrap();
        assert!(matches!(&ast.steps[0], 
            Step::Has { args: HasArgs::KeyValue { value: Literal::String(v), .. }, .. }
            if v == "He said \"hello\""
        ));
    }

    #[test]
    fn test_escaped_backslash() {
        let ast = parse("g.V().has('path', 'C:\\\\Users')").unwrap();
        assert!(matches!(&ast.steps[0], 
            Step::Has { args: HasArgs::KeyValue { value: Literal::String(v), .. }, .. }
            if v == "C:\\Users"
        ));
    }

    // ========== Map/List Tests ==========

    #[test]
    fn test_empty_list() {
        let ast = parse("g.inject([])").unwrap();
        assert!(matches!(&ast.source, SourceStep::Inject { values, .. } 
            if values.len() == 1 && matches!(&values[0], Literal::List(l) if l.is_empty())
        ));
    }

    #[test]
    fn test_empty_map() {
        let ast = parse("g.inject([:])").unwrap();
        assert!(matches!(&ast.source, SourceStep::Inject { values, .. } 
            if values.len() == 1 && matches!(&values[0], Literal::Map(m) if m.is_empty())
        ));
    }

    #[test]
    fn test_map_literal() {
        let ast = parse("g.inject([name: 'alice', age: 30])").unwrap();
        assert!(matches!(&ast.source, SourceStep::Inject { values, .. } 
            if values.len() == 1 && matches!(&values[0], Literal::Map(m) if m.len() == 2)
        ));
    }

    // ========== Anonymous Traversal Tests ==========

    #[test]
    fn test_anonymous_identity() {
        // __ with no steps acts as identity
        let ast = parse("g.V().where(__)").unwrap();
        if let Step::Where { args: WhereArgs::Traversal(trav), .. } = &ast.steps[0] {
            assert!(trav.steps.is_empty());
        } else {
            panic!("Expected where with traversal");
        }
    }

    // ========== Complex Query Tests ==========

    #[test]
    fn test_chain_of_steps() {
        let ast = parse("g.V().hasLabel('person').out('knows').values('name')").unwrap();
        assert_eq!(ast.steps.len(), 3);
    }

    #[test]
    fn test_nested_anonymous() {
        let ast = parse("g.V().where(__.out('knows').has('name', 'alice'))").unwrap();
        if let Step::Where { args: WhereArgs::Traversal(trav), .. } = &ast.steps[0] {
            assert_eq!(trav.steps.len(), 2);
        } else {
            panic!("Expected where with traversal");
        }
    }

    #[test]
    fn test_deeply_nested() {
        let ast = parse(
            "g.V().union(__.out('knows').where(__.out('created')), __.in('knows'))"
        ).unwrap();
        assert!(matches!(&ast.steps[0], Step::Union { traversals, .. } if traversals.len() == 2));
    }

    // ========== Error Tests ==========

    #[test]
    fn test_error_missing_source() {
        let result = parse("V().hasLabel('person')");
        assert!(result.is_err());
    }

    #[test]
    fn test_error_invalid_step() {
        let result = parse("g.V().invalidStep()");
        assert!(result.is_err());
    }

    #[test]
    fn test_error_unclosed_paren() {
        let result = parse("g.V().has('name'");
        assert!(result.is_err());
    }

    #[test]
    fn test_error_unclosed_string() {
        let result = parse("g.V().has('name");
        assert!(result.is_err());
    }
}
```

### Compiler Integration Tests

```rust
#[cfg(test)]
mod compiler_tests {
    use super::*;
    use crate::prelude::*;

    fn create_test_graph() -> Graph {
        let graph = Graph::new();
        
        let alice = graph.add_vertex("person", hashmap! {
            "name" => "alice",
            "age" => 30i64
        });
        let bob = graph.add_vertex("person", hashmap! {
            "name" => "bob",
            "age" => 25i64
        });
        let charlie = graph.add_vertex("person", hashmap! {
            "name" => "charlie",
            "age" => 35i64
        });
        
        graph.add_edge("knows", alice, bob, hashmap! {});
        graph.add_edge("knows", alice, charlie, hashmap! {});
        graph.add_edge("knows", bob, charlie, hashmap! {});
        
        graph
    }

    #[test]
    fn test_v_all() {
        let graph = create_test_graph();
        let results = graph.gremlin("g.V()").unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_has_label() {
        let graph = create_test_graph();
        let results = graph.gremlin("g.V().hasLabel('person')").unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_has_key_value() {
        let graph = create_test_graph();
        let results = graph.gremlin("g.V().has('name', 'alice')").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_out_navigation() {
        let graph = create_test_graph();
        let results = graph.gremlin(
            "g.V().has('name', 'alice').out('knows').values('name')"
        ).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_predicate_gt() {
        let graph = create_test_graph();
        let results = graph.gremlin("g.V().has('age', P.gt(25)).values('name')").unwrap();
        assert_eq!(results.len(), 2); // alice (30) and charlie (35)
    }

    #[test]
    fn test_predicate_between() {
        let graph = create_test_graph();
        let results = graph.gremlin("g.V().has('age', P.between(26, 35)).values('name')").unwrap();
        assert_eq!(results.len(), 1); // only alice (30)
    }

    #[test]
    fn test_count() {
        let graph = create_test_graph();
        let results = graph.gremlin("g.V().count()").unwrap();
        assert_eq!(results, vec![Value::Int(3)]);
    }

    #[test]
    fn test_limit() {
        let graph = create_test_graph();
        let results = graph.gremlin("g.V().limit(2)").unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_order_by() {
        let graph = create_test_graph();
        let results = graph.gremlin(
            "g.V().order().by('age', desc).values('name')"
        ).unwrap();
        assert_eq!(results[0], Value::String("charlie".to_string())); // age 35
    }

    #[test]
    fn test_union() {
        let graph = create_test_graph();
        let results = graph.gremlin(
            "g.V().has('name', 'alice').union(__.values('name'), __.values('age'))"
        ).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_coalesce() {
        let graph = create_test_graph();
        let results = graph.gremlin(
            "g.V().has('name', 'alice').coalesce(__.values('nickname'), __.values('name'))"
        ).unwrap();
        assert_eq!(results, vec![Value::String("alice".to_string())]);
    }

    #[test]
    fn test_where_traversal() {
        let graph = create_test_graph();
        let results = graph.gremlin(
            "g.V().where(__.out('knows').has('name', 'charlie')).values('name')"
        ).unwrap();
        assert_eq!(results.len(), 2); // alice and bob both know charlie
    }

    #[test]
    fn test_repeat_times() {
        let graph = create_test_graph();
        let results = graph.gremlin(
            "g.V().has('name', 'alice').repeat(__.out('knows')).times(2).dedup().values('name')"
        ).unwrap();
        // alice -> bob, charlie -> bob -> charlie, charlie -> (none new)
        // After dedup: bob, charlie
        assert!(results.len() >= 1);
    }

    #[test]
    fn test_as_select() {
        let graph = create_test_graph();
        let results = graph.gremlin(
            "g.V().has('name', 'alice').as('a').out('knows').as('b').select('a', 'b')"
        ).unwrap();
        assert_eq!(results.len(), 2); // alice -> bob, alice -> charlie
    }

    // ========== Mutation Tests ==========

    #[test]
    fn test_add_vertex() {
        let graph = create_test_graph();
        let results = graph.gremlin(
            "g.addV('person').property('name', 'dave').property('age', 40)"
        ).unwrap();
        assert_eq!(results.len(), 1);
        
        let count = graph.gremlin("g.V().hasLabel('person').count()").unwrap();
        assert_eq!(count, vec![Value::Int(4)]);
    }

    #[test]
    fn test_drop_vertex() {
        let graph = create_test_graph();
        graph.gremlin("g.V().has('name', 'charlie').drop()").unwrap();
        
        let count = graph.gremlin("g.V().hasLabel('person').count()").unwrap();
        assert_eq!(count, vec![Value::Int(2)]);
    }
}
```

### Snapshot Tests

Use `insta` crate for AST snapshot testing:

```rust
#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use insta::assert_debug_snapshot;

    #[test]
    fn test_snapshot_simple_query() {
        let ast = parse("g.V().hasLabel('person').out('knows').values('name')").unwrap();
        assert_debug_snapshot!(ast);
    }

    #[test]
    fn test_snapshot_complex_query() {
        let ast = parse(
            "g.V().has('age', P.gt(25)).where(__.out('knows').count().is(P.gte(2))).order().by('name').limit(10)"
        ).unwrap();
        assert_debug_snapshot!(ast);
    }

    #[test]
    fn test_snapshot_mutation() {
        let ast = parse(
            "g.addV('person').property('name', 'alice').property('age', 30)"
        ).unwrap();
        assert_debug_snapshot!(ast);
    }

    #[test]
    fn test_snapshot_repeat() {
        let ast = parse(
            "g.V().repeat(__.out('knows')).until(__.hasLabel('target')).emit().path()"
        ).unwrap();
        assert_debug_snapshot!(ast);
    }
}
```

## Implementation Phases

### Phase 1: Core Grammar & AST (4-6 hours)

**Files:**
- `src/gremlin/grammar.pest`
- `src/gremlin/ast.rs`
- `src/gremlin/error.rs`

**Deliverables:**
- Complete PEG grammar for Gremlin syntax
- AST type definitions with Span tracking
- Error types with source location support

**Tests:**
- Grammar can parse all example queries
- AST correctly represents query structure

### Phase 2: Parser Implementation (4-6 hours)

**Files:**
- `src/gremlin/parser.rs`

**Deliverables:**
- pest-based parser that builds AST from grammar
- Helper functions for each AST node type
- Comprehensive error messages

**Tests:**
- Parser unit tests for all step types
- Error case tests
- Snapshot tests for complex queries

### Phase 3: Compiler (6-8 hours)

**Files:**
- `src/gremlin/compiler.rs`

**Deliverables:**
- AST to Traversal compiler
- Predicate compilation
- Anonymous traversal handling
- Terminal step execution

**Tests:**
- Compiler integration tests
- End-to-end query execution tests
- Mutation tests

### Phase 4: Public API & Integration (2-3 hours)

**Files:**
- `src/gremlin/mod.rs`
- `src/lib.rs` (modification)
- `src/storage/graph.rs` (modification)

**Deliverables:**
- Clean public API
- `Graph::gremlin()` convenience method
- Documentation with examples

**Tests:**
- API usage tests
- Documentation tests

### Phase 5: Testing & Polish (4-6 hours)

**Deliverables:**
- Comprehensive test coverage (aim for 90%+)
- Performance benchmarks
- Documentation review
- Edge case handling

## Dependencies

### New Dependencies

```toml
[dependencies]
# Already in use for GQL
pest = "2.7"
pest_derive = "2.7"
thiserror = "1.0"

[features]
default = []
gremlin = ["gql"]
```

## TinkerPop Compatibility Notes

### Supported Syntax

| Feature | Status | Notes |
|---------|--------|-------|
| `g.V()`, `g.E()` | Supported | Full support |
| Navigation steps | Supported | out, in, both, outE, inE, etc. |
| Filter steps | Supported | has, hasLabel, where, is, and, or, not |
| Transform steps | Supported | values, valueMap, select, project, order |
| Branch steps | Supported | choose, union, coalesce, optional |
| Repeat steps | Supported | repeat, until, times, emit |
| Side effect steps | Supported | as, aggregate, store, cap |
| Mutation steps | Supported | addV, addE, property, drop |
| P predicates | Supported | eq, neq, lt, gt, between, within, etc. |
| TextP predicates | Supported | containing, startingWith, regex |
| Anonymous traversals | Supported | `__.out()`, `__.values()`, `__` (identity) |

### Unsupported Features

| Feature | Reason |
|---------|--------|
| Lambda expressions | Security, not portable |
| `match()` step | Complex pattern matching not yet implemented |
| `sack()` / `withSack()` | Requires stateful traverser |
| `subgraph()` | Complex graph construction |
| `io()` | Use native import/export |
| Remote execution | Out of scope for text parser |

### Syntax Differences

| TinkerPop | Interstellar | Reason |
|-----------|--------------|--------|
| `next()` | `next()` | Same |
| `toList()` | `toList()` | Same |
| `id` (as string) | Integer literal | IDs are u64 internally |
| Groovy closures | Not supported | Security |
| `[:]` | `[:]` | Empty map syntax |
| `['a', 'b']` | `['a', 'b']` | List syntax |
| `[a: 1, b: 2]` | `[a: 1, b: 2]` | Map syntax (key:value) |

## Success Criteria

1. **Parser Correctness**: All valid TinkerPop queries parse correctly
2. **Error Quality**: Parse errors include position and helpful messages
3. **Execution Correctness**: Compiled queries produce same results as Rust API
4. **Performance**: Parsing < 1ms for typical queries, compilation < 5ms
5. **Test Coverage**: > 90% coverage on parser and compiler
6. **Documentation**: Complete rustdoc with examples
