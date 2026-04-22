//! Abstract Syntax Tree types for Gremlin queries.
//!
//! This module defines the AST representation for parsed Gremlin queries,
//! including source steps, traversal steps, predicates, and literals.

/// Source span for error reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Span {
    /// Start byte offset
    pub start: usize,
    /// End byte offset
    pub end: usize,
}

impl Span {
    /// Create a new span
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }
}

/// A complete Gremlin traversal query.
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
/// Unlike GremlinTraversal, has no source step or terminal.
#[derive(Debug, Clone, PartialEq)]
pub struct AnonymousTraversal {
    /// The chain of traversal steps (may be empty for identity)
    pub steps: Vec<Step>,
    /// Source span for error reporting
    pub span: Span,
}

/// Source steps that initiate a traversal.
#[derive(Debug, Clone, PartialEq)]
pub enum SourceStep {
    /// g.V() - all vertices, g.V(id) - vertex by id, g.V(id, id, ...) - multiple
    V {
        ids: Vec<Literal>,
        /// Variable reference (e.g., g.V(alice))
        variable: Option<String>,
        span: Span,
    },
    /// g.E() - all edges, g.E(id) - edge by id
    E {
        ids: Vec<Literal>,
        /// Variable reference (e.g., g.E(edge_var))
        variable: Option<String>,
        span: Span,
    },
    /// g.addV('label') - create vertex
    AddV { label: String, span: Span },
    /// g.addE('label') - create edge
    AddE { label: String, span: Span },
    /// g.inject(values...) - inject values into traversal
    Inject { values: Vec<Literal>, span: Span },
    /// g.searchTextV('prop', query, k) - top-k vertices by full-text relevance.
    /// (spec-55c)
    SearchTextV {
        /// The indexed string property to search.
        property: String,
        /// The query, either a bare-string sugar (mapped to TextQuery::Match)
        /// or a structured TextQ DSL expression.
        query: TextQueryAst,
        /// Top-k cap.
        k: u64,
        span: Span,
    },
    /// g.searchTextE('prop', query, k) - top-k edges by full-text relevance.
    /// (spec-55c)
    SearchTextE {
        property: String,
        query: TextQueryAst,
        k: u64,
        span: Span,
    },
}

/// Terminal steps that execute the traversal and return results.
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

/// Individual traversal steps.
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
    And {
        traversals: Vec<AnonymousTraversal>,
        span: Span,
    },
    /// or(__.out(), __.in())
    Or {
        traversals: Vec<AnonymousTraversal>,
        span: Span,
    },
    /// not(__.out())
    Not {
        traversal: Box<AnonymousTraversal>,
        span: Span,
    },
    /// dedup(), dedup('label')
    Dedup {
        by_label: Option<String>,
        span: Span,
    },
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
    /// group()
    Group { span: Span },
    /// groupCount()
    GroupCount { span: Span },
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
    /// textScore() - read BM25 relevance score from the traverser sack
    /// populated by `g.searchTextV` / `g.searchTextE`. Emits one
    /// `Literal::Float`-backed value per traverser. (spec-55c)
    TextScore { span: Span },

    // ========== Branch Steps ==========
    /// choose(cond, true_trav, false_trav), choose(__.values('type'))
    Choose { args: ChooseArgs, span: Span },
    /// union(__.out(), __.in())
    Union {
        traversals: Vec<AnonymousTraversal>,
        span: Span,
    },
    /// coalesce(__.out(), __.in())
    Coalesce {
        traversals: Vec<AnonymousTraversal>,
        span: Span,
    },
    /// optional(__.out())
    Optional {
        traversal: Box<AnonymousTraversal>,
        span: Span,
    },
    /// local(__.out())
    Local {
        traversal: Box<AnonymousTraversal>,
        span: Span,
    },
    /// branch(__.values('type'))
    Branch {
        traversal: Box<AnonymousTraversal>,
        span: Span,
    },
    /// option('key', __.out()), option(none, __.identity())
    Option { args: OptionArgs, span: Span },

    // ========== Repeat Steps ==========
    /// repeat(__.out())
    Repeat {
        traversal: Box<AnonymousTraversal>,
        span: Span,
    },
    /// times(n)
    Times { count: u32, span: Span },
    /// until(__.hasLabel('target'))
    Until {
        traversal: Box<AnonymousTraversal>,
        span: Span,
    },
    /// emit(), emit(__.hasLabel('person'))
    Emit {
        traversal: Option<Box<AnonymousTraversal>>,
        span: Span,
    },

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
    SideEffect {
        traversal: Box<AnonymousTraversal>,
        span: Span,
    },
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

/// Arguments for has() step.
#[derive(Debug, Clone, PartialEq)]
pub enum HasArgs {
    /// has('key') - key existence
    Key(String),
    /// has('key', value) - key equals value
    KeyValue { key: String, value: Literal },
    /// has('key', P.gt(x)) - key matches predicate
    KeyPredicate { key: String, predicate: Predicate },
    /// has('label', 'key', value) - label + key + value
    LabelKeyValue {
        label: String,
        key: String,
        value: Literal,
    },
}

/// Arguments for where() step.
#[derive(Debug, Clone, PartialEq)]
pub enum WhereArgs {
    /// where(__.out())
    Traversal(Box<AnonymousTraversal>),
    /// where(P.eq('value'))
    Predicate(Predicate),
}

/// Arguments for is() step.
#[derive(Debug, Clone, PartialEq)]
pub enum IsArgs {
    /// is(value)
    Value(Literal),
    /// is(P.gt(x))
    Predicate(Predicate),
}

/// Arguments for valueMap() step.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ValueMapArgs {
    /// Include id and label tokens (valueMap(true))
    pub include_tokens: bool,
    /// Specific keys to include
    pub keys: Vec<String>,
}

/// Arguments for by() modulator.
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
    TraversalOrder {
        traversal: Box<AnonymousTraversal>,
        order: OrderDirection,
    },
}

/// Order direction for sorting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    /// Ascending order
    Asc,
    /// Descending order
    Desc,
    /// Random shuffle
    Shuffle,
}

/// Arguments for choose() step.
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

/// Arguments for option() step.
#[derive(Debug, Clone, PartialEq)]
pub enum OptionArgs {
    /// option('key', __.out())
    KeyValue {
        key: Literal,
        traversal: Box<AnonymousTraversal>,
    },
    /// option(none, __.identity())
    None { traversal: Box<AnonymousTraversal> },
}

/// Arguments for property() step.
#[derive(Debug, Clone, PartialEq)]
pub struct PropertyArgs {
    /// Optional cardinality (single, list, set)
    pub cardinality: Option<Cardinality>,
    /// Property key
    pub key: String,
    /// Property value
    pub value: Literal,
}

/// Property cardinality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cardinality {
    /// Single value (default)
    Single,
    /// List of values
    List,
    /// Set of unique values
    Set,
}

/// Arguments for from()/to() steps.
#[derive(Debug, Clone, PartialEq)]
pub enum FromToArgs {
    /// from('label') - select by as() label
    Label(String),
    /// from(__.select('a'))
    Traversal(Box<AnonymousTraversal>),
    /// from(vertexId)
    Id(Literal),
    /// from(variable) - reference to a script variable
    Variable(String),
}

/// Predicate for filtering.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    // Comparison predicates
    /// P.eq(value)
    Eq(Literal),
    /// P.neq(value)
    Neq(Literal),
    /// P.lt(value)
    Lt(Literal),
    /// P.lte(value)
    Lte(Literal),
    /// P.gt(value)
    Gt(Literal),
    /// P.gte(value)
    Gte(Literal),

    // Range predicates
    /// P.between(start, end) - [start, end)
    Between { start: Literal, end: Literal },
    /// P.inside(start, end) - (start, end)
    Inside { start: Literal, end: Literal },
    /// P.outside(start, end)
    Outside { start: Literal, end: Literal },

    // Collection predicates
    /// P.within(values...)
    Within(Vec<Literal>),
    /// P.without(values...)
    Without(Vec<Literal>),

    // Logical predicates
    /// P.and(p1, p2)
    And(Box<Predicate>, Box<Predicate>),
    /// P.or(p1, p2)
    Or(Box<Predicate>, Box<Predicate>),
    /// P.not(p)
    Not(Box<Predicate>),

    // Text predicates
    /// TextP.containing(s)
    Containing(String),
    /// TextP.notContaining(s)
    NotContaining(String),
    /// TextP.startingWith(s)
    StartingWith(String),
    /// TextP.notStartingWith(s)
    NotStartingWith(String),
    /// TextP.endingWith(s)
    EndingWith(String),
    /// TextP.notEndingWith(s)
    NotEndingWith(String),
    /// TextP.regex(pattern)
    Regex(String),
}

/// Literal values in Gremlin queries.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    /// String value
    String(String),
    /// Integer value
    Int(i64),
    /// Floating-point value
    Float(f64),
    /// Boolean value
    Bool(bool),
    /// Null value
    Null,
    /// List of values
    List(Vec<Literal>),
    /// Map of key-value pairs
    Map(Vec<(String, Literal)>),
}

// ============================================================
// Multi-Statement Script Types
// ============================================================

/// A complete Gremlin script containing one or more statements.
///
/// Scripts support variable assignment and reference:
/// ```gremlin
/// alice = g.addV('person').property('name', 'Alice').next()
/// bob = g.addV('person').property('name', 'Bob').next()
/// g.addE('knows').from(alice).to(bob).next()
/// g.V(alice).out('knows').values('name').toList()
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Script {
    /// The statements in this script
    pub statements: Vec<Statement>,
    /// Source span for error reporting
    pub span: Span,
}

/// A single statement in a Gremlin script.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// Variable assignment: `name = traversal`
    Assignment {
        /// The variable name being assigned to
        name: String,
        /// The traversal that produces the value
        traversal: GremlinTraversal,
        /// Source span for error reporting
        span: Span,
    },
    /// Standalone traversal execution
    Traversal {
        /// The traversal to execute
        traversal: GremlinTraversal,
        /// Source span for error reporting
        span: Span,
    },
}

/// Extended source step vertex specification for variable support.
#[derive(Debug, Clone, PartialEq)]
pub enum VertexSource {
    /// All vertices (g.V())
    All,
    /// Specific vertex IDs (g.V(1, 2, 3))
    Ids(Vec<Literal>),
    /// Variable reference (g.V(alice))
    Variable(String),
}

/// Extended edge endpoint specification for variable support.
#[derive(Debug, Clone, PartialEq)]
pub enum EdgeEndpoint {
    /// Explicit vertex ID
    VertexId(Literal),
    /// Reference to a step label (from('a'))
    StepLabel(String),
    /// Reference to a variable (from(alice))
    Variable(String),
    /// Sub-traversal that produces a vertex
    Traversal(Box<AnonymousTraversal>),
}

/// Structured full-text query for `g.searchTextV` / `g.searchTextE`.
///
/// Mirrors the `interstellar::storage::text::TextQuery` runtime enum. The
/// compiler converts this AST node into the runtime type before invoking
/// the FTS engine. Bare-string syntax in the grammar (e.g.,
/// `searchTextV('body', 'raft', 10)`) is sugared into [`TextQueryAst::Match`].
///
/// (spec-55c §3.2 — Gremlin TextQ DSL.)
#[derive(Debug, Clone, PartialEq)]
pub enum TextQueryAst {
    /// `TextQ.match('term1 term2')` — OR-of-tokens (disjunctive).
    Match(String),
    /// `TextQ.matchAll('term1 term2')` — AND-of-tokens (conjunctive).
    MatchAll(String),
    /// `TextQ.phrase('quick brown fox')` — exact phrase match.
    Phrase(String),
    /// `TextQ.prefix('foo')` — prefix expansion.
    Prefix(String),
    /// `TextQ.and(q1, q2, ...)` — conjunction of structured queries.
    And(Vec<TextQueryAst>),
    /// `TextQ.or(q1, q2, ...)` — disjunction of structured queries.
    Or(Vec<TextQueryAst>),
    /// `TextQ.not(q)` — negation of a structured query.
    Not(Box<TextQueryAst>),
}
