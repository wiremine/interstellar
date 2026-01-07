//! AST types for GQL queries.

/// Complete GQL query (minimal spike version)
#[derive(Debug, Clone)]
pub struct Query {
    pub match_clause: MatchClause,
    pub where_clause: Option<WhereClause>,
    pub return_clause: ReturnClause,
    pub order_clause: Option<OrderClause>,
    pub limit_clause: Option<LimitClause>,
}

/// MATCH clause with patterns
#[derive(Debug, Clone)]
pub struct MatchClause {
    pub patterns: Vec<Pattern>,
}

/// A pattern is a path through the graph
#[derive(Debug, Clone)]
pub struct Pattern {
    pub elements: Vec<PatternElement>,
}

#[derive(Debug, Clone)]
pub enum PatternElement {
    Node(NodePattern),
    Edge(EdgePattern),
}

/// Node pattern: (variable:Label {prop: value})
#[derive(Debug, Clone)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: Vec<(String, Literal)>,
}

/// Edge pattern: -[variable:TYPE]->
#[derive(Debug, Clone)]
pub struct EdgePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub direction: EdgeDirection,
    pub quantifier: Option<PathQuantifier>,
    pub properties: Vec<(String, Literal)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeDirection {
    Outgoing, // -->
    Incoming, // <--
    Both,     // --
}

#[derive(Debug, Clone)]
pub struct PathQuantifier {
    pub min: Option<u32>,
    pub max: Option<u32>,
}

/// WHERE clause (stub for spike)
#[derive(Debug, Clone)]
pub struct WhereClause {
    pub expression: Expression,
}

/// RETURN clause
#[derive(Debug, Clone)]
pub struct ReturnClause {
    pub items: Vec<ReturnItem>,
}

#[derive(Debug, Clone)]
pub struct ReturnItem {
    pub expression: Expression,
    pub alias: Option<String>,
}

/// ORDER BY clause (stub for spike)
#[derive(Debug, Clone)]
pub struct OrderClause {
    pub items: Vec<OrderItem>,
}

#[derive(Debug, Clone)]
pub struct OrderItem {
    pub expression: Expression,
    pub descending: bool,
}

/// LIMIT clause (stub for spike)
#[derive(Debug, Clone)]
pub struct LimitClause {
    pub limit: u64,
    pub offset: Option<u64>,
}

/// Expression types
#[derive(Debug, Clone)]
pub enum Expression {
    /// Variable reference: `n`
    Variable(String),

    /// Property access: `n.name`
    Property { variable: String, property: String },

    /// Literal value
    Literal(Literal),

    /// Binary operation (stub for spike)
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },

    /// Aggregate function (stub for spike)
    Aggregate {
        func: AggregateFunc,
        distinct: bool,
        expr: Box<Expression>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    // Comparison
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    // Logical
    And,
    Or,
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    // String
    Contains,
    StartsWith,
    EndsWith,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Collect,
}

/// Literal values
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

impl From<Literal> for crate::value::Value {
    fn from(lit: Literal) -> Self {
        match lit {
            Literal::Null => crate::value::Value::Null,
            Literal::Bool(b) => crate::value::Value::Bool(b),
            Literal::Int(n) => crate::value::Value::Int(n),
            Literal::Float(f) => crate::value::Value::Float(f),
            Literal::String(s) => crate::value::Value::String(s),
        }
    }
}
