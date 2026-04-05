/// A parsed LogSQL query.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub select: Vec<SelectItem>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<String>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u64>,
}

/// An item in the SELECT clause.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    /// `SELECT *`
    Star,
    /// A column reference, possibly aliased: `block_number AS bn`
    Column { name: String, alias: Option<String> },
    /// An aggregate or function call: `COUNT(*)`, `decode(data, 'uint256')`
    Function {
        name: String,
        args: Vec<Expr>,
        alias: Option<String>,
    },
}

/// An expression in WHERE, ORDER BY, or function args.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Column reference: `address`, `block_number`, `topic0`
    Column(String),
    /// Numeric literal: `100`, `18000000`
    Number(i64),
    /// String literal: `'0xdAC17...'`, `'uint256'`
    StringLit(String),
    /// `event'Transfer(address,address,uint256)'` → resolved to keccak256 B256
    EventHash(alloy_primitives::B256),
    /// `address'0xABC...'` → resolved to left-padded B256 for topic matching
    AddressPadded(alloy_primitives::B256),
    /// `latest` keyword — resolved at query execution time
    Latest,
    /// Binary operation: `a = b`, `a AND b`, `block_number >= 100`
    BinaryOp {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    /// `expr BETWEEN low AND high`
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    /// `expr IN (val1, val2, ...)`
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// `NOT expr`
    Not(Box<Expr>),
    /// Function call: `decode(data, 'uint256')`, `COUNT(*)`
    Function { name: String, args: Vec<Expr> },
    /// `*` (used inside COUNT(*))
    Star,
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    And,
    Or,
    Add,
    Sub,
}

/// ORDER BY item.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expr: Expr,
    pub desc: bool,
}
