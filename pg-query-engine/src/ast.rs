use pg_schema_cache::QualifiedName;

#[derive(Debug, Clone)]
pub enum ApiRequest {
    Read(ReadRequest),
    Insert(InsertRequest),
    Update(UpdateRequest),
    Delete(DeleteRequest),
    CallFunction(FunctionCall),
}

#[derive(Debug, Clone)]
pub struct ReadRequest {
    pub table: QualifiedName,
    pub select: Vec<SelectItem>,
    pub filters: FilterNode,
    pub order: Vec<OrderClause>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub count: CountOption,
}

#[derive(Debug, Clone)]
pub enum SelectItem {
    Column(String),
    /// Column with an explicit type cast: `select=name::text`
    Cast { column: String, pg_type: String },
    Star,
    Embed {
        alias: Option<String>,
        target: String,
        sub_request: Box<ReadRequest>,
    },
}

// ---------------------------------------------------------------------------
// Filter tree (supports and/or grouping)
// ---------------------------------------------------------------------------

/// Recursive filter tree supporting `and`/`or` grouping and `not` negation.
///
/// PostgREST syntax: `?or=(age.gt.18,name.eq.Alice)`,
/// `?and=(status.eq.active,not.deleted.eq.true)`
#[derive(Debug, Clone)]
pub enum FilterNode {
    And(Vec<FilterNode>),
    Or(Vec<FilterNode>),
    Not(Box<FilterNode>),
    Condition(Filter),
}

impl FilterNode {
    pub fn from_filters(filters: Vec<Filter>) -> Self {
        Self::And(filters.into_iter().map(Self::Condition).collect())
    }

    pub fn empty() -> Self {
        Self::And(Vec::new())
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, Self::And(v) | Self::Or(v) if v.is_empty())
    }
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub column: String,
    pub operator: FilterOp,
    pub value: FilterValue,
    pub negated: bool,
}

#[derive(Debug, Clone)]
pub enum FilterOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    Ilike,
    In,
    Is,
    Contains,
    ContainedIn,
    Overlaps,
    Fts(Option<String>),
    Plfts(Option<String>),
    Phfts(Option<String>),
    Wfts(Option<String>),
}

#[derive(Debug, Clone)]
pub enum FilterValue {
    Value(String),
    List(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct OrderClause {
    pub column: String,
    pub direction: OrderDirection,
    pub nulls: Option<NullsOrder>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullsOrder {
    First,
    Last,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CountOption {
    None,
    Exact,
    Planned,
    Estimated,
}

#[derive(Debug, Clone)]
pub struct InsertRequest {
    pub table: QualifiedName,
    pub rows: Vec<serde_json::Map<String, serde_json::Value>>,
    pub on_conflict: Option<ConflictAction>,
    pub returning: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictAction {
    MergeDuplicates,
    IgnoreDuplicates,
}

#[derive(Debug, Clone)]
pub struct UpdateRequest {
    pub table: QualifiedName,
    pub set: serde_json::Map<String, serde_json::Value>,
    pub filters: FilterNode,
    pub returning: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DeleteRequest {
    pub table: QualifiedName,
    pub filters: FilterNode,
    pub returning: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct FunctionCall {
    pub function: QualifiedName,
    pub params: serde_json::Map<String, serde_json::Value>,
    pub is_scalar: bool,
    pub read_request: Option<ReadRequest>,
}
