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
    pub filters: Vec<Filter>,
    pub order: Vec<OrderClause>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub count: CountOption,
}

#[derive(Debug, Clone)]
pub enum SelectItem {
    Column(String),
    Star,
    Embed {
        alias: Option<String>,
        target: String,
        sub_request: Box<ReadRequest>,
    },
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
    pub filters: Vec<Filter>,
    pub returning: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DeleteRequest {
    pub table: QualifiedName,
    pub filters: Vec<Filter>,
    pub returning: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct FunctionCall {
    pub function: QualifiedName,
    pub params: serde_json::Map<String, serde_json::Value>,
    pub is_scalar: bool,
    pub read_request: Option<ReadRequest>,
}
