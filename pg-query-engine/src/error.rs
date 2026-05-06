use pg_schema_cache::QualifiedName;

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("invalid filter: {0}")]
    InvalidFilter(String),

    #[error("unknown operator: {0}")]
    UnknownOperator(String),

    #[error("invalid order clause: {0}")]
    InvalidOrder(String),

    #[error("invalid select syntax: {0}")]
    InvalidSelect(String),

    #[error("invalid IS value (expected null/true/false): {0}")]
    InvalidIsValue(String),
}

#[derive(Debug, thiserror::Error)]
pub enum QueryEngineError {
    #[error("parse error: {0}")]
    Parse(#[from] ParseError),

    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("function not found: {0}")]
    FunctionNotFound(String),

    #[error("no relationship found between {0} and {1}")]
    NoRelationship(QualifiedName, String),

    #[error("table {0} has no primary key (required for upsert)")]
    NoPrimaryKey(QualifiedName),

    #[error("column not found: {0}.{1}")]
    ColumnNotFound(QualifiedName, String),
}
