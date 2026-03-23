//! Error types for the typed query layer.

#[derive(Debug, thiserror::Error)]
pub enum TypedError {
    #[error("wire error: {0}")]
    Wire(#[from] pg_wire::PgWireError),

    #[error("decode error: column {column}: {message}")]
    Decode { column: usize, message: String },

    #[error("column not found: {0}")]
    ColumnNotFound(String),

    #[error("unexpected null in column {0}")]
    UnexpectedNull(usize),

    #[error("row count mismatch: expected 1, got {0}")]
    NotExactlyOne(usize),

    #[error("type mismatch: expected OID {expected}, got {actual}")]
    TypeMismatch { expected: u32, actual: u32 },
}
