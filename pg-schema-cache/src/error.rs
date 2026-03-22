#[derive(Debug, thiserror::Error)]
pub enum SchemaCacheError {
    #[error("database error: {0}")]
    Database(#[from] tokio_postgres::Error),

    #[error("unexpected data from database: {0}")]
    UnexpectedData(String),
}
