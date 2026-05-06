mod error;
mod introspection;
mod listener;

pub use error::SchemaCacheError;
pub use listener::start_schema_listener;
// Re-export types from pg-schema-cache-tokio-postgres so pg-query-engine sees the same types.
pub use pg_schema_cache_tokio_postgres::{
    Column, FuncParam, Function, QualifiedName, RelType, Relationship, ReturnType, SchemaCache,
    Table, Volatility,
};

use resolute::Executor;

/// Introspects a PostgreSQL database and builds a [`SchemaCache`] containing
/// all tables, columns, relationships, and functions in the given schemas.
pub async fn build_schema_cache(
    db: &impl Executor,
    schemas: &[String],
) -> Result<SchemaCache, SchemaCacheError> {
    introspection::build(db, schemas).await
}
