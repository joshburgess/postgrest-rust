mod error;
mod introspection;
mod listener;

pub use error::SchemaCacheError;
pub use listener::start_schema_listener;
// Re-export the canonical SchemaCache types so consumers of this crate can
// use them without also depending on pg-schema-cache directly.
pub use pg_schema_cache::{
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
