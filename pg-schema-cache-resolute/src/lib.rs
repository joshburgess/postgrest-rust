mod error;
mod introspection;
mod listener;

pub use error::SchemaCacheError;
pub use listener::start_schema_listener;
pub use pg_schema_cache_types::*;

use resolute::Executor;

/// Introspects a PostgreSQL database and builds a [`SchemaCache`] containing
/// all tables, columns, relationships, and functions in the given schemas.
pub async fn build_schema_cache(
    db: &impl Executor,
    schemas: &[String],
) -> Result<SchemaCache, SchemaCacheError> {
    introspection::build(db, schemas).await
}
