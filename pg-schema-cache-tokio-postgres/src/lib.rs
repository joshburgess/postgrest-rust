mod error;
mod introspection;
mod listener;

pub use error::SchemaCacheError;
pub use listener::start_schema_listener;
pub use pg_schema_cache_types::*;

use tokio_postgres::Client;

/// Introspects a PostgreSQL database and builds a [`SchemaCache`] containing
/// all tables, columns, relationships, and functions in the given schemas.
pub async fn build_schema_cache(
    client: &Client,
    schemas: &[String],
) -> Result<SchemaCache, SchemaCacheError> {
    introspection::build(client, schemas).await
}
