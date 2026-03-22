use std::sync::Arc;

use tokio::sync::watch;

use crate::config::AppConfig;
use pg_schema_cache::SchemaCache;

pub struct AppState {
    pub pool: deadpool_postgres::Pool,
    pub schema_cache: watch::Receiver<Arc<SchemaCache>>,
    pub config: AppConfig,
    pub jwt_decoding_key: jsonwebtoken::DecodingKey,
    pub jwt_validation: jsonwebtoken::Validation,
}
