use std::sync::Arc;

use tokio::sync::watch;

use pg_wire::protocol::types::BackendMsg;
use pg_wire::{PgPipeline, WireConn};

use crate::{build_schema_cache, SchemaCache, SchemaCacheError};

/// Establishes a dedicated PostgreSQL connection, executes `LISTEN` on the
/// given channel, and rebuilds the [`SchemaCache`] whenever a notification
/// arrives. The new cache is broadcast to all holders of a
/// [`watch::Receiver<Arc<SchemaCache>>`].
///
/// This function runs indefinitely until the connection drops.
pub async fn start_schema_listener(
    addr: &str,
    user: &str,
    password: &str,
    database: &str,
    schemas: Vec<String>,
    tx: watch::Sender<Arc<SchemaCache>>,
    channel_name: &str,
) -> Result<(), SchemaCacheError> {
    let conn = WireConn::connect(addr, user, password, database).await?;
    let mut pg = PgPipeline::new(conn);

    // Identifier-quote the channel name to prevent injection.
    let quoted = format!("\"{}\"", channel_name.replace('"', "\"\""));
    pg.simple_query(&format!("LISTEN {quoted}")).await?;

    tracing::info!("Schema listener started on channel '{channel_name}'");

    loop {
        // Wait for notification via the connection's recv_msg.
        let msg = pg.conn().recv_msg().await?;
        if let BackendMsg::NotificationResponse { channel, .. } = msg {
            if channel == channel_name {
                tracing::info!("Schema reload notification received");
                match build_schema_cache(&mut pg, &schemas).await {
                    Ok(cache) => {
                        tx.send(Arc::new(cache)).ok();
                        tracing::info!("Schema cache reloaded");
                    }
                    Err(e) => {
                        tracing::error!("Schema cache reload failed: {e}");
                    }
                }
            }
        }
    }
}
