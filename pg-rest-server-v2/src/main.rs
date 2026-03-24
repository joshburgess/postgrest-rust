#![allow(clippy::result_large_err)]

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::sync::watch;

use pg_rest_server_v2::config::AppConfig;
use pg_rest_server_v2::state::AppState;

#[derive(Parser)]
#[command(name = "pg-rest-server-v2", about = "Automatic REST API for PostgreSQL")]
struct Cli {
    /// Path to TOML config file
    #[arg(long, default_value = "pg-rest.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Parse CLI args + load config.
    let cli = Cli::parse();
    let config = AppConfig::load(&cli.config)?;

    // 2. Init tracing.
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "info,tower_http=debug".into());

    if config.server.log_format == "json" {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(env_filter)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .init();
    }

    // 3. Parse PG URI for pg-wire pools.
    let (user, password, host, port, database) = parse_pg_uri(&config.database.uri);
    let wire_addr = format!("{host}:{port}");

    // 4. Build initial schema cache using a one-off pg-wire connection.
    tracing::info!("Loading schema cache...");
    let conn = pg_wire::WireConn::connect(&wire_addr, &user, &password, &database).await?;
    let mut pg = pg_wire::PgPipeline::new(conn);
    let cache =
        pg_schema_cache_v2::build_schema_cache(&mut pg, &config.database.schemas).await?;
    drop(pg);
    tracing::info!(
        "Schema cache loaded: {} tables, {} functions",
        cache.tables.len(),
        cache.functions.len()
    );

    // 5. Create watch channel.
    let (cache_tx, cache_rx) = watch::channel(Arc::new(cache));

    // 6. Build JWT decoding key.
    let jwt_decoding_key =
        jsonwebtoken::DecodingKey::from_secret(config.jwt.secret.as_bytes());
    let jwt_validation = {
        let mut v = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
        v.required_spec_claims = Default::default();
        v
    };

    // 7. Create pg-wire pools.
    // ConnPool: checkout/checkin pool for cold-path operations (EXPLAIN, health check).
    let conn_pool = pg_pool::ConnPool::<pg_pool::wire::WirePoolable>::new(
        pg_pool::ConnPoolConfig {
            addr: wire_addr.clone(),
            user: user.clone(),
            password: password.clone(),
            database: database.clone(),
            min_idle: 1,
            max_size: config.database.pool_size.max(2),
            ..Default::default()
        },
        pg_pool::LifecycleHooks::default(),
    )
    .await
    .map_err(|e| format!("ConnPool init failed: {e}"))?;
    tracing::info!("ConnPool created (max_size={})", config.database.pool_size.max(2));

    // AsyncPool: multiplexed connections for the hot path (pipelined binary protocol).
    let async_pool_size = config.database.pool_size.min(8); // cap at 8 PG backends
    let async_pool = pg_wire::AsyncPool::connect(
        &wire_addr, &user, &password, &database, async_pool_size,
    ).await?;
    tracing::info!("AsyncPool created with {} connections", async_pool_size);

    // 8. Build application state + router.
    let bind_addr = format!("{}:{}", config.server.host, config.server.port);
    let state = Arc::new(AppState {
        conn_pool,
        async_pool,
        schema_cache: cache_rx,
        schema_cache_tx: cache_tx,
        openapi_cache: tokio::sync::RwLock::new(("".into(), "".into())),
        anon_setup_sql: {
            let quoted = format!("\"{}\"", config.database.anon_role.replace('"', "\"\""));
            format!("BEGIN; SET LOCAL ROLE {quoted}")
        },
        config,
        jwt_decoding_key,
        jwt_validation,
        jwt_cache: pg_rest_server_v2::auth::JwtCache::new(),
    });

    // Build initial OpenAPI cache.
    {
        let specs = state.rebuild_openapi_cache();
        *state.openapi_cache.write().await = specs;
    }

    let app = pg_rest_server_v2::build_router(state.clone());

    // 8. Spawn reconnecting schema listener.
    tokio::spawn(schema_listener_loop(state));

    // 9. Start server.
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tracing::info!("Listening on {bind_addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

/// Reconnecting LISTEN/NOTIFY schema listener with exponential backoff.
async fn schema_listener_loop(state: Arc<AppState>) {
    let mut backoff = std::time::Duration::from_secs(1);

    let (user, password, host, port, database) = parse_pg_uri(&state.config.database.uri);
    let addr = format!("{host}:{port}");

    loop {
        let schemas = state.config.database.schemas.clone();

        match run_schema_listener(
            &addr, &user, &password, &database, &schemas, &state.schema_cache_tx,
        )
        .await
        {
            Ok(()) => break, // clean shutdown
            Err(e) => {
                tracing::error!("Schema listener error: {e}, retrying in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
            }
        }
    }
}

/// Single attempt at running the LISTEN/NOTIFY listener.
async fn run_schema_listener(
    addr: &str,
    user: &str,
    password: &str,
    database: &str,
    schemas: &[String],
    tx: &watch::Sender<Arc<pg_schema_cache_v2::SchemaCache>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let conn = pg_wire::WireConn::connect(addr, user, password, database).await?;
    let mut pg = pg_wire::PgPipeline::new(conn);

    let quoted = format!("\"{}\"", "pgrst".replace('"', "\"\""));
    pg.simple_query(&format!("LISTEN {quoted}")).await?;
    tracing::info!("Schema listener connected");

    loop {
        let msg = pg.conn().recv_msg().await?;
        if let pg_wire::protocol::types::BackendMsg::NotificationResponse { channel, .. } = msg {
            if channel == "pgrst" {
                tracing::info!("Schema reload notification received");
                match pg_schema_cache_v2::build_schema_cache(&mut pg, schemas).await {
                    Ok(cache) => {
                        tx.send(Arc::new(cache)).ok();
                        tracing::info!("Schema cache reloaded via NOTIFY");
                    }
                    Err(e) => tracing::error!("Schema reload failed: {e}"),
                }
            }
        }
    }
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl-c");
    tracing::info!("Shutdown signal received");
}

/// Parse a postgres:// URI into (user, password, host, port, database).
fn parse_pg_uri(uri: &str) -> (String, String, String, u16, String) {
    // postgres://user:password@host:port/database
    let rest = uri.strip_prefix("postgres://").unwrap_or(uri);
    let (auth, hostdb) = rest.split_once('@').unwrap_or(("postgres:postgres", rest));
    let (user, password) = auth.split_once(':').unwrap_or((auth, ""));
    let (hostport, database) = hostdb.split_once('/').unwrap_or((hostdb, "postgres"));
    let (host, port_str) = hostport.split_once(':').unwrap_or((hostport, "5432"));
    let port: u16 = port_str.parse().unwrap_or(5432);
    (
        user.to_string(),
        password.to_string(),
        host.to_string(),
        port,
        database.to_string(),
    )
}
