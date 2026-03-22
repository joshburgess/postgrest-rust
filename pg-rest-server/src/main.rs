mod auth;
mod config;
mod error;
mod handlers;
mod openapi;
mod state;

use std::path::PathBuf;
use std::sync::Arc;

use axum::routing::get;
use axum::Router;
use clap::Parser;
use tokio::sync::watch;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use config::AppConfig;
use handlers::*;
use state::AppState;

#[derive(Parser)]
#[command(name = "pg-rest-server", about = "Automatic REST API for PostgreSQL")]
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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tower_http=debug".into()),
        )
        .init();

    // 3. Create database pool.
    let pg_config: tokio_postgres::Config = config.database.uri.parse()?;
    let mgr = deadpool_postgres::Manager::new(pg_config, tokio_postgres::NoTls);
    let pool = deadpool_postgres::Pool::builder(mgr)
        .max_size(config.database.pool_size)
        .build()?;

    // 4. Build initial schema cache.
    tracing::info!("Loading schema cache…");
    let client = pool.get().await?;
    let cache =
        pg_schema_cache::build_schema_cache(&client, &config.database.schemas).await?;
    drop(client);
    tracing::info!(
        "Schema cache loaded: {} tables, {} functions",
        cache.tables.len(),
        cache.functions.len()
    );

    // 5. Create watch channel + spawn listener.
    let (cache_tx, cache_rx) = watch::channel(Arc::new(cache));

    let listener_uri = config.database.uri.clone();
    let listener_schemas = config.database.schemas.clone();
    tokio::spawn(async move {
        if let Err(e) = pg_schema_cache::start_schema_listener(
            &listener_uri,
            listener_schemas,
            cache_tx,
            "pgrst",
        )
        .await
        {
            tracing::error!("Schema listener stopped: {e}");
        }
    });

    // 6. Build JWT decoding key.
    let jwt_decoding_key =
        jsonwebtoken::DecodingKey::from_secret(config.jwt.secret.as_bytes());
    let jwt_validation = {
        let mut v = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
        v.required_spec_claims = Default::default(); // don't require exp
        v
    };

    // 7. Build application state.
    let bind_addr = format!("{}:{}", config.server.host, config.server.port);
    let state = Arc::new(AppState {
        pool,
        schema_cache: cache_rx,
        config,
        jwt_decoding_key,
        jwt_validation,
    });

    // 8. Build router.
    let app = Router::new()
        .route("/", get(handle_root))
        .route("/live", get(handle_live))
        .route("/ready", get(handle_ready))
        .route("/rpc/{function}", get(handle_rpc).post(handle_rpc))
        .route(
            "/{table}",
            get(handle_read)
                .post(handle_insert)
                .patch(handle_update)
                .delete(handle_delete),
        )
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state);

    // 9. Start server.
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tracing::info!("Listening on {bind_addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl-c");
    tracing::info!("Shutdown signal received");
}
