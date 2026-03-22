use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::sync::watch;

use pg_rest_server::config::AppConfig;
use pg_rest_server::state::AppState;

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
    let recycling = if config.database.prepared_statements {
        deadpool_postgres::RecyclingMethod::Fast
    } else {
        deadpool_postgres::RecyclingMethod::Clean
    };
    let mgr_config = deadpool_postgres::ManagerConfig { recycling_method: recycling };

    #[cfg(feature = "tls")]
    let pool = {
        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::from_iter(
                webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
            ))
            .with_no_client_auth();
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
        let mgr = deadpool_postgres::Manager::from_config(pg_config, tls, mgr_config);
        deadpool_postgres::Pool::builder(mgr)
            .max_size(config.database.pool_size)
            .build()?
    };

    #[cfg(not(feature = "tls"))]
    let pool = {
        let mgr = deadpool_postgres::Manager::from_config(
            pg_config,
            tokio_postgres::NoTls,
            mgr_config,
        );
        deadpool_postgres::Pool::builder(mgr)
            .max_size(config.database.pool_size)
            .build()?
    };

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
        v.required_spec_claims = Default::default();
        v
    };

    // 7. Build application state + router.
    let bind_addr = format!("{}:{}", config.server.host, config.server.port);
    let state = Arc::new(AppState {
        pool,
        schema_cache: cache_rx,
        config,
        jwt_decoding_key,
        jwt_validation,
    });

    let app = pg_rest_server::build_router(state);

    // 8. Start server.
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
