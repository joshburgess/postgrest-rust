pub mod auth;
pub mod config;
pub mod error;
pub mod handlers;
pub mod openapi;
pub mod state;

use std::sync::Arc;

use axum::routing::get;
use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use handlers::*;
use state::AppState;

/// Build the Axum router with all routes and middleware.
/// Separated from `main` so integration tests can construct the app
/// without binding a TCP listener.
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
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
        .with_state(state)
}
