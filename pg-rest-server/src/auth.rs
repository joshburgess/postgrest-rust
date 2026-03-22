use axum::http::{header, HeaderMap};

use crate::error::ApiError;
use crate::state::AppState;

#[derive(Debug, Clone)]
pub struct JwtClaims {
    pub role: String,
    /// Raw JSON string of all claims, forwarded to PostgreSQL as a GUC.
    pub raw: String,
}

/// Extract JWT claims from the Authorization header.
/// Returns `Ok(None)` for anonymous requests (no token).
pub fn extract_jwt_claims(
    headers: &HeaderMap,
    state: &AppState,
) -> Result<Option<JwtClaims>, ApiError> {
    let auth_value = match headers.get(header::AUTHORIZATION) {
        Some(v) => v,
        None => return Ok(None),
    };

    let auth_str = auth_value
        .to_str()
        .map_err(|_| ApiError::Unauthorized("invalid authorization header".into()))?;

    let token = auth_str
        .strip_prefix("Bearer ")
        .ok_or_else(|| ApiError::Unauthorized("expected Bearer token".into()))?;

    let data = jsonwebtoken::decode::<serde_json::Value>(
        token,
        &state.jwt_decoding_key,
        &state.jwt_validation,
    )
    .map_err(|e| ApiError::Unauthorized(e.to_string()))?;

    let role = data
        .claims
        .get("role")
        .and_then(|v| v.as_str())
        .unwrap_or(&state.config.database.anon_role)
        .to_string();

    let raw =
        serde_json::to_string(&data.claims).unwrap_or_default();

    Ok(Some(JwtClaims { role, raw }))
}
