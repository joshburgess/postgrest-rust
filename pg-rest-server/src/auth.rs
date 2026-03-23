use std::collections::HashMap;
use std::sync::Mutex;

use axum::http::{header, HeaderMap};

use crate::error::ApiError;
use crate::state::AppState;

#[derive(Debug, Clone)]
pub struct JwtClaims {
    pub role: String,
    /// Raw JSON string of all claims, forwarded to PostgreSQL as a GUC.
    pub raw: String,
}

/// LRU-style JWT cache: token string → validated claims.
/// Avoids redundant HMAC-SHA256 for repeated tokens.
pub struct JwtCache {
    entries: Mutex<HashMap<u64, JwtClaims>>,
}

impl Default for JwtCache {
    fn default() -> Self {
        Self::new()
    }
}

impl JwtCache {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::with_capacity(256)),
        }
    }

    fn hash_token(token: &str) -> u64 {
        // FNV-1a hash for fast lookup (not crypto — just cache key).
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in token.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    pub fn get(&self, token: &str) -> Option<JwtClaims> {
        let key = Self::hash_token(token);
        let cache = self.entries.lock().unwrap();
        cache.get(&key).cloned()
    }

    pub fn insert(&self, token: &str, claims: JwtClaims) {
        let key = Self::hash_token(token);
        let mut cache = self.entries.lock().unwrap();
        // Evict if too large.
        if cache.len() >= 1024 {
            cache.clear();
        }
        cache.insert(key, claims);
    }
}

/// Extract JWT claims from the Authorization header.
/// Uses a cache to skip HMAC validation for repeated tokens.
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

    // Check cache first.
    if let Some(claims) = state.jwt_cache.get(token) {
        return Ok(Some(claims));
    }

    // Cache miss — validate.
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

    let raw = serde_json::to_string(&data.claims).unwrap_or_default();

    let claims = JwtClaims { role, raw };
    state.jwt_cache.insert(token, claims.clone());
    Ok(Some(claims))
}
