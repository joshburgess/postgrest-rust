use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Json, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use pg_query_engine::{
    build_sql, parse_filter, parse_order, parse_select, ApiRequest, ConflictAction, CountOption,
    DeleteRequest, Filter, FunctionCall, InsertRequest, ReadRequest, SelectItem, SqlOutput,
    UpdateRequest,
};
use pg_schema_cache::{ReturnType, SchemaCache};

use crate::auth::{extract_jwt_claims, JwtClaims};
use crate::error::ApiError;
use crate::state::AppState;

// ---------------------------------------------------------------------------
// Query-string params that are NOT filters
// ---------------------------------------------------------------------------

const RESERVED_PARAMS: &[&str] = &["select", "order", "limit", "offset"];

// ---------------------------------------------------------------------------
// Prefer header
// ---------------------------------------------------------------------------

struct Preferences {
    return_repr: bool,
    count: CountOption,
    resolution: Option<ConflictAction>,
}

fn parse_prefer(headers: &HeaderMap) -> Preferences {
    let mut prefs = Preferences {
        return_repr: false,
        count: CountOption::None,
        resolution: None,
    };

    for value in headers.get_all("prefer") {
        if let Ok(s) = value.to_str() {
            for part in s.split(',') {
                match part.trim() {
                    "return=representation" => prefs.return_repr = true,
                    "return=minimal" => prefs.return_repr = false,
                    "count=exact" => prefs.count = CountOption::Exact,
                    "count=planned" => prefs.count = CountOption::Planned,
                    "count=estimated" => prefs.count = CountOption::Estimated,
                    "resolution=merge-duplicates" => {
                        prefs.resolution = Some(ConflictAction::MergeDuplicates)
                    }
                    "resolution=ignore-duplicates" => {
                        prefs.resolution = Some(ConflictAction::IgnoreDuplicates)
                    }
                    _ => {}
                }
            }
        }
    }

    prefs
}

// ---------------------------------------------------------------------------
// Range header
// ---------------------------------------------------------------------------

fn parse_range(headers: &HeaderMap) -> (Option<i64>, Option<i64>) {
    let s = match headers.get(header::RANGE).and_then(|v| v.to_str().ok()) {
        Some(s) => s,
        None => return (None, None),
    };
    let (start_s, end_s) = match s.split_once('-') {
        Some(pair) => pair,
        None => return (None, None),
    };
    let start: i64 = match start_s.parse() {
        Ok(v) => v,
        Err(_) => return (None, None),
    };
    let end: i64 = match end_s.parse() {
        Ok(v) => v,
        Err(_) => return (None, None),
    };
    (Some(end - start + 1), Some(start))
}

// ---------------------------------------------------------------------------
// Parse filters from query params
// ---------------------------------------------------------------------------

fn extract_filters(params: &HashMap<String, String>) -> Result<Vec<Filter>, ApiError> {
    params
        .iter()
        .filter(|(k, _)| !RESERVED_PARAMS.contains(&k.as_str()))
        .map(|(k, v)| parse_filter(k, v).map_err(ApiError::from))
        .collect()
}

// ---------------------------------------------------------------------------
// Execute helper
// ---------------------------------------------------------------------------

async fn execute_with_role(
    pool: &deadpool_postgres::Pool,
    claims: &Option<JwtClaims>,
    anon_role: &str,
    sql: &SqlOutput,
) -> Result<Option<String>, ApiError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    let role = claims
        .as_ref()
        .map(|c| c.role.as_str())
        .unwrap_or(anon_role);

    // SET LOCAL ROLE with identifier quoting.
    let quoted_role = format!("\"{}\"", role.replace('"', "\"\""));
    txn.batch_execute(&format!("SET LOCAL ROLE {quoted_role}"))
        .await?;

    // Forward JWT claims as a GUC variable.
    if let Some(claims) = claims {
        txn.execute(
            "SELECT set_config('request.jwt.claims', $1, true)",
            &[&claims.raw],
        )
        .await?;
    }

    // Execute the query.
    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = sql
        .params
        .iter()
        .map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();

    let rows = txn.query(&sql.sql, &param_refs).await?;
    txn.commit().await?;

    let json: Option<String> = rows
        .first()
        .and_then(|r| r.try_get::<_, String>(0).ok());

    Ok(json)
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

pub async fn handle_read(
    State(state): State<Arc<AppState>>,
    Path(table): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let claims = extract_jwt_claims(&headers, &state)?;
    let cache: Arc<SchemaCache> = state.schema_cache.borrow().clone();
    let schemas = &state.config.database.schemas;

    let table_meta = cache
        .find_table(&table, schemas)
        .ok_or_else(|| ApiError::TableNotFound(table.clone()))?;
    let table_qn = table_meta.name.clone();

    let select = parse_select(
        params.get("select").map(String::as_str).unwrap_or("*"),
    )?;
    let order = parse_order(
        params.get("order").map(String::as_str).unwrap_or(""),
    )?;
    let filters = extract_filters(&params)?;

    let (range_limit, range_offset) = parse_range(&headers);
    let limit = params
        .get("limit")
        .and_then(|l| l.parse().ok())
        .or(range_limit);
    let offset = params
        .get("offset")
        .and_then(|o| o.parse().ok())
        .or(range_offset);

    let req = ApiRequest::Read(ReadRequest {
        table: table_qn,
        select,
        filters,
        order,
        limit,
        offset,
        count: CountOption::None,
    });

    let sql = build_sql(&cache, &req, schemas)?;
    let json = execute_with_role(
        &state.pool,
        &claims,
        &state.config.database.anon_role,
        &sql,
    )
    .await?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        json.unwrap_or_else(|| "[]".to_string()),
    )
        .into_response())
}

pub async fn handle_insert(
    State(state): State<Arc<AppState>>,
    Path(table): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<Response, ApiError> {
    let claims = extract_jwt_claims(&headers, &state)?;
    let cache: Arc<SchemaCache> = state.schema_cache.borrow().clone();
    let schemas = &state.config.database.schemas;
    let prefs = parse_prefer(&headers);

    let table_meta = cache
        .find_table(&table, schemas)
        .ok_or_else(|| ApiError::TableNotFound(table.clone()))?;
    if !table_meta.insertable {
        return Err(ApiError::MethodNotAllowed);
    }
    let table_qn = table_meta.name.clone();

    let rows = body_to_rows(body)?;
    let returning = if prefs.return_repr {
        vec!["*".to_string()]
    } else {
        Vec::new()
    };

    let select = parse_select(
        params.get("select").map(String::as_str).unwrap_or("*"),
    )?;
    let _ = select; // TODO: use for column filtering on RETURNING

    let req = ApiRequest::Insert(InsertRequest {
        table: table_qn,
        rows,
        on_conflict: prefs.resolution,
        returning,
    });

    let sql = build_sql(&cache, &req, schemas)?;
    let json = execute_with_role(
        &state.pool,
        &claims,
        &state.config.database.anon_role,
        &sql,
    )
    .await?;

    match json {
        Some(j) => Ok((
            StatusCode::CREATED,
            [(header::CONTENT_TYPE, "application/json")],
            j,
        )
            .into_response()),
        None => Ok(StatusCode::CREATED.into_response()),
    }
}

pub async fn handle_update(
    State(state): State<Arc<AppState>>,
    Path(table): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<Response, ApiError> {
    let claims = extract_jwt_claims(&headers, &state)?;
    let cache: Arc<SchemaCache> = state.schema_cache.borrow().clone();
    let schemas = &state.config.database.schemas;
    let prefs = parse_prefer(&headers);

    let table_meta = cache
        .find_table(&table, schemas)
        .ok_or_else(|| ApiError::TableNotFound(table.clone()))?;
    if !table_meta.updatable {
        return Err(ApiError::MethodNotAllowed);
    }
    let table_qn = table_meta.name.clone();

    let set = match body {
        serde_json::Value::Object(m) => m,
        _ => return Err(ApiError::BadRequest("expected JSON object".into())),
    };

    let filters = extract_filters(&params)?;
    let returning = if prefs.return_repr {
        vec!["*".to_string()]
    } else {
        Vec::new()
    };

    let req = ApiRequest::Update(UpdateRequest {
        table: table_qn,
        set,
        filters,
        returning,
    });

    let sql = build_sql(&cache, &req, schemas)?;
    let json = execute_with_role(
        &state.pool,
        &claims,
        &state.config.database.anon_role,
        &sql,
    )
    .await?;

    match json {
        Some(j) => Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            j,
        )
            .into_response()),
        None => Ok(StatusCode::NO_CONTENT.into_response()),
    }
}

pub async fn handle_delete(
    State(state): State<Arc<AppState>>,
    Path(table): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let claims = extract_jwt_claims(&headers, &state)?;
    let cache: Arc<SchemaCache> = state.schema_cache.borrow().clone();
    let schemas = &state.config.database.schemas;
    let prefs = parse_prefer(&headers);

    let table_meta = cache
        .find_table(&table, schemas)
        .ok_or_else(|| ApiError::TableNotFound(table.clone()))?;
    if !table_meta.deletable {
        return Err(ApiError::MethodNotAllowed);
    }
    let table_qn = table_meta.name.clone();

    let filters = extract_filters(&params)?;
    let returning = if prefs.return_repr {
        vec!["*".to_string()]
    } else {
        Vec::new()
    };

    let req = ApiRequest::Delete(DeleteRequest {
        table: table_qn,
        filters,
        returning,
    });

    let sql = build_sql(&cache, &req, schemas)?;
    let json = execute_with_role(
        &state.pool,
        &claims,
        &state.config.database.anon_role,
        &sql,
    )
    .await?;

    match json {
        Some(j) => Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            j,
        )
            .into_response()),
        None => Ok(StatusCode::NO_CONTENT.into_response()),
    }
}

pub async fn handle_rpc(
    State(state): State<Arc<AppState>>,
    Path(function): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Option<Json<serde_json::Value>>,
) -> Result<Response, ApiError> {
    let claims = extract_jwt_claims(&headers, &state)?;
    let cache: Arc<SchemaCache> = state.schema_cache.borrow().clone();
    let schemas = &state.config.database.schemas;

    let func = cache
        .find_function(&function, schemas)
        .ok_or_else(|| ApiError::FunctionNotFound(function.clone()))?;

    let func_qn = func.name.clone();
    let is_scalar = matches!(
        func.return_type,
        ReturnType::Scalar(_) | ReturnType::Void
    );

    // Function params from body (POST) or query string (GET).
    let func_params = if let Some(Json(body)) = body {
        match body {
            serde_json::Value::Object(m) => m,
            _ => return Err(ApiError::BadRequest("expected JSON object".into())),
        }
    } else {
        params
            .iter()
            .filter(|(k, _)| !RESERVED_PARAMS.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect()
    };

    // Optional filtering/ordering of function results.
    let select = parse_select(
        params.get("select").map(String::as_str).unwrap_or("*"),
    )?;
    let order = parse_order(
        params.get("order").map(String::as_str).unwrap_or(""),
    )?;

    let has_select =
        select.iter().any(|s| !matches!(s, SelectItem::Star));
    let read_request = if has_select || !order.is_empty() {
        Some(ReadRequest {
            table: func_qn.clone(),
            select,
            filters: Vec::new(),
            order,
            limit: None,
            offset: None,
            count: CountOption::None,
        })
    } else {
        None
    };

    let req = ApiRequest::CallFunction(FunctionCall {
        function: func_qn,
        params: func_params,
        is_scalar,
        read_request,
    });

    let sql = build_sql(&cache, &req, schemas)?;
    let json = execute_with_role(
        &state.pool,
        &claims,
        &state.config.database.anon_role,
        &sql,
    )
    .await?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        json.unwrap_or_else(|| "null".to_string()),
    )
        .into_response())
}

// ---------------------------------------------------------------------------
// Health endpoints
// ---------------------------------------------------------------------------

pub async fn handle_live() -> StatusCode {
    StatusCode::OK
}

pub async fn handle_ready(State(state): State<Arc<AppState>>) -> StatusCode {
    match state.pool.get().await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn body_to_rows(
    body: serde_json::Value,
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, ApiError> {
    match body {
        serde_json::Value::Array(arr) => arr
            .into_iter()
            .map(|v| match v {
                serde_json::Value::Object(m) => Ok(m),
                _ => Err(ApiError::BadRequest(
                    "expected array of objects".into(),
                )),
            })
            .collect(),
        serde_json::Value::Object(m) => Ok(vec![m]),
        _ => Err(ApiError::BadRequest(
            "expected JSON object or array".into(),
        )),
    }
}
