use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Json, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use pg_query_engine::{
    build_count_sql, build_sql, parse_filter, parse_logic_filter, parse_order, parse_select,
    ApiRequest, ConflictAction, CountOption, DeleteRequest, FilterNode, FunctionCall,
    InsertRequest, ReadRequest, SelectItem, SqlOutput, UpdateRequest,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReturnPreference {
    Minimal,
    HeadersOnly,
    Representation,
}

struct Preferences {
    return_pref: ReturnPreference,
    count: CountOption,
    resolution: Option<ConflictAction>,
}

fn parse_prefer(headers: &HeaderMap) -> Preferences {
    let mut prefs = Preferences {
        return_pref: ReturnPreference::Minimal,
        count: CountOption::None,
        resolution: None,
    };

    for value in headers.get_all("prefer") {
        if let Ok(s) = value.to_str() {
            for part in s.split(',') {
                match part.trim() {
                    "return=representation" => prefs.return_pref = ReturnPreference::Representation,
                    "return=headers-only" => prefs.return_pref = ReturnPreference::HeadersOnly,
                    "return=minimal" => prefs.return_pref = ReturnPreference::Minimal,
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

fn extract_filters(params: &HashMap<String, String>) -> Result<FilterNode, ApiError> {
    let mut nodes: Vec<FilterNode> = Vec::new();

    for (key, value) in params {
        match key.as_str() {
            "or" | "and" => {
                nodes.push(parse_logic_filter(key, value).map_err(ApiError::from)?);
            }
            k if RESERVED_PARAMS.contains(&k) => continue,
            column => {
                nodes.push(FilterNode::Condition(
                    parse_filter(column, value).map_err(ApiError::from)?,
                ));
            }
        }
    }

    Ok(FilterNode::And(nodes))
}

/// Parse `Accept-Profile` (reads) or `Content-Profile` (writes) header
/// to select a specific schema.
fn resolve_schemas<'a>(
    headers: &HeaderMap,
    config_schemas: &'a [String],
) -> Result<&'a [String], ApiError> {
    let profile = headers
        .get("accept-profile")
        .or_else(|| headers.get("content-profile"))
        .and_then(|v| v.to_str().ok());

    if let Some(profile) = profile {
        if config_schemas.iter().any(|s| s == profile) {
            Ok(config_schemas)
        } else {
            Err(ApiError::BadRequest(format!(
                "schema '{profile}' is not in the configured search path"
            )))
        }
    } else {
        Ok(config_schemas)
    }
}

/// Check if the Accept header requests a singular (single-object) response.
fn wants_singular(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.contains("application/vnd.pgrst.object+json"))
        .unwrap_or(false)
}

/// Unwrap a JSON array to a single object for singular responses.
fn to_singular(json_body: &str) -> Result<String, ApiError> {
    let arr: Vec<serde_json::Value> = serde_json::from_str(json_body)
        .map_err(|_| ApiError::NotAcceptable("invalid JSON array".into()))?;
    match arr.len() {
        0 => Err(ApiError::NotAcceptable(
            "no rows returned for singular response".into(),
        )),
        1 => Ok(arr.into_iter().next().unwrap().to_string()),
        n => Err(ApiError::NotAcceptable(format!(
            "expected single row but got {n} rows"
        ))),
    }
}

/// Check if the Accept header requests a query plan.
fn wants_explain(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.contains("application/vnd.pgrst.plan+json"))
        .unwrap_or(false)
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

/// Execute a data query and an optional count query in the same transaction.
async fn execute_with_count(
    pool: &deadpool_postgres::Pool,
    claims: &Option<JwtClaims>,
    anon_role: &str,
    sql: &SqlOutput,
    count_sql: Option<&SqlOutput>,
) -> Result<(Option<String>, Option<i64>), ApiError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    let role = claims
        .as_ref()
        .map(|c| c.role.as_str())
        .unwrap_or(anon_role);

    let quoted_role = format!("\"{}\"", role.replace('"', "\"\""));
    txn.batch_execute(&format!("SET LOCAL ROLE {quoted_role}"))
        .await?;

    if let Some(claims) = claims {
        txn.execute(
            "SELECT set_config('request.jwt.claims', $1, true)",
            &[&claims.raw],
        )
        .await?;
    }

    // Data query.
    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = sql
        .params
        .iter()
        .map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let rows = txn.query(&sql.sql, &param_refs).await?;
    let json: Option<String> = rows
        .first()
        .and_then(|r| r.try_get::<_, String>(0).ok());

    // Count query (if requested).
    let total = if let Some(csql) = count_sql {
        let cp: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = csql
            .params
            .iter()
            .map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();
        let crow = txn.query_one(&csql.sql, &cp).await?;
        Some(crow.get::<_, i64>(0))
    } else {
        None
    };

    txn.commit().await?;
    Ok((json, total))
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
    let schemas = resolve_schemas(&headers, &state.config.database.schemas)?;

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

    let prefs = parse_prefer(&headers);
    let singular = wants_singular(&headers);

    let read_req = ReadRequest {
        table: table_qn,
        select,
        filters,
        order,
        limit,
        offset,
        count: prefs.count,
    };

    let mut sql = build_sql(&cache, &ApiRequest::Read(read_req.clone()), schemas)?;

    // EXPLAIN support: prepend EXPLAIN to return the query plan.
    if wants_explain(&headers) {
        sql.sql = format!("EXPLAIN (FORMAT JSON) {}", sql.sql);
        // EXPLAIN returns json type — execute directly and collect.
        let mut client = state.pool.get().await?;
        let txn = client.transaction().await?;
        let role = claims.as_ref().map(|c| c.role.as_str())
            .unwrap_or(&state.config.database.anon_role);
        let quoted_role = format!("\"{}\"", role.replace('"', "\"\""));
        txn.batch_execute(&format!("SET LOCAL ROLE {quoted_role}")).await?;
        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = sql
            .params.iter().map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
        let rows = txn.query(&sql.sql, &param_refs).await?;
        txn.commit().await?;
        // EXPLAIN FORMAT JSON returns a single row with a json column.
        let plan: serde_json::Value = rows.first()
            .and_then(|r| r.try_get::<_, serde_json::Value>(0).ok())
            .unwrap_or(serde_json::json!([]));
        return Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE.as_str(), "application/vnd.pgrst.plan+json")],
            plan.to_string(),
        )
            .into_response());
    }

    let count_sql = if prefs.count == CountOption::Exact {
        Some(build_count_sql(&cache, &read_req, schemas)?)
    } else {
        None
    };

    let (json, total) = execute_with_count(
        &state.pool,
        &claims,
        &state.config.database.anon_role,
        &sql,
        count_sql.as_ref(),
    )
    .await?;

    let body = json.unwrap_or_else(|| "[]".to_string());

    // Build Content-Range header.
    let content_range = if let Some(total) = total {
        let off = offset.unwrap_or(0);
        let row_count = count_json_array(&body);
        if row_count == 0 {
            format!("*/{total}")
        } else {
            format!("{}-{}/{total}", off, off + row_count as i64 - 1)
        }
    } else {
        "*/*".to_string()
    };

    // Singular response (application/vnd.pgrst.object+json).
    if singular {
        let obj = to_singular(&body)?;
        return Ok((
            StatusCode::OK,
            [
                (header::CONTENT_TYPE.as_str(), "application/vnd.pgrst.object+json"),
                ("content-range", &content_range),
            ],
            obj,
        )
            .into_response());
    }

    // Content negotiation: CSV or JSON.
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json");

    let (content_type, response_body) = if accept.contains("text/csv") {
        ("text/csv", json_array_to_csv(&body))
    } else {
        ("application/json", body)
    };

    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE.as_str(), content_type),
            ("content-range", &content_range),
        ],
        response_body,
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
    let returning = if prefs.return_pref == ReturnPreference::Representation {
        vec!["*".to_string()]
    } else {
        Vec::new()
    };

    let on_conflict_columns = params
        .get("on_conflict")
        .map(|s| s.split(',').map(|c| c.trim().to_string()).collect());

    let req = ApiRequest::Insert(InsertRequest {
        table: table_qn,
        rows,
        on_conflict: prefs.resolution,
        on_conflict_columns,
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

    match (prefs.return_pref, json) {
        (ReturnPreference::Representation, Some(j)) => Ok((
            StatusCode::CREATED,
            [(header::CONTENT_TYPE, "application/json")],
            j,
        )
            .into_response()),
        _ => Ok(StatusCode::CREATED.into_response()),
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
    let returning = if prefs.return_pref == ReturnPreference::Representation {
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
    let returning = if prefs.return_pref == ReturnPreference::Representation {
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
            filters: FilterNode::empty(),
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
// Root — OpenAPI spec
// ---------------------------------------------------------------------------

/// Serves the OpenAPI specification at `GET /`.
/// Defaults to OpenAPI 2.0 (Swagger) for PostgREST compatibility.
/// Use `?openapi-version=3` for OpenAPI 3.0.
pub async fn handle_root(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let cache: Arc<SchemaCache> = state.schema_cache.borrow().clone();

    let spec = match params.get("openapi-version").map(String::as_str) {
        Some("3") | Some("3.0") => crate::openapi::generate_v3(&cache, &state.config),
        _ => crate::openapi::generate_v2(&cache, &state.config),
    };

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/openapi+json"),
        ],
        spec.to_string(),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Schema reload
// ---------------------------------------------------------------------------

/// POST /reload — rebuild the schema cache from the database.
pub async fn handle_reload(
    State(state): State<Arc<AppState>>,
) -> Result<Response, ApiError> {
    let client = state.pool.get().await?;
    let cache =
        pg_schema_cache::build_schema_cache(&client, &state.config.database.schemas).await?;
    drop(client);

    let tables = cache.tables.len();
    let functions = cache.functions.len();
    state.schema_cache_tx.send(Arc::new(cache)).ok();

    tracing::info!("Schema cache reloaded: {tables} tables, {functions} functions");

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::json!({
            "message": "schema cache reloaded",
            "tables": tables,
            "functions": functions,
        })
        .to_string(),
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

/// Prometheus-compatible metrics endpoint.
pub async fn handle_metrics(State(state): State<Arc<AppState>>) -> Response {
    let pool_status = state.pool.status();
    let cache = state.schema_cache.borrow();

    let body = format!(
        "# HELP pg_rest_pool_size Current pool size\n\
         # TYPE pg_rest_pool_size gauge\n\
         pg_rest_pool_size {}\n\
         # HELP pg_rest_pool_available Available connections in pool\n\
         # TYPE pg_rest_pool_available gauge\n\
         pg_rest_pool_available {}\n\
         # HELP pg_rest_pool_max_size Maximum pool size\n\
         # TYPE pg_rest_pool_max_size gauge\n\
         pg_rest_pool_max_size {}\n\
         # HELP pg_rest_schema_tables Number of tables in schema cache\n\
         # TYPE pg_rest_schema_tables gauge\n\
         pg_rest_schema_tables {}\n\
         # HELP pg_rest_schema_functions Number of functions in schema cache\n\
         # TYPE pg_rest_schema_functions gauge\n\
         pg_rest_schema_functions {}\n",
        pool_status.size,
        pool_status.available,
        pool_status.max_size,
        cache.tables.len(),
        cache.functions.len(),
    );

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
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

/// Convert a JSON array of objects to CSV format.
fn json_array_to_csv(json_str: &str) -> String {
    let arr: Vec<serde_json::Map<String, serde_json::Value>> =
        match serde_json::from_str(json_str) {
            Ok(a) => a,
            Err(_) => return String::new(),
        };

    if arr.is_empty() {
        return String::new();
    }

    // Collect all column names from the first row (preserving order).
    let columns: Vec<&String> = arr[0].keys().collect();

    let mut out = String::new();

    // Header row.
    for (i, col) in columns.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&csv_escape(col));
    }
    out.push('\n');

    // Data rows.
    for row in &arr {
        for (i, col) in columns.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            match row.get(*col) {
                Some(serde_json::Value::Null) | None => {}
                Some(serde_json::Value::String(s)) => out.push_str(&csv_escape(s)),
                Some(v) => out.push_str(&v.to_string()),
            }
        }
        out.push('\n');
    }

    out
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Quickly count top-level elements of a JSON array string without full parsing.
fn count_json_array(s: &str) -> usize {
    let trimmed = s.trim();
    if trimmed == "[]" {
        return 0;
    }
    serde_json::from_str::<serde_json::Value>(trimmed)
        .ok()
        .and_then(|v| v.as_array().map(|a| a.len()))
        .unwrap_or(0)
}
