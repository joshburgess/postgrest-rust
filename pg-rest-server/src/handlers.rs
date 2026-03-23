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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandlingMode {
    Lenient,
    Strict,
}

struct Preferences {
    return_pref: ReturnPreference,
    count: CountOption,
    resolution: Option<ConflictAction>,
    handling: HandlingMode,
}

fn parse_prefer(headers: &HeaderMap) -> Preferences {
    let mut prefs = Preferences {
        return_pref: ReturnPreference::Minimal,
        count: CountOption::None,
        resolution: None,
        handling: HandlingMode::Lenient,
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
                    "handling=strict" => prefs.handling = HandlingMode::Strict,
                    "handling=lenient" => prefs.handling = HandlingMode::Lenient,
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

/// Like extract_filters but works with Vec<(String,String)> to support duplicate keys.
fn extract_filters_multi(params: &[(String, String)]) -> Result<FilterNode, ApiError> {
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

/// Parse raw query string into (key, value) pairs, preserving duplicates.
fn parse_query_pairs(query: &str) -> Vec<(String, String)> {
    if query.is_empty() {
        return Vec::new();
    }
    query
        .split('&')
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            Some((
                urlencoding_decode(k),
                urlencoding_decode(v),
            ))
        })
        .collect()
}

fn urlencoding_decode(s: &str) -> String {
    let mut bytes = Vec::with_capacity(s.len());
    let mut iter = s.bytes();
    while let Some(b) = iter.next() {
        match b {
            b'%' => {
                let hi = iter.next().and_then(hex_val);
                let lo = iter.next().and_then(hex_val);
                if let (Some(h), Some(l)) = (hi, lo) {
                    bytes.push(h << 4 | l);
                }
            }
            b'+' => bytes.push(b' '),
            _ => bytes.push(b),
        }
    }
    String::from_utf8(bytes).unwrap_or_default()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
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
// Pipeline protocol: send everything in 1 round trip
// ---------------------------------------------------------------------------

/// Inline bind parameters into the SQL string for use with simple_query.
/// Replaces $1, $2, etc. with properly escaped literal values.
#[allow(dead_code)]
fn inline_params(sql: &str, params: &[String]) -> String {
    let mut result = sql.to_string();
    // Replace in reverse order so $10 doesn't match $1 first.
    for (i, param) in params.iter().enumerate().rev() {
        let placeholder = format!("${}", i + 1);
        let escaped = format!("'{}'", param.replace('\'', "''"));
        result = result.replace(&placeholder, &escaped);
    }
    result
}

/// Extract the text result from simple_query response messages.
/// Scans for the last Row message which contains our JSON result.
#[allow(dead_code)]
fn extract_simple_query_result(
    msgs: &[tokio_postgres::SimpleQueryMessage],
) -> Option<String> {
    // The data query result is the last Row in the response.
    // Walk backwards to find it (skipping CommandComplete messages from COMMIT etc.).
    for msg in msgs.iter().rev() {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            return row.get(0).map(|s| s.to_string());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Execute helper
// ---------------------------------------------------------------------------

/// Execute via pg-wire: binary protocol pipelining.
/// Auth path: BEGIN + SET LOCAL ROLE + set_config + parameterized query + COMMIT
/// in ONE TCP write with binary-safe parameters.
/// Anon path: just the parameterized query (1 round trip).
async fn execute_wire(
    async_conn: &std::sync::Arc<pg_wire::AsyncConn>,
    claims: &Option<JwtClaims>,
    anon_role: &str,
    sql: &SqlOutput,
) -> Result<Option<String>, ApiError> {
    let param_bytes: Vec<Vec<u8>> = sql.params.iter().map(|s| s.as_bytes().to_vec()).collect();
    let param_refs: Vec<Option<&[u8]>> = param_bytes.iter().map(|b| Some(b.as_slice())).collect();
    let param_oids: Vec<u32> = vec![0; sql.params.len()];

    if claims.is_none() {
        // Anon: pipeline SET ROLE + parameterized query.
        let quoted_role = format!("\"{}\"", anon_role.replace('"', "\"\""));
        let setup = format!("BEGIN; SET LOCAL ROLE {quoted_role}");
        let rows = async_conn
            .exec_transaction(&setup, &sql.sql, &param_refs, &param_oids)
            .await
            .map_err(crate::error::map_wire_error)?;
        let json = rows
            .first()
            .and_then(|r| r.first())
            .and_then(|c| c.as_ref())
            .map(|b| String::from_utf8_lossy(b).into_owned());
        return Ok(json);
    }

    // Authenticated: pipeline transaction.
    let role = claims.as_ref().map(|c| c.role.as_str()).unwrap_or(anon_role);
    let quoted_role = format!("\"{}\"", role.replace('"', "\"\""));

    let setup_sql = if let Some(claims) = claims {
        let escaped = claims.raw.replace('\'', "''");
        format!(
            "BEGIN; SET LOCAL ROLE {quoted_role}; \
             SELECT set_config('request.jwt.claims', '{escaped}', true)"
        )
    } else {
        format!("BEGIN; SET LOCAL ROLE {quoted_role}")
    };

    let rows = async_conn
        .exec_transaction(&setup_sql, &sql.sql, &param_refs, &param_oids)
        .await
        .map_err(crate::error::map_wire_error)?;

    let json = rows
        .first()
        .and_then(|r| r.first())
        .and_then(|c| c.as_ref())
        .map(|b| String::from_utf8_lossy(b).into_owned());
    Ok(json)
}

/// Execute via pg-wire with optional count query.
async fn execute_wire_with_count(
    async_conn: &std::sync::Arc<pg_wire::AsyncConn>,
    claims: &Option<JwtClaims>,
    anon_role: &str,
    sql: &SqlOutput,
    count_sql: Option<&SqlOutput>,
) -> Result<(Option<String>, Option<i64>), ApiError> {
    let json = execute_wire(async_conn, claims, anon_role, sql).await?;

    let total = if let Some(csql) = count_sql {
        let cp: Vec<Vec<u8>> = csql.params.iter().map(|s| s.as_bytes().to_vec()).collect();
        let cpr: Vec<Option<&[u8]>> = cp.iter().map(|b| Some(b.as_slice())).collect();
        let co: Vec<u32> = vec![0; csql.params.len()];
        let rows = async_conn
            .exec_query(&csql.sql, &cpr, &co)
            .await
            .map_err(crate::error::map_wire_error)?;
        rows.first()
            .and_then(|r| r.first())
            .and_then(|c| c.as_ref())
            .and_then(|b| String::from_utf8_lossy(b).parse::<i64>().ok())
    } else {
        None
    };

    Ok((json, total))
}

// Legacy execute helpers (kept for fallback/schema cache which uses tokio-postgres).
#[allow(dead_code)]
async fn execute_with_role(
    pool: &deadpool_postgres::Pool,
    claims: &Option<JwtClaims>,
    anon_role: &str,
    sql: &SqlOutput,
) -> Result<Option<String>, ApiError> {
    // Fast path: anon requests — 1 round trip, no role switch.
    if claims.is_none() {
        let client = pool.get().await?;
        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = sql
            .params
            .iter()
            .map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();
        let rows = client.query(&sql.sql, &param_refs).await?;
        let json: Option<String> = rows
            .first()
            .and_then(|r| r.try_get::<_, String>(0).ok());
        return Ok(json);
    }

    // Authenticated path: pipeline everything into 1 simple_query call.
    // BEGIN + SET LOCAL ROLE + set_config + data query + COMMIT = 1 round trip.
    let client = pool.get().await?;
    let role = claims.as_ref().map(|c| c.role.as_str()).unwrap_or(anon_role);
    let quoted_role = format!("\"{}\"", role.replace('"', "\"\""));
    let inlined_query = inline_params(&sql.sql, &sql.params);

    let pipeline_sql = if let Some(claims) = claims {
        let escaped_claims = claims.raw.replace('\'', "''");
        format!(
            "BEGIN; \
             SET LOCAL ROLE {quoted_role}; \
             SELECT set_config('request.jwt.claims', '{escaped_claims}', true); \
             {inlined_query}; \
             COMMIT"
        )
    } else {
        format!(
            "BEGIN; \
             SET LOCAL ROLE {quoted_role}; \
             {inlined_query}; \
             COMMIT"
        )
    };

    match client.simple_query(&pipeline_sql).await {
        Ok(msgs) => {
            let json = extract_simple_query_result(&msgs);
            Ok(json)
        }
        Err(e) => {
            // Clean up the aborted transaction so the connection is reusable.
            let _ = client.simple_query("ROLLBACK").await;
            Err(ApiError::Database(e))
        }
    }
}

/// Execute a data query and an optional count query.
/// Fast path for anon reads: skip transaction, 1 round trip.
/// Authenticated or count queries: use transaction for role scoping.
#[allow(dead_code)]
async fn execute_with_count(
    pool: &deadpool_postgres::Pool,
    claims: &Option<JwtClaims>,
    anon_role: &str,
    sql: &SqlOutput,
    count_sql: Option<&SqlOutput>,
) -> Result<(Option<String>, Option<i64>), ApiError> {
    // Fast path: anon reads without count — 1 round trip via simple_query.
    if claims.is_none() && count_sql.is_none() {
        let client = pool.get().await?;
        let inlined = inline_params(&sql.sql, &sql.params);
        let msgs = client.simple_query(&inlined).await?;
        let json = extract_simple_query_result(&msgs);
        return Ok((json, None));
    }

    // Anon with count — pipeline data + count in one simple_query.
    if claims.is_none() {
        let client = pool.get().await?;
        let inlined = inline_params(&sql.sql, &sql.params);
        let pipeline = if let Some(csql) = count_sql {
            let inlined_count = inline_params(&csql.sql, &csql.params);
            format!("{inlined}; {inlined_count}")
        } else {
            inlined
        };
        let msgs = client.simple_query(&pipeline).await.map_err(|e| {
            // No transaction to rollback for anon path.
            ApiError::Database(e)
        })?;

        // Extract data (first Row) and count (second Row if present).
        let mut rows_iter = msgs.iter().filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                Some(r)
            } else {
                None
            }
        });
        let json = rows_iter.next().and_then(|r| r.get(0).map(|s| s.to_string()));
        let total = if count_sql.is_some() {
            rows_iter.next().and_then(|r| r.get(0).and_then(|s| s.parse::<i64>().ok()))
        } else {
            None
        };
        return Ok((json, total));
    }

    // Authenticated path: pipeline BEGIN + SET ROLE + set_config + query [+ count] + COMMIT.
    // All in 1 simple_query call = 1 TCP round trip.
    let client = pool.get().await?;
    let role = claims.as_ref().map(|c| c.role.as_str()).unwrap_or(anon_role);
    let quoted_role = format!("\"{}\"", role.replace('"', "\"\""));
    let inlined_query = inline_params(&sql.sql, &sql.params);

    let mut pipeline = if let Some(claims) = claims {
        let escaped_claims = claims.raw.replace('\'', "''");
        format!(
            "BEGIN; \
             SET LOCAL ROLE {quoted_role}; \
             SELECT set_config('request.jwt.claims', '{escaped_claims}', true); \
             {inlined_query}"
        )
    } else {
        format!(
            "BEGIN; \
             SET LOCAL ROLE {quoted_role}; \
             {inlined_query}"
        )
    };

    if let Some(csql) = count_sql {
        let inlined_count = inline_params(&csql.sql, &csql.params);
        pipeline.push_str("; ");
        pipeline.push_str(&inlined_count);
    }
    pipeline.push_str("; COMMIT");

    let msgs = match client.simple_query(&pipeline).await {
        Ok(m) => m,
        Err(e) => {
            let _ = client.simple_query("ROLLBACK").await;
            return Err(ApiError::Database(e));
        }
    };

    // Extract results: data query row, then optional count row.
    let mut rows_iter = msgs.iter().filter_map(|m| {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            Some(r)
        } else {
            None
        }
    });

    // Skip the set_config result row (if JWT claims were set).
    if claims.is_some() {
        rows_iter.next(); // set_config returns a row
    }

    let json = rows_iter.next().and_then(|r| r.get(0).map(|s| s.to_string()));
    let total = if count_sql.is_some() {
        rows_iter.next().and_then(|r| r.get(0).and_then(|s| s.parse::<i64>().ok()))
    } else {
        None
    };

    Ok((json, total))
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

pub async fn handle_read(
    State(state): State<Arc<AppState>>,
    Path(table): Path<String>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let params = parse_query_pairs(raw_query.as_deref().unwrap_or(""));
    let claims = extract_jwt_claims(&headers, &state)?;
    let cache: Arc<SchemaCache> = state.schema_cache.borrow().clone();
    let schemas = resolve_schemas(&headers, &state.config.database.schemas)?;

    let table_meta = cache
        .find_table(&table, schemas)
        .ok_or_else(|| ApiError::TableNotFound(table.clone()))?;
    let table_qn = table_meta.name.clone();

    let select = parse_select(
        params.iter().find(|(k,_)| k == "select").map(|(_,v)| v.as_str()).unwrap_or("*"),
    )?;
    // PostgREST uses the last order param (not combined).
    let order_str = params.iter()
        .rfind(|(k,_)| k == "order")
        .map(|(_,v)| v.as_str())
        .unwrap_or("");
    let order = parse_order(order_str)?;
    let filters = extract_filters_multi(&params)?;

    let (range_limit, range_offset) = parse_range(&headers);
    let limit = params.iter().find(|(k,_)| k == "limit")
        .and_then(|(_,v)| v.parse().ok())
        .or(range_limit);
    let offset = params.iter().find(|(k,_)| k == "offset")
        .and_then(|(_,v)| v.parse().ok())
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

    let (json, total) = execute_wire_with_count(
        &state.async_conn,
        &claims,
        &state.config.database.anon_role,
        &sql,
        count_sql.as_ref(),
    )
    .await?;

    let body = json.unwrap_or_else(|| "[]".to_string());

    // ETag: simple hash of the response body.
    let etag = format!("\"{}\"", simple_hash(&body));
    if let Some(if_none_match) = headers.get(header::IF_NONE_MATCH).and_then(|v| v.to_str().ok()) {
        if if_none_match == etag || if_none_match == "*" {
            return Ok(StatusCode::NOT_MODIFIED.into_response());
        }
    }

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

    // PostgREST returns 206 Partial Content when the result is a subset of the total.
    let row_count = count_json_array(&response_body);
    let status = if let Some(total) = total {
        if row_count < total as usize { StatusCode::PARTIAL_CONTENT } else { StatusCode::OK }
    } else {
        StatusCode::OK
    };

    Ok((
        status,
        [
            (header::CONTENT_TYPE.as_str(), content_type),
            ("content-range", &content_range),
            (header::ETAG.as_str(), &etag),
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
    } else if !table_meta.primary_key.is_empty() {
        // Return PK columns for the Location header even on minimal return.
        table_meta.primary_key.clone()
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
    let json = execute_wire(
        &state.async_conn,
        &claims,
        &state.config.database.anon_role,
        &sql,
    )
    .await?;

    // Build Location header from PK of the first inserted row.
    let location = json.as_deref().and_then(|body| {
        let arr: Vec<serde_json::Value> = serde_json::from_str(body).ok()?;
        let first = arr.first()?;
        let pk_filter: Vec<String> = table_meta
            .primary_key
            .iter()
            .filter_map(|pk| {
                let val = first.get(pk)?;
                let val_str = match val {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                Some(format!("{pk}=eq.{val_str}"))
            })
            .collect();
        if pk_filter.is_empty() {
            None
        } else {
            Some(format!("/{table}?{}", pk_filter.join("&")))
        }
    });

    let mut resp = match (prefs.return_pref, json) {
        (ReturnPreference::Representation, Some(j)) => (
            StatusCode::CREATED,
            [(header::CONTENT_TYPE, "application/json")],
            j,
        )
            .into_response(),
        _ => StatusCode::CREATED.into_response(),
    };

    if let Some(loc) = location {
        if let Ok(val) = loc.parse() {
            resp.headers_mut().insert(header::LOCATION, val);
        }
    }

    Ok(resp)
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
    let json = execute_wire(
        &state.async_conn,
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
    let json = execute_wire(
        &state.async_conn,
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
    let json = execute_wire(
        &state.async_conn,
        &claims,
        &state.config.database.anon_role,
        &sql,
    )
    .await?;

    // Void functions return 204 No Content (PostgREST compat).
    if matches!(func.return_type, ReturnType::Void) {
        return Ok(StatusCode::NO_CONTENT.into_response());
    }

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
    let cached = state.openapi_cache.read().await;
    let spec = match params.get("openapi-version").map(String::as_str) {
        Some("3") | Some("3.0") => cached.1.clone(),
        _ => cached.0.clone(),
    };
    drop(cached);

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/openapi+json")],
        spec,
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

    // Rebuild cached OpenAPI specs.
    let specs = state.rebuild_openapi_cache();
    *state.openapi_cache.write().await = specs;

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
// WebSocket NOTIFY forwarding
// ---------------------------------------------------------------------------

/// GET /ws?channel=my_channel — WebSocket endpoint that forwards PostgreSQL
/// NOTIFY messages to connected clients as JSON frames.
pub async fn handle_ws(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    ws: axum::extract::WebSocketUpgrade,
) -> Response {
    let channel = params
        .get("channel")
        .cloned()
        .unwrap_or_else(|| "pgrst".to_string());
    let uri = state.config.database.uri.clone();

    ws.on_upgrade(move |socket| ws_handler(socket, uri, channel))
}

async fn ws_handler(
    mut socket: axum::extract::ws::WebSocket,
    uri: String,
    channel: String,
) {
    use axum::extract::ws::Message;

    let conn = tokio_postgres::connect(&uri, tokio_postgres::NoTls).await;
    let (client, mut connection) = match conn {
        Ok(c) => c,
        Err(e) => {
            let _ = socket
                .send(Message::Text(
                    serde_json::json!({"error": e.to_string()}).to_string().into(),
                ))
                .await;
            return;
        }
    };

    let (notify_tx, mut notify_rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(tokio_postgres::AsyncMessage::Notification(n))) => {
                    if notify_tx.send(n).is_err() {
                        break;
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            }
        }
    });

    let quoted = format!("\"{}\"", channel.replace('"', "\"\""));
    if client
        .execute(&format!("LISTEN {quoted}"), &[])
        .await
        .is_err()
    {
        return;
    }

    loop {
        tokio::select! {
            Some(notification) = notify_rx.recv() => {
                let msg = serde_json::json!({
                    "channel": notification.channel(),
                    "payload": notification.payload(),
                });
                if socket.send(Message::Text(msg.to_string().into())).await.is_err() {
                    break;
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {} // ignore other messages
                }
            }
        }
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

/// Simple FNV-1a hash for ETag generation (not cryptographic).
fn simple_hash(s: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in s.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Quickly count top-level elements of a JSON array string without full parsing.
/// Count top-level elements of a JSON array without full parsing.
/// Counts commas at depth 1 (inside the outer array brackets).
fn count_json_array(s: &str) -> usize {
    let s = s.trim();
    if s.len() < 2 || s == "[]" {
        return 0;
    }
    let mut depth = 0i32;
    let mut count = 1usize; // at least one element if not empty
    let mut in_string = false;
    let mut prev = 0u8;
    for &b in s.as_bytes() {
        if in_string {
            if b == b'"' && prev != b'\\' {
                in_string = false;
            }
        } else {
            match b {
                b'"' => in_string = true,
                b'[' | b'{' => depth += 1,
                b']' | b'}' => depth -= 1,
                b',' if depth == 1 => count += 1,
                _ => {}
            }
        }
        prev = b;
    }
    count
}
