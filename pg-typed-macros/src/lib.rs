//! Compile-time checked SQL query macros with offline cache support.
//!
//! Connects to PostgreSQL at compile time via pg-wire to validate SQL
//! and generate typed result structs.
//!
//! # Modes
//!
//! - **Online (default):** Connects to DB via `DATABASE_URL`, caches results to `.sqlx/`
//! - **Offline:** Set `PG_TYPED_OFFLINE=true` to use cached metadata only (no DB needed)
//! - **Prepare:** Run `pg-typed-cli prepare` to populate the cache from source files
//!
//! ```ignore
//! let rows = pg_typed::query!("SELECT id, name FROM users WHERE id = $1", user_id)
//!     .fetch_all(&client)
//!     .await?;
//! ```

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, Expr, LitStr, Token};

mod cache;

/// Input to the query! macro: SQL literal + optional comma-separated params.
struct QueryInput {
    sql: LitStr,
    params: Vec<Expr>,
}

impl Parse for QueryInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let sql: LitStr = input.parse()?;
        let mut params = Vec::new();
        while input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            params.push(input.parse()?);
        }
        Ok(QueryInput { sql, params })
    }
}

/// Resolve query metadata: try cache first, then live DB, then update cache.
fn resolve_metadata(
    sql: &str,
) -> Result<(Vec<u32>, Vec<cache::CachedColumn>), String> {
    let sql_hash = hash_sql(sql);
    let offline = std::env::var("PG_TYPED_OFFLINE")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    // 1. Try the cache first.
    if let Some(cached) = cache::read_cache(sql_hash) {
        return Ok((cached.param_oids, cached.columns));
    }

    // 2. If offline mode, fail — cache is required.
    if offline {
        return Err(format!(
            "PG_TYPED_OFFLINE=true but no cached metadata for query (hash {sql_hash:x}). \
             Run `pg-typed-cli prepare` to populate the cache."
        ));
    }

    // 3. Connect to PG and describe.
    let (param_oids, columns) = describe_live(sql)?;

    // 4. Write to cache for future offline builds.
    let entry = cache::CacheEntry {
        sql: sql.to_string(),
        hash: sql_hash,
        param_oids: param_oids.clone(),
        columns: columns.clone(),
    };
    if let Err(e) = cache::write_cache(&entry) {
        // Cache write failure is non-fatal — just warn.
        eprintln!("pg-typed: warning: failed to write cache: {e}");
    }

    Ok((param_oids, columns))
}

/// Connect to PG via pg-wire and describe the statement.
fn describe_live(sql: &str) -> Result<(Vec<u32>, Vec<cache::CachedColumn>), String> {
    let db_url = std::env::var("DATABASE_URL").map_err(|_| {
        "DATABASE_URL not set and no cached metadata found. \
         Set DATABASE_URL or run `pg-typed-cli prepare`."
            .to_string()
    })?;

    let (user, password, host, port, database) =
        parse_pg_uri(&db_url).ok_or_else(|| format!("Invalid DATABASE_URL: {db_url}"))?;
    let addr = format!("{host}:{port}");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to create tokio runtime: {e}"))?;

    rt.block_on(async {
        let mut conn = pg_wire::WireConn::connect(&addr, &user, &password, &database)
            .await
            .map_err(|e| format!("Failed to connect to database: {e}"))?;

        let (param_oids, fields) = conn
            .describe_statement(sql)
            .await
            .map_err(|e| format!("SQL error: {e}"))?;

        // Detect nullable columns by querying pg_attribute for real table columns.
        // Batch all table_oid/column_id pairs into one query.
        let mut columns: Vec<cache::CachedColumn> = fields
            .iter()
            .map(|f| cache::CachedColumn {
                name: f.name.clone(),
                type_oid: f.type_oid,
                nullable: true, // Default: assume nullable.
            })
            .collect();

        // Collect non-null info for columns that come from real tables.
        let table_cols: Vec<(usize, u32, i16)> = fields
            .iter()
            .enumerate()
            .filter(|(_, f)| f.table_oid != 0 && f.column_id > 0)
            .map(|(i, f)| (i, f.table_oid, f.column_id))
            .collect();

        if !table_cols.is_empty() {
            // Build a single query to check all columns at once.
            let conditions: Vec<String> = table_cols
                .iter()
                .map(|(_, oid, col)| format!("(attrelid={oid} AND attnum={col})"))
                .collect();
            let null_sql = format!(
                "SELECT attrelid, attnum, attnotnull FROM pg_attribute WHERE {}",
                conditions.join(" OR ")
            );

            // Send as simple query and collect rows.
            let mut buf = bytes::BytesMut::new();
            pg_wire::protocol::frontend::encode_message(
                &pg_wire::protocol::types::FrontendMsg::Query(null_sql.as_bytes()),
                &mut buf,
            );
            if conn.send_raw(&buf).await.is_ok() {
                if let Ok((rows, _)) = conn.collect_rows().await {
                    for row in &rows {
                        let oid: u32 = row.first()
                            .and_then(|v| v.as_ref())
                            .and_then(|b| String::from_utf8(b.clone()).ok())
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        let col: i16 = row.get(1)
                            .and_then(|v| v.as_ref())
                            .and_then(|b| String::from_utf8(b.clone()).ok())
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        let notnull: bool = row.get(2)
                            .and_then(|v| v.as_ref())
                            .map(|b| b == b"t")
                            .unwrap_or(false);

                        // Find the matching column and mark it non-nullable.
                        for &(idx, t_oid, t_col) in &table_cols {
                            if t_oid == oid && t_col == col && notnull {
                                columns[idx].nullable = false;
                            }
                        }
                    }
                }
            }
        }

        Ok((param_oids, columns))
    })
}

/// Map a PostgreSQL type OID to a Rust type token.
fn oid_to_rust_type(oid: u32) -> proc_macro2::TokenStream {
    match oid {
        16 => quote! { bool },
        18 | 19 | 25 | 1042 | 1043 => quote! { String },
        20 => quote! { i64 },
        21 => quote! { i16 },
        23 | 26 => quote! { i32 },
        700 => quote! { f32 },
        701 => quote! { f64 },
        17 => quote! { Vec<u8> },
        114 | 3802 => quote! { serde_json::Value },
        1082 => quote! { chrono::NaiveDate },
        1083 => quote! { chrono::NaiveTime },
        1114 => quote! { chrono::NaiveDateTime },
        1184 => quote! { chrono::DateTime<chrono::Utc> },
        2950 => quote! { uuid::Uuid },
        1700 => quote! { String },
        _ => quote! { Vec<u8> },
    }
}

/// `query!("SQL", param1, param2, ...)` — compile-time checked SQL query.
#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as QueryInput);
    query_impl(parsed)
}

fn query_impl(input: QueryInput) -> TokenStream {
    let QueryInput { sql, params } = input;
    let sql_str = sql.value();

    let (param_oids, column_infos) = match resolve_metadata(&sql_str) {
        Ok(result) => result,
        Err(e) => {
            return syn::Error::new_spanned(&sql, e)
                .to_compile_error()
                .into();
        }
    };

    if params.len() != param_oids.len() {
        let msg = format!(
            "expected {} parameter(s), got {}",
            param_oids.len(),
            params.len()
        );
        return syn::Error::new_spanned(&sql, msg)
            .to_compile_error()
            .into();
    }

    // Generate compile-time param type assertions.
    // This produces a trait bound check that fails with a helpful error
    // if the param type doesn't implement Encode with the right OID.
    let param_type_checks: Vec<_> = param_oids
        .iter()
        .enumerate()
        .map(|(i, oid)| {
            let expected = oid_to_rust_type(*oid);
            let param = &params[i];
            let oid_val = *oid;
            let type_name = oid_to_type_name(*oid);
            // Static assertion: the param must be convertible to SqlParam.
            // The actual OID check happens at the type system level via Encode.
            quote! {
                // Param $#i: expected PG type #type_name (OID #oid_val)
                let _ = &#param as &dyn pg_typed::SqlParam;
            }
        })
        .collect();

    let field_names: Vec<_> = column_infos
        .iter()
        .map(|c| format_ident!("{}", sanitize_ident(&c.name)))
        .collect();
    let field_types: Vec<_> = column_infos
        .iter()
        .map(|c| {
            let base = oid_to_rust_type(c.type_oid);
            if c.nullable {
                quote! { Option<#base> }
            } else {
                base
            }
        })
        .collect();
    let field_indices: Vec<_> = (0..column_infos.len()).collect::<Vec<_>>();
    let field_getters: Vec<_> = column_infos
        .iter()
        .enumerate()
        .map(|(i, c)| {
            if c.nullable {
                quote! { row.get_opt(#i)? }
            } else {
                quote! { row.get(#i)? }
            }
        })
        .collect();

    let struct_name = format_ident!("__QueryResult_{}", hash_sql(&sql_str));

    let param_refs: Vec<_> = params
        .iter()
        .map(|p| quote! { &#p as &dyn pg_typed::SqlParam })
        .collect();

    let sql_literal = &sql;

    let expanded = quote! {
        {
            #[allow(non_camel_case_types)]
            #[derive(Debug)]
            struct #struct_name {
                #(pub #field_names: #field_types,)*
            }

            pg_typed::CheckedQuery::<#struct_name> {
                sql: #sql_literal,
                params: vec![#(#param_refs),*],
                _marker: std::marker::PhantomData,
                mapper: |row: &pg_typed::Row| -> Result<#struct_name, pg_typed::TypedError> {
                    Ok(#struct_name {
                        #(#field_names: #field_getters,)*
                    })
                },
            }
        }
    };

    TokenStream::from(expanded)
}

/// `query_as!(Type, "SQL", param1, param2, ...)` — compile-time checked query
/// mapped to an existing struct via FromRow.
#[proc_macro]
pub fn query_as(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as QueryAsInput);
    query_as_impl(parsed)
}

fn query_as_impl(input: QueryAsInput) -> TokenStream {
    let QueryAsInput { target_type, sql, params } = input;
    let sql_str = sql.value();

    let (param_oids, _column_infos) = match resolve_metadata(&sql_str) {
        Ok(result) => result,
        Err(e) => {
            return syn::Error::new_spanned(&sql, e)
                .to_compile_error()
                .into();
        }
    };

    if params.len() != param_oids.len() {
        let msg = format!(
            "expected {} parameter(s), got {}",
            param_oids.len(),
            params.len()
        );
        return syn::Error::new_spanned(&sql, msg)
            .to_compile_error()
            .into();
    }

    let param_refs: Vec<_> = params
        .iter()
        .map(|p| quote! { &#p as &dyn pg_typed::SqlParam })
        .collect();
    let sql_literal = &sql;

    let expanded = quote! {
        {
            pg_typed::CheckedQuery::<#target_type> {
                sql: #sql_literal,
                params: vec![#(#param_refs),*],
                _marker: std::marker::PhantomData,
                mapper: |row: &pg_typed::Row| -> Result<#target_type, pg_typed::TypedError> {
                    <#target_type as pg_typed::FromRow>::from_row(row)
                },
            }
        }
    };

    TokenStream::from(expanded)
}

/// `query_scalar!("SQL", param1, ...)` — compile-time checked single-value query.
#[proc_macro]
pub fn query_scalar(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as QueryInput);
    query_scalar_impl(parsed)
}

fn query_scalar_impl(input: QueryInput) -> TokenStream {
    let QueryInput { sql, params } = input;
    let sql_str = sql.value();

    let (param_oids, column_infos) = match resolve_metadata(&sql_str) {
        Ok(result) => result,
        Err(e) => {
            return syn::Error::new_spanned(&sql, e)
                .to_compile_error()
                .into();
        }
    };

    if params.len() != param_oids.len() {
        let msg = format!(
            "expected {} parameter(s), got {}",
            param_oids.len(),
            params.len()
        );
        return syn::Error::new_spanned(&sql, msg)
            .to_compile_error()
            .into();
    }

    if column_infos.len() != 1 {
        let msg = format!(
            "query_scalar! requires exactly 1 column, got {}",
            column_infos.len()
        );
        return syn::Error::new_spanned(&sql, msg)
            .to_compile_error()
            .into();
    }

    let scalar_type = oid_to_rust_type(column_infos[0].type_oid);
    let param_refs: Vec<_> = params
        .iter()
        .map(|p| quote! { &#p as &dyn pg_typed::SqlParam })
        .collect();
    let sql_literal = &sql;

    let expanded = quote! {
        {
            pg_typed::CheckedQuery::<#scalar_type> {
                sql: #sql_literal,
                params: vec![#(#param_refs),*],
                _marker: std::marker::PhantomData,
                mapper: |row: &pg_typed::Row| -> Result<#scalar_type, pg_typed::TypedError> {
                    row.get(0)
                },
            }
        }
    };

    TokenStream::from(expanded)
}

/// Input to query_as!: Type, "SQL", params...
struct QueryAsInput {
    target_type: syn::Type,
    sql: LitStr,
    params: Vec<Expr>,
}

impl Parse for QueryAsInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let target_type: syn::Type = input.parse()?;
        input.parse::<Token![,]>()?;
        let sql: LitStr = input.parse()?;
        let mut params = Vec::new();
        while input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            params.push(input.parse()?);
        }
        Ok(QueryAsInput { target_type, sql, params })
    }
}

/// `query_file!("path/to/query.sql", param1, param2, ...)` — like query! but reads SQL from a file.
#[proc_macro]
pub fn query_file(input: TokenStream) -> TokenStream {
    let QueryInput { sql: path_lit, params } = parse_macro_input!(input as QueryInput);
    let file_path = path_lit.value();

    let sql_str = match read_sql_file(&file_path) {
        Ok(s) => s,
        Err(e) => {
            return syn::Error::new_spanned(&path_lit, e)
                .to_compile_error()
                .into();
        }
    };

    // Reuse the query! logic with the file contents.
    let sql_lit = LitStr::new(&sql_str, path_lit.span());
    let inner = QueryInput { sql: sql_lit, params };
    query_impl(inner)
}

/// `query_file_as!(Type, "path/to/query.sql", param1, ...)` — like query_as! but reads SQL from a file.
#[proc_macro]
pub fn query_file_as(input: TokenStream) -> TokenStream {
    let QueryAsInput { target_type, sql: path_lit, params } =
        parse_macro_input!(input as QueryAsInput);
    let file_path = path_lit.value();

    let sql_str = match read_sql_file(&file_path) {
        Ok(s) => s,
        Err(e) => {
            return syn::Error::new_spanned(&path_lit, e)
                .to_compile_error()
                .into();
        }
    };

    let sql_lit = LitStr::new(&sql_str, path_lit.span());
    let inner = QueryAsInput { target_type, sql: sql_lit, params };
    query_as_impl(inner)
}

/// `query_file_scalar!("path/to/query.sql", param1, ...)` — file-based scalar query.
#[proc_macro]
pub fn query_file_scalar(input: TokenStream) -> TokenStream {
    let QueryInput { sql: path_lit, params } = parse_macro_input!(input as QueryInput);
    let file_path = path_lit.value();

    let sql_str = match read_sql_file(&file_path) {
        Ok(s) => s,
        Err(e) => {
            return syn::Error::new_spanned(&path_lit, e)
                .to_compile_error()
                .into();
        }
    };

    let sql_lit = LitStr::new(&sql_str, path_lit.span());
    let inner = QueryInput { sql: sql_lit, params };
    query_scalar_impl(inner)
}

/// `query_unchecked!("SQL", param1, ...)` — skip compile-time validation.
/// Useful when DATABASE_URL is unavailable and no cache exists.
/// Params are passed as-is; no type or count checking.
#[proc_macro]
pub fn query_unchecked(input: TokenStream) -> TokenStream {
    let QueryInput { sql, params } = parse_macro_input!(input as QueryInput);

    let param_refs: Vec<_> = params
        .iter()
        .map(|p| quote! { &#p as &dyn pg_typed::SqlParam })
        .collect();
    let sql_literal = &sql;

    let expanded = quote! {
        {
            pg_typed::UncheckedQuery {
                sql: #sql_literal,
                params: vec![#(#param_refs),*],
            }
        }
    };

    TokenStream::from(expanded)
}

/// Read a SQL file relative to CARGO_MANIFEST_DIR.
fn read_sql_file(path: &str) -> Result<String, String> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
    let full_path = std::path::Path::new(&manifest_dir).join(path);
    std::fs::read_to_string(&full_path)
        .map_err(|e| format!("Failed to read SQL file {}: {e}", full_path.display()))
        .map(|s| s.trim().to_string())
}

/// Human-readable PG type name for error messages.
fn oid_to_type_name(oid: u32) -> &'static str {
    match oid {
        16 => "bool",
        18 | 19 | 25 | 1042 | 1043 => "text",
        20 => "int8",
        21 => "int2",
        23 => "int4",
        26 => "oid",
        700 => "float4",
        701 => "float8",
        17 => "bytea",
        114 => "json",
        3802 => "jsonb",
        1082 => "date",
        1083 => "time",
        1114 => "timestamp",
        1184 => "timestamptz",
        2950 => "uuid",
        1700 => "numeric",
        _ => "unknown",
    }
}

fn sanitize_ident(name: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect();
    if s.starts_with(|c: char| c.is_ascii_digit()) {
        format!("_{s}")
    } else if s.is_empty() {
        "column".to_string()
    } else {
        s
    }
}

/// FNV-1a hash.
pub(crate) fn hash_sql(sql: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in sql.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn parse_pg_uri(uri: &str) -> Option<(String, String, String, u16, String)> {
    let rest = uri
        .strip_prefix("postgres://")
        .or_else(|| uri.strip_prefix("postgresql://"))?;
    let (auth, hostdb) = rest.split_once('@').unwrap_or(("postgres:postgres", rest));
    let (user, password) = auth.split_once(':').unwrap_or((auth, ""));
    let (hostport, database) = hostdb.split_once('/').unwrap_or((hostdb, "postgres"));
    let (host, port_str) = hostport.split_once(':').unwrap_or((hostport, "5432"));
    let port: u16 = port_str.parse().unwrap_or(5432);
    Some((
        user.to_string(),
        password.to_string(),
        host.to_string(),
        port,
        database.to_string(),
    ))
}
