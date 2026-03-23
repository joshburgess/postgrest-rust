//! Compile-time checked SQL query macros.
//!
//! Connects to PostgreSQL at compile time via pg-wire to validate SQL
//! and generate typed result structs. Requires `DATABASE_URL` env var.
//!
//! ```ignore
//! let rows = pg_typed::query!("SELECT id, name FROM users WHERE id = $1", user_id)
//!     .fetch_all(&client)
//!     .await?;
//! // rows[0].id: i32, rows[0].name: String — compile-time verified.
//! ```

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, Expr, LitStr, Token};

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

/// Column metadata from pg-wire Describe.
struct ColumnInfo {
    name: String,
    type_oid: u32,
}

/// Connect to PG at compile time using pg-wire and describe the statement.
fn describe_statement(sql: &str) -> Result<(Vec<u32>, Vec<ColumnInfo>), String> {
    let db_url = std::env::var("DATABASE_URL").map_err(|_| {
        "DATABASE_URL environment variable not set. \
         Set it to enable compile-time SQL checking, e.g.: \
         DATABASE_URL=postgres://user:pass@localhost/db"
            .to_string()
    })?;

    // Parse the postgres:// URI.
    let (user, password, host, port, database) = parse_pg_uri(&db_url)
        .ok_or_else(|| format!("Invalid DATABASE_URL: {db_url}"))?;
    let addr = format!("{host}:{port}");

    // Use a single-threaded tokio runtime for the compile-time connection.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to create tokio runtime: {e}"))?;

    rt.block_on(async {
        // Connect using pg-wire.
        let mut conn =
            pg_wire::WireConn::connect(&addr, &user, &password, &database)
                .await
                .map_err(|e| format!("Failed to connect to database: {e}"))?;

        // Describe the statement.
        let (param_oids, fields) = conn
            .describe_statement(sql)
            .await
            .map_err(|e| format!("SQL error: {e}"))?;

        let columns: Vec<ColumnInfo> = fields
            .iter()
            .map(|f| ColumnInfo {
                name: f.name.clone(),
                type_oid: f.type_oid,
            })
            .collect();

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
        1700 => quote! { String }, // numeric
        _ => quote! { Vec<u8> },   // fallback: raw bytes
    }
}

/// `query!("SQL", param1, param2, ...)` — compile-time checked SQL query.
///
/// Connects to PostgreSQL at compile time (via `DATABASE_URL` env var),
/// validates the SQL, checks parameter count, and generates a typed result struct.
///
/// Returns a `CheckedQuery` that can be executed with `.fetch_all(&client)`,
/// `.fetch_one(&client)`, or `.fetch_opt(&client)`.
#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    let QueryInput { sql, params } = parse_macro_input!(input as QueryInput);
    let sql_str = sql.value();

    // Validate at compile time by connecting to PG.
    let (param_oids, column_infos) = match describe_statement(&sql_str) {
        Ok(result) => result,
        Err(e) => {
            return syn::Error::new_spanned(&sql, e)
                .to_compile_error()
                .into();
        }
    };

    // Validate parameter count.
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

    // Generate column field names and types.
    let field_names: Vec<_> = column_infos
        .iter()
        .map(|c| format_ident!("{}", sanitize_ident(&c.name)))
        .collect();
    let field_types: Vec<_> = column_infos
        .iter()
        .map(|c| oid_to_rust_type(c.type_oid))
        .collect();
    let field_indices: Vec<_> = (0..column_infos.len()).collect::<Vec<_>>();

    // Generate a unique struct name.
    let struct_name = format_ident!("__QueryResult_{}", hash_sql(&sql_str));

    // Build the param references.
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
                        #(#field_names: row.get(#field_indices)?,)*
                    })
                },
            }
        }
    };

    TokenStream::from(expanded)
}

/// Sanitize a PG column name to a valid Rust identifier.
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

/// FNV-1a hash for unique struct names.
fn hash_sql(sql: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in sql.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Parse a postgres:// URI into (user, password, host, port, database).
fn parse_pg_uri(uri: &str) -> Option<(String, String, String, u16, String)> {
    let rest = uri.strip_prefix("postgres://").or_else(|| uri.strip_prefix("postgresql://"))?;
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
