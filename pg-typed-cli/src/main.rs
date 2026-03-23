//! CLI tool for pg-typed offline cache management.
//!
//! Usage:
//!   pg-typed-cli prepare    # Scan source files, connect to DB, cache query metadata
//!   pg-typed-cli check      # Verify all cached queries are still valid

use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "pg-typed-cli", about = "Offline cache management for pg-typed query!() macro")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Scan source files for query!() invocations, connect to DB, and cache metadata.
    Prepare {
        /// Database URL (overrides DATABASE_URL env var).
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
        /// Directory to scan for .rs files (default: current directory).
        #[arg(long, default_value = ".")]
        source_dir: PathBuf,
    },
    /// Verify all cached queries are still valid against the database.
    Check {
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedColumn {
    name: String,
    type_oid: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheEntry {
    sql: String,
    hash: u64,
    param_oids: Vec<u32>,
    columns: Vec<CachedColumn>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Prepare { database_url, source_dir } => {
            prepare(&database_url, &source_dir).await?;
        }
        Command::Check { database_url } => {
            check(&database_url).await?;
        }
    }
    Ok(())
}

/// Scan source files for query!() calls, describe each, write cache.
async fn prepare(
    database_url: &str,
    source_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let (user, password, host, port, database) = parse_pg_uri(database_url)
        .ok_or("Invalid DATABASE_URL")?;
    let addr = format!("{host}:{port}");

    // Find all query!() SQL strings in .rs files.
    let queries = scan_source_files(source_dir)?;
    if queries.is_empty() {
        println!("No query!() invocations found.");
        return Ok(());
    }
    println!("Found {} query!() invocations", queries.len());

    // Connect to PG.
    let mut conn = pg_wire::WireConn::connect(&addr, &user, &password, &database).await?;
    println!("Connected to {database}@{host}:{port}");

    // Create .sqlx directory.
    let cache_dir = find_workspace_root(source_dir)
        .unwrap_or_else(|| source_dir.to_path_buf())
        .join(".sqlx");
    std::fs::create_dir_all(&cache_dir)?;

    let mut cached = 0;
    let mut failed = 0;

    for sql in &queries {
        let hash = hash_sql(sql);
        match conn.describe_statement(sql).await {
            Ok((param_oids, fields)) => {
                let entry = CacheEntry {
                    sql: sql.clone(),
                    hash,
                    param_oids,
                    columns: fields
                        .iter()
                        .map(|f| CachedColumn {
                            name: f.name.clone(),
                            type_oid: f.type_oid,
                        })
                        .collect(),
                };
                let path = cache_dir.join(format!("query-{hash:016x}.json"));
                let json = serde_json::to_string_pretty(&entry)?;
                std::fs::write(&path, json)?;
                cached += 1;
            }
            Err(e) => {
                eprintln!("  FAIL: {sql}");
                eprintln!("        {e}");
                failed += 1;
            }
        }
    }

    println!("Cached {cached} queries, {failed} failed");
    if failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}

/// Check all cached queries against the live database.
async fn check(database_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (user, password, host, port, database) = parse_pg_uri(database_url)
        .ok_or("Invalid DATABASE_URL")?;
    let addr = format!("{host}:{port}");

    let cache_dir = PathBuf::from(".sqlx");
    if !cache_dir.is_dir() {
        println!("No .sqlx cache directory found. Run `pg-typed-cli prepare` first.");
        return Ok(());
    }

    let mut conn = pg_wire::WireConn::connect(&addr, &user, &password, &database).await?;
    println!("Connected to {database}@{host}:{port}");

    let mut ok = 0;
    let mut stale = 0;

    for entry in std::fs::read_dir(&cache_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map(|e| e == "json").unwrap_or(false) {
            let data = std::fs::read_to_string(&path)?;
            let cached: CacheEntry = serde_json::from_str(&data)?;

            match conn.describe_statement(&cached.sql).await {
                Ok((param_oids, fields)) => {
                    let cols: Vec<CachedColumn> = fields
                        .iter()
                        .map(|f| CachedColumn {
                            name: f.name.clone(),
                            type_oid: f.type_oid,
                        })
                        .collect();

                    if param_oids != cached.param_oids || !columns_match(&cols, &cached.columns) {
                        eprintln!("  STALE: {}", cached.sql);
                        stale += 1;
                    } else {
                        ok += 1;
                    }
                }
                Err(e) => {
                    eprintln!("  FAIL: {}", cached.sql);
                    eprintln!("        {e}");
                    stale += 1;
                }
            }
        }
    }

    println!("{ok} queries OK, {stale} stale");
    if stale > 0 {
        println!("Run `pg-typed-cli prepare` to update the cache.");
        std::process::exit(1);
    }
    Ok(())
}

fn columns_match(a: &[CachedColumn], b: &[CachedColumn]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| x.name == y.name && x.type_oid == y.type_oid)
}

/// Scan .rs files for `query!("...")` invocations and extract the SQL strings.
fn scan_source_files(dir: &Path) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut queries = Vec::new();
    scan_dir(dir, &mut queries)?;
    // Deduplicate.
    queries.sort();
    queries.dedup();
    Ok(queries)
}

fn scan_dir(dir: &Path, queries: &mut Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let name = path.file_name().unwrap_or_default().to_str().unwrap_or("");
            // Skip target, .git, etc.
            if name == "target" || name.starts_with('.') {
                continue;
            }
            scan_dir(&path, queries)?;
        } else if path.extension().map(|e| e == "rs").unwrap_or(false) {
            scan_file(&path, queries)?;
        }
    }
    Ok(())
}

/// Extract SQL strings from `query!("SQL" ...)` or `pg_typed::query!("SQL" ...)`.
fn scan_file(path: &Path, queries: &mut Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let source = std::fs::read_to_string(path)?;
    // Find `query!(` then skip whitespace to the opening `"`.
    let mut pos = 0;
    while let Some(idx) = source[pos..].find("query!(") {
        let after_paren = pos + idx + 7; // After `query!(`
        // Skip whitespace/newlines.
        let rest = &source[after_paren..];
        let trimmed = rest.trim_start();
        if !trimmed.starts_with('"') {
            pos = after_paren;
            continue;
        }
        let quote_start = after_paren + (rest.len() - trimmed.len()) + 1; // After the `"`
        if let Some(end) = find_string_end(&source, quote_start) {
            let sql = &source[quote_start..end];
            let sql = sql.replace("\\\"", "\"").replace("\\n", "\n").replace("\\\\", "\\");
            queries.push(sql);
            pos = end + 1;
        } else {
            pos = quote_start;
        }
    }
    Ok(())
}

/// Find the closing `"` of a Rust string literal, handling escapes.
fn find_string_end(source: &str, start: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut i = start;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            i += 2; // Skip escaped char.
        } else if bytes[i] == b'"' {
            return Some(i);
        } else {
            i += 1;
        }
    }
    None
}

fn hash_sql(sql: &str) -> u64 {
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

fn find_workspace_root(start: &Path) -> Option<PathBuf> {
    let mut dir = start.to_path_buf();
    if dir.is_file() {
        dir.pop();
    }
    loop {
        let cargo_toml = dir.join("Cargo.toml");
        if cargo_toml.exists() {
            if let Ok(contents) = std::fs::read_to_string(&cargo_toml) {
                if contents.contains("[workspace]") {
                    return Some(dir);
                }
            }
        }
        if !dir.pop() {
            return None;
        }
    }
}
