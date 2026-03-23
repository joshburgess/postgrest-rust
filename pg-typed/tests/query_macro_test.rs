//! Tests for the compile-time query!() macro.
//! Requires:
//!   docker compose up -d (PostgreSQL on port 54322)
//!   DATABASE_URL=postgres://postgres:postgres@127.0.0.1:54322/postgrest_test

use pg_typed::Client;

const ADDR: &str = "127.0.0.1:54322";
const USER: &str = "postgres";
const PASS: &str = "postgres";
const DB: &str = "postgrest_test";

async fn connect() -> Client {
    Client::connect(ADDR, USER, PASS, DB).await.unwrap()
}

// ---------------------------------------------------------------------------
// Basic compile-time checked queries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_macro_select_literal() {
    let client = connect().await;
    let rows = pg_typed::query!("SELECT 42::int4 AS answer")
        .fetch_all(&client)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].answer, 42);
}

#[tokio::test]
async fn test_query_macro_with_param() {
    let client = connect().await;
    let id = 1i32;
    let row = pg_typed::query!("SELECT id, name FROM api.authors WHERE id = $1", id)
        .fetch_one(&client)
        .await
        .unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.name, "Alice");
}

#[tokio::test]
async fn test_query_macro_multiple_params() {
    let client = connect().await;
    let a = 3i32;
    let b = 4i32;
    let row = pg_typed::query!("SELECT ($1::int4 + $2::int4) AS sum", a, b)
        .fetch_one(&client)
        .await
        .unwrap();
    assert_eq!(row.sum, 7);
}

#[tokio::test]
async fn test_query_macro_fetch_all() {
    let client = connect().await;
    let rows = pg_typed::query!("SELECT id, name FROM api.authors ORDER BY id")
        .fetch_all(&client)
        .await
        .unwrap();
    assert!(rows.len() >= 3);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].name, "Alice");
}

#[tokio::test]
async fn test_query_macro_fetch_opt_some() {
    let client = connect().await;
    let id = 1i32;
    let row = pg_typed::query!("SELECT name FROM api.authors WHERE id = $1", id)
        .fetch_opt(&client)
        .await
        .unwrap();
    assert!(row.is_some());
    assert_eq!(row.unwrap().name, "Alice");
}

#[tokio::test]
async fn test_query_macro_fetch_opt_none() {
    let client = connect().await;
    let id = 99999i32;
    let row = pg_typed::query!("SELECT name FROM api.authors WHERE id = $1", id)
        .fetch_opt(&client)
        .await
        .unwrap();
    assert!(row.is_none());
}

#[tokio::test]
async fn test_query_macro_multiple_columns() {
    let client = connect().await;
    let row = pg_typed::query!(
        "SELECT 1::int4 AS a, 'hello'::text AS b, true AS c, 3.14::float8 AS d"
    )
    .fetch_one(&client)
    .await
    .unwrap();
    assert_eq!(row.a, 1);
    assert_eq!(row.b, "hello");
    assert!(row.c);
    assert!((row.d - 3.14).abs() < 1e-10);
}

#[tokio::test]
async fn test_query_macro_text_type() {
    let client = connect().await;
    let name = "Alice".to_string();
    let rows = pg_typed::query!("SELECT id FROM api.authors WHERE name = $1", name)
        .fetch_all(&client)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 1);
}

#[tokio::test]
async fn test_query_macro_bigint() {
    let client = connect().await;
    let val = 9999999999i64;
    let row = pg_typed::query!("SELECT $1::int8 AS n", val)
        .fetch_one(&client)
        .await
        .unwrap();
    assert_eq!(row.n, 9999999999i64);
}
