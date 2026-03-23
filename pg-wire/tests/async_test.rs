//! Tests for the async writer/reader connection.
//! Requires: docker compose up -d (PostgreSQL on port 54322)

use pg_wire::{AsyncConn, WireConn};

const ADDR: &str = "127.0.0.1:54322";
const USER: &str = "postgres";
const PASS: &str = "postgres";
const DB: &str = "postgrest_test";

async fn connect() -> AsyncConn {
    let conn = WireConn::connect(ADDR, USER, PASS, DB).await.unwrap();
    AsyncConn::new(conn)
}

#[tokio::test]
async fn test_async_simple_query() {
    let ac = connect().await;
    let rows = ac
        .exec_query("SELECT 1 AS n", &[], &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(std::str::from_utf8(rows[0][0].as_ref().unwrap()).unwrap(), "1");
}

#[tokio::test]
async fn test_async_parameterized() {
    let ac = connect().await;
    let rows = ac
        .exec_query(
            "SELECT $1::text AS val",
            &[Some(b"hello" as &[u8])],
            &[0],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        std::str::from_utf8(rows[0][0].as_ref().unwrap()).unwrap(),
        "hello"
    );
}

#[tokio::test]
async fn test_async_multiple_rows() {
    let ac = connect().await;
    let rows = ac
        .exec_query("SELECT id, name FROM api.authors ORDER BY id", &[], &[])
        .await
        .unwrap();
    assert!(rows.len() >= 3);
}

#[tokio::test]
async fn test_async_pipeline_transaction() {
    let ac = connect().await;
    let rows = ac
        .exec_transaction(
            "BEGIN; SET LOCAL ROLE web_anon",
            "SELECT coalesce(json_agg(t), '[]')::text FROM (SELECT id, name FROM api.authors ORDER BY id) t",
            &[],
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let json = std::str::from_utf8(rows[0][0].as_ref().unwrap()).unwrap();
    assert!(json.contains("Alice"));
}

#[tokio::test]
async fn test_async_pipeline_with_jwt() {
    let ac = connect().await;
    let rows = ac
        .exec_transaction(
            "BEGIN; SET LOCAL ROLE test_user; SELECT set_config('request.jwt.claims', '{\"role\":\"test_user\"}', true)",
            "SELECT coalesce(json_agg(t), '[]')::text FROM (SELECT title, status FROM api.articles ORDER BY id) t",
            &[],
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let json = std::str::from_utf8(rows[0][0].as_ref().unwrap()).unwrap();
    assert!(json.contains("Draft"));
}

#[tokio::test]
async fn test_async_statement_cache() {
    let ac = connect().await;
    // First call — cache miss (Parse + Bind + Execute + Sync).
    let r1 = ac.exec_query("SELECT 42 AS n", &[], &[]).await.unwrap();
    // Second call — cache hit (Bind + Execute + Sync only).
    let r2 = ac.exec_query("SELECT 42 AS n", &[], &[]).await.unwrap();
    assert_eq!(r1.len(), 1);
    assert_eq!(r2.len(), 1);
}

#[tokio::test]
async fn test_async_concurrent_queries() {
    let ac = std::sync::Arc::new(connect().await);

    // Fire 10 concurrent queries on the same connection.
    let mut handles = Vec::new();
    for i in 0..10 {
        let ac = ac.clone();
        handles.push(tokio::spawn(async move {
            let sql = format!("SELECT {} AS n", i);
            let rows = ac.exec_query(&sql, &[], &[]).await.unwrap();
            let val = std::str::from_utf8(rows[0][0].as_ref().unwrap())
                .unwrap()
                .parse::<i32>()
                .unwrap();
            assert_eq!(val, i);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}
