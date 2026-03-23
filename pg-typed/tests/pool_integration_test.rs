//! Tests for TypedPool (pg-pool + pg-typed integration).
//! Requires: docker compose up -d (PostgreSQL on port 54322)

use pg_typed::TypedPool;

const ADDR: &str = "127.0.0.1:54322";
const USER: &str = "postgres";
const PASS: &str = "postgres";
const DB: &str = "postgrest_test";

#[tokio::test]
async fn test_typed_pool_connect() {
    let pool = TypedPool::connect(ADDR, USER, PASS, DB, 3).await.unwrap();
    let m = pool.metrics();
    assert_eq!(m.total, 1); // min_idle default = 1
}

#[tokio::test]
async fn test_typed_pool_query() {
    let pool = TypedPool::connect(ADDR, USER, PASS, DB, 3).await.unwrap();
    let client = pool.get().await.unwrap();
    let rows = client.query("SELECT 42::int4 AS n", &[]).await.unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 42);
}

#[tokio::test]
async fn test_typed_pool_parameterized() {
    let pool = TypedPool::connect(ADDR, USER, PASS, DB, 3).await.unwrap();
    let client = pool.get().await.unwrap();
    let rows = client
        .query("SELECT name FROM api.authors WHERE id = $1", &[&1i32])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "Alice");
}

#[tokio::test]
async fn test_typed_pool_multiple_checkouts() {
    let pool = TypedPool::connect(ADDR, USER, PASS, DB, 3).await.unwrap();

    // Multiple sequential checkouts.
    for i in 0..5i32 {
        let client = pool.get().await.unwrap();
        let rows = client
            .query("SELECT $1::int4 AS n", &[&i])
            .await
            .unwrap();
        assert_eq!(rows[0].get::<i32>(0).unwrap(), i);
    }
}

#[tokio::test]
async fn test_typed_pool_transaction() {
    let pool = TypedPool::connect(ADDR, USER, PASS, DB, 3).await.unwrap();
    let client = pool.get().await.unwrap();

    client.simple_query("CREATE TEMP TABLE pool_txn_test (id int)").await.unwrap();
    let txn = client.begin().await.unwrap();
    txn.execute("INSERT INTO pool_txn_test VALUES ($1)", &[&1i32]).await.unwrap();
    txn.execute("INSERT INTO pool_txn_test VALUES ($1)", &[&2i32]).await.unwrap();
    txn.commit().await.unwrap();

    let rows = client.query("SELECT count(*)::int4 FROM pool_txn_test", &[]).await.unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 2);
}

#[tokio::test]
async fn test_typed_pool_from_row() {
    #[derive(pg_typed::FromRow)]
    struct Author {
        id: i32,
        name: String,
    }

    let pool = TypedPool::connect(ADDR, USER, PASS, DB, 3).await.unwrap();
    let client = pool.get().await.unwrap();
    let rows = client
        .query("SELECT id, name FROM api.authors ORDER BY id", &[])
        .await
        .unwrap();

    let authors: Vec<Author> = rows
        .iter()
        .map(|r| pg_typed::FromRow::from_row(r).unwrap())
        .collect();
    assert!(authors.len() >= 3);
    assert_eq!(authors[0].name, "Alice");
}

#[tokio::test]
async fn test_typed_pool_acquire() {
    let pool = TypedPool::connect(ADDR, USER, PASS, DB, 3).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();
    // Use the raw WireConn for a simple query.
    let wire = conn.conn();
    use bytes::BytesMut;
    let mut buf = BytesMut::new();
    pg_wire::protocol::frontend::encode_message(
        &pg_wire::protocol::types::FrontendMsg::Query(b"SELECT 1"),
        &mut buf,
    );
    wire.send_raw(&buf).await.unwrap();
    let (rows, _) = wire.collect_rows().await.unwrap();
    assert_eq!(rows.len(), 1);
    // Connection returned to pool on drop.
    drop(conn);
}

#[tokio::test]
async fn test_typed_pool_drain() {
    let pool = TypedPool::connect(ADDR, USER, PASS, DB, 2).await.unwrap();
    pool.drain().await;
    assert_eq!(pool.metrics().total, 0);
}
