//! Integration tests for pg-typed.
//! Requires: docker compose up -d (PostgreSQL on port 54322)

use pg_typed::{Client, Decode, Encode, FromRow, SqlParam};

const ADDR: &str = "127.0.0.1:54322";
const USER: &str = "postgres";
const PASS: &str = "postgres";
const DB: &str = "postgrest_test";

async fn connect() -> Client {
    Client::connect(ADDR, USER, PASS, DB).await.unwrap()
}

// ---------------------------------------------------------------------------
// Binary encode/decode unit tests
// ---------------------------------------------------------------------------

#[test]
fn test_encode_decode_bool() {
    let mut buf = bytes::BytesMut::new();
    true.encode(&mut buf);
    assert_eq!(buf.as_ref(), &[1]);
    assert_eq!(bool::decode(&buf).unwrap(), true);

    buf.clear();
    false.encode(&mut buf);
    assert_eq!(buf.as_ref(), &[0]);
    assert_eq!(bool::decode(&buf).unwrap(), false);
}

#[test]
fn test_encode_decode_i16() {
    let mut buf = bytes::BytesMut::new();
    42i16.encode(&mut buf);
    assert_eq!(buf.as_ref(), &[0, 42]);
    assert_eq!(i16::decode(&buf).unwrap(), 42);

    buf.clear();
    (-1i16).encode(&mut buf);
    assert_eq!(i16::decode(&buf).unwrap(), -1);
}

#[test]
fn test_encode_decode_i32() {
    let mut buf = bytes::BytesMut::new();
    12345i32.encode(&mut buf);
    assert_eq!(i32::decode(&buf).unwrap(), 12345);

    buf.clear();
    i32::MIN.encode(&mut buf);
    assert_eq!(i32::decode(&buf).unwrap(), i32::MIN);

    buf.clear();
    i32::MAX.encode(&mut buf);
    assert_eq!(i32::decode(&buf).unwrap(), i32::MAX);
}

#[test]
fn test_encode_decode_i64() {
    let mut buf = bytes::BytesMut::new();
    123456789012345i64.encode(&mut buf);
    assert_eq!(i64::decode(&buf).unwrap(), 123456789012345);
}

#[test]
fn test_encode_decode_f32() {
    let mut buf = bytes::BytesMut::new();
    3.14f32.encode(&mut buf);
    let decoded = f32::decode(&buf).unwrap();
    assert!((decoded - 3.14).abs() < 1e-6);
}

#[test]
fn test_encode_decode_f64() {
    let mut buf = bytes::BytesMut::new();
    std::f64::consts::PI.encode(&mut buf);
    let decoded = f64::decode(&buf).unwrap();
    assert!((decoded - std::f64::consts::PI).abs() < 1e-15);
}

#[test]
fn test_encode_decode_string() {
    let mut buf = bytes::BytesMut::new();
    "hello world".encode(&mut buf);
    assert_eq!(String::decode(&buf).unwrap(), "hello world");
}

#[test]
fn test_encode_decode_bytes() {
    let mut buf = bytes::BytesMut::new();
    let data = vec![0u8, 1, 2, 255, 128];
    data.encode(&mut buf);
    assert_eq!(Vec::<u8>::decode(&buf).unwrap(), data);
}

#[test]
fn test_decode_wrong_size() {
    assert!(i32::decode(&[0, 0]).is_err());
    assert!(i64::decode(&[0, 0, 0, 0]).is_err());
    assert!(bool::decode(&[]).is_err());
    assert!(f32::decode(&[0]).is_err());
}

// ---------------------------------------------------------------------------
// Integration tests: binary-format queries against real PostgreSQL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_select_int() {
    let client = connect().await;
    let rows = client.query("SELECT $1::int4 AS n", &[&42i32]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let n: i32 = rows[0].get(0).unwrap();
    assert_eq!(n, 42);
}

#[tokio::test]
async fn test_query_select_text() {
    let client = connect().await;
    let rows = client
        .query("SELECT $1::text AS val", &[&"hello"])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let val: String = rows[0].get(0).unwrap();
    assert_eq!(val, "hello");
}

#[tokio::test]
async fn test_query_select_bool() {
    let client = connect().await;
    let rows = client
        .query("SELECT $1::bool AS flag", &[&true])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let flag: bool = rows[0].get(0).unwrap();
    assert!(flag);
}

#[tokio::test]
async fn test_query_select_float8() {
    let client = connect().await;
    let rows = client
        .query("SELECT $1::float8 AS val", &[&3.14f64])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let val: f64 = rows[0].get(0).unwrap();
    assert!((val - 3.14).abs() < 1e-10);
}

#[tokio::test]
async fn test_query_select_bigint() {
    let client = connect().await;
    let rows = client
        .query("SELECT $1::int8 AS val", &[&9999999999i64])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let val: i64 = rows[0].get(0).unwrap();
    assert_eq!(val, 9999999999);
}

#[tokio::test]
async fn test_query_multiple_columns() {
    let client = connect().await;
    let rows = client
        .query(
            "SELECT $1::int4 AS a, $2::text AS b, $3::bool AS c",
            &[&10i32, &"foo", &false],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let a: i32 = rows[0].get(0).unwrap();
    let b: String = rows[0].get(1).unwrap();
    let c: bool = rows[0].get(2).unwrap();
    assert_eq!(a, 10);
    assert_eq!(b, "foo");
    assert!(!c);
}

#[tokio::test]
async fn test_query_multiple_rows() {
    let client = connect().await;
    let rows = client
        .query("SELECT generate_series(1, 5)::int4 AS n", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 5);
    for (i, row) in rows.iter().enumerate() {
        let n: i32 = row.get(0).unwrap();
        assert_eq!(n, (i + 1) as i32);
    }
}

#[tokio::test]
async fn test_query_null() {
    let client = connect().await;
    let rows = client
        .query("SELECT NULL::int4 AS n", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let n: Option<i32> = rows[0].get_opt(0).unwrap();
    assert!(n.is_none());
}

#[tokio::test]
async fn test_query_one() {
    let client = connect().await;
    let row = client.query_one("SELECT 42::int4 AS n", &[]).await.unwrap();
    let n: i32 = row.get(0).unwrap();
    assert_eq!(n, 42);
}

#[tokio::test]
async fn test_query_one_not_found() {
    let client = connect().await;
    let result = client
        .query_one("SELECT 1 WHERE false", &[])
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_query_opt_some() {
    let client = connect().await;
    let row = client.query_opt("SELECT 42::int4 AS n", &[]).await.unwrap();
    assert!(row.is_some());
    let n: i32 = row.unwrap().get(0).unwrap();
    assert_eq!(n, 42);
}

#[tokio::test]
async fn test_query_opt_none() {
    let client = connect().await;
    let row = client.query_opt("SELECT 1 WHERE false", &[]).await.unwrap();
    assert!(row.is_none());
}

#[tokio::test]
async fn test_query_no_params() {
    let client = connect().await;
    let rows = client.query("SELECT 1::int4 AS n", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let n: i32 = rows[0].get(0).unwrap();
    assert_eq!(n, 1);
}

#[tokio::test]
async fn test_query_real_table() {
    let client = connect().await;
    let rows = client
        .query("SELECT id, name FROM api.authors ORDER BY id", &[])
        .await
        .unwrap();
    assert!(rows.len() >= 3);
    // Columns come back as binary ints and text.
    let id: i32 = rows[0].get(0).unwrap();
    assert_eq!(id, 1);
    let name: String = rows[0].get(1).unwrap();
    assert_eq!(name, "Alice");
}

#[tokio::test]
async fn test_query_with_filter() {
    let client = connect().await;
    let rows = client
        .query(
            "SELECT name FROM api.authors WHERE id = $1",
            &[&1i32],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let name: String = rows[0].get(0).unwrap();
    assert_eq!(name, "Alice");
}

#[tokio::test]
async fn test_statement_cache() {
    let client = connect().await;
    // First call: Parse + Bind + Execute + Sync
    let r1 = client.query("SELECT $1::int4 AS n", &[&1i32]).await.unwrap();
    // Second call: cache hit — Bind + Execute + Sync (no Parse)
    let r2 = client.query("SELECT $1::int4 AS n", &[&2i32]).await.unwrap();
    assert_eq!(r1[0].get::<i32>(0).unwrap(), 1);
    assert_eq!(r2[0].get::<i32>(0).unwrap(), 2);
}

#[tokio::test]
async fn test_error_recovery() {
    let client = connect().await;
    let result = client.query("SELECT * FROM nonexistent_xyz_table", &[]).await;
    assert!(result.is_err());
    // Connection should still work.
    let rows = client.query("SELECT 1::int4 AS n", &[]).await.unwrap();
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);
}

#[tokio::test]
async fn test_sequential_typed_queries() {
    let client = connect().await;
    for i in 0..10i32 {
        let rows = client.query("SELECT $1::int4 AS n", &[&i]).await.unwrap();
        let n: i32 = rows[0].get(0).unwrap();
        assert_eq!(n, i);
    }
}

// ---------------------------------------------------------------------------
// Column names (RowDescription)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_column_names() {
    let client = connect().await;
    let rows = client
        .query("SELECT 1::int4 AS id, 'hello'::text AS name", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].column_name(0), Some("id"));
    assert_eq!(rows[0].column_name(1), Some("name"));
}

#[tokio::test]
async fn test_get_by_name() {
    let client = connect().await;
    let rows = client
        .query("SELECT 42::int4 AS answer, 'hello'::text AS greeting", &[])
        .await
        .unwrap();
    let answer: i32 = rows[0].get_by_name("answer").unwrap();
    let greeting: String = rows[0].get_by_name("greeting").unwrap();
    assert_eq!(answer, 42);
    assert_eq!(greeting, "hello");
}

#[tokio::test]
async fn test_get_by_name_not_found() {
    let client = connect().await;
    let rows = client.query("SELECT 1::int4 AS n", &[]).await.unwrap();
    let result = rows[0].get_by_name::<i32>("nonexistent");
    assert!(result.is_err());
}

#[tokio::test]
async fn test_column_type_oids() {
    let client = connect().await;
    let rows = client
        .query("SELECT 1::int4 AS n, 'hi'::text AS s", &[])
        .await
        .unwrap();
    // int4 OID = 23, text OID = 25
    assert_eq!(rows[0].column_type_oid(0), Some(23));
    assert_eq!(rows[0].column_type_oid(1), Some(25));
}

// ---------------------------------------------------------------------------
// Execute (INSERT/UPDATE/DELETE with row count)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_execute_insert_and_delete() {
    let client = connect().await;

    // Create a temp table.
    client.simple_query("CREATE TEMP TABLE test_exec (id int)").await.unwrap();

    // Insert rows.
    let count = client
        .execute("INSERT INTO test_exec VALUES ($1)", &[&1i32])
        .await
        .unwrap();
    assert_eq!(count, 1);

    let count = client
        .execute("INSERT INTO test_exec VALUES ($1), ($2)", &[&2i32, &3i32])
        .await
        .unwrap();
    assert_eq!(count, 2);

    // Delete.
    let count = client
        .execute("DELETE FROM test_exec WHERE id > $1", &[&1i32])
        .await
        .unwrap();
    assert_eq!(count, 2);

    // Verify.
    let rows = client.query("SELECT id FROM test_exec", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);
}

#[tokio::test]
async fn test_execute_update() {
    let client = connect().await;
    client.simple_query("CREATE TEMP TABLE test_upd (id int, val text)").await.unwrap();
    client.execute("INSERT INTO test_upd VALUES ($1, $2)", &[&1i32, &"old"]).await.unwrap();
    client.execute("INSERT INTO test_upd VALUES ($1, $2)", &[&2i32, &"old"]).await.unwrap();

    let count = client
        .execute("UPDATE test_upd SET val = $1 WHERE id = $2", &[&"new", &1i32])
        .await
        .unwrap();
    assert_eq!(count, 1);

    let rows = client
        .query("SELECT val FROM test_upd WHERE id = $1", &[&1i32])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String>(0).unwrap(), "new");
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_transaction_commit() {
    let client = connect().await;
    client.simple_query("CREATE TEMP TABLE test_txn (id int)").await.unwrap();

    let txn = client.begin().await.unwrap();
    txn.execute("INSERT INTO test_txn VALUES ($1)", &[&1i32]).await.unwrap();
    txn.execute("INSERT INTO test_txn VALUES ($1)", &[&2i32]).await.unwrap();
    txn.commit().await.unwrap();

    let rows = client.query("SELECT id FROM test_txn ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<i32>(0).unwrap(), 1);
    assert_eq!(rows[1].get::<i32>(0).unwrap(), 2);
}

#[tokio::test]
async fn test_transaction_rollback() {
    let client = connect().await;
    client.simple_query("CREATE TEMP TABLE test_txn_rb (id int)").await.unwrap();

    let txn = client.begin().await.unwrap();
    txn.execute("INSERT INTO test_txn_rb VALUES ($1)", &[&1i32]).await.unwrap();
    txn.rollback().await.unwrap();

    // Table should be empty — insert was rolled back.
    let rows = client.query("SELECT id FROM test_txn_rb", &[]).await.unwrap();
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn test_transaction_query_inside() {
    let client = connect().await;
    client.simple_query("CREATE TEMP TABLE test_txn_q (id int)").await.unwrap();

    let txn = client.begin().await.unwrap();
    txn.execute(
        "INSERT INTO test_txn_q VALUES ($1), ($2), ($3)",
        &[&10i32, &20i32, &30i32],
    ).await.unwrap();
    let rows = txn.query("SELECT sum(id)::int4 FROM test_txn_q", &[]).await.unwrap();
    let sum: i32 = rows[0].get(0).unwrap();
    assert_eq!(sum, 60);
    txn.commit().await.unwrap();
}

// ---------------------------------------------------------------------------
// Simple query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_simple_query_ddl() {
    let client = connect().await;
    client.simple_query("CREATE TEMP TABLE test_simple (id int)").await.unwrap();
    client.simple_query("DROP TABLE test_simple").await.unwrap();
}

// ---------------------------------------------------------------------------
// FromRow trait (manual impl)
// ---------------------------------------------------------------------------

struct Author {
    id: i32,
    name: String,
}

impl pg_typed::FromRow for Author {
    fn from_row(row: &pg_typed::Row) -> Result<Self, pg_typed::TypedError> {
        Ok(Author {
            id: row.get_by_name("id")?,
            name: row.get_by_name("name")?,
        })
    }
}

#[tokio::test]
async fn test_from_row_manual() {
    let client = connect().await;
    let rows = client
        .query("SELECT id, name FROM api.authors WHERE id = $1", &[&1i32])
        .await
        .unwrap();
    let author = Author::from_row(&rows[0]).unwrap();
    assert_eq!(author.id, 1);
    assert_eq!(author.name, "Alice");
}

#[tokio::test]
async fn test_from_row_multiple() {
    let client = connect().await;
    let rows = client
        .query("SELECT id, name FROM api.authors ORDER BY id", &[])
        .await
        .unwrap();
    let authors: Vec<Author> = rows.iter().map(|r| Author::from_row(r).unwrap()).collect();
    assert!(authors.len() >= 3);
    assert_eq!(authors[0].name, "Alice");
    assert_eq!(authors[1].name, "Bob");
}

// ---------------------------------------------------------------------------
// FromRow derive macro
// ---------------------------------------------------------------------------

#[derive(pg_typed::FromRow)]
struct DerivedAuthor {
    id: i32,
    name: String,
}

#[tokio::test]
async fn test_derive_from_row_basic() {
    let client = connect().await;
    let rows = client
        .query("SELECT id, name FROM api.authors WHERE id = $1", &[&1i32])
        .await
        .unwrap();
    let author = DerivedAuthor::from_row(&rows[0]).unwrap();
    assert_eq!(author.id, 1);
    assert_eq!(author.name, "Alice");
}

#[derive(pg_typed::FromRow)]
struct DerivedAuthorWithBio {
    id: i32,
    name: String,
    bio: Option<String>,
}

#[tokio::test]
async fn test_derive_from_row_optional() {
    let client = connect().await;
    let rows = client
        .query("SELECT id, name, bio FROM api.authors WHERE id = $1", &[&1i32])
        .await
        .unwrap();
    let author = DerivedAuthorWithBio::from_row(&rows[0]).unwrap();
    assert_eq!(author.id, 1);
    assert_eq!(author.name, "Alice");
    assert!(author.bio.is_some());
}

#[derive(pg_typed::FromRow)]
struct RenamedFields {
    #[from_row(rename = "id")]
    author_id: i32,
    #[from_row(rename = "name")]
    author_name: String,
}

#[tokio::test]
async fn test_derive_from_row_rename() {
    let client = connect().await;
    let rows = client
        .query("SELECT id, name FROM api.authors WHERE id = $1", &[&1i32])
        .await
        .unwrap();
    let r = RenamedFields::from_row(&rows[0]).unwrap();
    assert_eq!(r.author_id, 1);
    assert_eq!(r.author_name, "Alice");
}

#[tokio::test]
async fn test_derive_from_row_multiple() {
    let client = connect().await;
    let rows = client
        .query("SELECT id, name FROM api.authors ORDER BY id", &[])
        .await
        .unwrap();
    let authors: Vec<DerivedAuthor> = rows
        .iter()
        .map(|r| DerivedAuthor::from_row(r).unwrap())
        .collect();
    assert!(authors.len() >= 3);
    assert_eq!(authors[0].name, "Alice");
    assert_eq!(authors[1].name, "Bob");
}

// ---------------------------------------------------------------------------
// Nullable parameters
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_null_param() {
    let client = connect().await;
    let val: Option<i32> = None;
    let rows = client.query("SELECT $1::int4 IS NULL AS is_null", &[&val]).await.unwrap();
    let is_null: bool = rows[0].get(0).unwrap();
    assert!(is_null);
}

#[tokio::test]
async fn test_some_param() {
    let client = connect().await;
    let val: Option<i32> = Some(42);
    let rows = client.query("SELECT $1::int4 AS n", &[&val]).await.unwrap();
    let n: i32 = rows[0].get(0).unwrap();
    assert_eq!(n, 42);
}

#[tokio::test]
async fn test_null_text_param() {
    let client = connect().await;
    let val: Option<String> = None;
    let rows = client.query("SELECT $1::text IS NULL AS is_null", &[&val]).await.unwrap();
    let is_null: bool = rows[0].get(0).unwrap();
    assert!(is_null);
}

// ---------------------------------------------------------------------------
// Chrono types
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_timestamp() {
    let client = connect().await;
    let now = chrono::Utc::now().naive_utc();
    let rows = client
        .query("SELECT $1::timestamp AS ts", &[&now])
        .await
        .unwrap();
    let ts: chrono::NaiveDateTime = rows[0].get(0).unwrap();
    // Within 1 second (microsecond precision).
    let diff = (ts - now).num_milliseconds().abs();
    assert!(diff < 1000, "timestamp diff {diff}ms should be < 1s");
}

#[tokio::test]
async fn test_timestamptz() {
    let client = connect().await;
    let now = chrono::Utc::now();
    let rows = client
        .query("SELECT $1::timestamptz AS ts", &[&now])
        .await
        .unwrap();
    let ts: chrono::DateTime<chrono::Utc> = rows[0].get(0).unwrap();
    let diff = (ts - now).num_milliseconds().abs();
    assert!(diff < 1000, "timestamp diff {diff}ms should be < 1s");
}

#[tokio::test]
async fn test_date() {
    let client = connect().await;
    let d = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let rows = client.query("SELECT $1::date AS d", &[&d]).await.unwrap();
    let result: chrono::NaiveDate = rows[0].get(0).unwrap();
    assert_eq!(result, d);
}

#[tokio::test]
async fn test_time() {
    let client = connect().await;
    let t = chrono::NaiveTime::from_hms_opt(14, 30, 0).unwrap();
    let rows = client.query("SELECT $1::time AS t", &[&t]).await.unwrap();
    let result: chrono::NaiveTime = rows[0].get(0).unwrap();
    assert_eq!(result, t);
}

// ---------------------------------------------------------------------------
// JSON types
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_jsonb() {
    let client = connect().await;
    let val = serde_json::json!({"key": "value", "num": 42});
    let rows = client
        .query("SELECT $1::jsonb AS j", &[&val])
        .await
        .unwrap();
    let result: serde_json::Value = rows[0].get(0).unwrap();
    assert_eq!(result["key"], "value");
    assert_eq!(result["num"], 42);
}

#[tokio::test]
async fn test_jsonb_array() {
    let client = connect().await;
    let val = serde_json::json!([1, 2, 3]);
    let rows = client
        .query("SELECT $1::jsonb AS j", &[&val])
        .await
        .unwrap();
    let result: serde_json::Value = rows[0].get(0).unwrap();
    assert_eq!(result, serde_json::json!([1, 2, 3]));
}

// ---------------------------------------------------------------------------
// UUID
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_uuid() {
    let client = connect().await;
    let id = uuid::Uuid::new_v4();
    let rows = client
        .query("SELECT $1::uuid AS id", &[&id])
        .await
        .unwrap();
    let result: uuid::Uuid = rows[0].get(0).unwrap();
    assert_eq!(result, id);
}

// ---------------------------------------------------------------------------
// Encode/Decode roundtrip unit tests for new types
// ---------------------------------------------------------------------------

#[test]
fn test_encode_decode_uuid() {
    let id = uuid::Uuid::new_v4();
    let mut buf = bytes::BytesMut::new();
    id.encode(&mut buf);
    assert_eq!(buf.len(), 16);
    assert_eq!(uuid::Uuid::decode(&buf).unwrap(), id);
}

#[test]
fn test_encode_decode_naive_date() {
    let d = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let mut buf = bytes::BytesMut::new();
    d.encode(&mut buf);
    assert_eq!(buf.len(), 4);
    assert_eq!(chrono::NaiveDate::decode(&buf).unwrap(), d);
}

#[test]
fn test_encode_decode_naive_time() {
    let t = chrono::NaiveTime::from_hms_micro_opt(14, 30, 45, 123456).unwrap();
    let mut buf = bytes::BytesMut::new();
    t.encode(&mut buf);
    assert_eq!(buf.len(), 8);
    assert_eq!(chrono::NaiveTime::decode(&buf).unwrap(), t);
}

#[test]
fn test_encode_decode_timestamp() {
    let dt = chrono::NaiveDate::from_ymd_opt(2024, 6, 15)
        .unwrap()
        .and_hms_opt(14, 30, 0)
        .unwrap();
    let mut buf = bytes::BytesMut::new();
    dt.encode(&mut buf);
    assert_eq!(buf.len(), 8);
    assert_eq!(chrono::NaiveDateTime::decode(&buf).unwrap(), dt);
}

#[test]
fn test_encode_decode_jsonb() {
    let val = serde_json::json!({"hello": "world"});
    let mut buf = bytes::BytesMut::new();
    val.encode(&mut buf);
    // First byte is JSONB version (1).
    assert_eq!(buf[0], 1);
    let decoded: serde_json::Value = serde_json::Value::decode(&buf).unwrap();
    assert_eq!(decoded, val);
}

// ---------------------------------------------------------------------------
// Array types
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_int_array() {
    let client = connect().await;
    let arr = vec![1i32, 2, 3, 4, 5];
    let rows = client
        .query("SELECT $1::int4[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<i32> = rows[0].get(0).unwrap();
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_bigint_array() {
    let client = connect().await;
    let arr = vec![100i64, 200, 300];
    let rows = client
        .query("SELECT $1::int8[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<i64> = rows[0].get(0).unwrap();
    assert_eq!(result, vec![100, 200, 300]);
}

#[tokio::test]
async fn test_text_array() {
    let client = connect().await;
    let arr = vec!["hello".to_string(), "world".to_string()];
    let rows = client
        .query("SELECT $1::text[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<String> = rows[0].get(0).unwrap();
    assert_eq!(result, vec!["hello", "world"]);
}

#[tokio::test]
async fn test_empty_array() {
    let client = connect().await;
    let arr: Vec<i32> = vec![];
    let rows = client
        .query("SELECT $1::int4[] AS arr", &[&arr])
        .await
        .unwrap();
    let result: Vec<i32> = rows[0].get(0).unwrap();
    assert!(result.is_empty());
}

// ---------------------------------------------------------------------------
// Numeric / Inet newtypes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_numeric_roundtrip() {
    let client = connect().await;
    // PG returns numeric in binary format. We decode it to PgNumeric (string).
    let rows = client
        .query("SELECT 123.456::numeric AS n", &[])
        .await
        .unwrap();
    let n: pg_typed::PgNumeric = rows[0].get(0).unwrap();
    assert_eq!(n.0, "123.456");
}

#[tokio::test]
async fn test_numeric_zero() {
    let client = connect().await;
    let rows = client
        .query("SELECT 0::numeric AS n", &[])
        .await
        .unwrap();
    let n: pg_typed::PgNumeric = rows[0].get(0).unwrap();
    assert_eq!(n.0, "0");
}

#[tokio::test]
async fn test_numeric_negative() {
    let client = connect().await;
    let rows = client
        .query("SELECT (-99.99)::numeric AS n", &[])
        .await
        .unwrap();
    let n: pg_typed::PgNumeric = rows[0].get(0).unwrap();
    assert_eq!(n.0, "-99.99");
}

#[tokio::test]
async fn test_inet_roundtrip() {
    let client = connect().await;
    let rows = client
        .query("SELECT '192.168.1.1/24'::inet AS addr", &[])
        .await
        .unwrap();
    let addr: pg_typed::PgInet = rows[0].get(0).unwrap();
    assert_eq!(addr.0, "192.168.1.1/24");
}

// ---------------------------------------------------------------------------
// Encode/Decode unit tests for arrays
// ---------------------------------------------------------------------------

#[test]
fn test_encode_decode_int_array() {
    let arr = vec![10i32, 20, 30];
    let mut buf = bytes::BytesMut::new();
    arr.encode(&mut buf);
    let decoded: Vec<i32> = Vec::<i32>::decode(&buf).unwrap();
    assert_eq!(decoded, arr);
}

#[test]
fn test_encode_decode_text_array() {
    let arr = vec!["foo".to_string(), "bar".to_string()];
    let mut buf = bytes::BytesMut::new();
    arr.encode(&mut buf);
    let decoded: Vec<String> = Vec::<String>::decode(&buf).unwrap();
    assert_eq!(decoded, arr);
}
