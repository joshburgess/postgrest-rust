//! PostgREST compatibility test runner.
//!
//! Sends identical HTTP requests to both PostgREST and pg-rest-server,
//! then compares status codes and JSON response bodies.
//!
//! Usage:
//!   # Start both servers:
//!   cd test/compat && docker compose up -d
//!   cargo run -p pg-rest-server --release -- --config test/fixtures/test-config.toml &
//!
//!   # Run compatibility tests:
//!   cargo run -p compat-test -- \
//!     --postgrest http://localhost:3100 \
//!     --ours http://localhost:3001

use clap::Parser;
use serde_json::Value;

#[derive(Parser)]
struct Args {
    /// PostgREST base URL
    #[arg(long, default_value = "http://localhost:3100")]
    postgrest: String,

    /// pg-rest-server base URL
    #[arg(long, default_value = "http://localhost:3001")]
    ours: String,

    /// JWT secret (must match both servers)
    #[arg(long, default_value = "reallyreallyreallyreallyverysafe")]
    jwt_secret: String,
}

struct TestCase {
    name: &'static str,
    method: &'static str,
    path: &'static str,
    body: Option<Value>,
    headers: Vec<(&'static str, String)>,
    /// If true, compare response bodies. If false, only compare status codes.
    compare_body: bool,
    /// If true, sort the JSON array before comparison (for order-independent checks).
    sort_array: bool,
}

struct TestResult {
    name: &'static str,
    passed: bool,
    detail: String,
}

fn make_jwt(secret: &str, role: &str) -> String {
    let claims = serde_json::json!({ "role": role });
    jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap()
}

fn test_cases(jwt_anon: &str, jwt_user: &str) -> Vec<TestCase> {
    let auth_anon = format!("Bearer {jwt_anon}");
    let auth_user = format!("Bearer {jwt_user}");

    vec![
        // ---- Basic reads ----
        TestCase {
            name: "GET /authors",
            method: "GET",
            path: "/authors?order=id.asc",
            body: None,
            headers: vec![("Authorization", auth_anon.clone())],
            compare_body: true,
            sort_array: false,
        },
        TestCase {
            name: "GET /books",
            method: "GET",
            path: "/books?order=id.asc",
            body: None,
            headers: vec![("Authorization", auth_anon.clone())],
            compare_body: true,
            sort_array: false,
        },
        // ---- Select columns ----
        TestCase {
            name: "GET /authors?select=name",
            method: "GET",
            path: "/authors?select=name&order=id.asc",
            body: None,
            headers: vec![("Authorization", auth_anon.clone())],
            compare_body: true,
            sort_array: false,
        },
        // ---- Filters ----
        TestCase {
            name: "GET /authors?name=eq.Alice",
            method: "GET",
            path: "/authors?name=eq.Alice",
            body: None,
            headers: vec![("Authorization", auth_anon.clone())],
            compare_body: true,
            sort_array: false,
        },
        TestCase {
            name: "GET /books?pages=gt.400",
            method: "GET",
            path: "/books?pages=gt.400&order=id.asc",
            body: None,
            headers: vec![("Authorization", auth_anon.clone())],
            compare_body: true,
            sort_array: false,
        },
        TestCase {
            name: "GET /authors?id=in.(1,2)",
            method: "GET",
            path: "/authors?id=in.(1,2)&order=id.asc",
            body: None,
            headers: vec![("Authorization", auth_anon.clone())],
            compare_body: true,
            sort_array: false,
        },
        TestCase {
            name: "GET /authors?bio=is.null",
            method: "GET",
            path: "/authors?bio=is.null&order=id.asc",
            body: None,
            headers: vec![("Authorization", auth_anon.clone())],
            compare_body: true,
            sort_array: false,
        },
        TestCase {
            name: "GET /authors?bio=not.is.null",
            method: "GET",
            path: "/authors?bio=not.is.null&order=id.asc",
            body: None,
            headers: vec![("Authorization", auth_anon.clone())],
            compare_body: true,
            sort_array: false,
        },
        // ---- Ordering ----
        TestCase {
            name: "GET /authors?order=name.desc",
            method: "GET",
            path: "/authors?order=name.desc&id=in.(1,2,3)",
            body: None,
            headers: vec![("Authorization", auth_anon.clone())],
            compare_body: true,
            sort_array: false,
        },
        // ---- Limit / Offset ----
        TestCase {
            name: "GET /authors?limit=1&offset=1",
            method: "GET",
            path: "/authors?limit=1&offset=1&order=id.asc",
            body: None,
            headers: vec![("Authorization", auth_anon.clone())],
            compare_body: true,
            sort_array: false,
        },
        // ---- Count ----
        TestCase {
            name: "GET /authors (count=exact)",
            method: "GET",
            path: "/authors?order=id.asc&id=in.(1,2,3)",
            body: None,
            headers: vec![
                ("Authorization", auth_anon.clone()),
                ("Prefer", "count=exact".into()),
            ],
            compare_body: true,
            sort_array: false,
        },
        // ---- Embedding ----
        TestCase {
            name: "GET /authors?select=name,books(title) (O2M)",
            method: "GET",
            path: "/authors?select=name,books(title)&name=eq.Alice",
            body: None,
            headers: vec![("Authorization", auth_anon.clone())],
            compare_body: true,
            sort_array: false,
        },
        TestCase {
            name: "GET /books?select=title,authors(name) (M2O)",
            method: "GET",
            path: "/books?select=title,authors(name)&id=eq.1",
            body: None,
            headers: vec![("Authorization", auth_anon.clone())],
            compare_body: true,
            sort_array: false,
        },
        // ---- RPC ----
        TestCase {
            name: "POST /rpc/add",
            method: "POST",
            path: "/rpc/add",
            body: Some(serde_json::json!({"a": 3, "b": 4})),
            headers: vec![("Authorization", auth_anon.clone())],
            compare_body: true,
            sort_array: false,
        },
        TestCase {
            name: "POST /rpc/search_books",
            method: "POST",
            path: "/rpc/search_books",
            body: Some(serde_json::json!({"query": "Rust"})),
            headers: vec![("Authorization", auth_anon.clone())],
            compare_body: true,
            sort_array: true, // order may differ
        },
        TestCase {
            name: "POST /rpc/greet (default param)",
            method: "POST",
            path: "/rpc/greet",
            body: Some(serde_json::json!({})),
            headers: vec![("Authorization", auth_anon.clone())],
            compare_body: true,
            sort_array: false,
        },
        // ---- RLS ----
        TestCase {
            name: "GET /articles (anon, RLS filters to published)",
            method: "GET",
            path: "/articles?order=id.asc",
            body: None,
            headers: vec![("Authorization", auth_anon.clone())],
            compare_body: true,
            sort_array: false,
        },
        TestCase {
            name: "GET /articles (test_user, sees all)",
            method: "GET",
            path: "/articles?order=id.asc",
            body: None,
            headers: vec![("Authorization", auth_user.clone())],
            compare_body: true,
            sort_array: false,
        },
        // ---- Or filter ----
        TestCase {
            name: "GET /authors?or=(name.eq.Alice,name.eq.Carol)",
            method: "GET",
            path: "/authors?or=(name.eq.Alice,name.eq.Carol)&order=id.asc",
            body: None,
            headers: vec![("Authorization", auth_anon.clone())],
            compare_body: true,
            sort_array: false,
        },
        // ---- Singular response ----
        TestCase {
            name: "GET /authors?id=eq.1 (singular)",
            method: "GET",
            path: "/authors?id=eq.1",
            body: None,
            headers: vec![
                ("Authorization", auth_anon.clone()),
                ("Accept", "application/vnd.pgrst.object+json".into()),
            ],
            compare_body: true,
            sort_array: false,
        },
        // ---- Nonexistent table ----
        TestCase {
            name: "GET /nonexistent (404)",
            method: "GET",
            path: "/nonexistent",
            body: None,
            headers: vec![("Authorization", auth_anon.clone())],
            compare_body: false,
            sort_array: false,
        },
        // ---- OpenAPI ----
        TestCase {
            name: "GET / (OpenAPI spec)",
            method: "GET",
            path: "/",
            body: None,
            headers: vec![],
            compare_body: false, // structure differs between implementations
            sort_array: false,
        },
    ]
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let client = reqwest::Client::new();

    let jwt_anon = make_jwt(&args.jwt_secret, "web_anon");
    let jwt_user = make_jwt(&args.jwt_secret, "test_user");

    let cases = test_cases(&jwt_anon, &jwt_user);
    let total = cases.len();
    let mut results: Vec<TestResult> = Vec::new();

    println!("Running {} compatibility tests...\n", total);
    println!(
        "  PostgREST: {}\n  pg-rest:    {}\n",
        args.postgrest, args.ours
    );

    for tc in &cases {
        let result = run_test(&client, &args.postgrest, &args.ours, tc).await;
        let icon = if result.passed { "✓" } else { "✗" };
        println!("  {icon} {}: {}", result.name, result.detail);
        results.push(result);
    }

    let passed = results.iter().filter(|r| r.passed).count();
    let failed = results.iter().filter(|r| !r.passed).count();

    println!("\n{passed}/{total} passed, {failed} failed");

    if failed > 0 {
        println!("\nFailed tests:");
        for r in &results {
            if !r.passed {
                println!("  ✗ {}: {}", r.name, r.detail);
            }
        }
        std::process::exit(1);
    }
}

async fn run_test(
    client: &reqwest::Client,
    postgrest_url: &str,
    ours_url: &str,
    tc: &TestCase,
) -> TestResult {
    let pg_resp = send_request(client, postgrest_url, tc).await;
    let our_resp = send_request(client, ours_url, tc).await;

    let (pg_status, pg_body) = match pg_resp {
        Ok(r) => r,
        Err(e) => {
            return TestResult {
                name: tc.name,
                passed: false,
                detail: format!("PostgREST request failed: {e}"),
            }
        }
    };
    let (our_status, our_body) = match our_resp {
        Ok(r) => r,
        Err(e) => {
            return TestResult {
                name: tc.name,
                passed: false,
                detail: format!("pg-rest request failed: {e}"),
            }
        }
    };

    // Compare status codes.
    if pg_status != our_status {
        return TestResult {
            name: tc.name,
            passed: false,
            detail: format!("status mismatch: PostgREST={pg_status}, ours={our_status}"),
        };
    }

    // Compare bodies if requested.
    if tc.compare_body {
        let pg_json: Result<Value, _> = serde_json::from_str(&pg_body);
        let our_json: Result<Value, _> = serde_json::from_str(&our_body);

        match (pg_json, our_json) {
            (Ok(mut pg_val), Ok(mut our_val)) => {
                if tc.sort_array {
                    sort_json_array(&mut pg_val);
                    sort_json_array(&mut our_val);
                }
                if pg_val != our_val {
                    return TestResult {
                        name: tc.name,
                        passed: false,
                        detail: format!(
                            "body mismatch:\n    PostgREST: {}\n    ours:      {}",
                            truncate(&pg_val.to_string(), 200),
                            truncate(&our_val.to_string(), 200),
                        ),
                    };
                }
            }
            (Err(e), _) => {
                return TestResult {
                    name: tc.name,
                    passed: false,
                    detail: format!("PostgREST returned invalid JSON: {e}"),
                }
            }
            (_, Err(e)) => {
                return TestResult {
                    name: tc.name,
                    passed: false,
                    detail: format!("pg-rest returned invalid JSON: {e}"),
                }
            }
        }
    }

    TestResult {
        name: tc.name,
        passed: true,
        detail: format!("status={pg_status}"),
    }
}

async fn send_request(
    client: &reqwest::Client,
    base: &str,
    tc: &TestCase,
) -> Result<(u16, String), String> {
    let url = format!("{base}{}", tc.path);
    let mut req = match tc.method {
        "GET" => client.get(&url),
        "POST" => client.post(&url),
        "PATCH" => client.patch(&url),
        "DELETE" => client.delete(&url),
        _ => return Err(format!("unsupported method: {}", tc.method)),
    };

    for (key, value) in &tc.headers {
        req = req.header(*key, value);
    }

    if let Some(body) = &tc.body {
        req = req.header("Content-Type", "application/json").json(body);
    }

    let resp = req.send().await.map_err(|e| e.to_string())?;
    let status = resp.status().as_u16();
    let body = resp.text().await.map_err(|e| e.to_string())?;
    Ok((status, body))
}

fn sort_json_array(val: &mut Value) {
    if let Value::Array(arr) = val {
        arr.sort_by_key(|a| a.to_string());
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}
