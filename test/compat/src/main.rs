//! PostgREST compatibility test runner.
//!
//! Sends identical HTTP requests to both PostgREST and pg-rest-server-*,
//! then compares status codes and JSON response bodies.
//!
//! Usage:
//!   cd test/compat && docker compose up -d
//!   cargo run -p pg-rest-server-tokio-postgres-pg-wired --release -- --config test/compat/pg-rest-compat.toml &
//!   cargo run -p compat-test
//!
//! Swap `pg-rest-server-tokio-postgres-pg-wired` for either
//! `pg-rest-server-tokio-postgres-deadpool` or `pg-rest-server-resolute` to
//! run the suite against the other backends.

mod cases;

use clap::Parser;
use serde_json::Value;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://localhost:3100")]
    postgrest: String,
    #[arg(long, default_value = "http://localhost:3101")]
    ours: String,
    #[arg(long, default_value = "reallyreallyreallyreallyverysafe")]
    jwt_secret: String,
    /// Only run tests whose name contains this string.
    #[arg(long)]
    filter: Option<String>,
}

pub struct TestCase {
    pub name: &'static str,
    pub method: &'static str,
    pub path: &'static str,
    pub body: Option<Value>,
    pub headers: Vec<(&'static str, String)>,
    pub compare_body: bool,
    pub sort_array: bool,
    /// If true, skip status code comparison (both servers just need to succeed).
    pub skip_status: bool,
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

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let client = reqwest::Client::new();

    let jwt_anon = make_jwt(&args.jwt_secret, "web_anon");
    let jwt_user = make_jwt(&args.jwt_secret, "test_user");

    let all_cases = cases::all_cases(&jwt_anon, &jwt_user);
    let cases: Vec<&TestCase> = match &args.filter {
        Some(f) => all_cases
            .iter()
            .filter(|tc| tc.name.contains(f.as_str()))
            .collect(),
        None => all_cases.iter().collect(),
    };
    let total = cases.len();

    println!("Running {total} compatibility tests...\n");
    println!(
        "  PostgREST: {}\n  pg-rest:    {}\n",
        args.postgrest, args.ours
    );

    let mut results: Vec<TestResult> = Vec::new();

    for tc in &cases {
        let result = run_test(&client, &args.postgrest, &args.ours, tc).await;
        let icon = if result.passed { "✓" } else { "✗" };
        if !result.passed {
            println!("  {icon} {}: {}", result.name, result.detail);
        } else {
            println!("  {icon} {}", result.name);
        }
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

    if pg_status != our_status && !tc.skip_status {
        return TestResult {
            name: tc.name,
            passed: false,
            detail: format!(
                "status mismatch: PostgREST={pg_status}, ours={our_status}\n    PG body: {}\n    Our body: {}",
                truncate(&pg_body, 200),
                truncate(&our_body, 200),
            ),
        };
    }

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
                            truncate(&pg_val.to_string(), 300),
                            truncate(&our_val.to_string(), 300),
                        ),
                    };
                }
            }
            (Err(e), _) => {
                return TestResult {
                    name: tc.name,
                    passed: false,
                    detail: format!(
                        "PostgREST returned invalid JSON: {e}\n    body: {}",
                        truncate(&pg_body, 200)
                    ),
                }
            }
            (_, Err(e)) => {
                return TestResult {
                    name: tc.name,
                    passed: false,
                    detail: format!(
                        "pg-rest returned invalid JSON: {e}\n    body: {}",
                        truncate(&our_body, 200)
                    ),
                }
            }
        }
    }

    TestResult {
        name: tc.name,
        passed: true,
        detail: String::new(),
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
        "OPTIONS" => client.request(reqwest::Method::OPTIONS, &url),
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
