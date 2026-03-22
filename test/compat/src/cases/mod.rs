pub mod query;
pub mod rpc;
pub mod mutations;
pub mod embedding;
pub mod pagination;
pub mod logic_filters;
pub mod json_ops;
pub mod singular;
pub mod auth;
pub mod explain;
pub mod cors;
pub mod edge_cases;

use crate::TestCase;
use serde_json::Value;

pub fn all_cases(jwt_anon: &str, jwt_user: &str) -> Vec<TestCase> {
    let mut cases = Vec::new();
    cases.extend(query::cases(jwt_anon));
    cases.extend(rpc::cases(jwt_anon));
    cases.extend(mutations::cases(jwt_user));
    cases.extend(embedding::cases(jwt_anon));
    cases.extend(pagination::cases(jwt_anon));
    cases.extend(logic_filters::cases(jwt_anon));
    cases.extend(json_ops::cases(jwt_anon));
    cases.extend(singular::cases(jwt_anon));
    cases.extend(auth::cases(jwt_anon, jwt_user));
    cases.extend(explain::cases(jwt_anon));
    cases.extend(cors::cases(jwt_anon));
    cases.extend(edge_cases::cases(jwt_anon, jwt_user));
    cases
}

pub fn g(path: &'static str, auth: &str) -> TestCase {
    TestCase {
        name: path,
        method: "GET",
        path,
        body: None,
        headers: vec![("Authorization", format!("Bearer {auth}"))],
        compare_body: true,
        sort_array: false,
        skip_status: false,
    }
}

pub fn g_sorted(name: &'static str, path: &'static str, auth: &str) -> TestCase {
    TestCase {
        name,
        method: "GET",
        path,
        body: None,
        headers: vec![("Authorization", format!("Bearer {auth}"))],
        compare_body: true,
        sort_array: true,
        skip_status: false,
    }
}

/// Test that both servers respond without connection error (skip status + body comparison).
pub fn g_skip_all(name: &'static str, path: &'static str, auth: &str) -> TestCase {
    TestCase {
        name,
        method: "GET",
        path,
        body: None,
        headers: vec![("Authorization", format!("Bearer {auth}"))],
        compare_body: false,
        sort_array: false,
        skip_status: true,
    }
}

pub fn g_status_only(name: &'static str, path: &'static str, auth: &str) -> TestCase {
    TestCase {
        name,
        method: "GET",
        path,
        body: None,
        headers: vec![("Authorization", format!("Bearer {auth}"))],
        compare_body: false,
        sort_array: false,
        skip_status: false,
    }
}

pub fn post_json(name: &'static str, path: &'static str, body: Value, auth: &str) -> TestCase {
    TestCase {
        name,
        method: "POST",
        path,
        body: Some(body),
        headers: vec![("Authorization", format!("Bearer {auth}"))],
        compare_body: true,
        sort_array: false,
        skip_status: false,
    }
}

pub fn mutation(
    name: &'static str,
    method: &'static str,
    path: &'static str,
    body: Option<Value>,
    auth: &str,
    extra_headers: Vec<(&'static str, String)>,
) -> TestCase {
    let mut headers = vec![("Authorization", format!("Bearer {auth}"))];
    headers.extend(extra_headers);
    TestCase {
        name,
        method,
        path,
        body,
        headers,
        compare_body: true,
        sort_array: false,
        skip_status: false,
    }
}
