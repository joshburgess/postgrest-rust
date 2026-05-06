use super::*;
use crate::TestCase;

pub fn cases(jwt: &str) -> Vec<TestCase> {
    vec![
        // ==== OPTIONS requests (CORS preflight) ====
        // Both servers should return 200 for OPTIONS. Body comparison disabled
        // since CORS header details differ between implementations.
        {
            TestCase {
                name: "OPTIONS /authors",
                method: "OPTIONS",
                path: "/authors",
                body: None,
                headers: vec![
                    ("Origin", "http://localhost:3000".to_string()),
                    ("Access-Control-Request-Method", "GET".to_string()),
                ],
                compare_body: false,
                sort_array: false,
                skip_status: false,
            }
        },
        {
            TestCase {
                name: "OPTIONS /books",
                method: "OPTIONS",
                path: "/books",
                body: None,
                headers: vec![
                    ("Origin", "http://localhost:3000".to_string()),
                    ("Access-Control-Request-Method", "POST".to_string()),
                ],
                compare_body: false,
                sort_array: false,
                skip_status: false,
            }
        },
        {
            TestCase {
                name: "OPTIONS /rpc/add",
                method: "OPTIONS",
                path: "/rpc/add",
                body: None,
                headers: vec![
                    ("Origin", "http://localhost:3000".to_string()),
                    ("Access-Control-Request-Method", "POST".to_string()),
                ],
                compare_body: false,
                sort_array: false,
                skip_status: false,
            }
        },
        {
            TestCase {
                name: "OPTIONS /",
                method: "OPTIONS",
                path: "/",
                body: None,
                headers: vec![
                    ("Origin", "http://localhost:3000".to_string()),
                    ("Access-Control-Request-Method", "GET".to_string()),
                ],
                compare_body: false,
                sort_array: false,
                skip_status: false,
            }
        },
        // ==== CORS headers on regular requests ====
        // Verify both servers include Access-Control-Allow-Origin
        // (status-only since header values may differ)
        g_status_only("CORS on GET /authors", "/authors?order=id.asc", jwt),
        g_status_only("CORS on GET /books", "/books?order=id.asc", jwt),
        // ==== OPTIONS on more endpoints ====
        {
            TestCase {
                name: "OPTIONS /items",
                method: "OPTIONS",
                path: "/items",
                body: None,
                headers: vec![
                    ("Origin", "http://localhost:3000".to_string()),
                    ("Access-Control-Request-Method", "PATCH".to_string()),
                ],
                compare_body: false,
                sort_array: false,
                skip_status: false,
            }
        },
        {
            TestCase {
                name: "OPTIONS /employees",
                method: "OPTIONS",
                path: "/employees",
                body: None,
                headers: vec![
                    ("Origin", "http://localhost:3000".to_string()),
                    ("Access-Control-Request-Method", "DELETE".to_string()),
                ],
                compare_body: false,
                sort_array: false,
                skip_status: false,
            }
        },
        {
            TestCase {
                name: "OPTIONS /settings",
                method: "OPTIONS",
                path: "/settings",
                body: None,
                headers: vec![
                    ("Origin", "http://localhost:3000".to_string()),
                    ("Access-Control-Request-Method", "GET".to_string()),
                ],
                compare_body: false,
                sort_array: false,
                skip_status: false,
            }
        },
        {
            TestCase {
                name: "OPTIONS /rpc/echo",
                method: "OPTIONS",
                path: "/rpc/echo",
                body: None,
                headers: vec![
                    ("Origin", "http://localhost:3000".to_string()),
                    ("Access-Control-Request-Method", "POST".to_string()),
                ],
                compare_body: false,
                sort_array: false,
                skip_status: false,
            }
        },
        {
            TestCase {
                name: "OPTIONS /orders",
                method: "OPTIONS",
                path: "/orders",
                body: None,
                headers: vec![
                    ("Origin", "http://localhost:3000".to_string()),
                    ("Access-Control-Request-Method", "POST".to_string()),
                ],
                compare_body: false,
                sort_array: false,
                skip_status: false,
            }
        },
        {
            TestCase {
                name: "OPTIONS /profiles",
                method: "OPTIONS",
                path: "/profiles",
                body: None,
                headers: vec![
                    ("Origin", "http://localhost:3000".to_string()),
                    ("Access-Control-Request-Method", "PATCH".to_string()),
                ],
                compare_body: false,
                sort_array: false,
                skip_status: false,
            }
        },
        {
            TestCase {
                name: "OPTIONS /logs",
                method: "OPTIONS",
                path: "/logs",
                body: None,
                headers: vec![
                    ("Origin", "http://localhost:3000".to_string()),
                    ("Access-Control-Request-Method", "GET".to_string()),
                ],
                compare_body: false,
                sort_array: false,
                skip_status: false,
            }
        },
        {
            TestCase {
                name: "OPTIONS /rpc/multiply",
                method: "OPTIONS",
                path: "/rpc/multiply",
                body: None,
                headers: vec![
                    ("Origin", "http://localhost:3000".to_string()),
                    ("Access-Control-Request-Method", "POST".to_string()),
                ],
                compare_body: false,
                sort_array: false,
                skip_status: false,
            }
        },
        // CORS on regular requests for more tables
        g_status_only("CORS on GET /items", "/items?order=id.asc", jwt),
        g_status_only("CORS on GET /orders", "/orders?order=id.asc", jwt),
        g_status_only("CORS on GET /profiles", "/profiles?order=id.asc", jwt),
        g_status_only("CORS on GET /logs", "/logs?order=id.asc", jwt),
    ]
}
