use super::*;
use crate::TestCase;
use serde_json::json;

pub fn cases(jwt: &str) -> Vec<TestCase> {
    vec![
        // ---- Scalar functions ----
        post_json("POST /rpc/add", "/rpc/add", json!({"a": 3, "b": 4}), jwt),
        post_json("POST /rpc/add (large)", "/rpc/add", json!({"a": 999, "b": 1}), jwt),
        post_json("POST /rpc/add (negative)", "/rpc/add", json!({"a": -5, "b": 3}), jwt),
        post_json("POST /rpc/echo", "/rpc/echo", json!({"value": "test"}), jwt),
        post_json("POST /rpc/echo (empty)", "/rpc/echo", json!({"value": ""}), jwt),
        post_json(
            "POST /rpc/echo (special chars)",
            "/rpc/echo",
            json!({"value": "hello 'world' \"foo\""}),
            jwt,
        ),

        // ---- SetOf functions ----
        post_json(
            "POST /rpc/search_books (Rust)",
            "/rpc/search_books",
            json!({"query": "Rust"}),
            jwt,
        ),
        post_json(
            "POST /rpc/search_books (SQL)",
            "/rpc/search_books",
            json!({"query": "SQL"}),
            jwt,
        ),
        post_json(
            "POST /rpc/search_books (no match)",
            "/rpc/search_books",
            json!({"query": "Nonexistent"}),
            jwt,
        ),
        post_json(
            "POST /rpc/authors_below",
            "/rpc/authors_below",
            json!({"max_id": 3}),
            jwt,
        ),

        // ---- Default params ----
        post_json("POST /rpc/greet (default)", "/rpc/greet", json!({}), jwt),
        post_json(
            "POST /rpc/greet (named)",
            "/rpc/greet",
            json!({"name": "Rust"}),
            jwt,
        ),
        post_json(
            "POST /rpc/multi_defaults (none)",
            "/rpc/multi_defaults",
            json!({}),
            jwt,
        ),
        post_json(
            "POST /rpc/multi_defaults (partial)",
            "/rpc/multi_defaults",
            json!({"a": 10}),
            jwt,
        ),
        post_json(
            "POST /rpc/multi_defaults (all)",
            "/rpc/multi_defaults",
            json!({"a": 10, "b": 20, "c": "world"}),
            jwt,
        ),

        // ---- GET for immutable/stable functions ----
        g("/rpc/add?a=10&b=20", jwt),
        g("/rpc/echo?value=hello", jwt),
        g("/rpc/greet?name=World", jwt),

        // ---- Void function (204 No Content, no body to compare) ----
        {
            let mut tc = post_json("POST /rpc/void_func", "/rpc/void_func", json!({}), jwt);
            tc.compare_body = false;
            tc
        },

        // ---- Function returning single record ----
        post_json(
            "POST /rpc/get_author",
            "/rpc/get_author",
            json!({"author_id": 1}),
            jwt,
        ),

        // ---- Function returning JSON ----
        post_json("POST /rpc/json_func", "/rpc/json_func", json!({}), jwt),

        // ---- Volatile function (POST only) ----
        post_json(
            "POST /rpc/reset_counter",
            "/rpc/reset_counter",
            json!({}),
            jwt,
        ),

        // ---- RPC 404 ----
        g_status_only(
            "POST /rpc/nonexistent (404)",
            "/rpc/nonexistent",
            jwt,
        ),
    ]
}
