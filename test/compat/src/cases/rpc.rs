use super::*;
use crate::TestCase;
use serde_json::json;

pub fn cases(jwt: &str) -> Vec<TestCase> {
    vec![
        // ==== Scalar functions ====
        post_json("rpc/add basic", "/rpc/add", json!({"a": 3, "b": 4}), jwt),
        post_json("rpc/add large", "/rpc/add", json!({"a": 999, "b": 1}), jwt),
        post_json("rpc/add negative", "/rpc/add", json!({"a": -5, "b": 3}), jwt),
        post_json("rpc/add zero", "/rpc/add", json!({"a": 0, "b": 0}), jwt),
        post_json("rpc/echo basic", "/rpc/echo", json!({"value": "test"}), jwt),
        post_json("rpc/echo empty", "/rpc/echo", json!({"value": ""}), jwt),
        post_json("rpc/echo special", "/rpc/echo", json!({"value": "hello 'world'"}), jwt),
        post_json("rpc/echo unicode", "/rpc/echo", json!({"value": "日本語"}), jwt),

        // ==== SetOf functions ====
        post_json("rpc/search_books Rust", "/rpc/search_books", json!({"query": "Rust"}), jwt),
        post_json("rpc/search_books SQL", "/rpc/search_books", json!({"query": "SQL"}), jwt),
        post_json("rpc/search_books no match", "/rpc/search_books", json!({"query": "Nonexistent"}), jwt),
        post_json("rpc/search_books empty", "/rpc/search_books", json!({"query": ""}), jwt),
        post_json("rpc/authors_below 3", "/rpc/authors_below", json!({"max_id": 3}), jwt),
        post_json("rpc/authors_below 1", "/rpc/authors_below", json!({"max_id": 1}), jwt),
        post_json("rpc/authors_below 100", "/rpc/authors_below", json!({"max_id": 100}), jwt),

        // ==== Default params ====
        post_json("rpc/greet default", "/rpc/greet", json!({}), jwt),
        post_json("rpc/greet named", "/rpc/greet", json!({"name": "Rust"}), jwt),
        post_json("rpc/multi_defaults none", "/rpc/multi_defaults", json!({}), jwt),
        post_json("rpc/multi_defaults partial a", "/rpc/multi_defaults", json!({"a": 10}), jwt),
        post_json("rpc/multi_defaults partial ab", "/rpc/multi_defaults", json!({"a": 10, "b": 20}), jwt),
        post_json("rpc/multi_defaults all", "/rpc/multi_defaults", json!({"a": 10, "b": 20, "c": "world"}), jwt),

        // ==== GET for immutable/stable functions ====
        g("/rpc/add?a=10&b=20", jwt),
        g("/rpc/echo?value=hello", jwt),
        g("/rpc/greet?name=World", jwt),
        g("/rpc/echo?value=", jwt),
        g("/rpc/add?a=0&b=0", jwt),

        // ==== Void function ====
        {
            let mut tc = post_json("rpc/void_func", "/rpc/void_func", json!({}), jwt);
            tc.compare_body = false;
            tc
        },

        // ==== Function returning single record ====
        post_json("rpc/get_author 1", "/rpc/get_author", json!({"author_id": 1}), jwt),
        post_json("rpc/get_author 2", "/rpc/get_author", json!({"author_id": 2}), jwt),

        // ==== Function returning JSON ====
        post_json("rpc/json_func", "/rpc/json_func", json!({}), jwt),

        // ==== Function returning NULL ====
        post_json("rpc/null_func", "/rpc/null_func", json!({}), jwt),

        // ==== Function with JSON param ====
        post_json("rpc/json_param", "/rpc/json_param", json!({"data": {"key": "hello"}}), jwt),
        post_json("rpc/json_param nested", "/rpc/json_param", json!({"data": {"key": "nested", "extra": true}}), jwt),

        // ==== Volatile function ====
        post_json("rpc/reset_counter", "/rpc/reset_counter", json!({}), jwt),

        // ==== Function returning TABLE ====
        post_json("rpc/get_items_by_price", "/rpc/get_items_by_price", json!({"min_price": 10}), jwt),
        post_json("rpc/get_items_by_price zero", "/rpc/get_items_by_price", json!({"min_price": 0}), jwt),
        post_json("rpc/get_items_by_price high", "/rpc/get_items_by_price", json!({"min_price": 1000}), jwt),

        // ==== Function that errors ====
        {
            let mut tc = post_json("rpc/error_func", "/rpc/error_func", json!({}), jwt);
            tc.compare_body = false; // error format may differ
            tc
        },

        // ==== RPC 404 ====
        g_status_only("rpc nonexistent 404", "/rpc/nonexistent", jwt),

        // ==== RPC with select/order on results ====
        g("/rpc/search_books?query=Rust&select=title&order=title.asc", jwt),
        g("/rpc/authors_below?max_id=10&select=name&order=name.asc", jwt),

        // ==== RPC GET with default params ====
        g("/rpc/greet", jwt),
        g("/rpc/multi_defaults", jwt),
        g("/rpc/multi_defaults?a=5", jwt),
        g("/rpc/multi_defaults?a=5&b=10", jwt),

        // ==== RPC with specific result count ====
        post_json("rpc/authors_below 0", "/rpc/authors_below", json!({"max_id": 0}), jwt),
        post_json("rpc/search_books partial", "/rpc/search_books", json!({"query": "Learn"}), jwt),

        // ==== GET rpc/add variations ====
        g("/rpc/add?a=1&b=1", jwt),
        g("/rpc/add?a=-100&b=100", jwt),
        g("/rpc/add?a=2147483647&b=0", jwt),

        // ==== RPC echo variations ====
        post_json("rpc/echo spaces", "/rpc/echo", json!({"value": "hello world"}), jwt),
        post_json("rpc/echo numbers", "/rpc/echo", json!({"value": "12345"}), jwt),
        g("/rpc/echo?value=with%20spaces", jwt),

        // ==== Boolean return function ====
        post_json("rpc/is_positive 5", "/rpc/is_positive", json!({"n": 5}), jwt),
        post_json("rpc/is_positive -1", "/rpc/is_positive", json!({"n": -1}), jwt),
        post_json("rpc/is_positive 0", "/rpc/is_positive", json!({"n": 0}), jwt),
        g("/rpc/is_positive?n=10", jwt),
        g("/rpc/is_positive?n=-5", jwt),

        // ==== Numeric return function ====
        post_json("rpc/multiply basic", "/rpc/multiply", json!({"a": 3, "b": 4}), jwt),
        post_json("rpc/multiply decimal", "/rpc/multiply", json!({"a": 2.5, "b": 4}), jwt),
        post_json("rpc/multiply zero", "/rpc/multiply", json!({"a": 0, "b": 100}), jwt),
        post_json("rpc/multiply negative", "/rpc/multiply", json!({"a": -3, "b": 7}), jwt),
        g("/rpc/multiply?a=10&b=20", jwt),

        // ==== Active profiles function ====
        post_json("rpc/active_profiles default", "/rpc/active_profiles", json!({}), jwt),
        post_json("rpc/active_profiles min80", "/rpc/active_profiles", json!({"min_score": 80}), jwt),
        post_json("rpc/active_profiles min100", "/rpc/active_profiles", json!({"min_score": 100}), jwt),
        g("/rpc/active_profiles?min_score=50", jwt),

        // ==== RPC with select on setof results ====
        g("/rpc/active_profiles?select=username,score&order=score.desc", jwt),
        g("/rpc/search_books?query=Rust&select=title", jwt),

        // ==== RPC singular response ====
        {
            let mut tc = post_json("rpc/get_author singular", "/rpc/get_author", json!({"author_id": 1}), jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
    ]
}
