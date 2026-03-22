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

        // ==== More multiply variations ====
        post_json("rpc/multiply large", "/rpc/multiply", json!({"a": 999.99, "b": 1000}), jwt),
        g("/rpc/multiply?a=0.5&b=0.5", jwt),
        g("/rpc/multiply?a=1&b=1", jwt),

        // ==== Active profiles with filter ====
        g("/rpc/active_profiles?select=username&order=username.asc", jwt),
        g("/rpc/active_profiles?min_score=0&select=username,score&order=score.desc", jwt),
        post_json("rpc/active_profiles min0", "/rpc/active_profiles", json!({"min_score": 0}), jwt),

        // ==== is_positive variations ====
        g("/rpc/is_positive?n=1", jwt),
        g("/rpc/is_positive?n=0", jwt),
        g("/rpc/is_positive?n=-1", jwt),
        post_json("rpc/is_positive 100", "/rpc/is_positive", json!({"n": 100}), jwt),
        post_json("rpc/is_positive -100", "/rpc/is_positive", json!({"n": -100}), jwt),

        // ==== echo boundary cases ====
        post_json("rpc/echo single char", "/rpc/echo", json!({"value": "x"}), jwt),
        post_json("rpc/echo with newline", "/rpc/echo", json!({"value": "line1\nline2"}), jwt),
        g("/rpc/echo?value=abc", jwt),

        // ==== greet boundary cases ====
        post_json("rpc/greet empty name", "/rpc/greet", json!({"name": ""}), jwt),
        post_json("rpc/greet long name", "/rpc/greet", json!({"name": "a very long name indeed"}), jwt),

        // ==== get_author variations ====
        post_json("rpc/get_author 3", "/rpc/get_author", json!({"author_id": 3}), jwt),

        // ==== search_books variations ====
        post_json("rpc/search_books Deep", "/rpc/search_books", json!({"query": "Deep"}), jwt),
        post_json("rpc/search_books Basic", "/rpc/search_books", json!({"query": "Basic"}), jwt),
        g("/rpc/search_books?query=Post&select=id,title&order=id.asc", jwt),

        // ==== get_items_by_price variations ====
        post_json("rpc/get_items_by_price 5", "/rpc/get_items_by_price", json!({"min_price": 5}), jwt),
        g("/rpc/get_items_by_price?min_price=20&select=name,price", jwt),

        // ==== count_by_status ====
        post_json("rpc/count_by_status completed", "/rpc/count_by_status", json!({"s": "completed"}), jwt),
        post_json("rpc/count_by_status pending", "/rpc/count_by_status", json!({"s": "pending"}), jwt),
        post_json("rpc/count_by_status shipped", "/rpc/count_by_status", json!({"s": "shipped"}), jwt),
        post_json("rpc/count_by_status none", "/rpc/count_by_status", json!({"s": "nonexistent"}), jwt),
        g("/rpc/count_by_status?s=completed", jwt),

        // ==== customer_orders ====
        post_json("rpc/customer_orders Alice", "/rpc/customer_orders", json!({"cust": "Alice"}), jwt),
        post_json("rpc/customer_orders Bob", "/rpc/customer_orders", json!({"cust": "Bob"}), jwt),
        post_json("rpc/customer_orders nobody", "/rpc/customer_orders", json!({"cust": "Nobody"}), jwt),
        g("/rpc/customer_orders?cust=Alice&select=customer,amount&order=amount.asc", jwt),
        g("/rpc/customer_orders?cust=Carol", jwt),

        // ==== concat_strings ====
        post_json("rpc/concat_strings basic", "/rpc/concat_strings", json!({"a": "hello", "b": "world"}), jwt),
        post_json("rpc/concat_strings sep", "/rpc/concat_strings", json!({"a": "foo", "b": "bar", "sep": "-"}), jwt),
        post_json("rpc/concat_strings default sep", "/rpc/concat_strings", json!({"a": "x", "b": "y"}), jwt),
        g("/rpc/concat_strings?a=left&b=right", jwt),
        g("/rpc/concat_strings?a=a&b=b&sep=_", jwt),

        // ==== clamp ====
        post_json("rpc/clamp in range", "/rpc/clamp", json!({"val": 5, "lo": 1, "hi": 10}), jwt),
        post_json("rpc/clamp below", "/rpc/clamp", json!({"val": -5, "lo": 0, "hi": 100}), jwt),
        post_json("rpc/clamp above", "/rpc/clamp", json!({"val": 999, "lo": 0, "hi": 100}), jwt),
        post_json("rpc/clamp edge", "/rpc/clamp", json!({"val": 10, "lo": 10, "hi": 10}), jwt),
        g("/rpc/clamp?val=50&lo=0&hi=100", jwt),
        g("/rpc/clamp?val=-1&lo=0&hi=10", jwt),

        // ==== sum_amounts ====
        post_json("rpc/sum_amounts default", "/rpc/sum_amounts", json!({}), jwt),
        post_json("rpc/sum_amounts min100", "/rpc/sum_amounts", json!({"min_amount": 100}), jwt),
        post_json("rpc/sum_amounts min1000", "/rpc/sum_amounts", json!({"min_amount": 1000}), jwt),
        g("/rpc/sum_amounts?min_amount=200", jwt),
        g("/rpc/sum_amounts", jwt),
    ]
}
