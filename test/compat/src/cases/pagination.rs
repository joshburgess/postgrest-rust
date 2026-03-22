use super::*;
use crate::TestCase;

pub fn cases(jwt: &str) -> Vec<TestCase> {
    vec![
        // ==== Limit ====
        g("/numbered?order=id.asc&limit=5", jwt),
        g("/numbered?order=id.asc&limit=1", jwt),
        g("/numbered?order=id.asc&limit=0", jwt),
        g("/numbered?order=id.asc&limit=100", jwt),
        g("/authors?order=id.asc&limit=1", jwt),
        g("/items?order=id.asc&limit=2", jwt),

        // ==== Offset ====
        g("/numbered?order=id.asc&limit=5&offset=0", jwt),
        g("/numbered?order=id.asc&limit=5&offset=5", jwt),
        g("/numbered?order=id.asc&limit=5&offset=50", jwt),
        g("/numbered?order=id.asc&limit=5&offset=95", jwt),
        g("/numbered?order=id.asc&limit=5&offset=100", jwt), // past end
        g("/numbered?order=id.asc&limit=1&offset=99", jwt), // last row
        g("/numbered?order=id.asc&offset=200", jwt), // way past end
        g("/authors?order=id.asc&limit=1&offset=1", jwt),
        g("/authors?order=id.asc&limit=1&offset=2", jwt),
        g("/authors?order=id.asc&limit=10&offset=0", jwt),

        // ==== Range header ====
        {
            let mut tc = g("/numbered?order=id.asc", jwt);
            tc.name = "Range: 0-4";
            tc.headers.push(("Range", "0-4".to_string()));
            tc
        },
        {
            let mut tc = g("/numbered?order=id.asc", jwt);
            tc.name = "Range: 10-19";
            tc.headers.push(("Range", "10-19".to_string()));
            tc
        },
        {
            let mut tc = g("/numbered?order=id.asc", jwt);
            tc.name = "Range: 95-99";
            tc.headers.push(("Range", "95-99".to_string()));
            tc
        },
        {
            let mut tc = g("/authors?order=id.asc", jwt);
            tc.name = "Range: 0-0 (single row)";
            tc.headers.push(("Range", "0-0".to_string()));
            tc
        },

        // ==== Count=exact ====
        {
            let mut tc = g("/authors?order=id.asc&id=in.(1,2,3)", jwt);
            tc.name = "count=exact authors";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/numbered?order=id.asc&limit=5", jwt);
            tc.name = "count=exact with limit";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/numbered?order=id.asc&limit=5&offset=50", jwt);
            tc.name = "count=exact with limit+offset";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/numbered?order=id.asc", jwt);
            tc.name = "count=exact full table";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/books?order=id.asc", jwt);
            tc.name = "count=exact books";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/items?order=id.asc&active=eq.true", jwt);
            tc.name = "count=exact with filter";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },

        // ==== More limit/offset combos ====
        g("/books?order=id.asc&limit=2&offset=0", jwt),
        g("/books?order=id.asc&limit=2&offset=2", jwt),
        g("/books?order=id.asc&limit=10&offset=0", jwt),
        g("/employees?order=id.asc&limit=3", jwt),
        g("/employees?order=id.asc&limit=3&offset=3", jwt),
        g("/profiles?order=id.asc&limit=2", jwt),
        g("/profiles?order=id.asc&limit=2&offset=2", jwt),

        // ==== Range on small tables ====
        {
            let mut tc = g("/books?order=id.asc", jwt);
            tc.name = "Range books 0-1";
            tc.headers.push(("Range", "0-1".to_string()));
            tc
        },
        {
            let mut tc = g("/employees?order=id.asc", jwt);
            tc.name = "Range employees 0-2";
            tc.headers.push(("Range", "0-2".to_string()));
            tc
        },

        // ==== Count on more tables ====
        {
            let mut tc = g("/profiles?order=id.asc", jwt);
            tc.name = "count profiles";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/tasks?order=id.asc", jwt);
            tc.name = "count tasks";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/unicode_test?order=id.asc", jwt);
            tc.name = "count unicode_test";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/compound_pk?order=k1.asc,k2.asc", jwt);
            tc.name = "count compound_pk";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },

        // ==== Count with limit (partial content 206) ====
        {
            let mut tc = g("/profiles?order=id.asc&limit=2", jwt);
            tc.name = "count profiles partial";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/books?order=id.asc&limit=2", jwt);
            tc.name = "count books partial";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
    ]
}
