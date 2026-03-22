use super::*;
use crate::TestCase;

pub fn cases(jwt: &str) -> Vec<TestCase> {
    vec![
        // ---- Limit ----
        g("/numbered?order=id.asc&limit=5", jwt),
        g("/numbered?order=id.asc&limit=1", jwt),
        g("/numbered?order=id.asc&limit=0", jwt),

        // ---- Offset ----
        g("/numbered?order=id.asc&limit=5&offset=0", jwt),
        g("/numbered?order=id.asc&limit=5&offset=5", jwt),
        g("/numbered?order=id.asc&limit=5&offset=95", jwt),
        g("/numbered?order=id.asc&limit=5&offset=100", jwt), // past end

        // ---- Range header ----
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

        // ---- Count=exact ----
        {
            let mut tc = g("/authors?order=id.asc&id=in.(1,2,3)", jwt);
            tc.name = "count=exact on authors";
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

        // ---- Large offset ----
        g("/numbered?order=id.asc&offset=200", jwt), // past all rows
    ]
}
