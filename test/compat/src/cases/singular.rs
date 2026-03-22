use super::*;
use crate::TestCase;

pub fn cases(jwt: &str) -> Vec<TestCase> {
    vec![
        // ==== Singular response (exactly one row) ====
        {
            let mut tc = g("/authors?id=eq.1", jwt);
            tc.name = "singular: one author";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/books?id=eq.1", jwt);
            tc.name = "singular: one book";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/settings?key=eq.theme", jwt);
            tc.name = "singular: setting";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/employees?id=eq.1", jwt);
            tc.name = "singular: employee";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ==== Singular with select ====
        {
            let mut tc = g("/authors?select=name&id=eq.1", jwt);
            tc.name = "singular: select columns";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/books?select=title,pages&id=eq.1", jwt);
            tc.name = "singular: select book cols";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ==== Singular with embedding ====
        {
            let mut tc = g("/books?select=title,authors(name)&id=eq.1", jwt);
            tc.name = "singular: with embed";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/authors?select=name,books(title)&id=eq.1", jwt);
            tc.name = "singular: O2M embed";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ==== Singular 406 (multiple rows) ====
        {
            let mut tc = g_status_only("singular: 406 multiple", "/authors", jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ==== Singular 406 (no rows) ====
        {
            let mut tc = g_status_only("singular: 406 empty", "/authors?name=eq.Nobody", jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ==== CSV content negotiation ====
        {
            let mut tc = g("/authors?order=id.asc&id=in.(1,2,3)", jwt);
            tc.name = "CSV output";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false; // CSV format details may differ
            tc
        },
        {
            let mut tc = g("/books?order=id.asc&select=title,pages", jwt);
            tc.name = "CSV books";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false;
            tc
        },

        // ==== OpenAPI v2 ====
        {
            let mut tc = g_status_only("OpenAPI v2 (swagger)", "/", jwt);
            tc.headers.clear();
            tc
        },

        // ==== More singular tests ====
        {
            let mut tc = g("/profiles?username=eq.bob", jwt);
            tc.name = "singular profile";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/employees?id=eq.4", jwt);
            tc.name = "singular employee 4";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/tasks?id=eq.1", jwt);
            tc.name = "singular task";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ==== More CSV tests ====
        {
            let mut tc = g("/employees?order=id.asc", jwt);
            tc.name = "CSV employees";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false;
            tc
        },
        {
            let mut tc = g("/profiles?order=id.asc", jwt);
            tc.name = "CSV profiles";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false;
            tc
        },
        {
            let mut tc = g("/items?order=id.asc&select=name,price", jwt);
            tc.name = "CSV items";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false;
            tc
        },

        // ==== Prefer: count variations ====
        {
            let mut tc = g("/authors?order=id.asc&id=in.(1,2,3)", jwt);
            tc.name = "Prefer count=exact";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
    ]
}
