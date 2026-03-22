use super::*;
use crate::TestCase;

pub fn cases(jwt: &str) -> Vec<TestCase> {
    vec![
        // ---- Singular response (exactly one row) ----
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

        // ---- Singular with select ----
        {
            let mut tc = g("/authors?select=name&id=eq.1", jwt);
            tc.name = "singular: select columns";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ---- Singular with embedding ----
        {
            let mut tc = g("/books?select=title,authors(name)&id=eq.1", jwt);
            tc.name = "singular: with embed";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ---- Singular 406 (multiple rows) ----
        {
            let mut tc = g_status_only("singular: 406 multiple", "/authors", jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ---- Singular 406 (no rows) ----
        {
            let mut tc = g_status_only("singular: 406 empty", "/authors?name=eq.Nobody", jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ---- CSV content negotiation ----
        {
            let mut tc = g("/authors?order=id.asc&id=in.(1,2,3)", jwt);
            tc.name = "CSV output";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false; // CSV format may differ slightly
            tc
        },

        // Note: Prefer return=representation is tested in mutations module with test_user role.

        // ---- OpenAPI v2 ----
        {
            let mut tc = g_status_only("OpenAPI v2 (swagger)", "/", jwt);
            tc.headers.clear(); // no auth needed
            tc
        },

        // Note: OpenAPI v3 (?openapi-version=3) is a pg-rest-server extension
        // that PostgREST doesn't support. Not included in compat tests.
    ]
}
