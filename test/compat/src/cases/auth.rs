use super::*;
use crate::TestCase;

pub fn cases(jwt_anon: &str, jwt_user: &str) -> Vec<TestCase> {
    vec![
        // ---- Anon role reads ----
        g("/authors?order=id.asc", jwt_anon),
        g("/books?order=id.asc", jwt_anon),

        // ---- RLS: anon sees only published articles ----
        {
            let mut tc = g("/articles?order=id.asc", jwt_anon);
            tc.name = "RLS: anon sees published only";
            tc
        },

        // ---- RLS: authenticated user sees all ----
        {
            let mut tc = g("/articles?order=id.asc", jwt_user);
            tc.name = "RLS: user sees all articles";
            tc
        },

        // ---- Anon cannot insert (no GRANT) ----
        {
            let mut tc = mutation(
                "anon INSERT denied",
                "POST",
                "/authors",
                Some(serde_json::json!({"name": "Hacker"})),
                jwt_anon,
                vec![("Prefer", "return=representation".to_string())],
            );
            tc.compare_body = false; // error format may differ
            tc
        },

        // ---- User can insert (status only — auto-generated IDs differ) ----
        {
            let mut tc = mutation(
                "user INSERT allowed",
                "POST",
                "/items",
                Some(serde_json::json!({"name": "auth-test-item"})),
                jwt_user,
                vec![("Prefer", "return=representation".to_string())],
            );
            tc.compare_body = false;
            tc
        },

        // ---- No auth header (treated as anon) ----
        {
            TestCase {
                name: "no auth header (anon)",
                method: "GET",
                path: "/authors?order=id.asc",
                body: None,
                headers: vec![], // no Authorization
                compare_body: false,
                sort_array: false,
                skip_status: false,
            }
        },

        // ---- Invalid JWT ----
        {
            TestCase {
                name: "invalid JWT (401)",
                method: "GET",
                path: "/authors",
                body: None,
                headers: vec![("Authorization", "Bearer invalid.token.here".to_string())],
                compare_body: false,
                sort_array: false,
                skip_status: false,
            }
        },

        // ---- Cleanup ----
        {
            let mut tc = mutation(
                "cleanup auth test",
                "DELETE",
                "/items?name=eq.auth-test-item",
                None,
                jwt_user,
                vec![("Prefer", "return=minimal".to_string())],
            );
            tc.compare_body = false;
            tc
        },
    ]
}
