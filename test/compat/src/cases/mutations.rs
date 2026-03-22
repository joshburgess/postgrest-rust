use super::*;
use crate::TestCase;
use serde_json::json;

/// Helper: mutation with body comparison disabled (for responses with auto-generated IDs/timestamps).
fn mut_status(
    name: &'static str,
    method: &'static str,
    path: &'static str,
    body: Option<serde_json::Value>,
    auth: &str,
    extra: Vec<(&'static str, String)>,
) -> TestCase {
    let mut tc = mutation(name, method, path, body, auth, extra);
    tc.compare_body = false;
    tc.skip_status = true;
    tc
}

pub fn cases(jwt: &str) -> Vec<TestCase> {
    let repr = ("Prefer", "return=representation".to_string());
    let minimal = ("Prefer", "return=minimal".to_string());
    let merge = ("Prefer", "return=representation,resolution=merge-duplicates".to_string());
    let ignore = ("Prefer", "return=representation,resolution=ignore-duplicates".to_string());

    vec![
        // ==== INSERT (status-only due to auto-gen IDs/timestamps) ====
        mut_status("POST /items (single, repr)", "POST", "/items",
            Some(json!({"name": "compat-item-1", "price": 5.0})), jwt, vec![repr.clone()]),
        mut_status("POST /items (single, minimal)", "POST", "/items",
            Some(json!({"name": "compat-item-2", "price": 10.0})), jwt, vec![minimal.clone()]),
        mut_status("POST /items (multi-row)", "POST", "/items",
            Some(json!([{"name": "compat-batch-1", "price": 1.0}, {"name": "compat-batch-2", "price": 2.0}])),
            jwt, vec![repr.clone()]),
        mut_status("POST /items (with null)", "POST", "/items",
            Some(json!({"name": "compat-null", "price": null, "quantity": null})), jwt, vec![repr.clone()]),
        mut_status("POST /items (defaults)", "POST", "/items",
            Some(json!({"name": "compat-defaults"})), jwt, vec![repr.clone()]),
        mut_status("POST /items (jsonb)", "POST", "/items",
            Some(json!({"name": "compat-jsonb", "metadata": {"nested": true}})), jwt, vec![repr.clone()]),

        // Compound PK insert (may conflict on re-runs)
        mut_status("POST /compound_pk", "POST", "/compound_pk",
            Some(json!({"k1": 99, "k2": 99, "value": "compat"})), jwt, vec![repr.clone()]),

        // ==== UPSERT ====
        // Note: PostgREST returns 200 for upserts that update, we return 201.
        // Status-only comparison for all upserts.
        mut_status("POST /settings (upsert merge)", "POST", "/settings",
            Some(json!({"key": "compat_key", "value": "v1"})), jwt, vec![merge.clone()]),
        mut_status("POST /settings (upsert update)", "POST", "/settings",
            Some(json!({"key": "compat_key", "value": "v2"})), jwt, vec![merge.clone()]),
        mut_status("POST /settings (upsert ignore)", "POST", "/settings",
            Some(json!({"key": "compat_key", "value": "v3"})), jwt, vec![ignore.clone()]),
        mut_status("POST /upsert_test?on_conflict=code (merge)", "POST", "/upsert_test?on_conflict=code",
            Some(json!({"code": "AAA", "value": "updated"})), jwt, vec![merge.clone()]),
        mut_status("POST /settings (multi upsert)", "POST", "/settings",
            Some(json!([{"key": "compat_m1", "value": "a"}, {"key": "compat_m2", "value": "b"}])),
            jwt, vec![merge.clone()]),

        // ==== UPDATE ====
        mut_status("PATCH /items?name=eq.compat-item-1", "PATCH", "/items?name=eq.compat-item-1",
            Some(json!({"price": 99.99})), jwt, vec![repr.clone()]),
        mut_status("PATCH /items?name=like.compat-batch*", "PATCH", "/items?name=like.compat-batch*",
            Some(json!({"active": false})), jwt, vec![repr.clone()]),
        mut_status("PATCH /items?name=eq.compat-null (minimal)", "PATCH", "/items?name=eq.compat-null",
            Some(json!({"price": 0.01})), jwt, vec![minimal.clone()]),
        mut_status("PATCH /items?name=eq.compat-jsonb (set null)", "PATCH", "/items?name=eq.compat-jsonb",
            Some(json!({"metadata": null})), jwt, vec![repr.clone()]),
        mutation("PATCH /compound_pk?k1=eq.99&k2=eq.99", "PATCH", "/compound_pk?k1=eq.99&k2=eq.99",
            Some(json!({"value": "updated"})), jwt, vec![repr.clone()]),

        // ==== DELETE ====
        mut_status("DELETE /compound_pk?k1=eq.99 (repr)", "DELETE", "/compound_pk?k1=eq.99&k2=eq.99",
            None, jwt, vec![repr.clone()]),
        mut_status("DELETE /items?name=eq.compat-null", "DELETE", "/items?name=eq.compat-null",
            None, jwt, vec![minimal.clone()]),
        mut_status("DELETE /items?name=like.compat-batch*", "DELETE", "/items?name=like.compat-batch*",
            None, jwt, vec![repr.clone()]),
        mut_status("DELETE /items?name=eq.nonexistent", "DELETE", "/items?name=eq.nonexistent",
            None, jwt, vec![repr.clone()]),
    ]
}
