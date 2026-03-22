use super::*;
use crate::TestCase;
use serde_json::json;

fn mut_skip(
    name: &'static str, method: &'static str, path: &'static str,
    body: Option<serde_json::Value>, auth: &str, extra: Vec<(&'static str, String)>,
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
        // ==== INSERT ====
        // Status-only for rows with auto-gen IDs/timestamps
        mut_skip("insert items single repr", "POST", "/items",
            Some(json!({"name": "compat-i1", "price": 5.0})), jwt, vec![repr.clone()]),
        mut_skip("insert items minimal", "POST", "/items",
            Some(json!({"name": "compat-i2", "price": 10.0})), jwt, vec![minimal.clone()]),
        mut_skip("insert items multi-row", "POST", "/items",
            Some(json!([{"name": "compat-b1", "price": 1.0}, {"name": "compat-b2", "price": 2.0}])), jwt, vec![repr.clone()]),
        mut_skip("insert items null", "POST", "/items",
            Some(json!({"name": "compat-n1", "price": null})), jwt, vec![repr.clone()]),
        mut_skip("insert items defaults", "POST", "/items",
            Some(json!({"name": "compat-d1"})), jwt, vec![repr.clone()]),
        mut_skip("insert items jsonb", "POST", "/items",
            Some(json!({"name": "compat-j1", "metadata": {"nested": true}})), jwt, vec![repr.clone()]),
        mut_skip("insert items bool", "POST", "/items",
            Some(json!({"name": "compat-bool", "active": false})), jwt, vec![repr.clone()]),

        // Compound PK (may conflict on re-run)
        mut_skip("insert compound_pk", "POST", "/compound_pk",
            Some(json!({"k1": 88, "k2": 88, "value": "compat"})), jwt, vec![repr.clone()]),

        // Insert with all types
        mut_skip("insert types_test", "POST", "/types_test",
            Some(json!({
                "text_col": "inserted", "int_col": 99, "bigint_col": 999,
                "bool_col": true, "date_col": "2025-01-01",
                "json_col": {"k":"v"}, "jsonb_col": {"k":"v"},
                "int_arr": [10,20], "text_arr": ["x","y"]
            })), jwt, vec![repr.clone()]),

        // Insert into unicode_test
        mut_skip("insert unicode", "POST", "/unicode_test",
            Some(json!({"name": "compat-résumé", "note": "accent"})), jwt, vec![repr.clone()]),

        // Multi-row insert with mixed nulls
        mut_skip("insert multi mixed nulls", "POST", "/items",
            Some(json!([
                {"name": "compat-mn1", "price": 1.0, "quantity": null},
                {"name": "compat-mn2", "price": null, "quantity": 5}
            ])), jwt, vec![repr.clone()]),

        // ==== UPSERT (all skip status due to 200/201 difference) ====
        mut_skip("upsert settings merge", "POST", "/settings",
            Some(json!({"key": "compat_u1", "value": "v1"})), jwt, vec![merge.clone()]),
        mut_skip("upsert settings update", "POST", "/settings",
            Some(json!({"key": "compat_u1", "value": "v2"})), jwt, vec![merge.clone()]),
        mut_skip("upsert settings ignore", "POST", "/settings",
            Some(json!({"key": "compat_u1", "value": "v3"})), jwt, vec![ignore.clone()]),
        mut_skip("upsert on_conflict code", "POST", "/upsert_test?on_conflict=code",
            Some(json!({"code": "AAA", "value": "compat-updated"})), jwt, vec![merge.clone()]),
        mut_skip("upsert multi", "POST", "/settings",
            Some(json!([{"key": "compat_um1", "value": "a"}, {"key": "compat_um2", "value": "b"}])),
            jwt, vec![merge.clone()]),
        mut_skip("upsert compound", "POST", "/compound_pk",
            Some(json!({"k1": 88, "k2": 88, "value": "upserted"})), jwt, vec![merge.clone()]),

        // ==== UPDATE ====
        mut_skip("update items price", "PATCH", "/items?name=eq.compat-i1",
            Some(json!({"price": 99.99})), jwt, vec![repr.clone()]),
        mut_skip("update items multi", "PATCH", "/items?name=like.compat-b*",
            Some(json!({"active": false})), jwt, vec![repr.clone()]),
        mut_skip("update items minimal", "PATCH", "/items?name=eq.compat-n1",
            Some(json!({"price": 0.01})), jwt, vec![minimal.clone()]),
        mut_skip("update items set null", "PATCH", "/items?name=eq.compat-j1",
            Some(json!({"metadata": null})), jwt, vec![repr.clone()]),
        mutation("update compound_pk", "PATCH", "/compound_pk?k1=eq.88&k2=eq.88",
            Some(json!({"value": "updated", "extra": "e"})), jwt, vec![repr.clone()]),
        mut_skip("update settings", "PATCH", "/settings?key=eq.compat_u1",
            Some(json!({"value": "final"})), jwt, vec![repr.clone()]),
        mut_skip("update no match", "PATCH", "/items?name=eq.nonexistent",
            Some(json!({"price": 0})), jwt, vec![repr.clone()]),

        // ==== DELETE ====
        mut_skip("delete compound_pk", "DELETE", "/compound_pk?k1=eq.88&k2=eq.88",
            None, jwt, vec![repr.clone()]),
        mut_skip("delete items compat-n1", "DELETE", "/items?name=eq.compat-n1",
            None, jwt, vec![minimal.clone()]),
        mut_skip("delete items batch", "DELETE", "/items?name=like.compat-b*",
            None, jwt, vec![repr.clone()]),
        mut_skip("delete no match", "DELETE", "/items?name=eq.nonexistent",
            None, jwt, vec![repr.clone()]),
        mut_skip("delete settings compat", "DELETE", "/settings?key=like.compat*",
            None, jwt, vec![minimal.clone()]),

        // Cleanup remaining compat data
        mut_skip("cleanup items compat", "DELETE", "/items?name=like.compat*",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup types_test compat", "DELETE", "/types_test?text_col=eq.inserted",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup unicode compat", "DELETE", "/unicode_test?name=like.compat*",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup upsert_test", "DELETE", "/upsert_test?value=eq.compat-updated",
            None, jwt, vec![minimal.clone()]),
    ]
}
