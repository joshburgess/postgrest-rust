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

        // ==== Profile mutations ====
        mut_skip("insert profile", "POST", "/profiles",
            Some(json!({"username": "compat-user", "email": "u@test.com", "age": 28})),
            jwt, vec![repr.clone()]),
        mut_skip("insert profile no email", "POST", "/profiles",
            Some(json!({"username": "compat-noemail", "age": 22})),
            jwt, vec![repr.clone()]),
        mut_skip("insert profile minimal", "POST", "/profiles",
            Some(json!({"username": "compat-min"})),
            jwt, vec![minimal.clone()]),
        mut_skip("update profile score", "PATCH", "/profiles?username=eq.compat-user",
            Some(json!({"score": 55.5})), jwt, vec![repr.clone()]),
        mut_skip("update profile set null", "PATCH", "/profiles?username=eq.compat-user",
            Some(json!({"bio": null, "age": null})), jwt, vec![repr.clone()]),

        // ==== Insert multi-row profiles ====
        mut_skip("insert profiles multi", "POST", "/profiles",
            Some(json!([
                {"username": "compat-m1", "score": 10},
                {"username": "compat-m2", "score": 20},
                {"username": "compat-m3", "score": 30}
            ])), jwt, vec![repr.clone()]),

        // ==== Delete with various filter types ====
        mut_skip("delete profiles by score", "DELETE", "/profiles?score=lt.35&username=like.compat-m*",
            None, jwt, vec![repr.clone()]),

        // ==== Insert into tasks ====
        mut_skip("insert task", "POST", "/tasks",
            Some(json!({"title": "compat-task", "project_id": 1, "assigned_to": 4})),
            jwt, vec![repr.clone()]),

        // ==== Insert with boolean variations ====
        mut_skip("insert items active false", "POST", "/items",
            Some(json!({"name": "compat-af", "active": false})), jwt, vec![repr.clone()]),
        mut_skip("insert items active true explicit", "POST", "/items",
            Some(json!({"name": "compat-at", "active": true, "price": 7.77})), jwt, vec![repr.clone()]),

        // ==== Insert with numeric edge cases ====
        mut_skip("insert items zero price", "POST", "/items",
            Some(json!({"name": "compat-z", "price": 0, "quantity": 0})), jwt, vec![repr.clone()]),
        mut_skip("insert items large price", "POST", "/items",
            Some(json!({"name": "compat-lp", "price": 99999.99})), jwt, vec![repr.clone()]),
        mut_skip("insert items negative qty", "POST", "/items",
            Some(json!({"name": "compat-nq", "quantity": -5})), jwt, vec![repr.clone()]),

        // ==== Insert employees (self-referencing FK) ====
        mut_skip("insert employee with mgr", "POST", "/employees",
            Some(json!({"name": "compat-emp1", "manager_id": 1})), jwt, vec![repr.clone()]),
        mut_skip("insert employee no mgr", "POST", "/employees",
            Some(json!({"name": "compat-emp2"})), jwt, vec![repr.clone()]),

        // ==== Insert into entities (arrays + JSONB) ====
        mut_skip("insert entity with arr", "POST", "/entities",
            Some(json!({"name": "compat-ent", "arr": ["x","y"], "data": {"k": "v"}})), jwt, vec![repr.clone()]),
        mut_skip("insert entity null arr", "POST", "/entities",
            Some(json!({"name": "compat-ent2", "arr": null, "data": null})), jwt, vec![repr.clone()]),

        // ==== Multi-row insert items ====
        mut_skip("insert items 3-row", "POST", "/items",
            Some(json!([
                {"name": "compat-3a", "price": 1.0},
                {"name": "compat-3b", "price": 2.0},
                {"name": "compat-3c", "price": 3.0}
            ])), jwt, vec![repr.clone()]),

        // ==== Update with various filter operators ====
        mut_skip("update items gt filter", "PATCH", "/items?name=like.compat-3*&price=gt.1",
            Some(json!({"quantity": 10})), jwt, vec![repr.clone()]),
        mut_skip("update items in filter", "PATCH", "/items?name=in.(compat-af,compat-at)",
            Some(json!({"quantity": 99})), jwt, vec![repr.clone()]),
        mut_skip("update items bool filter", "PATCH", "/items?active=eq.false&name=like.compat*",
            Some(json!({"active": true})), jwt, vec![repr.clone()]),

        // ==== Update set to specific values ====
        mut_skip("update profile email", "PATCH", "/profiles?username=eq.compat-user",
            Some(json!({"email": "new@test.com"})), jwt, vec![repr.clone()]),
        mut_skip("update profile active", "PATCH", "/profiles?username=eq.compat-noemail",
            Some(json!({"active": false})), jwt, vec![repr.clone()]),

        // ==== Update compound pk ====
        mutation("update compound after upsert", "PATCH", "/compound_pk?k1=eq.88&k2=eq.88",
            Some(json!({"extra": "final"})), jwt, vec![repr.clone()]),

        // ==== Upsert multi-row with mixed insert/update ====
        mut_skip("upsert settings 3-row", "POST", "/settings",
            Some(json!([
                {"key": "compat_s1", "value": "new1"},
                {"key": "compat_s2", "value": "new2"},
                {"key": "compat_s3", "value": "new3"}
            ])), jwt, vec![merge.clone()]),
        mut_skip("upsert settings update 2", "POST", "/settings",
            Some(json!([
                {"key": "compat_s1", "value": "upd1"},
                {"key": "compat_s2", "value": "upd2"}
            ])), jwt, vec![merge.clone()]),

        // ==== Insert into unicode_test ====
        mut_skip("insert unicode accent", "POST", "/unicode_test",
            Some(json!({"name": "compat-über", "note": "umlaut"})), jwt, vec![repr.clone()]),

        // ==== Delete with various filters ====
        mut_skip("delete items price filter", "DELETE", "/items?price=eq.0&name=like.compat*",
            None, jwt, vec![repr.clone()]),
        mut_skip("delete items active filter", "DELETE", "/items?active=eq.true&name=like.compat-3*",
            None, jwt, vec![repr.clone()]),
        mut_skip("delete employees compat", "DELETE", "/employees?name=like.compat*",
            None, jwt, vec![repr.clone()]),
        mut_skip("delete entities compat", "DELETE", "/entities?name=like.compat*",
            None, jwt, vec![repr.clone()]),

        // ==== Delete with or filter ====
        mut_skip("delete items or filter", "DELETE", "/items?or=(name.eq.compat-af,name.eq.compat-at)",
            None, jwt, vec![repr.clone()]),

        // ==== Delete with in filter ====
        mut_skip("delete items in filter", "DELETE", "/items?name=in.(compat-lp,compat-nq)",
            None, jwt, vec![repr.clone()]),

        // ==== Insert + read back verification ====
        mut_skip("insert settings verify", "POST", "/settings",
            Some(json!({"key": "compat_verify", "value": "check"})), jwt, vec![repr.clone()]),

        // ==== Orders table mutations ====
        mut_skip("insert order basic", "POST", "/orders",
            Some(json!({"customer": "compat-cust", "amount": 42.00})), jwt, vec![repr.clone()]),
        mut_skip("insert order with status", "POST", "/orders",
            Some(json!({"customer": "compat-cust", "amount": 100.00, "status": "shipped", "notes": "fast"})),
            jwt, vec![repr.clone()]),
        mut_skip("insert order minimal", "POST", "/orders",
            Some(json!({"customer": "compat-min", "amount": 0.01})), jwt, vec![minimal.clone()]),
        mut_skip("insert orders multi", "POST", "/orders",
            Some(json!([
                {"customer": "compat-batch", "amount": 10.00},
                {"customer": "compat-batch", "amount": 20.00},
                {"customer": "compat-batch", "amount": 30.00}
            ])), jwt, vec![repr.clone()]),
        mut_skip("update orders by customer", "PATCH", "/orders?customer=eq.compat-batch",
            Some(json!({"status": "completed"})), jwt, vec![repr.clone()]),
        mut_skip("update orders notes", "PATCH", "/orders?customer=eq.compat-cust&amount=eq.42",
            Some(json!({"notes": "updated note"})), jwt, vec![repr.clone()]),

        // ==== Logs table mutations ====
        mut_skip("insert log", "POST", "/logs",
            Some(json!({"level": "info", "message": "compat test", "context": {"test": true}})),
            jwt, vec![repr.clone()]),
        mut_skip("insert log minimal", "POST", "/logs",
            Some(json!({"message": "compat minimal"})), jwt, vec![repr.clone()]),
        mut_skip("insert logs multi", "POST", "/logs",
            Some(json!([
                {"level": "warn", "message": "compat-w1"},
                {"level": "error", "message": "compat-e1"},
                {"level": "debug", "message": "compat-d1"}
            ])), jwt, vec![repr.clone()]),
        mut_skip("update logs level", "PATCH", "/logs?message=like.compat*&level=eq.warn",
            Some(json!({"level": "info"})), jwt, vec![repr.clone()]),
        mut_skip("delete logs compat", "DELETE", "/logs?message=like.compat*",
            None, jwt, vec![repr.clone()]),

        // ==== More update patterns ====
        mut_skip("update items set metadata obj", "PATCH", "/items?name=like.compat-at*",
            Some(json!({"metadata": {"updated": true, "color": "red"}})), jwt, vec![repr.clone()]),
        mut_skip("update profiles score to zero", "PATCH", "/profiles?username=eq.compat-user",
            Some(json!({"score": 0})), jwt, vec![repr.clone()]),
        mut_skip("update items quantity batch", "PATCH", "/items?name=like.compat-3*",
            Some(json!({"quantity": 0, "active": false})), jwt, vec![repr.clone()]),

        // ==== Delete with complex filters ====
        mut_skip("delete orders by status", "DELETE", "/orders?status=eq.completed&customer=like.compat*",
            None, jwt, vec![repr.clone()]),
        mut_skip("delete orders remaining", "DELETE", "/orders?customer=like.compat*",
            None, jwt, vec![repr.clone()]),

        // ==== Upsert on upsert_test ====
        mut_skip("upsert upsert_test new", "POST", "/upsert_test?on_conflict=code",
            Some(json!({"code": "COMPAT1", "value": "new"})), jwt, vec![merge.clone()]),
        mut_skip("upsert upsert_test update", "POST", "/upsert_test?on_conflict=code",
            Some(json!({"code": "COMPAT1", "value": "updated"})), jwt, vec![merge.clone()]),
        mut_skip("upsert upsert_test ignore", "POST", "/upsert_test?on_conflict=code",
            Some(json!({"code": "COMPAT1", "value": "ignored"})), jwt, vec![ignore.clone()]),

        // Cleanup remaining compat data
        mut_skip("cleanup items", "DELETE", "/items?name=like.compat*",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup types_test", "DELETE", "/types_test?text_col=eq.inserted",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup unicode", "DELETE", "/unicode_test?name=like.compat*",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup upsert_test", "DELETE", "/upsert_test?value=eq.compat-updated",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup profiles", "DELETE", "/profiles?username=like.compat*",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup tasks", "DELETE", "/tasks?title=like.compat*",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup settings", "DELETE", "/settings?key=like.compat*",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup employees", "DELETE", "/employees?name=like.compat*",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup entities", "DELETE", "/entities?name=like.compat*",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup orders", "DELETE", "/orders?customer=like.compat*",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup logs", "DELETE", "/logs?message=like.compat*",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup upsert_test compat1", "DELETE", "/upsert_test?code=eq.COMPAT1",
            None, jwt, vec![minimal.clone()]),

        // ==== More insert patterns: orders with all fields ====
        mut_skip("insert order full", "POST", "/orders",
            Some(json!({"customer": "compat2-c", "amount": 555.55, "status": "shipped", "notes": "full fields"})),
            jwt, vec![repr.clone()]),
        mut_skip("insert order null notes", "POST", "/orders",
            Some(json!({"customer": "compat2-c", "amount": 1.00, "notes": null})),
            jwt, vec![repr.clone()]),

        // ==== Multi-row insert profiles ====
        mut_skip("insert profiles batch 4", "POST", "/profiles",
            Some(json!([
                {"username": "compat2-p1", "email": "p1@t.com", "age": 20, "score": 50},
                {"username": "compat2-p2", "age": 30},
                {"username": "compat2-p3", "bio": "tester"},
                {"username": "compat2-p4", "active": false, "score": 0}
            ])), jwt, vec![repr.clone()]),

        // ==== Update multiple columns at once ====
        mut_skip("update profile multi cols", "PATCH", "/profiles?username=eq.compat2-p1",
            Some(json!({"email": "new@t.com", "age": 21, "score": 51, "bio": "updated"})),
            jwt, vec![repr.clone()]),
        mut_skip("update profile active+score", "PATCH", "/profiles?username=eq.compat2-p4",
            Some(json!({"active": true, "score": 99.99})), jwt, vec![repr.clone()]),

        // ==== Insert logs with various contexts ====
        mut_skip("insert log nested ctx", "POST", "/logs",
            Some(json!({"level": "info", "message": "compat2-nested", "context": {"a": {"b": {"c": 1}}}})),
            jwt, vec![repr.clone()]),
        mut_skip("insert log array ctx", "POST", "/logs",
            Some(json!({"level": "debug", "message": "compat2-array", "context": {"tags": ["a","b","c"]}})),
            jwt, vec![repr.clone()]),
        mut_skip("insert log empty ctx", "POST", "/logs",
            Some(json!({"message": "compat2-empty-ctx"})), jwt, vec![repr.clone()]),

        // ==== Update with not filter ====
        mut_skip("update items not.active", "PATCH", "/items?name=like.compat2*&active=not.eq.false",
            Some(json!({"quantity": 777})), jwt, vec![repr.clone()]),

        // ==== Delete with not.is.null ====
        mut_skip("delete orders not null notes", "DELETE", "/orders?notes=not.is.null&customer=like.compat2*",
            None, jwt, vec![repr.clone()]),

        // ==== Insert into numbered (simple int table) ====
        mut_skip("insert numbered", "POST", "/numbered",
            Some(json!({"val": 9999})), jwt, vec![repr.clone()]),

        // ==== Insert multi-row logs ====
        mut_skip("insert logs batch 5", "POST", "/logs",
            Some(json!([
                {"message": "compat2-1"},
                {"message": "compat2-2"},
                {"message": "compat2-3"},
                {"message": "compat2-4"},
                {"message": "compat2-5"}
            ])), jwt, vec![repr.clone()]),

        // ==== Update with in filter ====
        mut_skip("update profiles in filter", "PATCH", "/profiles?username=in.(compat2-p2,compat2-p3)",
            Some(json!({"score": 42})), jwt, vec![repr.clone()]),

        // ==== Upsert profiles (unique on username) ====
        mut_skip("upsert profile new", "POST", "/profiles?on_conflict=username",
            Some(json!({"username": "compat2-ups", "score": 10})), jwt, vec![merge.clone()]),
        mut_skip("upsert profile update", "POST", "/profiles?on_conflict=username",
            Some(json!({"username": "compat2-ups", "score": 20})), jwt, vec![merge.clone()]),

        // ==== Delete with gt/lt ====
        mut_skip("delete logs by id range", "DELETE", "/logs?message=like.compat2*",
            None, jwt, vec![repr.clone()]),

        // ==== Cleanup round 2 ====
        mut_skip("cleanup2 orders", "DELETE", "/orders?customer=like.compat2*",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup2 profiles", "DELETE", "/profiles?username=like.compat2*",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup2 numbered", "DELETE", "/numbered?val=eq.9999",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup2 items", "DELETE", "/items?name=like.compat2*",
            None, jwt, vec![minimal.clone()]),

        // ==== Round 3: more mutation patterns to match PostgREST coverage ====

        // Insert with every column explicit
        mut_skip("insert order explicit", "POST", "/orders",
            Some(json!({"customer": "compat3-x", "amount": 77.77, "status": "pending", "notes": "explicit"})),
            jwt, vec![repr.clone()]),

        // Multi-row with different column sets per row
        mut_skip("insert logs varied cols", "POST", "/logs",
            Some(json!([
                {"level": "info", "message": "compat3-a", "context": {"k": 1}},
                {"message": "compat3-b"},
                {"level": "error", "message": "compat3-c"}
            ])), jwt, vec![repr.clone()]),

        // Update to set all nullable columns to null
        mut_skip("update order all null", "PATCH", "/orders?customer=eq.compat3-x",
            Some(json!({"notes": null})), jwt, vec![repr.clone()]),

        // Update with neq filter
        mut_skip("update orders neq filter", "PATCH", "/orders?customer=eq.compat3-x&status=neq.shipped",
            Some(json!({"status": "shipped"})), jwt, vec![repr.clone()]),

        // Delete with like filter
        mut_skip("delete orders like", "DELETE", "/orders?customer=like.compat3*",
            None, jwt, vec![repr.clone()]),

        // Insert into compound_pk with different values
        mut_skip("insert compound 77", "POST", "/compound_pk",
            Some(json!({"k1": 77, "k2": 77, "value": "compat3", "extra": "e3"})),
            jwt, vec![repr.clone()]),

        // Update compound_pk extra only
        mutation("update compound 77", "PATCH", "/compound_pk?k1=eq.77&k2=eq.77",
            Some(json!({"extra": "updated3"})), jwt, vec![repr.clone()]),

        // Delete compound_pk
        mut_skip("delete compound 77", "DELETE", "/compound_pk?k1=eq.77",
            None, jwt, vec![repr.clone()]),

        // Insert unicode with various characters
        mut_skip("insert unicode emoji-safe", "POST", "/unicode_test",
            Some(json!({"name": "compat3-tëst", "note": "diacritic"})),
            jwt, vec![repr.clone()]),

        // Upsert settings batch
        mut_skip("upsert settings batch 3", "POST", "/settings",
            Some(json!([
                {"key": "compat3_a", "value": "1"},
                {"key": "compat3_b", "value": "2"},
                {"key": "compat3_c", "value": "3"}
            ])), jwt, vec![merge.clone()]),

        // Update settings batch
        mut_skip("update settings compat3", "PATCH", "/settings?key=like.compat3*",
            Some(json!({"value": "updated"})), jwt, vec![repr.clone()]),

        // Insert into types_test with all non-null fields
        mut_skip("insert types full", "POST", "/types_test",
            Some(json!({
                "text_col": "compat3", "int_col": 42, "bigint_col": 420,
                "float_col": 3.5, "double_col": 2.75, "numeric_col": 99.99,
                "bool_col": false, "date_col": "2025-12-31",
                "time_col": "23:59:59",
                "json_col": {"compat": 3}, "jsonb_col": {"compat": 3},
                "int_arr": [7,8,9], "text_arr": ["compat","three"]
            })), jwt, vec![repr.clone()]),

        // Insert into employees chain (self-ref)
        mut_skip("insert emp chain", "POST", "/employees",
            Some(json!({"name": "compat3-mgr"})), jwt, vec![repr.clone()]),

        // Insert entity with large array
        mut_skip("insert entity large arr", "POST", "/entities",
            Some(json!({"name": "compat3-big", "arr": ["a","b","c","d","e","f","g"], "data": {"big": true}})),
            jwt, vec![repr.clone()]),

        // Delete everything matching compat3
        mut_skip("cleanup3 logs", "DELETE", "/logs?message=like.compat3*",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup3 unicode", "DELETE", "/unicode_test?name=like.compat3*",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup3 settings", "DELETE", "/settings?key=like.compat3*",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup3 types", "DELETE", "/types_test?text_col=eq.compat3",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup3 employees", "DELETE", "/employees?name=like.compat3*",
            None, jwt, vec![minimal.clone()]),
        mut_skip("cleanup3 entities", "DELETE", "/entities?name=like.compat3*",
            None, jwt, vec![minimal.clone()]),
    ]
}
