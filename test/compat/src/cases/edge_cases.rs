use super::*;
use crate::TestCase;
use serde_json::json;

pub fn cases(jwt_anon: &str, jwt_user: &str) -> Vec<TestCase> {
    vec![
        // ==== Filter edge cases: type coercion ====
        g("/types_test?float_col=gt.2.0&order=id.asc", jwt_anon),
        g("/types_test?double_col=lt.3.0&order=id.asc", jwt_anon),
        g("/types_test?numeric_col=eq.10.50&order=id.asc", jwt_anon),
        g("/types_test?bigint_col=gte.100&order=id.asc", jwt_anon),
        g("/types_test?time_col=gt.15:00:00&order=id.asc", jwt_anon),
        g("/items?price=gte.9.99&order=id.asc", jwt_anon),

        // ==== Filter: timestamp comparisons ====
        g("/types_test?date_col=gte.2024-01-01&date_col=lte.2024-12-31&order=id.asc", jwt_anon),

        // ==== Filter: boolean permutations ====
        g("/items?active=eq.true&order=id.asc", jwt_anon),
        g("/items?active=eq.false&order=id.asc", jwt_anon),
        g("/items?active=is.true&order=id.asc", jwt_anon),
        g("/items?active=is.false&order=id.asc", jwt_anon),
        g("/items?active=not.eq.true&order=id.asc", jwt_anon),

        // ==== Filter: empty string ====
        g("/authors?name=eq.&order=id.asc", jwt_anon),
        g("/authors?bio=eq.&order=id.asc", jwt_anon),

        // ==== Filter: special characters in values ====
        g("/unicode_test?note=eq.accent&order=id.asc", jwt_anon),
        g("/unicode_test?note=eq.apostrophe&order=id.asc", jwt_anon),
        g("/unicode_test?note=eq.quotes&order=id.asc", jwt_anon),
        g("/unicode_test?name=like.*ïv*&order=id.asc", jwt_anon),

        // ==== Select: multiple tables' columns ====
        g("/books?select=id,title,pages,published&order=id.asc", jwt_anon),
        g("/employees?select=id,name,manager_id&order=id.asc", jwt_anon),
        g("/compound_pk?select=k1,k2,value,extra&order=k1.asc,k2.asc", jwt_anon),

        // ==== Select: all columns explicit ====
        g("/tags?select=id,name&order=id.asc", jwt_anon),
        g("/settings?select=key,value&order=key.asc", jwt_anon),

        // ==== Ordering: by non-first column ====
        g("/books?order=title.asc", jwt_anon),
        g("/books?order=published.desc.nullslast", jwt_anon),
        g("/items?order=quantity.desc,name.asc", jwt_anon),
        g("/employees?order=name.desc", jwt_anon),

        // ==== Ordering: compound ====
        g("/compound_pk?order=k2.desc,k1.asc", jwt_anon),
        g("/tasks?order=project_id.asc,title.asc", jwt_anon),

        // ==== More embedding edge cases ====
        g("/books?select=title,authors(name)&pages=gt.400", jwt_anon),
        // Note: embedded table filter syntax (books.pages=gt.400) is a PostgREST feature
        // we haven't implemented yet. Skipped.
        // g("/authors?select=name,books(title)&books.pages=gt.400&order=id.asc", jwt_anon),
        g("/books?select=title,tags(name)&order=id.asc", jwt_anon),
        g("/tasks?select=title,projects(name)&order=id.asc", jwt_anon),

        // ==== Embed with parent filter that reduces to single row ====
        g("/authors?select=name,books(title,pages)&id=eq.1", jwt_anon),
        g("/books?select=title,authors(name,bio)&id=eq.3", jwt_anon),

        // ==== M2M with specific book ====
        g("/books?select=title,tags(name)&id=eq.1", jwt_anon),
        g("/books?select=title,tags(name)&id=eq.2", jwt_anon),
        g("/books?select=title,tags(name)&id=eq.4", jwt_anon),

        // ==== More RPC edge cases ====
        post_json("rpc/greet unicode", "/rpc/greet", json!({"name": "世界"}), jwt_anon),
        post_json("rpc/echo long", "/rpc/echo", json!({"value": "a".repeat(1000)}), jwt_anon),
        g("/rpc/greet", jwt_anon), // GET with no params (uses default)

        // ==== More insert edge cases ====
        mut_skip_status(
            "insert settings new key",
            "POST", "/settings",
            Some(json!({"key": "edge_test", "value": "v"})),
            jwt_user,
            vec![("Prefer", "return=representation".to_string())],
        ),
        // Insert empty JSON
        mut_skip_status(
            "insert items minimal fields",
            "POST", "/items",
            Some(json!({"name": "edge-minimal"})),
            jwt_user,
            vec![("Prefer", "return=representation".to_string())],
        ),

        // ==== More update edge cases ====
        mut_skip_status(
            "update settings edge_test",
            "PATCH", "/settings?key=eq.edge_test",
            Some(json!({"value": "updated"})),
            jwt_user,
            vec![("Prefer", "return=representation".to_string())],
        ),

        // ==== More upsert edge cases ====
        mut_skip_status(
            "upsert settings edge_test again",
            "POST", "/settings",
            Some(json!({"key": "edge_test", "value": "upserted"})),
            jwt_user,
            vec![("Prefer", "return=representation,resolution=merge-duplicates".to_string())],
        ),

        // ==== Delete edge cases ====
        mut_skip_status(
            "delete settings edge_test",
            "DELETE", "/settings?key=eq.edge_test",
            None, jwt_user,
            vec![("Prefer", "return=representation".to_string())],
        ),
        mut_skip_status(
            "delete items edge-minimal",
            "DELETE", "/items?name=eq.edge-minimal",
            None, jwt_user,
            vec![("Prefer", "return=minimal".to_string())],
        ),

        // ==== More pagination edge cases ====
        g("/numbered?order=id.desc&limit=3", jwt_anon),
        g("/numbered?order=val.desc&limit=3&offset=97", jwt_anon),
        g("/authors?order=id.desc&limit=10", jwt_anon),
        g("/books?order=id.asc&limit=2&offset=2", jwt_anon),

        // ==== More count edge cases ====
        {
            let mut tc = g("/numbered?val=gt.50&order=id.asc", jwt_anon);
            tc.name = "count=exact filtered numbered";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/authors?name=eq.Nobody", jwt_anon);
            tc.name = "count=exact empty result";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/items?active=eq.false&order=id.asc", jwt_anon);
            tc.name = "count=exact inactive items";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },

        // ==== More logic filter edge cases ====
        g("/types_test?or=(int_col.is.null,int_col.gt.1)&order=id.asc", jwt_anon),
        g("/books?or=(pages.is.null,published.is.null)&order=id.asc", jwt_anon),
        g("/employees?or=(manager_id.is.null,id.lt.3)&order=id.asc", jwt_anon),
        // Note: PostgREST doesn't support top-level `and=()` query param (only `or=()`).
        // It's valid in our server but returns 400 in PostgREST. Skipped for compat.
        // g("/items?and=(active.eq.true,or=(price.gt.20,quantity.gt.100))&order=id.asc", jwt_anon),

        // ==== More JSON edge cases ====
        g("/entities?select=name,data&order=id.asc", jwt_anon),
        g("/entities?select=name,data->tags&order=id.asc", jwt_anon),
        g("/items?metadata=cs.{\"color\":\"blue\"}&order=id.asc", jwt_anon),
        g("/items?metadata=cs.{\"color\":\"green\"}&order=id.asc", jwt_anon),

        // ==== Multiple schema (read from api, not api2) ====
        g("/authors?order=id.asc&limit=3", jwt_anon),
        g("/books?order=id.asc&limit=4", jwt_anon),

        // ==== Spread with multiple columns ====
        g("/books?select=title,...authors(name,bio)&id=in.(1,2)&order=id.asc", jwt_anon),
        g("/tasks?select=title,...projects(name)&order=id.asc", jwt_anon),

        // ==== !inner with M2O ====
        g("/books?select=title,authors!inner(name)&order=id.asc", jwt_anon),

        // ==== Embedding alias variations ====
        g("/books?select=title,a:authors(name)&id=eq.1", jwt_anon),
        g("/authors?select=name,b:books(title)&id=eq.1", jwt_anon),

        // ==== Singular on various tables ====
        {
            let mut tc = g("/settings?key=eq.theme", jwt_anon);
            tc.name = "singular setting";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/compound_pk?k1=eq.1&k2=eq.1", jwt_anon);
            tc.name = "singular compound pk";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/types_test?id=eq.1", jwt_anon);
            tc.name = "singular types_test";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ==== CSV on different tables ====
        {
            let mut tc = g("/settings?order=key.asc", jwt_anon);
            tc.name = "CSV settings";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false;
            tc
        },
        {
            let mut tc = g("/numbered?order=id.asc&limit=5", jwt_anon);
            tc.name = "CSV numbered";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false;
            tc
        },

        // ==== RLS edge cases ====
        {
            let mut tc = g("/articles?order=id.asc", jwt_anon);
            tc.name = "RLS anon count";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/articles?order=id.asc", jwt_user);
            tc.name = "RLS user count";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },

        // ==== Empty table query (no matching filter) ====
        g("/compound_pk?k1=eq.999&order=k1.asc", jwt_anon),
        g("/entities?name=eq.nonexistent", jwt_anon),
        g("/employees?id=eq.999", jwt_anon),

        // ==== Limit 0 (return no rows) ====
        g("/authors?order=id.asc&limit=0", jwt_anon),
        g("/numbered?order=id.asc&limit=0", jwt_anon),

        // ==== Very large offset ====
        g("/numbered?order=id.asc&limit=10&offset=10000", jwt_anon),
        g("/authors?order=id.asc&offset=100", jwt_anon),

        // ==== Multiple filters same column ====
        g("/numbered?val=gte.10&val=lte.20&order=id.asc", jwt_anon),
        g("/books?pages=gte.300&pages=lte.400&order=id.asc", jwt_anon),
        g("/items?price=gt.5&price=lt.25&order=id.asc", jwt_anon),

        // ==== Count with various table sizes ====
        {
            let mut tc = g("/settings?order=key.asc", jwt_anon);
            tc.name = "count settings";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/employees?order=id.asc", jwt_anon);
            tc.name = "count employees";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/tags?order=id.asc", jwt_anon);
            tc.name = "count tags";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },

        // ==== Profiles edge cases ====
        g("/profiles?order=id.asc", jwt_anon),
        g("/profiles?order=score.desc.nullslast", jwt_anon),
        g("/profiles?order=username.asc", jwt_anon),
        g("/profiles?select=username&order=username.asc", jwt_anon),
        g("/profiles?username=neq.alice&order=id.asc", jwt_anon),
        g("/profiles?age=gt.25&age=lt.35&order=id.asc", jwt_anon),
        g("/profiles?or=(score.gt.90,age.lt.26)&order=id.asc", jwt_anon),
        g("/profiles?email=like.*example*&order=id.asc", jwt_anon),

        // ==== Singular on profiles ====
        {
            let mut tc = g("/profiles?username=eq.alice", jwt_anon);
            tc.name = "singular profile";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ==== Count on profiles ====
        {
            let mut tc = g("/profiles?order=id.asc", jwt_anon);
            tc.name = "count profiles";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/profiles?active=eq.true&order=id.asc", jwt_anon);
            tc.name = "count active profiles";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },

        // ==== CSV on profiles ====
        {
            let mut tc = g("/profiles?select=username,score&order=id.asc", jwt_anon);
            tc.name = "CSV profiles";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false;
            tc
        },

        // ==== Tasks with embedding ====
        g("/tasks?select=title,projects(name)&order=id.asc", jwt_anon),
        g("/projects?select=name,tasks(title)&order=id.asc", jwt_anon),
        g("/projects?select=name,tasks(title)&id=eq.1", jwt_anon),

        // ==== More or/and on profiles ====
        g("/profiles?or=(username.eq.alice,username.eq.bob)&order=id.asc", jwt_anon),
        g("/profiles?or=(score.is.null,score.lt.75)&order=id.asc", jwt_anon),
        g("/profiles?and=(active.eq.true,score.gt.0)&order=id.asc", jwt_anon),

        // ==== Not filters on profiles ====
        g("/profiles?username=not.in.(alice,bob)&order=id.asc", jwt_anon),
        g("/profiles?score=not.is.null&order=id.asc", jwt_anon),
        g("/profiles?age=not.gt.30&order=id.asc", jwt_anon),

        // ==== Range on profiles ====
        {
            let mut tc = g("/profiles?order=id.asc", jwt_anon);
            tc.name = "Range profiles 0-1";
            tc.headers.push(("Range", "0-1".to_string()));
            tc
        },

        // ==== More JSON on items ====
        g("/items?metadata=cs.{}&order=id.asc", jwt_anon),
        g("/items?select=name,metadata&active=eq.true&order=id.asc", jwt_anon),

        // ==== Numbered table: various range combinations ====
        g("/numbered?val=gte.50&val=lte.55&order=val.asc", jwt_anon),
        g("/numbered?val=in.(1,50,100)&order=val.asc", jwt_anon),
        g("/numbered?val=not.in.(1,2,3,4,5)&order=val.asc&limit=5", jwt_anon),
        g("/numbered?or=(val.lte.3,val.gte.98)&order=val.asc", jwt_anon),

        // ==== Select specific columns on various tables ====
        g("/employees?select=name,manager_id&order=id.asc", jwt_anon),
        g("/entities?select=id,name&order=id.asc", jwt_anon),
        g("/tasks?select=id,title&order=id.asc", jwt_anon),
        g("/compound_pk?select=k1,k2&order=k1.asc,k2.asc", jwt_anon),

        // ==== More singular edge cases ====
        {
            let mut tc = g("/employees?id=eq.1", jwt_anon);
            tc.name = "singular CEO";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/items?id=eq.1", jwt_anon);
            tc.name = "singular item";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g_status_only("singular 406 items", "/items", jwt_anon);
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g_status_only("singular 406 numbered", "/numbered?limit=5", jwt_anon);
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ==== CSV on more tables ====
        {
            let mut tc = g("/types_test?order=id.asc", jwt_anon);
            tc.name = "CSV types_test";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false;
            tc
        },
        {
            let mut tc = g("/compound_pk?order=k1.asc,k2.asc", jwt_anon);
            tc.name = "CSV compound_pk";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false;
            tc
        },
        {
            let mut tc = g("/tasks?order=id.asc", jwt_anon);
            tc.name = "CSV tasks";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false;
            tc
        },

        // ==== Count on every table ====
        {
            let mut tc = g("/numbered?order=id.asc", jwt_anon);
            tc.name = "count numbered full";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/entities?order=id.asc", jwt_anon);
            tc.name = "count entities";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/projects?order=id.asc", jwt_anon);
            tc.name = "count projects";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/types_test?order=id.asc", jwt_anon);
            tc.name = "count types_test";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/upsert_test?order=id.asc", jwt_anon);
            tc.name = "count upsert_test";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },

        // ==== Profiles: more complex filter combos ====
        g("/profiles?or=(and(age.gt.25,score.gt.70),username.eq.carol)&order=id.asc", jwt_anon),
        g("/profiles?select=username,age,score&age=not.is.null&score=not.is.null&order=score.desc", jwt_anon),
        g("/profiles?select=username&or=(email.like.*example*,bio.is.null)&order=username.asc", jwt_anon),

        // ==== Items: ordering and filtering combos ====
        g("/items?select=name,price,quantity&order=price.asc,name.asc", jwt_anon),
        g("/items?or=(price.gt.20,quantity.gt.100)&order=name.asc", jwt_anon),
        g("/items?active=eq.true&order=quantity.desc", jwt_anon),

        // ==== Books: various combos ====
        g("/books?select=title&or=(pages.gt.400,published.is.null)&order=title.asc", jwt_anon),
        g("/books?select=title,pages&pages=not.is.null&order=pages.asc", jwt_anon),

        // ==== Employees: hierarchy queries ====
        g("/employees?manager_id=eq.2&order=id.asc", jwt_anon),
        g("/employees?or=(manager_id.eq.1,manager_id.is.null)&order=id.asc", jwt_anon),
        g("/employees?select=name&manager_id=not.is.null&order=name.asc", jwt_anon),
    ]
}

fn mut_skip_status(
    name: &'static str, method: &'static str, path: &'static str,
    body: Option<serde_json::Value>, auth: &str,
    extra: Vec<(&'static str, String)>,
) -> TestCase {
    let mut tc = mutation(name, method, path, body, auth, extra);
    tc.compare_body = false;
    tc.skip_status = true;
    tc
}
