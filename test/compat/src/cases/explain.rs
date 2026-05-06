use super::*;
use crate::TestCase;

fn explain(name: &'static str, path: &'static str, jwt: &str) -> TestCase {
    let mut tc = g_status_only(name, path, jwt);
    tc.headers
        .push(("Accept", "application/vnd.pgrst.plan+json".to_string()));
    tc
}

pub fn cases(jwt: &str) -> Vec<TestCase> {
    vec![
        // ==== Basic EXPLAIN on various tables ====
        explain("EXPLAIN authors", "/authors", jwt),
        explain("EXPLAIN books", "/books?order=id.asc", jwt),
        explain("EXPLAIN tags", "/tags", jwt),
        explain("EXPLAIN settings", "/settings", jwt),
        explain("EXPLAIN items", "/items", jwt),
        explain("EXPLAIN employees", "/employees", jwt),
        explain("EXPLAIN compound_pk", "/compound_pk", jwt),
        explain("EXPLAIN entities", "/entities", jwt),
        explain("EXPLAIN numbered", "/numbered", jwt),
        explain("EXPLAIN types_test", "/types_test", jwt),
        explain("EXPLAIN profiles", "/profiles", jwt),
        explain("EXPLAIN orders", "/orders", jwt),
        explain("EXPLAIN logs", "/logs", jwt),
        explain("EXPLAIN tasks", "/tasks", jwt),
        explain("EXPLAIN projects", "/projects", jwt),
        explain("EXPLAIN unicode_test", "/unicode_test", jwt),
        explain("EXPLAIN documents", "/documents", jwt),
        // ==== EXPLAIN with filters ====
        explain("EXPLAIN filter eq", "/books?pages=gt.300", jwt),
        explain("EXPLAIN filter in", "/authors?id=in.(1,2)", jwt),
        explain("EXPLAIN filter is.null", "/authors?bio=is.null", jwt),
        explain("EXPLAIN filter like", "/books?title=like.*Rust*", jwt),
        explain(
            "EXPLAIN filter or",
            "/authors?or=(name.eq.Alice,name.eq.Bob)",
            jwt,
        ),
        // ==== EXPLAIN with limit/offset ====
        explain("EXPLAIN with limit", "/numbered?limit=10", jwt),
        explain("EXPLAIN with offset", "/numbered?limit=5&offset=50", jwt),
        // ==== EXPLAIN with ordering ====
        explain("EXPLAIN with order", "/books?order=pages.desc", jwt),
        explain(
            "EXPLAIN with order nulls",
            "/books?order=published.asc.nullsfirst",
            jwt,
        ),
        // ==== EXPLAIN with select ====
        explain("EXPLAIN with select", "/authors?select=name", jwt),
        explain(
            "EXPLAIN with select multi",
            "/books?select=title,pages",
            jwt,
        ),
        // ==== EXPLAIN with embedding ====
        explain(
            "EXPLAIN embed O2M",
            "/authors?select=name,books(title)",
            jwt,
        ),
        explain(
            "EXPLAIN embed M2O",
            "/books?select=title,authors(name)",
            jwt,
        ),
        explain(
            "EXPLAIN embed M2M",
            "/books?select=title,tags(name)&id=eq.1",
            jwt,
        ),
        explain(
            "EXPLAIN embed !inner",
            "/authors?select=name,books!inner(title)",
            jwt,
        ),
        explain(
            "EXPLAIN embed spread",
            "/books?select=title,...authors(name)",
            jwt,
        ),
        // ==== EXPLAIN with complex queries ====
        explain(
            "EXPLAIN complex",
            "/books?pages=gt.300&order=title.asc&limit=2",
            jwt,
        ),
        explain(
            "EXPLAIN or+filter",
            "/items?or=(price.gt.20,quantity.gt.100)&active=eq.true",
            jwt,
        ),
        explain("EXPLAIN count", "/numbered?order=id.asc&limit=5", jwt),
        // ==== EXPLAIN on views ====
        explain("EXPLAIN view", "/authors_with_books", jwt),
        explain("EXPLAIN simple_items", "/simple_items", jwt),
        // ==== EXPLAIN on RPC ====
        // Note: PostgREST doesn't support EXPLAIN on RPC via GET with plan header.
        // Our server does. Status-only comparison handles this gracefully.

        // ==== EXPLAIN with JSON operators ====
        explain("EXPLAIN json cs", "/entities?data=cs.{\"x\":1}", jwt),
        explain("EXPLAIN json select", "/entities?select=name,data->x", jwt),
    ]
}
