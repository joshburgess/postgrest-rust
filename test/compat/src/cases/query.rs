use super::*;
use crate::TestCase;

pub fn cases(jwt: &str) -> Vec<TestCase> {
    vec![
        // ---- Basic reads ----
        g("/authors?order=id.asc", jwt),
        g("/books?order=id.asc", jwt),
        g("/tags?order=id.asc", jwt),
        g("/articles?order=id.asc", jwt),
        g("/settings?order=key.asc", jwt),
        g("/types_test?order=id.asc", jwt),
        g("/employees?order=id.asc", jwt),
        g("/compound_pk?order=k1.asc,k2.asc", jwt),
        g("/entities?order=id.asc", jwt),
        g("/items?order=id.asc", jwt),
        g("/documents?order=id.asc", jwt),

        // ---- Select columns ----
        g("/authors?select=name&order=id.asc", jwt),
        g("/authors?select=id,name&order=id.asc", jwt),
        g("/books?select=title,pages&order=id.asc", jwt),
        g("/authors?select=*&order=id.asc", jwt),
        g("/items?select=name,price&order=id.asc", jwt),

        // ---- Select with cast ----
        g("/authors?select=id::text,name&order=id.asc", jwt),

        // ---- eq filter ----
        g("/authors?name=eq.Alice", jwt),
        g("/authors?id=eq.1", jwt),
        g("/books?title=eq.Learning Rust", jwt),
        g("/types_test?bool_col=eq.true", jwt),
        g("/types_test?int_col=eq.1", jwt),
        g("/types_test?text_col=eq.hello", jwt),
        g("/settings?key=eq.theme", jwt),

        // ---- neq filter ----
        g("/authors?name=neq.Alice&order=id.asc", jwt),
        g("/types_test?bool_col=neq.true&order=id.asc", jwt),

        // ---- gt / gte / lt / lte ----
        g("/books?pages=gt.400&order=id.asc", jwt),
        g("/books?pages=gte.400&order=id.asc", jwt),
        g("/books?pages=lt.300&order=id.asc", jwt),
        g("/books?pages=lte.300&order=id.asc", jwt),
        g("/types_test?int_col=gt.1&order=id.asc", jwt),
        g("/types_test?numeric_col=gte.20&order=id.asc", jwt),
        g("/types_test?date_col=gt.2024-03-01&order=id.asc", jwt),
        g("/types_test?date_col=lt.2024-03-01&order=id.asc", jwt),

        // ---- like / ilike ----
        g("/authors?name=like.A*&order=id.asc", jwt),
        g("/authors?name=like.*o*&order=id.asc", jwt),
        g("/authors?name=ilike.alice&order=id.asc", jwt),
        g("/authors?name=ilike.*OB*&order=id.asc", jwt),
        g("/books?title=like.Learn*&order=id.asc", jwt),
        g("/books?title=ilike.*rust*&order=id.asc", jwt),

        // ---- in ----
        g("/authors?id=in.(1,2)&order=id.asc", jwt),
        g("/authors?id=in.(1,2,3)&order=id.asc", jwt),
        g("/authors?name=in.(Alice,Bob)&order=id.asc", jwt),
        g("/books?pages=in.(350,500)&order=id.asc", jwt),
        g("/types_test?int_col=in.(1,2)&order=id.asc", jwt),

        // ---- is ----
        g("/authors?bio=is.null&order=id.asc", jwt),
        g("/types_test?text_col=is.null&order=id.asc", jwt),
        g("/types_test?bool_col=is.true&order=id.asc", jwt),
        g("/types_test?bool_col=is.false&order=id.asc", jwt),
        g("/types_test?bool_col=is.null&order=id.asc", jwt),
        g("/books?published=is.null&order=id.asc", jwt),

        // ---- not.X negation ----
        g("/authors?bio=not.is.null&order=id.asc", jwt),
        g("/authors?name=not.eq.Alice&order=id.asc", jwt),
        g("/books?pages=not.gt.400&order=id.asc", jwt),
        g("/authors?id=not.in.(1,2)&order=id.asc", jwt),
        g("/types_test?bool_col=not.is.null&order=id.asc", jwt),
        g("/types_test?bool_col=not.is.true&order=id.asc", jwt),
        g("/books?title=not.like.*Rust*&order=id.asc", jwt),

        // ---- Ordering ----
        g("/authors?order=name.asc&id=in.(1,2,3)", jwt),
        g("/authors?order=name.desc&id=in.(1,2,3)", jwt),
        g("/books?order=pages.asc&order=title.asc", jwt),
        g("/books?order=pages.desc.nullslast", jwt),
        g("/books?order=pages.asc.nullsfirst", jwt),
        g("/books?order=published.desc.nullslast", jwt),

        // ---- Limit / Offset ----
        g("/authors?order=id.asc&limit=1", jwt),
        g("/authors?order=id.asc&limit=2", jwt),
        g("/authors?order=id.asc&limit=1&offset=1", jwt),
        g("/authors?order=id.asc&limit=1&offset=2", jwt),
        g("/numbered?order=id.asc&limit=5", jwt),
        g("/numbered?order=id.asc&limit=5&offset=95", jwt),

        // ---- Multiple filters (AND) ----
        g("/books?pages=gt.300&pages=lt.500&order=id.asc", jwt),
        g("/books?author_id=eq.1&pages=gt.400", jwt),
        g("/types_test?int_col=gt.0&text_col=eq.hello", jwt),

        // ---- Empty results ----
        g("/authors?name=eq.Nobody", jwt),
        g("/books?pages=gt.10000", jwt),
        g("/types_test?int_col=eq.999", jwt),

        // ---- Contains / Contained-in (arrays) ----
        g("/entities?arr=cs.{a,b}&order=id.asc", jwt),
        g("/entities?arr=cs.{b}&order=id.asc", jwt),
        g("/entities?arr=cd.{a,b,c,d,e}&order=id.asc", jwt),

        // ---- Overlaps (arrays) ----
        g("/entities?arr=ov.{a,d}&order=id.asc", jwt),
        g("/entities?arr=ov.{z}&order=id.asc", jwt),

        // ---- Views ----
        g("/authors_with_books?order=id.asc", jwt),
        g("/simple_items?order=id.asc", jwt),

        // ---- 404 for nonexistent table ----
        g_status_only("GET /nonexistent (404)", "/nonexistent", jwt),
    ]
}
