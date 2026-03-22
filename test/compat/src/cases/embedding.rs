use super::*;
use crate::TestCase;

pub fn cases(jwt: &str) -> Vec<TestCase> {
    vec![
        // ---- One-to-many ----
        g("/authors?select=name,books(title)&name=eq.Alice", jwt),
        g("/authors?select=name,books(title)&name=eq.Bob", jwt),
        g("/authors?select=name,books(title)&name=eq.Carol", jwt), // no books
        g("/authors?select=name,books(title,pages)&order=id.asc&id=in.(1,2,3)", jwt),

        // ---- Many-to-one ----
        g("/books?select=title,authors(name)&id=eq.1", jwt),
        g("/books?select=title,authors(name,bio)&order=id.asc", jwt),

        // ---- Aliased embed ----
        g("/books?select=title,author:authors(name)&id=eq.1", jwt),

        // ---- Nested embedding ----
        g("/authors?select=name,books(title,tags:book_tags(tag_id))&name=eq.Alice", jwt),

        // ---- Embed with filter on parent ----
        g("/authors?select=name,books(title)&id=gt.1&order=id.asc", jwt),

        // ---- Embed with select on embedded columns ----
        g("/books?select=title,authors(name)&order=id.asc", jwt),

        // ---- Self-referencing embed (status-only, embed structure may differ) ----
        g_status_only("self-ref embed CEO", "/employees?select=name,employees(name)&id=eq.1", jwt),
        g_status_only("self-ref embed VP", "/employees?select=name,employees(name)&id=eq.2", jwt),

        // ---- Many-to-many ----
        g("/books?select=title,tags(name)&id=eq.1", jwt),
        g("/books?select=title,tags(name)&id=eq.3", jwt),
        g_sorted(
            "tags→books M2M",
            "/tags?select=name,books(title)&id=eq.1",
            jwt,
        ),

        // ---- !inner embed (exclude parents without children) ----
        // Alice and Bob have books, Carol does not
        g("/authors?select=name,books!inner(title)&order=id.asc", jwt),

        // ---- Spread embed ----
        g("/books?select=title,...authors(name)&id=eq.1", jwt),
        g("/books?select=title,...authors(name,bio)&order=id.asc", jwt),

        // ---- Embed with embedded ordering ----
        // Note: PostgREST doesn't support ordering within embeds via URL,
        // so we only test the parent ordering with embeds
        g("/authors?select=name,books(title)&order=name.asc&id=in.(1,2)", jwt),

        // ---- Empty embed results ----
        g("/authors?select=name,books(title)&name=eq.Carol", jwt),
    ]
}
