use super::*;
use crate::TestCase;

pub fn cases(jwt: &str) -> Vec<TestCase> {
    vec![
        // ==== One-to-many ====
        g("/authors?select=name,books(title)&name=eq.Alice", jwt),
        g("/authors?select=name,books(title)&name=eq.Bob", jwt),
        g("/authors?select=name,books(title)&name=eq.Carol", jwt),
        g("/authors?select=name,books(title,pages)&order=id.asc&id=in.(1,2,3)", jwt),
        g("/authors?select=name,books(title)&order=id.asc&id=in.(1,2)", jwt),
        g("/projects?select=name,tasks(title)&order=id.asc", jwt),

        // ==== Many-to-one ====
        g("/books?select=title,authors(name)&id=eq.1", jwt),
        g("/books?select=title,authors(name,bio)&order=id.asc", jwt),
        g("/books?select=title,authors(name)&id=in.(1,2)&order=id.asc", jwt),
        g("/tasks?select=title,projects(name)&order=id.asc", jwt),

        // ==== Aliased embed ====
        g("/books?select=title,author:authors(name)&id=eq.1", jwt),
        g("/books?select=title,writer:authors(name)&order=id.asc", jwt),

        // ==== Nested embedding ====
        g("/authors?select=name,books(title,tags:book_tags(tag_id))&name=eq.Alice", jwt),

        // ==== Embed with filter on parent ====
        g("/authors?select=name,books(title)&id=gt.1&order=id.asc", jwt),
        g("/authors?select=name,books(title)&bio=not.is.null&order=id.asc", jwt),

        // ==== Embed with select on embedded columns ====
        g("/books?select=title,authors(name)&order=id.asc", jwt),
        g("/authors?select=name,books(id,title)&order=id.asc&id=in.(1,2,3)", jwt),

        // ==== Self-referencing embed (status-only) ====
        g_status_only("self-ref CEO", "/employees?select=name,employees(name)&id=eq.1", jwt),
        g_status_only("self-ref VP", "/employees?select=name,employees(name)&id=eq.2", jwt),

        // ==== Many-to-many ====
        g("/books?select=title,tags(name)&id=eq.1", jwt),
        g("/books?select=title,tags(name)&id=eq.3", jwt),
        g_sorted("tags→books M2M", "/tags?select=name,books(title)&id=eq.1", jwt),
        g("/books?select=title,tags(name)&order=id.asc&id=in.(1,2,3,4)", jwt),

        // ==== !inner embed ====
        g("/authors?select=name,books!inner(title)&order=id.asc", jwt),
        g("/authors?select=name,books!inner(title)&order=id.asc&id=in.(1,2,3)", jwt),

        // ==== Spread embed ====
        g("/books?select=title,...authors(name)&id=eq.1", jwt),
        g("/books?select=title,...authors(name,bio)&order=id.asc", jwt),
        g("/books?select=title,...authors(name)&order=id.asc&id=in.(1,2)", jwt),

        // ==== Embed with parent ordering ====
        g("/authors?select=name,books(title)&order=name.asc&id=in.(1,2)", jwt),
        g("/authors?select=name,books(title)&order=name.desc&id=in.(1,2,3)", jwt),

        // ==== Empty embed results ====
        g("/authors?select=name,books(title)&name=eq.Carol", jwt),
        g("/tags?select=name,books(title)&name=eq.beginner", jwt),

        // ==== Embed on views (should still work) ====
        g_status_only("view with books embed", "/authors_with_books?order=id.asc", jwt),

        // ==== Deep nesting (3 levels) ====
        g("/authors?select=name,books(title,book_tags(tag_id))&name=eq.Alice", jwt),
    ]
}
