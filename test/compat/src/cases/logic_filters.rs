use super::*;
use crate::TestCase;

pub fn cases(jwt: &str) -> Vec<TestCase> {
    vec![
        // ---- Simple or ----
        g("/authors?or=(name.eq.Alice,name.eq.Carol)&order=id.asc", jwt),
        g("/authors?or=(name.eq.Alice,name.eq.Bob,name.eq.Carol)&order=id.asc", jwt),
        g("/books?or=(pages.gt.400,pages.lt.300)&order=id.asc", jwt),

        // ---- or with different operators ----
        g("/authors?or=(name.eq.Alice,bio.is.null)&order=id.asc", jwt),
        g("/books?or=(title.like.*Rust*,pages.gt.400)&order=id.asc", jwt),
        g("/books?or=(published.is.null,pages.lt.300)&order=id.asc", jwt),

        // ---- Nested and inside or ----
        g("/authors?or=(name.eq.Alice,and(name.eq.Bob,bio.not.is.null))&order=id.asc", jwt),

        // ---- Nested or inside and (via top-level AND + or param) ----
        g("/books?or=(pages.gt.400,pages.lt.300)&author_id=eq.1&order=id.asc", jwt),
        g("/books?or=(pages.gt.400,pages.lt.300)&author_id=eq.2&order=id.asc", jwt),

        // ---- or with in ----
        g("/authors?or=(id.in.(1,2),name.eq.Carol)&order=id.asc", jwt),

        // ---- or with no matches ----
        g("/authors?or=(name.eq.Nobody,name.eq.NoOne)", jwt),

        // ---- and (explicit) ----
        g("/books?and=(pages.gt.300,pages.lt.500)&order=id.asc", jwt),

        // ---- Complex nested ----
        g("/books?or=(and(pages.gt.300,author_id.eq.1),and(pages.lt.300,author_id.eq.2))&order=id.asc", jwt),
    ]
}
