use super::*;
use crate::TestCase;

pub fn cases(jwt: &str) -> Vec<TestCase> {
    vec![
        // ==== Basic reads ====
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
        g("/unicode_test?order=id.asc", jwt),
        g("/projects?order=id.asc", jwt),
        g("/tasks?order=id.asc", jwt),

        // ==== Select columns ====
        g("/authors?select=name&order=id.asc", jwt),
        g("/authors?select=id,name&order=id.asc", jwt),
        g("/books?select=title,pages&order=id.asc", jwt),
        g("/authors?select=*&order=id.asc", jwt),
        g("/items?select=name,price&order=id.asc", jwt),
        g("/types_test?select=id,text_col,bool_col&order=id.asc", jwt),
        g("/compound_pk?select=k1,k2,value&order=k1.asc,k2.asc", jwt),
        g("/employees?select=id,name&order=id.asc", jwt),

        // ==== Select with cast ====
        g("/authors?select=id::text,name&order=id.asc", jwt),
        g("/books?select=pages::text&order=id.asc", jwt),

        // ==== eq filter ====
        g("/authors?name=eq.Alice", jwt),
        g("/authors?id=eq.1", jwt),
        g("/books?title=eq.Learning Rust", jwt),
        g("/types_test?bool_col=eq.true", jwt),
        g("/types_test?int_col=eq.1", jwt),
        g("/types_test?text_col=eq.hello", jwt),
        g("/settings?key=eq.theme", jwt),
        g("/types_test?bigint_col=eq.100", jwt),
        g("/types_test?date_col=eq.2024-01-01", jwt),
        g("/compound_pk?k1=eq.1&k2=eq.1", jwt),
        g("/employees?manager_id=eq.1&order=id.asc", jwt),

        // ==== neq filter ====
        g("/authors?name=neq.Alice&order=id.asc", jwt),
        g("/types_test?bool_col=neq.true&order=id.asc", jwt),
        g("/types_test?int_col=neq.1&order=id.asc", jwt),
        g("/books?pages=neq.350&order=id.asc", jwt),

        // ==== gt / gte / lt / lte ====
        g("/books?pages=gt.400&order=id.asc", jwt),
        g("/books?pages=gte.400&order=id.asc", jwt),
        g("/books?pages=lt.300&order=id.asc", jwt),
        g("/books?pages=lte.300&order=id.asc", jwt),
        g("/types_test?int_col=gt.1&order=id.asc", jwt),
        g("/types_test?numeric_col=gte.20&order=id.asc", jwt),
        g("/types_test?date_col=gt.2024-03-01&order=id.asc", jwt),
        g("/types_test?date_col=lt.2024-03-01&order=id.asc", jwt),
        g("/types_test?bigint_col=gt.150&order=id.asc", jwt),
        g("/types_test?numeric_col=lt.15&order=id.asc", jwt),
        g("/numbered?val=gt.95&order=id.asc", jwt),
        g("/numbered?val=lte.5&order=id.asc", jwt),
        g("/items?price=gt.10&order=id.asc", jwt),
        g("/items?price=lte.5&order=id.asc", jwt),

        // ==== like / ilike ====
        g("/authors?name=like.A*&order=id.asc", jwt),
        g("/authors?name=like.*o*&order=id.asc", jwt),
        g("/authors?name=ilike.alice&order=id.asc", jwt),
        g("/authors?name=ilike.*OB*&order=id.asc", jwt),
        g("/books?title=like.Learn*&order=id.asc", jwt),
        g("/books?title=ilike.*rust*&order=id.asc", jwt),
        g("/books?title=like.*Deep*&order=id.asc", jwt),
        g("/items?name=like.G*&order=id.asc", jwt),
        g("/items?name=ilike.*widget*&order=id.asc", jwt),
        g("/unicode_test?name=like.caf*&order=id.asc", jwt),

        // ==== in ====
        g("/authors?id=in.(1,2)&order=id.asc", jwt),
        g("/authors?id=in.(1,2,3)&order=id.asc", jwt),
        g("/authors?name=in.(Alice,Bob)&order=id.asc", jwt),
        g("/books?pages=in.(350,500)&order=id.asc", jwt),
        g("/types_test?int_col=in.(1,2)&order=id.asc", jwt),
        g("/settings?key=in.(theme,site_name)&order=key.asc", jwt),
        g("/employees?id=in.(1,2,3)&order=id.asc", jwt),

        // ==== is ====
        g("/authors?bio=is.null&order=id.asc", jwt),
        g("/types_test?text_col=is.null&order=id.asc", jwt),
        g("/types_test?bool_col=is.true&order=id.asc", jwt),
        g("/types_test?bool_col=is.false&order=id.asc", jwt),
        g("/types_test?bool_col=is.null&order=id.asc", jwt),
        g("/books?published=is.null&order=id.asc", jwt),
        g("/employees?manager_id=is.null&order=id.asc", jwt),
        g("/types_test?int_col=is.null&order=id.asc", jwt),
        g("/types_test?json_col=is.null&order=id.asc", jwt),
        g("/entities?arr=is.null&order=id.asc", jwt),
        g("/entities?data=is.null&order=id.asc", jwt),

        // ==== not.X negation ====
        g("/authors?bio=not.is.null&order=id.asc", jwt),
        g("/authors?name=not.eq.Alice&order=id.asc", jwt),
        g("/books?pages=not.gt.400&order=id.asc", jwt),
        g("/authors?id=not.in.(1,2)&order=id.asc", jwt),
        g("/types_test?bool_col=not.is.null&order=id.asc", jwt),
        g("/types_test?bool_col=not.is.true&order=id.asc", jwt),
        g("/books?title=not.like.*Rust*&order=id.asc", jwt),
        g("/books?title=not.ilike.*sql*&order=id.asc", jwt),
        g("/employees?manager_id=not.is.null&order=id.asc", jwt),
        g("/types_test?int_col=not.eq.1&order=id.asc", jwt),
        g("/types_test?date_col=not.is.null&order=id.asc", jwt),
        g("/items?active=not.eq.true&order=id.asc", jwt),

        // ==== Ordering ====
        g("/authors?order=name.asc&id=in.(1,2,3)", jwt),
        g("/authors?order=name.desc&id=in.(1,2,3)", jwt),
        g("/books?order=pages.desc.nullslast", jwt),
        g("/books?order=pages.asc.nullsfirst", jwt),
        g("/books?order=published.desc.nullslast", jwt),
        g("/types_test?order=int_col.asc.nullslast", jwt),
        g("/types_test?order=text_col.desc.nullsfirst", jwt),
        g("/employees?order=manager_id.asc.nullsfirst", jwt),
        g("/items?order=price.desc", jwt),
        g("/items?order=name.asc", jwt),
        g("/numbered?order=val.desc&limit=5", jwt),

        // ==== Multiple order columns (comma-separated, PostgREST syntax) ====
        g("/books?order=author_id.asc,title.asc", jwt),
        g("/employees?order=manager_id.asc.nullsfirst,name.asc", jwt),

        // ==== Limit / Offset ====
        g("/authors?order=id.asc&limit=1", jwt),
        g("/authors?order=id.asc&limit=2", jwt),
        g("/authors?order=id.asc&limit=1&offset=1", jwt),
        g("/authors?order=id.asc&limit=1&offset=2", jwt),
        g("/numbered?order=id.asc&limit=5", jwt),
        g("/numbered?order=id.asc&limit=5&offset=95", jwt),
        g("/numbered?order=id.asc&limit=10&offset=0", jwt),
        g("/numbered?order=id.asc&limit=1&offset=99", jwt),
        g("/items?order=id.asc&limit=2", jwt),

        // ==== Multiple filters (AND) ====
        g("/books?pages=gt.300&pages=lt.500&order=id.asc", jwt),
        g("/books?author_id=eq.1&pages=gt.400", jwt),
        g("/types_test?int_col=gt.0&text_col=eq.hello", jwt),
        g("/items?active=eq.true&price=gt.5&order=id.asc", jwt),
        g("/employees?manager_id=eq.1&name=like.*VP*&order=id.asc", jwt),
        g("/books?author_id=eq.2&published=not.is.null&order=id.asc", jwt),
        g("/types_test?bool_col=eq.true&int_col=eq.1", jwt),

        // ==== Empty results ====
        g("/authors?name=eq.Nobody", jwt),
        g("/books?pages=gt.10000", jwt),
        g("/types_test?int_col=eq.999", jwt),
        g("/items?name=eq.Nonexistent", jwt),
        g("/numbered?val=gt.1000", jwt),

        // ==== Contains / Contained-in (arrays) ====
        g("/entities?arr=cs.{a,b}&order=id.asc", jwt),
        g("/entities?arr=cs.{b}&order=id.asc", jwt),
        g("/entities?arr=cd.{a,b,c,d,e}&order=id.asc", jwt),
        g("/entities?arr=cs.{c}&order=id.asc", jwt),
        g("/entities?arr=cs.{a,c}&order=id.asc", jwt),

        // ==== Overlaps (arrays) ====
        g("/entities?arr=ov.{a,d}&order=id.asc", jwt),
        g("/entities?arr=ov.{z}&order=id.asc", jwt),
        g("/entities?arr=ov.{a}&order=id.asc", jwt),
        g("/entities?arr=ov.{d,e}&order=id.asc", jwt),

        // ==== Views ====
        g("/authors_with_books?order=id.asc", jwt),
        g("/simple_items?order=id.asc", jwt),
        g("/authors_with_books?book_count=gt.0&order=id.asc", jwt),

        // ==== Unicode / special characters in values ====
        g("/unicode_test?name=eq.café", jwt),
        g("/unicode_test?name=eq.naïve", jwt),
        g("/unicode_test?name=eq.日本語", jwt),

        // ==== Filtering on composite PK ====
        g("/compound_pk?k1=eq.1&order=k2.asc", jwt),
        g("/compound_pk?k2=eq.1&order=k1.asc", jwt),
        g("/compound_pk?k1=eq.1&k2=eq.2", jwt),

        // ==== 404 for nonexistent table ====
        g_status_only("GET /nonexistent (404)", "/nonexistent", jwt),
    ]
}
