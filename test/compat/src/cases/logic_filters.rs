use super::*;
use crate::TestCase;

pub fn cases(jwt: &str) -> Vec<TestCase> {
    vec![
        // ==== Simple or ====
        g("/authors?or=(name.eq.Alice,name.eq.Carol)&order=id.asc", jwt),
        g("/authors?or=(name.eq.Alice,name.eq.Bob,name.eq.Carol)&order=id.asc", jwt),
        g("/books?or=(pages.gt.400,pages.lt.300)&order=id.asc", jwt),
        g("/items?or=(active.eq.true,price.gt.20)&order=id.asc", jwt),

        // ==== or with different operators ====
        g("/authors?or=(name.eq.Alice,bio.is.null)&order=id.asc", jwt),
        g("/books?or=(title.like.*Rust*,pages.gt.400)&order=id.asc", jwt),
        g("/books?or=(published.is.null,pages.lt.300)&order=id.asc", jwt),
        g("/types_test?or=(int_col.eq.1,text_col.eq.world)&order=id.asc", jwt),
        g("/employees?or=(manager_id.is.null,name.like.*VP*)&order=id.asc", jwt),

        // ==== Nested and inside or ====
        g("/authors?or=(name.eq.Alice,and(name.eq.Bob,bio.not.is.null))&order=id.asc", jwt),
        g("/books?or=(and(pages.gt.400,author_id.eq.1),title.like.*SQL*)&order=id.asc", jwt),

        // ==== or + regular AND params ====
        g("/books?or=(pages.gt.400,pages.lt.300)&author_id=eq.1&order=id.asc", jwt),
        g("/books?or=(pages.gt.400,pages.lt.300)&author_id=eq.2&order=id.asc", jwt),
        g("/items?or=(price.gt.20,quantity.gt.100)&active=eq.true&order=id.asc", jwt),

        // ==== or with in ====
        g("/authors?or=(id.in.(1,2),name.eq.Carol)&order=id.asc", jwt),
        g("/books?or=(id.in.(1,4),pages.gt.400)&order=id.asc", jwt),

        // ==== or with no matches ====
        g("/authors?or=(name.eq.Nobody,name.eq.NoOne)", jwt),

        // ==== Explicit and ====
        g("/books?and=(pages.gt.300,pages.lt.500)&order=id.asc", jwt),
        g("/items?and=(active.eq.true,price.lt.10)&order=id.asc", jwt),

        // ==== Complex nested ====
        g("/books?or=(and(pages.gt.300,author_id.eq.1),and(pages.lt.300,author_id.eq.2))&order=id.asc", jwt),
        g("/authors?or=(and(name.eq.Alice,bio.not.is.null),and(name.eq.Carol,bio.is.null))&order=id.asc", jwt),

        // ==== or with like/ilike ====
        g("/books?or=(title.like.*Rust*,title.like.*SQL*)&order=id.asc", jwt),
        g("/authors?or=(name.ilike.*ali*,name.ilike.*car*)&order=id.asc", jwt),

        // ==== Deeply nested ====
        g("/books?or=(and(pages.gt.400,or(author_id.eq.1,author_id.eq.2)),title.eq.SQL Deep Dive)&order=id.asc", jwt),

        // ==== or on profiles ====
        g("/profiles?or=(username.eq.alice,username.eq.dave)&order=id.asc", jwt),
        g("/profiles?or=(score.gt.90,age.lt.26)&order=id.asc", jwt),
        g("/profiles?or=(email.is.null,bio.is.null)&order=id.asc", jwt),

        // ==== or with is.null ====
        g("/employees?or=(manager_id.is.null,id.gt.4)&order=id.asc", jwt),
        g("/types_test?or=(int_col.is.null,bool_col.is.null)&order=id.asc", jwt),

        // ==== or on tasks ====
        g("/tasks?or=(assigned_to.eq.4,assigned_to.eq.5)&order=id.asc", jwt),
        g("/tasks?or=(project_id.eq.1,created_by.eq.3)&order=id.asc", jwt),

        // ==== and nested inside or with profiles ====
        g("/profiles?or=(and(age.gt.30,score.gt.70),username.eq.bob)&order=id.asc", jwt),

        // ==== or with not ====
        g("/authors?or=(name.eq.Alice,bio.not.is.null)&order=id.asc", jwt),
        g("/numbered?or=(val.lte.2,val.gte.99)&order=val.asc", jwt),
    ]
}
