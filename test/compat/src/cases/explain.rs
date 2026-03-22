use super::*;
use crate::TestCase;

pub fn cases(jwt: &str) -> Vec<TestCase> {
    // EXPLAIN tests: PostgREST returns query plan when Accept: application/vnd.pgrst.plan+json.
    // PostgREST requires db-plan-enabled=true config for EXPLAIN. Without it, returns 406.
    // We skip status comparison since our test PostgREST doesn't have this enabled.
    vec![
        {
            let mut tc = g_skip_all("EXPLAIN authors", "/authors", jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.plan+json".to_string()));
            tc
        },
        {
            let mut tc = g_skip_all("EXPLAIN books", "/books?order=id.asc", jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.plan+json".to_string()));
            tc
        },
        {
            let mut tc = g_skip_all("EXPLAIN with filter", "/books?pages=gt.300", jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.plan+json".to_string()));
            tc
        },
        {
            let mut tc = g_skip_all("EXPLAIN with limit", "/numbered?limit=10", jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.plan+json".to_string()));
            tc
        },
        {
            let mut tc = g_skip_all("EXPLAIN with embed", "/authors?select=name,books(title)", jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.plan+json".to_string()));
            tc
        },
        {
            let mut tc = g_skip_all("EXPLAIN items", "/items?active=eq.true", jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.plan+json".to_string()));
            tc
        },
        {
            let mut tc = g_skip_all("EXPLAIN settings", "/settings", jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.plan+json".to_string()));
            tc
        },
        {
            let mut tc = g_skip_all("EXPLAIN with or", "/authors?or=(name.eq.Alice,name.eq.Bob)", jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.plan+json".to_string()));
            tc
        },
        {
            let mut tc = g_skip_all("EXPLAIN compound_pk", "/compound_pk", jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.plan+json".to_string()));
            tc
        },
        {
            let mut tc = g_skip_all("EXPLAIN numbered", "/numbered", jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.plan+json".to_string()));
            tc
        },
    ]
}
