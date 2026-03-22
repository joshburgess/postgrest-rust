use super::*;
use crate::TestCase;

pub fn cases(jwt: &str) -> Vec<TestCase> {
    vec![
        // ==== Singular response (exactly one row) ====
        {
            let mut tc = g("/authors?id=eq.1", jwt);
            tc.name = "singular: one author";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/books?id=eq.1", jwt);
            tc.name = "singular: one book";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/settings?key=eq.theme", jwt);
            tc.name = "singular: setting";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/employees?id=eq.1", jwt);
            tc.name = "singular: employee";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ==== Singular with select ====
        {
            let mut tc = g("/authors?select=name&id=eq.1", jwt);
            tc.name = "singular: select columns";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/books?select=title,pages&id=eq.1", jwt);
            tc.name = "singular: select book cols";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ==== Singular with embedding ====
        {
            let mut tc = g("/books?select=title,authors(name)&id=eq.1", jwt);
            tc.name = "singular: with embed";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/authors?select=name,books(title)&id=eq.1", jwt);
            tc.name = "singular: O2M embed";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ==== Singular 406 (multiple rows) ====
        {
            let mut tc = g_status_only("singular: 406 multiple", "/authors", jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ==== Singular 406 (no rows) ====
        {
            let mut tc = g_status_only("singular: 406 empty", "/authors?name=eq.Nobody", jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ==== CSV content negotiation ====
        {
            let mut tc = g("/authors?order=id.asc&id=in.(1,2,3)", jwt);
            tc.name = "CSV output";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false; // CSV format details may differ
            tc
        },
        {
            let mut tc = g("/books?order=id.asc&select=title,pages", jwt);
            tc.name = "CSV books";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false;
            tc
        },

        // ==== OpenAPI v2 ====
        {
            let mut tc = g_status_only("OpenAPI v2 (swagger)", "/", jwt);
            tc.headers.clear();
            tc
        },

        // ==== More singular tests ====
        {
            let mut tc = g("/profiles?username=eq.bob", jwt);
            tc.name = "singular profile";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/employees?id=eq.4", jwt);
            tc.name = "singular employee 4";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/tasks?id=eq.1", jwt);
            tc.name = "singular task";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ==== More CSV tests ====
        {
            let mut tc = g("/employees?order=id.asc", jwt);
            tc.name = "CSV employees";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false;
            tc
        },
        {
            let mut tc = g("/profiles?order=id.asc", jwt);
            tc.name = "CSV profiles";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false;
            tc
        },
        {
            let mut tc = g("/items?order=id.asc&select=name,price", jwt);
            tc.name = "CSV items";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false;
            tc
        },

        // ==== Prefer: count variations ====
        {
            let mut tc = g("/authors?order=id.asc&id=in.(1,2,3)", jwt);
            tc.name = "Prefer count=exact";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },

        // ==== More singular tests ====
        {
            let mut tc = g("/orders?id=eq.1", jwt);
            tc.name = "singular order 1";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/logs?id=eq.3", jwt);
            tc.name = "singular log 3";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/unicode_test?id=eq.1", jwt);
            tc.name = "singular unicode";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/numbered?val=eq.50", jwt);
            tc.name = "singular numbered";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/entities?id=eq.1", jwt);
            tc.name = "singular entity";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ==== Singular with select ====
        {
            let mut tc = g("/orders?select=customer,amount&id=eq.1", jwt);
            tc.name = "singular order select";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g("/profiles?select=username,score&username=eq.alice", jwt);
            tc.name = "singular profile select";
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ==== Singular 406 on various tables ====
        {
            let mut tc = g_status_only("singular 406 orders", "/orders", jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g_status_only("singular 406 profiles", "/profiles", jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },
        {
            let mut tc = g_status_only("singular 406 logs", "/logs", jwt);
            tc.headers.push(("Accept", "application/vnd.pgrst.object+json".to_string()));
            tc
        },

        // ==== CSV on more tables ====
        {
            let mut tc = g("/orders?select=customer,amount,status&order=id.asc", jwt);
            tc.name = "CSV orders select";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false;
            tc
        },
        {
            let mut tc = g("/logs?select=level,message&order=id.asc", jwt);
            tc.name = "CSV logs select";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false;
            tc
        },
        {
            let mut tc = g("/unicode_test?order=id.asc", jwt);
            tc.name = "CSV unicode";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false;
            tc
        },
        {
            let mut tc = g("/entities?select=id,name&order=id.asc", jwt);
            tc.name = "CSV entities";
            tc.headers.push(("Accept", "text/csv".to_string()));
            tc.compare_body = false;
            tc
        },

        // ==== Count on more tables ====
        {
            let mut tc = g("/orders?status=eq.completed&order=id.asc", jwt);
            tc.name = "count orders completed";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/logs?level=eq.info&order=id.asc", jwt);
            tc.name = "count logs info";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/profiles?active=eq.true&order=id.asc", jwt);
            tc.name = "count active profiles 2";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
        {
            let mut tc = g("/items?order=id.asc", jwt);
            tc.name = "count all items";
            tc.headers.push(("Prefer", "count=exact".to_string()));
            tc
        },
    ]
}
