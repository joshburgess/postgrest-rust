use super::*;
use crate::TestCase;

pub fn cases(jwt: &str) -> Vec<TestCase> {
    vec![
        // ---- JSONB contains ----
        g("/entities?data=cs.{\"x\":1}&order=id.asc", jwt),
        g("/entities?data=cs.{\"tags\":[\"alpha\"]}&order=id.asc", jwt),

        // ---- JSONB contained-in ----
        g("/entities?data=cd.{\"x\":1,\"tags\":[\"alpha\",\"beta\"],\"extra\":true}&order=id.asc", jwt),

        // ---- JSON select (arrow operators) ----
        g("/entities?select=name,data->x&order=id.asc", jwt),
        g("/entities?select=name,data->>x&order=id.asc", jwt),
        g("/items?select=name,metadata->color&order=id.asc", jwt),
        g("/items?select=name,metadata->>color&order=id.asc", jwt),

        // ---- JSONB filter with arrow in filter value ----
        // Note: PostgREST filters on JSONB use cs/cd operators, not arrow syntax in filters

        // ---- Types test JSON columns ----
        g("/types_test?select=id,json_col,jsonb_col&order=id.asc", jwt),
        g("/types_test?jsonb_col=cs.{\"a\":1}&order=id.asc", jwt),
    ]
}
