use super::*;
use crate::TestCase;

pub fn cases(jwt: &str) -> Vec<TestCase> {
    vec![
        // ==== JSONB contains ====
        g("/entities?data=cs.{\"x\":1}&order=id.asc", jwt),
        g("/entities?data=cs.{\"tags\":[\"alpha\"]}&order=id.asc", jwt),
        g("/entities?data=cs.{\"x\":2}&order=id.asc", jwt),
        g("/entities?data=cs.{\"tags\":[\"beta\"]}&order=id.asc", jwt),
        g("/items?metadata=cs.{\"color\":\"red\"}&order=id.asc", jwt),

        // ==== JSONB contained-in ====
        g("/entities?data=cd.{\"x\":1,\"tags\":[\"alpha\",\"beta\"],\"extra\":true}&order=id.asc", jwt),

        // ==== JSON select (arrow operators) ====
        g("/entities?select=name,data->x&order=id.asc", jwt),
        g("/entities?select=name,data->>x&order=id.asc", jwt),
        g("/items?select=name,metadata->color&order=id.asc", jwt),
        g("/items?select=name,metadata->>color&order=id.asc", jwt),
        g("/entities?select=name,data->tags&order=id.asc", jwt),

        // ==== JSONB column reads ====
        g("/types_test?select=id,json_col,jsonb_col&order=id.asc", jwt),
        g("/types_test?jsonb_col=cs.{\"a\":1}&order=id.asc", jwt),
        g("/types_test?jsonb_col=cs.{\"a\":2}&order=id.asc", jwt),

        // ==== JSONB is null ====
        g("/entities?data=is.null&order=id.asc", jwt),
        g("/entities?data=not.is.null&order=id.asc", jwt),

        // ==== Array column operations ====
        g("/types_test?select=id,int_arr,text_arr&order=id.asc", jwt),
        g("/entities?select=name,arr&order=id.asc", jwt),
        g("/entities?arr=is.null&order=id.asc", jwt),
        g("/entities?arr=not.is.null&order=id.asc", jwt),
    ]
}
