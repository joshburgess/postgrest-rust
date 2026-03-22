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

        // ==== More JSONB contains variations ====
        g("/entities?data=cs.{\"x\":3}&order=id.asc", jwt),
        g("/items?metadata=cs.{\"color\":\"green\"}&order=id.asc", jwt),
        g("/items?metadata=cs.{\"color\":\"blue\"}&order=id.asc", jwt),

        // ==== More array contains/overlaps ====
        g("/entities?arr=cs.{a}&order=id.asc", jwt),
        g("/entities?arr=cs.{e}&order=id.asc", jwt),
        g("/entities?arr=ov.{a,b}&order=id.asc", jwt),
        g("/entities?arr=ov.{c,d}&order=id.asc", jwt),
        g("/entities?arr=ov.{e}&order=id.asc", jwt),
        g("/entities?arr=cd.{a,b,c}&order=id.asc", jwt),

        // ==== JSON arrow with different paths ====
        g("/entities?select=name,data->>x&data=not.is.null&order=id.asc", jwt),
        g("/items?select=name,metadata->>color&active=eq.true&order=id.asc", jwt),

        // ==== Types test JSON columns ====
        g("/types_test?json_col=not.is.null&order=id.asc", jwt),
        g("/types_test?jsonb_col=not.is.null&order=id.asc", jwt),

        // ==== Int array operations ====
        g("/types_test?int_arr=cs.{1,2}&order=id.asc", jwt),
        g("/types_test?text_arr=cs.{foo}&order=id.asc", jwt),
        g("/types_test?int_arr=ov.{3,4}&order=id.asc", jwt),
        g("/types_test?text_arr=ov.{bar,baz}&order=id.asc", jwt),
    ]
}
