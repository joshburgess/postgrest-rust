use std::collections::HashMap;

use pg_query_engine::*;
use pg_schema_cache_tokio_postgres::*;
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_cache() -> SchemaCache {
    let mut tables = HashMap::new();
    tables.insert(
        QualifiedName::new("api", "items"),
        Table {
            name: QualifiedName::new("api", "items"),
            columns: vec![
                col("id", "int4", false, true, true),
                col("name", "text", false, false, false),
                col("value", "int4", true, false, false),
                col("note", "text", true, false, false),
            ],
            column_index: [
                ("id".into(), 0),
                ("name".into(), 1),
                ("value".into(), 2),
                ("note".into(), 3),
            ]
            .into_iter()
            .collect(),
            primary_key: vec!["id".into()],
            is_view: false,
            insertable: true,
            updatable: true,
            deletable: true,
            comment: None,
        },
    );
    SchemaCache {
        tables,
        relationships: Vec::new(),
        functions: HashMap::new(),
    }
}

fn col(name: &str, pg_type: &str, nullable: bool, has_default: bool, is_pk: bool) -> Column {
    Column {
        name: name.into(),
        pg_type: pg_type.into(),
        nullable,
        has_default,
        default_expr: None,
        max_length: None,
        is_pk,
        is_generated: false,
        comment: None,
        enum_values: None,
    }
}

// ---------------------------------------------------------------------------
// Property: parse_select never panics on arbitrary input
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn parse_select_does_not_panic(s in ".*") {
        let _ = parse_select(&s);
    }

    #[test]
    fn parse_filter_does_not_panic(col in "[a-z_]{1,10}", val in ".*") {
        let _ = parse_filter(&col, &val);
    }

    #[test]
    fn parse_order_does_not_panic(s in ".*") {
        let _ = parse_order(&s);
    }
}

// ---------------------------------------------------------------------------
// Property: valid filter operators always produce a Filter
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn valid_eq_filter_parses(val in "[a-zA-Z0-9_]{1,20}") {
        let f = parse_filter("col", &format!("eq.{val}"));
        prop_assert!(f.is_ok());
        let f = f.unwrap();
        prop_assert_eq!(f.column, "col");
        prop_assert!(!f.negated);
    }

    #[test]
    fn negated_filter_parses(val in "[a-zA-Z0-9]{1,20}") {
        let f = parse_filter("col", &format!("not.eq.{val}"));
        prop_assert!(f.is_ok());
        prop_assert!(f.unwrap().negated);
    }
}

// ---------------------------------------------------------------------------
// Property: build_sql always produces valid SQL structure for simple reads
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn build_read_sql_structure(
        limit in proptest::option::of(1i64..1000),
        offset in proptest::option::of(0i64..1000),
    ) {
        let cache = test_cache();
        let req = ApiRequest::Read(ReadRequest {
            table: QualifiedName::new("api", "items"),
            select: vec![SelectItem::Star],
            filters: FilterNode::empty(),
            order: Vec::new(),
            limit,
            offset,
            count: CountOption::None,
        });
        let out = build_sql(&cache, &req, &["api".into()]).unwrap();
        prop_assert!(out.sql.contains("json_agg"));
        prop_assert!(out.sql.contains("\"api\".\"items\""));
        if limit.is_some() {
            prop_assert!(out.sql.contains("LIMIT"));
        }
        if offset.is_some() {
            prop_assert!(out.sql.contains("OFFSET"));
        }
    }

    #[test]
    fn build_read_with_filters(
        col_idx in 0usize..4,
        val in "[a-zA-Z0-9]{1,10}",
    ) {
        let cache = test_cache();
        let columns = ["id", "name", "value", "note"];
        let col_name = columns[col_idx];

        let req = ApiRequest::Read(ReadRequest {
            table: QualifiedName::new("api", "items"),
            select: vec![SelectItem::Star],
            filters: FilterNode::from_filters(vec![Filter {
                column: col_name.into(),
                operator: FilterOp::Eq,
                value: FilterValue::Value(val.clone()),
                negated: false,
            }]),
            order: Vec::new(),
            limit: None,
            offset: None,
            count: CountOption::None,
        });
        let out = build_sql(&cache, &req, &["api".into()]).unwrap();
        prop_assert!(out.sql.contains("WHERE"));
        prop_assert_eq!(out.params.len(), 1);
        prop_assert_eq!(&out.params[0], &val);
    }
}

// ---------------------------------------------------------------------------
// Property: SQL output never contains unparameterized user values
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn filter_values_are_parameterized(
        val in "[a-zA-Z0-9]{1,20}",
    ) {
        let cache = test_cache();
        let req = ApiRequest::Read(ReadRequest {
            table: QualifiedName::new("api", "items"),
            select: vec![SelectItem::Star],
            filters: FilterNode::from_filters(vec![Filter {
                column: "name".into(),
                operator: FilterOp::Eq,
                value: FilterValue::Value(val.clone()),
                negated: false,
            }]),
            order: Vec::new(),
            limit: None,
            offset: None,
            count: CountOption::None,
        });
        let out = build_sql(&cache, &req, &["api".into()]).unwrap();
        // The literal value should NOT appear in the SQL — only in params.
        if val.len() > 2 {
            prop_assert!(!out.sql.contains(&val), "value leaked into SQL: {}", out.sql);
        }
        prop_assert!(out.params.contains(&val));
    }
}
