use std::collections::{HashMap, HashSet};

use pg_wire::PgPipeline;

use crate::error::SchemaCacheError;
use pg_schema_cache::*;

// ---------------------------------------------------------------------------
// SQL queries
// ---------------------------------------------------------------------------

const TABLES_QUERY: &str = "
SELECT
    n.nspname::text AS table_schema,
    c.relname::text AS table_name,
    c.relkind::text AS kind,
    obj_description(c.oid)::text AS comment,
    CASE
        WHEN c.relkind IN ('r', 'p', 'f') THEN true
        ELSE COALESCE(
            (SELECT v.is_insertable_into = 'YES'
             FROM information_schema.views v
             WHERE v.table_schema = n.nspname AND v.table_name = c.relname),
            false
        )
    END AS insertable,
    CASE
        WHEN c.relkind IN ('r', 'p', 'f') THEN true
        ELSE COALESCE(
            (SELECT v.is_updatable = 'YES'
             FROM information_schema.views v
             WHERE v.table_schema = n.nspname AND v.table_name = c.relname),
            false
        )
    END AS updatable
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY($1)
  AND c.relkind IN ('r', 'v', 'm', 'f', 'p')
ORDER BY n.nspname, c.relname
";

const COLUMNS_QUERY: &str = "
SELECT
    c.table_schema::text,
    c.table_name::text,
    c.column_name::text,
    c.udt_name::text AS pg_type,
    (c.is_nullable = 'YES') AS nullable,
    (c.column_default IS NOT NULL) AS has_default,
    c.column_default::text AS default_expr,
    c.character_maximum_length::int4 AS max_length,
    (c.is_generated = 'ALWAYS') AS is_generated,
    pgd.description::text AS comment
FROM information_schema.columns c
LEFT JOIN pg_catalog.pg_namespace pn ON pn.nspname = c.table_schema
LEFT JOIN pg_catalog.pg_class pc
    ON pc.relname = c.table_name AND pc.relnamespace = pn.oid
LEFT JOIN pg_catalog.pg_description pgd
    ON pgd.objoid = pc.oid AND pgd.objsubid = c.ordinal_position::int
WHERE c.table_schema = ANY($1)
ORDER BY c.table_schema, c.table_name, c.ordinal_position
";

const PRIMARY_KEYS_QUERY: &str = "
SELECT
    n.nspname::text AS table_schema,
    c.relname::text AS table_name,
    a.attname::text AS column_name
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_attribute a
    ON a.attrelid = c.oid AND a.attnum = ANY(con.conkey)
WHERE n.nspname = ANY($1)
  AND con.contype = 'p'
ORDER BY n.nspname, c.relname, a.attnum
";

const FOREIGN_KEYS_QUERY: &str = "
SELECT
    n1.nspname::text AS from_schema,
    c1.relname::text AS from_table,
    n2.nspname::text AS to_schema,
    c2.relname::text AS to_table,
    con.conname::text AS constraint_name,
    array_agg(a1.attname::text ORDER BY pos.ord) AS from_columns,
    array_agg(a2.attname::text ORDER BY pos.ord) AS to_columns
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class c1 ON c1.oid = con.conrelid
JOIN pg_catalog.pg_namespace n1 ON n1.oid = c1.relnamespace
JOIN pg_catalog.pg_class c2 ON c2.oid = con.confrelid
JOIN pg_catalog.pg_namespace n2 ON n2.oid = c2.relnamespace
CROSS JOIN LATERAL unnest(con.conkey, con.confkey)
    WITH ORDINALITY AS pos(from_attnum, to_attnum, ord)
JOIN pg_catalog.pg_attribute a1
    ON a1.attrelid = c1.oid AND a1.attnum = pos.from_attnum
JOIN pg_catalog.pg_attribute a2
    ON a2.attrelid = c2.oid AND a2.attnum = pos.to_attnum
WHERE con.contype = 'f'
  AND (n1.nspname = ANY($1) OR n2.nspname = ANY($1))
GROUP BY n1.nspname, c1.relname, n2.nspname, c2.relname, con.conname
";

const FUNCTIONS_QUERY: &str = "
SELECT
    n.nspname::text AS schema_name,
    p.proname::text AS function_name,
    p.provolatile::text AS volatility,
    p.proretset AS returns_set,
    rt.typname::text AS return_type_name,
    obj_description(p.oid)::text AS comment,
    p.prokind::text AS prokind,
    p.pronargs::int4 AS num_args,
    p.pronargdefaults::int4 AS num_defaults,
    COALESCE(p.proargnames, ARRAY[]::text[]) AS arg_names,
    COALESCE(p.proargmodes::text[], ARRAY[]::text[]) AS arg_modes,
    COALESCE(
        (SELECT array_agg(t.typname::text ORDER BY u.ord)
         FROM unnest(COALESCE(p.proallargtypes, p.proargtypes::oid[]))
              WITH ORDINALITY AS u(type_oid, ord)
         JOIN pg_catalog.pg_type t ON t.oid = u.type_oid),
        ARRAY[]::text[]
    ) AS arg_type_names
FROM pg_catalog.pg_proc p
JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
JOIN pg_catalog.pg_type rt ON rt.oid = p.prorettype
WHERE n.nspname = ANY($1)
  AND p.prokind IN ('f', 'p')
ORDER BY n.nspname, p.proname
";

const ENUMS_QUERY: &str = "
SELECT
    t.typname::text AS type_name,
    e.enumlabel::text AS enum_value
FROM pg_catalog.pg_type t
JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid
ORDER BY t.typname, e.enumsortorder
";

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

struct RawForeignKey {
    from_schema: String,
    from_table: String,
    to_schema: String,
    to_table: String,
    constraint_name: String,
    from_columns: Vec<String>,
    to_columns: Vec<String>,
}

// ---------------------------------------------------------------------------
// Helper functions for extracting data from simple-query rows
// ---------------------------------------------------------------------------

fn format_schema_array(schemas: &[String]) -> String {
    let quoted: Vec<String> = schemas
        .iter()
        .map(|s| format!("'{}'", s.replace('\'', "''")))
        .collect();
    format!("ARRAY[{}]", quoted.join(","))
}

fn col_str(row: &[Option<Vec<u8>>], idx: usize) -> String {
    row.get(idx)
        .and_then(|v| v.as_ref())
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default()
}

fn col_opt(row: &[Option<Vec<u8>>], idx: usize) -> Option<String> {
    row.get(idx)
        .and_then(|v| v.as_ref())
        .map(|b| String::from_utf8_lossy(b).into_owned())
}

fn col_bool(row: &[Option<Vec<u8>>], idx: usize) -> bool {
    col_str(row, idx) == "t"
}

fn col_i32(row: &[Option<Vec<u8>>], idx: usize) -> Option<i32> {
    col_opt(row, idx).and_then(|s| s.parse().ok())
}

fn col_string_array(row: &[Option<Vec<u8>>], idx: usize) -> Vec<String> {
    let s = col_str(row, idx);
    // PG text array format: {elem1,elem2,...}
    if s.len() <= 2 {
        return Vec::new();
    }
    s[1..s.len() - 1]
        .split(',')
        .map(|s| s.trim_matches('"').to_string())
        .collect()
}

fn col_opt_string_array(row: &[Option<Vec<u8>>], idx: usize) -> Vec<Option<String>> {
    let s = col_str(row, idx);
    if s.len() <= 2 {
        return Vec::new();
    }
    s[1..s.len() - 1]
        .split(',')
        .map(|s| {
            let s = s.trim();
            if s == "NULL" {
                None
            } else {
                Some(s.trim_matches('"').to_string())
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Public builder
// ---------------------------------------------------------------------------

pub(crate) async fn build(
    pg: &mut PgPipeline,
    schemas: &[String],
) -> Result<SchemaCache, SchemaCacheError> {
    let mut tables = load_tables(pg, schemas).await?;
    let columns = load_columns(pg, schemas).await?;
    let primary_keys = load_primary_keys(pg, schemas).await?;
    let raw_fks = load_foreign_keys(pg, schemas).await?;
    let functions = load_functions(pg, schemas).await?;
    let enums = load_enums(pg).await?;

    // Assemble: attach columns, PKs, and enum values to each table.
    for table in &mut tables {
        if let Some(cols) = columns.get(&table.name) {
            table.columns = cols.clone();
        }
        if let Some(pk_cols) = primary_keys.get(&table.name) {
            table.primary_key = pk_cols.clone();
            for col in &mut table.columns {
                col.is_pk = pk_cols.contains(&col.name);
            }
        }
        for col in &mut table.columns {
            if let Some(vals) = enums.get(&col.pg_type) {
                col.enum_values = Some(vals.clone());
            }
        }
    }

    let table_map: HashMap<QualifiedName, Table> = tables
        .into_iter()
        .map(|mut t| {
            t.rebuild_column_index();
            (t.name.clone(), t)
        })
        .collect();

    let relationships = build_relationships(&raw_fks, &table_map);

    Ok(SchemaCache {
        tables: table_map,
        relationships,
        functions,
    })
}

// ---------------------------------------------------------------------------
// Query executors
// ---------------------------------------------------------------------------

async fn load_tables(
    pg: &mut PgPipeline,
    schemas: &[String],
) -> Result<Vec<Table>, SchemaCacheError> {
    let schema_array = format_schema_array(schemas);
    let sql = TABLES_QUERY.replace("$1", &schema_array);
    let (rows, _) = pg
        .simple_query_rows(&sql)
        .await
        .map_err(SchemaCacheError::Database)?;
    let mut tables = Vec::with_capacity(rows.len());
    for row in &rows {
        let kind = col_str(row, 2);
        let is_view = kind == "v" || kind == "m";
        let insertable = col_bool(row, 4);
        let updatable = col_bool(row, 5);
        tables.push(Table {
            name: QualifiedName::new(col_str(row, 0), col_str(row, 1)),
            columns: Vec::new(),
            column_index: HashMap::new(),
            primary_key: Vec::new(),
            is_view,
            insertable,
            updatable,
            deletable: updatable,
            comment: col_opt(row, 3),
        });
    }
    Ok(tables)
}

async fn load_columns(
    pg: &mut PgPipeline,
    schemas: &[String],
) -> Result<HashMap<QualifiedName, Vec<Column>>, SchemaCacheError> {
    let schema_array = format_schema_array(schemas);
    let sql = COLUMNS_QUERY.replace("$1", &schema_array);
    let (rows, _) = pg
        .simple_query_rows(&sql)
        .await
        .map_err(SchemaCacheError::Database)?;
    let mut map: HashMap<QualifiedName, Vec<Column>> = HashMap::new();
    for row in &rows {
        let qn = QualifiedName::new(col_str(row, 0), col_str(row, 1));
        map.entry(qn).or_default().push(Column {
            name: col_str(row, 2),
            pg_type: col_str(row, 3),
            nullable: col_bool(row, 4),
            has_default: col_bool(row, 5),
            default_expr: col_opt(row, 6),
            max_length: col_i32(row, 7),
            is_pk: false, // set later from PK query
            is_generated: col_bool(row, 8),
            comment: col_opt(row, 9),
            enum_values: None, // set later from enum query
        });
    }
    Ok(map)
}

async fn load_primary_keys(
    pg: &mut PgPipeline,
    schemas: &[String],
) -> Result<HashMap<QualifiedName, Vec<String>>, SchemaCacheError> {
    let schema_array = format_schema_array(schemas);
    let sql = PRIMARY_KEYS_QUERY.replace("$1", &schema_array);
    let (rows, _) = pg
        .simple_query_rows(&sql)
        .await
        .map_err(SchemaCacheError::Database)?;
    let mut map: HashMap<QualifiedName, Vec<String>> = HashMap::new();
    for row in &rows {
        let qn = QualifiedName::new(col_str(row, 0), col_str(row, 1));
        map.entry(qn).or_default().push(col_str(row, 2));
    }
    Ok(map)
}

async fn load_foreign_keys(
    pg: &mut PgPipeline,
    schemas: &[String],
) -> Result<Vec<RawForeignKey>, SchemaCacheError> {
    let schema_array = format_schema_array(schemas);
    let sql = FOREIGN_KEYS_QUERY.replace("$1", &schema_array);
    let (rows, _) = pg
        .simple_query_rows(&sql)
        .await
        .map_err(SchemaCacheError::Database)?;
    let mut fks = Vec::with_capacity(rows.len());
    for row in &rows {
        fks.push(RawForeignKey {
            from_schema: col_str(row, 0),
            from_table: col_str(row, 1),
            to_schema: col_str(row, 2),
            to_table: col_str(row, 3),
            constraint_name: col_str(row, 4),
            from_columns: col_string_array(row, 5),
            to_columns: col_string_array(row, 6),
        });
    }
    Ok(fks)
}

async fn load_functions(
    pg: &mut PgPipeline,
    schemas: &[String],
) -> Result<HashMap<QualifiedName, Function>, SchemaCacheError> {
    let schema_array = format_schema_array(schemas);
    let sql = FUNCTIONS_QUERY.replace("$1", &schema_array);
    let (rows, _) = pg
        .simple_query_rows(&sql)
        .await
        .map_err(SchemaCacheError::Database)?;
    let mut map = HashMap::new();

    for row in &rows {
        let schema_name = col_str(row, 0);
        let function_name = col_str(row, 1);
        let volatility_char = col_str(row, 2);
        let returns_set = col_bool(row, 3);
        let return_type_name = col_str(row, 4);
        let comment = col_opt(row, 5);
        let prokind = col_str(row, 6);
        let num_args: i32 = col_i32(row, 7).unwrap_or(0);
        let num_defaults: i32 = col_i32(row, 8).unwrap_or(0);
        let arg_names = col_opt_string_array(row, 9);
        let arg_modes = col_string_array(row, 10);
        let arg_type_names = col_string_array(row, 11);

        let volatility = match volatility_char.as_str() {
            "i" => Volatility::Immutable,
            "s" => Volatility::Stable,
            _ => Volatility::Volatile,
        };

        let return_type = if return_type_name == "void" {
            ReturnType::Void
        } else if returns_set {
            ReturnType::SetOf(return_type_name)
        } else {
            ReturnType::Scalar(return_type_name)
        };

        // Build parameter list, filtering to IN/INOUT/VARIADIC only.
        let has_modes = !arg_modes.is_empty();
        let mut params = Vec::new();
        let mut in_count: i32 = 0;

        for (i, type_name) in arg_type_names.iter().enumerate() {
            let mode = if has_modes {
                arg_modes.get(i).map(|s| s.as_str()).unwrap_or("i")
            } else {
                "i"
            };
            if mode == "i" || mode == "b" || mode == "v" {
                in_count += 1;
                params.push(FuncParam {
                    name: arg_names
                        .get(i)
                        .and_then(|n| n.clone())
                        .unwrap_or_default(),
                    pg_type: type_name.clone(),
                    has_default: in_count > (num_args - num_defaults),
                });
            }
        }

        let qn = QualifiedName::new(schema_name, function_name);
        map.insert(
            qn.clone(),
            Function {
                name: qn,
                params,
                return_type,
                volatility,
                is_procedure: prokind == "p",
                comment,
            },
        );
    }

    Ok(map)
}

async fn load_enums(
    pg: &mut PgPipeline,
) -> Result<HashMap<String, Vec<String>>, SchemaCacheError> {
    let (rows, _) = pg
        .simple_query_rows(ENUMS_QUERY)
        .await
        .map_err(SchemaCacheError::Database)?;
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for row in &rows {
        let type_name = col_str(row, 0);
        let value = col_str(row, 1);
        map.entry(type_name).or_default().push(value);
    }
    Ok(map)
}

// ---------------------------------------------------------------------------
// Relationship builder
// ---------------------------------------------------------------------------

fn build_relationships(
    fks: &[RawForeignKey],
    tables: &HashMap<QualifiedName, Table>,
) -> Vec<Relationship> {
    let mut rels = Vec::new();

    // Direct FK relationships: each FK produces a ManyToOne and a OneToMany.
    for fk in fks {
        let from = QualifiedName::new(&fk.from_schema, &fk.from_table);
        let to = QualifiedName::new(&fk.to_schema, &fk.to_table);
        let col_pairs: Vec<(String, String)> = fk
            .from_columns
            .iter()
            .zip(&fk.to_columns)
            .map(|(a, b)| (a.clone(), b.clone()))
            .collect();

        // FK table -> referenced table (ManyToOne)
        rels.push(Relationship {
            from_table: from.clone(),
            to_table: to.clone(),
            columns: col_pairs.clone(),
            rel_type: RelType::ManyToOne,
            join_table: None,
            constraint_name: fk.constraint_name.clone(),
        });

        // Referenced table -> FK table (OneToMany), swap column pairs
        let reverse_pairs: Vec<(String, String)> =
            col_pairs.iter().map(|(a, b)| (b.clone(), a.clone())).collect();
        rels.push(Relationship {
            from_table: to,
            to_table: from,
            columns: reverse_pairs,
            rel_type: RelType::OneToMany,
            join_table: None,
            constraint_name: fk.constraint_name.clone(),
        });
    }

    // Infer ManyToMany through join tables.
    rels.extend(infer_m2m(fks, tables));

    rels
}

/// A join table is a table with exactly two FK constraints where every column
/// is either part of a FK or part of the primary key (e.g. `post_tags(post_id, tag_id)`).
fn infer_m2m(
    fks: &[RawForeignKey],
    tables: &HashMap<QualifiedName, Table>,
) -> Vec<Relationship> {
    let mut fks_by_table: HashMap<QualifiedName, Vec<&RawForeignKey>> = HashMap::new();
    for fk in fks {
        let qn = QualifiedName::new(&fk.from_schema, &fk.from_table);
        fks_by_table.entry(qn).or_default().push(fk);
    }

    let mut m2m = Vec::new();

    for (table_qn, table_fks) in &fks_by_table {
        if table_fks.len() != 2 {
            continue;
        }
        let table = match tables.get(table_qn) {
            Some(t) => t,
            None => continue,
        };

        let fk_columns: HashSet<&str> = table_fks
            .iter()
            .flat_map(|fk| fk.from_columns.iter().map(String::as_str))
            .collect();

        // Every column must be an FK column or a PK column.
        let is_join_table = table.columns.iter().all(|col| {
            fk_columns.contains(col.name.as_str()) || col.is_pk
        });

        if !is_join_table {
            continue;
        }

        let fk_a = &table_fks[0];
        let fk_b = &table_fks[1];

        let a = QualifiedName::new(&fk_a.to_schema, &fk_a.to_table);
        let b = QualifiedName::new(&fk_b.to_schema, &fk_b.to_table);

        m2m.push(Relationship {
            from_table: a.clone(),
            to_table: b.clone(),
            columns: Vec::new(),
            rel_type: RelType::ManyToMany,
            join_table: Some(table_qn.clone()),
            constraint_name: format!("{}_{}", fk_a.constraint_name, fk_b.constraint_name),
        });

        m2m.push(Relationship {
            from_table: b,
            to_table: a,
            columns: Vec::new(),
            rel_type: RelType::ManyToMany,
            join_table: Some(table_qn.clone()),
            constraint_name: format!("{}_{}", fk_b.constraint_name, fk_a.constraint_name),
        });
    }

    m2m
}
