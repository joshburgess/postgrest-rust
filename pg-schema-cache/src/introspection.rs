use std::collections::{HashMap, HashSet};

use tokio_postgres::Client;

use crate::error::SchemaCacheError;
use crate::types::*;

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
// Public builder
// ---------------------------------------------------------------------------

pub(crate) async fn build(
    client: &Client,
    schemas: &[String],
) -> Result<SchemaCache, SchemaCacheError> {
    let mut tables = load_tables(client, schemas).await?;
    let columns = load_columns(client, schemas).await?;
    let primary_keys = load_primary_keys(client, schemas).await?;
    let raw_fks = load_foreign_keys(client, schemas).await?;
    let functions = load_functions(client, schemas).await?;
    let enums = load_enums(client).await?;

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
        .map(|t| (t.name.clone(), t))
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
    client: &Client,
    schemas: &[String],
) -> Result<Vec<Table>, SchemaCacheError> {
    let rows = client.query(TABLES_QUERY, &[&schemas]).await?;
    let mut tables = Vec::with_capacity(rows.len());
    for row in &rows {
        let kind: String = row.get("kind");
        let is_view = kind == "v" || kind == "m";
        let insertable: bool = row.get("insertable");
        let updatable: bool = row.get("updatable");
        tables.push(Table {
            name: QualifiedName::new(
                row.get::<_, String>("table_schema"),
                row.get::<_, String>("table_name"),
            ),
            columns: Vec::new(),
            primary_key: Vec::new(),
            is_view,
            insertable,
            updatable,
            deletable: updatable,
            comment: row.get("comment"),
        });
    }
    Ok(tables)
}

async fn load_columns(
    client: &Client,
    schemas: &[String],
) -> Result<HashMap<QualifiedName, Vec<Column>>, SchemaCacheError> {
    let rows = client.query(COLUMNS_QUERY, &[&schemas]).await?;
    let mut map: HashMap<QualifiedName, Vec<Column>> = HashMap::new();
    for row in &rows {
        let qn = QualifiedName::new(
            row.get::<_, String>("table_schema"),
            row.get::<_, String>("table_name"),
        );
        map.entry(qn).or_default().push(Column {
            name: row.get("column_name"),
            pg_type: row.get("pg_type"),
            nullable: row.get("nullable"),
            has_default: row.get("has_default"),
            default_expr: row.get("default_expr"),
            max_length: row.get("max_length"),
            is_pk: false, // set later from PK query
            is_generated: row.get("is_generated"),
            comment: row.get("comment"),
            enum_values: None, // set later from enum query
        });
    }
    Ok(map)
}

async fn load_primary_keys(
    client: &Client,
    schemas: &[String],
) -> Result<HashMap<QualifiedName, Vec<String>>, SchemaCacheError> {
    let rows = client.query(PRIMARY_KEYS_QUERY, &[&schemas]).await?;
    let mut map: HashMap<QualifiedName, Vec<String>> = HashMap::new();
    for row in &rows {
        let qn = QualifiedName::new(
            row.get::<_, String>("table_schema"),
            row.get::<_, String>("table_name"),
        );
        map.entry(qn)
            .or_default()
            .push(row.get("column_name"));
    }
    Ok(map)
}

async fn load_foreign_keys(
    client: &Client,
    schemas: &[String],
) -> Result<Vec<RawForeignKey>, SchemaCacheError> {
    let rows = client.query(FOREIGN_KEYS_QUERY, &[&schemas]).await?;
    let mut fks = Vec::with_capacity(rows.len());
    for row in &rows {
        fks.push(RawForeignKey {
            from_schema: row.get("from_schema"),
            from_table: row.get("from_table"),
            to_schema: row.get("to_schema"),
            to_table: row.get("to_table"),
            constraint_name: row.get("constraint_name"),
            from_columns: row.get("from_columns"),
            to_columns: row.get("to_columns"),
        });
    }
    Ok(fks)
}

async fn load_functions(
    client: &Client,
    schemas: &[String],
) -> Result<HashMap<QualifiedName, Function>, SchemaCacheError> {
    let rows = client.query(FUNCTIONS_QUERY, &[&schemas]).await?;
    let mut map = HashMap::new();

    for row in &rows {
        let schema_name: String = row.get("schema_name");
        let function_name: String = row.get("function_name");
        let volatility_char: String = row.get("volatility");
        let returns_set: bool = row.get("returns_set");
        let return_type_name: String = row.get("return_type_name");
        let num_args: i32 = row.get("num_args");
        let num_defaults: i32 = row.get("num_defaults");
        let arg_names: Vec<Option<String>> = row.get("arg_names");
        let arg_modes: Vec<String> = row.get("arg_modes");
        let arg_type_names: Vec<String> = row.get("arg_type_names");

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
                comment: row.get("comment"),
            },
        );
    }

    Ok(map)
}

async fn load_enums(
    client: &Client,
) -> Result<HashMap<String, Vec<String>>, SchemaCacheError> {
    let rows = client.query(ENUMS_QUERY, &[]).await?;
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for row in &rows {
        let type_name: String = row.get("type_name");
        let value: String = row.get("enum_value");
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

        // FK table → referenced table (ManyToOne)
        rels.push(Relationship {
            from_table: from.clone(),
            to_table: to.clone(),
            columns: col_pairs.clone(),
            rel_type: RelType::ManyToOne,
            join_table: None,
            constraint_name: fk.constraint_name.clone(),
        });

        // Referenced table → FK table (OneToMany), swap column pairs
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
