use std::collections::{HashMap, HashSet};

use resolute::{Executor, FromRow};

use crate::error::SchemaCacheError;
use pg_schema_cache_types::*;

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
    COALESCE(
        (SELECT array_agg(COALESCE(n, '')) FROM unnest(p.proargnames) AS n),
        ARRAY[]::text[]
    ) AS arg_names,
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
// Row mappings (FromRow-derived)
// ---------------------------------------------------------------------------

#[derive(FromRow)]
struct TableRow {
    table_schema: String,
    table_name: String,
    kind: String,
    comment: Option<String>,
    insertable: bool,
    updatable: bool,
}

#[derive(FromRow)]
struct ColumnRow {
    table_schema: String,
    table_name: String,
    column_name: String,
    pg_type: String,
    nullable: bool,
    has_default: bool,
    default_expr: Option<String>,
    max_length: Option<i32>,
    is_generated: bool,
    comment: Option<String>,
}

#[derive(FromRow)]
struct PrimaryKeyRow {
    table_schema: String,
    table_name: String,
    column_name: String,
}

#[derive(FromRow)]
struct ForeignKeyRow {
    from_schema: String,
    from_table: String,
    to_schema: String,
    to_table: String,
    constraint_name: String,
    from_columns: Vec<String>,
    to_columns: Vec<String>,
}

#[derive(FromRow)]
struct FunctionRow {
    schema_name: String,
    function_name: String,
    volatility: String,
    returns_set: bool,
    return_type_name: String,
    comment: Option<String>,
    prokind: String,
    num_args: i32,
    num_defaults: i32,
    arg_names: Vec<String>,
    arg_modes: Vec<String>,
    arg_type_names: Vec<String>,
}

#[derive(FromRow)]
struct EnumRow {
    type_name: String,
    enum_value: String,
}

// ---------------------------------------------------------------------------
// Public builder
// ---------------------------------------------------------------------------

pub(crate) async fn build(
    db: &impl Executor,
    schemas: &[String],
) -> Result<SchemaCache, SchemaCacheError> {
    let schemas_vec = schemas.to_vec();

    let mut tables = load_tables(db, &schemas_vec).await?;
    let columns = load_columns(db, &schemas_vec).await?;
    let primary_keys = load_primary_keys(db, &schemas_vec).await?;
    let raw_fks = load_foreign_keys(db, &schemas_vec).await?;
    let functions = load_functions(db, &schemas_vec).await?;
    let enums = load_enums(db).await?;

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
// Query loaders
// ---------------------------------------------------------------------------

async fn load_tables(
    db: &impl Executor,
    schemas: &Vec<String>,
) -> Result<Vec<Table>, SchemaCacheError> {
    let rows = db.query(TABLES_QUERY, &[schemas]).await?;
    let mut tables = Vec::with_capacity(rows.len());
    for row in &rows {
        let r = TableRow::from_row(row)?;
        let is_view = r.kind == "v" || r.kind == "m";
        tables.push(Table {
            name: QualifiedName::new(r.table_schema, r.table_name),
            columns: Vec::new(),
            column_index: HashMap::new(),
            primary_key: Vec::new(),
            is_view,
            insertable: r.insertable,
            updatable: r.updatable,
            deletable: r.updatable,
            comment: r.comment,
        });
    }
    Ok(tables)
}

async fn load_columns(
    db: &impl Executor,
    schemas: &Vec<String>,
) -> Result<HashMap<QualifiedName, Vec<Column>>, SchemaCacheError> {
    let rows = db.query(COLUMNS_QUERY, &[schemas]).await?;
    let mut map: HashMap<QualifiedName, Vec<Column>> = HashMap::new();
    for row in &rows {
        let r = ColumnRow::from_row(row)?;
        let qn = QualifiedName::new(r.table_schema, r.table_name);
        map.entry(qn).or_default().push(Column {
            name: r.column_name,
            pg_type: r.pg_type,
            nullable: r.nullable,
            has_default: r.has_default,
            default_expr: r.default_expr,
            max_length: r.max_length,
            is_pk: false, // set later from PK query
            is_generated: r.is_generated,
            comment: r.comment,
            enum_values: None, // set later from enum query
        });
    }
    Ok(map)
}

async fn load_primary_keys(
    db: &impl Executor,
    schemas: &Vec<String>,
) -> Result<HashMap<QualifiedName, Vec<String>>, SchemaCacheError> {
    let rows = db.query(PRIMARY_KEYS_QUERY, &[schemas]).await?;
    let mut map: HashMap<QualifiedName, Vec<String>> = HashMap::new();
    for row in &rows {
        let r = PrimaryKeyRow::from_row(row)?;
        let qn = QualifiedName::new(r.table_schema, r.table_name);
        map.entry(qn).or_default().push(r.column_name);
    }
    Ok(map)
}

async fn load_foreign_keys(
    db: &impl Executor,
    schemas: &Vec<String>,
) -> Result<Vec<ForeignKeyRow>, SchemaCacheError> {
    let rows = db.query(FOREIGN_KEYS_QUERY, &[schemas]).await?;
    let mut fks = Vec::with_capacity(rows.len());
    for row in &rows {
        fks.push(ForeignKeyRow::from_row(row)?);
    }
    Ok(fks)
}

async fn load_functions(
    db: &impl Executor,
    schemas: &Vec<String>,
) -> Result<HashMap<QualifiedName, Function>, SchemaCacheError> {
    let rows = db.query(FUNCTIONS_QUERY, &[schemas]).await?;
    let mut map = HashMap::new();

    for row in &rows {
        let r = FunctionRow::from_row(row)?;

        let volatility = match r.volatility.as_str() {
            "i" => Volatility::Immutable,
            "s" => Volatility::Stable,
            _ => Volatility::Volatile,
        };

        let return_type = if r.return_type_name == "void" {
            ReturnType::Void
        } else if r.returns_set {
            ReturnType::SetOf(r.return_type_name)
        } else {
            ReturnType::Scalar(r.return_type_name)
        };

        // Build parameter list, filtering to IN/INOUT/VARIADIC only.
        let has_modes = !r.arg_modes.is_empty();
        let mut params = Vec::new();
        let mut in_count: i32 = 0;

        for (i, type_name) in r.arg_type_names.iter().enumerate() {
            let mode = if has_modes {
                r.arg_modes.get(i).map(|s| s.as_str()).unwrap_or("i")
            } else {
                "i"
            };
            if mode == "i" || mode == "b" || mode == "v" {
                in_count += 1;
                params.push(FuncParam {
                    name: r.arg_names.get(i).cloned().unwrap_or_default(),
                    pg_type: type_name.clone(),
                    has_default: in_count > (r.num_args - r.num_defaults),
                });
            }
        }

        let qn = QualifiedName::new(r.schema_name, r.function_name);
        map.insert(
            qn.clone(),
            Function {
                name: qn,
                params,
                return_type,
                volatility,
                is_procedure: r.prokind == "p",
                comment: r.comment,
            },
        );
    }

    Ok(map)
}

async fn load_enums(db: &impl Executor) -> Result<HashMap<String, Vec<String>>, SchemaCacheError> {
    let rows = db.query(ENUMS_QUERY, &[]).await?;
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for row in &rows {
        let r = EnumRow::from_row(row)?;
        map.entry(r.type_name).or_default().push(r.enum_value);
    }
    Ok(map)
}

// ---------------------------------------------------------------------------
// Relationship builder
// ---------------------------------------------------------------------------

fn build_relationships(
    fks: &[ForeignKeyRow],
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
        let reverse_pairs: Vec<(String, String)> = col_pairs
            .iter()
            .map(|(a, b)| (b.clone(), a.clone()))
            .collect();
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
fn infer_m2m(fks: &[ForeignKeyRow], tables: &HashMap<QualifiedName, Table>) -> Vec<Relationship> {
    let mut fks_by_table: HashMap<QualifiedName, Vec<&ForeignKeyRow>> = HashMap::new();
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
        let is_join_table = table
            .columns
            .iter()
            .all(|col| fk_columns.contains(col.name.as_str()) || col.is_pk);

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
