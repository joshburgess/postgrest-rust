use pg_schema_cache::{QualifiedName, RelType, Relationship, SchemaCache, Table};

use crate::ast::*;
use crate::error::QueryEngineError;

/// The output of SQL generation: a parameterised query with all bind values as
/// strings. The caller casts via `$N::<pg_type>` emitted in the SQL text.
#[derive(Debug, Clone)]
pub struct SqlOutput {
    pub sql: String,
    pub params: Vec<String>,
}

/// Build parameterised SQL from an API request, using the schema cache for type
/// information and relationship resolution.
pub fn build_sql(
    cache: &SchemaCache,
    request: &ApiRequest,
    schemas: &[String],
) -> Result<SqlOutput, QueryEngineError> {
    let mut b = SqlBuilder::new(cache, schemas);
    let sql = match request {
        ApiRequest::Read(r) => b.build_read(r)?,
        ApiRequest::Insert(r) => b.build_insert(r)?,
        ApiRequest::Update(r) => b.build_update(r)?,
        ApiRequest::Delete(r) => b.build_delete(r)?,
        ApiRequest::CallFunction(r) => b.build_function_call(r)?,
    };
    Ok(SqlOutput {
        sql,
        params: b.params,
    })
}

/// Build a `SELECT count(*)` query that shares the same filters as a read
/// request but ignores ordering, limit, and offset. Used for
/// `Prefer: count=exact`.
pub fn build_count_sql(
    cache: &SchemaCache,
    request: &ReadRequest,
    schemas: &[String],
) -> Result<SqlOutput, QueryEngineError> {
    let mut b = SqlBuilder::new(cache, schemas);
    let table = b.resolve_table(&request.table)?;
    let table_sql = quote_qualified(&request.table);
    let where_clause = b.build_where(&request.filters, table)?;
    let sql = format!("SELECT count(*) FROM {table_sql}{where_clause}");
    Ok(SqlOutput {
        sql,
        params: b.params,
    })
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

struct SqlBuilder<'a> {
    cache: &'a SchemaCache,
    schemas: &'a [String],
    params: Vec<String>,
    embed_counter: usize,
}

impl<'a> SqlBuilder<'a> {
    fn new(cache: &'a SchemaCache, schemas: &'a [String]) -> Self {
        Self {
            cache,
            schemas,
            params: Vec::new(),
            embed_counter: 0,
        }
    }

    fn add_param(&mut self, value: String) -> String {
        self.params.push(value);
        format!("${}", self.params.len())
    }

    fn next_embed_alias(&mut self) -> String {
        let idx = self.embed_counter;
        self.embed_counter += 1;
        format!("_e{idx}")
    }

    fn resolve_table(&self, qn: &QualifiedName) -> Result<&'a Table, QueryEngineError> {
        self.cache
            .tables
            .get(qn)
            .ok_or_else(|| QueryEngineError::TableNotFound(qn.to_string()))
    }

    fn find_table_by_name(&self, name: &str) -> Result<&'a Table, QueryEngineError> {
        self.cache
            .find_table(name, self.schemas)
            .ok_or_else(|| QueryEngineError::TableNotFound(name.to_string()))
    }

    // -----------------------------------------------------------------------
    // READ
    // -----------------------------------------------------------------------

    fn build_read(&mut self, req: &ReadRequest) -> Result<String, QueryEngineError> {
        let table = self.resolve_table(&req.table)?;
        let table_sql = quote_qualified(&req.table);

        let (select_clause, inner_conds) =
            self.build_select_list(&req.select, &req.table, table)?;
        let where_clause = self.build_where(&req.filters, table)?;
        let order_clause = Self::build_order(&req.order);
        let limit_clause = self.build_limit_offset(req.limit, req.offset);

        // Append !inner embed EXISTS conditions to the WHERE clause.
        let inner_where = if inner_conds.is_empty() {
            where_clause
        } else if where_clause.is_empty() {
            format!(" WHERE {}", inner_conds.join(" AND "))
        } else {
            format!("{where_clause} AND {}", inner_conds.join(" AND "))
        };

        let inner = format!(
            "SELECT {select_clause} FROM {table_sql}{inner_where}{order_clause}{limit_clause}"
        );

        Ok(format!(
            "SELECT coalesce(json_agg(_pg_t), '[]')::text FROM ({inner}) _pg_t"
        ))
    }

    /// Returns (select_clause, inner_join_conditions).
    fn build_select_list(
        &mut self,
        items: &[SelectItem],
        parent_qn: &QualifiedName,
        _parent_table: &Table,
    ) -> Result<(String, Vec<String>), QueryEngineError> {
        if items.is_empty() || (items.len() == 1 && matches!(items[0], SelectItem::Star)) {
            return Ok(("*".to_string(), Vec::new()));
        }

        let mut parts = Vec::with_capacity(items.len());
        let mut inner_conditions = Vec::new();

        for item in items {
            match item {
                SelectItem::Column(name) => {
                    parts.push(quote_ident(name));
                }
                SelectItem::Cast { column, pg_type } => {
                    parts.push(format!("{}::{}", quote_ident(column), pg_type));
                }
                SelectItem::JsonAccess {
                    column,
                    path,
                    as_text,
                    cast,
                } => {
                    let op = if *as_text { "->>" } else { "->" };
                    let escaped_path = path.replace('\'', "''");
                    let alias = quote_ident(path);
                    let expr = format!("{}{op}'{escaped_path}'", quote_ident(column));
                    match cast {
                        Some(t) => parts.push(format!("({expr})::{t} AS {alias}")),
                        None => parts.push(format!("{expr} AS {alias}")),
                    }
                }
                SelectItem::Star => {
                    parts.push("*".to_string());
                }
                SelectItem::Embed {
                    alias,
                    target,
                    inner,
                    sub_request,
                } => {
                    let (embed_sql, inner_cond) =
                        self.build_embed_subquery(parent_qn, target, sub_request, *inner)?;
                    let alias_name = alias.as_deref().unwrap_or(target.as_str());
                    parts.push(format!("{embed_sql} AS {}", quote_ident(alias_name)));
                    if let Some(cond) = inner_cond {
                        inner_conditions.push(cond);
                    }
                }
                SelectItem::Spread { target, columns } => {
                    let spread_parts =
                        self.build_spread(parent_qn, target, columns)?;
                    parts.extend(spread_parts);
                }
            }
        }

        Ok((parts.join(", "), inner_conditions))
    }

    /// Generate correlated subqueries for each spread column.
    /// Only valid for ManyToOne (to-one) relationships.
    fn build_spread(
        &self,
        parent_qn: &QualifiedName,
        target_name: &str,
        columns: &[SelectItem],
    ) -> Result<Vec<String>, QueryEngineError> {
        let target_table = self.find_table_by_name(target_name)?;
        let target_qn = target_table.name.clone();
        let target_sql = quote_qualified(&target_qn);
        let parent_sql = quote_qualified(parent_qn);

        // Find ManyToOne relationship from parent to target.
        let rels = self.cache.get_relationships(parent_qn);
        let rel = rels
            .iter()
            .find(|r| {
                r.from_table == *parent_qn
                    && r.to_table == target_qn
                    && r.rel_type == pg_schema_cache::RelType::ManyToOne
            })
            .ok_or_else(|| {
                QueryEngineError::NoRelationship(parent_qn.clone(), target_name.to_string())
            })?;

        // Build join condition.
        let join_cond: Vec<String> = rel
            .columns
            .iter()
            .map(|(from_col, to_col)| {
                format!(
                    "{}.{} = {}.{}",
                    target_sql,
                    quote_ident(to_col),
                    parent_sql,
                    quote_ident(from_col)
                )
            })
            .collect();
        let where_clause = join_cond.join(" AND ");

        // Resolve columns (* = all columns from target table).
        let col_names: Vec<String> = if columns.len() == 1
            && matches!(columns[0], SelectItem::Star)
        {
            target_table
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect()
        } else {
            columns
                .iter()
                .filter_map(|s| match s {
                    SelectItem::Column(n) => Some(n.clone()),
                    SelectItem::Cast { column, .. } => Some(column.clone()),
                    _ => None,
                })
                .collect()
        };

        Ok(col_names
            .iter()
            .map(|col| {
                format!(
                    "(SELECT {} FROM {} WHERE {}) AS {}",
                    quote_ident(col),
                    target_sql,
                    where_clause,
                    quote_ident(col)
                )
            })
            .collect())
    }

    // -----------------------------------------------------------------------
    // Embedding (correlated subqueries)
    // -----------------------------------------------------------------------

    fn build_embed_subquery(
        &mut self,
        parent_qn: &QualifiedName,
        target_name: &str,
        sub_request: &ReadRequest,
        inner_join: bool,
    ) -> Result<(String, Option<String>), QueryEngineError> {
        let target_table = self.find_table_by_name(target_name)?;
        let target_qn = target_table.name.clone();
        let target_sql = quote_qualified(&target_qn);
        let parent_sql = quote_qualified(parent_qn);
        let sub_alias = self.next_embed_alias();

        // Find relationship from parent to target.
        // Prefer the relationship where from_table == parent (natural direction).
        let rels = self.cache.get_relationships(parent_qn);
        let rel = rels
            .iter()
            .find(|r| r.from_table == *parent_qn && r.to_table == target_qn)
            .or_else(|| {
                rels.iter()
                    .find(|r| r.from_table == target_qn && r.to_table == *parent_qn)
            })
            .ok_or_else(|| {
                QueryEngineError::NoRelationship(parent_qn.clone(), target_name.to_string())
            })?;
        let rel = (*rel).clone();

        // Build sub-select columns (ignore inner conditions for nested embeds).
        let (sub_select, _nested_inner) =
            self.build_select_list(&sub_request.select, &target_qn, target_table)?;

        // Join condition.
        let join_conds = match &rel.rel_type {
            RelType::ManyToMany => {
                return self.build_m2m_embed(
                    &rel,
                    parent_qn,
                    &parent_sql,
                    &target_qn,
                    &target_sql,
                    &sub_select,
                    sub_request,
                    target_table,
                    &sub_alias,
                );
            }
            _ => self.build_join_conditions(&rel, parent_qn, &parent_sql, &target_sql),
        };

        // Sub-filters.
        let mut conditions = join_conds;
        conditions.extend(self.build_where_conditions(&sub_request.filters, target_table)?);
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };

        let sub_order = Self::build_order(&sub_request.order);
        let sub_limit = self.build_limit_offset(sub_request.limit, sub_request.offset);

        let inner =
            format!("SELECT {sub_select} FROM {target_sql}{where_clause}{sub_order}{sub_limit}");

        // For !inner, generate an EXISTS condition to filter parent rows.
        let exists_condition = if inner_join {
            Some(format!("EXISTS ({inner})"))
        } else {
            None
        };

        match rel.rel_type {
            RelType::ManyToOne => Ok((
                format!("(SELECT row_to_json({sub_alias}) FROM ({inner}) {sub_alias})"),
                exists_condition,
            )),
            RelType::OneToMany => Ok((
                format!(
                    "COALESCE((SELECT json_agg({sub_alias}) FROM ({inner}) {sub_alias}), '[]')"
                ),
                exists_condition,
            )),
            RelType::ManyToMany => unreachable!(), // handled above
        }
    }

    fn build_join_conditions(
        &self,
        rel: &Relationship,
        parent_qn: &QualifiedName,
        parent_sql: &str,
        target_sql: &str,
    ) -> Vec<String> {
        // Determine which direction the columns go.
        // columns are always (from_col, to_col) relative to from_table.
        if rel.from_table == *parent_qn {
            // Parent is from_table. Correlated ref: parent.from_col = target.to_col
            rel.columns
                .iter()
                .map(|(from_col, to_col)| {
                    format!(
                        "{}.{} = {}.{}",
                        target_sql,
                        quote_ident(to_col),
                        parent_sql,
                        quote_ident(from_col)
                    )
                })
                .collect()
        } else {
            // Parent is to_table (reverse lookup).
            rel.columns
                .iter()
                .map(|(from_col, to_col)| {
                    format!(
                        "{}.{} = {}.{}",
                        target_sql,
                        quote_ident(from_col),
                        parent_sql,
                        quote_ident(to_col)
                    )
                })
                .collect()
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn build_m2m_embed(
        &mut self,
        rel: &Relationship,
        parent_qn: &QualifiedName,
        parent_sql: &str,
        target_qn: &QualifiedName,
        target_sql: &str,
        sub_select: &str,
        sub_request: &ReadRequest,
        target_table: &Table,
        sub_alias: &str,
    ) -> Result<(String, Option<String>), QueryEngineError> {
        let join_qn = rel
            .join_table
            .as_ref()
            .ok_or_else(|| {
                QueryEngineError::NoRelationship(parent_qn.clone(), target_qn.name.clone())
            })?;
        let join_sql = quote_qualified(join_qn);

        // Find M2O rels from join table to parent and target.
        let join_rels = self.cache.get_relationships(join_qn);

        let jt_to_parent = join_rels
            .iter()
            .find(|r| r.rel_type == RelType::ManyToOne && r.to_table == *parent_qn)
            .ok_or_else(|| {
                QueryEngineError::NoRelationship(join_qn.clone(), parent_qn.name.clone())
            })?;

        let jt_to_target = join_rels
            .iter()
            .find(|r| r.rel_type == RelType::ManyToOne && r.to_table == *target_qn)
            .ok_or_else(|| {
                QueryEngineError::NoRelationship(join_qn.clone(), target_qn.name.clone())
            })?;

        // JOIN condition: join_table.fk = target.pk
        let target_join: Vec<String> = jt_to_target
            .columns
            .iter()
            .map(|(jt_col, tgt_col)| {
                format!(
                    "{}.{} = {}.{}",
                    join_sql,
                    quote_ident(jt_col),
                    target_sql,
                    quote_ident(tgt_col)
                )
            })
            .collect();

        // WHERE condition: join_table.fk = parent.pk (correlated)
        let parent_cond: Vec<String> = jt_to_parent
            .columns
            .iter()
            .map(|(jt_col, par_col)| {
                format!(
                    "{}.{} = {}.{}",
                    join_sql,
                    quote_ident(jt_col),
                    parent_sql,
                    quote_ident(par_col)
                )
            })
            .collect();

        let mut where_parts = parent_cond;
        where_parts.extend(self.build_where_conditions(&sub_request.filters, target_table)?);
        let where_clause = format!(" WHERE {}", where_parts.join(" AND "));

        let sub_order = Self::build_order(&sub_request.order);
        let sub_limit = self.build_limit_offset(sub_request.limit, sub_request.offset);

        let inner = format!(
            "SELECT {sub_select} FROM {target_sql} \
             JOIN {join_sql} ON {}{where_clause}{sub_order}{sub_limit}",
            target_join.join(" AND ")
        );

        Ok((
            format!(
                "COALESCE((SELECT json_agg({sub_alias}) FROM ({inner}) {sub_alias}), '[]')"
            ),
            None, // M2M inner join filtering not yet implemented
        ))
    }

    // -----------------------------------------------------------------------
    // INSERT
    // -----------------------------------------------------------------------

    fn build_insert(&mut self, req: &InsertRequest) -> Result<String, QueryEngineError> {
        let table = self.resolve_table(&req.table)?;
        let table_sql = quote_qualified(&req.table);

        // Collect all column names across all rows (preserving order).
        let mut columns: Vec<String> = Vec::new();
        // Collect generated column names to exclude from INSERT.
        let generated_cols: std::collections::HashSet<&str> = table
            .columns
            .iter()
            .filter(|c| c.is_generated)
            .map(|c| c.name.as_str())
            .collect();

        for row in &req.rows {
            for key in row.keys() {
                if !columns.contains(key) && !generated_cols.contains(key.as_str()) {
                    columns.push(key.clone());
                }
            }
        }

        let col_list = columns
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");

        // Build VALUES rows.
        let mut value_rows = Vec::with_capacity(req.rows.len());
        for row in &req.rows {
            let values: Vec<String> = columns
                .iter()
                .map(|col| match row.get(col) {
                    Some(serde_json::Value::Null) | None => "DEFAULT".to_string(),
                    Some(v) => {
                        let text = json_value_to_text(v);
                        let p = self.add_param(text);
                        cast_param(&p, table.get_column(col).map(|c| c.pg_type.as_str()))
                    }
                })
                .collect();
            value_rows.push(format!("({})", values.join(", ")));
        }

        let conflict = self.build_on_conflict(&req.on_conflict, &req.on_conflict_columns, table)?;
        let returning = Self::build_returning(&req.returning);

        let mutation = format!(
            "INSERT INTO {table_sql} ({col_list}) VALUES {}{conflict}{returning}",
            value_rows.join(", ")
        );

        Ok(Self::wrap_mutation(&mutation, &req.returning))
    }

    // -----------------------------------------------------------------------
    // UPDATE
    // -----------------------------------------------------------------------

    fn build_update(&mut self, req: &UpdateRequest) -> Result<String, QueryEngineError> {
        let table = self.resolve_table(&req.table)?;
        let table_sql = quote_qualified(&req.table);

        let set_parts: Vec<String> = req
            .set
            .iter()
            .map(|(col, val)| {
                if val.is_null() {
                    format!("{} = NULL", quote_ident(col))
                } else {
                    let text = json_value_to_text(val);
                    let p = self.add_param(text);
                    let cast =
                        cast_param(&p, table.get_column(col).map(|c| c.pg_type.as_str()));
                    format!("{} = {cast}", quote_ident(col))
                }
            })
            .collect();

        let where_clause = self.build_where(&req.filters, table)?;
        let returning = Self::build_returning(&req.returning);

        let mutation = format!(
            "UPDATE {table_sql} SET {}{where_clause}{returning}",
            set_parts.join(", ")
        );

        Ok(Self::wrap_mutation(&mutation, &req.returning))
    }

    // -----------------------------------------------------------------------
    // DELETE
    // -----------------------------------------------------------------------

    fn build_delete(&mut self, req: &DeleteRequest) -> Result<String, QueryEngineError> {
        let table = self.resolve_table(&req.table)?;
        let table_sql = quote_qualified(&req.table);

        let where_clause = self.build_where(&req.filters, table)?;
        let returning = Self::build_returning(&req.returning);

        let mutation = format!("DELETE FROM {table_sql}{where_clause}{returning}");

        Ok(Self::wrap_mutation(&mutation, &req.returning))
    }

    // -----------------------------------------------------------------------
    // Function call
    // -----------------------------------------------------------------------

    fn build_function_call(&mut self, req: &FunctionCall) -> Result<String, QueryEngineError> {
        let func = self
            .cache
            .find_function(&req.function.name, self.schemas)
            .ok_or_else(|| QueryEngineError::FunctionNotFound(req.function.to_string()))?;

        let func_sql = quote_qualified(&req.function);

        // Procedures use CALL instead of SELECT.
        if func.is_procedure {
            let args: Vec<String> = req
                .params
                .iter()
                .map(|(name, val)| {
                    let text = json_value_to_text(val);
                    let p = self.add_param(text);
                    let pg_type = func
                        .params
                        .iter()
                        .find(|fp| fp.name == *name)
                        .map(|fp| fp.pg_type.as_str());
                    let cast = cast_param(&p, pg_type);
                    format!("{} => {cast}", quote_ident(name))
                })
                .collect();
            return Ok(format!("CALL {func_sql}({})", args.join(", ")));
        }

        // Build named arguments: param_name => $N::type
        let args: Vec<String> = req
            .params
            .iter()
            .map(|(name, val)| {
                let text = json_value_to_text(val);
                let p = self.add_param(text);
                let pg_type = func
                    .params
                    .iter()
                    .find(|fp| fp.name == *name)
                    .map(|fp| fp.pg_type.as_str());
                let cast = cast_param(&p, pg_type);
                format!("{} => {cast}", quote_ident(name))
            })
            .collect();
        let args_sql = args.join(", ");

        if let Some(read_req) = &req.read_request {
            // Function result is filtered/ordered like a table.
            let table_ref = format!("{func_sql}({args_sql})");
            let select = if read_req.select.is_empty()
                || (read_req.select.len() == 1 && matches!(read_req.select[0], SelectItem::Star))
            {
                "*".to_string()
            } else {
                read_req
                    .select
                    .iter()
                    .filter_map(|item| match item {
                        SelectItem::Column(c) => Some(quote_ident(c)),
                        SelectItem::Cast { column, pg_type } => {
                            Some(format!("{}::{}", quote_ident(column), pg_type))
                        }
                        SelectItem::JsonAccess { column, path, as_text, cast } => {
                            let op = if *as_text { "->>" } else { "->" };
                            let escaped = path.replace('\'', "''");
                            let alias = quote_ident(path);
                            let expr = format!("{}{op}'{escaped}'", quote_ident(column));
                            Some(match cast {
                                Some(t) => format!("({expr})::{t} AS {alias}"),
                                None => format!("{expr} AS {alias}"),
                            })
                        }
                        SelectItem::Star => Some("*".to_string()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            };

            // We need a temporary Table to build filters against. Functions
            // returning SET OF a table type share that table's columns, but
            // we may not have that info. Build a minimal table with no column
            // metadata so filters skip the type cast.
            let dummy_table = Table {
                name: req.function.clone(),
                columns: Vec::new(),
                column_index: std::collections::HashMap::new(),
                primary_key: Vec::new(),
                is_view: false,
                insertable: false,
                updatable: false,
                deletable: false,
                comment: None,
            };

            let where_clause = self.build_where(&read_req.filters, &dummy_table)?;
            let order_clause = Self::build_order(&read_req.order);
            let limit_clause = self.build_limit_offset(read_req.limit, read_req.offset);

            let inner = format!(
                "SELECT {select} FROM {table_ref} _pg_f{where_clause}{order_clause}{limit_clause}"
            );

            if req.is_scalar {
                Ok(format!(
                    "SELECT row_to_json(_pg_t)::text FROM ({inner}) _pg_t"
                ))
            } else {
                Ok(format!(
                    "SELECT coalesce(json_agg(_pg_t), '[]')::text FROM ({inner}) _pg_t"
                ))
            }
        } else if req.is_scalar {
            Ok(format!(
                "SELECT to_jsonb({func_sql}({args_sql}))::text"
            ))
        } else {
            Ok(format!(
                "SELECT coalesce(json_agg(_pg_t), '[]')::text \
                 FROM {func_sql}({args_sql}) _pg_t"
            ))
        }
    }

    // -----------------------------------------------------------------------
    // WHERE clause
    // -----------------------------------------------------------------------

    fn build_where(
        &mut self,
        filters: &FilterNode,
        table: &Table,
    ) -> Result<String, QueryEngineError> {
        if filters.is_empty() {
            return Ok(String::new());
        }
        let condition = self.build_filter_node(filters, table)?;
        if condition.is_empty() {
            Ok(String::new())
        } else {
            Ok(format!(" WHERE {condition}"))
        }
    }

    /// Returns filter conditions as a Vec of SQL strings (for combining
    /// with JOIN conditions in embedding subqueries).
    fn build_where_conditions(
        &mut self,
        filters: &FilterNode,
        table: &Table,
    ) -> Result<Vec<String>, QueryEngineError> {
        if filters.is_empty() {
            return Ok(Vec::new());
        }
        let condition = self.build_filter_node(filters, table)?;
        if condition.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(vec![condition])
        }
    }

    fn build_filter_node(
        &mut self,
        node: &FilterNode,
        table: &Table,
    ) -> Result<String, QueryEngineError> {
        match node {
            FilterNode::And(children) => {
                let parts: Vec<String> = children
                    .iter()
                    .map(|c| self.build_filter_node(c, table))
                    .collect::<Result<_, _>>()?;
                let parts: Vec<&str> = parts.iter().filter(|s| !s.is_empty()).map(|s| s.as_str()).collect();
                match parts.len() {
                    0 => Ok(String::new()),
                    1 => Ok(parts[0].to_string()),
                    _ => Ok(format!("({})", parts.join(" AND "))),
                }
            }
            FilterNode::Or(children) => {
                let parts: Vec<String> = children
                    .iter()
                    .map(|c| self.build_filter_node(c, table))
                    .collect::<Result<_, _>>()?;
                let parts: Vec<&str> = parts.iter().filter(|s| !s.is_empty()).map(|s| s.as_str()).collect();
                match parts.len() {
                    0 => Ok(String::new()),
                    1 => Ok(parts[0].to_string()),
                    _ => Ok(format!("({})", parts.join(" OR "))),
                }
            }
            FilterNode::Not(child) => {
                let inner = self.build_filter_node(child, table)?;
                Ok(format!("NOT ({inner})"))
            }
            FilterNode::Condition(filter) => self.build_filter(filter, table),
        }
    }

    fn build_filter(
        &mut self,
        filter: &Filter,
        table: &Table,
    ) -> Result<String, QueryEngineError> {
        let col = quote_ident(&filter.column);
        let pg_type = table
            .get_column(&filter.column)
            .map(|c| c.pg_type.as_str());

        let condition = match (&filter.operator, &filter.value) {
            (FilterOp::Is, FilterValue::Value(v)) => {
                let kw = if filter.negated { "IS NOT" } else { "IS" };
                match v.to_lowercase().as_str() {
                    "null" => format!("{col} {kw} NULL"),
                    "true" => format!("{col} {kw} TRUE"),
                    "false" => format!("{col} {kw} FALSE"),
                    "unknown" => format!("{col} {kw} UNKNOWN"),
                    _ => {
                        return Err(QueryEngineError::Parse(
                            crate::error::ParseError::InvalidIsValue(v.clone()),
                        ))
                    }
                }
            }
            (FilterOp::In, FilterValue::List(values)) => {
                let placeholders: Vec<String> = values
                    .iter()
                    .map(|v| {
                        let p = self.add_param(v.clone());
                        cast_param(&p, pg_type)
                    })
                    .collect();
                let expr = format!("{col} IN ({})", placeholders.join(", "));
                if filter.negated {
                    format!("NOT ({expr})")
                } else {
                    expr
                }
            }
            (op, FilterValue::Value(v)) => {
                let p = self.add_param(v.clone());
                let cast = cast_param(&p, pg_type);
                let expr = match op {
                    FilterOp::Eq => format!("{col} = {cast}"),
                    FilterOp::Neq => format!("{col} <> {cast}"),
                    FilterOp::Gt => format!("{col} > {cast}"),
                    FilterOp::Gte => format!("{col} >= {cast}"),
                    FilterOp::Lt => format!("{col} < {cast}"),
                    FilterOp::Lte => format!("{col} <= {cast}"),
                    FilterOp::Like => format!("{col} LIKE {cast}"),
                    FilterOp::Ilike => format!("{col} ILIKE {cast}"),
                    FilterOp::Contains => format!("{col} @> {cast}"),
                    FilterOp::ContainedIn => format!("{col} <@ {cast}"),
                    FilterOp::Overlaps => format!("{col} && {cast}"),
                    FilterOp::Fts(lang) => fts_expr(&col, "to_tsquery", lang.as_deref(), &p),
                    FilterOp::Plfts(lang) => {
                        fts_expr(&col, "plainto_tsquery", lang.as_deref(), &p)
                    }
                    FilterOp::Phfts(lang) => {
                        fts_expr(&col, "phraseto_tsquery", lang.as_deref(), &p)
                    }
                    FilterOp::Wfts(lang) => {
                        fts_expr(&col, "websearch_to_tsquery", lang.as_deref(), &p)
                    }
                    FilterOp::Is | FilterOp::In => unreachable!(),
                };
                if filter.negated {
                    format!("NOT ({expr})")
                } else {
                    expr
                }
            }
            _ => {
                return Err(QueryEngineError::Parse(
                    crate::error::ParseError::InvalidFilter("bad filter combination".to_string()),
                ))
            }
        };

        Ok(condition)
    }

    // -----------------------------------------------------------------------
    // ORDER BY
    // -----------------------------------------------------------------------

    fn build_order(clauses: &[OrderClause]) -> String {
        if clauses.is_empty() {
            return String::new();
        }
        let parts: Vec<String> = clauses
            .iter()
            .map(|c| {
                let dir = match c.direction {
                    OrderDirection::Asc => "ASC",
                    OrderDirection::Desc => "DESC",
                };
                let nulls = match c.nulls {
                    Some(NullsOrder::First) => " NULLS FIRST",
                    Some(NullsOrder::Last) => " NULLS LAST",
                    None => "",
                };
                format!("{} {dir}{nulls}", quote_ident(&c.column))
            })
            .collect();
        format!(" ORDER BY {}", parts.join(", "))
    }

    // -----------------------------------------------------------------------
    // LIMIT / OFFSET
    // -----------------------------------------------------------------------

    fn build_limit_offset(&mut self, limit: Option<i64>, offset: Option<i64>) -> String {
        let mut s = String::new();
        if let Some(l) = limit {
            let p = self.add_param(l.to_string());
            s.push_str(&format!(" LIMIT ({p}::text)::int8"));
        }
        if let Some(o) = offset {
            let p = self.add_param(o.to_string());
            s.push_str(&format!(" OFFSET ({p}::text)::int8"));
        }
        s
    }

    // -----------------------------------------------------------------------
    // ON CONFLICT
    // -----------------------------------------------------------------------

    fn build_on_conflict(
        &self,
        action: &Option<ConflictAction>,
        conflict_columns: &Option<Vec<String>>,
        table: &Table,
    ) -> Result<String, QueryEngineError> {
        let action = match action {
            Some(a) => a,
            None => return Ok(String::new()),
        };

        // Use explicit conflict columns if provided, otherwise PK.
        let conflict_cols = if let Some(cols) = conflict_columns {
            cols.clone()
        } else if !table.primary_key.is_empty() {
            table.primary_key.clone()
        } else {
            return Err(QueryEngineError::NoPrimaryKey(table.name.clone()));
        };

        let pk_list = conflict_cols
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");

        match action {
            ConflictAction::IgnoreDuplicates => Ok(format!(" ON CONFLICT ({pk_list}) DO NOTHING")),
            ConflictAction::MergeDuplicates => {
                let set_parts: Vec<String> = table
                    .columns
                    .iter()
                    .filter(|c| !c.is_pk)
                    .map(|c| {
                        let q = quote_ident(&c.name);
                        format!("{q} = EXCLUDED.{q}")
                    })
                    .collect();
                if set_parts.is_empty() {
                    Ok(format!(" ON CONFLICT ({pk_list}) DO NOTHING"))
                } else {
                    Ok(format!(
                        " ON CONFLICT ({pk_list}) DO UPDATE SET {}",
                        set_parts.join(", ")
                    ))
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // RETURNING / mutation wrapping
    // -----------------------------------------------------------------------

    fn build_returning(cols: &[String]) -> String {
        if cols.is_empty() {
            return String::new();
        }
        if cols.len() == 1 && cols[0] == "*" {
            return " RETURNING *".to_string();
        }
        let list = cols
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        format!(" RETURNING {list}")
    }

    /// If the mutation has a RETURNING clause, wrap it in a CTE that
    /// JSON-aggregates the result.
    fn wrap_mutation(mutation: &str, returning: &[String]) -> String {
        if returning.is_empty() {
            mutation.to_string()
        } else {
            format!(
                "WITH _pg_mut AS ({mutation}) \
                 SELECT coalesce(json_agg(_pg_t), '[]')::text FROM _pg_mut _pg_t"
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Double-quote a SQL identifier, escaping any embedded double-quotes.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Schema-qualify and double-quote a name pair: `"schema"."name"`.
fn quote_qualified(qn: &QualifiedName) -> String {
    format!("{}.{}", quote_ident(&qn.schema), quote_ident(&qn.name))
}

/// Cast a text parameter to the target PostgreSQL type.
///
/// All bind parameters are sent as `text` via tokio-postgres. A direct
/// `$1::int4` would fail at the protocol level because the driver tries to
/// serialise `String` as `int4`. Using the double-cast `($1::text)::int4`
/// ensures the driver sends `text` and PostgreSQL converts server-side.
fn cast_param(placeholder: &str, pg_type: Option<&str>) -> String {
    match pg_type {
        Some("text" | "varchar" | "bpchar" | "name" | "citext") | None => {
            placeholder.to_string()
        }
        Some(t) => format!("({placeholder}::text)::{t}"),
    }
}

/// Convert a `serde_json::Value` to its text representation for binding.
fn json_value_to_text(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Build a full-text-search expression.
fn fts_expr(col: &str, tsquery_fn: &str, lang: Option<&str>, param: &str) -> String {
    match lang {
        Some(l) => format!("{col} @@ {tsquery_fn}('{l}', {param})"),
        None => format!("{col} @@ {tsquery_fn}({param})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_schema_cache::*;
    use std::collections::HashMap;

    fn test_cache() -> SchemaCache {
        let mut tables = HashMap::new();
        tables.insert(
            QualifiedName::new("api", "users"),
            Table {
                name: QualifiedName::new("api", "users"),
                columns: vec![
                    Column {
                        name: "id".into(),
                        pg_type: "int4".into(),
                        nullable: false,
                        has_default: true,
                        default_expr: None,
                        max_length: None,
                        is_pk: true,
                        is_generated: false,
                        comment: None,
                        enum_values: None,
                    },
                    Column {
                        name: "name".into(),
                        pg_type: "text".into(),
                        nullable: false,
                        has_default: false,
                        default_expr: None,
                        max_length: None,
                        is_pk: false,
                        is_generated: false,
                        comment: None,
                        enum_values: None,
                    },
                    Column {
                        name: "age".into(),
                        pg_type: "int4".into(),
                        nullable: true,
                        has_default: false,
                        default_expr: None,
                        max_length: None,
                        is_pk: false,
                        is_generated: false,
                        comment: None,
                        enum_values: None,
                    },
                ],
                column_index: [("id".to_string(), 0), ("name".to_string(), 1), ("age".to_string(), 2)]
                    .into_iter().collect(),
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

    #[test]
    fn test_build_simple_read() {
        let cache = test_cache();
        let req = ApiRequest::Read(ReadRequest {
            table: QualifiedName::new("api", "users"),
            select: vec![
                SelectItem::Column("id".into()),
                SelectItem::Column("name".into()),
            ],
            filters: FilterNode::from_filters(vec![Filter {
                column: "age".into(),
                operator: FilterOp::Gt,
                value: FilterValue::Value("18".into()),
                negated: false,
            }]),
            order: vec![OrderClause {
                column: "name".into(),
                direction: OrderDirection::Asc,
                nulls: None,
            }],
            limit: Some(10),
            offset: None,
            count: CountOption::None,
        });

        let out = build_sql(&cache, &req, &["api".into()]).unwrap();
        assert!(out.sql.contains("json_agg"));
        assert!(out.sql.contains("\"id\", \"name\""));
        assert!(out.sql.contains("\"age\" > ($1::text)::int4"));
        assert!(out.sql.contains("ORDER BY \"name\" ASC"));
        assert!(out.sql.contains("LIMIT ($2::text)::int8"));
        assert_eq!(out.params, vec!["18", "10"]);
    }

    #[test]
    fn test_build_insert_with_upsert() {
        let cache = test_cache();
        let mut row = serde_json::Map::new();
        row.insert("name".into(), serde_json::Value::String("alice".into()));
        row.insert("age".into(), serde_json::json!(30));

        let req = ApiRequest::Insert(InsertRequest {
            table: QualifiedName::new("api", "users"),
            rows: vec![row],
            on_conflict: Some(ConflictAction::MergeDuplicates),
            on_conflict_columns: None,
            returning: vec!["*".into()],
        });

        let out = build_sql(&cache, &req, &["api".into()]).unwrap();
        assert!(out.sql.contains("INSERT INTO"));
        assert!(out.sql.contains("ON CONFLICT"));
        assert!(out.sql.contains("DO UPDATE SET"));
        assert!(out.sql.contains("RETURNING *"));
        assert!(out.sql.contains("json_agg"));
    }

    #[test]
    fn test_build_delete() {
        let cache = test_cache();
        let req = ApiRequest::Delete(DeleteRequest {
            table: QualifiedName::new("api", "users"),
            filters: FilterNode::from_filters(vec![Filter {
                column: "id".into(),
                operator: FilterOp::Eq,
                value: FilterValue::Value("42".into()),
                negated: false,
            }]),
            returning: vec!["*".into()],
        });

        let out = build_sql(&cache, &req, &["api".into()]).unwrap();
        assert!(out.sql.contains("DELETE FROM"));
        assert!(out.sql.contains("\"id\" = ($1::text)::int4"));
        assert!(out.sql.contains("RETURNING *"));
    }
}
