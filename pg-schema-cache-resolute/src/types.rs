use std::collections::HashMap;

/// A schema-qualified database object name.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct QualifiedName {
    pub schema: String,
    pub name: String,
}

impl QualifiedName {
    pub fn new(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            name: name.into(),
        }
    }
}

impl std::fmt::Display for QualifiedName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "\"{}\".\"{}\"", self.schema, self.name)
    }
}

/// In-memory representation of a PostgreSQL database schema.
#[derive(Debug, Clone)]
pub struct SchemaCache {
    pub tables: HashMap<QualifiedName, Table>,
    pub relationships: Vec<Relationship>,
    pub functions: HashMap<QualifiedName, Function>,
}

impl SchemaCache {
    pub fn get_table(&self, schema: &str, name: &str) -> Option<&Table> {
        self.tables.get(&QualifiedName {
            schema: schema.to_owned(),
            name: name.to_owned(),
        })
    }

    /// Find a table by unqualified name, searching schemas in order.
    pub fn find_table(&self, name: &str, schemas: &[String]) -> Option<&Table> {
        schemas
            .iter()
            .find_map(|schema| self.get_table(schema, name))
    }

    /// Get all relationships involving a table (as source or target).
    pub fn get_relationships(&self, table: &QualifiedName) -> Vec<&Relationship> {
        self.relationships
            .iter()
            .filter(|r| r.from_table == *table || r.to_table == *table)
            .collect()
    }

    pub fn get_function(&self, schema: &str, name: &str) -> Option<&Function> {
        self.functions.get(&QualifiedName {
            schema: schema.to_owned(),
            name: name.to_owned(),
        })
    }

    /// Find a function by unqualified name, searching schemas in order.
    pub fn find_function(&self, name: &str, schemas: &[String]) -> Option<&Function> {
        schemas
            .iter()
            .find_map(|schema| self.get_function(schema, name))
    }
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: QualifiedName,
    pub columns: Vec<Column>,
    /// O(1) column lookup: name → index into `columns`.
    pub column_index: HashMap<String, usize>,
    pub primary_key: Vec<String>,
    pub is_view: bool,
    pub insertable: bool,
    pub updatable: bool,
    pub deletable: bool,
    pub comment: Option<String>,
}

impl Table {
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.column_index
            .get(name)
            .and_then(|&idx| self.columns.get(idx))
    }

    /// Rebuild the column_index after modifying columns.
    pub fn rebuild_column_index(&mut self) {
        self.column_index = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    /// PostgreSQL type name (udt_name), e.g. "int4", "text", "timestamptz".
    /// Array types have a leading underscore, e.g. "_int4".
    pub pg_type: String,
    pub nullable: bool,
    pub has_default: bool,
    pub default_expr: Option<String>,
    pub max_length: Option<i32>,
    pub is_pk: bool,
    /// True for GENERATED ALWAYS AS (...) STORED columns.
    pub is_generated: bool,
    pub comment: Option<String>,
    pub enum_values: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelType {
    OneToMany,
    ManyToOne,
    ManyToMany,
}

#[derive(Debug, Clone)]
pub struct Relationship {
    pub from_table: QualifiedName,
    pub to_table: QualifiedName,
    /// Column pairs: (from_column, to_column). Empty for ManyToMany.
    pub columns: Vec<(String, String)>,
    pub rel_type: RelType,
    /// For ManyToMany, the join table that links the two tables.
    pub join_table: Option<QualifiedName>,
    pub constraint_name: String,
}

#[derive(Debug, Clone)]
pub struct Function {
    pub name: QualifiedName,
    pub params: Vec<FuncParam>,
    pub return_type: ReturnType,
    pub volatility: Volatility,
    /// True for procedures (CALL), false for functions (SELECT).
    pub is_procedure: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FuncParam {
    pub name: String,
    pub pg_type: String,
    pub has_default: bool,
}

#[derive(Debug, Clone)]
pub enum ReturnType {
    Scalar(String),
    SetOf(String),
    Table(Vec<Column>),
    Void,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Volatility {
    Immutable,
    Stable,
    Volatile,
}
