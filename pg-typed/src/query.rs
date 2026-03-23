//! Typed query client built on pg-wire AsyncConn.
//!
//! Sends queries with binary format parameters and results,
//! returning typed Rows instead of raw bytes.

use bytes::BytesMut;

use pg_wire::protocol::frontend;
use pg_wire::protocol::types::{FormatCode, FrontendMsg};
use pg_wire::{AsyncConn, PipelineResponse, ResponseCollector, WireConn};

use crate::encode::Encode;
use crate::error::TypedError;
use crate::row::Row;

/// A typed query client wrapping an AsyncConn.
/// Sends parameters in binary format and requests binary results.
pub struct Client {
    conn: AsyncConn,
    /// Cached RowDescription for statement cache hits.
    row_desc_cache: std::sync::Mutex<std::collections::HashMap<String, Vec<ColMeta>>>,
}

#[derive(Clone)]
struct ColMeta {
    name: String,
    type_oid: u32,
    format: i16,
}

impl Client {
    /// Create a new typed client from a raw WireConn.
    pub fn new(conn: WireConn) -> Self {
        Self {
            conn: AsyncConn::new(conn),
            row_desc_cache: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Create from an existing AsyncConn.
    pub fn from_async_conn(conn: AsyncConn) -> Self {
        Self {
            conn,
            row_desc_cache: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Connect to PostgreSQL and create a typed client.
    pub async fn connect(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
    ) -> Result<Self, TypedError> {
        let wire = WireConn::connect(addr, user, password, database).await?;
        Ok(Self::new(wire))
    }

    /// Execute a query and return typed rows.
    ///
    /// Parameters are encoded in binary format. Results are requested in binary.
    ///
    /// ```ignore
    /// let rows = client.query("SELECT id, name FROM users WHERE id = $1", &[&42i32]).await?;
    /// for row in &rows {
    ///     let id: i32 = row.get(0)?;
    ///     let name: String = row.get(1)?;
    /// }
    /// ```
    pub async fn query(
        &self,
        sql: &str,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<Vec<Row>, TypedError> {
        let (stmt_name, needs_parse) = self.conn.lookup_or_alloc(sql);

        let mut buf = BytesMut::with_capacity(512);

        // Encode parameters in binary format.
        let param_formats: Vec<FormatCode> = vec![FormatCode::Binary; params.len()];
        let result_formats = [FormatCode::Binary]; // Request binary results.

        let mut param_oids: Vec<u32> = Vec::with_capacity(params.len());
        let mut param_values: Vec<Option<BytesMut>> = Vec::with_capacity(params.len());
        for p in params {
            param_oids.push(p.type_oid().as_u32());
            let mut val_buf = BytesMut::new();
            p.encode(&mut val_buf);
            param_values.push(Some(val_buf));
        }

        // Convert to the wire format expected by pg-wire.
        let param_refs: Vec<Option<&[u8]>> = param_values
            .iter()
            .map(|v| v.as_ref().map(|b| b.as_ref()))
            .collect();

        if needs_parse {
            frontend::encode_message(
                &FrontendMsg::Parse {
                    name: &stmt_name,
                    sql: sql.as_bytes(),
                    param_oids: &param_oids,
                },
                &mut buf,
            );
        }

        frontend::encode_message(
            &FrontendMsg::Bind {
                portal: b"",
                statement: &stmt_name,
                param_formats: &param_formats,
                params: &param_refs,
                result_formats: &result_formats,
            },
            &mut buf,
        );

        // Describe portal to get RowDescription (column names + types).
        // We need this to build typed Rows.
        frontend::encode_message(
            &FrontendMsg::Execute {
                portal: b"",
                max_rows: 0,
            },
            &mut buf,
        );

        frontend::encode_message(&FrontendMsg::Sync, &mut buf);

        let resp = self.conn.submit(buf, ResponseCollector::Rows).await?;
        match resp {
            PipelineResponse::Rows(raw_rows) => {
                // For now, convert raw rows using text format detection.
                // Binary format rows will have raw bytes; text format rows have UTF-8.
                // The format is determined by the result_formats in Bind.
                let rows = raw_rows
                    .into_iter()
                    .map(|data| Row {
                        columns: Vec::new(), // TODO: populate from RowDescription
                        type_oids: Vec::new(),
                        formats: vec![1; data.len()], // Binary format requested
                        data,
                    })
                    .collect();
                Ok(rows)
            }
            PipelineResponse::Done => Ok(Vec::new()),
        }
    }

    /// Execute a query and return exactly one row.
    pub async fn query_one(
        &self,
        sql: &str,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<Row, TypedError> {
        let rows = self.query(sql, params).await?;
        if rows.len() != 1 {
            return Err(TypedError::NotExactlyOne(rows.len()));
        }
        Ok(rows.into_iter().next().unwrap())
    }

    /// Execute a query and return an optional single row.
    pub async fn query_opt(
        &self,
        sql: &str,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<Option<Row>, TypedError> {
        let rows = self.query(sql, params).await?;
        match rows.len() {
            0 => Ok(None),
            1 => Ok(Some(rows.into_iter().next().unwrap())),
            n => Err(TypedError::NotExactlyOne(n)),
        }
    }

    /// Execute a statement that doesn't return rows (INSERT, UPDATE, DELETE).
    pub async fn execute(
        &self,
        sql: &str,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<u64, TypedError> {
        // For now, use the same query path. The row count comes from CommandComplete.
        // TODO: parse CommandComplete tag for affected row count.
        let _rows = self.query(sql, params).await?;
        Ok(0) // Placeholder — needs CommandComplete parsing
    }

    /// Check if the underlying connection is alive.
    pub fn is_alive(&self) -> bool {
        self.conn.is_alive()
    }
}
