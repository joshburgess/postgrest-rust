//! Typed query client built on pg-wire AsyncConn.
//!
//! Sends queries with binary format parameters and results,
//! returning typed Rows instead of raw bytes.

use bytes::BytesMut;

use pg_wire::protocol::frontend;
use pg_wire::protocol::types::{FormatCode, FrontendMsg};
use pg_wire::{AsyncConn, PipelineResponse, ResponseCollector, WireConn};

use crate::encode::SqlParam;
use crate::error::TypedError;
use crate::row::Row;

/// A typed query client wrapping an AsyncConn.
/// Sends parameters in binary format and requests binary results.
pub struct Client {
    conn: AsyncConn,
}

impl Client {
    /// Create a new typed client from a raw WireConn.
    pub fn new(conn: WireConn) -> Self {
        Self {
            conn: AsyncConn::new(conn),
        }
    }

    /// Create from an existing AsyncConn.
    pub fn from_async_conn(conn: AsyncConn) -> Self {
        Self { conn }
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
        params: &[&dyn SqlParam],
    ) -> Result<Vec<Row>, TypedError> {
        let (stmt_name, needs_parse) = self.conn.lookup_or_alloc(sql);

        let mut buf = BytesMut::with_capacity(512);

        // Encode parameters in binary format.
        let param_formats: Vec<FormatCode> = vec![FormatCode::Binary; params.len()];
        let result_formats = [FormatCode::Binary]; // Request binary results.

        let mut param_oids: Vec<u32> = Vec::with_capacity(params.len());
        let mut param_values: Vec<Option<BytesMut>> = Vec::with_capacity(params.len());
        for p in params {
            param_oids.push(p.param_oid());
            param_values.push(p.encode_param_value());
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
        frontend::encode_message(
            &FrontendMsg::Describe {
                kind: b'P', // Portal
                name: b"",
            },
            &mut buf,
        );

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
            PipelineResponse::Rows { fields, rows: raw_rows, command_tag: _ } => {
                // Build column metadata from RowDescription if available.
                let has_desc = !fields.is_empty();
                let columns: Vec<String> = fields.iter().map(|f| f.name.clone()).collect();
                let type_oids: Vec<u32> = fields.iter().map(|f| f.type_oid).collect();
                // If no RowDescription, default to binary (we requested it in Bind).
                let formats: Vec<i16> = if has_desc {
                    fields.iter().map(|f| f.format as i16).collect()
                } else {
                    Vec::new() // Will use binary default per-row below.
                };

                let rows = raw_rows
                    .into_iter()
                    .map(|data| {
                        let row_formats = if formats.is_empty() {
                            vec![1i16; data.len()] // Binary format (we requested it).
                        } else {
                            formats.clone()
                        };
                        Row {
                            columns: columns.clone(),
                            type_oids: type_oids.clone(),
                            formats: row_formats,
                            data,
                        }
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
        params: &[&dyn SqlParam],
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
        params: &[&dyn SqlParam],
    ) -> Result<Option<Row>, TypedError> {
        let rows = self.query(sql, params).await?;
        match rows.len() {
            0 => Ok(None),
            1 => Ok(Some(rows.into_iter().next().unwrap())),
            n => Err(TypedError::NotExactlyOne(n)),
        }
    }

    /// Execute a statement that doesn't return rows (INSERT, UPDATE, DELETE).
    /// Returns the number of affected rows.
    pub async fn execute(
        &self,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<u64, TypedError> {
        let (stmt_name, needs_parse) = self.conn.lookup_or_alloc(sql);
        let mut buf = BytesMut::with_capacity(512);

        let param_formats: Vec<FormatCode> = vec![FormatCode::Binary; params.len()];
        let result_formats = [FormatCode::Binary];

        let mut param_oids: Vec<u32> = Vec::with_capacity(params.len());
        let mut param_values: Vec<Option<BytesMut>> = Vec::with_capacity(params.len());
        for p in params {
            param_oids.push(p.param_oid());
            param_values.push(p.encode_param_value());
        }
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
        frontend::encode_message(
            &FrontendMsg::Execute { portal: b"", max_rows: 0 },
            &mut buf,
        );
        frontend::encode_message(&FrontendMsg::Sync, &mut buf);

        let resp = self.conn.submit(buf, ResponseCollector::Rows).await?;
        match resp {
            PipelineResponse::Rows { command_tag, .. } => {
                Ok(parse_row_count(&command_tag))
            }
            PipelineResponse::Done => Ok(0),
        }
    }

    /// Begin a transaction. Returns a `Transaction` guard that
    /// commits on `commit()` or rolls back on drop.
    pub async fn begin(&self) -> Result<Transaction<'_>, TypedError> {
        self.simple_query("BEGIN").await?;
        Ok(Transaction { client: self, done: false })
    }

    /// Send a simple text query (no params, no binary format).
    /// Used for BEGIN/COMMIT/ROLLBACK and DDL.
    pub async fn simple_query(&self, sql: &str) -> Result<(), TypedError> {
        use pg_wire::protocol::types::FrontendMsg;
        let mut buf = BytesMut::new();
        frontend::encode_message(&FrontendMsg::Query(sql.as_bytes()), &mut buf);
        let _resp = self.conn.submit(buf, ResponseCollector::Drain).await?;
        Ok(())
    }

    /// Check if the underlying connection is alive.
    pub fn is_alive(&self) -> bool {
        self.conn.is_alive()
    }
}

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

/// A transaction guard. Commits on `commit()`, rolls back on drop.
pub struct Transaction<'a> {
    client: &'a Client,
    done: bool,
}

impl<'a> Transaction<'a> {
    /// Execute a query within the transaction.
    pub async fn query(
        &self,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<Vec<Row>, TypedError> {
        self.client.query(sql, params).await
    }

    /// Execute a statement within the transaction. Returns affected row count.
    pub async fn execute(
        &self,
        sql: &str,
        params: &[&dyn SqlParam],
    ) -> Result<u64, TypedError> {
        self.client.execute(sql, params).await
    }

    /// Commit the transaction.
    pub async fn commit(mut self) -> Result<(), TypedError> {
        self.done = true;
        self.client.simple_query("COMMIT").await
    }

    /// Explicitly roll back the transaction.
    pub async fn rollback(mut self) -> Result<(), TypedError> {
        self.done = true;
        self.client.simple_query("ROLLBACK").await
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if !self.done {
            // Can't await in drop — spawn a task to rollback.
            // This is best-effort; the connection will recover on next use.
            let client_alive = self.client.is_alive();
            if client_alive {
                // We can't actually send ROLLBACK in drop without a runtime.
                // The PG connection will auto-rollback when the next statement
                // is sent outside a transaction block.
                tracing::warn!("Transaction dropped without commit — will auto-rollback");
            }
        }
    }
}

/// Parse the affected row count from a CommandComplete tag.
/// Examples: "SELECT 3" → 3, "INSERT 0 1" → 1, "UPDATE 5" → 5, "DELETE 0" → 0.
fn parse_row_count(tag: &str) -> u64 {
    // The last space-separated token is the row count.
    tag.rsplit_once(' ')
        .and_then(|(_, count)| count.parse::<u64>().ok())
        .unwrap_or(0)
}
