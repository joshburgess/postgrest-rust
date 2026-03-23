//! Pool of AsyncConns for spreading load across multiple PostgreSQL backends.
//!
//! Each AsyncConn maintains its own TCP connection, writer task, and reader task.
//! The pool dispatches requests round-robin across connections using an atomic counter.
//! No locks on the hot path.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::async_conn::AsyncConn;
use crate::connection::WireConn;
use crate::error::PgWireError;

/// A pool of N AsyncConns for parallel PostgreSQL backend utilization.
pub struct AsyncPool {
    conns: Vec<Arc<AsyncConn>>,
    counter: AtomicUsize,
}

impl AsyncPool {
    /// Create a pool of `size` AsyncConns, each with its own TCP connection.
    pub async fn connect(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
        size: usize,
    ) -> Result<Arc<Self>, PgWireError> {
        let mut conns = Vec::with_capacity(size);
        for _ in 0..size {
            let wire = WireConn::connect(addr, user, password, database).await?;
            conns.push(Arc::new(AsyncConn::new(wire)));
        }
        Ok(Arc::new(Self {
            conns,
            counter: AtomicUsize::new(0),
        }))
    }

    /// Get the next AsyncConn via round-robin (lock-free).
    #[inline]
    pub fn get(&self) -> &Arc<AsyncConn> {
        let idx = self.counter.fetch_add(1, Ordering::Relaxed) % self.conns.len();
        &self.conns[idx]
    }

    /// Number of connections in the pool.
    pub fn size(&self) -> usize {
        self.conns.len()
    }

    /// Execute a pipelined transaction on the next available connection.
    pub async fn exec_transaction(
        &self,
        setup_sql: &str,
        query_sql: &str,
        params: &[Option<&[u8]>],
        param_oids: &[u32],
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, PgWireError> {
        self.get()
            .exec_transaction(setup_sql, query_sql, params, param_oids)
            .await
    }

    /// Execute a parameterized query on the next available connection.
    pub async fn exec_query(
        &self,
        sql: &str,
        params: &[Option<&[u8]>],
        param_oids: &[u32],
    ) -> Result<Vec<Vec<Option<Vec<u8>>>>, PgWireError> {
        self.get().exec_query(sql, params, param_oids).await
    }
}
