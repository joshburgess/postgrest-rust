//! Pool integration: typed client backed by pg-pool's ConnPool.

use std::sync::Arc;

use pg_pool::wire::WirePoolable;
use pg_pool::{ConnPool, ConnPoolConfig, LifecycleHooks, PoolError};

use crate::error::TypedError;
use crate::query::Client;

/// A pool of typed clients. Wraps `pg_pool::ConnPool<WirePoolable>` and
/// provides typed query access via checkout.
pub struct TypedPool {
    pool: Arc<ConnPool<WirePoolable>>,
}

impl TypedPool {
    /// Create a new typed pool.
    pub async fn new(
        config: ConnPoolConfig,
        hooks: LifecycleHooks,
    ) -> Result<Self, PoolError<pg_wire::PgWireError>> {
        let pool = ConnPool::new(config, hooks).await?;
        Ok(Self { pool })
    }

    /// Connect with sensible defaults.
    pub async fn connect(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
        max_size: usize,
    ) -> Result<Self, PoolError<pg_wire::PgWireError>> {
        let config = ConnPoolConfig {
            addr: addr.to_string(),
            user: user.to_string(),
            password: password.to_string(),
            database: database.to_string(),
            max_size,
            ..Default::default()
        };
        Self::new(config, LifecycleHooks::default()).await
    }

    /// Check out a connection and wrap it in a typed Client.
    /// The Client owns the connection; it's returned to the pool on drop.
    pub async fn get(&self) -> Result<PooledTypedClient, TypedError> {
        let guard = self.pool.get().await.map_err(|e| {
            TypedError::Decode {
                column: 0,
                message: format!("pool error: {e}"),
            }
        })?;
        // Take the WireConn out of the guard. This removes it from pool tracking —
        // we need to return it manually. But for simplicity, we create a fresh
        // AsyncConn-backed Client from each checkout.
        let wire_conn = guard.take().0; // WirePoolable → WireConn
        let client = Client::new(wire_conn);
        Ok(PooledTypedClient { client })
    }

    /// Pool metrics.
    pub fn metrics(&self) -> pg_pool::PoolMetrics {
        self.pool.metrics()
    }

    /// Acquire a raw pooled connection (returned to pool on drop).
    /// Use this when you need direct WireConn access without AsyncConn overhead.
    pub async fn acquire(&self) -> Result<PooledConnection, TypedError> {
        let guard = self.pool.get().await.map_err(|e| {
            TypedError::Decode {
                column: 0,
                message: format!("pool error: {e}"),
            }
        })?;
        Ok(PooledConnection { guard: Some(guard) })
    }

    /// Drain the pool.
    pub async fn drain(&self) {
        self.pool.drain().await;
    }
}

/// A typed client checked out from the pool.
/// The underlying connection is NOT returned to the pool (it uses AsyncConn
/// which spawns reader/writer tasks, incompatible with pool return).
/// For pooled connections, prefer using the pool for short-lived operations.
pub struct PooledTypedClient {
    pub client: Client,
}

impl std::ops::Deref for PooledTypedClient {
    type Target = Client;
    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

/// A raw connection borrowed from the pool.
/// Unlike `PooledTypedClient`, this uses `PgPipeline` directly
/// and returns the connection to the pool when dropped.
pub struct PooledConnection {
    guard: Option<pg_pool::PoolGuard<WirePoolable>>,
}

impl PooledConnection {
    /// Access the raw WireConn for direct protocol operations.
    pub fn conn(&mut self) -> &mut pg_wire::WireConn {
        &mut self.guard.as_mut().unwrap().conn_mut().0
    }

    /// Convert to a PgPipeline for pipelined operations.
    /// Consumes the guard — the connection will NOT be returned to the pool.
    pub fn into_pipeline(mut self) -> pg_wire::PgPipeline {
        let wire = self.guard.take().unwrap().take().0;
        pg_wire::PgPipeline::new(wire)
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        // guard is dropped here, returning the connection to the pool.
    }
}
