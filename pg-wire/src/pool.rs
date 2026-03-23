use std::sync::Arc;

use tokio::sync::{Mutex, Semaphore};

use crate::connection::WireConn;
use crate::error::PgWireError;
use crate::pipeline::PgPipeline;

/// Simple async connection pool for PgPipeline.
/// Uses a semaphore to limit concurrent connections and a mutex-protected stack.
pub struct Pool {
    connections: Mutex<Vec<PgPipeline>>,
    semaphore: Arc<Semaphore>,
    config: PoolConfig,
}

#[derive(Clone)]
pub struct PoolConfig {
    pub addr: String,
    pub user: String,
    pub password: String,
    pub database: String,
    pub max_size: usize,
}

/// A pooled connection that returns itself to the pool on drop.
pub struct PooledConn {
    conn: Option<PgPipeline>,
    pool: Arc<Pool>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Pool {
    pub fn new(config: PoolConfig) -> Arc<Self> {
        let max_size = config.max_size;
        Arc::new(Self {
            connections: Mutex::new(Vec::with_capacity(max_size)),
            semaphore: Arc::new(Semaphore::new(max_size)),
            config,
        })
    }

    /// Get a connection from the pool, creating one if needed.
    pub async fn get(self: &Arc<Self>) -> Result<PooledConn, PgWireError> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| PgWireError::Protocol("Pool closed".into()))?;

        // Try to reuse an existing connection.
        let conn = {
            let mut conns = self.connections.lock().await;
            conns.pop()
        };

        let pipeline = match conn {
            Some(c) => c,
            None => {
                // Create a new connection.
                let wire = WireConn::connect(
                    &self.config.addr,
                    &self.config.user,
                    &self.config.password,
                    &self.config.database,
                )
                .await?;
                PgPipeline::new(wire)
            }
        };

        Ok(PooledConn {
            conn: Some(pipeline),
            pool: Arc::clone(self),
            _permit: permit,
        })
    }
}

impl PooledConn {
    /// Access the underlying PgPipeline.
    pub fn pipeline(&mut self) -> &mut PgPipeline {
        self.conn.as_mut().unwrap()
    }
}

impl Drop for PooledConn {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let pool = Arc::clone(&self.pool);
            // Return connection to pool asynchronously.
            tokio::spawn(async move {
                let mut conns = pool.connections.lock().await;
                conns.push(conn);
            });
        }
    }
}

impl std::ops::Deref for PooledConn {
    type Target = PgPipeline;
    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledConn {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().unwrap()
    }
}
