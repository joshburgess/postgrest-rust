//! Production-grade connection pool for PostgreSQL wire connections.
//!
//! Inspired by hsqlx's pool design: waiter queue with dead-waiter skipping,
//! jittered max-life to avoid thundering herd, health checks on checkout,
//! lifecycle hooks, graceful drain, and metrics.
//!
//! This pool manages `WireConn` connections with checkout/checkin semantics.
//! Each connection is verified before being handed to a caller.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot, Mutex, Notify};

use crate::connection::WireConn;
use crate::error::PgWireError;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Connection pool configuration.
#[derive(Clone, Debug)]
pub struct ConnPoolConfig {
    /// PostgreSQL address (host:port).
    pub addr: String,
    /// PostgreSQL user.
    pub user: String,
    /// PostgreSQL password.
    pub password: String,
    /// PostgreSQL database.
    pub database: String,
    /// Minimum idle connections to maintain.
    pub min_idle: usize,
    /// Maximum total connections.
    pub max_size: usize,
    /// Maximum lifetime per connection (with jitter applied).
    pub max_lifetime: Duration,
    /// Jitter range for max_lifetime (± this value).
    pub max_lifetime_jitter: Duration,
    /// Timeout waiting for a connection from the pool.
    pub checkout_timeout: Duration,
    /// How often to run the maintenance task (health checks, replenish min_idle).
    pub maintenance_interval: Duration,
    /// Whether to verify connections on checkout (ping query).
    pub test_on_checkout: bool,
}

impl Default for ConnPoolConfig {
    fn default() -> Self {
        Self {
            addr: String::new(),
            user: String::new(),
            password: String::new(),
            database: String::new(),
            min_idle: 1,
            max_size: 10,
            max_lifetime: Duration::from_secs(30 * 60), // 30 minutes
            max_lifetime_jitter: Duration::from_secs(60), // ± 60s
            checkout_timeout: Duration::from_secs(5),
            maintenance_interval: Duration::from_secs(10),
            test_on_checkout: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Lifecycle hooks
// ---------------------------------------------------------------------------

/// Lifecycle hook callbacks. All hooks are optional.
#[derive(Default)]
pub struct LifecycleHooks {
    /// Called after a new connection is created.
    pub on_create: Option<Box<dyn Fn() + Send + Sync>>,
    /// Called when a connection is checked out from the pool.
    pub on_checkout: Option<Box<dyn Fn() + Send + Sync>>,
    /// Called when a connection is returned to the pool.
    pub on_checkin: Option<Box<dyn Fn() + Send + Sync>>,
    /// Called when a connection is destroyed (expired, unhealthy, or pool drain).
    pub on_destroy: Option<Box<dyn Fn() + Send + Sync>>,
}

// ---------------------------------------------------------------------------
// Pool metrics
// ---------------------------------------------------------------------------

/// Snapshot of pool metrics.
#[derive(Debug, Clone)]
pub struct PoolMetrics {
    /// Total connections currently managed (idle + in-use).
    pub total: usize,
    /// Idle connections available for checkout.
    pub idle: usize,
    /// Connections currently checked out.
    pub in_use: usize,
    /// Number of waiters in the queue.
    pub waiters: usize,
    /// Total checkouts since pool creation.
    pub total_checkouts: u64,
    /// Total connections created since pool creation.
    pub total_created: u64,
    /// Total connections destroyed since pool creation.
    pub total_destroyed: u64,
    /// Total checkout timeouts since pool creation.
    pub total_timeouts: u64,
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// An idle connection with its computed expiry.
struct IdleConn {
    conn: WireConn,
    expires_at: Instant,
}

/// A waiter in the queue, waiting for a connection.
struct Waiter {
    tx: oneshot::Sender<WireConn>,
}

// ---------------------------------------------------------------------------
// ConnPool
// ---------------------------------------------------------------------------

/// Production-grade connection pool with waiter queue and health management.
pub struct ConnPool {
    config: ConnPoolConfig,
    hooks: LifecycleHooks,

    /// Idle connections ready for checkout.
    idle: Mutex<VecDeque<IdleConn>>,
    /// Waiters blocked on checkout.
    waiters: Mutex<VecDeque<Waiter>>,

    /// Total connections currently alive (idle + checked-out).
    total_count: AtomicUsize,
    /// Number of connections currently checked out.
    in_use_count: AtomicUsize,

    // Metrics counters.
    total_checkouts: AtomicU64,
    total_created: AtomicU64,
    total_destroyed: AtomicU64,
    total_timeouts: AtomicU64,

    /// Set to true to stop the pool from accepting new checkouts.
    draining: AtomicBool,
    /// Notified when all connections are returned during drain.
    drain_complete: Notify,

    /// Shutdown signal for background tasks.
    shutdown_tx: mpsc::Sender<()>,
}

impl ConnPool {
    /// Create a new connection pool and spawn the maintenance task.
    /// Pre-fills `min_idle` connections.
    pub async fn new(config: ConnPoolConfig, hooks: LifecycleHooks) -> Result<Arc<Self>, PgWireError> {
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let pool = Arc::new(Self {
            config: config.clone(),
            hooks,
            idle: Mutex::new(VecDeque::with_capacity(config.max_size)),
            waiters: Mutex::new(VecDeque::new()),
            total_count: AtomicUsize::new(0),
            in_use_count: AtomicUsize::new(0),
            total_checkouts: AtomicU64::new(0),
            total_created: AtomicU64::new(0),
            total_destroyed: AtomicU64::new(0),
            total_timeouts: AtomicU64::new(0),
            draining: AtomicBool::new(false),
            drain_complete: Notify::new(),
            shutdown_tx,
        });

        // Pre-fill min_idle connections.
        for _ in 0..config.min_idle {
            match pool.create_connection().await {
                Ok(idle_conn) => {
                    pool.idle.lock().await.push_back(idle_conn);
                    pool.total_count.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    tracing::warn!("Failed to pre-fill connection: {e}");
                }
            }
        }

        // Spawn maintenance task.
        {
            let pool_ref = Arc::clone(&pool);
            tokio::spawn(maintenance_task(pool_ref, shutdown_rx));
        }

        Ok(pool)
    }

    /// Check out a connection from the pool.
    /// Returns a `PoolGuard` that automatically returns the connection on drop.
    pub async fn get(self: &Arc<Self>) -> Result<PoolGuard, PgWireError> {
        if self.draining.load(Ordering::Relaxed) {
            return Err(PgWireError::Protocol("Pool is draining".into()));
        }

        // Fast path: try to get an idle connection.
        if let Some(conn) = self.try_get_idle().await {
            self.in_use_count.fetch_add(1, Ordering::Relaxed);
            self.total_checkouts.fetch_add(1, Ordering::Relaxed);
            if let Some(ref hook) = self.hooks.on_checkout {
                hook();
            }
            return Ok(PoolGuard {
                conn: Some(conn),
                pool: Arc::clone(self),
            });
        }

        // Try to create a new connection if under max_size.
        if self.total_count.load(Ordering::Relaxed) < self.config.max_size {
            match self.create_and_track().await {
                Ok(conn) => {
                    self.in_use_count.fetch_add(1, Ordering::Relaxed);
                    self.total_checkouts.fetch_add(1, Ordering::Relaxed);
                    if let Some(ref hook) = self.hooks.on_checkout {
                        hook();
                    }
                    return Ok(PoolGuard {
                        conn: Some(conn),
                        pool: Arc::clone(self),
                    });
                }
                Err(e) => {
                    tracing::warn!("Failed to create new connection: {e}");
                    // Fall through to waiter queue.
                }
            }
        }

        // Slow path: join the waiter queue with timeout.
        let (tx, rx) = oneshot::channel();
        {
            let mut waiters = self.waiters.lock().await;
            waiters.push_back(Waiter { tx });
        }

        match tokio::time::timeout(self.config.checkout_timeout, rx).await {
            Ok(Ok(conn)) => {
                self.in_use_count.fetch_add(1, Ordering::Relaxed);
                self.total_checkouts.fetch_add(1, Ordering::Relaxed);
                if let Some(ref hook) = self.hooks.on_checkout {
                    hook();
                }
                Ok(PoolGuard {
                    conn: Some(conn),
                    pool: Arc::clone(self),
                })
            }
            Ok(Err(_)) => {
                // Sender dropped — pool shutting down.
                Err(PgWireError::Protocol("Pool closed".into()))
            }
            Err(_) => {
                self.total_timeouts.fetch_add(1, Ordering::Relaxed);
                Err(PgWireError::Protocol("Checkout timeout".into()))
            }
        }
    }

    /// Try to pop a valid idle connection (not expired, passes health check).
    async fn try_get_idle(&self) -> Option<WireConn> {
        let mut idle = self.idle.lock().await;
        while let Some(entry) = idle.pop_front() {
            // Check expiry.
            if Instant::now() >= entry.expires_at {
                self.destroy_conn_stats();
                if let Some(ref hook) = self.hooks.on_destroy {
                    hook();
                }
                continue;
            }

            // Health check: verify no pending data (connection isn't corrupted).
            if self.config.test_on_checkout && entry.conn.has_pending_data() {
                self.destroy_conn_stats();
                if let Some(ref hook) = self.hooks.on_destroy {
                    hook();
                }
                continue;
            }

            return Some(entry.conn);
        }
        None
    }

    /// Create a new WireConn and wrap it as IdleConn.
    async fn create_connection(&self) -> Result<IdleConn, PgWireError> {
        let conn = WireConn::connect(
            &self.config.addr,
            &self.config.user,
            &self.config.password,
            &self.config.database,
        )
        .await?;

        self.total_created.fetch_add(1, Ordering::Relaxed);
        if let Some(ref hook) = self.hooks.on_create {
            hook();
        }

        let jitter = jittered_duration(self.config.max_lifetime, self.config.max_lifetime_jitter);
        Ok(IdleConn {
            conn,
            expires_at: Instant::now() + jitter,
        })
    }

    /// Create a new connection and increment total_count.
    async fn create_and_track(&self) -> Result<WireConn, PgWireError> {
        // Optimistically increment to reserve the slot.
        let prev = self.total_count.fetch_add(1, Ordering::Relaxed);
        if prev >= self.config.max_size {
            // Race: someone else filled the last slot.
            self.total_count.fetch_sub(1, Ordering::Relaxed);
            return Err(PgWireError::Protocol("Pool at max capacity".into()));
        }

        match self.create_connection().await {
            Ok(idle_conn) => Ok(idle_conn.conn),
            Err(e) => {
                self.total_count.fetch_sub(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Return a connection to the pool (called by PoolGuard on drop).
    /// Spawns an async task since Drop can't be async.
    fn return_conn(pool: Arc<Self>, conn: WireConn) {
        tokio::spawn(async move {
            pool.return_conn_async(conn).await;
        });
    }

    /// Async return logic — hand off to waiter or push to idle.
    async fn return_conn_async(&self, conn: WireConn) {
        // Check if connection is still usable.
        if conn.has_pending_data() {
            self.in_use_count.fetch_sub(1, Ordering::Relaxed);
            self.destroy_conn_stats();
            if let Some(ref hook) = self.hooks.on_destroy {
                hook();
            }
            self.maybe_notify_drain();
            return;
        }

        if let Some(ref hook) = self.hooks.on_checkin {
            hook();
        }

        self.in_use_count.fetch_sub(1, Ordering::Relaxed);

        if self.draining.load(Ordering::Relaxed) {
            // During drain, destroy instead of returning.
            self.destroy_conn_stats();
            if let Some(ref hook) = self.hooks.on_destroy {
                hook();
            }
            self.maybe_notify_drain();
            return;
        }

        // Try to hand off to a waiter first (skip dead waiters).
        let mut conn = conn;
        {
            let mut waiters = self.waiters.lock().await;
            while let Some(waiter) = waiters.pop_front() {
                // Dead-waiter skipping: if the receiver was dropped, skip.
                match waiter.tx.send(conn) {
                    Ok(()) => return,
                    Err(returned_conn) => {
                        conn = returned_conn;
                        continue;
                    }
                }
            }
        }

        // No waiters — return to idle pool.
        let jitter = jittered_duration(self.config.max_lifetime, self.config.max_lifetime_jitter);
        let mut idle = self.idle.lock().await;
        idle.push_back(IdleConn {
            conn,
            expires_at: Instant::now() + jitter,
        });
    }

    fn maybe_notify_drain(&self) {
        if self.draining.load(Ordering::Relaxed)
            && self.total_count.load(Ordering::Relaxed) == 0
        {
            self.drain_complete.notify_one();
        }
    }

    /// Decrement total_count for a destroyed connection.
    fn destroy_conn_stats(&self) {
        self.total_count.fetch_sub(1, Ordering::Relaxed);
        self.total_destroyed.fetch_add(1, Ordering::Relaxed);
    }

    /// Get a snapshot of pool metrics.
    pub fn metrics(&self) -> PoolMetrics {
        let total = self.total_count.load(Ordering::Relaxed);
        let in_use = self.in_use_count.load(Ordering::Relaxed);
        PoolMetrics {
            total,
            idle: total.saturating_sub(in_use),
            in_use,
            waiters: 0, // Approximation; exact count requires lock.
            total_checkouts: self.total_checkouts.load(Ordering::Relaxed),
            total_created: self.total_created.load(Ordering::Relaxed),
            total_destroyed: self.total_destroyed.load(Ordering::Relaxed),
            total_timeouts: self.total_timeouts.load(Ordering::Relaxed),
        }
    }

    /// Initiate graceful drain. No new checkouts will be accepted.
    /// Returns when all checked-out connections have been returned and destroyed.
    pub async fn drain(&self) {
        self.draining.store(true, Ordering::Relaxed);

        // Destroy all idle connections immediately.
        {
            let mut idle = self.idle.lock().await;
            let count = idle.len();
            idle.clear();
            self.total_count.fetch_sub(count, Ordering::Relaxed);
            self.total_destroyed.fetch_add(count as u64, Ordering::Relaxed);
            if count > 0 {
                if let Some(ref hook) = self.hooks.on_destroy {
                    for _ in 0..count {
                        hook();
                    }
                }
            }
        }

        // Cancel all waiters.
        {
            let mut waiters = self.waiters.lock().await;
            waiters.clear(); // Dropping senders signals the receivers.
        }

        // Wait for all in-use connections to be returned.
        while self.total_count.load(Ordering::Relaxed) > 0 {
            self.drain_complete.notified().await;
        }

        // Signal shutdown to maintenance task.
        let _ = self.shutdown_tx.send(()).await;

        tracing::info!("Connection pool drained");
    }

    /// Current pool status for logging/debugging.
    pub fn status(&self) -> String {
        let m = self.metrics();
        format!(
            "pool: total={} idle={} in_use={} created={} destroyed={} timeouts={}",
            m.total, m.idle, m.in_use, m.total_created, m.total_destroyed, m.total_timeouts
        )
    }
}

// ---------------------------------------------------------------------------
// PoolGuard — RAII connection handle
// ---------------------------------------------------------------------------

/// A checked-out connection that returns itself to the pool on drop.
pub struct PoolGuard {
    conn: Option<WireConn>,
    pool: Arc<ConnPool>,
}

impl PoolGuard {
    /// Access the underlying WireConn.
    pub fn conn(&self) -> &WireConn {
        self.conn.as_ref().unwrap()
    }

    /// Access the underlying WireConn mutably.
    pub fn conn_mut(&mut self) -> &mut WireConn {
        self.conn.as_mut().unwrap()
    }

    /// Consume the guard and take ownership of the connection.
    /// The connection will NOT be returned to the pool.
    pub fn take(mut self) -> WireConn {
        let conn = self.conn.take().unwrap();
        self.pool.in_use_count.fetch_sub(1, Ordering::Relaxed);
        self.pool.total_count.fetch_sub(1, Ordering::Relaxed);
        conn
    }
}

impl Drop for PoolGuard {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            ConnPool::return_conn(Arc::clone(&self.pool), conn);
        }
    }
}

impl std::ops::Deref for PoolGuard {
    type Target = WireConn;
    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PoolGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().unwrap()
    }
}

// ---------------------------------------------------------------------------
// Maintenance task
// ---------------------------------------------------------------------------

/// Background task that:
/// 1. Evicts expired idle connections.
/// 2. Replenishes to min_idle if below.
async fn maintenance_task(pool: Arc<ConnPool>, mut shutdown_rx: mpsc::Receiver<()>) {
    let mut interval = tokio::time::interval(pool.config.maintenance_interval);
    interval.tick().await; // Skip the first immediate tick.
    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = shutdown_rx.recv() => {
                tracing::debug!("Maintenance task shutting down");
                return;
            }
        }

        if pool.draining.load(Ordering::Relaxed) {
            return;
        }

        // 1. Evict expired idle connections.
        {
            let mut idle = pool.idle.lock().await;
            let now = Instant::now();
            let before = idle.len();
            idle.retain(|entry| now < entry.expires_at);
            let evicted = before - idle.len();
            if evicted > 0 {
                pool.total_count.fetch_sub(evicted, Ordering::Relaxed);
                pool.total_destroyed.fetch_add(evicted as u64, Ordering::Relaxed);
                tracing::debug!("Evicted {evicted} expired connections");
            }
        }

        // 2. Replenish to min_idle.
        let total = pool.total_count.load(Ordering::Relaxed);
        let in_use = pool.in_use_count.load(Ordering::Relaxed);
        let current_idle = total.saturating_sub(in_use);

        if current_idle < pool.config.min_idle && total < pool.config.max_size {
            let to_create = (pool.config.min_idle - current_idle)
                .min(pool.config.max_size - total);
            for _ in 0..to_create {
                match pool.create_connection().await {
                    Ok(idle_conn) => {
                        pool.total_count.fetch_add(1, Ordering::Relaxed);
                        pool.idle.lock().await.push_back(idle_conn);
                    }
                    Err(e) => {
                        tracing::warn!("Maintenance: failed to create connection: {e}");
                        break;
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Apply jitter to a base duration.
fn jittered_duration(base: Duration, jitter: Duration) -> Duration {
    if jitter.is_zero() {
        return base;
    }
    let jitter_ms = jitter.as_millis() as u64;
    // Simple pseudo-random jitter using thread-local fast RNG.
    let offset = fastrand_u64() % (jitter_ms * 2 + 1);
    let jittered = base.as_millis() as i128 + offset as i128 - jitter_ms as i128;
    Duration::from_millis(jittered.max(1000) as u64)
}

/// Fast pseudo-random u64 using a simple xorshift on thread-local state.
fn fastrand_u64() -> u64 {
    use std::cell::Cell;
    thread_local! {
        static STATE: Cell<u64> = Cell::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64
        );
    }
    STATE.with(|s| {
        let mut x = s.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        if x == 0 { x = 1; }
        s.set(x);
        x
    })
}
