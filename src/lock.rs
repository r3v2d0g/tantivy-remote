use std::{
    io,
    path::Path,
    sync::{Arc, RwLock},
    time::Duration,
};

use block_on_place::HandleExt;
use sqlx::{PgPool, Postgres, pool::PoolConnection};
use tantivy::directory::{DirectoryLock, INDEX_WRITER_LOCK, Lock, error::LockError};
use tokio::{
    runtime::Handle,
    select,
    sync::oneshot,
    task::JoinHandle,
    time::{self, MissedTickBehavior},
};
use uuid::Uuid;

/// How often an idle advisory-lock connection is kept alive.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// Seed used when hashing advisory-lock keys with [`gxhash`].
///
/// Fixed so that every process connected to the same database derives the same
/// key for a given `(index, lock path)`.
const LOCK_KEY_SEED: i64 = 0x7461_6e74_6976_792d; // "tantivy-"

/// Shared writer-fence state between [`AdvisoryLocks`] and [`MetadataStore`][1].
///
/// [1]: crate::metadata::MetadataStore
#[derive(Clone, Debug, Default)]
pub(crate) struct WriterFence {
    state: Arc<RwLock<FenceState>>,
}

/// Whether this directory instance currently holds, or has lost, the writer fence.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum FenceState {
    /// No writer lock is held by this instance.
    #[default]
    Open,

    /// This instance holds the writer lock with the given token.
    Held(Uuid),

    /// The advisory-lock session died; mutations from this instance must fail.
    Lost,
}

impl WriterFence {
    /// Returns the current fence state.
    pub fn state(&self) -> FenceState {
        *self.state.read().unwrap_or_else(|error| error.into_inner())
    }

    fn set(&self, state: FenceState) {
        *self
            .state
            .write()
            .unwrap_or_else(|error| error.into_inner()) = state;
    }
}

/// Acquires PostgreSQL session-level advisory locks for a directory.
#[derive(Clone, Debug)]
pub(crate) struct AdvisoryLocks {
    index: Uuid,
    pool: PgPool,
    rt: Handle,
    fence: WriterFence,
}

impl AdvisoryLocks {
    pub fn new(index: Uuid, pool: PgPool, rt: Handle, fence: WriterFence) -> Self {
        Self {
            index,
            pool,
            rt,
            fence,
        }
    }

    /// Acquires `lock` as a PostgreSQL session-level advisory lock.
    ///
    /// Checks out a dedicated pool connection for the lifetime of the returned
    /// [`DirectoryLock`]. Non-blocking locks use `pg_try_advisory_lock` and return
    /// [`LockError::LockBusy`] when already held; blocking locks use
    /// `pg_advisory_lock` and wait until the lock is free.
    ///
    /// Acquiring the index-writer lock also publishes a fencing token on
    /// `tantivy.directories.writer_token` and into the shared [`WriterFence`].
    pub fn acquire(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        let key = self.key(&lock.filepath);
        let writer = lock.filepath == INDEX_WRITER_LOCK.filepath;
        let mut connection = self
            .rt
            .block_on_place(self.pool.acquire())
            .map_err(|error| {
                let error = Arc::new(io::Error::other(error));
                LockError::IoError(error)
            })?;

        let acquired = if lock.is_blocking {
            let query = sqlx::query("SELECT pg_advisory_lock($1)")
                .bind(key)
                .execute(&mut *connection);

            self.rt
                .block_on_place(query)
                .map(|_| true)
                .map_err(|error| {
                    let error = Arc::new(io::Error::other(error));
                    LockError::IoError(error)
                })?
        } else {
            let query = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                .bind(key)
                .fetch_one(&mut *connection);

            self.rt.block_on_place(query).map_err(|error| {
                let error = Arc::new(io::Error::other(error));
                LockError::IoError(error)
            })?
        };

        if !acquired {
            return Err(LockError::LockBusy);
        }

        let token = if writer {
            let token = Uuid::new_v4();
            let query = sqlx::query(
                r#"
                UPDATE tantivy.directories
                SET writer_token = $1
                WHERE index = $2
                "#,
            );

            let query = query.bind(token).bind(self.index).execute(&mut *connection);
            self.rt.block_on_place(query).map_err(|error| {
                let error = Arc::new(io::Error::other(error));
                LockError::IoError(error)
            })?;

            self.fence.set(FenceState::Held(token));

            Some(token)
        } else {
            None
        };

        let (shutdown, shutdown_rx) = oneshot::channel();
        let task = self.rt.spawn(AdvisoryLockGuard::hold(
            connection,
            key,
            shutdown_rx,
            self.index,
            self.pool.clone(),
            self.fence.clone(),
            token,
        ));

        let guard = AdvisoryLockGuard {
            rt: self.rt.clone(),
            shutdown: Some(shutdown),
            task: Some(task),
        };

        Ok(DirectoryLock::from(Box::new(guard)))
    }

    /// [`gxhash`] of `(index UUID, lock path)` with a fixed seed.
    ///
    /// Advisory-lock keys are database-scoped. A 64-bit key keeps accidental collisions
    /// negligible while avoiding a schema migration or lock table.
    fn key(&self, path: &Path) -> i64 {
        let mut bytes = Vec::with_capacity(16 + path.as_os_str().len());
        bytes.extend_from_slice(self.index.as_bytes());
        bytes.extend_from_slice(path.as_os_str().as_encoded_bytes());

        i64::from_ne_bytes(gxhash::gxhash64(&bytes, LOCK_KEY_SEED).to_ne_bytes())
    }
}

/// Owns the PostgreSQL session, and therefore the advisory lock, until dropped.
struct AdvisoryLockGuard {
    rt: Handle,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl AdvisoryLockGuard {
    /// Holds `connection`'s advisory lock on `key` until `shutdown` fires.
    ///
    /// While waiting, periodically runs `SELECT 1` so an otherwise idle session is
    /// not closed by PostgreSQL or a network proxy (which would release the lock).
    /// On shutdown, unlocks explicitly and returns the connection to the pool when
    /// that succeeds; otherwise closes the connection so an uncertain lock state is
    /// never reused.
    ///
    /// If `token` is set (writer lock) and the keepalive fails, the shared
    /// [`WriterFence`] is poisoned to [`FenceState::Lost`] and the PostgreSQL
    /// token is cleared best-effort so mutating metadata calls from this instance
    /// cannot succeed.
    async fn hold(
        mut connection: PoolConnection<Postgres>,
        key: i64,
        mut shutdown: oneshot::Receiver<()>,
        index: Uuid,
        pool: PgPool,
        fence: WriterFence,
        token: Option<Uuid>,
    ) {
        let mut keepalive = time::interval(KEEPALIVE_INTERVAL);
        keepalive.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // `interval` ticks immediately; the lock acquisition itself just used the session.
        keepalive.tick().await;

        loop {
            select! {
                _ = &mut shutdown => {
                    if let Some(token) = token {
                        let query = sqlx::query(
                            r#"
                            UPDATE tantivy.directories
                            SET writer_token = NULL
                            WHERE index = $1
                              AND writer_token = $2
                            "#,
                        );

                        let query = query
                            .bind(index)
                            .bind(token)
                            .execute(&mut *connection);
                        let _ = query.await;
                        // Keep `Lost` if the keepalive already poisoned the fence.
                        if matches!(fence.state(), FenceState::Held(_)) {
                            fence.set(FenceState::Open);
                        }
                    }

                    let query = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
                        .bind(key)
                        .fetch_one(&mut *connection);

                    let unlocked = query.await;
                    if matches!(unlocked, Ok(true)) {
                        // Returning an explicitly unlocked connection to the pool is safe.
                        drop(connection);
                    } else {
                        // Never return a session whose lock state is uncertain to the pool.
                        let _ = connection.close().await;
                    }

                    return;
                }

                _ = keepalive.tick() => {
                    let query = sqlx::query("SELECT 1").execute(&mut *connection);
                    if query.await.is_ok() {
                        continue;
                    }

                    // A broken session has already lost its advisory lock. Close it
                    // rather than returning uncertain session state to the pool.
                    let _ = connection.close().await;

                    if let Some(token) = token {
                        // Stop local mutations immediately, even if clearing PG fails.
                        fence.set(FenceState::Lost);

                        if let Ok(mut conn) = pool.acquire().await {
                            let query = sqlx::query(
                                r#"
                                UPDATE tantivy.directories
                                SET writer_token = NULL
                                WHERE index = $1
                                  AND writer_token = $2
                                "#,
                            );

                            let query = query
                                .bind(index)
                                .bind(token)
                                .execute(&mut *conn);
                            let _ = query.await;
                        }
                    }

                    return;
                }
            }
        }
    }
}

impl Drop for AdvisoryLockGuard {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }

        if let Some(task) = self.task.take() {
            // Wait until the task has explicitly unlocked (or closed) the connection.
            // This prevents a newly-created writer from seeing a transient `LockBusy`.
            let _ = self.rt.block_on_place(task);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_keys_are_stable_and_namespaced() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let _guard = runtime.enter();

        let locks = AdvisoryLocks::new(
            Uuid::from_u128(1),
            PgPool::connect_lazy("postgresql://unused").expect("lazy pool"),
            Handle::current(),
            WriterFence::default(),
        );

        let writer = locks.key(Path::new(".tantivy-writer.lock"));
        let meta = locks.key(Path::new(".tantivy-meta.lock"));

        assert_eq!(writer, locks.key(Path::new(".tantivy-writer.lock")));
        assert_ne!(writer, meta);

        let other = AdvisoryLocks::new(
            Uuid::from_u128(2),
            PgPool::connect_lazy("postgresql://unused").expect("lazy pool"),
            Handle::current(),
            WriterFence::default(),
        );

        assert_ne!(writer, other.key(Path::new(".tantivy-writer.lock")));
    }
}
