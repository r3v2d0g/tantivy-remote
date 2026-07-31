use std::{
    io,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use derive_more::Debug;
use eyre::{Context as _, Result};
use opendal::Operator;
use sqlx::PgPool;

use crate::{
    cache::FileLookupCache,
    context::Context,
    utils::{PathExt, WrapIoErrorExt},
};

/// Takes care of storing and retrieving metadata about indexes.
///
/// Successful [`file_lookup`][1] results are cached in-process (see
/// [`FileLookupCache`][3]). Call [`prefetch_files`][2] once before opening many
/// segment components to collapse `O(N)` PostgreSQL round-trips into one bulk query.
/// Missing paths are **not** cached: a concurrent writer can still create them, and
/// a subsequent [`file_lookup`][1] will hit PostgreSQL. After another process commits
/// new files, call [`prefetch_files`][2] again (or rely on per-path cache fills) on
/// this store instance.
///
/// [1]: Self::file_lookup
/// [2]: Self::prefetch_files
/// [3]: crate::cache::FileLookupCache
#[derive(Clone, Debug)]
pub struct MetadataStore {
    /// Pool of connections to interact with PSQL.
    pool: PgPool,

    /// The underlying Opendal operator used to read and write files.
    operator: Operator,

    /// The configuration shared with the directory that owns this store.
    pub(crate) context: Context,

    /// Caches successful [`FileRecord`][1] lookups for this index.
    ///
    /// [1]: FileRecord
    files: FileLookupCache,

    /// Number of PostgreSQL round-trips performed by [`file_lookup`][1]
    /// (cache hits do not increment this).
    ///
    /// [1]: Self::file_lookup
    lookup_queries: Arc<AtomicU64>,
}

/// What the metadata store knows about a non-metadata file: whether it is
/// [logically empty][1] and, if it was [bundled][2], where its bytes live inside
/// the bundle object.
///
/// The file is bundled iff [`byte_length`][3] is `Some`.
///
/// [1]: crate::empty
/// [2]: crate::bundle
/// [3]: Self::byte_length
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileRecord {
    /// Whether the file is logically empty (reconstructed from memory on read).
    pub is_empty: bool,

    /// The file's byte offset within its bundle object (`0` when not bundled).
    pub byte_offset: i64,

    /// The file's byte length within its bundle object.
    ///
    /// `None` if the file is not bundled (its bytes are the whole object at its path).
    pub byte_length: Option<i64>,
}

impl MetadataStore {
    /// Creates a new metadata store for the index described by `context`.
    ///
    /// The `context` is cloned and kept for the lifetime of the store.
    ///
    /// If the index does not exists, it creates it.
    pub(crate) async fn open(context: &Context, pool: PgPool, operator: Operator) -> Result<Self> {
        let create = sqlx::query(
            r#"
            INSERT INTO tantivy.directories (index)
            VALUES ($1)
            ON CONFLICT DO NOTHING
            "#,
        );

        create
            .bind(context.index)
            .execute(&pool)
            .await
            .wrap_err("failed to create index")?;

        Ok(Self {
            pool,
            operator,
            context: context.clone(),
            files: FileLookupCache::default(),
            lookup_queries: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Returns how many PostgreSQL queries [`file_lookup`][1] has issued (cache hits do
    /// not count).
    ///
    /// Useful for metrics and for verifying that [`prefetch_files`][2] eliminated
    /// per-path lookups.
    ///
    /// [1]: Self::file_lookup
    /// [2]: Self::prefetch_files
    pub fn file_lookup_query_count(&self) -> u64 {
        self.lookup_queries.load(Ordering::Relaxed)
    }

    /// Returns `true` if there is a non-metadata file with the given path that exists
    /// in the metadata store.
    pub async fn file_exists(&self, path: &str) -> sqlx::Result<bool> {
        Ok(self.file_lookup(path).await?.is_some())
    }

    /// Looks up a non-metadata file by path, returning its [`FileRecord`] if it exists
    /// (and has not been deleted).
    ///
    /// Consults the in-process cache first. On a miss, issues a single-row PostgreSQL
    /// `SELECT` and, on a hit, inserts the record into the cache. Misses (`None`) are
    /// not cached.
    pub async fn file_lookup(&self, path: &str) -> sqlx::Result<Option<FileRecord>> {
        if let Some(record) = self.files.get(path) {
            return Ok(Some(record));
        }

        self.lookup_queries.fetch_add(1, Ordering::Relaxed);

        let query = sqlx::query_as(
            r#"
            SELECT is_empty, byte_offset, byte_length
            FROM tantivy.files
            WHERE index = $1
              AND path = $2
              AND deleted_at IS NULL
            "#,
        );

        let Some((is_empty, byte_offset, byte_length)) = query
            .bind(self.context.index)
            .bind(path)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };

        let record = FileRecord {
            is_empty,
            byte_offset,
            byte_length,
        };
        self.files.insert(path.to_owned(), record.clone());

        Ok(Some(record))
    }

    /// Loads every non-deleted file record for this index in one query.
    pub async fn list_files(&self) -> sqlx::Result<Vec<(String, FileRecord)>> {
        let query = sqlx::query_as(
            r#"
            SELECT path, is_empty, byte_offset, byte_length
            FROM tantivy.files
            WHERE index = $1
              AND deleted_at IS NULL
            "#,
        );

        let rows: Vec<(String, bool, i64, Option<i64>)> =
            query.bind(self.context.index).fetch_all(&self.pool).await?;

        Ok(rows
            .into_iter()
            .map(|(path, is_empty, byte_offset, byte_length)| {
                let record = FileRecord {
                    is_empty,
                    byte_offset,
                    byte_length,
                };

                (path, record)
            })
            .collect())
    }

    /// Loads all non-deleted file records for this index into the local cache in one
    /// query.
    ///
    /// Returns the number of rows loaded. Safe to call multiple times: each call
    /// **replaces** the cache contents with a fresh snapshot from PostgreSQL.
    ///
    /// Memory bound: `O(number of file rows)` for this index (~path + a few integers
    /// per segment component). At tens of thousands of segments this stays small.
    ///
    /// After another process commits new or deleted files, call this again before
    /// relying on the cache for a cold open/reload of those paths.
    pub async fn prefetch_files(&self) -> sqlx::Result<usize> {
        let files = self.list_files().await?;
        let count = files.len();
        self.files.replace_all(files);
        Ok(count)
    }

    /// Creates a non-metadata file in the metadata store.
    ///
    /// `is_empty` records whether the file was detected to be [logically empty][1] and
    /// therefore not stored in the object store / inner directory.
    ///
    /// `bundle` records where the file's bytes live: `None` means the file is its own
    /// object at its path, `Some((offset, length))` means its bytes live inside its
    /// segment's [bundle][2] object at `[offset, offset + length)`.
    ///
    /// On success the local lookup cache is updated for `path`.
    ///
    /// [1]: crate::empty
    /// [2]: crate::bundle
    pub async fn create_file(
        &self,
        path: &str,
        is_empty: bool,
        bundle: Option<(u64, u64)>,
    ) -> sqlx::Result<()> {
        let (byte_offset, byte_length) = match bundle {
            Some((offset, length)) => (offset as i64, Some(length as i64)),
            None => (0, None),
        };

        let create = sqlx::query(
            r#"
            INSERT INTO tantivy.files (index, path, is_empty, byte_offset, byte_length)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT DO NOTHING
            "#,
        );

        create
            .bind(self.context.index)
            .bind(path)
            .bind(is_empty)
            .bind(byte_offset)
            .bind(byte_length)
            .execute(&self.pool)
            .await?;

        let record = FileRecord {
            is_empty,
            byte_offset,
            byte_length,
        };

        self.files.insert(path.to_owned(), record);

        Ok(())
    }

    /// Marks the given non-metadata file as having been deleted in the metadata store.
    ///
    /// Returns `true` if the file was deleted, `false` if it did not exist or was
    /// already deleted.
    ///
    /// On a successful delete the path is removed from the local lookup cache.
    pub async fn delete_file(&self, path: &str) -> sqlx::Result<bool> {
        let update = sqlx::query_scalar(
            r#"
            UPDATE tantivy.files
            SET deleted_at = NOW()
            WHERE index = $1
              AND path = $2
              AND deleted_at IS NULL
            RETURNING 1
            "#,
        );

        let row: Option<i32> = update
            .bind(self.context.index)
            .bind(path)
            .fetch_optional(&self.pool)
            .await?;

        let deleted = row.is_some();
        if deleted {
            self.files.remove(path);
        }

        Ok(deleted)
    }

    /// Returns `true` if there is a metadata file with the given path stored in the
    /// metadata store.
    pub async fn metadata_exists(&self, path: &str) -> sqlx::Result<bool> {
        let query = sqlx::query_scalar(
            r#"
            SELECT 1
            FROM tantivy.metadata
            WHERE index = $1
              AND path = $2
            "#,
        );

        let row: Option<i32> = query
            .bind(self.context.index)
            .bind(path)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.is_some())
    }

    /// Reads the metadata file stored in the metadata store at the given path.
    ///
    /// This might read from the object store if the file is stored remotely.
    ///
    /// Returns `None` if the file does not exist.
    pub async fn read_metadata(&self, filepath: &Path) -> io::Result<Option<Vec<u8>>> {
        let query = sqlx::query_scalar(
            r#"
            SELECT content
            FROM tantivy.metadata
            WHERE index = $1
              AND path = $2
            "#,
        );

        let path = filepath.try_to_str::<io::Error>()?;
        let Some(content) = query
            .bind(self.context.index)
            .bind(path)
            .fetch_optional(&self.pool)
            .await
            .map_err(io::Error::wrapper(filepath))?
        else {
            return Ok(None);
        };

        // If the content was stored in PostgreSQL, return it as-is.
        if let Some(content) = content {
            return Ok(Some(content));
        }

        let path = self.context.path(filepath);
        let path = path.try_to_str::<io::Error>()?;

        let mut reader = self.operator.reader_with(path);
        if let Some(chunks) = self.context.read_chunks {
            reader = reader.chunk(chunks);
        }

        if let Some(concurrency) = self.context.read_concurrency {
            reader = reader.concurrent(concurrency);
        }

        let reader = reader.await?;
        let buffer = reader.read(..).await?;

        // TODO(MLB): avoid copying
        Ok(Some(buffer.to_vec()))
    }

    /// Writes the given content to the metadata store at the given path.
    ///
    /// If `content` contains more than `context.threshold` bytes, it is written to the
    /// object store.
    pub async fn write_metadata(&self, filepath: &Path, content: &[u8]) -> io::Result<()> {
        // Below `threshold`, we write to PostgreSQL.
        if content.len() < self.context.threshold {
            let query = sqlx::query(
                r#"
                INSERT INTO tantivy.metadata
                  (index, path, content)
                VALUES ($1, $2, $3)
                ON CONFLICT (index, path)
                DO UPDATE SET content = EXCLUDED.content
                "#,
            );

            let path = filepath.try_to_str::<io::Error>()?;
            query
                .bind(self.context.index)
                .bind(path)
                .bind(content)
                .execute(&self.pool)
                .await
                .map_err(io::Error::wrapper(filepath))?;

            return Ok(());
        }

        let path = self.context.path(filepath);
        let path = path.try_to_str::<io::Error>()?;

        let mut writer = self.operator.writer_with(path);
        if let Some(chunks) = self.context.write_chunks {
            writer = writer.chunk(chunks);
        }

        if let Some(concurrency) = self.context.write_concurrency {
            writer = writer.concurrent(concurrency);
        }

        let mut writer = writer.await?;
        writer.write_from(content).await?;
        writer.close().await?;

        // Record a marker row with `NULL` content so that `read_metadata` knows the
        // bytes live in the object store. This also clears any inline content left over
        // from a previous below-threshold write.
        let query = sqlx::query(
            r#"
            INSERT INTO tantivy.metadata
              (index, path, content)
            VALUES ($1, $2, NULL)
            ON CONFLICT (index, path)
            DO UPDATE SET content = NULL
            "#,
        );

        let path = filepath.try_to_str::<io::Error>()?;
        query
            .bind(self.context.index)
            .bind(path)
            .execute(&self.pool)
            .await
            .map_err(io::Error::wrapper(filepath))?;

        Ok(())
    }
}
