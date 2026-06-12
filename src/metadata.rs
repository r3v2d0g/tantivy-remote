use std::{io, path::Path};

use derive_more::Debug;
use eyre::{Context as _, Result};
use opendal::Operator;
use sqlx::PgPool;

use crate::{
    context::Context,
    utils::{PathExt, WrapIoErrorExt},
};

/// Takes care of storing and retrieving metadata about indexes.
#[derive(Clone, Debug)]
pub struct MetadataStore {
    /// Pool of connections to interact with PSQL.
    pool: PgPool,

    /// The underlying Opendal operator used to read and write files.
    operator: Operator,

    /// The configuration shared with the directory that owns this store.
    pub(crate) context: Context,
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
        })
    }

    /// Returns `true` if there is a non-metadata file with the given path that exists
    /// in the metadata store.
    pub async fn file_exists(&self, path: &str) -> sqlx::Result<bool> {
        Ok(self.file_lookup(path).await?.is_some())
    }

    /// Looks up a non-metadata file by path, returning whether it is [logically
    /// empty][1] if it exists (and has not been deleted), or `None` otherwise.
    ///
    /// [1]: crate::empty
    pub async fn file_lookup(&self, path: &str) -> sqlx::Result<Option<bool>> {
        let query = sqlx::query_scalar(
            r#"
            SELECT is_empty
            FROM tantivy.files
            WHERE index = $1
              AND path = $2
              AND deleted_at IS NULL
            "#,
        );

        query
            .bind(self.context.index)
            .bind(path)
            .fetch_optional(&self.pool)
            .await
    }

    /// Creates the non-metadata file into the metadata store.
    ///
    /// `is_empty` records whether the file was detected to be [logically empty][1] and
    /// therefore not stored in the object store / inner directory.
    ///
    /// [1]: crate::empty
    pub async fn create_file(&self, path: &str, is_empty: bool) -> sqlx::Result<()> {
        let create = sqlx::query(
            r#"
            INSERT INTO tantivy.files (index, path, is_empty)
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
            "#,
        );

        create
            .bind(self.context.index)
            .bind(path)
            .bind(is_empty)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Marks the given non-metadata file as having been deleted in the metadata store.
    ///
    /// Returns `true` if the file was deleted, `false` if it did not exist or was
    /// already deleted.
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

        Ok(row.is_some())
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
