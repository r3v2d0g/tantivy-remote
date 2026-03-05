use derive_more::Debug;
use eyre::{Context, Result};
use sqlx::PgPool;
use uuid::Uuid;

/// Takes care of storing and retrieving metadata about indexes.
#[derive(Clone, Debug)]
pub struct MetadataStore {
    /// The ID of the index this is storing the metadata of.
    index: Uuid,

    /// Pool of connections to interact with PSQL.
    pool: PgPool,
}

impl MetadataStore {
    /// Creates a new metadata store for the given index.
    ///
    /// If the index does not exists, it creates it.
    pub(crate) async fn open(index: Uuid, pool: PgPool) -> Result<Self> {
        let create = sqlx::query(
            r#"
            INSERT INTO tantivy.directories (index)
            VALUES ($1)
            ON CONFLICT DO NOTHING
            "#,
        );

        create
            .bind(index)
            .execute(&pool)
            .await
            .wrap_err("failed to create index")?;

        Ok(Self { index, pool })
    }

    /// Returns `true` if there is a non-metadata file with the given path that exists
    /// in the metadata store.
    pub async fn file_exists(&self, path: &str) -> sqlx::Result<bool> {
        let query = sqlx::query_scalar(
            r#"
            SELECT 1
            FROM tantivy.files
            WHERE index = $1
              AND path = $2
              AND deleted_at IS NULL
            "#,
        );

        let row: Option<i32> = query
            .bind(self.index)
            .bind(path)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.is_some())
    }

    /// Creates the non-metadata file into the metadata store.
    pub async fn create_file(&self, path: &str) -> sqlx::Result<()> {
        let create = sqlx::query(
            r#"
            INSERT INTO tantivy.files (index, path)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
            "#,
        );

        create
            .bind(self.index)
            .bind(path)
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
            .bind(self.index)
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
            .bind(self.index)
            .bind(path)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.is_some())
    }

    /// Reads the metadata file stored in the metadata store at the given path.
    ///
    /// Returns `None` if the file does not exist.
    pub async fn read_metadata(&self, path: &str) -> sqlx::Result<Option<Vec<u8>>> {
        let query = sqlx::query_scalar(
            r#"
            SELECT content
            FROM tantivy.metadata
            WHERE index = $1
              AND path = $2
            "#,
        );

        query
            .bind(self.index)
            .bind(path)
            .fetch_optional(&self.pool)
            .await
    }

    /// Writes the given content to the metadata store at the given path.
    pub async fn write_metadata(&self, path: &str, content: &[u8]) -> sqlx::Result<()> {
        let query = sqlx::query(
            r#"
            INSERT INTO tantivy.metadata
              (index, path, content)
            VALUES ($1, $2, $3)
            ON CONFLICT (index, path)
            DO UPDATE SET content = EXCLUDED.content
            "#,
        );

        query
            .bind(self.index)
            .bind(path)
            .bind(content)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}
