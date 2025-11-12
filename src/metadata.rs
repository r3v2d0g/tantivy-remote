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
        let create = sqlx::query!(
            r#"
            INSERT INTO tantivy.directories (index)
            VALUES ($1)
            ON CONFLICT DO NOTHING
            "#,
            index,
        );

        create
            .execute(&pool)
            .await
            .wrap_err("failed to create index")?;

        Ok(Self { index, pool })
    }

    /// Returns `true` if there is a file with the given path stored in the metadata
    /// store.
    pub async fn exists(&self, path: &str) -> sqlx::Result<bool> {
        let query = sqlx::query_scalar!(
            r#"
            SELECT 1
            FROM tantivy.metadata
            WHERE index = $1
              AND path = $2
            "#,
            self.index,
            path,
        );

        let row = query.fetch_optional(&self.pool).await?;

        Ok(row.is_some())
    }

    /// Reads the metadata file stored in the metadata store at the given path.
    ///
    /// Returns `None` if the file does not exist.
    pub async fn read(&self, path: &str) -> sqlx::Result<Option<Vec<u8>>> {
        let query = sqlx::query_scalar!(
            r#"
            SELECT content
            FROM tantivy.metadata
            WHERE index = $1
              AND path = $2
            "#,
            self.index,
            path,
        );

        query.fetch_optional(&self.pool).await
    }

    /// Writes the given content to the metadata store at the given path.
    pub async fn write(&self, path: &str, content: &[u8]) -> sqlx::Result<()> {
        let query = sqlx::query!(
            r#"
            INSERT INTO tantivy.metadata
              (index, path, content)
            VALUES ($1, $2, $3)
            ON CONFLICT (index, path)
            DO UPDATE SET content = EXCLUDED.content
            "#,
            self.index,
            path,
            content,
        );

        query.execute(&self.pool).await?;

        Ok(())
    }
}
