use derive_more::Debug;
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
    pub(crate) fn new(index: Uuid, pool: PgPool) -> Self {
        Self { index, pool }
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
