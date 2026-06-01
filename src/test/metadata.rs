use std::path::Path;

use opendal::{Operator, services::Memory};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{context::Context, metadata::MetadataStore};

/// Connects to the test database, panicking if it is unreachable.
async fn pool() -> PgPool {
    PgPool::connect("postgresql://postgres:postgres@localhost:15432/postgres")
        .await
        .expect("failed to connect to database")
}

/// Builds an in-memory operator for the object store backend.
fn operator() -> Operator {
    let service = Memory::default();
    Operator::new(service)
        .expect("failed to create operator")
        .finish()
}

/// Returns the raw `content` column for the given metadata path, where the outer
/// `Option` indicates whether the row exists and the inner one whether its content is
/// `NULL` (i.e. the file is stored remotely).
async fn raw_content(pool: &PgPool, index: Uuid, path: &str) -> Option<Option<Vec<u8>>> {
    sqlx::query_scalar(
        r#"
        SELECT content
        FROM tantivy.metadata
        WHERE index = $1
          AND path = $2
        "#,
    )
    .bind(index)
    .bind(path)
    .fetch_optional(pool)
    .await
    .expect("failed to read raw metadata content")
}

/// Metadata smaller than the threshold is stored inline in PostgreSQL and can be read
/// back unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn small_metadata_stored_in_postgresql() {
    let index = Uuid::new_v4();
    let pool = pool().await;
    let operator = operator();

    let mut context = Context::new(index);
    context.threshold = 1024;
    let store = MetadataStore::open(&context, pool.clone(), operator.clone())
        .await
        .expect("failed to open metadata store");

    let filepath = Path::new("meta.json");
    let content = vec![42u8; 512];

    store
        .write_metadata(filepath, &content)
        .await
        .expect("failed to write metadata");

    let read = store
        .read_metadata(filepath)
        .await
        .expect("failed to read metadata");
    assert_eq!(read.as_deref(), Some(content.as_slice()));

    // The content lives inline in PostgreSQL, so the row exists and is non-`NULL`.
    let raw = raw_content(&pool, index, "meta.json").await;
    assert_eq!(raw, Some(Some(content.clone())));

    // Nothing was written to the object store.
    let remote = operator
        .exists(context.path(filepath).to_str().unwrap())
        .await
        .expect("failed to check object store");
    assert!(!remote, "small metadata must not be stored remotely");
}

/// Metadata at or above the threshold is offloaded to the object store and can be read
/// back unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn large_metadata_stored_in_object_store() {
    let index = Uuid::new_v4();
    let pool = pool().await;
    let operator = operator();

    let mut context = Context::new(index);
    context.threshold = 1024;
    let store = MetadataStore::open(&context, pool.clone(), operator.clone())
        .await
        .expect("failed to open metadata store");

    let filepath = Path::new("meta.json");
    let content = vec![7u8; 4096];

    store
        .write_metadata(filepath, &content)
        .await
        .expect("failed to write metadata");

    let read = store
        .read_metadata(filepath)
        .await
        .expect("failed to read metadata");
    assert_eq!(read.as_deref(), Some(content.as_slice()));

    // The bytes live in the object store, not inline in PostgreSQL.
    let remote = operator
        .exists(context.path(filepath).to_str().unwrap())
        .await
        .expect("failed to check object store");
    assert!(remote, "large metadata must be stored remotely");
}

/// Reading a metadata file that was never written returns `None`.
#[tokio::test(flavor = "multi_thread")]
async fn missing_metadata_returns_none() {
    let index = Uuid::new_v4();
    let pool = pool().await;
    let operator = operator();

    let mut context = Context::new(index);
    context.threshold = 1024;
    let store = MetadataStore::open(&context, pool, operator)
        .await
        .expect("failed to open metadata store");

    let read = store
        .read_metadata(Path::new("missing.json"))
        .await
        .expect("failed to read metadata");
    assert_eq!(read, None);
}

/// Overwriting a file so that it crosses the threshold moves it from PostgreSQL to the
/// object store, and the latest content is what is read back.
#[tokio::test(flavor = "multi_thread")]
async fn rewrite_across_threshold() {
    let index = Uuid::new_v4();
    let pool = pool().await;
    let operator = operator();

    let mut context = Context::new(index);
    context.threshold = 1024;
    let store = MetadataStore::open(&context, pool.clone(), operator.clone())
        .await
        .expect("failed to open metadata store");

    let filepath = Path::new("meta.json");

    // First write a small payload (stored in PostgreSQL).
    let small = vec![1u8; 256];
    store
        .write_metadata(filepath, &small)
        .await
        .expect("failed to write small metadata");
    assert_eq!(
        store.read_metadata(filepath).await.unwrap().as_deref(),
        Some(small.as_slice())
    );

    // Then overwrite it with a large payload (offloaded to the object store).
    let large = vec![2u8; 4096];
    store
        .write_metadata(filepath, &large)
        .await
        .expect("failed to write large metadata");
    assert_eq!(
        store.read_metadata(filepath).await.unwrap().as_deref(),
        Some(large.as_slice())
    );
}
