use opendal::{Operator, services::Memory};
use sqlx::PgPool;
use tantivy::{
    Directory,
    directory::{INDEX_WRITER_LOCK, error::LockError},
};
use tokio::task;
use uuid::Uuid;

use crate::{FullDirectory, context::Context, lock::WriterFence, metadata::MetadataStore};

#[tokio::test(flavor = "multi_thread")]
async fn advisory_writer_lock_contends_and_releases() {
    let pool = PgPool::connect("postgresql://postgres:postgres@localhost:15432/postgres")
        .await
        .expect("failed to connect to database");
    let operator = Operator::new(Memory::default())
        .expect("failed to create operator")
        .finish();
    let index = Uuid::new_v4();

    let first = FullDirectory::open(index, operator.clone(), pool.clone())
        .await
        .expect("failed to open first directory");
    let second = FullDirectory::open(index, operator, pool)
        .await
        .expect("failed to open second directory");

    let first_ = first.clone();
    let guard = task::spawn_blocking(move || first_.acquire_lock(&INDEX_WRITER_LOCK))
        .await
        .expect("lock task panicked")
        .expect("failed to acquire first lock");

    let same_instance = first.clone();
    let result = task::spawn_blocking(move || same_instance.acquire_lock(&INDEX_WRITER_LOCK))
        .await
        .expect("lock task panicked");
    assert!(matches!(result, Err(LockError::LockBusy)));

    let second_ = second.clone();
    let result = task::spawn_blocking(move || second_.acquire_lock(&INDEX_WRITER_LOCK))
        .await
        .expect("lock task panicked");
    assert!(matches!(result, Err(LockError::LockBusy)));

    task::spawn_blocking(move || drop(guard))
        .await
        .expect("lock drop task panicked");

    let second_ = second.clone();
    let guard = task::spawn_blocking(move || second_.acquire_lock(&INDEX_WRITER_LOCK))
        .await
        .expect("lock task panicked")
        .expect("lock was not released");
    task::spawn_blocking(move || drop(guard))
        .await
        .expect("lock drop task panicked");
}

#[tokio::test(flavor = "multi_thread")]
async fn writer_fence_blocks_unfenced_and_stale_mutations() {
    let pool = PgPool::connect("postgresql://postgres:postgres@localhost:15432/postgres")
        .await
        .expect("failed to connect to database");
    let operator = Operator::new(Memory::default())
        .expect("failed to create operator")
        .finish();
    let index = Uuid::new_v4();

    let directory = FullDirectory::open(index, operator.clone(), pool.clone())
        .await
        .expect("failed to open directory");

    let directory_ = directory.clone();
    let guard = task::spawn_blocking(move || directory_.acquire_lock(&INDEX_WRITER_LOCK))
        .await
        .expect("lock task panicked")
        .expect("failed to acquire writer lock");

    // A store that never held the lock sees the published token and cannot mutate.
    let open = MetadataStore::open(
        &Context::new(index),
        pool.clone(),
        operator.clone(),
        WriterFence::default(),
    )
    .await
    .expect("failed to open unfenced store");
    let error = open
        .create_file("unfenced.idx", true, None)
        .await
        .expect_err("unfenced store mutated while writer lock held");
    assert!(error.to_string().contains("writer fence"));

    // Simulate another writer taking over by rotating the PostgreSQL token.
    sqlx::query(
        r#"
        UPDATE tantivy.directories
        SET writer_token = $1
        WHERE index = $2
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(index)
    .execute(&pool)
    .await
    .expect("failed to rotate writer token");

    let directory_ = directory.clone();
    let error = task::spawn_blocking(move || {
        directory_.atomic_write(std::path::Path::new("meta.json"), b"{}")
    })
    .await
    .expect("write task panicked")
    .expect_err("stale writer published metadata after token rotation");
    assert!(error.to_string().contains("writer fence"));

    task::spawn_blocking(move || drop(guard))
        .await
        .expect("lock drop task panicked");
}
