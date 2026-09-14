use std::path::Path;

use opendal::{Operator, services::Memory};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tantivy::{
    Directory, Index,
    directory::{META_LOCK, RamDirectory, error::OpenReadError},
};
use tokio::task;
use uuid::Uuid;

use crate::{
    FullDirectory, LightDirectory, context::Context, lock::WriterFence, metadata::MetadataStore,
};

async fn pool(read_only: bool) -> PgPool {
    let pool = PgPoolOptions::new()
        .after_connect(move |connection, _| {
            Box::pin(async move {
                if read_only {
                    sqlx::query("SET default_transaction_read_only = on")
                        .execute(connection)
                        .await?;
                }

                Ok(())
            })
        })
        .connect("postgresql://postgres:postgres@localhost:15432/postgres")
        .await
        .expect("failed to connect to database");

    if read_only {
        let setting: String = sqlx::query_scalar("SHOW default_transaction_read_only")
            .fetch_one(&pool)
            .await
            .expect("failed to read transaction read-only setting");

        assert_eq!(setting, "on");
    }

    pool
}

fn operator() -> Operator {
    Operator::new(Memory::default())
        .expect("failed to create in-memory operator")
        .finish()
}

async fn directory_exists(pool: &PgPool, index: Uuid) -> bool {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM tantivy.directories WHERE index = $1)")
        .bind(index)
        .fetch_one(pool)
        .await
        .expect("failed to check directory row")
}

#[tokio::test(flavor = "multi_thread")]
async fn metadata_read_only_open_leaves_missing_directory_absent() {
    let pool = pool(true).await;
    let index = Uuid::new_v4();
    let store = MetadataStore::open_read_only(
        &Context::new(index),
        pool.clone(),
        operator(),
        WriterFence::default(),
    )
    .await
    .expect("failed to open read-only metadata store");

    assert_eq!(
        store
            .read_metadata(Path::new("meta.json"))
            .await
            .expect("failed to read inline metadata"),
        None
    );

    assert!(
        !store
            .metadata_exists("meta.json")
            .await
            .expect("failed to check metadata existence")
    );
    assert_eq!(
        store
            .file_lookup("missing.idx")
            .await
            .expect("failed to look up missing file"),
        None
    );
    assert_eq!(
        store
            .prefetch_files()
            .await
            .expect("failed to prefetch file metadata"),
        0
    );
    assert!(!directory_exists(&pool, index).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn metadata_read_only_open_reads_existing_inline_and_remote_metadata() {
    let writer_pool = pool(false).await;
    let index = Uuid::new_v4();
    let operator = operator();
    let mut context = Context::new(index);
    context.threshold = 16;
    context.read_chunks = Some(8);
    context.read_concurrency = Some(2);

    let writer = MetadataStore::open(
        &context,
        writer_pool.clone(),
        operator.clone(),
        WriterFence::default(),
    )
    .await
    .expect("failed to open writer metadata store");

    assert!(directory_exists(&writer_pool, index).await);
    writer
        .write_metadata(Path::new("meta.json"), b"inline")
        .await
        .expect("failed to write inline metadata");

    let remote = vec![7; 64];
    writer
        .write_metadata(Path::new(".managed.json"), &remote)
        .await
        .expect("failed to write remote metadata");
    writer
        .create_file("empty.idx", true, None)
        .await
        .expect("failed to create file metadata");

    let reader =
        MetadataStore::open_read_only(&context, pool(true).await, operator, WriterFence::default())
            .await
            .expect("failed to open read-only metadata store");

    assert_eq!(
        reader
            .read_metadata(Path::new("meta.json"))
            .await
            .expect("failed to read inline metadata"),
        Some(b"inline".to_vec())
    );

    assert_eq!(
        reader
            .read_metadata(Path::new(".managed.json"))
            .await
            .expect("failed to read remote metadata"),
        Some(remote)
    );

    assert!(
        reader
            .file_exists("empty.idx")
            .await
            .expect("failed to check file existence")
    );
    assert_eq!(
        reader
            .prefetch_files()
            .await
            .expect("failed to prefetch file metadata"),
        1
    );
    assert_eq!(
        reader
            .read_metadata(Path::new("missing.json"))
            .await
            .expect("failed to read missing metadata"),
        None
    );

    sqlx::query("DELETE FROM tantivy.directories WHERE index = $1")
        .bind(index)
        .execute(&writer_pool)
        .await
        .expect("failed to clean up directory row");
}

fn assert_missing<D: Directory + Clone>(directory: D) {
    for path in ["meta.json", ".managed.json", "missing.idx"] {
        assert!(
            !directory
                .exists(Path::new(path))
                .expect("failed to check file existence")
        );
    }

    for path in ["meta.json", ".managed.json"] {
        assert!(matches!(
            directory.atomic_read(Path::new(path)),
            Err(OpenReadError::FileDoesNotExist(_))
        ));
    }

    assert!(matches!(
        directory.get_file_handle(Path::new("missing.idx")),
        Err(OpenReadError::FileDoesNotExist(_))
    ));

    assert!(Index::open(directory.clone()).is_err());
    assert!(
        !directory
            .exists(Path::new("meta.json"))
            .expect("failed to check file existence")
    );

    // Reader metadata locking retains the normal PostgreSQL advisory-lock path.
    let guard = directory
        .acquire_lock(&META_LOCK)
        .expect("failed to acquire metadata advisory lock");
    drop(guard);
}

#[tokio::test(flavor = "multi_thread")]
async fn directory_read_only_constructors_do_not_initialize_missing_indexes() {
    let pool = pool(true).await;
    let full_index = Uuid::new_v4();
    let light_index = Uuid::new_v4();
    let full = FullDirectory::open_read_only(full_index, operator(), pool.clone())
        .await
        .expect("failed to open read-only full directory");

    let inner = RamDirectory::default();
    let light =
        LightDirectory::open_read_only(inner.clone(), light_index, operator(), pool.clone())
            .await
            .expect("failed to open read-only light directory");

    assert_eq!(
        full.prefetch_files()
            .await
            .expect("failed to prefetch file metadata"),
        0
    );
    assert_eq!(
        light
            .prefetch_files()
            .await
            .expect("failed to prefetch file metadata"),
        0
    );
    task::spawn_blocking(move || {
        assert_missing(full);
        assert_missing(light);
        assert!(
            !inner
                .exists(Path::new("meta.json"))
                .expect("failed to check file existence")
        );
    })
    .await
    .expect("missing metadata task panicked");

    assert!(!directory_exists(&pool, full_index).await);
    assert!(!directory_exists(&pool, light_index).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn directory_writers_create_rows_and_read_only_constructors_read_metadata() {
    let writer_pool = pool(false).await;
    let reader_pool = pool(true).await;
    let operator = operator();
    let full_index = Uuid::new_v4();
    let light_index = Uuid::new_v4();
    let full = FullDirectory::open(full_index, operator.clone(), writer_pool.clone())
        .await
        .expect("failed to open full directory for writing");

    let inner = RamDirectory::default();
    let light = LightDirectory::open(
        inner.clone(),
        light_index,
        operator.clone(),
        writer_pool.clone(),
    )
    .await
    .expect("failed to open light directory for writing");

    assert!(directory_exists(&writer_pool, full_index).await);
    assert!(directory_exists(&writer_pool, light_index).await);

    let remote = vec![9; 64];
    let content = remote.clone();
    task::spawn_blocking(move || {
        let full = full.with_threshold(16);
        let light = light.with_threshold(16);
        for directory in [&full as &dyn Directory, &light as &dyn Directory] {
            directory
                .atomic_write(Path::new("meta.json"), b"inline")
                .expect("failed to write inline metadata");
            directory
                .atomic_write(Path::new(".managed.json"), &content)
                .expect("failed to write remote metadata");
        }
    })
    .await
    .expect("metadata write task panicked");

    let full = FullDirectory::open_read_only(full_index, operator.clone(), reader_pool.clone())
        .await
        .expect("failed to open read-only full directory")
        .with_read_chunks(8)
        .with_read_concurrency(2);

    let light = LightDirectory::open_read_only(inner, light_index, operator, reader_pool)
        .await
        .expect("failed to open read-only light directory")
        .with_read_chunks(8)
        .with_read_concurrency(2);

    task::spawn_blocking(move || {
        for directory in [&full as &dyn Directory, &light as &dyn Directory] {
            assert_eq!(
                directory
                    .atomic_read(Path::new("meta.json"))
                    .expect("failed to read inline metadata"),
                b"inline"
            );

            assert_eq!(
                directory
                    .atomic_read(Path::new(".managed.json"))
                    .expect("failed to read remote metadata"),
                remote
            );

            assert!(
                !directory
                    .exists(Path::new("missing.idx"))
                    .expect("failed to check file existence")
            );
        }
    })
    .await
    .expect("metadata read task panicked");

    for index in [full_index, light_index] {
        sqlx::query("DELETE FROM tantivy.directories WHERE index = $1")
            .bind(index)
            .execute(&writer_pool)
            .await
            .expect("failed to clean up directory row");
    }
}
