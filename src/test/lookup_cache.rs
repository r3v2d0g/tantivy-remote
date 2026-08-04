//! Tests for the in-process `file_lookup` cache and [`LightDirectory::prefetch_files`].

use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use eyre::Result;
use opendal::{Operator, services::Memory};
use sqlx::PgPool;
use tantivy::directory::{Directory, RamDirectory, TerminatingWrite};
use tokio::task;
use uuid::Uuid;

use crate::{
    LightDirectory,
    context::Context,
    empty::Empty,
    lock::WriterFence,
    metadata::{MetadataStore, NewFile},
};

/// Connects to the test database.
async fn pool() -> Result<PgPool> {
    let pool = PgPool::connect("postgresql://postgres:postgres@localhost:15432/postgres").await?;
    Ok(pool)
}

/// Builds an in-memory operator for the object store backend.
fn operator() -> Result<Operator> {
    let operator = Operator::new(Memory::default())?.finish();
    Ok(operator)
}

/// Writes `data` to `path` through a [`Directory`] and finalizes it (no sync).
fn write_file(dir: &impl Directory, path: &Path, data: &[u8]) -> Result<()> {
    let mut writer = dir.open_write(path)?;
    writer.write_all(data)?;
    writer.terminate()?;

    Ok(())
}

/// Writes `data` to `path` and syncs the directory (for empty / non-bundled files).
fn write_file_synced(dir: &impl Directory, path: &Path, data: &[u8]) -> Result<()> {
    write_file(dir, path, data)?;
    dir.sync_directory()?;

    Ok(())
}

/// Reads the whole file at `path` through a [`Directory`].
fn read_file(dir: &impl Directory, path: &Path) -> Result<Vec<u8>> {
    let handle = dir.get_file_handle(path)?;
    let len = handle.len();

    Ok(handle.read_bytes(0..len)?.to_vec())
}

/// A successful `file_lookup` is cached: a second call does not issue another SELECT.
#[tokio::test(flavor = "multi_thread")]
async fn file_lookup_caches_hits() -> Result<()> {
    let index = Uuid::new_v4();
    let pool = pool().await?;
    let operator = operator()?;
    let context = Context::new(index);

    let writer = MetadataStore::open(
        &context,
        pool.clone(),
        operator.clone(),
        WriterFence::default(),
    )
    .await?;

    writer.create_file("seg.fast", true, None).await?;

    // Fresh store over the same index: empty cache, same rows.
    let store = MetadataStore::open(&context, pool, operator, WriterFence::default()).await?;
    assert_eq!(store.file_lookup_query_count(), 0);

    let first = store.file_lookup("seg.fast").await?;
    assert_eq!(first.as_ref().map(|r| r.is_empty), Some(true));
    assert_eq!(store.file_lookup_query_count(), 1);

    let second = store.file_lookup("seg.fast").await?;
    assert_eq!(second, first);
    assert_eq!(
        store.file_lookup_query_count(),
        1,
        "second lookup must be served from the cache",
    );

    Ok(())
}

/// Missing paths are not cached, so a later create remains visible without prefetch.
#[tokio::test(flavor = "multi_thread")]
async fn file_lookup_does_not_poison_on_miss() -> Result<()> {
    let index = Uuid::new_v4();
    let pool = pool().await?;
    let operator = operator()?;
    let context = Context::new(index);

    let reader = MetadataStore::open(
        &context,
        pool.clone(),
        operator.clone(),
        WriterFence::default(),
    )
    .await?;

    let writer = MetadataStore::open(&context, pool, operator, WriterFence::default()).await?;

    assert!(reader.file_lookup("seg.idx").await?.is_none());
    assert_eq!(reader.file_lookup_query_count(), 1);

    writer.create_file("seg.idx", false, Some((0, 8))).await?;

    let found = reader.file_lookup("seg.idx").await?;
    assert!(
        found.is_some(),
        "a miss must not be cached permanently; a concurrent create must be visible",
    );

    assert_eq!(reader.file_lookup_query_count(), 2);

    Ok(())
}

/// `delete_file` removes the path from the cache so a subsequent lookup sees the miss.
#[tokio::test(flavor = "multi_thread")]
async fn delete_invalidates_lookup_cache() -> Result<()> {
    let index = Uuid::new_v4();
    let pool = pool().await?;
    let operator = operator()?;
    let context = Context::new(index);
    let store = MetadataStore::open(&context, pool, operator, WriterFence::default()).await?;

    store.create_file("seg.pos", true, None).await?;
    assert!(store.file_lookup("seg.pos").await?.is_some());
    // create_file already filled the cache — no SELECT yet.
    assert_eq!(store.file_lookup_query_count(), 0);

    assert!(store.delete_file("seg.pos").await?);
    assert!(store.file_lookup("seg.pos").await?.is_none());
    assert_eq!(
        store.file_lookup_query_count(),
        1,
        "after delete, lookup must miss the cache and hit PostgreSQL",
    );

    Ok(())
}

/// After `prefetch_files`, opening bundled components issues no per-path `file_lookup`
/// SELECTs, and the components are still served from `.bundle` ranges.
#[tokio::test(flavor = "multi_thread")]
async fn light_prefetch_avoids_per_path_lookups_for_bundles() -> Result<()> {
    let index = Uuid::new_v4();
    let pool = pool().await?;
    let operator = operator()?;
    let inner = RamDirectory::create();

    let writer = LightDirectory::open(inner.clone(), index, operator.clone(), pool.clone())
        .await?
        .with_bundling();

    let idx = (PathBuf::from("seg.idx"), b"postings".to_vec());
    let store = (PathBuf::from("seg.store"), b"doc store payload".to_vec());

    let writer_ = writer.clone();
    let files = [idx.clone(), store.clone()];
    task::spawn_blocking(move || -> Result<()> {
        for (path, data) in &files {
            write_file(&writer_, path, data)?;
        }

        writer_.sync_directory()?;
        Ok(())
    })
    .await??;

    assert!(inner.exists(Path::new("seg.bundle"))?);
    assert!(!inner.exists(&idx.0)?);
    assert!(!inner.exists(&store.0)?);

    // Fresh reader directory: empty lookup cache, same inner FS + PostgreSQL rows.
    let reader = LightDirectory::open(inner, index, operator, pool)
        .await?
        .with_bundling();

    let loaded = reader.prefetch_files().await?;
    assert!(
        loaded >= 2,
        "prefetch must load the bundled component rows (got {loaded})",
    );

    assert_eq!(reader.file_lookup_query_count(), 0);

    let reader_ = reader.clone();
    let idx_path = idx.0.clone();
    let store_path = store.0.clone();
    let read = task::spawn_blocking(move || -> Result<(Vec<u8>, Vec<u8>, u64)> {
        let a = read_file(&reader_, &idx_path)?;
        let b = read_file(&reader_, &store_path)?;
        Ok((a, b, reader_.file_lookup_query_count()))
    })
    .await??;

    assert_eq!(read.0, idx.1);
    assert_eq!(read.1, store.1);
    assert_eq!(
        read.2, 0,
        "bundled opens after prefetch must not issue per-path file_lookup SELECTs",
    );

    Ok(())
}

/// Without prefetch, repeated opens of the same bundled path still benefit from the
/// single-path cache fill.
#[tokio::test(flavor = "multi_thread")]
async fn light_single_lookup_cache_helps_repeated_opens() -> Result<()> {
    let index = Uuid::new_v4();
    let pool = pool().await?;
    let operator = operator()?;
    let inner = RamDirectory::create();

    let writer = LightDirectory::open(inner.clone(), index, operator.clone(), pool.clone())
        .await?
        .with_bundling();

    let path = PathBuf::from("seg.idx");
    let data = b"postings".to_vec();

    let writer_ = writer.clone();
    let path_ = path.clone();
    let data_ = data.clone();
    task::spawn_blocking(move || -> Result<()> {
        write_file(&writer_, &path_, &data_)?;
        writer_.sync_directory()?;
        Ok(())
    })
    .await??;

    let reader = LightDirectory::open(inner, index, operator, pool)
        .await?
        .with_bundling();

    let reader_ = reader.clone();
    let path_ = path.clone();
    let counts = task::spawn_blocking(move || -> Result<(u64, u64, Vec<u8>)> {
        let before = reader_.file_lookup_query_count();
        let bytes = read_file(&reader_, &path_)?;
        let after_first = reader_.file_lookup_query_count();
        let _ = read_file(&reader_, &path_)?;
        let after_second = reader_.file_lookup_query_count();
        Ok((after_first - before, after_second - after_first, bytes))
    })
    .await??;

    assert_eq!(counts.2, data);
    assert_eq!(counts.0, 1, "first open must hit PostgreSQL once");
    assert_eq!(counts.1, 0, "second open must use the cache");

    Ok(())
}

/// `exists` / `delete` through `LightDirectory` invalidate the lookup cache.
#[tokio::test(flavor = "multi_thread")]
async fn light_delete_invalidates_exists() -> Result<()> {
    let index = Uuid::new_v4();
    let pool = pool().await?;
    let operator = operator()?;
    let inner = RamDirectory::create();

    let dir = LightDirectory::open(inner, index, operator, pool).await?;
    let path = PathBuf::from("seg.pos");
    let empty = Empty::Composite.bytes().to_vec();

    let dir_ = dir.clone();
    let path_ = path.clone();
    let result = task::spawn_blocking(move || -> Result<(bool, bool, bool)> {
        write_file_synced(&dir_, &path_, &empty)?;
        let exists_before = dir_.exists(&path_)?;
        dir_.delete(&path_)?;
        let exists_after = dir_.exists(&path_)?;
        // A second exists after delete should still be false (no stale positive cache).
        let exists_again = dir_.exists(&path_)?;
        Ok((exists_before, exists_after, exists_again))
    })
    .await??;

    assert_eq!(result, (true, false, false));

    Ok(())
}

/// Two tasks racing a miss then a create do not leave a permanent "missing" entry.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_miss_then_create_is_visible() -> Result<()> {
    let index = Uuid::new_v4();
    let pool = pool().await?;
    let operator = operator()?;
    let context = Context::new(index);

    let store =
        Arc::new(MetadataStore::open(&context, pool, operator, WriterFence::default()).await?);
    let store_a = Arc::clone(&store);
    let store_b = Arc::clone(&store);

    let miss = tokio::spawn(async move { store_a.file_lookup("race.idx").await });
    let create =
        tokio::spawn(async move { store_b.create_file("race.idx", false, Some((0, 4))).await });

    let (miss, create) = tokio::try_join!(miss, create)?;
    // The miss may observe None or Some depending on scheduling; create must succeed.
    create?;
    let _ = miss?;

    assert!(
        store.file_lookup("race.idx").await?.is_some(),
        "after create, the path must be visible (no permanent negative cache)",
    );

    Ok(())
}

/// A conflicting `create_file` (live row or soft-deleted tombstone) errors and does not
/// poison the lookup cache with the requested record.
#[tokio::test(flavor = "multi_thread")]
async fn create_file_conflict_does_not_poison_cache() -> Result<()> {
    let index = Uuid::new_v4();
    let pool = pool().await?;
    let operator = operator()?;
    let context = Context::new(index);
    let store = MetadataStore::open(
        &context,
        pool.clone(),
        operator.clone(),
        WriterFence::default(),
    )
    .await?;

    store.create_file("seg.term", true, None).await?;

    // Live conflict: second create must fail and leave the original cached record.
    let live = store.create_file("seg.term", false, Some((0, 10))).await;
    assert!(
        matches!(&live, Err(sqlx::Error::Io(err)) if err.kind() == std::io::ErrorKind::AlreadyExists),
        "expected AlreadyExists, got {live:?}",
    );

    let original = store.file_lookup("seg.term").await?;
    assert_eq!(
        original,
        Some(crate::metadata::FileRecord {
            is_empty: true,
            byte_offset: 0,
            byte_length: None,
        }),
        "failed recreate must not overwrite the cached live record",
    );

    assert_eq!(store.file_lookup_query_count(), 0);
    assert!(store.delete_file("seg.term").await?);

    // Tombstone conflict: soft-deleted PK still blocks INSERT; must not cache a live hit.
    let tombstone = store.create_file("seg.term", false, Some((4, 8))).await;
    assert!(
        matches!(&tombstone, Err(sqlx::Error::Io(err)) if err.kind() == std::io::ErrorKind::AlreadyExists),
        "expected AlreadyExists on tombstone, got {tombstone:?}",
    );

    assert!(
        store.file_lookup("seg.term").await?.is_none(),
        "failed create after soft-delete must not report the file as present",
    );

    let fresh = MetadataStore::open(&context, pool, operator, WriterFence::default()).await?;
    assert!(
        fresh.file_lookup("seg.term").await?.is_none(),
        "fresh store must also see no live row",
    );

    Ok(())
}

/// A conflict anywhere in a bulk create rolls back every file in that sync and leaves
/// the lookup cache unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn create_files_is_atomic() -> Result<()> {
    let index = Uuid::new_v4();
    let pool = pool().await?;
    let operator = operator()?;
    let context = Context::new(index);
    let store = MetadataStore::open(
        &context,
        pool.clone(),
        operator.clone(),
        WriterFence::default(),
    )
    .await?;

    store.create_file("existing.idx", false, None).await?;

    let result = store
        .create_files(vec![
            NewFile::new("new.idx", false, Some((0, 8))),
            NewFile::new("existing.idx", true, None),
        ])
        .await;

    assert!(
        matches!(&result, Err(sqlx::Error::Io(err)) if err.kind() == std::io::ErrorKind::AlreadyExists),
        "expected AlreadyExists, got {result:?}",
    );

    assert!(
        store.file_lookup("new.idx").await?.is_none(),
        "rolled-back file must not be cached",
    );

    let fresh = MetadataStore::open(&context, pool, operator, WriterFence::default()).await?;
    assert!(
        fresh.file_lookup("new.idx").await?.is_none(),
        "rolled-back file must not exist in PostgreSQL",
    );

    Ok(())
}
