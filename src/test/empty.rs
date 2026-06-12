//! End-to-end tests for the [logically empty][1] file optimization, covering both the
//! [`FullDirectory`] (backed by [`opendal`]) and the [`LightDirectory`] (backed by an
//! inner [`Directory`]).
//!
//! Each test exercises the four requirements: an empty file is detected, recorded as
//! empty in PostgreSQL, *not* written to the backing store, and reconstructed from
//! memory on read.
//!
//! [1]: crate::empty

use std::{
    io::Write,
    path::{Path, PathBuf},
};

use eyre::Result;
use opendal::{Operator, services::Memory};
use sqlx::PgPool;
use tantivy::directory::{Directory, RamDirectory, TerminatingWrite};
use tokio::task;
use uuid::Uuid;

use crate::{FullDirectory, LightDirectory, empty::Empty};

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

/// Reads the `is_empty` flag for a file row, or `None` if it does not exist.
async fn file_is_empty(pool: &PgPool, index: Uuid, path: &str) -> Result<Option<bool>> {
    let is_empty = sqlx::query_scalar(
        r#"
        SELECT is_empty
        FROM tantivy.files
        WHERE index = $1
          AND path = $2
          AND deleted_at IS NULL
        "#,
    )
    .bind(index)
    .bind(path)
    .fetch_optional(pool)
    .await?;

    Ok(is_empty)
}

/// Writes `data` to `path` through a [`Directory`] and finalizes it, then syncs.
fn write_file(dir: &impl Directory, path: &Path, data: &[u8]) -> Result<()> {
    let mut writer = dir.open_write(path)?;
    writer.write_all(data)?;
    writer.terminate()?;
    dir.sync_directory()?;

    Ok(())
}

/// Reads the whole file at `path` through a [`Directory`].
fn read_file(dir: &impl Directory, path: &Path) -> Result<Vec<u8>> {
    let handle = dir.get_file_handle(path)?;
    let len = handle.len();

    Ok(handle.read_bytes(0..len)?.to_vec())
}

/// In the [`FullDirectory`], a logically empty file is recorded as empty, never written
/// to the object store, and reconstructed byte-for-byte from memory on read.
#[tokio::test(flavor = "multi_thread")]
async fn full_skips_and_reconstructs_empty_file() -> Result<()> {
    let index = Uuid::new_v4();
    let pool = pool().await?;
    let operator = operator()?;

    let dir = FullDirectory::open(index, operator.clone(), pool.clone()).await?;

    let path = PathBuf::from("seg.idx");
    let empty = Empty::Composite.bytes().to_vec();

    let dir_ = dir.clone();
    let empty_ = empty.clone();
    let path_ = path.clone();
    task::spawn_blocking(move || write_file(&dir_, &path_, &empty_)).await??;

    // (2) PostgreSQL records the file as empty.
    assert_eq!(file_is_empty(&pool, index, "seg.idx").await?, Some(true));

    // (3) The bytes were never written to the object store.
    let remote = format!("idx-{index}/seg.idx");
    assert!(
        !operator.exists(&remote).await?,
        "empty file must not be stored in the object store",
    );

    // (4) Reading reconstructs the exact bytes from memory.
    let dir_ = dir.clone();
    let path_ = path.clone();
    let read = task::spawn_blocking(move || read_file(&dir_, &path_)).await??;
    assert_eq!(read, empty);

    Ok(())
}

/// A non-empty file in the [`FullDirectory`] is stored in the object store as usual and
/// not flagged as empty.
#[tokio::test(flavor = "multi_thread")]
async fn full_stores_non_empty_file() -> Result<()> {
    let index = Uuid::new_v4();
    let pool = pool().await?;
    let operator = operator()?;

    let dir = FullDirectory::open(index, operator.clone(), pool.clone()).await?;

    let path = PathBuf::from("seg.idx");
    // The empty template with an extra trailing byte: same component, no longer empty.
    let mut data = Empty::Composite.bytes().to_vec();
    data.push(0xff);

    let dir_ = dir.clone();
    let data_ = data.clone();
    let path_ = path.clone();
    task::spawn_blocking(move || write_file(&dir_, &path_, &data_)).await??;

    assert_eq!(file_is_empty(&pool, index, "seg.idx").await?, Some(false));

    let remote = format!("idx-{index}/seg.idx");
    assert!(
        operator.exists(&remote).await?,
        "non-empty file must be stored in the object store",
    );

    let dir_ = dir.clone();
    let read = task::spawn_blocking(move || read_file(&dir_, &path)).await??;
    assert_eq!(read, data);

    Ok(())
}

/// In the [`LightDirectory`], a logically empty file is recorded as empty, never written
/// to the inner directory, and reconstructed from memory on read; a non-empty file goes
/// to the inner directory untouched.
#[tokio::test(flavor = "multi_thread")]
async fn light_skips_empty_and_forwards_non_empty() -> Result<()> {
    let index = Uuid::new_v4();
    let pool = pool().await?;
    let operator = operator()?;
    let inner = RamDirectory::create();

    let dir = LightDirectory::open(inner.clone(), index, operator, pool.clone()).await?;

    let empty_path = PathBuf::from("seg.fast");
    let empty = Empty::Fast.bytes().to_vec();
    let full_path = PathBuf::from("seg.store");
    let data = b"this is a real doc store payload".to_vec();

    let dir_ = dir.clone();
    let empty_ = empty.clone();
    let empty_path_ = empty_path.clone();
    let data_ = data.clone();
    let full_path_ = full_path.clone();
    task::spawn_blocking(move || -> Result<()> {
        write_file(&dir_, &empty_path_, &empty_)?;
        write_file(&dir_, &full_path_, &data_)?;
        Ok(())
    })
    .await??;

    // (2) The empty file is recorded as empty; the non-empty one is not tracked here.
    assert_eq!(file_is_empty(&pool, index, "seg.fast").await?, Some(true));
    assert_eq!(file_is_empty(&pool, index, "seg.store").await?, None);

    // (3) The empty file is absent from the inner directory; the non-empty one is there.
    assert!(
        !inner.exists(&empty_path)?,
        "empty file must not be written to the inner directory",
    );
    assert!(
        inner.exists(&full_path)?,
        "non-empty file must be written to the inner directory",
    );

    // (4) Both read back correctly: the empty one from memory, the other from the inner.
    let dir_ = dir.clone();
    let (empty_read, full_read) = task::spawn_blocking(move || -> Result<(Vec<u8>, Vec<u8>)> {
        Ok((
            read_file(&dir_, &empty_path)?,
            read_file(&dir_, &full_path)?,
        ))
    })
    .await??;
    assert_eq!(empty_read, empty);
    assert_eq!(full_read, data);

    Ok(())
}

/// `exists` and `delete` work for logically empty files in the [`LightDirectory`], even
/// though they never reach the inner directory.
#[tokio::test(flavor = "multi_thread")]
async fn light_exists_and_delete_empty_file() -> Result<()> {
    let index = Uuid::new_v4();
    let pool = pool().await?;
    let operator = operator()?;
    let inner = RamDirectory::create();

    let dir = LightDirectory::open(inner, index, operator, pool.clone()).await?;

    let path = PathBuf::from("seg.pos");
    let empty = Empty::Composite.bytes().to_vec();

    let dir_ = dir.clone();
    let path_ = path.clone();
    let exists_after_write = task::spawn_blocking(move || -> Result<(bool, bool)> {
        write_file(&dir_, &path_, &empty)?;
        let exists = dir_.exists(&path_)?;
        dir_.delete(&path_)?;
        Ok((exists, dir_.exists(&path_)?))
    })
    .await??;

    assert_eq!(exists_after_write, (true, false));
    assert_eq!(file_is_empty(&pool, index, "seg.pos").await?, None);

    Ok(())
}
