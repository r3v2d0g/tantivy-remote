//! End-to-end tests for segment-file [bundling][1], covering both the [`FullDirectory`]
//! (bundle object in the object store) and the [`LightDirectory`] (bundle file in the
//! inner directory).
//!
//! [1]: crate::bundle

use std::{
    io::Write,
    path::{Path, PathBuf},
};

use eyre::Result;
use opendal::{Operator, services::Memory};
use sqlx::PgPool;
use tantivy::{
    DocAddress, Index, IndexSettings, ReloadPolicy, Score, TantivyDocument,
    collector::TopDocs,
    directory::{Directory, RamDirectory, TerminatingWrite},
    doc,
    query::QueryParser,
    schema::{STORED, SchemaBuilder, TEXT},
};
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

/// Reads `(byte_offset, byte_length)` for a file row, or `None` if it does not exist.
async fn byte_range(pool: &PgPool, index: Uuid, path: &str) -> Result<Option<(i64, Option<i64>)>> {
    let row = sqlx::query_as(
        r#"
        SELECT byte_offset, byte_length
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

    Ok(row)
}

/// Writes `data` to `path` through a [`Directory`] and finalizes it.
fn write_file(dir: &impl Directory, path: &Path, data: &[u8]) -> Result<()> {
    let mut writer = dir.open_write(path)?;
    writer.write_all(data)?;
    writer.terminate()?;
    Ok(())
}

/// Reads the whole file at `path` through a [`Directory`].
fn read_file(dir: &impl Directory, path: &Path) -> Result<Vec<u8>> {
    let handle = dir.get_file_handle(path)?;
    let len = handle.len();
    Ok(handle.read_bytes(0..len)?.to_vec())
}

/// In the [`FullDirectory`] with bundling, a segment's component files are written as a
/// single `<segment>.bundle` object (not one object each), recorded with their byte
/// ranges, and read back byte-for-byte. A `.del` file stays standalone.
#[tokio::test(flavor = "multi_thread")]
async fn full_bundles_segment_components() -> Result<()> {
    let index = Uuid::new_v4();
    let pool = pool().await?;
    let operator = operator()?;

    let dir = FullDirectory::open(index, operator.clone(), pool.clone())
        .await?
        .with_bundling();

    let idx = (PathBuf::from("seg.idx"), b"postings bytes".to_vec());
    let term = (
        PathBuf::from("seg.term"),
        b"term dictionary bytes!!".to_vec(),
    );
    let del = (PathBuf::from("seg.7.del"), b"deletes".to_vec());

    let dir_ = dir.clone();
    let files = [idx.clone(), term.clone(), del.clone()];
    task::spawn_blocking(move || -> Result<()> {
        for (path, data) in &files {
            write_file(&dir_, path, data)?;
        }
        dir_.sync_directory()?;
        Ok(())
    })
    .await??;

    // The two components live in one bundle object; their individual objects do not exist.
    assert!(
        operator.exists(&format!("idx-{index}/seg.bundle")).await?,
        "the segment bundle object must exist",
    );
    assert!(!operator.exists(&format!("idx-{index}/seg.idx")).await?);
    assert!(!operator.exists(&format!("idx-{index}/seg.term")).await?);

    // The `.del` file is not bundled: it is its own object.
    assert!(operator.exists(&format!("idx-{index}/seg.7.del")).await?);
    assert!(
        !operator
            .exists(&format!("idx-{index}/seg.bundle.del"))
            .await?
    );

    // Components are laid out in sorted order: `seg.idx` then `seg.term`.
    assert_eq!(
        byte_range(&pool, index, "seg.idx").await?,
        Some((0, Some(idx.1.len() as i64))),
    );
    assert_eq!(
        byte_range(&pool, index, "seg.term").await?,
        Some((idx.1.len() as i64, Some(term.1.len() as i64))),
    );
    // The `.del` file is standalone: it has no byte length.
    assert_eq!(
        byte_range(&pool, index, "seg.7.del").await?,
        Some((0, None))
    );

    // Every file reads back byte-for-byte, bundled or not.
    let dir_ = dir.clone();
    let read = task::spawn_blocking(move || -> Result<Vec<Vec<u8>>> {
        Ok(vec![
            read_file(&dir_, &idx.0)?,
            read_file(&dir_, &term.0)?,
            read_file(&dir_, &del.0)?,
        ])
    })
    .await??;
    assert_eq!(read, vec![idx.1, term.1, del.1]);

    Ok(())
}

/// In the [`LightDirectory`] with bundling, a segment's components are written as a
/// single `<segment>.bundle` file in the inner directory (not one file each) and read
/// back byte-for-byte.
#[tokio::test(flavor = "multi_thread")]
async fn light_bundles_segment_components() -> Result<()> {
    let index = Uuid::new_v4();
    let pool = pool().await?;
    let operator = operator()?;
    let inner = RamDirectory::create();

    let dir = LightDirectory::open(inner.clone(), index, operator, pool.clone())
        .await?
        .with_bundling();

    let idx = (PathBuf::from("seg.idx"), b"postings".to_vec());
    let store = (PathBuf::from("seg.store"), b"doc store payload".to_vec());

    let dir_ = dir.clone();
    let files = [idx.clone(), store.clone()];
    task::spawn_blocking(move || -> Result<()> {
        for (path, data) in &files {
            write_file(&dir_, path, data)?;
        }
        dir_.sync_directory()?;
        Ok(())
    })
    .await??;

    // The inner directory holds the bundle, not the individual components.
    assert!(
        inner.exists(Path::new("seg.bundle"))?,
        "the bundle file must exist in the inner directory",
    );
    assert!(!inner.exists(&idx.0)?);
    assert!(!inner.exists(&store.0)?);

    assert_eq!(
        byte_range(&pool, index, "seg.idx").await?,
        Some((0, Some(idx.1.len() as i64))),
    );

    let dir_ = dir.clone();
    let read = task::spawn_blocking(move || -> Result<(Vec<u8>, Vec<u8>)> {
        Ok((read_file(&dir_, &idx.0)?, read_file(&dir_, &store.0)?))
    })
    .await??;
    assert_eq!(read, (idx.1, store.1));

    Ok(())
}

/// A `bundle_max_file_bytes` smaller than `Empty::max_len()` must not defeat empty
/// detection: the cap is floored so an empty component is still recognized and skipped,
/// never written out as an object.
#[tokio::test(flavor = "multi_thread")]
async fn bundling_with_tiny_cap_still_detects_empty() -> Result<()> {
    let index = Uuid::new_v4();
    let pool = pool().await?;
    let operator = operator()?;

    let dir = FullDirectory::open(index, operator.clone(), pool.clone())
        .await?
        .with_bundling()
        .with_bundle_max_file_bytes(1);

    // An empty `.fast` is 146 bytes — far above the requested cap of 1.
    let path = PathBuf::from("seg.fast");
    let empty = Empty::Fast.bytes().to_vec();

    let dir_ = dir.clone();
    let empty_ = empty.clone();
    let path_ = path.clone();
    task::spawn_blocking(move || -> Result<()> {
        write_file(&dir_, &path_, &empty_)?;
        dir_.sync_directory()?;
        Ok(())
    })
    .await??;

    // Recognized as empty: nothing was written to the object store, neither as its own
    // object nor as a bundle.
    assert!(!operator.exists(&format!("idx-{index}/seg.fast")).await?);
    assert!(!operator.exists(&format!("idx-{index}/seg.bundle")).await?);

    // And it still reconstructs from memory.
    let dir_ = dir.clone();
    let read = task::spawn_blocking(move || read_file(&dir_, &path)).await??;
    assert_eq!(read, empty);

    Ok(())
}

/// A real index built and searched through a bundling [`FullDirectory`] round-trips: the
/// segment files are bundled on write and served back to tantivy on read.
#[tokio::test(flavor = "multi_thread")]
async fn full_bundling_round_trips_a_real_index() -> Result<()> {
    let index_id = Uuid::new_v4();
    let pool = pool().await?;
    let operator = operator()?;

    let directory = FullDirectory::open(index_id, operator, pool)
        .await?
        .with_bundling();

    search_one(directory).await
}

/// The same, through a bundling [`LightDirectory`] over an in-memory inner directory.
#[tokio::test(flavor = "multi_thread")]
async fn light_bundling_round_trips_a_real_index() -> Result<()> {
    let index_id = Uuid::new_v4();
    let pool = pool().await?;
    let operator = operator()?;
    let inner = RamDirectory::create();

    let directory = LightDirectory::open(inner, index_id, operator, pool)
        .await?
        .with_bundling();

    search_one(directory).await
}

/// Builds a one-document index over `directory`, then reloads and searches it, asserting
/// the document is found — exercising the full bundled write/read cycle through tantivy.
async fn search_one(directory: impl Directory) -> Result<()> {
    let mut schema = SchemaBuilder::new();
    let title = schema.add_text_field("title", TEXT | STORED);
    let body = schema.add_text_field("body", TEXT);
    let schema = schema.build();

    let init = task::spawn_blocking(move || -> Result<(Index, _)> {
        let index = Index::create(directory, schema, IndexSettings::default())?;
        let writer = index.writer(50_000_000)?;
        Ok((index, writer))
    });
    let (index, mut writer) = init.await??;

    let write = task::spawn_blocking(move || -> Result<()> {
        writer.add_document(doc!(
            title => "The Old Man and the Sea",
            body => "He was an old man who fished alone in a skiff in the Gulf Stream.",
        ))?;
        writer.commit()?;
        Ok(())
    });
    write.await??;

    let parser = QueryParser::for_index(&index, vec![title, body]);
    let query = parser.parse_query("sea")?;

    let search = task::spawn_blocking(move || -> Result<Vec<(Score, DocAddress)>> {
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let top = reader
            .searcher()
            .search(&query, &TopDocs::with_limit(10).order_by_score())?;

        // Read the stored document back too, which reads from the bundled store file.
        for (_score, addr) in &top {
            let _doc: TantivyDocument = reader.searcher().doc(*addr)?;
        }

        Ok(top)
    });

    let top = search.await??;
    assert_eq!(top.len(), 1, "the indexed document should be found");

    Ok(())
}
