# `tantivy-remote`

An implementation of `tantivy`'s `Directory` that uses `opendal` and `sqlx`, the
former for the data, and the latter for the metadata.

## Logically empty files

When `tantivy` writes a segment, some `SegmentComponent`s end up holding no data
(e.g. `.pos` when no field records positions, `.fast` when there is no fast field,
`.fieldnorm` when no field has fieldnorms). Such a file is not byte-empty — it still
contains a small structural header and `tantivy`'s footer — but it encodes the
*default*/empty value for its component, so its bytes are constant for a given
`tantivy` version.

These *logically empty* files are recognized on write (see `src/empty.rs`), flagged
as such in PostgreSQL (the `is_empty` column on `tantivy.files`), and **not** written
to the object store (`FullDirectory`) or the inner `Directory` (`LightDirectory`). On
read they are reconstructed from an in-memory constant instead of hitting the backing
store. Matching against the exact captured bytes means a future `tantivy` format
change simply disables the optimization rather than risking corruption.

## Segment bundling (optional)

An index with many small segments produces a huge number of tiny objects (one
per `SegmentComponent`), which is expensive to write and read over an object
store. With bundling enabled, a segment's non-empty, non-`.del` component files
are buffered in memory as they are written and, at `sync_directory`,
concatenated into a single `<segment_uuid>.bundle` object. Each component then
lives at a byte range inside that object recorded in PostgreSQL. On read, a
bundled component is served as a sub-range of its bundle object.

`.del` files are never bundled (they are mutable and written after the segment),
and a component larger than `with_bundle_max_file_bytes` (default 16 MiB) stays
a standalone object so a large merge segment is never held in memory. Bundling
composes with the logically-empty optimization above: empty components are
skipped, not bundled.

## File-lookup prefetch

Empty and bundled component opens resolve through a PostgreSQL `file_lookup` when
the path is absent from the object store/inner directory. Successful results
are cached in-process. For a cold open or reload of a large bundled index, call
`prefetch_files()` once on the directory before footer warm/`SegmentReader::open`
so those opens do not pay one `SELECT` per path:

```rust
dir.prefetch_files().await?;
```

Missing paths are not cached (a concurrent writer can still create them). After
another process commits, call `prefetch_files` again on that directory instance.
Memory is `O(number of file rows)` for the index.

## PostgreSQL pool sizing and writer fencing

Tantivy's directory locks are implemented with PostgreSQL session-level advisory
locks. Each held lock checks out one connection for its full lifetime. A live
`IndexWriter` therefore reserves one pool connection, while short-lived metadata
locks may reserve another connection during reader reload or garbage collection.

Size the pool for the maximum number of concurrently open writers and metadata
locks, plus the connections needed for normal file and metadata queries. Lock
connections execute `SELECT 1` every 30 seconds so PostgreSQL and network proxies do
not close an otherwise idle session and release its advisory lock.

Acquiring the writer lock also publishes a fencing token on
`tantivy.directories.writer_token`. Mutating metadata operations
(`create_files`, `delete_file`, `write_metadata`) check that token in the same
PostgreSQL transaction as the mutation. If the lock session dies, the local fence
is poisoned and the token is cleared best-effort, so this writer cannot publish
further changes; if another writer takes over, the rotated token rejects the stale
writer the same way.

## Roadmap

We plan on implementing the following features:
- Automatic caching using `foyer`: caching the data stored in `opendal`
  intelligently, so that when reading the same data often, we avoid doing so over
  the network, spilling onto the disk so that the cache can both grow and survive
  restarts.

We *do not* plan on implementing the following features, although contributions
adding those are more than welcome:
- Automatic reloading: our use-case for this crate benefits from making the
  reloading manual – we thus did not implement any automatic reloading logic. This
  could be done using PostgreSQL's `LISTEN` and `NOTIFY`, although if implemented
  like so, it should be made optional, so that users that don't need automatic
  reloading don't pay any cost for it.
