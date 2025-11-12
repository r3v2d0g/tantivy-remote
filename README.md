# `tantivy-remote`

An implementation of `tantivy`'s `Directory` that uses `opendal` and `sqlx`, the
former for the data, and the latter for the metadata.

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
- Locking: similarly, our use-case for this crate guarantees that there cannot be
  more than one index writer at the same time – we thus did not implement any
  directory logic. This could be done using a PostgreSQL and a background `tokio`
  task updating some `last_alive_at` value, or using Redis. Similarly to automatic
  reloading, this should be made optional, so that users that can guarantee that
  there won't be more than one index writer using the same directory at any point
  in time don't have to pay any extra cost.
