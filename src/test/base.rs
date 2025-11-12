use opendal::{Operator, services::Memory};
use sqlx::PgPool;
use tantivy::{
    DocAddress, Index, IndexSettings, ReloadPolicy, Score, TantivyDocument,
    collector::TopDocs,
    doc,
    query::QueryParser,
    schema::{STORED, SchemaBuilder, TEXT},
};
use tokio::task;
use uuid::uuid;

use crate::RemoteDirectory;

#[tokio::test]
async fn basic() {
    let service = Memory::default();
    let operator = Operator::new(service)
        .expect("failed to create operator")
        .finish();

    let index = uuid!("53af7d56-d3e0-48f9-8663-07a66a7ca5e9");
    let pool = PgPool::connect("postgresql://postgres:postgres@localhost:15432/postgres")
        .await
        .expect("failed to connect to database");

    let cleanup = sqlx::query!(
        r#"
        DELETE
        FROM tantivy.metadata
        WHERE index = $1
        "#,
        index,
    );

    cleanup
        .execute(&pool)
        .await
        .expect("failed to clean up metadata");

    let directory = RemoteDirectory::new(index, operator, pool);

    let mut schema = SchemaBuilder::new();
    let title = schema.add_text_field("title", TEXT | STORED);
    let body = schema.add_text_field("body", TEXT);
    let schema = schema.build();

    let settings = IndexSettings::default();
    let init = task::spawn_blocking(move || {
        let index = Index::create(directory, schema, settings).expect("failed to create index");
        let writer = index
            .writer(100_000_000)
            .expect("failed to create index writer");

        (index, writer)
    });

    let (index, mut writer) = init.await.expect("failed to initialize index and writer");

    let index_ = index.clone();
    let init = task::spawn_blocking(move || {
        let reader = index_
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .expect("failed to create index reader");

        reader
    });

    let reader = init.await.expect("failed to initialize reader");

    let write = task::spawn_blocking(move || {
        writer
            .add_document(doc!(
                title => "The Old Man and the Sea",
                body => "He was an old man who fished alone in a skiff in \
                        the Gulf Stream and he had gone eighty-four days \
                        now without taking a fish."
            ))
            .expect("failed to add document");

        writer.commit().expect("failed to commit");
    });

    write.await.expect("failed to write");

    let parser = QueryParser::for_index(&index, vec![title, body]);
    let query = parser
        .parse_query("sea whale")
        .expect("failed to parse query");

    let reader_ = reader.clone();
    let query_ = query.box_clone();
    let search = task::spawn_blocking(move || {
        reader_
            .searcher()
            .search(&query_, &TopDocs::with_limit(10))
            .expect("failed to search")
    });

    // Searching returns no result because the reader was created before the writer
    // committed.
    let top_docs: Vec<(Score, DocAddress)> = search.await.expect("failed to search");
    assert!(top_docs.is_empty());

    let reader_ = reader.clone();
    let search = task::spawn_blocking(move || {
        reader_.reload().expect("failed to reload reader");
        reader_
            .searcher()
            .search(&query, &TopDocs::with_limit(10))
            .expect("failed to search")
    });

    let top_docs: Vec<(Score, DocAddress)> = search.await.expect("failed to search");
    assert_eq!(top_docs.len(), 1);

    for (_score, addr) in top_docs {
        let _doc: TantivyDocument = reader
            .searcher()
            .doc_async(addr)
            .await
            .expect("failed to read document");
    }
}
