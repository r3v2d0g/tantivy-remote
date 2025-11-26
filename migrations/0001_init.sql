CREATE SCHEMA tantivy;

CREATE TABLE tantivy.directories (
    index UUID NOT NULL PRIMARY KEY
);

CREATE TABLE tantivy.files (
    index UUID NOT NULL,
    path TEXT NOT NULL,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,

    FOREIGN KEY (index)
    REFERENCES tantivy.directories(index)
    ON DELETE CASCADE,

    PRIMARY KEY (index, path)
);

CREATE TABLE tantivy.metadata (
    index UUID NOT NULL,
    path TEXT NOT NULL,
    content BYTEA NOT NULL,

    FOREIGN KEY (index)
    REFERENCES tantivy.directories(index)
    ON DELETE CASCADE,

    PRIMARY KEY (index, path)
);
