DROP EXTENSION IF EXISTS vectors;
CREATE EXTENSION vectors;

CREATE TABLE authors (
        id SERIAL PRIMARY KEY,
        url TEXT NOT NULL UNIQUE,
        username TEXT NOT NULL
);

CREATE TABLE posts (
       id UUID PRIMARY KEY,
       content TEXT NOT NULL,
       post_url TEXT NOT NULL,
       tags JSONB NOT NULL,
       author_id INT NOT NULL,
       indexed_at TIMESTAMPTZ DEFAULT NOW(),
       content_ts tsvector GENERATED ALWAYS AS (to_tsvector('english', content)) STORED,
       FOREIGN KEY (author_id) REFERENCES authors(id)
);

CREATE TABLE attachments (
     id SERIAL PRIMARY KEY,
     post_id UUID NOT NULL,
     description TEXT,
     description_ts tsvector GENERATED ALWAYS AS (to_tsvector('english', description)) STORED,
     url TEXT NOT NULL,
     indexed_at TIMESTAMPTZ DEFAULT NOW(),
     FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
);

CREATE INDEX posts_author_id_idx ON posts(author_id);
CREATE INDEX post_created_at_idx ON posts(indexed_at);
CREATE INDEX post_id_idx ON posts(post);
CREATE INDEX posts_tags_idx ON posts USING GIN(description_ts);
CREATE INDEX posts_text_idx ON posts USING GIN(content_ts);

ALTER TABLE posts ADD COLUMN IF NOT EXISTS meilisearch_indexed BOOLEAN DEFAULT FALSE;

