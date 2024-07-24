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
       text_embedding vector(1024),
       indexed_at TIMESTAMPTZ DEFAULT NOW(),
       FOREIGN KEY (author_id) REFERENCES authors(id)
);

CREATE TABLE attachments (
     id SERIAL PRIMARY KEY,
     post_id UUID NOT NULL,
     description TEXT,
     url TEXT NOT NULL,
     vector vector(1024),
     indexed_at TIMESTAMPTZ DEFAULT NOW(),
     FOREIGN KEY (post_id) REFERENCES posts(id)
);

CREATE INDEX posts_text_embedding_idx ON posts USING vectors(text_embedding vector_cos_ops);
CREATE INDEX attachments_vector_idx ON attachments USING vectors(vector vector_cos_ops);

CREATE INDEX posts_author_id_idx ON posts(author_id);
CREATE INDEX posts_tags_idx ON posts USING GIN(tags);
CREATE INDEX posts_text_idx ON posts USING GIN(to_tsvector('english', content));
