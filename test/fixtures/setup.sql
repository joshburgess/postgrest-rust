-- ==========================================================================
-- Test database schema for postgrest-rust integration tests.
-- Mirrors PostgREST conventions: authenticator role, anon role, api schema.
-- ==========================================================================

-- Roles
CREATE ROLE authenticator LOGIN PASSWORD 'authenticator';
CREATE ROLE web_anon NOLOGIN;
CREATE ROLE test_user NOLOGIN;
GRANT web_anon TO authenticator;
GRANT test_user TO authenticator;

-- API schema
CREATE SCHEMA api;
GRANT USAGE ON SCHEMA api TO web_anon;
GRANT USAGE ON SCHEMA api TO test_user;

-- ==========================================================================
-- Tables
-- ==========================================================================

CREATE TABLE api.authors (
    id   serial PRIMARY KEY,
    name text   NOT NULL,
    bio  text
);
COMMENT ON TABLE api.authors IS 'Book authors';
COMMENT ON COLUMN api.authors.name IS 'Full name of the author';

CREATE TABLE api.books (
    id        serial  PRIMARY KEY,
    title     text    NOT NULL,
    author_id integer NOT NULL REFERENCES api.authors(id),
    pages     integer,
    published date
);

-- Many-to-many via a join table
CREATE TABLE api.tags (
    id   serial PRIMARY KEY,
    name text   NOT NULL UNIQUE
);

CREATE TABLE api.book_tags (
    book_id integer NOT NULL REFERENCES api.books(id),
    tag_id  integer NOT NULL REFERENCES api.tags(id),
    PRIMARY KEY (book_id, tag_id)
);

-- Enum type
CREATE TYPE api.status AS ENUM ('draft', 'published', 'archived');

CREATE TABLE api.articles (
    id      serial     PRIMARY KEY,
    title   text       NOT NULL,
    body    text,
    status  api.status NOT NULL DEFAULT 'draft',
    created timestamptz NOT NULL DEFAULT now()
);

-- Table with composite unique (for upsert tests)
CREATE TABLE api.settings (
    key   text NOT NULL,
    value text NOT NULL,
    PRIMARY KEY (key)
);

-- ==========================================================================
-- Views
-- ==========================================================================

CREATE VIEW api.authors_with_books AS
    SELECT a.id, a.name, count(b.id) AS book_count
    FROM api.authors a
    LEFT JOIN api.books b ON b.author_id = a.id
    GROUP BY a.id, a.name;

-- ==========================================================================
-- Functions
-- ==========================================================================

CREATE FUNCTION api.add(a integer, b integer)
    RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $$ SELECT a + b $$;

CREATE FUNCTION api.search_books(query text)
    RETURNS SETOF api.books
    LANGUAGE sql STABLE
    AS $$ SELECT * FROM api.books WHERE title ILIKE '%' || query || '%' $$;

CREATE FUNCTION api.greet(name text DEFAULT 'world')
    RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $$ SELECT 'Hello, ' || name || '!' $$;

-- ==========================================================================
-- Seed data
-- ==========================================================================

INSERT INTO api.authors (name, bio) VALUES
    ('Alice', 'Writes about Rust'),
    ('Bob', 'Writes about PostgreSQL'),
    ('Carol', NULL);

INSERT INTO api.books (title, author_id, pages, published) VALUES
    ('Learning Rust',      1, 350, '2024-01-15'),
    ('Advanced Rust',      1, 500, '2024-06-01'),
    ('PostgreSQL Basics',  2, 280, '2023-09-10'),
    ('SQL Deep Dive',      2, 420, NULL);

INSERT INTO api.tags (name) VALUES ('programming'), ('database'), ('beginner');

INSERT INTO api.book_tags (book_id, tag_id) VALUES
    (1, 1), (1, 3),   -- Learning Rust: programming, beginner
    (2, 1),            -- Advanced Rust: programming
    (3, 2), (3, 3),    -- PostgreSQL Basics: database, beginner
    (4, 2);            -- SQL Deep Dive: database

INSERT INTO api.articles (title, body, status) VALUES
    ('Hello World', 'First article', 'published'),
    ('Draft Post', 'Work in progress', 'draft');

INSERT INTO api.settings (key, value) VALUES
    ('site_name', 'Test Site'),
    ('theme', 'dark');

-- ==========================================================================
-- Permissions
-- ==========================================================================

GRANT SELECT ON ALL TABLES IN SCHEMA api TO web_anon;
GRANT ALL    ON ALL TABLES IN SCHEMA api TO test_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA api TO test_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA api TO web_anon;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA api TO test_user;

-- ==========================================================================
-- Row-level security (on articles)
-- ==========================================================================

ALTER TABLE api.articles ENABLE ROW LEVEL SECURITY;

-- web_anon can only see published articles
CREATE POLICY articles_anon ON api.articles
    FOR SELECT TO web_anon
    USING (status = 'published');

-- test_user can see and modify all articles
CREATE POLICY articles_user ON api.articles
    FOR ALL TO test_user
    USING (true)
    WITH CHECK (true);
