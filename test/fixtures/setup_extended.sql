-- ==========================================================================
-- Extended test schema for comprehensive PostgREST compatibility testing.
-- Loaded AFTER setup.sql (alphabetical order in docker-entrypoint-initdb.d).
-- ==========================================================================

-- ==========================================================================
-- Data types table (for filter/type coercion testing)
-- ==========================================================================

CREATE TABLE api.types_test (
    id           serial PRIMARY KEY,
    text_col     text,
    int_col      integer,
    bigint_col   bigint,
    float_col    real,
    double_col   double precision,
    numeric_col  numeric(10,2),
    bool_col     boolean,
    date_col     date,
    time_col     time,
    ts_col       timestamp,
    tstz_col     timestamptz,
    json_col     json,
    jsonb_col    jsonb,
    int_arr      integer[],
    text_arr     text[],
    inet_col     inet
);

INSERT INTO api.types_test (text_col, int_col, bigint_col, float_col, double_col, numeric_col,
    bool_col, date_col, time_col, ts_col, tstz_col, json_col, jsonb_col,
    int_arr, text_arr, inet_col) VALUES
    ('hello', 1, 100, 1.5, 2.5, 10.50, true, '2024-01-01', '12:00:00',
     '2024-01-01 12:00:00', '2024-01-01 12:00:00+00', '{"a":1}', '{"a":1,"b":[1,2]}',
     '{1,2,3}', '{foo,bar}', '192.168.1.0/24'),
    ('world', 2, 200, 2.5, 3.5, 20.75, false, '2024-06-15', '18:30:00',
     '2024-06-15 18:30:00', '2024-06-15 18:30:00+00', '{"a":2}', '{"a":2,"c":"x"}',
     '{4,5,6}', '{baz,qux}', '10.0.0.0/8'),
    (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
     NULL, NULL, NULL);

-- ==========================================================================
-- Self-referencing table
-- ==========================================================================

CREATE TABLE api.employees (
    id         serial PRIMARY KEY,
    name       text NOT NULL,
    manager_id integer REFERENCES api.employees(id)
);

INSERT INTO api.employees (name, manager_id) VALUES
    ('CEO', NULL),
    ('VP Engineering', 1),
    ('VP Sales', 1),
    ('Dev Lead', 2),
    ('Dev Senior', 4),
    ('Sales Rep', 3);

-- ==========================================================================
-- Composite primary key table
-- ==========================================================================

CREATE TABLE api.compound_pk (
    k1    integer NOT NULL,
    k2    integer NOT NULL,
    value text,
    extra text,
    PRIMARY KEY (k1, k2)
);

INSERT INTO api.compound_pk (k1, k2, value, extra) VALUES
    (1, 1, 'a', 'x'),
    (1, 2, 'b', 'y'),
    (2, 1, 'c', 'z');

-- ==========================================================================
-- Entities table (for array/JSONB filter testing)
-- ==========================================================================

CREATE TABLE api.entities (
    id   serial PRIMARY KEY,
    name text NOT NULL,
    arr  text[],
    data jsonb
);

INSERT INTO api.entities (name, arr, data) VALUES
    ('one',   '{a,b,c}',     '{"x":1,"tags":["alpha","beta"]}'),
    ('two',   '{b,c,d}',     '{"x":2,"tags":["beta","gamma"]}'),
    ('three', '{a,d,e}',     '{"x":3,"tags":["gamma","delta"]}'),
    ('four',  NULL,           NULL);

-- ==========================================================================
-- Table with nullable unique constraint (for upsert on non-PK)
-- ==========================================================================

CREATE TABLE api.upsert_test (
    id    serial PRIMARY KEY,
    code  text UNIQUE NOT NULL,
    value text NOT NULL DEFAULT 'default'
);

INSERT INTO api.upsert_test (code, value) VALUES
    ('AAA', 'first'),
    ('BBB', 'second');

-- ==========================================================================
-- Table for testing various insert/update edge cases
-- ==========================================================================

CREATE TABLE api.items (
    id          serial PRIMARY KEY,
    name        text NOT NULL,
    price       numeric(10,2) DEFAULT 0,
    quantity    integer DEFAULT 0,
    active      boolean DEFAULT true,
    metadata    jsonb DEFAULT '{}',
    created_at  timestamptz DEFAULT now()
);

INSERT INTO api.items (name, price, quantity, active, metadata) VALUES
    ('Widget', 9.99, 100, true, '{"color":"red"}'),
    ('Gadget', 24.99, 50, true, '{"color":"blue"}'),
    ('Gizmo', 4.99, 200, false, '{"color":"green"}');

-- ==========================================================================
-- Second schema for multi-schema tests
-- ==========================================================================

CREATE SCHEMA IF NOT EXISTS api2;
GRANT USAGE ON SCHEMA api2 TO web_anon;
GRANT USAGE ON SCHEMA api2 TO test_user;

CREATE TABLE api2.things (
    id    serial PRIMARY KEY,
    label text NOT NULL
);

INSERT INTO api2.things (label) VALUES ('alpha'), ('beta');

GRANT SELECT ON ALL TABLES IN SCHEMA api2 TO web_anon;
GRANT ALL ON ALL TABLES IN SCHEMA api2 TO test_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA api2 TO test_user;

-- ==========================================================================
-- Additional functions for RPC testing
-- ==========================================================================

-- Returns void
CREATE FUNCTION api.void_func()
    RETURNS void
    LANGUAGE sql VOLATILE
    AS $$ SELECT $$;

-- Returns single text
CREATE FUNCTION api.echo(value text)
    RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $$ SELECT value $$;

-- Returns a record type
CREATE FUNCTION api.get_author(author_id integer)
    RETURNS api.authors
    LANGUAGE sql STABLE
    AS $$ SELECT * FROM api.authors WHERE id = author_id $$;

-- Returns a table type with multiple columns
CREATE FUNCTION api.authors_below(max_id integer)
    RETURNS SETOF api.authors
    LANGUAGE sql STABLE
    AS $$ SELECT * FROM api.authors WHERE id < max_id ORDER BY id $$;

-- Volatile function (only POST)
CREATE FUNCTION api.reset_counter()
    RETURNS integer
    LANGUAGE sql VOLATILE
    AS $$ SELECT 0 $$;

-- Function with multiple default params
CREATE FUNCTION api.multi_defaults(a integer DEFAULT 1, b integer DEFAULT 2, c text DEFAULT 'hello')
    RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $$ SELECT a::text || '-' || b::text || '-' || c $$;

-- Overloaded function (same name, different params)
-- PostgREST picks the best match based on provided params
CREATE FUNCTION api.overloaded(a integer)
    RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $$ SELECT 'int:' || a::text $$;

CREATE FUNCTION api.overloaded(a text)
    RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $$ SELECT 'text:' || a $$;

-- Function returning JSON directly
CREATE FUNCTION api.json_func()
    RETURNS json
    LANGUAGE sql IMMUTABLE
    AS $$ SELECT '{"key":"value"}'::json $$;

-- ==========================================================================
-- Updatable view for mutation testing
-- ==========================================================================

CREATE VIEW api.simple_items AS
    SELECT id, name, price FROM api.items WHERE active = true;

-- Enable mutations on the view
CREATE OR REPLACE FUNCTION api.simple_items_insert()
    RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO api.items (name, price, active) VALUES (NEW.name, NEW.price, true)
    RETURNING id INTO NEW.id;
    RETURN NEW;
END;
$$;

CREATE TRIGGER simple_items_insert_trigger
    INSTEAD OF INSERT ON api.simple_items
    FOR EACH ROW EXECUTE FUNCTION api.simple_items_insert();

-- ==========================================================================
-- Table with full-text search column
-- ==========================================================================

CREATE TABLE api.documents (
    id      serial PRIMARY KEY,
    title   text NOT NULL,
    body    text NOT NULL,
    tsv     tsvector GENERATED ALWAYS AS (to_tsvector('english', title || ' ' || body)) STORED
);

INSERT INTO api.documents (title, body) VALUES
    ('Rust Programming', 'Learn about ownership and borrowing in Rust'),
    ('SQL Guide', 'Understanding joins and indexes in PostgreSQL'),
    ('Web Development', 'Building REST APIs with Rust and PostgreSQL');

-- ==========================================================================
-- Table for testing Range/pagination edge cases
-- ==========================================================================

CREATE TABLE api.numbered (
    id   serial PRIMARY KEY,
    val  integer NOT NULL
);

INSERT INTO api.numbered (val)
SELECT generate_series(1, 100);

-- ==========================================================================
-- Embed disambiguation tables (two FKs from one table to the same target)
-- ==========================================================================

CREATE TABLE api.projects (
    id   serial PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE api.tasks (
    id          serial PRIMARY KEY,
    title       text NOT NULL,
    project_id  integer NOT NULL REFERENCES api.projects(id),
    assigned_to integer REFERENCES api.employees(id),
    created_by  integer REFERENCES api.employees(id)
);

INSERT INTO api.projects (name) VALUES ('Alpha'), ('Beta');

INSERT INTO api.tasks (title, project_id, assigned_to, created_by) VALUES
    ('Design', 1, 4, 1),
    ('Implement', 1, 5, 2),
    ('Test', 2, 5, 3);

-- ==========================================================================
-- Unicode/special character data
-- ==========================================================================

CREATE TABLE api.unicode_test (
    id   serial PRIMARY KEY,
    name text NOT NULL,
    note text
);

INSERT INTO api.unicode_test (name, note) VALUES
    ('café', 'accent'),
    ('naïve', 'diaeresis'),
    ('日本語', 'japanese'),
    ('O''Brien', 'apostrophe'),
    ('line1' || chr(10) || 'line2', 'newline'),
    ('hello "world"', 'quotes');

-- ==========================================================================
-- Additional RPC functions
-- ==========================================================================

-- Function returning TABLE type
CREATE FUNCTION api.get_items_by_price(min_price numeric)
    RETURNS TABLE(id integer, name text, price numeric)
    LANGUAGE sql STABLE
    AS $$ SELECT id, name, price FROM api.items WHERE price >= min_price ORDER BY price $$;

-- Function with array parameter
CREATE FUNCTION api.array_param(ids integer[])
    RETURNS SETOF api.authors
    LANGUAGE sql STABLE
    AS $$ SELECT * FROM api.authors WHERE id = ANY(ids) ORDER BY id $$;

-- Function that returns NULL
CREATE FUNCTION api.null_func()
    RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $$ SELECT NULL::text $$;

-- Function with JSON parameter
CREATE FUNCTION api.json_param(data jsonb)
    RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $$ SELECT data->>'key' $$;

-- Function that raises an error
CREATE FUNCTION api.error_func()
    RETURNS void
    LANGUAGE plpgsql
    AS $$ BEGIN RAISE EXCEPTION 'test error' USING ERRCODE = 'P0001'; END $$;

-- Variadic function
CREATE FUNCTION api.variadic_func(VARIADIC nums integer[])
    RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $$ SELECT sum(x)::integer FROM unnest(nums) AS x $$;

-- ==========================================================================
-- Grant permissions on new tables
-- ==========================================================================

GRANT SELECT ON ALL TABLES IN SCHEMA api TO web_anon;
GRANT ALL ON ALL TABLES IN SCHEMA api TO test_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA api TO test_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA api TO web_anon;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA api TO test_user;
