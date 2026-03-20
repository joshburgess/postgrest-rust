# Building a PostgREST Alternative in Rust

## Design Document — March 2026

---

## 1. Introduction

This document outlines a complete plan for building a PostgREST-like automatic REST API server in Rust, with an eventual path toward an ActiveAdmin-style admin dashboard. The project leverages the mature Rust ecosystem of async runtimes, web frameworks, database drivers, and templating engines to create a high-performance, zero-boilerplate REST layer for any PostgreSQL database.

PostgREST is a standalone Haskell server that introspects a PostgreSQL schema and auto-generates a full REST API — no application code required. Tables become endpoints, foreign keys become embeddable relationships, PostgreSQL roles become the authorization layer, and JWT tokens drive authentication. The goal is to replicate this architecture in Rust, gaining the performance, safety, and ecosystem advantages Rust provides, while designing from the start for extensibility toward an admin UI.

---

## 2. PostgREST Architecture Recap

Before diving into the Rust plan, here is a summary of what PostgREST does and how it works internally. Understanding this is essential for knowing what we need to build.

### 2.1 Core pipeline

Every HTTP request flows through a clean pipeline:

1. **Warp HTTP server** — Receives the request. PostgREST uses Haskell's Warp library, a compiled server with lightweight green threads.
2. **Auth.hs** — Decodes JWT from the `Authorization: Bearer` header, extracts the `role` claim.
3. **ApiRequest.hs** — Parses the URL path (target table/view/function), query string (PostgREST filter syntax), headers, and body.
4. **Plan.hs** — Transforms the parsed request into an internal AST using the schema cache. Fills in SQL details like `ON CONFLICT` clauses and JOIN conditions.
5. **Query.hs** — Generates parameterized, prepared SQL from the AST.
6. **Hasql** — Executes the SQL against PostgreSQL via connection pool using the binary protocol.
7. **PostgreSQL** — Runs the query under the switched role (`SET LOCAL ROLE`), applying row-level security and GRANT permissions.

### 2.2 Schema cache

The heart of PostgREST. At startup, it queries PostgreSQL system catalogs to build an in-memory map of: all tables and views in exposed schemas, every column with its type, all foreign key relationships (powering resource embedding), callable functions, and primary keys/unique constraints (for upserts). The cache reloads without downtime via UNIX signals or PostgreSQL `NOTIFY`, and can auto-reload via DDL event triggers.

### 2.3 Authentication and authorization

PostgREST uses JWT tokens where a `role` claim maps to a PostgreSQL role. For each request, it executes `SET LOCAL ROLE <role>`, delegating all authorization to PostgreSQL's native GRANT permissions and row-level security policies. JWT claims are accessible in SQL via `current_setting('request.jwt.claims', true)`.

### 2.4 Resource embedding

Foreign key relationships in the schema cache enable automatic resource embedding — fetching related data in a single request via generated `LATERAL JOIN` queries. This is one of PostgREST's most powerful features and a key differentiator from simple CRUD generators.

### 2.5 Performance approach

Three factors drive PostgREST's speed: a compiled language with lightweight concurrency (Haskell/Warp), delegation of computation to the database (JSON serialization in SQL, authorization via RLS), and an efficient database driver (Hasql with binary protocol and connection pooling).

---

## 3. Why Rust

Rust is an excellent fit for this kind of project for several reasons:

- **Performance** — Rust compiles to native code with zero-cost abstractions and no garbage collector. The async runtime (Tokio) provides lightweight task scheduling comparable to Haskell's green threads, and web frameworks built on it (Axum, Hyper) consistently benchmark among the fastest HTTP servers in any language.
- **Memory safety without GC** — Rust's ownership model eliminates entire classes of bugs (use-after-free, data races) at compile time, which is critical for a long-running server handling concurrent database connections.
- **Ecosystem maturity** — As of 2026, the Rust web and database ecosystem is production-ready. Axum (v0.8.x) is the dominant web framework, SQLx (v0.8.x) and tokio-postgres are battle-tested async PostgreSQL drivers, and the Tower middleware ecosystem provides composable layers for auth, tracing, compression, and more.
- **Single binary deployment** — Rust compiles to a single static binary with no runtime dependencies (beyond libc or musl), making deployment trivial — identical to PostgREST's single-binary model.
- **Compile-time SQL checking** — SQLx provides compile-time verification of SQL queries against a real database, catching type mismatches and syntax errors before deployment.

---

## 4. Rust Ecosystem — Chosen Libraries

### 4.1 Async runtime: Tokio

Tokio is the standard async runtime for Rust, providing a multi-threaded task scheduler, async I/O, timers, and channels. Every library in our stack is built on Tokio.

### 4.2 Web framework: Axum

Axum is built by the Tokio team and is a thin layer on top of Hyper (HTTP) and Tower (middleware). It uses a macro-free API with extractors for parsing requests and Tower middleware for cross-cutting concerns. Key advantages for this project:

- **Router-centric design** — Routes map URL patterns to async handler functions. Dynamic route registration (which we need for auto-generated table endpoints) is straightforward.
- **Tower middleware compatibility** — JWT auth, tracing, compression, rate limiting, and CORS are all available as Tower layers, composable and reusable.
- **State management** — Application state (database pool, schema cache) is injected via `with_state()` and extracted in handlers.
- **Performance** — Comparable to Hyper directly, with minimal overhead.

### 4.3 Database driver: tokio-postgres (primary) + SQLx (secondary)

For the dynamic query execution path (API-generated SQL), **tokio-postgres** is the best fit:

- Lowest overhead of any Rust PostgreSQL driver — pure Rust, async-native, minimal abstraction.
- Supports the binary protocol for efficient data transfer.
- Accepts raw SQL strings with bind parameters — exactly what we need for dynamically generated queries.
- Supports `LISTEN/NOTIFY` for schema cache reloading.
- Built-in connection pooling via `deadpool-postgres` or `bb8-postgres`.

For the fixed query path (schema introspection queries), **SQLx** provides compile-time SQL checking:

- The `sqlx::query!()` macro verifies SQL against a real database at compile time.
- Catches type mismatches and column name errors before deployment.
- Perfect for the schema cache queries which are fixed SQL.
- Can be used alongside tokio-postgres in the same project.

Note: SQLx has had reported performance issues compared to tokio-postgres in some benchmarks. Since the hot path (API query execution) will use tokio-postgres directly, this is not a concern — SQLx is only used for the cold path (schema introspection at startup/reload).

### 4.4 Connection pooling: deadpool-postgres

A lightweight, async-aware connection pool for tokio-postgres. Provides configurable pool sizes, connection timeouts, and health checks. Alternative: `bb8-postgres`.

### 4.5 JWT: jsonwebtoken

The `jsonwebtoken` crate is the standard Rust JWT library. It supports HMAC, RSA, ECDSA, and EdDSA algorithms, with customizable validation (expiry, audience, issuer). For middleware integration, `tower-jwt` wraps `jsonwebtoken` in a Tower layer that automatically extracts and validates JWTs from the Authorization header and stores claims in the request extension.

### 4.6 Serialization: serde + serde_json

The universal Rust serialization framework. However, following PostgREST's design, we will serialize JSON **in PostgreSQL** (using `to_jsonb`, `json_agg`) and pass the resulting bytes directly to the HTTP response — avoiding serde deserialization/reserialization overhead on the hot path.

### 4.7 Configuration: config + clap

The `config` crate supports TOML/YAML/JSON config files with environment variable overrides. `clap` handles CLI argument parsing. Together they replicate PostgREST's config file + CLI model.

### 4.8 Templating (for admin UI): Askama

Askama compiles Jinja-like templates to Rust code at compile time, giving excellent performance and catching template errors before deployment. Combined with HTMX for interactivity, this provides a lightweight, JS-free admin UI stack. Alternative: Maud (Rust macros for HTML, no separate template files).

### 4.9 Observability: tracing + tower-http

The `tracing` crate provides structured, async-aware logging. `tower-http::trace::TraceLayer` automatically instruments every request with timing, status codes, and span context.

---

## 5. Proposed Architecture

### 5.1 Crate/layer overview

The project is organized as a Cargo workspace with four crates, where the bottom two are shared infrastructure consumed by both the REST API and the future admin UI.

```
┌─────────────────────────┐  ┌──────────────────────────┐
│   pg-rest-server (bin)  │  │   pg-admin-ui (bin)      │
│   Axum + JWT + routing  │  │   Axum + Askama + HTMX   │
└───────────┬─────────────┘  └───────────┬──────────────┘
            │                            │
            └─────────────┬──────────────┘
                          │
            ┌─────────────▼──────────────┐
            │   pg-query-engine (lib)    │
            │   AST → SQL builder        │
            │   Filters, embedding,      │
            │   ordering, pagination     │
            └─────────────┬──────────────┘
                          │
            ┌─────────────▼──────────────┐
            │   pg-schema-cache (lib)    │
            │   Catalog introspection    │
            │   FK graph, NOTIFY reload  │
            └─────────────┬──────────────┘
                          │
            ┌─────────────▼──────────────┐
            │   tokio-postgres /         │
            │   deadpool-postgres        │
            │   (external crates)        │
            └────────────────────────────┘
```

### 5.2 Crate: pg-schema-cache

A standalone library crate that introspects a PostgreSQL database and builds an in-memory representation of its schema.

#### 5.2.1 Core types

```rust
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct SchemaCache {
    pub tables: HashMap<QualifiedName, Table>,
    pub relationships: Vec<Relationship>,
    pub functions: HashMap<QualifiedName, Function>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct QualifiedName {
    pub schema: String,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: QualifiedName,
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
    pub is_view: bool,
    pub comment: Option<String>,
    pub insertable: bool,
    pub updatable: bool,
    pub deletable: bool,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub pg_type: String,
    pub nullable: bool,
    pub has_default: bool,
    pub default_expr: Option<String>,
    pub max_length: Option<i32>,
    pub is_pk: bool,
    pub comment: Option<String>,
    pub enum_values: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub enum RelType { OneToMany, ManyToOne, ManyToMany }

#[derive(Debug, Clone)]
pub struct Relationship {
    pub from_table: QualifiedName,
    pub to_table: QualifiedName,
    pub columns: Vec<(String, String)>,
    pub rel_type: RelType,
    pub join_table: Option<QualifiedName>,
    pub constraint_name: String,
}

#[derive(Debug, Clone)]
pub struct Function {
    pub name: QualifiedName,
    pub params: Vec<FuncParam>,
    pub return_type: ReturnType,
    pub volatility: Volatility,
    pub comment: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FuncParam {
    pub name: String,
    pub pg_type: String,
    pub has_default: bool,
}

#[derive(Debug, Clone)]
pub enum ReturnType { Scalar(String), SetOf(String), Table(Vec<Column>), Void }

#[derive(Debug, Clone)]
pub enum Volatility { Immutable, Stable, Volatile }
```

#### 5.2.2 Introspection queries

These query PostgreSQL system catalogs. Using SQLx's `query!` macro, they are compile-time checked:

- **Tables and views** — `SELECT * FROM information_schema.tables WHERE table_schema = ANY($1)`
- **Columns** — `SELECT * FROM information_schema.columns WHERE table_schema = ANY($1) ORDER BY ordinal_position`
- **Primary keys** — `SELECT ... FROM pg_constraint c JOIN pg_attribute a ON ... WHERE c.contype = 'p'`
- **Foreign keys** — `SELECT ... FROM pg_constraint c JOIN pg_attribute a1 ON ... JOIN pg_attribute a2 ON ... WHERE c.contype = 'f'` — this is the most critical query, powering resource embedding.
- **Functions** — `SELECT ... FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = ANY($1)`
- **Enum values** — `SELECT ... FROM pg_enum e JOIN pg_type t ON e.enumtypid = t.oid`
- **Comments** — `SELECT ... FROM pg_description` joined with `pg_class` and `pg_attribute`

#### 5.2.3 M2M inference

Many-to-many relationships are derived in Rust from the FK graph: if a table has exactly two foreign keys to two other tables and no other columns (besides a possible composite PK), it is inferred as a join table establishing a M2M relationship.

#### 5.2.4 Cache reloading

```rust
use tokio::sync::watch;
use tokio_postgres::AsyncMessage;

pub async fn schema_listener(
    conn: tokio_postgres::Client,
    tx: watch::Sender<SchemaCache>,
    config: SchemaCacheConfig,
) {
    conn.execute("LISTEN pgrst", &[]).await.unwrap();

    let mut stream = conn.notifications();
    while let Some(notification) = stream.next().await {
        if notification.channel() == "pgrst" {
            match build_schema_cache(&conn, &config).await {
                Ok(new_cache) => { tx.send(new_cache).ok(); }
                Err(e) => tracing::error!("Schema reload failed: {}", e),
            }
        }
    }
}
```

Using `tokio::sync::watch`, all request handlers hold a `watch::Receiver<SchemaCache>` and always see the latest cache without locking.

### 5.3 Crate: pg-query-engine

A standalone library crate that transforms parsed API requests into parameterized SQL.

#### 5.3.1 Request AST

```rust
#[derive(Debug, Clone)]
pub enum ApiRequest {
    Read(ReadRequest),
    Insert(InsertRequest),
    Update(UpdateRequest),
    Delete(DeleteRequest),
    CallFunction(FunctionCall),
}

#[derive(Debug, Clone)]
pub struct ReadRequest {
    pub table: QualifiedName,
    pub select: Vec<SelectItem>,
    pub filters: Vec<Filter>,
    pub order: Vec<OrderClause>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub count: CountOption,
}

#[derive(Debug, Clone)]
pub enum SelectItem {
    Column(String),
    Star,
    Embed {
        alias: Option<String>,
        target: String,
        sub_request: Box<ReadRequest>,
    },
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub column: String,
    pub operator: FilterOp,
    pub value: FilterValue,
}

#[derive(Debug, Clone)]
pub enum FilterOp {
    Eq, Neq, Gt, Gte, Lt, Lte,
    Like, Ilike, In, Is,
    Contains, ContainedIn, Overlaps,
    Fts, Plfts, Phfts, Wfts,
}

#[derive(Debug, Clone)]
pub struct InsertRequest {
    pub table: QualifiedName,
    pub rows: Vec<serde_json::Map<String, serde_json::Value>>,
    pub on_conflict: Option<ConflictAction>,
    pub returning: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct UpdateRequest {
    pub table: QualifiedName,
    pub set: serde_json::Map<String, serde_json::Value>,
    pub filters: Vec<Filter>,
    pub returning: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DeleteRequest {
    pub table: QualifiedName,
    pub filters: Vec<Filter>,
    pub returning: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct FunctionCall {
    pub function: QualifiedName,
    pub params: serde_json::Map<String, serde_json::Value>,
    pub is_scalar: bool,
    pub read_request: Option<ReadRequest>,
}
```

#### 5.3.2 SQL builder

The SQL builder takes an `ApiRequest` + `SchemaCache` and produces a SQL string with bind parameters:

```rust
pub struct SqlOutput {
    pub sql: String,
    pub params: Vec<Box<dyn tokio_postgres::types::ToSql + Send + Sync>>,
}

pub fn build_sql(
    cache: &SchemaCache,
    request: &ApiRequest,
) -> Result<SqlOutput, PlanError> {
    // ...
}
```

Key SQL generation strategies:

- **JSON in SQL** — Wrap results in `SELECT coalesce(json_agg(t), '[]')::text FROM (...) t` so PostgreSQL returns a JSON byte string that is passed directly to the HTTP response body.
- **LATERAL JOIN embedding** — For each `SelectItem::Embed`, generate `LEFT JOIN LATERAL (SELECT to_jsonb(sub) FROM target_table sub WHERE sub.fk_col = parent.pk_col) alias ON true`.
- **Parameterized values** — All user-supplied filter values become `$N` bind parameters. Column and table names are sanitized with double-quote escaping (not parameterizable in PostgreSQL).
- **Identifier quoting** — All table and column identifiers are wrapped in `"double_quotes"` to handle reserved words and mixed case.

#### 5.3.3 URL syntax parser

A parsing module converts PostgREST-compatible URL query strings into AST types:

| URL syntax | Parsed to |
|---|---|
| `?select=id,name` | `vec![Column("id"), Column("name")]` |
| `?select=*,author:authors(name)` | `vec![Star, Embed { alias: "author", target: "authors", sub: ... }]` |
| `?age=eq.25` | `Filter { column: "age", op: Eq, value: Int(25) }` |
| `?age=gt.18` | `Filter { column: "age", op: Gt, value: Int(18) }` |
| `?name=like.*smith*` | `Filter { column: "name", op: Like, value: Str("%smith%") }` |
| `?id=in.(1,2,3)` | `Filter { column: "id", op: In, value: List([1,2,3]) }` |
| `?order=name.asc,age.desc` | `vec![Order("name", Asc), Order("age", Desc)]` |
| `Range: 0-24` header | `limit: 25, offset: 0` |
| `Prefer: count=exact` | `count: Exact` |

### 5.4 Crate: pg-rest-server (binary)

The top-level application that wires everything together into an Axum server.

#### 5.4.1 Cargo.toml dependencies

```toml
[dependencies]
# Async runtime
tokio = { version = "1", features = ["full"] }

# Web framework + middleware
axum = "0.8"
tower = "0.5"
tower-http = { version = "0.6", features = ["trace", "cors", "compression-gzip"] }
tower-jwt = "0.3"

# Database
tokio-postgres = { version = "0.7", features = ["with-serde_json-1", "with-chrono-0_4"] }
deadpool-postgres = "0.14"

# Compile-time checked queries (for schema introspection)
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres"] }

# JWT
jsonwebtoken = { version = "10", features = ["aws_lc_rs"] }

# Serialization
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# Config
config = "0.14"
clap = { version = "4", features = ["derive"] }

# Observability
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

# Internal crates
pg-schema-cache = { path = "../pg-schema-cache" }
pg-query-engine = { path = "../pg-query-engine" }
```

#### 5.4.2 Application state

```rust
use std::sync::Arc;
use tokio::sync::watch;

pub struct AppState {
    pub pool: deadpool_postgres::Pool,
    pub schema_cache: watch::Receiver<SchemaCache>,
    pub config: AppConfig,
    pub jwt_decoding_key: jsonwebtoken::DecodingKey,
    pub jwt_validation: jsonwebtoken::Validation,
}
```

#### 5.4.3 Dynamic route registration

Unlike typical Axum apps with static routes, this server needs to handle any table name in the URL. This is done with a catch-all route:

```rust
let app = Router::new()
    .route("/{table}", get(handle_read).post(handle_insert)
                       .patch(handle_update).delete(handle_delete))
    .route("/rpc/{function}", post(handle_rpc))
    .route("/", get(handle_root_openapi))
    .layer(JwtLayer::new(decoding_key, validation))
    .layer(TraceLayer::new_for_http())
    .layer(CorsLayer::permissive())
    .with_state(Arc::new(state));
```

The `{table}` path parameter is extracted and validated against the schema cache in the handler.

#### 5.4.4 Request lifecycle

```rust
async fn handle_read(
    State(state): State<Arc<AppState>>,
    Path(table): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    Extension(claims): Extension<Option<JwtClaims>>,
) -> Result<Response, ApiError> {
    // 1. Get current schema cache
    let cache = state.schema_cache.borrow().clone();

    // 2. Resolve table
    let qualified = resolve_table(&cache, &state.config.db_schemas, &table)?;

    // 3. Parse query string into ReadRequest
    let read_req = parse_read_request(qualified, &params, &headers)?;

    // 4. Build SQL
    let sql_output = pg_query_engine::build_sql(&cache, &ApiRequest::Read(read_req))?;

    // 5. Execute with role switch
    let conn = state.pool.get().await?;
    let role = claims.map(|c| c.role).unwrap_or(&state.config.db_anon_role);

    conn.execute(
        "SELECT set_config('role', $1, true)",
        &[&role],
    ).await?;

    if let Some(claims_json) = claims.map(|c| &c.raw) {
        conn.execute(
            "SELECT set_config('request.jwt.claims', $1, true)",
            &[claims_json],
        ).await?;
    }

    let rows = conn.query(&sql_output.sql, &sql_output.params).await?;

    // 6. Return JSON response (already serialized by PostgreSQL)
    let json_bytes: &str = rows[0].get(0);
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        json_bytes.to_string(),
    ).into_response())
}
```

#### 5.4.5 Configuration

```toml
# pg-rest.toml
[database]
uri = "postgres://authenticator:pass@localhost:5432/mydb"
schemas = ["api"]
anon_role = "web_anon"
pool_size = 10
prepared_statements = true

[server]
port = 3000
host = "0.0.0.0"

[jwt]
secret = "your-32-char-minimum-secret-here"
# Or for asymmetric:
# secret_file = "@/path/to/public_key.pem"
# algorithm = "RS256"

[admin]
# Future: admin UI settings
enabled = false
port = 3001
```

#### 5.4.6 OpenAPI generation

The schema cache contains everything needed to auto-generate an OpenAPI 3.0 spec. At startup (and on schema reload), the server generates a spec describing every endpoint with: path parameters, query parameter filters, request body schemas (derived from column types), and response schemas. This is served at `GET /` with `Accept: application/openapi+json`.

### 5.5 Crate: pg-admin-ui (binary, future)

An ActiveAdmin-style admin dashboard that consumes the same schema cache and query engine.

#### 5.5.1 ActiveAdmin features to replicate

ActiveAdmin provides: auto-generated CRUD interfaces (index/show/new/edit) for every resource, customizable sidebar navigation, scoped filters, multiple index view styles (table/grid/block), export (JSON/CSV/XML), authentication (Devise), authorization (CanCan/Pundit), customizable dashboards, batch actions, and inline editing.

#### 5.5.2 Recommended stack: Axum + Askama + HTMX + Tailwind

This is the "RAAHT stack" that has gained significant traction in the Rust community:

- **Axum** — Same web framework as the REST server, sharing connection pools and middleware.
- **Askama** — Compile-time Jinja-like templates. Templates are checked at compile time and compiled to fast Rust code. Supports template inheritance (base layouts) and includes.
- **HTMX** — A 14kB JavaScript library that enables interactive behaviors (inline editing, dynamic filtering, pagination without full page reloads) by making server requests and swapping HTML fragments. No JavaScript framework required.
- **Tailwind CSS** — Utility-first CSS for consistent, responsive styling.

This combination means the admin UI is server-rendered HTML with sprinkles of interactivity — minimal JavaScript, maximum leverage of the Rust backend. It mirrors how Ruby on Rails + Turbo/Hotwire works, which is conceptually close to ActiveAdmin.

#### 5.5.3 Auto-generated views from the schema cache

| Schema cache data | Admin UI usage |
|---|---|
| Table list | Navigation sidebar entries |
| Column names + types | Table headers; form field types (text input, number, date picker, boolean toggle, enum dropdown, FK select) |
| Primary keys | Record identification, URL routing (`/admin/users/42`) |
| Foreign keys | Belongs-to dropdowns, has-many linked lists, association filters |
| Column nullability | Required vs. optional form field markers |
| Column defaults | Pre-filled values in new record forms |
| Comments (`COMMENT ON`) | Field labels and help text |
| Enum values | Dropdown option lists |
| Functions | Custom action buttons |
| Row counts (`pg_stat_user_tables`) | Badge counts in the navigation sidebar |

#### 5.5.4 Page types

- **Index page** — Paginated, sortable table with column headers derived from the schema. Sidebar filters for each column type. HTMX-powered pagination (no full page reload). Export buttons for CSV/JSON.
- **Show page** — Detail view of a single record. Related records displayed via the FK graph. Navigation links to related tables.
- **New/Edit page** — Form with field types derived from column types. Validation from NOT NULL constraints. FK fields render as searchable dropdowns.
- **Dashboard** — Configurable landing page with widgets: recent records, aggregate counts, charts.

#### 5.5.5 Authentication for admin UI

Two options:

- **Same JWT system** — Admin users authenticate via JWT, same as the REST API. A login page generates tokens.
- **Session-based** — For the admin UI specifically, cookie-based sessions may be more ergonomic. The `tower-sessions` crate provides session middleware for Axum with PostgreSQL-backed session storage.

Both approaches delegate authorization to PostgreSQL roles and RLS, maintaining the single-source-of-truth philosophy.

---

## 6. Development Roadmap

### Phase 1 — pg-schema-cache (library crate)

**Goal:** Standalone, well-tested crate that introspects PostgreSQL and produces an in-memory schema representation.

**Tasks:**
- Define core types (`SchemaCache`, `Table`, `Column`, `Relationship`, `Function`).
- Write catalog introspection queries with SQLx compile-time checking.
- Implement FK graph construction and M2M inference.
- Wire `LISTEN/NOTIFY` with `tokio::sync::watch` for non-blocking cache distribution.
- Test against schemas with: simple tables, views, composite FKs, self-referencing tables, M2M join tables, functions with various return types, enums.

**Deliverable:** `pg-schema-cache` crate published to crates.io or internal registry.

### Phase 2 — pg-query-engine (library crate)

**Goal:** Standalone crate that transforms request ASTs into parameterized SQL.

**Tasks:**
- Define request AST types (`ReadRequest`, `InsertRequest`, etc.).
- Implement PostgREST-compatible URL syntax parser.
- Implement SELECT generation: column selection, filtering (all operators), ordering, pagination.
- Implement resource embedding via LATERAL JOINs (O2M, M2O, M2M).
- Implement INSERT/UPDATE/DELETE generation with returning clauses.
- Implement upsert (`ON CONFLICT`) generation.
- Implement function call SQL generation.
- JSON wrapping (`json_agg`, `to_jsonb`) for database-side serialization.
- Property-based testing: generate random schemas + requests, verify SQL parses and executes correctly.

**Deliverable:** `pg-query-engine` crate.

### Phase 3 — pg-rest-server (binary crate)

**Goal:** A working PostgREST alternative that serves a REST API from any PostgreSQL database.

**Tasks:**
- Axum server setup with TOML config file and CLI parsing.
- Dynamic route registration with catch-all table/function routes.
- JWT authentication middleware with role extraction via `tower-jwt`.
- Per-request role switching (`SET LOCAL ROLE`) in transaction scope.
- JWT claims forwarding as PostgreSQL GUC variables.
- Content negotiation: JSON (default), CSV (via `Accept: text/csv`).
- `Prefer` header handling: `count=exact`, `return=representation`, `resolution=merge-duplicates`.
- Range header support for pagination with `Content-Range` response.
- OpenAPI 3.0 spec generation from schema cache.
- Admin health endpoints: `/live` and `/ready`.
- Graceful shutdown handling.
- Docker image (Alpine-based, single binary).
- Integration test suite against a real PostgreSQL instance.

**Deliverable:** `pg-rest-server` binary, Docker image, documentation.

### Phase 4 — pg-admin-ui (binary crate)

**Goal:** An ActiveAdmin-style web interface auto-generated from the database schema.

**Tasks:**
- Askama template hierarchy: base layout → page templates → partial fragments.
- HTMX integration for dynamic interactions.
- Tailwind CSS for styling.
- Auto-generated index pages with sortable columns, pagination, sidebar filters.
- Auto-generated show pages with related record navigation.
- Auto-generated new/edit forms with type-appropriate widgets.
- CSV/JSON export from index pages.
- Session-based authentication with PostgreSQL session store.
- Dashboard with configurable widgets.
- Batch actions (select rows → perform operation).
- Customization system: TOML config or Rust trait overrides for per-resource view customization.

**Deliverable:** `pg-admin-ui` binary.

### Phase 5 — Polish and production readiness

**Tasks:**
- Performance benchmarking against PostgREST (wrk, k6).
- Connection pool tuning documentation.
- PgBouncer compatibility (disable prepared statements mode).
- Nginx reverse proxy configuration guide.
- TLS termination documentation.
- Rate limiting via Tower middleware.
- Structured JSON logging.
- Prometheus metrics endpoint.
- Comprehensive error messages with PostgREST-compatible error codes.
- CI/CD pipeline with automated testing against multiple PostgreSQL versions.

---

## 7. Key Design Decisions

### 7.1 JSON serialization in PostgreSQL

**Decision: Yes.** Following PostgREST's design, the SQL builder wraps all read queries so PostgreSQL returns JSON directly. The Rust server passes the raw bytes to the HTTP response without deserialization. This eliminates a major performance bottleneck and keeps the server's role minimal — it's a router and query generator, not a data transformer.

### 7.2 tokio-postgres over SQLx for the hot path

**Decision: Use tokio-postgres for API query execution, SQLx for schema introspection.** tokio-postgres provides the lowest overhead for executing dynamically-built SQL strings with bind parameters. SQLx's compile-time checking is valuable for the fixed introspection queries but adds unnecessary abstraction for dynamic queries. This dual-driver approach gives us the best of both worlds.

### 7.3 Watch channel for schema cache

**Decision: `tokio::sync::watch`.** A watch channel allows one writer (the schema listener) and many readers (request handlers). Readers always see the latest value without locking, and the overhead is a single atomic load per request. This is simpler and faster than `Arc<RwLock>` or `Arc<Mutex>`.

### 7.4 Prepared statements

**Decision: On by default, configurable off.** Prepared statements cache query plans in PostgreSQL, improving performance for repeated queries. However, they are incompatible with PgBouncer in transaction pooling mode. A config flag (`prepared_statements = false`) disables them for compatibility.

### 7.5 Axum over Actix Web

**Decision: Axum.** While Actix Web is slightly faster in synthetic benchmarks, Axum's Tower middleware compatibility, simpler mental model, and tight Tokio integration make it the better choice for a project that relies heavily on composable middleware (JWT auth, tracing, CORS). The performance difference is negligible in a database-bound application.

### 7.6 HTMX + Askama over SPA for admin UI

**Decision: Server-rendered HTML.** An SPA (React, Yew, Leptos) would require maintaining a separate frontend codebase, a build pipeline, and duplicated type definitions. HTMX + Askama keeps everything in Rust, compiles to a single binary, and provides sufficient interactivity for an admin panel. This mirrors ActiveAdmin's own server-rendered approach.

---

## 8. Comparison: PostgREST vs. This Project

| Aspect | PostgREST (Haskell) | This project (Rust) |
|---|---|---|
| Language | Haskell (GHC, green threads) | Rust (Tokio, async tasks) |
| HTTP server | Warp | Axum (Hyper) |
| DB driver | Hasql (libpq, binary protocol) | tokio-postgres (pure Rust, binary protocol) |
| Compile-time SQL | None | SQLx for introspection queries |
| Connection pool | Hasql-pool | deadpool-postgres |
| JWT | jose (Haskell) | jsonwebtoken + tower-jwt |
| Middleware | Custom | Tower ecosystem (composable, reusable) |
| Admin UI | None | Planned (Askama + HTMX) |
| Deployment | Single binary | Single static binary (musl) |
| Memory model | GC (GHC runtime) | No GC (ownership + borrowing) |
| Ecosystem | Smaller (Haskell) | Larger (Rust crates) |
| Maturity | 10+ years, production-proven | New build, leveraging proven libraries |

---

## 9. Risk Assessment

| Risk | Likelihood | Mitigation |
|---|---|---|
| Dynamic SQL builder has edge cases | High | Extensive property-based testing; test against PostgREST's own test suite as a compatibility baseline |
| Schema cache query performance on large schemas | Medium | Follow PostgREST's optimized queries; profile and push more logic to SQL if needed |
| tokio-postgres binary protocol quirks | Low | Mature library, well-tested; fallback to text protocol for specific types |
| HTMX limitations for complex admin interactions | Medium | Evaluate per-feature; fallback to minimal JS for specific widgets (e.g., rich text editors) |
| Maintaining PostgREST URL syntax compatibility | Medium | Write a compatibility test suite that runs the same requests against both PostgREST and this server |

---

## 10. References

- [PostgREST documentation](https://docs.postgrest.org/en/stable/)
- [PostgREST architecture](https://docs.postgrest.org/en/v14/explanations/architecture.html)
- [PostgREST source code](https://github.com/PostgREST/postgrest)
- [Axum documentation](https://docs.rs/axum/latest/axum/)
- [Axum GitHub](https://github.com/tokio-rs/axum)
- [tokio-postgres](https://docs.rs/tokio-postgres/latest/tokio_postgres/)
- [SQLx](https://github.com/launchbadge/sqlx)
- [deadpool-postgres](https://docs.rs/deadpool-postgres/latest/deadpool_postgres/)
- [jsonwebtoken](https://github.com/Keats/jsonwebtoken)
- [tower-jwt](https://docs.rs/tower-jwt/latest/tower_jwt/)
- [Askama](https://docs.rs/askama/latest/askama/)
- [HTMX](https://htmx.org/)
- [ActiveAdmin](https://activeadmin.info/)
