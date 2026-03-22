# pg-rest-server

A high-performance PostgREST alternative written in Rust. Automatically generates a full REST API from any PostgreSQL database — no application code required.

## Quickstart

### Docker

```bash
# 1. Create a config file
cat > pg-rest.toml << 'EOF'
[database]
uri = "postgres://authenticator:password@localhost:5432/mydb"
schemas = ["api"]
anon_role = "web_anon"

[server]
port = 3000

[jwt]
secret = "your-jwt-secret-at-least-32-characters"
EOF

# 2. Run
docker run -v $(pwd)/pg-rest.toml:/etc/pg-rest/pg-rest.toml \
  -p 3000:3000 pg-rest-server
```

### From source

```bash
cargo install --path pg-rest-server
pg-rest-server --config pg-rest.toml
```

### Verify

```bash
curl http://localhost:3000/          # OpenAPI spec
curl http://localhost:3000/authors   # Read a table
curl http://localhost:3000/live      # Health check
```

## Feature Comparison

| Feature | PostgREST | pg-rest-server |
|---|:---:|:---:|
| **Core** | | |
| Auto-generated REST API from schema | Yes | Yes |
| Tables, views, materialized views | Yes | Yes |
| Foreign tables | Yes | Yes |
| JWT authentication | Yes | Yes |
| Role-based authorization (SET LOCAL ROLE) | Yes | Yes |
| Row-level security | Yes | Yes |
| JSON serialization in PostgreSQL | Yes | Yes |
| **Querying** | | |
| Column selection (`?select=`) | Yes | Yes |
| Filtering (eq, gt, lt, in, is, like, etc.) | Yes | Yes |
| Negation (`not.eq`, `not.is.null`) | Yes | Yes |
| Logical operators (`or`, `and` grouping) | Yes | Yes |
| Ordering (`?order=col.asc`) | Yes | Yes |
| Pagination (limit, offset, Range header) | Yes | Yes |
| Exact count (`Prefer: count=exact`) | Yes | Yes |
| Content-Range header | Yes | Yes |
| **Embedding** | | |
| One-to-many | Yes | Yes |
| Many-to-one | Yes | Yes |
| Many-to-many (inferred join tables) | Yes | Yes |
| `!inner` embed hint | Yes | Yes |
| Spread columns (`...table(*)`) | Yes | Yes |
| **Mutations** | | |
| INSERT (single and bulk) | Yes | Yes |
| UPDATE with filters | Yes | Yes |
| DELETE with filters | Yes | Yes |
| Upsert (`resolution=merge-duplicates`) | Yes | Yes |
| `on_conflict` specific columns | Yes | Yes |
| `Prefer: return=representation` | Yes | Yes |
| `Prefer: return=headers-only` | Yes | Yes |
| Location header on POST | Yes | Yes |
| Generated column awareness | Yes | Yes |
| **Functions** | | |
| RPC via POST `/rpc/function` | Yes | Yes |
| RPC via GET (immutable/stable) | Yes | Yes |
| Named parameters | Yes | Yes |
| Default parameters | Yes | Yes |
| Stored procedures (CALL) | Yes | Yes |
| **Content negotiation** | | |
| JSON (default) | Yes | Yes |
| CSV (`Accept: text/csv`) | Yes | Yes |
| Singular object (`application/vnd.pgrst.object+json`) | Yes | Yes |
| EXPLAIN plan (`application/vnd.pgrst.plan+json`) | Yes | Yes |
| OpenAPI 2.0 (Swagger) | Yes | Yes |
| OpenAPI 3.0 | No | Yes |
| **Select features** | | |
| Type casts (`column::type`) | Yes | Yes |
| JSON path (`data->key`, `data->>key`) | Yes | Yes |
| **Schema management** | | |
| Schema reload via NOTIFY | Yes | Yes |
| Schema reload via endpoint | No | Yes (`POST /reload`) |
| Multiple schema search path | Yes | Yes |
| Accept-Profile / Content-Profile | Yes | Yes |
| **Operational** | | |
| Single binary deployment | Yes | Yes |
| Health endpoints | No | Yes (`/live`, `/ready`) |
| Prometheus metrics | No | Yes (`/metrics`) |
| WebSocket NOTIFY forwarding | No | Yes (`/ws?channel=`) |
| Structured JSON logging | No | Yes |
| Configurable CORS | Limited | Yes |
| Request body size limits | No | Yes |
| Concurrency limits | No | Yes |
| ETag / If-None-Match | No | Yes |
| PgBouncer compatibility | Yes | Yes |
| TLS (rustls) | N/A (reverse proxy) | Yes (optional feature) |
| Docker image | Yes | Yes (Alpine, multi-stage) |
| **Language** | Haskell | Rust |

## Configuration

```toml
[database]
uri = "postgres://authenticator:pass@localhost:5432/mydb"
schemas = ["api"]            # Exposed schemas
anon_role = "web_anon"       # Role for unauthenticated requests
pool_size = 10               # Connection pool size
prepared_statements = true   # false for PgBouncer transaction mode

[server]
host = "0.0.0.0"
port = 3000
log_format = "text"          # "text" or "json"
cors_origins = []            # [] or ["*"] = permissive
body_limit = 1048576         # Max request body (bytes, default 1 MiB)
rate_limit = 0               # Max concurrent requests (0 = unlimited)

[jwt]
secret = "your-secret-here"  # HMAC-SHA256 secret
```

## Migration from PostgREST

pg-rest-server is designed as a drop-in replacement. To migrate:

1. **Config**: Convert your PostgREST config to TOML format:

   | PostgREST env var | pg-rest-server config |
   |---|---|
   | `PGRST_DB_URI` | `database.uri` |
   | `PGRST_DB_SCHEMAS` | `database.schemas` |
   | `PGRST_DB_ANON_ROLE` | `database.anon_role` |
   | `PGRST_JWT_SECRET` | `jwt.secret` |
   | `PGRST_SERVER_PORT` | `server.port` |

2. **URL syntax**: All PostgREST URL syntax is supported — filters, select, order, embedding, RPC calls work identically.

3. **Headers**: `Prefer`, `Range`, `Accept`, `Authorization`, `Accept-Profile`, `Content-Profile` headers work the same way.

4. **Differences**:
   - Additional endpoints: `/live`, `/ready`, `/metrics`, `/reload`, `/ws`
   - OpenAPI 3.0 support via `?openapi-version=3`
   - Error responses use PGRST-prefixed codes but JSON structure matches

5. **Verify compatibility**: Run the included compatibility test suite against both servers:

   ```bash
   cd test/compat && docker compose up -d
   pg-rest-server --config pg-rest-compat.toml &
   cargo run -p compat-test
   ```

## Architecture

```
pg-rest-server (bin)          ← Axum HTTP server, JWT, routing
    └── pg-query-engine (lib) ← URL parser, SQL builder
        └── pg-schema-cache (lib) ← PostgreSQL catalog introspection
```

## Development

```bash
# Start test database
docker compose up -d

# Run all tests (74 total: unit + proptest + integration)
cargo test

# Run compatibility tests against PostgREST
cd test/compat && docker compose up -d
cargo run -p pg-rest-server --release -- --config test/compat/pg-rest-compat.toml &
cargo run -p compat-test

# Benchmarks
cargo run -p pg-rest-server --release -- --config test/fixtures/test-config.toml &
k6 run bench/k6.js
```

## License

MIT
