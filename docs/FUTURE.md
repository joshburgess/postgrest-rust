# Future Plans

Potential features and extensions beyond PostgREST parity. None of these are committed; this is a living document of ideas worth exploring.

---

## Streaming / Real-time

### Server-Sent Events (SSE) on REST Endpoints
`GET /items` with `Accept: text/event-stream` that re-queries on NOTIFY and streams diffs to the client. Leverages the existing `PgListener` and LISTEN/NOTIFY infrastructure.

### WebSocket Subscriptions Tied to Table Changes
Subscribe to a table with filters, receive pushed rows matching the filter when INSERTs/UPDATEs/DELETEs happen. Similar to Supabase Realtime. Would extend the existing `/ws` endpoint beyond raw NOTIFY forwarding.

### Logical Replication Decoding
Bypass NOTIFY entirely and consume the PostgreSQL WAL stream for guaranteed delivery of change events. More reliable than trigger-based NOTIFY (no message loss under heavy load), but significantly more complex: requires managing replication slots, decoding the logical replication protocol, and handling slot lifecycle.

---

## Caching

### In-Memory Query Result Cache
Cache query results in-process, invalidated via the schema cache's existing LISTEN/NOTIFY channel. Configurable per-table or per-schema with TTL. Would skip the database round-trip entirely for repeated reads. The existing ETag support provides the HTTP-level foundation.

### Materialized View Auto-Refresh
Detect materialized views in the schema cache and expose configuration for automatic `REFRESH MATERIALIZED VIEW CONCURRENTLY` on a schedule or triggered by NOTIFY events on underlying tables.

---

## Query Capabilities

### Aggregate Endpoints (GROUP BY)
`GET /items?select=category,count()&group_by=category`. PostgREST doesn't support GROUP BY. Would enable sum, avg, min, max, count, and array_agg without requiring views or functions for simple aggregations.

### Computed / Virtual Columns
Expose SQL expressions as columns in the API without requiring views. Configured per-table, evaluated server-side. Example: a `full_name` column computed from `first_name || ' ' || last_name`.

### Cross-Schema Joins
Embedding across schema boundaries. Currently, embedding resolves foreign keys within the exposed schemas. This would allow joins between tables in different schemas when the FK relationship crosses schema boundaries.

### Bulk PATCH
Update different rows with different values in a single request. Accept a JSON array where each object includes the primary key and the fields to update. Translates to a single `UPDATE ... FROM jsonb_to_recordset(...)` or similar batched SQL.

### Cursor-Based Pagination
Keyset pagination as an alternative to offset-based pagination. Uses an opaque cursor encoding the last row's sort key values. Better performance for large datasets (no `OFFSET N` scan) and stable results under concurrent writes.

---

## Auth / Multi-tenancy

### OIDC / OAuth2 Token Validation
Verify JWTs against an issuer's JWKS endpoint (e.g., Auth0, Keycloak, Supabase Auth) instead of requiring a shared secret in the config. Cache the JWKS with automatic rotation. Would support RS256, ES256, and EdDSA out of the box.

### Tenant Isolation
Automatic `SET LOCAL` of a tenant ID GUC extracted from the JWT, enabling row-level security policies that filter by tenant without application code. Optionally support schema-per-tenant routing where the `Content-Profile` header or a JWT claim selects the tenant's schema.

---

## Developer Experience

### GraphQL Endpoint
Auto-generated GraphQL schema from the same schema cache that powers the REST API. Additive, doesn't replace REST. Would support queries, mutations, and subscriptions (if streaming is implemented). The schema cache already contains the type and relationship information needed.

### Client SDK Generation
Serve a generated TypeScript (or other language) client SDK directly from the API, derived from the OpenAPI spec. Alternatively, provide a CLI command that generates a typed client from the running server's OpenAPI endpoint.

### Admin RPC
Introspect live server state via authenticated API endpoints: pool metrics (active/idle/waiting connections), active queries, schema cache contents, configuration, and connection health. Extends the existing `/metrics` Prometheus endpoint with structured JSON responses.

---

## Operational

### Request Audit Logging to PostgreSQL
Optionally log request metadata (method, path, role, duration, status code, row count) to a PostgreSQL table for audit and analytics. Configurable per-schema or per-table. Uses the existing per-request transaction to INSERT into the audit table within the same transaction scope.

### Per-Role Rate Limiting
Configurable request rate limits by JWT role, not just global concurrency. Example: `web_anon` gets 100 req/s, `authenticated` gets 1000 req/s, `admin` is unlimited. Uses token bucket or sliding window, stored in-process (no external dependency).

### Read Replica Routing
Route GET requests to read replicas and mutations (POST/PATCH/DELETE) to the primary. Configurable replica pool with health checking and failover. The existing pool infrastructure (`pg-pool`) could manage multiple backend pools with routing logic in the Axum handler layer.
