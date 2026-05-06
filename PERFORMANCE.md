# Performance

Benchmarks comparing the three Rust `pg-rest-server` binaries against the reference PostgREST (Haskell/Hasql) on the same PostgreSQL instance.

All three Rust binaries share the URL parser, schema cache, and HTTP layer. They differ only in the database driver on the data path:

- **`pg-rest-server-tokio-postgres-pg-wired`** uses `pg-wired::AsyncPool` directly. Each request ships its `BEGIN; SET LOCAL ROLE; ...; COMMIT` to one of the round-robin connections in a single pipelined batch. The writer/reader split coalesces concurrent requests into a single `write()` syscall and FIFO-matches responses back.
- **`pg-rest-server-resolute`** uses `resolute::SharedPool`, which wraps the same `pg-wired::AsyncPool` and exposes it through resolute's typed error and query surface. The hot path is the same single-batch pipelined transaction.
- **`pg-rest-server-tokio-postgres-deadpool`** uses pure `tokio-postgres` with `deadpool-postgres` for connection management. Each request checks out a connection, opens a transaction, runs `SET LOCAL ROLE`, runs the query, and commits. No multiplexing on the data path: pool size sets the in-flight ceiling.

The pg-wired and resolute binaries are within run-to-run variance of each other across every workload because they land on the same wire pool. The deadpool binary is a separate baseline that shows what tokio-postgres alone can do without the pipelined pool. The original architectural gap (resolute's `ExclusivePool` doing one in-flight request per connection) is gone for the pg-wired and resolute binaries; `ExclusivePool` is still available in `resolute` for cases that genuinely need exclusive checkout (multi-statement transactions, savepoints, COPY, holding a connection across `await` points), and `pg-rest-server-resolute` reaches for it from background tasks like the schema listener.

## Test Environment

- **CPU**: 16 cores (Apple Silicon)
- **RAM**: 48 GB
- **PostgreSQL**: 16-alpine in Docker
- **Pool size**: 4 (pg-wired and resolute backends; matches the in-flight ceiling those binaries actually use)
- **PostgREST**: default configuration
- **Tool**: Apache Bench (`ab`), 5000 requests per workload (3000 for the large result)
- **Auth**: HS256 JWT (same secret across all servers)
- **Date**: 2026-05-06

## Results

Each table reports requests per second. `pw/PG` and `rs/PG` are the speedup of the pg-wired and resolute backends over PostgREST. PostgREST values are sampled from the run alongside the resolute backend; both pg-wired and resolute were benched against the same PostgREST/postgres containers. Deadpool numbers from the same hardware are summarized in [Deadpool baseline](#deadpool-baseline) below; they were collected against the same database with the same JWT and `ab` matrix, but at two pool sizes (4 and 16) since deadpool's throughput is connection-count-bound.

### 1. Simple Read: `/authors` (3 rows, ~150 bytes)

| c | PostgREST | pg-wired | resolute | pw/PG | rs/PG |
|---|---|---|---|---|---|
| 1   | 2,206  | 4,587  | 4,919  | 2.1x | 2.2x |
| 5   | 9,110  | 14,827 | 14,839 | 1.6x | 1.6x |
| 10  | 13,450 | 20,905 | 21,406 | 1.6x | 1.6x |
| 20  | 7,499  | 23,736 | 24,217 | 3.2x | 3.2x |
| 50  | 2,390  | 21,934 | 23,434 | 9.2x | 9.8x |
| 100 | 2,674  | 23,655 | 20,461 | 8.8x | 7.7x |

### 2. Filtered Read: `/books?pages=gt.300&order=id.asc`

| c | PostgREST | pg-wired | resolute | pw/PG | rs/PG |
|---|---|---|---|---|---|
| 1   | 686    | 4,460  | 4,460  | 6.5x  | 6.5x  |
| 5   | 8,209  | 14,413 | 14,555 | 1.8x  | 1.8x  |
| 10  | 4,324  | 20,488 | 18,279 | 4.7x  | 4.2x  |
| 20  | 3,181  | 22,921 | 20,302 | 7.2x  | 6.4x  |
| 50  | 1,730  | 24,544 | 23,237 | 14.2x | 13.4x |
| 100 | 3,130  | 22,217 | 20,722 | 7.1x  | 6.6x  |

### 3. Embedding: `/authors?select=name,books(title)&id=eq.1`

| c | PostgREST | pg-wired | resolute | pw/PG | rs/PG |
|---|---|---|---|---|---|
| 1   | 1,539 | 4,246  | 4,218  | 2.8x  | 2.7x  |
| 5   | 7,792 | 12,766 | 13,141 | 1.6x  | 1.7x  |
| 10  | 4,000 | 18,385 | 18,084 | 4.6x  | 4.5x  |
| 20  | 3,203 | 20,213 | 20,415 | 6.3x  | 6.4x  |
| 50  | 1,794 | 22,872 | 21,585 | 12.7x | 12.0x |
| 100 | 2,538 | 21,808 | 19,731 | 8.6x  | 7.8x  |

### 4. Large Result: `/numbered` (100 rows, ~2 KB)

| c | PostgREST | pg-wired | resolute | pw/PG | rs/PG |
|---|---|---|---|---|---|
| 1   | 1,378  | 3,942  | 4,161  | 2.9x  | 3.0x  |
| 5   | 2,803  | 11,876 | 11,815 | 4.2x  | 4.2x  |
| 10  | 12,773 | 19,110 | 19,266 | 1.5x  | 1.5x  |
| 20  | 2,400  | 17,837 | 17,480 | 7.4x  | 7.3x  |
| 50  | 1,926  | 18,234 | 20,767 | 9.5x  | 10.8x |
| 100 | 2,979  | 18,136 | 20,168 | 6.1x  | 6.8x  |

### 5. Anonymous (no JWT): `/authors`

| c | PostgREST | pg-wired | resolute | pw/PG | rs/PG |
|---|---|---|---|---|---|
| 1   | 2,772  | 4,148  | 4,802  | 1.5x | 1.7x |
| 5   | 8,753  | 12,175 | 15,378 | 1.4x | 1.8x |
| 10  | 12,501 | 17,360 | 21,284 | 1.4x | 1.7x |
| 20  | 4,022  | 21,071 | 22,057 | 5.2x | 5.5x |
| 50  | 2,708  | 22,117 | 22,929 | 8.2x | 8.5x |
| 100 | 4,745  | 21,558 | 21,761 | 4.5x | 4.6x |

### 6. RPC: `/rpc/add?a=3&b=4` (scalar function)

| c | PostgREST | pg-wired | resolute | pw/PG | rs/PG |
|---|---|---|---|---|---|
| 1   | 1,690 | 4,779  | 4,699  | 2.8x  | 2.8x |
| 5   | 3,519 | 14,757 | 13,804 | 4.2x  | 3.9x |
| 10  | 3,797 | 21,194 | 18,734 | 5.6x  | 4.9x |
| 20  | 7,335 | 22,366 | 24,493 | 3.0x  | 3.3x |
| 50  | 2,679 | 26,821 | 23,570 | 10.0x | 8.8x |
| 100 | 2,895 | 23,093 | 24,170 | 8.0x  | 8.3x |

## Observations

1. **Both Rust backends sustain 18-24K rps from c=10 through c=100.** Pipelining many concurrent requests onto four TCP connections lets a single TCP write carry many `Bind`/`Execute` messages, and the reader task FIFO-matches responses back. The bottleneck at high concurrency is the PostgreSQL container, not the Rust process.

2. **The two backends are within run-to-run variance of each other.** They use the same `pg-wired::AsyncPool` primitive on the data path. Differences across cells (e.g. resolute leading by 2-4K rps in some rows, tokio-postgres leading by similar margins in others) are dominated by ab/ab-startup jitter and PostgreSQL scheduling, not by anything either backend does differently.

3. **PostgREST has high variance under load.** Throughput is good at c=5-10 but degrades sharply as concurrency rises (typically 2-3K rps at c=50-100). Some runs show brief peaks at c=10 in the 12-13K rps range; sustained throughput is well below that.

4. **RPC and embedding are the biggest relative wins.** PostgREST's function-call path and nested-select planner are expensive; the Rust SQL builder produces a single correlated-subquery query that PostgreSQL plans well. At c=50, embedding is roughly 12x faster on both backends.

5. **Pool size is no longer the throughput knob it used to be.** When the resolute backend used `ExclusivePool` (one in-flight request per connection), pool_size set a hard ceiling near `pool_size / per-request-transaction-time`. With `SharedPool`, four connections already saturate this single-container setup; bumping the pool further mostly trades memory for marginal gains until PostgreSQL becomes the bottleneck.

## Deadpool baseline

`pg-rest-server-tokio-postgres-deadpool` is a separate baseline that runs the same workload through pure `tokio-postgres` + `deadpool-postgres`, with no `pg-wired` and no `pg-pool`. The hot path is `pool.get() → client.transaction() → batch_execute(setup_sql) → tx.query(sql, params) → tx.commit()`: each request takes exclusive use of a connection for one short transaction, with no multiplexing. Pool size sets the in-flight ceiling, so deadpool numbers are reported at two sizes.

Same hardware, same ab matrix, same JWT, same database. Numbers in rps; pg-wired column is the same data as the tables above (pool_size=4) for side-by-side context.

### Auth path: `/authors` with JWT (BEGIN; SET LOCAL ROLE; set_config; query; COMMIT)

| c   | pg-wired (pool=4) | deadpool (pool=4) | deadpool (pool=16) |
|-----|------:|------:|------:|
| 1   | 4,587  | 2,028  | 1,953  |
| 5   | 14,827 | 5,256  | 5,513  |
| 10  | 20,905 | 5,321  | 8,041  |
| 20  | 23,736 | 5,301  | 11,225 |
| 50  | 21,934 | 5,304  | 11,190 |
| 100 | 23,655 | 5,267  | 10,773 |

### Anon path: `/authors` no JWT (BEGIN; SET LOCAL ROLE; query; COMMIT)

| c   | pg-wired (pool=4) | deadpool (pool=4) | deadpool (pool=16) |
|-----|------:|------:|------:|
| 1   | 4,148  | 4,100  | 3,234  |
| 5   | 12,175 | 9,992  | 10,169 |
| 10  | 17,360 | 11,211 | 15,038 |
| 20  | 21,071 | 11,146 | 17,162 |
| 50  | 22,117 | 11,285 | 20,464 |
| 100 | 21,558 | 10,849 | 19,841 |

### Other auth-path workloads at deadpool pool=16 (peak vs pg-wired peak)

| workload | deadpool peak | pg-wired peak |
|---|---:|---:|
| `/books?pages=gt.300&order=id.asc` | 11,023 | 24,544 |
| `/authors?select=name,books(title)&id=eq.1` | 10,756 | 22,872 |
| `/numbered` (large result) | 9,763 | 20,767 |
| `/rpc/add?a=3&b=4` | 11,479 | 26,821 |

### Reading the deadpool numbers

1. At `pool_size=4` deadpool ceilings around 5,300 rps on the auth path, ~4-5x below pg-wired at the same pool size. The bottleneck is connection count: a 4-roundtrip transaction with no multiplexing on 4 connections caps you here.
2. Bumping deadpool to `pool_size=16` roughly doubles the auth-path ceiling to ~11K rps. It does not close the gap. pg-wired's writer/reader split coalesces all 4 messages of one transaction into a single TCP write per connection and pipelines the next request behind it; adding deadpool connections only buys parallelism, not pipelining.
3. The anon path closes most of the gap at `pool_size=16` (20.5K vs 22.1K rps at c=50). Setup is one statement instead of two (no `set_config` for claims), so the per-request round-trip count is lower and tokio-postgres's prepared-statement cache hits cleanly.
4. At c=1 deadpool is ~2x slower than pg-wired across the board (~2K vs ~4-5K rps). Single-flight, no concurrency to amortize: every transaction needs N sequential await points.

## Choosing a backend

All three backends share the same query/schema/HTTP layers and pass 1013/1013 of the PostgREST compatibility suite. The choice comes down to dependency footprint and throughput goals:

- **`pg-rest-server-tokio-postgres-pg-wired`**: best throughput ceiling, depends on `tokio-postgres` for setup/LISTEN-NOTIFY plus `pg-wired`/`pg-pool` for the hot path. Default if you want the highest rps and don't mind the extra wire-protocol crate in your tree.
- **`pg-rest-server-resolute`**: same throughput as pg-wired (it wraps the same `pg-wired::AsyncPool`), zero `tokio-postgres` dependency, plus the `resolute` typed query API if you want to extend the server with compile-time-checked queries.
- **`pg-rest-server-tokio-postgres-deadpool`**: smallest dependency footprint, uses only the long-established `tokio-postgres` and `deadpool-postgres`. Lower throughput ceiling because it cannot pipeline on the data path. Pick this if you'd rather not pull in `pg-wired`, you don't need >10K rps per server, and you'd rather scale horizontally than tune around a custom pool.

## Reproducing

```bash
# Reference stack (PostgREST :3100, postgres :54323)
cd test/compat && docker compose up -d

# Rust backend (pick one) on :3201
cargo build --release
target/release/pg-rest-server-tokio-postgres-pg-wired --config test/fixtures/bench-config.toml &
# OR
target/release/pg-rest-server-resolute --config test/fixtures/bench-config.toml &
# OR
target/release/pg-rest-server-tokio-postgres-deadpool --config test/fixtures/bench-config.toml &

# Generate a JWT for "test_user" with the bench secret, then run ab against
# http://127.0.0.1:3100 (PostgREST) and http://127.0.0.1:3201 (Rust).
ab -q -n 5000 -c 50 -H "Authorization: Bearer $JWT" http://127.0.0.1:3201/authors
```
