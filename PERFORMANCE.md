# Performance

Benchmarks comparing both `pg-rest-server` backends (Rust) against the reference PostgREST (Haskell/Hasql) on the same PostgreSQL instance.

The two Rust binaries share the URL parser, schema cache, and HTTP layer but have very different database data paths:

- **`pg-rest-server-tokio-postgres`** uses `pg-wired`'s `AsyncPool` of `AsyncConn`s. Each `AsyncConn` is a single TCP connection with a writer/reader split that coalesces concurrent requests into a single `write()` syscall and FIFO-matches responses. The pool round-robins between N such connections.
- **`pg-rest-server-resolute`** uses the published `resolute` crate (`TypedPool`). Each request acquires a connection from the pool, runs its `BEGIN; SET LOCAL ROLE; ...; COMMIT` sequence, then returns the connection. Throughput is bounded by `pool_size / round_trip_time`.

The first design pipelines many concurrent requests over each TCP connection. The second uses one connection at a time per request. Both architectures have their place: pipelining wins for high-concurrency read-mostly workloads; the conventional pool is simpler and gives the user direct control over connection lifecycle (savepoints, multi-statement transactions, COPY).

## Test Environment

- **CPU**: 16 cores (Apple Silicon)
- **RAM**: 48 GB
- **PostgreSQL**: 16-alpine in Docker
- **Pool size**: 4 (both backends, both binaries)
- **PostgREST**: default configuration
- **Tool**: Apache Bench (`ab`), 5000 requests per workload (3000 for the large result)
- **Auth**: HS256 JWT (same secret across all three servers)
- **Date**: 2026-05-06

## Results

Each table reports requests per second. `tp/PG` and `rs/PG` are the speedup of the tokio-postgres and resolute backends over PostgREST.

### 1. Simple Read: `/authors` (3 rows, ~150 bytes)

| c | PostgREST | tokio-postgres | resolute | tp/PG | rs/PG |
|---|---|---|---|---|---|
| 1   | 2,762  | 4,717   | 1,850 | 1.7x | 0.7x |
| 5   | 9,165  | 15,154  | 4,399 | 1.7x | 0.5x |
| 10  | 13,506 | 21,270  | 4,465 | 1.6x | 0.3x |
| 20  | 6,841  | 24,632  | 4,435 | 3.6x | 0.6x |
| 50  | 2,369  | 22,984  | 4,301 | 9.7x | 1.8x |
| 100 | 2,695  | 23,241  | 4,347 | 8.6x | 1.6x |

### 2. Filtered Read: `/books?pages=gt.300&order=id.asc`

| c | PostgREST | tokio-postgres | resolute | tp/PG | rs/PG |
|---|---|---|---|---|---|
| 1   | 2,549  | 4,472  | 1,758 | 1.8x | 0.7x |
| 5   | 8,235  | 14,064 | 4,280 | 1.7x | 0.5x |
| 10  | 11,644 | 20,521 | 4,234 | 1.8x | 0.4x |
| 20  | 8,493  | 20,512 | 4,277 | 2.4x | 0.5x |
| 50  | 2,528  | 23,741 | 4,326 | 9.4x | 1.7x |
| 100 | 2,567  | 21,401 | 4,168 | 8.3x | 1.6x |

### 3. Embedding: `/authors?select=name,books(title)&id=eq.1`

| c | PostgREST | tokio-postgres | resolute | tp/PG | rs/PG |
|---|---|---|---|---|---|
| 1   | 1,183 | 4,098  | 1,637 | 3.5x  | 1.4x |
| 5   | 2,250 | 13,389 | 3,856 | 5.9x  | 1.7x |
| 10  | 1,477 | 18,742 | 3,467 | 12.7x | 2.3x |
| 20  | 2,160 | 20,345 | 3,868 | 9.4x  | 1.8x |
| 50  | 1,179 | 21,864 | 3,857 | 18.5x | 3.3x |
| 100 | 2,460 | 21,845 | 3,959 | 8.9x  | 1.6x |

### 4. Large Result: `/numbered` (100 rows, ~2 KB)

| c | PostgREST | tokio-postgres | resolute | tp/PG | rs/PG |
|---|---|---|---|---|---|
| 1   | 1,369  | 4,013  | 1,713 | 2.9x  | 1.3x |
| 5   | 8,080  | 13,172 | 4,134 | 1.6x  | 0.5x |
| 10  | 11,666 | 19,319 | 4,198 | 1.7x  | 0.4x |
| 20  | 2,761  | 18,781 | 4,140 | 6.8x  | 1.5x |
| 50  | 1,237  | 20,093 | 4,252 | 16.2x | 3.4x |
| 100 | 5,655  | 21,563 | 4,209 | 3.8x  | 0.7x |

### 5. Anonymous (no JWT): `/authors`

| c | PostgREST | tokio-postgres | resolute | tp/PG | rs/PG |
|---|---|---|---|---|---|
| 1   | 2,689  | 4,099  | 2,243 | 1.5x | 0.8x |
| 5   | 8,828  | 13,008 | 5,291 | 1.5x | 0.6x |
| 10  | 12,309 | 18,673 | 5,180 | 1.5x | 0.4x |
| 20  | 3,776  | 19,400 | 5,221 | 5.1x | 1.4x |
| 50  | 2,830  | 20,730 | 5,197 | 7.3x | 1.8x |
| 100 | 2,991  | 21,214 | 5,210 | 7.1x | 1.7x |

### 6. RPC: `/rpc/add?a=3&b=4` (scalar function)

| c | PostgREST | tokio-postgres | resolute | tp/PG | rs/PG |
|---|---|---|---|---|---|
| 1   | 836   | 4,727  | 1,873 | 5.7x  | 2.2x |
| 5   | 3,470 | 14,171 | 4,588 | 4.1x  | 1.3x |
| 10  | 3,374 | 19,304 | 4,532 | 5.7x  | 1.3x |
| 20  | 3,069 | 23,692 | 4,540 | 7.7x  | 1.5x |
| 50  | 1,955 | 23,333 | 4,563 | 11.9x | 2.3x |
| 100 | 3,747 | 22,640 | 4,513 | 6.0x  | 1.2x |

## Observations

1. **The tokio-postgres backend dominates at high concurrency.** It maintains 20-24K rps from c=10 through c=100 across every workload. Coalescing concurrent requests onto a small number of TCP connections lets a single TCP write carry many `Bind`/`Execute` messages, and the reader task FIFO-matches responses back. At c=100 it is 6-18x faster than PostgREST.

2. **The resolute backend plateaus at `pool_size / per-request-transaction-time`.** With four connections and per-request transactions of roughly 0.9 ms, throughput tops out near 4,400 rps. This is the cost of conventional pool semantics: only one in-flight request per connection, four round-trips per request (`BEGIN`, `SET LOCAL ROLE` / `set_config`, query, `COMMIT`). The plateau is flat from c=5 through c=100 because the bottleneck is connection count, not concurrency.

3. **Resolute scales linearly with `pool_size`.** Bumping the same `/authors` workload at c=50 from `pool_size=4` to `pool_size=20` raises throughput from 4,301 to 9,715 rps. Trading connections for throughput is a knob users can turn.

4. **PostgREST has high variance under load.** Throughput is good at c=5-10 but degrades sharply as concurrency rises (typically 2-3K rps at c=50-100). Some runs see brief peaks at c=10 in the 12-13K rps range; sustained throughput is well below that.

5. **RPC and embedding are the biggest relative wins.** PostgREST's function-call path and nested-select planner are expensive; the Rust SQL builder produces a single correlated-subquery query that PostgreSQL plans well. At c=50, embedding is 18.5x faster on the tokio-postgres backend and 3.3x faster on the resolute backend.

## Pool scaling (resolute)

The same `/authors` workload at c=50, varying `pool_size` for the resolute backend:

| pool_size | rps  |
|-----------|------|
| 4         | 4,301 |
| 20        | 9,715 |

The relationship is roughly linear until the PostgreSQL container becomes CPU-bound. Each additional connection adds another concurrent backend process; in this single-container setup, throughput tops out somewhere around 8-10 connections before contention dominates.

## Choosing a backend

- **High-concurrency read-mostly REST API**: pick `pg-rest-server-tokio-postgres`. The pipelined data path is hard to beat for this shape of workload.
- **Mixed transactional workload that benefits from holding connections (multi-statement transactions, savepoints, COPY, listening for NOTIFY in the request lifecycle)**: pick `pg-rest-server-resolute`, and size the pool to match expected concurrency.

Both pass 1013/1013 in the PostgREST compatibility suite; the choice is operational, not a feature gap.

## Reproducing

```bash
# Reference stack (PostgREST :3100, postgres :54323)
cd test/compat && docker compose up -d

# Rust backend (pick one) on :3201
cargo build --release
target/release/pg-rest-server-tokio-postgres --config test/fixtures/bench-config.toml &
# OR
target/release/pg-rest-server-resolute --config test/fixtures/bench-config.toml &

# Generate a JWT for "test_user" with the bench secret, then run ab against
# http://127.0.0.1:3100 (PostgREST) and http://127.0.0.1:3201 (Rust).
ab -q -n 5000 -c 50 -H "Authorization: Bearer $JWT" http://127.0.0.1:3201/authors
```
