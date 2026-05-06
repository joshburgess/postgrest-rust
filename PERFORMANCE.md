# Performance

Benchmark results comparing pg-rest-server (Rust/pg-wired AsyncConn) against PostgREST (Haskell/Hasql) on the same PostgreSQL instance. Numbers below are for the `pg-rest-server-tokio-postgres` binary; the `pg-rest-server-resolute` variant uses the same `pg-wired` data path and is expected to be in the same range.

## Test Environment

- **CPU**: 16 cores (Apple Silicon)
- **RAM**: 48 GB
- **PostgreSQL**: 16-alpine in Docker (single container)
- **pg-rest-server**: Single AsyncConn (one TCP connection to PostgreSQL)
- **PostgREST**: Default configuration, connection pool
- **Tool**: Apache Bench (`ab`), 5000 requests per test (3000 for large results)
- **Auth**: JWT with HS256 (same secret for both servers)
- **Date**: 2026-03-23

## Architecture

pg-rest-server uses a custom wire protocol driver (`pg-wired`) with:
- **Async writer/reader split**: dedicated tokio tasks for send and receive
- **Message coalescing**: concurrent requests batched into single `write()` syscall
- **Binary extended query protocol**: parameterized queries (injection-safe)
- **Statement cache**: Parse on first use, Bind+Execute on subsequent (LRU, 256 entries)
- **Pipeline transactions**: `BEGIN; SET LOCAL ROLE; set_config` + parameterized query + `COMMIT` in one TCP write

## Results

### 1. Simple Read: `/authors` (3 rows, ~150 bytes)

| Concurrency | PostgREST | pg-rest-server | Speedup |
|---|---|---|---|
| 1 | 2,218 rps | 4,656 rps | 2.1x |
| 5 | 8,440 rps | 13,474 rps | 1.6x |
| 10 | 12,447 rps | 13,412 rps | 1.1x |
| 20 | 7,353 rps | 13,267 rps | 1.8x |
| 50 | 2,433 rps | 13,548 rps | 5.6x |
| 100 | 2,075 rps | 13,155 rps | 6.3x |

### 2. Filtered Read: `/books?pages=gt.300&order=id.asc` (2 rows)

| Concurrency | PostgREST | pg-rest-server | Speedup |
|---|---|---|---|
| 1 | 2,114 rps | 4,647 rps | 2.2x |
| 10 | 3,582 rps | 12,542 rps | 3.5x |
| 50 | 3,052 rps | 12,387 rps | 4.1x |
| 100 | 1,159 rps | 12,858 rps | 11.1x |

### 3. Embedding: `/authors?select=name,books(title)&id=eq.1` (nested JSON)

| Concurrency | PostgREST | pg-rest-server | Speedup |
|---|---|---|---|
| 1 | 1,430 rps | 4,356 rps | 3.0x |
| 10 | 3,778 rps | 11,004 rps | 2.9x |
| 50 | 2,469 rps | 11,455 rps | 4.6x |
| 100 | 2,507 rps | 12,093 rps | 4.8x |

### 4. Large Result: `/numbered` (100 rows, ~2KB)

| Concurrency | PostgREST | pg-rest-server | Speedup |
|---|---|---|---|
| 1 | 887 rps | 4,139 rps | 4.7x |
| 10 | 2,473 rps | 9,364 rps | 3.8x |
| 50 | 1,852 rps | 9,854 rps | 5.3x |
| 100 | 2,184 rps | 11,530 rps | 5.3x |

### 5. Anonymous (no JWT): `/authors`

| Concurrency | PostgREST | pg-rest-server | Speedup |
|---|---|---|---|
| 1 | 1,575 rps | 4,749 rps | 3.0x |
| 10 | 3,408 rps | 13,961 rps | 4.1x |
| 50 | 1,936 rps | 15,163 rps | 7.8x |
| 100 | 1,684 rps | 15,396 rps | 9.1x |

### 6. RPC: `/rpc/add?a=3&b=4` (scalar function)

| Concurrency | PostgREST | pg-rest-server | Speedup |
|---|---|---|---|
| 1 | 604 rps | 4,630 rps | 7.7x |
| 10 | 3,939 rps | 13,170 rps | 3.3x |
| 50 | 1,961 rps | 14,592 rps | 7.4x |
| 100 | 1,464 rps | 13,937 rps | 9.5x |

## Key Observations

1. **pg-rest-server is faster at every concurrency level** across all workloads, ranging from 1.1x to 11.1x faster.

2. **Consistent throughput under load**: pg-rest-server maintains ~13-15K rps from c=5 through c=200. PostgREST peaks at c=5-10 then degrades sharply (12K → 2K rps).

3. **Single-connection architecture**: All results above use a single TCP connection to PostgreSQL via the async writer/reader split. The writer task coalesces concurrent requests into one `write()` syscall; the reader task FIFO-matches responses to callers.

4. **Largest wins at high concurrency**: At c=100, pg-rest-server is 5-11x faster depending on workload. PostgREST's connection pool and GHC runtime introduce contention that degrades throughput.

5. **RPC is the biggest single-threaded win**: 7.7x faster at c=1 (4,630 vs 604 rps). PostgREST's function call path has high overhead.

6. **Large results scale well**: 100-row queries at 11,530 rps (c=100) show the zero-copy response passthrough works efficiently.

## Bottleneck Analysis

The current bottleneck is the **single PostgreSQL backend process**. Each TCP connection maps to one PG backend, which is single-threaded. At ~14K rps, the backend is near saturation.

**Next optimization**: Pool of N AsyncConns (N TCP connections → N PG backends). This would spread query execution across multiple backend processes, potentially reaching 40-60K rps on this hardware.

## Evolution of Performance

| Approach | Auth c=20 rps | Auth c=100 rps | Safety |
|---|---|---|---|
| tokio-postgres + transaction | 7,096 | 6,826 | Binary protocol |
| simple_query (inlined params) | 21,453 | 20,165 | String escaping |
| pg-wired pool (binary) | 10,796 | 6,826 | Binary protocol |
| pg-wired AsyncConn ×1 (binary) | 13,267 | 13,155 | Binary protocol |
| **pg-wired AsyncPool ×4 (binary)** | **25,036** | **23,987** | **Binary protocol** |
| PostgREST (reference) | 7,353 | 2,075 | Binary protocol |

The AsyncPool architecture delivers the best combination of speed and safety: binary protocol parameterization (injection-proof) with async message coalescing and multi-backend parallelism.

## AsyncPool Scaling

Round-robin dispatch across N AsyncConns (N TCP connections → N PG backends):

| Pool Size | c=1 | c=10 | c=50 | c=100 |
|---|---|---|---|---|
| 1 AsyncConn | 4,562 | 13,765 | 14,234 | 14,022 |
| 2 AsyncConns | 4,500 | 19,335 | 23,334 | 21,504 |
| 4 AsyncConns | 4,590 | 21,363 | 25,036 | 23,987 |
| 8 AsyncConns | 4,202 | 21,693 | 25,525 | 24,933 |
| PostgREST | 2,456 | 10,495 | 3,097 | 2,832 |

- c=1 is unaffected by pool size (single request, single backend)
- 2 connections nearly doubles throughput at c≥10
- 4 connections is the sweet spot (+76% over 1 conn)
- 8 connections shows diminishing returns (PG container CPU-bound)
- At c=100 with 4 conns: **8.5x faster** than PostgREST
