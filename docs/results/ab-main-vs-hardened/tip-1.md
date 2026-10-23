# ast-bench report

## Environment

- ast-bench: 0.1.0
- rustc: rustc 1.99.1 (58cf8f9c3 2026-10-13) (Homebrew)
- uname: Darwin Guilhermes-MacBook-Pro.local 27.0.0 Darwin Kernel Version 27.0.0: Tue Aug 11 21:06:24 PDT 2026; root:xnu-13432.1.9~1/RELEASE_ARM64_T6030 arm64
- CPU: Apple M3 Pro
- logical CPUs: 11
- build profile: release
- crate versions (Cargo.lock): rmcp 3.6.0, rmp-serde 1.3.1, serde_json 1.0.154, tokio 1.54.1
- git revision: 13a577d
- parameters: iterations=500, warmup=100, repeats=7, total-ops=2000, cpu-ops=20000, concurrency-levels=4,16,64, raw=false, quick=false

Protocols: `legacy` = agent-tools-server (JSON-Lines, UDS); `ast` = ast-server (MessagePack, UDS, this repository's SDK); `mcp-server` = real MCP server on rmcp =3.6.0 (stdio); `mcp-like-server` = synthetic stand-in (stdio, not a real MCP implementation).

## Methodology

All latency cells are closed-loop: a measured run issues a fixed number of operations and waits for each response before issuing the next, or, for the concurrency cells, keeps a fixed window of C requests in flight on the same connection(s). Offered load is therefore set by how fast the server answers, not by a fixed request rate. Every cell first runs `--warmup` discarded operations of the same shape, then `--repeats` measured runs of `--iterations` operations. Each run reports p50/p95/p99 over successful operations only; the tables show the median of each percentile across repeats, and for p50 also the min–max range. Errors and timeouts are counted per cell and never enter the percentiles; the expected value is 0. Startup is measured separately (spawn to first completed `system.noop`, process killed after each repetition) and is never added to per-call latency. Server CPU is the CPU-time delta of the server process around a fixed number of noops, divided by the number of calls. Peak RSS is sampled with `ps` across the whole run.

**Scenarios run:** noop, concurrency

## No-op (`system.noop`), sequential, persistent connection

The same call on every protocol. The AST ping row is the protocol floor and is not comparable to tool calls.

| Protocol | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| legacy (JSON-Lines, UDS) | 21.7 µs (17.6–22.3) | 28.3 | 36.6 | 44705 | 0 |
| AST (MessagePack, UDS) | 30.2 µs (28.6–30.4) | 40.9 | 47.1 | 32720 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 30.8 µs (24.7–31.7) | 34.5 | 37.5 | 32629 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 13.4 µs (13.3–13.5) | 15.5 | 16.8 | 73836 | 0 |
| AST ping (protocol floor, not comparable) | 19.2 µs (18.9–19.3) | 26.5 | 33.3 | 50485 | 0 |

## Concurrency: closed loop on persistent connections

Each model keeps C requests in flight. AST multiplexes them on one connection or on C connections; legacy uses C connections; MCP pipelines C requests on one stdio connection, matching responses by id. The synthetic MCP server is sequential by construction, so its latency includes queueing. Throughput is successful operations divided by wall time.

| Model · concurrency · connections | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| ast: 1 multiplexed connection · C=4 · connections=1 | 30.7 µs (30.3–31.5) | 47.4 | 57.5 | 124877 | 0 |
| ast: C connections (1 in flight each) · C=4 · connections=4 | 30.2 µs (29.6–31.1) | 50.8 | 61.7 | 124623 | 0 |
| legacy: C connections (1 in flight each) · C=4 · connections=4 | 28.5 µs (27.7–29.4) | 48.5 | 60.0 | 132147 | 0 |
| mcp-server (real): 1 process, 1 stdio, pipelined · C=4 · connections=1 | 84.4 µs (83.8–85.2) | 103.3 | 113.2 | 45227 | 0 |
| mcp-like-server (synthetic, sequential server): 1 stdio, pipelined · C=4 · connections=1 | 8.2 µs (7.6–8.8) | 17.0 | 19.7 | 352040 | 0 |
| ast: 1 multiplexed connection · C=16 · connections=1 | 117.1 µs (114.5–118.8) | 249.9 | 350.0 | 120381 | 0 |
| ast: C connections (1 in flight each) · C=16 · connections=16 | 110.5 µs (106.9–117.2) | 285.9 | 421.6 | 117926 | 0 |
| legacy: C connections (1 in flight each) · C=16 · connections=16 | 111.8 µs (108.7–124.7) | 255.7 | 378.0 | 120908 | 0 |
| mcp-server (real): 1 process, 1 stdio, pipelined · C=16 · connections=1 | 348.2 µs (344.1–350.8) | 382.0 | 399.6 | 45415 | 0 |
| mcp-like-server (synthetic, sequential server): 1 stdio, pipelined · C=16 · connections=1 | 30.9 µs (30.8–31.3) | 37.5 | 41.6 | 479090 | 0 |
| ast: 1 multiplexed connection · C=64 · connections=1 | 465.2 µs (446.7–513.5) | 839.5 | 988.8 | 127660 | 0 |
| ast: C connections (1 in flight each) · C=64 · connections=64 | 508.4 µs (427.7–522.5) | 933.8 | 1183.5 | 115354 | 0 |
| legacy: C connections (1 in flight each) · C=64 · connections=64 | 486.5 µs (350.1–554.0) | 870.5 | 1160.9 | 122229 | 0 |
| mcp-server (real): 1 process, 1 stdio, pipelined · C=64 · connections=1 | 1410.6 µs (1386.8–1444.7) | 1487.2 | 1521.1 | 45498 | 0 |
| mcp-like-server (synthetic, sequential server): 1 stdio, pipelined · C=64 · connections=1 | 126.7 µs (124.7–128.2) | 139.0 | 141.9 | 489816 | 0 |

## Not measured

- Allocations per call (covered by tests, not by this benchmark).
- Cancellation and deadline latency (covered by tests).
- Behaviour under a slow reader or back-pressure (covered by tests).
- Malformed or adversarial inputs (covered by tests).
- MCP over transports other than stdio (for example Streamable HTTP or SSE).
- Network transports (TCP, TLS) for any protocol.
- CPU of the benchmark client itself; server CPU only.
- Tools other than `system.noop`, `system.info` and `filesystem.read`, except discovery.
- Behaviour with many registered tools or large input schemas.

## Threats to validity

- Single machine and a single session. Absolute numbers vary across hardware and runs; compare results within one run.
- UDS (legacy, AST) and stdio pipes (MCP) have different kernel costs. Part of any difference is transport, not protocol.
- The benchmark client runs on the same host and competes for CPU with the servers, which inflates latency under concurrency.
- The MCP client is a minimal JSON-RPC client written for this benchmark, not the rmcp client. It does less work than an SDK client would, which biases the comparison in MCP's favour; any AST advantage shown here is, if anything, understated.
- The AST client is this repository's SDK, with its own task and channel overhead. That biases the comparison against AST, the opposite direction.
- The tools are trivial (`system.noop`, `system.info`, a small read). Real tool work will dominate latency in production.
- `ps` has coarse resolution: cputime is ~10 ms, RSS is whole KiB sampled about every 40 ms, so a short memory peak can be missed.
- The MCP client does not send `notifications/initialized` after `initialize`. This is a benchmark simplification, not a conformance claim.
- The `mcp-like-server` is a synthetic stand-in, not a real MCP implementation, and is sequential by construction; its pipelined latency includes queueing.
- The AST server defaults to 64 in-flight requests per connection, which bounds the multiplexed concurrency level; C = 64 is the boundary of that model.
- Batch results compare a BATCH frame (AST) with 20 pipelined requests (MCP). MCP has no batch operation, so the MCP row is pipelining, not batching.
- Startup includes up to ~1 ms of socket-polling granularity.
- Pipelined throughput is not per-call cost. With C requests in flight, reads and writes are batched, so throughput can rise several-fold with C (the synthetic MCP server goes from about 73k to over 350k ops/s between C=1 and C=4 in the quick run). Compare pipelined rows only with rows at the same C.
- These results do not replace measurement under real load.

---

Reproduce with: `cargo build --release && cargo run --release --bin ast-bench -- --only noop,concurrency`

