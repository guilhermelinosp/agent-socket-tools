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
| legacy (JSON-Lines, UDS) | 22.2 µs (15.7–22.6) | 28.2 | 37.0 | 43829 | 0 |
| AST (MessagePack, UDS) | 30.8 µs (30.3–31.1) | 41.8 | 46.7 | 31961 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 31.3 µs (30.7–31.5) | 35.0 | 37.4 | 31957 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 13.4 µs (13.2–13.5) | 15.7 | 16.8 | 73748 | 0 |
| AST ping (protocol floor, not comparable) | 19.2 µs (18.8–19.5) | 26.2 | 33.1 | 50923 | 0 |

## Concurrency: closed loop on persistent connections

Each model keeps C requests in flight. AST multiplexes them on one connection or on C connections; legacy uses C connections; MCP pipelines C requests on one stdio connection, matching responses by id. The synthetic MCP server is sequential by construction, so its latency includes queueing. Throughput is successful operations divided by wall time.

| Model · concurrency · connections | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| ast: 1 multiplexed connection · C=4 · connections=1 | 30.6 µs (29.9–30.9) | 47.8 | 57.4 | 124414 | 0 |
| ast: C connections (1 in flight each) · C=4 · connections=4 | 30.5 µs (28.6–30.7) | 50.5 | 61.2 | 123873 | 0 |
| legacy: C connections (1 in flight each) · C=4 · connections=4 | 28.5 µs (27.8–28.9) | 49.1 | 60.2 | 132326 | 0 |
| mcp-server (real): 1 process, 1 stdio, pipelined · C=4 · connections=1 | 84.6 µs (83.9–84.9) | 103.1 | 113.8 | 45175 | 0 |
| mcp-like-server (synthetic, sequential server): 1 stdio, pipelined · C=4 · connections=1 | 8.7 µs (8.2–8.9) | 17.6 | 20.2 | 337074 | 0 |
| ast: 1 multiplexed connection · C=16 · connections=1 | 116.7 µs (115.6–119.7) | 251.2 | 351.1 | 120115 | 0 |
| ast: C connections (1 in flight each) · C=16 · connections=16 | 104.0 µs (100.6–117.9) | 275.8 | 455.2 | 117629 | 0 |
| legacy: C connections (1 in flight each) · C=16 · connections=16 | 115.3 µs (111.5–120.7) | 280.5 | 422.2 | 119419 | 0 |
| mcp-server (real): 1 process, 1 stdio, pipelined · C=16 · connections=1 | 349.3 µs (347.5–351.5) | 384.5 | 399.5 | 45188 | 0 |
| mcp-like-server (synthetic, sequential server): 1 stdio, pipelined · C=16 · connections=1 | 30.5 µs (30.3–31.1) | 37.0 | 41.2 | 484281 | 0 |
| ast: 1 multiplexed connection · C=64 · connections=1 | 451.7 µs (441.6–467.5) | 785.5 | 980.9 | 129611 | 0 |
| ast: C connections (1 in flight each) · C=64 · connections=64 | 434.3 µs (396.2–455.2) | 768.9 | 962.9 | 131832 | 0 |
| legacy: C connections (1 in flight each) · C=64 · connections=64 | 460.5 µs (419.6–510.9) | 815.5 | 1054.0 | 127767 | 0 |
| mcp-server (real): 1 process, 1 stdio, pipelined · C=64 · connections=1 | 1408.5 µs (1404.8–1415.7) | 1479.2 | 1507.6 | 45403 | 0 |
| mcp-like-server (synthetic, sequential server): 1 stdio, pipelined · C=64 · connections=1 | 127.6 µs (125.3–130.4) | 138.5 | 144.1 | 489516 | 0 |

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

