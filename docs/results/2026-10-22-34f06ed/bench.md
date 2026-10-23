# ast-bench report

## Environment

- ast-bench: 0.1.0
- rustc: rustc 1.99.1 (58cf8f9c3 2026-10-13) (Homebrew)
- uname: Darwin Guilhermes-MacBook-Pro.local 27.0.0 Darwin Kernel Version 27.0.0: Tue Aug 11 21:06:24 PDT 2026; root:xnu-13432.1.9~1/RELEASE_ARM64_T6030 arm64
- CPU: Apple M3 Pro
- logical CPUs: 11
- build profile: release
- crate versions (Cargo.lock): rmcp 3.6.0, rmp-serde 1.3.1, serde_json 1.0.154, tokio 1.54.1
- git revision: 34f06ed
- parameters: iterations=500, warmup=100, repeats=5, total-ops=2000, cpu-ops=20000, concurrency-levels=1,4,16,64, raw=false, quick=false

Protocols: `legacy` = agent-tools-server (JSON-Lines, UDS); `ast` = ast-server (MessagePack, UDS, this repository's SDK); `mcp-server` = real MCP server on rmcp =3.6.0 (stdio); `mcp-like-server` = synthetic stand-in (stdio, not a real MCP implementation).

## Methodology

All latency cells are closed-loop: a measured run issues a fixed number of operations and waits for each response before issuing the next, or, for the concurrency cells, keeps a fixed window of C requests in flight on the same connection(s). Offered load is therefore set by how fast the server answers, not by a fixed request rate. Every cell first runs `--warmup` discarded operations of the same shape, then `--repeats` measured runs of `--iterations` operations. Each run reports p50/p95/p99 over successful operations only; the tables show the median of each percentile across repeats, and for p50 also the min–max range. Errors and timeouts are counted per cell and never enter the percentiles; the expected value is 0. Startup is measured separately (spawn to first completed `system.noop`, process killed after each repetition) and is never added to per-call latency. Server CPU is the CPU-time delta of the server process around a fixed number of noops, divided by the number of calls. Peak RSS is sampled with `ps` across the whole run.

**Scenarios run:** startup, noop, info, payload, discovery, batch, concurrency, deployment, cpu, rss

## Startup (separate from per-call latency)

Spawn to first completed `system.noop`. MCP includes `initialize`; AST includes HELLO. Each repeat uses a fresh process.

| Server | startup median (min–max) | errors |
|---|---|---|
| legacy (JSON-Lines, UDS) | 8.95 ms (8.48–9.83) | 0 |
| AST (MessagePack, UDS) | 5.67 ms (5.52–6.39) | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 3.17 ms (2.97–3.80) | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 2.24 ms (2.06–2.46) | 0 |

## No-op (`system.noop`), sequential, persistent connection

The same call on every protocol. The AST ping row is the protocol floor and is not comparable to tool calls.

| Protocol | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| legacy (JSON-Lines, UDS) | 22.2 µs (20.6–23.4) | 27.8 | 37.0 | 43998 | 0 |
| AST (MessagePack, UDS) | 30.1 µs (30.0–31.9) | 43.0 | 52.0 | 32309 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 31.0 µs (30.5–33.0) | 41.6 | 55.0 | 31008 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 13.3 µs (13.3–13.5) | 15.9 | 18.3 | 73543 | 0 |
| AST ping (protocol floor, not comparable) | 19.2 µs (19.0–19.7) | 26.6 | 33.8 | 49992 | 0 |

## Info (`system.info`), sequential, persistent connection

| Protocol | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| legacy (JSON-Lines, UDS) | 23.2 µs (22.9–24.3) | 29.1 | 38.4 | 42351 | 0 |
| AST (MessagePack, UDS) | 31.3 µs (30.9–32.3) | 44.0 | 50.5 | 31265 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 30.4 µs (30.1–30.8) | 35.3 | 40.8 | 32432 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 13.5 µs (13.4–14.0) | 16.2 | 18.5 | 71685 | 0 |

## Payload sizes (`filesystem.read`, sequential, persistent connection)

### tiny

| Protocol | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| legacy (JSON-Lines, UDS) | 41.9 µs (40.1–42.2) | 49.8 | 63.8 | 23142 | 0 |
| AST (MessagePack, UDS) | 48.6 µs (39.2–49.8) | 65.4 | 76.1 | 19705 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 48.7 µs (47.0–50.0) | 56.7 | 62.2 | 20250 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 29.5 µs (29.3–33.2) | 39.2 | 41.5 | 31498 | 0 |

### medium

| Protocol | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| legacy (JSON-Lines, UDS) | 70.4 µs (68.1–71.5) | 85.5 | 99.2 | 13869 | 0 |
| AST (MessagePack, UDS) | 71.0 µs (70.5–71.8) | 90.6 | 114.2 | 13508 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 74.3 µs (74.2–74.6) | 82.6 | 104.9 | 13366 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 55.5 µs (54.3–56.1) | 62.5 | 73.6 | 17710 | 0 |

### large

| Protocol | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| legacy (JSON-Lines, UDS) | 10526.0 µs (10520.9–10543.8) | 10732.4 | 10847.6 | 95 | 0 |
| AST (MessagePack, UDS) | 678.1 µs (672.6–679.1) | 754.2 | 897.2 | 1453 | 0 |
| AST streamed (invoke_stream, UDS) | 658.5 µs (652.9–659.6) | 750.9 | 828.8 | 1499 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 664.5 µs (663.7–665.5) | 742.4 | 841.5 | 1488 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 665.7 µs (660.9–675.5) | 747.7 | 811.4 | 1482 | 0 |

## Discovery (`tools/list` and AST/legacy discover)

| Protocol | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| legacy (JSON-Lines, UDS) | 18.6 µs (18.5–18.9) | 23.2 | 31.0 | 51925 | 0 |
| AST (MessagePack, UDS) | 25.0 µs (24.3–25.5) | 35.8 | 42.9 | 38252 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 34.7 µs (33.5–35.0) | 40.2 | 45.3 | 28517 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 16.6 µs (16.4–18.8) | 25.0 | 32.2 | 53937 | 0 |

## Batch: 20 `system.noop` calls per group

Latency is the wall time of one group of 20 calls (the median is across repeats). Throughput is groups per second. MCP has no batch operation, so its row is 20 pipelined requests on one connection.

| Mode | p50 median (min–max) | p95 median | p99 median | groups/s median | errors (must be 0) |
|---|---|---|---|---|---|
| ast batch (one BATCH frame, 20 calls) | 202.6 µs (202.4–204.2) | 245.3 | 304.2 | 4821 | 0 |
| ast sequential (20 invoke, one after another) | 619.4 µs (615.5–627.7) | 753.0 | 1002.5 | 1574 | 0 |
| ast concurrent (20 invoke, 1 connection) | 170.2 µs (163.9–184.4) | 228.3 | 249.1 | 5895 | 0 |
| mcp-server pipelined (20 in flight; MCP has no batch) | 426.2 µs (399.7–427.6) | 517.9 | 723.2 | 2295 | 0 |

## Concurrency: closed loop on persistent connections

Each model keeps C requests in flight. AST multiplexes them on one connection or on C connections; legacy uses C connections; MCP pipelines C requests on one stdio connection, matching responses by id. The synthetic MCP server is sequential by construction, so its latency includes queueing. Throughput is successful operations divided by wall time.

| Model · concurrency · connections | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| ast: 1 multiplexed connection · C=1 · connections=1 | 21.8 µs (21.6–21.9) | 33.3 | 39.2 | 43545 | 0 |
| ast: C connections (1 in flight each) · C=1 · connections=1 | 21.7 µs (21.6–22.2) | 32.1 | 38.7 | 43656 | 0 |
| legacy: C connections (1 in flight each) · C=1 · connections=1 | 19.9 µs (19.7–20.3) | 29.7 | 37.7 | 48667 | 0 |
| mcp-server (real): 1 process, 1 stdio, pipelined · C=1 · connections=1 | 26.8 µs (26.7–27.0) | 32.8 | 47.4 | 32485 | 0 |
| mcp-like-server (synthetic, sequential server): 1 stdio, pipelined · C=1 · connections=1 | 10.8 µs (10.6–10.8) | 13.8 | 23.7 | 73424 | 0 |
| ast: 1 multiplexed connection · C=4 · connections=1 | 31.2 µs (30.6–32.3) | 50.0 | 61.5 | 120861 | 0 |
| ast: C connections (1 in flight each) · C=4 · connections=4 | 30.1 µs (29.5–32.8) | 50.4 | 63.2 | 123452 | 0 |
| legacy: C connections (1 in flight each) · C=4 · connections=4 | 29.0 µs (27.2–30.5) | 49.2 | 64.0 | 130343 | 0 |
| mcp-server (real): 1 process, 1 stdio, pipelined · C=4 · connections=1 | 86.4 µs (86.1–86.7) | 118.1 | 153.2 | 43333 | 0 |
| mcp-like-server (synthetic, sequential server): 1 stdio, pipelined · C=4 · connections=1 | 7.8 µs (7.6–9.5) | 17.5 | 20.0 | 351059 | 0 |
| ast: 1 multiplexed connection · C=16 · connections=1 | 116.2 µs (101.6–121.2) | 253.2 | 333.6 | 118738 | 0 |
| ast: C connections (1 in flight each) · C=16 · connections=16 | 111.4 µs (107.8–116.5) | 241.9 | 337.0 | 124415 | 0 |
| legacy: C connections (1 in flight each) · C=16 · connections=16 | 111.4 µs (107.0–112.3) | 262.8 | 366.2 | 122471 | 0 |
| mcp-server (real): 1 process, 1 stdio, pipelined · C=16 · connections=1 | 352.3 µs (351.3–353.5) | 466.5 | 606.4 | 43438 | 0 |
| mcp-like-server (synthetic, sequential server): 1 stdio, pipelined · C=16 · connections=1 | 32.8 µs (32.5–35.6) | 39.9 | 45.2 | 445827 | 0 |
| ast: 1 multiplexed connection · C=64 · connections=1 | 437.0 µs (408.4–458.1) | 785.2 | 941.0 | 135648 | 0 |
| ast: C connections (1 in flight each) · C=64 · connections=64 | 433.0 µs (384.1–467.9) | 794.5 | 953.5 | 129134 | 0 |
| legacy: C connections (1 in flight each) · C=64 · connections=64 | 441.9 µs (397.1–443.5) | 775.2 | 968.1 | 134809 | 0 |
| mcp-server (real): 1 process, 1 stdio, pipelined · C=64 · connections=1 | 1422.1 µs (1410.3–1424.3) | 1869.8 | 2189.8 | 43624 | 0 |
| mcp-like-server (synthetic, sequential server): 1 stdio, pipelined · C=64 · connections=1 | 128.2 µs (127.2–129.7) | 140.4 | 149.8 | 484692 | 0 |

## Deployment comparison: one MCP server process per concurrent caller

**This table compares deployment models, not protocols.** Each row starts C separate `mcp-server` processes, each with its own stdio connection, and gives each one T/C sequential calls. It must not be read next to the persistent-connection tables above as a protocol comparison. The first throughput column excludes spawn and `initialize`; the second includes them, measured from the first spawn.

| Processes (C) | throughput excl. spawn+initialize (ops/s) | throughput incl. spawn+initialize (ops/s) | per-call p50 median (min–max) | p95 median | p99 median | errors (must be 0) |
|---|---|---|---|---|---|---|
| 1 | 32761 | 31423 | 29.2 µs (28.7–29.5) | 39.0 | 53.6 | 0 |
| 4 | 57542 | 54164 | 65.0 µs (63.3–67.3) | 110.2 | 137.7 | 0 |
| 16 | 71877 | 60368 | 213.9 µs (166.5–237.6) | 301.5 | 369.8 | 0 |
| 64 | 58442 | 37697 | 865.1 µs (838.5–1244.2) | 1430.3 | 1925.0 | 0 |

## Server CPU per call

CPU time of the server process (user + system, from `ps`) during a fixed number of sequential `system.noop` calls, divided by the calls. The benchmark client's own CPU is **not** included.

Resolution: `ps` reports cputime in ~10 ms steps, so each per-op value moves in steps of 10 ms / 20000 = 0.50 µs at this run's `--cpu-ops`. Equal min and max values are quantization, not stability.

| Server | server CPU µs/op median (min–max) | per-call p50 median (µs) | errors |
|---|---|---|---|
| legacy (JSON-Lines, UDS) | 19.0 µs (18.5–19.5) | 24.4 | 0 |
| AST (MessagePack, UDS) | 23.0 µs (23.0–23.5) | 32.5 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 42.0 µs (40.5–42.5) | 30.2 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 5.0 µs (5.0–5.0) | 13.2 | 0 |

## Peak resident memory per server

| Server | peak RSS (MiB) |
|---|---|
| legacy (JSON-Lines, UDS) | 8.5 |
| AST (MessagePack, UDS) | 9.0 |
| MCP real (`mcp-server`, rmcp, stdio) | 9.2 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 7.3 |

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

Reproduce with: `cargo build --release && cargo run --release --bin ast-bench`
