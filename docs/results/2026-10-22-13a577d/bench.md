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
- parameters: iterations=500, warmup=100, repeats=5, total-ops=2000, cpu-ops=20000, concurrency-levels=1,4,16,64, raw=false, quick=false

Protocols: `legacy` = agent-tools-server (JSON-Lines, UDS); `ast` = ast-server (MessagePack, UDS, this repository's SDK); `mcp-server` = real MCP server on rmcp =3.6.0 (stdio); `mcp-like-server` = synthetic stand-in (stdio, not a real MCP implementation).

## Methodology

All latency cells are closed-loop: a measured run issues a fixed number of operations and waits for each response before issuing the next, or, for the concurrency cells, keeps a fixed window of C requests in flight on the same connection(s). Offered load is therefore set by how fast the server answers, not by a fixed request rate. Every cell first runs `--warmup` discarded operations of the same shape, then `--repeats` measured runs of `--iterations` operations. Each run reports p50/p95/p99 over successful operations only; the tables show the median of each percentile across repeats, and for p50 also the min–max range. Errors and timeouts are counted per cell and never enter the percentiles; the expected value is 0. Startup is measured separately (spawn to first completed `system.noop`, process killed after each repetition) and is never added to per-call latency. Server CPU is the CPU-time delta of the server process around a fixed number of noops, divided by the number of calls. Peak RSS is sampled with `ps` across the whole run.

**Scenarios run:** startup, noop, info, payload, discovery, batch, concurrency, deployment, cpu, rss

## Startup (separate from per-call latency)

Spawn to first completed `system.noop`. MCP includes `initialize`; AST includes HELLO. Each repeat uses a fresh process.

| Server | startup median (min–max) | errors |
|---|---|---|
| legacy (JSON-Lines, UDS) | 6.18 ms (5.96–12.06) | 0 |
| AST (MessagePack, UDS) | 5.61 ms (5.02–6.15) | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 3.06 ms (2.68–3.80) | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 2.05 ms (1.81–2.62) | 0 |

## No-op (`system.noop`), sequential, persistent connection

The same call on every protocol. The AST ping row is the protocol floor and is not comparable to tool calls.

| Protocol | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| legacy (JSON-Lines, UDS) | 23.8 µs (20.4–24.7) | 37.7 | 68.9 | 40101 | 0 |
| AST (MessagePack, UDS) | 34.0 µs (30.8–46.6) | 84.0 | 129.0 | 23514 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 26.9 µs (23.8–32.3) | 64.0 | 100.5 | 27921 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 10.1 µs (9.5–14.5) | 21.0 | 43.2 | 76978 | 0 |
| AST ping (protocol floor, not comparable) | 13.3 µs (13.1–13.5) | 24.2 | 30.0 | 67926 | 0 |

## Info (`system.info`), sequential, persistent connection

| Protocol | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| legacy (JSON-Lines, UDS) | 17.6 µs (17.2–20.0) | 30.8 | 45.6 | 50590 | 0 |
| AST (MessagePack, UDS) | 24.7 µs (24.3–25.5) | 49.7 | 79.0 | 34573 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 23.5 µs (22.6–24.4) | 43.6 | 74.8 | 37018 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 11.3 µs (11.2–11.5) | 13.5 | 15.1 | 92294 | 0 |

## Payload sizes (`filesystem.read`, sequential, persistent connection)

### tiny

| Protocol | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| legacy (JSON-Lines, UDS) | 33.5 µs (32.6–34.8) | 50.5 | 80.6 | 28403 | 0 |
| AST (MessagePack, UDS) | 40.0 µs (38.6–40.3) | 67.0 | 102.1 | 22666 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 39.6 µs (38.8–42.3) | 65.3 | 160.6 | 22839 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 24.9 µs (23.8–31.1) | 38.3 | 141.4 | 33723 | 0 |

### medium

| Protocol | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| legacy (JSON-Lines, UDS) | 64.8 µs (63.3–65.1) | 132.4 | 237.2 | 12790 | 0 |
| AST (MessagePack, UDS) | 112.4 µs (53.6–151.1) | 296.3 | 490.1 | 6488 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 124.4 µs (107.4–131.7) | 217.6 | 267.7 | 7401 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 85.3 µs (84.2–175.7) | 156.6 | 237.5 | 10626 | 0 |

### large

| Protocol | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| legacy (JSON-Lines, UDS) | 10508.6 µs (10501.0–10527.3) | 10720.3 | 10805.5 | 95 | 0 |
| AST (MessagePack, UDS) | 671.5 µs (670.5–672.9) | 751.6 | 888.4 | 1476 | 0 |
| AST streamed (invoke_stream, UDS) | 654.5 µs (652.1–657.2) | 728.6 | 833.5 | 1508 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 654.2 µs (652.2–655.6) | 727.5 | 847.5 | 1513 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 650.9 µs (649.8–663.7) | 719.4 | 820.1 | 1524 | 0 |

## Discovery (`tools/list` and AST/legacy discover)

| Protocol | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| legacy (JSON-Lines, UDS) | 18.4 µs (18.3–18.7) | 21.2 | 29.5 | 54217 | 0 |
| AST (MessagePack, UDS) | 24.7 µs (24.1–26.2) | 34.0 | 40.8 | 39216 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 34.0 µs (33.5–34.2) | 39.0 | 42.7 | 29091 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 16.2 µs (16.0–18.0) | 21.4 | 24.2 | 59341 | 0 |

## Batch: 20 `system.noop` calls per group

Latency is the wall time of one group of 20 calls (the median is across repeats). Throughput is groups per second. MCP has no batch operation, so its row is 20 pipelined requests on one connection.

| Mode | p50 median (min–max) | p95 median | p99 median | groups/s median | errors (must be 0) |
|---|---|---|---|---|---|
| ast batch (one BATCH frame, 20 calls) | 198.0 µs (197.5–199.3) | 232.9 | 300.4 | 4949 | 0 |
| ast sequential (20 invoke, one after another) | 605.6 µs (603.2–611.3) | 719.5 | 880.2 | 1613 | 0 |
| ast concurrent (20 invoke, 1 connection) | 190.2 µs (169.1–197.8) | 239.8 | 258.6 | 5339 | 0 |
| mcp-server pipelined (20 in flight; MCP has no batch) | 426.8 µs (424.7–431.4) | 500.2 | 784.9 | 2272 | 0 |

## Concurrency: closed loop on persistent connections

Each model keeps C requests in flight. AST multiplexes them on one connection or on C connections; legacy uses C connections; MCP pipelines C requests on one stdio connection, matching responses by id. The synthetic MCP server is sequential by construction, so its latency includes queueing. Throughput is successful operations divided by wall time.

| Model · concurrency · connections | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| ast: 1 multiplexed connection · C=1 · connections=1 | 21.7 µs (21.7–21.8) | 33.1 | 40.3 | 43579 | 0 |
| ast: C connections (1 in flight each) · C=1 · connections=1 | 21.6 µs (21.6–21.8) | 32.9 | 38.7 | 43905 | 0 |
| legacy: C connections (1 in flight each) · C=1 · connections=1 | 20.5 µs (20.5–20.7) | 29.9 | 36.2 | 47432 | 0 |
| mcp-server (real): 1 process, 1 stdio, pipelined · C=1 · connections=1 | 27.0 µs (27.0–27.0) | 31.7 | 60.1 | 32130 | 0 |
| mcp-like-server (synthetic, sequential server): 1 stdio, pipelined · C=1 · connections=1 | 10.9 µs (10.8–10.9) | 13.2 | 23.8 | 72701 | 0 |
| ast: 1 multiplexed connection · C=4 · connections=1 | 32.0 µs (31.1–33.0) | 53.6 | 64.2 | 117402 | 0 |
| ast: C connections (1 in flight each) · C=4 · connections=4 | 31.2 µs (30.2–32.7) | 52.9 | 64.5 | 121857 | 0 |
| legacy: C connections (1 in flight each) · C=4 · connections=4 | 29.5 µs (27.8–30.7) | 52.9 | 63.5 | 124871 | 0 |
| mcp-server (real): 1 process, 1 stdio, pipelined · C=4 · connections=1 | 86.7 µs (85.7–86.9) | 116.6 | 166.6 | 43173 | 0 |
| mcp-like-server (synthetic, sequential server): 1 stdio, pipelined · C=4 · connections=1 | 8.2 µs (7.8–8.9) | 17.1 | 20.3 | 351244 | 0 |
| ast: 1 multiplexed connection · C=16 · connections=1 | 116.6 µs (111.0–117.7) | 254.2 | 334.2 | 119962 | 0 |
| ast: C connections (1 in flight each) · C=16 · connections=16 | 113.8 µs (100.1–115.0) | 249.0 | 352.6 | 121562 | 0 |
| legacy: C connections (1 in flight each) · C=16 · connections=16 | 119.1 µs (106.4–128.7) | 229.3 | 296.0 | 127375 | 0 |
| mcp-server (real): 1 process, 1 stdio, pipelined · C=16 · connections=1 | 355.6 µs (349.5–362.5) | 427.2 | 698.3 | 43119 | 0 |
| mcp-like-server (synthetic, sequential server): 1 stdio, pipelined · C=16 · connections=1 | 31.6 µs (31.3–31.8) | 38.8 | 42.8 | 460237 | 0 |
| ast: 1 multiplexed connection · C=64 · connections=1 | 478.8 µs (428.3–491.5) | 865.6 | 1144.4 | 121197 | 1 |
| ast: C connections (1 in flight each) · C=64 · connections=64 | 492.2 µs (446.2–509.3) | 908.2 | 1185.8 | 119175 | 0 |
| legacy: C connections (1 in flight each) · C=64 · connections=64 | 398.8 µs (364.3–417.0) | 738.2 | 928.8 | 132079 | 0 |
| mcp-server (real): 1 process, 1 stdio, pipelined · C=64 · connections=1 | 1415.6 µs (1413.6–1422.2) | 1851.6 | 2046.9 | 43758 | 0 |
| mcp-like-server (synthetic, sequential server): 1 stdio, pipelined · C=64 · connections=1 | 132.5 µs (130.1–133.3) | 142.1 | 150.7 | 475365 | 0 |

## Deployment comparison: one MCP server process per concurrent caller

**This table compares deployment models, not protocols.** Each row starts C separate `mcp-server` processes, each with its own stdio connection, and gives each one T/C sequential calls. It must not be read next to the persistent-connection tables above as a protocol comparison. The first throughput column excludes spawn and `initialize`; the second includes them, measured from the first spawn.

| Processes (C) | throughput excl. spawn+initialize (ops/s) | throughput incl. spawn+initialize (ops/s) | per-call p50 median (min–max) | p95 median | p99 median | errors (must be 0) |
|---|---|---|---|---|---|---|
| 1 | 32853 | 31857 | 29.2 µs (29.0–29.3) | 37.7 | 51.3 | 0 |
| 4 | 56853 | 53017 | 66.4 µs (65.8–72.2) | 111.2 | 135.0 | 0 |
| 16 | 66490 | 56232 | 226.8 µs (199.5–232.2) | 309.8 | 344.5 | 0 |
| 64 | 60159 | 37852 | 879.5 µs (872.4–964.3) | 1213.0 | 1917.2 | 0 |

## Server CPU per call

CPU time of the server process (user + system, from `ps`) during a fixed number of sequential `system.noop` calls, divided by the calls. The benchmark client's own CPU is **not** included.

Resolution: `ps` reports cputime in ~10 ms steps, so each per-op value moves in steps of 10 ms / 20000 = 0.50 µs at this run's `--cpu-ops`. Equal min and max values are quantization, not stability.

| Server | server CPU µs/op median (min–max) | per-call p50 median (µs) | errors |
|---|---|---|---|
| legacy (JSON-Lines, UDS) | 19.0 µs (19.0–19.5) | 24.5 | 0 |
| AST (MessagePack, UDS) | 23.0 µs (22.5–23.0) | 32.2 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 41.5 µs (40.5–42.0) | 30.3 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 5.0 µs (5.0–5.0) | 13.2 | 0 |

## Peak resident memory per server

| Server | peak RSS (MiB) |
|---|---|
| legacy (JSON-Lines, UDS) | 9.1 |
| AST (MessagePack, UDS) | 9.2 |
| MCP real (`mcp-server`, rmcp, stdio) | 9.3 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 5.3 |

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
