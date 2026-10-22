# ast-bench report

## Environment

- ast-bench: 0.1.0
- rustc: rustc 1.99.1 (58cf8f9c3 2026-10-13) (Homebrew)
- uname: Darwin Guilhermes-MacBook-Pro.local 27.0.0 Darwin Kernel Version 27.0.0: Tue Aug 11 21:06:24 PDT 2026; root:xnu-13432.1.9~1/RELEASE_ARM64_T6030 arm64
- CPU: Apple M3 Pro
- logical CPUs: 11
- build profile: release
- crate versions (Cargo.lock): rmcp 3.6.0, rmp-serde 1.3.1, serde_json 1.0.154, tokio 1.54.1
- git revision: 26fe02d
- parameters: iterations=500, warmup=100, repeats=5, total-ops=2000, cpu-ops=20000, concurrency-levels=1,4,16,64, raw=false, quick=false

Protocols: `legacy` = agent-tools-server (JSON-Lines, UDS); `ast` = ast-server (MessagePack, UDS, this repository's SDK); `mcp-server` = real MCP server on rmcp =3.6.0 (stdio); `mcp-like-server` = synthetic stand-in (stdio, not a real MCP implementation).

## Methodology

All latency cells are closed-loop: a measured run issues a fixed number of operations and waits for each response before issuing the next, or, for the concurrency cells, keeps a fixed window of C requests in flight on the same connection(s). Offered load is therefore set by how fast the server answers, not by a fixed request rate. Every cell first runs `--warmup` discarded operations of the same shape, then `--repeats` measured runs of `--iterations` operations. Each run reports p50/p95/p99 over successful operations only; the tables show the median of each percentile across repeats, and for p50 also the min–max range. Errors and timeouts are counted per cell and never enter the percentiles; the expected value is 0. Startup is measured separately (spawn to first completed `system.noop`, process killed after each repetition) and is never added to per-call latency. Server CPU is the CPU-time delta of the server process around a fixed number of noops, divided by the number of calls. Peak RSS is sampled with `ps` across the whole run.

## Startup (separate from per-call latency)

Spawn to first completed `system.noop`. MCP includes `initialize`; AST includes HELLO. Each repeat uses a fresh process.

| Server | startup median (min–max) | errors |
|---|---|---|
| legacy (JSON-Lines, UDS) | 9.16 ms (8.35–13.26) | 0 |
| AST (MessagePack, UDS) | 6.86 ms (5.86–8.62) | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 3.35 ms (2.83–4.36) | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 2.77 ms (2.51–3.39) | 0 |

## No-op (`system.noop`), sequential, persistent connection

The same call on every protocol. The AST ping row is the protocol floor and is not comparable to tool calls.

| Protocol | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| legacy (JSON-Lines, UDS) | 24.2 µs (23.7–24.8) | 34.3 | 54.7 | 39921 | 0 |
| AST (MessagePack, UDS) | 32.7 µs (31.7–34.0) | 47.9 | 60.6 | 29508 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 32.1 µs (31.8–32.5) | 39.5 | 61.0 | 30193 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 13.8 µs (13.7–13.9) | 16.8 | 20.7 | 70851 | 0 |
| AST ping (protocol floor, not comparable) | 19.3 µs (19.2–20.0) | 26.5 | 33.8 | 49880 | 0 |

## Info (`system.info`), sequential, persistent connection

| Protocol | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| legacy (JSON-Lines, UDS) | 23.9 µs (23.7–24.4) | 29.8 | 41.0 | 40546 | 0 |
| AST (MessagePack, UDS) | 32.3 µs (31.9–32.8) | 43.8 | 50.5 | 30428 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 31.0 µs (30.6–31.4) | 35.4 | 37.6 | 32078 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 13.9 µs (13.8–14.0) | 16.1 | 17.8 | 71204 | 0 |

## Payload sizes (`filesystem.read`, sequential, persistent connection)

### tiny

| Protocol | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| legacy (JSON-Lines, UDS) | 53.6 µs (52.9–54.2) | 66.0 | 89.4 | 18247 | 0 |
| AST (MessagePack, UDS) | 58.2 µs (57.5–59.7) | 74.5 | 86.4 | 16572 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 60.8 µs (59.9–62.0) | 72.0 | 91.4 | 16092 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 46.3 µs (46.2–46.5) | 52.3 | 57.5 | 21773 | 0 |

### medium

| Protocol | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| legacy (JSON-Lines, UDS) | 82.4 µs (82.0–82.6) | 97.9 | 110.2 | 11833 | 0 |
| AST (MessagePack, UDS) | 84.6 µs (84.5–85.0) | 97.2 | 105.5 | 11693 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 83.3 µs (79.9–83.4) | 94.5 | 111.2 | 11860 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 68.1 µs (67.9–68.4) | 74.8 | 93.7 | 14520 | 0 |

### large

| Protocol | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| legacy (JSON-Lines, UDS) | 10503.3 µs (10501.2–10539.1) | 10731.2 | 10811.0 | 95 | 0 |
| AST (MessagePack, UDS) | 654.8 µs (649.8–656.2) | 723.5 | 763.9 | 1507 | 0 |
| AST streamed (invoke_stream, UDS) | 660.1 µs (657.3–661.3) | 736.2 | 799.8 | 1501 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 636.1 µs (635.4–638.5) | 721.4 | 825.6 | 1548 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 625.2 µs (624.4–629.9) | 726.0 | 816.0 | 1571 | 0 |

## Discovery (`tools/list` and AST/legacy discover)

| Protocol | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| legacy (JSON-Lines, UDS) | 18.7 µs (18.6–18.8) | 21.8 | 29.4 | 52284 | 0 |
| AST (MessagePack, UDS) | 24.1 µs (24.0–24.7) | 34.5 | 39.7 | 39491 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 34.0 µs (33.8–34.2) | 40.4 | 53.0 | 28835 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 16.3 µs (16.1–16.5) | 20.5 | 23.7 | 59374 | 0 |

## Batch: 20 `system.noop` calls per group

Latency is the wall time of one group of 20 calls (the median is across repeats). Throughput is groups per second. MCP has no batch operation, so its row is 20 pipelined requests on one connection.

| Mode | p50 median (min–max) | p95 median | p99 median | groups/s median | errors (must be 0) |
|---|---|---|---|---|---|
| ast batch (one BATCH frame, 20 calls) | 199.5 µs (198.3–201.2) | 230.9 | 289.3 | 4915 | 0 |
| ast sequential (20 invoke, one after another) | 604.4 µs (597.9–614.2) | 722.1 | 897.5 | 1611 | 0 |
| ast concurrent (20 invoke, 1 connection) | 167.2 µs (166.2–170.9) | 226.9 | 241.9 | 6034 | 0 |
| mcp-server pipelined (20 in flight; MCP has no batch) | 425.5 µs (421.8–426.5) | 495.7 | 565.1 | 2318 | 0 |

## Concurrency: closed loop on persistent connections

Each model keeps C requests in flight. AST multiplexes them on one connection or on C connections; legacy uses C connections; MCP pipelines C requests on one stdio connection, matching responses by id. The synthetic MCP server is sequential by construction, so its latency includes queueing. Throughput is successful operations divided by wall time.

| Model · concurrency · connections | p50 median (min–max) | p95 median | p99 median | throughput median (ops/s) | errors (must be 0) |
|---|---|---|---|---|---|
| ast: 1 multiplexed connection · C=1 · connections=1 | 21.4 µs (21.4–21.6) | 28.9 | 36.3 | 44752 | 0 |
| ast: C connections (1 in flight each) · C=1 · connections=1 | 21.4 µs (21.4–21.5) | 32.0 | 37.0 | 44678 | 0 |
| legacy: C connections (1 in flight each) · C=1 · connections=1 | 20.1 µs (19.6–20.2) | 34.2 | 52.7 | 46171 | 0 |
| mcp-server (real): 1 process, 1 stdio, pipelined · C=1 · connections=1 | 27.0 µs (26.9–27.2) | 43.9 | 64.5 | 30883 | 0 |
| mcp-like-server (synthetic, sequential server): 1 stdio, pipelined · C=1 · connections=1 | 10.4 µs (5.3–10.5) | 12.2 | 15.3 | 77370 | 0 |
| ast: 1 multiplexed connection · C=4 · connections=1 | 30.9 µs (30.0–31.7) | 48.2 | 57.3 | 124545 | 0 |
| ast: C connections (1 in flight each) · C=4 · connections=4 | 29.9 µs (29.5–31.5) | 51.1 | 66.9 | 126213 | 0 |
| legacy: C connections (1 in flight each) · C=4 · connections=4 | 28.8 µs (28.5–29.8) | 51.5 | 66.8 | 127330 | 0 |
| mcp-server (real): 1 process, 1 stdio, pipelined · C=4 · connections=1 | 85.3 µs (84.5–85.9) | 114.2 | 144.4 | 43803 | 0 |
| mcp-like-server (synthetic, sequential server): 1 stdio, pipelined · C=4 · connections=1 | 7.9 µs (7.9–7.9) | 16.3 | 19.8 | 363741 | 0 |
| ast: 1 multiplexed connection · C=16 · connections=1 | 116.1 µs (110.1–118.8) | 245.4 | 342.0 | 121451 | 0 |
| ast: C connections (1 in flight each) · C=16 · connections=16 | 113.2 µs (100.5–114.6) | 258.0 | 341.4 | 121750 | 0 |
| legacy: C connections (1 in flight each) · C=16 · connections=16 | 107.7 µs (104.4–111.8) | 246.2 | 368.5 | 125935 | 0 |
| mcp-server (real): 1 process, 1 stdio, pipelined · C=16 · connections=1 | 351.5 µs (349.3–352.0) | 452.2 | 562.7 | 43743 | 0 |
| mcp-like-server (synthetic, sequential server): 1 stdio, pipelined · C=16 · connections=1 | 30.8 µs (30.5–31.1) | 38.2 | 43.6 | 474219 | 0 |
| ast: 1 multiplexed connection · C=64 · connections=1 | 460.5 µs (448.7–478.8) | 813.4 | 1017.3 | 126376 | 0 |
| ast: C connections (1 in flight each) · C=64 · connections=64 | 457.8 µs (419.6–473.2) | 788.8 | 986.2 | 131080 | 0 |
| legacy: C connections (1 in flight each) · C=64 · connections=64 | 445.0 µs (441.8–559.2) | 791.0 | 945.7 | 131814 | 0 |
| mcp-server (real): 1 process, 1 stdio, pipelined · C=64 · connections=1 | 1413.9 µs (1406.4–1424.5) | 1761.9 | 2125.6 | 43909 | 0 |
| mcp-like-server (synthetic, sequential server): 1 stdio, pipelined · C=64 · connections=1 | 129.8 µs (126.0–133.2) | 147.8 | 152.7 | 474669 | 0 |

## Deployment comparison: one MCP server process per concurrent caller

**This table compares deployment models, not protocols.** Each row starts C separate `mcp-server` processes, each with its own stdio connection, and gives each one T/C sequential calls. It must not be read next to the persistent-connection tables above as a protocol comparison. The first throughput column excludes spawn and `initialize`; the second includes them, measured from the first spawn.

| Processes (C) | throughput excl. spawn+initialize (ops/s) | throughput incl. spawn+initialize (ops/s) | per-call p50 median (min–max) | p95 median | p99 median | errors (must be 0) |
|---|---|---|---|---|---|---|
| 1 | 33209 | 32194 | 29.0 µs (29.0–29.4) | 36.2 | 48.7 | 0 |
| 4 | 56354 | 53317 | 65.9 µs (63.1–67.5) | 112.3 | 139.0 | 0 |
| 16 | 63626 | 54290 | 197.5 µs (177.5–226.8) | 274.9 | 331.2 | 0 |
| 64 | 58703 | 37733 | 860.9 µs (763.4–1017.3) | 1329.5 | 1915.8 | 0 |

## Server CPU per call

CPU time of the server process (user + system, from `ps`) during a fixed number of sequential `system.noop` calls, divided by the calls. The benchmark client's own CPU is **not** included.

Resolution: `ps` reports cputime in ~10 ms steps, so each per-op value moves in steps of 10 ms / 20000 = 0.50 µs at this run's `--cpu-ops`. Equal min and max values are quantization, not stability.

| Server | server CPU µs/op median (min–max) | per-call p50 median (µs) | errors |
|---|---|---|---|
| legacy (JSON-Lines, UDS) | 18.5 µs (18.5–18.5) | 24.0 | 0 |
| AST (MessagePack, UDS) | 23.5 µs (23.5–23.5) | 32.8 | 0 |
| MCP real (`mcp-server`, rmcp, stdio) | 41.0 µs (41.0–41.0) | 30.2 | 0 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 5.0 µs (4.5–5.0) | 13.0 | 0 |

## Peak resident memory per server

| Server | peak RSS (MiB) |
|---|---|
| legacy (JSON-Lines, UDS) | 6.9 |
| AST (MessagePack, UDS) | 9.5 |
| MCP real (`mcp-server`, rmcp, stdio) | 9.2 |
| MCP synthetic (`mcp-like-server`, stdio, synthetic) | 5.2 |

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
