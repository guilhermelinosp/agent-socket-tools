# Benchmarks

`ast-bench` compares AST with this repository's legacy JSON-Lines protocol and
with two MCP servers: `mcp-server` (real MCP, built on the official `rmcp`
SDK pinned to `=3.6.0`, over stdio) and `mcp-like-server` (a synthetic
stand-in, always labelled "synthetic"). Every protocol runs the same tool
calls and checks the same results.

## How to run

```sh
cargo build --release
cargo run --release --bin ast-bench -- --json-out bench.json   # full run; Markdown on stdout
cargo run --release --bin ast-bench -- --quick --json-out bench-quick.json   # smoke test
```

- The Markdown report goes to stdout. `--json-out PATH` writes the
  machine-readable results. `--raw` adds raw per-operation latencies (µs) to
  the JSON.
- `--quick` sets iterations 50, warmup 10, repeats 2, total-ops 200,
  cpu-ops 2000, concurrency levels 1,4,16. It overrides the other flags. Use
  it to check that the harness runs, not to produce numbers.
- Every option and its default is listed by `cargo run --bin ast-bench -- --help`.
- A debug build prints a `DEBUG BUILD — numbers are not valid` warning at the
  top of the report. Only `--release` numbers are meaningful.

Options: `--iterations` (500), `--warmup` (100), `--repeats` (5),
`--concurrency-levels` (1,4,16,64), `--total-ops` (2000, operations per
concurrency cell), `--cpu-ops` (20000), `--json-out`, `--raw`, `--quick`.

## Fairness rules

These rules are part of the benchmark's definition. A change that breaks one
of them is a change of methodology, not a tuning.

a. **Same call everywhere.** `noop` is the tool `system.noop`, `info` is
   `system.info`, and the payload is `filesystem.read`. AST uses
   `AstClient::invoke`, not `ping`. `ping` is reported as a separate row,
   "AST ping (protocol floor, not comparable)". Legacy uses the same invoke
   request. MCP uses `tools/call`.
b. **Four servers.** `agent-tools-server` (legacy, UDS), `ast-server` (UDS),
   `mcp-server` (real MCP, rmcp, stdio), `mcp-like-server` (synthetic, stdio).
c. **Warmup and repeats.** Each cell runs `--warmup` discarded operations, then
   `--repeats` measured runs. The table shows the median across repeats of
   p50, p95 and p99. For p50 it also shows the min–max range.
d. **Every result is checked.** A wrong result, an error or a timeout counts
   in `errors`. Its latency does not enter the percentiles. The expected
   `errors` value is 0 for every cell.
e. **Startup is separate.** Startup is the time from spawn to a completed
   first `system.noop`, measured `--repeats` times with a fresh process each
   time. MCP includes `initialize`; AST includes HELLO. Startup is never added
   to per-call latency.
f. **Concurrency is closed-loop with a fixed in-flight window C.** Each cell
   issues `--total-ops` `system.noop` calls.
   - AST, one multiplexed connection: C tasks share one `Arc<AstClient>`.
   - AST, C connections: one request in flight per connection.
   - Legacy, C connections: one request in flight per connection.
   - MCP real and synthetic: one process and one stdio connection, C requests
     pipelined. Responses are matched by JSON-RPC id, and latency is measured
     per request.
   
   The table "Deployment comparison: one MCP server process per concurrent
   caller" is separate. It starts C `mcp-server` processes, each with T/C
   sequential calls. It reports throughput with and without spawn and
   `initialize`. It compares deployment models, not protocols, and is never
   mixed with the persistent-connection tables.
g. **Batch.** A group is 20 `system.noop` calls. The compared modes are AST
   `batch`, AST 20 sequential invokes, AST 20 concurrent invokes on one
   connection, and MCP 20 pipelined requests. MCP has no batch operation.
   Latency is the wall time of one group, and throughput is groups per second.
h. **Payloads.** `filesystem.read` with tiny (2 B), medium (16 KiB) and large
   (512 KiB) files, sequential, on a persistent connection, for every protocol.
   AST is also measured with `invoke_stream` for the large file ("AST streamed").
i. **Discovery.** AST `discover`, legacy `discover`, MCP `tools/list` (real
   and synthetic). Each run checks that three tools are returned.
j. **Server CPU per call.** For each server, `--cpu-ops` sequential noops.
   CPU time is read from `ps -o cputime=` before and after and divided by the
   calls. The parser accepts Linux `[[dd-]hh:]mm:ss[.cc]` and macOS `M:SS.cc`.
   Resolution is about 10 ms, which is why `--cpu-ops` is large. The benchmark
   client's own CPU is not included.
k. **Peak RSS.** Each persistent server is sampled with `ps` every ~40 ms
   across the whole run, and the peak is reported per server.
l. **Not measured.** See the section below.

## Scenarios

| Scenario | What it measures |
|---|---|
| `startup` | Fresh-process startup per server (rule e). |
| `noop_sequential` / `info_sequential` | Sequential calls on a persistent connection (rule a). |
| `payload_sequential` | `filesystem.read` at three sizes, plus AST streamed for the large size (rule h). |
| `discovery` | `tools/list` and discover (rule i). |
| `batch` | Groups of 20 calls (rule g). |
| `concurrency` | Closed-loop with fixed in-flight window C (rule f). |
| `deployment_per_process` | One MCP process per caller, shown separately (rule f). |
| `server_cpu` | Server CPU per call (rule j). |
| `peak_rss` | Peak RSS per server (rule k). |

## Reading the JSON

The JSON document has three top-level keys:

- `environment`: rustc and uname output, CPU model, logical CPUs, build
  profile, versions of `rmcp`, `rmp-serde`, `tokio` and `serde_json` from
  `Cargo.lock`, and the git revision (best effort).
- `config`: every effective parameter of the run.
- `results`: one entry per scenario, protocol and parameter set.

Each entry has the shape below. The numbers are illustrative, not measurements:

```json
{
  "scenario": "noop_sequential",
  "protocol": "ast",
  "params": { "tool": "system.noop", "iterations": 500, "connection": "persistent" },
  "runs": [
    { "p50_us": 41.2, "p95_us": 60.3, "p99_us": 88.1, "throughput_ops_s": 22000.0, "ops": 500, "errors": 0 }
  ],
  "summary": {
    "p50_us_median": 41.2, "p50_us_min": 40.9, "p50_us_max": 42.0,
    "p95_us_median": 60.3, "p99_us_median": 88.1,
    "throughput_ops_s_median": 22000.0, "errors_total": 0
  }
}
```

- `runs` has one object per repeat. Scalar scenarios put their values in
  named fields. For example, `startup_ms` for startup, `server_cpu_us_per_op`
  for CPU, and `throughput_incl_spawn_ops_s` for the deployment comparison.
- `summary` has `<field>_median`, `<field>_min` and `<field>_max` for every
  numeric field, and `errors_total` and `ops_total`.
- With `--raw`, each entry also has `raw_us`: one array of latencies per run.
- Check `errors_total` first. Any value other than 0 invalidates the cell.

## Not measured

- Allocations per call (covered by tests, not by this benchmark).
- Cancellation and deadline latency (covered by tests).
- Behaviour under a slow reader or back-pressure (covered by tests).
- Malformed or adversarial inputs (covered by tests).
- MCP over transports other than stdio (for example Streamable HTTP or SSE).
- Network transports (TCP, TLS) for any protocol.
- CPU of the benchmark client itself. Only server CPU is reported.
- Tools other than `system.noop`, `system.info` and `filesystem.read`, except
  discovery.
- Behaviour with many registered tools or large input schemas.

## Threats to validity

- **Single machine and a single session.** Absolute numbers vary across
  hardware and runs. Compare results within one run.
- **Transport costs differ.** UDS (legacy, AST) and stdio pipes (MCP) have
  different kernel costs. Part of any difference is transport, not protocol.
- **Client and servers share the host.** The benchmark client competes for
  CPU with the servers, which inflates latency under concurrency.
- **The MCP client is minimal.** It is a JSON-RPC client written for this
  benchmark, not the rmcp client. It does less work than an SDK client would,
  which biases the comparison in MCP's favour. Any AST advantage shown is, if
  anything, understated.
- **The AST client is the repository's SDK.** It has its own task and channel
  overhead, which biases the comparison against AST.
- **Trivial tools.** `system.noop`, `system.info` and a small read. Real tool
  work dominates latency in production.
- **Coarse `ps` resolution.** CPU time is about 10 ms. RSS is whole KiB,
  sampled about every 40 ms, so short memory peaks can be missed.
- **No `notifications/initialized`.** The MCP client does not send this
  notification after `initialize`. This is a simplification, not a
  conformance claim.
- **Synthetic MCP server.** `mcp-like-server` is a stand-in, not a real MCP
  implementation. It is sequential by construction, so its pipelined latency
  includes queueing.
- **In-flight limit.** The AST server defaults to 64 in-flight requests per
  connection. That bounds the multiplexed model; C = 64 is the edge of it.
- **Batch is not like-for-like.** AST batch is one BATCH frame. The MCP row is
  pipelining, because MCP has no batch operation.
- **Startup granularity.** Startup includes up to about 1 ms of socket-polling
  granularity.
- **Pipelining is not per-call cost.** With C requests in flight, reads and
  writes are batched, so throughput can rise several-fold with C. Compare
  pipelined rows only with rows at the same C.
- **Not a load test.** These results do not replace measurement under real
  production load.

## Results

The full release run is stored in the repository:

- Markdown report: [`results/bench-2026-10-22.md`](results/bench-2026-10-22.md)
- Machine-readable JSON: [`results/bench-2026-10-22.json`](results/bench-2026-10-22.json)

Measured at commit `4f716f1` on an Apple M3 Pro (macOS arm64), rustc 1.99.1, with `rmcp =3.6.0`. Zero errors in every cell.

The interpretation and the target-by-target verdicts are in the
["MCP comparison"](AST_PROTOCOL.md#mcp-comparison) section of `docs/AST_PROTOCOL.md`.
