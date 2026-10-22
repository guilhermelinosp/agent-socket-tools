# AST vs MCP: engineering report (2026-10-22)

## (a) Status by theme

| Theme | Status | Evidence |
|---|---|---|
| Fair MCP baseline benchmarks | Done | docs/BENCHMARKS.md, docs/results/*, zero errors in every cell |
| BATCH and streaming bounds | Done | max_batch_size, max_batch_output_bytes, max_stream_bytes; tests in connection.rs/client.rs |
| Cancellation and deadlines | Done, with a documented limit | synchronous tools are not interrupted (docs/AST_PROTOCOL.md "Deadlines and cancellation"); no deterministic test for cancel-on-disconnect |
| Connection lifecycle limits | Done | idle timeout, write timeout, max connections, handshake timeout; slow-reader test |
| Protocol correctness | Done | stream sequence/total validated by Rust and Python clients; byte vectors in tests/ast_vectors.rs |
| Security | Documented, partially hardened | same-uid trust boundary, no per-tool authorization (docs/AST_PROTOCOL.md "Threat model and security") |
| Serialization and hot-path efficiency | Partly done | wire write coalescing and buffered reads; allocator calls per noop 18 → 16; further candidates not done (see below) |
| Cross-language interoperability | Python only | sdks/python, 23 tests against the real server |
| Optional MCP adapter | Done | src/bin/ast-mcp-adapter.rs, docs/MCP_ADAPTER.md |
| Tests and CI | Done | fmt, clippy, tests on Linux and macOS, audit, Python interop, ast-bench smoke test |
| Benchmark deliverables | Done | repeated runs, p50/p95/p99, JSON output, threats to validity |

## (b) Targets

- **2× lower p50 for small sequential calls** — NOT achieved (parity within noise at best, AST ping floor 19.3–22.0 µs).
- **≥2× throughput under concurrency vs one pipelined MCP connection** — achieved (2.1× to 2.8× for C≥4).
- **Lower server CPU per small call vs real MCP** — achieved (23.5 vs 41.0 µs).
- **Not vs the legacy JSON-Lines server** (18.5 µs).

## (c) Corrections to earlier claims

The ~76× concurrency and ~34× startup figures were artifacts of the earlier harness (synthetic MCP baseline, MCP as 20 separate processes) and are retracted. This report supersedes them.

## (d) Defects found by independent review and fixed

- Half-close cancelled in-flight responses (fixed, regression test).
- A streaming tool without a size produced an unusable STREAM_START (now an ERROR).
- The Python client desynchronised after protocol failures and had no timeout or byte cap (fixed).
- Zero-valued timeout flags were accepted (now rejected).

## (e) Not done (with reason)

- Inlining trivial tools to avoid spawn_blocking: would speed up the benchmark without helping real tools; an inline tool that blocks stalls the runtime.
- Client-side direct writes without a writer task: write_all is not cancel-safe.
- Idle/write timer and lock consolidation in the server: medium risk, not measured to matter.
- BATCH double serialization of each entry and the flattened BatchResultEntry encoding: batch-only cost, wire format change risk.
- Per-tool or per-client authorization.
- Other client languages.
- Attributing the −2 allocator calls.
- Re-running the C=64 per-connection cell that regressed 11%.
- Deterministic test for cancel-on-disconnect.
- CI result on main not yet observed.

## (f) How to reproduce

```sh
cargo build --release
cargo run --release --bin ast-bench -- --json-out bench.json   # full run; Markdown on stdout
cargo run --release --bin ast-bench -- --quick --json-out bench-quick.json   # smoke test
cargo run --release --bin alloc-probe
```

See docs/BENCHMARKS.md for the methodology, fairness rules and threats to validity.
