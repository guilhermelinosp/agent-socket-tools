# AST vs MCP: engineering report (2026-10-22, commit 34f06ed)

## (a) Scope and method

- Workload: local tool execution over persistent connections.
- Baseline: real MCP server built on rmcp =3.6.0 over stdio, serving the same tools. Release builds, Apple M3 Pro (macOS arm64), rustc 1.99.1.
- `ast-bench`: 500 iterations × 5 repeats after 100 warmup calls; medians across repeats. Interleaved A/B runs are used for regression checks.
- The synthetic `mcp-like-server` is never used as a comparison baseline.
- Single machine, single OS. Linux benchmarks were not run; Linux CI runs tests only.

## (b) Headline results

| Metric | AST | Real MCP | Result | Source |
|---|---|---|---|---|
| Sequential p50 (`system.noop`) | 30.1 µs | 31.0 µs | Tie (within noise) | 34f06ed bench.json; interleaved A/B: 30 vs 31 µs |
| Sequential p99 (`system.noop`) | 52.0 µs | 55.0 µs | Tie (single run) | 34f06ed |
| Throughput, concurrency 4 | 120,861 ops/s | 43,333 ops/s | 2.8× | 34f06ed |
| Throughput, concurrency 16 | 118,738 ops/s | 43,438 ops/s | 2.7× | 34f06ed |
| Throughput, concurrency 64 | 135,648 ops/s | 43,624 ops/s | 3.1× | 34f06ed |
| p99 under concurrency 16 | 333.6 µs | 606.4 µs | AST lower | 34f06ed |
| p99 under concurrency 64 | 941.0 µs | 2,189.8 µs | AST lower | 34f06ed |
| CPU per successful call (server, `ps`, 0.5 µs resolution) | 23.0 µs | 42.0 µs | AST −45% | 34f06ed |
| Allocator calls per call | 17 (1,713 B) | not measured | n/a | alloc.json (AST only) |
| Peak RSS over the run | 9,184 KiB | 9,392 KiB | Tie | 34f06ed |
| Memory under sustained load | not measured separately | not measured | n/a | peak RSS only |
| Startup to first completed call | 5.67 ms | 3.17 ms | AST slower | 34f06ed |
| Batch of 20 `system.noop` | 202.6 µs (BATCH frame); 170.2 µs (20 concurrent invokes); 619.4 µs (20 sequential) | 426.2 µs (20 pipelined; MCP has no batch) | AST BATCH 2.1× faster | 34f06ed |
| `filesystem.read` 2 B / 16 KiB / 512 KiB (p50) | 48.6 / 71.0 / 678.1 µs (512 KiB streamed: 658.5) | 48.7 / 74.3 / 664.5 µs | Tie | 34f06ed |

Concurrency 1: AST 43,545 vs MCP 32,485 ops/s (1.34×). Differences under ~7% are not distinguishable from noise (the MCP control moved up to 6.7% between runs). Client-observed latency is what the benchmark reports; server processing time is not measured separately — only server CPU per call. Behaviour under slow consumers and overload is covered by tests (slow-reader, resource-exhaustion, flow-control tests), not by a benchmark.

## (c) Objectives

| Objective | Status | Evidence |
|---|---|---|
| ≥2× throughput vs real MCP under concurrency | Achieved for C≥4 (2.7–3.1×; 1.34× at C=1) | 34f06ed bench.json |
| Lower server CPU per call | Achieved (−45%) | 34f06ed bench.json |
| Lower memory | Not achieved as a win: tie on peak RSS (9.2 vs 9.4 MiB) | 34f06ed bench.json |
| No unexplained p99 regressions | Achieved; the earlier suspected C=64 per-connection regression was not reproducible as a code effect (interleaved A/B, main vs hardened servers, same client: −10% to +9% p50 swings with a 17% run-to-run spread in `main` itself; cause is run-to-run variance of that cell, mechanism not profiled) | interleaved A/B |
| Sequential latency competitive with real MCP | Achieved as parity (not a win; the 2× target for small sequential calls is unmet: AST `ping`, which does no tool work, costs 19.2 µs) | 34f06ed bench.json |
| Hardening cost | Not measurable: A/B showed −3%…+3% on every AST cell, MCP control −0.6%…+0.3% | interleaved A/B |

## (d) What was fixed in this iteration

| Area | Change |
|---|---|
| Tool execution capacity | A global blocking-slot pool; the slot is held until the tool really finishes, so deadline/cancel abuse can no longer accumulate threads. `--max-blocking-tasks`, `--max-deadline-ms`; deadline 0 never starts a tool. |
| Cooperative cancellation | `CancelToken`, `Tool::invoke_with_cancel`; filesystem.read honours it. |
| Disconnect semantics | Clean FIN drains; errors cancel; first failed write cancels the rest. |
| Graceful shutdown | Stop accepting, cancel in-flight work including half-closed connections; `--shutdown-grace-secs`. |
| BATCH | Holds at most one live blocking execution. |
| Flow control | The in-flight slot is returned by the writer just before the terminal frame is written, fixing a rare RESOURCE_EXHAUSTED for clients at exactly `max_in_flight`: 1 in 320,000 operations before, 0 in 960,000 after — consistent with the fix, not a proof. |
| filesystem.read | Descriptor-relative traversal with O_NOFOLLOW on every component; symlinks, `..` and absolute paths refused; FIFOs refused without blocking; bounded read; real race test. |
| Least privilege | `--tools` allowlist. |
| Earlier in the session | Write timeout; stream sequence/total validation in both clients; half-close regression; Python client timeouts and caps. |

## (e) Defects found by independent review and fixed

- Half-close cancelled in-flight responses.
- Streaming tool without a size produced an unusable STREAM_START.
- Python client desynchronised after protocol failures.
- Zero-valued timeout flags.
- Cancellation tests that did not prove propagation (now fail without the fix, verified by mutation).
- BATCH slot accumulation.
- Half-closed connections ignored shutdown.

## (f) CI

Remote CI for the previously merged `main` commit fa4061d: all 7 jobs passed (run 39869107197: fmt, audit, clippy and test on Linux and macOS, Python interop). The hardening stack (branches hardening/lifecycle → hardening/filesystem-bench → hardening/lifecycle-fixes → hardening/inflight-release, tip 34f06ed) was validated locally only (fmt, clippy -D warnings, 240 Rust tests, Python interop tests from the earlier iteration); its remote CI result is not yet observed.

## (g) Remaining risks and next steps

- Synchronous tools that ignore the cancel token hold an execution slot until they finish, so a few slow tools can make the server answer RESOURCE_EXHAUSTED to others (bounded, documented).
- A peer that disappears without FIN is only detected on the next failed write (inherent to Unix sockets).
- No per-client or per-tool authorization beyond the `--tools` allowlist; same-uid processes are equally trusted.
- Filesystem residual risks: hardlinks and mounts inside the tree; malicious same-uid processes.
- Some lifecycle tests rely on a 100 ms settle window and may be flaky on a slow CI.
- No deterministic test for a streamed chunk read hitting RESOURCE_EXHAUSTED.
- MCP allocator calls, memory under sustained load, server processing time and Linux benchmark numbers are not measured.
- Python is the only non-Rust client.
- The sequential-latency 2× target would need a different architecture (or inline execution of trivial tools, deliberately rejected as benchmark-only).

## (h) Corrections to earlier claims

- The ~76× concurrency and ~34× startup figures were harness artifacts and are retracted.
- The wire-path subsection of `docs/AST_PROTOCOL.md` reports 2.8× at C=4 (124,545 vs 43,803 ops/s) from a single run. The final run supersedes it: 2.8× at C=4 (120,861 vs 43,333 ops/s), 2.7× at C=16, 3.1× at C=64.

## (i) How to reproduce

```sh
scripts/run-benchmarks.sh                                    # full run; writes docs/results/<date>-<sha>/
cargo run --release --bin alloc-probe                        # allocator calls per call
cargo run --release --bin ast-bench -- --only concurrency --concurrency-levels 64 --repeats 40   # C=64 stress
```

Raw results: `docs/results/2026-10-22-34f06ed/` (final), `docs/results/2026-10-22-13a577d/` (pre-fix, one error retained). Index: `docs/results/README.md`. Methodology: `docs/BENCHMARKS.md`.
