# Benchmark results

Archived outputs of `ast-bench` and `alloc-probe`. Files here are historical
and must not be edited; new runs go into a new dated directory.

| Path | What it is |
|---|---|
| `bench-2026-10-22.md`, `.json` | Generated at 4f716f1 (its `git_rev` field); committed in da7aa95. First run with the real MCP server (rmcp =3.6.0 over stdio). |
| `bench-2026-10-22-wire-opt.md`, `.json` | 26fe02d, wire-path optimization run; same harness, real MCP server as control. |
| `alloc-baseline.json` | Allocator calls per call before the wire-path change (`alloc-probe`). |
| `alloc-after-wire.json` | Allocator calls per call after the wire-path change (`alloc-probe`). Its `commit` field records aa0199c: it was measured with the code of 26fe02d applied to the working tree, before that commit was made. |
| `2026-10-22-13a577d/` | Pre-fix full run (`bench.md`, `bench.json`, `alloc.*`, `ENV.txt`). One error retained as evidence: RESOURCE_EXHAUSTED from the in-flight limit in the C=64 multiplexed cell. The error message was not recorded in this run (error sampling was added later); the attribution to the in-flight limit comes from the later stress runs in `stress-inflight-flow-control/`, whose error_samples carry the exact message. Root cause fixed in 34f06ed. |
| `ab-main-vs-hardened/` | Interleaved A/B, 3 rounds: fa4061d (main) vs the hardened tip, raw `ast-bench` JSON/MD/stderr, `run-ab.sh` and its own README. |
| `stress-inflight-flow-control/` | C=64 multiplexed stress runs, 4 before and 12 after the flow-control fix (`before/`, `after/`), with its own README. |
| `2026-10-22-34f06ed/` | Final full run (`bench.md`, `bench.json`, `alloc.md`, `alloc.json`, `ENV.txt`). Zero errors in every cell. `ENV.txt` lists the commands; "dirty paths: 1" is the untracked pre-fix result directory, so no source files were changed. |

## Reproducing a run

```sh
scripts/run-benchmarks.sh
```

The script builds release binaries, runs `ast-bench` and `alloc-probe`, and writes
`docs/results/<YYYY-MM-DD>-<shortsha>/` with the Markdown and JSON reports and an
`ENV.txt` (commit, dirty-path count, toolchain, CPU). It refuses to overwrite an
existing directory and does not commit. Extra arguments go to `ast-bench`, for example:

```sh
scripts/run-benchmarks.sh --only concurrency --concurrency-levels 64 --repeats 10
```

Methodology, fairness rules and threats to validity are in `docs/BENCHMARKS.md`.
