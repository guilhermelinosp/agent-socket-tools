# Interleaved A/B: main vs hardened tip

Each run is `ast-bench --only noop,concurrency --concurrency-levels 4,16,64 --repeats 7`.
Three rounds, alternating `main-N` and `tip-N` (`run-ab.sh`). Files: `<side>-<round>.json`
(full report), `.md` (table), `.err` (progress log), `progress.txt` (exit codes).

## What each side is

- **main**: the server binaries built from commit fa4061d (worktree build).
- **tip**: the server binaries of the hardened branch, built into `target/release` at the
  state of hardening/lifecycle-fixes (13a577d).

Both sides ran the same ast-bench client binary: the harness binary was copied from
`target/release` immediately before the runs, with no rebuild in between. The copy in the
scratchpad was not preserved in this repository, and `target/release/ast-bench` has been
rebuilt since, so its current hash cannot be used to verify this; the claim rests on the
order of operations, not on a stored hash.

Differences therefore reflect the server builds only. The `git_rev` field inside each JSON
is `git rev-parse` of the repository the run was started from; it reads 13a577d for all six
runs and does not identify the binaries.

`run-ab.sh` uses absolute paths from the original development machine
(`SCR` = scratchpad, `TIP` = repository checkout). Edit them before reusing it.

## Results

Numbers are computed from `summary.p50_us_median` of each run, averaged over the three
rounds per side. "Per round" is the same difference computed for each round pair.

| Cells | Round-mean p50 difference (tip vs main) | Per-round range |
|---|---|---|
| AST, concurrency C=4 and C=16, and sequential noop | −0.2% … +3.8% | −3.8% … +9.6% |
| AST, C=64 multiplexed / per-connection | −5.4% / −6.9% | −18.6% … +11.4% (per-connection) |
| Real MCP control (`mcp-server`), concurrency C=4, 16, 64 | −0.9% … +0.2% | −2.8% … +0.9% |

- The 17% run-to-run spread in `main` itself is confirmed for the AST per-connection C=64
  cell (max/min − 1 = 17.5% across its three rounds).
- Legacy JSON-Lines cells, for reference: −0.1%, +4.8%, +9.9% (C=4, 16, 64).
- With three rounds, differences of a few percent sit inside the observed round-to-round
  spread. The data do not show the hardened tip to be faster or slower.
