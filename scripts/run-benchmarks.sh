#!/usr/bin/env bash
# Reproducible benchmark run: builds release binaries, runs ast-bench and
# alloc-probe, and archives the results with the environment that produced
# them under docs/results/<YYYY-MM-DD>-<shortsha>/.
#
# Usage: scripts/run-benchmarks.sh [extra ast-bench args...]
#   e.g. scripts/run-benchmarks.sh --only concurrency --concurrency-levels 64 --repeats 10
#
# Refuses to overwrite an existing result directory. Does not commit or push.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

sha="$(git rev-parse --short HEAD)"
day="$(date +%Y-%m-%d)"
out_dir="docs/results/${day}-${sha}"

if [[ -e "$out_dir" ]]; then
    echo "run-benchmarks: $out_dir already exists; refusing to overwrite." >&2
    exit 1
fi

dirty_files="$(git status --porcelain | wc -l | tr -d ' ')"
if [[ "$dirty_files" != "0" ]]; then
    echo "run-benchmarks: WARNING: working tree has $dirty_files changed path(s); results are not tied to a clean commit." >&2
fi

cpu_model() {
    if [[ "$(uname -s)" == "Darwin" ]]; then
        sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown
    else
        grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//' || echo unknown
    fi
}

cargo build --release

mkdir "$out_dir"

ast_bench_cmd=(./target/release/ast-bench --json-out "$out_dir/bench.json" "$@")
alloc_probe_cmd=(./target/release/alloc-probe --json-out "$out_dir/alloc.json")

echo "run-benchmarks: running ast-bench -> $out_dir/bench.md"
"${ast_bench_cmd[@]}" > "$out_dir/bench.md"

echo "run-benchmarks: running alloc-probe -> $out_dir/alloc.md"
"${alloc_probe_cmd[@]}" > "$out_dir/alloc.md"

{
    echo "date:        $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "git HEAD:    $(git rev-parse HEAD)"
    echo "dirty paths: $dirty_files (0 means a clean tree)"
    echo "rustc:       $(rustc --version)"
    echo "cargo:       $(cargo --version)"
    echo "uname:       $(uname -a)"
    echo "cpu:         $(cpu_model)"
    echo ""
    echo "commands:"
    echo "  cargo build --release"
    echo "  ${ast_bench_cmd[*]} > $out_dir/bench.md"
    echo "  ${alloc_probe_cmd[*]} > $out_dir/alloc.md"
} > "$out_dir/ENV.txt"

echo "run-benchmarks: done. Results in $out_dir (not committed)."
