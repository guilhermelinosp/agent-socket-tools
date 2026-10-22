//! Report output: Markdown (stdout) and JSON (`--json-out`).

use crate::scenarios::{AST, AST_PING, AST_STREAMED, LEGACY, MCP_LIKE, MCP_REAL};
use crate::stats::Entry;
use crate::Config;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::process::Command;

const METHODOLOGY: &str = "All latency cells are closed-loop: a measured run issues a fixed number of operations and waits for each response before issuing the next, or, for the concurrency cells, keeps a fixed window of C requests in flight on the same connection(s). Offered load is therefore set by how fast the server answers, not by a fixed request rate. Every cell first runs `--warmup` discarded operations of the same shape, then `--repeats` measured runs of `--iterations` operations. Each run reports p50/p95/p99 over successful operations only; the tables show the median of each percentile across repeats, and for p50 also the min–max range. Errors and timeouts are counted per cell and never enter the percentiles; the expected value is 0. Startup is measured separately (spawn to first completed `system.noop`, process killed after each repetition) and is never added to per-call latency. Server CPU is the CPU-time delta of the server process around a fixed number of noops, divided by the number of calls. Peak RSS is sampled with `ps` across the whole run.";

const NOT_MEASURED: &[&str] = &[
    "Allocations per call (covered by tests, not by this benchmark).",
    "Cancellation and deadline latency (covered by tests).",
    "Behaviour under a slow reader or back-pressure (covered by tests).",
    "Malformed or adversarial inputs (covered by tests).",
    "MCP over transports other than stdio (for example Streamable HTTP or SSE).",
    "Network transports (TCP, TLS) for any protocol.",
    "CPU of the benchmark client itself; server CPU only.",
    "Tools other than `system.noop`, `system.info` and `filesystem.read`, except discovery.",
    "Behaviour with many registered tools or large input schemas.",
];

const THREATS: &[&str] = &[
    "Single machine and a single session. Absolute numbers vary across hardware and runs; compare results within one run.",
    "UDS (legacy, AST) and stdio pipes (MCP) have different kernel costs. Part of any difference is transport, not protocol.",
    "The benchmark client runs on the same host and competes for CPU with the servers, which inflates latency under concurrency.",
    "The MCP client is a minimal JSON-RPC client written for this benchmark, not the rmcp client. It does less work than an SDK client would, which biases the comparison in MCP's favour; any AST advantage shown here is, if anything, understated.",
    "The AST client is this repository's SDK, with its own task and channel overhead. That biases the comparison against AST, the opposite direction.",
    "The tools are trivial (`system.noop`, `system.info`, a small read). Real tool work will dominate latency in production.",
    "`ps` has coarse resolution: cputime is ~10 ms, RSS is whole KiB sampled about every 40 ms, so a short memory peak can be missed.",
    "The MCP client does not send `notifications/initialized` after `initialize`. This is a benchmark simplification, not a conformance claim.",
    "The `mcp-like-server` is a synthetic stand-in, not a real MCP implementation, and is sequential by construction; its pipelined latency includes queueing.",
    "The AST server defaults to 64 in-flight requests per connection, which bounds the multiplexed concurrency level; C = 64 is the boundary of that model.",
    "Batch results compare a BATCH frame (AST) with 20 pipelined requests (MCP). MCP has no batch operation, so the MCP row is pipelining, not batching.",
    "Startup includes up to ~1 ms of socket-polling granularity.",
    "Pipelined throughput is not per-call cost. With C requests in flight, reads and writes are batched, so throughput can rise several-fold with C (the synthetic MCP server goes from about 73k to over 350k ops/s between C=1 and C=4 in the quick run). Compare pipelined rows only with rows at the same C.",
    "These results do not replace measurement under real load.",
];

/// Facts about the machine and build, collected at run time (best effort).
#[derive(Serialize)]
pub struct Environment {
    pub ast_bench_version: &'static str,
    pub rustc: String,
    pub uname: String,
    pub cpu_model: String,
    pub logical_cpus: usize,
    pub build_profile: &'static str,
    pub crate_versions: BTreeMap<String, String>,
    pub git_rev: Option<String>,
}

impl Environment {
    pub fn collect() -> Self {
        Environment {
            ast_bench_version: env!("CARGO_PKG_VERSION"),
            rustc: command_output("rustc", &["--version"]).unwrap_or_else(|| "unknown".into()),
            uname: command_output("uname", &["-a"]).unwrap_or_else(|| "unknown".into()),
            cpu_model: cpu_model(),
            logical_cpus: std::thread::available_parallelism().map_or(0, |n| n.get()),
            build_profile: if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            },
            crate_versions: lock_versions(&["rmcp", "rmp-serde", "tokio", "serde_json"]),
            git_rev: command_output(
                "git",
                &[
                    "-C",
                    env!("CARGO_MANIFEST_DIR"),
                    "rev-parse",
                    "--short",
                    "HEAD",
                ],
            ),
        }
    }
}

fn command_output(cmd: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(cmd).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!text.is_empty()).then_some(text)
}

fn cpu_model() -> String {
    let model = if cfg!(target_os = "macos") {
        command_output("sysctl", &["-n", "machdep.cpu.brand_string"])
    } else {
        std::fs::read_to_string("/proc/cpuinfo")
            .ok()
            .and_then(|text| {
                text.lines()
                    .find(|l| l.starts_with("model name"))
                    .and_then(|l| l.split_once(':'))
                    .map(|(_, v)| v.trim().to_string())
            })
    };
    model.unwrap_or_else(|| "unknown".into())
}

/// Reads exact versions of the named crates from `Cargo.lock` (best effort).
fn lock_versions(names: &[&str]) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let Ok(text) = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.lock"))
    else {
        return out;
    };
    let mut current: Option<String> = None;
    for line in text.lines() {
        if let Some(name) = line
            .strip_prefix("name = \"")
            .and_then(|s| s.strip_suffix('"'))
        {
            current = Some(name.to_string());
        } else if let Some(version) = line
            .strip_prefix("version = \"")
            .and_then(|s| s.strip_suffix('"'))
        {
            if let Some(name) = current.take() {
                if names.contains(&name.as_str()) {
                    out.insert(name, version.to_string());
                }
            }
        }
    }
    out
}

impl Config {
    pub fn to_json(&self) -> Value {
        json!({
            "iterations": self.iterations,
            "warmup": self.warmup,
            "repeats": self.repeats,
            "total_ops": self.total_ops,
            "cpu_ops": self.cpu_ops,
            "concurrency_levels": self.levels,
            "raw": self.raw,
            "quick": self.quick,
            "debug_build": self.debug_build,
            "json_out": self.json_out.as_ref().map(|p| p.display().to_string()),
            "only": self.only,
            "scenarios_run": self.selected(),
            "batch_group_size": 20,
            "payload_sizes_bytes": { "tiny": 2, "medium": 16 * 1024, "large": 512 * 1024 },
            "request_timeout_s": 10,
        })
    }
}

pub fn json_document(env: &Environment, cfg: &Config, entries: &[Entry]) -> Value {
    json!({
        "environment": env,
        "config": cfg.to_json(),
        "results": entries,
    })
}

/// The full Markdown report.
pub fn markdown(env: &Environment, cfg: &Config, entries: &[Entry]) -> String {
    let mut md = String::new();
    if cfg.debug_build {
        md.push_str("> **DEBUG BUILD — numbers are not valid; use --release**\n\n");
    }
    md.push_str("# ast-bench report\n\n");

    section_environment(&mut md, env, cfg);
    md.push_str("## Methodology\n\n");
    let _ = writeln!(md, "{METHODOLOGY}\n");

    let _ = writeln!(md, "**Scenarios run:** {}\n", cfg.selected().join(", "));

    if cfg.wants("startup") {
        section_startup(&mut md, entries);
    }
    if cfg.wants("noop") {
        section_sequential(&mut md, entries, "noop_sequential", "## No-op (`system.noop`), sequential, persistent connection",
            "The same call on every protocol. The AST ping row is the protocol floor and is not comparable to tool calls.");
    }
    if cfg.wants("info") {
        section_sequential(
            &mut md,
            entries,
            "info_sequential",
            "## Info (`system.info`), sequential, persistent connection",
            "",
        );
    }
    if cfg.wants("payload") {
        section_payloads(&mut md, entries);
    }
    if cfg.wants("discovery") {
        section_discovery(&mut md, entries);
    }
    if cfg.wants("batch") {
        section_batch(&mut md, entries);
    }
    if cfg.wants("concurrency") {
        section_concurrency(&mut md, entries);
    }
    if cfg.wants("deployment") {
        section_deployment(&mut md, entries);
    }
    if cfg.wants("cpu") {
        section_cpu(&mut md, entries, cfg);
    }
    if cfg.wants("rss") {
        section_rss(&mut md, entries);
    }

    md.push_str("## Not measured\n\n");
    for item in NOT_MEASURED {
        let _ = writeln!(md, "- {item}");
    }
    md.push_str("\n## Threats to validity\n\n");
    for item in THREATS {
        let _ = writeln!(md, "- {item}");
    }
    md.push_str("\n---\n\n");
    if cfg.only.is_empty() {
        md.push_str(
            "Reproduce with: `cargo build --release && cargo run --release --bin ast-bench`\n",
        );
    } else {
        let _ = writeln!(
            md,
            "Reproduce with: `cargo build --release && cargo run --release --bin ast-bench -- --only {}`\n",
            cfg.only.join(",")
        );
    }
    md
}

fn section_environment(md: &mut String, env: &Environment, cfg: &Config) {
    md.push_str("## Environment\n\n");
    let _ = writeln!(md, "- ast-bench: {}", env.ast_bench_version);
    let _ = writeln!(md, "- rustc: {}", env.rustc);
    let _ = writeln!(md, "- uname: {}", env.uname);
    let _ = writeln!(md, "- CPU: {}", env.cpu_model);
    let _ = writeln!(md, "- logical CPUs: {}", env.logical_cpus);
    let _ = writeln!(md, "- build profile: {}", env.build_profile);
    let versions: Vec<String> = env
        .crate_versions
        .iter()
        .map(|(k, v)| format!("{k} {v}"))
        .collect();
    let _ = writeln!(md, "- crate versions (Cargo.lock): {}", versions.join(", "));
    let _ = writeln!(
        md,
        "- git revision: {}",
        env.git_rev.as_deref().unwrap_or("unknown")
    );
    let _ = writeln!(
        md,
        "- parameters: iterations={}, warmup={}, repeats={}, total-ops={}, cpu-ops={}, concurrency-levels={}, raw={}, quick={}",
        cfg.iterations,
        cfg.warmup,
        cfg.repeats,
        cfg.total_ops,
        cfg.cpu_ops,
        cfg.levels.iter().map(usize::to_string).collect::<Vec<_>>().join(","),
        cfg.raw,
        cfg.quick
    );
    md.push('\n');
    md.push_str("Protocols: `legacy` = agent-tools-server (JSON-Lines, UDS); `ast` = ast-server (MessagePack, UDS, this repository's SDK); `mcp-server` = real MCP server on rmcp =3.6.0 (stdio); `mcp-like-server` = synthetic stand-in (stdio, not a real MCP implementation).\n\n");
}

fn label(protocol: &str) -> &'static str {
    match protocol {
        LEGACY => "legacy (JSON-Lines, UDS)",
        AST => "AST (MessagePack, UDS)",
        AST_STREAMED => "AST streamed (invoke_stream, UDS)",
        AST_PING => "AST ping (protocol floor, not comparable)",
        MCP_REAL => "MCP real (`mcp-server`, rmcp, stdio)",
        MCP_LIKE => "MCP synthetic (`mcp-like-server`, stdio, synthetic)",
        _ => "unknown",
    }
}

fn table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut out = format!("| {} |\n", headers.join(" | "));
    let _ = writeln!(out, "|{}|", vec!["---"; headers.len()].join("|"));
    for row in rows {
        let _ = writeln!(out, "| {} |", row.join(" | "));
    }
    out.push('\n');
    out
}

fn p50_cell(e: &Entry) -> String {
    match (
        e.stat("p50_us", "median"),
        e.stat("p50_us", "min"),
        e.stat("p50_us", "max"),
    ) {
        (Some(m), Some(lo), Some(hi)) => format!("{m:.1} µs ({lo:.1}–{hi:.1})"),
        _ => "n/a".to_string(),
    }
}

fn median_cell(e: &Entry, key: &str, decimals: usize) -> String {
    e.stat(key, "median")
        .map_or("n/a".to_string(), |v| format!("{v:.decimals$}"))
}

/// Latency-table row: p50 with range, p95/p99 medians, throughput, errors.
fn latency_row(first: String, e: &Entry) -> Vec<String> {
    vec![
        first,
        p50_cell(e),
        median_cell(e, "p95_us", 1),
        median_cell(e, "p99_us", 1),
        median_cell(e, "throughput_ops_s", 0),
        e.errors_total().to_string(),
    ]
}

/// Headers matching [`latency_row`]'s columns.
fn latency_headers(first: &str, throughput: &str) -> Vec<String> {
    [
        first,
        "p50 median (min–max)",
        "p95 median",
        "p99 median",
        throughput,
        "errors (must be 0)",
    ]
    .iter()
    .map(|h| h.to_string())
    .collect()
}

fn headers_ref(v: &[String]) -> Vec<&str> {
    v.iter().map(String::as_str).collect()
}

fn select<'a>(entries: &'a [Entry], scenario: &str) -> Vec<&'a Entry> {
    entries.iter().filter(|e| e.scenario == scenario).collect()
}

fn section_startup(md: &mut String, entries: &[Entry]) {
    md.push_str("## Startup (separate from per-call latency)\n\n");
    md.push_str("Spawn to first completed `system.noop`. MCP includes `initialize`; AST includes HELLO. Each repeat uses a fresh process.\n\n");
    let rows: Vec<Vec<String>> = select(entries, "startup")
        .into_iter()
        .map(|e| {
            vec![
                label(&e.protocol).to_string(),
                match (
                    e.stat("startup_ms", "median"),
                    e.stat("startup_ms", "min"),
                    e.stat("startup_ms", "max"),
                ) {
                    (Some(m), Some(lo), Some(hi)) => format!("{m:.2} ms ({lo:.2}–{hi:.2})"),
                    _ => "n/a".to_string(),
                },
                e.errors_total().to_string(),
            ]
        })
        .collect();
    md.push_str(&table(
        &["Server", "startup median (min–max)", "errors"],
        &rows,
    ));
}

fn section_sequential(md: &mut String, entries: &[Entry], scenario: &str, title: &str, note: &str) {
    let rows = select(entries, scenario);
    if rows.is_empty() {
        return;
    }
    let _ = writeln!(md, "{title}\n");
    if !note.is_empty() {
        let _ = writeln!(md, "{note}\n");
    }
    let table_rows: Vec<Vec<String>> = rows
        .into_iter()
        .map(|e| latency_row(label(&e.protocol).to_string(), e))
        .collect();
    md.push_str(&table(
        &headers_ref(&latency_headers("Protocol", "throughput median (ops/s)")),
        &table_rows,
    ));
}

fn section_payloads(md: &mut String, entries: &[Entry]) {
    md.push_str("## Payload sizes (`filesystem.read`, sequential, persistent connection)\n\n");
    for size in ["tiny", "medium", "large"] {
        let rows: Vec<Vec<String>> = select(entries, "payload_sequential")
            .into_iter()
            .filter(|e| e.params["size"] == size)
            .map(|e| latency_row(label(&e.protocol).to_string(), e))
            .collect();
        if rows.is_empty() {
            continue;
        }
        let _ = writeln!(md, "### {size}\n");
        md.push_str(&table(
            &headers_ref(&latency_headers("Protocol", "throughput median (ops/s)")),
            &rows,
        ));
    }
}

fn section_discovery(md: &mut String, entries: &[Entry]) {
    let rows = select(entries, "discovery");
    if rows.is_empty() {
        return;
    }
    md.push_str("## Discovery (`tools/list` and AST/legacy discover)\n\n");
    let table_rows: Vec<Vec<String>> = rows
        .into_iter()
        .map(|e| latency_row(label(&e.protocol).to_string(), e))
        .collect();
    md.push_str(&table(
        &headers_ref(&latency_headers("Protocol", "throughput median (ops/s)")),
        &table_rows,
    ));
}

fn section_batch(md: &mut String, entries: &[Entry]) {
    let rows = select(entries, "batch");
    if rows.is_empty() {
        return;
    }
    md.push_str("## Batch: 20 `system.noop` calls per group\n\n");
    md.push_str("Latency is the wall time of one group of 20 calls (the median is across repeats). Throughput is groups per second. MCP has no batch operation, so its row is 20 pipelined requests on one connection.\n\n");
    let table_rows: Vec<Vec<String>> = rows
        .into_iter()
        .map(|e| latency_row(e.params["mode"].as_str().unwrap_or("").to_string(), e))
        .collect();
    md.push_str(&table(
        &headers_ref(&latency_headers("Mode", "groups/s median")),
        &table_rows,
    ));
}

fn section_concurrency(md: &mut String, entries: &[Entry]) {
    let rows = select(entries, "concurrency");
    if rows.is_empty() {
        return;
    }
    md.push_str("## Concurrency: closed loop on persistent connections\n\n");
    md.push_str("Each model keeps C requests in flight. AST multiplexes them on one connection or on C connections; legacy uses C connections; MCP pipelines C requests on one stdio connection, matching responses by id. The synthetic MCP server is sequential by construction, so its latency includes queueing. Throughput is successful operations divided by wall time.\n\n");
    let table_rows: Vec<Vec<String>> = rows
        .into_iter()
        .map(|e| {
            let model = e.params["model"].as_str().unwrap_or("");
            let c = e.params["concurrency"].as_u64().unwrap_or(0);
            let conns = e.params["connections"].as_u64().unwrap_or(0);
            latency_row(format!("{model} · C={c} · connections={conns}"), e)
        })
        .collect();
    md.push_str(&table(
        &headers_ref(&latency_headers(
            "Model · concurrency · connections",
            "throughput median (ops/s)",
        )),
        &table_rows,
    ));
}

fn section_deployment(md: &mut String, entries: &[Entry]) {
    let rows = select(entries, "deployment_per_process");
    if rows.is_empty() {
        return;
    }
    md.push_str("## Deployment comparison: one MCP server process per concurrent caller\n\n");
    md.push_str("**This table compares deployment models, not protocols.** Each row starts C separate `mcp-server` processes, each with its own stdio connection, and gives each one T/C sequential calls. It must not be read next to the persistent-connection tables above as a protocol comparison. The first throughput column excludes spawn and `initialize`; the second includes them, measured from the first spawn.\n\n");
    let table_rows: Vec<Vec<String>> = rows
        .into_iter()
        .map(|e| {
            vec![
                e.params["processes"].to_string(),
                median_cell(e, "throughput_ops_s", 0),
                median_cell(e, "throughput_incl_spawn_ops_s", 0),
                p50_cell(e),
                median_cell(e, "p95_us", 1),
                median_cell(e, "p99_us", 1),
                e.errors_total().to_string(),
            ]
        })
        .collect();
    md.push_str(&table(
        &[
            "Processes (C)",
            "throughput excl. spawn+initialize (ops/s)",
            "throughput incl. spawn+initialize (ops/s)",
            "per-call p50 median (min–max)",
            "p95 median",
            "p99 median",
            "errors (must be 0)",
        ],
        &table_rows,
    ));
}

fn section_cpu(md: &mut String, entries: &[Entry], cfg: &Config) {
    let rows = select(entries, "server_cpu");
    if rows.is_empty() {
        return;
    }
    md.push_str("## Server CPU per call\n\n");
    md.push_str("CPU time of the server process (user + system, from `ps`) during a fixed number of sequential `system.noop` calls, divided by the calls. The benchmark client's own CPU is **not** included.\n\n");
    let _ = writeln!(
        md,
        "Resolution: `ps` reports cputime in ~10 ms steps, so each per-op value moves in steps of 10 ms / {} = {:.2} µs at this run's `--cpu-ops`. Equal min and max values are quantization, not stability.\n",
        cfg.cpu_ops,
        10_000.0 / cfg.cpu_ops as f64
    );
    let table_rows: Vec<Vec<String>> = rows
        .into_iter()
        .map(|e| {
            vec![
                label(&e.protocol).to_string(),
                match (
                    e.stat("server_cpu_us_per_op", "median"),
                    e.stat("server_cpu_us_per_op", "min"),
                    e.stat("server_cpu_us_per_op", "max"),
                ) {
                    (Some(m), Some(lo), Some(hi)) => format!("{m:.1} µs ({lo:.1}–{hi:.1})"),
                    _ => "n/a".to_string(),
                },
                median_cell(e, "p50_us", 1),
                e.errors_total().to_string(),
            ]
        })
        .collect();
    md.push_str(&table(
        &[
            "Server",
            "server CPU µs/op median (min–max)",
            "per-call p50 median (µs)",
            "errors",
        ],
        &table_rows,
    ));
}

fn section_rss(md: &mut String, entries: &[Entry]) {
    let rows = select(entries, "peak_rss");
    if rows.is_empty() {
        return;
    }
    md.push_str("## Peak resident memory per server\n\n");
    let table_rows: Vec<Vec<String>> = rows
        .into_iter()
        .map(|e| {
            let mib = e
                .stat("peak_rss_kib", "median")
                .map_or("n/a".to_string(), |kib| format!("{:.1}", kib / 1024.0));
            vec![label(&e.protocol).to_string(), mib]
        })
        .collect();
    md.push_str(&table(&["Server", "peak RSS (MiB)"], &table_rows));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_has_header_separator_and_rows() {
        let t = table(&["a", "b"], &[vec!["1".into(), "2".into()]]);
        assert_eq!(t, "| a | b |\n|---|---|\n| 1 | 2 |\n\n");
    }

    #[test]
    fn json_document_shape() {
        let cfg = Config {
            iterations: 1,
            warmup: 0,
            repeats: 1,
            total_ops: 1,
            cpu_ops: 1,
            levels: vec![1],
            raw: false,
            quick: false,
            debug_build: false,
            json_out: None,
            only: Vec::new(),
        };
        let env = Environment::collect();
        let doc = json_document(&env, &cfg, &[]);
        assert!(doc["environment"].is_object());
        assert!(doc["config"].is_object());
        assert!(doc["results"].is_array());
    }
}
