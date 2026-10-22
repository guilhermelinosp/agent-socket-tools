//! The benchmark scenarios. Each function returns report entries and prints
//! nothing to stdout; progress and warnings go to stderr.

use crate::clients::{
    exec, sequential, AstHandle, AstStreamed, Expect, LegacyConn, McpStdio, Op, Target,
};
use crate::procs::{cpu_time_secs, wait_for_socket, SocketServer};
use crate::stats::{us, Cell, Entry, Run};
use crate::Config;
use agent_socket_tools::ast::client::{AstClient, AstClientConfig};
use agent_socket_tools::ast::message::{BatchInvoke, BatchOutcome};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Barrier;
use tokio::task::JoinSet;

pub const LEGACY: &str = "legacy";
pub const AST: &str = "ast";
pub const AST_STREAMED: &str = "ast-streamed";
pub const AST_PING: &str = "ast-ping";
pub const MCP_REAL: &str = "mcp-server";
pub const MCP_LIKE: &str = "mcp-like-server";

/// Calls per batch group (AST `batch` and MCP pipelined groups).
const GROUP: usize = 20;

/// Payloads read by `filesystem.read`: (label, file, bytes).
const PAYLOADS: [(&str, &str, usize); 3] = [
    ("tiny", "tiny.txt", 2),
    ("medium", "medium.txt", 16 * 1024),
    ("large", "large.txt", 512 * 1024),
];

/// The long-lived servers and connections used by most scenarios.
pub struct Persistent {
    pub legacy_server: SocketServer,
    pub ast_server: SocketServer,
    pub legacy: LegacyConn,
    pub ast: Arc<AstClient>,
    pub mcp_real: McpStdio,
    pub mcp_like: McpStdio,
}

/// Socket paths needed by scenarios that open their own connections.
pub struct Paths {
    pub legacy: PathBuf,
    pub ast: PathBuf,
}

fn note_failures(phase: &str, cell: &Cell) {
    if cell.errors > 0 {
        eprintln!(
            "  WARNING: {} {phase} errors; first: {}",
            cell.errors,
            cell.first_error.as_deref().unwrap_or("unknown")
        );
    }
}

fn progress(scenario: &str, protocol: &str, repeat: usize, repeats: usize) {
    eprintln!("[{scenario}] {protocol} repeat {repeat}/{repeats}");
}

/// Warmup (discarded), then `repeats` measured runs of `iterations` operations.
async fn measure<T: Target>(
    cfg: &Config,
    target: &mut T,
    op: &Op,
    scenario: &str,
    proto: &str,
) -> Vec<Run> {
    let mut runs = Vec::with_capacity(cfg.repeats);
    for r in 1..=cfg.repeats {
        progress(scenario, proto, r, cfg.repeats);
        note_failures("warmup", &sequential(target, op, cfg.warmup).await);
        let cell = sequential(target, op, cfg.iterations).await;
        note_failures("measured", &cell);
        runs.push(Run::from_cell(&cell, cfg.raw));
    }
    runs
}

/// AST `ping`, kept only as the protocol-floor row (not a tool call).
async fn ping_sequential(client: &AstClient, n: usize) -> Cell {
    let mut cell = Cell {
        start: Some(Instant::now()),
        ..Cell::default()
    };
    for _ in 0..n {
        let started = Instant::now();
        let outcome = client.ping().await.map(|_| ()).map_err(|e| e.to_string());
        cell.record(started, outcome);
    }
    cell.end = Some(Instant::now());
    cell
}

// ---- Startup (measured apart from per-call latency) ----

/// Spawn to first completed `system.noop`, with a fresh process per repeat
/// (killed afterwards). MCP includes `initialize`; AST includes HELLO.
pub async fn startup(cfg: &Config, fixtures: &Path, sockets: &Path) -> Vec<Entry> {
    let mut out = Vec::new();
    for proto in [LEGACY, AST, MCP_REAL, MCP_LIKE] {
        progress("startup", proto, 0, cfg.repeats);
        let mut runs = Vec::with_capacity(cfg.repeats);
        for _ in 0..cfg.repeats {
            match startup_once(proto, fixtures, sockets).await {
                Ok(ms) => runs.push(Run::scalar(1, 0, &[("startup_ms", ms)])),
                Err(e) => {
                    eprintln!("  WARNING: startup of {proto} failed: {e}");
                    runs.push(Run::scalar(0, 1, &[]));
                }
            }
        }
        out.push(Entry::new(
            "startup",
            proto,
            json!({ "measures": "spawn -> ready -> first system.noop completed", "unit": "ms" }),
            runs,
            false,
        ));
    }
    out
}

async fn startup_once(proto: &str, fixtures: &Path, sockets: &Path) -> Result<f64, String> {
    let started = Instant::now();
    match proto {
        LEGACY => {
            let path = sockets.join("startup-legacy.sock");
            let _server = SocketServer::spawn("agent-tools-server", fixtures, &path)?;
            if !wait_for_socket(&path).await {
                return Err("socket never became ready".to_string());
            }
            let mut conn = LegacyConn::connect(&path).await?;
            exec(&mut conn, &Op::noop()).await?;
        }
        AST => {
            let path = sockets.join("startup-ast.sock");
            let _server = SocketServer::spawn("ast-server", fixtures, &path)?;
            if !wait_for_socket(&path).await {
                return Err("socket never became ready".to_string());
            }
            let client = AstClient::connect(&path, AstClientConfig::default())
                .await
                .map_err(|e| e.to_string())?;
            exec(&mut AstHandle(Arc::new(client)), &Op::noop()).await?;
        }
        MCP_REAL | MCP_LIKE => {
            let bin = if proto == MCP_REAL {
                "mcp-server"
            } else {
                "mcp-like-server"
            };
            let mut proc = McpStdio::spawn(bin, fixtures)?;
            proc.initialize().await?;
            exec(&mut proc, &Op::noop()).await?;
        }
        other => return Err(format!("unknown protocol {other}")),
    }
    Ok(us(started.elapsed()) / 1000.0)
}

// ---- Sequential latency: noop, info, payloads, discovery ----

/// Same tool call on every protocol: `system.noop` (protocol floor) and
/// `system.info`. AST `ping` is reported as a separate, non-comparable row.
pub async fn noop_and_info(cfg: &Config, p: &mut Persistent) -> Vec<Entry> {
    let mut out = Vec::new();
    for (name, scenario, op) in [
        ("noop", "noop_sequential", Op::noop()),
        ("info", "info_sequential", Op::info()),
    ] {
        if !cfg.wants(name) {
            continue;
        }
        let params = json!({ "tool": op_tool(&op), "iterations": cfg.iterations, "connection": "persistent" });
        out.push(Entry::new(
            scenario,
            LEGACY,
            params.clone(),
            measure(cfg, &mut p.legacy, &op, scenario, LEGACY).await,
            cfg.raw,
        ));
        let mut ast = AstHandle(p.ast.clone());
        out.push(Entry::new(
            scenario,
            AST,
            params.clone(),
            measure(cfg, &mut ast, &op, scenario, AST).await,
            cfg.raw,
        ));
        out.push(Entry::new(
            scenario,
            MCP_REAL,
            params.clone(),
            measure(cfg, &mut p.mcp_real, &op, scenario, MCP_REAL).await,
            cfg.raw,
        ));
        out.push(Entry::new(
            scenario,
            MCP_LIKE,
            params,
            measure(cfg, &mut p.mcp_like, &op, scenario, MCP_LIKE).await,
            cfg.raw,
        ));
    }

    // The AST ping floor belongs to the no-op scenario's table.
    if cfg.wants("noop") {
        let mut runs = Vec::with_capacity(cfg.repeats);
        for r in 1..=cfg.repeats {
            progress("noop_sequential", AST_PING, r, cfg.repeats);
            let _ = ping_sequential(&p.ast, cfg.warmup).await;
            let cell = ping_sequential(&p.ast, cfg.iterations).await;
            runs.push(Run::from_cell(&cell, cfg.raw));
        }
        out.push(Entry::new(
            "noop_sequential",
            AST_PING,
            json!({ "op": "ping", "note": "protocol floor, not comparable to tool calls" }),
            runs,
            cfg.raw,
        ));
    }
    out
}

fn op_tool(op: &Op) -> &'static str {
    match op {
        Op::Call { tool, .. } => tool,
        Op::Discover => "discover",
    }
}

/// `filesystem.read` on every protocol, persistent connection, sequential.
/// AST is also measured with `invoke_stream` for the large payload.
pub async fn payloads(cfg: &Config, p: &mut Persistent) -> Vec<Entry> {
    let mut out = Vec::new();
    for (label, file, bytes) in PAYLOADS {
        let op = Op::read(file, bytes);
        let scenario = "payload_sequential";
        let params =
            json!({ "size": label, "file": file, "bytes": bytes, "tool": "filesystem.read" });
        out.push(Entry::new(
            scenario,
            LEGACY,
            params.clone(),
            measure(cfg, &mut p.legacy, &op, scenario, LEGACY).await,
            cfg.raw,
        ));
        let mut ast = AstHandle(p.ast.clone());
        out.push(Entry::new(
            scenario,
            AST,
            params.clone(),
            measure(cfg, &mut ast, &op, scenario, AST).await,
            cfg.raw,
        ));
        if label == "large" {
            let mut streamed = AstStreamed(p.ast.clone());
            out.push(Entry::new(
                scenario,
                AST_STREAMED,
                params.clone(),
                measure(cfg, &mut streamed, &op, scenario, AST_STREAMED).await,
                cfg.raw,
            ));
        }
        out.push(Entry::new(
            scenario,
            MCP_REAL,
            params.clone(),
            measure(cfg, &mut p.mcp_real, &op, scenario, MCP_REAL).await,
            cfg.raw,
        ));
        out.push(Entry::new(
            scenario,
            MCP_LIKE,
            params,
            measure(cfg, &mut p.mcp_like, &op, scenario, MCP_LIKE).await,
            cfg.raw,
        ));
    }
    out
}

/// Discovery on every protocol (AST discover, legacy discover, MCP tools/list).
pub async fn discovery(cfg: &Config, p: &mut Persistent) -> Vec<Entry> {
    let scenario = "discovery";
    let op = Op::Discover;
    let params = json!({ "expected_tools": crate::clients::EXPECTED_TOOLS });
    let mut ast = AstHandle(p.ast.clone());
    vec![
        Entry::new(
            scenario,
            LEGACY,
            params.clone(),
            measure(cfg, &mut p.legacy, &op, scenario, LEGACY).await,
            cfg.raw,
        ),
        Entry::new(
            scenario,
            AST,
            params.clone(),
            measure(cfg, &mut ast, &op, scenario, AST).await,
            cfg.raw,
        ),
        Entry::new(
            scenario,
            MCP_REAL,
            params.clone(),
            measure(cfg, &mut p.mcp_real, &op, scenario, MCP_REAL).await,
            cfg.raw,
        ),
        Entry::new(
            scenario,
            MCP_LIKE,
            params,
            measure(cfg, &mut p.mcp_like, &op, scenario, MCP_LIKE).await,
            cfg.raw,
        ),
    ]
}

// ---- Batch: groups of 20 noop calls ----

#[derive(Clone, Copy)]
enum BatchMode {
    AstBatch,
    AstSequential,
    AstConcurrent,
    McpPipelined,
}

impl BatchMode {
    fn id(self) -> &'static str {
        match self {
            BatchMode::AstBatch => "ast batch (one BATCH frame, 20 calls)",
            BatchMode::AstSequential => "ast sequential (20 invoke, one after another)",
            BatchMode::AstConcurrent => "ast concurrent (20 invoke, 1 connection)",
            BatchMode::McpPipelined => "mcp-server pipelined (20 in flight; MCP has no batch)",
        }
    }

    fn protocol(self) -> &'static str {
        match self {
            BatchMode::McpPipelined => MCP_REAL,
            _ => AST,
        }
    }
}

/// Runs one group of `GROUP` noop calls; the caller times the group.
async fn run_group(mode: BatchMode, p: &mut Persistent) -> Result<(), String> {
    match mode {
        BatchMode::AstBatch => {
            let calls: Vec<BatchInvoke> = (0..GROUP)
                .map(|i| BatchInvoke {
                    id: i as u64,
                    tool: "system.noop".to_string(),
                    args: json!({}),
                    deadline_ms: None,
                })
                .collect();
            let results = p.ast.batch(calls, None).await.map_err(|e| e.to_string())?;
            if results.len() != GROUP {
                return Err(format!("batch returned {} results", results.len()));
            }
            if results
                .iter()
                .all(|r| matches!(r.outcome, BatchOutcome::Ok { .. }))
            {
                Ok(())
            } else {
                Err("a batch entry failed".to_string())
            }
        }
        BatchMode::AstSequential => {
            let mut ast = AstHandle(p.ast.clone());
            let cell = sequential(&mut ast, &Op::noop(), GROUP).await;
            match cell.first_error {
                Some(e) => Err(e),
                None => Ok(()),
            }
        }
        BatchMode::AstConcurrent => {
            let mut set = JoinSet::new();
            for _ in 0..GROUP {
                let client = p.ast.clone();
                set.spawn(async move {
                    client
                        .invoke("system.noop", json!({}), None)
                        .await
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                });
            }
            let mut failure = None;
            while let Some(joined) = set.join_next().await {
                match joined {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => failure = Some(e),
                    Err(e) => failure = Some(e.to_string()),
                }
            }
            failure.map_or(Ok(()), Err)
        }
        BatchMode::McpPipelined => {
            let cell = p
                .mcp_real
                .pipelined(GROUP, GROUP, "system.noop", &json!({}), Expect::Empty)
                .await;
            match cell.first_error {
                Some(e) if cell.errors > 0 => Err(e),
                _ => Ok(()),
            }
        }
    }
}

async fn measure_batch(cfg: &Config, p: &mut Persistent, mode: BatchMode) -> Vec<Run> {
    let mut runs = Vec::with_capacity(cfg.repeats);
    for r in 1..=cfg.repeats {
        progress("batch", mode.id(), r, cfg.repeats);
        for _ in 0..cfg.warmup {
            let _ = run_group(mode, p).await;
        }
        let mut cell = Cell {
            start: Some(Instant::now()),
            ..Cell::default()
        };
        for _ in 0..cfg.iterations {
            let started = Instant::now();
            let outcome = run_group(mode, p).await;
            cell.record(started, outcome);
        }
        cell.end = Some(Instant::now());
        note_failures("batch", &cell);
        runs.push(Run::from_cell(&cell, cfg.raw));
    }
    runs
}

/// Latency is per group of 20 calls; throughput is groups per second.
pub async fn batch(cfg: &Config, p: &mut Persistent) -> Vec<Entry> {
    let mut out = Vec::new();
    for mode in [
        BatchMode::AstBatch,
        BatchMode::AstSequential,
        BatchMode::AstConcurrent,
        BatchMode::McpPipelined,
    ] {
        let params = json!({ "mode": mode.id(), "group_size": GROUP, "unit": "one group of 20 noop calls (group wall time)" });
        let runs = measure_batch(cfg, p, mode).await;
        out.push(Entry::new("batch", mode.protocol(), params, runs, cfg.raw));
    }
    out
}

// ---- Concurrency: closed loop with a fixed in-flight window ----

#[derive(Clone, Copy, PartialEq)]
enum Model {
    AstShared,
    AstConnections,
    LegacyConnections,
    McpReal,
    McpLike,
}

impl Model {
    fn id(self) -> &'static str {
        match self {
            Model::AstShared => "ast: 1 multiplexed connection",
            Model::AstConnections => "ast: C connections (1 in flight each)",
            Model::LegacyConnections => "legacy: C connections (1 in flight each)",
            Model::McpReal => "mcp-server (real): 1 process, 1 stdio, pipelined",
            Model::McpLike => "mcp-like-server (synthetic, sequential server): 1 stdio, pipelined",
        }
    }

    fn protocol(self) -> &'static str {
        match self {
            Model::AstShared | Model::AstConnections => AST,
            Model::LegacyConnections => LEGACY,
            Model::McpReal => MCP_REAL,
            Model::McpLike => MCP_LIKE,
        }
    }

    fn connections(self, c: usize) -> usize {
        match self {
            Model::AstShared | Model::McpReal | Model::McpLike => 1,
            Model::AstConnections | Model::LegacyConnections => c,
        }
    }
}

/// Splits `total` operations across `c` workers; the first `total % c` get one extra.
fn share(total: usize, c: usize, i: usize) -> usize {
    total / c + usize::from(i < total % c)
}

fn failed_cell(n: usize, message: String) -> Cell {
    let mut cell = Cell::default();
    for _ in 0..n {
        cell.fail(message.clone());
    }
    cell
}

async fn concurrent_cell(
    model: Model,
    c: usize,
    total: usize,
    p: &mut Persistent,
    paths: &Paths,
) -> Cell {
    match model {
        Model::McpReal => {
            p.mcp_real
                .pipelined(total, c, "system.noop", &json!({}), Expect::Empty)
                .await
        }
        Model::McpLike => {
            p.mcp_like
                .pipelined(total, c, "system.noop", &json!({}), Expect::Empty)
                .await
        }
        _ => spawn_workers(model, c, total, p.ast.clone(), paths).await,
    }
}

/// `c` tokio tasks, released together by a barrier; each performs its share
/// of the operations sequentially. Connection setup happens before the barrier.
async fn spawn_workers(
    model: Model,
    c: usize,
    total: usize,
    ast: Arc<AstClient>,
    paths: &Paths,
) -> Cell {
    let barrier = Arc::new(Barrier::new(c));
    let op = Op::noop();
    let mut set = JoinSet::new();
    for i in 0..c {
        let n = share(total, c, i);
        let barrier = barrier.clone();
        let op = op.clone();
        let ast = ast.clone();
        let legacy_path = paths.legacy.clone();
        let ast_path = paths.ast.clone();
        set.spawn(async move {
            match model {
                Model::AstShared => {
                    let mut target = AstHandle(ast);
                    barrier.wait().await;
                    sequential(&mut target, &op, n).await
                }
                Model::AstConnections => {
                    let connected = AstClient::connect(&ast_path, AstClientConfig::default())
                        .await
                        .map(Arc::new)
                        .map_err(|e| e.to_string());
                    barrier.wait().await;
                    match connected {
                        Ok(client) => sequential(&mut AstHandle(client), &op, n).await,
                        Err(e) => failed_cell(n, e),
                    }
                }
                Model::LegacyConnections => {
                    let connected = LegacyConn::connect(&legacy_path).await;
                    barrier.wait().await;
                    match connected {
                        Ok(mut conn) => sequential(&mut conn, &op, n).await,
                        Err(e) => failed_cell(n, e),
                    }
                }
                Model::McpReal | Model::McpLike => {
                    unreachable!("pipelined models run on the persistent process")
                }
            }
        });
    }
    let mut cells = Vec::with_capacity(c);
    while let Some(joined) = set.join_next().await {
        cells.push(joined.expect("worker task panicked"));
    }
    Cell::merge(cells)
}

/// For each concurrency level and model: `repeats` measured runs of
/// `total_ops` noop calls, each after a warmup of `warmup` calls.
pub async fn concurrency(cfg: &Config, p: &mut Persistent, paths: &Paths) -> Vec<Entry> {
    let mut out = Vec::new();
    for &c in &cfg.levels {
        for model in [
            Model::AstShared,
            Model::AstConnections,
            Model::LegacyConnections,
            Model::McpReal,
            Model::McpLike,
        ] {
            let scenario = "concurrency";
            let mut runs = Vec::with_capacity(cfg.repeats);
            for r in 1..=cfg.repeats {
                progress(scenario, model.id(), r, cfg.repeats);
                let _ = concurrent_cell(model, c, cfg.warmup, p, paths).await;
                let cell = concurrent_cell(model, c, cfg.total_ops, p, paths).await;
                note_failures("concurrency", &cell);
                runs.push(Run::from_cell(&cell, cfg.raw));
            }
            let params = json!({
                "model": model.id(),
                "concurrency": c,
                "connections": model.connections(c),
                "total_ops": cfg.total_ops,
                "tool": "system.noop",
            });
            out.push(Entry::new(
                scenario,
                model.protocol(),
                params,
                runs,
                cfg.raw,
            ));
        }
    }
    out
}

// ---- Deployment comparison: one MCP process per concurrent caller ----

/// `c` real `mcp-server` processes, each with its own stdio connection, doing
/// `total/c` sequential noops. Spawn and `initialize` happen before a barrier;
/// the throughput excluding them is `throughput_ops_s`, and the one including
/// them (from the first spawn) is `throughput_incl_spawn_ops_s`.
pub async fn deployment(cfg: &Config, fixtures: &Path) -> Vec<Entry> {
    let mut out = Vec::new();
    for &c in &cfg.levels {
        let mut runs = Vec::with_capacity(cfg.repeats);
        for r in 1..=cfg.repeats {
            progress("deployment_per_process", &format!("C={c}"), r, cfg.repeats);
            let _ = deployment_round(c, cfg.warmup, fixtures).await;
            let (cell, first_spawn) = deployment_round(c, cfg.total_ops, fixtures).await;
            note_failures("deployment", &cell);
            let incl_secs = cell.end.map_or(0.0, |end| {
                end.saturating_duration_since(first_spawn).as_secs_f64()
            });
            let mut run = Run::from_cell(&cell, cfg.raw);
            let incl = (incl_secs > 0.0).then(|| cell.ok as f64 / incl_secs);
            run.extra
                .insert("throughput_incl_spawn_ops_s".to_string(), json!(incl));
            runs.push(run);
        }
        let params = json!({
            "processes": c,
            "total_ops": cfg.total_ops,
            "tool": "system.noop",
            "note": "deployment model comparison, not a protocol comparison",
        });
        out.push(Entry::new(
            "deployment_per_process",
            MCP_REAL,
            params,
            runs,
            cfg.raw,
        ));
    }
    out
}

async fn deployment_round(c: usize, total: usize, fixtures: &Path) -> (Cell, Instant) {
    let barrier = Arc::new(Barrier::new(c));
    let mut set = JoinSet::new();
    for i in 0..c {
        let n = share(total, c, i);
        let barrier = barrier.clone();
        let dir = fixtures.to_path_buf();
        set.spawn(async move {
            let spawn_started = Instant::now();
            let proc = match McpStdio::spawn("mcp-server", &dir) {
                Err(e) => Err(e),
                Ok(mut proc) => match proc.initialize().await {
                    Ok(()) => Ok(proc),
                    Err(e) => Err(e),
                },
            };
            barrier.wait().await;
            let cell = match proc {
                Ok(mut proc) => sequential(&mut proc, &Op::noop(), n).await,
                Err(e) => failed_cell(n, e),
            };
            (cell, spawn_started)
        });
    }
    let mut cells = Vec::with_capacity(c);
    let mut first_spawn: Option<Instant> = None;
    while let Some(joined) = set.join_next().await {
        let (cell, spawned) = joined.expect("deployment task panicked");
        first_spawn = Some(first_spawn.map_or(spawned, |f| f.min(spawned)));
        cells.push(cell);
    }
    (Cell::merge(cells), first_spawn.unwrap_or_else(Instant::now))
}

// ---- Server CPU per call and peak RSS ----

/// CPU time of the server process around `cpu_ops` sequential noops, per op.
/// Client-side CPU of this benchmark is not included.
pub async fn cpu(cfg: &Config, p: &mut Persistent) -> Vec<Entry> {
    let legacy_pid = p.legacy_server.pid();
    let ast_pid = p.ast_server.pid();
    let mcp_real_pid = p.mcp_real.pid();
    let mcp_like_pid = p.mcp_like.pid();
    let mut ast = AstHandle(p.ast.clone());
    vec![
        Entry::new(
            "server_cpu",
            LEGACY,
            cpu_params(cfg),
            measure_cpu(cfg, &mut p.legacy, legacy_pid).await,
            cfg.raw,
        ),
        Entry::new(
            "server_cpu",
            AST,
            cpu_params(cfg),
            measure_cpu(cfg, &mut ast, ast_pid).await,
            cfg.raw,
        ),
        Entry::new(
            "server_cpu",
            MCP_REAL,
            cpu_params(cfg),
            measure_cpu(cfg, &mut p.mcp_real, mcp_real_pid).await,
            cfg.raw,
        ),
        Entry::new(
            "server_cpu",
            MCP_LIKE,
            cpu_params(cfg),
            measure_cpu(cfg, &mut p.mcp_like, mcp_like_pid).await,
            cfg.raw,
        ),
    ]
}

fn cpu_params(cfg: &Config) -> Value {
    json!({ "cpu_ops": cfg.cpu_ops, "tool": "system.noop", "resolution": "ps cputime, ~10 ms" })
}

async fn measure_cpu<T: Target>(cfg: &Config, target: &mut T, pid: u32) -> Vec<Run> {
    let op = Op::noop();
    let mut runs = Vec::with_capacity(cfg.repeats);
    for _ in 0..cfg.repeats {
        let _ = sequential(target, &op, cfg.warmup).await;
        let before = cpu_time_secs(pid);
        let cell = sequential(target, &op, cfg.cpu_ops).await;
        let after = cpu_time_secs(pid);
        let mut run = Run::from_cell(&cell, cfg.raw);
        match (before, after, cell.ok) {
            (Some(b), Some(a), ok) if ok > 0 => {
                let per_op = (a - b).max(0.0) * 1e6 / ok as f64;
                run.extra
                    .insert("server_cpu_us_per_op".to_string(), json!(per_op));
            }
            _ => run.errors += 1,
        }
        runs.push(run);
    }
    runs
}

/// Peak RSS of each persistent server over the whole run.
pub fn peak_rss(peaks: &[(&str, u64)]) -> Vec<Entry> {
    peaks
        .iter()
        .map(|(proto, kib)| {
            Entry::new(
                "peak_rss",
                proto,
                json!({ "sampling": "ps rss every ~40 ms across the whole run" }),
                vec![Run::scalar(0, 0, &[("peak_rss_kib", *kib as f64)])],
                false,
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shares_cover_total_exactly() {
        for (total, c) in [(2000, 64), (200, 16), (7, 4), (3, 16)] {
            let sum: usize = (0..c).map(|i| share(total, c, i)).sum();
            assert_eq!(sum, total);
        }
    }
}
