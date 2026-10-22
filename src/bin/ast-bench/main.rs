//! ast-bench: a reproducible, closed-loop benchmark comparing AST with the
//! legacy JSON-Lines protocol and two MCP servers: `mcp-server` (real MCP,
//! built on rmcp) and `mcp-like-server` (a synthetic stand-in, labelled as
//! such everywhere). Every protocol runs the same tool calls and checks the
//! same results. The Markdown report goes to stdout; `--json-out` writes the
//! machine-readable results.
//!
//! Run with: `cargo run --release --bin ast-bench -- --json-out bench.json`
//!
//! Modules: `procs` (server processes, RSS/CPU via ps), `clients` (protocol
//! clients and the closed-loop engine), `stats` (cells, runs, summaries),
//! `scenarios` (what is measured), `report` (Markdown and JSON).

mod clients;
mod procs;
mod report;
mod scenarios;
mod stats;

use agent_socket_tools::ast::client::{AstClient, AstClientConfig};
use clap::Parser;
use clients::{LegacyConn, McpStdio};
use procs::{wait_for_socket, ResourceSampler, SocketServer};
use scenarios::{Paths, Persistent};
use serde_json::Value;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(
    name = "ast-bench",
    version,
    about = "Reproducible AST vs legacy vs MCP benchmark"
)]
struct Cli {
    /// Measured operations per repeat (sequential cells).
    #[arg(long, default_value_t = 500)]
    iterations: usize,
    /// Discarded operations before each measured run.
    #[arg(long, default_value_t = 100)]
    warmup: usize,
    /// Measured repetitions per cell; medians are taken across them.
    #[arg(long, default_value_t = 5)]
    repeats: usize,
    /// Concurrency levels (in-flight requests), comma-separated.
    #[arg(long, value_delimiter = ',', default_value = "1,4,16,64")]
    concurrency_levels: Vec<usize>,
    /// Total operations per concurrency cell (split across its workers).
    #[arg(long, default_value_t = 2000)]
    total_ops: usize,
    /// Calls used to measure server CPU time per call.
    #[arg(long, default_value_t = 20000)]
    cpu_ops: usize,
    /// Write machine-readable JSON results to this path.
    #[arg(long)]
    json_out: Option<PathBuf>,
    /// Include raw per-operation latencies (microseconds) in the JSON.
    #[arg(long)]
    raw: bool,
    /// Smoke-test profile: iterations 50, warmup 10, repeats 2, total-ops 200,
    /// cpu-ops 2000, concurrency-levels 1,4,16. Overrides the values above.
    #[arg(long)]
    quick: bool,
}

/// The effective configuration of one run.
#[derive(Debug)]
pub struct Config {
    pub iterations: usize,
    pub warmup: usize,
    pub repeats: usize,
    pub total_ops: usize,
    pub cpu_ops: usize,
    pub levels: Vec<usize>,
    pub raw: bool,
    pub quick: bool,
    pub debug_build: bool,
    pub json_out: Option<PathBuf>,
}

impl Config {
    fn from_cli(cli: Cli) -> Result<Config, String> {
        let mut cfg = Config {
            iterations: cli.iterations,
            warmup: cli.warmup,
            repeats: cli.repeats,
            total_ops: cli.total_ops,
            cpu_ops: cli.cpu_ops,
            levels: cli.concurrency_levels,
            raw: cli.raw,
            quick: cli.quick,
            debug_build: cfg!(debug_assertions),
            json_out: cli.json_out,
        };
        if cfg.quick {
            cfg.iterations = 50;
            cfg.warmup = 10;
            cfg.repeats = 2;
            cfg.total_ops = 200;
            cfg.cpu_ops = 2000;
            cfg.levels = vec![1, 4, 16];
        }
        if cfg.iterations == 0 || cfg.repeats == 0 || cfg.total_ops == 0 || cfg.cpu_ops == 0 {
            return Err("--iterations, --repeats, --total-ops and --cpu-ops must be >= 1".into());
        }
        if cfg.levels.is_empty() || cfg.levels.contains(&0) {
            return Err(
                "--concurrency-levels must be non-empty and contain only values >= 1".into(),
            );
        }
        Ok(cfg)
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cfg = match Config::from_cli(Cli::parse()) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("ast-bench: {e}");
            return ExitCode::from(2);
        }
    };
    if cfg.debug_build {
        eprintln!("WARNING: DEBUG BUILD — numbers are not valid; use --release");
    }
    match run(&cfg).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ast-bench: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cfg: &Config) -> Result<(), String> {
    let fixtures = tempfile::tempdir().map_err(|e| e.to_string())?;
    let sockets = tempfile::tempdir().map_err(|e| e.to_string())?;
    std::fs::write(fixtures.path().join("tiny.txt"), "ok").map_err(|e| e.to_string())?;
    std::fs::write(fixtures.path().join("medium.txt"), "m".repeat(16 * 1024))
        .map_err(|e| e.to_string())?;
    std::fs::write(fixtures.path().join("large.txt"), "l".repeat(512 * 1024))
        .map_err(|e| e.to_string())?;

    eprintln!("starting persistent servers");
    let legacy_path = sockets.path().join("bench-legacy.sock");
    let ast_path = sockets.path().join("bench-ast.sock");
    let legacy_server = SocketServer::spawn("agent-tools-server", fixtures.path(), &legacy_path)?;
    let ast_server = SocketServer::spawn("ast-server", fixtures.path(), &ast_path)?;
    if !wait_for_socket(&legacy_path).await {
        return Err("agent-tools-server did not become ready".into());
    }
    if !wait_for_socket(&ast_path).await {
        return Err("ast-server did not become ready".into());
    }
    let legacy = LegacyConn::connect(&legacy_path).await?;
    let ast = Arc::new(
        AstClient::connect(&ast_path, AstClientConfig::default())
            .await
            .map_err(|e| e.to_string())?,
    );
    let mut mcp_real = McpStdio::spawn("mcp-server", fixtures.path())?;
    mcp_real.initialize().await?;
    let mut mcp_like = McpStdio::spawn("mcp-like-server", fixtures.path())?;
    mcp_like.initialize().await?;

    let samplers = [
        ("legacy", ResourceSampler::start(legacy_server.pid())),
        ("ast", ResourceSampler::start(ast_server.pid())),
        ("mcp-server", ResourceSampler::start(mcp_real.pid())),
        ("mcp-like-server", ResourceSampler::start(mcp_like.pid())),
    ];

    let paths = Paths {
        legacy: legacy_path,
        ast: ast_path,
    };
    let mut p = Persistent {
        legacy_server,
        ast_server,
        legacy,
        ast,
        mcp_real,
        mcp_like,
    };

    let mut entries = Vec::new();
    eprintln!("startup");
    entries.extend(scenarios::startup(cfg, fixtures.path(), sockets.path()).await);
    eprintln!("sequential latency");
    entries.extend(scenarios::noop_and_info(cfg, &mut p).await);
    entries.extend(scenarios::payloads(cfg, &mut p).await);
    entries.extend(scenarios::discovery(cfg, &mut p).await);
    eprintln!("batch");
    entries.extend(scenarios::batch(cfg, &mut p).await);
    eprintln!("concurrency");
    entries.extend(scenarios::concurrency(cfg, &mut p, &paths).await);
    eprintln!("deployment comparison");
    entries.extend(scenarios::deployment(cfg, fixtures.path()).await);
    eprintln!("server CPU");
    entries.extend(scenarios::cpu(cfg, &mut p).await);

    let peaks: Vec<(&str, u64)> = samplers
        .into_iter()
        .map(|(name, sampler)| (name, sampler.stop()))
        .collect();
    entries.extend(scenarios::peak_rss(&peaks));
    drop(p);

    let env = report::Environment::collect();
    print!("{}", report::markdown(&env, cfg, &entries));

    if let Some(path) = &cfg.json_out {
        let doc: Value = report::json_document(&env, cfg, &entries);
        let text = serde_json::to_string_pretty(&doc).map_err(|e| e.to_string())?;
        std::fs::write(path, text).map_err(|e| format!("cannot write {}: {e}", path.display()))?;
        eprintln!("wrote JSON results to {}", path.display());
    }
    Ok(())
}
