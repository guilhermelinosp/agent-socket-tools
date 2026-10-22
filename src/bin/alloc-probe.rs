//! Per-call allocation probe for the AST hot path.
//!
//! Runs an AST server and an `AstClient` in ONE process, so a single global
//! counting allocator sees both sides. Each scenario issues calls strictly
//! one at a time and reports the delta of the counters over N calls (after a
//! warmup). Client + server in one process, sequential; counts allocator
//! calls, not memory in use.

use agent_socket_tools::ast::client::{AstClient, AstClientConfig};
use agent_socket_tools::ast::connection::{run_connection, ConnectionConfig};
use agent_socket_tools::ast::message::{BatchInvoke, BatchOutcome, BatchResultEntry};
use agent_socket_tools::tools::{NoopTool, SystemInfoTool, ToolRegistry};
use agent_socket_tools::transport::BoundListener;
use clap::Parser;
use serde::Serialize;
use serde_json::json;
use std::alloc::{GlobalAlloc, Layout, System};
use std::error::Error;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;

/// Counts `alloc`/`alloc_zeroed`/`realloc` calls and the bytes they request.
/// `realloc` counts as one call adding `new_size` bytes. `dealloc` is not counted.
struct CountingAlloc {
    inner: System,
    calls: AtomicU64,
    bytes: AtomicU64,
}

impl CountingAlloc {
    fn record(&self, size: usize) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(size as u64, Ordering::Relaxed);
    }
}

// SAFETY: every method forwards to `System` with the caller's arguments
// unchanged; the only addition is relaxed atomic counter updates, which
// never touch the allocated memory.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.record(layout.size());
        self.inner.alloc(layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        self.record(layout.size());
        self.inner.alloc_zeroed(layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        self.record(new_size);
        self.inner.realloc(ptr, layout, new_size)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.inner.dealloc(ptr, layout)
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc {
    inner: System,
    calls: AtomicU64::new(0),
    bytes: AtomicU64::new(0),
};

fn snapshot() -> (u64, u64) {
    (
        GLOBAL.calls.load(Ordering::Relaxed),
        GLOBAL.bytes.load(Ordering::Relaxed),
    )
}

const MEASURED_CALLS: usize = 20_000;
const WARMUP_CALLS: usize = 2_000;
const BATCH_SIZE: usize = 20;

/// Allocator-level probe for AST's per-call cost.
#[derive(Parser, Debug)]
#[command(name = "alloc-probe", version)]
struct Cli {
    /// Write the results as JSON to this path, in addition to the stdout table.
    #[arg(long)]
    json_out: Option<PathBuf>,
}

#[derive(Serialize)]
struct Scenario {
    name: String,
    allocs_per_call: f64,
    bytes_per_call: f64,
}

#[derive(Serialize)]
struct Report<'a> {
    commit: String,
    scenarios: &'a [Scenario],
}

/// Runs `op` WARMUP_CALLS times, then MEASURED_CALLS times, and returns the
/// counter delta of the measured part divided by MEASURED_CALLS.
async fn measure<T, F, Fut>(name: &str, mut op: F) -> Result<Scenario, Box<dyn Error>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Box<dyn Error>>>,
{
    for _ in 0..WARMUP_CALLS {
        op().await?;
    }
    let (calls_before, bytes_before) = snapshot();
    for _ in 0..MEASURED_CALLS {
        op().await?;
    }
    let (calls_after, bytes_after) = snapshot();
    let n = MEASURED_CALLS as f64;
    Ok(Scenario {
        name: name.to_string(),
        allocs_per_call: (calls_after - calls_before) as f64 / n,
        bytes_per_call: (bytes_after - bytes_before) as f64 / n,
    })
}

/// Fails if any entry of a batch did not come back `Ok`, so a scenario that
/// quietly measures error paths cannot pass as a successful measurement.
fn check_batch(entries: &[BatchResultEntry]) -> Result<(), Box<dyn Error>> {
    if entries.len() != BATCH_SIZE {
        return Err(format!(
            "batch returned {} entries, expected {BATCH_SIZE}",
            entries.len()
        )
        .into());
    }
    for entry in entries {
        if !matches!(entry.outcome, BatchOutcome::Ok { .. }) {
            return Err(format!("batch entry {} was not Ok", entry.id).into());
        }
    }
    Ok(())
}

fn batch_calls() -> Vec<BatchInvoke> {
    (1..=BATCH_SIZE as u64)
        .map(|id| BatchInvoke {
            id,
            tool: "system.noop".to_string(),
            args: json!({}),
            deadline_ms: None,
        })
        .collect()
}

/// Same shape as `ast::connection`'s own serving loop, minus the connection
/// limiter (this probe opens exactly one connection).
fn start_server(listener: BoundListener, registry: Arc<ToolRegistry>) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok((stream, _addr)) = listener.accept().await {
            let registry = Arc::clone(&registry);
            tokio::spawn(async move {
                run_connection(stream, registry, ConnectionConfig::default()).await;
            });
        }
    })
}

fn git_commit() -> String {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn print_table(scenarios: &[Scenario]) {
    println!(
        "client + server in one process, sequential; counts allocator calls, not memory in use"
    );
    println!();
    println!("| cenário | allocs/call | bytes/call |");
    println!("| --- | ---: | ---: |");
    for s in scenarios {
        println!(
            "| {} | {:.2} | {:.2} |",
            s.name, s.allocs_per_call, s.bytes_per_call
        );
    }
}

fn write_json(path: &Path, scenarios: &[Scenario]) -> Result<(), Box<dyn Error>> {
    let report = Report {
        commit: git_commit(),
        scenarios,
    };
    let mut text = serde_json::to_string_pretty(&report)?;
    text.push('\n');
    std::fs::write(path, text)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    let socket_dir = tempfile::tempdir()?;
    let socket_path = socket_dir.path().join("probe.sock");

    let mut registry = ToolRegistry::new();
    registry.register(Box::new(SystemInfoTool));
    registry.register(Box::new(NoopTool));
    let listener = BoundListener::bind(&socket_path).await?;
    let server = start_server(listener, Arc::new(registry));

    let client = AstClient::connect(&socket_path, AstClientConfig::default()).await?;
    let c = &client;

    let mut scenarios = Vec::new();

    scenarios.push(
        measure("invoke system.noop", move || async move {
            c.invoke("system.noop", json!({}), None).await?;
            Ok::<(), Box<dyn Error>>(())
        })
        .await?,
    );

    scenarios.push(
        measure("invoke system.info", move || async move {
            c.invoke("system.info", json!({}), None).await?;
            Ok::<(), Box<dyn Error>>(())
        })
        .await?,
    );

    scenarios.push(
        measure("ping", move || async move {
            c.ping().await?;
            Ok::<(), Box<dyn Error>>(())
        })
        .await?,
    );

    let batch = measure("batch x20 system.noop (por batch)", move || async move {
        let entries = c.batch(batch_calls(), None).await?;
        check_batch(&entries)
    })
    .await?;
    let per_inner = Scenario {
        name: "batch x20 system.noop (por chamada interna)".to_string(),
        allocs_per_call: batch.allocs_per_call / BATCH_SIZE as f64,
        bytes_per_call: batch.bytes_per_call / BATCH_SIZE as f64,
    };
    scenarios.push(batch);
    scenarios.push(per_inner);

    client.close().await;
    server.abort();

    print_table(&scenarios);
    if let Some(path) = cli.json_out.as_deref() {
        write_json(path, &scenarios)?;
    }
    Ok(())
}
