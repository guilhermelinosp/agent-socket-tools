//! Reproducible benchmark: AST vs (1) this crate's own legacy JSON-Lines
//! protocol, both over Unix Domain Sockets, and (2) `mcp-like-server`
//! (`src/bin/mcp-like-server.rs`), a minimal, shape-faithful JSON-RPC
//! 2.0-over-stdio baseline implementing MCP's `initialize`/`tools/list`/
//! `tools/call` methods against the *exact same* `system.info`/
//! `filesystem.read` tools. **`mcp-like-server` is not the official MCP
//! reference implementation** — see its own doc comment and
//! `docs/AST_PROTOCOL.md`'s "MCP comparison" section for exactly what
//! comparing against it does and doesn't establish. All three baselines
//! do identical work in the same run on the same machine; only transport
//! and wire format differ.
//!
//! Run with: `cargo run --release --bin ast-bench`

use agent_socket_tools::ast::client::{AstClient, AstClientConfig};
use agent_socket_tools::protocol::{Operation, Request, Response, PROTOCOL_VERSION};
use agent_socket_tools::transport::MessageReader;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::process::{ChildStdin, Command as TokioCommand};

const ITERATIONS: usize = 500;
const CONCURRENCY: usize = 20;

struct Percentiles {
    p50: Duration,
    p95: Duration,
    p99: Duration,
}

fn percentiles(mut samples: Vec<Duration>) -> Percentiles {
    samples.sort();
    let at = |frac: f64| samples[((samples.len() as f64 - 1.0) * frac).round() as usize];
    Percentiles {
        p50: at(0.50),
        p95: at(0.95),
        p99: at(0.99),
    }
}

fn fmt_us(d: Duration) -> String {
    format!("{:.1}µs", d.as_secs_f64() * 1_000_000.0)
}

struct ResourceSampler {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    handle: std::thread::JoinHandle<(u64, f64)>, // (peak_rss_kb, avg_cpu_pct)
}

impl ResourceSampler {
    fn start(pid: u32) -> Self {
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_clone = stop.clone();
        let handle = std::thread::spawn(move || {
            let mut peak_rss_kb: u64 = 0;
            let mut cpu_samples = Vec::new();
            while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
                if let Ok(out) = Command::new("ps")
                    .args(["-o", "rss=,%cpu=", "-p", &pid.to_string()])
                    .output()
                {
                    let text = String::from_utf8_lossy(&out.stdout);
                    let mut parts = text.split_whitespace();
                    if let (Some(rss), Some(cpu)) = (parts.next(), parts.next()) {
                        if let Ok(rss_kb) = rss.parse::<u64>() {
                            peak_rss_kb = peak_rss_kb.max(rss_kb);
                        }
                        if let Ok(cpu_pct) = cpu.parse::<f64>() {
                            cpu_samples.push(cpu_pct);
                        }
                    }
                }
                std::thread::sleep(Duration::from_millis(40));
            }
            let avg_cpu = if cpu_samples.is_empty() {
                0.0
            } else {
                cpu_samples.iter().sum::<f64>() / cpu_samples.len() as f64
            };
            (peak_rss_kb, avg_cpu)
        });
        Self { stop, handle }
    }

    fn stop(self) -> (u64, f64) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        self.handle.join().unwrap_or((0, 0.0))
    }
}

// ---- Legacy JSON-Lines protocol client (minimal, for benchmarking) ----

async fn legacy_connect(path: &Path) -> UnixStream {
    UnixStream::connect(path).await.expect("connect legacy")
}

async fn legacy_roundtrip(stream: &mut UnixStream, request: &Request) -> Response {
    let mut line = serde_json::to_string(request).unwrap();
    line.push('\n');
    stream.write_all(line.as_bytes()).await.unwrap();
    let mut reader = MessageReader::new(4 * 1024 * 1024);
    let message = reader.next_message(stream).await.unwrap().unwrap();
    serde_json::from_str(&message).unwrap()
}

fn health_request(id: &str) -> Request {
    Request {
        version: PROTOCOL_VERSION,
        id: id.to_string(),
        op: Operation::Health,
        tool: None,
        args: None,
    }
}

fn read_request(id: &str, path: &str) -> Request {
    Request {
        version: PROTOCOL_VERSION,
        id: id.to_string(),
        op: Operation::Invoke,
        tool: Some("filesystem.read".to_string()),
        args: Some(serde_json::json!({ "path": path })),
    }
}

fn discover_request(id: &str) -> Request {
    Request {
        version: PROTOCOL_VERSION,
        id: id.to_string(),
        op: Operation::Discover,
        tool: None,
        args: None,
    }
}

// ---- Server process management ----

struct ServerProc {
    child: Child,
    socket_path: PathBuf,
}

impl Drop for ServerProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// `CARGO_BIN_EXE_*` is only set by Cargo for integration tests/benches
/// that *depend on* another binary target; a plain `[[bin]]` like this one
/// doesn't get it, since Cargo has no way to know at compile time that
/// this binary wants to shell out to its siblings. Resolved at runtime
/// instead: Cargo places every binary for a given profile in the same
/// directory, so this binary's own `current_exe()` with the filename
/// swapped finds them.
fn sibling_bin(name: &str) -> PathBuf {
    let mut path = std::env::current_exe().expect("current_exe");
    path.set_file_name(name);
    path
}

async fn start_legacy(read_dir: &Path, socket_dir: &Path) -> (ServerProc, Duration) {
    let socket_path = socket_dir.join("bench-legacy.sock");
    let started = Instant::now();
    let child = Command::new(sibling_bin("agent-tools-server"))
        .arg("--socket-path")
        .arg(&socket_path)
        .arg("--read-dir")
        .arg(read_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_for_socket(&socket_path).await;
    let startup = started.elapsed();
    (ServerProc { child, socket_path }, startup)
}

async fn start_ast(read_dir: &Path, socket_dir: &Path) -> (ServerProc, Duration) {
    let socket_path = socket_dir.join("bench-ast.sock");
    let started = Instant::now();
    let child = Command::new(sibling_bin("ast-server"))
        .arg("--socket-path")
        .arg(&socket_path)
        .arg("--read-dir")
        .arg(read_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_for_socket(&socket_path).await;
    let startup = started.elapsed();
    (ServerProc { child, socket_path }, startup)
}

async fn wait_for_socket(path: &Path) {
    for _ in 0..200 {
        if path.exists() && UnixStream::connect(path).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("server never came up at {path:?}");
}

// ---- mcp-like-server (JSON-RPC 2.0 over stdio) client ----

struct McpLikeProc {
    // Held only to keep the child alive and `kill_on_drop`-ed; never read
    // after spawn (stdin/stdout were taken out of it below).
    _child: tokio::process::Child,
    stdin: ChildStdin,
    stdout: BufReader<tokio::process::ChildStdout>,
    next_id: u64,
}

impl McpLikeProc {
    async fn call(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let request = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        let mut line = serde_json::to_string(&request).unwrap();
        line.push('\n');
        self.stdin.write_all(line.as_bytes()).await.unwrap();
        self.stdin.flush().await.unwrap();

        let mut response_line = String::new();
        self.stdout.read_line(&mut response_line).await.unwrap();
        serde_json::from_str(&response_line).unwrap()
    }

    async fn tools_call(&mut self, name: &str, args: Value) -> Value {
        self.call("tools/call", json!({ "name": name, "arguments": args }))
            .await
    }

    fn pid(&self) -> u32 {
        self._child.id().expect("child has not been reaped")
    }
}

async fn start_mcp_like(read_dir: &Path) -> (McpLikeProc, Duration) {
    let started = Instant::now();
    let mut child = TokioCommand::new(sibling_bin("mcp-like-server"))
        .arg(read_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let stdin = child.stdin.take().unwrap();
    let stdout = BufReader::new(child.stdout.take().unwrap());
    let mut proc = McpLikeProc {
        _child: child,
        stdin,
        stdout,
        next_id: 1,
    };
    // The "initialize" handshake real MCP clients perform before any
    // other call; `mcp-like-server` requires nothing from its result, but
    // sending it keeps this baseline's startup cost honest (a real
    // startup-time comparison needs to include it).
    let _ = proc
        .call(
            "initialize",
            json!({
                "protocolVersion": "2026-06-18",
                "capabilities": {},
                "clientInfo": { "name": "ast-bench", "version": "0.1.0" }
            }),
        )
        .await;
    let startup = started.elapsed();
    (proc, startup)
}

#[tokio::main]
async fn main() {
    println!("# AST vs legacy JSON-Lines vs mcp-like-server — benchmark report\n");
    println!("**`mcp-like-server` is a minimal, shape-faithful JSON-RPC 2.0-over-stdio baseline, not the official MCP reference implementation.** See `src/bin/mcp-like-server.rs`'s doc comment and `docs/AST_PROTOCOL.md`'s \"MCP comparison\" section for exactly what it does and doesn't establish.\n");
    print_environment();

    let fixture_dir = tempfile::tempdir().unwrap();
    std::fs::write(fixture_dir.path().join("tiny.txt"), "ok\n").unwrap();
    std::fs::write(fixture_dir.path().join("medium.txt"), "m".repeat(16 * 1024)).unwrap();
    std::fs::write(fixture_dir.path().join("large.txt"), "l".repeat(512 * 1024)).unwrap();

    let socket_dir = tempfile::tempdir().unwrap();
    let (legacy, legacy_startup) = start_legacy(fixture_dir.path(), socket_dir.path()).await;
    let (ast, ast_startup) = start_ast(fixture_dir.path(), socket_dir.path()).await;
    let (mut mcp, mcp_startup) = start_mcp_like(fixture_dir.path()).await;

    println!("## Startup time\n");
    println!("| Server | Time to usable connection |");
    println!("|---|---|");
    println!(
        "| agent-tools-server (legacy) | {:.2}ms |",
        legacy_startup.as_secs_f64() * 1000.0
    );
    println!(
        "| ast-server | {:.2}ms |",
        ast_startup.as_secs_f64() * 1000.0
    );
    println!(
        "| mcp-like-server (incl. `initialize`) | {:.2}ms |\n",
        mcp_startup.as_secs_f64() * 1000.0
    );

    let legacy_sampler = ResourceSampler::start(legacy.child.id());
    let ast_sampler = ResourceSampler::start(ast.child.id());
    let mcp_sampler = ResourceSampler::start(mcp.pid());

    bench_sequential_health(&legacy, &ast, &mut mcp).await;
    bench_persistent_vs_reconnect(&legacy, &ast).await;
    bench_payload_sizes(&legacy, &ast, &mut mcp).await;
    bench_concurrency(&legacy, &ast, fixture_dir.path()).await;
    bench_discover(&legacy, &ast, &mut mcp).await;
    print_encoded_sizes();

    let (legacy_peak_rss_kb, legacy_avg_cpu) = legacy_sampler.stop();
    let (ast_peak_rss_kb, ast_avg_cpu) = ast_sampler.stop();
    let (mcp_peak_rss_kb, mcp_avg_cpu) = mcp_sampler.stop();
    println!(
        "\n## Resident memory and CPU (sampled every ~40ms via `ps` across the whole run above)\n"
    );
    println!("| Server | Peak RSS | Avg CPU |");
    println!("|---|---|---|");
    println!(
        "| agent-tools-server (legacy) | {:.1} MiB | {:.1}% |",
        legacy_peak_rss_kb as f64 / 1024.0,
        legacy_avg_cpu
    );
    println!(
        "| ast-server | {:.1} MiB | {:.1}% |",
        ast_peak_rss_kb as f64 / 1024.0,
        ast_avg_cpu
    );
    println!(
        "| mcp-like-server | {:.1} MiB | {:.1}% |",
        mcp_peak_rss_kb as f64 / 1024.0,
        mcp_avg_cpu
    );

    println!("\n---\n");
    println!("Reproduce with: `cargo build --release && cargo run --release --bin ast-bench`");
}

fn print_environment() {
    let rustc = Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let uname = Command::new("uname")
        .arg("-a")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    println!("## Environment\n");
    println!("- {rustc}");
    println!("- {uname}");
    println!("- logical CPUs: {cpus}");
    println!("- iterations per scenario: {ITERATIONS}, concurrency: {CONCURRENCY}");
    println!(
        "- build profile: {}\n",
        if cfg!(debug_assertions) {
            "debug (use --release for real numbers)"
        } else {
            "release"
        }
    );
}

async fn bench_sequential_health(legacy: &ServerProc, ast: &ServerProc, mcp: &mut McpLikeProc) {
    println!("## No-op / health, sequential, persistent connection\n");
    println!("MCP has no dedicated no-op/health method in this baseline's implemented surface, so `tools/call system.info` (the cheapest real call available) stands in for it — this makes its number directly comparable to \"a round trip that does real work,\" not a true no-op, which the other two rows arguably understate relative to it for that reason.\n");
    println!("| Protocol | p50 | p95 | p99 | throughput |");
    println!("|---|---|---|---|---|");

    let mut stream = legacy_connect(&legacy.socket_path).await;
    let mut samples = Vec::with_capacity(ITERATIONS);
    let started = Instant::now();
    for i in 0..ITERATIONS {
        let t0 = Instant::now();
        let _ = legacy_roundtrip(&mut stream, &health_request(&i.to_string())).await;
        samples.push(t0.elapsed());
    }
    let elapsed = started.elapsed();
    let p = percentiles(samples);
    println!(
        "| legacy (JSON) | {} | {} | {} | {:.0} req/s |",
        fmt_us(p.p50),
        fmt_us(p.p95),
        fmt_us(p.p99),
        ITERATIONS as f64 / elapsed.as_secs_f64()
    );

    let client = AstClient::connect(&ast.socket_path, AstClientConfig::default())
        .await
        .unwrap();
    let mut samples = Vec::with_capacity(ITERATIONS);
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        let t0 = Instant::now();
        client.ping().await.unwrap();
        samples.push(t0.elapsed());
    }
    let elapsed = started.elapsed();
    let p = percentiles(samples);
    println!(
        "| AST (MessagePack) | {} | {} | {} | {:.0} req/s |",
        fmt_us(p.p50),
        fmt_us(p.p95),
        fmt_us(p.p99),
        ITERATIONS as f64 / elapsed.as_secs_f64()
    );
    client.close().await;

    let mut samples = Vec::with_capacity(ITERATIONS);
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        let t0 = Instant::now();
        let _ = mcp.tools_call("system.info", json!({})).await;
        samples.push(t0.elapsed());
    }
    let elapsed = started.elapsed();
    let p = percentiles(samples);
    println!(
        "| mcp-like (JSON-RPC/stdio) | {} | {} | {} | {:.0} req/s |\n",
        fmt_us(p.p50),
        fmt_us(p.p95),
        fmt_us(p.p99),
        ITERATIONS as f64 / elapsed.as_secs_f64()
    );
}

async fn bench_persistent_vs_reconnect(legacy: &ServerProc, ast: &ServerProc) {
    println!("## Persistent connection vs reconnect-per-call (health, 100 calls each)\n");
    println!("| Protocol | Mode | Total time | Per-call avg |");
    println!("|---|---|---|---|");

    const N: usize = 100;

    let mut stream = legacy_connect(&legacy.socket_path).await;
    let started = Instant::now();
    for i in 0..N {
        let _ = legacy_roundtrip(&mut stream, &health_request(&i.to_string())).await;
    }
    let persistent = started.elapsed();
    println!(
        "| legacy | persistent | {:.2}ms | {:.1}µs |",
        persistent.as_secs_f64() * 1000.0,
        persistent.as_secs_f64() * 1_000_000.0 / N as f64
    );

    let started = Instant::now();
    for i in 0..N {
        let mut s = legacy_connect(&legacy.socket_path).await;
        let _ = legacy_roundtrip(&mut s, &health_request(&i.to_string())).await;
    }
    let reconnect = started.elapsed();
    println!(
        "| legacy | reconnect-per-call | {:.2}ms | {:.1}µs |",
        reconnect.as_secs_f64() * 1000.0,
        reconnect.as_secs_f64() * 1_000_000.0 / N as f64
    );

    let client = AstClient::connect(&ast.socket_path, AstClientConfig::default())
        .await
        .unwrap();
    let started = Instant::now();
    for _ in 0..N {
        client.ping().await.unwrap();
    }
    let persistent_ast = started.elapsed();
    println!(
        "| AST | persistent | {:.2}ms | {:.1}µs |",
        persistent_ast.as_secs_f64() * 1000.0,
        persistent_ast.as_secs_f64() * 1_000_000.0 / N as f64
    );
    client.close().await;

    let started = Instant::now();
    for _ in 0..N {
        let c = AstClient::connect(&ast.socket_path, AstClientConfig::default())
            .await
            .unwrap();
        c.ping().await.unwrap();
        c.close().await;
    }
    let reconnect_ast = started.elapsed();
    println!(
        "| AST | reconnect-per-call (incl. HELLO) | {:.2}ms | {:.1}µs |\n",
        reconnect_ast.as_secs_f64() * 1000.0,
        reconnect_ast.as_secs_f64() * 1_000_000.0 / N as f64
    );
}

async fn bench_payload_sizes(legacy: &ServerProc, ast: &ServerProc, mcp: &mut McpLikeProc) {
    println!("## Payload sizes (filesystem.read, persistent connection, sequential)\n");
    println!("| Protocol | File | p50 | p95 |");
    println!("|---|---|---|---|");

    for (label, file) in [
        ("tiny (~2B)", "tiny.txt"),
        ("medium (16KiB)", "medium.txt"),
        ("large (512KiB)", "large.txt"),
    ] {
        let mut stream = legacy_connect(&legacy.socket_path).await;
        let mut samples = Vec::with_capacity(ITERATIONS);
        for i in 0..ITERATIONS {
            let t0 = Instant::now();
            let _ = legacy_roundtrip(&mut stream, &read_request(&i.to_string(), file)).await;
            samples.push(t0.elapsed());
        }
        let p = percentiles(samples);
        println!(
            "| legacy | {label} | {} | {} |",
            fmt_us(p.p50),
            fmt_us(p.p95)
        );

        let client = AstClient::connect(&ast.socket_path, AstClientConfig::default())
            .await
            .unwrap();
        let mut samples = Vec::with_capacity(ITERATIONS);
        for _ in 0..ITERATIONS {
            let t0 = Instant::now();
            client
                .invoke("filesystem.read", serde_json::json!({"path": file}), None)
                .await
                .unwrap();
            samples.push(t0.elapsed());
        }
        let p = percentiles(samples);
        println!("| AST | {label} | {} | {} |", fmt_us(p.p50), fmt_us(p.p95));
        client.close().await;

        let mut samples = Vec::with_capacity(ITERATIONS);
        for _ in 0..ITERATIONS {
            let t0 = Instant::now();
            let _ = mcp
                .tools_call("filesystem.read", json!({ "path": file }))
                .await;
            samples.push(t0.elapsed());
        }
        let p = percentiles(samples);
        println!(
            "| mcp-like | {label} | {} | {} |",
            fmt_us(p.p50),
            fmt_us(p.p95)
        );
    }
    println!();
}

async fn bench_concurrency(legacy: &ServerProc, ast: &ServerProc, read_dir: &Path) {
    println!("## Concurrent requests\n");
    println!("Legacy serves one request at a time *per connection*, so its only concurrency model is N separate connections. AST multiplexes N concurrent in-flight requests over a *single* connection; both are measured. `mcp-like-server`'s stdio transport is a single bidirectional pipe per process with no multi-client notion at all — its only way to serve N callers at once is N separate server *processes*, which is what's measured for it (process-spawn cost included, unlike the other two rows' connection-only cost).\n");
    println!("| Protocol | Concurrency model | Total time for {CONCURRENCY} calls | Throughput |");
    println!("|---|---|---|---|");

    let legacy_path = legacy.socket_path.clone();
    let started = Instant::now();
    let mut handles = Vec::new();
    for i in 0..CONCURRENCY {
        let path = legacy_path.clone();
        handles.push(tokio::spawn(async move {
            let mut s = legacy_connect(&path).await;
            legacy_roundtrip(&mut s, &health_request(&i.to_string())).await
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let elapsed = started.elapsed();
    println!(
        "| legacy | {CONCURRENCY} separate connections | {:.2}ms | {:.0} req/s |",
        elapsed.as_secs_f64() * 1000.0,
        CONCURRENCY as f64 / elapsed.as_secs_f64()
    );

    let ast_path = ast.socket_path.clone();
    let started = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..CONCURRENCY {
        let path = ast_path.clone();
        handles.push(tokio::spawn(async move {
            let c = AstClient::connect(&path, AstClientConfig::default())
                .await
                .unwrap();
            c.ping().await.unwrap();
            c.close().await;
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let elapsed = started.elapsed();
    println!(
        "| AST | {CONCURRENCY} separate connections | {:.2}ms | {:.0} req/s |",
        elapsed.as_secs_f64() * 1000.0,
        CONCURRENCY as f64 / elapsed.as_secs_f64()
    );

    let client = std::sync::Arc::new(
        AstClient::connect(&ast.socket_path, AstClientConfig::default())
            .await
            .unwrap(),
    );
    let started = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..CONCURRENCY {
        let client = std::sync::Arc::clone(&client);
        handles.push(tokio::spawn(async move { client.ping().await.unwrap() }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let elapsed = started.elapsed();
    println!(
        "| AST | {CONCURRENCY} multiplexed on 1 connection | {:.2}ms | {:.0} req/s |",
        elapsed.as_secs_f64() * 1000.0,
        CONCURRENCY as f64 / elapsed.as_secs_f64()
    );

    let started = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..CONCURRENCY {
        let dir = read_dir.to_path_buf();
        handles.push(tokio::spawn(async move {
            let (mut proc, _startup) = start_mcp_like(&dir).await;
            let _ = proc.tools_call("system.info", json!({})).await;
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let elapsed = started.elapsed();
    println!(
        "| mcp-like | {CONCURRENCY} separate processes (incl. spawn + initialize) | {:.2}ms | {:.0} req/s |\n",
        elapsed.as_secs_f64() * 1000.0,
        CONCURRENCY as f64 / elapsed.as_secs_f64()
    );
}

async fn bench_discover(legacy: &ServerProc, ast: &ServerProc, mcp: &mut McpLikeProc) {
    println!("## Discovery\n");
    println!("| Protocol | p50 | p95 |");
    println!("|---|---|---|");

    let mut stream = legacy_connect(&legacy.socket_path).await;
    let mut samples = Vec::with_capacity(ITERATIONS);
    for i in 0..ITERATIONS {
        let t0 = Instant::now();
        let _ = legacy_roundtrip(&mut stream, &discover_request(&i.to_string())).await;
        samples.push(t0.elapsed());
    }
    let p = percentiles(samples);
    println!("| legacy | {} | {} |", fmt_us(p.p50), fmt_us(p.p95));

    let client = AstClient::connect(&ast.socket_path, AstClientConfig::default())
        .await
        .unwrap();
    let mut samples = Vec::with_capacity(ITERATIONS);
    for _ in 0..ITERATIONS {
        let t0 = Instant::now();
        client.discover().await.unwrap();
        samples.push(t0.elapsed());
    }
    let p = percentiles(samples);
    println!("| AST | {} | {} |", fmt_us(p.p50), fmt_us(p.p95));
    client.close().await;

    let mut samples = Vec::with_capacity(ITERATIONS);
    for _ in 0..ITERATIONS {
        let t0 = Instant::now();
        let _ = mcp.call("tools/list", json!({})).await;
        samples.push(t0.elapsed());
    }
    let p = percentiles(samples);
    println!("| mcp-like | {} | {} |\n", fmt_us(p.p50), fmt_us(p.p95));
}

fn print_encoded_sizes() {
    use agent_socket_tools::ast::message::{Invoke, MessageType};
    use agent_socket_tools::ast::wire::HEADER_LEN;

    println!("## Encoded bytes per request (computed directly, not sniffed off the wire)\n");
    println!(
        "| Message | Legacy (JSON line incl. \\n) | AST (frame incl. header) | mcp-like (JSON-RPC line incl. \\n) |"
    );
    println!("|---|---|---|---|");

    let legacy_health = {
        let mut s = serde_json::to_string(&health_request("1")).unwrap();
        s.push('\n');
        s.len()
    };
    let ast_ping = HEADER_LEN
        + agent_socket_tools::ast::codec::encode(&agent_socket_tools::ast::message::Ping {})
            .unwrap()
            .len();
    let mcp_system_info = {
        let req = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"system.info","arguments":{}}});
        let mut s = serde_json::to_string(&req).unwrap();
        s.push('\n');
        s.len()
    };
    println!(
        "| health / ping / cheapest call | {legacy_health} bytes | {ast_ping} bytes | {mcp_system_info} bytes |"
    );

    let legacy_invoke = {
        let mut s = serde_json::to_string(&read_request("1", "medium.txt")).unwrap();
        s.push('\n');
        s.len()
    };
    let ast_invoke = HEADER_LEN
        + agent_socket_tools::ast::codec::encode(&Invoke {
            tool: "filesystem.read".to_string(),
            args: serde_json::json!({"path": "medium.txt"}),
            deadline_ms: None,
        })
        .unwrap()
        .len();
    let mcp_invoke = {
        let req = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"filesystem.read","arguments":{"path":"medium.txt"}}});
        let mut s = serde_json::to_string(&req).unwrap();
        s.push('\n');
        s.len()
    };
    println!(
        "| invoke filesystem.read (request only) | {legacy_invoke} bytes | {ast_invoke} bytes | {mcp_invoke} bytes |"
    );
    println!(
        "\nNote: `mcp-like-server` wraps every tool *result* as `{{\"content\":[{{\"type\":\"text\",\"text\":\"<JSON-stringified result>\"}}]}}` — matching real MCP's text-content convention for structured tool output — which means its response-side payload is JSON encoded, then that whole JSON string is itself embedded as one string field in a second, outer JSON object. This roughly doubles the response size relative to legacy's single-layer JSON, independent of anything this benchmark controls; it's an honest property of the convention being compared against, not an artifact of this baseline's implementation."
    );
    let _ = MessageType::Invoke;
}
