//! End-to-end tests for the real, compiled `ast-mcp-adapter` binary over
//! MCP stdio, forwarding to a real, compiled `ast-server` over its UDS.
//! Structure follows `tests/mcp_server_integration.rs` (MCP side) and
//! `tests/ast_integration.rs` (server side).

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::net::UnixStream;

struct McpProc {
    child: std::process::Child,
    stdin: std::process::ChildStdin,
    stdout: BufReader<std::process::ChildStdout>,
    next_id: u64,
}

impl Drop for McpProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl McpProc {
    fn start(socket_path: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_ast-mcp-adapter"))
            .arg("--socket-path")
            .arg(socket_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn ast-mcp-adapter");
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            stdin,
            stdout,
            next_id: 1,
        }
    }

    fn call(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let request = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        let mut line = serde_json::to_string(&request).unwrap();
        line.push('\n');
        self.stdin.write_all(line.as_bytes()).unwrap();
        self.stdin.flush().unwrap();

        let mut response_line = String::new();
        self.stdout.read_line(&mut response_line).unwrap();
        serde_json::from_str(&response_line).unwrap()
    }

    fn initialize(&mut self) -> Value {
        self.call(
            "initialize",
            json!({
                "protocolVersion": "2026-07-28",
                "capabilities": {},
                "clientInfo": { "name": "test", "version": "0.1" }
            }),
        )
    }
}

struct ServerHandle {
    child: Child,
    socket_path: PathBuf,
    _socket_dir: tempfile::TempDir,
    _read_dir: tempfile::TempDir,
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn wait_for_socket(path: &Path) {
    for _ in 0..100 {
        if path.exists() && UnixStream::connect(path).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("ast-server did not create a usable socket at {path:?} in time");
}

async fn start_ast_server() -> ServerHandle {
    let read_dir = tempfile::tempdir().unwrap();
    std::fs::write(read_dir.path().join("hello.txt"), "hello world\n").unwrap();

    let socket_dir = tempfile::tempdir().unwrap();
    let socket_path = socket_dir.path().join("ast-test.sock");

    let child = Command::new(env!("CARGO_BIN_EXE_ast-server"))
        .arg("--socket-path")
        .arg(&socket_path)
        .arg("--read-dir")
        .arg(read_dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn ast-server");

    wait_for_socket(&socket_path).await;

    ServerHandle {
        child,
        socket_path,
        _socket_dir: socket_dir,
        _read_dir: read_dir,
    }
}

/// Starts an adapter against `server` and completes MCP initialize.
fn start_adapter(server: &ServerHandle) -> McpProc {
    let mut proc = McpProc::start(&server.socket_path);
    proc.initialize();
    proc
}

#[tokio::test]
async fn initialize_returns_adapter_server_info() {
    let server = start_ast_server().await;
    let mut proc = McpProc::start(&server.socket_path);
    let resp = proc.initialize();
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 1);
    assert!(resp["result"]["protocolVersion"].is_string());
    assert_eq!(resp["result"]["serverInfo"]["name"], "ast-mcp-adapter");
}

#[tokio::test]
async fn tools_list_is_discovered_from_the_ast_server() {
    let server = start_ast_server().await;
    let mut proc = start_adapter(&server);
    let resp = proc.call("tools/list", json!({}));
    let tools = resp["result"]["tools"].as_array().unwrap();
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"system.noop"));
    assert!(names.contains(&"system.info"));
    assert!(names.contains(&"filesystem.read"));
    assert!(tools[0]["inputSchema"].is_object());
}

#[tokio::test]
async fn tools_call_filesystem_read_succeeds_for_authorized_file() {
    let server = start_ast_server().await;
    let mut proc = start_adapter(&server);
    let resp = proc.call(
        "tools/call",
        json!({ "name": "filesystem.read", "arguments": { "path": "hello.txt" } }),
    );
    assert_eq!(resp["result"]["isError"], false);
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let value: Value = serde_json::from_str(text).unwrap();
    assert_eq!(value["contents"], "hello world\n");
}

#[tokio::test]
async fn tools_call_rejects_traversal_as_an_error_result_not_a_protocol_error() {
    let server = start_ast_server().await;
    let mut proc = start_adapter(&server);
    let resp = proc.call(
        "tools/call",
        json!({ "name": "filesystem.read", "arguments": { "path": "../../etc/passwd" } }),
    );
    assert!(resp.get("error").is_none());
    assert_eq!(resp["result"]["isError"], true);
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains(':'), "expected 'CODE: message', got {text:?}");
}

#[tokio::test]
async fn tools_call_unknown_tool_is_a_jsonrpc_invalid_params_error() {
    let server = start_ast_server().await;
    let mut proc = start_adapter(&server);
    let resp = proc.call(
        "tools/call",
        json!({ "name": "does.not.exist", "arguments": {} }),
    );
    assert!(resp.get("result").is_none());
    assert_eq!(resp["error"]["code"], -32602);
}

#[tokio::test]
async fn tools_call_system_noop_succeeds() {
    let server = start_ast_server().await;
    let mut proc = start_adapter(&server);
    let resp = proc.call(
        "tools/call",
        json!({ "name": "system.noop", "arguments": {} }),
    );
    assert_eq!(resp["result"]["isError"], false);
}

#[tokio::test]
async fn exits_nonzero_when_no_ast_server_is_listening() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("no-such.sock");
    let output = Command::new(env!("CARGO_BIN_EXE_ast-mcp-adapter"))
        .arg("--socket-path")
        .arg(&missing)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to spawn ast-mcp-adapter");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("cannot connect"), "stderr: {stderr}");
}
