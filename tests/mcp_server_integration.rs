//! End-to-end tests for the real, compiled `mcp-server` binary (built on
//! the official `rmcp` SDK, `=3.6.0`) over its actual stdio transport —
//! the fair-MCP-baseline analogue of `tests/mcp_like_integration.rs`
//! (which tests the synthetic hand-rolled fallback instead).

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

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
    fn start(read_dir: &std::path::Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_mcp-server"))
            .arg(read_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn mcp-server");
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

fn start_with_fixture() -> (McpProc, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("hello.txt"), "hello world\n").unwrap();
    let proc = McpProc::start(dir.path());
    (proc, dir)
}

#[test]
fn initialize_negotiates_a_real_protocol_version() {
    let (mut proc, _dir) = start_with_fixture();
    let resp = proc.initialize();
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 1);
    // rmcp negotiates its own supported version rather than echoing
    // whatever the client claimed — real protocol-version negotiation,
    // not an artifact a synthetic server would reproduce.
    assert!(resp["result"]["protocolVersion"].is_string());
    assert_eq!(
        resp["result"]["serverInfo"]["name"],
        "agent-socket-tools-mcp-server"
    );
}

#[test]
fn tools_list_returns_both_mvp_tools() {
    let (mut proc, _dir) = start_with_fixture();
    proc.initialize();
    let resp = proc.call("tools/list", json!({}));
    let tools = resp["result"]["tools"].as_array().unwrap();
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"system.info"));
    assert!(names.contains(&"filesystem.read"));
    assert!(tools[0]["inputSchema"].is_object());
}

#[test]
fn tools_call_system_info_succeeds() {
    let (mut proc, _dir) = start_with_fixture();
    proc.initialize();
    let resp = proc.call(
        "tools/call",
        json!({ "name": "system.info", "arguments": {} }),
    );
    assert_eq!(resp["result"]["isError"], false);
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let value: Value = serde_json::from_str(text).unwrap();
    assert_eq!(value["os"], std::env::consts::OS);
}

#[test]
fn tools_call_filesystem_read_succeeds_for_authorized_file() {
    let (mut proc, _dir) = start_with_fixture();
    proc.initialize();
    let resp = proc.call(
        "tools/call",
        json!({ "name": "filesystem.read", "arguments": { "path": "hello.txt" } }),
    );
    assert_eq!(resp["result"]["isError"], false);
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    let value: Value = serde_json::from_str(text).unwrap();
    assert_eq!(value["contents"], "hello world\n");
}

#[test]
fn tools_call_rejects_traversal_as_an_error_result_not_a_protocol_error() {
    let (mut proc, _dir) = start_with_fixture();
    proc.initialize();
    let resp = proc.call(
        "tools/call",
        json!({ "name": "filesystem.read", "arguments": { "path": "../../etc/passwd" } }),
    );
    assert!(resp.get("error").is_none());
    assert_eq!(resp["result"]["isError"], true);
}

#[test]
fn tools_call_unknown_tool_is_an_error_result() {
    let (mut proc, _dir) = start_with_fixture();
    proc.initialize();
    let resp = proc.call(
        "tools/call",
        json!({ "name": "does.not.exist", "arguments": {} }),
    );
    assert_eq!(resp["result"]["isError"], true);
}

#[test]
fn serves_multiple_sequential_requests_on_one_process() {
    let (mut proc, _dir) = start_with_fixture();
    proc.initialize();
    for _ in 1..=5 {
        let resp = proc.call(
            "tools/call",
            json!({ "name": "system.info", "arguments": {} }),
        );
        assert_eq!(resp["result"]["isError"], false);
    }
}
