//! End-to-end tests for the real, compiled `mcp-like-server` binary over
//! its actual stdio transport — the MCP baseline's analogue of
//! `tests/integration.rs` and `tests/ast_integration.rs`.

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

struct McpProc {
    child: std::process::Child,
    stdin: std::process::ChildStdin,
    stdout: BufReader<std::process::ChildStdout>,
}

impl Drop for McpProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl McpProc {
    fn start(read_dir: &std::path::Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_mcp-like-server"))
            .arg(read_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn mcp-like-server");
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            stdin,
            stdout,
        }
    }

    fn call(&mut self, id: u64, method: &str, params: Value) -> Value {
        let request = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        let mut line = serde_json::to_string(&request).unwrap();
        line.push('\n');
        self.stdin.write_all(line.as_bytes()).unwrap();
        self.stdin.flush().unwrap();

        let mut response_line = String::new();
        self.stdout.read_line(&mut response_line).unwrap();
        serde_json::from_str(&response_line).unwrap()
    }
}

fn start_with_fixture() -> (McpProc, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("hello.txt"), "hello world\n").unwrap();
    let proc = McpProc::start(dir.path());
    (proc, dir)
}

#[test]
fn initialize_returns_a_well_formed_result() {
    let (mut proc, _dir) = start_with_fixture();
    let resp = proc.call(
        1,
        "initialize",
        json!({ "protocolVersion": "2026-06-18", "capabilities": {}, "clientInfo": { "name": "test", "version": "0.1" } }),
    );
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 1);
    assert!(resp["result"]["serverInfo"]["name"].is_string());
    assert_eq!(resp["result"]["capabilities"]["tools"], json!({}));
}

#[test]
fn tools_list_returns_both_mvp_tools() {
    let (mut proc, _dir) = start_with_fixture();
    let resp = proc.call(2, "tools/list", json!({}));
    let tools = resp["result"]["tools"].as_array().unwrap();
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"system.info"));
    assert!(names.contains(&"filesystem.read"));
    // Matches MCP's actual tool-descriptor shape: name/description/inputSchema.
    assert!(tools[0]["inputSchema"].is_object());
}

#[test]
fn tools_call_system_info_succeeds() {
    let (mut proc, _dir) = start_with_fixture();
    let resp = proc.call(
        3,
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
    let resp = proc.call(
        4,
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
    let resp = proc.call(
        5,
        "tools/call",
        json!({ "name": "filesystem.read", "arguments": { "path": "../../etc/passwd" } }),
    );
    // Matches real MCP convention: a tool-level failure is reported inside
    // a successful JSON-RPC response via isError:true, not a JSON-RPC
    // protocol-level `error` field (that's reserved for malformed
    // requests, unknown methods, etc).
    assert!(resp.get("error").is_none());
    assert_eq!(resp["result"]["isError"], true);
}

#[test]
fn tools_call_unknown_tool_is_an_error_result() {
    let (mut proc, _dir) = start_with_fixture();
    let resp = proc.call(
        6,
        "tools/call",
        json!({ "name": "does.not.exist", "arguments": {} }),
    );
    assert_eq!(resp["result"]["isError"], true);
}

#[test]
fn unknown_method_is_a_protocol_level_error() {
    let (mut proc, _dir) = start_with_fixture();
    let resp = proc.call(7, "frobnicate", json!({}));
    assert!(resp.get("result").is_none());
    assert_eq!(resp["error"]["code"], -32601);
}

#[test]
fn malformed_json_line_gets_a_parse_error() {
    let (mut proc, _dir) = start_with_fixture();
    proc.stdin.write_all(b"not json at all\n").unwrap();
    proc.stdin.flush().unwrap();
    let mut line = String::new();
    proc.stdout.read_line(&mut line).unwrap();
    let resp: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(resp["error"]["code"], -32700);
}

#[test]
fn serves_multiple_sequential_requests_on_one_process() {
    let (mut proc, _dir) = start_with_fixture();
    for i in 1..=5u64 {
        let resp = proc.call(
            i,
            "tools/call",
            json!({ "name": "system.info", "arguments": {} }),
        );
        assert_eq!(resp["id"], i);
        assert_eq!(resp["result"]["isError"], false);
    }
}
