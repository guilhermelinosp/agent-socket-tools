//! Protocol clients and the closed-loop engine. Every protocol exposes the
//! same two operations to the scenarios -- one tool call and one discovery --
//! through the [`Target`] trait, so a scenario issues the identical request
//! over each protocol and checks the identical result.

use crate::procs::sibling_bin;
use crate::stats::Cell;
use agent_socket_tools::ast::client::AstClient;
use agent_socket_tools::protocol::{Operation, Request, Response, PROTOCOL_VERSION};
use agent_socket_tools::transport::MessageReader;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::process::{ChildStdin, ChildStdout, Command as TokioCommand};
use tokio::time::{timeout, Duration};

/// Per-request ceiling for the benchmark's own clients. A timeout is an error.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Number of tools every server in this benchmark registers.
pub const EXPECTED_TOOLS: usize = 3;
const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

/// What a successful result must look like. A result that does not match
/// counts as an error, so a server that answers quickly but wrongly is caught.
#[derive(Clone, Copy, Debug)]
pub enum Expect {
    /// `system.noop`: an empty object.
    Empty,
    /// `system.info`: os, arch and family strings.
    SystemInfo,
    /// `filesystem.read`: `size_bytes` and `contents` of exactly this many bytes.
    ReadBytes(usize),
}

/// One benchmark operation, identical across protocols.
#[derive(Clone, Debug)]
pub enum Op {
    Call {
        tool: &'static str,
        args: Value,
        expect: Expect,
    },
    Discover,
}

impl Op {
    pub fn noop() -> Op {
        Op::Call {
            tool: "system.noop",
            args: json!({}),
            expect: Expect::Empty,
        }
    }

    pub fn info() -> Op {
        Op::Call {
            tool: "system.info",
            args: json!({}),
            expect: Expect::SystemInfo,
        }
    }

    pub fn read(file: &str, bytes: usize) -> Op {
        Op::Call {
            tool: "filesystem.read",
            args: json!({ "path": file }),
            expect: Expect::ReadBytes(bytes),
        }
    }
}

fn check(expect: Expect, value: &Value) -> Result<(), String> {
    let ok = match expect {
        Expect::Empty => value.as_object().is_some_and(|m| m.is_empty()),
        Expect::SystemInfo => ["os", "arch", "family"]
            .iter()
            .all(|key| value.get(*key).is_some_and(Value::is_string)),
        Expect::ReadBytes(n) => {
            value.get("size_bytes").and_then(Value::as_u64) == Some(n as u64)
                && value.get("contents").and_then(Value::as_str).map(str::len) == Some(n)
        }
    };
    if ok {
        Ok(())
    } else {
        Err(format!("unexpected result for {expect:?}"))
    }
}

fn check_discovery(count: usize) -> Result<(), String> {
    if count == EXPECTED_TOOLS {
        Ok(())
    } else {
        Err(format!(
            "discovered {count} tools, expected {EXPECTED_TOOLS}"
        ))
    }
}

pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

/// A protocol connection as seen by the scenarios.
pub trait Target: Send {
    fn call<'a>(&'a mut self, tool: &'a str, args: &'a Value) -> BoxFut<'a, Value>;
    /// Returns how many tools discovery reported.
    fn list<'a>(&'a mut self) -> BoxFut<'a, usize>;
}

/// Runs one operation and validates its result.
pub async fn exec<T: Target>(target: &mut T, op: &Op) -> Result<(), String> {
    match op {
        Op::Call { tool, args, expect } => {
            let value = target.call(tool, args).await?;
            check(*expect, &value)
        }
        Op::Discover => check_discovery(target.list().await?),
    }
}

/// Closed-loop sequential run: one operation at a time, each waited on.
pub async fn sequential<T: Target>(target: &mut T, op: &Op, n: usize) -> Cell {
    let mut cell = Cell {
        start: Some(Instant::now()),
        ..Cell::default()
    };
    for _ in 0..n {
        let started = Instant::now();
        let outcome = exec(target, op).await;
        cell.record(started, outcome);
    }
    cell.end = Some(Instant::now());
    cell
}

// ---- Legacy JSON-Lines protocol over a Unix socket ----

pub struct LegacyConn {
    stream: UnixStream,
    reader: MessageReader,
    next_id: u64,
}

impl LegacyConn {
    pub async fn connect(path: &Path) -> Result<Self, String> {
        let stream = UnixStream::connect(path)
            .await
            .map_err(|e| format!("connect: {e}"))?;
        Ok(Self {
            stream,
            reader: MessageReader::new(MAX_MESSAGE_BYTES),
            next_id: 0,
        })
    }

    fn next_request_id(&mut self) -> String {
        self.next_id += 1;
        self.next_id.to_string()
    }

    async fn roundtrip(&mut self, request: &Request) -> Result<Response, String> {
        let mut line = serde_json::to_string(request).map_err(|e| e.to_string())?;
        line.push('\n');
        let stream = &mut self.stream;
        let reader = &mut self.reader;
        timeout(REQUEST_TIMEOUT, async move {
            stream
                .write_all(line.as_bytes())
                .await
                .map_err(|e| e.to_string())?;
            let message = reader
                .next_message(stream)
                .await
                .map_err(|e| format!("{e:?}"))?
                .ok_or_else(|| "connection closed".to_string())?;
            serde_json::from_str::<Response>(&message).map_err(|e| e.to_string())
        })
        .await
        .map_err(|_| "timeout".to_string())?
    }

    async fn call_tool(&mut self, tool: &str, args: &Value) -> Result<Value, String> {
        let request = Request {
            version: PROTOCOL_VERSION,
            id: self.next_request_id(),
            op: Operation::Invoke,
            tool: Some(tool.to_string()),
            args: Some(args.clone()),
        };
        let response = self.roundtrip(&request).await?;
        if response.ok {
            Ok(response.result.unwrap_or(Value::Null))
        } else {
            Err(response.error.map(|e| e.message).unwrap_or_default())
        }
    }

    async fn discover(&mut self) -> Result<usize, String> {
        let request = Request {
            version: PROTOCOL_VERSION,
            id: self.next_request_id(),
            op: Operation::Discover,
            tool: None,
            args: None,
        };
        let response = self.roundtrip(&request).await?;
        match (response.ok, response.result) {
            (true, Some(Value::Array(tools))) => Ok(tools.len()),
            _ => Err("discover failed".to_string()),
        }
    }
}

impl Target for LegacyConn {
    fn call<'a>(&'a mut self, tool: &'a str, args: &'a Value) -> BoxFut<'a, Value> {
        Box::pin(async move { self.call_tool(tool, args).await })
    }

    fn list<'a>(&'a mut self) -> BoxFut<'a, usize> {
        Box::pin(async move { self.discover().await })
    }
}

// ---- AST via this repository's SDK (MessagePack, Unix socket) ----

/// Shares one AST connection; cloning the handle shares the connection.
#[derive(Clone)]
pub struct AstHandle(pub Arc<AstClient>);

impl Target for AstHandle {
    fn call<'a>(&'a mut self, tool: &'a str, args: &'a Value) -> BoxFut<'a, Value> {
        Box::pin(async move {
            self.0
                .invoke(tool, args.clone(), None)
                .await
                .map_err(|e| e.to_string())
        })
    }

    fn list<'a>(&'a mut self) -> BoxFut<'a, usize> {
        Box::pin(async move {
            self.0
                .discover()
                .await
                .map(|tools| tools.len())
                .map_err(|e| e.to_string())
        })
    }
}

/// Same connection, but every call uses `invoke_stream` (streamed result).
/// Discovery is not defined for this target and always fails.
pub struct AstStreamed(pub Arc<AstClient>);

impl Target for AstStreamed {
    fn call<'a>(&'a mut self, tool: &'a str, args: &'a Value) -> BoxFut<'a, Value> {
        Box::pin(async move {
            self.0
                .invoke_stream(tool, args.clone(), None)
                .await
                .map_err(|e| e.to_string())
        })
    }

    fn list<'a>(&'a mut self) -> BoxFut<'a, usize> {
        Box::pin(async move { Err("discovery is not measured in streamed mode".to_string()) })
    }
}

// ---- MCP over stdio (JSON-RPC 2.0, one message per line) ----

/// Minimal MCP stdio client written for this benchmark (not the rmcp client).
/// Used for both `mcp-server` (real rmcp) and `mcp-like-server` (synthetic).
pub struct McpStdio {
    child: tokio::process::Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl McpStdio {
    pub fn spawn(bin: &str, read_dir: &Path) -> Result<Self, String> {
        let mut child = TokioCommand::new(sibling_bin(bin))
            .arg(read_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| format!("cannot spawn {bin}: {e}"))?;
        let stdin = child.stdin.take().ok_or("no stdin")?;
        let stdout = BufReader::new(child.stdout.take().ok_or("no stdout")?);
        Ok(Self {
            child,
            stdin,
            stdout,
            next_id: 1,
        })
    }

    pub fn pid(&self) -> u32 {
        self.child.id().unwrap_or(0)
    }

    /// Performs `initialize`. Like the earlier harness, this client does not
    /// send `notifications/initialized` afterwards (see "Threats to validity").
    pub async fn initialize(&mut self) -> Result<(), String> {
        self.rpc(
            "initialize",
            json!({
                "protocolVersion": "2026-07-28",
                "capabilities": {},
                "clientInfo": { "name": "ast-bench", "version": env!("CARGO_PKG_VERSION") }
            }),
        )
        .await
        .map(|_| ())
    }

    async fn write_request(&mut self, method: &str, params: Value) -> Result<u64, String> {
        let id = self.next_id;
        self.next_id += 1;
        let mut line =
            json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }).to_string();
        line.push('\n');
        self.stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|e| format!("write: {e}"))?;
        self.stdin
            .flush()
            .await
            .map_err(|e| format!("flush: {e}"))?;
        Ok(id)
    }

    async fn read_response(&mut self) -> Result<Value, String> {
        let mut line = String::new();
        let read = timeout(REQUEST_TIMEOUT, self.stdout.read_line(&mut line))
            .await
            .map_err(|_| "timeout".to_string())?
            .map_err(|e| format!("read: {e}"))?;
        if read == 0 {
            return Err("server closed stdout".to_string());
        }
        serde_json::from_str(&line).map_err(|e| format!("bad json: {e}"))
    }

    /// One request, waiting for its own response (sequential use only).
    async fn rpc(&mut self, method: &str, params: Value) -> Result<Value, String> {
        let id = self.write_request(method, params).await?;
        loop {
            let response = self.read_response().await?;
            if response.get("id").and_then(Value::as_u64) == Some(id) {
                return Ok(response);
            }
        }
    }

    async fn tools_call(&mut self, tool: &str, args: &Value) -> Result<Value, String> {
        let response = self
            .rpc("tools/call", json!({ "name": tool, "arguments": args }))
            .await?;
        tool_result(&response)
    }

    async fn tools_list(&mut self) -> Result<usize, String> {
        let response = self.rpc("tools/list", json!({})).await?;
        response["result"]["tools"]
            .as_array()
            .map(Vec::len)
            .ok_or_else(|| "tools/list returned no tools array".to_string())
    }

    /// Keeps up to `window` requests in flight on this one connection and
    /// matches responses by JSON-RPC id. Latency of a request is the arrival
    /// time of its response minus its own send time. Exactly `total` requests
    /// are issued; anything left unanswered after a transport failure counts
    /// as an error.
    pub async fn pipelined(
        &mut self,
        total: usize,
        window: usize,
        tool: &str,
        args: &Value,
        expect: Expect,
    ) -> Cell {
        let mut cell = Cell {
            start: Some(Instant::now()),
            ..Cell::default()
        };
        let window = window.max(1);
        let mut in_flight: HashMap<u64, Instant> = HashMap::new();
        let mut sent = 0usize;
        let mut answered = 0usize;
        let mut fatal = false;

        while answered < total && !fatal {
            while !fatal && sent < total && in_flight.len() < window {
                match self
                    .write_request("tools/call", json!({ "name": tool, "arguments": args }))
                    .await
                {
                    Ok(id) => {
                        in_flight.insert(id, Instant::now());
                        sent += 1;
                    }
                    Err(e) => {
                        cell.note_error(e);
                        fatal = true;
                    }
                }
            }
            if fatal || answered >= total {
                break;
            }
            // Here `sent > answered`, so at least one request is in flight.
            let response = match self.read_response().await {
                Ok(response) => response,
                Err(e) => {
                    cell.note_error(e);
                    break;
                }
            };
            let sent_at = response
                .get("id")
                .and_then(Value::as_u64)
                .and_then(|id| in_flight.remove(&id));
            let Some(sent_at) = sent_at else {
                cell.note_error("response with an unknown id".to_string());
                break;
            };
            answered += 1;
            match tool_result(&response).and_then(|v| check(expect, &v)) {
                Ok(()) => cell.ok_latency(sent_at.elapsed()),
                Err(e) => cell.fail(e),
            }
        }

        cell.errors += (total - answered) as u64;
        cell.end = Some(Instant::now());
        cell
    }
}

/// Extracts the structured result of an MCP `tools/call` response. The server
/// wraps the JSON result as text content; that text is decoded again here.
fn tool_result(response: &Value) -> Result<Value, String> {
    if let Some(error) = response.get("error").filter(|e| !e.is_null()) {
        return Err(format!("rpc error: {error}"));
    }
    let result = response.get("result").ok_or("response has no result")?;
    let text = result["content"][0]["text"]
        .as_str()
        .ok_or("tool result has no text content")?;
    if result
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err(format!("tool error: {text}"));
    }
    serde_json::from_str(text).map_err(|e| format!("tool output is not JSON: {e}"))
}

impl Target for McpStdio {
    fn call<'a>(&'a mut self, tool: &'a str, args: &'a Value) -> BoxFut<'a, Value> {
        Box::pin(async move { self.tools_call(tool, args).await })
    }

    fn list<'a>(&'a mut self) -> BoxFut<'a, usize> {
        Box::pin(async move { self.tools_list().await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_result_decodes_text_content() {
        let response = json!({
            "jsonrpc": "2.0", "id": 1,
            "result": { "content": [{ "type": "text", "text": "{\"a\":1}" }], "isError": false }
        });
        assert_eq!(tool_result(&response).unwrap(), json!({"a": 1}));
    }

    #[test]
    fn tool_result_reports_is_error_and_rpc_errors() {
        let tool_err = json!({
            "result": { "content": [{ "type": "text", "text": "no such tool" }], "isError": true }
        });
        assert!(tool_result(&tool_err).is_err());
        let rpc_err = json!({ "error": { "code": -32601, "message": "nope" } });
        assert!(tool_result(&rpc_err).is_err());
    }

    #[test]
    fn expectations_reject_wrong_results() {
        assert!(check(Expect::Empty, &json!({})).is_ok());
        assert!(check(Expect::Empty, &json!({"x": 1})).is_err());
        assert!(check(
            Expect::ReadBytes(2),
            &json!({"size_bytes": 2, "contents": "ok"})
        )
        .is_ok());
        assert!(check(
            Expect::ReadBytes(3),
            &json!({"size_bytes": 2, "contents": "ok"})
        )
        .is_err());
    }
}
