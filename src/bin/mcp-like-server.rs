//! A minimal, shape-faithful MCP baseline server, for `ast-bench` to
//! compare AST against something that actually exists and actually runs
//! — not an invented number.
//!
//! **This is not the official MCP reference implementation, and does not
//! claim to be.** It implements JSON-RPC 2.0 over stdio with the same
//! transport framing real MCP servers use (one JSON object per line on
//! stdout, read one line of JSON-RPC request per line on stdin) and the
//! three methods a tool-invocation benchmark actually needs —
//! `initialize`, `tools/list`, `tools/call` — matching the real
//! protocol's request/response *shapes* for those methods closely enough
//! to be a fair stand-in for "a local MCP server doing equivalent work."
//! It does not implement MCP's resources, prompts, sampling,
//! roots, notifications, or the full capability-negotiation surface,
//! because `ast-bench`'s scenarios don't exercise any of that. See
//! `docs/AST_PROTOCOL.md`'s "MCP comparison" section for exactly what
//! this does and does not establish.
//!
//! Serves the exact same `system.info`/`filesystem.read` tools
//! (`agent_socket_tools::tools`) as both of this crate's own protocols,
//! so all three baselines in `ast-bench` do identical work — only the
//! transport and wire format differ.

use agent_socket_tools::tools::{FilesystemReadTool, SystemInfoTool, ToolRegistry};
use serde_json::{json, Value};
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

fn main() {
    let read_dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            eprintln!("usage: mcp-like-server <read-dir>");
            std::process::exit(2);
        });

    let mut registry = ToolRegistry::new();
    registry.register(Box::new(SystemInfoTool));
    match FilesystemReadTool::new(&read_dir) {
        Ok(tool) => registry.register(Box::new(tool)),
        Err(e) => {
            eprintln!("cannot authorize read directory {read_dir:?}: {e}");
            std::process::exit(1);
        }
    }

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break, // EOF or read error: stop
        };
        if line.trim().is_empty() {
            continue;
        }

        let request: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                write_response(
                    &mut stdout,
                    json!({
                        "jsonrpc": "2.0",
                        "id": Value::Null,
                        "error": { "code": -32700, "message": format!("parse error: {e}") }
                    }),
                );
                continue;
            }
        };

        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let method = request.get("method").and_then(Value::as_str).unwrap_or("");
        let params = request.get("params").cloned().unwrap_or(json!({}));

        let response = match method {
            "initialize" => json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "protocolVersion": "2026-06-18",
                    "capabilities": { "tools": {} },
                    "serverInfo": { "name": "mcp-like-server (agent-socket-tools benchmark baseline)", "version": "0.1.0" }
                }
            }),
            "tools/list" => {
                let tools: Vec<Value> = registry
                    .discover()
                    .into_iter()
                    .map(|t| {
                        json!({
                            "name": t.name,
                            "description": t.description,
                            "inputSchema": t.input_schema,
                        })
                    })
                    .collect();
                json!({ "jsonrpc": "2.0", "id": id, "result": { "tools": tools } })
            }
            "tools/call" => handle_tools_call(&registry, id.clone(), &params),
            "ping" => json!({ "jsonrpc": "2.0", "id": id, "result": {} }),
            other => json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": -32601, "message": format!("method not found: {other}") }
            }),
        };

        write_response(&mut stdout, response);
    }
}

fn handle_tools_call(registry: &ToolRegistry, id: Value, params: &Value) -> Value {
    let Some(name) = params.get("name").and_then(Value::as_str) else {
        return json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": -32602, "message": "missing \"name\"" }
        });
    };
    let Some(tool) = registry.get(name) else {
        return json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "content": [{ "type": "text", "text": format!("no such tool: {name}") }],
                "isError": true
            }
        });
    };
    let args = params.get("arguments").cloned().unwrap_or(json!({}));
    match tool.invoke(args) {
        Ok(value) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "content": [{ "type": "text", "text": serde_json::to_string(&value).unwrap() }],
                "isError": false
            }
        }),
        Err(e) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "content": [{ "type": "text", "text": e.to_string() }],
                "isError": true
            }
        }),
    }
}

fn write_response(stdout: &mut io::Stdout, response: Value) {
    let line = serde_json::to_string(&response).expect("Value always serializes");
    let _ = writeln!(stdout, "{line}");
    let _ = stdout.flush();
}
