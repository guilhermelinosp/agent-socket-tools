//! A real MCP server, built on the official `rmcp` SDK
//! (`modelcontextprotocol/rust-sdk`, pinned to `=3.6.0`) — the fair MCP
//! baseline `ast-bench` compares AST against. Unlike
//! `src/bin/mcp-like-server.rs` (a minimal, hand-rolled JSON-RPC-over-stdio
//! stand-in kept only as a documented synthetic fallback), this binary
//! speaks real MCP through a maintained, versioned implementation of the
//! actual protocol and transport — exactly what a fair comparison requires.
//!
//! Serves the identical tool implementations
//! (`src/tools::{SystemInfoTool, FilesystemReadTool}`) the legacy and AST
//! servers serve, over MCP's standard stdio transport, so a benchmark
//! against this binary is comparing protocols and transports, not
//! comparing different tool logic dressed up as the same tools.

use agent_socket_tools::tools::{FilesystemReadTool, NoopTool, SystemInfoTool, ToolRegistry};
use rmcp::handler::server::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, Implementation,
    InitializeResult, ListToolsResult, PaginatedRequestParams, ProtocolVersion, ServerCapabilities,
    Tool,
};
use rmcp::service::{RequestContext, ServiceExt};
use rmcp::transport::stdio;
use rmcp::{ErrorData as McpError, RoleServer};
use std::path::PathBuf;
use std::sync::Arc;

struct AgentToolsMcpServer {
    registry: Arc<ToolRegistry>,
}

impl ServerHandler for AgentToolsMcpServer {
    fn get_info(&self) -> InitializeResult {
        let mut info = InitializeResult::new(ServerCapabilities::builder().enable_tools().build());
        info.protocol_version = ProtocolVersion::LATEST;
        info.server_info =
            Implementation::new("agent-socket-tools-mcp-server", env!("CARGO_PKG_VERSION"))
                .with_description(
                    "Real MCP baseline (rmcp =3.6.0) serving agent-socket-tools' own tools",
                );
        info
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        let tools = self
            .registry
            .discover()
            .into_iter()
            .map(|d| {
                let schema = d.input_schema.as_object().cloned().unwrap_or_default();
                Tool::new(d.name, d.description, schema)
            })
            .collect();
        Ok(ListToolsResult {
            tools,
            ..Default::default()
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        let Some(tool) = self.registry.get(&request.name) else {
            return Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                "no such tool: {}",
                request.name
            ))])
            .into());
        };
        let args = request
            .arguments
            .map(serde_json::Value::Object)
            .unwrap_or(serde_json::json!({}));
        let result = match tool.invoke(args) {
            Ok(value) => CallToolResult::success(vec![ContentBlock::text(
                serde_json::to_string(&value).expect("tool results are always serializable"),
            )]),
            Err(e) => CallToolResult::error(vec![ContentBlock::text(e.to_string())]),
        };
        Ok(result.into())
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let read_dir = match std::env::args().nth(1).map(PathBuf::from) {
        Some(dir) => dir,
        None => {
            eprintln!("usage: mcp-server <read-dir>");
            return std::process::ExitCode::from(2);
        }
    };

    let mut registry = ToolRegistry::new();
    registry.register(Box::new(SystemInfoTool));
    registry.register(Box::new(NoopTool));
    match FilesystemReadTool::new(&read_dir) {
        Ok(tool) => registry.register(Box::new(tool)),
        Err(e) => {
            eprintln!("cannot authorize read directory {read_dir:?}: {e}");
            return std::process::ExitCode::FAILURE;
        }
    }

    let server = AgentToolsMcpServer {
        registry: Arc::new(registry),
    };

    let service = match server.serve(stdio()).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("failed to start MCP server: {e}");
            return std::process::ExitCode::FAILURE;
        }
    };

    if let Err(e) = service.waiting().await {
        eprintln!("MCP server exited with an error: {e}");
        return std::process::ExitCode::FAILURE;
    }

    std::process::ExitCode::SUCCESS
}
