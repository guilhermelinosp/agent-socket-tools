//! `ast-mcp-adapter`: an optional MCP stdio server that forwards MCP
//! requests to an already-running `ast-server`, so MCP hosts can use
//! tools served over AST without AST depending on MCP.
//!
//! It executes no tools itself. `tools/list` calls `discover()` on every
//! request, and `tools/call` calls `invoke()` on a persistent AST
//! connection. It is an interoperability bridge, outside AST's hot path:
//! it adds a hop (MCP stdio -> AST socket), so do not use it for
//! performance. See `docs/MCP_ADAPTER.md` for the semantic differences.
//!
//! Not implemented: batch, streaming, MCP->AST cancellation
//! (`notifications/cancelled`), and progress reporting.

use agent_socket_tools::ast::client::{AstClient, AstClientConfig, AstClientError};
use agent_socket_tools::ast::message::ErrorCode;
use agent_socket_tools::config::default_ast_socket_path;
use clap::Parser;
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
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "ast-mcp-adapter", version)]
struct Cli {
    /// Socket of a running `ast-server`. Defaults to the same path
    /// `ast-server` uses by default.
    #[arg(long)]
    socket_path: Option<PathBuf>,

    /// Fixed deadline, in milliseconds, applied to every forwarded call.
    #[arg(long, default_value_t = 10_000)]
    deadline_ms: u64,
}

struct AstMcpAdapter {
    client: Arc<AstClient>,
    deadline: Duration,
}

impl ServerHandler for AstMcpAdapter {
    fn get_info(&self) -> InitializeResult {
        let mut info = InitializeResult::new(ServerCapabilities::builder().enable_tools().build());
        info.protocol_version = ProtocolVersion::LATEST;
        info.server_info = Implementation::new("ast-mcp-adapter", env!("CARGO_PKG_VERSION"))
            .with_description("MCP stdio adapter forwarding tools to a running ast-server");
        info
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        // Discovery is repeated on every list: it is cheap, and it avoids
        // serving a stale tool list after the ast-server is restarted.
        let descriptors = self
            .client
            .discover()
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        let tools = descriptors
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
        let args = request
            .arguments
            .map(serde_json::Value::Object)
            .unwrap_or(serde_json::json!({}));
        let result = match self
            .client
            .invoke(&request.name, args, Some(self.deadline))
            .await
        {
            Ok(value) => {
                let text = serde_json::to_string(&value)
                    .map_err(|e| McpError::internal_error(e.to_string(), None))?;
                CallToolResult::success(vec![ContentBlock::text(text)])
            }
            Err(AstClientError::Server {
                code: ErrorCode::ToolNotFound,
                ..
            }) => {
                return Err(McpError::invalid_params(
                    format!("unknown tool: {}", request.name),
                    None,
                ));
            }
            // A tool-level failure (permission, invalid arguments, deadline,
            // traversal, ...) is shown to the model as an error result, the
            // way MCP expects tool failures to be reported.
            Err(AstClientError::Server { code, message }) => {
                CallToolResult::error(vec![ContentBlock::text(format!("{code:?}: {message}"))])
            }
            // Transport-level failures (closed, I/O, timeout, framing, ...)
            // are protocol-level problems with the adapter's own upstream.
            Err(e) => return Err(McpError::internal_error(e.to_string(), None)),
        };
        Ok(result.into())
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let socket_path = cli.socket_path.unwrap_or_else(default_ast_socket_path);

    let client = match AstClient::connect(&socket_path, AstClientConfig::default()).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "ast-mcp-adapter: cannot connect to ast-server at {}: {e}",
                socket_path.display()
            );
            return std::process::ExitCode::from(1);
        }
    };

    let adapter = AstMcpAdapter {
        client: Arc::new(client),
        deadline: Duration::from_millis(cli.deadline_ms),
    };

    let service = match adapter.serve(stdio()).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("failed to start MCP adapter: {e}");
            return std::process::ExitCode::FAILURE;
        }
    };

    if let Err(e) = service.waiting().await {
        eprintln!("MCP adapter exited with an error: {e}");
        return std::process::ExitCode::FAILURE;
    }

    std::process::ExitCode::SUCCESS
}
