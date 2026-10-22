# MCP adapter (`ast-mcp-adapter`)

## Purpose

`ast-mcp-adapter` lets an MCP host (for example, a desktop assistant that only
speaks MCP over stdio) use the tools served by a running `ast-server`. It is an
MCP stdio server that forwards `tools/list` and `tools/call` to the AST server
through `AstClient`. It executes no tools itself.

AST does not depend on MCP. The adapter is an optional, separate binary that
exists for interoperability. It is outside AST's hot path and adds one hop
(MCP stdio -> AST socket), so use it for compatibility, not for performance.
For performance numbers, see [BENCHMARKS.md](BENCHMARKS.md) and the
[MCP comparison](AST_PROTOCOL.md#mcp-comparison).

## Usage

Start an `ast-server` first, then run the adapter on the same socket:

```sh
ast-server --socket-path /path/to/ast.sock --read-dir /path/to/readable
ast-mcp-adapter --socket-path /path/to/ast.sock [--deadline-ms 10000]
```

If the adapter cannot connect to the socket at startup, it prints a message to
stderr and exits with code 1.

Example host configuration (`mcpServers` entry):

```json
{
  "mcpServers": {
    "ast-tools": {
      "command": "/absolute/path/to/ast-mcp-adapter",
      "args": ["--socket-path", "/path/to/ast.sock"]
    }
  }
}
```

| Flag | Default | Meaning |
|---|---|---|
| `--socket-path <PATH>` | `agent_socket_tools::config::default_ast_socket_path()` | Socket of the running `ast-server`. |
| `--deadline-ms <u64>` | `10000` | Deadline sent with every forwarded `tools/call`. |

## Semantic differences from native MCP

| Topic | Behavior in the adapter |
|---|---|
| Discovery | `tools/list` calls `discover()` on every request. Tool changes on the server are visible on the next list; no cache. |
| Tool failure | An AST error other than `ToolNotFound` becomes a successful JSON-RPC response with `isError: true` and text `CODE: message`, so the model can see it. |
| Unknown tool | `ToolNotFound` becomes a JSON-RPC protocol error, code `-32602` (invalid params). |
| Result | A successful AST result is returned as one text content block containing the JSON-encoded value. |
| Deadline | Fixed per adapter process via `--deadline-ms`. The MCP request does not carry a per-call deadline. |
| Transport failure | Closed connection, I/O, timeout or framing errors become JSON-RPC internal errors (`-32603`). |
| Batch | Not implemented. Each MCP request is one AST invoke. |
| Streaming | Not implemented. Results are never streamed through the adapter. |
| Cancellation | Not mapped. MCP `notifications/cancelled` is not forwarded to AST. |
| Progress | Not implemented. |
| Authorization | Unchanged. It is the `ast-server`'s policy. The adapter connects as the OS user that runs it. The threat model in [AST_PROTOCOL.md](AST_PROTOCOL.md#threat-model-and-security) applies: anyone who can run the adapter as that user can invoke the tools the server allows for that user. |

## Limits and notes

- The adapter is a single upstream connection. Concurrency is whatever the
  AST client provides on that connection, not an MCP-level concurrency model.
- The adapter does not start or supervise `ast-server`. If the server goes away
  after startup, calls fail with an internal error until the adapter is
  restarted.
- Do not use the adapter where latency or throughput matter. Use AST directly
  through `AstClient` or the Python SDK in `sdks/python/`.
