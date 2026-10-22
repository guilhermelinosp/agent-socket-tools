# agent-socket-tools

A minimal, MCP-free way for local AI agents to discover and invoke a small
set of authorized tools over a Unix Domain Socket — no HTTP, no TCP, no
dependency on the Model Context Protocol.

## Why

MCP is a general-purpose protocol for agent/tool integration, with its own
transport, capability negotiation, and SDKs. This project is the opposite
end of the spectrum: a single-file-sized wire protocol, line-delimited
JSON over a local Unix socket, with exactly three operations and two
built-in tools. It exists for cases where MCP's surface area is more than
the job needs — a local sandbox, a CI step, or a small agent harness that
wants tool access without an MCP SDK in the loop.

## Architecture

```
┌────────────────────┐        Unix Domain Socket        ┌────────────────────┐
│ agent-tools-client  │ ───── line-delimited JSON ─────► │ agent-tools-server │
│ (CLI)                │ ◄──────────────────────────────  │ (long-running)     │
└────────────────────┘                                   └────────────────────┘
                                                                    │
                                                           ┌────────┴────────┐
                                                           │   ToolRegistry   │
                                                           │  system.info     │
                                                           │  filesystem.read │
                                                           └──────────────────┘
```

Modules (`src/`):

| Module | Responsibility |
|---|---|
| `protocol.rs` | Wire types (`Request`, `Response`, `ErrorCode`), versioning, line codec |
| `transport.rs` | Socket bind/cleanup lifecycle, framing, per-connection dispatch |
| `tools/` | The `Tool` trait, `ToolRegistry`, and the two MVP tool implementations |
| `config.rs` | Default socket path resolution and tunables |
| `bin/agent-tools-server.rs` | Process entry point: CLI args, signal handling, accept loop |
| `bin/agent-tools-client.rs` | Process entry point: one request, one response, exit code |

The server accepts any number of concurrent connections (each handled on
its own Tokio task) and, within a connection, serves requests sequentially
until the client disconnects.

## Installing / building

Requires a stable Rust toolchain. Built and tested against **1.99.1** on
macOS (arm64); no lower bound has actually been verified against older
toolchains, so treat 1.99.1 as the only confirmed-working version rather
than inferring an MSRV from it. No other system dependencies — this targets
macOS and Linux via `tokio::net::UnixListener` and does not build on
Windows. Only tested on macOS so far; Linux should work (no platform-specific
code beyond what `tokio` itself handles), but hasn't been run there.

```bash
cargo build --release
# binaries land in target/release/agent-tools-server and
# target/release/agent-tools-client
```

## Running

Start the server, authorizing one directory for `filesystem.read`:

```bash
agent-tools-server --read-dir /path/to/authorized/dir
```

By default the socket is created at `$XDG_RUNTIME_DIR/agent-tools.sock`,
falling back to `$TMPDIR/agent-tools.sock` (or `/tmp/agent-tools.sock`) when
`XDG_RUNTIME_DIR` is unset — which is always the case on a stock macOS
install. Override with `--socket-path`.

Full server options:

```bash
agent-tools-server --help
```

```
Options:
      --socket-path <SOCKET_PATH>
      --read-dir <READ_DIR>
      --max-message-bytes <MAX_MESSAGE_BYTES>   [default: 1048576]
      --request-timeout-secs <REQUEST_TIMEOUT_SECS>  [default: 10]
```

Stop it with `Ctrl-C` or `SIGTERM`; either triggers a graceful shutdown
that unlinks the socket file.

## Using the client

```bash
# List available tools and their input schemas
agent-tools-client discover

# Check the server is up
agent-tools-client health

# Call a tool
agent-tools-client invoke --tool system.info --args '{}'
agent-tools-client invoke --tool filesystem.read --args '{"path":"notes.txt"}'
```

The client exits `0` when the response's `ok` field is `true`, and `1`
otherwise (malformed args, unauthorized path, unknown tool, timeout, etc.),
so it composes with shell `&&`/`||` the same way `curl -f` does.

`--socket-path` defaults to the same resolution rule as the server, so
pointing both at a non-default path only requires passing `--socket-path`
once per command, or exporting it into a shared variable in your shell.

## Protocol

See [`docs/PROTOCOL.md`](docs/PROTOCOL.md) for the full wire format,
operations, and error codes.

## Security model

- The socket file is created with `0600` permissions (owner read/write only)
  regardless of the process umask.
- `filesystem.read` is confined to a single base directory chosen by
  whoever starts the server, not by the client. Every request path is
  canonicalized (symlinks resolved, `..` collapsed) and checked against
  that base directory *after* resolution, which rejects both literal
  traversal (`../../etc/passwd`) and a symlink planted inside the base
  directory that points outside it.
- There is no shell/command execution tool in this MVP, by design.
- Messages are capped at `--max-message-bytes` (default 1 MiB) before a
  newline is even found, so a client can't force unbounded buffering. This
  is a *transport*-level cap on one JSON line (request or response); it is
  separate from `filesystem.read`'s own 1 MiB file-size cap
  (`MAX_READ_BYTES`, hard-coded, not currently exposed as a CLI flag). The
  two happen to share a default value today, but changing
  `--max-message-bytes` does not change how much of a file `filesystem.read`
  will return.
- Every tool invocation runs under `--request-timeout-secs` (default 10s).
  Note: for a tool that is already blocking on blocking I/O, this timeout
  bounds how long the *caller* waits — it cannot forcibly preempt a
  `spawn_blocking` task that's already running, since Rust has no safe
  mechanism to do that. Both MVP tools are fast, bounded, local operations,
  so in practice this only matters if a future tool is added that can block
  indefinitely.
- A stale socket file (left behind by a server that died without cleaning
  up, e.g. `SIGKILL`) is detected by attempting to connect to it before
  bind: a refused connection means it's safe to unlink and replace; a
  successful connection means another server is live, and startup fails
  instead of stealing its socket.

## Testing

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

`cargo test` runs:
- Unit tests for the protocol codec, the `ToolRegistry`, both tools
  (including the symlink-escape and traversal cases), and the socket
  bind/stale-reclaim/ownership-on-drop logic — all in `src/`.
- Integration tests (`tests/integration.rs`) that spawn the real compiled
  `agent-tools-server` binary, drive it with the real `agent-tools-client`
  binary and with a raw `UnixStream` (for malformed input and concurrency
  cases the CLI doesn't expose), and assert on process exit codes and
  socket-file lifecycle after both graceful (`SIGTERM`) and hard (`SIGKILL`)
  termination.

## What this intentionally does not do

- No arbitrary shell/command execution tool.
- No MCP compatibility layer, adapters, or dynamic plugin loading.
- No remote transport (TCP, HTTP, WebSocket) — Unix Domain Socket only.
- No authentication beyond filesystem permissions on the socket file
  itself, which is the standard Unix model for same-host IPC.

These are explicit scope boundaries for this first version, not oversights.
