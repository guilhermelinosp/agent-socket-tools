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

The server accepts connections up to `--max-connections` (each handled on
its own Tokio task) and, within a connection, serves requests sequentially
until the client disconnects, a framing error occurs, or the connection
goes quiet for longer than `--request-timeout-secs`.

**`agent-tools-client` is a test/debugging CLI, not an agent SDK.** It
sends exactly one request per invocation and prints the raw JSON response.
Nothing in this repository wires any particular AI agent or framework up
to this protocol automatically — an agent that wants to speak it needs to
either shell out to this binary or implement the client side of
`docs/PROTOCOL.md` (a few dozen lines: connect, write one line, read one
line) itself.

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

By default the socket is created at
`$XDG_RUNTIME_DIR/agent-socket-tools/agent-tools.sock`, falling back to
`$TMPDIR/agent-socket-tools/agent-tools.sock` when `XDG_RUNTIME_DIR` is
unset — which is always the case on a stock macOS install. The
`agent-socket-tools` directory is created (or validated, if it already
exists) as private to the current user before the socket is bound; see
[Socket directory hardening](#socket-directory-hardening) below. Override
the full path with `--socket-path` — if you do, its parent directory goes
through the same creation/validation step, so pointing it directly at a
shared, multi-user directory (bare `/tmp`, say) will be rejected; nest it
under a directory only you own instead.

Full server options:

```bash
agent-tools-server --help
```

```
Options:
      --socket-path <SOCKET_PATH>
      --read-dir <READ_DIR>
      --max-message-bytes <MAX_MESSAGE_BYTES>          [default: 1048576]
      --request-timeout-secs <REQUEST_TIMEOUT_SECS>    [default: 10]
      --max-connections <MAX_CONNECTIONS>              [default: 256]
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
otherwise (malformed args, unauthorized path, unknown tool, timeout, etc.,
or a client-side problem like the response exceeding `--max-message-bytes`
or `--response-timeout-secs` elapsing), so it composes with shell
`&&`/`||` the same way `curl -f` does.

`--socket-path` defaults to the same resolution rule as the server, so
pointing both at a non-default path only requires passing `--socket-path`
once per command, or exporting it into a shared variable in your shell.
`--max-message-bytes` (client-side) bounds how large a response the
client will buffer, mirroring the server's own request-side limit;
`--response-timeout-secs` (default 10s) bounds how long it waits for one.

## Protocol

See [`docs/PROTOCOL.md`](docs/PROTOCOL.md) for the full wire format,
operations, and error codes.

## Security model

### Socket directory hardening

- The socket's parent directory is created (if missing) as a private,
  `0700`, user-owned directory, or validated as one if it already exists.
  A predictable filename directly inside a shared, world-writable temp
  root (classic `/tmp/foo.sock` squatting) is exactly the attack this
  defends against — see `ensure_private_dir`/`validate_private_dir` in
  `src/transport.rs`.
- Symlinks are *followed*, not rejected outright — macOS's own `/tmp ->
  /private/tmp` is a symlink, and treating every symlink as hostile would
  reject that completely ordinary case. What's validated is the *real*
  directory a symlink resolves to: it must be owned by the current user
  and not writable by anyone else (group/world *read* or *traversal*
  access is tolerated; group/world *write* access is refused). This is
  why `/tmp` itself, used directly, is refused on a typical macOS/Linux
  box — it resolves to a root-owned directory — while the default
  `agent-socket-tools` subdirectory *under* `/tmp`, which this process
  creates and owns, is fine.
- The socket file itself is created with `0600` permissions. This is
  enforced twice: the process `umask` is temporarily tightened to `0077`
  for the duration of the `bind()` call (closing the brief window where a
  looser ambient umask would otherwise leave the file more permissive
  between creation and the next step), and then `chmod(0600)` is applied
  explicitly regardless, so the guarantee doesn't silently depend on the
  umask trick alone.
- A stale socket file (left behind by a server that died without cleaning
  up, e.g. `SIGKILL`) is detected by attempting to connect to it before
  bind: a refused connection means it's safe to unlink and replace; a
  successful connection means another server is live, and startup fails
  instead of stealing its socket. This has an inherent TOCTOU gap (the
  path could be replaced between that check and the actual `bind()`); this
  crate assumes `XDG_RUNTIME_DIR`/`TMPDIR` and their immediate parents
  aren't themselves under attack, same as the directory-ownership check
  above.

### Request/response framing and limits

- A connection-scoped buffer assembles complete, newline-terminated
  messages across however many `read()` calls it takes, correctly
  handling multiple pipelined requests arriving in one `read()` and a
  single request arriving fragmented across many — see `MessageReader` in
  `src/transport.rs`.
- Both requests and responses are capped at `--max-message-bytes` (default
  1 MiB): a request line over the limit is rejected before it's even fully
  received (`message_too_large`), and a *response* that would exceed the
  limit is replaced with a small, bounded `response_too_large` error
  instead of being sent, written, or truncated silently. This is a
  *transport*-level cap on one JSON line; it's independent of
  `filesystem.read`'s own 1 MiB file-size cap (`MAX_READ_BYTES` in
  `src/tools/filesystem_read.rs`, hard-coded, not currently a CLI flag) —
  the two happen to share a default value today, but changing
  `--max-message-bytes` does not change how much of a file
  `filesystem.read` will return, and a file comfortably under
  `MAX_READ_BYTES` can still produce a response over a *small*
  `--max-message-bytes`.
- EOF with a partial message already buffered (`incomplete_message`) and a
  line that isn't valid UTF-8 (`invalid_utf8`) are reported distinctly
  from `message_too_large` and from each other — see
  `docs/PROTOCOL.md`'s error code table.

### Timeouts and connection limits

- `--request-timeout-secs` (default 10s) is applied twice, independently:
  once to *receiving* one complete request (covering both "nothing
  arrived" and "a fragment arrived but the rest never did"), and once to
  *running* the tool it names. A pathological request can therefore take
  up to roughly 2x this value — this crate does not add a second
  configuration knob to shave that in half; see the doc comment on
  `handle_connection` for the reasoning.
- For the tool-execution half specifically: the timeout bounds how long
  the *caller* waits. It cannot forcibly abort a `spawn_blocking` task
  that's already running, since Rust has no safe mechanism to preempt one.
  Both MVP tools are fast, bounded, local operations, so in practice this
  only matters if a future tool is added that can block indefinitely.
- `--max-connections` (default 256) bounds how many connections are
  served concurrently. A connection beyond the limit is accepted (so its
  file descriptor doesn't sit unaccepted in the kernel's backlog), sent a
  bounded `connection_limit_exceeded` error, and closed — never silently
  dropped, and never left queued. Each connection's slot is tied to an
  RAII permit (`tokio::sync::OwnedSemaphorePermit`) held for its handling
  task's whole lifetime, so the slot is freed automatically on clean
  disconnect, timeout, or any other error — not just the success path.

### Filesystem confinement

- `filesystem.read` is confined to a single base directory chosen by
  whoever starts the server, not by the client. Every request path is
  canonicalized (symlinks resolved, `..` collapsed) and checked against
  that base directory *after* resolution, which rejects both literal
  traversal (`../../etc/passwd`) and a symlink planted inside the base
  directory that points outside it.
- The final `open` uses `O_NOFOLLOW`, and size/type checks read the
  already-open file descriptor's metadata rather than re-`stat`ing by
  path — this narrows, but does not eliminate, the TOCTOU gap between
  resolving a path and acting on it. See the doc comment on
  `FilesystemReadTool` in `src/tools/filesystem_read.rs` for exactly what
  is and isn't covered; the short version is that this crate assumes the
  authorized base directory isn't being concurrently modified by an
  untrusted actor while the server runs.
- There is no shell/command execution tool in this MVP, by design.

### Error messages

Error messages name the server-relative path a client requested (so a
legitimate caller can see what it got wrong) but never the resolved,
absolute filesystem path, a stack trace, or any other server-internal
detail.

## Testing

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

`cargo test` runs 70 tests total:
- 56 unit tests (in `src/`) for the protocol codec, the `ToolRegistry`,
  both tools (including the symlink-escape, TOCTOU-mitigation, and
  traversal cases), the socket bind/stale-reclaim/ownership-on-drop/
  private-directory logic, the `MessageReader` framing (pipelined
  messages, fragmented messages, exact and over-limit sizes, EOF
  mid-message, invalid UTF-8), the connection limiter, and response-size
  bounding.
- 14 integration tests (`tests/integration.rs`) that spawn the real
  compiled `agent-tools-server` binary, drive it with the real
  `agent-tools-client` binary and with a raw `UnixStream` (for malformed
  input, pipelining, and timeout/limit cases the CLI doesn't expose), and
  assert on process exit codes, response contents, and socket-file
  lifecycle after both graceful (`SIGTERM`) and hard (`SIGKILL`)
  termination.

Both unit and integration suites pass under `cargo test`'s default
parallelism as well as `cargo test -- --test-threads=1`; the latter is a
useful way to confirm a failure isn't test-order-dependent before blaming
the code under test.

## What this intentionally does not do

- No arbitrary shell/command execution tool.
- No MCP compatibility layer, adapters, or dynamic plugin loading.
- No remote transport (TCP, HTTP, WebSocket) — Unix Domain Socket only.
- No authentication beyond filesystem permissions on the socket file
  itself, which is the standard Unix model for same-host IPC.

These are explicit scope boundaries for this first version, not oversights.
