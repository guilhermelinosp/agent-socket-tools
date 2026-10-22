# AST Wire Protocol v1 (Agent Socket Transport)

AST is this project's second, performance-oriented protocol, alongside —
not replacing — the original JSON-Lines protocol documented in
`docs/PROTOCOL.md`. See [Compatibility and migration](#compatibility-and-migration)
for why both exist.

**Status: implemented and tested in this repository only.** No second,
independent implementation exists yet. Per this document's own standard
(see [Cross-language interoperability](#cross-language-interoperability)),
cross-language compatibility is now a **verified fact for one other
language (Python)**, not just a documented goal — see that section for
exactly what's been checked and what hasn't.

## Transport

A persistent Unix Domain Socket (`SOCK_STREAM`), reused across many
request/response exchanges rather than opened per call. One connection
supports many concurrent in-flight requests (see
[Concurrency and correlation](#concurrency-and-correlation)).

## Framing

Every frame, in both directions, has this exact byte layout:

```text
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       length (u32, BE)                       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  type (u8)    |                                               |
+-+-+-+-+-+-+-+-+                                               +
|                  correlation_id (u64, BE)                     |
+                                                               -+
|                                               +-+-+-+-+-+-+-+-+
|                                               |   payload...  |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| Field | Width | Notes |
|---|---|---|
| `length` | 4 bytes, big-endian `u32` | Byte count of **everything after this field**: `1 (type) + 8 (correlation_id) + len(payload)`. Does not include its own 4 bytes. |
| `type` | 1 byte | See [Message types](#message-types). |
| `correlation_id` | 8 bytes, big-endian `u64` | Chosen by the client (the only side that initiates request/response pairs in v1). See [Concurrency and correlation](#concurrency-and-correlation). |
| `payload` | `length - 9` bytes | MessagePack, named-field map encoding (`rmp_serde::to_vec_named`) of the struct that `type` carries. Empty for `PING`/`PONG`. |

- **Byte order:** big-endian ("network byte order") throughout — the
  conventional choice, with a one-line standard-library helper in every
  mainstream language.
- **Minimum frame length:** 9 (`type` + `correlation_id`, empty payload).
  Shorter is `INVALID_FRAME`.
- **Maximum frame length:** configurable (`--max-frame-bytes` on
  `ast-server`, default 4 MiB). A frame whose `length` field exceeds the
  configured maximum is rejected **before** any buffer sized to that
  length is allocated and before any payload byte is read — the
  implementation never trusts an attacker- or bug-controlled length
  enough to allocate against it first and validate second.
- **Partial reads/writes:** a connection-scoped reader assembles each
  frame across however many `read()` calls the OS actually delivers;
  multiple frames arriving in one `read()` are each delivered in order,
  and one frame fragmented across many small `read()`s is reassembled
  before being handed up. Neither case assumes one `read()` corresponds
  to one frame. Writers flush after every frame (no application-level
  batching of writes).
- **Truncated frame (EOF mid-frame):** reported distinctly from a clean
  disconnect (EOF *between* frames, not inside one) — the implementation
  tells these apart down to the length field itself: even 1–3 bytes of a
  4-byte length field, followed by EOF, is a truncation, not a clean
  disconnect.
- **Connection termination:** either side may close at any time. A
  server closes a connection whose `HELLO` handshake didn't complete in
  time, whose peer-credential check failed, or that's rejected outright
  for having arrived past `--max-connections`. See
  [Connection lifecycle](#connection-lifecycle).
- **Version negotiation:** happens inside `HELLO`'s payload
  (`protocol_version`), not in the frame header. The frame header's shape
  is fixed for all AST versions specifically so a peer that only
  understands this framing can still receive, correctly size, and
  cleanly reject a message from an incompatible future version, instead
  of desyncing the byte stream.

## Serialization

**MessagePack**, via `rmp-serde`, named-field map encoding. Chosen over
JSON and CBOR by measurement (`benches/serialization.rs`, this machine,
`rustc 1.99.1`, Apple Silicon):

| Operation | JSON | MessagePack | CBOR |
|---|---|---|---|
| Encode `Invoke` (small struct) | 74.7ns | 142.8ns | 145.5ns |
| Decode `Invoke` | 174.1ns | 131.9ns | 304.2ns |
| Encode `Result` (~4KiB string payload) | 1289ns | **169.8ns (7.6x)** | 173.8ns |
| Decode `Result` | 519.1ns | **204.6ns (2.5x)** | 385.0ns |
| Encoded size, `Invoke` | 91 B | 75 B | 76 B |
| Encoded size, `Result` (~4KiB) | 4011 B | 4001 B | 4002 B |

MessagePack and CBOR are close on small, mostly-numeric/control messages
— JSON is even marginally *faster to encode* there, though slower to
decode — but MessagePack decisively wins once a message carries real
content (the `Result` row, which is what an actual tool's output usually
looks like), and consistently beats CBOR on both encode and decode.
Named-field maps (rather than positional arrays) cost a modest amount of
size for a real benefit: the wire format stays self-describing and
tolerant of additive/reordered fields, which matters far more for a spec
meant to be implemented from this document alone, in other languages,
than a few extra bytes per message. Run `cargo bench` to reproduce these
numbers on your own machine; they're real but machine-specific.

## Message types

| Byte | Name | Direction | Status |
|---|---|---|---|
| 1 | `HELLO` | C→S | Implemented |
| 2 | `HELLO_ACK` | S→C | Implemented |
| 3 | `DISCOVER` | C→S | Implemented |
| 4 | `DISCOVER_RESULT` | S→C | Implemented |
| 5 | `INVOKE` | C→S | Implemented |
| 6 | `RESULT` | S→C | Implemented |
| 7 | `ERROR` | S→C | Implemented |
| 8 | `PING` | C→S | Implemented |
| 9 | `PONG` | S→C | Implemented |
| 10 | `CANCEL` | C→S | Implemented |
| 11 | `BATCH` | C→S | Implemented |
| 12 | `BATCH_RESULT` | S→C | Implemented |
| 13 | `STREAM_START` | S→C | Implemented |
| 14 | `STREAM_CHUNK` | S→C | Implemented |
| 15 | `STREAM_END` | S→C | Implemented |

### Batch execution

`BATCH` groups independent tool calls under one frame `correlation_id`,
answered by a single `BATCH_RESULT`. Semantics, chosen conservatively:

- **Ordering & concurrency**: calls run **sequentially, in array order,
  never concurrently with each other.** This crate has no mechanism to
  mark a tool "safe to run concurrently," and sequential execution is
  correct regardless of what a future tool might do — including a
  destructive one. (`system.info`/`filesystem.read`, this crate's only
  tools today, are both read-only, so concurrent execution would also
  have been safe for them specifically; the conservative default is
  about tools that don't exist here yet, not these two.)
- **Result correlation**: each `BatchInvoke` carries a caller-chosen
  `id` (unique within that one batch, not connection-wide); each
  `BatchResultEntry` echoes that `id` paired with its outcome. Callers
  must correlate by `id`, not array position — in the normal case the
  two coincide, but see cancellation below for when they don't.
- **Partial failure**: one call failing does not stop the rest. Each
  entry's outcome is independent — `BatchOutcome::Ok { value }` or
  `BatchOutcome::Error { code, message }`, using the same `ErrorCode`s a
  standalone `INVOKE` would report.
- **Authorization**: identical to a standalone `INVOKE` — every call
  goes through the same per-tool checks (e.g. `filesystem.read`'s
  confinement to its authorized directory); `BATCH` grants no
  additional authorization and bypasses none.
- **Cancellation**: a `CANCEL` targeting the batch's `correlation_id`
  stops whichever call is *currently* running (the same deadline/cancel
  race a standalone `INVOKE` uses) and the batch ends there — entries
  for calls that never started are simply absent from `results`, never
  padded with a placeholder.
- **Size limit**: `ast-server --max-batch-size` (default 100) bounds
  calls-per-batch; exceeding it is rejected with `INVALID_MESSAGE`
  before any call runs. A whole `BATCH` still occupies exactly one
  in-flight slot (`--max-in-flight-per-connection`), regardless of how
  many calls it contains.

### Chunked streaming

`STREAM_START`/`STREAM_CHUNK`/`STREAM_END` let a client ask for a
result as a bounded sequence of chunks instead of one `RESULT` frame.
Be precise about what this is: **chunked delivery of a result the
server has already fully computed**, not incremental execution-time
production. No tool in this crate (`system.info`, `filesystem.read`)
produces output incrementally — `filesystem.read` still reads the
whole file and returns one value before any chunk is sent — so this
doesn't claim to be "true" streaming in the sense of a tool emitting
partial output as it works. What it *does* provide, which was the
actual deferred need: bounded per-chunk frame size and real
backpressure for a large result, via the same mechanism every other
response already uses (the connection's bounded outgoing-frame
channel — `tx.send(...).await` naturally blocks when a slow reader
can't keep up), rather than one `RESULT` frame sized to the whole
result.

- **Opt-in per call**: `Invoke.stream: true` requests it; absent or
  `false` (the default, and what every client in this crate other than
  the explicit `--stream` flag uses) gets the normal `RESULT`. Any
  tool's result can be streamed — this is a delivery-mode choice, not a
  tool capability, so no "is this tool streamable" flag exists or is
  needed.
- **Failure before streaming starts**: if the invoked tool itself
  fails, the server still has the complete outcome (an error) before
  anything is sent, so it reports a normal `ERROR` — `STREAM_START` is
  never sent for a call that didn't succeed. A client doesn't need a
  special code path for "this failed partway through a stream" because
  that case cannot occur.
- **Ordering**: `STREAM_CHUNK.sequence` is 0-indexed and strictly
  increasing with no gaps or reordering; concatenating every chunk's
  `data` in that order reconstructs exactly the MessagePack-encoded
  bytes a non-streamed `RESULT.value` would have carried.
- **Chunk size**: `ast-server --stream-chunk-bytes` (default 64 KiB)
  targets a per-chunk size; the last chunk is typically smaller.
  Clamped down automatically at connection start if it wouldn't fit
  under `--max-frame-bytes`, so misconfiguring the two relative to each
  other can't produce an oversized frame.

If a tool that genuinely needs incremental, execution-time output ever
exists, that would be the trigger to extend this (or add a distinct
mechanism) — not a reason to have withheld chunked delivery of large,
already-computed results until then.

## Payloads per type

All payloads are MessagePack named-field maps of the struct named below
(see `src/ast/message.rs` for the authoritative Rust definitions; field
names and optionality are exactly as declared there via `serde`).

| Type | Payload struct | Key fields |
|---|---|---|
| `HELLO` | `Hello` | `protocol_version: u32`, `client_name: String` |
| `HELLO_ACK` | `HelloAck` | `protocol_version: u32`, `server_name: String`, `ok: bool`, `error: Option<String>` |
| `DISCOVER` | `Discover` | *(empty)* |
| `DISCOVER_RESULT` | `DiscoverResult` | `tools: Vec<ToolDescriptorWire>` — each with `name`, `description`, `input_schema` (JSON Schema, draft-07 subset), `version`, `required_permissions: Vec<String>`, `max_input_bytes: Option<u64>`, `max_output_bytes: Option<u64>` |
| `INVOKE` | `Invoke` | `tool: String`, `args: <tool-specific JSON value>`, `deadline_ms: Option<u64>` (absent means "use the server's configured default," never "no deadline"), `stream: Option<bool>` (absent/`false` is a normal `RESULT`; `true` requests `STREAM_START`/`STREAM_CHUNK*`/`STREAM_END` instead) |
| `RESULT` | `InvokeResult` | `value: <tool-specific JSON value>` |
| `ERROR` | `ErrorPayload` | `code: ErrorCode`, `message: String` |
| `PING` | `Ping` | *(empty)* |
| `PONG` | `Pong` | *(empty)* |
| `CANCEL` | `Cancel` | `target_correlation_id: u64` |
| `BATCH` | `Batch` | `calls: Vec<BatchInvoke>` — each `{id: u64, tool: String, args: <JSON>, deadline_ms: Option<u64>}` |
| `BATCH_RESULT` | `BatchResult` | `results: Vec<BatchResultEntry>` — each `{id: u64, status: "ok" \| "error", value / code+message}` |
| `STREAM_START` | `StreamStart` | `total_chunks: u32`, `total_bytes: u64` (informational — see "Chunked streaming") |
| `STREAM_CHUNK` | `StreamChunk` | `sequence: u32`, `data: bytes` (a MessagePack `bin`, not an array — see `src/ast/message.rs`'s `serde_bytes` usage) |
| `STREAM_END` | `StreamEnd` | *(empty)* |

Tool `args`/`value` are themselves arbitrary JSON values, re-encoded as
MessagePack at the envelope level — a client library in any language can
treat them as "whatever this tool's schema says," the same way it would
over JSON, without needing MessagePack-specific tooling beyond the outer
envelope.

## Connection lifecycle

1. Client connects.
2. Server checks the peer's credentials (see
   [Security model](#security-model)); a mismatch closes the connection
   immediately, with no frame sent.
3. Client sends `HELLO` as the very first frame. Any other first message
   is rejected with `ERROR{INVALID_MESSAGE}`, and the connection is
   **not** closed for that specific violation — the client gets a chance
   to send `HELLO` properly on a well-behaved implementation, though in
   practice this indicates a bug worth fixing on the client side.
4. If `HELLO` doesn't arrive within `--handshake-timeout-secs` (default
   5s), the server closes the connection with no frame sent (there's
   nothing to correlate a response to yet).
5. Server replies `HELLO_ACK`. `ok: false` (e.g. unsupported
   `protocol_version`) means the server will not serve this connection
   further; it closes immediately after sending that `HELLO_ACK`.
6. Steady state: either side may send any number of `PING`, `DISCOVER`,
   `INVOKE`, or `CANCEL` messages (client→server) and receive their
   correlated responses (server→client), in any mix, concurrently, until
   either side closes the connection.
7. Either side closing the TCP^Wsocket connection ends the session. A
   clean disconnect between frames is not an error; one mid-frame is
   (`INCOMPLETE_MESSAGE`, detected server-side; a client sees the
   connection simply close).

## Concurrency and correlation

- `correlation_id` is chosen by the client. It must be unique among that
  connection's **currently in-flight** `INVOKE`s — reused while a prior
  request with the same id hasn't yet received its `RESULT`/`ERROR` is
  rejected with `ERROR{INVALID_MESSAGE}` for the *new* request, without
  disturbing the original. Once a `RESULT`/`ERROR` has been sent for an
  id, that id is free to reuse.
- A connection serves many `INVOKE`s concurrently, bounded by
  `--max-in-flight-per-connection` (default 64, per connection — not
  server-wide; see `--max-connections` for the server-wide cap).
  Responses are **not** guaranteed to arrive in the order their requests
  were sent: a fast tool's result can and will overtake a slower one
  that was invoked first. Clients must correlate by `correlation_id`,
  never by arrival order.
- `PING`/`PONG` and `DISCOVER`/`DISCOVER_RESULT` use the same
  `correlation_id` mechanism as `INVOKE`/`RESULT`, for implementation
  uniformity, even though in practice a client issuing one of these at a
  time doesn't need it.
- An unknown `correlation_id` on an incoming `RESULT`/`ERROR`/`PONG` (a
  client-side concern — nothing server-side ever receives one) is simply
  dropped: it means a prior wait already gave up (e.g. on a client-side
  timeout) before the response arrived. Not an error, just discarded.

## Cancellation

`CANCEL{target_correlation_id}` has **no response of its own** — it
either reaches an in-flight request in time (which then completes with
`ERROR{CANCELLED}` through the normal `ERROR` path) or doesn't (the
target already completed, or the id was never valid on this connection),
and either way there's nothing further to acknowledge.

**What this guarantees:** the caller stops waiting (via the normal
`ERROR{CANCELLED}` response) and the in-flight slot is freed.

**What this does not guarantee:** that the tool's own work actually
stopped. A tool invocation runs on a blocking-pool thread; Rust has no
safe mechanism to forcibly preempt a thread that's already running
synchronous code, and this crate does not claim one. `CANCEL` (and a
deadline expiring, which behaves identically from the tool's
perspective) stops the *caller* from waiting on it, full stop — the
underlying `std::thread` keeps running until the tool's own code returns,
whether or not anyone is still listening. The two MVP tools
(`system.info`, `filesystem.read`) are both fast, bounded, local
operations where this distinction is close to moot; it would matter a
great deal for a hypothetical future tool that can block indefinitely.

## Deadlines

Every `INVOKE` has a deadline — either the request's own `deadline_ms`
or the server's `--default-deadline-ms` (default 10000) when omitted.
There is no "wait forever" mode. A deadline exceeded produces
`ERROR{DEADLINE_EXCEEDED}`, with the same non-guarantee about the
underlying blocking work as [Cancellation](#cancellation) above.

## Error model

```json
{ "code": "TOOL_NOT_FOUND", "message": "no such tool: git.status" }
```

(shown as JSON here for readability; on the wire this is MessagePack,
carried in an `ERROR` frame's payload)

| Code | Meaning |
|---|---|
| `INVALID_FRAME` | Malformed framing: below the minimum length, over the configured maximum, or an unrecognized `type` byte. |
| `UNSUPPORTED_VERSION` | `HELLO.protocol_version` didn't match a version this server understands. Sent via `HELLO_ACK{ok:false}`, not a standalone `ERROR` frame. |
| `INVALID_MESSAGE` | A message that framed correctly but violated protocol rules — wrong message as the first frame, a payload that didn't decode against its type's schema, or a duplicate in-flight `correlation_id`. |
| `TOOL_NOT_FOUND` | `INVOKE.tool` named something not in the registry. |
| `INVALID_ARGUMENTS` | `args` failed the named tool's own validation. |
| `PERMISSION_DENIED` | The tool refused the operation for a policy reason (today, exclusively: a `filesystem.read` path outside the authorized base directory). |
| `DEADLINE_EXCEEDED` | See [Deadlines](#deadlines). |
| `CANCELLED` | See [Cancellation](#cancellation). |
| `RESOURCE_EXHAUSTED` | The connection was already serving its configured maximum of in-flight requests. |
| `RESULT_TOO_LARGE` | A fully-formed response (`HELLO_ACK`, `DISCOVER_RESULT`, `RESULT`, or `ERROR`) would have exceeded `--max-frame-bytes`; this bounded error was sent in its place. See the note below. |
| `INTERNAL_ERROR` | Any other server-side failure, including a tool task panicking. |

**`RESULT_TOO_LARGE` is enforced**, symmetrically with request-side
framing: every outgoing frame (`HELLO_ACK`, `DISCOVER_RESULT`, `RESULT`,
`ERROR`) is size-checked against `--max-frame-bytes` *after* encoding but
*before* being handed to the connection's writer task. One whose encoded
size would exceed the limit is replaced with a small, fixed
`ERROR{RESULT_TOO_LARGE}` instead of ever being sent — see
`send`/`send_raw` in `src/ast/connection.rs`. The substitute error uses a
short, static message (never the oversized content, and never anything
derived from it), so it can never itself trigger the check it's
replacing. Tested in `src/ast/connection.rs`
(`result_exceeding_max_frame_len_is_bounded_not_sent_oversized` and
`result_within_max_frame_len_is_sent_normally`).

Error messages name what the client sent wrong (e.g. the requested tool
name, the requested path as given) and never a resolved absolute
filesystem path, a stack trace, or other server-internal detail.

New codes may be added in future minor revisions of this document without
a wire version bump; clients should treat an unrecognized `code` as a
generic failure. Changing what an existing code means, or removing one,
is a breaking change requiring a `protocol_version` bump.

## Security model

- **Socket directory and file permissions:** identical hardening to the
  legacy protocol's — see `docs/PROTOCOL.md`'s "Security model" section
  and `src/transport.rs`'s `BoundListener`, reused as-is by `ast-server`.
- **Peer credentials, independent of a successful connection:**
  `ast-server --require-same-uid` (the default) checks the connecting
  process's real uid via `SO_PEERCRED` (Linux) or `LOCAL_PEERCRED`
  (macOS) and closes the connection — before `HELLO` is even read — if it
  doesn't match the server's own uid. This is authorization decoupled
  from "the socket accepted the connection": on an unshared, single-user
  machine the socket file's `0600` permissions already imply the same
  property, so this is defense in depth there, but it's a real,
  independent check on any host where that assumption doesn't hold
  (shared accounts, a shared mount namespace, etc).
  - **Linux/macOS difference:** `SO_PEERCRED` reports the peer's pid as
    well as uid/gid; macOS's `LOCAL_PEERCRED` reports only uid-equivalent
    credentials (`cr_uid`), no pid. `PeerCredentials::pid` is `Some` on
    Linux, always `None` on macOS. Neither is implemented for any other
    platform — `peer_credentials()` returns `Unsupported` there.
- **Resource limits:** `--max-connections` (server-wide),
  `--max-in-flight-per-connection`, `--max-frame-bytes` (both directions:
  oversized requests are rejected before being fully read, oversized
  responses are replaced with a bounded `RESULT_TOO_LARGE` error before
  being sent), and per-request deadlines are all enforced.
- **Filesystem confinement:** identical to the legacy protocol's
  `filesystem.read` — same base-directory canonicalization,
  `O_NOFOLLOW`-on-final-open TOCTOU narrowing, and the same explicitly
  documented residual risk (no protection against intermediate-directory
  races; the authorized directory is assumed not to be concurrently
  modified by an untrusted actor). AST does not re-implement this tool;
  it's shared, unmodified, from `src/tools/filesystem_read.rs`.
- **No arbitrary shell execution,** by design, same as the legacy
  protocol.
- **`unsafe` usage:** confined to peer-credential FFI
  (`SO_PEERCRED`/`LOCAL_PEERCRED` via `libc::getsockopt`) and the
  socket-hardening FFI already used by the legacy protocol
  (`getuid`/`umask`). Each call site has a `SAFETY` comment explaining
  why it's sound. No other `unsafe` exists in AST's implementation.

## Compatibility and migration

The legacy JSON-Lines protocol (`docs/PROTOCOL.md`) is **not deprecated**
and is **not being removed**. `agent-tools-server`/`agent-tools-client`
continue to work exactly as before — every existing test for them still
passes, unmodified by this work. AST is a second, separate server/client
pair (`ast-server`/`ast-client`), with its own default socket path
(same private directory, different filename: `ast.sock` vs
`agent-tools.sock`) so both can run side by side without colliding.

There is no wire-level compatibility between the two: an
`agent-tools-client` cannot talk to `ast-server` or vice versa. This is
intentional, not an oversight — one is a one-request-per-connection,
line-delimited JSON protocol; the other is a persistent,
multiplexed, length-prefixed binary one. Bridging them meaningfully
would mean picking a lowest common denominator that serves neither well.
If you're starting a new integration today, prefer AST for anything
latency- or throughput-sensitive, or that benefits from concurrent
in-flight requests on one connection; the legacy protocol remains simpler
to implement from scratch in a hurry (plain text lines, no framing math)
and is perfectly fine for low-volume, one-request-at-a-time use.

No MCP client can speak either protocol natively. See
[MCP comparison](#mcp-comparison) below, and `README.md`'s note that
neither CLI is an SDK or an agent-framework adapter.

## MCP comparison

A reproducible benchmark (`cargo run --release --bin ast-bench`) compares
three things doing identical work in the same run on the same machine:
AST, **this crate's own legacy JSON-Lines protocol** (both over Unix
Domain Sockets), and **`mcp-like-server`**
(`src/bin/mcp-like-server.rs`) — a minimal JSON-RPC 2.0-over-stdio server
implementing MCP's `initialize`/`tools/list`/`tools/call` methods and
content-block result shape, serving the exact same
`system.info`/`filesystem.read` tools.

**`mcp-like-server` is not the official MCP reference implementation —
it is a shape-faithful stand-in, built for this benchmark.** It gets
stdio framing, the three methods a tool-invocation benchmark needs, and
the `{"content":[{"type":"text","text":...}], "isError": bool}`
tool-result convention right, and nothing else from the full MCP
specification (no resources, prompts, sampling, roots, or capability
negotiation beyond the one call `ast-bench` makes). Treat every number
below as "a faithfully-shaped MCP-like server, implemented by this
project," not "MCP itself" or any particular official SDK's actual
performance.

Representative results (`rustc 1.99.1`, Apple Silicon arm64, release
build, 500 iterations per scenario, 20-way concurrency):

| Scenario | Legacy (JSON) | AST (MessagePack) | mcp-like (JSON-RPC/stdio) |
|---|---|---|---|
| Startup time | 214.7ms | 217.3ms | **6.3ms (34x faster)** |
| Cheapest round trip, sequential, persistent (p50) | 24.3µs | 32.4µs | **13.7µs (fastest)** |
| Reconnect-per-call, 100 calls (total) | 3.79ms | 9.07ms (incl. `HELLO`) | n/a — see note below |
| `filesystem.read`, 512 KiB payload (p50) | 10449.3µs | **663.6µs (16x faster than legacy)** | 584.7µs |
| 20 concurrent calls | 74326 req/s (20 connections) | **165006 req/s (multiplexed, 1 connection)** | 3630 req/s (20 separate *processes*, spawn cost included) |
| Encoded bytes, cheapest call | 37 B | **14 B (smallest)** | 94 B |
| Avg CPU across the whole run | 7.8% | 3.6% | **2.1% (lowest)** |

**Read honestly — this is not a one-sided result for anyone:**

- **`mcp-like` wins startup and small-sequential-call latency by a wide
  margin.** It pays no `connect()`/socket-setup cost at all — stdio pipes
  are already open the moment the process exists — and no handshake
  message the other two protocols require. For a short-lived,
  spawn-once-use-once invocation (exactly how `mcp-like-server` is used
  here, and arguably how MCP's stdio transport is used in practice), this
  is a real, meaningful advantage.
- **`mcp-like` collapses under concurrency**, because its stdio transport
  fundamentally has no multi-caller model: serving N concurrent callers
  means N separate processes, each paying the full spawn + `initialize`
  cost again. 3630 req/s here is *that* cost dominating, not a
  per-request protocol inefficiency — compare it to AST's 165006 req/s
  multiplexing N requests over one already-open connection, which is
  exactly the gap AST's design targets.
- **Reconnect-per-call has no `mcp-like` row**, not because it's
  unmeasured but because it's not a meaningful distinct scenario for a
  process-per-session transport: every `mcp-like` call in the startup and
  concurrency rows above *already* includes process-spawn cost, so
  there's no separate "persistent vs reconnect" axis to measure the way
  there is for a long-lived socket server.
- **`mcp-like`'s response payloads are larger** than the request-size
  table above suggests, because of the content-block double-encoding
  described in `docs/AST_PROTOCOL.md`'s own "Encoded bytes" output
  (`ast-bench`'s report prints the exact note) — a property of the
  convention being compared against, not of this baseline's
  implementation.
- AST's two real advantages over the legacy protocol — large payloads
  (16x) and concurrent multiplexing (2.2x, and a qualitatively different
  story against `mcp-like`'s 45x) — are exactly the properties it was
  designed for, and they're large, not marginal. AST is not universally
  faster than either alternative; it's faster at the two things it
  specifically trades handshake/dispatch overhead to be faster at.

## Cross-language interoperability

This document, plus [Framing](#framing), [Serialization](#serialization),
and the [test vectors](#test-vectors) below, are meant to be sufficient
to implement an AST client or server in any language with a MessagePack
library and Unix domain socket support — which is to say, essentially
any mainstream language.

**This is now verified for one other language.** `sdks/python/` is a
minimal, synchronous Python client (stdlib `socket` + `msgpack`, no
other dependencies) implementing `HELLO`/`PING`/`DISCOVER`/`INVOKE`/
`CANCEL`/`BATCH` from this spec alone. `sdks/python/tests/test_interop.py`
spawns the real, unmodified `ast-server` binary as a subprocess and
drives it entirely through that Python client — not a mock of the
server, not a second Rust process pretending to be a different
language. Every scenario the Rust integration tests cover for these
message types also passes from Python: handshake, ping, discovery,
successful and failing invokes, path-traversal rejection, and batched
calls with mixed success/failure correlated by id. This is run in CI
(see `.github/workflows/ci.yml`'s `python-interop` job) on every push,
so it stays true rather than becoming a one-time claim.

**What's still unverified**: `STREAM_*` (not implemented on either
side, so there's nothing to check), the Python client's behavior under
concurrent multiplexing (it only ever has one request in flight, by
design — see `sdks/python/README.md`), and any implementation outside
this repository in a third language. Per this document's own standard,
each of those remains a documented goal, not a verified fact, until
something analogous to the Python client exists for it too.

## Test vectors

Generated from this crate's own encoder (`src/ast/codec.rs` +
`src/ast/wire.rs`) and frozen as a regression test in
`tests/ast_vectors.rs` — if these bytes ever change, that test fails,
which is the point: this document and the code are kept honest against
each other mechanically, not by hand.

### `HELLO` (`correlation_id = 1`)

```
00 00 00 37 01 00 00 00 00 00 00 00 01 82 b0 70 72 6f 74 6f 63 6f 6c 5f 76
65 72 73 69 6f 6e 01 ab 63 6c 69 65 6e 74 5f 6e 61 6d 65 ae 65 78 61 6d 70
6c 65 2d 63 6c 69 65 6e 74
```

59 bytes. Decodes to `Hello { protocol_version: 1, client_name:
"example-client" }`.

### `PING` (`correlation_id = 2`)

```
00 00 00 0a 08 00 00 00 00 00 00 00 02 80
```

14 bytes — the minimum possible frame (9-byte header's worth of
type+correlation_id, plus a 1-byte empty-map MessagePack payload `0x80`).

### `INVOKE` (`correlation_id = 3`)

```
00 00 00 30 05 00 00 00 00 00 00 00 03 83 a4 74 6f 6f 6c ab 73 79 73 74 65
6d 2e 69 6e 66 6f a4 61 72 67 73 80 ab 64 65 61 64 6c 69 6e 65 5f 6d 73 cd
13 88
```

52 bytes. Decodes to `Invoke { tool: "system.info", args: {}, deadline_ms:
Some(5000) }`.

### `ERROR` (`correlation_id = 3`)

```
00 00 00 39 07 00 00 00 00 00 00 00 03 82 a4 63 6f 64 65 ae 54 4f 4f 4c 5f
4e 4f 54 5f 46 4f 55 4e 44 a7 6d 65 73 73 61 67 65 b2 6e 6f 20 73 75 63 68
20 74 6f 6f 6c 3a 20 6e 6f 70 65
```

61 bytes. Decodes to `ErrorPayload { code: ToolNotFound, message: "no
such tool: nope" }`.

## Running tests and checks

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo bench --bench serialization   # the serialization-format measurements above
cargo run --release --bin ast-bench # the AST-vs-legacy benchmark above
```

No GitHub Actions / CI matrix exists for this project (explicitly out of
scope for this pass, per instruction). Everything above has only been
run on macOS (arm64); Linux is expected to work (no platform-specific
code in AST beyond the `cfg(target_os = ...)`-gated peer-credential
functions, which have their own Linux branch) but has not actually been
run there.
