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

MessagePack and CBOR are close to each other on small, mostly-numeric/
control messages — JSON is clearly faster to encode there (74.7ns vs
142.8ns, ~1.9x), though slower to decode (174.1ns vs 131.9ns) — but
MessagePack decisively wins once a message carries real content (the
`Result` row, which is what an actual tool's output usually looks
like), and consistently beats CBOR on both encode and decode.
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
There are **two distinct modes**, both opted into the same way
(`Invoke.stream: true`) and both using the same three message types —
`StreamStart.raw` (present or absent) tells a client which one it's
getting for a given call:

**1. Chunked delivery of an already-computed result** (`raw` absent).
Most tools use this: the server has already fully computed the
result — `system.info` always does, and `filesystem.read` does too
when streaming isn't requested — and this mode just splits the
already-encoded `RESULT.value` into bounded pieces rather than sending
one `RESULT` frame sized to the whole result. Not "true" streaming in
the sense of a tool emitting partial output as it works; what it
*does* provide: bounded per-chunk frame size and real backpressure,
via the same mechanism every other response already uses (the
connection's bounded outgoing-frame channel — `tx.send(...).await`
naturally blocks when a slow reader can't keep up; a reader that stalls on one frame for longer than `--write-timeout-secs` has its connection closed).

**2. Genuine incremental production** (`raw` present — implemented for
`filesystem.read`). The tool produces its output chunk by chunk, as it
goes, and the server never holds the complete streamed field in
memory at once — see [`src/tools/mod.rs`'s `StreamingTool`
trait](../src/tools/mod.rs). `STREAM_CHUNK.data` in this mode is the
tool's own raw output bytes for `raw.field_name` (UTF-8 text, for
`filesystem.read`'s `contents` field) — **not** a slice of an encoded
`Value` — and `raw.metadata` carries every other field the full result
needs (`path`, `size_bytes`). A conforming client reassembles the full
result by concatenating every chunk's `data` in `sequence` order,
decoding it per the field's own contract (UTF-8 text, for
`filesystem.read`), and inserting it into `raw.metadata` under
`raw.field_name`.

`filesystem.read`'s genuinely-streamed path also has its own, much
larger size ceiling (`MAX_STREAM_READ_BYTES`, 256 MiB) than its
non-streaming `invoke` path (`MAX_READ_BYTES`, 1 MiB) — deliberately:
peak server memory for the streamed path is bounded by chunk size
regardless of this ceiling, unlike `invoke`, where the ceiling itself
is the only thing bounding memory. The two caps are independent; a
file past `invoke`'s cap can still be read via streaming.

Shared across both modes:

- **Opt-in per call**: `Invoke.stream: true` requests it; absent or
  `false` (the default, and what every client in this crate other than
  the explicit `--stream` flag uses) gets the normal `RESULT`. Whether
  a given call ends up using mode 1 or mode 2 is decided by the server
  (does the invoked tool implement `StreamingTool`?), not by the
  client — a client that only understands "concatenate chunks,
  msgpack-decode the result" can be upgraded to support mode 2 without
  the wire format itself changing shape in an incompatible way, since
  both modes use the same three message types; a client that predates
  `StreamStart.raw` simply doesn't know what to do with a stream that
  sets it (this crate's own Rust and Python clients both handle it).
- **Failure before streaming starts**: if the invoked tool itself
  fails (including a genuinely-streaming tool's own validation, e.g.
  path traversal or non-UTF-8 content — `open_stream` validates
  synchronously before any chunk is produced, same as `invoke`), the
  server still has the complete outcome (an error) before anything is
  sent, so it reports a normal `ERROR` — `STREAM_START` is never sent
  for a call that didn't succeed.
- **Mid-transfer failure has no equivalent guarantee, for genuine
  streaming specifically**: once `STREAM_START` has gone out, there is
  no way to retract it. A deadline or cancellation firing *during* a
  genuinely-streamed transfer simply stops sending further frames (no
  `STREAM_END`, no error frame) rather than retroactively reporting
  `DeadlineExceeded`/`Cancelled` for a response shape already committed
  to — a documented limitation, not an oversight. A client's own
  overall timeout on the call eventually surfaces this (as a timeout,
  not a precise error code). Mode 1 doesn't have this gap, because by
  the time any chunk is sent the tool has already either fully
  succeeded or fully failed.
- **Ordering**: `STREAM_CHUNK.sequence` is 0-indexed and strictly
  increasing with no gaps or reordering in either mode.
- **Totals (normative)**: `StreamStart.total_bytes` is a promise, not a
  hint. A conforming server MUST send `STREAM_CHUNK.data` whose lengths
  sum to exactly `total_bytes` before `STREAM_END`. A conforming client
  MUST treat any divergence as a protocol error: a `sequence` that is not
  the next expected value, or a `STREAM_END` arriving when the received
  byte count differs from `total_bytes`. The Rust and Python clients in
  this repository both do this (`StreamProtocolViolation` in Rust,
  `AstProtocolError("stream protocol violation: ...")` in Python).
  `total_chunks` remains only an estimate: UTF-8 boundary handling in
  mode 2 can change the real chunk count.
- **Consuming a stream without aggregating it (Rust)**:
  `AstClient::invoke_stream_chunks` returns a `StreamHandle` whose
  `start()` exposes the `STREAM_START` (including `raw` metadata) and
  whose `next_chunk()` yields each validated chunk as it arrives, until
  `Ok(None)` after `STREAM_END`. `invoke_stream` is the aggregating form
  of the same handle, so both share one validation path. Dropping a
  handle before the stream ends deregisters it and sends a best-effort
  `CANCEL`, so the connection stays usable. A held-but-unread handle
  backs up the client's reader for that connection until it is read or
  dropped.
- **Chunk size**: `ast-server --stream-chunk-bytes` (default 64 KiB)
  targets a per-chunk size; the last chunk is typically smaller.
  Clamped down automatically at connection start if it wouldn't fit
  under `--max-frame-bytes`, so misconfiguring the two relative to each
  other can't produce an oversized frame.

Extending genuine incremental streaming to a future tool means
implementing `StreamingTool` for it — there is no generic way to
"turn on" mode 2 for a tool that only implements the plain `Tool`
trait, by design: a tool author has to actually write the incremental
reader, not merely flip a flag on a tool that still computes its whole
result up front.

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
| `STREAM_START` | `StreamStart` | `total_chunks: u32` (estimate only), `total_bytes: u64` (exact: the chunks must sum to it — see "Chunked streaming"), `raw: Option<RawStreamInfo>` (present only for genuine incremental streaming — `{metadata: <JSON>, field_name: String}`) |
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
   [Threat model and security](#threat-model-and-security)); a mismatch closes the connection
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

## Deadlines and cancellation

**Deadlines**

- `INVOKE.deadline_ms` is relative. v1 has no absolute deadline.
- The clock starts when the server begins handling the request, not when the frame arrives on the wire.
- When `deadline_ms` is absent, `--default-deadline-ms` applies (default 10000). There is no "no deadline" mode.
- A streamed `INVOKE` has one budget covering the stream open and every chunk read. Exceeding it sends `ERROR{DEADLINE_EXCEEDED}` mid-stream.
- The effective deadline is the requested (or default) value clamped to `--max-deadline-ms` (default 60000). It applies to each `INVOKE`, to each call of a `BATCH`, and to streams.
- An effective deadline of 0 ms is answered `DEADLINE_EXCEEDED` at once. The tool is not started and no execution slot is taken. **Behavior change:** earlier versions started the tool anyway with a 0 ms deadline; it now never runs.
- A `deadline_ms` above `--max-deadline-ms` is reduced to that maximum silently. The client gets no notice of the reduction. The only visible sign is `DEADLINE_EXCEEDED` when the call runs past the reduced value.
- The deadline is a **response** deadline: the client receives `DEADLINE_EXCEEDED` when it expires, even if the tool is still running.

**Cancellation**

- `CANCEL{target_correlation_id}` has no response of its own.
- A live target completes with `ERROR{CANCELLED}`. A `CANCEL` for an unknown or already-completed id is ignored.
- If completion, cancellation, or a deadline race, the first one wins. Each request receives exactly one terminal frame (`RESULT`, `BATCH_RESULT`, `STREAM_END`, or `ERROR`).
- `CANCEL` is handled by the connection's asynchronous read loop, so a slow tool never delays it.
- The server sets a cancellation token (`Tool::invoke_with_cancel`) whenever a request is answered without its tool's own result: deadline expiry, `CANCEL`, connection failure, and server shutdown. A tool that checks the token at its checkpoints stops early. Streaming tools do not receive a token yet.

**Execution is bounded, not interrupted**

- Each tool execution runs on a blocking thread and takes one of the server-wide `--max-blocking-tasks` slots (default 64). The slot is held until the tool actually returns, even after its caller was answered `DEADLINE_EXCEEDED` or `CANCELLED`.
- While every slot is taken, new executions (`INVOKE`, `BATCH` calls, stream opens and chunk reads) fail with `RESOURCE_EXHAUSTED` without starting.
- Rust cannot stop a running thread, and the server does not try. A tool that ignores its token keeps its slot until its own code returns, so tools must stay short and bounded, or check the token.
- A `BATCH` never keeps more than one blocking execution alive. Its calls run one at a time, and a call waits for the previous call's tool to return before it starts. The wait is bounded by the call's own deadline; a call that cannot start in time gets `DEADLINE_EXCEEDED` and its tool is never started. A call's total time can therefore reach its deadline for the wait plus its deadline for the run.

**Disconnects and shutdown**

- A clean end of the peer's stream (FIN, half-close) between frames drains and does **not** cancel: the connection stays open and each in-flight request still gets its response, until its terminal frame or its deadline. A non-cooperative tool therefore keeps the connection open until it returns or its deadline expires.
- After a clean end of stream, the first failed write (a write error or a write timeout) cancels the connection's remaining in-flight requests. Their terminal frames are then attempted as usual.
- **Known limit:** a Unix socket cannot tell a half-close from a full close until a write fails. If the peer has vanished completely, the server does not notice until it writes. Work for that connection continues until it completes or its deadline expires, bounded by `--max-deadline-ms` and the slots. The first failed write then cancels the rest.
- A read error (including a reset), an invalid or truncated frame, a failed write, or a write timeout ends the connection. Its in-flight requests are cancelled (token set, `CANCELLED` sent when the caller is still waiting), and their slots are released when their tools return.
- On `SIGINT`/`SIGTERM` the server stops accepting and cancels the in-flight requests of every connection. That includes connections that are half-closed and already past their clean end of stream. It then waits up to `--shutdown-grace-secs` (default 5) for connections to close, and exits. Tools still running at that point are abandoned with the process.

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
| `DEADLINE_EXCEEDED` | See [Deadlines and cancellation](#deadlines-and-cancellation). |
| `CANCELLED` | See [Deadlines and cancellation](#deadlines-and-cancellation). |
| `RESOURCE_EXHAUSTED` | The connection was already serving its configured maximum of in-flight requests, or every server-wide tool execution slot (`--max-blocking-tasks`) is taken. |
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

## Threat model and security

**Authentication vs authorization**

- `--require-same-uid` (default) authenticates the peer's OS uid through `SO_PEERCRED` (Linux) or `LOCAL_PEERCRED` (macOS), before `HELLO` is read. It checks only that the peer runs as the server's user.
- There is **no per-client and no per-tool authorization**. What a peer can do is limited by the tools registered on the server and by `filesystem.read`'s confinement. Any authenticated peer can invoke every registered tool.

**Trust boundary**

- Every process running as the server's uid is equally trusted and can invoke every tool.
- For different trust levels, run separate server instances (distinct socket directories and `--read-dir`) or use distinct OS users.

**Socket hardening**

- The socket directory is `0700` and the socket file `0600`, as in the legacy protocol (see `docs/PROTOCOL.md` and `src/transport.rs`).
- A stale socket file left by a dead server is reclaimed at bind. Anything else at that path is left alone.

**`filesystem.read` confinement**

- The base directory (`--read-dir`) is opened once at startup. Each request is resolved relative to that descriptor, one component at a time, with `openat` and `O_NOFOLLOW` on every component. The client's path never reaches a path-based syscall, so a directory swapped for a symlink during a request is refused, not followed.
- Refused before any filesystem access: empty paths, absolute paths (even ones inside the base), paths containing NUL, and any `..` component. `.` and empty components are ignored.
- Refused: any symlink in the requested path, including one that points inside the base. This is a deliberate rule, not a side effect.
- FIFOs, sockets and devices are refused without blocking. The final open uses `O_NONBLOCK`, and the regular-file check runs on the opened descriptor.
- Reads are bounded: 1 MiB for `INVOKE`, 256 MiB for streamed reads. A file that grows past the limit during an `INVOKE` read is an error, not a truncated result. Cancellation is checked between 64 KiB blocks.
- Error messages contain only the client's own text or a generic phrase. They never contain the host's base path.
- Residual risks, not solved by the server:
  - **Hardlinks.** A hardlink inside the tree to a file outside it cannot be detected. A writer who can create that link must already be able to write into the tree and read the target.
  - **Mounts.** A bind mount or other mount point inside the tree is followed. Only symlinks are refused.
  - **Same-UID processes.** A process running as the server's uid can read the same files directly and can also talk to the socket. The security boundary is the UID, not this tool. Keep `--read-dir` writable only by principals you trust at the server's level.

**Error messages**

- `ToolError::Failed` messages are sent to the peer verbatim. Tools must not place secrets (credentials, tokens, unrelated file contents) in error messages.

**Untrusted input**

- Frame size, batch length and output, stream size, in-flight count, and duplicate correlation ids are bounded or rejected. See [Resource limits](#resource-limits).

**Other**

- No arbitrary shell execution, by design.
- `unsafe` is confined to peer-credential FFI, the socket-hardening FFI shared with the legacy protocol, and the single `openat` wrapper behind `filesystem.read`. Each call site has a `SAFETY` comment.

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

No MCP client can speak either protocol natively. The optional
`ast-mcp-adapter` ([MCP_ADAPTER.md](MCP_ADAPTER.md)) bridges MCP stdio to a
running `ast-server` for interoperability; it is a separate binary outside
this protocol and adds a hop. See [MCP comparison](#mcp-comparison) below, and
`README.md`'s note that neither CLI is an SDK or an agent-framework adapter.

## MCP comparison

Measured with `ast-bench` (see [BENCHMARKS.md](BENCHMARKS.md) for methodology; raw results in `docs/results/bench-2026-10-22.{md,json}`): release build at commit 4f716f1, Apple M3 Pro (macOS arm64), rustc 1.99.1, 500 iterations × 5 repeats per cell after 100 warmup calls, medians across repeats, zero errors in every cell. The MCP baseline is a **real** server built on the official `rmcp` SDK (`=3.6.0`) over persistent stdio, serving the same tools as AST. `mcp-like-server` is a hand-rolled synthetic stand-in kept only as a lower bound for "JSON-RPC over stdio with almost no framework"; it is not a competitor. Single machine, single session: treat differences under ~10% as noise.

**Sequential latency, one persistent connection (p50, µs)**

| Call | legacy JSON-Lines/UDS | AST/UDS | MCP real (rmcp)/stdio | MCP synthetic |
|---|---|---|---|---|
| `system.noop` | 22.4 | 33.7 | 30.9 | 13.5 |
| `system.info` | 21.9 | 34.1 | 30.1 | 13.8 |
| `filesystem.read` tiny | 49.2 | 59.8 | 57.0 | 38.2 |
| `filesystem.read` 16 KiB | 77.6 | 82.2 | 80.7 | 64.5 |
| `filesystem.read` 512 KiB | 10524.0 | 670.6 (streamed: 731.3) | 636.2 | 625.3 |
| discovery | 18.6 | 23.8 | 33.8 | 17.0 |
| AST `ping` (protocol floor) | — | 22.0 | — | — |

**Concurrency: `system.noop`, closed loop, C requests in flight, throughput (ops/s)**

| C | AST, 1 multiplexed connection | AST, C connections | legacy, C connections | MCP real, 1 stdio, pipelined | MCP real, 1 process per caller (excl. spawn) |
|---|---|---|---|---|---|
| 1 | 37,724 | 38,323 | 48,559 | 32,294 | 32,926 |
| 4 | 91,963 | 114,017 | 130,336 | 44,120 | 59,298 |
| 16 | 117,296 | 123,158 | 130,056 | 44,021 | 65,192 |
| 64 | 108,742 | 142,076 | 147,510 | 44,028 | 57,746 |

The last column is a deployment model (one server process per caller), not a protocol comparison; the other columns all use persistent connections.

**Other**

| | legacy | AST | MCP real |
|---|---|---|---|
| Batch of 20 `system.noop` (wall µs) | — | 201.8 (BATCH frame); 182.7 (20 concurrent invokes); 695.6 (20 sequential) | 426.5 (20 pipelined; MCP has no batch) |
| Server CPU per `system.noop` (µs) | 18.5 | 25.0 | 41.0 |
| Peak RSS (MiB) | 8.0 | 7.9 | 9.1 |
| Startup to first completed call (ms) | 8.5 | 5.9 | 2.7 |

**Performance targets set for this work, and what was measured**

| Target (AST vs real MCP, persistent stdio) | Result |
|---|---|
| ≥2× lower p50 for small calls | **Not achieved.** AST is ~9% slower on `system.noop` (33.7 vs 30.9 µs), ~13% slower on `system.info`, ~5% slower on a tiny read, and at parity within noise for 16 KiB and 512 KiB reads. It is faster only on discovery (1.42×). The target is not reachable with this architecture: AST `ping`, which does no tool work at all, already costs 22.0 µs, so 2× below MCP's 30.9 µs (≈15 µs) lies below the protocol's own floor. A defensible target is parity within ~10% for single sequential calls. See the wire-path optimization subsection below: sequential latency reached parity with the real MCP server in a second run (32.7 vs 32.1 µs), still not 2×. |
| ≥2× throughput under concurrency | **Achieved** with one AST connection against one pipelined MCP connection from C=4 (2.08×), C=16 (2.66×) and C=64 (2.47×); not at C=1 (1.17×). MCP real plateaus near 44k ops/s regardless of C. Even the one-process-per-caller MCP deployment tops out near 65k ops/s, below a single AST connection at C≥16. |
| Lower CPU per small call | **Achieved vs MCP real** (25.0 vs 41.0 µs, −39%); **not** vs the legacy JSON-Lines server (18.5 µs). Allocations per call were not measured. |
| No unexplained p99 regression | **Not met for sequential calls**: noop p99 51.5 µs vs 40.2 µs for MCP real. Under concurrency AST's tail is lower (C=16: 262 vs 537 µs). |
| Predictable memory | Bounded by configuration (see "Resource limits") and exercised by tests; steady RSS is similar to or lower than MCP real (7.9 vs 9.1 MiB). Not covered by the benchmark. |

**What the numbers do and do not say**

- For single sequential calls with trivial payloads, AST is not faster than a lean MCP stdio server; the legacy JSON-Lines protocol over UDS is the lowest-latency option here. A binary encoding does not buy latency on tiny messages.
- AST's measured advantages are multiplexing many requests over one connection (2.1–2.7× the throughput of a pipelined MCP connection, with lower tail latency), lower server CPU per call than the real MCP server, faster discovery, and BATCH (2.1× faster than 20 pipelined MCP requests, but no faster than 20 concurrent invokes on the same AST connection). Its 512 KiB read is ~16× faster than the legacy protocol because the legacy protocol JSON-escapes the whole payload; against MCP real it is within noise.
- Semantic features not benchmarked: per-request deadlines, cancellation, peer-credential authentication, bounded streaming and batch budgets. MCP can pipeline requests on one connection, but this server does not run them concurrently (throughput is flat in C).
- **Correction.** An earlier version of this section reported MCP startup of ~6 ms versus ~230 ms for AST and a ~76× concurrency advantage. Both were artifacts of the previous harness (a synthetic stand-in as the MCP baseline, and MCP measured as 20 separate processes against a multiplexed AST connection). They are superseded by the numbers above.
- Request size on the wire, from the previous harness (the encodings have not changed): `filesystem.read` request 57 B for AST, 91 B legacy, 117 B MCP JSON-RPC. Wire size is not part of the current suite.

### After the wire-path optimization (commit 26fe02d)

Two changes to `src/ast/wire.rs` and the connection setup: frames with payloads up to 16 KiB are written with one `write(2)` instead of two, and every connection's read half goes through a `BufReader`, with the payload read straight into its final buffer (no intermediate `Vec`, no copy). Wire bytes are unchanged (`tests/ast_vectors.rs` passes untouched). Same harness, same machine, same parameters, run separately from the table above; the real MCP server was unchanged and serves as the control (its cells moved between −0.2% and +6.7%, so changes smaller than ~7% are not distinguishable from noise).

| Cell (p50 µs unless noted) | before (da7aa95) | after (26fe02d) | change |
|---|---|---|---|
| `system.noop` sequential | 33.7 | 32.7 | −3% (noise) |
| `system.info` sequential | 34.1 | 32.3 | −5% (noise) |
| AST `ping` (protocol floor) | 22.0 | 19.3 | −12% |
| C=1, 1 multiplexed connection | 25.9 | 21.4 | −17% |
| C=4, 1 multiplexed connection | 42.1 | 30.9 | −27% |
| C=4 throughput, multiplexed (ops/s) | 91,963 | 124,545 | +35% |
| C=64, 1 multiplexed connection | 580.7 | 460.5 | −21% |
| Batch of 20: 20 sequential invokes | 695.6 | 604.4 | −13% |
| 512 KiB read, streamed | 731.3 | 660.1 | −10% |
| Server CPU per `system.noop` (µs) | 25.0 | 23.5 | −6% |
| Allocator calls per `system.noop` (alloc-probe) | 18 | 16 | −2 |

Reading: sequential single-call latency did not change beyond noise. In this run AST `system.noop` p50 was 32.7 µs against 32.1 µs for the real MCP server (30.9 µs in the earlier run), i.e. parity within noise, not a measured win. The gains are in concurrent and multiplexed cells, where per-frame syscalls matter: AST on one connection now reaches 2.8× the throughput of a pipelined MCP connection at C=4 (124,545 vs 43,803 ops/s; 2.1× before). One cell moved the wrong way — C=64 with one connection per caller, 411.0 → 457.8 µs (+11%) — and was not re-run, so it is reported as unexplained rather than as noise. The −2 allocator calls were not attributed to the read or the write side. Raw results: `docs/results/bench-2026-10-22-wire-opt.{md,json}`, `docs/results/alloc-baseline.json`, `docs/results/alloc-after-wire.json`.

### Final comparison (commit 34f06ed)

The full table, methodology and objective status are in [ENGINEERING_REPORT.md](ENGINEERING_REPORT.md); raw data are in `docs/results/2026-10-22-34f06ed/`. This subsection supersedes the targets table above. The numbers in the earlier subsections are historical and dated.

- Sequential latency: parity with the real MCP server (`system.noop` p50 30.1 vs 31.0 µs), not the 2× target. AST `ping`, which does no tool work, costs 19.2 µs.
- Concurrent throughput: 2.8× (C=4), 2.7× (C=16) and 3.1× (C=64) the real MCP server, with lower p99 at C=16 and C=64.
- Server CPU per `system.noop`: 23.0 vs 42.0 µs (−45%).
- Startup is slower (5.67 vs 3.17 ms); peak RSS is a tie (9,184 vs 9,392 KiB).

## Cross-language interoperability

This document, plus [Framing](#framing), [Serialization](#serialization),
and the [test vectors](#test-vectors) below, are meant to be sufficient
to implement an AST client or server in any language with a MessagePack
library and Unix domain socket support — which is to say, essentially
any mainstream language.

**This is now verified for one other language.** `sdks/python/` is a
minimal, synchronous Python client (stdlib `socket` + `msgpack`, no
other dependencies) implementing `HELLO`/`PING`/`DISCOVER`/`INVOKE`/
`CANCEL`/`BATCH`/`STREAM_START`/`STREAM_CHUNK`/`STREAM_END` from this
spec alone — including both streaming modes, chunked-delivery and
genuine incremental streaming. `sdks/python/tests/test_interop.py`
spawns the real, unmodified `ast-server` binary as a subprocess and
drives it entirely through that Python client — not a mock of the
server, not a second Rust process pretending to be a different
language. Every scenario the Rust integration tests cover for these
message types also passes from Python: handshake, ping, discovery,
successful and failing invokes, path-traversal rejection, batched calls
with mixed success/failure correlated by id, and — the genuine-streaming
case specifically — a real 200 KB file streamed from the Rust server's
`filesystem.read` `StreamingTool` implementation and reassembled
correctly by the Python client's `raw`-mode handling. This is run in CI
(see `.github/workflows/ci.yml`'s `python-interop` job) on every push,
so it stays true rather than becoming a one-time claim.

**What's still unverified**: the Python client's behavior under
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

## Resource limits

Defaults are for `ast-server`. Values marked *fixed* are constants in the code and have no flag.

| Limit | Setting | Default | Behavior when exceeded |
|---|---|---|---|
| Connections | `--max-connections` | 256 | Extra connections are rejected. |
| In-flight requests per connection | `--max-in-flight-per-connection` | 64 | `RESOURCE_EXHAUSTED`. A `BATCH` occupies one slot. |
| Frame length, both directions | `--max-frame-bytes` | 4 MiB | Oversized inbound frame: `INVALID_FRAME`, then close. Oversized outbound frame: replaced by `RESULT_TOO_LARGE`. |
| Calls per `BATCH` | `--max-batch-size` | 100 | `INVALID_MESSAGE`. |
| Aggregate `BATCH` output | `--max-batch-output-bytes` | `--max-frame-bytes` | The call that would exceed it gets `RESULT_TOO_LARGE` and the batch stops. A 128-byte margin is kept for the envelope. |
| Stream chunk target | `--stream-chunk-bytes` | 64 KiB | Clamped below the frame limit at connection start. |
| Handshake | `--handshake-timeout-secs` | 5 | Connection closed if no valid `HELLO` arrives in time. |
| Idle | `--idle-timeout-secs` | 300 | Connection closed. Counts only while no request is in flight. |
| Write, per frame | `--write-timeout-secs` | 30 | Connection closed. |
| Default deadline | `--default-deadline-ms` | 10000 | `DEADLINE_EXCEEDED`. |
| Maximum deadline | `--max-deadline-ms` | 60000 | Larger requested deadlines are clamped to this value. |
| Tool executions, server-wide | `--max-blocking-tasks` | 64 | `RESOURCE_EXHAUSTED`. A slot is held until the tool returns. The blocking thread pool is sized to this value plus 8. |
| Shutdown grace period | `--shutdown-grace-secs` | 5 | Connections still open when it elapses are abandoned at exit. |
| Outgoing queue per connection | fixed | 64 frames | Bounded by frame count, not bytes. |
| `filesystem.read`, invoke | fixed (tool) | 1 MiB | The tool rejects larger files. |
| `filesystem.read`, streamed | fixed (tool) | 256 MiB | The tool rejects larger files. |
| Client streamed result | client `max_stream_bytes` | 256 MiB | The client rejects the stream. |
| Registered tools | `--tools` (comma-separated) | `system.info,system.noop,filesystem.read` | Not a limit, but the attack-surface setting. Unknown names fail at startup with exit code 2. Unlisted tools are not registered, so `INVOKE` answers `ToolNotFound`. `filesystem.read` is built only when listed, but `--read-dir` stays required. |

### In-flight slots and flow control

A request occupies one in-flight slot (`--max-in-flight-per-connection`) until the server starts writing its terminal frame: `RESULT`, `ERROR`, `BATCH_RESULT`, or the final `STREAM_END` or `ERROR` of a stream. The connection's writer returns the slot just before it writes that frame. A request refused for exceeding the limit receives an `ERROR` that does not take a slot.

Consequently, a client that never has more than `max-in-flight` requests without a received terminal frame is never refused with `RESOURCE_EXHAUSTED` for the in-flight limit, even when it sends its next request right after reading a response.

The outgoing queue per connection (64 frames) bounds what waits to be written. A queued terminal frame keeps its slot until the writer dequeues it, and a task blocked on a full queue keeps its slot too. A slow reader therefore cannot make the server accept an unbounded number of requests. Intermediate stream frames (`STREAM_START`, `STREAM_CHUNK`) take no slot.

## Known limitations

- Streaming: the Rust client's `invoke_stream` still preallocates up to the declared `total_bytes` (after checking it against `max_stream_bytes`, default 256 MiB) per in-flight stream; a hostile server can therefore make a client reserve that much per concurrent stream. The incremental `StreamHandle` API does not preallocate, and the Python client's `invoke_stream` does not either.
- Streaming totals in mode 2 come from the tool's `total_bytes_hint()`. A tool that returns `None` makes the server send `total_bytes: 0` with non-empty chunks, which conforming clients now reject as a protocol violation. No current tool does this (`filesystem.read` always reports its size), but a future one must report an exact total before it can use genuine streaming.
- Backpressure is bounded by frame count (the per-connection outgoing channel holds 64 frames), not bytes: with large `max_frame_bytes`, worst-case queued memory per connection is up to 64 × `max_frame_bytes`.
- BATCH has no aggregate server-side deadline: each call honors its own deadline, so a 100-call batch can run for the sum of its calls' deadlines. The client now sends a best-effort CANCEL when its own timeout fires.
- Tool executions are capped by `--max-blocking-tasks`, not by tool cooperation. A synchronous tool that ignores its token keeps its slot until it returns, so a few slow tools can make the server answer `RESOURCE_EXHAUSTED` to other requests.
- Streaming tools (`StreamingTool`) receive no cancellation token. Their `open_stream` and chunk reads cannot be interrupted cooperatively; they stop only when the caller is answered, and their slot is still held until the blocking call returns.
- A stream chunk read that finds every slot taken is a terminal ERROR (`RESOURCE_EXHAUSTED`), because `STREAM_START` has already been sent.
- On shutdown, a request whose tool is still running when the grace period ends never gets a terminal frame; the connection is abandoned with the process.
- BATCH serializes each entry twice (once to measure the running budget, once in the final BatchResult); unmeasured, a possible CPU optimization.
- Several buffer copies per frame on the hot path (`wire.rs` read path, chunk assembly in `filesystem_read.rs` and the client); unmeasured, candidates only if profiling shows them.
- The legacy JSON-Lines server rescans its read buffer from the start on every 4 KiB read while looking for a newline (O(n²) in the line length, bounded by the 1 MiB default message limit).
- `wire::read_frame` is not cancel-safe once it has consumed part of a frame; callers must treat a lost race as a reason to close the connection, never to retry the read (the idle timeout and the reader/writer race in `run_connection` are built that way).
- A mid-transfer failure of a genuinely streamed response is reported as a terminal ERROR, but a failure to enqueue STREAM_CHUNK/STREAM_END themselves (broken outgoing channel) is, by necessity, silent.
- The write timeout bounds each frame, not the whole transfer: a reader that drains just fast enough is never disconnected, and the request deadline does not cover frame writes.
- Performance claims in this document are from single runs on one machine (see "MCP comparison"); they are not statistical distributions.
