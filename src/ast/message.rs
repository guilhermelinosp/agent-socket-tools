//! AST Wire Protocol v1 — message types and their payloads.
//!
//! Every payload struct here is encoded as a MessagePack named-field map
//! (see [`crate::ast::codec`]). `HELLO`/`HELLO_ACK`, `DISCOVER`, `INVOKE`,
//! `RESULT`, `ERROR`, `PING`/`PONG`, `CANCEL`, `BATCH`/`BATCH_RESULT`, and
//! `STREAM_START`/`STREAM_CHUNK`/`STREAM_END` are all implemented. See
//! `docs/AST_PROTOCOL.md`'s "Chunked streaming" section for exactly what
//! the stream types do and don't provide (chunked delivery of an
//! already-computed result, not incremental execution-time production).

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const AST_PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    Hello = 1,
    HelloAck = 2,
    Discover = 3,
    DiscoverResult = 4,
    Invoke = 5,
    Result = 6,
    Error = 7,
    Ping = 8,
    Pong = 9,
    Cancel = 10,
    Batch = 11,
    BatchResult = 12,
    StreamStart = 13,
    StreamChunk = 14,
    StreamEnd = 15,
}

impl MessageType {
    pub fn from_u8(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Hello),
            2 => Some(Self::HelloAck),
            3 => Some(Self::Discover),
            4 => Some(Self::DiscoverResult),
            5 => Some(Self::Invoke),
            6 => Some(Self::Result),
            7 => Some(Self::Error),
            8 => Some(Self::Ping),
            9 => Some(Self::Pong),
            10 => Some(Self::Cancel),
            11 => Some(Self::Batch),
            12 => Some(Self::BatchResult),
            13 => Some(Self::StreamStart),
            14 => Some(Self::StreamChunk),
            15 => Some(Self::StreamEnd),
            _ => None,
        }
    }
}

/// Sent once, immediately after connecting, before anything else.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub protocol_version: u32,
    pub client_name: String,
}

/// The server's reply to `HELLO`. `ok: false` means the server won't
/// serve this connection at all (e.g. an unsupported `protocol_version`);
/// the connection is closed right after this is sent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloAck {
    pub protocol_version: u32,
    pub server_name: String,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Empty request; included as a distinct type (rather than overloading
/// `Ping`) so `docs/AST_PROTOCOL.md`'s per-type table stays self-describing
/// even though the payload carries nothing today.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Discover {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDescriptorWire {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub version: String,
    pub required_permissions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_input_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoverResult {
    pub tools: Vec<ToolDescriptorWire>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invoke {
    pub tool: String,
    pub args: Value,
    /// Milliseconds from when the server receives this message. Absent
    /// means "use the server's configured default" (see
    /// `--default-deadline-ms` in `ast-server --help`), not "no deadline" —
    /// this protocol has no unbounded-wait mode, by design.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u64>,
    /// `true` asks the server to deliver the result as
    /// `STREAM_START`/`STREAM_CHUNK*`/`STREAM_END` instead of a single
    /// `RESULT`, regardless of the result's size. Absent or `false`
    /// means the normal, unchunked `RESULT` — the default, and what
    /// every other client in this crate uses. See
    /// `docs/AST_PROTOCOL.md`'s "Chunked streaming" section for why this
    /// is a delivery-mode choice, not a tool capability: any tool's
    /// result can be streamed, because streaming chunks the server's
    /// already-fully-computed result rather than requiring the tool
    /// itself to produce output incrementally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvokeResult {
    pub value: Value,
}

/// First message of a streamed response. `total_bytes` is the size, in
/// bytes, of the MessagePack-encoded result value *before* chunking —
/// informational (e.g. for a progress bar), not load-bearing: a
/// conforming client reassembles the result from exactly the chunks it
/// receives, in `sequence` order, regardless of whether their total
/// matches this number.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamStart {
    pub total_chunks: u32,
    pub total_bytes: u64,
}

/// One chunk of a streamed response's encoded result value. `sequence`
/// is 0-indexed and strictly increasing with no gaps — a conforming
/// server never reorders or drops a chunk. `data` is a raw slice of the
/// MessagePack-encoded result value (`rmp_serde::to_vec_named`'s output,
/// cut at arbitrary byte boundaries, not aligned to MessagePack element
/// boundaries) — concatenating every chunk's `data` in `sequence` order
/// reconstructs exactly the bytes a non-streamed `RESULT.value` would
/// have carried, decodable as that same value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamChunk {
    pub sequence: u32,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

/// Terminates a stream that completed successfully. (A tool failure is
/// never reported this way — see `docs/AST_PROTOCOL.md`'s "Chunked
/// streaming" section: a failing call never starts a stream in the
/// first place, since the server already has the complete result,
/// success or `ErrorPayload`, before any `STREAM_START` is sent.)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StreamEnd {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    InvalidFrame,
    UnsupportedVersion,
    InvalidMessage,
    ToolNotFound,
    InvalidArguments,
    PermissionDenied,
    DeadlineExceeded,
    Cancelled,
    ResourceExhausted,
    ResultTooLarge,
    InternalError,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorPayload {
    pub code: ErrorCode,
    pub message: String,
}

/// Empty in both directions.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Ping {}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Pong {}

/// One call within a `BATCH`. `id` is chosen by the client and must be
/// unique *within this batch* (not connection-wide like
/// `correlation_id` — a fresh `Batch` can reuse `id`s a previous one
/// used). Otherwise identical in meaning to the matching fields on
/// [`Invoke`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchInvoke {
    pub id: u64,
    pub tool: String,
    pub args: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u64>,
}

/// Grouped execution of independent tool calls under a single frame
/// `correlation_id`. See `docs/AST_PROTOCOL.md`'s "Batch execution"
/// section for the full ordering/concurrency/partial-failure contract —
/// in short: calls run **sequentially, in array order, never
/// concurrently with each other**, by design (this crate has no
/// mechanism yet to mark a tool "safe to run concurrently," and the
/// conservative default is correct until one exists), and a failure in
/// one call does not stop the rest — except a `CANCEL` targeting this
/// batch's `correlation_id`, which does stop it, after whichever call is
/// currently running finishes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Batch {
    pub calls: Vec<BatchInvoke>,
}

/// One call's outcome within a [`BatchResult`]. `id` matches the
/// `BatchInvoke.id` it answers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum BatchOutcome {
    Ok { value: Value },
    Error { code: ErrorCode, message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResultEntry {
    pub id: u64,
    #[serde(flatten)]
    pub outcome: BatchOutcome,
}

/// Response to `BATCH`. `results` always has one entry per call that
/// actually ran — fewer than `calls.len()` iff the batch was cancelled
/// partway through (see [`Batch`]'s doc comment), never more, and never
/// reordered relative to the calls that did run (though see [`Batch`]
/// regarding why callers should still correlate by `id`, not position).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResult {
    pub results: Vec<BatchResultEntry>,
}

/// Best-effort request to stop an in-flight `INVOKE`. See
/// `docs/AST_PROTOCOL.md`'s "Cancellation" section for exactly what this
/// does and does not guarantee — in short: it guarantees the *caller*
/// stops waiting and the slot is freed, never that a blocking tool's
/// already-running work actually stops.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cancel {
    pub target_correlation_id: u64,
}
