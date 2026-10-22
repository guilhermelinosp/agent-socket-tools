//! AST Wire Protocol v1 — message types and their payloads.
//!
//! Every payload struct here is encoded as a MessagePack named-field map
//! (see [`crate::ast::codec`]). `HELLO`/`HELLO_ACK`, `DISCOVER`, `INVOKE`,
//! `RESULT`, `ERROR`, `PING`/`PONG`, and `CANCEL` are implemented. `BATCH`
//! and `STREAM_*` are reserved type codes — see `docs/AST_PROTOCOL.md` for
//! why they're deferred rather than implemented in v1.

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
    // Reserved, not implemented in v1 — see docs/AST_PROTOCOL.md §"Deferred".
    // Batch = 11,
    // StreamStart = 12,
    // StreamChunk = 13,
    // StreamEnd = 14,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvokeResult {
    pub value: Value,
}

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

/// Best-effort request to stop an in-flight `INVOKE`. See
/// `docs/AST_PROTOCOL.md`'s "Cancellation" section for exactly what this
/// does and does not guarantee — in short: it guarantees the *caller*
/// stops waiting and the slot is freed, never that a blocking tool's
/// already-running work actually stops.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cancel {
    pub target_correlation_id: u64,
}
