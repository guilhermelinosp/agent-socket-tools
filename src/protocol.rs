//! Wire protocol: one JSON object per line, UTF-8, newline-terminated.
//!
//! Version 1 defines three operations: `discover`, `invoke`, `health`.
//! Requests and responses are correlated by `id`, which the client chooses
//! and the server echoes back unchanged.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Current protocol version. Bumped on any breaking wire-format change.
pub const PROTOCOL_VERSION: u32 = 1;

/// A single request line sent by the client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub version: u32,
    pub id: String,
    pub op: Operation,
    /// Required for `invoke`, ignored otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    /// Required for `invoke`, ignored otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Discover,
    Invoke,
    Health,
}

/// A single response line sent by the server, correlated to a `Request` by `id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub version: u32,
    pub id: String,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ResponseError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseError {
    pub code: ErrorCode,
    pub message: String,
}

/// Stable, machine-matchable error codes. New variants are additive; existing
/// variants never change meaning once released.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// The request line was not valid JSON, or did not match the `Request` schema.
    InvalidRequest,
    /// `version` did not match a version this server understands.
    UnsupportedVersion,
    /// `tool` named a tool that is not registered.
    ToolNotFound,
    /// `args` failed validation against the tool's input schema.
    InvalidArgs,
    /// The tool rejected the operation for policy/authorization reasons
    /// (e.g. path outside the authorized base directory).
    Unauthorized,
    /// The tool ran but failed (e.g. file not found, I/O error).
    ToolError,
    /// The request did not complete within the configured timeout. This
    /// covers both "the request bytes never fully arrived in time" and
    /// "the tool didn't finish in time" — see `docs/PROTOCOL.md` for which
    /// phase applies.
    Timeout,
    /// A request line exceeded the configured maximum message size before
    /// a newline was found.
    MessageTooLarge,
    /// A fully-formed response would have exceeded the configured maximum
    /// message size. The server sends this bounded error instead of the
    /// oversized payload.
    ResponseTooLarge,
    /// The connection was closed because the socket reached end-of-file
    /// with a partial message already buffered (no newline ever arrived).
    IncompleteMessage,
    /// A request line contained bytes that were not valid UTF-8.
    InvalidUtf8,
    /// The server was already serving its configured maximum number of
    /// concurrent connections and closed this one immediately.
    ConnectionLimitExceeded,
    /// Any other server-side failure, including a tool task panicking and
    /// I/O errors on the socket itself that aren't one of the more
    /// specific codes above.
    Internal,
}

impl Response {
    pub fn ok(id: impl Into<String>, result: Value) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            id: id.into(),
            ok: true,
            result: Some(result),
            error: None,
        }
    }

    pub fn err(id: impl Into<String>, code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            id: id.into(),
            ok: false,
            result: None,
            error: Some(ResponseError {
                code,
                message: message.into(),
            }),
        }
    }

    /// Serializes to a single JSON line, including the trailing newline.
    pub fn to_line(&self) -> String {
        let mut s = serde_json::to_string(self).expect("Response always serializes");
        s.push('\n');
        s
    }
}

impl Request {
    /// Parses a single line of input (without requiring the caller to strip
    /// the trailing newline).
    pub fn from_line(line: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(line.trim_end_matches(['\n', '\r']))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrips_through_json() {
        let req = Request {
            version: PROTOCOL_VERSION,
            id: "req-001".into(),
            op: Operation::Invoke,
            tool: Some("git.status".into()),
            args: Some(serde_json::json!({"repository": "/workspace/project"})),
        };
        let line = serde_json::to_string(&req).unwrap();
        let parsed = Request::from_line(&line).unwrap();
        assert_eq!(parsed.id, "req-001");
        assert_eq!(parsed.op, Operation::Invoke);
        assert_eq!(parsed.tool.as_deref(), Some("git.status"));
    }

    #[test]
    fn from_line_strips_trailing_newline() {
        let line = r#"{"version":1,"id":"a","op":"health"}"#.to_string() + "\n";
        let parsed = Request::from_line(&line).unwrap();
        assert_eq!(parsed.op, Operation::Health);
    }

    #[test]
    fn from_line_rejects_garbage() {
        assert!(Request::from_line("not json").is_err());
    }

    #[test]
    fn response_ok_has_no_error_field_in_json() {
        let resp = Response::ok("req-001", serde_json::json!({"clean": true}));
        let line = resp.to_line();
        assert!(!line.contains("\"error\""));
        assert!(line.ends_with('\n'));
    }

    #[test]
    fn response_err_has_no_result_field_in_json() {
        let resp = Response::err("req-001", ErrorCode::ToolNotFound, "unknown tool");
        let line = resp.to_line();
        assert!(!line.contains("\"result\""));
        assert!(line.contains("\"tool_not_found\""));
    }

    #[test]
    fn error_code_serializes_snake_case() {
        let code = serde_json::to_string(&ErrorCode::InvalidArgs).unwrap();
        assert_eq!(code, "\"invalid_args\"");
    }
}
