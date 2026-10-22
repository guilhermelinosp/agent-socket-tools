//! Tool trait and registry. Each tool validates its own arguments against
//! its declared input schema and is responsible for its own authorization
//! decisions (e.g. path confinement).

mod filesystem_read;
mod system_info;

pub use filesystem_read::FilesystemReadTool;
pub use system_info::SystemInfoTool;

use serde_json::Value;
use std::collections::BTreeMap;

/// A policy/authorization failure, distinct from a tool-internal error.
#[derive(Debug, thiserror::Error)]
pub enum ToolError {
    #[error("invalid arguments: {0}")]
    InvalidArgs(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("tool failed: {0}")]
    Failed(String),
}

pub trait Tool: Send + Sync {
    /// Dotted tool name, e.g. `"system.info"`.
    fn name(&self) -> &str;

    /// One-line human description, returned by `discover`.
    fn description(&self) -> &str;

    /// JSON Schema (draft-07 subset) describing accepted `args`.
    fn input_schema(&self) -> Value;

    /// Validates `args` and executes the tool synchronously.
    ///
    /// Tools in this MVP are all synchronous and non-blocking in practice
    /// (in-memory lookups, single bounded file reads), so the registry runs
    /// them via `spawn_blocking` rather than requiring every tool to be
    /// written against an async trait.
    fn invoke(&self, args: Value) -> Result<Value, ToolError>;

    /// Returns `Some(self)` only for a tool whose output can be *produced*
    /// incrementally (see [`StreamingTool`]). Most tools don't override
    /// this — their result is small and/or inherently non-incremental, so
    /// an `INVOKE` requesting `stream: true` against them still gets
    /// ordinary chunked delivery of their fully-computed result (see
    /// `docs/AST_PROTOCOL.md`'s "Chunked streaming" section). Overriding
    /// this is only for a tool where true incremental production is both
    /// possible and worth the extra complexity — not a default every tool
    /// should reach for.
    fn as_streaming(&self) -> Option<&dyn StreamingTool> {
        None
    }
}

/// A tool whose output can be produced incrementally, chunk by chunk, as
/// it's generated — not merely chunked *after* being fully computed (that
/// generic, no-opt-in-required chunking is what `INVOKE.stream: true`
/// already gets against any [`Tool`] via `src/ast/connection.rs`'s
/// `send_streamed_result`). This is for the narrower case where a tool can
/// actually avoid holding its whole result in memory at once.
pub trait StreamingTool: Tool {
    /// Validates `args` and opens an incremental read, exactly as
    /// [`Tool::invoke`] would validate and execute — a failure here is a
    /// normal [`ToolError`], same meaning as a failing `invoke`. Must do
    /// all authorization/validation synchronously before returning; the
    /// returned reader is assumed already-authorized.
    fn open_stream(&self, args: Value) -> Result<StreamingToolOpen, ToolError>;
}

/// What [`StreamingTool::open_stream`] returns: everything about the
/// result that's known upfront (`metadata`), plus a reader for the one
/// field whose value is produced incrementally (`field_name`, `reader`).
/// The full result, once reassembled, is `metadata` with `field_name` set
/// to the concatenation of everything `reader` ever returned, decoded per
/// the reader's own contract (e.g. UTF-8 text).
pub struct StreamingToolOpen {
    pub metadata: Value,
    pub field_name: &'static str,
    pub reader: Box<dyn IncrementalBytes>,
}

impl std::fmt::Debug for StreamingToolOpen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingToolOpen")
            .field("metadata", &self.metadata)
            .field("field_name", &self.field_name)
            .field("reader", &"<dyn IncrementalBytes>")
            .finish()
    }
}

/// Bounded, incremental byte production for one streamed field.
///
/// Implementations must enforce their own content validation (e.g. UTF-8)
/// *as bytes are produced*, across chunk boundaries — never by buffering
/// everything and validating once at the end, which would defeat the
/// entire point of this trait existing instead of just calling
/// [`Tool::invoke`].
pub trait IncrementalBytes: Send {
    /// Total size in bytes, if known upfront. Used for `StreamStart`'s
    /// `total_bytes`, which clients enforce exactly: the chunks this
    /// reader yields must add up to it. Never report a size the reader
    /// will not produce.
    fn total_bytes_hint(&self) -> Option<u64>;

    /// Returns the next chunk of up to `max_len` bytes, or `Ok(None)`
    /// once exhausted. May return fewer than `max_len` bytes even before
    /// exhaustion (e.g. because validating a content boundary held some
    /// bytes back for the next call).
    fn next_chunk(&mut self, max_len: usize) -> Result<Option<Vec<u8>>, ToolError>;
}

#[derive(Debug, serde::Serialize)]
pub struct ToolDescriptor {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

/// Immutable set of tools available to a server instance.
pub struct ToolRegistry {
    tools: BTreeMap<String, Box<dyn Tool>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            tools: BTreeMap::new(),
        }
    }

    pub fn register(&mut self, tool: Box<dyn Tool>) {
        let name = tool.name().to_string();
        self.tools.insert(name, tool);
    }

    pub fn discover(&self) -> Vec<ToolDescriptor> {
        self.tools
            .values()
            .map(|t| ToolDescriptor {
                name: t.name().to_string(),
                description: t.description().to_string(),
                input_schema: t.input_schema(),
            })
            .collect()
    }

    pub fn get(&self, name: &str) -> Option<&dyn Tool> {
        self.tools.get(name).map(|b| b.as_ref())
    }
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}
