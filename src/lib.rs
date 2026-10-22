//! `agent-socket-tools`: MCP-free local IPC for AI agents invoking local
//! tools, over a Unix Domain Socket. Two coexisting protocols:
//!
//! - [`ast`]: AST (Agent Socket Transport) — length-prefixed framing,
//!   MessagePack payloads, persistent connections, concurrent in-flight
//!   requests, deadlines, and best-effort cancellation. The actively
//!   developed protocol — see `docs/AST_PROTOCOL.md`.
//! - [`protocol`] / [`transport`]: the original, simpler JSON-Lines
//!   protocol. Kept as-is and still fully supported — see
//!   `docs/PROTOCOL.md` — not deprecated, just not where new protocol
//!   features land. See `docs/AST_PROTOCOL.md`'s "Compatibility and
//!   migration" section for why both exist rather than one replacing the
//!   other.
//! - [`tools`]: the `Tool` trait, the registry, and the two MVP tools —
//!   shared by both protocols, since tool implementations are
//!   transport-independent by design.
//! - [`config`]: socket-path resolution and tunables shared by both.

pub mod ast;
pub mod config;
pub mod protocol;
pub mod tools;
pub mod transport;
