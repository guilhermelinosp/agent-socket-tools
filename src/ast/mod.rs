//! AST (Agent Socket Transport) — the length-prefixed, MessagePack-coded
//! successor to this crate's original JSON-Lines protocol (kept, as-is,
//! under [`crate::protocol`]/[`crate::transport`] — see `docs/AST_PROTOCOL.md`
//! §"Compatibility and migration" for why both exist side by side).

pub mod client;
pub mod codec;
pub mod connection;
pub mod message;
pub mod peercred;
pub mod wire;
