//! `agent-socket-tools`: a minimal, MCP-free way for local AI agents to
//! discover and invoke a small set of authorized tools over a Unix Domain
//! Socket.
//!
//! The crate is split into four areas, matching the project's module
//! boundaries:
//!
//! - [`protocol`]: the wire format (request/response types, error codes).
//! - [`transport`]: socket lifecycle and line-delimited JSON framing.
//! - [`tools`]: the `Tool` trait, the registry, and the two MVP tools.
//! - [`config`]: socket-path resolution and tunables.

pub mod config;
pub mod protocol;
pub mod tools;
pub mod transport;
