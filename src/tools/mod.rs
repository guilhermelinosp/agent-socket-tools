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
