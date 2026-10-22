use super::{Tool, ToolError};
use serde_json::{json, Value};

/// Returns basic, non-sensitive information about the host OS and architecture.
pub struct SystemInfoTool;

impl Tool for SystemInfoTool {
    fn name(&self) -> &str {
        "system.info"
    }

    fn description(&self) -> &str {
        "Returns the operating system family and CPU architecture of the host running the server."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {}
        })
    }

    fn invoke(&self, args: Value) -> Result<Value, ToolError> {
        if !matches!(&args, Value::Object(m) if m.is_empty()) && !args.is_null() {
            return Err(ToolError::InvalidArgs(
                "system.info takes no arguments".into(),
            ));
        }
        Ok(json!({
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "family": std::env::consts::FAMILY,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_os_and_arch() {
        let tool = SystemInfoTool;
        let result = tool.invoke(json!({})).unwrap();
        assert_eq!(result["os"], std::env::consts::OS);
        assert_eq!(result["arch"], std::env::consts::ARCH);
    }

    #[test]
    fn accepts_null_args() {
        let tool = SystemInfoTool;
        assert!(tool.invoke(Value::Null).is_ok());
    }

    #[test]
    fn rejects_non_empty_args() {
        let tool = SystemInfoTool;
        assert!(tool.invoke(json!({"unexpected": true})).is_err());
    }
}
