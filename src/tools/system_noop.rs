use super::{Tool, ToolError};
use serde_json::{json, Value};

/// Does nothing and returns an empty object. Exists only so benchmarks can
/// measure pure protocol/transport overhead without any tool work.
pub struct NoopTool;

impl Tool for NoopTool {
    fn name(&self) -> &str {
        "system.noop"
    }

    fn description(&self) -> &str {
        "Does nothing and returns an empty object; exists to measure pure protocol/transport overhead."
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
                "system.noop takes no arguments".into(),
            ));
        }
        Ok(json!({}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_empty_object() {
        let tool = NoopTool;
        assert_eq!(tool.invoke(json!({})).unwrap(), json!({}));
    }

    #[test]
    fn accepts_null_args() {
        let tool = NoopTool;
        assert!(tool.invoke(Value::Null).is_ok());
    }

    #[test]
    fn rejects_non_empty_args() {
        let tool = NoopTool;
        assert!(tool.invoke(json!({"unexpected": true})).is_err());
    }
}
