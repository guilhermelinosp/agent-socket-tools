use super::{Tool, ToolError};
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

/// Caps how much of a file is read into memory for a single `invoke`, to
/// bound server memory use regardless of the on-disk file size.
const MAX_READ_BYTES: u64 = 1024 * 1024; // 1 MiB

#[derive(Debug, Deserialize)]
struct Args {
    path: String,
}

/// Reads a UTF-8 text file from inside a single authorized base directory.
///
/// The base directory is fixed at construction time by the server operator,
/// never by the client. Every request path is resolved to an absolute,
/// symlink-free path and checked against that base directory before any
/// data is read, which also closes symlink-escape attacks: a symlink
/// *inside* the base directory that points *outside* it canonicalizes to a
/// path outside the base directory and is rejected the same way a literal
/// `../` traversal would be.
pub struct FilesystemReadTool {
    /// Canonicalized once at construction; `invoke` never re-reads this from
    /// the filesystem to avoid any race on the base directory itself.
    base_dir: PathBuf,
}

impl FilesystemReadTool {
    /// Fails if `base_dir` does not exist or cannot be canonicalized.
    pub fn new(base_dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let base_dir = base_dir.as_ref().canonicalize()?;
        Ok(Self { base_dir })
    }

    fn resolve(&self, requested: &str) -> Result<PathBuf, ToolError> {
        if requested.is_empty() {
            return Err(ToolError::InvalidArgs("path must not be empty".into()));
        }
        let candidate = if Path::new(requested).is_absolute() {
            PathBuf::from(requested)
        } else {
            self.base_dir.join(requested)
        };

        let canonical = candidate.canonicalize().map_err(|_| {
            ToolError::Failed(format!(
                "cannot read {requested:?}: not found or inaccessible"
            ))
        })?;

        if !canonical.starts_with(&self.base_dir) {
            return Err(ToolError::Unauthorized(format!(
                "{requested:?} resolves outside the authorized base directory"
            )));
        }

        Ok(canonical)
    }
}

impl Tool for FilesystemReadTool {
    fn name(&self) -> &str {
        "filesystem.read"
    }

    fn description(&self) -> &str {
        "Reads a UTF-8 text file from within the server's authorized base directory."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["path"],
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to the file, absolute or relative to the authorized base directory."
                }
            }
        })
    }

    fn invoke(&self, args: Value) -> Result<Value, ToolError> {
        let args: Args =
            serde_json::from_value(args).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;

        let resolved = self.resolve(&args.path)?;

        let metadata = std::fs::metadata(&resolved)
            .map_err(|e| ToolError::Failed(format!("stat failed: {e}")))?;
        if !metadata.is_file() {
            return Err(ToolError::Failed("not a regular file".into()));
        }
        if metadata.len() > MAX_READ_BYTES {
            return Err(ToolError::Failed(format!(
                "file exceeds the {MAX_READ_BYTES}-byte read limit"
            )));
        }

        let bytes =
            std::fs::read(&resolved).map_err(|e| ToolError::Failed(format!("read failed: {e}")))?;
        let contents = String::from_utf8(bytes)
            .map_err(|_| ToolError::Failed("file is not valid UTF-8".into()))?;

        Ok(json!({
            "path": resolved.strip_prefix(&self.base_dir).unwrap_or(&resolved),
            "size_bytes": metadata.len(),
            "contents": contents,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::symlink;

    fn tool_with_fixture() -> (tempfile::TempDir, FilesystemReadTool) {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("hello.txt"), "hello world\n").unwrap();
        fs::create_dir(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("sub/nested.txt"), "nested\n").unwrap();
        let tool = FilesystemReadTool::new(dir.path()).unwrap();
        (dir, tool)
    }

    #[test]
    fn reads_a_file_inside_the_base_dir() {
        let (_dir, tool) = tool_with_fixture();
        let result = tool.invoke(json!({"path": "hello.txt"})).unwrap();
        assert_eq!(result["contents"], "hello world\n");
    }

    #[test]
    fn reads_a_nested_file() {
        let (_dir, tool) = tool_with_fixture();
        let result = tool.invoke(json!({"path": "sub/nested.txt"})).unwrap();
        assert_eq!(result["contents"], "nested\n");
    }

    #[test]
    fn rejects_dot_dot_traversal() {
        let (_dir, tool) = tool_with_fixture();
        let err = tool
            .invoke(json!({"path": "../../etc/passwd"}))
            .unwrap_err();
        assert!(matches!(
            err,
            ToolError::Unauthorized(_) | ToolError::Failed(_)
        ));
    }

    #[test]
    fn rejects_symlink_escaping_base_dir() {
        let (dir, tool) = tool_with_fixture();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("secret.txt"), "secret\n").unwrap();
        symlink(
            outside.path().join("secret.txt"),
            dir.path().join("escape.txt"),
        )
        .unwrap();

        let err = tool.invoke(json!({"path": "escape.txt"})).unwrap_err();
        assert!(matches!(err, ToolError::Unauthorized(_)));
    }

    #[test]
    fn rejects_missing_file() {
        let (_dir, tool) = tool_with_fixture();
        assert!(tool.invoke(json!({"path": "nope.txt"})).is_err());
    }

    #[test]
    fn rejects_directories() {
        let (_dir, tool) = tool_with_fixture();
        let err = tool.invoke(json!({"path": "sub"})).unwrap_err();
        assert!(matches!(err, ToolError::Failed(_)));
    }

    #[test]
    fn rejects_missing_path_argument() {
        let (_dir, tool) = tool_with_fixture();
        assert!(tool.invoke(json!({})).is_err());
    }

    #[test]
    fn rejects_empty_path_argument() {
        let (_dir, tool) = tool_with_fixture();
        assert!(tool.invoke(json!({"path": ""})).is_err());
    }

    #[test]
    fn rejects_file_over_size_limit() {
        let (dir, tool) = tool_with_fixture();
        let big = vec![b'a'; (MAX_READ_BYTES + 1) as usize];
        fs::write(dir.path().join("big.txt"), big).unwrap();
        let err = tool.invoke(json!({"path": "big.txt"})).unwrap_err();
        assert!(matches!(err, ToolError::Failed(_)));
    }
}
