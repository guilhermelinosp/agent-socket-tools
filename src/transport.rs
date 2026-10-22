//! Unix Domain Socket transport: binding, connection lifecycle, and the
//! line-delimited JSON framing used to carry [`crate::protocol`] messages.

use crate::protocol::{ErrorCode, Operation, Request, Response};
use crate::tools::{ToolError, ToolRegistry};
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

/// A Unix socket this process has bound, tracked by (device, inode) so that
/// shutdown only unlinks the file if it still points at *this* listener.
/// If another process replaced the file at the same path in the meantime
/// (for example, a second server instance that took over after this one's
/// stale-socket check), the identity check fails and the file is left alone.
pub struct BoundListener {
    listener: UnixListener,
    path: PathBuf,
    identity: (u64, u64),
}

impl BoundListener {
    /// Binds `path`, replacing it first if it is a stale socket (a leftover
    /// file from a server that exited without cleaning up). A socket that is
    /// still accepting connections causes this to fail with
    /// [`io::ErrorKind::AddrInUse`] rather than being clobbered.
    pub async fn bind(path: &Path) -> io::Result<Self> {
        if path.exists() {
            match UnixStream::connect(path).await {
                Ok(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::AddrInUse,
                        format!("another server is already listening on {path:?}"),
                    ));
                }
                Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                    // Stale socket file with nothing listening behind it.
                    std::fs::remove_file(path)?;
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    // Disappeared between the existence check and connect; fine.
                }
                Err(e) => return Err(e),
            }
        }

        let listener = UnixListener::bind(path)?;

        // Restrict to the owner. UnixListener::bind creates the file with
        // the process umask, which may be more permissive than we want.
        let perms = std::fs::Permissions::from_mode(0o600);
        std::fs::set_permissions(path, perms)?;

        let metadata = std::fs::metadata(path)?;
        let identity = (metadata.dev(), metadata.ino());

        Ok(Self {
            listener,
            path: path.to_path_buf(),
            identity,
        })
    }

    pub async fn accept(&self) -> io::Result<(UnixStream, tokio::net::unix::SocketAddr)> {
        self.listener.accept().await
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for BoundListener {
    fn drop(&mut self) {
        if let Ok(metadata) = std::fs::metadata(&self.path) {
            if (metadata.dev(), metadata.ino()) == self.identity {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }
}

/// Reads one newline-terminated message, refusing to buffer more than
/// `max_bytes` while searching for the delimiter. Returns `Ok(None)` on a
/// clean EOF with no partial data (i.e. the peer closed the connection
/// between requests).
async fn read_line_capped(stream: &mut UnixStream, max_bytes: usize) -> io::Result<Option<String>> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            // Connection closed mid-message: treat what we have as final.
            break;
        }
        buf.extend_from_slice(&chunk[..n]);

        if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            buf.truncate(pos);
            break;
        }

        if buf.len() > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "message exceeds maximum size before a newline was found",
            ));
        }
    }

    if buf.len() > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "message exceeds maximum size",
        ));
    }

    String::from_utf8(buf)
        .map(Some)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Serves one accepted connection to completion, dispatching every
/// well-formed request line against `registry` until the peer disconnects
/// or sends something the transport itself must reject (oversized message,
/// non-UTF-8 bytes).
pub async fn handle_connection(
    mut stream: UnixStream,
    registry: Arc<ToolRegistry>,
    max_message_bytes: usize,
    request_timeout: Duration,
) {
    loop {
        let line = match read_line_capped(&mut stream, max_message_bytes).await {
            Ok(Some(line)) => line,
            Ok(None) => return, // clean disconnect
            Err(e) => {
                tracing::debug!(error = %e, "closing connection after transport error");
                let resp = Response::err(
                    "unknown",
                    ErrorCode::MessageTooLarge,
                    "request exceeded the maximum message size",
                );
                let _ = stream.write_all(resp.to_line().as_bytes()).await;
                return;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        let response = dispatch_line(&line, &registry, request_timeout).await;
        if stream
            .write_all(response.to_line().as_bytes())
            .await
            .is_err()
        {
            return;
        }
    }
}

async fn dispatch_line(
    line: &str,
    registry: &Arc<ToolRegistry>,
    request_timeout: Duration,
) -> Response {
    let request: Request = match Request::from_line(line) {
        Ok(r) => r,
        Err(e) => {
            return Response::err(
                "unknown",
                ErrorCode::InvalidRequest,
                format!("malformed request: {e}"),
            )
        }
    };

    if request.version != crate::protocol::PROTOCOL_VERSION {
        return Response::err(
            request.id,
            ErrorCode::UnsupportedVersion,
            format!(
                "server supports version {}, got {}",
                crate::protocol::PROTOCOL_VERSION,
                request.version
            ),
        );
    }

    match request.op {
        Operation::Health => Response::ok(
            request.id,
            serde_json::json!({
                "status": "ok",
                "tool_count": registry.discover().len(),
            }),
        ),
        Operation::Discover => Response::ok(request.id, serde_json::json!(registry.discover())),
        Operation::Invoke => dispatch_invoke(request, registry, request_timeout).await,
    }
}

async fn dispatch_invoke(
    request: Request,
    registry: &Arc<ToolRegistry>,
    request_timeout: Duration,
) -> Response {
    let Some(tool_name) = request.tool.clone() else {
        return Response::err(
            request.id,
            ErrorCode::InvalidRequest,
            "invoke requires a \"tool\" field",
        );
    };

    if registry.get(&tool_name).is_none() {
        return Response::err(
            request.id,
            ErrorCode::ToolNotFound,
            format!("no such tool: {tool_name}"),
        );
    }

    let args = request.args.unwrap_or(serde_json::Value::Null);

    // Tools in this MVP are fast and local, but they still run through a
    // blocking-pool task with a timeout so one slow or misbehaving tool
    // cannot stall the connection's event loop. Note that the timeout
    // bounds how long the *caller* waits; it cannot forcibly abort a
    // blocking-pool task that is already running, since Rust has no safe
    // mechanism to preempt one.
    //
    // The registry is cloned (an `Arc` bump, not a deep copy) into the
    // blocking closure and the tool looked up again there, which satisfies
    // `spawn_blocking`'s `'static` bound without any `unsafe` aliasing.
    let registry_for_blocking = Arc::clone(registry);
    let tool_name_for_blocking = tool_name.clone();
    let invocation = tokio::task::spawn_blocking(move || {
        let tool = registry_for_blocking
            .get(&tool_name_for_blocking)
            .expect("presence already checked before spawning");
        tool.invoke(args)
    });

    match tokio::time::timeout(request_timeout, invocation).await {
        Ok(Ok(Ok(value))) => Response::ok(request.id, value),
        Ok(Ok(Err(tool_err))) => tool_error_to_response(request.id, tool_err),
        Ok(Err(join_err)) => Response::err(
            request.id,
            ErrorCode::Internal,
            format!("tool task panicked: {join_err}"),
        ),
        Err(_elapsed) => Response::err(
            request.id,
            ErrorCode::Timeout,
            format!("tool {tool_name} did not complete within the configured timeout"),
        ),
    }
}

fn tool_error_to_response(id: String, err: ToolError) -> Response {
    match err {
        ToolError::InvalidArgs(msg) => Response::err(id, ErrorCode::InvalidArgs, msg),
        ToolError::Unauthorized(msg) => Response::err(id, ErrorCode::Unauthorized, msg),
        ToolError::Failed(msg) => Response::err(id, ErrorCode::ToolError, msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::SystemInfoTool;

    fn registry_with_system_info() -> Arc<ToolRegistry> {
        let mut registry = ToolRegistry::new();
        registry.register(Box::new(SystemInfoTool));
        Arc::new(registry)
    }

    #[tokio::test]
    async fn health_reports_tool_count() {
        let registry = registry_with_system_info();
        let line = r#"{"version":1,"id":"h1","op":"health"}"#;
        let resp = dispatch_line(line, &registry, Duration::from_secs(1)).await;
        assert!(resp.ok);
        assert_eq!(resp.result.unwrap()["tool_count"], 1);
    }

    #[tokio::test]
    async fn discover_lists_registered_tools() {
        let registry = registry_with_system_info();
        let line = r#"{"version":1,"id":"d1","op":"discover"}"#;
        let resp = dispatch_line(line, &registry, Duration::from_secs(1)).await;
        let tools = resp.result.unwrap();
        assert_eq!(tools[0]["name"], "system.info");
    }

    #[tokio::test]
    async fn invoke_runs_the_named_tool() {
        let registry = registry_with_system_info();
        let line = r#"{"version":1,"id":"i1","op":"invoke","tool":"system.info","args":{}}"#;
        let resp = dispatch_line(line, &registry, Duration::from_secs(1)).await;
        assert!(resp.ok);
        assert_eq!(resp.result.unwrap()["os"], std::env::consts::OS);
    }

    #[tokio::test]
    async fn invoke_unknown_tool_is_tool_not_found() {
        let registry = registry_with_system_info();
        let line = r#"{"version":1,"id":"i2","op":"invoke","tool":"nope.nope","args":{}}"#;
        let resp = dispatch_line(line, &registry, Duration::from_secs(1)).await;
        assert!(!resp.ok);
        assert_eq!(resp.error.unwrap().code, ErrorCode::ToolNotFound);
    }

    #[tokio::test]
    async fn invoke_without_tool_field_is_invalid_request() {
        let registry = registry_with_system_info();
        let line = r#"{"version":1,"id":"i3","op":"invoke"}"#;
        let resp = dispatch_line(line, &registry, Duration::from_secs(1)).await;
        assert_eq!(resp.error.unwrap().code, ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    async fn malformed_json_is_invalid_request() {
        let registry = registry_with_system_info();
        let resp = dispatch_line("not json", &registry, Duration::from_secs(1)).await;
        assert_eq!(resp.error.unwrap().code, ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    async fn unsupported_version_is_rejected() {
        let registry = registry_with_system_info();
        let line = r#"{"version":99,"id":"v1","op":"health"}"#;
        let resp = dispatch_line(line, &registry, Duration::from_secs(1)).await;
        assert_eq!(resp.error.unwrap().code, ErrorCode::UnsupportedVersion);
    }

    #[tokio::test]
    async fn bind_rejects_a_socket_that_is_actively_listening() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sock");
        let first = BoundListener::bind(&path).await.unwrap();
        let second = BoundListener::bind(&path).await;
        assert!(second.is_err());
        drop(first);
    }

    #[tokio::test]
    async fn bind_reclaims_a_stale_socket_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sock");

        // A bare `UnixListener`'s `Drop` does not unlink its socket path,
        // unlike `BoundListener`'s. That is exactly what a server killed
        // with SIGKILL leaves behind: a real AF_UNIX socket special file
        // with nothing listening behind it, which is what makes connecting
        // to it fail with `ConnectionRefused` rather than some other error.
        {
            let _raw = UnixListener::bind(&path).unwrap();
        }
        assert!(
            path.exists(),
            "the raw listener should have created the socket file"
        );

        let second = BoundListener::bind(&path).await;
        assert!(second.is_ok());
    }

    #[tokio::test]
    async fn drop_removes_only_the_file_this_instance_created() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sock");
        let listener = BoundListener::bind(&path).await.unwrap();
        // Simulate another process replacing the file at the same path.
        std::fs::remove_file(&path).unwrap();
        std::fs::File::create(&path).unwrap();
        drop(listener);
        assert!(
            path.exists(),
            "drop must not remove a file it didn't create"
        );
    }
}
