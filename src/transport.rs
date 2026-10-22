//! Unix Domain Socket transport: binding, connection lifecycle, and the
//! line-delimited JSON framing used to carry [`crate::protocol`] messages.

use crate::protocol::{ErrorCode, Operation, Request, Response};
use crate::tools::{ToolError, ToolRegistry};
use std::io;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Semaphore;

/// Serializes the umask-sensitive critical section in [`BoundListener::bind`]
/// against any other concurrent call to it within this process. `umask` is
/// process-wide OS state, not per-thread or per-task, so two overlapping
/// `bind` calls (e.g. this crate's own test suite, which runs many
/// `#[tokio::test]`s concurrently) could otherwise restore the wrong value
/// mid-critical-section.
static UMASK_GUARD: Mutex<()> = Mutex::new(());

/// A Unix socket this process has bound, tracked by (device, inode) so that
/// shutdown only unlinks the file if it still points at *this* listener.
/// If another process replaced the file at the same path in the meantime
/// (for example, a second server instance that took over after this one's
/// stale-socket check), the identity check fails and the file is left alone.
#[derive(Debug)]
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
    ///
    /// Before binding, this also creates (or validates) a private, `0700`,
    /// user-owned directory for `path`'s parent — see
    /// [`ensure_private_dir`] for what that does and does not guarantee.
    pub async fn bind(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            ensure_private_dir(parent)?;
        }

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

        let listener = {
            let _guard = UMASK_GUARD.lock().unwrap_or_else(|p| p.into_inner());
            // SAFETY: `umask` takes a plain integer and returns the previous
            // mask; it has no pointer arguments and cannot fail. Setting it
            // to `0o077` for the duration of the bind call below makes the
            // socket file's creation mode `0o600` atomically with respect to
            // the file actually becoming visible in the filesystem, closing
            // the window where a more permissive ambient umask would
            // otherwise leave it briefly group/world-accessible between
            // creation and the explicit `set_permissions` call further down.
            let previous = unsafe { libc::umask(0o077) };
            let result = UnixListener::bind(path);
            // SAFETY: same call, restoring the umask this process had
            // before entering this critical section.
            unsafe { libc::umask(previous) };
            result?
        };

        // Explicit, defense-in-depth re-assertion of the permissions,
        // independent of whether the umask trick above behaves identically
        // on every platform this ever runs on.
        if let Err(e) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)) {
            let _ = std::fs::remove_file(path); // don't leave a stale, wrongly-permissioned socket
            return Err(e);
        }

        let metadata = match std::fs::metadata(path) {
            Ok(m) => m,
            Err(e) => {
                let _ = std::fs::remove_file(path);
                return Err(e);
            }
        };
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

/// Creates `dir` with mode `0700` if it doesn't exist, or validates that an
/// existing `dir` is safe to reuse: resolves to a real directory (symlinks
/// are followed — see [`validate_private_dir`] for why that's fine), owned
/// by the current user, and not writable by anyone else (group/world read
/// or traversal access is tolerated — see [`validate_private_dir`] for why).
///
/// This defends against the classic shared-temp-directory attack where a
/// different local user plants a file or directory at a predictable path
/// ahead of this process. It does **not** defend against a malicious actor
/// who already has the same privileges as this process (e.g. root, or the
/// same uid running a different, buggy instance of this very program) —
/// that is out of scope for a same-host IPC tool. It also does not defend
/// against a TOCTOU race on `dir`'s *parent* directories; this crate
/// assumes `XDG_RUNTIME_DIR`/`TMPDIR`/`/tmp` themselves are not hostile.
///
/// Creation is **not** recursive: `dir`'s parent must already exist. In
/// practice `dir` is always one level under `XDG_RUNTIME_DIR`/`TMPDIR`
/// (see [`crate::config::default_socket_path`]), both of which are
/// guaranteed to exist already; `--socket-path` pointing somewhere with a
/// missing grandparent is treated as a configuration error, not something
/// to paper over by creating an arbitrary directory tree.
fn ensure_private_dir(dir: &Path) -> io::Result<()> {
    match std::fs::DirBuilder::new().mode(0o700).create(dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => validate_private_dir(dir),
        Err(e) => Err(e),
    }
}

fn validate_private_dir(dir: &Path) -> io::Result<()> {
    // Resolve to whatever real directory this actually is, following any
    // symlinks along the way — including entirely benign, OS-standard ones
    // like macOS's `/tmp -> /private/tmp`, which exist on every system and
    // are not attacker-controlled. What this check actually cares about is
    // not how many symlinks were involved in getting here, but whether the
    // *real* directory a socket file would be created in is one only the
    // current user can write to. A symlink that instead points somewhere
    // unsafe (owned by a different user, or group/world-writable) is still
    // rejected below, for that reason, once resolved — rejecting every
    // symlink unconditionally would also reject the safe, common case for
    // no real security benefit.
    let canonical = dir.canonicalize()?;
    let metadata = std::fs::metadata(&canonical)?;
    let dir = &canonical;
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{dir:?} exists and is not a directory"),
        ));
    }

    // SAFETY: `getuid` takes no arguments, cannot fail, and has no
    // memory-safety implications; it's a thin syscall wrapper.
    let current_uid = unsafe { libc::getuid() };
    if metadata.uid() != current_uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{dir:?} is owned by uid {}, not the current user (uid {current_uid}); refusing to reuse it",
                metadata.uid()
            ),
        ));
    }

    // The threat this guards against is someone *other* than the owner
    // being able to write into this directory — e.g. pre-creating a
    // malicious symlink at the socket's path before this process gets
    // there. Group/other *read* or *execute* (traversal) access doesn't
    // enable that attack, so it's accepted: many systems' default temp
    // directories (this crate's own test suite included — `tempfile`
    // creates its directories as `0750` on macOS) are already like this,
    // and rejecting them outright would make the common case unusable.
    // Group/other *write* access is exactly the attack surface, so that's
    // the only thing rejected here.
    let mode = metadata.permissions().mode() & 0o777;
    if mode & 0o022 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("{dir:?} is group- or world-writable (mode {mode:03o}); refusing to reuse it"),
        ));
    }

    Ok(())
}

/// What went wrong while trying to assemble one complete message.
#[derive(Debug)]
pub enum FramingError {
    /// A message exceeded the configured maximum size before a terminating
    /// newline was found.
    MessageTooLarge,
    /// The connection reached EOF with a non-empty, newline-less buffer —
    /// the peer disconnected mid-message rather than between messages.
    IncompleteMessage,
    /// A complete line's bytes were not valid UTF-8.
    InvalidUtf8,
    /// The underlying socket read itself failed.
    Io(io::Error),
}

/// Accumulates bytes read from a [`UnixStream`] until a complete,
/// newline-terminated message is available.
///
/// One `MessageReader` must be kept alive for the lifetime of a connection
/// (not recreated per call) so that bytes belonging to a *second* message,
/// read incidentally while looking for the first message's newline, are
/// preserved rather than discarded.
pub struct MessageReader {
    buf: Vec<u8>,
    max_bytes: usize,
}

impl MessageReader {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            buf: Vec::new(),
            max_bytes,
        }
    }

    /// Returns the next complete message, reading more from `stream` only
    /// when the buffer doesn't already contain one (which is exactly what
    /// makes multiple pipelined messages in a single `read()` call, and a
    /// single message fragmented across many `read()` calls, both work
    /// correctly). `Ok(None)` means a clean EOF with no partial data: the
    /// peer disconnected between messages, which is not an error.
    pub async fn next_message(
        &mut self,
        stream: &mut UnixStream,
    ) -> Result<Option<String>, FramingError> {
        loop {
            if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let mut line: Vec<u8> = self.buf.drain(..=pos).collect();
                line.pop(); // drop the newline delimiter itself
                if line.len() > self.max_bytes {
                    return Err(FramingError::MessageTooLarge);
                }
                return String::from_utf8(line)
                    .map(Some)
                    .map_err(|_| FramingError::InvalidUtf8);
            }

            if self.buf.len() > self.max_bytes {
                // No newline yet and already over budget: no amount of
                // further reading can produce a message within the limit,
                // so reject now rather than continuing to grow the buffer.
                return Err(FramingError::MessageTooLarge);
            }

            let mut chunk = [0u8; 4096];
            let n = stream.read(&mut chunk).await.map_err(FramingError::Io)?;
            if n == 0 {
                return if self.buf.is_empty() {
                    Ok(None)
                } else {
                    Err(FramingError::IncompleteMessage)
                };
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }
}

/// Serves one accepted connection to completion, dispatching every
/// well-formed request against `registry` until the peer disconnects, a
/// framing error occurs, or the connection goes quiet for longer than
/// `request_timeout`.
///
/// `request_timeout` is applied twice per request, independently: once to
/// receiving the complete request message (covering both "nothing arrived"
/// and "a fragment arrived but the rest never did"), and once to running
/// the tool in [`dispatch_invoke`]. A pathological client/tool pairing can
/// therefore take up to roughly `2 * request_timeout` for one request, by
/// design — this crate does not add a second configuration knob to shave
/// that in half.
pub async fn handle_connection(
    mut stream: UnixStream,
    registry: Arc<ToolRegistry>,
    max_message_bytes: usize,
    request_timeout: Duration,
) {
    let mut reader = MessageReader::new(max_message_bytes);

    loop {
        let line =
            match tokio::time::timeout(request_timeout, reader.next_message(&mut stream)).await {
                Ok(Ok(Some(line))) => line,
                Ok(Ok(None)) => return, // clean disconnect between messages
                Ok(Err(framing_err)) => {
                    let (code, message) = match framing_err {
                        FramingError::MessageTooLarge => (
                            ErrorCode::MessageTooLarge,
                            "request exceeded the maximum message size".to_string(),
                        ),
                        FramingError::IncompleteMessage => (
                            ErrorCode::IncompleteMessage,
                            "connection closed before a complete message was received".to_string(),
                        ),
                        FramingError::InvalidUtf8 => (
                            ErrorCode::InvalidUtf8,
                            "request line was not valid UTF-8".to_string(),
                        ),
                        FramingError::Io(e) => {
                            tracing::debug!(error = %e, "socket read failed");
                            (ErrorCode::Internal, "socket read failed".to_string())
                        }
                    };
                    let resp = Response::err("unknown", code, message);
                    let _ = stream.write_all(resp.to_line().as_bytes()).await;
                    return;
                }
                Err(_elapsed) => {
                    let resp = Response::err(
                        "unknown",
                        ErrorCode::Timeout,
                        "no complete request arrived within the configured timeout",
                    );
                    let _ = stream.write_all(resp.to_line().as_bytes()).await;
                    return;
                }
            };

        if line.trim().is_empty() {
            continue;
        }

        let response = dispatch_line(&line, &registry, request_timeout).await;
        let encoded = bound_response(response, max_message_bytes);
        if stream.write_all(encoded.as_bytes()).await.is_err() {
            return;
        }
    }
}

/// Serializes `response`, and if the result would exceed `max_bytes`,
/// substitutes a small, bounded [`ErrorCode::ResponseTooLarge`] error in
/// its place instead of writing the oversized payload. This is checked
/// here, once, for every response regardless of which operation produced
/// it — `discover` (a long tool list), `health`, and `invoke` (a tool's
/// own result, e.g. `filesystem.read`'s file contents) all go through it.
fn bound_response(response: Response, max_bytes: usize) -> String {
    let line = response.to_line();
    if line.len() <= max_bytes {
        return line;
    }
    Response::err(
        response.id,
        ErrorCode::ResponseTooLarge,
        "the tool's result exceeded the maximum message size",
    )
    .to_line()
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
    // mechanism to preempt one. A tool that ignores a timed-out caller and
    // keeps running will keep occupying a blocking-pool thread until it
    // finishes on its own.
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

/// Bounds how many connections [`crate::bin::agent_tools_server`]'s accept
/// loop will service concurrently. Acquiring a permit happens *after*
/// `accept()`, so an excess connection is accepted (so its file descriptor
/// doesn't sit unaccepted in the kernel's backlog) and then immediately
/// closed with a bounded error, rather than left to queue indefinitely.
pub struct ConnectionLimiter {
    semaphore: Arc<Semaphore>,
}

impl ConnectionLimiter {
    pub fn new(max_connections: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_connections)),
        }
    }

    /// Tries to admit one more connection. `None` means the limit is
    /// currently reached; the caller should reject this connection.
    /// Dropping the returned permit (e.g. when the connection-handling task
    /// ends, for any reason — clean disconnect, timeout, or error) frees
    /// the slot automatically.
    pub fn try_admit(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&self.semaphore).try_acquire_owned().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::SystemInfoTool;
    use tokio::io::{AsyncBufReadExt, BufReader};

    fn registry_with_system_info() -> Arc<ToolRegistry> {
        let mut registry = ToolRegistry::new();
        registry.register(Box::new(SystemInfoTool));
        Arc::new(registry)
    }

    // --- dispatch-level tests (unchanged behavior) --------------------

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

    // --- socket lifecycle tests (unchanged behavior) ------------------

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

    #[tokio::test]
    async fn bind_creates_a_private_0700_parent_directory() {
        let root = tempfile::tempdir().unwrap();
        // One level, matching real usage: `ensure_private_dir` creates a
        // single directory (not recursively, by design — see its doc
        // comment), and in production that single directory sits directly
        // under `XDG_RUNTIME_DIR`/`TMPDIR`, which always already exists.
        let parent = root.path().join("agent-socket-tools");
        let path = parent.join("test.sock");
        let listener = BoundListener::bind(&path).await.unwrap();
        let mode = std::fs::metadata(&parent).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
        drop(listener);
    }

    #[tokio::test]
    async fn bind_refuses_a_group_writable_existing_parent_directory() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("shared");
        std::fs::create_dir(&parent).unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o770)).unwrap();
        let path = parent.join("test.sock");
        let err = BoundListener::bind(&path).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
    }

    #[tokio::test]
    async fn bind_refuses_a_parent_reached_through_a_symlink_to_an_unsafe_dir() {
        let root = tempfile::tempdir().unwrap();
        let unsafe_dir = root.path().join("unsafe");
        std::fs::create_dir(&unsafe_dir).unwrap();
        std::fs::set_permissions(&unsafe_dir, std::fs::Permissions::from_mode(0o777)).unwrap();
        let link = root.path().join("link");
        std::os::unix::fs::symlink(&unsafe_dir, &link).unwrap();
        let path = link.join("test.sock");
        let err = BoundListener::bind(&path).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
    }

    #[tokio::test]
    async fn bind_accepts_a_parent_reached_through_a_symlink_to_a_safe_dir() {
        // Mirrors a completely ordinary, non-hostile case: macOS's own
        // `/tmp` is a symlink to `/private/tmp`. A symlink is not
        // inherently suspicious; what matters is whether the real
        // directory it resolves to is safe (see `validate_private_dir`).
        let root = tempfile::tempdir().unwrap();
        let real_dir = root.path().join("real");
        std::fs::create_dir(&real_dir).unwrap();
        std::fs::set_permissions(&real_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let link = root.path().join("link");
        std::os::unix::fs::symlink(&real_dir, &link).unwrap();
        let path = link.join("test.sock");
        let listener = BoundListener::bind(&path).await.unwrap();
        drop(listener);
    }

    #[tokio::test]
    async fn created_socket_file_has_owner_only_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sock");
        let listener = BoundListener::bind(&path).await.unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        drop(listener);
    }

    // --- framing tests (the P0 fix) -----------------------------------

    async fn connected_pair() -> (UnixStream, UnixStream) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pair.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let client = UnixStream::connect(&path).await.unwrap();
        let (server, _addr) = listener.accept().await.unwrap();
        (server, client)
    }

    #[tokio::test]
    async fn two_messages_in_one_write_are_both_delivered() {
        let (mut server, mut client) = connected_pair().await;
        client
            .write_all(b"{\"version\":1,\"id\":\"a\",\"op\":\"health\"}\n{\"version\":1,\"id\":\"b\",\"op\":\"health\"}\n")
            .await
            .unwrap();

        let mut reader = MessageReader::new(1024);
        let first = reader.next_message(&mut server).await.unwrap().unwrap();
        let second = reader.next_message(&mut server).await.unwrap().unwrap();
        assert!(first.contains("\"id\":\"a\""));
        assert!(second.contains("\"id\":\"b\""));
    }

    #[tokio::test]
    async fn one_message_fragmented_across_many_writes_is_assembled() {
        let (mut server, mut client) = connected_pair().await;
        let full = "{\"version\":1,\"id\":\"c\",\"op\":\"health\"}\n";
        for byte in full.as_bytes() {
            client.write_all(&[*byte]).await.unwrap();
        }

        let mut reader = MessageReader::new(1024);
        let message = reader.next_message(&mut server).await.unwrap().unwrap();
        assert!(message.contains("\"id\":\"c\""));
    }

    #[tokio::test]
    async fn message_exactly_at_the_limit_is_accepted() {
        let (mut server, mut client) = connected_pair().await;
        let payload = "x".repeat(10);
        assert_eq!(payload.len(), 10);
        client
            .write_all(format!("{payload}\n").as_bytes())
            .await
            .unwrap();

        let mut reader = MessageReader::new(10);
        let message = reader.next_message(&mut server).await.unwrap();
        assert_eq!(message, Some(payload));
    }

    #[tokio::test]
    async fn message_one_byte_over_the_limit_is_rejected() {
        let (mut server, mut client) = connected_pair().await;
        let payload = "x".repeat(11);
        client
            .write_all(format!("{payload}\n").as_bytes())
            .await
            .unwrap();

        let mut reader = MessageReader::new(10);
        let err = reader.next_message(&mut server).await.unwrap_err();
        assert!(matches!(err, FramingError::MessageTooLarge));
    }

    #[tokio::test]
    async fn oversized_message_without_a_newline_is_rejected_without_unbounded_growth() {
        let (mut server, mut client) = connected_pair().await;
        // More than the limit, and never terminated — this must be
        // rejected promptly rather than buffered forever. Deliberately
        // small enough to fit in one write within the kernel's socket
        // buffer: nothing is reading concurrently yet (this test's `await`
        // on `write_all` must resolve before `next_message` ever runs), so
        // a write any larger than the platform's send-buffer size would
        // block forever waiting for a reader that hasn't started.
        client.write_all(&vec![b'x'; 4096]).await.unwrap();

        let mut reader = MessageReader::new(10);
        let err = reader.next_message(&mut server).await.unwrap_err();
        assert!(matches!(err, FramingError::MessageTooLarge));
    }

    #[tokio::test]
    async fn eof_with_a_partial_message_is_incomplete_not_a_clean_disconnect() {
        let (mut server, mut client) = connected_pair().await;
        client.write_all(b"{\"version\":1").await.unwrap();
        drop(client); // EOF before any newline

        let mut reader = MessageReader::new(1024);
        let err = reader.next_message(&mut server).await.unwrap_err();
        assert!(matches!(err, FramingError::IncompleteMessage));
    }

    #[tokio::test]
    async fn clean_eof_between_messages_is_not_an_error() {
        let (mut server, client) = connected_pair().await;
        drop(client); // disconnect with nothing sent at all

        let mut reader = MessageReader::new(1024);
        let result = reader.next_message(&mut server).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn invalid_utf8_line_is_reported_distinctly() {
        let (mut server, mut client) = connected_pair().await;
        client.write_all(&[0xff, 0xfe, b'\n']).await.unwrap();

        let mut reader = MessageReader::new(1024);
        let err = reader.next_message(&mut server).await.unwrap_err();
        assert!(matches!(err, FramingError::InvalidUtf8));
    }

    // --- handle_connection-level tests: error classification ----------

    #[tokio::test]
    async fn handle_connection_reports_incomplete_message_distinctly() {
        let (server, mut client) = connected_pair().await;
        let registry = registry_with_system_info();
        let handle = tokio::spawn(handle_connection(
            server,
            registry,
            1024,
            Duration::from_secs(5),
        ));

        client.write_all(b"{\"version\":1").await.unwrap();
        // Shutting down the write half signals EOF to the server without
        // dropping the whole `UnixStream`, so this task can still read the
        // response the server sends back before closing its side.
        client.shutdown().await.unwrap();
        let mut reader = BufReader::new(&mut client);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();

        let resp: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(resp["error"]["code"], "incomplete_message");
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn handle_connection_reports_invalid_utf8_distinctly() {
        let (server, mut client) = connected_pair().await;
        let registry = registry_with_system_info();
        let handle = tokio::spawn(handle_connection(
            server,
            registry,
            1024,
            Duration::from_secs(5),
        ));

        client.write_all(&[0xff, 0xfe, b'\n']).await.unwrap();
        let mut reader = BufReader::new(&mut client);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();

        let resp: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(resp["error"]["code"], "invalid_utf8");
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn handle_connection_times_out_a_request_that_never_arrives() {
        let (server, mut client) = connected_pair().await;
        let registry = registry_with_system_info();
        let handle = tokio::spawn(handle_connection(
            server,
            registry,
            1024,
            Duration::from_millis(50),
        ));

        let mut reader = BufReader::new(&mut client);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();

        let resp: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(resp["error"]["code"], "timeout");
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn handle_connection_serves_requests_sequentially_on_one_connection() {
        let (server, mut client) = connected_pair().await;
        let registry = registry_with_system_info();
        let handle = tokio::spawn(handle_connection(
            server,
            registry,
            1024,
            Duration::from_secs(5),
        ));

        client
            .write_all(b"{\"version\":1,\"id\":\"1\",\"op\":\"health\"}\n")
            .await
            .unwrap();
        client
            .write_all(b"{\"version\":1,\"id\":\"2\",\"op\":\"health\"}\n")
            .await
            .unwrap();

        let mut reader = BufReader::new(&mut client);
        let mut first = String::new();
        let mut second = String::new();
        reader.read_line(&mut first).await.unwrap();
        reader.read_line(&mut second).await.unwrap();

        assert!(first.contains("\"id\":\"1\""));
        assert!(second.contains("\"id\":\"2\""));

        drop(client);
        handle.await.unwrap();
    }

    // --- response size enforcement ------------------------------------

    #[test]
    fn bound_response_passes_small_responses_through_unchanged() {
        let resp = Response::ok("r1", serde_json::json!({"a": 1}));
        let line = bound_response(resp, 1024);
        assert!(line.contains("\"a\":1"));
    }

    #[test]
    fn bound_response_at_exactly_the_limit_passes_through() {
        let resp = Response::ok("r1", serde_json::json!(null));
        let exact_len = resp.to_line().len();
        let line = bound_response(resp, exact_len);
        assert!(line.contains("\"ok\":true"));
    }

    #[test]
    fn bound_response_over_the_limit_becomes_a_bounded_error() {
        let big_contents = "x".repeat(10_000);
        let resp = Response::ok("r1", serde_json::json!({ "contents": big_contents }));
        let line = bound_response(resp, 100);
        assert!(line.len() <= 200); // the substituted error, not the payload
        let parsed: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
        assert_eq!(parsed["error"]["code"], "response_too_large");
        assert_eq!(parsed["id"], "r1");
    }

    // --- connection limiting --------------------------------------------

    #[test]
    fn connection_limiter_admits_up_to_its_capacity() {
        let limiter = ConnectionLimiter::new(2);
        let first = limiter.try_admit();
        let second = limiter.try_admit();
        let third = limiter.try_admit();
        assert!(first.is_some());
        assert!(second.is_some());
        assert!(third.is_none(), "a third connection should be rejected");
    }

    #[test]
    fn connection_limiter_frees_a_slot_when_a_permit_drops() {
        let limiter = ConnectionLimiter::new(1);
        let first = limiter.try_admit();
        assert!(first.is_some());
        assert!(
            limiter.try_admit().is_none(),
            "no slots left while the first permit is held"
        );
        drop(first);
        assert!(
            limiter.try_admit().is_some(),
            "dropping the permit must free its slot"
        );
    }
}
