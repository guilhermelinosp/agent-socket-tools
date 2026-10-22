use std::path::PathBuf;
use std::time::Duration;

pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 1024 * 1024; // 1 MiB per line
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_MAX_CONNECTIONS: usize = 256;

/// Name of the private, per-user subdirectory the socket lives in. Never
/// put the socket file directly in a shared temp root: a predictable
/// filename there is attacker-plantable (a different local user could
/// pre-create `agent-tools.sock` before this process does). A dedicated
/// `0700` subdirectory that this process creates and validates the
/// ownership/permissions of, instead, is the standard mitigation — see
/// [`crate::transport::BoundListener::bind`], which creates and checks it.
pub const SOCKET_DIR_NAME: &str = "agent-socket-tools";

/// Resolves the default socket path: `$XDG_RUNTIME_DIR/agent-socket-tools/agent-tools.sock`
/// when `XDG_RUNTIME_DIR` is set to an existing, absolute directory;
/// otherwise falls back to `$TMPDIR/agent-socket-tools/agent-tools.sock`
/// (or `/tmp/...` on platforms without `TMPDIR`), matching typical macOS
/// and minimal-Linux environments that have no systemd-managed runtime
/// directory.
///
/// This function only computes a path; it performs no filesystem I/O and
/// makes no guarantee the parent directory exists or has safe permissions.
/// [`crate::transport::BoundListener::bind`] is responsible for creating
/// and validating that directory before binding.
pub fn default_socket_path() -> PathBuf {
    default_socket_dir().join("agent-tools.sock")
}

/// Same private directory as [`default_socket_path`], different file:
/// AST and the legacy JSON-Lines protocol are two different servers that
/// can run at the same time without colliding, each with its own socket.
pub fn default_ast_socket_path() -> PathBuf {
    default_socket_dir().join("ast.sock")
}

fn default_socket_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("XDG_RUNTIME_DIR") {
        let dir = PathBuf::from(dir);
        if dir.is_absolute() && dir.is_dir() {
            return dir.join(SOCKET_DIR_NAME);
        }
    }
    std::env::temp_dir().join(SOCKET_DIR_NAME)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn falls_back_to_tmp_dir_when_xdg_runtime_dir_unset() {
        // SAFETY: test-local env mutation; no other test in this process
        // reads XDG_RUNTIME_DIR concurrently because cargo test runs each
        // test module's `env::var` access on the same thread-per-test model
        // and this test restores the variable before returning.
        let previous = std::env::var_os("XDG_RUNTIME_DIR");
        unsafe { std::env::remove_var("XDG_RUNTIME_DIR") };
        let path = default_socket_path();
        assert_eq!(path.file_name().unwrap(), "agent-tools.sock");
        assert!(path.starts_with(std::env::temp_dir()));
        assert!(path.to_string_lossy().contains(SOCKET_DIR_NAME));
        if let Some(previous) = previous {
            unsafe { std::env::set_var("XDG_RUNTIME_DIR", previous) };
        }
    }
}
