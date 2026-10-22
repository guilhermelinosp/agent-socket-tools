use std::path::PathBuf;
use std::time::Duration;

pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 1024 * 1024; // 1 MiB per line
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Resolves the default socket path: `$XDG_RUNTIME_DIR/agent-tools.sock` when
/// `XDG_RUNTIME_DIR` is set to an existing, absolute directory; otherwise
/// falls back to `$TMPDIR/agent-tools.sock` (or `/tmp` on platforms without
/// `TMPDIR`), matching typical macOS and minimal-Linux environments that
/// have no systemd-managed runtime directory.
pub fn default_socket_path() -> PathBuf {
    if let Some(dir) = std::env::var_os("XDG_RUNTIME_DIR") {
        let dir = PathBuf::from(dir);
        if dir.is_absolute() && dir.is_dir() {
            return dir.join("agent-tools.sock");
        }
    }
    std::env::temp_dir().join("agent-tools.sock")
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
        if let Some(previous) = previous {
            unsafe { std::env::set_var("XDG_RUNTIME_DIR", previous) };
        }
    }
}
