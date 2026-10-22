//! Server-process management: spawning sibling binaries, socket readiness,
//! peak-RSS sampling via `ps`, and per-process CPU time via `ps`.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::net::UnixStream;

/// Resolves a sibling binary next to this one. Cargo sets `CARGO_BIN_EXE_*`
/// only for targets that depend on another binary, not for a plain `[[bin]]`,
/// so the path is derived at runtime from `current_exe()`.
pub fn sibling_bin(name: &str) -> PathBuf {
    let mut path = std::env::current_exe().expect("current_exe");
    path.set_file_name(name);
    path
}

/// A legacy or AST server process listening on a Unix socket. Killed on drop.
pub struct SocketServer {
    child: Child,
}

impl SocketServer {
    pub fn spawn(bin: &str, read_dir: &Path, socket_path: &Path) -> Result<SocketServer, String> {
        let child = Command::new(sibling_bin(bin))
            .arg("--socket-path")
            .arg(socket_path)
            .arg("--read-dir")
            .arg(read_dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("cannot spawn {bin}: {e}"))?;
        Ok(SocketServer { child })
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }
}

impl Drop for SocketServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Polls until the socket accepts a connection. Polls every 1 ms, so the
/// startup measurement has at most ~1 ms of quantization from this loop.
pub async fn wait_for_socket(path: &Path) -> bool {
    for _ in 0..5_000 {
        if path.exists() && UnixStream::connect(path).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    false
}

/// Samples a process's resident set size (KiB) on a background thread.
/// `ps` is polled roughly every 40 ms, so a short spike can be missed.
pub struct ResourceSampler {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<u64>,
}

impl ResourceSampler {
    pub fn start(pid: u32) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = stop.clone();
        let handle = std::thread::spawn(move || {
            let mut peak_rss_kib: u64 = 0;
            while !stop_flag.load(Ordering::Relaxed) {
                if let Some(rss) = rss_kib(pid) {
                    peak_rss_kib = peak_rss_kib.max(rss);
                }
                std::thread::sleep(Duration::from_millis(40));
            }
            peak_rss_kib
        });
        Self { stop, handle }
    }

    /// Stops sampling and returns the peak RSS in KiB.
    pub fn stop(self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.join().unwrap_or(0)
    }
}

fn rss_kib(pid: u32) -> Option<u64> {
    let out = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

/// Cumulative CPU time (user + system) of a process, in seconds.
pub fn cpu_time_secs(pid: u32) -> Option<f64> {
    let out = Command::new("ps")
        .args(["-o", "cputime=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    parse_cputime(&String::from_utf8_lossy(&out.stdout))
}

/// Parses `ps` cputime output: Linux `[[dd-]hh:]mm:ss[.cc]` and macOS
/// `M:SS.cc` (both are accepted by this one parser).
pub fn parse_cputime(text: &str) -> Option<f64> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    let (days, clock) = match text.split_once('-') {
        Some((d, rest)) => (d.parse::<f64>().ok()?, rest),
        None => (0.0, text),
    };
    let mut seconds = 0.0;
    for part in clock.split(':') {
        seconds = seconds * 60.0 + part.parse::<f64>().ok()?;
    }
    Some(days * 86_400.0 + seconds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_macos_and_linux_cputime() {
        assert_eq!(parse_cputime("0:00.12\n"), Some(0.12));
        assert_eq!(parse_cputime("10:05.50"), Some(605.5));
        assert_eq!(parse_cputime("1:02:03"), Some(3723.0));
        assert_eq!(parse_cputime("00:00:00.01"), Some(0.01));
        assert_eq!(parse_cputime("2-01:00:00"), Some(176_400.0));
        assert_eq!(parse_cputime(""), None);
        assert_eq!(parse_cputime("garbage"), None);
    }
}
