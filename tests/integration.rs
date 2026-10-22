//! End-to-end tests: start the real `agent-tools-server` binary, talk to it
//! both through the real `agent-tools-client` binary and through a raw
//! Unix socket connection (for cases the client CLI doesn't expose, like
//! malformed request lines).

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

struct ServerHandle {
    // Declaration order is drop order: the child process is killed (with
    // SIGKILL, via `Child::kill`, so the server has no chance to run its
    // own socket cleanup) before the temp directories it was using are
    // removed by this struct instead.
    child: Child,
    socket_path: PathBuf,
    _socket_dir: tempfile::TempDir,
    _read_dir: tempfile::TempDir,
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn wait_for_socket(path: &Path) {
    for _ in 0..100 {
        if path.exists() && UnixStream::connect(path).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server did not create a usable socket at {path:?} in time");
}

async fn start_server() -> ServerHandle {
    let read_dir = tempfile::tempdir().unwrap();
    std::fs::write(read_dir.path().join("hello.txt"), "hello world\n").unwrap();

    let socket_dir = tempfile::tempdir().unwrap();
    let socket_path = socket_dir.path().join("agent-tools-test.sock");

    let child = Command::new(env!("CARGO_BIN_EXE_agent-tools-server"))
        .arg("--socket-path")
        .arg(&socket_path)
        .arg("--read-dir")
        .arg(read_dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn agent-tools-server");

    wait_for_socket(&socket_path).await;

    ServerHandle {
        child,
        socket_path,
        _socket_dir: socket_dir,
        _read_dir: read_dir,
    }
}

fn run_client(socket_path: &Path, args: &[&str]) -> (bool, serde_json::Value) {
    let output = Command::new(env!("CARGO_BIN_EXE_agent-tools-client"))
        .arg("--socket-path")
        .arg(socket_path)
        .args(args)
        .output()
        .expect("failed to spawn agent-tools-client");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let value: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("client stdout not JSON: {e}\n{stdout}"));
    (output.status.success(), value)
}

#[tokio::test]
async fn health_reports_ok() {
    let server = start_server().await;
    let (success, resp) = run_client(&server.socket_path, &["health"]);
    assert!(success);
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["result"]["status"], "ok");
}

#[tokio::test]
async fn discover_lists_both_mvp_tools() {
    let server = start_server().await;
    let (success, resp) = run_client(&server.socket_path, &["discover"]);
    assert!(success);
    let names: Vec<&str> = resp["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"system.info"));
    assert!(names.contains(&"filesystem.read"));
}

#[tokio::test]
async fn invoke_system_info_succeeds() {
    let server = start_server().await;
    let (success, resp) = run_client(
        &server.socket_path,
        &["invoke", "--tool", "system.info", "--args", "{}"],
    );
    assert!(success);
    assert_eq!(resp["result"]["os"], std::env::consts::OS);
}

#[tokio::test]
async fn invoke_filesystem_read_succeeds_for_authorized_file() {
    let server = start_server().await;
    let (success, resp) = run_client(
        &server.socket_path,
        &[
            "invoke",
            "--tool",
            "filesystem.read",
            "--args",
            r#"{"path":"hello.txt"}"#,
        ],
    );
    assert!(success);
    assert_eq!(resp["result"]["contents"], "hello world\n");
}

#[tokio::test]
async fn invoke_filesystem_read_rejects_traversal() {
    let server = start_server().await;
    let (success, resp) = run_client(
        &server.socket_path,
        &[
            "invoke",
            "--tool",
            "filesystem.read",
            "--args",
            r#"{"path":"../../etc/passwd"}"#,
        ],
    );
    assert!(!success);
    assert_eq!(resp["ok"], false);
}

#[tokio::test]
async fn invoke_unknown_tool_reports_tool_not_found() {
    let server = start_server().await;
    let (success, resp) = run_client(
        &server.socket_path,
        &["invoke", "--tool", "does.not.exist", "--args", "{}"],
    );
    assert!(!success);
    assert_eq!(resp["error"]["code"], "tool_not_found");
}

#[tokio::test]
async fn invoke_with_invalid_args_reports_invalid_args() {
    let server = start_server().await;
    let (success, resp) = run_client(
        &server.socket_path,
        &["invoke", "--tool", "filesystem.read", "--args", "{}"],
    );
    assert!(!success);
    assert_eq!(resp["error"]["code"], "invalid_args");
}

#[tokio::test]
async fn raw_malformed_line_gets_a_structured_error() {
    let server = start_server().await;
    let stream = UnixStream::connect(&server.socket_path).await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    write_half.write_all(b"this is not json\n").await.unwrap();

    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();

    let resp: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(resp["ok"], false);
    assert_eq!(resp["error"]["code"], "invalid_request");
}

#[tokio::test]
async fn server_serves_two_connections_concurrently() {
    let server = start_server().await;

    let connect = |socket_path: PathBuf| async move {
        let stream = UnixStream::connect(&socket_path).await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        write_half
            .write_all(b"{\"version\":1,\"id\":\"c\",\"op\":\"health\"}\n")
            .await
            .unwrap();
        let mut reader = BufReader::new(read_half);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        line
    };

    let a = connect(server.socket_path.clone());
    let b = connect(server.socket_path.clone());
    let (line_a, line_b) = tokio::join!(a, b);

    for line in [line_a, line_b] {
        let resp: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(resp["ok"], true);
    }
}

#[test]
fn server_removes_socket_file_on_graceful_shutdown() {
    // Not a #[tokio::test]: this test manages its own child process and
    // asserts on filesystem state after a clean SIGTERM, independent of any
    // async runtime.
    let read_dir = tempfile::tempdir().unwrap();
    let socket_dir = tempfile::tempdir().unwrap();
    let socket_path = socket_dir.path().join("shutdown-test.sock");

    let mut child = Command::new(env!("CARGO_BIN_EXE_agent-tools-server"))
        .arg("--socket-path")
        .arg(&socket_path)
        .arg("--read-dir")
        .arg(read_dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    for _ in 0..100 {
        if socket_path.exists() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(socket_path.exists(), "socket file was never created");

    // SIGTERM for a graceful shutdown, matching how a service manager would
    // stop the process.
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
    }

    let _ = child.wait();
    std::io::stdout().flush().ok();

    for _ in 0..100 {
        if !socket_path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("socket file was not removed after graceful shutdown");
}
