"""End-to-end interoperability tests: the Python `ast_client` against the
real, unmodified, compiled `ast-server` binary — proving the Rust-defined
wire format and message semantics are actually portable, not merely
documented as portable.

Run with the real server already built:
    cargo build --bin ast-server
    pip install msgpack pytest
    pytest sdks/python/tests/test_interop.py
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ast_client import AstClient, AstServerError  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[3]


def _find_ast_server_binary() -> Path:
    for profile in ("debug", "release"):
        candidate = REPO_ROOT / "target" / profile / "ast-server"
        if candidate.exists():
            return candidate
    pytest.skip("ast-server binary not built — run `cargo build --bin ast-server` first")


def _wait_for_socket(path: Path, timeout_s: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if path.exists():
            try:
                s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                s.connect(str(path))
                s.close()
                return
            except OSError:
                pass
        time.sleep(0.02)
    raise TimeoutError(f"ast-server did not create a usable socket at {path} in time")


@pytest.fixture
def server():
    binary = _find_ast_server_binary()
    with tempfile.TemporaryDirectory() as socket_dir, tempfile.TemporaryDirectory() as read_dir:
        Path(read_dir, "hello.txt").write_text("hello from python interop test\n")
        Path(read_dir, "large.txt").write_text("y" * 200_000)
        socket_path = Path(socket_dir) / "ast-py-interop.sock"
        proc = subprocess.Popen(
            [str(binary), "--socket-path", str(socket_path), "--read-dir", read_dir],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            _wait_for_socket(socket_path)
            yield socket_path
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def test_hello_handshake_succeeds(server):
    client = AstClient.connect(str(server))
    client.close()


def test_ping_round_trip(server):
    with AstClient.connect(str(server)) as client:
        client.ping()  # raises on anything but a well-formed PONG


def test_discover_lists_both_mvp_tools(server):
    with AstClient.connect(str(server)) as client:
        tools = client.discover()
    names = {t["name"] for t in tools}
    assert "system.info" in names
    assert "filesystem.read" in names


def test_invoke_system_info_returns_this_hosts_os(server):
    with AstClient.connect(str(server)) as client:
        result = client.invoke("system.info")
    assert result["os"] in ("linux", "macos")


def test_invoke_filesystem_read_succeeds_for_authorized_file(server):
    with AstClient.connect(str(server)) as client:
        result = client.invoke("filesystem.read", {"path": "hello.txt"})
    assert result["contents"] == "hello from python interop test\n"


def test_invoke_unknown_tool_raises_ast_server_error(server):
    with AstClient.connect(str(server)) as client:
        with pytest.raises(AstServerError) as exc_info:
            client.invoke("does.not.exist")
    assert exc_info.value.code == "TOOL_NOT_FOUND"


def test_invoke_filesystem_read_rejects_traversal(server):
    with AstClient.connect(str(server)) as client:
        with pytest.raises(AstServerError):
            client.invoke("filesystem.read", {"path": "../../etc/passwd"})


def test_batch_runs_multiple_calls_and_correlates_by_id(server):
    with AstClient.connect(str(server)) as client:
        results = client.batch(
            [
                {"id": 1, "tool": "system.info", "args": {}},
                {"id": 2, "tool": "filesystem.read", "args": {"path": "hello.txt"}},
                {"id": 3, "tool": "does.not.exist", "args": {}},
            ]
        )
    by_id = {entry["id"]: entry for entry in results}
    assert by_id[1]["status"] == "ok"
    assert by_id[2]["status"] == "ok"
    assert by_id[2]["value"]["contents"] == "hello from python interop test\n"
    assert by_id[3]["status"] == "error"
    assert by_id[3]["code"] == "TOOL_NOT_FOUND"


def test_multiple_sequential_calls_on_one_connection(server):
    with AstClient.connect(str(server)) as client:
        for _ in range(5):
            client.invoke("system.info")


def test_invoke_stream_matches_invoke_for_a_non_streaming_tool(server):
    with AstClient.connect(str(server)) as client:
        plain = client.invoke("system.info")
        streamed = client.invoke_stream("system.info")
    assert plain == streamed


def test_invoke_stream_against_a_genuinely_streaming_tool_matches_invoke(server):
    with AstClient.connect(str(server)) as client:
        plain = client.invoke("filesystem.read", {"path": "hello.txt"})
        streamed = client.invoke_stream("filesystem.read", {"path": "hello.txt"})
    assert plain == streamed
    assert streamed["contents"] == "hello from python interop test\n"


def test_invoke_stream_of_a_large_genuinely_streamed_file(server):
    with AstClient.connect(str(server)) as client:
        streamed = client.invoke_stream("filesystem.read", {"path": "large.txt"})
    assert streamed["contents"] == "y" * 200_000
    assert streamed["size_bytes"] == 200_000


def test_invoke_stream_of_an_unknown_tool_reports_the_same_error_invoke_would(server):
    with AstClient.connect(str(server)) as client:
        with pytest.raises(AstServerError) as exc_info:
            client.invoke_stream("does.not.exist")
    assert exc_info.value.code == "TOOL_NOT_FOUND"
