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
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ast_client import (  # noqa: E402
    AST_PROTOCOL_VERSION,
    TYPE_ERROR,
    TYPE_HELLO,
    TYPE_HELLO_ACK,
    TYPE_STREAM_CHUNK,
    TYPE_STREAM_END,
    TYPE_STREAM_START,
    AstClient,
    AstProtocolError,
    AstServerError,
    Frame,
    _pack,
    encode_frame,
    read_frame,
)

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


def test_filesystem_read_without_path_is_invalid_arguments(server):
    with AstClient.connect(str(server)) as client:
        with pytest.raises(AstServerError) as exc_info:
            client.invoke("filesystem.read", {})
    assert exc_info.value.code == "INVALID_ARGUMENTS"


def test_batch_over_the_call_limit_is_invalid_message(server):
    calls = [{"id": i, "tool": "system.info", "args": {}} for i in range(101)]
    with AstClient.connect(str(server)) as client:
        with pytest.raises(AstServerError) as exc_info:
            client.batch(calls)
    assert exc_info.value.code == "INVALID_MESSAGE"


def test_cancel_of_an_unknown_id_leaves_the_connection_usable(server):
    with AstClient.connect(str(server)) as client:
        client.cancel(999999)  # fire-and-forget: nothing to unknown ids
        client.ping()  # the next exchange must still be well-formed


@contextmanager
def _fake_server(frames_to_send):
    """A fake AST server on a Unix socket. It accepts one connection,
    answers HELLO with an ok HELLO_ACK, reads the INVOKE, then sends each
    ``(msg_type, payload_dict)`` in ``frames_to_send`` tagged with the
    INVOKE's correlation_id. It keeps the socket open until the client
    closes, so the client's next read fails only on the test's own terms.
    """
    with tempfile.TemporaryDirectory() as socket_dir:
        socket_path = os.path.join(socket_dir, "fake-ast.sock")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(socket_path)
        listener.listen(1)

        def serve() -> None:
            try:
                conn, _ = listener.accept()
                with conn:
                    hello = read_frame(conn)
                    ack = {
                        "protocol_version": AST_PROTOCOL_VERSION,
                        "server_name": "fake-ast-server",
                        "ok": True,
                        "error": None,
                    }
                    conn.sendall(
                        encode_frame(Frame(TYPE_HELLO_ACK, hello.correlation_id, _pack(ack)))
                    )
                    invoke = read_frame(conn)
                    for msg_type, payload in frames_to_send:
                        conn.sendall(
                            encode_frame(Frame(msg_type, invoke.correlation_id, _pack(payload)))
                        )
                    conn.recv(1)  # wait for the client to hang up
            except OSError:
                pass  # the client went away first; the test asserts on its side
            finally:
                listener.close()

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        try:
            yield socket_path
        finally:
            thread.join(timeout=5)


def test_stream_sequence_violation_raises():
    frames = [
        (TYPE_STREAM_START, {"total_chunks": 2, "total_bytes": 6}),
        (TYPE_STREAM_CHUNK, {"sequence": 0, "data": b"abc"}),
        (TYPE_STREAM_CHUNK, {"sequence": 2, "data": b"def"}),  # skips 1
        (TYPE_STREAM_END, {}),
    ]
    with _fake_server(frames) as path:
        with AstClient.connect(path) as client:
            with pytest.raises(AstProtocolError, match="stream protocol violation"):
                client.invoke_stream("whatever")


def test_stream_byte_total_mismatch_raises():
    frames = [
        (TYPE_STREAM_START, {"total_chunks": 1, "total_bytes": 10}),
        (TYPE_STREAM_CHUNK, {"sequence": 0, "data": b"abc"}),  # 3 of the promised 10
        (TYPE_STREAM_END, {}),
    ]
    with _fake_server(frames) as path:
        with AstClient.connect(path) as client:
            with pytest.raises(AstProtocolError, match="stream protocol violation"):
                client.invoke_stream("whatever")


def test_mid_stream_error_raises_server_error():
    frames = [
        (TYPE_STREAM_START, {"total_chunks": 2, "total_bytes": 20}),
        (TYPE_STREAM_CHUNK, {"sequence": 0, "data": b"abc"}),
        (TYPE_ERROR, {"code": "DEADLINE_EXCEEDED", "message": "simulated mid-stream failure"}),
    ]
    with _fake_server(frames) as path:
        with AstClient.connect(path) as client:
            with pytest.raises(AstServerError) as exc_info:
                client.invoke_stream("whatever")
    assert exc_info.value.code == "DEADLINE_EXCEEDED"


def test_stream_over_cap_is_rejected_before_reading_chunks():
    # Only STREAM_START is sent. The client must refuse on the declared size
    # alone; if it tried to read chunks first it would block until the timeout.
    frames = [(TYPE_STREAM_START, {"total_chunks": 1, "total_bytes": 100})]
    with _fake_server(frames) as path:
        with AstClient.connect(path, timeout=5.0, max_stream_bytes=10) as client:
            started = time.monotonic()
            with pytest.raises(AstProtocolError, match="max_stream_bytes"):
                client.invoke_stream("whatever")
            assert time.monotonic() - started < 2.0


def test_client_is_unusable_after_protocol_violation():
    frames = [
        (TYPE_STREAM_START, {"total_chunks": 2, "total_bytes": 6}),
        (TYPE_STREAM_CHUNK, {"sequence": 0, "data": b"abc"}),
        (TYPE_STREAM_CHUNK, {"sequence": 2, "data": b"def"}),  # skips 1
    ]
    with _fake_server(frames) as path:
        with AstClient.connect(path) as client:
            with pytest.raises(AstProtocolError, match="stream protocol violation"):
                client.invoke_stream("whatever")
            with pytest.raises(AstProtocolError, match="unusable after a protocol failure"):
                client.ping()


def test_server_error_does_not_break_the_connection(server):
    with AstClient.connect(str(server)) as client:
        with pytest.raises(AstServerError):
            client.invoke_stream("filesystem.read", {"path": "../../etc/passwd"})
        client.ping()  # the connection is still in sync and usable


@contextmanager
def _silent_server():
    """A fake AST server that accepts one connection and never writes to it."""
    with tempfile.TemporaryDirectory() as socket_dir:
        socket_path = os.path.join(socket_dir, "silent-ast.sock")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(socket_path)
        listener.listen(1)

        def serve() -> None:
            try:
                conn, _ = listener.accept()
                with conn:
                    while conn.recv(4096):  # discard the client's bytes; returns on hang-up
                        pass
            except OSError:
                pass
            finally:
                listener.close()

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        try:
            yield socket_path
        finally:
            thread.join(timeout=5)


def test_socket_timeout_raises():
    with _silent_server() as path:
        started = time.monotonic()
        with pytest.raises(AstProtocolError, match="timed out"):
            AstClient.connect(path, timeout=0.3)
        assert time.monotonic() - started < 3.0
