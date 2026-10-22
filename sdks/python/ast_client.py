"""A minimal, synchronous Python client for AST (Agent Socket Transport).

This exists to make "cross-language interoperability" in
``docs/AST_PROTOCOL.md`` a verified fact, not just a documented goal: it
speaks the exact wire format implemented in ``src/ast/`` — same framing,
same MessagePack payload shapes — against the real, unmodified
``ast-server`` binary, with no changes on the Rust side.

Scope, deliberately: this is a reference/interop client, not a
production SDK. It is synchronous and performs one request at a time per
call (no concurrent multiplexing of several in-flight requests the way
the Rust ``AstClient`` does) — proving the wire format and message
semantics are portable doesn't require reproducing Rust's concurrency
model in Python too. HELLO, PING, DISCOVER, INVOKE, CANCEL, and BATCH
are all implemented. Streamed INVOKEs are supported through
``invoke_stream``, which validates chunk sequence and total bytes (see
``docs/AST_PROTOCOL.md``'s "Chunked streaming" section); it aggregates
the whole result and has no incremental API like the Rust client's.

Requires only the stdlib plus ``msgpack`` (``pip install msgpack``).
"""

from __future__ import annotations

import socket
import struct
from dataclasses import dataclass, field
from typing import Any

import msgpack

AST_PROTOCOL_VERSION = 1

HEADER_LEN = 4 + 1 + 8  # length (u32) + type (u8) + correlation_id (u64)
MIN_FRAME_LEN = 9

# Message type bytes — must match src/ast/message.rs::MessageType exactly.
TYPE_HELLO = 1
TYPE_HELLO_ACK = 2
TYPE_DISCOVER = 3
TYPE_DISCOVER_RESULT = 4
TYPE_INVOKE = 5
TYPE_RESULT = 6
TYPE_ERROR = 7
TYPE_PING = 8
TYPE_PONG = 9
TYPE_CANCEL = 10
TYPE_BATCH = 11
TYPE_BATCH_RESULT = 12
TYPE_STREAM_START = 13
TYPE_STREAM_CHUNK = 14
TYPE_STREAM_END = 15


class AstProtocolError(Exception):
    """The peer sent something that doesn't fit the AST wire format."""


class AstServerError(Exception):
    """The server answered with an `ERROR` frame."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


@dataclass
class Frame:
    msg_type: int
    correlation_id: int
    payload: bytes


def encode_frame(frame: Frame) -> bytes:
    body = struct.pack(">BQ", frame.msg_type, frame.correlation_id) + frame.payload
    return struct.pack(">I", len(body)) + body


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    chunks = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise AstProtocolError("connection closed before a complete frame arrived")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_frame(sock: socket.socket, max_frame_len: int = 4 * 1024 * 1024) -> Frame:
    length_bytes = sock.recv(4)
    if len(length_bytes) < 4:
        length_bytes += _recv_exact(sock, 4 - len(length_bytes))
    (length,) = struct.unpack(">I", length_bytes)
    if length < MIN_FRAME_LEN:
        raise AstProtocolError(f"frame length {length} is below the minimum of {MIN_FRAME_LEN}")
    if length > max_frame_len:
        raise AstProtocolError(f"frame length {length} exceeds the configured maximum of {max_frame_len}")
    body = _recv_exact(sock, length)
    msg_type, correlation_id = struct.unpack(">BQ", body[:9])
    payload = body[9:]
    return Frame(msg_type=msg_type, correlation_id=correlation_id, payload=payload)


def _pack(obj: dict[str, Any]) -> bytes:
    return msgpack.packb(obj, use_bin_type=True)


def _unpack(data: bytes) -> dict[str, Any]:
    return msgpack.unpackb(data, raw=False)


@dataclass
class AstClient:
    """One AST connection. Not thread-safe; one call in flight at a time."""

    sock: socket.socket
    _next_correlation_id: int = field(default=1, init=False)

    @classmethod
    def connect(cls, socket_path: str, client_name: str = "python-ast-client") -> "AstClient":
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(socket_path)
        client = cls(sock=sock)
        client._hello(client_name)
        return client

    def _correlation_id(self) -> int:
        cid = self._next_correlation_id
        self._next_correlation_id += 1
        return cid

    def _send(self, msg_type: int, correlation_id: int, payload: dict[str, Any]) -> None:
        frame = Frame(msg_type=msg_type, correlation_id=correlation_id, payload=_pack(payload))
        self.sock.sendall(encode_frame(frame))

    def _request(self, msg_type: int, payload: dict[str, Any]) -> Frame:
        cid = self._correlation_id()
        self._send(msg_type, cid, payload)
        return read_frame(self.sock)

    def _hello(self, client_name: str) -> None:
        frame = self._request(
            TYPE_HELLO,
            {"protocol_version": AST_PROTOCOL_VERSION, "client_name": client_name},
        )
        if frame.msg_type != TYPE_HELLO_ACK:
            raise AstProtocolError(f"expected HELLO_ACK, got type {frame.msg_type}")
        ack = _unpack(frame.payload)
        if not ack.get("ok"):
            raise AstProtocolError(f"server rejected HELLO: {ack.get('error')}")

    def ping(self) -> None:
        frame = self._request(TYPE_PING, {})
        if frame.msg_type != TYPE_PONG:
            raise AstProtocolError(f"expected PONG, got type {frame.msg_type}")

    def discover(self) -> list[dict[str, Any]]:
        frame = self._request(TYPE_DISCOVER, {})
        self._raise_if_error(frame)
        if frame.msg_type != TYPE_DISCOVER_RESULT:
            raise AstProtocolError(f"expected DISCOVER_RESULT, got type {frame.msg_type}")
        return _unpack(frame.payload)["tools"]

    def invoke(self, tool: str, args: dict[str, Any] | None = None, deadline_ms: int | None = None) -> Any:
        payload: dict[str, Any] = {"tool": tool, "args": args or {}}
        if deadline_ms is not None:
            payload["deadline_ms"] = deadline_ms
        frame = self._request(TYPE_INVOKE, payload)
        self._raise_if_error(frame)
        if frame.msg_type != TYPE_RESULT:
            raise AstProtocolError(f"expected RESULT, got type {frame.msg_type}")
        return _unpack(frame.payload)["value"]

    def invoke_stream(
        self, tool: str, args: dict[str, Any] | None = None, deadline_ms: int | None = None
    ) -> Any:
        """Like :meth:`invoke`, but requests chunked/streamed delivery —
        returns the exact same reassembled value either way.

        Two wire modes exist (see ``docs/AST_PROTOCOL.md``'s "Chunked
        streaming" section), both handled here:

        - Chunked delivery of an already-computed result (``StreamStart``
          has no ``raw`` key): concatenated chunk bytes are the
          MessagePack encoding of the result value itself.
        - Genuine incremental streaming (``StreamStart["raw"]`` present,
          e.g. for ``filesystem.read``): concatenated chunk bytes are the
          raw UTF-8 bytes of one field (``raw["field_name"]``), inserted
          into ``raw["metadata"]`` to reassemble the full result — never
          MessagePack-decoded, since they were never MessagePack-encoded
          in the first place.
        """
        payload: dict[str, Any] = {"tool": tool, "args": args or {}, "stream": True}
        if deadline_ms is not None:
            payload["deadline_ms"] = deadline_ms
        cid = self._correlation_id()
        self._send(TYPE_INVOKE, cid, payload)

        first = self._recv_matching(cid)
        self._raise_if_error(first)
        if first.msg_type != TYPE_STREAM_START:
            raise AstProtocolError(f"expected STREAM_START, got type {first.msg_type}")
        start = _unpack(first.payload)
        declared_total = int(start["total_bytes"])

        encoded = bytearray()
        expected_sequence = 0
        while True:
            frame = self._recv_matching(cid)
            # A mid-stream ERROR is a server failure, not a protocol
            # violation: raise AstServerError just like a failed first frame.
            self._raise_if_error(frame)
            if frame.msg_type == TYPE_STREAM_CHUNK:
                chunk = _unpack(frame.payload)
                # Chunks must arrive as 0, 1, 2, ... with no gaps, repeats
                # or reordering (docs/AST_PROTOCOL.md, "Chunked streaming").
                if chunk["sequence"] != expected_sequence:
                    raise AstProtocolError(
                        f"stream protocol violation: chunk sequence {chunk['sequence']} "
                        f"arrived where {expected_sequence} was expected"
                    )
                expected_sequence += 1
                encoded.extend(chunk["data"])
            elif frame.msg_type == TYPE_STREAM_END:
                # The server always sends exactly STREAM_START.total_bytes.
                if len(encoded) != declared_total:
                    raise AstProtocolError(
                        f"stream protocol violation: STREAM_END after {len(encoded)} bytes, "
                        f"but STREAM_START declared total_bytes {declared_total}"
                    )
                break
            else:
                raise AstProtocolError(f"unexpected message type during stream: {frame.msg_type}")

        raw = start.get("raw")
        if raw is None:
            return _unpack(bytes(encoded))
        value = dict(raw["metadata"])
        value[raw["field_name"]] = bytes(encoded).decode("utf-8")
        return value

    def _recv_matching(self, correlation_id: int) -> Frame:
        """Reads frames until one matches `correlation_id`. This client is
        synchronous and single-request-at-a-time (see the module
        docstring), so in practice the very next frame always matches —
        this just makes that assumption explicit and fails loudly rather
        than silently misattributing a frame if it's ever violated.
        """
        frame = read_frame(self.sock)
        if frame.correlation_id != correlation_id:
            raise AstProtocolError(
                f"expected a frame for correlation_id={correlation_id}, got {frame.correlation_id} "
                "(this client does not support concurrent in-flight requests)"
            )
        return frame

    def batch(self, calls: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """`calls`: a list of `{"id", "tool", "args", "deadline_ms"?}`.

        Returns the raw `BatchResultEntry` list — each `{"id", "status":
        "ok"|"error", "value"}` or `{"id", "status": "error", "code",
        "message"}` — matching the Rust side's internally-tagged
        `BatchOutcome` encoding exactly (see `src/ast/message.rs`).
        """
        frame = self._request(TYPE_BATCH, {"calls": calls})
        self._raise_if_error(frame)
        if frame.msg_type != TYPE_BATCH_RESULT:
            raise AstProtocolError(f"expected BATCH_RESULT, got type {frame.msg_type}")
        return _unpack(frame.payload)["results"]

    def cancel(self, target_correlation_id: int) -> None:
        # Fire-and-forget, like the Rust client: CANCEL has no direct
        # response of its own (the request it targets gets an ERROR/
        # CANCELLED instead, delivered whenever that in-flight call's
        # read is next pumped).
        self._send(TYPE_CANCEL, self._correlation_id(), {"target_correlation_id": target_correlation_id})

    def _raise_if_error(self, frame: Frame) -> None:
        if frame.msg_type == TYPE_ERROR:
            err = _unpack(frame.payload)
            raise AstServerError(code=err["code"], message=err["message"])

    def close(self) -> None:
        self.sock.close()

    def __enter__(self) -> "AstClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
