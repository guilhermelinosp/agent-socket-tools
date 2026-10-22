# AST Python client

A minimal, synchronous Python implementation of the AST (Agent Socket
Transport) wire protocol, built to verify — not just document — that
`docs/AST_PROTOCOL.md` is actually sufficient to implement a conforming
client in a second language.

It is a reference/interop client, not a production SDK: one request in
flight at a time per call, no concurrent multiplexing (unlike the Rust
`AstClient`). HELLO, PING, DISCOVER, INVOKE, CANCEL, BATCH, and
`STREAM_START`/`STREAM_CHUNK`/`STREAM_END` are all implemented —
including both streaming modes (chunked delivery of an already-computed
result, and genuine incremental streaming for a tool like
`filesystem.read` that supports it — see `docs/AST_PROTOCOL.md`'s
"Chunked streaming" section for exactly what distinguishes the two).

## Usage

```python
from ast_client import AstClient

with AstClient.connect("/path/to/ast.sock") as client:
    tools = client.discover()
    result = client.invoke("system.info")
    streamed = client.invoke_stream("filesystem.read", {"path": "big.txt"})
    batch_results = client.batch([
        {"id": 1, "tool": "system.info", "args": {}},
        {"id": 2, "tool": "filesystem.read", "args": {"path": "notes.txt"}},
    ])
```

`invoke` and `invoke_stream` return the exact same reassembled value for
the same call — the difference is only how it travels on the wire.

## Running the interop tests

These start a real, unmodified `ast-server` binary as a subprocess and
drive it with this client — proof of interoperability, not a mock.

```bash
cargo build --bin ast-server   # from the repo root
pip install -r requirements.txt
pytest tests/test_interop.py -v
```
