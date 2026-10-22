# AST Python client

A minimal, synchronous Python implementation of the AST (Agent Socket
Transport) wire protocol, built to verify — not just document — that
`docs/AST_PROTOCOL.md` is actually sufficient to implement a conforming
client in a second language.

It is a reference/interop client, not a production SDK: one request in
flight at a time per call, no concurrent multiplexing (unlike the Rust
`AstClient`). HELLO, PING, DISCOVER, INVOKE, CANCEL, and BATCH are all
implemented; `STREAM_*` isn't, because the Rust side doesn't implement
it either.

## Usage

```python
from ast_client import AstClient

with AstClient.connect("/path/to/ast.sock") as client:
    tools = client.discover()
    result = client.invoke("system.info")
    batch_results = client.batch([
        {"id": 1, "tool": "system.info", "args": {}},
        {"id": 2, "tool": "filesystem.read", "args": {"path": "notes.txt"}},
    ])
```

## Running the interop tests

These start a real, unmodified `ast-server` binary as a subprocess and
drive it with this client — proof of interoperability, not a mock.

```bash
cargo build --bin ast-server   # from the repo root
pip install -r requirements.txt
pytest tests/test_interop.py -v
```
