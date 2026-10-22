# Wire protocol (version 1)

Transport: a Unix Domain Socket, stream mode (`SOCK_STREAM`). Framing: one
UTF-8 JSON object per line, terminated by `\n`. Both directions use the
same framing; a connection may carry any number of request/response pairs
in sequence before either side closes it.

This document describes version `1`. The `version` field exists so future,
breaking changes can be introduced as version `2`, `3`, etc. without
ambiguity; a server that doesn't understand a request's `version` responds
with `unsupported_version` rather than guessing.

## Request

```json
{
  "version": 1,
  "id": "req-001",
  "op": "invoke",
  "tool": "git.status",
  "args": { "repository": "/workspace/project" }
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `version` | integer | yes | Must equal `1` for this document. |
| `id` | string | yes | Chosen by the client, echoed back unchanged. Used to correlate responses when a connection is reused for multiple sequential requests. |
| `op` | `"discover" \| "invoke" \| "health"` | yes | |
| `tool` | string | only for `invoke` | Dotted tool name, e.g. `"system.info"`. Ignored for other ops. |
| `args` | object | only for `invoke` | Tool-specific; validated by the tool itself. Ignored for other ops. |

## Response

```json
{
  "version": 1,
  "id": "req-001",
  "ok": true,
  "result": { "clean": true, "changed_files": 0 }
}
```

```json
{
  "version": 1,
  "id": "req-001",
  "ok": false,
  "error": { "code": "tool_not_found", "message": "no such tool: git.status" }
}
```

| Field | Type | Present when |
|---|---|---|
| `version` | integer | always |
| `id` | string | always — echoes the request's `id`, or the literal string `"unknown"` when the request couldn't be parsed far enough to recover an `id` |
| `ok` | boolean | always |
| `result` | any JSON value | `ok == true` |
| `error` | object (`code`, `message`) | `ok == true` is false |

`result` and `error` are mutually exclusive and never both present.

## Operations

### `discover`

Takes no arguments. Returns an array of tool descriptors:

```json
{
  "version": 1,
  "id": "d1",
  "ok": true,
  "result": [
    {
      "name": "system.info",
      "description": "Returns the operating system family and CPU architecture of the host running the server.",
      "input_schema": { "type": "object", "additionalProperties": false, "properties": {} }
    },
    {
      "name": "filesystem.read",
      "description": "Reads a UTF-8 text file from within the server's authorized base directory.",
      "input_schema": {
        "type": "object",
        "additionalProperties": false,
        "required": ["path"],
        "properties": {
          "path": { "type": "string", "description": "Path to the file, absolute or relative to the authorized base directory." }
        }
      }
    }
  ]
}
```

`input_schema` is a JSON Schema (draft-07 subset) fragment describing
`args` for that tool. It is advisory — the server still validates `args`
itself on `invoke` regardless of what a client does with the schema.

### `health`

Takes no arguments. Returns the server's liveness and the number of
registered tools:

```json
{ "version": 1, "id": "h1", "ok": true, "result": { "status": "ok", "tool_count": 2 } }
```

### `invoke`

Requires `tool` (string) and, depending on the tool, `args` (object).
Returns the tool's own result shape on success, under `result`.

## Tools (MVP)

### `system.info`

- **Args:** none (an empty object, or the field omitted entirely).
- **Result:**
  ```json
  { "os": "macos", "arch": "aarch64", "family": "unix" }
  ```
  `os`/`arch`/`family` are Rust's `std::env::consts::{OS,ARCH,FAMILY}` for
  the host the *server* is running on, not the client.

### `filesystem.read`

- **Args:**
  ```json
  { "path": "relative/or/absolute/path.txt" }
  ```
  `path` is resolved against the server's `--read-dir`. It may be relative
  (joined to the base directory) or absolute (still checked against the
  base directory after resolution — an absolute path outside the base
  directory is rejected the same as a relative traversal).
- **Result:**
  ```json
  { "path": "relative/path.txt", "size_bytes": 42, "contents": "…" }
  ```
  `path` in the result is the resolved path relative to the base
  directory. `contents` is the full file content, which must be valid
  UTF-8 and no larger than 1 MiB (hard-coded `MAX_READ_BYTES`; not
  currently CLI-configurable).

## Error codes

| Code | Meaning |
|---|---|
| `invalid_request` | The request line wasn't valid JSON, or didn't match the `Request` shape (e.g. `invoke` without a `tool` field). |
| `unsupported_version` | `version` didn't match a version this server understands. |
| `tool_not_found` | `tool` named something not in the registry. |
| `invalid_args` | `args` failed the named tool's own validation. |
| `unauthorized` | The tool refused the operation for a policy reason — today, exclusively "the resolved path is outside the authorized base directory." |
| `tool_error` | The tool ran but failed for an operational reason (file not found, not a regular file, I/O error, non-UTF-8 content). |
| `timeout` | The tool did not complete within `--request-timeout-secs`. |
| `message_too_large` | A line exceeded `--max-message-bytes` before a newline was found. |
| `internal` | Any other server-side failure (e.g. the tool's task panicked). |

New codes may be added in future minor versions of this document without
a protocol version bump, since clients are expected to treat any
`error.code` they don't recognize as a generic failure. Removing or
changing the meaning of an existing code is a breaking change and requires
bumping `version`.

## Example session (raw bytes over the socket)

```
C→S  {"version":1,"id":"1","op":"health"}\n
S→C  {"version":1,"id":"1","ok":true,"result":{"status":"ok","tool_count":2}}\n
C→S  {"version":1,"id":"2","op":"invoke","tool":"system.info","args":{}}\n
S→C  {"version":1,"id":"2","ok":true,"result":{"os":"macos","arch":"aarch64","family":"unix"}}\n
```
