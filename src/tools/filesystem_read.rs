use super::{IncrementalBytes, StreamingTool, StreamingToolOpen, Tool, ToolError};
use serde::Deserialize;
use serde_json::{json, Value};
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

/// Caps how much of a file `invoke` (the non-streaming path) reads into
/// memory at once, to bound server memory use regardless of the on-disk
/// file size.
const MAX_READ_BYTES: u64 = 1024 * 1024; // 1 MiB

/// Caps how much of a file `open_stream` (the genuine-incremental path)
/// will stream, total. Much larger than `MAX_READ_BYTES` because the
/// whole point of streaming is that peak server memory stays bounded to
/// one chunk's size *regardless* of this limit — unlike `invoke`, where
/// the cap itself is the only thing bounding memory. Still finite, not
/// unbounded: an operator exposing this tool to untrusted callers should
/// not have to trust that every caller behaves, and a deadline alone
/// doesn't bound how many bytes a slow, deadline-respecting stream can
/// still produce before it fires.
const MAX_STREAM_READ_BYTES: u64 = 256 * 1024 * 1024; // 256 MiB

#[derive(Debug, Deserialize)]
struct Args {
    path: String,
}

/// Reads a UTF-8 text file from inside a single authorized base directory.
///
/// The base directory is fixed at construction time by the server operator,
/// never by the client. Every request path is resolved to an absolute,
/// symlink-free path and checked against that base directory before any
/// data is read, which also rejects a symlink *inside* the base directory
/// that points *outside* it: such a symlink canonicalizes to a path outside
/// the base directory and is rejected the same way a literal `../`
/// traversal would be.
///
/// ## TOCTOU
///
/// There is an unavoidable gap between resolving/validating a path and
/// acting on it: `resolve` canonicalizes and checks the path, and the
/// filesystem can change between that check and the subsequent `open`.
/// This implementation narrows, but does not eliminate, that window:
///
/// - The final `open` uses `O_NOFOLLOW`, so if something replaces the
///   target *file itself* with a symlink in that window, the open fails
///   outright instead of silently following the symlink to an
///   unauthorized location.
/// - Size and file-type checks are read from the already-open file
///   descriptor's metadata (`fstat`), not from a second, path-based `stat`
///   call, which removes the separate TOCTOU window that a path-based
///   check-then-open-then-check-again pattern would have.
///
/// What this does **not** protect against: `O_NOFOLLOW` only covers the
/// path's final component. An attacker who can rename or relink
/// *intermediate directories* inside the authorized tree between
/// canonicalization and `open` could still redirect where the open
/// ultimately lands — there is no portable (Linux+macOS) equivalent of
/// Linux's `openat2(RESOLVE_NO_SYMLINKS | RESOLVE_BENEATH)` available here
/// without a Linux-only dependency, which this project avoids per its
/// "no unnecessary dependencies" / "keep Linux and macOS compatible"
/// constraints. The threat model this crate assumes is: the authorized
/// base directory tree is not being concurrently modified by an untrusted
/// actor while the server is running. If that assumption doesn't hold in
/// your deployment, don't point `--read-dir` at a directory writable by
/// anyone other than whoever runs the server.
pub struct FilesystemReadTool {
    /// Canonicalized once at construction; `invoke` never re-reads this from
    /// the filesystem to avoid any race on the base directory itself.
    base_dir: PathBuf,
}

impl FilesystemReadTool {
    /// Fails if `base_dir` does not exist or cannot be canonicalized.
    pub fn new(base_dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let base_dir = base_dir.as_ref().canonicalize()?;
        Ok(Self { base_dir })
    }

    fn resolve(&self, requested: &str) -> Result<PathBuf, ToolError> {
        if requested.is_empty() {
            return Err(ToolError::InvalidArgs("path must not be empty".into()));
        }
        let candidate = if Path::new(requested).is_absolute() {
            PathBuf::from(requested)
        } else {
            self.base_dir.join(requested)
        };

        let canonical = candidate.canonicalize().map_err(|_| {
            ToolError::Failed(format!(
                "cannot read {requested:?}: not found or inaccessible"
            ))
        })?;

        if !canonical.starts_with(&self.base_dir) {
            return Err(ToolError::Unauthorized(format!(
                "{requested:?} resolves outside the authorized base directory"
            )));
        }

        Ok(canonical)
    }

    /// Resolves, opens (`O_NOFOLLOW`), and `fstat`s the requested file —
    /// every authorization/validation step `invoke` and `open_stream`
    /// both need, with no size cap applied yet (each caller enforces its
    /// own, different, limit). Returns the open file, its path relative
    /// to the base directory (for the result's `path` field), and its
    /// size from the file descriptor's own metadata.
    fn open_validated(&self, args: Value) -> Result<(std::fs::File, PathBuf, u64), ToolError> {
        let args: Args =
            serde_json::from_value(args).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;

        let resolved = self.resolve(&args.path)?;

        // `O_NOFOLLOW`: if the final path component was swapped for a
        // symlink after `resolve`'s canonicalization but before this open,
        // fail loudly (ELOOP) instead of transparently following it
        // somewhere unauthorized. See the TOCTOU note on this struct.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&resolved)
            .map_err(|e| ToolError::Failed(format!("open failed: {e}")))?;

        // Checked on the already-open descriptor (`fstat`), not via a
        // second path-based `stat`, so there's no separate race between
        // this check and the read below.
        let metadata = file
            .metadata()
            .map_err(|e| ToolError::Failed(format!("stat failed: {e}")))?;
        if !metadata.is_file() {
            return Err(ToolError::Failed("not a regular file".into()));
        }

        let relative = resolved
            .strip_prefix(&self.base_dir)
            .unwrap_or(&resolved)
            .to_path_buf();
        Ok((file, relative, metadata.len()))
    }
}

impl Tool for FilesystemReadTool {
    fn name(&self) -> &str {
        "filesystem.read"
    }

    fn description(&self) -> &str {
        "Reads a UTF-8 text file from within the server's authorized base directory."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["path"],
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to the file, absolute or relative to the authorized base directory."
                }
            }
        })
    }

    fn invoke(&self, args: Value) -> Result<Value, ToolError> {
        let (mut file, relative, size) = self.open_validated(args)?;
        if size > MAX_READ_BYTES {
            return Err(ToolError::Failed(format!(
                "file exceeds the {MAX_READ_BYTES}-byte read limit"
            )));
        }

        let mut bytes = Vec::with_capacity(size as usize);
        file.read_to_end(&mut bytes)
            .map_err(|e| ToolError::Failed(format!("read failed: {e}")))?;
        let contents = String::from_utf8(bytes)
            .map_err(|_| ToolError::Failed("file is not valid UTF-8".into()))?;

        Ok(json!({
            "path": relative,
            "size_bytes": size,
            "contents": contents,
        }))
    }

    fn as_streaming(&self) -> Option<&dyn StreamingTool> {
        Some(self)
    }
}

impl StreamingTool for FilesystemReadTool {
    fn open_stream(&self, args: Value) -> Result<StreamingToolOpen, ToolError> {
        let (file, relative, size) = self.open_validated(args)?;
        if size > MAX_STREAM_READ_BYTES {
            return Err(ToolError::Failed(format!(
                "file exceeds the {MAX_STREAM_READ_BYTES}-byte streaming limit"
            )));
        }

        Ok(StreamingToolOpen {
            metadata: json!({
                "path": relative,
                "size_bytes": size,
            }),
            field_name: "contents",
            reader: Box::new(Utf8FileChunkReader {
                file,
                remaining: size,
                leftover: Vec::new(),
            }),
        })
    }
}

/// Reads a file's bytes incrementally, validating UTF-8 *across* chunk
/// boundaries rather than requiring the whole file in memory first.
///
/// A multi-byte UTF-8 character can straddle two reads from disk (a
/// 4-byte character split 3-and-1 across a chunk boundary, say); `leftover`
/// holds whatever trailing bytes of the last read couldn't yet be
/// confirmed as a complete, valid sequence, and is prepended to the next
/// read before re-validating. At most 3 bytes can ever be held this way —
/// the longest possible *incomplete* prefix of a 4-byte UTF-8 sequence.
struct Utf8FileChunkReader {
    file: std::fs::File,
    remaining: u64,
    leftover: Vec<u8>,
}

impl IncrementalBytes for Utf8FileChunkReader {
    fn total_bytes_hint(&self) -> Option<u64> {
        Some(self.remaining + self.leftover.len() as u64)
    }

    fn next_chunk(&mut self, max_len: usize) -> Result<Option<Vec<u8>>, ToolError> {
        loop {
            if self.remaining == 0 && self.leftover.is_empty() {
                return Ok(None);
            }

            let want = max_len.saturating_sub(self.leftover.len()).max(1);
            let to_read = want.min(self.remaining as usize);
            let mut buf = vec![0u8; to_read];
            let n = self
                .file
                .read(&mut buf)
                .map_err(|e| ToolError::Failed(format!("read failed: {e}")))?;
            buf.truncate(n);
            self.remaining -= n as u64;

            let mut combined = std::mem::take(&mut self.leftover);
            combined.extend_from_slice(&buf);

            if n == 0 {
                // EOF. Any leftover here is a genuinely incomplete/invalid
                // trailing sequence — there's no more data coming to
                // complete it.
                if combined.is_empty() {
                    return Ok(None);
                }
                return Err(ToolError::Failed("file is not valid UTF-8".into()));
            }

            match std::str::from_utf8(&combined) {
                Ok(_) => return Ok(Some(combined)),
                Err(e) if e.error_len().is_some() => {
                    // A genuinely invalid byte, not just an incomplete
                    // sequence at the end — not something more data could
                    // ever fix.
                    return Err(ToolError::Failed("file is not valid UTF-8".into()));
                }
                Err(e) => {
                    let valid_up_to = e.valid_up_to();
                    let rest = combined.split_off(valid_up_to);
                    self.leftover = rest;
                    if combined.is_empty() {
                        // Nothing confirmed-valid yet this call; read more
                        // before returning anything. `remaining` strictly
                        // decreased above, so this loop always terminates.
                        continue;
                    }
                    return Ok(Some(combined));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::symlink;

    fn tool_with_fixture() -> (tempfile::TempDir, FilesystemReadTool) {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("hello.txt"), "hello world\n").unwrap();
        fs::create_dir(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("sub/nested.txt"), "nested\n").unwrap();
        let tool = FilesystemReadTool::new(dir.path()).unwrap();
        (dir, tool)
    }

    #[test]
    fn reads_a_file_inside_the_base_dir() {
        let (_dir, tool) = tool_with_fixture();
        let result = tool.invoke(json!({"path": "hello.txt"})).unwrap();
        assert_eq!(result["contents"], "hello world\n");
    }

    #[test]
    fn reads_a_nested_file() {
        let (_dir, tool) = tool_with_fixture();
        let result = tool.invoke(json!({"path": "sub/nested.txt"})).unwrap();
        assert_eq!(result["contents"], "nested\n");
    }

    #[test]
    fn rejects_dot_dot_traversal() {
        let (_dir, tool) = tool_with_fixture();
        let err = tool
            .invoke(json!({"path": "../../etc/passwd"}))
            .unwrap_err();
        assert!(matches!(
            err,
            ToolError::Unauthorized(_) | ToolError::Failed(_)
        ));
    }

    #[test]
    fn rejects_absolute_path_outside_base_dir() {
        let (_dir, tool) = tool_with_fixture();
        let err = tool.invoke(json!({"path": "/etc/hosts"})).unwrap_err();
        assert!(matches!(
            err,
            ToolError::Unauthorized(_) | ToolError::Failed(_)
        ));
    }

    #[test]
    fn accepts_absolute_path_inside_base_dir() {
        let (dir, tool) = tool_with_fixture();
        let absolute = dir.path().canonicalize().unwrap().join("hello.txt");
        let result = tool
            .invoke(json!({"path": absolute.to_str().unwrap()}))
            .unwrap();
        assert_eq!(result["contents"], "hello world\n");
    }

    #[test]
    fn rejects_symlink_escaping_base_dir() {
        let (dir, tool) = tool_with_fixture();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("secret.txt"), "secret\n").unwrap();
        symlink(
            outside.path().join("secret.txt"),
            dir.path().join("escape.txt"),
        )
        .unwrap();

        let err = tool.invoke(json!({"path": "escape.txt"})).unwrap_err();
        assert!(matches!(err, ToolError::Unauthorized(_)));
    }

    #[test]
    fn rejects_a_symlink_swapped_in_after_resolution() {
        // Approximates the TOCTOU window `O_NOFOLLOW` closes: by the time
        // `open` runs, the target is a symlink even though nothing in
        // `resolve`'s canonicalization saw one (because the file we
        // resolved against was a plain file when canonicalized, then
        // replaced). This isn't a perfect reproduction of the race (there's
        // no concurrency here), but it does verify `O_NOFOLLOW` itself
        // actually rejects a symlink at the final path component rather
        // than silently following it.
        let (dir, tool) = tool_with_fixture();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("secret.txt"), "secret\n").unwrap();
        let target = dir.path().join("hello.txt");
        fs::remove_file(&target).unwrap();
        symlink(outside.path().join("secret.txt"), &target).unwrap();

        let err = tool.invoke(json!({"path": "hello.txt"})).unwrap_err();
        assert!(matches!(err, ToolError::Unauthorized(_)));
    }

    #[test]
    fn rejects_missing_file() {
        let (_dir, tool) = tool_with_fixture();
        assert!(tool.invoke(json!({"path": "nope.txt"})).is_err());
    }

    #[test]
    fn rejects_directories() {
        let (_dir, tool) = tool_with_fixture();
        let err = tool.invoke(json!({"path": "sub"})).unwrap_err();
        assert!(matches!(err, ToolError::Failed(_)));
    }

    #[test]
    fn rejects_missing_path_argument() {
        let (_dir, tool) = tool_with_fixture();
        assert!(tool.invoke(json!({})).is_err());
    }

    #[test]
    fn rejects_empty_path_argument() {
        let (_dir, tool) = tool_with_fixture();
        assert!(tool.invoke(json!({"path": ""})).is_err());
    }

    #[test]
    fn rejects_file_over_size_limit() {
        let (dir, tool) = tool_with_fixture();
        let big = vec![b'a'; (MAX_READ_BYTES + 1) as usize];
        fs::write(dir.path().join("big.txt"), big).unwrap();
        let err = tool.invoke(json!({"path": "big.txt"})).unwrap_err();
        assert!(matches!(err, ToolError::Failed(_)));
    }

    #[test]
    fn accepts_file_exactly_at_size_limit() {
        let (dir, tool) = tool_with_fixture();
        let exact = vec![b'a'; MAX_READ_BYTES as usize];
        fs::write(dir.path().join("exact.txt"), &exact).unwrap();
        let result = tool.invoke(json!({"path": "exact.txt"})).unwrap();
        assert_eq!(result["size_bytes"], MAX_READ_BYTES);
    }

    #[test]
    fn rejects_non_utf8_file_contents() {
        let (dir, tool) = tool_with_fixture();
        fs::write(dir.path().join("binary.bin"), [0xff, 0xfe, 0x00, 0x01]).unwrap();
        let err = tool.invoke(json!({"path": "binary.bin"})).unwrap_err();
        assert!(matches!(err, ToolError::Failed(_)));
    }

    /// Drains an `IncrementalBytes` reader with a small `max_len`, so
    /// multi-call chunk-boundary behavior is actually exercised rather
    /// than returning everything in one call.
    fn drain(reader: &mut dyn IncrementalBytes, max_len: usize) -> Result<Vec<u8>, ToolError> {
        let mut out = Vec::new();
        while let Some(chunk) = reader.next_chunk(max_len)? {
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }

    #[test]
    fn open_stream_reassembles_to_the_same_contents_invoke_would_return() {
        let (dir, tool) = tool_with_fixture();
        fs::write(dir.path().join("stream.txt"), "x".repeat(10_000)).unwrap();

        let via_invoke = tool.invoke(json!({"path": "stream.txt"})).unwrap();

        let mut opened = tool.open_stream(json!({"path": "stream.txt"})).unwrap();
        assert_eq!(opened.field_name, "contents");
        let bytes = drain(opened.reader.as_mut(), 777).unwrap(); // deliberately not a divisor of 10_000
        let contents = String::from_utf8(bytes).unwrap();

        assert_eq!(contents, via_invoke["contents"]);
        assert_eq!(opened.metadata["size_bytes"], 10_000);
        assert_eq!(opened.metadata["path"], via_invoke["path"]);
    }

    #[test]
    fn open_stream_reassembles_correctly_when_a_multibyte_char_straddles_a_chunk_boundary() {
        // "a" (1 byte) + "😀" (4 bytes, U+1F600) + "b" (1 byte) = 6 bytes.
        // With max_len=2, the reader is forced to split mid-character:
        // byte 1 of "😀" lands in one read alongside "a", and the
        // remaining 3 bytes need another read (or two) before the
        // character is confirmed valid and emitted.
        let (dir, tool) = tool_with_fixture();
        let text = "a😀b";
        fs::write(dir.path().join("emoji.txt"), text).unwrap();

        let mut opened = tool.open_stream(json!({"path": "emoji.txt"})).unwrap();
        let bytes = drain(opened.reader.as_mut(), 2).unwrap();
        assert_eq!(String::from_utf8(bytes).unwrap(), text);
    }

    #[test]
    fn open_stream_rejects_invalid_utf8_discovered_only_at_eof() {
        let (dir, tool) = tool_with_fixture();
        // A valid ASCII prefix followed by a truncated multi-byte
        // sequence that never completes — only detectable once EOF
        // proves no more bytes are coming.
        fs::write(dir.path().join("truncated.bin"), [b'o', b'k', 0xe2, 0x82]).unwrap();

        let mut opened = tool.open_stream(json!({"path": "truncated.bin"})).unwrap();
        let err = drain(opened.reader.as_mut(), 4096).unwrap_err();
        assert!(matches!(err, ToolError::Failed(_)));
    }

    #[test]
    fn open_stream_rejects_genuinely_invalid_utf8_bytes() {
        let (dir, tool) = tool_with_fixture();
        fs::write(dir.path().join("binary.bin"), [0xff, 0xfe, 0x00, 0x01]).unwrap();

        let mut opened = tool.open_stream(json!({"path": "binary.bin"})).unwrap();
        let err = drain(opened.reader.as_mut(), 4096).unwrap_err();
        assert!(matches!(err, ToolError::Failed(_)));
    }

    #[test]
    fn open_stream_rejects_traversal_the_same_way_invoke_does() {
        let (_dir, tool) = tool_with_fixture();
        let err = tool
            .open_stream(json!({"path": "../../etc/passwd"}))
            .unwrap_err();
        assert!(matches!(
            err,
            ToolError::Unauthorized(_) | ToolError::Failed(_)
        ));
    }

    #[test]
    fn open_stream_accepts_a_file_past_invokes_smaller_cap() {
        // Exceeds MAX_READ_BYTES (invoke's 1 MiB cap) but is nowhere near
        // open_stream's own, much larger cap — proves the two caps are
        // genuinely independent, not the same limit under two names.
        let (dir, tool) = tool_with_fixture();
        let past_invoke_cap = vec![b'a'; (MAX_READ_BYTES + 1) as usize];
        fs::write(dir.path().join("big.txt"), &past_invoke_cap).unwrap();
        assert!(tool.invoke(json!({"path": "big.txt"})).is_err());
        assert!(tool.open_stream(json!({"path": "big.txt"})).is_ok());
    }

    #[test]
    fn open_stream_rejects_a_file_over_its_own_streaming_size_limit() {
        let (dir, tool) = tool_with_fixture();
        // A sparse file: `set_len` extends the apparent size without
        // writing real data, so this test doesn't actually allocate or
        // write hundreds of MiB to disk — only `fstat`'s reported size
        // is exercised, which is all `open_stream`'s cap check reads.
        let path = dir.path().join("huge.bin");
        let file = fs::File::create(&path).unwrap();
        file.set_len(MAX_STREAM_READ_BYTES + 1).unwrap();
        drop(file);

        let err = tool.open_stream(json!({"path": "huge.bin"})).unwrap_err();
        assert!(matches!(err, ToolError::Failed(_)));
    }
}
