use super::{CancelToken, IncrementalBytes, StreamingTool, StreamingToolOpen, Tool, ToolError};
use serde::Deserialize;
use serde_json::{json, Value};
use std::ffi::CString;
use std::fs::File;
use std::io::{self, Read};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::path::{Path, PathBuf};

/// Caps how much of a file `invoke` (the non-streaming path) reads into
/// memory at once, to bound server memory use regardless of the on-disk
/// file size.
const MAX_READ_BYTES: u64 = 1024 * 1024; // 1 MiB

/// Block size for the bounded read loop in `invoke`, so cancellation is
/// checked at least once per 64 KiB rather than only once per file.
const READ_BLOCK_BYTES: usize = 64 * 1024;

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
/// The base directory is opened once, at construction, as a file descriptor
/// that the struct keeps for its whole lifetime. Every later open is
/// relative to that descriptor (`openat`), one path component at a time,
/// with `O_NOFOLLOW` on every component. The client's path is never handed
/// to a path-based syscall, so there is no canonicalize-then-open window.
///
/// Request rules, enforced before any filesystem access:
///
/// - empty, absolute, NUL-containing, or any `..` component is refused;
/// - `.` and empty components (`a//b`) are ignored;
/// - any symlink in the requested path is refused, including one that
///   points inside the base directory (a deliberate, documented rule);
/// - FIFOs, sockets and devices are refused without blocking (`O_NONBLOCK`
///   on the final open, then a regular-file check on the descriptor).
///
/// ## Residual risks (not solved by this server)
///
/// - **Hardlinks.** A hardlink inside the tree to a file outside it is
///   indistinguishable from a regular file; the server cannot detect it.
///   Whoever can write into the tree already controls what it exposes.
/// - **Mounts.** A bind mount or other mount point inside the tree is
///   followed; `O_NOFOLLOW` only refuses symlinks, not mount crossings.
/// - **Same-UID processes.** A process running as the same user can read
///   the same files directly and can also talk to the socket. The security
///   boundary is the UID, not this tool.
pub struct FilesystemReadTool {
    /// Descriptor for the canonicalized base directory, opened once at
    /// construction. All request resolution starts from this descriptor.
    /// The base path itself is not stored: nothing reads it after
    /// construction, and keeping it would only be dead state.
    base_fd: OwnedFd,
}

impl FilesystemReadTool {
    /// Fails if `base_dir` does not exist, cannot be canonicalized, or is
    /// not a directory.
    pub fn new(base_dir: impl AsRef<Path>) -> io::Result<Self> {
        let base_dir = base_dir.as_ref().canonicalize()?;
        let base_file = File::open(&base_dir)?;
        if !base_file.metadata()?.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "base path is not a directory",
            ));
        }
        Ok(Self {
            base_fd: base_file.into(),
        })
    }

    /// Opens the requested path relative to the base descriptor, one
    /// component at a time, refusing symlinks and special files. Returns
    /// the open regular file, its path relative to the base directory, and
    /// its size from the descriptor's own metadata.
    fn open_validated(&self, args: Value) -> Result<(File, PathBuf, u64), ToolError> {
        let args: Args =
            serde_json::from_value(args).map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        let requested = args.path.as_str();

        let components = split_request(requested)?;
        let (last, dirs) = components
            .split_last()
            .ok_or_else(|| ToolError::Unauthorized("path must name a file".into()))?;

        let not_found = || {
            ToolError::Failed(format!(
                "cannot read {requested:?}: not found or inaccessible"
            ))
        };

        // Walk the intermediate directories. Each step opens a directory
        // with O_DIRECTORY|O_NOFOLLOW relative to the previous one, and the
        // previous descriptor is dropped as soon as the next one exists.
        let mut dir = self
            .base_fd
            .try_clone()
            .map_err(|e| ToolError::Failed(format!("descriptor clone failed: {e}")))?;
        for comp in dirs {
            let name = to_cstring(comp)?;
            dir = match openat_nofollow(
                dir.as_fd(),
                &name,
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            ) {
                Ok(fd) => fd,
                Err(e) => match e.raw_os_error() {
                    Some(libc::ELOOP) => return Err(symlink_refused()),
                    Some(libc::ENOTDIR) => {
                        // Either a symlink (some platforms report ENOTDIR for
                        // O_DIRECTORY|O_NOFOLLOW on one) or a plain file in a
                        // directory position. Re-open without O_DIRECTORY to
                        // tell them apart; a symlink reports ELOOP here.
                        // O_NONBLOCK keeps a FIFO from blocking this probe.
                        match openat_nofollow(
                            dir.as_fd(),
                            &name,
                            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK,
                        ) {
                            Err(e2) if e2.raw_os_error() == Some(libc::ELOOP) => {
                                return Err(symlink_refused());
                            }
                            _ => return Err(not_found()),
                        }
                    }
                    Some(libc::ENOENT) => return Err(not_found()),
                    Some(libc::EACCES) => {
                        return Err(ToolError::Failed("permission denied".into()))
                    }
                    _ => return Err(ToolError::Failed("open failed".into())),
                },
            };
        }

        // Final component: O_NONBLOCK so opening a FIFO, socket, or device
        // returns instead of blocking. The regular-file check below runs on
        // the descriptor, and for a regular file O_NONBLOCK has no effect on
        // reads, so it is left set.
        let name = to_cstring(last)?;
        let fd = match openat_nofollow(
            dir.as_fd(),
            &name,
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        ) {
            Ok(fd) => fd,
            Err(e) => {
                return Err(match e.raw_os_error() {
                    Some(libc::ELOOP) => symlink_refused(),
                    Some(libc::ENOENT) | Some(libc::ENOTDIR) => not_found(),
                    Some(libc::EACCES) => ToolError::Failed("permission denied".into()),
                    _ => ToolError::Failed("open failed".into()),
                })
            }
        };

        let file = File::from(fd);
        let metadata = file
            .metadata()
            .map_err(|_| ToolError::Failed("stat failed".into()))?;
        if !metadata.is_file() {
            return Err(ToolError::Failed("not a regular file".into()));
        }

        let relative = PathBuf::from(components.join("/"));
        Ok((file, relative, metadata.len()))
    }
}

/// Splits a client-supplied path into normal components. Refuses empty
/// paths, absolute paths, NUL bytes, and any `..`. Error messages contain
/// only the client's own text or a generic phrase, never host paths.
fn split_request(requested: &str) -> Result<Vec<&str>, ToolError> {
    if requested.is_empty() {
        return Err(ToolError::Unauthorized("path must not be empty".into()));
    }
    if requested.starts_with('/') {
        return Err(ToolError::Unauthorized(
            "absolute paths are not allowed".into(),
        ));
    }
    if requested.contains('\0') {
        return Err(ToolError::Unauthorized("path contains a NUL byte".into()));
    }
    let mut components = Vec::new();
    for comp in requested.split('/') {
        match comp {
            "" | "." => continue,
            ".." => {
                return Err(ToolError::Unauthorized(
                    "path traversal (`..`) is not allowed".into(),
                ))
            }
            other => components.push(other),
        }
    }
    if components.is_empty() {
        return Err(ToolError::Unauthorized("path must name a file".into()));
    }
    Ok(components)
}

fn to_cstring(component: &str) -> Result<std::ffi::CString, ToolError> {
    CString::new(component.as_bytes())
        .map_err(|_| ToolError::Unauthorized("path contains a NUL byte".into()))
}

fn symlink_refused() -> ToolError {
    ToolError::Unauthorized("symbolic links are not allowed".into())
}

/// Opens `name` relative to `dirfd` with the caller's flags. The only FFI
/// call in this module.
fn openat_nofollow(
    dirfd: BorrowedFd<'_>,
    name: &CString,
    flags: libc::c_int,
) -> io::Result<OwnedFd> {
    // SAFETY: `dirfd` is a live descriptor borrowed for the duration of this
    // call, so its raw value is valid. `name` is a NUL-terminated CString,
    // so its pointer is valid for reads up to its terminator. `openat` with
    // no O_CREAT takes no mode argument, so no variadic argument is read.
    // The return value is checked before use. A non-negative result is a
    // fresh descriptor owned by nothing else, and it is wrapped in OwnedFd
    // immediately below so it is closed exactly once.
    let fd = unsafe { libc::openat(dirfd.as_raw_fd(), name.as_ptr(), flags) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fd` was just returned by a successful `openat` and is not
    // owned by anything else, so ownership transfers to this OwnedFd once.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
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
        self.invoke_with_cancel(args, &CancelToken::new())
    }

    fn invoke_with_cancel(&self, args: Value, cancel: &CancelToken) -> Result<Value, ToolError> {
        let (file, relative, size) = self.open_validated(args)?;
        if size > MAX_READ_BYTES {
            return Err(ToolError::Failed(format!(
                "file exceeds the {MAX_READ_BYTES}-byte read limit"
            )));
        }

        let bytes = read_capped(file, MAX_READ_BYTES, cancel)?;
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

/// Reads at most `limit` bytes from `file` in `READ_BLOCK_BYTES` blocks,
/// checking `cancel` before each block.
///
/// The reader is wrapped in `take(limit + 1)`, so a file that grows after
/// its `fstat` still cannot push the read past `limit`. One byte over is
/// enough to prove the file grew, and that is reported as an error rather
/// than returned as truncated content.
fn read_capped(file: File, limit: u64, cancel: &CancelToken) -> Result<Vec<u8>, ToolError> {
    let mut reader = file.take(limit + 1);
    let mut out = Vec::new();
    let mut block = vec![0u8; READ_BLOCK_BYTES];
    loop {
        if cancel.is_cancelled() {
            return Err(ToolError::Failed("cancelled".into()));
        }
        let n = match reader.read(&mut block) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(ToolError::Failed(format!("read failed: {e}"))),
        };
        out.extend_from_slice(&block[..n]);
        if out.len() as u64 > limit {
            return Err(ToolError::Failed("file grew beyond the size limit".into()));
        }
    }
    Ok(out)
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
                // Real EOF. If `remaining` is still nonzero, the file
                // shrank on disk between the initial `fstat` and this
                // read — a genuine error, not a clean end of stream: the
                // caller was promised `remaining` more bytes and isn't
                // getting them.
                if self.remaining > 0 {
                    return Err(ToolError::Failed(
                        "file size changed during streaming (file is shorter than expected)".into(),
                    ));
                }
                // Any leftover here is a genuinely incomplete/invalid
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
    fn rejects_absolute_path_even_inside_base_dir() {
        // Behavior change: absolute paths are refused outright now, even
        // when they point inside the base directory. Previously accepted.
        let (dir, tool) = tool_with_fixture();
        let absolute = dir.path().canonicalize().unwrap().join("hello.txt");
        let err = tool
            .invoke(json!({"path": absolute.to_str().unwrap()}))
            .unwrap_err();
        assert!(matches!(err, ToolError::Unauthorized(_)));
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
    fn open_stream_reports_an_error_if_the_file_shrinks_during_streaming() {
        let (dir, tool) = tool_with_fixture();
        let path = dir.path().join("shrinking.txt");
        fs::write(&path, "x".repeat(1000)).unwrap();

        let mut opened = tool.open_stream(json!({"path": "shrinking.txt"})).unwrap();
        // Consume a bit, then shrink the file out from under the open
        // reader — simulating a concurrent truncation.
        let _ = opened.reader.next_chunk(100).unwrap();
        fs::write(&path, "x".repeat(10)).unwrap();

        let err = drain(opened.reader.as_mut(), 4096).unwrap_err();
        assert!(matches!(err, ToolError::Failed(_)));
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

    #[test]
    fn rejects_an_intermediate_symlink_directory() {
        // `d` points at another directory that is INSIDE the base. Still
        // refused: every symlink in the requested path is a rejection.
        let (dir, tool) = tool_with_fixture();
        symlink(dir.path().join("sub"), dir.path().join("d")).unwrap();

        let err = tool.invoke(json!({"path": "d/nested.txt"})).unwrap_err();
        assert!(matches!(err, ToolError::Unauthorized(_)));
    }

    #[test]
    fn rejects_dotdot_absolute_nul_and_empty_paths() {
        let (_dir, tool) = tool_with_fixture();
        for bad in [
            "",
            "/etc/hosts",
            "a/../hello.txt",
            "../hello.txt",
            "hello\0.txt",
        ] {
            let err = tool.invoke(json!({"path": bad})).unwrap_err();
            assert!(
                matches!(err, ToolError::Unauthorized(_)),
                "{bad:?} should be Unauthorized, got {err:?}"
            );
        }
    }

    #[test]
    fn current_and_empty_components_are_ignored() {
        let (_dir, tool) = tool_with_fixture();
        let result = tool.invoke(json!({"path": "./sub//nested.txt"})).unwrap();
        assert_eq!(result["contents"], "nested\n");
        assert_eq!(result["path"], "sub/nested.txt");
    }

    #[test]
    fn rejects_a_final_symlink_even_when_it_points_inside() {
        let (dir, tool) = tool_with_fixture();
        symlink(dir.path().join("hello.txt"), dir.path().join("alias.txt")).unwrap();

        let err = tool.invoke(json!({"path": "alias.txt"})).unwrap_err();
        assert!(matches!(err, ToolError::Unauthorized(_)));
    }

    #[test]
    fn a_fifo_in_the_tree_is_rejected_without_blocking() {
        // A FIFO with no writer would block a plain open forever. Run the
        // call on a worker thread and bound the wait, so a regression fails
        // this test instead of hanging the suite.
        use std::sync::mpsc;
        use std::time::{Duration, Instant};

        let (dir, tool) = tool_with_fixture();
        let fifo = dir.path().join("pipe");
        let status = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .expect("mkfifo binary must be available");
        assert!(status.success());

        let (tx, rx) = mpsc::channel();
        let started = Instant::now();
        std::thread::spawn(move || {
            let result = tool.invoke(json!({"path": "pipe"}));
            let _ = tx.send(result.map(|_| ()));
        });

        let outcome = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("invoke on a FIFO must return instead of blocking");
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(matches!(outcome, Err(ToolError::Failed(_))));
    }

    #[test]
    fn error_messages_do_not_contain_the_host_base_path() {
        let (dir, tool) = tool_with_fixture();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("s.txt"), "s").unwrap();
        symlink(outside.path().join("s.txt"), dir.path().join("link.txt")).unwrap();

        let base = dir.path().to_string_lossy().into_owned();
        let requests = [
            json!({"path": "../x"}),
            json!({"path": "link.txt"}),
            json!({"path": "nope.txt"}),
            json!({"path": "sub/missing.txt"}),
        ];
        for req in requests {
            let err = tool.invoke(req.clone()).unwrap_err();
            let text = err.to_string();
            assert!(!text.contains(&base), "{req} leaked the base path: {text}");
        }
    }

    #[test]
    fn cancelled_token_stops_the_read() {
        let (dir, tool) = tool_with_fixture();
        fs::write(dir.path().join("big.txt"), vec![b'a'; 1024 * 1024]).unwrap();

        let token = CancelToken::new();
        token.cancel();
        let err = tool
            .invoke_with_cancel(json!({"path": "big.txt"}), &token)
            .unwrap_err();
        assert!(matches!(err, ToolError::Failed(ref m) if m == "cancelled"));
    }

    /// Race test. It can only FAIL if the defect exists: a secret outside
    /// the base could appear in a successful result only if an intermediate
    /// directory were followed out of the tree. With the correct
    /// implementation no interleaving can produce that, so it never fails
    /// spuriously. The test is probabilistic only in the sense that the
    /// swapping may or may not land inside a given `openat` window, so a
    /// pass does not prove absence; it bounds the search. Errors from the
    /// tool are expected and fine. It takes about 1.5 s and always ends.
    #[test]
    fn swapping_an_intermediate_directory_never_leaks_outside_content() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::{Duration, Instant};

        let root = tempfile::tempdir().unwrap();
        let base = root.path().join("base");
        let d = base.join("d");
        fs::create_dir_all(&d).unwrap();
        fs::write(d.join("f.txt"), "inside").unwrap();
        let outside = root.path().join("outside");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("f.txt"), "SECRET-OUTSIDE").unwrap();
        let tool = FilesystemReadTool::new(&base).unwrap();

        let stop = AtomicBool::new(false);
        std::thread::scope(|s| {
            s.spawn(|| {
                let mut flip = false;
                while !stop.load(Ordering::Relaxed) {
                    // `remove_dir_all` unlinks a symlink without following it.
                    let _ = fs::remove_dir_all(&d);
                    if flip {
                        let _ = symlink(&outside, &d);
                    } else {
                        let _ = fs::create_dir(&d);
                        // Write only into a REAL directory, never through a
                        // symlink, so the outside secret is never overwritten.
                        if fs::symlink_metadata(&d)
                            .map(|m| m.is_dir())
                            .unwrap_or(false)
                        {
                            let _ = fs::write(d.join("f.txt"), "inside");
                        }
                    }
                    flip = !flip;
                }
            });

            let deadline = Instant::now() + Duration::from_millis(1500);
            while Instant::now() < deadline {
                if let Ok(value) = tool.invoke(json!({"path": "d/f.txt"})) {
                    let contents = value["contents"].as_str().unwrap_or_default();
                    assert!(
                        !contents.contains("SECRET-OUTSIDE"),
                        "read escaped the base directory"
                    );
                }
            }
            stop.store(true, Ordering::Relaxed);
        });
    }
}
