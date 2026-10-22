//! Reusable AST client SDK — the one place connection setup, framing, and
//! request/response correlation are implemented on the client side.
//! `ast-client` (the CLI) is a thin wrapper around this, not a second
//! protocol stack; anything embedding AST in a larger Rust program should
//! use this directly instead of shelling out to the CLI.
//!
//! Supports persistent connections with any number of concurrent
//! in-flight requests: each call gets its own `correlation_id` and awaits
//! its own response via a one-shot channel, so unrelated calls never
//! block each other and responses may arrive out of order relative to
//! when their requests were sent.

use crate::ast::codec;
use crate::ast::message::*;
use crate::ast::wire::{self, Frame, FrameLimits};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::BufReader;
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

#[derive(Debug, thiserror::Error)]
pub enum AstClientError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("codec error: {0}")]
    Codec(#[from] codec::CodecError),
    #[error("framing error: {0}")]
    Framing(#[from] wire::FrameError),
    #[error("the connection's reader/writer task is gone")]
    Closed,
    #[error("no response within the configured timeout")]
    Timeout,
    #[error("handshake rejected: {0}")]
    HandshakeRejected(String),
    #[error("server protocol version {server}, client supports {client}")]
    VersionMismatch { server: u32, client: u32 },
    #[error("server error {code:?}: {message}")]
    Server { code: ErrorCode, message: String },
    #[error("unexpected response message type: {0}")]
    UnexpectedResponseType(u8),
    #[error(
        "streamed result of {declared_or_received} bytes exceeds this client's configured max_stream_bytes ({limit})"
    )]
    StreamTooLarge {
        declared_or_received: u64,
        limit: usize,
    },
    #[error("genuinely-streamed content failed client-side validation: {0}")]
    InvalidStreamContent(String),
    #[error("stream protocol violation: {0}")]
    StreamProtocolViolation(String),
}

type Waiters = Arc<Mutex<HashMap<u64, oneshot::Sender<Frame>>>>;
/// Registered only for a correlation_id started via
/// [`AstClient::invoke_stream`] — an mpsc channel the reader task
/// forwards every frame bearing that id into (an `Error` if the call
/// failed before streaming began, or `StreamStart`/`StreamChunk*`/
/// `StreamEnd` if it didn't), instead of the one-shot `waiters` every
/// other request uses. Removed once a terminal frame (`Error` or
/// `StreamEnd`) for that id has been forwarded.
type StreamWaiters = Arc<Mutex<HashMap<u64, mpsc::Sender<Frame>>>>;

/// Sends a CANCEL for `target_correlation_id` without awaiting. Free
/// function so [`StreamHandle`] can use it without a `&AstClient`.
fn send_cancel_best_effort(
    tx: &mpsc::Sender<Frame>,
    next_id: &AtomicU64,
    target_correlation_id: u64,
) {
    let Ok(payload) = codec::encode(&Cancel {
        target_correlation_id,
    }) else {
        return;
    };
    let frame = Frame {
        msg_type: MessageType::Cancel as u8,
        correlation_id: next_id.fetch_add(1, Ordering::Relaxed),
        payload,
    };
    let _ = tx.try_send(frame);
}

fn decode_server_error(payload: &[u8]) -> Result<AstClientError, AstClientError> {
    let err: ErrorPayload = codec::decode(payload)?;
    Ok(AstClientError::Server {
        code: err.code,
        message: err.message,
    })
}

pub struct AstClient {
    tx: mpsc::Sender<Frame>,
    waiters: Waiters,
    stream_waiters: StreamWaiters,
    next_id: Arc<AtomicU64>,
    default_timeout: Duration,
    max_stream_bytes: usize,
    writer_task: JoinHandle<()>,
    reader_task: JoinHandle<()>,
}

pub struct AstClientConfig {
    pub client_name: String,
    pub frame_limits: FrameLimits,
    pub request_timeout: Duration,
    /// Hard ceiling on a streamed `INVOKE`'s total size, checked against
    /// `StreamStart.total_bytes` *before* this client preallocates any
    /// buffer sized to it — that field is a server-supplied,
    /// untrusted number; without this ceiling, a malicious or buggy
    /// server could declare an arbitrarily large value and force a huge
    /// allocation before a single byte of real data arrives. Also
    /// enforced against the *actual* cumulative bytes received (not just
    /// the declared total), since nothing requires a server's chunks to
    /// match what it declared. Default: 256 MiB.
    pub max_stream_bytes: usize,
}

impl Default for AstClientConfig {
    fn default() -> Self {
        Self {
            client_name: "ast-client-sdk".to_string(),
            frame_limits: FrameLimits::default(),
            request_timeout: Duration::from_secs(10),
            max_stream_bytes: 256 * 1024 * 1024,
        }
    }
}

/// Capacity of the buffered read half: one `read(2)` can pull several small
/// frames instead of issuing separate 1-, 3- and N-byte reads per frame.
const READ_BUF_CAPACITY: usize = 8 * 1024;

impl AstClient {
    pub async fn connect(path: &Path, config: AstClientConfig) -> Result<Self, AstClientError> {
        let stream = UnixStream::connect(path).await?;
        let (read_half, write_half) = stream.into_split();
        let mut read_half = BufReader::with_capacity(READ_BUF_CAPACITY, read_half);
        let (tx, mut rx) = mpsc::channel::<Frame>(64);
        let waiters: Waiters = Arc::new(Mutex::new(HashMap::new()));
        let stream_waiters: StreamWaiters = Arc::new(Mutex::new(HashMap::new()));

        let writer_task = tokio::spawn(async move {
            let mut write_half = write_half;
            while let Some(frame) = rx.recv().await {
                if wire::write_frame(&mut write_half, &frame).await.is_err() {
                    return;
                }
            }
        });

        let reader_waiters = Arc::clone(&waiters);
        let reader_stream_waiters = Arc::clone(&stream_waiters);
        let frame_limits = config.frame_limits;
        let reader_task = tokio::spawn(async move {
            loop {
                match wire::read_frame(&mut read_half, frame_limits).await {
                    Ok(Some(frame)) => {
                        let is_terminal_for_stream = matches!(
                            MessageType::from_u8(frame.msg_type),
                            Some(MessageType::Error) | Some(MessageType::StreamEnd)
                        );
                        let stream_sender = if is_terminal_for_stream {
                            reader_stream_waiters
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .remove(&frame.correlation_id)
                        } else {
                            reader_stream_waiters
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .get(&frame.correlation_id)
                                .cloned()
                        };
                        if let Some(sender) = stream_sender {
                            let _ = sender.send(frame).await;
                            continue;
                        }

                        let waiter = reader_waiters
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .remove(&frame.correlation_id);
                        if let Some(waiter) = waiter {
                            let _ = waiter.send(frame);
                        }
                        // A frame with no matching waiter (e.g. arriving
                        // after this client already gave up waiting on
                        // timeout) is dropped, not an error.
                    }
                    Ok(None) | Err(_) => return, // connection closed or framing error: nothing more to read
                }
            }
        });

        let client = Self {
            tx,
            waiters,
            stream_waiters,
            next_id: Arc::new(AtomicU64::new(1)),
            default_timeout: config.request_timeout,
            max_stream_bytes: config.max_stream_bytes,
            writer_task,
            reader_task,
        };

        client.do_hello(&config.client_name).await?;
        Ok(client)
    }

    fn next_correlation_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn request<T: serde::Serialize>(
        &self,
        msg_type: MessageType,
        payload: &T,
        timeout: Duration,
    ) -> Result<Frame, AstClientError> {
        let id = self.next_correlation_id();
        let (otx, orx) = oneshot::channel();
        self.waiters
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(id, otx);

        let bytes = codec::encode(payload)?;
        let frame = Frame {
            msg_type: msg_type as u8,
            correlation_id: id,
            payload: bytes,
        };
        if self.tx.send(frame).await.is_err() {
            self.waiters
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&id);
            return Err(AstClientError::Closed);
        }

        match tokio::time::timeout(timeout, orx).await {
            Ok(Ok(frame)) => Ok(frame),
            Ok(Err(_)) => Err(AstClientError::Closed),
            Err(_elapsed) => {
                self.waiters
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .remove(&id);
                self.send_best_effort_cancel(id);
                Err(AstClientError::Timeout)
            }
        }
    }

    /// Tells the server to stop working on a request this client has
    /// given up on. Non-blocking and best-effort by design: it never
    /// awaits the writer task, and a full or closed outgoing channel just
    /// means the server keeps running the request, which is no worse than
    /// before this existed.
    fn send_best_effort_cancel(&self, target_correlation_id: u64) {
        send_cancel_best_effort(&self.tx, &self.next_id, target_correlation_id);
    }

    /// Fire-and-forget: AST's `CANCEL` has no response, by design (see
    /// `docs/AST_PROTOCOL.md`'s "Cancellation" section), so this returns
    /// as soon as the message is queued for the writer task, not when
    /// the server has acted on it — there is nothing to await.
    pub async fn cancel(&self, target_correlation_id: u64) -> Result<(), AstClientError> {
        let bytes = codec::encode(&Cancel {
            target_correlation_id,
        })?;
        let frame = Frame {
            msg_type: MessageType::Cancel as u8,
            correlation_id: self.next_correlation_id(),
            payload: bytes,
        };
        self.tx
            .send(frame)
            .await
            .map_err(|_| AstClientError::Closed)
    }

    async fn do_hello(&self, client_name: &str) -> Result<(), AstClientError> {
        let hello = Hello {
            protocol_version: AST_PROTOCOL_VERSION,
            client_name: client_name.to_string(),
        };
        let frame = self
            .request(MessageType::Hello, &hello, self.default_timeout)
            .await?;
        if MessageType::from_u8(frame.msg_type) != Some(MessageType::HelloAck) {
            return Err(AstClientError::UnexpectedResponseType(frame.msg_type));
        }
        let ack: HelloAck = codec::decode(&frame.payload)?;
        if !ack.ok {
            return Err(AstClientError::HandshakeRejected(
                ack.error
                    .unwrap_or_else(|| "rejected with no reason given".to_string()),
            ));
        }
        if ack.protocol_version != AST_PROTOCOL_VERSION {
            return Err(AstClientError::VersionMismatch {
                server: ack.protocol_version,
                client: AST_PROTOCOL_VERSION,
            });
        }
        Ok(())
    }

    pub async fn ping(&self) -> Result<Duration, AstClientError> {
        let started = std::time::Instant::now();
        let frame = self
            .request(MessageType::Ping, &Ping {}, self.default_timeout)
            .await?;
        if MessageType::from_u8(frame.msg_type) != Some(MessageType::Pong) {
            return Err(AstClientError::UnexpectedResponseType(frame.msg_type));
        }
        Ok(started.elapsed())
    }

    pub async fn discover(&self) -> Result<Vec<ToolDescriptorWire>, AstClientError> {
        let frame = self
            .request(MessageType::Discover, &Discover {}, self.default_timeout)
            .await?;
        self.parse_discover_result(frame)
    }

    fn parse_discover_result(
        &self,
        frame: Frame,
    ) -> Result<Vec<ToolDescriptorWire>, AstClientError> {
        match MessageType::from_u8(frame.msg_type) {
            Some(MessageType::DiscoverResult) => {
                let result: DiscoverResult = codec::decode(&frame.payload)?;
                Ok(result.tools)
            }
            Some(MessageType::Error) => Err(decode_server_error(&frame.payload)?),
            _ => Err(AstClientError::UnexpectedResponseType(frame.msg_type)),
        }
    }

    pub async fn invoke(
        &self,
        tool: &str,
        args: Value,
        deadline: Option<Duration>,
    ) -> Result<Value, AstClientError> {
        let invoke = Invoke {
            tool: tool.to_string(),
            args,
            deadline_ms: deadline.map(|d| d.as_millis() as u64),
            stream: None,
        };
        // Give the client-side wait a little longer than the deadline we
        // told the server to enforce, so a `DEADLINE_EXCEEDED` error (sent
        // by the server) has a chance to arrive before this client gives
        // up on its own — a client timeout and a server deadline racing
        // each other would otherwise non-deterministically decide which
        // error the caller sees.
        let wait = deadline.unwrap_or(self.default_timeout) + Duration::from_secs(1);
        let frame = self.request(MessageType::Invoke, &invoke, wait).await?;
        match MessageType::from_u8(frame.msg_type) {
            Some(MessageType::Result) => {
                let result: InvokeResult = codec::decode(&frame.payload)?;
                Ok(result.value)
            }
            Some(MessageType::Error) => Err(decode_server_error(&frame.payload)?),
            _ => Err(AstClientError::UnexpectedResponseType(frame.msg_type)),
        }
    }

    /// Like [`Self::invoke`], but asks the server to deliver the result
    /// as `STREAM_START`/`STREAM_CHUNK*`/`STREAM_END` and reassembles it
    /// here — the caller gets back exactly the same `Value` either way.
    /// See `docs/AST_PROTOCOL.md`'s "Chunked streaming" section for why
    /// this exists: bounding the server's and this client's own peak
    /// memory for a large result to roughly one chunk at a time, instead
    /// of one whole encoded result, with real backpressure (both sides'
    /// bounded channels) rather than an unbounded buffer on either end.
    pub async fn invoke_stream(
        &self,
        tool: &str,
        args: Value,
        deadline: Option<Duration>,
    ) -> Result<Value, AstClientError> {
        let mut handle = self.invoke_stream_chunks(tool, args, deadline).await?;
        // `invoke_stream_chunks` already rejected a declared total over
        // `max_stream_bytes`, so this preallocation is bounded.
        let mut encoded = Vec::with_capacity(handle.start.total_bytes as usize);
        while let Some(chunk) = handle.next_chunk().await? {
            encoded.extend_from_slice(&chunk);
        }
        match handle.start.raw.take() {
            None => Ok(codec::decode(&encoded)?),
            Some(raw) => {
                // Genuinely-streamed mode: `encoded` is the raw field's own
                // bytes (UTF-8 text, for every current streaming tool), not
                // a slice of an encoded `Value` — reassemble by inserting it
                // into `raw.metadata` under `raw.field_name`, rather than
                // msgpack-decoding it.
                let text = String::from_utf8(encoded).map_err(|_| {
                    AstClientError::InvalidStreamContent(
                        "streamed bytes were not valid UTF-8".to_string(),
                    )
                })?;
                let mut value = raw.metadata;
                match value.as_object_mut() {
                    Some(map) => {
                        map.insert(raw.field_name, Value::String(text));
                        Ok(value)
                    }
                    None => Err(AstClientError::InvalidStreamContent(
                        "StreamStart.raw.metadata was not a JSON object".to_string(),
                    )),
                }
            }
        }
    }

    /// Starts a streamed `INVOKE` and returns a [`StreamHandle`] that yields
    /// each `STREAM_CHUNK` as it arrives, without aggregating them. Waits
    /// only for the first frame here: `STREAM_START` yields the handle, an
    /// `ERROR` becomes [`AstClientError::Server`]. `deadline` bounds the
    /// whole stream, as in [`AstClient::invoke_stream`].
    pub async fn invoke_stream_chunks(
        &self,
        tool: &str,
        args: Value,
        deadline: Option<Duration>,
    ) -> Result<StreamHandle, AstClientError> {
        let invoke = Invoke {
            tool: tool.to_string(),
            args,
            deadline_ms: deadline.map(|d| d.as_millis() as u64),
            stream: Some(true),
        };

        let id = self.next_correlation_id();
        let (stx, mut rx) = mpsc::channel::<Frame>(8);
        self.stream_waiters
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(id, stx);

        let wait = deadline.unwrap_or(self.default_timeout) + Duration::from_secs(1);
        let deadline_at = tokio::time::Instant::now() + wait;

        let sent: Result<(), AstClientError> = match codec::encode(&invoke) {
            Ok(payload) => {
                let frame = Frame {
                    msg_type: MessageType::Invoke as u8,
                    correlation_id: id,
                    payload,
                };
                self.tx
                    .send(frame)
                    .await
                    .map_err(|_| AstClientError::Closed)
            }
            Err(err) => Err(err.into()),
        };
        if let Err(err) = sent {
            self.abandon_stream(id, false);
            return Err(err);
        }

        // From here on, every early return must leave no stream_waiters
        // entry behind: `abandon_stream` does that, and sends CANCEL when
        // the server may still be streaming.
        let first = match tokio::time::timeout_at(deadline_at, rx.recv()).await {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                self.abandon_stream(id, false);
                return Err(AstClientError::Closed);
            }
            Err(_elapsed) => {
                self.abandon_stream(id, true);
                return Err(AstClientError::Timeout);
            }
        };
        match MessageType::from_u8(first.msg_type) {
            Some(MessageType::StreamStart) => {}
            Some(MessageType::Error) => {
                // The reader already dropped this id's entry on ERROR; the
                // server has nothing more to send, so no CANCEL.
                self.abandon_stream(id, false);
                return Err(decode_server_error(&first.payload)?);
            }
            _ => {
                self.abandon_stream(id, true);
                return Err(AstClientError::UnexpectedResponseType(first.msg_type));
            }
        }
        let start: StreamStart = match codec::decode(&first.payload) {
            Ok(start) => start,
            Err(err) => {
                self.abandon_stream(id, true);
                return Err(err.into());
            }
        };
        // `total_bytes` is server-supplied and untrusted — reject
        // outright, before allocating anything sized to it.
        if start.total_bytes > self.max_stream_bytes as u64 {
            self.abandon_stream(id, true);
            return Err(AstClientError::StreamTooLarge {
                declared_or_received: start.total_bytes,
                limit: self.max_stream_bytes,
            });
        }

        Ok(StreamHandle {
            start,
            rx,
            stream_waiters: Arc::clone(&self.stream_waiters),
            tx: self.tx.clone(),
            next_id: Arc::clone(&self.next_id),
            correlation_id: id,
            max_stream_bytes: self.max_stream_bytes,
            deadline: deadline_at,
            received_bytes: 0,
            next_sequence: 0,
            server_finished: false,
            completed: false,
            failed: false,
        })
    }

    /// Forgets a stream's `stream_waiters` entry and, if `cancel`, tells
    /// the server to stop. Used on every path that never produced (or is
    /// abandoning) a [`StreamHandle`].
    fn abandon_stream(&self, correlation_id: u64, cancel: bool) {
        self.stream_waiters
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&correlation_id);
        if cancel {
            self.send_best_effort_cancel(correlation_id);
        }
    }

    /// Runs `calls` as a single `BATCH`. The server executes them
    /// sequentially, in order (see `Batch`'s doc comment in
    /// `src/ast/message.rs` for exactly why no concurrency), so this
    /// waits roughly as long as the sum of each call's own work —
    /// `deadline` bounds the whole batch's wait, not any individual
    /// call's (each entry still carries its own `deadline_ms`).
    pub async fn batch(
        &self,
        calls: Vec<BatchInvoke>,
        deadline: Option<Duration>,
    ) -> Result<Vec<BatchResultEntry>, AstClientError> {
        let batch = Batch { calls };
        let wait = deadline.unwrap_or(self.default_timeout) + Duration::from_secs(1);
        let frame = self.request(MessageType::Batch, &batch, wait).await?;
        match MessageType::from_u8(frame.msg_type) {
            Some(MessageType::BatchResult) => {
                let result: BatchResult = codec::decode(&frame.payload)?;
                Ok(result.results)
            }
            Some(MessageType::Error) => Err(decode_server_error(&frame.payload)?),
            _ => Err(AstClientError::UnexpectedResponseType(frame.msg_type)),
        }
    }

    /// Closes the connection and waits for the background reader/writer
    /// tasks to finish. Dropping an `AstClient` without calling this also
    /// shuts the connection down (the writer task ends once `tx` is
    /// dropped), just without waiting for it.
    ///
    /// Waits for every outstanding `StreamHandle` to be dropped: a task that
    /// holds a live handle and awaits `close()` will wait forever. Drop or
    /// finish the handle first.
    pub async fn close(self) {
        drop(self.tx);
        let _ = self.writer_task.await;
        self.reader_task.abort();
    }
}

/// One streamed `INVOKE`, consumed chunk by chunk (see
/// [`AstClient::invoke_stream_chunks`]). Every chunk is checked as it
/// arrives: its `sequence` must be the next expected value, the running
/// byte total must stay within `max_stream_bytes`, and `STREAM_END` must
/// arrive exactly when the bytes received equal `StreamStart.total_bytes`.
///
/// Errors are sticky: after `next_chunk` returns `Err`, later calls return
/// `Err(AstClientError::Closed)` — drop the handle instead.
///
/// Dropping a handle before its stream has ended deregisters it on the
/// client and sends a best-effort CANCEL, so the connection stays usable.
/// Note that the client's reader task forwards this stream's frames
/// through a small bounded channel: a handle that is held but not read
/// stalls the reader for the whole connection until it is read or dropped.
pub struct StreamHandle {
    start: StreamStart,
    rx: mpsc::Receiver<Frame>,
    stream_waiters: StreamWaiters,
    tx: mpsc::Sender<Frame>,
    next_id: Arc<AtomicU64>,
    correlation_id: u64,
    max_stream_bytes: usize,
    deadline: tokio::time::Instant,
    received_bytes: u64,
    next_sequence: u32,
    /// The server will send nothing more for this id (`STREAM_END` or
    /// `ERROR` arrived, or the reader is gone), so there is nothing to cancel.
    server_finished: bool,
    /// `STREAM_END` arrived and validated.
    completed: bool,
    /// A previous `next_chunk` call returned an error.
    failed: bool,
}

impl StreamHandle {
    /// The `STREAM_START` this stream opened with, including `raw` metadata
    /// for genuinely-incremental streams.
    pub fn start(&self) -> &StreamStart {
        &self.start
    }

    /// The next chunk's bytes, exactly as the server sent them. `Ok(None)`
    /// once `STREAM_END` has been received and validated; `Ok(None)` keeps
    /// being returned after that.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, AstClientError> {
        if self.completed {
            return Ok(None);
        }
        if self.failed {
            return Err(AstClientError::Closed);
        }
        let outcome = self.read_next_chunk().await;
        if outcome.is_err() {
            self.failed = true;
        }
        outcome
    }

    async fn read_next_chunk(&mut self) -> Result<Option<Vec<u8>>, AstClientError> {
        let frame = match tokio::time::timeout_at(self.deadline, self.rx.recv()).await {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                self.server_finished = true;
                return Err(AstClientError::Closed);
            }
            Err(_elapsed) => return Err(AstClientError::Timeout),
        };
        match MessageType::from_u8(frame.msg_type) {
            Some(MessageType::StreamChunk) => {
                let chunk: StreamChunk = codec::decode(&frame.payload)?;
                if chunk.sequence != self.next_sequence {
                    return Err(AstClientError::StreamProtocolViolation(format!(
                        "STREAM_CHUNK sequence {} arrived where {} was expected",
                        chunk.sequence, self.next_sequence
                    )));
                }
                self.next_sequence += 1;
                // Nothing requires a server's chunks to add up to what
                // `StreamStart` declared, so bound the actual bytes too.
                let received = self.received_bytes + chunk.data.len() as u64;
                if received > self.max_stream_bytes as u64 {
                    return Err(AstClientError::StreamTooLarge {
                        declared_or_received: received,
                        limit: self.max_stream_bytes,
                    });
                }
                self.received_bytes = received;
                Ok(Some(chunk.data))
            }
            Some(MessageType::StreamEnd) => {
                self.server_finished = true;
                if self.received_bytes != self.start.total_bytes {
                    return Err(AstClientError::StreamProtocolViolation(format!(
                        "STREAM_END after {} bytes, but STREAM_START declared total_bytes {}",
                        self.received_bytes, self.start.total_bytes
                    )));
                }
                self.completed = true;
                Ok(None)
            }
            Some(MessageType::Error) => {
                self.server_finished = true;
                Err(decode_server_error(&frame.payload)?)
            }
            _ => Err(AstClientError::UnexpectedResponseType(frame.msg_type)),
        }
    }
}

impl Drop for StreamHandle {
    fn drop(&mut self) {
        self.stream_waiters
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.correlation_id);
        if !self.server_finished {
            send_cancel_best_effort(&self.tx, &self.next_id, self.correlation_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::connection::{run_connection, ConnectionConfig};
    use crate::tools::{FilesystemReadTool, SystemInfoTool, ToolRegistry};

    async fn start_test_server() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("ast-client-test.sock");
        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();

        let mut registry = ToolRegistry::new();
        registry.register(Box::new(SystemInfoTool));
        let registry = Arc::new(registry);

        tokio::spawn(async move {
            loop {
                let Ok((stream, _addr)) = listener.accept().await else {
                    return;
                };
                let registry = Arc::clone(&registry);
                tokio::spawn(run_connection(
                    stream,
                    registry,
                    ConnectionConfig::default(),
                ));
            }
        });

        (dir, socket_path)
    }

    async fn start_test_server_with_filesystem_read(
        read_dir: &std::path::Path,
    ) -> std::path::PathBuf {
        start_test_server_with_filesystem_read_and_chunk_bytes(
            read_dir,
            ConnectionConfig::default().stream_chunk_bytes,
        )
        .await
    }

    /// Like [`start_test_server_with_filesystem_read`], but with an explicit
    /// `stream_chunk_bytes`, so a test can force many small chunks.
    async fn start_test_server_with_filesystem_read_and_chunk_bytes(
        read_dir: &std::path::Path,
        stream_chunk_bytes: usize,
    ) -> std::path::PathBuf {
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("ast-client-fs-test.sock");
        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();

        let mut registry = ToolRegistry::new();
        registry.register(Box::new(FilesystemReadTool::new(read_dir).unwrap()));
        let registry = Arc::new(registry);

        tokio::spawn(async move {
            loop {
                let Ok((stream, _addr)) = listener.accept().await else {
                    return;
                };
                let registry = Arc::clone(&registry);
                tokio::spawn(run_connection(
                    stream,
                    registry,
                    ConnectionConfig {
                        stream_chunk_bytes,
                        ..Default::default()
                    },
                ));
            }
        });
        // Leaked deliberately: the socket directory must outlive this
        // function's return for the server task (spawned above, still
        // running) to keep serving it — this is test-only code, not a
        // pattern used anywhere in the shipped server/client.
        std::mem::forget(socket_dir);
        socket_path
    }

    #[tokio::test]
    async fn invoke_stream_against_a_genuinely_streaming_tool_matches_invoke() {
        let fixture_dir = tempfile::tempdir().unwrap();
        let contents = "genuine streaming via AstClient::invoke_stream\n".repeat(200);
        std::fs::write(fixture_dir.path().join("big.txt"), &contents).unwrap();
        let socket_path = start_test_server_with_filesystem_read(fixture_dir.path()).await;

        let client = AstClient::connect(&socket_path, AstClientConfig::default())
            .await
            .unwrap();

        let plain = client
            .invoke(
                "filesystem.read",
                serde_json::json!({"path": "big.txt"}),
                None,
            )
            .await
            .unwrap();
        let streamed = client
            .invoke_stream(
                "filesystem.read",
                serde_json::json!({"path": "big.txt"}),
                None,
            )
            .await
            .unwrap();

        assert_eq!(plain, streamed);
        assert_eq!(streamed["contents"], contents);

        client.close().await;
    }

    #[tokio::test]
    async fn connect_performs_hello_handshake() {
        let (_dir, path) = start_test_server().await;
        let client = AstClient::connect(&path, AstClientConfig::default())
            .await
            .unwrap();
        client.close().await;
    }

    #[tokio::test]
    async fn ping_round_trips() {
        let (_dir, path) = start_test_server().await;
        let client = AstClient::connect(&path, AstClientConfig::default())
            .await
            .unwrap();
        let rtt = client.ping().await.unwrap();
        assert!(rtt < Duration::from_secs(1));
        client.close().await;
    }

    #[tokio::test]
    async fn discover_lists_the_registered_tool() {
        let (_dir, path) = start_test_server().await;
        let client = AstClient::connect(&path, AstClientConfig::default())
            .await
            .unwrap();
        let tools = client.discover().await.unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "system.info");
        client.close().await;
    }

    #[tokio::test]
    async fn invoke_runs_the_tool() {
        let (_dir, path) = start_test_server().await;
        let client = AstClient::connect(&path, AstClientConfig::default())
            .await
            .unwrap();
        let value = client
            .invoke("system.info", serde_json::json!({}), None)
            .await
            .unwrap();
        assert_eq!(value["os"], std::env::consts::OS);
        client.close().await;
    }

    #[tokio::test]
    async fn invoke_unknown_tool_is_a_server_error() {
        let (_dir, path) = start_test_server().await;
        let client = AstClient::connect(&path, AstClientConfig::default())
            .await
            .unwrap();
        let err = client
            .invoke("does.not.exist", serde_json::json!({}), None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            AstClientError::Server {
                code: ErrorCode::ToolNotFound,
                ..
            }
        ));
        client.close().await;
    }

    #[tokio::test]
    async fn concurrent_invokes_on_one_connection_all_complete() {
        let (_dir, path) = start_test_server().await;
        let client = Arc::new(
            AstClient::connect(&path, AstClientConfig::default())
                .await
                .unwrap(),
        );

        let mut handles = Vec::new();
        for _ in 0..10 {
            let client = Arc::clone(&client);
            handles.push(tokio::spawn(async move {
                client
                    .invoke("system.info", serde_json::json!({}), None)
                    .await
            }));
        }
        for handle in handles {
            let value = handle.await.unwrap().unwrap();
            assert_eq!(value["os"], std::env::consts::OS);
        }
    }

    #[tokio::test]
    async fn invoke_stream_reassembles_to_the_same_value_invoke_would_return() {
        let (_dir, path) = start_test_server().await;
        let client = AstClient::connect(&path, AstClientConfig::default())
            .await
            .unwrap();

        let plain = client
            .invoke("system.info", serde_json::json!({}), None)
            .await
            .unwrap();
        let streamed = client
            .invoke_stream("system.info", serde_json::json!({}), None)
            .await
            .unwrap();
        assert_eq!(plain, streamed);

        client.close().await;
    }

    #[tokio::test]
    async fn invoke_stream_of_an_unknown_tool_reports_the_same_error_invoke_would() {
        let (_dir, path) = start_test_server().await;
        let client = AstClient::connect(&path, AstClientConfig::default())
            .await
            .unwrap();

        let err = client
            .invoke_stream("does.not.exist", serde_json::json!({}), None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            AstClientError::Server {
                code: ErrorCode::ToolNotFound,
                ..
            }
        ));

        client.close().await;
    }

    /// A fake AST server that performs a real HELLO handshake and then
    /// sends whatever raw frames the test gives it for the next INVOKE —
    /// used to simulate a malicious or buggy peer that lies about stream
    /// sizes, something the real `ast-server` would never do but that a
    /// defensive client still has to handle.
    async fn start_lying_stream_server(
        reply_frames: Vec<(MessageType, Vec<u8>)>,
    ) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("lying-server.sock");
        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();

        tokio::spawn(async move {
            let (mut stream, _addr) = listener.accept().await.unwrap();

            let hello_frame = wire::read_frame(&mut stream, FrameLimits::default())
                .await
                .unwrap()
                .unwrap();
            let ack = HelloAck {
                protocol_version: AST_PROTOCOL_VERSION,
                server_name: "lying-test-server".to_string(),
                ok: true,
                error: None,
            };
            wire::write_frame(
                &mut stream,
                &Frame {
                    msg_type: MessageType::HelloAck as u8,
                    correlation_id: hello_frame.correlation_id,
                    payload: codec::encode(&ack).unwrap(),
                },
            )
            .await
            .unwrap();

            let invoke_frame = wire::read_frame(&mut stream, FrameLimits::default())
                .await
                .unwrap()
                .unwrap();
            for (msg_type, payload) in reply_frames {
                wire::write_frame(
                    &mut stream,
                    &Frame {
                        msg_type: msg_type as u8,
                        correlation_id: invoke_frame.correlation_id,
                        payload,
                    },
                )
                .await
                .unwrap();
            }

            // Keep the connection open until the client is done with it.
            let mut buf = [0u8; 1];
            let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await;
        });

        (dir, socket_path)
    }

    #[tokio::test]
    async fn invoke_stream_rejects_a_server_declared_total_bytes_over_the_configured_limit() {
        let (_dir, path) = start_lying_stream_server(vec![(
            MessageType::StreamStart,
            codec::encode(&StreamStart {
                total_chunks: 1,
                total_bytes: 10 * 1024 * 1024 * 1024, // 10 GiB — absurd for a test payload
                raw: None,
            })
            .unwrap(),
        )])
        .await;

        let config = AstClientConfig {
            max_stream_bytes: 1024, // 1 KiB — the lying server's claim blows past this
            ..Default::default()
        };
        let client = AstClient::connect(&path, config).await.unwrap();

        let err = client
            .invoke_stream("whatever", serde_json::json!({}), None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, AstClientError::StreamTooLarge { .. }),
            "expected StreamTooLarge, got {err:?}"
        );

        client.close().await;
    }

    #[tokio::test]
    async fn invoke_stream_surfaces_a_mid_stream_error_as_a_server_error() {
        let (_dir, path) = start_lying_stream_server(vec![
            (
                MessageType::StreamStart,
                codec::encode(&StreamStart {
                    total_chunks: 2,
                    total_bytes: 20,
                    raw: None,
                })
                .unwrap(),
            ),
            (
                MessageType::Error,
                codec::encode(&ErrorPayload {
                    code: ErrorCode::DeadlineExceeded,
                    message: "simulated mid-stream failure".to_string(),
                })
                .unwrap(),
            ),
        ])
        .await;

        let client = AstClient::connect(&path, AstClientConfig::default())
            .await
            .unwrap();

        let err = client
            .invoke_stream("whatever", serde_json::json!({}), None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            AstClientError::Server {
                code: ErrorCode::DeadlineExceeded,
                ..
            }
        ));

        client.close().await;
    }

    #[tokio::test]
    async fn invoke_stream_rejects_actual_bytes_received_over_the_limit_even_if_declared_total_was_small(
    ) {
        let oversized_chunk = StreamChunk {
            sequence: 0,
            data: vec![0u8; 2048], // bigger than max_stream_bytes below
        };
        let (_dir, path) = start_lying_stream_server(vec![
            (
                MessageType::StreamStart,
                codec::encode(&StreamStart {
                    total_chunks: 1,
                    total_bytes: 10, // lies small, to get past the first check
                    raw: None,
                })
                .unwrap(),
            ),
            (
                MessageType::StreamChunk,
                codec::encode(&oversized_chunk).unwrap(),
            ),
        ])
        .await;

        let config = AstClientConfig {
            max_stream_bytes: 1024,
            ..Default::default()
        };
        let client = AstClient::connect(&path, config).await.unwrap();

        let err = client
            .invoke_stream("whatever", serde_json::json!({}), None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, AstClientError::StreamTooLarge { .. }),
            "expected StreamTooLarge from actual-bytes check, got {err:?}"
        );

        client.close().await;
    }

    /// A fake AST server that completes HELLO, reads the next request
    /// frame, and then never answers it. Whatever frame arrives after
    /// that (the test expects a CANCEL from the client's timeout) is
    /// handed to the test through the returned oneshot receiver.
    async fn start_silent_server_that_reports_the_next_frame() -> (
        tempfile::TempDir,
        std::path::PathBuf,
        tokio::sync::oneshot::Receiver<(Frame, Frame)>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("silent-server.sock");
        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
        let (report_tx, report_rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let (mut stream, _addr) = listener.accept().await.unwrap();

            let hello_frame = wire::read_frame(&mut stream, FrameLimits::default())
                .await
                .unwrap()
                .unwrap();
            let ack = HelloAck {
                protocol_version: AST_PROTOCOL_VERSION,
                server_name: "silent-test-server".to_string(),
                ok: true,
                error: None,
            };
            wire::write_frame(
                &mut stream,
                &Frame {
                    msg_type: MessageType::HelloAck as u8,
                    correlation_id: hello_frame.correlation_id,
                    payload: codec::encode(&ack).unwrap(),
                },
            )
            .await
            .unwrap();

            // The INVOKE: read it and deliberately say nothing back.
            let invoke_frame = wire::read_frame(&mut stream, FrameLimits::default())
                .await
                .unwrap()
                .unwrap();
            // The next frame the client sends should be the CANCEL.
            let next_frame = wire::read_frame(&mut stream, FrameLimits::default())
                .await
                .unwrap()
                .unwrap();
            let _ = report_tx.send((invoke_frame, next_frame));
        });

        (dir, socket_path, report_rx)
    }

    #[tokio::test]
    async fn request_timeout_sends_a_best_effort_cancel_to_the_server() {
        let (_dir, path, report_rx) = start_silent_server_that_reports_the_next_frame().await;
        let config = AstClientConfig {
            request_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        let client = AstClient::connect(&path, config).await.unwrap();

        let err = client
            .invoke("whatever", serde_json::json!({}), None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, AstClientError::Timeout),
            "expected Timeout, got {err:?}"
        );

        let (invoke_frame, cancel_frame) = tokio::time::timeout(Duration::from_secs(5), report_rx)
            .await
            .expect("server never received a frame after the INVOKE")
            .unwrap();
        assert_eq!(
            MessageType::from_u8(invoke_frame.msg_type),
            Some(MessageType::Invoke)
        );
        assert_eq!(
            MessageType::from_u8(cancel_frame.msg_type),
            Some(MessageType::Cancel),
            "expected a CANCEL after the client timed out"
        );
        let cancel: Cancel = codec::decode(&cancel_frame.payload).unwrap();
        assert_eq!(cancel.target_correlation_id, invoke_frame.correlation_id);

        client.close().await;
    }

    #[tokio::test]
    async fn invoke_stream_rejects_out_of_order_chunk_sequence() {
        let (_dir, path) = start_lying_stream_server(vec![
            (
                MessageType::StreamStart,
                codec::encode(&StreamStart {
                    total_chunks: 2,
                    total_bytes: 6,
                    raw: None,
                })
                .unwrap(),
            ),
            (
                MessageType::StreamChunk,
                codec::encode(&StreamChunk {
                    sequence: 0,
                    data: vec![1, 2, 3],
                })
                .unwrap(),
            ),
            (
                MessageType::StreamChunk,
                codec::encode(&StreamChunk {
                    sequence: 2, // skips sequence 1
                    data: vec![4, 5, 6],
                })
                .unwrap(),
            ),
            (
                MessageType::StreamEnd,
                codec::encode(&StreamEnd {}).unwrap(),
            ),
        ])
        .await;

        let client = AstClient::connect(&path, AstClientConfig::default())
            .await
            .unwrap();
        let err = client
            .invoke_stream("whatever", serde_json::json!({}), None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, AstClientError::StreamProtocolViolation(_)),
            "expected StreamProtocolViolation, got {err:?}"
        );

        client.close().await;
    }

    #[tokio::test]
    async fn invoke_stream_rejects_a_byte_total_mismatch_at_stream_end() {
        let (_dir, path) = start_lying_stream_server(vec![
            (
                MessageType::StreamStart,
                codec::encode(&StreamStart {
                    total_chunks: 1,
                    total_bytes: 10, // the server promises 10 bytes...
                    raw: None,
                })
                .unwrap(),
            ),
            (
                MessageType::StreamChunk,
                codec::encode(&StreamChunk {
                    sequence: 0,
                    data: vec![1, 2, 3], // ...and delivers 3
                })
                .unwrap(),
            ),
            (
                MessageType::StreamEnd,
                codec::encode(&StreamEnd {}).unwrap(),
            ),
        ])
        .await;

        let client = AstClient::connect(&path, AstClientConfig::default())
            .await
            .unwrap();
        let err = client
            .invoke_stream("whatever", serde_json::json!({}), None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, AstClientError::StreamProtocolViolation(_)),
            "expected StreamProtocolViolation, got {err:?}"
        );

        client.close().await;
    }

    #[tokio::test]
    async fn stream_chunks_api_yields_chunks_incrementally() {
        let fixture_dir = tempfile::tempdir().unwrap();
        let contents = "incremental chunk consumption via invoke_stream_chunks\n".repeat(20);
        assert!(contents.len() > 1000);
        std::fs::write(fixture_dir.path().join("big.txt"), &contents).unwrap();
        let socket_path =
            start_test_server_with_filesystem_read_and_chunk_bytes(fixture_dir.path(), 64).await;

        let client = AstClient::connect(&socket_path, AstClientConfig::default())
            .await
            .unwrap();
        let mut handle = client
            .invoke_stream_chunks(
                "filesystem.read",
                serde_json::json!({"path": "big.txt"}),
                None,
            )
            .await
            .unwrap();
        assert!(
            handle.start().raw.is_some(),
            "filesystem.read should stream genuinely"
        );

        let mut chunk_count = 0;
        let mut reassembled = Vec::new();
        while let Some(chunk) = handle.next_chunk().await.unwrap() {
            chunk_count += 1;
            reassembled.extend_from_slice(&chunk);
        }
        assert!(
            chunk_count > 1,
            "expected several chunks before STREAM_END, got {chunk_count}"
        );
        assert_eq!(reassembled, contents.as_bytes());
        assert!(handle.next_chunk().await.unwrap().is_none());

        // The handle holds a clone of the client's sender; release it so
        // `close()` can finish the writer task.
        drop(handle);
        client.close().await;
    }

    #[tokio::test]
    async fn dropping_a_stream_handle_early_leaves_the_connection_usable() {
        let fixture_dir = tempfile::tempdir().unwrap();
        let contents = "early drop payload\n".repeat(200);
        std::fs::write(fixture_dir.path().join("big.txt"), &contents).unwrap();
        let socket_path =
            start_test_server_with_filesystem_read_and_chunk_bytes(fixture_dir.path(), 64).await;

        let client = AstClient::connect(&socket_path, AstClientConfig::default())
            .await
            .unwrap();
        let mut handle = client
            .invoke_stream_chunks(
                "filesystem.read",
                serde_json::json!({"path": "big.txt"}),
                None,
            )
            .await
            .unwrap();
        let first = handle.next_chunk().await.unwrap();
        assert!(first.is_some_and(|chunk| !chunk.is_empty()));
        drop(handle); // stream is unfinished: deregisters it and sends CANCEL

        client
            .ping()
            .await
            .expect("the connection should still answer after an early drop");
        client.close().await;
    }
}
