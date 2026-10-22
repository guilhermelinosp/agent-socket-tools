//! AST runtime: per-connection dispatch, concurrency, deadlines, and
//! best-effort cancellation.
//!
//! One connection serves many concurrent in-flight `INVOKE`s. Each gets
//! its own task (bounded by `max_in_flight`), and responses are written
//! back as they complete — not necessarily in the order their requests
//! arrived — correlated to their request by `correlation_id`. A single
//! writer task owns the socket's write half and drains a channel fed by
//! every in-flight task, so concurrent completions never interleave their
//! bytes on the wire.

use crate::ast::codec;
use crate::ast::message::*;
use crate::ast::peercred::{self, PeerCredentials};
use crate::ast::wire::{self, Frame, FrameError, FrameLimits};
use crate::tools::{ToolError, ToolRegistry};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, Notify, Semaphore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerPolicy {
    /// Only accept connections from processes running as the same uid as
    /// this server. The default, and the only policy with a real security
    /// property on a shared-account or shared-namespace host.
    SameUid,
    /// Accept any peer the socket's own filesystem permissions let
    /// through. Only meaningful to pick deliberately (e.g. a socket
    /// intentionally made group-accessible); never the default.
    AllowAny,
}

#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub frame_limits: FrameLimits,
    pub max_in_flight: usize,
    pub default_deadline: Duration,
    pub handshake_timeout: Duration,
    /// Maximum time to wait for the *next* frame on an already-
    /// handshaken connection before closing it. Resets on every frame
    /// received — it bounds how long a connected-but-silent peer can
    /// hold this connection's resources (its slot in
    /// `--max-connections`, any held `in_flight` permits from requests
    /// still running), not any one request's own deadline (that's
    /// `INVOKE.deadline_ms`, entirely independent). Without this, a
    /// peer that completes `HELLO` and then never sends — or sends
    /// unbearably slowly — anything else holds a connection open
    /// forever, a resource-exhaustion vector no amount of per-request
    /// bounding fixes, since the connection itself is never making a
    /// request to bound.
    /// Applied only while this connection has zero in-flight requests —
    /// a client legitimately waiting out a long `deadline_ms` for a
    /// response it already asked for isn't idle just because it has no
    /// reason to send another frame meanwhile, so this never fires
    /// while anything is still being worked on. It re-arms as soon as
    /// every outstanding request has been answered.
    pub idle_timeout: Duration,
    pub peer_policy: PeerPolicy,
    pub server_name: String,
    /// Maximum number of calls in one `BATCH`. A whole `BATCH` counts as
    /// a single in-flight slot (see `max_in_flight`) regardless of this
    /// value — this bounds how much sequential work one slot can demand,
    /// not how many slots a batch consumes.
    pub max_batch_size: usize,
    /// Running budget, in bytes, for a `BATCH`'s aggregate encoded
    /// output. Checked incrementally, after each call's result is
    /// encoded — never by accumulating every result unbounded and only
    /// discovering the batch is too large when the whole `BatchResult`
    /// is serialized at the end. The call that would push the running
    /// total over this budget is reported as `ResultTooLarge` in its own
    /// entry and the batch stops there (same early-termination shape as
    /// a mid-batch cancellation — entries for calls that never ran are
    /// simply absent from `results`). Defaults to `frame_limits`'s
    /// `max_frame_len`, since that's the real ceiling a `BatchResult`
    /// must fit under regardless of this setting.
    pub max_batch_output_bytes: usize,
    /// Target size, in bytes, of one `STREAM_CHUNK`'s `data` when a
    /// client requests streamed delivery (`Invoke.stream: true`). The
    /// last chunk of a stream is typically smaller. Clamped down at
    /// connection start if it wouldn't fit inside `frame_limits` (see
    /// `run_connection`), so misconfiguring this relative to
    /// `max_frame_bytes` can't produce an oversized frame.
    pub stream_chunk_bytes: usize,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            frame_limits: FrameLimits::default(),
            max_in_flight: 64,
            default_deadline: Duration::from_secs(10),
            handshake_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(300),
            peer_policy: PeerPolicy::SameUid,
            server_name: "agent-socket-tools/ast".to_string(),
            max_batch_size: 100,
            max_batch_output_bytes: FrameLimits::default().max_frame_len as usize,
            stream_chunk_bytes: 64 * 1024,
        }
    }
}

type Cancellations = Arc<Mutex<HashMap<u64, Arc<Notify>>>>;

/// Serves one accepted connection to completion. Returns once the
/// connection ends for any reason (clean disconnect, framing error,
/// failed peer-credential check, failed handshake, or a fatal write
/// error) — never propagates an error to the caller, since by this point
/// there's nothing left to do with one except log it.
pub async fn run_connection(
    stream: UnixStream,
    registry: Arc<ToolRegistry>,
    config: ConnectionConfig,
) {
    if config.peer_policy == PeerPolicy::SameUid {
        match check_same_uid(&stream) {
            Ok(true) => {}
            Ok(false) => {
                tracing::warn!("rejecting connection: peer uid does not match ours");
                return;
            }
            Err(e) => {
                tracing::warn!(error = %e, "rejecting connection: peer credential check failed");
                return;
            }
        }
    }

    let (read_half, write_half) = stream.into_split();
    let (tx, mut rx) = mpsc::channel::<Frame>(64);

    let writer_task = tokio::spawn(async move {
        let mut write_half = write_half;
        while let Some(frame) = rx.recv().await {
            if wire::write_frame(&mut write_half, &frame).await.is_err() {
                return;
            }
        }
    });

    serve_reads(read_half, tx.clone(), registry, config).await;

    drop(tx); // lets the writer task's `rx.recv()` loop end
    let _ = writer_task.await;
}

fn check_same_uid(stream: &UnixStream) -> std::io::Result<bool> {
    let PeerCredentials { uid, .. } = peercred::peer_credentials(stream)?;
    // SAFETY: `getuid` takes no arguments and cannot fail.
    let our_uid = unsafe { libc::getuid() };
    Ok(uid == our_uid)
}

/// Resolves only once the connection has had zero in-flight requests
/// continuously for `idle_timeout` — not merely "zero in-flight at some
/// point". Polls `in_flight` in memory only (never touches the socket),
/// so it's always safe to race in a `tokio::select!` against a single,
/// never-retried `wire::read_frame` call: if this future wins the race,
/// the connection is closed outright (never another read attempt on the
/// same, possibly partially-consumed stream), which is what keeps that
/// race safe despite `read_frame` not being cancel-safe after it's
/// consumed some bytes of a frame.
async fn wait_until_continuously_idle(
    idle_timeout: Duration,
    in_flight: &Arc<Mutex<std::collections::HashSet<u64>>>,
) {
    let poll = idle_timeout
        .div_f64(20.0)
        .clamp(Duration::from_millis(1), Duration::from_secs(1));
    loop {
        while !in_flight
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_empty()
        {
            tokio::time::sleep(poll).await;
        }
        let became_idle_at = Instant::now();
        loop {
            tokio::time::sleep(poll.min(idle_timeout)).await;
            if !in_flight
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty()
            {
                break; // went busy again — restart the outer loop
            }
            if became_idle_at.elapsed() >= idle_timeout {
                return; // continuously idle for the whole window
            }
        }
    }
}

async fn serve_reads(
    mut read_half: tokio::net::unix::OwnedReadHalf,
    tx: mpsc::Sender<Frame>,
    registry: Arc<ToolRegistry>,
    config: ConnectionConfig,
) {
    let max_frame_len = config.frame_limits.max_frame_len;

    // --- Handshake: the first frame must be HELLO. ---
    let first = match tokio::time::timeout(
        config.handshake_timeout,
        wire::read_frame(&mut read_half, config.frame_limits),
    )
    .await
    {
        Ok(Ok(Some(frame))) => frame,
        Ok(Ok(None)) => return, // disconnected before saying anything
        Ok(Err(e)) => {
            tracing::debug!(error = %e, "framing error during handshake");
            return;
        }
        Err(_elapsed) => {
            tracing::debug!("handshake timed out waiting for HELLO");
            return;
        }
    };

    if MessageType::from_u8(first.msg_type) != Some(MessageType::Hello) {
        let _ = send_error(
            &tx,
            first.correlation_id,
            ErrorCode::InvalidMessage,
            "the first message on a connection must be HELLO",
            max_frame_len,
        )
        .await;
        return;
    }

    let hello: Hello = match codec::decode(&first.payload) {
        Ok(h) => h,
        Err(e) => {
            let _ = send_error(
                &tx,
                first.correlation_id,
                ErrorCode::InvalidMessage,
                &e.to_string(),
                max_frame_len,
            )
            .await;
            return;
        }
    };

    if hello.protocol_version != AST_PROTOCOL_VERSION {
        let ack = HelloAck {
            protocol_version: AST_PROTOCOL_VERSION,
            server_name: config.server_name.clone(),
            ok: false,
            error: Some(format!(
                "server supports protocol version {AST_PROTOCOL_VERSION}, client requested {}",
                hello.protocol_version
            )),
        };
        let _ = send(
            &tx,
            MessageType::HelloAck,
            first.correlation_id,
            &ack,
            max_frame_len,
        )
        .await;
        return;
    }

    let ack = HelloAck {
        protocol_version: AST_PROTOCOL_VERSION,
        server_name: config.server_name.clone(),
        ok: true,
        error: None,
    };
    if send(
        &tx,
        MessageType::HelloAck,
        first.correlation_id,
        &ack,
        max_frame_len,
    )
    .await
    .is_err()
    {
        return;
    }

    // --- Steady state. ---
    let in_flight: Arc<Mutex<std::collections::HashSet<u64>>> =
        Arc::new(Mutex::new(Default::default()));
    let cancellations: Cancellations = Arc::new(Mutex::new(HashMap::new()));
    let in_flight_limit = Arc::new(Semaphore::new(config.max_in_flight));

    loop {
        let read_result = tokio::select! {
            result = wire::read_frame(&mut read_half, config.frame_limits) => result,
            _ = wait_until_continuously_idle(config.idle_timeout, &in_flight) => {
                tracing::debug!(
                    idle_timeout = ?config.idle_timeout,
                    "closing connection: no frame received within the idle timeout"
                );
                return;
            }
        };

        let frame = match read_result {
            Ok(Some(f)) => f,
            Ok(None) => return, // clean disconnect
            Err(FrameError::TooLarge { .. }) => {
                let _ = send_error(
                    &tx,
                    0,
                    ErrorCode::InvalidFrame,
                    "frame exceeded the maximum size",
                    max_frame_len,
                )
                .await;
                return;
            }
            Err(FrameError::TooSmall(_)) => {
                let _ = send_error(
                    &tx,
                    0,
                    ErrorCode::InvalidFrame,
                    "frame was smaller than the minimum header",
                    max_frame_len,
                )
                .await;
                return;
            }
            Err(FrameError::Incomplete) => return, // peer disconnected mid-frame; nothing to say back
            Err(FrameError::Io(e)) => {
                tracing::debug!(error = %e, "socket read failed");
                return;
            }
        };

        match MessageType::from_u8(frame.msg_type) {
            Some(MessageType::Ping) => {
                if send(
                    &tx,
                    MessageType::Pong,
                    frame.correlation_id,
                    &Pong {},
                    max_frame_len,
                )
                .await
                .is_err()
                {
                    return;
                }
            }
            Some(MessageType::Discover) => {
                let tools = registry
                    .discover()
                    .into_iter()
                    .map(|t| ToolDescriptorWire {
                        name: t.name,
                        description: t.description,
                        input_schema: t.input_schema,
                        version: "1.0.0".to_string(),
                        required_permissions: vec![],
                        max_input_bytes: None,
                        max_output_bytes: None,
                    })
                    .collect();
                let result = DiscoverResult { tools };
                if send(
                    &tx,
                    MessageType::DiscoverResult,
                    frame.correlation_id,
                    &result,
                    max_frame_len,
                )
                .await
                .is_err()
                {
                    return;
                }
            }
            Some(MessageType::Invoke) => {
                let already_in_flight = {
                    let mut set = in_flight.lock().unwrap_or_else(|p| p.into_inner());
                    !set.insert(frame.correlation_id)
                };
                if already_in_flight {
                    let _ = send_error(
                        &tx,
                        frame.correlation_id,
                        ErrorCode::InvalidMessage,
                        "duplicate correlation_id: a request with this id is already in flight",
                        max_frame_len,
                    )
                    .await;
                    continue;
                }

                let permit = match Arc::clone(&in_flight_limit).try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        in_flight
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .remove(&frame.correlation_id);
                        let _ = send_error(
                            &tx,
                            frame.correlation_id,
                            ErrorCode::ResourceExhausted,
                            "server is already handling its configured maximum of in-flight requests on this connection",
                            max_frame_len,
                        )
                        .await;
                        continue;
                    }
                };

                let invoke: Invoke = match codec::decode(&frame.payload) {
                    Ok(i) => i,
                    Err(e) => {
                        in_flight
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .remove(&frame.correlation_id);
                        let _ = send_error(
                            &tx,
                            frame.correlation_id,
                            ErrorCode::InvalidMessage,
                            &e.to_string(),
                            max_frame_len,
                        )
                        .await;
                        continue;
                    }
                };

                let notify = Arc::new(Notify::new());
                cancellations
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(frame.correlation_id, Arc::clone(&notify));

                spawn_invoke(
                    frame.correlation_id,
                    invoke,
                    Arc::clone(&registry),
                    config.default_deadline,
                    notify,
                    permit,
                    Arc::clone(&in_flight),
                    Arc::clone(&cancellations),
                    tx.clone(),
                    max_frame_len,
                    effective_stream_chunk_bytes(config.stream_chunk_bytes, max_frame_len),
                );
            }
            Some(MessageType::Batch) => {
                let already_in_flight = {
                    let mut set = in_flight.lock().unwrap_or_else(|p| p.into_inner());
                    !set.insert(frame.correlation_id)
                };
                if already_in_flight {
                    let _ = send_error(
                        &tx,
                        frame.correlation_id,
                        ErrorCode::InvalidMessage,
                        "duplicate correlation_id: a request with this id is already in flight",
                        max_frame_len,
                    )
                    .await;
                    continue;
                }

                // A whole BATCH occupies exactly one in-flight slot,
                // regardless of how many calls it contains — see
                // `ConnectionConfig::max_batch_size`'s doc comment.
                let permit = match Arc::clone(&in_flight_limit).try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        in_flight
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .remove(&frame.correlation_id);
                        let _ = send_error(
                            &tx,
                            frame.correlation_id,
                            ErrorCode::ResourceExhausted,
                            "server is already handling its configured maximum of in-flight requests on this connection",
                            max_frame_len,
                        )
                        .await;
                        continue;
                    }
                };

                let batch: Batch = match codec::decode(&frame.payload) {
                    Ok(b) => b,
                    Err(e) => {
                        in_flight
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .remove(&frame.correlation_id);
                        let _ = send_error(
                            &tx,
                            frame.correlation_id,
                            ErrorCode::InvalidMessage,
                            &e.to_string(),
                            max_frame_len,
                        )
                        .await;
                        continue;
                    }
                };

                if batch.calls.len() > config.max_batch_size {
                    in_flight
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .remove(&frame.correlation_id);
                    let _ = send_error(
                        &tx,
                        frame.correlation_id,
                        ErrorCode::InvalidMessage,
                        &format!(
                            "batch has {} calls, exceeding the configured maximum of {}",
                            batch.calls.len(),
                            config.max_batch_size
                        ),
                        max_frame_len,
                    )
                    .await;
                    continue;
                }

                let notify = Arc::new(Notify::new());
                cancellations
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(frame.correlation_id, Arc::clone(&notify));

                spawn_batch(
                    frame.correlation_id,
                    batch,
                    Arc::clone(&registry),
                    config.default_deadline,
                    notify,
                    permit,
                    Arc::clone(&in_flight),
                    Arc::clone(&cancellations),
                    tx.clone(),
                    max_frame_len,
                    config.max_batch_output_bytes,
                );
            }
            Some(MessageType::Cancel) => {
                if let Ok(cancel) = codec::decode::<Cancel>(&frame.payload) {
                    if let Some(notify) = cancellations
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .get(&cancel.target_correlation_id)
                    {
                        notify.notify_one();
                    }
                    // An id that isn't (or is no longer) in flight is not
                    // an error: the request may have already completed, or
                    // the CANCEL may have raced it — both are fine, and
                    // CANCEL has no response of its own either way.
                }
            }
            Some(MessageType::Hello) => {
                let _ = send_error(
                    &tx,
                    frame.correlation_id,
                    ErrorCode::InvalidMessage,
                    "HELLO is only valid as the first message on a connection",
                    max_frame_len,
                )
                .await;
            }
            Some(
                MessageType::HelloAck
                | MessageType::DiscoverResult
                | MessageType::Result
                | MessageType::Error
                | MessageType::Pong
                | MessageType::BatchResult
                | MessageType::StreamStart
                | MessageType::StreamChunk
                | MessageType::StreamEnd,
            ) => {
                // These are server->client-only message types; receiving
                // one from a client is a protocol violation, not a crash.
                let _ = send_error(
                    &tx,
                    frame.correlation_id,
                    ErrorCode::InvalidMessage,
                    "this message type is server-to-client only",
                    max_frame_len,
                )
                .await;
            }
            None => {
                let _ = send_error(
                    &tx,
                    frame.correlation_id,
                    ErrorCode::InvalidFrame,
                    &format!("unknown message type byte: {}", frame.msg_type),
                    max_frame_len,
                )
                .await;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
/// Clamps a requested `STREAM_CHUNK` payload size so it always fits
/// inside one frame under `max_frame_len`, no matter how
/// `stream_chunk_bytes` is configured — leaves generous headroom (128
/// bytes) for the frame header and `StreamChunk`'s own `sequence` field
/// and MessagePack map/bin overhead, rather than computing the exact
/// minimum that would fit.
fn effective_stream_chunk_bytes(configured: usize, max_frame_len: u32) -> usize {
    let ceiling = (max_frame_len as usize).saturating_sub(wire::HEADER_LEN + 128);
    configured.min(ceiling).max(1)
}

#[allow(clippy::too_many_arguments)]
fn spawn_invoke(
    correlation_id: u64,
    invoke: Invoke,
    registry: Arc<ToolRegistry>,
    default_deadline: Duration,
    cancel_notify: Arc<Notify>,
    permit: tokio::sync::OwnedSemaphorePermit,
    in_flight: Arc<Mutex<std::collections::HashSet<u64>>>,
    cancellations: Cancellations,
    tx: mpsc::Sender<Frame>,
    max_frame_len: u32,
    stream_chunk_bytes: usize,
) {
    let wants_stream = invoke.stream.unwrap_or(false);
    let tool_streams_genuinely = wants_stream
        && registry
            .get(&invoke.tool)
            .is_some_and(|t| t.as_streaming().is_some());

    if tool_streams_genuinely {
        // Genuine incremental streaming keeps `in_flight`/`cancellations`
        // registered for the *entire* transfer, not just the time it
        // takes to open the file — unlike the other branch below, where
        // the whole result already exists in memory by the time chunking
        // even starts, here a `CANCEL` must still be able to interrupt an
        // in-progress disk-to-wire transfer, which can meaningfully take
        // a while for a large stream.
        tokio::spawn(async move {
            let _permit = permit;
            let deadline = invoke
                .deadline_ms
                .map(Duration::from_millis)
                .unwrap_or(default_deadline);
            let started = Instant::now();

            let opened = run_invoke_streaming(
                invoke,
                registry,
                deadline,
                started,
                Arc::clone(&cancel_notify),
            )
            .await;
            match opened {
                Ok(opened) => {
                    send_raw_streamed_result(
                        &tx,
                        correlation_id,
                        opened,
                        stream_chunk_bytes,
                        deadline,
                        started,
                        cancel_notify,
                        max_frame_len,
                    )
                    .await;
                }
                Err((code, message)) => {
                    let _ = send_error(&tx, correlation_id, code, &message, max_frame_len).await;
                }
            }

            in_flight
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&correlation_id);
            cancellations
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&correlation_id);
        });
        return;
    }

    tokio::spawn(async move {
        let _permit = permit; // held for this task's whole lifetime
        let deadline = invoke
            .deadline_ms
            .map(Duration::from_millis)
            .unwrap_or(default_deadline);

        let response = run_invoke(invoke, registry, deadline, cancel_notify).await;

        in_flight
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&correlation_id);
        cancellations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&correlation_id);

        match response {
            Ok(value) if wants_stream => {
                send_streamed_result(&tx, correlation_id, value, stream_chunk_bytes).await;
            }
            Ok(value) => {
                if send(
                    &tx,
                    MessageType::Result,
                    correlation_id,
                    &InvokeResult { value },
                    max_frame_len,
                )
                .await
                .is_err()
                {
                    tracing::debug!(correlation_id, "response could not be delivered: the connection's outgoing channel is closed or the response failed to encode");
                }
            }
            Err((code, message)) => {
                if send_error(&tx, correlation_id, code, &message, max_frame_len)
                    .await
                    .is_err()
                {
                    tracing::debug!(correlation_id, "response could not be delivered: the connection's outgoing channel is closed or the response failed to encode");
                }
            }
        }
    });
}

/// Like [`run_invoke`], but opens a genuinely incremental stream (see
/// [`agent_socket_tools::tools::StreamingTool`]) instead of computing the
/// tool's full result. Only called once the caller has already confirmed
/// the tool supports it.
async fn run_invoke_streaming(
    invoke: Invoke,
    registry: Arc<ToolRegistry>,
    deadline: Duration,
    started: Instant,
    cancel_notify: Arc<Notify>,
) -> Result<crate::tools::StreamingToolOpen, (ErrorCode, String)> {
    let Some(_tool) = registry.get(&invoke.tool) else {
        return Err((
            ErrorCode::ToolNotFound,
            format!("no such tool: {}", invoke.tool),
        ));
    };

    let tool_name = invoke.tool.clone();
    let args = invoke.args;
    let task = tokio::task::spawn_blocking(move || {
        let tool = registry
            .get(&tool_name)
            .expect("presence already checked before spawning");
        let streaming = tool
            .as_streaming()
            .expect("caller already confirmed this tool supports streaming");
        streaming.open_stream(args)
    });

    tokio::select! {
        result = task => match result {
            Ok(Ok(opened)) => Ok(opened),
            Ok(Err(tool_err)) => Err(tool_error_to_code(tool_err)),
            Err(join_err) => Err((ErrorCode::InternalError, format!("tool task panicked: {join_err}"))),
        },
        _ = tokio::time::sleep(deadline.saturating_sub(started.elapsed())) => {
            Err((ErrorCode::DeadlineExceeded, "the tool did not complete within the configured deadline".to_string()))
        }
        _ = cancel_notify.notified() => {
            Err((ErrorCode::Cancelled, "the request was cancelled".to_string()))
        }
    }
}

/// Sends a genuinely incremental stream: `open_stream`'s `metadata` goes
/// out immediately in `STREAM_START`, then each chunk is read from the
/// tool's `IncrementalBytes` reader (one bounded, blocking read at a
/// time, via `spawn_blocking`) and sent as raw bytes — the server never
/// holds more than one chunk's worth of the streamed field in memory,
/// regardless of its total size.
///
/// Deadline and cancellation are raced on every single chunk read, not
/// just once up front. Once `STREAM_START` has gone out there is no
/// way to retract it, but a mid-transfer failure (deadline, cancellation,
/// a panicked reader task, or the tool's own incremental read failing)
/// now sends a terminal `ERROR` frame with this stream's own
/// correlation_id instead of silently stopping — the client finds out
/// immediately, rather than only after its own overall timeout on the
/// call eventually fires.
///
/// `started` is the same clock `run_invoke_streaming` was given, so the
/// open step and the chunk-reading step share one `deadline` budget
/// rather than each getting a fresh one.
#[allow(clippy::too_many_arguments)]
async fn send_raw_streamed_result(
    tx: &mpsc::Sender<Frame>,
    correlation_id: u64,
    opened: crate::tools::StreamingToolOpen,
    chunk_bytes: usize,
    deadline: Duration,
    started: Instant,
    cancel_notify: Arc<Notify>,
    max_frame_len: u32,
) {
    let total_bytes = opened.reader.total_bytes_hint().unwrap_or(0);
    let total_chunks = if total_bytes == 0 {
        1
    } else {
        total_bytes.div_ceil(chunk_bytes.max(1) as u64).max(1) as u32
    };

    if send_raw(
        tx,
        MessageType::StreamStart,
        correlation_id,
        &StreamStart {
            total_chunks,
            total_bytes,
            raw: Some(RawStreamInfo {
                metadata: opened.metadata,
                field_name: opened.field_name.to_string(),
            }),
        },
    )
    .await
    .is_err()
    {
        return;
    }

    let mut reader = opened.reader;
    let mut sequence: u32 = 0;
    loop {
        let remaining = deadline.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            let _ = send_error(
                tx,
                correlation_id,
                ErrorCode::DeadlineExceeded,
                "the tool did not complete within the configured deadline",
                max_frame_len,
            )
            .await;
            return;
        }

        let read_task = tokio::task::spawn_blocking(move || {
            let chunk = reader.next_chunk(chunk_bytes.max(1));
            (reader, chunk)
        });

        let (returned_reader, chunk_result) = tokio::select! {
            result = read_task => match result {
                Ok(pair) => pair,
                Err(join_err) => {
                    let _ = send_error(
                        tx,
                        correlation_id,
                        ErrorCode::InternalError,
                        &format!("the streaming reader task panicked: {join_err}"),
                        max_frame_len,
                    )
                    .await;
                    return;
                }
            },
            _ = tokio::time::sleep(remaining) => {
                let _ = send_error(
                    tx,
                    correlation_id,
                    ErrorCode::DeadlineExceeded,
                    "the tool did not complete within the configured deadline",
                    max_frame_len,
                )
                .await;
                return;
            }
            _ = cancel_notify.notified() => {
                let _ = send_error(
                    tx,
                    correlation_id,
                    ErrorCode::Cancelled,
                    "the request was cancelled",
                    max_frame_len,
                )
                .await;
                return;
            }
        };
        reader = returned_reader;

        match chunk_result {
            Ok(Some(data)) => {
                if send_raw(
                    tx,
                    MessageType::StreamChunk,
                    correlation_id,
                    &StreamChunk { sequence, data },
                )
                .await
                .is_err()
                {
                    return;
                }
                sequence += 1;
            }
            Ok(None) => break, // exhausted
            Err(tool_err) => {
                let (code, message) = tool_error_to_code(tool_err);
                let _ = send_error(tx, correlation_id, code, &message, max_frame_len).await;
                return;
            }
        }
    }

    let _ = send_raw(tx, MessageType::StreamEnd, correlation_id, &StreamEnd {}).await;
}

/// Sends an already-computed, successful result as
/// `STREAM_START`/`STREAM_CHUNK*`/`STREAM_END` instead of one `RESULT`.
/// Each `tx.send(...).await` below blocks when the connection's bounded
/// outgoing-frame channel is full — the same backpressure every other
/// response already relies on — so a slow reader on the other end
/// throttles how fast this loop produces further chunks, rather than
/// buffering the whole result in memory on the server's behalf beyond
/// what's already needed to have computed `value` in the first place.
async fn send_streamed_result(
    tx: &mpsc::Sender<Frame>,
    correlation_id: u64,
    value: serde_json::Value,
    chunk_bytes: usize,
) {
    let encoded = match codec::encode(&value) {
        Ok(bytes) => bytes,
        Err(_) => return, // can't even report this as an ERROR without re-encoding the same failure
    };
    let total_chunks = encoded.chunks(chunk_bytes.max(1)).count().max(1) as u32;

    // STREAM_START/STREAM_CHUNK/STREAM_END are never themselves subject
    // to RESULT_TOO_LARGE substitution the way a plain RESULT is — each
    // chunk's size is already bounded by `chunk_bytes`
    // (`effective_stream_chunk_bytes` clamps it below `max_frame_len`),
    // so `send_raw` (not `send`) is correct here: there's no "too large"
    // case left to detect.
    if send_raw(
        tx,
        MessageType::StreamStart,
        correlation_id,
        &StreamStart {
            total_chunks,
            total_bytes: encoded.len() as u64,
            raw: None,
        },
    )
    .await
    .is_err()
    {
        return;
    }

    for (sequence, chunk) in encoded.chunks(chunk_bytes.max(1)).enumerate() {
        if send_raw(
            tx,
            MessageType::StreamChunk,
            correlation_id,
            &StreamChunk {
                sequence: sequence as u32,
                data: chunk.to_vec(),
            },
        )
        .await
        .is_err()
        {
            return;
        }
    }

    let _ = send_raw(tx, MessageType::StreamEnd, correlation_id, &StreamEnd {}).await;
}

/// Reserved margin, in bytes, subtracted from `max_batch_output_bytes`
/// before comparing against it — `BatchResult`'s own MessagePack map/array
/// framing overhead (beyond the sum of its entries) is small but nonzero,
/// so without this margin a batch whose entries sum to exactly the
/// configured budget could still overflow `max_frame_len` once wrapped in
/// the final `BatchResult`, discarding every result in the batch as one
/// `RESULT_TOO_LARGE` error instead of the more precise per-entry handling
/// this budget is supposed to provide.
const BATCH_RESULT_FRAMING_OVERHEAD_BYTES: usize = 128;

#[allow(clippy::too_many_arguments)]
fn spawn_batch(
    correlation_id: u64,
    batch: Batch,
    registry: Arc<ToolRegistry>,
    default_deadline: Duration,
    cancel_notify: Arc<Notify>,
    permit: tokio::sync::OwnedSemaphorePermit,
    in_flight: Arc<Mutex<std::collections::HashSet<u64>>>,
    cancellations: Cancellations,
    tx: mpsc::Sender<Frame>,
    max_frame_len: u32,
    max_batch_output_bytes: usize,
) {
    tokio::spawn(async move {
        let _permit = permit; // held for the whole batch's lifetime

        // Sequential, in array order, never concurrently with each other
        // — see `Batch`'s doc comment for why. Each call still goes
        // through the exact same `run_invoke` a standalone `INVOKE` does,
        // including its own deadline and racing the same shared
        // `cancel_notify`, so a `CANCEL` targeting this batch stops
        // whichever call is currently running the same way it would stop
        // a single in-flight `INVOKE`.
        //
        // `encoded_bytes_so_far` tracks a running, incremental estimate of
        // the aggregate output encoded so far (one call's result at a
        // time) against `max_batch_output_bytes` — never by accumulating
        // every result unbounded and discovering the batch is too large
        // only when the whole `BatchResult` is serialized at the end.
        // This bounds peak memory to "whatever's already in `results`
        // plus one more call's result," not "every result the batch ever
        // produces."
        let mut results = Vec::with_capacity(batch.calls.len());
        let mut encoded_bytes_so_far: usize = 0;
        for call in batch.calls {
            let id = call.id;
            let invoke = Invoke {
                tool: call.tool,
                args: call.args,
                deadline_ms: call.deadline_ms,
                // Streamed delivery is an INVOKE-level choice; BATCH
                // always collects each call's full result in one
                // BatchResultEntry; there's no such thing as "stream one
                // entry within a batch."
                stream: None,
            };
            let deadline = invoke
                .deadline_ms
                .map(Duration::from_millis)
                .unwrap_or(default_deadline);

            let outcome = run_invoke(
                invoke,
                Arc::clone(&registry),
                deadline,
                Arc::clone(&cancel_notify),
            )
            .await;
            let was_cancelled = matches!(outcome, Err((ErrorCode::Cancelled, _)));

            let mut entry = BatchResultEntry {
                id,
                outcome: match outcome {
                    Ok(value) => BatchOutcome::Ok { value },
                    Err((code, message)) => BatchOutcome::Error { code, message },
                },
            };

            // Measure this one entry's own encoded size and check it
            // against the remaining budget *before* committing it to
            // `results` — the entry that would tip the running total
            // over the budget is replaced with a small `ResultTooLarge`
            // error in its own slot (not silently dropped: the caller
            // needs to know this `id` was attempted, not merely
            // "never ran" like a call after cancellation or budget
            // exhaustion would be).
            let entry_bytes = codec::encode(&entry).map(|b| b.len()).unwrap_or(0);
            let exceeds_budget = encoded_bytes_so_far.saturating_add(entry_bytes)
                > max_batch_output_bytes.saturating_sub(BATCH_RESULT_FRAMING_OVERHEAD_BYTES);
            if exceeds_budget {
                entry = BatchResultEntry {
                    id,
                    outcome: BatchOutcome::Error {
                        code: ErrorCode::ResultTooLarge,
                        message: "this call's result, combined with the batch's prior results, exceeded the configured aggregate output budget".to_string(),
                    },
                };
                results.push(entry);
                break;
            }
            encoded_bytes_so_far += entry_bytes;
            results.push(entry);

            if was_cancelled {
                // Stop processing the rest of the batch; the entries that
                // never ran simply have no entry in `results` (see
                // `BatchResult`'s doc comment).
                break;
            }
        }

        in_flight
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&correlation_id);
        cancellations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&correlation_id);

        if send(
            &tx,
            MessageType::BatchResult,
            correlation_id,
            &BatchResult { results },
            max_frame_len,
        )
        .await
        .is_err()
        {
            tracing::debug!(correlation_id, "response could not be delivered: the connection's outgoing channel is closed or the response failed to encode");
        }
    });
}

async fn run_invoke(
    invoke: Invoke,
    registry: Arc<ToolRegistry>,
    deadline: Duration,
    cancel_notify: Arc<Notify>,
) -> Result<serde_json::Value, (ErrorCode, String)> {
    let Some(_tool) = registry.get(&invoke.tool) else {
        return Err((
            ErrorCode::ToolNotFound,
            format!("no such tool: {}", invoke.tool),
        ));
    };

    // Looked up again inside the blocking closure (an `Arc` bump, not a
    // deep copy) rather than borrowed across the `spawn_blocking` boundary
    // — see the equivalent comment in `src/transport.rs` for why.
    let tool_name = invoke.tool.clone();
    let args = invoke.args;
    let task = tokio::task::spawn_blocking(move || {
        let tool = registry
            .get(&tool_name)
            .expect("presence already checked before spawning");
        tool.invoke(args)
    });

    // Races the deadline, explicit cancellation, and the tool actually
    // finishing. Whichever comes first decides the outcome — but, same
    // caveat as the legacy protocol's timeout: neither "deadline exceeded"
    // nor "cancelled" can forcibly stop a `spawn_blocking` task that's
    // already running. Both guarantee the *caller* gets an answer and the
    // in-flight slot is freed; neither guarantees the tool's own work
    // actually stopped.
    tokio::select! {
        result = task => match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(tool_err)) => Err(tool_error_to_code(tool_err)),
            Err(join_err) => Err((ErrorCode::InternalError, format!("tool task panicked: {join_err}"))),
        },
        _ = tokio::time::sleep(deadline) => {
            Err((ErrorCode::DeadlineExceeded, "the tool did not complete within the configured deadline".to_string()))
        }
        _ = cancel_notify.notified() => {
            Err((ErrorCode::Cancelled, "the request was cancelled".to_string()))
        }
    }
}

fn tool_error_to_code(err: ToolError) -> (ErrorCode, String) {
    match err {
        ToolError::InvalidArgs(msg) => (ErrorCode::InvalidArguments, msg),
        ToolError::Unauthorized(msg) => (ErrorCode::PermissionDenied, msg),
        ToolError::Failed(msg) => (ErrorCode::InternalError, msg),
    }
}

/// Encodes and enqueues `payload` as-is, with no size check — used only
/// by [`send`] itself (for the actual, already-validated payload) and for
/// the `RESULT_TOO_LARGE` substitute error it sends instead of an
/// oversized one (a short, fixed string that can never itself trigger
/// the check it's replacing).
async fn send_raw<T: serde::Serialize>(
    tx: &mpsc::Sender<Frame>,
    msg_type: MessageType,
    correlation_id: u64,
    payload: &T,
) -> Result<(), ()> {
    let bytes = codec::encode(payload).map_err(|_| ())?;
    let frame = Frame {
        msg_type: msg_type as u8,
        correlation_id,
        payload: bytes,
    };
    tx.send(frame).await.map_err(|_| ())
}

/// Encodes `payload` and enqueues it — unless the resulting frame would
/// exceed `max_frame_len`, in which case a small, bounded
/// `ERROR{RESULT_TOO_LARGE}` is sent in its place instead. Checked here,
/// once, for every outgoing message regardless of which operation
/// produced it: `DISCOVER_RESULT` (a long tool list), `HELLO_ACK`, and
/// `RESULT`/`ERROR` (a tool's own result or error, e.g.
/// `filesystem.read`'s file contents) all go through it.
async fn send<T: serde::Serialize>(
    tx: &mpsc::Sender<Frame>,
    msg_type: MessageType,
    correlation_id: u64,
    payload: &T,
    max_frame_len: u32,
) -> Result<(), ()> {
    let bytes = codec::encode(payload).map_err(|_| ())?;
    if (wire::HEADER_LEN as u64 + bytes.len() as u64) > max_frame_len as u64 {
        let substitute = ErrorPayload {
            code: ErrorCode::ResultTooLarge,
            message: "the response exceeded the configured maximum frame size".to_string(),
        };
        return send_raw(tx, MessageType::Error, correlation_id, &substitute).await;
    }
    let frame = Frame {
        msg_type: msg_type as u8,
        correlation_id,
        payload: bytes,
    };
    tx.send(frame).await.map_err(|_| ())
}

async fn send_error(
    tx: &mpsc::Sender<Frame>,
    correlation_id: u64,
    code: ErrorCode,
    message: &str,
    max_frame_len: u32,
) -> Result<(), ()> {
    send(
        tx,
        MessageType::Error,
        correlation_id,
        &ErrorPayload {
            code,
            message: message.to_string(),
        },
        max_frame_len,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::wire::read_frame;
    use crate::tools::{
        FilesystemReadTool, IncrementalBytes, StreamingTool, StreamingToolOpen, SystemInfoTool,
        Tool, ToolError as RegistryToolError,
    };
    use serde_json::Value as JsonValue;
    use tokio::net::UnixListener;

    /// A test-only tool whose `open_stream` itself takes a configurable
    /// amount of time before returning, and whose reader takes its own
    /// configurable time on the first chunk read. Lets the test prove the
    /// open step and the chunk-reading step share one `deadline` budget,
    /// which an always-fast open (like `FilesystemReadTool`) cannot.
    struct SlowOpenStreamingTool {
        open_delay: Duration,
        chunk_delay: Duration,
    }
    impl Tool for SlowOpenStreamingTool {
        fn name(&self) -> &str {
            "test.slow_open_stream"
        }
        fn description(&self) -> &str {
            "a streaming tool whose open and first read are artificially slow, for tests"
        }
        fn input_schema(&self) -> JsonValue {
            serde_json::json!({})
        }
        fn invoke(&self, _args: JsonValue) -> Result<JsonValue, RegistryToolError> {
            Ok(serde_json::json!({"contents": ""}))
        }
        fn as_streaming(&self) -> Option<&dyn StreamingTool> {
            Some(self)
        }
    }
    impl StreamingTool for SlowOpenStreamingTool {
        fn open_stream(&self, _args: JsonValue) -> Result<StreamingToolOpen, RegistryToolError> {
            std::thread::sleep(self.open_delay);
            struct SlowOneChunkReader {
                chunk_delay: Duration,
                delivered: bool,
            }
            impl IncrementalBytes for SlowOneChunkReader {
                fn total_bytes_hint(&self) -> Option<u64> {
                    Some(1)
                }
                fn next_chunk(
                    &mut self,
                    _max_len: usize,
                ) -> Result<Option<Vec<u8>>, RegistryToolError> {
                    if self.delivered {
                        return Ok(None);
                    }
                    std::thread::sleep(self.chunk_delay);
                    self.delivered = true;
                    Ok(Some(b"x".to_vec()))
                }
            }
            Ok(StreamingToolOpen {
                metadata: serde_json::json!({}),
                field_name: "contents",
                reader: Box::new(SlowOneChunkReader {
                    chunk_delay: self.chunk_delay,
                    delivered: false,
                }),
            })
        }
    }

    /// A test-only tool that blocks the calling (blocking-pool) thread for
    /// a configurable duration, so deadline/cancellation paths have
    /// something to actually race against.
    struct SlowTool {
        sleep: Duration,
    }
    impl Tool for SlowTool {
        fn name(&self) -> &str {
            "test.slow"
        }
        fn description(&self) -> &str {
            "sleeps, for tests"
        }
        fn input_schema(&self) -> JsonValue {
            serde_json::json!({})
        }
        fn invoke(&self, _args: JsonValue) -> Result<JsonValue, RegistryToolError> {
            std::thread::sleep(self.sleep);
            Ok(serde_json::json!({"slept_ms": self.sleep.as_millis()}))
        }
    }

    /// A test-only tool that returns a result of a configurable size, for
    /// exercising `RESULT_TOO_LARGE`.
    struct BigResultTool {
        result_bytes: usize,
    }
    impl Tool for BigResultTool {
        fn name(&self) -> &str {
            "test.big_result"
        }
        fn description(&self) -> &str {
            "returns a large result, for tests"
        }
        fn input_schema(&self) -> JsonValue {
            serde_json::json!({})
        }
        fn invoke(&self, _args: JsonValue) -> Result<JsonValue, RegistryToolError> {
            Ok(serde_json::json!({ "data": "x".repeat(self.result_bytes) }))
        }
    }

    /// A test-only tool that always fails, for exercising `BATCH`'s
    /// partial-failure handling.
    struct FailingTool;
    impl Tool for FailingTool {
        fn name(&self) -> &str {
            "test.failing"
        }
        fn description(&self) -> &str {
            "always fails, for tests"
        }
        fn input_schema(&self) -> JsonValue {
            serde_json::json!({})
        }
        fn invoke(&self, _args: JsonValue) -> Result<JsonValue, RegistryToolError> {
            Err(RegistryToolError::Failed(
                "deliberate test failure".to_string(),
            ))
        }
    }

    async fn connected_pair_with_registry(
        registry: Arc<ToolRegistry>,
        config: ConnectionConfig,
    ) -> (UnixStream, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("connection-test.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let client = UnixStream::connect(&path).await.unwrap();
        let (server_side, _addr) = listener.accept().await.unwrap();
        tokio::spawn(run_connection(server_side, registry, config));
        (client, dir)
    }

    async fn send_raw(stream: &mut UnixStream, frame: Frame) {
        wire::write_frame(stream, &frame).await.unwrap();
    }

    async fn recv_raw(stream: &mut UnixStream) -> Frame {
        read_frame(stream, FrameLimits::default())
            .await
            .unwrap()
            .unwrap()
    }

    async fn do_hello(stream: &mut UnixStream) {
        let hello = Hello {
            protocol_version: AST_PROTOCOL_VERSION,
            client_name: "test".to_string(),
        };
        send_raw(
            stream,
            Frame {
                msg_type: MessageType::Hello as u8,
                correlation_id: 0,
                payload: codec::encode(&hello).unwrap(),
            },
        )
        .await;
        let ack_frame = recv_raw(stream).await;
        assert_eq!(
            MessageType::from_u8(ack_frame.msg_type),
            Some(MessageType::HelloAck)
        );
        let ack: HelloAck = codec::decode(&ack_frame.payload).unwrap();
        assert!(ack.ok);
    }

    fn registry_with(tools: Vec<Box<dyn Tool>>) -> Arc<ToolRegistry> {
        let mut registry = ToolRegistry::new();
        for tool in tools {
            registry.register(tool);
        }
        Arc::new(registry)
    }

    #[tokio::test]
    async fn genuine_stream_deadline_covers_open_and_chunk_reading_as_one_shared_budget() {
        // open_stream takes ~150ms; the first chunk read takes ~180ms;
        // deadline_ms is 200. With one shared budget the open eats 150ms,
        // leaving ~50ms for the read, so the read is cut off and the client
        // gets a terminal ERROR at ~200ms. If each phase instead got a fresh
        // 200ms (the bug), the read would finish at ~330ms with a normal
        // STREAM_END — so both the frame type and the timing discriminate.
        let registry = registry_with(vec![Box::new(SlowOpenStreamingTool {
            open_delay: Duration::from_millis(50),
            chunk_delay: Duration::from_millis(400),
        })]);
        let (mut client, _dir) =
            connected_pair_with_registry(registry, ConnectionConfig::default()).await;
        do_hello(&mut client).await;

        let invoke = Invoke {
            tool: "test.slow_open_stream".to_string(),
            args: serde_json::json!({}),
            deadline_ms: Some(200),
            stream: Some(true),
        };
        let sent_at = std::time::Instant::now();
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Invoke as u8,
                correlation_id: 1,
                payload: codec::encode(&invoke).unwrap(),
            },
        )
        .await;

        let start_frame = recv_raw(&mut client).await;
        assert_eq!(
            MessageType::from_u8(start_frame.msg_type),
            Some(MessageType::StreamStart)
        );

        let terminal = tokio::time::timeout(Duration::from_millis(1000), recv_raw(&mut client))
            .await
            .expect("a terminal frame must arrive well before the test's own timeout");
        let elapsed = sent_at.elapsed();
        assert_eq!(
            MessageType::from_u8(terminal.msg_type),
            Some(MessageType::Error),
            "the open and read phases must share one 200ms budget: the read should be \
             cut off by the deadline, not complete with STREAM_END ({elapsed:?} elapsed)"
        );
        assert!(
            elapsed < Duration::from_millis(300),
            "terminal frame took {elapsed:?} since the request was sent — the open and \
             chunk-reading phases are not sharing one deadline budget"
        );
    }

    #[tokio::test]
    async fn genuine_stream_deadline_exceeded_mid_transfer_sends_a_terminal_error() {
        // A reader whose first next_chunk() call blocks past the
        // deadline — proves a mid-transfer deadline now sends a real
        // ERROR frame instead of just going silent.
        struct ForeverPendingReader;
        impl IncrementalBytes for ForeverPendingReader {
            fn total_bytes_hint(&self) -> Option<u64> {
                Some(1)
            }
            fn next_chunk(
                &mut self,
                _max_len: usize,
            ) -> Result<Option<Vec<u8>>, RegistryToolError> {
                std::thread::sleep(Duration::from_secs(2));
                Ok(None)
            }
        }
        struct ForeverPendingTool;
        impl Tool for ForeverPendingTool {
            fn name(&self) -> &str {
                "test.forever_pending_stream"
            }
            fn description(&self) -> &str {
                "a streaming tool whose first chunk read never returns in time, for tests"
            }
            fn input_schema(&self) -> JsonValue {
                serde_json::json!({})
            }
            fn invoke(&self, _args: JsonValue) -> Result<JsonValue, RegistryToolError> {
                Ok(serde_json::json!({"contents": ""}))
            }
            fn as_streaming(&self) -> Option<&dyn StreamingTool> {
                Some(self)
            }
        }
        impl StreamingTool for ForeverPendingTool {
            fn open_stream(
                &self,
                _args: JsonValue,
            ) -> Result<StreamingToolOpen, RegistryToolError> {
                Ok(StreamingToolOpen {
                    metadata: serde_json::json!({}),
                    field_name: "contents",
                    reader: Box::new(ForeverPendingReader),
                })
            }
        }

        let registry = registry_with(vec![Box::new(ForeverPendingTool)]);
        let (mut client, _dir) =
            connected_pair_with_registry(registry, ConnectionConfig::default()).await;
        do_hello(&mut client).await;

        let invoke = Invoke {
            tool: "test.forever_pending_stream".to_string(),
            args: serde_json::json!({}),
            deadline_ms: Some(100),
            stream: Some(true),
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Invoke as u8,
                correlation_id: 1,
                payload: codec::encode(&invoke).unwrap(),
            },
        )
        .await;

        let start_frame = tokio::time::timeout(Duration::from_secs(2), recv_raw(&mut client))
            .await
            .expect("must receive STREAM_START promptly");
        assert_eq!(
            MessageType::from_u8(start_frame.msg_type),
            Some(MessageType::StreamStart)
        );

        let resp = tokio::time::timeout(Duration::from_secs(2), recv_raw(&mut client))
            .await
            .expect("must receive a terminal frame, not hang until this test's own timeout");
        assert_eq!(
            MessageType::from_u8(resp.msg_type),
            Some(MessageType::Error),
            "a mid-transfer deadline must send a terminal ERROR frame, not silently stop"
        );
        assert_eq!(resp.correlation_id, 1);
        let err: ErrorPayload = codec::decode(&resp.payload).unwrap();
        assert_eq!(err.code, ErrorCode::DeadlineExceeded);
    }

    #[tokio::test]
    async fn rejects_a_non_hello_first_message() {
        let registry = registry_with(vec![Box::new(SystemInfoTool)]);
        let (mut client, _dir) =
            connected_pair_with_registry(registry, ConnectionConfig::default()).await;

        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Ping as u8,
                correlation_id: 5,
                payload: codec::encode(&Ping {}).unwrap(),
            },
        )
        .await;
        let resp = recv_raw(&mut client).await;
        assert_eq!(
            MessageType::from_u8(resp.msg_type),
            Some(MessageType::Error)
        );
        let err: ErrorPayload = codec::decode(&resp.payload).unwrap();
        assert_eq!(err.code, ErrorCode::InvalidMessage);
    }

    #[tokio::test]
    async fn rejects_an_unsupported_protocol_version() {
        let registry = registry_with(vec![Box::new(SystemInfoTool)]);
        let (mut client, _dir) =
            connected_pair_with_registry(registry, ConnectionConfig::default()).await;

        let hello = Hello {
            protocol_version: 99,
            client_name: "test".to_string(),
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Hello as u8,
                correlation_id: 1,
                payload: codec::encode(&hello).unwrap(),
            },
        )
        .await;
        let resp = recv_raw(&mut client).await;
        let ack: HelloAck = codec::decode(&resp.payload).unwrap();
        assert!(!ack.ok);
    }

    #[tokio::test]
    async fn duplicate_in_flight_correlation_id_is_rejected() {
        let registry = registry_with(vec![Box::new(SlowTool {
            sleep: Duration::from_millis(300),
        })]);
        let (mut client, _dir) =
            connected_pair_with_registry(registry, ConnectionConfig::default()).await;
        do_hello(&mut client).await;

        let invoke = Invoke {
            tool: "test.slow".to_string(),
            args: serde_json::json!({}),
            deadline_ms: Some(5000),
            stream: None,
        };
        let payload = codec::encode(&invoke).unwrap();
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Invoke as u8,
                correlation_id: 7,
                payload: payload.clone(),
            },
        )
        .await;
        // Same correlation_id again, while the first is still in flight
        // (it sleeps 300ms; this second send happens immediately after).
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Invoke as u8,
                correlation_id: 7,
                payload,
            },
        )
        .await;

        let resp = recv_raw(&mut client).await;
        assert_eq!(
            MessageType::from_u8(resp.msg_type),
            Some(MessageType::Error)
        );
        let err: ErrorPayload = codec::decode(&resp.payload).unwrap();
        assert_eq!(err.code, ErrorCode::InvalidMessage);

        // The original (first) request still completes normally afterward.
        let resp2 = recv_raw(&mut client).await;
        assert_eq!(
            MessageType::from_u8(resp2.msg_type),
            Some(MessageType::Result)
        );
    }

    #[tokio::test]
    async fn deadline_exceeded_is_reported_and_does_not_hang() {
        let registry = registry_with(vec![Box::new(SlowTool {
            sleep: Duration::from_secs(5),
        })]);
        let (mut client, _dir) =
            connected_pair_with_registry(registry, ConnectionConfig::default()).await;
        do_hello(&mut client).await;

        let invoke = Invoke {
            tool: "test.slow".to_string(),
            args: serde_json::json!({}),
            deadline_ms: Some(50),
            stream: None,
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Invoke as u8,
                correlation_id: 1,
                payload: codec::encode(&invoke).unwrap(),
            },
        )
        .await;

        let resp = tokio::time::timeout(Duration::from_secs(2), recv_raw(&mut client))
            .await
            .unwrap();
        assert_eq!(
            MessageType::from_u8(resp.msg_type),
            Some(MessageType::Error)
        );
        let err: ErrorPayload = codec::decode(&resp.payload).unwrap();
        assert_eq!(err.code, ErrorCode::DeadlineExceeded);
    }

    #[tokio::test]
    async fn cancel_of_an_in_flight_request_reports_cancelled() {
        let registry = registry_with(vec![Box::new(SlowTool {
            sleep: Duration::from_secs(5),
        })]);
        let (mut client, _dir) =
            connected_pair_with_registry(registry, ConnectionConfig::default()).await;
        do_hello(&mut client).await;

        let invoke = Invoke {
            tool: "test.slow".to_string(),
            args: serde_json::json!({}),
            deadline_ms: Some(10_000),
            stream: None,
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Invoke as u8,
                correlation_id: 1,
                payload: codec::encode(&invoke).unwrap(),
            },
        )
        .await;

        tokio::time::sleep(Duration::from_millis(50)).await; // let it actually start
        let cancel = Cancel {
            target_correlation_id: 1,
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Cancel as u8,
                correlation_id: 2,
                payload: codec::encode(&cancel).unwrap(),
            },
        )
        .await;

        let resp = tokio::time::timeout(Duration::from_secs(2), recv_raw(&mut client))
            .await
            .unwrap();
        assert_eq!(
            MessageType::from_u8(resp.msg_type),
            Some(MessageType::Error)
        );
        let err: ErrorPayload = codec::decode(&resp.payload).unwrap();
        assert_eq!(err.code, ErrorCode::Cancelled);
    }

    #[tokio::test]
    async fn cancel_of_an_unknown_id_is_silently_ignored() {
        let registry = registry_with(vec![Box::new(SystemInfoTool)]);
        let (mut client, _dir) =
            connected_pair_with_registry(registry, ConnectionConfig::default()).await;
        do_hello(&mut client).await;

        let cancel = Cancel {
            target_correlation_id: 999,
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Cancel as u8,
                correlation_id: 1,
                payload: codec::encode(&cancel).unwrap(),
            },
        )
        .await;

        // Prove the connection is still alive and serving normally: a
        // subsequent, unrelated PING still gets its PONG.
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Ping as u8,
                correlation_id: 2,
                payload: codec::encode(&Ping {}).unwrap(),
            },
        )
        .await;
        let resp = recv_raw(&mut client).await;
        assert_eq!(MessageType::from_u8(resp.msg_type), Some(MessageType::Pong));
    }

    #[tokio::test]
    async fn exceeding_max_in_flight_reports_resource_exhausted() {
        let registry = registry_with(vec![Box::new(SlowTool {
            sleep: Duration::from_millis(500),
        })]);
        let config = ConnectionConfig {
            max_in_flight: 1,
            ..Default::default()
        };
        let (mut client, _dir) = connected_pair_with_registry(registry, config).await;
        do_hello(&mut client).await;

        let invoke = Invoke {
            tool: "test.slow".to_string(),
            args: serde_json::json!({}),
            deadline_ms: Some(5000),
            stream: None,
        };
        let payload = codec::encode(&invoke).unwrap();
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Invoke as u8,
                correlation_id: 1,
                payload: payload.clone(),
            },
        )
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await; // let the first one actually occupy the one slot
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Invoke as u8,
                correlation_id: 2,
                payload,
            },
        )
        .await;

        let resp = recv_raw(&mut client).await;
        assert_eq!(
            MessageType::from_u8(resp.msg_type),
            Some(MessageType::Error)
        );
        let err: ErrorPayload = codec::decode(&resp.payload).unwrap();
        assert_eq!(err.code, ErrorCode::ResourceExhausted);
    }

    #[tokio::test]
    async fn result_exceeding_max_frame_len_is_bounded_not_sent_oversized() {
        let registry = registry_with(vec![Box::new(BigResultTool { result_bytes: 1000 })]);
        // Large enough for HELLO_ACK itself to fit (it carries
        // `server_name` and is sent before this test's real assertion),
        // small enough that `BigResultTool`'s ~1000-byte result does not.
        let config = ConnectionConfig {
            frame_limits: FrameLimits { max_frame_len: 200 },
            ..Default::default()
        };
        let (mut client, _dir) = connected_pair_with_registry(registry, config).await;
        do_hello(&mut client).await;

        let invoke = Invoke {
            tool: "test.big_result".to_string(),
            args: serde_json::json!({}),
            deadline_ms: Some(5000),
            stream: None,
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Invoke as u8,
                correlation_id: 1,
                payload: codec::encode(&invoke).unwrap(),
            },
        )
        .await;

        // Bound the read itself: if the implementation regressed and
        // actually tried to send the 1000+-byte oversized frame while this
        // test's own reader is capped at 64 bytes too, the read would
        // error out (rather than this test hanging), which is still a
        // clear failure signal — but asserting on the *received* error is
        // the direct proof the fix works, not just that nothing crashed.
        let resp = recv_raw(&mut client).await;
        assert_eq!(
            MessageType::from_u8(resp.msg_type),
            Some(MessageType::Error)
        );
        let err: ErrorPayload = codec::decode(&resp.payload).unwrap();
        assert_eq!(err.code, ErrorCode::ResultTooLarge);
    }

    #[tokio::test]
    async fn result_within_max_frame_len_is_sent_normally() {
        let registry = registry_with(vec![Box::new(BigResultTool { result_bytes: 10 })]);
        let config = ConnectionConfig {
            frame_limits: FrameLimits {
                max_frame_len: 4096,
            },
            ..Default::default()
        };
        let (mut client, _dir) = connected_pair_with_registry(registry, config).await;
        do_hello(&mut client).await;

        let invoke = Invoke {
            tool: "test.big_result".to_string(),
            args: serde_json::json!({}),
            deadline_ms: Some(5000),
            stream: None,
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Invoke as u8,
                correlation_id: 1,
                payload: codec::encode(&invoke).unwrap(),
            },
        )
        .await;

        let resp = recv_raw(&mut client).await;
        assert_eq!(
            MessageType::from_u8(resp.msg_type),
            Some(MessageType::Result)
        );
        let result: InvokeResult = codec::decode(&resp.payload).unwrap();
        assert_eq!(result.value["data"], "x".repeat(10));
    }

    #[tokio::test]
    async fn unknown_message_type_byte_is_reported_as_invalid_frame() {
        let registry = registry_with(vec![Box::new(SystemInfoTool)]);
        let (mut client, _dir) =
            connected_pair_with_registry(registry, ConnectionConfig::default()).await;
        do_hello(&mut client).await;

        send_raw(
            &mut client,
            Frame {
                msg_type: 250,
                correlation_id: 1,
                payload: vec![],
            },
        )
        .await;
        let resp = recv_raw(&mut client).await;
        let err: ErrorPayload = codec::decode(&resp.payload).unwrap();
        assert_eq!(err.code, ErrorCode::InvalidFrame);
    }

    #[tokio::test]
    async fn peer_policy_same_uid_accepts_our_own_connection() {
        // We can't easily simulate a *different* uid connecting in a unit
        // test without running as another user, but this confirms the
        // policy doesn't reject the common, legitimate case: a connection
        // from the same user the server itself runs as.
        let registry = registry_with(vec![Box::new(SystemInfoTool)]);
        let config = ConnectionConfig {
            peer_policy: PeerPolicy::SameUid,
            ..Default::default()
        };
        let (mut client, _dir) = connected_pair_with_registry(registry, config).await;
        do_hello(&mut client).await; // would hang/fail if the connection were rejected pre-handshake
    }

    #[tokio::test]
    async fn batch_runs_all_calls_and_correlates_results_by_id() {
        let registry = registry_with(vec![Box::new(SystemInfoTool)]);
        let (mut client, _dir) =
            connected_pair_with_registry(registry, ConnectionConfig::default()).await;
        do_hello(&mut client).await;

        let batch = Batch {
            calls: vec![
                BatchInvoke {
                    id: 10,
                    tool: "system.info".to_string(),
                    args: serde_json::json!({}),
                    deadline_ms: None,
                },
                BatchInvoke {
                    id: 20,
                    tool: "system.info".to_string(),
                    args: serde_json::json!({}),
                    deadline_ms: None,
                },
            ],
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Batch as u8,
                correlation_id: 1,
                payload: codec::encode(&batch).unwrap(),
            },
        )
        .await;

        let resp = recv_raw(&mut client).await;
        assert_eq!(
            MessageType::from_u8(resp.msg_type),
            Some(MessageType::BatchResult)
        );
        let result: BatchResult = codec::decode(&resp.payload).unwrap();
        assert_eq!(result.results.len(), 2);
        assert_eq!(result.results[0].id, 10);
        assert_eq!(result.results[1].id, 20);
        for entry in &result.results {
            assert!(matches!(entry.outcome, BatchOutcome::Ok { .. }));
        }
    }

    #[tokio::test]
    async fn batch_partial_failure_does_not_stop_remaining_calls() {
        let registry = registry_with(vec![Box::new(SystemInfoTool), Box::new(FailingTool)]);
        let (mut client, _dir) =
            connected_pair_with_registry(registry, ConnectionConfig::default()).await;
        do_hello(&mut client).await;

        let batch = Batch {
            calls: vec![
                BatchInvoke {
                    id: 1,
                    tool: "test.failing".to_string(),
                    args: serde_json::json!({}),
                    deadline_ms: None,
                },
                BatchInvoke {
                    id: 2,
                    tool: "system.info".to_string(),
                    args: serde_json::json!({}),
                    deadline_ms: None,
                },
            ],
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Batch as u8,
                correlation_id: 1,
                payload: codec::encode(&batch).unwrap(),
            },
        )
        .await;

        let resp = recv_raw(&mut client).await;
        let result: BatchResult = codec::decode(&resp.payload).unwrap();
        assert_eq!(result.results.len(), 2);
        assert!(matches!(
            result.results[0].outcome,
            BatchOutcome::Error { .. }
        ));
        // The second call still ran, despite the first one failing.
        assert!(matches!(result.results[1].outcome, BatchOutcome::Ok { .. }));
    }

    #[tokio::test]
    async fn batch_exceeding_max_batch_size_is_rejected() {
        let registry = registry_with(vec![Box::new(SystemInfoTool)]);
        let config = ConnectionConfig {
            max_batch_size: 2,
            ..Default::default()
        };
        let (mut client, _dir) = connected_pair_with_registry(registry, config).await;
        do_hello(&mut client).await;

        let batch = Batch {
            calls: (0..3)
                .map(|id| BatchInvoke {
                    id,
                    tool: "system.info".to_string(),
                    args: serde_json::json!({}),
                    deadline_ms: None,
                })
                .collect(),
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Batch as u8,
                correlation_id: 1,
                payload: codec::encode(&batch).unwrap(),
            },
        )
        .await;

        let resp = recv_raw(&mut client).await;
        assert_eq!(
            MessageType::from_u8(resp.msg_type),
            Some(MessageType::Error)
        );
        let err: ErrorPayload = codec::decode(&resp.payload).unwrap();
        assert_eq!(err.code, ErrorCode::InvalidMessage);
    }

    #[tokio::test]
    async fn cancel_of_an_in_flight_batch_stops_remaining_calls() {
        let registry = registry_with(vec![Box::new(SlowTool {
            sleep: Duration::from_secs(5),
        })]);
        let (mut client, _dir) =
            connected_pair_with_registry(registry, ConnectionConfig::default()).await;
        do_hello(&mut client).await;

        let batch = Batch {
            calls: vec![
                BatchInvoke {
                    id: 1,
                    tool: "test.slow".to_string(),
                    args: serde_json::json!({}),
                    deadline_ms: Some(10_000),
                },
                BatchInvoke {
                    id: 2,
                    tool: "test.slow".to_string(),
                    args: serde_json::json!({}),
                    deadline_ms: Some(10_000),
                },
            ],
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Batch as u8,
                correlation_id: 1,
                payload: codec::encode(&batch).unwrap(),
            },
        )
        .await;

        tokio::time::sleep(Duration::from_millis(50)).await; // let the first call start
        let cancel = Cancel {
            target_correlation_id: 1,
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Cancel as u8,
                correlation_id: 2,
                payload: codec::encode(&cancel).unwrap(),
            },
        )
        .await;

        let resp = tokio::time::timeout(Duration::from_secs(2), recv_raw(&mut client))
            .await
            .unwrap();
        assert_eq!(
            MessageType::from_u8(resp.msg_type),
            Some(MessageType::BatchResult)
        );
        let result: BatchResult = codec::decode(&resp.payload).unwrap();
        // Only the call that was actually running when CANCEL arrived is
        // in the result, marked Cancelled; the second call never started.
        assert_eq!(result.results.len(), 1);
        assert_eq!(result.results[0].id, 1);
        assert!(matches!(
            result.results[0].outcome,
            BatchOutcome::Error {
                code: ErrorCode::Cancelled,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn duplicate_batch_correlation_id_is_rejected() {
        let registry = registry_with(vec![Box::new(SlowTool {
            sleep: Duration::from_millis(500),
        })]);
        let (mut client, _dir) =
            connected_pair_with_registry(registry, ConnectionConfig::default()).await;
        do_hello(&mut client).await;

        let batch = Batch {
            calls: vec![BatchInvoke {
                id: 1,
                tool: "test.slow".to_string(),
                args: serde_json::json!({}),
                deadline_ms: Some(10_000),
            }],
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Batch as u8,
                correlation_id: 1,
                payload: codec::encode(&batch).unwrap(),
            },
        )
        .await;
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Batch as u8,
                correlation_id: 1,
                payload: codec::encode(&batch).unwrap(),
            },
        )
        .await;

        let resp = recv_raw(&mut client).await;
        assert_eq!(
            MessageType::from_u8(resp.msg_type),
            Some(MessageType::Error)
        );
        let err: ErrorPayload = codec::decode(&resp.payload).unwrap();
        assert_eq!(err.code, ErrorCode::InvalidMessage);
    }

    #[tokio::test]
    async fn streamed_invoke_of_a_small_result_reassembles_to_the_same_value() {
        let registry = registry_with(vec![Box::new(SystemInfoTool)]);
        let (mut client, _dir) =
            connected_pair_with_registry(registry, ConnectionConfig::default()).await;
        do_hello(&mut client).await;

        let invoke = Invoke {
            tool: "system.info".to_string(),
            args: serde_json::json!({}),
            deadline_ms: None,
            stream: Some(true),
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Invoke as u8,
                correlation_id: 1,
                payload: codec::encode(&invoke).unwrap(),
            },
        )
        .await;

        let start_frame = recv_raw(&mut client).await;
        assert_eq!(
            MessageType::from_u8(start_frame.msg_type),
            Some(MessageType::StreamStart)
        );
        let start: StreamStart = codec::decode(&start_frame.payload).unwrap();
        assert_eq!(start.total_chunks, 1); // system.info's result is tiny

        let mut encoded = Vec::new();
        for _ in 0..start.total_chunks {
            let frame = recv_raw(&mut client).await;
            assert_eq!(
                MessageType::from_u8(frame.msg_type),
                Some(MessageType::StreamChunk)
            );
            let chunk: StreamChunk = codec::decode(&frame.payload).unwrap();
            encoded.extend_from_slice(&chunk.data);
        }
        assert_eq!(encoded.len(), start.total_bytes as usize);

        let end_frame = recv_raw(&mut client).await;
        assert_eq!(
            MessageType::from_u8(end_frame.msg_type),
            Some(MessageType::StreamEnd)
        );

        let value: JsonValue = codec::decode(&encoded).unwrap();
        assert_eq!(value["os"], std::env::consts::OS);
    }

    #[tokio::test]
    async fn streamed_invoke_of_a_large_result_splits_into_several_chunks_in_order() {
        let registry = registry_with(vec![Box::new(BigResultTool { result_bytes: 1000 })]);
        let config = ConnectionConfig {
            stream_chunk_bytes: 64, // forces many chunks for a 1000+-byte result
            ..Default::default()
        };
        let (mut client, _dir) = connected_pair_with_registry(registry, config).await;
        do_hello(&mut client).await;

        let invoke = Invoke {
            tool: "test.big_result".to_string(),
            args: serde_json::json!({}),
            deadline_ms: None,
            stream: Some(true),
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Invoke as u8,
                correlation_id: 1,
                payload: codec::encode(&invoke).unwrap(),
            },
        )
        .await;

        let start_frame = recv_raw(&mut client).await;
        let start: StreamStart = codec::decode(&start_frame.payload).unwrap();
        assert!(
            start.total_chunks > 1,
            "a result this much bigger than the 64-byte chunk size should need several chunks"
        );

        let mut encoded = Vec::new();
        for expected_sequence in 0..start.total_chunks {
            let frame = recv_raw(&mut client).await;
            assert_eq!(
                MessageType::from_u8(frame.msg_type),
                Some(MessageType::StreamChunk)
            );
            let chunk: StreamChunk = codec::decode(&frame.payload).unwrap();
            assert_eq!(chunk.sequence, expected_sequence);
            encoded.extend_from_slice(&chunk.data);
        }
        let end_frame = recv_raw(&mut client).await;
        assert_eq!(
            MessageType::from_u8(end_frame.msg_type),
            Some(MessageType::StreamEnd)
        );

        let value: JsonValue = codec::decode(&encoded).unwrap();
        assert_eq!(value["data"], "x".repeat(1000));
    }

    #[tokio::test]
    async fn non_streamed_invoke_is_unaffected_by_a_small_stream_chunk_bytes_config() {
        // Proves `stream: None`/`Some(false)` still gets one plain
        // RESULT even when the connection's configured chunk size is
        // tiny — streaming is opt-in per INVOKE, never automatic.
        let registry = registry_with(vec![Box::new(BigResultTool { result_bytes: 1000 })]);
        let config = ConnectionConfig {
            stream_chunk_bytes: 64,
            ..Default::default()
        };
        let (mut client, _dir) = connected_pair_with_registry(registry, config).await;
        do_hello(&mut client).await;

        let invoke = Invoke {
            tool: "test.big_result".to_string(),
            args: serde_json::json!({}),
            deadline_ms: None,
            stream: None,
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Invoke as u8,
                correlation_id: 1,
                payload: codec::encode(&invoke).unwrap(),
            },
        )
        .await;

        let frame = recv_raw(&mut client).await;
        assert_eq!(
            MessageType::from_u8(frame.msg_type),
            Some(MessageType::Result)
        );
        let result: InvokeResult = codec::decode(&frame.payload).unwrap();
        assert_eq!(result.value["data"], "x".repeat(1000));
    }

    #[test]
    fn effective_stream_chunk_bytes_never_exceeds_the_frame_limit() {
        // A generously large configured chunk size is clamped down to
        // fit comfortably under a small max_frame_len...
        assert!(effective_stream_chunk_bytes(1_000_000, 256) < 256);
        // ...while a small configured chunk size under a large
        // max_frame_len is left alone.
        assert_eq!(
            effective_stream_chunk_bytes(1024, wire::DEFAULT_MAX_FRAME_LEN),
            1024
        );
    }

    #[tokio::test]
    async fn idle_timeout_re_arms_after_an_in_flight_request_completes() {
        // Proves the specific bug: idle_timeout must restart counting from
        // when the connection actually becomes idle, not just skip applying
        // entirely because a request was in flight when the wait began.
        let registry = registry_with(vec![Box::new(SlowTool {
            sleep: Duration::from_millis(50),
        })]);
        let config = ConnectionConfig {
            idle_timeout: Duration::from_millis(150),
            ..Default::default()
        };
        let (mut client, _dir) = connected_pair_with_registry(registry, config).await;
        do_hello(&mut client).await;

        let invoke = Invoke {
            tool: "test.slow".to_string(),
            args: serde_json::json!({}),
            deadline_ms: Some(5_000),
            stream: None,
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Invoke as u8,
                correlation_id: 1,
                payload: codec::encode(&invoke).unwrap(),
            },
        )
        .await;

        // Consume the RESULT so the in-flight set becomes empty, then send
        // nothing further — the connection should close on its own well
        // within a generous test timeout, proving idle_timeout re-armed
        // after the request completed rather than waiting forever.
        let resp = recv_raw(&mut client).await;
        assert_eq!(
            MessageType::from_u8(resp.msg_type),
            Some(MessageType::Result)
        );

        let closed = tokio::time::timeout(Duration::from_secs(2), async {
            wire::read_frame(&mut client, FrameLimits::default()).await
        })
        .await
        .expect("idle_timeout must re-arm once the in-flight request completes, not hang forever");
        assert!(matches!(closed, Ok(None)));
    }

    #[tokio::test]
    async fn idle_connection_is_closed_after_the_configured_timeout() {
        // A connected-but-silent peer (past HELLO, never sends another
        // frame) must not hold its connection slot forever — proves
        // `idle_timeout` actually closes the connection, not just that
        // the config field exists.
        let registry = registry_with(vec![Box::new(SystemInfoTool)]);
        let config = ConnectionConfig {
            idle_timeout: Duration::from_millis(50),
            ..Default::default()
        };
        let (mut client, _dir) = connected_pair_with_registry(registry, config).await;
        do_hello(&mut client).await;

        // Send nothing further; the server should close the connection
        // on its own well within this test's own generous timeout.
        let closed = tokio::time::timeout(Duration::from_secs(2), async {
            wire::read_frame(&mut client, FrameLimits::default()).await
        })
        .await
        .expect("server should have closed the idle connection, not hung");

        assert!(
            matches!(closed, Ok(None)),
            "expected a clean disconnect from the idle timeout, got {closed:?}"
        );
    }

    #[tokio::test]
    async fn idle_timeout_does_not_fire_while_a_request_is_still_in_flight() {
        // A client that sent a long-running INVOKE and is legitimately
        // waiting out its own deadline isn't idle, even though it has
        // no reason to send another frame meanwhile — the idle timeout
        // must not disconnect it before the slow tool actually answers.
        let registry = registry_with(vec![Box::new(SlowTool {
            sleep: Duration::from_millis(300),
        })]);
        let config = ConnectionConfig {
            idle_timeout: Duration::from_millis(50), // much shorter than the tool's own sleep
            ..Default::default()
        };
        let (mut client, _dir) = connected_pair_with_registry(registry, config).await;
        do_hello(&mut client).await;

        let invoke = Invoke {
            tool: "test.slow".to_string(),
            args: serde_json::json!({}),
            deadline_ms: Some(5_000),
            stream: None,
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Invoke as u8,
                correlation_id: 1,
                payload: codec::encode(&invoke).unwrap(),
            },
        )
        .await;

        // The tool sleeps 300ms, well past the 50ms idle_timeout — if
        // idle_timeout wrongly applied while a request is in flight,
        // the connection would already be closed by the time this
        // response is due.
        let resp = tokio::time::timeout(Duration::from_secs(2), recv_raw(&mut client))
            .await
            .expect("connection was closed by idle_timeout despite an in-flight request");
        assert_eq!(
            MessageType::from_u8(resp.msg_type),
            Some(MessageType::Result)
        );
    }

    #[tokio::test]
    async fn genuine_streaming_invoke_sends_raw_mode_and_reassembles_correctly() {
        let fixture_dir = tempfile::tempdir().unwrap();
        let contents = "hello from genuine streaming\n".repeat(50); // multi-chunk with a small chunk size
        std::fs::write(fixture_dir.path().join("stream.txt"), &contents).unwrap();
        let read_tool = FilesystemReadTool::new(fixture_dir.path()).unwrap();

        let registry = registry_with(vec![Box::new(read_tool)]);
        let config = ConnectionConfig {
            stream_chunk_bytes: 64, // forces several chunks
            ..Default::default()
        };
        let (mut client, _dir) = connected_pair_with_registry(registry, config).await;
        do_hello(&mut client).await;

        let invoke = Invoke {
            tool: "filesystem.read".to_string(),
            args: serde_json::json!({"path": "stream.txt"}),
            deadline_ms: None,
            stream: Some(true),
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Invoke as u8,
                correlation_id: 1,
                payload: codec::encode(&invoke).unwrap(),
            },
        )
        .await;

        let start_frame = recv_raw(&mut client).await;
        assert_eq!(
            MessageType::from_u8(start_frame.msg_type),
            Some(MessageType::StreamStart)
        );
        let start: StreamStart = codec::decode(&start_frame.payload).unwrap();
        let raw = start
            .raw
            .expect("filesystem.read supports genuine streaming — StreamStart.raw must be Some");
        assert_eq!(raw.field_name, "contents");
        assert_eq!(raw.metadata["size_bytes"], contents.len());
        assert!(
            start.total_chunks > 1,
            "a chunk size of 64 against a {}-byte file should need several chunks",
            contents.len()
        );

        let mut received = Vec::new();
        let mut expected_sequence = 0u32;
        loop {
            let frame = recv_raw(&mut client).await;
            match MessageType::from_u8(frame.msg_type) {
                Some(MessageType::StreamChunk) => {
                    let chunk: StreamChunk = codec::decode(&frame.payload).unwrap();
                    assert_eq!(chunk.sequence, expected_sequence);
                    expected_sequence += 1;
                    received.extend_from_slice(&chunk.data);
                }
                Some(MessageType::StreamEnd) => break,
                other => panic!("unexpected message type: {other:?}"),
            }
        }

        // The received bytes are the file's own raw UTF-8 bytes — not a
        // slice of a msgpack-encoded Value — reassembled directly.
        assert_eq!(String::from_utf8(received).unwrap(), contents);
    }

    #[tokio::test]
    async fn genuine_streaming_invoke_respects_cancellation_mid_transfer() {
        // A large-ish file with a tiny chunk size and an artificial
        // per-chunk delay would be the realistic way to prove
        // mid-transfer cancellation actually interrupts an in-progress
        // stream rather than only ever racing a single, already-fast
        // open_stream call — but this crate's StreamingTool has no
        // injectable delay. Instead, this proves the weaker but still
        // real guarantee: CANCEL targeting a stream whose open_stream
        // step is still racing does stop it before any STREAM_START is
        // sent, the same as it would for a non-streaming INVOKE racing
        // run_invoke.
        let fixture_dir = tempfile::tempdir().unwrap();
        std::fs::write(fixture_dir.path().join("f.txt"), "hi\n").unwrap();
        let read_tool = FilesystemReadTool::new(fixture_dir.path()).unwrap();
        let registry = registry_with(vec![Box::new(read_tool)]);
        let (mut client, _dir) =
            connected_pair_with_registry(registry, ConnectionConfig::default()).await;
        do_hello(&mut client).await;

        let invoke = Invoke {
            tool: "filesystem.read".to_string(),
            args: serde_json::json!({"path": "f.txt"}),
            deadline_ms: Some(10_000),
            stream: Some(true),
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Invoke as u8,
                correlation_id: 1,
                payload: codec::encode(&invoke).unwrap(),
            },
        )
        .await;
        let cancel = Cancel {
            target_correlation_id: 1,
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Cancel as u8,
                correlation_id: 2,
                payload: codec::encode(&cancel).unwrap(),
            },
        )
        .await;

        // Either the cancellation wins the race (an ERROR{Cancelled}, no
        // STREAM_START ever sent) or the open was already fast enough to
        // finish first (a normal, complete stream) — both are correct
        // outcomes of a real race; what would NOT be correct is a hang.
        let resp = tokio::time::timeout(Duration::from_secs(2), recv_raw(&mut client))
            .await
            .expect("must not hang regardless of which side of the race wins");
        assert!(matches!(
            MessageType::from_u8(resp.msg_type),
            Some(MessageType::Error) | Some(MessageType::StreamStart)
        ));
    }

    #[tokio::test]
    async fn batch_output_budget_leaves_margin_for_batch_result_framing_overhead() {
        // A single entry whose encoded size sits just under max_frame_len
        // (within BatchResult's own framing overhead of it) must not make
        // the final BatchResult overflow max_frame_len once wrapped — the
        // whole batch would otherwise be replaced by a top-level
        // RESULT_TOO_LARGE ERROR. The framing-overhead margin must turn
        // that single entry into a per-entry ResultTooLarge instead.
        let registry = registry_with(vec![Box::new(BigResultTool { result_bytes: 360 })]);
        let config = ConnectionConfig {
            frame_limits: FrameLimits { max_frame_len: 400 },
            max_batch_output_bytes: 400,
            ..Default::default()
        };
        let (mut client, _dir) = connected_pair_with_registry(registry, config).await;
        do_hello(&mut client).await;

        let batch = Batch {
            calls: vec![BatchInvoke {
                id: 0,
                tool: "test.big_result".to_string(),
                args: serde_json::json!({}),
                deadline_ms: None,
            }],
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Batch as u8,
                correlation_id: 1,
                payload: codec::encode(&batch).unwrap(),
            },
        )
        .await;

        let resp = recv_raw(&mut client).await;
        assert_eq!(
            MessageType::from_u8(resp.msg_type),
            Some(MessageType::BatchResult),
            "the whole batch was rejected outright instead of returning a BatchResult"
        );
    }

    #[tokio::test]
    async fn batch_output_budget_is_checked_per_call_not_after_full_accumulation() {
        // Each call's result is ~1000 bytes; a budget of 1500 lets the
        // first succeed but not the second — proving the server checks
        // incrementally (one result's worth of slack) rather than
        // accumulating every call's result unbounded and only rejecting
        // the whole batch after serializing it all at the end.
        let registry = registry_with(vec![Box::new(BigResultTool { result_bytes: 1000 })]);
        let config = ConnectionConfig {
            max_batch_output_bytes: 1500,
            ..Default::default()
        };
        let (mut client, _dir) = connected_pair_with_registry(registry, config).await;
        do_hello(&mut client).await;

        let batch = Batch {
            calls: (0..3)
                .map(|id| BatchInvoke {
                    id,
                    tool: "test.big_result".to_string(),
                    args: serde_json::json!({}),
                    deadline_ms: None,
                })
                .collect(),
        };
        send_raw(
            &mut client,
            Frame {
                msg_type: MessageType::Batch as u8,
                correlation_id: 1,
                payload: codec::encode(&batch).unwrap(),
            },
        )
        .await;

        let resp = recv_raw(&mut client).await;
        assert_eq!(
            MessageType::from_u8(resp.msg_type),
            Some(MessageType::BatchResult),
            "the batch as a whole must not be rejected outright — only the offending entry"
        );
        let result: BatchResult = codec::decode(&resp.payload).unwrap();

        // Exactly 2 entries: call 0 (succeeded) and call 1 (tipped the
        // budget over, reported as ResultTooLarge in its own slot). Call
        // 2 never ran — absent from results, same as a mid-batch
        // cancellation.
        assert_eq!(result.results.len(), 2);
        assert_eq!(result.results[0].id, 0);
        assert!(matches!(result.results[0].outcome, BatchOutcome::Ok { .. }));
        assert_eq!(result.results[1].id, 1);
        assert!(matches!(
            result.results[1].outcome,
            BatchOutcome::Error {
                code: ErrorCode::ResultTooLarge,
                ..
            }
        ));
    }
}
