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
use std::time::Duration;
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
    pub peer_policy: PeerPolicy,
    pub server_name: String,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            frame_limits: FrameLimits::default(),
            max_in_flight: 64,
            default_deadline: Duration::from_secs(10),
            handshake_timeout: Duration::from_secs(5),
            peer_policy: PeerPolicy::SameUid,
            server_name: "agent-socket-tools/ast".to_string(),
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
        let frame = match wire::read_frame(&mut read_half, config.frame_limits).await {
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
                | MessageType::Pong,
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
) {
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
            Ok(value) => {
                let _ = send(
                    &tx,
                    MessageType::Result,
                    correlation_id,
                    &InvokeResult { value },
                    max_frame_len,
                )
                .await;
            }
            Err((code, message)) => {
                let _ = send_error(&tx, correlation_id, code, &message, max_frame_len).await;
            }
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
    use crate::tools::{SystemInfoTool, Tool, ToolError as RegistryToolError};
    use serde_json::Value as JsonValue;
    use tokio::net::UnixListener;

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
}
