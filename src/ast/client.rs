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
}

type Waiters = Arc<Mutex<HashMap<u64, oneshot::Sender<Frame>>>>;

pub struct AstClient {
    tx: mpsc::Sender<Frame>,
    waiters: Waiters,
    next_id: AtomicU64,
    default_timeout: Duration,
    writer_task: JoinHandle<()>,
    reader_task: JoinHandle<()>,
}

pub struct AstClientConfig {
    pub client_name: String,
    pub frame_limits: FrameLimits,
    pub request_timeout: Duration,
}

impl Default for AstClientConfig {
    fn default() -> Self {
        Self {
            client_name: "ast-client-sdk".to_string(),
            frame_limits: FrameLimits::default(),
            request_timeout: Duration::from_secs(10),
        }
    }
}

impl AstClient {
    pub async fn connect(path: &Path, config: AstClientConfig) -> Result<Self, AstClientError> {
        let stream = UnixStream::connect(path).await?;
        let (mut read_half, write_half) = stream.into_split();
        let (tx, mut rx) = mpsc::channel::<Frame>(64);
        let waiters: Waiters = Arc::new(Mutex::new(HashMap::new()));

        let writer_task = tokio::spawn(async move {
            let mut write_half = write_half;
            while let Some(frame) = rx.recv().await {
                if wire::write_frame(&mut write_half, &frame).await.is_err() {
                    return;
                }
            }
        });

        let reader_waiters = Arc::clone(&waiters);
        let frame_limits = config.frame_limits;
        let reader_task = tokio::spawn(async move {
            loop {
                match wire::read_frame(&mut read_half, frame_limits).await {
                    Ok(Some(frame)) => {
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
            next_id: AtomicU64::new(1),
            default_timeout: config.request_timeout,
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
                Err(AstClientError::Timeout)
            }
        }
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
            Some(MessageType::Error) => Err(self.decode_server_error(&frame.payload)?),
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
            Some(MessageType::Error) => Err(self.decode_server_error(&frame.payload)?),
            _ => Err(AstClientError::UnexpectedResponseType(frame.msg_type)),
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
            Some(MessageType::Error) => Err(self.decode_server_error(&frame.payload)?),
            _ => Err(AstClientError::UnexpectedResponseType(frame.msg_type)),
        }
    }

    fn decode_server_error(&self, payload: &[u8]) -> Result<AstClientError, AstClientError> {
        let err: ErrorPayload = codec::decode(payload)?;
        Ok(AstClientError::Server {
            code: err.code,
            message: err.message,
        })
    }

    /// Closes the connection and waits for the background reader/writer
    /// tasks to finish. Dropping an `AstClient` without calling this also
    /// shuts the connection down (the writer task ends once `tx` is
    /// dropped), just without waiting for it.
    pub async fn close(self) {
        drop(self.tx);
        let _ = self.writer_task.await;
        self.reader_task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::connection::{run_connection, ConnectionConfig};
    use crate::tools::{SystemInfoTool, ToolRegistry};

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
}
