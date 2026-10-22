use agent_socket_tools::ast::connection::{run_connection, ConnectionConfig, PeerPolicy};
use agent_socket_tools::config::default_ast_socket_path;
use agent_socket_tools::tools::{FilesystemReadTool, SystemInfoTool, ToolRegistry};
use agent_socket_tools::transport::BoundListener;
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Starts an AST (Agent Socket Transport) server.
///
/// Reuses the same private-socket-directory hardening as the legacy
/// `agent-tools-server` — see `src/transport.rs`'s `BoundListener` — just
/// with AST's own framing/dispatch on top instead of JSON-Lines.
#[derive(Parser, Debug)]
#[command(name = "ast-server", version)]
struct Cli {
    /// Defaults to the same private directory as `agent-tools-server`,
    /// but a different filename (`ast.sock`), so both can run at once.
    #[arg(long)]
    socket_path: Option<PathBuf>,

    /// Base directory `filesystem.read` is authorized to read from.
    #[arg(long)]
    read_dir: PathBuf,

    /// Maximum frame size, in bytes, for one AST message (request or
    /// response). See `docs/AST_PROTOCOL.md`'s framing section — this is
    /// checked against the frame's own length field before any buffer
    /// sized to it is allocated.
    #[arg(long, default_value_t = agent_socket_tools::ast::wire::DEFAULT_MAX_FRAME_LEN)]
    max_frame_bytes: u32,

    /// Default deadline, in milliseconds, for one `INVOKE` when the
    /// request itself doesn't specify one. A request-level `deadline_ms`
    /// always overrides this.
    #[arg(long, default_value_t = 10_000)]
    default_deadline_ms: u64,

    /// Maximum concurrent in-flight `INVOKE`s *per connection*. Unlike the
    /// legacy protocol (one request at a time per connection), AST
    /// connections are persistent and multiplex many concurrent requests,
    /// so this bounds that multiplexing rather than the connection count.
    #[arg(long, default_value_t = 64)]
    max_in_flight_per_connection: usize,

    /// Maximum concurrent connections, server-wide.
    #[arg(long, default_value_t = 256)]
    max_connections: usize,

    /// Maximum number of calls in one `BATCH` message. A whole batch
    /// still counts as a single in-flight slot (see
    /// `max_in_flight_per_connection`) regardless of this value.
    #[arg(long, default_value_t = 100)]
    max_batch_size: usize,

    /// Timeout, in seconds, to receive a valid `HELLO` after a connection
    /// is accepted. A peer that doesn't send one in time is disconnected.
    #[arg(long, default_value_t = 5)]
    handshake_timeout_secs: u64,

    /// Only accept connections from processes running as the same user as
    /// this server, checked via `SO_PEERCRED` (Linux) / `LOCAL_PEERCRED`
    /// (macOS) — independent of, and in addition to, the socket file's
    /// own `0600` permissions. See `docs/AST_PROTOCOL.md`'s security
    /// section for why this exists even though the filesystem permissions
    /// already imply it on an unshared machine.
    #[arg(long, default_value_t = true)]
    require_same_uid: bool,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let socket_path = cli.socket_path.unwrap_or_else(default_ast_socket_path);

    let mut registry = ToolRegistry::new();
    registry.register(Box::new(SystemInfoTool));
    match FilesystemReadTool::new(&cli.read_dir) {
        Ok(tool) => registry.register(Box::new(tool)),
        Err(e) => {
            tracing::error!(dir = ?cli.read_dir, error = %e, "cannot authorize read directory");
            return std::process::ExitCode::FAILURE;
        }
    }
    let registry = Arc::new(registry);

    let listener = match BoundListener::bind(&socket_path).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(path = ?socket_path, error = %e, "failed to bind socket");
            return std::process::ExitCode::FAILURE;
        }
    };

    let config = ConnectionConfig {
        frame_limits: agent_socket_tools::ast::wire::FrameLimits {
            max_frame_len: cli.max_frame_bytes,
        },
        max_in_flight: cli.max_in_flight_per_connection,
        default_deadline: Duration::from_millis(cli.default_deadline_ms),
        handshake_timeout: Duration::from_secs(cli.handshake_timeout_secs),
        peer_policy: if cli.require_same_uid {
            PeerPolicy::SameUid
        } else {
            PeerPolicy::AllowAny
        },
        server_name: "agent-socket-tools/ast".to_string(),
        max_batch_size: cli.max_batch_size,
    };

    tracing::info!(
        path = ?listener.path(),
        tools = registry.discover().len(),
        read_dir = ?cli.read_dir,
        max_connections = cli.max_connections,
        max_in_flight_per_connection = cli.max_in_flight_per_connection,
        require_same_uid = cli.require_same_uid,
        "ast-server listening"
    );

    run_until_shutdown(listener, registry, config, cli.max_connections).await;

    tracing::info!("ast-server shut down");
    std::process::ExitCode::SUCCESS
}

async fn run_until_shutdown(
    listener: BoundListener,
    registry: Arc<ToolRegistry>,
    config: ConnectionConfig,
    max_connections: usize,
) {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");
    let limiter = agent_socket_tools::transport::ConnectionLimiter::new(max_connections);

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _addr)) => {
                        match limiter.try_admit() {
                            Some(permit) => {
                                let registry = Arc::clone(&registry);
                                let config = config.clone();
                                tokio::spawn(async move {
                                    let _permit = permit;
                                    run_connection(stream, registry, config).await;
                                });
                            }
                            None => {
                                tracing::warn!(max_connections, "rejecting connection: limit reached");
                                // AST has no "connection accepted but refused"
                                // frame before HELLO — unlike the legacy
                                // protocol's single request/response model,
                                // sending any frame here would require
                                // pretending a handshake happened. The
                                // connection is simply closed; a well-behaved
                                // client sees it close immediately after
                                // connecting and can retry.
                                drop(stream);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to accept connection");
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("received SIGINT, shutting down");
                break;
            }
            _ = terminate.recv() => {
                tracing::info!("received SIGTERM, shutting down");
                break;
            }
        }
    }
}
