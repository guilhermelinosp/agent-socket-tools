use agent_socket_tools::config::{
    default_socket_path, DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_MESSAGE_BYTES,
};
use agent_socket_tools::protocol::{ErrorCode, Response};
use agent_socket_tools::tools::{FilesystemReadTool, SystemInfoTool, ToolRegistry};
use agent_socket_tools::transport::{handle_connection, BoundListener, ConnectionLimiter};
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

/// Starts the agent-tools server and exposes its tools over a Unix Domain Socket.
#[derive(Parser, Debug)]
#[command(name = "agent-tools-server", version)]
struct Cli {
    /// Path to the Unix Domain Socket to create.
    /// Defaults to `$XDG_RUNTIME_DIR/agent-socket-tools/agent-tools.sock`,
    /// falling back to `$TMPDIR/agent-socket-tools/agent-tools.sock` when
    /// XDG_RUNTIME_DIR is unset or not a directory. Either way, the parent
    /// directory is created (or validated) as a private, 0700, user-owned
    /// directory before the socket is bound.
    #[arg(long)]
    socket_path: Option<PathBuf>,

    /// Base directory `filesystem.read` is authorized to read from.
    /// Paths requested outside this directory (including via symlinks) are
    /// rejected.
    #[arg(long)]
    read_dir: PathBuf,

    /// Maximum size, in bytes, of a single request or response line.
    #[arg(long, default_value_t = DEFAULT_MAX_MESSAGE_BYTES)]
    max_message_bytes: usize,

    /// Timeout, in seconds, applied independently to (a) receiving one
    /// complete request and (b) running the tool it names. A pathological
    /// request can therefore take up to roughly 2x this value; see
    /// `docs/PROTOCOL.md` for why this isn't split into two separate knobs.
    #[arg(long, default_value_t = 10)]
    request_timeout_secs: u64,

    /// Maximum number of connections served concurrently. A connection
    /// beyond this limit is accepted, sent a bounded
    /// `connection_limit_exceeded` error, and closed immediately — it is
    /// never silently dropped or left queued.
    #[arg(long, default_value_t = DEFAULT_MAX_CONNECTIONS)]
    max_connections: usize,
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
    let socket_path = cli.socket_path.unwrap_or_else(default_socket_path);
    let request_timeout = Duration::from_secs(cli.request_timeout_secs);

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

    tracing::info!(
        path = ?listener.path(),
        tools = registry.discover().len(),
        read_dir = ?cli.read_dir,
        max_connections = cli.max_connections,
        request_timeout_secs = cli.request_timeout_secs,
        "agent-tools-server listening"
    );

    run_until_shutdown(
        listener,
        registry,
        cli.max_message_bytes,
        request_timeout,
        cli.max_connections,
    )
    .await;

    tracing::info!("agent-tools-server shut down");
    std::process::ExitCode::SUCCESS
}

async fn run_until_shutdown(
    listener: BoundListener,
    registry: Arc<ToolRegistry>,
    max_message_bytes: usize,
    request_timeout: Duration,
    max_connections: usize,
) {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");
    let limiter = ConnectionLimiter::new(max_connections);

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((mut stream, _addr)) => {
                        match limiter.try_admit() {
                            Some(permit) => {
                                let registry = Arc::clone(&registry);
                                tokio::spawn(async move {
                                    // `permit` is held for the task's whole
                                    // lifetime and dropped — freeing the
                                    // slot — no matter how the connection
                                    // ends: clean disconnect, timeout, or
                                    // any other error.
                                    let _permit = permit;
                                    handle_connection(stream, registry, max_message_bytes, request_timeout).await;
                                });
                            }
                            None => {
                                tracing::warn!(max_connections, "rejecting connection: limit reached");
                                let resp = Response::err(
                                    "unknown",
                                    ErrorCode::ConnectionLimitExceeded,
                                    format!("server is already serving its configured maximum of {max_connections} connections"),
                                );
                                let _ = stream.write_all(resp.to_line().as_bytes()).await;
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

    // `listener` drops here, which unlinks the socket file (see
    // `BoundListener`'s `Drop` impl) unless another process has since
    // replaced it at the same path.
}
