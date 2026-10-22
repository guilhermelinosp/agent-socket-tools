use agent_socket_tools::config::{default_socket_path, DEFAULT_MAX_MESSAGE_BYTES};
use agent_socket_tools::tools::{FilesystemReadTool, SystemInfoTool, ToolRegistry};
use agent_socket_tools::transport::{handle_connection, BoundListener};
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Starts the agent-tools server and exposes its tools over a Unix Domain Socket.
#[derive(Parser, Debug)]
#[command(name = "agent-tools-server", version)]
struct Cli {
    /// Path to the Unix Domain Socket to create.
    /// Defaults to `$XDG_RUNTIME_DIR/agent-tools.sock`, falling back to
    /// `$TMPDIR/agent-tools.sock` when XDG_RUNTIME_DIR is unset or not a
    /// directory.
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

    /// Timeout, in seconds, for a single tool invocation.
    #[arg(long, default_value_t = 10)]
    request_timeout_secs: u64,
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
        "agent-tools-server listening"
    );

    run_until_shutdown(listener, registry, cli.max_message_bytes, request_timeout).await;

    tracing::info!("agent-tools-server shut down");
    std::process::ExitCode::SUCCESS
}

async fn run_until_shutdown(
    listener: BoundListener,
    registry: Arc<ToolRegistry>,
    max_message_bytes: usize,
    request_timeout: Duration,
) {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _addr)) => {
                        let registry = Arc::clone(&registry);
                        tokio::spawn(async move {
                            handle_connection(stream, registry, max_message_bytes, request_timeout).await;
                        });
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
