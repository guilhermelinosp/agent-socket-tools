use agent_socket_tools::config::{default_socket_path, DEFAULT_MAX_MESSAGE_BYTES};
use agent_socket_tools::protocol::{Operation, Request, Response, PROTOCOL_VERSION};
use agent_socket_tools::transport::MessageReader;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;

/// Connects to an agent-tools-server over its Unix Domain Socket and sends
/// one request.
///
/// This is a test/debugging CLI, not an SDK: it sends exactly one request
/// per invocation and prints the raw JSON response. An AI agent that wants
/// to speak this protocol natively needs to implement the client side of
/// `docs/PROTOCOL.md` itself (or shell out to this binary); nothing here
/// provides automatic integration with any particular agent framework.
#[derive(Parser, Debug)]
#[command(name = "agent-tools-client", version)]
struct Cli {
    /// Path to the server's Unix Domain Socket. Defaults to the same
    /// resolution rule the server uses.
    #[arg(long)]
    socket_path: Option<PathBuf>,

    /// Maximum size, in bytes, this client will buffer while waiting for
    /// the server's response line. A response larger than this is reported
    /// as a client-side error rather than buffered without bound.
    #[arg(long, default_value_t = DEFAULT_MAX_MESSAGE_BYTES)]
    max_message_bytes: usize,

    /// How long to wait for the server's response before giving up.
    #[arg(long, default_value_t = 10)]
    response_timeout_secs: u64,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Lists the tools the server exposes and their input schemas.
    Discover,
    /// Reports whether the server is reachable and healthy.
    Health,
    /// Invokes a named tool with JSON arguments.
    Invoke {
        /// Dotted tool name, e.g. "system.info".
        #[arg(long)]
        tool: String,
        /// Tool arguments as a JSON object. Defaults to `{}`.
        #[arg(long, default_value = "{}")]
        args: String,
    },
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let socket_path = cli.socket_path.unwrap_or_else(default_socket_path);
    let response_timeout = Duration::from_secs(cli.response_timeout_secs);

    let (op, tool, args) = match cli.command {
        Command::Discover => (Operation::Discover, None, None),
        Command::Health => (Operation::Health, None, None),
        Command::Invoke { tool, args } => {
            let parsed: serde_json::Value = match serde_json::from_str(&args) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("invalid --args JSON: {e}");
                    return std::process::ExitCode::FAILURE;
                }
            };
            (Operation::Invoke, Some(tool), Some(parsed))
        }
    };

    let request = Request {
        version: PROTOCOL_VERSION,
        id: format!("cli-{}", std::process::id()),
        op,
        tool,
        args,
    };

    match send_request(
        &socket_path,
        &request,
        cli.max_message_bytes,
        response_timeout,
    )
    .await
    {
        Ok(response) => {
            let pretty = serde_json::to_string_pretty(&response).expect("Response serializes");
            println!("{pretty}");
            if response.ok {
                std::process::ExitCode::SUCCESS
            } else {
                std::process::ExitCode::FAILURE
            }
        }
        Err(e) => {
            eprintln!("error talking to {socket_path:?}: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn send_request(
    socket_path: &std::path::Path,
    request: &Request,
    max_message_bytes: usize,
    response_timeout: Duration,
) -> std::io::Result<Response> {
    let mut stream = UnixStream::connect(socket_path).await?;

    let mut line = serde_json::to_string(request).expect("Request serializes");
    line.push('\n');
    stream.write_all(line.as_bytes()).await?;
    stream.flush().await?;

    // Reuses the server's own framing logic so the client enforces the
    // same "don't buffer an unbounded response" guarantee, not an
    // unrelated, separately-maintained copy of it.
    let mut reader = MessageReader::new(max_message_bytes);
    let message =
        match tokio::time::timeout(response_timeout, reader.next_message(&mut stream)).await {
            Ok(Ok(Some(line))) => line,
            Ok(Ok(None)) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "server closed the connection without responding",
                ))
            }
            Ok(Err(framing_err)) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{framing_err:?}"),
                ))
            }
            Err(_elapsed) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "no response within the configured timeout",
                ))
            }
        };

    serde_json::from_str(&message)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}
