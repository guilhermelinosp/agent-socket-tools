use agent_socket_tools::config::default_socket_path;
use agent_socket_tools::protocol::{Operation, Request, Response, PROTOCOL_VERSION};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

/// Connects to an agent-tools-server over its Unix Domain Socket and sends
/// one request.
#[derive(Parser, Debug)]
#[command(name = "agent-tools-client", version)]
struct Cli {
    /// Path to the server's Unix Domain Socket. Defaults to the same
    /// resolution rule the server uses: `$XDG_RUNTIME_DIR/agent-tools.sock`,
    /// falling back to `$TMPDIR/agent-tools.sock`.
    #[arg(long)]
    socket_path: Option<PathBuf>,

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

    match send_request(&socket_path, &request).await {
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
) -> std::io::Result<Response> {
    let stream = UnixStream::connect(socket_path).await?;
    let (read_half, mut write_half) = stream.into_split();

    let mut line = serde_json::to_string(request).expect("Request serializes");
    line.push('\n');
    write_half.write_all(line.as_bytes()).await?;
    write_half.flush().await?;

    let mut reader = BufReader::new(read_half);
    let mut response_line = String::new();
    let n = reader.read_line(&mut response_line).await?;
    if n == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "server closed the connection without responding",
        ));
    }

    serde_json::from_str(&response_line)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}
