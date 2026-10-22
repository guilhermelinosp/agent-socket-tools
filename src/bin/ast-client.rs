use agent_socket_tools::ast::client::{AstClient, AstClientConfig};
use agent_socket_tools::config::default_ast_socket_path;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::time::Duration;

/// Connects to an `ast-server` and sends requests over a persistent AST
/// connection.
///
/// Like `agent-tools-client`, this is a test/debugging CLI, not an SDK —
/// it's a thin wrapper over [`agent_socket_tools::ast::client::AstClient`],
/// the actual reusable client. A Rust program embedding AST should use
/// that directly; this binary exists to drive it from a shell for manual
/// testing, scripting, and the benchmark harness.
#[derive(Parser, Debug)]
#[command(name = "ast-client", version)]
struct Cli {
    #[arg(long)]
    socket_path: Option<PathBuf>,

    #[arg(long, default_value_t = 10)]
    request_timeout_secs: u64,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Connects, performs the HELLO handshake, and exits — a smoke test.
    Hello,
    /// Measures one PING/PONG round trip.
    Ping,
    /// Lists the server's tools and their input schemas.
    Discover,
    /// Invokes a tool once.
    Invoke {
        #[arg(long)]
        tool: String,
        #[arg(long, default_value = "{}")]
        args: String,
        /// Overrides the server's default deadline for this one call.
        #[arg(long)]
        deadline_ms: Option<u64>,
    },
    /// Runs several tool calls as one BATCH. The server executes them
    /// sequentially, in order — see `docs/AST_PROTOCOL.md`'s "Batch
    /// execution" section.
    Batch {
        /// A JSON array of `{"id": <u64>, "tool": <string>, "args":
        /// <object>, "deadline_ms": <u64, optional>}` objects. `id` must
        /// be unique within this one batch.
        #[arg(long)]
        calls: String,
    },
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let socket_path = cli.socket_path.unwrap_or_else(default_ast_socket_path);
    let config = AstClientConfig {
        client_name: "ast-client-cli".to_string(),
        request_timeout: Duration::from_secs(cli.request_timeout_secs),
        ..Default::default()
    };

    let client = match AstClient::connect(&socket_path, config).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to connect to {socket_path:?}: {e}");
            return std::process::ExitCode::FAILURE;
        }
    };

    let outcome = match cli.command {
        Command::Hello => {
            println!("{{\"ok\":true}}");
            Ok(())
        }
        Command::Ping => match client.ping().await {
            Ok(rtt) => {
                println!(
                    "{{\"ok\":true,\"round_trip_ms\":{}}}",
                    rtt.as_secs_f64() * 1000.0
                );
                Ok(())
            }
            Err(e) => Err(e),
        },
        Command::Discover => match client.discover().await {
            Ok(tools) => {
                let json = serde_json::to_string_pretty(&tools).expect("serializes");
                println!("{json}");
                Ok(())
            }
            Err(e) => Err(e),
        },
        Command::Invoke {
            tool,
            args,
            deadline_ms,
        } => {
            let args: serde_json::Value = match serde_json::from_str(&args) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("invalid --args JSON: {e}");
                    return std::process::ExitCode::FAILURE;
                }
            };
            match client
                .invoke(&tool, args, deadline_ms.map(Duration::from_millis))
                .await
            {
                Ok(value) => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&value).expect("serializes")
                    );
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }
        Command::Batch { calls } => {
            let calls: Vec<agent_socket_tools::ast::message::BatchInvoke> =
                match serde_json::from_str(&calls) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("invalid --calls JSON: {e}");
                        return std::process::ExitCode::FAILURE;
                    }
                };
            match client.batch(calls, None).await {
                Ok(results) => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&results).expect("serializes")
                    );
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }
    };

    client.close().await;

    match outcome {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            std::process::ExitCode::FAILURE
        }
    }
}
