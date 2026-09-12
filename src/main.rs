use std::{env, io, process::ExitCode};

use boundwire::{protocol::Limits, server::run_server};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

const DEFAULT_ADDRESS: &str = "127.0.0.1:7000";
const USAGE: &str = "Usage: boundwire serve [ADDRESS]";

#[derive(Debug, PartialEq)]
enum Command {
    Serve(String),
    Help,
}

#[tokio::main]
async fn main() -> ExitCode {
    match parse_args(env::args().skip(1)) {
        Ok(Command::Help) => {
            println!("{USAGE}");
            ExitCode::SUCCESS
        }
        Ok(Command::Serve(address)) => match serve(&address).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("error: {error}");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            eprintln!("error: {error}\n\n{USAGE}");
            ExitCode::from(2)
        }
    }
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Command, String> {
    let mut args = args.into_iter();
    let Some(command) = args.next() else {
        return Err("missing command".to_owned());
    };

    match command.as_str() {
        "-h" | "--help" => no_more_args(args, Command::Help),
        "serve" => match args.next() {
            None => Ok(Command::Serve(DEFAULT_ADDRESS.to_owned())),
            Some(flag) if flag == "-h" || flag == "--help" => no_more_args(args, Command::Help),
            Some(address) => no_more_args(args, Command::Serve(address)),
        },
        _ => Err(format!("unknown command: {command}")),
    }
}

fn no_more_args(
    mut remaining: impl Iterator<Item = String>,
    command: Command,
) -> Result<Command, String> {
    match remaining.next() {
        Some(argument) => Err(format!("unexpected argument: {argument}")),
        None => Ok(command),
    }
}

async fn serve(address: &str) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;
    let local_address = listener.local_addr()?;
    let shutdown = CancellationToken::new();
    let server = run_server(listener, Limits::default(), shutdown.clone());
    tokio::pin!(server);

    println!("listening on {local_address}");

    let report = tokio::select! {
        result = &mut server => result?,
        result = tokio::signal::ctrl_c() => {
            result?;
            shutdown.cancel();
            server.await?
        }
    };

    println!(
        "stopped: {} accepted, {} rejected",
        report.accepted_connections, report.rejected_connections
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Command, DEFAULT_ADDRESS, parse_args};

    fn parse(arguments: &[&str]) -> Result<Command, String> {
        parse_args(arguments.iter().map(|argument| (*argument).to_owned()))
    }

    #[test]
    fn serve_uses_default_address() {
        assert_eq!(
            parse(&["serve"]),
            Ok(Command::Serve(DEFAULT_ADDRESS.to_owned()))
        );
    }

    #[test]
    fn serve_accepts_custom_address() {
        assert_eq!(
            parse(&["serve", "0.0.0.0:9000"]),
            Ok(Command::Serve("0.0.0.0:9000".to_owned()))
        );
    }

    #[test]
    fn help_is_available_at_both_levels() {
        assert_eq!(parse(&["--help"]), Ok(Command::Help));
        assert_eq!(parse(&["serve", "--help"]), Ok(Command::Help));
    }

    #[test]
    fn invalid_arguments_are_rejected() {
        assert_eq!(parse(&[]), Err("missing command".to_owned()));
        assert_eq!(parse(&["run"]), Err("unknown command: run".to_owned()));
        assert_eq!(
            parse(&["serve", "127.0.0.1:7000", "extra"]),
            Err("unexpected argument: extra".to_owned())
        );
    }
}
