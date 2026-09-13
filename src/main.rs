use std::{env, io, process::ExitCode};

use boundwire::{
    client::{publish, subscribe},
    protocol::Limits,
    server::run_server,
};
use bytes::Bytes;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

const DEFAULT_ADDRESS: &str = "127.0.0.1:7000";
const USAGE: &str = "Usage:
  boundwire serve [ADDRESS]
  boundwire subscribe <TOPIC> [ADDRESS]
  boundwire publish <TOPIC> <PAYLOAD> [ADDRESS]";

#[derive(Debug, PartialEq)]
enum Command {
    Serve(String),
    Subscribe {
        topic: String,
        address: String,
    },
    Publish {
        topic: String,
        payload: String,
        address: String,
    },
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
        Ok(Command::Subscribe { topic, address }) => match listen(&address, &topic).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("error: {error}");
                ExitCode::FAILURE
            }
        },
        Ok(Command::Publish {
            topic,
            payload,
            address,
        }) => match publish(&address, &topic, Bytes::from(payload)).await {
            Ok(receipt) => {
                println!(
                    "published event {}: {} matched, {} enqueued, {} evicted",
                    receipt.event_id, receipt.matched, receipt.enqueued, receipt.evicted
                );
                ExitCode::SUCCESS
            }
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
    let args: Vec<_> = args.into_iter().collect();
    match args.as_slice() {
        [flag] if flag == "-h" || flag == "--help" => Ok(Command::Help),
        [command, flag]
            if matches!(command.as_str(), "serve" | "subscribe" | "publish")
                && matches!(flag.as_str(), "-h" | "--help") =>
        {
            Ok(Command::Help)
        }
        [command] if command == "serve" => Ok(Command::Serve(DEFAULT_ADDRESS.to_owned())),
        [command, address] if command == "serve" => Ok(Command::Serve(address.clone())),
        [command, topic] if command == "subscribe" => Ok(Command::Subscribe {
            topic: topic.clone(),
            address: DEFAULT_ADDRESS.to_owned(),
        }),
        [command, topic, address] if command == "subscribe" => Ok(Command::Subscribe {
            topic: topic.clone(),
            address: address.clone(),
        }),
        [command, topic, payload] if command == "publish" => Ok(Command::Publish {
            topic: topic.clone(),
            payload: payload.clone(),
            address: DEFAULT_ADDRESS.to_owned(),
        }),
        [command, topic, payload, address] if command == "publish" => Ok(Command::Publish {
            topic: topic.clone(),
            payload: payload.clone(),
            address: address.clone(),
        }),
        [] => Err("missing command".to_owned()),
        [command, ..] if !matches!(command.as_str(), "serve" | "subscribe" | "publish") => {
            Err(format!("unknown command: {command}"))
        }
        _ => Err("invalid arguments".to_owned()),
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

async fn listen(address: &str, topic: &str) -> io::Result<()> {
    let mut subscription = subscribe(address, topic).await?;
    println!("subscribed to {topic} on {address}");

    loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                return Ok(());
            }
            event = subscription.next_event() => match event? {
                Some(event) => println!(
                    "{} {} {}",
                    event.event_id,
                    event.topic,
                    String::from_utf8_lossy(&event.payload)
                ),
                None => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "server closed")),
            }
        }
    }
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
    fn subscribe_uses_default_or_custom_address() {
        assert_eq!(
            parse(&["subscribe", "blocks"]),
            Ok(Command::Subscribe {
                topic: "blocks".to_owned(),
                address: DEFAULT_ADDRESS.to_owned(),
            })
        );
        assert_eq!(
            parse(&["subscribe", "blocks", "127.0.0.1:8000"]),
            Ok(Command::Subscribe {
                topic: "blocks".to_owned(),
                address: "127.0.0.1:8000".to_owned(),
            })
        );
    }

    #[test]
    fn publish_uses_default_or_custom_address() {
        assert_eq!(
            parse(&["publish", "blocks", "block-100"]),
            Ok(Command::Publish {
                topic: "blocks".to_owned(),
                payload: "block-100".to_owned(),
                address: DEFAULT_ADDRESS.to_owned(),
            })
        );
        assert_eq!(
            parse(&["publish", "blocks", "block-100", "127.0.0.1:8000"]),
            Ok(Command::Publish {
                topic: "blocks".to_owned(),
                payload: "block-100".to_owned(),
                address: "127.0.0.1:8000".to_owned(),
            })
        );
    }

    #[test]
    fn help_is_available_at_both_levels() {
        assert_eq!(parse(&["--help"]), Ok(Command::Help));
        assert_eq!(parse(&["serve", "--help"]), Ok(Command::Help));
        assert_eq!(parse(&["subscribe", "--help"]), Ok(Command::Help));
        assert_eq!(parse(&["publish", "--help"]), Ok(Command::Help));
    }

    #[test]
    fn invalid_arguments_are_rejected() {
        assert_eq!(parse(&[]), Err("missing command".to_owned()));
        assert_eq!(parse(&["run"]), Err("unknown command: run".to_owned()));
        assert_eq!(parse(&["subscribe"]), Err("invalid arguments".to_owned()));
        assert_eq!(
            parse(&["publish", "blocks"]),
            Err("invalid arguments".to_owned())
        );
        assert_eq!(
            parse(&["serve", "127.0.0.1:7000", "extra"]),
            Err("invalid arguments".to_owned())
        );
    }
}
