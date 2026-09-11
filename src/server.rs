use std::{io, sync::Arc};

use futures_util::{SinkExt, StreamExt};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc},
    task::JoinSet,
};
use tokio_util::{codec::LengthDelimitedCodec, sync::CancellationToken};

use crate::{
    broker::{BrokerHandle, ConnectionId, spawn_broker},
    protocol::{
        ClientMessage, ErrorCode, Limits, ServerMessage, decode_client_message,
        encode_server_message,
    },
};

#[derive(Debug, PartialEq)]
pub struct ServerReport {
    pub accepted_connections: u64,
    pub rejected_connections: u64,
}

pub async fn run_server(
    listener: TcpListener,
    limits: Limits,
    shutdown: CancellationToken,
) -> io::Result<ServerReport> {
    limits
        .validate()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;

    let (broker, broker_task) = spawn_broker(limits.broker_queue, shutdown.clone());
    let permits = Arc::new(Semaphore::new(limits.max_connections));
    let mut connections = JoinSet::new();
    let mut next_connection_id = 1_u64;
    let mut report = ServerReport {
        accepted_connections: 0,
        rejected_connections: 0,
    };
    let mut server_error = None;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            joined = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = joined {
                    shutdown.cancel();
                    server_error = Some(io::Error::other(format!("connection task failed: {error}")));
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(connection) => connection,
                    Err(error) => {
                        shutdown.cancel();
                        server_error = Some(error);
                        break;
                    }
                };

                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    report.rejected_connections += 1;
                    drop(stream);
                    continue;
                };

                let Some(following_id) = next_connection_id.checked_add(1) else {
                    report.rejected_connections += 1;
                    drop(stream);
                    continue;
                };
                let id = ConnectionId(next_connection_id);
                next_connection_id = following_id;
                report.accepted_connections += 1;

                connections.spawn(serve_connection(
                    stream,
                    id,
                    permit,
                    limits.clone(),
                    broker.clone(),
                    shutdown.clone(),
                ));
            }
        }
    }

    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            server_error.get_or_insert_with(|| {
                io::Error::other(format!("connection task failed: {error}"))
            });
        }
    }

    drop(broker);
    broker_task
        .await
        .map_err(|error| io::Error::other(format!("broker task failed: {error}")))?;
    match server_error {
        Some(error) => Err(error),
        None => Ok(report),
    }
}

async fn serve_connection(
    stream: TcpStream,
    id: ConnectionId,
    _permit: tokio::sync::OwnedSemaphorePermit,
    limits: Limits,
    broker: BrokerHandle,
    shutdown: CancellationToken,
) {
    let mut framed = LengthDelimitedCodec::builder()
        .max_frame_length(limits.max_frame_len)
        .new_framed(stream);
    let (outgoing, mut outbound) = mpsc::channel(limits.outbound_queue);
    let force_close = CancellationToken::new();
    let mut active = false;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = force_close.cancelled() => break,
            message = outbound.recv() => {
                let Some(message) = message else {
                    break;
                };
                let Ok(encoded) = encode_server_message(&message, &limits) else {
                    break;
                };
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = force_close.cancelled() => break,
                    result = framed.send(encoded) => {
                        if result.is_err() {
                            break;
                        }
                    }
                }
            }
            incoming = framed.next() => {
                let Some(Ok(frame)) = incoming else {
                    break;
                };
                let Ok(message) = decode_client_message(frame, &limits) else {
                    break;
                };
                let request_id = message.request_id();

                match message {
                    ClientMessage::Hello { .. } if !active => {
                        if broker
                            .register(
                                id,
                                request_id,
                                outgoing.clone(),
                                force_close.clone(),
                            )
                            .await
                            .is_err()
                        {
                            break;
                        }
                        active = true;
                    }
                    ClientMessage::Hello { .. } => {
                        if queue_error(
                            &outgoing,
                            &force_close,
                            request_id,
                            ErrorCode::AlreadyActive,
                            "HELLO is only valid once",
                        ) {
                            break;
                        }
                    }
                    ClientMessage::Subscribe { topic, .. } if active => {
                        if broker.subscribe(id, request_id, topic).await.is_err() {
                            break;
                        }
                    }
                    ClientMessage::Publish { topic, payload, .. } if active => {
                        if broker
                            .publish(id, request_id, topic, payload)
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    ClientMessage::Subscribe { .. } | ClientMessage::Publish { .. } => {
                        if queue_error(
                            &outgoing,
                            &force_close,
                            request_id,
                            ErrorCode::ExpectedHello,
                            "send HELLO before other commands",
                        ) {
                            break;
                        }
                    }
                }
            }
        }
    }

    broker.remove(id).await;
}

fn queue_error(
    outgoing: &mpsc::Sender<ServerMessage>,
    force_close: &CancellationToken,
    request_id: u32,
    code: ErrorCode,
    detail: &'static str,
) -> bool {
    match outgoing.try_send(ServerMessage::Error {
        request_id,
        code,
        detail,
    }) {
        Ok(()) => false,
        Err(_) => {
            force_close.cancel();
            true
        }
    }
}
