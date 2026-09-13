use std::{io, time::Duration};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::{net::TcpStream, time::timeout};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::protocol::{
    ClientId, ClientMessage, Limits, PUBLISH_KIND, ReceivedMessage, SUBSCRIBE_KIND, Topic,
    decode_server_message, encode_client_message,
};

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
const HELLO_REQUEST_ID: u32 = 1;
const COMMAND_REQUEST_ID: u32 = 2;

#[derive(Debug, PartialEq)]
pub struct PublishReceipt {
    pub event_id: u64,
    pub matched: u16,
    pub enqueued: u16,
    pub evicted: u16,
}

#[derive(Debug, PartialEq)]
pub struct Event {
    pub event_id: u64,
    pub topic: String,
    pub payload: Bytes,
}

pub struct Subscription {
    framed: Framed<TcpStream, LengthDelimitedCodec>,
    limits: Limits,
}

pub async fn publish(address: &str, topic: &str, payload: Bytes) -> io::Result<PublishReceipt> {
    let limits = Limits::default();
    let mut framed = connect(address, "boundwire-publisher", &limits).await?;
    let topic = parse_topic(topic, &limits)?;
    send(
        &mut framed,
        &limits,
        ClientMessage::Publish {
            request_id: COMMAND_REQUEST_ID,
            topic,
            payload,
        },
    )
    .await?;

    match receive_with_timeout(&mut framed, &limits).await? {
        ReceivedMessage::Ack {
            request_id: COMMAND_REQUEST_ID,
            acknowledged_kind: PUBLISH_KIND,
            event_id,
            matched,
            enqueued,
            evicted,
        } => Ok(PublishReceipt {
            event_id,
            matched,
            enqueued,
            evicted,
        }),
        message => Err(unexpected("publish acknowledgement", message)),
    }
}

pub async fn subscribe(address: &str, topic: &str) -> io::Result<Subscription> {
    let limits = Limits::default();
    let mut framed = connect(address, "boundwire-subscriber", &limits).await?;
    let topic = parse_topic(topic, &limits)?;
    send(
        &mut framed,
        &limits,
        ClientMessage::Subscribe {
            request_id: COMMAND_REQUEST_ID,
            topic,
        },
    )
    .await?;

    match receive_with_timeout(&mut framed, &limits).await? {
        ReceivedMessage::Ack {
            request_id: COMMAND_REQUEST_ID,
            acknowledged_kind: SUBSCRIBE_KIND,
            ..
        } => Ok(Subscription { framed, limits }),
        message => Err(unexpected("subscription acknowledgement", message)),
    }
}

impl Subscription {
    pub async fn next_event(&mut self) -> io::Result<Option<Event>> {
        let Some(frame) = self.framed.next().await else {
            return Ok(None);
        };
        let message = decode_server_message(frame?, &self.limits).map_err(invalid_data)?;
        match message {
            ReceivedMessage::Event {
                event_id,
                topic,
                payload,
            } => Ok(Some(Event {
                event_id,
                topic: topic.into_string(),
                payload,
            })),
            message => Err(unexpected("event", message)),
        }
    }
}

async fn connect(
    address: &str,
    client_id: &str,
    limits: &Limits,
) -> io::Result<Framed<TcpStream, LengthDelimitedCodec>> {
    let stream = TcpStream::connect(address).await?;
    let mut framed = LengthDelimitedCodec::builder()
        .max_frame_length(limits.max_frame_len)
        .new_framed(stream);
    let client_id = ClientId::parse(client_id.as_bytes(), limits).map_err(invalid_input)?;
    send(
        &mut framed,
        limits,
        ClientMessage::Hello {
            request_id: HELLO_REQUEST_ID,
            client_id,
        },
    )
    .await?;

    match receive_with_timeout(&mut framed, limits).await? {
        ReceivedMessage::Ready {
            request_id: HELLO_REQUEST_ID,
            ..
        } => Ok(framed),
        message => Err(unexpected("ready response", message)),
    }
}

async fn send(
    framed: &mut Framed<TcpStream, LengthDelimitedCodec>,
    limits: &Limits,
    message: ClientMessage,
) -> io::Result<()> {
    let frame = encode_client_message(&message, limits).map_err(invalid_input)?;
    framed.send(frame).await
}

async fn receive_with_timeout(
    framed: &mut Framed<TcpStream, LengthDelimitedCodec>,
    limits: &Limits,
) -> io::Result<ReceivedMessage> {
    let frame = timeout(RESPONSE_TIMEOUT, framed.next())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "server response timed out"))?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "server closed"))??;
    let message = decode_server_message(frame, limits).map_err(invalid_data)?;
    if let ReceivedMessage::Error {
        request_id,
        code,
        detail,
    } = message
    {
        return Err(io::Error::other(format!(
            "server rejected request {request_id} with code {code}: {detail}"
        )));
    }
    Ok(message)
}

fn parse_topic(topic: &str, limits: &Limits) -> io::Result<Topic> {
    Topic::parse(topic.as_bytes(), limits).map_err(invalid_input)
}

fn invalid_input(error: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
}

fn invalid_data(error: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

fn unexpected(expected: &str, received: ReceivedMessage) -> io::Error {
    invalid_data(format!("expected {expected}, received {received:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::run_server;
    use tokio::net::TcpListener;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn subscriber_receives_published_payload() -> io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let shutdown = CancellationToken::new();
        let server = tokio::spawn(run_server(listener, Limits::default(), shutdown.clone()));

        let mut subscription = subscribe(&address.to_string(), "blocks").await?;
        let receipt = publish(
            &address.to_string(),
            "blocks",
            Bytes::from_static(b"block-100"),
        )
        .await?;
        let event = subscription.next_event().await?.expect("event");

        assert_eq!(receipt.matched, 1);
        assert_eq!(receipt.enqueued, 1);
        assert_eq!(event.event_id, receipt.event_id);
        assert_eq!(event.topic, "blocks");
        assert_eq!(event.payload, Bytes::from_static(b"block-100"));

        shutdown.cancel();
        server.await.map_err(io::Error::other)??;
        Ok(())
    }
}
