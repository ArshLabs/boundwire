use std::{io, net::SocketAddr, time::Duration};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};
use tokio_util::{codec::LengthDelimitedCodec, sync::CancellationToken};

use boundwire::{
    protocol::{Limits, VERSION},
    server::{ServerReport, run_server},
};

const HELLO: u8 = 0x01;
const SUBSCRIBE: u8 = 0x02;
const PUBLISH: u8 = 0x04;
const READY: u8 = 0x81;
const ACK: u8 = 0x82;
const EVENT: u8 = 0x83;
const ERROR: u8 = 0x85;

struct TestServer {
    address: SocketAddr,
    shutdown: CancellationToken,
    task: JoinHandle<io::Result<ServerReport>>,
}

impl TestServer {
    async fn start(limits: Limits) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run_server(listener, limits, shutdown.clone()));
        Ok(Self {
            address,
            shutdown,
            task,
        })
    }

    async fn stop(self) -> io::Result<ServerReport> {
        self.shutdown.cancel();
        self.task
            .await
            .map_err(|error| io::Error::other(error.to_string()))?
    }
}

struct TestClient {
    framed: tokio_util::codec::Framed<TcpStream, LengthDelimitedCodec>,
}

impl TestClient {
    async fn connect(address: SocketAddr, limits: &Limits, name: &[u8]) -> io::Result<Self> {
        let stream = TcpStream::connect(address).await?;
        let framed = LengthDelimitedCodec::builder()
            .max_frame_length(limits.max_frame_len)
            .new_framed(stream);
        let mut client = Self { framed };
        client.send(hello(1, name)).await?;
        assert!(matches!(
            client.receive().await?,
            ServerFrame::Ready { request_id: 1, .. }
        ));
        Ok(client)
    }

    async fn send(&mut self, frame: Bytes) -> io::Result<()> {
        self.framed.send(frame).await
    }

    async fn receive(&mut self) -> io::Result<ServerFrame> {
        let frame = timeout(Duration::from_secs(5), self.framed.next())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "server response timed out"))?
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "server closed"))??;
        decode_server(frame)
    }
}

#[derive(Debug)]
enum ServerFrame {
    Ready {
        request_id: u32,
    },
    Ack {
        request_id: u32,
        matched: u16,
        enqueued: u16,
        evicted: u16,
    },
    Event {
        topic: Bytes,
        payload: Bytes,
    },
    Error {
        request_id: u32,
        code: u16,
    },
}

#[tokio::test]
async fn accepts_fragmented_and_coalesced_frames() -> io::Result<()> {
    let limits = Limits::default();
    let server = TestServer::start(limits.clone()).await?;
    let mut stream = TcpStream::connect(server.address).await?;

    let hello = outer_frame(&hello(1, b"combined"));
    stream.write_all(&hello[..2]).await?;
    tokio::task::yield_now().await;
    stream.write_all(&hello[2..]).await?;

    let mut framed = LengthDelimitedCodec::builder()
        .max_frame_length(limits.max_frame_len)
        .new_framed(stream);
    let ready = receive_from(&mut framed).await?;
    assert!(matches!(ready, ServerFrame::Ready { request_id: 1, .. }));

    let mut combined = BytesMut::new();
    combined.extend_from_slice(&outer_frame(&subscribe(2, b"blocks")));
    combined.extend_from_slice(&outer_frame(&publish(3, b"blocks", b"block-100")));
    framed.get_mut().write_all(&combined).await?;

    assert!(matches!(
        receive_from(&mut framed).await?,
        ServerFrame::Ack { request_id: 2, .. }
    ));
    assert!(matches!(
        receive_from(&mut framed).await?,
        ServerFrame::Event { ref payload, .. } if payload.as_ref() == b"block-100"
    ));
    assert!(matches!(
        receive_from(&mut framed).await?,
        ServerFrame::Ack { request_id: 3, .. }
    ));

    server.stop().await?;
    Ok(())
}

#[tokio::test]
async fn oversized_frame_closes_only_its_connection() -> io::Result<()> {
    let limits = Limits::default();
    let server = TestServer::start(limits.clone()).await?;
    let mut good = TestClient::connect(server.address, &limits, b"good").await?;
    let mut bad = TcpStream::connect(server.address).await?;

    bad.write_u32((limits.max_frame_len + 1) as u32).await?;
    let mut byte = [0_u8; 1];
    let read = timeout(Duration::from_secs(5), bad.read(&mut byte))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "oversized client stayed open"))??;
    assert_eq!(read, 0);

    good.send(subscribe(2, b"still-alive")).await?;
    assert!(matches!(
        good.receive().await?,
        ServerFrame::Ack { request_id: 2, .. }
    ));

    server.stop().await?;
    Ok(())
}

#[tokio::test]
async fn rejects_commands_until_hello_arrives() -> io::Result<()> {
    let limits = Limits::default();
    let server = TestServer::start(limits.clone()).await?;
    let stream = TcpStream::connect(server.address).await?;
    let mut framed = LengthDelimitedCodec::builder()
        .max_frame_length(limits.max_frame_len)
        .new_framed(stream);

    framed.send(publish(1, b"blocks", b"too-early")).await?;
    assert!(matches!(
        receive_from(&mut framed).await?,
        ServerFrame::Error {
            request_id: 1,
            code: 1,
        }
    ));

    framed.send(hello(2, b"late-hello")).await?;
    assert!(matches!(
        receive_from(&mut framed).await?,
        ServerFrame::Ready { request_id: 2 }
    ));

    server.stop().await?;
    Ok(())
}

#[tokio::test]
async fn repeated_hello_is_rejected_without_closing_connection() -> io::Result<()> {
    let limits = Limits::default();
    let server = TestServer::start(limits.clone()).await?;
    let mut client = TestClient::connect(server.address, &limits, b"original").await?;

    client.send(hello(2, b"replacement")).await?;
    assert!(matches!(
        client.receive().await?,
        ServerFrame::Error {
            request_id: 2,
            code: 2,
        }
    ));

    client.send(subscribe(3, b"still-active")).await?;
    assert!(matches!(
        client.receive().await?,
        ServerFrame::Ack { request_id: 3, .. }
    ));

    server.stop().await?;
    Ok(())
}

#[tokio::test]
async fn silent_connection_releases_its_slot() -> io::Result<()> {
    let limits = Limits {
        max_connections: 1,
        ..Limits::default()
    };
    let server = TestServer::start(limits.clone()).await?;
    let mut silent = TcpStream::connect(server.address).await?;

    let mut byte = [0_u8; 1];
    let read = timeout(Duration::from_secs(7), silent.read(&mut byte))
        .await
        .map_err(|_| {
            io::Error::new(io::ErrorKind::TimedOut, "silent connection kept its slot")
        })??;
    assert_eq!(read, 0);

    let _client = TestClient::connect(server.address, &limits, b"after-timeout").await?;
    server.stop().await?;
    Ok(())
}

#[tokio::test]
async fn active_connection_survives_handshake_deadline() -> io::Result<()> {
    let limits = Limits::default();
    let server = TestServer::start(limits.clone()).await?;
    let mut client = TestClient::connect(server.address, &limits, b"active").await?;

    tokio::time::sleep(Duration::from_millis(5_100)).await;
    client.send(subscribe(2, b"still-active")).await?;
    assert!(matches!(
        client.receive().await?,
        ServerFrame::Ack { request_id: 2, .. }
    ));

    server.stop().await?;
    Ok(())
}

#[tokio::test]
async fn reports_connections_rejected_at_limit() -> io::Result<()> {
    let limits = Limits {
        max_connections: 1,
        ..Limits::default()
    };
    let server = TestServer::start(limits.clone()).await?;
    let _active = TestClient::connect(server.address, &limits, b"active").await?;
    let mut rejected = TcpStream::connect(server.address).await?;

    let mut byte = [0_u8; 1];
    let read = timeout(Duration::from_secs(5), rejected.read(&mut byte))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "extra connection stayed open"))??;
    assert_eq!(read, 0);

    let report = server.stop().await?;
    assert_eq!(report.accepted_connections, 1);
    assert_eq!(report.rejected_connections, 1);
    Ok(())
}

#[tokio::test]
async fn slow_socket_does_not_block_fast_subscriber() -> io::Result<()> {
    let limits = Limits {
        outbound_queue: 2,
        max_connections: 8,
        ..Limits::default()
    };
    let server = TestServer::start(limits.clone()).await?;
    let mut slow = TestClient::connect(server.address, &limits, b"slow").await?;
    let mut fast = TestClient::connect(server.address, &limits, b"fast").await?;
    let mut publisher = TestClient::connect(server.address, &limits, b"publisher").await?;

    slow.send(subscribe(2, b"blocks")).await?;
    assert!(matches!(slow.receive().await?, ServerFrame::Ack { .. }));
    fast.send(subscribe(2, b"blocks")).await?;
    assert!(matches!(fast.receive().await?, ServerFrame::Ack { .. }));

    let marker = Bytes::from_static(b"final-marker");
    let marker_for_reader = marker.clone();
    let fast_reader = tokio::spawn(async move {
        loop {
            match fast.receive().await? {
                ServerFrame::Event { topic, payload, .. }
                    if topic.as_ref() == b"blocks" && payload == marker_for_reader =>
                {
                    return Ok::<_, io::Error>(());
                }
                ServerFrame::Event { .. } => {}
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected fast-subscriber frame: {other:?}"),
                    ));
                }
            }
        }
    });

    let large_payload = vec![7_u8; limits.max_payload_len];
    let mut eviction_seen = false;
    for request_id in 10..=2_058 {
        publisher
            .send(publish(request_id, b"blocks", &large_payload))
            .await?;
        let ServerFrame::Ack {
            matched,
            enqueued,
            evicted,
            ..
        } = publisher.receive().await?
        else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "publisher expected ACK",
            ));
        };
        assert!(enqueued <= matched);
        if evicted == 1 {
            eviction_seen = true;
            break;
        }
    }
    assert!(
        eviction_seen,
        "the non-reading socket never created queue pressure"
    );

    publisher.send(publish(3_000, b"blocks", &marker)).await?;
    assert!(matches!(
        publisher.receive().await?,
        ServerFrame::Ack {
            matched: 1,
            enqueued: 1,
            evicted: 0,
            ..
        }
    ));
    timeout(Duration::from_secs(10), fast_reader)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "fast subscriber missed marker"))?
        .map_err(|error| io::Error::other(error.to_string()))??;

    drop(slow);
    server.stop().await?;
    Ok(())
}

fn hello(request_id: u32, name: &[u8]) -> Bytes {
    named_frame(HELLO, request_id, name, &[])
}

fn subscribe(request_id: u32, topic: &[u8]) -> Bytes {
    named_frame(SUBSCRIBE, request_id, topic, &[])
}

fn publish(request_id: u32, topic: &[u8], payload: &[u8]) -> Bytes {
    named_frame(PUBLISH, request_id, topic, payload)
}

fn named_frame(kind: u8, request_id: u32, name: &[u8], payload: &[u8]) -> Bytes {
    let mut frame = BytesMut::with_capacity(7 + name.len() + payload.len());
    frame.put_u8(VERSION);
    frame.put_u8(kind);
    frame.put_u32(request_id);
    frame.put_u8(name.len() as u8);
    frame.extend_from_slice(name);
    frame.extend_from_slice(payload);
    frame.freeze()
}

fn outer_frame(inner: &Bytes) -> Bytes {
    let mut frame = BytesMut::with_capacity(4 + inner.len());
    frame.put_u32(inner.len() as u32);
    frame.extend_from_slice(inner);
    frame.freeze()
}

async fn receive_from(
    framed: &mut tokio_util::codec::Framed<TcpStream, LengthDelimitedCodec>,
) -> io::Result<ServerFrame> {
    let frame = timeout(Duration::from_secs(5), framed.next())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "server response timed out"))?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "server closed"))??;
    decode_server(frame)
}

fn decode_server(frame: BytesMut) -> io::Result<ServerFrame> {
    if frame.len() < 6 {
        return Err(invalid("short server frame"));
    }
    let mut bytes = frame.freeze();
    if bytes.get_u8() != VERSION {
        return Err(invalid("wrong server protocol version"));
    }
    let kind = bytes.get_u8();
    let request_id = bytes.get_u32();

    let decoded = match kind {
        READY if bytes.len() == 8 => {
            let _connection_id = bytes.get_u64();
            ServerFrame::Ready { request_id }
        }
        ACK if bytes.len() == 15 => {
            let _acknowledged_kind = bytes.get_u8();
            let _event_id = bytes.get_u64();
            ServerFrame::Ack {
                request_id,
                matched: bytes.get_u16(),
                enqueued: bytes.get_u16(),
                evicted: bytes.get_u16(),
            }
        }
        EVENT if bytes.len() >= 9 => {
            let _event_id = bytes.get_u64();
            let topic_len = bytes.get_u8() as usize;
            if bytes.len() < topic_len {
                return Err(invalid("truncated event topic"));
            }
            ServerFrame::Event {
                topic: bytes.split_to(topic_len),
                payload: bytes,
            }
        }
        ERROR if bytes.len() >= 4 => {
            let code = bytes.get_u16();
            let detail_len = bytes.get_u16() as usize;
            if bytes.len() != detail_len {
                return Err(invalid("invalid error detail length"));
            }
            ServerFrame::Error { request_id, code }
        }
        _ => return Err(invalid("unknown or malformed server frame")),
    };

    Ok(decoded)
}

fn invalid(detail: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, detail)
}
