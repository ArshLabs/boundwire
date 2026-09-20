use std::{error::Error, fmt};

use bytes::{Buf, BufMut, Bytes, BytesMut};

pub const VERSION: u8 = 1;

const HELLO: u8 = 0x01;
const SUBSCRIBE: u8 = 0x02;
const PUBLISH: u8 = 0x04;
const READY: u8 = 0x81;
const ACK: u8 = 0x82;
const EVENT: u8 = 0x83;
const ERROR: u8 = 0x85;
const HEADER_LEN: usize = 6;

#[derive(Clone, Debug)]
pub struct Limits {
    pub max_frame_len: usize,
    pub max_payload_len: usize,
    pub max_name_len: usize,
    pub broker_queue: usize,
    pub outbound_queue: usize,
    pub max_connections: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_len: 16 * 1024,
            max_payload_len: 12 * 1024,
            max_name_len: 64,
            broker_queue: 128,
            outbound_queue: 32,
            max_connections: 64,
        }
    }
}

impl Limits {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.broker_queue == 0 || self.max_connections == 0 {
            return Err(ProtocolError::InvalidLimits(
                "broker queue and connection limits must be nonzero",
            ));
        }
        if self.outbound_queue < 2 {
            return Err(ProtocolError::InvalidLimits(
                "outbound queue must hold an event and its acknowledgement",
            ));
        }
        if self.max_name_len == 0 || self.max_name_len > u8::MAX as usize {
            return Err(ProtocolError::InvalidLimits(
                "name limit must fit in one byte",
            ));
        }
        let event_len = self
            .max_payload_len
            .checked_add(HEADER_LEN + 8 + 1)
            .and_then(|length| length.checked_add(self.max_name_len));
        if event_len.is_none_or(|length| length > self.max_frame_len) {
            return Err(ProtocolError::InvalidLimits(
                "a maximum event must fit in one frame",
            ));
        }
        if self.max_connections > u16::MAX as usize {
            return Err(ProtocolError::InvalidLimits(
                "connection limit must fit in acknowledgement counts",
            ));
        }
        if self.max_frame_len > u32::MAX as usize {
            return Err(ProtocolError::InvalidLimits(
                "frame limit must fit in the four-byte length prefix",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ClientId(String);

impl ClientId {
    pub fn parse(bytes: &[u8], limits: &Limits) -> Result<Self, ProtocolError> {
        if bytes.is_empty() || bytes.len() > limits.max_name_len {
            return Err(ProtocolError::InvalidClientId);
        }
        if !bytes.iter().all(|byte| (0x21..=0x7e).contains(byte)) {
            return Err(ProtocolError::InvalidClientId);
        }
        Ok(Self(
            String::from_utf8(bytes.to_vec()).expect("validated ASCII is UTF-8"),
        ))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct Topic(String);

impl Topic {
    pub fn parse(bytes: &[u8], limits: &Limits) -> Result<Self, ProtocolError> {
        if bytes.is_empty() || bytes.len() > limits.max_name_len {
            return Err(ProtocolError::InvalidTopic);
        }
        if !bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/'))
        {
            return Err(ProtocolError::InvalidTopic);
        }
        Ok(Self(
            String::from_utf8(bytes.to_vec()).expect("validated ASCII is UTF-8"),
        ))
    }

    pub(crate) fn into_string(self) -> String {
        self.0
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum ClientMessage {
    Hello {
        request_id: u32,
        client_id: ClientId,
    },
    Subscribe {
        request_id: u32,
        topic: Topic,
    },
    Publish {
        request_id: u32,
        topic: Topic,
        payload: Bytes,
    },
}

impl ClientMessage {
    pub fn request_id(&self) -> u32 {
        match self {
            Self::Hello { request_id, .. }
            | Self::Subscribe { request_id, .. }
            | Self::Publish { request_id, .. } => *request_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ServerMessage {
    Ready {
        request_id: u32,
        connection_id: u64,
    },
    Ack {
        request_id: u32,
        acknowledged_kind: u8,
        event_id: u64,
        matched: u16,
        enqueued: u16,
        evicted: u16,
    },
    Event {
        event_id: u64,
        topic: Topic,
        payload: Bytes,
    },
    Error {
        request_id: u32,
        code: ErrorCode,
        detail: &'static str,
    },
}

#[derive(Debug, PartialEq)]
pub(crate) enum ReceivedMessage {
    Ready {
        request_id: u32,
        connection_id: u64,
    },
    Ack {
        request_id: u32,
        acknowledged_kind: u8,
        event_id: u64,
        matched: u16,
        enqueued: u16,
        evicted: u16,
    },
    Event {
        event_id: u64,
        topic: Topic,
        payload: Bytes,
    },
    Error {
        request_id: u32,
        code: u16,
        detail: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(u16)]
pub(crate) enum ErrorCode {
    ExpectedHello = 1,
    AlreadyActive = 2,
    EventIdExhausted = 3,
}

#[derive(Debug, PartialEq)]
pub enum ProtocolError {
    InvalidLimits(&'static str),
    FrameTooShort,
    UnknownVersion(u8),
    UnknownKind(u8),
    ZeroRequestId,
    TruncatedField,
    TrailingBytes,
    InvalidClientId,
    InvalidTopic,
    PayloadTooLarge,
    EncodedFrameTooLarge,
    UnexpectedRequestId,
    InvalidUtf8,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimits(detail) => write!(f, "invalid limits: {detail}"),
            Self::FrameTooShort => f.write_str("frame is shorter than the common header"),
            Self::UnknownVersion(version) => write!(f, "unsupported protocol version {version}"),
            Self::UnknownKind(kind) => write!(f, "unknown message kind {kind:#04x}"),
            Self::ZeroRequestId => f.write_str("client request ID must be nonzero"),
            Self::TruncatedField => f.write_str("frame ends inside a declared field"),
            Self::TrailingBytes => f.write_str("frame contains unexpected trailing bytes"),
            Self::InvalidClientId => {
                f.write_str("client ID is empty, too long, or contains invalid bytes")
            }
            Self::InvalidTopic => {
                f.write_str("topic is empty, too long, or contains invalid bytes")
            }
            Self::PayloadTooLarge => f.write_str("publish payload exceeds the configured limit"),
            Self::EncodedFrameTooLarge => {
                f.write_str("server message exceeds the configured frame limit")
            }
            Self::UnexpectedRequestId => f.write_str("message contains an unexpected request ID"),
            Self::InvalidUtf8 => f.write_str("message contains invalid UTF-8"),
        }
    }
}

impl Error for ProtocolError {}

pub(crate) fn decode_client_message(
    frame: BytesMut,
    limits: &Limits,
) -> Result<ClientMessage, ProtocolError> {
    if frame.len() < HEADER_LEN {
        return Err(ProtocolError::FrameTooShort);
    }

    let mut bytes = frame.freeze();
    let version = bytes.get_u8();
    if version != VERSION {
        return Err(ProtocolError::UnknownVersion(version));
    }
    let kind = bytes.get_u8();
    let request_id = bytes.get_u32();
    if request_id == 0 {
        return Err(ProtocolError::ZeroRequestId);
    }

    match kind {
        HELLO => {
            let value = take_named_field(&mut bytes, limits)?;
            let client_id = ClientId::parse(&value, limits)?;
            require_empty(&bytes)?;
            Ok(ClientMessage::Hello {
                request_id,
                client_id,
            })
        }
        SUBSCRIBE => {
            let value = take_named_field(&mut bytes, limits)?;
            let topic = Topic::parse(&value, limits)?;
            require_empty(&bytes)?;
            Ok(ClientMessage::Subscribe { request_id, topic })
        }
        PUBLISH => {
            let value = take_named_field(&mut bytes, limits)?;
            let topic = Topic::parse(&value, limits)?;
            if bytes.len() > limits.max_payload_len {
                return Err(ProtocolError::PayloadTooLarge);
            }
            Ok(ClientMessage::Publish {
                request_id,
                topic,
                payload: bytes,
            })
        }
        other => Err(ProtocolError::UnknownKind(other)),
    }
}

pub(crate) fn encode_client_message(
    message: &ClientMessage,
    limits: &Limits,
) -> Result<Bytes, ProtocolError> {
    let mut frame = BytesMut::new();
    frame.put_u8(VERSION);

    match message {
        ClientMessage::Hello {
            request_id,
            client_id,
        } => {
            frame.put_u8(HELLO);
            frame.put_u32(*request_id);
            put_named_field(&mut frame, client_id.0.as_bytes());
        }
        ClientMessage::Subscribe { request_id, topic } => {
            frame.put_u8(SUBSCRIBE);
            frame.put_u32(*request_id);
            put_named_field(&mut frame, topic.0.as_bytes());
        }
        ClientMessage::Publish {
            request_id,
            topic,
            payload,
        } => {
            if payload.len() > limits.max_payload_len {
                return Err(ProtocolError::PayloadTooLarge);
            }
            frame.put_u8(PUBLISH);
            frame.put_u32(*request_id);
            put_named_field(&mut frame, topic.0.as_bytes());
            frame.extend_from_slice(payload);
        }
    }

    if message.request_id() == 0 {
        return Err(ProtocolError::ZeroRequestId);
    }
    if frame.len() > limits.max_frame_len {
        return Err(ProtocolError::EncodedFrameTooLarge);
    }
    Ok(frame.freeze())
}

pub(crate) fn decode_server_message(
    frame: BytesMut,
    limits: &Limits,
) -> Result<ReceivedMessage, ProtocolError> {
    if frame.len() < HEADER_LEN {
        return Err(ProtocolError::FrameTooShort);
    }

    let mut bytes = frame.freeze();
    let version = bytes.get_u8();
    if version != VERSION {
        return Err(ProtocolError::UnknownVersion(version));
    }
    let kind = bytes.get_u8();
    let request_id = bytes.get_u32();

    match kind {
        READY => {
            require_request_id(request_id, true)?;
            require_remaining(&bytes, 8)?;
            let connection_id = bytes.get_u64();
            require_empty(&bytes)?;
            Ok(ReceivedMessage::Ready {
                request_id,
                connection_id,
            })
        }
        ACK => {
            require_request_id(request_id, true)?;
            require_remaining(&bytes, 15)?;
            let message = ReceivedMessage::Ack {
                request_id,
                acknowledged_kind: bytes.get_u8(),
                event_id: bytes.get_u64(),
                matched: bytes.get_u16(),
                enqueued: bytes.get_u16(),
                evicted: bytes.get_u16(),
            };
            require_empty(&bytes)?;
            Ok(message)
        }
        EVENT => {
            require_request_id(request_id, false)?;
            require_remaining(&bytes, 8)?;
            let event_id = bytes.get_u64();
            let topic = Topic::parse(&take_named_field(&mut bytes, limits)?, limits)?;
            if bytes.len() > limits.max_payload_len {
                return Err(ProtocolError::PayloadTooLarge);
            }
            Ok(ReceivedMessage::Event {
                event_id,
                topic,
                payload: bytes,
            })
        }
        ERROR => {
            require_request_id(request_id, true)?;
            require_remaining(&bytes, 4)?;
            let code = bytes.get_u16();
            let detail_len = bytes.get_u16() as usize;
            require_remaining(&bytes, detail_len)?;
            let detail = String::from_utf8(bytes.split_to(detail_len).to_vec())
                .map_err(|_| ProtocolError::InvalidUtf8)?;
            require_empty(&bytes)?;
            Ok(ReceivedMessage::Error {
                request_id,
                code,
                detail,
            })
        }
        other => Err(ProtocolError::UnknownKind(other)),
    }
}

pub(crate) fn encode_server_message(
    message: &ServerMessage,
    limits: &Limits,
) -> Result<Bytes, ProtocolError> {
    let mut frame = BytesMut::new();
    frame.put_u8(VERSION);

    match message {
        ServerMessage::Ready {
            request_id,
            connection_id,
        } => {
            frame.put_u8(READY);
            frame.put_u32(*request_id);
            frame.put_u64(*connection_id);
        }
        ServerMessage::Ack {
            request_id,
            acknowledged_kind,
            event_id,
            matched,
            enqueued,
            evicted,
        } => {
            frame.put_u8(ACK);
            frame.put_u32(*request_id);
            frame.put_u8(*acknowledged_kind);
            frame.put_u64(*event_id);
            frame.put_u16(*matched);
            frame.put_u16(*enqueued);
            frame.put_u16(*evicted);
        }
        ServerMessage::Event {
            event_id,
            topic,
            payload,
        } => {
            if payload.len() > limits.max_payload_len {
                return Err(ProtocolError::PayloadTooLarge);
            }
            frame.put_u8(EVENT);
            frame.put_u32(0);
            frame.put_u64(*event_id);
            frame.put_u8(topic.0.len() as u8);
            frame.extend_from_slice(topic.0.as_bytes());
            frame.extend_from_slice(payload);
        }
        ServerMessage::Error {
            request_id,
            code,
            detail,
        } => {
            frame.put_u8(ERROR);
            frame.put_u32(*request_id);
            frame.put_u16(*code as u16);
            frame.put_u16(detail.len() as u16);
            frame.extend_from_slice(detail.as_bytes());
        }
    }

    if frame.len() > limits.max_frame_len {
        return Err(ProtocolError::EncodedFrameTooLarge);
    }
    Ok(frame.freeze())
}

fn take_named_field(bytes: &mut Bytes, limits: &Limits) -> Result<Bytes, ProtocolError> {
    if !bytes.has_remaining() {
        return Err(ProtocolError::TruncatedField);
    }
    let length = bytes.get_u8() as usize;
    if length > limits.max_name_len || bytes.remaining() < length {
        return Err(ProtocolError::TruncatedField);
    }
    Ok(bytes.split_to(length))
}

fn put_named_field(frame: &mut BytesMut, value: &[u8]) {
    frame.put_u8(value.len() as u8);
    frame.extend_from_slice(value);
}

fn require_remaining(bytes: &Bytes, length: usize) -> Result<(), ProtocolError> {
    if bytes.remaining() < length {
        Err(ProtocolError::TruncatedField)
    } else {
        Ok(())
    }
}

fn require_request_id(request_id: u32, present: bool) -> Result<(), ProtocolError> {
    if (request_id != 0) == present {
        Ok(())
    } else {
        Err(ProtocolError::UnexpectedRequestId)
    }
}

fn require_empty(bytes: &Bytes) -> Result<(), ProtocolError> {
    if bytes.has_remaining() {
        Err(ProtocolError::TrailingBytes)
    } else {
        Ok(())
    }
}

pub(crate) const SUBSCRIBE_KIND: u8 = SUBSCRIBE;
pub(crate) const PUBLISH_KIND: u8 = PUBLISH;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_publish_at_payload_limit() {
        let limits = Limits::default();
        let mut frame = BytesMut::new();
        frame.put_u8(VERSION);
        frame.put_u8(PUBLISH);
        frame.put_u32(7);
        frame.put_u8(6);
        frame.extend_from_slice(b"blocks");
        frame.resize(HEADER_LEN + 1 + 6 + limits.max_payload_len, 9);

        let decoded = decode_client_message(frame, &limits).unwrap();

        assert!(matches!(
            decoded,
            ClientMessage::Publish {
                request_id: 7,
                payload,
                ..
            } if payload.len() == limits.max_payload_len
        ));
    }

    #[test]
    fn rejects_trailing_hello_bytes() {
        let limits = Limits::default();
        let mut frame = BytesMut::from(&b"\x01\x01\x00\x00\x00\x01\x01ax"[..]);

        assert_eq!(
            decode_client_message(frame.split(), &limits),
            Err(ProtocolError::TrailingBytes)
        );
    }

    #[test]
    fn validates_limit_relationships() {
        let limits = Limits {
            max_frame_len: HEADER_LEN + 1 + 64 + 12 * 1024,
            ..Limits::default()
        };

        assert!(matches!(
            limits.validate(),
            Err(ProtocolError::InvalidLimits(_))
        ));
    }

    #[test]
    fn rejects_single_slot_outbound_queue() {
        let limits = Limits {
            outbound_queue: 1,
            ..Limits::default()
        };

        assert!(matches!(
            limits.validate(),
            Err(ProtocolError::InvalidLimits(_))
        ));
    }
}
