//! AST Wire Protocol v1 — framing.
//!
//! Every frame on the wire has this exact layout:
//!
//! ```text
//! +----------------+--------+-------------------+------------------+
//! | length (u32)   | type   | correlation_id    | payload          |
//! | 4 bytes, BE     | 1 byte | 8 bytes, BE (u64) | `length - 9` B   |
//! +----------------+--------+-------------------+------------------+
//! ```
//!
//! - `length` is the byte count of everything *after* the length field
//!   itself — i.e. `1 (type) + 8 (correlation_id) + payload.len()`. It does
//!   not include its own 4 bytes. This is the one piece of the header a
//!   receiver must interpret before it knows how much more to read, so its
//!   width and byte order are fixed and never change across protocol
//!   versions: version negotiation happens inside `HELLO`'s payload, not
//!   in the frame header, specifically so a future version can still be
//!   framed (and rejected cleanly, instead of desyncing the stream) by a
//!   peer that only understands v1 framing.
//! - All multi-byte integers are big-endian ("network byte order"), the
//!   conventional choice for wire protocols and the one every mainstream
//!   language's standard library has a one-line helper for.
//! - `type` is one of [`crate::ast::message::MessageType`]'s discriminants.
//! - `correlation_id` is chosen by whichever side initiates a
//!   request/response pair (always the client, in this version); see
//!   `docs/AST_PROTOCOL.md` for the full correlation rules (duplicate and
//!   unknown IDs, `PING`/`PONG`, etc).
//! - `payload` is a MessagePack-encoded, named-field map (`rmp_serde`'s
//!   `to_vec_named`) of whichever struct that `type` carries — see
//!   [`crate::ast::message`]. `PING`, `PONG`, and `CANCEL`'s payloads are
//!   tiny or empty.
//!
//! Minimum frame length is 9 (type + correlation_id, empty payload).
//! Maximum is configurable (see [`FrameLimits`]); frames over the limit
//! are rejected based on the *length field alone*, before any payload
//! bytes are read or any buffer sized to the untrusted length is
//! allocated — avoiding the "allocate first, validate later" trap that a
//! hostile or buggy peer could otherwise use to force large allocations.

use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const HEADER_LEN: usize = 4 + 1 + 8;
pub const MIN_FRAME_LEN: u32 = 9; // type (1) + correlation_id (8), empty payload
pub const DEFAULT_MAX_FRAME_LEN: u32 = 4 * 1024 * 1024; // 4 MiB

#[derive(Debug, Clone, Copy)]
pub struct FrameLimits {
    pub max_frame_len: u32,
}

impl Default for FrameLimits {
    fn default() -> Self {
        Self {
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub msg_type: u8,
    pub correlation_id: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("frame length {0} is below the minimum of {MIN_FRAME_LEN}")]
    TooSmall(u32),
    #[error("frame length {len} exceeds the configured maximum of {max}")]
    TooLarge { len: u32, max: u32 },
    #[error("connection closed before a complete frame arrived")]
    Incomplete,
    #[error("socket I/O error: {0}")]
    Io(#[from] io::Error),
}

/// Reads exactly one frame, or `Ok(None)` on a clean EOF that arrived
/// between frames (not mid-frame — that's [`FrameError::Incomplete`]).
///
/// The declared length is validated against `limits` *before* the
/// payload buffer is allocated: a frame claiming to be larger than
/// `limits.max_frame_len` is rejected immediately, without ever sizing an
/// allocation to the untrusted value, and without reading past the
/// 4-byte length field.
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    limits: FrameLimits,
) -> Result<Option<Frame>, FrameError> {
    // Read the length field one byte at a time for its first byte, so a
    // true clean disconnect (zero bytes of a new frame ever arrived) can
    // be told apart from a partial one (at least one byte of the length
    // field itself arrived, then EOF) — `read_exact` alone can't make
    // that distinction, since it reports `UnexpectedEof` either way.
    let mut len_buf = [0u8; 4];
    let first_byte = reader.read(&mut len_buf[0..1]).await?;
    if first_byte == 0 {
        return Ok(None);
    }
    reader
        .read_exact(&mut len_buf[1..4])
        .await
        .map_err(|e| match e.kind() {
            io::ErrorKind::UnexpectedEof => FrameError::Incomplete,
            _ => FrameError::Io(e),
        })?;
    let len = u32::from_be_bytes(len_buf);

    if len < MIN_FRAME_LEN {
        return Err(FrameError::TooSmall(len));
    }
    if len > limits.max_frame_len {
        return Err(FrameError::TooLarge {
            len,
            max: limits.max_frame_len,
        });
    }

    let mut rest = vec![0u8; len as usize];
    reader
        .read_exact(&mut rest)
        .await
        .map_err(|e| match e.kind() {
            io::ErrorKind::UnexpectedEof => FrameError::Incomplete,
            _ => FrameError::Io(e),
        })?;

    let msg_type = rest[0];
    let correlation_id =
        u64::from_be_bytes(rest[1..9].try_into().expect("checked by MIN_FRAME_LEN"));
    let payload = rest[9..].to_vec();

    Ok(Some(Frame {
        msg_type,
        correlation_id,
        payload,
    }))
}

/// Writes exactly one frame. Returns an error (rather than panicking) if
/// `frame.payload` is large enough that `1 + 8 + payload.len()` would not
/// fit in a `u32` — this is a local programming-error guard, not expected
/// to be reachable through any network input, since [`read_frame`] never
/// produces a payload larger than `limits.max_frame_len` in the first
/// place and this crate's own tools are bounded well under `u32::MAX`.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &Frame,
) -> Result<(), FrameError> {
    let body_len = 1usize + 8 + frame.payload.len();
    let len: u32 = body_len.try_into().map_err(|_| FrameError::TooLarge {
        len: u32::MAX,
        max: u32::MAX - 1,
    })?;

    let mut header = Vec::with_capacity(HEADER_LEN);
    header.extend_from_slice(&len.to_be_bytes());
    header.push(frame.msg_type);
    header.extend_from_slice(&frame.correlation_id.to_be_bytes());

    writer.write_all(&header).await?;
    writer.write_all(&frame.payload).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn limits(max: u32) -> FrameLimits {
        FrameLimits { max_frame_len: max }
    }

    #[tokio::test]
    async fn round_trips_a_frame_through_write_then_read() {
        let frame = Frame {
            msg_type: 7,
            correlation_id: 42,
            payload: vec![1, 2, 3, 4, 5],
        };
        let mut buf = Vec::new();
        write_frame(&mut buf, &frame).await.unwrap();

        let mut cursor = Cursor::new(buf);
        let decoded = read_frame(&mut cursor, limits(1024))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decoded.msg_type, 7);
        assert_eq!(decoded.correlation_id, 42);
        assert_eq!(decoded.payload, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn round_trips_an_empty_payload() {
        let frame = Frame {
            msg_type: 1,
            correlation_id: 0,
            payload: vec![],
        };
        let mut buf = Vec::new();
        write_frame(&mut buf, &frame).await.unwrap();
        assert_eq!(buf.len(), HEADER_LEN);

        let mut cursor = Cursor::new(buf);
        let decoded = read_frame(&mut cursor, limits(1024))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decoded.payload, Vec::<u8>::new());
    }

    #[tokio::test]
    async fn two_frames_in_one_buffer_are_both_readable() {
        let a = Frame {
            msg_type: 1,
            correlation_id: 1,
            payload: vec![0xAA],
        };
        let b = Frame {
            msg_type: 2,
            correlation_id: 2,
            payload: vec![0xBB, 0xCC],
        };
        let mut buf = Vec::new();
        write_frame(&mut buf, &a).await.unwrap();
        write_frame(&mut buf, &b).await.unwrap();

        let mut cursor = Cursor::new(buf);
        let first = read_frame(&mut cursor, limits(1024))
            .await
            .unwrap()
            .unwrap();
        let second = read_frame(&mut cursor, limits(1024))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.correlation_id, 1);
        assert_eq!(second.correlation_id, 2);
        assert_eq!(second.payload, vec![0xBB, 0xCC]);
    }

    #[tokio::test]
    async fn clean_eof_between_frames_is_not_an_error() {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        let result = read_frame(&mut cursor, limits(1024)).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn eof_mid_frame_is_incomplete() {
        let frame = Frame {
            msg_type: 1,
            correlation_id: 1,
            payload: vec![1, 2, 3],
        };
        let mut buf = Vec::new();
        write_frame(&mut buf, &frame).await.unwrap();
        buf.truncate(buf.len() - 1); // chop off the last payload byte

        let mut cursor = Cursor::new(buf);
        let err = read_frame(&mut cursor, limits(1024)).await.unwrap_err();
        assert!(matches!(err, FrameError::Incomplete));
    }

    #[tokio::test]
    async fn eof_mid_length_field_is_incomplete_eof_not_a_clean_disconnect() {
        // Two bytes of a 4-byte length field: not enough to even know how
        // much more to read, and not zero bytes either.
        let mut cursor = Cursor::new(vec![0u8, 0u8]);
        let err = read_frame(&mut cursor, limits(1024)).await.unwrap_err();
        assert!(matches!(err, FrameError::Incomplete));
    }

    #[tokio::test]
    async fn length_below_minimum_is_rejected_without_reading_further() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&5u32.to_be_bytes()); // below MIN_FRAME_LEN (9)
                                                    // Deliberately no more bytes: proves the rejection happens from the
                                                    // length field alone, before trying to read a body.
        let mut cursor = Cursor::new(buf);
        let err = read_frame(&mut cursor, limits(1024)).await.unwrap_err();
        assert!(matches!(err, FrameError::TooSmall(5)));
    }

    #[tokio::test]
    async fn length_above_configured_max_is_rejected_without_allocating_it() {
        let mut buf = Vec::new();
        // A length claiming 1 GiB, with no body at all behind it. If this
        // implementation allocated a buffer sized to the claimed length
        // before validating it, this test would hang or exhaust memory
        // instead of returning an error immediately.
        buf.extend_from_slice(&(1024u32 * 1024 * 1024).to_be_bytes());
        let mut cursor = Cursor::new(buf);
        let err = read_frame(&mut cursor, limits(DEFAULT_MAX_FRAME_LEN))
            .await
            .unwrap_err();
        assert!(matches!(err, FrameError::TooLarge { .. }));
    }

    #[tokio::test]
    async fn frame_exactly_at_the_limit_is_accepted() {
        let payload = vec![0u8; 100 - 9];
        let frame = Frame {
            msg_type: 1,
            correlation_id: 1,
            payload,
        };
        let mut buf = Vec::new();
        write_frame(&mut buf, &frame).await.unwrap();

        let mut cursor = Cursor::new(buf);
        let decoded = read_frame(&mut cursor, limits(100)).await.unwrap();
        assert!(decoded.is_some());
    }

    #[tokio::test]
    async fn frame_one_byte_over_the_limit_is_rejected() {
        let payload = vec![0u8; 100 - 9 + 1];
        let frame = Frame {
            msg_type: 1,
            correlation_id: 1,
            payload,
        };
        let mut buf = Vec::new();
        write_frame(&mut buf, &frame).await.unwrap();

        let mut cursor = Cursor::new(buf);
        let err = read_frame(&mut cursor, limits(100)).await.unwrap_err();
        assert!(matches!(err, FrameError::TooLarge { .. }));
    }

    #[tokio::test]
    async fn fragmented_frame_across_many_small_reads_is_assembled() {
        use tokio::io::duplex;
        let frame = Frame {
            msg_type: 3,
            correlation_id: 99,
            payload: b"hello world, this is a fragmented frame".to_vec(),
        };
        let mut encoded = Vec::new();
        write_frame(&mut encoded, &frame).await.unwrap();

        let (mut client, mut server) = duplex(encoded.len() + 16);
        let writer = tokio::spawn(async move {
            for byte in encoded {
                client.write_all(&[byte]).await.unwrap();
                tokio::task::yield_now().await;
            }
        });

        let decoded = read_frame(&mut server, limits(1024))
            .await
            .unwrap()
            .unwrap();
        writer.await.unwrap();
        assert_eq!(decoded.correlation_id, 99);
        assert_eq!(decoded.payload, frame.payload);
    }
}
