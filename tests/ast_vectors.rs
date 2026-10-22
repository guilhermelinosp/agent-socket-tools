//! Interoperability test vectors for `docs/AST_PROTOCOL.md`.
//!
//! These are the exact bytes a conforming client or server in *any*
//! language must produce/accept for these five canonical messages. They
//! are generated from this crate's own encoder (not hand-written), then
//! frozen here as a regression test: if the wire format ever drifts —
//! intentionally or not — this test catches it, and the hex dumps quoted
//! in `docs/AST_PROTOCOL.md` are these exact values, kept in sync by hand
//! (there is no other implementation yet to cross-check against; see
//! that document's "Cross-language interoperability" section for what
//! that does and doesn't mean about this protocol's portability today).

use agent_socket_tools::ast::codec;
use agent_socket_tools::ast::message::*;
use agent_socket_tools::ast::wire::{write_frame, Frame};

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

async fn encode_frame(frame: &Frame) -> Vec<u8> {
    let mut buf = Vec::new();
    write_frame(&mut buf, frame).await.unwrap();
    buf
}

#[tokio::test]
async fn hello_vector() {
    let hello = Hello {
        protocol_version: 1,
        client_name: "example-client".to_string(),
    };
    let frame = Frame {
        msg_type: MessageType::Hello as u8,
        correlation_id: 1,
        payload: codec::encode(&hello).unwrap(),
    };
    let bytes = encode_frame(&frame).await;
    assert_eq!(bytes.len(), 59);
    assert_eq!(
        hex(&bytes),
        "00 00 00 37 01 00 00 00 00 00 00 00 01 82 b0 70 72 6f 74 6f 63 6f 6c 5f 76 65 72 73 69 6f 6e \
         01 ab 63 6c 69 65 6e 74 5f 6e 61 6d 65 ae 65 78 61 6d 70 6c 65 2d 63 6c 69 65 6e 74"
    );
}

#[tokio::test]
async fn ping_vector() {
    let frame = Frame {
        msg_type: MessageType::Ping as u8,
        correlation_id: 2,
        payload: codec::encode(&Ping {}).unwrap(),
    };
    let bytes = encode_frame(&frame).await;
    assert_eq!(bytes.len(), 14);
    assert_eq!(hex(&bytes), "00 00 00 0a 08 00 00 00 00 00 00 00 02 80");
}

#[tokio::test]
async fn invoke_vector() {
    let invoke = Invoke {
        tool: "system.info".to_string(),
        args: serde_json::json!({}),
        deadline_ms: Some(5000),
    };
    let frame = Frame {
        msg_type: MessageType::Invoke as u8,
        correlation_id: 3,
        payload: codec::encode(&invoke).unwrap(),
    };
    let bytes = encode_frame(&frame).await;
    assert_eq!(bytes.len(), 52);
    assert_eq!(
        hex(&bytes),
        "00 00 00 30 05 00 00 00 00 00 00 00 03 83 a4 74 6f 6f 6c ab 73 79 73 74 65 6d 2e 69 6e 66 6f \
         a4 61 72 67 73 80 ab 64 65 61 64 6c 69 6e 65 5f 6d 73 cd 13 88"
    );
}

#[tokio::test]
async fn error_vector() {
    let err = ErrorPayload {
        code: ErrorCode::ToolNotFound,
        message: "no such tool: nope".to_string(),
    };
    let frame = Frame {
        msg_type: MessageType::Error as u8,
        correlation_id: 3,
        payload: codec::encode(&err).unwrap(),
    };
    let bytes = encode_frame(&frame).await;
    assert_eq!(bytes.len(), 61);
    assert_eq!(
        hex(&bytes),
        "00 00 00 39 07 00 00 00 00 00 00 00 03 82 a4 63 6f 64 65 ae 54 4f 4f 4c 5f 4e 4f 54 5f 46 4f \
         55 4e 44 a7 6d 65 73 73 61 67 65 b2 6e 6f 20 73 75 63 68 20 74 6f 6f 6c 3a 20 6e 6f 70 65"
    );
}

#[tokio::test]
async fn batch_vector() {
    let batch = Batch {
        calls: vec![BatchInvoke {
            id: 1,
            tool: "system.info".to_string(),
            args: serde_json::json!({}),
            deadline_ms: None,
        }],
    };
    let frame = Frame {
        msg_type: MessageType::Batch as u8,
        correlation_id: 4,
        payload: codec::encode(&batch).unwrap(),
    };
    let bytes = encode_frame(&frame).await;
    assert_eq!(bytes.len(), 49);
    assert_eq!(
        hex(&bytes),
        "00 00 00 2d 0b 00 00 00 00 00 00 00 04 81 a5 63 61 6c 6c 73 91 83 a2 69 64 01 a4 74 6f 6f 6c \
         ab 73 79 73 74 65 6d 2e 69 6e 66 6f a4 61 72 67 73 80"
    );
}
