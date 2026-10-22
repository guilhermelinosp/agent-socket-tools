//! MessagePack encode/decode for AST payloads, chosen over CBOR and JSON
//! by measurement (`benches/serialization.rs`) rather than assumption —
//! see `docs/AST_PROTOCOL.md`'s "Serialization" section for the numbers.
//!
//! Encoding uses named-field maps (`to_vec_named`), not positional arrays:
//! slightly larger on the wire, but self-describing and tolerant of field
//! reordering or additive schema changes, which matters far more than a
//! few extra bytes for a protocol meant to be implementable from a spec
//! alone, in other languages, without sharing Rust struct layouts.

use serde::{de::DeserializeOwned, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("failed to encode payload: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("failed to decode payload: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
}

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    Ok(rmp_serde::to_vec_named(value)?)
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    Ok(rmp_serde::from_slice(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::message::{ErrorCode, ErrorPayload};

    #[test]
    fn round_trips_a_payload() {
        let original = ErrorPayload {
            code: ErrorCode::ToolNotFound,
            message: "no such tool: git.status".to_string(),
        };
        let bytes = encode(&original).unwrap();
        let decoded: ErrorPayload = decode(&bytes).unwrap();
        assert_eq!(decoded.code, ErrorCode::ToolNotFound);
        assert_eq!(decoded.message, "no such tool: git.status");
    }

    #[test]
    fn rejects_garbage_bytes() {
        let err = decode::<ErrorPayload>(&[0xff, 0xff, 0xff]);
        assert!(err.is_err());
    }
}
