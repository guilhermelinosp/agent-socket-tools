//! Picks AST's wire serialization format by measurement, not assumption.
//!
//! Compares JSON (`serde_json`, the existing legacy protocol's format),
//! MessagePack (`rmp-serde`), and CBOR (`ciborium`) encoding and decoding
//! a representative pair of messages: an `Invoke` request (small-ish
//! structured args) and a `Result` response carrying a `filesystem.read`-
//! sized payload (a few KB of text). Run with `cargo bench`.

use criterion::{criterion_group, criterion_main, Criterion};
use serde::{Deserialize, Serialize};
use std::hint::black_box;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InvokePayload {
    tool: String,
    args: serde_json::Value,
    deadline_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResultPayload {
    path: String,
    size_bytes: u64,
    contents: String,
}

fn invoke_sample() -> InvokePayload {
    InvokePayload {
        tool: "filesystem.read".to_string(),
        args: serde_json::json!({ "path": "notes/project-overview.txt" }),
        deadline_ms: Some(10_000),
    }
}

fn result_sample() -> ResultPayload {
    ResultPayload {
        path: "notes/project-overview.txt".to_string(),
        size_bytes: 4096,
        contents: "lorem ipsum dolor sit amet ".repeat(146), // ~4 KiB
    }
}

fn bench_invoke(c: &mut Criterion) {
    let sample = invoke_sample();
    let mut group = c.benchmark_group("encode_invoke");
    group.bench_function("json", |b| {
        b.iter(|| black_box(serde_json::to_vec(&sample).unwrap()))
    });
    group.bench_function("msgpack", |b| {
        b.iter(|| black_box(rmp_serde::to_vec_named(&sample).unwrap()))
    });
    group.bench_function("cbor", |b| {
        b.iter(|| {
            let mut buf = Vec::new();
            ciborium::into_writer(&sample, &mut buf).unwrap();
            black_box(buf)
        })
    });
    group.finish();

    let json_bytes = serde_json::to_vec(&sample).unwrap();
    let msgpack_bytes = rmp_serde::to_vec_named(&sample).unwrap();
    let mut cbor_bytes = Vec::new();
    ciborium::into_writer(&sample, &mut cbor_bytes).unwrap();

    let mut group = c.benchmark_group("decode_invoke");
    group.bench_function("json", |b| {
        b.iter(|| black_box(serde_json::from_slice::<InvokePayload>(&json_bytes).unwrap()))
    });
    group.bench_function("msgpack", |b| {
        b.iter(|| black_box(rmp_serde::from_slice::<InvokePayload>(&msgpack_bytes).unwrap()))
    });
    group.bench_function("cbor", |b| {
        b.iter(|| {
            black_box(ciborium::from_reader::<InvokePayload, _>(cbor_bytes.as_slice()).unwrap())
        })
    });
    group.finish();

    eprintln!(
        "encoded sizes (invoke): json={} msgpack={} cbor={} bytes",
        json_bytes.len(),
        msgpack_bytes.len(),
        cbor_bytes.len()
    );
}

fn bench_result(c: &mut Criterion) {
    let sample = result_sample();
    let mut group = c.benchmark_group("encode_result");
    group.bench_function("json", |b| {
        b.iter(|| black_box(serde_json::to_vec(&sample).unwrap()))
    });
    group.bench_function("msgpack", |b| {
        b.iter(|| black_box(rmp_serde::to_vec_named(&sample).unwrap()))
    });
    group.bench_function("cbor", |b| {
        b.iter(|| {
            let mut buf = Vec::new();
            ciborium::into_writer(&sample, &mut buf).unwrap();
            black_box(buf)
        })
    });
    group.finish();

    let json_bytes = serde_json::to_vec(&sample).unwrap();
    let msgpack_bytes = rmp_serde::to_vec_named(&sample).unwrap();
    let mut cbor_bytes = Vec::new();
    ciborium::into_writer(&sample, &mut cbor_bytes).unwrap();

    let mut group = c.benchmark_group("decode_result");
    group.bench_function("json", |b| {
        b.iter(|| black_box(serde_json::from_slice::<ResultPayload>(&json_bytes).unwrap()))
    });
    group.bench_function("msgpack", |b| {
        b.iter(|| black_box(rmp_serde::from_slice::<ResultPayload>(&msgpack_bytes).unwrap()))
    });
    group.bench_function("cbor", |b| {
        b.iter(|| {
            black_box(ciborium::from_reader::<ResultPayload, _>(cbor_bytes.as_slice()).unwrap())
        })
    });
    group.finish();

    eprintln!(
        "encoded sizes (result): json={} msgpack={} cbor={} bytes",
        json_bytes.len(),
        msgpack_bytes.len(),
        cbor_bytes.len()
    );
}

criterion_group!(benches, bench_invoke, bench_result);
criterion_main!(benches);
