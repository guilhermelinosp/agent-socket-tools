//! Protocol-agnostic measurement primitives and the result model that is
//! serialized to JSON. A [`Cell`] is the raw outcome of one closed-loop run,
//! a [`Run`] is its statistics, and an [`Entry`] groups the repeats of one
//! scenario/protocol/parameter combination with median/min/max summaries.

use serde::Serialize;
use serde_json::{json, Map, Value};
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

/// Raw outcome of one measured run. Latencies hold successful operations
/// only; failures are counted, never timed, so they cannot skew percentiles.
#[derive(Default)]
pub struct Cell {
    pub latencies_us: Vec<f64>,
    pub ok: u64,
    pub errors: u64,
    /// The first distinct error messages seen, at most [`MAX_ERROR_SAMPLES`].
    pub error_samples: Vec<String>,
    pub start: Option<Instant>,
    pub end: Option<Instant>,
}

/// How many distinct error messages are kept per cell, run and entry.
pub const MAX_ERROR_SAMPLES: usize = 5;

/// Adds `message` to `samples` unless it is already there or the list is full.
pub fn push_error_sample(samples: &mut Vec<String>, message: String) {
    if samples.len() < MAX_ERROR_SAMPLES && !samples.contains(&message) {
        samples.push(message);
    }
}

impl Cell {
    pub fn wall(&self) -> Duration {
        match (self.start, self.end) {
            (Some(s), Some(e)) => e.saturating_duration_since(s),
            _ => Duration::ZERO,
        }
    }

    /// The first error message recorded, if any.
    pub fn first_error(&self) -> Option<&str> {
        self.error_samples.first().map(String::as_str)
    }

    /// Records one completed operation that was started at `started`.
    pub fn record(&mut self, started: Instant, outcome: Result<(), String>) {
        match outcome {
            Ok(()) => self.ok_latency(started.elapsed()),
            Err(message) => self.fail(message),
        }
    }

    pub fn ok_latency(&mut self, latency: Duration) {
        self.latencies_us.push(us(latency));
        self.ok += 1;
    }

    /// Counts one failed operation (it has been answered, with an error).
    pub fn fail(&mut self, message: String) {
        self.errors += 1;
        self.note_error(message);
    }

    /// Keeps an error message for diagnostics without counting it.
    pub fn note_error(&mut self, message: String) {
        push_error_sample(&mut self.error_samples, message);
    }

    pub fn merge(cells: Vec<Cell>) -> Cell {
        let mut out = Cell::default();
        for c in cells {
            out.latencies_us.extend(c.latencies_us);
            out.ok += c.ok;
            out.errors += c.errors;
            for message in c.error_samples {
                push_error_sample(&mut out.error_samples, message);
            }
            out.start = earliest(out.start, c.start);
            out.end = latest(out.end, c.end);
        }
        out
    }
}

fn earliest(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (x, y) => x.or(y),
    }
}

fn latest(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (x, y) => x.or(y),
    }
}

pub fn us(d: Duration) -> f64 {
    d.as_secs_f64() * 1e6
}

fn sorted(values: &[f64]) -> Vec<f64> {
    let mut v = values.to_vec();
    v.sort_by(|a, b| a.total_cmp(b));
    v
}

/// Nearest-rank percentile over an already sorted slice.
pub fn percentile(sorted_values: &[f64], fraction: f64) -> Option<f64> {
    if sorted_values.is_empty() {
        return None;
    }
    let idx = ((sorted_values.len() as f64 - 1.0) * fraction).round() as usize;
    sorted_values.get(idx).copied()
}

pub fn median(values: &[f64]) -> Option<f64> {
    let v = sorted(values);
    let n = v.len();
    match n {
        0 => None,
        _ if n % 2 == 1 => Some(v[n / 2]),
        _ => Some((v[n / 2 - 1] + v[n / 2]) / 2.0),
    }
}

/// Statistics of one measured run. Latency fields are absent for runs that
/// measure only scalars (startup, CPU per op, peak RSS).
#[derive(Serialize, Clone, Debug, Default)]
pub struct Run {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p50_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p95_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p99_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throughput_ops_s: Option<f64>,
    pub ops: u64,
    pub errors: u64,
    /// The first distinct error messages of this run (empty when none).
    pub error_samples: Vec<String>,
    /// Named scalar measurements specific to a scenario.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
    #[serde(skip)]
    pub raw_us: Option<Vec<f64>>,
}

impl Run {
    pub fn from_cell(cell: &Cell, keep_raw: bool) -> Run {
        let s = sorted(&cell.latencies_us);
        let secs = cell.wall().as_secs_f64();
        Run {
            p50_us: percentile(&s, 0.50),
            p95_us: percentile(&s, 0.95),
            p99_us: percentile(&s, 0.99),
            throughput_ops_s: (secs > 0.0).then(|| cell.ok as f64 / secs),
            ops: cell.ok,
            errors: cell.errors,
            error_samples: cell.error_samples.clone(),
            extra: Map::new(),
            raw_us: keep_raw.then(|| cell.latencies_us.clone()),
        }
    }

    /// A run that carries only named scalar measurements.
    pub fn scalar(ops: u64, errors: u64, values: &[(&str, f64)]) -> Run {
        let mut run = Run {
            ops,
            errors,
            ..Run::default()
        };
        for (key, value) in values {
            run.extra.insert((*key).to_string(), json!(value));
        }
        run
    }

    fn value_of(&self, key: &str) -> Option<f64> {
        match key {
            "p50_us" => self.p50_us,
            "p95_us" => self.p95_us,
            "p99_us" => self.p99_us,
            "throughput_ops_s" => self.throughput_ops_s,
            other => self.extra.get(other).and_then(Value::as_f64),
        }
    }
}

/// The repeats of one scenario/protocol/parameter combination.
#[derive(Serialize)]
pub struct Entry {
    pub scenario: String,
    pub protocol: String,
    pub params: Value,
    pub runs: Vec<Run>,
    pub summary: Map<String, Value>,
    /// The first distinct error messages across all repeats (empty when none).
    pub error_samples: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_us: Option<Vec<Vec<f64>>>,
}

impl Entry {
    pub fn new(
        scenario: &str,
        protocol: &str,
        params: Value,
        mut runs: Vec<Run>,
        keep_raw: bool,
    ) -> Entry {
        let raw_us = keep_raw.then(|| {
            runs.iter_mut()
                .map(|r| r.raw_us.take().unwrap_or_default())
                .collect()
        });
        let summary = summarize(&runs);
        let mut error_samples = Vec::new();
        for run in &runs {
            for message in &run.error_samples {
                push_error_sample(&mut error_samples, message.clone());
            }
        }
        Entry {
            scenario: scenario.to_string(),
            protocol: protocol.to_string(),
            params,
            runs,
            summary,
            error_samples,
            raw_us,
        }
    }

    /// `stat("p50_us", "median")` reads the summary field `p50_us_median`.
    pub fn stat(&self, key: &str, which: &str) -> Option<f64> {
        self.summary
            .get(&format!("{key}_{which}"))
            .and_then(Value::as_f64)
    }

    pub fn errors_total(&self) -> u64 {
        self.summary
            .get("errors_total")
            .and_then(Value::as_u64)
            .unwrap_or(0)
    }
}

/// Median, min and max across repeats for every numeric field, plus totals.
fn summarize(runs: &[Run]) -> Map<String, Value> {
    let mut keys: BTreeSet<String> = ["p50_us", "p95_us", "p99_us", "throughput_ops_s"]
        .iter()
        .map(|k| k.to_string())
        .collect();
    for run in runs {
        keys.extend(run.extra.keys().cloned());
    }
    let mut out = Map::new();
    for key in &keys {
        let values: Vec<f64> = runs.iter().filter_map(|r| r.value_of(key)).collect();
        if values.is_empty() {
            continue;
        }
        let min = values.iter().copied().fold(f64::INFINITY, f64::min);
        let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        out.insert(format!("{key}_median"), json!(median(&values)));
        out.insert(format!("{key}_min"), json!(min));
        out.insert(format!("{key}_max"), json!(max));
    }
    out.insert(
        "ops_total".to_string(),
        json!(runs.iter().map(|r| r.ops).sum::<u64>()),
    );
    out.insert(
        "errors_total".to_string(),
        json!(runs.iter().map(|r| r.errors).sum::<u64>()),
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_of_odd_and_even_counts() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median(&[4.0, 1.0, 2.0, 3.0]), Some(2.5));
        assert_eq!(median(&[]), None);
    }

    #[test]
    fn percentile_uses_nearest_rank() {
        let s = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(percentile(&s, 0.50), Some(3.0));
        assert_eq!(percentile(&s, 0.99), Some(5.0));
        assert_eq!(percentile(&[], 0.5), None);
    }

    #[test]
    fn failures_never_enter_latency_percentiles() {
        let mut cell = Cell::default();
        cell.ok_latency(Duration::from_micros(10));
        cell.fail("boom".into());
        let run = Run::from_cell(&cell, false);
        assert_eq!(run.ops, 1);
        assert_eq!(run.errors, 1);
        assert_eq!(run.p50_us, Some(10.0));
    }

    #[test]
    fn summary_reports_median_min_max_and_error_total() {
        let runs = vec![
            Run::scalar(1, 0, &[("startup_ms", 1.0)]),
            Run::scalar(1, 0, &[("startup_ms", 3.0)]),
            Run::scalar(1, 1, &[("startup_ms", 2.0)]),
        ];
        let e = Entry::new("startup", "legacy", json!({}), runs, false);
        assert_eq!(e.stat("startup_ms", "median"), Some(2.0));
        assert_eq!(e.stat("startup_ms", "min"), Some(1.0));
        assert_eq!(e.stat("startup_ms", "max"), Some(3.0));
        assert_eq!(e.errors_total(), 1);
    }

    #[test]
    fn error_samples_are_distinct_and_capped_at_five() {
        let mut cell = Cell::default();
        for i in 0..20 {
            cell.fail(format!("error {}", i % 7));
        }
        // 20 failures, but only the first five distinct messages are kept.
        assert_eq!(cell.errors, 20);
        assert_eq!(
            cell.error_samples,
            ["error 0", "error 1", "error 2", "error 3", "error 4"]
        );
        assert_eq!(cell.first_error(), Some("error 0"));
    }

    #[test]
    fn error_samples_merge_across_cells_without_exceeding_the_cap() {
        let mut a = Cell::default();
        a.fail("x".into());
        a.fail("y".into());
        let mut b = Cell::default();
        b.fail("y".into());
        for m in ["p", "q", "r", "s"] {
            b.fail(m.into());
        }
        let merged = Cell::merge(vec![a, b]);
        assert_eq!(merged.errors, 7);
        assert_eq!(merged.error_samples, ["x", "y", "p", "q", "r"]);
    }

    #[test]
    fn entry_aggregates_error_samples_across_repeats() {
        let mut first = Cell::default();
        first.fail("timeout".into());
        let mut second = Cell::default();
        second.fail("timeout".into());
        second.fail("connection reset".into());
        let runs = vec![
            Run::from_cell(&first, false),
            Run::from_cell(&second, false),
            Run::from_cell(&Cell::default(), false),
        ];
        let e = Entry::new("concurrency", "ast", json!({}), runs, false);
        assert_eq!(e.errors_total(), 3);
        assert_eq!(e.error_samples, ["timeout", "connection reset"]);
        assert_eq!(e.runs[2].error_samples, Vec::<String>::new());
    }
}
