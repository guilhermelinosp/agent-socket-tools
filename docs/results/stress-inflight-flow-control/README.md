# Stress: multiplexed AST connection at C=64, before and after the in-flight fix

Command, repeated: `ast-bench --only concurrency --concurrency-levels 64 --repeats 40`.
Each run covers one cell, `ast: 1 multiplexed connection`, with 40 repeats of 2,000 operations
(80,000 operations per run). Files: `before/stress-N.{json,md,err}`, `after/after-N.{json,md,err}`.

- **before/**: 4 runs on 51638cb, flow-control fix not applied. 1 error in 320,000 operations
  (319,999 successful + 1 error, in `stress-3`). The error sample is
  `server error ResourceExhausted: server is already handling its configured maximum of in-flight requests on this connection`.
- **after/**: 12 runs, 0 errors in 960,000 operations (960,000 successful).

Without the fix, the rate observed before (1 in 320,000) predicts about 3 errors in 960,000
operations. Zero errors is consistent with the fix but is not proof: the rate rests on a single observed error.

Provenance: the `git_rev` field inside every JSON reads 51638cb, because it is the HEAD of the
repository at run time. The `after/` runs were made on the working tree that was committed as
34f06ed a few minutes later, so that field does not identify the fix.
