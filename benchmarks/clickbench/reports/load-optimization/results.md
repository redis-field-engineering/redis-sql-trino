# Local loader verification — 2026-10-08

Implemented Arrow column encoding with NumPy byte materialization, persistent worker clients/readers, configurable row and byte pipeline limits, parallel prefix loading, exact index-count checks, and stage metrics. The original loader remains available.

## 10M-row local comparison

First 10,000,000 physical rows from the retained sample, all 105 columns, 8 worker processes. Each encoder runs twice in alternating order. Both use the same new worker/batching architecture, 5,000-row batches and pipeline ceiling, and a 16 MiB pipeline byte budget. This compares the original scalar value encoder with the optimized encoder; it does not measure the original loader end to end. The source was cached; no cold-cache claim is made.

| Encoder | Trial 1 | Trial 2 | Mean | Mean throughput |
|---|---:|---:|---:|---:|
| Legacy scalar | 82.900 s | 85.944 s | 84.422 s | 118,452 rows/s |
| Arrow/NumPy | 18.857 s | 19.882 s | 19.369 s | 516,282 rows/s |

Local decoding, encoding and hash-mapping preparation improved **4.36×** by the ratio of mean elapsed times. No network writes, Redis indexing or persistence are included. This is not a measured Redis Cloud or 100M-row loading speedup.

Environment: macOS-26.7.1-arm64-arm-64bit-Mach-O; Python 3.13.16; pyarrow 20.0.0; NumPy 2.2.6; redis-py 6.4.0.

## Correctness and safety

- All 105 encoded fields across 100,000 source rows matched the original encoder (10.5 million field comparisons).
- Five unit checks passed: signed 64-bit boundaries/Unicode/dates/timestamps, NULL rejection, pipeline bounds and physical keys, write-error propagation, and index-error rejection.
- A fresh local Redis 8.6.2 instance indexed 1,000 rows; all 105 stored field values per row matched the original encoder. A repeat load into that prefix was rejected.
- The temporary local Redis container was removed. No cloud resources were provisioned.

## Evidence and next measurement

[Raw comparison](10m-final/comparison.json), [local Redis verification](integration-final/redis-local-validation.json), and [experiment instructions](../../redis/LOAD-OPTIMIZATION.md). Individual trial stage metrics are retained beside the comparison; verbose logs remain local.

The earlier `10m-local` prototype was superseded and interrupted; its partial trials are excluded. The 1M prototype and earlier integration check are exploratory, not final comparisons.

Use `load-optimized.py` without `--limit` on a newly authorized dedicated environment for a fresh full-data load, following the experiment instructions. Before estimating 100M loading time, measure Redis writes plus index completion on the same 10M dataset and run the pipeline/worker matrix. Deferred indexing and compiled/raw RESP streaming remain unimplemented experiments.
