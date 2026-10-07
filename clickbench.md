# Redis Cloud ClickBench: initial results and comparisons

> **Historical baseline, superseded on 2026-10-07.** The timings and product comparisons below measure connector `83d05fc44627ccb9971a40e44e67a78f702bf2e6`. They predate merged safe integer-widening aggregation pushdown [#131](https://github.com/redis-field-engineering/redis-sql-trino/pull/131) and per-query scan metrics [#132](https://github.com/redis-field-engineering/redis-sql-trino/pull/132). A fresh 43-query, three-attempt sweep is running at `4aa71cea12abe7e2f1dc93a8f276418292edd915`, using the same full dataset, hardware, settings and timeouts. Do not interpret these older comparisons as the patched connector's performance.

The patched Cloud query-3 `EXPLAIN` pushes SUM, COUNT and AVG into Redis and removes Trino's aggregation stages. The deployed artifact SHA-256 is `d055a151b6cca28dc9ccce4ba7a1beed92be98e69a10d2d6af9647dbe72c81bb`; 47 focused regression tests passed. Full SQL count remains 99,997,497. Actual patched timings and correctness checks are pending.

The first four ClickBench queries returned correct results on the full
99,997,497-row dataset, with three completed attempts per query. Performance
varies sharply: counts and a single average take seconds, while the mixed
aggregation takes about eight minutes. This is a preliminary Redis Cloud +
Trino measurement, not a completed 43-query benchmark or an overall product ranking.

## Run configuration

- Date: 2026-10-07 UTC; full ClickBench Parquet dataset, all 105 columns.
- Redis Cloud Pro 8.6.2, all RAM, 1,000 GB dataset capacity, 40 shards, RESP3,
  OSS Cluster API enabled, no replication, `noeviction`, AOF every second.
- Redis hosts: three AWS `r8g.16xlarge` instances. Trino runner:
  one `r7a.4xlarge`, 128 GiB RAM, 96 GiB JVM heap.
- Client and Redis servers share the AWS account, VPC, region `us-east-1`,
  and physical availability zone `use1-az6` (`us-east-1a`).
- Redis and runner data disks: gp3, 16,000 IOPS and 1,000 MiB/s each.
- Trino 483; connector revision
  `83d05fc44627ccb9971a40e44e67a78f702bf2e6`.
- Redis aggregation, Lettuce command, and Trino query limits: 20 minutes.
  Redis timeout policy is `fail`, preventing partial results from counting as success.
- Trino restarts and the runner's page cache is cleared before each query block.
  Redis Cloud remains running, so these measurements require the `no-cold` tag.
  Every attempt consumes all result pages; there is no query-result cache.
- Flex is excluded. These results do not measure Flex v1 or v2.

## Initial Redis Cloud timings

All timings are end-to-end wall-clock seconds, including receiving the result.
Displayed values are rounded; comparisons use the recorded values.

| Query | Run 1 | Run 2 | Run 3 | Best warm run |
|---|---:|---:|---:|---:|
| Q1: Count all rows | 4.763667 | 3.831736 | 3.839600 | 3.831736 |
| Q2: Count where `AdvEngineID <> 0` | 1.201744 | 0.187878 | 0.186251 | 0.186251 |
| Q3: Sum + count + average width | 494.379892 | 466.242832 | 480.790212 | 466.242832 |
| Q4: Average `UserID` | 6.011204 | 5.080468 | 5.041198 | 5.041198 |

The exact SQL is:

```sql
SELECT COUNT(*) FROM hits;
SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;
SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;
SELECT AVG(UserID) FROM hits;
```

## Comparison with published ClickBench results

The table uses the lower timing of attempts 2 and 3, matching ClickBench's
hot-run calculation. All values are seconds, for queries Q1–Q4 above.
Published entries are the latest standard, untuned `c6a.4xlarge` entries in the
captured upstream snapshot for the four self-hosted products, plus Snowflake
Gen2 XS. Product names link to the exact source JSON at a pinned upstream commit.

| Product | Q1: count all | Q2: filtered count | Q3: mixed aggregation | Q4: AVG(UserID) |
|---|---:|---:|---:|---:|
| **Redis Cloud + Trino, OSS Cluster API** | **3.831736** | **0.186251** | **466.242832** | **5.041198** |
| [ClickHouse](https://github.com/ClickHouse/ClickBench/blob/29b09614bf8ffcbd46ac83f64804cb060422c15d/clickhouse/results/20261007/c6a.4xlarge.json) | 0.001 | 0.001 | 0.034 | 0.039 |
| [DuckDB](https://github.com/ClickHouse/ClickBench/blob/29b09614bf8ffcbd46ac83f64804cb060422c15d/duckdb/results/20260511/c6a.4xlarge.json) | 0.018 | 0.041 | 0.074 | 0.086 |
| [Trino over Parquet](https://github.com/ClickHouse/ClickBench/blob/29b09614bf8ffcbd46ac83f64804cb060422c15d/trino/results/20260511/c6a.4xlarge.json) | 2.609 | 2.753 | 2.881 | 2.632 |
| [Snowflake Gen2 XS](https://github.com/ClickHouse/ClickBench/blob/29b09614bf8ffcbd46ac83f64804cb060422c15d/snowflake/results/20260727/xs_gen2.json) | 0.093 | 0.173 | 0.231 | 0.317 |
| [PostgreSQL](https://github.com/ClickHouse/ClickBench/blob/29b09614bf8ffcbd46ac83f64804cb060422c15d/postgresql/results/20260921/c6a.4xlarge.json) | 258.348 | 258.405 | 258.393 | 258.399 |

| Published product | Result date | Configuration |
|---|---|---|
| ClickHouse | 2026-10-07 | One AWS `c6a.4xlarge` |
| DuckDB | 2026-05-11 | One AWS `c6a.4xlarge` |
| Trino over Parquet | 2026-05-11 | One AWS `c6a.4xlarge` |
| Snowflake Gen2 XS | 2026-07-27 | Snowflake XS warehouse, Gen2 |
| PostgreSQL | 2026-09-21 | One AWS `c6a.4xlarge` |

Hardware differs substantially. Redis uses three large database hosts plus a
separate Trino runner; the self-hosted comparison entries use one instance.
Versions, test dates, storage layouts, cache behavior and deployment architectures
also differ. These are observed latency comparisons, not equal-hardware or
cost-normalized measurements. Very fast count queries can benefit from metadata
or index optimizations and do not demonstrate full-table scan throughput.

## What the initial results show

- The mixed aggregation (Q3) takes 466.24 seconds: approximately
  **162× longer than Trino over Parquet** and **6,301× longer than DuckDB**
  in the selected published entries.
- Trino execution progress showed Q3 processing the full approximately 100 million
  rows. This points toward the connector/data-access path as a performance target;
  it does not isolate Redis server execution time from connector transfer and
  Trino processing.
- Redis is faster than this PostgreSQL entry on counts and the average, but Q3
  takes approximately 1.8× as long.
- The first four queries favor the analytical engines overall. The remaining
  queries are needed before drawing conclusions across the full benchmark.

## Correctness, evidence and outstanding work

All 12 completed attempts passed independent full-data DuckDB 1.4.1 reference
checks. Integer results are checked exactly; floating-point values use a relative
tolerance of `1e-10` and absolute tolerance of `1e-8`. Each query's three attempts
produced the same CSV SHA-256, listed below.

| Query | CSV output SHA-256 |
|---|---|
| Q1 | `a1c61cbdb37536d9a62a8584884da6e3aa9f9040f039f9422f14b095f0dbff36` |
| Q2 | `1dc69b996f5f9055899df69f13967aeb73dbe9d93d6e258b03363fe76cbd906a` |
| Q3 | `01ddc5e9381079f340bce69ad8b6745b379c862191a001f5a68aea1f624c92bf` |
| Q4 | `5f8caed905d8885a51334ff6eab8f16cd280ffc7f8d8f5b576e213d22e590d03` |

Observed first-attempt output rows, in query order:

```csv
99997497
630500
7280088,99997497,1513.4879349030107
2.528953029789787e+18
```

The full 43-query, three-attempt sweep remains in progress. Completed timings,
CSV outputs, checksums and correctness reports are retained by the benchmark
workflow; a complete submission will be added to
[ClickBench PR #2443](https://github.com/ClickHouse/ClickBench/pull/2443)
after the sweep and validation finish. Failed or incorrect attempts are `null`.

The default API comparison remains blocked by Redis Cloud control-plane
`PROVISION_FAILURE`, including mode changes and fresh database probes; see
[issue #126](https://github.com/redis-field-engineering/redis-sql-trino/issues/126).
Only OSS Cluster API has been measured here, so no API winner can be established.
The finishing workflow retries the server-side switch after the cluster sweep;
changing only the client flag is not a valid default API measurement.

The BIGINT grouping precision fix is merged in
[PR #125](https://github.com/redis-field-engineering/redis-sql-trino/pull/125).
The explicit aggregation timeout is in
[PR #127](https://github.com/redis-field-engineering/redis-sql-trino/pull/127).

Fresh-load time is unavailable because ingestion resumed after external runner
termination. Redis RAM measured after loading was 487,413,933,024 bytes, including
indexes. Persistent AOF size is unavailable, so this is not a complete storage-size
measurement. Submitted `load_time` and `data_size` remain null. The finishing
workflow removes dedicated infrastructure after retaining the results.
