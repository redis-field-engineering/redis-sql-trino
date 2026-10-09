# Redis Cloud ClickBench results and comparisons

## Complete PR154 driver sweep: October 9, 2026 Pacific

All **43 unchanged queries, three attempts each (129 outputs), pass independent full-data validation** on 99,997,497 rows. This is one consistent connector revision, `d83d504d65cd5b3bd14fd9bd559f5acad6ffedf2`, with production JAR SHA256 `a9e069c287a027b84be4e1bbe03ef2f6025d8e843e563e49cbde7c3d6e4e70eb`. No targeted acceptance samples or older-revision timings are substituted. Q24 best warm is **2.991580s** and Q34 **62.044117s**; both previous failure cases pass all three attempts.

Trino 483, Redis Cloud 8.6, RESP3, OSS Cluster API, verified 40 shards / 1000 GB all RAM, Standard QPF, no replication, noeviction, AOF every second. Three AWS r8g.16xlarge Redis hosts plus an r7a.4xlarge Trino runner share us-east-1, VPC and physical AZ use1-az6. Data disks have 16000 IOPS / 1000 MiB/s. Trino restarts and runner page cache clears per query block; Redis stays running, so `no-cold` applies. Query limit: 1200 seconds. Best warm is the minimum of attempts 2 and 3. Persistent data size is unmeasured. No verified default-versus-OSS server comparison is available.

### Backup initialization and ingestion

This sweep used the verified compatible full-data backup. Restore plus verified index readiness took **545.174150s (9m 5s)**, versus **1036.643924s (17m 17s)** for separately measured fresh Arrow ingestion plus indexing on the same driver: **1.9015x** faster in this single-trial setup diagnostic. Restore time is not ingestion and is not reported as ClickBench `load_time`; that field retains the separately measured fresh-ingestion value, excluding download and table creation. The premature 16.445s readiness observation saw old data before asynchronous import started and is invalid and excluded.

All 40 RDB files (50,181,080,429 bytes) have verified SHA256 values. Exact restored SQL/index counts are 99,997,497, with zero index failures; post-restore Q24/Q34/Q35 outputs independently validate. The private encrypted S3 backup has 30-day object expiry and is retained after compute teardown. Raw RDB files and credentials are not published.

For future benchmark initialization, **always restore a verified compatible backup when available**. Verify every shard checksum, dataset/source/schema and Redis compatibility, exact restored SQL/index counts, zero index failures and independent query correctness. Use fresh ingestion when no suitable backup exists or when explicitly measuring ingestion performance. Keep restore setup timing separate from ingestion metrics.

| Query | Attempt 1 (s) | Attempt 2 (s) | Attempt 3 (s) | Best warm (s) | Correctness |
|---|---:|---:|---:|---:|---|
| Q1 | 2.949415 | 2.838876 | 2.839760 | 2.838876 | 3 valid outputs |
| Q2 | 0.281510 | 0.192954 | 0.185813 | 0.185813 | 3 valid outputs |
| Q3 | 6.524263 | 6.428483 | 6.478129 | 6.428483 | 3 valid outputs |
| Q4 | 4.086578 | 3.991875 | 4.009102 | 3.991875 | 3 valid outputs |
| Q5 | 106.994954 | 101.596537 | 99.663822 | 99.663822 | 3 valid outputs |
| Q6 | 71.255473 | 63.358781 | 63.014049 | 63.014049 | 3 valid outputs |
| Q7 | 67.309233 | 62.859659 | 57.794620 | 57.794620 | 3 valid outputs |
| Q8 | 2.671320 | 1.901141 | 1.954668 | 1.901141 | 3 valid outputs |
| Q9 | 108.169553 | 104.407130 | 105.166889 | 104.407130 | 3 valid outputs |
| Q10 | 111.883567 | 100.865239 | 101.657988 | 100.865239 | 3 valid outputs |
| Q11 | 116.309333 | 101.769272 | 101.259427 | 101.259427 | 3 valid outputs |
| Q12 | 110.231814 | 103.101666 | 103.428366 | 103.101666 | 3 valid outputs |
| Q13 | 71.384488 | 58.733820 | 63.422950 | 58.733820 | 3 valid outputs |
| Q14 | 109.603335 | 102.026118 | 100.548743 | 100.548743 | 3 valid outputs |
| Q15 | 83.157092 | 67.814141 | 72.072635 | 67.814141 | 3 valid outputs |
| Q16 | 107.357466 | 101.758360 | 99.840934 | 99.840934 | 3 valid outputs |
| Q17 | 108.229105 | 99.352973 | 101.241822 | 99.352973 | 3 valid outputs |
| Q18 | 108.746655 | 103.092399 | 99.390767 | 99.390767 | 3 valid outputs |
| Q19 | 112.548316 | 100.350021 | 104.333626 | 100.350021 | 3 valid outputs |
| Q20 | 0.238739 | 0.112976 | 0.105313 | 0.105313 | 3 valid outputs |
| Q21 | 4.549125 | 4.457410 | 4.460852 | 4.457410 | 3 valid outputs |
| Q22 | 14.653248 | 7.637822 | 7.606456 | 7.606456 | 3 valid outputs |
| Q23 | 14.883391 | 7.686048 | 7.678627 | 7.678627 | 3 valid outputs |
| Q24 | 3.168479 | 2.991580 | 3.005566 | 2.991580 | 3 valid outputs |
| Q25 | 88.264063 | 69.896392 | 69.610698 | 69.610698 | 3 valid outputs |
| Q26 | 67.273928 | 57.264276 | 57.938492 | 57.264276 | 3 valid outputs |
| Q27 | 80.668309 | 74.201337 | 69.456862 | 69.456862 | 3 valid outputs |
| Q28 | 90.173839 | 74.217984 | 71.970268 | 71.970268 | 3 valid outputs |
| Q29 | 73.233046 | 63.860541 | 63.317530 | 63.317530 | 3 valid outputs |
| Q30 | 66.578750 | 57.968464 | 59.927839 | 57.968464 | 3 valid outputs |
| Q31 | 112.252675 | 99.735685 | 102.151100 | 99.735685 | 3 valid outputs |
| Q32 | 116.807249 | 111.847437 | 111.193444 | 111.193444 | 3 valid outputs |
| Q33 | 113.791374 | 101.690890 | 108.469994 | 101.690890 | 3 valid outputs |
| Q34 | 73.787982 | 62.044117 | 62.563425 | 62.044117 | 3 valid outputs |
| Q35 | 73.478535 | 63.798067 | 62.253618 | 62.253618 | 3 valid outputs |
| Q36 | 70.841405 | 59.693944 | 60.705792 | 59.693944 | 3 valid outputs |
| Q37 | 2.994225 | 2.565837 | 2.373994 | 2.373994 | 3 valid outputs |
| Q38 | 2.982137 | 2.319347 | 2.202879 | 2.202879 | 3 valid outputs |
| Q39 | 0.906873 | 0.575224 | 0.551283 | 0.551283 | 3 valid outputs |
| Q40 | 4.813524 | 3.895219 | 3.910068 | 3.895219 | 3 valid outputs |
| Q41 | 1.569389 | 1.137893 | 1.026270 | 1.026270 | 3 valid outputs |
| Q42 | 1.318841 | 0.961890 | 0.912079 | 0.912079 | 3 valid outputs |
| Q43 | 2.896885 | 2.226068 | 2.197874 | 2.197874 | 3 valid outputs |

[Complete sweep evidence](benchmarks/clickbench/reports/full-d83d504-20261009/). [Backup/restore evidence](benchmarks/clickbench/reports/backup-restore-20261009/).

## PR154 full-data acceptance: October 9, 2026 Pacific

The merged [PR154](https://github.com/redis-field-engineering/redis-sql-trino/pull/154) fixes are independently validated on **99,997,497 rows** for Q24 and Q34. Q24 previously timed out at 1200 seconds on all attempts; Q34 previously failed with `MAX_AGGREGATE_GROUPS`. The unchanged SQL now produces correct results on all three attempts for both queries.

| Query purpose | Attempt 1 (s) | Attempt 2 (s) | Attempt 3 (s) | Best warm (s) | Correctness |
|---|---:|---:|---:|---:|---|
| Q24: Earliest 10 full rows whose URL contains google | 4.065388 | 3.933026 | 3.914345 | 3.914345 | 3 independent reference matches |
| Q34: Top 10 URLs by visit count | 75.063332 | 64.139660 | 61.687396 | 61.687396 | 3 independent reference matches |
| Q35: Same URL grouping with a constant column (control) | 79.409368 | 62.152862 | 62.306080 | 62.152862 | 3 independent reference matches |

Connector `d83d504d65cd5b3bd14fd9bd559f5acad6ffedf2`; production JAR SHA256 `a9e069c287a027b84be4e1bbe03ef2f6025d8e843e563e49cbde7c3d6e4e70eb`. The plugin was built from a clean Git archive with Maven and Temurin 25. Trino 483, Redis Cloud 8.6, RESP3, verified 40 shards / 1000 GB all RAM, OSS Cluster API, no replication, noeviction, AOF every second, Standard QPF. Three r8g.16xlarge Redis hosts and an r7a.4xlarge runner share AWS us-east-1, a VPC, and physical AZ `use1-az6`; all three data disks were verified at 16000 IOPS / 1000 MiB/s. Trino restarts and runner page cache clears per query block while Redis stays running (`no-cold`). Each query has a 1200-second limit; best warm is the minimum of attempts 2 and 3.

Full-data Arrow ingestion plus indexing took **1036.644 seconds (17m 17s)**; exact SQL count and indexed document count both equal 99,997,497, with zero index failures. Source Parquet SHA256: `a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`. No 10M loader trials ran in this cohort.

Q24's captured first-attempt metrics report 15,911 candidate rows received, 10 exact hash commands, 1,050 hydrated fields, and 10 scan output rows. Q34's captured plan has partial and final Trino aggregation by URL, avoiding Redis GROUPBY's 1M-group limit. The column values, row ordering, and grouped counts match retained independent full-data DuckDB references. Original CSV checksums were verified and public artifacts scanned against private credentials.

This is a targeted acceptance cohort, not a new 43-query leaderboard result. The earlier result matrix below remains pinned to its original revision; these timings must not replace two rows in that matrix. A full 43-query sweep on one revision is needed to publish a complete new matrix.

The subsequent backup/restore diagnostic and complete same-driver sweep are documented above. The targeted acceptance cohort remains separate.

[Acceptance samples, plans, metrics and correctness evidence](benchmarks/clickbench/reports/acceptance-20261009/).

## Completed full-data sweep: October 8, 2026 Pacific

The OSS Cluster API sweep completed all 43 queries, three attempts each, on **99,997,497 rows**. Independent DuckDB references validated **123 attempts**; Q24 timed out at 1200 seconds on all three attempts and Q34 exceeded Redis's 1,000,000 aggregate-group limit on all three attempts. Failed attempts are null timings. This run supersedes the partial status below; historical results remain separate.

Connector `dbcb518c541b3eba22b4471cb71509233ab09efa` was built from a clean Git archive. Production JAR SHA256: `80e92f3bc3462c278b7feda9218c00b90c6f5cc1183a5147b2fa01fcdfce91ea`. Redis 8.6.2, Trino 483, RESP3, configured 40 shards and 1000 GB all RAM, Standard QPF, no replication, noeviction, AOF every second; three r8g.16xlarge Redis hosts and an r7a.4xlarge Trino runner, co-located in AWS us-east-1 / physical AZ use1-az6. Data disks use 16,000 IOPS and 1000 MiB/s. Redis remains running; Trino restarts and runner page cache clears before each query block, so **no-cold** applies.

Fresh full-data Arrow ingestion plus indexing took **1036.954 seconds (17m 17s)**, approximately 96,434 rows/s. This excludes source download and table creation. Separate alternating 10M diagnostics measured legacy 151.540/147.753 seconds and Arrow 110.458/110.989 seconds: mean end-to-end speedup **1.35x** (26% less elapsed time). These compare encoders within the same batched loader, not the entire old loader architecture, and are not public ClickBench query results.

The first diagnostic failed only after writing/indexing 10M rows because FT.INFO returned a RESP3 map. It was preserved and excluded, the response parser was corrected, and all four trials ran on fresh tables. The connector JAR, SQL and encoders stayed pinned. Harness patch hashes are retained.

An actual default API server switch failed again with PROVISION_FAILURE; the server remained OSS-enabled. [Issue 126](https://github.com/redis-field-engineering/redis-sql-trino/issues/126#issuecomment-6073461815) records task evidence. There is no valid default-versus-OSS comparison. This is a four-instance deployment; the historical product comparisons below use different hardware and must not be treated as a current overall ranking.

[Machine-readable result and validation evidence](benchmarks/clickbench/reports/latest-20261008/). CSV checksums and the retained archive checksum were verified; public artifacts were scanned against private credentials. Persistent AOF data size was not measured.

| Query | Attempt 1 (s) | Attempt 2 (s) | Attempt 3 (s) | Best warm (s) | Status |
|---|---:|---:|---:|---:|---|
| Q1 | 3.832767 | 3.731575 | 3.743327 | 3.731575 | 3 reference-validated outputs |
| Q2 | 0.293565 | 0.206764 | 0.194698 | 0.194698 | 3 reference-validated outputs |
| Q3 | 7.483404 | 7.436010 | 7.417720 | 7.417720 | 3 reference-validated outputs |
| Q4 | 5.077072 | 5.034888 | 5.050787 | 5.034888 | 3 reference-validated outputs |
| Q5 | 107.652407 | 98.884259 | 100.012283 | 98.884259 | 3 reference-validated outputs |
| Q6 | 68.357893 | 58.657447 | 58.357384 | 58.357384 | 3 reference-validated outputs |
| Q7 | 70.852863 | 57.088298 | 57.611882 | 57.088298 | 3 reference-validated outputs |
| Q8 | 0.316646 | 0.220289 | 0.206302 | 0.206302 | 3 reference-validated outputs |
| Q9 | 110.567296 | 108.150800 | 103.201975 | 103.201975 | 3 reference-validated outputs |
| Q10 | 115.977695 | 100.087744 | 99.589758 | 99.589758 | 3 reference-validated outputs |
| Q11 | 110.140409 | 100.429488 | 99.507256 | 99.507256 | 3 reference-validated outputs |
| Q12 | 109.361463 | 100.587360 | 104.332689 | 100.587360 | 3 reference-validated outputs |
| Q13 | 70.848580 | 60.113362 | 59.159377 | 59.159377 | 3 reference-validated outputs |
| Q14 | 111.863989 | 101.014802 | 102.937841 | 101.014802 | 3 reference-validated outputs |
| Q15 | 81.741882 | 68.700690 | 70.061985 | 68.700690 | 3 reference-validated outputs |
| Q16 | 107.692175 | 99.677764 | 98.988101 | 98.988101 | 3 reference-validated outputs |
| Q17 | 111.369929 | 103.822758 | 100.857107 | 100.857107 | 3 reference-validated outputs |
| Q18 | 109.698586 | 100.120761 | 100.855060 | 100.120761 | 3 reference-validated outputs |
| Q19 | 109.871893 | 108.598808 | 107.456444 | 107.456444 | 3 reference-validated outputs |
| Q20 | 109.641855 | 99.968368 | 100.298755 | 99.968368 | 3 reference-validated outputs |
| Q21 | 73.541089 | 63.090782 | 63.000734 | 63.000734 | 3 reference-validated outputs |
| Q22 | 79.482919 | 71.005822 | 78.904122 | 71.005822 | 3 reference-validated outputs |
| Q23 | 118.720715 | 110.889096 | 106.121431 | 106.121431 | 3 reference-validated outputs |
| Q24 | null | null | null | null | 3 timeouts |
| Q25 | 85.790605 | 70.890275 | 70.546872 | 70.546872 | 3 reference-validated outputs |
| Q26 | 66.661776 | 58.224253 | 58.945083 | 58.224253 | 3 reference-validated outputs |
| Q27 | 89.616702 | 66.464186 | 68.850166 | 66.464186 | 3 reference-validated outputs |
| Q28 | 92.729550 | 75.951426 | 74.116397 | 74.116397 | 3 reference-validated outputs |
| Q29 | 72.968122 | 62.819798 | 62.549552 | 62.549552 | 3 reference-validated outputs |
| Q30 | 70.917921 | 63.769079 | 58.638641 | 58.638641 | 3 reference-validated outputs |
| Q31 | 121.650860 | 102.327436 | 101.062267 | 101.062267 | 3 reference-validated outputs |
| Q32 | 125.575019 | 109.074141 | 109.851205 | 109.074141 | 3 reference-validated outputs |
| Q33 | 114.573551 | 109.594823 | 103.893837 | 103.893837 | 3 reference-validated outputs |
| Q34 | null | null | null | null | 3 aggregate-group errors |
| Q35 | 73.332704 | 69.128806 | 64.086092 | 64.086092 | 3 reference-validated outputs |
| Q36 | 70.617349 | 64.813536 | 59.369522 | 59.369522 | 3 reference-validated outputs |
| Q37 | 2.425337 | 2.125027 | 1.925666 | 1.925666 | 3 reference-validated outputs |
| Q38 | 2.420162 | 2.056889 | 1.987625 | 1.987625 | 3 reference-validated outputs |
| Q39 | 0.453552 | 0.275276 | 0.260027 | 0.260027 | 3 reference-validated outputs |
| Q40 | 4.304982 | 3.681698 | 3.692690 | 3.681698 | 3 reference-validated outputs |
| Q41 | 3.777505 | 3.367676 | 3.210796 | 3.210796 | 3 reference-validated outputs |
| Q42 | 3.277723 | 2.721035 | 2.697576 | 2.697576 | 3 reference-validated outputs |
| Q43 | 2.332001 | 1.938546 | 1.934694 | 1.934694 | 3 reference-validated outputs |

## Historical partial runs


> **Historical baseline, superseded on 2026-10-07.** The timings and product comparisons below measure connector `83d05fc44627ccb9971a40e44e67a78f702bf2e6`. They predate merged safe integer-widening aggregation pushdown [#131](https://github.com/redis-field-engineering/redis-sql-trino/pull/131) and per-query scan metrics [#132](https://github.com/redis-field-engineering/redis-sql-trino/pull/132). A fresh 43-query, three-attempt sweep is running at `4aa71cea12abe7e2f1dc93a8f276418292edd915`, using the same full dataset, hardware, settings and timeouts. Do not interpret these older comparisons as the patched connector's performance.

The patched Cloud query-3 `EXPLAIN` pushes SUM, COUNT and AVG into Redis and removes Trino's aggregation stages. The deployed artifact SHA-256 is `d055a151b6cca28dc9ccce4ba7a1beed92be98e69a10d2d6af9647dbe72c81bb`; 47 focused regression tests passed. Full SQL count remains 99,997,497. The first four patched queries now have 12 completed, independently validated attempts; the full sweep is still running.

The first four ClickBench queries returned correct results on the full
99,997,497-row dataset, with three completed attempts per query. Performance
varies sharply: counts and a single average take seconds, while the mixed
aggregation takes about eight minutes. This is a preliminary Redis Cloud +
Trino measurement, not a completed 43-query benchmark or an overall product ranking.

## Patched query-3 result (partial rerun)

The mixed aggregation `SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth)` now pushes into Redis instead of streaming the full dataset through Trino. On the same loaded data and hardware:

| Connector revision | Try 1 (s) | Try 2 (s) | Try 3 (s) | Best warm time (s) |
|---|---:|---:|---:|---:|
| Historical `83d05fc` | 494.379892 | 466.242832 | 480.790212 | 466.242832 |
| Patched `4aa71ce` | 9.705052 | 8.664592 | 8.639648 | 8.639648 |

This is **53.97× faster** using the best of attempts 2 and 3, consistent with the earlier comparison convention. All three patched outputs match the independent full-data DuckDB reference and the original result checksum `01ddc5e9381079f340bce69ad8b6745b379c862191a001f5a68aea1f624c92bf`. This improvement applies to query 3; the complete 43-query rerun and default-versus-OSS-Cluster-API comparison remain pending. The historical tables below are retained as the original baseline.

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

Dedicated Redis Cloud subscription/database, all four EC2 instances, security group, data volumes, ephemeral artifact bucket, deadline Lambda/rule, IAM roles and instance profile are confirmed deleted. The private encrypted 40-file backup is confirmed retained with 30-day object expiry; local raw archives and validation records remain retained.
