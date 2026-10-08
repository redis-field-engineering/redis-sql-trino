# QPF 2x cohort

Redis Cloud subscription `3443582`, database `14679387`, was active with `queryPerformanceFactor=2x` during this run. The database used Redis 8.6, RAM storage, RESP3, OSS Cluster API, and no replication. The run loaded only `hits_5m`, containing the first 5,000,000 physical rows of the approved Parquet file (SHA-256 `78aecd305590b48cc3524ea93010eb0806a415de828478d261617383cd3a2eaf`). The source, index, and Trino count checks all returned exactly 5,000,000; FT.INFO reported no indexing and zero indexing failures.

The pinned connector JAR SHA-256 was `971773ea31dcfae27a0d5ab76329e1c32dd65061d8d4f22d8ff608b20baee94d` at revision `74a7d8708fbe07ee96891af89ca132f16e183559`. It ran on Trino 483 with aggregation pushdown enabled, four scan connections, cursor count 1,000, and one connector split. QPF changes Redis query-worker allocation; it does not turn on multi-split scanning.

Two independent readiness rounds, 17 seconds apart, each passed direct Redis `FT.AGGREGATE COUNT` and Trino `SELECT COUNT(*)` at 5,000,000. The timed cohort ran Q3, Q5, Q6, and Q30 three times each. Before each query block, the runner restarted Trino and cleared its Linux page cache. Redis stayed running. Every attempt used a fresh RedisCluster client for the post-attempt direct count check. Queries were not retried. All 12 result CSVs matched the retained DuckDB 5M references.

| Query | Attempts (seconds) | Median (seconds) |
|---|---|---:|
| Q3 | 0.5914, 0.4993, 0.4965 | 0.4993 |
| Q5 | 26.2594, 24.4817, 20.8059 | 24.4817 |
| Q6 | 21.3512, 21.2017, 21.1709 | 21.2017 |
| Q30 | 21.6553, 22.1671, 20.1473 | 21.6553 |

This is an exploratory comparison with the Standard cohort, not a controlled QPF-only comparison. Standard retained the 99,997,497-row `hits`, 10,000,000-row `hits_10m`, and 5,000,000-row `hits_5m` indexes. This new database contained only `hits_5m`. The 2x Redis hosts were three `r8g.16xlarge` instances; each managed data volume was 1,598 GiB gp3 at 5,992 IOPS and 250 MiB/s. Standard's managed data volumes were 16,000 IOPS and 1,000 MiB/s. The clusters had separate provisioning and cache state. No causal speedup conclusion should be drawn.

The temporary subscription, database, runner, secret, and volumes were deleted after the result archive was downloaded; local validation then completed from the retained evidence. `manifest.json`, `hardware.json`, `readiness.json`, `samples.jsonl`, `correctness.json`, query plans, CSV outputs, health checks, and Trino query metrics provide the retained evidence. The post-cohort database-wide memory snapshot was not retained; the recorded load-time endpoint INFO memory value has an uncertain scope.
