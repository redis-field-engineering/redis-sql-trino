# Redis query performance factor cohort

This cohort measured the Redis Cloud query performance factor on the pinned Redis SQL Trino connector artifact. QPF controls Redis Query Engine worker allocation; changing QPF does not enable connector multi-split scanning. The tested connector revision `74a7d8708fbe07ee96891af89ca132f16e183559` used one split with four scan connections and cursor count 1,000.

The Standard and 2x cohorts each ran Q3, Q5, Q6, and Q30 three times against the same first 5,000,000 physical Parquet rows. Each query block restarted Trino and cleared the runner page cache while Redis remained running. A fresh RedisCluster client checked an exact direct aggregate count before each block and after every attempt. The 2x database also passed two separate direct Redis and Trino readiness rounds 17 seconds apart; Standard passed its separately documented recovery rounds. No query retries were used. All 12 results in each cohort passed the retained 5M DuckDB references.

| Query | Standard median (s) | 2x median (s) |
|---|---:|---:|
| Q3 | 0.4227 | 0.4993 |
| Q5 | 24.4090 | 24.4817 |
| Q6 | 23.5797 | 21.2017 |
| Q30 | 23.0231 | 21.6553 |

The comparison is exploratory and does not support a causal QPF speedup claim. The Standard database retained indexes for 99,997,497, 10,000,000, and 5,000,000 rows; the separate 2x database contained only the 5M index. The managed Redis data volumes also differed: Standard used 16,000 IOPS and 1,000 MiB/s, while 2x used 5,992 IOPS and 250 MiB/s. The cohorts ran on separately provisioned hosts with different cache and resident-data states, and the tested 2x connector build predates the automatic multi-split scanning change.

See [`qpf-summary.json`](qpf-summary.json), [`standard/`](standard/), and [`2x/`](2x/) for manifests, timings, query outputs, health records, reference validation, and hardware metadata. The earlier attempt to change QPF on the Standard database failed in the Cloud control plane; the authorized 2x cohort therefore ran on its own temporary database, which was deleted after evidence retention.
