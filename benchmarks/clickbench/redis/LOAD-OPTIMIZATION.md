# Loading performance experiments

The original `load.py` remains unchanged. `load-optimized.py` adds Arrow column
encoding with native NumPy byte materialization, one Redis client and Parquet
reader per worker process, configurable
row/byte pipeline bounds, parallel loading of a physical prefix of the dataset,
and separate decoding, encoding, enqueueing, write and index-wait measurements.
Both default and OSS Cluster API use the existing credential-file client factory;
cluster mode retains redis-py's routing rather than using a single raw RESP socket.

Use the same exact source, table schema, Redis persistence, hardware and API mode
for comparisons. Preserve all 105 fields, exact signed 64-bit IDs, millisecond
representations of source epoch seconds, ISO dates, and physical ordinal keys.
No schema changes, precision reduction or preaggregation are included.

## Local verification without cloud resources

From this directory, use Python 3.13 and install the dependencies first:

```sh
python3.13 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python test-load-optimized.py
.venv/bin/python compare-load-encoding.py /path/to/hits.parquet \
  --rows 10000000 --verify-rows 100000 --workers 8 --repeats 2 \
  --output ../reports/load-optimization/10m-local
```

The comparison alternates encoder order and checks every field in the first
100,000 physical rows against the original encoder. Synthetic checks cover
64-bit boundaries, Unicode, dates, epoch seconds, NULL rejection, pipeline bounds
and duplicate IDs. Set `--verify-rows 10000000` for full subset encoding parity.
The `legacy` comparison uses the original value encoder inside the new worker
and batching architecture; it is not a wall-time measurement of the old loader.
Both modes include row mapping preparation and the same pipeline sizing work.
Neither sends commands to Redis. Stage elapsed times are summed over workers and
may exceed wall time. Estimated wire bytes are a conservative framing estimate,
not measured network traffic. A single row larger than the configured pipeline
byte budget is sent alone and counted as `oversized_rows`.

Redis Software or Redis Cloud is the default for new performance measurements.
The following small integration check explicitly uses Redis Open Source for
loader compatibility; it is not the default query benchmark environment:

```sh
docker run -d --name clickbench-load-verification --memory 512m \
  -p 127.0.0.1:16380:6379 redis:8.6.2 redis-server --save '' --appendonly no
.venv/bin/python verify-load-local.py /path/to/hits.parquet \
  --output ../reports/load-optimization/integration
docker rm -f clickbench-load-verification
```

This verifies all stored fields and the exact indexed count, then checks that a
repeated load is rejected before any overwrite. It does not benchmark Trino.

## Redis Cloud measurement when a new environment is authorized

Create a fresh dedicated table using the connector's normal DDL, replacing the
`hits` name in `create.sql` with a unique diagnostic table such as `hits_load_10m`.
Do not load over the retained full-data benchmark or reuse a populated prefix.
Do not add hash tags to ordinal keys: they would concentrate writes in one slot.

```sh
export REDIS_CONNECTION_FILE=/secure/path/redis-connection.json
# Set true only when the database actually has OSS Cluster API enabled.
export REDIS_CLUSTER=true
.venv/bin/python load-optimized.py /path/to/hits.parquet \
  --limit 10000000 --table hits_load_10m --workers 8 \
  --batch-rows 5000 --pipeline-rows 5000 --max-pipeline-bytes 16777216 \
  --metrics ../reports/load-optimization/cloud-10m.json
```

The private connection JSON contains redis-py constructor options (`host`, `port`,
`username`, `password`, and TLS options as required by the deployment). Keep it
outside Git and readable only by the benchmark user.

The index must exist before writing. The command verifies exactly 10M indexed
rows and zero index failures. Query/reference validation is still required before
reporting query correctness. A fresh full-data load omits `--limit`; it requires
exactly 99,997,497 source rows. This new loader intentionally does not resume.

Start with a controlled matrix on fresh tables: legacy versus Arrow with identical
settings; then pipeline row limits 1000, 5000 and 10000; then workers 8, 16 and 32.
Keep the 16 MiB byte budget unless memory measurements justify changing it. Run
sequentially, repeat in reverse order, and record runner CPU/RSS/network and
per-shard CPU/write throughput, indexing state and persistence disk utilization.
The default settings are starting points, not an established optimum.

Compare total time until all target documents are indexed, not just HSET time.
Testing deferred index creation requires a separate connector-compatible procedure:
load hashes, then create a full index **without SKIPINITIALSCAN**, preserve table
metadata, and include its scan/build time. This experiment is not implemented here.
A compiled/raw RESP loader and pipelined conversion/network overlap remain possible
next steps if measured stage timings justify their complexity. No cloud speedup
or IOPS bottleneck is inferred from the local encoding experiment.
