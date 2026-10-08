#!/usr/bin/env python3
"""Instrumented, bounded ClickBench loader; --encode-only never contacts Redis."""
import argparse
import atexit
from concurrent.futures import ProcessPoolExecutor
import json
from pathlib import Path
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from load import client, column_types, encode, EXPECTED_ROWS

STATE = {}


def encoded_columns(batch, types, encoder):
    """Return UTF-8 bytes, preserving the reference loader's field representation."""
    arrays = {name.lower(): batch.column(i) for i, name in enumerate(batch.schema.names)}
    output = []
    for name, typ in types:
        array = arrays[name]
        if array.null_count:
            raise ValueError(f'NULL in {name}')
        if encoder == 'legacy':
            output.append([encode(value, typ).encode('utf-8') for value in array.to_pylist()])
            continue
        if typ == 'timestamp(3)':
            # The source holds epoch seconds, not milliseconds. Avoid floats for IDs.
            if pa.types.is_timestamp(array.type):
                seconds = pc.cast(pc.cast(array, pa.timestamp('s'), safe=True), pa.int64())
            else:
                seconds = pc.cast(array, pa.int64(), safe=True)
            array = pc.multiply_checked(seconds, pa.scalar(1000, pa.int64()))
        elif typ == 'date' and pa.types.is_integer(array.type):
            array = pc.cast(pc.cast(array, pa.int32(), safe=True), pa.date32())
        # Arrow formats whole columns in native code; cast through string validates UTF-8.
        strings = pc.cast(array, pa.string(), safe=True)
        # NumPy materializes bytes in native code, avoiding Arrow scalar wrappers.
        output.append(pc.cast(strings, pa.binary()).to_numpy(zero_copy_only=False).tolist())
    return output


def initialize(path, options):
    STATE.update(source=pq.ParquetFile(path), options=options, types=column_types())
    STATE['names'] = [name.encode('utf-8') for name, _ in STATE['types']]
    STATE['client'] = None if options['encode_only'] else client()
    if STATE['client'] is not None:
        atexit.register(STATE['client'].close)


def run_group(task):
    group, offset, target = task
    options, source, types = STATE['options'], STATE['source'], STATE['types']
    r = STATE['client']
    metrics = dict(rows=0, decode_seconds=0.0, encode_seconds=0.0,
                   enqueue_seconds=0.0, write_seconds=0.0, pipelines=0,
                   estimated_wire_bytes=0, oversized_rows=0)
    names = STATE['names']
    # RESP framing allowance per argument deliberately overestimates typical lengths.
    overhead = 64 + sum(len(name) + 40 for name in names)
    pending, pending_bytes = 0, 0
    pipe = None if r is None else r.pipeline(transaction=False)

    def flush():
        nonlocal pending, pending_bytes, pipe
        if not pending:
            return
        started = time.perf_counter()
        if pipe is not None:
            pipe.execute()  # Fail on any Redis error; never report a partial success.
            pipe = r.pipeline(transaction=False)
        metrics['write_seconds'] += time.perf_counter() - started
        metrics['pipelines'] += 1
        pending, pending_bytes = 0, 0

    iterator = source.iter_batches(batch_size=options['batch_rows'], row_groups=[group])
    while metrics['rows'] < target:
        started = time.perf_counter()
        batch = next(iterator).slice(0, min(options['batch_rows'], target - metrics['rows']))
        metrics['decode_seconds'] += time.perf_counter() - started
        started = time.perf_counter()
        columns = encoded_columns(batch, types, options['encoder'])
        # Compute per-row payload lengths in Arrow instead of 105 Python len calls/row.
        lengths = pa.array([overhead] * batch.num_rows, type=pa.int64())
        for values in columns:
            lengths = pc.add(lengths, pc.cast(pc.binary_length(pa.array(values, type=pa.binary())), pa.int64()))
        sizes = lengths.to_pylist()
        metrics['encode_seconds'] += time.perf_counter() - started
        for position, size in enumerate(sizes):
            key = f"{options['table']}:{offset + metrics['rows']}".encode('utf-8')
            size += len(key)
            if pending and (pending >= options['pipeline_rows'] or pending_bytes + size > options['max_pipeline_bytes']):
                flush()
            if size > options['max_pipeline_bytes']:
                metrics['oversized_rows'] += 1  # Single commands cannot be split safely.
            started = time.perf_counter()
            mapping = dict(zip(names, (values[position] for values in columns)))
            if pipe is not None:
                pipe.hset(key, mapping=mapping)
            metrics['enqueue_seconds'] += time.perf_counter() - started
            metrics['rows'] += 1
            metrics['estimated_wire_bytes'] += size
            pending += 1
            pending_bytes += size
    flush()
    return metrics


def verify_index(r, table, rows, timeout):
    deadline = time.monotonic() + timeout
    while True:
        raw = r.execute_command('FT.INFO', table)
        info = dict(zip(raw[::2], raw[1::2]))
        errors = info.get(b'Index Errors', [])
        errors = dict(zip(errors[::2], errors[1::2]))
        if int(info.get(b'hash_indexing_failures', 0)) or int(errors.get(b'indexing failures', 0)):
            raise RuntimeError('Search indexing failures detected')
        if int(info[b'num_docs']) == rows and not int(info[b'indexing']):
            return
        if time.monotonic() >= deadline:
            raise RuntimeError('Index did not reach the exact target row count')
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('parquet')
    parser.add_argument('--limit', type=int)
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--batch-rows', type=int, default=5000)
    parser.add_argument('--pipeline-rows', type=int, default=5000)
    parser.add_argument('--max-pipeline-bytes', type=int, default=16 * 1024 * 1024)
    parser.add_argument('--encoder', choices=['legacy', 'arrow'], default='arrow')
    parser.add_argument('--table', default='hits')
    parser.add_argument('--encode-only', action='store_true')
    parser.add_argument('--metrics', type=Path, required=True)
    parser.add_argument('--index-timeout', type=int, default=3600)
    args = parser.parse_args()
    for name in ['workers', 'batch_rows', 'pipeline_rows', 'max_pipeline_bytes', 'index_timeout']:
        if getattr(args, name) <= 0:
            parser.error(f'{name} must be positive')
    source = pq.ParquetFile(args.parquet)
    total = source.metadata.num_rows
    target = args.limit if args.limit is not None else EXPECTED_ROWS
    if target <= 0 or target > total or (args.limit is None and total != EXPECTED_ROWS):
        parser.error('Requested row count does not match available data')
    options = {key: value for key, value in vars(args).items() if key not in ['parquet', 'metrics']}
    r = None
    if not args.encode_only:
        r = client()
        # Dedicated fresh database only. Preserve indexes/connector metadata, reject
        # any existing row keys so counts cannot conceal overwrites or stale rows.
        if next(r.scan_iter(match=f'{args.table}:*', count=1000), None) is not None:
            raise RuntimeError('Target prefix contains existing keys; use a fresh table')
        r.execute_command('FT.INFO', args.table)  # Require index before writing.
    tasks, offset = [], 0
    for group in range(source.num_row_groups):
        count = min(source.metadata.row_group(group).num_rows, target - offset)
        if count <= 0:
            break
        tasks.append((group, offset, count))
        offset += count
    started = time.perf_counter()
    with ProcessPoolExecutor(max_workers=args.workers, initializer=initialize,
                             initargs=(args.parquet, options)) as pool:
        results = list(pool.map(run_group, tasks))
    ingestion_seconds = time.perf_counter() - started
    if sum(item['rows'] for item in results) != target:
        raise RuntimeError('Incomplete load')
    index_started = time.perf_counter()
    if r is not None:
        try:
            verify_index(r, args.table, target, args.index_timeout)
        finally:
            r.close()
    elapsed = time.perf_counter() - started
    report = dict(mode='encoding-only' if args.encode_only else 'redis-load',
                  source=str(Path(args.parquet).resolve()), source_rows=total,
                  target_rows=target, options=options, ingestion_seconds=ingestion_seconds,
                  index_wait_seconds=time.perf_counter() - index_started,
                  elapsed_seconds=elapsed, rows_per_second=target / elapsed,
                  worker_totals={key: sum(item[key] for item in results) for key in results[0]},
                  correctness='encoding checks required' if args.encode_only else 'exact indexed count; query validation required')
    args.metrics.parent.mkdir(parents=True, exist_ok=True)
    args.metrics.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


if __name__ == '__main__':
    main()
