#!/usr/bin/env python3
"""Sequential local encoding comparison; no Redis calls or cloud resources."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import platform
import subprocess
import sys
import time

import pyarrow.parquet as pq
from load import column_types, encode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('parquet')
    parser.add_argument('--rows', type=int, default=10000000)
    parser.add_argument('--verify-rows', type=int, default=100000)
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--repeats', type=int, default=2)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if min(args.rows, args.verify_rows, args.workers, args.repeats) <= 0:
        parser.error('All numeric options must be positive')
    args.output.mkdir(parents=True, exist_ok=True)
    loader = Path(__file__).with_name('load-optimized.py')
    spec = importlib.util.spec_from_file_location('optimized', loader)
    optimized = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(optimized)
    types = column_types()
    checked, digest = 0, hashlib.sha256()
    for batch in pq.ParquetFile(args.parquet).iter_batches(batch_size=1000):
        batch = batch.slice(0, min(batch.num_rows, args.verify_rows - checked))
        actual = optimized.encoded_columns(batch, types, 'arrow')
        raw = {name.lower(): values for name, values in batch.to_pydict().items()}
        for index, (name, typ) in enumerate(types):
            expected = [encode(value, typ).encode('utf-8') for value in raw[name]]
            if actual[index] != expected:
                raise RuntimeError(f'Encoding mismatch in {name}, batch starting {checked}')
            for value in expected:
                digest.update(len(value).to_bytes(8, 'big'))
                digest.update(value)
        checked += batch.num_rows
        if checked == args.verify_rows:
            break
    if checked != args.verify_rows:
        raise RuntimeError('Not enough rows for requested verification')
    runs = []
    for repeat in range(args.repeats):
        # Alternate order to reduce systematic filesystem-cache bias.
        encoders = ['legacy', 'arrow'] if repeat % 2 == 0 else ['arrow', 'legacy']
        for encoder in encoders:
            stem = f'{encoder}-{repeat + 1}'
            metrics = args.output / f'{stem}.json'
            command = [sys.executable, str(loader), args.parquet, '--limit', str(args.rows),
                       '--workers', str(args.workers), '--encoder', encoder, '--encode-only',
                       '--metrics', str(metrics)]
            print(f'Profiling {encoder}, repeat {repeat + 1}, {args.rows:,} rows', flush=True)
            with (args.output / f'{stem}.log').open('w') as log:
                subprocess.run(command, stdout=log, stderr=log, check=True)
            runs.append(json.loads(metrics.read_text()))
    report = dict(scope='Local decoding, encoding and HSET mapping preparation only; no Redis writes, indexing or network',
                  platform=platform.platform(), python=sys.version.split()[0],
                  loader_sha256=hashlib.sha256(loader.read_bytes()).hexdigest(),
                  verified_rows=checked, verified_columns=len(types),
                  reference_encoding_sha256=digest.hexdigest(), runs=runs,
                  completed_at_unix=time.time())
    (args.output / 'comparison.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'verified_rows': checked, 'runs': [dict(encoder=r['options']['encoder'], elapsed_seconds=r['elapsed_seconds'], rows_per_second=r['rows_per_second']) for r in runs]}))


if __name__ == '__main__':
    main()
