#!/usr/bin/env python3
"""Verify optimized writes against a fresh local Redis with Search enabled."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import pyarrow.parquet as pq
import redis
from load import column_types, encode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('parquet')
    parser.add_argument('--port', type=int, default=16380)
    parser.add_argument('--rows', type=int, default=1000)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.rows <= 0 or args.rows > 10000:
        parser.error('Use 1 to 10000 rows for this small integration check')
    r = redis.Redis(host='127.0.0.1', port=args.port)
    if r.dbsize() != 0:
        raise RuntimeError('Integration check requires a fresh local database')
    args.output.mkdir(parents=True, exist_ok=True)
    types, schema = column_types(), []
    for name, typ in types:
        schema.extend([name, 'TAG' if typ in ('varchar', 'date') else 'NUMERIC'])
    r.execute_command('FT.CREATE', 'loader_check', 'ON', 'HASH', 'PREFIX', 1,
                      'loader_check:', 'SKIPINITIALSCAN', 'SCHEMA', *schema)
    with tempfile.TemporaryDirectory() as directory:
        config = Path(directory) / 'connection.json'
        config.write_text(json.dumps({'host': '127.0.0.1', 'port': args.port}))
        config.chmod(0o600)
        env = os.environ.copy()
        env['REDIS_CONNECTION_FILE'] = str(config)
        env.pop('REDIS_CLUSTER', None)
        command = [sys.executable, str(Path(__file__).with_name('load-optimized.py')),
                   args.parquet, '--limit', str(args.rows), '--workers', '2',
                   '--batch-rows', '333', '--pipeline-rows', '100',
                   '--max-pipeline-bytes', '65536', '--table', 'loader_check',
                   '--metrics', str(args.output / 'redis-local.json')]
        subprocess.run(command, env=env, check=True)
        checked = 0
        for batch in pq.ParquetFile(args.parquet).iter_batches(batch_size=1000):
            raw = {name.lower(): values for name, values in batch.to_pydict().items()}
            for pos in range(min(batch.num_rows, args.rows - checked)):
                expected = {name.encode(): encode(raw[name][pos], typ).encode() for name, typ in types}
                if r.hgetall(f'loader_check:{checked}') != expected:
                    raise RuntimeError(f'Hash value mismatch at ordinal {checked}')
                checked += 1
            if checked == args.rows:
                break
        if checked != args.rows:
            raise RuntimeError('Incomplete reference verification')
        retry = subprocess.run(command, env=env, capture_output=True)
        if retry.returncode == 0 or b'Target prefix contains existing keys' not in retry.stderr:
            raise RuntimeError('Existing-prefix guard did not reject the repeated load')
    r.close()
    report = dict(scope='Local integration; not Cloud throughput', rows=checked,
                  columns=len(types), hash_values_match_reference=True,
                  indexed_rows=checked, repeated_load_rejected=True)
    (args.output / 'redis-local-validation.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


if __name__ == '__main__':
    main()
