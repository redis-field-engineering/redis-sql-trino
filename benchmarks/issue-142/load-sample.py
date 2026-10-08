#!/usr/bin/env python3
"""Load a projection of the cached ClickBench sample into an isolated local Redis."""
import argparse
import hashlib
import json
import socket
import time
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow.compute as pc


def command(*args):
    parts = [str(arg).encode() for arg in args]
    return b'*%d\r\n' % len(parts) + b''.join(b'$%d\r\n' % len(part) + part + b'\r\n' for part in parts)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('parquet')
    parser.add_argument('--port', type=int, default=16379)
    parser.add_argument('--output', default='benchmarks/issue-142/sample.json')
    args = parser.parse_args()
    start = time.monotonic()
    source = pq.ParquetFile(args.parquet)
    with socket.create_connection(('127.0.0.1', args.port)) as connection:
        reader = connection.makefile('rb')
        def send(*tokens):
            connection.sendall(command(*tokens))
            reply = reader.readline()
            if reply.startswith(b'-'):
                raise RuntimeError(reply)
            return reply
        # A new index is required; an existing table is never overwritten.
        send('FT.CREATE', 'hits', 'ON', 'HASH', 'PREFIX', 1, 'hits:', 'SKIPINITIALSCAN', 'SCHEMA',
             'ordinal', 'NUMERIC', 'userid', 'NUMERIC', 'searchphrase', 'TAG', 'advengineid', 'NUMERIC')
        send('SET', '__trino:columns:hits', json.dumps({'ordinal': 'bigint', 'userid': 'bigint',
             'searchphrase': 'varchar', 'advengineid': 'smallint'}))
        row = 0
        for batch in source.iter_batches(batch_size=5000, columns=['UserID', 'SearchPhrase', 'AdvEngineID']):
            data = batch.to_pydict()
            writes = bytearray()
            for userid, phrase, adv in zip(data['UserID'], data['SearchPhrase'], data['AdvEngineID']):
                writes.extend(command('HSET', f'hits:{row}', 'ordinal', row, 'userid', userid,
                                      'searchphrase', phrase, 'advengineid', adv))
                row += 1
            connection.sendall(writes)
            for _ in range(batch.num_rows):
                reply = reader.readline()
                if not reply.startswith(b':'):
                    raise RuntimeError(reply)
            if row % 500000 == 0:
                print(f'loaded={row} elapsed={time.monotonic()-start:.1f}', flush=True)
    table = source.read(columns=['UserID', 'SearchPhrase', 'AdvEngineID'])
    result = {'rows': row, 'parquet_sha256': hashlib.file_digest(open(args.parquet, 'rb'), 'sha256').hexdigest(),
              'q5': pc.count_distinct(table['UserID']).as_py(), 'q6': pc.count_distinct(table['SearchPhrase']).as_py(),
              'scan': pc.sum(pc.utf8_length(table['SearchPhrase'])).as_py(),
              'q3': pc.sum(table['AdvEngineID']).as_py(), 'load_seconds': time.monotonic()-start,
              'projection': ['ordinal (generated)', 'UserID', 'SearchPhrase', 'AdvEngineID']}
    Path(args.output).write_text(json.dumps(result, indent=2)+'\n')
    print(result)


if __name__ == '__main__':
    main()
