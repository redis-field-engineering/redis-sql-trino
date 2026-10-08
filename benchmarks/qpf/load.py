#!/usr/bin/env python3
"""Load exactly the retained 5M physical rows, all 105 fields, without modifying values."""
from concurrent.futures import ProcessPoolExecutor, as_completed
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import time

import pyarrow.parquet as pq
import redis

ROOT = Path(os.environ.get('QPF_ROOT', '/opt/qpf'))
ROWS = 5_000_000
SHA = '78aecd305590b48cc3524ea93010eb0806a415de828478d261617383cd3a2eaf'
spec = importlib.util.spec_from_file_location('original_load', ROOT / 'redis/load.py')
original = importlib.util.module_from_spec(spec)
spec.loader.exec_module(original)


def client():
    options = json.loads((ROOT / 'connection.json').read_text())
    return redis.RedisCluster(**options, decode_responses=True, socket_timeout=1200,
        socket_connect_timeout=10, retry=redis.retry.Retry(redis.backoff.NoBackoff(), 0))


def worker(task):
    group, offset = task
    source = pq.ParquetFile(ROOT / 'hits-5m.parquet')
    types = original.column_types()
    count = 0
    with client() as connection:
        for batch in source.iter_batches(batch_size=1000, row_groups=[group]):
            columns = {name.lower(): values for name, values in batch.to_pydict().items()}
            pipe = connection.pipeline(transaction=False)
            for position in range(batch.num_rows):
                pipe.hset(f'hits_5m:{offset + count}', mapping={name: original.encode(columns[name][position], typ) for name, typ in types})
                count += 1
            pipe.execute()
    return count


if __name__ == '__main__':
    assert hashlib.sha256((ROOT / 'hits-5m.parquet').read_bytes()).hexdigest() == SHA
    parquet = pq.ParquetFile(ROOT / 'hits-5m.parquet')
    assert parquet.metadata.num_rows == ROWS
    with client() as connection:
        info = connection.execute_command('FT.INFO', 'hits_5m')
        assert int(info['num_docs']) == 0, 'Dedicated index must be empty before loading'
    tasks = []
    offset = 0
    for group in range(parquet.num_row_groups):
        tasks.append((group, offset))
        offset += parquet.metadata.row_group(group).num_rows
    loaded = 0
    with ProcessPoolExecutor(max_workers=8) as pool:
        for future in as_completed([pool.submit(worker, task) for task in tasks]):
            loaded += future.result()
            print(json.dumps({'loadedRows': loaded, 'expectedRows': ROWS}), flush=True)
    assert loaded == ROWS
    with client() as connection:
        for _ in range(600):
            info = connection.execute_command('FT.INFO', 'hits_5m')
            assert int(info.get('hash_indexing_failures', 0)) == 0
            if int(info['num_docs']) == ROWS and not int(info['indexing']):
                break
            time.sleep(1)
        else:
            raise RuntimeError('Index did not reach 5M rows')
        result = {'rows': ROWS, 'columns': len(original.column_types()), 'datasetSha256': SHA,
                  'indexInfo': info, 'memoryInfo': connection.info('memory')}
    (ROOT / 'out').mkdir(exist_ok=True)
    (ROOT / 'out/load-integrity.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'loadedRows': loaded, 'indexingFailures': 0}), flush=True)
