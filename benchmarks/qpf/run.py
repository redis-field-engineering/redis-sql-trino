#!/usr/bin/env python3
"""Controlled Redis Software QPF x connector splits x SQL concurrency benchmark."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import random
import subprocess
import tarfile
import threading
import time
import urllib.request
import urllib.parse

import boto3
import redis
import software

ROOT = software.ROOT
OUT = ROOT / 'out'
QUERIES = [3, 5, 6, 30]
SERVER = 'http://127.0.0.1:18080'
HEADERS = {'X-Trino-User': 'qpf-benchmark', 'X-Trino-Catalog': 'redis',
           'X-Trino-Schema': 'default', 'X-Trino-Time-Zone': 'UTC',
           'X-Trino-Session': 'query_max_run_time=1200s'}


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def connection():
    return redis.RedisCluster(**json.loads((ROOT / 'connection.json').read_text()),
        decode_responses=True, socket_connect_timeout=10, socket_timeout=1200,
        retry=redis.retry.Retry(redis.backoff.NoBackoff(), 0), cluster_error_retry_attempts=0)


def health(label):
    with connection() as client:
        info = client.execute_command('FT.INFO', 'hits_5m')
        reply = client.execute_command('FT.AGGREGATE', 'hits_5m', '*', 'TIMEOUT', '1200000',
            'GROUPBY', '0', 'REDUCE', 'COUNT', '0', 'AS', 'documents', 'DIALECT', '2')
    result = {'label': label, 'utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
              'indexDocuments': int(info['num_docs']), 'indexing': int(info['indexing']),
              'indexFailures': int(info.get('hash_indexing_failures', 0)),
              'documents': int(reply['results'][0]['extra_attributes']['documents']),
              'warnings': reply.get('warning', [])}
    write(OUT / f'{label}-health.json', result)
    assert result['indexDocuments'] == result['documents'] == 5_000_000, result
    assert not result['indexing'] and not result['indexFailures'] and not result['warnings'], result
    return result


def shard_settings():
    uid = software.database_uid()
    db = software.api(f'/v1/bdbs/{uid}?extended=true')
    keys = ['process_id', 'redis_version', 'aof_rewrite_in_progress', 'rdb_bgsave_in_progress']
    shards = software.api(f'/v1/bdbs/{uid}/shards?' + urllib.parse.urlencode([('extra_info_keys', key) for key in keys]))
    # REST INFO snapshots identify the processes; read live /proc counters to avoid
    # sampling the management API's cached CPU statistics around timed rounds.
    pids = [int(shard['redis_info']['process_id']) for shard in shards]
    code = """import json,os,time
from pathlib import Path
result = {}
for pid in %r:
    root = Path('/proc') / str(pid)
    fields = (root / 'stat').read_text().split(') ', 1)[1].split()
    ticks = os.sysconf('SC_CLK_TCK')
    result[str(pid)] = {'cpuUserSeconds': int(fields[11]) / ticks,
        'cpuSystemSeconds': int(fields[12]) / ticks,
        'cpuSampleMonotonic': time.monotonic(),
        'threadNames': sorted(path.read_text().strip() for path in root.glob('task/*/comm'))}
print(json.dumps(result))
""" % pids
    processes = json.loads(subprocess.check_output(['docker', 'exec', 'qpf-redis-software', 'python3', '-c', code], text=True))
    result = []
    for shard in shards:
        info = shard['redis_info']
        result.append({'shardId': shard['uid'], 'processId': info['process_id'],
            'redisVersion': info['redis_version'], 'workers': int(db['search']['search-workers']),
            'workerSettingSource': 'Software extended database configuration',
            **processes[str(info['process_id'])],
            'aofRewriteInProgress': info.get('aof_rewrite_in_progress'),
            'rdbSaveInProgress': info.get('rdb_bgsave_in_progress')})
    assert len(result) == 2, result
    return result


def cgroup_path(container):
    pid = subprocess.check_output(['docker', 'inspect', '-f', '{{.State.Pid}}', container], text=True).strip()
    group = next(line.split('::', 1)[1] for line in Path(f'/proc/{pid}/cgroup').read_text().splitlines() if line.startswith('0::'))
    return Path('/sys/fs/cgroup') / group.lstrip('/')


def usage(path):
    cpu = dict(line.split() for line in (path / 'cpu.stat').read_text().splitlines())
    return {'cpuSeconds': int(cpu['usage_usec']) / 1e6,
            'memoryBytes': int((path / 'memory.current').read_text()),
            'throttledSeconds': int(cpu.get('throttled_usec', 0)) / 1e6}


class Monitor:
    def __init__(self):
        self.paths = {name: cgroup_path(name) for name in ['qpf-redis-software', 'qpf-trino']}
        self.samples = []
        self.done = threading.Event()
        self.thread = threading.Thread(target=self.collect, daemon=True)

    def sample(self):
        self.samples.append({'monotonic': time.monotonic(), **{name: usage(path) for name, path in self.paths.items()}})

    def collect(self):
        while not self.done.wait(1):
            self.sample()

    def start(self):
        self.sample()
        self.thread.start()

    def finish(self):
        self.done.set()
        self.thread.join()
        self.sample()
        elapsed = self.samples[-1]['monotonic'] - self.samples[0]['monotonic']
        summary = {}
        for name in self.paths:
            summary[name] = {
                'averageCpuCores': (self.samples[-1][name]['cpuSeconds'] - self.samples[0][name]['cpuSeconds']) / elapsed,
                'peakSampledMemoryBytes': max(row[name]['memoryBytes'] for row in self.samples),
                'throttledSeconds': self.samples[-1][name]['throttledSeconds'] - self.samples[0][name]['throttledSeconds']}
        return {'elapsedSeconds': elapsed, 'summary': summary, 'samples': self.samples}


def validate(rows, reference):
    expected = reference['rows']
    if len(rows) != len(expected):
        return False
    for actual, wanted in zip(rows, expected):
        if len(actual) != len(wanted):
            return False
        for value, target in zip(actual, wanted):
            if isinstance(target, float):
                if not math.isclose(float(value), target, rel_tol=1e-10, abs_tol=1e-8):
                    return False
            elif value != target:
                return False
    return True


def query_metrics(query_id):
    request = urllib.request.Request(SERVER + '/v1/query/' + query_id, headers=HEADERS)
    with urllib.request.urlopen(request, timeout=30) as response:
        data = json.load(response)
    stats = data['queryStats']
    scans = [{key: operator.get(key) for key in ['stageId', 'operatorType', 'totalDrivers',
        'physicalInputPositions', 'connectorMetrics']} for operator in stats.get('operatorSummaries', [])
        if operator.get('connectorMetrics')]
    return {'queryId': query_id, 'state': data['state'], 'queryStats': {key: stats.get(key) for key in
        ['elapsedTime', 'executionTime', 'queuedTime', 'totalCpuTime', 'physicalInputPositions',
         'physicalInputReadTime', 'physicalInputDataSize', 'peakUserMemoryReservation']},
        'sourceDrivers': sum(scan['totalDrivers'] or 0 for scan in scans), 'scanMetrics': scans}


def execute(sql, label, number=None, warmup=False, capture_metrics=True):
    rows = []
    query_id = None
    error = None
    seconds = None
    request = urllib.request.Request(SERVER + '/v1/statement', sql.strip().removesuffix(';').encode(), HEADERS)
    started = time.perf_counter()
    try:
        while True:
            with urllib.request.urlopen(request, timeout=1230) as response:
                page = json.load(response)
            query_id = page.get('id', query_id)
            if page.get('error'):
                raise RuntimeError(page['error']['message'])
            rows.extend(page.get('data', []))
            if not page.get('nextUri'):
                break
            request = urllib.request.Request(page['nextUri'], headers=HEADERS)
        seconds = time.perf_counter() - started
    except Exception as failure:
        error = {'type': type(failure).__name__, 'message': str(failure)}
        if query_id:
            try:
                urllib.request.urlopen(urllib.request.Request(SERVER + '/v1/query/' + query_id, method='DELETE', headers=HEADERS), timeout=10).close()
            except Exception:
                pass
    path = OUT / f'{label}.csv'
    with path.open('w', newline='') as handle:
        csv.writer(handle).writerows(rows)
    valid = error is None
    if number is not None:
        valid = valid and validate(rows, json.loads((ROOT / f'reference/q{number:02d}.json').read_text()))
    result = {'label': label, 'query': number, 'warmup': warmup, 'queryId': query_id,
              'seconds': seconds if valid else None, 'observedWallSeconds': time.perf_counter() - started,
              'valid': valid, 'error': error, 'rows': len(rows), 'outputSha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    if query_id and capture_metrics:
        metrics = query_metrics(query_id)
        write(OUT / f'{label}-metrics.json', metrics)
        result['sourceDrivers'] = metrics['sourceDrivers']
    write(OUT / f'{label}-result.json', result)
    return result


def checkpoint():
    manifest = json.loads((ROOT / 'runtime.json').read_text())
    password = json.loads((ROOT / 'connection.json').read_text())['password'].encode()
    for path in OUT.rglob('*'):
        if path.is_file():
            assert password not in path.read_bytes(), 'Credential detected in ' + path.name
    archive = ROOT / 'results.tar.gz'
    with tarfile.open(archive, 'w:gz') as bundle:
        bundle.add(OUT, arcname='out')
    boto3.client('s3', region_name='us-east-1').upload_file(str(archive), manifest['bucket'],
        manifest['prefix'] + 'results/latest.tar.gz', ExtraArgs={'ServerSideEncryption': 'AES256'})


def profile_q3(factor):
    command = ['FT.PROFILE', 'hits_5m', 'AGGREGATE', 'QUERY', '*', 'LOAD', '2',
        '@advengineid', '@resolutionwidth', 'GROUPBY', '0',
        'REDUCE', 'SUM', '1', '@advengineid', 'AS', 'sum_advengineid',
        'REDUCE', 'COUNT', '0', 'AS', 'documents',
        'REDUCE', 'AVG', '1', '@resolutionwidth', 'AS', 'avg_width', 'TIMEOUT', '1200000', 'DIALECT', '2']
    with connection() as client:
        response = client.execute_command(*command, target_nodes=client.get_default_node())
    assert not response['Results'].get('warning'), response['Results']
    assert len(response['Results']['results']) == 1, response['Results']
    write(OUT / f'qpf{factor}-q3-server-profile.json', {'command': command, 'response': response,
        'scope': 'Untimed direct Redis profile of the pushed Q3 reducers; separate from SQL latency samples.'})


def main(rounds=3, repeats=3):
    OUT.mkdir(exist_ok=True)
    assert not (OUT / 'cells.jsonl').exists(), 'Use a new result directory for a new benchmark; do not silently retry cells'
    start_spec = importlib.util.spec_from_file_location('start_trino', Path(__file__).with_name('start-trino.py'))
    start_trino = importlib.util.module_from_spec(start_spec)
    start_spec.loader.exec_module(start_trino)
    statements = (ROOT / 'queries.sql').read_text().splitlines()
    cells = [(factor, splits, concurrent) for factor in [0, 2] for splits in [1, 0] for concurrent in [1, 4, 8]]
    random.Random(20261008).shuffle(cells)
    write(OUT / 'design.json', {'seed': 20261008, 'cells': cells, 'roundsPerCell': rounds,
        'queriesPerRound': repeats * 4, 'workload': QUERIES, 'deployment': 'Redis Software',
        'warmup': 'One untimed execution of each query after each Trino restart; Redis stays running.',
        'cpuMeaning': 'cgroup v2 CPU seconds / wall seconds, including all Redis Software services or the whole Trino container.',
        'tailCaution': 'Small-sample latency quantiles are descriptive, not a production SLO.'})
    all_results = []
    initial_processes = None
    try:
        for position, (factor, splits, concurrent) in enumerate(cells, 1):
            label = f'qpf{factor}-splits{splits}-clients{concurrent}'
            config = software.set_factor(factor)
            workers = software.wait_for(lambda: (value if all(row['workers'] == (0 if factor == 0 else 3) for row in value) else None)
                if (value := shard_settings()) else None)
            processes = sorted((row['shardId'], row['processId']) for row in workers)
            if initial_processes is None:
                initial_processes = processes
            assert processes == initial_processes, 'Redis shard restart would invalidate the matched comparison'
            write(OUT / f'{label}-software.json', {'configuration': config, 'shards': workers})
            start_trino.start(splits)
            health(label + '-before')
            if not (OUT / f'qpf{factor}-q3-server-profile.json').exists():
                profile_q3(factor)
            warmups = []
            for number in QUERIES:
                result = execute(statements[number - 1], f'{label}-warmup-q{number:02d}', number, warmup=True)
                assert result['valid'], result
                warmups.append(result)
            print(json.dumps({'cell': label, 'warmupComplete': True,
                'sourceDrivers': {r['query']: r['sourceDrivers'] for r in warmups},
                'workersPerShard': [row['workers'] for row in workers]}), flush=True)
            for repeat in range(1, rounds + 1):
                work = QUERIES * repeats
                random.Random(20261008 + repeat).shuffle(work)
                health(f'{label}-r{repeat}-before')
                before_shards = shard_settings()
                monitor = Monitor()
                monitor.start()
                started = time.perf_counter()
                results = []
                try:
                    with ThreadPoolExecutor(max_workers=concurrent) as pool:
                        futures = [pool.submit(execute, statements[number - 1], f'{label}-r{repeat}-n{n:02d}-q{number:02d}', number, False, False)
                                   for n, number in enumerate(work, 1)]
                        for future in as_completed(futures):
                            result = future.result()
                            results.append(result)
                            if not result['valid']:
                                for pending in futures:
                                    pending.cancel()
                                raise RuntimeError('Incorrect or failed SQL; cohort stopped without query retry: ' + result['label'])
                finally:
                    elapsed = time.perf_counter() - started
                    cpu = monitor.finish()
                    write(OUT / f'{label}-r{repeat}-cpu.json', cpu)
                after_shards = shard_settings()
                assert sorted((row['shardId'], row['processId']) for row in after_shards) == initial_processes, 'Redis shard restarted during timed round'
                before_by_id = {row['shardId']: row for row in before_shards}
                shard_cores = 0
                for after in after_shards:
                    before = before_by_id[after['shardId']]
                    shard_cores += (after['cpuUserSeconds'] + after['cpuSystemSeconds']
                        - before['cpuUserSeconds'] - before['cpuSystemSeconds']) / (after['cpuSampleMonotonic'] - before['cpuSampleMonotonic'])
                health(f'{label}-r{repeat}-after')
                for result in results:
                    metrics = query_metrics(result['queryId'])
                    write(OUT / f"{result['label']}-metrics.json", metrics)
                    result['sourceDrivers'] = metrics['sourceDrivers']
                    write(OUT / f"{result['label']}-result.json", result)
                record = {'cell': label, 'factor': factor, 'splits': splits, 'clients': concurrent, 'round': repeat,
                    'elapsedSeconds': elapsed, 'completedQueries': len(results), 'queriesPerSecond': len(results) / elapsed,
                    'averageActiveRequests': sum(row['seconds'] for row in results) / elapsed,
                    'redisShardCpuCores': shard_cores, 'shardCountersBefore': before_shards, 'shardCountersAfter': after_shards,
                    'cpu': cpu['summary'], 'results': results, 'allCorrect': True}
                with (OUT / 'cells.jsonl').open('a') as handle:
                    handle.write(json.dumps(record) + '\n')
                all_results.extend(results)
                checkpoint()
                print(json.dumps({'position': position, 'cells': len(cells), 'cell': label, 'round': repeat,
                    'correctQueries': len(results), 'seconds': elapsed, 'queriesPerSecond': len(results) / elapsed,
                    'redisCpuCores': shard_cores}), flush=True)
        write(OUT / 'completion.json', {'complete': True, 'cells': len(cells), 'roundsPerCell': rounds,
              'measuredQueries': len(all_results), 'allCorrect': all(row['valid'] for row in all_results)})
        checkpoint()
    except BaseException as error:
        write(OUT / 'completion.json', {'complete': False, 'error': {'type': type(error).__name__, 'message': str(error)}})
        checkpoint()
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--repeats', type=int, default=3, help='Each of the four SQL queries appears this many times per round')
    args = parser.parse_args()
    main(args.rounds, args.repeats)
