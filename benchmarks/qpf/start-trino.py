#!/usr/bin/env python3
"""Start the pinned connector with one split or automatic selection."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request

ROOT = Path(os.environ.get('QPF_ROOT', '/opt/qpf'))
CONTAINER = 'qpf-trino'


def start(splits):
    assert splits in (0, 1)
    connection = json.loads((ROOT / 'connection.json').read_text())
    manifest = json.loads((ROOT / 'runtime.json').read_text())
    directory = ROOT / 'trino-etc'
    (directory / 'catalog').mkdir(parents=True, exist_ok=True)
    properties = {
        'connector.name': 'redisearch', 'redisearch.uri': f"redis://{connection['host']}:{connection['port']}?timeout=1200s",
        'redisearch.password': connection['password'], 'redisearch.username': connection.get('username', 'default'),
        'redisearch.cluster': 'true', 'redisearch.default-schema-name': 'default',
        'redisearch.query-timeout-ms': 1200000, 'redisearch.aggregation-pushdown.enabled': 'true',
        'redisearch.scan-connections': 8, 'redisearch.scan-splits': splits, 'redisearch.cursor-count': 1000}
    catalog = directory / 'catalog/redis.properties'
    catalog.write_text(''.join(f'{key}={value}\n' for key, value in properties.items()))
    catalog.chmod(0o600)
    (directory / 'config.properties').write_text('''coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=18080
discovery.uri=http://localhost:18080
query.max-memory=5GB
query.max-memory-per-node=5GB
memory.heap-headroom-per-node=2GB
query.max-history=2000
''')
    (directory / 'log.properties').write_text('com.redis.trino.RediSearchAutoScanPlanner=DEBUG\n')
    if not (directory / 'jvm.config').exists():
        image = manifest['trinoImage']
        container = subprocess.check_output(['docker', 'create', image], text=True).strip()
        subprocess.run(['docker', 'cp', container + ':/etc/trino/jvm.config', str(directory / 'jvm.config')], check=True)
        subprocess.run(['docker', 'rm', container], check=True, stdout=subprocess.DEVNULL)
        lines = (directory / 'jvm.config').read_text().splitlines()
        (directory / 'jvm.config').write_text('\n'.join('-Xmx8G' if line.startswith('-Xmx') else line for line in lines) + '\n')
    subprocess.run(['chown', '-R', '1000:1000', str(directory)], check=True)
    subprocess.run(['docker', 'rm', '-f', CONTAINER], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    command = ['docker', 'run', '-d', '--name', CONTAINER, '--network', 'host', '--memory', '12g', '--cpuset-cpus', '8-15',
        '-v', f'{ROOT}/plugin/redis-sql-trino-0.4.2-SNAPSHOT:/usr/lib/trino/plugin/redisearch:ro',
        '-v', f'{directory}/catalog:/etc/trino/catalog:ro']
    for filename in ['jvm.config', 'config.properties', 'log.properties']:
        command += ['-v', f'{directory}/{filename}:/etc/trino/{filename}:ro']
    subprocess.run(command + [manifest['trinoImage']], check=True, stdout=subprocess.DEVNULL)
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen('http://127.0.0.1:18080/v1/info', timeout=5) as response:
                if not json.load(response).get('starting'):
                    return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError('Trino did not become ready')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--splits', type=int, choices=[0, 1], default=0)
    start(parser.parse_args().splits)
