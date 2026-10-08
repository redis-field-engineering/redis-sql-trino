#!/usr/bin/env python3
"""Manage an isolated Redis Software benchmark; credentials stay in a private file."""
import argparse
import base64
import json
import os
from pathlib import Path
import secrets
import ssl
import subprocess
import time
import urllib.error
import urllib.request

ROOT = Path(os.environ.get('QPF_ROOT', '/opt/qpf'))
SECRET = ROOT / 'software-secret.json'
IMAGE = 'redislabs/redis:8.2.0-78.18'
CONTAINER = 'qpf-redis-software'


def write_private(path, data):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as handle:
        json.dump(data, handle)


def api(path, method='GET', body=None, authenticated=True):
    headers = {'Content-Type': 'application/json'}
    if authenticated:
        config = json.loads(SECRET.read_text())
        token = base64.b64encode(f"{config['username']}:{config['password']}".encode()).decode()
        headers['Authorization'] = 'Basic ' + token
    request = urllib.request.Request('https://127.0.0.1:9443' + path, method=method,
        headers=headers, data=None if body is None else json.dumps(body).encode())
    # The isolated Software container uses its generated self-signed certificate.
    try:
        with urllib.request.urlopen(request, context=ssl._create_unverified_context(), timeout=30) as response:
            raw = response.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as error:
        detail = error.read().decode('utf-8', 'replace')
        if SECRET.exists():
            detail = detail.replace(json.loads(SECRET.read_text())['password'], '[redacted]')
        raise RuntimeError(f'Software API {method} {path}: HTTP {error.code}: {detail[:1000]}') from None


def wait_for(check, seconds=300):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except Exception as error:
            last = error
        time.sleep(2)
    raise RuntimeError(f'Readiness deadline exceeded: {last}')


def snapshot():
    db = api('/v1/bdbs/' + str(database_uid()))
    allowed = ('uid', 'name', 'status', 'memory_size', 'redis_version', 'shards_count',
               'replication', 'data_persistence', 'aof_policy', 'query_performance_factor',
               'oss_cluster', 'module_list', 'port', 'eviction_policy')
    return {key: db.get(key) for key in allowed}


def set_factor(factor):
    assert factor in (0, 2)
    api('/v1/bdbs/' + str(database_uid()), 'PUT', {'query_performance_factor': {'active': factor != 0, 'scaling_factor': factor}})
    def ready():
        db = snapshot()
        qpf = db.get('query_performance_factor') or {}
        if db['status'] == 'active' and bool(qpf.get('active')) == (factor != 0):
            if factor == 0 or qpf.get('scaling_factor') == factor:
                return db
        return None
    return wait_for(ready)


def database_uid():
    databases = [db for db in api('/v1/bdbs') if db['name'] == 'qpf-controlled-5m']
    assert len(databases) == 1, 'Expected exactly one owned benchmark database'
    return databases[0]['uid']


def bootstrap():
    ROOT.mkdir(parents=True, exist_ok=True)
    if SECRET.exists():
        assert software_cluster_name() == 'qpf-benchmark.local', 'Refuse to modify a different cluster'
        if api('/v1/bdbs'):
            finish_database(json.loads(SECRET.read_text()))
            return
        create_database(json.loads(SECRET.read_text()))
        return
    write_private(SECRET, {'username': 'benchmark@redis.test', 'password': secrets.token_urlsafe(32)})
    image = json.loads((ROOT / 'runtime.json').read_text()).get('redisImage', IMAGE)
    subprocess.run(['docker', 'run', '-d', '--name', CONTAINER, '--network', 'host',
        '--cap-add', 'SYS_RESOURCE', '--memory', '48g', '--cpuset-cpus', '0-7', image], check=True)
    wait_for(lambda: api('/v1/bootstrap', authenticated=False))
    credentials = json.loads(SECRET.read_text())
    api('/v1/bootstrap/create_cluster', 'POST', {
        'action': 'create_cluster', 'cluster': {'name': 'qpf-benchmark.local'},
        'node': {'paths': {'persistent_path': '/var/opt/redislabs/persist', 'ephemeral_path': '/var/opt/redislabs/tmp'}},
        'credentials': credentials}, authenticated=False)
    node = wait_for(lambda: api('/v1/nodes/1'))
    versions = [v['redis_version'] for v in node['supported_database_versions'] if v['db_type'] == 'redis']
    assert '8.6' in versions, versions
    api('/v1/nodes/1', 'PUT', {'external_addr': ['127.0.0.1']})
    create_database(credentials)


def software_cluster_name():
    return api('/v1/cluster')['name']


def create_database(credentials):
    db = api('/v1/bdbs', 'POST', {
        'name': 'qpf-controlled-5m', 'type': 'redis', 'memory_size': 36 * 1024**3,
        'port': 12000, 'redis_version': '8.6', 'authentication_redis_pass': credentials['password'],
        'module_list': [{'module_name': 'search', 'module_args': ''}, {'module_name': 'ReJSON', 'module_args': ''}],
        'sharding': True, 'shards_count': 2, 'replication': False, 'oss_cluster': True,
        'oss_cluster_api_preferred_ip_type': 'external', 'proxy_policy': 'all-master-shards',
        'shard_key_regex': [{'regex': '.*\\{(?<tag>.*)\\}.*'}, {'regex': '(?<tag>.*)'}],
        'data_persistence': 'aof', 'aof_policy': 'appendfsync-every-sec', 'eviction_policy': 'noeviction'})
    finish_database(credentials)


def finish_database(credentials):
    wait_for(lambda: snapshot() if snapshot()['status'] == 'active' else None)
    write_private(ROOT / 'connection.json', {'host': '127.0.0.1', 'port': 12000,
        'username': 'default', 'password': credentials['password'], 'protocol': 3})
    print(json.dumps(snapshot()))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['bootstrap', 'snapshot', 'standard', '2x'])
    args = parser.parse_args()
    if args.action == 'bootstrap':
        bootstrap()
    else:
        print(json.dumps(snapshot() if args.action == 'snapshot' else set_factor(0 if args.action == 'standard' else 2)))
