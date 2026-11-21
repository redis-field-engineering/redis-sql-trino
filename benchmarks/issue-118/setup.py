#!/usr/bin/env python3
"""Bootstrap only the disposable trino-issue118-software Docker cluster."""
import base64
import json
import ssl
import time
import urllib.error
import urllib.request

BASE = 'https://127.0.0.1:19443/v1'
CONTEXT = ssl._create_unverified_context()
AUTH = base64.b64encode(b'local@example.test:issue118-local-test').decode()

def request(path, body=None, authenticated=True, method=None):
    headers = {'Content-Type': 'application/json'}
    if authenticated:
        headers['Authorization'] = 'Basic ' + AUTH
    req = urllib.request.Request(BASE + path,
        data=None if body is None else json.dumps(body).encode(), headers=headers, method=method)
    with urllib.request.urlopen(req, context=CONTEXT, timeout=10) as response:
        data = response.read()
        return json.loads(data) if data else None

def wait(label, action):
    deadline = time.monotonic() + 180
    while True:
        try:
            value = action()
            if value:
                return value
        except (OSError, urllib.error.HTTPError):
            pass
        if time.monotonic() > deadline:
            raise RuntimeError('Timed out: ' + label)
        time.sleep(2)

def state():
    try:
        return request('/bootstrap', authenticated=False)['bootstrap_status']['state']
    except urllib.error.HTTPError:
        return request('/bootstrap')['bootstrap_status']['state']

wait('API', state)
if state() == 'idle':
    request('/bootstrap/create_cluster', {
        'action': 'create_cluster', 'cluster': {'name': 'cluster.local'},
        'node': {'bigstore_enabled': True, 'paths': {
            'persistent_path': '/var/opt/redislabs/persist',
            'ephemeral_path': '/var/opt/redislabs/tmp',
            'bigstore_path': '/var/opt/redislabs/flash'}},
        'credentials': {'username': 'local@example.test', 'password': 'issue118-local-test'}
    }, authenticated=False)
wait('cluster bootstrap', lambda: state() == 'completed')
node = wait('node API', lambda: request('/nodes/1'))
versions = [v['redis_version'] for v in node['supported_database_versions'] if v['db_type'] == 'redis']
redis_version = max(versions, key=lambda version: tuple(map(int, version.split('.'))))
print('Using newest supported Redis database version: ' + redis_version, flush=True)
existing = {db['name']: db for db in request('/bdbs')}
for name, port, flex in [('issue118-ram', 12018, False), ('issue118-flex', 12019, True)]:
    if name not in existing:
        spec = {'name': name, 'type': 'redis', 'redis_version': redis_version,
                'port': port, 'memory_size': (1024 if flex else 256) * 1024**2}
        if flex:
            spec.update(bigstore=True, bigstore_ram_size=256 * 1024**2, search_on_bigstore=True)
        def create():
            return request('/bdbs', spec)
        db = wait(name + ' creation', create)
    else:
        db = existing[name]
    uid = db['uid']
    wait(name + ' activation', lambda: request('/bdbs/' + str(uid))['status'] == 'active')
    db = request('/bdbs/' + str(uid))
    print(json.dumps({k: db.get(k) for k in ('name', 'uid', 'version', 'port', 'bigstore',
        'bigstore_version', 'bigstore_ram_size', 'search_on_bigstore', 'module_list')}), flush=True)
