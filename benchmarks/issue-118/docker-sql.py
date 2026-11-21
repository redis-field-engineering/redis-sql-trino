#!/usr/bin/env python3
"""Run the SQL reproduction through Trino's CLI inside Docker."""
import subprocess
import time

def sql(statement, expected_error=None, observe=False):
    result = subprocess.run(['docker', 'exec', 'trino-issue118-trino', 'trino',
        '--execute', statement, '--output-format', 'JSON'], capture_output=True, text=True)
    output = (result.stdout + result.stderr).strip()
    print(statement + '\n' + output + '\n', flush=True)
    if observe:
        print('Observed exit code: ' + str(result.returncode), flush=True)
    elif expected_error is None:
        if result.returncode:
            raise RuntimeError('SQL failed')
    elif result.returncode == 0 or expected_error not in output:
        raise RuntimeError('Expected SQL error: ' + expected_error)
    return output

for attempt in range(60):
    ready = subprocess.run(['docker', 'exec', 'trino-issue118-trino', 'trino', '--execute', 'SELECT 1'],
        capture_output=True)
    if ready.returncode == 0:
        break
    time.sleep(2)
else:
    raise RuntimeError('Trino did not become ready')

sql('CREATE TABLE software_ram.tpch.issue118_packaged (id varchar, name varchar)')
sql("INSERT INTO software_ram.tpch.issue118_packaged VALUES ('1', 'docker-desktop')")
rows = sql("SELECT name FROM software_ram.tpch.issue118_packaged WHERE id = '1'")
assert 'docker-desktop' in rows
sql('SELECT count(*) FROM software_ram.tpch.issue118_packaged')
sql('SHOW TABLES FROM software_ram.tpch')
sql('DROP TABLE software_ram.tpch.issue118_packaged')
sql('SHOW TABLES FROM no_search.tpch', "unknown command 'FT._LIST'")
sql('CREATE TABLE no_search.tpch.issue118_packaged (id varchar)', "unknown command 'FT._LIST'")

subprocess.run(['docker', 'exec', 'trino-issue118-software', '/opt/redislabs/bin/redis-cli', '-e', '-p', '12019',
    'FT.CREATE', 'issue118_packaged_flex', 'ON', 'HASH', 'PREFIX', '1', 'issue118_packaged_flex:',
    'SKIPINITIALSCAN', 'SCHEMA', 'id', 'TAG'], check=True)
sql('SHOW TABLES FROM software_flex.tpch')
sql('SELECT * FROM software_flex.tpch.issue118_packaged_flex', observe=True)
numeric = sql('CREATE TABLE software_flex.tpch.issue118_packaged_numeric (id bigint)', observe=True)
subprocess.run(['docker', 'exec', 'trino-issue118-software', '/opt/redislabs/bin/redis-cli', '-e', '-p', '12019',
    'FT.DROPINDEX', 'issue118_packaged_flex'], check=True)
if numeric == 'CREATE TABLE':
    subprocess.run(['docker', 'exec', 'trino-issue118-software', '/opt/redislabs/bin/redis-cli', '-e', '-p', '12019',
        'FT.DROPINDEX', 'issue118_packaged_numeric'], check=True)
    subprocess.run(['docker', 'exec', 'trino-issue118-software', '/opt/redislabs/bin/redis-cli', '-e', '-p', '12019',
        'DEL', '__trino:columns:issue118_packaged_numeric'], check=True)
print('Packaged Docker checks completed; inspect observed Flex outcomes above.', flush=True)
