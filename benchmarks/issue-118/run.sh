#!/usr/bin/env bash
# Local-only reproduction; uses four dedicated disposable Docker containers.
set -euo pipefail
cd "$(dirname "$0")/../.."
test_source=src/test/java/com/redis/trino/TestIssue118Retest.java
if [[ -e "$test_source" ]]; then
    echo "Refusing to overwrite $test_source" >&2
    exit 1
fi
docker pull redislabs/redis:latest
docker image inspect redislabs/redis:latest --format '{{json .RepoDigests}} {{.Id}}'
docker rm -f trino-issue118-software trino-issue118-no-search trino-issue118-acl 2>/dev/null || true
docker run -d --name trino-issue118-software --cap-add SYS_RESOURCE \
    -p 127.0.0.1:19443:9443 -p 127.0.0.1:12018:12018 -p 127.0.0.1:12019:12019 \
    redislabs/redis:latest
docker run -d --name trino-issue118-no-search -p 127.0.0.1:12020:6379 \
    --entrypoint /usr/local/bin/redis-server redis:8.4 --save '' --appendonly no
docker run -d --name trino-issue118-acl -p 127.0.0.1:12021:6379 \
    redis:8.4 redis-server --save '' --appendonly no
python3 benchmarks/issue-118/setup.py
cp benchmarks/issue-118/TestIssue118Retest.java "$test_source"
trap 'rm -f "$test_source"' EXIT
./mvnw -Dtest=TestIssue118Retest -Dasciidoctor.skip=true test
./mvnw -DskipTests -Dasciidoctor.skip=true package
plugin_version=$(python3 - <<'PY'
import xml.etree.ElementTree as ET
root = ET.parse('pom.xml').getroot()
print(root.find('{http://maven.apache.org/POM/4.0.0}version').text)
PY
)
docker rm -f trino-issue118-trino 2>/dev/null || true
docker run -d --name trino-issue118-trino -p 127.0.0.1:18018:8080 \
    -v "$PWD/target/redis-sql-trino-$plugin_version:/usr/lib/trino/plugin/redisearch:ro" \
    -v "$PWD/benchmarks/issue-118/catalog:/etc/trino/catalog:ro" trinodb/trino:483
python3 benchmarks/issue-118/docker-sql.py
# Leave the containers running for interactive inspection.
