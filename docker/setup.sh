#!/bin/bash
set -e

# The Trino image has no package manager to install gettext, so substitute ${VAR} references with awk
envsubst() {
  awk '{
    line = $0; out = ""
    while (match(line, /\$\{[A-Za-z_][A-Za-z0-9_]*\}/)) {
      out = out substr(line, 1, RSTART - 1) ENVIRON[substr(line, RSTART + 2, RLENGTH - 3)]
      line = substr(line, RSTART + RLENGTH)
    }
    print out line
  }'
}

# REDISEARCH_<NAME> sets redisearch.<name>, e.g. REDISEARCH_CURSOR_COUNT sets redisearch.cursor-count.
# Unset variables are left out so the connector's defaults apply.
REDISEARCH_PROPERTIES="USERNAME PASSWORD CLUSTER RESP2 INSECURE CACERT_PATH CERT_PATH KEY_PATH KEY_PASSWORD CASE_INSENSITIVE_NAMES CURSOR_COUNT DEFAULT_LIMIT"

redisearch_catalog() {
  echo "connector.name=redisearch"
  echo "redisearch.uri=${REDISEARCH_URI:-redis://host.docker.internal:6379}"
  for name in $REDISEARCH_PROPERTIES; do
    local var=REDISEARCH_$name property=${name,,}
    if [ -n "${!var}" ]; then
      echo "redisearch.${property//_/-}=${!var}"
    fi
  done
}

# Keep the image's (or a mounted) catalog file unless a REDISEARCH_* variable is set
for name in URI $REDISEARCH_PROPERTIES; do
  var=REDISEARCH_$name
  if [ -n "${!var}" ]; then
    redisearch_catalog > /etc/trino/catalog/redisearch.properties
    break
  fi
done

export TRINO_DISCOVERY_URI=${TRINO_DISCOVERY_URI:-http://localhost:8080}
if [[ -z "${TRINO_NODE_TYPE}" ]]; then
    echo "Configuring a single-node Trino cluster"
elif [[ $TRINO_NODE_TYPE == "coordinator" ]]; then
    echo "Configuring a coordinator Trino node"
    envsubst < /tmp/coordinator.config.properties.template > /etc/trino/config.properties
elif [[ $TRINO_NODE_TYPE == "worker" ]]; then
    echo "Configuring a worker Trino node"
    envsubst < /tmp/worker.config.properties.template > /etc/trino/config.properties
else
    printf '%s\n' "Invalid TRINO_NODE_TYPE parameter: $TRINO_NODE_TYPE" >&2
    exit 1
fi

# run-trino sets node.id to the container hostname
exec /usr/lib/trino/bin/run-trino
