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

REDISEARCH_ENVS=0
if [ ! -z "${REDISEARCH_URI}" ] || [ ! -z "${REDISEARCH_USERNAME}" ] || [ ! -z "${REDISEARCH_PASSWORD}" ] || [ ! -z "${REDISEARCH_CLUSTER}" ] \
|| [ ! -z "${REDISEARCH_CACERT_PATH}" ] || [ ! -z "${REDISEARCH_KEY_PATH}" ] || [ ! -z "${REDISEARCH_KEY_PASSWORD}" ] || [ ! -z "${REDISEARCH_CERT_PATH}" ]; then
  REDISEARCH_ENVS=1
fi

export REDISEARCH_URI=${REDISEARCH_URI:-redis://host.docker.internal:6379}
export REDISEARCH_USERNAME=${REDISEARCH_USERNAME}
export REDISEARCH_PASSWORD=${REDISEARCH_PASSWORD}
export REDISEARCH_CLUSTER=${REDISEARCH_CLUSTER:-false}
export REDISEARCH_CACERT_PATH=${REDISEARCH_CACERT_PATH}
export REDISEARCH_KEY_PATH=${REDISEARCH_KEY_PATH}
export REDISEARCH_KEY_PASSWORD=${REDISEARCH_KEY_PASSWORD}
export REDISEARCH_CERT_PATH=${REDISEARCH_CERT_PATH}

if [ -f /tmp/redisearch.properties.template ] && [ $REDISEARCH_ENVS -eq 1 ]; then
  envsubst < /tmp/redisearch.properties.template > /etc/trino/catalog/redisearch.properties
fi

export TRINO_NODE_ID=$(cat /proc/sys/kernel/random/uuid)
echo "TRINO_NODE_ID=$TRINO_NODE_ID"
envsubst < /tmp/trino.node.properties.template > /etc/trino/node.properties

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

chown -R trino:trino /etc/trino

/usr/lib/trino/bin/run-trino