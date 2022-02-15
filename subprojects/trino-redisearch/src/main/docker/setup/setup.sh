#!/bin/bash
set -e

REDISEARCH_ENVS=0
if [ ! -z "${REDISEARCH_URI}" ] ; then
  REDISEARCH_ENVS=1
fi

export REDISEARCH_URI=${REDISEARCH_URI:-redis://docker.for.mac.host.internal:6379}

if [ -f /tmp/redisearch.properties.template ] && [ $REDISEARCH_ENVS -eq 1 ]; then
  envsubst < /tmp/redisearch.properties.template > /etc/trino/catalog/redisearch.properties
fi

export TRINO_NODE_ID=$(uuidgen)
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