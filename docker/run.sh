#!/bin/bash

set -e
(
if lsof -Pi :6379 -sTCP:LISTEN -t >/dev/null ; then
    echo "Please terminate the local redis-server on 6379"
    exit 1
fi
if lsof -Pi :8080 -sTCP:LISTEN -t >/dev/null ; then
    echo "Please terminate the local server on 8080"
    exit 1
fi
)

echo "Building the Trino connector for RediSearch"
(
cd ..
./gradlew clean build -x test
unzip -j ./subprojects/trino-redisearch/build/distributions/trino-redisearch-*.zip -d ./subprojects/trino-redisearch/build/distributions/trino-redisearch
)

echo "Starting docker ."
docker-compose up -d

function clean_up {
    echo -e "\n\nSHUTTING DOWN\n\n"
    docker-compose down
    if [ -z "$1" ]
    then
      echo -e "Bye!\n"
    else
      echo -e "$1"
    fi
}

sleep 5

trap clean_up EXIT

sleep 1

redis-cli FT.CREATE beers ON HASH PREFIX 1 beer: SCHEMA id TAG SORTABLE brewery_id TAG SORTABLE name TEXT SORTABLE abv NUMERIC SORTABLE descript TEXT style_name TAG SORTABLE cat_name TAG SORTABLE

riot-file import http://developer.redis.com/riot/beers.json hset --keyspace beer --keys id

docker exec -it trino trino --catalog redisearch --schema default