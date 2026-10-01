ARG TRINO_VERSION=483

FROM docker.io/library/maven:3.9-eclipse-temurin-25 AS builder
WORKDIR /root/redis-sql-trino
COPY . /root/redis-sql-trino
ENV MAVEN_FAST_INSTALL="-DskipTests -Dair.check.skip-all=true -Dmaven.javadoc.skip=true -Dmaven.gitcommitid.skip=true -B -q -T 1C"
RUN mvn package $MAVEN_FAST_INSTALL

FROM trinodb/trino:${TRINO_VERSION}

COPY --from=builder --chown=trino:trino /root/redis-sql-trino/target/redis-sql-trino-*/* /usr/lib/trino/plugin/redisearch/

USER root:root
COPY --chown=trino:trino docker/etc /etc/trino
COPY docker/template docker/setup.sh /tmp/

RUN chmod 0777 /tmp/setup.sh

USER trino:trino

CMD ["/tmp/setup.sh"]