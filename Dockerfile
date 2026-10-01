ARG TRINO_VERSION=483

# The plugin jars are platform-independent, so build them once on the native platform rather than
# under QEMU emulation for every target platform.
FROM --platform=$BUILDPLATFORM docker.io/library/maven:3.9-eclipse-temurin-25 AS builder
WORKDIR /root/redis-sql-trino
COPY . /root/redis-sql-trino
ENV MAVEN_FAST_INSTALL="-DskipTests -Dair.check.skip-all=true -Dmaven.javadoc.skip=true -Dmaven.gitcommitid.skip=true -B -q -T 1C"
RUN mvn package $MAVEN_FAST_INSTALL

# Keep RUN out of this stage: it is built once per target platform, and without RUN no target needs QEMU.
FROM trinodb/trino:${TRINO_VERSION}

COPY --from=builder --chown=trino:trino /root/redis-sql-trino/target/redis-sql-trino-*/* /usr/lib/trino/plugin/redisearch/
COPY --chown=trino:trino docker/etc /etc/trino
COPY docker/template /tmp/
COPY --chmod=0777 docker/setup.sh /tmp/

CMD ["/tmp/setup.sh"]