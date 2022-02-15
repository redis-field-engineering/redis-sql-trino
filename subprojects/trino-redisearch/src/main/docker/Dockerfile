ARG TRINO_VERSION

FROM trinodb/trino:${TRINO_VERSION}

ARG BUILD_VERSION

COPY setup/trino-redisearch-${BUILD_VERSION}.tar /tmp/trino-redisearch.tar

USER root:root
RUN \
    yum -y -q install gettext && \
    tar xvf /tmp/trino-redisearch.tar -C /tmp && \
    mkdir /usr/lib/trino/plugin/redisearch && \
    find /tmp/trino-redisearch-* -type f -exec mv -i {} /usr/lib/trino/plugin/redisearch \; && \
    chown -R trino:trino /usr/lib/trino/plugin/redisearch

COPY --chown=trino:trino setup/etc /etc/trino
COPY setup/template setup/setup.sh /tmp/

RUN chmod 0777 /tmp/setup.sh

USER trino:trino

CMD ["/tmp/setup.sh"]