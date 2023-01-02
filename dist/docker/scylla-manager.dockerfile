ARG BASE_IMAGE

FROM $BASE_IMAGE
ARG ARCH
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    apt-get clean && \
    apt-get autoremove && \
    rm -rf /var/lib/apt/lists/*

COPY scylla-manager-*$ARCH.deb /
RUN dpkg -i scylla-manager-*$ARCH.deb && rm /scylla-manager-*.deb
COPY docker/scylla-manager.yaml /etc/scylla-manager/

USER scylla-manager
ENV HOME /var/lib/scylla-manager/
ENTRYPOINT ["/usr/bin/scylla-manager"]
