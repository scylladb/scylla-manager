FROM ubuntu
ARG ARCH=amd64

RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get install -y --no-install-recommends ca-certificates && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY release/scylla-manager-*$ARCH.deb /
RUN dpkg -i scylla-manager-*$ARCH.deb && rm /scylla-manager-*.deb
COPY docker/scylla-manager.yaml /etc/scylla-manager/

USER scylla-manager
ENV HOME=/var/lib/scylla-manager/
ENTRYPOINT ["/usr/bin/scylla-manager"]
