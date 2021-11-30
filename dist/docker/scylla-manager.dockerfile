FROM ubuntu:20.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates bash-completion && \
    apt-get clean && \
    apt-get autoremove && \
    rm -rf /var/lib/apt/lists/*

COPY release/deb/scylla-manager-*.deb /
RUN dpkg -i scylla-manager-*.deb && rm /scylla-manager-*.deb
COPY docker/scylla-manager.yaml /etc/scylla-manager/

USER scylla-manager
ENV HOME /var/lib/scylla-manager/
ENTRYPOINT ["/usr/bin/scylla-manager"]
