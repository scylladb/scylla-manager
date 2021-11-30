FROM ubuntu:20.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    apt-get clean && \
    apt-get autoremove && \
    rm -rf /var/lib/apt/lists/*

COPY release/deb/scylla-manager-agent*.deb /
RUN dpkg -i scylla-manager-agent*.deb && rm /scylla-manager-agent*.deb

USER scylla-manager
ENV HOME /var/lib/scylla-manager/
ENTRYPOINT ["/usr/bin/scylla-manager-agent"]
