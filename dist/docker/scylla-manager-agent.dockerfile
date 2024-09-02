FROM ubuntu
ARG ARCH=amd64

RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get install -y --no-install-recommends ca-certificates && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY release/scylla-manager-agent*$ARCH.deb /
RUN dpkg -i scylla-manager-agent*$ARCH.deb && rm /scylla-manager-agent*.deb

USER scylla-manager
ENV HOME=/var/lib/scylla-manager/
ENTRYPOINT ["/usr/bin/scylla-manager-agent"]
