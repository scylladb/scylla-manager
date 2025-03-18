FROM docker.io/redhat/ubi9-minimal:latest
ARG ARCH=x86_64

RUN microdnf -y update && \
    microdnf -y upgrade && \
    microdnf install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY release/scylla-manager-agent*$ARCH.rpm /
RUN rpm -ivh scylla-manager-agent*$ARCH.rpm && rm /scylla-manager-agent*.rpm

USER scylla-manager
ENV HOME=/var/lib/scylla-manager/
ENTRYPOINT ["/usr/bin/scylla-manager-agent"]
