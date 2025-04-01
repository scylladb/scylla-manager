FROM registry.access.redhat.com/ubi9-minimal:latest
ARG ARCH=x86_64

COPY release/scylla-manager-*$ARCH.rpm /
COPY docker/scylla-manager.yaml /etc/scylla-manager/
COPY license/LICENSE.* /licenses/

RUN microdnf -y update && \
    microdnf -y upgrade && \
    microdnf install -y ca-certificates && \
    microdnf clean all && \
    rpm -ivh scylla-manager-*$ARCH.rpm && \
    rm /scylla-manager-*.rpm

USER scylla-manager
ENV HOME=/var/lib/scylla-manager/
ENTRYPOINT ["/usr/bin/scylla-manager"]
