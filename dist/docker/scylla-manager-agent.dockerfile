FROM registry.access.redhat.com/ubi9-minimal:latest
ARG ARCH=x86_64

COPY release/scylla-manager-agent*$ARCH.rpm /
COPY license/LICENSE.* /licenses/

RUN microdnf -y update && \
    microdnf -y upgrade && \
    microdnf install -y ca-certificates && \
    microdnf clean all && \
    rpm -ivh scylla-manager-agent*$ARCH.rpm && \
    rm /scylla-manager-agent*.rpm

USER scylla-manager
ENV HOME=/var/lib/scylla-manager/
ENTRYPOINT ["/usr/bin/scylla-manager-agent"]
