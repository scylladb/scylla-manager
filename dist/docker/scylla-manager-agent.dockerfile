FROM registry.access.redhat.com/ubi9-minimal:latest
ARG ARCH=x86_64

COPY license/LICENSE.* /licenses/

# Bind-mount RPMs so they don't persist in an image layer.
RUN --mount=type=bind,source=release,target=/release \
    microdnf -y --nodocs --setopt=install_weak_deps=0 update && \
    microdnf -y --nodocs --setopt=install_weak_deps=0 upgrade && \
    microdnf clean all && \
    rpm -ivh /release/scylla-manager-agent-*$ARCH.rpm

USER scylla-manager
ENV HOME=/var/lib/scylla-manager/
ENTRYPOINT ["/usr/bin/scylla-manager-agent"]
