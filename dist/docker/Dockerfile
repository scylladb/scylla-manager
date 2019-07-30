FROM centos:7 as base

# Install Scylla Manager.
COPY scylla-manager.repo /etc/yum.repos.d/
RUN yum -y install epel-release && \
    yum -y clean expire-cache && \
    yum -y update && \
    yum install -y scylla-manager-server scylla-manager-client && \
    yum clean all && \
    rm /etc/yum.repos.d/scylla-manager.repo

# Set scylla-manager ID to 1000
RUN usermod -u 1000 scylla-manager && groupmod -g 1000 scylla-manager

COPY scylla-manager-docker.yaml /etc/scylla-manager/

EXPOSE 56080
EXPOSE 56443
EXPOSE 56090
EXPOSE 56112

VOLUME /var/lib/scylla-manager/.certs

USER scylla-manager
ENV HOME /var/lib/scylla-manager/
ENTRYPOINT ["/usr/bin/scylla-manager"]
CMD ["-c", "/etc/scylla-manager/scylla-manager.yaml", "-c", "/etc/scylla-manager/scylla-manager-docker.yaml"]
