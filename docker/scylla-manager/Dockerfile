FROM centos:7 as base

# Install Scylla Manager.
COPY scylla-manager.repo /etc/yum.repos.d/
RUN yum -y install epel-release && \
    yum -y clean expire-cache && \
    yum -y update && \
    yum install -y scylla-manager-server scylla-manager-client && \
    yum clean all && \
    rm /etc/yum.repos.d/scylla-manager.repo

COPY scylla-manager-docker.yaml /etc/scylla-manager/

EXPOSE 5080
EXPOSE 5443
EXPOSE 5090
EXPOSE 5112

USER scylla-manager
ENV HOME /var/lib/scylla-manager/
ENTRYPOINT ["/usr/bin/scylla-manager"]
CMD ["-c", "/etc/scylla-manager/scylla-manager.yaml", "-c", "/etc/scylla-manager/scylla-manager-docker.yaml"]
