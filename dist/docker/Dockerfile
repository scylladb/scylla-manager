FROM centos:7 as base

RUN /usr/sbin/groupadd -r scylla-manager -g 1000
RUN /usr/sbin/useradd \
 -g scylla-manager -d /var/lib/scylla-manager -m -s /sbin/nologin -r scylla-manager -u 1000

# Install Scylla Manager.
COPY scylla-manager.repo /etc/yum.repos.d/
RUN yum -y install epel-release && \
    yum -y clean expire-cache && \
    yum -y update && \
    yum install -y scylla-manager-server scylla-manager-client && \
    yum clean all && \
    rm /etc/yum.repos.d/scylla-manager.repo

USER 1000:1000

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
