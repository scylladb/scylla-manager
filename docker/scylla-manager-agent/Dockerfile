FROM centos:7 as base

COPY docker-entrypoint.sh /usr/bin/docker-entrypoint.sh
COPY scylla-manager.repo /etc/yum.repos.d/
RUN yum -y install epel-release && \
    yum -y clean expire-cache && \
    yum -y update && \
    yum install -y scylla-manager-agent && \
    yum clean all && \
    rm /etc/yum.repos.d/scylla-manager.repo

RUN rm -f /var/lib/scylla-manager/scylla_manager.crt && \
    rm -f /var/lib/scylla-manager/scylla_manager.key && \
    chmod --reference=/usr/bin/scylla-manager-agent /usr/bin/docker-entrypoint.sh

RUN mkdir -p /var/lib/scylla-manager-agent
ENV HOME /var/lib/scylla-manager-agent/

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]