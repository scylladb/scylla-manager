FROM centos:7 as base

ADD http://downloads.scylladb.com.s3.amazonaws.com/manager/rpm/unstable/centos/master/latest/scylla-manager.repo /etc/yum.repos.d/
RUN yum -y install epel-release scylla-manager-agent && \
    yum clean all && \
    rm -f /etc/yum.repos.d/scylla-manager.repo
COPY docker-entrypoint.sh /usr/bin/docker-entrypoint.sh

RUN rm -f /var/lib/scylla-manager/scylla_manager.crt && \
    rm -f /var/lib/scylla-manager/scylla_manager.key && \
    chmod --reference=/usr/bin/scylla-manager-agent /usr/bin/docker-entrypoint.sh

RUN mkdir -p /var/lib/scylla-manager-agent
ENV HOME /var/lib/scylla-manager-agent/

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]