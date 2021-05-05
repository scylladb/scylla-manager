FROM centos:7 as base

ARG BRANCH
ADD http://downloads.scylladb.com.s3.amazonaws.com/manager/rpm/unstable/centos/${BRANCH}/latest/scylla-manager.repo /etc/yum.repos.d/
RUN yum -y install epel-release scylla-manager-agent && \
    yum clean all && \
    rm -f /etc/yum.repos.d/scylla-manager.repo

RUN mkdir -p /var/lib/scylla-manager-agent
ENV HOME /var/lib/scylla-manager-agent/

ENTRYPOINT ["/usr/bin/scylla-manager-agent"]