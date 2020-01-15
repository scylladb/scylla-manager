FROM scylladb/scylla:latest

COPY scylla-manager-agent-docker-entrypoint.sh /scylla-manager-agent-docker-entrypoint.sh
COPY scylla-manager.repo /etc/yum.repos.d/
COPY scylla-manager-agent.conf /etc/supervisord.conf.d

RUN yum -y install epel-release && \
    yum -y clean expire-cache && \
    yum -y update && \
    yum install -y scylla-manager-agent && \
    yum clean all && \
    rm /etc/yum.repos.d/scylla-manager.repo

RUN rm -f /var/lib/scylla-manager/scylla_manager.crt && \
    rm -f /var/lib/scylla-manager/scylla_manager.key && \
    chmod --reference=/usr/bin/scylla-manager-agent /scylla-manager-agent-docker-entrypoint.sh