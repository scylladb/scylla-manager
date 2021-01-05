FROM scylladb/scylla:latest

ADD http://downloads.scylladb.com.s3.amazonaws.com/manager/rpm/unstable/centos/master/latest/scylla-manager.repo /etc/yum.repos.d/
RUN yum -y install epel-release scylla-manager-agent && \
    yum clean all && \
    rm -f /etc/yum.repos.d/scylla-manager.repo

RUN echo -e "[program:scylla-manager-agent]\n\
command=/usr/bin/scylla-manager-agent\n\
autorestart=true\n\
stdout_logfile=/dev/stdout\n\
stdout_logfile_maxbytes=0\n\
stderr_logfile=/dev/stderr\n\
stderr_logfile_maxbytes=0" > /etc/supervisord.conf.d/scylla-manager-agent.conf

RUN echo -e "auth_token: token\n\
s3:\n\
    access_key_id: minio\n\
    secret_access_key: minio123\n\
    provider: Minio\n\
    endpoint: http://minio:9000" > /etc/scylla-manager-agent/scylla-manager-agent.yaml
