ARG SCYLLA_VERSION

FROM scylladb/scylla:${SCYLLA_VERSION}

# Install sshd:
RUN yum makecache && \
    yum -y install openssh-server passwd && \
    yum clean all
RUN mkdir /var/run/sshd
RUN /usr/sbin/sshd-keygen

# Create scylla-manager user:
RUN useradd scylla-manager && echo -e "test\ntest" | (passwd --stdin scylla-manager)

# Supervisord configuration:
ADD etc/supervisord.conf.d/sshd.conf /etc/supervisord.conf.d/sshd.conf
