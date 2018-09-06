ARG SCYLLA_VERSION

FROM scylladb/scylla:${SCYLLA_VERSION}

# Install sshd
RUN yum makecache && \
    yum -y install initscripts openssh-server passwd sudo && \
    yum clean all
RUN mkdir /var/run/sshd
RUN /usr/sbin/sshd-keygen

# Supervisord configuration
ADD etc/supervisord.conf.d/sshd.conf /etc/supervisord.conf.d/sshd.conf

# Set root password
RUN echo -e "root\nroot" | passwd --stdin root
