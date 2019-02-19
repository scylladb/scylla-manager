ARG SCYLLA_VERSION

FROM scylladb/scylla:${SCYLLA_VERSION}

# Install sshd.
RUN yum makecache && \
    yum -y install initscripts openssh-server passwd sudo && \
    yum clean all
RUN mkdir /var/run/sshd
RUN /usr/sbin/sshd-keygen

# Add supervisord configuration for sshd.
ADD sshd.conf /etc/supervisord.conf.d/sshd.conf

# Add scylla configuration.
COPY scylla.yaml /etc/scylla/scylla.yaml

# Set root password.
RUN echo -e "root\nroot" | passwd --stdin root

# Install SSL certificate.
ADD db.crt db.key /etc/scylla/
