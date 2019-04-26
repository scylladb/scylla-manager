ARG SCYLLA_VERSION

FROM scylladb/scylla:${SCYLLA_VERSION}

# Set root password
RUN echo -e "root\nroot" | passwd --stdin root

# Install sshd
RUN yum makecache && \
    yum -y install initscripts openssh-server openssh-clients passwd sudo && \
    yum clean all
RUN mkdir /var/run/sshd
RUN /usr/sbin/sshd-keygen

# Add sshd configuration
COPY etc/ssh/sshd_config /etc/ssh/sshd_config

# Add supervisord configuration for sshd
ADD etc/supervisord.conf.d/sshd.conf /etc/supervisord.conf.d/sshd.conf

# Add scylla configuration
COPY etc/scylla/scylla.yaml /etc/scylla/scylla.yaml

# Install SSL certificate
ADD db.crt db.key /etc/scylla/

# Force running Scylla processes as scylla user
RUN echo "user: scylla" >> /etc/supervisord.conf.d/scylla-server.conf
RUN echo "user: scylla" >> /etc/supervisord.conf.d/scylla-housekeeping.conf
RUN echo "user: scylla" >> /etc/supervisord.conf.d/scylla-jmx.conf
