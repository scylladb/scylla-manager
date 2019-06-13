ARG SCYLLA_VERSION

FROM scylladb/scylla:${SCYLLA_VERSION}

# Install 3rd party tools
RUN yum makecache && \
    yum -y install initscripts net-tools passwd sudo && \
    yum clean all

# Set root password
RUN echo -e "root\nroot" | passwd --stdin root

# Install sshd
RUN yum makecache && \
    yum -y install openssh-server openssh-clients && \
    yum clean all
RUN mkdir /var/run/sshd && /usr/sbin/sshd-keygen

# Add sshd configuration
COPY etc/ssh/sshd_config /etc/ssh/sshd_config

# Add supervisord configuration for sshd
ADD etc/supervisord.conf.d/sshd.conf /etc/supervisord.conf.d/sshd.conf

# Add scylla configuration
COPY etc/scylla/scylla.yaml /etc/scylla/scylla.yaml

# Install SSL certificate
ADD db.crt db.key /etc/scylla/
RUN chown scylla:scylla /etc/scylla/db.*

# Force running Scylla processes as scylla user
RUN echo "user=scylla" >> /etc/supervisord.conf.d/scylla-server.conf && \
    echo "user=scylla" >> /etc/supervisord.conf.d/scylla-housekeeping.conf && \
    echo "user=scylla" >> /etc/supervisord.conf.d/scylla-jmx.conf

# Create scylla-manager user
RUN groupadd -r scylla-manager && \
    useradd -g scylla-manager -d /var/lib/scylla-manager -m -s /sbin/nologin -r scylla-manager && \
    usermod -ou $(id -u scylla) scylla-manager

# Add supervisord configuration for agent
ADD etc/supervisord.conf.d/scylla-manager-agent.conf /etc/supervisord.conf.d/scylla-manager-agent.conf

# Add agent configuration
COPY etc/scylla-manager-agent/scylla-manager-agent.yaml /etc/scylla-manager-agent/scylla-manager-agent.yaml
