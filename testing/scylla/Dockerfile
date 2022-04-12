ARG SCYLLA_IMAGE
ARG SCYLLA_VERSION

FROM ${SCYLLA_IMAGE}:${SCYLLA_VERSION}

# Install 3rd party tools
RUN apt-get install -y iptables less net-tools

# Set root password
RUN echo "root:root" | chpasswd root

# Add symbolic links for the db key and cert
RUN ln -s /etc/scylla/certs/db.key /etc/scylla/db.key && \
    ln -s /etc/scylla/certs/db.crt /etc/scylla/db.crt && \
    ln -s /etc/scylla/certs/ca.crt /etc/scylla/ca.crt

# Add supervisord configuration for agent
ADD etc/supervisord.conf.d/scylla-manager-agent.conf /etc/supervisord.conf.d/scylla-manager-agent.conf

# Remove 3rd party services
RUN rm /etc/supervisord.conf.d/scylla-housekeeping.conf \
       /etc/supervisord.conf.d/scylla-node-exporter.conf

# Allow SSH as root
RUN echo "PermitRootLogin yes" >> /etc/ssh/sshd_config

# Add agent config dir, needed to copy the actual config file into it later
RUN mkdir /etc/scylla-manager-agent
