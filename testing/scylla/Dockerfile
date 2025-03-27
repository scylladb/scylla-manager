ARG SCYLLA_VERSION

FROM scylladb/${SCYLLA_VERSION}

# Install 3rd party tools
USER root
RUN (apt-get update && apt-get install -y iptables less net-tools openssh-server) || (microdnf install --refresh -y iptables less net-tools openssh-server)

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
RUN mkdir /var/run/sshd && chmod 0755 /var/run/sshd && ssh-keygen -A && \
    echo "PermitRootLogin yes" >> /etc/ssh/sshd_config && \
    echo "PasswordAuthentication yes" >> /etc/ssh/sshd_config && \
    echo "PubkeyAuthentication no" >> /etc/ssh/sshd_config && \
    echo "PermitEmptyPasswords yes" >> /etc/ssh/sshd_config && \
    rm -rf /etc/ssh/sshd_config.d/*

# Add agent config dir, needed to copy the actual config file into it later
RUN mkdir /etc/scylla-manager-agent
