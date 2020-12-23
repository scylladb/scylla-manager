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

# Add symbolic links for the db key and cert
RUN ln -s /etc/scylla/certs/db.key /etc/scylla/db.key && \
    ln -s /etc/scylla/certs/db.crt /etc/scylla/db.crt && \
    ln -s /etc/scylla/certs/ca.crt /etc/scylla/ca.crt

# Add cqlsh configuration
RUN rm -f /root/.cqlshrc
COPY cqlshrc /root/.cassandra/cqlshrc

# Add sshd configuration
COPY etc/ssh/sshd_config /etc/ssh/sshd_config

# Add supervisord configuration for sshd
ADD etc/supervisord.conf.d/sshd.conf /etc/supervisord.conf.d/sshd.conf

# Add supervisord configuration for agent
ADD etc/supervisord.conf.d/scylla-manager-agent.conf /etc/supervisord.conf.d/scylla-manager-agent.conf
