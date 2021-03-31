ARG SCYLLA_VERSION

FROM scylladb/scylla:${SCYLLA_VERSION}

# Install 3rd party tools
RUN yum makecache && \
    yum -y install initscripts less net-tools passwd sudo && \
    yum clean all

# Set root password
RUN echo -e "root\nroot" | passwd --stdin root

# Add symbolic links for the db key and cert
RUN ln -s /etc/scylla/certs/db.key /etc/scylla/db.key && \
    ln -s /etc/scylla/certs/db.crt /etc/scylla/db.crt && \
    ln -s /etc/scylla/certs/ca.crt /etc/scylla/ca.crt

# Add cqlsh configuration
RUN rm -f /root/.cqlshrc
COPY cqlshrc /root/.cassandra/cqlshrc

# Add supervisord configuration for agent
ADD etc/supervisord.conf.d/scylla-manager-agent.conf /etc/supervisord.conf.d/scylla-manager-agent.conf

# Overwrite supervisord configuration for scylla
ADD etc/supervisord.conf.d/scylla-server.conf /etc/supervisord.conf.d/scylla-server.conf
