FROM centos:7 as pkg-env

ARG VERSION_NAME

ADD release/rpm /tmp
RUN yum install -y /tmp/scylla-manager-server-${VERSION_NAME}.x86_64.rpm
RUN sed -e 's/^https:[ ]*127.0.0.1:56443$/# &\nhttps: 0.0.0.0:56443/' -i /etc/scylla-manager/scylla-manager.yaml
RUN sed -E -e 's/^([ ]+)# level: info$/& \n\1mode: stderr/' -i /etc/scylla-manager/scylla-manager.yaml
RUN sed -E -e 's/^([ ]+-[ ]*)127.0.0.1/\1scylla/' -i /etc/scylla-manager/scylla-manager.yaml

FROM gcr.io/distroless/base
COPY --from=pkg-env /usr/bin/scylla-manager /usr/bin/
COPY --from=pkg-env /etc/scylla-manager /etc/scylla-manager/
COPY --from=pkg-env /var/lib/scylla-manager /val/lib/

EXPOSE 56443
WORKDIR /var/lib/scylla-manager
ENTRYPOINT ["/usr/bin/scylla-manager"]
