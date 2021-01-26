# Scylla Manager

[Scylla Manager](https://docs.scylladb.com/operating-scylla/manager/) is a product for database operations automation tool for [ScyllaDB](https://www.scylladb.com/).
It can schedule tasks such as repairs and backups. Scylla Manager can manage multiple Scylla clusters and run cluster-wide tasks in a controlled and predictable way.

Scylla Manager is available for Scylla Enterprise customers and Scylla Open Source users.
With Scylla Open Source, Scylla Manager is limited to 5 nodes.
See the [Scylla Manager Proprietary Software](https://www.scylladb.com/scylla-manager-software-license-agreement) for details.

## Docker Compose Example
[Docker compose](https://docs.docker.com/compose/) is a tool for defining and running multi-container applications without having to orchestrate the participating containers by hand.

__Purpose of the example__

This example uses single node Scylla cluster and MinIO and should not be used in a production setting.
Please see the [Scylla Manager Operations Guide](https://docs.scylladb.com/operating-scylla/manager/) for a proper production setup.
Once you have the example up and running you can try out the various commands for running repairs and backups that Scylla Manager provides.

__Procedure__

1. Copy the following yaml and save it to your current working directory as `docker-compose.yaml`.

```yaml
version: "3.7"

services:
  scylla-manager:
    image: scylladb/scylla-manager:${SCYLLA_MANAGER_VERSION}
    networks:
      public:
    depends_on:
      - scylla-manager-db

  scylla-manager-db:
    image: scylladb/scylla:latest
    volumes:
      - scylla_manager_db_data:/var/lib/scylla
    networks:
      public:
    command: --smp 1 --memory 100M

  scylla:
    build:
      context: .
    image: scylladb/scylla-with-agent:${SCYLLA_MANAGER_VERSION}
    volumes:
      - scylla_data:/var/lib/scylla
    networks:
      public:
        ipv4_address: 192.168.100.100
    command: --smp 1 --memory 1G

  minio:
    image: minio/minio:latest
    volumes:
      - minio_data:/data
    networks:
      public:
    ports:
      - "9001:9000"
    environment:
      MINIO_ACCESS_KEY: minio
      MINIO_SECRET_KEY: minio123
    command: server /data

volumes:
  minio_data:
  scylla_data:
  scylla_manager_db_data:

networks:
  public:
    driver: bridge
    ipam:
      driver: default
      config:
        - subnet: 192.168.100.0/24
```

This instructs Docker to create two containers, a scylla-manager container that runs the Scylla Manager Server and scylla-manager-db container that runs the Scylla database that Scylla Manager uses to save its data.
It also gives you a [MinIO](https://min.io/) instance for backups and a scylla instance to use for your application.
Please bear in mind that this is not a production setup as that would most require much more and advanced configuration of storage and security.

2. Copy the following `Dockerfile` to the same directory as the `docker-compose.yaml` file you saved in item 1.
This docker file is used to create a ScyllaDB image with the Scylla Manager Agent patched on top of it.
The reason for this is that the Agent needs access to ScyllaDB's data files in order to create backups.

```Dockerfile
FROM scylladb/scylla:latest

ADD http://downloads.scylladb.com.s3.amazonaws.com/manager/rpm/unstable/centos/master/latest/scylla-manager.repo /etc/yum.repos.d/
RUN yum -y install epel-release scylla-manager-agent && \
    yum clean all && \
    rm -f /etc/yum.repos.d/scylla-manager.repo

RUN echo -e "[program:scylla-manager-agent]\n\
command=/usr/bin/scylla-manager-agent\n\
autorestart=true\n\
stdout_logfile=/dev/stdout\n\
stdout_logfile_maxbytes=0\n\
stderr_logfile=/dev/stderr\n\
stderr_logfile_maxbytes=0" > /etc/supervisord.conf.d/scylla-manager-agent.conf

RUN echo -e "auth_token: token\n\
s3:\n\
    access_key_id: minio\n\
    secret_access_key: minio123\n\
    provider: Minio\n\
    endpoint: http://minio:9000" > /etc/scylla-manager-agent/scylla-manager-agent.yaml

```

3. Create the containers by running the `docker-compose build` command.

```bash
docker-compose build
```

4. Start the containers by running the `docker-compose up` command.

```bash
docker-compose up -d
```

5. Verify that Scylla Manager started by using the `logs` command.

```bash
docker-compose logs -f scylla-manager
```

## Using sctool

Use docker-compose exec to invoke bash in the `scylla-manager` container to add the one node cluster you created above to Scylla Manager.

```bash
docker-compose exec scylla-manager sctool cluster add --name test --host=scylla --auth-token=token
defe1ffe-c992-4ca2-9fad-82a61f39ad9e
 __
/  \     Cluster added! You can set it as default, by exporting its name or ID as env variable:
@  @     $ export SCYLLA_MANAGER_CLUSTER=defe1ffe-c992-4ca2-9fad-82a61f39ad9e
|  |     $ export SCYLLA_MANAGER_CLUSTER=<name>
|| |/
|| ||    Now run:
|\_/|    $ sctool status -c defe1ffe-c992-4ca2-9fad-82a61f39ad9e
\___/    $ sctool task list -c defe1ffe-c992-4ca2-9fad-82a61f39ad9e
```

```bash
docker-compose exec scylla-manager sctool status
Cluster: cluster (defe1ffe-c992-4ca2-9fad-82a61f39ad9e)
Datacenter: datacenter1
+----+----------+----------+-----------------+----------+------+----------+--------+-----------------------------+--------------------------------------+
|    | CQL      | REST     | Address         | Uptime   | CPUs | Memory   | Scylla | Agent                       | Host ID                              |
+----+----------+----------+-----------------+----------+------+----------+--------+-----------------------------+--------------------------------------+
| UN | UP (0ms) | UP (0ms) | 192.168.100.100 | 7h39m45s | 4    | 31.13GiB | 4.2.1  | 666.dev-0.20201230.d4b270c9 | 1987c401-9609-4d92-b8ef-25cfe81b101a |
+----+----------+----------+-----------------+----------+------+----------+--------+-----------------------------+--------------------------------------+
```

See [online docs](https://manager.docs.scylladb.com/) for further details.
