# Docker

ScyllaDB Manager docker images can be downloaded from Docker hub:

* [https://hub.docker.com/r/scylladb/scylla-manager](https://hub.docker.com/r/scylladb/scylla-manager)
* [https://hub.docker.com/r/scylladb/scylla-manager-agent](https://hub.docker.com/r/scylladb/scylla-manager-agent)

The following procedure enables you to play and experiment with ScyllaDB Manager as you read the docs.

Note that [ScyllaDB Kubernetes Operator](https://github.com/scylladb/scylla-operator) would setup ScyllaDB Manager for you.

**Prerequisites**

Docker command installed.

**Procedure**

1. Save as `docker-compose.yaml`
   ```yaml
   services:
     scylla-manager:
       image: scylladb/scylla-manager
       networks:
         public:
       depends_on:
         - scylla-manager-db

     scylla-manager-db:
       image: scylladb/scylla
       volumes:
         - scylla_manager_db_data:/var/lib/scylla
       networks:
         public:
       command: --smp 1 --memory 1G

     scylla:
       build:
         context: .
       image: scylladb/scylla-with-agent
       volumes:
         - scylla_data:/var/lib/scylla
       networks:
         public:
           ipv4_address: 192.168.100.100
       command: --smp 1 --memory 1G

     minio:
       image: minio/minio
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
2. Save as `Dockerfile`
   ```default
   FROM scylladb/scylla-manager-agent:latest as agent
   FROM scylladb/scylla:latest

   COPY --from=agent /usr/bin/scylla-manager-agent /usr/bin/

   USER root

   RUN cat > /etc/supervisord.conf.d/scylla-manager-agent.conf <<'EOF'
   [program:scylla-manager-agent]
   command=/usr/bin/scylla-manager-agent
   autorestart=true
   stdout_logfile=/dev/stdout
   stdout_logfile_maxbytes=0
   stderr_logfile=/dev/stderr
   stderr_logfile_maxbytes=0
   EOF

   RUN mkdir -p /etc/scylla-manager-agent && cat > /etc/scylla-manager-agent/scylla-manager-agent.yaml <<'EOF'
   auth_token: token
   s3:
       access_key_id: minio
       secret_access_key: minio123
       provider: Minio
       endpoint: http://minio:9000
   EOF

   USER scylla
   ```
3. Run containers, this will run:
   * ScyllaDB node with ScyllaDB Manager Agent installed (a toy cluster)
   * MinIO (for backups)
   * ScyllaDB Manager and its backend ScyllaDB instance

   ```none
   docker compose up --build -d
   docker compose logs -f scylla-manager
   ```
4. Wait until the server is started, you should see something like
   ```none
   scylla-manager_1     | {"L":"INFO","T":"2021-05-07T12:55:42.964Z","M":"Starting HTTP server","address":":5080","_trace_id":"sgZk4CPpSx2KkeXU9CqIKg"}
   scylla-manager_1     | {"L":"INFO","T":"2021-05-07T12:55:42.964Z","M":"Starting HTTPS server","address":":5443","client_ca":"","_trace_id":"sgZk4CPpSx2KkeXU9CqIKg"}
   scylla-manager_1     | {"L":"INFO","T":"2021-05-07T12:55:42.964Z","M":"Starting Prometheus server","address":":5090","_trace_id":"sgZk4CPpSx2KkeXU9CqIKg"}
   scylla-manager_1     | {"L":"INFO","T":"2021-05-07T12:55:42.964Z","M":"Starting debug server","address":"127.0.0.1:5112","_trace_id":"sgZk4CPpSx2KkeXU9CqIKg"}
   scylla-manager_1     | {"L":"INFO","T":"2021-05-07T12:55:42.964Z","M":"Service started","_trace_id":"sgZk4CPpSx2KkeXU9CqIKg"}
   ```
5. Start bash session in scylla-manager container
   ```none
   docker compose exec scylla-manager bash
   ```
6. Add the cluster to ScyllaDB Manager
   ```none
   sctool cluster add --name prod-cluster --host=scylla --auth-token=token
   ```

   this will print the cluster added message
   ```none
   defe1ffe-c992-4ca2-9fad-82a61f39ad9e
    __
   /  \     Cluster added! You can set it as default, by exporting its name or ID as env variable:
   @  @     $ export SCYLLA_MANAGER_CLUSTER=defe1ffe-c992-4ca2-9fad-82a61f39ad9e
   |  |     $ export SCYLLA_MANAGER_CLUSTER=<name>
   || |/
   || ||    Now run:
   |\_/|    $ sctool status -c defe1ffe-c992-4ca2-9fad-82a61f39ad9e
   \___/    $ sctool tasks -c defe1ffe-c992-4ca2-9fad-82a61f39ad9e
   ```
7. Create a Bucket for Backups in MinIO

If you wish to create backups with ScyllaDB Manager using MinIO as a target you
need to first create a “bucket” directory to use as the backup target.

Making sure you are back on your host shell and not in the scylla-manager
container, run the following:

> ```none
> docker compose exec minio sh -c "mkdir /data/docker"
> ```

Afterwards you can schedule backups in ScyllaDB Manager using “s3:docker” as the
backup location.
