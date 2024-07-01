======
Docker
======

Scylla Manager docker images can be downloaded from Docker hub:

* https://hub.docker.com/r/scylladb/scylla-manager
* https://hub.docker.com/r/scylladb/scylla-manager-agent

The following procedure enables you to play and experiment with Scylla Manager as you read the docs.

Note that `Scylla Kubernetes Operator <https://github.com/scylladb/scylla-operator>`_ would setup Scylla Manager for you.

**Prerequisites**

Docker and docker-compose commands installed.

**Procedure**

#. Save as ``docker-compose.yaml``

   .. literalinclude:: docker-compose.yaml
      :language: yaml

#. Save as ``Dockerfile``

   .. literalinclude:: Dockerfile

#. Run containers, this will run:

   * Scylla node with Scylla Manager Agent installed (a toy cluster)
   * MinIO (for backups)
   * Scylla Manager and its backend Scylla instance

   .. code-block:: none

      docker-compose up --build -d
      docker-compose logs -f scylla-manager

#. Wait until the server is started, you should see something like

   .. code-block:: none

      scylla-manager_1     | {"L":"INFO","T":"2021-05-07T12:55:42.964Z","M":"Starting HTTP server","address":":5080","_trace_id":"sgZk4CPpSx2KkeXU9CqIKg"}
      scylla-manager_1     | {"L":"INFO","T":"2021-05-07T12:55:42.964Z","M":"Starting HTTPS server","address":":5443","client_ca":"","_trace_id":"sgZk4CPpSx2KkeXU9CqIKg"}
      scylla-manager_1     | {"L":"INFO","T":"2021-05-07T12:55:42.964Z","M":"Starting Prometheus server","address":":5090","_trace_id":"sgZk4CPpSx2KkeXU9CqIKg"}
      scylla-manager_1     | {"L":"INFO","T":"2021-05-07T12:55:42.964Z","M":"Starting debug server","address":"127.0.0.1:5112","_trace_id":"sgZk4CPpSx2KkeXU9CqIKg"}
      scylla-manager_1     | {"L":"INFO","T":"2021-05-07T12:55:42.964Z","M":"Service started","_trace_id":"sgZk4CPpSx2KkeXU9CqIKg"}


#. Start bash session in scylla-manager container

   .. code-block:: none

      docker-compose exec scylla-manager bash

#. Add the cluster to Scylla Manager

   .. code-block:: none

      sctool cluster add --name test --host=scylla --auth-token=token

   this will print the cluster added message

   .. code-block:: none

      defe1ffe-c992-4ca2-9fad-82a61f39ad9e
       __
      /  \     Cluster added! You can set it as default, by exporting its name or ID as env variable:
      @  @     $ export SCYLLA_MANAGER_CLUSTER=defe1ffe-c992-4ca2-9fad-82a61f39ad9e
      |  |     $ export SCYLLA_MANAGER_CLUSTER=<name>
      || |/
      || ||    Now run:
      |\_/|    $ sctool status -c defe1ffe-c992-4ca2-9fad-82a61f39ad9e
      \___/    $ sctool tasks -c defe1ffe-c992-4ca2-9fad-82a61f39ad9e

#. Create a Bucket for Backups in MinIO

If you wish to create backups with Scylla Manager using MinIO as a target you
need to first create a "bucket" directory to use as the backup target.

Making sure you are back on your host shell and not in the scylla-manager
container, run the following:

   .. code-block:: none

      docker-compose exec minio sh -c "mkdir /data/docker"

Afterwards you can schedule backups in Scylla Manager using "s3:docker" as the
backup location.