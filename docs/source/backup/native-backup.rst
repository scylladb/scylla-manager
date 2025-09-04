=============
Native Backup
=============

| ScyllaDB Manager uses ScyllaDB Manager Agents deployed on each ScyllaDB node in order to coordinate backups.
| Those agents serve as a proxy to `ScyllaDB REST API <https://docs.scylladb.com/manual/stable/operating-scylla/rest.html>`_, but they also act as `rclone <https://github.com/scylladb/rclone>`_ servers responsible for communication between the node and backup location.

Since rclone server is separate from ScyllaDB internal schedulers, yet they both live on the same machine,
it is allocated with limited resources in order not to interfere with ScyllaDB performance.

This solution results in:
  - inefficient resource utilization (especially when backup is running under high load)
  - longer backup duration (limited resources allocated to rclone server)
  - increased disk storage utilization (snapshots are stored on disk for a longer time)

| Native backup aims to solve these problems by moving backup responsibilities from rclone server into ScyllaDB itself.
| It's important to note that both native and rclone backups performed by ScyllaDB Manager have the same :doc:`specification <specification>` consisting of backup directory layout, manifest, schema and SSTable files. This means that all backups can be used for all types of restores (e.g. regular restore, 1-1-restore, native restore, ...).
| In the `Status`_ section you can find the parts of backup procedure already moved to ScyllaDB.

Status
======

| This section contains the parts of backup procedure already moved to ScyllaDB.
| All other parts are still performed by the rclone server (e.g. deduplication of SSTables, retention of old backups).
| The ``ScyllaDB Version`` column describes the ScyllaDB version from which the functionality is considered production ready, even though the functionality might be available in earlier versions as well.

.. list-table::
   :widths: 15 10 50 25
   :header-rows: 1

   * - Functionality
     - ScyllaDB Version
     - Description
     - Limitations
   * - SSTable upload
     - >= 2025.2
     - As is the most time and resource consuming part of the backup procedure, moving it to ScyllaDB brings the most benefits.
       It also leverages knowledge of ScyllaDB internals in order to prioritize the upload of already compacted SSTables, that thus can be completely deleted after being uploaded,
       which reduces disk storage utilization caused by the snapshot. In order to ensure that native SSTable upload
       does not interfere with ScyllaDB performance, it should be throttled by configuring
       `stream_io_throughput_mb_per_sec <https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html#confval-stream_io_throughput_mb_per_sec>`_ in `scylla.yaml`.
     - | Works with S3 provider only.
       | Does not support creation of :ref:`versioned SSTables <backup-versioned-sstables>`.

.. _configure-native-backup-in-scylla:

Configuration
=============

In order to configure native backup, perform the following steps for each ScyllaDB node:

#. Make sure that ScyllaDB Manager Agent is :ref:`configured <install-agent>` as always

#. Configure `object_storage_endpoints <https://docs.scylladb.com/manual/stable/operating-scylla/admin.html#configuring-object-storage-experimental>`_ in `scylla.yaml`

    This configures the backup location access for the ScyllaDB node itself. The backup location access for ScyllaDB Manager Agent needs to be configured separately,
    in the same way in which it is done for the regular backup purposes (see :doc:`setup-amazon-s3`). The configurations in `scylla.yaml` and `scylla-manager-agent.yaml` should be matching.
    An example of matching configurations is shown below:

    `scylla-manager-agent.yaml`:

    .. code-block:: yaml

       s3:
         provider: AWS
         region: us-east-1
         endpoint: https://s3.us-east-1.amazonaws.com:443

    `scylla.yaml`:

    .. code-block:: yaml

       object_storage_endpoints:
         - name: s3.us-east-1.amazonaws
           port: 443
           https: true
           aws_region: us-east-1

#. Throttle `stream_io_throughput_mb_per_sec <https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html#confval-stream_io_throughput_mb_per_sec>`_ in `scylla.yaml`.

    In order to ensure that native SSTable upload does not interfere with ScyllaDB performance, it should be throttled by configuring
    `stream_io_throughput_mb_per_sec <https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html#confval-stream_io_throughput_mb_per_sec>`_ in `scylla.yaml`.

Usage
=====

The native backup usage is controlled with the :ref:`sctool backup --method <sctool-backup>` flag.
It supports three values: ``native``, ``rclone`` and ``auto``:

  * ``native``: Use all native backup functionalities listed in `Status`_ section.
    Use this value for native backup configuration validation and testing.
    Note that this will fail when:

    * ScyllaDB is not configured properly (see ``object_storage_endpoints`` in `Configuration`_)
    * ScyllaDB does not expose all of the native functionalities (they don't need to be production ready in `Status`_)
    * Other provider than S3 is used (see limitations in `Status`_)
    * The upload of snapshot directories would result in creation of versioned SSTables (see limitations in `Status`_)

  * ``auto``: Use native backup functionalities when possible, otherwise fallback to rclone backup.
    Use this value for production backups. It will use native backup functionality only when it is
    considered production ready (see version support in `Status`_). The fallback works on per snapshot directory basis,
    so it allows for utilizing native backup functionalities for most snapshot directories,
    even if a small subset of them contains versioned SSTables.

  * ``rclone``: Use rclone backup functionalities only. This effectively disables all native backup functionalities.

Note that :ref:`sctool backup --rate-limit --transfers <sctool-backup>` flags do not take effect when using native backup,
as the upload performance is controlled directly by the ScyllaDB itself. In order to control upload performance on the ScyllaDB side
configure `stream_io_throughput_mb_per_sec <https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html#confval-stream_io_throughput_mb_per_sec>`_ in `scylla.yaml`.

You can :ref:`create a new backup task <sctool-backup>` with desired method:

.. code-block:: none

   sctool backup -c <cluster ID> -L <backup locations> --method native

You can also :ref:`update existing backup task <backup-update>` to use a different method:

.. code-block:: none

   sctool backup update -c <cluster ID> <backup task ID> -L <backup locations> --method auto



