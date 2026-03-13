==============
Native Restore
==============

Native restore is an optimization similar to :doc:`native backup <../backup/native-backup>`.

ScyllaDB Manager uses ScyllaDB Manager Agents deployed on each ScyllaDB node to coordinate restore.
These agents serve as a proxy to `ScyllaDB REST API <https://docs.scylladb.com/manual/stable/operating-scylla/rest.html>`_ and also act as `rclone <https://github.com/scylladb/rclone>`_ servers responsible for communication between the node and backup location.

Since rclone server is separate from ScyllaDB internal schedulers, yet they both live on the same machine,
it is resource-limited at the cgroup level (agent runs in ``scylla-helper.slice`` under ``scylla.slice`` with lower CPU/IO priority).
Moreover, having separate processes responsible for fetching files from backup location and streaming those files into the cluster results
in the need of additional costly synchronization.

This solution results in:

* Saving files downloaded from backup location on disk before streaming them into the cluster (unnecessary disk writes before streaming)
* Inefficient resource utilization (file download handled by rclone server while file streaming handled by ScyllaDB)
* Longer restore duration (limited resources allocated to rclone server)

Native restore aims to solve these problems by moving restore responsibilities from rclone server into ScyllaDB itself.
Just like with native backup, both native and rclone restores performed by ScyllaDB Manager rely on the same :doc:`backup specification </backup/specification>`,
so any backup (native or rclone) can be restored using any restore method (assuming it meets given restore method limitations).
In the `Status`_ section you can find the parts of restore procedure already moved to ScyllaDB.

Status
======

This section contains the list of stages in the restore procedure now managed by ScyllaDB.
All other stages are still performed by the rclone server.
The ``ScyllaDB Version`` column describes the ScyllaDB version from which the functionality is considered production ready,
even though the functionality might be available in earlier versions as well.

.. list-table::
   :widths: 25 15 50 25
   :header-rows: 1

   * - Functionality
     - ScyllaDB Version
     - Description
     - Limitations
   * - SSTable streaming directly from :doc:`s3 </backup/setup-s3-compatible-storage>`
     - 2026.1
     - As this is the most time- and resource-consuming part of the restore procedure, moving it to ScyllaDB brings the most benefits.
       It also allows for not saving downloaded SSTables on disk before streaming them into the cluster.
       Unlike native backup, when performing restore on a cluster which doesn't currently serve user traffic,
       it's best not to throttle native restore with
       `stream_io_throughput_mb_per_sec <https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html#confval-stream_io_throughput_mb_per_sec>`_ in `scylla.yaml`
       to obtain the best performance.
     - Does not support restoration of :ref:`versioned SSTables <backup-versioned-sstables>`.
       Does not support restoration of SSTables with integer based IDs.
   * - SSTable streaming directly from :doc:`gcs </backup/setup-gcs>`
     - 2026.1
     - Same as above.
     - Same as above.

Configuration
=============

Native restore requires the same ScyllaDB and ScyllaDB Manager Agent configuration as native backup.
Follow the steps described in :ref:`configure-native-backup-in-scylla` to configure each ScyllaDB node.
Throttling `stream_io_throughput_mb_per_sec <https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html#confval-stream_io_throughput_mb_per_sec>`_
is not recommended, as explained in `Status`_ section.

Usage
=====

The native restore usage is controlled with the :ref:`sctool restore --method <sctool-restore>` flag.
It supports three values: ``rclone`` (default), ``native``, and ``auto``:

  * ``native``: Uses all native restore functionalities listed in the `Status`_ section.
    Use this value for native restore configuration validation and testing.
    Note that this will fail when:

    * ScyllaDB is not configured properly (see ``object_storage_endpoints`` in `Configuration`_)
    * Provider not supported by used ScyllaDB version is used (see limitations in `Status`_)
    * Restored backup contains versioned SSTables (see limitations in `Status`_)
    * Restored backup contains SSTables with integer based IDs (see limitations in `Status`_)

  * ``auto``: Uses native restore functionalities when possible, otherwise falls back to rclone restore.
    Use this value for production restores. It will use native restore functionality only when it is
    considered production ready (see version support in `Status`_). The fallback works on per restored batch basis,
    so it allows for utilizing native restore functionalities for most restored batches,
    even if a small subset of them contains SSTables not compatible with native restore.

  * ``rclone``: Uses rclone restore functionalities only. This effectively disables all native restore functionalities.

Note that :ref:`sctool restore --rate-limit --transfers --unpin-agent-cpu <sctool-restore>` flags do not take effect when using native restore,
as the streaming performance is controlled directly by ScyllaDB itself. To control streaming performance on the ScyllaDB side,
configure `stream_io_throughput_mb_per_sec <https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html#confval-stream_io_throughput_mb_per_sec>`_ in `scylla.yaml`.

You can :ref:`create restore task <sctool-restore>` with the desired method:

.. code-block:: none

   sctool restore -c <cluster ID> -L <backup location> --snapshot-tag <tag> --method native
