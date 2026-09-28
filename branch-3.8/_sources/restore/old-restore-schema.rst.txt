===============================================
Restore schema for ScyllaDB 5.4/2024.1 or older
===============================================

.. note:: Currently, Scylla Manager supports only entire schema restoration, so ``--keyspace`` flag is not allowed.

.. note:: Because of small size of schema files, resuming schema restoration always starts from scratch.

.. include:: ../common/restore-raft-schema-warn.rst

| In order to restore ScyllaDB cluster schema use :ref:`sctool restore <sctool-restore>` with ``--restore-schema`` flag.
| Please note that the term *schema* specifically refers to the data residing in the ``system_schema keyspace``, such as keyspace and table definitions. All other data stored in keyspaces managed by ScyllaDB, such as authentication data in the ``system_auth`` keyspace, is restored as part of the :doc:`restore tables procedure <restore-tables>`.
| The restore schema procedure works with any cluster size, so the backed-up cluster can have a different number of nodes per data center than the restore destination cluster. However, it is important that the restore destination cluster consists of at least all of the data centers present in the backed-up cluster.

Prerequisites
=============

* ScyllaDB Manager with CQL credentials to restore destination cluster.

* It is strongly advised to restore schema only into an empty cluster with no schema change history of the keyspace that is going to be restored.
   Otherwise, the restored schema might be overwritten by the already existing one and cause unexpected errors.

* All nodes in restore destination cluster should be in the ``UN`` state (See `nodetool status <https://docs.scylladb.com/manual/stable/operating-scylla/nodetool-commands/status.html>`_ for details).

Procedure
=========

This section contains a description of the restore-schema procedure performed by ScyllaDB Manager.

Because of being unable to alter schema tables ``tombstone_gc`` option, restore procedure "simulates ad-hoc repair"
by duplicating data from **each backed-up node into each node** in restore destination cluster.
However, the small size of schema files makes this overhead negligible.

    * Validate that all nodes are in the ``UN`` state
    * For each backup location:

      * Find all ScyllaDB *nodes* with location access and use them for restoring schema from this location
      * List backup manifests for specified snapshot tag
    * For each manifest:

        * Filter relevant tables from the manifest
        * For each table:

          * For each *node* (in ``--parallel``):

            * Download all SSTables
    * For all nodes in restore destination cluster:

        * `nodetool refresh <https://docs.scylladb.com/manual/stable/operating-scylla/nodetool-commands/refresh.html>`_ on all downloaded schema tables (full parallel)

Follow-up action
================

After successful restore it is important to perform necessary follow-up action. In case of restoring schema,
you should make a `rolling restart <https://docs.scylladb.com/manual/stable/operating-scylla/procedures/config-change/rolling-restart.html>`_ of an entire cluster.
Without the restart, the restored schema might not be visible, and querying it can return various errors.

.. _restore-schema-workaround:

Restoring schema into a cluster with ScyllaDB **5.4.X** or **2024.1.X** with **consistent_cluster_management**
==============================================================================================================

Restoring schema when using ScyllaDB **5.4.X** or **2024.1.X** with ``consistent_cluster_management: true`` in ``scylla.yaml``
is not supported. In such case, you should perform the following workaround:

    * Create a fresh cluster with ``consistent_cluster_management: false`` configured in ``scylla.yaml`` and a desired ScyllaDB version.
    * Restore schema via :ref:`sctool restore <sctool-restore>` with ``--restore-schema`` flag.
    * Perform `rolling restart <https://docs.scylladb.com/manual/stable/operating-scylla/procedures/config-change/rolling-restart.html>`_ of an entire cluster.
    * Follow the steps of the `Enable Raft procedure <https://docs.scylladb.com/manual/stable/architecture/raft.html>`_.