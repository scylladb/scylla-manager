==============
Restore schema
==============

.. note:: Currently, Scylla Manager supports only entire schema restoration, so ``--keyspace`` flag is not allowed.

.. note:: Because of small size of schema files, resuming schema restoration always starts from scratch.

In order to restore Scylla cluster schema use :ref:`sctool restore <sctool-restore>` with ``--restore-schema`` flag.

Prerequisites
=============

* Scylla Manager with CQL credentials to restore destination cluster.

* It is strongly advised to restore schema only into an empty cluster. Otherwise, the restored schema might be overwritten
   by the already existing one and cause unexpected errors.

* All nodes in restore destination cluster should be in the ``UN`` state (See `nodetool status <https://docs.scylladb.com/stable/operating-scylla/nodetool-commands/status.html>`_ for details).

Follow-up action
================

After successful restore it is important to perform necessary follow-up action. In case of restoring schema,
you should make a `rolling restart <https://docs.scylladb.com/stable/operating-scylla/procedures/config-change/rolling-restart.html>`_ of an entire cluster.
Without the restart, the restored schema might not be visible, and querying it can return various errors.

Process
=======

Because of being unable to alter schema tables ``tombstone_gc`` option, restore procedure "simulates ad-hoc repair"
by duplicating data from **each backed-up node into each node** in restore destination cluster.
Fortunately, the small size of schema files makes this overhead negligible.

    * Validate that all nodes are in the ``UN`` state
    * For each backup location:

      * Find all Scylla *nodes* with location access and use them for restoring schema from this location
      * List backup manifests for specified snapshot tag
    * For each manifest:

        * Filter relevant tables from the manifest
        * For each table:

          * For each *node* (in ``--parallel``):

            * Download all SSTables
    * For all nodes in restore destination cluster:

        * `nodetool refresh <https://docs.scylladb.com/stable/operating-scylla/nodetool-commands/refresh.html#nodetool-refresh>`_ on all downloaded schema tables (full parallel)