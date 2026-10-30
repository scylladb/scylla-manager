==============
Restore tables
==============

.. note:: ScyllaDB Manager does not support restoring content of `CDC log tables <https://docs.scylladb.com/manual/stable/features/cdc/cdc-log-table.html>`_ nor `LWT state tables <https://docs.scylladb.com/manual/stable/features/lwt.html#paxos-state-tables>`_.

.. warning:: Data related to *authentication* and *service levels* is a part of the backed up CQL schema file, but it is not automatically restored as a part of the restore tables procedure. To restore it, it needs to be fetched from the backup location and applied manually via CQL.

.. warning::
    In case of Alternator clusters, when restore task is completed, restored LSIs are available, yet they are still in the process of applying view updates.
    This means that querying LSIs right after restore might return partial results. When all updates are applied, LSIs will return full results.
    The same applies to GSIs for ScyllaDB versions older than 2025.1.

| To restore the content of the tables (rows), use the :ref:`sctool restore <sctool-restore>` command with the ``--restore-tables`` flag.
| The restore tables procedure works with any cluster topologies, so the backed-up cluster can have a different number of nodes or data centers than the restore destination cluster.

| You can add the ``--keyspace`` flag to define the base tables that will be restored. All Materialized Views and Secondary Indexes of those tables will be automatically recreated in order to ensure the correct data state.

Prerequisites
=============

* ScyllaDB Manager requires CQL credentials (`sctool cluster update --username --password <cluster-update>`_) with:

    * `permission to alter <https://docs.scylladb.com/manual/stable/operating-scylla/security/authorization.html#permissions>`_ restored tables.
    * `permission to drop and create <https://docs.scylladb.com/manual/stable/operating-scylla/security/authorization.html#permissions>`_ Materialized Views and Secondary Indexes of restored tables.

* In case of an Alternator cluster, ScyllaDB Manager additionally requires Alternator credentials (`sctool cluster update --alternator-access-key-id --alternator-secret-access-key <cluster-update>`_) with the same permissions as above. Alternator credentials can point to the same underlying CQL role as the CQL credentials (See `Alternator docs <https://docs.scylladb.com/manual/stable/alternator/compatibility.html#authentication-and-authorization>`_ for details).

* Restoring the content of the tables assumes that the correct schema (identical as in the backup) is already present in the destination cluster.

   ScyllaDB Manager does NOT validate that, so it's user responsibility to ensure it. The only form of validation
   that ScyllaDB Manager performs is checking whether restored tables are present in destination cluster,
   but it does not validate their columns types nor other properties. In case destination cluster is missing correct schema,
   it should be :doc:`restored <restore-schema>` first.

   Note that this requirement also applies to the keyspace schema.
   Keyspace (where restored tables live) should have the correct schema, including the replication strategy.
   It can be changed after successful restore according to: `How to Safely Increase the Replication Factor <https://docs.scylladb.com/manual/stable/kb/rf-increase.html>`_.

* It is strongly advised to restore the contents of the tables only into `truncated <https://docs.scylladb.com/manual/stable/cql/ddl.html#truncate-statement>`_ tables.

   Otherwise, restored tables' contents might be overwritten by the already existing ones.
   Note that an empty table is not necessarily truncated!

* All nodes in restore destination cluster should be in the ``UN`` state (See `nodetool status <https://docs.scylladb.com/manual/stable/operating-scylla/nodetool-commands/status.html>`_ for details).

Procedure
=========

This section contains a description of the restore-tables procedure performed by ScyllaDB Manager.

    * For each backup location:

      * Find live ScyllaDB nodes that will be used for restoring current location

        * Find *nodes* local to location
        * If non can be found, find any node with location access

      * List backup manifests for specified snapshot tag
    * Record restored views definitions and drop them
    * Record restored tables ``tombstone_gc`` mode and change it to ``tombstone_gc = {'mode': 'disabled'}`` (ensure restored data consistency)
    * For each manifest:

        * Filter relevant tables from the manifest
        * For each table:

          * Group SSTables to bundles by ID
          * Join bundles into batches containing ``batch size`` bundles each
    * For each batch (in ``--parallel``):

            * Select *node* with enough free space to restore given batch
            * Download batch to *node's* upload directory
            * Call `load and stream <https://docs.scylladb.com/manual/stable/operating-scylla/nodetool-commands/refresh.html#load-and-stream>`_ with ``primary_replica_only``
    * :ref:`Repair <sctool-repair>` restored tables
    * Reset all restored tables ``tombstone_gc`` to its original mode
    * Recreate restored views

Information about original ``tombstone_gc`` mode, views definitions and repair progress is included in :ref:`sctool progress --details <task-progress>`.


.. _1-1-restore:

1-1 restore
============

ScyllaDB Manager supports much faster restore of tables content using the :ref:`sctool restore 1-1-restore <sctool-1-1-restore>` command.

Limitations:
  * The source and destination clusters must have the same topology (DCs, racks, nodes structure and tokens).

  * Only works with vnode based keyspaces.

  * Performance will be much better than the regular restore, but only if ScyllaDB version is 2025.2 or later.

Please refer to the :ref:`sctool restore 1-1-restore <sctool-1-1-restore>` documentation for more details.
