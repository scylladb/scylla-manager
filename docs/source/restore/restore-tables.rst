==============
Restore tables
==============

.. note:: Currently, Scylla Manager does not support restoring content of `CDC log tables <https://docs.scylladb.com/stable/using-scylla/cdc/cdc-log-table.html>`_.

To restore the content of the tables (rows), use the :ref:`sctool restore <sctool-restore>` command with the ``--restore-tables`` flag.

You can add the ``--keyspace`` flag to define the base tables that will be restored. All Materialized Views and Secondary Indexes of those tables will be automatically recreated in order to ensure the correct data state.

Prerequisites
=============

* Scylla Manager requires CQL credentials with:

    * `permission to alter <https://opensource.docs.scylladb.com/stable/operating-scylla/security/authorization.html#permissions>`_ restored tables.
    * `permission to drop and create <https://opensource.docs.scylladb.com/stable/operating-scylla/security/authorization.html#permissions>`_ Materialized Views and Secondary Indexes of restored tables.

* Restoring the content of the tables assumes that the correct schema (identical as in the backup) is already present in the destination cluster.

   Scylla Manager does NOT validate that, so it's user responsibility to ensure it. The only form of validation
   that Scylla Manager performs is checking whether restored tables are present in destination cluster,
   but it does not validate their columns types nor other properties. In case destination cluster is missing correct schema,
   it should be :doc:`restored <restore-schema>` first.

   Note that this requirement also applies to the keyspace schema.
   Keyspace (where restored tables live) should have the correct schema, including the replication strategy.
   It can be changed after successful restore according to: `How to Safely Increase the Replication Factor <https://opensource.docs.scylladb.com/stable/kb/rf-increase.html>`_.

* It is strongly advised to restore the contents of the tables only into `truncated <https://docs.scylladb.com/stable/cql/ddl.html#truncate-statement>`_ tables.

   Otherwise, restored tables' contents might be overwritten by the already existing ones.
   Note that an empty table is not necessarily truncated!

* All nodes in restore destination cluster should be in the ``UN`` state (See `nodetool status <https://docs.scylladb.com/stable/operating-scylla/nodetool-commands/status.html>`_ for details).

Process
=======

Restore procedure works iteratively over restored locations, manifests and tables.

    * For each backup location:

      * Find live Scylla nodes that will be used for restoring current location

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
            * Call `load and stream <https://docs.scylladb.com/stable/operating-scylla/nodetool-commands/refresh.html#load-and-stream>`_ with ``primary_replica_only``
    * :ref:`Repair <sctool-repair>` restored tables
    * Reset all restored tables ``tombstone_gc`` to its original mode
    * Recreate restored views

Information about original ``tombstone_gc`` mode, views definitions and repair progress is included in :ref:`sctool progress --details <task-progress>`.
