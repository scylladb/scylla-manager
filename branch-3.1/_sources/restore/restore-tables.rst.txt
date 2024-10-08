==============
Restore tables
==============

.. note:: Currently, Scylla Manager does not support restoring content of `CDC log tables <https://docs.scylladb.com/stable/using-scylla/cdc/cdc-log-table.html>`_.

To restore the content of the tables (rows), use the :ref:`sctool restore <sctool-restore>` command with the ``--restore-tables`` flag.

Prerequisites
=============

* Scylla Manager requires CQL credentials with `permission to alter <https://docs.scylladb.com/stable/operating-scylla/security/authorization.html#permissions>`_ restored tables.

* Restoring the content of the tables assumes that the correct schema is already present in the destination cluster.
   Scylla Manager does NOT validate that, so it's user responsibility to ensure it. The only form of validation
   that Scylla Manager performs is checking whether restored tables are present in destination cluster,
   but it does not validate their columns types nor other properties. In case destination cluster is missing correct schema,
   it should be :doc:`restored <restore-schema>` first.

* It is strongly advised to restore the contents of the tables only into `truncated <https://docs.scylladb.com/stable/cql/ddl.html#truncate-statement>`_ tables.
   Otherwise, restored tables' contents might be overwritten by the already existing ones.
   Note that an empty table is not necessarily truncated!

* It is important to remember current mode of the `tombstone_gc <https://www.scylladb.com/2022/06/30/preventing-data-resurrection-with-repair-based-tombstone-garbage-collection/>`_ option.
   In order to ensure data consistency, Scylla Manager alters restored tables options with ``tombstone_gc = {'mode': 'disabled'}``.
   It's user responsibility to reset the ``tombstone_gc`` option as a part of the restore tables *follow-up action*.

* **Materialized views and secondary indexes must be recreated after the table is successfully restored.**

Follow-up action
================

After the successful restore, it is important to perform the necessary follow-up action. After restoring the content of the tables,
you should :ref:`repair <sctool-repair>` all restored keyspaces and tables.
Without the repair, the restored content of the tables might not be visible, and querying it can return various errors.
*ONLY AFTER* repairing restored tables, you should set their `tombstone_gc <https://www.scylladb.com/2022/06/30/preventing-data-resurrection-with-repair-based-tombstone-garbage-collection/>`_ option to desired mode (e.g. previous mode or strongly recommended ``tombstone_gc = {'mode': 'repair'}``).
Without that, purging tombstones for those tables will remain disabled. This can result in great memory consumption over time, as deleted rows won't ever be removed from database's memory.

The last step is to recreate Materialized Views and Secondary Indexes. To do it, just drop materialized views and indexes, then execute index creation and materialized views creation commands. See examples below.

.. code-block::

    // To list all indexes
    SELECT keyspace_name, table_name, index_name FROM system_schema.indexes WHERE keyspace_name='<input>' AND table_name='input';

    // To get the index creation command
    DESCRIBE <keyspace_name>.<index_name>;

    // To drop the index
    DROP INDEX <keyspace_name>.<index_name>;

    // To list all materialized views (please note that index creates the view as well - view name suffix is "_index" then)
    // Do not recreate materialized view that are part of the index definition.
    SELECT keyspace_name, view_name, base_table_name FROM system_schema.views WHERE keyspace_name='<input>' AND base_table_name='<input>';

    // To get the materialized view creation command
    DESCRIBE <keyspace_name>.<view_name>;

    // To drop the materialized view
    DROP MATERIALIZED VIEW <keyspace_name>.<view_name>;

Process
=======

Restore procedure works iteratively over restored locations, manifests and tables.

    * For each backup location:

      * Find live Scylla nodes that will be used for restoring current location

        * Find *nodes* local to location
        * If non can be found, find any node with location access

      * List backup manifests for specified snapshot tag
    * Alter all restored tables with ``tombstone_gc = {'mode': 'disabled'}`` (ensure restored data consistency)
    * For each manifest:

        * Filter relevant tables from the manifest
        * For each table:

          * Group SSTables to bundles by ID
          * Join bundles into batches containing ``batch size`` bundles each
    * For each batch (in ``--parallel``):

            * Select *node* with enough free space to restore given batch
            * Download batch to *node's* upload directory
            * Call `load and stream <https://docs.scylladb.com/stable/operating-scylla/nodetool-commands/refresh.html#load-and-stream>`_ with ``primary_replica_only``