===============================================
Restore schema for ScyllaDB 6.0/2024.2 or newer
===============================================

.. note:: Currently, ScyllaDB Manager supports only entire schema restoration, so ``--keyspace`` flag is not allowed.

| In order to restore ScyllaDB cluster schema use :ref:`sctool restore <sctool-restore>` with ``--restore-schema`` flag.
| Please note that the term *schema* specifically refers to the data residing in the ``system_schema keyspace``, such as keyspace and table definitions. All other data stored in keyspaces managed by ScyllaDB is restored as part of the :doc:`restore tables procedure <restore-tables>`.
| The restore schema procedure works with any cluster size, so the backed-up cluster can have a different number of nodes than the restore destination cluster.

Prerequisites
=============

* ScyllaDB Manager requires CQL credentials with

    * `permission to create <https://docs.scylladb.com/manual/stable/operating-scylla/security/authorization.html#permissions>`_ restored keyspaces.

* In case of an Alternator cluster, ScyllaDB Manager additionally requires Alternator credentials (`sctool cluster update --alternator-access-key-id --alternator-secret-access-key <cluster-update>`_) with the same permissions as above. Alternator credentials can point to the same underlying CQL role as the CQL credentials (See `Alternator docs <https://docs.scylladb.com/manual/stable/alternator/compatibility.html#authentication-and-authorization>`_ for details).

* No overlapping schema in restore destination cluster (see the procedure below for more details)

* Restore destination cluster must consist of the same DCs as the backed up cluster (see the procedure below for more details)

Procedure
=========

ScyllaDB Manager simply applies the backed up output of ``DESCRIBE SCHEMA WITH INTERNALS`` via CQL.

For this reason, restoring schema will fail when any restored CQL object (keyspace/table/type/...) is already present in the cluster.
In such case, you should first drop the overlapping schema and then proceed with restore.

Another problem could be that restored keyspace was defined with ``NetworkTopologyStrategy`` containing DCs that are not present in the restore destination cluster.
This would result in CQL error when trying to create such keyspace.
In such case, you should manually fetch the backed-up schema file (see :ref:`backup schema specification <backup-schema-spec>`),
change problematic DC names, and apply all CQL statements.

In case of an error, Manager will try to rollback all applied schema changes.