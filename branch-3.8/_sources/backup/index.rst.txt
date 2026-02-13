======
Backup
======

.. toctree::
   :hidden:
   :maxdepth: 2

   setup-amazon-s3
   setup-s3-compatible-storage
   setup-gcs
   setup-azure-blobstorage
   native-backup
   examples
   specification

.. contents::
   :depth: 2
   :local:

Using :ref:`sctool backup <sctool-backup>` command, you can schedule a backup of a managed cluster.
Backups and repairs are scheduled in the same manner, you can start, stop, resume, and track task progress on demand.

.. note:: If you are using ScyllaDB Manager deployed by ScyllaDB Operator, see dedicated `ScyllaDB Operator documentation <https://operator.docs.scylladb.com/stable/architecture/manager.html>`_.

The following backup storage engines are supported:

* Amazon S3,
* S3 compatible API storage providers such as Ceph or MinIO,
* Google Cloud Storage.

For the purposes of backing up schema, ScyllaDB Manager requires CQL credentials (`sctool cluster update --username --password <cluster-update>`_).
In case of an Alternator cluster, ScyllaDB Manager additionally requires Alternator credentials (`sctool cluster update --alternator-access-key-id --alternator-secret-access-key <cluster-update>`_).
Alternator credentials can point to the same underlying CQL role as the CQL credentials (See `Alternator docs <https://docs.scylladb.com/manual/stable/alternator/compatibility.html#authentication-and-authorization>`_ for details).

Features
========

* Glob patterns to select keyspaces or tables to back up
* Deduplication of SSTables
* Retention of old data
* Throttling of upload speed
* Configurable upload destination per datacenter
* Pause and resume

Selecting tables and nodes to back up
=====================================

| The ``--keyspace``/``--dc`` flags allow for specifying glob pattern for selecting tables/data centers to back up.
| Even when table should be backed up according to ``--keyspace`` flag, but it is not replicated in specified data centers (``--dc`` flag), the table won't be backed up.

| All currently down nodes are ignored for the backup procedure.
| In case table should be backed up, but some of its token ranges are not replicated on any currently live node in the cluster, the backup will fail.

| `Materialized Views <https://docs.scylladb.com/manual/stable/features/materialized-views.html>`_ and `Secondary Indexes <https://docs.scylladb.com/manual/stable/features/secondary-indexes.html>`_
  won't be backed up, as they should be restored by recreating them on the restored base table (see `ScyllaDB docs <https://docs.scylladb.com/manual/stable/operating-scylla/procedures/backup-restore/restore.html#repeat-the-following-steps-for-each-node-in-the-cluster>`_).
| In order to ensure that data residing in View table is preserved, make sure to backup its base table.

| `LWT state tables <https://docs.scylladb.com/manual/stable/features/lwt.html#paxos-state-tables>`_ won't be backed up,
  as they only store the state of ongoing LWT queries and do not store user data. Restoring these tables is not supported either.

Process
=======

The backup procedure consists of multiple steps executed sequentially.

#. **Snapshot** - Take a snapshot of data on each node (according to backup configuration settings).

   Note that ScyllaDB Manager halts `tablets <https://docs.scylladb.com/manual/stable/architecture/tablets.html>`_  migration for the duration of this step.
#. **Schema** - Upload the schema in CQL text format to the backup storage destination,
   this requires that you added the cluster with CQL username and password.
   If you didn't you can :ref:`update the cluster using sctool <cluster-update>` at any point in time.

   Starting from ScyllaDB 6.0 and 2024.2 (and compatible ScyllaDB Manager 3.3),
   these CQL files are necessary for schema restoration (:ref:`sctool restore --restore-schema <sctool-restore>`).
#. **Upload** - Upload the snapshot to the backup storage destination.
#. **Manifest** - Upload the manifest file containing metadata about the backup.
#. **Purge** - If the retention threshold has been reached, remove the oldest backup from the storage location.

.. _backup-location:

Backup location
===============

You need to create a backup location for example an S3 bucket.
We recommend creating it in the same region as Scylla nodes to minimize cross region data transfer costs.
In multi-dc deployments you should create a bucket per datacenter, each located in the datacenter's region.

Details may differ depending on the storage engine, please consult:

* :doc:`Setup Amazon S3 <setup-amazon-s3>`
* :doc:`Setup S3 compatible storage <setup-s3-compatible-storage>`
* :doc:`Setup Google Cloud Storage <setup-gcs>`
* :doc:`Setup Azure Blob Storage <setup-azure-blobstorage>`

Removing backups
================

Backups may require a lot of storage space. They are purged according to the retention defined on the backup task.

`Sctool` can be used to remove snapshots of clusters that are no longer managed by Scylla Manager.
The removal process is performed through the Scylla Manager Agent installed on Scylla nodes.

However, it's recommended to delete the snapshots from the storage before removing the cluster from Scylla Manager.
Otherwise, you will need to add the cluster again, list the snapshots in the given location, and remove them using the new cluster as the coordinator.
Another option is to purge them manually. If you want to remove the snapshots manually, please refer to the :doc:`backup specification <specification>`
and remove them accordingly.

Interrupted ScyllaDB Manager backup tasks might leave not yet uploaded snapshots on nodes' disks.
They are automatically cleaned up after backup task finishes its execution (either after backup was resumed or it was started from scratch).
In case those snapshots are not needed and only result in disk space amplification, they can be cleaned up manually with
:ref:`sctool backup delete local-snapshots <backup-delete-local-snapshots>` command.
