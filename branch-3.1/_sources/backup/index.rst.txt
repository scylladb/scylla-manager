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
   examples
   specification

.. contents::
   :depth: 2
   :local:

Using :ref:`sctool backup <sctool-backup>` command, you can schedule a backup of a managed cluster.
Backups and repairs are scheduled in the same manner, you can start, stop, resume, and track task progress on demand.
The following backup storage engines are supported:

* Amazon S3,
* S3 compatible API storage providers such as Ceph or MinIO,
* Google Cloud Storage.

Features
========

* Glob patterns to select keyspaces or tables to backup
* Deduplication of SSTables
* Retention of old data
* Throttling of upload speed
* Configurable upload destination per datacenter
* Pause and resume

Process
=======

The backup procedure consists of multiple steps executed sequentially.

#. **Snapshot** - Take a snapshot of data on each node (according to backup configuration settings).
#. (Optional) **Schema** - Upload the schema in CQL text format to the backup storage destination,
   this requires that you added the cluster with CQL username and password.
   If you didn't you can :ref:`update the cluster using sctool <cluster-update>` at any point in time.
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
