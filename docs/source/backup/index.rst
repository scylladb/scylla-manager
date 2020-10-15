================
Backup a Cluster
================

.. toctree::
   :hidden:
   :maxdepth: 2

   prepare-nodes
   grant-access-S3
   grant-access-gcs
   backup-examples
   get-schema
   restore-a-backup

Using :ref:`sctool <sctool-backup>`, you can backup and restore your managed Scylla clusters under Scylla Manager.
Backups are scheduled in the same manner as repairs, you can start, stop, and track backup operations on demand.
Scylla Manager can backup to Amazon S3, S3 compatible API storage providers such as Ceph or MinIO and Google Cloud Storage.

Benefits of using Scylla Manager backups
========================================

Scylla Manager automates the backup process and allows you to configure how and when backup occurs.
The advantages of using Scylla Manager for backup operations are:

* Data selection - backup a single table or an entire cluster, the choice is up to you
* Data deduplication - prevents multiple uploads of the same SSTable
* Data retention - purge old data automatically when all goes right, or failover when something goes wrong
* Data throttling - control how fast you upload or Pause/resume the backup
* No cross-region traffic - configurable upload destination per datacenter
* Pause and resume - backup upload can be paused and resumed later, it will continue where it left off
* Retries - retries in case of errors
* Lower disruption to workflow of the Scylla Manager Agent due to cgroups and/or CPU pinning
* Visibility - everything is managed from one place, progress can be read using CLI, REST API or Prometheus metrics, you can dig into details and get to know progress of individual tables and nodes


The backup process
==================

The backup procedure consists of multiple steps executed sequentially.
It runs parallel on all nodes unless you limit it with the ``--snapshot-parallel`` or ``--upload-parallel`` :ref:`flag <backup-parameters>`.

#. **Snapshot** - Take a snapshot of data on each node (according to backup configuration settings).
#. **Schema** - (Optional) Upload the schema CQL to the backup storage destination, this requires that you added the cluster with ``--username`` and ``--password`` flags.
   See :ref:`Add a Cluster <add-cluster>` for reference.
#. **Upload** - Upload the snapshot to the backup storage destination.
#. **Manifest** - Upload the manifest file containing metadata about the backup.
#. **Purge** - If the retention threshold has been reached, remove the oldest backup from the storage location.
