======
Backup
======

Using sctool, you can backup and restore your managed Scylla clusters under Scylla Manager.
Backups are scheduled in the same manner as repairs, you can start, stop, and track backup operations on demand.
Scylla Manager can backup to Amazon S3, S3 compatible API storage providers such as Ceph or MinIO and Google Cloud Storage.

.. contents::
   :depth: 3
   :local:

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
It runs parallel on all nodes unless you limit it with the ``--snapshot-parallel`` or ``--upload-parallel`` `flag <../sctool/#backup-parameters>`_.

#. **Snapshot** - Take a snapshot of data on each node (according to backup configuration settings).
#. **Schema** - (Optional) Upload the schema CQL to the backup storage destination, this requires that you added the cluster with ``--username`` and ``--password`` flags. See `Add Cluster <../add-a-cluster/#create-a-managed-cluster>`_ for reference.
#. **Upload** - Upload the snapshot to the backup storage destination.
#. **Manifest** - Upload the manifest file containing metadata about the backup.
#. **Purge** - If the retention threshold has been reached, remove the oldest backup from the storage location.

Prepare nodes for backup
========================

#. Create a storage location for the backup.
   Currently, Scylla Manager supports `Amazon S3 buckets <https://aws.amazon.com/s3/>`_ and `Google Cloud Storage buckets <https://cloud.google.com/storage>`_ .
   You can use an bucket that you already created.
   We recommend using an bucket in the same region where your nodes are to minimize cross region data transfer costs.
   In multi-dc deployments you should create a bucket per datacenter, each located in the datacenter's region.
#. Choose how you want to configure access to the bucket.
   You can use an IAM role (recommended) or you can add your credentials to the agent configuration file.
   The later method is less secure as you will be propagating each node with this security information and in cases where you need to change the key, you will have replace it on each node.

Amazon S3
---------

#. Create an `IAM role <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html>`_ for the S3 bucket which adheres to your company security policy.
#. `Attach the IAM role <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html#attach-iam-role>`_ to **each EC2 instace (node)** in the cluster.

Sample IAM policy for *scylla-manager-backup* bucket:

.. code-block:: none

   {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetBucketLocation",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads"
                ],
                "Resource": [
                    "arn:aws:s3:::scylla-manager-backup"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:DeleteObject",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts"
                ],
                "Resource": [
                    "arn:aws:s3:::scylla-manager-backup/*"
                ]
            }
        ]
   }

**To add your AWS credentials the Scylla Manager Agent configuration file**

Edit the ``/etc/scylla-manager-agent/scylla-manager-agent.yaml``

#. Uncomment the ``s3:`` line, for parameters note the two spaces in front, it's a yaml file.
#. Uncomment and set ``access_key_id`` and ``secret_access_key``, refer to `AWS Credentials Configuration <../agent-configuration-file/#aws-credentials-configuration>`_ for details.
#. If NOT running in AWS EC2 instance uncomment and set ``region`` to region where you created the S3 bucket.

Google Cloud Storage
--------------------

If your application runs inside a Google Cloud environment we recommend using automatic Service Account authentication.

**Automatic Service Account authorization**

#. Collect list of `service accounts <https://cloud.google.com/compute/docs/access/service-accounts>`_ used by **each** of the nodes.
#. Make sure that each of service account has read/write `access scope <https://cloud.google.com/compute/docs/access/service-accounts#accesscopesiam>`_ to Cloud Storage.
#. For each service account from the list, add `Storage Object Admin role <https://cloud.google.com/storage/docs/access-control/iam-roles>`_ in bucket permissions settings.

**Manually add your Service Account credentials the Scylla Manager Agent configuration file**

Alternatively you can configure service account credentials manually. Use `this instruction <https://cloud.google.com/docs/authentication/production#manually>`_ to get the service account file.

This step has to be done on **each** Scylla Node instance.

#. Uncomment the ``gcs:`` line, for parameters note the two spaces in front, it's a yaml file.
#. Uncomment and set ``service_account_file`` with path to service account credentials file.
#. For each service account used by the nodes, add `Storage Object Admin role <https://cloud.google.com/storage/docs/access-control/iam-roles>`_ in bucket permissions settings.

Troubleshooting
---------------

To troubleshoot Node to bucket connectivity issues you can run:

.. code-block:: none

   scylla-manager-agent check-location --debug --location s3:<your S3 bucket name>

Schedule a backup
=================

The most recommended way to run a backup is across an entire cluster.
Backups can be scheduled to run on single or multiple datacenters, keyspaces or tables.
The backup procedure can be customized allowing you to plan your backups according to your IT policy.
All parameters can be found in the `sctool reference <../sctool/#backup>`_.
If you want to check if all of your nodes can connect to the backup storage location see `Perform a Dry Run of a Backup`_.
Following examples will use Amazon S3 as a storage provider.

**Prerequisites**

#. Backup locations (buckets) created.
#. Access rights to backup locations granted to Nodes, see `Prepare Nodes for Backup`_.

Create a scheduled backup
-------------------------

Use the example below to run the sctool backup command.

.. code-block:: none

   sctool backup -c <id|name> -L <list of locations> [-s <date>] [-i <time-unit>]

where:

* ``-c`` - the `name <../sctool/#cluster-add>`_ you used when you created the cluster
* ``-L`` - points to backup storage location in ``s3:<your S3 bucket name>`` format or ``<your DC name>:s3:<your S3 bucket name>`` if you want to specify location for a datacenter
* ``-s`` - the time you want the backup to begin
* ``-i`` - the time interval you want to use in between consecutive backups

The command returns the task ID. You will need this ID for additional actions.
If you want to run the backup only once, leave out the interval argument (``-i``).
In case when you want the backup to start immediately, but you want it to schedule it to repeat at a determined interval, leave out the start flag (``-s``) and set the interval flag (``-i``) to the time you want the backup to reoccur.

Schedule a daily backup
.......................

This command will schedule a backup at 9th Dec 2019 at 15:15:06 UTC time zone, backup will be repeated every day, and all the data will be stored in S3 under the ``my-backups`` bucket.

.. code-block:: none

   sctool backup -c prod-cluster -L 's3:my-backups' -s '2019-12-09T15:16:05Z' -i 24h
   backup/3208ff15-6e8f-48b2-875c-d3c73f545410

Command returns the task ID (backup/3208ff15-6e8f-48b2-875c-d3c73f545410, in this case).
This ID can be used to query the status of the backup task, to defer the task to another time, or to cancel the task See `Managing Tasks <../sctool/#managing-tasks>`_.

Schedule a daily, weekly, and monthly backup
............................................
This command series will schedule a backup on 9th Dec 2019 at 15:15:06 UTC time zone, and will repeat the backup every day (keeping the last 7 days), every week (keeping the previous week) and every month (keeping the previous month).
All the data will be stored in S3 under the ``my-backups`` bucket.

.. code-block:: none

   sctool backup -c prod-cluster -L 's3:my-backups' --retention 7 -s '2019-12-09T15:16:05Z' -i 24h

   sctool backup -c prod-cluster -L 's3:my-backups' --retention 2 -s '2019-12-09T15:16:05Z' -i 7d

   sctool backup -c prod-cluster -L 's3:my-backups' --retention 1 -s '2019-12-09T15:16:05Z' -i 30d

Schedule a backup for a specific DC, keyspace, or table
--------------------------------------------------------
In order to schedule backup of particular data center, you have to specify ``--dc`` parameter.
You can specify more than one DC, or use glob pattern to match multiple DCs or exclude some of them.

For Example, you have the following DCs in your cluster: dc1, dc2, dc3

Backup one specific DC
......................

In this example you backup the only dc1 every 2 days.

.. code-block:: none

   sctool backup -c prod-cluster --dc 'dc1' -L 's3:dc1-backups' -i 2d


Backup all DCs except for those specified
.........................................

.. code-block:: none

   sctool backup -c prod-cluster -i 30d --dc '*,!dc2' -L 's3:my-backups'

Backup to a specific location per DC
....................................

If your data centers are located in different regions, you can also specify different locations.
If your buckets are created in the same regions as your data centers, you may save some bandwidth costs.

.. code-block:: none

   sctool backup -c prod-cluster -i 30d --dc 'eu-dc,us-dc' -L 's3:eu-dc:eu-backups,s3:us-dc:us-backups'

Backup a specific keyspace or table
...................................

In order to schedule backup of particular keyspace or table, you have to provide ``-K`` parameter.
You can specify more than one keyspace/table or use glob pattern to match multiple keyspaces/tables or exclude them.

.. code-block:: none

   sctool backup -c prod-cluster -i 30d -K 'auth_service.*,!auth_service.lru_cache' --dc 'dc1' -L 's3:dc1-backups'

Create an ad-hoc backup
=======================

An ad-hoc backup runs immediately and does not repeat.
This procedure shows the most frequently used backup commands.
Additional parameters can be used. Refer to `backup parameters <../sctool/#backup-parameters>`_.

**Procedure**

To run an immediate backup on the prod-cluster cluster, saving the backup in my-backups, run the following command
replacing the ``-c`` cluster flag with your cluster's cluster name or ID and replace the ``-L`` flag with your backup's location:

.. code-block:: none

   sctool backup -c prod-cluster -L 's3:my-backups'

Perform a dry run of a backup
=============================

We recommend to use ``--dry-run`` parameter prior scheduling a backup.
It's a useful way to verify whether all necessary prerequisites are fulfilled.
Add the parameter to the end of your backup command, so if it works, you can erase it and schedule the backup with no need to make any other changes.

Dry run verifies if nodes are able to access the backup location provided.
If it's not accessible, an error message will be displayed, and the backup is not be scheduled.

.. code-block:: none

   sctool backup -c prod-cluster -L 's3:test-bucket' --dry-run
   NOTICE: dry run mode, backup is not scheduled

   Error: failed to get backup target: location is not accessible
    192.168.100.23: failed to access s3:test-bucket make sure that the location is correct and credentials are set
    192.168.100.22: failed to access s3:test-bucket make sure that the location is correct and credentials are set
    192.168.100.21: failed to access s3:test-bucket make sure that the location is correct and credentials are set

The dry run gives you the chance to resolve all configuration or access issues before executing an actual backup.

If the dry run completes successfully, a summary of the backup is displayed. For example:

.. code-block:: none

   sctool backup -c prod-cluster -L 's3:backups' --dry-run
   NOTICE: dry run mode, backup is not scheduled

   Data Centers:
   - AWS_EU_CENTRAL_1

   Keyspaces:
   - system_auth all (4 tables)
   - system_distributed all (2 tables)
   - system_schema all (12 tables)
   - system_traces all (5 tables)
   - test_keyspace all (10 tables)

   Disk size: ~740.69GiB

   Locations:
   - s3:backups

   Bandwidth Limits:
   - 100 MiB/s

   Snapshot Parallel Limits:
   - All hosts in parallel

   Upload Parallel Limits:
   - All hosts in parallel

   Retention: Last 3 backups

Monitor progress of the backup task
===================================

Progress of the backup task can be monitored by using `sctool task progress <../sctool/#task-progress>`_ command and providing UUID of the backup task.

.. code-block:: none

   sctool task progress backup/3208ff15-6e8f-48b2-875c-d3c73f545410 -c prod-cluster

List the contents of a specific backup
=======================================

List all backups
----------------------

Lists all backups currently in storage that are managed by Scylla Manager.

.. code-block:: none

   sctool backup list -c prod-cluster
   Snapshots:
     - sm_20200805091422UTC (740.69GiB)
     - sm_20200805073801UTC (740.70GiB)
   Keyspaces:
     - system_auth (4 tables)
     - system_distributed (2 tables)
     - system_schema (12 tables)
     - system_traces (5 tables)
     - test_keyspace (10 tables)

List files that were uploaded during a specific backup
-------------------------------------------------------

You can list all files that were uploaded during particular backup.

To list the files use:

.. code-block:: none

   sctool backup files -c prod-cluster --snapshot-tag sm_20200805091422UTC

   s3://manager-test-release22/backup/sst/cluster/9d0ee0ee-5cf5-4633-a1ea-5441b0983e6e/dc/AWS_EU_CENTRAL_1/node/455228ab-2d7b-470f-8a1d-69c9d7bac0e2/keyspace/system_auth/table/role_attributes/6b8c7359a84333f2a1d85dc6a187436f/la-2-big-CompressionInfo.db 	 system_auth/role_attributes
   s3://manager-test-release22/backup/sst/cluster/9d0ee0ee-5cf5-4633-a1ea-5441b0983e6e/dc/AWS_EU_CENTRAL_1/node/455228ab-2d7b-470f-8a1d-69c9d7bac0e2/keyspace/system_auth/table/role_attributes/6b8c7359a84333f2a1d85dc6a187436f/la-2-big-Data.db 	 system_auth/role_attributes
   s3://manager-test-release22/backup/sst/cluster/9d0ee0ee-5cf5-4633-a1ea-5441b0983e6e/dc/AWS_EU_CENTRAL_1/node/455228ab-2d7b-470f-8a1d-69c9d7bac0e2/keyspace/system_auth/table/role_attributes/6b8c7359a84333f2a1d85dc6a187436f/la-2-big-Digest.sha1 	 system_auth/role_attributes
   s3://manager-test-release22/backup/sst/cluster/9d0ee0ee-5cf5-4633-a1ea-5441b0983e6e/dc/AWS_EU_CENTRAL_1/node/455228ab-2d7b-470f-8a1d-69c9d7bac0e2/keyspace/system_auth/table/role_attributes/6b8c7359a84333f2a1d85dc6a187436f/la-2-big-Filter.db 	 system_auth/role_attributes
   [...]

Additional resources
--------------------

`Scylla Snapshots </kb/snapshots/>`_

Delete backup snapshot
=========================

If you decide that you don't want to wait until a particular snapshot expires according to its retention policy, there is a command which allows you to delete a single snapshot from a provided location.

This operation is aware of the Manager deduplication policy, and will not delete any SSTable file referenced by another snapshot.

.. warning:: This operation is irreversible! Use it with great caution!

.. code-block:: none

   sctool backup delete -c prod-cluster -L s3:backups --snapshot-tag sm_20200805091422UTC

Once a snapshot is deleted, it won't show up in backup listing anymore.
