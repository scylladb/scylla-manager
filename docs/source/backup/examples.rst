========
Examples
========

.. contents::
   :depth: 1
   :local:

The most recommended way to run a backup is across an entire cluster.
Backups can be scheduled to run on single or multiple datacenters, keyspaces or tables.
The backup procedure can be customized allowing you to plan your backups according to your IT policy.
All parameters can be found in the :ref:`sctool backup <sctool-backup>` command reference.
If you want to check if all of your nodes can connect to the backup storage location see :ref:`Perform a dry run of a Backup <dry-run>`.
Following examples will use AWS S3 as a storage provider.

**Prerequisites**

#. Backup locations (buckets) created and configured.

Create a scheduled backup
-------------------------

Use the example below to run the sctool backup command.

.. code-block:: none

   sctool backup -c <id|name> -L <list of locations> [-s <date>] [-i <time-unit>]

where:

* ``-c`` - the :ref:`name <cluster-add>` you used when you created the cluster
* ``-L`` - points to backup storage location in ``s3:<your S3 bucket name>`` format or ``<your DC name>:s3:<your S3 bucket name>`` if you want to specify location for a datacenter
* ``-s`` - the time you want the backup to begin
* ``-i`` - the time interval you want to use in between consecutive backups

The command returns the task ID. You will need this ID for additional actions.
If you want to run the backup only once, leave out the interval argument (``-i``).
In case you want the backup to start immediately, but you want it to schedule it to repeat at a determined interval, leave out the start flag (``-s``) and set the interval flag (``-i``) to the time you want the backup to reoccur.

Schedule a daily backup
.......................

This command will schedule a backup at 9th Dec 2019 at 15:15:06 UTC time zone, backup will be repeated every day, and all the data will be stored in S3 under the ``my-backups`` bucket.

.. code-block:: none

   sctool backup -c prod-cluster -L 's3:my-backups' -s '2019-12-09T15:16:05Z' -i 24h
   backup/3208ff15-6e8f-48b2-875c-d3c73f545410

The above command returns the task ID (backup/3208ff15-6e8f-48b2-875c-d3c73f545410, in this case).
This ID can be used to query the status of the backup task, to defer the task to another time, or to cancel the task See :ref:`Managing Tasks <task-commands>`.

Schedule a daily, weekly, and monthly backup
............................................
This command series will schedule a backup on 9th Dec 2019 at 15:15:06 UTC time zone, and will repeat the backup every day (keeping the last 7 days), every week (keeping the previous week), and every month (keeping the previous month).
All the data will be stored in S3 under the ``my-backups`` bucket.

.. code-block:: none

   sctool backup -c prod-cluster -L 's3:my-backups' --retention 7 -s '2019-12-09T15:16:05Z' -i 24h

   sctool backup -c prod-cluster -L 's3:my-backups' --retention 2 -s '2019-12-09T15:16:05Z' -i 7d

   sctool backup -c prod-cluster -L 's3:my-backups' --retention 2 -s '2019-12-09T15:16:05Z' -i 30d

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

In order to schedule backup of a particular keyspace or table, you have to provide ``-K`` parameter.
You can specify more than one keyspace/table or use glob pattern to match multiple keyspaces/tables or exclude them.

.. code-block:: none

   sctool backup -c prod-cluster -i 30d -K 'auth_service.*,!auth_service.lru_cache' --dc 'dc1' -L 's3:dc1-backups'

Create an ad-hoc backup
-----------------------

An ad-hoc backup runs immediately and does not repeat.
This procedure shows the most frequently used backup commands.
Additional parameters can be used. Refer to :ref:`backup parameters <backup-parameters>`.

**Procedure**

To run an immediate backup on the prod-cluster cluster, saving the backup in my-backups, run the following command
replacing the ``-c`` cluster flag with your cluster's cluster name or ID and replace the ``-L`` flag with your backup's location:

.. code-block:: none

   sctool backup -c prod-cluster -L 's3:my-backups'


Create backup for archiving keyspace
------------------------------------

We can archive any particular keyspace, and keep it in our bucket regardless of an already sheduled backups.

**Procedure**

For this purpose we can create separate backup task. 
This task will be run only once, without repeating time and with ``--retention 1``.

.. code-block:: none
   
   sctool backup -c prod-cluster -L 's3:my-backups' -K 'Keyspace' --retention 1


Now we can disable it by command:

.. code-block:: none
   
   sctool task update backup/4d79ee63-7721-4105-8c6a-5b98c65c3e21 -e false


If we decide to delete above task, backup will be kept in our storage for the next 30 days.

.. _dry-run:

Perform a dry run of a backup
-----------------------------

We recommend to use ``--dry-run`` parameter prior to scheduling a backup.
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
-----------------------------------

Progress of the backup task can be monitored by using :ref:`sctool task progress <task-progress>` command and providing UUID of the backup task.

.. code-block:: none

   sctool task progress backup/3208ff15-6e8f-48b2-875c-d3c73f545410 -c prod-cluster
