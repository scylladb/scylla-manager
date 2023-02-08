========
Examples
========

.. contents::
   :depth: 2
   :local:

Prerequisites
------------------

#. Backup locations (buckets) with backed-up data created by :ref:`backup <sctool-backup>` task.
#. Snapshot tag of backup that you want to restore.

To get a list of all available snapshot tags, use :ref:`sctool backup list <backup-list>` command.

Schedule restore
----------------

Use the example below to run the sctool restore command.

.. code-block:: none

   sctool restore -c <id|name> -L <list of locations> -T <Snapshot tag> --restore-<schema|tables>
   restore/3208ff15-6e8f-48b2-875c-d3c73f545410

where:

* ``-c`` - the :ref:`name <cluster-add>` you used when you created the cluster
* ``-L`` - points to backup storage location in ``s3:<your S3 bucket name>`` format
* ``-T`` - snapshot tag that you want to restore
* ``--restore-schema or --restore-tables`` - type of restore task

The command returns the task ID. You will need this ID for additional actions.

Complete the restore procedure - restore both the schema and the content of the tables
--------------------------------------------------------------------------------------

#. Ensure that all prerequisites of restoring schema are met.
   The easiest way to achieve that is to perform restore schema on a new empty cluster.

#. Restore schema using :ref:`sctool restore <sctool-restore>`.

   .. code-block:: none

     sctool restore -c prod-cluster -L s3:test-bucket -T snapshot_tag --restore-schema
     restore/3208ff15-6e8f-48b2-875c-d3c73f545410

#. Ensure that restoring schema has finished using :ref:`sctool progress <task-progress>`.

   .. code-block:: none

     sctool progress restore/3208ff15-6e8f-48b2-875c-d3c73f545410 -c prod-cluster
     Run:            774ce0fe-a7b4-11ed-82da-0892040e83bb
     Status:         DONE - restart required (see restore docs)
     Start time:     16 Feb 23 17:09:55 CET
     End time:       16 Feb 23 17:09:59 CET
     Duration:       4s
     Progress:       100% | 100%
     Snapshot Tag:   sm_20230216160631UTC

     ╭───────────────┬─────────────┬──────────┬──────────┬────────────┬──────────────┬────────╮
     │ Keyspace      │    Progress │     Size │  Success │ Downloaded │ Deduplicated │ Failed │
     ├───────────────┼─────────────┼──────────┼──────────┼────────────┼──────────────┼────────┤
     │ system_schema │ 100% | 100% │ 557.728k │ 557.728k │   557.728k │            0 │      0 │
     ╰───────────────┴─────────────┴──────────┴──────────┴────────────┴──────────────┴────────╯

#. Perform restore schema follow-up action - `rolling restart <https://docs.scylladb.com/stable/operating-scylla/procedures/config-change/rolling-restart.html>`_.

#. Ensure that all prerequisites of restoring the content of the tables are met.
   The easiest way to achieve that is to restore schema with :doc:`sctool restore <restore-schema>` (so that's what has already been done in this example).
   You don't need to follow this step if you are sure that the destination cluster has the correct schema of restored tables and
   that those tables are `truncated <https://docs.scylladb.com/stable/cql/ddl.html#truncate-statement>`_.

#. Restore the content of the tables using :ref:`sctool restore <sctool-restore>`.

   .. code-block:: none

     sctool restore -c prod-cluster -L s3:test-bucket -T snapshot_tag --restore-tables -K test_keyspace
     restore/31043443-8214-42d3-be98-ed9771c48dde

#. Ensure that restoring the content of the tables has finished using :ref:`sctool progress <task-progress>`.

   .. code-block:: none

     sctool progress restore/31043443-8214-42d3-be98-ed9771c48dde -c prod-cluster --details
     Run:            e10ec718-a7bf-11ed-b05a-0892040e83bb
     Status:         DONE - repair required (see restore docs)
     Start time:     16 Feb 23 17:17:29 CET
     End time:       16 Feb 23 17:18:44 CET
     Duration:       1m15s
     Progress:       100% | 100%
     Snapshot Tag:   sm_20230216160631UTC

     ╭───────────────┬─────────────┬────────┬─────────┬────────────┬──────────────┬────────╮
     │ Keyspace      │    Progress │   Size │ Success │ Downloaded │ Deduplicated │ Failed │
     ├───────────────┼─────────────┼────────┼─────────┼────────────┼──────────────┼────────┤
     │ test_keyspace │ 100% | 100% │ 7.938M │  7.938M │     7.938M │            0 │      0 │
     ╰───────────────┴─────────────┴────────┴─────────┴────────────┴──────────────┴────────╯

     Keyspace: test_keyspace
     ╭────────┬─────────────┬────────┬─────────┬────────────┬──────────────┬────────┬────────────────────────┬────────────────────────╮
     │ Table  │ Progress    │ Size   │ Success │ Downloaded │ Deduplicated │ Failed │ Started at             │ Completed at           │
     ├────────┼─────────────┼────────┼─────────┼────────────┼──────────────┼────────┼────────────────────────┼────────────────────────┤
     │ table1 │ 100% | 100% │ 2.642M │  2.642M │     2.642M │            0 │      0 │ 16 Feb 23 17:17:35 CET │ 16 Feb 23 17:18:37 CET │
     ├────────┼─────────────┼────────┼─────────┼────────────┼──────────────┼────────┼────────────────────────┼────────────────────────┤
     │ table2 │ 100% | 100% │ 2.657M │  2.657M │     2.657M │            0 │      0 │ 16 Feb 23 17:17:31 CET │ 16 Feb 23 17:18:33 CET │
     ├────────┼─────────────┼────────┼─────────┼────────────┼──────────────┼────────┼────────────────────────┼────────────────────────┤
     │ table3 │ 100% | 100% │ 2.640M │  2.640M │     2.640M │            0 │      0 │ 16 Feb 23 17:17:39 CET │ 16 Feb 23 17:18:41 CET │
     ╰────────┴─────────────┴────────┴─────────┴────────────┴──────────────┴────────┴────────────────────────┴────────────────────────╯

#. Perform restore tables follow-up action - :ref:`sctool repair <sctool-repair>`.

   .. code-block:: none

     sctool repair -c prod-cluster -K test_keyspace
     repair/7ff514c1-c55d-4a1b-841c-cb98225aa05d`

#. Ensure that repair has finished using :ref:`sctool progress <task-progress>`.

   .. code-block:: none

     sctool progress repair/7ff514c1-c55d-4a1b-841c-cb98225aa05d -c prod-cluster
     Run:            b4621e0f-a7c1-11ed-b05b-0892040e83bb
     Status:         DONE
     Start time:     16 Feb 23 17:22:32 CET
     End time:       16 Feb 23 17:22:36 CET
     Duration:       2s
     Progress:       100%
     Datacenters:
       - dc1
       - dc2

     ╭───────────────────────────────┬────────────────────────────────┬──────────┬──────────╮
     │ Keyspace                      │                          Table │ Progress │ Duration │
     ├───────────────────────────────┼────────────────────────────────┼──────────┼──────────┤
     │ test_keyspace                 │                         table1 │ 100%     │ 0s       │
     │ test_keyspace                 │                         table2 │ 100%     │ 0s       │
     │ test_keyspace                 │                         table3 │ 100%     │ 0s       │
     ╰───────────────────────────────┴────────────────────────────────┴──────────┴──────────╯


Now all schema and the content of the selected tables should be properly restored into the destination cluster.

Perform a dry run of a restore
------------------------------

We recommend performing a  dry run of a restore prior to scheduling the restore.
It's a useful way to verify whether all necessary prerequisites are fulfilled.
To perform a dry run, add the ``--dry-run`` parameter at the end of the restore command. If it works, you can remove the parameter from the command and schedule the restore without making any other changes.

A dry run verifies if nodes can access the provided backup location.
If the location is not accessible, an error message will be displayed, and the restore is not scheduled.

.. code-block:: none

   sctool restore -c prod-cluster -L s3:test-bucket -T snapshot_tag --restore-tables --dry-run

   Error: failed to get backup target: location is not accessible
    192.168.100.23: failed to access s3:test-bucket make sure that the location is correct and credentials are set
    192.168.100.22: failed to access s3:test-bucket make sure that the location is correct and credentials are set
    192.168.100.21: failed to access s3:test-bucket make sure that the location is correct and credentials are set

Dry run also verifies if any backup contents are matched by given glob patterns.
If not, an error message will be displayed, and restore is not scheduled.

.. code-block:: none

   sctool restore -c prod-cluster -L s3:test-bucket -T snapshot_tag -K non_existing_keyspace --restore-tables --dry-run
   Error: get restore units: no data in backup locations match given keyspace pattern

Performing a dry run allows you to resolve all configuration or access issues before executing an actual restore.

If the dry run completes successfully, a summary of the restore is displayed. For example:

.. code-block:: none

   sctool restore -c prod-cluster -L s3:test-backup -T snapshot_tag -K test_keyspace --restore-tables --dry-run
   NOTICE: dry run mode, restore is not scheduled

   Keyspaces:
     - test_keyspace: 7.938M (table2: 2.657M, table1: 2.642M, table3: 2.640M)

   Disk size: ~7.938M

   Locations:
     - s3:test-backup

   Snapshot Tag:   sm_20230216160631UTC
   Batch Size:     2
   Parallel:       1
