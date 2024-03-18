.. _task-commands:

Task
----

The task command set allows you to schedule, start, stop and modify tasks.

.. code-block:: none

   sctool task <command> [flags] [global flags]

**Subcommands**

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Subcommand
     - Usage
   * - `task delete`_
     - Delete a task.
   * - `task history`_
     - Show run history of a task.
   * - `task list`_
     - Show available tasks and their last run status.
   * - `task progress`_
     - Show the task progress.
   * - `task start`_
     - Start executing a task.
   * - `task stop`_
     - Stop executing a task.
   * - `task update`_
     - Modify a task.

task delete
===========

This command deletes a task from manager.
Note that a task can be disabled if you want to temporarily turn it off (see `task update`_).

**Syntax:**

.. code-block:: none

   sctool task delete <task type/id> --cluster <id|name> [global flags]

.. include:: ../_common/task-id-note.rst

task delete parameters
......................

In addition to the :ref:`global-flags`, task delete takes the following parameter:

=====

.. include:: ../_common/param-cluster.rst

=====

Example: task delete
....................

This example deletes the repair from the task list.
You need the task ID for this action.
This can be retrieved using the command ``sctool task list``.
Once the repair is removed, you cannot resume the repair.
You will have to create a new one.

.. code-block:: none

   sctool task delete -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd


task history
============

This command shows a details about task run history for a give task.

**Syntax:**

.. code-block:: none

   sctool task history <task type/id> --cluster <id|name>
   [--limit <number of results>] [global flags]

.. include:: ../_common/task-id-note.rst

task history parameters
.......................

In addition to the :ref:`global-flags`, task history takes the following parameters:

=====

.. include:: ../_common/param-cluster.rst

=====

.. _task-history-param-limit:

``--limit <number of results>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Limits the number of returned results.

**Default** 10

=====

Example: task history
.....................

.. code-block:: none

   sctool task history repair/730a134a-4792-4139-bc6c-75d2ba7a1e62 -c prod-cluster

   ╭──────────────────────────────────────┬────────────────────────┬────────────────────────┬──────────┬────────────────────────────────────────────────╮
   │ ID                                   │ Start time             │ End time               │ Duration │ Status                                         │
   ├──────────────────────────────────────┼────────────────────────┼────────────────────────┼──────────┼────────────────────────────────────────────────┤
   │ f81ba8ad-ad79-11e8-915f-42010af000a9 │ 01 Jan 18 00:00:00 UTC │ 01 Jan 18 00:00:30 UTC │ 30s      │ STOPPED                                        │
   │ e02d2caf-ad2a-11e8-915e-42010af000a9 │ 31 Feb 18 14:33:05 UTC │ 31 Feb 18 14:34:35 UTC │ 90s      │ SUCCESS                                        │
   │ 7a8c6fe2-ad29-11e8-915d-42010af000a9 │ 31 Mar 18 14:23:20 UTC │ 31 Mar 18 14:23:20 UTC │ 0s       │ ERROR failed to load units …                   │
   │ 08f75324-610d-11e9-9aac-42010af000a9 │ 05 Apr 19 12:33:42 UTC │ 05 Apr 19 12:33:43 UTC │ 1s       │ DONE                                           │
   │ 000681e1-610d-11e9-9aab-42010af000a9 │ 09 Apr 19 12:33:27 UTC │ 09 Apr 19 12:33:28 UTC │ 1s       │ DONE                                           │
   │ f715fb82-610c-11e9-9aaa-42010af000a9 │ 11 Apr 19 12:33:12 UTC │ 11 Apr 19 12:33:13 UTC │ 1s       │ DONE                                           │
   │ ee251fc0-610c-11e9-9aa9-42010af000a9 │ 13 Apr 19 12:32:57 UTC │ 13 Apr 19 12:32:58 UTC │ 1s       │ DONE                                           │
   │ e5343b52-610c-11e9-9aa8-42010af000a9 │ 15 Apr 19 15:32:42 UTC │ 15 Apr 19 15:32:43 UTC │ 1s       │ DONE                                           │
   │ dc435562-610c-11e9-9aa7-42010af000a9 │ 17 Apr 19 12:32:27 UTC │ 17 Apr 19 12:32:28 UTC │ 1s       │ DONE                                           │
   ╰──────────────────────────────────────┴────────────────────────┴────────────────────────┴──────────┴────────────────────────────────────────────────╯

.. _task-list:

task list
=========

This command shows all of the scheduled tasks for the specified cluster.
If cluster is not set this would output a table for every cluster.
Each row contains task type and ID, separated by a slash, task properties, next activation and last status information.
For more information on a task consult `task history`_ and `task progress`_.

**Syntax:**

.. code-block:: none

   sctool task list [--cluster <id|name>] [--all] [--sort <sort-key>]
   [--status <status>] [--type <task type>] [global flags]

task list parameters
....................

In addition to the :ref:`global-flags`, task list takes the following parameters:

=====

.. include:: ../_common/param-cluster.rst

=====

.. _task-list-param-all:

``--all``
^^^^^^^^^

Lists all tasks, including those which have been disabled.
Disabled tasks are prefixed with ``*``.
For example ``*repair/afe9a610-e4c7-4d05-860e-5a0ddf14d7aa``.

=====

.. _task-list-param-sort:

``--sort <sort-key>``
^^^^^^^^^^^^^^^^^^^^^

Returns a list of tasks sorted according to the last run status and sort key which you provide.
Accepted sort key values are:

* ``start-time``
* ``next-activation``
* ``end-time``
* ``status``

``start-time``, ``next-activation``, and ``end-time`` are sorted in ascending order.

``status`` is sorted using the following order: "NEW", "RUNNING", "STOPPED", "DONE", "ERROR", "ABORTED".

=====

.. _task-list-param-status:

``--status <status>``
^^^^^^^^^^^^^^^^^^^^^

Filters tasks according to their last run status.
Accepted values are NEW, STARTING, RUNNING, STOPPING, STOPPED, DONE, ERROR, ABORTED.

=====

.. _task-list-param-t:

``-t, --type <task type>``
^^^^^^^^^^^^^^^^^^^^^^^^^^

Display only tasks of a given type.

=====

Example: task list
..................

.. code-block:: none

   sctool task list
   Cluster: prod-cluster (c1bbabf3-cad1-4a59-ab8f-84e2a73b623f)
   ╭───────────────────────────────────────────────────────┬──────────────────────────────────────┬───────────────────────────────┬─────────╮
   │ Task                                                  │ Arguments                            │ Next run                      │ Status  │
   ├───────────────────────────────────────────────────────┼──────────────────────────────────────┼───────────────────────────────┼─────────┤
   │ healthcheck/ebccaade-4487-4ce9-80ee-d39e295c752e      │                                      │ 02 Apr 20 10:47:48 UTC (+15s) │ DONE    │
   │ healthcheck_rest/eff03af0-21ee-473a-b674-5e4bedc37b8b │                                      │ 02 Apr 20 11:02:03 UTC (+1h)  │ DONE    │
   │ backup/2d7df8bb-69bc-4782-a52f-1ec87f9c2c1c           │ -L s3:manager-backup-tests-eu-west-1 │                               │ RUNNING │
   ╰───────────────────────────────────────────────────────┴──────────────────────────────────────┴───────────────────────────────┴─────────╯

Setting the ``--all`` flag will also list disabled tasks which are not shown in the regular view.
Disabled tasks are prefixed with a ``*``.

.. _task-progress:

task progress
=============

This command shows details of the latest run (or still running) task.

**Syntax:**

.. code-block:: none

   sctool task progress <task type/id> --cluster <id|name> [--details] [global flags]

.. include:: ../_common/task-id-note.rst

task progress parameters
........................


In addition to the :ref:`global-flags`, repair progress takes the following parameters:

=====

.. include:: ../_common/param-cluster.rst

=====

.. _task-progress-param-details:

``--details``
^^^^^^^^^^^^^

More detailed progress data, depending on task type.

====

Example: task progress
......................

This example displays the progress of a running repair.

.. code-block:: none

   sctool task progress repair/dff91fd1-f430-4f98-8932-373644fe647e -c prod-cluster
   Status:         RUNNING
   Start time:     17 Apr 19 12:55:57 UTC
   Duration:       46s
   Progress:       0.45%
   Datacenters:
     - dc1
     - dc2
   ╭───────────────────────┬───────╮
   │ system_auth           │ 3.85% │
   │ system_distributed    │ 0.00% │
   │ system_traces         │ 0.00% │
   │ test_keyspace_dc1_rf2 │ 0.00% │
   │ test_keyspace_dc1_rf3 │ 0.00% │
   │ test_keyspace_dc2_rf2 │ 0.00% │
   │ test_keyspace_dc2_rf3 │ 0.00% │
   │ test_keyspace_rf2     │ 0.00% │
   │ test_keyspace_rf3     │ 0.00% │
   ╰───────────────────────┴───────╯

The ``--details`` flag shows each host’s shard repair progress, with the shards numbered from zero.

.. code-block:: none

   sctool task progress repair/dff91fd1-f430-4f98-8932-373644fe647e -c prod-cluster --details
   Status:         RUNNING
   Start time:     17 Apr 19 12:55:57 UTC
   Duration:       3m0s
   Progress:       1.91%
   Datacenters:
     - dc1
     - dc2
   ╭───────────────────────┬────────╮
   │ system_auth           │ 16.30% │
   │ system_distributed    │ 0.00%  │
   │ system_traces         │ 0.00%  │
   │ test_keyspace_dc1_rf2 │ 0.00%  │
   │ test_keyspace_dc1_rf3 │ 0.00%  │
   │ test_keyspace_dc2_rf2 │ 0.00%  │
   │ test_keyspace_dc2_rf3 │ 0.00%  │
   │ test_keyspace_rf2     │ 0.00%  │
   │ test_keyspace_rf3     │ 0.00%  │
   ╰───────────────────────┴────────╯
   ╭───────────────────────┬───────┬──────────┬───────────────┬─────────────────┬───────────────╮
   │ system_auth           │ shard │ progress │ segment_count │ segment_success │ segment_error │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ 192.168.100.11        │ 0     │ 100.00%  │ 748           │ 748             │ 0             │
   │ 192.168.100.11        │ 1     │ 100.00%  │ 757           │ 757             │ 0             │
   │ 192.168.100.22        │ 0     │ 2.40%    │ 791           │ 19              │ 0             │
   │ 192.168.100.22        │ 1     │ 2.35%    │ 807           │ 19              │ 0             │
   │ 192.168.100.12        │ 0     │ 0.00%    │ 740           │ 0               │ 0             │
   │ 192.168.100.12        │ 1     │ 0.00%    │ 740           │ 0               │ 0             │
   │ 192.168.100.13        │ 0     │ 0.00%    │ 922           │ 0               │ 0             │
   │ 192.168.100.13        │ 1     │ 0.00%    │ 930           │ 0               │ 0             │
   │ 192.168.100.21        │ 0     │ 0.00%    │ 765           │ 0               │ 0             │
   │ 192.168.100.21        │ 1     │ 0.00%    │ 767           │ 0               │ 0             │
   │ 192.168.100.23        │ 0     │ 0.00%    │ 752           │ 0               │ 0             │
   │ 192.168.100.23        │ 1     │ 0.00%    │ 747           │ 0               │ 0             │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ system_distributed    │ shard │ progress │ segment_count │ segment_success │ segment_error │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ 192.168.100.11        │ 0     │ 0.00%    │ 748           │ 0               │ 0             │
   │ 192.168.100.11        │ 1     │ 0.00%    │ 757           │ 0               │ 0             │
   │ 192.168.100.12        │ 0     │ 0.00%    │ 740           │ 0               │ 0             │
   │ 192.168.100.12        │ 1     │ 0.00%    │ 740           │ 0               │ 0             │
   │ 192.168.100.13        │ 0     │ 0.00%    │ 922           │ 0               │ 0             │
   │ 192.168.100.13        │ 1     │ 0.00%    │ 930           │ 0               │ 0             │
   │ 192.168.100.21        │ 0     │ 0.00%    │ 765           │ 0               │ 0             │
   │ 192.168.100.21        │ 1     │ 0.00%    │ 767           │ 0               │ 0             │
   │ 192.168.100.22        │ 0     │ 0.00%    │ 791           │ 0               │ 0             │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ system_traces         │ shard │ progress │ segment_count │ segment_success │ segment_error │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ 192.168.100.11        │ 0     │ 0.00%    │ 748           │ 0               │ 0             │
   │ 192.168.100.11        │ 1     │ 0.00%    │ 757           │ 0               │ 0             │
   │ 192.168.100.12        │ 0     │ 0.00%    │ 740           │ 0               │ 0             │
   │ 192.168.100.12        │ 1     │ 0.00%    │ 740           │ 0               │ 0             │
   │ 192.168.100.13        │ 0     │ 0.00%    │ 922           │ 0               │ 0             │
   │ 192.168.100.13        │ 1     │ 0.00%    │ 930           │ 0               │ 0             │
   │ 192.168.100.21        │ 0     │ 0.00%    │ 765           │ 0               │ 0             │
   │ 192.168.100.21        │ 1     │ 0.00%    │ 767           │ 0               │ 0             │
   │ 192.168.100.22        │ 0     │ 0.00%    │ 791           │ 0               │ 0             │
   │ 192.168.100.22        │ 1     │ 0.00%    │ 807           │ 0               │ 0             │
   │ 192.168.100.23        │ 0     │ 0.00%    │ 752           │ 0               │ 0             │
   │ 192.168.100.23        │ 1     │ 0.00%    │ 747           │ 0               │ 0             │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ test_keyspace_dc1_rf2 │ shard │ progress │ segment_count │ segment_success │ segment_error │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ 192.168.100.11        │ 0     │ 0.00%    │ 1339          │ 0               │ 0             │
   │ 192.168.100.11        │ 1     │ 0.00%    │ 1339          │ 0               │ 0             │
   │ 192.168.100.12        │ 0     │ 0.00%    │ 1482          │ 0               │ 0             │
   │ 192.168.100.12        │ 1     │ 0.00%    │ 1480          │ 0               │ 0             │
   │ 192.168.100.13        │ 0     │ 0.00%    │ 1523          │ 0               │ 0             │
   │ 192.168.100.13        │ 1     │ 0.00%    │ 1528          │ 0               │ 0             │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ test_keyspace_dc1_rf3 │ shard │ progress │ segment_count │ segment_success │ segment_error │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ 192.168.100.11        │ 0     │ 0.00%    │ 1339          │ 0               │ 0             │
   │ 192.168.100.11        │ 1     │ 0.00%    │ 1339          │ 0               │ 0             │
   │ 192.168.100.12        │ 0     │ 0.00%    │ 1482          │ 0               │ 0             │
   │ 192.168.100.12        │ 1     │ 0.00%    │ 1480          │ 0               │ 0             │
   │ 192.168.100.13        │ 0     │ 0.00%    │ 1523          │ 0               │ 0             │
   │ 192.168.100.13        │ 1     │ 0.00%    │ 1528          │ 0               │ 0             │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ test_keyspace_dc2_rf2 │ shard │ progress │ segment_count │ segment_success │ segment_error │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ 192.168.100.21        │ 0     │ 0.00%    │ 1349          │ 0               │ 0             │
   │ 192.168.100.21        │ 1     │ 0.00%    │ 1343          │ 0               │ 0             │
   │ 192.168.100.22        │ 0     │ 0.00%    │ 1550          │ 0               │ 0             │
   │ 192.168.100.22        │ 1     │ 0.00%    │ 1561          │ 0               │ 0             │
   │ 192.168.100.23        │ 0     │ 0.00%    │ 1450          │ 0               │ 0             │
   │ 192.168.100.23        │ 1     │ 0.00%    │ 1465          │ 0               │ 0             │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ test_keyspace_dc2_rf3 │ shard │ progress │ segment_count │ segment_success │ segment_error │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ 192.168.100.21        │ 0     │ 0.00%    │ 1349          │ 0               │ 0             │
   │ 192.168.100.21        │ 1     │ 0.00%    │ 1343          │ 0               │ 0             │
   │ 192.168.100.22        │ 0     │ 0.00%    │ 1550          │ 0               │ 0             │
   │ 192.168.100.22        │ 1     │ 0.00%    │ 1561          │ 0               │ 0             │
   │ 192.168.100.23        │ 0     │ 0.00%    │ 1450          │ 0               │ 0             │
   │ 192.168.100.23        │ 1     │ 0.00%    │ 1465          │ 0               │ 0             │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ test_keyspace_rf2     │ shard │ progress │ segment_count │ segment_success │ segment_error │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ 192.168.100.21        │ 0     │ 0.00%    │ 1349          │ 0               │ 0             │
   │ 192.168.100.21        │ 1     │ 0.00%    │ 1343          │ 0               │ 0             │
   │ 192.168.100.22        │ 0     │ 0.00%    │ 1550          │ 0               │ 0             │
   │ 192.168.100.22        │ 1     │ 0.00%    │ 1561          │ 0               │ 0             │
   │ 192.168.100.23        │ 0     │ 0.00%    │ 1450          │ 0               │ 0             │
   │ 192.168.100.23        │ 1     │ 0.00%    │ 1465          │ 0               │ 0             │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ test_keyspace_rf3     │ shard │ progress │ segment_count │ segment_success │ segment_error │
   ├───────────────────────┼───────┼──────────┼───────────────┼─────────────────┼───────────────┤
   │ 192.168.100.11        │ 0     │ 0.00%    │ 1339          │ 0               │ 0             │
   │ 192.168.100.11        │ 1     │ 0.00%    │ 1339          │ 0               │ 0             │
   │ 192.168.100.12        │ 0     │ 0.00%    │ 1482          │ 0               │ 0             │
   │ 192.168.100.12        │ 1     │ 0.00%    │ 1480          │ 0               │ 0             │
   │ 192.168.100.13        │ 0     │ 0.00%    │ 1523          │ 0               │ 0             │
   │ 192.168.100.13        │ 1     │ 0.00%    │ 1528          │ 0               │ 0             │
   ╰───────────────────────┴───────┴──────────┴───────────────┴─────────────────┴───────────────╯

Example with a backup task

.. code-block:: none

   sctool task progress -c prod-cluster backup/f0642e3e-e1cb-44ba-8447-f8d43672bcfd
   Arguments:      -L s3:manager-backup-tests-eu-west-1
   Status:         RUNNING
   Start time:     02 Apr 20 10:09:27 UTC
   Duration:       40m30s
   Progress:       1%
   Snapshot Tag:   sm_20200402100931UTC
   Datacenters:
   - eu-west
   ╭──────────────┬──────────┬───────────┬──────────┬──────────────┬────────╮
   │ Host         │ Progress │      Size │  Success │ Deduplicated │ Failed │
   ├──────────────┼──────────┼───────────┼──────────┼──────────────┼────────┤
   │ 10.0.114.68  │       1% │ 952.11GiB │ 13.22GiB │       538KiB │     0B │
   │ 10.0.138.46  │       1% │ 938.00GiB │ 13.43GiB │       830MiB │     0B │
   │ 10.0.196.204 │       1% │ 934.58GiB │ 13.79GiB │       206MiB │     0B │
   │ 10.0.66.115  │       1% │ 897.43GiB │ 12.17GiB │       523KiB │     0B │
   ╰──────────────┴──────────┴───────────┴──────────┴──────────────┴────────╯

task start
==========

This command initiates a task run.
Note that if a repair task is already running on a cluster, other repair tasks runs on that cluster will fail.

**Syntax:**

.. code-block:: none

   sctool task start <task type/id> --cluster <id|name> [global flags]

.. include:: ../_common/task-id-note.rst

task start parameters
.....................

In addition to the :ref:`global-flags`, task start takes the following parameters:

=====

.. include:: ../_common/param-cluster.rst

=====

.. _task-start-param-continue:

``--continue``
^^^^^^^^^^^^^^

Try to resume the last run.

**Default** true

=====

Example: task start
...................

This example resumes running which was previously stopped.
To start a repair which is scheduled, but is currently not running use the ``task update`` command making sure to set the start time to ``now``.
See `Example: task update`_.


If you have stopped a repair you can resume it by running the following command.
You will need the task ID for this action.
This can be retrieved using the command ``sctool task list``.

.. code-block:: none

   sctool task start -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd

.. _task-stop:

task stop
=========

Stops a specified task, stopping an already stopped task has no effect.
If you want to stop all tasks see :doc:`Suspend and Resume </sctool/suspend-resume>`.

**Syntax:**

.. code-block:: none

   sctool task stop <task type/id> --cluster <id|name> [global flags]

.. include:: ../_common/task-id-note.rst

task stop parameters
.....................

In addition to the :ref:`global-flags`, task stop takes the following parameter:

=====

.. include:: ../_common/param-cluster.rst

=====

Example: task stop
..................

This example immediately stops a running repair.
The task is not deleted and can be resumed at a later time.
You will need the task ID for this action.
This can be retrieved using the command ``sctool task list``.

.. code-block:: none

   sctool task stop -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd

.. _task-update:

task update
===========

This command changes generic task parameters such as schedule.

**Syntax:**

.. code-block:: none

   sctool task update <task type/id> --cluster <id|name> [--enabled <bool>]
   [--name <alias>] [--tags <list of tags>]
   [--interval <time between task runs>]
   [--start-date <now+duration|RFC3339>]
   [global flags]

.. include:: ../_common/task-id-note.rst

task update parameters
......................

In addition to :ref:`global-flags`, task stop takes the following parameters:

=====

.. include:: ../_common/param-cluster.rst

=====

.. _task-update-param-e:

``-e, --enabled``
^^^^^^^^^^^^^^^^^

Setting enabled to false disables the task.
Disabled tasks are not executed and hidden from task list.
To show disabled tasks invoke ``sctool task list --all`` (see `task list`_).

**Default** true

=====

.. _task-update-param-n:

``-n, --name <alias>``
^^^^^^^^^^^^^^^^^^^^^^

Adds a name to a task.

=====

.. _task-update-param-tags:

``--tags <list of tags>``
^^^^^^^^^^^^^^^^^^^^^^^^^

Allows you to tag the task with a list of text.

=====

.. include:: ../_common/task-params.rst

=====

Example: task update
....................

This example disables the task

.. code-block:: none

   sctool task update -c prod-cluster repair/4d79ee63-7721-4105-8c6a-5b98c65c3e21 --enabled false


This example reschedules the repair to run in 3 hours from now instead of whatever time it was supposed to run and sets the repair to run every two days.
The new time you set replaces the time which was previously set.

.. code-block:: none

   sctool task update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd -s now+3h --interval 2d
