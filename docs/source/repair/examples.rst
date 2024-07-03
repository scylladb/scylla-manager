=========
Examples
=========

.. contents::
   :depth: 1
   :local:

This document contains examples for scheduling and running repairs.

.. _schedule-a-repair:

Schedule a repair
=================

A cluster, added to Scylla Manager, has a repair task created for it.
This task repairs the entire cluster every week.
You can change it, add additional repairs, or delete it.
You can schedule repairs to run in the future on a regular basis or schedule repairs to run once as you need them.
Any repair can be rescheduled, paused, resumed, or deleted.

Create a scheduled repair
-------------------------

While the most recommended way to run a repair is across an entire cluster, repairs can be scheduled to run on a single/multiple datacenters, keyspaces, or tables.
The selection mechanism, based on the glob patterns, gives you a lot of flexibility.
Scheduled repairs run according to the cron expression you set.
Additional parameters are described in the :ref:`sctool repair <repair-commands>` command reference.

Use the example below to run the sctool repair command.

.. code-block:: none

   sctool repair -c <id|name> [--cron <expression>]

where:

* ``-c`` - the :ref:`name <cluster-add>` you used when you created the cluster
* ``--cron`` - defines task schedule as a cron expression

The command returns the task ID. You will need this ID for additional actions.
If you want to run the repair only once, leave out the cron argument (``--cron``).

Schedule a weekly repair
........................

This command will schedule a repair running at 20:00, on every Saturday, every month.

.. code-block:: none

   sctool repair -c prod-cluster --cron "0 0 20 ? * SAT"
   repair/41014974-f7a7-4d67-b75c-44f59cdc5105

Command returns the task ID (repair/41014974-f7a7-4d67-b75c-44f59cdc5105, in this case).
This ID can be used to query the status of the repair task, to defer the task to another time, or to cancel the task See :ref:`Managing Tasks <task-commands>`.

Schedule a repair for a specific DC, keyspace, or table
--------------------------------------------------------
In order to schedule repair of particular data center, you have to specify ``--dc`` parameter.
You can specify more than one DC, or use glob pattern to match multiple DCs or exclude some of them.

For Example, you have the following DCs in your cluster: dc1, dc2, dc3

Repair one specific DC
......................

In this example, only dc1 is repaired.

.. code-block:: none

   sctool repair -c prod-cluster --dc 'dc1'

Repair all DCs except for those specified
.........................................

.. code-block:: none

   sctool repair -c prod-cluster --dc '*,!dc2'

Repair a specific keyspace or table
...................................

In order to schedule repair of particular keyspace or table, you have to provide ``-K`` parameter.
You can specify more than one keyspace/table or use glob pattern to match multiple keyspaces/tables or exclude them.

.. code-block:: none

   sctool repair -c prod-cluster -K 'system_distributed.*,!system_distributed.view_build_status' --dc 'dc1'

Repair a specific node
......................

In this example, you repair only token ranges replicated by the node with IP ``34.203.122.52``.

.. code-block:: none

   sctool repair -c prod-cluster --host 34.203.122.52


Create an ad-hoc repair
=======================

An ad-hoc repair runs immediately and does not repeat.
This procedure shows the most frequently used repair commands.
Additional parameters can be used. Refer to :ref:`repair parameters <sctool-repair>`.

**Procedure**

To run an immediate repair on the cluster, run the following command specifying the name or ID of your cluster with the ``-c`` flag:

.. code-block:: none

   sctool repair -c <cluster_name|cluster_id>

Example:

.. code-block:: none

   sctool repair -c prod-cluster

Perform a dry run of a repair
=============================

We recommend to use ``--dry-run`` parameter prior scheduling a repair if you specify datacenter, keyspace or table filters.
It's a useful way to verify that all the data you want will be repaired.
Add the parameter to the end of your repair command, so if it works, you can erase it and schedule the repair with no need to make any other changes.
If you do tables filtering you can pass ``--show-tables`` flag in order to print the table names next to keyspaces.

If the dry run completes successfully, a summary of the repair is displayed. For example:

.. code-block:: none

   sctool repair -c prod-cluster -K system*,test_keyspace.* --dry-run
   NOTICE: dry run mode, repair is not scheduled

   Token Ranges:
   Data Centers:
     - AWS_EU_CENTRAL_1

   Keyspaces:
     - system_distributed (3 tables)
     - system_traces (5 tables)
     - test_keyspace (10 tables)

Note that if a keyspace has no tables or a table is empty it will not be listed here.
Nevertheless you can still schedule the repair, the glob patterns are evaluated before each repair run so when data is there it will be repaired.

Monitor progress of the repair task
===================================

Progress of the repair task can be monitored by using the :ref:`sctool progress <task-progress>` command and providing UUID of the repair task.

.. code-block:: console

  sctool progress -c prod-cluster repair
  Run:            84227f44-4e2a-11ee-acca-0892040e83bb
  Status:         RUNNING
  Start time:     08 Sep 23 11:31:40 CEST
  Duration:       6s
  Progress:       50%
  Intensity:      5/5 (max) // running with '--intensity 0' when 5 is the max supported value
  Parallel:       2/3       // running with '--parallel 2'  when 3 is the max supported value
  Datacenters:
    - dc1
    - dc2

  ╭───────────────────────────────┬────────────────────────────────┬──────────┬──────────╮
  │ Keyspace                      │                          Table │ Progress │ Duration │
  ├───────────────────────────────┼────────────────────────────────┼──────────┼──────────┤
  │ test_keyspace                 │                   test_table_1 │ 100%     │ 0s       │
  │ test_keyspace                 │                   test_table_2 │ 100%     │ 0s       │
  │ test_keyspace                 │                   test_table_3 │ 0%       │ 0s       │
  │ test_keyspace                 │                   test_table_4 │ 0%       │ 0s       │
  ╰───────────────────────────────┴────────────────────────────────┴──────────┴──────────╯

Note that:

  * displayed parallel and intensity account for changes introduce with :ref:`sctool repair control <repair-control>`
  * when displaying progress of a running task:

    * maximal supported values for parallel and intensity will be displayed (parallel and intensity will be trimmed to them)
  * when displaying progress of an already finished task:

    * untrimmed parallel and intensity will be displayed