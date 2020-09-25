======
Repair
======

Repair is important to make sure that data across the nodes is consistent.
This is the last resort of defence with an eventually consistent database.
When you create a cluster a repair job is automatically scheduled.
This task is set to occur each week by default, but you can change it to another time, change its parameters or add additional repair tasks if you need.

.. contents::
   :depth: 3
   :local:

Benefits of using Scylla Manager repairs
========================================

Scylla Manager automates the repair process and allows you to configure how and when repair occurs.
The advantages of using Scylla Manager for repair operations are:

#. Data selection - repair a single table or an entire cluster, the choice is up to you
#. Control - user controls the degree of parallelism, and intensity of a repair, the parameters may be modified on the flight

   - it's fast by default, Scylla Manager chooses the optimal number of parallel repairs supported by the cluster by default
   - it's in sync with node, intensity may be automatically adjusted to the maximum supported by a node
#. Pause and resume - repair can be paused and resumed later, it will continue where it left off
#. Retries - retries in case of transport (HTTP) errors, and retries of repairs of failed token ranges
#. Schema changes - Scylla Manager handles adding and removing tables and keyspaces during repair
#. Visibility - everything is managed from one place, progress can be read using CLI, REST API or Prometheus metrics, you can dig into details and get to know progress of individual tables and nodes

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
Scheduled repairs run every X days depending on the frequency you set. 
Additional parameters are described in the `sctool Reference <../sctool/#repair-parameters>`_.

Use the example below to run the sctool repair command.

.. code-block:: none

   sctool repair -c <id|name> [-s <date>] [-i <time-unit>]

where:

* ``-c`` - the `name <../sctool/#cluster-add>`_ you used when you created the cluster
* ``-s`` - the time you want the repair to begin
* ``-i`` - the time interval you want to use in between consecutive repairs

The command returns the task ID. You will need this ID for additional actions.
If you want to run the repair only once, leave out the interval argument (``-i``).
In case when you want the repair to start immediately, but you want it to schedule it to repeat at a determined interval, leave out the start flag (``-s``) and set the interval flag (``-i``) to the time you want the repair to reoccur.

Schedule a weekly repair
........................

This command will schedule a repair in 4 hours, repair will be repeated every week.

.. code-block:: none

   sctool repair -c prod-cluster -s now+4h -i 7d
   repair/41014974-f7a7-4d67-b75c-44f59cdc5105

Command returns the task ID (repair/41014974-f7a7-4d67-b75c-44f59cdc5105, in this case).
This ID can be used to query the status of the repair task, to defer the task to another time, or to cancel the task See `Managing Tasks <../sctool/#managing-tasks>`_.

Schedule a repair for a specific DC, keyspace, or table
--------------------------------------------------------
In order to schedule repair of particular data center, you have to specify ``--dc`` parameter.
You can specify more than one DC, or use glob pattern to match multiple DCs or exclude some of them.

For Example, you have the following DCs in your cluster: dc1, dc2, dc3

Repair one specific DC
......................

In this example you repair the only dc1 every 2 days.

.. code-block:: none

   sctool repair -c prod-cluster --dc 'dc1' -L 's3:dc1-repairs' -i 2d

Repair all DCs except for those specified
.........................................

.. code-block:: none

   sctool repair -c prod-cluster -i 30d --dc '*,!dc2' -L 's3:my-repairs'

Repair a specific keyspace or table
...................................

In order to schedule repair of particular keyspace or table, you have to provide ``-K`` parameter.
You can specify more than one keyspace/table or use glob pattern to match multiple keyspaces/tables or exclude them.

.. code-block:: none

   sctool repair -c prod-cluster -i 30d -K 'auth_service.*,!auth_service.lru_cache' --dc 'dc1' -L 's3:dc1-repairs'

Create an ad-hoc repair
=======================

An ad-hoc repair runs immediately and does not repeat.
This procedure shows the most frequently used repair commands.
Additional parameters can be used. Refer to `repair parameters <../sctool/#repair-parameters>`_.

**Procedure**

To run an immediate repair on the prod-cluster cluster, saving the repair in my-repairs, run the following command
replacing the ``-c`` cluster flag with your cluster's cluster name or ID and replace the ``-L`` flag with your repair's location:

.. code-block:: none

   sctool repair -c prod-cluster -L 's3:my-repairs'

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
     - system_auth (3 tables)
     - system_distributed (3 tables)
     - system_traces (5 tables)
     - test_keyspace (10 tables)

Note that if a keyspace has no tables or a table is empty it will not be listed here.
Nevertheless you can still schedule the repair, the glob patterns are evaluated before each repair run so when data is there it will be repaired.

Monitor progress of the repair task
===================================

Progress of the repair task can be monitored by using `sctool task progress <../sctool/#task-progress>`_ command and providing UUID of the repair task.

.. code-block:: none

   sctool task progress repair/143d160f-e53c-4890-a9e7-149561376cfd -c prod-cluster

Change the repair speed
=======================

Repair speed is controlled by two parameters: ``--intensity`` and ``--parallel``.
Those parameters can be set when you:

* Schedule a repair with `sctool repair <../sctool/#repair>`_
* Update a repair with `sctool repair update <../sctool/#repair-update>`_
* Run a repair with `sctool repair control <../sctool/#repair-control>`_

Please read the detailed information on the flags in the sctool reference: `intensity <../sctool/#intensity-float>`_, `parallel <../sctool/#parallel-integer>`_.

Repair faster
-------------

By default Scylla Manager runs repairs with full parallelism, the way to make faster is by increasing the intensity.
In this situation try setting ``--intensity 0``, that would adjust the number of token ranges per Scylla repair job to the maximum supported (in parallel) by a repair master node.
If you want to go faster than that you can set intensity to high values but this is not recommended.
By doing so you can make some time savings on Scylla repair job creation and status checking.
You pay with repair granularity, and in case you need to pause or retry the amount of repeated work will be significant.

Speedup a running repair
........................

If a repair is running on a cluster you can specify intensity and parallelism level that should be applied while it is running.
Stopping the task and running again would reset the values.
Current values for intensity and parallel can be checked in `sctool task progress <../sctool/#task-progress>`_.

Run the following command to adjust the number of token ranges per Scylla repair job to the maximum supported (in parallel) by a repair master node.

.. code-block:: none

   sctool repair control -c prod-cluster --intensity 0

Speedup the future runs of a repair
...................................

If you wish to change intensity and parallelism level of a repair task use `sctool repair update <../sctool/#repair-update>`_.

Run the following command to adjust the number of token ranges per Scylla repair job to the maximum supported (in parallel) by a repair master node.

.. code-block:: none

   sctool repair update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd --intensity 0

Repair slower
-------------

You can make repair run slower by changing the level of parallelism or intensity.
By default Scylla Manager runs repairs with full parallelism.
Try setting ``--parallel 1``, that would cap the number of Scylla repair jobs in the cluster to 1, and give air to some nodes.
This would have the same result as running Scylla Manager 2.1 or earlier in terms of parallelism.
You can also change the number of token ranges repaired in a single Scylla repair job.
Try setting ``--intensity 0.5``, that would repair the number of token ranges equal to half of the number of the shards of the repair master node for each job.
This may not, however, free the shards from repairing since every token range is owned by many shards.

Slowdown a running repair
.........................

If a repair is running on a cluster you can specify intensity and parallelism level that should be applied while it is running.
Stopping the task and running again would reset the values.
Current values for intensity and parallel can be checked in `sctool task progress <../sctool/#task-progress>`_.

Run the following command to limit the number of parallel Scylla repair jobs in the cluster to 1.

.. code-block:: none

   sctool repair control -c prod-cluster --parallel 1

Run the following command to repair the number of token ranges equal to half of the number of the shards of the repair master node in each Scylla repair job.

.. code-block:: none

   sctool repair control -c prod-cluster --intensity 0.5

Slowdown the future runs of a repair
....................................

If you wish to change intensity and parallelism level of a repair task use `sctool repair update <../sctool/#repair-update>`_.

Run the following command to limit the number of parallel Scylla repair jobs in the cluster to 1.

.. code-block:: none

   sctool repair update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd --parallel 1

Run the following command to repair the number of token ranges equal to half of the number of the shards of the repair master node in each Scylla repair job.

.. code-block:: none

   sctool repair update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd --intensity 0.5
