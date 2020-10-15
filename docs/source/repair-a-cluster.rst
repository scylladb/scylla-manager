================
Repair a Cluster
================

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

* Data selection - repair a single table or an entire cluster, the choice is up to you
* Control - user controls the degree of parallelism, and intensity of a repair, the parameters may be modified on the flight
* Fast by default - Scylla Manager chooses the optimal number of parallel repairs supported by the cluster by default
* In sync with node - intensity may be automatically adjusted to the maximum supported by a node
* Pause and resume - repair can be paused and resumed later, it will continue where it left off
* Retries - retries in case of transport (HTTP) errors, and retries of repairs of failed token ranges
* Schema changes - Scylla Manager handles adding and removing tables and keyspaces during repair
* Visibility - everything is managed from one place, progress can be read using CLI, REST API or Prometheus metrics, you can dig into details and get to know progress of individual tables and nodes

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
Scheduled repairs run every X days depending on the frequency you set. 
Additional parameters are described in the :ref:`sctool Repair Reference <repair-commands>`.

Use the example below to run the sctool repair command.

.. code-block:: none

   sctool repair -c <id|name> [-s <date>] [-i <time-unit>]

where:

* ``-c`` - the :ref:`name <cluster-add>` you used when you created the cluster
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
This ID can be used to query the status of the repair task, to defer the task to another time, or to cancel the task See :ref:`Managing Tasks <task-commands>`.

Schedule a repair for a specific DC, keyspace, or table
--------------------------------------------------------
In order to schedule repair of particular data center, you have to specify ``--dc`` parameter.
You can specify more than one DC, or use glob pattern to match multiple DCs or exclude some of them.

For Example, you have the following DCs in your cluster: dc1, dc2, dc3

Repair one specific DC
......................

In this example, only dc1 is repaired. The repair repeats every 5 days.

.. code-block:: none

   sctool repair -c prod-cluster -i 5d --dc 'dc1'

Repair all DCs except for those specified
.........................................

.. code-block:: none

   sctool repair -c prod-cluster -i 5d --dc '*,!dc2'

Repair a specific keyspace or table
...................................

In order to schedule repair of particular keyspace or table, you have to provide ``-K`` parameter.
You can specify more than one keyspace/table or use glob pattern to match multiple keyspaces/tables or exclude them.

.. code-block:: none

   sctool repair -c prod-cluster -K 'auth_service.*,!auth_service.lru_cache' --dc 'dc1'

Repair a specific node
......................

In this example, you repair only token ranges replicated by the node with IP ``34.203.122.52``.

.. code-block:: none

   sctool repair -c prod-cluster --host 34.203.122.52

Create an ad-hoc repair
=======================

An ad-hoc repair runs immediately and does not repeat.
This procedure shows the most frequently used repair commands.
Additional parameters can be used. Refer to :ref:`repair parameters <repair-parameters>`.

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

Progress of the repair task can be monitored by using the :ref:`sctool task progress <task-progress>` command and providing UUID of the repair task.

.. code-block:: none

   sctool task progress repair/143d160f-e53c-4890-a9e7-149561376cfd -c prod-cluster

.. _change-speed:

Change the repair speed
=======================

Repair speed is controlled by two parameters: ``--intensity`` and ``--parallel``.
Those parameters can be set when you:

* Schedule a repair with :ref:`sctool repair <sctool-repair>`
* Update a repair with :ref:`sctool repair update <reschedule-a-repair>`
* Run a repair with :ref:`sctool repair control <repair-control>`

Please read the detailed information on the flags in the sctool reference: :ref:`intensity <intensity-float>`, :ref:`parallel <parallel-integer>`.

Repair faster
-------------

By default Scylla Manager runs repairs with full parallelism, the way to make repairs faster is by increasing the intensity.
In this example, try setting ``--intensity 0``, that would adjust the number of token ranges per Scylla repair job to the maximum supported (in parallel) by a repair master node.
If you want to go faster than that, you can set intensity to a higher value, but this is not recommended.
By doing so, you can make some time savings on the Scylla repair job creation and status checking.
You pay with repair granularity, and in case you need to pause or retry the repair, the amount of repeated work will be significant.

Speedup a running repair
........................

If a repair is running on a cluster, you can change the intensity and parallelism level that should be applied while it is running.
Stopping the task and running it again would reset the values.
You can view your current values for intensity and parallel with the :ref:`sctool task progress <task-progress>` command.

Run the following command to adjust the number of token ranges per Scylla repair job to the maximum supported (in parallel) by a repair master node.

.. code-block:: none

   sctool repair control -c prod-cluster --intensity 0

Speedup the future runs of a repair
...................................

If you wish to change intensity and parallelism level of a repair task that is scheduled, but not running use :ref:`sctool repair update <repair-update>`.

For example, the following command adjusts the number of token ranges per Scylla repair job to the maximum supported (in parallel) by a repair master node.

.. code-block:: none

   sctool repair update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd --intensity 0

Repair slower
-------------

You can make repair run slower by changing the level of parallelism or intensity.
By default Scylla Manager runs repairs with full parallelism.
Setting ``--parallel 1``, that caps the number of Scylla repair jobs in the cluster to 1, and gives space to some nodes.
This would have the same result as running Scylla Manager 2.1 or earlier in terms of parallelism.
For Scylla clusters that **do not support row-level repair**, you can change the number of shards that are being repaired in parallel.
In this case, setting ``--intensity 0.5``, that runs the repair on half of the shards in parallel.

.. note::
   For Scylla clusters that are row-level repair enabled, setting intensity below 1 has the **same effect** as setting intensity 1.

Slowdown a running repair
.........................

If a repair is running on a cluster, you can change intensity and parallelism levels while it is running.
Stopping the task and running again, would reset the values.
You can view your current values for intensity and parallel with the :ref:`sctool task progress <task-progress>` command.

Run the following command to limit the number of parallel Scylla repair jobs in the cluster to 1.

.. code-block:: none

   sctool repair control -c prod-cluster --parallel 1

For clusters **not supporting row-level repair**.
Run the following command to repair half of the shards on repair master node in parallel.

.. code-block:: none

   sctool repair control -c prod-cluster --intensity 0.5

Slowdown the future runs of a repair
....................................

If you wish to change intensity and parallelism level of a repair task use :ref:`sctool repair update <repair-update>`.

Run the following command to limit the number of parallel Scylla repair jobs in the cluster to 1.

.. code-block:: none

   sctool repair update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd --parallel 1

For clusters **not supporting row-level repair**.
Run the following command to repair half of the shards on repair master node in parallel.

.. code-block:: none

   sctool repair update -c prod-cluster repair/143d160f-e53c-4890-a9e7-149561376cfd --intensity 0.5
